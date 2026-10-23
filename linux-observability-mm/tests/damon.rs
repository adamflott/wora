#![cfg(target_os = "linux")]

use std::{fs, os::unix::fs::PermissionsExt};
use wora_observability_linux_mm::{CollectionLimit, CollectionLimits, DamonStats, MemoryReadError, MemorySkipReason, collect_with_limits};

// Synthetic layouts from the version-tagged DAMON_STAT documentation, not
// booted-kernel fixtures. 6.19 adds signed active times/interval; 7.2 adds PID.
#[test]
fn versioned_layouts_preserve_monitor_state_signed_percentiles_and_units() -> Result<(), Box<dyn std::error::Error>> {
    for version in [617, 619, 702] {
        let root = tempfile::tempdir()?;
        let parameters = root.path().join("module/damon_stat/parameters");
        fs::create_dir_all(&parameters)?;
        fs::write(parameters.join("enabled"), "N\n")?;
        fs::write(parameters.join("estimated_memory_bandwidth"), "18446744073709551615\n")?;
        let expected: Vec<i64> = (0..=100).map(|n| if version >= 619 { n - 50 } else { n }).collect();
        fs::write(
            parameters.join("memory_idle_ms_percentiles"),
            expected.iter().map(i64::to_string).collect::<Vec<_>>().join(",") + "\n",
        )?;
        if version >= 619 {
            fs::write(parameters.join("aggr_interval_us"), "500000\n")?;
        }
        if version >= 702 {
            fs::write(parameters.join("kdamond_pid"), "-1\n")?;
        }
        let stats = DamonStats::from_sys_root(root.path());
        assert_eq!(stats.enabled, Some(false));
        // Disabling monitoring does not turn retained estimates into zero.
        assert_eq!(stats.estimated_memory_bandwidth, Some(u64::MAX));
        assert_eq!(stats.memory_idle_ms_percentiles.as_ref(), Some(&expected));
        assert_eq!(stats.aggr_interval_us, (version >= 619).then_some(500000));
        assert_eq!(stats.kdamond_pid, (version >= 702).then_some(-1));
        assert_eq!(
            stats.files.unavailable.len(),
            if version == 617 {
                2
            } else if version == 619 {
                1
            } else {
                0
            }
        );
        assert!(stats.files.unavailable.values().all(|error| matches!(error, MemoryReadError::NotFound)));
        assert_eq!(fs::read_to_string(parameters.join("enabled"))?, "N\n");
        #[cfg(feature = "serde1")]
        {
            let decoded: DamonStats = serde_json::from_str(&serde_json::to_string(&stats)?)?;
            assert_eq!(decoded.memory_idle_ms_percentiles, Some(expected));
            assert_eq!(decoded.estimated_memory_bandwidth, Some(u64::MAX));
            assert_eq!(decoded.kdamond_pid, stats.kdamond_pid);
        }
        fs::write(parameters.join("enabled"), "Y\n")?;
        fs::write(parameters.join("kdamond_pid"), "123\n")?;
        let active = DamonStats::from_sys_root(root.path());
        assert_eq!(active.enabled, Some(true));
        assert_eq!(active.kdamond_pid, Some(123));
    }
    Ok(())
}

#[test]
fn malformed_percentiles_fail_atomically_without_discarding_other_parameters() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let parameters = root.path().join("module/damon_stat/parameters");
    fs::create_dir_all(&parameters)?;
    fs::write(parameters.join("enabled"), "Y\n")?;
    for bad in [
        vec!["0"; 100].join(","),
        vec!["0"; 102].join(","),
        vec!["9223372036854775808"; 101].join(","),
        "0,,1".into(),
    ] {
        fs::write(parameters.join("memory_idle_ms_percentiles"), bad)?;
        let stats = DamonStats::from_sys_root(root.path());
        assert!(stats.memory_idle_ms_percentiles.is_none());
        assert!(matches!(
            stats.files.unavailable.get("memory_idle_ms_percentiles"),
            Some(MemoryReadError::ReadFailed(_))
        ));
        assert_eq!(stats.enabled, Some(true));
    }
    for (name, bad) in [
        ("enabled", "yes"),
        ("kdamond_pid", "2147483648"),
        ("aggr_interval_us", "-1"),
        ("estimated_memory_bandwidth", "1.5"),
    ] {
        fs::write(parameters.join(name), bad)?;
    }
    let stats = DamonStats::from_sys_root(root.path());
    assert!(stats.enabled.is_none());
    assert!(stats.kdamond_pid.is_none());
    assert!(stats.aggr_interval_us.is_none());
    assert!(stats.estimated_memory_bandwidth.is_none());
    assert_eq!(stats.files.unavailable.len(), 5);
    Ok(())
}

#[test]
fn reads_share_budgets_and_exclude_links_and_write_only_parameters() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let parameters = root.path().join("module/damon_stat/parameters");
    fs::create_dir_all(&parameters)?;
    fs::write(parameters.join("enabled"), "N\n")?;
    fs::write(parameters.join("estimated_memory_bandwidth"), "123456789\n")?;
    let (stats, report) = collect_with_limits(
        CollectionLimits {
            input_bytes: 8,
            file_bytes: 8,
            line_bytes: 8,
            ..Default::default()
        },
        || DamonStats::from_sys_root(root.path()),
    )?;
    assert_eq!(stats.enabled, Some(false));
    assert!(stats.estimated_memory_bandwidth.is_none());
    assert!(report.limit_exceeded.is_some());
    assert!(report.input_bytes <= 8);
    let (stats, report) = collect_with_limits(
        CollectionLimits {
            files: 1,
            ..Default::default()
        },
        || DamonStats::from_sys_root(root.path()),
    )?;
    assert_eq!(report.files, 1);
    assert_eq!(report.limit_exceeded, Some(CollectionLimit::Files));
    assert_eq!(stats.enabled, Some(false));
    assert!(stats.files.unavailable.contains_key("."));
    std::os::unix::fs::symlink("enabled", parameters.join("kdamond_pid"))?;
    fs::write(parameters.join("aggr_interval_us"), "4")?;
    fs::set_permissions(parameters.join("aggr_interval_us"), fs::Permissions::from_mode(0o200))?;
    let stats = DamonStats::from_sys_root(root.path());
    assert_eq!(stats.files.skipped.get("kdamond_pid"), Some(&MemorySkipReason::NotRegularFile));
    assert_eq!(stats.files.skipped.get("aggr_interval_us"), Some(&MemorySkipReason::WriteOnly));
    assert_eq!(fs::read_to_string(parameters.join("enabled"))?, "N\n");
    Ok(())
}
