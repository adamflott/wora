#![cfg(target_os = "linux")]
use std::{fs, os::unix::fs::symlink};
use wora_observability_linux_mm::{MemoryFiles, MemoryReadError, MemorySkipReason, NumaStats, ParsedMemoryValue};

#[test]
fn access_class_topology_preserves_node_scope_units_and_independent_failures() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let node = root.path().join("devices/system/node/node9");
    for (class, name, value) in [
        (0, "read_latency", "112\n"),
        (0, "write_latency", "130\n"),
        (0, "read_bandwidth", "18446744073709551615\n"),
        (0, "write_bandwidth", "0\n"),
        (1, "read_latency", "23\n"),
    ] {
        let directory = node.join(format!("access{class}/initiators"));
        fs::create_dir_all(&directory)?;
        fs::write(directory.join(name), value)?;
    }
    symlink("access0", node.join("access5"))?;
    let snapshot = NumaStats::from_sys_root(root.path());
    let files = &snapshot.nodes[&9];
    let latency = &files.parsed["access0/initiators/read_latency"];
    assert_eq!(latency.get("value").and_then(ParsedMemoryValue::as_u64), Some(112));
    assert_eq!(latency.get("unit"), Some(&ParsedMemoryValue::try_text("ns")?));
    let bandwidth = &files.parsed["access0/initiators/read_bandwidth"];
    assert_eq!(bandwidth.get("value").and_then(ParsedMemoryValue::as_u64), Some(u64::MAX));
    assert_eq!(bandwidth.get("unit"), Some(&ParsedMemoryValue::try_text("MiB/s")?));
    assert_eq!(
        files.parsed["access0/initiators/write_bandwidth"]
            .get("value")
            .and_then(ParsedMemoryValue::as_u64),
        Some(0)
    );
    assert!(matches!(
        files.unavailable.get("access1/initiators/write_latency"),
        Some(MemoryReadError::NotFound)
    ));
    assert_eq!(files.skipped["access5"], MemorySkipReason::NotRegularFile);
    #[cfg(feature = "serde1")]
    assert_eq!(
        serde_json::to_value(&snapshot)?["nodes"]["9"]["parsed"]["access0/initiators/read_latency"]["unit"],
        "ns"
    );
    for bad in ["-1", "112 ns", "NaN", "18446744073709551616", "1\n2"] {
        fs::write(node.join("access0/initiators/read_latency"), bad)?;
        let snapshot = NumaStats::from_sys_root(root.path());
        assert!(!snapshot.nodes[&9].parsed.contains_key("access0/initiators/read_latency"));
        assert!(snapshot.nodes[&9].unavailable.contains_key("access0/initiators/read_latency"));
        assert!(snapshot.nodes[&9].parsed.contains_key("access0/initiators/read_bandwidth"));
    }
    Ok(())
}

#[test]
fn access_class_traversal_and_attribute_reads_are_bounded() -> Result<(), Box<dyn std::error::Error>> {
    use wora_observability_linux_mm::{CollectionLimit, CollectionLimits, collect_with_limits};
    let root = tempfile::tempdir()?;
    let node = root.path().join("devices/system/node/node0");
    for class in 0..65 {
        fs::create_dir_all(node.join(format!("access{class}/initiators")))?;
    }
    fs::write(node.join("access0/initiators/read_latency"), "1000\n")?;
    fs::write(node.join("access0/initiators/scan"), [0xff])?;
    symlink("scan", node.join("access0/initiators/write_latency"))?;
    let stats = NumaStats::from_sys_root(root.path());
    let files = &stats.nodes[&0];
    assert!(files.unavailable.contains_key("."));
    assert_eq!(files.parsed.len() + files.unavailable.len() + files.skipped.len(), 6 + 64 * 4 + 1);
    assert_eq!(files.skipped["access0/initiators/write_latency"], MemorySkipReason::NotRegularFile);
    assert!(!files.unavailable.contains_key("access0/initiators/scan"));
    let (stats, report) = collect_with_limits(
        CollectionLimits {
            file_bytes: 4,
            line_bytes: 4,
            ..Default::default()
        },
        || NumaStats::from_sys_root(root.path()),
    )?;
    assert!(!stats.nodes[&0].parsed.contains_key("access0/initiators/read_latency"));
    assert_eq!(report.limit_exceeded, Some(CollectionLimit::FileBytes));
    let (_, report) = collect_with_limits(
        CollectionLimits {
            entries: 1,
            ..Default::default()
        },
        || NumaStats::from_sys_root(root.path()),
    )?;
    assert_eq!(report.limit_exceeded, Some(CollectionLimit::Entries));
    Ok(())
}

#[test]
fn weighted_interleave_supports_sparse_69_weights_and_616_auto_mode_without_writes() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let directory = root.path().join("kernel/mm/mempolicy/weighted_interleave");
    fs::create_dir_all(&directory)?;
    fs::write(directory.join("node0"), "1\n")?;
    fs::write(directory.join("node99"), "255\n")?;
    let legacy = NumaStats::from_sys_root(root.path()).weighted_interleave;
    assert_eq!(legacy.parsed["node0"], ParsedMemoryValue::Unsigned(1));
    assert_eq!(legacy.parsed["node99"], ParsedMemoryValue::Unsigned(255));
    assert!(!legacy.parsed.contains_key("auto"));
    for mode in [true, false] {
        let text = format!("{mode}\n");
        fs::write(directory.join("auto"), &text)?;
        let policy = NumaStats::from_sys_root(root.path()).weighted_interleave;
        assert_eq!(policy.parsed["auto"], ParsedMemoryValue::Bool(mode));
        assert_eq!(fs::read_to_string(directory.join("auto"))?, text);
        assert_eq!(fs::read_to_string(directory.join("node99"))?, "255\n");
        #[cfg(feature = "serde1")]
        assert_eq!(serde_json::to_value(&policy)?["parsed"]["auto"], mode);
    }
    fs::remove_file(directory.join("node99"))?;
    assert!(!NumaStats::from_sys_root(root.path()).weighted_interleave.parsed.contains_key("node99"));
    Ok(())
}

#[test]
fn weighted_interleave_isolates_bad_values_excludes_links_and_shares_limits() -> Result<(), Box<dyn std::error::Error>> {
    use wora_observability_linux_mm::{CollectionLimit, CollectionLimits, collect_with_limits, parse_memory_file};
    let root = tempfile::tempdir()?;
    let directory = root.path().join("kernel/mm/mempolicy/weighted_interleave");
    fs::create_dir_all(&directory)?;
    fs::write(directory.join("node9"), "2\n")?;
    fs::write(directory.join("auto"), "true\n")?;
    fs::write(directory.join("future_control"), [0xff])?;
    symlink("future_control", directory.join("node3"))?;
    for bad in ["0", "256", "-1", "+1", "1 2", "1\n2", "new"] {
        fs::write(directory.join("node0"), bad)?;
        let policy = NumaStats::from_sys_root(root.path()).weighted_interleave;
        assert!(!policy.parsed.contains_key("node0"));
        assert!(policy.unavailable.contains_key("node0"));
        assert_eq!(policy.parsed["node9"], ParsedMemoryValue::Unsigned(2));
        assert_eq!(policy.skipped["node3"], MemorySkipReason::NotRegularFile);
        assert!(!policy.unavailable.contains_key("future_control"));
    }
    assert!(parse_memory_file(directory.join("auto"), "unknown").is_err());
    let (_, report) = collect_with_limits(
        CollectionLimits {
            entries: 1,
            ..Default::default()
        },
        || NumaStats::from_sys_root(root.path()),
    )?;
    assert_eq!(report.limit_exceeded, Some(CollectionLimit::Entries));
    fs::remove_dir_all(&directory)?;
    let policy = NumaStats::from_sys_root(root.path()).weighted_interleave;
    assert!(matches!(policy.unavailable.get("."), Some(MemoryReadError::NotFound)));
    Ok(())
}

#[test]
fn sparse_nodes_tiers_and_native_units_survive_capture() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    for (name, value) in [
        ("devices/system/node/online", "0,9\n"),
        ("devices/system/node/node9/meminfo", "Node 9 MemFree: 9007199254740993 kB\n"),
        ("devices/system/node/node9/numastat", "numa_hit 18446744073709551615\n"),
        ("devices/system/node/node9/distance", "20 10\n"),
        ("devices/virtual/memory_tiering/memory_tier22/nodelist", "9\n"),
    ] {
        let path = root.path().join(name);
        fs::create_dir_all(path.parent().ok_or("no parent")?)?;
        fs::write(path, value)?;
    }
    symlink(root.path().join("devices/system/node/node9"), root.path().join("devices/system/node/node10"))?;
    let snapshot = NumaStats::from_sys_root(root.path());
    assert_eq!(snapshot.nodes.len(), 1);
    assert_eq!(
        snapshot.nodes[&9].parsed["meminfo"]
            .get("MemFree")
            .and_then(|value| value.get("value"))
            .and_then(ParsedMemoryValue::as_u64),
        Some(9_007_199_254_740_993)
    );
    assert_eq!(
        snapshot.tiers[&22].parsed["nodelist"],
        ParsedMemoryValue::try_list(vec![ParsedMemoryValue::try_map(std::collections::BTreeMap::from([
            ("start", ParsedMemoryValue::Unsigned(9)),
            ("end", ParsedMemoryValue::Unsigned(9))
        ]))?])?
    );
    assert_eq!(snapshot.nodes[&9].unavailable["vmstat"], MemoryReadError::NotFound);
    #[cfg(feature = "serde1")]
    assert_eq!(serde_json::to_value(snapshot)?["nodes"]["9"]["parsed"]["distance"], serde_json::json!([20, 10]));
    Ok(())
}

#[test]
fn bounded_files_preserve_precision_and_isolate_bad_inputs() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    for (name, value) in [("large", "18446744073709551615"), ("signed", "-3"), ("overflow", "18446744073709551616")] {
        fs::write(root.path().join(name), value)?;
    }
    fs::write(root.path().join("oversized"), vec![b'x'; 4 * 1024 * 1024 + 1])?;
    symlink(root.path().join("large"), root.path().join("link"))?;
    let snapshot = MemoryFiles::from_names(root.path(), &["large", "signed", "overflow", "oversized", "link", "../escape"]);
    assert_eq!(snapshot.parsed["large"], ParsedMemoryValue::Unsigned(u64::MAX));
    assert_eq!(snapshot.parsed["signed"], ParsedMemoryValue::Signed(-3));
    assert_eq!(snapshot.skipped["link"], MemorySkipReason::NotRegularFile);
    for name in ["overflow", "oversized", "../escape"] {
        assert!(snapshot.unavailable.contains_key(name));
    }
    Ok(())
}
