#![cfg(target_os = "linux")]

use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};

use wora_observability_linux_mm::{MemorySysctls, ParsedMemoryValue as Value, SysctlReadError, SysctlSkipReason};

#[test]
fn retains_native_values_vectors_and_new_memory_settings() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    for namespace in ["vm", "kernel", "fs"] {
        fs::create_dir_all(root.path().join("sys").join(namespace))?;
    }
    // Representative values observed as a normal user on Linux 6.18.55.
    for (path, value) in [
        ("vm/swappiness", "60\n"),
        ("vm/lowmem_reserve_ratio", "256\t256\t32\t0\t0\n"),
        ("vm/numa_zonelist_order", "Node\n"),
        ("vm/defrag_mode", "0\n"),
        ("vm/future_memory_setting", "9\n"),
        ("kernel/shmall", "18446744073692774399\n"),
        ("kernel/shm_next_id", "-1\n"),
        ("kernel/numa_balancing", "0\n"),
        ("kernel/numa_balancing_promote_rate_limit_MBps", "65536\n"),
        ("kernel/numa_balancing_future_setting", "10\n"),
        ("kernel/perf_event_mlock_kb", "516\n"),
        ("fs/file-nr", "2446\t0\t9223372036854775807\n"),
        ("fs/inode-state", "282231\t1787\t0\t0\t0\t0\t0\n"),
        ("fs/dentry-state", "313090\t295429\t45\t0\t31382\t0\n"),
        ("kernel/hostname", "outside the memory scope\n"),
        ("fs/suid_dumpable", "outside the memory scope\n"),
    ] {
        fs::write(root.path().join("sys").join(path), value)?;
    }

    let snapshot = MemorySysctls::from_proc_root(root.path());
    assert_eq!(snapshot.parsed["vm.swappiness"], Value::Unsigned(60));
    assert_eq!(
        snapshot.parsed["vm.lowmem_reserve_ratio"],
        Value::try_list([256, 256, 32, 0, 0].map(Value::Unsigned))?
    );
    assert_eq!(snapshot.parsed["vm.numa_zonelist_order"], Value::try_text("Node")?);
    assert_eq!(snapshot.parsed["vm.defrag_mode"], Value::Unsigned(0));
    assert_eq!(snapshot.parsed["vm.future_memory_setting"], Value::Unsigned(9));
    assert_eq!(snapshot.parsed["kernel.shmall"], Value::Unsigned(18446744073692774399));
    assert_eq!(snapshot.parsed["kernel.shm_next_id"], Value::Signed(-1));
    assert_eq!(snapshot.parsed["kernel.numa_balancing_promote_rate_limit_MBps"], Value::Unsigned(65536));
    assert_eq!(snapshot.parsed["kernel.numa_balancing_future_setting"], Value::Unsigned(10));
    assert_eq!(snapshot.parsed["kernel.perf_event_mlock_kb"], Value::Unsigned(516));
    assert_eq!(snapshot.parsed["fs.file-nr"].get("max_files").and_then(Value::as_u64), Some(i64::MAX as u64));
    assert_eq!(snapshot.parsed["fs.inode-state"].get("nr_inodes").and_then(Value::as_u64), Some(282231));
    assert_eq!(snapshot.parsed["fs.dentry-state"].get("nr_negative").and_then(Value::as_u64), Some(31382));
    assert!(!snapshot.parsed.contains_key("kernel.hostname"));
    assert!(!snapshot.parsed.contains_key("fs.suid_dumpable"));
    assert!(snapshot.unavailable.is_empty());

    #[cfg(feature = "serde1")]
    {
        let json = serde_json::to_value(&snapshot)?;
        assert_eq!(json["parsed"]["kernel.shmall"], 18_446_744_073_692_774_399_u64);
        assert_eq!(json["parsed"]["vm.lowmem_reserve_ratio"], serde_json::json!([256, 256, 32, 0, 0]));
        assert!(json.get("values").is_none());
        let restored: MemorySysctls = serde_json::from_value(json)?;
        assert_eq!(restored, snapshot);
    }
    Ok(())
}

#[test]
fn excludes_actions_side_effect_reads_and_non_regular_files() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let vm = root.path().join("sys/vm");
    fs::create_dir_all(&vm)?;
    // If opened, these files would produce invalid UTF-8 errors. Exclusions
    // apply by sysctl name even when a privileged collector could read them.
    for name in ["stat_refresh", "compact_memory", "drop_caches"] {
        fs::write(vm.join(name), [0xff])?;
    }
    fs::write(vm.join("future_write_only_action"), [0xff])?;
    fs::set_permissions(vm.join("future_write_only_action"), fs::Permissions::from_mode(0o200))?;
    fs::write(vm.join("swappiness"), "60\n")?;
    symlink(vm.join("swappiness"), vm.join("linked_value"))?;
    fs::create_dir(vm.join("nested"))?;

    let snapshot = MemorySysctls::from_proc_root(root.path());
    assert_eq!(snapshot.parsed.len(), 1);
    assert_eq!(snapshot.parsed["vm.swappiness"], Value::Unsigned(60));
    assert_eq!(snapshot.skipped["vm.stat_refresh"], SysctlSkipReason::ReadSideEffect);
    assert_eq!(snapshot.skipped["vm.compact_memory"], SysctlSkipReason::WriteOnly);
    assert_eq!(snapshot.skipped["vm.drop_caches"], SysctlSkipReason::WriteOnly);
    assert_eq!(snapshot.skipped["vm.future_write_only_action"], SysctlSkipReason::WriteOnly);
    assert_eq!(snapshot.skipped["vm.linked_value"], SysctlSkipReason::NotRegularFile);
    assert_eq!(snapshot.skipped["vm.nested"], SysctlSkipReason::NotRegularFile);
    assert!(!snapshot.unavailable.keys().any(|key| key.starts_with("vm.")));
    Ok(())
}

#[test]
fn failed_sources_do_not_discard_other_values_or_retain_stale_data() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let vm = root.path().join("sys/vm");
    fs::create_dir_all(&vm)?;
    fs::write(vm.join("swappiness"), "60\n")?;
    fs::write(vm.join("invalid_text"), [0xff])?;

    let snapshot = MemorySysctls::from_proc_root(root.path());
    assert_eq!(snapshot.parsed["vm.swappiness"], Value::Unsigned(60));
    assert!(matches!(snapshot.unavailable["vm.invalid_text"], SysctlReadError::ReadFailed(_)));
    assert_eq!(snapshot.unavailable["kernel"], SysctlReadError::NotFound);
    assert_eq!(snapshot.unavailable["fs"], SysctlReadError::NotFound);

    fs::remove_file(vm.join("swappiness"))?;
    let next = MemorySysctls::from_proc_root(root.path());
    assert!(!next.parsed.contains_key("vm.swappiness"));
    assert!(!next.unavailable.contains_key("vm.swappiness"));
    Ok(())
}

#[test]
fn restricted_values_are_reported_as_permission_denied() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let vm = root.path().join("sys/vm");
    fs::create_dir_all(&vm)?;
    let path = vm.join("mmap_rnd_bits");
    fs::write(&path, "32\n")?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o000))?;
    if fs::read_to_string(&path).is_ok() {
        eprintln!("skipping permission-denied assertion: current user can bypass file permissions");
        return Ok(());
    }
    fs::write(vm.join("swappiness"), "60\n")?;
    let snapshot = MemorySysctls::from_proc_root(root.path());
    assert_eq!(snapshot.unavailable["vm.mmap_rnd_bits"], SysctlReadError::PermissionDenied);
    assert_eq!(snapshot.parsed["vm.swappiness"], Value::Unsigned(60));
    #[cfg(feature = "serde1")]
    assert_eq!(serde_json::to_value(snapshot)?["unavailable"]["vm.mmap_rnd_bits"], "permission_denied");
    Ok(())
}
