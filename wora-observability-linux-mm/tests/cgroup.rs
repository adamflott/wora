#![cfg(target_os = "linux")]

use std::fs;
use std::path::Path;

use wora_observability_linux_mm::{CgroupMemory, CgroupMemoryStats, CgroupMemoryValue, MemoryReadError, MemorySkipReason};

fn mount_line(root: &str, directory: &Path) -> String {
    let escaped = directory.to_string_lossy().replace('\\', "\\134").replace(' ', "\\040");
    format!("10 1 0:20 {root} {escaped} rw - cgroup2 cgroup rw\n")
}

#[test]
fn captures_hugetlb_limits_local_events_and_effective_memory_nodes() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    for (name, value) in [
        ("hugetlb.2MB.current", "9007199254740993\n"),
        ("hugetlb.1GB.max", "max\n"),
        ("hugetlb.2MB.events", "max 8\n"),
        ("hugetlb.2MB.events.local", "max 3\n"),
        ("hugetlb.2MB.numa_stat", "total=4096 N0=0 N9=4096\n"),
        ("cpuset.mems", "\n"),
        ("cpuset.mems.effective", "0-1,9\n"),
    ] {
        fs::write(root.path().join(name), value)?;
    }
    let stats = CgroupMemory::from_directory(root.path());
    assert!(stats.unavailable.is_empty());
    assert!(matches!(stats.parsed["hugetlb.2MB.current"], CgroupMemoryValue::Number(9_007_199_254_740_993)));
    assert!(matches!(&stats.parsed["hugetlb.1GB.max"], CgroupMemoryValue::Text(v) if v == "max"));
    assert!(matches!(&stats.parsed["hugetlb.2MB.events.local"], CgroupMemoryValue::Counters(v) if v["max"] == 3));
    assert!(
        matches!(&stats.parsed["cpuset.mems.effective"], CgroupMemoryValue::Structured(wora_observability_linux_mm::ParsedMemoryValue::List(v)) if v.len() == 2)
    );
    Ok(())
}

#[test]
fn discovers_mount_roots_escaped_paths_and_visible_ancestor_limits() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = tempfile::tempdir()?;
    let proc = fixture.path().join("proc");
    let mount = fixture.path().join("cgroup mount");
    let current = mount.join("service/worker");
    fs::create_dir_all(proc.join("self"))?;
    fs::create_dir_all(&current)?;
    fs::write(proc.join("self/cgroup"), "3:cpu:/ignored\n0::/tenant/service/worker\n")?;
    fs::write(proc.join("self/mountinfo"), mount_line("/tenant", &mount))?;
    fs::write(current.join("memory.current"), "9007199254740993\n")?;
    fs::write(current.join("memory.max"), "max\n")?;
    fs::write(mount.join("service/memory.max"), "1048576\n")?;
    fs::write(mount.join("memory.high"), "2097152\n")?;
    fs::write(mount.join("service/memory.reclaim"), [0xff])?;

    let snapshot = CgroupMemoryStats::from_proc_root(&proc);
    assert!(snapshot.unavailable.is_empty(), "{:?}", snapshot.unavailable);
    assert_eq!(snapshot.mount_point.as_deref(), Some(mount.as_path()));
    assert_eq!(snapshot.mount_root.as_deref(), Some(Path::new("/tenant")));
    let group = snapshot.current.ok_or("missing cgroup")?;
    assert_eq!(group.directory, current);
    assert!(matches!(group.parsed["memory.current"], CgroupMemoryValue::Number(9_007_199_254_740_993)));
    assert!(matches!(&group.parsed["memory.max"], CgroupMemoryValue::Text(value) if value == "max"));
    assert_eq!(snapshot.ancestors.len(), 2);
    assert!(matches!(snapshot.ancestors[0].parsed["memory.max"], CgroupMemoryValue::Number(1_048_576)));
    assert!(matches!(snapshot.ancestors[1].parsed["memory.high"], CgroupMemoryValue::Number(2_097_152)));
    Ok(())
}

#[test]
fn retains_counters_sparse_numa_pressure_and_unknown_files() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    for (name, text) in [
        ("memory.stat", "anon 4096\nfuture_counter 18446744073709551615\n"),
        ("memory.events", "high 10\noom_kill 2\n"),
        ("memory.events.local", "high 3\noom_kill 1\n"),
        ("memory.numa_stat", "anon N2=4096 N99=8192\nfile N99=9007199254740993\n"),
        (
            "memory.pressure",
            "some avg10=1.00 avg60=2.00 avg300=3.00 total=9007199254740993\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=10\n",
        ),
        ("memory.future_text", "new kernel layout\n"),
        ("memory.swap.max", "max\n"),
    ] {
        fs::write(root.path().join(name), text)?;
    }
    fs::write(root.path().join("memory.reclaim"), [0xff])?;
    fs::write(root.path().join("cgroup.kill"), [0xff])?;
    let snapshot = CgroupMemory::from_directory(root.path());
    assert!(snapshot.unavailable.is_empty(), "{:?}", snapshot.unavailable);
    assert!(matches!(&snapshot.parsed["memory.stat"], CgroupMemoryValue::Counters(values) if values["future_counter"] == u64::MAX));
    assert!(matches!(&snapshot.parsed["memory.numa_stat"], CgroupMemoryValue::Numa(values) if values["file"][&99] == 9_007_199_254_740_993));
    assert!(matches!(&snapshot.parsed["memory.pressure"], CgroupMemoryValue::Pressure(value) if value.some.total == 9_007_199_254_740_993));
    assert_eq!(snapshot.skipped["memory.reclaim"], MemorySkipReason::WriteOnly);
    assert!(!snapshot.parsed.contains_key("cgroup.kill"));
    #[cfg(feature = "serde1")]
    {
        let json = serde_json::to_value(&snapshot)?;
        assert_eq!(json["parsed"]["memory.events"]["high"], 10);
        assert_eq!(json["parsed"]["memory.events.local"]["high"], 3);
        assert_eq!(json["parsed"]["memory.stat"]["future_counter"], u64::MAX);
        assert_eq!(json["parsed"]["memory.swap.max"], "max");
    }
    Ok(())
}

#[test]
fn reports_v1_missing_mounts_and_unsafe_paths_without_guessing() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    fs::create_dir_all(root.path().join("self"))?;
    fs::write(root.path().join("self/cgroup"), "3:memory:/legacy\n")?;
    let snapshot = CgroupMemoryStats::from_proc_root(root.path());
    assert!(snapshot.current.is_none());
    assert!(snapshot.unavailable.contains_key("self/cgroup"));
    for membership in ["0::/../escape\n", "0::/deleted (deleted)\n", "0::/one\n0::/two\n"] {
        fs::write(root.path().join("self/cgroup"), membership)?;
        assert!(CgroupMemoryStats::from_proc_root(root.path()).current.is_none());
    }
    fs::write(root.path().join("self/cgroup"), "0::/valid\n")?;
    fs::write(root.path().join("self/mountinfo"), "")?;
    assert!(CgroupMemoryStats::from_proc_root(root.path()).unavailable.contains_key("cgroup2"));
    Ok(())
}

#[test]
fn selects_the_closest_mount_and_handles_a_namespace_root() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let proc = root.path().join("proc");
    let general = root.path().join("general");
    let scoped = root.path().join("scoped");
    fs::create_dir_all(proc.join("self"))?;
    fs::create_dir_all(scoped.join("service"))?;
    fs::write(proc.join("self/cgroup"), "0::/tenant/service\n")?;
    fs::write(
        proc.join("self/mountinfo"),
        format!(
            "{}{}{}",
            mount_line("/", &general),
            mount_line("/tenant2", &general),
            mount_line("/tenant", &scoped)
        ),
    )?;
    fs::write(scoped.join("service/memory.current"), "123\n")?;
    let snapshot = CgroupMemoryStats::from_proc_root(&proc);
    assert_eq!(snapshot.mount_point.as_deref(), Some(scoped.as_path()));
    assert_eq!(snapshot.ancestors.len(), 1);
    assert_eq!(snapshot.ancestors[0].directory, scoped);
    fs::write(proc.join("self/cgroup"), "0::/\n")?;
    fs::write(proc.join("self/mountinfo"), mount_line("/", &scoped))?;
    let namespace = CgroupMemoryStats::from_proc_root(&proc);
    assert!(namespace.unavailable.is_empty());
    assert_eq!(namespace.current.ok_or("namespace root missing")?.directory, scoped);
    assert!(namespace.ancestors.is_empty());
    Ok(())
}

#[test]
fn malformed_files_are_isolated_and_do_not_return_partial_counters() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    for malformed in ["anon 1\nanon 2\n", "anon 1\nbad -1\n", "anon 1\nbad 18446744073709551616\n", "bad\n"] {
        fs::write(root.path().join("memory.stat"), malformed)?;
        fs::write(root.path().join("memory.current"), "123\n")?;
        let snapshot = CgroupMemory::from_directory(root.path());
        assert!(!snapshot.parsed.contains_key("memory.stat"));
        assert!(matches!(snapshot.unavailable["memory.stat"], MemoryReadError::ReadFailed(_)));
        assert!(matches!(snapshot.parsed["memory.current"], CgroupMemoryValue::Number(123)));
    }
    fs::write(root.path().join("memory.numa_stat"), "anon N2=1 N2=2\n")?;
    assert!(CgroupMemory::from_directory(root.path()).unavailable.contains_key("memory.numa_stat"));
    Ok(())
}

#[test]
fn equally_rooted_mounts_use_the_lexicographically_first_mount_point() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let proc = root.path().join("proc");
    let first = root.path().join("a");
    let last = root.path().join("z");
    fs::create_dir_all(proc.join("self"))?;
    fs::create_dir_all(first.join("service"))?;
    fs::write(proc.join("self/cgroup"), "0::/service\n")?;
    for mounts in [
        format!("{}{}", mount_line("/", &last), mount_line("/", &first)),
        format!("{}{}", mount_line("/", &first), mount_line("/", &last)),
    ] {
        fs::write(proc.join("self/mountinfo"), mounts)?;
        assert_eq!(CgroupMemoryStats::from_proc_root(&proc).mount_point.as_deref(), Some(first.as_path()));
    }
    Ok(())
}
