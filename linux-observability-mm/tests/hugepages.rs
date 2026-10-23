#![cfg(target_os = "linux")]

use std::fs;
use std::os::unix::fs::symlink;

use wora_observability_linux_mm::{HugePageFiles, HugePageStats, MemoryReadError, MemorySkipReason, ParsedMemoryValue};

#[test]
fn collects_policy_per_size_stats_and_sparse_node_pools() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    for (path, value) in [
        ("kernel/mm/transparent_hugepage/enabled", "always [madvise] never\n"),
        ("kernel/mm/transparent_hugepage/hpage_pmd_size", "2097152\n"),
        ("kernel/mm/transparent_hugepage/khugepaged/full_scans", "12\n"),
        ("kernel/mm/transparent_hugepage/hugepages-64kB/enabled", "always inherit [never]\n"),
        ("kernel/mm/transparent_hugepage/hugepages-64kB/stats/anon_fault_alloc", "9007199254740993\n"),
        ("kernel/mm/transparent_hugepage/hugepages-64kB/stats/future_counter", "18446744073709551615\n"),
        ("kernel/mm/hugepages/hugepages-2048kB/free_hugepages", "10\n"),
        ("kernel/mm/hugepages/hugepages-1048576kB/free_hugepages", "2\n"),
        ("devices/system/node/node99/hugepages/hugepages-2048kB/free_hugepages", "7\n"),
    ] {
        let path = root.path().join(path);
        fs::create_dir_all(path.parent().ok_or("parent missing")?)?;
        fs::write(path, value)?;
    }
    let snapshot = HugePageStats::from_sys_root(root.path());
    assert_eq!(
        snapshot.transparent.parsed["enabled"].get("selected"),
        Some(&ParsedMemoryValue::try_text("madvise")?)
    );
    assert_eq!(snapshot.transparent.parsed["hpage_pmd_size"], ParsedMemoryValue::Unsigned(2_097_152));
    assert_eq!(
        snapshot.transparent.parsed["hugepages-64kB/stats/future_counter"],
        ParsedMemoryValue::Unsigned(u64::MAX)
    );
    assert_eq!(snapshot.hugetlb.parsed["hugepages-1048576kB/free_hugepages"], ParsedMemoryValue::Unsigned(2));
    assert_eq!(snapshot.nodes[&99].parsed["hugepages-2048kB/free_hugepages"], ParsedMemoryValue::Unsigned(7));
    #[cfg(feature = "serde1")]
    assert_eq!(
        serde_json::to_value(&snapshot)?["transparent"]["parsed"]["hugepages-64kB/stats/anon_fault_alloc"],
        9_007_199_254_740_993_u64
    );
    Ok(())
}

#[test]
fn isolates_failures_and_never_opens_demotion_actions_or_symlinks() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let pool = root.path().join("hugepages-2048kB");
    fs::create_dir_all(&pool)?;
    fs::write(pool.join("demote"), [0xff])?;
    fs::write(pool.join("free_hugepages"), "10\n")?;
    fs::write(pool.join("nr_hugepages"), [0xff])?;
    fs::write(pool.join("surplus_hugepages"), "18446744073709551616\n")?;
    symlink(pool.join("free_hugepages"), pool.join("linked_value"))?;
    let snapshot = HugePageFiles::from_directory(root.path());
    assert_eq!(snapshot.parsed["hugepages-2048kB/free_hugepages"], ParsedMemoryValue::Unsigned(10));
    assert_eq!(snapshot.skipped["hugepages-2048kB/demote"], MemorySkipReason::WriteOnly);
    assert_eq!(snapshot.skipped["hugepages-2048kB/linked_value"], MemorySkipReason::NotRegularFile);
    assert!(matches!(snapshot.unavailable["hugepages-2048kB/nr_hugepages"], MemoryReadError::ReadFailed(_)));
    assert!(snapshot.unavailable.contains_key("hugepages-2048kB/surplus_hugepages"));
    Ok(())
}
