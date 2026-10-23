#![cfg(target_os = "linux")]
#![allow(clippy::unwrap_used)]

use wora_observability_linux_mm::{
    BuddyInfo, CgroupMemoryStats, Current, HugePageStats, MemorySysctls, PageTypeInfo, ProcError, ProcessMemoryStats, SlabInfo, Swaps, SysctlSkipReason,
    ZoneInfo,
};

#[test]
#[ignore = "requires a running Linux kernel with readable /proc/buddyinfo"]
fn reads_live_buddyinfo_through_procfs_current() {
    let info = BuddyInfo::current().unwrap();
    assert!(!info.zones.is_empty());
    assert!(info.zones.iter().all(|zone| !zone.zone.is_empty() && !zone.free_blocks.is_empty()));
}

#[test]
#[ignore = "requires a running Linux kernel with readable /proc/zoneinfo"]
fn reads_live_zoneinfo_through_procfs_current() {
    let info = ZoneInfo::current().unwrap();
    assert!(!info.zones.is_empty());
    assert!(!info.nodes.is_empty());
    assert!(info.zones.iter().all(|zone| !zone.zone.is_empty() && !zone.protection.is_empty()));
}

#[test]
#[ignore = "requires a running Linux kernel with readable /proc/swaps"]
fn reads_live_swaps_through_procfs_current() {
    let info = Swaps::current().unwrap();
    // No active swap is valid; live tests never enable swap themselves.
    assert!(info.entries.iter().all(|entry| !entry.path.is_empty()));
}

#[test]
#[ignore = "requires Linux /proc/slabinfo; permission denial is an expected unprivileged result"]
fn reads_live_slabinfo_or_reports_permission_denied() {
    match SlabInfo::current() {
        Ok(info) => assert!(!info.caches.is_empty()),
        Err(ProcError::PermissionDenied(_)) => {}
        other => panic!("unexpected slabinfo result: {other:?}"),
    }
}

#[test]
#[ignore = "requires Linux /proc/pagetypeinfo; potentially expensive; permission denial is expected without root"]
fn reads_live_pagetypeinfo_or_reports_permission_denied() {
    match PageTypeInfo::current() {
        Ok(info) => assert!(!info.nodes.is_empty()),
        Err(ProcError::PermissionDenied(_)) => {}
        other => panic!("unexpected pagetypeinfo result: {other:?}"),
    }
}

#[test]
#[ignore = "requires Linux /proc/sys; reads settings as the current user and skips action controls"]
fn reads_live_memory_sysctls_without_opening_action_controls() {
    let snapshot = MemorySysctls::current();
    assert!(snapshot.parsed.contains_key("vm.swappiness"));
    assert!(!snapshot.parsed.contains_key("vm.stat_refresh"));
    if std::path::Path::new("/proc/sys/vm/stat_refresh").exists() {
        assert_eq!(snapshot.skipped["vm.stat_refresh"], SysctlSkipReason::ReadSideEffect);
    }
    for name in ["vm.compact_memory", "vm.drop_caches"] {
        assert!(!snapshot.parsed.contains_key(name));
    }
}

#[test]
#[ignore = "requires a visible cgroup v2 mount and readable process membership"]
fn reads_live_cgroup_v2_memory() {
    let snapshot = CgroupMemoryStats::current();
    assert!(snapshot.unavailable.is_empty(), "{:?}", snapshot.unavailable);
    let current = snapshot.current.unwrap();
    assert!(current.directory.is_absolute());
    assert!(!current.parsed.contains_key("memory.reclaim"));
}

#[test]
#[ignore = "requires readable Linux THP/HugeTLB sysfs interfaces"]
fn reads_live_hugepages_without_demotion_actions() {
    let snapshot = HugePageStats::current();
    assert!(!snapshot.transparent.parsed.is_empty());
    assert!(!snapshot.hugetlb.parsed.is_empty());
    assert!(!snapshot.hugetlb.parsed.keys().any(|path| path.ends_with("/demote")));
}

#[test]
#[ignore = "requires /proc/self/smaps_rollup; walks the current process page tables"]
fn reads_live_process_rollup() {
    let snapshot = ProcessMemoryStats::current();
    assert_eq!(snapshot.pid, std::process::id());
    let rollup = snapshot.smaps_rollup.unwrap();
    assert!(rollup.values["Rss"].bytes().is_some());
    assert!(rollup.values["Pss"].bytes().is_some());
}
