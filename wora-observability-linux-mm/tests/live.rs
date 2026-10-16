#![cfg(target_os = "linux")]
#![allow(clippy::unwrap_used)]

use wora_observability_linux_mm::{BuddyInfo, Current, PageTypeInfo, ProcError, SlabInfo, Swaps, ZoneInfo};

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
