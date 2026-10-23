#![cfg(target_os = "linux")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{self, Read};

use wora_observability_linux_mm::{BuddyInfo, FromRead, ProcError};

#[test]
fn parses_recorded_arm64_and_x86_64_tables() {
    let arm = BuddyInfo::from_read(include_bytes!("fixtures/arm64-buddyinfo").as_slice()).unwrap();
    assert_eq!(arm.zones.len(), 2);
    assert_eq!(arm.zones[0].node, 0);
    assert_eq!(arm.zones[0].zone, "DMA");
    assert_eq!(arm.zones[0].free_blocks, [2553, 2202, 767, 304, 141, 55, 37, 28, 13, 153, 112]);
    assert_eq!(arm.zones[1].zone, "Normal");
    assert_eq!(arm.zones[1].free_blocks[10], 3);

    let x86 = BuddyInfo::from_read(include_bytes!("fixtures/x86_64-buddyinfo").as_slice()).unwrap();
    assert_eq!(x86.zones.len(), 3);
    assert_eq!(x86.zones[1].zone, "DMA32");
    assert_eq!(x86.zones[1].free_blocks[10], 483);
    assert_eq!(x86.zones[2].free_blocks, [541, 178, 22, 12, 3, 3, 3, 5, 2, 2, 396]);
}

#[test]
fn supports_sparse_nodes_unknown_zones_whitespace_and_varied_order_counts() {
    let info =
        BuddyInfo::from_read(b"\nNode\t2,\tzone\tFutureZone\t9007199254740993\t0\t18446744073709551615\r\n\nNode 99, zone Movable 0 4 2".as_slice()).unwrap();
    assert_eq!(info.zones[0].node, 2);
    assert_eq!(info.zones[0].zone, "FutureZone");
    assert_eq!(info.zones[0].free_blocks, [9007199254740993, 0, u64::MAX]);
    assert_eq!(info.zones[1].node, 99);
    assert_eq!(info.zones[1].free_blocks, [0, 4, 2]);
    let more = format!("Node 4294967295, zone Normal {}", vec!["1"; 16].join(" "));
    let info = BuddyInfo::from_read(more.as_bytes()).unwrap();
    assert_eq!(info.zones[0].node, u32::MAX);
    assert_eq!(info.zones[0].free_blocks.len(), 16);
    assert!(BuddyInfo::from_read(b"Node 0, zone DMA 0\n".as_slice()).is_ok());
}

#[test]
fn rejects_invalid_or_partial_tables_without_panicking() {
    for input in [
        "",
        " \n\t",
        "header",
        "node 0, zone DMA 1",
        "Node 0 zone DMA 1",
        "Node , zone DMA 1",
        "Node -1, zone DMA 1",
        "Node +1, zone DMA 1",
        "Node 4294967296, zone DMA 1",
        "Node 0, zones DMA 1",
        "Node 0, zone",
        "Node 0, zone DMA",
        "Node 0, zone DMA -1",
        "Node 0, zone DMA +1",
        "Node 0, zone DMA NaN",
        "Node 0, zone DMA 18446744073709551616",
        "Node 0, zone DMA 1\nNode 0, zone DMA 2\n",
        "Node 0, zone DMA 1 2 3\nNode 1, zone Normal 1 2\n",
    ] {
        assert!(matches!(BuddyInfo::from_read(input.as_bytes()), Err(ProcError::Other(_))), "accepted {input:?}");
    }
    let error = BuddyInfo::from_read(b"\nNode 0, zone DMA 1\nNode 0, zone Normal bad\n".as_slice()).unwrap_err();
    assert!(error.to_string().contains("line 3"));
}

#[test]
fn propagates_io_failures_after_valid_rows() {
    struct FailedRead;
    impl Read for FailedRead {
        fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::ErrorKind::PermissionDenied.into())
        }
    }
    let input = b"Node 0, zone Normal 1\n".as_slice().chain(FailedRead);
    assert!(matches!(BuddyInfo::from_read(input), Err(ProcError::PermissionDenied(_))));
}

#[test]
fn reads_alternate_paths_and_preserves_not_found_context() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("buddyinfo");
    match BuddyInfo::from_file(&path) {
        Err(ProcError::NotFound(Some(actual))) => assert_eq!(actual, path),
        other => panic!("expected not-found path, got {other:?}"),
    }
    std::fs::write(&path, "Node 0, zone Normal 0 2\n").unwrap();
    assert_eq!(BuddyInfo::from_file(&path).unwrap().zones[0].free_blocks, [0, 2]);
}

#[cfg(feature = "serde1")]
#[test]
fn serde_round_trip_retains_integer_precision() {
    let info = BuddyInfo::from_read(b"Node 2, zone Normal 9007199254740993 18446744073709551615\n".as_slice()).unwrap();
    let json = serde_json::to_string(&info).unwrap();
    assert!(json.contains("9007199254740993"));
    assert!(json.contains("18446744073709551615"));
    let restored: BuddyInfo = serde_json::from_str(&json).unwrap();
    assert_eq!(restored, info);
}
