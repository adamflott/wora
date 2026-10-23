#![cfg(target_os = "linux")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{self, Read};

use wora_observability_linux_mm::{FreeBlockCount as Count, FromRead, PageTypeInfo, ProcError};

const INPUT: &str = include_str!("fixtures/synthetic-pagetypeinfo");

#[test]
fn parses_recorded_arm64_and_x86_64_free_lists_and_pageblock_tables() {
    for (input, zones, cma) in [
        (include_str!("fixtures/arm64-pagetypeinfo"), 2, true),
        (include_str!("fixtures/x86_64-pagetypeinfo"), 3, false),
    ] {
        let info = PageTypeInfo::from_read(input.as_bytes()).unwrap();
        assert_eq!(info.nodes.len(), 1);
        let node = &info.nodes[0];
        assert_eq!(node.node, 0);
        assert_eq!((node.page_block_order, node.pages_per_block), (9, 512));
        assert_eq!(node.orders, (0..11).collect::<Vec<_>>());
        let mut expected = vec!["Unmovable", "Movable", "Reclaimable", "HighAtomic", "Isolate"];
        if cma {
            expected.insert(4, "CMA");
        }
        assert_eq!(node.migrate_types, expected);
        assert_eq!(node.free_blocks.len(), zones * expected.len());
        assert_eq!(node.page_blocks.len(), zones);
        assert!(node.mixed_blocks.is_none());
        assert!(node.free_blocks.iter().all(|row| row.counts.len() == 11));
        assert!(node.page_blocks.iter().all(|row| row.counts.len() == expected.len()));
    }
}

#[test]
fn preserves_sparse_nodes_unknown_names_bounds_optional_mixed_counts_and_header_order() {
    let info = PageTypeInfo::from_read(INPUT.replace('\n', "\r\n").as_bytes()).unwrap();
    assert_eq!(info.nodes.iter().map(|n| n.node).collect::<Vec<_>>(), [2, 99]);
    let node = &info.nodes[0];
    assert_eq!(node.orders, [0, 1, 2]);
    assert_eq!(node.free_blocks[0].zone, "FutureZone");
    assert_eq!(
        node.free_blocks[0].counts,
        [Count::Exact(9007199254740993), Count::AtLeast(100000), Count::Exact(0)]
    );
    assert_eq!(node.free_blocks[1].migrate_type, "FutureType");
    assert_eq!(node.free_blocks[1].counts, [Count::AtLeast(7), Count::Exact(2), Count::Exact(u64::MAX)]);
    assert_eq!(node.page_blocks[0].counts["Movable"], u64::MAX);
    assert_eq!(node.page_blocks[0].counts["FutureType"], 3);
    let mixed = node.mixed_blocks.as_ref().unwrap();
    assert_eq!(mixed[0].counts["Movable"], 1);
    assert_eq!(mixed[0].counts["FutureType"], 2);
    assert_eq!(info.nodes[1].orders, [0]);
    assert!(info.nodes[1].mixed_blocks.is_none());
    // A cap at u64::MAX is retained directly, without overflowing by adding one.
    let largest = INPUT.replace(">100000", ">18446744073709551615");
    assert_eq!(
        PageTypeInfo::from_read(largest.as_bytes()).unwrap().nodes[0].free_blocks[0].counts[1],
        Count::AtLeast(u64::MAX)
    );
}

#[test]
fn rejects_incomplete_tables_bad_metadata_duplicates_and_invalid_counts_without_partial_success() {
    let mut cases = vec![String::new(), " \n\t".into(), "Page block order: 9\n".into(), "Pages per block: 512\n".into()];
    for (from, to) in [
        ("Page block order: 2", "Page block order: 64"),
        ("Page block order: 2", "Page block order: 4294967296"),
        ("Pages per block: 4", "Pages per block: 3"),
        ("Pages per block: 4", "Pages per block: -4"),
        ("at order 0 1 2", "at order 0 2 3"),
        ("at order 0 1 2", "at order 0 0 1"),
        ("at order 0 1 2", "at order"),
        ("Node 2,", "Node 2"),
        ("Node 2,", "Node -2,"),
        ("Node 2,", "Node 4294967296,"),
        ("zone FutureZone, type Movable", "zone FutureZone type Movable"),
        ("type Movable 9007199254740993", "type Movable -1"),
        ("type Movable 9007199254740993", "type Movable +1"),
        ("type Movable 9007199254740993", "type Movable 18446744073709551616"),
        (">100000", ">=100000"),
        (">100000", ">>100000"),
        (">100000", ">18446744073709551616"),
        ("> 7 2 18446744073709551615", ">"),
        ("> 7 2 18446744073709551615", "7 2"),
        ("Number of blocks type FutureType Movable", "Number of blocks type FutureType FutureType"),
        ("Number of blocks type FutureType Movable", "Number of blocks type"),
        ("Number of blocks type FutureType Movable", "Number of blocks type FutureType OtherType"),
        (
            "Node 2, zone FutureZone 3 18446744073709551615",
            "Node 2, zone FutureZone >3 18446744073709551615",
        ),
        ("Node 2, zone FutureZone 3 18446744073709551615", "Node 2, zone FutureZone 3"),
        (
            "Node 2, zone FutureZone 3 18446744073709551615",
            "Node 3, zone FutureZone 3 18446744073709551615",
        ),
        ("Number of mixed blocks Movable FutureType", "Number of mixed blocks Movable OtherType"),
        ("Node 2, zone FutureZone 1 2", "Node 2, zone FutureZone 1 2 3"),
        ("Node 2, zone Normal 0 0\n", ""),
        ("Node 2, zone Normal, type FutureType 0 0 0\n", ""),
        ("Node 2, zone Normal 0 7\n", ""),
    ] {
        cases.push(INPUT.replace(from, to));
    }
    cases.push(format!("{INPUT}{INPUT}"));
    let row = "Node 2, zone Normal, type FutureType 0 0 0\n";
    cases.push(INPUT.replace(row, &format!("{row}{row}")));
    let row = "Node 2, zone Normal 0 7\n";
    cases.push(INPUT.replace(row, &format!("{row}{row}")));
    cases.push(INPUT.split("Number of blocks type").next().unwrap().into());
    cases.push(format!(
        "{}Number of mixed blocks Movable FutureType\n",
        INPUT.split("Number of mixed blocks").next().unwrap()
    ));
    for input in cases {
        assert!(
            matches!(PageTypeInfo::from_read(input.as_bytes()), Err(ProcError::Other(_))),
            "accepted {input:?}"
        );
    }
    let error = PageTypeInfo::from_read(format!("{INPUT}broken\n").as_bytes()).unwrap_err();
    assert!(error.to_string().contains("pagetypeinfo line "));
}

#[test]
fn reads_alternate_paths_and_propagates_io_errors() {
    struct FailedRead;
    impl Read for FailedRead {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::ErrorKind::PermissionDenied.into())
        }
    }
    assert!(matches!(
        PageTypeInfo::from_read(INPUT.as_bytes().chain(FailedRead)),
        Err(ProcError::PermissionDenied(_))
    ));
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("pagetypeinfo");
    assert!(matches!(PageTypeInfo::from_file(&path), Err(ProcError::NotFound(Some(actual))) if actual == path));
    std::fs::write(&path, INPUT).unwrap();
    assert_eq!(PageTypeInfo::from_file(&path).unwrap(), PageTypeInfo::from_read(INPUT.as_bytes()).unwrap());
}

#[cfg(feature = "serde1")]
#[test]
fn serde_round_trip_retains_bounds_integer_precision_and_optional_sections() {
    let info = PageTypeInfo::from_read(INPUT.as_bytes()).unwrap();
    let json = serde_json::to_string(&info).unwrap();
    assert!(json.contains("18446744073709551615"));
    assert!(json.contains("9007199254740993"));
    assert!(json.contains("\"at_least\":100000"));
    assert_eq!(serde_json::from_str::<PageTypeInfo>(&json).unwrap(), info);
}
