#![cfg(target_os = "linux")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{self, Read};

use wora_observability_linux_mm::{FromRead, ProcError, ZoneInfo};

const SYNTHETIC: &str = include_str!("fixtures/synthetic-zoneinfo");

#[test]
fn parses_recorded_arm64_and_x86_64_including_unpopulated_zones() {
    for (input, populated, high, stack) in [
        (include_str!("fixtures/arm64-zoneinfo"), 2, 9692, 1912),
        (include_str!("fixtures/x86_64-zoneinfo"), 3, 97, 2272),
    ] {
        let info = ZoneInfo::from_read(input.as_bytes()).unwrap();
        assert_eq!(info.nodes.len(), 1);
        assert_eq!(info.nodes[&0]["nr_kernel_stack"], stack);
        assert_eq!(info.zones.len(), 5);
        assert_eq!(info.zones[0].watermarks.high_pages, high);
        assert_eq!(info.zones[0].pagesets.len(), 4);
        assert!(info.zones.iter().all(|z| !z.stats.contains_key("nr_kernel_stack")));
        assert_eq!(info.zones.iter().filter(|z| z.present_pages > 0).count(), populated);
        for zone in info.zones.iter().filter(|z| z.present_pages == 0) {
            assert!(zone.stats.is_empty());
            assert!(zone.pagesets.is_empty());
            assert_eq!(zone.protection, [0; 5]);
        }
    }
}

#[test]
fn preserves_sparse_topology_optional_fields_new_fields_and_precise_integers() {
    let info = ZoneInfo::from_read(SYNTHETIC.replace('\n', "\r\n").as_bytes()).unwrap();
    assert_eq!(info.nodes.keys().copied().collect::<Vec<_>>(), [2, 99]);
    assert_eq!(info.nodes[&2]["nr_written"], u64::MAX);
    let zone = &info.zones[0];
    assert_eq!(zone.node, 2);
    assert_eq!(zone.zone, "FutureZone");
    assert_eq!(zone.free_pages, 9007199254740993);
    assert_eq!(zone.watermarks.high_pages, 30);
    assert_eq!(zone.watermarks.boost_pages, Some(8));
    assert_eq!(zone.watermarks.promo_pages, Some(40));
    assert_eq!(zone.cma_pages, Some(16));
    assert_eq!(zone.protection, [0, 2, u64::MAX]);
    assert_eq!(zone.stats["future_zone_stat"], 7);
    assert_eq!(zone.stats["reserved_highatomic"], 8);
    assert_eq!(zone.pagesets[0].cpu, 7);
    assert_eq!(zone.pagesets[0].stats["high"], 500);
    assert_eq!(zone.pagesets[0].stats["high_max"], 600);
    assert_eq!(zone.pagesets[0].stats["vm stats threshold"], 12);
    assert_eq!(info.zones[1].watermarks.boost_pages, None);
    assert_eq!(info.zones[1].watermarks.promo_pages, None);
    assert_eq!(info.zones[1].cma_pages, None);
    // An older layout may print node statistics at the end of a zone.
    let old =
        "Node 5, zone Normal\n pages free 1\n min 2\n low 3\n high 4\n spanned 8\n present 8\n managed 6\n protection: (0)\n per-node stats\n nr_written 9\n";
    let old = ZoneInfo::from_read(old.as_bytes()).unwrap();
    assert_eq!(old.nodes[&5]["nr_written"], 9);
    assert!(old.zones[0].stats.is_empty());
    // Linux 4.19-style pagesets, including UP without a VM-stat threshold.
    let older = SYNTHETIC
        .replace("        boost 8\n", "")
        .replace("        promo 40\n", "")
        .replace("        cma 16\n", "")
        .replace("              high_min: 100\n", "")
        .replace("              high_max: 600\n", "")
        .replace("  vm stats threshold: 12\n", "");
    let older = ZoneInfo::from_read(older.as_bytes()).unwrap();
    assert_eq!(older.zones[0].watermarks.high_pages, 30);
    assert_eq!(older.zones[0].pagesets[0].stats.len(), 3);
}

#[test]
fn rejects_invalid_and_partial_inputs_with_line_context() {
    let base = SYNTHETIC.split("Node 2, zone Device").next().unwrap();
    let mut cases = vec![String::new(), " \n\t".into(), "min 1".into(), "Node 0, zone Normal\n".into()];
    for (from, to) in [
        ("Node 2,", "Node 2"),
        ("Node 2,", "Node -2,"),
        ("Node 2,", "Node 4294967296,"),
        ("zone FutureZone", "zone FutureZone extra"),
        ("pages free 9007199254740993", "pages free -1"),
        ("min 10", "min +10"),
        ("min 10", "min 18446744073709551616"),
        ("min 10", "min 10\n min 11"),
        ("min 10", "min 10 trailing"),
        ("        managed 800\n", ""),
        ("protection: (0, 2, 18446744073709551615)", "protection: ()"),
        ("protection: (0, 2, 18446744073709551615)", "protection: (0, 2,)"),
        ("protection: (0, 2, 18446744073709551615)", "protection: (0, 2"),
        ("cpu: 7", "cpu: 4294967296"),
        ("cpu: 7", "cpu: 7\n    cpu: 7"),
        ("              batch: 3\n", ""),
        ("              batch: 3", "              batch: 3\n              batch: 4"),
        ("  pagesets\n", ""),
        ("      nr_kernel_stack 4096\n", "      nr_dirtied 4\n"),
    ] {
        cases.push(base.replace(from, to));
    }
    cases.push(format!("{base}{base}"));
    cases.push(format!("{base}Node 2, zone Device\n per-node stats\n nr_written 3\n"));
    cases.push(base.replace("  per-node stats\n", "  per-node stats\n  per-node stats\n"));
    cases.push("Node 0, zone DMA\n per-node stats\n pages free 1\n min 1\n low 1\n high 1\n spanned 1\n present 1\n managed 1\n protection: (0)\n".into());
    for input in cases {
        let error = ZoneInfo::from_read(input.as_bytes()).unwrap_err();
        assert!(matches!(error, ProcError::Other(_)), "{error}");
        assert!(error.to_string().contains("zoneinfo"));
    }
    let error = ZoneInfo::from_read(format!("{base}broken\n").as_bytes()).unwrap_err();
    assert!(error.to_string().contains("line "));
}

#[test]
fn propagates_read_failures_and_alternate_path_errors() {
    struct FailedRead;
    impl Read for FailedRead {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::ErrorKind::PermissionDenied.into())
        }
    }
    assert!(matches!(
        ZoneInfo::from_read(SYNTHETIC.as_bytes().chain(FailedRead)),
        Err(ProcError::PermissionDenied(_))
    ));
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("zoneinfo");
    assert!(matches!(ZoneInfo::from_file(&path), Err(ProcError::NotFound(Some(actual))) if actual == path));
    std::fs::write(&path, SYNTHETIC).unwrap();
    assert_eq!(ZoneInfo::from_file(path).unwrap(), ZoneInfo::from_read(SYNTHETIC.as_bytes()).unwrap());
}

#[cfg(feature = "serde1")]
#[test]
fn serde_round_trip_retains_all_scopes_and_integer_precision() {
    let info = ZoneInfo::from_read(SYNTHETIC.as_bytes()).unwrap();
    let json = serde_json::to_string(&info).unwrap();
    assert!(json.contains("9007199254740993"));
    assert!(json.contains("18446744073709551615"));
    assert_eq!(serde_json::from_str::<ZoneInfo>(&json).unwrap(), info);
}
