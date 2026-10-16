#![cfg(target_os = "linux")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{self, Read};

use wora_observability_linux_mm::{FromRead, ProcError, SlabInfo};

const INPUT: &str = include_str!("fixtures/synthetic-slabinfo");
const STATS: &str = include_str!("fixtures/slabinfo-statistics");

#[test]
fn parses_recorded_arm64_and_x86_64_tables_without_assuming_cache_names() {
    for input in [include_str!("fixtures/arm64-slabinfo"), include_str!("fixtures/x86_64-slabinfo")] {
        let info = SlabInfo::from_read(input.as_bytes()).unwrap();
        assert_eq!(info.caches.len(), input.lines().count() - 2);
        assert!(info.caches.len() > 150);
        let cache = info.caches.iter().find(|c| c.name == "kmalloc-64").unwrap();
        assert_eq!(cache.object_size_bytes, 64);
        assert_eq!(cache.objects_per_slab, 64);
        assert_eq!(cache.pages_per_slab, 1);
        assert!(info.caches.iter().all(|c| c.global_stats.is_none() && c.cpu_stats.is_none()));
    }
}

#[test]
fn preserves_native_fields_large_integers_legacy_statistics_and_empty_tables() {
    let input = INPUT.replace(' ', "\t").replace('\n', "\r\n");
    let info = SlabInfo::from_read(input.as_bytes()).unwrap();
    assert_eq!(info.caches.len(), 3);
    assert_eq!(info.caches[0].active_objects, 0);
    let cache = &info.caches[1];
    assert_eq!(cache.active_objects, 9007199254740993);
    assert_eq!(cache.objects, u64::MAX);
    assert_eq!(cache.object_size_bytes, 96);
    assert_eq!(cache.objects_per_slab, 42);
    assert_eq!(cache.pages_per_slab, 1);
    assert_eq!((cache.limit, cache.batch_count, cache.shared_factor), (120, 60, 8));
    assert_eq!((cache.active_slabs, cache.slabs, cache.shared_available_objects), (10, 20, 3));
    assert_eq!(info.caches[2].name, "future:cache");
    let legacy = SlabInfo::from_read(STATS.as_bytes()).unwrap();
    let global = legacy.caches[0].global_stats.as_ref().unwrap();
    assert_eq!(global.list_allocations, 9007199254740993);
    assert_eq!(global.max_objects, u64::MAX);
    assert_eq!((global.grown, global.reaped, global.errors, global.max_freeable), (2, 3, 4, 5));
    assert_eq!((global.node_allocations, global.remote_frees, global.alien_overflow), (6, 7, 8));
    let cpu = legacy.caches[0].cpu_stats.as_ref().unwrap();
    assert_eq!((cpu.allocation_hits, cpu.allocation_misses, cpu.free_hits, cpu.free_misses), (9, 10, 11, 12));
    for input in [INPUT, STATS] {
        let header = input.lines().take(2).collect::<Vec<_>>().join("\n");
        assert!(SlabInfo::from_read(header.as_bytes()).unwrap().caches.is_empty());
    }
}

#[test]
fn rejects_unsupported_versions_bad_sections_duplicate_caches_and_incomplete_tables() {
    let mut invalid = vec!["".into(), " \n\t".into(), "slabinfo - version: 2.1\n".into()];
    for (from, to) in [
        ("version: 2.1", "version: 1.1"),
        ("version: 2.1", "version: 2.0"),
        ("version: 2.1", "version: 3.0"),
        ("version: 2.1", "version: 2.1 (other)"),
        ("<num_objs>", "<unknown>"),
        ("empty_cache 0 0 64", "empty_cache -1 0 64"),
        ("empty_cache 0 0 64", "empty_cache 0 +1 64"),
        ("empty_cache 0 0 64", "empty_cache 0 0 NaN"),
        ("empty_cache 0 0 64", "empty_cache 0 18446744073709551616 64"),
        (" : tunables 0", " : bad_section 0"),
        (" : tunables 0 0 0", " : tunables 0 0"),
        (" : tunables 0 0 0", " : tunables 0 0 -1"),
        (" : slabdata 0 0 0", " : slabdata 0 0 invalid"),
        (" : slabdata 0 0 0", " : slabdata 0 0 0 extra"),
        ("future:cache", "large_cache"),
    ] {
        invalid.push(INPUT.replace(from, to));
    }
    for (from, to) in [
        (" (statistics)", ""),
        (" : globalstat 9007199254740993", " : wrongstat 9007199254740993"),
        (" : globalstat 9007199254740993", " : globalstat -1"),
        (" : cpustat 9 10 11 12", " : cpustat 9 10 11"),
        (" : cpustat 9 10 11 12", " : cpustat 9 10 11 12 13"),
        (" : cpustat 9 10 11 12", " : cpustat 9 10 11 -1"),
    ] {
        invalid.push(STATS.replace(from, to));
    }
    for input in invalid {
        assert!(matches!(SlabInfo::from_read(input.as_bytes()), Err(ProcError::Other(_))), "accepted {input:?}");
    }
    let error = SlabInfo::from_read(format!("{INPUT}broken\n").as_bytes()).unwrap_err();
    assert!(error.to_string().contains("slabinfo line 6"));
}

#[test]
fn propagates_io_errors_and_reads_alternate_files_with_path_context() {
    struct FailedRead;
    impl Read for FailedRead {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::ErrorKind::PermissionDenied.into())
        }
    }
    assert!(matches!(
        SlabInfo::from_read(INPUT.as_bytes().chain(FailedRead)),
        Err(ProcError::PermissionDenied(_))
    ));
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("slabinfo");
    assert!(matches!(SlabInfo::from_file(&path), Err(ProcError::NotFound(Some(actual))) if actual == path));
    std::fs::write(&path, STATS).unwrap();
    assert_eq!(SlabInfo::from_file(&path).unwrap(), SlabInfo::from_read(STATS.as_bytes()).unwrap());
}

#[cfg(feature = "serde1")]
#[test]
fn serde_round_trip_preserves_plain_and_statistics_tables_with_precise_integers() {
    for input in [INPUT, STATS] {
        let info = SlabInfo::from_read(input.as_bytes()).unwrap();
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("18446744073709551615"));
        assert!(json.contains("9007199254740993"));
        assert_eq!(serde_json::from_str::<SlabInfo>(&json).unwrap(), info);
    }
}
