#![cfg(target_os = "linux")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{self, Read};

use wora_observability_linux_mm::{FromRead, ProcError, SwapType, Swaps};

const HEADER: &str = "Filename\tType\tSize\tUsed\tPriority\n";
const SYNTHETIC: &str = include_str!("fixtures/synthetic-swaps");

#[test]
fn recorded_header_only_tables_are_valid_empty_snapshots() {
    for input in [
        include_str!("fixtures/arm64-swaps-empty"),
        include_str!("fixtures/x86_64-swaps-empty"),
        "\n\t \nFilename Type Size Used Priority\r\n\n",
    ] {
        assert!(Swaps::from_read(input.as_bytes()).unwrap().entries.is_empty());
    }
}

#[test]
fn preserves_types_priorities_native_units_and_decodes_paths_once() {
    let info = Swaps::from_read(SYNTHETIC.replace('\n', "\r\n").as_bytes()).unwrap();
    assert_eq!(info.entries.len(), 3);
    let device = &info.entries[0];
    assert_eq!(device.path, "/dev/zram0");
    assert_eq!(device.swap_type, SwapType::Partition);
    assert_eq!(device.swap_type.as_str(), "partition");
    assert_eq!(device.size_kib, 1048576);
    assert_eq!(device.used_kib, 128);
    assert_eq!(device.priority, 100);
    let file = &info.entries[1];
    assert_eq!(file.path, "/swap file\\literal");
    assert_eq!(file.swap_type, SwapType::File);
    assert_eq!(file.size_kib, 9007199254741);
    assert_eq!(file.used_kib, 1234567);
    assert_eq!(file.priority, -2);
    assert_eq!(info.entries[2].swap_type, SwapType::Other("future_type".into()));
    assert_eq!(info.entries[2].swap_type.as_str(), "future_type");
    let input = format!(
        "{HEADER}/swap\\040tab\\011line\\012slash\\134040 file 18446744073709551615 9007199254740993 -2147483648\n/目录\u{a0}swap\rname partition 0 0 2147483647"
    );
    let info = Swaps::from_read(input.as_bytes()).unwrap();
    assert_eq!(info.entries[0].path, "/swap tab\tline\nslash\\040");
    assert_eq!(info.entries[0].size_kib, u64::MAX);
    assert_eq!(info.entries[0].used_kib, 9007199254740993);
    assert_eq!(info.entries[0].priority, i32::MIN);
    assert_eq!(info.entries[1].path, "/目录\u{a0}swap\rname");
    assert_eq!(info.entries[1].priority, i32::MAX);
}

#[test]
fn rejects_missing_headers_malformed_rows_and_duplicate_decoded_paths() {
    for input in ["", " \t\n", "Filename Type Size Used", "/swap file 1 0 -2"] {
        assert!(matches!(Swaps::from_read(input.as_bytes()), Err(ProcError::Other(_))));
    }
    for row in [
        "Filename Type Size Used Priority",
        "/swap file 1 0",
        "/swap file 1 0 -2 extra",
        "/swap file -1 0 -2",
        "/swap file +1 0 -2",
        "/swap file 1 -1 -2",
        "/swap file 1 NaN -2",
        "/swap file 18446744073709551616 0 -2",
        "/swap file 1 18446744073709551616 -2",
        "/swap file 1 0 +2",
        "/swap file 1 0 --2",
        "/swap file 1 0 -",
        "/swap file 1 0 2147483648",
        "/swap file 1 0 -2147483649",
        "/swap\\ file 1 0 -2",
        "/swap\\04 file 1 0 -2",
        "/swap\\08a file 1 0 -2",
        "/swap\\400 file 1 0 -2",
        "/swap\\000 file 1 0 -2",
        "/swap\0name file 1 0 -2",
        "/swap\\377 file 1 0 -2",
        "/swap file 1 0 -2\n/swap partition 2 0 -3",
        "/swap\\040file file 1 0 -2\n/swap\\040file file 2 0 -3",
        "/swap file 1 0 -2\n/\\163wap file 2 0 -3",
    ] {
        let input = format!("{HEADER}{row}\n");
        assert!(matches!(Swaps::from_read(input.as_bytes()), Err(ProcError::Other(_))), "accepted {row:?}");
    }
    let error = Swaps::from_read(format!("{HEADER}/swap file 1 0 -2\nbroken\n").as_bytes()).unwrap_err();
    assert!(error.to_string().contains("swaps line 3"));
}

#[test]
fn propagates_io_failures_and_reads_alternate_paths_without_opening_backing_files() {
    struct FailedRead;
    impl Read for FailedRead {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::ErrorKind::PermissionDenied.into())
        }
    }
    assert!(matches!(
        Swaps::from_read(SYNTHETIC.as_bytes().chain(FailedRead)),
        Err(ProcError::PermissionDenied(_))
    ));
    assert!(
        Swaps::from_read(
            format!("{HEADER}/swap")
                .bytes()
                .chain([255])
                .chain(b" file 1 0 -2\n".iter().copied())
                .collect::<Vec<_>>()
                .as_slice()
        )
        .is_err()
    );
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("swaps");
    assert!(matches!(Swaps::from_file(&path), Err(ProcError::NotFound(Some(actual))) if actual == path));
    std::fs::write(&path, SYNTHETIC).unwrap();
    assert_eq!(Swaps::from_file(&path).unwrap(), Swaps::from_read(SYNTHETIC.as_bytes()).unwrap());
}

#[cfg(feature = "serde1")]
#[test]
fn serde_round_trip_preserves_paths_types_and_precise_integers() {
    let input = format!("{SYNTHETIC}/huge partition 18446744073709551615 9007199254740993 -2147483648\n");
    let info = Swaps::from_read(input.as_bytes()).unwrap();
    let json = serde_json::to_string(&info).unwrap();
    assert!(json.contains("18446744073709551615"));
    assert!(json.contains("9007199254740993"));
    assert_eq!(serde_json::from_str::<Swaps>(&json).unwrap(), info);
}
