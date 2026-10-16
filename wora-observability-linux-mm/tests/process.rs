#![cfg(target_os = "linux")]

use std::fs;

use wora_observability_linux_mm::{FromRead, MemoryReadError, ProcessMemoryStats, SmapsRollup};

const HEADER: &str = "1000-9000 ---p 00000000 00:00 0 [rollup]\n";

#[test]
fn retains_rollup_fields_units_unknown_values_and_checked_byte_conversion() -> Result<(), Box<dyn std::error::Error>> {
    let input = format!(
        "{HEADER}Rss: 4916 kB\nPss: 1478 kB\nPrivate_Dirty: 748 kB\nSwapPss: 0 kB\nLocked: 0 kB\nFuture: 18446744073709551615 kB\nFutureCounter: 9007199254740993\nFutureUnit: 7 widgets\n"
    );
    let rollup = SmapsRollup::from_read(input.as_bytes())?;
    assert_eq!(rollup.address_range, (4096, 36864));
    assert_eq!(rollup.values["Pss"].bytes(), Some(1_513_472));
    assert_eq!(rollup.values["Future"].value, u64::MAX);
    assert_eq!(rollup.values["Future"].bytes(), None);
    assert_eq!(rollup.values["FutureCounter"].unit, None);
    assert_eq!(rollup.values["FutureUnit"].bytes(), None);
    #[cfg(feature = "serde1")]
    {
        let json = serde_json::to_value(&rollup)?;
        assert_eq!(json["values"]["Future"]["value"], u64::MAX);
        assert_eq!(json["values"]["Pss"]["unit"], "kB");
    }
    Ok(())
}

#[test]
fn rejects_malformed_or_partial_rollups_without_panics() {
    for input in [
        "".into(),
        HEADER.into(),
        "Rss: 1 kB\nPss: 1 kB\n".into(),
        format!("{HEADER}Rss: 1 kB\nPss: -1 kB\n"),
        format!("{HEADER}Rss: 1 kB\nPss: 18446744073709551616 kB\n"),
        format!("{HEADER}Rss: 1 kB\nPss: 1 kB\nRss: 2 kB\n"),
        format!("{HEADER}Rss: 1 kB\nPss: 1 kB unexpected\n"),
        "9000-1000 ---p 00000000 00:00 0 [rollup]\nRss: 1 kB\nPss: 1 kB\n".into(),
    ] {
        assert!(SmapsRollup::from_read(input.as_bytes()).is_err(), "{input}");
    }
}

#[test]
fn process_identity_and_failures_remain_explicit() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let missing = ProcessMemoryStats::from_process_root(123, root.path());
    assert_eq!(missing.pid, 123);
    assert_eq!(missing.unavailable["smaps_rollup"], MemoryReadError::NotFound);
    fs::write(root.path().join("smaps_rollup"), format!("{HEADER}Rss: 4 kB\nPss: 2 kB\n"))?;
    let snapshot = ProcessMemoryStats::from_process_root(123, root.path());
    assert_eq!(snapshot.smaps_rollup.ok_or("rollup missing")?.values["Rss"].bytes(), Some(4096));
    fs::write(root.path().join("smaps_rollup"), "malformed")?;
    let failed = ProcessMemoryStats::from_process_root(123, root.path());
    assert!(failed.smaps_rollup.is_none());
    assert!(matches!(failed.unavailable["smaps_rollup"], MemoryReadError::ReadFailed(_)));
    Ok(())
}
