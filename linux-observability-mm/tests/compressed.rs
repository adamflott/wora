#![cfg(target_os = "linux")]
use std::{fs, os::unix::fs::symlink};
use wora_observability_linux_mm::{CompressedMemoryStats, MemoryReadError, ParsedMemoryValue, ZramMemoryStat};

#[test]
fn zswap_failures_and_incompressible_pages_preserve_units_and_precision() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let source = root.path().join("kernel/debug/zswap");
    fs::create_dir_all(&source)?;
    for (name, value) in [
        ("reject_compress_fail", "0"),
        ("decompress_fail", "7"),
        ("stored_incompressible_pages", "18446744073709551615"),
    ] {
        fs::write(source.join(name), value)?;
    }
    let snapshot = CompressedMemoryStats::from_sys_root(root.path());
    assert_eq!(snapshot.zswap.parsed["reject_compress_fail"], ParsedMemoryValue::Unsigned(0));
    assert_eq!(snapshot.zswap.parsed["decompress_fail"], ParsedMemoryValue::Unsigned(7));
    assert_eq!(snapshot.zswap.parsed["stored_incompressible_pages"], ParsedMemoryValue::Unsigned(u64::MAX));
    assert!(matches!(snapshot.zswap.unavailable.get("stored_pages"), Some(MemoryReadError::NotFound)));
    Ok(())
}

#[test]
fn absent_zswap_counters_are_errors_and_disappearing_counters_do_not_stay_cached() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let source = root.path().join("kernel/debug/zswap");
    fs::create_dir_all(&source)?;
    fs::write(source.join("stored_pages"), "23")?;
    fs::write(source.join("decompress_fail"), "4")?;
    let before = CompressedMemoryStats::from_sys_root(root.path());
    assert_eq!(before.zswap.parsed["decompress_fail"], ParsedMemoryValue::Unsigned(4));
    fs::remove_file(source.join("decompress_fail"))?;
    let after = CompressedMemoryStats::from_sys_root(root.path());
    for name in ["reject_compress_fail", "decompress_fail", "stored_incompressible_pages"] {
        assert!(!after.zswap.parsed.contains_key(name));
        assert!(matches!(after.zswap.unavailable.get(name), Some(MemoryReadError::NotFound)));
    }
    assert_eq!(after.zswap.parsed["stored_pages"], ParsedMemoryValue::Unsigned(23));
    Ok(())
}

#[test]
fn device_links_native_tables_and_future_fields_are_preserved() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    fs::create_dir_all(root.path().join("block"))?;
    fs::create_dir_all(root.path().join("devices/virtual/block/zram9"))?;
    fs::write(
        root.path().join("devices/virtual/block/zram9/mm_stat"),
        "100 50 80 0 90 2 3 4 5 18446744073709551615\n",
    )?;
    fs::write(root.path().join("devices/virtual/block/zram9/reset"), [0xff])?;
    symlink("../devices/virtual/block/zram9", root.path().join("block/zram9"))?;
    symlink("/tmp", root.path().join("block/zram10"))?;
    fs::create_dir_all(root.path().join("kernel/debug/zswap"))?;
    fs::write(root.path().join("kernel/debug/zswap/stored_pages"), "9007199254740993")?;
    let snapshot = CompressedMemoryStats::from_sys_root(root.path());
    assert_eq!(snapshot.zram[&9].memory.as_ref().ok_or("no table")?.fields["mem_used_total"], 80);
    assert_eq!(snapshot.zram[&9].memory.as_ref().ok_or("no table")?.extra, [u64::MAX]);
    assert!(!snapshot.zram[&9].files.unavailable.contains_key("reset"));
    assert!(snapshot.unavailable.contains_key("zram10"));
    assert_eq!(snapshot.zswap.parsed["stored_pages"], ParsedMemoryValue::Unsigned(9_007_199_254_740_993));
    for malformed in ["1 2", "1 2 3 4 5 6 -1", "1 2 3 4 5 6 18446744073709551616"] {
        assert!(ZramMemoryStat::parse(malformed).is_err());
    }
    Ok(())
}
