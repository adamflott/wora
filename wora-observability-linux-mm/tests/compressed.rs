#![cfg(target_os = "linux")]
use std::{fs, os::unix::fs::symlink};
use wora_observability_linux_mm::{CompressedMemoryStats, ParsedMemoryValue, ZramMemoryStat};

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
