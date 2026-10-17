#![cfg(target_os = "linux")]
use std::{fs, os::unix::fs::symlink};
use wora_observability_linux_mm::{ParsedMemoryValue, SlubStats};

#[test]
fn cache_aliases_do_not_duplicate_accounting_and_actions_are_excluded() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let cache = root.path().join("kernel/slab/:0000040");
    fs::create_dir_all(&cache)?;
    fs::write(cache.join("objects"), "100 N0=25 N9=75\n")?;
    fs::write(cache.join("alloc_fastpath"), "18446744073709551615\n")?;
    fs::write(cache.join("validate"), [0xff])?;
    fs::write(cache.join("shrink"), [0xff])?;
    symlink(":0000040", root.path().join("kernel/slab/kmalloc-64"))?;
    symlink("/tmp", root.path().join("kernel/slab/escape"))?;
    let result = SlubStats::from_sys_root(root.path());
    assert_eq!(result.caches.len(), 1);
    assert_eq!(result.aliases["kmalloc-64"], ":0000040");
    let files = &result.caches[":0000040"];
    assert_eq!(files.parsed["alloc_fastpath"], ParsedMemoryValue::Unsigned(u64::MAX));
    assert_eq!(files.parsed["objects"].get("total").and_then(ParsedMemoryValue::as_u64), Some(100));
    assert_eq!(
        files.parsed["objects"]
            .get("nodes")
            .and_then(|nodes| nodes.get("9"))
            .and_then(ParsedMemoryValue::as_u64),
        Some(75)
    );
    assert!(!files.unavailable.contains_key("validate"));
    assert!(!files.unavailable.contains_key("shrink"));
    assert!(result.unavailable.contains_key("escape"));
    Ok(())
}
