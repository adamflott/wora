#![cfg(target_os = "linux")]
use std::{fs, os::unix::fs::symlink};
use wora_observability_linux_mm::{MemoryFiles, MemoryReadError, MemorySkipReason, NumaStats, ParsedMemoryValue};

#[test]
fn sparse_nodes_tiers_and_native_units_survive_capture() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    for (name, value) in [
        ("devices/system/node/online", "0,9\n"),
        ("devices/system/node/node9/meminfo", "Node 9 MemFree: 9007199254740993 kB\n"),
        ("devices/system/node/node9/numastat", "numa_hit 18446744073709551615\n"),
        ("devices/system/node/node9/distance", "20 10\n"),
        ("devices/virtual/memory_tiering/memory_tier22/nodelist", "9\n"),
    ] {
        let path = root.path().join(name);
        fs::create_dir_all(path.parent().ok_or("no parent")?)?;
        fs::write(path, value)?;
    }
    symlink(root.path().join("devices/system/node/node9"), root.path().join("devices/system/node/node10"))?;
    let snapshot = NumaStats::from_sys_root(root.path());
    assert_eq!(snapshot.nodes.len(), 1);
    assert_eq!(
        snapshot.nodes[&9].parsed["meminfo"]
            .get("MemFree")
            .and_then(|value| value.get("value"))
            .and_then(ParsedMemoryValue::as_u64),
        Some(9_007_199_254_740_993)
    );
    assert_eq!(
        snapshot.tiers[&22].parsed["nodelist"],
        ParsedMemoryValue::List(vec![ParsedMemoryValue::Map(std::collections::BTreeMap::from([
            ("start".into(), ParsedMemoryValue::Unsigned(9)),
            ("end".into(), ParsedMemoryValue::Unsigned(9))
        ]))])
    );
    assert_eq!(snapshot.nodes[&9].unavailable["vmstat"], MemoryReadError::NotFound);
    #[cfg(feature = "serde1")]
    assert_eq!(serde_json::to_value(snapshot)?["nodes"]["9"]["parsed"]["distance"], serde_json::json!([20, 10]));
    Ok(())
}

#[test]
fn bounded_files_preserve_precision_and_isolate_bad_inputs() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    for (name, value) in [("large", "18446744073709551615"), ("signed", "-3"), ("overflow", "18446744073709551616")] {
        fs::write(root.path().join(name), value)?;
    }
    fs::write(root.path().join("oversized"), vec![b'x'; 4 * 1024 * 1024 + 1])?;
    symlink(root.path().join("large"), root.path().join("link"))?;
    let snapshot = MemoryFiles::from_names(root.path(), &["large", "signed", "overflow", "oversized", "link", "../escape"]);
    assert_eq!(snapshot.parsed["large"], ParsedMemoryValue::Unsigned(u64::MAX));
    assert_eq!(snapshot.parsed["signed"], ParsedMemoryValue::Signed(-3));
    assert_eq!(snapshot.skipped["link"], MemorySkipReason::NotRegularFile);
    for name in ["overflow", "oversized", "../escape"] {
        assert!(snapshot.unavailable.contains_key(name));
    }
    Ok(())
}
