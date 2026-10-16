#![cfg(target_os = "linux")]
use std::fs;
use wora_observability_linux_mm::{MemoryHardwareStats, MemoryValue};

#[test]
fn hardware_scopes_and_cma_ranges_remain_separate_and_actions_untouched() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    for (file, text) in [
        ("devices/system/memory/memory17/state", "online\n"),
        ("devices/system/memory/block_size_bytes", "8000000\n"),
        ("devices/system/edac/mc/mc2/ce_count", "9\n"),
        ("devices/system/edac/mc/mc2/dimm15/dimm_ce_count", "4\n"),
        ("devices/system/edac/mc/mc2/dimm15/dimm_label", "DIMM_A1\n"),
        ("devices/system/node/node9/memory_failure/recovered", "3\n"),
        ("kernel/debug/cma/pool/count", "65536\n"),
        ("kernel/debug/cma/pool/ranges/1/base_pfn", "9007199254740993\n"),
    ] {
        let path = root.path().join(file);
        fs::create_dir_all(path.parent().ok_or("no parent")?)?;
        fs::write(path, text)?;
    }
    fs::write(root.path().join("devices/system/edac/mc/mc2/reset_counters"), [0xff])?;
    fs::write(root.path().join("kernel/debug/cma/pool/alloc"), [0xff])?;
    fs::write(root.path().join("kernel/debug/cma/pool/free"), [0xff])?;
    let data = MemoryHardwareStats::from_sys_root(root.path());
    assert_eq!(data.blocks[&17].values["state"], MemoryValue::Text("online".into()));
    assert_eq!(data.edac["mc2/dimm15"].values["dimm_ce_count"], MemoryValue::Unsigned(4));
    assert_eq!(data.poisoned[&9].values["recovered"], MemoryValue::Unsigned(3));
    assert_eq!(data.cma["pool/ranges/1"].values["base_pfn"], MemoryValue::Unsigned(9_007_199_254_740_993));
    assert!(!data.edac["mc2"].unavailable.contains_key("reset_counters"));
    assert!(!data.cma["pool"].unavailable.contains_key("alloc"));
    Ok(())
}
