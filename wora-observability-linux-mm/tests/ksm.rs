#![cfg(target_os = "linux")]
use std::{fs, os::unix::fs::PermissionsExt};
use wora_observability_linux_mm::{KsmStats, MemorySkipReason, ParsedMemoryValue};

#[test]
fn signed_profit_future_counters_and_process_scope_are_retained() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let global = root.path().join("kernel/mm/ksm");
    fs::create_dir_all(&global)?;
    fs::create_dir(root.path().join("process"))?;
    for (name, value) in [("general_profit", "-123"), ("future_pages", "18446744073709551615"), ("run", "0")] {
        fs::write(global.join(name), value)?;
    }
    fs::write(global.join("action"), [0xff])?;
    fs::set_permissions(global.join("action"), fs::Permissions::from_mode(0o200))?;
    fs::write(root.path().join("process/ksm_stat"), "ksm_process_profit -20\nksm_rmap_items 10\n")?;
    let result = KsmStats::from_roots(root.path(), root.path().join("process"), 77);
    assert_eq!(result.pid, 77);
    assert_eq!(result.global.parsed["general_profit"], ParsedMemoryValue::Signed(-123));
    assert_eq!(result.global.parsed["future_pages"], ParsedMemoryValue::Unsigned(u64::MAX));
    assert_eq!(result.global.skipped["action"], MemorySkipReason::WriteOnly);
    assert_eq!(
        result.process.parsed["ksm_stat"].get("ksm_rmap_items").and_then(ParsedMemoryValue::as_u64),
        Some(10)
    );
    assert_eq!(fs::read_to_string(global.join("run"))?, "0");
    Ok(())
}
