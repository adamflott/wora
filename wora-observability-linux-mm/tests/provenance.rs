#![cfg(target_os = "linux")]
use wora_observability_linux_mm::{KernelLeakStats, MemoryValue, PageOwnerStats};

#[test]
fn provenance_is_bounded_and_does_not_change_tracking_or_filters() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("page_owner_stacks"))?;
    let dump = "Page allocated via order 0\nPFN 42 pid 9\n allocation_stack+0x1\n";
    std::fs::write(root.path().join("page_owner"), dump)?;
    std::fs::write(root.path().join("page_owner_stacks/count_threshold"), "100\n")?;
    std::fs::write(root.path().join("kmemleak"), "unreferenced object 0xffff0000 (size 32):\n backtrace\n")?;
    let owner = PageOwnerStats::from_debug_root_with_limit(root.path(), 8);
    assert!(!owner.files.values.contains_key("page_owner"));
    assert!(owner.files.unavailable.contains_key("page_owner"));
    assert_eq!(owner.files.values["page_owner_stacks/count_threshold"], MemoryValue::Unsigned(100));
    assert!(KernelLeakStats::from_debug_root(root.path()).files.values.contains_key("kmemleak"));
    assert_eq!(std::fs::read_to_string(root.path().join("page_owner"))?, dump);
    assert_eq!(std::fs::read_to_string(root.path().join("page_owner_stacks/count_threshold"))?, "100\n");
    assert!(PageOwnerStats::from_debug_root_with_limit(root.path(), 0).files.values.is_empty());
    Ok(())
}
