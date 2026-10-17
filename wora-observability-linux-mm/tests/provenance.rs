#![cfg(target_os = "linux")]
use wora_observability_linux_mm::{KernelLeakStats, PageOwnerStats, ParsedMemoryValue};

#[test]
fn provenance_is_bounded_and_does_not_change_tracking_or_filters() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("page_owner_stacks"))?;
    let dump = "Page allocated via order 0\nPFN 42 pid 9\n allocation_stack+0x1\n";
    std::fs::write(root.path().join("page_owner"), dump)?;
    std::fs::write(root.path().join("page_owner_stacks/count_threshold"), "100\n")?;
    std::fs::write(root.path().join("kmemleak"), "unreferenced object 0xffff0000 (size 32):\n backtrace\n")?;
    let owner = PageOwnerStats::from_debug_root_with_limit(root.path(), 8);
    assert!(!owner.files.parsed.contains_key("page_owner"));
    assert!(owner.files.unavailable.contains_key("page_owner"));
    assert_eq!(owner.files.parsed["page_owner_stacks/count_threshold"], ParsedMemoryValue::Unsigned(100));
    let leaks = KernelLeakStats::from_debug_root(root.path());
    assert!(!leaks.files.parsed.contains_key("kmemleak"));
    assert!(leaks.files.unavailable.contains_key("kmemleak"));
    assert_eq!(std::fs::read_to_string(root.path().join("page_owner"))?, dump);
    assert_eq!(std::fs::read_to_string(root.path().join("page_owner_stacks/count_threshold"))?, "100\n");
    assert!(PageOwnerStats::from_debug_root_with_limit(root.path(), 0).files.parsed.is_empty());
    Ok(())
}
