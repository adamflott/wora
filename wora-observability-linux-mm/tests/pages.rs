#![cfg(target_os = "linux")]
use wora_observability_linux_mm::{PageMapEntry, PageRange, PhysicalPageStats, ProcessPageMap, read_page_entries};

#[test]
fn bounded_reads_decode_flags_swap_and_ambiguous_zero_pfns() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let bytes: Vec<_> = [0u64, 1u64 << 63, (1u64 << 63) | (1u64 << 55) | 42]
        .into_iter()
        .flat_map(u64::to_ne_bytes)
        .collect();
    for file in ["kpageflags", "kpagecount", "pagemap"] {
        std::fs::write(root.path().join(file), &bytes)?;
    }
    let range = PageRange { start: 1, count: 2 };
    let physical = PhysicalPageStats::from_proc_root(root.path(), range);
    assert_eq!(physical.flags.ok_or("no flags")?, [1u64 << 63, (1u64 << 63) | (1u64 << 55) | 42]);
    assert!(physical.cgroup_inodes.is_none());
    let process = ProcessPageMap::from_process_root(99, root.path(), range);
    let entries = process.entries.ok_or("no pagemap")?;
    assert!(entries[0].present);
    assert_eq!(entries[0].pfn, None);
    assert_eq!(entries[1].pfn, Some(42));
    assert!(entries[1].soft_dirty);
    let swapped = PageMapEntry::decode((1u64 << 62) | (123 << 5) | 7);
    assert_eq!((swapped.swap_type, swapped.swap_offset, swapped.pfn), (Some(7), Some(123), None));
    for range in [
        PageRange { start: 2, count: 2 },
        PageRange { start: u64::MAX, count: 1 },
        PageRange { start: 0, count: 0 },
        PageRange { start: 0, count: 65537 },
    ] {
        assert!(read_page_entries(root.path().join("pagemap"), range).is_err());
    }
    Ok(())
}
