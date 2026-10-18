#![cfg(target_os = "linux")]
use wora_observability_linux_mm::{PageMapEntry, PageRange, PhysicalPageStats, ProcessPageMap, read_page_entries};

#[test]
fn bit_57_uses_vma_wp_rwp_context_and_preserves_unknown_and_legacy_flags() -> Result<(), Box<dyn std::error::Error>> {
    use wora_observability_linux_mm::{MemoryMapping, ParsedMemoryValue, UserfaultfdProtection};
    for (flags, expected) in [
        ("uw", UserfaultfdProtection::WriteProtected),
        ("ur", UserfaultfdProtection::ReadWriteProtected),
        ("uw ur", UserfaultfdProtection::Unknown),
        ("rd wr", UserfaultfdProtection::Unknown),
        ("", UserfaultfdProtection::Unknown),
    ] {
        let mappings = MemoryMapping::parse_smaps(&format!("1000-3000 rw-p 0 00:00 0\nVmFlags: {flags}\n"))?;
        let vm_flags = mappings[0].fields.get("VmFlags");
        // Protection markers also occur in non-present/swapped entries.
        for state in [0, 1u64 << 62, 1u64 << 63] {
            let raw = state | (1 << 57) | (1 << 58) | 42;
            let entry = PageMapEntry::decode(raw);
            assert_eq!(entry.userfaultfd_protection(vm_flags), expected);
            assert!(entry.uffd_write_protected); // public compatibility alias
            assert!(entry.guard);
            assert_eq!(entry.raw, raw);
            assert_eq!(
                PageMapEntry::decode(state | 42).userfaultfd_protection(vm_flags),
                UserfaultfdProtection::Unprotected
            );
            #[cfg(feature = "serde1")]
            {
                let decoded: PageMapEntry = serde_json::from_str(&serde_json::to_string(&entry)?)?;
                assert_eq!(decoded, entry);
                assert_eq!(decoded.userfaultfd_protection(vm_flags), expected);
            }
        }
    }
    let entry = PageMapEntry::decode(1 << 57);
    assert_eq!(entry.userfaultfd_protection(None), UserfaultfdProtection::Unknown);
    assert_eq!(
        entry.userfaultfd_protection(Some(&ParsedMemoryValue::try_text("ur")?)),
        UserfaultfdProtection::Unknown
    );
    assert_eq!(
        entry.userfaultfd_protection(Some(&ParsedMemoryValue::try_list(vec![ParsedMemoryValue::Unsigned(1)])?)),
        UserfaultfdProtection::Unknown
    );
    let header_only = MemoryMapping::parse_smaps("1000-3000 rw-p 0 00:00 0\n")?;
    assert_eq!(
        entry.userfaultfd_protection(header_only[0].fields.get("VmFlags")),
        UserfaultfdProtection::Unknown
    );
    Ok(())
}

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
