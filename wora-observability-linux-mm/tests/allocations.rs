#![cfg(target_os = "linux")]
use wora_observability_linux_mm::{AllocationSites, KernelAllocationStats, ParsedMemoryValue};

#[test]
fn versioned_sites_keep_accuracy_markers_precision_and_native_vmalloc_data() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::write(
        root.path().join("allocinfo"),
        "allocinfo - version: 2.0\n# size count location\n18446744073709551615 4 mm/filemap.c:123 func:test [module] accurate:no future:yes\n",
    )?;
    std::fs::write(
        root.path().join("vmallocinfo"),
        "0x0000000000000000-0x0000000000000000 4096 caller pages=1 vmalloc N9=1\n",
    )?;
    let result = KernelAllocationStats::from_proc_root(root.path());
    let site = &result.allocation_sites.ok_or("no sites")?.sites[0];
    assert_eq!(site.bytes, u64::MAX);
    assert!(site.attributes.iter().any(|v| v == "accurate:no"));
    assert!(matches!(result.files.parsed["vmallocinfo"], ParsedMemoryValue::List(_)));
    for text in [
        "",
        "allocinfo - version: 9.0\n",
        "allocinfo - version: 2.0\n1 2\n",
        "allocinfo - version: 2.0\n18446744073709551616 1 x\n",
    ] {
        assert!(AllocationSites::parse(text).is_err());
    }
    Ok(())
}
