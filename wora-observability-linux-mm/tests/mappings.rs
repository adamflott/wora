#![cfg(target_os = "linux")]
use wora_observability_linux_mm::{MemoryMapping, MemoryValue, ProcessMemoryDetails, ProcessMemoryStats};

#[test]
fn mapping_fields_flags_paths_and_process_controls_are_preserved() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let text = "1000-3000 rw-p 00000000 00:00 0 /path with spaces (deleted)\nRss: 12 kB\nFuture: 9007199254740993 kB\nVmFlags: rd wr zz\n4000-5000 r--p 00000000 00:00 0\nRss: 0 kB\n";
    std::fs::write(root.path().join("smaps"), text)?;
    std::fs::write(root.path().join("oom_score_adj"), "-1000\n")?;
    std::fs::write(root.path().join("status"), "VmLck:\t12 kB\nVmPTE:\t4 kB\n")?;
    let detail = ProcessMemoryDetails::from_process_root(42, root.path());
    let mappings = detail.mappings.ok_or("missing mappings")?;
    assert_eq!(mappings.len(), 2);
    assert_eq!(mappings[0].fields["Future"], "9007199254740993 kB");
    assert_eq!(mappings[0].fields["VmFlags"], "rd wr zz");
    assert!(mappings[0].header.ends_with("/path with spaces (deleted)"));
    assert_eq!(
        ProcessMemoryStats::from_process_root(42, root.path()).information.values["oom_score_adj"],
        MemoryValue::Signed(-1000)
    );
    for invalid in ["Rss: 1 kB\n", "1000-900 rw-p 0 00:00 0\n", "1000-3000 rw-p 0 00:00 0\nRss: 1 kB\nRss: 2 kB\n"] {
        assert!(MemoryMapping::parse_smaps(invalid).is_err());
    }
    Ok(())
}
