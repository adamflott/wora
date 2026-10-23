#![cfg(target_os = "linux")]
use wora_observability_linux_mm::{MemoryMapping, ParsedMemoryValue, ProcessMemoryDetails, ProcessMemoryStats};

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
    assert_eq!(
        mappings[0].fields["Future"].get("value").and_then(ParsedMemoryValue::as_u64),
        Some(9_007_199_254_740_993)
    );
    assert_eq!(
        mappings[0].fields["VmFlags"],
        ParsedMemoryValue::try_list(["rd", "wr", "zz"].map(ParsedMemoryValue::try_text).into_iter().collect::<Result<Vec<_>, _>>()?)?
    );
    assert_eq!(mappings[0].pathname, "/path with spaces (deleted)");
    assert_eq!(mappings[0].permissions, "rw-p");
    assert_eq!(mappings[0].offset, 0);
    assert_eq!(mappings[0].device, (0, 0));
    assert_eq!(mappings[0].inode, 0);
    #[cfg(feature = "serde1")]
    {
        let json = serde_json::to_value(&mappings[0])?;
        assert!(json.get("header").is_none());
        assert_eq!(json["fields"]["Future"]["value"], 9_007_199_254_740_993_u64);
        assert_eq!(json["fields"]["VmFlags"], serde_json::json!(["rd", "wr", "zz"]));
    }
    assert_eq!(
        ProcessMemoryStats::from_process_root(42, root.path()).information.parsed["oom_score_adj"],
        ParsedMemoryValue::Signed(-1000)
    );
    for invalid in ["Rss: 1 kB\n", "1000-900 rw-p 0 00:00 0\n", "1000-3000 rw-p 0 00:00 0\nRss: 1 kB\nRss: 2 kB\n"] {
        assert!(MemoryMapping::parse_smaps(invalid).is_err());
    }
    Ok(())
}
