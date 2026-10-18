#![cfg(target_os = "linux")]
use wora_observability_linux_mm::{LruHistogram, LruStats};

// Synthetic full-statistics rows from mm/vmscan.c in 6.12/7.2. Suffixes are
// significant: r/e/p are counts, R/T averages, x unavailable placeholders.
fn full_generation(sequence: u64, averages: bool) -> String {
    let mut text = format!(" {sequence} 100 9007199254740993x 42\n");
    for tier in 0..4 {
        text += &if averages {
            format!(" {tier} 5R 9T 0x 0R 0T 0x\n")
        } else {
            format!(" {tier} 5r 9e 0p 0x 0x 0x\n")
        };
    }
    text
}

#[test]
fn full_histograms_preserve_cgroup_node_generation_tier_and_availability() -> Result<(), Box<dyn std::error::Error>> {
    use wora_observability_linux_mm::ParsedMemoryValue;
    let root = tempfile::tempdir()?;
    std::fs::create_dir_all(root.path().join("kernel/debug"))?;
    let text = format!(
        "memcg 4 /user.slice/a  b\n node 9\n{} 1t 2y 3f 4a\n{} 0x 0x 0x 0x\nmemcg 8 /other\n node 0\n{}",
        full_generation(2, false),
        full_generation(3, true),
        full_generation(2, true),
    );
    std::fs::write(root.path().join("kernel/debug/lru_gen_full"), &text)?;
    let data = LruStats::from_sys_root(root.path());
    let ParsedMemoryValue::List(records) = &data.debug.parsed["lru_gen_full"] else {
        return Err("no full records".into());
    };
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].get("cgroup_id").and_then(ParsedMemoryValue::as_u64), Some(4));
    assert_eq!(records[0].get("cgroup_path"), Some(&ParsedMemoryValue::try_text("/user.slice/a  b")?));
    assert_eq!(records[0].get("node").and_then(ParsedMemoryValue::as_u64), Some(9));
    assert_eq!(records[0].get("sequence").and_then(ParsedMemoryValue::as_u64), Some(2));
    let pages = records[0].get("anonymous_pages").ok_or("no pages")?;
    assert_eq!(pages.get("value").and_then(ParsedMemoryValue::as_u64), Some(9_007_199_254_740_993));
    assert_eq!(pages.get("expired").and_then(ParsedMemoryValue::as_bool), Some(true));
    assert_eq!(records[2].get("cgroup_id").and_then(ParsedMemoryValue::as_u64), Some(8));
    assert!(records[2].get("mm_stats").is_none());
    #[cfg(feature = "serde1")]
    {
        let json = serde_json::to_value(&data.debug.parsed["lru_gen_full"])?;
        assert_eq!(json[0]["tiers"][0]["anonymous"][0]["tag"], "r");
        assert_eq!(json[0]["tiers"][0]["file"][0]["tag"], "x");
        assert!(json[0]["tiers"][0]["file"][0].get("value").is_none());
        assert_eq!(json[1]["tiers"][0]["anonymous"][0]["tag"], "R");
        assert_eq!(json[1]["tiers"][0]["file"][0]["value"], 0);
        assert_eq!(json[0]["mm_stats"][0]["tag"], "t");
        assert!(json[1]["mm_stats"][0].get("value").is_none());
    }
    assert_eq!(std::fs::read_to_string(root.path().join("kernel/debug/lru_gen_full"))?, text);
    let no_memcg = format!("memcg 0 \n node 0\n{}", full_generation(0, true));
    assert!(wora_observability_linux_mm::parse_memory_file("lru_gen_full", &no_memcg)?.is_some());
    assert_eq!(LruHistogram::parse("memcg 0 \n node 0\n 0 1 2 3\n")?[0].cgroup_path, "");
    Ok(())
}

#[test]
fn full_histograms_reject_incomplete_unknown_or_duplicate_scopes_and_obey_limits() -> Result<(), Box<dyn std::error::Error>> {
    use wora_observability_linux_mm::{CollectionLimit, CollectionLimits, ParsedMemoryValue, collect_with_limits, parse_memory_file};
    let valid = format!("memcg 4 /\nnode 9\n{}", full_generation(2, true));
    for bad in [
        "memcg 4 /\n".to_owned(),
        "node 9\n".to_owned(),
        valid.replace("3 5R 9T 0x 0R 0T 0x\n", ""),
        valid.replace("0x 0R", "1x 0R"),
        valid.replace("5R", "5r"),
        valid.replace("9007199254740993x", "18446744073709551616x"),
        valid.clone() + &full_generation(2, true),
        valid.clone() + " 1t 2f 3y 4a\n",
        valid.clone() + "node 9\n",
        valid.clone() + &(3..7).map(|sequence| full_generation(sequence, true)).collect::<String>(),
    ] {
        assert!(parse_memory_file("lru_gen_full", &bad).is_err(), "accepted {bad}");
    }
    assert_eq!(parse_memory_file("lru_gen_full", "")?, Some(ParsedMemoryValue::try_list(Vec::new())?));
    let root = tempfile::tempdir()?;
    std::fs::create_dir_all(root.path().join("kernel/debug"))?;
    std::fs::write(root.path().join("kernel/debug/lru_gen_full"), valid)?;
    let (stats, report) = collect_with_limits(
        CollectionLimits {
            file_bytes: 64,
            line_bytes: 64,
            ..Default::default()
        },
        || LruStats::from_sys_root(root.path()),
    )?;
    assert!(!stats.debug.parsed.contains_key("lru_gen_full"));
    assert!(stats.debug.unavailable.contains_key("lru_gen_full"));
    assert_eq!(report.limit_exceeded, Some(CollectionLimit::FileBytes));
    Ok(())
}

#[test]
fn generations_remain_scoped_and_collection_does_not_age_or_reclaim() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir_all(root.path().join("kernel/debug"))?;
    let text = "memcg 4 /user.slice/job\n node 9\n 6 100 9007199254740993 42\n 7 20 9 10\nmemcg 8 /other\n node 0\n 6 1 0 0\n";
    std::fs::write(root.path().join("kernel/debug/lru_gen"), text)?;
    let data = LruStats::from_sys_root(root.path());
    let histograms = data.histograms.ok_or("missing histograms")?;
    assert_eq!(histograms.len(), 2);
    assert_eq!(histograms[0].generations[0].anonymous_pages, 9_007_199_254_740_993);
    assert_eq!(histograms[0].node, 9);
    assert_eq!(std::fs::read_to_string(root.path().join("kernel/debug/lru_gen"))?, text);
    for invalid in [
        "node 0",
        "1 2 3 4",
        "memcg 1 /\nnode 0\n1 2 3 4\n1 2 3 4",
        "memcg 1 /\nnode 0\n1 2 3 18446744073709551616",
    ] {
        assert!(LruHistogram::parse(invalid).is_err());
    }
    Ok(())
}
