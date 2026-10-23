#![cfg(target_os = "linux")]

use std::fs;
use wora_observability_linux_mm::{
    CollectionLimit, CollectionLimits, MemoryReadError, MemorySkipReason, ParsedMemoryValue, ShrinkerStats, collect_with_limits, parse_memory_file,
};

#[test]
fn shrinker_counts_keep_object_units_identifiers_and_node_column_order() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    for name in ["a-cache-1", "b-cache-2", "c-cache-3"] {
        fs::create_dir_all(root.path().join("shrinker").join(name))?;
    }
    let count = root.path().join("shrinker/a-cache-1/count");
    let text = "0 9007199254740993 0\n18446744073709551615 0 7\n";
    fs::write(&count, text)?;
    fs::write(root.path().join("shrinker/a-cache-1/scan"), [0xff])?;
    fs::write(root.path().join("shrinker/b-cache-2/count"), "")?;
    let snapshot = ShrinkerStats::from_debug_root(root.path());
    let ParsedMemoryValue::List(rows) = &snapshot.counts["a-cache-1"].parsed["count"] else {
        return Err("no rows".into());
    };
    assert_eq!(rows[1].get("cgroup_id").and_then(ParsedMemoryValue::as_u64), Some(u64::MAX));
    assert_eq!(
        rows[0].get("node_counts"),
        Some(&ParsedMemoryValue::try_list(vec![
            ParsedMemoryValue::Unsigned(9_007_199_254_740_993),
            ParsedMemoryValue::Unsigned(0)
        ])?)
    );
    assert_eq!(snapshot.counts["b-cache-2"].parsed["count"], ParsedMemoryValue::try_list(Vec::new())?);
    assert!(matches!(snapshot.counts["c-cache-3"].unavailable.get("count"), Some(MemoryReadError::NotFound)));
    assert!(!snapshot.counts["a-cache-1"].unavailable.contains_key("scan"));
    assert_eq!(fs::read_to_string(count)?, text);
    #[cfg(feature = "serde1")]
    {
        let decoded: ShrinkerStats = serde_json::from_str(&serde_json::to_string(&snapshot)?)?;
        assert_eq!(decoded.counts["a-cache-1"].parsed, snapshot.counts["a-cache-1"].parsed);
    }
    Ok(())
}

#[test]
fn shrinker_requests_entries_and_sources_are_admitted_before_more_work() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    for name in ["a", "b"] {
        let directory = root.path().join("shrinker").join(name);
        fs::create_dir_all(&directory)?;
        fs::write(directory.join("count"), "1 123456789\n")?;
    }
    let (snapshot, report) = collect_with_limits(CollectionLimits::default(), || ShrinkerStats::from_debug_root_with_read_limit(root.path(), 1))?;
    assert_eq!(snapshot.counts.keys().map(String::as_str).collect::<Vec<_>>(), ["a"]);
    assert!(snapshot.unavailable.contains_key("."));
    assert_eq!(report.files, 1);
    assert_eq!(report.limit_exceeded, Some(CollectionLimit::Files));
    let (snapshot, report) = collect_with_limits(
        CollectionLimits {
            entries: 1,
            ..Default::default()
        },
        || ShrinkerStats::from_debug_root(root.path()),
    )?;
    assert!(snapshot.counts.is_empty());
    assert_eq!(report.limit_exceeded, Some(CollectionLimit::Entries));
    let (snapshot, report) = collect_with_limits(
        CollectionLimits {
            file_bytes: 8,
            line_bytes: 8,
            ..Default::default()
        },
        || ShrinkerStats::from_debug_root(root.path()),
    )?;
    assert!(!snapshot.counts["a"].parsed.contains_key("count"));
    assert_eq!(report.limit_exceeded, Some(CollectionLimit::FileBytes));
    for invalid in [0, 257] {
        let snapshot = ShrinkerStats::from_debug_root_with_read_limit(root.path(), invalid);
        assert!(snapshot.counts.is_empty());
        assert!(snapshot.unavailable.contains_key("."));
    }
    fs::remove_file(root.path().join("shrinker/b/count"))?;
    std::os::unix::fs::symlink("../a/count", root.path().join("shrinker/b/count"))?;
    std::os::unix::fs::symlink("a", root.path().join("shrinker/c"))?;
    let snapshot = ShrinkerStats::from_debug_root(root.path());
    assert_eq!(snapshot.counts["b"].skipped["count"], MemorySkipReason::NotRegularFile);
    assert!(!snapshot.counts.contains_key("c"));
    Ok(())
}

#[test]
fn malformed_shrinker_tables_fail_atomically_without_assuming_dense_nodes() -> Result<(), Box<dyn std::error::Error>> {
    for bad in ["1\n", "1 -1\n", "1 18446744073709551616\n", "1 2\n1 3\n", "1 2 3\n2 4\n"] {
        assert!(parse_memory_file("shrinker/example/count", bad).is_err());
    }
    let too_many = "1 ".to_owned() + &vec!["0"; 4097].join(" ");
    assert!(parse_memory_file("shrinker/example/count", &too_many).is_err());
    assert_eq!(parse_memory_file("shrinker/example/count", "")?, Some(ParsedMemoryValue::try_list(Vec::new())?));
    Ok(())
}
