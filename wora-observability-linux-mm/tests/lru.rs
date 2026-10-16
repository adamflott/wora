#![cfg(target_os = "linux")]
use wora_observability_linux_mm::{LruHistogram, LruStats};

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
