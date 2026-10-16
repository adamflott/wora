#![cfg(target_os = "linux")]
use wora_observability_linux_mm::{FragmentationStats, FragmentationZone};

#[test]
fn exact_indices_sentinels_and_sparse_zones_are_not_rounded() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("extfrag"))?;
    std::fs::write(root.path().join("extfrag/extfrag_index"), "Node 9, zone Normal -1.000 0.001 0.999 1.000\n")?;
    let stats = FragmentationStats::from_debug_root(root.path());
    assert_eq!(stats.external.ok_or("no indices")?[0].orders, [-1000, 1, 999, 1000]);
    assert!(stats.unusable.is_none());
    for text in [
        "Node 1, zone DMA 1.001",
        "Node 1, zone DMA 0.00",
        "Node 1, zone DMA -0.500",
        "Node 1, zone DMA 0.001\nNode 1, zone DMA 0.002",
    ] {
        assert!(FragmentationZone::parse_table(text).is_err());
    }
    Ok(())
}
