#![cfg(target_os = "linux")]
use wora_observability_linux_mm::{MemoryTraceParser, MemoryTraceRecord};

#[test]
fn trace_identity_timing_loss_and_unknown_records_remain_explicit() -> Result<(), Box<dyn std::error::Error>> {
    let mut parser = MemoryTraceParser::new();
    parser.parse_line(" job-name-42 [009] .... 12.000001: mm_vmscan_direct_reclaim_begin: order=0 gfp_flags=GFP_KERNEL")?;
    let Some(MemoryTraceRecord::Event { event }) = parser.parse_line(" job-name-42 [003] .... 12.000009: mm_vmscan_direct_reclaim_end: nr_reclaimed=8")? else {
        return Err("no event".into());
    };
    assert_eq!((event.task.as_str(), event.tid, event.cpu, event.duration_ns), ("job-name", 42, 3, Some(8000)));
    let Some(MemoryTraceRecord::Event { event }) =
        parser.parse_line(" trigger-9 [000] d..2 13.000000001: mark_victim: pid=77 comm=some victim total-vm=4096kB future=hello")?
    else {
        return Err("no victim".into());
    };
    assert_eq!((event.tid, event.victim_pid), (9, Some(77)));
    assert!(event.payload.contains("future=hello"));
    parser.parse_line(" job-name-42 [009] .... 14.000001: mm_compaction_begin: zone=Normal")?;
    assert!(matches!(parser.parse_line("CPU:9 [LOST 5 EVENTS]")?, Some(MemoryTraceRecord::Lost { .. })));
    let Some(MemoryTraceRecord::Event { event }) = parser.parse_line(" job-name-42 [009] .... 15.000001: mm_compaction_end: status=success")? else {
        return Err("no event".into());
    };
    assert_eq!(event.duration_ns, None);
    assert!(matches!(
        parser.parse_line("unfamiliar kernel record")?,
        Some(MemoryTraceRecord::Unparsed { .. })
    ));
    assert!(parser.parse_line(&"x".repeat(65537)).is_err());
    Ok(())
}
