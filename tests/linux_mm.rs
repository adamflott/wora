#![cfg(target_os = "linux")]

use wora::o11y::linux_mm::{BuddyInfo, Current, FreeBlockCount, Swaps};
use wora::o11y::{HostStats, LinuxMemoryStats, O11yChannelSink, O11yEventKind, O11ySink, o11y_new_ev_hoststats};

#[test]
fn routine_tables_are_shared_and_replacements_preserve_retained_snapshots() -> Result<(), Box<dyn std::error::Error>> {
    use wora::events::MemorySnapshot;
    use wora::o11y::{LinuxMemoryData, o11y_new_ev_hoststats_owned};
    let budget = wora::o11y::linux_mm::HeapBudget::default();
    let mut stats = HostStats {
        linux_mm: LinuxMemoryStats::try_new(&budget, || {
            Ok(LinuxMemoryData {
                vmstat: Some(wora::o11y::linux_mm::VmStat(std::collections::HashMap::from([("test".into(), 1)]))),
                ..Default::default()
            })
        })?,
        ..Default::default()
    };
    let snapshot = MemorySnapshot::from_host_stats(&stats);
    let event = o11y_new_ev_hoststats_owned::<()>(stats.clone());
    let O11yEventKind::HostStats(event_stats) = event.kind else {
        return Err("wrong event".into());
    };
    assert!(stats.linux_mm.shares_data_with(&snapshot.linux_mm));
    assert!(stats.linux_mm.shares_data_with(&event_stats.linux_mm));
    stats.linux_mm = stats.linux_mm.try_rebuild(&budget, |_| {
        Ok(LinuxMemoryData {
            vmstat: Some(wora::o11y::linux_mm::VmStat(std::collections::HashMap::from([("test".into(), 2)]))),
            ..Default::default()
        })
    })?;
    assert!(!stats.linux_mm.shares_data_with(&snapshot.linux_mm));
    assert_eq!(stats.linux_mm.vmstat.as_ref().ok_or("missing table")?.0["test"], 2);
    assert_eq!(snapshot.linux_mm.vmstat.as_ref().ok_or("missing table")?.0["test"], 1);
    assert_eq!(event_stats.linux_mm.vmstat.as_ref().ok_or("missing table")?.0["test"], 1);
    assert!(budget.usage().retained_bytes > 0);
    drop((stats, snapshot, event_stats));
    assert_eq!(budget.usage().retained_bytes, 0);
    Ok(())
}

#[test]
fn routine_generation_admission_skips_and_recovers_after_readers_release_data() -> Result<(), Box<dyn std::error::Error>> {
    use wora::o11y::{LinuxMemoryLimits, RuntimeEnvironment, SystemRuntimeEnvironment};
    let provider = SystemRuntimeEnvironment::with_linux_memory_limits(LinuxMemoryLimits {
        live_snapshots: 3,
        ..Default::default()
    })?;
    let _startup = provider.initial_host()?;
    let first = provider.refresh_host_stats()?.ok_or("missing first sample")?;
    let _second = provider.refresh_host_stats()?.ok_or("missing second sample")?;
    assert!(provider.clone().refresh_host_stats()?.is_none());
    assert_eq!(provider.memory_sampling_status()?.live_snapshots, 3);
    assert_eq!(provider.memory_sampling_status()?.skipped_samples, 1);
    drop(first);
    assert!(provider.refresh_host_stats()?.is_some());
    assert!(provider.memory_sampling_status()?.live_snapshots <= 3);
    Ok(())
}

#[test]
fn diagnostic_generation_admission_reuses_stale_data_until_a_slot_is_released() -> Result<(), Box<dyn std::error::Error>> {
    use wora::o11y::{LinuxMemoryCollector, LinuxMemoryEnvironment, LinuxMemoryLimits, LinuxMemoryOptions, RuntimeEnvironment};
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("self"))?;
    let smaps = root.path().join("self/smaps");
    std::fs::write(&smaps, "1000-3000 rw-p 0 00:00 0\nRss: 12 kB\n")?;
    let options = LinuxMemoryOptions::new()
        .enable(LinuxMemoryCollector::ProcessMappings)
        .with_roots(root.path(), root.path())
        .with_interval(std::time::Duration::from_nanos(1))
        .with_memory_limits(LinuxMemoryLimits {
            live_snapshots: 3,
            ..Default::default()
        });
    let provider = LinuxMemoryEnvironment::new(CapturedEnvironment, options)?;
    let _startup = provider.initial_host()?;
    let first = provider.refresh_host_stats()?.ok_or("missing first sample")?;
    let second = provider.refresh_host_stats()?.ok_or("missing second sample")?;
    std::fs::write(&smaps, "malformed")?;
    let stale = provider.clone().refresh_host_stats()?.ok_or("missing stale sample")?;
    assert!(std::ptr::eq(
        second.linux_mm.diagnostics().ok_or("missing batch")?,
        stale.linux_mm.diagnostics().ok_or("missing batch")?
    ));
    assert_eq!(provider.diagnostic_sampling_status()?.live_snapshots, 3);
    assert_eq!(provider.diagnostic_sampling_status()?.skipped_samples, 1);
    drop(first);
    let fresh = provider.refresh_host_stats()?.ok_or("missing recovered sample")?;
    assert!(!std::ptr::eq(
        stale.linux_mm.diagnostics().ok_or("missing batch")?,
        fresh.linux_mm.diagnostics().ok_or("missing batch")?
    ));
    assert!(
        fresh
            .linux_mm
            .diagnostics()
            .ok_or("missing batch")?
            .process_mappings
            .as_ref()
            .ok_or("missing mappings")?
            .mappings
            .is_none()
    );
    Ok(())
}

#[test]
fn invalid_and_oversized_snapshot_budgets_fail_explicitly() -> Result<(), Box<dyn std::error::Error>> {
    use wora::o11y::{LinuxMemoryLimits, RuntimeEnvironment, SystemRuntimeEnvironment};
    assert!(
        SystemRuntimeEnvironment::with_linux_memory_limits(LinuxMemoryLimits {
            snapshot_bytes: 0,
            ..Default::default()
        })
        .is_err()
    );
    let provider = SystemRuntimeEnvironment::with_linux_memory_limits(LinuxMemoryLimits {
        snapshot_bytes: 1,
        ..Default::default()
    })?;
    assert!(provider.initial_host().is_err());
    assert_eq!(provider.memory_sampling_status()?.live_snapshots, 0);
    Ok(())
}

#[derive(Clone)]
struct CapturedEnvironment;

impl wora::o11y::RuntimeEnvironment for CapturedEnvironment {
    fn initial_host(&self) -> Result<wora::o11y::Host, wora::o11y::O11yError> {
        Ok(wora::o11y::Host::from_parts(Default::default(), Default::default()))
    }
    fn initial_process_stats(&self) -> Option<wora::o11y::ProcessStats> {
        None
    }
    fn refresh_host_stats(&self) -> Result<Option<HostStats>, wora::o11y::O11yError> {
        Ok(Some(HostStats::default()))
    }
    fn refresh_process_stats(&self) -> Option<wora::o11y::ProcessStats> {
        None
    }
}

#[test]
fn all_selected_diagnostics_reach_serialized_memory_events() -> Result<(), Box<dyn std::error::Error>> {
    use wora::o11y::linux_mm::PageRange;
    use wora::o11y::{LinuxMemoryEnvironment, LinuxMemoryOptions, RuntimeEnvironment};
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("self"))?;
    for name in ["kpageflags", "kpagecount", "kpagecgroup", "self/pagemap"] {
        std::fs::write(root.path().join(name), 42u64.to_ne_bytes())?;
    }
    let options = LinuxMemoryOptions::all_snapshots()
        .with_roots(root.path(), root.path())
        .with_physical_page_range(PageRange { start: 0, count: 1 })
        .with_virtual_page_range(PageRange { start: 0, count: 1 });
    let provider = LinuxMemoryEnvironment::new(CapturedEnvironment, options)?;
    let stats = provider.initial_host()?.stats;
    let json = serde_json::to_value(wora::events::MemorySnapshot::from_host_stats(&stats))?;
    let diagnostics = &json["linux_mm"]["diagnostics"];
    for field in [
        "process_mappings",
        "slub",
        "kernel_allocations",
        "fragmentation",
        "lru_generations",
        "physical_pages",
        "page_map",
        "page_owner",
        "kernel_leaks",
        "hardware",
        "damon",
        "shrinkers",
    ] {
        assert!(!diagnostics[field].is_null(), "missing {field}");
    }
    assert_eq!(diagnostics["physical_pages"]["flags"][0], 42);
    assert!(diagnostics["slub"]["unavailable"]["."].is_string());
    assert!(
        LinuxMemoryEnvironment::new(
            CapturedEnvironment,
            LinuxMemoryOptions::new().with_physical_page_range(PageRange { start: 0, count: 0 })
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn damon_diagnostics_are_opt_in_cached_and_serialized() -> Result<(), Box<dyn std::error::Error>> {
    use wora::o11y::{LinuxMemoryCollector, LinuxMemoryEnvironment, LinuxMemoryOptions, RuntimeEnvironment};
    let root = tempfile::tempdir()?;
    let parameters = root.path().join("module/damon_stat/parameters");
    std::fs::create_dir_all(&parameters)?;
    std::fs::write(parameters.join("enabled"), "N\n")?;
    std::fs::write(parameters.join("kdamond_pid"), "-1\n")?;
    std::fs::write(parameters.join("memory_idle_ms_percentiles"), vec!["-17"; 101].join(","))?;
    let options = LinuxMemoryOptions::new().with_roots(root.path(), root.path());
    let disabled = LinuxMemoryEnvironment::new(CapturedEnvironment, options.clone())?.initial_host()?;
    assert!(disabled.stats.linux_mm.diagnostics().is_none());
    let provider = LinuxMemoryEnvironment::new(CapturedEnvironment, options.enable(LinuxMemoryCollector::Damon))?;
    let first = provider.initial_host()?.stats;
    std::fs::write(parameters.join("enabled"), "Y\n")?;
    let cached = provider.clone().refresh_host_stats()?.ok_or("no stats")?;
    assert!(std::ptr::eq(
        first.linux_mm.diagnostics().ok_or("no diagnostics")?,
        cached.linux_mm.diagnostics().ok_or("no diagnostics")?,
    ));
    let json = serde_json::to_value(wora::events::MemorySnapshot::from_host_stats(&cached))?;
    let damon = &json["linux_mm"]["diagnostics"]["damon"];
    assert_eq!(damon["enabled"], false);
    assert_eq!(damon["kdamond_pid"], -1);
    assert_eq!(damon["memory_idle_ms_percentiles"][100], -17);
    assert_eq!(std::fs::read_to_string(parameters.join("enabled"))?, "Y\n");
    Ok(())
}

#[test]
fn selected_kmemleak_suspects_reach_memory_events_without_object_bytes() -> Result<(), Box<dyn std::error::Error>> {
    use wora::o11y::{LinuxMemoryCollector, LinuxMemoryEnvironment, LinuxMemoryOptions, RuntimeEnvironment};
    let root = tempfile::tempdir()?;
    let debug = root.path().join("kernel/debug");
    std::fs::create_dir_all(&debug)?;
    std::fs::write(
        debug.join("kmemleak"),
        "unreferenced object 0x0 (size 3):\n  comm \"worker\", pid 9, jiffies 123\n  hex dump (first 3 bytes):\n    61 62 63                                         abc\n  backtrace (crc 1):\n    allocate+0x1/0x2\n",
    )?;
    let provider = LinuxMemoryEnvironment::new(
        CapturedEnvironment,
        LinuxMemoryOptions::new()
            .enable(LinuxMemoryCollector::KernelLeaks)
            .with_roots(root.path(), root.path()),
    )?;
    let stats = provider.initial_host()?.stats;
    let json = serde_json::to_value(wora::events::MemorySnapshot::from_host_stats(&stats))?;
    let suspect = &json["linux_mm"]["diagnostics"]["kernel_leaks"]["files"]["parsed"]["kmemleak"][0];
    assert_eq!(suspect["size_bytes"], 3);
    assert_eq!(suspect["discarded_dump_bytes"], 3);
    assert_eq!(suspect["stack"][0], "allocate+0x1/0x2");
    assert!(!serde_json::to_string(&json)?.contains("61 62 63"));
    Ok(())
}

#[test]
fn shrinker_selection_limits_and_object_counts_reach_memory_events() -> Result<(), Box<dyn std::error::Error>> {
    use wora::o11y::{LinuxMemoryCollector, LinuxMemoryEnvironment, LinuxMemoryOptions, RuntimeEnvironment};
    let root = tempfile::tempdir()?;
    for name in ["a", "b"] {
        let directory = root.path().join("kernel/debug/shrinker").join(name);
        std::fs::create_dir_all(&directory)?;
        std::fs::write(directory.join("count"), "0 23 0\n")?;
        std::fs::write(directory.join("scan"), [0xff])?;
    }
    let mut options = LinuxMemoryOptions::new()
        .enable(LinuxMemoryCollector::Shrinkers)
        .with_roots(root.path(), root.path());
    options.shrinker_count_reads = 1;
    let provider = LinuxMemoryEnvironment::new(CapturedEnvironment, options.clone())?;
    let stats = provider.initial_host()?.stats;
    let json = serde_json::to_value(wora::events::MemorySnapshot::from_host_stats(&stats))?;
    let shrinkers = &json["linux_mm"]["diagnostics"]["shrinkers"];
    assert_eq!(shrinkers["counts"]["a"]["parsed"]["count"][0]["node_counts"][0], 23);
    assert!(shrinkers["counts"].get("b").is_none());
    assert_eq!(json["linux_mm"]["diagnostics"]["collection"]["limit_exceeded"], "files");
    for invalid in [0, 257] {
        options.shrinker_count_reads = invalid;
        assert!(LinuxMemoryEnvironment::new(CapturedEnvironment, options.clone()).is_err());
    }
    Ok(())
}

#[test]
fn diagnostic_selection_caching_and_independent_timestamps() -> Result<(), Box<dyn std::error::Error>> {
    use wora::o11y::{LinuxMemoryCollector, LinuxMemoryEnvironment, LinuxMemoryOptions, RuntimeEnvironment};
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("self"))?;
    std::fs::write(root.path().join("self/smaps"), "1000-3000 rw-p 0 00:00 0\nRss: 12 kB\n")?;
    let options = LinuxMemoryOptions::new()
        .enable(LinuxMemoryCollector::ProcessMappings)
        .with_roots(root.path(), root.path());
    let provider = LinuxMemoryEnvironment::new(CapturedEnvironment, options.clone())?;
    let initial_sample = provider.initial_host()?.stats;
    let initial = initial_sample.linux_mm.diagnostics().ok_or("no diagnostics")?;
    std::fs::write(root.path().join("self/smaps"), "malformed")?;
    let cached_sample = provider.clone().refresh_host_stats()?.ok_or("no stats")?;
    let cached = cached_sample.linux_mm.diagnostics().ok_or("no diagnostics")?;
    assert_eq!(initial.sampled_at, cached.sampled_at);
    assert!(std::ptr::eq(initial, cached));
    assert_eq!(
        cached.process_mappings.as_ref().ok_or("no mappings")?.mappings.as_ref().ok_or("bad mappings")?[0].fields["Rss"]
            .get("value")
            .and_then(wora::o11y::linux_mm::ParsedMemoryValue::as_u64),
        Some(12)
    );
    let fresh = LinuxMemoryEnvironment::new(CapturedEnvironment, options.with_interval(std::time::Duration::from_nanos(1)))?;
    assert!(
        fresh
            .initial_host()?
            .stats
            .linux_mm
            .diagnostics()
            .ok_or("no diagnostics")?
            .process_mappings
            .as_ref()
            .ok_or("no mappings")?
            .mappings
            .is_none()
    );
    assert!(
        LinuxMemoryEnvironment::new(CapturedEnvironment, LinuxMemoryOptions::new())?
            .initial_host()?
            .stats
            .linux_mm
            .diagnostics()
            .is_none()
    );
    assert!(LinuxMemoryEnvironment::new(CapturedEnvironment, LinuxMemoryOptions::new().with_interval(std::time::Duration::ZERO)).is_err());
    Ok(())
}

#[test]
fn diagnostic_batches_are_shared_and_retained_snapshots_survive_refresh() -> Result<(), Box<dyn std::error::Error>> {
    use wora::events::MemorySnapshot;
    use wora::o11y::{LinuxMemoryCollector, LinuxMemoryDiagnostics, LinuxMemoryEnvironment, LinuxMemoryOptions, RuntimeEnvironment};

    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("self"))?;
    let smaps = root.path().join("self/smaps");
    std::fs::write(&smaps, "1000-3000 rw-p 0 00:00 0\nRss: 12 kB\n")?;
    let options = LinuxMemoryOptions::new()
        .enable(LinuxMemoryCollector::ProcessMappings)
        .with_roots(root.path(), root.path())
        .with_interval(std::time::Duration::from_nanos(1));
    let provider = LinuxMemoryEnvironment::new(CapturedEnvironment, options)?;
    let rss = |batch: &LinuxMemoryDiagnostics| {
        batch
            .process_mappings
            .as_ref()
            .and_then(|details| details.mappings.as_ref())
            .and_then(|mappings| mappings.first())
            .and_then(|mapping| mapping.fields.get("Rss"))
            .and_then(|value| value.get("value"))
            .and_then(wora::o11y::linux_mm::ParsedMemoryValue::as_u64)
            .ok_or("missing mapping RSS")
    };

    let retained_with_old = {
        let stats = provider.initial_host()?.stats;
        let initial = stats.linux_mm.diagnostics().ok_or("no diagnostics")?;
        let cloned_stats = stats.clone();
        let snapshot = MemorySnapshot::from_host_stats(&stats);
        let cloned_snapshot = snapshot.clone();
        let O11yEventKind::HostStats(event_stats) = o11y_new_ev_hoststats::<()>(&stats).kind else {
            return Err("expected host stats event".into());
        };
        for diagnostics in [
            &cloned_stats.linux_mm.diagnostics(),
            &snapshot.linux_mm.diagnostics(),
            &cloned_snapshot.linux_mm.diagnostics(),
            &event_stats.linux_mm.diagnostics(),
        ] {
            assert!(std::ptr::eq(initial, diagnostics.ok_or("no cloned diagnostics")?));
        }

        std::fs::write(&smaps, "1000-3000 rw-p 0 00:00 0\nRss: 24 kB\n")?;
        let refreshed = provider.refresh_host_stats()?.ok_or("no refreshed stats")?;
        let next = refreshed.linux_mm.diagnostics().ok_or("no refreshed diagnostics")?;
        assert!(!std::ptr::eq(initial, next));
        assert_eq!(rss(initial)?, 12);
        assert_eq!(rss(snapshot.linux_mm.diagnostics().ok_or("no snapshot diagnostics")?)?, 12);
        assert_eq!(rss(next)?, 24);
        provider.diagnostic_heap_usage()?.retained_bytes
    };
    assert_eq!(provider.diagnostic_sampling_status()?.live_snapshots, 1);
    assert!(provider.diagnostic_heap_usage()?.retained_bytes < retained_with_old);
    Ok(())
}

#[tokio::test]
async fn captured_linux_memory_metrics_reach_host_stats_sinks() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    for (name, contents) in [
        ("buddyinfo", include_str!("../wora-observability-linux-mm/tests/fixtures/arm64-buddyinfo")),
        ("zoneinfo", include_str!("../wora-observability-linux-mm/tests/fixtures/synthetic-zoneinfo")),
        ("swaps", include_str!("../wora-observability-linux-mm/tests/fixtures/synthetic-swaps")),
        ("slabinfo", include_str!("../wora-observability-linux-mm/tests/fixtures/synthetic-slabinfo")),
        (
            "pagetypeinfo",
            include_str!("../wora-observability-linux-mm/tests/fixtures/synthetic-pagetypeinfo"),
        ),
    ] {
        tokio::fs::write(root.path().join(name), contents).await?;
    }
    let meminfo = [
        "MemTotal",
        "MemFree",
        "Buffers",
        "Cached",
        "SwapCached",
        "Active",
        "Inactive",
        "SwapTotal",
        "SwapFree",
        "Dirty",
        "Writeback",
        "Mapped",
        "Slab",
        "Committed_AS",
        "VmallocTotal",
        "VmallocUsed",
        "VmallocChunk",
    ]
    .map(|name| format!("{name}: 16384 kB\n"))
    .concat();
    tokio::fs::write(root.path().join("meminfo"), meminfo).await?;
    tokio::fs::write(root.path().join("vmstat"), "nr_free_pages 1234\nnr_anomalous -2\n").await?;
    tokio::fs::create_dir(root.path().join("pressure")).await?;
    tokio::fs::write(
        root.path().join("pressure/memory"),
        "some avg10=1.00 avg60=2.00 avg300=3.00 total=9007199254740993\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=10\n",
    )
    .await?;
    tokio::fs::create_dir(root.path().join("sysvipc")).await?;
    tokio::fs::write(
        root.path().join("sysvipc/shm"),
        "key shmid perms size cpid lpid nattch uid gid cuid cgid atime dtime ctime rss swap\n1 2 600 8192 10 11 1 0 0 0 0 0 0 0 4096 0\n",
    )
    .await?;
    tokio::fs::write(root.path().join("iomem"), "00001000-00001fff : System RAM\n").await?;
    tokio::fs::create_dir_all(root.path().join("sys/vm")).await?;
    tokio::fs::create_dir_all(root.path().join("sys/kernel")).await?;
    tokio::fs::write(root.path().join("sys/vm/swappiness"), "60\n").await?;
    tokio::fs::write(root.path().join("sys/vm/stat_refresh"), [0xff]).await?;
    tokio::fs::write(root.path().join("sys/kernel/shmall"), "18446744073692774399\n").await?;
    tokio::fs::create_dir_all(root.path().join("sys/fs")).await?;
    tokio::fs::write(root.path().join("sys/fs/dentry-state"), "317916\t300300\t45\t0\t34063\t0\n").await?;
    tokio::fs::create_dir_all(root.path().join("self")).await?;
    tokio::fs::write(root.path().join("self/ksm_stat"), "ksm_rmap_items 0\nksm_merge_any: no\n").await?;
    let cgroup_root = root.path().join("cgroup");
    tokio::fs::create_dir_all(cgroup_root.join("workload")).await?;
    tokio::fs::write(root.path().join("self/cgroup"), "0::/workload\n").await?;
    tokio::fs::write(
        root.path().join("self/mountinfo"),
        format!("10 1 0:20 / {} rw - cgroup2 cgroup rw\n", cgroup_root.display()),
    )
    .await?;
    tokio::fs::write(cgroup_root.join("workload/memory.current"), "9007199254740993\n").await?;
    tokio::fs::write(cgroup_root.join("workload/memory.max"), "max\n").await?;
    tokio::fs::write(cgroup_root.join("memory.max"), "1048576\n").await?;
    tokio::fs::write(
        root.path().join("self/smaps_rollup"),
        "1000-9000 ---p 00000000 00:00 0 [rollup]\nRss: 100 kB\nPss: 60 kB\n",
    )
    .await?;
    let sys_root = root.path().join("sysfs");
    tokio::fs::create_dir_all(sys_root.join("kernel/mm/hugepages/hugepages-2048kB")).await?;
    tokio::fs::write(sys_root.join("kernel/mm/hugepages/hugepages-2048kB/free_hugepages"), "7\n").await?;

    let stats = HostStats {
        linux_mm: LinuxMemoryStats::from_roots(root.path(), &sys_root),
        ..HostStats::default()
    };
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let mut sink = O11yChannelSink::new(sender);
    sink.handle_event(&o11y_new_ev_hoststats::<()>(&stats)).await?;
    let event = receiver.recv().await.ok_or("host stats event missing")?;
    let O11yEventKind::HostStats(snapshot) = event.kind else {
        return Err("expected host stats event".into());
    };
    let mm = &snapshot.linux_mm;
    assert_eq!(mm.meminfo.as_ref().ok_or("meminfo missing")?.mem_total, 16_777_216);
    assert_eq!(mm.vmstat.as_ref().ok_or("vmstat missing")?.0["nr_anomalous"], -2);
    assert_eq!(mm.pressure.as_ref().ok_or("memory pressure missing")?.some.total, 9_007_199_254_740_993);
    assert_eq!(mm.shared_memory.as_ref().ok_or("shared memory missing")?.0[0].rss, 4096);
    assert_eq!(mm.iomem.as_ref().ok_or("iomem missing")?.0.len(), 1);
    assert_eq!(mm.buddyinfo.as_ref().ok_or("buddyinfo missing")?.zones[0].free_blocks[0], 2553);
    assert_eq!(mm.zoneinfo.as_ref().ok_or("zoneinfo missing")?.zones[0].free_pages, 9_007_199_254_740_993);
    assert_eq!(mm.swaps.as_ref().ok_or("swaps missing")?.entries[1].size_kib, 9_007_199_254_741);
    assert_eq!(mm.slabinfo.as_ref().ok_or("slabinfo missing")?.caches[1].objects, u64::MAX);
    assert_eq!(mm.sysctls.parsed["vm.swappiness"].as_u64(), Some(60));
    assert_eq!(mm.sysctls.parsed["kernel.shmall"].as_u64(), Some(18_446_744_073_692_774_399));
    assert_eq!(mm.sysctls.skipped["vm.stat_refresh"], wora::o11y::linux_mm::SysctlSkipReason::ReadSideEffect);
    assert_eq!(
        mm.pagetypeinfo.as_ref().ok_or("pagetypeinfo missing")?.nodes[0].free_blocks[0].counts[1],
        FreeBlockCount::AtLeast(100000)
    );

    let json = serde_json::to_value(&snapshot)?;
    assert_eq!(json["linux_mm"]["zoneinfo"]["zones"][0]["free_pages"], 9_007_199_254_740_993_u64);
    assert_eq!(json["linux_mm"]["slabinfo"]["caches"][1]["objects"], u64::MAX);
    assert_eq!(json["linux_mm"]["swaps"]["entries"][1]["priority"], -2);
    let event = wora::events::Event::<()>::SystemResource(wora::events::SystemResourceStat::MemorySnapshot(std::sync::Arc::new(
        wora::events::MemorySnapshot::from_host_stats(&snapshot),
    )));
    let event_json = serde_json::to_value(event)?;
    let memory = &event_json["SystemResource"]["MemorySnapshot"]["linux_mm"];
    assert_eq!(memory["meminfo"]["mem_total"], 16_777_216);
    assert_eq!(memory["pressure"]["some"]["total"], 9_007_199_254_740_993_u64);
    assert_eq!(memory["vmstat"]["nr_free_pages"], 1234);
    assert_eq!(memory["buddyinfo"]["zones"][0]["free_blocks"][0], 2553);
    assert_eq!(memory["sysctls"]["parsed"]["vm.swappiness"], 60);
    assert!(memory["sysctls"].get("values").is_none());
    assert_eq!(memory["sysctls"]["parsed"]["kernel.shmall"], 18446744073692774399_u64);
    assert_eq!(memory["sysctls"]["parsed"]["fs.dentry-state"]["nr_negative"], 34063);
    assert_eq!(memory["ksm"]["process"]["parsed"]["ksm_stat"]["ksm_rmap_items"], 0);
    assert_eq!(memory["ksm"]["process"]["parsed"]["ksm_stat"]["ksm_merge_any"], false);
    assert_eq!(memory["cgroup"]["current"]["parsed"]["memory.current"], 9_007_199_254_740_993_u64);
    assert_eq!(memory["cgroup"]["current"]["parsed"]["memory.max"], "max");
    assert_eq!(memory["cgroup"]["ancestors"][0]["parsed"]["memory.max"], 1_048_576);
    assert_eq!(memory["hugepages"]["hugetlb"]["parsed"]["hugepages-2048kB/free_hugepages"], 7);
    assert_eq!(memory["process"]["pid"], std::process::id());
    assert_eq!(memory["process"]["smaps_rollup"]["values"]["Pss"]["value"], 60);
    assert_eq!(memory["process"]["smaps_rollup"]["values"]["Pss"]["unit"], "kB");
    Ok(())
}

#[test]
fn missing_or_malformed_sources_do_not_discard_other_metrics() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("buddyinfo"), "malformed")?;
    std::fs::write(root.path().join("swaps"), "Filename Type Size Used Priority\n")?;
    // A directory produces a read error, independently of the test user's privileges.
    std::fs::create_dir(root.path().join("slabinfo"))?;

    let snapshot = LinuxMemoryStats::from_proc_root(root.path());
    assert!(snapshot.buddyinfo.is_none());
    assert!(snapshot.zoneinfo.is_none());
    assert!(snapshot.slabinfo.is_none());
    assert!(snapshot.pagetypeinfo.is_none());
    assert!(snapshot.swaps.as_ref().ok_or("valid empty swap table missing")?.entries.is_empty());

    // A later failed read must not retain an earlier successful snapshot.
    std::fs::remove_file(root.path().join("swaps"))?;
    assert!(LinuxMemoryStats::from_proc_root(root.path()).swaps.is_none());
    Ok(())
}

#[test]
fn host_stats_collect_and_refresh_linux_memory_sources() {
    let mut stats = HostStats::new(&sysinfo::System::new());
    if BuddyInfo::current().is_ok() {
        assert!(stats.linux_mm.buddyinfo.is_some());
    }
    if Swaps::current().is_ok() {
        assert!(stats.linux_mm.swaps.is_some());
    }

    stats.linux_mm = LinuxMemoryStats::default();
    assert!(stats.update().is_ok());
    if BuddyInfo::current().is_ok() {
        assert!(stats.linux_mm.buddyinfo.is_some());
    }
    if Swaps::current().is_ok() {
        assert!(stats.linux_mm.swaps.is_some());
    }
}

#[test]
fn replacement_admission_precedes_build_and_credit_follows_the_last_handle() -> Result<(), Box<dyn std::error::Error>> {
    use wora::o11y::LinuxMemoryData;
    use wora::o11y::linux_mm::{HeapBudget, HeapLimits};
    let probe = HeapBudget::default();
    let sample = LinuxMemoryStats::try_new(&probe, || Ok(LinuxMemoryData::default()))?;
    let required = probe.usage().retained_bytes;
    drop(sample);
    assert_eq!(probe.usage().retained_bytes, 0);
    let budget = HeapBudget::new(HeapLimits {
        retained_bytes: required,
        ..Default::default()
    })?;
    let original = LinuxMemoryStats::try_new(&budget, || Ok(LinuxMemoryData::default()))?;
    let retained = original.clone();
    assert_eq!(budget.usage().retained_bytes, required);
    let mut invoked = false;
    let refused = original.try_rebuild(&budget, |_| {
        invoked = true;
        Ok(LinuxMemoryData::default())
    });
    assert!(refused.is_err());
    assert!(!invoked);
    assert!(original.shares_data_with(&retained));
    assert_eq!(budget.usage().retained_peak_bytes, required);
    drop(original);
    assert_eq!(budget.usage().retained_bytes, required);
    drop(retained);
    assert_eq!(budget.usage().retained_bytes, 0);

    assert!(LinuxMemoryStats::try_new(&budget, || Err(std::io::Error::other("failed builder"))).is_err());
    assert_eq!(budget.usage().retained_bytes, 0);
    assert_eq!(budget.usage().scratch_bytes, 0);
    Ok(())
}

#[test]
fn provider_heap_budgets_are_shared_and_rejected_samples_release_credit() -> Result<(), Box<dyn std::error::Error>> {
    use wora::o11y::linux_mm::HeapLimits;
    use wora::o11y::{LinuxMemoryLimits, RuntimeEnvironment, SystemRuntimeEnvironment};
    assert!(
        SystemRuntimeEnvironment::with_linux_memory_limits(LinuxMemoryLimits {
            heap: HeapLimits {
                scratch_bytes: 0,
                ..Default::default()
            },
            ..Default::default()
        })
        .is_err()
    );
    let provider = SystemRuntimeEnvironment::with_linux_memory_limits(LinuxMemoryLimits {
        heap: HeapLimits {
            retained_bytes: 1,
            ..Default::default()
        },
        ..Default::default()
    })?;
    assert!(provider.initial_host().is_err());
    assert_eq!(provider.memory_heap_usage().retained_bytes, 0);
    assert_eq!(provider.memory_sampling_status()?.live_snapshots, 0);

    let provider = SystemRuntimeEnvironment::with_linux_memory_limits(LinuxMemoryLimits::default())?;
    let first = provider.initial_host()?.stats;
    let before = provider.memory_heap_usage().retained_bytes;
    let cached = provider.clone().initial_host()?.stats;
    assert!(first.linux_mm.shares_data_with(&cached.linux_mm));
    assert_eq!(provider.memory_heap_usage().retained_bytes, before);
    let next = provider.refresh_host_stats()?.ok_or("missing refresh")?;
    assert!(provider.memory_heap_usage().retained_bytes > before);
    assert_eq!(next.linux_mm.heap_usage(), Some(provider.memory_heap_usage()));
    Ok(())
}
