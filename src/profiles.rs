//! Ready-to-run configurations for common small applications.
//!
//! Profiles combine the existing application lifecycle, executor, and
//! observability pipeline with defaults for a particular workload. Start with
//! [`MemoryReporter::run`], or use [`MemoryReporter::run_with`] to supply the
//! executor, filesystem, and runner policies yourself.

use std::time::Duration;

use async_trait::async_trait;
use tokio::io::{AsyncWrite, AsyncWriteExt, Stdout};
use tokio::sync::mpsc::Sender;

use crate::errors::{MainEarlyReturn, WoraSetupError};
use crate::events::{ControlEvent, Event, MemorySnapshot};
use crate::exec::AsyncExecutor;
use crate::exec_unix::UnixLikeUser;
use crate::lock::LockBackend;
use crate::o11y::{O11yEvent, O11yEventKind, O11yFailurePolicy, O11yPipeline, O11ySink, O11ySinkError, RuntimeEnvironment, o11y_new_ev_hoststats};
use crate::restart_policy::MainRetryAction;
use crate::vfs::{PhysicalVFS, WFS};
use crate::{App, EventLoopAction, NoConfig, NoSecrets, ReadinessState, RunnerOptions, Wora, exec_async_runner_with_options};

/// Memory-reporting profile with complete JSON-lines output.
///
/// Defaults to the `wora-memory-report` application name, a per-user Unix
/// executor, standard output, and a five-second sampling interval. Reports the
/// startup sample and each subsequent host statistics event, flushing each
/// record as it arrives. Output failures stop the runner and return an error.
/// Termination signals use the runner's normal graceful shutdown behavior.
///
/// Each line is a serialized [`MemorySnapshot`]: a timestamp, memory and swap
/// totals in bytes, and, on Linux, all sources in `LinuxMemoryStats`, including
/// nested zone, node, per-CPU, cache, allocation-order, and memory sysctl data.
/// Source units and page-type lower bounds are preserved. Unavailable sources
/// are `null`; slab and page-type information commonly requires root. Sysctl
/// values retain their native text, with per-key access errors and exclusions.
/// `vm.stat_refresh` and write-only action controls are never read.
/// No privileges are changed by the profile.
/// On macOS, reports contain the portable memory and swap totals.
///
/// ```no_run
/// use wora::profiles::MemoryReporter;
///
/// #[tokio::main]
/// async fn main() -> Result<(), wora::errors::MainEarlyReturn> {
///     MemoryReporter::new().run().await
/// }
/// ```
pub struct MemoryReporter<W = Stdout> {
    name: &'static str,
    interval: Duration,
    writer: W,
}

impl Default for MemoryReporter {
    fn default() -> Self {
        Self {
            name: "wora-memory-report",
            interval: Duration::from_secs(5),
            writer: tokio::io::stdout(),
        }
    }
}

impl MemoryReporter {
    /// Create a profile with the documented defaults.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<W> MemoryReporter<W> {
    /// Set the application name used for directories, locks, and boot state.
    pub fn with_name(mut self, name: &'static str) -> Self {
        self.name = name;
        self
    }

    /// Set the host and process sampling interval (five seconds by default).
    ///
    /// A zero interval is rejected when running the profile. Linux page-type
    /// collection can be expensive; increase this interval when appropriate.
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Replace standard output with an asynchronous writer, such as a file.
    ///
    /// The writer is owned by the observability pipeline and flushed after each
    /// report, so output does not wait for the periodic pipeline flush.
    pub fn with_writer<NewW>(self, writer: NewW) -> MemoryReporter<NewW> {
        MemoryReporter {
            name: self.name,
            interval: self.interval,
            writer,
        }
    }
}

impl<W: AsyncWrite + Unpin + Send + 'static> MemoryReporter<W> {
    /// Run with a per-user Unix executor, physical filesystem, and default
    /// runner options. No configuration or secret files are required.
    pub async fn run(self) -> Result<(), MainEarlyReturn> {
        let fs = PhysicalVFS::new();
        let exec = UnixLikeUser::new(self.name, fs.clone()).await?;
        self.run_with(exec, fs, RunnerOptions::new()).await
    }

    /// Run with a caller-supplied executor, filesystem, and runner options.
    ///
    /// This allows service-manager executors, custom signal mapping, and
    /// alternate telemetry providers while retaining the profile's reports.
    /// Construct the executor with the same name supplied to [`Self::with_name`].
    /// The executor must create both watch roots, as for
    /// [`crate::exec_async_runner`]. This method does not install a global tracing
    /// subscriber; applications can configure tracing separately.
    pub async fn run_with<L: LockBackend, R: RuntimeEnvironment>(
        self,
        exec: impl AsyncExecutor<(), ()> + 'static,
        fs: impl WFS + 'static,
        options: RunnerOptions<(), L, R>,
    ) -> Result<(), MainEarlyReturn> {
        let pipeline = O11yPipeline::builder()
            .host_stats_interval(self.interval)
            .failure_policy(O11yFailurePolicy::FailFast)
            .sink("memory-report", MemoryReportSink { writer: self.writer })
            .build()
            .map_err(|error| WoraSetupError::Str(error.to_string()))?;
        exec_async_runner_with_options(exec, MemoryReportingApp { name: self.name }, fs, pipeline, options).await
    }
}

struct MemoryReportSink<W> {
    writer: W,
}

#[async_trait]
impl<W: AsyncWrite + Unpin + Send> O11ySink<()> for MemoryReportSink<W> {
    async fn handle_event(&mut self, event: &O11yEvent<()>) -> Result<(), O11ySinkError> {
        if let O11yEventKind::HostStats(stats) = &event.kind {
            let mut snapshot = MemorySnapshot::from_host_stats(stats);
            snapshot.timestamp = event.timestamp;
            let mut line = serde_json::to_vec(&snapshot)?;
            line.push(b'\n');
            self.writer.write_all(&line).await?;
            self.writer.flush().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), O11ySinkError> {
        self.writer.flush().await?;
        Ok(())
    }
}

struct MemoryReportingApp {
    name: &'static str,
}

#[async_trait]
impl App<(), ()> for MemoryReportingApp {
    type AppConfig = NoConfig;
    type AppSecrets = NoSecrets;
    type Setup = ();

    fn name(&self) -> &'static str {
        self.name
    }

    async fn setup(
        &mut self,
        wora: &Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS + 'static,
        metrics: Sender<O11yEvent<()>>,
        _is_first_boot: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        metrics.send(o11y_new_ev_hoststats(wora.stats_from_start())).await?;
        wora.report_readiness(ReadinessState::Ready);
        Ok(())
    }

    async fn main(
        &mut self,
        wora: &mut Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
    ) -> MainRetryAction {
        match wora
            .run_event_loop(self, fs, |_app, _wora, event| match event {
                Event::Control(ControlEvent::Shutdown(_)) => EventLoopAction::Exit(MainRetryAction::Success),
                _ => EventLoopAction::Continue,
            })
            .await
        {
            Ok(action) => action,
            Err(error) => {
                tracing::error!(%error, "memory reporter reload failed");
                MainRetryAction::UseExitCode(78)
            }
        }
    }

    async fn end(&mut self, _wora: &Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS + 'static, _metrics: Sender<O11yEvent<()>>) {}
}

#[cfg(test)]
mod tests {
    use tokio::io::BufWriter;

    use super::*;
    use crate::o11y::{HostStats, MemStats, SwapStats, o11y_new_ev_finish};

    #[tokio::test]
    async fn reports_only_memory_and_flushes_each_json_line() -> Result<(), Box<dyn std::error::Error>> {
        let mut sink = MemoryReportSink {
            writer: BufWriter::new(Vec::new()),
        };
        sink.handle_event(&o11y_new_ev_finish()).await?;
        assert!(sink.writer.get_ref().is_empty());

        let mut stats = HostStats {
            memory: MemStats {
                total: 100,
                used: 60,
                free: 40,
            },
            swap: SwapStats { total: 20, used: 5, free: 15 },
            ..HostStats::default()
        };
        let event = o11y_new_ev_hoststats(&stats);
        sink.handle_event(&event).await?;
        // Inspect the underlying writer before the pipeline's periodic flush.
        assert!(sink.writer.get_ref().ends_with(b"\n"));
        stats.memory.used = 70;
        sink.handle_event(&o11y_new_ev_hoststats(&stats)).await?;
        let output = std::str::from_utf8(sink.writer.get_ref())?;
        let reports = output.lines().map(serde_json::from_str::<serde_json::Value>).collect::<Result<Vec<_>, _>>()?;
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0]["timestamp"], serde_json::to_value(event.timestamp)?);
        assert_eq!(reports[0]["memory"]["total"], 100);
        assert_eq!(reports[0]["memory"]["used"], 60);
        assert_eq!(reports[0]["swap"]["used"], 5);
        assert_eq!(reports[1]["memory"]["used"], 70);
        assert!(reports[0].get("cpu").is_none());
        #[cfg(target_os = "linux")]
        {
            assert_eq!(reports[0]["linux_mm"].as_object().ok_or("Linux sources missing")?.len(), 11);
            assert!(reports[0]["linux_mm"]["meminfo"].is_null());
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn linux_reports_preserve_nested_values_and_source_units() -> Result<(), Box<dyn std::error::Error>> {
        use crate::o11y::LinuxMemoryStats;
        use crate::o11y::linux_mm::{BuddyInfo, FromRead, MemorySysctls, PageTypeInfo, SlabInfo, Swaps, SysctlReadError, SysctlSkipReason, VmStat, ZoneInfo};

        let stats = HostStats {
            linux_mm: LinuxMemoryStats {
                buddyinfo: Some(BuddyInfo::from_read(
                    include_bytes!("../wora-observability-linux-mm/tests/fixtures/arm64-buddyinfo").as_slice(),
                )?),
                zoneinfo: Some(ZoneInfo::from_read(
                    include_bytes!("../wora-observability-linux-mm/tests/fixtures/synthetic-zoneinfo").as_slice(),
                )?),
                swaps: Some(Swaps::from_read(
                    include_bytes!("../wora-observability-linux-mm/tests/fixtures/synthetic-swaps").as_slice(),
                )?),
                slabinfo: Some(SlabInfo::from_read(
                    include_bytes!("../wora-observability-linux-mm/tests/fixtures/synthetic-slabinfo").as_slice(),
                )?),
                pagetypeinfo: Some(PageTypeInfo::from_read(
                    include_bytes!("../wora-observability-linux-mm/tests/fixtures/synthetic-pagetypeinfo").as_slice(),
                )?),
                vmstat: Some(VmStat(std::collections::HashMap::from([("nr_anomalous".into(), -2)]))),
                sysctls: MemorySysctls {
                    values: std::collections::BTreeMap::from([
                        ("vm.swappiness".into(), "60".into()),
                        ("vm.lowmem_reserve_ratio".into(), "256\t256\t32\t0\t0".into()),
                        ("kernel.shmall".into(), "18446744073692774399".into()),
                    ]),
                    unavailable: std::collections::BTreeMap::from([("vm.mmap_rnd_bits".into(), SysctlReadError::PermissionDenied)]),
                    skipped: std::collections::BTreeMap::from([("vm.stat_refresh".into(), SysctlSkipReason::ReadSideEffect)]),
                },
                ..LinuxMemoryStats::default()
            },
            ..HostStats::default()
        };
        let mut sink = MemoryReportSink { writer: Vec::new() };
        sink.handle_event(&o11y_new_ev_hoststats(&stats)).await?;
        let report: serde_json::Value = serde_json::from_slice(&sink.writer)?;
        let mm = &report["linux_mm"];
        assert_eq!(mm["vmstat"]["nr_anomalous"], -2);
        assert_eq!(mm["buddyinfo"]["zones"][0]["free_blocks"][0], 2553);
        assert_eq!(mm["zoneinfo"]["zones"][0]["free_pages"], 9_007_199_254_740_993_u64);
        assert_eq!(mm["zoneinfo"]["nodes"]["2"]["nr_written"], u64::MAX);
        assert_eq!(mm["zoneinfo"]["zones"][0]["pagesets"][0]["cpu"], 7);
        assert_eq!(mm["zoneinfo"]["zones"][0]["pagesets"][0]["stats"]["high"], 500);
        assert_eq!(mm["zoneinfo"]["zones"][0]["watermarks"]["high_pages"], 30);
        assert_eq!(mm["swaps"]["entries"][1]["size_kib"], 9_007_199_254_741_u64);
        assert_eq!(mm["swaps"]["entries"][1]["priority"], -2);
        assert_eq!(mm["slabinfo"]["caches"][1]["objects"], u64::MAX);
        assert_eq!(mm["pagetypeinfo"]["nodes"][0]["free_blocks"][0]["counts"][1]["at_least"], 100000);
        assert_eq!(mm["sysctls"]["values"]["vm.swappiness"], "60");
        assert_eq!(mm["sysctls"]["values"]["vm.lowmem_reserve_ratio"], "256\t256\t32\t0\t0");
        assert_eq!(mm["sysctls"]["values"]["kernel.shmall"], "18446744073692774399");
        assert_eq!(mm["sysctls"]["unavailable"]["vm.mmap_rnd_bits"], "permission_denied");
        assert_eq!(mm["sysctls"]["skipped"]["vm.stat_refresh"], "read_side_effect");
        Ok(())
    }
}
