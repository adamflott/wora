//! Ready-to-run configurations for common small applications.
//!
//! Profiles combine the existing application lifecycle, executor, and
//! observability pipeline with defaults for a particular workload. Start with
//! [`MemoryReporter::run`], or use [`MemoryReporter::run_with`] to supply the
//! executor, filesystem, and runner policies yourself.

use std::time::Duration;

use async_trait::async_trait;
use serde::Serialize;
use tokio::io::{AsyncWrite, AsyncWriteExt, Stdout};
use tokio::sync::mpsc::Sender;

use crate::errors::{MainEarlyReturn, WoraSetupError};
use crate::events::{ControlEvent, Event};
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
/// The reporting examples use one Tokio worker to reduce per-thread memory
/// overhead. The profile runs on the caller's runtime; choose its worker count
/// when creating that runtime. Periodic synchronous collection runs on a blocking
/// worker.
///
/// Each line has the serialized [`crate::events::MemorySnapshot`] format: a
/// timestamp, memory and swap totals in bytes, and, on Linux, all sources in
/// `LinuxMemoryStats`, including nested zone, node, per-CPU, cache,
/// allocation-order, and memory sysctl data.
/// Linux reports also include the collecting process's cgroup v2 usage and
/// ancestor limits, THP/HugeTLB sysfs views, and process `smaps_rollup` with PID.
/// Source units and page-type lower bounds are preserved. Unavailable sources
/// are `null`; slab and page-type information commonly requires root. Sysctl
/// values use structured scalars and tables, with per-key errors and exclusions.
/// `vm.stat_refresh` and write-only action controls are never read.
/// No privileges are changed by the profile.
/// On macOS, reports contain the portable memory and swap totals.
///
/// ```no_run
/// use wora::profiles::MemoryReporter;
///
/// #[tokio::main(worker_threads = 1)]
/// async fn main() -> Result<(), wora::errors::MainEarlyReturn> {
///     MemoryReporter::new().run().await
/// }
/// ```
pub struct MemoryReporter<W = Stdout> {
    output_byte_limit: usize,
    #[cfg(target_os = "linux")]
    trace: Option<crate::o11y::LinuxMemoryTraceOptions>,
    #[cfg(target_os = "linux")]
    diagnostics: crate::o11y::LinuxMemoryOptions,
    name: &'static str,
    interval: Duration,
    writer: W,
}

impl Default for MemoryReporter {
    fn default() -> Self {
        Self {
            output_byte_limit: 8 * 1024 * 1024,
            #[cfg(target_os = "linux")]
            trace: None,
            #[cfg(target_os = "linux")]
            diagnostics: crate::o11y::LinuxMemoryOptions::default(),
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
    /// Set collection and retained-generation limits for Linux memory reporting.
    /// Applies to diagnostics and, with [`Self::run`], the default routine sampler.
    /// With [`Self::run_with`], configure the caller's telemetry provider separately.
    #[cfg(target_os = "linux")]
    pub fn with_linux_memory_limits(mut self, limits: crate::o11y::LinuxMemoryLimits) -> Self {
        self.diagnostics.limits = limits;
        self
    }

    /// Limit each JSON record, including its newline, to this many bytes.
    ///
    /// Defaults to 8 MiB. Zero is rejected before starting the runner. A record
    /// that exceeds the limit fails the sink without writing a partial record.
    /// The sink reuses one buffer whose requested capacity stays within this
    /// limit; writer-owned buffering is outside this limit.
    pub fn with_output_byte_limit(mut self, limit: usize) -> Self {
        self.output_byte_limit = limit;
        self
    }

    /// Enable runner-owned tracefs memory events as additional JSON-line records.
    /// Creates/configures only a private tracefs instance, requiring privileges.
    #[cfg(target_os = "linux")]
    pub fn with_linux_memory_trace(mut self, options: crate::o11y::LinuxMemoryTraceOptions) -> Self {
        self.trace = Some(options);
        self
    }
    /// Enable selected Linux memory diagnostics with a separate sampling interval.
    ///
    /// Diagnostic data retains its own timestamp between collections. Expensive
    /// diagnostics are disabled by default, including when running as root.
    #[cfg(target_os = "linux")]
    pub fn with_linux_memory_diagnostics(mut self, options: crate::o11y::LinuxMemoryOptions) -> Self {
        self.diagnostics = options;
        self
    }
    /// Set the application name used for directories, locks, and boot state.
    pub fn with_name(mut self, name: &'static str) -> Self {
        self.name = name;
        self
    }

    /// Set the host and process sampling interval (five seconds by default).
    ///
    /// A zero interval is rejected when running the profile. Linux page-type
    /// collection and process rollup page-table walks can be expensive; increase
    /// this interval when appropriate.
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
            output_byte_limit: self.output_byte_limit,
            #[cfg(target_os = "linux")]
            trace: self.trace,
            #[cfg(target_os = "linux")]
            diagnostics: self.diagnostics,
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
        #[cfg(target_os = "linux")]
        let options = RunnerOptions::new().with_runtime_environment(
            crate::o11y::SystemRuntimeEnvironment::with_linux_memory_limits(self.diagnostics.limits).map_err(|error| WoraSetupError::Str(error.to_string()))?,
        );
        #[cfg(not(target_os = "linux"))]
        let options = RunnerOptions::new();
        self.run_with(exec, fs, options).await
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
        if self.output_byte_limit == 0 {
            return Err(WoraSetupError::Str("memory report output byte limit must be nonzero".into()).into());
        }
        #[cfg(target_os = "linux")]
        let options = {
            let environment = crate::o11y::LinuxMemoryEnvironment::new(options.runtime_environment.clone(), self.diagnostics)
                .map_err(|error| WoraSetupError::Str(error.to_string()))?;
            options.with_runtime_environment(environment)
        };
        let pipeline = O11yPipeline::builder()
            .host_stats_interval(self.interval)
            .failure_policy(O11yFailurePolicy::FailFast)
            .sink("memory-report", MemoryReportSink::new(self.writer, self.output_byte_limit))
            .build()
            .map_err(|error| WoraSetupError::Str(error.to_string()))?;
        #[cfg(target_os = "linux")]
        let pipeline = match self.trace {
            Some(options) => pipeline
                .with_linux_memory_trace(options)
                .map_err(|error| WoraSetupError::Str(error.to_string()))?,
            None => pipeline,
        };
        exec_async_runner_with_options(exec, MemoryReportingApp { name: self.name }, fs, pipeline, options).await
    }
}

struct MemoryReportSink<W> {
    writer: W,
    buffer: ReportBuffer,
}

impl<W> MemoryReportSink<W> {
    fn new(writer: W, limit: usize) -> Self {
        Self {
            writer,
            buffer: ReportBuffer { bytes: Vec::new(), limit },
        }
    }
}

struct ReportBuffer {
    bytes: Vec<u8>,
    limit: usize,
}

impl std::io::Write for ReportBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let required = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|required| *required <= self.limit)
            .ok_or_else(|| std::io::Error::other("memory report exceeds output byte limit"))?;
        if required > self.bytes.capacity() {
            let capacity = required.max(self.bytes.capacity().saturating_mul(2)).min(self.limit);
            self.bytes.try_reserve_exact(capacity - self.bytes.len()).map_err(std::io::Error::other)?;
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl ReportBuffer {
    fn serialize(&mut self, value: &impl Serialize) -> Result<(), O11ySinkError> {
        self.bytes.clear();
        let result = (|| {
            serde_json::to_writer(&mut *self, value)?;
            std::io::Write::write_all(self, b"\n")?;
            Ok(())
        })();
        if result.is_err() {
            self.bytes.clear();
        }
        result
    }
}

// Borrow the memory fields so reporting does not construct another owned
// MemorySnapshot. Keep this view's JSON identical to the public snapshot type.
#[derive(Serialize)]
struct MemoryReport<'a> {
    timestamp: &'a chrono::DateTime<chrono::Utc>,
    memory: &'a crate::o11y::MemStats,
    swap: &'a crate::o11y::SwapStats,
    #[cfg(target_os = "linux")]
    linux_mm: &'a crate::o11y::LinuxMemoryStats,
}

#[cfg(target_os = "linux")]
#[derive(Serialize)]
struct MemoryTraceReport<'a> {
    timestamp: &'a chrono::DateTime<chrono::Utc>,
    memory_trace: &'a crate::o11y::linux_mm::MemoryTraceRecord,
}

#[async_trait]
impl<W: AsyncWrite + Unpin + Send> O11ySink<()> for MemoryReportSink<W> {
    async fn handle_event(&mut self, event: &O11yEvent<()>) -> Result<(), O11ySinkError> {
        #[cfg(target_os = "linux")]
        if let O11yEventKind::MemoryTrace(record) = &event.kind {
            self.buffer.serialize(&MemoryTraceReport {
                timestamp: &event.timestamp,
                memory_trace: record,
            })?;
            self.writer.write_all(&self.buffer.bytes).await?;
            self.writer.flush().await?;
            return Ok(());
        }
        if let O11yEventKind::HostStats(stats) = &event.kind {
            let snapshot = MemoryReport {
                timestamp: &event.timestamp,
                memory: &stats.memory,
                swap: &stats.swap,
                #[cfg(target_os = "linux")]
                linux_mm: &stats.linux_mm,
            };
            self.buffer.serialize(&snapshot)?;
            self.writer.write_all(&self.buffer.bytes).await?;
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
    use crate::events::MemorySnapshot;
    use crate::o11y::{HostStats, MemStats, SwapStats, o11y_new_ev_finish};

    #[tokio::test]
    async fn report_limit_includes_newline_and_never_writes_a_partial_record() -> Result<(), Box<dyn std::error::Error>> {
        let event = o11y_new_ev_hoststats::<()>(&HostStats::default());
        let mut reference = MemoryReportSink::new(Vec::new(), 64 * 1024);
        reference.handle_event(&event).await?;
        let size = reference.writer.len();

        let mut exact = MemoryReportSink::new(Vec::new(), size);
        exact.handle_event(&event).await?;
        assert_eq!(exact.writer, reference.writer);
        let allocation = exact.buffer.bytes.as_ptr();
        let capacity = exact.buffer.bytes.capacity();
        for _ in 0..20 {
            exact.handle_event(&event).await?;
        }
        assert_eq!(exact.buffer.bytes.as_ptr(), allocation);
        assert_eq!(exact.buffer.bytes.capacity(), capacity);
        assert!(capacity <= size);

        let mut short = MemoryReportSink::new(Vec::new(), size - 1);
        assert!(short.handle_event(&event).await.is_err());
        assert!(short.writer.is_empty());
        assert!(short.buffer.bytes.is_empty());
        assert!(short.buffer.bytes.capacity() < size);
        Ok(())
    }

    #[test]
    fn report_buffer_accounts_for_json_escaping_and_recovers_after_overflow() -> Result<(), Box<dyn std::error::Error>> {
        let mut buffer = ReportBuffer { bytes: Vec::new(), limit: 9 };
        buffer.serialize(&"\0")?;
        assert_eq!(buffer.bytes, b"\"\\u0000\"\n");
        assert!(buffer.serialize(&"\0\0").is_err());
        assert!(buffer.bytes.is_empty());
        buffer.serialize(&1)?;
        assert_eq!(buffer.bytes, b"1\n");
        assert!(buffer.bytes.capacity() <= 9);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn trace_records_are_flushed_as_distinct_json_lines() -> Result<(), Box<dyn std::error::Error>> {
        let mut sink = MemoryReportSink::new(BufWriter::new(Vec::new()), 8 * 1024 * 1024);
        sink.handle_event(&O11yEvent {
            timestamp: chrono::Utc::now(),
            kind: O11yEventKind::MemoryTrace(Box::new(crate::o11y::linux_mm::MemoryTraceRecord::Lost {
                raw: "CPU:0 [LOST 5 EVENTS]".into(),
            })),
        })
        .await?;
        let value: serde_json::Value = serde_json::from_slice(sink.writer.get_ref())?;
        assert_eq!(value["memory_trace"]["kind"], "lost");
        assert!(value.get("memory").is_none());
        assert!(sink.writer.get_ref().ends_with(b"\n"));
        Ok(())
    }

    #[tokio::test]
    async fn reports_only_memory_and_flushes_each_json_line() -> Result<(), Box<dyn std::error::Error>> {
        let mut sink = MemoryReportSink::new(BufWriter::new(Vec::new()), 8 * 1024 * 1024);
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
        let mut expected = MemorySnapshot::from_host_stats(&stats);
        expected.timestamp = event.timestamp;
        sink.handle_event(&event).await?;
        // Inspect the underlying writer before the pipeline's periodic flush.
        assert!(sink.writer.get_ref().ends_with(b"\n"));
        stats.memory.used = 70;
        sink.handle_event(&o11y_new_ev_hoststats(&stats)).await?;
        let output = std::str::from_utf8(sink.writer.get_ref())?;
        let reports = output.lines().map(serde_json::from_str::<serde_json::Value>).collect::<Result<Vec<_>, _>>()?;
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0], serde_json::to_value(expected)?);
        assert_eq!(reports[0]["timestamp"], serde_json::to_value(event.timestamp)?);
        assert_eq!(reports[0]["memory"]["total"], 100);
        assert_eq!(reports[0]["memory"]["used"], 60);
        assert_eq!(reports[0]["swap"]["used"], 5);
        assert_eq!(reports[1]["memory"]["used"], 70);
        assert!(reports[0].get("cpu").is_none());
        #[cfg(target_os = "linux")]
        {
            assert!(reports[0]["linux_mm"].get("cgroup").is_some());
            assert!(reports[0]["linux_mm"]["meminfo"].is_null());
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn linux_reports_preserve_nested_values_and_source_units() -> Result<(), Box<dyn std::error::Error>> {
        use crate::o11y::LinuxMemoryData;
        use crate::o11y::linux_mm::{
            BuddyInfo, CgroupMemory, CgroupMemoryStats, CgroupMemoryValue, FromRead, HugePageFiles, HugePageStats, MemorySysctls, PageTypeInfo,
            ParsedMemoryValue, ProcessMemoryStats, SlabInfo, SmapsRollup, Swaps, SysctlReadError, SysctlSkipReason, VmStat, ZoneInfo,
        };

        let stats = HostStats {
            linux_mm: LinuxMemoryData {
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
                    parsed: std::collections::BTreeMap::from([
                        ("vm.swappiness".into(), ParsedMemoryValue::Unsigned(60)),
                        (
                            "vm.lowmem_reserve_ratio".into(),
                            ParsedMemoryValue::try_list([256, 256, 32, 0, 0].map(ParsedMemoryValue::Unsigned))?,
                        ),
                        ("kernel.shmall".into(), ParsedMemoryValue::Unsigned(18_446_744_073_692_774_399)),
                    ]),
                    unavailable: std::collections::BTreeMap::from([("vm.mmap_rnd_bits".into(), SysctlReadError::PermissionDenied)]),
                    skipped: std::collections::BTreeMap::from([("vm.stat_refresh".into(), SysctlSkipReason::ReadSideEffect)]),
                },
                cgroup: Box::new(CgroupMemoryStats {
                    current: Some(CgroupMemory {
                        directory: "/captured/cgroup/workload".into(),
                        parsed: std::collections::BTreeMap::from([("memory.max".into(), CgroupMemoryValue::Text("max".into()))]),
                        ..CgroupMemory::default()
                    }),
                    ..CgroupMemoryStats::default()
                }),
                hugepages: Some(Box::new(HugePageStats {
                    hugetlb: HugePageFiles {
                        parsed: std::collections::BTreeMap::from([("hugepages-2048kB/free_hugepages".into(), ParsedMemoryValue::Unsigned(7))]),
                        ..HugePageFiles::default()
                    },
                    ..HugePageStats::default()
                })),
                process: Box::new(ProcessMemoryStats {
                    pid: 123,
                    smaps_rollup: Some(SmapsRollup::from_read(
                        "1000-9000 ---p 00000000 00:00 0 [rollup]\nRss: 100 kB\nPss: 60 kB\n".as_bytes(),
                    )?),
                    ..ProcessMemoryStats::default()
                }),
                ..LinuxMemoryData::default()
            }
            .into(),
            ..HostStats::default()
        };
        let mut sink = MemoryReportSink::new(Vec::new(), 8 * 1024 * 1024);
        let event = o11y_new_ev_hoststats(&stats);
        sink.handle_event(&event).await?;
        let report: serde_json::Value = serde_json::from_slice(&sink.writer)?;
        let mut expected = MemorySnapshot::from_host_stats(&stats);
        expected.timestamp = event.timestamp;
        assert_eq!(report, serde_json::to_value(expected)?);
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
        assert_eq!(mm["sysctls"]["parsed"]["vm.swappiness"], 60);
        assert_eq!(mm["sysctls"]["parsed"]["vm.lowmem_reserve_ratio"], serde_json::json!([256, 256, 32, 0, 0]));
        assert_eq!(mm["sysctls"]["parsed"]["kernel.shmall"], 18_446_744_073_692_774_399_u64);
        assert!(mm["sysctls"].get("values").is_none());
        assert_eq!(mm["sysctls"]["unavailable"]["vm.mmap_rnd_bits"], "permission_denied");
        assert_eq!(mm["sysctls"]["skipped"]["vm.stat_refresh"], "read_side_effect");
        assert_eq!(mm["cgroup"]["current"]["parsed"]["memory.max"], "max");
        assert!(mm["cgroup"]["current"].get("values").is_none());
        assert_eq!(mm["hugepages"]["hugetlb"]["parsed"]["hugepages-2048kB/free_hugepages"], 7);
        assert!(mm["hugepages"]["hugetlb"].get("values").is_none());
        assert_eq!(mm["process"]["pid"], 123);
        assert_eq!(mm["process"]["smaps_rollup"]["values"]["Pss"]["value"], 60);
        assert_eq!(mm["process"]["smaps_rollup"]["values"]["Pss"]["unit"], "kB");
        Ok(())
    }
}
