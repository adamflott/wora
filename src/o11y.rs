use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use derive_builder::Builder;
use derive_getters::Getters;
#[cfg(target_os = "linux")]
use procfs;
#[cfg(target_os = "linux")]
use procfs::ProcError;
use serde::Serialize;
use sysinfo::{Disks, Networks, Pid, System};
use thiserror::Error;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tracing::{Id, Level};
use tracing_subscriber::Layer;

use crate::{HealthState, Leadership, ReadinessState};

/// Timestamped observability event emitted by WORA or an application.
#[derive(Debug)]
pub struct O11yEvent<T> {
    /// Event creation time.
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Event payload.
    pub kind: O11yEventKind<T>,
}

/// Build an observability initialization event.
pub fn o11y_new_ev_init<T>(log_dir: PathBuf) -> O11yEvent<T> {
    O11yEvent {
        timestamp: chrono::Utc::now(),
        kind: O11yEventKind::Init(log_dir),
    }
}

/// Build an observability finish event.
pub fn o11y_new_ev_finish<T>() -> O11yEvent<T> {
    O11yEvent {
        timestamp: chrono::Utc::now(),
        kind: O11yEventKind::Finish,
    }
}

/// Build a request to flush observability state.
pub fn o11y_new_ev_flush<T>() -> O11yEvent<T> {
    O11yEvent {
        timestamp: chrono::Utc::now(),
        kind: O11yEventKind::Flush,
    }
}

/// Build a request to clear observability state.
pub fn o11y_new_ev_clear<T>() -> O11yEvent<T> {
    O11yEvent {
        timestamp: chrono::Utc::now(),
        kind: O11yEventKind::Clear,
    }
}

/// Build a request to reconnect observability outputs.
pub fn o11y_new_ev_reconnect<T>() -> O11yEvent<T> {
    O11yEvent {
        timestamp: chrono::Utc::now(),
        kind: O11yEventKind::Reconnect,
    }
}

/// Build a queue status event.
pub fn o11y_new_ev_status<T>(cap: usize, max_cap: usize) -> O11yEvent<T> {
    O11yEvent {
        timestamp: chrono::Utc::now(),
        kind: O11yEventKind::Status(cap, max_cap),
    }
}

/// Build a host information event.
pub fn o11y_new_ev_hostinfo<T>(hi: &HostInfo) -> O11yEvent<T> {
    O11yEvent {
        timestamp: chrono::Utc::now(),
        kind: O11yEventKind::HostInfo(hi.clone()),
    }
}
/// Build a host statistics event.
pub fn o11y_new_ev_hoststats<T>(hs: &HostStats) -> O11yEvent<T> {
    O11yEvent {
        timestamp: chrono::Utc::now(),
        kind: O11yEventKind::HostStats(hs.clone()),
    }
}

/// Build a process statistics event.
pub fn o11y_new_ev_processstats<T>(ps: &ProcessStats) -> O11yEvent<T> {
    O11yEvent {
        timestamp: chrono::Utc::now(),
        kind: O11yEventKind::ProcessStats(ps.clone()),
    }
}

/// Build a runtime metrics event.
pub fn o11y_new_ev_runtime_metrics<T>(rm: &RuntimeMetrics) -> O11yEvent<T> {
    O11yEvent {
        timestamp: chrono::Utc::now(),
        kind: O11yEventKind::RuntimeMetrics(rm.clone()),
    }
}

/// Build a tracing span lifecycle event.
pub fn o11y_new_ev_span<T>(id: tracing::Id, kind: O11ySpanEventKind) -> O11yEvent<T> {
    O11yEvent {
        timestamp: chrono::Utc::now(),
        kind: O11yEventKind::Span(id, kind),
    }
}

/// Build a tracing log event.
pub fn o11y_new_ev_log<T>(lvl: Level, target: String, name: String) -> O11yEvent<T> {
    O11yEvent {
        timestamp: chrono::Utc::now(),
        kind: O11yEventKind::Log(lvl, target, name),
    }
}

/// Build an application-defined observability event.
pub fn o11y_new_ev_app<T>(m: T) -> O11yEvent<T> {
    O11yEvent {
        timestamp: chrono::Utc::now(),
        kind: O11yEventKind::App(m),
    }
}

/// Tracing span lifecycle action.
#[derive(Debug)]
pub enum O11ySpanEventKind {
    /// Span was entered.
    Enter,
    /// Span was exited.
    Exit,
    /// Span was closed.
    Close,
}
/// Observability event payload.
#[derive(Debug)]
pub enum O11yEventKind<T> {
    /// Observability pipeline was initialized with the given log directory.
    Init(PathBuf),
    /// Observability pipeline is finishing.
    Finish,
    /// Flush buffered state.
    Flush,
    /// Clear buffered state.
    Clear,
    /// Reconnect outputs.
    Reconnect,
    /// Queue capacity status.
    Status(usize, usize),

    /// Static host information.
    HostInfo(HostInfo),
    /// Host resource statistics.
    HostStats(HostStats),
    /// Current process resource statistics.
    ProcessStats(ProcessStats),
    /// Runtime state and counters.
    RuntimeMetrics(RuntimeMetrics),

    /// Tracing span event.
    Span(Id, O11ySpanEventKind),
    /// Tracing log event.
    Log(Level, String, String),

    /// Application-defined metric or event.
    App(T),
}

/// Basic metric value representation.
#[derive(Debug)]
pub enum O11yMetricValue {
    /// Monotonic counter.
    Counter(u64),
}

/// Current process resource statistics.
#[derive(Clone, Default, Debug, Serialize)]
pub struct ProcessStats {
    /// Process identifier.
    pub pid: u32,
    /// Resident memory usage in bytes.
    pub memory: u64,
    /// Virtual memory usage in bytes.
    pub virtual_memory: u64,
    /// CPU usage percentage reported by `sysinfo`.
    pub cpu_usage: f32,
    /// Accumulated CPU time in milliseconds.
    pub accumulated_cpu_time: u64,
    /// Process runtime in seconds.
    pub run_time: u64,
    /// Process start time in seconds since epoch.
    pub start_time: u64,
    /// Bytes read since the previous refresh.
    pub read_bytes: u64,
    /// Total bytes read.
    pub total_read_bytes: u64,
    /// Bytes written since the previous refresh.
    pub written_bytes: u64,
    /// Total bytes written.
    pub total_written_bytes: u64,
}

impl ProcessStats {
    /// Collect statistics for the current process.
    pub fn current() -> Option<Self> {
        let mut sys = System::new_all();
        sys.refresh_all();
        Self::from_system(&sys, std::process::id())
    }

    /// Collect statistics for `pid` from `sys`.
    pub fn from_system(sys: &System, pid: u32) -> Option<Self> {
        let process = sys.process(Pid::from_u32(pid))?;
        let disk = process.disk_usage();
        Some(Self {
            pid,
            memory: process.memory(),
            virtual_memory: process.virtual_memory(),
            cpu_usage: process.cpu_usage(),
            accumulated_cpu_time: process.accumulated_cpu_time(),
            run_time: process.run_time(),
            start_time: process.start_time(),
            read_bytes: disk.read_bytes,
            total_read_bytes: disk.total_read_bytes,
            written_bytes: disk.written_bytes,
            total_written_bytes: disk.total_written_bytes,
        })
    }
}

/// Runtime state and counters exported as observability metrics.
#[derive(Clone, Debug, Serialize)]
pub struct RuntimeMetrics {
    /// Stable application name.
    pub app_name: String,
    /// Current process identifier.
    pub pid: u32,
    /// Current leadership role.
    pub leadership: Leadership,
    /// Latest reported health state.
    pub health: HealthState,
    /// Latest reported readiness state.
    pub readiness: ReadinessState,
    /// Number of restarts applied by the runner.
    pub restart_count: u32,
    /// Current remaining event channel capacity.
    pub event_backlog_capacity: usize,
    /// Maximum event channel capacity.
    pub event_backlog_max_capacity: usize,
}

/// Error returned by observability sinks and processors.
#[derive(Debug, Error)]
pub enum O11ySinkError {
    #[error("o11y sink: io")]
    Io(#[from] std::io::Error),
    #[error("o11y sink: serialization")]
    Serialization(#[from] serde_json::Error),
    #[error("o11y sink: mutex poisoned")]
    Poisoned,
}

#[async_trait::async_trait]
/// Sink for processed observability events.
pub trait O11ySink<T>: Send {
    /// Handle a single event.
    async fn handle_event(&mut self, event: &O11yEvent<T>) -> Result<(), O11ySinkError>;

    /// Flush buffered sink state.
    async fn flush(&mut self) -> Result<(), O11ySinkError> {
        Ok(())
    }
}

fn event_kind_name<T>(kind: &O11yEventKind<T>) -> &'static str {
    match kind {
        O11yEventKind::Init(_) => "init",
        O11yEventKind::Finish => "finish",
        O11yEventKind::Flush => "flush",
        O11yEventKind::Clear => "clear",
        O11yEventKind::Reconnect => "reconnect",
        O11yEventKind::Status(_, _) => "status",
        O11yEventKind::HostInfo(_) => "host_info",
        O11yEventKind::HostStats(_) => "host_stats",
        O11yEventKind::ProcessStats(_) => "process_stats",
        O11yEventKind::RuntimeMetrics(_) => "runtime_metrics",
        O11yEventKind::Span(_, _) => "span",
        O11yEventKind::Log(_, _, _) => "log",
        O11yEventKind::App(_) => "app",
    }
}

/// Sink that collects debug-formatted event lines into shared memory.
pub struct O11yMemorySink {
    entries: Arc<Mutex<Vec<String>>>,
}

impl O11yMemorySink {
    /// Create a sink backed by `entries`.
    pub fn new(entries: Arc<Mutex<Vec<String>>>) -> Self {
        Self { entries }
    }
}

#[async_trait::async_trait]
impl<T: Debug + Send + Sync + 'static> O11ySink<T> for O11yMemorySink {
    async fn handle_event(&mut self, event: &O11yEvent<T>) -> Result<(), O11ySinkError> {
        let mut entries = self.entries.lock().map_err(|_| O11ySinkError::Poisoned)?;
        entries.push(format!("{} {} {:?}", event.timestamp.to_rfc3339(), event_kind_name(&event.kind), event.kind));
        Ok(())
    }
}

#[derive(Serialize)]
struct JsonLine<'a> {
    timestamp: String,
    kind: &'static str,
    payload: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    sink: Option<&'a str>,
}

/// Sink that writes events as JSON lines.
pub struct O11yJsonLinesSink {
    path: PathBuf,
    sink_name: Option<String>,
    writer: Option<BufWriter<tokio::fs::File>>,
}

impl O11yJsonLinesSink {
    /// Create a JSON-lines sink writing to `path`.
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            sink_name: None,
            writer: None,
        }
    }

    /// Add a static sink name to emitted records.
    pub fn with_name(mut self, sink_name: impl Into<String>) -> Self {
        self.sink_name = Some(sink_name.into());
        self
    }
}

#[async_trait::async_trait]
impl<T: Debug + Send + Sync + 'static> O11ySink<T> for O11yJsonLinesSink {
    async fn handle_event(&mut self, event: &O11yEvent<T>) -> Result<(), O11ySinkError> {
        let line = JsonLine {
            timestamp: event.timestamp.to_rfc3339(),
            kind: event_kind_name(&event.kind),
            payload: format!("{:?}", event.kind),
            sink: self.sink_name.as_deref(),
        };
        if self.writer.is_none() {
            let file = OpenOptions::new().create(true).append(true).open(&self.path).await?;
            self.writer = Some(BufWriter::new(file));
        }
        let Some(writer) = self.writer.as_mut() else {
            return Err(O11ySinkError::Io(std::io::Error::other("json-lines writer was not initialized")));
        };
        writer.write_all(serde_json::to_string(&line)?.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), O11ySinkError> {
        if let Some(writer) = self.writer.as_mut() {
            writer.flush().await?;
        }
        Ok(())
    }
}

/// Observability processor that fans events out to one or more sinks.
pub struct O11yProcessor<T> {
    sinks: Vec<Box<dyn O11ySink<T>>>,
}

impl<T: Sync> O11yProcessor<T> {
    /// Create a processor with `sinks`.
    pub fn new(sinks: Vec<Box<dyn O11ySink<T>>>) -> Self {
        Self { sinks }
    }

    /// Process a single event.
    pub async fn process_event(&mut self, event: &O11yEvent<T>) -> Result<(), O11ySinkError> {
        for sink in &mut self.sinks {
            sink.handle_event(event).await?;
            if matches!(event.kind, O11yEventKind::Flush | O11yEventKind::Finish) {
                sink.flush().await?;
            }
        }
        Ok(())
    }

    /// Run until `receiver` closes.
    pub async fn run(mut self, mut receiver: Receiver<O11yEvent<T>>) -> Result<(), O11ySinkError> {
        while let Some(event) = receiver.recv().await {
            self.process_event(&event).await?;
        }
        for sink in &mut self.sinks {
            sink.flush().await?;
        }
        Ok(())
    }

    /// Spawn the processor on the Tokio runtime.
    pub fn spawn(self, receiver: Receiver<O11yEvent<T>>) -> JoinHandle<Result<(), O11ySinkError>>
    where
        T: Send + 'static,
    {
        tokio::spawn(self.run(receiver))
    }
}

impl Default for O11yMetricValue {
    fn default() -> Self {
        O11yMetricValue::Counter(0)
    }
}

impl O11yMetricValue {
    /// Increment the metric value by one when supported.
    pub fn inc(&mut self) {
        match self {
            O11yMetricValue::Counter(v) => *v += 1,
        }
    }
}

/// Options used by the runner to schedule and send observability events.
#[derive(Debug, Builder, Getters)]
pub struct O11yProcessorOptions<T> {
    sender: Sender<O11yEvent<T>>,
    flush_interval: std::time::Duration,
    status_interval: std::time::Duration,
    host_stats_interval: std::time::Duration,
}

struct MEVisitor<T>(Level, Sender<O11yEvent<T>>);

impl<T> tracing::field::Visit for MEVisitor<T> {
    fn record_error(&mut self, field: &tracing::field::Field, value: &(dyn std::error::Error + 'static)) {
        let _ = self
            .1
            .try_send(o11y_new_ev_log(self.0, "".to_string(), format!("{} {:?}", field.name(), value)));
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let _ = self
            .1
            .try_send(o11y_new_ev_log(self.0, "".to_string(), format!("{} {:?}", field.name(), value)));
    }
}
/// `tracing_subscriber` layer that forwards spans and events into WORA.
pub struct Observability<T> {
    /// Destination for generated observability events.
    pub tx: Sender<O11yEvent<T>>,
    /// Minimum tracing level to forward.
    pub level: Level,
}

impl<S, T: Send + Sync + 'static> Layer<S> for Observability<T>
where
    S: tracing::Subscriber,
    S: for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_record(&self, _span: &tracing::Id, _values: &tracing::span::Record<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        // TODO println!("span id:{:?} {:?}", span, _values);
    }
    fn on_enter(&self, id: &tracing::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        match ctx.span(id) {
            None => {}
            Some(_span) => {
                let _ = self.tx.try_send(o11y_new_ev_span(id.clone(), O11ySpanEventKind::Enter));
            }
        }
    }
    fn on_exit(&self, id: &tracing::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        match ctx.span(id) {
            None => {}
            Some(_span) => {
                let _ = self.tx.try_send(o11y_new_ev_span(id.clone(), O11ySpanEventKind::Exit));
            }
        }
    }
    fn on_close(&self, id: tracing::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        match ctx.span(&id) {
            None => {}
            Some(_span) => {
                let _ = self.tx.try_send(o11y_new_ev_span(id.clone(), O11ySpanEventKind::Close));
            }
        }
    }
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: tracing_subscriber::layer::Context<'_, S>) {
        let lvl = *event.metadata().level();

        if self.level >= lvl {
            let _ = self
                .tx
                .try_send(o11y_new_ev_log(lvl, event.metadata().target().to_string(), event.metadata().name().to_string()));

            let mut visitor = MEVisitor(lvl, self.tx.clone());
            event.record(&mut visitor);
        }
    }
}

#[derive(Error, Debug)]
#[error(transparent)]
/// Observability setup or collection error.
pub enum O11yError {
    #[cfg(target_os = "linux")]
    #[error("procfs")]
    ProcFs(#[from] ProcError),
    #[error("invalid boot time {0}")]
    InvalidBootTime(u64),
    #[error("unsupported os {0}")]
    UnsupportedOS(String),
}
/// Operating systems recognized by WORA host metadata.
#[derive(Default, Clone, Debug, Serialize)]
pub enum SupportedOSes {
    /// Linux distributions.
    Linux,
    /// macOS.
    OSX,
    /// Unknown operating system.
    #[default]
    Unknown,
}

impl Display for SupportedOSes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SupportedOSes::Linux => {
                write!(f, "linux")
            }
            SupportedOSes::OSX => {
                write!(f, "osx")
            }
            SupportedOSes::Unknown => {
                write!(f, "unknown")
            }
        }
    }
}
#[derive(Clone, Default, Debug, Serialize)]
/// CPU information captured from `sysinfo`.
pub struct Cpu {
    name: String,
    brand: String,
    freq: u64,
    usage: f32,
}

#[derive(Clone, Default, Debug, Serialize)]
/// Memory statistics captured from `sysinfo`.
pub struct MemStats {
    pub total: u64,
    pub free: u64,
    pub used: u64,
}

#[derive(Clone, Default, Debug, Serialize)]
/// Swap statistics captured from `sysinfo`.
pub struct SwapStats {
    pub total: u64,
    pub used: u64,
    pub free: u64,
}

#[derive(Clone, Default, Debug, Serialize)]
/// Load average statistics.
pub struct LoadAvg {
    pub one: f64,
    pub five: f64,
    pub fifteen: f64,
}

#[derive(Clone, Debug, Serialize)]
/// Filesystem disk information.
pub struct Disk {
    pub name: String,
    pub kind: String,
    pub file_system: String,
    pub mount_point: PathBuf,
    pub total_space: u64,
    pub available_space: u64,
    pub is_removable: bool,
}

#[derive(Clone, Debug, Serialize)]
/// Network I/O counters.
pub struct NetIO {
    pub received: u64,
    pub total_received: u64,
    pub transmitted: u64,
    pub total_transmitted: u64,
    pub packets_received: u64,
    pub total_packets_received: u64,
    pub packets_transmitted: u64,
    pub total_packets_transmitted: u64,
    pub errors_on_received: u64,
    pub total_errors_on_received: u64,
    pub errors_on_transmitted: u64,
    pub total_errors_on_transmitted: u64,
}

#[derive(Debug, Getters)]
/// Host metadata and resource snapshot.
pub struct Host {
    sys: System,
    pub info: HostInfo,
    pub stats: HostStats,
}

impl Host {
    /// Collect host information and resource statistics.
    pub fn new() -> Result<Self, O11yError> {
        let mut sys = sysinfo::System::new_all();
        sys.refresh_all();

        let info = HostInfo::new(&sys)?;
        let stats = HostStats::new(&sys);

        Ok(Self { sys, info, stats })
    }

    /// Refresh host information and resource statistics.
    pub fn update(&mut self) -> Result<(), O11yError> {
        self.sys.refresh_all();
        self.info.update(&self.sys)?;
        self.stats.update()?;
        Ok(())
    }
}
/// System stats/information from `sysinfo`
#[derive(Clone, Default, Debug, Serialize, Getters)]
pub struct HostStats {
    pub cpu: Vec<Cpu>,
    pub memory: MemStats,
    pub load: LoadAvg,
    pub swap: SwapStats,
    pub fs: Vec<Disk>,
    pub net_io: HashMap<String, NetIO>,
}

impl HostStats {
    /// Collect host resource statistics from `sysinfo`.
    pub fn new(sys: &System) -> Self {
        let mut cpus = vec![];
        for cpu in sys.cpus() {
            cpus.push(Cpu {
                name: cpu.name().to_string(),
                brand: cpu.brand().to_string(),
                freq: cpu.frequency(),
                usage: cpu.cpu_usage(),
            })
        }
        let mut fs = vec![];
        let disks = Disks::new_with_refreshed_list();
        for disk in &disks {
            fs.push(Disk {
                name: disk.name().to_string_lossy().to_string(),
                kind: format!("{:?}", disk.kind()),
                file_system: disk.file_system().to_string_lossy().to_string(),
                mount_point: disk.mount_point().to_path_buf(),
                total_space: disk.total_space(),
                available_space: disk.available_space(),
                is_removable: disk.is_removable(),
            });
        }

        let mut net_io = HashMap::new();
        let networks = Networks::new_with_refreshed_list();
        for (if_name, net_data) in &networks {
            net_io.insert(
                if_name.to_string(),
                NetIO {
                    received: net_data.received(),
                    total_received: net_data.total_received(),
                    transmitted: net_data.transmitted(),
                    total_transmitted: net_data.total_transmitted(),
                    packets_received: net_data.packets_received(),
                    total_packets_received: net_data.total_packets_received(),
                    packets_transmitted: net_data.packets_transmitted(),
                    total_packets_transmitted: net_data.total_packets_transmitted(),
                    errors_on_received: net_data.errors_on_received(),
                    total_errors_on_received: net_data.total_errors_on_received(),
                    errors_on_transmitted: net_data.errors_on_transmitted(),
                    total_errors_on_transmitted: net_data.total_errors_on_transmitted(),
                },
            );
        }

        let mem_total = sys.total_memory();
        let mem_free = sys.free_memory();
        let mem_used = sys.used_memory();
        let load_avg = System::load_average();
        let swap_total = sys.total_swap();
        let swap_used = sys.used_swap();
        let swap_free = sys.free_swap();

        HostStats {
            cpu: cpus,
            memory: MemStats {
                total: mem_total,
                free: mem_free,
                used: mem_used,
            },
            load: LoadAvg {
                one: load_avg.one,
                five: load_avg.five,
                fifteen: load_avg.fifteen,
            },
            swap: SwapStats {
                total: swap_total,
                used: swap_used,
                free: swap_free,
            },
            fs,
            net_io,
        }
    }

    /// Refresh host resource statistics.
    pub fn update(&mut self) -> Result<(), O11yError> {
        let mut sys = System::new_all();
        sys.refresh_all();
        *self = HostStats::new(&sys);
        Ok(())
    }
}
#[derive(Clone, Default, Debug, Serialize, Getters)]
/// Static and semi-static host information.
pub struct HostInfo {
    pub os_type: SupportedOSes,
    pub os_name: String,
    pub os_version: Option<String>,
    pub kernel_version: Option<String>,
    pub architecture: Option<String>,
    pub hostname: Option<String>,
    pub ncpus: usize,
    pub maxcpus: usize,
    pub boot_time: DateTime<Utc>,
    #[cfg(target_os = "linux")]
    pub boot_kernel_cmd: Option<Vec<String>>,
    #[cfg(target_os = "linux")]
    pub ticks_per_sec: u64,
    #[cfg(target_os = "linux")]
    pub current_process_arp_entries: Vec<procfs::net::ARPEntry>,
    #[cfg(target_os = "linux")]
    pub current_process_routes: Vec<procfs::net::RouteEntry>,
    #[cfg(target_os = "linux")]
    pub current_process_tcp: Vec<procfs::net::TcpNetEntry>,
    #[cfg(target_os = "linux")]
    pub current_process_tcp6: Vec<procfs::net::TcpNetEntry>,
    #[cfg(target_os = "linux")]
    pub current_process_udp: Vec<procfs::net::UdpNetEntry>,
    #[cfg(target_os = "linux")]
    pub current_process_udp6: Vec<procfs::net::UdpNetEntry>,
    #[cfg(target_os = "linux")]
    pub current_process_unix: Vec<procfs::net::UnixNetEntry>,
}

fn os_type() -> Result<SupportedOSes, O11yError> {
    let os_type = match System::distribution_id().as_str() {
        "ubuntu" | "linux" | "nixos" => SupportedOSes::Linux,
        "macos" => SupportedOSes::OSX,
        unsupported => return Err(O11yError::UnsupportedOS(unsupported.to_string())),
    };
    Ok(os_type)
}

impl HostInfo {
    #[cfg(target_os = "linux")]
    /// Collect Linux host information.
    pub fn new(sys: &System) -> Result<Self, O11yError> {
        let os_type = os_type()?;
        let osinfo = os_info::get();
        let boot_time = procfs::boot_time()?.to_utc();
        let boot_kernel_cmd = procfs::cmdline()?;
        let ticks_per_sec = procfs::ticks_per_second();

        let current_process_arp_entries = procfs::net::arp()?;
        let current_process_routes = procfs::net::route()?;
        let current_process_tcp = procfs::net::tcp()?;
        let current_process_tcp6 = procfs::net::tcp6()?;
        let current_process_udp = procfs::net::udp()?;
        let current_process_udp6 = procfs::net::udp6()?;
        let current_process_unix = procfs::net::unix()?;

        Ok(Self {
            os_type,
            os_name: System::distribution_id(),
            os_version: System::os_version(),
            kernel_version: System::kernel_version(),
            architecture: osinfo.architecture().map(|v| v.to_string()),
            hostname: System::host_name(),
            ncpus: sysinfo::System::physical_core_count().unwrap_or(0),
            maxcpus: sys.cpus().len(),
            boot_time,
            boot_kernel_cmd: Some(boot_kernel_cmd),
            ticks_per_sec,
            current_process_arp_entries,
            current_process_routes,
            current_process_tcp,
            current_process_tcp6,
            current_process_udp,
            current_process_udp6,
            current_process_unix,
        })
    }

    #[cfg(target_os = "linux")]
    /// Refresh Linux host information fields that may change at runtime.
    pub fn update(&mut self, sys: &System) -> Result<(), O11yError> {
        self.ncpus = sysinfo::System::physical_core_count().unwrap_or(0);
        self.maxcpus = sys.cpus().len();
        self.ticks_per_sec = procfs::ticks_per_second();
        self.current_process_arp_entries = procfs::net::arp()?;
        self.current_process_routes = procfs::net::route()?;
        self.current_process_tcp = procfs::net::tcp()?;
        self.current_process_tcp6 = procfs::net::tcp6()?;
        self.current_process_udp = procfs::net::udp()?;
        self.current_process_udp6 = procfs::net::udp6()?;
        self.current_process_unix = procfs::net::unix()?;

        Ok(())
    }

    #[cfg(target_os = "macos")]
    /// Collect macOS host information.
    pub fn new(sys: &System) -> Result<Self, O11yError> {
        let os_type = os_type()?;
        let osinfo = os_info::get();

        let boot_time_epoch = sysinfo::System::boot_time();
        let boot_time = DateTime::from_timestamp(boot_time_epoch as i64, 0).ok_or(O11yError::InvalidBootTime(boot_time_epoch))?;

        Ok(Self {
            os_type,
            os_name: System::distribution_id(),
            os_version: System::os_version(),
            kernel_version: System::kernel_version(),
            architecture: osinfo.architecture().map(|v| v.to_string()),
            hostname: System::host_name(),
            ncpus: System::physical_core_count().unwrap_or(0),
            maxcpus: sys.cpus().len(),
            boot_time,
        })
    }

    #[cfg(target_os = "macos")]
    /// Refresh macOS host information fields that may change at runtime.
    pub fn update(&mut self, sys: &System) -> Result<(), O11yError> {
        self.ncpus = System::physical_core_count().unwrap_or(0);
        self.maxcpus = sys.cpus().len();

        Ok(())
    }
}
