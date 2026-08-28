use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
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
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{Id, Level};
use tracing_subscriber::Layer;

use crate::{HealthState, Leadership, ReadinessState};

/// Timestamped observability event emitted by WORA or an application.
#[derive(Clone, Debug)]
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
#[derive(Clone, Debug)]
pub enum O11ySpanEventKind {
    /// Span was entered.
    Enter,
    /// Span was exited.
    Exit,
    /// Span was closed.
    Close,
}
/// Observability event payload.
#[derive(Clone, Debug)]
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

/// Provider for runtime host and process telemetry.
pub trait RuntimeEnvironment: Clone + Send + Sync + 'static {
    /// Collect the initial host snapshot used to build `Wora`.
    fn initial_host(&self) -> Result<Host, O11yError>;

    /// Collect initial process statistics, if available.
    fn initial_process_stats(&self) -> Option<ProcessStats>;

    /// Refresh host statistics, if available.
    fn refresh_host_stats(&self) -> Result<Option<HostStats>, O11yError>;

    /// Refresh process statistics, if available.
    fn refresh_process_stats(&self) -> Option<ProcessStats>;
}

/// Default runtime environment backed by `sysinfo` and host OS APIs.
#[derive(Clone, Debug)]
pub struct SystemRuntimeEnvironment {
    host_sampler: Arc<std::sync::Mutex<Option<Host>>>,
    process_sampler: Arc<std::sync::Mutex<System>>,
}

impl Default for SystemRuntimeEnvironment {
    fn default() -> Self {
        Self {
            host_sampler: Arc::new(std::sync::Mutex::new(None)),
            process_sampler: Arc::new(std::sync::Mutex::new(System::new_all())),
        }
    }
}

impl RuntimeEnvironment for SystemRuntimeEnvironment {
    fn initial_host(&self) -> Result<Host, O11yError> {
        let mut guard = self
            .host_sampler
            .lock()
            .map_err(|_| O11yError::RuntimeEnvironment("host sampler poisoned".to_string()))?;
        if let Some(host) = guard.as_ref() {
            return Ok(Host::from_parts(host.info.clone(), host.stats.clone()));
        }

        let host = Host::new()?;
        let snapshot = Host::from_parts(host.info.clone(), host.stats.clone());
        *guard = Some(host);
        Ok(snapshot)
    }

    fn initial_process_stats(&self) -> Option<ProcessStats> {
        self.refresh_process_stats()
    }

    fn refresh_host_stats(&self) -> Result<Option<HostStats>, O11yError> {
        let mut guard = self
            .host_sampler
            .lock()
            .map_err(|_| O11yError::RuntimeEnvironment("host sampler poisoned".to_string()))?;
        match guard.as_mut() {
            Some(host) => {
                host.update()?;
                Ok(Some(host.stats().clone()))
            }
            None => {
                let host = Host::new()?;
                let stats = host.stats().clone();
                *guard = Some(host);
                Ok(Some(stats))
            }
        }
    }

    fn refresh_process_stats(&self) -> Option<ProcessStats> {
        let mut guard = self.process_sampler.lock().ok()?;
        guard.refresh_all();
        ProcessStats::from_system(&guard, std::process::id())
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
    #[error("o11y sink: io: {0}")]
    Io(#[from] std::io::Error),
    #[error("o11y sink: serialization: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("o11y sink: mutex poisoned")]
    Poisoned,
    /// An exporter-specific operation failed.
    #[error("o11y sink: backend: {0}")]
    Backend(String),
}

/// Behavior when one sink fails while processing an event.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum O11yFailurePolicy {
    /// Disable the failed sink, keep remaining sinks alive, and retain the
    /// error in [`O11yPipelineStatus`].
    #[default]
    Isolate,
    /// Stop the observability processor immediately.
    FailFast,
}

/// Settings that may be changed while an application is running.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct O11yRuntimeSettings {
    /// Periodic sink flush interval.
    pub flush_interval: std::time::Duration,
    /// Queue status reporting interval.
    pub status_interval: std::time::Duration,
    /// Host and process sampling interval.
    pub host_stats_interval: std::time::Duration,
    /// Sink failure behavior.
    pub failure_policy: O11yFailurePolicy,
}

/// Atomic update containing only settings that should change.
#[derive(Clone, Copy, Debug, Default)]
pub struct O11ySettingsPatch {
    flush_interval: Option<std::time::Duration>,
    status_interval: Option<std::time::Duration>,
    host_stats_interval: Option<std::time::Duration>,
    failure_policy: Option<O11yFailurePolicy>,
}

impl O11ySettingsPatch {
    /// Create an empty settings patch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Change the periodic sink flush interval.
    pub fn flush_interval(mut self, value: std::time::Duration) -> Self {
        self.flush_interval = Some(value);
        self
    }

    /// Change the queue status reporting interval.
    pub fn status_interval(mut self, value: std::time::Duration) -> Self {
        self.status_interval = Some(value);
        self
    }

    /// Change the host and process sampling interval.
    pub fn host_stats_interval(mut self, value: std::time::Duration) -> Self {
        self.host_stats_interval = Some(value);
        self
    }

    /// Change sink failure behavior.
    pub fn failure_policy(mut self, value: O11yFailurePolicy) -> Self {
        self.failure_policy = Some(value);
        self
    }

    fn apply(self, settings: &mut O11yRuntimeSettings) -> Result<(), O11yControlError> {
        for (name, value) in [
            ("flush", self.flush_interval),
            ("status", self.status_interval),
            ("host stats", self.host_stats_interval),
        ] {
            if value.is_some_and(|interval| interval.is_zero()) {
                return Err(O11yControlError::InvalidSettings(format!("{name} interval must be greater than zero")));
            }
        }
        if let Some(value) = self.flush_interval {
            settings.flush_interval = value;
        }
        if let Some(value) = self.status_interval {
            settings.status_interval = value;
        }
        if let Some(value) = self.host_stats_interval {
            settings.host_stats_interval = value;
        }
        if let Some(value) = self.failure_policy {
            settings.failure_policy = value;
        }
        Ok(())
    }
}

/// Error returned by runtime observability control operations.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum O11yControlError {
    /// The pipeline has been built but its runner has not started.
    #[error("observability pipeline is not running")]
    NotRunning,
    /// The observability processor is no longer running.
    #[error("observability control channel is closed")]
    Closed,
    /// A requested sink does not exist.
    #[error("observability sink {0:?} was not found")]
    SinkNotFound(String),
    /// A runtime setting is invalid.
    #[error("invalid observability settings: {0}")]
    InvalidSettings(String),
    /// A sink rejected a control operation.
    #[error("observability sink control failed: {0}")]
    Sink(String),
}

/// Runtime state of a named sink.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum O11ySinkState {
    /// The sink is receiving events.
    Enabled,
    /// The sink was disabled through the control handle.
    Disabled,
    /// The sink was isolated after returning an error.
    Failed(String),
}

/// Current state of a named observability sink.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct O11ySinkStatus {
    /// Stable sink name supplied to the pipeline builder.
    pub name: String,
    /// Whether the sink is enabled, disabled, or failed.
    pub state: O11ySinkState,
}

/// Runtime state of a managed observability service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum O11yServiceState {
    /// The pipeline has been built but the runner has not started.
    Pending,
    /// The service future is currently running.
    Running,
    /// The service stopped without returning an error.
    Stopped,
    /// The service returned an error.
    Failed(String),
}

/// Current state of a managed observability service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct O11yServiceStatus {
    /// Stable service name supplied to the pipeline builder.
    pub name: String,
    /// Current lifecycle state.
    pub state: O11yServiceState,
}

/// Snapshot of mutable observability pipeline state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct O11yPipelineStatus {
    /// Current interval and failure-policy settings.
    pub settings: O11yRuntimeSettings,
    /// Status of every registered sink, keyed by stable name.
    pub sinks: HashMap<String, O11ySinkStatus>,
    /// Status of every managed exporter service, keyed by stable name.
    pub services: HashMap<String, O11yServiceStatus>,
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

    /// Clear backend state when supported.
    async fn clear(&mut self) -> Result<(), O11ySinkError> {
        Err(O11ySinkError::Backend("clear is not supported by this sink".to_string()))
    }

    /// Reconnect backend resources when supported.
    async fn reconnect(&mut self) -> Result<(), O11ySinkError> {
        Err(O11ySinkError::Backend("reconnect is not supported by this sink".to_string()))
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

    async fn clear(&mut self) -> Result<(), O11ySinkError> {
        self.entries.lock().map_err(|_| O11ySinkError::Poisoned)?.clear();
        Ok(())
    }
}

/// Sink that writes debug-formatted events to standard output.
#[derive(Default)]
pub struct O11yStdoutSink;

#[async_trait::async_trait]
impl<T: Debug + Send + Sync + 'static> O11ySink<T> for O11yStdoutSink {
    async fn handle_event(&mut self, event: &O11yEvent<T>) -> Result<(), O11ySinkError> {
        println!("{} {} {:?}", event.timestamp.to_rfc3339(), event_kind_name(&event.kind), event.kind);
        Ok(())
    }

    async fn clear(&mut self) -> Result<(), O11ySinkError> {
        Ok(())
    }
}

/// Sink that intentionally discards every event.
#[derive(Default)]
pub struct O11yDiscardSink;

#[async_trait::async_trait]
impl<T: Send + Sync + 'static> O11ySink<T> for O11yDiscardSink {
    async fn handle_event(&mut self, _event: &O11yEvent<T>) -> Result<(), O11ySinkError> {
        Ok(())
    }

    async fn clear(&mut self) -> Result<(), O11ySinkError> {
        Ok(())
    }
}

/// Sink that forwards cloned events to another Tokio channel.
pub struct O11yChannelSink<T> {
    sender: Sender<O11yEvent<T>>,
}

impl<T> O11yChannelSink<T> {
    /// Create a forwarding sink using `sender`.
    pub fn new(sender: Sender<O11yEvent<T>>) -> Self {
        Self { sender }
    }
}

#[async_trait::async_trait]
impl<T: Clone + Send + Sync + 'static> O11ySink<T> for O11yChannelSink<T> {
    async fn handle_event(&mut self, event: &O11yEvent<T>) -> Result<(), O11ySinkError> {
        self.sender
            .send(event.clone())
            .await
            .map_err(|_| O11ySinkError::Backend("observability forwarding channel is closed".to_string()))
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

    async fn clear(&mut self) -> Result<(), O11ySinkError> {
        if let Some(writer) = self.writer.as_mut() {
            writer.flush().await?;
        }
        self.writer = None;
        OpenOptions::new().create(true).write(true).truncate(true).open(&self.path).await?;
        Ok(())
    }
}

/// Observability processor that fans events out to one or more sinks.
pub struct O11yProcessor<T> {
    sinks: Vec<O11ySinkEntry<T>>,
    failure_policy: O11yFailurePolicy,
}

struct O11ySinkEntry<T> {
    name: String,
    state: O11ySinkState,
    sink: Box<dyn O11ySink<T>>,
}

impl<T: Sync> O11yProcessor<T> {
    /// Create a processor with `sinks`.
    pub fn new(sinks: Vec<Box<dyn O11ySink<T>>>) -> Self {
        Self {
            sinks: sinks
                .into_iter()
                .enumerate()
                .map(|(index, sink)| O11ySinkEntry {
                    name: format!("sink-{index}"),
                    state: O11ySinkState::Enabled,
                    sink,
                })
                .collect(),
            failure_policy: O11yFailurePolicy::Isolate,
        }
    }

    fn from_entries(sinks: Vec<O11ySinkEntry<T>>, failure_policy: O11yFailurePolicy) -> Self {
        Self { sinks, failure_policy }
    }

    /// Select how failures from individual sinks are handled.
    pub fn with_failure_policy(mut self, failure_policy: O11yFailurePolicy) -> Self {
        self.failure_policy = failure_policy;
        self
    }

    /// Process a single event.
    pub async fn process_event(&mut self, event: &O11yEvent<T>) -> Result<(), O11ySinkError> {
        self.process_event_isolated(event).await
    }

    /// Run until `receiver` closes.
    pub async fn run(mut self, mut receiver: Receiver<O11yEvent<T>>) -> Result<(), O11ySinkError> {
        while let Some(event) = receiver.recv().await {
            self.process_event_isolated(&event).await?;
            if matches!(event.kind, O11yEventKind::Finish) {
                break;
            }
        }
        for sink in &mut self.sinks {
            if matches!(sink.state, O11ySinkState::Enabled) {
                sink.sink.flush().await?;
            }
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

    pub(crate) async fn run_controlled(
        mut self,
        mut receiver: Receiver<O11yEvent<T>>,
        mut commands: Receiver<O11yCommand>,
        settings_tx: watch::Sender<O11yRuntimeSettings>,
    ) -> Result<(), O11ySinkError> {
        loop {
            tokio::select! {
                event = receiver.recv() => match event {
                    Some(event) => {
                        self.process_event_isolated(&event).await?;
                        if matches!(event.kind, O11yEventKind::Finish) { break; }
                    }
                    None => break,
                },
                command = commands.recv(), if !commands.is_closed() => if let Some(command) = command {
                    self.handle_command(command, &settings_tx).await;
                }
            }
        }
        for sink in &mut self.sinks {
            if matches!(sink.state, O11ySinkState::Enabled) {
                sink.sink.flush().await?;
            }
        }
        Ok(())
    }

    async fn process_event_isolated(&mut self, event: &O11yEvent<T>) -> Result<(), O11ySinkError> {
        for sink in &mut self.sinks {
            if !matches!(sink.state, O11ySinkState::Enabled) {
                continue;
            }
            let result = match &event.kind {
                O11yEventKind::Flush => sink.sink.flush().await,
                O11yEventKind::Clear => sink.sink.clear().await,
                O11yEventKind::Reconnect => sink.sink.reconnect().await,
                _ => match sink.sink.handle_event(event).await {
                    Ok(()) if matches!(event.kind, O11yEventKind::Finish) => sink.sink.flush().await,
                    result => result,
                },
            };
            if let Err(error) = result {
                if self.failure_policy == O11yFailurePolicy::FailFast {
                    return Err(error);
                }
                tracing::error!(sink = %sink.name, %error, "disabling failed observability sink");
                sink.state = O11ySinkState::Failed(error.to_string());
            }
        }
        Ok(())
    }

    async fn handle_command(&mut self, command: O11yCommand, settings_tx: &watch::Sender<O11yRuntimeSettings>) {
        match command {
            O11yCommand::Apply { patch, response } => {
                let mut settings = *settings_tx.borrow();
                let result = patch.apply(&mut settings).map(|()| {
                    self.failure_policy = settings.failure_policy;
                    settings_tx.send_replace(settings);
                    settings
                });
                let _ = response.send(result);
            }
            O11yCommand::Settings { response } => {
                let _ = response.send(Ok(*settings_tx.borrow()));
            }
            O11yCommand::Status { services, response } => {
                let sinks = self
                    .sinks
                    .iter()
                    .map(|sink| {
                        (
                            sink.name.clone(),
                            O11ySinkStatus {
                                name: sink.name.clone(),
                                state: sink.state.clone(),
                            },
                        )
                    })
                    .collect();
                let _ = response.send(Ok(O11yPipelineStatus {
                    settings: *settings_tx.borrow(),
                    sinks,
                    services,
                }));
            }
            O11yCommand::Enable { name, response } => {
                let result = self
                    .sinks
                    .iter_mut()
                    .find(|sink| sink.name == name)
                    .ok_or(O11yControlError::SinkNotFound(name))
                    .map(|sink| sink.state = O11ySinkState::Enabled);
                let _ = response.send(result);
            }
            O11yCommand::Disable { name, response } => {
                let result = self
                    .sinks
                    .iter_mut()
                    .find(|sink| sink.name == name)
                    .ok_or(O11yControlError::SinkNotFound(name))
                    .map(|sink| sink.state = O11ySinkState::Disabled);
                let _ = response.send(result);
            }
            O11yCommand::Flush { response } => {
                let _ = response.send(control_sinks(&mut self.sinks, SinkControl::Flush).await);
            }
            O11yCommand::Clear { response } => {
                let _ = response.send(control_sinks(&mut self.sinks, SinkControl::Clear).await);
            }
            O11yCommand::Reconnect { response } => {
                let _ = response.send(control_sinks(&mut self.sinks, SinkControl::Reconnect).await);
            }
        }
    }
}

enum SinkControl {
    Flush,
    Clear,
    Reconnect,
}

async fn control_sinks<T>(sinks: &mut [O11ySinkEntry<T>], control: SinkControl) -> Result<(), O11yControlError> {
    let mut first_error = None;
    for sink in sinks.iter_mut().filter(|sink| matches!(sink.state, O11ySinkState::Enabled)) {
        let result = match control {
            SinkControl::Flush => sink.sink.flush().await,
            SinkControl::Clear => sink.sink.clear().await,
            SinkControl::Reconnect => sink.sink.reconnect().await,
        };
        if let Err(error) = result {
            first_error.get_or_insert_with(|| O11yControlError::Sink(format!("{}: {error}", sink.name)));
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
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
#[doc(hidden)]
#[derive(Debug, Builder, Getters)]
pub struct O11yProcessorOptions<T> {
    sender: Sender<O11yEvent<T>>,
    flush_interval: std::time::Duration,
    status_interval: std::time::Duration,
    host_stats_interval: std::time::Duration,
}

pub(crate) enum O11yCommand {
    Apply {
        patch: O11ySettingsPatch,
        response: oneshot::Sender<Result<O11yRuntimeSettings, O11yControlError>>,
    },
    Settings {
        response: oneshot::Sender<Result<O11yRuntimeSettings, O11yControlError>>,
    },
    Status {
        services: HashMap<String, O11yServiceStatus>,
        response: oneshot::Sender<Result<O11yPipelineStatus, O11yControlError>>,
    },
    Enable {
        name: String,
        response: oneshot::Sender<Result<(), O11yControlError>>,
    },
    Disable {
        name: String,
        response: oneshot::Sender<Result<(), O11yControlError>>,
    },
    Flush {
        response: oneshot::Sender<Result<(), O11yControlError>>,
    },
    Clear {
        response: oneshot::Sender<Result<(), O11yControlError>>,
    },
    Reconnect {
        response: oneshot::Sender<Result<(), O11yControlError>>,
    },
}

/// Cloneable runtime control plane for an observability pipeline.
///
/// Commands are ordered relative to other commands. Event producers use a
/// separate bounded channel, so callers should await a control operation
/// before emitting events that depend on its new state.
pub struct O11yControlHandle {
    sender: Sender<O11yCommand>,
    started: watch::Receiver<bool>,
    services: Arc<Mutex<HashMap<String, O11yServiceStatus>>>,
}

impl Clone for O11yControlHandle {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            started: self.started.clone(),
            services: self.services.clone(),
        }
    }
}

impl O11yControlHandle {
    fn ensure_running(&self) -> Result<(), O11yControlError> {
        if *self.started.borrow() { Ok(()) } else { Err(O11yControlError::NotRunning) }
    }

    /// Atomically apply a runtime settings patch and return the new settings.
    pub async fn apply(&self, patch: O11ySettingsPatch) -> Result<O11yRuntimeSettings, O11yControlError> {
        self.ensure_running()?;
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(O11yCommand::Apply { patch, response })
            .await
            .map_err(|_| O11yControlError::Closed)?;
        receiver.await.map_err(|_| O11yControlError::Closed)?
    }

    /// Read the processor's current runtime settings.
    pub async fn settings(&self) -> Result<O11yRuntimeSettings, O11yControlError> {
        self.ensure_running()?;
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(O11yCommand::Settings { response })
            .await
            .map_err(|_| O11yControlError::Closed)?;
        receiver.await.map_err(|_| O11yControlError::Closed)?
    }

    /// Return current settings and state for every named sink and service.
    pub async fn status(&self) -> Result<O11yPipelineStatus, O11yControlError> {
        self.ensure_running()?;
        let (response, receiver) = oneshot::channel();
        let services = self.services.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).clone();
        self.sender
            .send(O11yCommand::Status { services, response })
            .await
            .map_err(|_| O11yControlError::Closed)?;
        receiver.await.map_err(|_| O11yControlError::Closed)?
    }

    /// Enable a named sink, including one disabled after an isolated failure.
    pub async fn enable_sink(&self, name: impl Into<String>) -> Result<(), O11yControlError> {
        self.sink_command(name.into(), true).await
    }

    /// Disable a named sink without removing its state.
    pub async fn disable_sink(&self, name: impl Into<String>) -> Result<(), O11yControlError> {
        self.sink_command(name.into(), false).await
    }

    async fn sink_command(&self, name: String, enable: bool) -> Result<(), O11yControlError> {
        self.ensure_running()?;
        let (response, receiver) = oneshot::channel();
        let command = if enable {
            O11yCommand::Enable { name, response }
        } else {
            O11yCommand::Disable { name, response }
        };
        self.sender.send(command).await.map_err(|_| O11yControlError::Closed)?;
        receiver.await.map_err(|_| O11yControlError::Closed)?
    }

    /// Flush every enabled sink.
    pub async fn flush(&self) -> Result<(), O11yControlError> {
        self.simple_command(|response| O11yCommand::Flush { response }).await
    }

    /// Clear backend state for every enabled sink.
    pub async fn clear(&self) -> Result<(), O11yControlError> {
        self.simple_command(|response| O11yCommand::Clear { response }).await
    }

    /// Reconnect every enabled sink.
    pub async fn reconnect(&self) -> Result<(), O11yControlError> {
        self.simple_command(|response| O11yCommand::Reconnect { response }).await
    }

    async fn simple_command(&self, command: impl FnOnce(oneshot::Sender<Result<(), O11yControlError>>) -> O11yCommand) -> Result<(), O11yControlError> {
        self.ensure_running()?;
        let (response, receiver) = oneshot::channel();
        self.sender.send(command(response)).await.map_err(|_| O11yControlError::Closed)?;
        receiver.await.map_err(|_| O11yControlError::Closed)?
    }
}

/// An owned observability pipeline ready to be run with a WORA application.
pub struct O11yPipeline<T> {
    options: O11yProcessorOptions<T>,
    receiver: Receiver<O11yEvent<T>>,
    processor: O11yProcessor<T>,
    services: Vec<O11yService>,
    commands: Receiver<O11yCommand>,
    settings_tx: watch::Sender<O11yRuntimeSettings>,
    settings_rx: watch::Receiver<O11yRuntimeSettings>,
    control: O11yControlHandle,
    started_tx: watch::Sender<bool>,
    service_statuses: Arc<Mutex<HashMap<String, O11yServiceStatus>>>,
}

pub(crate) struct O11yPipelineParts<T> {
    pub options: O11yProcessorOptions<T>,
    pub receiver: Receiver<O11yEvent<T>>,
    pub processor: O11yProcessor<T>,
    pub services: Vec<O11yService>,
    pub commands: Receiver<O11yCommand>,
    pub settings_tx: watch::Sender<O11yRuntimeSettings>,
    pub settings_rx: watch::Receiver<O11yRuntimeSettings>,
    pub started_tx: watch::Sender<bool>,
    pub service_statuses: Arc<Mutex<HashMap<String, O11yServiceStatus>>>,
}

impl<T> O11yPipeline<T> {
    /// Create a pipeline builder with production-friendly defaults.
    pub fn builder() -> O11yPipelineBuilder<T> {
        O11yPipelineBuilder::default()
    }

    /// Clone the event sender for tracing layers or application producers.
    pub fn sender(&self) -> Sender<O11yEvent<T>> {
        self.options.sender.clone()
    }

    /// Create a tracing layer connected to this pipeline.
    pub fn tracing_layer(&self, level: Level) -> Observability<T> {
        Observability { tx: self.sender(), level }
    }

    /// Clone the runtime control handle for storage inside the application.
    pub fn control_handle(&self) -> O11yControlHandle {
        self.control.clone()
    }

    pub(crate) fn into_parts(self) -> O11yPipelineParts<T> {
        O11yPipelineParts {
            options: self.options,
            receiver: self.receiver,
            processor: self.processor,
            services: self.services,
            commands: self.commands,
            settings_tx: self.settings_tx,
            settings_rx: self.settings_rx,
            started_tx: self.started_tx,
            service_statuses: self.service_statuses,
        }
    }
}

pub(crate) struct O11yService {
    pub name: String,
    pub future: Pin<Box<dyn Future<Output = Result<(), String>> + Send>>,
}

/// Builder for a complete observability channel and sink processor.
pub struct O11yPipelineBuilder<T> {
    capacity: usize,
    flush_interval: std::time::Duration,
    status_interval: std::time::Duration,
    host_stats_interval: std::time::Duration,
    failure_policy: O11yFailurePolicy,
    sinks: Vec<(String, Box<dyn O11ySink<T>>)>,
    services: Vec<O11yService>,
}

impl<T> Default for O11yPipelineBuilder<T> {
    fn default() -> Self {
        Self {
            capacity: 64,
            flush_interval: std::time::Duration::from_secs(30),
            status_interval: std::time::Duration::from_secs(30),
            host_stats_interval: std::time::Duration::from_secs(30),
            failure_policy: O11yFailurePolicy::Isolate,
            sinks: Vec::new(),
            services: Vec::new(),
        }
    }
}

impl<T: Sync> O11yPipelineBuilder<T> {
    /// Set the bounded event channel capacity.
    pub fn capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }

    /// Add a sink to the pipeline.
    pub fn sink(mut self, name: impl Into<String>, sink: impl O11ySink<T> + 'static) -> Self {
        self.sinks.push((name.into(), Box::new(sink)));
        self
    }

    /// Add a long-running exporter service whose lifecycle is owned by the runner.
    pub fn service<E, F>(mut self, name: impl Into<String>, service: F) -> Self
    where
        E: std::error::Error,
        F: Future<Output = Result<(), E>> + Send + 'static,
    {
        self.services.push(O11yService {
            name: name.into(),
            future: Box::pin(async move { service.await.map_err(|error| error.to_string()) }),
        });
        self
    }

    /// Set the periodic sink flush interval.
    pub fn flush_interval(mut self, interval: std::time::Duration) -> Self {
        self.flush_interval = interval;
        self
    }

    /// Set the queue status event interval.
    pub fn status_interval(mut self, interval: std::time::Duration) -> Self {
        self.status_interval = interval;
        self
    }

    /// Set the host and process sampling interval.
    pub fn host_stats_interval(mut self, interval: std::time::Duration) -> Self {
        self.host_stats_interval = interval;
        self
    }

    /// Select how failures from individual sinks are handled.
    pub fn failure_policy(mut self, policy: O11yFailurePolicy) -> Self {
        self.failure_policy = policy;
        self
    }

    /// Build the channel, runtime options, and processor as one owned value.
    pub fn build(self) -> Result<O11yPipeline<T>, O11yPipelineBuildError> {
        if self.capacity == 0 {
            return Err(O11yPipelineBuildError::ZeroCapacity);
        }
        if self.sinks.is_empty() {
            return Err(O11yPipelineBuildError::NoSinks);
        }
        for (name, interval) in [
            ("flush", self.flush_interval),
            ("status", self.status_interval),
            ("host stats", self.host_stats_interval),
        ] {
            if interval.is_zero() {
                return Err(O11yPipelineBuildError::ZeroInterval(name));
            }
        }
        let mut service_names = std::collections::HashSet::new();
        for service in &self.services {
            if service.name.trim().is_empty() {
                return Err(O11yPipelineBuildError::InvalidServiceName);
            }
            if !service_names.insert(service.name.clone()) {
                return Err(O11yPipelineBuildError::DuplicateServiceName(service.name.clone()));
            }
        }
        let mut sink_names = std::collections::HashSet::new();
        let mut sinks = Vec::with_capacity(self.sinks.len());
        for (name, sink) in self.sinks {
            if name.trim().is_empty() {
                return Err(O11yPipelineBuildError::InvalidSinkName);
            }
            if !sink_names.insert(name.clone()) {
                return Err(O11yPipelineBuildError::DuplicateSinkName(name));
            }
            sinks.push(O11ySinkEntry {
                name,
                state: O11ySinkState::Enabled,
                sink,
            });
        }
        let (sender, receiver) = tokio::sync::mpsc::channel(self.capacity);
        let settings = O11yRuntimeSettings {
            flush_interval: self.flush_interval,
            status_interval: self.status_interval,
            host_stats_interval: self.host_stats_interval,
            failure_policy: self.failure_policy,
        };
        let (settings_tx, settings_rx) = watch::channel(settings);
        let (started_tx, started) = watch::channel(false);
        let (control_tx, commands) = tokio::sync::mpsc::channel(32);
        let service_statuses = Arc::new(Mutex::new(
            self.services
                .iter()
                .map(|service| {
                    (
                        service.name.clone(),
                        O11yServiceStatus {
                            name: service.name.clone(),
                            state: O11yServiceState::Pending,
                        },
                    )
                })
                .collect(),
        ));
        let options = O11yProcessorOptions {
            sender,
            flush_interval: self.flush_interval,
            status_interval: self.status_interval,
            host_stats_interval: self.host_stats_interval,
        };
        let processor = O11yProcessor::from_entries(sinks, self.failure_policy);
        Ok(O11yPipeline {
            options,
            receiver,
            processor,
            services: self.services,
            commands,
            settings_tx,
            settings_rx,
            control: O11yControlHandle {
                sender: control_tx,
                started,
                services: service_statuses.clone(),
            },
            started_tx,
            service_statuses,
        })
    }
}

/// Invalid owned-pipeline configuration.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum O11yPipelineBuildError {
    /// A bounded Tokio channel cannot have zero capacity.
    #[error("observability channel capacity must be greater than zero")]
    ZeroCapacity,
    /// A pipeline without sinks would silently discard all events.
    #[error("observability pipeline must contain at least one sink")]
    NoSinks,
    /// Periodic runtime tasks require a non-zero interval.
    #[error("observability {0} interval must be greater than zero")]
    ZeroInterval(&'static str),
    /// Sink names are used as stable runtime identifiers and cannot be empty.
    #[error("observability sink name cannot be empty")]
    InvalidSinkName,
    /// Sink names must be unique within a pipeline.
    #[error("duplicate observability sink name: {0}")]
    DuplicateSinkName(String),
    /// Service names are used in supervision errors and cannot be empty.
    #[error("observability service name cannot be empty")]
    InvalidServiceName,
    /// Service names must be unique within a pipeline.
    #[error("duplicate observability service name: {0}")]
    DuplicateServiceName(String),
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
    #[error("runtime environment {0}")]
    RuntimeEnvironment(String),
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
    /// Build a host snapshot from explicit parts.
    pub fn from_parts(info: HostInfo, stats: HostStats) -> Self {
        Self {
            sys: System::new_all(),
            info,
            stats,
        }
    }

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

#[cfg(test)]
mod pipeline_tests {
    use super::*;

    struct FailingSink;

    #[async_trait::async_trait]
    impl O11ySink<()> for FailingSink {
        async fn handle_event(&mut self, _event: &O11yEvent<()>) -> Result<(), O11ySinkError> {
            Err(O11ySinkError::Backend("intentional failure".to_string()))
        }
    }

    #[tokio::test]
    async fn control_handle_updates_settings_and_toggles_named_sink() -> Result<(), Box<dyn std::error::Error>> {
        let entries = Arc::new(Mutex::new(Vec::new()));
        let pipeline = O11yPipeline::<()>::builder()
            .sink("failing", FailingSink)
            .sink("memory", O11yMemorySink::new(entries.clone()))
            .service("exporter", std::future::pending::<Result<(), std::io::Error>>())
            .build()?;
        let control = pipeline.control_handle();
        assert_eq!(control.settings().await, Err(O11yControlError::NotRunning));
        let parts = pipeline.into_parts();
        parts.started_tx.send_replace(true);
        let mut settings_rx = parts.settings_rx;
        let sender = parts.options.sender.clone();
        let task = tokio::spawn(parts.processor.run_controlled(parts.receiver, parts.commands, parts.settings_tx));

        let updated = control
            .apply(
                O11ySettingsPatch::new()
                    .flush_interval(std::time::Duration::from_secs(7))
                    .host_stats_interval(std::time::Duration::from_secs(11))
                    .failure_policy(O11yFailurePolicy::FailFast),
            )
            .await?;
        assert_eq!(updated.flush_interval, std::time::Duration::from_secs(7));
        assert_eq!(updated.host_stats_interval, std::time::Duration::from_secs(11));
        assert_eq!(updated.failure_policy, O11yFailurePolicy::FailFast);
        settings_rx.changed().await?;
        assert_eq!(*settings_rx.borrow(), updated);
        control.apply(O11ySettingsPatch::new().failure_policy(O11yFailurePolicy::Isolate)).await?;

        sender.send(o11y_new_ev_status(1, 2)).await?;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let status = control.status().await?;
        assert!(matches!(status.sinks["failing"].state, O11ySinkState::Failed(_)));
        assert_eq!(status.services["exporter"].state, O11yServiceState::Pending);

        control.disable_sink("memory").await?;
        assert_eq!(control.status().await?.sinks["memory"].state, O11ySinkState::Disabled);
        sender.send(o11y_new_ev_status(1, 2)).await?;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        control.enable_sink("memory").await?;
        assert_eq!(control.status().await?.sinks["memory"].state, O11ySinkState::Enabled);
        sender.send(o11y_new_ev_status(1, 2)).await?;
        control.flush().await?;
        sender.send(o11y_new_ev_finish()).await?;
        task.await??;

        assert_eq!(entries.lock().map_err(|error| std::io::Error::other(error.to_string()))?.len(), 3);
        Ok(())
    }

    #[test]
    fn settings_patch_rejects_zero_intervals_atomically() {
        let mut settings = O11yRuntimeSettings {
            flush_interval: std::time::Duration::from_secs(1),
            status_interval: std::time::Duration::from_secs(2),
            host_stats_interval: std::time::Duration::from_secs(3),
            failure_policy: O11yFailurePolicy::Isolate,
        };
        let original = settings;
        let result = O11ySettingsPatch::new()
            .flush_interval(std::time::Duration::from_secs(9))
            .status_interval(std::time::Duration::ZERO)
            .apply(&mut settings);
        assert!(matches!(result, Err(O11yControlError::InvalidSettings(_))));
        assert_eq!(settings, original);
    }
}
