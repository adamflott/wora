//! Write Once Run Anywhere (WORA)
//!
//! A framework for building applications (daemons, etc.) that run in different environments (Linux, Kubernetes, etc.).
//!
//! Just like Java's claim, it really doesn't run everywhere. no_std or embedded environments are not supported.

use std::fmt::Debug;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use async_trait::async_trait;
use chrono::Utc;
use nix::unistd::getpid;
use serde::Serialize;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::Instrument;
use tracing::{debug, error, info, trace, warn};

mod boot;
pub mod dirs;
pub mod errors;
pub mod events;
pub mod exec;
pub mod exec_env;
pub mod exec_unix;
pub mod lock;
pub mod o11y;
pub mod prelude;
pub mod restart_policy;
pub mod vfs;

use crate::boot::{default_boot_root, resolve_boot_state};
use crate::dirs::Dirs;
use crate::errors::{MainEarlyReturn, ReloadError, SetupFailure, VfsError, WoraSetupError};
use crate::events::*;
use crate::exec::*;
use crate::lock::{LockBackend, LockError, ProcLockBackend};
use crate::o11y::*;
use crate::restart_policy::*;
use crate::vfs::*;

const EVENT_BUFFER_SIZE: usize = 1024;

/// Runtime context passed to applications.
///
/// `Wora` gives app lifecycle hooks access to executor directories, host
/// information, event channels, leadership state, and observability settings.
pub struct Wora<AppEv, AppMetric> {
    /// application name
    pub app_name: String,
    /// common directories for all executors
    pub dirs: Dirs,
    /// current directory where the process was invoked (an executor will likely override this)
    pub initial_working_dir: PathBuf,
    /// host info/stats
    pub host: Host,
    /// process id
    pid: nix::unistd::Pid,
    /// `Event` producer
    pub sender: Sender<Event<AppEv>>,
    /// `Event` receiver
    pub receiver: Receiver<Event<AppEv>>,
    /// leadership state
    pub leadership: Leadership,
    /// Observability channel and scheduling options.
    pub o11y: O11yProcessorOptions<AppMetric>,
    /// Shared runtime status for health and readiness supervision.
    status: RuntimeStatusHandle,
}

/// Current leadership role for a workload.
#[derive(Clone, Debug, Serialize)]
pub enum Leadership {
    /// This process is currently the active leader.
    Leader,
    /// This process is currently a follower.
    Follower,
    /// Leadership has not been established.
    Unknown,
}

/// Health state reported by an application.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum HealthState {
    /// The application is healthy.
    Ok,
    /// The application is intentionally suspended.
    Suspended,
    /// Health could not be determined yet and should be retried.
    TryAgain,
    /// The application is unhealthy.
    Failed,
    /// Health has not been reported.
    Unknown,
}

/// Readiness state reported by an application.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum ReadinessState {
    /// The application is ready to serve.
    Ready,
    /// The application is still warming up or otherwise not ready.
    NotReady,
    /// The application is draining existing work and should stop receiving new work.
    Draining,
    /// The application is stopping and should no longer be considered ready.
    Stopping,
    /// Readiness has not been reported.
    Unknown,
}

/// Cloneable handle used by apps and background tasks to report health and readiness.
#[derive(Clone, Debug)]
pub struct RuntimeStatusHandle {
    health: watch::Sender<HealthState>,
    readiness: watch::Sender<ReadinessState>,
}

impl RuntimeStatusHandle {
    fn new() -> Self {
        let (health, _) = watch::channel(HealthState::Unknown);
        let (readiness, _) = watch::channel(ReadinessState::Ready);
        Self { health, readiness }
    }

    /// Report a new health state.
    pub fn report_health(&self, state: HealthState) {
        let _ = self.health.send(state);
    }

    /// Report a new readiness state.
    pub fn report_readiness(&self, state: ReadinessState) {
        let _ = self.readiness.send(state);
    }

    /// Transition the runtime into a draining state.
    pub fn begin_draining(&self) {
        self.report_readiness(ReadinessState::Draining);
    }

    /// Transition the runtime into a stopping state.
    pub fn begin_stopping(&self) {
        self.report_readiness(ReadinessState::Stopping);
    }

    /// Subscribe to health state changes.
    pub fn subscribe_health(&self) -> watch::Receiver<HealthState> {
        self.health.subscribe()
    }

    /// Subscribe to readiness state changes.
    pub fn subscribe_readiness(&self) -> watch::Receiver<ReadinessState> {
        self.readiness.subscribe()
    }

    /// Return the latest reported health state.
    pub fn health_state(&self) -> HealthState {
        self.health.borrow().clone()
    }

    /// Return the latest reported readiness state.
    pub fn readiness_state(&self) -> ReadinessState {
        self.readiness.borrow().clone()
    }
}

/// WORA API
impl<AppEv: Send + Sync + 'static, AppMetric: Send + Sync + 'static> Wora<AppEv, AppMetric> {
    /// Create a new runtime context.
    pub fn new(dirs: &Dirs, app_name: String, ev_buf_size: usize, o11y: O11yProcessorOptions<AppMetric>) -> Result<Wora<AppEv, AppMetric>, WoraSetupError> {
        Self::new_with_runtime_environment(dirs, app_name, ev_buf_size, o11y, SystemRuntimeEnvironment::default())
    }

    /// Create a new runtime context with an explicit runtime environment.
    pub fn new_with_runtime_environment<R: RuntimeEnvironment>(
        dirs: &Dirs,
        app_name: String,
        ev_buf_size: usize,
        o11y: O11yProcessorOptions<AppMetric>,
        runtime_environment: R,
    ) -> Result<Wora<AppEv, AppMetric>, WoraSetupError> {
        let pid = getpid();

        let (tx, rx) = channel(ev_buf_size);

        let host = runtime_environment.initial_host()?;
        let status = RuntimeStatusHandle::new();

        let current_dir = std::env::current_dir()?;

        Ok(Wora {
            app_name,
            initial_working_dir: current_dir,
            dirs: dirs.clone(),
            host,
            pid,
            sender: tx,
            receiver: rx,
            leadership: Leadership::Unknown,
            o11y,
            status,
        })
    }

    /// Return the host statistics captured when the runtime started.
    pub fn stats_from_start(&self) -> &HostStats {
        &self.host.stats
    }

    /// Return the executor directory layout.
    pub fn dirs(&self) -> &Dirs {
        &self.dirs
    }

    /// Emit an event onto the application event channel.
    pub async fn emit_event(&self, ev: Event<AppEv>) {
        match self.sender.send(ev).await {
            Ok(_) => {
                debug!("event:sent");
            }
            Err(send_err) => {
                error!("event:send:error: {:?}", send_err);
            }
        }
    }

    /// Return the host operating system identifier.
    pub fn host_os_name(&self) -> &str {
        &self.host.info.os_name
    }
    /// Return the host operating system version, when available.
    pub fn host_os_version(&self) -> Option<String> {
        self.host.info.os_version.clone()
    }
    /// Return the host architecture, when available.
    pub fn host_architecture(&self) -> Option<String> {
        self.host.info.architecture.clone()
    }
    /// Return the host name, when available.
    pub fn host_hostname(&self) -> Option<String> {
        self.host.info.hostname.clone()
    }
    /// Return the number of physical CPU cores detected at startup.
    pub fn host_cpu_count(&self) -> usize {
        self.host.info.ncpus
    }
    /// Return the number of logical CPUs detected at startup.
    pub fn host_cpu_max(&self) -> usize {
        self.host.info.maxcpus
    }

    /// Return the shared runtime status handle.
    ///
    /// Applications and background tasks should use this handle, or the
    /// convenience methods on `Wora`, to report health and readiness changes to
    /// the runtime supervisor.
    pub fn status_handle(&self) -> RuntimeStatusHandle {
        self.status.clone()
    }

    /// Report application health to the runtime supervisor.
    pub fn report_health(&self, state: HealthState) {
        self.status.report_health(state);
    }

    /// Report application readiness to the runtime supervisor.
    pub fn report_readiness(&self, state: ReadinessState) {
        self.status.report_readiness(state);
    }

    /// Mark the runtime as draining and no longer willing to accept new work.
    pub fn begin_draining(&self) {
        self.status.begin_draining();
    }

    /// Mark the runtime as stopping.
    pub fn begin_stopping(&self) {
        self.status.begin_stopping();
    }

    /// Return the latest application health state known to the runtime.
    pub fn health_state(&self) -> HealthState {
        self.status.health_state()
    }

    /// Return the latest application readiness state known to the runtime.
    pub fn readiness_state(&self) -> ReadinessState {
        self.status.readiness_state()
    }

    /// Apply a typed config or secret reload event to `app`.
    ///
    /// This helper is intended to be called from `App::main` when the app
    /// receives `Event::ConfigChanged` or `Event::SecretChanged`.
    pub async fn apply_reload_event<A>(&self, app: &mut A, fs: impl WFS + 'static, event: &Event<AppEv>) -> Result<ReloadHandling, ReloadError>
    where
        A: App<AppEv, AppMetric> + Send,
    {
        match event {
            Event::ConfigChanged(change) => {
                let changed_paths = (!change.paths.is_empty()).then_some(change.paths.as_slice());
                let reload = load_config_reload::<A::AppConfig>(&self.dirs.metadata_root_dir, app.name(), fs, changed_paths).await?;
                app.reload_config(reload)
                    .await
                    .map_err(|err| ReloadError::Message(format!("failed to apply config reload for {}: {}", app.name(), err)))?;
                Ok(ReloadHandling::ConfigApplied)
            }
            Event::SecretChanged(change) => {
                let changed_paths = (!change.paths.is_empty()).then_some(change.paths.as_slice());
                let reload = load_secret_reload::<A::AppSecrets>(&self.dirs.secrets_root_dir, fs, changed_paths).await?;
                app.reload_secrets(reload)
                    .await
                    .map_err(|err| ReloadError::Message(format!("failed to apply secret reload for {}: {}", app.name(), err)))?;
                Ok(ReloadHandling::SecretsApplied)
            }
            _ => Ok(ReloadHandling::NotHandled),
        }
    }

    /// Run a reload-aware event loop for event-driven applications.
    ///
    /// Typed config and secret reloads are applied before `handler` receives
    /// the event.
    pub async fn run_event_loop<A, F>(&mut self, app: &mut A, fs: impl WFS + 'static, mut handler: F) -> Result<MainRetryAction, ReloadError>
    where
        A: App<AppEv, AppMetric> + Send,
        F: FnMut(&mut A, &mut Wora<AppEv, AppMetric>, Event<AppEv>) -> EventLoopAction + Send,
    {
        while let Some(event) = self.receiver.recv().await {
            self.apply_reload_event(app, fs.clone(), &event).await?;
            match handler(app, self, event) {
                EventLoopAction::Continue => {}
                EventLoopAction::Exit(action) => return Ok(action),
            }
        }

        Ok(MainRetryAction::Success)
    }

    /// Sleep for `duration`, then emit `ev`.
    pub async fn schedule_event(&self, duration: tokio::time::Duration, ev: Event<AppEv>) {
        tokio::time::sleep(duration).await;
        self.emit_event(ev).await
    }

    /// Spawn a periodic background task.
    ///
    /// The task receives a clone of the observability sender. Returning
    /// `TaskOp::Requeue` schedules the next run; returning `TaskOp::Abort`
    /// stops the loop.
    pub async fn schedule_task<F, Fut>(&self, duration: tokio::time::Duration, future: F) -> JoinHandle<TaskOp>
    where
        F: Fn(Sender<O11yEvent<AppMetric>>) -> Fut + Send + 'static,
        Fut: Future<Output = TaskOp> + Send,
    {
        let tx = self.o11y.sender().clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(duration).await;
                match future(tx.clone()).await {
                    TaskOp::Requeue => {
                        trace!("wora:task action:requeue");
                    }
                    TaskOp::Abort => {
                        info!("wora:task action:abort");
                        return TaskOp::Abort;
                    }
                }
            }
        })
    }
}

/// Result from a scheduled background task.
#[derive(Debug, Clone)]
pub enum TaskOp {
    /// Continue scheduling the task.
    Requeue,
    /// Stop scheduling the task.
    Abort,
}

/// Application configuration parser.
pub trait Config {
    /// Parsed configuration type used by the application.
    type ConfigT: Default + Send + 'static;
    /// Parse the main configuration file.
    fn parse_main_config_file(data: String) -> Result<Self::ConfigT, Box<dyn std::error::Error>>;

    /// Parse a supplemental configuration file.
    fn parse_supplemental_config_file(_file_path: PathBuf, _data: String) -> Result<Self::ConfigT, Box<dyn std::error::Error>> {
        Ok(Self::ConfigT::default())
    }
}

/// Typed configuration file payload.
#[derive(Clone, Debug)]
pub struct ConfigFile<T> {
    /// Source file path.
    pub path: PathBuf,
    /// Parsed configuration value.
    pub value: T,
}

/// Typed configuration reload payload.
#[derive(Clone, Debug)]
pub struct ConfigReload<T> {
    /// Parsed main config, if present in the reload set.
    pub main: Option<T>,
    /// Parsed supplemental config files.
    pub supplemental: Vec<ConfigFile<T>>,
}

impl<T> Default for ConfigReload<T> {
    fn default() -> Self {
        Self {
            main: None,
            supplemental: Vec::new(),
        }
    }
}

/// Typed secret file payload.
#[derive(Clone, Debug)]
pub struct SecretFile<T> {
    /// Source file path.
    pub path: PathBuf,
    /// Secret key, usually derived from the filename.
    pub key: String,
    /// Parsed secret value.
    pub value: T,
}

/// Typed secret reload payload.
#[derive(Clone, Debug)]
pub struct SecretReload<T> {
    /// Parsed secret files included in the reload set.
    pub files: Vec<SecretFile<T>>,
}

impl<T> Default for SecretReload<T> {
    fn default() -> Self {
        Self { files: Vec::new() }
    }
}

/// Secret parser used for initial secret load and runtime reload handling.
pub trait Secrets {
    /// Parsed secret value type used by the application.
    type SecretT: Send + 'static;

    /// Parse a secret file.
    fn parse_secret_file(file_path: PathBuf, data: Vec<u8>) -> Result<Self::SecretT, Box<dyn std::error::Error>>;
}

/// Empty secret implementation for apps that do not use secrets.
pub struct NoSecrets;
impl Secrets for NoSecrets {
    type SecretT = ();

    fn parse_secret_file(_file_path: PathBuf, _data: Vec<u8>) -> Result<Self::SecretT, Box<dyn std::error::Error>> {
        Ok(())
    }
}

/// Result from applying a typed runtime reload helper.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReloadHandling {
    /// The event was not a typed reload event.
    NotHandled,
    /// A typed config reload was applied.
    ConfigApplied,
    /// A typed secret reload was applied.
    SecretsApplied,
}

/// Control flow returned by `Wora::run_event_loop`.
#[derive(Clone, Debug)]
pub enum EventLoopAction {
    /// Keep processing events.
    Continue,
    /// Exit the loop with the given runner action.
    Exit(MainRetryAction),
}

/// Empty configuration implementation for apps that do not use config files.
pub struct NoConfig;
impl Config for NoConfig {
    type ConfigT = ();
    fn parse_main_config_file(_data: String) -> Result<Self::ConfigT, Box<dyn std::error::Error>> {
        Ok(())
    }
}
#[async_trait]
/// Application lifecycle implemented by WORA workloads.
///
/// Runtime health and readiness supervision is driven through
/// [`Wora::report_health`], [`Wora::report_readiness`], or a cloned
/// [`RuntimeStatusHandle`], not by polling an app callback.
pub trait App<AppEv: Send + Sync + 'static, AppMetric: Send + Sync + 'static> {
    /// Configuration parser for the app.
    type AppConfig: Config;
    /// Secret parser for the app.
    type AppSecrets: Secrets;
    /// Setup artifact type returned by `setup`.
    type Setup;
    /// Stable application name used for directories, locks, and logging.
    fn name(&self) -> &'static str;

    /// Return whether multiple instances may run concurrently.
    fn allow_concurrent_executions(&self) -> bool {
        false
    }

    /// Apply an initially loaded application configuration.
    ///
    /// For event-driven apps, runtime config reloads usually flow through
    /// `reload_config` instead of this hook directly.
    async fn configure(&mut self, _config: <Self::AppConfig as Config>::ConfigT) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    /// Apply a typed configuration reload.
    async fn reload_config(&mut self, reload: ConfigReload<<Self::AppConfig as Config>::ConfigT>) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(config) = reload.main {
            self.configure(config).await?;
        }
        Ok(())
    }

    /// Apply a typed secret reload.
    async fn reload_secrets(&mut self, _reload: SecretReload<<Self::AppSecrets as Secrets>::SecretT>) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    /// Initialize application state after executor setup and before `main`.
    async fn setup(
        &mut self,
        wora: &Wora<AppEv, AppMetric>,
        exec: impl AsyncExecutor<AppEv, AppMetric>,
        fs: impl WFS + 'static,
        metrics: Sender<O11yEvent<AppMetric>>,
        is_first_boot: bool,
    ) -> Result<Self::Setup, Box<dyn std::error::Error>>;

    /// Run the primary application loop.
    async fn main(
        &mut self,
        wora: &mut Wora<AppEv, AppMetric>,
        exec: impl AsyncExecutor<AppEv, AppMetric>,
        fs: impl WFS + 'static,
        metrics: Sender<O11yEvent<AppMetric>>,
    ) -> MainRetryAction;

    /// Clean up application state after `main` returns.
    async fn end(
        &mut self,
        wora: &Wora<AppEv, AppMetric>,
        exec: impl AsyncExecutor<AppEv, AppMetric>,
        fs: impl WFS + 'static,
        metrics: Sender<O11yEvent<AppMetric>>,
    );
}

async fn configure_app_from_metadata<AppEv, AppMetric, A>(app: &mut A, wora: &Wora<AppEv, AppMetric>, fs: impl WFS + 'static) -> Result<(), MainEarlyReturn>
where
    AppEv: Send + Sync + 'static,
    AppMetric: Send + Sync + 'static,
    A: App<AppEv, AppMetric> + Send,
{
    match load_config_reload::<A::AppConfig>(&wora.dirs.metadata_root_dir, app.name(), fs.clone(), None).await {
        Ok(reload) => {
            app.reload_config(reload)
                .instrument(tracing::info_span!("app:run:configure"))
                .await
                .map_err(|err| WoraSetupError::Str(format!("failed to apply config {}: {}", wora.dirs.metadata_root_dir.display(), err)))?;
            Ok(())
        }
        Err(ReloadError::Vfs(err)) => {
            debug!("config:init:skip path:{} error:{}", wora.dirs.metadata_root_dir.display(), err);
            Ok(())
        }
        Err(err) => Err(WoraSetupError::Str(err.to_string()).into()),
    }
}

async fn load_initial_secrets<AppEv, AppMetric, A>(app: &mut A, wora: &Wora<AppEv, AppMetric>, fs: impl WFS + 'static) -> Result<(), MainEarlyReturn>
where
    AppEv: Send + Sync + 'static,
    AppMetric: Send + Sync + 'static,
    A: App<AppEv, AppMetric> + Send,
{
    match load_secret_reload::<A::AppSecrets>(&wora.dirs.secrets_root_dir, fs.clone(), None).await {
        Ok(reload) => {
            if !reload.files.is_empty() {
                app.reload_secrets(reload)
                    .instrument(tracing::info_span!("app:run:secrets"))
                    .await
                    .map_err(|err| WoraSetupError::Str(format!("failed to apply secrets {}: {}", wora.dirs.secrets_root_dir.display(), err)))?;
            }
            Ok(())
        }
        Err(ReloadError::Vfs(err)) => {
            debug!("secrets:init:skip path:{} error:{}", wora.dirs.secrets_root_dir.display(), err);
            Ok(())
        }
        Err(err) => Err(WoraSetupError::Str(err.to_string()).into()),
    }
}

fn config_change_event(app_name: &str, metadata_root: &Path, notify_event: notify::Event) -> ConfigChange {
    let main_config_path = metadata_root.join(format!("{app_name}.toml"));
    ConfigChange::new(ChangeKind::from_notify_kind(&notify_event.kind), main_config_path, notify_event.paths)
}

fn secret_change_event(notify_event: notify::Event) -> SecretChange {
    SecretChange::new(ChangeKind::from_notify_kind(&notify_event.kind), notify_event.paths)
}

fn is_main_config_path(app_name: &str, path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == format!("{app_name}.toml"))
}

async fn parse_config_file<C: Config>(path: PathBuf, fs: impl WFS + 'static, is_main: bool) -> Result<Option<ConfigFile<C::ConfigT>>, ReloadError> {
    match fs.read_to_string(&path).await {
        Ok(data) => {
            let value = if is_main {
                C::parse_main_config_file(data)
            } else {
                C::parse_supplemental_config_file(path.clone(), data)
            }
            .map_err(|err| ReloadError::Message(format!("failed to parse config {}: {}", path.display(), err)))?;

            Ok(Some(ConfigFile { path, value }))
        }
        Err(VfsError::Io(err))
            if matches!(
                err.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::IsADirectory | std::io::ErrorKind::InvalidInput
            ) =>
        {
            Ok(None)
        }
        Err(err) => Err(err.into()),
    }
}

async fn metadata_paths(fs: impl WFS + 'static, metadata_root: &PathBuf) -> Result<Vec<PathBuf>, ReloadError> {
    fs.list_dir(metadata_root).await.map_err(ReloadError::from)
}

async fn secret_paths(fs: impl WFS + 'static, secrets_root: &PathBuf) -> Result<Vec<PathBuf>, ReloadError> {
    fs.list_dir(secrets_root).await.map_err(ReloadError::from)
}

async fn load_config_reload<C>(
    metadata_root: &PathBuf,
    app_name: &str,
    fs: impl WFS + 'static,
    changed_paths: Option<&[PathBuf]>,
) -> Result<ConfigReload<C::ConfigT>, ReloadError>
where
    C: Config,
{
    let mut reload = ConfigReload::default();
    let paths = match changed_paths {
        Some(paths) => paths.to_vec(),
        None => metadata_paths(fs.clone(), metadata_root).await?,
    };

    for path in paths {
        let is_main = is_main_config_path(app_name, &path);
        if let Some(parsed) = parse_config_file::<C>(path.clone(), fs.clone(), is_main).await? {
            if is_main {
                reload.main = Some(parsed.value);
            } else {
                reload.supplemental.push(parsed);
            }
        }
    }

    Ok(reload)
}

async fn load_secret_reload<S>(
    secrets_root: &PathBuf,
    fs: impl WFS + 'static,
    changed_paths: Option<&[PathBuf]>,
) -> Result<SecretReload<S::SecretT>, ReloadError>
where
    S: Secrets,
{
    let mut reload = SecretReload::default();
    let paths = match changed_paths {
        Some(paths) => paths.to_vec(),
        None => secret_paths(fs.clone(), secrets_root).await?,
    };

    for path in paths {
        let key = match path.file_name().and_then(|value| value.to_str()) {
            Some(key) => key.to_string(),
            None => continue,
        };

        match fs.read(&path).await {
            Ok(data) => {
                let value = S::parse_secret_file(path.clone(), data)
                    .map_err(|err| ReloadError::Message(format!("failed to parse secret {}: {}", path.display(), err)))?;
                reload.files.push(SecretFile { path, key, value });
            }
            Err(VfsError::Io(err))
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::IsADirectory | std::io::ErrorKind::InvalidInput
                ) => {}
            Err(err) => return Err(err.into()),
        }
    }

    Ok(reload)
}

enum ShutdownReason {
    External,
    Unhealthy,
}

enum RuntimeSupervisionEvent {
    ShutdownRequested(ShutdownReason, Option<chrono::NaiveDateTime>),
}

async fn wait_for_ready_notification<AppEv, AppMetric, E, F>(
    exec: E,
    app_name: String,
    dirs: Dirs,
    fs: F,
    mut readiness_rx: watch::Receiver<ReadinessState>,
) -> Result<(), SetupFailure>
where
    AppEv: Send + Sync + 'static,
    AppMetric: Send + Sync + 'static,
    E: AsyncExecutor<AppEv, AppMetric>,
    F: WFS + 'static,
{
    if *readiness_rx.borrow() == ReadinessState::Ready {
        return exec.on_runtime_ready(&app_name, &dirs, fs).await;
    }

    loop {
        if readiness_rx.changed().await.is_err() {
            return Ok(());
        }
        if *readiness_rx.borrow() == ReadinessState::Ready {
            return exec.on_runtime_ready(&app_name, &dirs, fs).await;
        }
    }
}

fn runtime_metrics_snapshot<AppEv, AppMetric>(wora: &Wora<AppEv, AppMetric>, restart_count: u32) -> RuntimeMetrics
where
    AppEv: Send + Sync + 'static,
    AppMetric: Send + Sync + 'static,
{
    RuntimeMetrics {
        app_name: wora.app_name.clone(),
        pid: std::process::id(),
        leadership: wora.leadership.clone(),
        health: wora.health_state(),
        readiness: wora.readiness_state(),
        restart_count,
        event_backlog_capacity: wora.sender.capacity(),
        event_backlog_max_capacity: wora.sender.max_capacity(),
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_app_main_with_restart_policy<AppEv, AppMetric, A, E, F>(
    app: &mut A,
    wora: &mut Wora<AppEv, AppMetric>,
    exec: E,
    fs: F,
    metrics_sender: Sender<O11yEvent<AppMetric>>,
    restart_options: RestartPolicyOptions,
    supervision_rx: &mut Receiver<RuntimeSupervisionEvent>,
    restart_counter: Arc<AtomicU32>,
) -> Result<(), MainEarlyReturn>
where
    AppEv: Send + Sync + 'static,
    AppMetric: Send + Sync + 'static,
    A: App<AppEv, AppMetric>,
    E: AsyncExecutor<AppEv, AppMetric> + Clone,
    F: WFS + 'static,
{
    let mut retry_count = 0u32;

    loop {
        let status_handle = wora.status_handle();
        let app_event_sender = wora.sender.clone();
        let app_name = wora.app_name.clone();
        let runtime_dirs = wora.dirs.clone();
        status_handle.report_health(HealthState::Unknown);
        restart_counter.store(retry_count, Ordering::Relaxed);
        let _ = metrics_sender
            .send(o11y_new_ev_runtime_metrics(&runtime_metrics_snapshot(wora, retry_count)))
            .await;

        let main_future = app
            .main(wora, exec.clone(), fs.clone(), metrics_sender.clone())
            .instrument(tracing::info_span!("app:run:main", retry = retry_count));
        tokio::pin!(main_future);

        let mut shutdown_deadline: Option<Pin<Box<tokio::time::Sleep>>> = None;
        let mut drain_deadline: Option<Pin<Box<tokio::time::Sleep>>> = None;
        let mut shutdown_reason = ShutdownReason::External;
        let mut shutdown_timestamp = None;
        let mut draining_started = false;
        let mut stopping_started = false;

        let main_result = loop {
            tokio::select! {
                result = &mut main_future => break result,
                Some(event) = supervision_rx.recv() => {
                    let RuntimeSupervisionEvent::ShutdownRequested(reason, timestamp) = event;
                    if !draining_started {
                        draining_started = true;
                        shutdown_reason = reason;
                        shutdown_timestamp = timestamp;
                        status_handle.begin_draining();
                        exec.on_runtime_draining(&app_name, &runtime_dirs, fs.clone()).await?;
                        if restart_options.supervision.drain_grace_period.is_zero() {
                            if !stopping_started {
                                stopping_started = true;
                                status_handle.begin_stopping();
                                shutdown_deadline = Some(Box::pin(tokio::time::sleep(restart_options.supervision.shutdown_grace_period)));
                                let _ = app_event_sender.send(Event::Control(ControlEvent::Shutdown(timestamp))).await;
                            }
                        } else {
                            drain_deadline = Some(Box::pin(tokio::time::sleep(restart_options.supervision.drain_grace_period)));
                        }
                    }
                }
                _ = async {
                    if let Some(deadline) = drain_deadline.as_mut() {
                        deadline.await;
                    }
                }, if drain_deadline.is_some() => {
                    drain_deadline = None;
                    if !stopping_started {
                        stopping_started = true;
                        status_handle.begin_stopping();
                        shutdown_deadline = Some(Box::pin(tokio::time::sleep(restart_options.supervision.shutdown_grace_period)));
                        let _ = app_event_sender.send(Event::Control(ControlEvent::Shutdown(shutdown_timestamp))).await;
                    }
                }
                _ = async {
                    if let Some(deadline) = shutdown_deadline.as_mut() {
                        deadline.await;
                    }
                }, if shutdown_deadline.is_some() => {
                    match shutdown_reason {
                        ShutdownReason::External => {
                            break MainRetryAction::UseExitCode(restart_options.supervision.forced_shutdown_exit_code);
                        }
                        ShutdownReason::Unhealthy => {
                            break match restart_options.supervision.unhealthy_action {
                                UnhealthyAction::Ignore => MainRetryAction::UseExitCode(restart_options.supervision.forced_shutdown_exit_code),
                                UnhealthyAction::RequestShutdown => MainRetryAction::UseExitCode(restart_options.supervision.forced_shutdown_exit_code),
                                UnhealthyAction::UseRestartPolicy => MainRetryAction::UseRestartPolicy,
                            };
                        }
                    }
                }
            }
        };

        match main_result {
            MainRetryAction::UseExitCode(ec) => return Err(MainEarlyReturn::UseExitCode(ec)),
            MainRetryAction::Success => return Ok(()),
            MainRetryAction::UseRestartPolicy => match restart_options.policy {
                WorkloadRestartPolicy::ExitWithWorkloadReturn => return Err(MainEarlyReturn::UseExitCode(1)),
                WorkloadRestartPolicy::RetryInstantly => {
                    if !restart_options.can_retry(retry_count) {
                        return Err(MainEarlyReturn::UseExitCode(1));
                    }
                    retry_count = retry_count.saturating_add(1);
                    restart_counter.store(retry_count, Ordering::Relaxed);
                    info!("app:run:restart policy:retry_instantly retry:{}", retry_count);
                }
                WorkloadRestartPolicy::RetryPause => {
                    if !restart_options.can_retry(retry_count) {
                        return Err(MainEarlyReturn::UseExitCode(1));
                    }
                    retry_count = retry_count.saturating_add(1);
                    restart_counter.store(retry_count, Ordering::Relaxed);
                    let pause = restart_options.pause_for_retry(retry_count);
                    info!("app:run:restart policy:retry_pause retry:{} pause:{:?}", retry_count, pause);
                    tokio::time::sleep(pause).await;
                }
                WorkloadRestartPolicy::ExponentialBackoff => {
                    if !restart_options.can_retry(retry_count) {
                        return Err(MainEarlyReturn::UseExitCode(1));
                    }
                    retry_count = retry_count.saturating_add(1);
                    restart_counter.store(retry_count, Ordering::Relaxed);
                    let pause = restart_options.pause_for_retry(retry_count);
                    info!("app:run:restart policy:exponential_backoff retry:{} pause:{:?}", retry_count, pause);
                    tokio::time::sleep(pause).await;
                }
            },
        }
    }
}

// TODOs
// - create a non-file locking variant

/// Options used by the async runner.
///
/// This is the preferred extension point for app-typed runtime policies such
/// as signal mapping and for runner infrastructure choices such as lock
/// backend, restart behavior, boot-state location, and telemetry provider.
#[derive(Clone)]
pub struct RunnerOptions<AppEv, L = ProcLockBackend, R = SystemRuntimeEnvironment> {
    /// Optional boot-state directory. Defaults to WORA's host temp boot root.
    pub boot_dir: Option<PathBuf>,
    /// Restart and supervision behavior.
    pub restart: RestartPolicyOptions,
    /// Translate platform signals into application/runtime events.
    ///
    /// The default mapper turns `SIGHUP` into
    /// `ControlEvent::ReloadConfiguration`, `SIGUSR1` into
    /// `ControlEvent::LogRotation`, and termination signals into shutdown.
    /// The runner supervises shutdown; reload/log-rotation controls are
    /// delivered to the application.
    pub signal_mapper: SignalMapper<AppEv>,
    /// Lock backend used to serialize workload execution.
    pub lock_backend: L,
    /// Runtime environment provider used for host and process telemetry.
    pub runtime_environment: R,
}

impl<AppEv: Send + Sync + 'static> Default for RunnerOptions<AppEv> {
    fn default() -> Self {
        Self {
            boot_dir: None,
            restart: RestartPolicyOptions::default(),
            signal_mapper: default_signal_mapper(),
            lock_backend: ProcLockBackend,
            runtime_environment: SystemRuntimeEnvironment::default(),
        }
    }
}

impl<AppEv: Send + Sync + 'static> RunnerOptions<AppEv> {
    /// Create default runner options.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<AppEv: Send + Sync + 'static, L, R> RunnerOptions<AppEv, L, R> {
    /// Set the boot-state directory.
    pub fn with_boot_dir(mut self, boot_dir: impl Into<PathBuf>) -> Self {
        self.boot_dir = Some(boot_dir.into());
        self
    }

    /// Set restart and supervision behavior.
    pub fn with_restart_options(mut self, restart: RestartPolicyOptions) -> Self {
        self.restart = restart;
        self
    }

    /// Set a custom platform signal mapper.
    pub fn with_signal_mapper(mut self, mapper: impl Fn(RuntimeSignal) -> Option<Event<AppEv>> + Send + Sync + 'static) -> Self {
        self.signal_mapper = Arc::new(mapper);
        self
    }

    /// Set the lock backend used to serialize workload execution.
    pub fn with_lock_backend<NewL>(self, lock_backend: NewL) -> RunnerOptions<AppEv, NewL, R> {
        RunnerOptions {
            boot_dir: self.boot_dir,
            restart: self.restart,
            signal_mapper: self.signal_mapper,
            lock_backend,
            runtime_environment: self.runtime_environment,
        }
    }

    /// Set the runtime environment provider used for telemetry.
    pub fn with_runtime_environment<NewR>(self, runtime_environment: NewR) -> RunnerOptions<AppEv, L, NewR> {
        RunnerOptions {
            boot_dir: self.boot_dir,
            restart: self.restart,
            signal_mapper: self.signal_mapper,
            lock_backend: self.lock_backend,
            runtime_environment,
        }
    }
}

/// Run apps via an `async` based executor with default [`RunnerOptions`].
///
/// The executor is expected to create all configured directory roots during
/// setup, including `metadata_root_dir` and `secrets_root_dir`. The runner
/// installs watchers on both roots after setup so it can emit typed config and
/// secret reload events.
pub async fn exec_async_runner<AppEv: Send + Sync + 'static, AppMetric: Debug + Send + Sync + 'static>(
    exec: impl AsyncExecutor<AppEv, AppMetric> + 'static,
    app: impl App<AppEv, AppMetric> + Send + 'static,
    fs: impl WFS + 'static,
    o11y: O11yProcessorOptions<AppMetric>,
) -> Result<(), MainEarlyReturn> {
    exec_async_runner_with_options(exec, app, fs, o11y, RunnerOptions::default()).await
}

/// Run apps via an `async` based executor with explicit runner options.
///
/// The executor is expected to create all configured directory roots during
/// setup, including `metadata_root_dir` and `secrets_root_dir`. The runner
/// installs watchers on both roots after setup so it can emit typed config and
/// secret reload events.
#[allow(clippy::too_many_lines)]
pub async fn exec_async_runner_with_options<AppEv: Send + Sync + 'static, AppMetric: Debug + Send + Sync + 'static, L: LockBackend, R: RuntimeEnvironment>(
    exec: impl AsyncExecutor<AppEv, AppMetric> + 'static,
    app: impl App<AppEv, AppMetric> + Send + 'static,
    fs: impl WFS + 'static,
    o11y: O11yProcessorOptions<AppMetric>,
    options: RunnerOptions<AppEv, L, R>,
) -> Result<(), MainEarlyReturn> {
    let RunnerOptions {
        boot_dir,
        restart,
        signal_mapper,
        lock_backend,
        runtime_environment,
    } = options;
    let mut exec = exec;
    let mut app = app;
    let mut lock_path = PathBuf::new();
    lock_path.push(&exec.dirs().runtime_root_dir);
    let lock_fp = if app.allow_concurrent_executions() {
        let now = Utc::now();
        let ts = now.timestamp_millis().to_string();
        app.name().to_owned() + ts.as_str() + ".lock"
    } else {
        app.name().to_owned() + ".lock"
    };

    lock_path.push(&lock_fp);

    let o11y_tx = o11y.sender().clone();

    let _ = o11y_tx.send(o11y_new_ev_init(exec.dirs().log_root_dir.clone())).await;

    match lock_backend.try_lock(&lock_path) {
        Ok(lock_guard) => {
            info!("exec:run:lock_file created:{:?}", &lock_path);

            let metrics_sender = o11y.sender().clone();
            let status_interval = *o11y.status_interval();
            let flush_interval = *o11y.flush_interval();
            let hs_interval = *o11y.host_stats_interval();
            let restart_counter = Arc::new(AtomicU32::new(0));

            let mut wora = Wora::new_with_runtime_environment(exec.dirs(), app.name().to_string(), EVENT_BUFFER_SIZE, o11y, runtime_environment.clone())?;
            let mut health_rx = wora.status_handle().subscribe_health();
            let readiness_rx = wora.status_handle().subscribe_readiness();
            let (runtime_event_tx, mut runtime_event_rx) = channel::<Event<AppEv>>(EVENT_BUFFER_SIZE);
            let (supervision_tx, mut supervision_rx) = channel::<RuntimeSupervisionEvent>(EVENT_BUFFER_SIZE);
            let dispatch_app_sender = wora.sender.clone();
            let dispatch_supervision_sender = supervision_tx.clone();
            let runtime_dispatch_task = tokio::spawn(async move {
                while let Some(event) = runtime_event_rx.recv().await {
                    if let Event::Control(ControlEvent::Shutdown(timestamp)) = &event {
                        let _ = dispatch_supervision_sender
                            .send(RuntimeSupervisionEvent::ShutdownRequested(ShutdownReason::External, *timestamp))
                            .await;
                    }

                    if dispatch_app_sender.send(event).await.is_err() {
                        break;
                    }
                }
            });

            let health_supervision_sender = supervision_tx.clone();
            let health_event_sender = wora.sender.clone();
            let health_supervision_task = tokio::spawn(async move {
                loop {
                    if health_rx.changed().await.is_err() {
                        break;
                    }

                    if *health_rx.borrow() == HealthState::Failed {
                        let timestamp = Some(Utc::now().naive_utc());
                        let _ = health_event_sender.send(Event::Control(ControlEvent::Shutdown(timestamp))).await;
                        let _ = health_supervision_sender
                            .send(RuntimeSupervisionEvent::ShutdownRequested(ShutdownReason::Unhealthy, timestamp))
                            .await;
                    }
                }
            });

            let runtime_event_tasks = exec
                .spawn_runtime_event_sources_with_signal_mapper(runtime_event_tx.clone(), signal_mapper.clone())
                .instrument(tracing::info_span!("exec:run:event_sources"))
                .await?;

            let _ = metrics_sender.send(o11y_new_ev_hostinfo(wora.host.info())).await;
            let _ = metrics_sender
                .send(o11y_new_ev_runtime_metrics(&runtime_metrics_snapshot(
                    &wora,
                    restart_counter.load(Ordering::Relaxed),
                )))
                .await;
            if let Some(process_stats) = runtime_environment.initial_process_stats() {
                let _ = metrics_sender.send(o11y_new_ev_processstats(&process_stats)).await;
            }

            wora.schedule_task(status_interval, move |tx| async move {
                let cap = tx.capacity();
                let max_cap = tx.max_capacity();
                let _ = tx.send(o11y_new_ev_status(cap, max_cap)).await;
                TaskOp::Requeue
            })
            .await;

            wora.schedule_task(flush_interval, move |tx| async move {
                let _ = tx.send(o11y_new_ev_flush()).await;
                TaskOp::Requeue
            })
            .await;

            let runtime_status = wora.status_handle();
            let runtime_app_name = wora.app_name.clone();
            let runtime_leadership = wora.leadership.clone();
            let runtime_metrics_restart_counter = restart_counter.clone();
            let runtime_environment = runtime_environment.clone();
            wora.schedule_task(hs_interval, move |tx| {
                let runtime_status = runtime_status.clone();
                let runtime_app_name = runtime_app_name.clone();
                let runtime_leadership = runtime_leadership.clone();
                let runtime_metrics_restart_counter = runtime_metrics_restart_counter.clone();
                let runtime_environment = runtime_environment.clone();
                async move {
                    let host_stats = match runtime_environment.refresh_host_stats() {
                        Ok(stats) => stats,
                        Err(err) => {
                            error!("o11y:host stats refresh error: {}", err);
                            None
                        }
                    };
                    if let Some(host_stats) = host_stats {
                        let _ = tx.send(o11y_new_ev_hoststats(&host_stats)).await;
                    }

                    let process_stats = runtime_environment.refresh_process_stats();
                    if let Some(process_stats) = process_stats {
                        let _ = tx.send(o11y_new_ev_processstats(&process_stats)).await;
                    }

                    let runtime_metrics = RuntimeMetrics {
                        app_name: runtime_app_name.clone(),
                        pid: std::process::id(),
                        leadership: runtime_leadership.clone(),
                        health: runtime_status.health_state(),
                        readiness: runtime_status.readiness_state(),
                        restart_count: runtime_metrics_restart_counter.load(Ordering::Relaxed),
                        event_backlog_capacity: tx.capacity(),
                        event_backlog_max_capacity: tx.max_capacity(),
                    };
                    let _ = tx.send(o11y_new_ev_runtime_metrics(&runtime_metrics)).await;

                    TaskOp::Requeue
                }
            })
            .await;

            let boot_state = resolve_boot_state(fs.clone(), boot_dir.unwrap_or_else(default_boot_root), app.name())
                .await
                .map_err(WoraSetupError::from)?;
            let is_first_boot = boot_state.is_first_boot();
            debug!("boot:state app:{} state:{:?}", app.name(), boot_state);

            exec.setup(&wora, fs.clone()).instrument(tracing::info_span!("exec:run:setup")).await?;

            configure_app_from_metadata(&mut app, &wora, fs.clone()).await?;
            load_initial_secrets(&mut app, &wora, fs.clone()).await?;

            let mut rc = Err(MainEarlyReturn::UseExitCode(1));

            match app
                .setup(&wora, exec.clone(), fs.clone(), metrics_sender.clone(), is_first_boot)
                .instrument(tracing::info_span!("app:run:setup"))
                .await
            {
                Ok(_) => {
                    trace!("checking executor directories exist...");

                    for dir in [
                        &wora.dirs.root_dir,
                        &wora.dirs.log_root_dir,
                        &wora.dirs.metadata_root_dir,
                        &wora.dirs.data_root_dir,
                        &wora.dirs.cache_root_dir,
                    ] {
                        if !fs.dir_exists(dir).await? {
                            error!("directory {:?} does not exist", dir);
                            return Err(MainEarlyReturn::WoraSetup(WoraSetupError::DirectoryDoesNotExistOnFilesystem(dir.clone())));
                        }
                    }

                    info!(
                        host.hostname = wora.host_hostname(),
                        host.platform = wora.host_architecture(),
                        host.os_name = wora.host_os_name(),
                        host.os_version = wora.host_os_version(),
                        host.cpu_count = wora.host_cpu_count(),
                        host.cpu_max = wora.host_cpu_max(),
                    );
                    info!("dirs.root: {:?}", wora.dirs.root_dir);
                    info!("dirs.log: {:?}", wora.dirs.log_root_dir);
                    info!("dirs.metadata: {:?}", wora.dirs.metadata_root_dir);
                    info!("dirs.data: {:?}", wora.dirs.data_root_dir);
                    info!("dirs.runtime: {:?}", wora.dirs.runtime_root_dir);
                    info!("dirs.cache: {:?}", wora.dirs.cache_root_dir);
                    info!("dirs.secrets: {:?}", wora.dirs.secrets_root_dir);

                    let ready_task = tokio::spawn(wait_for_ready_notification::<AppEv, AppMetric, _, _>(
                        exec.clone(),
                        wora.app_name.clone(),
                        wora.dirs.clone(),
                        fs.clone(),
                        readiness_rx,
                    ));

                    info!("notify:watch:dir: {:?}", &wora.dirs.metadata_root_dir);
                    let mut watcher = fs.watch_dir(&wora.dirs.metadata_root_dir).await?;
                    info!("notify:watch:secrets: {:?}", &wora.dirs.secrets_root_dir);
                    let mut secret_watcher = fs.watch_dir(&wora.dirs.secrets_root_dir).await?;
                    let metadata_root = wora.dirs.metadata_root_dir.clone();
                    let app_name = wora.app_name.clone();
                    let ev_sender = runtime_event_tx.clone();
                    let secret_ev_sender = runtime_event_tx.clone();

                    let watcher_task = tokio::spawn(async move {
                        while let Some(res) = watcher.receiver().recv().await {
                            match res {
                                Ok(event) => {
                                    info!("changed: {:?}", event);
                                    match ev_sender
                                        .send(Event::ConfigChanged(config_change_event(&app_name, &metadata_root, event)))
                                        .await
                                    {
                                        Ok(_) => {}
                                        Err(send_err) => {
                                            error!("send error: {:?}", send_err);
                                        }
                                    }
                                }
                                Err(e) => error!("watch error: {:?}", e),
                            }
                        }
                    });
                    let secret_watcher_task = tokio::spawn(async move {
                        while let Some(res) = secret_watcher.receiver().recv().await {
                            match res {
                                Ok(event) => {
                                    info!("secret changed: {:?}", event);
                                    match secret_ev_sender.send(Event::SecretChanged(secret_change_event(event))).await {
                                        Ok(_) => {}
                                        Err(send_err) => {
                                            error!("send error: {:?}", send_err);
                                        }
                                    }
                                }
                                Err(e) => error!("watch error: {:?}", e),
                            }
                        }
                    });

                    info!(process_id = wora.pid.to_string(), app_name = app.name());

                    if exec.is_ready(&wora, fs.clone()).instrument(tracing::info_span!("exec:run:is_ready")).await {
                        rc = run_app_main_with_restart_policy(
                            &mut app,
                            &mut wora,
                            exec.clone(),
                            fs.clone(),
                            metrics_sender.clone(),
                            restart.clone(),
                            &mut supervision_rx,
                            restart_counter.clone(),
                        )
                        .await;

                        if matches!(rc, Err(MainEarlyReturn::UseExitCode(_))) {
                            remove_lock_artifact(&lock_backend, &lock_path).await;
                        }
                    } else {
                        warn!(comp = "exec", method = "run", is_ready = false);
                    }

                    watcher_task.abort();
                    secret_watcher_task.abort();
                    match ready_task.await {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => return Err(MainEarlyReturn::SetupFailed(err)),
                        Err(join_err) if join_err.is_cancelled() => {}
                        Err(join_err) => {
                            return Err(MainEarlyReturn::WoraSetup(WoraSetupError::Str(format!(
                                "readiness supervision task failed: {join_err}"
                            ))));
                        }
                    }

                    exec.on_runtime_stopping(&wora.app_name, &wora.dirs, fs.clone())
                        .instrument(tracing::info_span!("exec:run:on_runtime_stopping"))
                        .await?;

                    app.end(&wora, exec.clone(), fs.clone(), metrics_sender.clone())
                        .instrument(tracing::info_span!("app:run:end"))
                        .await;
                }
                Err(setup_err) => {
                    error!("{:?}", setup_err)
                }
            }

            exec.end(&wora, fs.clone()).instrument(tracing::info_span!("exec:run:end")).await;

            health_supervision_task.abort();
            runtime_dispatch_task.abort();
            for task in runtime_event_tasks {
                task.abort();
            }

            drop(lock_guard);
            remove_lock_artifact(&lock_backend, &lock_path).await;

            let _ = o11y_tx.send(o11y_new_ev_finish()).await;

            rc
        }
        Err(LockError::AlreadyLocked(path)) => {
            info!("lock already held: {:?}", &path);

            let _ = o11y_tx.send(o11y_new_ev_finish()).await;

            Err(MainEarlyReturn::AlreadyRunning(path))
        }
        Err(LockError::Io(err)) => {
            error!("lock file:{:?} error:{:?}", &lock_path, err);

            let _ = o11y_tx.send(o11y_new_ev_finish()).await;

            Err(MainEarlyReturn::IO(err))
        }
    }
}

async fn remove_lock_artifact<L: LockBackend>(lock_backend: &L, lock_path: &PathBuf) {
    match lock_backend.cleanup(lock_path).await {
        Ok(_) => {
            debug!("lock:removed file:{:?}", lock_path);
        }
        Err(VfsError::Io(rm_err)) => {
            error!("lock file:{:?} error:{}", lock_path, rm_err);
        }
        Err(err) => {
            error!("lock artifact:{:?} error:{}", lock_path, err);
        }
    }
}
