//! Write Once Run Anywhere (WORA)
//!
//! A framework for building applications (daemons, etc.) that run in different environments (Linux, Kubernetes, etc.).
//!
//! Just like Java's claim, it really doesn't run everywhere. no_std or embedded environments are not supported.

use std::fmt::Debug;
use std::future::Future;
use std::path::PathBuf;

use async_trait::async_trait;
use chrono::Utc;
use libc::{SIGHUP, SIGINT, SIGQUIT, SIGTERM, SIGUSR1, SIGUSR2};
use nix::unistd::getpid;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use proc_lock::try_lock;
use serde::Serialize;
use tokio::signal::unix::SignalKind;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tracing::Instrument;
use tracing::{debug, error, info, trace, warn};

pub mod dirs;
pub mod errors;
pub mod events;
pub mod exec;
pub mod exec_unix;
pub mod o11y;
pub mod prelude;
pub mod restart_policy;
pub mod vfs;

use crate::dirs::Dirs;
use crate::errors::{MainEarlyReturn, WoraSetupError};
use crate::events::*;
use crate::exec::*;
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

/// WORA API
impl<AppEv: Send + Sync + 'static, AppMetric: Send + Sync + 'static> Wora<AppEv, AppMetric> {
    /// Create a new runtime context and register Unix signal forwarding.
    pub fn new(dirs: &Dirs, app_name: String, ev_buf_size: usize, o11y: O11yProcessorOptions<AppMetric>) -> Result<Wora<AppEv, AppMetric>, WoraSetupError> {
        let pid = getpid();

        let (tx, rx) = channel(ev_buf_size);

        for &signum in [SIGHUP, SIGTERM, SIGQUIT, SIGUSR1, SIGUSR2, SIGINT].iter() {
            let send = tx.clone();
            let mut sig = tokio::signal::unix::signal(SignalKind::from_raw(signum))?;
            tokio::spawn(async move {
                loop {
                    sig.recv().await;
                    if send.send(Event::UnixSignal(signum)).await.is_err() {
                        break;
                    };
                }
            });
        }

        let host = Host::new()?;

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
    type ConfigT: Default;
    /// Parse the main configuration file.
    fn parse_main_config_file(data: String) -> Result<Self::ConfigT, Box<dyn std::error::Error>>;

    /// Parse a supplemental configuration file.
    fn parse_supplemental_config_file(_file_path: PathBuf, _data: String) -> Result<Self::ConfigT, Box<dyn std::error::Error>> {
        Ok(Self::ConfigT::default())
    }
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
pub trait App<AppEv, AppMetric> {
    /// Configuration parser for the app.
    type AppConfig: Config;
    /// Setup artifact type returned by `setup`.
    type Setup;
    /// Stable application name used for directories, locks, and logging.
    fn name(&self) -> &'static str;

    /// Return whether multiple instances may run concurrently.
    fn allow_concurrent_executions(&self) -> bool {
        false
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

    /// Report application health.
    async fn is_healthy(&mut self) -> HealthState;

    /// Clean up application state after `main` returns.
    async fn end(
        &mut self,
        wora: &Wora<AppEv, AppMetric>,
        exec: impl AsyncExecutor<AppEv, AppMetric>,
        fs: impl WFS + 'static,
        metrics: Sender<O11yEvent<AppMetric>>,
    );
}

fn async_watcher() -> notify::Result<(RecommendedWatcher, Receiver<notify::Result<notify::Event>>)> {
    let (tx, rx) = channel(1);

    let watcher = RecommendedWatcher::new(
        move |res| {
            futures::executor::block_on(async {
                if tx.send(res).await.is_err() {
                    error!("notify:watch:send failed");
                }
            })
        },
        notify::Config::default(),
    )?;

    Ok((watcher, rx))
}

async fn run_app_main_with_restart_policy<AppEv, AppMetric, A, E, F>(
    app: &mut A,
    wora: &mut Wora<AppEv, AppMetric>,
    exec: E,
    fs: F,
    metrics_sender: Sender<O11yEvent<AppMetric>>,
    restart_policy: WorkloadRestartPolicy,
    restart_pause: std::time::Duration,
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
        match app
            .main(wora, exec.clone(), fs.clone(), metrics_sender.clone())
            .instrument(tracing::info_span!("app:run:main", retry = retry_count))
            .await
        {
            MainRetryAction::UseExitCode(ec) => return Err(MainEarlyReturn::UseExitCode(ec)),
            MainRetryAction::Success => return Ok(()),
            MainRetryAction::UseRestartPolicy => match restart_policy {
                WorkloadRestartPolicy::ExitWithWorkloadReturn => return Err(MainEarlyReturn::UseExitCode(1)),
                WorkloadRestartPolicy::RetryInstantly => {
                    retry_count = retry_count.saturating_add(1);
                    info!("app:run:restart policy:retry_instantly retry:{}", retry_count);
                }
                WorkloadRestartPolicy::RetryPause => {
                    retry_count = retry_count.saturating_add(1);
                    info!("app:run:restart policy:retry_pause retry:{}", retry_count);
                    tokio::time::sleep(restart_pause).await;
                }
                WorkloadRestartPolicy::ExponentialBackoff => {
                    retry_count = retry_count.saturating_add(1);
                    let multiplier = 2u32.saturating_pow(retry_count.min(10));
                    let pause = restart_pause.saturating_mul(multiplier);
                    info!("app:run:restart policy:exponential_backoff retry:{} pause:{:?}", retry_count, pause);
                    tokio::time::sleep(pause).await;
                }
            },
        }
    }
}

// TODOs
// - create a non-file locking variant

/// Run apps via an `async` based executor
pub async fn exec_async_runner<AppEv: Send + Sync + 'static, AppMetric: Debug + Send + Sync + 'static>(
    exec: impl AsyncExecutor<AppEv, AppMetric>,
    app: impl App<AppEv, AppMetric> + Send + 'static,
    fs: impl WFS + 'static,
    o11y: O11yProcessorOptions<AppMetric>,
    maybe_boot_dir: Option<PathBuf>,
) -> Result<(), MainEarlyReturn> {
    exec_async_runner_with_restart_policy(
        exec,
        app,
        fs,
        o11y,
        maybe_boot_dir,
        WorkloadRestartPolicy::default(),
        std::time::Duration::from_secs(1),
    )
    .await
}

/// Run apps via an `async` based executor with an explicit restart policy.
pub async fn exec_async_runner_with_restart_policy<AppEv: Send + Sync + 'static, AppMetric: Debug + Send + Sync + 'static>(
    mut exec: impl AsyncExecutor<AppEv, AppMetric>,
    mut app: impl App<AppEv, AppMetric> + Send + 'static,
    fs: impl WFS + 'static,
    o11y: O11yProcessorOptions<AppMetric>,
    maybe_boot_dir: Option<PathBuf>,
    restart_policy: WorkloadRestartPolicy,
    restart_pause: std::time::Duration,
) -> Result<(), MainEarlyReturn> {
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
    let lock = proc_lock::LockPath::FullPath(&lock_path);

    let o11y_tx = o11y.sender().clone();

    let _ = o11y_tx.send(o11y_new_ev_init(exec.dirs().log_root_dir.clone())).await;

    match try_lock(&lock) {
        Ok(lock_guard) => {
            info!("exec:run:lock_file created:{:?}", &lock_path);

            let metrics_sender = o11y.sender().clone();
            let status_interval = *o11y.status_interval();
            let flush_interval = *o11y.flush_interval();
            let hs_interval = *o11y.host_stats_interval();

            let mut wora = Wora::new(exec.dirs(), app.name().to_string(), EVENT_BUFFER_SIZE, o11y)?;

            let _ = metrics_sender.send(o11y_new_ev_hostinfo(wora.host.info())).await;

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

            wora.schedule_task(hs_interval, move |_tx| async move {
                // TODO
                //let _ = tx.send(mehs()).await;
                TaskOp::Requeue
            })
            .await;

            let mut boot_dir = maybe_boot_dir.unwrap_or_else(|| {
                    #[cfg(target_os = "linux")]
                    let fp = PathBuf::from("//var/run");
                    #[cfg(target_os = "macos")]
                    let fp = PathBuf::from("/tmp/");
                    fp
                }
            );
            boot_dir.push(format!(".{}_booted", app.name()));

            let mut is_first_boot = false;
            match fs.create_dir(&boot_dir).await {
                Ok(_) => {
                    is_first_boot = true;
                    debug!("dir:first_boot:created dir:{} is_first_boot:{}", boot_dir.display(), is_first_boot);
                }
                Err(_) => {
                    debug!("dir:first_boot dir:{} is_first_boot:{}", boot_dir.display(), is_first_boot);
                }
            }

            exec.setup(&wora, fs.clone()).instrument(tracing::info_span!("exec:run:setup")).await?;

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
                        if !dir.exists() {
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

                    let (mut watcher, mut watch_rx) = async_watcher()?;

                    info!("notify:watch:dir: {:?}", &wora.dirs.metadata_root_dir);
                    watcher.watch(std::path::Path::new(&wora.dirs.metadata_root_dir), RecursiveMode::Recursive)?;
                    let ev_sender = wora.sender.clone();

                    tokio::spawn(async move {
                        while let Some(res) = watch_rx.recv().await {
                            match res {
                                Ok(event) => {
                                    info!("changed: {:?}", event);
                                    match ev_sender.send(Event::ConfigChange(event)).await {
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
                            restart_policy.clone(),
                            restart_pause,
                        )
                        .await;

                        if matches!(rc, Err(MainEarlyReturn::UseExitCode(_))) {
                            remove_lock_file(&lock_path).await;
                        }
                    } else {
                        warn!(comp = "exec", method = "run", is_ready = false);
                    }

                    app.end(&wora, exec.clone(), fs.clone(), metrics_sender.clone())
                        .instrument(tracing::info_span!("app:run:end"))
                        .await;
                }
                Err(setup_err) => {
                    error!("{:?}", setup_err)
                }
            }

            exec.end(&wora, fs.clone()).instrument(tracing::info_span!("exec:run:end")).await;

            drop(lock_guard);
            remove_lock_file(&lock_path).await;

            let _ = o11y_tx.send(o11y_new_ev_finish()).await;

            rc
        }
        Err(err) => {
            error!("lock file:{:?} error:{:?}", &lock_path, err);

            let _ = o11y_tx.send(o11y_new_ev_finish()).await;

            Err(MainEarlyReturn::UseExitCode(111)) // TODO fix print and return
        }
    }
}

async fn remove_lock_file(lock_path: &PathBuf) {
    match tokio::fs::remove_file(lock_path).await {
        Ok(_) => {
            debug!("lock:removed file:{:?}", lock_path);
        }
        Err(rm_err) => {
            error!("lock file:{:?} error:{}", lock_path, rm_err);
        }
    }
}
