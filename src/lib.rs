//! Write Once Run Anywhere (WORA)
//!
//! A framework for building applications (daemons, etc) that run in different environments (Linux, Kubernetes, etc).
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
use crate::o11y::*;
use events::*;
use exec::*;
use restart_policy::*;
use vfs::*;

const EVENT_BUFFER_SIZE: usize = 1024;

/// All workloads will be able to access the WORA API
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
    pub o11y: O11yProcessorOptions<AppMetric>,
}

#[derive(Clone, Debug, Serialize)]
pub enum Leadership {
    Leader,
    Follower,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum HealthState {
    Ok,
    Suspended,
    TryAgain,
    Failed,
    Unknown,
}

/// WORA API
impl<AppEv: Send + Sync + 'static, AppMetric: Send + Sync + 'static> Wora<AppEv, AppMetric> {
    pub fn new(dirs: &Dirs, app_name: String, ev_buf_size: usize, o11y: O11yProcessorOptions<AppMetric>) -> Result<Wora<AppEv, AppMetric>, WoraSetupError> {
        trace!("checking executor directories exist...");

        for dir in [
            &dirs.root_dir,
            &dirs.log_root_dir,
            &dirs.metadata_root_dir,
            &dirs.data_root_dir,
            &dirs.cache_root_dir,
        ] {
            if !dir.exists() {
                error!("directory {:?} does not exist", dir);
                return Err(WoraSetupError::DirectoryDoesNotExistOnFilesystem(dir.clone()));
            }
        }

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
            o11y: o11y,
        })
    }

    pub fn stats_from_start(&self) -> &HostStats {
        &self.host.stats
    }

    pub fn dirs(&self) -> &Dirs {
        &self.dirs
    }

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

    pub fn host_os_name(&self) -> &str {
        &self.host.info.os_name
    }
    pub fn host_os_version(&self) -> Option<String> {
        self.host.info.os_version.clone()
    }
    pub fn host_architecture(&self) -> Option<String> {
        self.host.info.architecture.clone()
    }
    pub fn host_hostname(&self) -> Option<String> {
        self.host.info.hostname.clone()
    }
    pub fn host_cpu_count(&self) -> usize {
        self.host.info.ncpus
    }
    pub fn host_cpu_max(&self) -> usize {
        self.host.info.maxcpus
    }

    pub async fn schedule_event(&self, duration: tokio::time::Duration, ev: Event<AppEv>) {
        tokio::time::sleep(duration).await;
        self.emit_event(ev).await
    }

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

#[derive(Debug, Clone)]
pub enum TaskOp {
    Requeue,
    Abort,
}

pub trait Config {
    type ConfigT: Default;
    fn parse_main_config_file(data: String) -> Result<Self::ConfigT, Box<dyn std::error::Error>>;

    fn parse_supplemental_config_file(_file_path: PathBuf, _data: String) -> Result<Self::ConfigT, Box<dyn std::error::Error>> {
        Ok(Self::ConfigT::default())
    }
}

pub struct NoConfig;
impl Config for NoConfig {
    type ConfigT = ();
    fn parse_main_config_file(_data: String) -> Result<Self::ConfigT, Box<dyn std::error::Error>> {
        Ok(())
    }
}
#[async_trait]
pub trait App<AppEv, AppMetric> {
    type AppConfig: Config;
    type Setup;
    fn name(&self) -> &'static str;

    fn allow_concurrent_executions(&self) -> bool {
        false
    }

    async fn setup(
        &mut self,
        wora: &Wora<AppEv, AppMetric>,
        exec: impl AsyncExecutor<AppEv, AppMetric>,
        fs: impl WFS,
        metrics: Sender<O11yEvent<AppMetric>>,
    ) -> Result<Self::Setup, Box<dyn std::error::Error>>;

    async fn main(
        &mut self,
        wora: &mut Wora<AppEv, AppMetric>,
        exec: impl AsyncExecutor<AppEv, AppMetric>,
        fs: impl WFS,
        metrics: Sender<O11yEvent<AppMetric>>,
    ) -> MainRetryAction;

    async fn is_healthy(&mut self) -> HealthState;

    async fn end(&mut self, wora: &Wora<AppEv, AppMetric>, exec: impl AsyncExecutor<AppEv, AppMetric>, fs: impl WFS, metrics: Sender<O11yEvent<AppMetric>>);
}

fn async_watcher() -> notify::Result<(RecommendedWatcher, Receiver<notify::Result<notify::Event>>)> {
    let (tx, rx) = channel(1);

    let watcher = RecommendedWatcher::new(
        move |res| {
            futures::executor::block_on(async {
                tx.send(res).await.unwrap();
            })
        },
        notify::Config::default(),
    )?;

    Ok((watcher, rx))
}

// TODOs
// - create a non-file locking variant

/// Run apps via an `async` based executor
pub async fn exec_async_runner<AppEv: Send + Sync + 'static, AppMetric: Debug + Send + Sync + 'static>(
    mut exec: impl AsyncExecutor<AppEv, AppMetric>,
    mut app: impl App<AppEv, AppMetric> + 'static,
    fs: impl WFS,
    o11y: O11yProcessorOptions<AppMetric>,
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
        Ok(guard) => {
            info!("exec:run:lock_file created:{:?}", &lock_path);

            let metrics_sender = o11y.sender().clone();
            let status_interval = o11y.status_interval().clone();
            let flush_interval = o11y.flush_interval().clone();
            let hs_interval = o11y.host_stats_interval().clone();

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

            exec.setup(&wora, fs.clone()).instrument(tracing::info_span!("exec:run:setup")).await?;

            let mut rc = Err(MainEarlyReturn::UseExitCode(1));

            match app
                .setup(&wora, exec.clone(), fs.clone(), metrics_sender.clone())
                .instrument(tracing::info_span!("app:run:setup"))
                .await
            {
                Ok(_) => {
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
                        match app
                            .main(&mut wora, exec.clone(), fs.clone(), metrics_sender.clone())
                            .instrument(tracing::info_span!("app:run:main"))
                            .await
                        {
                            MainRetryAction::UseExitCode(ec) => {
                                match std::fs::remove_file(&lock_path) {
                                    Ok(_) => {
                                        debug!("lock:removed file:{:?}", &lock_path);
                                    }
                                    Err(rm_err) => {
                                        error!("lock file:{:?} error:{}", &lock_path, rm_err);
                                    }
                                }

                                rc = Err(MainEarlyReturn::UseExitCode(ec));
                            }
                            MainRetryAction::UseRestartPolicy => {
                                app.main(&mut wora, exec.clone(), fs.clone(), metrics_sender.clone())
                                    .instrument(tracing::info_span!("app:run:main:retry"))
                                    .await;
                            }
                            MainRetryAction::Success => {}
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

            drop(guard);

            match std::fs::remove_file(&lock_path) {
                Ok(_) => {
                    debug!("lock:removed file:{:?}", &lock_path);
                }
                Err(rm_err) => {
                    error!("lock file:{:?} error:{}", &lock_path, rm_err);
                }
            }

            let _ = o11y_tx.send(o11y_new_ev_finish()).await;

            rc
        }
        Err(err) => {
            error!("lock file:{:?} error:{:?}", &lock_path, err);

            match std::fs::remove_file(&lock_path) {
                Ok(_) => {
                    debug!("lock:removed file:{:?}", &lock_path);
                }
                Err(rm_err) => {
                    error!("lock file:{:?} error:{}", &lock_path, rm_err);
                }
            }

            let _ = o11y_tx.send(o11y_new_ev_finish()).await;

            return Err(MainEarlyReturn::UseExitCode(111)); // TODO fix print and return
        }
    }
}
