//! Write Once Run Anywhere (WORA)
//!
//! A framework for building applications (daemons, etc) that run in different environments (Linux, Kubernetes, etc).
//!
//! Just like Java's claim, it really doesn't run everywhere. no_std or embedded environments are not supported.

use std::path::PathBuf;

use async_trait::async_trait;
use chrono::Utc;
use libc::{SIGHUP, SIGINT, SIGQUIT, SIGTERM, SIGUSR1, SIGUSR2};
use nix::unistd::getpid;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use tokio::signal::unix::SignalKind;
use tokio::sync::mpsc;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::Instrument;
use tracing::{error, info, trace, warn};

pub mod dirs;
pub mod errors;
pub mod events;
pub mod exec;
pub mod exec_unix;
pub mod metrics;
pub mod restart_policy;

use crate::dirs::Dirs;
use crate::errors::{MainEarlyReturn, SetupFailure, WoraSetupError};
use events::*;
use exec::*;
use metrics::*;
use restart_policy::*;

const EVENT_BUFFER_SIZE: usize = 1024;

/// All possible system stats
#[derive(Debug)]
pub struct Stats {
    pub host_info: statgrab::HostInfo,
    pub cpu: statgrab::CPUStats,
    pub memory: statgrab::MemStats,
    pub load: statgrab::LoadStats,
    pub user: statgrab::UserStats,
    pub swap: statgrab::SwapStats,
    pub fs: Vec<statgrab::FilesystemStats>,
    pub disk_io: Vec<statgrab::DiskIOStats>,
    pub net_io: Vec<statgrab::NetworkIOStats>,
    pub net_if: Vec<statgrab::NetworkIfaceStats>,
    pub page: statgrab::PageStats,
    pub process: Vec<statgrab::ProcessStats>,
    pub process_count: statgrab::ProcessCount,
}

/// All workloads will be able to access the WORA API
pub struct Wora {
    /// application name
    pub app_name: String,
    /// common directories for all executors
    pub dirs: Dirs,
    /// statgrab handle
    sg: statgrab::SGHandle,
    /// last stat collection
    stats: Stats,
    /// process id
    pid: nix::unistd::Pid,
    /// `Event` producer
    pub sender: Sender<Event>,
    /// `Event` recevier
    pub receiver: Receiver<Event>,
    /// leadership state
    pub leadership: Leadership,
}

#[derive(Clone, Debug)]
pub enum Leadership {
    Leader,
    Follower,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HealthState {
    Ok,
    Suspended,
    TryAgain,
    Failed,
    Unknown,
}

/// WORA API
impl Wora {
    pub fn new(dirs: &Dirs, app_name: String, ev_buf_size: usize) -> Result<Wora, WoraSetupError> {
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
                return Err(WoraSetupError::DirectoryDoesNotExistOnFilesystem(
                    dir.clone(),
                ));
            }
        }

        let sg = statgrab::init(true)?;

        let stats = Stats {
            host_info: sg.get_host_info(),
            cpu: sg.get_cpu_stats(),
            memory: sg.get_mem_stats(),
            load: sg.get_load_stats(),
            user: sg.get_user_stats(),
            swap: sg.get_swap_stats(),
            fs: sg.get_fs_stats(),
            disk_io: sg.get_disk_io_stats(),
            net_io: sg.get_network_io_stats(),
            net_if: sg.get_network_iface_stats(),
            page: sg.get_page_stats(),
            process: sg.get_process_stats(),
            process_count: sg.get_process_count_of(statgrab::ProcessCountSource::Entire),
        };

        let pid = getpid();

        let (tx, rx) = mpsc::channel(ev_buf_size);

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

        Ok(Wora {
            app_name,
            dirs: dirs.clone(),
            sg,
            stats,
            pid,
            sender: tx,
            receiver: rx,
            leadership: Leadership::Unknown,
        })
    }

    pub fn stats_from_start(&self) -> &Stats {
        &self.stats
    }

    pub fn dirs(&self) -> &Dirs {
        &self.dirs
    }

    pub async fn emit_event(&self, ev: Event) -> () {
        match self.sender.send(ev).await {
            Ok(_) => {}
            Err(send_err) => {
                error!("event:send:error: {:?}", send_err);
            }
        }
        ()
    }

    pub fn host_os_name(&self) -> &str {
        &self.stats.host_info.os_name
    }
    pub fn host_os_release(&self) -> &str {
        &self.stats.host_info.os_release
    }
    pub fn host_os_version(&self) -> &str {
        &self.stats.host_info.os_version
    }
    pub fn host_platform(&self) -> &str {
        &self.stats.host_info.platform
    }
    pub fn host_hostname(&self) -> &str {
        &self.stats.host_info.hostname
    }
    pub fn host_cpu_count(&self) -> u32 {
        self.stats.host_info.ncpus
    }
    pub fn host_cpu_max(&self) -> u32 {
        self.stats.host_info.maxcpus
    }
    pub fn host_type(&self) -> String {
        format!("{:?}", self.stats.host_info.host_state)
    }
}

pub trait Config {
    type ConfigT: Default;
    fn parse_main_config_file(data: String) -> Result<Self::ConfigT, Box<dyn std::error::Error>>;

    fn parse_supplemental_config_file(
        _file_path: PathBuf,
        _data: String,
    ) -> Result<Self::ConfigT, Box<dyn std::error::Error>> {
        Ok(Self::ConfigT::default())
    }
}

pub struct NoConfig;
impl Config for NoConfig {
    type ConfigT = ();
    fn parse_main_config_file(data: String) -> Result<Self::ConfigT, Box<dyn std::error::Error>> {
        Ok(())
    }
}
#[async_trait]
pub trait App {
    type AppMetricsProducer: MetricProcessor;
    type AppConfig: Config;

    fn name(&self) -> &'static str;

    async fn setup(
        &mut self,
        wora: &Wora,
        exec: &(dyn Executor + Send + Sync),
        metrics: &(dyn MetricProcessor + Send + Sync),
    ) -> Result<(), SetupFailure>;

    async fn main(
        &mut self,
        wora: &mut Wora,
        exec: &(dyn Executor + Send + Sync),
        metrics: &mut (dyn MetricProcessor + Send + Sync),
    ) -> MainRetryAction;

    async fn is_healthy() -> HealthState;

    async fn end(
        &mut self,
        wora: &Wora,
        exec: &(dyn Executor + Send + Sync),
        metrics: &(dyn MetricProcessor + Send + Sync),
    );
}

fn async_watcher() -> notify::Result<(RecommendedWatcher, Receiver<notify::Result<notify::Event>>)>
{
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

pub async fn exec_async_runner(
    mut exec: impl AsyncExecutor + Sync + Send + Executor,
    mut app: impl App + Sync + Send + 'static,
    mut metrics: impl MetricProcessor + Sync + Send,
) -> Result<(), MainEarlyReturn> {
    let mut wora = Wora::new(exec.dirs(), app.name().to_string(), EVENT_BUFFER_SIZE)?;

    let mut exec_metrics = MetricExecutorTimings::default();

    metrics.add(&Metric::Counter("exec:run:setup:start".into()));
    exec.setup(&wora, &metrics)
        .instrument(tracing::info_span!("exec:run:setup"))
        .await?;
    exec_metrics.setup_finish = Some(Utc::now());
    metrics.add(&Metric::Counter("exec:run:setup:finish".into()));

    let mut app_metrics = MetricAppTimings::default();
    app_metrics.setup_finish = Some(Utc::now());
    match app
        .setup(&wora, &exec, &metrics)
        .instrument(tracing::info_span!("app:run:setup"))
        .await
    {
        Ok(_) => {
            info!(
                host.hostname = wora.host_hostname(),
                host.platform = wora.host_platform(),
                host.os_name = wora.host_os_name(),
                host.os_release = wora.host_os_release(),
                host.os_version = wora.host_os_version(),
                host.cpu_count = wora.host_cpu_count(),
                host.cpu_max = wora.host_cpu_max(),
                host.machine_type = wora.host_type()
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
            watcher.watch(
                std::path::Path::new(&wora.dirs.metadata_root_dir),
                RecursiveMode::Recursive,
            )?;
            let ev_sender = wora.sender.clone();

            tokio::spawn(async move {
                while let Some(res) = watch_rx.recv().await {
                    match res {
                        Ok(event) => {
                            info!("changed: {:?}", event);
                            for path in event.paths {
                                match tokio::fs::read_to_string(&path).await {
                                    Ok(data) => {
                                        ev_sender
                                            .send(Event::ConfigChange(path.clone(), data))
                                            .await;
                                    }
                                    Err(_) => {}
                                }
                            }
                        }
                        Err(e) => error!("watch error: {:?}", e),
                    }
                }
            });

            info!(process_id = wora.pid.to_string(), app_name = app.name());

            if exec
                .is_ready(&wora, &metrics)
                .instrument(tracing::info_span!("exec:run:is_ready"))
                .await
            {
                match app
                    .main(&mut wora, &exec, &mut metrics)
                    .instrument(tracing::info_span!("app:run:main"))
                    .await
                {
                    MainRetryAction::UseExitCode(ec) => {
                        return Err(MainEarlyReturn::UseExitCode(ec));
                    }
                    MainRetryAction::UseRestartPolicy => {
                        app.main(&mut wora, &exec, &mut metrics)
                            .instrument(tracing::info_span!("app:run:main:retry"))
                            .await;
                    }
                    MainRetryAction::Success => {}
                }
            } else {
                warn!(comp = "exec", method = "run", is_ready = false);
            }

            app.end(&wora, &exec, &metrics)
                .instrument(tracing::info_span!("app:run:end"))
                .await;
        }
        Err(setup_err) => {
            error!("{:?}", setup_err)
        }
    }

    exec_metrics.end_start = Some(Utc::now());
    metrics.add(&Metric::Counter("exec:run:end:start".into()));
    exec.end(&wora, &metrics)
        .instrument(tracing::info_span!("exec:run:end"))
        .await;
    exec_metrics.end_start = Some(Utc::now());
    metrics.add(&Metric::Counter("exec:run:end:finish".into()));

    Ok(())
}
