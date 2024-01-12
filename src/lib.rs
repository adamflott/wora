//! Write Once Run Anywhere (WORA)
//!
//! A framework for building applications (daemons, etc) that run in different environments (Linux, Kubernetes, etc).
//!
//! Just like Java's claim, it really doesn't run everywhere. no_std or embedded environments are not supported.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use libc::{SIGHUP, SIGINT, SIGQUIT, SIGTERM, SIGUSR1, SIGUSR2};
use nix::unistd::getpid;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use proc_lock::try_lock;
use serde::Serialize;
use sysinfo::{CpuExt, NetworkExt, NetworksExt, SystemExt};
use tokio::signal::unix::SignalKind;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::Instrument;
use tracing::{debug, error, info, trace, warn};
use vfs::async_vfs::filesystem::AsyncFileSystem;

pub mod dirs;
pub mod errors;
pub mod events;
pub mod exec;
pub mod exec_unix;
pub mod metrics;
pub mod restart_policy;

use crate::dirs::Dirs;
use crate::errors::{MainEarlyReturn, WoraSetupError};
use events::*;
use exec::*;
use metrics::*;
use restart_policy::*;

const EVENT_BUFFER_SIZE: usize = 1024;


#[derive(Clone, Debug, Serialize)]
pub struct HostInfo {
    pub os_name: String,
    pub os_version: Option<String>,
    pub kernel_version: Option<String>,
    pub architecture: Option<String>,
    pub hostname: Option<String>,
    pub ncpus: usize,
    pub maxcpus: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct Cpu {
    name: String,
    brand: String,
    freq: u64,
    usage: f32,
}

#[derive(Clone, Debug, Serialize)]
pub struct MemStats {
    pub total: u64,
    pub free: u64,
    pub used: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct SwapStats {
    pub total: u64,
    pub used: u64,
    pub free: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct LoadAvg {
    pub one: f64,
    pub five: f64,
    pub fifteen: f64,
}

#[derive(Clone, Debug, Serialize)]
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

/// System stats/information from `sysinfo`
#[derive(Clone, Debug, Serialize)]
pub struct Stats {
    pub host_info: HostInfo,
    pub cpu: Vec<Cpu>,
    pub memory: MemStats,
    pub load: LoadAvg,
    pub swap: SwapStats,
    pub fs: Vec<Disk>,
    pub net_io: HashMap<String, NetIO>,
}

/// All workloads will be able to access the WORA API
pub struct Wora<T> {
    /// application name
    pub app_name: String,
    /// common directories for all executors
    pub dirs: Dirs,
    /// statgrab handle
    pub si: Arc<sysinfo::System>,
    /// last stat collection
    pub stats: Stats,
    /// process id
    pid: nix::unistd::Pid,
    /// `Event` producer
    pub sender: Sender<Event<T>>,
    /// `Event` receiver
    pub receiver: Receiver<Event<T>>,
    /// leadership state
    pub leadership: Leadership,
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
impl<T: std::fmt::Debug + Send + Sync + 'static> Wora<T> {
    pub fn new(
        dirs: &Dirs,
        app_name: String,
        ev_buf_size: usize,
    ) -> Result<Wora<T>, WoraSetupError> {
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

        let mut sys = sysinfo::System::new_all();
        sys.refresh_all();

        let osinfo = os_info::get();

        let mut cpus = vec![];
        for cpu in sys.cpus() {
            cpus.push(Cpu {
                name: cpu.name().to_string(),
                brand: cpu.brand().to_string(),
                freq: cpu.frequency(),
                usage: cpu.cpu_usage(),
            })
        }
        let fs = vec![];

        let mut net_io = HashMap::new();
        for (if_name, net_data) in sys.networks().iter() {
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

        let stats = Stats {
            host_info: HostInfo {
                os_name: sys.distribution_id(),
                os_version: sys.os_version(),
                kernel_version: sys.kernel_version(),
                architecture: osinfo.architecture().map(|v| v.to_string()),
                hostname: sys.host_name(),
                ncpus: sys.physical_core_count().unwrap_or(0),
                maxcpus: sys.cpus().len(),
            },
            cpu: cpus,
            memory: MemStats {
                total: sys.total_memory(),
                free: sys.free_memory(),
                used: sys.used_memory(),
            },
            load: LoadAvg {
                one: sys.load_average().one,
                five: sys.load_average().five,
                fifteen: sys.load_average().fifteen,
            },
            swap: SwapStats {
                total: sys.total_swap(),
                used: sys.used_swap(),
                free: sys.free_swap(),
            },
            fs: fs,
            net_io: net_io,
        };

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

        Ok(Wora {
            app_name,
            dirs: dirs.clone(),
            si: Arc::new(sys),
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

    pub async fn emit_event(&self, ev: Event<T>) -> () {
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
    pub fn host_os_version(&self) -> Option<String> {
        self.stats.host_info.os_version.clone()
    }
    pub fn host_architecture(&self) -> Option<String> {
        self.stats.host_info.architecture.clone()
    }
    pub fn host_hostname(&self) -> Option<String> {
        self.stats.host_info.hostname.clone()
    }
    pub fn host_cpu_count(&self) -> usize {
        self.stats.host_info.ncpus
    }
    pub fn host_cpu_max(&self) -> usize {
        self.stats.host_info.maxcpus
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
    fn parse_main_config_file(_data: String) -> Result<Self::ConfigT, Box<dyn std::error::Error>> {
        Ok(())
    }
}
#[async_trait]
pub trait App<T> {
    type AppMetricsProducer: MetricProcessor;
    type AppConfig: Config;

    fn name(&self) -> &'static str;

    fn allow_concurrent_executions(&self) -> bool {
        false
    }

    async fn setup(
        &mut self,
        wora: &Wora<T>,
        exec: &(dyn Executor + Send + Sync),
        fs: impl AsyncFileSystem,
        metrics: &(dyn MetricProcessor + Send + Sync),
    ) -> Result<(), Box<dyn std::error::Error>>;

    async fn main(
        &mut self,
        wora: &mut Wora<T>,
        exec: &(dyn Executor + Send + Sync),
        metrics: &mut (dyn MetricProcessor + Send + Sync),
    ) -> MainRetryAction;

    async fn is_healthy() -> HealthState;

    async fn end(
        &mut self,
        wora: &Wora<T>,
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

// TODO create a non-file locking variant

/// Run apps via an `async` based executor
pub async fn exec_async_runner<T: std::fmt::Debug + Send + Sync + 'static>(
    mut exec: impl AsyncExecutor<T> + Sync + Send + Executor,
    mut app: impl App<T> + Sync + Send + 'static,
    fs: impl AsyncFileSystem,
    mut metrics: impl MetricProcessor + Sync + Send,
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
    match try_lock(&lock) {
        Ok(guard) => {
            let mut wora = Wora::new(exec.dirs(), app.name().to_string(), EVENT_BUFFER_SIZE)?;

            let mut exec_metrics = MetricExecutorTimings::default();

            //metrics.add(&Metric::Counter("exec:run:setup:start".into()));
            exec.setup(&wora, &fs, &metrics)
                .instrument(tracing::info_span!("exec:run:setup"))
                .await?;
            exec_metrics.setup_finish = Some(Utc::now());
            //metrics.add(&Metric::Counter("exec:run:setup:finish".into()));

            let mut app_metrics = MetricAppTimings::default();
            app_metrics.setup_finish = Some(Utc::now());
            match app
                .setup(&wora, &exec, fs, &metrics)
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
                                                match ev_sender
                                                    .send(Event::ConfigChange(path.clone(), data))
                                                    .await
                                                {
                                                    Ok(_) => {}
                                                    Err(send_err) => {
                                                        error!("send error: {:?}", send_err);
                                                    }
                                                }
                                            }
                                            Err(read_err) => {
                                                error!("read error: {:?}", read_err);
                                            }
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
                                match std::fs::remove_file(&lock_path) {
                                    Ok(_) => {
                                        debug!("lock:removed file:{:?}", &lock_path);
                                    }
                                    Err(rm_err) => {
                                        error!("lock file:{:?} error:{}", &lock_path, rm_err);
                                    }
                                }

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
            //metrics.add(&Metric::Counter("exec:run:end:start".into()));
            exec.end(&wora, &metrics)
                .instrument(tracing::info_span!("exec:run:end"))
                .await;
            exec_metrics.end_start = Some(Utc::now());
            //metrics.add(&Metric::Counter("exec:run:end:finish".into()));

            drop(guard);

            match std::fs::remove_file(&lock_path) {
                Ok(_) => {
                    debug!("lock:removed file:{:?}", &lock_path);
                }
                Err(rm_err) => {
                    error!("lock file:{:?} error:{}", &lock_path, rm_err);
                }
            }

            Ok(())
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

            return Err(MainEarlyReturn::UseExitCode(111)); // TODO fix print and return
        }
    }
}
