#![allow(dead_code)]

//! Write Once Run Anywhere (WORA)
//!
//! A framework for building applications (daemons, etc) that run in different environments (Linux, Kubernetes, etc).
//!
//! Just like Java's claim, it really doesn't run everywhere. no_std or embedded environments are not supported.

use std::collections::HashMap;
use std::path::PathBuf;

use async_trait::async_trait;
use caps::CapSet;
use directories::ProjectDirs;
use libc::{RLIM_INFINITY, SIGINT};
use libc::{SIGHUP, SIGQUIT, SIGTERM, SIGUSR1, SIGUSR2};
use log::{debug, error, info};
use nix::sys::{
    mman::{mlockall, MlockAllFlags},
    resource::{setrlimit, Resource},
};
use nix::unistd::{chdir, getpid};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use tokio::signal::unix::SignalKind;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use users::{get_effective_username, get_user_by_name, switch::set_both_uid};

pub mod errors;
pub mod events;
pub mod restart_policy;

use crate::errors::{MainEarlyReturn, SetupFailure, WoraSetupError};
use events::*;
use restart_policy::*;

/// All possible system stats
#[derive(Debug)]
pub struct Stats {
    host_info: statgrab::HostInfo,
    cpu: statgrab::CPUStats,
    memory: statgrab::MemStats,
    load: statgrab::LoadStats,
    user: statgrab::UserStats,
    swap: statgrab::SwapStats,
    fs: Vec<statgrab::FilesystemStats>,
    disk_io: Vec<statgrab::DiskIOStats>,
    net_io: Vec<statgrab::NetworkIOStats>,
    net_if: Vec<statgrab::NetworkIfaceStats>,
    page: statgrab::PageStats,
    process: Vec<statgrab::ProcessStats>,
    process_count: statgrab::ProcessCount,
}

#[derive(Clone, Debug)]
pub struct Dirs {
    pub root_dir: PathBuf,
    pub log_root_dir: PathBuf,
    pub metadata_root_dir: PathBuf,
    pub data_root_dir: PathBuf,
    pub runtime_root_dir: PathBuf,
    pub cache_root_dir: PathBuf,
}

/// All workloads will be able to access the WORA API
pub struct Wora {
    pub app_name: String,
    pub dirs: Dirs,
    sg: statgrab::SGHandle,
    stats: Stats,
    fs_notify: RecommendedWatcher,
    pid: nix::unistd::Pid,
    pub sender: Sender<Event>,
    pub receiver: Receiver<Event>,
}

/// WORA API
impl Wora {
    pub fn new(dirs: &Dirs, app_name: String) -> Result<Wora, WoraSetupError> {
        println!("checking executor directories exist...");

        for dir in [
            &dirs.root_dir,
            &dirs.log_root_dir,
            &dirs.metadata_root_dir,
            &dirs.data_root_dir,
            &dirs.runtime_root_dir,
            &dirs.cache_root_dir,
        ] {
            if !dir.exists() {
                eprintln!("directory {:?} does not exist", dir);
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

        let watcher = notify::recommended_watcher(|res| match res {
            Ok(event) => println!("event: {:?}", event),
            Err(e) => println!("watch error: {:?}", e),
        })?;

        let pid = getpid();

        let (tx, rx1) = mpsc::channel(16); // TODO

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
            fs_notify: watcher,
            pid,
            sender: tx,
            receiver: rx1,
        })
    }

    pub fn stats_from_start(&self) -> &Stats {
        &self.stats
    }

    pub fn dirs(&self) -> &Dirs {
        &self.dirs
    }
}

pub fn new_arg_parser_clap<Args: clap::Parser>() -> Result<Args, clap::error::Error> {
    clap::Parser::try_parse()
}

///
#[async_trait]
pub trait MetricProcessor {
    async fn setup(&mut self) -> Result<(), SetupFailure>;
    async fn add(&mut self, m: Metric) -> Result<(), SetupFailure>;
    async fn end(&self);
}

pub enum Metric {
    Counter(String),
}

/// Common methods for all `Executors`
pub trait Executor {
    ///
    fn dirs(&self) -> &Dirs;

    /// Disable memory limits
    fn disable_memory_limits(&self) -> Result<(), SetupFailure> {
        setrlimit(Resource::RLIMIT_MEMLOCK, RLIM_INFINITY, RLIM_INFINITY)?;
        Ok(())
    }

    //  Disable paging memory to swap
    fn disable_paging_mem_to_swap(&self) -> Result<(), SetupFailure> {
        mlockall(MlockAllFlags::all())?;
        Ok(())
    }

    //  Disable core files
    fn disable_core_dumps(&self) -> Result<(), SetupFailure> {
        setrlimit(Resource::RLIMIT_CORE, 0, 0)?;
        Ok(())
    }

    // Switch to a non-root user
    fn run_as_user_and_group(&self, user_name: &str, group_name: &str) -> Result<(), SetupFailure> {
        let newuser = get_user_by_name(user_name)
            .ok_or(SetupFailure::UnknownSystemUser(user_name.to_string()))?;
        set_both_uid(newuser.uid(), newuser.uid())?;

        Ok(())
    }

    fn has_no_caps(&self) -> Result<bool, SetupFailure> {
        let effective = caps::read(None, CapSet::Effective)?;
        Ok(effective.is_empty())
    }

    fn is_running_as_root(&self) -> bool {
        get_effective_username().unwrap_or("".into()) == "root"
    }
}

pub struct MetricsProducerStdout {
    handle: tokio::io::Stdout,
    counters: HashMap<String, u64>,
}

impl MetricsProducerStdout {
    pub async fn new() -> Self {
        Self {
            handle: tokio::io::stdout(),
            counters: HashMap::new(),
        }
    }
}
#[async_trait]
impl MetricProcessor for MetricsProducerStdout {
    async fn setup(&mut self) -> Result<(), SetupFailure> {
        Ok(())
    }

    async fn add(&mut self, m: Metric) -> Result<(), SetupFailure> {
        match m {
            Metric::Counter(key) => match self.counters.get_mut(&key) {
                Some(val) => {
                    *val += 1;
                    info!("metrics:stdout:counter:incr: key:{};count:{}", &key, val);
                }
                None => {
                    info!("metrics:stdout:counter:incr: key:{};count:0", &key);
                    self.counters.insert(key, 1);
                }
            },
        }
        Ok(())
    }

    async fn end(&self) {}
}

#[async_trait]
pub trait AsyncExecutor: Executor {
    async fn setup(
        &mut self,
        wora: &Wora,
        metrics: &(dyn MetricProcessor + Send + Sync),
    ) -> Result<(), SetupFailure>;
    async fn is_ready(&self, wora: &Wora, metrics: &(dyn MetricProcessor + Send + Sync)) -> bool;
    async fn end(&self, wora: &Wora, metrics: &(dyn MetricProcessor + Send + Sync));
}

#[derive(Debug)]
pub struct UnixLike {
    pub dirs: Dirs,
}

impl UnixLike {
    pub async fn new(_app_name: &str) -> Self {
        let dirs = Dirs {
            root_dir: PathBuf::from("/"),
            log_root_dir: PathBuf::from("/var/log"),
            metadata_root_dir: PathBuf::from("/etc/"),
            data_root_dir: PathBuf::from("/usr/local/data"),
            runtime_root_dir: PathBuf::from("/run/"),
            cache_root_dir: PathBuf::from("/var/run/"),
        };
        UnixLike { dirs }
    }
}

#[async_trait]
impl MetricProcessor for UnixLike {
    async fn setup(&mut self) -> Result<(), SetupFailure> {
        Ok(())
    }
    async fn add(&mut self, _m: Metric) -> Result<(), SetupFailure> {
        Ok(())
    }
    async fn end(&self) {
        ()
    }
}

impl Executor for UnixLike {
    fn dirs(&self) -> &Dirs {
        &self.dirs
    }
}

pub struct UnixLikeSystem {
    unix: UnixLike,
}

impl UnixLikeSystem {
    pub async fn new(app_name: &str) -> Self {
        let unix = UnixLike::new(app_name).await;
        UnixLikeSystem { unix }
    }
}

#[async_trait]
impl MetricProcessor for UnixLikeSystem {
    async fn add(&mut self, _m: Metric) -> Result<(), SetupFailure> {
        Ok(())
    }
    async fn setup(&mut self) -> Result<(), SetupFailure> {
        Ok(())
    }
    async fn end(&self) {
        ()
    }
}

impl Executor for UnixLikeSystem {
    fn dirs(&self) -> &Dirs {
        &self.unix.dirs
    }
}

#[async_trait]
impl AsyncExecutor for UnixLikeSystem {
    async fn setup(
        &mut self,
        _wora: &Wora,
        _metrics: &(dyn MetricProcessor + Send + Sync),
    ) -> Result<(), SetupFailure> {
        Ok(())
    }

    async fn is_ready(&self, _wora: &Wora, _metrics: &(dyn MetricProcessor + Send + Sync)) -> bool {
        true
    }

    async fn end(&self, _wora: &Wora, _metrics: &(dyn MetricProcessor + Send + Sync)) {
        ()
    }
}

pub struct UnixLikeUser {
    unix: UnixLike,
}

impl UnixLikeUser {
    pub async fn new(app_name: &str) -> Self {
        let proj_dirs = ProjectDirs::from("com", "wora", app_name).unwrap();

        let dirs = Dirs {
            root_dir: PathBuf::from("/"),
            log_root_dir: proj_dirs.data_local_dir().to_path_buf(),
            metadata_root_dir: proj_dirs.config_dir().to_path_buf(),
            data_root_dir: proj_dirs.data_dir().to_path_buf(),
            runtime_root_dir: proj_dirs
                .runtime_dir()
                .unwrap_or(std::env::temp_dir().as_path())
                .to_path_buf(),
            cache_root_dir: proj_dirs.cache_dir().to_path_buf(),
        };

        let mut unix = UnixLike::new(app_name).await;
        unix.dirs = dirs;

        UnixLikeUser { unix }
    }
}

#[async_trait]
impl MetricProcessor for UnixLikeUser {
    async fn setup(&mut self) -> Result<(), SetupFailure> {
        Ok(())
    }
    async fn add(&mut self, _m: Metric) -> Result<(), SetupFailure> {
        Ok(())
    }
    async fn end(&self) {
        ()
    }
}

impl Executor for UnixLikeUser {
    fn dirs(&self) -> &Dirs {
        &self.unix.dirs
    }
}

#[async_trait]
impl AsyncExecutor for UnixLikeUser {
    async fn setup(
        &mut self,
        wora: &Wora,
        _metrics: &(dyn MetricProcessor + Send + Sync),
    ) -> Result<(), SetupFailure> {
        let dirs = &wora.dirs;

        println!("exec:setup:io:chdir({:?}): trying", &wora.dirs.root_dir);
        chdir(&wora.dirs.root_dir)?;
        println!("exec:setup:io:chdir({:?}): success", &wora.dirs.root_dir);

        for dir in [
            &dirs.log_root_dir,
            &dirs.metadata_root_dir,
            &dirs.data_root_dir,
            &dirs.runtime_root_dir,
            &dirs.cache_root_dir,
        ] {
            // TODO - maybe theres a better way to handle logging before the app sets up logging? use a simple internal logger?
            println!("exec:setup:io:create dir:{:?}: trying", dir);
            std::fs::create_dir_all(dir)?;
            println!("exec:setup:io:create dir:{:?}: success", dir);
        }

        Ok(())
    }

    async fn is_ready(&self, _wora: &Wora, _metrics: &(dyn MetricProcessor + Send + Sync)) -> bool {
        true
    }

    async fn end(&self, _wora: &Wora, _metrics: &(dyn MetricProcessor + Send + Sync)) {
        ()
    }
}

#[async_trait]
pub trait App {
    type AppMetricsProducer: MetricProcessor;

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
    async fn end(
        &mut self,
        wora: &Wora,
        exec: &(dyn Executor + Send + Sync),
        metrics: &(dyn MetricProcessor + Send + Sync),
    );
}

pub async fn exec_async_runner(
    mut exec: impl AsyncExecutor + Sync + Send + Executor,
    mut app: impl App,
    mut metrics: impl MetricProcessor + Sync + Send,
) -> Result<(), MainEarlyReturn> {
    let mut wora = Wora::new(exec.dirs(), app.name().to_string())?;

    exec.setup(&wora, &metrics).await?;
    match app.setup(&wora, &exec, &metrics).await {
        Ok(_) => {
            info!("dirs.root: {:?}", wora.dirs.root_dir);
            info!("dirs.log: {:?}", wora.dirs.log_root_dir);
            info!("dirs.metadata: {:?}", wora.dirs.metadata_root_dir);
            info!("dirs.data: {:?}", wora.dirs.data_root_dir);
            info!("dirs.runtime: {:?}", wora.dirs.runtime_root_dir);
            info!("dirs.cache: {:?}", wora.dirs.cache_root_dir);

            info!("host: {:?}", wora.stats_from_start().host_info);

            info!("notify:watch:dir: {:?}", &wora.dirs.metadata_root_dir);
            wora.fs_notify.watch(
                std::path::Path::new(&wora.dirs.metadata_root_dir),
                RecursiveMode::Recursive,
            )?;

            info!("{}", Event::PhaseEnd(Phase::Setup).to_string());
            info!("{}", Event::PhaseBegin(Phase::Main).to_string());
            info!(
                "{}",
                Event::Begin(app.name().to_string(), ProcessId(wora.pid)).to_string()
            );

            if exec.is_ready(&wora, &metrics).await {
                match app.main(&mut wora, &exec, &mut metrics).await {
                    MainRetryAction::UseExitCode(ec) => {
                        info!("{}", Event::PhaseEnd(Phase::Main).to_string());
                        return Err(MainEarlyReturn::UseExitCode(ec));
                    }
                    MainRetryAction::UseRestartPolicy => {
                        info!("{}", Event::PhaseEnd(Phase::Main).to_string());

                        info!("{}", Event::PhaseBegin(Phase::Main).to_string());
                        app.main(&mut wora, &exec, &mut metrics).await;
                        info!("{}", Event::PhaseEnd(Phase::Main).to_string());
                    }
                    MainRetryAction::Success => {
                        info!("{}", Event::PhaseEnd(Phase::Main).to_string());
                    }
                }
            } else {
                // TODO
            }

            info!("{}", Event::PhaseBegin(Phase::End).to_string());
            app.end(&wora, &exec, &metrics).await;
            info!("{}", Event::PhaseEnd(Phase::End).to_string());
        }
        Err(setup_err) => {
            error!("{:?}", setup_err)
        }
    }

    debug!("exec:run:end:start");
    exec.end(&wora, &metrics).await;
    debug!("exec:run:end:finish");

    Ok(())
}
