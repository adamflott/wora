#![allow(dead_code)]

//! Write Once Run Anywhere (WORA)
//!
//! A framework for building applications (daemons, etc) that run in different environments (Linux, Kubernetes, etc).
//!
//! Just like Java's claim, it really doesn't run everywhere. no_std or embedded environments are not supported.

use std::path::PathBuf;

use async_trait::async_trait;
use caps::CapSet;
use directories::ProjectDirs;
use libc::{RLIM_INFINITY, SIGINT};
use libc::{SIGHUP, SIGQUIT, SIGTERM, SIGUSR1, SIGUSR2};
use log::{debug, error, info, trace, warn};
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

        let (tx, rx1) = mpsc::channel(16);

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

pub trait Executor {
    fn dirs(&self) -> &Dirs;
    fn disable_memory_limits(&self) -> Result<(), SetupFailure>;
    //  Disable paging memory to swap
    fn disable_paging_mem_to_swap(&self) -> Result<(), SetupFailure>;
    //  Disable core files
    fn disable_core_dumps(&self) -> Result<(), SetupFailure>;
    // Switch to a non-root user
    fn run_as_user_and_group(&self, user_name: &str, group_name: &str) -> Result<(), SetupFailure>;
    fn has_no_caps(&self) -> Result<bool, SetupFailure>;
    fn is_running_as_root(&self) -> bool;
}

#[async_trait]
pub trait AsyncExecutor: Executor {
    async fn setup(&mut self, wora: &Wora) -> Result<(), SetupFailure>;
    async fn is_ready(&self, wora: &Wora) -> bool;
    async fn end(&self, wora: &Wora);
}

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

impl Executor for UnixLike {
    fn dirs(&self) -> &Dirs {
        &self.dirs
    }

    fn disable_memory_limits(&self) -> Result<(), SetupFailure> {
        setrlimit(Resource::RLIMIT_MEMLOCK, RLIM_INFINITY, RLIM_INFINITY)?;
        Ok(())
    }

    fn disable_paging_mem_to_swap(&self) -> Result<(), SetupFailure> {
        mlockall(MlockAllFlags::all())?;
        Ok(())
    }

    fn disable_core_dumps(&self) -> Result<(), SetupFailure> {
        setrlimit(Resource::RLIMIT_CORE, 0, 0)?;
        Ok(())
    }

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

pub struct UnixLikeSystem {
    unix: UnixLike,
}

impl UnixLikeSystem {
    pub async fn new(app_name: &str) -> Self {
        let unix = UnixLike::new(app_name).await;
        UnixLikeSystem { unix }
    }
}

impl Executor for UnixLikeSystem {
    fn dirs(&self) -> &Dirs {
        &self.unix.dirs
    }

    fn disable_memory_limits(&self) -> Result<(), SetupFailure> {
        self.unix.disable_memory_limits()
    }

    fn disable_paging_mem_to_swap(&self) -> Result<(), SetupFailure> {
        self.unix.disable_paging_mem_to_swap()
    }

    fn disable_core_dumps(&self) -> Result<(), SetupFailure> {
        self.unix.disable_core_dumps()
    }

    fn run_as_user_and_group(&self, user_name: &str, group_name: &str) -> Result<(), SetupFailure> {
        self.unix.run_as_user_and_group(user_name, group_name)
    }

    fn has_no_caps(&self) -> Result<bool, SetupFailure> {
        self.unix.has_no_caps()
    }

    fn is_running_as_root(&self) -> bool {
        self.unix.is_running_as_root()
    }
}

#[async_trait]
impl AsyncExecutor for UnixLikeSystem {
    async fn setup(&mut self, _wora: &Wora) -> Result<(), SetupFailure> {
        Ok(())
    }

    async fn is_ready(&self, _wora: &Wora) -> bool {
        true
    }

    async fn end(&self, _wora: &Wora) {
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

impl Executor for UnixLikeUser {
    fn dirs(&self) -> &Dirs {
        &self.unix.dirs
    }

    fn disable_memory_limits(&self) -> Result<(), SetupFailure> {
        self.unix.disable_memory_limits()
    }

    fn disable_paging_mem_to_swap(&self) -> Result<(), SetupFailure> {
        self.unix.disable_paging_mem_to_swap()
    }

    fn disable_core_dumps(&self) -> Result<(), SetupFailure> {
        self.unix.disable_core_dumps()
    }

    fn run_as_user_and_group(&self, user_name: &str, group_name: &str) -> Result<(), SetupFailure> {
        self.unix.run_as_user_and_group(user_name, group_name)
    }

    fn has_no_caps(&self) -> Result<bool, SetupFailure> {
        self.unix.has_no_caps()
    }

    fn is_running_as_root(&self) -> bool {
        self.unix.is_running_as_root()
    }
}

#[async_trait]
impl AsyncExecutor for UnixLikeUser {
    async fn setup(&mut self, wora: &Wora) -> Result<(), SetupFailure> {
        let dirs = &wora.dirs;

        println!("exec: setup: io: chdir({:?}): trying", &wora.dirs.root_dir);
        chdir(&wora.dirs.root_dir)?;
        println!("exec: setup: io: chdir({:?}): success", &wora.dirs.root_dir);

        for dir in [
            &dirs.log_root_dir,
            &dirs.metadata_root_dir,
            &dirs.data_root_dir,
            &dirs.runtime_root_dir,
            &dirs.cache_root_dir,
        ] {
            // TODO - maybe theres a better way to handle logging before the app sets up logging? use a simple internal logger?
            println!("exec: setup: io: create dir:{:?}: trying", dir);
            std::fs::create_dir_all(dir)?;
            println!("exec: setup: io: create dir:{:?}: success", dir);
        }

        Ok(())
    }

    async fn is_ready(&self, _wora: &Wora) -> bool {
        true
    }

    async fn end(&self, _wora: &Wora) {
        ()
    }
}

#[async_trait]
pub trait App {
    fn name(&self) -> &'static str;
    async fn setup(
        &mut self,
        wora: &Wora,
        exec: &(dyn Executor + Send + Sync),
    ) -> Result<(), SetupFailure>;
    async fn main(
        &mut self,
        wora: &mut Wora,
        exec: &(dyn Executor + Send + Sync),
    ) -> MainRetryAction;
    async fn end(&mut self, wora: &Wora, exec: &(dyn Executor + Send + Sync));
}

pub async fn exec_async_runner(
    mut exec: impl AsyncExecutor + Sync + Send,
    mut app: impl App,
) -> Result<(), MainEarlyReturn> {
    let mut wora = Wora::new(exec.dirs(), app.name().to_string())?;

    exec.setup(&wora).await?;
    match app.setup(&wora, &exec).await {
        Ok(_) => {
            info!("dirs.root: {:?}", wora.dirs.root_dir);
            info!("dirs.log: {:?}", wora.dirs.log_root_dir);
            info!("dirs.metadata: {:?}", wora.dirs.metadata_root_dir);
            info!("dirs.data: {:?}", wora.dirs.data_root_dir);
            info!("dirs.runtime: {:?}", wora.dirs.runtime_root_dir);
            info!("dirs.cache: {:?}", wora.dirs.cache_root_dir);

            info!("host: {:?}", wora.stats_from_start().host_info);

            info!(
                "recursively watching metadata directory for changes: {:?}",
                &wora.dirs.metadata_root_dir
            );
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

            if exec.is_ready(&wora).await {
                match app.main(&mut wora, &exec).await {
                    MainRetryAction::UseExitCode(ec) => {
                        info!("{}", Event::PhaseEnd(Phase::Main).to_string());
                        return Err(MainEarlyReturn::UseExitCode(ec));
                    }
                    MainRetryAction::UseRestartPolicy => {
                        info!("{}", Event::PhaseEnd(Phase::Main).to_string());

                        info!("{}", Event::PhaseBegin(Phase::Main).to_string());
                        app.main(&mut wora, &exec).await;
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
            app.end(&wora, &exec).await;
            info!("{}", Event::PhaseEnd(Phase::End).to_string());
        }
        Err(setup_err) => {
            error!("{:?}", setup_err)
        }
    }

    debug!("Running executor end()");
    exec.end(&wora).await;
    debug!("done Running executor end()");

    Ok(())
}
