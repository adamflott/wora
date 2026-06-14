use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use directories::ProjectDirs;
use libc::{SIGHUP, SIGINT, SIGQUIT, SIGTERM, SIGUSR1, SIGUSR2};
use nix::unistd::chdir;
use tokio::signal::unix::SignalKind;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tracing::{error, trace};

use crate::Wora;
use crate::dirs::Dirs;
use crate::errors::{SetupFailure, VfsError};
use crate::events::{ControlEvent, Event};
use crate::{AsyncExecutor, WFS};

/// Shared Unix-like directory layout.
#[derive(Clone, Debug)]
pub struct UnixLike {
    /// Directories used by the executor.
    pub dirs: Dirs,
    injected_control_events: Option<Arc<Mutex<Option<Receiver<ControlEvent>>>>>,
}

impl UnixLike {
    /// Build a system-style Unix layout.
    pub async fn new(_app_name: &str) -> Self {
        let dirs = Dirs {
            root_dir: PathBuf::from("/"),
            log_root_dir: PathBuf::from("/var/log"),
            metadata_root_dir: PathBuf::from("/etc/"),
            data_root_dir: PathBuf::from("/usr/local/data"),
            runtime_root_dir: PathBuf::from("/run/"),
            cache_root_dir: PathBuf::from("/var/run/"),
            secrets_root_dir: PathBuf::from("/var/run/"),
        };
        UnixLike {
            dirs,
            injected_control_events: None,
        }
    }

    pub(crate) fn with_control_event_receiver(mut self, receiver: Receiver<ControlEvent>) -> Self {
        self.injected_control_events = Some(Arc::new(Mutex::new(Some(receiver))));
        self
    }

    fn control_event_for_signal(signum: i32) -> Option<ControlEvent> {
        match signum {
            SIGHUP => Some(ControlEvent::ReloadConfiguration),
            SIGTERM | SIGINT | SIGQUIT => Some(ControlEvent::Shutdown(Some(chrono::Utc::now().naive_utc()))),
            SIGUSR1 => Some(ControlEvent::LogRotation),
            SIGUSR2 => None,
            _ => None,
        }
    }

    pub(crate) async fn spawn_runtime_event_sources<AppEv: Send + 'static>(&self, sender: Sender<Event<AppEv>>) -> Result<Vec<JoinHandle<()>>, SetupFailure> {
        if let Some(injected_controls) = &self.injected_control_events {
            let send = sender;
            let injected_controls = injected_controls.clone();
            return Ok(vec![tokio::spawn(async move {
                let Some(mut receiver) = injected_controls.lock().await.take() else {
                    return;
                };
                while let Some(control_event) = receiver.recv().await {
                    if send.send(Event::Control(control_event)).await.is_err() {
                        break;
                    }
                }
            })]);
        }

        let mut tasks = Vec::new();

        for &signum in [SIGHUP, SIGTERM, SIGQUIT, SIGUSR1, SIGUSR2, SIGINT].iter() {
            let send = sender.clone();
            let mut sig = tokio::signal::unix::signal(SignalKind::from_raw(signum))?;
            tasks.push(tokio::spawn(async move {
                loop {
                    sig.recv().await;

                    if let Some(control_event) = UnixLike::control_event_for_signal(signum) {
                        if send.send(Event::Control(control_event)).await.is_err() {
                            break;
                        }
                    }
                }
            }));
        }

        Ok(tasks)
    }

    pub(crate) async fn create_directories(&self, fs: impl WFS) -> Result<(), SetupFailure> {
        for dir in [
            &self.dirs.root_dir,
            &self.dirs.log_root_dir,
            &self.dirs.metadata_root_dir,
            &self.dirs.data_root_dir,
            &self.dirs.runtime_root_dir,
            &self.dirs.cache_root_dir,
            &self.dirs.secrets_root_dir,
        ] {
            trace!("exec:setup:io:create dir:{:?}: trying", dir);
            fs.create_dir(dir).await.map_err(|err| {
                error!("exec:setup:io:create dir:{:?}: error:{}", dir, err);
                SetupFailure::Vfs(err)
            })?;
            trace!("exec:setup:io:create dir:{:?}: success", dir);
        }

        Ok(())
    }

    pub(crate) fn chdir_root(&self) -> Result<(), SetupFailure> {
        trace!("exec:setup:io:chdir({:?}): trying", &self.dirs.root_dir);
        chdir(&self.dirs.root_dir)?;
        trace!("exec:setup:io:chdir({:?}): success", &self.dirs.root_dir);
        Ok(())
    }
}

/// Executor for system-level Unix deployments.
///
/// This executor uses paths such as `/var/log`, `/etc`, and `/run` and expects
/// the process to have permission to use them.
#[derive(Clone, Debug)]
pub struct UnixLikeSystem {
    unix: UnixLike,
}

impl UnixLikeSystem {
    /// Create a system-level Unix executor.
    pub async fn new(app_name: &str) -> Self {
        let unix = UnixLike::new(app_name).await;
        UnixLikeSystem { unix }
    }

    /// Inject a control-event receiver for deterministic testing.
    pub fn with_control_event_receiver(mut self, receiver: Receiver<ControlEvent>) -> Self {
        self.unix = self.unix.clone().with_control_event_receiver(receiver);
        self
    }
}

#[async_trait]
impl<AppEv: Send + 'static, AppMetric> AsyncExecutor<AppEv, AppMetric> for UnixLikeSystem {
    fn id(&self) -> &'static str {
        "unix-like-system"
    }
    fn dirs(&self) -> &Dirs {
        &self.unix.dirs
    }

    async fn setup(&mut self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) -> Result<(), SetupFailure> {
        Ok(())
    }

    async fn spawn_runtime_event_sources(&self, sender: Sender<Event<AppEv>>) -> Result<Vec<JoinHandle<()>>, SetupFailure> {
        self.unix.spawn_runtime_event_sources(sender).await
    }

    async fn is_ready(&self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) -> bool {
        true
    }

    async fn end(&self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) {}
}

/// Executor for per-user Unix deployments.
///
/// Directories are derived from `directories::ProjectDirs`, making this the
/// safer default for examples and local development.
#[derive(Clone, Debug)]
pub struct UnixLikeUser {
    unix: UnixLike,
}

impl UnixLikeUser {
    /// Create a per-user Unix executor for `app_name`.
    pub async fn new(app_name: &str, _fs: impl WFS) -> Result<Self, VfsError> {
        let proj_dirs = ProjectDirs::from("com", "wora", app_name).ok_or_else(|| VfsError::ProjectDirsUnavailable(app_name.to_string()))?;

        let dirs = Dirs {
            root_dir: PathBuf::from("/"),
            log_root_dir: proj_dirs.data_local_dir().to_path_buf(),
            metadata_root_dir: proj_dirs.config_dir().to_path_buf(),
            data_root_dir: proj_dirs.data_dir().to_path_buf(),
            runtime_root_dir: proj_dirs.runtime_dir().unwrap_or(std::env::temp_dir().as_path()).to_path_buf(),
            cache_root_dir: proj_dirs.cache_dir().to_path_buf(),
            secrets_root_dir: proj_dirs.cache_dir().to_path_buf(),
        };

        let mut unix = UnixLike::new(app_name).await;
        unix.dirs = dirs;

        Ok(UnixLikeUser { unix })
    }

    /// Inject a control-event receiver for deterministic testing.
    pub fn with_control_event_receiver(mut self, receiver: Receiver<ControlEvent>) -> Self {
        self.unix = self.unix.clone().with_control_event_receiver(receiver);
        self
    }
}

#[async_trait]
impl<AppEv: Send + Sync + 'static, AppMetric: Send + Sync> AsyncExecutor<AppEv, AppMetric> for UnixLikeUser {
    fn id(&self) -> &'static str {
        "unix-like-user"
    }
    fn dirs(&self) -> &Dirs {
        &self.unix.dirs
    }

    async fn setup(&mut self, _wora: &Wora<AppEv, AppMetric>, fs: impl WFS) -> Result<(), SetupFailure> {
        self.unix.chdir_root()?;
        self.unix.create_directories(fs).await?;
        Ok(())
    }

    async fn spawn_runtime_event_sources(&self, sender: Sender<Event<AppEv>>) -> Result<Vec<JoinHandle<()>>, SetupFailure> {
        self.unix.spawn_runtime_event_sources(sender).await
    }

    async fn is_ready(&self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) -> bool {
        true
    }

    async fn end(&self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) {}
}

/// Executor that maps every directory to `/tmp`.
///
/// Useful for smoke tests or constrained environments where the normal system
/// and user directory layouts are not available.
#[derive(Clone, Debug)]
pub struct UnixLikeBare {
    unix: UnixLike,
}

impl UnixLikeBare {
    /// Create a bare Unix executor.
    pub async fn new(app_name: &str) -> Self {
        let tmp = PathBuf::from("/tmp");
        let dirs = Dirs {
            root_dir: tmp.clone(),
            log_root_dir: tmp.clone(),
            metadata_root_dir: tmp.clone(),
            data_root_dir: tmp.clone(),
            runtime_root_dir: tmp.clone(),
            cache_root_dir: tmp.clone(),
            secrets_root_dir: tmp.clone(),
        };

        let mut unix = UnixLike::new(app_name).await;
        unix.dirs = dirs;

        UnixLikeBare { unix }
    }

    /// Inject a control-event receiver for deterministic testing.
    pub fn with_control_event_receiver(mut self, receiver: Receiver<ControlEvent>) -> Self {
        self.unix = self.unix.clone().with_control_event_receiver(receiver);
        self
    }
}

#[async_trait]
impl<AppEv: Send + 'static, AppMetric> AsyncExecutor<AppEv, AppMetric> for UnixLikeBare {
    fn id(&self) -> &'static str {
        "unix-like-bare"
    }

    fn dirs(&self) -> &Dirs {
        &self.unix.dirs
    }

    async fn setup(&mut self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) -> Result<(), SetupFailure> {
        Ok(())
    }

    async fn spawn_runtime_event_sources(&self, sender: Sender<Event<AppEv>>) -> Result<Vec<JoinHandle<()>>, SetupFailure> {
        self.unix.spawn_runtime_event_sources(sender).await
    }

    async fn is_ready(&self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) -> bool {
        true
    }

    async fn end(&self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) {}
}
