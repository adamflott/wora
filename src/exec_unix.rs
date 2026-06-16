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
use crate::{AsyncExecutor, RuntimeSignal, SignalMapper, WFS, default_signal_mapper};

/// Shared Unix-like directory layout.
///
/// Built-in Unix-like executors convert OS signals into [`RuntimeSignal`]
/// values and then apply the runner's signal mapper. The default mapper
/// preserves WORA's historical control-event behavior, while
/// [`crate::RunnerOptions::with_signal_mapper`] lets applications map those
/// signals to app-defined events.
#[derive(Clone, Debug)]
pub struct UnixLike {
    /// Directories used by the executor.
    pub dirs: Dirs,
    injected_control_events: Option<Arc<Mutex<Option<Receiver<ControlEvent>>>>>,
    injected_signals: Option<Arc<Mutex<Option<Receiver<RuntimeSignal>>>>>,
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
            injected_signals: None,
        }
    }

    pub(crate) fn with_control_event_receiver(mut self, receiver: Receiver<ControlEvent>) -> Self {
        self.injected_control_events = Some(Arc::new(Mutex::new(Some(receiver))));
        self
    }

    pub(crate) fn with_signal_receiver(mut self, receiver: Receiver<RuntimeSignal>) -> Self {
        self.injected_signals = Some(Arc::new(Mutex::new(Some(receiver))));
        self
    }

    fn runtime_signal_for_raw(signum: i32) -> Option<RuntimeSignal> {
        match signum {
            SIGHUP => Some(RuntimeSignal::Hangup),
            SIGINT => Some(RuntimeSignal::Interrupt),
            SIGQUIT => Some(RuntimeSignal::Quit),
            SIGTERM => Some(RuntimeSignal::Terminate),
            SIGUSR1 => Some(RuntimeSignal::User1),
            SIGUSR2 => Some(RuntimeSignal::User2),
            _ => None,
        }
    }

    pub(crate) async fn spawn_runtime_event_sources<AppEv: Send + 'static>(&self, sender: Sender<Event<AppEv>>) -> Result<Vec<JoinHandle<()>>, SetupFailure> {
        self.spawn_runtime_event_sources_with_signal_mapper(sender, default_signal_mapper()).await
    }

    pub(crate) async fn spawn_runtime_event_sources_with_signal_mapper<AppEv: Send + 'static>(
        &self,
        sender: Sender<Event<AppEv>>,
        signal_mapper: SignalMapper<AppEv>,
    ) -> Result<Vec<JoinHandle<()>>, SetupFailure> {
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

        if let Some(injected_signals) = &self.injected_signals {
            let send = sender;
            let injected_signals = injected_signals.clone();
            return Ok(vec![tokio::spawn(async move {
                let Some(mut receiver) = injected_signals.lock().await.take() else {
                    return;
                };
                while let Some(signal) = receiver.recv().await {
                    if let Some(event) = signal_mapper(signal)
                        && send.send(event).await.is_err()
                    {
                        break;
                    }
                }
            })]);
        }

        let mut tasks = Vec::new();

        for &signum in [SIGHUP, SIGTERM, SIGQUIT, SIGUSR1, SIGUSR2, SIGINT].iter() {
            let send = sender.clone();
            let signal_mapper = signal_mapper.clone();
            let mut sig = tokio::signal::unix::signal(SignalKind::from_raw(signum))?;
            tasks.push(tokio::spawn(async move {
                while sig.recv().await.is_some() {
                    if let Some(event) = UnixLike::runtime_signal_for_raw(signum).and_then(|signal| signal_mapper(signal))
                        && send.send(event).await.is_err()
                    {
                        break;
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

    /// Inject a runtime signal receiver for deterministic testing and custom signal mapping.
    pub fn with_signal_receiver(mut self, receiver: Receiver<RuntimeSignal>) -> Self {
        self.unix = self.unix.clone().with_signal_receiver(receiver);
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

    async fn spawn_runtime_event_sources_with_signal_mapper(
        &self,
        sender: Sender<Event<AppEv>>,
        signal_mapper: SignalMapper<AppEv>,
    ) -> Result<Vec<JoinHandle<()>>, SetupFailure> {
        self.unix.spawn_runtime_event_sources_with_signal_mapper(sender, signal_mapper).await
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

    /// Inject a runtime signal receiver for deterministic testing and custom signal mapping.
    pub fn with_signal_receiver(mut self, receiver: Receiver<RuntimeSignal>) -> Self {
        self.unix = self.unix.clone().with_signal_receiver(receiver);
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

    async fn spawn_runtime_event_sources_with_signal_mapper(
        &self,
        sender: Sender<Event<AppEv>>,
        signal_mapper: SignalMapper<AppEv>,
    ) -> Result<Vec<JoinHandle<()>>, SetupFailure> {
        self.unix.spawn_runtime_event_sources_with_signal_mapper(sender, signal_mapper).await
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

    /// Inject a runtime signal receiver for deterministic testing and custom signal mapping.
    pub fn with_signal_receiver(mut self, receiver: Receiver<RuntimeSignal>) -> Self {
        self.unix = self.unix.clone().with_signal_receiver(receiver);
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

    async fn spawn_runtime_event_sources_with_signal_mapper(
        &self,
        sender: Sender<Event<AppEv>>,
        signal_mapper: SignalMapper<AppEv>,
    ) -> Result<Vec<JoinHandle<()>>, SetupFailure> {
        self.unix.spawn_runtime_event_sources_with_signal_mapper(sender, signal_mapper).await
    }

    async fn is_ready(&self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) -> bool {
        true
    }

    async fn end(&self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) {}
}
