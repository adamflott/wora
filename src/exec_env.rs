use std::env;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use directories::ProjectDirs;
#[cfg(target_os = "macos")]
use launchd::Launchd;
#[cfg(target_os = "macos")]
use launchd::sockets::{Socket, SocketFamily, SocketOptions, SocketType, Sockets};
#[cfg(target_os = "linux")]
use sd_notify::NotifyState;
#[cfg(target_os = "linux")]
use std::os::linux::net::SocketAddrExt;
#[cfg(target_os = "macos")]
use std::os::unix::io::RawFd;
#[cfg(all(target_family = "unix", target_os = "linux"))]
use std::os::unix::net::SocketAddr;
#[cfg(target_family = "unix")]
use std::os::unix::net::UnixDatagram;
use thiserror::Error;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tracing::{debug, trace};

use crate::dirs::Dirs;
use crate::errors::{SetupFailure, VfsError};
use crate::events::{ControlEvent, Event};
use crate::exec::AsyncExecutor;
use crate::exec_unix::UnixLike;
use crate::{WFS, Wora};

fn first_env_path(var_name: &str) -> Option<PathBuf> {
    env::var_os(var_name).and_then(|value| value.to_string_lossy().split(':').find(|segment| !segment.is_empty()).map(PathBuf::from))
}

fn existing_path_or(path: impl AsRef<Path>, fallback: PathBuf) -> PathBuf {
    if path.as_ref().exists() { path.as_ref().to_path_buf() } else { fallback }
}

async fn create_executor_dirs(fs: impl WFS, dirs: &[PathBuf]) -> Result<(), SetupFailure> {
    for dir in dirs {
        trace!("exec:setup:io:create dir:{:?}: trying", dir);
        fs.create_dir(dir).await?;
        trace!("exec:setup:io:create dir:{:?}: success", dir);
    }
    Ok(())
}

#[cfg(target_family = "unix")]
fn send_unix_datagram(socket: &str, payload: &str) -> Result<(), SetupFailure> {
    let datagram = UnixDatagram::unbound()?;

    #[cfg(target_os = "linux")]
    if let Some(abstract_name) = socket.strip_prefix('@') {
        let addr = SocketAddr::from_abstract_name(abstract_name.as_bytes())?;
        datagram.connect_addr(&addr)?;
        let _sent = datagram.send(payload.as_bytes())?;
        return Ok(());
    }

    datagram.connect(socket)?;
    let _sent = datagram.send(payload.as_bytes())?;
    Ok(())
}

#[cfg(not(target_family = "unix"))]
fn send_unix_datagram(_socket: &str, _payload: &str) -> Result<(), SetupFailure> {
    Ok(())
}

/// launchd executor API error.
#[cfg(target_os = "macos")]
#[derive(Debug, Error)]
pub enum LaunchdExecutorError {
    #[error("launchd plist")]
    Plist(#[from] launchd::Error),
    #[error("launchd socket activation")]
    SocketActivation(#[from] raunch::Error),
}

/// Service-manager scope for systemd executors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SystemdScope {
    /// System service managed by PID 1.
    System,
    /// Per-user service managed by the user service manager.
    User,
}

/// Executor for systemd-managed services.
///
/// This executor uses the standard `NOTIFY_SOCKET` transport for readiness and
/// stopping notifications and consumes directory environment variables such as
/// `STATE_DIRECTORY`, `CACHE_DIRECTORY`, and `RUNTIME_DIRECTORY` when they are
/// provided by systemd.
#[derive(Clone, Debug)]
pub struct SystemdExecutor {
    unix: UnixLike,
    scope: SystemdScope,
    notify_socket: Option<String>,
}

impl SystemdExecutor {
    /// Create a system-scoped systemd executor.
    pub async fn system(app_name: &str) -> Self {
        let runtime_dir = first_env_path("RUNTIME_DIRECTORY").unwrap_or_else(|| PathBuf::from(format!("/run/{app_name}")));
        let cache_dir = first_env_path("CACHE_DIRECTORY").unwrap_or_else(|| PathBuf::from(format!("/var/cache/{app_name}")));
        let data_dir = first_env_path("STATE_DIRECTORY").unwrap_or_else(|| PathBuf::from(format!("/var/lib/{app_name}")));
        let metadata_dir = first_env_path("CONFIGURATION_DIRECTORY").unwrap_or_else(|| PathBuf::from(format!("/etc/{app_name}")));
        let log_dir = first_env_path("LOGS_DIRECTORY").unwrap_or_else(|| PathBuf::from(format!("/var/log/{app_name}")));
        let secrets_dir = existing_path_or("/run/secrets", runtime_dir.join("secrets"));

        let mut unix = UnixLike::new(app_name).await;
        unix.dirs = Dirs {
            root_dir: PathBuf::from("/"),
            log_root_dir: log_dir,
            metadata_root_dir: metadata_dir,
            data_root_dir: data_dir,
            runtime_root_dir: runtime_dir,
            cache_root_dir: cache_dir,
            secrets_root_dir: secrets_dir,
        };

        Self {
            unix,
            scope: SystemdScope::System,
            notify_socket: env::var("NOTIFY_SOCKET").ok(),
        }
    }

    /// Create a user-scoped systemd executor.
    pub async fn user(app_name: &str) -> Result<Self, VfsError> {
        let proj_dirs = ProjectDirs::from("com", "wora", app_name).ok_or_else(|| VfsError::ProjectDirsUnavailable(app_name.to_string()))?;
        let runtime_dir = first_env_path("RUNTIME_DIRECTORY")
            .or_else(|| env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from).map(|base| base.join(app_name)))
            .unwrap_or_else(|| proj_dirs.runtime_dir().unwrap_or(std::env::temp_dir().as_path()).join(app_name));
        let cache_dir = first_env_path("CACHE_DIRECTORY").unwrap_or_else(|| proj_dirs.cache_dir().to_path_buf());
        let data_dir = first_env_path("STATE_DIRECTORY").unwrap_or_else(|| proj_dirs.data_dir().to_path_buf());
        let metadata_dir = first_env_path("CONFIGURATION_DIRECTORY").unwrap_or_else(|| proj_dirs.config_dir().to_path_buf());
        let log_dir = first_env_path("LOGS_DIRECTORY").unwrap_or_else(|| proj_dirs.data_local_dir().to_path_buf());
        let secrets_dir = existing_path_or(runtime_dir.join("secrets"), cache_dir.join("secrets"));

        let mut unix = UnixLike::new(app_name).await;
        unix.dirs = Dirs {
            root_dir: PathBuf::from("/"),
            log_root_dir: log_dir,
            metadata_root_dir: metadata_dir,
            data_root_dir: data_dir,
            runtime_root_dir: runtime_dir,
            cache_root_dir: cache_dir,
            secrets_root_dir: secrets_dir,
        };

        Ok(Self {
            unix,
            scope: SystemdScope::User,
            notify_socket: env::var("NOTIFY_SOCKET").ok(),
        })
    }

    /// Override the notify socket path used by readiness/stopping hooks.
    pub fn with_notify_socket(mut self, notify_socket: impl Into<String>) -> Self {
        self.notify_socket = Some(notify_socket.into());
        self
    }

    /// Inject a control-event receiver for deterministic testing.
    pub fn with_control_event_receiver(mut self, receiver: Receiver<ControlEvent>) -> Self {
        self.unix = self.unix.clone().with_control_event_receiver(receiver);
        self
    }

    fn send_notify_message(&self, payload: String) -> Result<(), SetupFailure> {
        #[cfg(target_os = "linux")]
        {
            match &self.notify_socket {
                Some(socket) => {
                    debug!("systemd:notify socket:{} payload:{}", socket, payload.replace('\n', ";"));
                    send_unix_datagram(socket, &payload)
                }
                None => {
                    let mut states = Vec::new();
                    for line in payload.lines() {
                        if let Some(message) = line.strip_prefix("STATUS=") {
                            states.push(NotifyState::Status(message));
                        } else if line == "READY=1" {
                            states.push(NotifyState::Ready);
                        } else if line == "STOPPING=1" {
                            states.push(NotifyState::Stopping);
                        } else if let Some(pid) = line.strip_prefix("MAINPID=") {
                            states.push(NotifyState::MainPid(pid.parse()?));
                        } else {
                            states.push(NotifyState::Custom(line));
                        }
                    }
                    sd_notify::notify(&states).map_err(SetupFailure::IO)
                }
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            match &self.notify_socket {
                Some(socket) => {
                    debug!("systemd:notify socket:{} payload:{}", socket, payload.replace('\n', ";"));
                    send_unix_datagram(socket, &payload)
                }
                None => Ok(()),
            }
        }
    }
}

#[async_trait]
impl<AppEv: Send + Sync + 'static, AppMetric: Send + Sync> AsyncExecutor<AppEv, AppMetric> for SystemdExecutor {
    fn id(&self) -> &'static str {
        match self.scope {
            SystemdScope::System => "systemd-system",
            SystemdScope::User => "systemd-user",
        }
    }

    fn dirs(&self) -> &Dirs {
        &self.unix.dirs
    }

    async fn setup(&mut self, _wora: &Wora<AppEv, AppMetric>, fs: impl WFS) -> Result<(), SetupFailure> {
        self.unix.chdir_root()?;
        self.unix.create_directories(fs).await
    }

    async fn spawn_runtime_event_sources(&self, sender: Sender<Event<AppEv>>) -> Result<Vec<JoinHandle<()>>, SetupFailure> {
        self.unix.spawn_runtime_event_sources(sender).await
    }

    async fn on_runtime_ready(&self, app_name: &str, _dirs: &Dirs, _fs: impl WFS) -> Result<(), SetupFailure> {
        self.send_notify_message(format!(
            "READY=1\nSTATUS={} ready via {}\nMAINPID={}",
            app_name,
            match self.scope {
                SystemdScope::System => "systemd-system",
                SystemdScope::User => "systemd-user",
            },
            std::process::id()
        ))
    }

    async fn is_ready(&self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) -> bool {
        true
    }

    async fn on_runtime_stopping(&self, app_name: &str, _dirs: &Dirs, _fs: impl WFS) -> Result<(), SetupFailure> {
        self.send_notify_message(format!("STOPPING=1\nSTATUS={} stopping", app_name))
    }

    async fn end(&self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) {}
}

/// Service-manager scope for launchd executors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LaunchdScope {
    /// A per-user LaunchAgent.
    Agent,
    /// A system-wide LaunchDaemon.
    Daemon,
}

/// Executor for macOS launchd-managed workloads.
///
/// launchd does not provide a readiness protocol comparable to `sd_notify`, so
/// this executor focuses on native directory layout and signal translation.
#[derive(Clone, Debug)]
pub struct LaunchdExecutor {
    unix: UnixLike,
    scope: LaunchdScope,
    socket_names: Vec<String>,
}

impl LaunchdExecutor {
    /// Create a LaunchAgent-style executor.
    pub async fn agent(app_name: &str) -> Result<Self, VfsError> {
        let proj_dirs = ProjectDirs::from("com", "wora", app_name).ok_or_else(|| VfsError::ProjectDirsUnavailable(app_name.to_string()))?;
        let mut unix = UnixLike::new(app_name).await;
        unix.dirs = Dirs {
            root_dir: PathBuf::from("/"),
            log_root_dir: proj_dirs.data_local_dir().to_path_buf(),
            metadata_root_dir: proj_dirs.preference_dir().to_path_buf(),
            data_root_dir: proj_dirs.data_dir().to_path_buf(),
            runtime_root_dir: env::var_os("TMPDIR").map(PathBuf::from).unwrap_or_else(std::env::temp_dir).join(app_name),
            cache_root_dir: proj_dirs.cache_dir().to_path_buf(),
            secrets_root_dir: proj_dirs.cache_dir().join("secrets"),
        };
        Ok(Self {
            unix,
            scope: LaunchdScope::Agent,
            socket_names: vec![],
        })
    }

    /// Create a LaunchDaemon-style executor.
    pub async fn daemon(app_name: &str) -> Self {
        let mut unix = UnixLike::new(app_name).await;
        unix.dirs = Dirs {
            root_dir: PathBuf::from("/"),
            log_root_dir: PathBuf::from(format!("/Library/Logs/{app_name}")),
            metadata_root_dir: PathBuf::from(format!("/Library/Preferences/{app_name}")),
            data_root_dir: PathBuf::from(format!("/Library/Application Support/{app_name}")),
            runtime_root_dir: existing_path_or("/var/run", PathBuf::from("/tmp")).join(app_name),
            cache_root_dir: PathBuf::from(format!("/Library/Caches/{app_name}")),
            secrets_root_dir: existing_path_or("/var/db", PathBuf::from("/tmp")).join(app_name).join("secrets"),
        };
        Self {
            unix,
            scope: LaunchdScope::Daemon,
            socket_names: vec![],
        }
    }

    /// Register a launchd socket activation name for generated plist output.
    pub fn with_socket_name(mut self, socket_name: impl Into<String>) -> Self {
        self.socket_names.push(socket_name.into());
        self
    }

    /// Return the configured launchd socket activation names.
    pub fn socket_names(&self) -> &[String] {
        &self.socket_names
    }

    /// Inject a control-event receiver for deterministic testing.
    pub fn with_control_event_receiver(mut self, receiver: Receiver<ControlEvent>) -> Self {
        self.unix = self.unix.clone().with_control_event_receiver(receiver);
        self
    }

    /// Build a launchd plist job description for this executor.
    #[cfg(target_os = "macos")]
    pub fn launchd_job<P: AsRef<Path>>(&self, label: &str, program: P, program_arguments: Vec<String>) -> Result<Launchd, LaunchdExecutorError> {
        let mut job = Launchd::new(label, program)?
            .with_program_arguments(program_arguments)
            .with_working_directory(&self.unix.dirs.data_root_dir)?
            .with_standard_out_path(self.unix.dirs.log_root_dir.join("stdout.log"))?
            .with_standard_error_path(self.unix.dirs.log_root_dir.join("stderr.log"))?
            .with_environment_variables(std::collections::HashMap::from([(
                "WORA_EXECUTOR".to_string(),
                match self.scope {
                    LaunchdScope::Agent => "launchd-agent".to_string(),
                    LaunchdScope::Daemon => "launchd-daemon".to_string(),
                },
            )]))
            .run_at_load();

        for socket_name in &self.socket_names {
            let socket = Socket::new(socket_name, SocketOptions::new().with_type(SocketType::Stream).with_family(SocketFamily::Unix));
            job = job.with_socket(Sockets::from(socket));
        }

        Ok(job)
    }

    /// Activate a launchd-managed socket by name.
    #[cfg(target_os = "macos")]
    pub fn activate_socket(&self, socket_name: &str) -> Result<Vec<RawFd>, LaunchdExecutorError> {
        Ok(raunch::activate_socket(socket_name)?)
    }
}

#[async_trait]
impl<AppEv: Send + Sync + 'static, AppMetric: Send + Sync> AsyncExecutor<AppEv, AppMetric> for LaunchdExecutor {
    fn id(&self) -> &'static str {
        match self.scope {
            LaunchdScope::Agent => "launchd-agent",
            LaunchdScope::Daemon => "launchd-daemon",
        }
    }

    fn dirs(&self) -> &Dirs {
        &self.unix.dirs
    }

    async fn setup(&mut self, _wora: &Wora<AppEv, AppMetric>, fs: impl WFS) -> Result<(), SetupFailure> {
        self.unix.chdir_root()?;
        self.unix.create_directories(fs).await
    }

    async fn spawn_runtime_event_sources(&self, sender: Sender<Event<AppEv>>) -> Result<Vec<JoinHandle<()>>, SetupFailure> {
        self.unix.spawn_runtime_event_sources(sender).await
    }

    async fn is_ready(&self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) -> bool {
        true
    }

    async fn end(&self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) {}
}

/// Executor for generic container or Kubernetes-style deployments.
///
/// This executor assumes the orchestrator owns config and secret mounts while
/// the workload owns writable runtime, cache, data, and optional readiness
/// files.
#[derive(Clone, Debug)]
pub struct ContainerExecutor {
    unix: UnixLike,
    readiness_file: Option<PathBuf>,
    termination_log: Option<PathBuf>,
}

impl ContainerExecutor {
    /// Create a container-oriented executor with Kubernetes-friendly defaults.
    pub async fn new(app_name: &str) -> Self {
        let writable_root = std::env::temp_dir().join(app_name);
        let runtime_root = writable_root.join("run");
        let metadata_root = if Path::new("/etc/config").exists() {
            PathBuf::from("/etc/config")
        } else {
            writable_root.join("config")
        };
        let secrets_root = if Path::new("/var/run/secrets/kubernetes.io/serviceaccount").exists() {
            PathBuf::from("/var/run/secrets/kubernetes.io/serviceaccount")
        } else if Path::new("/var/run/secrets").exists() {
            PathBuf::from("/var/run/secrets")
        } else {
            writable_root.join("secrets")
        };

        let mut unix = UnixLike::new(app_name).await;
        unix.dirs = Dirs {
            root_dir: PathBuf::from("/"),
            log_root_dir: writable_root.join("logs"),
            metadata_root_dir: metadata_root,
            data_root_dir: existing_path_or("/data", writable_root.join("data")),
            runtime_root_dir: runtime_root.clone(),
            cache_root_dir: writable_root.join("cache"),
            secrets_root_dir: secrets_root,
        };

        Self {
            unix,
            readiness_file: None,
            termination_log: None,
        }
    }

    /// Configure a file to create when the runtime is ready.
    pub fn with_readiness_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.readiness_file = Some(path.into());
        self
    }

    /// Configure a termination log file written during shutdown.
    pub fn with_termination_log(mut self, path: impl Into<PathBuf>) -> Self {
        self.termination_log = Some(path.into());
        self
    }

    /// Inject a control-event receiver for deterministic testing.
    pub fn with_control_event_receiver(mut self, receiver: Receiver<ControlEvent>) -> Self {
        self.unix = self.unix.clone().with_control_event_receiver(receiver);
        self
    }
}

#[async_trait]
impl<AppEv: Send + Sync + 'static, AppMetric: Send + Sync> AsyncExecutor<AppEv, AppMetric> for ContainerExecutor {
    fn id(&self) -> &'static str {
        "container"
    }

    fn dirs(&self) -> &Dirs {
        &self.unix.dirs
    }

    async fn setup(&mut self, _wora: &Wora<AppEv, AppMetric>, fs: impl WFS) -> Result<(), SetupFailure> {
        self.unix.chdir_root()?;
        create_executor_dirs(
            fs,
            &[
                self.unix.dirs.log_root_dir.clone(),
                self.unix.dirs.metadata_root_dir.clone(),
                self.unix.dirs.data_root_dir.clone(),
                self.unix.dirs.runtime_root_dir.clone(),
                self.unix.dirs.cache_root_dir.clone(),
                self.unix.dirs.secrets_root_dir.clone(),
            ],
        )
        .await
    }

    async fn spawn_runtime_event_sources(&self, sender: Sender<Event<AppEv>>) -> Result<Vec<JoinHandle<()>>, SetupFailure> {
        self.unix.spawn_runtime_event_sources(sender).await
    }

    async fn on_runtime_ready(&self, app_name: &str, _dirs: &Dirs, fs: impl WFS) -> Result<(), SetupFailure> {
        if let Some(path) = &self.readiness_file {
            if let Some(parent) = path.parent() {
                fs.create_dir(parent).await?;
            }
            fs.write(path, format!("ready:{}\n", app_name).as_bytes()).await?;
        }
        Ok(())
    }

    async fn is_ready(&self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) -> bool {
        true
    }

    async fn on_runtime_stopping(&self, app_name: &str, _dirs: &Dirs, fs: impl WFS) -> Result<(), SetupFailure> {
        if let Some(path) = &self.termination_log {
            if let Some(parent) = path.parent() {
                fs.create_dir(parent).await?;
            }
            fs.write(path, format!("stopping:{}\n", app_name).as_bytes()).await?;
        }

        if let Some(path) = &self.readiness_file {
            match fs.remove_file(path).await {
                Ok(_) => {}
                Err(VfsError::Io(err)) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(SetupFailure::Vfs(err)),
            }
        }

        Ok(())
    }

    async fn end(&self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) {}
}
