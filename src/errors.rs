use crate::boot::BootMarkerError;
use crate::o11y::O11yError;
#[cfg(target_os = "linux")]
use caps::errors::CapsError;
use nix::errno::Errno;
use thiserror::Error;

#[derive(Error, Debug)]
#[error(transparent)]
pub enum WoraSetupError {
    #[error("dirs")]
    Dirs,
    #[error("io: {0}")]
    IO(#[from] std::io::Error),
    #[error("ioerrno")]
    IOErrno(Errno),
    #[error("env vars: {0}")]
    ParseEnvVar(#[from] std::env::VarError),
    #[error("addr parse: {0}")]
    ParseIP(#[from] std::net::AddrParseError),
    #[error("str: {0}")]
    Str(String),
    #[error("notify: {0}")]
    FSNotify(#[from] notify::Error),
    #[error("dir missing: ({0:?})")]
    DirectoryDoesNotExistOnFilesystem(std::path::PathBuf),
    #[error("boot marker: {0}")]
    BootMarker(#[from] BootMarkerError),
    #[error("metric: {0}")]
    O11y(#[from] O11yError),
}

#[derive(Clone, Error, Debug)]
pub enum NewWorkloadError {
    #[error("args: {0}")]
    Args(String),
    #[error("env: {0}")]
    Env(String),
}

impl From<std::env::VarError> for NewWorkloadError {
    fn from(err: std::env::VarError) -> Self {
        NewWorkloadError::Env(err.to_string())
    }
}

#[derive(Debug, Error)]
pub enum VfsError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("notify: {0}")]
    Notify(#[from] notify::Error),
    #[error("project directories unavailable for app {0}")]
    ProjectDirsUnavailable(String),
}

#[derive(Debug, Error)]
#[error(transparent)]
pub enum SetupFailure {
    #[error("setup: I/O: {0}")]
    IO(#[from] std::io::Error),
    #[error("setup: Errno: {0}")]
    Errno(#[from] Errno),
    #[error("setup: logger")]
    Logger,
    #[error("setup: parse int: {0}")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("setup: unknown system user: {0}")]
    UnknownSystemUser(String),
    #[cfg(target_os = "linux")]
    #[error("setup: capability: {0}")]
    #[cfg(target_os = "linux")]
    Capability(#[from] CapsError),
    #[error("JSON parsing error: {0}")]
    JSON(#[from] serde_json::Error),
    #[error("setup: vfs: {0}")]
    Vfs(#[from] VfsError),
    #[error("setup: path is not valid UTF-8: {0:?}")]
    NonUtf8Path(std::path::PathBuf),
}

#[derive(Debug, Error)]
#[error(transparent)]
pub enum MainEarlyReturn {
    #[error("io: {0}")]
    IO(#[from] std::io::Error),
    #[error("vfs: {0}")]
    Vfs(#[from] VfsError),
    #[error("notify: {0}")]
    Notify(#[from] notify::Error),
    #[error("wora setup: {0}")]
    WoraSetup(#[from] WoraSetupError),
    #[error("workload: {0}")]
    NewWorkload(#[from] NewWorkloadError),
    #[error("setup: {0}")]
    SetupFailed(#[from] SetupFailure),
    #[error("already running: {0}")]
    AlreadyRunning(std::path::PathBuf),
    #[error("exit code: {0}")]
    UseExitCode(i8),
    #[error("clap: {0}")]
    ArgParsingClap(#[from] clap::error::Error),
    #[error("env vars: {0}")]
    EnvParser(#[from] EnvVarsParseError),
    #[error("logger: {0}")]
    SetupError(#[from] log::SetLoggerError),
}

#[derive(Debug, Error)]
pub enum LoggerSetupError {
    #[error("logger: io: {0}")]
    IO(#[from] std::io::Error),
    #[error("logger: setup: {0}")]
    SetupError(#[from] log::SetLoggerError),
}

impl From<log::SetLoggerError> for SetupFailure {
    fn from(_err: log::SetLoggerError) -> Self {
        SetupFailure::Logger
    }
}

#[derive(Debug, Error)]
pub enum EnvVarsParseError {}

#[derive(Debug, Error)]
pub enum ReloadError {
    #[error("reload: vfs: {0}")]
    Vfs(#[from] VfsError),
    #[error("reload: notify: {0}")]
    Notify(#[from] notify::Error),
    #[error("reload: {0}")]
    Message(String),
}
