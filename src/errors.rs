use caps::errors::CapsError;
use nix::errno::Errno;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum WoraSetupError {
    #[error("dirs")]
    Dirs,
    #[error("statgrab")]
    Statgrab(statgrab::SGError),
    #[error("io")]
    IO(#[from] std::io::Error),
    #[error("ioerrno")]
    IOErrno(Errno),
    #[error("env vars")]
    ParseEnvVar(#[from] std::env::VarError),
    #[error("addr parse")]
    ParseIP(#[from] std::net::AddrParseError),
    #[error("str")]
    Str(String),
    #[error("notify")]
    FSNotify(#[from] notify::Error),
    #[error("dir missing")]
    DirectoryDoesNotExistOnFilesystem(std::path::PathBuf),
}

impl From<statgrab::SGError> for WoraSetupError {
    fn from(err: statgrab::SGError) -> Self {
        WoraSetupError::Statgrab(err)
    }
}

#[derive(Clone, Error, Debug)]
pub enum NewWorkloadError {
    #[error("args")]
    Args(String),
    #[error("env")]
    Env(String),
}

impl From<std::env::VarError> for NewWorkloadError {
    fn from(err: std::env::VarError) -> Self {
        NewWorkloadError::Env(err.to_string())
    }
}

#[derive(Debug, Error)]
pub enum SetupFailure {
    #[error("setup: I/O")]
    IO(#[from] std::io::Error),
    #[error("setup: Errno")]
    Errno(#[from] Errno),
    #[error("setup: logger")]
    Logger,
    #[error("setup: parse int")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("setup: unknown system user")]
    UnknownSystemUser(String),
    #[error("setup: capability")]
    Capability(#[from] CapsError),
}

#[derive(Debug, Error)]
pub enum MainEarlyReturn {
    #[error("io")]
    IO(#[from] std::io::Error),
    #[error("notify")]
    Notify(#[from] notify::Error),
    #[error("statgrab")]
    Statgrab(statgrab::SGError),
    #[error("wora setup")]
    WoraSetup(#[from] WoraSetupError),
    #[error("workload")]
    NewWorkload(#[from] NewWorkloadError),
    #[error("setup")]
    SetupFailed(#[from] SetupFailure),
    #[error("exit code")]
    UseExitCode(i8),
    #[error("clap")]
    ArgParsingClap(#[from] clap::error::Error),
    #[error("env vars")]
    EnvParser(#[from] EnvVarsParseError),
}

#[derive(Debug, Error)]
pub enum LoggerSetupError {
    #[error("logger: io")]
    IO(#[from] std::io::Error),
    #[error("logger: setup")]
    SetupError(#[from] log::SetLoggerError),
}

impl From<log::SetLoggerError> for SetupFailure {
    fn from(_err: log::SetLoggerError) -> Self {
        SetupFailure::Logger
    }
}

#[derive(Debug, Error)]
pub enum EnvVarsParseError {}
