use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::path::PathBuf;

use decimal_percentage::Percentage;
use nix::unistd::Pid;
use serde::{Serialize, Serializer};

use crate::Leadership;
use crate::o11y::{Cpu, Disk, LoadAvg, MemStats, NetIO, SwapStats};

/// Stable classification of a filesystem-backed change event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum ChangeKind {
    /// One or more files were created.
    Created,
    /// One or more files were modified.
    Modified,
    /// One or more files were removed.
    Removed,
    /// The underlying watcher reported a change that does not map cleanly to
    /// create/modify/remove.
    Other,
}

impl ChangeKind {
    pub(crate) fn from_notify_kind(kind: &notify::EventKind) -> Self {
        match kind {
            notify::EventKind::Create(_) => Self::Created,
            notify::EventKind::Modify(_) => Self::Modified,
            notify::EventKind::Remove(_) => Self::Removed,
            _ => Self::Other,
        }
    }
}

/// Typed workload configuration change event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ConfigChange {
    /// Stable classification of the underlying change.
    pub kind: ChangeKind,
    /// Paths reported by the watcher for this config change.
    pub paths: Vec<PathBuf>,
    /// Whether the app's primary `{app_name}.toml` file is included.
    pub main_config_changed: bool,
    /// Metadata paths other than the app's primary config file.
    pub supplemental_paths: Vec<PathBuf>,
}

impl ConfigChange {
    /// Build a typed config change event from watcher paths.
    pub fn new(kind: ChangeKind, main_config_path: PathBuf, paths: Vec<PathBuf>) -> Self {
        let main_config_changed = paths.iter().any(|path| path == &main_config_path);
        let supplemental_paths = paths.iter().filter(|path| *path != &main_config_path).cloned().collect();
        Self {
            kind,
            paths,
            main_config_changed,
            supplemental_paths,
        }
    }
}

/// Typed workload secret change event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SecretChange {
    /// Stable classification of the underlying change.
    pub kind: ChangeKind,
    /// Paths reported by the watcher for this secret change.
    pub paths: Vec<PathBuf>,
}

impl SecretChange {
    /// Build a typed secret change event from watcher paths.
    pub fn new(kind: ChangeKind, paths: Vec<PathBuf>) -> Self {
        Self { kind, paths }
    }
}

/// Process Id (pid)
#[derive(Clone, Debug)]
pub struct ProcessId(pub Pid);

impl Serialize for ProcessId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i32(self.0.as_raw())
    }
}

/// stats for CPU, disk, file system, load, memory, network usage, process, etc from the system
#[derive(Clone, Debug, Serialize)]
pub enum SystemResourceStat {
    CPU(Cpu),
    Disk(HashMap<String, Disk>),
    Load(LoadAvg),
    Memory(MemStats),
    NetworkIO(HashMap<String, NetIO>),
    Swap(SwapStats),
}

/// Platform-neutral runtime control request.
///
/// Executors should translate environment-specific lifecycle inputs into these
/// events so applications can react without depending on a specific OS signal
/// or control mechanism. The runner supervises shutdown requests specially;
/// reload, suspend, and log-rotation requests are delivered to the application
/// as control notifications unless the app maps the original signal to a
/// different event with [`crate::RunnerOptions::with_signal_mapper`].
#[derive(Clone, Debug, Serialize)]
pub enum ControlEvent {
    /// Request application-handled configuration reload.
    ///
    /// This event is not automatically converted into a typed
    /// `Event::ConfigChanged` reload by the runner.
    ReloadConfiguration,
    /// Request application-handled graceful suspension.
    Suspend(Option<chrono::NaiveDateTime>),
    /// Request graceful shutdown.
    Shutdown(Option<chrono::NaiveDateTime>),
    /// Request application-handled log rotation or log sink reopen.
    LogRotation,
}

/// wora and system based events
///
#[derive(Clone, Debug, Serialize)]
pub enum Event<T> {
    /// Platform-neutral lifecycle or operator control request.
    Control(ControlEvent),

    // system
    /// System stats.
    SystemResource(SystemResourceStat),
    /// System CPU threshold reached.
    SystemResourceCPUThreshold(Percentage, Cpu),
    /// System load threshold reached.
    SystemResourceLoadThreshold(Percentage, LoadAvg),
    /// System memory threshold reached.
    SystemResourceMemoryThreshold(Percentage, MemStats),

    // workload
    /// Workload configuration has changed.
    ConfigChanged(ConfigChange),
    /// Workload secrets have changed.
    SecretChanged(SecretChange),
    /// Workload leadership role has changed.
    LeadershipChanged(Leadership, Leadership),
    /// Custom workload/app events.
    App(T),
}

impl<T: Debug> Display for Event<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
