use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};

use decimal_percentage::Percentage;
use nix::unistd::Pid;
use serde::{Serialize, Serializer};

use crate::Leadership;
use crate::o11y::{Cpu, Disk, LoadAvg, MemStats, NetIO, SwapStats};

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
/// or control mechanism.
#[derive(Clone, Debug, Serialize)]
pub enum ControlEvent {
    /// Request configuration reload.
    ReloadConfiguration,
    /// Request graceful suspension.
    Suspend(Option<chrono::NaiveDateTime>),
    /// Request graceful shutdown.
    Shutdown(Option<chrono::NaiveDateTime>),
    /// Request log rotation or log sink reopen.
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
    ConfigChanged(notify::Event),
    /// Workload secrets have changed.
    SecretChanged(notify::Event),
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
