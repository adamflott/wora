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
    /// platform-neutral lifecycle or operator control request
    Control(ControlEvent),
    // system
    /// when a Unix signal arrives
    UnixSignal(i32),
    /// system stats
    SystemResource(SystemResourceStat),
    /// system stat thresholds reached
    SystemResourceCPUThreshold(Percentage, Cpu),
    SystemResourceLoadThreshold(Percentage, LoadAvg),
    SystemResourceMemoryThreshold(Percentage, MemStats),

    // workload
    /// workload configuration has changed
    ConfigChange(notify::Event),

    // workload operations
    /// workload is being requested to reload configuration
    ///
    /// Prefer `Event::Control(ControlEvent::ReloadConfiguration)` for new
    /// code. This variant remains for compatibility with earlier APIs.
    #[allow(dead_code)]
    ReloadConfiguration,
    /// workload is being requested to suspend
    ///
    /// Prefer `Event::Control(ControlEvent::Suspend(_))` for new code.
    Suspended(Option<chrono::NaiveDateTime>),
    /// workload is being requested to shutdown
    ///
    /// Prefer `Event::Control(ControlEvent::Shutdown(_))` for new code.
    Shutdown(Option<chrono::NaiveDateTime>),
    /// workload is being requested to change leadership role
    LeadershipChange(Leadership, Leadership),

    /// workload is being requested to rotate logging
    ///
    /// Prefer `Event::Control(ControlEvent::LogRotation)` for new code.
    LogRotation,

    /// custom workload/app events
    App(T),
}

impl<T: Debug> Display for Event<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
