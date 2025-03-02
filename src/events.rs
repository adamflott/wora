use std::collections::HashMap;

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

/// wora and system based events
///
#[derive(Clone, Debug, Serialize)]
pub enum Event<T> {
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
    /// workload is being requested to suspend
    Suspended(Option<chrono::NaiveDateTime>),
    /// workload is being requested to shutdown
    Shutdown(Option<chrono::NaiveDateTime>),
    /// workload is being requested to change leadership role
    LeadershipChange(Leadership, Leadership),

    /// workload is being requested to rotate logging
    LogRotation,

    /// custom workload/app events
    App(T),
}

impl<T: std::fmt::Debug> ToString for Event<T> {
    fn to_string(&self) -> String {
        format!("{:?}", self)
    }
}
