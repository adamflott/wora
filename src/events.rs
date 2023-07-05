use std::collections::HashMap;
use std::path::PathBuf;

use nix::unistd::Pid;
use serde::{Serialize, Serializer};

use crate::Leadership;

use decimal_percentage::Percentage;


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
#[derive(Clone, Debug)]
pub enum SystemResourceStat {
    CPUPercents(statgrab::CPUPercents),
    CPU(statgrab::CPUStats),
    Disk(HashMap<String, statgrab::DiskIOStats>),
    Filesystem(HashMap<String, statgrab::FilesystemStats>),
    Load(statgrab::LoadStats),
    Memory(statgrab::MemStats),
    NetworkIO(HashMap<String, statgrab::NetworkIOStats>),
    NetworkInterface(HashMap<String, statgrab::NetworkIfaceStats>),
    Page(statgrab::PageStats),
    ProcessCount(statgrab::ProcessCount),
    Process(HashMap<String, statgrab::ProcessStats>),
    Swap(statgrab::SwapStats),
    User(HashMap<String, statgrab::UserStats>),
}

impl Serialize for SystemResourceStat {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        todo!()
    }
}

/// wora and system based events
///
/// TODO - implement Serialize, Deserialize
#[derive(Clone, Debug)]
pub enum Event<T> {
    // system
    /// when a Unix signal arrives
    UnixSignal(i32),
    /// system stats
    SystemResource(SystemResourceStat),
    /// system stat thresholds reached
    SystemResourceCPUThreshold(Percentage, statgrab::CPUPercents),
    SystemResourceLoadThreshold(Percentage, statgrab::LoadStats),
    SystemResourceMemoryThreshold(Percentage, statgrab::MemStats),

    // workload
    /// workload configuration has changed
    ConfigChange(PathBuf, String),

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
