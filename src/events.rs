use std::collections::HashMap;
use std::path::PathBuf;

use nix::unistd::Pid;
use serde::{Serialize, Serializer};

use crate::Leadership;

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
    Memory(statgrab::LoadStats),
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
pub enum Event {
    // system
    /// when a Unix signal arrives
    UnixSignal(i32),
    /// system stats
    SystemResource(SystemResourceStat),

    // workload
    ///
    ConfigChange(PathBuf, String),

    // operations
    Suspended(Option<chrono::NaiveDateTime>),
    Shutdown(Option<chrono::NaiveDateTime>),
    LogRotation,
    LeadershipChange(Leadership, Leadership),
}

impl ToString for Event {
    fn to_string(&self) -> String {
        format!("{:?}", self)
    }
}
