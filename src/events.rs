use std::collections::HashMap;

use nix::unistd::Pid;

use serde::{Deserialize, Serialize, Serializer};
//use serde_json::Result;

use crate::restart_policy::*;

// # General

/// Percentage from 0 to 100
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Percent(i8);

// # System

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

// # Communication

/// wora control socket operation.
#[derive(Clone, Debug, Serialize)]
pub enum SocketOp {
    Open,
    Bind,
    Accept,
    Listen,
    Recv,
    Close,
}

/// wora phase. One of setup, main, end.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Phase {
    Setup,
    Main,
    End,
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
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
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
    // wora
    /// start of the wora running the workload
    Begin(String, ProcessId),
    /// start of a phase change in wora
    PhaseBegin(Phase),
    /// end of a phase change in wora
    PhaseEnd(Phase),
    /// end of wora running the workload
    End,
    /// workload retry count, policy, and next action
    WorkloadRetry(usize, WorkloadRestartPolicy, MainRetryAction),

    // system
    /// when a Unix signal arrives
    UnixSignal(i32),
    /// system stats
    SystemResource(SystemResourceStat),

    // workload
    ConfigChange,
    ControlOp(SocketOp),

    Suspended,
    Shutdown,
    LogRotation,
}

impl ToString for Event {
    fn to_string(&self) -> String {
        format!("{:?}", self)
    }
}
