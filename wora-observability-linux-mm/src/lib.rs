#![cfg_attr(target_os = "linux", doc = include_str!("../README.md"))]
#![cfg(target_os = "linux")]

mod buddyinfo;
mod cgroup;
mod hugepages;
mod pagetypeinfo;
mod process;
mod slabinfo;
mod swaps;
mod sysctl;
mod zoneinfo;

pub use buddyinfo::{BuddyInfo, BuddyZone};
pub use cgroup::{CgroupMemory, CgroupMemoryStats, CgroupMemoryValue};
pub use hugepages::{HugePageFiles, HugePageStats, HugePageValue};
pub use pagetypeinfo::{FreeBlockCount, PageTypeBlockCounts, PageTypeFreeList, PageTypeInfo, PageTypeNode};
pub use process::{ProcessMemoryStats, SmapsRollup, SmapsRollupValue};
pub use procfs_core::{FromBufRead, FromRead, ProcError, ProcResult};
pub use procfs_core::{Iomem, Meminfo, MemoryPressure, PhysicalMemoryMap, PressureRecord, SharedMemorySegments, Shm, VmStat};
pub use slabinfo::{SlabCache, SlabCpuStats, SlabGlobalStats, SlabInfo};
pub use swaps::{SwapEntry, SwapType, Swaps};
/// Read failures shared by memory information collectors.
pub use sysctl::SysctlReadError as MemoryReadError;
/// Reasons a memory information entry was deliberately not opened.
pub use sysctl::SysctlSkipReason as MemorySkipReason;
pub use sysctl::{MemorySysctls, SysctlReadError, SysctlSkipReason};
pub use zoneinfo::{ZoneInfo, ZoneInfoZone, ZonePageSet, ZoneWatermarks};

#[cfg(target_os = "linux")]
pub use procfs::Current;
