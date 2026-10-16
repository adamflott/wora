#![cfg_attr(target_os = "linux", doc = include_str!("../README.md"))]
#![cfg(target_os = "linux")]

mod buddyinfo;
mod pagetypeinfo;
mod slabinfo;
mod swaps;
mod sysctl;
mod zoneinfo;

pub use buddyinfo::{BuddyInfo, BuddyZone};
pub use pagetypeinfo::{FreeBlockCount, PageTypeBlockCounts, PageTypeFreeList, PageTypeInfo, PageTypeNode};
pub use procfs_core::{FromBufRead, FromRead, ProcError, ProcResult};
pub use procfs_core::{Iomem, Meminfo, MemoryPressure, PhysicalMemoryMap, PressureRecord, SharedMemorySegments, Shm, VmStat};
pub use slabinfo::{SlabCache, SlabCpuStats, SlabGlobalStats, SlabInfo};
pub use swaps::{SwapEntry, SwapType, Swaps};
pub use sysctl::{MemorySysctls, SysctlReadError, SysctlSkipReason};
pub use zoneinfo::{ZoneInfo, ZoneInfoZone, ZonePageSet, ZoneWatermarks};

#[cfg(target_os = "linux")]
pub use procfs::Current;
