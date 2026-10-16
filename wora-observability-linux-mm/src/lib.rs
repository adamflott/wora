#![cfg_attr(target_os = "linux", doc = include_str!("../README.md"))]
#![cfg(target_os = "linux")]

mod allocations;
mod buddyinfo;
mod cgroup;
mod compressed;
mod files;
mod fragmentation;
mod hugepages;
mod ksm;
mod lru;
mod mappings;
mod numa;
mod pages;
mod pagetypeinfo;
mod process;
mod provenance;
mod slabinfo;
mod slub;
mod swaps;
mod sysctl;
mod zoneinfo;

pub use allocations::{AllocationSite, AllocationSites, KernelAllocationStats};
pub use buddyinfo::{BuddyInfo, BuddyZone};
pub use cgroup::{CgroupMemory, CgroupMemoryStats, CgroupMemoryValue};
pub use compressed::{CompressedMemoryStats, ZramDevice, ZramMemoryStat};
pub use files::{MemoryFiles, MemoryValue};
pub use fragmentation::{FragmentationStats, FragmentationZone};
pub use hugepages::{HugePageFiles, HugePageStats, HugePageValue};
pub use ksm::KsmStats;
pub use lru::{LruGeneration, LruHistogram, LruStats};
pub use mappings::{MemoryMapping, ProcessMemoryDetails};
pub use numa::NumaStats;
pub use pages::{PageMapEntry, PageRange, PhysicalPageStats, ProcessPageMap, read_page_entries};
pub use pagetypeinfo::{FreeBlockCount, PageTypeBlockCounts, PageTypeFreeList, PageTypeInfo, PageTypeNode};
pub use process::{ProcessMemoryStats, SmapsRollup, SmapsRollupValue};
pub use procfs_core::{FromBufRead, FromRead, ProcError, ProcResult};
pub use procfs_core::{Iomem, Meminfo, MemoryPressure, PhysicalMemoryMap, PressureRecord, SharedMemorySegments, Shm, VmStat};
pub use provenance::{KernelLeakStats, PageOwnerStats};
pub use slabinfo::{SlabCache, SlabCpuStats, SlabGlobalStats, SlabInfo};
pub use slub::SlubStats;
pub use swaps::{SwapEntry, SwapType, Swaps};
/// Read failures shared by memory information collectors.
pub use sysctl::SysctlReadError as MemoryReadError;
/// Reasons a memory information entry was deliberately not opened.
pub use sysctl::SysctlSkipReason as MemorySkipReason;
pub use sysctl::{MemorySysctls, SysctlReadError, SysctlSkipReason};
pub use zoneinfo::{ZoneInfo, ZoneInfoZone, ZonePageSet, ZoneWatermarks};

#[cfg(target_os = "linux")]
pub use procfs::Current;
