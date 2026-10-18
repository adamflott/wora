#![cfg_attr(target_os = "linux", doc = include_str!("../README.md"))]
#![cfg(target_os = "linux")]

mod allocations;
mod buddyinfo;
mod budget;
mod cgroup;
mod compressed;
mod damon;
mod files;
mod fragmentation;
mod hardware;
mod heap;
mod hugepages;
mod ksm;
mod lru;
mod mappings;
mod numa;
mod pages;
mod pagetypeinfo;
mod process;
mod process_values;
mod provenance;
mod shrinker;
mod slabinfo;
mod slub;
mod storage;
mod structured;
mod swaps;
mod sysctl;
mod trace;
mod zoneinfo;

pub use allocations::{AllocationSite, AllocationSites, KernelAllocationStats};
pub use buddyinfo::{BuddyInfo, BuddyZone};
pub use budget::{CollectionLimit, CollectionLimits, CollectionReport, collect_with_limits};
pub use cgroup::{CgroupMemory, CgroupMemoryStats, CgroupMemoryValue};
pub use compressed::{CompressedMemoryStats, ZramDevice, ZramMemoryStat};
pub use damon::DamonStats;
pub use files::{MemoryFiles, read_source};
pub use fragmentation::{FragmentationStats, FragmentationZone};
pub use hardware::MemoryHardwareStats;
pub use heap::{HeapBudget, HeapError, HeapKind, HeapLimits, HeapReservation, HeapUsage, with_heap_budget};
pub use hugepages::{HugePageFiles, HugePageStats};
pub use ksm::KsmStats;
pub use lru::{LruGeneration, LruHistogram, LruStats};
pub use mappings::{MemoryMapping, ProcessMemoryDetails};
pub use numa::NumaStats;
pub use pages::{PageMapEntry, PageRange, PhysicalPageStats, ProcessPageMap, UserfaultfdProtection, read_page_entries};
pub use pagetypeinfo::{FreeBlockCount, PageTypeBlockCounts, PageTypeFreeList, PageTypeInfo, PageTypeNode};
pub use process::{ProcessMemoryStats, SmapsRollup, SmapsRollupValue};
/// `/proc/iomem` physical address ranges and labels, including nested regions.
/// Addresses may be redacted by kernel policy; alternate files can be parsed.
#[doc(inline)]
pub use procfs_core::Iomem;
/// `/proc/meminfo` host inventory. The procfs parser converts `kB` (KiB) sizes
/// to bytes; count fields such as HugeTLB page counts retain their own meaning.
#[doc(inline)]
pub use procfs_core::Meminfo;
/// `/proc/pressure/memory` host stall accounting; cgroup `memory.pressure` uses
/// the same parser. Scope follows the file supplied to the reader.
#[doc(inline)]
pub use procfs_core::MemoryPressure;
/// One inclusive physical address range and label from `/proc/iomem`, used in
/// [`Iomem`]. These ranges are not resident-memory allocations.
#[doc(inline)]
pub use procfs_core::PhysicalMemoryMap;
/// One `some` or `full` line from `/proc/pressure/memory` or cgroup
/// `memory.pressure`: stall averages are percentages and total time is microseconds.
#[doc(inline)]
pub use procfs_core::PressureRecord;
/// System V shared-memory inventory from `/proc/sysvipc/shm`; contains [`Shm`] rows.
#[doc(inline)]
pub use procfs_core::SharedMemorySegments;
/// One `/proc/sysvipc/shm` row: size/resident/swap amounts are bytes; IDs,
/// attachment counts, permissions and timestamps retain the kernel's units.
#[doc(inline)]
pub use procfs_core::Shm;
/// `/proc/vmstat` named VM gauges and cumulative counters. Units depend on each
/// kernel key; values are not uniformly byte sizes or page counts.
#[doc(inline)]
pub use procfs_core::VmStat;
pub use procfs_core::{FromBufRead, FromRead, ProcError, ProcResult};
pub use provenance::{KernelLeakStats, PageOwnerStats};
pub use shrinker::ShrinkerStats;
pub use slabinfo::{SlabCache, SlabCpuStats, SlabGlobalStats, SlabInfo};
pub use slub::SlubStats;
pub use storage::{MemoryList, MemoryMap, MemoryText};
pub use structured::{ParsedMemoryValue, parse_memory_file, parse_sysctl_value};
pub use swaps::{SwapEntry, SwapType, Swaps};
/// Read failures shared by memory information collectors.
pub use sysctl::SysctlReadError as MemoryReadError;
/// Reasons a memory information entry was deliberately not opened.
pub use sysctl::SysctlSkipReason as MemorySkipReason;
pub use sysctl::{MemorySysctls, SysctlReadError, SysctlSkipReason};
pub use trace::{MemoryTraceEvent, MemoryTraceParser, MemoryTraceRecord, MemoryTracepoint};
pub use zoneinfo::{ZoneInfo, ZoneInfoZone, ZonePageSet, ZoneWatermarks};

#[cfg(target_os = "linux")]
pub use procfs::Current;
