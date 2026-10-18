use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Serialize;

use super::linux_mm::{
    BuddyInfo, CgroupMemoryStats, FromRead, HugePageStats, Iomem, Meminfo, MemoryPressure, MemorySysctls, PageTypeInfo, ProcessMemoryStats,
    SharedMemorySegments, SlabInfo, Swaps, VmStat, ZoneInfo,
};

/// Linux memory-management snapshots carried by [`super::HostStats`].
///
/// Each source is read independently. Missing, restricted, or malformed files
/// produce `None` for that source and a debug log, without discarding other host
/// metrics. Sysctls, cgroup memory, hugepages, and process rollups retain
/// individual read failures in their own snapshots. Cgroup and process fields
/// describe the collector's workload, while the other fields describe the host.
/// Values retain their source units and page-type lower bounds.
#[derive(Clone, Default, Debug, Serialize)]
pub struct LinuxMemoryStats {
    /// Explicitly enabled diagnostics, with an independent collection timestamp.
    ///
    /// Snapshots share each diagnostic batch until the next collection. Use
    /// [`Arc::make_mut`] to obtain an independent writable copy when needed.
    pub diagnostics: Option<Arc<super::LinuxMemoryDiagnostics>>,
    /// KSM saved/shared pages, scanner policy and process-specific accounting.
    pub ksm: Option<Box<super::linux_mm::KsmStats>>,
    /// Zram device accounting and zswap policy/debugfs counters.
    pub compressed: Option<Box<super::linux_mm::CompressedMemoryStats>>,
    /// Host NUMA nodes, locality statistics and memory-tier membership.
    pub numa: Option<Box<super::linux_mm::NumaStats>>,
    /// Overall host memory inventory from `/proc/meminfo`, with sizes in bytes.
    pub meminfo: Option<Box<Meminfo>>,
    /// Kernel VM gauges and counters from `/proc/vmstat`, in their native units.
    pub vmstat: Option<VmStat>,
    /// Memory stall pressure from `/proc/pressure/memory`.
    pub pressure: Option<MemoryPressure>,
    /// System V shared memory segments from `/proc/sysvipc/shm`.
    pub shared_memory: Option<SharedMemorySegments>,
    /// Physical memory regions from `/proc/iomem`; addresses may be restricted.
    pub iomem: Option<Iomem>,
    /// Free allocator blocks by NUMA node, zone, and allocation order.
    pub buddyinfo: Option<BuddyInfo>,
    /// Zone inventory, watermarks, and node/zone/per-CPU statistics.
    pub zoneinfo: Option<ZoneInfo>,
    /// Swap devices and files, including sizes and usage in KiB.
    pub swaps: Option<Swaps>,
    /// Slab cache geometry and optional legacy allocator statistics.
    pub slabinfo: Option<SlabInfo>,
    /// Free lists and pageblock counts by migration type.
    pub pagetypeinfo: Option<PageTypeInfo>,
    /// Structured memory sysctl settings in native units, with per-key failures.
    ///
    /// Includes VM, kernel shared-memory and NUMA, and selected filesystem
    /// settings. Action controls and `vm.stat_refresh` are never opened.
    pub sysctls: MemorySysctls,
    /// Collecting process's cgroup v2 usage, events, pressure, and visible ancestor limits.
    pub cgroup: Box<CgroupMemoryStats>,
    /// THP policies/per-size statistics and global/per-node HugeTLB pools.
    ///
    /// Present when a sysfs root was supplied. Individual access failures are
    /// recorded within the snapshot. Proc-only captures leave this as `None`.
    pub hugepages: Option<Box<HugePageStats>>,
    /// Collecting process's PID and memory mapping rollup, in native source units.
    pub process: Box<ProcessMemoryStats>,
}

impl LinuxMemoryStats {
    /// Collect a best-effort snapshot from `/proc` and `/sys`.
    ///
    /// Reads are synchronous. Slab and page-type information commonly requires
    /// root; page-type collection and process rollup page-table walks can be
    /// expensive. The runner uses the host
    /// statistics interval to control the sampling frequency and performs periodic
    /// collection on a blocking worker.
    pub fn current() -> Self {
        Self::from_roots("/proc", "/sys")
    }

    /// Collect procfs and sysfs memory information from explicitly supplied roots.
    ///
    /// Cgroup mount paths are discovered from `proc_root/self/mountinfo` and used
    /// as written. Hugepage paths are relative to `sys_root`; captured roots never
    /// fall back to the running host's sysfs.
    pub fn from_roots(proc_root: impl AsRef<Path>, sys_root: impl AsRef<Path>) -> Self {
        Self {
            ksm: Some(Box::new(super::linux_mm::KsmStats::from_roots(
                &sys_root,
                proc_root.as_ref().join("self"),
                std::process::id(),
            ))),
            hugepages: Some(Box::new(HugePageStats::from_sys_root(&sys_root))),
            numa: Some(Box::new(super::linux_mm::NumaStats::from_sys_root(&sys_root))),
            compressed: Some(Box::new(super::linux_mm::CompressedMemoryStats::from_sys_root(&sys_root))),
            ..Self::from_proc_root(proc_root)
        }
    }

    /// Collect from an alternate proc mount or directory of captured files.
    ///
    /// Files must use their procfs names, with sysctls under `sys/{vm,kernel,fs}`.
    /// Each read is synchronous. A failed procfs snapshot becomes `None`;
    /// individual sysctl failures are recorded in [`MemorySysctls::unavailable`].
    /// Cgroup and process sources use `self/` files. Hugepages remain `None`;
    /// use [`Self::from_roots`] to include an explicit sysfs root.
    pub fn from_proc_root(root: impl AsRef<Path>) -> Self {
        let root = root.as_ref();
        Self {
            meminfo: read(root.join("meminfo")).map(Box::new),
            vmstat: read(root.join("vmstat")),
            pressure: read(root.join("pressure/memory")),
            shared_memory: read(root.join("sysvipc/shm")),
            iomem: read(root.join("iomem")),
            buddyinfo: read(root.join("buddyinfo")),
            zoneinfo: read(root.join("zoneinfo")),
            swaps: read(root.join("swaps")),
            slabinfo: read(root.join("slabinfo")),
            pagetypeinfo: read(root.join("pagetypeinfo")),
            sysctls: MemorySysctls::from_proc_root(root),
            cgroup: Box::new(CgroupMemoryStats::from_proc_root(root)),
            hugepages: None,
            numa: None,
            compressed: None,
            ksm: None,
            diagnostics: None,
            process: Box::new(ProcessMemoryStats::from_process_root(std::process::id(), root.join("self"))),
        }
    }
}

fn read<T: FromRead>(path: PathBuf) -> Option<T> {
    match T::from_file(&path) {
        Ok(snapshot) => Some(snapshot),
        Err(error) => {
            tracing::debug!(path = %path.display(), %error, "Linux memory statistics source unavailable");
            None
        }
    }
}
