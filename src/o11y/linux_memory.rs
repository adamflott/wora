use std::path::{Path, PathBuf};

use serde::Serialize;

use super::linux_mm::{BuddyInfo, FromRead, Iomem, Meminfo, MemoryPressure, PageTypeInfo, SharedMemorySegments, SlabInfo, Swaps, VmStat, ZoneInfo};

/// Linux memory-management snapshots carried by [`super::HostStats`].
///
/// Each source is read independently. Missing, restricted, or malformed files
/// produce `None` for that source and a debug log, without discarding other host
/// metrics. Values retain their source units and page-type lower bounds.
#[derive(Clone, Default, Debug, Serialize)]
pub struct LinuxMemoryStats {
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
}

impl LinuxMemoryStats {
    /// Collect a best-effort snapshot from `/proc`.
    ///
    /// Reads are synchronous. Slab and page-type information commonly requires
    /// root; page-type collection can be expensive. The runner uses the host
    /// statistics interval to control the sampling frequency and performs periodic
    /// collection on a blocking worker.
    pub fn current() -> Self {
        Self::from_proc_root("/proc")
    }

    /// Collect from an alternate proc mount or directory of captured files.
    ///
    /// Files must use their procfs names. Each read is synchronous and a failed
    /// source becomes `None`, independently of the other sources.
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
