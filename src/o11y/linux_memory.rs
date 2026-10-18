use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use serde::Serialize;

use super::linux_mm::{
    BuddyInfo, CgroupMemoryStats, CollectionLimits, CollectionReport, FromRead, HugePageStats, Iomem, Meminfo, MemoryPressure, MemorySysctls, PageTypeInfo,
    ProcessMemoryStats, SharedMemorySegments, SlabInfo, Swaps, VmStat, ZoneInfo, collect_with_limits,
};

/// Collection limits and retained-generation admission for Linux memory samples.
///
/// Input bytes are not heap bytes or RSS. Combined input, entry, line/path and
/// generation limits bound collector-owned data growth. Caller-created copies,
/// custom providers, queues and output writers require their own limits.
#[derive(Clone, Copy, Debug)]
pub struct LinuxMemoryLimits {
    /// Separate retained/scratch allocation allowances for instrumented storage.
    /// Includes a caller-configured allocator reserve, not measured RSS.
    pub heap: super::linux_mm::HeapLimits,
    /// Shared source/work budget for one routine or diagnostic batch.
    pub collection: CollectionLimits,
    /// Maximum live generations, including the replacement under construction.
    /// At least three are required to allow startup, current and replacement data.
    pub live_snapshots: usize,
    /// Maximum JSON-encoded bytes in one admitted routine or diagnostic batch.
    /// This is a payload limit, not a measurement of allocator overhead.
    pub snapshot_bytes: usize,
    /// Aggregate JSON-encoded bytes in live batches, reserving `snapshot_bytes`
    /// before constructing a replacement. Routine/diagnostic providers each own
    /// a separate allowance. Rust heap and allocator overhead are additional.
    pub retained_snapshot_bytes: usize,
}

impl Default for LinuxMemoryLimits {
    fn default() -> Self {
        Self {
            heap: super::linux_mm::HeapLimits::default(),
            collection: CollectionLimits::default(),
            live_snapshots: 4,
            snapshot_bytes: 8 * 1024 * 1024,
            retained_snapshot_bytes: 32 * 1024 * 1024,
        }
    }
}

impl LinuxMemoryLimits {
    /// Validate limits before collecting or allocating snapshot data.
    pub fn validate(&self) -> std::io::Result<()> {
        self.heap.validate()?;
        self.collection.validate()?;
        if self.snapshot_bytes == 0 || self.retained_snapshot_bytes < self.snapshot_bytes {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid memory snapshot byte limits"));
        }
        if !(3..=1024).contains(&self.live_snapshots) {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "live memory snapshots must be 3..=1024"));
        }
        Ok(())
    }
}

/// Linux memory-management sample with independently shared routine/diagnostic data.
///
/// Field reads use `Deref`. Build a replacement with [`Self::try_rebuild`]; there
/// is no mutable dereference or public owning Arc. JSON field paths are unchanged.
/// Clones share both data and its allocation lease until the last handle drops.
/// `Default` is a shared empty sentinel, not a collected sample; its fixed
/// initialization/control allocations are outside provider heap counters.
///
/// ```compile_fail
/// let mut sample = wora::o11y::LinuxMemoryStats::default();
/// sample.vmstat = None; // Published routine tables are immutable.
/// ```
#[derive(Clone, Debug, Serialize)]
pub struct LinuxMemoryStats {
    /// Explicitly enabled diagnostics, shared until the next diagnostic batch.
    pub(super) diagnostics: Option<Arc<super::LinuxMemoryDiagnostics>>,
    #[serde(flatten)]
    pub(super) data: Arc<LinuxMemoryData>,
    #[serde(skip)]
    pub(super) allocation: Option<Arc<super::linux_mm::HeapReservation>>,
}

impl Default for LinuxMemoryStats {
    fn default() -> Self {
        static EMPTY: LazyLock<Arc<LinuxMemoryData>> = LazyLock::new(|| Arc::new(LinuxMemoryData::default()));
        Self {
            data: Arc::clone(&EMPTY),
            diagnostics: None,
            allocation: None,
        }
    }
}

impl Deref for LinuxMemoryStats {
    type Target = LinuxMemoryData;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

/// Routine Linux memory tables, shared by host samples, events and memory caches.
///
/// Paths below use the live `/proc` and `/sys` roots. [`LinuxMemoryStats::from_roots`]
/// substitutes captured/alternate roots; process files are under `<proc_root>/self`.
/// Reads are independent, not atomic across files or with `sysinfo` totals. Source
/// units are retained unless a field explicitly documents conversion to bytes.
/// Host, process, cgroup, NUMA and allocator views overlap and must not be summed.
#[derive(Default, Debug, Serialize)]
pub struct LinuxMemoryData {
    /// Collection accounting; an exhausted limit explicitly marks incomplete data.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collection: Option<CollectionReport>,
    /// KSM pages/policy from `/sys/kernel/mm/ksm/*` and process accounting from
    /// `/proc/self/{ksm_stat,ksm_merging_pages}`. Native units are retained.
    pub ksm: Option<Box<super::linux_mm::KsmStats>>,
    /// Zram accounting from `/sys/block/zram*`, zswap policy from
    /// `/sys/module/zswap/parameters` and counters from `/sys/kernel/debug/zswap`.
    pub compressed: Option<Box<super::linux_mm::CompressedMemoryStats>>,
    /// NUMA inventory/statistics from `/sys/devices/system/node` and tier membership
    /// from `/sys/devices/virtual/memory_tiering`; see [`super::linux_mm::NumaStats`].
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
    /// Free blocks by node/zone/order from `/proc/buddyinfo`; counts are blocks,
    /// with `2^order` base pages per block, not bytes.
    pub buddyinfo: Option<BuddyInfo>,
    /// Inventory/watermarks and node/zone/per-CPU statistics from `/proc/zoneinfo`;
    /// page fields and native counter units remain distinct.
    pub zoneinfo: Option<ZoneInfo>,
    /// Swap devices/files from `/proc/swaps`, including sizes and usage in KiB.
    pub swaps: Option<Swaps>,
    /// Slab geometry and optional legacy statistics from `/proc/slabinfo`;
    /// object sizes are bytes, slab geometry uses base pages.
    pub slabinfo: Option<SlabInfo>,
    /// Free blocks/pageblocks by migration type from `/proc/pagetypeinfo`;
    /// capped counts retain lower bounds. Reading can drain per-CPU page lists
    /// when the kernel's page-owner mixed-table reporting is active.
    pub pagetypeinfo: Option<PageTypeInfo>,
    /// Structured memory sysctl settings in native units, with per-key failures.
    ///
    /// Reads `/proc/sys/vm`, selected `/proc/sys/kernel` shared-memory/NUMA/perf
    /// settings and `/proc/sys/fs` cache/descriptor inventories; see [`MemorySysctls`].
    /// Action controls and `vm.stat_refresh` are never opened.
    pub sysctls: MemorySysctls,
    /// Cgroup v2 discovered via `/proc/self/{cgroup,mountinfo}`, then read from
    /// `memory.*`, selected `dmem.*`, `hugetlb.*` and cpuset memory-node files in its mounted directory
    /// and visible parents. See [`CgroupMemoryStats`] for scope and discovery limits.
    pub cgroup: Box<CgroupMemoryStats>,
    /// THP policies/counters from `/sys/kernel/mm/transparent_hugepage`, HugeTLB
    /// pools from `/sys/kernel/mm/hugepages` and `node*/hugepages` under
    /// `/sys/devices/system/node`. File units and page-size labels are retained.
    ///
    /// Present when a sysfs root was supplied. Individual access failures are
    /// recorded within the snapshot. Proc-only captures leave this as `None`.
    pub hugepages: Option<Box<HugePageStats>>,
    /// Collecting PID and `/proc/self/{status,limits,stat,oom_score,oom_score_adj}`
    /// information plus `/proc/self/smaps_rollup`. Rollup `kB` means KiB;
    /// [`super::linux_mm::SmapsRollupValue::bytes`] converts with overflow checks.
    pub process: Box<ProcessMemoryStats>,
}

impl LinuxMemoryStats {
    /// Reserve shared snapshot storage before invoking a caller-supplied builder.
    ///
    /// Instrumented builders use `budget`; prebuilt caller values and remaining
    /// uninstrumented typed tables are not retroactively charged. The closure
    /// performs no implicit OS reads. Failed construction releases its lease.
    pub fn try_new(budget: &super::linux_mm::HeapBudget, build: impl FnOnce() -> std::io::Result<LinuxMemoryData>) -> std::io::Result<Self> {
        use super::linux_mm::{HeapKind, HeapReservation, with_heap_budget};
        let bytes = std::mem::size_of::<LinuxMemoryData>() + std::mem::size_of::<HeapReservation>() + 4 * std::mem::size_of::<usize>();
        let allocation = budget.try_reserve(HeapKind::Retained, bytes, 2)?;
        let data = with_heap_budget(budget, build)?;
        Ok(Self {
            data: Arc::new(data),
            diagnostics: None,
            allocation: Some(Arc::new(allocation)),
        })
    }

    /// Admit shared storage for already-built caller data, without collecting.
    /// Existing child allocations retain their original ownership/budgets.
    pub fn try_from_data(data: LinuxMemoryData, budget: &super::linux_mm::HeapBudget) -> std::io::Result<Self> {
        Self::try_new(budget, || Ok(data))
    }

    /// Build an independently admitted replacement while preserving this sample.
    /// Reuse the same budget to account for old/new overlap. Diagnostics remain
    /// shared. The builder receives borrowed data and must explicitly construct
    /// its replacement; there is no unmetered deep clone of the entire sample.
    pub fn try_rebuild(
        &self,
        budget: &super::linux_mm::HeapBudget,
        build: impl FnOnce(&LinuxMemoryData) -> std::io::Result<LinuxMemoryData>,
    ) -> std::io::Result<Self> {
        let mut replacement = Self::try_new(budget, || build(&self.data))?;
        replacement.diagnostics = self.diagnostics.clone();
        Ok(replacement)
    }

    /// Borrow the routine tables without acquiring an owning Arc.
    pub fn data(&self) -> &LinuxMemoryData {
        &self.data
    }

    /// Borrow selected diagnostic tables and their independent sampling timestamp.
    pub fn diagnostics(&self) -> Option<&super::LinuxMemoryDiagnostics> {
        self.diagnostics.as_deref()
    }

    /// Whether two handles retain the same routine allocation; performs no I/O.
    pub fn shares_data_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.data, &other.data)
    }

    /// Inspect the aggregate budget owning this sample's shared storage.
    /// `None` denotes the empty default sentinel. These counters also include
    /// other live samples/builders using that budget, not process RSS.
    pub fn heap_usage(&self) -> Option<super::linux_mm::HeapUsage> {
        self.allocation.as_ref().map(|allocation| allocation.budget().usage())
    }

    /// Collect `/proc` and `/sys` with default shared input/work limits.
    pub fn current() -> Self {
        Self::from_roots("/proc", "/sys")
    }

    /// Collect explicit procfs/sysfs trees with default shared limits.
    pub fn from_roots(proc_root: impl AsRef<Path>, sys_root: impl AsRef<Path>) -> Self {
        Self::collect(
            proc_root.as_ref(),
            Some(sys_root.as_ref()),
            CollectionLimits::default(),
            &super::linux_mm::HeapBudget::current(),
        )
        .unwrap_or_else(|error| {
            tracing::debug!(%error, "memory collection failed");
            Self::default()
        })
    }

    /// Collect explicit trees using one shared budget for all routine sources.
    /// Invalid limits fail before I/O. Source failures remain in their snapshots;
    /// exhausted limits also appear in `data.collection` as incomplete accounting.
    pub fn from_roots_with_limits(proc_root: impl AsRef<Path>, sys_root: impl AsRef<Path>, limits: LinuxMemoryLimits) -> std::io::Result<Self> {
        limits.validate()?;
        let budget = super::linux_mm::HeapBudget::new(limits.heap)?;
        let sample = Self::collect(proc_root.as_ref(), Some(sys_root.as_ref()), limits.collection, &budget)?;
        super::generations::snapshot_size(sample.data(), limits.snapshot_bytes).map_err(std::io::Error::other)?;
        Ok(sample)
    }

    /// Collect captured/live roots with shared allocation admission across calls.
    /// Source limits apply to this batch; the supplied heap budget also includes
    /// retained older generations. Individual source errors remain explicit.
    pub fn from_roots_with_budget(
        proc_root: impl AsRef<Path>,
        sys_root: impl AsRef<Path>,
        limits: CollectionLimits,
        budget: &super::linux_mm::HeapBudget,
    ) -> std::io::Result<Self> {
        Self::collect(proc_root.as_ref(), Some(sys_root.as_ref()), limits, budget)
    }

    /// Collect a procfs tree with default shared limits and no host-sysfs fallback.
    pub fn from_proc_root(root: impl AsRef<Path>) -> Self {
        Self::collect(root.as_ref(), None, CollectionLimits::default(), &super::linux_mm::HeapBudget::current()).unwrap_or_else(|error| {
            tracing::debug!(%error, "memory collection failed");
            Self::default()
        })
    }

    fn collect(root: &Path, sys: Option<&Path>, limits: CollectionLimits, budget: &super::linux_mm::HeapBudget) -> std::io::Result<Self> {
        limits.validate()?;
        if root.as_os_str().len() > limits.path_bytes || sys.is_some_and(|sys| sys.as_os_str().len() > limits.path_bytes) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "memory source roots exceed path byte limit",
            ));
        }
        Self::try_new(budget, || {
            let (mut data, report) = collect_with_limits(limits, || LinuxMemoryData {
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
                hugepages: sys.map(|sys| Box::new(HugePageStats::from_sys_root(sys))),
                numa: sys.map(|sys| Box::new(super::linux_mm::NumaStats::from_sys_root(sys))),
                compressed: sys.map(|sys| Box::new(super::linux_mm::CompressedMemoryStats::from_sys_root(sys))),
                ksm: sys.map(|sys| Box::new(super::linux_mm::KsmStats::from_roots(sys, root.join("self"), std::process::id()))),
                process: Box::new(ProcessMemoryStats::from_process_root(std::process::id(), root.join("self"))),
                collection: None,
            })?;
            data.collection = Some(report);
            Ok(data)
        })
    }
}

fn read<T: FromRead>(path: PathBuf) -> Option<T> {
    match super::linux_mm::read_source(&path) {
        Ok(snapshot) => Some(snapshot),
        Err(error) => {
            tracing::debug!(path = %path.display(), %error, "Linux memory statistics source unavailable");
            None
        }
    }
}
