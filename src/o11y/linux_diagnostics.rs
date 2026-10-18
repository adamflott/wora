use super::{Host, HostStats, O11yError, ProcessStats, RuntimeEnvironment};
use serde::Serialize;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Optional Linux memory collectors, sampled separately from routine host stats.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum LinuxMemoryCollector {
    /// Full smaps/mapping/NUMA views for the collecting process.
    ProcessMappings,
    /// Per-cache/per-node SLUB occupancy, policy and optional debugging counters.
    Slub,
    /// Kernel call-site allocation accounting and native vmalloc area inventory.
    KernelAllocations,
    /// Debugfs unusable-memory and external-fragmentation indices.
    Fragmentation,
    /// MGLRU runtime policy and existing per-cgroup/per-node age histograms.
    LruGenerations,
    /// Explicit bounded physical/virtual page ranges; never scans all memory.
    PhysicalPages,
    /// Page-owner per-page and aggregate allocation stack/handle dumps.
    PageOwner,
    /// Kernel leak suspects; explicitly selected because reads may incur scans.
    KernelLeaks,
    /// Memory hotplug inventory, EDAC/DIMM errors, poison recovery and CMA areas.
    MemoryHardware,
}

/// Explicit selection and cadence for potentially costly memory diagnostics.
#[derive(Clone, Debug)]
pub struct LinuxMemoryOptions {
    /// Aggregate collection limits and maximum retained diagnostic generations.
    pub limits: super::LinuxMemoryLimits,
    /// Per-file page-owner/kmemleak dump bound, 1..=64 MiB (default 4 MiB).
    pub provenance_byte_limit: u64,
    /// Optional physical PFN range; required to collect kpage arrays.
    pub physical_pages: Option<super::linux_mm::PageRange>,
    /// Optional collecting-process virtual page range for pagemap.
    pub virtual_pages: Option<super::linux_mm::PageRange>,
    /// Selected collectors; empty by default.
    pub collectors: BTreeSet<LinuxMemoryCollector>,
    /// Minimum time between diagnostic collections; defaults to 60 seconds.
    pub interval: Duration,
    /// Explicit procfs root, default `/proc`.
    pub proc_root: PathBuf,
    /// Explicit sysfs root, default `/sys` (including `kernel/debug` if mounted).
    pub sys_root: PathBuf,
}

impl Default for LinuxMemoryOptions {
    fn default() -> Self {
        Self {
            limits: super::LinuxMemoryLimits::default(),
            provenance_byte_limit: 4 * 1024 * 1024,
            physical_pages: None,
            virtual_pages: None,
            collectors: BTreeSet::new(),
            interval: Duration::from_secs(60),
            proc_root: "/proc".into(),
            sys_root: "/sys".into(),
        }
    }
}

impl LinuxMemoryOptions {
    /// Set aggregate collection limits and live diagnostic generation admission.
    pub fn with_memory_limits(mut self, limits: super::LinuxMemoryLimits) -> Self {
        self.limits = limits;
        self
    }
    /// Explicitly enable every snapshot diagnostic except range-based page scans.
    /// Includes costly page-owner and kmemleak reads. Event tracing is separate.
    pub fn all_snapshots() -> Self {
        [
            LinuxMemoryCollector::ProcessMappings,
            LinuxMemoryCollector::Slub,
            LinuxMemoryCollector::KernelAllocations,
            LinuxMemoryCollector::Fragmentation,
            LinuxMemoryCollector::LruGenerations,
            LinuxMemoryCollector::PageOwner,
            LinuxMemoryCollector::KernelLeaks,
            LinuxMemoryCollector::MemoryHardware,
        ]
        .into_iter()
        .fold(Self::new(), Self::enable)
    }
    /// Enable bounded physical-page collection for an explicit PFN range.
    pub fn with_physical_page_range(mut self, range: super::linux_mm::PageRange) -> Self {
        self.physical_pages = Some(range);
        self.collectors.insert(LinuxMemoryCollector::PhysicalPages);
        self
    }
    /// Enable bounded pagemap collection for collecting-process virtual page indices.
    pub fn with_virtual_page_range(mut self, range: super::linux_mm::PageRange) -> Self {
        self.virtual_pages = Some(range);
        self.collectors.insert(LinuxMemoryCollector::PhysicalPages);
        self
    }
    /// Start with all diagnostic collectors disabled.
    pub fn new() -> Self {
        Self::default()
    }
    /// Enable a diagnostic collector. Repeated selections are idempotent.
    pub fn enable(mut self, collector: LinuxMemoryCollector) -> Self {
        self.collectors.insert(collector);
        self
    }
    /// Set the diagnostic interval. Zero is rejected when constructing the provider.
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }
    /// Supply explicit procfs/sysfs trees for alternate mounts or captured data.
    pub fn with_roots(mut self, proc_root: impl Into<PathBuf>, sys_root: impl Into<PathBuf>) -> Self {
        self.proc_root = proc_root.into();
        self.sys_root = sys_root.into();
        self
    }
}

/// Diagnostic data with its own timestamp; may be older than the host snapshot.
#[derive(Clone, Debug, Serialize)]
pub struct LinuxMemoryDiagnostics {
    /// Aggregate source accounting; an exhausted limit marks this batch incomplete.
    pub collection: super::linux_mm::CollectionReport,
    /// Memory blocks, hardware errors and reserved contiguous allocation areas.
    pub hardware: Option<Box<super::linux_mm::MemoryHardwareStats>>,
    /// Existing page allocation provenance, without changing tracking/filter state.
    pub page_owner: Option<Box<super::linux_mm::PageOwnerStats>>,
    /// Existing kernel leak suspects; only present with explicit KernelLeaks selection.
    pub kernel_leaks: Option<Box<super::linux_mm::KernelLeakStats>>,
    /// Exactly the requested PFN range, with independent kpage file errors.
    pub physical_pages: Option<Box<super::linux_mm::PhysicalPageStats>>,
    /// Exactly the requested process virtual-page range; PFNs may be redacted.
    pub page_map: Option<Box<super::linux_mm::ProcessPageMap>>,
    /// MGLRU policy and page-age histograms, without active aging or reclaim.
    pub lru_generations: Option<Box<super::linux_mm::LruStats>>,
    /// Exact per-order fragmentation indices, without floating-point conversion.
    pub fragmentation: Option<Box<super::linux_mm::FragmentationStats>>,
    /// Kernel allocation sites and vmalloc areas, requiring kernel support/access.
    pub kernel_allocations: Option<Box<super::linux_mm::KernelAllocationStats>>,
    /// SLUB details, including explicit cache aliases.
    pub slub: Option<Box<super::linux_mm::SlubStats>>,
    /// Time this diagnostic batch was collected, independent of host sample time.
    pub sampled_at: chrono::DateTime<chrono::Utc>,
    /// Full process mapping details, present only when explicitly selected.
    pub process_mappings: Option<Box<super::linux_mm::ProcessMemoryDetails>>,
}

/// Decorates any existing telemetry provider with selected Linux diagnostics.
///
/// Clones and published snapshots share each cached diagnostic batch. Collection
/// happens on the runner's blocking worker after startup, no more often than
/// `interval`. A refresh creates a new batch; retained snapshots keep their
/// original data and timestamp. No settings or tracing controls are changed.
#[derive(Clone, Debug)]
pub struct LinuxMemoryEnvironment<R> {
    inner: R,
    options: LinuxMemoryOptions,
    cache: Arc<Mutex<DiagnosticCache>>,
}

#[derive(Debug, Default)]
struct DiagnosticCache {
    sample: Option<(Instant, Arc<LinuxMemoryDiagnostics>)>,
    generations: super::generations::Generations<LinuxMemoryDiagnostics>,
}

impl<R> LinuxMemoryEnvironment<R> {
    /// Validate options and wrap a telemetry provider without performing I/O.
    pub fn new(inner: R, options: LinuxMemoryOptions) -> Result<Self, O11yError> {
        options.limits.validate().map_err(|error| O11yError::RuntimeEnvironment(error.to_string()))?;
        if options.proc_root.as_os_str().len() > options.limits.collection.path_bytes
            || options.sys_root.as_os_str().len() > options.limits.collection.path_bytes
        {
            return Err(O11yError::RuntimeEnvironment("memory source roots exceed path byte limit".into()));
        }
        if !(1..=64 * 1024 * 1024).contains(&options.provenance_byte_limit) {
            return Err(O11yError::RuntimeEnvironment("provenance byte limit must be 1..=64 MiB".into()));
        }
        if options.collectors.contains(&LinuxMemoryCollector::PhysicalPages) {
            if options.physical_pages.is_none() && options.virtual_pages.is_none() {
                return Err(O11yError::RuntimeEnvironment(
                    "PhysicalPages requires an explicit PFN or virtual-page range".into(),
                ));
            }
            for range in [options.physical_pages, options.virtual_pages].into_iter().flatten() {
                range.validate().map_err(|e| O11yError::RuntimeEnvironment(e.to_string()))?;
            }
        }
        if options.interval.is_zero() {
            return Err(O11yError::RuntimeEnvironment("Linux memory diagnostic interval must be nonzero".into()));
        }
        Ok(Self {
            inner,
            options,
            cache: Arc::new(Mutex::new(DiagnosticCache::default())),
        })
    }

    /// Inspect diagnostic generations and refreshes skipped due to retained batches.
    /// A skipped refresh reuses the previous batch and its original timestamp.
    pub fn diagnostic_sampling_status(&self) -> Result<super::MemorySamplingStatus, O11yError> {
        self.cache
            .lock()
            .map(|cache| cache.generations.status())
            .map_err(|_| O11yError::RuntimeEnvironment("Linux memory diagnostic cache poisoned".into()))
    }

    fn decorate(&self, stats: &mut HostStats) -> Result<(), O11yError> {
        if self.options.collectors.is_empty() {
            return Ok(());
        }
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| O11yError::RuntimeEnvironment("Linux memory diagnostic cache poisoned".into()))?;
        if cache.sample.as_ref().is_none_or(|(time, _)| time.elapsed() >= self.options.interval) && cache.generations.admit(self.options.limits) {
            let selected = |kind| self.options.collectors.contains(&kind);
            let (mut data, report) = super::linux_mm::collect_with_limits(self.options.limits.collection, || LinuxMemoryDiagnostics {
                collection: super::linux_mm::CollectionReport::default(),
                hardware: selected(LinuxMemoryCollector::MemoryHardware)
                    .then(|| Box::new(super::linux_mm::MemoryHardwareStats::from_sys_root(&self.options.sys_root))),
                page_owner: selected(LinuxMemoryCollector::PageOwner).then(|| {
                    Box::new(super::linux_mm::PageOwnerStats::from_debug_root_with_limit(
                        self.options.sys_root.join("kernel/debug"),
                        self.options.provenance_byte_limit,
                    ))
                }),
                kernel_leaks: selected(LinuxMemoryCollector::KernelLeaks).then(|| {
                    Box::new(super::linux_mm::KernelLeakStats::from_debug_root_with_limit(
                        self.options.sys_root.join("kernel/debug"),
                        self.options.provenance_byte_limit,
                    ))
                }),
                physical_pages: selected(LinuxMemoryCollector::PhysicalPages)
                    .then_some(self.options.physical_pages)
                    .flatten()
                    .map(|range| Box::new(super::linux_mm::PhysicalPageStats::from_proc_root(&self.options.proc_root, range))),
                page_map: selected(LinuxMemoryCollector::PhysicalPages)
                    .then_some(self.options.virtual_pages)
                    .flatten()
                    .map(|range| {
                        Box::new(super::linux_mm::ProcessPageMap::from_process_root(
                            std::process::id(),
                            self.options.proc_root.join("self"),
                            range,
                        ))
                    }),
                lru_generations: selected(LinuxMemoryCollector::LruGenerations)
                    .then(|| Box::new(super::linux_mm::LruStats::from_sys_root(&self.options.sys_root))),
                fragmentation: selected(LinuxMemoryCollector::Fragmentation)
                    .then(|| Box::new(super::linux_mm::FragmentationStats::from_debug_root(self.options.sys_root.join("kernel/debug")))),
                kernel_allocations: selected(LinuxMemoryCollector::KernelAllocations)
                    .then(|| Box::new(super::linux_mm::KernelAllocationStats::from_proc_root(&self.options.proc_root))),
                slub: selected(LinuxMemoryCollector::Slub).then(|| Box::new(super::linux_mm::SlubStats::from_sys_root(&self.options.sys_root))),
                sampled_at: chrono::Utc::now(),
                process_mappings: selected(LinuxMemoryCollector::ProcessMappings).then(|| {
                    Box::new(super::linux_mm::ProcessMemoryDetails::from_process_root(
                        std::process::id(),
                        self.options.proc_root.join("self"),
                    ))
                }),
            })
            .map_err(|error| O11yError::RuntimeEnvironment(error.to_string()))?;
            data.collection = report;
            let bytes = super::generations::snapshot_size(&data, self.options.limits.snapshot_bytes)?;
            let data = Arc::new(data);
            cache.generations.track(&data, bytes);
            cache.sample = Some((Instant::now(), data));
        }
        stats.linux_mm.diagnostics = cache.sample.as_ref().map(|(_, data)| Arc::clone(data));
        Ok(())
    }
}

impl<R: RuntimeEnvironment> RuntimeEnvironment for LinuxMemoryEnvironment<R> {
    fn initial_host(&self) -> Result<Host, O11yError> {
        let mut host = self.inner.initial_host()?;
        self.decorate(&mut host.stats)?;
        Ok(host)
    }
    fn initial_process_stats(&self) -> Option<ProcessStats> {
        self.inner.initial_process_stats()
    }
    fn refresh_host_stats(&self) -> Result<Option<HostStats>, O11yError> {
        let mut stats = self.inner.refresh_host_stats()?;
        if let Some(stats) = stats.as_mut() {
            self.decorate(stats)?;
        }
        Ok(stats)
    }
    fn refresh_process_stats(&self) -> Option<ProcessStats> {
        self.inner.refresh_process_stats()
    }
}
