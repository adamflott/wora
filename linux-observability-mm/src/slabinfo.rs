use std::collections::BTreeSet;
use std::io::BufRead;

use procfs_core::{FromBufRead, ProcError, ProcResult};

const COLUMNS: &str = "# name <active_objs> <num_objs> <objsize> <objperslab> <pagesperslab> : tunables <limit> <batchcount> <sharedfactor> : slabdata <active_slabs> <num_slabs> <sharedavail>";
const STAT_COLUMNS: &str = " : globalstat <listallocs> <maxobjs> <grown> <reaped> <error> <maxfreeable> <nodeallocs> <remotefrees> <alienoverflow> : cpustat <allochit> <allocmiss> <freehit> <freemiss>";

/// A version 2.1 `/proc/slabinfo` snapshot, including optional legacy SLAB
/// statistics. Other format versions are rejected explicitly.
///
/// Implements procfs's parsing traits everywhere and `Current` on Linux. Live
/// access commonly requires root; permission errors are returned unchanged.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct SlabInfo {
    /// Cache rows in source order. A complete header without rows is valid.
    pub caches: Vec<SlabCache>,
}

/// Inventory and configuration of one slab cache. Values are preserved as
/// reported; observations of different fields need not form an atomic view.
///
/// One cache row in `/proc/slabinfo`, parsed by [`SlabInfo`]. Object sizes
/// are bytes, geometry uses objects/slabs/base pages, and optional statistics
/// retain their allocator-specific meanings.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct SlabCache {
    /// Kernel cache name, preserved verbatim.
    pub name: String,
    /// Kernel-reported active objects (`active_objs`), which can be approximate.
    pub active_objects: u64,
    /// Total objects (`num_objs`).
    pub objects: u64,
    /// Reported object size (`objsize`), in bytes. This can include allocator
    /// padding or overhead; it is not necessarily the requested payload size.
    pub object_size_bytes: u64,
    /// Reported objects per slab (`objperslab`).
    pub objects_per_slab: u64,
    /// Reported base pages per slab (`pagesperslab`). Do not assume a 4 KiB page.
    /// Mixed slab allocation orders can make derived resident-byte totals inexact.
    pub pages_per_slab: u64,
    /// Per-CPU object caching limit (`tunables/limit`); normally zero under SLUB.
    pub limit: u64,
    /// Per-CPU transfer batch count (`tunables/batchcount`).
    pub batch_count: u64,
    /// Shared cache factor (`tunables/sharedfactor`), in native kernel units.
    pub shared_factor: u64,
    /// Active slabs (`slabdata/active_slabs`).
    pub active_slabs: u64,
    /// Total slabs (`slabdata/num_slabs`).
    pub slabs: u64,
    /// Available objects in shared arrays (`slabdata/sharedavail`).
    pub shared_available_objects: u64,
    /// Optional legacy SLAB global statistics, present with the statistics format.
    pub global_stats: Option<SlabGlobalStats>,
    /// Optional legacy SLAB hit/miss counters, aggregated across CPUs for this
    /// cache. The file does not provide individual CPU IDs.
    pub cpu_stats: Option<SlabCpuStats>,
}

/// Optional `globalstat` values from the legacy SLAB allocator. All are
/// cumulative events except the two maximum-value gauges.
///
/// Parsed from a cache row's `globalstat` section in `/proc/slabinfo` when
/// legacy allocator statistics are present. Maxima are gauges; other fields
/// are cumulative counters for that cache.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct SlabGlobalStats {
    /// `listallocs` counter.
    pub list_allocations: u64,
    /// `maxobjs` high-water gauge.
    pub max_objects: u64,
    /// `grown` counter.
    pub grown: u64,
    /// `reaped` counter.
    pub reaped: u64,
    /// `error` counter.
    pub errors: u64,
    /// `maxfreeable` high-water gauge of freeable slabs.
    pub max_freeable: u64,
    /// `nodeallocs` counter.
    pub node_allocations: u64,
    /// `remotefrees` counter.
    pub remote_frees: u64,
    /// `alienoverflow` counter.
    pub alien_overflow: u64,
}

/// Optional `cpustat` counters from the legacy SLAB allocator.
///
/// Parsed from a cache row's `cpustat` section in `/proc/slabinfo`. These are
/// cache-wide aggregated CPU hit/miss counters, not individual CPU memory totals.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct SlabCpuStats {
    /// `allochit` counter.
    pub allocation_hits: u64,
    /// `allocmiss` counter.
    pub allocation_misses: u64,
    /// `freehit` counter.
    pub free_hits: u64,
    /// `freemiss` counter.
    pub free_misses: u64,
}

fn invalid(line: usize, reason: impl std::fmt::Display) -> ProcError {
    ProcError::Other(format!("slabinfo line {line}: {reason}"))
}

fn unsigned(raw: &str, line: usize) -> ProcResult<u64> {
    if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid(line, format!("invalid unsigned value: {raw:?}")));
    }
    raw.parse().map_err(|_| invalid(line, "value exceeds u64"))
}

impl FromBufRead for SlabInfo {
    fn from_buf_read<R: BufRead>(reader: R) -> ProcResult<Self> {
        let mut statistics = None;
        let mut columns = false;
        let mut caches = Vec::new();
        let mut names = BTreeSet::new();
        for (offset, line) in reader.lines().enumerate() {
            let line = line?;
            let line_number = offset + 1;
            let fields: Vec<_> = line.split_ascii_whitespace().collect();
            if fields.is_empty() {
                continue;
            }
            let Some(stats) = statistics else {
                statistics = Some(match fields.as_slice() {
                    ["slabinfo", "-", "version:", "2.1"] => false,
                    ["slabinfo", "-", "version:", "2.1", "(statistics)"] => true,
                    _ => {
                        return Err(invalid(line_number, "expected slabinfo version 2.1, optionally (statistics)"));
                    }
                });
                continue;
            };
            if !columns {
                let expected = if stats { format!("{COLUMNS}{STAT_COLUMNS}") } else { COLUMNS.into() };
                if fields != expected.split_ascii_whitespace().collect::<Vec<_>>() {
                    return Err(invalid(line_number, "unexpected or missing column header"));
                }
                columns = true;
                continue;
            }
            let expected = if stats { 33 } else { 16 };
            if fields.len() != expected || fields[6] != ":" || fields[7] != "tunables" || fields[11] != ":" || fields[12] != "slabdata" {
                return Err(invalid(line_number, format!("expected {expected} columns with tunables and slabdata sections")));
            }
            if stats && (fields[16] != ":" || fields[17] != "globalstat" || fields[27] != ":" || fields[28] != "cpustat") {
                return Err(invalid(line_number, "expected globalstat and cpustat sections"));
            }
            if !names.insert(fields[0].to_owned()) {
                return Err(invalid(line_number, "duplicate cache name"));
            }
            let value = |index| unsigned(fields[index], line_number);
            caches.push(SlabCache {
                name: fields[0].into(),
                active_objects: value(1)?,
                objects: value(2)?,
                object_size_bytes: value(3)?,
                objects_per_slab: value(4)?,
                pages_per_slab: value(5)?,
                limit: value(8)?,
                batch_count: value(9)?,
                shared_factor: value(10)?,
                active_slabs: value(13)?,
                slabs: value(14)?,
                shared_available_objects: value(15)?,
                global_stats: if stats {
                    Some(SlabGlobalStats {
                        list_allocations: value(18)?,
                        max_objects: value(19)?,
                        grown: value(20)?,
                        reaped: value(21)?,
                        errors: value(22)?,
                        max_freeable: value(23)?,
                        node_allocations: value(24)?,
                        remote_frees: value(25)?,
                        alien_overflow: value(26)?,
                    })
                } else {
                    None
                },
                cpu_stats: if stats {
                    Some(SlabCpuStats {
                        allocation_hits: value(29)?,
                        allocation_misses: value(30)?,
                        free_hits: value(31)?,
                        free_misses: value(32)?,
                    })
                } else {
                    None
                },
            });
        }
        if !columns {
            return Err(ProcError::Other("slabinfo: missing version or column header".into()));
        }
        Ok(Self { caches })
    }
}

#[cfg(target_os = "linux")]
impl procfs::Current for SlabInfo {
    const PATH: &'static str = "/proc/slabinfo";
}
