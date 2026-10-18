use std::collections::BTreeMap;
use std::path::Path;

use crate::files::{entries, numbered};
use crate::{MemoryFiles, MemoryReadError};

/// Host NUMA inventory, per-node accounting and memory-tier membership.
///
/// Node IDs can be sparse. Counters, distances, CPU/node lists and `kB` units
/// remain exactly as exposed by sysfs; node totals must not be added to host
/// totals. This view does not infer a process's allowed nodes or effective budget.
///
/// Reads inventory files under `/sys/devices/system/node`, per-node
/// `node*/{meminfo,numastat,vmstat,cpulist,cpumap,distance}` and tier membership
/// from `/sys/devices/virtual/memory_tiering/memory_tier*/nodelist`.
/// Global weighted-interleave policy comes from
/// `/sys/kernel/mm/mempolicy/weighted_interleave/{nodeN,auto}`.
/// [`Self::from_sys_root`] substitutes an explicit sysfs/captured root.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct NumaStats {
    /// Online/possible nodes and nodes with CPU or memory resources.
    pub inventory: MemoryFiles,
    /// `meminfo`, `numastat`, `vmstat`, `cpulist`, `cpumap` and `distance` per node.
    pub nodes: BTreeMap<u32, MemoryFiles>,
    /// Tier ID to native `nodelist`; a lower ID represents a faster tier.
    pub tiers: BTreeMap<u32, MemoryFiles>,
    /// Global relative node weights (integers 1..=255, Linux 6.9+) and optional
    /// `auto` boolean (6.16+), keyed by `nodeN`/`auto` below
    /// `/sys/kernel/mm/mempolicy/weighted_interleave`. These affect new
    /// MPOL_WEIGHTED_INTERLEAVE allocations, not existing placement or the
    /// collecting process's policy. An absent `auto` key means unknown mode.
    pub weighted_interleave: MemoryFiles,
    /// Directory discovery failures, keyed by relative sysfs path.
    pub unavailable: BTreeMap<String, MemoryReadError>,
}

impl NumaStats {
    /// Read the running host's sysfs NUMA information synchronously.
    pub fn current() -> Self {
        Self::from_sys_root("/sys")
    }

    /// Read an explicit sysfs mount or captured tree; never falls back to `/sys`.
    pub fn from_sys_root(root: impl AsRef<Path>) -> Self {
        let root = root.as_ref();
        let mut result = Self {
            inventory: MemoryFiles::directory(&root.join("devices/system/node"), |name| {
                matches!(name, "online" | "possible" | "has_cpu" | "has_memory" | "has_normal_memory" | "has_high_memory")
            }),
            ..Self::default()
        };
        for (relative, prefix, nodes) in [("devices/system/node", "node", true), ("devices/virtual/memory_tiering", "memory_tier", false)] {
            match entries(&root.join(relative)) {
                Ok(entries) => {
                    for (name, path) in entries {
                        let Some(id) = numbered(&name, prefix) else { continue };
                        if !std::fs::symlink_metadata(&path).is_ok_and(|m| m.is_dir()) {
                            continue;
                        }
                        let files = MemoryFiles::from_names(
                            path,
                            if nodes {
                                &["meminfo", "numastat", "vmstat", "cpulist", "cpumap", "distance"]
                            } else {
                                &["nodelist"]
                            },
                        );
                        if nodes {
                            result.nodes.insert(id, files);
                        } else {
                            result.tiers.insert(id, files);
                        }
                    }
                }
                Err(error) => {
                    result.unavailable.insert(relative.into(), error.into());
                }
            }
        }
        result.weighted_interleave = MemoryFiles::directory(&root.join("kernel/mm/mempolicy/weighted_interleave"), |name| {
            name == "auto" || numbered(name, "node").is_some()
        });
        result
    }
}
