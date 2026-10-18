use crate::files::{entries, resolve_directory};
use crate::{MemoryFiles, MemoryReadError};
use std::collections::BTreeMap;
use std::path::Path;

/// Opt-in modern SLUB cache details, without counting aliases as separate caches.
///
/// Reads selected state/counter files under `/sys/kernel/slab/<cache>`,
/// including native object/slab/node counts, geometry, policies and enabled
/// debugging counters. Aliases are directory links, not additional caches.
/// [`Self::from_sys_root`] substitutes an explicit sysfs root.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct SlubStats {
    /// Canonical cache directory names to native per-cache files.
    pub caches: BTreeMap<String, MemoryFiles>,
    /// Alias name to canonical cache name; no duplicated accounting.
    pub aliases: BTreeMap<String, String>,
    /// Enumeration/resolution failures, keyed by cache name or `.`.
    pub unavailable: BTreeMap<String, MemoryReadError>,
}

impl SlubStats {
    /// Read `/sys/kernel/slab`; debugging counters depend on kernel configuration.
    pub fn current() -> Self {
        Self::from_sys_root("/sys")
    }
    /// Read an explicit sysfs tree. Cache links must stay within `kernel/slab`.
    ///
    /// Only known state/counter files are read. Validation, shrinking, cache
    /// flushing and counter resets are never requested, even with root access.
    pub fn from_sys_root(root: impl AsRef<Path>) -> Self {
        let root = root.as_ref().join("kernel/slab");
        let mut result = Self::default();
        let entries = match entries(&root) {
            Ok(entries) => entries,
            Err(error) => {
                result.unavailable.insert(".".into(), error.into());
                return result;
            }
        };
        for (name, path) in entries {
            let canonical = match resolve_directory(&root, &path) {
                Ok(path) => path,
                Err(error) => {
                    result.unavailable.insert(name, error.into());
                    continue;
                }
            };
            let Some(cache) = canonical.file_name().and_then(|name| name.to_str()).map(str::to_owned) else {
                continue;
            };
            if name != cache {
                result.aliases.insert(name, cache.clone());
            }
            result.caches.entry(cache).or_insert_with(|| {
                MemoryFiles::directory(&canonical, |name| {
                    matches!(
                        name,
                        "aliases"
                            | "align"
                            | "object_size"
                            | "slab_size"
                            | "objs_per_slab"
                            | "order"
                            | "objects"
                            | "objects_partial"
                            | "total_objects"
                            | "slabs"
                            | "partial"
                            | "cpu_slabs"
                            | "cpu_partial"
                            | "slabs_cpu_partial"
                            | "reclaim_account"
                            | "cache_dma"
                            | "hwcache_align"
                            | "destroy_by_rcu"
                            | "ctor"
                            | "usersize"
                            | "red_zone"
                            | "poison"
                            | "sanity_checks"
                            | "store_user"
                            | "trace"
                            | "failslab"
                            | "remote_node_defrag_ratio"
                            | "alloc_calls"
                            | "free_calls"
                            | "alloc_fastpath"
                            | "alloc_slowpath"
                            | "free_fastpath"
                            | "free_slowpath"
                            | "free_frozen"
                            | "free_add_partial"
                            | "free_remove_partial"
                            | "alloc_from_partial"
                            | "alloc_slab"
                            | "alloc_refill"
                            | "free_slab"
                            | "cpuslab_flush"
                            | "deactivate_full"
                            | "deactivate_empty"
                            | "deactivate_to_head"
                            | "deactivate_to_tail"
                            | "deactivate_remote_frees"
                            | "deactivate_bypass"
                            | "order_fallback"
                            | "cmpxchg_double_fail"
                            | "cmpxchg_double_cpu_fail"
                            | "cpu_partial_alloc"
                            | "cpu_partial_free"
                            | "cpu_partial_node"
                            | "cpu_partial_drain"
                    )
                })
            });
        }
        result
    }
}
