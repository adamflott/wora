use crate::files::{entries, numbered, resolve_directory};
use crate::{MemoryFiles, MemoryReadError};
use std::collections::BTreeMap;
use std::path::Path;

/// Memory block inventory, hardware error accounting and reserved CMA areas.
///
/// Controller/DIMM counters overlap; no totals are synthesized. Presence depends
/// on the architecture, hardware/driver support, kernel configuration and mounts.
///
/// Sources are `/sys/devices/system/memory/{block_size_bytes,auto_online_blocks,
/// crash_hotplug}`, per-block `memory*/{state,online,removable,phys_index,valid_zones}`,
/// selected files under `/sys/devices/system/edac/mc/mc*` and its DIMM/rank/csrow
/// children, node `memory_failure/*` counters and `/sys/kernel/debug/cma/*`.
/// [`Self::from_sys_root`] substitutes an explicit sysfs root; no mount is created.
/// Native sizes/counters/PFNs/bitmaps remain distinct. `block_size_bytes` is parsed
/// from its hexadecimal source, not inferred from page size.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct MemoryHardwareStats {
    /// Memory block size converted from the kernel's hexadecimal byte value.
    pub block_size_bytes: Option<u64>,
    /// Memory block size and automatic-online policy, in native formats.
    pub memory: MemoryFiles,
    /// Sparse memory-block IDs, with online state, removability and zones.
    pub blocks: BTreeMap<u32, MemoryFiles>,
    /// EDAC controller/DIMM/rank/csrow relative paths to native information files.
    pub edac: BTreeMap<String, MemoryFiles>,
    /// Per-NUMA-node poisoned-page recovery counters.
    pub poisoned: BTreeMap<u32, MemoryFiles>,
    /// CMA area/range relative paths to PFN/count/bitmap information.
    pub cma: BTreeMap<String, MemoryFiles>,
    /// Discovery errors keyed by relative source path.
    pub unavailable: BTreeMap<String, MemoryReadError>,
}

impl MemoryHardwareStats {
    /// Observe existing host state. No memory is onlined, offlined or poisoned.
    pub fn current() -> Self {
        Self::from_sys_root("/sys")
    }
    /// Read sysfs and existing `kernel/debug/cma`, with bounded enumeration.
    /// Never resets EDAC counters, scrubs memory, or allocates/releases CMA pages.
    pub fn from_sys_root(root: impl AsRef<Path>) -> Self {
        let root = root.as_ref();
        let mut result = Self {
            memory: MemoryFiles::from_names(root.join("devices/system/memory"), &["block_size_bytes", "auto_online_blocks", "crash_hotplug"]),
            ..Self::default()
        };
        result.block_size_bytes = result.memory.parsed.get("block_size_bytes").and_then(crate::ParsedMemoryValue::as_u64);
        for (relative, prefix, blocks) in [("devices/system/memory", "memory", true), ("devices/system/node", "node", false)] {
            match entries(&root.join(relative)) {
                Ok(entries) => {
                    for (name, path) in entries {
                        let Some(id) = numbered(&name, prefix) else { continue };
                        match resolve_directory(root, &path) {
                            Ok(path) => {
                                if blocks {
                                    result.blocks.insert(
                                        id,
                                        MemoryFiles::from_names(path, &["state", "online", "removable", "phys_index", "valid_zones"]),
                                    );
                                } else {
                                    result.poisoned.insert(
                                        id,
                                        MemoryFiles::from_names(path.join("memory_failure"), &["total", "ignored", "failed", "delayed", "recovered"]),
                                    );
                                }
                            }
                            Err(error) => {
                                result.unavailable.insert(format!("{relative}/{name}"), error.into());
                            }
                        }
                    }
                }
                Err(error) => {
                    result.unavailable.insert(relative.into(), error.into());
                }
            }
        }
        result.read_edac(root);
        result.read_cma(root);
        result
    }

    fn read_edac(&mut self, root: &Path) {
        let relative = "devices/system/edac/mc";
        let controllers = match entries(&root.join(relative)) {
            Ok(v) => v,
            Err(e) => {
                self.unavailable.insert(relative.into(), e.into());
                return;
            }
        };
        for (name, path) in controllers {
            if numbered(&name, "mc").is_none() {
                continue;
            }
            let path = match resolve_directory(root, &path) {
                Ok(v) => v,
                Err(e) => {
                    self.unavailable.insert(format!("{relative}/{name}"), e.into());
                    continue;
                }
            };
            self.edac.insert(name.clone(), MemoryFiles::directory(&path, edac_file));
            match entries(&path) {
                Ok(children) => {
                    for (child, child_path) in children {
                        if !["dimm", "rank", "csrow"].iter().any(|prefix| numbered(&child, prefix).is_some()) {
                            continue;
                        }
                        match resolve_directory(root, &child_path) {
                            Ok(path) => {
                                self.edac.insert(format!("{name}/{child}"), MemoryFiles::directory(&path, edac_file));
                            }
                            Err(e) => {
                                self.unavailable.insert(format!("{relative}/{name}/{child}"), e.into());
                            }
                        }
                    }
                }
                Err(e) => {
                    self.unavailable.insert(format!("{relative}/{name}"), e.into());
                }
            }
        }
    }

    fn read_cma(&mut self, root: &Path) {
        let relative = "kernel/debug/cma";
        let areas = match entries(&root.join(relative)) {
            Ok(v) => v,
            Err(e) => {
                self.unavailable.insert(relative.into(), e.into());
                return;
            }
        };
        for (area, path) in areas {
            let path = match resolve_directory(root, &path) {
                Ok(v) => v,
                Err(e) => {
                    self.unavailable.insert(format!("{relative}/{area}"), e.into());
                    continue;
                }
            };
            self.cma
                .insert(area.clone(), MemoryFiles::from_names(&path, &["base_pfn", "count", "order_per_bit", "bitmap"]));
            // Multiple contiguous ranges are present only on newer kernels.
            if path.join("ranges").exists() {
                match entries(&path.join("ranges")) {
                    Ok(ranges) => {
                        for (name, path) in ranges {
                            if numbered(&name, "").is_none() {
                                continue;
                            }
                            match resolve_directory(root, &path) {
                                Ok(path) => {
                                    self.cma
                                        .insert(format!("{area}/ranges/{name}"), MemoryFiles::from_names(path, &["base_pfn", "bitmap"]));
                                }
                                Err(e) => {
                                    self.unavailable.insert(format!("{relative}/{area}/ranges/{name}"), e.into());
                                }
                            }
                        }
                    }
                    Err(e) => {
                        self.unavailable.insert(format!("{relative}/{area}/ranges"), e.into());
                    }
                }
            }
        }
    }
}

fn edac_file(name: &str) -> bool {
    matches!(
        name,
        "ce_count"
            | "ue_count"
            | "ce_noinfo_count"
            | "ue_noinfo_count"
            | "mc_name"
            | "size_mb"
            | "seconds_since_reset"
            | "max_location"
            | "dimm_ce_count"
            | "dimm_ue_count"
            | "dimm_label"
            | "dimm_location"
            | "dimm_mem_type"
            | "dimm_dev_type"
            | "dimm_edac_mode"
            | "size"
            | "mem_type"
            | "dev_type"
            | "edac_mode"
    ) || name.ends_with("_ce_count")
        || name.ends_with("_ue_count")
        || name.ends_with("_dimm_label")
}
