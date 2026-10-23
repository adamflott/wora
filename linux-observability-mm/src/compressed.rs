use std::collections::BTreeMap;
use std::path::Path;

use crate::files::{entries, numbered, resolve_directory};
use crate::{MemoryFiles, MemoryReadError};

/// Host compressed-swap configuration and accounting, separate from swap usage.
///
/// Reads selected files in `/sys/module/zswap/parameters`,
/// `/sys/kernel/debug/zswap` and `/sys/block/zram*` (resolving device links
/// within the supplied sysfs root). Source byte/page/counter units are retained.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct CompressedMemoryStats {
    /// Zswap runtime policy from module parameters.
    pub zswap_parameters: MemoryFiles,
    /// Selected counters from `/sys/kernel/debug/zswap`; commonly restricted.
    /// `reject_compress_fail` and `decompress_fail` count failures;
    /// `stored_incompressible_pages` counts pages, not bytes. Missing counters
    /// remain per-file errors on kernels that do not expose them.
    pub zswap: MemoryFiles,
    /// Zram devices keyed by sparse device ID (including devices not used as swap).
    pub zram: BTreeMap<u32, ZramDevice>,
    /// Device discovery or resolution errors.
    pub unavailable: BTreeMap<String, MemoryReadError>,
}

/// One zram device's parsed configuration and I/O/statistics tables.
///
/// Files are relative to one resolved `/sys/block/zram<id>` device directory.
/// `memory` comes from its `mm_stat`; `files` includes selected policy, size,
/// `io_stat`, `bd_stat` and block `stat` files. This is per-device accounting.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct ZramDevice {
    /// Structured files, including `io_stat` and `bd_stat`.
    pub files: MemoryFiles,
    /// Named `mm_stat` fields; absent if missing or malformed.
    pub memory: Option<ZramMemoryStat>,
}

/// Zram `mm_stat` values. Sizes are bytes; `*_pages` and `pages_compacted` are counts.
///
/// Parsed from `/sys/block/zram<id>/mm_stat` by [`CompressedMemoryStats`].
/// `fields` names the kernel's positional columns; `extra` retains unknown
/// trailing columns without assigning units.
#[derive(Clone, Default, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct ZramMemoryStat {
    /// Known positional fields, preserving all unsigned integer precision.
    pub fields: BTreeMap<String, u64>,
    /// Future trailing fields whose semantics are not known to this reader.
    pub extra: Vec<u64>,
}

impl ZramMemoryStat {
    /// Parse at least the original seven fields; retain optional newer fields.
    pub fn parse(text: &str) -> Result<Self, MemoryReadError> {
        const NAMES: &[&str] = &[
            "orig_data_size",
            "compr_data_size",
            "mem_used_total",
            "mem_limit",
            "mem_used_max",
            "same_pages",
            "pages_compacted",
            "huge_pages",
            "huge_pages_since",
        ];
        let values = text
            .split_ascii_whitespace()
            .map(|v| {
                if v.is_empty() || !v.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(MemoryReadError::read_failed("invalid mm_stat number"));
                }
                v.parse::<u64>().map_err(|e| MemoryReadError::read_failed(e.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if values.len() < 7 {
            return Err(MemoryReadError::read_failed("mm_stat requires at least seven fields"));
        }
        Ok(Self {
            fields: NAMES.iter().zip(&values).map(|(name, value)| ((*name).into(), *value)).collect(),
            extra: values.into_iter().skip(NAMES.len()).collect(),
        })
    }
}

impl CompressedMemoryStats {
    /// Read `/sys`, including existing zswap debugfs files. Never mounts debugfs.
    pub fn current() -> Self {
        Self::from_sys_root("/sys")
    }

    /// Read a supplied sysfs tree, with debugfs at `kernel/debug` inside it.
    ///
    /// No device is initialized, reset, compacted, recompressed or written back.
    /// Zswap counters are kernel-dependent; failures remain per-file errors.
    pub fn from_sys_root(root: impl AsRef<Path>) -> Self {
        let root = root.as_ref();
        let mut result = Self {
            zswap_parameters: MemoryFiles::from_names(
                root.join("module/zswap/parameters"),
                &[
                    "enabled",
                    "compressor",
                    "zpool",
                    "max_pool_percent",
                    "accept_threshold_percent",
                    "shrinker_enabled",
                ],
            ),
            zswap: MemoryFiles::from_names(
                root.join("kernel/debug/zswap"),
                &[
                    "pool_total_size",
                    "stored_pages",
                    "stored_incompressible_pages",
                    "same_filled_pages",
                    "pool_limit_hit",
                    "written_back_pages",
                    "reject_compress_poor",
                    "reject_compress_fail",
                    "decompress_fail",
                    "reject_alloc_fail",
                    "reject_kmemcache_fail",
                    "reject_reclaim_fail",
                    "duplicate_entry",
                ],
            ),
            ..Self::default()
        };
        match entries(&root.join("block")) {
            Ok(entries) => {
                for (name, path) in entries {
                    let Some(id) = numbered(&name, "zram") else { continue };
                    match resolve_directory(root, &path) {
                        Ok(path) => {
                            let mut files = MemoryFiles::from_names(
                                &path,
                                &[
                                    "io_stat",
                                    "bd_stat",
                                    "stat",
                                    "disksize",
                                    "comp_algorithm",
                                    "recomp_algorithm",
                                    "backing_dev",
                                    "max_comp_streams",
                                    "mem_limit",
                                    "writeback_limit",
                                    "writeback_limit_enable",
                                ],
                            );
                            let memory = files.read_with(&path, "mm_stat", 4 * 1024 * 1024, ZramMemoryStat::parse);
                            result.zram.insert(id, ZramDevice { files, memory });
                        }
                        Err(error) => {
                            result.unavailable.insert(name, error.into());
                        }
                    }
                }
            }
            Err(error) => {
                result.unavailable.insert("block".into(), error.into());
            }
        }
        result
    }
}
