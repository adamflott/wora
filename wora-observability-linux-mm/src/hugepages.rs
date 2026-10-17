use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use crate::{MemoryReadError, MemorySkipReason};

/// THP policy/per-size statistics and global/per-node HugeTLB pools from sysfs.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct HugePageStats {
    /// `/sys/kernel/mm/transparent_hugepage`, including khugepaged and per-size stats.
    pub transparent: HugePageFiles,
    /// `/sys/kernel/mm/hugepages`, with pools keyed by their kernel size directory.
    pub hugetlb: HugePageFiles,
    /// Per-node HugeTLB pools, keyed by sparse NUMA node ID.
    pub nodes: BTreeMap<u32, HugePageFiles>,
    /// Failures enumerating the node hierarchy, keyed by relative sysfs path.
    pub unavailable: BTreeMap<String, MemoryReadError>,
}

/// Files within one THP or HugeTLB sysfs hierarchy.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct HugePageFiles {
    /// Values keyed by relative path, retaining size labels and field names.
    ///
    /// Numeric values retain source units: page counts, byte sizes, milliseconds,
    /// and other tunables are not added together or normalized. Policies expose
    /// their selected value and choices. Raw file contents are not retained.
    pub parsed: BTreeMap<String, crate::ParsedMemoryValue>,
    /// Per-file failures; directory failures use the directory path or `.`.
    pub unavailable: BTreeMap<String, MemoryReadError>,
    /// Entries not opened, including `demote`, write-only files, and symlinks.
    pub skipped: BTreeMap<String, MemorySkipReason>,
}

impl HugePageStats {
    /// Collect synchronously from `/sys`, without opening action controls.
    pub fn current() -> Self {
        Self::from_sys_root("/sys")
    }

    /// Collect from an alternate sysfs mount or captured sysfs directory.
    ///
    /// Reads `kernel/mm/{transparent_hugepage,hugepages}` and each
    /// `devices/system/node/node*/hugepages` tree. Traversal is bounded to size
    /// directories, khugepaged, and per-size statistics; symlinks are skipped.
    pub fn from_sys_root(root: impl AsRef<Path>) -> Self {
        let root = root.as_ref();
        let mut snapshot = Self {
            transparent: HugePageFiles::from_directory(root.join("kernel/mm/transparent_hugepage")),
            hugetlb: HugePageFiles::from_directory(root.join("kernel/mm/hugepages")),
            ..Self::default()
        };
        let nodes = match fs::read_dir(root.join("devices/system/node")) {
            Ok(nodes) => nodes,
            Err(error) => {
                snapshot.unavailable.insert("devices/system/node".into(), error.into());
                return snapshot;
            }
        };
        for entry in nodes {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    snapshot.unavailable.insert("devices/system/node".into(), error.into());
                    continue;
                }
            };
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some(node) = name
                .strip_prefix("node")
                .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
            else {
                continue;
            };
            let node = match node.parse::<u32>() {
                Ok(node) => node,
                Err(error) => {
                    snapshot.unavailable.insert(name.into(), MemoryReadError::ReadFailed(error.to_string()));
                    continue;
                }
            };
            match entry.file_type() {
                Ok(kind) if kind.is_dir() => {
                    snapshot.nodes.insert(node, HugePageFiles::from_directory(entry.path().join("hugepages")));
                }
                Ok(_) => {}
                Err(error) => {
                    snapshot.unavailable.insert(name.into(), error.into());
                }
            }
        }
        snapshot
    }
}

impl HugePageFiles {
    /// Read a THP/HugeTLB hierarchy using the same exclusions as live collection.
    pub fn from_directory(root: impl AsRef<Path>) -> Self {
        let mut snapshot = Self::default();
        snapshot.read_tree(root.as_ref(), Path::new(""), 0);
        snapshot
    }

    fn read_tree(&mut self, root: &Path, relative: &Path, depth: usize) {
        let key = if relative.as_os_str().is_empty() {
            ".".into()
        } else {
            relative.to_string_lossy().into_owned()
        };
        let entries = match fs::read_dir(root.join(relative)) {
            Ok(entries) => entries,
            Err(error) => {
                self.unavailable.insert(key, error.into());
                return;
            }
        };
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    self.unavailable.insert(key.clone(), error.into());
                    continue;
                }
            };
            let path = relative.join(entry.file_name());
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let field = path.to_string_lossy().into_owned();
            if name == "demote" {
                self.skipped.insert(field, MemorySkipReason::WriteOnly);
                continue;
            }
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(error) => {
                    self.unavailable.insert(field, error.into());
                    continue;
                }
            };
            if metadata.is_dir() && ((depth == 0 && (name == "khugepaged" || page_size_directory(name))) || (depth == 1 && name == "stats")) {
                self.read_tree(root, &path, depth + 1);
                continue;
            }
            if !metadata.is_file() {
                self.skipped.insert(field, MemorySkipReason::NotRegularFile);
                continue;
            }
            let mode = metadata.permissions().mode();
            if mode & 0o444 == 0 && mode & 0o222 != 0 {
                self.skipped.insert(field, MemorySkipReason::WriteOnly);
                continue;
            }
            let result = fs::read_to_string(entry.path()).map_err(MemoryReadError::from).and_then(|text| {
                crate::parse_memory_file(root.join(&path), &text)?.ok_or_else(|| MemoryReadError::ReadFailed("unsupported hugepage-information layout".into()))
            });
            match result {
                Ok(value) => {
                    self.parsed.insert(field, value);
                }
                Err(error) => {
                    self.unavailable.insert(field, error);
                }
            }
        }
    }
}

fn page_size_directory(name: &str) -> bool {
    name.strip_prefix("hugepages-")
        .and_then(|name| name.strip_suffix("kB"))
        .is_some_and(|size| !size.is_empty() && size.bytes().all(|byte| byte.is_ascii_digit()))
}
