use crate::{MemoryFiles, MemoryReadError};
use std::collections::BTreeSet;
use std::path::Path;

/// Multi-Gen LRU runtime policy and existing generation statistics.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct LruStats {
    /// Stable sysfs `enabled` and `min_ttl_ms` state.
    pub policy: MemoryFiles,
    /// Per-file errors for debugfs `lru_gen` and `lru_gen_full`.
    pub debug: MemoryFiles,
    /// Parsed basic histograms; no generation aging or reclaim is requested.
    pub histograms: Option<Vec<LruHistogram>>,
}

/// One memory cgroup and node's non-cumulative page-age histogram.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct LruHistogram {
    /// Kernel memory-cgroup ID (not a PID).
    pub cgroup_id: u64,
    /// Native cgroup path printed by debugfs.
    pub cgroup_path: String,
    /// NUMA node ID.
    pub node: u32,
    /// Native generation sequence, age, anonymous pages and file pages.
    pub generations: Vec<LruGeneration>,
}

/// One generation; page counts remain pages, not bytes or cumulative counts.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct LruGeneration {
    /// Kernel generation sequence number.
    pub sequence: u64,
    /// Estimated age in milliseconds.
    pub age_ms: u64,
    /// Anonymous base-page count.
    pub anonymous_pages: u64,
    /// File-backed base-page count.
    pub file_pages: u64,
}

impl LruStats {
    /// Observe the host's existing MGLRU state; requires kernel support.
    pub fn current() -> Self {
        Self::from_sys_root("/sys")
    }
    /// Read supplied sysfs/debugfs captures; no enablement or commands are written.
    pub fn from_sys_root(root: impl AsRef<Path>) -> Self {
        let root = root.as_ref();
        let policy = MemoryFiles::from_names(root.join("kernel/mm/lru_gen"), &["enabled", "min_ttl_ms"]);
        let mut debug = MemoryFiles::from_names(root.join("kernel/debug"), &["lru_gen_full"]);
        let histograms = debug.read_with(&root.join("kernel/debug"), "lru_gen", 4 * 1024 * 1024, LruHistogram::parse);
        Self { policy, debug, histograms }
    }
}

impl LruHistogram {
    /// Parse the basic MGLRU debugfs format, failing atomically on malformed rows.
    pub fn parse(text: &str) -> Result<Vec<Self>, MemoryReadError> {
        let mut result: Vec<Self> = Vec::new();
        let mut cgroup: Option<(u64, String)> = None;
        let mut current = None;
        let mut nodes = BTreeSet::new();
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let fields: Vec<_> = line.split_ascii_whitespace().collect();
            match fields[0] {
                "memcg" => {
                    if fields.len() < 3 {
                        return Err(invalid());
                    }
                    cgroup = Some((number(fields[1])?, fields[2..].join(" ")));
                    current = None;
                }
                "node" => {
                    if fields.len() != 2 {
                        return Err(invalid());
                    }
                    let (id, path) = cgroup.as_ref().ok_or_else(invalid)?;
                    let node = u32::try_from(number(fields[1])?).map_err(|_| invalid())?;
                    if !nodes.insert((*id, node)) {
                        return Err(invalid());
                    }
                    result.push(Self {
                        cgroup_id: *id,
                        cgroup_path: path.clone(),
                        node,
                        generations: Vec::new(),
                    });
                    current = Some(result.len() - 1);
                }
                _ => {
                    if fields.len() != 4 {
                        return Err(invalid());
                    }
                    let index = current.ok_or_else(invalid)?;
                    let generation = LruGeneration {
                        sequence: number(fields[0])?,
                        age_ms: number(fields[1])?,
                        anonymous_pages: number(fields[2])?,
                        file_pages: number(fields[3])?,
                    };
                    if result[index].generations.iter().any(|g| g.sequence == generation.sequence) {
                        return Err(invalid());
                    }
                    result[index].generations.push(generation);
                }
            }
        }
        Ok(result)
    }
}

fn invalid() -> MemoryReadError {
    MemoryReadError::ReadFailed("malformed basic MGLRU histogram".into())
}
fn number(value: &str) -> Result<u64, MemoryReadError> {
    if !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid());
    }
    value.parse().map_err(|_| invalid())
}
