use crate::structured::map;
use crate::{MemoryFiles, MemoryReadError, ParsedMemoryValue};
use std::collections::BTreeSet;
use std::path::Path;

/// Multi-Gen LRU runtime policy and existing generation statistics.
///
/// Reads `/sys/kernel/mm/lru_gen/{enabled,min_ttl_ms}` for policy and
/// `/sys/kernel/debug/{lru_gen,lru_gen_full}` for existing statistics. Basic
/// histograms come from `lru_gen`; full records preserve the reviewed 6.12/7.2
/// debugfs layout and its availability/history markers.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct LruStats {
    /// Stable sysfs `enabled` and `min_ttl_ms` state.
    pub policy: MemoryFiles,
    /// `parsed["lru_gen_full"]` lists generation records with cgroup ID/path,
    /// node, sequence, age (ms), page counts, four tiers and optional MM stats.
    /// Page-count `expired` flags preserve the kernel's `x` marker. Tier columns
    /// for anonymous/file memory retain `r/e/p` (refaulted/evicted/protected),
    /// `R/T` (moving averages), or `x` (unavailable, with no numeric `value`).
    /// MM `t/y/f/a` (or uppercase) count leaf total/young and nonleaf found/added;
    /// their case is preserved. Values are native counts, never bytes.
    /// Limits: 4096 cgroup/node scopes, four generations/scope and 8 KiB/line,
    /// plus the source/shared I/O limits. Read/parse failures remain per-file.
    pub debug: MemoryFiles,
    /// Parsed basic histograms; no generation aging or reclaim is requested.
    pub histograms: Option<Vec<LruHistogram>>,
}

/// One memory cgroup and node's non-cumulative page-age histogram.
///
/// Parsed from a `memcg`/`node` section of `/sys/kernel/debug/lru_gen`.
/// Its cgroup identity is the one printed by that file, not necessarily the
/// collecting process's current cgroup or a `kpagecgroup` inode number.
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
///
/// One generation row of `/sys/kernel/debug/lru_gen`, within [`LruHistogram`];
/// the four columns are sequence, age in milliseconds, anonymous pages and file pages.
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
                    cgroup = Some(memcg(line)?);
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

pub(crate) fn full_histograms(text: &str) -> Result<ParsedMemoryValue, MemoryReadError> {
    let mut lines = text.lines().filter(|line| !line.trim().is_empty()).peekable();
    let mut cgroup = None;
    let mut node = None;
    let mut scopes = BTreeSet::new();
    let mut sequences = BTreeSet::new();
    let mut result = crate::MemoryList::default();
    while let Some(line) = lines.next() {
        if line.len() > 8192 {
            return Err(full_invalid());
        }
        match line.split_ascii_whitespace().next() {
            Some("memcg") => {
                if cgroup.is_some() && (node.is_none() || sequences.is_empty()) {
                    return Err(full_invalid());
                }
                cgroup = Some(memcg(line)?);
                node = None;
                sequences.clear();
            }
            Some("node") => {
                if node.is_some() && sequences.is_empty() {
                    return Err(full_invalid());
                }
                let fields = full_fields(line, 2)?;
                let (id, _) = cgroup.as_ref().ok_or_else(full_invalid)?;
                let id_node = u32::try_from(number(fields[1])?).map_err(|_| full_invalid())?;
                if scopes.len() == 4096 || !scopes.insert((*id, id_node)) {
                    return Err(full_invalid());
                }
                node = Some(id_node);
                sequences.clear();
            }
            _ => {
                let (id, path) = cgroup.as_ref().ok_or_else(full_invalid)?;
                let node = node.ok_or_else(full_invalid)?;
                let fields = full_fields(line, 4)?;
                let sequence = number(fields[0])?;
                if sequences.len() == 4 || !sequences.insert(sequence) {
                    return Err(full_invalid());
                }
                let mut tiers = crate::MemoryList::default();
                for tier in 0..4 {
                    let row = full_fields(lines.next().ok_or_else(full_invalid)?, 7)?;
                    if number(row[0])? != tier {
                        return Err(full_invalid());
                    }
                    tiers.try_push(map([
                        ("tier", ParsedMemoryValue::Unsigned(tier)),
                        ("anonymous", tagged_columns(&row[1..4], &["rep", "RTx", "xxx"])?),
                        ("file", tagged_columns(&row[4..7], &["rep", "RTx", "xxx"])?),
                    ])?)?;
                }
                let mut record = crate::MemoryMap::try_from_iter([
                    ("cgroup_id", ParsedMemoryValue::Unsigned(*id)),
                    ("cgroup_path", ParsedMemoryValue::try_text(path.clone())?),
                    ("node", ParsedMemoryValue::Unsigned(u64::from(node))),
                    ("sequence", ParsedMemoryValue::Unsigned(sequence)),
                    ("age_ms", ParsedMemoryValue::Unsigned(number(fields[1])?)),
                    ("anonymous_pages", full_pages(fields[2])?),
                    ("file_pages", full_pages(fields[3])?),
                    ("tiers", ParsedMemoryValue::List(tiers)),
                ])?;
                let has_mm = lines
                    .peek()
                    .and_then(|line| line.split_ascii_whitespace().next())
                    .and_then(|field| field.as_bytes().last())
                    .is_some_and(|tag| matches!(tag, b't' | b'T' | b'x'));
                if has_mm {
                    let row = full_fields(lines.next().ok_or_else(full_invalid)?, 4)?;
                    record.try_insert("mm_stats", tagged_columns(&row, &["tyfa", "TYFA", "xxxx"])?)?;
                }
                result.try_push(ParsedMemoryValue::Map(record))?;
            }
        }
    }
    if cgroup.is_some() && (node.is_none() || sequences.is_empty()) {
        return Err(full_invalid());
    }
    Ok(ParsedMemoryValue::List(result))
}

fn memcg(line: &str) -> Result<(u64, String), MemoryReadError> {
    let (fields, path) = crate::process_values::prefix(line, 2)?;
    if fields[0] != "memcg" {
        return Err(invalid());
    }
    // CONFIG_MEMCG=n prints an empty path; preserve spaces inside real paths.
    Ok((number(fields[1])?, path.into()))
}

fn full_fields(line: &str, count: usize) -> Result<crate::MemoryList<&str>, MemoryReadError> {
    if line.len() > 8192 {
        return Err(full_invalid());
    }
    let mut fields = crate::MemoryList::scratch(crate::HeapBudget::current());
    for field in line.split_ascii_whitespace().take(count + 1) {
        fields.try_push(field)?;
    }
    if fields.len() != count {
        return Err(full_invalid());
    }
    Ok(fields)
}

fn full_pages(text: &str) -> Result<ParsedMemoryValue, MemoryReadError> {
    let value = text.strip_suffix('x').unwrap_or(text);
    map([
        ("value", ParsedMemoryValue::Unsigned(number(value)?)),
        ("expired", ParsedMemoryValue::Bool(value.len() != text.len())),
    ])
}

fn tagged_columns(fields: &[&str], layouts: &[&str]) -> Result<ParsedMemoryValue, MemoryReadError> {
    let mut tags = String::new();
    let mut values = crate::MemoryList::default();
    for field in fields {
        let split = field.len().checked_sub(1).ok_or_else(full_invalid)?;
        let (value, tag) = field.split_at_checked(split).ok_or_else(full_invalid)?;
        let value = number(value)?;
        tags.push_str(tag);
        let mut column = crate::MemoryMap::try_from_iter([("tag", ParsedMemoryValue::try_text(tag)?)])?;
        if tag == "x" {
            if value != 0 {
                return Err(full_invalid());
            }
        } else {
            column.try_insert("value", ParsedMemoryValue::Unsigned(value))?;
        }
        values.try_push(ParsedMemoryValue::Map(column))?;
    }
    if !layouts.contains(&tags.as_str()) {
        return Err(full_invalid());
    }
    Ok(ParsedMemoryValue::List(values))
}

fn full_invalid() -> MemoryReadError {
    MemoryReadError::read_failed("malformed or unsupported full MGLRU histogram")
}

fn invalid() -> MemoryReadError {
    MemoryReadError::read_failed("malformed basic MGLRU histogram")
}
fn number(value: &str) -> Result<u64, MemoryReadError> {
    if !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid());
    }
    value.parse().map_err(|_| invalid())
}
