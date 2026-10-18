use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use crate::{MemoryFiles, MemoryReadError, ParsedMemoryValue};

/// Opt-in object counts from `/sys/kernel/debug/shrinker/<name-id>/count`.
///
/// Each read invokes kernel `count_objects` callbacks across applicable cgroups
/// and nodes; seq_file retries can repeat them. Request/byte limits do not bound
/// callback count or CPU time within a read. `scan` is never opened. Requires
/// mounted debugfs, kernel support and access to these normally restricted files.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct ShrinkerStats {
    /// Shrinker directory name -> independent `count` records/errors.
    /// Records contain the kernel's `cgroup_id` (inode ID on older kernels)
    /// and `node_counts` in possible-node order. Column positions are not node
    /// IDs when IDs are sparse. Zero ID denotes non-memcg accounting as exposed
    /// by the kernel. Counts are objects, not bytes, and scopes can overlap.
    /// Empty tables mean no nonzero rows were reported. Omitted rows/columns
    /// are not expanded into invented zeroes. Files cap at 4 MiB, 65536 rows and
    /// 4096 node columns, plus shared input/work limits.
    pub counts: BTreeMap<String, MemoryFiles>,
    /// Discovery/admission failures; `.` marks incomplete or invalid selection.
    pub unavailable: BTreeMap<String, MemoryReadError>,
}

impl ShrinkerStats {
    /// Read at most eight shrinker count files from the existing live debugfs.
    pub fn current() -> Self {
        Self::from_debug_root("/sys/kernel/debug")
    }

    /// Read an alternate debugfs tree/capture, with at most eight count requests.
    pub fn from_debug_root(root: impl AsRef<Path>) -> Self {
        Self::from_debug_root_with_read_limit(root, 8)
    }

    /// Limit count requests to 1..=256, in sorted directory-name order.
    /// Discovery visits at most 4096 entries; shared limits can reduce this.
    /// Exhaustion marks the batch incomplete before another file is opened.
    pub fn from_debug_root_with_read_limit(root: impl AsRef<Path>, count_reads: usize) -> Self {
        let mut result = Self::default();
        if !(1..=256).contains(&count_reads) {
            result
                .unavailable
                .insert(".".into(), crate::structured::invalid("shrinker count requests must be 1..=256"));
            return result;
        }
        let entries = match crate::files::entries(&root.as_ref().join("shrinker")) {
            Ok(entries) => entries,
            Err(error) => {
                result.unavailable.insert(".".into(), error.into());
                return result;
            }
        };
        for (name, path) in entries {
            match std::fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.is_dir() => {}
                Ok(_) => continue,
                Err(error) => {
                    result.unavailable.insert(name, error.into());
                    continue;
                }
            }
            if result.counts.len() == count_reads || !crate::budget::file_available() {
                result
                    .unavailable
                    .insert(".".into(), crate::budget::exceeded(crate::CollectionLimit::Files).into());
                break;
            }
            result.counts.insert(name, MemoryFiles::from_names(path, &["count"]));
        }
        result
    }
}

pub(crate) fn counts(text: &str) -> Result<ParsedMemoryValue, MemoryReadError> {
    let mut rows = crate::MemoryList::default();
    let mut seen = BTreeSet::new();
    let mut columns = None;
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        if rows.len() == 65536 {
            return Err(invalid());
        }
        let mut fields = line.split_ascii_whitespace();
        let cgroup = number(fields.next().ok_or_else(invalid)?)?;
        if !seen.insert(cgroup) {
            return Err(invalid());
        }
        let mut counts = crate::MemoryList::default();
        for field in fields {
            if counts.len() == 4096 {
                return Err(invalid());
            }
            counts.try_push(ParsedMemoryValue::Unsigned(number(field)?))?;
        }
        if counts.is_empty() || columns.is_some_and(|columns| columns != counts.len()) {
            return Err(invalid());
        }
        columns = Some(counts.len());
        rows.try_push(crate::structured::map([
            ("cgroup_id", ParsedMemoryValue::Unsigned(cgroup)),
            ("node_counts", ParsedMemoryValue::List(counts)),
        ])?)?;
    }
    Ok(ParsedMemoryValue::List(rows))
}

fn number(text: &str) -> Result<u64, MemoryReadError> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid());
    }
    text.parse().map_err(|_| invalid())
}

fn invalid() -> MemoryReadError {
    crate::structured::invalid("malformed or oversized shrinker count table")
}
