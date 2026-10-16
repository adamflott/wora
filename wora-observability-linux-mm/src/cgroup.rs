use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};

use crate::{FromRead, MemoryPressure, MemoryReadError, MemorySkipReason};

/// Current process's cgroup v2 memory accounting and visible ancestor limits.
///
/// Discovery reads `/proc/self/cgroup` and `/proc/self/mountinfo`, accounting for
/// cgroup2 mount roots and escaped mount paths. Cgroup v1 is not substituted.
/// Only ancestors inside the selected mount are visible; these are local
/// settings, not a computed effective limit or remaining allocation allowance.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct CgroupMemoryStats {
    /// Membership path reported by `/proc/self/cgroup`.
    pub membership: Option<PathBuf>,
    /// Selected cgroup2 mount point in the process's mount namespace.
    pub mount_point: Option<PathBuf>,
    /// Cgroup hierarchy root exposed by that mount.
    pub mount_root: Option<PathBuf>,
    /// Current cgroup's memory files; absent if discovery failed.
    pub current: Option<CgroupMemory>,
    /// Visible parents, nearest first, containing usage and limit settings.
    pub ancestors: Vec<CgroupMemory>,
    /// Discovery failures, keyed by proc file or `cgroup2`.
    pub unavailable: BTreeMap<String, MemoryReadError>,
}

/// Memory files belonging to one explicitly identified cgroup directory.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct CgroupMemory {
    /// Actual directory used for reads.
    pub directory: PathBuf,
    /// Successfully parsed files, keyed by kernel filename.
    ///
    /// Sizes retain bytes, counters retain counts, and pressure retains kernel
    /// percentages/microseconds. `memory.events` and `memory.events.local` stay
    /// separate: the former normally includes descendants, the latter does not.
    pub values: BTreeMap<String, CgroupMemoryValue>,
    /// Per-file read/parse failures; directory failures use `.`.
    pub unavailable: BTreeMap<String, MemoryReadError>,
    /// Action controls and non-regular or write-only entries not opened.
    pub skipped: BTreeMap<String, MemorySkipReason>,
}

/// Native cgroup memory file contents, without converting unlimited limits to zero.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde1", serde(untagged))]
pub enum CgroupMemoryValue {
    /// Scalar count, size in bytes, or boolean represented by the kernel as 0/1.
    Number(u64),
    /// Named counters/gauges, including new `memory.stat` and event fields.
    Counters(BTreeMap<String, u64>),
    /// Memory category -> sparse NUMA node ID -> bytes.
    Numa(BTreeMap<String, BTreeMap<u32, u64>>),
    /// Cgroup-specific memory pressure stall information.
    Pressure(MemoryPressure),
    /// Literal `max`, or an unfamiliar file's native text without trailing newlines.
    Text(String),
}

impl CgroupMemoryStats {
    /// Collect the current process's cgroup v2 memory information synchronously.
    pub fn current() -> Self {
        Self::from_proc_root("/proc")
    }

    /// Discover through `root/self/{cgroup,mountinfo}`.
    ///
    /// Mount points in captured mountinfo are used as written. To read fixtures,
    /// point those mounts at fixture directories. Unsupported or inconsistent
    /// namespace paths produce an explicit failure rather than guessed host data.
    pub fn from_proc_root(root: impl AsRef<Path>) -> Self {
        let mut snapshot = Self::default();
        if let Err((source, error)) = snapshot.collect(root.as_ref()) {
            snapshot.unavailable.insert(source.into(), error);
        }
        snapshot
    }

    fn collect(&mut self, root: &Path) -> Result<(), (&'static str, MemoryReadError)> {
        let membership = fs::read_to_string(root.join("self/cgroup")).map_err(|error| ("self/cgroup", error.into()))?;
        let mut paths = membership.lines().filter_map(|line| line.strip_prefix("0::"));
        let path = paths.next().ok_or_else(|| ("self/cgroup", invalid("no cgroup v2 membership")))?;
        if paths.next().is_some() || path.ends_with(" (deleted)") {
            return Err(("self/cgroup", invalid("ambiguous or deleted cgroup membership")));
        }
        let path = PathBuf::from(path);
        validate_path(&path).map_err(|error| ("self/cgroup", error))?;
        self.membership = Some(path.clone());
        let mountinfo = fs::read_to_string(root.join("self/mountinfo")).map_err(|error| ("self/mountinfo", error.into()))?;
        let mut mounts = Vec::new();
        for line in mountinfo.lines() {
            let Some((fields, filesystem)) = line.split_once(" - ") else { continue };
            if filesystem.split_ascii_whitespace().next() != Some("cgroup2") {
                continue;
            }
            let fields: Vec<_> = fields.split_ascii_whitespace().collect();
            if fields.len() < 6 {
                return Err(("self/mountinfo", invalid("incomplete cgroup2 mount")));
            }
            let mount_root = decode_path(fields[3]).map_err(|error| ("self/mountinfo", error))?;
            let mount_point = decode_path(fields[4]).map_err(|error| ("self/mountinfo", error))?;
            if let Ok(relative) = path.strip_prefix(&mount_root) {
                mounts.push((mount_root.clone(), mount_point.clone(), mount_point.join(relative)));
            }
        }
        // Prefer a mount rooted closest to this cgroup. Ties are deterministic.
        mounts.sort_by(|left, right| {
            right
                .0
                .components()
                .count()
                .cmp(&left.0.components().count())
                .then_with(|| left.1.cmp(&right.1))
        });
        let (mount_root, mount_point, directory) = mounts
            .into_iter()
            .next()
            .ok_or_else(|| ("cgroup2", invalid("no matching visible cgroup2 mount")))?;
        self.mount_point = Some(mount_point.clone());
        self.mount_root = Some(mount_root);
        self.current = Some(CgroupMemory::from_directory(&directory));
        let mut parent = directory;
        while parent != mount_point {
            if !parent.pop() || !parent.starts_with(&mount_point) {
                break;
            }
            self.ancestors.push(CgroupMemory::collect(&parent, true));
        }
        Ok(())
    }
}

impl CgroupMemory {
    /// Read a specific cgroup directory without discovery or ancestor traversal.
    ///
    /// Reads regular `memory.*` files only. `memory.reclaim` is never opened.
    /// Absent kernel-dependent files are omitted. Each failed read is independent.
    pub fn from_directory(directory: impl AsRef<Path>) -> Self {
        Self::collect(directory.as_ref(), false)
    }

    fn collect(directory: &Path, limits_only: bool) -> Self {
        let mut snapshot = Self {
            directory: directory.into(),
            ..Self::default()
        };
        let entries = match fs::read_dir(directory) {
            Ok(entries) => entries,
            Err(error) => {
                snapshot.unavailable.insert(".".into(), error.into());
                return snapshot;
            }
        };
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    snapshot.unavailable.insert(".".into(), error.into());
                    continue;
                }
            };
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if !name.starts_with("memory.") || (limits_only && !ancestor_file(name)) {
                continue;
            }
            if name == "memory.reclaim" {
                snapshot.skipped.insert(name.into(), MemorySkipReason::WriteOnly);
                continue;
            }
            let result = (|| {
                let metadata = entry.metadata()?;
                if !metadata.is_file() {
                    snapshot.skipped.insert(name.into(), MemorySkipReason::NotRegularFile);
                    return Ok(None);
                }
                let mode = metadata.permissions().mode();
                if mode & 0o444 == 0 && mode & 0o222 != 0 {
                    snapshot.skipped.insert(name.into(), MemorySkipReason::WriteOnly);
                    return Ok(None);
                }
                let text = fs::read_to_string(entry.path())?;
                parse_value(name, &text).map(Some)
            })();
            match result {
                Ok(Some(value)) => {
                    snapshot.values.insert(name.into(), value);
                }
                Ok(None) => {}
                Err(error) => {
                    snapshot.unavailable.insert(name.into(), error);
                }
            }
        }
        snapshot
    }
}

fn ancestor_file(name: &str) -> bool {
    matches!(
        name,
        "memory.current"
            | "memory.min"
            | "memory.low"
            | "memory.high"
            | "memory.max"
            | "memory.swap.current"
            | "memory.swap.high"
            | "memory.swap.max"
            | "memory.zswap.current"
            | "memory.zswap.max"
    )
}

fn parse_value(name: &str, text: &str) -> Result<CgroupMemoryValue, MemoryReadError> {
    match name {
        "memory.stat" | "memory.events" | "memory.events.local" | "memory.swap.events" | "memory.zswap.events" => {
            let mut values = BTreeMap::new();
            for line in text.lines().filter(|line| !line.trim().is_empty()) {
                let fields: Vec<_> = line.split_ascii_whitespace().collect();
                if fields.len() != 2 || values.insert(fields[0].into(), unsigned(fields[1])?).is_some() {
                    return Err(invalid("malformed or duplicate memory counter"));
                }
            }
            Ok(CgroupMemoryValue::Counters(values))
        }
        "memory.numa_stat" => {
            let mut categories = BTreeMap::new();
            for line in text.lines().filter(|line| !line.trim().is_empty()) {
                let mut fields = line.split_ascii_whitespace();
                let category = fields.next().ok_or_else(|| invalid("missing NUMA category"))?;
                let mut nodes = BTreeMap::new();
                for field in fields {
                    let (node, value) = field.split_once('=').ok_or_else(|| invalid("malformed NUMA counter"))?;
                    let node = node.strip_prefix('N').ok_or_else(|| invalid("invalid NUMA node"))?;
                    let node = u32::try_from(unsigned(node)?).map_err(|_| invalid("NUMA node exceeds u32"))?;
                    if nodes.insert(node, unsigned(value)?).is_some() {
                        return Err(invalid("duplicate NUMA node"));
                    }
                }
                if nodes.is_empty() || categories.insert(category.into(), nodes).is_some() {
                    return Err(invalid("missing nodes or duplicate NUMA category"));
                }
            }
            Ok(CgroupMemoryValue::Numa(categories))
        }
        "memory.pressure" => MemoryPressure::from_read(text.as_bytes())
            .map(CgroupMemoryValue::Pressure)
            .map_err(|error| invalid(error.to_string())),
        _ => {
            let value = text.trim();
            if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
                Ok(CgroupMemoryValue::Number(unsigned(value)?))
            } else if value == "max" || !known_scalar(name) {
                Ok(CgroupMemoryValue::Text(text.trim_end_matches('\n').into()))
            } else {
                Err(invalid("malformed scalar memory value"))
            }
        }
    }
}

fn known_scalar(name: &str) -> bool {
    ancestor_file(name) || matches!(name, "memory.peak" | "memory.swap.peak" | "memory.oom.group" | "memory.zswap.writeback")
}

fn unsigned(value: &str) -> Result<u64, MemoryReadError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid("expected unsigned integer"));
    }
    value.parse().map_err(|_| invalid("value exceeds u64"))
}

fn invalid(message: impl Into<String>) -> MemoryReadError {
    MemoryReadError::ReadFailed(message.into())
}

fn validate_path(path: &Path) -> Result<(), MemoryReadError> {
    if !path.is_absolute() || path.components().any(|part| matches!(part, Component::ParentDir | Component::CurDir)) {
        return Err(invalid("cgroup paths must be absolute without parent traversal"));
    }
    Ok(())
}

fn decode_path(value: &str) -> Result<PathBuf, MemoryReadError> {
    let mut decoded = Vec::new();
    let mut bytes = value.bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'\\' {
            let mut value = 0_u16;
            for _ in 0..3 {
                let digit = bytes
                    .next()
                    .filter(|byte| (b'0'..=b'7').contains(byte))
                    .ok_or_else(|| invalid("invalid mount path escape"))?;
                value = value * 8 + u16::from(digit - b'0');
            }
            let byte = u8::try_from(value).map_err(|_| invalid("mount path escape exceeds byte"))?;
            if byte == 0 {
                return Err(invalid("NUL in mount path"));
            }
            decoded.push(byte);
        } else {
            decoded.push(byte);
        }
    }
    let path = PathBuf::from(String::from_utf8(decoded).map_err(|_| invalid("mount path is not UTF-8"))?);
    validate_path(&path)?;
    Ok(path)
}
