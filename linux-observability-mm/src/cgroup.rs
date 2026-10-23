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
///
/// Reads selected `memory.*`, `hugetlb.*`, `dmem.*`, `cpuset.mems` and
/// `cpuset.mems.effective` files under [`Self::directory`]. Discovery normally
/// selects a mounted cgroup v2 directory via [`CgroupMemoryStats`]; callers can
/// supply a captured directory instead. `memory.reclaim` is never opened.
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
    /// DMEM tables map opaque device-region names to bytes or literal `max`:
    /// `current`/`peak` include descendants, `min`/`low`/`max` are local settings,
    /// and root-only `capacity` is device capacity, not an allocation allowance.
    /// Raw file contents are not retained.
    pub parsed: BTreeMap<String, CgroupMemoryValue>,
    /// Per-file read/parse failures; directory failures use `.`.
    pub unavailable: BTreeMap<String, MemoryReadError>,
    /// Action controls and non-regular or write-only entries not opened.
    pub skipped: BTreeMap<String, MemorySkipReason>,
}

/// Native cgroup memory file contents, without converting unlimited limits to zero.
///
/// Parsed from a file named by [`CgroupMemory::parsed`] under its cgroup
/// directory. The filename/counter key determines scope and units: values may
/// be byte sizes, event/page counts, node lists or pressure percentages and
/// microseconds. There is no blanket conversion of numeric values to bytes.
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
    /// Literal `max` or a textual setting with no numeric meaning.
    Text(String),
    /// Structured CPU/node ranges, HugeTLB NUMA counters, or DMEM region tables.
    /// DMEM values are bytes (even with non-system page sizes) or literal `max`.
    Structured(crate::ParsedMemoryValue),
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
        let membership = crate::files::read_text(&root.join("self/cgroup"), 4 * 1024 * 1024).map_err(|error| ("self/cgroup", error.into()))?;
        let mut paths = membership.lines().filter_map(|line| line.strip_prefix("0::"));
        let path = paths.next().ok_or_else(|| ("self/cgroup", invalid("no cgroup v2 membership")))?;
        if paths.next().is_some() || path.ends_with(" (deleted)") {
            return Err(("self/cgroup", invalid("ambiguous or deleted cgroup membership")));
        }
        crate::budget::check_path(Path::new(path)).map_err(|error| ("self/cgroup", error.into()))?;
        let path = PathBuf::from(path);
        validate_path(&path).map_err(|error| ("self/cgroup", error))?;
        self.membership = Some(path.clone());
        let mountinfo = crate::files::read_text(&root.join("self/mountinfo"), 4 * 1024 * 1024).map_err(|error| ("self/mountinfo", error.into()))?;
        let mut best: Option<(PathBuf, PathBuf, PathBuf)> = None;
        for line in mountinfo.lines() {
            let Some((fields, filesystem)) = line.split_once(" - ") else { continue };
            if filesystem.split_ascii_whitespace().next() != Some("cgroup2") {
                continue;
            }
            let mut fields = fields.split_ascii_whitespace();
            let root = fields.nth(3).ok_or_else(|| ("self/mountinfo", invalid("incomplete cgroup2 mount")))?;
            let point = fields.next().ok_or_else(|| ("self/mountinfo", invalid("incomplete cgroup2 mount")))?;
            if fields.next().is_none() {
                return Err(("self/mountinfo", invalid("incomplete cgroup2 mount")));
            }
            let mount_root = decode_path(root).map_err(|error| ("self/mountinfo", error))?;
            let mount_point = decode_path(point).map_err(|error| ("self/mountinfo", error))?;
            if let Ok(relative) = path.strip_prefix(&mount_root) {
                let replace = best.as_ref().is_none_or(|(previous_root, previous_point, _)| {
                    mount_root.components().count() > previous_root.components().count()
                        || (mount_root.components().count() == previous_root.components().count() && mount_point < *previous_point)
                });
                if replace {
                    let directory = mount_point.join(relative);
                    crate::budget::check_path(&directory).map_err(|e| ("self/mountinfo", e.into()))?;
                    best = Some((mount_root, mount_point, directory));
                }
            }
        }
        // Prefer a mount rooted closest to this cgroup. Ties are deterministic.
        let (mount_root, mount_point, directory) = best.ok_or_else(|| ("cgroup2", invalid("no matching visible cgroup2 mount")))?;
        self.mount_point = Some(mount_point.clone());
        self.mount_root = Some(mount_root);
        self.current = Some(CgroupMemory::from_directory(&directory));
        let mut parent = directory;
        while parent != mount_point {
            if self.ancestors.len() >= crate::budget::limits().ancestors {
                return Err(("cgroup2", crate::budget::exceeded(crate::CollectionLimit::Ancestors).into()));
            }
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
    /// Reads regular `memory.*`, `hugetlb.*`, known `dmem.*` and cpuset memory-node files.
    /// DMEM requires Linux 6.14+ and an enabled controller/device provider;
    /// development-kernel `dmem.peak` (7.3-rc7 ABI) is read only when present.
    /// `memory.reclaim` and unknown DMEM files are never opened.
    /// Absent kernel-dependent files are omitted. Each failed read is independent.
    pub fn from_directory(directory: impl AsRef<Path>) -> Self {
        Self::collect(directory.as_ref(), false)
    }

    fn collect(directory: &Path, limits_only: bool) -> Self {
        let mut snapshot = Self {
            directory: directory.into(),
            ..Self::default()
        };
        let entries = match crate::budget::check_path(directory).and_then(|()| fs::read_dir(directory)) {
            Ok(entries) => entries,
            Err(error) => {
                snapshot.unavailable.insert(".".into(), error.into());
                return snapshot;
            }
        };
        for (index, entry) in entries.enumerate() {
            if let Err(error) = crate::budget::charge_entry().and_then(|()| {
                if index >= 4096 {
                    Err(crate::budget::exceeded(crate::CollectionLimit::Entries))
                } else {
                    Ok(())
                }
            }) {
                snapshot.unavailable.insert(".".into(), error.into());
                break;
            }
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    snapshot.unavailable.insert(".".into(), error.into());
                    continue;
                }
            };
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if !(name.starts_with("memory.") || name.starts_with("hugetlb.") || dmem_file(name) || matches!(name, "cpuset.mems" | "cpuset.mems.effective"))
                || (limits_only && !ancestor_file(name))
            {
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
                let text = crate::files::read_text(&entry.path(), 4 * 1024 * 1024)?;
                parse_value(name, &text).map(Some)
            })();
            match result {
                Ok(Some(value)) => {
                    snapshot.parsed.insert(name.into(), value);
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
    if dmem_file(name) {
        return name != "dmem.peak";
    }
    if matches!(name, "cpuset.mems" | "cpuset.mems.effective") || (name.starts_with("hugetlb.") && (name.ends_with(".max") || name.ends_with(".current"))) {
        return true;
    }
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

fn dmem_file(name: &str) -> bool {
    matches!(name, "dmem.current" | "dmem.capacity" | "dmem.min" | "dmem.low" | "dmem.max" | "dmem.peak")
}

fn parse_value(name: &str, text: &str) -> Result<CgroupMemoryValue, MemoryReadError> {
    match name {
        name if dmem_file(name) => {
            let mut regions = crate::MemoryMap::default();
            for line in text.lines().filter(|line| !line.trim().is_empty()) {
                let mut fields = line.split_ascii_whitespace();
                let region = fields.next().ok_or_else(|| invalid("missing DMEM region"))?;
                let value = fields.next().ok_or_else(|| invalid("missing DMEM byte value"))?;
                let value = if value == "max" && matches!(name, "dmem.min" | "dmem.low" | "dmem.max") {
                    crate::ParsedMemoryValue::try_text("max")?
                } else {
                    crate::ParsedMemoryValue::Unsigned(unsigned(value)?)
                };
                if fields.next().is_some() || regions.try_insert(region, value)?.is_some() {
                    return Err(invalid("malformed or duplicate DMEM region"));
                }
            }
            Ok(CgroupMemoryValue::Structured(crate::ParsedMemoryValue::Map(regions)))
        }
        "cpuset.mems" | "cpuset.mems.effective" => crate::structured::ranges(text).map(CgroupMemoryValue::Structured),
        name if matches!(
            name,
            "memory.stat" | "memory.events" | "memory.events.local" | "memory.swap.events" | "memory.zswap.events"
        ) || (name.starts_with("hugetlb.") && (name.ends_with(".events") || name.ends_with(".events.local"))) =>
        {
            let mut values = BTreeMap::new();
            for line in text.lines().filter(|line| !line.trim().is_empty()) {
                let mut fields = line.split_ascii_whitespace();
                let key = fields.next().ok_or_else(|| invalid("missing memory counter"))?;
                let value = fields.next().ok_or_else(|| invalid("missing memory counter value"))?;
                if fields.next().is_some() || values.insert(key.into(), unsigned(value)?).is_some() {
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
            } else if value == "max" {
                Ok(CgroupMemoryValue::Text(value.into()))
            } else if !known_scalar(name) {
                match crate::parse_memory_file(name, text)? {
                    Some(crate::ParsedMemoryValue::Text(value)) => Ok(CgroupMemoryValue::Text(value.to_string())),
                    Some(value) => Ok(CgroupMemoryValue::Structured(value)),
                    None => Err(invalid("unsupported cgroup memory-information layout")),
                }
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

fn invalid(message: impl AsRef<str>) -> MemoryReadError {
    MemoryReadError::read_failed(message)
}

fn validate_path(path: &Path) -> Result<(), MemoryReadError> {
    crate::budget::check_path(path)?;
    if !path.is_absolute() || path.components().any(|part| matches!(part, Component::ParentDir | Component::CurDir)) {
        return Err(invalid("cgroup paths must be absolute without parent traversal"));
    }
    Ok(())
}

fn decode_path(value: &str) -> Result<PathBuf, MemoryReadError> {
    let mut decoded = Vec::new();
    let mut bytes = value.bytes();
    while let Some(byte) = bytes.next() {
        if decoded.len() >= crate::budget::limits().path_bytes {
            return Err(crate::budget::exceeded(crate::CollectionLimit::PathBytes).into());
        }
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
