use crate::{MemoryFiles, MemoryReadError, MemoryValue};
use std::collections::BTreeMap;
use std::path::Path;

/// Opt-in per-mapping process accounting. Reading `smaps` walks page tables.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct ProcessMemoryDetails {
    /// Explicit process identity; other PIDs are subject to procfs/ptrace policy.
    pub pid: u32,
    /// Native `smaps`, `maps`, and `numa_maps` (4 MiB maximum per file).
    pub files: MemoryFiles,
    /// Parsed smaps mappings; `None` means unavailable or malformed, not empty.
    pub mappings: Option<Vec<MemoryMapping>>,
}

/// One smaps mapping, retaining unfamiliar fields, flags, units and pathname text.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct MemoryMapping {
    /// Original header, including permissions, offset, device, inode and pathname.
    pub header: String,
    /// Half-open virtual address interval, in bytes.
    pub address_range: (u64, u64),
    /// Native field values including their units, e.g. `Rss -> "12 kB"`.
    pub fields: BTreeMap<String, String>,
}

impl ProcessMemoryDetails {
    /// Collect the current process; no referenced bits or policies are modified.
    pub fn current() -> Self {
        Self::from_process_root(std::process::id(), "/proc/self")
    }

    /// Collect one explicitly identified process directory or captured files.
    pub fn from_process_root(pid: u32, root: impl AsRef<Path>) -> Self {
        let mut files = MemoryFiles::from_names(root, &["maps", "smaps", "numa_maps"]);
        let mappings = match files.values.get("smaps") {
            Some(MemoryValue::Text(text)) => match MemoryMapping::parse_smaps(text) {
                Ok(mappings) => Some(mappings),
                Err(error) => {
                    files.unavailable.insert("smaps.parsed".into(), error);
                    None
                }
            },
            Some(_) => {
                files
                    .unavailable
                    .insert("smaps.parsed".into(), MemoryReadError::ReadFailed("expected smaps text".into()));
                None
            }
            None => None,
        };
        Self { pid, files, mappings }
    }
}

impl MemoryMapping {
    /// Parse a complete smaps capture, rejecting malformed headers/duplicate fields.
    /// Empty text is valid for a process with no user mappings.
    pub fn parse_smaps(text: &str) -> Result<Vec<Self>, MemoryReadError> {
        let invalid = || MemoryReadError::ReadFailed("malformed smaps mapping or duplicate field".into());
        let mut mappings: Vec<Self> = Vec::new();
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let first = line.split_ascii_whitespace().next().ok_or_else(invalid)?;
            if let Some((start, end)) = first.split_once('-') {
                let fields: Vec<_> = line.split_ascii_whitespace().collect();
                if fields.len() < 5 || start.is_empty() || end.is_empty() || !start.bytes().chain(end.bytes()).all(|b| b.is_ascii_hexdigit()) {
                    return Err(invalid());
                }
                let range = (
                    u64::from_str_radix(start, 16).map_err(|_| invalid())?,
                    u64::from_str_radix(end, 16).map_err(|_| invalid())?,
                );
                if range.0 >= range.1 || fields[1].len() != 4 || !fields[2].bytes().all(|b| b.is_ascii_hexdigit()) || fields[4].parse::<u64>().is_err() {
                    return Err(invalid());
                }
                mappings.push(Self {
                    header: line.into(),
                    address_range: range,
                    fields: BTreeMap::new(),
                });
            } else {
                let (name, value) = line.split_once(':').ok_or_else(invalid)?;
                let mapping = mappings.last_mut().ok_or_else(invalid)?;
                if name.is_empty() || value.trim().is_empty() || mapping.fields.insert(name.into(), value.trim().into()).is_some() {
                    return Err(invalid());
                }
            }
        }
        Ok(mappings)
    }
}
