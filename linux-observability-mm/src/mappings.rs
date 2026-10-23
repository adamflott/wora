use crate::storage::CollectMemory;
use crate::structured::{atom, hexadecimal, words};
use crate::{MemoryFiles, MemoryReadError, ParsedMemoryValue};
use std::path::Path;

/// Opt-in per-mapping process accounting. Reading `smaps` walks page tables.
///
/// Reads `/proc/self/{maps,numa_maps,smaps}` by default.
/// [`Self::from_process_root`] substitutes an explicit `/proc/<pid>` or captured
/// process directory; native `kB` quantities mean KiB and page counts remain pages.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct ProcessMemoryDetails {
    /// Explicit process identity; other PIDs are subject to procfs/ptrace policy.
    pub pid: u32,
    /// Structured `maps` and `numa_maps`, plus errors (4 MiB maximum per file).
    pub files: MemoryFiles,
    /// Parsed smaps mappings; `None` means unavailable or malformed, not empty.
    pub mappings: Option<crate::MemoryList<MemoryMapping>>,
}

/// One smaps mapping, with parsed metadata, accounting, flags and native units.
///
/// Parsed from one `/proc/<pid>/smaps` section (or a `maps` header via the
/// structured parser). `fields` comes from the lines following that header;
/// quantities retain their source unit, normally `kB` meaning KiB.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct MemoryMapping {
    /// Half-open virtual address interval, in bytes.
    pub address_range: (u64, u64),
    /// Native mapping permission flags, such as `rw-p`.
    pub permissions: crate::MemoryText,
    /// Byte offset within the backing file.
    pub offset: u64,
    /// Backing device's major and minor numbers.
    pub device: (u64, u64),
    /// Backing inode number; zero for anonymous mappings.
    pub inode: u64,
    /// Pathname or kernel mapping label; empty for unnamed mappings.
    pub pathname: crate::MemoryText,
    /// Structured accounting quantities and VM flag lists, keyed by kernel name.
    pub fields: crate::MemoryMap<ParsedMemoryValue>,
}

impl ProcessMemoryDetails {
    /// Collect the current process; no referenced bits or policies are modified.
    pub fn current() -> Self {
        Self::from_process_root(std::process::id(), "/proc/self")
    }

    /// Collect one explicitly identified process directory or captured files.
    pub fn from_process_root(pid: u32, root: impl AsRef<Path>) -> Self {
        let root = root.as_ref();
        let mut files = MemoryFiles::from_names(root, &["maps", "numa_maps"]);
        let mappings = files.read_with(root, "smaps", 4 * 1024 * 1024, MemoryMapping::parse_smaps);
        Self { pid, files, mappings }
    }
}

impl MemoryMapping {
    /// Parse a complete smaps capture, rejecting malformed headers/duplicate fields.
    /// Empty text is valid for a process with no user mappings.
    pub fn parse_smaps(text: &str) -> Result<crate::MemoryList<Self>, MemoryReadError> {
        crate::heap::with_default_budget(|| Self::parse_inner(text))
    }

    fn parse_inner(text: &str) -> Result<crate::MemoryList<Self>, MemoryReadError> {
        let invalid = || MemoryReadError::read_failed("malformed smaps mapping or duplicate field");
        let mut mappings: crate::MemoryList<Self> = crate::MemoryList::default();
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let first = line.split_ascii_whitespace().next().ok_or_else(invalid)?;
            if let Some((start, end)) = first.split_once('-') {
                let (fields, pathname) = crate::process_values::prefix(line, 5)?;
                if start.is_empty() || end.is_empty() || !start.bytes().chain(end.bytes()).all(|b| b.is_ascii_hexdigit()) {
                    return Err(invalid());
                }
                let range = (
                    u64::from_str_radix(start, 16).map_err(|_| invalid())?,
                    u64::from_str_radix(end, 16).map_err(|_| invalid())?,
                );
                let permissions = fields[1].as_bytes();
                if range.0 >= range.1
                    || permissions.len() != 4
                    || !matches!(permissions[0], b'r' | b'-')
                    || !matches!(permissions[1], b'w' | b'-')
                    || !matches!(permissions[2], b'x' | b'-')
                    || !matches!(permissions[3], b'p' | b's')
                {
                    return Err(invalid());
                }
                let parse_hex = |text: &str| hexadecimal(text)?.as_u64().ok_or_else(invalid);
                let (major, minor) = fields[3].split_once(':').ok_or_else(invalid)?;
                let inode = fields[4].parse::<u64>().map_err(|_| invalid())?;
                mappings.try_push(Self {
                    address_range: range,
                    permissions: crate::MemoryText::try_new(fields[1])?,
                    offset: parse_hex(fields[2])?,
                    device: (parse_hex(major)?, parse_hex(minor)?),
                    inode,
                    pathname: crate::MemoryText::try_new(pathname)?,
                    fields: crate::MemoryMap::default(),
                })?;
                if mappings.len() > 65536 {
                    return Err(invalid());
                }
            } else {
                let (name, value) = line.split_once(':').ok_or_else(invalid)?;
                let mapping = mappings.try_get_mut(mappings.len().saturating_sub(1))?.ok_or_else(invalid)?;
                let parsed = if name == "VmFlags" {
                    ParsedMemoryValue::List(words(value)?.iter().map(|v| ParsedMemoryValue::try_text(*v)).collect_memory()?)
                } else {
                    if value.trim().is_empty() {
                        return Err(invalid());
                    }
                    atom(value)?
                };
                if name.is_empty() || mapping.fields.try_insert(name, parsed)?.is_some() {
                    return Err(invalid());
                }
            }
        }
        Ok(mappings)
    }
}
