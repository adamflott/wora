use crate::{MemoryFiles, MemoryReadError};
use std::path::Path;

/// Opt-in kernel allocation attribution, retaining native vmalloc area details.
///
/// Reads `/proc/vmallocinfo` into `files` and `/proc/allocinfo` into
/// `allocation_sites`. Alternate proc roots replace `/proc`; parsing supplied
/// text performs no I/O. Attribution is kernel-wide, not process RSS.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct KernelAllocationStats {
    /// Structured vmalloc areas and per-file errors; limited to 4 MiB each.
    pub files: MemoryFiles,
    /// Parsed allocation sites; absent if unsupported, inaccessible or malformed.
    pub allocation_sites: Option<AllocationSites>,
}

/// Versioned `/proc/allocinfo` data. These are overlapping attribution views.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct AllocationSites {
    /// Kernel format version (`1.0` or `2.0`).
    pub version: String,
    /// Kernel allocation locations and their current accounting.
    pub sites: Vec<AllocationSite>,
}

/// One kernel allocation call site, including optional accuracy markers.
///
/// Each record comes from one `/proc/allocinfo` row in [`AllocationSites`];
/// bytes and allocation counts retain that row's native accounting scope.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct AllocationSite {
    /// Accounted bytes; `accurate:no` means absolute accounting is inaccurate.
    pub bytes: u64,
    /// Accounted allocation count, subject to the same accuracy marker.
    pub allocations: u64,
    /// Source file and line as printed by the kernel.
    pub location: String,
    /// Function, module, accuracy markers and future fields, without interpretation.
    pub attributes: Vec<String>,
}

impl KernelAllocationStats {
    /// Read the host's allocation attribution files, without enabling profiling.
    pub fn current() -> Self {
        Self::from_proc_root("/proc")
    }
    /// Read explicit procfs/captured files; symbols may be restricted or redacted.
    /// Allocation profiling requires `CONFIG_MEM_ALLOC_PROFILING` and enablement.
    pub fn from_proc_root(root: impl AsRef<Path>) -> Self {
        let root = root.as_ref();
        let mut files = MemoryFiles::from_names(root, &["vmallocinfo"]);
        let allocation_sites = files.read_with(root, "allocinfo", 4 * 1024 * 1024, AllocationSites::parse);
        Self { files, allocation_sites }
    }
}

impl AllocationSites {
    /// Parse known allocinfo versions atomically, preserving all trailing metadata.
    pub fn parse(text: &str) -> Result<Self, MemoryReadError> {
        let mut lines = text.lines();
        let version = lines
            .next()
            .and_then(|line| line.strip_prefix("allocinfo - version: "))
            .map(str::trim)
            .ok_or_else(|| invalid("missing allocinfo version"))?;
        if !matches!(version, "1.0" | "2.0") {
            return Err(invalid("unsupported allocinfo version"));
        }
        let mut sites = Vec::new();
        for line in lines.filter(|line| !line.trim().is_empty() && !line.trim_start().starts_with('#')) {
            let fields: Vec<_> = line.split_ascii_whitespace().collect();
            if fields.len() < 3 {
                return Err(invalid("incomplete allocation site"));
            }
            let unsigned = |value: &str| {
                if !value.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(invalid("invalid allocation count"));
                }
                value.parse::<u64>().map_err(|_| invalid("allocation count overflow"))
            };
            sites.push(AllocationSite {
                bytes: unsigned(fields[0])?,
                allocations: unsigned(fields[1])?,
                location: fields[2].into(),
                attributes: fields[3..].iter().map(|v| (*v).into()).collect(),
            });
        }
        Ok(Self {
            version: version.into(),
            sites,
        })
    }
}

fn invalid(message: &str) -> MemoryReadError {
    MemoryReadError::read_failed(message)
}
