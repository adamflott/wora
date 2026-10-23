use std::fs::{self, File};
use std::io::{self, Read};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use crate::{MemoryReadError, MemorySkipReason};

/// Bounded, independent reads of explicitly selected memory information files.
///
/// Keys are relative source paths. Numeric scalars retain integer precision;
/// tables use named fields. Missing, malformed and denied files
/// remain distinguishable from zero. Reads are not atomic across files.
///
/// For each `parsed` key, the input path is the collector's root joined with
/// that relative key. This container does not retain the root: its enclosing
/// type documents whether that root is a process, sysfs node/device, or debugfs
/// directory. Values keep the file's units; parser-only calls perform no reads.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct MemoryFiles {
    /// Structured numbers, named tables, flags, units and ranges.
    /// Raw file contents are not retained. Unsupported or malformed layouts use
    /// the source path in `unavailable`, just like read failures.
    pub parsed: crate::MemoryMap<crate::ParsedMemoryValue>,
    /// Read/parse failures; `.` denotes directory or budget-admission failure.
    pub unavailable: crate::MemoryMap<MemoryReadError>,
    /// Entries deliberately not opened, including symlinks and write-only files.
    pub skipped: crate::MemoryMap<MemorySkipReason>,
    /// Inline failure when even a source/error-map entry cannot be admitted.
    /// Prevents allocation failures from looking like successfully empty data.
    #[cfg_attr(feature = "serde1", serde(default, skip_serializing_if = "Option::is_none"))]
    pub allocation_error: Option<crate::HeapError>,
}

impl Default for MemoryFiles {
    fn default() -> Self {
        let budget = crate::HeapBudget::current();
        Self {
            parsed: crate::MemoryMap::new(budget.clone()),
            unavailable: crate::MemoryMap::new(budget.clone()),
            skipped: crate::MemoryMap::new(budget),
            allocation_error: None,
        }
    }
}

impl MemoryFiles {
    /// Read named relative files. Names must be normal path components.
    ///
    /// Each file is limited to 4 MiB. Oversized or non-UTF-8 contents produce a
    /// read failure, never a successful partial value. No file is opened to write.
    pub fn from_names(root: impl AsRef<Path>, names: &[&str]) -> Self {
        Self::from_names_with_limit(root, names, 4 * 1024 * 1024)
    }

    /// Read named information files with an explicit per-file byte limit.
    /// Limits must be between one byte and 64 MiB; invalid limits fail before I/O.
    pub fn from_names_with_limit(root: impl AsRef<Path>, names: &[&str], limit: u64) -> Self {
        let budget = crate::HeapBudget::current();
        crate::with_heap_budget(&budget, || Self::collect_names(root.as_ref(), names, limit))
    }

    /// Read selected sources in a caller-owned shared allocation budget.
    /// Source failures remain per-file errors; allocation refusal returns an error.
    /// Captured roots are used as supplied and are not fresh live samples.
    pub fn try_from_names(root: impl AsRef<Path>, names: &[&str], limit: u64, budget: &crate::HeapBudget) -> Result<Self, crate::HeapError> {
        let result = crate::with_heap_budget(budget, || Self::collect_names(root.as_ref(), names, limit));
        match result.allocation_error {
            Some(error) => Err(error),
            None => Ok(result),
        }
    }

    fn collect_names(root: &Path, names: &[&str], limit: u64) -> Self {
        let mut result = Self::default();
        for name in names {
            if !crate::budget::file_available() {
                result.record_error(".", crate::budget::exceeded(crate::CollectionLimit::Files).into());
                break;
            }
            result.read(root, name, limit);
        }
        result
    }

    pub(crate) fn directory(root: &Path, select: impl Fn(&str) -> bool) -> Self {
        let mut result = Self::default();
        match entries(root) {
            Ok(entries) => {
                for (name, _) in entries {
                    if select(&name) {
                        result.read(root, &name, 4 * 1024 * 1024);
                    }
                }
            }
            Err(error) => {
                result.record_error(".", error.into());
            }
        }
        result
    }

    pub(crate) fn read(&mut self, root: &Path, name: &str, limit: u64) {
        if let Some(value) = self.read_with(root, name, limit, |text| {
            crate::parse_memory_file(root.join(name), text)?.ok_or_else(|| MemoryReadError::read_failed("unsupported memory-information layout"))
        }) && let Err(error) = self.parsed.try_insert(name, value)
        {
            self.record_error(name, error.into());
        }
    }

    pub(crate) fn record_error(&mut self, name: &str, error: MemoryReadError) {
        if let MemoryReadError::Allocation(error) = &error {
            self.allocation_error.get_or_insert(*error);
        }
        if let Err(error) = self.unavailable.try_insert(name, error) {
            self.allocation_error.get_or_insert(error);
        }
    }

    pub(crate) fn record_skipped(&mut self, name: &str, reason: MemorySkipReason) {
        if let Err(error) = self.skipped.try_insert(name, reason) {
            self.allocation_error.get_or_insert(error);
        }
    }

    // Specialized collectors consume the bounded text once and store only their
    // typed result. All reads share path validation, exclusions and error keys.
    pub(crate) fn read_with<T>(&mut self, root: &Path, name: &str, limit: u64, parse: impl FnOnce(&str) -> Result<T, MemoryReadError>) -> Option<T> {
        // Charge explicit names before metadata: missing sources still create
        // error records. Oversized/unadmitted names use one fixed error key.
        let admitted = crate::budget::charge_file().and_then(|()| {
            crate::budget::check_path(root)?;
            crate::budget::check_path(Path::new(name))
        });
        if let Err(error) = admitted {
            self.record_error(".", error.into());
            return None;
        }
        let result: Result<Option<T>, MemoryReadError> = (|| {
            if !(1..=64 * 1024 * 1024).contains(&limit) {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "file limit must be 1..=64 MiB").into());
            }
            let relative = Path::new(name);
            if relative.components().any(|c| !matches!(c, std::path::Component::Normal(_))) {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "expected a relative information-file path").into());
            }
            let mut path = root.to_path_buf();
            for component in relative.components() {
                path.push(component);
                crate::budget::check_path(&path)?;
                let metadata = fs::symlink_metadata(&path)?;
                if metadata.file_type().is_symlink() {
                    self.record_skipped(name, MemorySkipReason::NotRegularFile);
                    return Ok(None);
                }
            }
            let metadata = fs::symlink_metadata(&path)?;
            if !metadata.is_file() {
                self.record_skipped(name, MemorySkipReason::NotRegularFile);
                return Ok(None);
            }
            if metadata.permissions().mode() & 0o444 == 0 && metadata.permissions().mode() & 0o222 != 0 {
                self.record_skipped(name, MemorySkipReason::WriteOnly);
                return Ok(None);
            }
            let budget = self.parsed.budget().clone();
            crate::with_heap_budget(&budget, || {
                let text = read_admitted_text(&path, limit)?;
                parse(&text).map(Some)
            })
        })();
        match result {
            Ok(value) => value,
            Err(error) => {
                self.record_error(name, error);
                None
            }
        }
    }
}

pub(crate) struct ReadText {
    text: String,
    _reservation: Option<crate::HeapReservation>,
}
impl std::ops::Deref for ReadText {
    type Target = str;
    fn deref(&self) -> &str {
        &self.text
    }
}

pub(crate) fn read_text(path: &Path, limit: u64) -> io::Result<ReadText> {
    crate::budget::charge_file()?;
    read_admitted_text(path, limit)
}

fn read_admitted_text(path: &Path, limit: u64) -> io::Result<ReadText> {
    crate::budget::check_path(path)?;
    let limit = crate::budget::file_limit(usize::try_from(limit).unwrap_or(usize::MAX));
    let mut data = Vec::new();
    let budget = crate::HeapBudget::current();
    let mut reservation = None;
    let mut file = File::open(path)?;
    let mut chunk = [0; 8192];
    let mut line_bytes = 0;
    let line_limit = crate::budget::limits().line_bytes;
    loop {
        let allowance = limit.saturating_sub(data.len()).min(crate::budget::remaining_bytes());
        // Probe one byte past a full allowance to distinguish EOF from a valid
        // prefix of an oversized file. That byte is never retained or parsed.
        let capacity = chunk.len().min(allowance.saturating_add(1));
        let count = file.read(&mut chunk[..capacity])?;
        if count == 0 {
            break;
        }
        if count > limit.saturating_sub(data.len()) {
            let error = crate::budget::exceeded(crate::CollectionLimit::FileBytes);
            let _ = crate::budget::charge_bytes(count);
            return Err(error);
        }
        crate::budget::charge_bytes(count)?;
        for byte in &chunk[..count] {
            if *byte == b'\n' {
                line_bytes = 0;
            } else {
                line_bytes += 1;
            }
            if line_bytes > line_limit {
                return Err(crate::budget::exceeded(crate::CollectionLimit::LineBytes));
            }
        }
        if data.capacity() - data.len() < count {
            let capacity = (data.len() + count).max(data.capacity().saturating_mul(2)).min(limit);
            let replacement = budget.try_reserve(crate::HeapKind::Scratch, capacity, 1)?;
            data.try_reserve_exact(capacity - data.len()).map_err(io::Error::other)?;
            if data.capacity() > capacity {
                return Err(crate::HeapError::AllocationFailed.into());
            }
            reservation = Some(replacement);
        }
        data.extend_from_slice(&chunk[..count]);
    }
    let text = String::from_utf8(data).map_err(io::Error::other)?;
    Ok(ReadText {
        text,
        _reservation: reservation,
    })
}

/// Read and parse a procfs source through the active collection budget.
///
/// Without a scoped budget, text is limited to 4 MiB and lines to 64 KiB.
/// Oversized sources fail before parsing; valid-looking prefixes are not used.
pub fn read_source<T: crate::FromRead>(path: impl AsRef<Path>) -> crate::ProcResult<T> {
    let text = read_text(path.as_ref(), 4 * 1024 * 1024)?;
    T::from_read(text.as_bytes())
}

pub(crate) fn entries(root: &Path) -> io::Result<Vec<(String, PathBuf)>> {
    crate::budget::check_path(root)?;
    let mut result = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        crate::budget::charge_entry()?;
        if result.len() >= 4096 {
            return Err(crate::budget::exceeded(crate::CollectionLimit::Entries));
        }
        let name = entry.file_name().into_string().map_err(|_| io::Error::other("non-UTF-8 entry name"))?;
        let path = entry.path();
        crate::budget::check_path(&path)?;
        result.push((name, path));
    }
    result.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(result)
}

pub(crate) fn numbered(name: &str, prefix: &str) -> Option<u32> {
    let suffix = name.strip_prefix(prefix)?;
    (!suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()))
        .then(|| suffix.parse().ok())
        .flatten()
}

// Sysfs class/device links are intentional. Resolve only selected directories,
// and require their targets to remain inside the explicitly supplied sysfs tree.
pub(crate) fn resolve_directory(root: &Path, path: &Path) -> io::Result<PathBuf> {
    let root = root.canonicalize()?;
    let path = path.canonicalize()?;
    if !path.starts_with(root) || !path.is_dir() {
        return Err(io::Error::other("sysfs directory link escapes supplied root"));
    }
    Ok(path)
}
