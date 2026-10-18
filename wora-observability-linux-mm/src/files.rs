use std::collections::BTreeMap;
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
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct MemoryFiles {
    /// Structured numbers, named tables, flags, units and ranges.
    /// Raw file contents are not retained. Unsupported or malformed layouts use
    /// the source path in `unavailable`, just like read failures.
    pub parsed: BTreeMap<String, crate::ParsedMemoryValue>,
    /// Read/parse failures; `.` denotes directory or budget-admission failure.
    pub unavailable: BTreeMap<String, MemoryReadError>,
    /// Entries deliberately not opened, including symlinks and write-only files.
    pub skipped: BTreeMap<String, MemorySkipReason>,
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
        let mut result = Self::default();
        for name in names {
            if !crate::budget::file_available() {
                result
                    .unavailable
                    .insert(".".into(), crate::budget::exceeded(crate::CollectionLimit::Files).into());
                break;
            }
            result.read(root.as_ref(), name, limit);
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
                result.unavailable.insert(".".into(), error.into());
            }
        }
        result
    }

    pub(crate) fn read(&mut self, root: &Path, name: &str, limit: u64) {
        if let Some(value) = self.read_with(root, name, limit, |text| {
            crate::parse_memory_file(root.join(name), text)?.ok_or_else(|| MemoryReadError::ReadFailed("unsupported memory-information layout".into()))
        }) {
            self.parsed.insert(name.into(), value);
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
            self.unavailable.insert(".".into(), error.into());
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
                    self.skipped.insert(name.into(), MemorySkipReason::NotRegularFile);
                    return Ok(None);
                }
            }
            let metadata = fs::symlink_metadata(&path)?;
            if !metadata.is_file() {
                self.skipped.insert(name.into(), MemorySkipReason::NotRegularFile);
                return Ok(None);
            }
            if metadata.permissions().mode() & 0o444 == 0 && metadata.permissions().mode() & 0o222 != 0 {
                self.skipped.insert(name.into(), MemorySkipReason::WriteOnly);
                return Ok(None);
            }
            let text = read_admitted_text(&path, limit)?;
            parse(&text).map(Some)
        })();
        match result {
            Ok(value) => value,
            Err(error) => {
                self.unavailable.insert(name.into(), error);
                None
            }
        }
    }
}

pub(crate) fn read_text(path: &Path, limit: u64) -> io::Result<String> {
    crate::budget::charge_file()?;
    read_admitted_text(path, limit)
}

fn read_admitted_text(path: &Path, limit: u64) -> io::Result<String> {
    crate::budget::check_path(path)?;
    let limit = crate::budget::file_limit(usize::try_from(limit).unwrap_or(usize::MAX));
    let mut data = Vec::new();
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
            data.try_reserve_exact(capacity - data.len()).map_err(io::Error::other)?;
        }
        data.extend_from_slice(&chunk[..count]);
    }
    String::from_utf8(data).map_err(io::Error::other)
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
