use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, Read};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use crate::{MemoryReadError, MemorySkipReason};

/// One native kernel value. Tables, units, lists and policy strings remain text.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde1", serde(untagged))]
pub enum MemoryValue {
    /// Unsigned scalar, without unit conversion.
    Unsigned(u64),
    /// Negative scalar, without unit conversion.
    Signed(i64),
    /// Native file contents, excluding trailing newlines.
    Text(String),
}

/// Bounded, independent reads of explicitly selected memory information files.
///
/// Keys are relative source paths. Numeric scalars retain integer precision;
/// tables and unfamiliar layouts retain native text. Missing and denied files
/// remain distinguishable from zero. Reads are not atomic across files.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct MemoryFiles {
    /// Successfully read values, in each source's native units.
    pub values: BTreeMap<String, MemoryValue>,
    /// Read failures; `.` denotes directory enumeration failure.
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
        let result = (|| {
            if !(1..=64 * 1024 * 1024).contains(&limit) {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "file limit must be 1..=64 MiB"));
            }
            let relative = Path::new(name);
            if relative.components().any(|c| !matches!(c, std::path::Component::Normal(_))) {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "expected a relative information-file path"));
            }
            let mut path = root.to_path_buf();
            for component in relative.components() {
                path.push(component);
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
            let text = read_text(&path, limit)?;
            let text = text.trim_end_matches('\n');
            let scalar = text.trim();
            let value = if !scalar.is_empty() && scalar.bytes().all(|b| b.is_ascii_digit()) {
                MemoryValue::Unsigned(scalar.parse().map_err(io::Error::other)?)
            } else if scalar.strip_prefix('-').is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())) {
                MemoryValue::Signed(scalar.parse().map_err(io::Error::other)?)
            } else {
                MemoryValue::Text(text.into())
            };
            Ok(Some(value))
        })();
        match result {
            Ok(Some(value)) => {
                self.values.insert(name.into(), value);
            }
            Ok(None) => {}
            Err(error) => {
                self.unavailable.insert(name.into(), error.into());
            }
        }
    }
}

pub(crate) fn read_text(path: &Path, limit: u64) -> io::Result<String> {
    let mut data = Vec::new();
    File::open(path)?.take(limit.saturating_add(1)).read_to_end(&mut data)?;
    if data.len() as u64 > limit {
        return Err(io::Error::other(format!("information file exceeds {limit} byte limit")));
    }
    String::from_utf8(data).map_err(io::Error::other)
}

pub(crate) fn entries(root: &Path) -> io::Result<Vec<(String, PathBuf)>> {
    let mut result = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if result.len() >= 4096 {
            return Err(io::Error::other("directory exceeds 4096 entry limit"));
        }
        let name = entry.file_name().into_string().map_err(|_| io::Error::other("non-UTF-8 entry name"))?;
        result.push((name, entry.path()));
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
