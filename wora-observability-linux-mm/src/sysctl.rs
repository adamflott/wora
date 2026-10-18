use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

/// Best-effort snapshot of Linux memory-related sysctls.
///
/// Collects regular files directly under `/proc/sys/vm`, kernel shared-memory
/// (`shm*`), NUMA-balancing (`numa_balancing*`), and `perf_event_mlock_kb`
/// settings, plus selected filesystem cache and descriptor inventories.
/// New VM, shared-memory, and NUMA-balancing names are retained automatically.
///
/// Values are structured scalars, vectors or named filesystem accounting fields,
/// preserving native units and exact integers larger than `i64::MAX`.
/// Keys use dotted sysctl names, such as `vm.swappiness`.
/// Reads are synchronous and independent; a failed file does not discard other
/// values. A snapshot is not an atomic view of all settings.
///
/// No file is opened for writing. `vm.stat_refresh` is never opened because
/// even reading it flushes per-CPU VM statistics. The write-only action files
/// `vm.compact_memory` and `vm.drop_caches` are also never opened.
///
/// Kernel files are under `/proc/sys/kernel`; filesystem files are
/// `/proc/sys/fs/{file-max,file-nr,nr_open,inode-nr,inode-state,dentry-state}`.
/// A dotted key such as `vm.swappiness` maps to `/proc/sys/vm/swappiness`.
/// [`Self::from_proc_root`] substitutes the proc root for captured inputs.
#[derive(Clone, Default, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct MemorySysctls {
    /// Parsed scalars, numeric arrays and named filesystem counter vectors.
    /// Keys use dotted sysctl names. Raw file contents are not retained.
    pub parsed: BTreeMap<String, crate::ParsedMemoryValue>,
    /// Failed reads or parses, including denied files and missing namespaces.
    ///
    /// Directory enumeration failures use the namespace (`vm`, `kernel`, or
    /// `fs`) as the key. Unsupported settings absent from a directory are omitted.
    pub unavailable: BTreeMap<String, SysctlReadError>,
    /// Present entries deliberately excluded from collection.
    pub skipped: BTreeMap<String, SysctlSkipReason>,
}

/// Why a memory sysctl or namespace could not be read.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde1", serde(rename_all = "snake_case"))]
pub enum SysctlReadError {
    /// Structured storage or scratch admission failed; no successful zero is implied.
    Allocation(crate::HeapError),
    /// The current user cannot read the file or enumerate the directory.
    PermissionDenied,
    /// The namespace is missing, or a file disappeared after enumeration.
    NotFound,
    /// Another I/O failure, including invalid UTF-8, with its diagnostic.
    ReadFailed(crate::MemoryText),
}

impl From<io::Error> for SysctlReadError {
    fn from(error: io::Error) -> Self {
        if let Some(error) = error.get_ref().and_then(|error| error.downcast_ref::<crate::HeapError>()) {
            return Self::Allocation(*error);
        }
        match error.kind() {
            io::ErrorKind::PermissionDenied => Self::PermissionDenied,
            io::ErrorKind::NotFound => Self::NotFound,
            _ => Self::read_failed(error.to_string()),
        }
    }
}

impl SysctlReadError {
    /// Retain a caller/OS diagnostic using fallible text admission. When that
    /// fails, retain an inline allocation failure instead of allocating an error.
    pub fn read_failed(message: impl AsRef<str>) -> Self {
        match crate::MemoryText::try_new(message.as_ref()) {
            Ok(message) => Self::ReadFailed(message),
            Err(error) => Self::Allocation(error),
        }
    }
}

impl std::fmt::Display for SysctlReadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Allocation(error) => std::fmt::Display::fmt(error, formatter),
            Self::PermissionDenied => formatter.write_str("permission denied"),
            Self::NotFound => formatter.write_str("source not found"),
            Self::ReadFailed(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for SysctlReadError {}

impl From<crate::HeapError> for SysctlReadError {
    fn from(error: crate::HeapError) -> Self {
        Self::Allocation(error)
    }
}

/// Why a present memory sysctl entry was not opened.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde1", serde(rename_all = "snake_case"))]
pub enum SysctlSkipReason {
    /// Reading the entry changes VM statistics (`vm.stat_refresh`).
    ReadSideEffect,
    /// A write-only action control, rather than a readable value.
    WriteOnly,
    /// A directory, symbolic link, or another non-regular entry.
    NotRegularFile,
}

impl MemorySysctls {
    /// Collect the current memory-related settings from `/proc/sys`.
    ///
    /// Privileges are not changed. Restricted settings are recorded in
    /// [`Self::unavailable`], and action controls in [`Self::skipped`].
    pub fn current() -> Self {
        Self::from_proc_root("/proc")
    }

    /// Collect from an alternate proc mount or directory of captured files.
    ///
    /// `root` is the proc root; settings are read under `root/sys/{vm,kernel,fs}`.
    /// The same exclusions apply to alternate roots, even when running as root.
    pub fn from_proc_root(root: impl AsRef<Path>) -> Self {
        let sys_root = root.as_ref().join("sys");
        let mut snapshot = Self::default();
        snapshot.read_namespace(&sys_root, "vm", |_| true);
        snapshot.read_namespace(&sys_root, "kernel", |name| {
            name.starts_with("shm") || name.starts_with("numa_balancing") || name == "perf_event_mlock_kb"
        });
        snapshot.read_namespace(&sys_root, "fs", |name| {
            matches!(name, "file-max" | "file-nr" | "inode-nr" | "inode-state" | "dentry-state" | "nr_open")
        });
        snapshot
    }

    fn read_namespace(&mut self, root: &Path, namespace: &str, include: impl Fn(&str) -> bool) {
        let entries = match fs::read_dir(root.join(namespace)) {
            Ok(entries) => entries,
            Err(error) => {
                self.unavailable.insert(namespace.into(), error.into());
                return;
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
                self.unavailable.insert(namespace.into(), error.into());
                break;
            }
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    self.unavailable.insert(namespace.into(), error.into());
                    continue;
                }
            };
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                self.unavailable.insert(namespace.into(), SysctlReadError::read_failed("non-UTF-8 sysctl name"));
                continue;
            };
            if !include(name) {
                continue;
            }
            let key = format!("{namespace}.{name}");
            // Apply these exclusions before metadata or read operations. In
            // particular, a privileged collector must never read stat_refresh.
            match key.as_str() {
                "vm.stat_refresh" => {
                    self.skipped.insert(key, SysctlSkipReason::ReadSideEffect);
                    continue;
                }
                "vm.compact_memory" | "vm.drop_caches" => {
                    self.skipped.insert(key, SysctlSkipReason::WriteOnly);
                    continue;
                }
                _ => {}
            }
            match self.read_value(&entry, &key) {
                Ok(Some(value)) => match crate::parse_sysctl_value(&key, &value) {
                    Ok(parsed) => {
                        self.parsed.insert(key, parsed);
                    }
                    Err(error) => {
                        self.unavailable.insert(key, error);
                    }
                },
                Ok(None) => {}
                Err(error) => {
                    self.unavailable.insert(key, error.into());
                }
            }
        }
    }

    fn read_value(&mut self, entry: &fs::DirEntry, key: &str) -> io::Result<Option<crate::files::ReadText>> {
        // DirEntry::metadata does not follow symbolic links.
        let metadata = entry.metadata()?;
        if !metadata.is_file() {
            self.skipped.insert(key.into(), SysctlSkipReason::NotRegularFile);
            return Ok(None);
        }
        let mode = metadata.permissions().mode();
        if mode & 0o444 == 0 && mode & 0o222 != 0 {
            self.skipped.insert(key.into(), SysctlSkipReason::WriteOnly);
            return Ok(None);
        }
        let value = crate::files::read_text(&entry.path(), 4 * 1024 * 1024)?;
        Ok(Some(value))
    }
}
