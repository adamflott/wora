use std::collections::BTreeSet;
use std::io::BufRead;

use procfs_core::{FromBufRead, ProcError, ProcResult};

/// Active swap areas from `/proc/swaps`, in source order.
///
/// A header with no rows is a valid snapshot with no active swap. Implements
/// procfs's parsing traits and `Current` on Linux.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct Swaps {
    /// Active swap files and devices. Their paths are not canonicalized or opened.
    pub entries: Vec<SwapEntry>,
}

/// One active swap area. Sizes and usage are current gauges, not event counters.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct SwapEntry {
    /// UTF-8 backing path with the kernel's octal escapes decoded once.
    pub path: String,
    /// Backing object type, including names introduced by future kernels.
    pub swap_type: SwapType,
    /// Usable size in KiB (1024 bytes), independent of the kernel's page size.
    pub size_kib: u64,
    /// Currently used KiB (1024 bytes), independent of the kernel's page size.
    pub used_kib: u64,
    /// Signed kernel priority; automatically assigned priorities can be negative.
    pub priority: i32,
}

/// Backing type printed in the swap table.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde1", serde(rename_all = "snake_case"))]
pub enum SwapType {
    /// A regular swap file.
    File,
    /// A block device, including zram devices.
    Partition,
    /// An unfamiliar type, retained verbatim for forward compatibility.
    Other(String),
}

impl SwapType {
    /// The original kernel type name.
    pub fn as_str(&self) -> &str {
        match self {
            Self::File => "file",
            Self::Partition => "partition",
            Self::Other(name) => name,
        }
    }
}

fn invalid(line: usize, reason: impl std::fmt::Display) -> ProcError {
    ProcError::Other(format!("swaps line {line}: {reason}"))
}

fn unsigned(raw: &str, line: usize, field: &str) -> ProcResult<u64> {
    if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid(line, format!("invalid unsigned {field}: {raw:?}")));
    }
    raw.parse().map_err(|_| invalid(line, format!("{field} exceeds u64")))
}

fn decode_path(raw: &str, line: usize) -> ProcResult<String> {
    let mut decoded = Vec::with_capacity(raw.len());
    let mut bytes = raw.bytes();
    while let Some(byte) = bytes.next() {
        let byte = if byte == b'\\' {
            let mut value = 0_u16;
            for _ in 0..3 {
                let digit = bytes
                    .next()
                    .filter(|b| (b'0'..=b'7').contains(b))
                    .ok_or_else(|| invalid(line, "expected a three-digit octal path escape"))?;
                value = value * 8 + u16::from(digit - b'0');
            }
            u8::try_from(value).map_err(|_| invalid(line, "path escape exceeds one byte"))?
        } else {
            byte
        };
        if byte == 0 {
            return Err(invalid(line, "NUL in swap path"));
        }
        decoded.push(byte);
    }
    String::from_utf8(decoded).map_err(|_| invalid(line, "swap path is not UTF-8"))
}

impl FromBufRead for Swaps {
    fn from_buf_read<R: BufRead>(reader: R) -> ProcResult<Self> {
        let mut entries = Vec::new();
        let mut paths = BTreeSet::new();
        let mut header = false;
        for (offset, line) in reader.lines().enumerate() {
            let line = line?;
            let line_number = offset + 1;
            // The kernel escapes spaces/tabs/newlines/backslashes in paths.
            // Other whitespace (including Unicode whitespace) is a path byte.
            let fields: Vec<_> = line.split([' ', '\t']).filter(|s| !s.is_empty()).collect();
            if fields.is_empty() {
                continue;
            }
            if !header {
                if fields != ["Filename", "Type", "Size", "Used", "Priority"] {
                    return Err(invalid(line_number, "expected Filename Type Size Used Priority header"));
                }
                header = true;
                continue;
            }
            if fields.len() != 5 {
                return Err(invalid(line_number, "expected five swap columns"));
            }
            let path = decode_path(fields[0], line_number)?;
            if !paths.insert(path.clone()) {
                return Err(invalid(line_number, "duplicate swap path"));
            }
            let swap_type = match fields[1] {
                "file" => SwapType::File,
                "partition" => SwapType::Partition,
                other => SwapType::Other(other.into()),
            };
            let priority = fields[4];
            let magnitude = priority.strip_prefix('-').unwrap_or(priority);
            if magnitude.is_empty() || !magnitude.bytes().all(|b| b.is_ascii_digit()) {
                return Err(invalid(line_number, "invalid signed priority"));
            }
            let priority = priority.parse().map_err(|_| invalid(line_number, "priority exceeds i32"))?;
            entries.push(SwapEntry {
                path,
                swap_type,
                size_kib: unsigned(fields[2], line_number, "size")?,
                used_kib: unsigned(fields[3], line_number, "used")?,
                priority,
            });
        }
        if !header {
            return Err(ProcError::Other("swaps: missing header".into()));
        }
        Ok(Self { entries })
    }
}

#[cfg(target_os = "linux")]
impl procfs::Current for Swaps {
    const PATH: &'static str = "/proc/swaps";
}
