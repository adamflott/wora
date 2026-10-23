use std::collections::BTreeMap;
use std::io::BufRead;
use std::path::Path;

use crate::{FromBufRead, MemoryReadError, ProcError, ProcResult};

/// Process-scoped memory accounting, separate from host and cgroup totals.
///
/// Reads `/proc/self/{status,limits,stat,oom_score,oom_score_adj,smaps_rollup}`
/// by default. [`Self::from_process_root`] accepts another identified process
/// directory or captured files. `status`/rollup `kB` values mean KiB; `stat`
/// RSS remains base pages and `vsize` remains bytes. Limits keep their stated
/// units and `unlimited` values; OOM scores remain unitless.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct ProcessMemoryStats {
    /// Native status, limits, fault counters and OOM preferences; no settings changed.
    pub information: crate::MemoryFiles,
    /// Process whose rollup was requested.
    pub pid: u32,
    /// Summed mapping statistics, including RSS, PSS, private/shared memory and swap.
    pub smaps_rollup: Option<SmapsRollup>,
    /// Read/parse failures, keyed by `smaps_rollup`.
    pub unavailable: BTreeMap<String, MemoryReadError>,
}

impl ProcessMemoryStats {
    /// Read the current process's rollup synchronously.
    ///
    /// The kernel walks page tables; this can be expensive for large processes.
    /// Access to another process depends on procfs/ptrace policy.
    pub fn current() -> Self {
        Self::from_process_root(std::process::id(), "/proc/self")
    }

    /// Read an explicitly identified process directory or captured process files.
    ///
    /// `root` corresponds to `/proc/<pid>`, not the proc mount. The caller supplies
    /// the PID so captured data can retain its original process identity.
    pub fn from_process_root(pid: u32, root: impl AsRef<Path>) -> Self {
        let mut snapshot = Self {
            pid,
            information: crate::MemoryFiles::from_names(&root, &["status", "limits", "stat", "oom_score", "oom_score_adj"]),
            ..Self::default()
        };
        match crate::read_source(root.as_ref().join("smaps_rollup")) {
            Ok(rollup) => snapshot.smaps_rollup = Some(rollup),
            Err(error) => {
                let error = match error {
                    ProcError::PermissionDenied(_) => MemoryReadError::PermissionDenied,
                    ProcError::NotFound(_) => MemoryReadError::NotFound,
                    other => MemoryReadError::read_failed(other.to_string()),
                };
                snapshot.unavailable.insert("smaps_rollup".into(), error);
            }
        }
        snapshot
    }
}

/// `/proc/<pid>/smaps_rollup`, retaining unknown numeric fields and their units.
///
/// All currently documented memory amounts use `kB`, meaning KiB (1024 bytes).
/// Values remain in source units; use [`SmapsRollupValue::bytes`] for a checked
/// conversion. No base page size is assumed. The range is the kernel's rollup
/// address span, not one contiguous allocation or a virtual-memory size total.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct SmapsRollup {
    /// Start/end virtual addresses printed in the rollup header.
    pub address_range: (u64, u64),
    /// Named numeric fields such as `Pss`, `Private_Dirty`, `SwapPss`, and `Locked`.
    pub values: BTreeMap<String, SmapsRollupValue>,
}

/// One rollup value with its original kernel unit.
///
/// Parsed from a named numeric line in `/proc/<pid>/smaps_rollup` by
/// [`SmapsRollup`]. Its map key preserves the original accounting field name;
/// no value is computed from host or cgroup totals.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct SmapsRollupValue {
    /// Unsigned numeric value, without unit conversion.
    pub value: u64,
    /// Source unit, normally `kB`; absent for unitless fields.
    pub unit: Option<String>,
}

impl SmapsRollupValue {
    /// Convert a `kB` (KiB) memory amount to bytes without overflowing.
    ///
    /// Returns `None` for another unit, a unitless field, or overflow.
    pub fn bytes(&self) -> Option<u64> {
        (self.unit.as_deref() == Some("kB")).then(|| self.value.checked_mul(1024)).flatten()
    }
}

impl FromBufRead for SmapsRollup {
    fn from_buf_read<R: BufRead>(reader: R) -> ProcResult<Self> {
        let mut address_range = None;
        let mut values = BTreeMap::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            if address_range.is_none() {
                let fields: Vec<_> = line.split_ascii_whitespace().collect();
                if fields.len() != 6 || fields[5] != "[rollup]" {
                    return Err(invalid("expected rollup header"));
                }
                let (start, end) = fields[0].split_once('-').ok_or_else(|| invalid("invalid rollup address range"))?;
                let parse_address = |address: &str| {
                    if address.is_empty() || !address.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                        return Err(invalid("invalid virtual address"));
                    }
                    u64::from_str_radix(address, 16).map_err(|_| invalid("virtual address exceeds u64"))
                };
                let range = (parse_address(start)?, parse_address(end)?);
                if range.0 >= range.1 {
                    return Err(invalid("empty or reversed rollup range"));
                }
                address_range = Some(range);
                continue;
            }
            let (name, contents) = line.split_once(':').ok_or_else(|| invalid("missing rollup field separator"))?;
            if name.is_empty() || !name.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_') {
                return Err(invalid("invalid rollup field name"));
            }
            let fields: Vec<_> = contents.split_ascii_whitespace().collect();
            if !(1..=2).contains(&fields.len()) || !fields[0].bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(invalid("invalid rollup numeric field"));
            }
            let value = fields[0].parse::<u64>().map_err(|_| invalid("rollup value exceeds u64"))?;
            let value = SmapsRollupValue {
                value,
                unit: fields.get(1).map(|unit| (*unit).into()),
            };
            if values.insert(name.into(), value).is_some() {
                return Err(invalid("duplicate rollup field"));
            }
        }
        let address_range = address_range.ok_or_else(|| invalid("missing rollup header"))?;
        if !values.contains_key("Rss") || !values.contains_key("Pss") {
            return Err(invalid("missing RSS or PSS"));
        }
        Ok(Self { address_range, values })
    }
}

fn invalid(message: impl Into<String>) -> ProcError {
    ProcError::Other(format!("smaps_rollup: {}", message.into()))
}
