use std::path::Path;

use crate::{MemoryFiles, MemoryReadError};

/// Existing host-wide DAMON_STAT results from `/sys/module/damon_stat/parameters`.
///
/// Requires `CONFIG_DAMON_STAT` (Linux 6.17+). Reads at most 4 KiB per parameter,
/// without enabling monitoring or changing its settings. Files are independent,
/// non-atomic observations: disabled monitoring can leave old result values.
/// Missing newer parameters remain per-file errors rather than zeroes.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct DamonStats {
    /// `enabled`: the module's `Y`/`N` monitor state at read time.
    pub enabled: Option<bool>,
    /// `estimated_memory_bandwidth`: sampled access-intensity estimate in bytes/s,
    /// not a hardware bandwidth measurement. No conversion is applied.
    pub estimated_memory_bandwidth: Option<u64>,
    /// `memory_idle_ms_percentiles`: exactly 101 signed millisecond values,
    /// indexed by percentile 0..=100. Since 6.19, negative values represent how
    /// long an active region kept its access-frequency level, multiplied by -1.
    pub memory_idle_ms_percentiles: Option<Vec<i64>>,
    /// `aggr_interval_us`: auto-tuned aggregation interval in microseconds (6.19+).
    pub aggr_interval_us: Option<u64>,
    /// `kdamond_pid`: worker PID, or -1 when disabled (7.2+).
    pub kdamond_pid: Option<i32>,
    /// Per-parameter errors and exclusions, relative to the parameters directory.
    /// Successful typed values are stored above; `files.parsed` stays empty.
    pub files: MemoryFiles,
}

impl DamonStats {
    /// Observe existing live DAMON_STAT state without enabling it.
    pub fn current() -> Self {
        Self::from_sys_root("/sys")
    }

    /// Read a supplied sysfs tree or capture, without claiming a fresh live sample.
    pub fn from_sys_root(root: impl AsRef<Path>) -> Self {
        let root = root.as_ref().join("module/damon_stat/parameters");
        let mut files = MemoryFiles::default();
        let enabled = files.read_with(&root, "enabled", 4096, |text| match text.trim() {
            "Y" => Ok(true),
            "N" => Ok(false),
            _ => Err(invalid()),
        });
        let estimated_memory_bandwidth = files.read_with(&root, "estimated_memory_bandwidth", 4096, unsigned);
        let memory_idle_ms_percentiles = files.read_with(&root, "memory_idle_ms_percentiles", 4096, |text| {
            let mut values = Vec::with_capacity(101);
            for value in text.trim().split(',') {
                if values.len() == 101 {
                    return Err(invalid());
                }
                values.push(signed(value)?);
            }
            if values.len() != 101 {
                return Err(invalid());
            }
            Ok(values)
        });
        let aggr_interval_us = files.read_with(&root, "aggr_interval_us", 4096, unsigned);
        let kdamond_pid = files.read_with(&root, "kdamond_pid", 4096, |text| i32::try_from(signed(text.trim())?).map_err(|_| invalid()));
        Self {
            enabled,
            estimated_memory_bandwidth,
            memory_idle_ms_percentiles,
            aggr_interval_us,
            kdamond_pid,
            files,
        }
    }
}

fn unsigned(text: &str) -> Result<u64, MemoryReadError> {
    let value = text.trim();
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid());
    }
    value.parse().map_err(|_| invalid())
}

fn signed(value: &str) -> Result<i64, MemoryReadError> {
    let digits = value.strip_prefix('-').unwrap_or(value);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid());
    }
    value.parse().map_err(|_| invalid())
}

fn invalid() -> MemoryReadError {
    MemoryReadError::read_failed("malformed DAMON_STAT parameter")
}
