use crate::MemoryFiles;
use std::path::Path;

/// Kernel Samepage Merging accounting and scanner configuration.
///
/// Native page counts, byte profit estimates and milliseconds remain distinct.
/// Negative profit is retained. KSM statistics overlap other memory accounting.
///
/// Reads all regular, readable files directly under `/sys/kernel/mm/ksm` and
/// `/proc/self/{ksm_stat,ksm_merging_pages}`. [`Self::from_roots`] accepts explicit
/// sysfs/process roots; the `pid` is supplied metadata for those process files.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct KsmStats {
    /// All directly readable KSM sysfs files, including future counter names.
    pub global: MemoryFiles,
    /// PID associated with the process KSM files.
    pub pid: u32,
    /// Process-specific KSM counters and yes/no flags in `parsed["ksm_stat"]`.
    pub process: MemoryFiles,
}

impl KsmStats {
    /// Read host KSM and the collecting process's KSM accounting.
    pub fn current() -> Self {
        Self::from_roots("/sys", "/proc/self", std::process::id())
    }

    /// Read explicit sysfs and process roots, retaining the supplied PID.
    ///
    /// Only reads state. In particular `run` is never written, so this cannot
    /// enable merging, unmerge pages, or change the scanner's policy.
    pub fn from_roots(sys_root: impl AsRef<Path>, process_root: impl AsRef<Path>, pid: u32) -> Self {
        Self {
            global: MemoryFiles::directory(&sys_root.as_ref().join("kernel/mm/ksm"), |_| true),
            pid,
            process: MemoryFiles::from_names(process_root, &["ksm_stat", "ksm_merging_pages"]),
        }
    }
}
