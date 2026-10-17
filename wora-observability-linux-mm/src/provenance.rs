use crate::MemoryFiles;
use std::path::Path;

/// Opt-in page allocation provenance from debugfs. Requires page-owner support
/// and normally `page_owner=on` at boot, plus privileged debugfs access.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct PageOwnerStats {
    /// Structured numeric settings and errors for unsupported stack/page dumps.
    pub files: MemoryFiles,
}

impl PageOwnerStats {
    /// Read existing page-owner tracking without enabling it or changing filters.
    pub fn current() -> Self {
        Self::from_debug_root("/sys/kernel/debug")
    }
    /// Read a supplied debugfs root with a 4 MiB per-file bound.
    pub fn from_debug_root(root: impl AsRef<Path>) -> Self {
        Self::from_debug_root_with_limit(root, 4 * 1024 * 1024)
    }
    /// Read with an explicit byte limit (1..=64 MiB). Oversized sources fail
    /// independently; no truncated stack dump is presented as complete.
    pub fn from_debug_root_with_limit(root: impl AsRef<Path>, limit: u64) -> Self {
        Self {
            files: MemoryFiles::from_names_with_limit(
                root,
                &[
                    "page_owner",
                    "page_owner_stacks/show_stacks",
                    "page_owner_stacks/show_handles",
                    "page_owner_stacks/show_stacks_handles",
                    "page_owner_stacks/count_threshold",
                ],
                limit,
            ),
        }
    }
}

/// Opt-in kernel leak suspects, requiring `CONFIG_DEBUG_KMEMLEAK` and enablement.
/// These are diagnostic findings, not proof of a leak or a complete leak census.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct KernelLeakStats {
    /// Read errors or an unsupported-layout error for unparsed suspect dumps.
    pub files: MemoryFiles,
}

impl KernelLeakStats {
    /// Read existing kmemleak findings. Reading can incur scanning work depending
    /// on the kernel; this must not be included in routine sampling implicitly.
    pub fn current() -> Self {
        Self::from_debug_root("/sys/kernel/debug")
    }
    /// Read a supplied debugfs root with a 4 MiB limit; never writes scan/clear/off.
    pub fn from_debug_root(root: impl AsRef<Path>) -> Self {
        Self::from_debug_root_with_limit(root, 4 * 1024 * 1024)
    }
    /// Read with an explicit byte limit (1..=64 MiB), failing rather than truncating.
    pub fn from_debug_root_with_limit(root: impl AsRef<Path>, limit: u64) -> Self {
        Self {
            files: MemoryFiles::from_names_with_limit(root, &["kmemleak"], limit),
        }
    }
}
