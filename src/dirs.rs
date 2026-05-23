use std::path::PathBuf;

/// Filesystem roots used by an executor.
///
/// Executors choose these paths based on the environment they target. For
/// example, `UnixLikeUser` maps them into the current user's platform-specific
/// project directories, while `UnixLikeSystem` uses system-level Unix paths.
#[derive(Clone, Debug)]
pub struct Dirs {
    /// Top-level root used as the process working directory during setup.
    pub root_dir: PathBuf,
    /// Directory for logs and observability output.
    pub log_root_dir: PathBuf,
    /// Directory for configuration and metadata watched by WORA.
    pub metadata_root_dir: PathBuf,
    /// Directory for durable application data.
    pub data_root_dir: PathBuf,
    /// Directory for runtime state such as lock files.
    pub runtime_root_dir: PathBuf,
    /// Directory for cache files.
    pub cache_root_dir: PathBuf,
    /// Directory for secrets or secret references.
    pub secrets_root_dir: PathBuf,
}
