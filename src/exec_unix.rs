use std::path::PathBuf;

use async_trait::async_trait;
use directories::ProjectDirs;
use nix::unistd::chdir;
use tracing::{error, trace};

use crate::Wora;
use crate::dirs::Dirs;
use crate::errors::{SetupFailure, VfsError};
use crate::{AsyncExecutor, WFS};

/// Shared Unix-like directory layout.
#[derive(Clone, Debug)]
pub struct UnixLike {
    /// Directories used by the executor.
    pub dirs: Dirs,
}

impl UnixLike {
    /// Build a system-style Unix layout.
    pub async fn new(_app_name: &str) -> Self {
        let dirs = Dirs {
            root_dir: PathBuf::from("/"),
            log_root_dir: PathBuf::from("/var/log"),
            metadata_root_dir: PathBuf::from("/etc/"),
            data_root_dir: PathBuf::from("/usr/local/data"),
            runtime_root_dir: PathBuf::from("/run/"),
            cache_root_dir: PathBuf::from("/var/run/"),
            secrets_root_dir: PathBuf::from("/var/run/"),
        };
        UnixLike { dirs }
    }
}

/// Executor for system-level Unix deployments.
///
/// This executor uses paths such as `/var/log`, `/etc`, and `/run` and expects
/// the process to have permission to use them.
#[derive(Clone, Debug)]
pub struct UnixLikeSystem {
    unix: UnixLike,
}

impl UnixLikeSystem {
    /// Create a system-level Unix executor.
    pub async fn new(app_name: &str) -> Self {
        let unix = UnixLike::new(app_name).await;
        UnixLikeSystem { unix }
    }
}

#[async_trait]
impl<AppEv, AppMetric> AsyncExecutor<AppEv, AppMetric> for UnixLikeSystem {
    fn id(&self) -> &'static str {
        "unix-like-system"
    }
    fn dirs(&self) -> &Dirs {
        &self.unix.dirs
    }

    async fn setup(&mut self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) -> Result<(), SetupFailure> {
        Ok(())
    }

    async fn is_ready(&self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) -> bool {
        true
    }

    async fn end(&self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) {}
}

/// Executor for per-user Unix deployments.
///
/// Directories are derived from `directories::ProjectDirs`, making this the
/// safer default for examples and local development.
#[derive(Clone, Debug)]
pub struct UnixLikeUser {
    unix: UnixLike,
}

impl UnixLikeUser {
    /// Create a per-user Unix executor for `app_name`.
    pub async fn new(app_name: &str, _fs: impl WFS) -> Result<Self, VfsError> {
        let proj_dirs = ProjectDirs::from("com", "wora", app_name).unwrap();

        let dirs = Dirs {
            root_dir: PathBuf::from("/"),
            log_root_dir: proj_dirs.data_local_dir().to_path_buf(),
            metadata_root_dir: proj_dirs.config_dir().to_path_buf(),
            data_root_dir: proj_dirs.data_dir().to_path_buf(),
            runtime_root_dir: proj_dirs.runtime_dir().unwrap_or(std::env::temp_dir().as_path()).to_path_buf(),
            cache_root_dir: proj_dirs.cache_dir().to_path_buf(),
            secrets_root_dir: proj_dirs.cache_dir().to_path_buf(),
        };

        let mut unix = UnixLike::new(app_name).await;
        unix.dirs = dirs;

        Ok(UnixLikeUser { unix })
    }
}

#[async_trait]
impl<AppEv: Send + Sync, AppMetric: Send + Sync> AsyncExecutor<AppEv, AppMetric> for UnixLikeUser {
    fn id(&self) -> &'static str {
        "unix-like-user"
    }
    fn dirs(&self) -> &Dirs {
        &self.unix.dirs
    }

    async fn setup(&mut self, wora: &Wora<AppEv, AppMetric>, fs: impl WFS) -> Result<(), SetupFailure> {
        let dirs = &wora.dirs;

        trace!("exec:setup:io:chdir({:?}): trying", &wora.dirs.root_dir);
        chdir(&wora.dirs.root_dir)?;
        trace!("exec:setup:io:chdir({:?}): success", &wora.dirs.root_dir);

        for dir in [
            &dirs.root_dir,
            &dirs.log_root_dir,
            &dirs.metadata_root_dir,
            &dirs.data_root_dir,
            &dirs.runtime_root_dir,
            &dirs.cache_root_dir,
        ] {
            trace!("exec:setup:io:create dir:{:?}: trying", dir);
            let _ = fs.create_dir(dir.to_str().unwrap()).await.map_err(|err| {
                error!("exec:setup:io:create dir:{:?}: error:{}", dir, err);
                // directory may already exist, in which case this is not a terminating error
            });
            trace!("exec:setup:io:create dir:{:?}: success", dir);
        }

        Ok(())
    }

    async fn is_ready(&self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) -> bool {
        true
    }

    async fn end(&self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) {}
}

/// Executor that maps every directory to `/tmp`.
///
/// Useful for smoke tests or constrained environments where the normal system
/// and user directory layouts are not available.
#[derive(Clone, Debug)]
pub struct UnixLikeBare {
    unix: UnixLike,
}

impl UnixLikeBare {
    /// Create a bare Unix executor.
    pub async fn new(app_name: &str) -> Self {
        let tmp = PathBuf::from("/tmp");
        let dirs = Dirs {
            root_dir: tmp.clone(),
            log_root_dir: tmp.clone(),
            metadata_root_dir: tmp.clone(),
            data_root_dir: tmp.clone(),
            runtime_root_dir: tmp.clone(),
            cache_root_dir: tmp.clone(),
            secrets_root_dir: tmp.clone(),
        };

        let mut unix = UnixLike::new(app_name).await;
        unix.dirs = dirs;

        UnixLikeBare { unix }
    }
}

#[async_trait]
impl<AppEv, AppMetric> AsyncExecutor<AppEv, AppMetric> for UnixLikeBare {
    fn id(&self) -> &'static str {
        "unix-like-bare"
    }

    fn dirs(&self) -> &Dirs {
        &self.unix.dirs
    }

    async fn setup(&mut self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) -> Result<(), SetupFailure> {
        Ok(())
    }

    async fn is_ready(&self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) -> bool {
        true
    }

    async fn end(&self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) {}
}
