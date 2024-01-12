use std::path::PathBuf;

use async_trait::async_trait;
use directories::ProjectDirs;
use nix::unistd::chdir;
use tracing::trace;
use vfs::async_vfs::AsyncFileSystem;

use crate::dirs::Dirs;
use crate::errors::SetupFailure;
use crate::metrics::*;
use crate::AsyncExecutor;
use crate::Executor;
use crate::Wora;

#[derive(Debug)]
pub struct UnixLike {
    pub dirs: Dirs,
}

impl UnixLike {
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

#[async_trait]
impl MetricProcessor for UnixLike {
    async fn setup(&mut self) -> Result<(), SetupFailure> {
        Ok(())
    }
    async fn add(&mut self, _m: &dyn MetricEncoder) -> Result<(), SetupFailure> {
        Ok(())
    }
    async fn end(&self) {
        ()
    }
}

impl Executor for UnixLike {
    fn id(&self) -> &'static str {
        "unix-like"
    }
    fn dirs(&self) -> &Dirs {
        &self.dirs
    }
}

pub struct UnixLikeSystem {
    unix: UnixLike,
}

impl UnixLikeSystem {
    pub async fn new(app_name: &str) -> Self {
        let unix = UnixLike::new(app_name).await;
        UnixLikeSystem { unix }
    }
}

#[async_trait]
impl MetricProcessor for UnixLikeSystem {
    async fn setup(&mut self) -> Result<(), SetupFailure> {
        Ok(())
    }
    async fn add(&mut self, _m: &dyn MetricEncoder) -> Result<(), SetupFailure> {
        Ok(())
    }
    async fn end(&self) {
        ()
    }
}

impl Executor for UnixLikeSystem {
    fn id(&self) -> &'static str {
        "unix-like-system"
    }

    fn dirs(&self) -> &Dirs {
        &self.unix.dirs
    }
}

#[async_trait]
impl<T> AsyncExecutor<T> for UnixLikeSystem {
    type Setup = ();
    async fn setup(
        &mut self,
        _wora: &Wora<T>,
        _fs: &(dyn AsyncFileSystem),
        _metrics: &(dyn MetricProcessor + Send + Sync),
    ) -> Result<Self::Setup, SetupFailure> {
        Ok(())
    }

    async fn is_ready(
        &self,
        _wora: &Wora<T>,
        _metrics: &(dyn MetricProcessor + Send + Sync),
    ) -> bool {
        true
    }

    async fn end(&self, _wora: &Wora<T>, _metrics: &(dyn MetricProcessor + Send + Sync)) {
        ()
    }
}

pub struct UnixLikeUser {
    unix: UnixLike,
}

impl UnixLikeUser {
    pub async fn new(app_name: &str) -> Result<Self, std::io::Error> {
        let proj_dirs = ProjectDirs::from("com", "wora", app_name).unwrap();

        let dirs = Dirs {
            root_dir: PathBuf::from("/"),
            log_root_dir: proj_dirs.data_local_dir().to_path_buf(),
            metadata_root_dir: proj_dirs.config_dir().to_path_buf(),
            data_root_dir: proj_dirs.data_dir().to_path_buf(),
            runtime_root_dir: proj_dirs
                .runtime_dir()
                .unwrap_or(std::env::temp_dir().as_path())
                .to_path_buf(),
            cache_root_dir: proj_dirs.cache_dir().to_path_buf(),
            secrets_root_dir: proj_dirs.cache_dir().to_path_buf(),
        };

        std::fs::create_dir_all(&dirs.runtime_root_dir)?;
        std::fs::create_dir_all(&dirs.cache_root_dir)?;
        std::fs::create_dir_all(&dirs.data_root_dir)?;
        std::fs::create_dir_all(&dirs.metadata_root_dir)?;

        let mut unix = UnixLike::new(app_name).await;
        unix.dirs = dirs;

        Ok(UnixLikeUser { unix })
    }
}

#[async_trait]
impl MetricProcessor for UnixLikeUser {
    async fn setup(&mut self) -> Result<(), SetupFailure> {
        Ok(())
    }
    async fn add(&mut self, _m: &dyn MetricEncoder) -> Result<(), SetupFailure> {
        Ok(())
    }
    async fn end(&self) {
        ()
    }
}

impl Executor for UnixLikeUser {
    fn id(&self) -> &'static str {
        "unix-like-user"
    }
    fn dirs(&self) -> &Dirs {
        &self.unix.dirs
    }
}

#[async_trait]
impl<T: Send + Sync> AsyncExecutor<T> for UnixLikeUser {
    type Setup = ();
    async fn setup(
        &mut self,
        wora: &Wora<T>,
        _fs: &(dyn AsyncFileSystem),
        _metrics: &(dyn MetricProcessor + Send + Sync),
    ) -> Result<Self::Setup, SetupFailure> {
        let dirs = &wora.dirs;

        trace!("exec:setup:io:chdir({:?}): trying", &wora.dirs.root_dir);
        chdir(&wora.dirs.root_dir)?;
        trace!("exec:setup:io:chdir({:?}): success", &wora.dirs.root_dir);

        for dir in [
            &dirs.log_root_dir,
            &dirs.metadata_root_dir,
            &dirs.data_root_dir,
            &dirs.runtime_root_dir,
            &dirs.cache_root_dir,
        ] {
            trace!("exec:setup:io:create dir:{:?}: trying", dir);
            tokio::fs::create_dir_all(dir).await?;
            trace!("exec:setup:io:create dir:{:?}: success", dir);
        }

        Ok(())
    }

    async fn is_ready(
        &self,
        _wora: &Wora<T>,
        _metrics: &(dyn MetricProcessor + Send + Sync),
    ) -> bool {
        true
    }

    async fn end(&self, _wora: &Wora<T>, _metrics: &(dyn MetricProcessor + Send + Sync)) {
        ()
    }
}

pub struct UnixLikeBare {
    unix: UnixLike,
}

impl UnixLikeBare {
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
impl MetricProcessor for UnixLikeBare {
    async fn setup(&mut self) -> Result<(), SetupFailure> {
        Ok(())
    }
    async fn add(&mut self, _m: &dyn MetricEncoder) -> Result<(), SetupFailure> {
        Ok(())
    }
    async fn end(&self) {
        ()
    }
}

impl Executor for UnixLikeBare {
    fn id(&self) -> &'static str {
        "unix-like-bare"
    }
    fn dirs(&self) -> &Dirs {
        &self.unix.dirs
    }
}

#[async_trait]
impl<T> AsyncExecutor<T> for UnixLikeBare {
    type Setup = ();
    async fn setup(
        &mut self,
        _wora: &Wora<T>,
        _fs: &(dyn AsyncFileSystem),
        _metrics: &(dyn MetricProcessor + Send + Sync),
    ) -> Result<Self::Setup, SetupFailure> {
        Ok(())
    }

    async fn is_ready(
        &self,
        _wora: &Wora<T>,
        _metrics: &(dyn MetricProcessor + Send + Sync),
    ) -> bool {
        true
    }

    async fn end(&self, _wora: &Wora<T>, _metrics: &(dyn MetricProcessor + Send + Sync)) {
        ()
    }
}
