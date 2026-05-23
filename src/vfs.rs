use std::fmt::Debug;
use std::path::Path;

use async_trait::async_trait;
use tokio::fs::{File, ReadDir};

use crate::errors::VfsError;

/// Virtual filesystem interface used by executors and applications.
///
/// This trait keeps application lifecycle code decoupled from a specific
/// filesystem implementation. `PhysicalVFS` is the default implementation for
/// the host filesystem.
#[async_trait]
pub trait WFS: Debug + Clone + Send + Sync {
    /// Construct a new filesystem handle.
    fn new() -> Self;
    /// Create a directory and any missing parents at `path`.
    async fn create_dir<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<(), VfsError>;
    /// Read the entries in `path`.
    async fn read_dir<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<ReadDir, VfsError>;
    /// Open an existing file for reading.
    async fn open_file<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<File, VfsError>;
    /// Create or truncate a file for writing.
    async fn create_file<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<File, VfsError>;
    /// Read a UTF-8 file into a string.
    async fn read_to_string<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<String, VfsError>;
    /// Return whether `path` exists.
    async fn dir_exists<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<bool, VfsError>;
}

/// Host filesystem-backed `WFS` implementation.
#[derive(Debug, Clone)]
pub struct PhysicalVFS;

#[async_trait]
impl WFS for PhysicalVFS {
    fn new() -> Self {
        Self {}
    }

    async fn create_dir<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<(), VfsError> {
        tokio::fs::create_dir_all(path).await.map_err(VfsError::Io)
    }
    async fn read_dir<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<ReadDir, VfsError> {
        tokio::fs::read_dir(path).await.map_err(VfsError::Io)
    }

    async fn open_file<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<File, VfsError> {
        File::open(path).await.map_err(VfsError::Io)
    }
    async fn create_file<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<File, VfsError> {
        File::create(path).await.map_err(VfsError::Io)
    }
    async fn read_to_string<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<String, VfsError> {
        tokio::fs::read_to_string(path).await.map_err(VfsError::Io)
    }

    async fn dir_exists<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<bool, VfsError> {
        tokio::fs::try_exists(path).await.map_err(VfsError::Io)
    }
}
