use std::fmt::Debug;
use std::path::Path;

use async_trait::async_trait;
use tokio::fs::ReadDir;

use crate::errors::VfsError;

#[async_trait]
pub trait WFS: Debug + Clone + Send + Sync {
    fn new() -> Self;
    async fn create_dir<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<(), VfsError>;
    async fn read_dir<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<ReadDir, VfsError>;
    async fn read_to_string<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<String, VfsError>;
}

#[derive(Debug, Clone)]
pub struct PhysicalVFS;

#[async_trait]
impl WFS for PhysicalVFS {
    fn new() -> Self {
        Self {}
    }

    async fn create_dir<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<(), VfsError> {
        tokio::fs::create_dir(path).await.map_err(VfsError::Io)
    }
    async fn read_dir<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<ReadDir, VfsError> {
        tokio::fs::read_dir(path).await.map_err(VfsError::Io)
    }

    async fn read_to_string<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<String, VfsError> {
        tokio::fs::read_to_string(path).await.map_err(VfsError::Io)
    }
}
