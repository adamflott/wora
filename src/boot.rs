use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::errors::VfsError;
use crate::vfs::WFS;

/// Explicit boot-state marker for a workload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BootState {
    FirstBoot,
    SubsequentBoot,
}

impl BootState {
    pub(crate) fn is_first_boot(self) -> bool {
        matches!(self, Self::FirstBoot)
    }
}

#[derive(Debug, Error)]
pub enum BootMarkerError {
    #[error("boot marker path is not a file: {0}")]
    InvalidType(PathBuf),
    #[error("boot marker filesystem error: {0}")]
    Vfs(#[from] VfsError),
}

pub(crate) async fn resolve_boot_state<F: WFS>(fs: F, boot_root: PathBuf, app_name: &str) -> Result<BootState, BootMarkerError> {
    fs.create_dir(&boot_root).await?;
    let marker_path = marker_path(&boot_root, app_name);

    match fs.read(&marker_path).await {
        Ok(_) => Ok(BootState::SubsequentBoot),
        Err(err) if is_not_found(&err) => {
            let now = chrono::Utc::now();
            fs.write(&marker_path, now.to_string().as_bytes())
                .await
                .map_err(|err| map_boot_marker_error(marker_path, err))?;
            Ok(BootState::FirstBoot)
        }
        Err(err) => Err(map_boot_marker_error(marker_path, err)),
    }
}

pub(crate) fn default_boot_root() -> PathBuf {
    PathBuf::from("/tmp")
}

fn marker_path(boot_root: &Path, app_name: &str) -> PathBuf {
    boot_root.join(format!(".{app_name}.booted"))
}

fn is_not_found(err: &VfsError) -> bool {
    matches!(err, VfsError::Io(io) if io.kind() == std::io::ErrorKind::NotFound)
}

fn map_boot_marker_error(marker_path: PathBuf, err: VfsError) -> BootMarkerError {
    match &err {
        VfsError::Io(io) if matches!(io.kind(), std::io::ErrorKind::IsADirectory | std::io::ErrorKind::NotADirectory) => {
            BootMarkerError::InvalidType(marker_path)
        }
        _ => BootMarkerError::Vfs(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vfs::InMemoryVFS;

    #[tokio::test]
    async fn resolve_boot_state_marks_first_boot_once() -> Result<(), Box<dyn std::error::Error>> {
        let fs = InMemoryVFS::new();
        let boot_root = PathBuf::from("/boot");

        assert_eq!(resolve_boot_state(fs.clone(), boot_root.clone(), "demo").await?, BootState::FirstBoot);
        assert_eq!(resolve_boot_state(fs, boot_root, "demo").await?, BootState::SubsequentBoot);
        Ok(())
    }

    #[tokio::test]
    async fn resolve_boot_state_rejects_directory_marker() -> Result<(), Box<dyn std::error::Error>> {
        let fs = InMemoryVFS::new();
        let boot_root = PathBuf::from("/boot");
        fs.create_dir(&boot_root).await?;
        fs.create_dir(boot_root.join(".demo.booted")).await?;

        let err = match resolve_boot_state(fs, boot_root, "demo").await {
            Ok(_) => panic!("directory marker should fail"),
            Err(err) => err,
        };
        match err {
            BootMarkerError::InvalidType(path) => assert_eq!(path, PathBuf::from("/boot/.demo.booted")),
            other => panic!("unexpected boot marker error: {other:?}"),
        }

        Ok(())
    }
}
