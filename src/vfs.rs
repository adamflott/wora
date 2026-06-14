use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use tokio::fs::{File, ReadDir};

use crate::errors::VfsError;

/// Virtual filesystem interface used by executors and applications.
///
/// Prefer the higher-level helpers such as `write`, `remove_file`, and
/// `list_dir` when writing new runtime code. They can be implemented by both
/// host-backed and fully virtual filesystems. The lower-level `tokio::fs`
/// handle methods are preserved for compatibility but may not be supported by
/// every virtual backend.
///
/// `exec_async_runner` still relies on native filesystem watchers through
/// `notify`, so fully virtual end-to-end runner tests are not yet available.
/// `InMemoryVFS` is most useful for targeted runtime helpers, executor hooks,
/// and application logic that only needs the higher-level operations.
#[async_trait]
pub trait WFS: Debug + Clone + Send + Sync {
    /// Construct a new filesystem handle.
    fn new() -> Self;

    /// Create a directory and any missing parents at `path`.
    async fn create_dir<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<(), VfsError>;

    /// Return direct children of `path`.
    async fn list_dir<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<Vec<PathBuf>, VfsError>;

    /// Remove a file if it exists.
    async fn remove_file<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<(), VfsError>;

    /// Write raw bytes to `path`, truncating any existing file.
    async fn write<P: AsRef<Path> + Send + Sync>(&self, path: P, data: &[u8]) -> Result<(), VfsError>;

    /// Open a directory stream for `path`.
    ///
    /// This method is primarily intended for host-backed implementations.
    async fn read_dir<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<ReadDir, VfsError>;

    /// Open an existing file for reading.
    ///
    /// This method is primarily intended for host-backed implementations.
    async fn open_file<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<File, VfsError>;

    /// Create or truncate a file for writing.
    ///
    /// This method is primarily intended for host-backed implementations.
    async fn create_file<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<File, VfsError>;

    /// Read a UTF-8 file into a string.
    async fn read_to_string<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<String, VfsError>;

    /// Read a file into raw bytes.
    async fn read<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<Vec<u8>, VfsError>;

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

    async fn list_dir<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<Vec<PathBuf>, VfsError> {
        let mut entries = tokio::fs::read_dir(path).await.map_err(VfsError::Io)?;
        let mut paths = Vec::new();
        while let Some(entry) = entries.next_entry().await.map_err(VfsError::Io)? {
            paths.push(entry.path());
        }
        Ok(paths)
    }

    async fn remove_file<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<(), VfsError> {
        tokio::fs::remove_file(path).await.map_err(VfsError::Io)
    }

    async fn write<P: AsRef<Path> + Send + Sync>(&self, path: P, data: &[u8]) -> Result<(), VfsError> {
        tokio::fs::write(path, data).await.map_err(VfsError::Io)
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

    async fn read<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<Vec<u8>, VfsError> {
        tokio::fs::read(path).await.map_err(VfsError::Io)
    }

    async fn dir_exists<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<bool, VfsError> {
        tokio::fs::try_exists(path).await.map_err(VfsError::Io)
    }
}

#[derive(Clone, Debug, Default)]
enum InMemoryNode {
    #[default]
    Directory,
    File(Vec<u8>),
}

#[derive(Clone, Debug, Default)]
struct InMemoryState {
    nodes: BTreeMap<PathBuf, InMemoryNode>,
}

impl InMemoryState {
    fn ensure_dir(&mut self, path: &Path) {
        let normalized = normalize_path(path);
        self.nodes.entry(normalized.clone()).or_insert(InMemoryNode::Directory);
        if let Some(parent) = normalized.parent() {
            if parent != normalized {
                self.ensure_dir(parent);
            }
        }
    }

    fn ensure_parent_dir(&self, path: &Path) -> Result<(), VfsError> {
        let Some(parent) = path.parent() else {
            return Ok(());
        };
        match self.nodes.get(parent) {
            Some(InMemoryNode::Directory) => Ok(()),
            Some(InMemoryNode::File(_)) => Err(VfsError::Io(std::io::Error::new(
                std::io::ErrorKind::NotADirectory,
                format!("parent path is a file: {}", parent.display()),
            ))),
            None => Err(VfsError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("missing parent directory: {}", parent.display()),
            ))),
        }
    }
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        normalized.push(component);
    }
    if normalized.as_os_str().is_empty() { PathBuf::from(".") } else { normalized }
}

/// Fully in-memory `WFS` implementation.
///
/// This implementation is intended for tests and pure virtual workflows that
/// do not require direct `tokio::fs::File` or `tokio::fs::ReadDir` handles.
/// Runtime code should prefer `list_dir`, `write`, `remove_file`, `read`, and
/// `read_to_string` so it can work with either `PhysicalVFS` or `InMemoryVFS`.
#[derive(Clone, Debug, Default)]
pub struct InMemoryVFS {
    state: Arc<RwLock<InMemoryState>>,
}

impl InMemoryVFS {
    fn with_state<T>(&self, f: impl FnOnce(&InMemoryState) -> Result<T, VfsError>) -> Result<T, VfsError> {
        let guard = self
            .state
            .read()
            .map_err(|_| VfsError::Io(std::io::Error::other("in-memory vfs read lock poisoned")))?;
        f(&guard)
    }

    fn with_state_mut<T>(&self, f: impl FnOnce(&mut InMemoryState) -> Result<T, VfsError>) -> Result<T, VfsError> {
        let mut guard = self
            .state
            .write()
            .map_err(|_| VfsError::Io(std::io::Error::other("in-memory vfs write lock poisoned")))?;
        f(&mut guard)
    }
}

#[async_trait]
impl WFS for InMemoryVFS {
    fn new() -> Self {
        let mut nodes = BTreeMap::new();
        nodes.insert(PathBuf::from("."), InMemoryNode::Directory);
        nodes.insert(PathBuf::from("/"), InMemoryNode::Directory);
        Self {
            state: Arc::new(RwLock::new(InMemoryState { nodes })),
        }
    }

    async fn create_dir<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<(), VfsError> {
        self.with_state_mut(|state| {
            let normalized = normalize_path(path.as_ref());
            if matches!(state.nodes.get(&normalized), Some(InMemoryNode::File(_))) {
                return Err(VfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("file already exists at {}", normalized.display()),
                )));
            }
            state.ensure_dir(&normalized);
            Ok(())
        })
    }

    async fn list_dir<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<Vec<PathBuf>, VfsError> {
        let dir = normalize_path(path.as_ref());
        self.with_state(|state| {
            match state.nodes.get(&dir) {
                Some(InMemoryNode::Directory) => {}
                Some(InMemoryNode::File(_)) => {
                    return Err(VfsError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotADirectory,
                        format!("path is a file: {}", dir.display()),
                    )));
                }
                None => {
                    return Err(VfsError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("directory not found: {}", dir.display()),
                    )));
                }
            }

            let mut children = BTreeSet::new();
            for candidate in state.nodes.keys() {
                if let Some(parent) = candidate.parent() {
                    if parent == dir && candidate != &dir {
                        children.insert(candidate.clone());
                    }
                }
            }
            Ok(children.into_iter().collect())
        })
    }

    async fn remove_file<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<(), VfsError> {
        let file_path = normalize_path(path.as_ref());
        self.with_state_mut(|state| match state.nodes.remove(&file_path) {
            Some(InMemoryNode::File(_)) => Ok(()),
            Some(InMemoryNode::Directory) => Err(VfsError::Io(std::io::Error::new(
                std::io::ErrorKind::IsADirectory,
                format!("path is a directory: {}", file_path.display()),
            ))),
            None => Err(VfsError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("file not found: {}", file_path.display()),
            ))),
        })
    }

    async fn write<P: AsRef<Path> + Send + Sync>(&self, path: P, data: &[u8]) -> Result<(), VfsError> {
        let file_path = normalize_path(path.as_ref());
        self.with_state_mut(|state| {
            state.ensure_parent_dir(&file_path)?;
            if matches!(state.nodes.get(&file_path), Some(InMemoryNode::Directory)) {
                return Err(VfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::IsADirectory,
                    format!("path is a directory: {}", file_path.display()),
                )));
            }
            state.nodes.insert(file_path, InMemoryNode::File(data.to_vec()));
            Ok(())
        })
    }

    async fn read_dir<P: AsRef<Path> + Send + Sync>(&self, _path: P) -> Result<ReadDir, VfsError> {
        Err(VfsError::UnsupportedOperation(
            "InMemoryVFS does not expose tokio::fs::ReadDir; use list_dir instead",
        ))
    }

    async fn open_file<P: AsRef<Path> + Send + Sync>(&self, _path: P) -> Result<File, VfsError> {
        Err(VfsError::UnsupportedOperation(
            "InMemoryVFS does not expose tokio::fs::File; use read or read_to_string instead",
        ))
    }

    async fn create_file<P: AsRef<Path> + Send + Sync>(&self, _path: P) -> Result<File, VfsError> {
        Err(VfsError::UnsupportedOperation("InMemoryVFS does not expose tokio::fs::File; use write instead"))
    }

    async fn read_to_string<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<String, VfsError> {
        let bytes = self.read(path).await?;
        String::from_utf8(bytes).map_err(|err| VfsError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, err)))
    }

    async fn read<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<Vec<u8>, VfsError> {
        let file_path = normalize_path(path.as_ref());
        self.with_state(|state| match state.nodes.get(&file_path) {
            Some(InMemoryNode::File(data)) => Ok(data.clone()),
            Some(InMemoryNode::Directory) => Err(VfsError::Io(std::io::Error::new(
                std::io::ErrorKind::IsADirectory,
                format!("path is a directory: {}", file_path.display()),
            ))),
            None => Err(VfsError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("file not found: {}", file_path.display()),
            ))),
        })
    }

    async fn dir_exists<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<bool, VfsError> {
        let normalized = normalize_path(path.as_ref());
        self.with_state(|state| Ok(state.nodes.contains_key(&normalized)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn in_memory_vfs_round_trips_files_and_directories() -> Result<(), Box<dyn std::error::Error>> {
        let fs = InMemoryVFS::new();
        fs.create_dir("/app/config").await?;
        fs.write("/app/config/demo.toml", b"enabled = true").await?;

        assert!(fs.dir_exists("/app/config").await?);
        assert_eq!(fs.read_to_string("/app/config/demo.toml").await?, "enabled = true");

        let entries = fs.list_dir("/app/config").await?;
        assert_eq!(entries, vec![PathBuf::from("/app/config/demo.toml")]);

        fs.remove_file("/app/config/demo.toml").await?;
        assert!(!fs.dir_exists("/app/config/demo.toml").await?);
        Ok(())
    }

    #[tokio::test]
    async fn in_memory_vfs_requires_existing_parent_directory_for_writes() {
        let fs = InMemoryVFS::new();
        let err = fs.write("/missing/demo.toml", b"hello").await.unwrap_err();
        match err {
            VfsError::Io(io) => assert_eq!(io.kind(), std::io::ErrorKind::NotFound),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn in_memory_vfs_reports_unsupported_low_level_handles() {
        let fs = InMemoryVFS::new();
        let err = match fs.create_file("/tmp/demo").await {
            Ok(_) => panic!("create_file unexpectedly succeeded"),
            Err(err) => err,
        };
        assert!(matches!(err, VfsError::UnsupportedOperation(_)));

        let err = match fs.read_dir("/tmp").await {
            Ok(_) => panic!("read_dir unexpectedly succeeded"),
            Err(err) => err,
        };
        assert!(matches!(err, VfsError::UnsupportedOperation(_)));
    }

    #[tokio::test]
    async fn physical_vfs_write_and_list_dir_helpers_work() -> Result<(), Box<dyn std::error::Error>> {
        let root = std::env::temp_dir().join(format!("wora-vfs-{}", std::process::id()));
        let file_path = root.join("nested").join("sample.txt");
        let fs = PhysicalVFS::new();

        let parent = file_path.parent().ok_or_else(|| std::io::Error::other("missing parent dir"))?;
        fs.create_dir(parent).await?;
        fs.write(&file_path, b"hello").await?;

        let entries = fs.list_dir(parent).await?;
        assert_eq!(entries, vec![file_path.clone()]);
        assert_eq!(fs.read_to_string(&file_path).await?, "hello");

        fs.remove_file(&file_path).await?;
        Ok(())
    }
}
