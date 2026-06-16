use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc::{Receiver, Sender, channel};

use crate::errors::VfsError;

/// Filesystem watch stream returned by `WFS::watch_dir`.
pub struct VfsWatcher {
    receiver: Receiver<notify::Result<notify::Event>>,
    _guard: WatchGuard,
}

enum WatchGuard {
    Native { _watcher: RecommendedWatcher },
    InMemory { state: Arc<RwLock<InMemoryState>>, id: u64 },
}

impl std::fmt::Debug for VfsWatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VfsWatcher").finish_non_exhaustive()
    }
}

impl VfsWatcher {
    /// Return a mutable receiver for filesystem watch events.
    pub fn receiver(&mut self) -> &mut Receiver<notify::Result<notify::Event>> {
        &mut self.receiver
    }
}

impl Drop for WatchGuard {
    fn drop(&mut self) {
        let WatchGuard::InMemory { state, id } = self else {
            return;
        };

        if let Ok(mut guard) = state.write() {
            guard.watchers.retain(|watch| watch.id != *id);
        }
    }
}

#[derive(Clone, Debug)]
struct InMemoryWatchRegistration {
    id: u64,
    root: PathBuf,
    sender: Sender<notify::Result<notify::Event>>,
}

/// Virtual filesystem interface used by executors and applications.
///
/// Prefer the higher-level helpers such as `write`, `remove_file`, `list_dir`,
/// and `watch_dir` when writing runtime code. The trait is intentionally
/// high-level so both host-backed and virtual filesystems can implement it
/// coherently.
///
/// `exec_async_runner` can now receive `notify`-shaped watch events from the
/// active VFS implementation. `InMemoryVFS` therefore supports runner-level
/// config and secret reload workflows. Fully virtual runner flows should pair
/// it with an in-memory lock backend through
/// `RunnerOptions::with_lock_backend(...)`.
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

    /// Watch a directory recursively for change events.
    async fn watch_dir<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<VfsWatcher, VfsError>;

    /// Read a UTF-8 file into a string.
    async fn read_to_string<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<String, VfsError>;

    /// Read a file into raw bytes.
    async fn read<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<Vec<u8>, VfsError>;

    /// Return whether `path` exists and is a directory.
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

    async fn watch_dir<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<VfsWatcher, VfsError> {
        let watch_path = path.as_ref().to_path_buf();
        let (tx, rx) = channel(8);
        let watcher = RecommendedWatcher::new(
            move |res| match tx.try_send(res) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    log::warn!("notify:watch:send dropped event because receiver channel is full");
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    log::debug!("notify:watch:send skipped event because receiver channel is closed");
                }
            },
            notify::Config::default(),
        )?;
        let mut watcher = watcher;
        watcher.watch(&watch_path, RecursiveMode::Recursive)?;
        Ok(VfsWatcher {
            receiver: rx,
            _guard: WatchGuard::Native { _watcher: watcher },
        })
    }

    async fn read_to_string<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<String, VfsError> {
        tokio::fs::read_to_string(path).await.map_err(VfsError::Io)
    }

    async fn read<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<Vec<u8>, VfsError> {
        tokio::fs::read(path).await.map_err(VfsError::Io)
    }

    async fn dir_exists<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<bool, VfsError> {
        match tokio::fs::metadata(path).await {
            Ok(metadata) => Ok(metadata.is_dir()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(err) => Err(VfsError::Io(err)),
        }
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
    watchers: Vec<InMemoryWatchRegistration>,
    next_watch_id: u64,
}

impl InMemoryState {
    fn ensure_dir(&mut self, path: &Path) {
        let normalized = normalize_path(path);
        self.nodes.entry(normalized.clone()).or_insert(InMemoryNode::Directory);
        if let Some(parent) = normalized.parent()
            && parent != normalized
        {
            self.ensure_dir(parent);
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
/// This implementation is intended for tests and pure virtual workflows.
/// Runtime code can target the same high-level interface for both
/// `PhysicalVFS` and `InMemoryVFS`.
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

    fn matching_watchers(state: &InMemoryState, path: &Path) -> Vec<Sender<notify::Result<notify::Event>>> {
        let normalized = normalize_path(path);
        state
            .watchers
            .iter()
            .filter(|watch| normalized.starts_with(&watch.root))
            .map(|watch| watch.sender.clone())
            .collect()
    }

    async fn emit_watch_event(&self, path: PathBuf, kind: notify::EventKind, watchers: Vec<Sender<notify::Result<notify::Event>>>) {
        if watchers.is_empty() {
            return;
        }

        let event = notify::Event::new(kind).add_path(path);
        for watcher in watchers {
            let _ = watcher.send(Ok(event.clone())).await;
        }
    }

    #[cfg(test)]
    fn watcher_count(&self) -> Result<usize, VfsError> {
        self.with_state(|state| Ok(state.watchers.len()))
    }
}

#[async_trait]
impl WFS for InMemoryVFS {
    fn new() -> Self {
        let mut nodes = BTreeMap::new();
        nodes.insert(PathBuf::from("."), InMemoryNode::Directory);
        nodes.insert(PathBuf::from("/"), InMemoryNode::Directory);
        Self {
            state: Arc::new(RwLock::new(InMemoryState {
                nodes,
                watchers: Vec::new(),
                next_watch_id: 1,
            })),
        }
    }

    async fn create_dir<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<(), VfsError> {
        let normalized = normalize_path(path.as_ref());
        let watchers = self.with_state_mut(|state| {
            if matches!(state.nodes.get(&normalized), Some(InMemoryNode::File(_))) {
                return Err(VfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("file already exists at {}", normalized.display()),
                )));
            }
            let existed = state.nodes.contains_key(&normalized);
            state.ensure_dir(&normalized);
            Ok(if existed { Vec::new() } else { Self::matching_watchers(state, &normalized) })
        })?;
        self.emit_watch_event(normalized, notify::EventKind::Create(notify::event::CreateKind::Folder), watchers)
            .await;
        Ok(())
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
                if let Some(parent) = candidate.parent()
                    && parent == dir
                    && candidate != &dir
                {
                    children.insert(candidate.clone());
                }
            }
            Ok(children.into_iter().collect())
        })
    }

    async fn remove_file<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<(), VfsError> {
        let file_path = normalize_path(path.as_ref());
        let watchers = self.with_state_mut(|state| match state.nodes.remove(&file_path) {
            Some(InMemoryNode::File(_)) => Ok(Self::matching_watchers(state, &file_path)),
            Some(InMemoryNode::Directory) => Err(VfsError::Io(std::io::Error::new(
                std::io::ErrorKind::IsADirectory,
                format!("path is a directory: {}", file_path.display()),
            ))),
            None => Err(VfsError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("file not found: {}", file_path.display()),
            ))),
        })?;
        self.emit_watch_event(file_path, notify::EventKind::Remove(notify::event::RemoveKind::File), watchers)
            .await;
        Ok(())
    }

    async fn write<P: AsRef<Path> + Send + Sync>(&self, path: P, data: &[u8]) -> Result<(), VfsError> {
        let file_path = normalize_path(path.as_ref());
        let (watchers, kind) = self.with_state_mut(|state| {
            state.ensure_parent_dir(&file_path)?;
            if matches!(state.nodes.get(&file_path), Some(InMemoryNode::Directory)) {
                return Err(VfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::IsADirectory,
                    format!("path is a directory: {}", file_path.display()),
                )));
            }
            let existed = matches!(state.nodes.get(&file_path), Some(InMemoryNode::File(_)));
            state.nodes.insert(file_path.clone(), InMemoryNode::File(data.to_vec()));
            let watchers = Self::matching_watchers(state, &file_path);
            let kind = if existed {
                notify::EventKind::Modify(notify::event::ModifyKind::Data(notify::event::DataChange::Content))
            } else {
                notify::EventKind::Create(notify::event::CreateKind::File)
            };
            Ok((watchers, kind))
        })?;
        self.emit_watch_event(file_path, kind, watchers).await;
        Ok(())
    }

    async fn watch_dir<P: AsRef<Path> + Send + Sync>(&self, path: P) -> Result<VfsWatcher, VfsError> {
        let root = normalize_path(path.as_ref());
        let (tx, rx) = channel(8);
        let watch_id = self.with_state_mut(|state| match state.nodes.get(&root) {
            Some(InMemoryNode::Directory) => {
                let watch_id = state.next_watch_id;
                state.next_watch_id += 1;
                state.watchers.push(InMemoryWatchRegistration {
                    id: watch_id,
                    root,
                    sender: tx,
                });
                Ok(watch_id)
            }
            Some(InMemoryNode::File(_)) => Err(VfsError::Io(std::io::Error::new(
                std::io::ErrorKind::NotADirectory,
                format!("path is a file: {}", path.as_ref().display()),
            ))),
            None => Err(VfsError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("directory not found: {}", path.as_ref().display()),
            ))),
        })?;
        Ok(VfsWatcher {
            receiver: rx,
            _guard: WatchGuard::InMemory {
                state: self.state.clone(),
                id: watch_id,
            },
        })
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
        self.with_state(|state| Ok(matches!(state.nodes.get(&normalized), Some(InMemoryNode::Directory))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn in_memory_vfs_round_trips_files_and_directories() -> Result<(), Box<dyn std::error::Error>> {
        let fs = InMemoryVFS::new();
        fs.create_dir("/app/config").await?;
        fs.write("/app/config/demo.toml", b"enabled = true").await?;

        assert!(fs.dir_exists("/app/config").await?);
        assert!(!fs.dir_exists("/app/config/demo.toml").await?);
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
        let err = match fs.write("/missing/demo.toml", b"hello").await {
            Ok(()) => panic!("write unexpectedly succeeded"),
            Err(err) => err,
        };
        match err {
            VfsError::Io(io) => assert_eq!(io.kind(), std::io::ErrorKind::NotFound),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn in_memory_vfs_emits_watch_events() -> Result<(), Box<dyn std::error::Error>> {
        let fs = InMemoryVFS::new();
        fs.create_dir("/app/config").await?;
        let mut watcher = fs.watch_dir("/app").await?;

        fs.write("/app/config/demo.toml", b"enabled = true").await?;

        let event = timeout(Duration::from_secs(1), watcher.receiver().recv())
            .await?
            .ok_or_else(|| std::io::Error::other("watcher closed"))??;
        assert_eq!(event.paths, vec![PathBuf::from("/app/config/demo.toml")]);
        Ok(())
    }

    #[tokio::test]
    async fn in_memory_vfs_deregisters_watchers_on_drop() -> Result<(), Box<dyn std::error::Error>> {
        let fs = InMemoryVFS::new();
        fs.create_dir("/app/config").await?;

        let watcher = fs.watch_dir("/app").await?;
        assert_eq!(fs.watcher_count()?, 1);

        drop(watcher);

        assert_eq!(fs.watcher_count()?, 0);
        Ok(())
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
        assert!(fs.dir_exists(parent).await?);
        assert!(!fs.dir_exists(&file_path).await?);
        assert_eq!(fs.read_to_string(&file_path).await?, "hello");

        fs.remove_file(&file_path).await?;
        Ok(())
    }
}
