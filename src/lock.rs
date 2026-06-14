use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use proc_lock::try_lock;

use crate::errors::VfsError;

/// Backend used by the runner to serialize workload execution.
#[async_trait]
pub trait LockBackend: Clone + Send + Sync + 'static {
    /// Guard returned when a lock is acquired.
    type Guard: Send + 'static;

    /// Acquire a non-blocking lock for `path`.
    fn try_lock(&self, path: &Path) -> Result<Self::Guard, std::io::Error>;

    /// Clean up any lock artifact after the workload exits.
    async fn cleanup(&self, path: &Path) -> Result<(), VfsError>;
}

/// Default filesystem-backed lock backend using `proc_lock`.
#[derive(Clone, Debug, Default)]
pub struct ProcLockBackend;

#[async_trait]
impl LockBackend for ProcLockBackend {
    type Guard = Box<dyn Send>;

    fn try_lock(&self, path: &Path) -> Result<Self::Guard, std::io::Error> {
        Ok(Box::new(try_lock(&proc_lock::LockPath::FullPath(path))?))
    }

    async fn cleanup(&self, path: &Path) -> Result<(), VfsError> {
        match tokio::fs::remove_file(path).await {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(VfsError::Io(err)),
        }
    }
}

#[derive(Clone, Debug)]
#[doc(hidden)]
pub struct InMemoryGuard {
    path: PathBuf,
    locks: Arc<Mutex<HashSet<PathBuf>>>,
}

impl Drop for InMemoryGuard {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.locks.lock() {
            guard.remove(&self.path);
        }
    }
}

/// In-memory lock backend for tests and virtualized runner flows.
#[derive(Clone, Debug, Default)]
pub struct InMemoryLockBackend {
    locks: Arc<Mutex<HashSet<PathBuf>>>,
}

#[async_trait]
impl LockBackend for InMemoryLockBackend {
    type Guard = InMemoryGuard;

    fn try_lock(&self, path: &Path) -> Result<Self::Guard, std::io::Error> {
        let mut guard = self.locks.lock().map_err(|_| std::io::Error::other("in-memory lock backend poisoned"))?;
        let normalized = path.to_path_buf();
        if !guard.insert(normalized.clone()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("lock already held: {}", normalized.display()),
            ));
        }
        Ok(InMemoryGuard {
            path: normalized,
            locks: self.locks.clone(),
        })
    }

    async fn cleanup(&self, path: &Path) -> Result<(), VfsError> {
        let mut guard = self
            .locks
            .lock()
            .map_err(|_| VfsError::Io(std::io::Error::other("in-memory lock backend poisoned")))?;
        guard.remove(path);
        Ok(())
    }
}
