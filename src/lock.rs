use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use proc_lock::try_lock;

use crate::errors::VfsError;

/// Error returned when a runtime lock cannot be acquired.
#[derive(Debug)]
pub enum LockError {
    /// Another workload instance already holds the requested lock path.
    AlreadyLocked(PathBuf),
    /// The backend failed while trying to acquire the lock.
    Io(std::io::Error),
}

impl fmt::Display for LockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LockError::AlreadyLocked(path) => write!(f, "lock already held: {}", path.display()),
            LockError::Io(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for LockError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LockError::AlreadyLocked(_) => None,
            LockError::Io(err) => Some(err),
        }
    }
}

impl From<std::io::Error> for LockError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

/// Backend used by the runner to serialize workload execution.
///
/// This is a supported public extension point. Backends are expected to:
///
/// - acquire locks in a non-blocking manner
/// - return [`LockError::AlreadyLocked`] when another process or runtime
///   instance already owns the lock
/// - release the lock when the returned guard is dropped
/// - make [`cleanup`](LockBackend::cleanup) idempotent
///
/// The built-in runner uses this trait through
/// `exec_async_runner_with_restart_options_and_lock_backend(...)`.
#[async_trait]
pub trait LockBackend: Clone + Send + Sync + 'static {
    /// Guard returned when a lock is acquired.
    type Guard: Send + 'static;

    /// Acquire a non-blocking lock for `path`.
    fn try_lock(&self, path: &Path) -> Result<Self::Guard, LockError>;

    /// Clean up any lock artifact after the workload exits.
    async fn cleanup(&self, path: &Path) -> Result<(), VfsError>;
}

/// Default filesystem-backed lock backend using `proc_lock`.
#[derive(Clone, Debug, Default)]
pub struct ProcLockBackend;

#[async_trait]
impl LockBackend for ProcLockBackend {
    type Guard = Box<dyn Send>;

    fn try_lock(&self, path: &Path) -> Result<Self::Guard, LockError> {
        match try_lock(&proc_lock::LockPath::FullPath(path)) {
            Ok(guard) => Ok(Box::new(guard)),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Err(LockError::AlreadyLocked(path.to_path_buf())),
            Err(err) => Err(LockError::Io(err)),
        }
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
///
/// This backend follows the same non-blocking semantics as
/// [`ProcLockBackend`], but the lock state is scoped to the cloned backend
/// instance graph rather than the host filesystem.
#[derive(Clone, Debug, Default)]
pub struct InMemoryLockBackend {
    locks: Arc<Mutex<HashSet<PathBuf>>>,
}

#[async_trait]
impl LockBackend for InMemoryLockBackend {
    type Guard = InMemoryGuard;

    fn try_lock(&self, path: &Path) -> Result<Self::Guard, LockError> {
        let mut guard = self
            .locks
            .lock()
            .map_err(|_| LockError::Io(std::io::Error::other("in-memory lock backend poisoned")))?;
        let normalized = path.to_path_buf();
        if !guard.insert(normalized.clone()) {
            return Err(LockError::AlreadyLocked(normalized));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_memory_lock_backend_reports_contention() {
        let backend = InMemoryLockBackend::default();
        let path = PathBuf::from("/runtime/example.lock");

        let _guard = match backend.try_lock(&path) {
            Ok(guard) => guard,
            Err(err) => panic!("first lock should succeed: {err}"),
        };
        let err = match backend.try_lock(&path) {
            Ok(_) => panic!("second lock should report contention"),
            Err(err) => err,
        };

        match err {
            LockError::AlreadyLocked(locked_path) => assert_eq!(locked_path, path),
            other => panic!("unexpected lock error: {other:?}"),
        }
    }

    #[test]
    fn in_memory_lock_backend_releases_on_guard_drop() {
        let backend = InMemoryLockBackend::default();
        let path = PathBuf::from("/runtime/example.lock");

        let guard = match backend.try_lock(&path) {
            Ok(guard) => guard,
            Err(err) => panic!("initial lock should succeed: {err}"),
        };
        drop(guard);

        if let Err(err) = backend.try_lock(&path) {
            panic!("lock should be reacquirable after drop: {err}");
        }
    }
}
