use std::sync::{Arc, Weak};

/// Live shared memory generations and refreshes skipped because readers retained them.
///
/// Computed from WORA's provider-owned generation tracker, not procfs/sysfs.
/// Encoded sizes count serialized JSON payload bytes; they exclude heap/allocator
/// overhead and cannot be interpreted as process RSS or a whole-daemon memory bound.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MemorySamplingStatus {
    /// Live generations, not the number of handles referring to them.
    pub live_snapshots: usize,
    /// Refresh attempts skipped before collection because admission was full.
    pub skipped_samples: u64,
    /// JSON-encoded bytes in live admitted generations; excludes Rust container overhead.
    pub retained_snapshot_bytes: usize,
}

#[derive(Debug)]
pub(super) struct Generations<T> {
    // Drop the Weak before releasing header credit: a Weak keeps the Arc's
    // backing allocation alive even after its payload has been destroyed.
    live: Vec<(Weak<T>, usize, Option<Arc<super::linux_mm::HeapReservation>>)>,
    skipped: u64,
}

impl<T> Default for Generations<T> {
    fn default() -> Self {
        Self { live: Vec::new(), skipped: 0 }
    }
}

impl<T> Generations<T> {
    pub(super) fn admit(&mut self, limits: super::LinuxMemoryLimits) -> bool {
        self.live.retain(|(weak, _, _)| weak.strong_count() != 0);
        let used: usize = self.live.iter().map(|(_, bytes, _)| bytes).sum();
        if self.live.len() < limits.live_snapshots && limits.snapshot_bytes <= limits.retained_snapshot_bytes.saturating_sub(used) {
            return true;
        }
        self.skipped = self.skipped.saturating_add(1);
        false
    }

    pub(super) fn track(&mut self, data: &Arc<T>, bytes: usize, allocation: Option<Arc<super::linux_mm::HeapReservation>>) {
        self.live.push((Arc::downgrade(data), bytes, allocation));
    }

    pub(super) fn status(&self) -> MemorySamplingStatus {
        MemorySamplingStatus {
            live_snapshots: self.live.iter().filter(|(weak, _, _)| weak.strong_count() != 0).count(),
            skipped_samples: self.skipped,
            retained_snapshot_bytes: self
                .live
                .iter()
                .filter(|(weak, _, _)| weak.strong_count() != 0)
                .map(|(_, bytes, _)| bytes)
                .sum(),
        }
    }
}

/// Count encoded bytes without constructing an output buffer. The input/work
/// budget bounds construction; this separate limit controls published payloads.
pub(super) fn snapshot_size(value: &impl serde::Serialize, limit: usize) -> Result<usize, super::O11yError> {
    struct Counter {
        bytes: usize,
        limit: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.bytes = self
                .bytes
                .checked_add(bytes.len())
                .filter(|size| *size <= self.limit)
                .ok_or_else(|| std::io::Error::other("memory snapshot exceeds encoded byte limit"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { bytes: 0, limit };
    serde_json::to_writer(&mut counter, value).map_err(|error| super::O11yError::RuntimeEnvironment(error.to_string()))?;
    Ok(counter.bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_admission_reserves_the_replacement_before_collection() {
        let mut generations = Generations::default();
        let limits = super::super::LinuxMemoryLimits {
            snapshot_bytes: 8,
            retained_snapshot_bytes: 12,
            ..Default::default()
        };
        let first = Arc::new(1);
        assert!(generations.admit(limits));
        generations.track(&first, 7, None);
        assert!(!generations.admit(limits));
        assert_eq!(generations.status().retained_snapshot_bytes, 7);
        assert_eq!(generations.status().skipped_samples, 1);
        drop(first);
        assert!(generations.admit(limits));
        assert_eq!(generations.status().live_snapshots, 0);
        assert_eq!(generations.status().retained_snapshot_bytes, 0);
    }

    #[test]
    fn weak_generation_headers_keep_credit_until_pruned() -> Result<(), super::super::linux_mm::HeapError> {
        use super::super::linux_mm::{HeapBudget, HeapKind};
        let budget = HeapBudget::default();
        let allocation = Arc::new(budget.try_reserve(HeapKind::Retained, 128, 1)?);
        let mut generations = Generations::default();
        let data = Arc::new(1);
        generations.track(&data, 1, Some(allocation.clone()));
        drop((data, allocation));
        assert!(budget.usage().retained_bytes > 0);
        assert_eq!(generations.status().live_snapshots, 0);
        assert!(generations.admit(super::super::LinuxMemoryLimits::default()));
        assert_eq!(budget.usage().retained_bytes, 0);
        Ok(())
    }
}
