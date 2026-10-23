use std::cell::RefCell;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Caller-configured allocation allowances, in bytes; not kernel measurements.
///
/// Retained storage and temporary scratch have independent allowances. Each
/// admitted allocation includes an explicit overhead reserve. That reserve is
/// an accounting policy, not a portable measurement of allocator metadata/RSS.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeapLimits {
    /// Aggregate live retained storage, including overlapping generations.
    pub retained_bytes: usize,
    /// Aggregate live temporary storage, including replacement buffers.
    pub scratch_bytes: usize,
    /// Additional reserved bytes per allocation, beyond its requested layout.
    pub allocation_overhead: usize,
}

impl Default for HeapLimits {
    fn default() -> Self {
        Self {
            retained_bytes: 64 * 1024 * 1024,
            scratch_bytes: 16 * 1024 * 1024,
            allocation_overhead: 64,
        }
    }
}

impl HeapLimits {
    /// Validate the policy before creating a budget or starting collection.
    pub fn validate(&self) -> Result<(), HeapError> {
        if self.retained_bytes == 0 || self.scratch_bytes == 0 {
            return Err(HeapError::InvalidLimits);
        }
        Ok(())
    }
}

/// Ownership class chosen by the collector, not a Linux memory-accounting scope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub enum HeapKind {
    /// Storage that can outlive collection, including shared snapshot contents.
    Retained,
    /// Temporary read, parse, copy or serialization storage.
    Scratch,
}

/// Live and peak reservation bytes from one WORA allocation budget.
///
/// Includes requested storage and configured per-allocation overhead. Does not
/// measure uninstrumented allocations, allocator-retained pages or process RSS.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HeapUsage {
    /// Currently reserved retained bytes.
    pub retained_bytes: usize,
    /// Highest simultaneous retained reservation since budget creation.
    pub retained_peak_bytes: usize,
    /// Currently reserved temporary bytes.
    pub scratch_bytes: usize,
    /// Highest simultaneous temporary reservation since budget creation.
    pub scratch_peak_bytes: usize,
}

#[derive(Debug, Default)]
struct Counter {
    live: AtomicUsize,
    peak: AtomicUsize,
}

#[derive(Debug)]
struct State {
    limits: HeapLimits,
    retained: Counter,
    scratch: Counter,
}

/// Shared admission for instrumented allocations across collectors and threads.
///
/// Clones share counters. Reserve before allocating and keep the returned lease
/// with the allocation until it is freed. Reservations are released on drop,
/// including failed construction and unwinding. Budget bookkeeping itself has a
/// fixed control allocation outside these counters.
#[derive(Clone, Debug)]
pub struct HeapBudget(Arc<State>);

impl Default for HeapBudget {
    fn default() -> Self {
        Self::from_valid_limits(HeapLimits::default())
    }
}

impl HeapBudget {
    fn from_valid_limits(limits: HeapLimits) -> Self {
        Self(Arc::new(State {
            limits,
            retained: Counter::default(),
            scratch: Counter::default(),
        }))
    }

    /// Create shared counters from caller-supplied limits; performs no OS reads.
    pub fn new(limits: HeapLimits) -> Result<Self, HeapError> {
        limits.validate()?;
        Ok(Self::from_valid_limits(limits))
    }

    /// The caller's policy, unchanged by allocation or collection.
    pub fn limits(&self) -> HeapLimits {
        self.0.limits
    }

    /// Inspect reservation counters. Concurrent fields are not one atomic sample.
    pub fn usage(&self) -> HeapUsage {
        HeapUsage {
            retained_bytes: self.0.retained.live.load(Ordering::Acquire),
            retained_peak_bytes: self.0.retained.peak.load(Ordering::Acquire),
            scratch_bytes: self.0.scratch.live.load(Ordering::Acquire),
            scratch_peak_bytes: self.0.scratch.peak.load(Ordering::Acquire),
        }
    }

    fn counter(&self, kind: HeapKind) -> (&Counter, usize) {
        match kind {
            HeapKind::Retained => (&self.0.retained, self.0.limits.retained_bytes),
            HeapKind::Scratch => (&self.0.scratch, self.0.limits.scratch_bytes),
        }
    }

    /// Reserve `bytes + allocations * allocation_overhead` before allocation.
    ///
    /// Arithmetic overflow and exhausted allowance refuse admission without
    /// changing counters. Reserve replacement capacity while the old lease lives.
    pub fn try_reserve(&self, kind: HeapKind, bytes: usize, allocations: usize) -> Result<HeapReservation, HeapError> {
        let bytes = allocations
            .checked_mul(self.0.limits.allocation_overhead)
            .and_then(|overhead| bytes.checked_add(overhead))
            .ok_or(HeapError::CapacityOverflow)?;
        let (counter, limit) = self.counter(kind);
        let previous = counter
            .live
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |live| {
                live.checked_add(bytes).filter(|next| *next <= limit)
            })
            .map_err(|_| {
                crate::budget::mark_exceeded(match kind {
                    HeapKind::Retained => crate::CollectionLimit::RetainedHeap,
                    HeapKind::Scratch => crate::CollectionLimit::ScratchHeap,
                });
                HeapError::Exhausted(kind)
            })?;
        counter.peak.fetch_max(previous + bytes, Ordering::AcqRel);
        Ok(HeapReservation {
            budget: self.clone(),
            kind,
            bytes,
        })
    }

    /// Clone the current synchronous scope's budget, or create a default budget.
    /// Without a scope, separate calls create independent allowances.
    pub fn current() -> Self {
        ACTIVE.with(|active| active.borrow().clone()).unwrap_or_default()
    }
}

/// Allocation credit owned by the storage it admits; dropping it returns bytes.
///
/// This lease is intentionally not `Clone`. Share the containing allocation to
/// share ownership; a new allocation requires a new reservation.
#[derive(Debug)]
pub struct HeapReservation {
    budget: HeapBudget,
    kind: HeapKind,
    bytes: usize,
}

impl HeapReservation {
    /// Shared budget that owns this reservation, not an OS memory source.
    pub fn budget(&self) -> &HeapBudget {
        &self.budget
    }

    /// Charged bytes, including the configured overhead reserve.
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for HeapReservation {
    fn drop(&mut self) {
        self.budget.counter(self.kind).0.live.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

/// Allocation admission/construction failure, independent of OS source errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub enum HeapError {
    /// A retained or scratch allowance was zero.
    InvalidLimits,
    /// The requested bytes would exceed the shared allowance.
    Exhausted(HeapKind),
    /// Capacity/layout arithmetic overflowed.
    CapacityOverflow,
    /// The allocator refused a fallible container reservation.
    AllocationFailed,
}

impl fmt::Display for HeapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimits => f.write_str("invalid memory heap limits"),
            Self::Exhausted(kind) => write!(f, "memory {kind:?} allocation budget exhausted"),
            Self::CapacityOverflow => f.write_str("memory allocation capacity overflow"),
            Self::AllocationFailed => f.write_str("memory allocation failed"),
        }
    }
}
impl std::error::Error for HeapError {}
impl From<HeapError> for std::io::Error {
    fn from(error: HeapError) -> Self {
        Self::other(error)
    }
}

thread_local! { static ACTIVE: RefCell<Option<HeapBudget>> = const { RefCell::new(None) }; }

struct Restore(Option<HeapBudget>);
impl Drop for Restore {
    fn drop(&mut self) {
        ACTIVE.with(|active| *active.borrow_mut() = self.0.take());
    }
}

/// Supply the allocation budget used by synchronous reader/parser builders.
///
/// Nested calls restore the previous budget even on unwind. Collected storage
/// retains its leases after this scope ends. This scope does not follow async
/// tasks or newly spawned threads; pass the shared budget to each worker.
pub fn with_heap_budget<T>(budget: &HeapBudget, collect: impl FnOnce() -> T) -> T {
    let previous = ACTIVE.with(|active| active.borrow_mut().replace(budget.clone()));
    let _restore = Restore(previous);
    collect()
}

pub(crate) fn with_default_budget<T>(collect: impl FnOnce() -> T) -> T {
    if ACTIVE.with(|active| active.borrow().is_some()) {
        collect()
    } else {
        with_heap_budget(&HeapBudget::default(), collect)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlap_is_reserved_and_released_on_failure_and_last_owner_drop() -> Result<(), HeapError> {
        let budget = HeapBudget::new(HeapLimits {
            retained_bytes: 100,
            scratch_bytes: 20,
            allocation_overhead: 4,
        })?;
        let old = Arc::new(budget.try_reserve(HeapKind::Retained, 56, 1)?);
        assert_eq!(budget.usage().retained_bytes, 60);
        assert!(matches!(
            budget.try_reserve(HeapKind::Retained, 40, 1),
            Err(HeapError::Exhausted(HeapKind::Retained))
        ));
        let replacement = budget.try_reserve(HeapKind::Retained, 36, 1)?;
        assert_eq!(budget.usage().retained_peak_bytes, 100);
        drop(replacement);
        let retained = old.clone();
        drop(old);
        assert_eq!(budget.usage().retained_bytes, 60);
        drop(retained);
        assert_eq!(budget.usage().retained_bytes, 0);
        let scratch = budget.try_reserve(HeapKind::Scratch, 16, 1)?;
        assert!(budget.try_reserve(HeapKind::Scratch, 1, 0).is_err());
        drop(scratch);
        assert_eq!(budget.usage().scratch_bytes, 0);
        assert_eq!(budget.try_reserve(HeapKind::Retained, usize::MAX, 1).err(), Some(HeapError::CapacityOverflow));
        Ok(())
    }

    #[test]
    fn nested_scopes_restore_the_budget_after_unwind() {
        let outer = HeapBudget::default();
        let inner = HeapBudget::default();
        with_heap_budget(&outer, || {
            let result = std::panic::catch_unwind(|| with_heap_budget(&inner, || panic!("collector failure")));
            assert!(result.is_err());
            assert!(Arc::ptr_eq(&HeapBudget::current().0, &outer.0));
        });
    }
}
