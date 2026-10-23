use std::fmt;
use std::ops::{Deref, Index};
use std::sync::Arc;

use crate::{HeapBudget, HeapError, HeapKind, HeapReservation};

fn layout<T>(capacity: usize) -> Result<usize, HeapError> {
    capacity.checked_mul(std::mem::size_of::<T>()).ok_or(HeapError::CapacityOverflow)
}

struct ListStorage<T> {
    values: Vec<T>,
    _reservation: HeapReservation,
}

/// Fallibly grown, shared list of parsed values; not an OS memory measurement.
///
/// Charges requested capacity, its shared control block and the configured
/// allocation reserve before growth. Cloning shares storage. Mutating a shared
/// list reserves a replacement while the old allocation remains charged. Only
/// the list storage is charged here; elements must account for their own heaps.
pub struct MemoryList<T> {
    data: Option<Arc<ListStorage<T>>>,
    budget: HeapBudget,
    kind: HeapKind,
}

impl<T> Clone for MemoryList<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            budget: self.budget.clone(),
            kind: self.kind,
        }
    }
}
impl<T> Default for MemoryList<T> {
    fn default() -> Self {
        Self::new(HeapBudget::current())
    }
}
impl<T> MemoryList<T> {
    /// Create empty storage in a caller-supplied budget, without a list allocation.
    pub fn new(budget: HeapBudget) -> Self {
        Self {
            data: None,
            budget,
            kind: HeapKind::Retained,
        }
    }

    /// Create temporary parser storage charged to the scratch allowance.
    pub fn scratch(budget: HeapBudget) -> Self {
        Self {
            data: None,
            budget,
            kind: HeapKind::Scratch,
        }
    }

    /// The budget owning this container's allocations.
    pub fn budget(&self) -> &HeapBudget {
        &self.budget
    }

    /// Requested element capacity currently retained by this list.
    pub fn capacity(&self) -> usize {
        self.data.as_ref().map_or(0, |data| data.values.capacity())
    }

    /// Release this handle's storage, retaining the same budget for later growth.
    pub fn clear(&mut self) {
        self.data = None;
    }
}
impl<T: Clone> MemoryList<T> {
    fn writable(&mut self, additional: usize) -> Result<&mut Vec<T>, HeapError> {
        let needed = self.len().checked_add(additional).ok_or(HeapError::CapacityOverflow)?;
        let unique = self
            .data
            .as_ref()
            .is_some_and(|data| Arc::strong_count(data) == 1 && Arc::weak_count(data) == 0);
        if !unique || needed > self.capacity() {
            let capacity = if std::mem::size_of::<T>() == 0 {
                needed
            } else if needed <= self.capacity() {
                self.capacity()
            } else {
                needed.max(self.capacity().checked_mul(2).ok_or(HeapError::CapacityOverflow)?).max(4)
            };
            let bytes = layout::<T>(capacity)?
                .checked_add(std::mem::size_of::<ListStorage<T>>() + 2 * std::mem::size_of::<usize>())
                .ok_or(HeapError::CapacityOverflow)?;
            let reservation = self.budget.try_reserve(self.kind, bytes, 2)?;
            let mut values = Vec::new();
            values.try_reserve_exact(capacity).map_err(|_| HeapError::AllocationFailed)?;
            // A nonstandard allocator must not leave extra reported capacity retained.
            if std::mem::size_of::<T>() != 0 && values.capacity() > capacity {
                return Err(HeapError::AllocationFailed);
            }
            values.extend(self.iter().cloned());
            self.data = Some(Arc::new(ListStorage {
                values,
                _reservation: reservation,
            }));
        }
        self.data
            .as_mut()
            .and_then(Arc::get_mut)
            .map(|data| &mut data.values)
            .ok_or(HeapError::AllocationFailed)
    }

    /// Admit capacity before appending; refusal leaves the list unchanged.
    pub fn try_push(&mut self, value: T) -> Result<(), HeapError> {
        self.writable(1)?.push(value);
        Ok(())
    }

    /// Obtain mutable element access, reserving a copy first when storage is shared.
    pub fn try_get_mut(&mut self, index: usize) -> Result<Option<&mut T>, HeapError> {
        if index >= self.len() {
            return Ok(None);
        }
        Ok(self.writable(0)?.get_mut(index))
    }

    fn try_insert(&mut self, index: usize, value: T) -> Result<(), HeapError> {
        self.writable(1)?.insert(index, value);
        Ok(())
    }

    fn try_replace(&mut self, index: usize, value: T) -> Result<T, HeapError> {
        Ok(std::mem::replace(&mut self.writable(0)?[index], value))
    }

    /// Build using the active synchronous budget, reserving before each growth.
    pub fn try_from_iter(values: impl IntoIterator<Item = T>) -> Result<Self, HeapError> {
        let mut result = Self::default();
        for value in values {
            result.try_push(value)?;
        }
        Ok(result)
    }
}

/// Owning iterator that shares storage and clones each returned element.
///
/// Creating the iterator does not allocate; element-owned clone behavior follows
/// `T`. Parsed values/text share their own storage.
pub struct MemoryListIntoIter<T> {
    values: MemoryList<T>,
    next: usize,
}
impl<T: Clone> Iterator for MemoryListIntoIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        let value = self.values.get(self.next)?.clone();
        self.next += 1;
        Some(value)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.values.len() - self.next;
        (len, Some(len))
    }
}
impl<T: Clone> ExactSizeIterator for MemoryListIntoIter<T> {}
impl<T: Clone> IntoIterator for MemoryList<T> {
    type Item = T;
    type IntoIter = MemoryListIntoIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        MemoryListIntoIter { values: self, next: 0 }
    }
}

pub(crate) trait CollectMemory: Iterator + Sized {
    fn collect_memory<T: Clone, E: From<HeapError>>(self) -> Result<MemoryList<T>, E>
    where
        Self: Iterator<Item = Result<T, E>>,
    {
        let mut result = MemoryList::default();
        for value in self {
            result.try_push(value?)?;
        }
        Ok(result)
    }
}
impl<I: Iterator> CollectMemory for I {}
impl<T> Deref for MemoryList<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        self.data.as_ref().map_or(&[], |data| data.values.as_slice())
    }
}
impl<T: fmt::Debug> fmt::Debug for MemoryList<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.deref().fmt(f)
    }
}
impl<T: PartialEq> PartialEq for MemoryList<T> {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}
impl<T: Eq> Eq for MemoryList<T> {}
impl<'a, T> IntoIterator for &'a MemoryList<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Shared, immutable text copied from a documented source or caller input.
///
/// Units/scope follow the containing parsed field. Clones share its String and
/// reservation; construction admits string capacity and shared control storage.
#[derive(Clone)]
pub struct MemoryText(Arc<TextStorage>);
struct TextStorage {
    value: String,
    _reservation: HeapReservation,
}
impl MemoryText {
    /// Copy text using the active synchronous budget; performs no OS reads.
    pub fn try_new(text: &str) -> Result<Self, HeapError> {
        Self::with_budget(text, &HeapBudget::current())
    }

    /// Copy text after reserving capacity in the specified budget.
    pub fn with_budget(text: &str, budget: &HeapBudget) -> Result<Self, HeapError> {
        let bytes = text
            .len()
            .checked_add(std::mem::size_of::<TextStorage>() + 2 * std::mem::size_of::<usize>())
            .ok_or(HeapError::CapacityOverflow)?;
        let reservation = budget.try_reserve(HeapKind::Retained, bytes, if text.is_empty() { 1 } else { 2 })?;
        let mut value = String::new();
        value.try_reserve_exact(text.len()).map_err(|_| HeapError::AllocationFailed)?;
        if value.capacity() > text.len() {
            return Err(HeapError::AllocationFailed);
        }
        value.push_str(text);
        Ok(Self(Arc::new(TextStorage {
            value,
            _reservation: reservation,
        })))
    }

    /// Borrow the original UTF-8 text without allocation.
    pub fn as_str(&self) -> &str {
        &self.0.value
    }
}
impl Deref for MemoryText {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}
impl AsRef<str> for MemoryText {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}
impl fmt::Debug for MemoryText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}
impl fmt::Display for MemoryText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
impl<T: AsRef<str>> PartialEq<T> for MemoryText {
    fn eq(&self, other: &T) -> bool {
        self.as_str() == other.as_ref()
    }
}
impl Eq for MemoryText {}

/// Sorted named storage with fallible key/capacity growth and shared clones.
///
/// Keys are source/field names; values retain their containing type's scope and
/// units. Unlike a tree map, requested entry capacity is explicit. Value-owned
/// heaps require their own reservations. Iteration and JSON keys are sorted.
#[derive(Clone)]
pub struct MemoryMap<V> {
    entries: MemoryList<(MemoryText, V)>,
}
impl<V> Default for MemoryMap<V> {
    fn default() -> Self {
        Self::new(HeapBudget::current())
    }
}
impl<V> MemoryMap<V> {
    /// Create an empty map in the specified allocation budget.
    pub fn new(budget: HeapBudget) -> Self {
        Self {
            entries: MemoryList::new(budget),
        }
    }
    /// The budget owning entry/key allocations.
    pub fn budget(&self) -> &HeapBudget {
        self.entries.budget()
    }
    /// Number of stored keys.
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    /// Whether the map has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    /// Borrow one value by key.
    pub fn get(&self, key: &str) -> Option<&V> {
        self.position(key).ok().map(|index| &self.entries[index].1)
    }
    /// Whether a source/field key is present.
    pub fn contains_key(&self, key: &str) -> bool {
        self.position(key).is_ok()
    }
    /// Borrow keys/values in lexical key order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&str, &V)> {
        self.entries.iter().map(|(key, value)| (key.as_str(), value))
    }
    /// Borrow keys in lexical order.
    pub fn keys(&self) -> impl ExactSizeIterator<Item = &str> {
        self.entries.iter().map(|(key, _)| key.as_str())
    }
    /// Borrow values in lexical key order.
    pub fn values(&self) -> impl ExactSizeIterator<Item = &V> {
        self.entries.iter().map(|(_, value)| value)
    }
    fn position(&self, key: &str) -> Result<usize, usize> {
        self.entries.binary_search_by(|(name, _)| name.as_str().cmp(key))
    }
}
impl<V: Clone> MemoryMap<V> {
    /// Reserve before insertion/copy-on-write; replacing a key returns its old value.
    pub fn try_insert(&mut self, key: impl AsRef<str>, value: V) -> Result<Option<V>, HeapError> {
        let key = key.as_ref();
        match self.position(key) {
            Ok(index) => {
                let key = self.entries[index].0.clone();
                self.entries.try_replace(index, (key, value)).map(|(_, value)| Some(value))
            }
            Err(index) => {
                let key = MemoryText::with_budget(key, self.budget())?;
                self.entries.try_insert(index, (key, value))?;
                Ok(None)
            }
        }
    }

    /// Build source/field entries using the active synchronous allocation budget.
    pub fn try_from_iter<K: AsRef<str>>(entries: impl IntoIterator<Item = (K, V)>) -> Result<Self, HeapError> {
        let mut result = Self::default();
        for (key, value) in entries {
            result.try_insert(key.as_ref(), value)?;
        }
        Ok(result)
    }
}
impl<V> Index<&str> for MemoryMap<V> {
    type Output = V;
    fn index(&self, key: &str) -> &V {
        &self.entries[self.position(key).unwrap_or(self.len())].1
    }
}
impl<V: fmt::Debug> fmt::Debug for MemoryMap<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}
impl<V: PartialEq> PartialEq for MemoryMap<V> {
    fn eq(&self, other: &Self) -> bool {
        self.entries == other.entries
    }
}
impl<V: Eq> Eq for MemoryMap<V> {}

#[cfg(feature = "serde1")]
mod serde_impl {
    use super::*;
    use serde::{
        Deserialize, Deserializer, Serialize, Serializer,
        de::{Error, MapAccess, SeqAccess, Visitor},
    };

    impl Serialize for MemoryText {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.serialize_str(self.as_str())
        }
    }
    impl<'de> Deserialize<'de> for MemoryText {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            struct TextVisitor;
            impl<'de> Visitor<'de> for TextVisitor {
                type Value = MemoryText;
                fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str("UTF-8 memory text")
                }
                fn visit_str<E: Error>(self, value: &str) -> Result<Self::Value, E> {
                    MemoryText::try_new(value).map_err(E::custom)
                }
            }
            deserializer.deserialize_str(TextVisitor)
        }
    }
    impl<T: Serialize> Serialize for MemoryList<T> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            self.deref().serialize(serializer)
        }
    }
    impl<'de, T: Deserialize<'de> + Clone> Deserialize<'de> for MemoryList<T> {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            struct ListVisitor<T>(std::marker::PhantomData<T>);
            impl<'de, T: Deserialize<'de> + Clone> Visitor<'de> for ListVisitor<T> {
                type Value = MemoryList<T>;
                fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str("memory values")
                }
                fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                    let mut values = MemoryList::default();
                    while let Some(value) = seq.next_element()? {
                        values.try_push(value).map_err(A::Error::custom)?;
                    }
                    Ok(values)
                }
            }
            deserializer.deserialize_seq(ListVisitor(std::marker::PhantomData))
        }
    }
    impl<V: Serialize> Serialize for MemoryMap<V> {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            serializer.collect_map(self.iter())
        }
    }
    impl<'de, V: Deserialize<'de> + Clone> Deserialize<'de> for MemoryMap<V> {
        fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            struct MapVisitor<V>(std::marker::PhantomData<V>);
            impl<'de, V: Deserialize<'de> + Clone> Visitor<'de> for MapVisitor<V> {
                type Value = MemoryMap<V>;
                fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.write_str("named memory values")
                }
                fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                    let mut values = MemoryMap::default();
                    while let Some((key, value)) = map.next_entry::<String, V>()? {
                        if values.try_insert(&key, value).map_err(A::Error::custom)?.is_some() {
                            return Err(A::Error::custom("duplicate memory key"));
                        }
                    }
                    Ok(values)
                }
            }
            deserializer.deserialize_map(MapVisitor(std::marker::PhantomData))
        }
    }
}
