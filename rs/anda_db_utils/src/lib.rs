//! Standalone utility types maintained alongside the AndaDB workspace.
//!
//! The crate intentionally stays small and dependency-light. It currently
//! provides:
//!
//! - [`UniqueVec`], an insertion-ordered vector that rejects duplicates.
//! - [`CountingWriter`], a writer that counts serialized bytes without storing
//!   the payload.
//! - [`Pipe`], a small functional-style chaining trait.
//!
//! # Hashing
//!
//! Hash-based structures in this crate use `rustc-hash` (FxHash) without a
//! random seed, matching the rest of the AndaDB workspace. FxHash has no
//! collision resistance: do not use these types where an adversary controls
//! the hashed keys and hash-flooding (O(n²) degradation) is a concern.

use core::ops::Deref;
use rustc_hash::{FxBuildHasher, FxHashSet};
use serde::{
    de::{Deserialize, Deserializer},
    ser::{Serialize, Serializer},
};
use std::{borrow::Borrow, hash::Hash};

/// A trait for functional-style method chaining.
///
/// Allows any value to be passed through a function, enabling
/// fluent interfaces and functional programming patterns.
pub trait Pipe<T> {
    /// Passes the value through a function.
    ///
    /// # Arguments
    ///
    /// * `f` - Function to apply to the value
    ///
    /// # Returns
    ///
    /// The result of applying the function to the value
    fn pipe<F, R>(self, f: F) -> R
    where
        F: FnOnce(Self) -> R,
        Self: Sized;
}

impl<T> Pipe<T> for T {
    fn pipe<F, R>(self, f: F) -> R
    where
        F: FnOnce(Self) -> R,
    {
        f(self)
    }
}

/// A helper utility to efficiently push or extend a `Vec` with unique items.
///
/// This struct maintains an internal `HashSet` to keep track of existing items,
/// providing an optimized way to perform multiple non-existent insertions.
/// It is designed to be used with a `Vec` that it helps manage.
///
/// # Memory cost
///
/// Every element is stored **twice** — once in the ordered `Vec` and once in
/// the membership `HashSet` — trading roughly 2x memory for O(1) duplicate
/// checks. For large owned elements (e.g. long `String` keys) this doubles
/// the payload memory; callers holding many large collections should weigh
/// this against a plain `Vec` with linear-scan deduplication.
///
/// # Examples
///
/// ```rust
/// use anda_db_utils::UniqueVec;
///
/// let vec = vec![1, 2, 3];
/// let mut extender = UniqueVec::from(vec);
///
/// // Push an item that already exists (no change)
/// extender.push(2);
/// assert_eq!(extender.as_ref(), &[1, 2, 3]);
///
/// // Push a new item
/// extender.push(4);
/// assert_eq!(extender.as_ref(), &[1, 2, 3, 4]);
///
/// // Extend with a list of items
/// extender.extend(vec![3, 5, 6]);
/// assert_eq!(extender.as_ref(), &[1, 2, 3, 4, 5, 6]);
/// ```
#[derive(Clone, Debug)]
pub struct UniqueVec<T> {
    set: FxHashSet<T>,
    vec: Vec<T>,
}

struct UniqueVecSetRebuildGuard<'a, T>
where
    T: Eq + Hash + Clone,
{
    set: &'a mut FxHashSet<T>,
    vec: &'a mut Vec<T>,
}

impl<T> Drop for UniqueVecSetRebuildGuard<'_, T>
where
    T: Eq + Hash + Clone,
{
    fn drop(&mut self) {
        // Retain-style edits can only delete elements, never duplicate them, so
        // equal lengths imply the set still mirrors the vec exactly. Rebuild only
        // on divergence (e.g. a panicking predicate or an inconsistent Hash/Eq
        // implementation interrupted the incremental set maintenance).
        if self.set.len() != self.vec.len() {
            self.set.clear();
            self.set.extend(self.vec.iter().cloned());
        }
    }
}

impl<T> Default for UniqueVec<T> {
    /// Creates an empty `UniqueVec`.
    fn default() -> Self {
        Self {
            set: FxHashSet::default(),
            vec: Vec::new(),
        }
    }
}

// A small all-unique sample can justify bounded preallocation, never trusting
// an arbitrarily large input hint. Each container reserves at most this many
// element bytes (HashSet bucket rounding/control bytes add overhead).
const CONSTRUCTION_SAMPLE_LEN: usize = 256;
const CONSTRUCTION_RESERVE_BYTES: usize = 1024 * 1024;
const SPARSE_CAPACITY_FACTOR: usize = 4;
const MIN_SPARSE_CAPACITY: usize = 64;

fn construction_reserve<T>(hint: usize) -> usize {
    hint.min(CONSTRUCTION_RESERVE_BYTES / std::mem::size_of::<T>().max(1))
}

impl<T> From<Vec<T>> for UniqueVec<T>
where
    T: Eq + Hash + Clone,
{
    /// Creates a `UniqueVec` from a `Vec`.
    ///
    /// The extender is initialized with all the unique items from the vector.
    fn from(mut vec: Vec<T>) -> Self {
        // The input length says nothing about the number of distinct values.
        // Grow the set with the unique population and avoid cloning duplicates.
        let input_len = vec.len();
        let mut seen = 0usize;
        let mut check_duplicates = std::mem::needs_drop::<T>();
        let mut set = FxHashSet::default();
        vec.retain(|item| {
            seen += 1;
            // Cheap, non-dropping values can use a single hash probe; cloning
            // owned payloads is avoided for duplicates.
            let inserted = (!check_duplicates || !set.contains(item)) && set.insert(item.clone());
            check_duplicates |= !inserted;
            if seen == CONSTRUCTION_SAMPLE_LEN && set.len() == CONSTRUCTION_SAMPLE_LEN {
                set.reserve(construction_reserve::<T>(input_len - seen));
            }
            inserted
        });
        let mut result = Self { set, vec };
        result.compact_sparse();
        result
    }
}

impl<T> FromIterator<T> for UniqueVec<T>
where
    T: Eq + Hash + Clone,
{
    /// Creates a `UniqueVec` from an iterator.
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut result = Self::default();
        let mut iter = iter.into_iter();
        let mut seen = 0;
        let mut check_duplicates = std::mem::needs_drop::<T>();
        while let Some(item) = iter.next() {
            check_duplicates |= !result.push_constructing(item, check_duplicates);
            seen += 1;
            if seen == CONSTRUCTION_SAMPLE_LEN && result.len() == CONSTRUCTION_SAMPLE_LEN {
                let reserve = construction_reserve::<T>(iter.size_hint().0);
                result.vec.reserve(reserve);
                result.set.reserve(reserve);
            }
        }
        result.compact_sparse();
        result
    }
}

impl<T> From<UniqueVec<T>> for Vec<T> {
    /// Converts a `UniqueVec` into a `Vec`.
    fn from(extender: UniqueVec<T>) -> Self {
        extender.vec
    }
}

impl<T> AsRef<[T]> for UniqueVec<T> {
    /// Returns a slice containing the entire vector.
    fn as_ref(&self) -> &[T] {
        &self.vec
    }
}

impl<T> Deref for UniqueVec<T> {
    type Target = Vec<T>;

    /// Dereferences the `UniqueVec` to a `Vec`.
    fn deref(&self) -> &Self::Target {
        &self.vec
    }
}

impl<T> UniqueVec<T>
where
    T: Eq + Hash + Clone,
{
    // Only used while constructing an unpublished value. If clone/hash or
    // allocation panics, the entire local result is dropped; public mutations
    // continue to use push's rollback guard.
    fn push_constructing(&mut self, item: T, check_duplicates: bool) -> bool {
        if check_duplicates && self.set.contains(&item) {
            return false;
        }
        if self.set.insert(item.clone()) {
            self.vec.push(item);
            true
        } else {
            false
        }
    }

    fn compact_sparse(&mut self) {
        let threshold = self
            .vec
            .len()
            .saturating_mul(SPARSE_CAPACITY_FACTOR)
            .max(MIN_SPARSE_CAPACITY);
        if self.vec.capacity() > threshold {
            self.vec.shrink_to_fit();
        }
        if self.set.capacity() > threshold {
            self.set.shrink_to_fit();
        }
    }

    /// Creates a new, empty `UniqueVec`.
    pub fn new() -> Self {
        UniqueVec::default()
    }

    /// Creates a new, empty `UniqueVec` with a specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        UniqueVec {
            set: FxHashSet::with_capacity_and_hasher(capacity, FxBuildHasher),
            vec: Vec::with_capacity(capacity),
        }
    }

    /// Returns `true` if the `UniqueVec` contains the specified item.
    pub fn contains<Q>(&self, item: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.set.contains(item)
    }

    /// Pushes an item to the vector if it does not already exist.
    ///
    /// # Arguments
    ///
    /// * `item` - The item to add.
    ///
    /// # Returns
    ///
    /// `true` if the item was added, `false` otherwise.
    ///
    /// # Panic safety
    ///
    /// The set/vec invariant (`set` mirrors `vec` exactly) holds even if any
    /// step unwinds:
    ///
    /// * `contains` / `clone` panic — nothing has been mutated yet.
    /// * `vec.push` panics (capacity overflow) — `Vec::push` leaves the vector
    ///   unchanged on panic and the set has not been touched.
    /// * `set.insert` panics (a custom `Hash` impl may panic) — a drop guard
    ///   pops the element that was just pushed onto the vector. Without the
    ///   guard the vector would keep an element the set does not know about,
    ///   and a later `push` of an equal element would insert a duplicate.
    pub fn push(&mut self, item: T) -> bool {
        // Membership test first: duplicates are rejected without paying for a
        // clone.
        if self.set.contains(&item) {
            return false;
        }

        struct VecRollbackGuard<'a, T> {
            vec: &'a mut Vec<T>,
            armed: bool,
        }
        impl<T> Drop for VecRollbackGuard<'_, T> {
            fn drop(&mut self) {
                if self.armed {
                    self.vec.pop();
                }
            }
        }

        self.vec.push(item.clone());
        let mut guard = VecRollbackGuard {
            vec: &mut self.vec,
            armed: true,
        };
        let inserted = self.set.insert(item);
        // Defuse only when the set accepted the element. `inserted == false`
        // means an inconsistent `Hash`/`Eq` implementation disagreed with the
        // `contains` probe above; keep set and vec in agreement by letting the
        // guard pop the vector copy.
        guard.armed = !inserted;
        drop(guard);
        inserted
    }

    /// Extends the vector with items from an iterator that do not already exist.
    ///
    /// # Arguments
    ///
    /// * `items` - An iterator providing the items to add.
    pub fn extend(&mut self, items: impl IntoIterator<Item = T>) {
        Extend::extend(self, items);
    }

    /// Retains only the elements specified by the predicate.
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&T) -> bool,
    {
        let guard = UniqueVecSetRebuildGuard {
            set: &mut self.set,
            vec: &mut self.vec,
        };
        let set = &mut *guard.set;
        guard.vec.retain(|item| {
            if f(item) {
                true
            } else {
                set.remove(item);
                false
            }
        });
    }

    /// Removes and returns the element at `index`.
    ///
    /// # Panics
    ///
    /// Panics if `index` is out of bounds (same semantics as
    /// [`Vec::remove`]).
    ///
    /// # Panic safety
    ///
    /// Bounds checking and user Hash/Eq code run before mutation. The set copy
    /// is taken without dropping it, the vector is edited, and only then is the
    /// copy dropped. Even a custom destructor panic leaves membership consistent.
    pub fn remove(&mut self, index: usize) -> T {
        self.remove_at(index, false)
    }

    fn remove_at(&mut self, index: usize, swap: bool) -> T {
        // Take ownership without running T::drop until both containers agree.
        // Bounds checking and user Hash/Eq code run before either edit.
        let duplicate = self.set.take(&self.vec[index]);
        let item = if swap {
            self.vec.swap_remove(index)
        } else {
            self.vec.remove(index)
        };
        drop(duplicate);
        item
    }

    /// Removes **an element** from the vector and returns it.
    /// The first element that satisfies the predicate will be removed.
    ///
    /// # Panic safety
    ///
    /// See [`Self::remove`]: the set entry is removed first so that a panic
    /// in `set.take` (custom Hash/Eq impls) leaves both containers untouched.
    pub fn remove_if<P>(&mut self, mut predicate: P) -> Option<T>
    where
        P: FnMut(&T) -> bool,
    {
        self.vec
            .iter()
            .position(&mut predicate)
            .map(|index| self.remove(index))
    }

    /// Removes **an element** from the vector and returns it.
    /// The last element is swapped into its place.
    ///
    /// # Panic safety
    ///
    /// See [`Self::remove`]: the set entry is removed first so that a panic
    /// in `set.take` (custom Hash/Eq impls) leaves both containers untouched.
    pub fn swap_remove_if<P>(&mut self, mut predicate: P) -> Option<T>
    where
        P: FnMut(&T) -> bool,
    {
        self.vec
            .iter()
            .position(&mut predicate)
            .map(|index| self.remove_at(index, true))
    }

    /// Intersects the `UniqueVec` with another `UniqueVec`.
    pub fn intersect_with(&mut self, other: &UniqueVec<T>) {
        self.retain(|item| other.contains(item));
    }

    /// Returns the inner `Vec` of the `UniqueVec`.
    pub fn into_vec(self) -> Vec<T> {
        self.vec
    }

    /// Returns the inner `FxHashSet` of the `UniqueVec`.
    pub fn into_set(self) -> FxHashSet<T> {
        self.set
    }

    /// Converts the `UniqueVec` to a `Vec`.
    pub fn to_vec(&self) -> Vec<T> {
        self.vec.clone()
    }

    /// Converts the `UniqueVec` to a `FxHashSet`.
    pub fn to_set(&self) -> FxHashSet<T> {
        self.set.clone()
    }
}

impl<T: PartialEq> PartialEq for UniqueVec<T> {
    /// Two `UniqueVec`s are equal when their vectors are equal (same elements
    /// in the same order). The membership set mirrors the vector, so it does
    /// not need to be compared.
    fn eq(&self, other: &Self) -> bool {
        self.vec == other.vec
    }
}

impl<T: Eq> Eq for UniqueVec<T> {}

impl<T> Extend<T> for UniqueVec<T>
where
    T: Eq + Hash + Clone,
{
    /// Extends the vector with items from an iterator that do not already exist.
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        // Element-by-element via `push` so the set/vec invariant holds after
        // every step, even if the iterator (or an allocation) panics midway.
        for item in iter {
            self.push(item);
        }
    }
}

impl<T> Serialize for UniqueVec<T>
where
    T: Serialize,
{
    /// Serializes the `UniqueVec` as a sequence.
    #[inline]
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.vec.iter())
    }
}

impl<'de, T> Deserialize<'de> for UniqueVec<T>
where
    T: Eq + Hash + Clone + Deserialize<'de>,
{
    /// Deserializes a sequence into a `UniqueVec`.
    #[inline]
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor<T>(std::marker::PhantomData<T>);
        impl<'de, T: Eq + Hash + Clone + Deserialize<'de>> serde::de::Visitor<'de> for Visitor<T> {
            type Value = UniqueVec<T>;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a sequence")
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let mut values = UniqueVec::new();
                let mut check_duplicates = std::mem::needs_drop::<T>();
                while let Some(item) = seq.next_element()? {
                    check_duplicates |= !values.push_constructing(item, check_duplicates);
                }
                Ok(values)
            }
        }
        deserializer.deserialize_seq(Visitor(std::marker::PhantomData))
    }
}

/// Utility for counting the size of serialized CBOR data.
///
/// Note: for computing the encoded size of a CBOR value, prefer
/// `cbor2::serialized_size` (the workspace convention); it avoids driving a
/// full serializer through the `Write` trait. This type is kept as a
/// general-purpose byte-counting `Write` sink for other serialization
/// formats and for backwards compatibility.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CountingWriter {
    count: usize,
}

impl Default for CountingWriter {
    /// Creates a new `CountingWriter` with a count of 0.
    fn default() -> Self {
        Self::new()
    }
}

impl CountingWriter {
    /// Creates a new `CountingWriter`.
    pub const fn new() -> Self {
        CountingWriter { count: 0 }
    }

    /// Returns the current count of bytes written.
    pub const fn size(&self) -> usize {
        self.count
    }
}

impl std::io::Write for CountingWriter {
    /// Implements the write method for the Write trait.
    /// This simply counts the bytes without actually writing them.
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = buf.len();
        self.count = self
            .count
            .checked_add(len)
            .ok_or_else(|| std::io::Error::other("byte count overflow"))?;
        Ok(len)
    }

    /// Implements the flush method for the Write trait.
    /// This is a no-op since we're not actually writing data.
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
