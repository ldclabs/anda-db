//! Shared utility types used by the AndaDB workspace.
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

impl<T> From<Vec<T>> for UniqueVec<T>
where
    T: Eq + Hash + Clone,
{
    /// Creates a `UniqueVec` from a `Vec`.
    ///
    /// The extender is initialized with all the unique items from the vector.
    fn from(mut vec: Vec<T>) -> Self {
        let mut set = FxHashSet::with_capacity_and_hasher(vec.len(), FxBuildHasher);
        vec.retain(|item| set.insert(item.clone()));
        Self { set, vec }
    }
}

impl<T> FromIterator<T> for UniqueVec<T>
where
    T: Eq + Hash + Clone,
{
    /// Creates a `UniqueVec` from an iterator.
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let vec: Vec<T> = iter.into_iter().collect();
        vec.into()
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
    /// The set entry is removed **before** the vector entry: `set.remove` may
    /// panic (custom `Hash` impls), and doing it first leaves both containers
    /// untouched on unwind. The subsequent `vec.remove` cannot panic because
    /// the index was already bounds-checked.
    pub fn remove(&mut self, index: usize) -> T {
        // Bounds check (may panic) happens before any mutation.
        self.set.remove(&self.vec[index]);
        self.vec.remove(index)
    }

    /// Removes **an element** from the vector and returns it.
    /// The first element that satisfies the predicate will be removed.
    ///
    /// # Panic safety
    ///
    /// See [`Self::remove`]: the set entry is removed first so that a panic
    /// in `set.remove` (custom `Hash` impls) leaves both containers untouched.
    pub fn remove_if<P>(&mut self, mut predicate: P) -> Option<T>
    where
        P: FnMut(&T) -> bool,
    {
        if let Some(index) = self.vec.iter().position(&mut predicate) {
            self.set.remove(&self.vec[index]);
            Some(self.vec.remove(index))
        } else {
            None
        }
    }

    /// Removes **an element** from the vector and returns it.
    /// The last element is swapped into its place.
    ///
    /// # Panic safety
    ///
    /// See [`Self::remove`]: the set entry is removed first so that a panic
    /// in `set.remove` (custom `Hash` impls) leaves both containers untouched.
    pub fn swap_remove_if<P>(&mut self, mut predicate: P) -> Option<T>
    where
        P: FnMut(&T) -> bool,
    {
        if let Some(index) = self.vec.iter().position(&mut predicate) {
            self.set.remove(&self.vec[index]);
            Some(self.vec.swap_remove(index))
        } else {
            None
        }
    }

    /// Intersects the `UniqueVec` with another `UniqueVec`.
    pub fn intersect_with(&mut self, other: &UniqueVec<T>) {
        let guard = UniqueVecSetRebuildGuard {
            set: &mut self.set,
            vec: &mut self.vec,
        };
        let set = &mut *guard.set;
        guard.vec.retain(|item| {
            if other.set.contains(item) {
                true
            } else {
                set.remove(item);
                false
            }
        });
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
        let vec: Vec<T> = Deserialize::deserialize(deserializer)?;
        Ok(UniqueVec::from(vec))
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
mod tests {
    use super::*;
    use std::{borrow::Cow, io::Write};

    #[test]
    fn test_pipe_trait() {
        // Test basic pipe functionality
        let result = 5.pipe(|x| x * 2).pipe(|x| x + 1);
        assert_eq!(result, 11);

        // Test pipe with different types
        let string_result = "hello"
            .pipe(|s| s.to_uppercase())
            .pipe(|s| format!("{} world", s));
        assert_eq!(string_result, "HELLO world");

        // Test pipe with closure that changes type
        let vec_result = vec![1, 2, 3].pipe(|v| v.len()).pipe(|len| len as f64);
        assert_eq!(vec_result, 3.0);
    }

    #[test]
    fn test_unique_vec_new() {
        let uv: UniqueVec<i32> = UniqueVec::new();
        assert_eq!(uv.len(), 0);
        assert!(uv.is_empty());
    }

    #[test]
    fn test_unique_vec_with_capacity() {
        let uv: UniqueVec<i32> = UniqueVec::with_capacity(10);
        assert_eq!(uv.len(), 0);
        assert_eq!(uv.capacity(), 10);
    }

    #[test]
    fn test_unique_vec_from_vec() {
        let vec = vec![1, 2, 2, 3, 2, 1];
        let uv = UniqueVec::from(vec);
        assert_eq!(uv.len(), 3);
        assert!(uv.contains(&1));
        assert!(uv.contains(&2));
        assert!(uv.contains(&3));
    }

    #[test]
    fn test_unique_vec_from_iterator() {
        let uv: UniqueVec<i32> = [2, 2, 1, 3, 2, 1].iter().cloned().collect();
        assert_eq!(uv.len(), 3);
        assert!(uv.contains(&2));
        assert!(uv.contains(&1));
        assert!(uv.contains(&3));
    }

    #[test]
    fn test_unique_vec_push() {
        let mut uv = UniqueVec::new();

        // Push new items
        assert!(uv.push(1));
        assert!(uv.push(2));
        assert!(uv.push(3));
        assert_eq!(uv.len(), 3);

        // Push duplicate items
        assert!(!uv.push(1));
        assert!(!uv.push(2));
        assert_eq!(uv.len(), 3);

        // Verify order is maintained
        assert_eq!(uv.as_ref(), &[1, 2, 3]);
    }

    #[test]
    fn test_unique_vec_extend() {
        let mut uv = UniqueVec::from(vec![1, 2, 3]);

        // Extend with mix of new and existing items
        uv.extend(vec![3, 4, 5, 2, 6]);

        assert_eq!(uv.len(), 6);
        assert_eq!(uv.as_ref(), &[1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_unique_vec_retain() {
        let mut uv = UniqueVec::from(vec![1, 2, 3, 4, 5]);

        // Retain only even numbers
        uv.retain(|&x| x % 2 == 0);

        assert_eq!(uv.len(), 2);
        assert_eq!(uv.as_ref(), &[2, 4]);
        assert!(uv.contains(&2));
        assert!(uv.contains(&4));
        assert!(!uv.contains(&1));
        assert!(!uv.contains(&3));
        assert!(!uv.contains(&5));
    }

    #[test]
    fn test_unique_vec_retain_keeps_set_consistent_after_panic() {
        let mut uv = UniqueVec::from(vec![1, 2, 3, 4, 5]);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            uv.retain(|&x| {
                if x == 3 {
                    panic!("intentional retain panic");
                }

                x % 2 == 0
            });
        }));

        assert!(result.is_err());
        for value in 1..=5 {
            assert_eq!(uv.contains(&value), uv.as_ref().contains(&value));
        }
    }

    #[test]
    fn test_unique_vec_retain_without_removal_keeps_set_consistent() {
        let mut uv = UniqueVec::from(vec![1, 2, 3]);

        uv.retain(|_| true);

        assert_eq!(uv.as_ref(), &[1, 2, 3]);
        // The set must still reject duplicates and accept new items.
        assert!(!uv.push(2));
        assert!(uv.push(4));
        assert_eq!(uv.as_ref(), &[1, 2, 3, 4]);
    }

    #[test]
    fn test_unique_vec_intersect_with_superset_and_disjoint() {
        let mut uv = UniqueVec::from(vec![1, 2, 3]);
        let superset = UniqueVec::from(vec![1, 2, 3, 4, 5]);

        // Intersecting with a superset removes nothing.
        uv.intersect_with(&superset);
        assert_eq!(uv.as_ref(), &[1, 2, 3]);
        assert!(!uv.push(3));

        // Intersecting with a disjoint set removes everything; removed items
        // must be insertable again afterwards.
        let disjoint = UniqueVec::from(vec![7, 8]);
        uv.intersect_with(&disjoint);
        assert!(uv.is_empty());
        assert!(!uv.contains(&1));
        assert!(uv.push(1));
    }

    #[test]
    fn test_unique_vec_remove() {
        let mut uv = UniqueVec::from(vec![1, 2, 3, 4, 5]);

        let removed = uv.remove(2); // Remove element at index 2 (value 3)
        assert_eq!(removed, 3);
        assert_eq!(uv.len(), 4);
        assert_eq!(uv.as_ref(), &[1, 2, 4, 5]);
        assert!(!uv.contains(&3));
    }

    #[test]
    #[should_panic]
    fn test_unique_vec_remove_out_of_bounds() {
        let mut uv = UniqueVec::from(vec![1, 2, 3]);
        uv.remove(5); // Should panic
    }

    #[test]
    fn test_unique_vec_remove_if() {
        let mut uv = UniqueVec::from(vec![1, 2, 3, 4, 5]);

        // Remove first even number
        let removed = uv.remove_if(|&x| x % 2 == 0);
        assert_eq!(removed, Some(2));
        assert_eq!(uv.len(), 4);
        assert_eq!(uv.as_ref(), &[1, 3, 4, 5]);
        assert!(!uv.contains(&2));

        // Try to remove non-existent condition
        let removed = uv.remove_if(|&x| x > 10);
        assert_eq!(removed, None);
        assert_eq!(uv.len(), 4);
    }

    #[test]
    fn test_unique_vec_swap_remove_if() {
        let mut uv = UniqueVec::from(vec![1, 2, 3, 4, 5]);

        // Remove first even number (swap with last)
        let removed = uv.swap_remove_if(|&x| x % 2 == 0);
        assert_eq!(removed, Some(2));
        assert_eq!(uv.len(), 4);
        // After swap_remove, the last element (5) should be in position of removed element
        assert_eq!(uv.as_ref(), &[1, 5, 3, 4]);
        assert!(!uv.contains(&2));
    }

    #[test]
    fn test_unique_vec_contains() {
        let uv = UniqueVec::from(vec![1, 2, 3]);

        assert!(uv.contains(&1));
        assert!(uv.contains(&2));
        assert!(uv.contains(&3));
        assert!(!uv.contains(&4));
    }

    #[test]
    fn test_unique_vec_intersect_with() {
        let mut uv1 = UniqueVec::from(vec![1, 2, 3, 4, 5]);
        let uv2 = UniqueVec::from(vec![3, 4, 5, 6, 7]);

        uv1.intersect_with(&uv2);

        assert_eq!(uv1.len(), 3);
        assert!(uv1.contains(&3));
        assert!(uv1.contains(&4));
        assert!(uv1.contains(&5));
        assert!(!uv1.contains(&1));
        assert!(!uv1.contains(&2));
    }

    #[test]
    fn test_unique_vec_to_vec() {
        let uv = UniqueVec::from(vec![1, 2, 2, 3]);
        let vec = uv.to_vec();
        assert_eq!(vec, vec![1, 2, 3]);
    }

    #[test]
    fn test_unique_vec_to_set() {
        let uv = UniqueVec::from(vec![1, 2, 3]);
        let set = uv.to_set();
        assert_eq!(set.len(), 3);
        assert!(set.contains(&1));
        assert!(set.contains(&2));
        assert!(set.contains(&3));
    }

    #[test]
    fn test_unique_vec_as_ref() {
        let uv = UniqueVec::from(vec![1, 2, 3]);
        let slice: &[i32] = uv.as_ref();
        assert_eq!(slice, &[1, 2, 3]);
    }

    #[test]
    fn test_unique_vec_deref() {
        let uv = UniqueVec::from(vec![1, 2, 3]);
        // Test deref by calling Vec methods directly
        assert_eq!(uv.len(), 3);
        assert_eq!(uv[0], 1);
        assert_eq!(uv[1], 2);
        assert_eq!(uv[2], 3);
    }

    #[test]
    fn test_unique_vec_into_vec() {
        let uv = UniqueVec::from(vec![1, 2, 3]);
        let vec: Vec<i32> = uv.into();
        assert_eq!(vec, vec![1, 2, 3]);
    }

    #[test]
    fn test_unique_vec_serialize_deserialize() {
        let uv = UniqueVec::from(vec![1, 2, 2, 3, 2, 1]); // Duplicates should be removed

        // Serialize
        let json = serde_json::to_string(&uv).unwrap();
        assert_eq!(json, "[1,2,3]");

        // Deserialize
        let deserialized: UniqueVec<i32> = serde_json::from_str("[1,3,2,3,3,2,1]").unwrap();
        assert_eq!(deserialized.len(), 3);
        assert_eq!(deserialized.as_ref(), &[1, 3, 2]);
    }

    #[test]
    fn test_unique_vec_deserialize_borrowed_values() {
        fn deserialize_unique_vec_cow<'a>(json: &'a str) -> UniqueVec<Cow<'a, str>> {
            serde_json::from_str(json).unwrap()
        }

        let json = String::from(r#"["alpha","beta","alpha"]"#);
        let deserialized = deserialize_unique_vec_cow(&json);

        assert_eq!(
            deserialized.as_ref(),
            &[Cow::Borrowed("alpha"), Cow::Borrowed("beta")]
        );
    }

    #[test]
    fn test_unique_vec_clone() {
        let uv1 = UniqueVec::from(vec![1, 2, 3]);
        let uv2 = uv1.clone();

        assert_eq!(uv1.len(), uv2.len());
        assert_eq!(uv1.as_ref(), uv2.as_ref());
    }

    #[test]
    fn test_counting_writer_new() {
        let writer = CountingWriter::new();
        assert_eq!(writer.size(), 0);
    }

    #[test]
    fn test_counting_writer_default() {
        let writer = CountingWriter::default();
        assert_eq!(writer.size(), 0);
    }

    #[test]
    fn test_counting_writer_write() {
        let mut writer = CountingWriter::new();

        let result = writer.write(b"hello");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 5);
        assert_eq!(writer.size(), 5);

        let result = writer.write(b" world");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 6);
        assert_eq!(writer.size(), 11);
    }

    #[test]
    fn test_counting_writer_flush() {
        let mut writer = CountingWriter::new();
        let result = writer.flush();
        assert!(result.is_ok());
        assert_eq!(writer.size(), 0); // Flush doesn't change size
    }

    #[test]
    fn test_counting_writer_multiple_writes() {
        let mut writer = CountingWriter::new();

        // Multiple writes should accumulate
        writer.write_all(b"a").unwrap();
        assert_eq!(writer.size(), 1);

        writer.write_all(b"bc").unwrap();
        assert_eq!(writer.size(), 3);

        writer.write_all(b"defg").unwrap();
        assert_eq!(writer.size(), 7);
    }

    #[test]
    fn test_counting_writer_empty_write() {
        let mut writer = CountingWriter::new();

        let result = writer.write(b"");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
        assert_eq!(writer.size(), 0);
    }

    #[test]
    fn test_unique_vec_edge_cases() {
        // Test with empty vector
        let uv = UniqueVec::from(vec![] as Vec<i32>);
        assert_eq!(uv.len(), 0);
        assert!(uv.is_empty());

        // Test with single element
        let mut uv = UniqueVec::from(vec![42]);
        assert_eq!(uv.len(), 1);
        assert!(uv.contains(&42));

        // Test removing the only element
        let removed = uv.remove(0);
        assert_eq!(removed, 42);
        assert_eq!(uv.len(), 0);
        assert!(!uv.contains(&42));
    }

    #[test]
    fn test_unique_vec_string_type() {
        let mut uv = UniqueVec::new();

        uv.push("hello".to_string());
        uv.push("world".to_string());
        uv.push("hello".to_string()); // Duplicate

        assert_eq!(uv.len(), 2);
        assert!(uv.contains("hello"));
        assert!(uv.contains("world"));
    }

    #[test]
    fn test_counting_writer_overflow() {
        let mut writer = CountingWriter { count: usize::MAX };
        let err = writer.write(b"x").unwrap_err();

        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        assert_eq!(writer.size(), usize::MAX);
    }

    #[test]
    fn test_unique_vec_extend_panicking_iterator_keeps_set_consistent() {
        let mut uv = UniqueVec::from(vec![1, 2]);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            uv.extend((3..10).inspect(|&x| {
                if x == 5 {
                    panic!("intentional extend panic");
                }
            }));
        }));
        assert!(result.is_err());

        // Elements yielded before the panic are applied; set and vec agree.
        assert_eq!(uv.as_ref(), &[1, 2, 3, 4]);
        for value in 1..10 {
            assert_eq!(uv.contains(&value), uv.as_ref().contains(&value));
        }
        // The set still rejects duplicates and accepts new items.
        assert!(!uv.push(4));
        assert!(uv.push(5));
        assert_eq!(uv.as_ref(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_unique_vec_cbor_round_trip_and_dedup() {
        // Round-trip through CBOR (the workspace's on-disk format).
        let uv = UniqueVec::from(vec![1u64, 2, 3]);
        let mut buf = Vec::new();
        cbor2::to_writer(&uv, &mut buf).unwrap();
        let decoded: UniqueVec<u64> = cbor2::from_reader(&buf[..]).unwrap();
        assert_eq!(decoded, uv);

        // CBOR input containing duplicates (e.g. a corrupted or crash-window
        // bucket file in anda_db_btree) is deduplicated on load, preserving
        // first-occurrence order.
        let mut buf = Vec::new();
        cbor2::to_writer(&vec![5u64, 1, 5, 2, 1], &mut buf).unwrap();
        let decoded: UniqueVec<u64> = cbor2::from_reader(&buf[..]).unwrap();
        assert_eq!(decoded.as_ref(), &[5, 1, 2]);
        assert!(decoded.contains(&2));
        assert!(!decoded.contains(&9));
    }

    #[test]
    fn test_counting_writer_matches_cbor2_serialized_size() {
        let value = (42u64, "hello".to_string(), vec![1u8, 2, 3]);
        let mut writer = CountingWriter::new();
        cbor2::to_writer(&value, &mut writer).unwrap();
        assert_eq!(
            writer.size() as u64,
            cbor2::serialized_size(&value).unwrap()
        );
    }

    #[test]
    fn test_unique_vec_partial_eq() {
        let a = UniqueVec::from(vec![1, 2, 3]);
        let b = UniqueVec::from(vec![1, 2, 2, 3]);
        let c = UniqueVec::from(vec![3, 2, 1]);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    use std::cell::Cell;
    use std::hash::{Hash, Hasher};

    thread_local! {
        /// Remaining `Hash::hash` calls before the next one panics.
        /// `None` disables panicking.
        static HASH_PANIC_COUNTDOWN: Cell<Option<usize>> = const { Cell::new(None) };
        /// When `true`, the next `Clone::clone` call panics.
        static CLONE_PANIC: Cell<bool> = const { Cell::new(false) };
    }

    /// Element type whose `Hash` / `Clone` impls can be armed to panic,
    /// simulating adversarial or buggy user types.
    #[derive(Debug, PartialEq, Eq)]
    struct Evil(u32);

    impl Hash for Evil {
        fn hash<H: Hasher>(&self, state: &mut H) {
            HASH_PANIC_COUNTDOWN.with(|c| {
                if let Some(n) = c.get() {
                    if n == 0 {
                        c.set(None);
                        panic!("intentional Hash panic");
                    }
                    c.set(Some(n - 1));
                }
            });
            self.0.hash(state);
        }
    }

    impl Clone for Evil {
        fn clone(&self) -> Self {
            CLONE_PANIC.with(|c| {
                if c.get() {
                    c.set(false);
                    panic!("intentional Clone panic");
                }
            });
            Evil(self.0)
        }
    }

    fn assert_evil_invariant(uv: &UniqueVec<Evil>, universe: std::ops::RangeInclusive<u32>) {
        // set and vec must agree exactly, and vec must have no duplicates.
        for value in universe {
            let item = Evil(value);
            assert_eq!(
                uv.contains(&item),
                uv.as_ref().contains(&item),
                "set/vec diverged for {value}"
            );
            assert!(
                uv.as_ref().iter().filter(|x| **x == item).count() <= 1,
                "duplicate element {value} in vec"
            );
        }
    }

    #[test]
    fn test_unique_vec_push_hash_panic_keeps_invariant() {
        let mut uv: UniqueVec<Evil> = UniqueVec::new();
        assert!(uv.push(Evil(1)));
        assert!(uv.push(Evil(2)));

        // `push` hashes twice: once in `contains`, once in `set.insert`.
        // Arm the panic for the second hash so `vec.push` has already
        // succeeded when `set.insert` unwinds — the historical window where
        // vec ⊋ set let a later push insert a duplicate.
        HASH_PANIC_COUNTDOWN.with(|c| c.set(Some(1)));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            uv.push(Evil(3));
        }));
        HASH_PANIC_COUNTDOWN.with(|c| c.set(None));
        assert!(result.is_err());

        // The drop guard must have rolled the vector back.
        assert_eq!(uv.as_ref(), &[Evil(1), Evil(2)]);
        assert_evil_invariant(&uv, 1..=3);

        // Re-pushing the same element must add it exactly once.
        assert!(uv.push(Evil(3)));
        assert!(!uv.push(Evil(3)));
        assert_eq!(uv.as_ref(), &[Evil(1), Evil(2), Evil(3)]);
        assert_evil_invariant(&uv, 1..=3);
    }

    #[test]
    fn test_unique_vec_push_clone_panic_keeps_invariant() {
        let mut uv: UniqueVec<Evil> = UniqueVec::new();
        assert!(uv.push(Evil(1)));

        CLONE_PANIC.with(|c| c.set(true));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            uv.push(Evil(2));
        }));
        CLONE_PANIC.with(|c| c.set(false));
        assert!(result.is_err());

        assert_eq!(uv.as_ref(), &[Evil(1)]);
        assert_evil_invariant(&uv, 1..=2);
        assert!(uv.push(Evil(2)));
        assert_evil_invariant(&uv, 1..=2);
    }

    #[test]
    fn test_unique_vec_remove_hash_panic_keeps_invariant() {
        let mut uv: UniqueVec<Evil> = UniqueVec::new();
        for v in 1..=3 {
            assert!(uv.push(Evil(v)));
        }

        // `remove` / `remove_if` / `swap_remove_if` hash once (`set.remove`).
        // A panic there must leave both containers untouched instead of the
        // historical set ⊋ vec state where the element could never be
        // re-inserted.
        HASH_PANIC_COUNTDOWN.with(|c| c.set(Some(0)));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            uv.remove(1);
        }));
        HASH_PANIC_COUNTDOWN.with(|c| c.set(None));
        assert!(result.is_err());
        assert_eq!(uv.as_ref(), &[Evil(1), Evil(2), Evil(3)]);
        assert_evil_invariant(&uv, 1..=3);

        HASH_PANIC_COUNTDOWN.with(|c| c.set(Some(0)));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            uv.remove_if(|x| x.0 == 2);
        }));
        HASH_PANIC_COUNTDOWN.with(|c| c.set(None));
        assert!(result.is_err());
        assert_eq!(uv.as_ref(), &[Evil(1), Evil(2), Evil(3)]);
        assert_evil_invariant(&uv, 1..=3);

        HASH_PANIC_COUNTDOWN.with(|c| c.set(Some(0)));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            uv.swap_remove_if(|x| x.0 == 2);
        }));
        HASH_PANIC_COUNTDOWN.with(|c| c.set(None));
        assert!(result.is_err());
        assert_eq!(uv.as_ref(), &[Evil(1), Evil(2), Evil(3)]);
        assert_evil_invariant(&uv, 1..=3);

        // With panics disarmed, removal still works normally.
        assert_eq!(uv.remove_if(|x| x.0 == 2), Some(Evil(2)));
        assert_eq!(uv.as_ref(), &[Evil(1), Evil(3)]);
        assert_evil_invariant(&uv, 1..=3);
        assert!(uv.push(Evil(2)));
        assert_evil_invariant(&uv, 1..=3);
    }
}
