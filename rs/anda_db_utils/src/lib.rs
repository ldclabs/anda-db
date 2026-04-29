use core::ops::Deref;
use rustc_hash::{FxBuildHasher, FxHashSet};
use serde::{
    de::{Deserialize, DeserializeOwned, Deserializer},
    ser::{Serialize, Serializer},
};
use std::{borrow::Borrow, hash::Hash};

mod cbor_size;

pub use cbor_size::{CborSizeError, estimate_cbor_size, try_estimate_cbor_size};

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
    pub fn push(&mut self, item: T) -> bool {
        if self.set.insert(item.clone()) {
            self.vec.push(item);
            true
        } else {
            false
        }
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
        self.vec.retain(&mut f);
        self.set.clear();
        self.set.extend(self.vec.iter().cloned());
    }

    /// Removes and returns the element at `index`.
    pub fn remove(&mut self, index: usize) -> T {
        let item = self.vec.remove(index);
        self.set.remove(&item);
        item
    }

    /// Removes **an element** from the vector and returns it.
    /// The first element that satisfies the predicate will be removed.
    pub fn remove_if<P>(&mut self, mut predicate: P) -> Option<T>
    where
        P: FnMut(&T) -> bool,
    {
        if let Some(index) = self.vec.iter().position(&mut predicate) {
            let item = self.vec.remove(index);
            self.set.remove(&item);
            Some(item)
        } else {
            None
        }
    }

    /// Removes **an element** from the vector and returns it.
    /// The last element is swapped into its place.
    pub fn swap_remove_if<P>(&mut self, mut predicate: P) -> Option<T>
    where
        P: FnMut(&T) -> bool,
    {
        if let Some(index) = self.vec.iter().position(&mut predicate) {
            let item = self.vec.swap_remove(index);
            self.set.remove(&item);
            Some(item)
        } else {
            None
        }
    }

    /// Intersects the `UniqueVec` with another `UniqueVec`.
    pub fn intersect_with(&mut self, other: &UniqueVec<T>) {
        self.vec.retain(|item| other.set.contains(item));
        self.set.clear();
        self.set.extend(self.vec.iter().cloned());
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

impl<T> Extend<T> for UniqueVec<T>
where
    T: Eq + Hash + Clone,
{
    /// Extends the vector with items from an iterator that do not already exist.
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        self.vec.extend(
            iter.into_iter()
                .filter(|item| self.set.insert(item.clone())),
        );
    }
}

impl<T> Serialize for UniqueVec<T>
where
    T: Eq + Hash + Clone + Serialize,
{
    /// Serializes the `UniqueVec` as a sequence.
    #[inline]
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.vec.iter())
    }
}

impl<'de, T> Deserialize<'de> for UniqueVec<T>
where
    T: Eq + Hash + Clone + DeserializeOwned,
{
    /// Deserializes a sequence into a `UniqueVec`.
    #[inline]
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let vec: Vec<T> = Deserialize::deserialize(deserializer)?;
        Ok(UniqueVec::from(vec))
    }
}

/// Utility for counting the size of serialized CBOR data.
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
    use std::io::Write;

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
}
