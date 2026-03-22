//! # Anda-DB B-tree Index Library
//!
//! This module provides a B-tree based index implementation for Anda-DB.
//! It supports indexing fields of various types including u64, i64, String, and binary data.
//! The implementation is optimized for concurrent access and efficient range queries.
//!
//! ## Features
//! - Thread-safe concurrent access
//! - Efficient range queries
//! - Support for various data types
//! - Bucket-based storage for better incremental persistent
//! - Efficient serialization and deserialization in CBOR format

use anda_db_utils::{UniqueVec, estimate_cbor_size};
use dashmap::DashMap;
use parking_lot::RwLock;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;
use std::{
    collections::BTreeSet,
    fmt::Debug,
    hash::Hash,
    io::{Read, Write},
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
};

use crate::{BTreeError, BoxError};

/// B-tree index for efficient key-value lookups
///
/// This structure provides a thread-safe B-tree index implementation
/// that supports concurrent reads and writes, as well as efficient range queries.
pub struct BTreeIndex<PK, FV>
where
    PK: Ord + Debug + Clone + Serialize + DeserializeOwned,
    FV: Eq + Ord + Hash + Debug + Clone + Serialize + DeserializeOwned,
{
    /// Index name
    name: String,

    /// Index configuration
    config: BTreeConfig,

    /// Buckets store information about where posting entries are stored and their current state
    /// The mapping is: bucket_id -> (bucket_size, is_dirty, vec<field_values>, dirty_version)
    /// - bucket_size: Current size of the bucket in bytes
    /// - is_dirty: Indicates if the bucket has new data that needs to be persisted
    /// - field_values: List of field values stored in this bucket
    /// - dirty_version: Monotonic counter increased on every bucket mutation
    buckets: DashMap<u32, (usize, bool, UniqueVec<FV>, u64)>,

    /// Inverted index mapping field values to posting values
    postings: DashMap<FV, PostingValue<PK>>,

    /// B-tree set for efficient range queries
    btree: RwLock<BTreeSet<FV>>,

    /// Index metadata
    metadata: RwLock<BTreeMetadata>,

    /// Maximum bucket ID currently in use
    max_bucket_id: AtomicU32,

    /// Number of query operations performed
    query_count: AtomicU64,

    /// Last saved version of the index
    last_saved_version: AtomicU64,

    /// Number of dirty buckets pending persistence.
    ///
    /// Used to make `flush()` and `store_dirty_buckets()` fast when there is nothing to persist.
    dirty_bucket_count: AtomicU32,
}

/// Type alias for posting values: (bucket id, update version, Vec<document id>)
/// - bucket_id: The bucket where this posting is stored
/// - update_version: Version number that increases with each update
/// - document_ids: List of document IDs associated with this field value
type PostingValue<PK> = (u32, u64, UniqueVec<PK>);

/// Configuration parameters for the B-tree index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreeConfig {
    /// Maximum size of a bucket before creating a new one
    /// When a bucket's stored data exceeds this size,
    /// a new bucket should be created for new data
    pub bucket_overload_size: usize,

    /// Whether to allow duplicate primary keys in an indexed field value
    /// If false, attempting to insert a duplicate key will result in an error
    pub allow_duplicates: bool,
}

impl Default for BTreeConfig {
    fn default() -> Self {
        BTreeConfig {
            bucket_overload_size: 1024 * 512,
            allow_duplicates: true,
        }
    }
}

/// Index metadata containing configuration and statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreeMetadata {
    /// Index name
    pub name: String,

    /// Index configuration
    pub config: BTreeConfig,

    /// Index statistics
    pub stats: BTreeStats,
}

/// Index statistics for monitoring and diagnostics
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BTreeStats {
    /// Last insertion timestamp (unix ms)
    pub last_inserted: u64,

    /// Last deletion timestamp (unix ms)
    pub last_deleted: u64,

    /// Last saved timestamp (unix ms)
    pub last_saved: u64,

    /// Updated version for the index. It will be incremented when the index is updated.
    pub version: u64,

    /// Number of elements in the index
    pub num_elements: u64,

    /// Number of query operations performed
    pub query_count: u64,

    /// Number of insert operations performed
    pub insert_count: u64,

    /// Number of delete operations performed
    pub delete_count: u64,

    /// Maximum bucket ID currently in use
    pub max_bucket_id: u32,
}

// Helper structure for serialization and deserialization of index metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BTreeIndexOwned {
    // Index metadata
    metadata: BTreeMetadata,
}

// Reference structure for serializing the index
#[derive(Serialize)]
struct BTreeIndexRef<'a> {
    metadata: &'a BTreeMetadata,
}

// Helper structure for serialization and deserialization of bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "PK: Serialize, FV: Serialize",
    deserialize = "PK: DeserializeOwned, FV: DeserializeOwned"
))]
struct BucketOwned<PK, FV>
where
    PK: Eq + Ord + Hash + Clone,
    FV: Eq + Ord + Hash + Clone,
{
    #[serde(rename = "p")]
    postings: FxHashMap<FV, PostingValue<PK>>,
}

// Reference structure for serializing bucket
#[derive(Serialize)]
struct BucketRef<'a, PK, FV>
where
    PK: Eq + Ord + Hash + Clone + Serialize,
    FV: Eq + Ord + Hash + Clone + Serialize,
{
    #[serde(rename = "p")]
    postings: &'a FxHashMap<&'a FV, dashmap::mapref::one::Ref<'a, FV, PostingValue<PK>>>,
}

/// Range query specification for flexible querying
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RangeQuery<FV> {
    /// Equal to a specific key
    Eq(FV),

    /// Greater than a specific key
    Gt(FV),

    /// Greater than or equal to a specific key
    Ge(FV),

    /// Less than a specific key
    Lt(FV),

    /// Less than or equal to a specific key
    Le(FV),

    /// Between two keys (inclusive)
    Between(FV, FV),

    /// Include specific keys
    Include(Vec<FV>),

    /// A logical OR query that requires at least one subquery to match
    Or(Vec<Box<RangeQuery<FV>>>),

    /// A logical AND query that requires all subqueries to match
    And(Vec<Box<RangeQuery<FV>>>),

    /// A logical NOT query that negates the result of its subquery
    Not(Box<RangeQuery<FV>>),
}

impl<FV> RangeQuery<FV> {
    pub fn try_convert_from<FV1>(value: RangeQuery<FV1>) -> Result<Self, BoxError>
    where
        FV: Ord,
        FV: TryFrom<FV1, Error = BoxError>,
    {
        match value {
            RangeQuery::Eq(key) => Ok(RangeQuery::Eq(key.try_into()?)),
            RangeQuery::Gt(key) => Ok(RangeQuery::Gt(key.try_into()?)),
            RangeQuery::Ge(key) => Ok(RangeQuery::Ge(key.try_into()?)),
            RangeQuery::Lt(key) => Ok(RangeQuery::Lt(key.try_into()?)),
            RangeQuery::Le(key) => Ok(RangeQuery::Le(key.try_into()?)),
            RangeQuery::Between(start_key, end_key) => Ok(RangeQuery::Between(
                start_key.try_into()?,
                end_key.try_into()?,
            )),
            RangeQuery::Include(keys) => {
                let converted_keys = keys
                    .into_iter()
                    .map(|key| key.try_into())
                    .collect::<Result<Vec<FV>, _>>()?;
                Ok(RangeQuery::Include(converted_keys))
            }
            RangeQuery::And(queries) => {
                let converted_queries = queries
                    .into_iter()
                    .map(|query| RangeQuery::try_convert_from(*query))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RangeQuery::And(
                    converted_queries.into_iter().map(Box::new).collect(),
                ))
            }
            RangeQuery::Or(queries) => {
                let converted_queries = queries
                    .into_iter()
                    .map(|query| RangeQuery::try_convert_from(*query))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RangeQuery::Or(
                    converted_queries.into_iter().map(Box::new).collect(),
                ))
            }
            RangeQuery::Not(query) => {
                let converted_query = RangeQuery::try_convert_from(*query)?;
                Ok(RangeQuery::Not(Box::new(converted_query)))
            }
        }
    }
}

impl<PK, FV> BTreeIndex<PK, FV>
where
    PK: Ord + Eq + Hash + Debug + Clone + Serialize + DeserializeOwned,
    FV: Ord + Eq + Hash + Debug + Clone + Serialize + DeserializeOwned,
{
    fn mark_bucket_dirty(&self, bucket: &mut (usize, bool, UniqueVec<FV>, u64)) {
        bucket.3 = bucket.3.wrapping_add(1);
        if !bucket.1 {
            bucket.1 = true;
            self.dirty_bucket_count.fetch_add(1, Ordering::Release);
        }
    }

    /// Creates a new empty B-tree index with the given configuration
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the index
    /// * `config` - Optional B-tree configuration parameters
    ///
    /// # Returns
    ///
    /// * `BTreeIndex` - A new instance of the B-tree index
    pub fn new(name: String, config: Option<BTreeConfig>) -> Self {
        let config = config.unwrap_or_default();
        let stats = BTreeStats {
            version: 1,
            ..Default::default()
        };
        BTreeIndex {
            name: name.clone(),
            config: config.clone(),
            postings: DashMap::new(),
            buckets: DashMap::from_iter(vec![(0, (0, false, UniqueVec::default(), 0))]),
            btree: RwLock::new(BTreeSet::new()),
            metadata: RwLock::new(BTreeMetadata {
                name,
                config,
                stats,
            }),
            max_bucket_id: AtomicU32::new(0),
            query_count: AtomicU64::new(0),
            last_saved_version: AtomicU64::new(0),
            dirty_bucket_count: AtomicU32::new(0),
        }
    }

    /// Loads an index from metadata reader and a closure for loading buckets.
    ///
    /// # Arguments
    ///
    /// * `metadata` - Metadata reader
    /// * `f` - Closure for loading buckets
    ///
    /// # Returns
    ///
    /// * `Result<Self, BTreeError>` - Loaded index or error.
    pub async fn load_all<R: Read, F>(metadata: R, f: F) -> Result<Self, BTreeError>
    where
        F: AsyncFnMut(u32) -> Result<Option<Vec<u8>>, BoxError>,
    {
        let mut index = Self::load_metadata(metadata)?;
        index.load_buckets(f).await?;
        Ok(index)
    }

    /// Loads an index from a reader
    /// This only loads metadata, you need to call [`Self::load_buckets`] to load the actual posting data
    ///
    /// # Arguments
    ///
    /// * `r` - Any type implementing the [`Read`] trait
    ///
    /// # Returns
    ///
    /// * `Result<Self, Error>` - Loaded index or error
    pub fn load_metadata<R: Read>(r: R) -> Result<Self, BTreeError> {
        // Deserialize the index metadata
        let index: BTreeIndexOwned =
            ciborium::from_reader(r).map_err(|err| BTreeError::Serialization {
                name: "unknown".to_string(),
                source: err.into(),
            })?;

        // Extract configuration values
        let max_bucket_id = AtomicU32::new(index.metadata.stats.max_bucket_id);
        let query_count = AtomicU64::new(index.metadata.stats.query_count);
        let last_saved_version = AtomicU64::new(index.metadata.stats.version);

        Ok(BTreeIndex {
            name: index.metadata.name.clone(),
            config: index.metadata.config.clone(),
            postings: DashMap::with_capacity(index.metadata.stats.num_elements as usize),
            buckets: DashMap::from_iter(vec![(0, (0, false, UniqueVec::default(), 0))]),
            btree: RwLock::new(BTreeSet::new()),
            metadata: RwLock::new(index.metadata),
            query_count,
            max_bucket_id,
            last_saved_version,
            dirty_bucket_count: AtomicU32::new(0),
        })
    }

    /// Loads data from buckets using the provided async function
    /// This function should be called during database startup to load all bucket data
    /// and form a complete index
    ///
    /// # Arguments
    ///
    /// * `f` - Async function that reads posting data from a specified bucket.
    ///   `F: AsyncFn(u32) -> Result<Option<Vec<u8>>, BTreeError>`
    ///   The function should take a bucket ID as input and return a vector of bytes
    ///   containing the serialized bucket data. If the bucket does not exist,
    ///   it should return `Ok(None)`.
    ///
    /// # Returns
    ///
    /// * `Result<(), BTreeError>` - Success or error
    pub async fn load_buckets<F>(&mut self, mut f: F) -> Result<(), BTreeError>
    where
        F: AsyncFnMut(u32) -> Result<Option<Vec<u8>>, BoxError>,
    {
        for i in 0..=self.max_bucket_id.load(Ordering::Relaxed) {
            let data = f(i).await.map_err(|err| BTreeError::Generic {
                name: self.name.clone(),
                source: err,
            })?;
            if let Some(data) = data {
                let bucket: BucketOwned<PK, FV> =
                    ciborium::from_reader(&data[..]).map_err(|err| BTreeError::Serialization {
                        name: self.name.clone(),
                        source: err.into(),
                    })?;
                let bks = bucket.postings.keys().cloned().collect::<Vec<_>>();
                self.btree.write().extend(bks.iter().cloned());
                // Update bucket information
                // Larger buckets have the most recent state and can override smaller buckets
                self.buckets.insert(i, (data.len(), false, bks.into(), 0));
                self.postings.extend(bucket.postings);
            }
        }

        Ok(())
    }

    /// Returns the number of keys in the index
    pub fn len(&self) -> usize {
        self.postings.len()
    }

    /// Returns whether the index is empty
    pub fn is_empty(&self) -> bool {
        self.postings.is_empty()
    }

    /// Returns the index name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the index whether it allows duplicate keys
    pub fn allow_duplicates(&self) -> bool {
        self.config.allow_duplicates
    }

    /// Returns the index metadata
    /// This includes up-to-date statistics about the index
    pub fn metadata(&self) -> BTreeMetadata {
        let mut metadata = self.metadata.read().clone();
        metadata.stats.num_elements = self.postings.len() as u64;
        metadata.stats.query_count = self.query_count.load(Ordering::Relaxed);
        metadata.stats.max_bucket_id = self.max_bucket_id.load(Ordering::Relaxed);
        metadata
    }

    /// Gets current statistics about the index
    pub fn stats(&self) -> BTreeStats {
        let mut stats = { self.metadata.read().stats.clone() };
        stats.num_elements = self.postings.len() as u64;
        stats.query_count = self.query_count.load(Ordering::Relaxed);
        stats.max_bucket_id = self.max_bucket_id.load(Ordering::Relaxed);
        stats
    }

    /// Inserts a document_id-field_value pair to the index
    ///
    /// # Arguments
    ///
    /// * `doc_id` - Document identifier
    /// * `field_value` - Key to index
    /// * `now_ms` - Current timestamp in milliseconds
    ///
    /// # Returns
    ///
    /// * `Ok(bool)` if the document_id-field_value pair was successfully added
    /// * `Err(BTreeError)` if failed
    pub fn insert(&self, doc_id: PK, field_value: FV, now_ms: u64) -> Result<bool, BTreeError> {
        let bucket = self.max_bucket_id.load(Ordering::Relaxed);

        // Ensure the current bucket exists.
        // This avoids races where max_bucket_id advances before the bucket entry is created,
        // and also supports calling insert() after load_metadata() but before load_buckets().
        self.buckets
            .entry(bucket)
            .or_insert_with(|| (0, false, UniqueVec::default(), 0));

        // Calculate the size increase for this insertion
        let mut is_new = false;
        let mut size_increase = 0;
        let mut target_bucket = bucket;
        match self.postings.entry(field_value.clone()) {
            dashmap::Entry::Occupied(mut entry) => {
                let posting = entry.get_mut();
                target_bucket = posting.0;

                // Unique index semantics: allow idempotent insert of the same (doc_id, field_value)
                // while rejecting a different doc_id for an existing field_value.
                if !self.config.allow_duplicates && !posting.2.contains(&doc_id) {
                    return Err(BTreeError::AlreadyExists {
                        name: self.name.clone(),
                        id: json!(doc_id),
                        value: json!(field_value),
                    });
                }

                // Add doc_id if it doesn't exist
                if posting.2.push(doc_id.clone()) {
                    size_increase = estimate_cbor_size(&doc_id) + 2;
                    posting.1 += 1; // increment version
                }
            }
            dashmap::Entry::Vacant(entry) => {
                // Create a new posting for this field value
                let posting = (bucket, 1, vec![doc_id].into());
                size_increase = estimate_cbor_size(&posting) + 2;
                entry.insert(posting);
                is_new = true;
            }
        };

        if is_new {
            // Add the field value to the B-tree for range queries
            self.btree.write().insert(field_value.clone());
        }

        // If the index was modified, update bucket state
        let mut new_bucket = 0;
        if size_increase > 0 {
            // Update bucket state
            let mut b = self
                .buckets
                .entry(target_bucket)
                .or_insert_with(|| (0, false, UniqueVec::default(), 0));

            // Check if the bucket has enough space
            if b.2.is_empty() || b.0 + size_increase < self.config.bucket_overload_size {
                b.0 += size_increase;
                // Mark as dirty, needs to be persisted
                self.mark_bucket_dirty(&mut b);
                // Add field value to bucket if not already present
                b.2.push(field_value.clone());
            } else {
                // If the current bucket is full, create a new one
                let mut size_decrease = 0;
                new_bucket = self.max_bucket_id.fetch_add(1, Ordering::Relaxed) + 1;
                {
                    if let Some(mut posting) = self.postings.get_mut(&field_value) {
                        // Update the posting's bucket ID
                        posting.0 = new_bucket;
                        size_decrease = estimate_cbor_size(&posting) + 2;
                        size_increase = size_decrease;
                    }
                }
                // Remove the current field value from the current bucket
                // The freed space can still accommodate small growth in other field values
                if b.2.swap_remove_if(|k| &field_value == k).is_some() {
                    b.0 = b.0.saturating_sub(size_decrease);
                    // Source bucket must be marked dirty, otherwise stale on-disk
                    // entries may survive and be resurrected after restart.
                    self.mark_bucket_dirty(&mut b);
                }
            }
        }

        if new_bucket > 0 {
            // Create a new bucket and migrate this data to it
            match self.buckets.entry(new_bucket) {
                dashmap::Entry::Vacant(entry) => {
                    // Create a new bucket with the initial size
                    entry.insert((size_increase, true, vec![field_value].into(), 1));
                    self.dirty_bucket_count.fetch_add(1, Ordering::Release);
                }
                dashmap::Entry::Occupied(mut entry) => {
                    let bucket_entry = entry.get_mut();
                    bucket_entry.0 += size_increase;
                    self.mark_bucket_dirty(bucket_entry);
                    bucket_entry.2.push(field_value);
                }
            }
        }

        if size_increase > 0 {
            self.update_metadata(|m| {
                m.stats.version += 1;
                m.stats.last_inserted = now_ms;
                m.stats.insert_count += 1;
            });
        }

        Ok(size_increase > 0)
    }

    /// Removes a document_id-field_value pair from the index with hook function
    ///
    /// # Arguments
    ///
    /// * `doc_id` - Document identifier
    /// * `field_value` - field to remove
    /// * `now_ms` - Current timestamp in milliseconds
    ///
    /// # Returns
    ///
    /// * `bool` - `true` if the document_id-field_value pair was successfully removed, `false` otherwise
    pub fn remove(&self, doc_id: PK, field_value: FV, now_ms: u64) -> bool {
        let mut removed = false;
        let mut size_decrease = 0;
        let mut posting_empty = false;
        let mut bucket_id = 0;

        {
            if let Some(mut posting) = self.postings.get_mut(&field_value) {
                bucket_id = posting.0;
                let prev_posting_size = estimate_cbor_size(&*posting) + 2;
                if posting.2.swap_remove_if(|id| id == &doc_id).is_some() {
                    removed = true;
                    posting.1 += 1; // increment version
                    posting_empty = posting.2.is_empty();
                    size_decrease = if posting_empty {
                        prev_posting_size
                    } else {
                        estimate_cbor_size(&doc_id) + 2
                    };
                }
            }
        }

        if removed {
            // Update the bucket state
            if let Some(mut b) = self.buckets.get_mut(&bucket_id) {
                b.0 = b.0.saturating_sub(size_decrease);
                self.mark_bucket_dirty(&mut b);

                if posting_empty {
                    // remove FV from the bucket
                    b.2.swap_remove_if(|k| &field_value == k);
                }
            }

            if posting_empty {
                // Atomically check-and-remove: only remove if the posting is still empty.
                // Between dropping the `get_mut` above and here, a concurrent `insert`
                // could have added a new doc_id, making the posting non-empty again.
                if let dashmap::Entry::Occupied(entry) = self.postings.entry(field_value.clone())
                    && entry.get().2.is_empty()
                {
                    entry.remove();
                    self.btree.write().remove(&field_value);
                }
            }

            self.update_metadata(|m| {
                m.stats.version += 1;
                m.stats.last_deleted = now_ms;
                m.stats.delete_count += 1;
            });
        }

        removed
    }

    /// Inserts document_id-field_values to the index
    ///
    /// This method is more efficient than calling insert() multiple times
    /// as it can optimize bucket allocation and reduce lock contention.
    ///
    /// # Arguments
    ///
    /// * `doc_id` - Document identifier
    /// * `field_values` - Array of field values to index for this document
    /// * `now_ms` - Current timestamp in milliseconds
    ///
    /// # Returns
    ///
    /// * `Result<usize, BTreeError>` - Number of items successfully inserted or error
    pub fn insert_array(
        &self,
        doc_id: PK,
        field_values: Vec<FV>,
        now_ms: u64,
    ) -> Result<usize, BTreeError> {
        if field_values.is_empty() {
            return Ok(0);
        }

        // Track which values were successfully inserted
        let mut inserted_count = 0;
        // Track which buckets were modified and need updates
        let mut bucket_updates: FxHashMap<u32, (usize, FxHashSet<FV>)> = FxHashMap::default();
        // New values that need to be added to the B-tree
        let mut new_btree_values = Vec::new();

        // Phase 1: collect existing postings and prepare modifications
        // Skip duplicate field values if not allowed
        if !self.config.allow_duplicates {
            for field_value in &field_values {
                if let Some(posting) = self.postings.get(field_value)
                    && !posting.2.contains(&doc_id)
                {
                    return Err(BTreeError::AlreadyExists {
                        name: self.name.clone(),
                        id: json!(doc_id),
                        value: json!(field_value),
                    });
                }
            }
        }

        // Ensure the current bucket exists (see insert()).
        let bucket_id = self.max_bucket_id.load(Ordering::Relaxed);
        self.buckets
            .entry(bucket_id)
            .or_insert_with(|| (0, false, UniqueVec::default(), 0));

        for field_value in field_values {
            let mut size_increase = 0;
            let mut target_bucket_id = bucket_id;
            match self.postings.entry(field_value.clone()) {
                dashmap::Entry::Occupied(mut entry) => {
                    let posting = entry.get_mut();
                    // Track the posting's actual bucket, not the current max_bucket_id
                    target_bucket_id = posting.0;

                    // Re-check uniqueness constraint atomically while holding the entry lock.
                    // The pre-check above may have passed, but a concurrent insert could have
                    // added a different doc_id between the pre-check and here.
                    if !self.config.allow_duplicates && !posting.2.contains(&doc_id) {
                        return Err(BTreeError::AlreadyExists {
                            name: self.name.clone(),
                            id: json!(doc_id),
                            value: json!(field_value),
                        });
                    }

                    // Only add the doc_id if it's not already present
                    if posting.2.push(doc_id.clone()) {
                        // Calculate size increase for this insertion
                        size_increase = estimate_cbor_size(&doc_id) + 2;
                        posting.1 += 1; // Increment version
                    }
                }
                dashmap::Entry::Vacant(entry) => {
                    // Create a new posting for this field value
                    let posting = (bucket_id, 1, vec![doc_id.clone()].into());
                    size_increase = estimate_cbor_size(&posting) + 2;
                    // Insert the new posting
                    entry.insert(posting);
                    // Remember to add this to the B-tree for range queries
                    new_btree_values.push(field_value.clone());
                }
            };

            if size_increase > 0 {
                // Update the bucket size tracking for the posting's actual bucket
                let bucket_entry = bucket_updates
                    .entry(target_bucket_id)
                    .or_insert_with(|| (0, FxHashSet::default()));
                bucket_entry.0 += size_increase;
                bucket_entry.1.insert(field_value);
                inserted_count += 1;
            }
        }

        // Add all new values to the B-tree in a single operation
        if !new_btree_values.is_empty() {
            self.btree.write().extend(new_btree_values);
        }

        // Phase 2: handle bucket overflow and updates
        // Process each field value individually to avoid migrating existing values unnecessarily.
        // field_values_to_migrate: (old_bucket_id, field_value, size)
        let mut field_values_to_migrate: Vec<(u32, FV, usize)> = Vec::new();
        for (bucket_id, (_size_increase, field_values)) in bucket_updates {
            let mut bucket_entry = self
                .buckets
                .entry(bucket_id)
                .or_insert_with(|| (0, false, UniqueVec::default(), 0));

            self.mark_bucket_dirty(&mut bucket_entry);
            for fv in field_values {
                let fv_size = if let Some(posting) = self.postings.get(&fv) {
                    estimate_cbor_size(&posting) + 2
                } else {
                    continue;
                };

                if bucket_entry.2.contains(&fv) {
                    // Field value already tracked in this bucket; just update the size.
                    bucket_entry.0 = fv_size.max(bucket_entry.0);
                } else if bucket_entry.2.is_empty()
                    || bucket_entry.0 + fv_size < self.config.bucket_overload_size
                {
                    // Bucket has room, add directly
                    bucket_entry.0 += fv_size;
                    bucket_entry.2.push(fv);
                } else {
                    // Bucket full, schedule migration for this new field value
                    field_values_to_migrate.push((bucket_id, fv, fv_size));
                }
            }
        }

        // Phase 3: Create new buckets if needed
        if !field_values_to_migrate.is_empty() {
            let mut next_bucket_id = self.max_bucket_id.fetch_add(1, Ordering::Relaxed) + 1;

            {
                self.buckets
                    .entry(next_bucket_id)
                    .or_insert_with(|| (0, false, UniqueVec::default(), 0));
                // release the lock on the entry
            }

            for (old_bucket_id, field_value, size) in field_values_to_migrate {
                if let Some(mut posting) = self.postings.get_mut(&field_value) {
                    posting.0 = next_bucket_id;
                }

                if let Some(mut ob) = self.buckets.get_mut(&old_bucket_id)
                    && ob.2.swap_remove_if(|k| &field_value == k).is_some()
                {
                    ob.0 = ob.0.saturating_sub(size);
                    // Source bucket must be marked dirty, see insert() migration path.
                    self.mark_bucket_dirty(&mut ob);
                }

                let mut new_bucket = false;
                if let Some(mut nb) = self.buckets.get_mut(&next_bucket_id) {
                    if nb.2.is_empty() || nb.0 + size < self.config.bucket_overload_size {
                        // Bucket has enough space, update directly
                        nb.0 += size;
                        self.mark_bucket_dirty(&mut nb);
                        nb.2.push(field_value.clone());
                    } else {
                        // Bucket doesn't have enough space, need to migrate to the next bucket
                        new_bucket = true;
                    }
                }

                if new_bucket {
                    next_bucket_id = self.max_bucket_id.fetch_add(1, Ordering::Relaxed) + 1;
                    // update the posting's bucket_id again
                    if let Some(mut posting) = self.postings.get_mut(&field_value) {
                        posting.0 = next_bucket_id;
                    }

                    match self.buckets.entry(next_bucket_id) {
                        dashmap::Entry::Vacant(entry) => {
                            // Create a new bucket with the initial size
                            entry.insert((size, true, vec![field_value].into(), 1));
                            self.dirty_bucket_count.fetch_add(1, Ordering::Release);
                        }
                        dashmap::Entry::Occupied(mut entry) => {
                            let bucket_entry = entry.get_mut();
                            bucket_entry.0 += size;
                            self.mark_bucket_dirty(bucket_entry);
                            bucket_entry.2.push(field_value);
                        }
                    }
                }
            }
        }

        // Update metadata if any items were inserted
        if inserted_count > 0 {
            self.update_metadata(|m| {
                m.stats.version += 1;
                m.stats.last_inserted = now_ms;
                m.stats.insert_count += inserted_count as u64;
            });
        }

        Ok(inserted_count)
    }

    /// Batch removes multiple document_id-field_value pairs from the index
    ///
    /// This method is more efficient than calling remove() multiple times
    /// as it can optimize bucket updates and reduce lock contention.
    ///
    /// # Arguments
    ///
    /// * `doc_id` - Document identifier
    /// * `field_values` - Array of field values to remove for this document
    /// * `now_ms` - Current timestamp in milliseconds
    ///
    /// # Returns
    ///
    /// * `usize` - Number of items successfully removed
    pub fn remove_array(&self, doc_id: PK, field_values: Vec<FV>, now_ms: u64) -> usize {
        if field_values.is_empty() {
            return 0;
        }

        // Track removal statistics
        let mut removed_count = 0;
        // Track which buckets were modified
        let mut bucket_updates: FxHashMap<u32, (usize, FxHashSet<FV>)> = FxHashMap::default();
        // Track which field values are completely removed
        let mut values_to_remove = Vec::new();

        // First pass: collect which postings to modify
        for field_value in field_values {
            let mut removed = false;
            let mut size_decrease = 0;
            let mut posting_empty = false;
            let mut bucket_id = 0;

            // Check if this field value exists
            if let Some(mut posting) = self.postings.get_mut(&field_value) {
                bucket_id = posting.0;

                let prev_posting_size = estimate_cbor_size(&*posting) + 2;

                // Check if the document ID exists in the posting
                if posting.2.swap_remove_if(|id| id == &doc_id).is_some() {
                    removed = true;
                    posting.1 += 1; // Increment version
                    posting_empty = posting.2.is_empty();

                    // Calculate size decrease based on whether this key is fully removed.
                    size_decrease = if posting_empty {
                        prev_posting_size
                    } else {
                        estimate_cbor_size(&doc_id) + 2
                    };
                }
            }

            if removed {
                // If posting is now empty, mark for removal
                if posting_empty {
                    values_to_remove.push(field_value.clone());
                }

                // Update bucket tracking
                let bucket_entry = bucket_updates
                    .entry(bucket_id)
                    .or_insert_with(|| (0, FxHashSet::default()));
                bucket_entry.0 += size_decrease;
                bucket_entry.1.insert(field_value);

                removed_count += 1;
            }
        }

        // Remove empty postings from the index and B-tree.
        // Use atomic check-and-remove: a concurrent `insert` might have re-populated
        // a posting between the first pass and here, so only remove if still empty.
        let mut actually_removed = FxHashSet::default();
        for value in &values_to_remove {
            if let dashmap::Entry::Occupied(entry) = self.postings.entry(value.clone())
                && entry.get().2.is_empty()
            {
                entry.remove();
                actually_removed.insert(value.clone());
            }
        }
        if !actually_removed.is_empty() {
            let mut btree = self.btree.write();
            for value in &actually_removed {
                btree.remove(value);
            }
        }

        // Update all modified buckets
        for (bucket_id, (size_decrease, field_values)) in bucket_updates {
            if let Some(mut bucket) = self.buckets.get_mut(&bucket_id) {
                bucket.0 = bucket.0.saturating_sub(size_decrease);
                self.mark_bucket_dirty(&mut bucket); // Mark as dirty

                // Remove field values that are completely removed
                for fv in &field_values {
                    if actually_removed.contains(fv) {
                        bucket.2.swap_remove_if(|k| k == fv);
                    }
                }
            }
        }

        // Update metadata if any items were removed
        if removed_count > 0 {
            self.update_metadata(|m| {
                m.stats.version += 1;
                m.stats.last_deleted = now_ms;
                m.stats.delete_count += removed_count as u64;
            });
        }

        removed_count
    }

    /// Batch updates the index for a document
    ///
    /// # Arguments
    ///
    /// * `doc_id` - doc ID
    /// * `old_field_values` - old field values (without duplicates)
    /// * `new_field_values` - new field values (without duplicates)
    /// * `now_ms` - current timestamp (milliseconds)
    ///
    /// # Returns
    /// * `Result<(usize, usize), BTreeError>` - (removed count, inserted count)
    pub fn batch_update(
        &self,
        doc_id: PK,
        old_field_values: Vec<FV>,
        new_field_values: Vec<FV>,
        now_ms: u64,
    ) -> Result<(usize, usize), BTreeError> {
        // 去重
        let old_set: FxHashSet<_> = old_field_values.into_iter().collect();
        let new_set: FxHashSet<_> = new_field_values.into_iter().collect();

        // 需要插入的值 = 新集合 - 旧集合
        let to_insert: Vec<_> = new_set.difference(&old_set).cloned().collect();
        // 需要删除的值 = 旧集合 - 新集合
        let to_remove: Vec<_> = old_set.difference(&new_set).cloned().collect();

        let inserted = if !to_insert.is_empty() {
            self.insert_array(doc_id.clone(), to_insert, now_ms)?
        } else {
            0
        };

        let removed = if !to_remove.is_empty() {
            self.remove_array(doc_id, to_remove, now_ms)
        } else {
            0
        };

        Ok((removed, inserted))
    }

    /// Queries the index for an exact key match
    ///
    /// # Arguments
    ///
    /// * `field_value` - Key to query for
    /// * `f` - Function to apply to the posting value
    ///
    /// # Returns
    ///
    /// * `Option<R>` - Result of the function applied to the posting value
    pub fn query_with<F, R>(&self, field_value: &FV, f: F) -> Option<R>
    where
        F: FnOnce(&Vec<PK>) -> Option<R>,
    {
        self.query_count.fetch_add(1, Ordering::Relaxed);

        self.postings
            .get(field_value)
            .and_then(|posting| f(&posting.2))
    }

    /// Queries the index using a range query
    ///
    /// # Arguments
    ///
    /// * `query` - Range query specification
    /// * `f` - Function to apply to the posting value. The function should return a tuple
    ///   containing a boolean indicating if the query should continue and an optional result.
    ///
    /// # Returns
    ///
    /// * `Vec<R>` - Vector of results from the function applied to the posting values
    pub fn range_query_with<F, R>(&self, query: RangeQuery<FV>, mut f: F) -> Vec<R>
    where
        F: FnMut(&FV, &Vec<PK>) -> (bool, Vec<R>),
    {
        let mut results = Vec::new();
        if self.postings.is_empty() {
            return results;
        }

        self.query_count.fetch_add(1, Ordering::Relaxed);

        match query {
            RangeQuery::Eq(key) => {
                if let Some(posting) = self.postings.get(&key) {
                    let (conti, rt) = f(&key, &posting.2);
                    results.extend(rt);
                    if !conti {
                        return results;
                    }
                }
            }
            RangeQuery::Gt(start_key) => {
                for k in self.btree.read().range((
                    std::ops::Bound::Excluded(start_key),
                    std::ops::Bound::Unbounded,
                )) {
                    if let Some(posting) = self.postings.get(k) {
                        let (conti, rt) = f(k, &posting.2);
                        results.extend(rt);
                        if !conti {
                            return results;
                        }
                    }
                }
            }
            RangeQuery::Ge(start_key) => {
                for k in self
                    .btree
                    .read()
                    .range(std::ops::RangeFrom { start: start_key })
                {
                    if let Some(posting) = self.postings.get(k) {
                        let (conti, rt) = f(k, &posting.2);
                        results.extend(rt);
                        if !conti {
                            return results;
                        }
                    }
                }
            }
            RangeQuery::Lt(end_key) => {
                // 倒序遍历以支持 limit 提前终止，但最终结果按正序返回
                let mut groups: Vec<Vec<R>> = Vec::new();
                for k in self
                    .btree
                    .read()
                    .range(std::ops::RangeTo { end: end_key })
                    .rev()
                {
                    if let Some(posting) = self.postings.get(k) {
                        let (conti, rt) = f(k, &posting.2);
                        if !rt.is_empty() {
                            groups.push(rt);
                        }
                        if !conti {
                            break;
                        }
                    }
                }
                // 组级反转，保持每个 key 内部顺序不变，整体 key 正序
                return groups.into_iter().rev().flatten().collect();
            }
            RangeQuery::Le(end_key) => {
                let mut groups: Vec<Vec<R>> = Vec::new();
                for k in self
                    .btree
                    .read()
                    .range(std::ops::RangeToInclusive { end: end_key })
                    .rev()
                {
                    if let Some(posting) = self.postings.get(k) {
                        let (conti, rt) = f(k, &posting.2);
                        if !rt.is_empty() {
                            groups.push(rt);
                        }
                        if !conti {
                            break;
                        }
                    }
                }

                // 组级反转，保持每个 key 内部顺序不变，整体 key 正序
                return groups.into_iter().rev().flatten().collect();
            }
            RangeQuery::Between(start_key, end_key) => {
                if start_key > end_key {
                    return results; // empty result for invalid range
                }
                for k in self.btree.read().range(start_key..=end_key) {
                    if let Some(posting) = self.postings.get(k) {
                        let (conti, rt) = f(k, &posting.2);
                        results.extend(rt);
                        if !conti {
                            return results;
                        }
                    }
                }
            }
            RangeQuery::Include(keys) => {
                let keys = BTreeSet::from_iter(keys);
                for k in keys.into_iter() {
                    if let Some(posting) = self.postings.get(&k) {
                        let (conti, rt) = f(&k, &posting.2);
                        results.extend(rt);
                        if !conti {
                            return results;
                        }
                    }
                }
            }
            RangeQuery::And(queries) => {
                // 先找出最小结果集的子查询，减少交集计算量
                let keys = self.range_keys(RangeQuery::And(queries));
                for k in keys {
                    if let Some(posting) = self.postings.get(&k) {
                        let (conti, rt) = f(&k, &posting.2);
                        results.extend(rt);
                        if !conti {
                            return results;
                        }
                    }
                }
            }
            RangeQuery::Or(queries) => {
                let keys = self.range_keys(RangeQuery::Or(queries));
                for k in keys {
                    if let Some(posting) = self.postings.get(&k) {
                        let (conti, rt) = f(&k, &posting.2);
                        results.extend(rt);
                        if !conti {
                            return results;
                        }
                    }
                }
            }
            RangeQuery::Not(query) => {
                // 先收集要排除的 key，再遍历全集差集
                let exclude: FxHashSet<FV> = self.range_keys(*query).into_iter().collect();

                for k in self.btree.read().iter() {
                    if exclude.contains(k) {
                        continue;
                    }
                    if let Some(posting) = self.postings.get(k) {
                        let (conti, rt) = f(k, &posting.2);
                        results.extend(rt);
                        if !conti {
                            return results;
                        }
                    }
                }
            }
        }

        results
    }

    /// Returns a vector of keys in the index
    /// This method is useful for iterating over all keys in the index.
    /// It supports pagination with `cursor` and `limit` parameters.
    /// # Arguments
    ///
    /// * `cursor` - The cursor to start pagination from (exclusive)
    /// * `limit` - Maximum number of keys to return
    ///
    /// # Returns
    ///
    /// * `Vec<FV>` - Vector of field values (keys) in the index
    ///
    pub fn keys(&self, cursor: Option<FV>, limit: Option<usize>) -> Vec<FV> {
        match (cursor, limit) {
            (Some(cursor), Some(limit)) => self
                .btree
                .read()
                .range((
                    std::ops::Bound::Excluded(cursor),
                    std::ops::Bound::Unbounded,
                ))
                .take(limit)
                .cloned()
                .collect(),
            (Some(cursor), None) => self
                .btree
                .read()
                .range((
                    std::ops::Bound::Excluded(cursor),
                    std::ops::Bound::Unbounded,
                ))
                .cloned()
                .collect(),
            (None, Some(limit)) => self.btree.read().iter().take(limit).cloned().collect(),
            (None, None) => self.btree.read().iter().cloned().collect(),
        }
    }

    fn range_keys(&self, query: RangeQuery<FV>) -> Vec<FV> {
        let mut results: Vec<FV> = Vec::new();

        match query {
            RangeQuery::Eq(key) => {
                if self.btree.read().contains(&key) {
                    results.push(key);
                }
            }
            RangeQuery::Gt(start_key) => {
                results.extend(
                    self.btree
                        .read()
                        .range((
                            std::ops::Bound::Excluded(start_key),
                            std::ops::Bound::Unbounded,
                        ))
                        .cloned(),
                );
            }
            RangeQuery::Ge(start_key) => {
                results.extend(
                    self.btree
                        .read()
                        .range(std::ops::RangeFrom { start: start_key })
                        .cloned(),
                );
            }
            RangeQuery::Lt(end_key) => {
                results.extend(
                    self.btree
                        .read()
                        .range(std::ops::RangeTo { end: end_key })
                        .cloned(),
                );
            }
            RangeQuery::Le(end_key) => {
                results.extend(
                    self.btree
                        .read()
                        .range(std::ops::RangeToInclusive { end: end_key })
                        .cloned(),
                );
            }
            RangeQuery::Between(start_key, end_key) => {
                results.extend(self.btree.read().range(start_key..=end_key).cloned());
            }
            RangeQuery::Include(keys) => {
                let keys = BTreeSet::from_iter(keys);
                let btree = self.btree.read();
                results.extend(keys.into_iter().filter(|k| btree.contains(k)));
            }
            RangeQuery::And(queries) => {
                let mut iter = queries.into_iter();
                if let Some(query) = iter.next() {
                    let mut intersection: BTreeSet<FV> =
                        self.range_keys(*query).into_iter().collect();

                    for query in iter {
                        let keys: FxHashSet<FV> = self.range_keys(*query).into_iter().collect();
                        intersection.retain(|k| keys.contains(k));
                        if intersection.is_empty() {
                            return vec![];
                        }
                    }

                    results.extend(intersection);
                }
            }
            RangeQuery::Or(queries) => {
                // Use BTreeSet to ensure keys are returned in global B-tree order,
                // so that early-stop/limit semantics stay deterministic.
                let mut merged = BTreeSet::new();
                for query in queries {
                    merged.extend(self.range_keys(*query));
                }
                results.extend(merged);
            }
            RangeQuery::Not(query) => {
                let exclude: FxHashSet<FV> = self.range_keys(*query).into_iter().collect();
                results.extend(
                    self.btree
                        .read()
                        .iter()
                        .filter(|k| !exclude.contains(k))
                        .cloned(),
                );
            }
        }

        results
    }

    /// Stores the index metadata and dirty buckets to persistent storage.
    pub async fn flush<W: Write, F>(
        &self,
        metadata: W,
        now_ms: u64,
        f: F,
    ) -> Result<bool, BTreeError>
    where
        F: AsyncFnMut(u32, &[u8]) -> Result<bool, BoxError>,
    {
        let meta_saved = self.store_metadata(metadata, now_ms)?;
        let had_dirty = self.has_dirty_buckets();
        if !meta_saved && !had_dirty {
            return Ok(false);
        }
        self.store_dirty_buckets(f).await?;
        Ok(meta_saved || had_dirty)
    }

    /// Returns whether there are dirty buckets pending persistence.
    pub fn has_dirty_buckets(&self) -> bool {
        self.dirty_bucket_count.load(Ordering::Acquire) > 0
    }

    /// Compacts fragmented buckets by re-binning all field values into fewer, properly-sized
    /// buckets using a first-fit-decreasing bin-packing strategy.
    ///
    /// This is intended as a one-time repair after the bucket-splitting bug that created
    /// many tiny buckets. After compaction all buckets are marked dirty and will be
    /// persisted on the next [`flush`](Self::flush) call.
    ///
    /// # Returns
    ///
    /// `(old_bucket_count, new_bucket_count)`
    pub fn compact_buckets(&self) -> (usize, usize) {
        let old_count = self.buckets.len();
        if old_count <= 1 {
            return (old_count, old_count);
        }

        // Step 1: Estimate each field value's serialized contribution.
        let mut fv_sizes: Vec<(FV, usize)> = self
            .postings
            .iter()
            .map(|entry| {
                let size = estimate_cbor_size(&(entry.key(), entry.value())) + 2;
                (entry.key().clone(), size)
            })
            .collect();

        if fv_sizes.is_empty() {
            self.buckets.clear();
            self.buckets.insert(0, (0, true, UniqueVec::default(), 1));
            self.max_bucket_id.store(0, Ordering::Relaxed);
            self.dirty_bucket_count.store(1, Ordering::Release);
            self.update_metadata(|m| {
                m.stats.version += 1;
            });
            return (old_count, 1);
        }

        // Step 2: Sort by size descending for better packing.
        fv_sizes.sort_unstable_by(|a, b| b.1.cmp(&a.1));

        // Step 3: First-fit-decreasing bin packing.
        let limit = self.config.bucket_overload_size;
        // Each bin: (accumulated_size, field_values)
        let mut bins: Vec<(usize, Vec<FV>)> = Vec::new();

        for (fv, size) in fv_sizes {
            if let Some(bin) = bins.iter_mut().find(|b| b.0 + size < limit) {
                bin.0 += size;
                bin.1.push(fv);
            } else {
                bins.push((size, vec![fv]));
            }
        }

        // Step 4: Rebuild buckets.
        self.buckets.clear();
        let new_count = bins.len();
        let max_id = new_count.saturating_sub(1) as u32;

        for (i, (size, field_values)) in bins.into_iter().enumerate() {
            let bucket_id = i as u32;

            // Update posting references.
            for fv in &field_values {
                if let Some(mut posting) = self.postings.get_mut(fv) {
                    posting.0 = bucket_id;
                }
            }

            self.buckets
                .insert(bucket_id, (size, true, field_values.into(), 1));
        }

        self.max_bucket_id.store(max_id, Ordering::Relaxed);
        self.dirty_bucket_count
            .store(new_count as u32, Ordering::Release);
        self.update_metadata(|m| {
            m.stats.version += 1;
        });

        (old_count, new_count)
    }

    /// Stores the index metadata to a writer
    ///
    /// # Arguments
    ///
    /// * `w` - Any type implementing the [`Write`] trait
    /// * `now_ms` - Current timestamp in milliseconds
    ///
    /// # Returns
    ///
    /// * `Result<bool, Error>` - true if the metadata was saved, false if the version was not updated
    pub fn store_metadata<W: Write>(&self, w: W, now_ms: u64) -> Result<bool, BTreeError> {
        // Fast path: if the version is already saved, avoid cloning metadata.
        let current_version = { self.metadata.read().stats.version };
        if self.last_saved_version.load(Ordering::Relaxed) >= current_version {
            return Ok(false);
        }

        let mut meta = self.metadata();
        // Atomically claim the right to serialize this version.
        // Only one concurrent caller will see prev < meta.stats.version and proceed.
        let prev_saved_version = self
            .last_saved_version
            .fetch_max(meta.stats.version, Ordering::Relaxed);
        if prev_saved_version >= meta.stats.version {
            // No need to save if the version is not updated
            return Ok(false);
        }

        meta.stats.last_saved = now_ms.max(meta.stats.last_saved);
        if let Err(err) = ciborium::into_writer(&BTreeIndexRef { metadata: &meta }, w) {
            // Serialization failed: try to revert only if no other writer has already
            // advanced this atomic to a newer version.
            let _ = self.last_saved_version.compare_exchange(
                meta.stats.version,
                prev_saved_version,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
            return Err(BTreeError::Serialization {
                name: self.name.clone(),
                source: err.into(),
            });
        }

        self.update_metadata(|m| {
            m.stats.last_saved = meta.stats.last_saved.max(m.stats.last_saved);
        });

        Ok(true)
    }

    /// Stores dirty buckets to persistent storage using the provided async function
    ///
    /// This method iterates through all buckets and persists those that have been modified
    /// since the last save operation.
    ///
    /// # Arguments
    ///
    /// * `f` - Async function that writes bucket data to persistent storage
    ///   The function takes a bucket ID and serialized data, and returns whether to continue
    ///
    /// # Returns
    ///
    /// * `Result<(), BTreeError>` - Success or error
    pub async fn store_dirty_buckets<F>(&self, mut f: F) -> Result<(), BTreeError>
    where
        F: AsyncFnMut(u32, &[u8]) -> Result<bool, BoxError>,
    {
        let dirty_count_snapshot = self.dirty_bucket_count.load(Ordering::Acquire);
        if dirty_count_snapshot == 0 {
            return Ok(());
        }

        // Snapshot dirty bucket IDs first to avoid holding mutable bucket refs across await.
        let dirty_bucket_ids: Vec<u32> = self
            .buckets
            .iter()
            .filter_map(|b| if b.1 { Some(*b.key()) } else { None })
            .collect();

        // Self-heal: if the counter indicates dirty buckets but none are actually dirty,
        // attempt to reset the counter only when it has not changed since snapshot.
        // This avoids permanently reporting dirty state due to possible counter drift.
        if dirty_bucket_ids.is_empty() {
            let _ = self.dirty_bucket_count.compare_exchange(
                dirty_count_snapshot,
                0,
                Ordering::AcqRel,
                Ordering::Relaxed,
            );
            return Ok(());
        }

        let mut buf = Vec::with_capacity(4096);
        for bucket_id in dirty_bucket_ids {
            let Some(bucket) = self.buckets.get(&bucket_id) else {
                continue;
            };
            if !bucket.1 {
                continue;
            }
            let dirty_version = bucket.3;

            // If the bucket is dirty, it needs to be persisted.
            // Scope the postings Ref guards so they are dropped before the
            // potentially slow async write, reducing lock contention.
            {
                let postings: FxHashMap<_, _> = bucket
                    .2
                    .iter()
                    .filter_map(|fv| self.postings.get(fv).map(|p| (fv, p)))
                    .collect();

                buf.clear();
                ciborium::into_writer(
                    &BucketRef {
                        postings: &postings,
                    },
                    &mut buf,
                )
                .map_err(|err| BTreeError::Serialization {
                    name: self.name.clone(),
                    source: err.into(),
                })?;
            }
            drop(bucket);

            let conti = f(bucket_id, &buf)
                .await
                .map_err(|err| BTreeError::Generic {
                    name: self.name.clone(),
                    source: err,
                })?;

            // Only mark as clean if it is still dirty after persistence.
            if let Some(mut bucket) = self.buckets.get_mut(&bucket_id)
                && bucket.1
                && bucket.3 == dirty_version
            {
                bucket.1 = false;
                let _ = self.dirty_bucket_count.fetch_update(
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                    |v| Some(v.saturating_sub(1)),
                );
            }

            if !conti {
                return Ok(());
            }
        }

        Ok(())
    }

    /// Updates the index metadata
    ///
    /// # Arguments
    ///
    /// * `f` - Function that modifies the metadata
    fn update_metadata<F>(&self, f: F)
    where
        F: FnOnce(&mut BTreeMetadata),
    {
        let mut metadata = self.metadata.write();
        f(&mut metadata);
    }
}

impl<PK> BTreeIndex<PK, String>
where
    PK: Ord + Debug + Clone + Serialize + DeserializeOwned,
{
    /// Specialized version of prefix query for String type
    /// Searches the index using a prefix.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Prefix to query for
    /// * `f` - Function to apply to the posting value. The function should return a tuple
    ///   containing a boolean indicating if the query should continue and an optional result.
    ///
    /// # Returns
    /// * `Vec<R>` - Vector of results from the function applied to the posting values
    pub fn prefix_query_with<F, R>(&self, prefix: &str, mut f: F) -> Vec<R>
    where
        F: FnMut(&str, &Vec<PK>) -> (bool, Option<R>),
    {
        let mut results = Vec::new();
        if self.postings.is_empty() {
            return results;
        }

        self.query_count.fetch_add(1, Ordering::Relaxed);
        // 空前缀：遍历全部键
        if prefix.is_empty() {
            for k in self.btree.read().iter() {
                if let Some(posting) = self.postings.get(k) {
                    let (con, rt) = f(k, &posting.2);
                    if let Some(r) = rt {
                        results.push(r);
                    }
                    if !con {
                        break;
                    }
                }
            }
            return results;
        }

        // [lower, upper] 区间：upper = prefix + char::MAX，覆盖所有以 prefix 开头的字符串
        let lower = prefix.to_string();
        let mut upper = String::with_capacity(prefix.len() + 4);
        upper.push_str(prefix);
        upper.push(char::MAX);

        for k in self.btree.read().range(lower..=upper) {
            if let Some(posting) = self.postings.get(k) {
                let (con, rt) = f(k, &posting.2);
                if let Some(r) = rt {
                    results.push(r);
                }
                if !con {
                    break;
                }
            }
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::{Barrier, Mutex};

    // 获取当前时间戳（毫秒）
    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    // 辅助函数：创建一个测试用的 B-tree 索引
    fn create_test_index() -> BTreeIndex<u64, String> {
        let config = BTreeConfig {
            bucket_overload_size: 1024,
            allow_duplicates: true,
        };
        BTreeIndex::new("test_index".to_string(), Some(config))
    }

    // 辅助函数：创建一个测试用的 B-tree 索引并插入一些数据
    fn create_populated_index() -> BTreeIndex<u64, String> {
        let index = create_test_index();

        // 插入一些测试数据
        let _ = index.insert(1, "apple".to_string(), now_ms());
        let _ = index.insert(2, "banana".to_string(), now_ms());
        let _ = index.insert(3, "cherry".to_string(), now_ms());
        let _ = index.insert(4, "date".to_string(), now_ms());
        let _ = index.insert(5, "eggplant".to_string(), now_ms());

        // 测试重复键
        let _ = index.insert(6, "apple".to_string(), now_ms());
        let _ = index.insert(7, "banana".to_string(), now_ms());

        index
    }

    #[test]
    fn test_create_index() {
        let index = create_test_index();

        assert_eq!(index.name(), "test_index");
        assert_eq!(index.len(), 0);
        assert!(index.is_empty());

        let metadata = index.metadata();
        assert_eq!(metadata.name, "test_index");
        assert_eq!(metadata.stats.num_elements, 0);
    }

    #[test]
    fn test_insert() {
        let index = create_test_index();

        // 测试插入
        let result = index.insert(1, "apple".to_string(), now_ms());
        assert!(result.is_ok());
        assert!(result.unwrap());

        assert_eq!(index.len(), 1);
        assert!(!index.is_empty());

        // 测试重复插入相同的文档ID和字段值
        let result = index.insert(1, "apple".to_string(), now_ms());
        assert!(result.is_ok());
        assert!(!result.unwrap()); // 应该返回 false，因为没有实际插入新数据

        // 测试插入相同字段值但不同文档ID
        let result = index.insert(2, "apple".to_string(), now_ms());
        assert!(result.is_ok());
        assert!(result.unwrap());

        // 测试不允许重复键的情况
        let config = BTreeConfig {
            bucket_overload_size: 1024,
            allow_duplicates: false,
        };
        let unique_index = BTreeIndex::new("unique_index".to_string(), Some(config));

        let result = unique_index.insert(1, "apple".to_string(), now_ms());
        assert!(result.is_ok());

        // unique 索引：重复插入同一个 doc_id 应该是幂等的
        let result = unique_index.insert(1, "apple".to_string(), now_ms());
        assert!(result.is_ok());
        assert!(!result.unwrap());

        let result = unique_index.insert(2, "apple".to_string(), now_ms());
        assert!(result.is_err());
        match result {
            Err(BTreeError::AlreadyExists { .. }) => (),
            _ => panic!("Expected AlreadyExists error"),
        }
    }

    #[test]
    fn test_insert_idempotent_does_not_update_stats() {
        let index = create_test_index();

        let inserted = index.insert(1, "apple".to_string(), now_ms()).unwrap();
        assert!(inserted);
        let stats_after_first = index.stats();

        let inserted = index.insert(1, "apple".to_string(), now_ms()).unwrap();
        assert!(!inserted);
        let stats_after_second = index.stats();

        assert_eq!(stats_after_first.insert_count, 1);
        assert_eq!(
            stats_after_second.insert_count,
            stats_after_first.insert_count
        );
        assert_eq!(stats_after_second.version, stats_after_first.version);
    }

    #[test]
    fn test_remove() {
        let index = create_populated_index();

        // 测试删除存在的条目
        let result = index.remove(1, "apple".to_string(), now_ms());
        assert!(result);

        // 测试删除不存在的条目
        let result = index.remove(100, "nonexistent".to_string(), now_ms());
        assert!(!result);

        // key 存在但 doc_id 不存在：应该返回 false
        let result = index.remove(999, "banana".to_string(), now_ms());
        assert!(!result);

        // 测试删除后的搜索
        let result = index.query_with(&"apple".to_string(), |ids| Some(ids.clone()));
        assert!(result.is_some());
        let ids = result.unwrap();
        assert!(!ids.contains(&1)); // ID 1 已被删除
        assert!(ids.contains(&6)); // ID 6 仍然存在

        // 测试删除所有相关文档后，键应该被完全移除
        let result = index.remove(6, "apple".to_string(), now_ms());
        assert!(result);

        let result = index.query_with(&"apple".to_string(), |ids| Some(ids.clone()));
        assert!(result.is_none()); // 键应该已经被完全移除
    }

    #[test]
    fn test_query() {
        let index = create_populated_index();

        // 测试精确搜索
        let result = index.query_with(&"apple".to_string(), |ids| Some(ids.clone()));
        assert!(result.is_some());
        let ids = result.unwrap();
        assert!(ids.contains(&1));
        assert!(ids.contains(&6));

        // 测试搜索不存在的键
        let result = index.query_with(&"nonexistent".to_string(), |ids| Some(ids.clone()));
        assert!(result.is_none());
    }

    #[test]
    fn test_range_query() {
        let index = create_populated_index();
        let apple = "apple".to_string();
        let banana = "banana".to_string();
        let cherry = "cherry".to_string();
        let date = "date".to_string();
        let eggplant = "eggplant".to_string();

        // 测试等于查询
        let query = RangeQuery::Eq(apple.clone());
        let results =
            index.range_query_with(query, |k, ids| (true, vec![(k.clone(), ids.clone())]));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "apple");

        // 测试大于查询
        let query = RangeQuery::Gt(cherry.clone());
        let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
        assert_eq!(results.len(), 2);
        assert!(results.contains(&"date".to_string()));
        assert!(results.contains(&"eggplant".to_string()));

        // 测试大于等于查询
        let query = RangeQuery::Ge(cherry.clone());
        let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
        assert_eq!(results.len(), 3);
        assert!(results.contains(&"cherry".to_string()));

        // 测试小于查询
        let query = RangeQuery::Lt(cherry.clone());
        let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
        assert_eq!(results.len(), 2);
        assert!(results.contains(&apple));
        assert!(results.contains(&banana));

        // 测试小于等于查询
        let query = RangeQuery::Le(cherry.clone());
        let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
        assert_eq!(results.len(), 3);
        assert!(results.contains(&cherry));

        // 测试范围查询
        let query = RangeQuery::Between(banana.clone(), date.clone());
        let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
        assert_eq!(results.len(), 3);
        assert!(results.contains(&banana));
        assert!(results.contains(&cherry));
        assert!(results.contains(&date));

        // 测试包含查询
        let keys = vec![apple.clone(), eggplant.clone()];
        let query = RangeQuery::Include(keys);
        let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
        assert_eq!(results.len(), 2);
        assert!(results.contains(&apple));
        assert!(results.contains(&eggplant));

        // 测试提前终止搜索
        let query = RangeQuery::Ge(apple.clone());
        let results = index.range_query_with(query, |k, _| (k != "banana", vec![k.clone()]));
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], "apple");
        assert_eq!(results[1], "banana");
    }

    #[test]
    fn test_logical_queries() {
        let index = create_populated_index();

        // 额外插入一些测试数据以丰富测试用例
        let _ = index.insert(8, "grape".to_string(), now_ms());
        let _ = index.insert(9, "fig".to_string(), now_ms());
        let _ = index.insert(10, "berry".to_string(), now_ms());
        let _ = index.insert(11, "berry".to_string(), now_ms());

        // 准备常用的查询键
        let apple = "apple".to_string();
        let banana = "banana".to_string();
        let berry = "berry".to_string();
        let cherry = "cherry".to_string();
        let date = "date".to_string();
        let eggplant = "eggplant".to_string();
        let fig = "fig".to_string();
        let grape = "grape".to_string();

        // ===== 测试 AND 操作 =====
        // 测试两个有交集的范围的 AND 操作
        let query = RangeQuery::And(vec![
            Box::new(RangeQuery::Le(date.clone())), // <= date (apple, banana, cherry, date)
            Box::new(RangeQuery::Ge(cherry.clone())), // >= cherry (cherry, date, eggplant, fig, grape)
        ]);

        let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
        assert_eq!(results.len(), 2);
        assert!(results.contains(&cherry));
        assert!(results.contains(&date));

        // 测试空交集的 AND 操作
        let query = RangeQuery::And(vec![
            Box::new(RangeQuery::Lt(cherry.clone())), // < cherry (apple, banana)
            Box::new(RangeQuery::Gt(date.clone())),   // > date (eggplant, fig, grape)
        ]);

        let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
        assert_eq!(results.len(), 0); // 应该为空集

        // 测试精确匹配和范围查询的 AND 操作
        let query = RangeQuery::And(vec![
            Box::new(RangeQuery::Ge(banana.clone())),   // >= banana
            Box::new(RangeQuery::Lt(eggplant.clone())), // < eggplant
            Box::new(RangeQuery::Eq(cherry.clone())),   // == cherry
        ]);

        let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
        assert_eq!(results.len(), 1);
        assert!(results.contains(&cherry));

        // ===== 测试 OR 操作 =====
        // 测试两个不相交范围的 OR 操作
        let query = RangeQuery::Or(vec![
            Box::new(RangeQuery::Le(banana.clone())), // <= banana (apple, banana)
            Box::new(RangeQuery::Ge(fig.clone())),    // >= fig (fig, grape)
        ]);

        let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
        assert_eq!(results.len(), 4);
        assert!(results.contains(&apple));
        assert!(results.contains(&banana));
        assert!(results.contains(&fig));
        assert!(results.contains(&grape));

        // 测试有重叠的 OR 操作
        let query = RangeQuery::Or(vec![
            Box::new(RangeQuery::Between(banana.clone(), date.clone())), // banana到date
            Box::new(RangeQuery::Between(cherry.clone(), fig.clone())),  // cherry到fig
        ]);

        let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
        assert_eq!(results.len(), 6);
        assert!(results.contains(&banana));
        assert!(results.contains(&berry));
        assert!(results.contains(&cherry));
        assert!(results.contains(&date));
        assert!(results.contains(&eggplant));
        assert!(results.contains(&fig));

        // ===== 测试 NOT 操作 =====
        // 测试基本的 NOT 操作
        let query = RangeQuery::Not(Box::new(RangeQuery::Between(
            cherry.clone(),
            eggplant.clone(),
        )));

        let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
        assert!(results.contains(&apple));
        assert!(results.contains(&banana));
        assert!(results.contains(&fig));
        assert!(results.contains(&grape));
        assert!(!results.contains(&cherry));
        assert!(!results.contains(&date));
        assert!(!results.contains(&eggplant));

        // 测试 NOT + Eq 操作
        let query = RangeQuery::Not(Box::new(RangeQuery::Eq(apple.clone())));

        let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
        assert!(!results.contains(&apple));
        assert!(results.contains(&banana));
        assert!(results.contains(&cherry));
        // ...验证其它键

        // ===== 测试复合逻辑查询 =====
        // 测试 AND(OR, OR) 复杂嵌套
        let query = RangeQuery::And(vec![
            Box::new(RangeQuery::Or(vec![
                Box::new(RangeQuery::Le(cherry.clone())), // <= cherry
                Box::new(RangeQuery::Ge(fig.clone())),    // >= fig
            ])),
            Box::new(RangeQuery::Or(vec![
                Box::new(RangeQuery::Le(banana.clone())),   // <= banana
                Box::new(RangeQuery::Ge(eggplant.clone())), // >= eggplant
            ])),
        ]);

        let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
        assert!(results.contains(&apple));
        assert!(results.contains(&banana));
        assert!(results.contains(&fig));
        assert!(results.contains(&grape));
        assert!(!results.contains(&cherry));
        assert!(!results.contains(&date));

        // 测试 OR(NOT, NOT) 复杂嵌套
        let query = RangeQuery::Or(vec![
            Box::new(RangeQuery::Not(Box::new(RangeQuery::Ge(date.clone())))), // NOT >= date
            Box::new(RangeQuery::Not(Box::new(RangeQuery::Le(cherry.clone())))), // NOT <= cherry
        ]);

        let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
        // 这应该返回所有键，因为每个键要么 < date 要么 > cherry
        assert_eq!(results.len(), index.len());

        // 测试 NOT(AND) 复合操作
        let query = RangeQuery::Not(Box::new(RangeQuery::And(vec![
            Box::new(RangeQuery::Ge(cherry.clone())),   // >= cherry
            Box::new(RangeQuery::Le(eggplant.clone())), // <= eggplant
        ])));

        let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
        assert!(results.contains(&apple));
        assert!(results.contains(&banana));
        assert!(results.contains(&fig));
        assert!(results.contains(&grape));
        assert!(!results.contains(&cherry));
        assert!(!results.contains(&date));
        assert!(!results.contains(&eggplant));

        // 测试提前终止功能
        let query = RangeQuery::Or(vec![
            Box::new(RangeQuery::Ge(apple.clone())),
            Box::new(RangeQuery::Le(grape.clone())),
        ]);

        let mut count = 0;
        let results = index.range_query_with(query, |_, _| {
            count += 1;
            (count < 3, vec![count.to_string()])
        });

        assert_eq!(results.len(), 3);
        assert_eq!(count, 3); // 确认查询在第三项后停止
    }

    #[test]
    fn test_range_query_lt_le_full_order() {
        let index = create_populated_index();
        // keys: apple < banana < cherry < date < eggplant

        // Lt(date) -> apple, banana, cherry (正序)
        let results = index.range_query_with(RangeQuery::Lt("date".to_string()), |k, _| {
            (true, vec![k.clone()])
        });
        assert_eq!(results, vec!["apple", "banana", "cherry"]);

        // Le(date) -> apple, banana, cherry, date (正序)
        let results = index.range_query_with(RangeQuery::Le("date".to_string()), |k, _| {
            (true, vec![k.clone()])
        });
        assert_eq!(results, vec!["apple", "banana", "cherry", "date"]);
    }

    #[test]
    fn test_range_query_lt_le_with_early_stop_limit_semantics() {
        let index = create_populated_index();
        // 目标：模拟“小于 date 的 2 条数据”，即距离 date 最近的 2 个 key，且最终为正序

        // Lt(date): 倒序遍历是 cherry, banana, apple，截断 2 个 => [cherry, banana]，最终正序 [banana, cherry]
        let mut count = 0usize;
        let results = index.range_query_with(RangeQuery::Lt("date".to_string()), |k, _| {
            count += 1;
            (count < 2, vec![k.clone()])
        });
        assert_eq!(results, vec!["banana", "cherry"]);

        // Le(date): 倒序遍历是 date, cherry, banana, apple，截断 2 个 => [date, cherry]，最终正序 [cherry, date]
        let mut count = 0usize;
        let results = index.range_query_with(RangeQuery::Le("date".to_string()), |k, _| {
            count += 1;
            (count < 2, vec![k.clone()])
        });
        assert_eq!(results, vec!["cherry", "date"]);

        // 再测试当“上限”大于可返回数量时，返回全部（正序）
        let mut count = 0usize;
        let results = index.range_query_with(RangeQuery::Lt("banana".to_string()), |k, _| {
            count += 1;
            (count < 10, vec![k.clone()])
        });
        assert_eq!(results, vec!["apple"]);
    }

    #[test]
    fn test_range_query_lt_le_group_order_preserved() {
        let index = create_populated_index();
        // 让每个 key 返回多个结果，验证倒序遍历后“组内顺序”保持，并最终整体正序
        // 取 Lt(date) 最近 2 个 key：banana, cherry，最终顺序应为：
        // banana-1, banana-2, cherry-1, cherry-2

        let mut count = 0usize;
        let results = index.range_query_with(RangeQuery::Lt("date".to_string()), |k, _| {
            count += 1;
            let v = vec![format!("{k}-1"), format!("{k}-2")];
            (count < 2, v)
        });
        assert_eq!(
            results,
            vec![
                "banana-1".to_string(),
                "banana-2".to_string(),
                "cherry-1".to_string(),
                "cherry-2".to_string()
            ]
        );

        // Le(date) 最近 2 个：cherry, date，组内顺序保持：
        // cherry-1, cherry-2, date-1, date-2
        let mut count = 0usize;
        let results = index.range_query_with(RangeQuery::Le("date".to_string()), |k, _| {
            count += 1;
            let v = vec![format!("{k}-1"), format!("{k}-2")];
            (count < 2, v)
        });
        assert_eq!(
            results,
            vec![
                "cherry-1".to_string(),
                "cherry-2".to_string(),
                "date-1".to_string(),
                "date-2".to_string()
            ]
        );
    }

    #[test]
    fn test_range_keys() {
        let index = create_populated_index();

        // 测试 range_keys 方法处理 And 逻辑
        let apple = "apple".to_string();
        let banana = "banana".to_string();
        let cherry = "cherry".to_string();
        let eggplant = "eggplant".to_string();

        let query = RangeQuery::And(vec![
            Box::new(RangeQuery::Ge(banana.clone())),
            Box::new(RangeQuery::Le(cherry.clone())),
        ]);

        let keys = index.range_keys(query);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&banana));
        assert!(keys.contains(&cherry));

        // 测试 range_keys 方法处理 Or 逻辑
        let query = RangeQuery::Or(vec![
            Box::new(RangeQuery::Eq(apple.clone())),
            Box::new(RangeQuery::Eq(eggplant.clone())),
        ]);

        let keys = index.range_keys(query);
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&apple));
        assert!(keys.contains(&eggplant));

        // 测试 range_keys 方法处理 Not 逻辑
        let query = RangeQuery::Not(Box::new(RangeQuery::Eq(apple.clone())));

        let keys = index.range_keys(query);
        assert!(!keys.contains(&apple));
        assert!(keys.contains(&banana));
        assert!(keys.contains(&cherry));
    }

    #[test]
    fn test_prefix_query() {
        let index = create_populated_index();

        // 插入一些带前缀的数据
        let _ = index.insert(10, "app".to_string(), now_ms());
        let _ = index.insert(11, "application".to_string(), now_ms());

        // 测试前缀搜索
        let results = index.prefix_query_with("app", |k, _| (true, Some(k.to_string())));
        assert_eq!(results.len(), 3);
        assert!(results.contains(&"app".to_string()));
        assert!(results.contains(&"apple".to_string()));
        assert!(results.contains(&"application".to_string()));

        // 测试提前终止搜索
        let results = index.prefix_query_with("app", |k, _| (k != "apple", Some(k.to_string())));
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], "app");
        assert_eq!(results[1], "apple");
    }

    #[tokio::test]
    async fn test_serialization() {
        let index = create_populated_index();

        // 序列化元数据
        let mut buf = Vec::new();
        let result = index.store_metadata(&mut buf, now_ms());
        assert!(result.is_ok());

        println!("Serialized metadata: {:?}", hex::encode(&buf));

        // 反序列化元数据
        let result = BTreeIndex::<u64, String>::load_metadata(&buf[..]);
        let mut loaded_index = result.unwrap();

        // 验证元数据
        assert_eq!(loaded_index.name(), "test_index");
        assert_eq!(loaded_index.len(), 0); // 注意：load_metadata 只加载元数据，不加载 postings

        // 模拟 load_buckets 函数
        let bucket_data = Arc::new(Mutex::new(Vec::new()));

        // 存储 bucket 数据
        {
            let bucket_data_clone = bucket_data.clone();
            let result = index
                .store_dirty_buckets(async |bucket_id, data| {
                    let mut guard = bucket_data_clone.lock().await;
                    while guard.len() <= bucket_id as usize {
                        guard.push(Vec::new());
                    }
                    guard[bucket_id as usize] = data.to_vec();
                    Ok(true)
                })
                .await;
            assert!(result.is_ok());
        }

        // 加载 bucket 数据
        {
            let bucket_data_clone = bucket_data.clone();
            let result = loaded_index
                .load_buckets(async |bucket_id| {
                    let guard = bucket_data_clone.lock().await;
                    if bucket_id as usize >= guard.len() {
                        return Err(BTreeError::Generic {
                            name: "test".to_string(),
                            source: "Bucket not found".into(),
                        }
                        .into());
                    }
                    Ok(Some(guard[bucket_id as usize].clone()))
                })
                .await;
            assert!(result.is_ok());
        }

        // 验证加载后的索引
        assert_eq!(loaded_index.len(), index.len());

        // 测试搜索
        let result = loaded_index.query_with(&"apple".to_string(), |ids| Some(ids.clone()));
        assert!(result.is_some());
        let ids = result.unwrap();
        assert!(ids.contains(&1));
        assert!(ids.contains(&6));
    }

    #[tokio::test]
    async fn test_flush_persists_dirty_buckets_even_if_metadata_already_saved() {
        let index = create_test_index();
        let _ = index.insert(1, "apple".to_string(), now_ms()).unwrap();

        // 先只保存元数据（模拟：元数据已落盘，但 buckets 还没落盘）
        let mut meta_buf = Vec::new();
        assert!(index.store_metadata(&mut meta_buf, now_ms()).unwrap());
        assert!(index.has_dirty_buckets());

        // 再 flush：即使元数据版本没变化，也应继续持久化 dirty buckets
        let writes = Arc::new(Mutex::new(0usize));
        let writes_clone = writes.clone();
        let mut meta_buf2 = Vec::new();
        let saved = index
            .flush(&mut meta_buf2, now_ms(), async move |_, _| {
                let mut g = writes_clone.lock().await;
                *g += 1;
                Ok(true)
            })
            .await
            .unwrap();

        assert!(saved);
        assert_eq!(*writes.lock().await, 1);
        assert!(!index.has_dirty_buckets());
    }

    #[tokio::test]
    async fn test_store_dirty_buckets_propagates_write_error() {
        let index = create_test_index();
        index.insert(1, "apple".to_string(), now_ms()).unwrap();
        assert!(index.has_dirty_buckets());

        let err = index
            .store_dirty_buckets(async |_, _| Err::<bool, BoxError>("write failed".into()))
            .await
            .unwrap_err();

        match err {
            BTreeError::Generic { .. } => {}
            other => panic!("Expected Generic error, got: {other:?}"),
        }

        assert!(index.has_dirty_buckets());
    }

    #[tokio::test]
    async fn test_store_dirty_buckets_keeps_dirty_when_mutated_during_persist() {
        let index = Arc::new(create_test_index());
        index.insert(1, "apple".to_string(), now_ms()).unwrap();
        assert!(index.has_dirty_buckets());

        let index_for_cb = index.clone();
        index
            .store_dirty_buckets(async move |_, _| {
                index_for_cb
                    .insert(2, "banana".to_string(), now_ms())
                    .unwrap();
                Ok(true)
            })
            .await
            .unwrap();

        // A concurrent mutation happened during persistence, so bucket must remain dirty.
        assert!(index.has_dirty_buckets());

        index
            .store_dirty_buckets(async |_, _| Ok(true))
            .await
            .unwrap();
        assert!(!index.has_dirty_buckets());
    }

    #[tokio::test]
    async fn test_store_dirty_buckets_self_heals_drifted_counter() {
        let index = create_test_index();
        index.insert(1, "apple".to_string(), now_ms()).unwrap();
        assert!(index.has_dirty_buckets());

        index
            .store_dirty_buckets(async |_, _| Ok(true))
            .await
            .unwrap();
        assert!(!index.has_dirty_buckets());

        // Simulate counter drift: no bucket is dirty but counter is stale (>0).
        index.dirty_bucket_count.store(1, Ordering::Release);

        index
            .store_dirty_buckets(async |_, _| {
                panic!("no dirty bucket should be written");
            })
            .await
            .unwrap();

        assert!(!index.has_dirty_buckets());
    }

    #[tokio::test]
    async fn test_migrated_source_bucket_is_persisted_to_prevent_resurrection() {
        let config = BTreeConfig {
            bucket_overload_size: 80,
            allow_duplicates: true,
        };
        let index = BTreeIndex::new("resurrection_test".to_string(), Some(config));

        // Step 1: initial data persisted in bucket 0.
        index.insert(1, "apple".to_string(), now_ms()).unwrap();

        let metadata_buf = Arc::new(Mutex::new(Vec::new()));
        let bucket_data = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));

        {
            let mut meta = metadata_buf.lock().await;
            index
                .flush(&mut *meta, now_ms(), {
                    let bucket_data = bucket_data.clone();
                    async move |bucket_id, data| {
                        let mut guard = bucket_data.lock().await;
                        while guard.len() <= bucket_id as usize {
                            guard.push(Vec::new());
                        }
                        guard[bucket_id as usize] = data.to_vec();
                        Ok(true)
                    }
                })
                .await
                .unwrap();
        }

        // Step 2: force migration of "apple" to a new bucket.
        let mut doc_id = 2u64;
        while index.stats().max_bucket_id == 0 && doc_id < 200 {
            index.insert(doc_id, "apple".to_string(), now_ms()).unwrap();
            doc_id += 1;
        }
        assert!(index.stats().max_bucket_id > 0);

        {
            let mut meta = metadata_buf.lock().await;
            index
                .flush(&mut *meta, now_ms(), {
                    let bucket_data = bucket_data.clone();
                    async move |bucket_id, data| {
                        let mut guard = bucket_data.lock().await;
                        while guard.len() <= bucket_id as usize {
                            guard.push(Vec::new());
                        }
                        guard[bucket_id as usize] = data.to_vec();
                        Ok(true)
                    }
                })
                .await
                .unwrap();
        }

        // Step 3: remove all docs for "apple", persist again.
        for id in 1..doc_id {
            index.remove(id, "apple".to_string(), now_ms());
        }
        assert!(
            index
                .query_with(&"apple".to_string(), |ids| Some(ids.clone()))
                .is_none()
        );

        {
            let mut meta = metadata_buf.lock().await;
            index
                .flush(&mut *meta, now_ms(), {
                    let bucket_data = bucket_data.clone();
                    async move |bucket_id, data| {
                        let mut guard = bucket_data.lock().await;
                        while guard.len() <= bucket_id as usize {
                            guard.push(Vec::new());
                        }
                        guard[bucket_id as usize] = data.to_vec();
                        Ok(true)
                    }
                })
                .await
                .unwrap();
        }

        // Step 4: reload and verify no resurrection.
        let meta = { metadata_buf.lock().await.clone() };
        let mut loaded = BTreeIndex::<u64, String>::load_metadata(&meta[..]).unwrap();

        loaded
            .load_buckets({
                let bucket_data = bucket_data.clone();
                async move |bucket_id| {
                    let guard = bucket_data.lock().await;
                    if bucket_id as usize >= guard.len() {
                        return Ok(None);
                    }
                    if guard[bucket_id as usize].is_empty() {
                        return Ok(None);
                    }
                    Ok(Some(guard[bucket_id as usize].clone()))
                }
            })
            .await
            .unwrap();

        assert!(
            loaded
                .query_with(&"apple".to_string(), |ids| Some(ids.clone()))
                .is_none(),
            "apple should not resurrect from stale source bucket"
        );
    }

    #[test]
    fn test_insert_after_load_metadata_without_loading_buckets() {
        let meta = BTreeMetadata {
            name: "loaded_index".to_string(),
            config: BTreeConfig {
                bucket_overload_size: 1024,
                allow_duplicates: true,
            },
            stats: BTreeStats {
                version: 1,
                max_bucket_id: 3,
                ..Default::default()
            },
        };

        let owned = BTreeIndexOwned { metadata: meta };
        let mut buf = Vec::new();
        ciborium::into_writer(&owned, &mut buf).unwrap();

        let index = BTreeIndex::<u64, String>::load_metadata(&buf[..]).unwrap();
        let result = index.insert(1, "apple".to_string(), now_ms());
        assert!(result.is_ok());
    }

    #[test]
    fn test_bucket_overflow() {
        // 创建一个非常小的 bucket 大小的索引，以便测试 bucket 溢出
        let config = BTreeConfig {
            bucket_overload_size: 100, // 非常小的 bucket 大小
            allow_duplicates: true,
        };
        let index = BTreeIndex::new("overflow_test".to_string(), Some(config));

        // 插入足够多的数据以触发 bucket 溢出
        for i in 0..100 {
            let key = format!("key_{i}");
            let _ = index.insert(i, key, now_ms());
        }

        // 验证创建了多个 bucket
        println!("index.stats(): {:?}", index.stats());
        assert!(index.stats().max_bucket_id > 1);

        // 验证所有数据都可以被搜索到
        for i in 0..100 {
            let key = format!("key_{i}");
            let result = index.query_with(&key, |ids| Some(ids.clone()));
            assert!(result.is_some());
            let ids = result.unwrap();
            assert!(ids.contains(&i));
        }
    }

    #[test]
    fn test_insert_array() {
        let index = create_test_index();

        // Test batch insert with empty values
        let result = index.insert_array(1, vec![], now_ms());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);

        // Test batch insert with multiple values
        let values = vec![
            "apple".to_string(),
            "banana".to_string(),
            "cherry".to_string(),
        ];
        let result = index.insert_array(1, values.clone(), now_ms());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 3);

        // Verify all values were inserted
        for value in &values {
            let result = index.query_with(value, |ids| Some(ids.clone()));
            assert!(result.is_some());
            let ids = result.unwrap();
            assert!(ids.contains(&1));
        }

        // Test inserting duplicate document ID for existing values (should be no-op)
        let result = index.insert_array(1, values.clone(), now_ms());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);

        // Test inserting new document ID for existing values
        let result = index.insert_array(2, values.clone(), now_ms());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 3);

        // Verify both document IDs are present
        for value in &values {
            let result = index.query_with(value, |ids| Some(ids.clone()));
            assert!(result.is_some());
            let ids = result.unwrap();
            assert!(ids.contains(&1));
            assert!(ids.contains(&2));
        }

        // Test with non-duplicate configuration
        let config = BTreeConfig {
            bucket_overload_size: 1024,
            allow_duplicates: false,
        };
        let unique_index = BTreeIndex::new("unique_index".to_string(), Some(config));

        // First insert should succeed
        let result = unique_index.insert_array(1, vec!["apple".to_string()], now_ms());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);

        // Second insert with same value but different doc_id should fail
        let result = unique_index.insert_array(2, vec!["apple".to_string()], now_ms());
        assert!(result.is_err());

        // Test bucket overflow handling
        let small_bucket_config = BTreeConfig {
            bucket_overload_size: 50,
            allow_duplicates: true,
        };
        let overflow_index =
            BTreeIndex::new("overflow_test".to_string(), Some(small_bucket_config));

        // Create large values that will cause bucket overflow
        let large_values: Vec<_> = (0..20).map(|i| format!("large_value_{i}")).collect();

        let result = overflow_index.insert_array(1, large_values.clone(), now_ms());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 20);

        let result = overflow_index.insert_array(2, large_values.clone(), now_ms());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 20);

        // Verify bucket overflow occurred and created multiple buckets
        let stats = overflow_index.stats();
        println!("Overflow index stats: {stats:?}");
        assert!(stats.max_bucket_id > 0);

        // Verify all values can still be found
        for value in &large_values {
            let result = overflow_index.query_with(value, |ids| Some(ids.clone()));
            assert!(result.is_some());
            let ids = result.unwrap();
            assert!(ids.contains(&1));
            assert!(ids.contains(&2));
        }
    }

    #[test]
    fn test_remove_array() {
        let index = create_test_index();

        // 首先插入一批数据
        let values = vec![
            "apple".to_string(),
            "banana".to_string(),
            "cherry".to_string(),
            "date".to_string(),
            "eggplant".to_string(),
        ];

        // 插入相同的值，但使用不同的文档ID
        let _ = index.insert_array(1, values.clone(), now_ms());
        let _ = index.insert_array(2, values.clone(), now_ms());
        let _ = index.insert_array(3, vec![values[0].clone(), values[1].clone()], now_ms());

        // 确认初始数据已正确插入
        for value in &values {
            let result = index.query_with(value, |ids| Some(ids.clone()));
            assert!(result.is_some());
            let ids = result.unwrap();

            if value == "apple" || value == "banana" {
                assert_eq!(ids.len(), 3); // 这些值应该有3个文档ID
                assert!(ids.contains(&1) && ids.contains(&2) && ids.contains(&3));
            } else {
                assert_eq!(ids.len(), 2); // 其他值应该只有2个文档ID
                assert!(ids.contains(&1) && ids.contains(&2));
            }
        }

        // 测试1: 批量删除空列表 - 应该无效果
        let removed = index.remove_array(1, vec![], now_ms());
        assert_eq!(removed, 0);
        assert_eq!(index.len(), 5); // 索引中的键数量不变

        // 测试2: 批量删除部分存在的值
        let remove_values = vec![
            "apple".to_string(),
            "nonexistent".to_string(), // 不存在的值
            "banana".to_string(),
        ];
        let removed = index.remove_array(1, remove_values, now_ms());
        assert_eq!(removed, 2); // 只有2个值被实际删除

        // 验证删除结果 - apple和banana仍然存在，但不再包含文档ID 1
        let apple_result = index.query_with(&"apple".to_string(), |ids| Some(ids.clone()));
        assert!(apple_result.is_some());
        let apple_ids = apple_result.unwrap();
        assert_eq!(apple_ids.len(), 2);
        assert!(!apple_ids.contains(&1) && apple_ids.contains(&2) && apple_ids.contains(&3));

        let banana_result = index.query_with(&"banana".to_string(), |ids| Some(ids.clone()));
        assert!(banana_result.is_some());
        let banana_ids = banana_result.unwrap();
        assert_eq!(banana_ids.len(), 2);
        assert!(!banana_ids.contains(&1) && banana_ids.contains(&2) && banana_ids.contains(&3));

        // 测试3: 删除某个值的最后一个文档ID - 该键应该从索引中完全移除
        // 首先删除date和eggplant的文档ID 2，只剩下文档ID 1
        let _ = index.remove_array(
            2,
            vec!["date".to_string(), "eggplant".to_string()],
            now_ms(),
        );

        // 然后删除最后剩余的文档ID
        let remove_values = vec!["date".to_string(), "eggplant".to_string()];
        let removed = index.remove_array(1, remove_values, now_ms());
        assert_eq!(removed, 2);

        // 验证这些键已经完全从索引中移除
        assert!(
            index
                .query_with(&"date".to_string(), |ids| Some(ids.clone()))
                .is_none()
        );
        assert!(
            index
                .query_with(&"eggplant".to_string(), |ids| Some(ids.clone()))
                .is_none()
        );

        // 验证索引中的键数量减少
        assert_eq!(index.len(), 3); // 现在只剩下apple, banana, cherry

        // 测试4: 测试统计信息更新
        let stats = index.stats();
        assert!(stats.delete_count > 0);

        // 测试5: 测试从多个桶中删除（首先创建具有溢出的索引）
        let small_bucket_config = BTreeConfig {
            bucket_overload_size: 50,
            allow_duplicates: true,
        };
        let overflow_index =
            BTreeIndex::new("overflow_test".to_string(), Some(small_bucket_config));

        // 插入足够多的数据以触发桶溢出
        let large_values: Vec<_> = (0..20).map(|i| format!("large_value_{i}")).collect();
        let _ = overflow_index.insert_array(1, large_values.clone(), now_ms());
        let _ = overflow_index.insert_array(2, large_values.clone(), now_ms());

        // 验证桶溢出
        let stats = overflow_index.stats();
        assert!(stats.max_bucket_id > 0);

        // 删除所有文档ID 1的条目
        let removed = overflow_index.remove_array(1, large_values.clone(), now_ms());
        assert_eq!(removed, 20);

        // 验证所有键仍然存在，但只包含文档ID 2
        for value in &large_values {
            let result = overflow_index.query_with(value, |ids| Some(ids.clone()));
            assert!(result.is_some());
            let ids = result.unwrap();
            assert_eq!(ids.len(), 1);
            assert!(ids.contains(&2));
        }

        // 删除所有文档ID 2的条目 - 这应该完全清空索引
        let removed = overflow_index.remove_array(2, large_values.clone(), now_ms());
        assert_eq!(removed, 20);
        assert_eq!(overflow_index.len(), 0);

        // 验证所有键都已被移除
        for value in &large_values {
            let result = overflow_index.query_with(value, |ids| Some(ids.clone()));
            assert!(result.is_none());
        }
    }

    #[test]
    fn test_batch_update() {
        let index = create_test_index();

        // 初始插入 ["a", "b"]
        let _ = index.insert_array(1, vec!["a".to_string(), "b".to_string()], now_ms());

        // 1. 只增加新值
        let (removed, inserted) = index
            .batch_update(
                1,
                vec!["a".to_string(), "b".to_string()],
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
                now_ms(),
            )
            .unwrap();
        assert_eq!(removed, 0);
        assert_eq!(inserted, 1);
        let ids = index
            .query_with(&"c".to_string(), |ids| Some(ids.clone()))
            .unwrap();
        assert!(ids.contains(&1));

        // 2. 只减少旧值
        let (removed, inserted) = index
            .batch_update(
                1,
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
                vec!["a".to_string()],
                now_ms(),
            )
            .unwrap();
        assert_eq!(removed, 2);
        assert_eq!(inserted, 0);
        assert_eq!(
            index
                .query_with(&"a".to_string(), |ids| {
                    println!("ids for 'a': {:?}", ids);
                    Some(ids.clone())
                })
                .unwrap()
                .len(),
            1
        );
        assert!(
            index
                .query_with(&"c".to_string(), |ids| Some(ids.clone()))
                .is_none()
        );

        // 3. 增减混合
        let (removed, inserted) = index
            .batch_update(
                1,
                vec!["a".to_string()],
                vec!["b".to_string(), "c".to_string()],
                now_ms(),
            )
            .unwrap();
        assert_eq!(removed, 1);
        assert_eq!(inserted, 2);
        let ids_b = index
            .query_with(&"b".to_string(), |ids| Some(ids.clone()))
            .unwrap();
        let ids_c = index
            .query_with(&"c".to_string(), |ids| Some(ids.clone()))
            .unwrap();
        assert!(ids_b.contains(&1));
        assert!(ids_c.contains(&1));
        assert!(
            index
                .query_with(&"a".to_string(), |ids| Some(ids.clone()))
                .unwrap_or_default()
                .is_empty()
        );

        // 4. 完全替换
        let (removed, inserted) = index
            .batch_update(
                1,
                vec!["b".to_string(), "c".to_string()],
                vec!["x".to_string(), "y".to_string()],
                now_ms(),
            )
            .unwrap();
        assert_eq!(removed, 2);
        assert_eq!(inserted, 2);
        let ids_x = index
            .query_with(&"x".to_string(), |ids| Some(ids.clone()))
            .unwrap();
        let ids_y = index
            .query_with(&"y".to_string(), |ids| Some(ids.clone()))
            .unwrap();
        assert!(ids_x.contains(&1));
        assert!(ids_y.contains(&1));
        assert!(
            index
                .query_with(&"b".to_string(), |ids| Some(ids.clone()))
                .unwrap_or_default()
                .is_empty()
        );
        assert!(
            index
                .query_with(&"c".to_string(), |ids| Some(ids.clone()))
                .unwrap_or_default()
                .is_empty()
        );

        // 5. 新旧完全相同，无变化
        let (removed, inserted) = index
            .batch_update(
                1,
                vec!["x".to_string(), "y".to_string()],
                vec!["x".to_string(), "y".to_string()],
                now_ms(),
            )
            .unwrap();
        assert_eq!(removed, 0);
        assert_eq!(inserted, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_chaos() {
        let index = Arc::new(BTreeIndex::<u64, String>::new(
            "chaos_index".to_string(),
            Some(BTreeConfig {
                bucket_overload_size: 256,
                allow_duplicates: true,
            }),
        ));

        let n_threads = 10;
        let n_keys_per_thread = 100;
        let barrier = Arc::new(Barrier::new(n_threads));
        let mut handles = Vec::new();

        for t in 0..n_threads {
            let index = index.clone();
            let b = barrier.clone();
            handles.push(tokio::spawn(async move {
                // 等待所有线程准备好
                b.wait().await;

                let base = t * n_keys_per_thread;
                let items: Vec<_> = (0..n_keys_per_thread)
                    .map(|i| format!("key_{}", base + i))
                    .collect();
                // 多次调用 insert_array，模拟混乱
                for j in 0..5 {
                    let _ = index.insert_array((base + j) as u64, items.clone(), now_ms());
                }
            }));
        }

        // 等待所有任务完成
        futures::future::try_join_all(handles).await.unwrap();

        // 检查所有数据都能被检索到
        for t in 0..n_threads {
            let base = t * n_keys_per_thread;
            for i in 0..n_keys_per_thread {
                let key = format!("key_{}", base + i);
                let result = index.query_with(&key, |ids| Some(ids.clone()));
                assert!(result.is_some(), "key {key} not found");

                // 验证该键包含5个文档ID
                let ids = result.unwrap();
                assert_eq!(ids.len(), 5, "key {key} should have 5 doc IDs");

                for j in 0..5 {
                    let doc_id = (base + j) as u64;
                    assert!(ids.contains(&doc_id), "id {doc_id} not found for key {key}");
                }
            }
        }

        // 记录当前索引的大小
        let size_before_remove = index.len();
        assert_eq!(size_before_remove, n_threads * n_keys_per_thread);
        println!("索引大小 (删除前): {size_before_remove}");

        // 第二阶段：多线程同时批量删除数据
        let barrier = Arc::new(Barrier::new(n_threads));
        let mut handles = Vec::new();

        for t in 0..n_threads {
            let index = index.clone();
            let b = barrier.clone();
            handles.push(tokio::spawn(async move {
                // 等待所有线程准备好
                b.wait().await;

                let base = t * n_keys_per_thread;
                let items: Vec<_> = (0..n_keys_per_thread)
                    .map(|i| format!("key_{}", base + i))
                    .collect();

                // 删除前3个文档ID
                for j in 0..3 {
                    let doc_id = (base + j) as u64;
                    let removed = index.remove_array(doc_id, items.clone(), now_ms());
                    assert_eq!(
                        removed, n_keys_per_thread,
                        "应删除 {n_keys_per_thread} 个键，实际删除 {removed}"
                    );
                }
            }));
        }

        // 等待所有删除任务完成
        futures::future::try_join_all(handles).await.unwrap();

        // 验证删除结果：
        // 1. 所有键都应该仍然存在，因为每个键仍有2个文档ID (4和5)
        // 2. 每个键现在应该只包含2个文档ID
        for t in 0..n_threads {
            let base = t * n_keys_per_thread;
            for i in 0..n_keys_per_thread {
                let key = format!("key_{}", base + i);
                let result = index.query_with(&key, |ids| Some(ids.clone()));
                assert!(result.is_some(), "删除后键 {key} 不应该被完全移除");

                let ids = result.unwrap();
                assert_eq!(ids.len(), 2, "删除后键 {key} 应该有2个文档ID");

                // 验证文档ID 0,1,2已被删除，3,4仍然存在
                for j in 0..3 {
                    let doc_id = (base + j) as u64;
                    assert!(!ids.contains(&doc_id), "文档ID {doc_id} 应该已被删除");
                }

                for j in 3..5 {
                    let doc_id = (base + j) as u64;
                    assert!(ids.contains(&doc_id), "文档ID {doc_id} 应该仍然存在");
                }
            }
        }

        // 第三阶段：删除所有剩余的文档ID，清空索引
        let mut handles = Vec::new();

        for t in 0..n_threads {
            let index = index.clone();
            handles.push(tokio::spawn(async move {
                let base = t * n_keys_per_thread;
                let items: Vec<_> = (0..n_keys_per_thread)
                    .map(|i| format!("key_{}", base + i))
                    .collect();

                // 删除剩余的2个文档ID
                for j in 3..5 {
                    let doc_id = (base + j) as u64;
                    index.remove_array(doc_id, items.clone(), now_ms());
                }
            }));
        }

        // 等待所有删除任务完成
        futures::future::try_join_all(handles).await.unwrap();

        // 验证索引现在应该是空的
        assert_eq!(index.len(), 0, "删除所有文档ID后索引应该为空");

        // 尝试查找任意键，应该返回None
        for t in 0..n_threads {
            let base = t * n_keys_per_thread;
            for i in 0..n_keys_per_thread {
                let key = format!("key_{}", base + i);
                let result = index.query_with(&key, |ids| Some(ids.clone()));
                assert!(result.is_none(), "键 {key} 应该已完全从索引中移除");
            }
        }
    }

    #[test]
    fn test_stats() {
        let index = create_test_index();

        // 初始状态
        let stats = index.stats();
        assert_eq!(stats.num_elements, 0);
        assert_eq!(stats.query_count, 0);
        assert_eq!(stats.insert_count, 0);
        assert_eq!(stats.delete_count, 0);

        // 插入一些数据
        let _ = index.insert(1, "apple".to_string(), now_ms());
        let _ = index.insert(2, "banana".to_string(), now_ms());

        // 检查插入后的统计信息
        let stats = index.stats();
        assert_eq!(stats.num_elements, 2);
        assert_eq!(stats.insert_count, 2);

        // 执行一些搜索
        let _ = index.query_with(&"apple".to_string(), |_| Some(()));
        let _: Vec<()> =
            index.range_query_with(RangeQuery::Ge("a".to_string()), |_, _| (true, vec![]));

        // 检查搜索后的统计信息
        let stats = index.stats();
        assert_eq!(stats.query_count, 2);

        // 删除一些数据
        let _ = index.remove(1, "apple".to_string(), now_ms());

        // 检查删除后的统计信息
        let stats = index.stats();
        assert_eq!(stats.num_elements, 1);
        assert_eq!(stats.delete_count, 1);
    }

    #[test]
    fn test_insert_array_uses_correct_bucket_for_existing_postings() {
        // Regression test: insert_array Occupied branch must track size in the
        // posting's actual bucket, not the current max_bucket_id.
        let config = BTreeConfig {
            bucket_overload_size: 80, // small to force migration
            allow_duplicates: true,
        };
        let index = BTreeIndex::new("bucket_track".to_string(), Some(config));

        // Fill bucket 0 until a migration happens (creates bucket 1+).
        let mut doc = 1u64;
        while index.stats().max_bucket_id == 0 && doc < 200 {
            index.insert(doc, "alpha".to_string(), now_ms()).unwrap();
            doc += 1;
        }
        let bucket_after_migration = index.stats().max_bucket_id;
        assert!(bucket_after_migration > 0, "migration should have occurred");

        // "alpha" now lives in the migrated bucket (> 0).
        // Insert a new value "beta" via single insert so it lands in the current max bucket.
        index.insert(1, "beta".to_string(), now_ms()).unwrap();

        // Now use insert_array to add a doc to BOTH "alpha" and "beta".
        // The fix ensures "alpha"'s size_increase is attributed to its actual bucket,
        // not the current max_bucket_id.
        let result =
            index.insert_array(999, vec!["alpha".to_string(), "beta".to_string()], now_ms());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2);

        // Verify both postings contain doc 999.
        let alpha_ids = index
            .query_with(&"alpha".to_string(), |ids| Some(ids.clone()))
            .unwrap();
        assert!(alpha_ids.contains(&999));
        let beta_ids = index
            .query_with(&"beta".to_string(), |ids| Some(ids.clone()))
            .unwrap();
        assert!(beta_ids.contains(&999));
    }

    #[test]
    fn test_insert_array_enforces_unique_in_occupied_branch() {
        // Regression test: insert_array Occupied branch must re-check allow_duplicates
        // atomically while holding the entry lock, matching insert() behaviour.
        let config = BTreeConfig {
            bucket_overload_size: 1024,
            allow_duplicates: false,
        };
        let unique_index = BTreeIndex::new("unique_array".to_string(), Some(config));

        // Insert doc 1 with "apple" via single insert.
        unique_index
            .insert(1, "apple".to_string(), now_ms())
            .unwrap();

        // insert_array with a different doc_id for the same field_value should fail.
        let result = unique_index.insert_array(2, vec!["apple".to_string()], now_ms());
        assert!(result.is_err());
        match result {
            Err(BTreeError::AlreadyExists { .. }) => {}
            other => panic!("Expected AlreadyExists, got: {other:?}"),
        }

        // insert_array with the SAME doc_id should be idempotent (no error, 0 inserted).
        let result = unique_index.insert_array(1, vec!["apple".to_string()], now_ms());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_range_keys_or_returns_sorted_order() {
        let index = create_populated_index();
        // keys: apple < banana < cherry < date < eggplant

        // Or of two non-overlapping ranges in reverse declaration order.
        // Previously would return keys in subquery order (eggplant first)
        // due to FxHashSet dedup; now must return global B-tree order.
        let query = RangeQuery::Or(vec![
            Box::new(RangeQuery::Ge("eggplant".to_string())),
            Box::new(RangeQuery::Le("banana".to_string())),
        ]);

        let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
        assert_eq!(
            results,
            vec!["apple", "banana", "eggplant"],
            "Or query must return keys in global B-tree order"
        );
    }

    #[test]
    fn test_range_keys_or_deduplicates() {
        let index = create_populated_index();
        // Overlapping ranges: banana..=cherry and apple..=cherry
        let query = RangeQuery::Or(vec![
            Box::new(RangeQuery::Between(
                "banana".to_string(),
                "cherry".to_string(),
            )),
            Box::new(RangeQuery::Between(
                "apple".to_string(),
                "cherry".to_string(),
            )),
        ]);

        let results = index.range_query_with(query, |k, _| (true, vec![k.clone()]));
        // Should be deduplicated and sorted
        assert_eq!(results, vec!["apple", "banana", "cherry"]);
    }

    #[tokio::test]
    async fn test_compact_buckets() {
        // Create an index with a tiny bucket limit to force excessive splitting,
        // simulating the fragmentation caused by the old bug.
        let config = BTreeConfig {
            bucket_overload_size: 50,
            allow_duplicates: true,
        };
        let index: BTreeIndex<u64, String> =
            BTreeIndex::new("compact_test".to_string(), Some(config));

        let values: Vec<String> = (0..30).map(|i| format!("value_{i:03}")).collect();
        for (i, v) in values.iter().enumerate() {
            index.insert(i as u64, v.clone(), now_ms()).unwrap();
        }

        let before = index.stats();
        let bucket_count_before = before.max_bucket_id + 1;
        println!("Before compact: {} buckets", bucket_count_before);
        assert!(bucket_count_before > 2, "should have multiple buckets");

        // Serialize fragmented index
        let mut metadata_buf = Vec::new();
        let mut bucket_data: std::collections::HashMap<u32, Vec<u8>> =
            std::collections::HashMap::new();
        index
            .flush(&mut metadata_buf, 1, async |id: u32, data: &[u8]| {
                bucket_data.insert(id, data.to_vec());
                Ok(true)
            })
            .await
            .unwrap();

        // Reload with a large bucket limit
        let mut loaded: BTreeIndex<u64, String> =
            BTreeIndex::load_metadata(&metadata_buf[..]).unwrap();
        loaded.config.bucket_overload_size = 1024 * 512;
        loaded.metadata.write().config.bucket_overload_size = 1024 * 512;
        loaded
            .load_buckets(async |id| Ok(bucket_data.get(&id).cloned()))
            .await
            .unwrap();

        // Capture query results before compaction
        let queries: Vec<&str> = vec!["value_000", "value_010", "value_020"];
        let results_before: Vec<Option<Vec<u64>>> = queries
            .iter()
            .map(|q| loaded.query_with(&q.to_string(), |ids| Some(ids.to_vec())))
            .collect();

        // Compact
        let (old, new) = loaded.compact_buckets();
        println!("Compacted: {} -> {} buckets", old, new);
        assert!(
            new < old,
            "compaction should reduce bucket count significantly"
        );
        assert!(
            new <= 2,
            "with 512K limit all postings should fit in 1-2 buckets, got {}",
            new,
        );

        // Verify query results are unchanged
        for (i, q) in queries.iter().enumerate() {
            let result = loaded.query_with(&q.to_string(), |ids| Some(ids.to_vec()));
            assert_eq!(
                results_before[i], result,
                "query '{}' result changed after compaction",
                q
            );
        }

        // Verify flush + reload works
        let mut metadata_buf2 = Vec::new();
        let mut bucket_data2: std::collections::HashMap<u32, Vec<u8>> =
            std::collections::HashMap::new();
        loaded
            .flush(&mut metadata_buf2, 100, async |id: u32, data: &[u8]| {
                bucket_data2.insert(id, data.to_vec());
                Ok(true)
            })
            .await
            .unwrap();

        let final_loaded: BTreeIndex<u64, String> =
            BTreeIndex::load_all(&metadata_buf2[..], async |id| {
                Ok(bucket_data2.get(&id).cloned())
            })
            .await
            .unwrap();

        assert_eq!(
            final_loaded.stats().num_elements,
            loaded.stats().num_elements
        );
        for q in &queries {
            let orig = loaded.query_with(&q.to_string(), |ids| Some(ids.to_vec()));
            let reloaded = final_loaded.query_with(&q.to_string(), |ids| Some(ids.to_vec()));
            assert_eq!(orig, reloaded, "query '{}' mismatch after reload", q);
        }
    }
}
