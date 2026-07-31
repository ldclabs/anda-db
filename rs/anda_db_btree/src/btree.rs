//! # Anda-DB B-tree Index Library
//!
//! A thread-safe, bucket-based B-tree index for Anda-DB. It maps every indexed
//! field value to the set of primary keys (document ids) that contain it, and
//! supports both exact and range lookups.
//!
//! ## Data Model
//!
//! Conceptually the index is an inverted map:
//!
//! ```text
//! field_value (FV)  →  Posting { bucket_id, version, Vec<primary_key (PK)> }
//! ```
//!
//! Field values are additionally kept in an in-memory [`std::collections::BTreeSet`]
//! to enable sorted range iteration. To scale persistence, postings are grouped
//! into **buckets** of a target size (configurable via
//! [`BTreeConfig::bucket_overload_size`]). Each bucket is serialized/loaded as
//! a single CBOR blob, which makes incremental flush cheap: only buckets whose
//! content changed since the last `flush` are rewritten.
//!
//! ## In-memory State
//!
//! The [`BTreeIndex`] struct keeps four coordinated collections:
//!
//! | Field       | Type                                    | Purpose                                                    |
//! |-------------|-----------------------------------------|------------------------------------------------------------|
//! | `postings`  | `DashMap<FV, PostingValue<PK>>`         | Per-field-value posting list, enables concurrent point ops |
//! | `btree`     | `RwLock<BTreeSet<FV>>`                  | Ordered key set for range / prefix queries                 |
//! | `buckets`   | `DashMap<u32, (size, dirty, FVs, ver)>` | Packing metadata used to schedule incremental flushes      |
//! | `metadata`  | `RwLock<BTreeMetadata>`                 | Name, config and aggregate statistics                      |
//!
//! ## Concurrency Model
//!
//! - All mutating operations take only *fine-grained* locks: DashMap shards for
//!   postings / buckets, and the `btree` `RwLock` only while inserting or
//!   removing a key from the ordered set.
//! - Uniqueness (when `allow_duplicates == false`) is re-checked inside the
//!   `postings` entry lock to avoid TOCTOU races against concurrent writers.
//! - When a posting is removed, the empty-check is re-run inside the entry
//!   lock so a concurrent `insert` cannot have the key silently deleted.
//! - Flush is lock-friendly: bucket contents are snapshotted inside the lock,
//!   then the caller's async writer runs **after** the lock is released.
//!
//! ## Persistence Model
//!
//! The library never writes to disk itself — callers supply async closures to
//! [`BTreeIndex::flush`] / [`BTreeIndex::flush_owned_with`]. Every dirty
//! bucket is written to a **fresh** immutable object keyed by
//! `(bucket_id, generation)`, then the metadata — whose *manifest* maps every
//! live bucket id to its current generation — is committed last. The metadata
//! write is the single atomic commit point:
//!
//! - A crash or error before the commit leaves the new objects as
//!   unreferenced garbage; a loader still sees the previous complete
//!   snapshot.
//! - After the commit, the replaced objects are garbage; they are returned
//!   as [`FlushOutcome::obsolete`] for best-effort deletion.
//!
//! Metadata persisted by pre-manifest releases (no manifest, un-suffixed
//! bucket objects) is still loadable: the loader falls back to scanning
//! bucket ids `0..=max_bucket_id` at generation `0` and keeps the legacy
//! reconciliation (higher bucket id wins for duplicated postings, empty
//! postings are tombstones). The first flush upgrades the durable layout to
//! the manifest format.
//!
//! ## Concurrency contract
//!
//! Concurrent `insert*`/`remove*`/query calls are safe, and so is running
//! [`BTreeIndex::compact_buckets`] alongside them: compaction rebuilds the
//! bucket map non-atomically, so it holds an internal mutation gate
//! exclusively while mutations hold it shared. Coordinating mutations against
//! `flush`, and flushes against each other or against compaction, is the
//! **caller's** responsibility (`anda_db`'s `Collection` holds an exclusive
//! operation gate across every flush). A single writer per durable index is a
//! deployment contract.
//!
//! ## Features
//!
//! - Point lookup ([`BTreeIndex::query_with`]) and range queries
//!   ([`BTreeIndex::range_query_with`] with [`RangeQuery::Eq`] /
//!   [`RangeQuery::Gt`] / [`RangeQuery::Ge`] / [`RangeQuery::Lt`] /
//!   [`RangeQuery::Le`] / [`RangeQuery::Between`] / [`RangeQuery::Include`] /
//!   [`RangeQuery::And`] / [`RangeQuery::Or`] / [`RangeQuery::Not`]).
//! - String prefix queries via [`BTreeIndex::prefix_query_with`].
//! - Batch variants ([`BTreeIndex::insert_array`],
//!   [`BTreeIndex::remove_array`], [`BTreeIndex::batch_update`]) with reduced
//!   lock contention.
//! - [`BTreeIndex::compact_buckets`] re-packs fragmented buckets using
//!   first-fit-decreasing bin packing.

use anda_db_utils::UniqueVec;
use dashmap::DashMap;
use parking_lot::RwLock;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    future::Future,
    hash::Hash,
    io::{Read, Write},
    sync::atomic::{AtomicU32, AtomicU64, Ordering},
};

use crate::{BTreeError, BoxError};

/// Exact CBOR-serialized size of `value`, propagating serialization failures.
///
/// Use this on write paths *before* any state is mutated, so a PK/FV whose
/// `Serialize` impl fails is rejected as [`BTreeError::Serialization`] instead
/// of panicking mid-operation.
fn try_cbor_serialized_size<T: ?Sized + Serialize>(value: &T) -> Result<usize, BoxError> {
    let size = cbor2::serialized_size(value)?;
    usize::try_from(size).map_err(BoxError::from)
}

/// Infallible CBOR size estimate for bookkeeping on already-validated values.
///
/// Serialization is deterministic, and every value reaching this helper was
/// already serialized successfully on the insert path, so a failure here is
/// not expected. If it does happen, fall back to `0`: bucket sizes are
/// advisory packing estimates combined with saturating arithmetic everywhere,
/// so a degraded estimate can only worsen bucket packing, never corrupt data.
fn cbor_serialized_size<T: ?Sized + Serialize>(value: &T) -> usize {
    try_cbor_serialized_size(value).unwrap_or(0)
}

/// Converts a PK/FV into a `serde_json::Value` for error reporting without
/// panicking on values that cannot be represented as JSON (e.g. maps with
/// non-string keys); such values degrade to `Value::Null`.
fn json_value<T: ?Sized + Serialize>(value: &T) -> serde_json::Value {
    serde_json::to_value(value).unwrap_or(serde_json::Value::Null)
}

/// Estimated serialized size of one full `(field_value, posting)` bucket
/// entry.
///
/// Every site that accounts for a whole posting entry (create, migrate,
/// remove-last, compaction) must use this same formula: the field value key
/// contributes to the serialized bucket alongside the posting payload, and
/// mixing key-inclusive with key-exclusive estimates would let bucket sizes
/// drift from reality (long string keys made buckets overshoot
/// `bucket_overload_size` before this was unified).
fn posting_entry_size<FV, P>(field_value: &FV, posting: &P) -> usize
where
    FV: Serialize,
    P: Serialize,
{
    cbor_serialized_size(&(field_value, posting)) + 2
}

/// Fallible variant of [`posting_entry_size`] for paths that can still reject
/// the value before mutating any state.
fn try_posting_entry_size<FV, P>(field_value: &FV, posting: &P) -> Result<usize, BoxError>
where
    FV: Serialize,
    P: Serialize,
{
    Ok(try_cbor_serialized_size(&(field_value, posting))? + 2)
}

fn previous_posting_size_after_append<PK, FV>(
    field_value: &FV,
    bucket_id: u32,
    version_after_append: u64,
    doc_ids_after_append: &UniqueVec<PK>,
) -> usize
where
    PK: Eq + Hash + Clone + Serialize,
    FV: Serialize,
{
    // Drop the most recently appended doc_id to approximate the pre-append
    // posting. Between the append and this call another thread may have
    // appended its own doc_id to the same posting, so the popped element is
    // not necessarily the one appended by the current caller; the resulting
    // size is still a valid one-element-smaller estimate, and any residual
    // drift in bucket accounting is bounded by the saturating arithmetic at
    // the call sites. Do not assert on the popped element here.
    let mut previous_doc_ids = doc_ids_after_append.to_vec();
    previous_doc_ids.pop();

    let previous = (
        bucket_id,
        version_after_append.saturating_sub(1),
        UniqueVec::from(previous_doc_ids),
    );
    posting_entry_size(field_value, &previous)
}

/// Thread-safe, bucket-based B-tree index with range query support.
///
/// `PK` is the primary key type (typically the document id) and `FV` is the
/// indexed field value type. The index maintains an inverted mapping
/// `FV → Vec<PK>` together with an ordered `BTreeSet<FV>` to serve range and
/// prefix queries efficiently.
///
/// See the [crate-level documentation](crate) for architecture, concurrency
/// model, and persistence details.
///
/// # Type parameters
///
/// - `PK`: primary key. Must be `Ord + Eq + Hash + Clone + Serialize +
///   DeserializeOwned + Debug`.
/// - `FV`: field value. Same bounds as `PK`.
///
/// # Invariants
///
/// 1. Every key in `btree` has a corresponding entry in `postings`, and vice
///    versa. Empty postings are removed together with their btree key.
/// 2. Each posting is tracked by exactly one bucket. Migrations mark both the
///    source and destination bucket dirty.
/// 3. `max_bucket_id` is monotonic. It may exceed the actual largest populated
///    bucket id transiently during concurrent inserts; `load_buckets` tolerates
///    sparse bucket ids up to `max_bucket_id`.
pub struct BTreeIndex<PK, FV>
where
    PK: Ord + Debug + Clone + Serialize + DeserializeOwned,
    FV: Eq + Ord + Hash + Debug + Clone + Serialize + DeserializeOwned,
{
    /// Index name
    name: String,

    /// Index configuration
    config: BTreeConfig,

    /// Packing metadata for each on-disk bucket.
    ///
    /// `bucket_id → (bucket_size, is_dirty, field_values, dirty_version)`:
    ///
    /// - `bucket_size`  — estimated CBOR size (bytes) of the bucket payload.
    ///   Used to decide when to spill into a fresh bucket.
    /// - `is_dirty`     — `true` if there are unpersisted changes.
    /// - `field_values` — the set of `FV` whose posting lives in this bucket.
    /// - `dirty_version`— monotonic counter, bumped on every mutation. It is
    ///   sampled before an async write and re-checked after it, so that a
    ///   concurrent mutation during persistence keeps the bucket dirty.
    buckets: DashMap<u32, (usize, bool, UniqueVec<FV>, u64)>,

    /// Inverted index: field value → posting list. See [`PostingValue`].
    postings: DashMap<FV, PostingValue<PK>>,

    /// Ordered key set backing all range/prefix queries.
    btree: RwLock<BTreeSet<FV>>,

    /// Index metadata (name, config, stats).
    metadata: RwLock<BTreeMetadata>,

    /// Highest bucket id currently in use (monotonic).
    max_bucket_id: AtomicU32,

    /// Cumulative number of query operations performed.
    query_count: AtomicU64,

    /// Version of the last successfully persisted metadata.
    /// Prevents re-serializing identical metadata.
    last_saved_version: AtomicU64,

    /// Held *shared* by every synchronous mutation and *exclusively* by
    /// [`BTreeIndex::compact_buckets`], which rebuilds the whole bucket map
    /// non-atomically: a posting created after compaction snapshotted
    /// `postings` would otherwise be re-binned into nothing and silently lost
    /// on the next flush. Mutations still run concurrently with each other —
    /// they only take the shared side — and this is the first lock a mutation
    /// acquires, so it never nests inside a DashMap shard guard.
    mutation_gate: RwLock<()>,
}

/// Identifies one durable bucket object.
///
/// A bucket's content is stored in immutable, generation-suffixed objects.
/// `generation == 0` refers to the legacy (pre-manifest) object that was
/// keyed by bucket id alone; generations `>= 1` are produced by the manifest
/// protocol and each flush writes replaced buckets to a **new** generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BucketObject {
    /// Stable bucket id.
    pub bucket_id: u32,
    /// Object generation; `0` denotes the legacy un-suffixed object.
    pub generation: u64,
}

/// Result of a [`BTreeIndex::flush`] / [`BTreeIndex::flush_owned_with`] call.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlushOutcome {
    /// Whether anything (metadata and possibly buckets) was persisted.
    pub saved: bool,
    /// Bucket objects that the newly committed manifest no longer references.
    /// The caller should delete them best-effort; a failed deletion only
    /// leaks storage space and never affects future loads.
    pub obsolete: Vec<BucketObject>,
}

/// Posting list for a single field value: `(bucket_id, update_version, doc_ids)`.
///
/// - `bucket_id`      — the bucket currently storing this posting.
/// - `update_version` — monotonic counter bumped on every doc-id add/remove.
/// - `doc_ids`        — unique list of primary keys. Appends preserve order,
///   but removals use swap-remove, so the remaining ids may be reordered
///   after any deletion. Do not rely on insertion order.
type PostingValue<PK> = (u32, u64, UniqueVec<PK>);

/// Configuration parameters for the B-tree index
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BTreeConfig {
    /// Maximum size of a bucket before creating a new one
    /// When a bucket's stored data exceeds this size,
    /// a new bucket should be created for new data
    ///
    /// Values below [`BTreeConfig::MIN_BUCKET_OVERLOAD_SIZE`] are clamped up
    /// by [`BTreeIndex::new`] / [`BTreeIndex::load_metadata`].
    pub bucket_overload_size: usize,

    /// Whether to allow duplicate primary keys in an indexed field value
    /// If false, attempting to insert a duplicate key will result in an error
    pub allow_duplicates: bool,
}

impl BTreeConfig {
    /// Minimum accepted `bucket_overload_size`.
    ///
    /// A zero (or tiny) value would make almost every new field value spill
    /// into its own bucket, exploding the bucket/file count without any
    /// correctness benefit, so constructors clamp smaller values up to this
    /// floor. The floor is deliberately low to keep small bucket sizes usable
    /// for testing.
    pub const MIN_BUCKET_OVERLOAD_SIZE: usize = 64;

    fn clamp(&mut self) {
        self.bucket_overload_size = self
            .bucket_overload_size
            .max(Self::MIN_BUCKET_OVERLOAD_SIZE);
    }
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BTreeMetadata {
    /// Index name
    pub name: String,

    /// Index configuration
    pub config: BTreeConfig,

    /// Index statistics
    pub stats: BTreeStats,

    /// Bucket manifest: `bucket_id -> generation` of the durable object that
    /// currently holds the bucket's content (`0` = legacy un-suffixed object).
    ///
    /// The manifest is the loader's single source of truth: a posting exists
    /// only in the bucket objects it references. Metadata persisted before
    /// the manifest protocol deserializes with an empty map, which selects
    /// the legacy bucket-id-scan load path.
    #[serde(default)]
    pub buckets: BTreeMap<u32, u64>,
}

/// Index statistics for monitoring and diagnostics
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

/// One immutable bucket payload captured for a coordinated flush.
struct BucketPersistenceSnapshot {
    bucket_id: u32,
    dirty_version: u64,
    data: Vec<u8>,
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

// Reference structure for serializing bucket (test-only: the flush path
// serializes owned snapshots via `BucketOwned`).
#[cfg(test)]
#[derive(Serialize)]
struct BucketRef<'a, PK, FV>
where
    PK: Eq + Ord + Hash + Clone + Serialize,
    FV: Eq + Ord + Hash + Clone + Serialize,
{
    #[serde(rename = "p")]
    postings: &'a FxHashMap<&'a FV, dashmap::mapref::one::Ref<'a, FV, PostingValue<PK>>>,
}

/// Range query specification for flexible querying.
///
/// Queries compose: logical combinators (`And`, `Or`, `Not`) may contain any
/// other variants, enabling arbitrary boolean predicates over field values.
///
/// Ordering semantics:
///
/// - `Gt`, `Ge`, `Between`, `Include`, `And`, `Or`, `Not` emit results in
///   ascending key order.
/// - `Lt` and `Le` iterate in *descending* key order internally so that
///   early-termination (via the callback's `continue` flag) keeps the
///   *closest-to-upper-bound* keys; the final result is re-ordered ascending,
///   with per-key output preserved.
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

    /// Between two keys (inclusive on both ends).
    ///
    /// Empty result when `start > end`.
    Between(FV, FV),

    /// Include specific keys (duplicates are deduplicated, results sorted).
    Include(Vec<FV>),

    /// A logical OR query that returns the union of all subquery results
    /// (deduplicated, in ascending key order).
    Or(Vec<Box<RangeQuery<FV>>>),

    /// A logical AND query that returns the intersection of all subquery
    /// results.
    And(Vec<Box<RangeQuery<FV>>>),

    /// A logical NOT query that returns every indexed key not matched by the
    /// inner subquery.
    Not(Box<RangeQuery<FV>>),
}

impl<FV> RangeQuery<FV> {
    /// Maximum supported nesting depth for composed queries.
    ///
    /// Query evaluation is recursive, so an unbounded depth would let a
    /// deeply nested query built from untrusted input (e.g. a parsed filter
    /// expression) overflow the stack. Queries nested deeper than this are
    /// rejected: [`Self::try_convert_from`] returns an error and
    /// [`BTreeIndex::range_query_with`] returns an empty result.
    pub const MAX_DEPTH: usize = 64;

    /// Returns the nesting depth of this query (a leaf query has depth 1).
    ///
    /// Computed iteratively so that arbitrarily deep queries can be measured
    /// without recursing.
    pub fn depth(&self) -> usize {
        let mut max_depth = 0;
        let mut stack: Vec<(&RangeQuery<FV>, usize)> = vec![(self, 1)];
        while let Some((query, depth)) = stack.pop() {
            max_depth = max_depth.max(depth);
            match query {
                RangeQuery::And(queries) | RangeQuery::Or(queries) => {
                    stack.extend(queries.iter().map(|q| (q.as_ref(), depth + 1)));
                }
                RangeQuery::Not(query) => stack.push((query.as_ref(), depth + 1)),
                _ => {}
            }
        }
        max_depth
    }

    /// Translates a `RangeQuery<FV1>` into a `RangeQuery<FV>` by applying a
    /// `TryFrom<FV1>` conversion to every key.
    ///
    /// Useful for adapting user-facing typed queries (e.g. JSON values) to the
    /// storage-level field value type without rewriting query shape.
    ///
    /// # Errors
    ///
    /// Returns an error when any key conversion fails, or when the query is
    /// nested deeper than [`Self::MAX_DEPTH`].
    pub fn try_convert_from<FV1>(value: RangeQuery<FV1>) -> Result<Self, BoxError>
    where
        FV: Ord,
        FV: TryFrom<FV1, Error = BoxError>,
    {
        // Depth is checked once at the outermost call; the recursion below
        // then stays within a bounded stack budget. `depth()` is iterative,
        // and recursive calls go through `try_convert_from_inner`.
        if value.depth() > Self::MAX_DEPTH {
            return Err(format!(
                "range query nesting depth exceeds the maximum of {}",
                Self::MAX_DEPTH
            )
            .into());
        }
        Self::try_convert_from_inner(value)
    }

    fn try_convert_from_inner<FV1>(value: RangeQuery<FV1>) -> Result<Self, BoxError>
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
                    .map(|query| RangeQuery::try_convert_from_inner(*query))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RangeQuery::And(
                    converted_queries.into_iter().map(Box::new).collect(),
                ))
            }
            RangeQuery::Or(queries) => {
                let converted_queries = queries
                    .into_iter()
                    .map(|query| RangeQuery::try_convert_from_inner(*query))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RangeQuery::Or(
                    converted_queries.into_iter().map(Box::new).collect(),
                ))
            }
            RangeQuery::Not(query) => {
                let converted_query = RangeQuery::try_convert_from_inner(*query)?;
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
    /// Marks a bucket as dirty and bumps its `dirty_version`.
    ///
    /// The `dirty_version` counter is sampled while a flush serializes the
    /// bucket and re-checked after the commit; if it has changed, the bucket
    /// remains dirty for the next flush.
    fn mark_bucket_dirty(&self, bucket: &mut (usize, bool, UniqueVec<FV>, u64)) {
        bucket.3 = bucket.3.wrapping_add(1);
        bucket.1 = true;
    }

    fn serialize_bucket_snapshot(
        &self,
        bucket_id: u32,
    ) -> Result<Option<BucketPersistenceSnapshot>, BTreeError> {
        let Some(bucket) = self.buckets.get(&bucket_id) else {
            return Ok(None);
        };
        if !bucket.1 {
            return Ok(None);
        }

        let dirty_version = bucket.3;
        // Clone an owned payload. The coordinated flush may await several
        // object-store writes after this point, so neither DashMap guards nor
        // a later mutation may influence the bytes being committed.
        // A migrated posting can still be listed by its source bucket; writing
        // it here too would let the stale copy win on load (the loader forces
        // `posting.0 = i` walking buckets in ascending id order). Persist only
        // the postings this bucket actually owns.
        let postings: FxHashMap<_, _> = bucket
            .2
            .iter()
            .filter_map(|field_value| {
                self.postings
                    .get(field_value)
                    .filter(|posting| posting.0 == bucket_id && !posting.2.is_empty())
                    .map(|posting| (field_value.clone(), posting.clone()))
            })
            .collect();
        drop(bucket);

        let mut data = Vec::with_capacity(4096);
        cbor2::to_writer(&BucketOwned { postings }, &mut data).map_err(|err| {
            BTreeError::Serialization {
                name: self.name.clone(),
                source: err.into(),
            }
        })?;

        Ok(Some(BucketPersistenceSnapshot {
            bucket_id,
            dirty_version,
            data,
        }))
    }

    /// Serializes every dirty bucket, sorted by bucket id for deterministic
    /// write order.
    fn serialize_dirty_buckets(&self) -> Result<Vec<BucketPersistenceSnapshot>, BTreeError> {
        let mut dirty_bucket_ids: Vec<u32> = self
            .buckets
            .iter()
            .filter_map(|bucket| bucket.1.then_some(*bucket.key()))
            .collect();
        dirty_bucket_ids.sort_unstable();

        let mut buckets = Vec::with_capacity(dirty_bucket_ids.len());
        for bucket_id in dirty_bucket_ids {
            if let Some(snapshot) = self.serialize_bucket_snapshot(bucket_id)? {
                buckets.push(snapshot);
            }
        }
        Ok(buckets)
    }

    fn mark_bucket_snapshot_saved(&self, bucket_id: u32, dirty_version: u64) {
        if let Some(mut bucket) = self.buckets.get_mut(&bucket_id)
            && bucket.1
            && bucket.3 == dirty_version
        {
            bucket.1 = false;
        }
    }

    fn remove_btree_key_if_posting_absent(&self, field_value: &FV) {
        let mut btree = self.btree.write();
        if !self.postings.contains_key(field_value) {
            btree.remove(field_value);
        }
    }

    fn range_query_seed_rank(query: &RangeQuery<FV>) -> u8 {
        match query {
            RangeQuery::Eq(_) => 0,
            RangeQuery::Between(start_key, end_key) if start_key > end_key => 0,
            RangeQuery::Include(keys) if keys.is_empty() => 0,
            RangeQuery::Include(_) => 1,
            RangeQuery::Between(_, _) => 2,
            RangeQuery::Gt(_) | RangeQuery::Ge(_) | RangeQuery::Lt(_) | RangeQuery::Le(_) => 3,
            RangeQuery::And(queries) => queries
                .iter()
                .map(|query| Self::range_query_seed_rank(query))
                .min()
                .unwrap_or(0),
            RangeQuery::Or(_) => 4,
            RangeQuery::Not(_) => 5,
        }
    }

    fn range_key_matches_query(key: &FV, query: &RangeQuery<FV>) -> bool {
        match query {
            RangeQuery::Eq(value) => key == value,
            RangeQuery::Gt(start_key) => key > start_key,
            RangeQuery::Ge(start_key) => key >= start_key,
            RangeQuery::Lt(end_key) => key < end_key,
            RangeQuery::Le(end_key) => key <= end_key,
            RangeQuery::Between(start_key, end_key) => {
                start_key <= end_key && key >= start_key && key <= end_key
            }
            RangeQuery::Include(keys) => keys.iter().any(|value| value == key),
            RangeQuery::Or(queries) => queries
                .iter()
                .any(|query| Self::range_key_matches_query(key, query)),
            RangeQuery::And(queries) => {
                !queries.is_empty()
                    && queries
                        .iter()
                        .all(|query| Self::range_key_matches_query(key, query))
            }
            RangeQuery::Not(query) => !Self::range_key_matches_query(key, query),
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
        let mut config = config.unwrap_or_default();
        config.clamp();
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
                buckets: BTreeMap::new(),
            }),
            max_bucket_id: AtomicU32::new(0),
            query_count: AtomicU64::new(0),
            last_saved_version: AtomicU64::new(0),
            mutation_gate: RwLock::new(()),
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
        F: AsyncFnMut(BucketObject) -> Result<Option<Vec<u8>>, BoxError>,
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
        let mut index: BTreeIndexOwned =
            cbor2::from_reader(r).map_err(|err| BTreeError::Serialization {
                name: "unknown".to_string(),
                source: err.into(),
            })?;
        // Same floor as `new()`: persisted metadata may carry a degenerate
        // (e.g. zero) bucket_overload_size from an older or corrupted file.
        index.metadata.config.clamp();

        // Extract configuration values
        let max_bucket_id = AtomicU32::new(index.metadata.stats.max_bucket_id);
        let query_count = AtomicU64::new(index.metadata.stats.query_count);
        let last_saved_version = AtomicU64::new(index.metadata.stats.version);

        // `num_elements` comes from untrusted storage; cap the pre-allocation
        // so a corrupted value cannot trigger a huge allocation (or a capacity
        // overflow panic). The map grows on demand past this hint anyway.
        const MAX_PREALLOCATED_CAPACITY: u64 = 1 << 16;
        let capacity = index
            .metadata
            .stats
            .num_elements
            .min(MAX_PREALLOCATED_CAPACITY) as usize;

        Ok(BTreeIndex {
            name: index.metadata.name.clone(),
            config: index.metadata.config.clone(),
            postings: DashMap::with_capacity(capacity),
            buckets: DashMap::from_iter(vec![(0, (0, false, UniqueVec::default(), 0))]),
            btree: RwLock::new(BTreeSet::new()),
            metadata: RwLock::new(index.metadata),
            query_count,
            max_bucket_id,
            last_saved_version,
            mutation_gate: RwLock::new(()),
        })
    }

    /// Loads data from buckets using the provided async function
    /// This function should be called during database startup to load all bucket data
    /// and form a complete index
    ///
    /// When the loaded metadata carries a bucket manifest, `f` is invoked
    /// once per referenced [`BucketObject`]; without a manifest (data
    /// persisted by a pre-manifest release) every bucket id in
    /// `0..=max_bucket_id` is probed at generation `0` (the legacy object).
    /// Returning `Ok(None)` leaves that bucket empty, which allows read-only
    /// partial loads; a partially loaded index must not be flushed, since a
    /// flush persists exactly the loaded content.
    ///
    /// # Ordering
    ///
    /// This is a bootstrap step: it must complete before the index takes any
    /// mutation. Loaded postings are authoritative and *replace* the in-memory
    /// entry for the same field value — that replacement is what implements
    /// the higher-bucket-wins repair of stale duplicates, so it cannot merge
    /// instead. A pair inserted between [`load_metadata`](Self::load_metadata)
    /// and this call would therefore be discarded without a trace. Prefer
    /// [`load_all`](Self::load_all), which sequences both steps.
    ///
    /// # Arguments
    ///
    /// * `f` - Async function that reads posting data for a bucket object.
    ///   If the object does not exist, it should return `Ok(None)`.
    ///
    /// # Returns
    ///
    /// * `Result<(), BTreeError>` - Success or error
    pub async fn load_buckets<F>(&mut self, mut f: F) -> Result<(), BTreeError>
    where
        F: AsyncFnMut(BucketObject) -> Result<Option<Vec<u8>>, BoxError>,
    {
        let manifest = { self.metadata.read().buckets.clone() };
        let legacy = manifest.is_empty();
        let objects: Vec<BucketObject> = if legacy {
            (0..=self.max_bucket_id.load(Ordering::Relaxed))
                .map(|bucket_id| BucketObject {
                    bucket_id,
                    generation: 0,
                })
                .collect()
        } else {
            manifest
                .iter()
                .map(|(bucket_id, generation)| BucketObject {
                    bucket_id: *bucket_id,
                    generation: *generation,
                })
                .collect()
        };

        let mut loaded_bucket_ids: Vec<u32> = Vec::new();
        for object in objects {
            let i = object.bucket_id;
            let data = f(object).await.map_err(|err| BTreeError::Generic {
                name: self.name.clone(),
                source: err,
            })?;
            if data.is_none() && !legacy {
                // The manifest references this bucket but the caller skipped
                // (or lost) its object. Keep an empty placeholder so the next
                // flush carries the manifest entry forward instead of
                // silently dropping the durable object.
                self.buckets
                    .entry(i)
                    .or_insert_with(|| (0, false, UniqueVec::default(), 0));
                continue;
            }
            if let Some(data) = data {
                loaded_bucket_ids.push(i);
                let bucket: BucketOwned<PK, FV> =
                    cbor2::from_reader(&data[..]).map_err(|err| BTreeError::Serialization {
                        name: self.name.clone(),
                        source: err.into(),
                    })?;
                let mut bks = UniqueVec::with_capacity(bucket.postings.len());
                let mut loaded_keys = Vec::with_capacity(bucket.postings.len());
                // Set when this bucket file contains stale entries (an empty
                // posting persisted by a crash window); the bucket is loaded
                // as dirty so the next flush rewrites the file without them.
                let mut needs_repair = false;

                // Higher bucket ids are the newer state when a migrated posting
                // appears in more than one bucket. Reconcile the old in-memory
                // bucket ownership and mark it dirty so the stale lower bucket
                // is repaired on the next flush.
                for (field_value, mut posting) in bucket.postings {
                    // An empty posting can only reach disk when a flush
                    // sampled the bucket between "posting emptied by remove()"
                    // and "posting entry removed", and a crash followed before
                    // the next flush repaired the file. Registering it would
                    // create a "ghost" key visible to `keys()`, range queries
                    // and `len()` with no backing documents. Treat it as a
                    // tombstone instead: skip it, drop any stale copy already
                    // loaded from an older bucket, and mark the affected
                    // buckets dirty to self-heal on the next flush.
                    if posting.2.is_empty() {
                        needs_repair = true;
                        if let Some((_, previous)) = self.postings.remove(&field_value) {
                            let previous_bucket_id = previous.0;
                            if previous_bucket_id != i
                                && let Some(mut previous_bucket) =
                                    self.buckets.get_mut(&previous_bucket_id)
                                && previous_bucket
                                    .2
                                    .swap_remove_if(|key| key == &field_value)
                                    .is_some()
                            {
                                let previous_size = posting_entry_size(&field_value, &previous);
                                previous_bucket.0 = previous_bucket.0.saturating_sub(previous_size);
                                self.mark_bucket_dirty(&mut previous_bucket);
                            }
                            self.btree.write().remove(&field_value);
                        }
                        continue;
                    }

                    posting.0 = i;
                    if let Some(previous) = self.postings.insert(field_value.clone(), posting) {
                        let previous_bucket_id = previous.0;
                        if previous_bucket_id != i
                            && let Some(mut previous_bucket) =
                                self.buckets.get_mut(&previous_bucket_id)
                            && previous_bucket
                                .2
                                .swap_remove_if(|key| key == &field_value)
                                .is_some()
                        {
                            let previous_size = posting_entry_size(&field_value, &previous);
                            previous_bucket.0 = previous_bucket.0.saturating_sub(previous_size);
                            self.mark_bucket_dirty(&mut previous_bucket);
                        }
                    }

                    bks.push(field_value.clone());
                    loaded_keys.push(field_value);
                }

                self.btree.write().extend(loaded_keys);
                // `data.len()` (the on-disk payload length) seeds the bucket
                // size here, while runtime mutations apply estimated deltas
                // (`posting_entry_size` + fudge). The two baselines can drift
                // slightly; the size is only used for packing decisions and
                // is always combined with saturating arithmetic.
                self.buckets
                    .insert(i, (data.len(), needs_repair, bks, u64::from(needs_repair)));
            }
        }

        if legacy && !loaded_bucket_ids.is_empty() {
            // Record in memory where each loaded bucket's durable object
            // lives (generation 0 = legacy object). The next flush commits a
            // real manifest whose clean buckets keep referencing these legacy
            // objects until they are rewritten.
            self.update_metadata(|m| {
                m.buckets = loaded_bucket_ids.iter().map(|id| (*id, 0)).collect();
            });
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
        // Shared with other mutations, exclusive against `compact_buckets`.
        let _mutation_guard = self.mutation_gate.read();

        // Validate `doc_id` serialization up-front, before any state is
        // mutated, so a failing `Serialize` impl surfaces as an error instead
        // of a panic (and never leaves a half-applied insert behind).
        let doc_id_size =
            try_cbor_serialized_size(&doc_id).map_err(|err| BTreeError::Serialization {
                name: self.name.clone(),
                source: err,
            })? + 2;

        let bucket = self.max_bucket_id.load(Ordering::Relaxed);

        // Ensure the current bucket exists.
        // This avoids races where max_bucket_id advances before the bucket entry is created,
        // and also covers an index restored by load_metadata() alone (its
        // `max_bucket_id` watermark is ahead of the buckets it materialized).
        // Inserting between load_metadata() and load_buckets() is NOT supported:
        // the load overwrites postings by design (see `load_buckets`).
        // contains_key (shard read lock) first: every insert hits the same current
        // bucket id, so taking the shard write lock via entry() each time would
        // serialize concurrent inserts on this hot path.
        if !self.buckets.contains_key(&bucket) {
            self.buckets
                .entry(bucket)
                .or_insert_with(|| (0, false, UniqueVec::default(), 0));
        }

        // Calculate the size increase for this insertion
        let mut is_new = false;
        let mut size_increase = 0;
        let mut appended_existing_posting = false;
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
                        id: json_value(&doc_id),
                        value: json_value(&field_value),
                    });
                }

                // Add doc_id if it doesn't exist. Measure only the appended
                // doc_id, never the whole posting: doing the latter on every
                // insert would make growing a single posting O(n^2).
                if posting.2.push(doc_id.clone()) {
                    size_increase = doc_id_size;
                    appended_existing_posting = true;
                    posting.1 += 1; // increment version
                }
            }
            dashmap::Entry::Vacant(entry) => {
                // Create a new posting for this field value
                let posting = (bucket, 1, vec![doc_id.clone()].into());
                // Reject an unserializable field value before inserting it:
                // nothing has been mutated yet, so returning here is clean.
                size_increase = try_posting_entry_size(&field_value, &posting).map_err(|err| {
                    BTreeError::Serialization {
                        name: self.name.clone(),
                        source: err,
                    }
                })?;
                entry.insert(posting);
                is_new = true;
            }
        };

        if is_new {
            // Add the field value to the B-tree for range queries.
            //
            // Re-check the posting inside the btree lock: a concurrent `remove`
            // may have already deleted the just-created posting, and its btree
            // cleanup (which also takes the btree lock, see
            // `remove_btree_key_if_posting_absent`) found nothing to remove.
            // Inserting unconditionally would leave a phantom key in the btree
            // with no backing posting.
            let mut btree = self.btree.write();
            if self.postings.contains_key(&field_value) {
                btree.insert(field_value.clone());
            }
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
                // If the current bucket is full, create a new one.
                //
                // Known benign drift: this migration writes `posting.0 =
                // new_bucket` before the destination bucket entry is created
                // below. A concurrent `remove` that empties the posting in
                // that window skips its bucket accounting (the destination
                // entry does not exist yet), so the in-memory bucket size may
                // over-count and `bucket.2` may keep the field value until the
                // next compaction. This only degrades packing decisions —
                // flush filters postings through the live `postings` map, so
                // the persisted state stays correct — and all size arithmetic
                // saturates.
                let mut source_size_decrease = 0;
                new_bucket = self.max_bucket_id.fetch_add(1, Ordering::Relaxed) + 1;
                {
                    if let Some(mut posting) = self.postings.get_mut(&field_value) {
                        // Update the posting's bucket ID
                        // The source bucket tracked this posting WITHOUT the
                        // just-appended doc_id (the migration path never added
                        // `size_increase` to it), so reclaim the exact
                        // pre-insert size. Compute this only on migration: CBOR
                        // sequence length and integer width can grow at
                        // boundaries such as 23 -> 24, so subtracting only the
                        // appended doc_id from the post-insert size is not exact.
                        source_size_decrease = if appended_existing_posting {
                            previous_posting_size_after_append(
                                &field_value,
                                target_bucket,
                                posting.1,
                                &posting.2,
                            )
                        } else {
                            0
                        };

                        posting.0 = new_bucket;
                        let migrated_posting_size = posting_entry_size(&field_value, &*posting);
                        size_increase = migrated_posting_size;
                    } else {
                        size_increase = 0;
                        new_bucket = 0;
                    }
                }
                // Remove the current field value from the current bucket
                // The freed space can still accommodate small growth in other field values
                if b.2.swap_remove_if(|k| &field_value == k).is_some() {
                    b.0 = b.0.saturating_sub(source_size_decrease);
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

    /// Removes a document_id-field_value pair from the index
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
        // Shared with other mutations, exclusive against `compact_buckets`.
        let _mutation_guard = self.mutation_gate.read();

        let mut removed = false;
        let mut doc_size_decrease = 0;
        let mut full_size_decrease = 0;
        let mut posting_empty = false;
        let mut bucket_id = 0;

        {
            if let Some(mut posting) = self.postings.get_mut(&field_value) {
                bucket_id = posting.0;
                // The whole-posting size is only consumed when this removal
                // empties the posting (it then becomes the bucket's full size
                // decrease). Measure it only in that case — when the posting
                // holds a single doc_id — to avoid an O(n) CBOR pass on every
                // removal from a large posting.
                let prev_posting_size = if posting.2.len() == 1 {
                    posting_entry_size(&field_value, &*posting)
                } else {
                    0
                };
                if posting.2.swap_remove_if(|id| id == &doc_id).is_some() {
                    removed = true;
                    posting.1 += 1; // increment version
                    posting_empty = posting.2.is_empty();
                    doc_size_decrease = cbor_serialized_size(&doc_id) + 2;
                    full_size_decrease = prev_posting_size;
                }
            }
        }

        if removed {
            let mut entry_removed = false;
            if posting_empty {
                // Atomically check-and-remove: only remove if the posting is still empty.
                // Between dropping the `get_mut` above and here, a concurrent `insert`
                // could have added a new doc_id, making the posting non-empty again.
                entry_removed = self
                    .postings
                    .remove_if(&field_value, |_, posting| posting.2.is_empty())
                    .is_some();

                if entry_removed {
                    self.remove_btree_key_if_posting_absent(&field_value);
                }
            }

            let size_decrease = if entry_removed {
                full_size_decrease
            } else {
                doc_size_decrease
            };

            // Update the bucket state
            if let Some(mut b) = self.buckets.get_mut(&bucket_id) {
                b.0 = b.0.saturating_sub(size_decrease);
                self.mark_bucket_dirty(&mut b);

                if entry_removed {
                    // remove FV from the bucket
                    let remove_from_bucket = match self.postings.get(&field_value) {
                        Some(posting) => posting.0 != bucket_id,
                        None => true,
                    };
                    if remove_from_bucket {
                        b.2.swap_remove_if(|k| &field_value == k);
                    }
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

    /// Batch-inserts `(doc_id, field_value)` pairs sharing the same `doc_id`.
    ///
    /// This is materially more efficient than calling [`Self::insert`] in a
    /// loop: bucket size tracking and B-tree key insertion are amortised, and
    /// the posting lock is acquired once per field value.
    ///
    /// The operation proceeds in three phases:
    ///
    /// 1. **Posting update** — for each field value, either append `doc_id` to
    ///    the existing posting or create a fresh one. Per-bucket size deltas
    ///    are accumulated.
    /// 2. **Bucket accounting** — for every affected bucket, apply the
    ///    aggregate delta. Newly created postings remain in the bucket when it
    ///    still has room, otherwise they are scheduled for migration.
    /// 3. **Migration** — postings that no longer fit are moved to freshly
    ///    allocated buckets; both source and destination buckets are marked
    ///    dirty so a crash cannot resurrect stale data.
    ///
    /// # Arguments
    ///
    /// * `doc_id` - Document identifier
    /// * `field_values` - Field values to index for this document. Duplicates
    ///   are coalesced.
    /// * `now_ms` - Current timestamp in milliseconds
    ///
    /// # Returns
    ///
    /// Number of new `(doc_id, field_value)` associations actually created.
    /// Idempotent calls return `0`.
    ///
    /// # Errors
    ///
    /// Returns [`BTreeError::AlreadyExists`] when `allow_duplicates` is `false`
    /// and one of the field values already maps to a different `doc_id`.
    /// In the sequential case this is rejected by a pre-check before any
    /// mutation. If the conflict only appears mid-loop (a concurrent writer
    /// added a conflicting `doc_id` after the pre-check), associations created
    /// for field values processed before the conflicting one remain applied,
    /// with consistent internal bookkeeping; values after it are not processed.
    pub fn insert_array(
        &self,
        doc_id: PK,
        field_values: Vec<FV>,
        now_ms: u64,
    ) -> Result<usize, BTreeError> {
        if field_values.is_empty() {
            return Ok(0);
        }

        // Shared with other mutations, exclusive against `compact_buckets`.
        let _mutation_guard = self.mutation_gate.read();

        // Validate `doc_id` serialization up-front, before any state is
        // mutated (see `insert`).
        let doc_id_size =
            try_cbor_serialized_size(&doc_id).map_err(|err| BTreeError::Serialization {
                name: self.name.clone(),
                source: err,
            })? + 2;

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
                        id: json_value(&doc_id),
                        value: json_value(field_value),
                    });
                }
            }
        }

        // Ensure the current bucket exists (see insert()).
        let bucket_id = self.max_bucket_id.load(Ordering::Relaxed);
        if !self.buckets.contains_key(&bucket_id) {
            self.buckets
                .entry(bucket_id)
                .or_insert_with(|| (0, false, UniqueVec::default(), 0));
        }

        // An error detected mid-loop (uniqueness violation, or a field value
        // whose serialization fails) must NOT return early: postings already
        // modified in this call still need their btree keys and bucket
        // accounting (phases below), otherwise they would be invisible to
        // range queries and silently dropped by the next flush. Record the
        // error, stop processing further values, finish the bookkeeping for
        // what was applied, then surface the error.
        let mut deferred_error: Option<BTreeError> = None;

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
                        deferred_error = Some(BTreeError::AlreadyExists {
                            name: self.name.clone(),
                            id: json_value(&doc_id),
                            value: json_value(&field_value),
                        });
                        break;
                    }

                    // Only add the doc_id if it's not already present
                    if posting.2.push(doc_id.clone()) {
                        // Calculate size increase for this insertion
                        size_increase = doc_id_size;
                        posting.1 += 1; // Increment version
                    }
                }
                dashmap::Entry::Vacant(entry) => {
                    // Create a new posting for this field value
                    let posting = (bucket_id, 1, vec![doc_id.clone()].into());
                    // Reject an unserializable field value before inserting
                    // it; nothing has been mutated for this value yet, so
                    // stop the loop and surface the error after finishing the
                    // bookkeeping for the values already applied.
                    match try_posting_entry_size(&field_value, &posting) {
                        Ok(size) => size_increase = size,
                        Err(err) => {
                            deferred_error = Some(BTreeError::Serialization {
                                name: self.name.clone(),
                                source: err,
                            });
                            break;
                        }
                    }
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

        // Add all new values to the B-tree in a single operation.
        // Same phantom-key guard as in `insert`: skip keys whose posting was
        // concurrently removed between posting creation and this point.
        if !new_btree_values.is_empty() {
            let mut btree = self.btree.write();
            for field_value in new_btree_values {
                if self.postings.contains_key(&field_value) {
                    btree.insert(field_value);
                }
            }
        }

        // Phase 2: handle bucket overflow and updates
        // Process each field value individually to avoid migrating existing values unnecessarily.
        // field_values_to_migrate: (old_bucket_id, field_value, size)
        let mut field_values_to_migrate: Vec<(u32, FV, usize)> = Vec::new();
        for (bucket_id, (size_delta, field_values)) in bucket_updates {
            let mut bucket_entry = self
                .buckets
                .entry(bucket_id)
                .or_insert_with(|| (0, false, UniqueVec::default(), 0));

            self.mark_bucket_dirty(&mut bucket_entry);
            // Apply the aggregate delta computed in Phase 1. This covers both:
            //   * full posting size for newly-created postings, and
            //   * the per-doc_id growth for postings already living in this bucket.
            // Per-fv branches below only deal with placement of new postings.
            bucket_entry.0 = bucket_entry.0.saturating_add(size_delta);

            for fv in field_values {
                if bucket_entry.2.contains(&fv) {
                    // Existing posting whose growth was already accounted for above.
                    continue;
                }

                // Newly-created posting; decide whether it stays in this bucket or migrates.
                //
                // Known benign drift: `fv_size` is recomputed here from the
                // current posting state, which under concurrent writers may
                // differ from the delta accumulated in Phase 1 (so the
                // rollback below can be slightly off). This only degrades
                // packing decisions; the persisted state stays correct and
                // all size arithmetic saturates.
                let fv_size = if let Some(posting) = self.postings.get(&fv) {
                    posting_entry_size(&fv, &*posting)
                } else {
                    // Posting was concurrently removed; nothing more to do.
                    continue;
                };

                if bucket_entry.2.is_empty() || bucket_entry.0 < self.config.bucket_overload_size {
                    // Bucket has room (size already includes this fv via size_delta).
                    bucket_entry.2.push(fv);
                } else {
                    // Bucket is over the soft limit; migrate this fv to a fresh bucket.
                    // Roll back the size we tentatively added for it.
                    bucket_entry.0 = bucket_entry.0.saturating_sub(fv_size);
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
                {
                    // entry().or_insert_with() instead of get_mut(): the bucket
                    // normally exists, but if it ever went missing the posting
                    // would silently stop being tracked by any bucket and be
                    // lost on the next reload.
                    let mut nb = self
                        .buckets
                        .entry(next_bucket_id)
                        .or_insert_with(|| (0, false, UniqueVec::default(), 0));
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

        if let Some(err) = deferred_error {
            return Err(err);
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

        // Shared with other mutations, exclusive against `compact_buckets`.
        let _mutation_guard = self.mutation_gate.read();

        // Track removal statistics
        let mut removed_count = 0;
        // Track removals until we know whether empty postings were fully removed
        // or concurrently re-populated.
        let mut pending_removals = Vec::new();

        // First pass: collect which postings to modify
        for field_value in field_values {
            let mut removed = false;
            let mut doc_size_decrease = 0;
            let mut full_size_decrease = 0;
            let mut posting_empty = false;
            let mut bucket_id = 0;

            // Check if this field value exists
            if let Some(mut posting) = self.postings.get_mut(&field_value) {
                bucket_id = posting.0;

                // Only needed when this removal empties the posting; measuring it
                // only for a single-element posting avoids an O(n) CBOR pass on
                // every removal. See remove() for the rationale.
                let prev_posting_size = if posting.2.len() == 1 {
                    posting_entry_size(&field_value, &*posting)
                } else {
                    0
                };

                // Check if the document ID exists in the posting
                if posting.2.swap_remove_if(|id| id == &doc_id).is_some() {
                    removed = true;
                    posting.1 += 1; // Increment version
                    posting_empty = posting.2.is_empty();

                    // Calculate size decrease based on whether this key is fully removed.
                    doc_size_decrease = cbor_serialized_size(&doc_id) + 2;
                    full_size_decrease = prev_posting_size;
                }
            }

            if removed {
                pending_removals.push((
                    field_value,
                    bucket_id,
                    doc_size_decrease,
                    full_size_decrease,
                    posting_empty,
                ));
                removed_count += 1;
            }
        }

        // Remove empty postings from the index.
        // Use atomic check-and-remove: a concurrent `insert` might have re-populated
        // a posting between the first pass and here, so only remove if still empty.
        let mut entries_removed = FxHashSet::default();
        let mut bucket_updates: FxHashMap<u32, (usize, FxHashSet<FV>)> = FxHashMap::default();
        for (field_value, bucket_id, doc_size_decrease, full_size_decrease, posting_empty) in
            pending_removals
        {
            let mut entry_removed = false;
            if posting_empty
                && self
                    .postings
                    .remove_if(&field_value, |_, posting| posting.2.is_empty())
                    .is_some()
            {
                entry_removed = true;
                entries_removed.insert(field_value.clone());
            }

            let size_decrease = if entry_removed {
                full_size_decrease
            } else {
                doc_size_decrease
            };
            let bucket_entry = bucket_updates
                .entry(bucket_id)
                .or_insert_with(|| (0, FxHashSet::default()));
            bucket_entry.0 += size_decrease;
            bucket_entry.1.insert(field_value);
        }

        if !entries_removed.is_empty() {
            for value in &entries_removed {
                self.remove_btree_key_if_posting_absent(value);
            }
        }

        // Update all modified buckets
        for (bucket_id, (size_decrease, field_values)) in bucket_updates {
            if let Some(mut bucket) = self.buckets.get_mut(&bucket_id) {
                bucket.0 = bucket.0.saturating_sub(size_decrease);
                self.mark_bucket_dirty(&mut bucket); // Mark as dirty

                // Remove field values that are completely removed
                for fv in &field_values {
                    if entries_removed.contains(fv) {
                        let remove_from_bucket = match self.postings.get(fv) {
                            Some(posting) => posting.0 != bucket_id,
                            None => true,
                        };
                        if remove_from_bucket {
                            bucket.2.swap_remove_if(|k| k == fv);
                        }
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
    ///
    /// # Re-entrancy
    ///
    /// `f` runs while internal locks are held. It must not call back into the
    /// same index (e.g. `insert` / `remove`), or it may deadlock.
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
    ///
    /// # Re-entrancy
    ///
    /// `f` runs while internal locks are held (including the btree read lock
    /// during range scans). It must not call back into the same index, or it
    /// may deadlock.
    ///
    /// # Depth limit
    ///
    /// Queries nested deeper than [`RangeQuery::MAX_DEPTH`] are rejected
    /// (query evaluation is recursive; the cap prevents a stack overflow on
    /// maliciously deep queries). **This non-`try` method cannot report the
    /// rejection through its return type**: it returns an empty result — which
    /// is indistinguishable from "no matches" — and emits a `log::warn!` with
    /// the index name and the offending depth. If you need a hard error
    /// instead, validate the depth up-front: [`RangeQuery::try_convert_from`]
    /// returns an `Err` for over-deep queries, and [`RangeQuery::depth`] lets
    /// you check the cap explicitly before calling this method.
    pub fn range_query_with<F, R>(&self, query: RangeQuery<FV>, f: F) -> Vec<R>
    where
        F: FnMut(&FV, &Vec<PK>) -> (bool, Vec<R>),
    {
        self.range_query_inner(query, false, f)
    }

    /// Like [`range_query_with`](Self::range_query_with), but walks the key
    /// space from the **largest** matching key downwards.
    ///
    /// Both directions stop as soon as `f` returns `false`, so a caller that
    /// wants the *last* page of a range pays for that page only. The results
    /// are returned in ascending key order either way — the direction decides
    /// **which** keys a bounded scan collects, never how they are ordered.
    ///
    /// Scan direction is the caller's choice precisely because it cannot be
    /// derived from the query shape: `Lt(x)` bounded by a limit means "the
    /// smallest matches below x" to one caller and "the largest" to another,
    /// and silently picking one made the same predicate return opposite ends
    /// depending on where it appeared in a composite filter.
    pub fn range_query_rev_with<F, R>(&self, query: RangeQuery<FV>, f: F) -> Vec<R>
    where
        F: FnMut(&FV, &Vec<PK>) -> (bool, Vec<R>),
    {
        self.range_query_inner(query, true, f)
    }

    fn range_query_inner<F, R>(&self, query: RangeQuery<FV>, descending: bool, mut f: F) -> Vec<R>
    where
        F: FnMut(&FV, &Vec<PK>) -> (bool, Vec<R>),
    {
        let mut results = Vec::new();
        if self.postings.is_empty() {
            return results;
        }
        let depth = query.depth();
        if depth > RangeQuery::<FV>::MAX_DEPTH {
            log::warn!(
                action = "range_query_with",
                index = self.name.as_str(),
                depth = depth,
                max_depth = RangeQuery::<FV>::MAX_DEPTH;
                "BTreeIndex '{}': range query nesting depth {} exceeds the maximum of {}; \
                 returning an empty result",
                self.name,
                depth,
                RangeQuery::<FV>::MAX_DEPTH,
            );
            return results;
        }

        self.query_count.fetch_add(1, Ordering::Relaxed);

        // One walk for every arm: `descending` decides which keys a bounded
        // scan collects, never how they are ordered on the way out. Both
        // directions stop as soon as `f` says so, so either end of a range is
        // equally cheap to page.
        macro_rules! walk {
            ($keys:expr) => {{
                let keys = $keys;
                if descending {
                    let mut groups: Vec<Vec<R>> = Vec::new();
                    for k in keys.rev() {
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
                    // Group-level reversal: keys ascend again while each key's
                    // own posting order stays as the callback produced it.
                    return groups.into_iter().rev().flatten().collect();
                }
                for k in keys {
                    if let Some(posting) = self.postings.get(k) {
                        let (conti, rt) = f(k, &posting.2);
                        results.extend(rt);
                        if !conti {
                            return results;
                        }
                    }
                }
            }};
        }

        match query {
            RangeQuery::Eq(key) => {
                if let Some(posting) = self.postings.get(&key) {
                    let (_, rt) = f(&key, &posting.2);
                    results.extend(rt);
                }
            }
            RangeQuery::Gt(start_key) => {
                let btree = self.btree.read();
                walk!(btree.range((
                    std::ops::Bound::Excluded(start_key),
                    std::ops::Bound::Unbounded,
                )))
            }
            RangeQuery::Ge(start_key) => {
                let btree = self.btree.read();
                walk!(btree.range(std::ops::RangeFrom { start: start_key }))
            }
            RangeQuery::Lt(end_key) => {
                let btree = self.btree.read();
                walk!(btree.range(std::ops::RangeTo { end: end_key }))
            }
            RangeQuery::Le(end_key) => {
                let btree = self.btree.read();
                walk!(btree.range(std::ops::RangeToInclusive { end: end_key }))
            }
            RangeQuery::Between(start_key, end_key) => {
                if start_key > end_key {
                    return results; // empty result for invalid range
                }
                let btree = self.btree.read();
                walk!(btree.range(start_key..=end_key))
            }
            RangeQuery::Include(keys) => {
                let keys = BTreeSet::from_iter(keys);
                walk!(keys.iter())
            }
            RangeQuery::And(queries) => {
                // 先找出最小结果集的子查询，减少交集计算量
                let keys = self.range_keys(RangeQuery::And(queries));
                walk!(keys.iter())
            }
            RangeQuery::Or(queries) => {
                let keys = self.range_keys(RangeQuery::Or(queries));
                walk!(keys.iter())
            }
            RangeQuery::Not(query) => {
                // 先收集要排除的 key，再遍历全集差集
                let exclude: FxHashSet<FV> = self.range_keys(*query).into_iter().collect();
                let btree = self.btree.read();
                walk!(btree.iter().filter(|k| !exclude.contains(*k)))
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
                if start_key <= end_key {
                    results.extend(self.btree.read().range(start_key..=end_key).cloned());
                }
            }
            RangeQuery::Include(keys) => {
                let keys = BTreeSet::from_iter(keys);
                let btree = self.btree.read();
                results.extend(keys.into_iter().filter(|k| btree.contains(k)));
            }
            RangeQuery::And(queries) => {
                if queries.is_empty() {
                    return results;
                }

                let mut queries = queries;
                let seed_index = queries
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, query)| Self::range_query_seed_rank(query))
                    .map(|(index, _)| index)
                    .unwrap_or(0);
                let seed_query = *queries.swap_remove(seed_index);
                let mut intersection: BTreeSet<FV> =
                    self.range_keys(seed_query).into_iter().collect();

                for query in queries {
                    // `Include` is the common expensive predicate here: probe
                    // a hash set instead of an O(m) linear scan per key.
                    if let RangeQuery::Include(keys) = query.as_ref() {
                        let keys: FxHashSet<&FV> = keys.iter().collect();
                        intersection.retain(|key| keys.contains(key));
                    } else {
                        intersection.retain(|key| Self::range_key_matches_query(key, &query));
                    }
                    if intersection.is_empty() {
                        return vec![];
                    }
                }

                results.extend(intersection);
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

    /// Persists metadata and every currently-dirty bucket.
    ///
    /// This is a convenience wrapper around
    /// [`flush_owned_with`](Self::flush_owned_with) that writes the metadata
    /// blob to `metadata`; see `flush_owned_with` for the manifest commit
    /// protocol.
    ///
    /// # Arguments
    ///
    /// * `metadata` - writer that receives the CBOR-encoded metadata blob.
    /// * `now_ms`   - current unix-ms timestamp, recorded into `stats.last_saved`.
    /// * `f`        - async function used to persist each dirty bucket.
    ///
    /// # Durability
    ///
    /// `W` must be a "written means durable" target: this method treats the
    /// `write_all` into `metadata` as the manifest commit point. Once it
    /// returns, the index advances `last_saved_version`, publishes the new
    /// manifest, clears every dirty mark, and reports the objects the previous
    /// manifest referenced in [`FlushOutcome::obsolete`] — which the caller is
    /// expected to delete. If `W` merely stages bytes for a later fallible
    /// upload and that upload fails, the durable metadata still points at
    /// generations the caller was just told to delete: unrecoverable bucket
    /// loss. Use [`flush_owned_with`](Self::flush_owned_with) and perform the
    /// upload inside its metadata callback, so a failure leaves the generation
    /// uncommitted and fully retryable.
    ///
    /// # Returns
    ///
    /// See [`flush_owned_with`](Self::flush_owned_with).
    pub async fn flush<W: Write, F, Fut>(
        &self,
        metadata: W,
        now_ms: u64,
        f: F,
    ) -> Result<FlushOutcome, BTreeError>
    where
        F: FnMut(BucketObject, Vec<u8>) -> Fut,
        Fut: Future<Output = Result<(), BoxError>>,
    {
        self.flush_owned_with(
            now_ms,
            move |data: Vec<u8>| {
                let mut metadata = metadata;
                async move {
                    metadata.write_all(&data).map_err(BoxError::from)?;
                    Ok(())
                }
            },
            f,
        )
        .await
    }

    /// Persists every dirty bucket to a new immutable object, then commits
    /// the metadata whose manifest references them.
    ///
    /// # Manifest commit protocol
    ///
    /// Every dirty bucket is serialized and written to a **fresh** object
    /// keyed by `(bucket_id, generation)` — the generation is this flush's
    /// metadata version, so a bucket object is never mutated in place once a
    /// committed manifest references it. The metadata (carrying the manifest
    /// `bucket_id -> generation`) is written last; that single write is the
    /// atomic commit point:
    ///
    /// * A crash or error **before** the metadata commit leaves the new
    ///   bucket objects as unreferenced garbage. A loader still sees the
    ///   previous manifest — a complete, consistent snapshot. Nothing is
    ///   lost; the next flush retries at a later generation.
    /// * **After** the commit, the objects replaced by this flush are
    ///   garbage. They are returned as [`FlushOutcome::obsolete`] for the
    ///   caller to delete best-effort; a failed deletion only leaks space.
    ///
    /// [`compact_buckets`](Self::compact_buckets) needs no special write
    /// ordering under this protocol: the repacked layout becomes visible
    /// atomically with the manifest commit, and every pre-compaction object
    /// is reported as obsolete.
    ///
    /// # Durability
    ///
    /// `metadata_writer` returning `Ok(())` *is* the commit: this method then
    /// advances `last_saved_version`, publishes the manifest in memory, clears
    /// every dirty mark and reports the replaced objects as
    /// [`FlushOutcome::obsolete`] for the caller to delete. It must therefore
    /// return `Ok(())` only once the metadata blob is durably stored — never
    /// after merely staging it for a later fallible upload. A failure after a
    /// premature `Ok(())` leaves the durable metadata referencing generations
    /// the caller was just told to delete, and those buckets cannot be
    /// recovered. Perform the upload *inside* the callback and propagate its
    /// error instead: nothing is committed and the whole flush is retried at a
    /// later generation.
    ///
    /// # Concurrency
    ///
    /// The caller must not run a flush concurrently with mutations,
    /// compaction, or another flush (see the crate-level concurrency
    /// contract). Bucket payloads and the metadata are serialized before the
    /// first await, so callback latency never holds internal locks.
    ///
    /// # Arguments
    ///
    /// * `now_ms` - current unix-ms timestamp, recorded into `stats.last_saved`.
    /// * `metadata_writer` - async callback receiving the CBOR-encoded
    ///   metadata blob; it must return `Ok(())` only once the write is
    ///   durable, because it is the commit point.
    /// * `bucket_writer` - async callback invoked once per dirty bucket with
    ///   the target [`BucketObject`] and the CBOR payload. It must
    ///   create/overwrite the object addressed by `(bucket_id, generation)`.
    ///
    /// # Returns
    ///
    /// * `Ok(outcome)` with [`FlushOutcome::saved`] `== false` when the index
    ///   was already fully persisted (no callback was invoked).
    /// * `Ok(outcome)` with `saved == true` after a successful commit;
    ///   [`FlushOutcome::obsolete`] lists the replaced bucket objects.
    /// * `Err` on serialization failure or when any callback fails (nothing
    ///   was committed in that case).
    ///
    /// # Note on callback bounds
    ///
    /// The callbacks are plain `FnMut`/`FnOnce` closures returning a named
    /// future type and take owned `Vec<u8>` blobs: `AsyncFn*` bounds here
    /// make the resulting future's `Send`-ness non-generalizable over
    /// lifetimes (rustc: "implementation of `Send` is not general enough"),
    /// which would break every downstream `tokio::spawn` of a flush.
    pub async fn flush_owned_with<M, MFut, F, FFut>(
        &self,
        now_ms: u64,
        metadata_writer: M,
        mut bucket_writer: F,
    ) -> Result<FlushOutcome, BTreeError>
    where
        M: FnOnce(Vec<u8>) -> MFut,
        MFut: Future<Output = Result<(), BoxError>>,
        F: FnMut(BucketObject, Vec<u8>) -> FFut,
        FFut: Future<Output = Result<(), BoxError>>,
    {
        // Synchronous snapshot phase: serialize dirty buckets and the
        // manifest-bearing metadata before the first await.
        let has_dirty = self.has_dirty_buckets();
        if !has_dirty && !self.has_pending_metadata_flush() {
            return Ok(FlushOutcome::default());
        }

        // A bucket object only becomes reachable through the manifest, so
        // dirty buckets always require a metadata commit. Loading can mark
        // buckets dirty (stale-entry repair) without bumping the stats
        // version; force a fresh version in that case.
        if has_dirty && !self.has_pending_metadata_flush() {
            self.update_metadata(|m| m.stats.version += 1);
        }

        let dirty = self.serialize_dirty_buckets()?;

        let mut meta = self.metadata();
        meta.stats.last_saved = now_ms.max(meta.stats.last_saved);
        // This flush's generation: unique per committed manifest because the
        // stats version increases monotonically and is claimed exactly once.
        let generation = meta.stats.version;

        // Build the new manifest: dirty buckets move to this generation,
        // clean buckets keep their committed object. In-memory buckets that
        // were never persisted (e.g. the empty initial bucket) stay out.
        let committed = meta.buckets.clone();
        let dirty_ids: FxHashSet<u32> = dirty.iter().map(|s| s.bucket_id).collect();
        let mut manifest = BTreeMap::new();
        for entry in self.buckets.iter() {
            let id = *entry.key();
            if dirty_ids.contains(&id) {
                manifest.insert(id, generation);
            } else if let Some(committed_generation) = committed.get(&id) {
                manifest.insert(id, *committed_generation);
            }
        }
        meta.buckets = manifest.clone();

        let mut meta_buf = Vec::with_capacity(256);
        cbor2::to_writer(&BTreeIndexRef { metadata: &meta }, &mut meta_buf).map_err(|err| {
            BTreeError::Serialization {
                name: self.name.clone(),
                source: err.into(),
            }
        })?;

        // Objects the previous manifest referenced that the new one replaces
        // or drops (bucket rewrites, compaction leftovers, legacy objects).
        let obsolete: Vec<BucketObject> = committed
            .iter()
            .filter(|(id, generation)| manifest.get(id) != Some(generation))
            .map(|(id, generation)| BucketObject {
                bucket_id: *id,
                generation: *generation,
            })
            .collect();

        // Phase 1: write every dirty bucket to its new immutable object.
        // Unreachable until the commit below, so any failure here leaves the
        // previous durable snapshot fully intact.
        let saved_marks: Vec<(u32, u64)> = dirty
            .iter()
            .map(|snapshot| (snapshot.bucket_id, snapshot.dirty_version))
            .collect();
        for snapshot in dirty {
            bucket_writer(
                BucketObject {
                    bucket_id: snapshot.bucket_id,
                    generation,
                },
                snapshot.data,
            )
            .await
            .map_err(|source| BTreeError::Generic {
                name: self.name.clone(),
                source,
            })?;
        }

        // Phase 2: the manifest commit — the single atomic point.
        metadata_writer(meta_buf)
            .await
            .map_err(|source| BTreeError::Generic {
                name: self.name.clone(),
                source,
            })?;

        // Publish the committed state in memory.
        self.last_saved_version
            .fetch_max(generation, Ordering::Release);
        self.update_metadata(|m| {
            m.stats.last_saved = meta.stats.last_saved.max(m.stats.last_saved);
            m.buckets = manifest;
        });
        for (bucket_id, dirty_version) in saved_marks {
            self.mark_bucket_snapshot_saved(bucket_id, dirty_version);
        }

        Ok(FlushOutcome {
            saved: true,
            obsolete,
        })
    }

    /// Returns whether there are dirty buckets pending persistence.
    pub fn has_dirty_buckets(&self) -> bool {
        self.buckets.iter().any(|bucket| bucket.1)
    }

    /// Returns whether metadata has a newer logical version than the last
    /// serialized metadata snapshot.
    pub fn has_pending_metadata_flush(&self) -> bool {
        let current_version = { self.metadata.read().stats.version };
        self.last_saved_version.load(Ordering::Acquire) < current_version
    }

    /// Compacts fragmented buckets by re-binning all field values into fewer, properly-sized
    /// buckets using a first-fit-decreasing bin-packing strategy.
    ///
    /// This is intended as a one-time repair after the bucket-splitting bug that created
    /// many tiny buckets. After compaction all buckets are marked dirty and will be
    /// persisted on the next [`flush`](Self::flush) call.
    ///
    /// # Concurrency
    ///
    /// This method rebuilds the bucket map non-atomically, so it takes the
    /// index's mutation gate **exclusively**: concurrent `insert*` / `remove*`
    /// calls block for its duration instead of losing postings created between
    /// the `postings` snapshot and the rebuild (such a posting would belong to
    /// no bucket, and buckets are what gets serialized). Excluding *flushes*
    /// remains the caller's responsibility (see the crate-level concurrency
    /// contract). It also requires a fully loaded index.
    ///
    /// # Persistence
    ///
    /// The repacked layout becomes durable atomically with the next flush's
    /// manifest commit; every pre-compaction bucket object is reported in
    /// that flush's [`FlushOutcome::obsolete`] for best-effort deletion. No
    /// special write ordering is required. See `anda_db`'s `BTree::compact`
    /// for a production wrapper.
    ///
    /// # Returns
    ///
    /// `(old_bucket_count, new_bucket_count)`
    pub fn compact_buckets(&self) -> (usize, usize) {
        // Exclusive: no mutation may observe — or add to — the half-rebuilt
        // bucket map. Every mutator takes the shared side of this gate before
        // touching any other lock, so the ordering is uniform and deadlock-free.
        let _mutation_guard = self.mutation_gate.write();

        let old_count = self.buckets.len();
        if old_count <= 1 {
            return (old_count, old_count);
        }

        // Step 1: Estimate each field value's serialized contribution.
        let mut fv_sizes: Vec<(FV, usize)> = self
            .postings
            .iter()
            .map(|entry| {
                let size = posting_entry_size(entry.key(), entry.value());
                (entry.key().clone(), size)
            })
            .collect();

        if fv_sizes.is_empty() {
            self.buckets.clear();
            self.buckets.insert(0, (0, true, UniqueVec::default(), 1));
            self.max_bucket_id.store(0, Ordering::Relaxed);
            self.update_metadata(|m| {
                m.stats.version += 1;
            });
            return (old_count, 1);
        }

        // Step 2: Sort by size descending for better packing.
        fv_sizes.sort_unstable_by_key(|b| std::cmp::Reverse(b.1));

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
        self.update_metadata(|m| {
            m.stats.version += 1;
        });

        (old_count, new_count)
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
    ///
    /// # Re-entrancy
    ///
    /// `f` runs while internal locks are held (including the btree read lock).
    /// It must not call back into the same index, or it may deadlock.
    pub fn prefix_query_with<F, R>(&self, prefix: &str, mut f: F) -> Vec<R>
    where
        F: FnMut(&str, &Vec<PK>) -> (bool, Option<R>),
    {
        let mut results = Vec::new();
        if self.postings.is_empty() {
            return results;
        }

        self.query_count.fetch_add(1, Ordering::Relaxed);

        // 从 prefix 起正序遍历，遇到第一个不以 prefix 开头的键即终止。
        // 以 prefix 开头的键在 BTreeSet 中是连续区段，因此这种写法是完备的；
        // 而旧实现构造 "prefix + char::MAX" 作为闭区间上界，会漏掉
        // "prefix + char::MAX + 任意后缀" 这类键。空前缀自然退化为全量遍历。
        for k in self.btree.read().range(prefix.to_string()..) {
            if !k.starts_with(prefix) {
                break;
            }
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
    use std::collections::HashMap;
    use std::io;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::Barrier;

    // 获取当前时间戳（毫秒）
    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    /// In-memory model of the durable store: one metadata object plus one
    /// object per `(bucket_id, generation)`, mirroring the production
    /// object-store layout.
    #[derive(Default, Clone)]
    struct MemStore {
        metadata: Vec<u8>,
        buckets: HashMap<BucketObject, Vec<u8>>,
    }

    /// Flushes `index` into `store` the way the production adapter does:
    /// buckets first, then the manifest commit, then best-effort deletion of
    /// the obsolete objects.
    async fn flush_to<PK, FV>(
        index: &BTreeIndex<PK, FV>,
        store: &mut MemStore,
        now_ms: u64,
    ) -> FlushOutcome
    where
        PK: Ord + Eq + Hash + Debug + Clone + Serialize + DeserializeOwned,
        FV: Eq + Ord + Hash + Debug + Clone + Serialize + DeserializeOwned,
    {
        let mut meta_buf: Vec<u8> = Vec::new();
        let buckets = &mut store.buckets;
        let outcome = index
            .flush_owned_with(
                now_ms,
                |data| {
                    meta_buf = data;
                    std::future::ready(Ok(()))
                },
                |object, data| {
                    buckets.insert(object, data);
                    std::future::ready(Ok(()))
                },
            )
            .await
            .unwrap();
        if outcome.saved {
            store.metadata = meta_buf;
            for object in &outcome.obsolete {
                store.buckets.remove(object);
            }
        }
        outcome
    }

    /// Loads a complete index from `store`.
    async fn load_from<PK, FV>(store: &MemStore) -> BTreeIndex<PK, FV>
    where
        PK: Ord + Eq + Hash + Debug + Clone + Serialize + DeserializeOwned,
        FV: Eq + Ord + Hash + Debug + Clone + Serialize + DeserializeOwned,
    {
        BTreeIndex::load_all(&store.metadata[..], async |object| {
            Ok(store.buckets.get(&object).cloned())
        })
        .await
        .unwrap()
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

    fn encode_bucket(index: &BTreeIndex<u64, String>, bucket_id: u32) -> Vec<u8> {
        let bucket = index.buckets.get(&bucket_id).unwrap();
        let postings: rustc_hash::FxHashMap<_, _> = bucket
            .2
            .iter()
            .filter_map(|fv| index.postings.get(fv).map(|posting| (fv, posting)))
            .collect();
        let mut buf = Vec::new();
        cbor2::to_writer(
            &BucketRef {
                postings: &postings,
            },
            &mut buf,
        )
        .unwrap();
        buf
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
    struct TestKey(String);

    /// A value whose serialization fails on demand (`.1 == true`), used to
    /// exercise the non-panicking serialization error paths.
    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize)]
    struct Flaky(u8, bool);

    impl Serialize for Flaky {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            if self.1 {
                Err(serde::ser::Error::custom("flaky serialization failure"))
            } else {
                self.0.serialize(serializer)
            }
        }
    }

    impl TryFrom<String> for TestKey {
        type Error = BoxError;

        fn try_from(value: String) -> Result<Self, Self::Error> {
            if value == "bad" {
                Err("bad key".into())
            } else {
                Ok(TestKey(value))
            }
        }
    }

    struct FailingWriter;

    impl Write for FailingWriter {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("writer failed"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
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
    fn test_default_config_getters_and_query_private_helpers() {
        let default_config = BTreeConfig::default();
        assert_eq!(default_config.bucket_overload_size, 1024 * 512);
        assert!(default_config.allow_duplicates);

        let index = BTreeIndex::<u64, String>::new("defaulted".to_string(), None);
        assert_eq!(index.name(), "defaulted");
        assert!(index.allow_duplicates());
        assert!(index.has_pending_metadata_flush());
        assert!(!index.has_dirty_buckets());
        assert_eq!(index.keys(None, None), Vec::<String>::new());
        assert_eq!(
            index.range_query_with(RangeQuery::Ge("a".to_string()), |_, _| (true, vec![1_u64])),
            Vec::<u64>::new()
        );

        assert_eq!(
            BTreeIndex::<u64, String>::range_query_seed_rank(&RangeQuery::Eq("a".to_string())),
            0
        );
        assert_eq!(
            BTreeIndex::<u64, String>::range_query_seed_rank(&RangeQuery::Between(
                "z".to_string(),
                "a".to_string(),
            )),
            0
        );
        assert_eq!(
            BTreeIndex::<u64, String>::range_query_seed_rank(&RangeQuery::Include(vec![])),
            0
        );
        assert_eq!(
            BTreeIndex::<u64, String>::range_query_seed_rank(&RangeQuery::Include(vec![
                "a".to_string()
            ])),
            1
        );
        assert_eq!(
            BTreeIndex::<u64, String>::range_query_seed_rank(&RangeQuery::Between(
                "a".to_string(),
                "z".to_string(),
            )),
            2
        );
        assert_eq!(
            BTreeIndex::<u64, String>::range_query_seed_rank(&RangeQuery::Gt("a".to_string())),
            3
        );
        assert_eq!(
            BTreeIndex::<u64, String>::range_query_seed_rank(&RangeQuery::Or(vec![])),
            4
        );
        assert_eq!(
            BTreeIndex::<u64, String>::range_query_seed_rank(&RangeQuery::Not(Box::new(
                RangeQuery::Eq("a".to_string())
            ))),
            5
        );
        assert_eq!(
            BTreeIndex::<u64, String>::range_query_seed_rank(&RangeQuery::And(vec![])),
            0
        );

        let key = "m".to_string();
        assert!(BTreeIndex::<u64, String>::range_key_matches_query(
            &key,
            &RangeQuery::Gt("a".to_string())
        ));
        assert!(BTreeIndex::<u64, String>::range_key_matches_query(
            &key,
            &RangeQuery::Ge("m".to_string())
        ));
        assert!(BTreeIndex::<u64, String>::range_key_matches_query(
            &key,
            &RangeQuery::Lt("z".to_string())
        ));
        assert!(BTreeIndex::<u64, String>::range_key_matches_query(
            &key,
            &RangeQuery::Le("m".to_string())
        ));
        assert!(BTreeIndex::<u64, String>::range_key_matches_query(
            &key,
            &RangeQuery::Include(vec!["m".to_string()])
        ));
        assert!(BTreeIndex::<u64, String>::range_key_matches_query(
            &key,
            &RangeQuery::Eq("m".to_string())
        ));
        assert!(BTreeIndex::<u64, String>::range_key_matches_query(
            &key,
            &RangeQuery::Between("a".to_string(), "z".to_string())
        ));
        assert!(!BTreeIndex::<u64, String>::range_key_matches_query(
            &key,
            &RangeQuery::Between("z".to_string(), "a".to_string())
        ));
        assert!(BTreeIndex::<u64, String>::range_key_matches_query(
            &key,
            &RangeQuery::Or(vec![
                Box::new(RangeQuery::Eq("x".to_string())),
                Box::new(RangeQuery::Eq("m".to_string())),
            ])
        ));
        assert!(BTreeIndex::<u64, String>::range_key_matches_query(
            &key,
            &RangeQuery::And(vec![
                Box::new(RangeQuery::Ge("a".to_string())),
                Box::new(RangeQuery::Le("z".to_string())),
            ])
        ));
        assert!(BTreeIndex::<u64, String>::range_key_matches_query(
            &key,
            &RangeQuery::Not(Box::new(RangeQuery::Eq("x".to_string())))
        ));
        assert!(!BTreeIndex::<u64, String>::range_key_matches_query(
            &key,
            &RangeQuery::And(vec![])
        ));

        let populated = create_populated_index();
        assert_eq!(
            populated.keys(Some("apple".to_string()), Some(2)),
            vec!["banana".to_string(), "cherry".to_string()]
        );
        assert_eq!(
            populated.keys(Some("cherry".to_string()), None),
            vec!["date".to_string(), "eggplant".to_string()]
        );
        assert_eq!(
            populated.keys(None, Some(2)),
            vec!["apple".to_string(), "banana".to_string()]
        );

        let mut writer = FailingWriter;
        writer.flush().unwrap();
    }

    #[test]
    fn test_range_query_try_convert_from_all_variants_and_errors() {
        let converted =
            RangeQuery::<TestKey>::try_convert_from(RangeQuery::Eq("a".to_string())).unwrap();
        assert!(matches!(converted, RangeQuery::Eq(TestKey(ref v)) if v == "a"));

        assert!(matches!(
            RangeQuery::<TestKey>::try_convert_from(RangeQuery::Gt("a".to_string())).unwrap(),
            RangeQuery::Gt(TestKey(ref v)) if v == "a"
        ));
        assert!(matches!(
            RangeQuery::<TestKey>::try_convert_from(RangeQuery::Ge("a".to_string())).unwrap(),
            RangeQuery::Ge(TestKey(ref v)) if v == "a"
        ));
        assert!(matches!(
            RangeQuery::<TestKey>::try_convert_from(RangeQuery::Lt("a".to_string())).unwrap(),
            RangeQuery::Lt(TestKey(ref v)) if v == "a"
        ));
        assert!(matches!(
            RangeQuery::<TestKey>::try_convert_from(RangeQuery::Le("a".to_string())).unwrap(),
            RangeQuery::Le(TestKey(ref v)) if v == "a"
        ));
        assert!(matches!(
            RangeQuery::<TestKey>::try_convert_from(RangeQuery::Between(
                "a".to_string(),
                "z".to_string(),
            ))
            .unwrap(),
            RangeQuery::Between(TestKey(ref a), TestKey(ref z)) if a == "a" && z == "z"
        ));
        assert!(matches!(
            RangeQuery::<TestKey>::try_convert_from(RangeQuery::Include(vec![
                "a".to_string(),
                "b".to_string(),
            ]))
            .unwrap(),
            RangeQuery::Include(keys) if keys == vec![TestKey("a".to_string()), TestKey("b".to_string())]
        ));
        assert!(matches!(
            RangeQuery::<TestKey>::try_convert_from(RangeQuery::And(vec![
                Box::new(RangeQuery::Ge("a".to_string())),
                Box::new(RangeQuery::Le("z".to_string())),
            ]))
            .unwrap(),
            RangeQuery::And(queries) if queries.len() == 2
        ));
        assert!(matches!(
            RangeQuery::<TestKey>::try_convert_from(RangeQuery::Or(vec![
                Box::new(RangeQuery::Eq("a".to_string())),
                Box::new(RangeQuery::Eq("b".to_string())),
            ]))
            .unwrap(),
            RangeQuery::Or(queries) if queries.len() == 2
        ));
        assert!(matches!(
            RangeQuery::<TestKey>::try_convert_from(RangeQuery::Not(Box::new(RangeQuery::Eq(
                "a".to_string()
            ))))
            .unwrap(),
            RangeQuery::Not(_)
        ));

        assert!(
            RangeQuery::<TestKey>::try_convert_from(RangeQuery::Eq("bad".to_string())).is_err()
        );
        assert!(
            RangeQuery::<TestKey>::try_convert_from(RangeQuery::And(vec![
                Box::new(RangeQuery::Eq("ok".to_string())),
                Box::new(RangeQuery::Eq("bad".to_string())),
            ]))
            .is_err()
        );
    }

    #[test]
    fn test_range_query_with_early_stop_variants_and_prefix_empty() {
        let index = create_populated_index();

        for query in [
            RangeQuery::Eq("apple".to_string()),
            RangeQuery::Gt("apple".to_string()),
            RangeQuery::Ge("apple".to_string()),
            RangeQuery::Between("apple".to_string(), "date".to_string()),
            RangeQuery::Include(vec!["banana".to_string(), "date".to_string()]),
            RangeQuery::And(vec![
                Box::new(RangeQuery::Ge("apple".to_string())),
                Box::new(RangeQuery::Le("date".to_string())),
            ]),
            RangeQuery::Or(vec![
                Box::new(RangeQuery::Eq("apple".to_string())),
                Box::new(RangeQuery::Eq("date".to_string())),
            ]),
            RangeQuery::Not(Box::new(RangeQuery::Eq("apple".to_string()))),
        ] {
            let values =
                index.range_query_with(query, |key, ids| (false, vec![(key.clone(), ids.len())]));
            assert_eq!(values.len(), 1);
        }

        assert_eq!(
            index.range_query_with(
                RangeQuery::Between("z".to_string(), "a".to_string()),
                |key, _| (true, vec![key.clone()])
            ),
            Vec::<String>::new()
        );

        assert_eq!(
            index.range_keys(RangeQuery::Gt("cherry".to_string())),
            vec!["date".to_string(), "eggplant".to_string()]
        );
        assert_eq!(
            index.range_keys(RangeQuery::Include(vec![
                "missing".to_string(),
                "banana".to_string(),
            ])),
            vec!["banana".to_string()]
        );

        let all_prefix =
            index.prefix_query_with("", |key, ids| (false, Some((key.to_string(), ids.len()))));
        assert_eq!(all_prefix, vec![("apple".to_string(), 2)]);

        let empty = create_test_index();
        assert_eq!(
            empty.prefix_query_with("", |key, _| (true, Some(key.to_string()))),
            Vec::<String>::new()
        );
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
        // 方向由调用方选择，而不是由查询形态推断：同一个 Lt/Le 查询配同一个
        // 上限，正向扫描取最小的若干个 key，反向扫描取最大的若干个，两者的
        // 输出都是正序。

        // 反向：Lt(date) 倒序遍历 cherry, banana, apple，截断 2 个
        // => [cherry, banana]，最终正序 [banana, cherry]
        let mut count = 0usize;
        let results = index.range_query_rev_with(RangeQuery::Lt("date".to_string()), |k, _| {
            count += 1;
            (count < 2, vec![k.clone()])
        });
        assert_eq!(results, vec!["banana", "cherry"]);

        // 正向：同样的 Lt(date) 从最小 key 开始，截断 2 个 => [apple, banana]
        let mut count = 0usize;
        let results = index.range_query_with(RangeQuery::Lt("date".to_string()), |k, _| {
            count += 1;
            (count < 2, vec![k.clone()])
        });
        assert_eq!(results, vec!["apple", "banana"]);

        // 反向：Le(date) 倒序遍历 date, cherry, banana, apple，截断 2 个
        // => [date, cherry]，最终正序 [cherry, date]
        let mut count = 0usize;
        let results = index.range_query_rev_with(RangeQuery::Le("date".to_string()), |k, _| {
            count += 1;
            (count < 2, vec![k.clone()])
        });
        assert_eq!(results, vec!["cherry", "date"]);

        // 正向：Le(date) 截断 2 个 => [apple, banana]
        let mut count = 0usize;
        let results = index.range_query_with(RangeQuery::Le("date".to_string()), |k, _| {
            count += 1;
            (count < 2, vec![k.clone()])
        });
        assert_eq!(results, vec!["apple", "banana"]);

        // 反向扫描同样适用于 Gt/Ge/Between：Ge(banana) 取最大的 2 个
        // （banana, cherry, date, eggplant 中最大的两个）
        let mut count = 0usize;
        let results = index.range_query_rev_with(RangeQuery::Ge("banana".to_string()), |k, _| {
            count += 1;
            (count < 2, vec![k.clone()])
        });
        assert_eq!(results, vec!["date", "eggplant"]);

        // 再测试当“上限”大于可返回数量时，两个方向都返回全部（正序）
        let mut count = 0usize;
        let results = index.range_query_with(RangeQuery::Lt("banana".to_string()), |k, _| {
            count += 1;
            (count < 10, vec![k.clone()])
        });
        assert_eq!(results, vec!["apple"]);

        let mut count = 0usize;
        let results = index.range_query_rev_with(RangeQuery::Lt("banana".to_string()), |k, _| {
            count += 1;
            (count < 10, vec![k.clone()])
        });
        assert_eq!(results, vec!["apple"]);
    }

    #[test]
    fn test_range_query_lt_le_group_order_preserved() {
        let index = create_populated_index();
        // 反向遍历后“组内顺序”保持，并最终整体正序。
        // 取 Lt(date) 最大的 2 个 key：banana, cherry，最终顺序应为：
        // banana-1, banana-2, cherry-1, cherry-2

        let mut count = 0usize;
        let results = index.range_query_rev_with(RangeQuery::Lt("date".to_string()), |k, _| {
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

        // Le(date) 最大的 2 个：cherry, date，组内顺序保持：
        // cherry-1, cherry-2, date-1, date-2
        let mut count = 0usize;
        let results = index.range_query_rev_with(RangeQuery::Le("date".to_string()), |k, _| {
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

        // 正向遍历取最小的 2 个 key，组内顺序同样保持
        let mut count = 0usize;
        let results = index.range_query_with(RangeQuery::Le("date".to_string()), |k, _| {
            count += 1;
            let v = vec![format!("{k}-1"), format!("{k}-2")];
            (count < 2, v)
        });
        assert_eq!(
            results,
            vec![
                "apple-1".to_string(),
                "apple-2".to_string(),
                "banana-1".to_string(),
                "banana-2".to_string()
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
    fn test_range_keys_invalid_between_inside_logical_queries() {
        let index = create_populated_index();

        let invalid_between = RangeQuery::Between("date".to_string(), "banana".to_string());

        let results = index.range_query_with(
            RangeQuery::Or(vec![
                Box::new(invalid_between.clone()),
                Box::new(RangeQuery::Eq("apple".to_string())),
            ]),
            |key, _| (true, vec![key.clone()]),
        );
        assert_eq!(results, vec!["apple"]);

        let results = index.range_query_with(
            RangeQuery::And(vec![
                Box::new(RangeQuery::Ge("apple".to_string())),
                Box::new(invalid_between.clone()),
            ]),
            |key, _| (true, vec![key.clone()]),
        );
        assert!(results.is_empty());

        let results = index
            .range_query_with(RangeQuery::Not(Box::new(invalid_between)), |key, _| {
                (true, vec![key.clone()])
            });
        assert_eq!(results, index.keys(None, None));
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

        // 通过 manifest flush 持久化元数据与全部脏桶
        let mut store = MemStore::default();
        let outcome = flush_to(&index, &mut store, now_ms()).await;
        assert!(outcome.saved);

        println!("Serialized metadata: {:?}", hex::encode(&store.metadata));

        // 重新加载
        let loaded_index: BTreeIndex<u64, String> = load_from(&store).await;

        // 验证加载后的索引
        assert_eq!(loaded_index.name(), "test_index");
        assert_eq!(loaded_index.len(), index.len());

        // 测试搜索
        let result = loaded_index.query_with(&"apple".to_string(), |ids| Some(ids.clone()));
        assert!(result.is_some());
        let ids = result.unwrap();
        assert!(ids.contains(&1));
        assert!(ids.contains(&6));
    }

    /// A dirty bucket always forces a manifest commit — bucket objects are
    /// unreachable until the metadata references them. This covers the
    /// load-time repair path, which marks buckets dirty without bumping the
    /// stats version.
    #[tokio::test]
    async fn test_flush_commits_manifest_even_if_metadata_version_unchanged() {
        let index = create_test_index();
        index.insert(1, "apple".to_string(), now_ms()).unwrap();
        let mut store = MemStore::default();
        flush_to(&index, &mut store, now_ms()).await;
        assert!(!index.has_pending_metadata_flush());
        assert!(!index.has_dirty_buckets());

        // Simulate a load-time repair: dirty bucket, no version bump.
        {
            let mut bucket = index.buckets.get_mut(&0).expect("bucket 0 exists");
            index.mark_bucket_dirty(&mut bucket);
        }
        assert!(!index.has_pending_metadata_flush());
        assert!(index.has_dirty_buckets());

        let before = store.clone();
        let outcome = flush_to(&index, &mut store, now_ms()).await;
        assert!(outcome.saved);
        assert!(!index.has_dirty_buckets());
        assert!(!index.has_pending_metadata_flush());
        assert_ne!(
            before.metadata, store.metadata,
            "the manifest commit must rewrite the metadata"
        );

        let reloaded: BTreeIndex<u64, String> = load_from(&store).await;
        assert_eq!(reloaded.len(), index.len());
    }

    #[tokio::test]
    async fn test_flush_propagates_bucket_write_error_and_commits_nothing() {
        let index = create_test_index();
        index.insert(1, "apple".to_string(), now_ms()).unwrap();
        assert!(index.has_dirty_buckets());

        let mut meta_buf = Vec::new();
        let err = index
            .flush(&mut meta_buf, now_ms(), |_, _| {
                std::future::ready(Err::<(), BoxError>("write failed".into()))
            })
            .await
            .unwrap_err();

        match err {
            BTreeError::Generic { .. } => {}
            other => panic!("Expected Generic error, got: {other:?}"),
        }

        assert!(meta_buf.is_empty(), "no manifest commit on bucket failure");
        assert!(index.has_dirty_buckets());
        assert!(index.has_pending_metadata_flush());
    }

    #[tokio::test]
    async fn test_migrated_source_bucket_is_persisted_to_prevent_resurrection() {
        let config = BTreeConfig {
            bucket_overload_size: 80,
            allow_duplicates: true,
        };
        let index = BTreeIndex::new("resurrection_test".to_string(), Some(config));
        let mut store = MemStore::default();

        // Step 1: initial data persisted in bucket 0.
        index.insert(1, "apple".to_string(), now_ms()).unwrap();
        flush_to(&index, &mut store, now_ms()).await;

        // Step 2: force migration of "apple" to a new bucket.
        let mut doc_id = 2u64;
        while index.stats().max_bucket_id == 0 && doc_id < 200 {
            index.insert(doc_id, "apple".to_string(), now_ms()).unwrap();
            doc_id += 1;
        }
        assert!(index.stats().max_bucket_id > 0);
        flush_to(&index, &mut store, now_ms()).await;

        // Step 3: remove all docs for "apple", persist again.
        for id in 1..doc_id {
            index.remove(id, "apple".to_string(), now_ms());
        }
        assert!(
            index
                .query_with(&"apple".to_string(), |ids| Some(ids.clone()))
                .is_none()
        );
        flush_to(&index, &mut store, now_ms()).await;

        // Step 4: reload and verify no resurrection.
        let loaded: BTreeIndex<u64, String> = load_from(&store).await;
        assert!(
            loaded
                .query_with(&"apple".to_string(), |ids| Some(ids.clone()))
                .is_none(),
            "apple should not resurrect from a stale bucket object"
        );
    }

    #[tokio::test]
    async fn test_legacy_load_reconciles_stale_source_bucket_duplicate() {
        let config = BTreeConfig {
            bucket_overload_size: 80,
            allow_duplicates: true,
        };
        let index = BTreeIndex::new("partial_migration_flush".to_string(), Some(config));

        // Persist the pre-migration state (apple lives in bucket 0).
        index.insert(1, "apple".to_string(), now_ms()).unwrap();
        let mut store = MemStore::default();
        flush_to(&index, &mut store, now_ms()).await;
        let stale_bucket0 = store
            .buckets
            .values()
            .next()
            .expect("bucket 0 must be persisted")
            .clone();

        // Migrate "apple" out of bucket 0 in memory.
        let mut doc_id = 2u64;
        while index.stats().max_bucket_id == 0 && doc_id < 200 {
            index.insert(doc_id, "apple".to_string(), now_ms()).unwrap();
            doc_id += 1;
        }
        let apple = "apple".to_string();
        let migrated_bucket_id = index.postings.get(&apple).unwrap().0;
        assert!(
            migrated_bucket_id > 0,
            "apple should migrate out of bucket 0"
        );

        // Craft the legacy (pre-manifest) crash layout: metadata without a
        // manifest, the stale bucket 0 object, and the migrated destination
        // bucket — the old protocol could crash in exactly this state.
        let mut legacy_meta = index.metadata();
        legacy_meta.buckets = BTreeMap::new();
        let mut metadata_buf = Vec::new();
        cbor2::to_writer(
            &BTreeIndexRef {
                metadata: &legacy_meta,
            },
            &mut metadata_buf,
        )
        .unwrap();
        let legacy_store = MemStore {
            metadata: metadata_buf,
            buckets: HashMap::from_iter([
                (
                    BucketObject {
                        bucket_id: 0,
                        generation: 0,
                    },
                    stale_bucket0,
                ),
                (
                    BucketObject {
                        bucket_id: migrated_bucket_id,
                        generation: 0,
                    },
                    encode_bucket(&index, migrated_bucket_id),
                ),
            ]),
        };

        let loaded: BTreeIndex<u64, String> = load_from(&legacy_store).await;
        assert!(
            loaded.has_dirty_buckets(),
            "stale source bucket needs repair"
        );
        assert_eq!(
            loaded.postings.get(&apple).unwrap().0,
            migrated_bucket_id,
            "the higher-numbered legacy bucket must win"
        );

        // The repair flush upgrades to the manifest format; a reload keeps
        // the reconciled ownership.
        let mut repaired = legacy_store.clone();
        let outcome = flush_to(&loaded, &mut repaired, now_ms()).await;
        assert!(outcome.saved);

        let reloaded: BTreeIndex<u64, String> = load_from(&repaired).await;
        assert_eq!(reloaded.postings.get(&apple).unwrap().0, migrated_bucket_id);
        assert!(!reloaded.has_dirty_buckets());
    }

    /// The manifest commit must be the last write of a flush: every dirty
    /// bucket object precedes the metadata, and each is written to a fresh
    /// generation-suffixed object.
    #[tokio::test]
    async fn test_flush_writes_all_buckets_before_manifest_commit() {
        let config = BTreeConfig {
            bucket_overload_size: 80,
            allow_duplicates: true,
        };
        let index = BTreeIndex::new("ordered_migration_flush".to_string(), Some(config));
        let mut store = MemStore::default();
        index.insert(1, "apple".to_string(), now_ms()).unwrap();
        flush_to(&index, &mut store, now_ms()).await;

        // Dirty both the old bucket and freshly-allocated migration targets.
        let mut doc_id = 2u64;
        while index.stats().max_bucket_id == 0 && doc_id < 200 {
            index.insert(doc_id, "apple".to_string(), now_ms()).unwrap();
            doc_id += 1;
        }
        assert!(index.stats().max_bucket_id > 0);

        #[derive(Debug, PartialEq, Eq)]
        enum Event {
            Bucket(u32),
            Metadata,
        }
        let events = std::cell::RefCell::new(Vec::<Event>::new());
        index
            .flush_owned_with(
                now_ms(),
                |_data| {
                    events.borrow_mut().push(Event::Metadata);
                    std::future::ready(Ok(()))
                },
                |object, _data| {
                    assert!(
                        object.generation > 0,
                        "bucket writes must target generation-suffixed objects"
                    );
                    events.borrow_mut().push(Event::Bucket(object.bucket_id));
                    std::future::ready(Ok(()))
                },
            )
            .await
            .unwrap();

        let events = events.into_inner();
        assert!(
            events.len() > 1,
            "expected bucket writes and one metadata write: {events:?}"
        );
        assert_eq!(
            events.last(),
            Some(&Event::Metadata),
            "the manifest commit must come last: {events:?}"
        );
        assert_eq!(
            events.iter().filter(|e| **e == Event::Metadata).count(),
            1,
            "exactly one metadata write: {events:?}"
        );
    }

    /// A crash after some (or all) new-generation bucket objects are written
    /// but before the manifest commit must leave the previous snapshot fully
    /// intact — the new objects are unreferenced garbage — and a retry must
    /// converge.
    #[tokio::test]
    async fn test_flush_crash_before_manifest_commit_keeps_previous_snapshot() {
        let config = BTreeConfig {
            bucket_overload_size: 80,
            allow_duplicates: true,
        };
        let index = BTreeIndex::new("crash_before_commit".to_string(), Some(config));
        let mut store = MemStore::default();
        index.insert(1, "apple".to_string(), now_ms()).unwrap();
        flush_to(&index, &mut store, now_ms()).await;

        // Migrate "apple" and add new keys, then crash mid-flush: the first
        // bucket write succeeds, the second fails, the metadata is never
        // written.
        let mut doc_id = 2u64;
        while index.stats().max_bucket_id == 0 && doc_id < 200 {
            index.insert(doc_id, "apple".to_string(), now_ms()).unwrap();
            doc_id += 1;
        }
        index
            .insert(doc_id, "banana".to_string(), now_ms())
            .unwrap();

        let mut crashed = store.clone();
        let mut metadata_written = false;
        {
            let buckets = &mut crashed.buckets;
            let mut writes = 0usize;
            let err = index
                .flush_owned_with(
                    now_ms(),
                    |_data| {
                        metadata_written = true;
                        std::future::ready(Ok(()))
                    },
                    |object, data| {
                        if writes >= 1 {
                            return std::future::ready(Err::<(), BoxError>(
                                "crash after first bucket".into(),
                            ));
                        }
                        writes += 1;
                        buckets.insert(object, data);
                        std::future::ready(Ok(()))
                    },
                )
                .await
                .unwrap_err();
            assert!(matches!(err, BTreeError::Generic { .. }));
        }
        assert!(
            !metadata_written,
            "the manifest must not be committed when a bucket write fails"
        );

        // Reload from the old manifest plus orphaned objects: the previous
        // snapshot is complete, the uncommitted mutations are invisible.
        let loaded: BTreeIndex<u64, String> = load_from(&crashed).await;
        let apple_ids = loaded
            .query_with(&"apple".to_string(), |ids| Some(ids.clone()))
            .expect("apple must survive the crash");
        assert_eq!(apple_ids, vec![1], "only the committed posting is visible");
        assert!(
            loaded
                .query_with(&"banana".to_string(), |ids| Some(ids.clone()))
                .is_none(),
            "uncommitted key must stay invisible"
        );

        // The retry persists one complete new snapshot.
        let mut recovered_store = crashed;
        assert!(flush_to(&index, &mut recovered_store, now_ms()).await.saved);
        let recovered: BTreeIndex<u64, String> = load_from(&recovered_store).await;
        assert_eq!(recovered.len(), index.len());
        let apple_ids = recovered
            .query_with(&"apple".to_string(), |ids| Some(ids.clone()))
            .expect("apple must be present after recovery");
        assert_eq!(apple_ids.len() as u64, doc_id - 1);
        assert!(
            recovered
                .query_with(&"banana".to_string(), |ids| Some(ids.clone()))
                .is_some()
        );
    }

    /// After the manifest commit, the replaced objects are garbage. A crash
    /// (or plain failure) before they are deleted must not affect reloads:
    /// the manifest never references them.
    #[tokio::test]
    async fn test_reload_unaffected_when_obsolete_deletion_fails() {
        let index = create_populated_index();
        let mut store = MemStore::default();
        flush_to(&index, &mut store, now_ms()).await;

        // Mutate and flush again, but "crash" between the manifest commit
        // and the cleanup: keep every obsolete object in the store.
        index.insert(100, "fig".to_string(), now_ms()).unwrap();
        let outcome;
        let mut meta_buf: Vec<u8> = Vec::new();
        {
            let buckets = &mut store.buckets;
            outcome = index
                .flush_owned_with(
                    now_ms(),
                    |data| {
                        meta_buf = data;
                        std::future::ready(Ok(()))
                    },
                    |object, data| {
                        buckets.insert(object, data);
                        std::future::ready(Ok(()))
                    },
                )
                .await
                .unwrap();
        }
        assert!(outcome.saved);
        assert!(
            !outcome.obsolete.is_empty(),
            "the rewritten bucket's previous object must be reported obsolete"
        );
        for object in &outcome.obsolete {
            assert!(
                store.buckets.contains_key(object),
                "test setup: obsolete {object:?} must still exist in the store"
            );
        }
        store.metadata = meta_buf;

        // Reload with the leaked garbage still present: invisible.
        let loaded: BTreeIndex<u64, String> = load_from(&store).await;
        assert_eq!(loaded.len(), index.len());
        let ids = loaded
            .query_with(&"fig".to_string(), |ids| Some(ids.clone()))
            .expect("fig must be present");
        assert_eq!(ids, vec![100]);
        assert!(
            !loaded.has_dirty_buckets(),
            "leaked garbage must not dirty anything on load"
        );
    }

    /// Data persisted by a pre-manifest release (metadata without a manifest,
    /// un-suffixed bucket objects) loads correctly, and the first flush
    /// upgrades the durable layout to the manifest format while retiring the
    /// rewritten legacy objects.
    #[tokio::test]
    async fn test_legacy_format_loads_and_upgrades_on_first_flush() {
        let config = BTreeConfig {
            bucket_overload_size: 80,
            allow_duplicates: true,
        };
        let index = BTreeIndex::new("legacy_upgrade".to_string(), Some(config));
        for (id, key) in [
            (1u64, "apple"),
            (2, "banana"),
            (3, "cherry"),
            (4, "date"),
            (5, "eggplant"),
            (6, "fig"),
            (7, "grape"),
        ] {
            index.insert(id, key.to_string(), now_ms()).unwrap();
        }
        let mut store = MemStore::default();
        flush_to(&index, &mut store, now_ms()).await;
        assert!(
            store.buckets.len() > 1,
            "scenario must span several buckets"
        );

        // Transform the store into the legacy layout: strip the manifest from
        // the metadata and re-key every bucket object to generation 0.
        let mut legacy_meta = index.metadata();
        legacy_meta.buckets = BTreeMap::new();
        let mut metadata_buf = Vec::new();
        cbor2::to_writer(
            &BTreeIndexRef {
                metadata: &legacy_meta,
            },
            &mut metadata_buf,
        )
        .unwrap();
        let legacy_store = MemStore {
            metadata: metadata_buf,
            buckets: store
                .buckets
                .iter()
                .map(|(object, data)| {
                    (
                        BucketObject {
                            bucket_id: object.bucket_id,
                            generation: 0,
                        },
                        data.clone(),
                    )
                })
                .collect(),
        };

        // Legacy data loads through the bucket-id-scan path.
        let loaded: BTreeIndex<u64, String> = load_from(&legacy_store).await;
        assert_eq!(loaded.len(), index.len());
        for (id, key) in [(1u64, "apple"), (7, "grape")] {
            let ids = loaded
                .query_with(&key.to_string(), |ids| Some(ids.clone()))
                .unwrap_or_else(|| panic!("{key} must load from the legacy layout"));
            assert!(ids.contains(&id));
        }

        // Mutate and flush: the durable metadata upgrades to the manifest
        // format; rewritten legacy objects are reported obsolete.
        loaded.insert(8, "honeydew".to_string(), now_ms()).unwrap();
        let mut upgraded = legacy_store.clone();
        let outcome = flush_to(&loaded, &mut upgraded, now_ms()).await;
        assert!(outcome.saved);
        for object in &outcome.obsolete {
            assert_eq!(
                object.generation, 0,
                "only replaced legacy objects may be obsolete here"
            );
        }

        let upgraded_meta = BTreeIndex::<u64, String>::load_metadata(&upgraded.metadata[..])
            .unwrap()
            .metadata();
        assert!(
            !upgraded_meta.buckets.is_empty(),
            "the first flush must commit a manifest"
        );

        let reloaded: BTreeIndex<u64, String> = load_from(&upgraded).await;
        assert_eq!(reloaded.len(), 8);
        for key in ["apple", "grape", "honeydew"] {
            assert!(
                reloaded
                    .query_with(&key.to_string(), |ids| Some(ids.clone()))
                    .is_some(),
                "{key} must be present after the format upgrade"
            );
        }
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
            buckets: BTreeMap::new(),
        };

        let owned = BTreeIndexOwned { metadata: meta };
        let mut buf = Vec::new();
        cbor2::to_writer(&owned, &mut buf).unwrap();

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

    #[test]
    fn test_insert_array_grows_bucket_size_for_existing_postings() {
        // Regression test: insert_array must accumulate the per-doc_id size delta
        // for existing postings into the bucket size; otherwise buckets silently
        // exceed bucket_overload_size and never split.
        let config = BTreeConfig {
            bucket_overload_size: 1024,
            allow_duplicates: true,
        };
        let index: BTreeIndex<u64, String> =
            BTreeIndex::new("size_growth".to_string(), Some(config));

        // Seed three field values into bucket 0.
        index
            .insert_array(
                1,
                vec!["a".to_string(), "b".to_string(), "c".to_string()],
                now_ms(),
            )
            .unwrap();
        let initial_size = index.buckets.get(&0).unwrap().0;
        assert!(initial_size > 0);

        // Add many additional doc_ids to all three EXISTING postings via insert_array.
        for doc_id in 2u64..50 {
            index
                .insert_array(
                    doc_id,
                    vec!["a".to_string(), "b".to_string(), "c".to_string()],
                    now_ms(),
                )
                .unwrap();
        }

        let grown_size = index.buckets.get(&0).unwrap().0;
        // Bucket size must reflect the new doc_ids, not stay flat.
        assert!(
            grown_size > initial_size,
            "bucket size should grow when doc_ids are appended via insert_array \
             (initial={initial_size}, after={grown_size})"
        );
    }

    #[test]
    fn test_insert_migration_subtracts_previous_posting_size_from_source_bucket() {
        let config = BTreeConfig {
            bucket_overload_size: 128,
            allow_duplicates: true,
        };
        let index: BTreeIndex<u64, String> =
            BTreeIndex::new("single_insert_migration_size".to_string(), Some(config));

        index.insert(1, "anchor".to_string(), now_ms()).unwrap();
        for doc_id in 1u64..=23 {
            index
                .insert(doc_id, "moving".to_string(), now_ms())
                .unwrap();
        }

        let moving_key = "moving".to_string();
        let previous_posting_size = {
            let posting = index.postings.get(&moving_key).unwrap();
            posting_entry_size(&moving_key, &*posting)
        };

        let forced_source_size = {
            let mut bucket = index.buckets.get_mut(&0).unwrap();
            bucket.0 = index.config.bucket_overload_size - 1;
            bucket.0
        };

        index.insert(24, moving_key.clone(), now_ms()).unwrap();

        let moved_posting = index.postings.get(&moving_key).unwrap();
        assert_ne!(moved_posting.0, 0, "posting should migrate to a new bucket");

        let source_bucket = index.buckets.get(&0).unwrap();
        assert_eq!(
            source_bucket.0,
            forced_source_size.saturating_sub(previous_posting_size),
            "source bucket must reclaim the migrated posting's pre-insert size"
        );
        assert!(!source_bucket.2.contains(&moving_key));
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
        let mut store = MemStore::default();
        flush_to(&index, &mut store, 1).await;
        let objects_before: Vec<BucketObject> = store.buckets.keys().copied().collect();

        // Reload with a large bucket limit
        let mut loaded: BTreeIndex<u64, String> =
            BTreeIndex::load_metadata(&store.metadata[..]).unwrap();
        loaded.config.bucket_overload_size = 1024 * 512;
        loaded.metadata.write().config.bucket_overload_size = 1024 * 512;
        loaded
            .load_buckets(async |object| Ok(store.buckets.get(&object).cloned()))
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

        // Verify flush + reload works; every pre-compaction object is
        // replaced or dropped, and the manifest commit retires it.
        let mut store2 = store.clone();
        let outcome = flush_to(&loaded, &mut store2, 100).await;
        assert!(outcome.saved);
        for object in &objects_before {
            assert!(
                outcome.obsolete.contains(object),
                "pre-compaction {object:?} must be reported obsolete"
            );
            assert!(
                !store2.buckets.contains_key(object),
                "pre-compaction {object:?} must be deleted from the store"
            );
        }

        let final_loaded: BTreeIndex<u64, String> = load_from(&store2).await;

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

    #[tokio::test]
    async fn test_load_metadata_and_bucket_error_paths() {
        match BTreeIndex::<u64, String>::load_metadata(&b"not cbor"[..]) {
            Err(BTreeError::Serialization { .. }) => {}
            Err(other) => panic!("expected metadata serialization error, got {other:?}"),
            Ok(_) => panic!("expected metadata serialization error"),
        }

        let metadata = BTreeMetadata {
            name: "load_errors".to_string(),
            config: BTreeConfig::default(),
            stats: BTreeStats {
                version: 7,
                max_bucket_id: 0,
                query_count: 3,
                ..Default::default()
            },
            buckets: BTreeMap::new(),
        };
        let mut metadata_buf = Vec::new();
        cbor2::to_writer(
            &BTreeIndexRef {
                metadata: &metadata,
            },
            &mut metadata_buf,
        )
        .unwrap();

        let mut generic_error_index: BTreeIndex<u64, String> =
            BTreeIndex::load_metadata(&metadata_buf[..]).unwrap();
        assert_eq!(generic_error_index.stats().query_count, 3);
        let err = generic_error_index
            .load_buckets(async |_| Err::<Option<Vec<u8>>, _>("bucket load failed".into()))
            .await
            .unwrap_err();
        assert!(matches!(err, BTreeError::Generic { .. }));

        let mut serialization_error_index: BTreeIndex<u64, String> =
            BTreeIndex::load_metadata(&metadata_buf[..]).unwrap();
        let err = serialization_error_index
            .load_buckets(async |_| Ok(Some(b"not a bucket".to_vec())))
            .await
            .unwrap_err();
        assert!(matches!(err, BTreeError::Serialization { .. }));
    }

    #[tokio::test]
    async fn test_load_buckets_reconciles_duplicate_postings_from_newer_bucket() {
        let metadata = BTreeMetadata {
            name: "duplicate_load".to_string(),
            config: BTreeConfig {
                bucket_overload_size: 256,
                allow_duplicates: true,
            },
            stats: BTreeStats {
                version: 4,
                max_bucket_id: 1,
                ..Default::default()
            },
            buckets: BTreeMap::new(),
        };
        let mut metadata_buf = Vec::new();
        cbor2::to_writer(
            &BTreeIndexRef {
                metadata: &metadata,
            },
            &mut metadata_buf,
        )
        .unwrap();

        let mut old_postings = FxHashMap::default();
        old_postings.insert("same".to_string(), (0, 1, vec![1_u64].into()));
        let mut old_bucket = Vec::new();
        cbor2::to_writer(
            &BucketOwned {
                postings: old_postings,
            },
            &mut old_bucket,
        )
        .unwrap();

        let mut new_postings = FxHashMap::default();
        new_postings.insert("same".to_string(), (1, 2, vec![2_u64].into()));
        let mut new_bucket = Vec::new();
        cbor2::to_writer(
            &BucketOwned {
                postings: new_postings,
            },
            &mut new_bucket,
        )
        .unwrap();

        let mut loaded: BTreeIndex<u64, String> =
            BTreeIndex::load_metadata(&metadata_buf[..]).unwrap();
        loaded
            .load_buckets(async |object| {
                Ok(match object.bucket_id {
                    0 => Some(old_bucket.clone()),
                    1 => Some(new_bucket.clone()),
                    _ => None,
                })
            })
            .await
            .unwrap();

        assert_eq!(
            loaded.query_with(&"same".to_string(), |ids| Some(ids.clone())),
            Some(vec![2])
        );
        assert_eq!(loaded.postings.get("same").unwrap().0, 1);
        let old_bucket = loaded.buckets.get(&0).unwrap();
        assert!(old_bucket.1, "stale source bucket should be marked dirty");
        assert!(!old_bucket.2.contains(&"same".to_string()));
        assert!(loaded.has_dirty_buckets());
    }

    #[tokio::test]
    async fn test_flush_error_and_noop_paths() {
        // A failing metadata writer surfaces as an error and commits nothing.
        let index = create_test_index();
        let err = index
            .flush(FailingWriter, now_ms(), |_, _| std::future::ready(Ok(())))
            .await
            .unwrap_err();
        assert!(matches!(err, BTreeError::Generic { .. }));
        assert!(index.has_pending_metadata_flush());

        // A fresh index flushes once, then reports nothing to do.
        let fresh = create_test_index();
        assert!(
            fresh
                .flush(Vec::new(), now_ms(), |_, _| std::future::ready(Ok(())))
                .await
                .unwrap()
                .saved
        );
        assert!(
            !fresh
                .flush(Vec::new(), now_ms(), |_, _| std::future::ready(Ok(())))
                .await
                .unwrap()
                .saved
        );

        // A failing bucket writer keeps everything dirty and retryable.
        let dirty = create_test_index();
        dirty.insert(1, "apple".to_string(), now_ms()).unwrap();
        let err = dirty
            .flush(Vec::new(), now_ms(), |_, _| {
                std::future::ready(Err::<(), BoxError>("write failed".into()))
            })
            .await
            .unwrap_err();
        assert!(matches!(err, BTreeError::Generic { .. }));
        assert!(dirty.has_dirty_buckets());
    }

    #[test]
    fn test_compact_buckets_repairs_empty_multi_bucket_index() {
        let index: BTreeIndex<u64, String> =
            BTreeIndex::new("empty_compact".to_string(), Some(BTreeConfig::default()));
        index.buckets.insert(1, (0, false, UniqueVec::default(), 0));
        index.max_bucket_id.store(1, Ordering::Relaxed);
        assert_eq!(index.compact_buckets(), (2, 1));
        assert_eq!(index.stats().max_bucket_id, 0);
        assert!(index.has_dirty_buckets());
        assert_eq!(index.buckets.get(&0).unwrap().3, 1);
    }

    #[test]
    fn test_prefix_query_includes_keys_containing_char_max() {
        // Regression test: the old implementation used "prefix + char::MAX" as
        // an inclusive upper bound, which missed keys like "app\u{10FFFF}x".
        let index = create_test_index();

        let plain = "app".to_string();
        let with_max = format!("app{}", char::MAX);
        let with_max_suffix = format!("app{}x", char::MAX);

        index.insert(1, plain.clone(), now_ms()).unwrap();
        index.insert(2, with_max.clone(), now_ms()).unwrap();
        index.insert(3, with_max_suffix.clone(), now_ms()).unwrap();
        index.insert(4, "apz".to_string(), now_ms()).unwrap();

        let results = index.prefix_query_with("app", |k, _| (true, Some(k.to_string())));
        assert_eq!(
            results,
            vec![plain, with_max, with_max_suffix],
            "prefix query must cover every key starting with the prefix"
        );
    }

    #[test]
    fn test_load_metadata_caps_corrupted_num_elements_preallocation() {
        // A corrupted/hostile num_elements must not drive a giant
        // pre-allocation (or a capacity-overflow panic) on load.
        let metadata = BTreeMetadata {
            name: "huge".to_string(),
            config: BTreeConfig::default(),
            stats: BTreeStats {
                version: 1,
                num_elements: u64::MAX,
                ..Default::default()
            },
            buckets: BTreeMap::new(),
        };
        let mut buf = Vec::new();
        cbor2::to_writer(
            &BTreeIndexRef {
                metadata: &metadata,
            },
            &mut buf,
        )
        .unwrap();

        let index = BTreeIndex::<u64, String>::load_metadata(&buf[..]).unwrap();
        assert_eq!(index.len(), 0);
        index.insert(1, "a".to_string(), now_ms()).unwrap();
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn test_chaos_concurrent_insert_remove_no_phantom_btree_keys() {
        // Regression test for the phantom-btree-key race: an insert that
        // creates a posting races with a remove that deletes the posting
        // before the insert has added the key to the btree; the stale key
        // must not survive in the btree.
        //
        // Plain OS threads (not tokio tasks): the remover busy-spins to stay
        // temporally adjacent to the inserter, and a busy-spinning tokio task
        // would starve the inserter task via the worker's non-stealable LIFO
        // slot, while OS threads are preempted fairly.
        for _ in 0..5 {
            let index = BTreeIndex::<u64, String>::new(
                "phantom_chaos".to_string(),
                Some(BTreeConfig {
                    bucket_overload_size: 4096,
                    allow_duplicates: true,
                }),
            );

            let n_keys = 3000u64;
            let barrier = std::sync::Barrier::new(2);

            std::thread::scope(|s| {
                // Inserter: each (doc_id, key) pair is inserted exactly once.
                s.spawn(|| {
                    barrier.wait();
                    for i in 0..n_keys {
                        index.insert(i, format!("k{i}"), now_ms()).unwrap();
                    }
                });

                // Remover: spins until each pair is actually removed, staying
                // right on the inserter's heels so the remove lands inside
                // insert's create-posting → add-btree-key window as often as
                // possible. Every key therefore ends fully removed.
                s.spawn(|| {
                    barrier.wait();
                    for i in 0..n_keys {
                        let key = format!("k{i}");
                        let mut spins = 0u64;
                        while !index.remove(i, key.clone(), now_ms()) {
                            spins += 1;
                            assert!(spins < 100_000_000, "remover starved at ({i}, {key})");
                            if spins.is_multiple_of(64) {
                                // Guarantee inserter progress even on a
                                // single-core machine.
                                std::thread::yield_now();
                            } else {
                                std::hint::spin_loop();
                            }
                        }
                    }
                });
            });

            // Every pair was inserted once and removed once, so both postings
            // and btree must be empty; any leftover btree key is a phantom.
            assert_eq!(index.len(), 0);
            assert_eq!(
                index.keys(None, None),
                Vec::<String>::new(),
                "phantom btree keys without postings survived"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_chaos_unique_insert_array_keeps_bookkeeping_consistent() {
        // Regression test: when a unique-index insert_array hits a concurrent
        // uniqueness conflict mid-loop, the values inserted before the conflict
        // must keep consistent bookkeeping (btree keys + bucket tracking) so
        // they survive flush + reload.
        for _ in 0..10 {
            let index = Arc::new(BTreeIndex::<u64, String>::new(
                "unique_chaos".to_string(),
                Some(BTreeConfig {
                    bucket_overload_size: 256,
                    allow_duplicates: false,
                }),
            ));
            let keys: Vec<String> = (0..50).map(|i| format!("k_{i:02}")).collect();
            let barrier = Arc::new(Barrier::new(2));
            let mut handles = Vec::new();
            for doc_id in 1..=2u64 {
                let index = index.clone();
                let keys = keys.clone();
                let b = barrier.clone();
                handles.push(tokio::spawn(async move {
                    b.wait().await;
                    // One of the two calls may fail with AlreadyExists; the
                    // applied prefix must still be consistent.
                    let _ = index.insert_array(doc_id, keys, now_ms());
                }));
            }
            futures::future::try_join_all(handles).await.unwrap();

            let btree_keys = index.keys(None, None);
            for key in &btree_keys {
                assert!(index.postings.contains_key(key));
            }
            assert_eq!(
                btree_keys.len(),
                index.postings.len(),
                "every applied posting must have a btree key"
            );

            let mut store = MemStore::default();
            flush_to(&index, &mut store, now_ms()).await;

            let loaded: BTreeIndex<u64, String> = load_from(&store).await;
            assert_eq!(
                loaded.len(),
                index.len(),
                "postings lost across flush + reload"
            );
            for key in &btree_keys {
                let original = index.query_with(key, |ids| Some(ids.clone()));
                let reloaded = loaded.query_with(key, |ids| Some(ids.clone()));
                assert_eq!(original, reloaded, "posting for {key} differs after reload");
            }
        }
    }

    #[tokio::test]
    async fn test_load_buckets_skips_empty_posting_ghost_key() {
        // Regression test: a crash between "remove() empties the posting" and
        // the next repairing flush can persist a bucket containing an empty
        // posting. Loading it must not register a "ghost" key.
        let metadata = BTreeMetadata {
            name: "ghost".to_string(),
            config: BTreeConfig {
                bucket_overload_size: 256,
                allow_duplicates: true,
            },
            stats: BTreeStats {
                version: 3,
                max_bucket_id: 0,
                ..Default::default()
            },
            buckets: BTreeMap::new(),
        };
        let mut metadata_buf = Vec::new();
        cbor2::to_writer(
            &BTreeIndexRef {
                metadata: &metadata,
            },
            &mut metadata_buf,
        )
        .unwrap();

        let mut postings = FxHashMap::default();
        postings.insert("alive".to_string(), (0u32, 1u64, vec![1u64].into()));
        postings.insert(
            "ghost".to_string(),
            (0u32, 2u64, UniqueVec::<u64>::default()),
        );
        let mut bucket_buf = Vec::new();
        cbor2::to_writer(&BucketOwned { postings }, &mut bucket_buf).unwrap();

        let mut loaded: BTreeIndex<u64, String> =
            BTreeIndex::load_metadata(&metadata_buf[..]).unwrap();
        loaded
            .load_buckets(async |object| {
                Ok(if object.bucket_id == 0 {
                    Some(bucket_buf.clone())
                } else {
                    None
                })
            })
            .await
            .unwrap();

        assert_eq!(loaded.len(), 1, "empty posting must not count as a key");
        assert_eq!(loaded.keys(None, None), vec!["alive".to_string()]);
        assert_eq!(
            loaded.query_with(&"ghost".to_string(), |ids| Some(ids.clone())),
            None
        );
        let not_alive: Vec<String> = loaded.range_query_with(
            RangeQuery::Not(Box::new(RangeQuery::Eq("alive".to_string()))),
            |k, _| (true, vec![k.clone()]),
        );
        assert!(
            not_alive.is_empty(),
            "Not query must not surface the ghost key"
        );
        assert!(
            loaded.has_dirty_buckets(),
            "bucket containing the ghost must be loaded dirty to self-heal"
        );

        // The self-heal flush rewrites bucket 0 without the ghost posting.
        let mut repaired: HashMap<u32, Vec<u8>> = Default::default();
        {
            let repaired = &mut repaired;
            loaded
                .flush(Vec::new(), now_ms(), |object, data| {
                    repaired.insert(object.bucket_id, data);
                    std::future::ready(Ok(()))
                })
                .await
                .unwrap();
        }
        let bucket: BucketOwned<u64, String> =
            cbor2::from_reader(&repaired.get(&0).unwrap()[..]).unwrap();
        assert!(bucket.postings.contains_key("alive"));
        assert!(
            !bucket.postings.contains_key("ghost"),
            "repaired bucket must not contain the empty posting"
        );
        assert!(!loaded.has_dirty_buckets());
    }

    #[tokio::test]
    async fn test_load_buckets_empty_posting_tombstones_stale_lower_bucket_copy() {
        // A migrated posting that was emptied and sampled into the higher
        // bucket acts as a tombstone: it must also drop the stale non-empty
        // copy loaded from the lower (older) bucket.
        let metadata = BTreeMetadata {
            name: "tombstone".to_string(),
            config: BTreeConfig {
                bucket_overload_size: 256,
                allow_duplicates: true,
            },
            stats: BTreeStats {
                version: 5,
                max_bucket_id: 1,
                ..Default::default()
            },
            buckets: BTreeMap::new(),
        };
        let mut metadata_buf = Vec::new();
        cbor2::to_writer(
            &BTreeIndexRef {
                metadata: &metadata,
            },
            &mut metadata_buf,
        )
        .unwrap();

        let mut old_postings = FxHashMap::default();
        old_postings.insert("same".to_string(), (0u32, 1u64, vec![1u64].into()));
        let mut old_bucket = Vec::new();
        cbor2::to_writer(
            &BucketOwned {
                postings: old_postings,
            },
            &mut old_bucket,
        )
        .unwrap();

        let mut new_postings = FxHashMap::default();
        new_postings.insert(
            "same".to_string(),
            (1u32, 2u64, UniqueVec::<u64>::default()),
        );
        let mut new_bucket = Vec::new();
        cbor2::to_writer(
            &BucketOwned {
                postings: new_postings,
            },
            &mut new_bucket,
        )
        .unwrap();

        let mut loaded: BTreeIndex<u64, String> =
            BTreeIndex::load_metadata(&metadata_buf[..]).unwrap();
        loaded
            .load_buckets(async |object| {
                Ok(match object.bucket_id {
                    0 => Some(old_bucket.clone()),
                    1 => Some(new_bucket.clone()),
                    _ => None,
                })
            })
            .await
            .unwrap();

        assert_eq!(loaded.len(), 0, "tombstoned key must not survive reload");
        assert!(loaded.keys(None, None).is_empty());
        assert_eq!(
            loaded.query_with(&"same".to_string(), |ids| Some(ids.clone())),
            None
        );
        let stale_bucket = loaded.buckets.get(&0).unwrap();
        assert!(stale_bucket.1, "stale lower bucket must be marked dirty");
        assert!(!stale_bucket.2.contains(&"same".to_string()));
        drop(stale_bucket);
        assert!(loaded.has_dirty_buckets());
    }

    #[tokio::test]
    async fn test_flush_filters_transiently_empty_posting() {
        // A posting that is empty at serialization time (e.g. loaded legacy
        // data mid-repair) must not be persisted: an empty posting on disk
        // would resurrect as a ghost key.
        let index = create_test_index();
        index.insert(1, "a".to_string(), now_ms()).unwrap();
        index.insert(2, "b".to_string(), now_ms()).unwrap();
        {
            let mut posting = index.postings.get_mut(&"a".to_string()).unwrap();
            posting.2.swap_remove_if(|id| *id == 1);
            posting.1 += 1;
        }

        let mut written: HashMap<u32, Vec<u8>> = Default::default();
        {
            let written = &mut written;
            index
                .flush(Vec::new(), now_ms(), |object, data| {
                    written.insert(object.bucket_id, data);
                    std::future::ready(Ok(()))
                })
                .await
                .unwrap();
        }

        let bucket: BucketOwned<u64, String> =
            cbor2::from_reader(&written.get(&0).unwrap()[..]).unwrap();
        assert!(
            !bucket.postings.contains_key("a"),
            "transiently empty posting must not be persisted"
        );
        assert!(bucket.postings.contains_key("b"));
    }

    /// A posting owned by bucket N must be written **only** into bucket N's
    /// object, even while a higher-numbered bucket still lists its field value
    /// (a leftover of a migration or of compaction). Persisting it into both
    /// makes the stale copy win on reload — `load_buckets` forces
    /// `posting.0 = i` walking buckets in ascending id order — which
    /// resurrects the doc set that was current when the non-owning bucket was
    /// last written and drops everything appended to the posting since.
    #[tokio::test]
    async fn test_flush_skips_posting_listed_by_a_non_owning_bucket() {
        let index = create_test_index();
        let mut store = MemStore::default();
        let apple = "apple".to_string();
        let banana = "banana".to_string();

        // Bucket 0 owns "apple", bucket 1 owns "banana".
        index.insert(1, apple.clone(), now_ms()).unwrap();
        index.max_bucket_id.store(1, Ordering::Relaxed);
        index.insert(10, banana.clone(), now_ms()).unwrap();
        assert_eq!(index.postings.get(&apple).unwrap().0, 0);
        assert_eq!(index.postings.get(&banana).unwrap().0, 1);

        // Corrupt the packing metadata the way an interrupted migration can:
        // bucket 1 also lists "apple", whose posting bucket 0 owns.
        index.buckets.get_mut(&1).unwrap().2.push(apple.clone());

        flush_to(&index, &mut store, now_ms()).await;
        assert!(!index.has_dirty_buckets());

        // Appending to the posting dirties its owner only, so the stale
        // bucket 1 object stays pinned at the generation committed above.
        index.insert(2, apple.clone(), now_ms()).unwrap();
        assert!(index.buckets.get(&0).unwrap().1, "owner must be dirty");
        assert!(
            !index.buckets.get(&1).unwrap().1,
            "the non-owning bucket must not be rewritten"
        );
        flush_to(&index, &mut store, now_ms()).await;

        // The non-owning bucket's object must not carry the foreign posting.
        let generation = *index.metadata().buckets.get(&1).unwrap();
        let bucket1: BucketOwned<u64, String> = cbor2::from_reader(
            &store.buckets[&BucketObject {
                bucket_id: 1,
                generation,
            }][..],
        )
        .unwrap();
        assert!(
            !bucket1.postings.contains_key(&apple),
            "a bucket must never persist a posting owned by another bucket"
        );
        assert!(bucket1.postings.contains_key(&banana));

        let loaded: BTreeIndex<u64, String> = load_from(&store).await;
        assert_eq!(
            loaded.query_with(&apple, |ids| Some(ids.clone())),
            Some(vec![1, 2]),
            "the stale copy in the higher-numbered bucket must not win on load"
        );
        assert_eq!(loaded.postings.get(&apple).unwrap().0, 0);
    }

    #[test]
    fn test_compact_buckets_restores_ownership_invariants() {
        let index = create_test_index();
        for i in 0..200u64 {
            index.insert(i, format!("key-{i:03}"), now_ms()).unwrap();
        }
        // Fragment the buckets before compacting.
        for i in (0..200u64).step_by(2) {
            assert!(index.remove(i, format!("key-{i:03}"), now_ms()));
        }

        let (old_count, new_count) = index.compact_buckets();
        assert!(new_count <= old_count);
        assert_eq!(
            index.stats().max_bucket_id as usize,
            new_count - 1,
            "max_bucket_id must match the compacted bucket range"
        );
        assert_eq!(index.buckets.len(), new_count);

        // Every bucket's tracked field values point back at that bucket, and
        // every posting is tracked by exactly one bucket.
        let mut tracked = 0usize;
        for bucket in index.buckets.iter() {
            let id = *bucket.key();
            assert!(
                (id as usize) < new_count,
                "bucket id beyond compacted range"
            );
            assert!(bucket.1, "compacted buckets must be dirty for persistence");
            for fv in bucket.2.iter() {
                let posting = index
                    .postings
                    .get(fv)
                    .expect("bucket must track only live postings");
                assert_eq!(posting.0, id, "posting bucket id must match owner");
            }
            tracked += bucket.2.len();
        }
        assert_eq!(
            tracked,
            index.postings.len(),
            "every posting must be tracked by exactly one bucket"
        );
    }

    /// Compaction must exclude mutations, not merely be documented as
    /// requiring the caller to do so: it holds the mutation gate exclusively
    /// while every mutator holds it shared.
    #[test]
    fn test_compaction_excludes_mutations() {
        let index = Arc::new(BTreeIndex::new(
            "compact_exclusion".to_string(),
            Some(BTreeConfig {
                bucket_overload_size: BTreeConfig::MIN_BUCKET_OVERLOAD_SIZE,
                allow_duplicates: true,
            }),
        ));
        for id in 0..8_u64 {
            index
                .insert(id, format!("key-{id}-{}", "x".repeat(96)), now_ms())
                .unwrap();
        }
        assert!(index.buckets.len() > 1);

        let mutation_in_progress = index.mutation_gate.read();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let compact_index = index.clone();
        let compact = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let result = compact_index.compact_buckets();
            done_tx.send(result).unwrap();
        });

        started_rx.recv().unwrap();
        assert!(
            done_rx
                .recv_timeout(std::time::Duration::from_millis(50))
                .is_err(),
            "compaction ran while a mutation held the shared gate"
        );
        drop(mutation_in_progress);
        let (old_count, new_count) = done_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        compact.join().unwrap();
        assert!(new_count <= old_count);
    }

    /// Regression: `compact_buckets` snapshots `postings`, clears `buckets`
    /// and re-bins the snapshot. A posting created by a concurrent `insert`
    /// after the snapshot ended up in no bucket at all — and only bucket
    /// contents are serialized — so `insert` returned `Ok` while the value
    /// silently vanished from the durable index on the next flush.
    #[tokio::test]
    async fn test_compaction_never_loses_concurrent_inserts() {
        let index = Arc::new(BTreeIndex::<u64, String>::new(
            "compact_concurrent_insert".to_string(),
            Some(BTreeConfig {
                bucket_overload_size: BTreeConfig::MIN_BUCKET_OVERLOAD_SIZE,
                allow_duplicates: true,
            }),
        ));
        // Seed enough postings that each compaction has real work to do.
        for id in 0..64u64 {
            index.insert(id, format!("seed-{id:04}"), now_ms()).unwrap();
        }

        const WRITES: u64 = 400;
        let writer_index = index.clone();
        let writer = std::thread::spawn(move || {
            for id in 0..WRITES {
                writer_index
                    .insert(1_000 + id, format!("live-{id:04}"), now_ms())
                    .unwrap();
            }
        });
        let mut compactions = 0usize;
        while !writer.is_finished() {
            index.compact_buckets();
            compactions += 1;
        }
        writer.join().unwrap();
        assert!(compactions > 0, "no compaction overlapped the writer");

        // A flush persists bucket contents only, so a posting that no bucket
        // lists disappears on reload even though `insert` reported success.
        let mut store = MemStore::default();
        flush_to(&index, &mut store, now_ms()).await;
        let loaded: BTreeIndex<u64, String> = load_from(&store).await;
        let missing: Vec<String> = (0..WRITES)
            .map(|id| format!("live-{id:04}"))
            .filter(|key| loaded.query_with(key, |ids| Some(ids.clone())).is_none())
            .collect();
        assert!(
            missing.is_empty(),
            "{} concurrently inserted values were lost, e.g. {:?}",
            missing.len(),
            &missing[..missing.len().min(5)]
        );
        assert_eq!(loaded.len(), index.len());
    }

    #[test]
    fn test_range_query_depth_cap() {
        let max_depth = RangeQuery::<String>::MAX_DEPTH;

        assert_eq!(RangeQuery::Eq("a".to_string()).depth(), 1);
        assert_eq!(
            RangeQuery::Not(Box::new(RangeQuery::Eq("a".to_string()))).depth(),
            2
        );
        assert_eq!(
            RangeQuery::And(vec![
                Box::new(RangeQuery::Eq("a".to_string())),
                Box::new(RangeQuery::Not(Box::new(RangeQuery::Gt("b".to_string())))),
            ])
            .depth(),
            3
        );

        let index = create_populated_index();

        // Exactly at the cap: still evaluated (odd number of Nots ->
        // complement of Eq("apple")).
        let mut at_cap = RangeQuery::Eq("apple".to_string());
        for _ in 0..(max_depth - 1) {
            at_cap = RangeQuery::Not(Box::new(at_cap));
        }
        assert_eq!(at_cap.depth(), max_depth);
        let keys: Vec<String> = index.range_query_with(at_cap, |k, _| (true, vec![k.clone()]));
        assert_eq!(
            keys,
            vec![
                "banana".to_string(),
                "cherry".to_string(),
                "date".to_string(),
                "eggplant".to_string(),
            ]
        );

        // Far over the cap: rejected with an empty result instead of
        // recursing 4000+ frames deep.
        let mut over = RangeQuery::Eq("apple".to_string());
        for _ in 0..4096 {
            over = RangeQuery::Not(Box::new(over));
        }
        assert_eq!(over.depth(), 4097);
        let keys: Vec<String> = index.range_query_with(over, |k, _| (true, vec![k.clone()]));
        assert!(keys.is_empty(), "over-deep query must be rejected");

        // try_convert_from rejects over-deep queries up-front.
        let mut deep = RangeQuery::Eq("ok".to_string());
        for _ in 0..max_depth {
            deep = RangeQuery::Not(Box::new(deep));
        }
        let err = RangeQuery::<TestKey>::try_convert_from(deep).unwrap_err();
        assert!(err.to_string().contains("depth"), "unexpected error: {err}");

        let shallow = RangeQuery::Not(Box::new(RangeQuery::Eq("ok".to_string())));
        assert!(RangeQuery::<TestKey>::try_convert_from(shallow).is_ok());
    }

    #[test]
    fn test_range_query_depth_cap_logs_warning() {
        struct CaptureLogger;
        static CAPTURED: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
        impl log::Log for CaptureLogger {
            fn enabled(&self, metadata: &log::Metadata) -> bool {
                metadata.level() <= log::Level::Warn
            }
            fn log(&self, record: &log::Record) {
                if self.enabled(record.metadata()) {
                    CAPTURED.lock().unwrap().push(record.args().to_string());
                }
            }
            fn flush(&self) {}
        }
        static LOGGER: CaptureLogger = CaptureLogger;
        // No other test in this binary installs a logger; ignore the error
        // anyway so this test cannot fail on logger-installation racing.
        let _ = log::set_logger(&LOGGER);
        log::set_max_level(log::LevelFilter::Warn);

        let index = create_populated_index();
        let over_depth = RangeQuery::<String>::MAX_DEPTH + 1;
        let mut over = RangeQuery::Eq("apple".to_string());
        for _ in 0..over_depth {
            over = RangeQuery::Not(Box::new(over));
        }
        let keys: Vec<String> = index.range_query_with(over, |k, _| (true, vec![k.clone()]));
        assert!(keys.is_empty(), "over-deep query must be rejected");

        let captured = CAPTURED.lock().unwrap();
        assert!(
            captured.iter().any(|msg| {
                msg.contains("exceeds the maximum")
                    && msg.contains(index.name())
                    && msg.contains(&(over_depth + 1).to_string())
            }),
            "expected a depth-cap warning containing the index name and depth, got: {captured:?}"
        );
    }

    #[test]
    fn test_concurrent_appends_to_same_posting_with_migrations() {
        // Regression test for the removed debug_assert in
        // previous_posting_size_after_append: two threads appending to the
        // same field value while bucket migrations run means the popped doc
        // id is not necessarily this thread's; that used to panic in debug
        // builds.
        let index = BTreeIndex::<u64, String>::new(
            "hot_fv".to_string(),
            Some(BTreeConfig {
                bucket_overload_size: 128,
                allow_duplicates: true,
            }),
        );
        let n = 400u64;
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|s| {
            for t in 0..2u64 {
                let index = &index;
                let barrier = &barrier;
                s.spawn(move || {
                    barrier.wait();
                    for i in 0..n {
                        assert!(
                            index
                                .insert(t * n + i, "hot".to_string(), now_ms())
                                .unwrap()
                        );
                    }
                });
            }
        });

        let len = index
            .query_with(&"hot".to_string(), |ids| Some(ids.len()))
            .unwrap();
        assert_eq!(len, (2 * n) as usize);
    }

    #[tokio::test]
    async fn test_flush_metadata_commit_failure_then_retry() {
        let index = create_test_index();
        index.insert(1, "apple".to_string(), now_ms()).unwrap();

        let err = index
            .flush_owned_with(
                now_ms(),
                |_| std::future::ready(Err::<(), BoxError>("upload failed".into())),
                |_, _| std::future::ready(Ok(())),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, BTreeError::Generic { .. }));
        assert!(
            index.has_pending_metadata_flush(),
            "failed commit must keep the metadata version pending"
        );
        assert!(
            index.has_dirty_buckets(),
            "failed commit must keep buckets dirty"
        );

        let outcome = index
            .flush_owned_with(
                now_ms(),
                |data| {
                    assert!(!data.is_empty());
                    std::future::ready(Ok(()))
                },
                |_, _| std::future::ready(Ok(())),
            )
            .await
            .unwrap();
        assert!(
            outcome.saved,
            "retry after failure must persist a complete snapshot"
        );
        assert!(!index.has_pending_metadata_flush());
        assert!(!index.has_dirty_buckets());

        let again = index
            .flush_owned_with(
                now_ms(),
                |_| std::future::ready(Ok(())),
                |_, _| std::future::ready(Ok(())),
            )
            .await
            .unwrap();
        assert!(!again.saved, "already-saved state must be skipped");
    }

    /// Cancellation (the flush future is dropped at an await point) before
    /// the manifest commit must leave everything retryable.
    #[tokio::test]
    async fn test_cancelled_flush_before_commit_stays_retryable() {
        let index = Arc::new(create_test_index());
        index.insert(1, "apple".to_string(), now_ms()).unwrap();

        let entered = Arc::new(tokio::sync::Notify::new());
        let task_index = index.clone();
        let task_entered = entered.clone();
        let task = tokio::spawn(async move {
            task_index
                .flush_owned_with(
                    now_ms(),
                    move |_| {
                        let task_entered = task_entered.clone();
                        async move {
                            task_entered.notify_one();
                            std::future::pending::<Result<(), BoxError>>().await
                        }
                    },
                    |_, _| std::future::ready(Ok(())),
                )
                .await
        });
        entered.notified().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());

        assert!(index.has_pending_metadata_flush());
        assert!(index.has_dirty_buckets());
        let mut store = MemStore::default();
        assert!(flush_to(index.as_ref(), &mut store, now_ms()).await.saved);
        assert!(!index.has_pending_metadata_flush());
        assert!(!index.has_dirty_buckets());
        let loaded: BTreeIndex<u64, String> = load_from(&store).await;
        assert_eq!(
            loaded.query_with(&"apple".to_string(), |ids| Some(ids.clone())),
            Some(vec![1])
        );
    }

    #[tokio::test]
    async fn test_flush_owned_with_round_trip() {
        let index = create_test_index();
        index.insert(1, "apple".to_string(), now_ms()).unwrap();

        let mut store = MemStore::default();
        let outcome = flush_to(&index, &mut store, now_ms()).await;
        assert!(outcome.saved);

        let loaded: BTreeIndex<u64, String> = load_from(&store).await;
        assert_eq!(
            loaded.query_with(&"apple".to_string(), |ids| Some(ids.clone())),
            Some(vec![1])
        );

        let idle = index
            .flush_owned_with(
                now_ms(),
                |_| std::future::ready(Ok(())),
                |_, _| std::future::ready(Ok(())),
            )
            .await
            .unwrap();
        assert!(!idle.saved, "fully persisted index must short-circuit");
    }

    #[test]
    fn test_bucket_overload_size_is_clamped() {
        let index = BTreeIndex::<u64, String>::new(
            "clamped".to_string(),
            Some(BTreeConfig {
                bucket_overload_size: 0,
                allow_duplicates: true,
            }),
        );
        assert_eq!(
            index.metadata().config.bucket_overload_size,
            BTreeConfig::MIN_BUCKET_OVERLOAD_SIZE
        );

        // Persisted metadata carrying a degenerate value is clamped on load.
        let metadata = BTreeMetadata {
            name: "clamped_load".to_string(),
            config: BTreeConfig {
                bucket_overload_size: 1,
                allow_duplicates: true,
            },
            stats: BTreeStats {
                version: 1,
                ..Default::default()
            },
            buckets: BTreeMap::new(),
        };
        let mut buf = Vec::new();
        cbor2::to_writer(
            &BTreeIndexRef {
                metadata: &metadata,
            },
            &mut buf,
        )
        .unwrap();
        let loaded = BTreeIndex::<u64, String>::load_metadata(&buf[..]).unwrap();
        assert_eq!(
            loaded.metadata().config.bucket_overload_size,
            BTreeConfig::MIN_BUCKET_OVERLOAD_SIZE
        );
    }

    #[test]
    fn test_insert_rejects_unserializable_values_without_mutation() {
        // PK whose serialization fails: rejected before any mutation.
        let index = BTreeIndex::<Flaky, String>::new("bad_pk".to_string(), None);
        let err = index
            .insert(Flaky(1, true), "k".to_string(), now_ms())
            .unwrap_err();
        assert!(matches!(err, BTreeError::Serialization { .. }));
        assert_eq!(index.len(), 0);
        assert!(index.keys(None, None).is_empty());
        assert!(!index.has_dirty_buckets());
        assert!(
            index
                .insert(Flaky(1, false), "k".to_string(), now_ms())
                .unwrap()
        );

        // FV whose serialization fails: rejected before the posting exists.
        let index = BTreeIndex::<u64, Flaky>::new("bad_fv".to_string(), None);
        let err = index.insert(1, Flaky(2, true), now_ms()).unwrap_err();
        assert!(matches!(err, BTreeError::Serialization { .. }));
        assert_eq!(index.len(), 0);
        assert!(index.keys(None, None).is_empty());
        assert!(!index.has_dirty_buckets());
        assert!(index.insert(1, Flaky(2, false), now_ms()).unwrap());
    }

    #[test]
    fn test_insert_array_defers_serialization_error_and_keeps_applied_values() {
        let index = BTreeIndex::<u64, Flaky>::new("bad_fv_array".to_string(), None);
        let err = index
            .insert_array(
                1,
                vec![Flaky(1, false), Flaky(2, true), Flaky(3, false)],
                now_ms(),
            )
            .unwrap_err();
        assert!(matches!(err, BTreeError::Serialization { .. }));

        // The value applied before the failure keeps consistent bookkeeping.
        assert_eq!(index.len(), 1);
        assert_eq!(index.keys(None, None), vec![Flaky(1, false)]);
        assert_eq!(
            index.query_with(&Flaky(1, false), |ids| Some(ids.clone())),
            Some(vec![1])
        );
        // The failing value and the values after it were not applied.
        assert!(
            index
                .query_with(&Flaky(2, true), |ids| Some(ids.clone()))
                .is_none()
        );
        assert!(
            index
                .query_with(&Flaky(3, false), |ids| Some(ids.clone()))
                .is_none()
        );
    }
}
