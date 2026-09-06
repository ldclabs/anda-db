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
//! | `postings`  | `DashMap<FV, Posting<PK>>`         | Per-field-value posting list, enables concurrent point ops |
//! | `btree`     | `RwLock<BTreeSet<FV>>`                  | Ordered key set for range / prefix queries                 |
//! | `buckets`   | `DashMap<u32, BucketState<FV>>` | Packing metadata used to schedule incremental flushes      |
//! | `metadata`  | `RwLock<BTreeMetadata>`                 | Name, config and aggregate statistics                      |
//!
//! ## Concurrency Model
//!
//! - Mutations share a compaction gate, lock posting/bucket shards, and
//!   briefly lock metadata for statistics. The ordered-key write lock is
//!   needed only when creating or removing a key.
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
//!   best-fit-decreasing bin packing.

use anda_db_utils::UniqueVec;
mod posting;
use posting::PostingList;
mod state;
use dashmap::DashMap;
use parking_lot::RwLock;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use state::{BucketState, Posting, Removal};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    future::Future,
    hash::Hash,
    io::{Read, Write},
    ops::Bound,
    sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
};

use crate::{BTreeError, BoxError};

/// Serialization-only views. No posting, PK, FV or membership map is cloned.
struct BucketView<'a, PK, FV>
where
    PK: Ord + Eq + Hash + Debug + Clone + Serialize + DeserializeOwned,
    FV: Ord + Eq + Hash + Debug + Clone + Serialize + DeserializeOwned,
{
    index: &'a BTreeIndex<PK, FV>,
    id: u32,
    fields: &'a UniqueVec<FV>,
}
impl<PK, FV> Serialize for BucketView<'_, PK, FV>
where
    PK: Ord + Eq + Hash + Debug + Clone + Serialize + DeserializeOwned,
    FV: Ord + Eq + Hash + Debug + Clone + Serialize + DeserializeOwned,
{
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("Bucket", 1)?;
        state.serialize_field("p", &PostingsView(self))?;
        state.end()
    }
}
struct PostingsView<'a, 'b, PK, FV>(&'a BucketView<'b, PK, FV>)
where
    PK: Ord + Eq + Hash + Debug + Clone + Serialize + DeserializeOwned,
    FV: Ord + Eq + Hash + Debug + Clone + Serialize + DeserializeOwned;
impl<PK, FV> Serialize for PostingsView<'_, '_, PK, FV>
where
    PK: Ord + Eq + Hash + Debug + Clone + Serialize + DeserializeOwned,
    FV: Ord + Eq + Hash + Debug + Clone + Serialize + DeserializeOwned,
{
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(None)?;
        for key in self.0.fields.iter() {
            if let Some(posting) = self.0.index.postings.get(key)
                && posting.bucket_id == self.0.id
                && !posting.docs.is_empty()
            {
                map.serialize_entry(key, &*posting)?;
            }
        }
        map.end()
    }
}

/// Largest `max_bucket_id` accepted from pre-manifest metadata.
///
/// Legacy loading probes every bucket id in `0..=max_bucket_id`, and that
/// watermark comes from untrusted storage: a corrupted value would turn into
/// billions of callback invocations. No legitimate legacy index comes
/// anywhere near this many bucket objects.
const MAX_LEGACY_BUCKET_ID: u32 = 1 << 20;

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
    doc_ids_after_append: &PostingList<PK>,
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
    let previous = (
        bucket_id,
        version_after_append.saturating_sub(1),
        &doc_ids_after_append[..doc_ids_after_append.len().saturating_sub(1)],
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
/// 3. `max_bucket_id` only grows between compactions. It may exceed the
///    actual largest populated bucket id transiently during concurrent
///    inserts, bucket ids may be sparse, and [`BTreeIndex::compact_buckets`]
///    renumbers buckets densely from `0` and resets it. Durable objects are
///    addressed by `(bucket_id, generation)`, so a reused id never collides
///    with a retired object.
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
    buckets: DashMap<u32, BucketState<FV>>,

    /// Inverted index: field value → posting list. See [`Posting`].
    postings: DashMap<FV, Posting<PK>>,

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

    /// Fast-path hint for [`BTreeIndex::has_dirty_buckets`]: `false` means no
    /// bucket is dirty, so pollers skip the full bucket scan. It is raised
    /// after every dirty mark and lowered only by a flush that verified — at
    /// quiescence, which the flush contract guarantees — that nothing is
    /// dirty any more, so it can never hide a dirty bucket.
    dirty_hint: AtomicBool,

    /// Bootstrap completeness. A flush rebuilds the manifest from the
    /// in-memory bucket map, so flushing an index whose buckets were never
    /// loaded would retire committed objects; read-only states refuse it.
    load_state: LoadState,
    legacy_format: bool,
}

/// Whether the index is complete enough to accept mutations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadState {
    /// Metadata has been decoded but postings have not been loaded.
    MetadataOnly,
    /// A partial, interrupted or failed load; only queries are allowed.
    Partial,
    /// All required bucket objects are present. New indexes start here.
    Ready,
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

/// Whether compaction actually rebuilt the bucket layout. Counts alone cannot
/// distinguish a changed layout from a no-op.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionOutcome {
    /// Number of buckets before compaction.
    pub old_bucket_count: usize,
    /// Number of buckets after compaction.
    pub new_bucket_count: usize,
    /// True when bucket ownership/contents were rebuilt and need persistence.
    pub changed: bool,
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
type StoredPosting<PK> = (u32, u64, PostingList<PK>);

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

    /// Whether one field value can belong to multiple different primary keys.
    /// The same (primary key, field value) pair is always idempotent.
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
    /// only in the bucket objects it references. The loader separately tracks
    /// whether this field was absent in legacy metadata: a present empty map
    /// is a modern empty index and never scans old bucket objects.
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

// Preserve the distinction between an absent legacy manifest and a present,
// empty manifest without changing the public BTreeMetadata representation.
#[derive(Deserialize)]
struct IndexForLoad {
    metadata: MetadataForLoad,
}

#[derive(Deserialize)]
struct MetadataForLoad {
    name: String,
    config: BTreeConfig,
    stats: BTreeStats,
    #[serde(default, deserialize_with = "manifest_for_load")]
    buckets: Option<BTreeMap<u32, u64>>,
}

fn manifest_for_load<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<BTreeMap<u32, u64>>, D::Error> {
    // Only absence selects legacy mode. An explicit null is malformed.
    BTreeMap::deserialize(deserializer).map(Some)
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
    postings: FxHashMap<FV, StoredPosting<PK>>,
}

pub use query::RangeQuery;
mod compaction;
mod mutation;
mod persistence;
mod query;

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
    fn mark_bucket_dirty(&self, bucket: &mut BucketState<FV>) {
        // Raise the poller hint *before* the dirty bit. A reader that loads
        // `false` has then provably run before this store, hence before the
        // bit was set, so short-circuiting its scan is a correct answer as of
        // that moment. Setting the bit first would leave a window in which a
        // reader reports "clean" for a bucket that is already dirty.
        self.dirty_hint.store(true, Ordering::Release);
        bucket.dirty_version = bucket.dirty_version.wrapping_add(1);
        bucket.dirty = true;
    }

    /// Returns the id of the bucket new postings currently go to, creating
    /// its entry if a concurrent migration advanced `max_bucket_id` before
    /// materializing the bucket (or if the index was restored by
    /// `load_metadata` alone, whose watermark is ahead of its buckets).
    ///
    /// `contains_key` (shard read lock) first: every insert hits the same
    /// current bucket id, so taking the shard write lock via `entry()` each
    /// time would serialize concurrent inserts on this hot path.
    fn current_bucket(&self) -> u32 {
        let bucket_id = self.max_bucket_id.load(Ordering::Relaxed);
        if !self.buckets.contains_key(&bucket_id) {
            self.buckets.entry(bucket_id).or_default();
        }
        bucket_id
    }

    /// Registers `field_value` — whose posting already points at
    /// `bucket_id` — in the freshly allocated bucket `bucket_id`, creating
    /// the entry unless a concurrent writer materialized it first.
    fn register_in_new_bucket(&self, bucket_id: u32, size: usize, field_value: FV) {
        match self.buckets.entry(bucket_id) {
            dashmap::Entry::Vacant(entry) => {
                // Born dirty, so the hint is raised here rather than through
                // `mark_bucket_dirty` (same ordering rationale).
                self.dirty_hint.store(true, Ordering::Release);
                entry.insert(BucketState::new(size, true, vec![field_value].into(), 1));
            }
            dashmap::Entry::Occupied(mut entry) => {
                let bucket = entry.get_mut();
                bucket.size = bucket.size.saturating_add(size);
                self.mark_bucket_dirty(bucket);
                bucket.fields.push(field_value);
            }
        }
    }

    /// Whether `field_value`'s posting, already listed by `bucket`, may keep
    /// growing where it is.
    ///
    /// A posting that fills a bucket on its own stays put: migrating it would
    /// only land it alone in a fresh bucket that is just as full, leaving an
    /// empty bucket behind on every append. One that shares its bucket does
    /// move out once the bucket is over the soft limit, so a hot posting ends
    /// up isolated instead of dragging its neighbours into every rewrite.
    ///
    /// `insert` and `insert_array` must agree here — they diverged before,
    /// and `insert_array` never migrating an existing posting is exactly the
    /// bug this rule replaced.
    fn member_posting_stays(&self, bucket: &BucketState<FV>, additional_size: usize) -> bool {
        bucket.size.saturating_add(additional_size) < self.config.bucket_overload_size
            || bucket.fields.len() == 1
    }

    /// Detaches `field_value` from the bucket that owned `previous`, a
    /// posting superseded while loading (a newer copy lives in bucket
    /// `current_bucket_id`, or the key was tombstoned). The old bucket's
    /// size estimate is corrected and it is marked dirty so its stale
    /// on-disk copy is rewritten by the next flush.
    fn detach_superseded_posting(
        &self,
        field_value: &FV,
        previous: &Posting<PK>,
        current_bucket_id: u32,
    ) {
        let previous_bucket_id = previous.bucket_id;
        if previous_bucket_id != current_bucket_id
            && let Some(mut previous_bucket) = self.buckets.get_mut(&previous_bucket_id)
            && previous_bucket
                .fields
                .swap_remove_if(|key| key == field_value)
                .is_some()
        {
            let previous_size = posting_entry_size(field_value, previous);
            previous_bucket.size = previous_bucket.size.saturating_sub(previous_size);
            self.mark_bucket_dirty(&mut previous_bucket);
        }
    }

    fn serialize_bucket_snapshot(
        &self,
        bucket_id: u32,
    ) -> Result<Option<BucketPersistenceSnapshot>, BTreeError> {
        let Some(bucket) = self.buckets.get(&bucket_id) else {
            return Ok(None);
        };
        if !bucket.dirty {
            return Ok(None);
        }

        let dirty_version = bucket.dirty_version;
        let mut data = Vec::with_capacity(bucket.size.clamp(256, 1024 * 1024));
        cbor2::to_writer(
            &BucketView {
                index: self,
                id: bucket_id,
                fields: &bucket.fields,
            },
            &mut data,
        )
        .map_err(|err| BTreeError::Serialization {
            name: self.name.clone(),
            source: err.into(),
        })?;
        drop(bucket);

        Ok(Some(BucketPersistenceSnapshot {
            bucket_id,
            dirty_version,
            data,
        }))
    }

    /// Captures only dirty bucket identities; payloads are encoded one at a time.
    fn dirty_bucket_ids(&self) -> Vec<u32> {
        let mut ids: Vec<u32> = self
            .buckets
            .iter()
            .filter_map(|bucket| bucket.dirty.then_some(*bucket.key()))
            .collect();
        ids.sort_unstable();
        ids
    }

    fn mark_bucket_snapshot_saved(&self, bucket_id: u32, dirty_version: u64) {
        if let Some(mut bucket) = self.buckets.get_mut(&bucket_id)
            && bucket.dirty
            && bucket.dirty_version == dirty_version
        {
            bucket.dirty = false;
        }
    }

    fn remove_btree_key_if_posting_absent(&self, field_value: &FV) {
        self.remove_btree_keys_if_postings_absent(std::slice::from_ref(field_value));
    }

    /// Drops each key in `field_values` from the ordered set, under a single
    /// btree write lock.
    ///
    /// The posting is re-checked *inside* that lock: a concurrent `insert`
    /// may have re-created the entry after the caller emptied it, and
    /// removing the key anyway would leave a posting no range query can
    /// reach.
    fn remove_btree_keys_if_postings_absent<'a, I>(&self, field_values: I)
    where
        FV: 'a,
        I: IntoIterator<Item = &'a FV>,
    {
        let mut field_values = field_values.into_iter().peekable();
        if field_values.peek().is_none() {
            return;
        }
        let mut btree = self.btree.write();
        for field_value in field_values {
            if !self.postings.contains_key(field_value) {
                btree.remove(field_value);
            }
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
            buckets: DashMap::from_iter(vec![(0, BucketState::default())]),
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
            dirty_hint: AtomicBool::new(false),
            load_state: LoadState::Ready,
            legacy_format: false,
        }
    }

    /// Returns the bootstrap state. Only `Ready` accepts mutations.
    pub fn load_state(&self) -> LoadState {
        self.load_state
    }

    fn ensure_ready(&self) -> Result<(), BTreeError> {
        if self.load_state != LoadState::Ready {
            return Err(BTreeError::Generic {
                name: self.name.clone(),
                source: format!(
                    "index is read-only ({:?}); load all required buckets first",
                    self.load_state
                )
                .into(),
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
        self.query_count.fetch_add(1, Ordering::Relaxed);
        let mut results = Vec::new();
        if self.postings.is_empty() {
            return results;
        }

        // 从 prefix 起正序遍历，遇到第一个不以 prefix 开头的键即终止。
        // 以 prefix 开头的键在 BTreeSet 中是连续区段，因此这种写法是完备的；
        // 而旧实现构造 "prefix + char::MAX" 作为闭区间上界，会漏掉
        // "prefix + char::MAX + 任意后缀" 这类键。空前缀自然退化为全量遍历。
        for k in self
            .btree
            .read()
            .range::<str, _>((Bound::Included(prefix), Bound::Unbounded))
        {
            if !k.starts_with(prefix) {
                break;
            }
            if let Some(posting) = self.postings.get(k) {
                let (con, rt) = f(k, &posting.docs);
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
mod tests;
