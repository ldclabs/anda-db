# anda_db_btree Technical Documentation

**Crate**: `anda_db_btree`
**Version**: 0.5.9
**Last Updated**: 2026-04-25

---

## Table of Contents

1. [Overview](#1-overview)
2. [Data Model](#2-data-model)
3. [Architecture and Internal State](#3-architecture-and-internal-state)
4. [Concurrency Model](#4-concurrency-model)
5. [Persistence Model](#5-persistence-model)
6. [Query System](#6-query-system)
7. [Configuration](#7-configuration)
8. [API Reference](#8-api-reference)
9. [Usage Examples](#9-usage-examples)
10. [Bucket Management & Compaction](#10-bucket-management--compaction)
11. [Error Handling](#11-error-handling)
12. [Performance Considerations](#12-performance-considerations)
13. [Best Practices](#13-best-practices)
14. [Invariants and Correctness Notes](#14-invariants-and-correctness-notes)

---

## 1. Overview

### 1.1 What is `anda_db_btree`

`anda_db_btree` is a generic, in-memory **inverted B-tree index** with
incremental on-disk persistence. It is one of the three indexing backends that
power AndaDB (alongside BM25 full-text search and HNSW vector search) and is
the workhorse for exact-match, range, and prefix queries over scalar fields.

A single index maps:

```
field_value (FV)  →  set of primary keys (PK)
```

and additionally maintains an ordered key set so that range iteration is
`O(log n + k)` rather than `O(n)`.

### 1.2 Design Goals

| Goal                              | How it is achieved                                                                                          |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| **Thread-safe concurrent writes** | Fine-grained sharded locks via `DashMap`; no global lock around the index                                   |
| **Scalable persistence**          | Postings are grouped into size-bounded **buckets**; only dirty buckets are rewritten on flush               |
| **Consistent snapshots**          | Per-bucket `dirty_version` counter prevents a concurrent mutation from being silently lost during async I/O |
| **Flexible query composition**    | `RangeQuery` supports boolean combinators (`And`/`Or`/`Not`) over any primitive range                       |
| **Generic key types**             | Any `Ord + Eq + Hash + Clone + Serialize + DeserializeOwned` type works as `PK` or `FV`                     |
| **Efficient format**              | CBOR serialization; bucket payload is self-contained                                                        |

### 1.3 When to Use It

Use `anda_db_btree` when you need:

- Exact-match lookups on a field (`WHERE field = ?`).
- Range scans (`WHERE field BETWEEN x AND y`, `WHERE field > x`).
- Prefix scans on string keys.
- Composite boolean queries over a single field.
- Incremental persistence — the index can be flushed frequently without
  rewriting all data.

It is **not** a general full-text engine (use `anda_db_tfs`) nor a nearest-
neighbour engine for dense vectors (use `anda_db_hnsw`).

---

## 2. Data Model

### 2.1 Core Entities

```
┌─────────────────┐
│ field_value FV  │  Any Ord + Eq + Hash + Clone (Serde)
└────────┬────────┘
         │
         ▼
┌──────────────────────────────────────────┐
│ Posting                                  │
│ ┌────────────┬───────────────┬─────────┐ │
│ │ bucket_id  │ update_version│ doc_ids │ │   doc_ids: UniqueVec<PK>
│ │   u32      │      u64      │ Vec<PK> │ │
│ └────────────┴───────────────┴─────────┘ │
└──────────────────────────────────────────┘
```

- `bucket_id` — the persistence bucket currently owning this posting.
- `update_version` — monotonic counter, bumped on every doc-id add/remove. It
  lets external observers detect posting-level changes without holding locks.
- `doc_ids` — unique, insertion-ordered list of primary keys. Backed by
  [`anda_db_utils::UniqueVec`] for O(1) membership checks combined with
  deterministic iteration order.

### 2.2 Bucket

A **bucket** is the unit of persistence. Each bucket owns a subset of the
field values and is serialized as a single CBOR blob:

```rust
struct BucketOnDisk<PK, FV> {
    postings: FxHashMap<FV, (bucket_id, version, Vec<PK>)>,
}
```

In memory, each bucket carries packing metadata:

| Field                         | Meaning                                          |
| ----------------------------- | ------------------------------------------------ |
| `bucket_size: usize`          | Estimated CBOR size used to decide when to spill |
| `is_dirty: bool`              | Unpersisted changes pending                      |
| `field_values: UniqueVec<FV>` | Which postings live in this bucket               |
| `dirty_version: u64`          | Monotonic counter for safe concurrent flush      |

### 2.3 Identifiers

- `bucket_id: u32` — dense, monotonically-assigned; bucket 0 always exists.
- `max_bucket_id: AtomicU32` — upper bound used during load. May transiently
  exceed the actual largest populated bucket during concurrent inserts.

---

## 3. Architecture and Internal State

```text
┌─────────────────────────── BTreeIndex<PK, FV> ───────────────────────────┐
│                                                                         │
│   postings : DashMap<FV, (u32, u64, UniqueVec<PK>)>                     │
│   btree    : RwLock<BTreeSet<FV>>         ◀── ordered key set           │
│   buckets  : DashMap<u32, (size, dirty, UniqueVec<FV>, version)>        │
│   metadata : RwLock<BTreeMetadata>                                      │
│                                                                         │
│   max_bucket_id      : AtomicU32                                        │
│   query_count        : AtomicU64                                        │
│   last_saved_version : AtomicU64                                        │
│   dirty_bucket_count : AtomicU32                                        │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.1 Why Three Collections?

| Collection | Purpose                                        | Access pattern                    |
| ---------- | ---------------------------------------------- | --------------------------------- |
| `postings` | Point lookup, holds the actual `doc_ids`       | `O(1)` by key                     |
| `btree`    | Range/prefix iteration over `FV`               | `O(log n + k)` range              |
| `buckets`  | Persistence packing, independent of query path | Only touched by writers and flush |

Keeping them separate means range queries only lock the small `btree` while
point operations only lock a DashMap shard, so read-heavy workloads scale
almost linearly with cores.

### 3.2 Module Layout

```
rs/anda_db_btree/
├── src/
│   ├── lib.rs           # crate root, re-exports
│   ├── btree.rs         # BTreeIndex, RangeQuery, BTreeConfig, …
│   └── error.rs         # BTreeError enum
├── examples/
│   └── btree_demo.rs
├── benches/
└── Cargo.toml
```

---

## 4. Concurrency Model

The index is designed to be cloned into `Arc` and shared across tasks.

### 4.1 Lock Granularity

| Operation                               | Locks acquired                                                                                                          |
| --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `insert`, `insert_array`                | DashMap shard for the posting, then DashMap shard for the bucket; `btree` write lock **only** when a new key is added   |
| `remove`, `remove_array`                | Same as insert; `btree` write lock only when a posting becomes empty                                                    |
| `query_with`                            | DashMap read shard for the posting                                                                                      |
| `range_query_with`, `prefix_query_with` | `btree` read lock for the duration of iteration; postings are fetched via DashMap read shards per key                   |
| `flush`, `store_dirty_buckets`          | DashMap read for bucket scan; CBOR is built inside the bucket guard, then released before `await`-ing the user's writer |

### 4.2 Race-free Guarantees

- **Uniqueness re-check.** When `allow_duplicates == false`, `insert` and
  `insert_array` re-verify the constraint inside the `postings` entry lock
  after the pre-check, closing a TOCTOU gap against concurrent writers.
- **Atomic posting removal.** Deleting an empty posting is performed via
  `Entry::Occupied` + empty re-check, so a concurrent `insert` that re-adds
  a doc id wins over the removal and keeps the posting alive.
- **Crash-consistent migration.** Moving a posting from bucket A to bucket B
  marks **both** buckets dirty. After a crash, replaying the two latest
  on-disk bucket files yields the correct state with no duplicates or
  resurrections.
- **Dirty-version check on flush.** If a bucket is mutated while its CBOR
  blob is being written, its `dirty_version` changes; the post-write clean
  step then refuses to clear the dirty flag, guaranteeing the mutation is
  persisted on the next flush.

### 4.3 Ordering of Ops

`Ordering::Relaxed` is used for statistics counters; `Ordering::AcqRel` /
`Ordering::Acquire` is used for `dirty_bucket_count` to synchronize the
fast-path of `flush`.

---

## 5. Persistence Model

### 5.1 Files on Disk (Caller's Responsibility)

The crate is **storage-agnostic** — it hands raw CBOR bytes to user-supplied
async callbacks. A typical layout is:

```
<index-dir>/
├── metadata.cbor         ← BTreeMetadata blob
├── bucket_0.cbor
├── bucket_1.cbor
└── bucket_N.cbor
```

`metadata.cbor` is written by [`store_metadata`] / [`flush`]; each
`bucket_*.cbor` is written by [`store_dirty_buckets`] / [`flush`].

### 5.2 Lifecycle

```text
                  ┌──────────────┐
                  │   new(…)     │
                  └──────┬───────┘
                         ▼
  ┌───────────────────────────────────────────────────────┐
  │                insert / insert_array /                 │
  │                remove / remove_array                   │
  └──────────┬────────────────────────────┬────────────────┘
             │                            │
             ▼                            ▼
   dirty_bucket_count += …       metadata.stats.version += 1
             │                            │
             └────────────┬───────────────┘
                          ▼
                    ┌──────────┐
                    │  flush   │
                    └────┬─────┘
                         │
                 ┌───────┴─────────┐
                 ▼                 ▼
         store_metadata     store_dirty_buckets
```

### 5.3 Version Tracking

| Counter                    | Bumped on                     | Consumed by                                                         |
| -------------------------- | ----------------------------- | ------------------------------------------------------------------- |
| `BTreeStats::version`      | every mutating operation      | `store_metadata` (skips write when `last_saved_version >= version`) |
| per-bucket `dirty_version` | every mutation to that bucket | `store_dirty_buckets` (keeps bucket dirty when mutated during I/O)  |

### 5.4 Loading

```rust
// Option A: two-phase
let mut idx = BTreeIndex::load_metadata(meta_reader)?;
idx.load_buckets(async |id| Ok(store.get(id).cloned())).await?;

// Option B: one-shot
let idx = BTreeIndex::load_all(meta_reader, async |id| …).await?;
```

`load_buckets` iterates `0..=max_bucket_id`; missing buckets are tolerated
(the callback returns `Ok(None)`), so compaction or bucket deletion is safe.

---

## 6. Query System

All queries are expressed through `RangeQuery<FV>`:

```rust
pub enum RangeQuery<FV> {
    Eq(FV),
    Gt(FV), Ge(FV), Lt(FV), Le(FV),
    Between(FV, FV),       // inclusive on both ends
    Include(Vec<FV>),      // explicit key set
    And(Vec<Box<RangeQuery<FV>>>),
    Or(Vec<Box<RangeQuery<FV>>>),
    Not(Box<RangeQuery<FV>>),
}
```

### 6.1 Query Methods

| Method                            | Shape                   | Notes                                              |
| --------------------------------- | ----------------------- | -------------------------------------------------- |
| `query_with(&FV, f)`              | exact match             | single DashMap lookup                              |
| `range_query_with(RangeQuery, f)` | any combinator          | streaming, supports early termination              |
| `prefix_query_with(&str, f)`      | `FV = String` only      | implemented via `range(prefix..=prefix+char::MAX)` |
| `keys(cursor, limit)`             | paginated key iteration | ordered, exclusive cursor                          |

### 6.2 Callback Contract

Every query accepts a callback `f(key, ids)` returning `(continue, Vec<R>)`:

- `continue == false` stops iteration.
- The returned `Vec<R>` is appended to the result. Return an empty vec to
  filter a key out, or multiple items per key for fan-out.

### 6.3 Ordering Semantics

| Variant                                              | Emit order                                                                                                                      |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `Gt`, `Ge`, `Between`, `Include`, `And`, `Or`, `Not` | ascending key order                                                                                                             |
| `Lt`, `Le`                                           | **ascending** final order, but iteration is *descending* internally so early termination keeps the keys nearest the upper bound |

This matters when combining with `take(N)`: for `Lt(date) limit 2`, you get
the two largest keys strictly less than `date`, returned ascending.

### 6.4 Logical Combinators

- `Or(subqueries)` returns the **deduplicated union** in global B-tree order
  regardless of subquery declaration order. This guarantees deterministic
  `limit` semantics across subquery permutations.
- `And(subqueries)` intersects the smallest candidate set first for speed.
- `Not(q)` iterates the full key set and excludes matches of `q`; it is
  `O(|index| + |q|)` and should be kept out of hot paths on large indices.

### 6.5 Prefix Query (String keys only)

```rust
let results = idx.prefix_query_with("app", |k, ids| {
    (true, Some((k.to_string(), ids.clone())))
});
```

Internally translates to a range scan `"app"..="app\u{10ffff}"`. An empty
prefix iterates every key in order.

---

## 7. Configuration

```rust
pub struct BTreeConfig {
    pub bucket_overload_size: usize, // default 512 * 1024
    pub allow_duplicates: bool,      // default true
}
```

| Field                  | Meaning                                                                                                                   | Tuning advice                                                                                              |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `bucket_overload_size` | soft target size of a bucket's CBOR payload before spilling into a new one                                                | Larger = fewer files, faster load, but each flush rewrites more bytes. 256 KiB – 2 MiB is a sensible range |
| `allow_duplicates`     | if `false`, enforces uniqueness of `field_value`: a second `doc_id` on the same value returns `BTreeError::AlreadyExists` | Set for unique indexes (PK, unique columns)                                                                |

Bucket overflow is a *soft* limit: the first posting that would cross the
threshold triggers spilling to a fresh bucket. Postings larger than the limit
still fit (they just end up alone in their bucket).

---

## 8. API Reference

### 8.1 Construction & Loading

```rust
impl<PK, FV> BTreeIndex<PK, FV> {
    pub fn new(name: String, config: Option<BTreeConfig>) -> Self;

    pub fn load_metadata<R: Read>(r: R) -> Result<Self, BTreeError>;
    pub async fn load_buckets<F>(&mut self, f: F) -> Result<(), BTreeError>
        where F: AsyncFnMut(u32) -> Result<Option<Vec<u8>>, BoxError>;
    pub async fn load_all<R: Read, F>(metadata: R, f: F) -> Result<Self, BTreeError>
        where F: AsyncFnMut(u32) -> Result<Option<Vec<u8>>, BoxError>;
}
```

### 8.2 Introspection

```rust
pub fn name(&self) -> &str;
pub fn len(&self) -> usize;
pub fn is_empty(&self) -> bool;
pub fn allow_duplicates(&self) -> bool;
pub fn metadata(&self) -> BTreeMetadata;
pub fn stats(&self) -> BTreeStats;
pub fn has_dirty_buckets(&self) -> bool;
```

### 8.3 Mutation

```rust
pub fn insert(&self, doc_id: PK, field_value: FV, now_ms: u64)
    -> Result<bool, BTreeError>;
pub fn remove(&self, doc_id: PK, field_value: FV, now_ms: u64) -> bool;

pub fn insert_array(&self, doc_id: PK, field_values: Vec<FV>, now_ms: u64)
    -> Result<usize, BTreeError>;
pub fn remove_array(&self, doc_id: PK, field_values: Vec<FV>, now_ms: u64)
    -> usize;

pub fn batch_update(&self, doc_id: PK,
                    old: Vec<FV>, new: Vec<FV>, now_ms: u64)
    -> Result<(usize /*removed*/, usize /*inserted*/), BTreeError>;
```

Return-value conventions:

- `insert` returns `true` iff it created a new `(doc_id, field_value)` pair.
  Re-inserting the same pair is idempotent and returns `false` (no version
  bump, no stat change).
- `insert_array` returns the count of newly created pairs.
- `remove` returns `true` iff the pair existed.
- `batch_update` is a diff-based wrapper: it computes
  `new − old` (to insert) and `old − new` (to remove).

### 8.4 Querying

```rust
pub fn query_with<F, R>(&self, field_value: &FV, f: F) -> Option<R>
    where F: FnOnce(&Vec<PK>) -> Option<R>;

pub fn range_query_with<F, R>(&self, query: RangeQuery<FV>, f: F) -> Vec<R>
    where F: FnMut(&FV, &Vec<PK>) -> (bool, Vec<R>);

pub fn keys(&self, cursor: Option<FV>, limit: Option<usize>) -> Vec<FV>;

// Only available for FV = String:
impl<PK> BTreeIndex<PK, String> {
    pub fn prefix_query_with<F, R>(&self, prefix: &str, f: F) -> Vec<R>
        where F: FnMut(&str, &Vec<PK>) -> (bool, Option<R>);
}
```

### 8.5 Persistence

```rust
pub fn store_metadata<W: Write>(&self, w: W, now_ms: u64)
    -> Result<bool, BTreeError>;
pub async fn store_dirty_buckets<F>(&self, f: F) -> Result<(), BTreeError>
    where F: AsyncFnMut(u32, &[u8]) -> Result<bool, BoxError>;
pub async fn flush<W: Write, F>(&self, metadata: W, now_ms: u64, f: F)
    -> Result<bool, BTreeError>
    where F: AsyncFnMut(u32, &[u8]) -> Result<bool, BoxError>;

pub fn compact_buckets(&self) -> (usize /*old*/, usize /*new*/);
```

### 8.6 Types

```rust
pub struct BTreeMetadata { pub name, pub config, pub stats }
pub struct BTreeStats {
    pub last_inserted, pub last_deleted, pub last_saved: u64,
    pub version, pub num_elements,
    pub query_count, pub insert_count, pub delete_count: u64,
    pub max_bucket_id: u32,
}

pub enum BTreeError {
    Generic       { name: String, source: BoxError },
    Serialization { name: String, source: BoxError },
    NotFound      { name: String, id: Value, value: Value },
    AlreadyExists { name: String, id: Value, value: Value },
}
```

---

## 9. Usage Examples

### 9.1 Minimal

```rust
use anda_db_btree::{BTreeConfig, BTreeIndex, RangeQuery};

let idx = BTreeIndex::<u64, String>::new(
    "by_title".into(),
    Some(BTreeConfig { bucket_overload_size: 512 * 1024, allow_duplicates: true }),
);

idx.insert(1, "apple".into(), now_ms)?;
idx.insert(2, "banana".into(), now_ms)?;
idx.insert(3, "apple".into(), now_ms)?; // second doc for "apple"

// exact lookup
let ids = idx.query_with(&"apple".to_string(), |v| Some(v.clone()));
assert_eq!(ids.unwrap(), vec![1, 3]);

// range
let keys = idx.range_query_with(
    RangeQuery::Ge("apple".into()),
    |k, _| (true, vec![k.clone()]),
);
```

### 9.2 Batch + Update

```rust
// Replace a document's tags: old = [a, b], new = [b, c, d]
let (removed, inserted) = idx.batch_update(
    doc_id, vec!["a".into(), "b".into()],
             vec!["b".into(), "c".into(), "d".into()], now_ms)?;
assert_eq!((removed, inserted), (1, 2));
```

### 9.3 Complex Boolean Queries

```rust
use anda_db_btree::RangeQuery::*;

// (tag >= "cat" AND tag <= "dog") OR tag == "zebra"
let q = Or(vec![
    Box::new(And(vec![
        Box::new(Ge("cat".into())),
        Box::new(Le("dog".into())),
    ])),
    Box::new(Eq("zebra".into())),
]);

let hits = idx.range_query_with(q, |k, ids| (true, vec![(k.clone(), ids.clone())]));
```

### 9.4 Persistence Round-Trip

```rust
use std::fs::File;
use std::io::{Read, Write};

// ---- Save ----
let meta = File::create("meta.cbor")?;
idx.flush(meta, now_ms, async |id, data| {
    File::create(format!("bucket_{id}.cbor"))?.write_all(data)?;
    Ok(true)
}).await?;

// ---- Load ----
let mut loaded = BTreeIndex::<u64, String>::load_metadata(File::open("meta.cbor")?)?;
loaded.load_buckets(async |id| {
    let mut f = File::open(format!("bucket_{id}.cbor"))?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    Ok(Some(buf))
}).await?;
```

See the runnable [examples/btree_demo.rs](../rs/anda_db_btree/examples/btree_demo.rs).

### 9.5 Concurrent Writes

```rust
use std::sync::Arc;

let idx = Arc::new(BTreeIndex::<u64, String>::new("shared".into(), None));
let mut tasks = Vec::new();
for t in 0..8 {
    let idx = idx.clone();
    tasks.push(tokio::spawn(async move {
        for i in 0..1_000 {
            idx.insert((t * 1_000 + i) as u64, format!("k{i}"), now_ms).unwrap();
        }
    }));
}
futures::future::try_join_all(tasks).await.unwrap();
```

---

## 10. Bucket Management & Compaction

### 10.1 Spill Policy

A posting is placed in the **current max bucket** if that bucket still has
room (`bucket_size < bucket_overload_size`). Otherwise a new bucket is
allocated and `max_bucket_id` is incremented. Migration in `insert_array`
only moves postings that actually overflow the destination bucket — existing
postings are not re-binned unnecessarily.

### 10.2 Migration Semantics

When a posting moves from bucket `A` to bucket `B`:

1. `posting.bucket_id` is updated to `B`.
2. Bucket `A`'s `field_values` loses the key; `A.is_dirty = true`.
3. Bucket `B`'s `field_values` gains the key; `B.is_dirty = true`.

Both dirty flags ensure a crash after step 3 cannot cause the posting to be
re-read from `A` on the next load.

### 10.3 Compaction

`compact_buckets()` re-bins every posting using **first-fit-decreasing**
bin packing. This is intended for **offline** repair (e.g. after replaying a
legacy index whose buckets were over-split by an older bug). The procedure:

1. Estimates the serialized size of each posting.
2. Sorts descending by size.
3. Places each posting into the first bucket that still has room.
4. Clears `buckets`, rewrites bucket ids `0..=max`, marks all dirty.

The next `flush` will rewrite every bucket file. Call `compact_buckets` only
when the index is quiesced (no concurrent writers).

---

## 11. Error Handling

```rust
pub enum BTreeError {
    Generic       { name, source },
    Serialization { name, source },
    NotFound      { name, id, value },
    AlreadyExists { name, id, value },
}
```

| Variant         | When raised                                                                 | Typical caller action         |
| --------------- | --------------------------------------------------------------------------- | ----------------------------- |
| `AlreadyExists` | `allow_duplicates = false` and a different `doc_id` already owns that value | Surface as 409 Conflict       |
| `Serialization` | CBOR encode/decode failure                                                  | Fatal; bucket file is suspect |
| `Generic`       | wrap-around of any user I/O error returned from the flush/load callbacks    | Propagate with context        |
| `NotFound`      | reserved for higher-level callers; not emitted by current APIs              | n/a                           |

All errors carry the index `name` for observability.

---

## 12. Performance Considerations

### 12.1 Complexity

| Operation                              | Amortized cost                                               |
| -------------------------------------- | ------------------------------------------------------------ |
| `insert` / `remove`                    | `O(1)` hash lookup + `O(log n)` for new/removed keys (btree) |
| `insert_array(k items)`                | `O(k)` amortized, one `btree` write-lock round for new keys  |
| `query_with`                           | `O(1)`                                                       |
| `range_query_with Gt/Ge/Lt/Le/Between` | `O(log n + k)`                                               |
| `prefix_query_with`                    | `O(log n + k)`                                               |
| `And` of N subqueries                  | `O(min_set_size × N)` with early termination                 |
| `Not`, full-set scans                  | `O(n)`                                                       |
| `flush`                                | `O(dirty_buckets × bucket_size)`                             |

### 12.2 Memory

- `postings` stores every `(FV, Vec<PK>)` pair; expect `~overhead(32B) +
  size_of(FV) + sum(size_of(PK))` per entry.
- `btree` adds another `size_of(FV)` per key.
- Bucket metadata is negligible (a few dozen bytes per bucket).

For very large indices (tens of millions of keys) consider sharding into
multiple `BTreeIndex` instances by key prefix.

### 12.3 Tuning Checklist

1. **Bucket size** — raise `bucket_overload_size` (e.g. to 2 MiB) if flushes
   dominate; lower it if cold-load latency dominates.
2. **Batch writes** — prefer `insert_array` / `batch_update` over tight
   single-row loops: they amortize `btree` write locks and statistic updates.
3. **Flush cadence** — `flush` is cheap when nothing is dirty (fast-path on
   `dirty_bucket_count == 0`). Call it frequently.
4. **Concurrent readers** — `query_with` and `range_query_with` are lock-free
   against other readers; scale readers freely.
5. **Avoid `Not` in hot paths** — it scans the entire key set.

---

## 13. Best Practices

### 13.1 Schema Design

- Use a **unique index** (`allow_duplicates = false`) for every field that
  represents an identity (e.g. external IDs). It turns silent duplication
  bugs into loud `AlreadyExists` errors.
- Keep `FV` compact. For long strings, hash to a fixed key and store the
  original separately — it shrinks both the CBOR payload and the `btree`
  footprint.

### 13.2 Writing

- Prefer `batch_update` for "replace the tags of this doc" patterns; it
  avoids the redundant remove-then-reinsert of shared values.
- Keep `doc_id` small and `Copy`-like (`u64`, `[u8; 16]`): every posting
  stores it inline.

### 13.3 Querying

- Use `Include(Vec<FV>)` for sparse key lists instead of building an `Or` of
  `Eq` subqueries — it avoids the extra combinator overhead.
- When paginating, use `keys(cursor, limit)` directly over `btree` instead
  of `range_query_with` — it bypasses the posting map entirely.

### 13.4 Persistence

- Call `flush` whenever your host commits a logical transaction. The
  fast-path check (`dirty_bucket_count` and `last_saved_version`) makes
  idempotent calls essentially free.
- Make the flush callbacks **fsync** at their own cadence. The crate
  guarantees the in-memory model is correct after `Ok(_)`; durability
  depends entirely on the caller's I/O layer.

### 13.5 Recovery

- After a crash, re-run `load_all(metadata, loader)`. Any bucket missing on
  disk is treated as empty, which is safe because the metadata version is
  only bumped after the mutation is observable.
- If you detect fragmentation or legacy over-split buckets, run
  `compact_buckets()` once while the index is idle, then `flush` once more.

---

## 14. Invariants and Correctness Notes

The implementation upholds the following invariants; tests in
[`btree.rs`](../rs/anda_db_btree/src/btree.rs) pin them down.

1. **Posting ↔ btree bijection.** Every key in `btree` has a non-empty
   posting, and every non-empty posting has a key in `btree`.
2. **Bucket ownership.** Every posting is tracked by exactly one bucket. A
   migration is always a three-step update (posting's `bucket_id`, source
   removal, destination insertion) with both buckets left dirty.
3. **Uniqueness re-check.** `insert` and `insert_array` re-validate
   `allow_duplicates = false` inside the `postings` entry lock.
4. **Empty-posting removal is atomic.** The btree key is removed only if the
   posting is still empty at the moment of removal.
5. **Dirty version consistency.** `store_dirty_buckets` clears the dirty
   flag only when the bucket's `dirty_version` matches the value it sampled
   before issuing the async write — preserving concurrent mutations.
6. **Version monotonicity.** `BTreeStats::version` is strictly increasing
   across every mutating operation; idempotent inserts/removes do **not**
   bump it.
7. **Bucket size accounting.** For any posting living in bucket `B`, its
   size contribution flows through `B.bucket_size`; this is the foundation
   of the spill policy. `insert_array` accumulates per-bucket aggregate
   deltas in Phase 1 and applies them atomically in Phase 2 (see the
   "insert_array three phases" doc comment).
8. **`max_bucket_id` monotonicity.** Never decreases while the index is
   live; `compact_buckets` resets it atomically as part of the full rebuild.

---

## References

- Source: [rs/anda_db_btree](../rs/anda_db_btree)
- Module overview: [rs/anda_db_btree/src/btree.rs](../rs/anda_db_btree/src/btree.rs)
- Example: [rs/anda_db_btree/examples/btree_demo.rs](../rs/anda_db_btree/examples/btree_demo.rs)
- Utilities: `UniqueVec`, `estimate_cbor_size` in
  [rs/anda_db_utils](../rs/anda_db_utils)
