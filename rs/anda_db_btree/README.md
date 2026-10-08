# anda_db_btree

[![Crates.io](https://img.shields.io/crates/v/anda_db_btree.svg)](https://crates.io/crates/anda_db_btree)
[![Docs.rs](https://docs.rs/anda_db_btree/badge.svg)](https://docs.rs/anda_db_btree)

`anda_db_btree` is the exact-match and range-index engine of
[AndaDB](https://github.com/ldclabs/anda-db). It is an embedded, in-memory
inverted B-tree — field value → posting list of document ids — with
incremental bucketized persistence, designed for concurrent filtering in AI
memory systems.

## What this crate provides

- `BTreeIndex<PK, FV>` with exact lookup (`query_with`), ordered range scans
  (`range_query_with`, `range_query_rev_with`), string prefix scans
  (`prefix_query_with`), key pagination (`keys`) and cardinality estimates.
- `RangeQuery` with `Eq`, `Gt`, `Ge`, `Lt`, `Le`, `Between`, `Include` and
  boolean `And` / `Or` / `Not`, compiled to streaming key ranges. The
  fallible `try_range_query_with` distinguishes an invalid query from an
  empty result.
- `BTreeConfig`: `allow_duplicates` (one value for many ids, or a unique
  index) and `bucket_overload_size` (the soft size of a persisted bucket).
- Concurrent inserts, removes and queries without an external service.
- Incremental persistence: `flush` / `flush_owned_with` write metadata and
  only the dirty buckets; `compact_buckets` repacks a fragmented index.
- Loading modes: `load_all` (strict and complete), `load_metadata` followed
  by `load_buckets`, and `load_buckets_partial` for read-only inspection of
  incomplete data, reported through `load_state()`.

## Getting started

```toml
[dependencies]
anda_db_btree = "0.14"
```

```rust
use anda_db_btree::{BTreeIndex, RangeQuery};

let index = BTreeIndex::<u64, String>::new("tags".into(), None);
index.insert(1, "rust".into(), 1)?;
// Re-inserting the same (id, value) pair is idempotent.
assert!(!index.insert(1, "rust".into(), 2)?);

let keys = index.try_range_query_with(
    RangeQuery::Ge("r".into()),
    |key, _ids| (false, vec![key.clone()]),
)?;
assert_eq!(keys, vec!["rust"]);
# Ok::<(), anda_db_btree::BTreeError>(())
```

The range callback returns `(continue, items)`: `false` stops the scan after
collecting `items`. Callbacks run under the index's internal locks and must
not call back into the same index. The crate documentation also shows a full
flush and `load_all` round trip.

This crate is normally used through `anda_db`, where B-Tree indexes back
collection filters, uniqueness checks and id pagination; it can also be
embedded on its own.

## Recovery rules

- Complete loading treats a missing manifest object as an error.
- `load_buckets_partial` is only for inspecting incomplete data: check
  `load_state()` (`Ready`, `Partial` or `MetadataOnly`) before attempting
  mutations, which are refused until the index is ready.
- Persistence calls must be serialized by the caller.

## Validation and performance

```bash
cargo test -p anda_db_btree --all-targets --all-features
ANDA_BTREE_RUN_BENCH=1 cargo bench -p anda_db_btree --bench workloads
```

The tests include a property-based model check and concurrency tests. The
[maintenance report](../../docs/anda_db_btree-maintenance.md) records the
recovery fixes, compatibility checks, benchmark results and memory
trade-offs.

## Technical reference

- [docs/anda_db_btree.md](../../docs/anda_db_btree.md): design, range
  queries, bucket persistence and correctness notes
- [docs/anda_db.md](../../docs/anda_db.md): how collections use the index

## Related crates

- [`anda_db`](../anda_db): collection-level query execution
- [`anda_db_tfs`](../anda_db_tfs) and [`anda_db_hnsw`](../anda_db_hnsw): the
  lexical and vector indexes it is combined with

## License

MIT. See [LICENSE](../../LICENSE).
