# anda_db_hnsw

`anda_db_hnsw` is the approximate-nearest-neighbor vector index used by
AndaDB. It implements a persistable HNSW graph optimized for embedded AI-memory
workloads, with `bf16` vector storage and incremental flush support.

## What This Crate Provides

- HNSW approximate-nearest-neighbor search
- configurable distance metrics and graph parameters
- `bf16` vector storage for lower memory usage
- concurrent read/write behavior suited to embedded services
- incremental persistence of metadata, ids, and dirty node blobs
- reusable vector-search primitives for `anda_db`

## When to Use It

Use `anda_db_hnsw` when you need:

- semantic search over embeddings
- an embeddable ANN index inside a Rust application
- persistent vector search without an external vector database
- control over HNSW construction and search tradeoffs

## Getting Started

Add the crate to your project:

```toml
[dependencies]
anda_db_hnsw = "0.14"
```

This crate is normally used through `anda_db`, but it can also be embedded
independently for lower-level vector-search use cases.

Use `HnswIndex::try_new` for validated configuration. Search requests above
4,096 results return an error; `SearchOptions` can override ef per query.
Persistence calls must be serialized by the caller. Prefer
`flush_with_options` for explicit completion status and bounded parallel I/O,
then purge committed deletions. Fixed-key objects support recovery of partial
progress when node, IDs and metadata replacements use backend conditional writes;
they are not multi-object transactions. The technical reference explains
generation markers, numeric limits and legacy-format loading.

For repeatable performance measurements, see [the benchmark guide](benches/README.md).

## Technical Reference

Deep technical documentation for this crate lives in:

- [docs/anda_db_hnsw.md](../../docs/anda_db_hnsw.md)
- [docs/anda_db.md](../../docs/anda_db.md)

## Related Crates

- `anda_db` for collection-level semantic retrieval
- `anda_db_tfs` for lexical search that can be fused with vector results

## License

MIT. See [LICENSE](../../LICENSE).
