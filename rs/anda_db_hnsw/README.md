# anda_db_hnsw

[![Crates.io](https://img.shields.io/crates/v/anda_db_hnsw.svg)](https://crates.io/crates/anda_db_hnsw)
[![Docs.rs](https://docs.rs/anda_db_hnsw/badge.svg)](https://docs.rs/anda_db_hnsw)

`anda_db_hnsw` is the approximate-nearest-neighbor vector index of
[AndaDB](https://github.com/ldclabs/anda-db). It implements a persistable
Hierarchical Navigable Small World (HNSW) graph for embedded AI-memory
workloads, with `bf16` vector storage and incremental flushing.

## What this crate provides

- `HnswIndex` with `insert` / `insert_f32`, `remove`, `search` /
  `search_f32`, and `search_f32_in_ids` for search restricted to a candidate
  set.
- Distance metrics: `Euclidean` (the default), `Cosine`, `InnerProduct` and
  `Manhattan`, with vectorized distance kernels.
- `HnswConfig`: `dimension`, `max_layers`, `max_connections` (M),
  `ef_construction`, `ef_search`, `distance_metric`, `scale_factor`,
  `select_neighbors_strategy` (`Heuristic` or `Simple`) and
  `reconnect_on_delete`.
- `bf16` vector storage for lower memory use; `f32` queries are not
  quantized.
- Concurrent reads and writes suited to embedded services.
- Incremental persistence of metadata, ids and dirty node objects, with a
  `RecoveryReport` after loading.

## Getting started

```toml
[dependencies]
anda_db_hnsw = "0.14"
```

```rust
use anda_db_hnsw::{DistanceMetric, HnswConfig, HnswIndex};

fn main() -> Result<(), anda_db_hnsw::HnswError> {
    let index = HnswIndex::try_new(
        "embeddings".into(),
        Some(HnswConfig {
            dimension: 3,
            distance_metric: DistanceMetric::Cosine,
            ..Default::default()
        }),
    )?;
    index.insert_f32(1, vec![0.9, 0.1, 0.0], 0)?;
    index.insert_f32(2, vec![0.1, 0.9, 0.0], 0)?;

    // (id, distance) pairs, nearest first.
    let hits = index.search_f32(&[0.8, 0.2, 0.0], 1)?;
    assert_eq!(hits[0].0, 1);
    Ok(())
}
```

Set `dimension` and `distance_metric` to match the embedding model: the
defaults are 512 dimensions and Euclidean distance, and every insert and
search checks the dimension.

This crate is normally used through `anda_db`, where an HNSW index backs a
`Vector` or `Option<Vector>` field; it can also be embedded on its own.

## Limits and persistence

- `HnswIndex::try_new` validates the configuration; `new` is the infallible
  constructor.
- `top_k` and the effective `ef_search` must not exceed 4,096; a larger
  request is an error rather than silent truncation. `SearchOptions` can
  override `ef_search` per query, which is raised to at least `top_k`.
- Persistence calls must be serialized by the caller. Prefer
  `flush_with_options` for explicit completion status and bounded parallel
  I/O, then purge committed deletions.
- Fixed-key node, id and metadata objects support recovery of partial
  progress when the backend provides conditional writes; they are not
  multi-object transactions. The technical reference explains generation
  markers, numeric limits and legacy-format loading.

## Validation and performance

```bash
cargo test -p anda_db_hnsw
CARGO_PROFILE_BENCH_OPT_LEVEL=3 cargo bench -p anda_db_hnsw --bench hnsw_index -- --run
```

The tests include recall floors. The benchmark is opt-in (`--run`); see the
[benchmark guide](benches/README.md) for parameters and output columns, and
[docs/anda_db_hnsw_benchmarks.md](../../docs/anda_db_hnsw_benchmarks.md) for
recorded results.

## Technical reference

- [docs/anda_db_hnsw.md](../../docs/anda_db_hnsw.md): algorithm, `bf16`
  storage, insertion pipeline and persistence artifacts
- [docs/anda_db.md](../../docs/anda_db.md): vector search in collections

## Related crates

- [`anda_db`](../anda_db): collection-level semantic retrieval
- [`anda_db_tfs`](../anda_db_tfs): lexical search fused with vector results

## License

MIT. See [LICENSE](../../LICENSE).
