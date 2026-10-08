# anda_db

[![Crates.io](https://img.shields.io/crates/v/anda_db.svg)](https://crates.io/crates/anda_db)
[![Docs.rs](https://docs.rs/anda_db/badge.svg)](https://docs.rs/anda_db)

`anda_db` is the embedded database core of the
[AndaDB](https://github.com/ldclabs/anda-db) workspace. It gives Rust
applications schema-aware collections, object-store-backed persistence, and
three built-in retrieval modes: B-Tree for exact and range filters, BM25 for
full-text search, and HNSW for vector similarity search.

## What this crate provides

- **Database lifecycle** through `AndaDB`: create, connect or open a
  database on any `object_store` backend; flush, close, read-only mode and a
  periodic `auto_flush` task.
- **Collections** with schema validation on write, document CRUD
  (`add`, `add_from`, `get`, `get_as`, `update`, `remove`) and engine-assigned
  `u64` document ids.
- **Indexes** created in the collection's open callback:
  - B-Tree (`create_btree_index_nx`) on one field, or on several fields as a
    unique composite key;
  - BM25 (`create_bm25_index_nx`) over one or more text fields, with the
    default tokenizer or Jieba for Chinese;
  - HNSW (`create_hnsw_index_nx`) over a `Vector` or `Option<Vector>` field.
- **Queries**:
  - `search` / `search_as` / `search_ids`: BM25 text search, vector search,
    or both fused with reciprocal-rank fusion, optionally narrowed by a
    B-Tree `Filter` (`Field`, `And`, `Or`, `Not`);
  - `search_with_options` with `SearchOptions` for oversampling, candidate
    caps and exact pre-filtered search on small match sets;
  - `query_ids` / `query_last_ids` for bounded, ordered id pagination over a
    filter, and `query_all_ids` for in-process callers that need every match.
- **Persistence and recovery**: incremental index flushing, compressed and
  cached object I/O, checkpoints, crash recovery on open and
  `reconcile_storage` for explicit repair.
- **Extensions**: small typed metadata entries at database and collection
  scope.
- **Customization**: `set_tokenizer` for BM25 and `IndexHooks` for derived
  index keys, searchable text or alternative vector encodings.

## Getting started

```toml
[dependencies]
anda_db = "0.14"
object_store = "0.14"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

```rust
use anda_db::{
    collection::CollectionConfig,
    database::{AndaDB, DBConfig},
    index::HnswConfig,
    query::{Filter, Query, RangeQuery, Search},
    schema::{AndaDBSchema, Fv, Vector, vector_from_f32},
};
use object_store::memory::InMemory;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, AndaDBSchema)]
struct Note {
    _id: u64,
    /// Owner of the note.
    owner: String,
    body: String,
    embedding: Option<Vector>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: "notes_db".into(),
            description: "Notes".into(),
            ..Default::default()
        },
    )
    .await?;

    let notes = db
        .open_or_create_collection(
            Note::schema()?,
            CollectionConfig {
                name: "notes".into(),
                description: "Agent notes".into(),
            },
            async |c| {
                c.create_btree_index_nx(&["owner"]).await?;
                c.create_bm25_index_nx(&["body"]).await?;
                c.create_hnsw_index_nx(
                    "embedding",
                    HnswConfig {
                        dimension: 3,
                        ..Default::default()
                    },
                )
                .await?;
                Ok(())
            },
        )
        .await?;

    for (body, vector) in [
        ("Alice prefers dark mode", [0.9, 0.1, 0.0]),
        ("Alice works in Shenzhen", [0.1, 0.9, 0.0]),
    ] {
        notes
            .add_from(&Note {
                _id: 0,
                owner: "alice".into(),
                body: body.into(),
                embedding: Some(vector_from_f32(vector.to_vec())),
            })
            .await?;
    }

    let owner = Filter::Field(("owner".into(), RangeQuery::Eq(Fv::Text("alice".into()))));
    let hits: Vec<Note> = notes
        .search_as(Query {
            search: Some(Search {
                text: Some("dark mode".into()),
                vector: Some(vec![0.8, 0.2, 0.0]),
                ..Default::default()
            }),
            filter: Some(owner.clone()),
            limit: Some(5),
        })
        .await?;
    assert_eq!(hits[0].body, "Alice prefers dark mode");

    // Newest-first page of ids matching a B-Tree filter.
    let newest = notes.query_last_ids(owner, Some(10)).await?;
    assert_eq!(newest.len(), 2);

    db.close().await?;
    Ok(())
}
```

For local files, wrap `LocalFileSystem` in
[`anda_object_store::MetaStoreBuilder`](../anda_object_store): the native
filesystem backend does not support the conditional updates the database
relies on. The workspace [README](../../README.md#quick-start-embedded-database)
shows that setup, and the bundled example exercises tokenizers, filters and
vector search end to end:

```bash
cargo run -p anda_db --example db_demo --features full
```

The demo writes to `./debug/metastore`.

## Feature flags

| Feature | Effect                                                                    |
| ------- | ------------------------------------------------------------------------- |
| `full`  | Enables `object_store/fs` for the local filesystem backend and the demo.  |

BM25 tokenization, including Jieba, is always compiled in: every collection
installs `default_tokenizer()` when it opens.

## Operating rules

- **One live writer per database namespace.** Clone `AndaDB` and
  `Arc<Collection>` handles for concurrent tasks. `DBConfig::lock` is a
  stored token check, not a distributed writer lease.
- **Await mutations to completion.** `add`, `update`, `remove`, `flush` and
  `close` must not be cancelled midway; a dropped mutation can poison the
  collection handle, and the database reopens it through recovery.
- **Configure in the open callback.** Install tokenizers and index hooks at
  the start of the callback, before recovery-triggering operations. An already
  open (cached) handle skips the callback; call
  `db.close_collection(name).await?` before reopening with a different
  configuration.
- **Indexes**: filters name B-Tree indexes, and `_id` is queryable without
  one. Multi-field B-Tree indexes are always unique and support tuple
  equality, not tuple-ordered ranges.
- **Vectors**: store `Vector` values built with `vector_from_f32`, query with
  `Vec<f32>`, and set the HNSW `dimension` and `distance_metric` to match the
  embedding model (the default metric is Euclidean).
- **Schema upgrades** require a higher schema version; new fields must be
  optional, and persisted field indexes are preserved.
- **Storage settings** (`StorageConfig`) are fixed when a database is first
  initialized; passing a different startup configuration later does not
  change them.

Heavy async APIs return boxed futures
(`fn … -> impl Future<Output = …> + Send`) so that debug builds fit the stack
of a spawned thread; callers simply `.await` them.

## Testing and benchmarks

```bash
cargo test -p anda_db --all-features
cargo test -p anda_db --test crash_recovery --test format_compat
cargo bench -p anda_db --bench core_workloads
```

`format_compat` reads checked-in fixtures written by earlier releases;
`crash_recovery` drives fault injection through
`anda_object_store::FaultStore`. `ANDA_BENCH_DOCS` and
`ANDA_BENCH_ITERATIONS` scale the benchmark. See the
[testing guide](../../docs/testing.md).

## Technical reference

- [docs/anda_db.md](../../docs/anda_db.md): lifecycle, indexing, query
  model, durability and recovery
- [docs/anda_db_schema.md](../../docs/anda_db_schema.md)
- [docs/anda_db_btree.md](../../docs/anda_db_btree.md),
  [docs/anda_db_tfs.md](../../docs/anda_db_tfs.md),
  [docs/anda_db_hnsw.md](../../docs/anda_db_hnsw.md)
- [docs/anda_object_store.md](../../docs/anda_object_store.md)
- [Agent-facing quick reference](../../skills/anda-db/references/anda_db_quick_ref.md)

## Related crates

- [`anda_db_schema`](../anda_db_schema) and
  [`anda_db_derive`](../anda_db_derive): field types, schemas, documents and
  derive macros (re-exported as `anda_db::schema`)
- [`anda_db_btree`](../anda_db_btree), [`anda_db_tfs`](../anda_db_tfs),
  [`anda_db_hnsw`](../anda_db_hnsw): the index engines
- [`anda_object_store`](../anda_object_store): metadata and encryption
  wrappers over `object_store`
- [`anda_db_server`](../anda_db_server): this database over HTTP
- [`anda_cognitive_nexus`](../anda_cognitive_nexus): a KIP knowledge graph
  built on this crate

## License

MIT. See [LICENSE](../../LICENSE).
