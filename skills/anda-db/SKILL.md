---
name: anda-db
description: Build and maintain Rust applications using AndaDB, including schemas, document CRUD, indexed retrieval, persistence, and object-store wrappers. Use when updating AndaDB examples or API documentation, or integrating this repository's KIP and Cognitive Nexus engines.
---

# AndaDB

Use the checked-out source as the authority for API signatures and behavior.
This skill targets the workspace's 0.14 release family (Rust edition 2024,
MSRV 1.95). Crate patch versions can diverge; check each `Cargo.toml` when
pinning versions. Examples use the public `anda_db` re-exports.

## Choose the relevant reference

- [Core API](references/anda_db_quick_ref.md): collection lifecycle, CRUD,
  B-Tree/BM25/HNSW indexes, filtered search, pagination, and maintenance.
- [Schemas and CBOR](references/schema_and_cbor.md): nested `FieldTyped`
  structs, type overrides, serialization, and schema upgrades.
- [Storage and recovery](references/storage_and_recovery.md): local or cloud
  backends, encryption, cache budgets, cancellation, and graceful shutdown.
- [KIP and Cognitive Nexus](references/kip_and_nexus.md): protocol versus
  engine APIs, the TypeScript engine, host integration, and conformance checks.

Read the reference needed for the task; ordinary document storage needs only
the core API. Repository links resolve relative to this skill's files; they
require an AndaDB checkout.

## Dependencies and features

For a local embedded application:

```toml
[package]
name = "anda-memory-example"
version = "0.1.0"
edition = "2024"
rust-version = "1.95"

[dependencies]
anda_db = { version = "0.14", features = ["full"] }
anda_object_store = "0.14"
object_store = { version = "0.14", features = ["fs"] }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

`anda_db/full` only enables `object_store/fs`. B-Tree, BM25, HNSW and Jieba
are already available without it. Add `anda_db_hnsw = "0.14"` only for direct
low-level types such as `DistanceMetric`; add `cbor2 = "1"` for direct CBOR
work and `tokio-util = "0.7"` for `CancellationToken` with `auto_flush`.

## Working example

```rust
use anda_db::{
    collection::CollectionConfig,
    database::{AndaDB, DBConfig},
    index::HnswConfig,
    query::{Filter, Query, RangeQuery, Search},
    schema::{AndaDBSchema, Fv, Vector, vector_from_f32},
    storage::StorageConfig,
};
use anda_object_store::MetaStoreBuilder;
use object_store::local::LocalFileSystem;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, AndaDBSchema)]
struct Memory {
    _id: u64,
    topic: String,
    body: String,
    embedding: Vector,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all("./db")?;
    let store = Arc::new(
        MetaStoreBuilder::new(
            LocalFileSystem::new_with_prefix("./db")?.with_fsync(true),
            10_000,
        )
        .build(),
    );
    let db = AndaDB::connect(
        store,
        DBConfig {
            name: "agent_memory".into(),
            description: "Embedded AI memory".into(),
            storage: StorageConfig::default().with_cache_max_bytes(64 * 1024 * 1024),
            lock: None,
        },
    )
    .await?;

    let memories = db
        .open_or_create_collection(
            Memory::schema()?,
            CollectionConfig {
                name: "memories".into(),
                description: "Long-term memory collection".into(),
            },
            async |c| {
                // Install a custom tokenizer/hooks here, before index creation.
                c.create_btree_index_nx(&["topic"]).await?;
                c.create_bm25_index_nx(&["topic", "body"]).await?;
                c.create_hnsw_index_nx(
                    "embedding",
                    HnswConfig {
                        dimension: 4,
                        ..Default::default()
                    },
                )
                .await?;
                Ok(())
            },
        )
        .await?;

    let id = memories
        .add_from(&Memory {
            _id: 0, // The collection allocates the real id.
            topic: "rust".into(),
            body: "Rust is well suited to embedded AI memory services.".into(),
            embedding: vector_from_f32(vec![0.1, 0.2, 0.3, 0.4]),
        })
        .await?;

    let results: Vec<Memory> = memories
        .search_as(Query {
            search: Some(Search {
                text: Some("embedded AI memory".into()),
                vector: Some(vec![0.1, 0.2, 0.3, 0.4]),
                ..Default::default()
            }),
            filter: Some(Filter::Field((
                "topic".into(),
                RangeQuery::Eq(Fv::Text("rust".into())),
            ))),
            limit: Some(10),
        })
        .await?;

    let loaded: Memory = memories.get_as(id).await?;
    println!("Loaded {}, found {}", loaded.topic, results.len());
    db.close().await?;
    Ok(())
}
```

## Essential constraints

- Share one live writer instance per database namespace, including within a
  process. Clone `AndaDB` / `Arc<Collection>` for concurrent tasks. `DBConfig::lock`
  is a stored token check, not a distributed writer lease.
- Await mutating operations to completion. Dropping an in-flight mutation can
  poison the collection; recover by reopening it through the database. See
  [cancellation and shutdown](references/storage_and_recovery.md#cancellation-and-shutdown).
- Configure indexes in the `&mut Collection` open callback. An already active
  cached handle is returned without running that callback again. Close with
  `db.close_collection(name).await?` before reopening to change configuration.
- Use stored `Vector` values via `vector_from_f32`; queries use `Vec<f32>`.
  Match the embedding model's dimension and metric explicitly.
- Filters name B-Tree indexes; `_id` is queryable without creating one.
  Multi-field B-Tree indexes are always unique and support tuple equality,
  not tuple-ordered ranges.
- Use `cbor2`; do not introduce direct `ciborium` usage. Encoded sizes use
  `cbor2::serialized_size`.

## Source and validation

Check implementations as well as prose when changing examples:

- [Core example](../../rs/anda_db/examples/db_demo.rs) and
  [core documentation](../../docs/anda_db.md).
- [Database lifecycle](../../rs/anda_db/src/database.rs),
  [collection operations](../../rs/anda_db/src/collection/), and
  [query types](../../rs/anda_db/src/query.rs).
- [Schema implementation](../../rs/anda_db_schema/src/),
  [derive implementation](../../rs/anda_db_derive/src/), and
  [storage wrappers](../../rs/anda_object_store/src/).

For documentation-only changes, compile/run changed runnable examples against
local path dependencies in a temporary project; Markdown code fences in this
skill are not Cargo doctests. For code changes, run affected crate tests, then
the workspace checks required by the task:

```bash
cargo check --workspace --all-features
cargo test --workspace --all-features
cargo run -p anda_db --example db_demo --features full
```

The demo creates data under `./debug/metastore`. KIP cross-engine checks are
listed in the [KIP reference](references/kip_and_nexus.md).
