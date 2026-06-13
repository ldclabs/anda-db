---
name: anda-db
description: Use this skill whenever you need to help users work with AndaDB in Rust programs or update AndaDB repository documentation. This includes creating databases and collections, deriving schemas, CRUD operations, B-Tree filters, BM25 full-text search, HNSW vector search, hybrid retrieval, object-store persistence, encrypted storage, KIP/Cognitive Nexus integration, and avoiding outdated ciborium-era examples. Invoke it when users mention AndaDB, AI agent memory, embedding storage, hybrid search, KIP, or Cognitive Nexus in this repository.
---

# AndaDB Skill

This skill provides source-aligned guidance for working with the current Anda DB
workspace. Prefer the patterns below over older examples that used direct
`AndaDB::create(store, ...)`, unwrapped `LocalFileSystem`, `Vec<bf16>` literals,
or `ciborium`.

## Quick Start Template

```rust
use anda_db::{
    collection::CollectionConfig,
    database::{AndaDB, DBConfig},
    index::HnswConfig,
    query::{Query, Search},
    schema::{AndaDBSchema, Vector, vector_from_f32},
    storage::StorageConfig,
};
use object_store::local::LocalFileSystem;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, AndaDBSchema)]
struct Memory {
    _id: u64,
    title: String,
    body: String,
    embedding: Vector,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(LocalFileSystem::new_with_prefix("./db")?);

    let db = AndaDB::connect(
        store,
        DBConfig {
            name: "agent_memory".into(),
            description: "Embedded AI memory".into(),
            storage: StorageConfig::default(),
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
                c.create_bm25_index_nx(&["title", "body"]).await?;
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
            _id: 0,
            title: "Rust".into(),
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
            limit: Some(10),
            ..Default::default()
        })
        .await?;

    let loaded: Memory = memories.get_as(id).await?;
    println!("Loaded {}, found {}", loaded.title, results.len());

    db.close().await?;
    Ok(())
}
```

## Core Dependencies

```toml
[dependencies]
anda_db = { version = "0.8", features = ["full"] }
object_store = { version = "0.13", features = ["fs"] }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

Add direct low-level crates only when using their public APIs directly:

```toml
anda_db_hnsw = "0.8"       # e.g. DistanceMetric
anda_object_store = "0.8" # MetaStoreBuilder / EncryptedStoreBuilder
cbor2 = "1"               # direct CBOR values, readers, writers, size
```

## Current API Rules

- Use `Arc<dyn object_store::ObjectStore>` when opening a database.
- Prefer `AndaDB::connect` for application startup; use `create` or `open` only
  when the failure mode matters.
- Prefer `open_or_create_collection` plus `_nx` index creation methods for
  idempotent startup.
- Use `add_from(&typed_value)`, `get_as::<T>(id)`, and `search_as::<T>(query)`
  for typed application structs.
- `search_as`, `search`, and `search_ids` take `Query` by value, not `&Query`.
- Stored vector fields use `anda_db::schema::Vector`; query vectors use
  `Vec<f32>`. Convert stored vectors with `vector_from_f32`.
- For filters, wrap values in `Fv`/`FieldValue`, for example
  `RangeQuery::Eq(Fv::Text("active".into()))`.

## Type Mapping (Rust -> AndaDB)

| Rust Type | AndaDB FieldType |
|-----------|------------------|
| `bool` | `Bool` |
| `i8` through `i64`, `isize` | `I64` |
| `u8` through `u64`, `usize` | `U64` |
| `f32` | `F32` |
| `f64` | `F64` |
| `String`, `&str` | `Text` |
| `Vec<u8>`, `[u8; N]` | `Bytes` |
| `Vector` / `Vec<bf16>` | `Vector` |
| `Vec<T>` | `Array(T)` |
| `BTreeMap<String, V>` / `HashMap<String, V>` | `Map(String, V)` |
| `Option<T>` | `Option(T)` |
| `serde_json::Value` | `Json` |

For arbitrary binary payloads serialized through Serde, use explicit bytes
types such as `FieldValue::Bytes` or Serde byte helpers where appropriate. Do
not assume every `Vec<u8>` inside a nested arbitrary type will be treated as a
CBOR byte string.

## Derive Macro Attributes

```rust
use anda_db::schema::{AndaDBSchema, Vector};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, AndaDBSchema)]
struct Doc {
    _id: u64,                 // reserved document id field
    #[serde(rename = "name")] // schema field name follows serde rename
    title: String,
    #[field_type = "Bytes"]   // override inferred field type
    #[unique]                 // enforce uniqueness when indexed
    checksum: [u8; 32],
    /// Used by LLM context and schema documentation.
    embedding: Vector,
}
```

## Index Types

Create indexes during `open_or_create_collection` startup. Prefer `_nx` methods
when startup can run repeatedly.

**HNSW (vector search)**

```rust
c.create_hnsw_index_nx(
    "embedding",
    HnswConfig {
        dimension: 384,
        ..Default::default()
    },
)
.await?;
```

If you need a non-default distance metric, add `anda_db_hnsw` as a direct
dependency and set `distance_metric: anda_db_hnsw::DistanceMetric::Cosine`.

**B-Tree (exact match / range filters)**

```rust
c.create_btree_index_nx(&["category"]).await?;
c.create_btree_index_nx(&["tenant", "created_at"]).await?;
```

**BM25 (full-text search)**

```rust
c.create_bm25_index_nx(&["title", "body"]).await?;
```

For Chinese tokenization with the `full` feature, see
`rs/anda_db/examples/db_demo.rs` for `jieba_tokenizer()`.

## Document CRUD

```rust
use anda_db::schema::Fv;
use std::collections::BTreeMap;

let id = collection.add_from(&doc).await?;
let loaded: MyDoc = collection.get_as(id).await?;

let mut fields = BTreeMap::new();
fields.insert("status".to_string(), Fv::Text("archived".into()));
let updated = collection.update(id, fields).await?;

let removed = collection.remove(id).await?;
```

Use `collection.add(document)` only when you are already working with an
`anda_db::schema::Document`.

## Search with Filters

```rust
use anda_db::query::{Filter, Query, RRFReranker, RangeQuery, Search};
use anda_db::schema::Fv;

let results: Vec<MyDoc> = collection
    .search_as(Query {
        search: Some(Search {
            text: Some("query text".into()),
            vector: Some(vec![0.1_f32; 384]),
            reranker: Some(RRFReranker::default()),
            ..Default::default()
        }),
        filter: Some(Filter::Field((
            "category".into(),
            RangeQuery::Eq(Fv::Text("news".into())),
        ))),
        limit: Some(20),
    })
    .await?;
```

## CBOR Rules

Use `cbor2` directly:

```rust
use cbor2::{from_reader, serialized_size, to_writer};

let mut buf = Vec::new();
to_writer(&value, &mut buf)?;
let size = serialized_size(&value)?;
let decoded = from_reader(buf.as_slice())?;
```

Do not introduce new direct `ciborium` usage. The old `cbor_size` helper module
has been replaced by `cbor2::serialized_size`.

## Workspace Crates

| Crate | Purpose |
|-------|---------|
| `anda_db` | Core embedded database |
| `anda_db_schema` | Schema, field values, documents |
| `anda_db_derive` | Derive macros |
| `anda_db_btree` | Exact-match and range index |
| `anda_db_tfs` | BM25 full-text search |
| `anda_db_hnsw` | HNSW vector index |
| `anda_db_utils` | Shared utilities |
| `anda_object_store` | Metadata and encrypted object-store wrappers |
| `anda_kip` | Knowledge Interaction Protocol |
| `anda_cognitive_nexus` | Reference AI memory graph runtime |
| `anda_db_server` | HTTP server for core database APIs |
| `anda_cognitive_nexus_server` | HTTP/JSON-RPC server for Cognitive Nexus |
| `anda_db_shard_proxy` | Shard proxy for multi-tenant deployments |

## Reference Documentation

Read these before making broad API or documentation changes:

- `references/anda_db_quick_ref.md` - compact API reference for agents
- `README.md` - workspace overview and current quick start
- `docs/anda_db.md` - core database design and usage
- `docs/anda_db_schema.md` - field types, values, schemas, documents
- `docs/anda_db_derive.md` - derive macro behavior
- `docs/anda_db_btree.md` - B-Tree index behavior
- `docs/anda_db_tfs.md` - BM25 full-text engine
- `docs/anda_db_hnsw.md` - HNSW vector search
- `docs/anda_object_store.md` - metadata and encryption wrappers
- `docs/anda_kip.md` - KIP protocol
- `docs/anda_cognitive_nexus.md` - Cognitive Nexus runtime
