# AndaDB - AI Agent Database

Anda DB is a modular Rust workspace for embedded AI memory systems. The core
crate is a schema-aware document database with B-Tree, BM25, and HNSW retrieval,
backed by the `object_store` abstraction.

## Project Structure

```text
rs/
├── anda_db/                      # Core embedded database library
├── anda_db_schema/               # Schema, FieldType, FieldValue, Document
├── anda_db_derive/               # Derive macros: AndaDBSchema, FieldTyped
├── anda_db_btree/                # Exact-match and range index
├── anda_db_tfs/                  # BM25 full-text search
├── anda_db_hnsw/                 # HNSW vector index
├── anda_db_utils/                # Shared utilities
├── anda_object_store/            # Metadata and encrypted object-store wrappers
├── anda_kip/                     # Knowledge Interaction Protocol
├── anda_cognitive_nexus/         # Reference KIP memory graph runtime
├── anda_db_server/               # HTTP server for core database APIs
├── anda_cognitive_nexus_server/  # HTTP/JSON-RPC server for Cognitive Nexus
└── anda_db_shard_proxy/          # Shard proxy for multi-tenant deployments

py/
└── anda_cognitive_nexus_py/      # Python binding crate, excluded from workspace by default
```

## Working with AndaDB

When writing Rust code that uses AndaDB, read the local skill first:

```text
skills/anda-db/SKILL.md
```

That skill contains the current API patterns for:

- database and collection lifecycle
- typed schema derivation
- document CRUD
- B-Tree, BM25, and HNSW indexes
- hybrid search with filters
- storage backends and object-store wrappers
- CBOR serialization conventions

## Key Dependencies

For embedded database usage, start with:

```toml
anda_db = { version = "0.8", features = ["full"] }
object_store = { version = "0.13", features = ["fs"] }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

Use `cbor2` for CBOR work. Do not add new direct `ciborium` usage. Use
`cbor2::serialized_size` when encoded-size calculation is needed.

## Documentation

- [Main documentation](docs/README.md)
- [Core database](docs/anda_db.md)
- [Schema model](docs/anda_db_schema.md)
- [Derive macros](docs/anda_db_derive.md)
- [B-Tree index](docs/anda_db_btree.md)
- [BM25 full-text search](docs/anda_db_tfs.md)
- [HNSW vector search](docs/anda_db_hnsw.md)
- [Object store wrappers](docs/anda_object_store.md)
- [KIP](docs/anda_kip.md)
- [Cognitive Nexus](docs/anda_cognitive_nexus.md)

## Quick Start

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

    memories
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

    println!("Found {} results", results.len());
    db.close().await?;
    Ok(())
}
```

## Building and Testing

```bash
cargo check --workspace --all-features
cargo test --workspace --all-features
cargo test -p anda_db --lib
cargo run -p anda_db --example db_demo --features full
```

The Python binding under `py/anda_cognitive_nexus_py` is not part of the default
Rust workspace member list.
