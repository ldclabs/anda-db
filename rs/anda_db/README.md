# anda_db

`anda_db` is the core crate of **Anda DB**: an embedded Rust database for AI agents that stores documents, builds multiple index types, and runs structured queries and hybrid retrieval.

It is designed to be linked into your application (not a hosted database service) and persists data via the [`object_store`](https://docs.rs/object_store) ecosystem.

## Highlights

- **Embedded & async:** Integrate as a Rust library; built around `tokio`.
- **Object-store persistence:** Use local filesystem, S3-compatible storage, in-memory, etc.
- **Document schema:** Define typed documents via `AndaDBSchema` derive.
- **Indexes:**
  - **B-Tree** for exact match and range filters (powered by [`anda_db_btree`](https://docs.rs/anda_db_btree)).
  - **BM25** for full-text retrieval (powered by [`anda_db_tfs`](https://docs.rs/anda_db_tfs), requires feature `full`).
  - **HNSW** for ANN vector search (powered by [`anda_db_hnsw`](https://docs.rs/anda_db_hnsw)).
- **Hybrid search:** Combine BM25 + vector search with RRF.
- **Encryption (optional):** Wrap storage with [`anda_object_store`](https://docs.rs/anda_object_store) for metadata + AES-256-GCM at rest.

## Installation

Add dependencies (pick the object_store backend you need):

```toml
[dependencies]
anda_db = "0.7"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }

# Required to provide an ObjectStore implementation:
object_store = { version = "0.13", features = ["fs"] }

# Optional (recommended for local filesystem): adds metadata + conditional put support
anda_object_store = "0.3"
```

### Feature flags

- `default`: no extra features
- `full`: enables full-text search dependencies (BM25 via Tantivy + jieba integration)

If you want BM25 / hybrid search, enable:

```toml
anda_db = { version = "0.7", features = ["full"] }
```

## Quickstart (minimal, in-memory)

This example creates a collection with an HNSW vector index and performs a vector search.

```rust
use anda_db::{
    collection::CollectionConfig,
    database::{AndaDB, DBConfig},
    error::DBError,
    index::HnswConfig,
    query::{Query, Search},
    schema::{AndaDBSchema, Vector, vector_from_f32},
};
use object_store::memory::InMemory;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, AndaDBSchema)]
pub struct Doc {
    pub _id: u64,
    pub text: String,
    pub embedding: Vector,
}

#[tokio::main]
async fn main() -> Result<(), DBError> {
    let db = AndaDB::connect(Arc::new(InMemory::new()), DBConfig::default()).await?;

    let collection = db
        .open_or_create_collection(
            Doc::schema()?,
            CollectionConfig {
                name: "docs".to_string(),
                description: "Demo documents".to_string(),
            },
            async |c| {
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

    collection
        .add_from(&Doc {
            _id: 0,
            text: "Rust is focused on safety.".to_string(),
            embedding: vector_from_f32(vec![0.1, 0.2, 0.3, 0.4]),
        })
        .await?;
    collection.flush(anda_db::unix_ms()).await?;

    let hits: Vec<Doc> = collection
        .search_as(Query {
            search: Some(Search {
                vector: Some(vec![0.1, 0.2, 0.3, 0.4]),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await?;

    println!("hits={}", hits.len());
    db.close().await?;
    Ok(())
}
```

## Full demo (persistent, BM25 + HNSW + hybrid)

The repo includes a complete runnable example:

- Source: [examples/db_demo.rs](examples/db_demo.rs)
- Run: `cargo run -p anda_db --example db_demo --features full`

Notes:

- BM25 / hybrid search requires feature `full`.
- For better tokenization (especially CJK), set a tokenizer (the demo uses `anda_db_tfs::jieba_tokenizer`).

## Storage backends

`AndaDB::connect` accepts any `Arc<dyn object_store::ObjectStore>`.

### Local filesystem (recommended)

For local filesystem, wrapping with `anda_object_store::MetaStoreBuilder` improves correctness/performance by providing metadata and conditional put support:

```rust
use anda_object_store::MetaStoreBuilder;
use object_store::local::LocalFileSystem;

let store = MetaStoreBuilder::new(LocalFileSystem::new_with_prefix("./db")?, 10000).build();
```

### Encryption at rest (optional)

See the `EncryptedStoreBuilder` examples in [../anda_object_store/README.md](../anda_object_store/README.md).

## License

Copyright © 2025 [LDC Labs](https://github.com/ldclabs).

`ldclabs/anda-db` is licensed under the MIT License. See [LICENSE](../../LICENSE) for details.
