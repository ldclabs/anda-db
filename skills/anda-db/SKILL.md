---
name: anda-db
description: Use this skill whenever you need to help users work with AndaDB in Rust programs. This skill is designed for tasks like: adding AndaDB to a Rust project, creating databases and collections, implementing document CRUD operations, setting up vector search (HNSW), full-text search (BM25), or B-Tree indexes, configuring storage backends, and implementing search with filters. Make sure to invoke this skill when users mention AndaDB, document databases for AI agents, vector search, or embedding storage in Rust projects.
---

# AndaDB Skill

This skill provides comprehensive guidance for working with AndaDB in Rust programs.

## Quick Start Template

```rust
use anda_db::{AndaDB, DBConfig, CollectionConfig};
use anda_db::schema::AndaDBSchema;
use anda_db::index::{HnswConfig, BTreeConfig};
use anda_db::query::{Query, Search, Filter, RangeQuery};
use anda_db::storage::StorageConfig;
use anda_db_hnsw::DistanceMetric;
use object_store::local::LocalFileSystem;
use serde::{Serialize, Deserialize};
use std::sync::Arc;

#[derive(AndaDBSchema, Serialize, Deserialize)]
struct MyDoc {
    _id: u64,
    title: String,
    content: String,
    embedding: Vec<bf16>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = LocalFileSystem::new("./data")?;
    let db = AndaDB::create(store, DBConfig {
        name: "my_db".into(),
        description: "".into(),
        storage: StorageConfig::default(),
        lock: None,
    }).await?;

    let collection = db.create_collection(
        Arc::new(MyDoc::schema()?),
        CollectionConfig {
            name: "docs".into(),
            description: "".into(),
        },
        |c| async move {
            c.create_hnsw_index("embedding", HnswConfig {
                dimension: 384,
                distance_metric: DistanceMetric::Cosine,
                ..Default::default()
            }).await?;
            Ok(())
        }
    ).await?;

    // CRUD operations (all async)
    let doc = MyDoc { _id: 1, title: "Hello".into(), content: "World".into(), embedding: vec![bf16::from_f32(0.1); 384] };
    collection.add(doc).await?;

    // Search (async)
    let results = collection.search_as::<MyDoc>(&Query {
        search: Some(Search { text: Some("hello".into()), ..Default::default() }),
        limit: Some(10),
        ..Default::default()
    }).await?;

    Ok(())
}
```

## Core Dependencies

```toml
[dependencies]
anda_db = { version = "0.7", features = ["full"] }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

## Type Mapping (Rust → AndaDB)

| Rust Type | AndaDB FieldType |
|-----------|------------------|
| `bool` | `Bool` |
| `i8-i64`, `isize` | `I64` |
| `u8-u64`, `usize` | `U64` |
| `f32` | `F32` |
| `f64` | `F64` |
| `String`, `&str` | `Text` |
| `Vec<u8>`, `[u8; N]` | `Bytes` |
| `Vec<bf16>` | `Vector` |
| `Vec<T>` | `Array(T)` |
| `HashMap<String, V>` | `Map(String, V)` |
| `Option<T>` | `Option(T)` |

## Derive Macro Attributes

```rust
#[derive(AndaDBSchema)]
struct Doc {
    _id: u64,                           // Auto-recognized as ID
    #[field_type = "Bytes"]             // Override inferred type
    #[unique]                           // Unique constraint
    #[serde(rename = "name")]         // Schema field name
    /// Description for LLM context     // Field description
    field: String,
}
```

## Index Types

**HNSW (Vector Search)**:
```rust
c.create_hnsw_index("embedding", HnswConfig {
    dimension: 384,
    max_connections: 16,
    ef_construction: 100,
    ef_search: 50,
    distance_metric: DistanceMetric::Cosine,
    ..Default::default()
}).await?;
```

**BTree (Exact Match/Range)**:
```rust
c.create_btree_index(["category", "status"]).await?;
```

**BM25 (Full-Text Search)**:
```rust
c.create_bm25_index(["title", "content"]).await?;
```

## Document CRUD (All Async)

```rust
// Create
let id = collection.add(doc).await?;

// Read
let doc = collection.get(id).await?;

// Update
collection.update(id, fields).await?;

// Delete
collection.remove(id).await?;
```

## Search with Filters

```rust
let query = Query {
    search: Some(Search {
        text: Some("query".into()),
        vector: Some(vec![0.1_f32; 384]),
        reranker: Some(anda_db::index::RRFReranker { k: 60 }),
        ..Default::default()
    }),
    filter: Some(Filter::Field((
        "category".into(),
        RangeQuery::Eq("news".into())
    ))),
    limit: Some(20),
};
let results = collection.search_as::<MyDoc>(&query).await?;
```

## Workspace Crates

| Crate | Purpose |
|-------|---------|
| `anda_db` | Core database |
| `anda_db_schema` | Type system |
| `anda_db_derive` | Derive macros |
| `anda_db_btree` | B-Tree index |
| `anda_db_tfs` | BM25 full-text |
| `anda_db_hnsw` | HNSW vector index |
| `anda_object_store` | Storage + encryption |

## Reference Documentation

For detailed documentation, read:
- `references/anda_db_quick_ref.md` - Quick reference sheet
