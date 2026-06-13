# AndaDB Quick Reference

This reference tracks the current Anda DB workspace API. Prefer it over older
examples that used unwrapped object stores, borrowed `Query` values, or direct
`ciborium` calls.

## Cargo Dependencies

```toml
[dependencies]
anda_db = { version = "0.8", features = ["full"] }
object_store = { version = "0.13", features = ["fs"] }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# Optional direct dependencies for low-level APIs
anda_db_hnsw = "0.8"
anda_object_store = "0.8"
cbor2 = "1"
```

## Common Imports

```rust
use anda_db::{
    collection::{Collection, CollectionConfig},
    database::{AndaDB, DBConfig},
    error::DBError,
    index::HnswConfig,
    query::{Filter, Query, RRFReranker, RangeQuery, Search},
    schema::{
        AndaDBSchema, FieldKey, FieldType, FieldValue, Fv, Json, Schema, Vector, vector_from_f32,
    },
    storage::StorageConfig,
};
use object_store::local::LocalFileSystem;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
```

Use `anda_db_hnsw::DistanceMetric` only when you need to override
`HnswConfig::distance_metric`.

## Database Lifecycle

```rust
let store = Arc::new(LocalFileSystem::new_with_prefix("./db")?);
let config = DBConfig {
    name: "agent_memory".into(),
    description: "Embedded AI memory".into(),
    storage: StorageConfig::default(),
    lock: None,
};

let db = AndaDB::connect(store.clone(), config.clone()).await?; // open or create
let db = AndaDB::create(store.clone(), config.clone()).await?;  // fail if exists
let db = AndaDB::open(store, config).await?;                    // fail if missing

db.flush().await?;
db.auto_flush(cancel_token, Duration::from_secs(30)).await?;
db.close().await?;
```

The object store argument is `Arc<dyn object_store::ObjectStore>`. For local
files, use `LocalFileSystem::new_with_prefix`.

## Collection Lifecycle

```rust
let collection = db
    .open_or_create_collection(
        MyDoc::schema()?,
        CollectionConfig {
            name: "docs".into(),
            description: "Searchable documents".into(),
        },
        async |c| {
            c.create_btree_index_nx(&["tenant"]).await?;
            c.create_bm25_index_nx(&["title", "body"]).await?;
            c.create_hnsw_index_nx(
                "embedding",
                HnswConfig {
                    dimension: 384,
                    ..Default::default()
                },
            )
            .await?;
            Ok(())
        },
    )
    .await?;

let existing = db.open_collection("docs".to_string(), async |_c| Ok(())).await?;
db.delete_collection("docs").await?;
```

Use `create_collection` when creation must fail if the collection already
exists. Use `open_or_create_collection` for normal service startup.

## Schema Derive Example

```rust
#[derive(Debug, Clone, Serialize, Deserialize, AndaDBSchema)]
struct Article {
    _id: u64,
    /// Article title.
    title: String,
    /// Article body text.
    body: String,
    /// Searchable embedding vector.
    embedding: Vector,
    /// Publication status.
    status: Option<String>,
    #[unique]
    slug: String,
    #[field_type = "Bytes"]
    hash: [u8; 32],
}
```

`_id: u64` is the reserved document id field. Field doc comments become schema
descriptions. `#[serde(rename = "...")]` controls the schema field name.

## Document Operations

```rust
let id = collection.add_from(&article).await?;
let loaded: Article = collection.get_as(id).await?;
let raw = collection.get(id).await?;

let mut fields = BTreeMap::new();
fields.insert("status".to_string(), Fv::Text("published".into()));
let updated = collection.update(id, fields).await?;

let removed = collection.remove(id).await?;
```

`add_from` serializes a typed value through the collection schema. Use
`collection.add(document)` only when you already have a schema-valid
`anda_db::schema::Document`.

## Query Building

```rust
// Simple text search
let q = Query {
    search: Some(Search {
        text: Some("query".into()),
        ..Default::default()
    }),
    limit: Some(10),
    ..Default::default()
};

// Vector search. Query vectors are Vec<f32>.
let q = Query {
    search: Some(Search {
        vector: Some(vec![0.1_f32; 384]),
        ..Default::default()
    }),
    limit: Some(10),
    ..Default::default()
};

// Hybrid text + vector search with RRF reranking.
let q = Query {
    search: Some(Search {
        text: Some("query".into()),
        vector: Some(vec![0.1_f32; 384]),
        reranker: Some(RRFReranker::default()),
        ..Default::default()
    }),
    limit: Some(20),
    ..Default::default()
};

// Filtered search. Filter values are Fv / FieldValue.
let q = Query {
    search: Some(Search {
        text: Some("query".into()),
        ..Default::default()
    }),
    filter: Some(Filter::Field((
        "status".into(),
        RangeQuery::Eq(Fv::Text("active".into())),
    ))),
    limit: Some(10),
};

let results: Vec<Article> = collection.search_as(q).await?;
```

`collection.search(query)`, `collection.search_as::<T>(query)`, and
`collection.search_ids(query)` all take `Query` by value.

## RangeQuery Variants

```rust
RangeQuery::Eq(value)           // equal
RangeQuery::Gt(value)           // greater than
RangeQuery::Ge(value)           // greater than or equal
RangeQuery::Lt(value)           // less than
RangeQuery::Le(value)           // less than or equal
RangeQuery::Between(lo, hi)     // inclusive range
RangeQuery::Include(values)     // set membership
RangeQuery::And(queries)        // all boxed range conditions
RangeQuery::Or(queries)         // any boxed range condition
RangeQuery::Not(query)          // negated boxed range condition
```

## Filter Combinators

```rust
Filter::Field(("field".into(), RangeQuery::Eq(Fv::Text("value".into()))))
Filter::And(vec![Box::new(filter1), Box::new(filter2)])
Filter::Or(vec![Box::new(filter1), Box::new(filter2)])
Filter::Not(Box::new(filter))
```

The field name in `Filter::Field` should match a B-Tree index virtual field.
For compound B-Tree indexes, use the virtual field name produced by the index
configuration.

## HnswConfig Options

| Option | Default | Description |
|--------|---------|-------------|
| `dimension` | `512` | Vector dimension |
| `max_layers` | `16` | Maximum graph layers |
| `max_connections` | `32` | HNSW M parameter |
| `ef_construction` | `200` | Build-time candidate count |
| `ef_search` | `50` | Search-time candidate count |
| `distance_metric` | `Euclidean` | Distance function |

```rust
use anda_db_hnsw::DistanceMetric;

let config = HnswConfig {
    dimension: 384,
    distance_metric: DistanceMetric::Cosine,
    ..Default::default()
};
```

`Search::vector` is always `Vec<f32>`. Stored vector fields should be
`Vector`/`Vec<bf16>` and are usually created with `vector_from_f32`.

## Distance Metrics

```rust
DistanceMetric::Euclidean    // L2 distance
DistanceMetric::Cosine       // 1 - cosine similarity
DistanceMetric::InnerProduct // negative dot product
DistanceMetric::Manhattan    // L1 distance
```

All metrics are distances, so smaller is more similar.

## StorageConfig Defaults

```rust
StorageConfig {
    cache_max_capacity: 10000,          // number of cached items; 0 disables cache
    compress_level: 3,                  // zstd level; 0 disables compression
    object_chunk_size: 256 * 1024,      // 256 KiB
    max_small_object_size: 2000 * 1024, // 2 MiB
    bucket_overload_size: 1024 * 1024,  // 1 MiB
}
```

## Error Handling

```rust
use anda_db::error::DBError;

match collection.add_from(&doc).await {
    Ok(id) => println!("added: {id}"),
    Err(DBError::AlreadyExists { name, path, .. }) => {
        println!("already exists: {name} at {path}");
    }
    Err(err) => return Err(err.into()),
}
```

## CBOR Serialization

```rust
use cbor2::{from_reader, serialized_size, to_writer};

let mut buf = Vec::new();
to_writer(&field_value, &mut buf)?;
let encoded_len = serialized_size(&field_value)?;
let decoded: FieldValue = from_reader(buf.as_slice())?;
```

Use `cbor2::serialized_size` instead of the removed `cbor_size` module. Do not
add new direct `ciborium` usage.

## Object Store Backends

```rust
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;

let memory_store = Arc::new(InMemory::new());
let local_store = Arc::new(LocalFileSystem::new_with_prefix("./db")?);
```

Other backends such as S3, GCS, Azure Blob, and HTTP are available through
`object_store` features selected by the embedding application.

## Object Store Wrappers

```rust
use anda_object_store::{EncryptedStoreBuilder, MetaStoreBuilder};
use object_store::local::LocalFileSystem;

let local = LocalFileSystem::new_with_prefix("./encrypted-db")?;

let metastore = MetaStoreBuilder::new(local, 10000).build();

let encrypted = EncryptedStoreBuilder::with_secret(metastore, 10000, [0_u8; 32])
    .with_chunk_size(1024 * 1024)
    .with_conditional_put()
    .build();
```

Enable `EncryptedStoreBuilder::with_conditional_put()` for local-file backed
encrypted deployments that need portable compare-and-swap semantics.

## Async Runtime Setup

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
```

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
}
```

## Build Checks

```bash
cargo check --workspace --all-features
cargo test --workspace --all-features
cargo run -p anda_db --example db_demo --features full
```
