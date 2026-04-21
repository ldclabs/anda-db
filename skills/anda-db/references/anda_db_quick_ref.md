# AndaDB Quick Reference

## Cargo Dependencies

```toml
[dependencies]
anda_db = { version = "0.7", features = ["full"] }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# Optional
anda_db_hnsw = "0.4"    # Vector search
anda_object_store = "0.3" # Storage with encryption
```

## Common Imports

```rust
use anda_db::{AndaDB, DBConfig, CollectionConfig};
use anda_db::schema::{Schema, AndaDBSchema, FieldType, FieldValue};
use anda_db::index::{HnswConfig, BTreeConfig, BM25Config};
use anda_db::query::{Query, Search, Filter, RangeQuery};
use anda_db::storage::StorageConfig;
use anda_db_hnsw::DistanceMetric;
use anda_db::error::DBError;
use object_store::local::LocalFileSystem;
use bf16::bf16;
use serde::{Serialize, Deserialize};
use std::sync::Arc;
```

## AndaDB Lifecycle

```rust
// Create new database
let db = AndaDB::create(store, db_config).await?;

// Connect (create if not exists)
let db = AndaDB::connect(store, db_config).await?;

// Open (fails if doesn't exist)
let db = AndaDB::open(store, db_config).await?;

// Persistence
db.flush().await?;
db.auto_flush(cancel_token, Duration::from_secs(30)).await?;
```

## Collection Lifecycle

```rust
// Create collection
let collection = db.create_collection(schema, config, |c| async {
    c.create_hnsw_index("embedding", hnsw_config).await?;
    Ok(())
}).await?;

// Open existing collection
let collection = db.open_collection("name", |c| async { Ok(()) }).await?;

// Delete collection
db.delete_collection("name").await?;
```

## Document Operations (All Async)

```rust
// Add document (returns ID)
let id = collection.add(doc).await?;

// Get document
let doc = collection.get(id).await?;

// Update fields
collection.update(id, fields).await?;

// Remove document
collection.remove(id).await?;
```

## Query Building

```rust
// Simple text search
Query {
    search: Some(Search {
        text: Some("query".into()),
        ..Default::default()
    }),
    limit: Some(10),
    ..Default::default()
}

// Vector search
Query {
    search: Some(Search {
        vector: Some(query_vector),
        ..Default::default()
    }),
    ..Default::default()
}

// Hybrid with RRF reranking
Query {
    search: Some(Search {
        text: Some("query".into()),
        vector: Some(query_vector),
        reranker: Some(RRFReranker { k: 60 }),
        ..Default::default()
    }),
    limit: Some(20),
    ..Default::default()
}

// With filter
Query {
    search: Some(Search { text: Some("query".into()), ..Default::default() }),
    filter: Some(Filter::Field(("status".into(), RangeQuery::Eq("active".into())))),
    ..Default::default()
}
```

## RangeQuery Variants

```rust
RangeQuery::Eq(value)           // Equal
RangeQuery::Gt(value)           // Greater than
RangeQuery::Ge(value)           // Greater than or equal
RangeQuery::Lt(value)           // Less than
RangeQuery::Le(value)           // Less than or equal
RangeQuery::Between(lo, hi)     // Between (inclusive)
RangeQuery::In(values)          // In set
```

## Filter Combinators

```rust
Filter::Field(("field".into(), RangeQuery::Eq("value".into())))
Filter::And(vec![filter1, filter2])
Filter::Or(vec![filter1, filter2])
Filter::Not(Box::new(filter))
```

## HnswConfig Options

| Option | Default | Description |
|--------|---------|-------------|
| `dimension` | 512 | Vector dimension |
| `max_layers` | 16 | Max graph layers |
| `max_connections` | 32 | M parameter |
| `ef_construction` | 200 | Build-time candidates |
| `ef_search` | 50 | Search-time candidates |
| `distance_metric` | Euclidean | Distance function |

## Distance Metrics

```rust
use anda_db_hnsw::DistanceMetric;

DistanceMetric::Euclidean    // L2 distance
DistanceMetric::Cosine      // 1 - cosine similarity
DistanceMetric::InnerProduct // Negative dot product
DistanceMetric::Manhattan   // L1 distance
```

## StorageConfig Options

```rust
StorageConfig {
    cache_max_capacity: 1024 * 1024 * 1024, // 1GB
    compress_level: 3,                        // Zstd level
    object_chunk_size: 8 * 1024 * 1024,     // 8MB
    max_small_object_size: 64 * 1024,       // 64KB
    bucket_overload_size: 512 * 1024,       // 512KB
}
```

## Error Handling

```rust
use anda_db::error::DBError;

match collection.add(doc).await {
    Ok(id) => println!("Added: {}", id),
    Err(DBError::AlreadyExists { path, id }) => println!("Already exists: {} at {}", id, path),
    Err(e) => return Err(e.into()),
}
```

## Schema Derive Example

```rust
use anda_db::schema::AndaDBSchema;

#[derive(AndaDBSchema, Serialize, Deserialize)]
struct Article {
    _id: u64,
    /// Article title
    title: String,
    /// Article body text
    content: String,
    /// Searchable embedding vector
    embedding: Vec<bf16>,
    /// Publication status
    status: Option<String>,
    #[unique]
    slug: String,
}
```

## CBOR Serialization

```rust
use ciborium::{cbor, from_reader, into_writer};

let mut buf = Vec::new();
into_writer(&field_value, &mut buf)?;
let decoded: FieldValue = from_reader(buf.as_slice())?;
```

## Object Store Backends

```rust
// In-memory
object_store::memory::InMemory::new();

// Local filesystem
object_store::local::LocalFileSystem::new("path")?;

// S3
object_store::aws::AmazonS3::new_from_environment()?;
```

## Encrypted Storage

```rust
use anda_object_store::{EncryptedStoreBuilder, MetaStoreBuilder};

let store = EncryptedStoreBuilder::with_secret(
    local_fs,
    1000,               // cache capacity
    secret_key,          // [u8; 32]
)
.with_chunk_size(1024 * 1024)  // 1MB chunks
.with_conditional_put()
.build()?;
```

## Async Runtime Setup

```rust
[dependencies]
tokio = { version = "1", features = ["full"] }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // code here
    Ok(())
}
```
