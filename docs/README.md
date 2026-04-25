# AndaDB Technical Documentation

**Version**: 0.7.26
**Last Updated**: 2026-04-21

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Core Components](#3-core-components)
4. [Data Model](#4-data-model)
5. [Indexing System](#5-indexing-system)
6. [Query System](#6-query-system)
7. [Storage Layer](#7-storage-layer)
8. [Network Communication](#8-network-communication)
9. [Security Features](#9-security-features)
10. [KIP Protocol](#10-kip-protocol)
11. [Quick Start](#11-quick-start)
12. [API Reference](#12-api-reference)
13. [Performance Tuning](#13-performance-tuning)
14. [Best Practices](#14-best-practices)
15. [Detailed Documentation](#15-detailed-documentation)

---

## 1. Overview

### 1.1 What is AndaDB

AndaDB is an embedded Rust database designed for AI Agents, focusing on knowledge storage and memory management. It integrates traditional document database, vector database, and knowledge graph capabilities into a unified storage layer, providing persistent long-term memory for AI Agents.

### 1.2 Core Features

| Feature                  | Description                                                           |
| ------------------------ | --------------------------------------------------------------------- |
| **Multi-modal Indexing** | B-Tree (exact match), BM25 (full-text search), HNSW (vector search)   |
| **Hybrid Retrieval**     | Combine multiple retrieval methods using RRF (Reciprocal Rank Fusion) |
| **Knowledge Graph**      | Concept nodes and proposition links via KIP protocol                  |
| **Flexible Schema**      | Schema evolution and field type validation                            |
| **Object Storage**       | Local filesystem, S3-compatible, in-memory storage                    |
| **Optional Encryption**  | AES-256-GCM data encryption at rest                                   |
| **Async Design**         | Built on Tokio async runtime                                          |
| **Compressed Storage**   | Zstd compression support                                              |

### 1.3 Design Goals

1. **Sustainable AI Memory**: Provide persistent, queryable, updatable long-term memory for AI Agents
2. **Model-Friendly**: Syntax optimized for LLM to generate correct queries
3. **Self-Describing**: Schema information stored within the graph itself, supports introspection

---

## 2. Architecture

### 2.1 Overall Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Client Applications                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐    │
│  │ Cognitive Nexus │  │ Shard Proxy     │  │ Direct Access    │    │
│  │ (KIP Protocol)  │  │ (Shard Proxy)   │  │ (HTTP RPC)      │    │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘    │
└───────────┼────────────────────┼────────────────────┼──────────────┘
            │                    │                    │
            ▼                    ▼                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         AndaDB Core Layer                            │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │                        AndaDB                                │    │
│  │  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐ │    │
│  │  │ Collection│  │  Query   │  │  Index   │  │  Storage  │ │    │
│  │  │  Manager  │  │  Engine  │  │  Manager │  │   Layer   │ │    │
│  │  └───────────┘  └───────────┘  └───────────┘  └───────────┘ │    │
│  └─────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
            │                    │                    │
            ▼                    ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ anda_db_btree   │  │  anda_db_tfs   │  │ anda_db_hnsw   │
│ (B-Tree Index)   │  │  (BM25 Search) │  │ (HNSW Vector)  │
└─────────────────┘  └─────────────────┘  └─────────────────┘
```

### 2.2 Component Dependencies

```
anda_db (core)
├── anda_db_schema (type system)
│   └── anda_db_derive (derive macros)
├── anda_db_btree (B-Tree index)
├── anda_db_tfs (full-text search)
├── anda_db_hnsw (vector index)
├── anda_object_store (object storage)
│   └── encryption (AES-256-GCM encryption)
├── anda_kip (knowledge protocol)
│   └── anda_cognitive_nexus (cognitive nexus)
└── anda_db_server (HTTP server)
    └── anda_db_shard_proxy (shard proxy)
```

### 2.3 Data Flow

```
Write Data Flow:
  Client → Collection.add() → Schema validation → Index update → Storage persist

Read Data Flow:
  Client → Collection.get() → Storage read → Index query → Return result

Search Data Flow:
  Client → Query.search() → [BM25 | HNSW | BTree] → RRF fusion → Return results
```

---

## 3. Core Components

### 3.1 AndaDB (`anda_db`)

Main database type managing collections and storage.

**Key Methods**:

```rust
// Create or connect to database
let db = AndaDB::create(object_store, config).await?;
let db = AndaDB::connect(object_store, config).await?;

// Collection management
let collection = db.create_collection(schema, config, f).await?;
let collection = db.open_collection("my_collection", f).await?;
db.delete_collection("my_collection").await?;

// Persistence control
db.flush().await?;
db.auto_flush(cancel_token, Duration::from_secs(30)).await?;
```

### 3.2 Collection (`collection.rs`)

A logical grouping of documents with the same schema.

**Key Methods**:

```rust
// Document operations
collection.add(document)?;
collection.add_from(value)?;
collection.update(id, fields)?;
collection.upsert(document)?;
collection.remove(id)?;
let doc = collection.get(id)?;

// Index management
collection.create_btree_index(fields)?;
collection.create_bm25_index(fields)?;
collection.create_hnsw_index("embedding", hnsw_config)?;

// Search
let results = collection.search_as::<MyDoc>(&query)?;
```

### 3.3 Schema (`anda_db_schema`)

Defines document structure and field types.

**Supported Field Types**:

| Rust Type            | AndaDB Type | Description             |
| -------------------- | ----------- | ----------------------- |
| `String`, `&str`     | `Text`      | Text                    |
| `bool`               | `Bool`      | Boolean                 |
| `i8-i64`, `isize`    | `I64`       | 64-bit signed integer   |
| `u8-u64`, `usize`    | `U64`       | 64-bit unsigned integer |
| `f32`                | `F32`       | 32-bit floating point   |
| `f64`                | `F64`       | 64-bit floating point   |
| `Vec<u8>`            | `Bytes`     | Byte array              |
| `Vec<bf16>`          | `Vector`    | BF16 vector             |
| `Vec<T>`             | `Array`     | Array                   |
| `HashMap<String, T>` | `Map`       | Map                     |
| `Option<T>`          | `Option`    | Optional value          |
| `Json`               | `Json`      | JSON object             |

**Derive Macro Usage**:

```rust
use anda_db::schema::{AndaDBSchema, FieldType};

#[derive(AndaDBSchema, Debug, Serialize, Deserialize)]
struct MyDoc {
    _id: u64,                    // Auto-added, unique, default index 0
    title: String,
    content: String,
    embedding: Vec<bf16>,       // Vector field
    #[field_type = "U64"]
    timestamp: i64,
    #[unique]
    unique_key: String,
}

let schema = MyDoc::schema();
```

---

## 4. Data Model

### 4.1 Document Structure

```rust
struct Document {
    _id: u64,                    // Document unique ID (auto-generated)
    _type: String,               // Document type (Schema name)
    _created: u64,                // Creation timestamp (ms)
    _updated: u64,               // Update timestamp (ms)
    [other fields...]: FieldValue, // User-defined fields
}
```

### 4.2 Field Value Types

```rust
enum FieldValue {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    F32(f32),
    Text(String),
    Json(serde_json::Value),
    Bytes(Vec<u8>),
    Vector(Vec<bf16>),
    Array(Vec<FieldValue>),
    Map(BTreeMap<String, FieldValue>),
    Option(Option<Box<FieldValue>>),
}
```

### 4.3 Example Document

```rust
use anda_db::schema::{Schema, FieldType, FieldEntry, FieldValue};
use bf16::bf16;

let schema = Schema::builder()
    .add_field("title", FieldType::Text, "Document title")
    .add_field("content", FieldType::Text, "Document content")
    .add_field("embedding", FieldType::Vector { dimension: 512 }, "Vector embedding")
    .add_field("tags", FieldType::Array(Box::new(FieldType::Text)), "Tags")
    .build()?;

let doc = serde_json::json!({
    "title": "Rust Programming Language",
    "content": "Rust is a systems programming language focused on safety and concurrency",
    "embedding": vec![bf16::from_f32(0.1); 512],
    "tags": ["programming", "systems", "safety"]
});
```

---

## 5. Indexing System

### 5.1 B-Tree Index (`anda_db_btree`)

Inverted index for exact match and range queries.

**Features**:
- Equality queries: `Eq`, `Gt`, `Ge`, `Lt`, `Le`, `Between`
- Prefix queries (strings)
- Logical combinations: `Or`, `And`, `Not`
- Duplicate keys support
- Incremental persistence

**Configuration**:

```rust
struct BTreeConfig {
    bucket_overload_size: usize,  // Bucket size threshold (default 512KB)
    allow_duplicates: bool,       // Allow duplicates (default true)
}
```

**Usage Example**:

```rust
// Create B-Tree index
collection.create_btree_index(["category", "status"])?;

// Query
let query = Query {
    filter: Some(Filter::Field((
        "category".to_string(),
        RangeQuery::Eq("electronics".to_string())
    ))),
    ..Default::default()
};
```

### 5.2 BM25 Index (`anda_db_tfs`)

Full-text search index based on Tantivy using BM25 relevance ranking algorithm.

**BM25 Parameters**:
- `k1`: Term frequency saturation (default 1.2)
- `b`: Document length normalization (default 0.75)

**Tokenizers**:
- `SimpleTokenizer`: Default English tokenization
- `jieba_tokenizer()`: Chinese tokenization (requires `tantivy-jieba` feature)

**Usage Example**:

```rust
// Create BM25 index
collection.create_bm25_index(["title", "content"])?;

// Search
let query = Query {
    search: Some(Search {
        text: Some("Rust programming".to_string()),
        ..Default::default()
    }),
    ..Default::default()
};
```

### 5.3 HNSW Index (`anda_db_hnsw`)

Hierarchical Navigable Small World graph algorithm for approximate nearest neighbor (ANN) vector search.

**Configuration**:

```rust
struct HnswConfig {
    dimension: usize,                    // Vector dimension (default 512)
    max_layers: usize,                   // Max layers (default 16)
    max_connections: usize,              // M parameter (default 32)
    ef_construction: usize,             // Construction candidates (default 200)
    ef_search: usize,                   // Search candidates (default 50)
    distance_metric: DistanceMetric,      // Distance metric (default Euclidean)
}
```

**Distance Metrics**:

| Metric         | Description                          |
| -------------- | ------------------------------------ |
| `Euclidean`    | Euclidean distance (L2)              |
| `Cosine`       | Cosine distance (1 - cos similarity) |
| `InnerProduct` | Inner product (negative)             |
| `Manhattan`    | Manhattan distance (L1)              |

**Usage Example**:

```rust
use anda_db::index::HnswConfig;
use anda_db_hnsw::DistanceMetric;

let hnsw_config = HnswConfig {
    dimension: 768,
    max_connections: 16,
    ef_construction: 100,
    ef_search: 50,
    distance_metric: DistanceMetric::Cosine,
    ..Default::default()
};

collection.create_hnsw_index("embedding", hnsw_config)?;

// Vector search
let query = Query {
    search: Some(Search {
        vector: Some(query_vector),
        ..Default::default()
    }),
    limit: Some(10),
    ..Default::default()
};
```

---

## 6. Query System

### 6.1 Query Structure

```rust
pub struct Query {
    pub search: Option<Search>,     // Search configuration
    pub filter: Option<Filter>,      // Filter conditions
    pub limit: Option<usize>,       // Result limit
}
```

### 6.2 Search Configuration

```rust
pub struct Search {
    pub text: Option<String>,              // Full-text query
    pub vector: Option<Vec<f32>>,          // Vector query
    pub bm25_params: Option<BM25Params>,   // BM25 parameters
    pub reranker: Option<RRFReranker>,     // RRF reranking config
    pub logical_search: bool,               // Enable AND/OR/NOT operators
}
```

### 6.3 Filter

```rust
pub enum Filter {
    Field((String, RangeQuery<Fv>)),   // Single field filter
    Or(Vec<Box<Filter>>),             // OR logic
    And(Vec<Box<Filter>>),            // AND logic
    Not(Box<Filter>),                 // NOT logic
}
```

### 6.4 Hybrid Search

RRF (Reciprocal Rank Fusion) fuses multiple retrieval results:

```rust
let query = Query {
    search: Some(Search {
        text: Some("Rust programming".to_string()),
        vector: Some(query_vector),
        reranker: Some(RRFReranker { k: 60 }),
        ..Default::default()
    }),
    filter: Some(Filter::Field((
        "status".to_string(),
        RangeQuery::Eq("published".to_string())
    ))),
    limit: Some(20),
    ..Default::default()
};

let results = collection.search_as::<MyDoc>(&query)?;
```

---

## 7. Storage Layer

### 7.1 Storage Configuration

```rust
pub struct StorageConfig {
    pub cache_max_capacity: u64,       // Cache capacity (0 disables)
    pub compress_level: i32,           // Zstd compression level (0 disables, 1-22)
    pub object_chunk_size: usize,      // Object chunk size
    pub max_small_object_size: usize, // Small object threshold
    pub bucket_overload_size: usize,   // Bucket overflow size
}
```

### 7.2 Storage Backends

```rust
// In-memory storage
let store = object_store::memory::InMemory::new();

// Local filesystem
let store = object_store::local::LocalFileSystem::new("path/to/storage");

// S3-compatible storage
let store = object_store::aws::AmazonS3::new_from_environment()?;
```

### 7.3 Encrypted Storage

```rust
use anda_object_store::{EncryptedStoreBuilder, MetaStoreBuilder};
use aes_gcm::KeyInit;

// Create encrypted storage
let secret = [0u8; 32]; // 32-byte key
let store = EncryptedStoreBuilder::with_secret(local_fs, 1000, secret)
    .with_chunk_size(1024 * 1024)  // 1MB chunks
    .with_conditional_put()
    .build()?;
```

**Encryption Features**:
- AES-256-GCM encryption
- Chunked encryption (default 256KB)
- Independent authentication tag per chunk
- Unique nonce based on chunk index

---

## 8. Network Communication

### 8.1 anda_db_server

HTTP RPC server based on Axum.

**Endpoints**:

| Method            | Path             | Description                                  |
| ----------------- | ---------------- | -------------------------------------------- |
| `POST /`          | Root methods     | Server info, create database, etc.           |
| `POST /{db_name}` | Database methods | Collection operations, document CRUD, search |

**Request Format**:

```json
{
  "method": "create_collection",
  "params": {
    "config": { "name": "my_collection" },
    "schema": { ... }
  }
}
```

**Response Format**:

```json
{
  "result": { ... },
  "error": null
}
```

### 8.2 Shard Proxy (`anda_db_shard_proxy`)

Reverse proxy routing requests to correct shard instances based on routing table.

**Routing Model**:

```
Client → Shard Proxy → [Shard A | Shard B | Shard C]
                         (PostgreSQL routing table)
```

**Admin API**:

| Method | Path                          | Description                 |
| ------ | ----------------------------- | --------------------------- |
| GET    | `/_admin/db_shards/{db_name}` | Get database-shard mapping  |
| PUT    | `/_admin/db_shards`           | Create/update shard mapping |
| DELETE | `/_admin/db_shards`           | Delete shard mapping        |
| PUT    | `/_admin/shard_backends`      | Create/update shard backend |

---

## 9. Security Features

### 9.1 Authentication

```rust
// Start server with API key
ADDR=0.0.0.0:8080 API_KEY=your-secret-key anda_db_server

// Client request with auth
curl -H "Authorization: Bearer your-secret-key" \
     -X POST http://localhost:8080/my_db \
     -d '{"method": "get_information", "params": {}}'
```

### 9.2 Transport Encryption

Production environments should use HTTPS/TLS termination at load balancer or reverse proxy.

### 9.3 Encryption at Rest

AES-256-GCM encryption via `EncryptedStore`:

- Data encrypted before writing to storage
- Data decrypted when reading
- Each chunk has independent authentication tag
- Data cannot be decrypted even if storage media is stolen

---

## 10. KIP Protocol

KIP (Knowledge Interaction Protocol) is a protocol for AI Agents to interact with knowledge graphs.

### 10.1 Protocol Components

| Language | Purpose                | Operation Type |
| -------- | ---------------------- | -------------- |
| KQL      | Knowledge Query        | Read-only      |
| KML      | Knowledge Manipulation | Read-write     |
| META     | Meta Operations        | Administration |

### 10.2 KQL Example

```kql
FIND(?drug.name, ?drug.attributes.risk_level)
WHERE {
    ?drug {type: "Drug"}
    ?headache {name: "Headache"}
    (?drug, "treats", ?headache)
    FILTER(?drug.attributes.risk_level < 3)
}
ORDER BY ?drug.attributes.risk_level ASC
LIMIT 10
```

### 10.3 KML Example

```kml
UPSERT {
    CONCEPT ?new_drug {
        { type: "Drug", name: "Aspirin" }
        SET ATTRIBUTES {
            molecular_formula: "C9H8O4",
            risk_level: 1
        }
        SET PROPOSITIONS {
            ("treats", { type: "Symptom", name: "Headache" })
        }
    }
}
WITH METADATA {
    source: "Medical Database v2.1",
    confidence: 0.95
}
```

### 10.4 META Example

```sql
DESCRIBE PRIMER           -- Get agent identity and domain map
DESCRIBE CONCEPT TYPE "Drug"  -- Get type schema
SEARCH CONCEPT "aspirin" LIMIT 5  -- Search concepts
```

---

## 11. Quick Start

### 11.1 Install Dependencies

```toml
# Cargo.toml
[dependencies]
anda_db = { version = "0.7", features = ["full"] }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

### 11.2 Basic Usage

```rust
use anda_db::{AndaDB, DBConfig, CollectionConfig};
use anda_db::schema::{Schema, FieldType, FieldEntry, AndaDBSchema};
use anda_db::index::{HnswConfig, RRFeranker};
use anda_db::query::{Query, Search, Filter, RangeQuery};
use anda_db::storage::StorageConfig;
use anda_db_hnsw::DistanceMetric;
use object_store::local::LocalFileSystem;
use bf16::bf16;
use serde::{Serialize, Deserialize};

#[derive(AndaDBSchema, Serialize, Deserialize, Debug)]
struct MyDoc {
    _id: u64,
    title: String,
    content: String,
    embedding: Vec<bf16>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create object store
    let store = LocalFileSystem::new("./data")?;

    // 2. Create database config
    let db_config = DBConfig {
        name: "my_db".to_string(),
        description: "My first AndaDB".to_string(),
        storage: StorageConfig::default(),
        lock: None,
    };

    // 3. Create database
    let db = AndaDB::create(store, db_config).await?;

    // 4. Create collection
    let schema = MyDoc::schema();
    let coll_config = CollectionConfig {
        name: "documents".to_string(),
        description: "Document collection".to_string(),
    };

    let collection = db
        .create_collection(schema, coll_config, |c| async {
            // 5. Create index
            c.create_hnsw_index("embedding", HnswConfig {
                dimension: 384,
                distance_metric: DistanceMetric::Cosine,
                ..Default::default()
            }).await?;
            Ok(())
        })
        .await?;

    // 6. Add document
    let doc = MyDoc {
        _id: 1,
        title: "Hello AndaDB".to_string(),
        content: "Welcome to the world of AI databases".to_string(),
        embedding: vec![bf16::from_f32(0.1); 384],
    };
    collection.add(&doc)?;

    // 7. Get document
    let result = collection.get(1u64)?;
    println!("Found: {:?}", result);

    // 8. Search
    let query = Query {
        search: Some(Search {
            text: Some("AI database".to_string()),
            ..Default::default()
        }),
        limit: Some(10),
        ..Default::default()
    };

    let results = collection.search_as::<MyDoc>(&query)?;
    println!("Found {} results", results.len());

    Ok(())
}
```

### 11.3 Start HTTP Server

```rust
use anda_db_server::{build_router, handler::AppState};
use axum::Router;

#[tokio::main]
async fn main() {
    let store = LocalFileSystem::new("./data").unwrap();
    let state = AppState::new(store, "my_server".to_string(), None);

    let app: Router = build_router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
```

### 11.4 Test with curl

```bash
# Get server info
curl -X POST http://127.0.0.1:8080/ \
  -H "Content-Type: application/json" \
  -d '{"method": "get_information", "params": {}}'

# Create database
curl -X POST http://127.0.0.1:8080/ \
  -H "Content-Type: application/json" \
  -d '{"method": "create_database", "params": {"name": "test_db"}}'

# Add document
curl -X POST http://127.0.0.1:8080/test_db \
  -H "Content-Type: application/json" \
  -d '{
    "method": "add_document",
    "params": {
      "collection": "docs",
      "document": {
        "_id": 1,
        "title": "Hello",
        "content": "World"
      }
    }
  }'
```

---

## 12. API Reference

### 12.1 AndaDB API

| Method                                 | Return Type          | Description                |
| -------------------------------------- | -------------------- | -------------------------- |
| `create(config, f)`                    | `Result<Self>`       | Create new database        |
| `connect(config)`                      | `Result<Self>`       | Connect or create database |
| `open(config)`                         | `Result<Self>`       | Open existing database     |
| `flush()`                              | `Result<()>`         | Flush all data to storage  |
| `close()`                              | `Result<()>`         | Close database             |
| `create_collection(schema, config, f)` | `Result<Collection>` | Create collection          |
| `open_collection(name, f)`             | `Result<Collection>` | Open collection            |
| `delete_collection(name)`              | `Result<()>`         | Delete collection          |
| `metadata()`                           | `DBMetadata`         | Get database metadata      |
| `stats()`                              | `StorageStats`       | Get storage statistics     |
| `set_read_only(bool)`                  | `Result<()>`         | Set read-only mode         |

### 12.2 Collection API

| Method                             | Return Type      | Description         |
| ---------------------------------- | ---------------- | ------------------- |
| `add(doc)`                         | `Result<u64>`    | Add document        |
| `add_from(value)`                  | `Result<u64>`    | Add from JSON value |
| `get(id)`                          | `Result<Doc>`    | Get document        |
| `update(id, fields)`               | `Result<()>`     | Update fields       |
| `upsert(doc)`                      | `Result<u64>`    | Insert or update    |
| `remove(id)`                       | `Result<()>`     | Delete document     |
| `len()`                            | `usize`          | Document count      |
| `search_as::<T>(query)`            | `Result<Vec<T>>` | Search documents    |
| `create_hnsw_index(field, config)` | `Result<()>`     | Create HNSW index   |

### 12.3 Error Codes

| Code | Name                | Description        |
| ---- | ------------------- | ------------------ |
| 1001 | `InvalidSyntax`     | Syntax error       |
| 2001 | `TypeMismatch`      | Type mismatch      |
| 3001 | `ReferenceError`    | Reference error    |
| 3002 | `NotFound`          | Not found          |
| 4001 | `ExecutionTimeout`  | Execution timeout  |
| 4002 | `ResourceExhausted` | Resource exhausted |

---

## 13. Performance Tuning

### 13.1 HNSW Parameter Tuning

| Parameter         | Default | Tuning Suggestions                                              |
| ----------------- | ------- | --------------------------------------------------------------- |
| `ef_construction` | 200     | Increase for better build quality, decrease for faster build    |
| `ef_search`       | 50      | Increase for better search accuracy, decrease for lower latency |
| `max_connections` | 32      | Increase for better graph quality, increases memory usage       |
| `max_layers`      | 16      | Adjust based on data scale                                      |

### 13.2 BM25 Parameter Tuning

| Parameter | Default | Tuning Suggestions                              |
| --------- | ------- | ----------------------------------------------- |
| `k1`      | 1.2     | Term frequency saturation, range [1.2, 2.0]     |
| `b`       | 0.75    | Document length normalization, range [0.0, 1.0] |

### 13.3 Storage Configuration

```rust
let storage_config = StorageConfig {
    cache_max_capacity: 1024 * 1024 * 1024,  // 1GB cache
    compress_level: 3,                        // Medium compression
    object_chunk_size: 8 * 1024 * 1024,       // 8MB chunks
    max_small_object_size: 64 * 1024,         // 64KB small object threshold
    bucket_overload_size: 512 * 1024,         // 512KB bucket size
};
```

### 13.4 Memory Optimization

- Use `bf16` instead of `f32` for vector storage, reduces memory by 50%
- Enable Zstd compression to reduce storage space
- Set cache size appropriately to avoid OOM

---

## 14. Best Practices

### 14.1 Schema Design

1. **Avoid Over-Indexing**: Each index consumes extra storage and update overhead
2. **Choose Appropriate Field Types**: Use the most specific type
3. **Schema Evolution**: Use `upgrade_with` for gradual schema upgrades

### 14.2 Vector Search

1. **Use Normalized Vectors**: For cosine distance, ensure vectors are normalized
2. **Batch Insert**: Use batch insert for better performance
3. **Set Appropriate Dimension**: Based on actual model output dimension

### 14.3 Search Optimization

1. **Combined Filtering**: Use Filter to reduce candidate set
2. **Limit Results**: Use `limit` to avoid returning too many results
3. **RRF Fusion**: Use RRF for hybrid search scenarios to improve recall

### 14.4 Persistence Strategy

1. **Regular Flush**: Use `auto_flush` for periodic persistence
2. **Flush Before Close**: Ensure all data is written
3. **Monitor Statistics**: Use `stats()` to monitor storage state

### 14.5 Security Recommendations

1. **Use API Key**: Enable authentication in production
2. **Encrypt Sensitive Data**: Use `EncryptedStore` for encryption at rest
3. **Network Isolation**: Deploy server in private network
4. **Regular Backup**: Backup data storage regularly

---

## 15. Detailed Documentation

### anda_db_schema

`anda_db_schema` is the core type system library for AndaDB. For detailed documentation, see:

**[anda_db_schema Detailed Documentation](./anda_db_schema.md)**

Includes:
- Complete FieldType enum definition
- FieldValue type conversions
- FieldEntry and Schema construction
- Document model operations
- Resource type definition
- Serialization/deserialization mechanisms
- Complete API reference with usage examples

### anda_db_derive

`anda_db_derive` provides procedural macros for auto-generating Schema code. For detailed documentation, see:

**[anda_db_derive Detailed Documentation](./anda_db_derive.md)**

Includes:
- `FieldTyped` and `AndaDBSchema` derive macros
- `#[field_type]`, `#[unique]`, `#[serde(rename)]` attributes
- Complete Rust type to FieldType mapping table
- Derive macro internal implementation principles

---

## Appendix A: Crate Index

| Crate                         | Version | Description                    |
| ----------------------------- | ------- | ------------------------------ |
| `anda_db`                     | 0.7.26  | Core database                  |
| `anda_db_server`              | 0.7     | HTTP RPC server                |
| `anda_db_schema`              | 0.4     | Schema type system             |
| `anda_db_btree`               | 0.5.9   | B-Tree index                   |
| `anda_db_tfs`                 | 0.4     | BM25 full-text search          |
| `anda_db_hnsw`                | 0.4.10  | HNSW vector index              |
| `anda_db_utils`               | -       | Utility functions              |
| `anda_db_derive`              | 0.4     | Derive macros                  |
| `anda_object_store`           | 0.3.1   | Object storage + encryption    |
| `anda_kip`                    | 0.7.9   | Knowledge interaction protocol |
| `anda_cognitive_nexus`        | 0.7     | Cognitive nexus implementation |
| `anda_cognitive_nexus_server` | 0.1     | Cognitive server               |
| `anda_db_shard_proxy`         | -       | Shard proxy                    |

---

## Appendix B: Feature Flags

| Feature         | Description                   |
| --------------- | ----------------------------- |
| `default`       | No extra features             |
| `full`          | Enable Tantivy BM25 and jieba |
| `tantivy`       | Enable Tantivy dependency     |
| `tantivy-jieba` | Enable Chinese tokenization   |

---

*Document generated: 2026-04-21*
