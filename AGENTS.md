# AndaDB - AI Agent Database

AndaDB is an embedded Rust database designed for AI Agents, focusing on knowledge storage and memory management.

## Project Structure

```
rs/
├── anda_db/              # Core database library
├── anda_db_server/       # HTTP RPC server
├── anda_db_schema/       # Type system (FieldType, FieldValue, Schema)
├── anda_db_derive/       # Derive macros (AndaDBSchema, FieldTyped)
├── anda_db_btree/        # B-Tree index
├── anda_db_tfs/          # BM25 full-text search
├── anda_db_hnsw/         # HNSW vector index
├── anda_db_utils/        # Utilities
├── anda_object_store/    # Object storage with encryption
├── anda_kip/             # Knowledge Interaction Protocol
├── anda_cognitive_nexus/  # Cognitive nexus implementation
└── anda_db_shard_proxy/  # Shard proxy for multi-tenant
```

## Working with AndaDB

When writing Rust code that uses AndaDB, use the **anda-db skill**:

```
/anda-db
```

This skill provides:
- Quick reference for common patterns
- Type mapping tables
- Derive macro usage
- Index configuration examples
- Error handling guidance

## Key Dependencies

```toml
anda_db = { version = "0.7", features = ["full"] }
tokio = { version = "1", features = ["full"] }
```

## Documentation

- [Main Documentation](docs/README.md)
- [anda_db_schema Details](docs/anda_db_schema.md)
- [anda_db_derive Details](docs/anda_db_derive.md)

## Quick Start

```rust
use anda_db::{AndaDB, DBConfig, CollectionConfig};
use anda_db::schema::AndaDBSchema;
use object_store::local::LocalFileSystem;

#[derive(AndaDBSchema)]
struct Doc { _id: u64, title: String }

let store = LocalFileSystem::new("./data")?;
let db = AndaDB::create(store, DBConfig {
    name: "db".into(),
    description: "".into(),
    storage: Default::default(),
    lock: None,
}).await?;
```

## Building and Testing

```bash
cargo build
cargo test
cargo test -p anda_db --lib
```
