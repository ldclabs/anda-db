# Anda DB

[![Build Status](https://github.com/ldclabs/anda-db/actions/workflows/test.yml/badge.svg)](https://github.com/ldclabs/anda-db/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/ldclabs/anda-db/blob/main/LICENSE)

Anda DB is a modular Rust workspace for building durable AI memory systems.
At its core is an embedded, schema-aware document database with three
retrieval modes built in:

- B-Tree indexes for exact match and range filters
- BM25 indexes for full-text search
- HNSW indexes for vector similarity search

On top of that core, the workspace provides a portable object-store-backed
persistence layer, a declarative knowledge protocol called [KIP](https://github.com/ldclabs/kip), and the
reference Cognitive Nexus runtime that turns the database into a persistent
knowledge graph for AI agents.

## What Anda DB Is For

Anda DB is designed for applications that need more than a plain key-value
store but less than a full external database service. It works especially well
for:

- long-term memory for AI agents
- embedded retrieval systems inside Rust services
- hybrid search over structured, lexical, and semantic data
- knowledge-graph workloads with explicit protocol execution
- deployments that need to run locally during development and on cloud object
  storage in production

The core design goal is simple: keep the data model, retrieval logic, and
persistence lifecycle inside the application process, while still supporting
durability, recovery, and rich search.

## Use Cases / Deployment Modes

### Use Cases

Anda DB is a good fit for several product and platform patterns:

- **Agent long-term memory**: persist facts, observations, preferences, events, and embeddings for one or many agents
- **Embedded hybrid retrieval**: combine structured filters, BM25 lexical search, and vector similarity search inside a Rust service
- **Knowledge-graph memory systems**: model concepts and propositions through KIP and the Cognitive Nexus runtime
- **Private or regulated AI deployments**: keep storage inside your own environment while still using modern object-store semantics and optional encryption-at-rest
- **Multi-tenant memory platforms**: expose many logical databases behind one service layer and shard them when needed

### Deployment Modes

The workspace supports several deployment shapes without changing the core data model:

- **Embedded library mode**: link `anda_db` directly into a Rust application for the lowest-latency, in-process integration
- **Single-node persistent mode**: run on local filesystem storage for self-hosted or appliance-style deployments
- **Cloud object storage mode**: keep the same database logic while targeting S3, GCS, Azure Blob, or other `object_store` backends enabled by the embedding application
- **Database service mode**: expose the core database through `anda_db_server`
- **KIP memory service mode**: expose the Cognitive Nexus through `anda_cognitive_nexus_server`
- **Sharded service mode**: route multiple logical databases through `anda_db_shard_proxy` for multi-tenant deployments

This separation between storage, protocol, and service layers is what allows
Anda DB to scale from a single embedded process to a routed, service-oriented
memory platform.

## Key Capabilities

- Embedded database engine with no mandatory external database service
- Schema validation and document-oriented collections
- Hybrid retrieval via BM25 + HNSW + RRF reranking
- Portable persistence through `object_store`
- Optional transparent encryption-at-rest through `anda_object_store`
- KIP parser and executor model for graph-shaped AI memory
- Reference Cognitive Nexus implementation built on top of AndaDB
- Optional HTTP server and shard-proxy layers for service deployments

## Why `object_store` Matters

One of Anda DB's main architectural strengths is that persistence is built on
top of the `object_store::ObjectStore` trait instead of being tied to one local
filesystem implementation.

That means the same database logic can be reused across multiple storage
backends, depending on which `object_store` features your application enables:

- in-memory storage for tests and ephemeral runs
- local filesystem storage for embedded deployments
- Amazon S3
- Google Cloud Storage
- Azure Blob Storage
- HTTP/WebDAV-compatible object storage

This portability is important for AI memory systems. You can develop locally,
test in-process, and later move the same storage model onto cloud object
storage without rewriting the database layer.

On top of that abstraction, `anda_object_store` adds:

- portable conditional-update semantics via `MetaStore`
- transparent chunked AES-256-GCM encryption via `EncryptedStore`

## Workspace Overview

The workspace is layered rather than monolithic.

| Crate                         | Role                                                                           |
| ----------------------------- | ------------------------------------------------------------------------------ |
| `anda_db`                     | Core embedded database: collections, queries, indexes, and storage integration |
| `anda_db_schema`              | Type system, field values, schemas, and document model                         |
| `anda_db_derive`              | Derive macros such as `AndaDBSchema` and `FieldTyped`                          |
| `anda_db_btree`               | Exact-match and range index engine                                             |
| `anda_db_tfs`                 | Embedded BM25 full-text search engine                                          |
| `anda_db_hnsw`                | HNSW approximate-nearest-neighbor vector index                                 |
| `anda_object_store`           | Portable metadata and encryption wrappers for `object_store`                   |
| `anda_kip`                    | KIP parser, AST, request/response model, executor framework                    |
| `anda_cognitive_nexus`        | Reference KIP executor and AI memory graph runtime                             |
| `anda_db_server`              | HTTP server for the core database layer                                        |
| `anda_cognitive_nexus_server` | HTTP/JSON-RPC server for the Cognitive Nexus                                   |
| `anda_db_shard_proxy`         | Shard-routing proxy for multi-tenant deployments                               |

## Architecture at a Glance

```text
Application or Agent Runtime
  -> anda_kip                    protocol and request model
  -> anda_cognitive_nexus        reference memory graph runtime
  -> anda_db                     embedded storage and retrieval core
     -> anda_db_schema           schema and document model
     -> anda_db_derive           schema generation macros
     -> anda_db_btree            exact and range index
     -> anda_db_tfs              BM25 lexical search
     -> anda_db_hnsw             HNSW vector search
     -> anda_object_store        metadata and encryption wrappers
     -> object_store             backend abstraction for local and cloud storage
```

## Quick Start: Embedded Database Usage

Add the core dependencies to your `Cargo.toml`.

```toml
[dependencies]
anda_db = { version = "0.7", features = ["full"] }
object_store = { version = "0.13", features = ["fs"] }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

Example:

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

For a richer end-to-end example with BM25 tokenization, filtering, and vector
search, see [rs/anda_db/examples/db_demo.rs](./rs/anda_db/examples/db_demo.rs).

## KIP and the Cognitive Nexus

The workspace includes a higher-level memory layer for agent reasoning.

- `anda_kip` defines the Knowledge Interaction Protocol: parser, AST,
  request/response model, error codes, and executor trait.
- `anda_cognitive_nexus` is the reference KIP backend built on top of
  `anda_db`, storing concepts and propositions in persistent collections.

Use these crates when you want a graph-shaped, protocol-driven AI memory system
instead of a direct document-database integration.

## Service Layers

If you want to expose Anda DB over the network rather than embed it directly,
the workspace provides optional service crates.

- `anda_db_server` for the core database API
- `anda_cognitive_nexus_server` for KIP over HTTP/JSON-RPC
- `anda_db_shard_proxy` for shard routing and multi-tenant entrypoints

These layers are optional. The primary design remains embedded-first.

## Documentation

The root README is the overview. The deeper technical references live in
[docs/README.md](./docs/README.md).

Key documents:

- [docs/anda_db.md](./docs/anda_db.md): core embedded database design and API model
- [docs/anda_db_schema.md](./docs/anda_db_schema.md): schema, field values, and documents
- [docs/anda_db_derive.md](./docs/anda_db_derive.md): derive macros and field-type inference
- [docs/anda_db_btree.md](./docs/anda_db_btree.md): exact and range index internals
- [docs/anda_db_tfs.md](./docs/anda_db_tfs.md): BM25 full-text engine
- [docs/anda_db_hnsw.md](./docs/anda_db_hnsw.md): HNSW vector index
- [docs/anda_object_store.md](./docs/anda_object_store.md): metadata and encryption wrappers over `object_store`
- [docs/anda_kip.md](./docs/anda_kip.md): KIP parser and executor framework
- [docs/anda_cognitive_nexus.md](./docs/anda_cognitive_nexus.md): reference AI memory graph runtime

## Build and Test

```bash
cargo build
cargo test
```

For crate-specific testing, run commands such as:

```bash
cargo test -p anda_db
cargo test -p anda_db_btree
cargo test -p anda_kip
```

## License

Anda DB is licensed under the MIT License. See [LICENSE](./LICENSE) for details.