# Anda DB

[![Build Status](https://github.com/ldclabs/anda-db/actions/workflows/test.yml/badge.svg)](https://github.com/ldclabs/anda-db/actions)
[![Crates.io](https://img.shields.io/crates/v/anda_db.svg)](https://crates.io/crates/anda_db)
[![Docs.rs](https://docs.rs/anda_db/badge.svg)](https://docs.rs/anda_db)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/ldclabs/anda-db/blob/main/LICENSE)

Anda DB is a modular Rust workspace for building durable AI memory systems.
At its core is an embedded, schema-aware document database with three
retrieval modes built in:

- B-Tree indexes for exact match and range filters
- BM25 indexes for full-text search
- HNSW indexes for vector similarity search

On top of that core, the workspace provides a portable object-store-backed
persistence layer, an SDK for the [KIP](https://github.com/ldclabs/KIP)
(Knowledge Interaction Protocol) 2.0 cognitive state protocol, and two
reference KIP engines: the Rust Cognitive Nexus built on Anda DB, and an
independent TypeScript engine that runs inside SQLite-backed Cloudflare
Durable Objects.

## Current release

The Rust crates form the **0.14** release family. Patch versions diverge
after a release, so pin each crate by reading its own `Cargo.toml`:

| Package                                                                  | Version |
| ------------------------------------------------------------------------ | ------- |
| `anda_cognitive_nexus`                                                   | 0.14.4  |
| `anda_db`                                                                | 0.14.2  |
| `anda_db_btree`, `anda_db_tfs`, `anda_kip`                               | 0.14.1  |
| other Rust crates, the Python binding, `@ldclabs/kip-do` (npm)           | 0.14.0  |
| `cf-tokenizer` (standalone service, own versioning)                      | 1.0.0   |

- Rust 1.95 or newer is required (edition 2024).
- The KIP protocol version is `2.0`, following KIP `11a82ec`, with the
  bundled `kip://profiles/cognitive-memory@2.0.0` draft Schema Package. KIP
  protocol versions are independent of package versions.
- Upgrading from 0.13 is breaking for KIP clients and for Spaces activated
  under the earlier CognitiveMemory draft. The first open of a Nexus store by
  0.14.1 or newer upgrades its collection schemas; do not reopen it with
  0.14.0 afterwards.

Read the [changelog](CHANGELOG.md) before upgrading. KIP 1.x stores migrate
through the [v1 migration guide](docs/kip-v1-migration.md).

## What Anda DB is for

Anda DB is designed for applications that need more than a plain key-value
store but less than a full external database service:

- long-term memory for AI agents
- embedded retrieval inside Rust services
- hybrid search over structured, lexical and semantic data
- knowledge-graph memory with explicit protocol execution
- deployments that run on a local filesystem during development and on cloud
  object storage in production

The design goal is to keep the data model, retrieval logic and persistence
lifecycle inside the application process, while still supporting durability,
crash recovery and rich search.

### Use cases

- **Agent long-term memory**: persist facts, observations, preferences,
  events and embeddings for one or many agents
- **Embedded hybrid retrieval**: combine B-Tree filters, BM25 lexical search
  and vector similarity search inside a Rust service
- **Knowledge-graph memory**: record who claimed what, on what evidence, and
  what changed when, through KIP and a Cognitive Nexus
- **Private or regulated deployments**: keep storage inside your own
  environment, with optional encryption at rest
- **Multi-tenant memory platforms**: expose many logical databases behind one
  service layer and shard them when needed

### Deployment modes

The same data model supports several deployment shapes:

| Mode                     | Entry point                                                                |
| ------------------------ | -------------------------------------------------------------------------- |
| Embedded Rust library    | link [`anda_db`](rs/anda_db) or [`anda_cognitive_nexus`](rs/anda_cognitive_nexus) into your process |
| Local persistent storage | `object_store` local filesystem wrapped in `anda_object_store::MetaStore`  |
| Cloud object storage     | S3, GCS, Azure Blob or another `object_store` backend enabled by your app  |
| Database service         | [`anda-db-server`](rs/anda_db_server): CBOR-first HTTP RPC                  |
| KIP memory service       | [`anda-cognitive-nexus-server`](rs/anda_cognitive_nexus_server): HTTP/JSON-RPC |
| Sharded service          | [`anda-db-shard-proxy`](rs/anda_db_shard_proxy) in front of database servers |
| Cloudflare edge          | [`@ldclabs/kip-do`](ts/kip-do): one Nexus per Durable Object                |
| Python                   | [`anda_cognitive_nexus_py`](py/anda_cognitive_nexus_py): in-process Nexus   |

## Key capabilities

- Embedded database engine with no mandatory external service
- Schema validation, versioned schema upgrades and derive macros for Rust
  structs
- Hybrid retrieval: BM25 + HNSW fused with reciprocal-rank fusion, filtered
  through B-Tree indexes
- Portable persistence through `object_store`, with incremental index
  flushing, checkpoints and crash recovery
- Format-compatibility fixtures and a crash-consistency harness for the
  storage layer
- Optional transparent AES-256-GCM encryption at rest
- A KIP 2.0 SDK (parser, AST, request envelope, error registry, executor
  seam) and two reference engines held to one shared conformance suite
- Optional HTTP server and shard-proxy layers for service deployments

## Why `object_store` matters

Persistence is built on the `object_store::ObjectStore` trait instead of one
local filesystem implementation, so the same database logic runs on whichever
backends your application enables:

- in-memory storage for tests and ephemeral runs
- local filesystem storage for embedded deployments
- Amazon S3, Google Cloud Storage and Azure Blob Storage
- HTTP/WebDAV-compatible object storage

You can develop locally, test in-process, and move the same storage model to
cloud object storage without rewriting the database layer. On top of that
abstraction, [`anda_object_store`](rs/anda_object_store) adds:

- `MetaStore`: side-car metadata, logical ETags and portable conditional
  updates for backends without native support (such as the local
  filesystem)
- `EncryptedStore`: chunked AES-256-GCM encryption with authenticated
  metadata and seekable range reads

## Workspace overview

| Path                                                             | Role                                                                    | Distribution |
| ---------------------------------------------------------------- | ----------------------------------------------------------------------- | ------------ |
| [`rs/anda_db`](rs/anda_db)                                       | Core embedded database: collections, queries, indexes, storage          | crates.io    |
| [`rs/anda_db_schema`](rs/anda_db_schema)                         | Field types, values, schemas and documents                              | crates.io    |
| [`rs/anda_db_derive`](rs/anda_db_derive)                         | `AndaDBSchema` and `FieldTyped` derive macros                           | crates.io    |
| [`rs/anda_db_btree`](rs/anda_db_btree)                           | Exact-match and range index                                             | crates.io    |
| [`rs/anda_db_tfs`](rs/anda_db_tfs)                               | BM25 full-text search                                                   | crates.io    |
| [`rs/anda_db_hnsw`](rs/anda_db_hnsw)                             | HNSW vector index                                                       | crates.io    |
| [`rs/anda_db_utils`](rs/anda_db_utils)                           | Standalone `UniqueVec`; not used by the other crates                    | crates.io    |
| [`rs/anda_object_store`](rs/anda_object_store)                   | Metadata and encryption wrappers over `object_store`                    | crates.io    |
| [`rs/anda_kip`](rs/anda_kip)                                     | KIP 2.0 SDK: parser, AST, envelope, errors, executor seam, specs        | crates.io    |
| [`rs/anda_cognitive_nexus`](rs/anda_cognitive_nexus)             | Reference Rust KIP engine on Anda DB                                    | crates.io    |
| [`rs/anda_db_server`](rs/anda_db_server)                         | HTTP server for the core database API                                   | source       |
| [`rs/anda_cognitive_nexus_server`](rs/anda_cognitive_nexus_server) | HTTP/JSON-RPC server for the Cognitive Nexus                          | source, Docker |
| [`rs/anda_db_shard_proxy`](rs/anda_db_shard_proxy)               | PostgreSQL-routed shard proxy for multi-tenant deployments              | source       |
| [`rs/anda_kip_wasm`](rs/anda_kip_wasm)                           | WASM build of the Rust parser, the test oracle for `kip-do`; own workspace | not published |
| [`rs/cf-tokenizer`](rs/cf-tokenizer)                             | Stateless Jieba tokenizer service for Cloudflare Containers; own workspace | Docker     |
| [`ts/kip-do`](ts/kip-do)                                         | Independent TypeScript KIP engine on Cloudflare Durable Objects         | npm          |
| [`py/anda_cognitive_nexus_py`](py/anda_cognitive_nexus_py)       | Python binding for the Rust Nexus; not a default workspace member       | source       |
| [`fixtures/kip-conformance-2.0`](fixtures/kip-conformance-2.0)   | Vendored KIP engine suite that both engines run                         | —            |
| [`skills/anda-db`](skills/anda-db)                               | Agent-facing usage guide and API references                             | —            |
| [`docs`](docs)                                                   | Technical, maintenance and benchmark documents                          | —            |

## Architecture at a glance

```text
Agent / application
  │
  ├─ KIP 2.0 (Rust)
  │   anda_kip                   parser, AST, envelope, error registry, Executor trait
  │    └─ anda_cognitive_nexus   transactions, projection, Governance, Schema Packages
  │        └─ anda_db            ↓
  │
  ├─ Documents and hybrid search
  │   anda_db                    collections, queries, recovery
  │    ├─ anda_db_schema, anda_db_derive        schema and document model
  │    ├─ anda_db_btree, anda_db_tfs, anda_db_hnsw   B-Tree, BM25, HNSW indexes
  │    └─ anda_object_store → object_store       local or cloud storage
  │
  └─ KIP 2.0 (Cloudflare)
      @ldclabs/kip-lang → ts/kip-do  Durable Object SQLite
      (parser checked against anda_kip compiled to WASM: rs/anda_kip_wasm)
```

The service crates wrap these libraries: `anda_db_server` exposes `anda_db`,
`anda_cognitive_nexus_server` exposes `anda_cognitive_nexus`, and
`anda_db_shard_proxy` routes requests across `anda_db_server` shards.

## Quick start: embedded database

```toml
[dependencies]
anda_db = { version = "0.14", features = ["full"] }
anda_object_store = "0.14"
object_store = { version = "0.14", features = ["fs"] }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

`anda_db/full` only enables `object_store/fs`; the B-Tree, BM25, HNSW and
Jieba support are always compiled in.

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
    // The local filesystem has no conditional updates; MetaStore adds them.
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
            // Runs only when the collection is opened fresh: install
            // tokenizers and index hooks here, before creating indexes.
            async |c| {
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

    // Hybrid search: BM25 and HNSW results fused by RRF, filtered by B-Tree.
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

Operating rules worth knowing before production use:

- Share one live writer per database namespace; clone `AndaDB` and
  `Arc<Collection>` handles for concurrent tasks.
- Await mutations, flushes and `close()` to completion. A cancelled mutation
  can poison a collection handle; reopen it through the database.
- The open callback is skipped for an already open handle. Call
  `db.close_collection(name)` before reopening with a different index
  configuration.
- Set the HNSW `dimension` and `distance_metric` to match your embedding
  model; the default metric is Euclidean.

The [anda_db README](rs/anda_db/README.md) and
[docs/anda_db.md](docs/anda_db.md) cover the full API. A richer runnable
example is [rs/anda_db/examples/db_demo.rs](rs/anda_db/examples/db_demo.rs):

```bash
cargo run -p anda_db --example db_demo --features full
```

## KIP and the Cognitive Nexus

KIP 2.0 separates what a single self-describing graph would blur together:
meaning, belief, evidence, provenance, mnemonic state, retention, Governance
and Schema. A **Proposition** is a truth-neutral `(subject, predicate,
object)` tuple; an **Assertion** records one actor's commitment to it with a
stance, mode, confidence and Evidence; and what is currently believed is a
**projection** computed from Assertions under a named policy, never stored as
truth.

- [`anda_kip`](rs/anda_kip) is the protocol SDK: KQL/KML/META parsers, the
  executable AST, the request/response envelope, the Core Error Registry, the
  `Executor` trait, agent-facing prompts and function definitions, and the
  vendored KIP specification.
- [`anda_cognitive_nexus`](rs/anda_cognitive_nexus) is the reference engine:
  transactions, KQL with two time axes, `BELIEF` projection, META, Capsules,
  a separate Governance control plane and versioned Schema Packages, all
  stored in Anda DB collections.
- [`@ldclabs/kip-do`](ts/kip-do) is an independent TypeScript engine on
  Durable Object SQLite. Its parser (`@ldclabs/kip-lang`) is compared field
  for field against the Rust parser compiled to WASM.
- Both engines run the shared suite in
  [`fixtures/kip-conformance-2.0`](fixtures/kip-conformance-2.0).

Ask an engine for `DESCRIBE CAPABILITIES` before relying on optional
behavior: gaps are reported as structured data and refused as
`UnsupportedCapability`. Brain hosts that add scheduling, evaluation or
dispatch should read the
[Brain host contracts](docs/anda-brain-nexus-contracts.md).

## Documentation

The root README is the overview; [docs/README.md](docs/README.md) is the
documentation hub ([中文](docs/README.zh.md)). Most documents have a Chinese
`.zh.md` companion.

| Topic                         | Document                                                                                                   |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------- |
| Core database                 | [anda_db.md](docs/anda_db.md)                                                                              |
| Schemas and derives           | [anda_db_schema.md](docs/anda_db_schema.md), [anda_db_derive.md](docs/anda_db_derive.md)                   |
| Indexes                       | [anda_db_btree.md](docs/anda_db_btree.md), [anda_db_tfs.md](docs/anda_db_tfs.md), [anda_db_hnsw.md](docs/anda_db_hnsw.md) |
| Storage wrappers              | [anda_object_store.md](docs/anda_object_store.md)                                                          |
| KIP SDK                       | [anda_kip.md](docs/anda_kip.md), [specification](rs/anda_kip/SPECIFICATION.md), [syntax](rs/anda_kip/KIPSyntax.md) |
| Cognitive Nexus               | [anda_cognitive_nexus.md](docs/anda_cognitive_nexus.md)                                                    |
| Brain host integration        | [anda-brain-nexus-contracts.md](docs/anda-brain-nexus-contracts.md)                                        |
| Engine parity                 | [kip-do-nexus-parity.md](docs/kip-do-nexus-parity.md)                                                      |
| KIP 1.x migration             | [kip-v1-migration.md](docs/kip-v1-migration.md)                                                            |
| Testing                       | [testing.md](docs/testing.md)                                                                              |
| Benchmarks                    | [docs/benchmarks](docs/benchmarks), [million-row query report (中文)](docs/query-performance-million.zh.md) |
| Agent-facing usage guide      | [skills/anda-db](skills/anda-db/SKILL.md)                                                                  |

## Development

Run commands from the repository root. Root `cargo --workspace` commands do
not include `rs/anda_kip_wasm`, `rs/cf-tokenizer` (separate workspaces),
`ts/kip-do` or the Python binding.

| Scope                                        | Command                                                            |
| -------------------------------------------- | ------------------------------------------------------------------ |
| Rust workspace compile                       | `cargo check --workspace --all-features`                           |
| Rust workspace tests                         | `cargo test --workspace --all-features`                            |
| Core database tests                          | `cargo test -p anda_db --all-features`                             |
| Crash recovery and format compatibility      | `cargo test -p anda_db --test crash_recovery --test format_compat` |
| Schema and derive                            | `cargo test -p anda_db_schema -p anda_db_derive`                   |
| KIP SDK and Rust engine                      | `cargo test -p anda_kip -p anda_cognitive_nexus`                   |
| TypeScript engine (Node 24, pnpm 11)         | `make test-ts`                                                     |
| Formatting and Clippy (rewrites files)       | `make lint`                                                        |
| Formatting check only                        | `cargo fmt --all -- --check`                                       |

`make test-all` adds format-compatibility checks and KIP fuzzing (nightly and
`cargo-fuzz`); `make test-full` also runs the TypeScript checks. The Python
binding is tested with `make test-py` after enabling its workspace member; see
its [README](py/anda_cognitive_nexus_py/README.md).

Changes to KIP command strings in Rust sources or tests, to the error
registry or to the conformance fixtures feed generated TypeScript files: run
`pnpm run codegen` in `ts/kip-do` and commit the result. A Rust parser change
also needs `pnpm run build:oracle-wasm`. See [AGENTS.md](AGENTS.md) for the
full contributor workflow.

## License

Anda DB is licensed under the MIT License. See [LICENSE](LICENSE) for details.
