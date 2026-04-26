# AndaDB Technical Documentation

Version: 0.7.26

## Overview

AndaDB is the core embedded database crate in the AndaDB workspace. It is designed for AI-agent memory and knowledge workloads where application code needs to store structured documents, index them in multiple retrieval modes, and keep persistence logic local to the process instead of delegating it to an external database service.

At the crate level, AndaDB provides:

- A database object that manages collections and shared persistence
- Schema-aware collections for document CRUD
- Exact-match and range retrieval through B-Tree indexes
- Full-text retrieval through BM25 indexes
- Vector similarity retrieval through HNSW indexes
- Hybrid retrieval with reciprocal-rank-fusion reranking
- An object-store-backed persistence layer with optional compression and caching

The crate is intentionally small in surface area. Most user-facing work happens through these modules:

- `database`: database lifecycle and collection management
- `collection`: document operations, index management, search, and metadata
- `query`: hybrid search, filters, and reranking types
- `index`: index types and index hooks
- `schema`: re-exported type system from `anda_db_schema`
- `storage`: persistence, compression, object versions, and I/O stats
- `error`: the unified `DBError` type

## Design Goals

AndaDB is optimized for a specific class of systems rather than for general-purpose relational workloads.

Its main goals are:

- Durable long-term memory for AI agents
- Unified structured, lexical, and semantic retrieval
- Explicit schema validation at write time
- Embeddable deployment with no mandatory external database server
- Storage abstraction through `object_store`
- Predictable operational control for flushing, checkpoints, and read-only mode

In practice, the library behaves like a document database with specialized retrieval primitives for agent memory.

## Architecture

The `anda_db` crate sits on top of several lower-level workspace crates.

```text
Application
  -> anda_db::database::AndaDB
	 -> anda_db::collection::Collection
		-> anda_db::index::{BTree, BM25, Hnsw}
		-> anda_db::storage::Storage
		   -> object_store::ObjectStore

Supporting crates:
  - anda_db_schema: schema and field-value system
  - anda_db_btree: exact/range index engine
  - anda_db_tfs: BM25 text index engine
  - anda_db_hnsw: vector index engine
  - anda_db_utils: supporting utilities such as UniqueVec
```

The core execution model is:

1. Validate incoming documents against a schema.
2. Derive index values for the document.
3. Update in-memory index state.
4. Persist the document to object storage.
5. Flush metadata and index files to make state durable.

The library also contains recovery-oriented logic for reopening collections, loading persisted indexes, and repairing some partially flushed states.

## Core Concepts

### Database

`AndaDB` is the top-level handle. A database owns:

- A database name
- A shared `object_store` backend
- A storage namespace rooted at the database name
- Database metadata
- A set of open collections

Important methods:

- `AndaDB::create`: create a new database and fail if metadata already exists
- `AndaDB::connect`: open an existing database or create a new one
- `AndaDB::open`: open an existing database and fail if it does not exist
- `create_collection`: create a new collection
- `open_collection`: open an existing collection
- `open_or_create_collection`: open if present, otherwise create
- `delete_collection`: remove a collection and its persisted data
- `flush`: flush open collections and database metadata
- `close`: switch the database to read-only mode and flush pending state

The database also exposes `extensions`, which are lightweight user-defined metadata entries persisted alongside database metadata.

### Collection

A collection is the main unit of application work. All documents in a collection share one schema.

A collection owns:

- The collection name
- The active schema
- The collection storage namespace
- Zero or more B-Tree, BM25, and HNSW indexes
- Collection statistics and metadata
- In-memory document-id tracking structures
- Tokenizer configuration for text indexing
- Optional custom `IndexHooks`

Important methods:

- `add` and `add_from`
- `get` and `get_as`
- `update`
- `remove`
- `search` and `search_as`
- `search_ids` and `query_ids`
- `create_btree_index`, `create_bm25_index`, `create_hnsw_index`
- `compact_btree_index`, `compact_bm25_index`
- `flush` and `close`

Collections also expose their own `extensions` map for storing small application-specific metadata.

### Schema

The schema system comes from `anda_db_schema` and is re-exported through `anda_db::schema`.

There are two common ways to define schemas:

- Derive them from Rust types with `AndaDBSchema`
- Build them programmatically with schema builders and field entries

Typical field categories include:

- scalar values such as integers, floats, booleans, and text
- byte arrays
- vectors
- arrays and maps
- JSON-like values
- optional values

The collection validates documents against its schema before they are accepted.

### Document Identity

Document ids are unsigned 64-bit integers. The collection manages document-id assignment internally when using `add` and `add_from`.

Each collection also maintains:

- `max_document_id`
- an ordered `BTreeSet` of ids for range traversal
- a bitmap (`croaring::Treemap`) for efficient membership and persistence

This combination supports fast containment checks, ordered scans, and durable recovery.

## Indexing Model

AndaDB supports three complementary index families.

### B-Tree Indexes

B-Tree indexes are used for:

- exact match
- range queries
- unique constraints
- compound virtual-field indexes

They are the backbone of the `Filter::Field` query model and of direct id-range filtering.

Examples:

- filter by thread id
- filter by created_at range
- enforce uniqueness on external keys
- query a synthetic compound key built from multiple fields

For multi-field B-Tree indexes, the collection combines the indexed field values into a deterministic binary representation and stores it as a virtual field.

### BM25 Indexes

BM25 indexes support full-text retrieval over one or more fields.

Important properties:

- Collections can customize tokenization via `set_tokenizer`
- A BM25 index may span multiple fields
- Queries can run in standard or logical-search mode
- Results can be fused with vector results through RRF

The default BM25 path is collection-local. You create the index once and then use `Query.search.text` to retrieve documents.

### HNSW Indexes

HNSW indexes support approximate nearest-neighbor retrieval over vector fields.

Important properties:

- The indexed field must be a vector field
- Index construction is parameterized by `HnswConfig`
- Query vectors are supplied as `Vec<f32>`
- Search returns ranked document ids which can be fused with BM25 results

This index family is the semantic-retrieval path for embeddings or representation vectors.

### Hybrid Retrieval and RRF

If both text and vector search are present in a `Query`, AndaDB executes both and combines their ranked id lists with `RRFReranker`.

The reranker:

- assigns each result list a reciprocal-rank score
- sums scores across lists
- sorts by descending combined score

This is a pragmatic hybrid-search strategy that keeps query semantics simple while still allowing multiple retrieval signals.

## Query Model

The query surface is intentionally compact.

### `Query`

`Query` contains:

- `search: Option<Search>`
- `filter: Option<Filter>`
- `limit: Option<usize>`

The practical execution order is:

1. Produce ranked candidates from text and or vector search
2. Apply filters
3. Enforce the final limit

### `Search`

`Search` contains:

- `text`: optional BM25 text query
- `vector`: optional HNSW query vector
- `bm25_params`: optional tuning for BM25 scoring
- `reranker`: optional custom RRF configuration
- `logical_search`: whether to enable logical search operators in BM25

### `Filter`

`Filter` supports recursive logic:

- `Field((index_name, range_query))`
- `Or(Vec<Box<Filter>>)`
- `And(Vec<Box<Filter>>)`
- `Not(Box<Filter>)`

This makes it possible to express compound constraints such as:

- vector search constrained to a thread id
- full-text retrieval constrained to a time window
- id-range scans excluding known records

### Limits and Candidate Expansion

Internally, hybrid search may fetch more than the final limit before filtering so that ranking and filtering still produce useful final results. The public limit remains the final output contract.

## Storage Layer

The storage module implements persistence on top of `object_store`.

### Storage Backend Portability

One of AndaDB's most important design choices is that it does not bind persistence to a single local disk implementation. Instead, it builds on the `object_store::ObjectStore` trait, which provides a uniform async API for object storage services and local environments.

This means the same AndaDB application can be wired to different storage backends with minimal changes in database code. Depending on which `object_store` feature flags are enabled by the embedding application, the storage layer can target:

- in-memory storage for tests and ephemeral runs
- local filesystem storage for embedded deployments
- Amazon S3
- Google Cloud Storage
- Azure Blob Storage
- HTTP/WebDAV-compatible object storage

This portability matters for AI memory systems because it lets the same collection, indexing, and flush logic move across local development, self-hosted environments, and cloud object storage without redesigning the database layer.

It is also important that `object_store` models object-store semantics rather than POSIX filesystem semantics. In practice, this gives AndaDB a better foundation for durable metadata and index persistence, because the underlying abstraction supports capabilities such as conditional reads and writes, multipart upload, bulk deletion, and buffered adapters that map directly onto modern cloud storage systems.

In other words, AndaDB is not merely using `object_store` as a convenience wrapper. It is using it as the portability and durability boundary for the whole persistence layer. The exact backend is a deployment choice; the database logic above it remains the same.

### Storage Namespacing

The database and each collection use distinct storage prefixes.

Within a collection, persisted objects include:

- collection metadata
- document-id bitmap
- document bodies
- B-Tree metadata and buckets
- BM25 metadata and buckets
- HNSW metadata, ids, and node files
- storage metadata for checkpointing and I/O statistics

### Small Objects vs Streaming Writes

The storage layer distinguishes between:

- small objects written by `put` or `put_bytes`
- streamed objects written by `stream_writer`

`StorageConfig.max_small_object_size` protects the small-object path from oversized payloads.

### Compression

Storage can compress payloads with zstd.

Important details:

- compression is optional and controlled by `StorageConfig.compress_level`
- compression is skipped if it does not reduce size
- decompression is guarded by a maximum-size policy to reduce decompression-bomb risk

### Caching

Small objects may be cached in-memory using `moka`.

The cache is intended for:

- frequently read metadata objects
- small document reads
- repeated access patterns in agent loops

Cache size is configured with `cache_max_capacity`.

### Versioned Updates

The storage layer tracks `ObjectVersion` values built from object-store metadata such as ETag and version id.

These versions are used for conditional updates so the library can:

- avoid silent clobbering of newer state
- detect precondition failures
- coordinate metadata and index flushes safely

## Flush, Durability, and Recovery

Persistence in AndaDB is incremental rather than transactional in the relational-database sense.

### What `flush` Does

At collection level, `flush` persists:

- updated collection metadata
- document-id bitmap when needed
- dirty index state
- storage checkpoint metadata

At database level, `flush`:

- flushes all open collections
- persists database metadata

### `close` Semantics

`close` places the database or collection into read-only mode and then flushes pending changes. This is the expected end-of-life operation for a process that owns mutable state.

### Auto Flush

The database provides `auto_flush(cancel_token, interval)` for background periodic flushing. This is useful in agent runtimes that want bounded persistence lag without flushing after every single write.

### Recovery Strategy

On reopening a collection, the library:

- loads collection metadata
- loads the persisted document-id bitmap
- loads persisted indexes
- may auto-repair index state for documents that were written but not fully reflected in index persistence

This recovery path is one of the key reasons the crate is well suited to long-running agent memory processes.

## Read-Only Mode and Safety Controls

Both the database and collections can be switched into read-only mode.

This is useful for:

- controlled shutdown
- maintenance windows
- serving queries from a stable snapshot in-process

Database-level configuration also supports an optional opaque lock value. This lets applications enforce that only processes with the expected lock material may open a database for mutation.

## Metadata and Extensions

Both databases and collections support user-defined lightweight extensions.

These are appropriate for:

- application version markers
- ingestion cursors
- sync checkpoints
- small runtime hints
- policy flags

They are not appropriate for large payloads because they live in frequently read metadata objects.

## Error Model

The crate exposes a unified `DBError` enum.

Major categories include:

- generic errors
- schema errors
- storage errors
- index errors
- not found and already exists conditions
- precondition failures
- serialization failures
- payload-too-large errors

The error model intentionally preserves enough structure for callers to distinguish:

- logical application problems such as missing collections
- concurrency and conditional-write problems
- persistence failures from the underlying object store
- validation failures from schemas and indexes

## Example Workflow

This is the common end-to-end flow for an application using `anda_db`.

```rust
use anda_db::{
	database::{AndaDB, DBConfig},
	collection::CollectionConfig,
	index::HnswConfig,
	query::{Filter, Query, RangeQuery, Search},
	schema::{AndaDBSchema, Fv, vector_from_f32},
	storage::StorageConfig,
};
use object_store::local::LocalFileSystem;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, AndaDBSchema)]
struct Memory {
	_id: u64,
	created_at: u64,
	topic: String,
	body: String,
	embedding: anda_db::schema::Vector,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let store = Arc::new(LocalFileSystem::new_with_prefix("./data")?);
	let db = AndaDB::connect(
		store,
		DBConfig {
			name: "agent_memory".into(),
			description: "Long-term agent memory".into(),
			storage: StorageConfig::default(),
			lock: None,
		},
	)
	.await?;

	let schema = Memory::schema()?;
	let memories = db
		.open_or_create_collection(
			schema,
			CollectionConfig {
				name: "memories".into(),
				description: "Semantic and lexical memory store".into(),
			},
			async |c| {
				c.create_btree_index_nx(&["created_at"]).await?;
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

	let memory = Memory {
		_id: 0,
		created_at: 1_713_000_000,
		topic: "rust".into(),
		body: "Rust makes long-running agent services safer.".into(),
		embedding: vector_from_f32(vec![0.1, 0.2, 0.3, 0.4]),
	};

	let id = memories.add_from(&memory).await?;

	let results: Vec<Memory> = memories
		.search_as(Query {
			search: Some(Search {
				text: Some("rust agent safety".into()),
				vector: Some(vec![0.1, 0.2, 0.3, 0.4]),
				..Default::default()
			}),
			filter: Some(Filter::Field((
				"created_at".into(),
				RangeQuery::Ge(Fv::U64(1_700_000_000)),
			))),
			limit: Some(10),
		})
		.await?;

	assert!(!results.is_empty());
	let _ = id;
	db.close().await?;
	Ok(())
}
```

## Operational Guidance

### Choose Indexes Deliberately

Use B-Tree indexes for fields you filter on frequently. Use BM25 only for fields whose textual content should participate in retrieval. Use HNSW only for vector fields with stable dimensionality.

### Keep Extensions Small

Database and collection extensions should stay lightweight because they are persisted in hot metadata objects.

### Flush on a Policy, Not by Accident

For write-heavy agent systems, choose either:

- periodic `auto_flush`
- explicit flushes after ingestion batches
- explicit close on shutdown

Relying only on process exit is not a good persistence policy.

### Prefer Schema-Derived Models

When the application already has Rust structs for memories or knowledge objects, deriving `AndaDBSchema` reduces schema drift and keeps serialization aligned with storage.

### Tune Storage for Payload Shape

If documents and index objects are mostly small, caching and small-object writes are effective defaults. If payloads are large, revisit compression and chunk sizing in `StorageConfig`.

## Module Reference

### `database`

Defines:

- `AndaDB`
- `DBConfig`
- `DBMetadata`

Responsibilities:

- open, create, connect, and close the database
- own shared storage and open collections
- coordinate collection creation and deletion
- expose database metadata and extensions

### `collection`

Defines:

- `Collection`
- `CollectionConfig`
- `CollectionMetadata`
- `CollectionStats`

Responsibilities:

- validate, insert, update, remove, and read documents
- create and manage indexes
- execute filtering and hybrid retrieval
- expose collection stats, metadata, and extensions

### `query`

Defines:

- `Query`
- `Search`
- `Filter`
- `RRFReranker`
- re-exported `RangeQuery`

Responsibilities:

- describe retrieval intent independently of storage mechanics
- support hybrid retrieval and recursive filter composition

### `index`

Defines the index façade over the underlying workspace engines and includes `IndexHooks` for custom index-value extraction.

### `storage`

Defines:

- `Storage`
- `StorageConfig`
- `StorageMetadata`
- `StorageStats`
- `ObjectVersion`

Responsibilities:

- encode and decode persisted objects
- manage object versions and conditional writes
- expose cached reads and write helpers
- track storage-level metrics and checkpoints

### `schema`

Re-exports the type system from `anda_db_schema`, including:

- field types and field values
- schema builders and validation
- derived schema support
- document conversion helpers

## Relationship to Other Workspace Crates

The `anda_db` crate is the embedded core. Other crates in the workspace build on top of it:

- `anda_db_server` exposes HTTP RPC
- `anda_db_shard_proxy` adds sharded and multi-tenant routing
- `anda_cognitive_nexus` builds higher-level knowledge workflows
- `anda_kip` defines the protocol layer used by adjacent components

If you only need an in-process memory database in Rust, `anda_db` is the direct entry point.

## Summary

AndaDB is best understood as a schema-aware, embeddable, multi-index document store specialized for AI memory systems. Its strength is not only that it stores data, but that it keeps lexical, structural, and semantic retrieval close to the write path while remaining deployable as a normal Rust library.

For agent builders, that combination is the core value proposition: one collection model, one persistence layer, and multiple retrieval paths that can be fused into a single memory access pattern.
