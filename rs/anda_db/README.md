# anda_db

`anda_db` is the embedded database core of the AndaDB workspace. It gives Rust
applications schema-aware collections, object-store-backed persistence, and
three built-in retrieval modes: B-Tree for exact and range filters, BM25 for
full-text search, and HNSW for vector similarity search.

## What This Crate Provides

- Database lifecycle management through `AndaDB`
- Schema-aware collections and document CRUD
- B-Tree, BM25, and HNSW index integration
- Hybrid search with reciprocal-rank fusion reranking
- Object-store-backed persistence with checkpoints, flushing, and recovery
- Lightweight metadata extensions at database and collection scope

## When to Use It

Use `anda_db` when you want:

- an embedded database inside a Rust service
- durable AI memory without operating an external database service
- structured, lexical, and semantic retrieval in one collection model
- local development on filesystem or in-memory storage, with optional cloud object storage in production

## Getting Started

Add the crate to your project:

```toml
[dependencies]
anda_db = { version = "0.7", features = ["full"] }
object_store = { version = "0.13", features = ["fs"] }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

For a complete runnable example, see:

- `examples/db_demo.rs`
- `cargo run -p anda_db --example db_demo --features full`

For a product-level introduction, see the workspace root [README](../../README.md).

## Technical Reference

Deep technical documentation for this crate lives in:

- [docs/anda_db.md](../../docs/anda_db.md)
- [docs/anda_db_schema.md](../../docs/anda_db_schema.md)
- [docs/anda_db_btree.md](../../docs/anda_db_btree.md)
- [docs/anda_db_tfs.md](../../docs/anda_db_tfs.md)
- [docs/anda_db_hnsw.md](../../docs/anda_db_hnsw.md)
- [docs/anda_object_store.md](../../docs/anda_object_store.md)

## Related Crates

- `anda_db_schema` for field types, schemas, and documents
- `anda_db_derive` for `AndaDBSchema` and `FieldTyped`
- `anda_db_btree` for exact and range indexing
- `anda_db_tfs` for BM25 full-text search
- `anda_db_hnsw` for vector search
- `anda_object_store` for metadata and encryption wrappers over `object_store`

## License

MIT. See [LICENSE](../../LICENSE).
