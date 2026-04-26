# anda_db_tfs

`anda_db_tfs` is the BM25 full-text retrieval engine used by AndaDB. It is an
embedded text-search component designed for long-term textual memory, lexical
ranking, and tokenizer customization inside Rust applications.

## What This Crate Provides

- BM25 ranking for textual retrieval
- tokenizer pipelines for embedded search workloads
- optional jieba-based tokenization for CJK text
- concurrent reads and writes
- incremental persistence through dirty-bucket flushing
- reusable search primitives for the `anda_db` collection layer

## When to Use It

Use `anda_db_tfs` when you need:

- full-text search without operating an external search service
- lexical ranking over one or more document fields
- tokenizer control for multilingual AI-memory workloads
- incremental on-disk persistence for an embedded search index

## Getting Started

Add the crate to your project:

```toml
[dependencies]
anda_db_tfs = { version = "0.5", features = ["full"] }
```

This crate is normally used through `anda_db`, but it can also be used as a
standalone embedded BM25 engine.

## Technical Reference

Deep technical documentation for this crate lives in:

- [docs/anda_db_tfs.md](../../docs/anda_db_tfs.md)
- [docs/anda_db.md](../../docs/anda_db.md)

## Related Crates

- `anda_db` for collection-level hybrid retrieval
- `anda_db_hnsw` for vector search that can be fused with BM25 results

## License

MIT. See [LICENSE](../../LICENSE).
