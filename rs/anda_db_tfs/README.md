# anda_db_tfs

[![Crates.io](https://img.shields.io/crates/v/anda_db_tfs.svg)](https://crates.io/crates/anda_db_tfs)
[![Docs.rs](https://docs.rs/anda_db_tfs/badge.svg)](https://docs.rs/anda_db_tfs)

`anda_db_tfs` is the BM25 full-text search engine of
[AndaDB](https://github.com/ldclabs/anda-db): a thread-safe, embeddable
inverted index built for the long-term textual memory of AI agents.

## What this crate provides

- `BM25Index` with Okapi BM25 ranking and configurable `k1` / `b`
  (`BM25Params`), set per index (`BM25Config`) or per query.
- Composable tokenization through `TokenizerChain`: `default_tokenizer()`
  for Latin, Cyrillic and Arabic text, `jieba_tokenizer()` for Chinese, or
  any Tantivy tokenizer with filters.
- A boolean query language — `AND`, `OR`, `NOT` and parentheses — through
  `search_advanced` / `try_search_advanced`.
- Scoped search for a pre-filtered id set (`search_scoped`, and
  `prepare_scope` + `search_prepared_by` to reuse one scope across queries).
- Concurrent inserts, removes and searches from multiple threads.
- Incremental persistence: postings are sharded into buckets with a soft
  CBOR size target, `flush` / `flush_with` rewrite only dirty buckets, and
  `compact_buckets` repacks a fragmented index (best-fit decreasing).
- Strict loading (`load_all_strict`, `load_buckets_strict`) that refuses a
  missing bucket instead of serving an incomplete index.

## Feature flags

| Feature             | Default | Effect                                                   |
| ------------------- | ------- | -------------------------------------------------------- |
| `tantivy`           | yes     | Tantivy tokenizers and `default_tokenizer()`             |
| `tantivy-jieba`     | no      | `jieba_tokenizer()` for Chinese segmentation             |
| `full`              | no      | Both of the above                                        |

`anda_db` always depends on this crate with `full`.

## Getting started

```toml
[dependencies]
anda_db_tfs = { version = "0.14", features = ["full"] }
```

```rust
use anda_db_tfs::{BM25Index, jieba_tokenizer};

let index = BM25Index::new("notes".to_string(), jieba_tokenizer(), None);
index.insert(1, "Rust is fast and memory efficient", 0).unwrap();
index.insert(2, "Rust 安全、并发、实用", 0).unwrap();
index.insert(3, "Python is a dynamic language", 0).unwrap();

// Ranked keyword search: (document id, score), best first.
let hits = index.search("rust memory", 10, None);
assert_eq!(hits[0].0, 1);

// Chinese text is segmented by Jieba on both the index and query paths.
let hits = index.search("安全", 10, None);
assert_eq!(hits[0].0, 2);

// Boolean queries.
let hits = index.search_advanced("rust AND NOT python", 10, None);
assert_eq!(hits.len(), 2);
```

`search_advanced` swallows query errors as an empty result; use
`try_search_advanced` to tell an invalid query from no matches. A `NOT` that
needs the complement of the whole index is refused on indexes above 10,000
documents; inside an `AND` with a positive operand it has no such limit.

Persisting and reloading an index, with bucket objects written to files, is
shown end to end in the demo:

```bash
cargo run -p anda_db_tfs --example tfs_demo --features full
```

This crate is normally used through `anda_db`, which installs
`default_tokenizer()` whenever a collection opens; switch to Jieba with
`Collection::set_tokenizer` at the start of the open callback, before
recovery or index creation. It can also be used as a standalone embedded
BM25 engine.

## Validation and performance

```bash
cargo test -p anda_db_tfs --all-features
cargo bench -p anda_db_tfs --features full --bench tfs_index
cargo bench -p anda_db_tfs --features full --bench tfs_tokenizer
```

The tests include a property-based model check and allocation checks.
Measurements and the review checklist are in
[docs/anda_db_tfs_review.md](../../docs/anda_db_tfs_review.md).

## Technical reference

- [docs/anda_db_tfs.md](../../docs/anda_db_tfs.md): BM25, the tokenizer
  pipeline, bucket sharding and persistence layout
- [docs/anda_db.md](../../docs/anda_db.md): hybrid search in collections

## Related crates

- [`anda_db`](../anda_db): collection-level hybrid retrieval
- [`anda_db_hnsw`](../anda_db_hnsw): vector search fused with BM25 results
- [`anda_db_btree`](../anda_db_btree): filters that narrow a search

## License

MIT. See [LICENSE](../../LICENSE).
