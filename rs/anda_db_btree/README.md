# anda_db_btree

`anda_db_btree` is the exact-match and range-index engine used by AndaDB. It is
an embedded, in-memory inverted B-tree with incremental persistence, designed to
support high-concurrency filtering workloads in AI memory systems.

## What This Crate Provides

- exact-match lookup on scalar keys
- range queries over ordered values
- prefix queries for string-like keys
- incremental persistence through bucketized storage
- concurrent reads and writes without an external service
- boolean composition through `RangeQuery`

## When to Use It

Use `anda_db_btree` when you need:

- collection filters such as equality, greater-than, less-than, or ranges
- uniqueness and exact lookup over structured fields
- an embeddable index rather than a standalone search engine
- durable index flushing with partial rewrites of dirty buckets

## Getting Started

Add the crate to your project:

```toml
[dependencies]
anda_db_btree = "0.5"
```

This crate is normally used through `anda_db`, but it can also be embedded
independently in lower-level storage or indexing code.

## Technical Reference

Deep technical documentation for this crate lives in:

- [docs/anda_db_btree.md](../../docs/anda_db_btree.md)
- [docs/anda_db.md](../../docs/anda_db.md)

## Related Crates

- `anda_db` for collection-level query execution
- `anda_db_utils` for supporting utilities such as `UniqueVec`

## License

MIT. See [LICENSE](../../LICENSE).
