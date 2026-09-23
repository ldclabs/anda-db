# anda_db_utils

`anda_db_utils` provides standalone collection and serialization utilities.
It remains available to downstream users; current workspace production code
does not depend on these helpers.

## What This Crate Provides

- `UniqueVec<T>` for ordered unique collections
- `Pipe` for lightweight functional-style chaining
- `CountingWriter` for byte-counting during serialization workflows

## When to Use It

Use `anda_db_utils` when you need:

- deterministic uniqueness with vector-like iteration order
- a small utility dependency for application code

## Getting Started

Add the crate to your project:

```toml
[dependencies]
anda_db_utils = "0.13"
```

For CBOR encoded sizes, prefer `cbor2::serialized_size`; `CountingWriter`
remains useful as a general-purpose counting sink for other formats.

## Related Crates

- `anda_db_btree` and `anda_db_tfs` for embedded index implementations
- `anda_db` for the top-level embedded database layer

## License

MIT. See [LICENSE](../../LICENSE).
