# anda_db_utils

`anda_db_utils` provides standalone collection and serialization utilities.
It remains available to downstream users; current workspace production code
does not depend on these helpers.

## What This Crate Provides

- `UniqueVec<T>` for ordered unique collections

## When to Use It

Use `anda_db_utils` when you need:

- deterministic uniqueness with vector-like iteration order
- a small utility dependency for application code

## Getting Started

Add the crate to your project:

```toml
[dependencies]
anda_db_utils = "0.14"
```

## Related Crates

- `anda_db_btree` and `anda_db_tfs` for embedded index implementations
- `anda_db` for the top-level embedded database layer

## License

MIT. See [LICENSE](../../LICENSE).
