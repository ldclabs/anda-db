# anda_db_utils

`anda_db_utils` contains small supporting utilities shared across the AndaDB
workspace. It is not an end-user database crate on its own, but it provides
reusable building blocks used by indexing, persistence, and supporting code.

## What This Crate Provides

- `UniqueVec<T>` for ordered unique collections
- `Pipe` for lightweight functional-style chaining
- `CountingWriter` for byte-counting during serialization workflows
- `estimate_cbor_size` / `try_estimate_cbor_size` for CBOR size estimation
	without buffers
- small helper primitives reused by multiple workspace crates

## When to Use It

Use `anda_db_utils` when you need:

- deterministic uniqueness with vector-like iteration order
- serialization size estimation without materializing full output buffers
- a small utility dependency shared with the rest of the AndaDB stack

## Getting Started

Add the crate to your project:

```toml
[dependencies]
anda_db_utils = "0.2"
```

This crate is most often consumed indirectly through higher-level workspace
crates such as `anda_db`, `anda_db_btree`, and `anda_db_tfs`.

## Related Crates

- `anda_db_btree` and `anda_db_tfs` for embedded index implementations that use these helpers
- `anda_db` for the top-level embedded database layer

## License

MIT. See [LICENSE](../../LICENSE).
