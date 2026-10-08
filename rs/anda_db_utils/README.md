# anda_db_utils

[![Crates.io](https://img.shields.io/crates/v/anda_db_utils.svg)](https://crates.io/crates/anda_db_utils)
[![Docs.rs](https://docs.rs/anda_db_utils/badge.svg)](https://docs.rs/anda_db_utils)

`anda_db_utils` is a small, dependency-light utility crate maintained
alongside the [AndaDB](https://github.com/ldclabs/anda-db) workspace. It
remains available to downstream users; no other workspace crate depends on
it.

## What this crate provides

- `UniqueVec<T>`: an insertion-ordered vector that rejects duplicates, with
  O(1) membership checks (`contains`, `push`, `extend`), order-preserving
  removal (`remove`, `remove_if`, `retain`), `swap_remove_if`,
  `intersect_with`, and conversions to `Vec` or a hash set. It serializes as
  a plain sequence through serde.

0.14 removed the former `Pipe` and `CountingWriter` helpers; see the
[changelog](../../CHANGELOG.md).

## Getting started

```toml
[dependencies]
anda_db_utils = "0.14"
```

```rust
use anda_db_utils::UniqueVec;

let mut tags = UniqueVec::from(vec!["rust", "db"]);
assert!(!tags.push("rust")); // already present
assert!(tags.push("ai"));
assert_eq!(tags.as_ref(), &["rust", "db", "ai"]);
```

## Trade-offs

- Every element is stored twice — in the ordered `Vec` and in a membership
  set — trading roughly twice the memory for constant-time duplicate checks.
- Hashing uses unseeded FxHash, like the rest of the workspace. It has no
  collision resistance: avoid it where an adversary controls the keys and
  hash flooding is a concern.

## Testing

```bash
cargo test -p anda_db_utils
cargo bench -p anda_db_utils --bench unique_vec
```

## License

MIT. See [LICENSE](../../LICENSE).
