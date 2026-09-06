# anda_db_derive

`anda_db_derive` is the procedural-macro layer of the AndaDB workspace. It
turns ordinary Rust structs into AndaDB schema definitions, reducing boilerplate
and keeping application data models aligned with the database type system.

## What This Crate Provides

- `AndaDBSchema` for generating a complete `Schema`
- `FieldTyped` for generating nested `FieldType` descriptions, with a fallible
  `try_field_type()` constructor that detects indirect recursion
- support for `#[field_type = "..."]` overrides
- support for `#[unique]`
- serde-aware naming: `#[serde(rename = "...")]` and
  `#[serde(rename_all = "...")]` are honoured, and
  `#[serde(skip)]` / `#[serde(skip_serializing)]` fields are excluded, so the
  generated schema always matches the serialized representation
- compile-time validation: field names AndaDB cannot store, duplicate names,
  `_id` misuse, and unsupported `#[serde(flatten)]`, `#[serde(transparent)]`, container `tag` / `into`
  are reported with precise spans
- extraction of doc comments into schema field descriptions

## When to Use It

Use `anda_db_derive` when you want to:

- define collection schemas from Rust structs instead of building them manually
- keep application models and storage schemas synchronized
- generate nested map-like field types from user-defined structs
- reduce repetitive schema boilerplate in embedded database code

## Getting Started

Add the derive crate alongside `anda_db_schema`:

```toml
[dependencies]
anda_db_schema = "0.11"
anda_db_derive = "0.11"
serde = { version = "1", features = ["derive"] }
```

Typical usage:

```rust
use anda_db_derive::AndaDBSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, AndaDBSchema)]
struct MyDoc {
    _id: u64,
    title: String,
    body: String,
}

assert!(MyDoc::schema().is_ok());
```

Direct recursive fields require a non-recursive `#[field_type]` override.
For derived nested types, `schema()` propagates construction errors from
`try_field_type()`. The existing `field_type()` convenience method remains
available and panics on an invalid declaration.

Borrowed map keys such as `BTreeMap<&str, u64>` and `BTreeMap<&[u8], String>`
are inferred through their serialized key types. Fixed struct fields cannot
use the wildcard sentinel names `"*"` or `i64::MIN`.

## Technical Reference

Deep technical documentation for this crate lives in:

- [docs/anda_db_derive.md](../../docs/anda_db_derive.md)
- [docs/anda_db_schema.md](../../docs/anda_db_schema.md)

## Related Crates

- `anda_db_schema` for the underlying schema and document types
- `anda_db` for the embedded database that consumes generated schemas

## License

MIT. See [LICENSE](../../LICENSE).
