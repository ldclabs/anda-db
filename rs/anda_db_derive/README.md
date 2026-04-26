# anda_db_derive

`anda_db_derive` is the procedural-macro layer of the AndaDB workspace. It
turns ordinary Rust structs into AndaDB schema definitions, reducing boilerplate
and keeping application data models aligned with the database type system.

## What This Crate Provides

- `AndaDBSchema` for generating a complete `Schema`
- `FieldTyped` for generating nested `FieldType` descriptions
- support for `#[field_type = "..."]` overrides
- support for `#[unique]` and `#[serde(rename = "...")]`
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
anda_db_schema = "0.4"
anda_db_derive = "0.4"
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
```

## Technical Reference

Deep technical documentation for this crate lives in:

- [docs/anda_db_derive.md](../../docs/anda_db_derive.md)
- [docs/anda_db_schema.md](../../docs/anda_db_schema.md)

## Related Crates

- `anda_db_schema` for the underlying schema and document types
- `anda_db` for the embedded database that consumes generated schemas

## License

MIT. See [LICENSE](../../LICENSE).
