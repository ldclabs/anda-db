# anda_db_schema

`anda_db_schema` is the type-system layer of the AndaDB workspace. It defines
field types, field values, field entries, schemas, documents, and helper types
used across the embedded database, derive macros, and higher-level memory
components.

## What This Crate Provides

- `FieldType` for schema-level type declarations
- `FieldValue` for runtime values validated against schemas
- `FieldEntry` for per-field metadata such as type, uniqueness, and description
- `Schema` and schema builders
- `Document` and `DocumentOwned`
- `Resource` and other shared model types used by the workspace

## When to Use It

Use `anda_db_schema` when you need to:

- define or inspect collection schemas directly
- construct documents programmatically
- validate field values before insertion
- build tooling around AndaDB's type model
- share the same document vocabulary across multiple crates

## Getting Started

Add the crate to your project:

```toml
[dependencies]
anda_db_schema = "0.4"
serde = { version = "1", features = ["derive"] }
```

This crate is commonly paired with `anda_db_derive` when you want schemas to be
generated automatically from Rust structs.

## Technical Reference

Deep technical documentation for this crate lives in:

- [docs/anda_db_schema.md](../../docs/anda_db_schema.md)
- [docs/anda_db_derive.md](../../docs/anda_db_derive.md)

## Related Crates

- `anda_db` for the embedded database built on top of this type system
- `anda_db_derive` for `AndaDBSchema` and `FieldTyped`

## License

MIT. See [LICENSE](../../LICENSE).
