# anda_db_derive

[![Crates.io](https://img.shields.io/crates/v/anda_db_derive.svg)](https://crates.io/crates/anda_db_derive)
[![Docs.rs](https://docs.rs/anda_db_derive/badge.svg)](https://docs.rs/anda_db_derive)

`anda_db_derive` is the procedural-macro layer of the
[AndaDB](https://github.com/ldclabs/anda-db) workspace. It turns ordinary Rust
structs into AndaDB schema definitions, so application data models and
storage schemas stay aligned with the serialized document shape.

Both macros are re-exported by `anda_db_schema` and by `anda_db::schema`;
most applications import them from there. Generated code resolves its types
through `anda_db_schema` (or the `anda_db::schema` re-export), so call sites
need no extra imports.

## What this crate provides

- `AndaDBSchema`: generates `schema() -> Result<Schema, SchemaError>` for a
  collection document. The struct needs a `_id: u64` field.
- `FieldTyped`: generates `field_type() -> FieldType` describing a nested
  struct as a `FieldType::Map`, plus a fallible `try_field_type()` that
  detects indirect recursion.
- Type inference for primitives, strings, bytes, `Vector`, `Option`, `Vec`,
  maps (including borrowed keys such as `BTreeMap<&str, u64>`) and nested
  `FieldTyped` structs.
- Field attributes:
  - `#[field_type = "..."]` overrides the inferred type with a small DSL:
    primitives (`Text`, `Bytes`, `U64`, …, or the Rust spellings `String`,
    `u64`, `bool`, …), `Array<T>`, `Option<T>` and `Map<String|Text|I64|Bytes, T>`;
  - `#[unique]` marks a field unique;
  - `#[cbor(key = N)]` uses an integer CBOR map key for nested structs that
    also derive `cbor2::Cbor`;
  - doc comments become field descriptions.
- serde awareness: `#[serde(rename = "...")]` and
  `#[serde(rename_all = "...")]` are honoured, and `#[serde(skip)]` /
  `#[serde(skip_serializing)]` fields are excluded, so the schema matches the
  serialized representation.
- Compile-time errors, with precise spans, for field names AndaDB cannot
  store, duplicate names, `_id` misuse, `#[serde(flatten)]`,
  `#[serde(transparent)]`, container `tag` / `into`, and container
  `#[cbor(array)]` / `#[cbor(tag = ...)]` (both derives describe untagged
  maps).

## Getting started

```toml
[dependencies]
anda_db_schema = "0.14"
serde = { version = "1", features = ["derive"] }
```

`anda_db_schema` re-exports the macros; depend on `anda_db_derive = "0.14"`
directly only when you do not use `anda_db_schema`.

```rust
use anda_db_derive::{AndaDBSchema, FieldTyped};
use serde::{Deserialize, Serialize};

/// A string newtype that inference cannot see through.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(transparent)]
struct Slug(String);

#[derive(Debug, Clone, Serialize, Deserialize, FieldTyped)]
struct Author {
    name: String,
    #[serde(rename = "mail")]
    email: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, AndaDBSchema)]
struct Article {
    _id: u64,
    /// The article title.
    #[unique]
    title: String,
    #[field_type = "Text"]
    slug: Slug,
    author: Author,
    tags: Vec<String>,
}

let schema = Article::schema().unwrap();
assert!(schema.get_field("title").unwrap().unique());
assert!(schema.get_field("slug").is_some());
assert!(Author::try_field_type().is_ok());
```

## Pitfalls

- A direct recursive field needs a non-recursive `#[field_type]` override.
  For nested derived types, `schema()` propagates errors from
  `try_field_type()`; the `field_type()` convenience method panics on an
  invalid declaration.
- `#[serde(skip_serializing_if = "...")]` on a non-`Option` field describes
  the field as required while serde may omit it, so `Document::try_from`
  later fails with "field ... is required". Use an `Option<T>` field instead.
- `#[serde(with = "...")]` and `serialize_with` can change the serialized
  shape; pair them with an explicit `#[field_type = "..."]` override.
- Fixed struct fields cannot use the wildcard sentinel names `"*"` or
  `i64::MIN`, which are reserved for open maps.

## Testing

```bash
cargo test -p anda_db_schema -p anda_db_derive
```

The Rust example in this README is compiled as a doctest, and the
`tests/ui` trybuild cases pin the compile-time diagnostics.

## Technical reference

- [docs/anda_db_derive.md](../../docs/anda_db_derive.md): attributes, type
  inference and the `field_type` DSL
- [docs/anda_db_schema.md](../../docs/anda_db_schema.md)

## Related crates

- [`anda_db_schema`](../anda_db_schema): the schema and document types
- [`anda_db`](../anda_db): the embedded database that consumes generated
  schemas

## License

MIT. See [LICENSE](../../LICENSE).
