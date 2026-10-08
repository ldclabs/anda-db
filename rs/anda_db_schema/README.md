# anda_db_schema

[![Crates.io](https://img.shields.io/crates/v/anda_db_schema.svg)](https://crates.io/crates/anda_db_schema)
[![Docs.rs](https://docs.rs/anda_db_schema/badge.svg)](https://docs.rs/anda_db_schema)

`anda_db_schema` is the type-system layer of the
[AndaDB](https://github.com/ldclabs/anda-db) workspace. It defines the field
types, field values, schemas and documents shared by the embedded database,
its derive macros and the higher-level memory components. `anda_db`
re-exports it as `anda_db::schema`.

## What this crate provides

- `FieldType` (alias `Ft`): the closed set of declarable types — `Bool`,
  `I64`, `U64`, `F64`, `F32`, `Bytes`, `Text`, `Json`, `Vector` (`bf16`),
  `Array`, `Map` (typed keys, with wildcard keys for open maps) and `Option`.
- `FieldValue` (alias `Fv`): the runtime value validated against a
  `FieldType`, convertible to and from CBOR (`Cbor`) and serde-compatible in
  both JSON and CBOR.
- `FieldEntry` (alias `Fe`): one field's name, type, description, uniqueness
  flag and stable numeric index, used as the on-disk key.
- `Schema` and `SchemaBuilder`: an ordered, versioned set of field entries
  with an implicit unique `_id: U64`, and forward-compatible upgrades through
  `Schema::upgrade_with`.
- `Document` (bound to a schema) and `DocumentOwned` (standalone).
- `Resource`: a predefined structure for external files, blobs and URIs.
- `Vector` (`Vec<bf16>`) with `vector_from_f32` / `vector_from_f64`.
- The `AndaDBSchema` and `FieldTyped` derive macros, re-exported from
  [`anda_db_derive`](../anda_db_derive).

## Getting started

```toml
[dependencies]
anda_db_schema = "0.14"
serde = { version = "1", features = ["derive"] }
```

Derive a schema and round-trip a typed value through a `Document`:

```rust
use anda_db_schema::{AndaDBSchema, Document, Vector, vector_from_f32};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, PartialEq, Serialize, Deserialize, AndaDBSchema)]
struct Note {
    _id: u64,
    body: String,
    embedding: Vector,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Note::schema()?);
    let note = Note {
        _id: 1,
        body: "A persistent memory".into(),
        embedding: vector_from_f32(vec![0.25, 0.5]),
    };
    let doc = Document::try_from(schema, &note)?;
    let restored: Note = doc.try_into()?;
    assert_eq!(restored, note);
    Ok(())
}
```

Or build one by hand and fill a document field by field:

```rust
use anda_db_schema::{Document, FieldEntry, FieldType, Fv, Schema};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = Schema::builder();
    builder
        .with_version(1)
        .add_field(FieldEntry::new("title".into(), FieldType::Text)?.with_unique())?
        .add_field(FieldEntry::new(
            "tags".into(),
            FieldType::Option(Box::new(FieldType::Array(vec![FieldType::Text]))),
        )?)?;
    let schema = Arc::new(builder.build()?);
    assert_eq!(schema.version(), 1);

    let mut doc = Document::new(schema);
    doc.set_id(1);
    doc.set_field("title", Fv::Text("Hello".into()))?;
    assert_eq!(doc.get_field("title"), Some(&Fv::Text("Hello".into())));
    Ok(())
}
```

## Serialization rules

- Persistence normalizes values to CBOR. `NaN` is rejected so `FieldValue`
  keeps a meaningful equality.
- JSON serialization rejects non-finite `F32`/`F64` values instead of writing
  `null`; binary CBOR still represents infinities.
- Human-readable formats separate text from bytes with explicit `txt:` and
  `b64:` prefixes.
- An untyped round trip keeps the data but normalizes some variants (`F32` →
  `F64`, non-negative `I64` → `U64`, `Vector` → `Array(U64)`); extracting
  with the declared `FieldType` restores the declared variant.

## Schema upgrades

`Schema::upgrade_with` carries persisted field indexes and nested-key
deletion history from the previous schema, so stored documents stay readable.
An upgrade needs a higher version, and new fields must be optional. An older
schema with incomplete history needs a full scan of raw documents and pending
recovery images before new fields can be allocated: the AndaDB collection
performs that scan automatically during an upgrade, and custom storage
integrations use `Schema::history_recovery()`.

## Testing

```bash
cargo test -p anda_db_schema -p anda_db_derive
```

The Rust examples in this README are compiled and run as doctests.

## Technical reference

- [docs/anda_db_schema.md](../../docs/anda_db_schema.md): type system,
  document model and on-disk format
- [docs/anda_db_derive.md](../../docs/anda_db_derive.md)
- [Schemas and CBOR quick reference](../../skills/anda-db/references/schema_and_cbor.md)

## Related crates

- [`anda_db`](../anda_db): the embedded database built on this type system
- [`anda_db_derive`](../anda_db_derive): `AndaDBSchema` and `FieldTyped`

## License

MIT. See [LICENSE](../../LICENSE).
