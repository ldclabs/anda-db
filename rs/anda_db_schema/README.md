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
anda_db_schema = "0.14"
serde = { version = "1", features = ["derive"] }
```

This crate is commonly paired with `anda_db_derive` when you want schemas to be
generated automatically from Rust structs.

A typed document round trip:

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

Schema upgrades preserve field indexes and nested-key deletion history.
An older schema with incomplete history needs a full scan of raw documents
and pending recovery images before new fields can be allocated. The core
AndaDB collection performs this scan automatically during upgrade; custom
storage integrations use `Schema::history_recovery()`.

JSON serialization rejects non-finite `F32`/`F64` values instead of silently
writing null. Binary CBOR still represents infinities. Text/bytes are
separated using explicit `txt:`/`b64:` prefixes in human-readable formats.

## Technical Reference

Deep technical documentation for this crate lives in:

- [docs/anda_db_schema.md](../../docs/anda_db_schema.md)
- [docs/anda_db_derive.md](../../docs/anda_db_derive.md)

## Related Crates

- `anda_db` for the embedded database built on top of this type system
- `anda_db_derive` for `AndaDBSchema` and `FieldTyped`

## License

MIT. See [LICENSE](../../LICENSE).
