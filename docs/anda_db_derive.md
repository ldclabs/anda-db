# `anda_db_derive` Technical Documentation

**Crate version**: 0.4
**Last updated**: 2026-04-25

---

## Table of Contents

1. [Overview](#1-overview)
2. [Derive Macros](#2-derive-macros)
3. [Attributes](#3-attributes)
4. [Type Inference Rules](#4-type-inference-rules)
5. [The `field_type` DSL](#5-the-field_type-dsl)
6. [Usage Examples](#6-usage-examples)
7. [Internal Implementation](#7-internal-implementation)
8. [Diagnostics](#8-diagnostics)

---

## 1. Overview

### 1.1 Purpose

`anda_db_derive` is a procedural-macro crate that generates AndaDB schema
glue code from ordinary Rust structs. Two derives are exported and re-exported
through `anda_db_schema`:

| Macro          | Generated method                                 |
| -------------- | ------------------------------------------------ |
| `FieldTyped`   | `pub fn field_type() -> FieldType`               |
| `AndaDBSchema` | `pub fn schema() -> Result<Schema, SchemaError>` |

Both macros operate on structs with named fields. Tuple structs, unit
structs, enums and unions are rejected with a `compile_error!`.

### 1.2 Crate layout

```
rs/anda_db_derive/
├── src/
│   ├── lib.rs           # public macro entry points
│   ├── schema.rs        # AndaDBSchema implementation
│   ├── field_typed.rs   # FieldTyped implementation
│   └── common.rs        # shared parsing / type-inference helpers
├── Cargo.toml
└── README.md
```

### 1.3 Required scope

Generated code references `FieldType`, `FieldKey`, `FieldEntry`, `Schema` and
`SchemaError` by bare name. The recommended pattern is to import them via the
`anda_db_schema` prelude (or a glob import) at the call site.

---

## 2. Derive Macros

### 2.1 `FieldTyped`

```rust
#[proc_macro_derive(FieldTyped, attributes(field_type))]
```

For each named field, the macro emits one `(key, FieldType)` tuple and packs
them into a `FieldType::Map`. The resulting `field_type()` is what
`AndaDBSchema` (and `determine_field_type`) call when they encounter a
nested user-defined struct.

```rust
use anda_db_schema::{FieldType, FieldTyped};

#[derive(FieldTyped)]
struct User {
    id: u64,
    name: String,
    age: u32,
}

// Expanded:
impl User {
    pub fn field_type() -> FieldType {
        FieldType::Map(
            vec![
                ("id".into(),   FieldType::U64),
                ("name".into(), FieldType::Text),
                ("age".into(),  FieldType::U64),
            ]
            .into_iter()
            .collect(),
        )
    }
}
```

### 2.2 `AndaDBSchema`

```rust
#[proc_macro_derive(AndaDBSchema, attributes(field_type, unique))]
```

Builds a complete `Schema` via `Schema::builder()`. The `_id: u64` field is
**mandatory** — the macro validates its type at compile time and skips it
during code generation, since the builder injects the primary-key column
itself.

```rust
use anda_db_derive::AndaDBSchema;
use anda_db_schema::{FieldEntry, FieldType, Schema, SchemaError};

#[derive(AndaDBSchema)]
struct Article {
    /// Article unique identifier
    _id: u64,
    /// Article title
    title: String,
    /// Article content
    content: String,
    /// View count
    views: u64,
}

// Expanded (abbreviated):
impl Article {
    pub fn schema() -> Result<Schema, SchemaError> {
        let mut builder = Schema::builder();
        builder.add_field(
            FieldEntry::new("title".to_string(), FieldType::Text)?
                .with_description("Article title".to_string()),
        )?;
        builder.add_field(
            FieldEntry::new("content".to_string(), FieldType::Text)?
                .with_description("Article content".to_string()),
        )?;
        builder.add_field(
            FieldEntry::new("views".to_string(), FieldType::U64)?
                .with_description("View count".to_string()),
        )?;
        builder.build()
    }
}
```

---

## 3. Attributes

| Attribute                   | Applies to                   | Effect                                                                                              |
| --------------------------- | ---------------------------- | --------------------------------------------------------------------------------------------------- |
| `#[field_type = "..."]`     | `FieldTyped`, `AndaDBSchema` | Override the inferred field type using the [DSL](#5-the-field_type-dsl).                            |
| `#[unique]`                 | `AndaDBSchema` only          | Adds `FieldEntry::with_unique()` to the generated entry.                                            |
| `#[serde(rename = "name")]` | both                         | Use the renamed identifier as the schema field name. Other serde keys are ignored.                  |
| `/// doc comment`           | `AndaDBSchema` only          | All `///` lines on a field are joined with a space and emitted as `FieldEntry::with_description()`. |

Notes:

- `#[unique]` is silently ignored by `FieldTyped` because that macro does not
  generate a `Schema`.
- Multiple `///` lines are concatenated; empty doc lines are dropped before
  joining.
- Only the first `serde(rename = "...")` is honoured; other serde syntax that
  fails to parse is skipped without raising an error.

---

## 4. Type Inference Rules

When `#[field_type]` is **not** present, the macros walk the field's Rust
type and produce a `FieldType` token stream.

### 4.1 Primitives

| Rust type                          | `FieldType` |
| ---------------------------------- | ----------- |
| `bool`                             | `Bool`      |
| `i8`, `i16`, `i32`, `i64`, `isize` | `I64`       |
| `u8`, `u16`, `u32`, `u64`, `usize` | `U64`       |
| `f32`                              | `F32`       |
| `f64`                              | `F64`       |

### 4.2 Strings and bytes

| Rust type                                                              | `FieldType` |
| ---------------------------------------------------------------------- | ----------- |
| `String`, `&str`                                                       | `Text`      |
| `Vec<u8>`, `[u8; N]`                                                   | `Bytes`     |
| `Bytes`, `ByteArray`, `ByteBuf`                                        | `Bytes`     |
| `BytesB64`, `ByteArrayB64`, `ByteBufB64`                               | `Bytes`     |
| `serde_bytes::Bytes`, `serde_bytes::ByteArray`, `serde_bytes::ByteBuf` | `Bytes`     |

### 4.3 Vectors and JSON

| Rust type                   | `FieldType` |
| --------------------------- | ----------- |
| `Vec<bf16>`, `[bf16; N]`    | `Vector`    |
| `Json`, `serde_json::Value` | `Json`      |

> Bare `bf16` (or `half::bf16`) is **not** a valid field type. Wrap it in
> a `Vec` to obtain `Vector`, or annotate the field with `#[field_type =
> "F32"]` if a scalar is desired.

### 4.4 Collections

| Rust type                                               | `FieldType`       |
| ------------------------------------------------------- | ----------------- |
| `Vec<T>`, `HashSet<T>`, `BTreeSet<T>`                   | `Array(T)`        |
| `[T; N]` (with `T` a supported non-byte/non-bf16 type)  | `Array(T)`        |
| `HashMap<K, V>`, `BTreeMap<K, V>`, `serde_json::Map<…>` | `Map({"*" => V})` |

For maps the key `K` must be one of:

- a string-like type (`String`, `&str`) → wildcard text key `"*"`
- a bytes-like type (`Vec<u8>`, `Bytes`, `ByteArray`, `ByteBuf`, `*B64`) →
  wildcard bytes key `b"*"`

Any other key type is a compile error.

### 4.5 Optionality and user-defined types

| Rust type                             | `FieldType`                                        |
| ------------------------------------- | -------------------------------------------------- |
| `Option<T>`                           | `Option(T)`                                        |
| Any other path (single segment) `Foo` | `Foo::field_type()` — **must** derive `FieldTyped` |

Selected fully qualified paths are recognised explicitly even if the leading
segment is not the type name:

- `serde_bytes::Bytes` / `ByteArray` / `ByteBuf` → `Bytes`
- `serde_json::Value` → `Json`
- `half::bf16` → compile error (with guidance)

---

## 5. The `field_type` DSL

The string passed to `#[field_type = "..."]` is parsed by
`parse_field_type_str` and accepts the grammar below (whitespace anywhere
is ignored):

```text
type        := primitive | array | option | map
primitive   := "Bytes" | "Text" | "U64" | "I64"
             | "F64"   | "F32"  | "Bool" | "Json" | "Vector"
array       := "Array<" type ">"
option      := "Option<" type ">"
map         := "Map<" map_key "," type ">"
map_key     := "String" | "Text" | "Bytes"
```

### 5.1 String / Text equivalence

`String` and `Text` are **synonyms** for map keys: `FieldType` only has a
`Text` variant, but `Map<String, T>` reads more naturally for users coming
from `HashMap<String, _>`. Both:

```rust
#[field_type = "Map<String, Json>"]
#[field_type = "Map<Text, Json>"]
```

expand to the same wildcard `Map({"*" => Json})`.

### 5.2 Examples

| DSL string                  | `FieldType`                  |
| --------------------------- | ---------------------------- |
| `"Bytes"`                   | `Bytes`                      |
| `"Array<U64>"`              | `Array(U64)`                 |
| `"Option<Text>"`            | `Option(Text)`               |
| `"Map<String, Json>"`       | `Map({"*" => Json})`         |
| `"Map<Text, Array<U64>>"`   | `Map({"*" => Array(U64)})`   |
| `"Map<Bytes, F64>"`         | `Map({b"*" => F64})`         |
| `"Option<Map<Bytes, F64>>"` | `Option(Map({b"*" => F64}))` |

### 5.3 Diagnostic guarantees

Unrecognised input produces a `compile_error!` at the original macro span:

```text
Unsupported field type: '...'. Supported types: Bytes, Text, U64, I64,
F64, F32, Bool, Json, Vector, Array<T>, Option<T>, Map<String, T>,
Map<Text, T>, Map<Bytes, T>
```

```text
Unsupported Map key type: '...'. Expected 'String', 'Text' or 'Bytes'.
```

```text
Invalid Map field type: '...'. Expected 'Map<KeyType, ValueType>'.
```

---

## 6. Usage Examples

### 6.1 Full schema with mixed features

```rust
use anda_db_derive::AndaDBSchema;
use anda_db_schema::{Document, FieldEntry, FieldType, Fv, Schema, SchemaError, bf16};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(AndaDBSchema, Serialize, Deserialize, Debug)]
struct Article {
    /// AndaDB-managed primary key
    _id: u64,
    /// Article title (uniquely indexed)
    #[unique]
    title: String,
    /// Article body
    content: String,
    /// Cumulative view counter
    views: u64,
    /// Publication flag
    published: bool,
    /// Optional list of tags
    tags: Option<Vec<String>>,
    /// Free-form author metadata
    author_meta: Option<serde_json::Value>,
    /// Embedding vector
    embedding: Vec<bf16>,
}

fn main() -> Result<(), SchemaError> {
    let schema = Arc::new(Article::schema()?);

    let mut doc = Document::new(schema.clone());
    doc.set_id(1);
    doc.set_field("title",     Fv::Text("Hello World".into()))?;
    doc.set_field("content",   Fv::Text("This is the body".into()))?;
    doc.set_field("views",     Fv::U64(0))?;
    doc.set_field("published", Fv::Bool(true))?;
    doc.set_field("embedding", Fv::Vector(vec![bf16::from_f32(0.1); 512]))?;
    Ok(())
}
```

### 6.2 Nesting a `FieldTyped` struct

```rust
use anda_db_derive::{AndaDBSchema, FieldTyped};
use anda_db_schema::FieldType;

#[derive(FieldTyped, Debug)]
struct GeoLocation {
    latitude: f64,
    longitude: f64,
}

#[derive(AndaDBSchema)]
struct Place {
    _id: u64,
    name: String,
    /// `GeoLocation::field_type()` is invoked at expansion time.
    location: GeoLocation,
}
```

### 6.3 Overriding inferred types

```rust
use anda_db_derive::AndaDBSchema;
use ic_auth_types::Xid;

#[derive(AndaDBSchema)]
struct Transaction {
    _id: u64,
    /// Treat the `Xid` newtype as a fixed-length byte field.
    #[field_type = "Bytes"]
    tx_id: Xid,
    /// Optional list of byte ids — three nested DSL constructs in one go.
    #[field_type = "Option<Array<Bytes>>"]
    inputs: Option<Vec<Xid>>,
    amount: u64,
}
```

---

## 7. Internal Implementation

### 7.1 `common.rs`

| Symbol                 | Responsibility                                                |
| ---------------------- | ------------------------------------------------------------- |
| `find_rename_attr`     | Extract `serde(rename = "...")`.                              |
| `find_field_type_attr` | Extract and parse `#[field_type = "..."]`.                    |
| `parse_field_type_str` | Compile the `field_type` DSL into a `FieldType` token stream. |
| `determine_field_type` | Infer `FieldType` directly from a `syn::Type`.                |
| `is_u8_type`           | Predicate for `u8`.                                           |
| `is_string_type`       | Predicate for `String` / `str`.                               |
| `is_bytes_type`        | Predicate for the supported byte container types.             |
| `is_bf16_type`         | Predicate for `bf16`.                                         |
| `is_u64_type`          | Predicate used to validate the `_id: u64` requirement.        |

#### Map parsing details

`parse_field_type_str` finds the **top-level** comma inside `Map<...>` by
counting angle-bracket depth. This is what allows nested types such as
`Map<Text, Array<U64>>` or `Option<Map<Bytes, F64>>` to parse correctly.
Whitespace is trimmed on both sides of every separator, so writes like
`Map< Text , Array<U64> >` are also accepted.

### 7.2 `schema.rs`

Pipeline executed by `anda_db_schema_derive`:

1. Parse the input as `DeriveInput`.
2. Reject anything that is not a struct with named fields.
3. For every field:
   - read the (optional) serde rename;
   - extract `///` doc comments (joined with spaces);
   - resolve the field type via `find_field_type_attr` *or*
     `determine_field_type`;
   - if the field is `_id`, verify it is `u64` and skip code generation;
   - look for `#[unique]` and choose between four `FieldEntry` builder
     templates (with/without description × with/without unique).
4. Emit `impl <Struct> { pub fn schema() -> Result<Schema, SchemaError> { … } }`.

### 7.3 `field_typed.rs`

Same parse / validation prelude as `schema.rs`. For each field the macro
produces a `(rename_or_name.into(), <field_type>)` tuple and collects them
into a single `FieldType::Map`.

### 7.4 Worked example

Input:

```rust
#[derive(AndaDBSchema)]
struct User {
    _id: u64,
    /// User's email
    #[unique]
    email: String,
}
```

Expansion:

```rust
impl User {
    pub fn schema() -> Result<Schema, SchemaError> {
        let mut builder = Schema::builder();
        builder.add_field(
            FieldEntry::new("email".to_string(), FieldType::Text)?
                .with_description("User's email".to_string())
                .with_unique(),
        )?;
        builder.build()
    }
}
```

---

## 8. Diagnostics

### 8.1 Compile-time errors

| Message                                                                           | Cause                                                                          |
| --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| `FieldTyped only supports structs`                                                | Applied to an enum or union.                                                   |
| `FieldTyped only supports structs with named fields`                              | Applied to a tuple or unit struct.                                             |
| `AndaDBSchema only supports structs`                                              | Applied to an enum or union.                                                   |
| `AndaDBSchema only supports structs with named fields`                            | Applied to a tuple or unit struct.                                             |
| `The '_id' field must be of type u64`                                             | The struct declares `_id` with a type other than `u64`.                        |
| `Unsupported field type: '...'. Supported types: …`                               | DSL string in `#[field_type]` was not recognised.                              |
| `Unsupported Map key type: '...'. Expected 'String', 'Text' or 'Bytes'.`          | Unsupported key in `Map<K, V>` DSL.                                            |
| `Invalid Map field type: '...'. Expected 'Map<KeyType, ValueType>'.`              | DSL `Map<…>` string lacks a comma-separated key/value pair.                    |
| `Unsupported type: '...'. Consider: …`                                            | Inference failed (references, tuples, trait objects, etc.).                    |
| `Unable to determine Vec element type for: ...`                                   | Generic argument missing on a `Vec` / `HashSet` / `BTreeSet`.                  |
| `Map key type must be String or bytes (e.g., Vec<u8>, [u8; N]), found: ...`       | `HashMap`/`BTreeMap` key inferred as something neither string- nor bytes-like. |
| `Standalone \`half::bf16\` is not supported as a field type. Use \`Vec<bf16>\` …` | Bare `bf16` field without `Vec`/override.                                      |

### 8.2 Runtime errors

`schema()` ultimately calls into `Schema::builder().build()`, which can fail
with:

| Variant                   | Description                     |
| ------------------------- | ------------------------------- |
| `SchemaError::FieldName`  | Invalid field name.             |
| `SchemaError::FieldType`  | Invalid field type.             |
| `SchemaError::Validation` | Schema-level validation failed. |

---

*Document generated: 2026-04-25*
