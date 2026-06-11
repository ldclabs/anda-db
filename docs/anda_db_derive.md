# `anda_db_derive` Technical Documentation

**Crate version**: 0.5
**Last updated**: 2026-06-11

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

Builds a complete `Schema` via `Schema::builder()`. Declaring the `_id: u64`
field is **optional** — the builder injects the primary-key column either
way. When declared, the macro validates at compile time that it is `u64` and
that serde keeps serializing it as `"_id"` (beware `rename_all` rules), and
skips it during code generation.

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

| Attribute                          | Applies to                   | Effect                                                                                              |
| ---------------------------------- | ---------------------------- | --------------------------------------------------------------------------------------------------- |
| `#[field_type = "..."]`            | `FieldTyped`, `AndaDBSchema` | Override the inferred field type using the [DSL](#5-the-field_type-dsl).                            |
| `#[unique]`                        | `AndaDBSchema` only          | Adds `FieldEntry::with_unique()` to the generated entry.                                            |
| `#[serde(rename = "name")]`        | both                         | Use the serialized name as the schema field name (directional form: the `serialize` half is used).  |
| `#[serde(rename_all = "...")]`     | both (container level)       | Apply serde's case rule to all fields without an explicit rename, mirroring serde's precedence.     |
| `#[serde(skip)]` / `#[serde(skip_serializing)]` | both            | The field never appears in serialized output and is excluded from the schema / type map.            |
| `#[serde(flatten)]`                | both                         | **Rejected** with a compile error: flattened keys cannot be described by a per-field schema entry.  |
| `#[serde(transparent)]`            | both (container level)       | **Rejected** with a compile error: the struct serializes as its inner field, not as a map.          |
| `/// doc comment`                  | `AndaDBSchema` only          | All `///` lines on a field are joined with a space and emitted as `FieldEntry::with_description()`. |

Notes:

- `#[unique]` is silently ignored by `FieldTyped` because that macro does not
  generate a `Schema`.
- Multiple `///` lines are concatenated; empty doc lines are dropped before
  joining.
- Only the first `serde(rename = "...")` is honoured; other serde syntax that
  fails to parse is skipped without raising an error. `#[serde(with = "...")]`
  / `serialize_with` may change the serialized shape — combine them with an
  explicit `#[field_type = "..."]` override when they do.
- `AndaDBSchema` validates every resulting field name against AndaDB's naming
  rules (`[a-z0-9_]`, at most 64 bytes) at compile time, and also rejects
  duplicate names and collisions with the reserved `_id` column. Keys of
  nested `FieldTyped` maps are free-form, so case rules like `camelCase` are
  fine there, while on `AndaDBSchema` they are only usable when the resulting
  names stay valid (`snake_case` / `lowercase`, or explicit renames).

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

### 4.5 Optionality, smart pointers and user-defined types

| Rust type                                  | `FieldType`                                          |
| ------------------------------------------ | ---------------------------------------------------- |
| `Option<T>`                                | `Option(T)`                                          |
| `Box<T>` / `Arc<T>` / `Rc<T>` / `Cow<'_, T>` | the inner `T` (serde serializes these transparently) |
| Any other path `Foo` (incl. `Foo<G>`)      | `<Foo>::field_type()` — **must** derive `FieldTyped` |

Selected fully qualified paths are recognised explicitly even if the leading
segment is not the type name:

- `serde_bytes::Bytes` / `ByteArray` / `ByteBuf` → `Bytes`
- `serde_json::Value` → `Json`
- `half::bf16` → compile error (with guidance)

Parenthesized types (`(String)`) and the invisible groups produced by
`macro_rules!` substitution are unwrapped transparently, so macro-generated
structs infer the same way as hand-written ones.

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

| Symbol                       | Responsibility                                                      |
| ---------------------------- | ------------------------------------------------------------------- |
| `named_fields`               | Shared "struct with named fields" validation for both derives.      |
| `parse_container_serde_attrs`| Extract `rename_all` (incl. directional form) and `transparent`.    |
| `parse_field_serde_attrs`    | Extract `rename` (incl. directional form), `skip*` and `flatten`.   |
| `RenameRule`                 | serde-compatible `rename_all` case conversion.                      |
| `effective_field_name`       | Resolve the serialized name (explicit rename wins over rename_all). |
| `validate_schema_field_name` | Compile-time mirror of `anda_db_schema::validate_field_name`.       |
| `resolve_field_type`         | `#[field_type]` override or fall back to inference.                 |
| `find_field_type_attr`       | Extract and parse `#[field_type = "..."]`.                          |
| `parse_field_type_str`       | Compile the `field_type` DSL into a `FieldType` token stream.       |
| `determine_field_type`       | Infer `FieldType` directly from a `syn::Type`.                      |
| `is_u8_type`                 | Predicate for `u8`.                                                 |
| `is_string_type`             | Predicate for `String` / `str`.                                     |
| `is_bytes_type`              | Predicate for the supported byte container types.                   |
| `is_bf16_type`               | Predicate for `bf16`.                                               |
| `is_u64_type`                | Predicate used to validate the `_id: u64` requirement.              |

All fallible helpers return `syn::Result` with errors spanned at the
offending field, type or attribute, so diagnostics point at the user's code
instead of the `#[derive(...)]` line.

#### Map parsing details

`parse_field_type_str` finds the **top-level** comma inside `Map<...>` by
counting angle-bracket depth. This is what allows nested types such as
`Map<Text, Array<U64>>` or `Option<Map<Bytes, F64>>` to parse correctly.
Whitespace is trimmed on both sides of every separator, so writes like
`Map< Text , Array<U64> >` are also accepted.

### 7.2 `schema.rs`

Pipeline executed by `anda_db_schema_derive`:

1. Parse the input as `DeriveInput`.
2. Reject anything that is not a struct with named fields, and reject
   `#[serde(transparent)]`.
3. For every field:
   - if the field is `_id`, verify it is `u64` and still serializes as
     `"_id"`, then skip code generation;
   - skip fields marked `#[serde(skip)]` / `#[serde(skip_serializing)]`,
     reject `#[serde(flatten)]`;
   - resolve the serialized name (explicit rename > `rename_all` > Rust
     identifier) and validate it against AndaDB's naming rules, the reserved
     `_id` column and previously seen names;
   - resolve the field type via `#[field_type]` *or* `determine_field_type`;
   - extract `///` doc comments (joined with spaces) and `#[unique]`, then
     compose `FieldEntry::new(...)?[.with_description(...)][.with_unique()]`.
4. Emit `impl <Struct> { pub fn schema() -> Result<Schema, SchemaError> { … } }`.
   Per-field errors are emitted in place (as spanned `compile_error!`s) so
   every offending field is reported in a single compilation pass.

### 7.3 `field_typed.rs`

Same parse / validation prelude as `schema.rs` (minus the AndaDB naming
restrictions — nested map keys are free-form). For each serialized field the
macro produces a `(serialized_name.into(), <field_type>)` tuple and collects
them into a single `FieldType::Map`.

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

All messages are spanned at the offending field, type or attribute. Types in
messages are rendered as Rust source (e.g. `(u64, u64)`), not as AST dumps.

| Message                                                                            | Cause                                                                          |
| ----------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| `FieldTyped only supports structs`                                                  | Applied to an enum or union.                                                   |
| `FieldTyped only supports structs with named fields`                                | Applied to a tuple or unit struct.                                             |
| `AndaDBSchema only supports structs`                                                | Applied to an enum or union.                                                   |
| `AndaDBSchema only supports structs with named fields`                              | Applied to a tuple or unit struct.                                             |
| `The '_id' field must be of type u64`                                               | The struct declares `_id` with a type other than `u64`.                        |
| `serde renames `_id` to "...", but the primary key must serialize as "_id"; …`      | `rename_all`/`rename` changes how `_id` serializes.                            |
| `field "..." serializes as "_id", which collides with the auto-generated primary key` | Another field is renamed to `_id`.                                           |
| `schema field name "..." is not a valid AndaDB field name (...)`                    | The serialized name violates `[a-z0-9_]{1,64}` (`AndaDBSchema` only).          |
| `duplicate schema field name "..." (after serde renaming)`                          | Two fields end up with the same serialized name.                               |
| `#[serde(flatten)] is not supported: …`                                             | Flattened keys cannot be described by a per-field schema entry.                |
| `… does not support #[serde(transparent)]: …`                                       | Transparent structs do not serialize as maps.                                  |
| `unknown #[serde(rename_all = "...")] rule; …`                                      | Unrecognised case rule (would silently desync schema and data otherwise).      |
| `Unsupported field type: '...'. Supported types: …`                                 | DSL string in `#[field_type]` was not recognised.                              |
| `Unsupported Map key type: '...'. Expected 'String', 'Text' or 'Bytes'.`            | Unsupported key in `Map<K, V>` DSL.                                            |
| `Invalid Map field type: '...'. Expected 'Map<KeyType, ValueType>'.`                | DSL `Map<…>` string lacks a comma-separated key/value pair.                    |
| `Unsupported type: \`...\`. Consider: …`                                            | Inference failed (tuples, trait objects, etc.).                                |
| `Unable to determine Vec element type for: ...`                                     | Generic argument missing on a `Vec` / `HashSet` / `BTreeSet`.                  |
| `Unable to determine Option element type`                                           | Generic argument missing on an `Option`.                                       |
| `Unable to determine the inner type of: ...`                                        | Generic argument missing on a `Box` / `Arc` / `Rc` / `Cow`.                    |
| `Map key type must be String or bytes (e.g., Vec<u8>, ByteArray, ByteBuf), found: …`| `HashMap`/`BTreeMap` key inferred as something neither string- nor bytes-like. |
| `Standalone \`bf16\` is not supported as a field type. Use \`Vec<bf16>\` …`         | Bare `bf16` field without `Vec`/override.                                      |

### 8.2 Runtime errors

`schema()` ultimately calls into `Schema::builder().build()`, which can fail
with:

| Variant                   | Description                     |
| ------------------------- | ------------------------------- |
| `SchemaError::FieldName`  | Invalid field name.             |
| `SchemaError::FieldType`  | Invalid field type.             |
| `SchemaError::Validation` | Schema-level validation failed. |

---

*Document updated: 2026-06-11*
