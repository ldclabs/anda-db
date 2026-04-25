# `anda_db_schema` — Technical Reference

> Type system, schema definitions and document model used across all
> [Anda DB](https://github.com/ldclabs/anda-db) sub-crates.

|                 |                                                                                          |
| :-------------- | :--------------------------------------------------------------------------------------- |
| Crate           | [`anda_db_schema`](../rs/anda_db_schema/)                                                |
| Version         | `0.4.x`                                                                                  |
| Companion crate | [`anda_db_derive`](../rs/anda_db_derive/) (re-exported as `AndaDBSchema` / `FieldTyped`) |

---

## Contents

1. [Overview](#1-overview)
2. [Type system](#2-type-system)
3. [Field values](#3-field-values)
4. [Field entries](#4-field-entries)
5. [Schemas and migration](#5-schemas-and-migration)
6. [Documents](#6-documents)
7. [Resource type](#7-resource-type)
8. [Derive macros](#8-derive-macros)
9. [Serialization](#9-serialization)
10. [Errors](#10-errors)
11. [API reference](#11-api-reference)
12. [Cookbook](#12-cookbook)

---

## 1. Overview

### 1.1 Responsibilities

`anda_db_schema` provides the foundational vocabulary of Anda DB:

- describe what a field looks like (`FieldType`),
- carry actual runtime values (`FieldValue`),
- bundle field metadata (`FieldEntry`),
- compose them into a versioned `Schema`,
- and represent persisted records as `Document` / `DocumentOwned`.

These primitives are designed for two concurrent goals:

- **Compact, deterministic on-disk format** — values are normalized into
  CBOR (via [`ciborium`](https://docs.rs/ciborium)), and `FieldEntry`/`Schema`
  serialize their keys to single letters to keep records small.
- **Self-describing dynamic typing** — the closed `FieldType` enum lets
  the database accept arbitrary user structs while still validating every
  field at write time.

### 1.2 Conceptual hierarchy

```
Schema ─────────────────────────── document layout (versioned)
 ├── _id : FieldEntry (required, U64, idx = 0, unique)
 ├── …  : FieldEntry
 │        ├── name        — unique within a schema
 │        ├── description — human / LLM facing
 │        ├── type        — FieldType
 │        ├── unique      — collection-level uniqueness flag
 │        └── idx         — stable on-disk key
 │
 ├── FieldType  (closed enum)
 │   ├── primitives        Bool I64 U64 F64 F32 Bytes Text Json Vector
 │   └── composites        Array(Vec<Ft>)  Map(BTreeMap<FieldKey, Ft>)  Option(Box<Ft>)
 │
 └── FieldValue (closed enum)
     ├── one variant per primitive type
     ├── Vector(Vec<bf16>)
     ├── Array(Vec<FieldValue>)
     ├── Map(BTreeMap<FieldKey, FieldValue>)
     └── Null   ← absent value of an Option(_) field
```

### 1.3 Source layout

```text
rs/anda_db_schema/src/
├── lib.rs          # crate-level docs, re-exports, validate_field_name
├── error.rs        # SchemaError, BoxError
├── field.rs        # FieldType, FieldKey, FieldValue, FieldEntry
├── schema.rs       # Schema, SchemaBuilder
├── document.rs     # Document, DocumentOwned
├── resource.rs     # Resource (predefined schema)
└── value_serde.rs  # FieldKey/FieldValue Serialize / Deserialize
```

---

## 2. Type system

### 2.1 `FieldType`

```rust
pub enum FieldType {
    // primitives
    Bool, I64, U64, F64, F32, Bytes, Text, Json, Vector,
    // composites
    Array(Vec<FieldType>),
    Map(BTreeMap<FieldKey, FieldType>),
    Option(Box<FieldType>),
}
```

Aliases:

| Alias    | Concrete type |
| :------- | :------------ |
| `Ft`     | `FieldType`   |
| `Vector` | `Vec<bf16>`   |

### 2.2 Primitive types and their Rust counterparts

| `FieldType` | Rust source types accepted by `AndaDBSchema`                                           |
| :---------- | :------------------------------------------------------------------------------------- |
| `Bool`      | `bool`                                                                                 |
| `I64`       | `i8`, `i16`, `i32`, `i64`, `isize`                                                     |
| `U64`       | `u8`, `u16`, `u32`, `u64`, `usize`                                                     |
| `F32`       | `f32`                                                                                  |
| `F64`       | `f64`                                                                                  |
| `Bytes`     | `Vec<u8>`, `[u8; N]`, `serde_bytes::*`, `ic_auth_types::ByteBufB64`, `ByteArrayB64<N>` |
| `Text`      | `String`, `&str`                                                                       |
| `Json`      | `serde_json::Value`                                                                    |
| `Vector`    | `Vec<bf16>`, `[bf16; N]`                                                               |

### 2.3 Composite types

#### `Array`

`FieldType::Array` carries a `Vec<FieldType>` whose length determines the
shape:

| `types.len()` | Semantics                                                                         |
| :------------ | :-------------------------------------------------------------------------------- |
| `0`           | Heterogeneous — values are accepted as-is (mostly for back-fill/ad-hoc data).     |
| `1`           | Homogeneous array. Every element must satisfy the single inner type.              |
| `N > 1`       | Tuple-like — `values.len()` must equal `N` and elements are matched positionally. |

#### `Map`

`FieldType::Map` is keyed by `FieldKey` (text or bytes). It supports two
shapes:

- **Wildcard map** — exactly one entry whose key is the wildcard
  (`"*"` for text, `b"*"` for bytes). Any key is allowed at runtime, and
  every value must match the wildcard's value type.
- **Schema-bound map** — the keys present in the type are the only legal
  keys in the value. Required keys are those whose value type is *not*
  `Option`.

```rust
// Wildcard text map (≅ HashMap<String, U64>)
Ft::Map([(TEXT_WILDCARD_KEY.clone(), Ft::U64)].into_iter().collect());

// Schema-bound (only "title" and optional "subtitle" allowed)
Ft::Map([
    ("title".into(),    Ft::Text),
    ("subtitle".into(), Ft::Option(Box::new(Ft::Text))),
].into_iter().collect());
```

#### `Option`

`FieldType::Option(Box<Ft>)` is the only way to declare a nullable field.
A field whose type is *not* `Option` is treated as required by both
`Schema::validate` and `FieldEntry::validate`.

### 2.4 `FieldKey`

```rust
pub enum FieldKey {
    Text(String),
    Bytes(Vec<u8>),
}
```

Two pre-built constants are exposed for the wildcard convention:

```rust
pub static TEXT_WILDCARD_KEY:  LazyLock<FieldKey>; // "*"
pub static BYTES_WILDCARD_KEY: LazyLock<FieldKey>; // b"*"
```

Convertible from `String`, `&str`, `Vec<u8>`, `[u8; N]`, `&[u8]`, and
`ciborium::Value` (text or bytes).

### 2.5 Field name rules

`validate_field_name` enforces a strict ASCII vocabulary so that names
remain stable across all storage backends:

- non-empty, at most **64 bytes**,
- only `a`–`z`, `0`–`9`, and `_`.

`_id` is a valid field name; it is the **only** name reserved by the
crate (assigned `idx = 0` and `unique`).

### 2.6 Type-level methods

| Method                   | Purpose                                              |
| :----------------------- | :--------------------------------------------------- |
| `FieldType::allows_null` | Returns `true` for `Option(_)` only.                 |
| `FieldType::extract`     | CBOR → `FieldValue`, requiring CBOR to match `self`. |
| `FieldType::validate`    | Checks an existing `FieldValue` against `self`.      |

`extract` is type-driven (used when parsing structured input), while
`FieldValue::try_from` is shape-driven (used when reading untyped CBOR).

---

## 3. Field values

### 3.1 `FieldValue`

```rust
pub enum FieldValue {
    Bool(bool),  I64(i64),  U64(u64),  F64(f64),  F32(f32),
    Bytes(Vec<u8>),  Text(String),  Json(serde_json::Value),
    Vector(Vec<bf16>),
    Array(Vec<FieldValue>),
    Map(BTreeMap<FieldKey, FieldValue>),
    Null,
}
```

Alias: `Fv = FieldValue`.

`FieldValue: PartialEq` is meaningful because `FieldValue::f64_from` /
`f32_from` reject `NaN` when extracting from CBOR.

### 3.2 Building values

#### From owned Rust values

`From` is implemented for every primitive plus the obvious collection
types:

| `From<T>`                                            | Result variant    |
| :--------------------------------------------------- | :---------------- |
| `bool` / `i64` / `u64` / `f64` / `f32`               | one-to-one        |
| `Vec<u8>`                                            | `Bytes`           |
| `String`                                             | `Text`            |
| `serde_json::Value`                                  | `Json`            |
| `Vec<bf16>`                                          | `Vector`          |
| `Vec<T>` (where `T: Into<FieldValue>`)               | `Array`           |
| `BTreeSet<T>`, `HashSet<T>`                          | `Array`           |
| `BTreeMap<K, V>`, `HashMap<K, V>`, `serde_json::Map` | `Map`             |
| `FieldKey`                                           | `Text` or `Bytes` |

#### From any `Serialize` value

```rust
let fv = Fv::serialized(&my_struct, Some(&Ft::Array(vec![Ft::Vector])))?;
```

`serialized` first encodes through CBOR, then either calls
`FieldType::extract` (when a type hint is given) or falls back to
`FieldValue::try_from`. The hint is required when sub-values cannot be
inferred from CBOR alone — most notably for `Vector` (whose CBOR shape is
indistinguishable from `Array<U64>`).

### 3.3 Reading values

`TryFrom` is implemented for every primitive both by-value and by
reference, plus several collection forms:

| Target                                 | Source variant                          |
| :------------------------------------- | :-------------------------------------- |
| `bool` / `i64` / `u64` / `f64` / `f32` | matching primitive                      |
| `Vec<u8>` / `[u8; N]`                  | `Bytes`                                 |
| `String` / `&str`                      | `Text`                                  |
| `serde_json::Value`                    | `Json`                                  |
| `Vec<bf16>` / `[bf16; N]`              | `Vector`                                |
| `Vec<T>`                               | `Array` (when `T: TryFrom<FieldValue>`) |
| `BTreeMap<FieldKey, T>`                | `Map`                                   |

For arbitrary `DeserializeOwned` types, use:

```rust
let user: MyUser = fv.deserialized()?;
```

`deserialized` round-trips through CBOR and therefore handles every type
serde can deserialize.

### 3.4 Convenience accessors

`FieldValue::get_field_as<'a, T>(&'a self, key: &FieldKey) -> Option<&'a T>`
shortcuts the `Fv::Map(_) → BTreeMap::get → TryFrom` chain when reading a
nested map.

### 3.5 Vector helpers

```rust
pub fn vector_from_f32(v: Vec<f32>) -> Vector;
pub fn vector_from_f64(v: Vec<f64>) -> Vector;
```

Both perform lossy `bf16::from_f32` / `bf16::from_f64` element-wise.

---

## 4. Field entries

### 4.1 Definition

```rust
pub struct FieldEntry {
    name: String,        // serialized as "n"
    description: String, // serialized as "d"
    r#type: FieldType,   // serialized as "t"
    unique: bool,        // serialized as "u"
    idx: usize,          // serialized as "i"
}
```

Long-form keys (`name`, `description`, `type`, `unique`, `index`) are
accepted as `serde(alias = …)` for compatibility.

### 4.2 Builder

```rust
let entry = FieldEntry::new("title".into(), Ft::Text)?
    .with_description("Article title".into())
    .with_unique();          // optional
// .with_idx(N)              ← rarely set by hand; the SchemaBuilder
//                              assigns indexes automatically.
```

`new` runs `validate_field_name` immediately.

### 4.3 Accessors

| Method       | Returns                                |
| :----------- | :------------------------------------- |
| `name()`     | `&str`                                 |
| `r#type()`   | `&FieldType`                           |
| `required()` | `true` iff the type is not `Option(_)` |
| `unique()`   | `bool`                                 |
| `idx()`      | `usize`                                |

### 4.4 Mutators

| Method          | Purpose                                                            |
| :-------------- | :----------------------------------------------------------------- |
| `with_idx(idx)` | Builder-style; consumes self.                                      |
| `set_idx(idx)`  | Mutates in place; used by `Schema::upgrade_with` to avoid cloning. |

### 4.5 Validation

`FieldEntry::extract(cbor, validate)` chains `FieldType::extract` with an
optional `validate` step, and `FieldEntry::validate` enforces:

1. `Null` is only legal for `Option(_)` types.
2. The value must satisfy `FieldType::validate`.

---

## 5. Schemas and migration

### 5.1 Definition

```rust
pub struct Schema {
    idx:     BTreeSet<usize>,
    fields:  BTreeMap<String, FieldEntry>,
    version: u64,
}
```

Invariants enforced both by `SchemaBuilder` and by deserialization:

- `_id` is present, `U64`, `unique`, with `idx == 0`.
- All field names pass `validate_field_name`.
- All `idx` values are unique and `≤ u16::MAX` (so a schema can host
  `u16::MAX + 1 = 65 536` fields including `_id`).

### 5.2 `SchemaBuilder`

```rust
let schema = Schema::builder()
    .with_version(1)                                     // optional
    .add_field(FieldEntry::new("title".into(), Ft::Text)?)?
    .add_field(FieldEntry::new("views".into(), Ft::U64)?)?
    .with_resource("thumbnail", /* required = */ false)? // optional helper
    .build()?;
```

`add_field` assigns an `idx` automatically (`1`, `2`, … in insertion
order). `_id` is added by `SchemaBuilder::new` with `idx = 0`.

### 5.3 Inspection API

```rust
schema.version()                  // u64
schema.len() / is_empty()
schema.get_field(name)            // Option<&FieldEntry>
schema.get_field_or_err(name)?    // Result<&FieldEntry, SchemaError>
schema.iter()                     // impl Iterator<Item = &FieldEntry>
schema.validate(&values)?
```

`validate` checks both that every key in `values` has a matching field
*and* that every required field appears.

### 5.4 Versioning and migration

Schemas are versioned to support **gradual** migration. The new schema is
typically built from code (`#[derive(AndaDBSchema)]`) with sequential
indexes; the old schema is loaded from storage with whatever indexes were
assigned before.

```rust
new_schema.upgrade_with(&old_schema)?;
```

`upgrade_with` rules:

1. `new.version > old.version` is required.
2. **Existing fields** keep their old `idx`; their `FieldType` must be
   unchanged (type changes are explicitly rejected).
3. **New fields** get fresh indexes starting at `max(old.idx) + 1`, so
   the indexes of removed fields are *never* reused.

This guarantees that any record persisted under the old schema can still
be read after the upgrade.

### 5.5 `IndexedFieldValues`

```rust
pub type IndexedFieldValues = BTreeMap<usize, FieldValue>;
```

The canonical container of a document's payload — keyed by `idx`, not
by name.

---

## 6. Documents

### 6.1 Two flavours

```rust
pub struct Document      { fields: IndexedFieldValues, schema: Arc<Schema> }
pub struct DocumentOwned { pub fields: IndexedFieldValues } // serializable
pub type   DocumentId = u64;
```

`Document` is the runtime API (it can validate field-by-field against
its schema). `DocumentOwned` is the on-disk and over-the-wire shape; its
serialized form is `{ "f": IndexedFieldValues }` — a single short key
to keep records compact.

### 6.2 Construction

```rust
// Empty:
let mut doc = Document::new(schema.clone());

// From an existing payload (validated against the schema):
let doc = Document::try_from_doc(schema.clone(), owned_doc)?;

// From any Serialize value (validated):
let doc = Document::try_from(schema.clone(), &my_struct)?;
```

### 6.3 Reading

```rust
doc.id();                                     // DocumentId
doc.get_field("title");                       // Option<&Fv>
doc.get_field_or_err("title")?;               // Result<&Fv, SchemaError>
let title: String = doc.get_field_as("title")?;
let user:  TestUser = doc.try_into()?;        // consumes the Document
```

`try_into` rebuilds a CBOR map from the document — substituting CBOR
`Null` for absent optional fields — and lets serde do the rest.

### 6.4 Mutating

```rust
doc.set_id(42);
doc.set_field("title", Fv::Text("Hi".into()))?;       // checks the type
doc.set_field_as("views", &123u64)?;                  // serialize-then-store
doc.remove_field("title");                            // Option<Fv>
doc.set_doc(owned_doc)?;                              // bulk replace
```

### 6.5 Conversion

```rust
let owned: DocumentOwned = doc.into(); // drops the Schema reference
```

### 6.6 Serialization shape

```json
{ "f": { "0": 42, "1": "Hi", "2": 123 } }
```

Top-level keys are field `idx` values rendered as decimal strings (this
is JSON's only key form; CBOR uses native integer keys).

---

## 7. Resource type

`Resource` is a predefined struct describing an external asset — useful
both as a stand-alone collection and as an embedded sub-document.

```rust
#[derive(AndaDBSchema, FieldTyped, Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct Resource {
    pub _id:         u64,                          // primary key
    pub tags:        Vec<String>,                  // type tags, e.g. ["text", "md"]
    pub name:        String,                       // human-readable name
    pub description: Option<String>,
    pub uri:         Option<String>,
    pub mime_type:   Option<String>,
    pub blob:        Option<ByteBufB64>,           // inline payload
    pub size:        Option<u64>,
    #[unique] pub hash: Option<ByteArrayB64<32>>,  // SHA3-256
    pub metadata:    Option<Map<String, Json>>,
}
```

Embed it in any other schema:

```rust
#[derive(AndaDBSchema)]
struct Article {
    _id: u64,
    title: String,
    thumbnail: Option<Resource>, // expands to FieldType::Option(Resource::field_type())
}
```

The `Schema::with_resource(name, required)` builder helper does the same
thing without needing a derive.

---

## 8. Derive macros

Both macros are re-exported from `anda_db_schema`:

```rust
use anda_db_schema::{AndaDBSchema, FieldTyped};
```

### 8.1 `AndaDBSchema`

Generates `MyStruct::schema() -> Result<Schema, SchemaError>`. The
struct **must** declare `_id: u64` so the generated schema is valid.

### 8.2 `FieldTyped`

Generates `MyStruct::field_type() -> FieldType`. The result is a
`FieldType::Map` whose entries map `field_name` → `FieldType`. This is
how nested user structs participate in schemas: `AndaDBSchema` calls the
derived `field_type()` of any sub-struct it encounters.

### 8.3 Attributes

| Attribute                         | Effect                                            |
| :-------------------------------- | :------------------------------------------------ |
| `#[field_type = "FieldTypeExpr"]` | Override the inferred type (see below).           |
| `#[unique]`                       | Mark a field as unique (requires `AndaDBSchema`). |
| `#[serde(rename = "newName")]`    | Use `newName` as the schema field name.           |
| `///` doc comment                 | Captured as the field's `description`.            |

`#[field_type = "..."]` accepts a Rust expression that resolves to a
`FieldType`, e.g.:

```rust
#[field_type = "Bytes"]
some_id: [u8; 16],

#[field_type = "Array(vec![FieldType::F32])"]
embedding_f32: Vec<f32>,
```

### 8.4 Type inference table

| Rust source                                                   | Inferred `FieldType`                  |
| :------------------------------------------------------------ | :------------------------------------ |
| `bool`                                                        | `Bool`                                |
| `i8` … `i64`, `isize`                                         | `I64`                                 |
| `u8` … `u64`, `usize`                                         | `U64`                                 |
| `f32` / `f64`                                                 | `F32` / `F64`                         |
| `String`, `&str`                                              | `Text`                                |
| `Vec<u8>`, `[u8; N]`, `Bytes`, `ByteArrayB64`, `ByteBufB64`   | `Bytes`                               |
| `Vec<bf16>`, `[bf16; N]`                                      | `Vector`                              |
| `serde_json::Value`                                           | `Json`                                |
| `Vec<T>`, `HashSet<T>`, `BTreeSet<T>`                         | `Array(vec![T])`                      |
| `HashMap<K, V>`, `BTreeMap<K, V>`, `Map<K, V>` (`K = String`) | `Map({"*": V})`                       |
| `HashMap<K, V>` etc. with byte-string keys                    | `Map({b"*": V})`                      |
| `Option<T>`                                                   | `Option(T)`                           |
| any other path `Foo`                                          | `Foo::field_type()` (must be derived) |

---

## 9. Serialization

### 9.1 Two formats, one model

`FieldValue` and `FieldKey` have hand-written `Serialize` /
`Deserialize` impls that branch on `is_human_readable()`:

|                             | Human-readable (JSON, …)              | Binary (CBOR, MessagePack, …) |
| :-------------------------- | :------------------------------------ | :---------------------------- |
| `Bytes` / `FieldKey::Bytes` | URL-safe Base64 string                | native byte string            |
| `Vector`                    | array of `u16` (bf16 bits)            | same                          |
| `Json`                      | JSON delegated to `serde_json::Value` | same                          |
| `Null`                      | `null` / unit                         | `null`                        |

When deserializing in human-readable mode, a textual value that
successfully decodes as URL-safe Base64 is *promoted* to `Bytes`. This
matches the convention used by `ic_auth_types::ByteBufB64`.

### 9.2 CBOR examples

```
Fv::Null                  → f6
Fv::Bool(true)            → f5
Fv::U64(42)               → 18 2a
Fv::I64(-42)              → 38 29
Fv::Text("hello")         → 65 68 65 6c 6c 6f
Fv::Bytes([1,2,3,4])      → 44 01 02 03 04
Fv::Array([U64(1), Text("hello")])
                          → 82 01 65 68 65 6c 6c 6f
```

### 9.3 Full round-trip with type hints

CBOR alone cannot distinguish a `Vector` from an `Array<U64>` (both are
sequences of small integers), so when serializing arbitrary user data
into a `FieldValue` you can supply a `FieldType` hint:

```rust
let vv = vec![[bf16::from_f32(1.0), bf16::from_f32(1.1)]];

let fv = Fv::serialized(&vv, None)?;
// → Array([Array([U64(16256), U64(16269)])])

let fv = Fv::serialized(&vv, Some(&Ft::Array(vec![Ft::Vector])))?;
// → Array([Vector([1.0, 1.1])])
```

Both representations deserialize back into `Vec<[bf16; 2]>` thanks to
`half`'s serde impl, but only the typed form preserves the original
shape on disk.

---

## 10. Errors

```rust
pub enum SchemaError {
    Schema(String),       // schema-level invariant violated
    FieldType(String),    // malformed FieldType
    FieldValue(String),   // value does not satisfy its FieldType
    FieldName(String),    // illegal field name
    Validation(String),   // document fails Schema::validate
    Serialization(String) // CBOR / serde error
}

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
```

`BoxError` is the error type returned by all `TryFrom<FieldValue>` impls.

---

## 11. API reference

### 11.1 Type aliases (re-exported from the crate root)

| Alias                | Concrete type                              |
| :------------------- | :----------------------------------------- |
| `Ft`                 | `FieldType`                                |
| `Fv`                 | `FieldValue`                               |
| `Fe`                 | `FieldEntry`                               |
| `Cbor`               | `ciborium::Value`                          |
| `Json`               | `serde_json::Value`                        |
| `Map<K, V>`          | `serde_json::Map<K, V>`                    |
| `Vector`             | `Vec<bf16>`                                |
| `DocumentId`         | `u64`                                      |
| `IndexedFieldValues` | `BTreeMap<usize, FieldValue>`              |
| `BoxError`           | `Box<dyn std::error::Error + Send + Sync>` |

### 11.2 Public types

| Type            | Notes                                      |
| :-------------- | :----------------------------------------- |
| `FieldType`     | Closed type enum.                          |
| `FieldKey`      | Map key (`Text` / `Bytes`).                |
| `FieldValue`    | Runtime value.                             |
| `FieldEntry`    | Field metadata, persists with each schema. |
| `Schema`        | Versioned set of `FieldEntry`.             |
| `SchemaBuilder` | Construction helper for `Schema`.          |
| `Document`      | Schema-bound document.                     |
| `DocumentOwned` | Standalone serializable document.          |
| `Resource`      | Predefined schema for external assets.     |
| `SchemaError`   | Crate's error enum.                        |

### 11.3 Free functions

```rust
pub fn validate_field_name(s: &str) -> Result<(), SchemaError>;
pub fn vector_from_f32(v: Vec<f32>) -> Vector;
pub fn vector_from_f64(v: Vec<f64>) -> Vector;
```

### 11.4 Constants and statics

| Item                 | Value                   |
| :------------------- | :---------------------- |
| `Schema::ID_KEY`     | `"_id"`                 |
| `TEXT_WILDCARD_KEY`  | `FieldKey::Text("*")`   |
| `BYTES_WILDCARD_KEY` | `FieldKey::Bytes(b"*")` |

---

## 12. Cookbook

### 12.1 Minimal schema, hand-built

```rust
use anda_db_schema::{Fe, Ft, Schema};
use std::sync::Arc;

let schema = Schema::builder()
    .add_field(Fe::new("title".into(),   Ft::Text)?
        .with_description("Document title".into()))?
    .add_field(Fe::new("content".into(), Ft::Text)?)?
    .add_field(Fe::new("views".into(),   Ft::U64)?)?
    .build()?;
let schema = Arc::new(schema);
```

### 12.2 Same schema via the derive macro

```rust
use anda_db_schema::{AndaDBSchema, Schema};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize, AndaDBSchema)]
struct Article {
    /// Document primary key
    _id: u64,
    /// Document title
    title: String,
    /// Document body
    content: String,
    /// View count
    views: u64,
}

let schema = Arc::new(Article::schema()?);
```

### 12.3 Building and reading a document

```rust
use anda_db_schema::{Document, Fv};

let mut doc = Document::new(schema.clone());
doc.set_id(1);
doc.set_field("title",   Fv::Text("Hello".into()))?;
doc.set_field("content", Fv::Text("World".into()))?;
doc.set_field("views",   Fv::U64(42))?;

let title = doc.get_field_as::<String>("title")?;
let owned: DocumentOwned = doc.into();
```

### 12.4 From a struct, with full validation

```rust
let article = Article {
    _id: 1,
    title: "Hello".into(),
    content: "World".into(),
    views: 42,
};
let doc = Document::try_from(schema.clone(), &article)?;
let back: Article = doc.try_into()?;
```

### 12.5 Schema migration

```rust
// Persisted v1 schema:
let old = Schema::builder()
    .with_version(1)
    .add_field(Fe::new("name".into(), Ft::Text)?)?
    .add_field(Fe::new("age".into(),  Ft::Option(Box::new(Ft::U64)))?)?
    .build()?;

// New code defines v2 with an additional `email` field:
let mut new = Schema::builder()
    .with_version(2)
    .add_field(Fe::new("name".into(),  Ft::Text)?)?
    .add_field(Fe::new("age".into(),   Ft::Option(Box::new(Ft::U64)))?)?
    .add_field(Fe::new("email".into(), Ft::Option(Box::new(Ft::Text)))?)?
    .build()?;

new.upgrade_with(&old)?;

// `name` keeps idx=1, `age` keeps idx=2, `email` gets idx=3.
```

### 12.6 Embedding a `Resource`

```rust
use anda_db_schema::{AndaDBSchema, Resource};

#[derive(AndaDBSchema)]
struct Article {
    _id: u64,
    title: String,
    thumbnail: Option<Resource>, // recursive use of Resource::field_type()
}
```

---

*Document maintained alongside `rs/anda_db_schema/`. Update both when the
public API changes.*
