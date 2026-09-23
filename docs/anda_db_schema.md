# `anda_db_schema` — Technical Reference

[中文版](anda_db_schema.zh.md)

> Type system, schema definitions and document model used across all
> [Anda DB](https://github.com/ldclabs/anda-db) sub-crates.

|                 |                                                                                          |
| :-------------- | :--------------------------------------------------------------------------------------- |
| Crate           | [`anda_db_schema`](../rs/anda_db_schema/)                                                |
| Version         | `0.13.x`                                                                                  |
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
  CBOR (via [`cbor2`](https://docs.rs/cbor2)), and `FieldEntry`/`Schema`
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
├── field.rs        # stable public facade and aliases
├── field/
│   ├── field_type.rs # declarations, typed preparation and compatibility
│   ├── key.rs        # FieldKey and wildcard maps
│   ├── value.rs      # FieldValue and CBOR/JSON conversions
│   ├── entry.rs      # FieldEntry
│   ├── budget.rs     # structural complexity checks
│   └── tests.rs      # field-level regressions
├── schema/history.rs # nested-key tombstones and legacy-history recovery
├── type_construction.rs # fallible derive construction guard
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

`FieldType::Map` is keyed by `FieldKey` (text, signed `i64`, or bytes). It
supports three shapes:

- **Open map** — an empty declaration accepts arbitrary map entries. It
  cannot change to or from a fixed-key declaration by a metadata-only upgrade.
  An empty `FieldTyped` struct uses this open representation as well.

- **Wildcard map** — exactly one entry whose key is the wildcard
  (`"*"` for text, `i64::MIN` for integer keys, `b"*"` for bytes; see
  `FieldKey::is_wildcard`). Any key of that variant is allowed at runtime,
  and every value must match the wildcard's value type. A wildcard key mixed
  with other keys is a malformed declaration
  (`FieldType::validate_declaration`).
- **Schema-bound map** — the keys present in the type are the only legal
  keys in the value. Required keys are those whose value type is *not*
  `Option`.

```rust
// Wildcard text map (≅ HashMap<String, U64>)
Ft::Map([(TEXT_WILDCARD_KEY.clone(), Ft::U64)].into_iter().collect());

// Wildcard integer-keyed map (≅ BTreeMap<i64, Text>)
Ft::Map([(I64_WILDCARD_KEY.clone(), Ft::Text)].into_iter().collect());

// Schema-bound (only "title" and optional "subtitle" allowed)
Ft::Map([
    ("title".into(),    Ft::Text),
    ("subtitle".into(), Ft::Option(Box::new(Ft::Text))),
].into_iter().collect());
```

#### `Option`

`FieldType::Option(Box<Ft>)` is the only way to declare a nullable field.
A field whose type is *not* `Option` is treated as required by both
`Schema::validate` and `FieldEntry::validate`. A required `Json` field may
contain a JSON null payload, but its key must still be present.
For such a field, `set_field(name, Fv::Null)` normalizes to
`Fv::Json(Json::Null)`, matching `try_from`, `set_field_as`, and storage reads.
`Option<Json>` keeps the optional `Fv::Null` representation.

### 2.4 `FieldKey`

```rust
pub enum FieldKey {
    Text(String),
    I64(i64),
    Bytes(Vec<u8>),
}
```

Three pre-built constants are exposed for the wildcard convention:

```rust
pub static TEXT_WILDCARD_KEY:  LazyLock<FieldKey>; // "*"
pub static I64_WILDCARD_KEY:   LazyLock<FieldKey>; // i64::MIN
pub static BYTES_WILDCARD_KEY: LazyLock<FieldKey>; // b"*"
```

`FieldKey::is_wildcard()` recognises these sentinels, and
`as_wildcard_map(&BTreeMap<FieldKey, FieldType>)` returns the single
wildcard entry of a homogeneous map type, if it is one.

Convertible from `String`, `&str`, signed integer types up to `i64`,
`Vec<u8>`, `[u8; N]`, `&[u8]`, and `cbor2::Value` (text, integer, bytes, or
an array of integers in `0..=255` — the shape serde gives `Vec<u8>` /
`[u8; N]` map keys, coerced into a `Bytes` key).

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
| `FieldType::validate_declaration` | Checks that `self` is a well-formed declaration: no `Option<Option<T>>`, no wildcard key mixed with other keys, bounded nesting. Run by `FieldEntry::new`, `SchemaBuilder::add_field` and `Schema` deserialization. |
| `FieldType::extract`     | CBOR → `FieldValue`, requiring CBOR to match `self`. |
| `FieldType::validate`    | Checks an existing `FieldValue` against `self`, accepting the read-back shapes listed in §3.3. |
| `FieldType::is_compatible_upgrade_of` | Whether a stored field may be re-declared as `self` (§5.4). |

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
| `FieldKey`                                           | `Text`, `I64` or `Bytes` |

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

Read-back shapes are accepted where generic deserialization cannot restore
the declared variant: `i64` also takes a non-negative `U64`, `f32` takes an
`F64` a stored `f32` can read back as, `Vec<bf16>` takes an array of bf16 bit
patterns, and `f64` / `f32` take an `I64` / `U64` — JSON has a single number
type, so `1.0` arrives as `1`. `f64` takes any integer (`as f64`: exact to
2^53, rounded beyond); `f32` takes only the integers an `f32` holds exactly,
so that a value like `16777217` is rejected in both its integer and its float
spelling rather than being rounded in one of them. `FieldType::validate`
applies the same rules, and reading a document (`Document::try_from_doc`)
folds these shapes into the canonical variant.

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

`FieldEntry::coerce(value)` is the entry point for a `FieldValue` that did
not arrive as CBOR (a JSON API payload, say): it runs the value through the
same coercion rules `Document::try_from` applies — a `Bytes` field accepts an
array of `0..=255`, a float field an integer, an `I64` field a non-negative
`U64`, a `Vector` field an array of bf16 bit patterns — and then enforces the
complexity budget. `Document::set_field` goes through it, so creating and
updating a document accept the same shapes. Canonical values retain their
buffers; typed containers are traversed directly, and read materialization
combines pruning, normalization and type checking before a single nesting-depth
check per field. Already-stored values may predate node and container-size
admission limits, so reads, recovery and index rebuilding preserve those wide
values. New inserts and replacement fields still enforce the full default
budget; a partial update leaves unchanged legacy fields intact.

---

## 5. Schemas and migration

### 5.1 Definition

```rust
pub struct Schema {
    idx:     BTreeSet<usize>,
    fields:  BTreeMap<String, FieldEntry>,
    version: u64,
    // Private allocation watermark, legacy flag and nested-key history.
}
```

Invariants enforced both by `SchemaBuilder` and by deserialization:

- `_id` is present, `U64`, `unique`, with `idx == 0`.
- All field names pass `validate_field_name`.
- All `idx` values are unique and `≤ u16::MAX` (so a schema can host
  `u16::MAX + 1 = 65 536` fields including `_id`).

### 5.2 `SchemaBuilder`

```rust
let mut builder = Schema::builder();
builder.with_version(1);
builder.add_field(FieldEntry::new("title".into(), Ft::Text)?)?;
builder.add_field(FieldEntry::new("views".into(), Ft::U64)?)?;
builder.with_resource("thumbnail", false)?;
let schema = builder.build()?;
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
2. **Existing fields** keep their old `idx` and `unique` flag. Their
   `FieldType` may only change in ways that keep every stored value
   readable (`FieldType::is_compatible_upgrade_of`): a type may become
   optional (`T` → `Option<T>`, at the top level or inside a composite),
   and a nested struct (`Map` with explicit keys) may gain an optional key
   or lose a key. Everything else is rejected.
3. **New fields** must be optional and get fresh indexes from the old
   schema's *allocation watermark* (`Schema::allocated_idx_end`), so the
   indexes of removed fields are *never* reused.

This guarantees that any record persisted under the old schema can still
be read after the upgrade. On read, values stored under a removed field's
index are dropped; an index at or above the watermark marks foreign or
corrupt data and is rejected.

Nested field deletions are recorded as tombstones in schema metadata, including
inside arrays, optional values and wildcard map values. A later version cannot
reuse a deleted path without a data migration. `is_compatible_upgrade_of` is a
structural check; `Schema::upgrade_with` additionally checks this history.

Schemas persisted without `next_idx` or nested history remain readable.
However, an unknown allocation watermark cannot authorize new top-level
indexes, and incomplete nested history cannot authorize new nested keys.
The core collection scans **all raw stored documents**, including unregistered
objects and both images in valid, replayable mutation intents, before upgrading
such a schema. It applies the same decoding, reserved-ID and path/sequence checks
as mutation replay, so unusable recovery records cannot block or distort schema
allocation. A missing allocation watermark is inferred from that scan; an
existing watermark remains authoritative while independently missing nested-key
history is recovered. Scan failures leave metadata unchanged.

The upgraded schema is persisted before the open callback can write documents.
This keeps newly assigned field indexes recoverable if the callback fails or is
cancelled. The recovered watermark and history are therefore durable before
such writes, and subsequent upgrades do not need to repeat the scan.

Custom storage integrations use a recovery accumulator while excluding writers:

```rust
let mut recovery = old_schema.history_recovery();
for raw_document in all_raw_documents_and_recovery_images {
    recovery.observe(&raw_document)?;
}
let recovered = recovery.finish(); // certifies that the scan is complete
new_schema.upgrade_with(&recovered)?;
```

Never feed already-pruned values or only an index-selected subset into recovery.
`has_upgrade_history()` reports whether both allocation and nested-key history
are complete. `has_allocation_watermark()` separately describes index history.

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

`try_into` rebuilds a name-keyed CBOR map from the document — omitting
absent fields so `#[serde(default)]` applies — and lets serde do the rest.

### 6.4 Mutating

```rust
doc.set_id(42);
doc.set_field("title", Fv::Text("Hi".into()))?;       // coerces like try_from, then stores
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

Generates `MyStruct::schema() -> Result<Schema, SchemaError>`. Declaring
`_id: u64` on the struct is required. The builder injects its metadata,
while the struct must actually serialize the `"_id"` field. It cannot be
skipped, assigned an integer CBOR key, or given a `field_type` override.

### 8.2 `FieldTyped`

Generates `MyStruct::try_field_type() -> Result<FieldType, SchemaError>`
and the existing `field_type() -> FieldType` convenience wrapper. The result is a
`FieldType::Map` whose entries map `field_name` → `FieldType`. This is
how nested user structs participate in schemas: `AndaDBSchema` calls the
fallible constructor of derived nested types, propagating recursive-type
errors. The infallible wrapper panics on invalid declarations; custom legacy
`field_type()` methods continue to work.

### 8.3 Attributes

| Attribute                              | Effect                                                            |
| :------------------------------------- | :---------------------------------------------------------------- |
| `#[field_type = "TypeDSL"]`            | Override the inferred type (see below).                           |
| `#[unique]`                            | Mark a field as unique (requires `AndaDBSchema`).                 |
| `#[serde(rename = "new_name")]`        | Use the serialized name as the schema field name.                 |
| `#[serde(rename_all = "...")]`         | Container-level case rule, honoured like serde does.              |
| `#[serde(skip)]` / `skip_serializing`  | Exclude the never-serialized field from the schema.               |
| `///` doc comment                      | Captured as the field's `description`.                            |

`#[field_type = "..."]` accepts a small type DSL (whitespace-insensitive):
primitives (`Bytes`, `Text`, `U64`, `I64`, `F64`, `F32`, `Bool`, `Json`,
`Vector`), plus `Array<T>`, `Option<T>` and `Map<String|Text|Bytes, T>`:

```rust
#[field_type = "Bytes"]
some_id: [u8; 16],

#[field_type = "Array<F32>"]
samples: Vec<f32>,

#[field_type = "Option<Map<Text, Json>>"]
extra: Option<HashMap<String, Value>>,
```

See [anda_db_derive.md](./anda_db_derive.md) for the full grammar and
diagnostics.

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
| `Box<T>`, `Arc<T>`, `Rc<T>`, `Cow<'_, T>`                     | inferred from `T` (serde-transparent) |
| any other path `Foo`                                          | `<Foo>::field_type()` (must be derived) |

---

## 9. Serialization

### 9.1 Two formats, one model

`FieldValue` and `FieldKey` have hand-written `Serialize` /
`Deserialize` impls that branch on `is_human_readable()`:

|                             | Human-readable (JSON, …)              | Binary (CBOR, MessagePack, …) |
| :-------------------------- | :------------------------------------ | :---------------------------- |
| `FieldKey::I64`             | `i64:<decimal>` string                | native integer                |
| `Bytes` / `FieldKey::Bytes` | `b64:<url-safe Base64>` string                | native byte string            |
| `Vector`                    | array of `u16` (bf16 bits)            | same                          |
| `Json`                      | JSON with reserved text/key prefixes escaped | plain JSON data shape                          |
| `Null`                      | `null` / unit                         | `null`                        |

Only explicit prefixes are decoded: `b64:` for bytes, `i64:` for integer
map keys, and `txt:` to escape reserved prefixes in text. Ordinary strings
such as `"test"` remain text. Embedded JSON strings and object keys follow
the same escaping rule. Malformed prefixed values are errors.

Duplicate document indexes, field-value map keys and type-declaration map keys
are rejected before they can overwrite earlier entries. JSON serialization of
non-finite scalar floats returns an error; CBOR preserves infinities. F32
read-back checks use the actual JSON formatter/parser rather than Rust Display.

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
`half`'s serde impl, but the declared schema is needed to recover the canonical `Vector` variant
after an untyped storage round trip.

---

## 10. Errors

```rust
pub enum SchemaError {
    Schema(String),       // schema-level invariant violated
    FieldType(String),    // malformed FieldType declaration (validate_declaration)
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
| `Cbor`               | `cbor2::Value`                            |
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
| `FieldKey`      | Map key (`Text` / `I64` / `Bytes`).        |
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
pub fn as_wildcard_map(m: &BTreeMap<FieldKey, FieldType>) -> Option<(&FieldKey, &FieldType)>;
pub fn vector_from_f32(v: Vec<f32>) -> Vector;
pub fn vector_from_f64(v: Vec<f64>) -> Vector;
```

### 11.4 Constants and statics

| Item                   | Value                                                        |
| :--------------------- | :----------------------------------------------------------- |
| `Schema::ID_KEY`       | `"_id"`                                                      |
| `MAX_CONVERSION_DEPTH` | `128` — nesting bound of the CBOR ⇄ `FieldValue` conversions |
| `TEXT_WILDCARD_KEY`    | `FieldKey::Text("*")`                                        |
| `I64_WILDCARD_KEY`     | `FieldKey::I64(i64::MIN)`                                    |
| `BYTES_WILDCARD_KEY`   | `FieldKey::Bytes(b"*")`                                      |

---

## 12. Cookbook

### 12.1 Minimal schema, hand-built

```rust
use anda_db_schema::{Fe, Ft, Schema};
use std::sync::Arc;

let mut builder = Schema::builder();
builder.add_field(Fe::new("title".into(), Ft::Text)?
    .with_description("Document title".into()))?;
builder.add_field(Fe::new("content".into(), Ft::Text)?)?;
builder.add_field(Fe::new("views".into(), Ft::U64)?)?;
let schema = builder.build()?;
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
let mut builder = Schema::builder();
builder.with_version(1);
builder.add_field(Fe::new("name".into(), Ft::Text)?)?;
let old = builder.build()?;

let mut builder = Schema::builder();
builder.with_version(2);
builder.add_field(Fe::new("name".into(), Ft::Text)?)?;
builder.add_field(Fe::new("email".into(), Ft::Option(Box::new(Ft::Text)))?)?;
let mut new = builder.build()?;
new.upgrade_with(&old)?;
// name keeps idx=1; email gets idx=2.
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
