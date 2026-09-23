# Schemas and CBOR

Source: [schema types](../../../rs/anda_db_schema/src/),
[derive implementation](../../../rs/anda_db_derive/src/),
[derive guide](../../../docs/anda_db_derive.md), and
[schema guide](../../../docs/anda_db_schema.md).

## Typed documents and nested structs

```rust
use anda_db::schema::{AndaDBSchema, FieldTyped, Vector};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, FieldTyped)]
struct Source {
    url: String,
    label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, AndaDBSchema)]
struct Article {
    _id: u64,
    /// Title used for retrieval.
    title: String,
    body: String,
    embedding: Vector,
    status: Option<String>,
    #[unique]
    slug: String,
    hash: [u8; 32],
    source: Option<Source>,
}
```

`AndaDBSchema` generates `schema() -> Result<Schema, SchemaError>`.
`FieldTyped` describes nested named structs and generates
`try_field_type() -> Result<FieldType, SchemaError>` plus a compatibility
`field_type()` method that can panic on invalid/recursive types. Prefer the
fallible method when constructing types manually. Generated code resolves
through a direct `anda_db_schema` dependency or `anda_db::schema`; extra
imports of builder internals are not required.

Both derives require named-field structs. Top-level documents must declare
`_id: u64` and serialize it as `"_id"`; the schema builder reserves numeric
index 0 for it. Top-level field names match `[a-z0-9_]{1,64}`.

## Type mapping

The notation below describes schema shapes; e.g. the Rust enum constructor
for a homogeneous array is `FieldType::Array(vec![element_type])`.

| Rust type | Schema shape |
| --- | --- |
| `bool` | `Bool` |
| Signed integers through `i64`, `isize` | `I64` |
| Unsigned integers through `u64`, `usize` | `U64` |
| `f32` / `f64` | `F32` / `F64` |
| `String`, `&str` | `Text` |
| `Vec<u8>`, `[u8; N]`, supported byte wrappers | `Bytes` |
| `Vector`, `Vec<bf16>`, `[bf16; N]` | `Vector` |
| Other `Vec<T>`, sets, sequences, `[T; N]` | Homogeneous `Array(T)` |
| Tuple `(A, B, ...)` with at least two elements | Fixed tuple-like array |
| `BTreeMap<K, V>`, `HashMap<K, V>` | Wildcard map with value type `V` |
| `Option<T>` | `Option(T)` |
| `Json`, `serde_json::Value` | `Json` |
| `Box<T>`, `Arc<T>`, `Rc<T>`, `Cow<'_, T>` | Inner `T` |
| User-defined named struct | Nested `FieldTyped` map or manual type constructor |

Map keys must be text, signed integers, or supported bytes types. Wildcard
schema keys are respectively `"*"`, `i64::MIN`, or `b"*"`; they describe
the map's key type, not a fixed struct field. Unsigned map keys are unsupported.

`u128`/`i128`, standalone `bf16`, nested `Option<Option<T>>`, and one-element
tuples are not inferred. A `Vec<f32>` is an array of floats, **not** a stored
embedding: use `Vector` with `vector_from_f32`. Smart-pointer serialization
also needs the corresponding Serde features (e.g. `serde/rc` for `Arc`).

## Attributes and serialized shape

- `#[serde(rename = "...")]` and container `rename_all` determine serialized
  names; directional renames use the serialization name. Keep serialization
  and deserialization compatible for typed reads. Nested struct keys may be
  free-form; top-level names must still satisfy AndaDB's naming rule.
- `#[serde(skip)]` / `skip_serializing` excludes a field from the schema.
  `#[serde(default)]` does not make a required schema field optional.
- `#[serde(flatten)]` and container `transparent` are rejected.
- Container `#[cbor(array)]` and `#[cbor(tag = ...)]` are rejected by both
  derives: they require an untagged map. Integer keys on nested fields remain
  supported.
- `#[unique]` is a top-level field constraint, enforced by a B-Tree index.
  It does not add uniqueness to nested `FieldTyped` maps.
- `#[field_type = "Bytes"]` overrides inference. Other examples include
  `"Option<Text>"`, `"Array<U64>"`, and `"Map<String, Json>"`.
  An override declares the serialized shape; it does not change Serde output.
  Use it with custom serializers and aliases only when that shape matches.
- `#[cbor(key = N)]` applies only to nested `FieldTyped` structs with a
  matching integer-key CBOR serializer (e.g. `cbor2::Cbor` with its `derive`
  feature). It is rejected on top-level document fields. Fixed nested keys
  cannot use the reserved wildcard names `"*"` or `i64::MIN`.

## Schema evolution

Derived schemas start at version 0. There is no schema-version derive
attribute; set the version explicitly before opening:

```rust
let mut schema = Article::schema()?;
schema.with_version(1);
let collection = db.open_or_create_collection(
    schema,
    CollectionConfig {
        name: "articles".into(),
        description: "Searchable articles".into(),
    },
    async |_c| Ok(()),
).await?;
```

Use a monotonically higher version for changes. If the collection is active,
close it with `db.close_collection("articles").await?` first; a request to
upgrade an active handle errors. Equal/lower versions use the stored schema,
so they do not validate or apply newly edited struct fields.

A fresh open merges the new schema with persisted field indexes:

- Existing fields retain their numeric indexes; removed indexes are never
  reused. New fields must be optional.
- Required types can become optional. Fixed nested maps may gain optional
  keys or lose keys. Incompatible type changes, optional-to-required changes,
  and changes to existing uniqueness flags are rejected.
- Retired nested keys cannot be reused without a data migration. Incomplete
  legacy allocation/key history requires a complete consistent scan. The
  collection upgrade path recovers it from stored documents and mutation-intent
  images; standalone users can use `Schema::history_recovery`. Do not invent
  allocation history.
- The upgraded schema is stored before the open callback can write documents.
  A subsequent callback error does **not** roll back a completed upgrade.
  Schema upgrades do not rebuild or remove indexes. An upgrade that retires a
  top-level field referenced by a B-Tree, BM25 or HNSW index is rejected before
  the new schema is stored. Open under the old schema, remove affected indexes,
  close, then upgrade. Plan any required rebuild explicitly.

Use `collection.schema()` for raw documents after opening. The storage format
uses stable numeric field indexes; deriving a new schema does not reconstruct
their historical assignments. Standalone `Schema::upgrade_with(&old)`
performs the compatibility/index merge but does not persist anything itself.

## CBOR serialization

```rust
use anda_db::schema::Fv;
use cbor2::{from_reader, serialized_size, to_writer};

let value = Fv::Text("memory".into());
let mut buf = Vec::new();
to_writer(&value, &mut buf)?;
let encoded_len: u64 = serialized_size(&value)?;
let decoded: Fv = from_reader(buf.as_slice())?;
assert_eq!(encoded_len, buf.len() as u64);
assert_eq!(decoded, value);
```

Use `cbor2::serialized_size` for encoded sizes. Use
`cbor2::to_canonical_vec` when deterministic bytes are needed for keys/digests;
ordinary `to_writer` is not a substitute for a specified canonical format.

A required `Json` field must be present, but its payload can be JSON null.
`set_field(name, Fv::Null)` stores `Fv::Json(Json::Null)` for that declared
type, matching creation and `set_field_as`. An `Option<Json>` field retains
the optional `Fv::Null` representation.

Untyped `FieldValue` round trips preserve data but can change variants:
`F32 -> F64`, non-negative `I64 -> U64`, `Vector -> Array(U64 bits)`, and
`Json -> Map`/primitive. Use schema-aware conversion or `FieldType::extract`
to restore the declared shape; do not demand enum-variant identity from an
untyped decode.

Schema-aware conversion recognizes byte arrays, but Serde does not serialize
every arbitrary nested `Vec<u8>` as a CBOR byte string. Use explicit
`Fv::Bytes` or a byte serializer where the wire format requires one. For large
binary fields prefer `serde_bytes::ByteBuf` / `ByteBufB64`: a `Vec<u8>`
serializes one integer per byte, making `Document::try_from` several hundred
times slower for a 64 KiB payload. No new
direct `ciborium` usage; the former `cbor_size` helper is gone.
