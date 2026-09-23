//! Derive macros for generating AndaDB schema metadata from Rust structs.
//!
//! This crate exposes two procedural macros:
//!
//! - [`FieldTyped`] generates a `field_type()` associated function that
//!   describes a struct as an `anda_db_schema::FieldType::Map`.
//!   Its `try_field_type()` counterpart reports recursive/invalid types.
//! - [`AndaDBSchema`] generates a `schema()` associated function returning an
//!   `anda_db_schema::Schema` for collection creation.
//!
//! Both macros follow serde field naming rules where possible, so generated
//! metadata matches the serialized document shape used by AndaDB.
//!
//! Generated code is self-contained: it resolves `Schema`, `SchemaError`,
//! `FieldEntry`, `FieldType` and `FieldKey` through the `anda_db_schema`
//! crate (or the `anda_db::schema` re-export when only `anda_db` is a
//! dependency), so call sites do not need to import those names.

use proc_macro::TokenStream;

mod common;
mod field_typed;
mod schema;

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeExamples;

/// A derive macro that generates a `field_type()` associated function for a
/// struct.
///
/// The generated method returns a `FieldType::Map` whose keys are the
/// serialized field names and whose values are the inferred or explicitly
/// overridden `anda_db_schema::FieldType` for each field. It is the
/// building block used by `AndaDBSchema` for nested user-defined types.
///
/// # Attributes
///
/// - `#[field_type = "TypeName"]` -- override the inferred type. The string
///   accepts a small DSL: primitives (`Bytes`, `Text`, `U64`, ...), as well
///   as `Array<T>`, `Option<T>`, `Map<String, T>`, `Map<Text, T>`,
///   `Map<I64, T>` and `Map<Bytes, T>` (where `T` is itself any supported
///   type, including nested wrappers). For a *value*, the Rust spellings
///   (`String`, `str`, `u64`, `i32`, `f64`, `bool`, ...) are accepted as
///   synonyms of the `FieldType` names; a *map key* stays limited to
///   `String` / `Text` / `Bytes` / `I64` plus `i8` ... `isize`, the only key
///   variants `FieldKey` has. `Option<Option<T>>` is rejected.
/// - `#[cbor(key = N)]` -- for nested structs that also derive
///   `cbor2::Cbor`, use the integer CBOR map key as the generated
///   `FieldKey` instead of the serde text name.
/// - `#[serde(rename = "name")]` / `#[serde(rename_all = "...")]` -- the
///   generated map follows the *serialized* field names, so field-level
///   renames and container-level case rules (e.g. `camelCase`) are both
///   honoured, with the same precedence as serde itself.
/// - `#[serde(skip)]` / `#[serde(skip_serializing)]` -- the field never
///   appears in serialized output and is therefore excluded from the
///   generated map.
/// - `#[serde(flatten)]` and `#[serde(transparent)]` are rejected with a
///   compile error: they change the serialized shape in ways a per-field
///   schema cannot describe.
/// - Other serde options are ignored. Note that `#[serde(with = "...")]` /
///   `serialize_with` may change the serialized shape -- combine them with
///   an explicit `#[field_type = "..."]` override when they do.
/// - `#[serde(tag = "...")]` and `#[serde(into = "...")]` are rejected:
///   they change the container's serialized shape.
/// - Container `#[cbor(array)]` and `#[cbor(tag = ...)]` are rejected:
///   AndaDB derives describe untagged maps.
/// - Fixed map keys `"*"` and `i64::MIN` are reserved for wildcard maps.
///   Direct recursive fields must use an explicit non-recursive override.
///
/// The generated `try_field_type() -> Result<FieldType, SchemaError>` detects
/// indirect recursion, including type aliases. `AndaDBSchema::schema()` uses
/// this fallible path automatically for derived nested types. The legacy
/// `field_type()` convenience method panics on invalid declarations; use
/// `try_field_type()` when the type graph may be recursive.
///
/// **Warning:** `#[serde(skip_serializing_if = "...")]` on a **non-`Option`**
/// field is a trap: the field is described as *required*, but serde may omit
/// it at runtime, which then fails `Document::try_from` with
/// `field ... is required`. Either use an `Option<T>` field or make the
/// declared type optional via `#[field_type = "Option<...>"]`.
///
/// # Type inference
///
/// When `#[field_type]` is absent, the type is inferred from the Rust type:
///
/// - `String` / `&str` -> `Text`
/// - integers / floats / `bool` -> their numeric `FieldType`
/// - `Vec<u8>`, `[u8; N]`, `Bytes`, `ByteBuf`, `ByteArray`, `*B64` -> `Bytes`
/// - `Vec<bf16>`, `[bf16; N]` -> `Vector`
/// - `Vec<T>` / `VecDeque<T>` / `LinkedList<T>` / `BinaryHeap<T>` /
///   `HashSet<T>` / `BTreeSet<T>` -> `Array(T)`
/// - `(A, B, ...)` -> the tuple-like `Array([A, B, ...])`; a one-element
///   tuple is a compile error (`Array` with one inner type is a homogeneous
///   array of any length)
/// - `HashMap<K, V>` / `BTreeMap<K, V>` (string-, signed integer-, or
///   bytes-like key, `[u8; N]` included) -> `Map`
/// - `Option<T>` -> `Option(T)`; `Option<Option<T>>` is a compile error
///   (serde serializes `Some(None)` and `None` identically)
/// - `u128` / `i128` are a compile error: AndaDB integers are 64-bit
/// - `Box<T>` / `Arc<T>` / `Rc<T>` / `Cow<'_, T>` -> the inner `T` (serde
///   serializes these wrappers transparently)
/// - `serde_json::Value`, `Json` -> `Json`
/// - any other path -> the type's `field_type()` function (so the type must
///   itself derive `FieldTyped`)
///
/// Inference matches on the *name* of the type (last path segment), like
/// serde does: a user-defined type that happens to be called `Option`,
/// `Vec`, `Json`, `Vector`, `Bytes`, ... will be misidentified. Use an
/// explicit `#[field_type = "..."]` override for such types.
///
/// A field whose type is a bare generic parameter (e.g. `value: T`) cannot
/// be inferred and is rejected with a compile error; annotate it with
/// `#[field_type = "..."]`.
///
/// Standalone `bf16` values are intentionally rejected -- vectors, not
/// scalars, are the supported abstraction.
///
/// All diagnostics are spanned at the offending field, type or attribute.
///
/// # Example
///
/// ```rust
/// use anda_db_schema::{ByteArrayB64, FieldTyped};
///
/// #[derive(FieldTyped)]
/// struct User {
///     #[field_type = "Bytes"]
///     id: ByteArrayB64<12>,
///     name: String,
///     age: u32,
/// }
/// assert!(User::try_field_type().is_ok());
/// ```
#[proc_macro_derive(FieldTyped, attributes(field_type, cbor, serde))]
pub fn field_typed_derive(input: TokenStream) -> TokenStream {
    field_typed::field_typed_derive(input)
}

/// A derive macro that generates a `schema()` associated function for a
/// struct.
///
/// The generated method builds a fully-formed `anda_db_schema::Schema`
/// using `Schema::builder()`, with one `FieldEntry` per serialized field
/// (excluding `_id`, which is provided by the builder itself).
///
/// # Attributes
///
/// - `#[field_type = "TypeName"]` -- override the inferred type. Same DSL as
///   for `FieldTyped`; see that macro's docs for the full grammar.
/// - `#[unique]` -- mark the field as having a unique constraint
///   (`FieldEntry::with_unique`).
/// - `#[serde(rename = "name")]` / `#[serde(rename_all = "...")]` -- the
///   schema follows the *serialized* field names, so field-level renames and
///   container-level case rules (e.g. `camelCase`) are both honoured, with
///   the same precedence as serde itself.
/// - `#[serde(skip)]` / `#[serde(skip_serializing)]` -- the field never
///   appears in serialized output and is therefore excluded from the schema.
/// - `#[serde(flatten)]` and `#[serde(transparent)]` are rejected with a
///   compile error: they change the serialized shape in ways a per-field
///   schema cannot describe.
/// - `#[cbor(key = N)]` is rejected with a compile error: top-level document
///   fields are stored under their text names, so an integer CBOR key could
///   never match the schema. (It remains supported in nested structs
///   deriving `FieldTyped`.)
/// - Container `#[cbor(array)]` and `#[cbor(tag = ...)]` are rejected:
///   documents must serialize as untagged maps.
/// - Doc comments (`/// ...`) are concatenated and used as the field
///   description (`FieldEntry::with_description`).
///
/// **Warning:** `#[serde(skip_serializing_if = "...")]` on a **non-`Option`**
/// field is a trap: the schema marks the field *required*, but serde may
/// omit it at runtime, which then fails `Document::try_from` with
/// `field ... is required`. Either use an `Option<T>` field or make the
/// declared type optional via `#[field_type = "Option<...>"]`.
///
/// Two fields that would serialize under the same schema name (e.g. via
/// renames) are rejected at compile time.
///
/// # Special fields
///
/// The struct **must** declare an `_id: u64` field. Its `FieldEntry` is
/// injected by the schema builder (so it is skipped during code generation),
/// but the builder injects it as *required*, and `Document::try_from` reads
/// every required field out of the serialized value — a struct that does not
/// serialize an `"_id"` key would fail at runtime with
/// `field "_id" is required`. A missing `_id`, or one removed from the
/// serialized form by `#[serde(skip)]` / `#[serde(skip_serializing)]`, is
/// therefore a compile error.
///
/// `_id` must be of type `u64` and keep serializing as `"_id"` (beware
/// `rename_all` rules: add `#[serde(rename = "_id")]` if needed); this is
/// validated at compile time. A `#[field_type]` override on `_id` is
/// rejected: the primary key is always `FieldType::U64`.
///
/// # Example
///
/// ```rust
/// use anda_db_derive::AndaDBSchema;
///
/// #[derive(AndaDBSchema)]
/// struct User {
///     /// AndaDB-managed primary key
///     _id: u64,
///     /// User's unique identifier
///     #[field_type = "Bytes"]
///     #[unique]
///     id: [u8; 12],
///     /// User's display name
///     name: String,
///     /// User's age in years
///     age: Option<u32>,
///     /// Whether the user account is active
///     active: bool,
///     /// User tags for categorization
///     tags: Vec<String>,
/// }
/// assert!(User::schema().is_ok());
/// ```
///
/// Expands to:
///
/// ```text
/// impl User {
///     pub fn schema() -> Result<Schema, SchemaError> {
///         // ... generated schema construction code
///     }
/// }
/// ```
#[proc_macro_derive(AndaDBSchema, attributes(field_type, unique, cbor, serde))]
pub fn anda_db_schema_derive(input: TokenStream) -> TokenStream {
    schema::anda_db_schema_derive(input)
}
