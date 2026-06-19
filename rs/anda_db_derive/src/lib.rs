//! Derive macros for generating AndaDB schema metadata from Rust structs.
//!
//! This crate exposes two procedural macros:
//!
//! - [`FieldTyped`] generates a `field_type()` associated function that
//!   describes a struct as an `anda_db_schema::FieldType::Map`.
//! - [`AndaDBSchema`] generates a `schema()` associated function returning an
//!   `anda_db_schema::Schema` for collection creation.
//!
//! Both macros follow serde field naming rules where possible, so generated
//! metadata matches the serialized document shape used by AndaDB.

use proc_macro::TokenStream;

mod common;
mod field_typed;
mod schema;

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
///   type, including nested wrappers).
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
///
/// # Type inference
///
/// When `#[field_type]` is absent, the type is inferred from the Rust type:
///
/// - `String` / `&str` -> `Text`
/// - integers / floats / `bool` -> their numeric `FieldType`
/// - `Vec<u8>`, `[u8; N]`, `Bytes`, `ByteBuf`, `ByteArray`, `*B64` -> `Bytes`
/// - `Vec<bf16>`, `[bf16; N]` -> `Vector`
/// - `Vec<T>` / `HashSet<T>` / `BTreeSet<T>` -> `Array(T)`
/// - `HashMap<K, V>` / `BTreeMap<K, V>` (string-, signed integer-, or
///   bytes-like key) -> `Map`
/// - `Option<T>` -> `Option(T)`
/// - `Box<T>` / `Arc<T>` / `Rc<T>` / `Cow<'_, T>` -> the inner `T` (serde
///   serializes these wrappers transparently)
/// - `serde_json::Value`, `Json` -> `Json`
/// - any other path -> the type's `field_type()` function (so the type must
///   itself derive `FieldTyped`)
///
/// Standalone `bf16` values are intentionally rejected -- vectors, not
/// scalars, are the supported abstraction.
///
/// All diagnostics are spanned at the offending field, type or attribute.
///
/// # Example
///
/// ```rust,ignore
/// use anda_db_schema::{FieldType, FieldTyped};
/// use ic_auth_types::Xid;
///
/// #[derive(FieldTyped)]
/// struct User {
///     #[field_type = "Bytes"]
///     id: Xid,
///     name: String,
///     age: u32,
/// }
/// ```
#[proc_macro_derive(FieldTyped, attributes(field_type, cbor))]
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
/// - Doc comments (`/// ...`) are concatenated and used as the field
///   description (`FieldEntry::with_description`).
///
/// Two fields that would serialize under the same schema name (e.g. via
/// renames) are rejected at compile time.
///
/// # Special fields
///
/// The `_id: u64` primary-key column is injected by the schema builder
/// automatically, so declaring it on the struct is optional. When declared,
/// it must be of type `u64` and keep serializing as `"_id"` (beware
/// `rename_all` rules: add `#[serde(rename = "_id")]` if needed); it is
/// validated at compile time and skipped during code generation.
///
/// # Example
///
/// ```rust,ignore
/// use anda_db_schema::{FieldEntry, FieldType, Schema, SchemaError};
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
/// ```
///
/// Expands to:
///
/// ```rust,ignore
/// impl User {
///     pub fn schema() -> Result<Schema, SchemaError> {
///         // ... generated schema construction code
///     }
/// }
/// ```
#[proc_macro_derive(AndaDBSchema, attributes(field_type, unique))]
pub fn anda_db_schema_derive(input: TokenStream) -> TokenStream {
    schema::anda_db_schema_derive(input)
}
