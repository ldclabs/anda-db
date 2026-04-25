use proc_macro::TokenStream;

mod common;
mod field_typed;
mod schema;

/// A derive macro that generates a `field_type()` associated function for a
/// struct.
///
/// The generated method returns a `FieldType::Map` whose keys are the
/// (possibly serde-renamed) field names and whose values are the inferred or
/// explicitly overridden [`anda_db_schema::FieldType`] for each field. It is
/// the building block used by `AndaDBSchema` for nested user-defined types.
///
/// # Attributes
///
/// - `#[field_type = "TypeName"]` -- override the inferred type. The string
///   accepts a small DSL: primitives (`Bytes`, `Text`, `U64`, ...), as well
///   as `Array<T>`, `Option<T>`, `Map<String, T>`, `Map<Text, T>` and
///   `Map<Bytes, T>` (where `T` is itself any supported type, including
///   nested wrappers).
/// - `#[serde(rename = "name")]` -- use the renamed identifier as the
///   schema field name. Other serde options are ignored.
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
/// - `HashMap<K, V>` / `BTreeMap<K, V>` (string- or bytes-like key) -> `Map`
/// - `Option<T>` -> `Option(T)`
/// - `serde_json::Value`, `Json` -> `Json`
/// - any other path -> the type's `field_type()` function (so the type must
///   itself derive `FieldTyped`)
///
/// Standalone `bf16` values are intentionally rejected -- vectors, not
/// scalars, are the supported abstraction.
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
#[proc_macro_derive(FieldTyped, attributes(field_type))]
pub fn field_typed_derive(input: TokenStream) -> TokenStream {
    field_typed::field_typed_derive(input)
}

/// A derive macro that generates a `schema()` associated function for a
/// struct.
///
/// The generated method builds a fully-formed [`anda_db_schema::Schema`]
/// using `Schema::builder()`, with one `FieldEntry` per field (excluding the
/// mandatory `_id: u64`, which is provided by the builder itself).
///
/// # Attributes
///
/// - `#[field_type = "TypeName"]` -- override the inferred type. Same DSL as
///   for `FieldTyped`; see that macro's docs for the full grammar.
/// - `#[unique]` -- mark the field as having a unique constraint
///   (`FieldEntry::with_unique`).
/// - `#[serde(rename = "name")]` -- use the renamed identifier as the schema
///   field name.
/// - Doc comments (`/// ...`) are concatenated and used as the field
///   description (`FieldEntry::with_description`).
///
/// # Special fields
///
/// The struct **must** declare `_id: u64`. The field is validated at compile
/// time but skipped during code generation -- AndaDB manages the primary
/// key automatically.
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
