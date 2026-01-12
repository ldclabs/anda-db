use proc_macro::TokenStream;

mod common;
mod field_typed;
mod schema;

/// A derive macro that generates a `field_type()` function for structs.
///
/// This macro analyzes the struct fields and their types, mapping them to the
/// appropriate `FieldType` enum variants. It supports common Rust types and
/// handles Option<T> wrappers.
///
/// # Attributes
///
/// - `field_type = "TypeName"`: Override the inferred type with a specific FieldType
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
///
/// This will generate a `field_type()` method that returns a `FieldType::Map`
/// containing the type information for each field.
#[proc_macro_derive(FieldTyped, attributes(field_type))]
pub fn field_typed_derive(input: TokenStream) -> TokenStream {
    field_typed::field_typed_derive(input)
}

/// A derive macro that generates a `schema()` function for structs.
///
/// This macro creates an AndaDB Schema definition based on the struct fields.
/// It automatically handles field type mapping and supports various attributes
/// for customization.
///
/// # Attributes
///
/// - `field_type = "TypeName"`: Override the inferred type with a specific FieldType
/// - `unique`: Mark the field as unique in the collection
/// - `serde(rename = "new_name")`: Use a different name in the schema
/// - Doc comments (`///`) are used as field descriptions
///
/// # Example
///
/// ```rust,ignore
/// use anda_db_schema::{FieldEntry, FieldType, Schema, SchemaError};
/// use anda_db_derive::AndaDBSchema;
///
/// #[derive(AndaDBSchema)]
/// struct User {
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
/// This will generate:
/// ```rust,ignore
/// impl User {
///     pub fn schema() -> Result<Schema, SchemaError> {
///         // ... generated schema creation code
///     }
/// }
/// ```
#[proc_macro_derive(AndaDBSchema, attributes(field_type, unique))]
pub fn anda_db_schema_derive(input: TokenStream) -> TokenStream {
    schema::anda_db_schema_derive(input)
}
