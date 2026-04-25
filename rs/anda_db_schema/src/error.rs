//! Error types used throughout the `anda_db_schema` crate.
use thiserror::Error;

/// A boxed, thread-safe `std::error::Error`.
///
/// This is the canonical error type used by `TryFrom` conversions in this
/// crate, where the conversion may fail for several unrelated reasons.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Errors produced when building, validating or (de)serializing a schema,
/// a field entry, or a field value.
#[derive(Error, Debug)]
pub enum SchemaError {
    /// The schema definition itself is invalid — for example a duplicate
    /// field name, an out-of-range index, or an incompatible upgrade.
    #[error("Invalid schema: {0}")]
    Schema(String),

    /// A `FieldType` declaration is malformed (e.g. an unsupported nested
    /// type, an invalid `Map` key type, …).
    #[error("Invalid field type: {0}")]
    FieldType(String),

    /// A `FieldValue` does not satisfy its declared `FieldType`.
    #[error("Invalid field value: {0}")]
    FieldValue(String),

    /// A field name violates the rules enforced by
    /// [`validate_field_name`](crate::validate_field_name).
    #[error("Invalid field name: {0}")]
    FieldName(String),

    /// A document fails schema validation — usually because a required
    /// field is missing or because a field appears that the schema does
    /// not declare.
    #[error("Field validation failed: {0}")]
    Validation(String),

    /// CBOR or serde (de)serialization failed.
    #[error("Serialization error: {0}")]
    Serialization(String),
}
