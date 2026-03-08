//! Error types for schema module
use thiserror::Error;

/// A type alias for a boxed error that is thread-safe and sendable across threads.
/// This is commonly used as a return type for functions that can return various error types.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Schema related errors
#[derive(Error, Debug)]
pub enum SchemaError {
    #[error("Invalid schema: {0}")]
    Schema(String),

    /// Invalid field type error
    #[error("Invalid field type: {0}")]
    FieldType(String),

    /// Invalid field value error
    #[error("Invalid field value: {0}")]
    FieldValue(String),

    /// Invalid field name error
    #[error("Invalid field name: {0}")]
    FieldName(String),

    /// Field validation error
    #[error("Field validation failed: {0}")]
    Validation(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),
}
