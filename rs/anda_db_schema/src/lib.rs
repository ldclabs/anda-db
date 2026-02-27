mod document;
mod error;
mod field;
mod resource;
mod schema;
mod value_serde;

pub use anda_db_derive::{AndaDBSchema, FieldTyped};

pub use document::*;
pub use error::*;
pub use field::*;
pub use resource::*;
pub use schema::*;

/// Validate a field name
///
/// Field names must:
/// - Not be empty
/// - Not exceed 64 characters
/// - Contain only lowercase letters, numbers, and underscores
///
/// # Arguments
/// * `s` - The field name to validate
///
/// # Returns
/// * `Result<(), SchemaError>` - Ok if valid, or an error message if invalid
pub fn validate_field_name(s: &str) -> Result<(), SchemaError> {
    if s.is_empty() {
        return Err(SchemaError::FieldName("empty string".to_string()));
    }

    if s.len() > 64 {
        return Err(SchemaError::FieldName(format!(
            "string length {} exceeds the limit 64",
            s.len()
        )));
    }

    // Only ASCII characters are allowed, so byte-level checking is sufficient and faster
    for &b in s.as_bytes() {
        if !matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'_' ) {
            return Err(SchemaError::FieldName(format!(
                "Invalid character {:?} in {s:?}",
                char::from(b)
            )));
        }
    }
    Ok(())
}
