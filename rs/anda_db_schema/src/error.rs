//! Error types used throughout the `anda_db_schema` crate.
use std::fmt;
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

    /// A `FieldType` declaration is malformed — `Option<Option<T>>`, a `Map`
    /// mixing a wildcard key with other keys, or excessive nesting. See
    /// [`FieldType::validate_declaration`](crate::FieldType::validate_declaration).
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

impl SchemaError {
    /// The message without the variant's `Display` prefix, so wrapping one
    /// error into another does not repeat "Invalid field value: ".
    pub(crate) fn detail(&self) -> &str {
        match self {
            Self::Schema(msg)
            | Self::FieldType(msg)
            | Self::FieldValue(msg)
            | Self::FieldName(msg)
            | Self::Validation(msg)
            | Self::Serialization(msg) => msg,
        }
    }
}

/// Maximum bytes of a value's `Debug` output embedded in an error message.
const BRIEF_LIMIT: usize = 128;

/// Formats a value's `Debug` output for an error message, cut off after
/// [`BRIEF_LIMIT`] bytes. A mistyped blob or vector would otherwise produce
/// an error larger than the payload itself; formatting also stops at the cut.
pub(crate) struct Brief<'a, T: ?Sized>(pub(crate) &'a T);

impl<T: fmt::Debug + ?Sized> fmt::Display for Brief<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct Limited<'a, 'b> {
            out: &'a mut fmt::Formatter<'b>,
            left: usize,
            truncated: bool,
        }

        impl fmt::Write for Limited<'_, '_> {
            fn write_str(&mut self, s: &str) -> fmt::Result {
                if s.len() <= self.left {
                    self.left -= s.len();
                    return self.out.write_str(s);
                }
                self.out.write_str(&s[..s.floor_char_boundary(self.left)])?;
                self.left = 0;
                self.truncated = true;
                // Stops the remaining Debug output.
                Err(fmt::Error)
            }
        }

        let mut out = Limited {
            out: f,
            left: BRIEF_LIMIT,
            truncated: false,
        };
        match fmt::write(&mut out, format_args!("{:?}", self.0)) {
            Err(_) if out.truncated => out.out.write_str("…"),
            result => result,
        }
    }
}
