//! # `anda_db_schema`
//!
//! Type system and schema definitions for [Anda DB](https://github.com/ldclabs/anda-db),
//! the embedded knowledge & memory database for AI Agents.
//!
//! This crate provides the building blocks used across all Anda DB sub-crates:
//!
//! - [`FieldType`] (alias [`Ft`]): a closed enum of every type a field may declare,
//!   including primitive, composite (`Array`, `Map`) and `Option` variants.
//! - [`FieldValue`] (alias [`Fv`]): the runtime representation of an actual value.
//!   It can losslessly round-trip with [`Cbor`](ciborium::Value) and is
//!   serde-compatible for both human-readable (JSON) and binary (CBOR) formats.
//! - [`FieldEntry`] (alias [`Fe`]): metadata for a single field — name, type,
//!   description, uniqueness flag and a stable numeric `idx` used as the on-disk
//!   key (instead of the field name) to keep records compact.
//! - [`Schema`] / [`SchemaBuilder`]: an ordered, versioned collection of
//!   `FieldEntry` values. Schemas are versioned and support forward-compatible
//!   migration via [`Schema::upgrade_with`].
//! - [`Document`] / [`DocumentOwned`]: schema-bound and standalone document
//!   representations.
//! - [`Resource`]: a predefined struct describing an external resource
//!   (file, blob, URI…) referenced from a document.
//!
//! ## Derive macros
//!
//! Two macros are re-exported from `anda_db_derive`:
//!
//! - [`AndaDBSchema`] — generates a `schema()` constructor from a Rust struct.
//! - [`FieldTyped`] — generates a `field_type()` constructor returning the
//!   nested `FieldType::Map` describing the struct's layout.
//!
//! See the crate-level guide in `docs/anda_db_schema.md` for a full tour and
//! [`SCHEMA.md`] for the on-disk format.
//!
//! ## Storage format
//!
//! All values are normalized to CBOR for persistence. The CBOR encoding is
//! deterministic and small; floating point values disallow `NaN` so that
//! [`FieldValue`] keeps a meaningful `PartialEq`.

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

/// Validate a field name against Anda DB's naming rules.
///
/// A valid field name must:
/// - be non-empty,
/// - be at most 64 bytes long, and
/// - contain only ASCII lowercase letters (`a`–`z`), digits (`0`–`9`)
///   and underscores (`_`).
///
/// The `_id` field used as the document primary key is also a valid name.
///
/// # Arguments
/// * `s` - The field name to validate
///
/// # Returns
/// * `Ok(())` if `s` is a legal field name.
/// * `Err(SchemaError::FieldName)` describing the first violation otherwise.
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
