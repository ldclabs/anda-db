//! Field types, keys, values and metadata. The facade preserves public paths
//! while each module owns one part of the type system.
use crate::{BoxError, SchemaError, validate_field_name};
use std::collections::BTreeMap;

mod budget;
mod entry;
mod field_type;
mod key;
mod value;

pub use budget::{FieldValueBudget, MAX_CONVERSION_DEPTH};
pub use entry::FieldEntry;
pub use field_type::FieldType;
pub use key::{BYTES_WILDCARD_KEY, FieldKey, I64_WILDCARD_KEY, TEXT_WILDCARD_KEY, as_wildcard_map};
pub use value::{FieldValue, vector_from_f32, vector_from_f64};

use budget::check_conversion_depth;
use field_type::{ValueMode, is_f32_read_back};
use key::check_wildcard_key;
use value::{
    cbor_into_json, exact_f32_from_integer, field_value_into_json, field_value_to_cbor,
    u8_array_from, validate_json_shape,
};

/// Re-export Map from serde_json
pub use serde_json::Map;

/// Re-export bf16 from half crate
pub use half::bf16;

pub use ic_auth_types::{ByteArrayB64, ByteBufB64};

/// Type alias for `Vec<bf16>`
pub type Vector = Vec<bf16>;

/// Type alias for FieldType
pub type Ft = FieldType;

/// Type alias for FieldValue
pub type Fv = FieldValue;

/// Type alias for FieldEntry
pub type Fe = FieldEntry;

/// Type alias for cbor2::Value
pub type Cbor = cbor2::Value;

/// Type alias for serde_json::Value
pub type Json = serde_json::Value;

/// Type alias for [`BTreeMap<usize, FieldValue>`], the canonical container
/// for a document's field values.
///
/// Keys are the stable [`FieldEntry::idx`] values from the document's schema,
/// not field names. This keeps records compact on disk and makes lookups
/// constant in space regardless of name length.
pub type IndexedFieldValues = BTreeMap<usize, FieldValue>;

#[cfg(test)]
mod tests;
