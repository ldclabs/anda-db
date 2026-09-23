//! Per-field schema metadata and validation boundaries.
use super::*;
use serde::{Deserialize, Serialize};
/// Metadata for a single field in a [`Schema`](crate::Schema).
///
/// `FieldEntry` ties a textual `name` to a [`FieldType`] together with an
/// optional human-readable description, an `unique` flag (used by
/// collection-level uniqueness indexes) and a stable numeric `idx`.
///
/// The numeric `idx` is assigned by the schema builder and is what gets
/// persisted alongside each field value: documents are stored as
/// `BTreeMap<idx, FieldValue>` rather than `BTreeMap<name, FieldValue>` to
/// keep records compact. Schema migrations preserve `idx` values for
/// fields that exist in both the old and new schemas.
///
/// On the wire, every key is renamed to a single letter (`n`, `d`, `t`,
/// `u`, `i`) for the same reason. Long-form names are accepted on input
/// via `serde(alias = ...)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldEntry {
    /// Field name. Must be unique within a schema and conform to the rules
    /// enforced by [`validate_field_name`].
    #[serde(rename = "n", alias = "name")]
    name: String,

    /// Human-readable description of the field.
    ///
    /// Used as documentation for tools and as context for LLM clients that
    /// inspect the schema to decide how to populate or query a field.
    #[serde(rename = "d", alias = "description")]
    description: String,

    /// Declared type of the field.
    #[serde(rename = "t", alias = "type")]
    r#type: FieldType,

    /// Whether the field value must be unique across all documents in the
    /// collection. Enforcement is performed at the collection layer.
    #[serde(rename = "u", alias = "unique")]
    unique: bool,

    /// Stable numeric index used as the persistent key for this field's
    /// values. The `_id` field always has `idx == 0`.
    #[serde(rename = "i", alias = "index")]
    idx: usize,
}

impl FieldEntry {
    /// Create a new field entry
    ///
    /// # Arguments
    /// * `name` - Field name
    /// * `r#type` - Field type
    ///
    /// # Returns
    /// * `Result<Self, SchemaError>` - The created field entry, or an error
    ///   when the name violates [`validate_field_name`] or the type is not a
    ///   well-formed declaration (see [`FieldType::validate_declaration`])
    pub fn new(name: String, r#type: FieldType) -> Result<Self, SchemaError> {
        validate_field_name(&name)?;
        r#type.validate_declaration()?;
        Ok(Self {
            name,
            r#type,
            description: String::new(),
            unique: false,
            idx: 0,
        })
    }

    /// Set the field description
    ///
    /// # Arguments
    /// * `description` - Field description
    ///
    /// # Returns
    /// * `Self` - The modified field entry
    pub fn with_description(mut self, description: String) -> Self {
        self.description = description;
        self
    }

    /// Mark the field as unique
    ///
    /// # Returns
    /// * `Self` - The modified field entry
    pub fn with_unique(mut self) -> Self {
        self.unique = true;
        self
    }

    /// Set the field index
    ///
    /// # Arguments
    /// * `idx` - Field index value
    ///
    /// # Returns
    /// * `Self` - The modified field entry
    pub fn with_idx(mut self, idx: usize) -> Self {
        self.idx = idx;
        self
    }

    /// Set the field index in place.
    ///
    /// Useful when you need to update the index of an existing entry without
    /// cloning all of its other data (e.g. during schema migration).
    ///
    /// # Arguments
    /// * `idx` - Field index value
    ///
    /// # Returns
    /// * `&mut Self` - The modified field entry
    pub fn set_idx(&mut self, idx: usize) -> &mut Self {
        self.idx = idx;
        self
    }

    /// Repairs the one malformed declaration older versions could persist:
    /// see [`FieldType::flatten_nested_options`]. Run by `Schema`
    /// deserialization before [`FieldType::validate_declaration`].
    pub(crate) fn repair_declaration(&mut self) -> &mut Self {
        self.r#type.flatten_nested_options();
        self
    }

    /// Get the field name
    ///
    /// # Returns
    /// * `&str` - The field name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the field type
    ///
    /// # Returns
    /// * `&FieldType` - The field type
    pub fn r#type(&self) -> &FieldType {
        &self.r#type
    }

    /// Check if this field is required.
    ///
    /// A field is required if its type is NOT `FieldType::Option(_)`.
    ///
    /// # Returns
    /// * `bool` - True if the field is required
    pub fn required(&self) -> bool {
        !matches!(self.r#type, FieldType::Option(_))
    }

    /// Check if the field is unique
    ///
    /// # Returns
    /// * `bool` - True if the field is unique
    pub fn unique(&self) -> bool {
        self.unique
    }

    /// Get the field index
    ///
    /// # Returns
    /// * `usize` - The field index
    pub fn idx(&self) -> usize {
        self.idx
    }

    /// Extract a field value from a CBOR value
    ///
    /// # Arguments
    /// * `val` - The CBOR value to extract from
    /// * `validate` - Whether to validate the extracted value
    ///
    /// # Returns
    /// * `Result<FieldValue, SchemaError>` - The extracted field value or an error message
    pub fn extract(&self, val: Cbor, validate: bool) -> Result<FieldValue, SchemaError> {
        match self.r#type.extract(val) {
            Ok(v) => {
                if validate {
                    self.validate(&v)?;
                }
                Ok(v)
            }
            Err(err) => Err(self.invalid(err)),
        }
    }

    /// Coerces a field value into this field's declared shape, then validates
    /// it. This is the entry point for values that did not arrive as CBOR
    /// (a JSON API payload, say).
    ///
    /// [`FieldEntry::validate`] alone accepts only a value that already *is*
    /// the declared variant (or a documented read-back shape of it), whereas
    /// [`Document::try_from`](crate::Document::try_from) runs every value
    /// through the [`FieldType::extract`] CBOR coercion first — so a `Bytes`
    /// field accepts an array of `0..=255` there but not here. Any API that
    /// takes [`FieldValue`]s from a client must go through this function, or
    /// creating a document and updating the same field accept different
    /// shapes and a client can write a document it cannot then update.
    /// [`Document::set_field`](crate::Document::set_field) does. Canonical
    /// values and typed containers retain their buffers; only noncanonical
    /// shapes use the CBOR compatibility adapter.
    ///
    /// # Arguments
    /// * `value` - The field value to coerce
    ///
    /// # Returns
    /// * `Result<FieldValue, SchemaError>` - The canonical value or an error
    pub fn coerce(&self, value: FieldValue) -> Result<FieldValue, SchemaError> {
        if value == FieldValue::Null && self.r#type != FieldType::Json {
            // Keep `validate`'s "field is required" wording; `extract` would
            // only report the type mismatch.
            self.validate(&value)?;
            return Ok(value);
        }

        let value = self
            .r#type
            .prepare(value, 0, ValueMode::Write)
            .map_err(|err| self.invalid(err))?;
        // Mirrors `Document::try_from`: typed coercion also enforces the
        // write-admission budget for newly supplied values.
        value
            .validate_complexity()
            .map_err(|err| self.invalid(err))?;
        Ok(value)
    }

    /// Materialize one stored value, with a single typed traversal and one
    /// complexity check. The caller commits the document only on success.
    pub(crate) fn prepare_read(&self, value: FieldValue) -> Result<FieldValue, SchemaError> {
        let value = self.r#type.prepare(value, 0, ValueMode::Read)?;
        // Persisted values may predate write-admission size limits. Enforce
        // nesting safety while preserving those already-stored wide values.
        // New inserts and changed fields still use the default write budget.
        value.validate_complexity_with(super::FieldValueBudget {
            max_nodes: usize::MAX,
            max_array_len: usize::MAX,
            max_map_entries: usize::MAX,
            ..Default::default()
        })?;
        Ok(value)
    }

    /// Validate a field value against this field entry's constraints
    ///
    /// # Arguments
    /// * `value` - The field value to validate
    ///
    /// # Returns
    /// * `Result<(), SchemaError>` - Ok if valid, or an error message if invalid
    pub fn validate(&self, value: &FieldValue) -> Result<(), SchemaError> {
        if value == &FieldValue::Null && self.r#type != FieldType::Json {
            if matches!(self.r#type, FieldType::Option(_)) {
                return Ok(());
            }

            return Err(SchemaError::FieldValue(format!(
                "field {} is required, expected type {:?}",
                self.name, self.r#type
            )));
        }

        self.r#type.validate(value).map_err(|err| self.invalid(err))
    }

    fn invalid(&self, err: SchemaError) -> SchemaError {
        SchemaError::FieldValue(format!(
            "field {} is invalid, error: {}",
            self.name,
            err.detail()
        ))
    }
}
