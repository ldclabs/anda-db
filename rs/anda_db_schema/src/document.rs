//! [`Document`] and [`DocumentOwned`] — the runtime and on-disk
//! representations of an Anda DB document.
use cbor2::Value;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::sync::Arc;

use super::{Fv, IndexedFieldValues, Schema, SchemaError};

/// The unique identifier for a document within a collection.
///
/// Document IDs are 64-bit unsigned integers stored under the reserved
/// `_id` field with [`FieldEntry::idx`](crate::FieldEntry::idx) `== 0`.
pub type DocumentId = u64;

/// A single Anda DB document together with its [`Schema`].
///
/// `Document` keeps an [`Arc<Schema>`] reference so that field accessors
/// and mutators can validate and translate field names into stable on-disk
/// indexes. Use [`Document::new`] to start an empty document, or one of
/// the `try_from_*` constructors to build one from existing data.
///
/// `Document` is [`Serialize`] but not [`Deserialize`]: in order to be
/// loaded back from storage you must combine a [`DocumentOwned`] with a
/// `Schema`, see [`Document::try_from_doc`].
#[derive(Clone, Debug)]
pub struct Document {
    /// Field values indexed by their stable schema-assigned `idx`.
    fields: IndexedFieldValues,
    /// Reference to the schema that defines the document structure.
    schema: Arc<Schema>,
}

/// A standalone document without an attached [`Schema`].
///
/// `DocumentOwned` is the on-disk and over-the-wire representation: it
/// carries only the field values keyed by `idx`. To validate or to access
/// fields by name, pair it with a `Schema` via
/// [`Document::try_from_doc`].
///
/// The serialized layout is `{ "f": IndexedFieldValues }`. The single key
/// `f` is intentionally short to keep records compact.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DocumentOwned {
    /// Field values indexed by their stable schema-assigned `idx`.
    /// The `_id` field (`idx == 0`) is required.
    #[serde(rename = "f")]
    pub fields: IndexedFieldValues,
}

#[derive(Clone, Debug, Serialize)]
struct DocumentRef<'a> {
    #[serde(rename = "f")]
    pub fields: &'a IndexedFieldValues,
}

impl From<Document> for DocumentOwned {
    /// Converts a Document to DocumentOwned.
    ///
    /// # Arguments
    /// * `doc` - The Document to convert
    ///
    /// # Returns
    /// A new DocumentOwned containing the fields from the Document
    fn from(doc: Document) -> Self {
        Self { fields: doc.fields }
    }
}

impl Document {
    /// Creates a new Document with the specified schema and ID.
    ///
    /// # Arguments
    /// * `schema` - The schema that defines the document structure
    /// * `id` - The unique identifier for the document
    ///
    /// # Returns
    /// A new Document instance
    pub fn new(schema: Arc<Schema>) -> Self {
        Self {
            fields: IndexedFieldValues::new(),
            schema,
        }
    }

    /// Creates a Document from a DocumentOwned, validating against the schema.
    ///
    /// Values stored under an index the schema does not declare are dropped
    /// instead of failing validation: [`Schema::upgrade_with`] allows field
    /// removal and never reuses removed indexes, so such values can only be
    /// stale data written under an older schema. They disappear from storage
    /// the next time the document is rewritten. Entries of removed keys of a
    /// *nested* struct are dropped the same way (see
    /// [`FieldType::prune_undeclared`](crate::FieldType::prune_undeclared)).
    ///
    /// Generic (schema-less) deserialization cannot restore the declared
    /// variant of every value: a non-negative `I64` reads back as `U64` and
    /// an `F32` reads back as `F64`, and a `Vector` reads back as an array of
    /// U64 bf16 bit patterns. Such read-back shapes are normalized into the
    /// canonical variant here (see
    /// [`FieldType::normalize`](crate::FieldType::normalize)) so that index
    /// maintenance and field accessors always observe the declared variant.
    ///
    /// # Arguments
    /// * `schema` - The schema to validate against
    /// * `doc` - The DocumentOwned to convert
    ///
    /// # Returns
    /// * `Result<Self, SchemaError>` - The validated Document or an error
    pub fn try_from_doc(schema: Arc<Schema>, mut doc: DocumentOwned) -> Result<Self, SchemaError> {
        Self::drop_retired_fields(&schema, &mut doc.fields)?;
        Self::normalize_fields(&schema, &mut doc.fields);
        schema.validate(&doc.fields)?;

        Ok(Self {
            fields: doc.fields,
            schema,
        })
    }

    /// Drops values stored under indexes of since-removed fields, and
    /// rejects values under indexes this schema lineage never allocated.
    ///
    /// Removed-field leftovers are the only legitimate source of undeclared
    /// indexes *below* the allocation watermark ([`Schema::upgrade_with`]
    /// never reuses them), so those are safe to discard silently. An index at
    /// or above the watermark means the bytes belong to a different schema
    /// lineage — corrupt data, the wrong collection, or a newer writer — and
    /// silently deleting such data on the next rewrite would be destructive.
    fn drop_retired_fields(
        schema: &Schema,
        fields: &mut IndexedFieldValues,
    ) -> Result<(), SchemaError> {
        let allocated_end = schema.allocated_idx_end();
        if let Some(idx) = fields.keys().find(|idx| **idx >= allocated_end) {
            return Err(SchemaError::Validation(format!(
                "document contains field index {idx} that this schema \
                 (allocation watermark {allocated_end}) never declared; \
                 refusing to silently drop foreign or corrupt data"
            )));
        }
        fields.retain(|idx, _| schema.contains_idx(*idx));
        Ok(())
    }

    /// Prepares freshly deserialized values for validation: entries of
    /// removed *nested* struct fields are dropped — the one-level-down
    /// analogue of [`Document::drop_retired_fields`], see
    /// [`FieldType::prune_undeclared`](crate::FieldType::prune_undeclared) —
    /// and read-back value shapes are folded into the schema's canonical
    /// variants. See [`Document::try_from_doc`].
    fn normalize_fields(schema: &Schema, fields: &mut IndexedFieldValues) {
        for field in schema.iter() {
            if let Some(value) = fields.get_mut(&field.idx()) {
                field.r#type().prune_undeclared(value);
                field.r#type().normalize(value);
            }
        }
    }

    /// Creates a Document by serializing and validating a value against the schema.
    ///
    /// # Arguments
    /// * `schema` - The schema to validate against
    /// * `doc` - The value to serialize into a document
    ///
    /// # Returns
    /// * `Result<Self, SchemaError>` - The validated document or an error
    ///
    /// # Type Parameters
    /// * `T` - The type of the value to serialize
    pub fn try_from<T>(schema: Arc<Schema>, doc: &T) -> Result<Self, SchemaError>
    where
        T: Serialize,
    {
        let doc = Value::serialized(doc).map_err(|err| {
            SchemaError::Serialization(format!("failed to serialize document: {err:?}"))
        })?;
        let doc = doc.into_map().map_err(|err| {
            SchemaError::Validation(format!(
                "invalid document, expected CBOR map value, got {err:?}"
            ))
        })?;

        let mut fields = IndexedFieldValues::new();
        for (k, v) in doc {
            let k = k.into_text().map_err(|err| {
                SchemaError::Validation(format!(
                    "invalid document field key, expected CBOR text value, got {err:?}"
                ))
            })?;

            let field = schema.get_field_or_err(&k)?;
            let value = field.extract(v, false)?;
            // `extract` is strict about types, but the structural complexity
            // budget still has to be enforced here: a value that exceeds it
            // could be written but would fail validation on read-back.
            value.validate_complexity().map_err(|err| {
                SchemaError::FieldValue(format!("field {k:?} is invalid, error: {err}"))
            })?;
            if fields.insert(field.idx(), value).is_some() {
                return Err(SchemaError::Validation(format!(
                    "duplicate field {k:?} in document"
                )));
            }
        }

        // `FieldEntry::extract` is strict, so every present value already conforms
        // to its declared type; only required-field presence remains to check.
        for field in schema.iter() {
            if field.required() && !fields.contains_key(&field.idx()) {
                return Err(SchemaError::Validation(format!(
                    "field {:?} is required",
                    field.name()
                )));
            }
        }

        Ok(Self { fields, schema })
    }

    /// Deserializes the document into the specified type.
    ///
    /// The document is re-encoded as a CBOR byte stream and decoded from it,
    /// rather than walked as a `cbor2::Value` tree: the streaming decoder
    /// bridges CBOR byte strings into serde sequences, which is required to
    /// deserialize `FieldType::Bytes` fields into `Vec<u8>` / `[u8; N]`.
    ///
    /// # Returns
    /// * `Result<T, SchemaError>` - The deserialized value or an error
    ///
    /// # Type Parameters
    /// * `T` - The type to deserialize the document to
    pub fn try_into<T>(self) -> Result<T, SchemaError>
    where
        T: DeserializeOwned,
    {
        for field in self.schema.iter() {
            if field.required() && !self.fields.contains_key(&field.idx()) {
                return Err(SchemaError::Validation(format!(
                    "field {:?} is required",
                    field.name()
                )));
            }
        }

        // Serializes the document as a name-keyed CBOR map. Fields absent
        // from the document are omitted instead of being emitted as `null`:
        // `#[serde(default)]` only fills in *missing* keys, so writing an
        // explicit `null` would make `Document::try_from` -> `try_into` a
        // one-way trip for any type that skips a field on serialization. A
        // field that is present with a stored [`Fv::Null`] still serializes
        // as `null`.
        struct DocAsNamedMap<'a> {
            schema: &'a Schema,
            fields: &'a IndexedFieldValues,
        }

        impl Serialize for DocAsNamedMap<'_> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                use serde::ser::SerializeMap;

                // The map length hint must count only the entries actually
                // emitted below, or the CBOR map header would be wrong.
                let len = self
                    .schema
                    .iter()
                    .filter(|field| self.fields.contains_key(&field.idx()))
                    .count();
                let mut map = serializer.serialize_map(Some(len))?;
                for field in self.schema.iter() {
                    if let Some(value) = self.fields.get(&field.idx()) {
                        map.serialize_entry(field.name(), value)?;
                    }
                }
                map.end()
            }
        }

        let mut buf = Vec::with_capacity(256);
        cbor2::to_writer(
            &DocAsNamedMap {
                schema: &self.schema,
                fields: &self.fields,
            },
            &mut buf,
        )
        .map_err(|err| SchemaError::Serialization(format!("Failed to serialize: {err}")))?;

        cbor2::from_reader(&buf[..])
            .map_err(|err| SchemaError::Serialization(format!("Failed to deserialize: {err}")))
    }

    /// Gets the document's unique identifier.
    ///
    /// Returns `0` when the `_id` field has not been set yet. Collections
    /// assign ids starting from `1`, so `0` doubles as the "unassigned"
    /// sentinel; use [`Document::try_id`] to distinguish the two explicitly.
    ///
    /// # Returns
    /// The document's ID, or `0` if unset.
    pub fn id(&self) -> DocumentId {
        self.try_id().unwrap_or(0)
    }

    /// Gets the document's unique identifier, or `None` when the `_id` field
    /// has not been set.
    pub fn try_id(&self) -> Option<DocumentId> {
        match self.fields.get(&0) {
            Some(Fv::U64(id)) => Some(*id),
            _ => None,
        }
    }

    /// Sets the document's unique identifier.
    pub fn set_id(&mut self, id: DocumentId) -> &mut Self {
        self.fields.insert(0, Fv::U64(id));
        self
    }

    /// Gets the fields of the document.
    pub fn fields(&self) -> &IndexedFieldValues {
        &self.fields
    }

    /// Gets a field value by name.
    ///
    /// # Arguments
    /// * `name` - The name of the field to retrieve
    ///
    /// # Returns
    /// * `Option<&Fv>` - The field value or None if not found
    pub fn get_field(&self, name: &str) -> Option<&Fv> {
        if let Some(field) = self.schema.get_field(name) {
            self.fields.get(&field.idx())
        } else {
            None
        }
    }

    /// Gets a field value by name or returns an error if it doesn't exist.
    ///
    /// # Arguments
    /// * `name` - The name of the field to retrieve
    ///
    /// # Returns
    /// * `Result<&Fv, SchemaError>` - The field value or an error if not found
    pub fn get_field_or_err(&self, name: &str) -> Result<&Fv, SchemaError> {
        if let Some(field) = self.schema.get_field(name) {
            self.fields.get(&field.idx()).ok_or_else(|| {
                SchemaError::Validation(format!(
                    "field {:?} at {} not found in document",
                    name,
                    field.idx()
                ))
            })
        } else {
            Err(SchemaError::Validation(format!(
                "field {name:?} not found in schema"
            )))
        }
    }

    /// Sets a field value by name.
    ///
    /// Read-back value shapes (e.g. a non-negative `I64` observed as `U64`)
    /// are normalized into the field's canonical variant before being
    /// stored, mirroring [`Document::try_from_doc`].
    ///
    /// # Arguments
    /// * `name` - The name of the field to set
    /// * `value` - The value to store
    ///
    /// # Returns
    /// * `Result<(), SchemaError>` - Success or an error
    pub fn set_field(&mut self, name: &str, mut value: Fv) -> Result<&mut Self, SchemaError> {
        if let Some(field) = self.schema.get_field(name) {
            field.r#type().normalize(&mut value);
            field.validate(&value)?;
            self.fields.insert(field.idx(), value);
            return Ok(self);
        }

        Err(SchemaError::Validation(format!(
            "field {name:?} not found in schema"
        )))
    }

    /// Removes a field value by name.
    ///
    /// # Arguments
    /// * `name` - The name of the field to remove
    ///
    /// # Returns
    /// * `Option<Fv>` - The removed field value or None if not found
    pub fn remove_field(&mut self, name: &str) -> Option<Fv> {
        if let Some(field) = self.schema.get_field(name) {
            return self.fields.remove(&field.idx());
        }
        None
    }

    /// Gets a field value by name and deserializes it to the specified type.
    ///
    /// # Arguments
    /// * `name` - The name of the field to retrieve
    ///
    /// # Returns
    /// * `Result<T, SchemaError>` - The deserialized value or an error
    ///
    /// # Type Parameters
    /// * `T` - The type to deserialize the field value to
    pub fn get_field_as<T>(&self, name: &str) -> Result<T, SchemaError>
    where
        T: DeserializeOwned,
    {
        if let Some(field) = self.schema.get_field(name) {
            if let Some(value) = self.fields.get(&field.idx()) {
                return value.deserialized();
            } else {
                return Err(SchemaError::Validation(format!(
                    "field {name:?} not found in document"
                )));
            }
        }
        Err(SchemaError::Validation(format!(
            "field {name:?} not found in schema"
        )))
    }

    /// Sets a field value by serializing the provided value.
    ///
    /// # Arguments
    /// * `name` - The name of the field to set
    /// * `value` - The value to serialize and store
    ///
    /// # Returns
    /// * `Result<(), SchemaError>` - Success or an error
    ///
    /// # Type Parameters
    /// * `T` - The type of the value to serialize
    pub fn set_field_as<T>(&mut self, name: &str, value: &T) -> Result<&mut Self, SchemaError>
    where
        T: Serialize,
    {
        let field = self.schema.get_field_or_err(name)?;
        let value = Fv::serialized(value, Some(field.r#type()))?;
        field.validate(&value)?;
        self.fields.insert(field.idx(), value);
        Ok(self)
    }

    /// Updates the document with values from a DocumentOwned.
    ///
    /// Values stored under indexes of since-removed fields are dropped,
    /// values under never-allocated indexes are rejected, and read-back
    /// value shapes are normalized into their canonical variants, mirroring
    /// [`Document::try_from_doc`].
    ///
    /// # Arguments
    /// * `doc` - The DocumentOwned containing the new values
    ///
    /// # Returns
    /// * `Result<(), SchemaError>` - Success or an error
    pub fn set_doc(&mut self, mut doc: DocumentOwned) -> Result<(), SchemaError> {
        Self::drop_retired_fields(&self.schema, &mut doc.fields)?;
        Self::normalize_fields(&self.schema, &mut doc.fields);
        self.schema.validate(&doc.fields)?;
        self.fields = doc.fields;

        Ok(())
    }
}

impl Serialize for Document {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let doc = DocumentRef {
            fields: &self.fields,
        };
        doc.serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AndaDBSchema, FieldTyped, Fv, Resource, Vector, vector_from_f32};
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;

    #[derive(Debug, Serialize, Deserialize, PartialEq, AndaDBSchema)]
    struct TestUser {
        _id: u64,
        /// User's display name
        name: String,
        /// User's age in years
        age: u64,
        /// Whether the user account is active
        active: Option<bool>,
        /// User tags for categorization
        tags: Option<Vec<String>>,
        /// User metadata with creation and update timestamps
        meta: Option<BTreeMap<String, u64>>,
        /// Optional profile picture resource
        picture: Option<Resource>,
    }

    #[test]
    fn test_document_with_id() {
        let schema = Arc::new(TestUser::schema().unwrap());
        let id = 99u64;
        println!("Schema: {schema:#?}");
        // Schema: Schema {
        //     idx: {
        //         0,
        //         1,
        //         2,
        //         3,
        //         4,
        //         5,
        //         6,
        //     },
        //     fields: {
        //         "_id": FieldEntry {
        //             name: "_id",
        //             description: "\"_id\" is a u64 field, used as an internal unique identifier",
        //             type: U64,
        //             unique: true,
        //             idx: 0,
        //         },
        //         "active": FieldEntry {
        //             name: "active",
        //             description: "Whether the user account is active",
        //             type: Option(Bool),
        //             unique: false,
        //             idx: 3,
        //         },
        //         "age": FieldEntry {
        //             name: "age",
        //             description: "User's age in years",
        //             type: U64,
        //             unique: false,
        //             idx: 2,
        //         },
        //         "meta": FieldEntry {
        //             name: "meta",
        //             description: "User metadata with creation and update timestamps",
        //             type: Option(Map({"*": U64})),
        //             unique: false,
        //             idx: 5,
        //         },
        //         "name": FieldEntry {
        //             name: "name",
        //             description: "User's display name",
        //             type: Text,
        //             unique: false,
        //             idx: 1,
        //         },
        //         "picture": FieldEntry {
        //             name: "picture",
        //             description: "Optional profile picture resource",
        //             type: Option(Map({"b": Option(Bytes), "d": Option(Text), "h": Option(Bytes), "m": Option(Text), "n": Option(Text), "s": Option(U64), "t": Text, "u": Option(Text)})),
        //             unique: false,
        //             idx: 6,
        //         },
        //         "tags": FieldEntry {
        //             name: "tags",
        //             description: "User tags for categorization",
        //             type: Option(Array([Text])),
        //             unique: false,
        //             idx: 4,
        //         },
        //     },
        // }

        let mut doc = Document::new(schema);
        assert!(doc.fields.is_empty());
        assert_eq!(doc.id(), 0);
        doc.set_id(id);
        assert_eq!(doc.id(), id);
    }

    #[test]
    fn test_document_try_from_doc() {
        let schema = Arc::new(TestUser::schema().unwrap());
        let id = 99u64;

        // 创建有效的字段值
        let mut fields = IndexedFieldValues::new();
        fields.insert(1, Fv::Text("John Doe".to_string()));
        fields.insert(2, Fv::U64(30));

        let mut owned_doc = DocumentOwned { fields };

        assert!(Document::try_from_doc(schema.clone(), owned_doc.clone()).is_err());
        owned_doc.fields.insert(0, Fv::U64(id));

        let doc = Document::try_from_doc(schema.clone(), owned_doc).unwrap();
        assert_eq!(doc.id(), id);
        assert_eq!(doc.fields.len(), 3);
        assert_eq!(doc.get_field("age").unwrap(), &Fv::U64(30));
    }

    #[test]
    fn test_document_try_from() {
        let schema = Arc::new(TestUser::schema().unwrap());

        let test_user = TestUser {
            _id: 99,
            name: "John Doe".to_string(),
            age: 30,
            active: Some(true),
            tags: Some(vec!["user".to_string(), "admin".to_string()]),
            meta: Some(BTreeMap::from([
                ("created".to_string(), 1625097600),
                ("updated".to_string(), 1625097600),
            ])),
            picture: None,
        };

        let doc = Document::try_from(schema.clone(), &test_user).unwrap();

        assert_eq!(doc.id(), 99);
        assert_eq!(
            doc.get_field("name").unwrap(),
            &Fv::Text("John Doe".to_string())
        );
        assert_eq!(doc.get_field("age").unwrap(), &Fv::U64(30));
        assert_eq!(doc.get_field("active").unwrap(), &Fv::Bool(true));

        // 检查数组字段
        if let Fv::Array(tags) = doc.get_field("tags").unwrap() {
            assert_eq!(tags.len(), 2);
            assert_eq!(tags[0], Fv::Text("user".to_string()));
            assert_eq!(tags[1], Fv::Text("admin".to_string()));
        } else {
            panic!("Expected Array field");
        }

        // 检查映射字段
        if let Fv::Map(meta) = doc.get_field("meta").unwrap() {
            assert_eq!(meta.len(), 2);
            assert_eq!(meta.get(&"created".into()).unwrap(), &Fv::U64(1625097600));
            assert_eq!(meta.get(&"updated".into()).unwrap(), &Fv::U64(1625097600));
        } else {
            panic!("Expected Map field");
        }
    }

    #[test]
    fn test_document_try_as() {
        let schema = Arc::new(TestUser::schema().unwrap());

        let test_user = TestUser {
            _id: 99,
            name: "John Doe".to_string(),
            age: 30,
            active: Some(true),
            tags: Some(vec!["user".to_string(), "admin".to_string()]),
            meta: Some(BTreeMap::from([
                ("created".to_string(), 1625097600),
                ("updated".to_string(), 1625097600),
            ])),
            picture: None,
        };

        let doc = Document::try_from(schema.clone(), &test_user).unwrap();

        // 测试反序列化回原始结构体
        let deserialized: TestUser = doc.try_into().unwrap();

        assert_eq!(deserialized, test_user);
    }

    #[test]
    fn test_document_get_set_field() {
        let schema = Arc::new(TestUser::schema().unwrap());
        let mut doc = Document::new(schema.clone());

        // 测试设置字段
        doc.set_field("name", Fv::Text("John Doe".to_string()))
            .unwrap()
            .set_field("age", Fv::U64(30))
            .unwrap();

        // 测试获取字段
        assert_eq!(
            doc.get_field("name").unwrap(),
            &Fv::Text("John Doe".to_string())
        );
        assert_eq!(doc.get_field("age").unwrap(), &Fv::U64(30));

        // 测试设置不存在的字段
        assert!(
            doc.set_field("unknown", Fv::Text("value".to_string()))
                .is_err()
        );

        // 测试获取不存在的字段
        assert!(doc.get_field("unknown").is_none());
    }

    #[test]
    fn test_document_get_set_field_as() {
        let schema = Arc::new(TestUser::schema().unwrap());

        let mut doc = Document::new(schema.clone());

        // 测试设置字段（使用序列化）
        doc.set_field_as("name", &"John Doe".to_string()).unwrap();
        doc.set_field_as("age", &30u64).unwrap();
        doc.set_field_as("active", &true).unwrap();

        // 测试获取字段（使用反序列化）
        let name: String = doc.get_field_as("name").unwrap();
        let age: u64 = doc.get_field_as("age").unwrap();
        let active: bool = doc.get_field_as("active").unwrap();

        assert_eq!(name, "John Doe");
        assert_eq!(age, 30);
        assert!(active);

        // 测试设置不存在的字段
        assert!(doc.set_field_as("unknown", &"value".to_string()).is_err());

        // 测试获取不存在的字段
        let result: Result<String, _> = doc.get_field_as("unknown");
        assert!(result.is_err());
    }

    #[test]
    fn test_document_set_doc() {
        let schema = Arc::new(TestUser::schema().unwrap());

        let mut doc = Document::new(schema.clone());

        // 创建有效的字段值
        let mut fields = IndexedFieldValues::new();
        fields.insert(1, Fv::Text("John Doe".to_string())); // name
        fields.insert(2, Fv::U64(30)); // age

        let mut owned_doc = DocumentOwned { fields };

        assert!(doc.set_doc(owned_doc.clone()).is_err());
        owned_doc.fields.insert(
            0,
            Fv::U64(99), // id
        ); // id
        doc.set_doc(owned_doc).unwrap();

        // 验证文档是否正确设置
        assert_eq!(doc.id(), 99);
        assert_eq!(doc.fields.len(), 3);
        assert_eq!(
            doc.get_field("name").unwrap(),
            &Fv::Text("John Doe".to_string())
        );
        assert_eq!(doc.get_field("age").unwrap(), &Fv::U64(30));
    }

    #[test]
    fn test_document_from_to_owned() {
        let schema = Arc::new(TestUser::schema().unwrap());

        let mut doc = Document::new(schema.clone());
        doc.set_id(99);
        doc.set_field("name", Fv::Text("John Doe".to_string()))
            .unwrap();
        doc.set_field("age", Fv::U64(30)).unwrap();

        // 转换为 DocumentOwned
        let owned: DocumentOwned = doc.into();

        // 验证转换是否正确
        assert_eq!(owned.fields.len(), 3);

        // 转换回 Document
        let doc2 = Document::try_from_doc(schema.clone(), owned).unwrap();

        // 验证转换是否正确
        assert_eq!(doc2.id(), 99);
        assert_eq!(doc2.fields.len(), 3);
        assert_eq!(
            doc2.get_field("name").unwrap(),
            &Fv::Text("John Doe".to_string())
        );
        assert_eq!(doc2.get_field("age").unwrap(), &Fv::U64(30));
    }

    #[test]
    fn test_document_validation_errors() {
        let schema = Arc::new(TestUser::schema().unwrap());

        // 测试缺少必填字段
        let test_user_missing_required = serde_json::json!({
            "_id": 18,
            "name": "John Doe",
            // 缺少必填的 age 字段
            "active": true
        });

        let result = Document::try_from(schema.clone(), &test_user_missing_required);
        assert!(result.is_err());

        // 测试字段类型不匹配
        let test_user_wrong_type = serde_json::json!({
            "_id": "18",
            "name": "John Doe",
            "age": "thirty", // 应该是数字
            "active": true
        });

        let result = Document::try_from(schema.clone(), &test_user_wrong_type);
        assert!(result.is_err());
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, AndaDBSchema)]
    struct NumDoc {
        _id: u64,
        /// Signed counter; non-negative values drift to U64 on read-back.
        count: i64,
        /// f32 values drift to F64 on read-back.
        ratio: f32,
        maybe: Option<i64>,
        nums: Vec<i64>,
    }

    #[test]
    fn stored_documents_with_i64_and_f32_fields_read_back() {
        // Regression: untyped CBOR deserialization returns non-negative I64
        // values as U64 and f32 values as F64. Documents written with such
        // fields must still validate and decode after a storage round trip.
        let schema = Arc::new(NumDoc::schema().unwrap());
        let value = NumDoc {
            _id: 1,
            count: 5,
            ratio: 2.71,
            maybe: Some(7),
            nums: vec![0, 1, -2, i64::MAX],
        };

        let doc = Document::try_from(schema.clone(), &value).unwrap();
        let owned: DocumentOwned = doc.into();

        // Full storage chain: DocumentOwned -> CBOR bytes -> DocumentOwned.
        let mut bytes = Vec::new();
        cbor2::to_writer(&owned, &mut bytes).unwrap();
        let restored: DocumentOwned = cbor2::from_reader(bytes.as_slice()).unwrap();

        // Read-back shapes (non-negative I64 as U64, F32 as F64) are
        // normalized back into the canonical variants...
        let doc = Document::try_from_doc(schema.clone(), restored).unwrap();
        assert_eq!(doc.get_field("count").unwrap(), &Fv::I64(5));
        assert_eq!(doc.get_field("ratio").unwrap(), &Fv::F32(2.71));
        assert_eq!(doc.get_field("maybe").unwrap(), &Fv::I64(7));
        assert_eq!(
            doc.get_field("nums").unwrap(),
            &Fv::Array(vec![Fv::I64(0), Fv::I64(1), Fv::I64(-2), Fv::I64(i64::MAX)])
        );

        // ...and decode back into the original struct losslessly.
        let round: NumDoc = doc.try_into().unwrap();
        assert_eq!(round, value);

        // Negative values keep the canonical I64 shape.
        let negative = NumDoc {
            _id: 2,
            count: -5,
            ratio: -1.5,
            maybe: None,
            nums: vec![-1],
        };
        let doc = Document::try_from(schema.clone(), &negative).unwrap();
        let owned: DocumentOwned = doc.into();
        let mut bytes = Vec::new();
        cbor2::to_writer(&owned, &mut bytes).unwrap();
        let restored: DocumentOwned = cbor2::from_reader(bytes.as_slice()).unwrap();
        let round: NumDoc = Document::try_from_doc(schema, restored)
            .unwrap()
            .try_into()
            .unwrap();
        assert_eq!(round, negative);
    }

    #[test]
    fn stored_documents_with_f32_fields_read_back_through_json() {
        // Regression (#28): serde_json serializes an F32 with the f32
        // shortest-decimal form, so the JSON read-back F64 is not the exact
        // widening (2.71 vs 2.7100000381...). Such documents must still
        // validate, normalize, and decode.
        let schema = Arc::new(NumDoc::schema().unwrap());
        let value = NumDoc {
            _id: 1,
            count: 5,
            ratio: 2.71,
            maybe: Some(7),
            nums: vec![0, 1, -2],
        };

        let doc = Document::try_from(schema.clone(), &value).unwrap();
        let owned: DocumentOwned = doc.into();

        let json = serde_json::to_string(&owned).unwrap();
        let restored: DocumentOwned = serde_json::from_str(&json).unwrap();

        let doc = Document::try_from_doc(schema, restored).unwrap();
        assert_eq!(doc.get_field("ratio").unwrap(), &Fv::F32(2.71));
        assert_eq!(doc.get_field("count").unwrap(), &Fv::I64(5));
        let round: NumDoc = doc.try_into().unwrap();
        assert_eq!(round, value);
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, AndaDBSchema)]
    struct VectorDoc {
        _id: u64,
        embedding: Vector,
    }

    #[test]
    fn stored_vector_read_back_is_canonical_for_get_field() {
        let schema = Arc::new(VectorDoc::schema().unwrap());
        let value = VectorDoc {
            _id: 1,
            embedding: vector_from_f32(vec![1.5, -2.0, 0.25]),
        };

        let doc = Document::try_from(schema.clone(), &value).unwrap();
        let owned: DocumentOwned = doc.into();
        let mut bytes = Vec::new();
        cbor2::to_writer(&owned, &mut bytes).unwrap();
        let restored: DocumentOwned = cbor2::from_reader(bytes.as_slice()).unwrap();

        assert!(matches!(restored.fields.get(&1), Some(Fv::Array(_))));
        let doc = Document::try_from_doc(schema, restored).unwrap();
        assert_eq!(
            doc.get_field("embedding"),
            Some(&Fv::Vector(value.embedding.clone()))
        );
        let round: VectorDoc = doc.try_into().unwrap();
        assert_eq!(round, value);
    }

    #[test]
    fn set_field_normalizes_read_back_shapes() {
        let schema = Arc::new(NumDoc::schema().unwrap());
        let mut doc = Document::new(schema);
        doc.set_id(1);
        doc.set_field("count", Fv::U64(5)).unwrap();
        assert_eq!(doc.get_field("count").unwrap(), &Fv::I64(5));
        doc.set_field("ratio", Fv::F64(2.71)).unwrap();
        assert_eq!(doc.get_field("ratio").unwrap(), &Fv::F32(2.71));
        // Out-of-range U64 is still rejected, not silently truncated.
        assert!(
            doc.set_field("count", Fv::U64(i64::MAX as u64 + 1))
                .is_err()
        );
    }

    #[test]
    fn try_from_doc_drops_values_of_removed_schema_fields() {
        // A real schema upgrade that removed a field: the stored document
        // still carries a value under the retired idx (inside the allocation
        // watermark). It must be readable, with the stale value dropped.
        #[derive(Debug, Serialize, Deserialize, AndaDBSchema)]
        struct UserV1 {
            _id: u64,
            name: String,
            age: u64,
            bio: Option<String>,
        }

        #[derive(Debug, Serialize, Deserialize, AndaDBSchema)]
        struct UserV2 {
            _id: u64,
            name: String,
            age: u64,
        }

        let v1 = UserV1::schema().unwrap();
        let mut v2 = UserV2::schema().unwrap();
        v2.with_version(v1.version() + 1);
        v2.upgrade_with(&v1).unwrap();
        let retired_idx = 3usize; // `bio` in v1, removed in v2
        assert!(!v2.contains_idx(retired_idx));
        assert_eq!(v2.allocated_idx_end(), 4);
        let schema = Arc::new(v2);

        let mut fields = IndexedFieldValues::new();
        fields.insert(0, Fv::U64(7));
        fields.insert(1, Fv::Text("John".to_string()));
        fields.insert(2, Fv::U64(30));
        fields.insert(retired_idx, Fv::Text("stale".to_string()));

        let doc = Document::try_from_doc(
            schema.clone(),
            DocumentOwned {
                fields: fields.clone(),
            },
        )
        .unwrap();
        assert_eq!(doc.fields().len(), 3);
        assert!(!doc.fields().contains_key(&retired_idx));

        // set_doc mirrors the lenient behaviour.
        let mut doc2 = Document::new(schema.clone());
        doc2.set_doc(DocumentOwned {
            fields: fields.clone(),
        })
        .unwrap();
        assert_eq!(doc2.fields().len(), 3);
        assert!(!doc2.fields().contains_key(&retired_idx));

        // An index this schema lineage never allocated marks foreign or
        // corrupt data: it is rejected instead of being silently dropped.
        fields.insert(99, Fv::Text("foreign".to_string()));
        assert!(
            Document::try_from_doc(
                schema.clone(),
                DocumentOwned {
                    fields: fields.clone(),
                },
            )
            .is_err()
        );
        let mut doc3 = Document::new(schema);
        assert!(doc3.set_doc(DocumentOwned { fields }).is_err());
    }

    #[test]
    fn nested_structs_can_gain_an_optional_key_and_lose_a_key() {
        // Regression: a nested `FieldTyped` struct could never gain or lose a
        // field in either direction. `upgrade_with` rejected every FieldType
        // change, and a non-wildcard `Map` rejected any key it did not
        // declare -- so removing a nested field made every already-stored
        // document fail `try_from_doc`, i.e. the collection became unreadable.
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, FieldTyped)]
        struct NestedV1 {
            a: String,
        }

        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, FieldTyped)]
        struct NestedV2 {
            a: String,
            b: Option<String>,
        }

        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, AndaDBSchema)]
        struct DocV1 {
            _id: u64,
            nested: NestedV1,
        }

        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, AndaDBSchema)]
        struct DocV2 {
            _id: u64,
            nested: NestedV2,
        }

        // Forward: adding an optional nested key is a compatible upgrade.
        let v1 = DocV1::schema().unwrap();
        let mut v2 = DocV2::schema().unwrap();
        v2.with_version(v1.version() + 1);
        v2.upgrade_with(&v1).unwrap();

        // A document written under v1 still reads under v2: the new key is
        // simply absent, which an optional type accepts.
        let v1 = Arc::new(v1);
        let stored = Document::try_from(
            v1.clone(),
            &DocV1 {
                _id: 1,
                nested: NestedV1 { a: "x".to_string() },
            },
        )
        .unwrap();
        let v2 = Arc::new(v2);
        let read = Document::try_from_doc(v2.clone(), stored.clone().into()).unwrap();
        assert_eq!(
            read.clone().try_into::<DocV2>().unwrap(),
            DocV2 {
                _id: 1,
                nested: NestedV2 {
                    a: "x".to_string(),
                    b: None,
                },
            }
        );

        // Backward: removing a nested key is compatible too, and the stale
        // entry every stored document carries is dropped on read instead of
        // making the document unreadable.
        let mut back = DocV1::schema().unwrap();
        back.with_version(v2.version() + 1);
        back.upgrade_with(&v2).unwrap();
        let back = Arc::new(back);

        let stored_v2 = Document::try_from(
            v2.clone(),
            &DocV2 {
                _id: 2,
                nested: NestedV2 {
                    a: "y".to_string(),
                    b: Some("gone".to_string()),
                },
            },
        )
        .unwrap();
        let read = Document::try_from_doc(back.clone(), stored_v2.clone().into()).unwrap();
        assert_eq!(
            read.get_field("nested"),
            Some(&Fv::Map(BTreeMap::from([(
                "a".into(),
                Fv::Text("y".to_string())
            )])))
        );
        assert_eq!(
            read.try_into::<DocV1>().unwrap(),
            DocV1 {
                _id: 2,
                nested: NestedV1 { a: "y".to_string() },
            }
        );
        // `set_doc` takes the same path.
        let mut doc = Document::new(back);
        doc.set_doc(stored_v2.into()).unwrap();
        assert_eq!(
            doc.get_field("nested"),
            Some(&Fv::Map(BTreeMap::from([(
                "a".into(),
                Fv::Text("y".to_string())
            )])))
        );

        // Genuinely incompatible nested changes are still rejected: a new
        // *required* key, and a key whose type changed.
        //
        // These four types exist only so the derive macros can emit the schema
        // shapes compared below; the fields are consumed by the macro, never
        // read by the test itself.
        #[derive(Debug, FieldTyped)]
        #[allow(dead_code)]
        struct NestedRequired {
            a: String,
            b: String,
        }

        #[derive(Debug, FieldTyped)]
        #[allow(dead_code)]
        struct NestedRetyped {
            a: u64,
        }

        #[derive(Debug, AndaDBSchema)]
        #[allow(dead_code)]
        struct DocRequired {
            _id: u64,
            nested: NestedRequired,
        }

        #[derive(Debug, AndaDBSchema)]
        #[allow(dead_code)]
        struct DocRetyped {
            _id: u64,
            nested: NestedRetyped,
        }

        let mut bad = DocRequired::schema().unwrap();
        bad.with_version(v1.version() + 1);
        let err = bad.upgrade_with(&v1).unwrap_err();
        assert!(
            err.to_string().contains("incompatible type changes"),
            "{err}"
        );

        let mut bad = DocRetyped::schema().unwrap();
        bad.with_version(v1.version() + 1);
        let err = bad.upgrade_with(&v1).unwrap_err();
        assert!(
            err.to_string().contains("incompatible type changes"),
            "{err}"
        );

        // Writing an undeclared nested key is still an error -- exactly like
        // an undeclared *top-level* field name. Evolution needs a version
        // bump, it is not silently absorbed at write time.
        let err = Document::try_from(
            v1,
            &DocV2 {
                _id: 3,
                nested: NestedV2 {
                    a: "z".to_string(),
                    b: Some("new".to_string()),
                },
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("invalid map key"), "{err}");
    }

    #[test]
    fn try_from_enforces_the_complexity_budget() {
        #[derive(Debug, Serialize, Deserialize, AndaDBSchema)]
        struct JsonDoc {
            _id: u64,
            data: Option<serde_json::Value>,
        }

        let schema = Arc::new(JsonDoc::schema().unwrap());

        // A JSON value nested beyond FieldValueBudget::max_depth (64) must be
        // rejected at write time; it could not be read back if stored.
        let mut deep = serde_json::json!(true);
        for _ in 0..100 {
            deep = serde_json::json!([deep]);
        }
        let err = Document::try_from(
            schema.clone(),
            &JsonDoc {
                _id: 1,
                data: Some(deep),
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("maximum depth"), "err: {err}");

        // A shallow value is unaffected.
        assert!(
            Document::try_from(
                schema,
                &JsonDoc {
                    _id: 1,
                    data: Some(serde_json::json!({"a": [1, 2, 3]})),
                },
            )
            .is_ok()
        );
    }

    #[test]
    fn absent_fields_are_omitted_so_serde_defaults_apply() {
        // Regression: absent fields used to serialize as an explicit `null`,
        // which `#[serde(default)]` never fills in (it only covers *missing*
        // keys). A value whose serialization skips a field could therefore be
        // written but never read back into its own type.
        #[derive(Debug, Serialize, Deserialize, PartialEq, AndaDBSchema)]
        struct TagDoc {
            _id: u64,
            /// Tags, skipped entirely when empty.
            #[field_type = "Option<Array<Text>>"]
            #[serde(default, skip_serializing_if = "Vec::is_empty")]
            tags: Vec<String>,
        }

        let schema = Arc::new(TagDoc::schema().unwrap());
        let value = TagDoc {
            _id: 1,
            tags: Vec::new(),
        };
        let doc = Document::try_from(schema.clone(), &value).unwrap();
        assert_eq!(doc.fields().len(), 1);

        let round: TagDoc = doc.try_into().unwrap();
        assert_eq!(round, value);

        // A field that *is* present with a null value still serializes as
        // `null`; only genuinely absent fields disappear.
        let schema = Arc::new(TestUser::schema().unwrap());
        let mut doc = Document::new(schema);
        doc.set_id(7);
        doc.set_field("name", Fv::Text("Ada".to_string())).unwrap();
        doc.set_field("age", Fv::U64(42)).unwrap();
        doc.set_field("active", Fv::Null).unwrap();

        let named: BTreeMap<String, Fv> = doc.clone().try_into().unwrap();
        assert_eq!(named.get("active"), Some(&Fv::Null));
        assert!(!named.contains_key("tags"));
        assert_eq!(named.len(), 4);

        let user: TestUser = doc.try_into().unwrap();
        assert_eq!(user.active, None);
        assert_eq!(user.tags, None);
    }

    #[test]
    fn test_document_try_from_rejects_duplicate_field_keys() {
        struct DuplicateKeys;

        impl Serialize for DuplicateKeys {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(Some(4))?;
                map.serialize_entry("_id", &1u64)?;
                map.serialize_entry("name", "a")?;
                map.serialize_entry("age", &1u64)?;
                map.serialize_entry("name", "b")?;
                map.end()
            }
        }

        let schema = Arc::new(TestUser::schema().unwrap());
        let err = Document::try_from(schema, &DuplicateKeys).unwrap_err();
        assert!(err.to_string().contains("duplicate field"));
    }

    #[test]
    fn document_error_paths_and_optional_defaults_are_exercised() {
        struct FailingSerialize;

        impl Serialize for FailingSerialize {
            fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                Err(serde::ser::Error::custom("boom"))
            }
        }

        let schema = Arc::new(TestUser::schema().unwrap());

        let err = Document::try_from(schema.clone(), &FailingSerialize).unwrap_err();
        assert!(err.to_string().contains("failed to serialize document"));

        let numeric_key = BTreeMap::from([(1u64, 2u64)]);
        let err = Document::try_from(schema.clone(), &numeric_key).unwrap_err();
        assert!(err.to_string().contains("expected CBOR text value"));

        let tuple_doc = ("not", "a map");
        let err = Document::try_from(schema.clone(), &tuple_doc).unwrap_err();
        assert!(err.to_string().contains("expected CBOR map value"));

        let mut doc = Document::new(schema.clone());
        doc.set_id(7);
        doc.set_field("age", Fv::U64(42)).unwrap();
        assert!(doc.get_field_or_err("name").is_err());
        assert!(doc.get_field_or_err("missing").is_err());
        assert!(doc.get_field_as::<String>("name").is_err());
        assert!(doc.get_field_as::<String>("missing").is_err());
        assert_eq!(doc.remove_field("missing"), None);
        assert!(doc.clone().try_into::<TestUser>().is_err());

        doc.set_field("name", Fv::Text("Ada".to_string())).unwrap();
        let user = doc.try_into::<TestUser>().unwrap();
        assert_eq!(user._id, 7);
        assert_eq!(user.name, "Ada");
        assert_eq!(user.age, 42);
        assert_eq!(user.active, None);
        assert_eq!(user.tags, None);
        assert_eq!(user.meta, None);
        assert_eq!(user.picture, None);
    }
}
