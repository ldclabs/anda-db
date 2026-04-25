//! [`Document`] and [`DocumentOwned`] — the runtime and on-disk
//! representations of an Anda DB document.
use ciborium::Value;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::sync::Arc;

use super::{Cbor, Fv, IndexedFieldValues, Schema, SchemaError};

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
    /// # Arguments
    /// * `schema` - The schema to validate against
    /// * `doc` - The DocumentOwned to convert
    ///
    /// # Returns
    /// * `Result<Self, SchemaError>` - The validated Document or an error
    pub fn try_from_doc(schema: Arc<Schema>, doc: DocumentOwned) -> Result<Self, SchemaError> {
        schema.validate(&doc.fields)?;

        Ok(Self {
            fields: doc.fields,
            schema,
        })
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

        let mut doc_owned = DocumentOwned::default();
        for (k, v) in doc {
            let k = k.into_text().map_err(|err| {
                SchemaError::Validation(format!(
                    "invalid document field key, expected CBOR text value, got {err:?}"
                ))
            })?;

            let field = schema.get_field_or_err(&k)?;
            let value = field.extract(v, false)?;
            doc_owned.fields.insert(field.idx(), value);
        }

        Self::try_from_doc(schema, doc_owned)
    }

    /// Deserializes the document into the specified type.
    ///
    /// # Returns
    /// * `Result<T, SchemaError>` - The deserialized value or an error
    ///
    /// # Type Parameters
    /// * `T` - The type to deserialize the document to
    pub fn try_into<T>(mut self) -> Result<T, SchemaError>
    where
        T: DeserializeOwned,
    {
        let mut doc: Vec<(Cbor, Cbor)> = Vec::with_capacity(self.schema.len());
        for field in self.schema.iter() {
            if let Some(value) = self.fields.remove(&field.idx()) {
                doc.push((field.name().into(), value.into()));
            } else if field.required() {
                return Err(SchemaError::Validation(format!(
                    "field {:?} is required",
                    field.name()
                )));
            } else {
                doc.push((field.name().into(), Cbor::Null));
            }
        }

        Cbor::Map(doc)
            .deserialized()
            .map_err(|err| SchemaError::Serialization(format!("Failed to deserialize: {err}")))
    }

    /// Gets the document's unique identifier.
    ///
    /// # Returns
    /// A reference to the document's ID
    pub fn id(&self) -> DocumentId {
        match self.fields.get(&0) {
            Some(Fv::U64(id)) => *id,
            _ => 0,
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
    /// # Arguments
    /// * `name` - The name of the field to set
    /// * `value` - The value to store
    ///
    /// # Returns
    /// * `Result<(), SchemaError>` - Success or an error
    pub fn set_field(&mut self, name: &str, value: Fv) -> Result<&mut Self, SchemaError> {
        if let Some(field) = self.schema.get_field(name) {
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
                return value.to_owned().deserialized();
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
    /// # Arguments
    /// * `doc` - The DocumentOwned containing the new values
    ///
    /// # Returns
    /// * `Result<(), SchemaError>` - Success or an error
    pub fn set_doc(&mut self, doc: DocumentOwned) -> Result<(), SchemaError> {
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
    use crate::{AndaDBSchema, FieldEntry, FieldKey, FieldType, Fv, Resource};
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
}
