use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

use crate::{FieldEntry, FieldType, IndexedFieldValues, Resource, SchemaError};

/// Schema represents Anda DB document schema definition.
/// It contains a collection of fields and their indexes.
#[derive(Debug, Clone)]
pub struct Schema {
    /// Set of field indexes for quick lookup
    idx: BTreeSet<usize>,
    /// Map of field names to field entries
    fields: BTreeMap<String, FieldEntry>,
}

impl Schema {
    /// The key name for the ID field. it is a special u64 field used as an internal unique identifier in a collection. It is always present in the schema with idx 0.
    pub const ID_KEY: &str = "_id";

    /// Creates a new SchemaBuilder instance.
    ///
    /// # Returns
    /// A new SchemaBuilder with default settings.
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::new()
    }

    /// Returns the number of fields in the schema.
    /// This includes the "_id" field and any other fields defined in the schema.
    ///
    /// # Returns
    /// The number of fields.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Checks if the schema has no fields.
    ///
    /// # Returns
    /// `true` if the schema has no fields, `false` otherwise.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Gets a field by name.
    ///
    /// # Arguments
    /// * `name` - The name of the field to get.
    ///
    /// # Returns
    /// Some(&FieldEntry) if the field exists, None otherwise.
    pub fn get_field(&self, name: &str) -> Option<&FieldEntry> {
        self.fields.get(name)
    }

    /// Gets a field by name or returns an error if it doesn't exist.
    ///
    /// # Arguments
    /// * `name` - The name of the field to get.
    ///
    /// # Returns
    /// Ok(&FieldEntry) if the field exists, Err(SchemaError) otherwise.
    pub fn get_field_or_err(&self, name: &str) -> Result<&FieldEntry, SchemaError> {
        self.fields
            .get(name)
            .ok_or_else(|| SchemaError::Validation(format!("field {name:?} not found in schema")))
    }

    /// Returns an iterator over all fields in the schema.
    ///
    /// # Returns
    /// An iterator yielding references to FieldEntry.
    pub fn iter(&self) -> impl Iterator<Item = &FieldEntry> {
        self.fields.values()
    }

    /// Validates a set of field values against this schema.
    ///
    /// # Arguments
    /// * `values` - The field values to validate.
    ///
    /// # Returns
    /// Ok(()) if validation succeeds, Err(SchemaError) otherwise.
    ///
    /// # Errors
    /// Returns an error if:
    /// - A field index in values doesn't exist in the schema
    /// - A required field is missing
    /// - A field value doesn't match the field type
    pub fn validate(&self, values: &IndexedFieldValues) -> Result<(), SchemaError> {
        // Validate that all field indexes in values exist in the schema
        for idx in values.keys() {
            if !self.idx.contains(idx) {
                return Err(SchemaError::Validation(format!(
                    "field index {idx:?} not found in schema"
                )));
            }
        }

        // Validate each field's value and check for required fields
        for field in self.fields.values() {
            if let Some(value) = values.get(&field.idx()) {
                field.validate(value)?;
            } else if field.required() {
                return Err(SchemaError::Validation(format!(
                    "field {:?} is required",
                    field.name()
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
struct SchemaRef<'a> {
    fields: Vec<&'a FieldEntry>,
}

#[derive(Debug, Clone, Deserialize)]
struct SchemaOwned {
    fields: Vec<FieldEntry>,
}

impl Serialize for Schema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let val = SchemaRef {
            fields: self.fields.values().collect(),
        };
        val.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Schema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let val = SchemaOwned::deserialize(deserializer)?;

        // Validate invariants here because `FieldEntry` derives `Deserialize` and would
        // otherwise allow invalid names / duplicate indexes.
        let mut idx = BTreeSet::<usize>::new();
        let mut fields = BTreeMap::<String, FieldEntry>::new();

        for f in val.fields.into_iter() {
            crate::validate_field_name(f.name()).map_err(serde::de::Error::custom)?;

            if f.idx() > u16::MAX as usize {
                return Err(serde::de::Error::custom(format!(
                    "field index {:?} exceeds u16::MAX",
                    f.idx()
                )));
            }

            if !idx.insert(f.idx()) {
                return Err(serde::de::Error::custom(format!(
                    "duplicate field index {:?}",
                    f.idx()
                )));
            }

            let name = f.name().to_string();
            if fields.insert(name.clone(), f).is_some() {
                return Err(serde::de::Error::custom(format!(
                    "duplicate field name {name:?}"
                )));
            }
        }

        let id = fields.get(Schema::ID_KEY).ok_or_else(|| {
            serde::de::Error::custom(format!(
                "schema is missing required field {:?}",
                Schema::ID_KEY
            ))
        })?;

        if id.idx() != 0 {
            return Err(serde::de::Error::custom(format!(
                "field {:?} must have index 0, got {:?}",
                Schema::ID_KEY,
                id.idx()
            )));
        }

        if id.r#type() != &FieldType::U64 {
            return Err(serde::de::Error::custom(format!(
                "field {:?} must have type U64, got {:?}",
                Schema::ID_KEY,
                id.r#type()
            )));
        }

        if !id.unique() {
            return Err(serde::de::Error::custom(format!(
                "field {:?} must be unique",
                Schema::ID_KEY
            )));
        }

        Ok(Schema { idx, fields })
    }
}

/// SchemaBuilder is used to construct a Schema instance.
/// It provides methods to add fields and build the final schema.
#[derive(Clone, Debug)]
pub struct SchemaBuilder {
    idx: usize,
    fields: BTreeMap<String, FieldEntry>,
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaBuilder {
    /// Creates a new SchemaBuilder instance.
    ///
    /// # Returns
    /// A new SchemaBuilder with default settings.
    pub fn new() -> SchemaBuilder {
        SchemaBuilder {
            idx: 0,
            fields: BTreeMap::from([(
                Schema::ID_KEY.to_string(),
                FieldEntry::new(Schema::ID_KEY.to_string(), FieldType::U64)
                    .unwrap()
                    .with_unique()
                    .with_idx(0)
                    .with_description(format!(
                        "{:?} is a u64 field, used as an internal unique identifier",
                        Schema::ID_KEY
                    )),
            )]),
        }
    }

    pub fn with_resource(&mut self, field: &str, required: bool) -> Result<&mut Self, SchemaError> {
        let ft = Resource::field_type();
        let ft = if required {
            ft
        } else {
            FieldType::Option(Box::new(ft))
        };
        let entry = FieldEntry::new(field.to_string(), ft)?.with_description(format!(
            "{field:?} is a field of type Resource, used to store resources"
        ));

        self.add_field(entry)
    }

    /// Adds a field to the schema.
    ///
    /// # Arguments
    /// * `entry` - The field entry to add.
    ///
    /// # Returns
    /// Ok(()) if the field was added successfully, Err(SchemaError) otherwise.
    ///
    /// # Errors
    /// Returns an error if:
    /// - A field with the same name already exists
    /// - The maximum number of fields has been reached
    pub fn add_field(&mut self, entry: FieldEntry) -> Result<&mut Self, SchemaError> {
        if self.fields.contains_key(entry.name()) {
            return Err(SchemaError::Schema(format!(
                "Field {:?} already exists in schema",
                entry.name()
            )));
        }

        self.idx += 1;
        if self.idx > u16::MAX as usize {
            return Err(SchemaError::Schema(
                "Schema has reached the maximum number of fields".to_string(),
            ));
        }

        self.fields
            .insert(entry.name().to_string(), entry.with_idx(self.idx));
        Ok(self)
    }

    /// Builds the final Schema from this builder.
    ///
    /// # Returns
    /// Ok(Schema) if the schema is valid, Err(SchemaError) otherwise.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The schema has no fields
    /// - The schema has too many fields
    pub fn build(self) -> Result<Schema, SchemaError> {
        // Field index 0 is reserved for `_id`, so maximum field count is `u16::MAX + 1`.
        const MAX_FIELDS: usize = u16::MAX as usize + 1;
        if self.fields.len() > MAX_FIELDS {
            return Err(SchemaError::Schema(
                "Schema has reached the maximum number of fields".to_string(),
            ));
        }

        Ok(Schema {
            idx: self.fields.values().map(|f| f.idx()).collect(),
            fields: self.fields,
        })
    }
}

impl PartialEq for Schema {
    /// Compares two Schema instances for equality.
    /// Two schemas are equal if they have the same fields.
    fn eq(&self, other: &Schema) -> bool {
        self.fields == other.fields
    }
}

impl Eq for Schema {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Fe, Ft, Fv};
    use serde_json::json;

    #[test]
    fn test_schema_builder() {
        let mut builder = SchemaBuilder::new();
        assert_eq!(builder.fields.len(), 1); // 只有 ID 字段

        // 测试添加 ID 字段
        let id_field = Fe::new("_id".to_string(), Ft::U64).unwrap();
        // ID 字段已经存在，添加失败
        assert!(builder.add_field(id_field).is_err());

        // 测试添加普通字段
        let name_field = Fe::new("name".to_string(), Ft::Text).unwrap();
        assert!(builder.add_field(name_field).is_ok());

        let age_field = Fe::new("age".to_string(), Ft::Option(Box::new(Ft::U64))).unwrap();
        assert!(builder.add_field(age_field).is_ok());

        // 测试添加重复字段
        let duplicate_field = Fe::new("name".to_string(), Ft::Text).unwrap();
        assert!(builder.add_field(duplicate_field).is_err());

        // 构建 Schema
        let schema = builder.build().unwrap();

        // 验证 Schema 字段数量
        assert_eq!(schema.len(), 3);
        assert!(!schema.is_empty());

        // 验证字段索引
        assert!(schema.idx.contains(&0)); // _id
        assert!(schema.idx.contains(&1)); // name
        assert!(schema.idx.contains(&2)); // age

        // 验证获取字段
        let id_field = schema.get_field(Schema::ID_KEY).unwrap();
        assert_eq!(id_field.name(), Schema::ID_KEY);
        assert_eq!(id_field.idx(), 0);
        assert!(id_field.required());
        assert!(id_field.unique());

        let name_field = schema.get_field("name").unwrap();
        assert_eq!(name_field.name(), "name");
        assert_eq!(name_field.idx(), 1);
        assert!(name_field.required());

        let age_field = schema.get_field("age").unwrap();
        assert_eq!(age_field.name(), "age");
        assert_eq!(age_field.idx(), 2);
        assert!(!age_field.required());

        // 测试不存在的字段
        assert!(schema.get_field("unknown").is_none());
    }

    #[test]
    fn test_schema_validation() {
        let mut builder = SchemaBuilder::new();

        // 添加字段
        let name_field = Fe::new("name".to_string(), Ft::Text).unwrap();
        builder.add_field(name_field).unwrap();

        let age_field = Fe::new("age".to_string(), Ft::U64).unwrap();
        builder.add_field(age_field).unwrap();

        let schema = builder.build().unwrap();

        // 创建有效的字段值
        let mut valid_values = IndexedFieldValues::new();
        valid_values.insert(0, Fv::U64(99));
        valid_values.insert(1, Fv::Text("John".to_string()));
        valid_values.insert(2, Fv::U64(30));

        // 验证有效值
        assert!(schema.validate(&valid_values).is_ok());
        // 验证无效值
        valid_values.insert(0, Fv::I64(99));
        assert!(schema.validate(&valid_values).is_err());

        // 缺少必填字段
        let mut missing_required = IndexedFieldValues::new();
        missing_required.insert(0, Fv::Text("user1".to_string()));
        missing_required.insert(1, Fv::Text("John".to_string()));
        // 缺少 age 字段
        assert!(schema.validate(&missing_required).is_err());

        // 无效的字段索引
        let mut invalid_index = IndexedFieldValues::new();
        invalid_index.insert(0, Fv::U64(99));
        invalid_index.insert(1, Fv::Text("John".to_string()));
        invalid_index.insert(2, Fv::U64(30));
        invalid_index.insert(99, Fv::Text("Invalid".to_string())); // 无效索引
        assert!(schema.validate(&invalid_index).is_err());

        // 字段类型不匹配
        let mut invalid_type = IndexedFieldValues::new();
        invalid_type.insert(0, Fv::U64(99));
        invalid_type.insert(1, Fv::Text("John".to_string()));
        invalid_type.insert(2, Fv::Text("30".to_string())); // 应该是 Integer
        assert!(schema.validate(&invalid_type).is_err());
    }

    #[test]
    fn test_schema_builder_limits() {
        // 测试空 Schema
        let empty_builder = SchemaBuilder::new();
        assert!(empty_builder.build().is_ok());

        // 测试最大字段数限制
        let mut builder = SchemaBuilder::new();

        // 设置 idx 接近 u16::MAX
        builder.idx = u16::MAX as usize - 1;
        let test_field = Fe::new("test".to_string(), Ft::Text).unwrap();
        assert!(builder.add_field(test_field).is_ok());

        // 添加超过限制的字段
        let overflow_field = Fe::new("overflow".to_string(), Ft::Text).unwrap();
        assert!(builder.add_field(overflow_field).is_err());
    }

    #[test]
    fn test_schema_equality() {
        let mut builder1 = SchemaBuilder::new();
        let name_field1 = Fe::new("name".to_string(), Ft::Text).unwrap();
        builder1.add_field(name_field1).unwrap();
        let schema1 = builder1.build().unwrap();

        let mut builder2 = SchemaBuilder::new();
        let name_field2 = Fe::new("name".to_string(), Ft::Text).unwrap();
        builder2.add_field(name_field2).unwrap();
        let schema2 = builder2.build().unwrap();

        // 相同结构的 Schema 应该相等
        assert_eq!(schema1, schema2);

        // 不同结构的 Schema
        let mut builder3 = SchemaBuilder::new();
        let age_field3 = Fe::new("name".to_string(), Ft::U64).unwrap();
        builder3.add_field(age_field3).unwrap();
        let schema3 = builder3.build().unwrap();

        assert_ne!(schema1, schema3);
    }

    #[test]
    fn test_schema_iter() {
        let mut builder = SchemaBuilder::new();
        let name_field = Fe::new("name".to_string(), Ft::Text).unwrap();
        builder.add_field(name_field).unwrap();
        let schema = builder.build().unwrap();

        let fields: Vec<&FieldEntry> = schema.iter().collect();
        assert_eq!(fields.len(), 2);

        // 验证迭代器返回的字段
        let field_names: Vec<&str> = fields.iter().map(|f| f.name()).collect();
        println!("Field names: {field_names:?}");
        assert!(field_names.contains(&"_id"));
        assert!(field_names.contains(&"name"));
    }

    #[test]
    fn test_schema_serde_roundtrip_json() {
        let mut builder = SchemaBuilder::new();
        builder
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        builder
            .add_field(Fe::new("age".to_string(), Ft::Option(Box::new(Ft::U64))).unwrap())
            .unwrap();

        let schema = builder.build().unwrap();
        let v = serde_json::to_value(&schema).unwrap();
        let schema2: Schema = serde_json::from_value(v).unwrap();
        assert_eq!(schema, schema2);
    }

    #[test]
    fn test_schema_deserialize_rejects_invalid_invariants() {
        let mut builder = SchemaBuilder::new();
        builder
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        let schema = builder.build().unwrap();

        // Start from a valid JSON representation, then mutate it.
        let _v = serde_json::to_value(&schema).unwrap();

        // 1) Missing _id
        let mut missing_id = serde_json::to_value(&schema).unwrap();
        let fields_missing = missing_id
            .get_mut("fields")
            .and_then(|x| x.as_array_mut())
            .unwrap();
        fields_missing.retain(|f| f.get("n") != Some(&json!("_id")));
        assert!(serde_json::from_value::<Schema>(missing_id).is_err());

        // 2) Invalid field name
        let mut invalid_name = serde_json::to_value(&schema).unwrap();
        let fields2 = invalid_name
            .get_mut("fields")
            .and_then(|x| x.as_array_mut())
            .unwrap();
        if let Some(name_field) = fields2
            .iter_mut()
            .find(|f| f.get("n") == Some(&json!("name")))
        {
            name_field["n"] = json!("Name");
        }
        assert!(serde_json::from_value::<Schema>(invalid_name).is_err());

        // 3) Duplicate idx (make `name` use idx 0)
        let mut dup_idx = serde_json::to_value(&schema).unwrap();
        let fields3 = dup_idx
            .get_mut("fields")
            .and_then(|x| x.as_array_mut())
            .unwrap();
        if let Some(name_field) = fields3
            .iter_mut()
            .find(|f| f.get("n") == Some(&json!("name")))
        {
            name_field["i"] = json!(0);
        }
        assert!(serde_json::from_value::<Schema>(dup_idx).is_err());

        // 4) _id wrong type
        let mut id_wrong_type = serde_json::to_value(&schema).unwrap();
        let fields4 = id_wrong_type
            .get_mut("fields")
            .and_then(|x| x.as_array_mut())
            .unwrap();
        if let Some(id_field) = fields4
            .iter_mut()
            .find(|f| f.get("n") == Some(&json!("_id")))
        {
            id_field["t"] = json!("Text");
        }
        assert!(serde_json::from_value::<Schema>(id_wrong_type).is_err());
    }
}
