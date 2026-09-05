//! [`Schema`] and [`SchemaBuilder`] — the document layout description
//! consumed by all higher-level Anda DB collections.
//!
//! A [`Schema`] is an ordered, versioned collection of [`FieldEntry`]
//! values. The mandatory `_id: U64` field is reserved as the document
//! primary key and always carries `idx = 0`. Schemas are forward-compatible
//! and can be migrated with [`Schema::upgrade_with`].
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

use crate::{FieldEntry, FieldType, IndexedFieldValues, Resource, SchemaError};

/// Document schema definition for Anda DB.
///
/// A `Schema` describes:
///
/// - which fields a document may contain (by name and type),
/// - their stable on-disk indexes (`FieldEntry::idx`), and
/// - a monotonic `version` used to coordinate schema migrations.
///
/// Every schema implicitly contains the reserved `_id` field of type
/// [`FieldType::U64`] with `idx == 0` (see [`Schema::ID_KEY`]). It is added
/// automatically by [`SchemaBuilder::new`].
///
/// `Schema` is `Serialize` / `Deserialize` and round-trips through both
/// JSON and CBOR. Deserialization re-validates every invariant (`_id`
/// presence, unique field names and indexes, valid field name characters,
/// well-formed field types, `idx <= u16::MAX`) so it is safe to load
/// schemas coming from untrusted storage.
///
/// Equality compares the *declaration* — the fields (name, type,
/// uniqueness, idx) and the version — but not the allocation watermark,
/// which is bookkeeping about the lineage's history rather than part of
/// what the schema declares.
#[derive(Debug, Clone)]
pub struct Schema {
    /// Set of field indexes for O(log n) membership tests during
    /// validation.
    idx: BTreeSet<usize>,
    /// Field definitions keyed by field name for fast name-based lookup.
    fields: BTreeMap<String, FieldEntry>,
    /// Monotonic schema version. A higher value indicates a newer schema.
    /// Used by [`Schema::upgrade_with`] to authorize migrations.
    version: u64,
    /// High-water mark (exclusive) of every field index this schema lineage
    /// has ever allocated, persisted across upgrades. It serves two
    /// purposes: [`Schema::upgrade_with`] allocates new indexes from here so
    /// a removed top field's index can never be reused, and
    /// [`Schema::allocated_idx_end`] lets document decoding distinguish
    /// stale values of removed fields (silently droppable) from foreign or
    /// corrupt indexes (an error).
    ///
    /// It is always persisted and only ever grows, `legacy_lineage` or not:
    /// a watermark that went backwards would re-allocate the index of a
    /// removed field to a new one.
    next_idx: usize,
    /// `true` for a lineage first persisted by a version that had no
    /// watermark (no `next_idx` on the wire). Its history of removed indexes
    /// is unknown, so `next_idx` is only a *cursor* there — it says where
    /// allocation continues, not which indexes were never allocated — and
    /// document decoding keeps the lenient pre-watermark behaviour: every
    /// undeclared index is dropped on read, never rejected as foreign. The
    /// status is sticky: it survives [`Schema::upgrade_with`] and
    /// re-serialization. See [`Schema::has_allocation_watermark`].
    legacy_lineage: bool,
}

impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        // `idx` is derived from `fields`; `next_idx` is deliberately left
        // out (see the type-level docs).
        self.version == other.version && self.fields == other.fields
    }
}

impl Eq for Schema {}

impl Schema {
    /// The key name for the ID field. it is a special u64 field used as an internal unique identifier in a collection. It is always present in the schema with idx 0.
    pub const ID_KEY: &str = "_id";

    /// Returns the schema version.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Sets the schema version.
    pub fn with_version(&mut self, version: u64) -> &mut Self {
        self.version = version;
        self
    }

    /// Returns `true` if `self` has a higher version than `other`,
    /// indicating that a schema migration is needed.
    pub fn needs_upgrade(&self, other: &Schema) -> bool {
        self.version > other.version
    }

    /// Upgrades this schema using field indexes from an older persisted schema.
    ///
    /// The new schema (self) is typically built from program code where field indexes
    /// are assigned sequentially. The old schema comes from persistent storage with
    /// fixed indexes. This method ensures index consistency by:
    ///
    /// 1. For fields present in both schemas: inherits the `idx` from the old schema,
    ///    and verifies the field type has not changed incompatibly (see
    ///    [`FieldType::is_compatible_upgrade_of`] — a type may become optional
    ///    and a nested struct may gain an optional key or lose a key, nothing
    ///    else changes).
    /// 2. For new fields (in self but not in old): assigns fresh indexes starting
    ///    from the old schema's allocation watermark
    ///    ([`Schema::allocated_idx_end`]), so no index of a removed field is
    ///    ever reused.
    /// 3. For removed fields (in old but not in self): their indexes are simply not
    ///    reused, preventing data corruption.
    ///
    /// # Arguments
    /// * `old` - The old schema loaded from persistent storage.
    ///
    /// # Errors
    /// - If `self.version` is not greater than `old.version`.
    /// - If a field that exists in both schemas changed to an incompatible type.
    /// - If a field that exists only in the new schema is required.
    /// - If assigning indexes to new fields would exceed `u16::MAX`.
    ///
    /// On error `self` is left unchanged.
    pub fn upgrade_with(&mut self, old: &Schema) -> Result<(), SchemaError> {
        if !self.needs_upgrade(old) {
            return Err(SchemaError::Schema(format!(
                "new schema version {} must be greater than old version {}",
                self.version, old.version
            )));
        }

        // Allocate new indexes from the old schema's high-water mark: this
        // covers indexes of removed fields too (including a removed *top*
        // field, which `max(idx) + 1` alone would reuse). The first pass
        // only validates, so that `self` stays untouched when any field is
        // rejected.
        let mut next_idx = old.allocated_idx_end();
        for (name, field) in self.fields.iter() {
            if let Some(old_field) = old.fields.get(name) {
                // Field exists in both: the type may only change in ways that
                // leave every already-stored document readable — a nested
                // struct gaining an optional key or losing one. Nothing here
                // rewrites documents, so anything else is rejected.
                if !field.r#type().is_compatible_upgrade_of(old_field.r#type()) {
                    return Err(SchemaError::Schema(format!(
                        "field {name:?} type changed from {:?} to {:?}, incompatible type changes are not allowed",
                        old_field.r#type(),
                        field.r#type()
                    )));
                }

                // The unique flag must not change either: indexes derive their
                // duplicate policy from it at creation time and existing data
                // is never re-validated, so a silent flip would leave the
                // collection and its indexes inconsistent.
                if field.unique() != old_field.unique() {
                    return Err(SchemaError::Schema(format!(
                        "field {name:?} unique flag changed from {} to {}, unique changes are not allowed",
                        old_field.unique(),
                        field.unique()
                    )));
                }
            } else {
                if field.required() {
                    return Err(SchemaError::Schema(format!(
                        "new field {name:?} must be optional when upgrading schema"
                    )));
                }

                if next_idx > u16::MAX as usize {
                    return Err(SchemaError::Schema(
                        "Schema has reached the maximum number of fields".to_string(),
                    ));
                }

                next_idx += 1;
            }
        }

        // Second pass: apply the index assignments (infallible).
        let mut next_idx = old.allocated_idx_end();
        for (name, field) in self.fields.iter_mut() {
            if let Some(old_field) = old.fields.get(name) {
                // Field exists in both: inherit the persisted idx.
                field.set_idx(old_field.idx());
            } else {
                // New field: assign the next available index. Removed fields keep
                // their old indexes unallocated so they are never reused.
                field.set_idx(next_idx);
                next_idx += 1;
            }
        }

        // Rebuild the idx set from the updated fields and carry the
        // allocation watermark forward. The watermark advances for a legacy
        // lineage too — otherwise removing the highest field would let the
        // next upgrade hand its index to a new field — while the *leniency*
        // of a legacy lineage stays: its unknown history means an index above
        // the cursor cannot be called foreign, so the stale values of fields
        // removed before the upgrade must keep being dropped, not rejected.
        self.idx = self.fields.values().map(|f| f.idx()).collect();
        self.next_idx = next_idx;
        self.legacy_lineage = old.legacy_lineage;
        Ok(())
    }

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

    /// Returns `true` if the schema declares a field with this stable index.
    ///
    /// Useful to detect stale values written under an older schema whose
    /// field has since been removed by [`Schema::upgrade_with`] (removed
    /// indexes are never reused).
    pub fn contains_idx(&self, idx: usize) -> bool {
        self.idx.contains(&idx)
    }

    /// Returns the exclusive upper bound of every field index this schema
    /// lineage has ever allocated.
    ///
    /// An undeclared index below this bound belonged to a since-removed
    /// field (its stale values are droppable); an index at or above it was
    /// never allocated and marks foreign or corrupt data. For a legacy
    /// lineage (see [`Schema::has_allocation_watermark`]) it is only an
    /// allocation cursor — new fields are still assigned from here, so a
    /// removed index is never reused, but document decoding does not treat
    /// higher indexes as foreign.
    pub fn allocated_idx_end(&self) -> usize {
        self.next_idx
            .max(self.idx.last().map_or(0, |last| last + 1))
    }

    /// Returns `true` when this schema lineage carries an allocation
    /// watermark, i.e. it was created — or upgraded from a schema created —
    /// by a version that records one.
    ///
    /// A legacy lineage, persisted before the watermark existed, cannot tell
    /// the index of a field it once removed from foreign data. Document
    /// decoding therefore keeps the lenient pre-watermark behaviour for it
    /// (every undeclared index is dropped) instead of rejecting indexes at
    /// or above [`Schema::allocated_idx_end`]. Only the *leniency* is
    /// legacy: [`Schema::allocated_idx_end`] still advances monotonically
    /// for such a lineage, so a removed field's index is never handed to a
    /// new field. The status is sticky: it survives
    /// [`Schema::upgrade_with`] and re-serialization.
    pub fn has_allocation_watermark(&self) -> bool {
        !self.legacy_lineage
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
    version: u64,
    next_idx: usize,
    /// Only written for a legacy lineage, so the encoding of every schema
    /// created by this version is byte-identical to before the flag existed.
    #[serde(skip_serializing_if = "core::ops::Not::not")]
    legacy: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct SchemaOwned {
    fields: Vec<FieldEntry>,
    #[serde(default)]
    version: u64,
    /// Missing in schemas persisted by versions that had no watermark, which
    /// is what marks the lineage legacy; see
    /// `Schema::has_allocation_watermark`. Once loaded, the watermark is
    /// always written back (as `max(idx) + 1` at worst), so it can only grow.
    #[serde(default)]
    next_idx: usize,
    /// Set by this version for a legacy lineage. Absent for a schema
    /// persisted before the flag existed, where `next_idx == 0` (missing, or
    /// the `0` briefly written for a legacy lineage) says the same thing.
    #[serde(default)]
    legacy: bool,
}

impl Serialize for Schema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let val = SchemaRef {
            fields: self.fields.values().collect(),
            version: self.version,
            // Always the real high-water mark, legacy or not: it is what the
            // next upgrade allocates from. The `legacy` flag, not a missing
            // or zeroed watermark, is what keeps the lineage lenient after
            // the round trip (see `Schema::has_allocation_watermark`).
            next_idx: self.allocated_idx_end(),
            legacy: self.legacy_lineage,
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
        // otherwise allow invalid names / malformed types / duplicate indexes.
        let mut idx = BTreeSet::<usize>::new();
        let mut fields = BTreeMap::<String, FieldEntry>::new();

        for mut f in val.fields.into_iter() {
            crate::validate_field_name(f.name()).map_err(serde::de::Error::custom)?;
            // A schema persisted before `validate_declaration` existed may
            // carry `Option<Option<T>>`, which the derive used to infer.
            // That shape is indistinguishable from `Option<T>` on the wire,
            // so repair it instead of rejecting the schema — the alternative
            // is a collection nobody can open. Everything else still fails.
            f.repair_declaration()
                .r#type()
                .validate_declaration()
                .map_err(|err| serde::de::Error::custom(format!("field {:?}: {err}", f.name())))?;

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

        Ok(Schema {
            idx,
            fields,
            version: val.version,
            next_idx: val.next_idx,
            // A schema persisted without a watermark (`next_idx` missing, or
            // the `0` a previous build wrote for such a lineage) is legacy;
            // once flagged, the flag itself carries the status forward.
            legacy_lineage: val.legacy || val.next_idx == 0,
        })
    }
}

/// SchemaBuilder is used to construct a Schema instance.
/// It provides methods to add fields and build the final schema.
#[derive(Clone, Debug)]
pub struct SchemaBuilder {
    idx: usize,
    fields: BTreeMap<String, FieldEntry>,
    version: u64,
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
            version: 0,
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

    /// Sets the schema version.
    pub fn with_version(&mut self, version: u64) -> &mut Self {
        self.version = version;
        self
    }

    /// Add a [`Resource`]-typed field to the schema.
    ///
    /// This is a convenience wrapper around [`add_field`](Self::add_field)
    /// that derives its `FieldType` from [`Resource::field_type`] and wraps
    /// it in [`FieldType::Option`] when `required` is `false`.
    ///
    /// # Arguments
    /// * `field` - Name to register the resource under.
    /// * `required` - When `true`, the field must always be present;
    ///   otherwise it becomes optional.
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

        let next_idx = self.idx + 1;
        if next_idx > u16::MAX as usize {
            return Err(SchemaError::Schema(
                "Schema has reached the maximum number of fields".to_string(),
            ));
        }

        self.idx = next_idx;
        self.fields
            .insert(entry.name().to_string(), entry.with_idx(self.idx));
        Ok(self)
    }

    /// Builds the final Schema from this builder.
    ///
    /// Every invariant is enforced while building — `_id` is injected by
    /// [`SchemaBuilder::new`], [`SchemaBuilder::add_field`] bounds the field
    /// count and rejects duplicates — so this cannot fail today. It keeps the
    /// `Result` signature so that existing callers, including the `schema()`
    /// functions generated by `#[derive(AndaDBSchema)]`, stay source-compatible.
    pub fn build(self) -> Result<Schema, SchemaError> {
        let idx: BTreeSet<usize> = self.fields.values().map(|f| f.idx()).collect();
        Ok(Schema {
            next_idx: idx.last().map_or(0, |last| last + 1),
            idx,
            fields: self.fields,
            version: self.version,
            legacy_lineage: false,
        })
    }
}

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
        assert_eq!(builder.idx, u16::MAX as usize);
        assert!(!builder.fields.contains_key("overflow"));
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

    #[test]
    fn test_schema_version() {
        // 默认 version 为 0
        let mut builder = SchemaBuilder::new();
        builder
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        let schema_v0 = builder.build().unwrap();
        assert_eq!(schema_v0.version(), 0);
        let mut schema_v1 = schema_v0.clone();
        schema_v1.with_version(1);
        assert_eq!(schema_v1.version(), 1);

        // 设置 version
        let mut builder = SchemaBuilder::new();
        builder.with_version(2);
        builder
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        let schema_v2 = builder.build().unwrap();
        assert_eq!(schema_v2.version(), 2);

        // needs_upgrade: 高版本 → 低版本 = true
        assert!(schema_v2.needs_upgrade(&schema_v0));
        // needs_upgrade: 低版本 → 高版本 = false
        assert!(!schema_v0.needs_upgrade(&schema_v2));
        // needs_upgrade: 相同版本 = false
        assert!(!schema_v0.needs_upgrade(&schema_v0));

        // 不同 version 的 schema 不相等
        assert_ne!(schema_v0, schema_v2);
    }

    #[test]
    fn test_schema_version_serde_roundtrip() {
        let mut builder = SchemaBuilder::new();
        builder.with_version(3);
        builder
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        let schema = builder.build().unwrap();
        assert_eq!(schema.version(), 3);

        let v = serde_json::to_value(&schema).unwrap();
        assert_eq!(v.get("version").unwrap().as_u64().unwrap(), 3);

        let schema2: Schema = serde_json::from_value(v).unwrap();
        assert_eq!(schema, schema2);
        assert_eq!(schema2.version(), 3);
    }

    #[test]
    fn test_schema_version_defaults_to_zero_on_deserialize() {
        // 模拟旧版本序列化数据（无 version 字段）
        let mut builder = SchemaBuilder::new();
        builder
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        let schema = builder.build().unwrap();

        let mut v = serde_json::to_value(&schema).unwrap();
        // 移除 version 字段，模拟旧数据
        v.as_object_mut().unwrap().remove("version");

        let schema2: Schema = serde_json::from_value(v).unwrap();
        assert_eq!(schema2.version(), 0);
        assert_eq!(schema, schema2);
    }

    #[test]
    fn test_upgrade_with_inherits_old_idx() {
        // old schema v1: _id(0), name(1), age(2)
        let mut old_builder = SchemaBuilder::new();
        old_builder.with_version(1);
        old_builder
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        old_builder
            .add_field(Fe::new("age".to_string(), Ft::Option(Box::new(Ft::U64))).unwrap())
            .unwrap();
        let old = old_builder.build().unwrap();
        assert_eq!(old.get_field("name").unwrap().idx(), 1);
        assert_eq!(old.get_field("age").unwrap().idx(), 2);

        // new schema v2: _id, name, age, email (builder assigns idx 1,2,3)
        let mut new_builder = SchemaBuilder::new();
        new_builder.with_version(2);
        new_builder
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        new_builder
            .add_field(Fe::new("age".to_string(), Ft::Option(Box::new(Ft::U64))).unwrap())
            .unwrap();
        new_builder
            .add_field(Fe::new("email".to_string(), Ft::Option(Box::new(Ft::Text))).unwrap())
            .unwrap();
        let mut new_schema = new_builder.build().unwrap();

        // Before upgrade_with, builder-assigned idx
        assert_eq!(new_schema.get_field("email").unwrap().idx(), 3);

        // After upgrade_with, old fields keep their idx, new field gets next_idx=3
        new_schema.upgrade_with(&old).unwrap();
        assert_eq!(new_schema.get_field("_id").unwrap().idx(), 0);
        assert_eq!(new_schema.get_field("name").unwrap().idx(), 1);
        assert_eq!(new_schema.get_field("age").unwrap().idx(), 2);
        assert_eq!(new_schema.get_field("email").unwrap().idx(), 3);
    }

    #[test]
    fn test_upgrade_with_removed_field_idx_not_reused() {
        // old schema v1: _id(0), name(1), age(2), bio(3)
        let mut old_builder = SchemaBuilder::new();
        old_builder.with_version(1);
        old_builder
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        old_builder
            .add_field(Fe::new("age".to_string(), Ft::Option(Box::new(Ft::U64))).unwrap())
            .unwrap();
        old_builder
            .add_field(Fe::new("bio".to_string(), Ft::Option(Box::new(Ft::Text))).unwrap())
            .unwrap();
        let old = old_builder.build().unwrap();
        assert_eq!(old.get_field("bio").unwrap().idx(), 3);

        // new schema v2: remove "bio", add "email"
        // builder assigns: name=1, age=2, email=3
        let mut new_builder = SchemaBuilder::new();
        new_builder.with_version(2);
        new_builder
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        new_builder
            .add_field(Fe::new("age".to_string(), Ft::Option(Box::new(Ft::U64))).unwrap())
            .unwrap();
        new_builder
            .add_field(Fe::new("email".to_string(), Ft::Option(Box::new(Ft::Text))).unwrap())
            .unwrap();
        let mut new_schema = new_builder.build().unwrap();
        new_schema.upgrade_with(&old).unwrap();

        // name and age keep old idx
        assert_eq!(new_schema.get_field("name").unwrap().idx(), 1);
        assert_eq!(new_schema.get_field("age").unwrap().idx(), 2);
        // email gets idx=4 (max old idx was 3, so next is 4), NOT reusing bio's 3
        assert_eq!(new_schema.get_field("email").unwrap().idx(), 4);
    }

    #[test]
    fn test_upgrade_with_removed_field_old_documents_still_read_back() {
        use crate::{Document, DocumentOwned};
        use std::sync::Arc;

        // old schema v1: _id(0), name(1), bio(2)
        let mut old_builder = SchemaBuilder::new();
        old_builder.with_version(1);
        old_builder
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        old_builder
            .add_field(Fe::new("bio".to_string(), Ft::Option(Box::new(Ft::Text))).unwrap())
            .unwrap();
        let old = old_builder.build().unwrap();

        // new schema v2: "bio" removed.
        let mut new_builder = SchemaBuilder::new();
        new_builder.with_version(2);
        new_builder
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        let mut new_schema = new_builder.build().unwrap();
        new_schema.upgrade_with(&old).unwrap();
        assert!(!new_schema.contains_idx(2));

        // A document written under the old schema still carries idx 2.
        let mut fields = IndexedFieldValues::new();
        fields.insert(0, Fv::U64(7));
        fields.insert(1, Fv::Text("Ada".to_string()));
        fields.insert(2, Fv::Text("stale bio".to_string()));

        // Strict validation of the raw fields still rejects the stale idx...
        assert!(new_schema.validate(&fields).is_err());

        // ...but the document read path drops it and succeeds.
        let doc = Document::try_from_doc(Arc::new(new_schema), DocumentOwned { fields }).unwrap();
        assert_eq!(doc.get_field("name").unwrap(), &Fv::Text("Ada".into()));
        assert_eq!(doc.fields().len(), 2);
    }

    #[test]
    fn test_upgrade_with_rejects_new_required_field() {
        let mut old_builder = SchemaBuilder::new();
        old_builder.with_version(1);
        old_builder
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        let old = old_builder.build().unwrap();

        let mut new_builder = SchemaBuilder::new();
        new_builder.with_version(2);
        new_builder
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        new_builder
            .add_field(Fe::new("email".to_string(), Ft::Text).unwrap())
            .unwrap();
        let mut new_schema = new_builder.build().unwrap();

        let err = new_schema.upgrade_with(&old).unwrap_err();
        assert!(
            format!("{err:?}").contains("must be optional"),
            "expected required field error, got: {err:?}"
        );
    }

    #[test]
    fn test_upgrade_with_rejects_index_overflow() {
        let mut old_builder = SchemaBuilder::new();
        old_builder.with_version(1);
        old_builder.idx = u16::MAX as usize - 1;
        old_builder
            .add_field(Fe::new("last".to_string(), Ft::Text).unwrap())
            .unwrap();
        let old = old_builder.build().unwrap();
        assert_eq!(old.get_field("last").unwrap().idx(), u16::MAX as usize);

        let mut new_builder = SchemaBuilder::new();
        new_builder.with_version(2);
        new_builder
            .add_field(Fe::new("last".to_string(), Ft::Text).unwrap())
            .unwrap();
        new_builder
            .add_field(Fe::new("next".to_string(), Ft::Option(Box::new(Ft::Text))).unwrap())
            .unwrap();
        let mut new_schema = new_builder.build().unwrap();

        assert!(new_schema.upgrade_with(&old).is_err());
    }

    #[test]
    fn test_upgrade_with_rejects_type_change() {
        let mut old_builder = SchemaBuilder::new();
        old_builder.with_version(1);
        old_builder
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        let old = old_builder.build().unwrap();

        // Try changing "name" from Text to U64
        let mut new_builder = SchemaBuilder::new();
        new_builder.with_version(2);
        new_builder
            .add_field(Fe::new("name".to_string(), Ft::U64).unwrap())
            .unwrap();
        let mut new_schema = new_builder.build().unwrap();

        let err = new_schema.upgrade_with(&old).unwrap_err();
        assert!(
            format!("{err:?}").contains("type changed"),
            "expected type change error, got: {err:?}"
        );
    }

    #[test]
    fn test_upgrade_with_is_atomic_on_error() {
        // old schema v1: _id(0), age(1), name(2)
        let mut old_builder = SchemaBuilder::new();
        old_builder.with_version(1);
        old_builder
            .add_field(Fe::new("age".to_string(), Ft::U64).unwrap())
            .unwrap();
        old_builder
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        let old = old_builder.build().unwrap();

        // new schema v2: insertion order flipped so the builder assigns
        // name=1, age=2 — i.e. "age" must inherit a different idx (1) during the
        // upgrade. "name" changes type (Text → U64), which fails the upgrade
        // *after* "age" would already have been visited.
        let mut new_builder = SchemaBuilder::new();
        new_builder.with_version(2);
        new_builder
            .add_field(Fe::new("name".to_string(), Ft::U64).unwrap())
            .unwrap();
        new_builder
            .add_field(Fe::new("age".to_string(), Ft::U64).unwrap())
            .unwrap();
        let mut new_schema = new_builder.build().unwrap();
        let snapshot = new_schema.clone();

        assert!(new_schema.upgrade_with(&old).is_err());
        // The failed upgrade must not leave the schema partially migrated:
        // "age" must keep the builder-assigned idx 2, not the inherited 1.
        assert_eq!(new_schema, snapshot);
        assert_eq!(new_schema.get_field("age").unwrap().idx(), 2);
    }

    #[test]
    fn test_upgrade_with_rejects_unique_change() {
        let mut old_builder = SchemaBuilder::new();
        old_builder.with_version(1);
        old_builder
            .add_field(Fe::new("email".to_string(), Ft::Text).unwrap())
            .unwrap();
        let old = old_builder.build().unwrap();

        // Flip `unique` from false to true.
        let mut new_builder = SchemaBuilder::new();
        new_builder.with_version(2);
        new_builder
            .add_field(
                Fe::new("email".to_string(), Ft::Text)
                    .unwrap()
                    .with_unique(),
            )
            .unwrap();
        let mut new_schema = new_builder.build().unwrap();

        let err = new_schema.upgrade_with(&old).unwrap_err();
        assert!(
            format!("{err:?}").contains("unique flag changed"),
            "expected unique change error, got: {err:?}"
        );
    }

    #[test]
    fn test_upgrade_with_rejects_lower_version() {
        let mut old_builder = SchemaBuilder::new();
        old_builder.with_version(3);
        old_builder
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        let old = old_builder.build().unwrap();

        let mut new_builder = SchemaBuilder::new();
        new_builder.with_version(2);
        new_builder
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        let mut new_schema = new_builder.build().unwrap();

        assert!(new_schema.upgrade_with(&old).is_err());
    }

    #[test]
    fn legacy_schema_without_watermark_stays_lenient_and_sticky() {
        let opt = || Ft::Option(Box::new(Ft::Text));
        let mut builder = Schema::builder();
        builder
            .add_field(Fe::new("a".into(), opt()).unwrap())
            .unwrap();
        builder
            .add_field(Fe::new("b".into(), opt()).unwrap())
            .unwrap();
        let built = builder.build().unwrap();
        assert!(built.has_allocation_watermark());

        // A schema persisted before the watermark existed has no `next_idx`.
        let mut value = serde_json::to_value(&built).unwrap();
        value.as_object_mut().unwrap().remove("next_idx");
        let legacy: Schema = serde_json::from_value(value).unwrap();
        assert!(!legacy.has_allocation_watermark());
        assert_eq!(legacy.allocated_idx_end(), 3);
        // Declaration equality ignores the watermark.
        assert_eq!(legacy, built);

        // Re-serializing keeps the lineage legacy.
        let again: Schema = serde_json::from_value(serde_json::to_value(&legacy).unwrap()).unwrap();
        assert!(!again.has_allocation_watermark());

        // So does upgrading it; allocation continues from `max(idx) + 1`.
        let mut next = Schema::builder();
        next.add_field(Fe::new("a".into(), opt()).unwrap()).unwrap();
        next.add_field(Fe::new("c".into(), opt()).unwrap()).unwrap();
        next.with_version(1);
        let mut next = next.build().unwrap();
        next.upgrade_with(&legacy).unwrap();
        assert!(!next.has_allocation_watermark());
        assert_eq!(next.get_field("c").unwrap().idx(), 3);
        assert_eq!(next.allocated_idx_end(), 4);

        // A watermarked lineage stays watermarked.
        let mut next = Schema::builder();
        next.add_field(Fe::new("a".into(), opt()).unwrap()).unwrap();
        next.with_version(1);
        let mut next = next.build().unwrap();
        next.upgrade_with(&built).unwrap();
        assert!(next.has_allocation_watermark());
        assert_eq!(next.allocated_idx_end(), 3);
    }

    #[test]
    fn legacy_lineage_never_reuses_the_index_of_a_removed_field() {
        // Leniency and allocation are separate concerns: a legacy lineage
        // keeps dropping undeclared indexes, but its watermark still has to
        // advance across the removal of its *highest* field, or the next
        // upgrade would hand that index to a new field and read the removed
        // field's stale bytes as the new field's value.
        let opt = || Ft::Option(Box::new(Ft::Text));
        let build = |names: &[&str], version: u64| {
            let mut b = Schema::builder();
            for name in names {
                b.add_field(Fe::new((*name).into(), opt()).unwrap()).unwrap();
            }
            b.with_version(version);
            b.build().unwrap()
        };
        // Persist and reload, the way a collection does between upgrades.
        let reload = |schema: &Schema| -> Schema {
            serde_json::from_value(serde_json::to_value(schema).unwrap()).unwrap()
        };

        let v0 = build(&["a", "b", "c"], 0);
        assert_eq!(v0.get_field("c").unwrap().idx(), 3);
        let mut value = serde_json::to_value(&v0).unwrap();
        value.as_object_mut().unwrap().remove("next_idx");
        let v0: Schema = serde_json::from_value(value).unwrap();
        assert!(!v0.has_allocation_watermark());

        // v1 removes the highest field.
        let mut v1 = build(&["a", "b"], 1);
        v1.upgrade_with(&v0).unwrap();
        let v1 = reload(&v1);
        assert!(!v1.has_allocation_watermark(), "leniency is still sticky");
        assert_eq!(v1.allocated_idx_end(), 4, "the watermark must not go back");

        // v2 adds a field: it gets a fresh index, not `c`'s.
        let mut v2 = build(&["a", "b", "d"], 2);
        v2.upgrade_with(&v1).unwrap();
        assert_eq!(v2.get_field("d").unwrap().idx(), 4);
        assert!(!reload(&v2).has_allocation_watermark());
    }

    #[test]
    fn upgrade_with_allows_making_a_field_optional() {
        let mut old = Schema::builder();
        old.add_field(Fe::new("name".into(), Ft::Text).unwrap())
            .unwrap();
        let old = old.build().unwrap();

        let mut new = Schema::builder();
        new.add_field(Fe::new("name".into(), Ft::Option(Box::new(Ft::Text))).unwrap())
            .unwrap();
        new.with_version(1);
        let mut new = new.build().unwrap();
        new.upgrade_with(&old).unwrap();
        assert_eq!(new.get_field("name").unwrap().idx(), 1);

        // The reverse still fails: stored nulls would not validate.
        let mut back = Schema::builder();
        back.add_field(Fe::new("name".into(), Ft::Text).unwrap())
            .unwrap();
        back.with_version(2);
        let mut back = back.build().unwrap();
        assert!(back.upgrade_with(&new).is_err());
    }

    #[test]
    fn schema_deserialize_rejects_malformed_field_types() {
        let mut builder = Schema::builder();
        builder
            .add_field(Fe::new("a".into(), Ft::Option(Box::new(Ft::Text))).unwrap())
            .unwrap();
        let schema = builder.build().unwrap();
        let with_type = |t: serde_json::Value| {
            let mut value = serde_json::to_value(&schema).unwrap();
            let field = value["fields"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|f| f["n"] == "a")
                .unwrap();
            field["t"] = t;
            value
        };

        // A nested `Option` is what the derive used to infer, so a persisted
        // schema can carry one. It is indistinguishable from a single
        // `Option` on the wire and is repaired, not rejected: rejecting it
        // would make every document of that collection unreachable.
        let repaired: Schema =
            serde_json::from_value(with_type(json!({"Option": {"Option": "Text"}}))).unwrap();
        assert_eq!(
            repaired.get_field("a").unwrap().r#type(),
            &Ft::Option(Box::new(Ft::Text))
        );
        let repaired: Schema = serde_json::from_value(with_type(
            json!({"Array": [{"Option": {"Option": {"Option": "Text"}}}]}),
        ))
        .unwrap();
        assert_eq!(
            repaired.get_field("a").unwrap().r#type(),
            &Ft::Array(vec![Ft::Option(Box::new(Ft::Text))])
        );

        // Shapes with no lossless repair still fail the load.
        let mixed = Ft::Map(BTreeMap::from([
            (crate::FieldKey::from("*"), Ft::U64),
            (crate::FieldKey::from("k"), Ft::U64),
        ]));
        let err =
            serde_json::from_value::<Schema>(with_type(serde_json::to_value(&mixed).unwrap()))
                .unwrap_err();
        assert!(err.to_string().contains("wildcard"), "{err}");
    }
}

