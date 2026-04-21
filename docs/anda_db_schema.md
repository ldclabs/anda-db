# anda_db_schema Technical Documentation

**Version**: 0.4
**Last Updated**: 2026-04-21

---

## Table of Contents

1. [Overview](#1-overview)
2. [Field Type System (FieldType)](#2-field-type-system-fieldtype)
3. [Field Value (FieldValue)](#3-field-value-fieldvalue)
4. [Field Entry (FieldEntry)](#4-field-entry-fieldentry)
5. [Schema Definition](#5-schema-definition)
6. [Document Model](#6-document-model)
7. [Resource Type](#7-resource-type)
8. [Derive Macros](#8-derive-macros)
9. [Serialization and Deserialization](#9-serialization-and-deserialization)
10. [Error Handling](#10-error-handling)
11. [API Reference](#11-api-reference)
12. [Usage Examples](#12-usage-examples)

---

## 1. Overview

### 1.1 Module Responsibilities

`anda_db_schema` is the core type system library for AndaDB, responsible for:

- Defining all supported field types (`FieldType`)
- Storing actual field values (`FieldValue`)
- Describing field metadata (`FieldEntry`)
- Defining document structure (`Schema`)
- Document serialization/deserialization
- Field name validation

### 1.2 Core Concept Hierarchy

```
Schema (Document Structure Definition)
├── FieldEntry (Field Definition)
│   ├── name: String
│   ├── r#type: FieldType
│   ├── description: String
│   ├── unique: bool
│   └── idx: usize
├── ...
│
├── FieldType (Field Type Enum)
│   ├── Bool, I64, U64, F64, F32, Bytes, Text, Json, Vector
│   ├── Array(Vec<FieldType>)
│   ├── Map(BTreeMap<FieldKey, FieldType>)
│   └── Option(Box<FieldType>)
│
└── FieldValue (Field Value Enum)
    ├── Bool(bool), I64(i64), U64(u64), F64(f64), F32(f32)
    ├── Bytes(Vec<u8>), Text(String), Json(serde_json::Value)
    ├── Vector(Vec<bf16>), Array(Vec<FieldValue>)
    ├── Map(BTreeMap<FieldKey, FieldValue>)
    └── Null
```

### 1.3 Module Structure

```
anda_db_schema/
├── src/
│   ├── lib.rs           # Main entry, exports all public types
│   ├── field.rs         # FieldType, FieldValue, FieldEntry, FieldKey
│   ├── schema.rs        # Schema definition and SchemaBuilder
│   ├── document.rs      # Document and DocumentOwned
│   ├── resource.rs      # Resource type definition
│   ├── value_serde.rs   # FieldValue serialization/deserialization
│   └── error.rs         # SchemaError and BoxError
│
├── Cargo.toml
└── README.md
```

---

## 2. Field Type System (FieldType)

### 2.1 Type Definition

`FieldType` is an enum defining all field types supported by AndaDB:

```rust
pub enum FieldType {
    Bool,                    // Boolean value
    I64,                     // 64-bit signed integer
    U64,                     // 64-bit unsigned integer
    F64,                     // 64-bit floating point
    F32,                     // 32-bit floating point
    Bytes,                   // Binary data
    Text,                    // UTF-8 text
    Json,                    // JSON value
    Vector,                  // bf16 vector (Vec<bf16>)
    Array(Vec<FieldType>),   // Array
    Map(BTreeMap<FieldKey, FieldType>),  // Map
    Option(Box<FieldType>),   // Optional type
}
```

### 2.2 Type Aliases

```rust
pub type Ft = FieldType;   // Type alias
pub type Vector = Vec<bf16>; // Vector type
```

### 2.3 Basic Types

| FieldType | Rust Type | Description |
|-----------|-----------|-------------|
| `Bool` | `bool` | Boolean value |
| `I64` | `i8`, `i16`, `i32`, `i64`, `isize` | Signed 64-bit integer |
| `U64` | `u8`, `u16`, `u32`, `u64`, `usize` | Unsigned 64-bit integer |
| `F64` | `f64` | 64-bit floating point |
| `F32` | `f32` | 32-bit floating point |
| `Bytes` | `Vec<u8>`, `[u8; N]`, `Bytes`, `ByteArray` | Byte array |
| `Text` | `String`, `&str` | UTF-8 encoded text |
| `Json` | `serde_json::Value`, `Json` | JSON value |
| `Vector` | `Vec<bf16>`, `[bf16; N]` | bf16 vector |

### 2.4 Composite Types

#### Array (Array)

Homogeneous array, all elements have the same type:

```rust
// String array
FieldType::Array(vec![FieldType::Text])

// Multi-dimensional vector (array of arrays)
FieldType::Array(vec![FieldType::Array(vec![FieldType::F64])])
```

#### Map (Map)

Key-value pair collection, supports two key types:

```rust
// Wildcard Map - all values have the same type
FieldType::Map(BTreeMap::from([("*".into(), FieldType::U64)]))
// Equivalent to Map<String, U64>

// Bytes key Map
FieldType::Map(BTreeMap::from([(b"*".as_slice().into(), FieldType::Text)]))
// Equivalent to Map<Bytes, Text>
```

#### Option (Optional)

Represents value can be null:

```rust
FieldType::Option(Box::new(FieldType::Text))
// Equivalent to Option<String>
```

### 2.5 Field Name Validation

Field names must satisfy the following rules:

```rust
pub fn validate_field_name(s: &str) -> Result<(), SchemaError> {
    // 1. Cannot be empty
    // 2. Cannot exceed 64 characters
    // 3. Can only contain lowercase letters, numbers, and underscores
}
```

**Valid Examples**:
- `user_id`
- `name`
- `a1`
- `a_1`

**Invalid Examples**:
- `UserId` (contains uppercase letters)
- `user-id` (contains hyphen)
- `user.id` (contains dot)
- `user id` (contains space)

### 2.6 FieldKey

Map key type, supports two forms:

```rust
pub enum FieldKey {
    Text(String),    // Text key
    Bytes(Vec<u8>), // Bytes key
}
```

**Wildcard Keys**:
```rust
pub static TEXT_WILDCARD_KEY: LazyLock<FieldKey> = "*".into();
pub static BYTES_WILDCARD_KEY: LazyLock<FieldKey> = b"*".into();
```

### 2.7 Type Methods

#### allows_null()

Check if type allows null values:

```rust
pub fn allows_null(&self) -> bool {
    matches!(self, FieldType::Option(_))
}
```

#### extract()

Extract specified type FieldValue from CBOR value:

```rust
pub fn extract(&self, value: Cbor) -> Result<FieldValue, SchemaError>
```

#### validate()

Validate if FieldValue matches this field type:

```rust
pub fn validate(&self, value: &FieldValue) -> Result<(), SchemaError>
```

---

## 3. Field Value (FieldValue)

### 3.1 Type Definition

`FieldValue` is an enum storing actual data values:

```rust
pub enum FieldValue {
    Bool(bool),                            // Boolean value
    I64(i64),                              // Signed 64-bit integer
    U64(u64),                              // Unsigned 64-bit integer
    F64(f64),                              // 64-bit floating point
    F32(f32),                              // 32-bit floating point
    Bytes(Vec<u8>),                        // Byte array
    Text(String),                           // Text
    Json(serde_json::Value),               // JSON value
    Vector(Vec<bf16>),                     // bf16 vector
    Array(Vec<FieldValue>),               // Array
    Map(BTreeMap<FieldKey, FieldValue>),  // Map
    Null,                                  // Null value
}
```

### 3.2 Type Alias

```rust
pub type Fv = FieldValue;  // Field value type alias
```

### 3.3 Type Conversions

#### From Trait Implementation

The following types can be directly converted to FieldValue:

```rust
impl From<bool> for FieldValue
impl From<i64> for FieldValue
impl From<u64> for FieldValue
impl From<f64> for FieldValue
impl From<f32> for FieldValue
impl From<Vec<u8>> for FieldValue
impl From<String> for FieldValue
impl From<Json> for FieldValue
impl From<Vec<bf16>> for FieldValue

// Collection types
impl<T> From<Vec<T>> where T: Into<FieldValue>
impl<T> From<BTreeSet<T>> where T: Into<FieldValue>
impl<T> From<HashSet<T>> where T: Into<FieldValue>

// Map types
impl<K, V> From<BTreeMap<K, V>> where K: Into<FieldKey>, V: Into<FieldValue>
impl<K, V> From<HashMap<K, V>> where K: Into<FieldKey>, V: Into<FieldValue>
```

#### TryFrom Trait Implementation

The following types can be converted from FieldValue back to the original type:

```rust
impl TryFrom<FieldValue> for bool
impl TryFrom<FieldValue> for i64
impl TryFrom<FieldValue> for u64
impl TryFrom<FieldValue> for f64
impl TryFrom<FieldValue> for f32
impl TryFrom<FieldValue> for Vec<u8>
impl TryFrom<FieldValue> for [u8; N]  // Fixed-size array
impl TryFrom<FieldValue> for String
impl TryFrom<FieldValue> for Json
impl TryFrom<FieldValue> for Vec<bf16>
impl TryFrom<FieldValue> for [bf16; N]  // Fixed-size array

// Collection types
impl<T> TryFrom<FieldValue> for Vec<T> where T: TryFrom<FieldValue>
impl<T> TryFrom<FieldValue> for BTreeMap<FieldKey, T>
```

### 3.4 CBOR Conversion

FieldValue can convert to/from CBOR format:

```rust
impl From<FieldValue> for Cbor

impl TryFrom<Cbor> for FieldValue {
    fn try_from(value: Cbor) -> Result<Self, SchemaError>
}
```

**Conversion Rules**:
- `FieldValue::Bool` ↔ `Cbor::Bool`
- `FieldValue::I64/U64` ↔ `Cbor::Integer`
- `FieldValue::F64/F32` ↔ `Cbor::Float`
- `FieldValue::Bytes` ↔ `Cbor::Bytes`
- `FieldValue::Text` ↔ `Cbor::Text`
- `FieldValue::Vector` ↔ `Cbor::Array` (bf16 converted to u16 bits)
- `FieldValue::Array` ↔ `Cbor::Array`
- `FieldValue::Map` ↔ `Cbor::Map`
- `FieldValue::Null` ↔ `Cbor::Null`

### 3.5 JSON Serialization

FieldValue implements full Serialize/Deserialize:

```rust
impl Serialize for FieldValue {
    // Text → string
    // Bytes → Base64 encoded (human-readable mode) or byte array
    // Vector → u16 bits array
    // Array → JSON array
    // Map → JSON object
}
```

**Bytes Base64 Encoding Example**:
```json
{"Kg==":"Kg=="}  // Base64 encoded {"*":"*}
```

### 3.6 Convenience Methods

#### serialized()

Convert any serializable type to FieldValue:

```rust
pub fn serialized<T: Serialize>(
    value: &T,
    ft: Option<&FieldType>
) -> Result<Self, SchemaError>
```

#### deserialized()

Deserialize FieldValue to specified type:

```rust
pub fn deserialized<T: DeserializeOwned>(self) -> Result<T, SchemaError>
```

#### get_field_as()

Get field from Map-type FieldValue:

```rust
pub fn get_field_as<'a, T: ?Sized>(&'a self, field: &FieldKey) -> Option<&'a T>
where
    &'a T: TryFrom<&'a FieldValue>
```

---

## 4. Field Entry (FieldEntry)

### 4.1 Type Definition

`FieldEntry` describes complete metadata for a field:

```rust
pub struct FieldEntry {
    name: String,           // Field name
    r#type: FieldType,      // Field type
    description: String,    // Field description (for LLM understanding)
    unique: bool,          // Whether unique
    idx: usize,            // Field index (for storage optimization)
}
```

### 4.2 Serialization Format

Field names are abbreviated in serialization to save storage space:

```rust
pub struct FieldEntry {
    #[serde(rename = "n", alias = "name")]
    name: String,

    #[serde(rename = "d", alias = "description")]
    description: String,

    #[serde(rename = "t", alias = "type")]
    r#type: FieldType,

    #[serde(rename = "u", alias = "unique")]
    unique: bool,

    #[serde(rename = "i", alias = "index")]
    idx: usize,
}
```

### 4.3 Builder Methods

```rust
// Create basic field entry
let entry = FieldEntry::new("user_name".to_string(), FieldType::Text)?;

// Set description
entry.with_description("User's display name".to_string())

// Mark as unique
entry.with_unique()

// Set index
entry.with_idx(1)
```

### 4.4 Accessor Methods

```rust
pub fn name(&self) -> &str           // Get field name
pub fn r#type(&self) -> &FieldType  // Get field type
pub fn required(&self) -> bool       // Whether required field
pub fn unique(&self) -> bool         // Whether unique
pub fn idx(&self) -> usize           // Get field index
```

### 4.5 Validation Methods

#### extract()

Extract and validate field value from CBOR value:

```rust
pub fn extract(&self, val: Cbor, validate: bool) -> Result<FieldValue, SchemaError>
```

#### validate()

Validate if FieldValue meets this field's constraints:

```rust
pub fn validate(&self, value: &FieldValue) -> Result<(), SchemaError>
```

**Validation Rules**:
1. Required fields (`FieldType::Option` excluded) cannot be `Null`
2. Value type must match `FieldType`
3. Array length must match declared type
4. Map keys must all exist in type definition

---

## 5. Schema Definition

### 5.1 Type Definition

`Schema` defines complete document structure:

```rust
pub struct Schema {
    idx: BTreeSet<usize>,                    // Field index set
    fields: BTreeMap<String, FieldEntry>,    // Field name → Field entry
    version: u64,                           // Schema version number
}
```

### 5.2 Schema Characteristics

- **ID Field Reserved**: `_id` field is mandatory, type must be `U64`, index is 0
- **Max Field Count**: `u16::MAX + 1` (65536 fields)
- **Index Compression**: Fields stored by index instead of name, saves space
- **Version Support**: Supports schema evolution

### 5.3 SchemaBuilder

Build Schema using Builder pattern:

```rust
let schema = Schema::builder()
    .with_version(1)  // Optional: set version number
    .add_field(
        FieldEntry::new("title".to_string(), FieldType::Text)?
            .with_description("Document title")
    )?
    .add_field(
        FieldEntry::new("content".to_string(), FieldType::Text)?
            .with_description("Document content")
    )?
    .build()?;
```

**Automatically Added Fields**:
```rust
Schema::builder() automatically adds:
- _id: U64 (unique, index=0, description="\"_id\" is a u64 field, used as an internal unique identifier")
```

### 5.4 Schema Methods

```rust
pub fn version(&self) -> u64
pub fn len(&self) -> usize
pub fn is_empty(&self) -> bool
pub fn get_field(&self, name: &str) -> Option<&FieldEntry>
pub fn get_field_or_err(&self, name: &str) -> Result<&FieldEntry, SchemaError>
pub fn iter(&self) -> impl Iterator<Item = &FieldEntry>
pub fn validate(&self, values: &IndexedFieldValues) -> Result<(), SchemaError>
```

### 5.5 Schema Evolution (upgrade_with)

Use `upgrade_with` for gradual migration when application code changes but data already exists:

```rust
pub fn upgrade_with(&mut self, old: &Schema) -> Result<(), SchemaError>
```

**Migration Rules**:
1. New Schema version must be greater than old version
2. Existing fields inherit old index, type cannot change
3. New fields assigned smallest available index
4. Deleted field indices are never reused

**Example**:

```rust
// Old Schema v1: _id(0), name(1), age(2)
let old = Schema::builder()
    .with_version(1)
    .add_field(Fe::new("name".to_string(), Ft::Text)?)?
    .add_field(Fe::new("age".to_string(), Ft::Option(Box::new(Ft::U64)))?)?
    .build()?;

// New Schema v2: _id, name, age, email
let mut new = Schema::builder()
    .with_version(2)
    .add_field(Fe::new("name".to_string(), Ft::Text)?)?
    .add_field(Fe::new("age".to_string(), Ft::Option(Box::new(Ft::U64)))?)?
    .add_field(Fe::new("email".to_string(), Ft::Option(Box::new(Ft::Text)))?)?
    .build()?;

// Execute migration
new.upgrade_with(&old)?;

// Result: _id(0), name(1), age(2), email(3)
// Note: name and age keep their original indices
```

### 5.6 IndexedFieldValues

Container for storing document field values, using field index as key:

```rust
pub type IndexedFieldValues = BTreeMap<usize, FieldValue>;
```

---

## 6. Document Model

### 6.1 Document Structure

```rust
pub struct Document {
    fields: IndexedFieldValues,   // Field value collection
    schema: Arc<Schema>,         // Reference to Schema
}

pub struct DocumentOwned {
    pub fields: IndexedFieldValues,  // Field value collection (no Schema reference)
}

pub type DocumentId = u64;  // Document ID type alias
```

### 6.2 Creating Documents

```rust
// Create empty document from Schema
let doc = Document::new(schema.clone());

// Convert from DocumentOwned
let doc = Document::try_from_doc(schema.clone(), doc_owned)?;

// Convert from any serializable type
let doc = Document::try_from(schema.clone(), &my_struct)?;
```

### 6.3 Accessing Fields

```rust
// Get document ID
doc.id()

// Get field value
doc.get_field("name")           // Option<&Fv>
doc.get_field_or_err("name")?  // Result<&Fv, SchemaError>

// Get and deserialize
doc.get_field_as::<String>("name")?
```

### 6.4 Modifying Fields

```rust
// Set field value
doc.set_field("name", Fv::Text("John".to_string()))?;
doc.set_field("age", Fv::U64(30))?;

// Set and serialize
doc.set_field_as("name", &"John".to_string())?;
doc.set_field_as("age", &30u64)?;

// Remove field
doc.remove_field("name");  // Option<Fv>

// Update document
doc.set_doc(doc_owned)?;
```

### 6.5 Type Conversions

```rust
// Document → DocumentOwned
let owned: DocumentOwned = doc.into();

// Document → custom type
let my_struct: MyStruct = doc.try_into()?;
```

### 6.6 Serialization

Document implements Serialize trait, output format:

```json
{
  "f": {
    "0": 99,
    "1": "John Doe",
    "2": 30
  }
}
```

---

## 7. Resource Type

### 7.1 Type Definition

`Resource` is a predefined Schema type for storing external resource references for AI Agents:

```rust
#[derive(Debug, Default, Clone, Serialize, Deserialize, FieldTyped, PartialEq, AndaDBSchema)]
pub struct Resource {
    pub _id: u64,                              // Unique identifier

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,            // Type tags

    pub name: String,                          // Resource name

    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,          // Description

    #[serde(skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,                  // URI

    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,           // MIME type

    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob: Option<ByteBufB64>,            // Binary data

    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,                    // Size in bytes

    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<ByteArrayB64<32>>,     // SHA3-256 hash

    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Map<String, Json>>, // Metadata
}
```

### 7.2 Using Resource Fields

Use Resource in custom Schema:

```rust
#[derive(AndaDBSchema)]
struct Article {
    _id: u64,
    title: String,
    thumbnail: Option<Resource>,  // Optional thumbnail resource
}
```

---

## 8. Derive Macros

### 8.1 AndaDBSchema

Automatically generate Schema definition from Rust struct:

```rust
#[derive(AndaDBSchema)]
struct MyDoc {
    _id: u64,                    // Auto-recognized as ID field
    title: String,              // → Text
    content: String,            // → Text
    age: Option<u64>,          // → Option(U64)
    tags: Vec<String>,          // → Array(Text)
    embedding: Vec<bf16>,     // → Vector
    metadata: HashMap<String, Json>, // → Map(String, Json)
}
```

### 8.2 FieldTyped

Generate `field_type()` method:

```rust
#[derive(FieldTyped)]
struct MyStruct {
    name: String,
    count: u64,
}

// Generated code:
impl MyStruct {
    pub fn field_type() -> FieldType {
        FieldType::Map(vec![
            ("name".into(), FieldType::Text),
            ("count".into(), FieldType::U64),
        ].into_iter().collect())
    }
}
```

### 8.3 Attributes

#### #[field_type = "TypeName"]

Override inferred type:

```rust
#[derive(AndaDBSchema)]
struct MyDoc {
    _id: u64,
    #[field_type = "Bytes"]    // Override to Bytes type
    custom_id: [u8; 16],
}
```

#### #[unique]

Mark field as unique:

```rust
#[derive(AndaDBSchema)]
struct MyDoc {
    _id: u64,
    #[unique]
    email: String,  // Unique field
}
```

#### #[serde(rename = "name")]

Use different Schema field name:

```rust
#[derive(AndaDBSchema, Serialize, Deserialize)]
struct MyDoc {
    _id: u64,
    #[serde(rename = "userName")]
    user_name: String,  // Named "userName" in Schema
}
```

#### Doc Comments

Doc comments automatically become field descriptions:

```rust
#[derive(AndaDBSchema)]
struct MyDoc {
    /// User's display name
    name: String,  // description = "User's display name"
}
```

### 8.4 Type Mapping Table

| Rust Type | FieldType |
|-----------|-----------|
| `bool` | `Bool` |
| `i8`, `i16`, `i32`, `i64`, `isize` | `I64` |
| `u8`, `u16`, `u32`, `u64`, `usize` | `U64` |
| `f32` | `F32` |
| `f64` | `F64` |
| `String`, `&str` | `Text` |
| `Vec<u8>`, `[u8; N]`, `Bytes` | `Bytes` |
| `Vec<bf16>`, `[bf16; N]` | `Vector` |
| `serde_json::Value` | `Json` |
| `Vec<T>`, `HashSet<T>`, `BTreeSet<T>` | `Array(T)` |
| `HashMap<String, V>`, `BTreeMap<String, V>` | `Map(*, V)` |
| `HashMap<Bytes, V>`, `BTreeMap<Bytes, V>` | `Map(b*, V)` |
| `Option<T>` | `Option(T)` |
| Custom struct | Recursively use its `field_type()` |

---

## 9. Serialization and Deserialization

### 9.1 CBOR Format

Internal storage uses CBOR format with compact binary representation:

```rust
use ciborium::{cbor, from_reader, into_writer};

// Serialize
let field_value = Fv::Array(vec![Fv::U64(1), Fv::Text("hello".to_string())]);
let mut serialized = Vec::new();
into_writer(&field_value, &mut serialized).unwrap();
// Output: 82016568656c6c6f

// Deserialize
let deserialized: Fv = from_reader(serialized.as_slice()).unwrap();
```

### 9.2 JSON Format

Used for human-readable serialization and API transmission:

```rust
use serde_json;

// Serialize
let field_value = Fv::Text("hello".to_string());
let json = serde_json::to_string(&field_value).unwrap();
// Output: "hello"

// Deserialize
let deserialized: Fv = serde_json::from_str(&json).unwrap();
```

### 9.3 Bytes Base64 Encoding

In JSON human-readable mode, Bytes type uses Base64 encoding:

```rust
let bytes = Fv::Bytes(vec![1, 2, 3, 4]);
let json = serde_json::to_string(&bytes).unwrap();
// Output: "AQIDBA=="

let decoded: Fv = serde_json::from_str(&json).unwrap();
```

### 9.4 Vector Serialization

bf16 vector serializes to u16 bits array:

```rust
let vector = Fv::Vector(vec![bf16::from_f32(1.0), bf16::from_f32(1.1)]);
let cbor: Cbor = vector.clone().into();
// Stored as [16256, 16269] (各自的 bits)

// Deserialize
let restored: Fv = FieldValue::try_from(cbor).unwrap();
```

---

## 10. Error Handling

### 10.1 SchemaError Enum

```rust
pub enum SchemaError {
    #[error("Invalid schema: {0}")]
    Schema(String),

    #[error("Invalid field type: {0}")]
    FieldType(String),

    #[error("Invalid field value: {0}")]
    FieldValue(String),

    #[error("Invalid field name: {0}")]
    FieldName(String),

    #[error("Field validation failed: {0}")]
    Validation(String),

    #[error("Serialization error: {0}")]
    Serialization(String),
}
```

### 10.2 BoxError Type Alias

```rust
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
```

### 10.3 Common Error Scenarios

| Scenario | Error Type |
|----------|------------|
| Invalid field name | `FieldName` |
| Type mismatch | `FieldType` |
| Value validation failed | `FieldValue` |
| Missing required field | `Validation` |
| CBOR serialization failed | `Serialization` |
| Invalid Schema structure | `Schema` |

---

## 11. API Reference

### 11.1 Type Aliases

```rust
pub type Ft = FieldType;           // Field type
pub type Fv = FieldValue;         // Field value
pub type Fe = FieldEntry;          // Field entry
pub type Json = serde_json::Value; // JSON value
pub type Cbor = ciborium::Value;   // CBOR value
pub type Vector = Vec<bf16>;       // Vector type
pub type Map<K, V> = serde_json::Map<K, V>; // JSON Map
pub type BoxError = Box<dyn std::error::Error + Send + Sync>; // Error type
pub type DocumentId = u64;         // Document ID
pub type IndexedFieldValues = BTreeMap<usize, FieldValue>; // Field value index map
```

### 11.2 Main Structs

| Struct | Description |
|--------|-------------|
| `FieldType` | Field type enum |
| `FieldValue` | Field value enum |
| `FieldEntry` | Field definition |
| `FieldKey` | Map key type |
| `Schema` | Document structure definition |
| `SchemaBuilder` | Schema builder |
| `Document` | Document (with Schema reference) |
| `DocumentOwned` | Document (without Schema reference) |
| `Resource` | Resource type |

### 11.3 Main Functions

```rust
// Field name validation
pub fn validate_field_name(s: &str) -> Result<(), SchemaError>

// Vector conversion
pub fn vector_from_f32(v: Vec<f32>) -> Vector
pub fn vector_from_f64(v: Vec<f64>) -> Vector
```

---

## 12. Usage Examples

### 12.1 Basic Schema Definition

```rust
use anda_db_schema::{
    Schema, SchemaBuilder, FieldEntry, FieldType as Ft,
    FieldValue as Fv, Document, DocumentOwned,
};
use std::sync::Arc;

let schema = Schema::builder()
    .add_field(
        FieldEntry::new("title".to_string(), Ft::Text)?
            .with_description("Document title")
    )?
    .add_field(
        FieldEntry::new("content".to_string(), Ft::Text)?
            .with_description("Document body text")
    )?
    .add_field(
        FieldEntry::new("views".to_string(), Ft::U64)?
            .with_description("View count")
    )?
    .build()?;

let schema = Arc::new(schema);
```

### 12.2 Using Derive Macro

```rust
use anda_db_schema::{Schema, AndaDBSchema, FieldType, Document};
use anda_db_derive::AndaDBSchema;
use std::sync::Arc;
use serde::{Serialize, Deserialize};

#[derive(AndaDBSchema, Serialize, Deserialize, Debug)]
struct Article {
    /// Article unique identifier
    _id: u64,
    /// Article title
    title: String,
    /// Article content
    content: String,
    /// View count
    views: u64,
    /// Optional tags
    tags: Option<Vec<String>>,
    /// Embedding vector for semantic search
    embedding: Vec<bf16>,
}

let schema = Arc::new(Article::schema().unwrap());
```

### 12.3 Creating and Manipulating Documents

```rust
use anda_db_schema::{Document, Fv};

let doc = Document::new(schema.clone());

// Set ID
doc.set_id(1);

// Set fields
doc.set_field("title", Fv::Text("Hello World".to_string()))?;
doc.set_field("content", Fv::Text("This is my first article".to_string()))?;
doc.set_field("views", Fv::U64(100))?;

// Get field
if let Some(Fv::Text(title)) = doc.get_field("title") {
    println!("Title: {}", title);
}

// Convert to DocumentOwned
let owned: DocumentOwned = doc.into();
```

### 12.4 Creating Documents from Structs

```rust
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct ArticleInput {
    _id: u64,
    title: String,
    content: String,
    views: u64,
}

let input = ArticleInput {
    _id: 1,
    title: "Hello".to_string(),
    content: "World".to_string(),
    views: 42,
};

let doc = Document::try_from(schema.clone(), &input)?;
println!("Document ID: {}", doc.id());
```

### 12.5 Schema Validation

```rust
use anda_db_schema::{IndexedFieldValues, Fv};

// Create valid field values
let mut values = IndexedFieldValues::new();
values.insert(0, Fv::U64(1));        // _id
values.insert(1, Fv::Text("Title".to_string()));  // title
values.insert(2, Fv::Text("Content".to_string())); // content
values.insert(3, Fv::U64(0));        // views

// Validate
schema.validate(&values)?;  // Ok(())

// Create invalid field values (type mismatch)
let mut invalid_values = IndexedFieldValues::new();
invalid_values.insert(0, Fv::Text("not_u64".to_string())); // Should be U64
invalid_values.insert(1, Fv::Text("Title".to_string()));

schema.validate(&invalid_values);  // Err(...)
```

### 12.6 Schema Evolution

```rust
// Old Schema (loaded from storage)
let old_schema = Schema::builder()
    .with_version(1)
    .add_field(Fe::new("name".to_string(), Ft::Text)?)?
    .add_field(Fe::new("age".to_string(), Ft::U64)?)?
    .build()?;

// New Schema (application code)
let mut new_schema = Schema::builder()
    .with_version(2)
    .add_field(Fe::new("name".to_string(), Ft::Text)?)?
    .add_field(Fe::new("age".to_string(), Ft::U64)?)?
    .add_field(Fe::new("email".to_string(), Ft::Option(Box::new(Ft::Text)))?)?
    .build()?;

// Execute migration
new_schema.upgrade_with(&old_schema)?;
```

---

## Appendix A: CBOR Serialization Format Reference

| FieldValue | CBOR Encoding | Example |
|------------|---------------|---------|
| `Null` | `f6` | `Null` |
| `Bool(true)` | `f5` | `true` |
| `Bool(false)` | `f4` | `false` |
| `U64(42)` | `18 2a` | `42` |
| `I64(-42)` | `39 0029` | `-42` |
| `F64(3.14)` | `fb 40091eb851eb851f` | `3.14` |
| `Text("hello")` | `65 hello` | `"hello"` |
| `Bytes([1,2,3,4])` | `44 01020304` | `01 02 03 04` |
| `Array([U64(1), Text("hello")])` | `82 01 65 hello` | `[1, "hello"]` |
| `Map({"key": Text("value")})` | `a1 63key 65value` | `{"key": "value"}` |

---

## Appendix B: Integration with Other Libraries

### B.1 serde

anda_db_schema is fully serde-compatible:

```rust
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct MyStruct {
    field: String,
}
```

### B.2 ciborium

Used for CBOR serialization:

```rust
use ciborium::{cbor, from_reader, into_writer};

let value: Fv = from_reader(data.as_slice())?;
into_writer(&value, &mut output)?;
```

### B.3 half

Used for bf16 type:

```rust
use half::bf16;

let vec = vec![bf16::from_f32(1.0), bf16::from_f32(2.0)];
let fv = Fv::Vector(vec);
```

---

*Document generated: 2026-04-21*
