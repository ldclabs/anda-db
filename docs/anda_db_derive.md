# anda_db_derive Technical Documentation

**Version**: 0.4
**Last Updated**: 2026-04-21

---

## Table of Contents

1. [Overview](#1-overview)
2. [Derive Macros](#2-derive-macros)
3. [Attribute Details](#3-attribute-details)
4. [Type Mapping](#4-type-mapping)
5. [Usage Examples](#5-usage-examples)
6. [Internal Implementation](#6-internal-implementation)

---

## 1. Overview

### 1.1 Module Responsibilities

`anda_db_derive` provides two procedural macros for auto-generating AndaDB Schema-related code from Rust structs:

- `FieldTyped` - Generates `field_type()` method
- `AndaDBSchema` - Generates complete `schema()` method

### 1.2 Module Structure

```
anda_db_derive/
├── src/
│   ├── lib.rs           # Macro definition entry
│   ├── schema.rs        # AndaDBSchema macro implementation
│   ├── field_typed.rs   # FieldTyped macro implementation
│   └── common.rs        # Shared utility functions
├── Cargo.toml
└── README.md
```

---

## 2. Derive Macros

### 2.1 FieldTyped

Generates `field_type()` function returning the struct's type mapping:

```rust
#[proc_macro_derive(FieldTyped, attributes(field_type))]
pub fn field_typed_derive(input: TokenStream) -> TokenStream
```

**Usage Example**:

```rust
use anda_db_schema::{FieldType, FieldTyped};

#[derive(FieldTyped)]
struct User {
    id: u64,
    name: String,
    age: u32,
}

// Generates:
impl User {
    pub fn field_type() -> FieldType {
        FieldType::Map(vec![
            ("id".into(), FieldType::U64),
            ("name".into(), FieldType::Text),
            ("age".into(), FieldType::U64),
        ].into_iter().collect())
    }
}
```

### 2.2 AndaDBSchema

Generates complete `schema()` function for creating AndaDB Schema:

```rust
#[proc_macro_derive(AndaDBSchema, attributes(field_type, unique))]
pub fn anda_db_schema_derive(input: TokenStream) -> TokenStream
```

**Usage Example**:

```rust
use anda_db_schema::{Schema, SchemaError, AndaDBSchema};
use anda_db_derive::AndaDBSchema;

#[derive(AndaDBSchema)]
struct Article {
    /// Article unique identifier
    _id: u64,
    /// Article title
    title: String,
    /// Article content
    content: String,
    /// View count
    views: u64,
}

// Generates:
impl Article {
    pub fn schema() -> Result<Schema, SchemaError> {
        let mut builder = Schema::builder();

        builder.add_field(
            FieldEntry::new("title".to_string(), FieldType::Text)?
                .with_description("Article title")
        )?;
        builder.add_field(
            FieldEntry::new("content".to_string(), FieldType::Text)?
                .with_description("Article content")
        )?;
        builder.add_field(
            FieldEntry::new("views".to_string(), FieldType::U64)?
                .with_description("View count")
        )?;

        builder.build()
    }
}
```

---

## 3. Attribute Details

### 3.1 #[field_type = "TypeName"]

Override auto-inferred field type:

```rust
#[derive(AndaDBSchema)]
struct MyDoc {
    _id: u64,
    #[field_type = "Bytes"]    // Override to Bytes type
    custom_id: [u8; 16],
    #[field_type = "Option<Text>"]  // Override to optional text
    nickname: Option<String>,
}
```

**Supported Type Strings**:

| String | FieldType |
|--------|-----------|
| `"Bytes"` | `FieldType::Bytes` |
| `"Text"` | `FieldType::Text` |
| `"U64"` | `FieldType::U64` |
| `"I64"` | `FieldType::I64` |
| `"F64"` | `FieldType::F64` |
| `"F32"` | `FieldType::F32` |
| `"Bool"` | `FieldType::Bool` |
| `"Json"` | `FieldType::Json` |
| `"Vector"` | `FieldType::Vector` |
| `"Array<T>"` | `FieldType::Array(vec![T])` |
| `"Option<T>"` | `FieldType::Option(Box::new(T))` |
| `"Map<String, T>"` | `FieldType::Map({*: T})` |
| `"Map<Bytes, T>"` | `FieldType::Map({b*: T})` |

### 3.2 #[unique]

Mark field as having a unique constraint:

```rust
#[derive(AndaDBSchema)]
struct User {
    _id: u64,
    #[unique]
    email: String,  // Unique constraint
}
```

**Generated Code**:

```rust
builder.add_field(
    FieldEntry::new("email".to_string(), FieldType::Text)?
        .with_unique()
)?;
```

### 3.3 #[serde(rename = "name")]

Use serde's rename attribute to use a different name in Schema:

```rust
#[derive(AndaDBSchema, Serialize, Deserialize)]
struct User {
    _id: u64,
    #[serde(rename = "userName")]
    user_name: String,  // Field named "userName" in Schema
}
```

### 3.4 Doc Comments (///)

Doc comments are automatically extracted as field descriptions:

```rust
#[derive(AndaDBSchema)]
struct User {
    /// User's unique email address
    email: String,
    /// User's display name in the system
    display_name: String,
}
```

**Generated Code**:

```rust
builder.add_field(
    FieldEntry::new("email".to_string(), FieldType::Text)?
        .with_description("User's unique email address")
)?;
builder.add_field(
    FieldEntry::new("display_name".to_string(), FieldType::Text)?
        .with_description("User's display name in the system")
)?;
```

---

## 4. Type Mapping

### 4.1 Basic Types

| Rust Type | FieldType |
|-----------|-----------|
| `bool` | `Bool` |
| `i8`, `i16`, `i32`, `i64`, `isize` | `I64` |
| `u8`, `u16`, `u32`, `u64`, `usize` | `U64` |
| `f32` | `F32` |
| `f64` | `F64` |

### 4.2 String and Bytes

| Rust Type | FieldType |
|-----------|-----------|
| `String`, `&str` | `Text` |
| `Vec<u8>`, `[u8; N]` | `Bytes` |
| `Bytes`, `ByteArray`, `ByteBuf` | `Bytes` |
| `ByteArrayB64`, `ByteBufB64` | `Bytes` |
| `serde_bytes::ByteArray`, `serde_bytes::ByteBuf`, `serde_bytes::Bytes` | `Bytes` |

### 4.3 Vector Types

| Rust Type | FieldType |
|-----------|-----------|
| `Vec<bf16>`, `[bf16; N]` | `Vector` |
| `half::bf16` | `Bf16` (internally mapped to Vector) |

### 4.4 JSON

| Rust Type | FieldType |
|-----------|-----------|
| `serde_json::Value`, `Json` | `Json` |

### 4.5 Collection Types

| Rust Type | FieldType |
|-----------|-----------|
| `Vec<T>` | `Array(T)` |
| `HashSet<T>`, `BTreeSet<T>` | `Array(T)` |

**Inner element type mapping**: See basic type mapping rules.

### 4.6 Map Types

| Rust Type | FieldType |
|-----------|-----------|
| `HashMap<String, V>`, `BTreeMap<String, V>` | `Map({*: V})` |
| `Map<String, V>` | `Map({*: V})` |
| `HashMap<Bytes, V>`, `BTreeMap<Bytes, V>` | `Map({b*: V})` |

### 4.7 Optional Types

| Rust Type | FieldType |
|-----------|-----------|
| `Option<T>` | `Option(T)` |

### 4.8 Custom Types

For custom structs that implement `FieldTyped`:

```rust
#[derive(FieldTyped)]
struct Address {
    street: String,
    city: String,
}

#[derive(AndaDBSchema)]
struct User {
    _id: u64,
    address: Address,  // Uses Address::field_type()
}
```

---

## 5. Usage Examples

### 5.1 Complete Example

```rust
use anda_db_schema::{Schema, FieldEntry, FieldType, Document, AndaDBSchema, Fv};
use anda_db_derive::AndaDBSchema;
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use bf16::bf16;

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
    /// Whether article is published
    published: bool,
    /// Optional tags
    tags: Option<Vec<String>>,
    /// Author metadata
    author_meta: Option<serde_json::Value>,
    /// Embedding vector
    embedding: Vec<bf16>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create Schema
    let schema = Arc::new(Article::schema()?);
    println!("Schema: {:#?}", schema);

    // Create document
    let mut doc = Document::new(schema.clone());
    doc.set_id(1);
    doc.set_field("title", Fv::Text("Hello World".to_string()))?;
    doc.set_field("content", Fv::Text("This is content".to_string()))?;
    doc.set_field("views", Fv::U64(0))?;
    doc.set_field("published", Fv::Bool(true))?;
    doc.set_field("embedding", Fv::Vector(vec![bf16::from_f32(0.1); 512]))?;

    println!("Document: {:#?}", doc);
    println!("Title: {:?}", doc.get_field("title"));

    Ok(())
}
```

### 5.2 Example with Custom Types

```rust
use anda_db_schema::{Schema, FieldType, AndaDBSchema, FieldTyped};
use anda_db_derive::AndaDBSchema;

#[derive(FieldTyped, Debug)]
struct GeoLocation {
    latitude: f64,
    longitude: f64,
}

#[derive(AndaDBSchema)]
struct Place {
    _id: u64,
    name: String,
    location: GeoLocation,  // Uses custom type's field_type()
}
```

### 5.3 Example with Type Overrides

```rust
use anda_db_schema::{Schema, AndaDBSchema};
use anda_db_derive::AndaDBSchema;
use ic_auth_types::Xid;

#[derive(AndaDBSchema)]
struct Transaction {
    _id: u64,
    #[field_type = "Bytes"]  // Use Xid as byte array
    tx_id: Xid,
    amount: u64,
}
```

---

## 6. Internal Implementation

### 6.1 common.rs - Shared Logic

#### find_rename_attr()

Extract rename from serde attribute:

```rust
pub fn find_rename_attr(attrs: &[Attribute]) -> Option<String>
```

#### find_field_type_attr()

Extract field_type attribute value:

```rust
pub fn find_field_type_attr(attrs: &[Attribute]) -> Option<TokenStream>
```

#### parse_field_type_str()

Parse type string to TokenStream:

```rust
pub fn parse_field_type_str(type_str: &str) -> TokenStream
```

#### determine_field_type()

Infer FieldType from Rust type:

```rust
pub fn determine_field_type(ty: &Type) -> Result<TokenStream, String>
```

### 6.2 schema.rs - AndaDBSchema Implementation

Key steps:
1. Parse DeriveInput
2. Iterate through all named fields
3. Extract attributes (rename, field_type, unique, doc comments)
4. Determine field type
5. Generate SchemaBuilder code

### 6.3 field_typed.rs - FieldTyped Implementation

Key steps:
1. Parse DeriveInput
2. Iterate through all named fields
3. Generate `field_type()` method body

### 6.4 Code Generation Example

Input:
```rust
#[derive(AndaDBSchema)]
struct User {
    /// User's email
    #[unique]
    email: String,
}
```

Generated:
```rust
impl User {
    pub fn schema() -> Result<Schema, SchemaError> {
        let mut builder = Schema::builder();

        builder.add_field(
            FieldEntry::new("email".to_string(), FieldType::Text)?
                .with_description("User's email")
                .with_unique()
        )?;

        builder.build()
    }
}
```

---

## Appendix A: Error Messages

### A.1 Compile-Time Errors

| Error Message | Cause |
|--------------|-------|
| `FieldTyped only supports structs with named fields` | Tuple struct or unit struct not supported |
| `AndaDBSchema only supports structs` | Enum or union not supported |
| `The '_id' field must be of type u64` | _id field type is incorrect |
| `Unsupported type: '...'` | Unsupported Rust type |
| `Invalid map type` | Map key type is not String or Bytes |
| `Unable to determine Vec element type` | Vec type argument is incomplete |

### A.2 Runtime Errors

| Error Type | Description |
|-----------|-------------|
| `SchemaError::FieldName` | Invalid field name |
| `SchemaError::FieldType` | Invalid field type |
| `SchemaError::Validation` | Schema validation failed |

---

*Document generated: 2026-04-21*
