# `anda_db_derive`

A Rust procedural macro crate that provides derive macros for AndaDB schema types.

## Features

- `FieldTyped` derive macro: Automatically generates a `field_type()` function for structs that maps Rust types to AndaDB schema field types.
- `AndaDBSchema` derive macro: Automatically generates a `schema()` function for structs that creates complete AndaDB Schema definitions.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
anda_db_schema = "0.2"
```

## Usage

### FieldTyped Derive Macro

The `FieldTyped` derive macro analyzes struct fields and their types, mapping them to the appropriate `FieldType` enum variants. It supports common Rust types and handles `Option<T>` wrappers.

#### Supported Type Mappings

| Rust Type                                          | AndaDB FieldType        |
| -------------------------------------------------- | ----------------------- |
| `String`, `&str`                                   | `FieldType::Text`       |
| `bool`                                             | `FieldType::Bool`       |
| `i8`, `i16`, `i32`, `i64`, `isize`                 | `FieldType::I64`        |
| `u8`, `u16`, `u32`, `u64`, `usize`                 | `FieldType::U64`        |
| `f32`                                              | `FieldType::F32`        |
| `f64`                                              | `FieldType::F64`        |
| `Vec<u8>`, `[u8]`, `ByteArray`, `ByteBuf`, `Bytes` | `FieldType::Bytes`      |
| `Vec<bf16>`, `[bf16]`                              | `FieldType::Vector`     |
| `Vec<T>`, `HashSet<T>`, `BTreeSet<T>`              | `FieldType::Array`      |
| `Option<T>`                                        | `FieldType::Option`     |
| `HashMap<String, T>`, `BTreeMap<String, T>`        | `FieldType::Map`        |
| `bf16`, `half::bf16`                               | `FieldType::Bf16`       |
| `Json`, `serde_json::Value`                        | `FieldType::Json`       |
| Custom structs                                     | Nested `FieldType::Map` |

#### Attributes

- `field_type = "TypeName"`: Override the inferred type with a specific FieldType
- `serde(rename = "field_name")`: Use the renamed field in the generated schema

#### Example

```rust
use anda_db_schema::{FieldTyped, FieldType, Json};
use serde::{Deserialize, Serialize};
use serde_json::Map;
use std::collections::HashMap;
use ic_auth_types::Xid;

// Define a struct with the FieldTyped derive macro
#[derive(Serialize, Deserialize, FieldTyped)]
struct User {
    name: String,
    age: u32,

    // HashMap will be correctly mapped to FieldType::Map
    tags: HashMap<String, String>,

    // Optional fields are handled automatically
    email: Option<String>,

    #[field_type = "Array<Bytes>"]
    #[serde(rename = "ids")]
    follow_ids: Vec<Xid>,
}

// Define a nested struct
#[derive(Serialize, Deserialize, FieldTyped)]
struct Document {
    // Custom field type override
    #[field_type = "Bytes"]
    id: Xid,

    #[serde(rename = "c")]
    content: String,

    attributes: Map<String, serde_json::Value>,
    metadata: Map<String, Json>,

    // Support nested struct
    author: User,
}

fn main() {
    // Get the field type schema for User
    let user_schema = User::field_type();
    println!("User schema: {:?}", user_schema);

    // Get the field type schema for Document
    let doc_schema = Document::field_type();
    println!("Document schema: {:?}", doc_schema);
}
```

The generated `field_type()` method returns a `FieldType::Map` containing the type information for each field, which can be used for schema validation, serialization, or other purposes in AndaDB.

### AndaDBSchema Derive Macro

The `AndaDBSchema` derive macro creates complete AndaDB Schema definitions based on struct fields. It automatically handles field type mapping and supports various attributes for customization.

#### Attributes

- `field_type = "TypeName"`: Override the inferred type with a specific FieldType
- `unique`: Mark the field as unique in the collection
- `serde(rename = "new_name")`: Use a different name in the schema
- Doc comments (`///`) are used as field descriptions

#### Special Field Handling

- The `_id` field is automatically handled by AndaDB and must be of type `u64`
- Fields marked with `#[unique]` will have unique constraints in the schema
- Doc comments are extracted and used as field descriptions in the schema

#### Example

```rust
use anda_db_schema::{FieldEntry, FieldType, Json, Map, Schema, SchemaError};
use anda_db_derive::AndaDBSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, AndaDBSchema)]
struct User {
    /// Auto-generated unique identifier (handled by AndaDB)
    _id: u64,

    /// User's unique email address
    #[unique]
    email: String,

    /// User's display name
    name: String,

    /// User's age in years
    age: Option<u32>,

    /// Whether the user account is active
    active: bool,

    /// User tags for categorization
    tags: Vec<String>,

    /// User metadata as key-value pairs
    metadata: Map<String, Json>,

    /// Custom field type for binary data
    #[field_type = "Bytes"]
    avatar: [u8; 32],

    /// Renamed field in schema
    #[serde(rename = "created_at")]
    creation_time: u64,
}

#[derive(Serialize, Deserialize, AndaDBSchema)]
struct Document {
    _id: u64,

    /// Document title
    #[unique]
    title: String,

    /// Document content
    content: String,

    /// Document author reference
    author_id: u64,

    /// Document tags
    tags: Vec<String>,

    /// Vector embeddings for similarity search
    #[field_type = "Vector"]
    embeddings: Vec<half::bf16>,

    /// Document properties
    properties: Map<String, Properties>,
}

#[derive(Deserialize, Serialize, FieldTyped)]
struct Properties {
    #[serde(rename = "a")]
    pub attributes: Map<String, Json>,
    #[serde(rename = "m")]
    pub metadata: Map<String, Json>,
}

fn main() -> Result<(), SchemaError> {
    // Generate schema for User
    let user_schema = User::schema()?;
    println!("User schema: {:?}", user_schema);

    // Generate schema for Document
    let doc_schema = Document::schema()?;
    println!("Document schema: {:?}", doc_schema);

    Ok(())
}
```

The generated `schema()` method returns a `Result<Schema, SchemaError>` containing the complete schema definition that can be used to create collections in AndaDB.

#### Schema Features

- **Automatic Type Inference**: Rust types are automatically mapped to appropriate AndaDB field types
- **Unique Constraints**: Fields marked with `#[unique]` will have unique constraints
- **Field Descriptions**: Doc comments are extracted and used as field descriptions
- **Custom Types**: Use `#[field_type = "TypeName"]` to override automatic type inference
- **Field Renaming**: Support for `serde(rename = "name")` attributes
- **Nested Structures**: Support for nested structs and complex types

## Error Handling

Both derive macros provide compile-time error checking:

- Unsupported field types will generate compile errors with helpful messages
- Invalid `_id` field types (must be `u64`) will be caught at compile time
- Malformed attributes will be detected during compilation

## License

Copyright © 2026 [LDC Labs](https://github.com/ldclabs).

`ldclabs/anda-db` is licensed under the MIT License. See [LICENSE](../../LICENSE) for the full license text.
