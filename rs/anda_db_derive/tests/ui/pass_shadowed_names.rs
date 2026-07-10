//! User types sharing names with `anda_db_schema` items must not be shadowed
//! by macro-generated code (regression: the generated body used to `use`
//! bare `FieldType` / `FieldKey` / `Schema` names into its scope).
use anda_db_derive::{AndaDBSchema, FieldTyped};

#[derive(FieldTyped)]
struct FieldType {
    x: u64,
}

#[derive(FieldTyped)]
struct FieldKey {
    y: String,
}

#[derive(FieldTyped)]
struct Schema {
    z: bool,
}

#[derive(FieldTyped)]
struct Nested {
    ft: FieldType,
    key: FieldKey,
    schema: Schema,
}

#[derive(AndaDBSchema)]
struct Doc {
    _id: u64,
    ft: FieldType,
}

fn main() {
    let _ = Nested::field_type();
    let _ = Doc::schema().unwrap();
}
