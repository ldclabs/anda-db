use anda_db_derive::{AndaDBSchema, FieldTyped};

#[derive(FieldTyped)]
struct DuplicateType {
    #[field_type = "Text"]
    #[field_type = "Bytes"]
    value: String,
}

#[derive(AndaDBSchema)]
struct InvalidUnique {
    _id: u64,
    #[unique = false]
    value: String,
}

#[derive(AndaDBSchema)]
struct DuplicateUnique {
    _id: u64,
    #[unique]
    #[unique]
    value: String,
}

fn main() {}
