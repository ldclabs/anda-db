//! `_id` is always `FieldType::U64`; a `#[field_type]` override used to be
//! silently ignored.
use anda_db_derive::AndaDBSchema;

#[derive(AndaDBSchema)]
struct Doc {
    #[field_type = "Text"]
    _id: u64,
    name: String,
}

fn main() {}
