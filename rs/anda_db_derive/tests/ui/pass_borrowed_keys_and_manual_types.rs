use anda_db_schema::{FieldType, FieldTyped, AndaDBSchema};
use std::collections::BTreeMap;

struct Manual;
impl Manual {
    fn field_type() -> FieldType { FieldType::Text }
}

#[derive(FieldTyped)]
struct Borrowed<'a> {
    text: BTreeMap<&'a str, u64>,
    bytes: BTreeMap<&'a [u8], String>,
    boxed: BTreeMap<Box<str>, bool>,
    custom: Manual,
}

#[derive(AndaDBSchema)]
struct Doc<'a> {
    _id: u64,
    values: Borrowed<'a>,
}

fn main() {
    assert!(Borrowed::try_field_type().is_ok());
    assert!(Doc::schema().is_ok());
}
