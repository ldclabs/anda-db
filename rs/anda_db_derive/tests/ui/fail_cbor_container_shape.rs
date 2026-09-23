use anda_db_derive::{AndaDBSchema, FieldTyped};

#[derive(FieldTyped)]
#[cbor(array)]
struct NestedArray {
    value: u64,
}

#[derive(FieldTyped)]
#[cbor(tag = 61)]
struct NestedTagged {
    value: u64,
}

#[derive(AndaDBSchema)]
#[cbor(array)]
struct ArrayDoc {
    _id: u64,
}

#[derive(AndaDBSchema)]
#[cbor(tag = 61)]
struct TaggedDoc {
    _id: u64,
}

fn main() {}
