use anda_db_derive::{AndaDBSchema, FieldTyped};

#[derive(AndaDBSchema)]
#[serde(tag = "kind")]
struct Tagged {
    _id: u64,
}

#[derive(FieldTyped)]
#[serde(into = "String")]
struct Converted {
    value: String,
}

#[derive(FieldTyped)]
#[field_type = "Json"]
struct ContainerOverride {
    value: String,
}

fn main() {}
