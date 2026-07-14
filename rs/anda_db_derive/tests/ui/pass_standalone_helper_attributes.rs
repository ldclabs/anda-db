//! Each derive must register every helper attribute it parses. These structs
//! intentionally do not derive Serde or the sibling AndaDB macro, so another
//! derive cannot accidentally register `serde` or `cbor` on their behalf.
use anda_db_derive::{AndaDBSchema, FieldTyped};

#[derive(FieldTyped)]
#[serde(rename_all = "camelCase")]
struct NestedClaims {
    #[cbor(key = 1)]
    issuer_name: String,
    #[serde(skip)]
    ignored: String,
}

#[derive(AndaDBSchema)]
#[serde(rename_all = "snake_case")]
struct Document {
    #[serde(rename = "_id")]
    _id: u64,
    display_name: String,
}

fn main() {
    let nested = NestedClaims::field_type();
    let anda_db_schema::FieldType::Map(fields) = nested else {
        panic!("nested struct should derive a map");
    };
    assert!(fields.contains_key(&anda_db_schema::FieldKey::I64(1)));

    let schema = Document::schema().unwrap();
    assert!(schema.get_field("display_name").is_some());
}
