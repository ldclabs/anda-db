use anda_db_derive::FieldTyped;

#[derive(FieldTyped)]
struct TextKey {
    #[serde(rename = "*")]
    value: String,
}

#[derive(FieldTyped)]
struct IntegerKey {
    #[cbor(key = -9223372036854775808)]
    value: u64,
}

fn main() {}
