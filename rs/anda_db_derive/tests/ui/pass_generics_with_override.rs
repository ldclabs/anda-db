//! Generic structs work when generic fields carry a `#[field_type]`
//! override; generic user types resolve through their own `field_type()`.
use anda_db_derive::{AndaDBSchema, FieldTyped};

#[derive(FieldTyped)]
struct Wrapper<T> {
    #[field_type = "Json"]
    value: T,
    label: String,
}

#[derive(FieldTyped)]
struct UsesWrapper {
    wrapped: Wrapper<u64>,
}

#[derive(AndaDBSchema)]
struct Doc<T> {
    _id: u64,
    #[field_type = "Option<Json>"]
    payload: Option<T>,
}

fn main() {
    let _ = Wrapper::<u8>::field_type();
    let _ = UsesWrapper::field_type();
    let _ = Doc::<u8>::schema().unwrap();
}
