#![allow(dead_code)]
use anda_db_schema::{AndaDBSchema, FieldTyped};

#[derive(FieldTyped)]
struct First {
    second: Option<Box<Second>>,
}
#[derive(FieldTyped)]
struct Second {
    first: Vec<First>,
}

type Alias = Aliased;
#[derive(FieldTyped)]
struct Aliased {
    next: Option<Box<Alias>>,
}

#[derive(AndaDBSchema)]
struct Doc {
    _id: u64,
    data: First,
}

#[derive(FieldTyped)]
struct Leaf {
    value: String,
}

#[derive(FieldTyped)]
struct Explicit {
    #[field_type = "Json"]
    next: Option<Box<Self>>,
}

#[derive(FieldTyped)]
struct Handle<T> {
    token: [u8; 16],
    #[serde(skip)]
    marker: std::marker::PhantomData<T>,
}
#[derive(FieldTyped)]
struct UsesHandle {
    handle: Handle<UsesHandle>,
}

#[test]
fn indirect_cycles_return_errors_without_poisoning_later_builds() {
    for error in [
        First::try_field_type().unwrap_err(),
        Aliased::try_field_type().unwrap_err(),
        Doc::schema().unwrap_err(),
    ] {
        assert!(error.to_string().contains("recursive"), "{error}");
    }
    Leaf::try_field_type().unwrap();
    Explicit::try_field_type().unwrap();
    UsesHandle::try_field_type().unwrap();
}
