//! A bare generic field cannot provide `field_type()`; the macro must emit
//! a targeted error pointing at the field type (not an E0599 on the derive).
use anda_db_derive::FieldTyped;

#[derive(FieldTyped)]
struct Wrapper<T> {
    inner: T,
}

fn main() {}
