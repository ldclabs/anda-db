//! A bare generic field whose parameter is bound by a trait named
//! `FieldTyped` resolves through `<T>::field_type()` — the pre-0.9.2
//! behavior for monomorphized generic fields, restored.
use anda_db_schema::{FieldKey, FieldType};

/// A user-defined trait that supplies the schema fragment for bound generic
/// parameters. Detection is by trait name (`FieldTyped`), any path prefix.
pub trait FieldTyped {
    fn field_type() -> FieldType;
}

impl FieldTyped for u64 {
    fn field_type() -> FieldType {
        FieldType::U64
    }
}

#[derive(anda_db_derive::FieldTyped)]
struct Wrapper<T: FieldTyped> {
    inner: T,
    label: String,
}

#[derive(anda_db_derive::FieldTyped)]
struct WhereWrapper<T>
where
    T: FieldTyped,
{
    // Nested occurrences of the parameter resolve through the bound as well.
    items: Vec<T>,
}

fn main() {
    let ft = Wrapper::<u64>::field_type();
    match ft {
        FieldType::Map(map) => {
            assert_eq!(map.get(&FieldKey::from("inner")), Some(&FieldType::U64));
            assert_eq!(map.get(&FieldKey::from("label")), Some(&FieldType::Text));
        }
        other => panic!("expected FieldType::Map, got {other:?}"),
    }

    let ft = WhereWrapper::<u64>::field_type();
    match ft {
        FieldType::Map(map) => {
            assert_eq!(
                map.get(&FieldKey::from("items")),
                Some(&FieldType::Array(vec![FieldType::U64]))
            );
        }
        other => panic!("expected FieldType::Map, got {other:?}"),
    }
}
