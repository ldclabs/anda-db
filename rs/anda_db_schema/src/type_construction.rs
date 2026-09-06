//! Compatibility bridge used by generated code. New derives expose a
//! fallible constructor; handwritten legacy field_type methods still work.
use crate::{FieldType, MAX_CONVERSION_DEPTH, SchemaError};
use std::{cell::RefCell, marker::PhantomData};

thread_local! {
    static BUILDING: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
}

pub trait FallibleFieldTyped {
    fn try_field_type() -> Result<FieldType, SchemaError>;
}

pub struct TypeProbe<T: ?Sized>(PhantomData<*const T>);

impl<T: ?Sized> TypeProbe<T> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T: ?Sized> Default for TypeProbe<T> {
    fn default() -> Self {
        Self::new()
    }
}

pub trait ResolveFieldType {
    fn resolve(self, fallback: impl FnOnce() -> FieldType) -> Result<FieldType, SchemaError>;
}

// Autoref fallback preserves user-defined inherent methods and generic
// bounds that predate the fallible derive protocol.
impl<T: ?Sized> ResolveFieldType for &TypeProbe<T> {
    fn resolve(self, fallback: impl FnOnce() -> FieldType) -> Result<FieldType, SchemaError> {
        Ok(fallback())
    }
}

impl<T: FallibleFieldTyped + ?Sized> ResolveFieldType for TypeProbe<T> {
    fn resolve(self, _: impl FnOnce() -> FieldType) -> Result<FieldType, SchemaError> {
        T::try_field_type()
    }
}

pub fn build<T: ?Sized>(
    f: impl FnOnce() -> Result<FieldType, SchemaError>,
) -> Result<FieldType, SchemaError> {
    let name = std::any::type_name::<T>();
    let outermost = BUILDING.with(|stack| {
        let mut stack = stack.borrow_mut();
        if stack.contains(&name) || stack.len() >= MAX_CONVERSION_DEPTH {
            return Err(SchemaError::FieldType(format!("recursive or excessively nested field type {name}; use an explicit non-recursive field_type override")));
        }
        let outermost = stack.is_empty();
        stack.push(name);
        Ok(outermost)
    })?;
    struct Guard;
    impl Drop for Guard {
        fn drop(&mut self) {
            BUILDING.with(|stack| {
                stack.borrow_mut().pop();
            });
        }
    }
    let _guard = Guard;
    let value = f()?;
    if outermost {
        value.validate_declaration()?;
    }
    Ok(value)
}
