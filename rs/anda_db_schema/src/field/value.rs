//! Runtime values, conversions and bounded JSON/CBOR adapters.
use super::*;
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::{BTreeSet, HashMap, HashSet, btree_map::Entry},
    fmt,
};
/// The runtime value of a field.
///
/// Each variant corresponds 1:1 to a [`FieldType`] variant, with the
/// addition of [`FieldValue::Null`] which represents an absent value for
/// [`FieldType::Option`]. Values convert to and from CBOR via
/// [`From<FieldValue> for Cbor`] and [`FieldValue::try_from`]. The *data*
/// is preserved, but an untyped round trip normalizes some variants
/// (`F32` → `F64`, non-negative `I64` → `U64`, `Vector` → `Array(U64)`,
/// `Json` → `Map`/primitive); pairing the CBOR with a [`FieldType`] via
/// [`FieldType::extract`] restores the declared variant, and
/// [`FieldType::validate`] accepts the normalized read-back shapes.
///
/// `FieldValue` derives `PartialEq`, but float values are required to be
/// non-NaN (we don't enforce this in the type system, but it is
/// enforced by [`FieldValue::f64_from`] / [`FieldValue::f32_from`]
/// when extracting from CBOR) so that equality is reflexive in practice.
#[derive(Clone, PartialEq)]
pub enum FieldValue {
    /// Boolean value
    Bool(bool),
    /// Signed 64-bit integer value
    I64(i64),
    /// Unsigned 64-bit integer value
    U64(u64),
    /// 64-bit floating point value
    F64(f64),
    /// 32-bit floating point value
    F32(f32),
    /// Binary data value
    Bytes(Vec<u8>),
    /// UTF-8 encoded text value
    Text(String),
    /// JSON value
    Json(Json),
    /// `Vec<bf16>`, bf16: 16-bit floating point type implementing the bfloat16 format.
    /// Detail: <https://docs.rs/half/latest/half/struct.bf16.html>
    Vector(Vec<bf16>),
    /// Array of field values
    Array(Vec<FieldValue>),
    /// Map with typed field keys and field values
    Map(BTreeMap<FieldKey, FieldValue>),
    /// Null value (for optional fields)
    Null,
}

impl From<FieldValue> for Cbor {
    /// Convert a FieldValue to a CBOR value
    ///
    /// This conversion is infallible and therefore cannot report a value
    /// nested deeper than [`MAX_CONVERSION_DEPTH`]: the offending subtree is
    /// truncated to [`Cbor::Null`] rather than recursing until the stack is
    /// exhausted. Use [`FieldValue::try_into_cbor`] to get the same
    /// [`SchemaError::FieldValue`] the opposite direction
    /// ([`FieldValue::try_from`]) returns.
    ///
    /// # Arguments
    /// * `value` - The FieldValue to convert
    ///
    /// # Returns
    /// * `Cbor` - The converted CBOR value
    fn from(value: FieldValue) -> Self {
        // The best-effort path truncates instead of erroring, so the
        // fallback is unreachable.
        field_value_to_cbor(value, 0, false).unwrap_or(Cbor::Null)
    }
}

/// Depth-tracked body of the [`FieldValue`] → [`Cbor`] conversion.
///
/// Beyond [`MAX_CONVERSION_DEPTH`] `strict` decides the outcome: the fallible
/// entry point ([`FieldValue::try_into_cbor`]) reports the overflow, while the
/// infallible [`From`] impl truncates the over-deep subtree to [`Cbor::Null`].
pub(super) fn field_value_to_cbor(
    value: FieldValue,
    depth: usize,
    strict: bool,
) -> Result<Cbor, SchemaError> {
    if let Err(err) = check_conversion_depth(depth) {
        return if strict { Err(err) } else { Ok(Cbor::Null) };
    }

    Ok(match value {
        FieldValue::Bool(b) => Cbor::Bool(b),
        FieldValue::I64(i) => Cbor::Integer(i.into()),
        FieldValue::U64(u) => Cbor::Integer(u.into()),
        FieldValue::F64(f) => Cbor::Float(f),
        FieldValue::F32(f) => Cbor::Float(f as f64),
        FieldValue::Bytes(b) => Cbor::Bytes(b),
        FieldValue::Text(t) => Cbor::Text(t),
        // The JSON payload carries its own nesting, counted from here on.
        FieldValue::Json(obj) => json_to_cbor_at(obj, depth, strict)?,
        FieldValue::Vector(arr) => {
            Cbor::Array(arr.into_iter().map(|f| f.to_bits().into()).collect())
        }
        FieldValue::Array(arr) => Cbor::Array(
            arr.into_iter()
                .map(|v| field_value_to_cbor(v, depth + 1, strict))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        FieldValue::Map(obj) => {
            let mut entries = Vec::with_capacity(obj.len());
            for (k, v) in obj {
                entries.push((
                    match k {
                        FieldKey::Text(s) => Cbor::Text(s),
                        FieldKey::I64(i) => Cbor::Integer(i.into()),
                        FieldKey::Bytes(b) => Cbor::Bytes(b),
                    },
                    field_value_to_cbor(v, depth + 1, strict)?,
                ));
            }
            Cbor::Map(entries)
        }

        FieldValue::Null => Cbor::Null,
    })
}

// Only the mechanical one-variant conversions share a macro. Numeric
// coercion, fallible conversion and ownership-sensitive reads stay explicit.
macro_rules! impl_from_value {
    ($($ty:ty => $variant:ident),* $(,)?) => {$(
        impl From<$ty> for FieldValue {
            fn from(value: $ty) -> Self { FieldValue::$variant(value) }
        }
    )*};
}

impl_from_value! {
    bool => Bool, i64 => I64, u64 => U64, f64 => F64, f32 => F32,
    Vec<u8> => Bytes, String => Text, Json => Json, Vec<bf16> => Vector,
}

impl<T> From<Vec<T>> for FieldValue
where
    T: Into<FieldValue>,
{
    fn from(val: Vec<T>) -> Self {
        FieldValue::Array(val.into_iter().map(|v| v.into()).collect())
    }
}

impl<T> From<BTreeSet<T>> for FieldValue
where
    T: Into<FieldValue>,
{
    fn from(val: BTreeSet<T>) -> Self {
        FieldValue::Array(val.into_iter().map(|v| v.into()).collect())
    }
}

impl<T> From<HashSet<T>> for FieldValue
where
    T: Into<FieldValue>,
{
    fn from(val: HashSet<T>) -> Self {
        FieldValue::Array(val.into_iter().map(|v| v.into()).collect())
    }
}

impl<K, V> From<BTreeMap<K, V>> for FieldValue
where
    K: Into<FieldKey>,
    V: Into<FieldValue>,
{
    fn from(obj: BTreeMap<K, V>) -> Self {
        FieldValue::Map(obj.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
    }
}

impl<K, V> From<HashMap<K, V>> for FieldValue
where
    K: Into<FieldKey>,
    V: Into<FieldValue>,
{
    fn from(obj: HashMap<K, V>) -> Self {
        FieldValue::Map(obj.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
    }
}

impl From<serde_json::Map<String, Json>> for FieldValue {
    fn from(obj: serde_json::Map<String, Json>) -> Self {
        FieldValue::Map(obj.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
    }
}

impl From<FieldKey> for FieldValue {
    fn from(key: FieldKey) -> Self {
        match key {
            FieldKey::Text(s) => FieldValue::Text(s),
            FieldKey::I64(i) => FieldValue::I64(i),
            FieldKey::Bytes(b) => FieldValue::Bytes(b),
        }
    }
}

impl TryFrom<FieldValue> for bool {
    type Error = BoxError;

    fn try_from(value: FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::Bool(v) => Ok(v),
            _ => Err(SchemaError::FieldValue(format!("expected Bool, got {value:?}")).into()),
        }
    }
}

impl<'a> TryFrom<&'a FieldValue> for bool {
    type Error = BoxError;

    fn try_from(value: &'a FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::Bool(v) => Ok(*v),
            _ => Err(SchemaError::FieldValue(format!("expected Bool, got {value:?}")).into()),
        }
    }
}

impl TryFrom<FieldValue> for i64 {
    type Error = BoxError;

    fn try_from(value: FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::I64(v) => Ok(v),
            // Read-back shape: a non-negative I64 comes back as U64 through
            // generic CBOR (see `FieldType::validate`).
            FieldValue::U64(v) if v <= i64::MAX as u64 => Ok(v as i64),
            _ => Err(SchemaError::FieldValue(format!("expected I64, got {value:?}")).into()),
        }
    }
}

impl<'a> TryFrom<&'a FieldValue> for i64 {
    type Error = BoxError;

    fn try_from(value: &'a FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::I64(v) => Ok(*v),
            // Read-back shape: a non-negative I64 comes back as U64 through
            // generic CBOR (see `FieldType::validate`).
            FieldValue::U64(v) if *v <= i64::MAX as u64 => Ok(*v as i64),
            _ => Err(SchemaError::FieldValue(format!("expected I64, got {value:?}")).into()),
        }
    }
}

impl<'a> TryFrom<&'a FieldValue> for &'a i64 {
    type Error = BoxError;

    fn try_from(value: &'a FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::I64(v) => Ok(v),
            _ => Err(SchemaError::FieldValue(format!("expected I64, got {value:?}")).into()),
        }
    }
}

impl TryFrom<FieldValue> for u64 {
    type Error = BoxError;

    fn try_from(value: FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::U64(v) => Ok(v),
            _ => Err(SchemaError::FieldValue(format!("expected U64, got {value:?}")).into()),
        }
    }
}

impl<'a> TryFrom<&'a FieldValue> for u64 {
    type Error = BoxError;

    fn try_from(value: &'a FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::U64(v) => Ok(*v),
            _ => Err(SchemaError::FieldValue(format!("expected U64, got {value:?}")).into()),
        }
    }
}

impl<'a> TryFrom<&'a FieldValue> for &'a u64 {
    type Error = BoxError;

    fn try_from(value: &'a FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::U64(v) => Ok(v),
            _ => Err(SchemaError::FieldValue(format!("expected U64, got {value:?}")).into()),
        }
    }
}

impl TryFrom<FieldValue> for f64 {
    type Error = BoxError;

    fn try_from(value: FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::F64(v) => Ok(v),
            // JSON integer for a float field (see `FieldType::validate`).
            FieldValue::I64(v) => Ok(v as f64),
            FieldValue::U64(v) => Ok(v as f64),
            _ => Err(SchemaError::FieldValue(format!("expected F64, got {value:?}")).into()),
        }
    }
}

impl<'a> TryFrom<&'a FieldValue> for f64 {
    type Error = BoxError;

    fn try_from(value: &'a FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::F64(v) => Ok(*v),
            // JSON integer for a float field (see `FieldType::validate`).
            FieldValue::I64(v) => Ok(*v as f64),
            FieldValue::U64(v) => Ok(*v as f64),
            _ => Err(SchemaError::FieldValue(format!("expected F64, got {value:?}")).into()),
        }
    }
}

impl TryFrom<FieldValue> for f32 {
    type Error = BoxError;

    fn try_from(value: FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::F32(v) => Ok(v),
            // Read-back shape: an F32 comes back as an F64 through generic
            // CBOR or JSON (see `FieldType::validate` / `is_f32_read_back`).
            FieldValue::F64(v) if is_f32_read_back(v) => Ok(v as f32),
            // JSON integer for a float field, only when it is exact (see
            // `FieldType::validate`).
            FieldValue::I64(v) if exact_f32_from_integer(v as i128).is_some() => Ok(v as f32),
            FieldValue::U64(v) if exact_f32_from_integer(v as i128).is_some() => Ok(v as f32),
            _ => Err(SchemaError::FieldValue(format!("expected F32, got {value:?}")).into()),
        }
    }
}

impl<'a> TryFrom<&'a FieldValue> for f32 {
    type Error = BoxError;

    fn try_from(value: &'a FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::F32(v) => Ok(*v),
            // Read-back shape: an F32 comes back as an F64 through generic
            // CBOR or JSON (see `FieldType::validate` / `is_f32_read_back`).
            FieldValue::F64(v) if is_f32_read_back(*v) => Ok(*v as f32),
            // JSON integer for a float field, only when it is exact (see
            // `FieldType::validate`).
            FieldValue::I64(v) if exact_f32_from_integer(*v as i128).is_some() => Ok(*v as f32),
            FieldValue::U64(v) if exact_f32_from_integer(*v as i128).is_some() => Ok(*v as f32),
            _ => Err(SchemaError::FieldValue(format!("expected F32, got {value:?}")).into()),
        }
    }
}

impl TryFrom<FieldValue> for Vec<u8> {
    type Error = BoxError;

    fn try_from(value: FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::Bytes(v) => Ok(v),
            _ => Err(SchemaError::FieldValue(format!("expected Bytes, got {value:?}")).into()),
        }
    }
}

impl<'a> TryFrom<&'a FieldValue> for &'a Vec<u8> {
    type Error = BoxError;

    fn try_from(value: &'a FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::Bytes(v) => Ok(v),
            _ => Err(SchemaError::FieldValue(format!("expected Bytes, got {value:?}")).into()),
        }
    }
}

impl<'a> TryFrom<&'a FieldValue> for &'a [u8] {
    type Error = BoxError;

    fn try_from(value: &'a FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::Bytes(v) => Ok(v),
            _ => Err(SchemaError::FieldValue(format!("expected Bytes, got {value:?}")).into()),
        }
    }
}

impl<const N: usize> TryFrom<FieldValue> for [u8; N] {
    type Error = BoxError;

    fn try_from(value: FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::Bytes(v) => Ok(v.try_into().map_err(|v: Vec<u8>| {
                SchemaError::FieldValue(format!("expected {N} bytes, got {}", v.len()))
            })?),
            _ => Err(SchemaError::FieldValue(format!("expected Bytes, got {value:?}")).into()),
        }
    }
}

impl TryFrom<FieldValue> for String {
    type Error = BoxError;

    fn try_from(value: FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::Text(v) => Ok(v),
            _ => Err(SchemaError::FieldValue(format!("expected Text, got {value:?}")).into()),
        }
    }
}

impl<'a> TryFrom<&'a FieldValue> for &'a String {
    type Error = BoxError;

    fn try_from(value: &'a FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::Text(v) => Ok(v),
            _ => Err(SchemaError::FieldValue(format!("expected Text, got {value:?}")).into()),
        }
    }
}

impl<'a> TryFrom<&'a FieldValue> for &'a str {
    type Error = BoxError;

    fn try_from(value: &'a FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::Text(v) => Ok(v),
            _ => Err(SchemaError::FieldValue(format!("expected Text, got {value:?}")).into()),
        }
    }
}

impl TryFrom<FieldValue> for Json {
    type Error = BoxError;

    fn try_from(value: FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::Json(v) => Ok(v),
            _ => Err(SchemaError::FieldValue(format!("expected Json, got {value:?}")).into()),
        }
    }
}

impl<'a> TryFrom<&'a FieldValue> for &'a Json {
    type Error = BoxError;

    fn try_from(value: &'a FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::Json(v) => Ok(v),
            _ => Err(SchemaError::FieldValue(format!("expected Json, got {value:?}")).into()),
        }
    }
}

impl TryFrom<FieldValue> for Vec<bf16> {
    type Error = BoxError;

    fn try_from(value: FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::Vector(v) => Ok(v),
            // Read-back shape: a Vector comes back as an Array of bf16 bit
            // patterns through generic CBOR (see `FieldType::validate`).
            FieldValue::Array(arr)
                if arr
                    .iter()
                    .all(|v| matches!(v, FieldValue::U64(u) if *u <= u16::MAX as u64)) =>
            {
                Ok(arr
                    .into_iter()
                    .map(|v| match v {
                        FieldValue::U64(u) => bf16::from_bits(u as u16),
                        _ => unreachable!("checked above"),
                    })
                    .collect())
            }
            _ => Err(SchemaError::FieldValue(format!("expected Vector, got {value:?}")).into()),
        }
    }
}

impl<'a> TryFrom<&'a FieldValue> for &'a Vec<bf16> {
    type Error = BoxError;

    fn try_from(value: &'a FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::Vector(v) => Ok(v),
            _ => Err(SchemaError::FieldValue(format!("expected Vector, got {value:?}")).into()),
        }
    }
}

impl<const N: usize> TryFrom<FieldValue> for [bf16; N] {
    type Error = BoxError;

    fn try_from(value: FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::Vector(v) => Ok(v.try_into().map_err(|v: Vec<bf16>| {
                SchemaError::FieldValue(format!("expected {N} elements, got {}", v.len()))
            })?),
            _ => Err(SchemaError::FieldValue(format!("expected Vector, got {value:?}")).into()),
        }
    }
}

impl<T> TryFrom<FieldValue> for Vec<T>
where
    T: TryFrom<FieldValue, Error = BoxError>,
{
    type Error = BoxError;

    fn try_from(value: FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::Array(arr) => {
                let mut rt = Vec::with_capacity(arr.len());
                for v in arr {
                    rt.push(v.try_into()?);
                }
                Ok(rt)
            }
            _ => Err(SchemaError::FieldValue(format!("expected Array, got {value:?}")).into()),
        }
    }
}

impl<'a, T> TryFrom<&'a FieldValue> for Vec<&'a T>
where
    &'a T: TryFrom<&'a FieldValue, Error = BoxError>,
{
    type Error = BoxError;

    fn try_from(value: &'a FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::Array(arr) => {
                let mut rt = Vec::with_capacity(arr.len());
                for v in arr {
                    rt.push(v.try_into()?);
                }
                Ok(rt)
            }
            _ => Err(SchemaError::FieldValue(format!("expected Array, got {value:?}")).into()),
        }
    }
}

impl<T> TryFrom<FieldValue> for BTreeMap<FieldKey, T>
where
    T: TryFrom<FieldValue, Error = BoxError>,
{
    type Error = BoxError;

    fn try_from(value: FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::Map(map) => {
                let mut rt = BTreeMap::new();
                for (k, v) in map {
                    rt.insert(k, v.try_into()?);
                }
                Ok(rt)
            }
            _ => Err(SchemaError::FieldValue(format!("expected Map, got {value:?}")).into()),
        }
    }
}

impl fmt::Debug for FieldValue {
    /// Debug formatting for FieldValue
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldValue::Bool(v) => write!(f, "Bool({v})"),
            FieldValue::I64(v) => write!(f, "I64({v})"),
            FieldValue::U64(v) => write!(f, "U64({v})"),
            FieldValue::F64(v) => write!(f, "F64({v})"),
            FieldValue::F32(v) => write!(f, "F32({v})"),
            FieldValue::Bytes(v) => write!(f, "Bytes({v:?})"),
            FieldValue::Text(v) => write!(f, "Text({v:?})"),
            FieldValue::Json(v) => write!(f, "Json({v:?})"),
            FieldValue::Vector(v) => write!(f, "Vector({v:?})"),
            FieldValue::Array(v) => write!(f, "Array({v:?})"),
            FieldValue::Map(v) => write!(f, "Map({v:?})"),
            FieldValue::Null => write!(f, "Null"),
        }
    }
}

impl FieldValue {
    /// Converts this value into CBOR, rejecting containers nested deeper than
    /// [`MAX_CONVERSION_DEPTH`].
    ///
    /// This is the bounded counterpart of the infallible
    /// [`From<FieldValue> for Cbor`] impl (which truncates instead of
    /// reporting) and the mirror image of [`FieldValue::try_from`]: both
    /// directions now fail with the same [`SchemaError::FieldValue`] rather
    /// than recursing until the stack is exhausted. `FieldValue` can carry
    /// nesting up to the *format's* limit (`serde_json` 128, `cbor2` 256),
    /// which is above the [`FieldValueBudget::max_depth`] validation applies.
    pub fn try_into_cbor(self) -> Result<Cbor, SchemaError> {
        field_value_to_cbor(self, 0, true)
    }

    /// Create a Bool FieldValue from a CBOR value
    ///
    /// # Arguments
    /// * `value` - The CBOR value to convert
    ///
    /// # Returns
    /// * `Result<Self, SchemaError>` - The converted FieldValue or an error message
    pub fn bool_from(value: Cbor) -> Result<Self, SchemaError> {
        match value {
            Cbor::Bool(b) => Ok(FieldValue::Bool(b)),
            v => Err(SchemaError::FieldValue(format!("expected Bool, got {v:?}"))),
        }
    }

    /// Create an I64 FieldValue from a CBOR value
    ///
    /// # Arguments
    /// * `value` - The CBOR value to convert
    ///
    /// # Returns
    /// * `Result<Self, SchemaError>` - The converted FieldValue or an error message
    pub fn i64_from(value: Cbor) -> Result<Self, SchemaError> {
        match value {
            Cbor::Integer(i) => {
                Ok(FieldValue::I64(i.try_into().map_err(|v| {
                    SchemaError::FieldValue(format!("expected I64, got {v:?}"))
                })?))
            }
            v => Err(SchemaError::FieldValue(format!("expected I64, got {v:?}"))),
        }
    }

    /// Create a U64 FieldValue from a CBOR value
    ///
    /// # Arguments
    /// * `value` - The CBOR value to convert
    ///
    /// # Returns
    /// * `Result<Self, SchemaError>` - The converted FieldValue or an error message
    pub fn u64_from(value: Cbor) -> Result<Self, SchemaError> {
        match value {
            Cbor::Integer(i) => {
                Ok(FieldValue::U64(i.try_into().map_err(|v| {
                    SchemaError::FieldValue(format!("expected U64, got {v:?}"))
                })?))
            }
            v => Err(SchemaError::FieldValue(format!("expected U64, got {v:?}"))),
        }
    }

    /// Create an F64 FieldValue from a CBOR value
    ///
    /// CBOR integers are accepted as well: JSON has a single number type, so
    /// `1.0` arrives as the integer `1`. The conversion is serde's own
    /// (`as f64`) — exact up to 2^53, rounded beyond.
    ///
    /// # Arguments
    /// * `value` - The CBOR value to convert
    ///
    /// # Returns
    /// * `Result<Self, SchemaError>` - The converted FieldValue or an error message
    pub fn f64_from(value: Cbor) -> Result<Self, SchemaError> {
        match value {
            Cbor::Float(f) if !f.is_nan() => Ok(FieldValue::F64(f)),
            Cbor::Integer(i) => Ok(FieldValue::F64(integer_to_f64(i))),
            v => Err(SchemaError::FieldValue(format!("expected F64, got {v:?}"))),
        }
    }

    /// Create an F32 FieldValue from a CBOR value
    ///
    /// Precision truncation (f64 → f32) is accepted, but a finite value
    /// outside the f32 range is rejected instead of silently becoming
    /// infinite. Explicit infinities pass through unchanged. A CBOR integer
    /// is accepted only when an `f32` holds it exactly (see
    /// `exact_f32_from_integer`) — unlike [`FieldValue::f64_from`], which
    /// takes any integer, because `F32` also rejects the `F64` spelling of a
    /// value no `f32` can hold.
    ///
    /// # Arguments
    /// * `value` - The CBOR value to convert
    ///
    /// # Returns
    /// * `Result<Self, SchemaError>` - The converted FieldValue or an error message
    pub fn f32_from(value: Cbor) -> Result<Self, SchemaError> {
        match value {
            Cbor::Float(f) if !f.is_nan() => {
                let v = f as f32;
                if v.is_infinite() && f.is_finite() {
                    return Err(SchemaError::FieldValue(format!(
                        "expected F32, got out-of-range F64 {f:?}"
                    )));
                }
                Ok(FieldValue::F32(v))
            }
            Cbor::Integer(i) => {
                let i = i128::from(i);
                exact_f32_from_integer(i)
                    .map(FieldValue::F32)
                    .ok_or_else(|| {
                        SchemaError::FieldValue(format!(
                            "expected F32, got integer {i} that no f32 holds exactly"
                        ))
                    })
            }
            v => Err(SchemaError::FieldValue(format!("expected F32, got {v:?}"))),
        }
    }

    /// Create a Bytes FieldValue from a CBOR value
    ///
    /// Besides CBOR byte strings, a CBOR array whose elements are all
    /// integers in `0..=255` is accepted and coerced into bytes. This is the
    /// shape `Vec<u8>` / `[u8; N]` struct fields produce through serde: the
    /// generic `Vec<T>` and array serializers emit an integer sequence, not a
    /// byte string, so a `FieldType::Bytes` field must accept both.
    ///
    /// # Arguments
    /// * `value` - The CBOR value to convert
    ///
    /// # Returns
    /// * `Result<Self, SchemaError>` - The converted FieldValue or an error message
    pub fn bytes_from(value: Cbor) -> Result<Self, SchemaError> {
        match value {
            Cbor::Bytes(b) => Ok(FieldValue::Bytes(b)),
            Cbor::Array(arr) => Ok(FieldValue::Bytes(u8_array_from(arr)?)),
            v => Err(SchemaError::FieldValue(format!(
                "expected Bytes, got {v:?}"
            ))),
        }
    }

    /// Create a Text FieldValue from a CBOR value
    ///
    /// # Arguments
    /// * `value` - The CBOR value to convert
    ///
    /// # Returns
    /// * `Result<Self, SchemaError>` - The converted FieldValue or an error message
    pub fn text_from(value: Cbor) -> Result<Self, SchemaError> {
        match value {
            Cbor::Text(t) => Ok(FieldValue::Text(t)),
            v => Err(SchemaError::FieldValue(format!("expected Text, got {v:?}"))),
        }
    }

    /// Create a Json FieldValue from a CBOR value
    ///
    /// # Arguments
    /// * `value` - The CBOR value to convert
    ///
    /// # Returns
    /// * `Result<Self, SchemaError>` - The converted FieldValue or an error message
    pub fn json_from(value: Cbor) -> Result<Self, SchemaError> {
        Ok(FieldValue::Json(cbor_into_json(value, 0)?))
    }

    /// Create a Vector FieldValue from a CBOR value
    ///
    /// # Arguments
    /// * `value` - The CBOR value to convert
    ///
    /// # Returns
    /// * `Result<Self, SchemaError>` - The converted FieldValue or an error message
    pub fn vector_from(value: Cbor) -> Result<Self, SchemaError> {
        match value {
            Cbor::Array(arr) => {
                let mut vector = Vec::with_capacity(arr.len());
                for value in arr {
                    vector.push(Self::bf16_from(value)?);
                }
                Ok(FieldValue::Vector(vector))
            }
            v => Err(SchemaError::FieldValue(format!(
                "expected Vector, got {v:?}"
            ))),
        }
    }

    /// Create a bf16 from a CBOR value
    ///
    /// # Arguments
    /// * `value` - The CBOR value to convert
    ///
    /// # Returns
    /// * `Result<Self, SchemaError>` - The converted FieldValue or an error message
    pub fn bf16_from(value: Cbor) -> Result<bf16, SchemaError> {
        match value {
            Cbor::Integer(i) => {
                Ok(bf16::from_bits(i.try_into().map_err(|v| {
                    SchemaError::FieldValue(format!("expected u16, got {v:?}"))
                })?))
            }
            v => Err(SchemaError::FieldValue(format!("expected bf16, got {v:?}"))),
        }
    }

    /// Create an Array FieldValue from a CBOR value
    ///
    /// # Arguments
    /// * `value` - The CBOR value to convert
    /// * `types` - The field types for the array elements
    ///
    /// # Returns
    /// * `Result<Self, SchemaError>` - The converted FieldValue or an error message
    pub fn array_from(value: Cbor, types: &[FieldType]) -> Result<Self, SchemaError> {
        Self::array_from_at(value, types, 0)
    }

    /// Depth-tracked body of [`FieldValue::array_from`]; see
    /// [`MAX_CONVERSION_DEPTH`].
    pub(super) fn array_from_at(
        value: Cbor,
        types: &[FieldType],
        depth: usize,
    ) -> Result<Self, SchemaError> {
        check_conversion_depth(depth)?;

        match value {
            Cbor::Array(values) => match types.len() {
                0 => Ok(FieldValue::Array(
                    values
                        .into_iter()
                        .map(|v| FieldValue::try_from_at(v, depth + 1))
                        .collect::<Result<Vec<_>, _>>()?,
                )),
                1 => {
                    let ft = types.first().unwrap();
                    Ok(FieldValue::Array(
                        values
                            .into_iter()
                            .map(|v| ft.extract_at(v, depth + 1))
                            .collect::<Result<Vec<_>, _>>()?,
                    ))
                }
                _ => {
                    if types.len() != values.len() {
                        return Err(SchemaError::FieldValue(format!(
                            "Invalid array length, expected {:?}, got {:?}",
                            types.len(),
                            values.len()
                        )));
                    }

                    let mut rt: Vec<FieldValue> = Vec::with_capacity(types.len());
                    for (ft, val) in types.iter().zip(values) {
                        rt.push(ft.extract_at(val, depth + 1)?);
                    }

                    Ok(FieldValue::Array(rt))
                }
            },
            v => Err(SchemaError::FieldValue(format!(
                "expected Array, got {v:?}"
            ))),
        }
    }

    /// Create a Map FieldValue from a CBOR value
    ///
    /// # Arguments
    /// * `value` - The CBOR value to convert
    /// * `types` - The field types for the map values, keyed by field name
    ///
    /// # Returns
    /// * `Result<Self, SchemaError>` - The converted FieldValue or an error message
    pub fn map_from(
        value: Cbor,
        types: &BTreeMap<FieldKey, FieldType>,
    ) -> Result<Self, SchemaError> {
        Self::map_from_at(value, types, 0)
    }

    /// Depth-tracked body of [`FieldValue::map_from`]; see
    /// [`MAX_CONVERSION_DEPTH`].
    pub(super) fn map_from_at(
        value: Cbor,
        types: &BTreeMap<FieldKey, FieldType>,
        depth: usize,
    ) -> Result<Self, SchemaError> {
        check_conversion_depth(depth)?;

        match value {
            Cbor::Map(values) => {
                let wildcard_map = as_wildcard_map(types);

                let mut vals: BTreeMap<FieldKey, FieldValue> = BTreeMap::new();
                for (k, v) in values {
                    let k: FieldKey = k.try_into().map_err(|err| {
                        SchemaError::FieldValue(format!("invalid map key: {err:?}"))
                    })?;

                    let v = if types.is_empty() {
                        FieldValue::try_from_at(v, depth + 1)?
                    } else if let Some((wildcard, ft)) = wildcard_map {
                        // The sentinel pins the key variant too, otherwise a
                        // `Map<Text, T>` could be filled with integer keys and
                        // never read back into its declared Rust type.
                        check_wildcard_key(&k, wildcard)?;
                        ft.extract_at(v, depth + 1)?
                    } else if let Some(ft) = types.get(&k) {
                        ft.extract_at(v, depth + 1)?
                    } else {
                        return Err(SchemaError::FieldValue(format!("invalid map key {k:?}")));
                    };

                    match vals.entry(k) {
                        Entry::Vacant(e) => {
                            e.insert(v);
                        }
                        Entry::Occupied(e) => {
                            return Err(SchemaError::FieldValue(format!(
                                "duplicate map key {:?}",
                                e.key()
                            )));
                        }
                    }
                }

                // `extract` already guarantees that every present value conforms to
                // its declared type, so only missing required keys remain to check.
                if !types.is_empty() && wildcard_map.is_none() {
                    for (k, ft) in types {
                        if !vals.contains_key(k) && !ft.allows_null() {
                            return Err(SchemaError::FieldValue(format!(
                                "required map key {k:?} is missing"
                            )));
                        }
                    }
                }
                Ok(FieldValue::Map(vals))
            }
            v => Err(SchemaError::FieldValue(format!("expected Map, got {v:?}"))),
        }
    }

    /// Try to create a FieldValue from a CBOR value, inferring the type
    ///
    /// # Arguments
    /// * `value` - The CBOR value to convert
    ///
    /// # Returns
    /// * `Result<Self, SchemaError>` - The converted FieldValue or an error message
    pub fn try_from(value: Cbor) -> Result<Self, SchemaError> {
        Self::try_from_at(value, 0)
    }

    /// Depth-tracked body of [`FieldValue::try_from`]; see
    /// [`MAX_CONVERSION_DEPTH`].
    pub(super) fn try_from_at(value: Cbor, depth: usize) -> Result<Self, SchemaError> {
        check_conversion_depth(depth)?;

        match value {
            Cbor::Bool(_) => Self::bool_from(value),
            Cbor::Integer(i) => {
                let z = cbor2::value::Integer::from(0);
                if i >= z {
                    Self::u64_from(value)
                } else {
                    Self::i64_from(value)
                }
            }
            Cbor::Float(_) => Self::f64_from(value),
            Cbor::Bytes(_) => Self::bytes_from(value),
            Cbor::Text(_) => Self::text_from(value),
            Cbor::Array(_) => Self::array_from_at(value, &[], depth),
            Cbor::Map(_) => Self::map_from_at(value, &BTreeMap::new(), depth),
            Cbor::Null => Ok(FieldValue::Null),
            Cbor::Tag(_, val) => Self::try_from_at(*val, depth + 1),
            v => Err(SchemaError::FieldValue(format!(
                "invalid CBOR value: {v:?}"
            ))),
        }
    }

    /// Create a FieldValue by serializing a value
    ///
    /// # Arguments
    /// * `value` - The value to serialize
    /// * `ft` - Optional field type to use for extraction
    ///
    /// # Returns
    /// * `Result<Self, SchemaError>` - The serialized FieldValue or an error message
    pub fn serialized<T: ?Sized + Serialize>(
        value: &T,
        ft: Option<&FieldType>,
    ) -> Result<Self, SchemaError> {
        let rt = Cbor::serialized(value)
            .map_err(|v| SchemaError::FieldValue(format!("failed to serialize: {v:?}")))?;
        match ft {
            Some(ft) => ft.extract(rt),
            None => FieldValue::try_from(rt),
        }
    }

    /// Deserialize a FieldValue into a value of type T
    ///
    /// The value is re-encoded as a CBOR byte stream and decoded from it:
    /// the streaming decoder bridges CBOR byte strings into serde sequences,
    /// which is required to deserialize [`FieldValue::Bytes`] into `Vec<u8>`
    /// or `[u8; N]`.
    ///
    /// # Returns
    /// * `Result<T, SchemaError>` - The deserialized value or an error message
    pub fn deserialized<T: DeserializeOwned>(&self) -> Result<T, SchemaError> {
        let mut buf = Vec::with_capacity(128);
        cbor2::to_writer(self, &mut buf)
            .map_err(|v| SchemaError::FieldValue(format!("Failed to serialize: {v:?}")))?;
        cbor2::from_reader(&buf[..])
            .map_err(|v| SchemaError::FieldValue(format!("Failed to deserialize: {v:?}")))
    }

    /// Get a field value from a map as a reference T
    ///
    /// # Arguments
    /// * `field` - The field name to look up
    ///
    /// # Returns
    /// * `Option<&T>` - The field value if found and convertible, None otherwise
    pub fn get_field_as<'a, T: ?Sized>(&'a self, field: &FieldKey) -> Option<&'a T>
    where
        &'a T: TryFrom<&'a FieldValue>,
    {
        if let Fv::Map(m) = self
            && let Some(v) = m.get(field)
        {
            return v.try_into().ok();
        }
        None
    }
}

/// Convert a `Vec<f32>` into a [`Vector`] (i.e. `Vec<bf16>`) by lossy
/// conversion of every element.
pub fn vector_from_f32(v: Vec<f32>) -> Vector {
    v.into_iter().map(bf16::from_f32).collect()
}

/// Convert a `Vec<f64>` into a [`Vector`] (i.e. `Vec<bf16>`) by lossy
/// conversion of every element.
pub fn vector_from_f64(v: Vec<f64>) -> Vector {
    v.into_iter().map(bf16::from_f64).collect()
}

/// Converts a JSON value into a CBOR value.
///
/// Every JSON value has a CBOR representation, so the mapping itself needs no
/// error path. Nesting does: `depth`/`strict` behave exactly as in
/// [`field_value_to_cbor`] — beyond [`MAX_CONVERSION_DEPTH`] the over-deep
/// subtree is either reported or truncated to [`Cbor::Null`], never recursed
/// into. Numbers map to the narrowest CBOR integer that fits, falling back to
/// a float.
pub(super) fn json_to_cbor_at(
    value: Json,
    depth: usize,
    strict: bool,
) -> Result<Cbor, SchemaError> {
    if let Err(err) = check_conversion_depth(depth) {
        return if strict { Err(err) } else { Ok(Cbor::Null) };
    }

    Ok(match value {
        Json::Null => Cbor::Null,
        Json::Bool(b) => Cbor::Bool(b),
        Json::Number(n) => {
            if let Some(u) = n.as_u64() {
                Cbor::Integer(u.into())
            } else if let Some(i) = n.as_i64() {
                Cbor::Integer(i.into())
            } else {
                // `serde_json::Number` cannot hold NaN; `as_f64` is only
                // `None` for out-of-range arbitrary-precision numbers, which
                // saturate to an infinite float of the matching sign here.
                Cbor::Float(n.as_f64().unwrap_or_else(|| {
                    if n.to_string().starts_with('-') {
                        f64::NEG_INFINITY
                    } else {
                        f64::INFINITY
                    }
                }))
            }
        }
        Json::String(s) => Cbor::Text(s),
        Json::Array(arr) => Cbor::Array(
            arr.into_iter()
                .map(|v| json_to_cbor_at(v, depth + 1, strict))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Json::Object(obj) => {
            let mut entries = Vec::with_capacity(obj.len());
            for (k, v) in obj {
                entries.push((Cbor::Text(k), json_to_cbor_at(v, depth + 1, strict)?));
            }
            Cbor::Map(entries)
        }
    })
}

/// Converts a CBOR array of integers in `0..=255` — the shape serde gives
/// `Vec<u8>` and `[u8; N]`, which have no byte-string specialization — into
/// bytes. Shared by [`FieldValue::bytes_from`] (values) and the
/// `TryFrom<Value>` impl of [`FieldKey`] (map keys).
pub(super) fn u8_array_from(arr: Vec<Cbor>) -> Result<Vec<u8>, SchemaError> {
    let mut bytes = Vec::with_capacity(arr.len());
    for v in arr {
        match v {
            Cbor::Integer(i) => bytes.push(u8::try_from(i).map_err(|v| {
                SchemaError::FieldValue(format!(
                    "expected Bytes, got array element {v:?} outside u8 range"
                ))
            })?),
            v => {
                return Err(SchemaError::FieldValue(format!(
                    "expected Bytes, got array element {v:?}"
                )));
            }
        }
    }
    Ok(bytes)
}

/// Converts a CBOR integer into the `f64` an `F64` / `F32` field stores for
/// it: the plain `as` conversion serde applies when a JSON integer token
/// meets a float field — exact up to 2^53, rounded beyond.
pub(super) fn integer_to_f64(i: cbor2::value::Integer) -> f64 {
    i128::from(i) as f64
}

/// Returns the `f32` that holds the integer `v` exactly, or `None` when the
/// conversion would round.
///
/// An `F32` field accepts a JSON integer only under this rule, so that it
/// answers the same way whichever spelling a client sends: `16777217` and
/// `16777217.0` are one value, and only `JSON.stringify` decides which one
/// arrives. The `F64` shape of the same value is already gated by
/// [`is_f32_read_back`], which rejects it for the same reason.
pub(super) fn exact_f32_from_integer(v: i128) -> Option<f32> {
    let f = v as f32;
    (f as i128 == v).then_some(f)
}

/// Checks JSON representability without allocating or cloning payloads.
pub(super) fn validate_json_shape(value: &FieldValue, depth: usize) -> Result<(), SchemaError> {
    check_conversion_depth(depth)?;
    match value {
        FieldValue::Bytes(_) => Err(SchemaError::FieldValue(
            "bytes have no JSON representation".into(),
        )),
        FieldValue::Json(json) => validate_json_depth(json, depth),
        FieldValue::Array(values) => values
            .iter()
            .try_for_each(|v| validate_json_shape(v, depth + 1)),
        FieldValue::Map(values) => {
            for (key, value) in values {
                if !matches!(key, FieldKey::Text(_)) {
                    return Err(SchemaError::FieldValue(format!(
                        "JSON requires a text map key, got {key:?}"
                    )));
                }
                validate_json_shape(value, depth + 1)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub(super) fn validate_json_depth(value: &Json, depth: usize) -> Result<(), SchemaError> {
    check_conversion_depth(depth)?;
    match value {
        Json::Array(values) => values
            .iter()
            .try_for_each(|v| validate_json_depth(v, depth + 1)),
        Json::Object(values) => values
            .values()
            .try_for_each(|v| validate_json_depth(v, depth + 1)),
        _ => Ok(()),
    }
}

/// Consumes read-back shapes to restore a JSON payload. Strings, map keys
/// and existing JSON subtrees move into the result without being cloned.
pub(super) fn field_value_into_json(value: FieldValue, depth: usize) -> Result<Json, SchemaError> {
    check_conversion_depth(depth)?;
    Ok(match value {
        FieldValue::Null => Json::Null,
        FieldValue::Bool(b) => Json::Bool(b),
        FieldValue::I64(i) => Json::Number(i.into()),
        FieldValue::U64(u) => Json::Number(u.into()),
        FieldValue::F64(f) => serde_json::Number::from_f64(f).map_or(Json::Null, Json::Number),
        FieldValue::F32(f) => {
            serde_json::Number::from_f64(f64::from(f)).map_or(Json::Null, Json::Number)
        }
        FieldValue::Text(s) => Json::String(s),
        FieldValue::Json(json) => {
            validate_json_depth(&json, depth)?;
            json
        }
        FieldValue::Vector(vector) => Json::Array(
            vector
                .into_iter()
                .map(|f| Json::Number(f.to_bits().into()))
                .collect(),
        ),
        FieldValue::Array(values) => {
            let mut array = Vec::with_capacity(values.len());
            for value in values {
                array.push(field_value_into_json(value, depth + 1)?);
            }
            Json::Array(array)
        }
        FieldValue::Map(values) => {
            let mut object = serde_json::Map::with_capacity(values.len());
            for (key, value) in values {
                let FieldKey::Text(key) = key else {
                    return Err(SchemaError::FieldValue(format!(
                        "JSON requires a text map key, got {key:?}"
                    )));
                };
                object.insert(key, field_value_into_json(value, depth + 1)?);
            }
            Json::Object(object)
        }
        FieldValue::Bytes(_) => {
            return Err(SchemaError::FieldValue(
                "bytes have no JSON representation".into(),
            ));
        }
    })
}

/// Bounded CBOR -> JSON conversion. Unlike Value::deserialized, this entry
/// shares the conversion depth budget of its surrounding typed containers.
pub(super) fn cbor_into_json(value: Cbor, depth: usize) -> Result<Json, SchemaError> {
    check_conversion_depth(depth)?;
    Ok(match value {
        Cbor::Null => Json::Null,
        Cbor::Bool(b) => Json::Bool(b),
        Cbor::Integer(i) => {
            if let Ok(u) = u64::try_from(i) {
                Json::Number(u.into())
            } else if let Ok(i) = i64::try_from(i) {
                Json::Number(i.into())
            } else {
                return Err(SchemaError::FieldValue(
                    "integer outside JSON's supported 64-bit range".into(),
                ));
            }
        }
        Cbor::Float(f) => serde_json::Number::from_f64(f).map_or(Json::Null, Json::Number),
        Cbor::Text(s) => Json::String(s),
        Cbor::Array(values) => {
            let mut array = Vec::with_capacity(values.len());
            for value in values {
                array.push(cbor_into_json(value, depth + 1)?);
            }
            Json::Array(array)
        }
        Cbor::Map(values) => {
            let mut object = serde_json::Map::with_capacity(values.len());
            for (key, value) in values {
                let Cbor::Text(key) = key else {
                    return Err(SchemaError::FieldValue(
                        "JSON requires text map keys".into(),
                    ));
                };
                match object.entry(key) {
                    serde_json::map::Entry::Vacant(entry) => {
                        entry.insert(cbor_into_json(value, depth + 1)?);
                    }
                    serde_json::map::Entry::Occupied(_) => {
                        return Err(SchemaError::FieldValue("duplicate JSON map key".into()));
                    }
                }
            }
            Json::Object(object)
        }
        Cbor::Tag(_, _) => {
            return Err(SchemaError::FieldValue(
                "CBOR tags have no JSON representation".into(),
            ));
        }
        _ => {
            return Err(SchemaError::FieldValue(
                "value has no JSON representation".into(),
            ));
        }
    })
}
