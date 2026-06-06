//! Field type and value definitions for Anda DB.
//!
//! This module is the heart of the crate's type system and exposes three
//! tightly related concepts:
//!
//! - [`FieldType`] / [`Ft`] — the *declared* type of a field. It is a closed
//!   enum covering primitives (`Bool`, `I64`, `U64`, `F64`, `F32`, `Bytes`,
//!   `Text`, `Json`, `Vector`) plus three composites: `Array`, `Map` and
//!   `Option`.
//! - [`FieldValue`] / [`Fv`] — the *runtime* value of a field. Each variant
//!   matches a `FieldType`, plus a dedicated [`FieldValue::Null`] representing
//!   the absence of a value for [`FieldType::Option`].
//! - [`FieldEntry`] / [`Fe`] — the metadata that ties a field name to its
//!   `FieldType` together with description, uniqueness and a stable numeric
//!   index used for compact storage.
//!
//! Values round-trip through CBOR (via [`Cbor`](ciborium::Value)) for
//! persistence and through JSON for human-readable APIs. In JSON mode,
//! [`FieldValue::Bytes`] is encoded as a Base64 (URL-safe) string.
use base64::{Engine, prelude::BASE64_URL_SAFE};
use ciborium::Value;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt,
};

use crate::{BoxError, SchemaError, validate_field_name};

/// Re-export Map from serde_json
pub use serde_json::Map;

/// Re-export bf16 from half crate
pub use half::bf16;

pub use ic_auth_types::{ByteArrayB64, ByteBufB64};

/// Type alias for Vec<bf16>
pub type Vector = Vec<bf16>;

/// Type alias for FieldType
pub type Ft = FieldType;

/// Type alias for FieldValue
pub type Fv = FieldValue;

/// Type alias for FieldEntry
pub type Fe = FieldEntry;

/// Type alias for ciborium::Value
pub type Cbor = ciborium::Value;

/// Type alias for serde_json::Value
pub type Json = serde_json::Value;

/// Type alias for [`BTreeMap<usize, FieldValue>`], the canonical container
/// for a document's field values.
///
/// Keys are the stable [`FieldEntry::idx`] values from the document's schema,
/// not field names. This keeps records compact on disk and makes lookups
/// constant in space regardless of name length.
pub type IndexedFieldValues = BTreeMap<usize, FieldValue>;

/// The type of a field declared in a [`Schema`](crate::Schema).
///
/// `FieldType` is the closed enum of every type supported by Anda DB.
/// It is purely descriptive: a value of this enum is metadata, never
/// payload. The matching payload type is [`FieldValue`].
///
/// Composite variants:
/// - [`FieldType::Array`] holds either zero, one, or several inner types.
///   With one inner type the array is *homogeneous* (every element must
///   match it). With several inner types it is a fixed-size *tuple-like*
///   array.
/// - [`FieldType::Map`] declares per-key types. A wildcard map
///   (`{ "*": T }` or `{ b"*": T }`) matches any key with values of type
///   `T`. See [`TEXT_WILDCARD_KEY`] / [`BYTES_WILDCARD_KEY`].
/// - [`FieldType::Option`] makes a field nullable.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    /// Boolean value
    Bool,
    /// Signed 64-bit integer
    I64,
    /// Unsigned 64-bit integer
    U64,
    /// 64-bit floating point number
    F64,
    /// 32-bit floating point number
    F32,
    /// Binary data
    Bytes,
    /// UTF-8 encoded text
    Text,
    /// JSON value
    Json,
    /// Vec<bf16>, bf16: 16-bit floating point type implementing the bfloat16 format.
    /// Detail: https://docs.rs/half/latest/half/struct.bf16.html
    Vector,
    /// Array of field types
    Array(Vec<FieldType>),
    /// Map with string keys and field type values
    Map(BTreeMap<FieldKey, FieldType>),
    /// Optional field type
    Option(Box<FieldType>),
}

impl fmt::Debug for FieldType {
    /// Debug formatting for FieldType
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldType::Bool => write!(f, "Bool"),
            FieldType::I64 => write!(f, "I64"),
            FieldType::U64 => write!(f, "U64"),
            FieldType::F64 => write!(f, "F64"),
            FieldType::F32 => write!(f, "F32"),
            FieldType::Bytes => write!(f, "Bytes"),
            FieldType::Text => write!(f, "Text"),
            FieldType::Json => write!(f, "Json"),
            FieldType::Vector => write!(f, "Vector"),
            FieldType::Array(v) => write!(f, "Array({v:?})"),
            FieldType::Map(v) => write!(f, "Map({v:?})"),
            FieldType::Option(v) => write!(f, "Option({v:?})"),
        }
    }
}

impl FieldType {
    /// Returns `true` if this type accepts [`FieldValue::Null`], i.e. it is an
    /// [`Option`](FieldType::Option) variant.
    pub fn allows_null(&self) -> bool {
        matches!(self, FieldType::Option(_))
    }

    /// Coerce a CBOR value into a [`FieldValue`] that conforms to this type.
    ///
    /// This is more strict than [`FieldValue::try_from`]: instead of inferring
    /// a value from the CBOR shape, `extract` requires the CBOR to match
    /// `self`. For [`Option`](FieldType::Option), CBOR `null` produces
    /// [`FieldValue::Null`].
    ///
    /// # Arguments
    /// * `value` - The CBOR value to convert.
    ///
    /// # Errors
    /// Returns [`SchemaError::FieldValue`] when the CBOR shape does not
    /// match `self` (e.g. extracting a `Text` from CBOR `Bytes`).
    pub fn extract(&self, value: Cbor) -> Result<FieldValue, SchemaError> {
        match &self {
            FieldType::Bool => FieldValue::bool_from(value),
            FieldType::I64 => FieldValue::i64_from(value),
            FieldType::U64 => FieldValue::u64_from(value),
            FieldType::F64 => FieldValue::f64_from(value),
            FieldType::F32 => FieldValue::f32_from(value),
            FieldType::Bytes => FieldValue::bytes_from(value),
            FieldType::Text => FieldValue::text_from(value),
            FieldType::Json => FieldValue::json_from(value),
            FieldType::Vector => FieldValue::vector_from(value),
            FieldType::Array(types) => FieldValue::array_from(value, types),
            FieldType::Map(types) => FieldValue::map_from(value, types),
            FieldType::Option(ft) => {
                if value == Cbor::Null {
                    return Ok(FieldValue::Null);
                }
                ft.extract(value)
            }
        }
    }

    /// Validate that `value` is acceptable for this type.
    ///
    /// `Vector` accepts both [`FieldValue::Vector`] and an
    /// [`Array`](FieldValue::Array) of `U64` (the latter is how a `Vector`
    /// is observed when it is read back through generic CBOR without type
    /// information). `Json` accepts any value because JSON is itself
    /// dynamically typed.
    ///
    /// `Option(T)` accepts [`FieldValue::Null`]; non-`Option` types reject it.
    ///
    /// # Errors
    /// Returns [`SchemaError::FieldValue`] describing the first mismatch.
    pub fn validate(&self, value: &FieldValue) -> Result<(), SchemaError> {
        match (self, value) {
            (FieldType::Bool, FieldValue::Bool(_)) => Ok(()),
            (FieldType::I64, FieldValue::I64(_)) => Ok(()),
            (FieldType::U64, FieldValue::U64(_)) => Ok(()),
            (FieldType::F64, FieldValue::F64(v)) if !v.is_nan() => Ok(()),
            (FieldType::F64, FieldValue::F64(v)) => Err(SchemaError::FieldValue(format!(
                "expected non-NaN F64, got {v:?}"
            ))),
            (FieldType::F32, FieldValue::F32(v)) if !v.is_nan() => Ok(()),
            (FieldType::F32, FieldValue::F32(v)) => Err(SchemaError::FieldValue(format!(
                "expected non-NaN F32, got {v:?}"
            ))),
            (FieldType::Bytes, FieldValue::Bytes(_)) => Ok(()),
            (FieldType::Text, FieldValue::Text(_)) => Ok(()),
            (FieldType::Json, _) => Ok(()),
            (FieldType::Vector, FieldValue::Vector(_)) => Ok(()),
            (FieldType::Vector, FieldValue::Array(values)) => {
                if values.iter().all(|v| matches!(v, FieldValue::U64(_))) {
                    return Ok(());
                }
                Err(SchemaError::FieldValue(format!(
                    "expected Vector, got {values:?}"
                )))
            }
            (FieldType::Array(types), FieldValue::Array(values)) => match types.len() {
                0 => Ok(()),
                1 => {
                    let ft = types.first().unwrap();
                    for fv in values.iter() {
                        ft.validate(fv)?;
                    }
                    Ok(())
                }
                _ => {
                    if values.len() != types.len() {
                        return Err(SchemaError::FieldValue(format!(
                            "invalid array length, expected {:?}, got {:?}",
                            types.len(),
                            values.len()
                        )));
                    }

                    for (i, ft) in types.iter().enumerate() {
                        if let Some(fv) = values.get(i) {
                            ft.validate(fv)?;
                        } else {
                            return Err(SchemaError::FieldValue(format!(
                                "no value at array[{i}], expected type {ft:?}",
                            )));
                        }
                    }
                    Ok(())
                }
            },
            (FieldType::Map(types), FieldValue::Map(values)) => validate_map_fields(types, values),
            (FieldType::Option(ft), val) => {
                if val == &FieldValue::Null {
                    return Ok(());
                }
                ft.validate(val)
            }
            _ => Err(SchemaError::FieldValue(format!(
                "expected type {self:?}, got value {value:?}"
            ))),
        }
    }
}

/// A key in a [`FieldType::Map`] / [`FieldValue::Map`].
///
/// Map keys may be either UTF-8 [`FieldKey::Text`] or arbitrary
/// [`FieldKey::Bytes`]. The two are kept distinct in CBOR so that a `Bytes`
/// key is never confused with the textual representation of the same
/// payload.
///
/// In JSON serialization, a `Bytes` key is rendered as a URL-safe Base64
/// string. On the way back, a `Text` value that successfully decodes as
/// Base64 is treated as a `Bytes` key.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FieldKey {
    /// A UTF-8 text key.
    Text(String),
    /// An arbitrary byte-string key.
    Bytes(Vec<u8>),
}

/// The wildcard text key (`"*"`) used to express a homogeneous `Map<Text, T>`.
pub static TEXT_WILDCARD_KEY: std::sync::LazyLock<FieldKey> =
    std::sync::LazyLock::new(|| "*".into());

/// The wildcard byte key (`b"*"`) used to express a homogeneous `Map<Bytes, T>`.
pub static BYTES_WILDCARD_KEY: std::sync::LazyLock<FieldKey> =
    std::sync::LazyLock::new(|| b"*".into());

impl FieldKey {
    /// Returns the [`FieldType`] that the key itself uses
    /// ([`FieldType::Text`] for `Text`, [`FieldType::Bytes`] for `Bytes`).
    pub fn field_type(&self) -> FieldType {
        match self {
            FieldKey::Text(_) => FieldType::Text,
            FieldKey::Bytes(_) => FieldType::Bytes,
        }
    }

    /// Borrow the raw bytes of this key, regardless of variant.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            FieldKey::Text(s) => s.as_bytes(),
            FieldKey::Bytes(b) => b,
        }
    }
}

impl From<String> for FieldKey {
    fn from(s: String) -> Self {
        FieldKey::Text(s)
    }
}

impl From<&str> for FieldKey {
    fn from(s: &str) -> Self {
        FieldKey::Text(s.to_string())
    }
}

impl From<Vec<u8>> for FieldKey {
    fn from(b: Vec<u8>) -> Self {
        FieldKey::Bytes(b)
    }
}

impl<const N: usize> From<[u8; N]> for FieldKey {
    fn from(b: [u8; N]) -> Self {
        FieldKey::Bytes(b.into())
    }
}

impl From<&[u8]> for FieldKey {
    fn from(b: &[u8]) -> Self {
        FieldKey::Bytes(b.to_vec())
    }
}

impl<const N: usize> From<&[u8; N]> for FieldKey {
    fn from(b: &[u8; N]) -> Self {
        FieldKey::Bytes(b.to_vec())
    }
}

impl TryFrom<Value> for FieldKey {
    type Error = BoxError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Text(s) => Ok(FieldKey::Text(s)),
            Value::Bytes(b) => Ok(FieldKey::Bytes(b)),
            _ => Err(
                SchemaError::FieldValue(format!("expected Text or Bytes, got {value:?}")).into(),
            ),
        }
    }
}

impl std::fmt::Display for FieldKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldKey::Text(s) => write!(f, "{s}"),
            FieldKey::Bytes(b) => write!(f, "{}", BASE64_URL_SAFE.encode(b)),
        }
    }
}

/// The runtime value of a field.
///
/// Each variant corresponds 1:1 to a [`FieldType`] variant, with the
/// addition of [`FieldValue::Null`] which represents an absent value for
/// [`FieldType::Option`]. All variants serialize losslessly to and from
/// CBOR via [`From<FieldValue> for Cbor`] and [`FieldValue::try_from`].
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
    /// Vec<bf16>, bf16: 16-bit floating point type implementing the bfloat16 format.
    /// Detail: https://docs.rs/half/latest/half/struct.bf16.html
    Vector(Vec<bf16>),
    /// Array of field values
    Array(Vec<FieldValue>),
    /// Map with string keys and field values
    Map(BTreeMap<FieldKey, FieldValue>),
    /// Null value (for optional fields)
    Null,
}

impl From<FieldValue> for Cbor {
    /// Convert a FieldValue to a CBOR value
    ///
    /// # Arguments
    /// * `value` - The FieldValue to convert
    ///
    /// # Returns
    /// * `Cbor` - The converted CBOR value
    fn from(value: FieldValue) -> Self {
        match value {
            FieldValue::Bool(b) => Cbor::Bool(b),
            FieldValue::I64(i) => Cbor::Integer(i.into()),
            FieldValue::U64(u) => Cbor::Integer(u.into()),
            FieldValue::F64(f) => Cbor::Float(f),
            FieldValue::F32(f) => Cbor::Float(f as f64),
            FieldValue::Bytes(b) => Cbor::Bytes(b),
            FieldValue::Text(t) => Cbor::Text(t),
            // JSON value can always be serialized to CBOR format!
            FieldValue::Json(obj) => {
                Cbor::serialized(&obj).expect("Failed to serialize JSON to CBOR")
            }
            FieldValue::Vector(arr) => {
                Cbor::Array(arr.into_iter().map(|f| f.to_bits().into()).collect())
            }
            FieldValue::Array(arr) => Cbor::Array(arr.into_iter().map(Cbor::from).collect()),
            FieldValue::Map(obj) => {
                let obj = obj
                    .into_iter()
                    .map(|(k, v)| {
                        (
                            match k {
                                FieldKey::Text(s) => Cbor::Text(s),
                                FieldKey::Bytes(b) => Cbor::Bytes(b),
                            },
                            Cbor::from(v),
                        )
                    })
                    .collect();
                Cbor::Map(obj)
            }

            FieldValue::Null => Cbor::Null,
        }
    }
}

impl From<bool> for FieldValue {
    fn from(val: bool) -> Self {
        FieldValue::Bool(val)
    }
}

impl From<i64> for FieldValue {
    fn from(val: i64) -> Self {
        FieldValue::I64(val)
    }
}

impl From<u64> for FieldValue {
    fn from(val: u64) -> Self {
        FieldValue::U64(val)
    }
}

impl From<f64> for FieldValue {
    fn from(val: f64) -> Self {
        FieldValue::F64(val)
    }
}

impl From<f32> for FieldValue {
    fn from(val: f32) -> Self {
        FieldValue::F32(val)
    }
}

impl From<Vec<u8>> for FieldValue {
    fn from(val: Vec<u8>) -> Self {
        FieldValue::Bytes(val)
    }
}

impl From<String> for FieldValue {
    fn from(val: String) -> Self {
        FieldValue::Text(val)
    }
}

impl From<Json> for FieldValue {
    fn from(val: Json) -> Self {
        FieldValue::Json(val)
    }
}

impl From<Vec<bf16>> for FieldValue {
    fn from(val: Vec<bf16>) -> Self {
        FieldValue::Vector(val)
    }
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
            _ => Err(SchemaError::FieldValue(format!("expected I64, got {value:?}")).into()),
        }
    }
}

impl<'a> TryFrom<&'a FieldValue> for i64 {
    type Error = BoxError;

    fn try_from(value: &'a FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::I64(v) => Ok(*v),
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
            _ => Err(SchemaError::FieldValue(format!("expected F64, got {value:?}")).into()),
        }
    }
}

impl<'a> TryFrom<&'a FieldValue> for f64 {
    type Error = BoxError;

    fn try_from(value: &'a FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::F64(v) => Ok(*v),
            _ => Err(SchemaError::FieldValue(format!("expected F64, got {value:?}")).into()),
        }
    }
}

impl TryFrom<FieldValue> for f32 {
    type Error = BoxError;

    fn try_from(value: FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::F32(v) => Ok(v),
            _ => Err(SchemaError::FieldValue(format!("expected F32, got {value:?}")).into()),
        }
    }
}

impl<'a> TryFrom<&'a FieldValue> for f32 {
    type Error = BoxError;

    fn try_from(value: &'a FieldValue) -> Result<Self, Self::Error> {
        match value {
            FieldValue::F32(v) => Ok(*v),
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
    /// # Arguments
    /// * `value` - The CBOR value to convert
    ///
    /// # Returns
    /// * `Result<Self, SchemaError>` - The converted FieldValue or an error message
    pub fn f64_from(value: Cbor) -> Result<Self, SchemaError> {
        match value {
            Cbor::Float(f) if !f.is_nan() => Ok(FieldValue::F64(f)),
            v => Err(SchemaError::FieldValue(format!("expected F64, got {v:?}"))),
        }
    }

    /// Create an F32 FieldValue from a CBOR value
    ///
    /// # Arguments
    /// * `value` - The CBOR value to convert
    ///
    /// # Returns
    /// * `Result<Self, SchemaError>` - The converted FieldValue or an error message
    pub fn f32_from(value: Cbor) -> Result<Self, SchemaError> {
        match value {
            Cbor::Float(f) if !f.is_nan() => Ok(FieldValue::F32(f as f32)),
            v => Err(SchemaError::FieldValue(format!("expected F32, got {v:?}"))),
        }
    }

    /// Create a Bytes FieldValue from a CBOR value
    ///
    /// # Arguments
    /// * `value` - The CBOR value to convert
    ///
    /// # Returns
    /// * `Result<Self, SchemaError>` - The converted FieldValue or an error message
    pub fn bytes_from(value: Cbor) -> Result<Self, SchemaError> {
        match value {
            Cbor::Bytes(b) => Ok(FieldValue::Bytes(b)),
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
        let val: Json = value
            .deserialized()
            .map_err(|v| SchemaError::FieldValue(format!("expected Json, got {v:?}")))?;
        Ok(FieldValue::Json(val))
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
            Cbor::Array(arr) => Ok(FieldValue::Vector(
                arr.into_iter()
                    .map(Self::bf16_from)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
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
        match value {
            Cbor::Array(values) => match types.len() {
                0 => Ok(FieldValue::Array(
                    values
                        .into_iter()
                        .map(FieldValue::try_from)
                        .collect::<Result<Vec<_>, _>>()?,
                )),
                1 => {
                    let ft = types.first().unwrap();
                    Ok(FieldValue::Array(
                        values
                            .into_iter()
                            .map(|v| ft.extract(v))
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
                        rt.push(ft.extract(val)?);
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
        match value {
            Cbor::Map(values) => {
                if types.is_empty() {
                    return Ok(FieldValue::Map(
                        values
                            .into_iter()
                            .map(|(k, v)| {
                                let key = match k {
                                    Cbor::Text(s) => FieldKey::Text(s),
                                    Cbor::Bytes(b) => FieldKey::Bytes(b),
                                    _ => {
                                        return Err(SchemaError::FieldValue(format!(
                                            "invalid map key: {k:?}"
                                        )));
                                    }
                                };
                                Ok::<_, SchemaError>((key, FieldValue::try_from(v)?))
                            })
                            .collect::<Result<BTreeMap<_, _>, _>>()?,
                    ));
                }

                let wildcard_map = as_wildcard_map(types);

                let mut vals: BTreeMap<FieldKey, FieldValue> = BTreeMap::new();
                for (k, v) in values {
                    let k = k.try_into().map_err(|err| {
                        SchemaError::FieldType(format!("invalid map key: {err:?}"))
                    })?;
                    if vals.contains_key(&k) {
                        return Err(SchemaError::FieldValue(format!("duplicate map key {k:?}")));
                    }

                    match wildcard_map {
                        Some(ft) => {
                            // Special case for wildcard map
                            let v = ft.extract(v)?;
                            vals.insert(k, v);
                            continue;
                        }
                        None => match types.get(&k) {
                            None => {
                                return Err(SchemaError::FieldValue(format!(
                                    "invalid map key {k:?}"
                                )));
                            }
                            Some(ft) => {
                                let v = ft.extract(v)?;
                                vals.insert(k, v);
                            }
                        },
                    }
                }

                validate_map_fields(types, &vals)?;
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
        match value {
            Cbor::Bool(_) => Self::bool_from(value),
            Cbor::Integer(i) => {
                let z = ciborium::value::Integer::from(0);
                if i >= z {
                    Self::u64_from(value)
                } else {
                    Self::i64_from(value)
                }
            }
            Cbor::Float(_) => Self::f64_from(value),
            Cbor::Bytes(_) => Self::bytes_from(value),
            Cbor::Text(_) => Self::text_from(value),
            Cbor::Array(_) => Self::array_from(value, &[]),
            Cbor::Map(_) => Self::map_from(value, &BTreeMap::new()),
            Cbor::Null => Ok(FieldValue::Null),
            Cbor::Tag(_, val) => Self::try_from(*val),
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
    /// # Returns
    /// * `Result<T, SchemaError>` - The deserialized value or an error message
    pub fn deserialized<T: DeserializeOwned>(self) -> Result<T, SchemaError> {
        let val: Cbor = self.into();
        val.deserialized()
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

/// Metadata for a single field in a [`Schema`](crate::Schema).
///
/// `FieldEntry` ties a textual `name` to a [`FieldType`] together with an
/// optional human-readable description, an `unique` flag (used by
/// collection-level uniqueness indexes) and a stable numeric `idx`.
///
/// The numeric `idx` is assigned by the schema builder and is what gets
/// persisted alongside each field value: documents are stored as
/// `BTreeMap<idx, FieldValue>` rather than `BTreeMap<name, FieldValue>` to
/// keep records compact. Schema migrations preserve `idx` values for
/// fields that exist in both the old and new schemas.
///
/// On the wire, every key is renamed to a single letter (`n`, `d`, `t`,
/// `u`, `i`) for the same reason. Long-form names are accepted on input
/// via `serde(alias = ...)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldEntry {
    /// Field name. Must be unique within a schema and conform to the rules
    /// enforced by [`validate_field_name`].
    #[serde(rename = "n", alias = "name")]
    name: String,

    /// Human-readable description of the field.
    ///
    /// Used as documentation for tools and as context for LLM clients that
    /// inspect the schema to decide how to populate or query a field.
    #[serde(rename = "d", alias = "description")]
    description: String,

    /// Declared type of the field.
    #[serde(rename = "t", alias = "type")]
    r#type: FieldType,

    /// Whether the field value must be unique across all documents in the
    /// collection. Enforcement is performed at the collection layer.
    #[serde(rename = "u", alias = "unique")]
    unique: bool,

    /// Stable numeric index used as the persistent key for this field's
    /// values. The `_id` field always has `idx == 0`.
    #[serde(rename = "i", alias = "index")]
    idx: usize,
}

impl FieldEntry {
    /// Create a new field entry
    ///
    /// # Arguments
    /// * `name` - Field name
    /// * `r#type` - Field type
    ///
    /// # Returns
    /// * `Result<Self, SchemaError>` - The created field entry or an error message
    pub fn new(name: String, r#type: FieldType) -> Result<Self, SchemaError> {
        validate_field_name(&name)?;
        Ok(Self {
            name,
            r#type,
            description: String::new(),
            unique: false,
            idx: 0,
        })
    }

    /// Set the field description
    ///
    /// # Arguments
    /// * `description` - Field description
    ///
    /// # Returns
    /// * `Self` - The modified field entry
    pub fn with_description(mut self, description: String) -> Self {
        self.description = description;
        self
    }

    /// Mark the field as unique
    ///
    /// # Returns
    /// * `Self` - The modified field entry
    pub fn with_unique(mut self) -> Self {
        self.unique = true;
        self
    }

    /// Set the field index
    ///
    /// # Arguments
    /// * `idx` - Field index value
    ///
    /// # Returns
    /// * `Self` - The modified field entry
    pub fn with_idx(mut self, idx: usize) -> Self {
        self.idx = idx;
        self
    }

    /// Set the field index in place.
    ///
    /// Useful when you need to update the index of an existing entry without
    /// cloning all of its other data (e.g. during schema migration).
    ///
    /// # Arguments
    /// * `idx` - Field index value
    ///
    /// # Returns
    /// * `&mut Self` - The modified field entry
    pub fn set_idx(&mut self, idx: usize) -> &mut Self {
        self.idx = idx;
        self
    }

    /// Get the field name
    ///
    /// # Returns
    /// * `&str` - The field name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the field type
    ///
    /// # Returns
    /// * `&FieldType` - The field type
    pub fn r#type(&self) -> &FieldType {
        &self.r#type
    }

    /// Check if this field is required.
    ///
    /// A field is required if its type is NOT `FieldType::Option(_)`.
    ///
    /// # Returns
    /// * `bool` - True if the field is required
    pub fn required(&self) -> bool {
        !matches!(self.r#type, FieldType::Option(_))
    }

    /// Check if the field is unique
    ///
    /// # Returns
    /// * `bool` - True if the field is unique
    pub fn unique(&self) -> bool {
        self.unique
    }

    /// Get the field index
    ///
    /// # Returns
    /// * `usize` - The field index
    pub fn idx(&self) -> usize {
        self.idx
    }

    /// Extract a field value from a CBOR value
    ///
    /// # Arguments
    /// * `val` - The CBOR value to extract from
    /// * `validate` - Whether to validate the extracted value
    ///
    /// # Returns
    /// * `Result<FieldValue, SchemaError>` - The extracted field value or an error message
    pub fn extract(&self, val: Cbor, validate: bool) -> Result<FieldValue, SchemaError> {
        match self.r#type.extract(val) {
            Ok(v) => {
                if validate {
                    self.validate(&v)?;
                }
                Ok(v)
            }
            Err(e) => Err(SchemaError::FieldValue(format!(
                "field {} is invalid, error: {}",
                self.name, e
            ))),
        }
    }

    /// Validate a field value against this field entry's constraints
    ///
    /// # Arguments
    /// * `value` - The field value to validate
    ///
    /// # Returns
    /// * `Result<(), SchemaError>` - Ok if valid, or an error message if invalid
    pub fn validate(&self, value: &FieldValue) -> Result<(), SchemaError> {
        if value == &FieldValue::Null {
            if matches!(self.r#type, FieldType::Option(_)) {
                return Ok(());
            }

            return Err(SchemaError::FieldValue(format!(
                "field {} is required, expected type {:?}",
                self.name, self.r#type
            )));
        }

        self.r#type.validate(value).map_err(|err| {
            SchemaError::FieldValue(format!("field {} is invalid, error: {}", self.name, err))
        })
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

/// If `m` describes a *wildcard* map — i.e. it has exactly one entry whose
/// key is [`TEXT_WILDCARD_KEY`] or [`BYTES_WILDCARD_KEY`] — return the value
/// type of that entry. Otherwise return `None`.
fn as_wildcard_map(m: &BTreeMap<FieldKey, FieldType>) -> Option<&FieldType> {
    match m.len() {
        1 => m
            .get(&TEXT_WILDCARD_KEY)
            .or_else(|| m.get(&BYTES_WILDCARD_KEY)),
        _ => None,
    }
}

fn validate_map_fields(
    types: &BTreeMap<FieldKey, FieldType>,
    values: &BTreeMap<FieldKey, FieldValue>,
) -> Result<(), SchemaError> {
    if types.is_empty() {
        return Ok(());
    }

    if let Some(ft) = as_wildcard_map(types) {
        for fv in values.values() {
            ft.validate(fv)?;
        }
        return Ok(());
    }

    if let Some(k) = values.keys().find(|k| !types.contains_key(*k)) {
        return Err(SchemaError::FieldValue(format!("invalid map key {k:?}")));
    }

    for (k, ft) in types {
        let rt = match values.get(k) {
            None => ft.validate(&FieldValue::Null),
            Some(v) => ft.validate(v),
        };

        rt.map_err(|err| {
            SchemaError::FieldValue(format!("invalid map value at key {k:?}, error: {err}"))
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ciborium::{cbor, from_reader, into_writer};
    use ic_auth_types::{Xid, cbor_into_vec};
    use serde_json::json;

    #[test]
    fn test_field_key() {
        let val = FieldValue::Map(BTreeMap::from([(
            FieldKey::Text("*".into()),
            FieldValue::Text("*".into()),
        )]));
        let data = cbor_into_vec(&cbor!({ "*" => "*" }).unwrap()).unwrap();
        assert_eq!(cbor_into_vec(&val).unwrap(), data);
        let val2: FieldValue = from_reader(data.as_slice()).unwrap();
        assert_eq!(val, val2);

        let val = FieldValue::Map(BTreeMap::from([(
            FieldKey::Bytes(b"*".to_vec()),
            FieldValue::Bytes(b"*".to_vec()),
        )]));
        let data = cbor_into_vec(&Value::Map(vec![(
            Value::Bytes(b"*".to_vec()),
            Value::Bytes(b"*".to_vec()),
        )]))
        .unwrap();
        // println!("data: {:?}", hex::encode(&data));
        assert_eq!(cbor_into_vec(&val).unwrap(), data);
        let val2: FieldValue = from_reader(data.as_slice()).unwrap();
        assert_eq!(val, val2);
        let data = serde_json::to_string(&val).unwrap();
        println!("json: {}", data);
        assert_eq!(data, r#"{"Kg==":"Kg=="}"#);
        let val2: FieldValue = serde_json::from_str(&data).unwrap();
        assert_eq!(val, val2);
    }

    #[test]
    fn test_field_type_debug() {
        assert_eq!(format!("{:?}", FieldType::Bool), "Bool");
        assert_eq!(format!("{:?}", FieldType::I64), "I64");
        assert_eq!(format!("{:?}", FieldType::U64), "U64");
        assert_eq!(format!("{:?}", FieldType::F64), "F64");
        assert_eq!(format!("{:?}", FieldType::F32), "F32");
        assert_eq!(format!("{:?}", FieldType::Bytes), "Bytes");
        assert_eq!(format!("{:?}", FieldType::Text), "Text");
        assert_eq!(format!("{:?}", FieldType::Json), "Json");
        assert_eq!(format!("{:?}", FieldType::Vector), "Vector");

        let array_type = FieldType::Array(vec![FieldType::U64]);
        assert_eq!(format!("{array_type:?}"), "Array([U64])");

        let mut map = BTreeMap::new();
        map.insert("key".into(), FieldType::Text);
        let map_type = FieldType::Map(map);
        assert_eq!(format!("{map_type:?}"), "Map({Text(\"key\"): Text})");

        let option_type = FieldType::Option(Box::new(FieldType::Bool));
        assert_eq!(format!("{option_type:?}"), "Option(Bool)");
    }

    #[test]
    fn test_field_value_debug() {
        assert_eq!(format!("{:?}", FieldValue::Bool(true)), "Bool(true)");
        assert_eq!(format!("{:?}", FieldValue::I64(-42)), "I64(-42)");
        assert_eq!(format!("{:?}", FieldValue::U64(42)), "U64(42)");
        assert_eq!(format!("{:?}", FieldValue::F64(3.15)), "F64(3.15)");
        assert_eq!(format!("{:?}", FieldValue::F32(2.71)), "F32(2.71)");
        assert_eq!(
            format!("{:?}", FieldValue::Bytes(vec![1, 2, 3])),
            "Bytes([1, 2, 3])"
        );
        assert_eq!(
            format!("{:?}", FieldValue::Text("hello".to_string())),
            "Text(\"hello\")"
        );

        let json_val = FieldValue::Json(json!({"name": "test"}));
        assert_eq!(
            format!("{json_val:?}"),
            "Json(Object {\"name\": String(\"test\")})"
        );

        assert_eq!(
            format!("{:?}", FieldValue::Vector(vec![bf16::from_f32(1.5)])),
            "Vector([1.5])"
        );

        let array_val = FieldValue::Array(vec![FieldValue::U64(1), FieldValue::U64(2)]);
        assert_eq!(format!("{array_val:?}"), "Array([U64(1), U64(2)])");

        let mut map = BTreeMap::new();
        map.insert("key".into(), FieldValue::Text("value".to_string()));
        let map_val = FieldValue::Map(map);
        assert_eq!(
            format!("{map_val:?}"),
            "Map({Text(\"key\"): Text(\"value\")})"
        );

        assert_eq!(format!("{:?}", FieldValue::Null), "Null");
    }

    #[test]
    fn test_field_type_extract() {
        // Bool
        let bool_val = FieldType::Bool.extract(Cbor::Bool(true)).unwrap();
        assert_eq!(bool_val, FieldValue::Bool(true));

        // U64
        let u64_val = FieldType::U64.extract(cbor!(42).unwrap()).unwrap();
        assert_eq!(u64_val, FieldValue::U64(42));

        // I64
        let i64_val = FieldType::I64.extract(Cbor::Integer((-42).into())).unwrap();
        assert_eq!(i64_val, FieldValue::I64(-42));

        // F64
        let f64_val = FieldType::F64.extract(Cbor::Float(3.15)).unwrap();
        assert_eq!(f64_val, FieldValue::F64(3.15));

        // F32
        let f32_val = FieldType::F32.extract(Cbor::Float(2.71)).unwrap();
        assert_eq!(f32_val, FieldValue::F32(2.71_f32));

        // Bytes
        let bytes_val = FieldType::Bytes
            .extract(Cbor::Bytes(vec![1, 2, 3]))
            .unwrap();
        assert_eq!(bytes_val, FieldValue::Bytes(vec![1, 2, 3]));

        // Text
        let text_val = FieldType::Text
            .extract(Cbor::Text("hello".to_string()))
            .unwrap();
        assert_eq!(text_val, FieldValue::Text("hello".to_string()));

        // Vector
        let vector_val = FieldType::Vector
            .extract(Cbor::Array(vec![
                Cbor::Integer(bf16::from_f32(1.1).to_bits().into()),
                Cbor::Integer(bf16::from_f32(1.2).to_bits().into()),
            ]))
            .unwrap();
        assert_eq!(
            vector_val,
            FieldValue::Vector(vec![bf16::from_f32(1.1), bf16::from_f32(1.2)])
        );

        // Array with single type
        let array_type = FieldType::Array(vec![FieldType::U64]);
        let array_cbor = Cbor::Array(vec![Cbor::Integer(1.into()), Cbor::Integer(2.into())]);
        let array_val = array_type.extract(array_cbor).unwrap();
        assert_eq!(
            array_val,
            FieldValue::Array(vec![FieldValue::U64(1), FieldValue::U64(2)])
        );

        // Array with multiple types
        let array_type = FieldType::Array(vec![FieldType::U64, FieldType::Text]);
        let array_cbor = Cbor::Array(vec![
            Cbor::Integer(1.into()),
            Cbor::Text("hello".to_string()),
        ]);
        let array_val = array_type.extract(array_cbor).unwrap();
        assert_eq!(
            array_val,
            FieldValue::Array(vec![
                FieldValue::U64(1),
                FieldValue::Text("hello".to_string()),
            ])
        );

        // Map
        let mut map_type = BTreeMap::new();
        map_type.insert("_id".into(), FieldType::U64);
        map_type.insert("name".into(), FieldType::Text);
        let map_type = FieldType::Map(map_type);

        let map_cbor = Cbor::Map(vec![
            (Cbor::Text("_id".to_string()), Cbor::Integer(1.into())),
            (
                Cbor::Text("name".to_string()),
                Cbor::Text("test".to_string()),
            ),
        ]);

        let map_val = map_type.extract(map_cbor).unwrap();
        let mut expected_map = BTreeMap::new();
        expected_map.insert("_id".into(), FieldValue::U64(1));
        expected_map.insert("name".into(), FieldValue::Text("test".to_string()));
        assert_eq!(map_val, FieldValue::Map(expected_map));

        // Option (Some)
        let option_type = FieldType::Option(Box::new(FieldType::Bool));
        let option_val = option_type.extract(Cbor::Bool(true)).unwrap();
        assert_eq!(option_val, FieldValue::Bool(true));

        // Option (None)
        let option_val = option_type.extract(Cbor::Null).unwrap();
        assert_eq!(option_val, FieldValue::Null);
    }

    #[test]
    fn test_field_type_validate() {
        // Bool
        assert!(FieldType::Bool.validate(&FieldValue::Bool(true)).is_ok());
        assert!(FieldType::Bool.validate(&FieldValue::U64(1)).is_err());

        // U64
        assert!(FieldType::U64.validate(&FieldValue::U64(42)).is_ok());
        assert!(FieldType::U64.validate(&FieldValue::I64(42)).is_err());

        // I64
        assert!(FieldType::I64.validate(&FieldValue::I64(-42)).is_ok());
        assert!(FieldType::I64.validate(&FieldValue::U64(42)).is_err());

        // F64
        assert!(FieldType::F64.validate(&FieldValue::F64(3.15)).is_ok());
        assert!(FieldType::F64.validate(&FieldValue::F64(f64::NAN)).is_err());
        assert!(FieldType::F64.validate(&FieldValue::F32(3.15)).is_err());

        // F32
        assert!(FieldType::F32.validate(&FieldValue::F32(2.71)).is_ok());
        assert!(FieldType::F32.validate(&FieldValue::F32(f32::NAN)).is_err());
        assert!(FieldType::F32.validate(&FieldValue::F64(2.71)).is_err());

        // Bytes
        assert!(
            FieldType::Bytes
                .validate(&FieldValue::Bytes(vec![1, 2, 3]))
                .is_ok()
        );
        assert!(
            FieldType::Bytes
                .validate(&FieldValue::Text("bytes".to_string()))
                .is_err()
        );

        // Text
        assert!(
            FieldType::Text
                .validate(&FieldValue::Text("hello".to_string()))
                .is_ok()
        );
        assert!(
            FieldType::Text
                .validate(&FieldValue::Bytes(vec![104, 101, 108, 108, 111]))
                .is_err()
        );

        // Json
        assert!(
            FieldType::Json
                .validate(&FieldValue::Json(json!({"key": "value"})))
                .is_ok()
        );
        assert!(
            FieldType::Json
                .validate(&FieldValue::Text("json".to_string()))
                .is_ok()
        );

        // Vector
        assert!(
            FieldType::Vector
                .validate(&FieldValue::Vector(vec![bf16::from_f32(1.5)]))
                .is_ok()
        );
        assert!(
            FieldType::Vector
                .validate(&FieldValue::Array(vec![FieldValue::U64(1)]))
                .is_ok()
        );
        assert!(
            FieldType::Vector
                .validate(&FieldValue::Array(vec![FieldValue::I64(-1)]))
                .is_err()
        );

        // Array with single type
        let array_type = FieldType::Array(vec![FieldType::U64]);
        let array_val = FieldValue::Array(vec![FieldValue::U64(1), FieldValue::U64(2)]);
        assert!(array_type.validate(&array_val).is_ok());

        let invalid_array_val = FieldValue::Array(vec![
            FieldValue::U64(1),
            FieldValue::Text("invalid".to_string()),
        ]);
        assert!(array_type.validate(&invalid_array_val).is_err());

        // Array with multiple types
        let array_type = FieldType::Array(vec![FieldType::U64, FieldType::Text]);
        let array_val = FieldValue::Array(vec![
            FieldValue::U64(1),
            FieldValue::Text("hello".to_string()),
        ]);
        assert!(array_type.validate(&array_val).is_ok());

        let invalid_array_val = FieldValue::Array(vec![FieldValue::U64(1)]);
        assert!(array_type.validate(&invalid_array_val).is_err());

        // Map
        let mut map_type = BTreeMap::new();
        map_type.insert("_id".into(), FieldType::U64);
        map_type.insert("name".into(), FieldType::Text);
        let map_type = FieldType::Map(map_type);

        let mut map_val = BTreeMap::new();
        map_val.insert("_id".into(), FieldValue::U64(1));
        map_val.insert("name".into(), FieldValue::Text("test".to_string()));
        let map_val = FieldValue::Map(map_val);
        assert!(map_type.validate(&map_val).is_ok());

        let mut invalid_map_val = BTreeMap::new();
        invalid_map_val.insert("_id".into(), FieldValue::Text("invalid".to_string()));
        invalid_map_val.insert("name".into(), FieldValue::Text("test".to_string()));
        let invalid_map_val = FieldValue::Map(invalid_map_val);
        assert!(map_type.validate(&invalid_map_val).is_err());

        // Option (Some)
        let option_type = FieldType::Option(Box::new(FieldType::Bool));
        assert!(option_type.validate(&FieldValue::Bool(true)).is_ok());
        assert!(option_type.validate(&FieldValue::Null).is_ok());
        assert!(option_type.validate(&FieldValue::U64(42)).is_err());
    }

    #[test]
    fn test_field_type_extract_rejects_missing_required_map_key() {
        let map_type = FieldType::Map(BTreeMap::from([
            ("name".into(), FieldType::Text),
            ("age".into(), FieldType::Option(Box::new(FieldType::U64))),
        ]));

        let missing_required = Cbor::Map(vec![(
            Cbor::Text("age".to_string()),
            Cbor::Integer(42.into()),
        )]);
        assert!(map_type.extract(missing_required).is_err());

        let missing_optional = Cbor::Map(vec![(
            Cbor::Text("name".to_string()),
            Cbor::Text("Ada".to_string()),
        )]);
        let extracted = map_type.extract(missing_optional).unwrap();
        assert!(map_type.validate(&extracted).is_ok());
    }

    #[test]
    fn test_field_value_conversion() {
        // Bool
        let bool_val = FieldValue::Bool(true);
        let cbor: Cbor = bool_val.clone().into();
        assert_eq!(FieldValue::try_from(cbor).unwrap(), bool_val);

        // U64
        let u64_val = FieldValue::U64(42);
        let cbor: Cbor = u64_val.clone().into();
        assert_eq!(FieldValue::try_from(cbor).unwrap(), u64_val);

        // I64
        let i64_val = FieldValue::I64(-42);
        let cbor: Cbor = i64_val.clone().into();
        assert_eq!(FieldValue::try_from(cbor).unwrap(), i64_val);

        // F64
        let f64_val = FieldValue::F64(3.15);
        let cbor: Cbor = f64_val.clone().into();
        assert_eq!(FieldValue::try_from(cbor).unwrap(), f64_val);

        // F32
        let f32_val = FieldValue::F32(2.71);
        let cbor: Cbor = f32_val.clone().into();
        // 注意：F32转换为CBOR后再转回来会变成F64
        if let FieldValue::F64(f64_val) = FieldValue::try_from(cbor).unwrap() {
            assert!((f64_val - 2.71).abs() < f32::EPSILON as f64);
        } else {
            panic!("Expected F64");
        }

        // Bytes
        let bytes_val = FieldValue::Bytes(vec![1, 2, 3]);
        let cbor: Cbor = bytes_val.clone().into();
        assert_eq!(FieldValue::try_from(cbor).unwrap(), bytes_val);

        // Text
        let text_val = FieldValue::Text("hello".to_string());
        let cbor: Cbor = text_val.clone().into();
        assert_eq!(FieldValue::try_from(cbor).unwrap(), text_val);

        // Json
        let json_val = FieldValue::Json(json!({"name": "test"}));
        let cbor: Cbor = json_val.into();
        // JSON转换为CBOR后再转回来会变成Map
        let mut expected_map = BTreeMap::new();
        expected_map.insert("name".into(), FieldValue::Text("test".to_string()));
        assert_eq!(
            FieldValue::try_from(cbor).unwrap(),
            FieldValue::Map(expected_map)
        );

        // Vector
        let vector_val = FieldValue::Vector(vec![bf16::from_f32(1.5)]);
        let cbor: Cbor = vector_val.clone().into();
        // Vector转换为CBOR后再转回来会变成Array
        let expected_array =
            FieldValue::Array(vec![FieldValue::U64(bf16::from_f32(1.5).to_bits() as u64)]);
        assert_eq!(FieldValue::try_from(cbor).unwrap(), expected_array);

        // Array
        let array_val = FieldValue::Array(vec![FieldValue::U64(1), FieldValue::U64(2)]);
        let cbor: Cbor = array_val.clone().into();
        assert_eq!(FieldValue::try_from(cbor).unwrap(), array_val);

        // Map
        let mut map = BTreeMap::new();
        map.insert("key".into(), FieldValue::Text("value".to_string()));
        let map_val = FieldValue::Map(map);
        let cbor: Cbor = map_val.clone().into();
        assert_eq!(FieldValue::try_from(cbor).unwrap(), map_val);

        // Null
        let null_val = FieldValue::Null;
        let cbor: Cbor = null_val.clone().into();
        assert_eq!(FieldValue::try_from(cbor).unwrap(), null_val);
    }

    #[test]
    fn test_field_entry() {
        // 创建字段
        let field = FieldEntry::new("user_id".to_string(), FieldType::U64)
            .unwrap()
            .with_unique()
            .with_idx(1);

        assert_eq!(field.name(), "user_id");
        assert_eq!(field.r#type(), &FieldType::U64);
        assert!(field.unique());
        assert_eq!(field.idx(), 1);

        // 测试提取值
        let val = field.extract(Cbor::Integer(42.into()), true).unwrap();
        assert_eq!(val, FieldValue::U64(42));

        // 测试验证值
        assert!(field.validate(&FieldValue::U64(42)).is_ok());
        assert!(field.validate(&FieldValue::I64(42)).is_err());

        // 测试必填字段的空值验证
        assert!(field.validate(&FieldValue::Null).is_err());

        // 测试非必填字段的空值验证
        let optional_field = FieldEntry::new(
            "optional".to_string(),
            FieldType::Option(Box::new(FieldType::U64)),
        )
        .unwrap();
        assert!(optional_field.validate(&FieldValue::Null).is_ok());
    }

    #[test]
    fn test_validate_field_name() {
        // 有效的字段名
        assert!(validate_field_name("user_id").is_ok());
        assert!(validate_field_name("a").is_ok());
        assert!(validate_field_name("a1").is_ok());
        assert!(validate_field_name("a_1").is_ok());

        // 无效的字段名
        assert!(validate_field_name("").is_err()); // 空字符串
        assert!(validate_field_name("A").is_err()); // 大写字母
        assert!(validate_field_name("user-id").is_err()); // 包含连字符
        assert!(validate_field_name("user.id").is_err()); // 包含点
        assert!(validate_field_name("user id").is_err()); // 包含空格

        // 超长字段名
        let long_name = "a".repeat(65);
        assert!(validate_field_name(&long_name).is_err());
    }

    #[test]
    fn test_serialization() {
        // 测试 FieldType 序列化和反序列化
        let field_type = Ft::Array(vec![Ft::U64, Ft::Text]);
        let serialized = serde_json::to_string(&field_type).unwrap();
        println!("Serialized FieldType: {serialized}");
        let deserialized: Ft = serde_json::from_str(&serialized).unwrap();
        assert_eq!(field_type, deserialized);
        let mut serialized = Vec::new();
        into_writer(&field_type, &mut serialized).unwrap();
        println!("Serialized FieldType: {:?}", hex::encode(&serialized));
        let deserialized: Ft = from_reader(&serialized[..]).unwrap();
        assert_eq!(field_type, deserialized);

        // 测试 FieldValue 序列化和反序列化
        let field_value = Fv::Array(vec![Fv::U64(1), Fv::Text("hello".to_string())]);
        let mut serialized = Vec::new();
        into_writer(&field_value, &mut serialized).unwrap();
        println!("Serialized FieldValue: {:?}", hex::encode(&serialized));
        assert_eq!(hex::encode(&serialized), "82016568656c6c6f");
        let deserialized: Fv = from_reader(&serialized[..]).unwrap();
        assert_eq!(field_value, deserialized);

        let field_value = Fv::Bytes(vec![1, 2, 3, 4]);
        let mut serialized = Vec::new();
        into_writer(&field_value, &mut serialized).unwrap();
        println!("Serialized bytes: {:?}", hex::encode(&serialized));
        assert_eq!(hex::encode(&serialized), "4401020304");
        let deserialized: Fv = from_reader(&serialized[..]).unwrap();
        assert_eq!(field_value, deserialized);

        // 测试 FieldEntry 序列化和反序列化
        let field_entry = Fe::new("id".to_string(), Ft::Bytes)
            .unwrap()
            .with_unique()
            .with_idx(0);
        let mut serialized = Vec::new();
        into_writer(&field_entry, &mut serialized).unwrap();
        let deserialized: Fe = from_reader(&serialized[..]).unwrap();
        assert_eq!(field_entry, deserialized);

        let xid = Xid([1u8; 12]);
        let mut data = Vec::new();
        into_writer(&xid, &mut data).unwrap();
        println!("Serialized Xid: {:?}", hex::encode(&data));
        assert_eq!(hex::encode(&data), "4c010101010101010101010101");
        let cb: Cbor = from_reader(&data[..]).unwrap();
        let fv: FieldValue = FieldValue::try_from(cb).unwrap();
        let deserialized_xid: Xid = fv.deserialized().unwrap();
        assert_eq!(xid, deserialized_xid);

        let vv = vec![
            [bf16::from_f32(1.0), bf16::from_f32(1.1)],
            [bf16::from_f32(2.0), bf16::from_f32(2.1)],
        ];
        // bf16 使用了 u16 存储，未提供 Ft 时将序列化成 u64
        let fv = Fv::serialized(&vv, None).unwrap();
        assert_eq!(
            fv,
            Fv::Array(vec![
                Fv::Array(vec![Fv::U64(16256), Fv::U64(16269)]),
                Fv::Array(vec![Fv::U64(16384), Fv::U64(16390),])
            ])
        );
        // 虽然 Fv 类型不对，但还是可以反序列化成 Vec<[bf16; 2]>
        let vv2: Vec<[bf16; 2]> = fv.deserialized().unwrap();
        assert_eq!(vv, vv2);

        // 提供了 Ft 后才能完全正确的序列化
        let fv = Fv::serialized(&vv, Some(&Ft::Array(vec![Ft::Vector]))).unwrap();
        assert_eq!(
            fv,
            Fv::Array(vec![
                Fv::Vector(vec![bf16::from_f32(1.0), bf16::from_f32(1.1)]),
                Fv::Vector(vec![bf16::from_f32(2.0), bf16::from_f32(2.1),])
            ])
        );
        let vv2: Vec<[bf16; 2]> = fv.deserialized().unwrap();
        assert_eq!(vv, vv2);
    }

    #[test]
    fn test_nan_field_value_rejected_by_serde() {
        assert!(serde_json::to_string(&Fv::F64(f64::NAN)).is_err());
        assert!(serde_json::to_string(&Fv::F32(f32::NAN)).is_err());

        let mut serialized = Vec::new();
        into_writer(&Cbor::Float(f64::NAN), &mut serialized).unwrap();
        assert!(from_reader::<Fv, _>(&serialized[..]).is_err());
    }

    #[test]
    fn field_type_and_key_helpers_cover_byte_keys_and_wildcards() {
        assert!(!FieldType::Text.allows_null());
        assert!(FieldType::Option(Box::new(FieldType::Text)).allows_null());
        assert!(
            FieldType::Array(vec![])
                .validate(&FieldValue::Array(vec![]))
                .is_ok()
        );

        let text_key = FieldKey::from("name".to_string());
        assert_eq!(text_key.field_type(), FieldType::Text);
        assert_eq!(text_key.as_bytes(), b"name");
        assert_eq!(text_key.to_string(), "name");

        let bytes_from_vec = FieldKey::from(vec![1, 2, 3]);
        let bytes_from_array = FieldKey::from([4, 5, 6]);
        let bytes_from_slice = FieldKey::from(&[7, 8, 9][..]);
        assert_eq!(bytes_from_vec.field_type(), FieldType::Bytes);
        assert_eq!(bytes_from_vec.as_bytes(), &[1, 2, 3]);
        assert_eq!(bytes_from_array, FieldKey::Bytes(vec![4, 5, 6]));
        assert_eq!(bytes_from_slice, FieldKey::Bytes(vec![7, 8, 9]));
        assert_eq!(bytes_from_vec.to_string(), "AQID");

        assert_eq!(
            FieldKey::try_from(Value::Bytes(vec![10, 11])).unwrap(),
            FieldKey::Bytes(vec![10, 11])
        );
        assert!(FieldKey::try_from(Value::Bool(true)).is_err());
        assert_eq!(*BYTES_WILDCARD_KEY, FieldKey::Bytes(b"*".to_vec()));

        let wildcard_type = FieldType::Map(BTreeMap::from([(
            FieldKey::from(b"*".as_slice()),
            FieldType::U64,
        )]));
        let wildcard_value = FieldValue::Map(BTreeMap::from([
            (FieldKey::from(vec![0]), FieldValue::U64(1)),
            (FieldKey::from(vec![1]), FieldValue::U64(2)),
        ]));
        assert!(wildcard_type.validate(&wildcard_value).is_ok());

        let wildcard_cbor = Cbor::Map(vec![
            (Cbor::Bytes(vec![0]), Cbor::Integer(1.into())),
            (Cbor::Bytes(vec![1]), Cbor::Integer(2.into())),
        ]);
        assert_eq!(
            wildcard_type.extract(wildcard_cbor).unwrap(),
            wildcard_value
        );

        let fixed_type = FieldType::Map(BTreeMap::from([
            (FieldKey::from(b"id".as_slice()), FieldType::U64),
            (
                FieldKey::from(b"optional".as_slice()),
                FieldType::Option(Box::new(FieldType::Text)),
            ),
        ]));
        let missing_optional = FieldValue::Map(BTreeMap::from([(
            FieldKey::from(b"id".as_slice()),
            FieldValue::U64(9),
        )]));
        assert!(fixed_type.validate(&missing_optional).is_ok());
        let invalid_key = FieldValue::Map(BTreeMap::from([(
            FieldKey::from(b"unknown".as_slice()),
            FieldValue::U64(9),
        )]));
        assert!(fixed_type.validate(&invalid_key).is_err());
    }

    #[test]
    fn field_value_from_impls_cover_collections_and_cbor_byte_map_branch() {
        assert_eq!(FieldValue::from(true), FieldValue::Bool(true));
        assert_eq!(FieldValue::from(-7_i64), FieldValue::I64(-7));
        assert_eq!(FieldValue::from(7_u64), FieldValue::U64(7));
        assert_eq!(FieldValue::from(1.5_f64), FieldValue::F64(1.5));
        assert_eq!(FieldValue::from(2.5_f32), FieldValue::F32(2.5));
        assert_eq!(
            FieldValue::from(vec![1_u8, 2, 3]),
            FieldValue::Bytes(vec![1, 2, 3])
        );
        assert_eq!(
            FieldValue::from("hello".to_string()),
            FieldValue::Text("hello".to_string())
        );
        assert_eq!(
            FieldValue::from(json!({"a": 1})),
            FieldValue::Json(json!({"a": 1}))
        );

        let vector = vec![bf16::from_f32(1.0), bf16::from_f32(2.0)];
        assert_eq!(FieldValue::from(vector.clone()), FieldValue::Vector(vector));

        let from_vec: FieldValue = vec![1_u64, 2_u64].into();
        assert_eq!(
            from_vec,
            FieldValue::Array(vec![FieldValue::U64(1), FieldValue::U64(2)])
        );

        let mut ordered = BTreeSet::new();
        ordered.insert(1_u64);
        ordered.insert(2_u64);
        assert_eq!(FieldValue::from(ordered), from_vec);

        let mut unordered = HashSet::new();
        unordered.insert(1_u64);
        unordered.insert(2_u64);
        let mut unordered_values = match FieldValue::from(unordered) {
            FieldValue::Array(values) => values,
            other => panic!("expected array, got {other:?}"),
        };
        unordered_values.sort_by_key(|v| match v {
            FieldValue::U64(v) => *v,
            other => panic!("unexpected value {other:?}"),
        });
        assert_eq!(
            unordered_values,
            vec![FieldValue::U64(1), FieldValue::U64(2)]
        );

        let tree_map = BTreeMap::from([(FieldKey::from(vec![1, 2]), 9_u64)]);
        let tree_map_value = FieldValue::from(tree_map);
        assert_eq!(
            tree_map_value,
            FieldValue::Map(BTreeMap::from([(
                FieldKey::Bytes(vec![1, 2]),
                FieldValue::U64(9)
            )]))
        );
        let cbor: Cbor = tree_map_value.into();
        assert_eq!(
            cbor,
            Cbor::Map(vec![(Cbor::Bytes(vec![1, 2]), Cbor::Integer(9.into()))])
        );

        let hash_map = HashMap::from([("answer".to_string(), 42_u64)]);
        assert_eq!(
            FieldValue::from(hash_map),
            FieldValue::Map(BTreeMap::from([(
                FieldKey::Text("answer".to_string()),
                FieldValue::U64(42),
            )]))
        );

        let json_map = serde_json::Map::from_iter([("flag".to_string(), json!(true))]);
        assert_eq!(
            FieldValue::from(json_map),
            FieldValue::Map(BTreeMap::from([(
                FieldKey::Text("flag".to_string()),
                FieldValue::Json(json!(true)),
            )]))
        );

        assert_eq!(
            FieldValue::from(FieldKey::Text("key".to_string())),
            FieldValue::Text("key".to_string())
        );
        assert_eq!(
            FieldValue::from(FieldKey::Bytes(vec![1, 2])),
            FieldValue::Bytes(vec![1, 2])
        );
    }

    #[test]
    fn field_value_try_from_impls_cover_success_and_error_paths() {
        assert!(bool::try_from(FieldValue::Bool(true)).unwrap());
        assert!(bool::try_from(&FieldValue::Bool(true)).unwrap());
        assert!(bool::try_from(FieldValue::Text("no".into())).is_err());
        assert!(bool::try_from(&FieldValue::Text("no".into())).is_err());

        let i64_value = FieldValue::I64(-9);
        assert_eq!(i64::try_from(i64_value.clone()).unwrap(), -9);
        assert_eq!(i64::try_from(&i64_value).unwrap(), -9);
        assert_eq!(<&i64>::try_from(&i64_value).unwrap(), &-9);
        assert!(i64::try_from(FieldValue::U64(9)).is_err());
        assert!(i64::try_from(&FieldValue::U64(9)).is_err());
        assert!(<&i64>::try_from(&FieldValue::U64(9)).is_err());

        let u64_value = FieldValue::U64(9);
        assert_eq!(u64::try_from(u64_value.clone()).unwrap(), 9);
        assert_eq!(u64::try_from(&u64_value).unwrap(), 9);
        assert_eq!(<&u64>::try_from(&u64_value).unwrap(), &9);
        assert!(u64::try_from(FieldValue::I64(-9)).is_err());
        assert!(u64::try_from(&FieldValue::I64(-9)).is_err());
        assert!(<&u64>::try_from(&FieldValue::I64(-9)).is_err());

        assert_eq!(f64::try_from(FieldValue::F64(1.25)).unwrap(), 1.25);
        assert_eq!(f64::try_from(&FieldValue::F64(1.25)).unwrap(), 1.25);
        assert!(f64::try_from(FieldValue::F32(1.25)).is_err());
        assert!(f64::try_from(&FieldValue::F32(1.25)).is_err());

        assert_eq!(f32::try_from(FieldValue::F32(1.25)).unwrap(), 1.25);
        assert_eq!(f32::try_from(&FieldValue::F32(1.25)).unwrap(), 1.25);
        assert!(f32::try_from(FieldValue::F64(1.25)).is_err());
        assert!(f32::try_from(&FieldValue::F64(1.25)).is_err());

        let bytes = FieldValue::Bytes(vec![1, 2, 3]);
        assert_eq!(Vec::<u8>::try_from(bytes.clone()).unwrap(), vec![1, 2, 3]);
        assert_eq!(<&Vec<u8>>::try_from(&bytes).unwrap(), &vec![1, 2, 3]);
        assert_eq!(<[u8; 3]>::try_from(bytes.clone()).unwrap(), [1, 2, 3]);
        assert!(<[u8; 2]>::try_from(bytes.clone()).is_err());
        assert!(Vec::<u8>::try_from(FieldValue::Text("bytes".into())).is_err());
        assert!(<&Vec<u8>>::try_from(&FieldValue::Text("bytes".into())).is_err());
        assert!(<[u8; 3]>::try_from(FieldValue::Text("bytes".into())).is_err());

        let text = FieldValue::Text("hello".to_string());
        assert_eq!(String::try_from(text.clone()).unwrap(), "hello");
        assert_eq!(<&String>::try_from(&text).unwrap(), "hello");
        assert_eq!(<&str>::try_from(&text).unwrap(), "hello");
        assert!(String::try_from(FieldValue::Bytes(vec![])).is_err());
        assert!(<&String>::try_from(&FieldValue::Bytes(vec![])).is_err());
        assert!(<&str>::try_from(&FieldValue::Bytes(vec![])).is_err());

        let json_value = FieldValue::Json(json!({"name": "Ada"}));
        assert_eq!(
            Json::try_from(json_value.clone()).unwrap(),
            json!({"name": "Ada"})
        );
        assert_eq!(
            <&Json>::try_from(&json_value).unwrap(),
            &json!({"name": "Ada"})
        );
        assert!(Json::try_from(FieldValue::Text("json".into())).is_err());
        assert!(<&Json>::try_from(&FieldValue::Text("json".into())).is_err());

        let vector = FieldValue::Vector(vec![bf16::from_f32(1.0), bf16::from_f32(2.0)]);
        assert_eq!(
            Vec::<bf16>::try_from(vector.clone()).unwrap(),
            vec![bf16::from_f32(1.0), bf16::from_f32(2.0),]
        );
        assert_eq!(
            <&Vec<bf16>>::try_from(&vector).unwrap(),
            &vec![bf16::from_f32(1.0), bf16::from_f32(2.0),]
        );
        assert_eq!(
            <[bf16; 2]>::try_from(vector.clone()).unwrap(),
            [bf16::from_f32(1.0), bf16::from_f32(2.0)]
        );
        assert!(<[bf16; 3]>::try_from(vector.clone()).is_err());
        assert!(Vec::<bf16>::try_from(FieldValue::Array(vec![])).is_err());
        assert!(<&Vec<bf16>>::try_from(&FieldValue::Array(vec![])).is_err());
        assert!(<[bf16; 2]>::try_from(FieldValue::Array(vec![])).is_err());

        let array = FieldValue::Array(vec![FieldValue::U64(1), FieldValue::U64(2)]);
        assert_eq!(Vec::<u64>::try_from(array.clone()).unwrap(), vec![1, 2]);
        assert_eq!(Vec::<&u64>::try_from(&array).unwrap(), vec![&1, &2]);
        assert!(Vec::<u64>::try_from(FieldValue::U64(1)).is_err());
        assert!(Vec::<&u64>::try_from(&FieldValue::U64(1)).is_err());
        assert!(
            Vec::<u64>::try_from(FieldValue::Array(vec![FieldValue::Text("bad".into())])).is_err()
        );
        assert!(
            Vec::<&u64>::try_from(&FieldValue::Array(vec![FieldValue::Text("bad".into())]))
                .is_err()
        );

        let map = FieldValue::Map(BTreeMap::from([("id".into(), FieldValue::U64(42))]));
        let converted = BTreeMap::<FieldKey, u64>::try_from(map).unwrap();
        assert_eq!(converted.get(&FieldKey::Text("id".into())), Some(&42));
        assert!(BTreeMap::<FieldKey, u64>::try_from(FieldValue::U64(1)).is_err());
        assert!(
            BTreeMap::<FieldKey, u64>::try_from(FieldValue::Map(BTreeMap::from([(
                "bad".into(),
                FieldValue::Text("not u64".into()),
            )])))
            .is_err()
        );
    }

    #[test]
    fn field_value_extract_error_branches_and_accessors_are_exercised() {
        assert!(FieldValue::i64_from(Cbor::Integer(u64::MAX.into())).is_err());
        assert!(FieldValue::i64_from(Cbor::Text("bad".into())).is_err());
        assert!(FieldValue::u64_from(Cbor::Integer((-1).into())).is_err());
        assert!(FieldValue::u64_from(Cbor::Text("bad".into())).is_err());
        assert!(FieldValue::f64_from(Cbor::Float(f64::NAN)).is_err());
        assert!(FieldValue::f32_from(Cbor::Float(f64::NAN)).is_err());
        assert!(
            FieldValue::json_from(Cbor::Map(vec![(Cbor::Bytes(vec![1]), Cbor::Null,)])).is_err()
        );
        assert!(FieldValue::vector_from(Cbor::Text("bad".into())).is_err());
        assert!(FieldValue::bf16_from(Cbor::Integer((u64::from(u16::MAX) + 1).into())).is_err());
        assert!(FieldValue::bf16_from(Cbor::Text("bad".into())).is_err());
        assert!(FieldValue::array_from(Cbor::Text("bad".into()), &[]).is_err());
        assert!(
            FieldValue::array_from(
                Cbor::Array(vec![Cbor::Integer(1.into())]),
                &[FieldType::U64, FieldType::Text],
            )
            .is_err()
        );
        assert!(FieldValue::map_from(Cbor::Text("bad".into()), &BTreeMap::new()).is_err());
        assert!(
            FieldValue::map_from(
                Cbor::Map(vec![(Cbor::Integer(1.into()), Cbor::Text("bad".into()))]),
                &BTreeMap::new(),
            )
            .is_err()
        );
        assert!(
            FieldValue::map_from(
                Cbor::Map(vec![(Cbor::Integer(1.into()), Cbor::Text("bad".into()))]),
                &BTreeMap::from([("name".into(), FieldType::Text)]),
            )
            .is_err()
        );
        assert!(
            FieldValue::map_from(
                Cbor::Map(vec![(
                    Cbor::Text("unknown".into()),
                    Cbor::Text("bad".into())
                )]),
                &BTreeMap::from([("name".into(), FieldType::Text)]),
            )
            .is_err()
        );
        assert_eq!(
            FieldValue::try_from(Cbor::Tag(1, Box::new(Cbor::Text("tagged".to_string())),))
                .unwrap(),
            FieldValue::Text("tagged".to_string())
        );

        let map = FieldValue::Map(BTreeMap::from([(
            FieldKey::Text("name".into()),
            FieldValue::Text("Ada".into()),
        )]));
        assert_eq!(
            map.get_field_as::<str>(&FieldKey::Text("name".into())),
            Some("Ada")
        );
        assert_eq!(
            map.get_field_as::<str>(&FieldKey::Text("missing".into())),
            None
        );
        assert_eq!(
            FieldValue::Text("Ada".into()).get_field_as::<str>(&FieldKey::Text("name".into())),
            None
        );

        let mut entry = FieldEntry::new("nickname".to_string(), FieldType::Text)
            .unwrap()
            .with_description("Display name".to_string());
        assert_eq!(entry.name(), "nickname");
        assert_eq!(entry.r#type(), &FieldType::Text);
        assert!(entry.required());
        assert!(!entry.unique());
        assert_eq!(entry.idx(), 0);
        assert_eq!(entry.set_idx(3).idx(), 3);
        assert_eq!(
            entry.extract(Cbor::Text("Ada".to_string()), false).unwrap(),
            FieldValue::Text("Ada".into())
        );
        assert!(entry.extract(Cbor::Integer(1.into()), true).is_err());
    }
}
