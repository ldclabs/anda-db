//! Typed map keys and wildcard recognition.
use super::*;
use base64::{Engine, prelude::BASE64_URL_SAFE};
use cbor2::Value;
/// A key in a [`FieldType::Map`] / [`FieldValue::Map`].
///
/// Map keys may be UTF-8 [`FieldKey::Text`], signed 64-bit
/// [`FieldKey::I64`] integers, or arbitrary [`FieldKey::Bytes`]. The
/// variants are kept distinct in CBOR so that a `Bytes` or `I64` key is never
/// confused with the textual representation of the same payload.
///
/// In JSON serialization, an `I64` key is rendered as `i64:<decimal>` and a
/// `Bytes` key as `b64:<url-safe base64>`; a `Text` key that itself starts
/// with a reserved prefix (`i64:`, `b64:`, `txt:`) is escaped as
/// `txt:<original>`. Only these explicit prefixes are interpreted on the way
/// back — ordinary text always round-trips verbatim.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FieldKey {
    /// A UTF-8 text key.
    Text(String),
    /// A signed 64-bit integer key.
    I64(i64),
    /// An arbitrary byte-string key.
    Bytes(Vec<u8>),
}

/// The wildcard text key (`"*"`) used to express a homogeneous `Map<Text, T>`.
pub static TEXT_WILDCARD_KEY: std::sync::LazyLock<FieldKey> =
    std::sync::LazyLock::new(|| "*".into());

/// The wildcard byte key (`b"*"`) used to express a homogeneous `Map<Bytes, T>`.
pub static BYTES_WILDCARD_KEY: std::sync::LazyLock<FieldKey> =
    std::sync::LazyLock::new(|| b"*".into());

/// The wildcard integer key (`i64::MIN`) used to express a homogeneous
/// `Map<I64, T>`.
pub static I64_WILDCARD_KEY: std::sync::LazyLock<FieldKey> =
    std::sync::LazyLock::new(|| FieldKey::I64(i64::MIN));

impl FieldKey {
    /// Returns `true` when this key is the wildcard sentinel of its variant:
    /// `"*"` for text, `b"*"` for bytes and `i64::MIN` for integers.
    ///
    /// A [`FieldType::Map`] whose *only* entry carries a wildcard key is a
    /// homogeneous map (`Map<Text, T>`, `Map<Bytes, T>`, `Map<I64, T>`): any
    /// key of that variant is accepted and every value must have type `T`.
    /// See [`as_wildcard_map`].
    pub fn is_wildcard(&self) -> bool {
        match self {
            FieldKey::Text(s) => s == "*",
            FieldKey::I64(i) => *i == i64::MIN,
            FieldKey::Bytes(b) => b.as_slice() == b"*",
        }
    }

    /// Returns the [`FieldType`] that the key itself uses
    /// ([`FieldType::Text`] for `Text`, [`FieldType::I64`] for `I64`,
    /// [`FieldType::Bytes`] for `Bytes`).
    pub fn field_type(&self) -> FieldType {
        match self {
            FieldKey::Text(_) => FieldType::Text,
            FieldKey::I64(_) => FieldType::I64,
            FieldKey::Bytes(_) => FieldType::Bytes,
        }
    }

    /// Borrow the raw bytes of this key, regardless of variant.
    ///
    /// Integer keys are exposed as native-endian bytes of the stored `i64`.
    /// Use CBOR serialization when a stable cross-platform wire encoding is
    /// required.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            FieldKey::Text(s) => s.as_bytes(),
            FieldKey::I64(i) => {
                // SAFETY: `i` is stored inside `self`, so the returned byte
                // slice cannot outlive the referenced integer. Any bit pattern
                // is valid for `u8`, and the slice length is exactly the size
                // of the integer.
                unsafe {
                    std::slice::from_raw_parts(
                        (i as *const i64).cast::<u8>(),
                        std::mem::size_of::<i64>(),
                    )
                }
            }
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

impl From<i8> for FieldKey {
    fn from(i: i8) -> Self {
        FieldKey::I64(i.into())
    }
}

impl From<i16> for FieldKey {
    fn from(i: i16) -> Self {
        FieldKey::I64(i.into())
    }
}

impl From<i32> for FieldKey {
    fn from(i: i32) -> Self {
        FieldKey::I64(i.into())
    }
}

impl From<i64> for FieldKey {
    fn from(i: i64) -> Self {
        FieldKey::I64(i)
    }
}

impl From<isize> for FieldKey {
    fn from(i: isize) -> Self {
        FieldKey::I64(i as i64)
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
            Value::Integer(i) => Ok(FieldKey::I64(i.try_into().map_err(|v| {
                SchemaError::FieldValue(format!("expected I64 map key, got {v:?}"))
            })?)),
            Value::Bytes(b) => Ok(FieldKey::Bytes(b)),
            // `Vec<u8>` / `[u8; N]` map keys reach CBOR as an integer array
            // (serde has no byte-string specialization for them) — the same
            // shape `FieldValue::bytes_from` accepts for values.
            Value::Array(arr) => Ok(FieldKey::Bytes(u8_array_from(arr)?)),
            _ => Err(SchemaError::FieldValue(format!(
                "expected Text, I64, Bytes or an array of 0..=255 as map key, got {value:?}"
            ))
            .into()),
        }
    }
}

impl std::fmt::Display for FieldKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldKey::Text(s) => write!(f, "{s}"),
            FieldKey::I64(i) => write!(f, "{i}"),
            FieldKey::Bytes(b) => write!(f, "{}", BASE64_URL_SAFE.encode(b)),
        }
    }
}

/// If `m` describes a *wildcard* map — i.e. it has exactly one entry whose
/// key [`is_wildcard`](FieldKey::is_wildcard) (`"*"`, `b"*"` or `i64::MIN`)
/// — return that entry: the sentinel key, whose variant is the declared key
/// type, and the value type every entry must have. Otherwise return `None`.
///
/// Every other `Map` declares its keys explicitly (this is what
/// `#[derive(FieldTyped)]` emits for a nested struct), so it is homogeneous in
/// neither key nor value. Consumers outside this crate — index key-type
/// resolution in particular — must use this function rather than approximating
/// it with a one-entry check, or the two layers disagree about which maps are
/// wildcards.
pub fn as_wildcard_map(m: &BTreeMap<FieldKey, FieldType>) -> Option<(&FieldKey, &FieldType)> {
    if m.len() != 1 {
        return None;
    }
    m.iter().next().filter(|(key, _)| key.is_wildcard())
}

/// Returns an error when `key` is not the key variant that the wildcard
/// sentinel `wildcard` declares.
///
/// A wildcard sentinel pins the key *type* as well as the value type: values
/// stored under a key of another variant validate structurally but can never
/// be deserialized back into the declared Rust type (a `BTreeMap<String, _>`
/// rejects an integer key).
pub(super) fn check_wildcard_key(key: &FieldKey, wildcard: &FieldKey) -> Result<(), SchemaError> {
    if key.field_type() != wildcard.field_type() {
        return Err(SchemaError::FieldValue(format!(
            "invalid map key {key:?}, expected a {:?} key",
            wildcard.field_type()
        )));
    }
    Ok(())
}
