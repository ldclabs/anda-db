//! Hand-written `serde` implementations for [`FieldKey`] and [`FieldValue`].
//!
//! `FieldValue` cannot use the derived `Deserialize` because it must be
//! reconstructable from any self-describing serde data model (CBOR, JSON).
//! The visitor in this module walks the data model directly
//! and chooses the most precise variant for the input.
//!
//! In *human-readable* formats (e.g. JSON), [`FieldValue::Bytes`] and
//! [`FieldKey::Bytes`] are encoded as URL-safe Base64 strings. On the way
//! back, a textual value that successfully decodes as Base64 is treated as
//! `Bytes`. This is the same trick used by `ic_auth_types::ByteBufB64`.
use base64::{Engine, prelude::BASE64_URL_SAFE};
use serde::{
    de,
    ser::{Serialize, SerializeMap, SerializeSeq, Serializer},
};
use std::collections::BTreeMap;

use crate::{FieldKey, FieldValue};

impl Serialize for FieldKey {
    #[inline]
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            FieldKey::Text(x) => serializer.serialize_str(x),
            FieldKey::Bytes(x) => {
                if serializer.is_human_readable() {
                    BASE64_URL_SAFE.encode(x).serialize(serializer)
                } else {
                    serializer.serialize_bytes(x)
                }
            }
        }
    }
}

impl<'de> de::Deserialize<'de> for FieldKey {
    #[inline]
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let is_human_readable = deserializer.is_human_readable();
        let val = deserializer.deserialize_any(KeyVisitor)?;

        if is_human_readable
            && let FieldKey::Text(x) = &val
            && let Ok(decoded) = BASE64_URL_SAFE.decode(x)
        {
            return Ok(FieldKey::Bytes(decoded));
        }
        Ok(val)
    }
}

impl Serialize for FieldValue {
    #[inline]
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            FieldValue::Bool(x) => serializer.serialize_bool(*x),
            FieldValue::I64(x) => serializer.serialize_i64(*x),
            FieldValue::U64(x) => serializer.serialize_u64(*x),
            FieldValue::F64(x) => serializer.serialize_f64(*x),
            FieldValue::F32(x) => serializer.serialize_f32(*x),
            FieldValue::Bytes(x) => {
                if serializer.is_human_readable() {
                    BASE64_URL_SAFE.encode(x).serialize(serializer)
                } else {
                    serializer.serialize_bytes(x)
                }
            }
            FieldValue::Text(x) => serializer.serialize_str(x),
            FieldValue::Json(x) => x.serialize(serializer),
            FieldValue::Null => serializer.serialize_unit(),
            FieldValue::Vector(x) => {
                let mut seq = serializer.serialize_seq(Some(x.len()))?;
                for v in x {
                    seq.serialize_element(&v.to_bits())?;
                }
                seq.end()
            }
            FieldValue::Array(x) => {
                let mut seq = serializer.serialize_seq(Some(x.len()))?;
                for v in x {
                    seq.serialize_element(v)?;
                }
                seq.end()
            }
            FieldValue::Map(x) => {
                let mut map = serializer.serialize_map(Some(x.len()))?;
                for (k, v) in x {
                    map.serialize_entry(k, v)?;
                }
                map.end()
            }
        }
    }
}

impl<'de> de::Deserialize<'de> for FieldValue {
    #[inline]
    fn deserialize<D: de::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let is_human_readable = deserializer.is_human_readable();
        let val = deserializer.deserialize_any(Visitor)?;

        if is_human_readable
            && let FieldValue::Text(x) = &val
            && let Ok(decoded) = BASE64_URL_SAFE.decode(x)
        {
            return Ok(FieldValue::Bytes(decoded));
        }
        Ok(val)
    }
}

struct KeyVisitor;

impl<'de> de::Visitor<'de> for KeyVisitor {
    type Value = FieldKey;

    fn expecting(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(formatter, "string or bytes")
    }

    #[inline]
    fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
        Ok(FieldKey::Text(v.into()))
    }

    #[inline]
    fn visit_borrowed_str<E: de::Error>(self, v: &'de str) -> Result<Self::Value, E> {
        Ok(FieldKey::Text(v.into()))
    }

    #[inline]
    fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
        Ok(FieldKey::Text(v))
    }

    #[inline]
    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
        Ok(FieldKey::Bytes(v.to_vec()))
    }

    #[inline]
    fn visit_borrowed_bytes<E: de::Error>(self, v: &'de [u8]) -> Result<Self::Value, E> {
        Ok(FieldKey::Bytes(v.to_vec()))
    }

    #[inline]
    fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
        Ok(FieldKey::Bytes(v))
    }

    #[inline]
    fn visit_seq<A: de::SeqAccess<'de>>(self, mut acc: A) -> Result<Self::Value, A::Error> {
        let mut seq: Vec<u8> = Vec::new();

        while let Some(elem) = acc.next_element()? {
            seq.push(elem);
        }

        Ok(FieldKey::Bytes(seq))
    }
}

struct Visitor;

impl<'de> de::Visitor<'de> for Visitor {
    type Value = FieldValue;

    fn expecting(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(formatter, "a valid CBOR item")
    }

    #[inline]
    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Self::Value, E> {
        Ok(FieldValue::Bool(v))
    }

    #[inline]
    fn visit_f32<E: de::Error>(self, v: f32) -> Result<Self::Value, E> {
        Ok(FieldValue::F32(v))
    }

    #[inline]
    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
        Ok(FieldValue::F64(v))
    }

    #[inline]
    fn visit_i8<E: de::Error>(self, v: i8) -> Result<Self::Value, E> {
        Ok(FieldValue::I64(v.into()))
    }

    #[inline]
    fn visit_i16<E: de::Error>(self, v: i16) -> Result<Self::Value, E> {
        Ok(FieldValue::I64(v.into()))
    }

    #[inline]
    fn visit_i32<E: de::Error>(self, v: i32) -> Result<Self::Value, E> {
        Ok(FieldValue::I64(v.into()))
    }

    #[inline]
    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
        Ok(FieldValue::I64(v))
    }

    #[inline]
    fn visit_i128<E: de::Error>(self, v: i128) -> Result<Self::Value, E> {
        Ok(FieldValue::I64(
            i64::try_from(v).map_err(|_| de::Error::custom("i128 overflow"))?,
        ))
    }

    #[inline]
    fn visit_u8<E: de::Error>(self, v: u8) -> Result<Self::Value, E> {
        Ok(FieldValue::U64(v.into()))
    }

    #[inline]
    fn visit_u16<E: de::Error>(self, v: u16) -> Result<Self::Value, E> {
        Ok(FieldValue::U64(v.into()))
    }

    #[inline]
    fn visit_u32<E: de::Error>(self, v: u32) -> Result<Self::Value, E> {
        Ok(FieldValue::U64(v.into()))
    }

    #[inline]
    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
        Ok(FieldValue::U64(v))
    }

    #[inline]
    fn visit_u128<E: de::Error>(self, v: u128) -> Result<Self::Value, E> {
        Ok(FieldValue::U64(
            u64::try_from(v).map_err(|_| de::Error::custom("u128 overflow"))?,
        ))
    }

    #[inline]
    fn visit_char<E: de::Error>(self, v: char) -> Result<Self::Value, E> {
        Ok(FieldValue::Text(v.into()))
    }

    #[inline]
    fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
        Ok(FieldValue::Text(v.into()))
    }

    #[inline]
    fn visit_borrowed_str<E: de::Error>(self, v: &'de str) -> Result<Self::Value, E> {
        Ok(FieldValue::Text(v.into()))
    }

    #[inline]
    fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
        Ok(FieldValue::Text(v))
    }

    #[inline]
    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
        Ok(FieldValue::Bytes(v.to_vec()))
    }

    #[inline]
    fn visit_borrowed_bytes<E: de::Error>(self, v: &'de [u8]) -> Result<Self::Value, E> {
        Ok(FieldValue::Bytes(v.to_vec()))
    }

    #[inline]
    fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
        Ok(FieldValue::Bytes(v))
    }

    #[inline]
    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(FieldValue::Null)
    }

    #[inline]
    fn visit_some<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(self)
    }

    #[inline]
    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(FieldValue::Null)
    }

    #[inline]
    fn visit_newtype_struct<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(self)
    }

    #[inline]
    fn visit_seq<A: de::SeqAccess<'de>>(self, mut acc: A) -> Result<Self::Value, A::Error> {
        let mut seq = Vec::new();

        while let Some(elem) = acc.next_element()? {
            seq.push(elem);
        }

        Ok(FieldValue::Array(seq))
    }

    #[inline]
    fn visit_map<A: de::MapAccess<'de>>(self, mut acc: A) -> Result<Self::Value, A::Error> {
        let mut map = Vec::<(FieldKey, FieldValue)>::with_capacity(
            acc.size_hint().filter(|&l| l < 1024).unwrap_or(0),
        );

        while let Some(kv) = acc.next_entry()? {
            map.push(kv);
        }

        Ok(FieldValue::Map(BTreeMap::from_iter(map)))
    }
}
