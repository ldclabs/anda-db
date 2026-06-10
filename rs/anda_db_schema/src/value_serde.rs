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
            FieldValue::F64(x) => {
                if x.is_nan() {
                    return Err(serde::ser::Error::custom("cannot serialize NaN F64"));
                }
                serializer.serialize_f64(*x)
            }
            FieldValue::F32(x) => {
                if x.is_nan() {
                    return Err(serde::ser::Error::custom("cannot serialize NaN F32"));
                }
                serializer.serialize_f32(*x)
            }
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
        let mut seq: Vec<u8> =
            Vec::with_capacity(acc.size_hint().filter(|&l| l < 1024).unwrap_or(0));

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
        if v.is_nan() {
            return Err(E::custom("cannot deserialize NaN F32"));
        }
        Ok(FieldValue::F32(v))
    }

    #[inline]
    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
        if v.is_nan() {
            return Err(E::custom("cannot deserialize NaN F64"));
        }
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
        let mut seq: Vec<FieldValue> =
            Vec::with_capacity(acc.size_hint().filter(|&l| l < 1024).unwrap_or(0));

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FieldKey, FieldValue};
    use base64::{Engine, prelude::BASE64_URL_SAFE};
    use half::bf16;
    use serde::{
        Deserialize as _,
        de::{IntoDeserializer, Visitor as _},
    };
    use serde_json::json;

    fn cbor_roundtrip<T, U>(value: &T) -> U
    where
        T: Serialize,
        U: for<'de> de::Deserialize<'de>,
    {
        let mut bytes = Vec::new();
        ciborium::into_writer(value, &mut bytes).unwrap();
        ciborium::from_reader(bytes.as_slice()).unwrap()
    }

    #[test]
    fn field_key_serde_covers_text_bytes_and_human_readable_base64() {
        let text = FieldKey::Text("not base64!".into());
        let encoded_text = serde_json::to_string(&text).unwrap();
        assert_eq!(encoded_text, r#""not base64!""#);
        assert_eq!(
            serde_json::from_str::<FieldKey>(&encoded_text).unwrap(),
            text
        );

        let bytes = FieldKey::Bytes(vec![1, 2, 3]);
        let encoded_bytes = serde_json::to_string(&bytes).unwrap();
        assert_eq!(encoded_bytes, r#""AQID""#);
        assert_eq!(
            serde_json::from_str::<FieldKey>(&encoded_bytes).unwrap(),
            bytes
        );

        assert_eq!(
            cbor_roundtrip::<_, FieldKey>(&FieldKey::Bytes(vec![4, 5])),
            FieldKey::Bytes(vec![4, 5])
        );
        assert_eq!(
            serde_json::from_value::<FieldKey>(json!([7, 8])).unwrap(),
            FieldKey::Bytes(vec![7, 8])
        );
    }

    #[test]
    fn field_value_serializes_all_variants_and_decodes_human_readable_bytes() {
        assert_eq!(
            serde_json::to_value(FieldValue::Bool(true)).unwrap(),
            json!(true)
        );
        assert_eq!(
            serde_json::to_value(FieldValue::I64(-1)).unwrap(),
            json!(-1)
        );
        assert_eq!(serde_json::to_value(FieldValue::U64(1)).unwrap(), json!(1));
        assert_eq!(
            serde_json::to_value(FieldValue::F64(1.5)).unwrap(),
            json!(1.5)
        );
        assert_eq!(
            serde_json::to_value(FieldValue::F32(1.25)).unwrap(),
            json!(1.25)
        );
        assert_eq!(
            serde_json::to_value(FieldValue::Text("not base64!".into())).unwrap(),
            json!("not base64!")
        );
        assert_eq!(
            serde_json::to_value(FieldValue::Json(json!({"a": 1}))).unwrap(),
            json!({"a": 1})
        );
        assert_eq!(
            serde_json::to_value(FieldValue::Null).unwrap(),
            serde_json::Value::Null
        );
        assert_eq!(
            serde_json::to_value(FieldValue::Vector(vec![bf16::from_f32(1.0)])).unwrap(),
            json!([bf16::from_f32(1.0).to_bits()])
        );
        assert_eq!(
            serde_json::to_value(FieldValue::Array(vec![FieldValue::U64(1)])).unwrap(),
            json!([1])
        );

        let encoded = serde_json::to_string(&FieldValue::Bytes(vec![1, 2, 3])).unwrap();
        assert_eq!(encoded, r#""AQID""#);
        assert_eq!(
            serde_json::from_str::<FieldValue>(&encoded).unwrap(),
            FieldValue::Bytes(vec![1, 2, 3])
        );
        assert_eq!(
            serde_json::from_value::<FieldValue>(json!("not base64!")).unwrap(),
            FieldValue::Text("not base64!".into())
        );

        let mut map = BTreeMap::new();
        map.insert(FieldKey::Text("not base64!".into()), FieldValue::Bool(true));
        map.insert(FieldKey::Bytes(vec![9]), FieldValue::U64(9));
        let json_value = serde_json::to_value(FieldValue::Map(map.clone())).unwrap();
        assert_eq!(json_value["not base64!"], json!(true));
        assert_eq!(json_value[BASE64_URL_SAFE.encode([9])], json!(9));
        let decoded: FieldValue = serde_json::from_value(json_value).unwrap();
        assert_eq!(decoded, FieldValue::Map(map));
    }

    #[test]
    fn field_value_deserialize_visitors_cover_numbers_arrays_maps_and_errors() {
        assert_eq!(
            serde_json::from_value::<FieldValue>(json!(true)).unwrap(),
            FieldValue::Bool(true)
        );
        assert_eq!(
            serde_json::from_value::<FieldValue>(json!(-5)).unwrap(),
            FieldValue::I64(-5)
        );
        assert_eq!(
            serde_json::from_value::<FieldValue>(json!(5)).unwrap(),
            FieldValue::U64(5)
        );
        assert_eq!(
            serde_json::from_value::<FieldValue>(json!(1.5)).unwrap(),
            FieldValue::F64(1.5)
        );
        assert_eq!(
            serde_json::from_value::<FieldValue>(serde_json::Value::Null).unwrap(),
            FieldValue::Null
        );
        assert_eq!(
            serde_json::from_value::<FieldValue>(json!([1, "not base64!"])).unwrap(),
            FieldValue::Array(vec![
                FieldValue::U64(1),
                FieldValue::Text("not base64!".into())
            ])
        );

        let value = cbor_roundtrip::<_, FieldValue>(&vec![1u8, 2u8, 3u8]);
        assert_eq!(
            value,
            FieldValue::Array(vec![
                FieldValue::U64(1),
                FieldValue::U64(2),
                FieldValue::U64(3)
            ])
        );
        assert_eq!(
            cbor_roundtrip::<_, FieldValue>(&serde_bytes::ByteBuf::from(vec![1, 2])),
            FieldValue::Bytes(vec![1, 2])
        );
        assert_eq!(
            cbor_roundtrip::<_, FieldValue>(&'x'),
            FieldValue::Text("x".into())
        );
        assert_eq!(
            cbor_roundtrip::<_, FieldValue>(&1.25f32),
            FieldValue::F64(1.25)
        );
        let f32_deserializer: serde::de::value::F32Deserializer<serde::de::value::Error> =
            1.25f32.into_deserializer();
        assert_eq!(
            FieldValue::deserialize(f32_deserializer).unwrap(),
            FieldValue::F32(1.25)
        );

        let i128_deserializer: serde::de::value::I128Deserializer<serde::de::value::Error> =
            (i128::from(i64::MAX) + 1).into_deserializer();
        let result = FieldValue::deserialize(i128_deserializer);
        let err = result.unwrap_err();
        assert!(err.to_string().contains("i128 overflow"));

        let u128_deserializer: serde::de::value::U128Deserializer<serde::de::value::Error> =
            (u128::from(u64::MAX) + 1).into_deserializer();
        let result = FieldValue::deserialize(u128_deserializer);
        let err = result.unwrap_err();
        assert!(err.to_string().contains("u128 overflow"));

        assert!(serde_json::from_str::<FieldValue>("NaN").is_err());
        assert!(serde_json::to_string(&FieldValue::F64(f64::NAN)).is_err());
        assert!(serde_json::to_string(&FieldValue::F32(f32::NAN)).is_err());
    }

    #[test]
    fn visitors_directly_cover_narrow_numeric_bytes_and_option_paths() {
        type DeError = serde::de::value::Error;

        assert_eq!(
            KeyVisitor.visit_borrowed_bytes::<DeError>(b"key").unwrap(),
            FieldKey::Bytes(b"key".to_vec())
        );
        assert_eq!(
            KeyVisitor
                .visit_byte_buf::<DeError>(b"owned-key".to_vec())
                .unwrap(),
            FieldKey::Bytes(b"owned-key".to_vec())
        );

        assert_eq!(
            Visitor.visit_i8::<DeError>(-8).unwrap(),
            FieldValue::I64(-8)
        );
        assert_eq!(
            Visitor.visit_i16::<DeError>(-16).unwrap(),
            FieldValue::I64(-16)
        );
        assert_eq!(
            Visitor.visit_i32::<DeError>(-32).unwrap(),
            FieldValue::I64(-32)
        );
        assert_eq!(Visitor.visit_u8::<DeError>(8).unwrap(), FieldValue::U64(8));
        assert_eq!(
            Visitor.visit_u16::<DeError>(16).unwrap(),
            FieldValue::U64(16)
        );
        assert_eq!(
            Visitor.visit_u32::<DeError>(32).unwrap(),
            FieldValue::U64(32)
        );
        assert_eq!(
            Visitor.visit_char::<DeError>('x').unwrap(),
            FieldValue::Text("x".to_string())
        );
        assert_eq!(
            Visitor.visit_borrowed_bytes::<DeError>(b"value").unwrap(),
            FieldValue::Bytes(b"value".to_vec())
        );
        assert_eq!(
            Visitor
                .visit_byte_buf::<DeError>(b"owned-value".to_vec())
                .unwrap(),
            FieldValue::Bytes(b"owned-value".to_vec())
        );
        assert_eq!(Visitor.visit_none::<DeError>().unwrap(), FieldValue::Null);
        assert_eq!(Visitor.visit_unit::<DeError>().unwrap(), FieldValue::Null);
        let some_deserializer: serde::de::value::U8Deserializer<DeError> = 42u8.into_deserializer();
        assert_eq!(
            Visitor.visit_some(some_deserializer).unwrap(),
            FieldValue::U64(42)
        );
        let newtype_deserializer: serde::de::value::StrDeserializer<DeError> =
            "nested".into_deserializer();
        assert_eq!(
            Visitor.visit_newtype_struct(newtype_deserializer).unwrap(),
            FieldValue::Text("nested".to_string())
        );

        assert!(Visitor.visit_i128::<DeError>(i128::MAX).is_err());
        assert!(Visitor.visit_u128::<DeError>(u128::MAX).is_err());
    }
}
