//! Strict portable JSON decoding (KIP §9.3, §70.1, kip-jcs-safe-v1).
use crate::{Json, KipError, Number};
use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, SeqAccess, Visitor},
};
use std::fmt;

pub const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// Unlike serde's default `Option`, a present JSON null remains `Some(Null)`.
pub(crate) fn deserialize_present_json<'de, D: Deserializer<'de>>(
    decoder: D,
) -> Result<Option<Json>, D::Error> {
    Json::deserialize(decoder).map(Some)
}

pub(crate) fn validate_number(number: &Number) -> Result<(), KipError> {
    let valid = number.as_f64().is_some_and(|value| {
        value.is_finite() && (value.fract() != 0.0 || value.abs() <= MAX_SAFE_INTEGER as f64)
    });
    if valid {
        Ok(())
    } else {
        Err(KipError::invalid_request_envelope(
            "KIP numbers must be finite binary64 with safe integral values",
        ))
    }
}

/// Validate the source token before serde or an adapter discards its digits.
pub fn portable_number(source: &str) -> Result<Number, String> {
    serde_json::from_str::<Number>(source).map_err(|_| "invalid JSON number syntax".to_string())?;
    let value: f64 = source
        .parse()
        .map_err(|_| "invalid JSON number".to_string())?;
    if !value.is_finite() || (value.fract() == 0.0 && value.abs() > MAX_SAFE_INTEGER as f64) {
        return Err("KIP numbers must be finite binary64 with safe integral values".into());
    }
    if value == 0.0
        && source
            .split(['e', 'E'])
            .next()
            .unwrap_or("")
            .bytes()
            .any(|c| matches!(c, b'1'..=b'9'))
    {
        return Err("nonzero JSON number underflows to zero".into());
    }
    if value.fract() == 0.0 {
        Ok(Number::from(value as i64))
    } else {
        Number::from_f64(value).ok_or_else(|| "non-finite JSON number".into())
    }
}

/// Validate already-bound JSON too. Source underflow/duplicate keys must be
/// checked by `parse_canonical_json` before reaching this boundary.
pub fn validate_json(value: &Json) -> Result<(), KipError> {
    fn walk(value: &Json, depth: usize) -> Result<(), KipError> {
        if depth > 128 {
            return Err(KipError::resource_exhausted("JSON nesting exceeds 128"));
        }
        match value {
            Json::Number(n) => {
                validate_number(n)?;
            }
            Json::Array(items) => {
                for item in items {
                    walk(item, depth + 1)?;
                }
            }
            Json::Object(items) => {
                for item in items.values() {
                    walk(item, depth + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    walk(value, 0)
}

/// Strict JSON, rejecting duplicate decoded keys, invalid Unicode, BOM, and
/// lossy numeric source tokens. `&str` already guarantees valid UTF-8.
pub fn parse_canonical_json(source: &str) -> Result<Json, KipError> {
    // Scan outside strings, before the deserializer can round/underflow numbers.
    let bytes = source.as_bytes();
    let mut i = 0;
    let mut quoted = false;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' if quoted => i += 2,
            b'"' => {
                quoted = !quoted;
                i += 1;
            }
            b'-' | b'0'..=b'9' if !quoted => {
                let start = i;
                i += 1;
                while i < bytes.len()
                    && matches!(bytes[i], b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-')
                {
                    i += 1;
                }
                portable_number(&source[start..i]).map_err(KipError::invalid_request_envelope)?;
            }
            _ => i += 1,
        }
    }
    serde_json::from_str::<StrictValue>(source)
        .map(|value| value.0)
        .map_err(|error| {
            KipError::invalid_request_envelope(format!("invalid portable JSON: {error}"))
        })
}

struct StrictValue(Json);
impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct StrictVisitor;
        impl<'de> Visitor<'de> for StrictVisitor {
            type Value = StrictValue;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("portable JSON with unique keys")
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(StrictValue(Json::Null))
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
                let number = portable_number(&v.to_string()).map_err(E::custom)?;
                Ok(StrictValue(Json::Number(number)))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
                Ok(StrictValue(v.into()))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(StrictValue(value)) = seq.next_element()? {
                    values.push(value);
                }
                Ok(StrictValue(Json::Array(values)))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut values = crate::Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(de::Error::custom("duplicate decoded JSON key"));
                    }
                    values.insert(key, map.next_value::<StrictValue>()?.0);
                }
                Ok(StrictValue(Json::Object(values)))
            }
        }
        decoder.deserialize_any(StrictVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn portable_source_boundaries() {
        for source in [
            "9007199254740992",
            "9007199254740993.0",
            "9007199254740993e0",
            "-9007199254740992",
            "1e-400",
            "1e400",
            "+1",
            ".1",
            "01",
            "1.",
            "\u{feff}null",
            r#"{"a":1,"\u0061":2}"#,
            r#""\ud800""#,
        ] {
            assert!(parse_canonical_json(source).is_err(), "{source}");
        }
        for source in [
            "9007199254740991",
            "-9007199254740991",
            "0e-400",
            "-0",
            "5e-324",
            "0.000001",
        ] {
            assert!(parse_canonical_json(source).is_ok(), "{source}");
        }
    }
}
