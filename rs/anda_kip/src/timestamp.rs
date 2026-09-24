//! Canonical protocol timestamps (KIP §6.5). Validation never rewrites input.
use crate::{Json, KipError};
use chrono::{DateTime, Utc};

pub const FORMAT: &str = "YYYY-MM-DDTHH:mm:ss.SSSZ";

/// Parses a valid calendar instant with exactly three fractional digits in UTC.
pub fn parse(value: &str, field: &str) -> Result<DateTime<Utc>, KipError> {
    let invalid = || {
        KipError::constraint_violation(format!(
            "`{field}` must be a valid UTC timestamp in {FORMAT} form, got {value:?}"
        ))
    };
    let bytes = value.as_bytes();
    if bytes.len() != 24
        || bytes.iter().enumerate().any(|(i, byte)| match i {
            4 | 7 => *byte != b'-',
            10 => *byte != b'T',
            13 | 16 => *byte != b':',
            19 => *byte != b'.',
            23 => *byte != b'Z',
            _ => !byte.is_ascii_digit(),
        })
        // Chrono accepts leap-second encodings; the portable millisecond
        // calendar shared with the JavaScript engine does not.
        || &bytes[17..19] > b"59".as_slice()
    {
        return Err(invalid());
    }
    DateTime::parse_from_rfc3339(value)
        .map(|at| at.with_timezone(&Utc))
        .map_err(|_| invalid())
}

/// Parses a JSON timestamp. Callers decide whether absence/null is permitted.
pub fn parse_value(value: &Json, field: &str) -> Result<DateTime<Utc>, KipError> {
    let text = value
        .as_str()
        .ok_or_else(|| KipError::type_mismatch(format!("`{field}` must be a timestamp string")))?;
    parse(text, field)
}

/// Checks a JSON timestamp. Callers decide whether absence/null is permitted.
pub fn validate_value(value: &Json, field: &str) -> Result<(), KipError> {
    parse_value(value, field).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn accepts_only_canonical_calendar_instants() {
        for value in [
            "0000-01-01T00:00:00.000Z",
            "2024-02-29T12:34:56.123Z",
            "9999-12-31T23:59:59.999Z",
        ] {
            assert!(parse(value, "at").is_ok(), "{value}");
        }
        for value in [
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00.1Z",
            "2026-01-01T00:00:00.12Z",
            "2026-01-01T00:00:00.1234Z",
            "2026-01-01T00:00:00.000+00:00",
            "2026-01-01t00:00:00.000z",
            "2026-01-01 00:00:00.000Z",
            "2026-02-29T00:00:00.000Z",
            "2024-02-30T00:00:00.000Z",
            "2026-01-01T24:00:00.000Z",
            "2026-01-01T00:00:60.000Z",
            "2026-01-01T00:00:00.000Z\n",
            "yesterday",
            "",
        ] {
            let error = parse(value, "observed_at").unwrap_err();
            assert_eq!(error.name(), "ConstraintViolation", "{value}");
            assert!(error.message.contains("observed_at"));
        }
        for value in [json!(0), json!(null), json!(true), json!([]), json!({})] {
            assert_eq!(
                validate_value(&value, "at").unwrap_err().name(),
                "TypeMismatch"
            );
        }
    }
}
