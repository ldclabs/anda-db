//! KIP time coordinates: world validity, observation, assertion and transaction
//! time remain independent. Protocol inputs are validated without rewriting
//! (§6.5); engine clocks truncate to milliseconds. Fixed-width UTC strings sort
//! chronologically, allowing indexed time ranges without a second time column.

use anda_kip::KipError;
use chrono::{DateTime, SecondsFormat, Utc};

/// A canonical UTC instant with exactly three fractional-second digits.
pub type Timestamp = String;

/// Sentinels for open-started/open-ended indexed ranges; not protocol timestamps.
pub const TIME_MIN: &str = "";
pub const TIME_MAX: &str = "~";

/// Validates a protocol input and returns it unchanged.
///
/// The historical name is retained for callers; noncanonical inputs are no
/// longer normalized. String violations are ConstraintViolation (§6.5).
pub fn normalize(value: &str, field: &str) -> Result<Timestamp, KipError> {
    anda_kip::timestamp::parse(value, field)?;
    Ok(value.to_string())
}

/// Writes an engine instant, truncating sub-millisecond precision.
pub fn format(at: DateTime<Utc>) -> Timestamp {
    at.to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub fn now() -> Timestamp {
    format(Utc::now())
}

/// Reads a canonical protocol timestamp into an instant.
pub fn parse(value: &str) -> Result<DateTime<Utc>, KipError> {
    anda_kip::timestamp::parse(value, "timestamp")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inputs_are_validated_without_normalizing() {
        let value = "2026-08-16T02:00:00.123Z";
        assert_eq!(normalize(value, "observed_at").unwrap(), value);
        for invalid in ["2026-08-16T02:00:00Z", "2026-08-16T10:00:00.000+08:00"] {
            assert_eq!(
                normalize(invalid, "observed_at").unwrap_err().name(),
                "ConstraintViolation"
            );
        }
        assert!(normalize(&now(), "clock").is_ok());
    }

    #[test]
    fn engine_clock_truncates_instead_of_rounding() {
        let at = DateTime::parse_from_rfc3339("2026-08-16T23:59:59.999999999Z").unwrap();
        assert_eq!(format(at.with_timezone(&Utc)), "2026-08-16T23:59:59.999Z");
    }

    #[test]
    fn lexicographic_order_is_chronological_order() {
        let mut stamps = [
            "2026-01-01T00:00:00.000Z",
            "2025-12-31T23:59:59.999Z",
            "2026-01-01T00:00:00.001Z",
            "2099-12-31T23:59:59.000Z",
        ];
        let mut chronological: Vec<_> = stamps.iter().map(|s| parse(s).unwrap()).collect();
        chronological.sort();
        stamps.sort();
        assert_eq!(
            stamps.to_vec(),
            chronological.into_iter().map(format).collect::<Vec<_>>()
        );
        assert!(TIME_MAX > "9999-12-31T23:59:59.999Z");
        assert!(TIME_MIN < "0000-01-01T00:00:00.000Z");
    }
}
