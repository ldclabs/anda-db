//! # Time coordinates
//!
//! KIP keeps four independent time axes apart (Spec §36): world validity,
//! observation time, assertion time and engine transaction time. Confusing any
//! two of them is a semantic bug, not a formatting one, so the engine never
//! defaults one from another — it only agrees on how a timestamp is written.
//!
//! Every timestamp is normalized on write to
//!
//! ```text
//! YYYY-MM-DDTHH:MM:SS.sssZ
//! ```
//!
//! which has one useful property: it is fixed-width UTC, so lexicographic
//! order *is* chronological order. A B-Tree range query over the stored text
//! answers "valid at time T" directly, with no parsing per row and no second
//! numeric column that could drift out of step with the string beside it.

use anda_kip::KipError;
use chrono::{DateTime, SecondsFormat, Utc};

/// A normalized instant: fixed-width UTC RFC 3339 with milliseconds.
pub type Timestamp = String;

/// The lower bound of the timestamp ordering, for open-started ranges.
pub const TIME_MIN: &str = "";
/// An upper bound above every normalized timestamp, for open-ended ranges.
///
/// `~` sorts above every character a normalized timestamp can contain, so an
/// absent `valid_until` compares as "still applies" without a special case in
/// every range query.
pub const TIME_MAX: &str = "~";

/// Normalizes an RFC 3339 timestamp to the canonical stored form.
///
/// The input's offset is honored and then converted to UTC: an offset is a way
/// of writing an instant, not a separate instant, and keeping it would break
/// the lexicographic ordering the storage layer relies on.
pub fn normalize(value: &str, field: &str) -> Result<Timestamp, KipError> {
    let parsed = DateTime::parse_from_rfc3339(value).map_err(|err| {
        KipError::type_mismatch(format!(
            "`{field}` must be an RFC 3339 timestamp, got {value:?}: {err}"
        ))
    })?;
    Ok(format(parsed.with_timezone(&Utc)))
}

/// Writes an instant in the canonical stored form.
pub fn format(at: DateTime<Utc>) -> Timestamp {
    at.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// The current instant, in the canonical stored form.
pub fn now() -> Timestamp {
    format(Utc::now())
}

/// Reads a stored timestamp back into an instant.
pub fn parse(value: &str) -> Result<DateTime<Utc>, KipError> {
    DateTime::parse_from_rfc3339(value)
        .map(|at| at.with_timezone(&Utc))
        .map_err(|err| KipError::type_mismatch(format!("{value:?} is not a timestamp: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_offset_is_a_spelling_not_a_different_instant() {
        let utc = normalize("2026-08-16T02:00:00Z", "observed_at").unwrap();
        let shifted = normalize("2026-08-16T10:00:00+08:00", "observed_at").unwrap();
        assert_eq!(utc, shifted);
        assert_eq!(utc, "2026-08-16T02:00:00.000Z");
    }

    #[test]
    fn lexicographic_order_is_chronological_order() {
        // This is the property every temporal range query depends on.
        let mut stamps = [
            normalize("2026-01-01T00:00:00Z", "t").unwrap(),
            normalize("2025-12-31T23:59:59.999Z", "t").unwrap(),
            normalize("2026-01-01T00:00:00.001Z", "t").unwrap(),
            normalize("2099-12-31T23:59:59Z", "t").unwrap(),
        ];
        let chronological: Vec<DateTime<Utc>> = {
            let mut parsed: Vec<_> = stamps.iter().map(|s| parse(s).unwrap()).collect();
            parsed.sort();
            parsed
        };
        stamps.sort();
        assert_eq!(
            stamps.to_vec(),
            chronological.into_iter().map(format).collect::<Vec<_>>()
        );
    }

    #[test]
    fn the_open_ended_sentinel_sorts_above_every_real_timestamp() {
        assert!(TIME_MAX > normalize("9999-12-31T23:59:59Z", "t").unwrap().as_str());
        assert!(TIME_MIN < normalize("0001-01-01T00:00:00Z", "t").unwrap().as_str());
    }

    #[test]
    fn a_malformed_timestamp_names_its_field() {
        let err = normalize("yesterday", "valid_from").unwrap_err();
        assert!(err.message.contains("valid_from"));
        assert_eq!(err.name(), "TypeMismatch");
    }

    #[test]
    fn normalization_is_idempotent() {
        let once = normalize("2026-08-16T10:00:00+08:00", "t").unwrap();
        assert_eq!(normalize(&once, "t").unwrap(), once);
        assert!(normalize(&now(), "t").is_ok());
    }
}
