//! KIP time coordinates: world validity, observation, assertion and transaction
//! time remain independent. Protocol inputs are validated without rewriting
//! (§6.5); engine clocks truncate to milliseconds. Fixed-width UTC strings sort
//! chronologically, allowing indexed time ranges without a second time column.

use anda_kip::{Json, KipError};
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

/// One `valid_time` endpoint (§25.2, §25.5): an exact instant, or a time
/// bound for an instant known only within `[earliest, latest]`.
///
/// Stored in the row's string column: an instant as itself, a bound as its
/// canonical JSON object. The two cannot collide — a Timestamp never starts
/// with `{` — and a bound keeps the precision the source actually had.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Point {
    Exact(Timestamp),
    Bound {
        earliest: Option<Timestamp>,
        latest: Option<Timestamp>,
    },
}

impl Point {
    /// Reads a written endpoint; `None` for an absent or null one.
    pub fn read(value: Option<&Json>, field: &str) -> Result<Option<Point>, KipError> {
        match value {
            None | Some(Json::Null) => Ok(None),
            Some(Json::String(text)) => Ok(Some(Point::Exact(normalize(text, field)?))),
            Some(Json::Object(bound)) => {
                if bound.is_empty() || bound.keys().any(|k| k != "earliest" && k != "latest") {
                    return Err(KipError::constraint_violation(format!(
                        "`{field}` as a time bound takes `earliest` and/or `latest` and nothing \
                         else (§25.5)"
                    )));
                }
                let side = |name: &str| -> Result<Option<Timestamp>, KipError> {
                    match bound.get(name) {
                        None | Some(Json::Null) => Ok(None),
                        Some(Json::String(text)) => {
                            Ok(Some(normalize(text, &format!("{field}.{name}"))?))
                        }
                        Some(other) => Err(KipError::type_mismatch(format!(
                            "`{field}.{name}` must be a timestamp, got {other}"
                        ))),
                    }
                };
                let (earliest, latest) = (side("earliest")?, side("latest")?);
                if earliest.is_none() && latest.is_none() {
                    return Err(KipError::constraint_violation(format!(
                        "`{field}` as a time bound needs `earliest` or `latest` (§25.5)"
                    )));
                }
                if let (Some(lo), Some(hi)) = (&earliest, &latest)
                    && lo > hi
                {
                    return Err(KipError::constraint_violation(format!(
                        "`{field}` requires earliest <= latest (§25.5)"
                    )));
                }
                Ok(Some(Point::Bound { earliest, latest }))
            }
            Some(other) => Err(KipError::type_mismatch(format!(
                "`{field}` must be a timestamp or a time bound, got {other}"
            ))),
        }
    }

    /// The storage encoding; see the type.
    pub fn store(&self) -> String {
        match self {
            Point::Exact(at) => at.clone(),
            bound => anda_kip::canonical_json(&bound.to_json()),
        }
    }

    /// Reads a stored endpoint; `None` for an empty column.
    pub fn load(text: &str) -> Option<Point> {
        if text.is_empty() {
            return None;
        }
        if !text.starts_with('{') {
            return Some(Point::Exact(text.to_string()));
        }
        let value: Json = serde_json::from_str(text).ok()?;
        Some(Point::Bound {
            earliest: value["earliest"].as_str().map(str::to_string),
            latest: value["latest"].as_str().map(str::to_string),
        })
    }

    /// The wire form: a Timestamp string or a `{earliest, latest}` object.
    pub fn to_json(&self) -> Json {
        match self {
            Point::Exact(at) => Json::String(at.clone()),
            Point::Bound { earliest, latest } => {
                let mut bound = anda_kip::Map::new();
                if let Some(at) = earliest {
                    bound.insert("earliest".into(), Json::String(at.clone()));
                }
                if let Some(at) = latest {
                    bound.insert("latest".into(), Json::String(at.clone()));
                }
                Json::Object(bound)
            }
        }
    }

    /// Whether this is an exact instant rather than a bound.
    pub fn is_exact(&self) -> bool {
        matches!(self, Point::Exact(_))
    }

    /// The closed range of possible instants; `None` is unbounded.
    pub fn range(&self) -> Range {
        match self {
            Point::Exact(at) => Range {
                lo: Some(at.clone()),
                hi: Some(at.clone()),
            },
            Point::Bound { earliest, latest } => Range {
                lo: earliest.clone(),
                hi: latest.clone(),
            },
        }
    }
}

/// A closed range `[lo, hi]` of possible instants; `None` is -∞ for `lo` and
/// +∞ for `hi`. Canonical Timestamps compare chronologically as strings.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Range {
    pub lo: Option<Timestamp>,
    pub hi: Option<Timestamp>,
}

impl Range {
    /// An instant known exactly.
    pub fn exact(at: &str) -> Self {
        Range {
            lo: Some(at.to_string()),
            hi: Some(at.to_string()),
        }
    }
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

    #[test]
    fn a_time_bound_round_trips_through_storage() {
        let written = serde_json::json!({"latest": "2026-01-01T00:00:00.000Z", "earliest": "2025-01-01T00:00:00.000Z"});
        let point = Point::read(Some(&written), "valid_time.from")
            .unwrap()
            .unwrap();
        assert!(!point.is_exact());
        let stored = point.store();
        assert_eq!(Point::load(&stored), Some(point.clone()));
        assert_eq!(point.to_json(), written);
        let exact = Point::read(Some(&Json::from("2026-01-01T00:00:00.000Z")), "x")
            .unwrap()
            .unwrap();
        assert_eq!(exact.store(), "2026-01-01T00:00:00.000Z");
        assert_eq!(Point::load(""), None);
        for bad in [
            serde_json::json!({}),
            serde_json::json!({"earliest": "2026-02-01T00:00:00.000Z", "latest": "2026-01-01T00:00:00.000Z"}),
            serde_json::json!({"around": "2026-01-01T00:00:00.000Z"}),
        ] {
            assert_eq!(
                Point::read(Some(&bad), "x").unwrap_err().name(),
                "ConstraintViolation"
            );
        }
    }
}
