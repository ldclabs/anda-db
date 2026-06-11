//! # Helper Module
//!
//! Utility functions and traits for the Anda Cognitive Nexus system.
//! Provides field extraction, sorting, predicate matching, and error conversion utilities.

use anda_db::error::DBError;
use anda_db_utils::Pipe;
use anda_kip::{
    EntityType, FilterExpression, FilterOperand, Json, KipError, METADATA_SCORE,
    METADATA_UPDATED_AT, METADATA_VERSION, Map, OrderByCondition, OrderDirection, PredTerm,
    is_reserved_metadata_key, validate_dot_path_var,
};
use rustc_hash::FxHashMap;
use std::borrow::Cow;

use crate::entity::{Concept, EntityID, Properties, Proposition};

/// Returns the engine-maintained `_version` of an **existing** element.
///
/// Elements created before version tracking have no `_version` key; they are
/// treated as version `1` (creation counts as the first mutation). Callers
/// must map "element does not exist" to `0` themselves — that is the value
/// `EXPECT VERSION 0` (create-only) guards against.
pub fn system_metadata_version(metadata: &Map<String, Json>) -> u64 {
    metadata
        .get(METADATA_VERSION)
        .and_then(|v| v.as_u64())
        .unwrap_or(1)
}

/// Marks an element as freshly created: `_version = 1`, `_updated_at = now`.
pub fn init_system_metadata(metadata: &mut Map<String, Json>, now_ms: u64) {
    metadata.insert(METADATA_VERSION.to_string(), Json::from(1u64));
    metadata.insert(
        METADATA_UPDATED_AT.to_string(),
        Json::String(unix_ms_to_iso8601(now_ms)),
    );
}

/// Marks an element as mutated: increments `_version` and refreshes
/// `_updated_at`. Must be called on every successful mutation of an existing
/// element (KIP §2.11.1).
pub fn bump_system_metadata(metadata: &mut Map<String, Json>, now_ms: u64) {
    let version = system_metadata_version(metadata).saturating_add(1);
    metadata.insert(METADATA_VERSION.to_string(), Json::from(version));
    metadata.insert(
        METADATA_UPDATED_AT.to_string(),
        Json::String(unix_ms_to_iso8601(now_ms)),
    );
}

/// Rejects author-supplied metadata keys in the reserved `_` namespace with
/// `KIP_2002` (ConstraintViolation). The reserved namespace is engine-maintained
/// and read-only to KML (KIP §2.11.1).
pub fn reject_reserved_metadata_keys<'a, I>(keys: I) -> Result<(), KipError>
where
    I: IntoIterator<Item = &'a String>,
{
    for key in keys {
        if is_reserved_metadata_key(key) {
            return Err(KipError::constraint_violation(format!(
                "Metadata key {key:?} is reserved (`_` namespace is engine-maintained and read-only to KML)"
            )));
        }
    }
    Ok(())
}

/// Normalizes a raw BM25 score into `[0, 1]` relative to the best hit of the
/// result set, rounded to 6 decimal places for stable JSON output.
pub fn normalize_search_score(score: f32, max_score: f32) -> f64 {
    let normalized = if max_score > 0.0 {
        (score / max_score) as f64
    } else {
        0.0
    };
    (normalized.clamp(0.0, 1.0) * 1e6).round() / 1e6
}

/// Attaches the transient `metadata._score` field (KIP §5.2.2) to a search
/// hit. The score lives only in the response — it is never persisted.
pub fn attach_search_score(hit: &mut Json, score: f64) {
    if let Some(metadata) = hit.get_mut("metadata").and_then(|m| m.as_object_mut()) {
        metadata.insert(METADATA_SCORE.to_string(), Json::from(score));
    } else if let Some(obj) = hit.as_object_mut() {
        obj.insert(
            "metadata".to_string(),
            Json::Object(Map::from_iter([(
                METADATA_SCORE.to_string(),
                Json::from(score),
            )])),
        );
    }
}

/// Serializes a value as compact JSON for embedding in generated KIP source.
/// The KIP grammar accepts standard JSON (quoted keys included) wherever a
/// JSON value or map is expected, so `serde_json` output is always parseable.
pub fn to_kip_json<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_string(value).expect("JSON serialization of in-memory values cannot fail")
}

/// Returns a copy of `metadata` without the reserved `_` namespace.
/// Used by `EXPORT`: engine bookkeeping is not knowledge and never leaves
/// the source engine (KIP §5.3).
pub fn strip_reserved_metadata(metadata: &Map<String, Json>) -> Map<String, Json> {
    metadata
        .iter()
        .filter(|(key, _)| !is_reserved_metadata_key(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

/// Formats a Unix timestamp in milliseconds as an ISO 8601 / RFC 3339 UTC
/// string (e.g., `2026-06-11T08:30:00.123Z`).
pub fn unix_ms_to_iso8601(ms: u64) -> String {
    let secs = ms / 1000;
    let millis = ms % 1000;
    let days = (secs / 86_400) as i64;
    let tod = secs % 86_400;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{millis:03}Z",
        tod / 3600,
        (tod % 3600) / 60,
        tod % 60,
    )
}

/// Converts days since the Unix epoch to a (year, month, day) civil date
/// (Howard Hinnant's `civil_from_days` algorithm).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Extracts field values from a concept using a dot-notation path.
///
/// # Arguments
///
/// * `concept` - The concept to extract from
/// * `path` - Dot-notation field path (e.g., ["attributes", "name"])
///
/// # Returns
///
/// The extracted JSON value or an error if the path is invalid
///
/// # Supported Paths
///
/// * `[]` - Returns the complete concept node
/// * `["id"]` - Returns the entity ID
/// * `["type"]` - Returns the concept type
/// * `["name"]` - Returns the concept name
/// * `["attributes"]` - Returns all attributes
/// * `["attributes", "key"]` - Returns specific attribute
/// * `["metadata"]` - Returns all metadata
/// * `["metadata", "key"]` - Returns specific metadata
pub fn extract_concept_field_value(concept: &Concept, path: &[String]) -> Result<Json, KipError> {
    validate_dot_path_var(path, EntityType::ConceptNode)?;

    if path.is_empty() {
        return Ok(concept.to_concept_node());
    }

    match path[0].as_str() {
        "id" => Ok(concept.entity_id().to_string().into()),
        "type" => Ok(concept.r#type.clone().into()),
        "name" => Ok(concept.name.clone().into()),
        "attributes" => {
            if path.len() == 1 {
                Ok(concept.attributes.clone().into())
            } else {
                concept
                    .attributes
                    .get(&path[1])
                    .cloned()
                    .unwrap_or(Json::Null)
                    .pipe(Ok)
            }
        }
        "metadata" => {
            if path.len() == 1 {
                Ok(concept.metadata.clone().into())
            } else {
                concept
                    .metadata
                    .get(&path[1])
                    .cloned()
                    .unwrap_or(Json::Null)
                    .pipe(Ok)
            }
        }
        _ => Err(KipError::invalid_syntax(format!(
            "Invalid field path: {}",
            path.join(".")
        ))),
    }
}

/// Extracts field values from a proposition using a dot-notation path.
///
/// # Arguments
///
/// * `proposition` - The proposition to extract from
/// * `predicate` - The specific predicate to use
/// * `path` - Dot-notation field path
///
/// # Returns
///
/// The extracted JSON value or an error if the path/predicate is invalid
///
/// # Supported Paths
///
/// * `[]` - Returns the complete proposition link
/// * `["id"]` - Returns the proposition entity ID
/// * `["subject"]` - Returns the subject entity ID
/// * `["object"]` - Returns the object entity ID
/// * `["predicate"]` - Returns the predicate name
/// * `["attributes"]` - Returns predicate-specific attributes
/// * `["attributes", "key"]` - Returns specific attribute
/// * `["metadata"]` - Returns predicate-specific metadata
/// * `["metadata", "key"]` - Returns specific metadata
pub fn extract_proposition_field_value(
    proposition: &Proposition,
    predicate: &str,
    path: &[String],
) -> Result<Json, KipError> {
    validate_dot_path_var(path, EntityType::PropositionLink)?;

    if !proposition.predicates.contains(predicate) {
        return Err(KipError::internal_error(format!(
            "Invalid predicate: {}",
            predicate
        )));
    }

    if path.is_empty() {
        return proposition
            .to_proposition_link(predicate)
            .ok_or_else(|| KipError::invalid_syntax(format!("Invalid predicate: {}", predicate)));
    }

    let prop = proposition
        .properties
        .get(predicate)
        .map(Cow::Borrowed)
        .unwrap_or_else(|| {
            Cow::Owned(Properties {
                attributes: Map::new(),
                metadata: Map::new(),
            })
        });

    match path[0].as_str() {
        "id" => Ok(proposition
            .entity_id(predicate.to_string())
            .to_string()
            .into()),
        "subject" => Ok(proposition.subject.to_string().into()),
        "object" => Ok(proposition.object.to_string().into()),
        "predicate" => Ok(predicate.into()),
        "attributes" => {
            if path.len() == 1 {
                Ok(prop.attributes.clone().into())
            } else {
                prop.attributes
                    .get(&path[1])
                    .cloned()
                    .unwrap_or(Json::Null)
                    .pipe(Ok)
            }
        }
        "metadata" => {
            if path.len() == 1 {
                Ok(prop.metadata.clone().into())
            } else {
                prop.metadata
                    .get(&path[1])
                    .cloned()
                    .unwrap_or(Json::Null)
                    .pipe(Ok)
            }
        }
        _ => Err(KipError::invalid_syntax(format!(
            "Invalid field path: {}",
            path.join(".")
        ))),
    }
}

/// Applies sorting to entity-value pairs based on order conditions.
///
/// # Arguments
///
/// * `values` - Vector of (EntityID, JSON) pairs to sort
/// * `var` - Variable name to match against order conditions
/// * `order_by` - Sorting conditions to apply
///
/// # Returns
///
/// Sorted vector of entity-value pairs
///
/// # Supported Types
///
/// * Numbers - Sorted numerically
/// * Strings - Sorted lexicographically
/// * Booleans - false < true
pub fn apply_order_by<'a>(
    mut values: Vec<(&'a EntityID, Json)>,
    var: &str,
    order_by: &[OrderByCondition],
) -> Vec<(&'a EntityID, Json)> {
    values.sort_by(|(_, a), (_, b)| {
        for cond in order_by {
            // Skip aggregation ORDER BY — handled in grouped execution path
            if cond.is_aggregation() {
                continue;
            }
            if cond.variable.var != var {
                continue; // Only process conditions for the current variable
            }

            let path = format!("/{}", cond.variable.path.join("/"));

            let a_val = a.pointer(&path);
            let b_val = b.pointer(&path);

            let ordering = match (a_val, b_val) {
                (Some(Json::Number(a)), Some(Json::Number(b))) => a
                    .as_f64()
                    .unwrap_or(0.0)
                    .partial_cmp(&b.as_f64().unwrap_or(0.0)),
                (Some(Json::String(a)), Some(Json::String(b))) => Some(a.cmp(b)),
                (Some(Json::Bool(a)), Some(Json::Bool(b))) => Some(a.cmp(b)),
                _ => None,
            };

            if let Some(ord) = ordering {
                let result = match cond.direction {
                    OrderDirection::Asc => ord,
                    OrderDirection::Desc => ord.reverse(),
                };

                if result != std::cmp::Ordering::Equal {
                    return result;
                }
            }
        }
        std::cmp::Ordering::Equal
    });

    values
}

/// Matches a predicate term against a proposition.
///
/// # Arguments
///
/// * `proposition` - The proposition to match against
/// * `predicate` - The predicate term to match
///
/// # Returns
///
/// Optional tuple of (subject, matched_predicates, object) or None if no match
///
/// # Predicate Types
///
/// * `Literal` - Exact predicate name match
/// * `Variable` - Matches all predicates in the proposition
/// * `Alternative` - Matches any predicate from a set of alternatives
pub fn match_predicate_against_proposition(
    proposition: &Proposition,
    predicate: &PredTerm,
) -> Result<Option<(EntityID, Vec<String>, EntityID)>, KipError> {
    match predicate {
        PredTerm::Literal(pred) => {
            if proposition.predicates.contains(pred) {
                Ok(Some((
                    proposition.subject.clone(),
                    vec![pred.clone()],
                    proposition.object.clone(),
                )))
            } else {
                Ok(None)
            }
        }
        PredTerm::Variable(_) => Ok(Some((
            proposition.subject.clone(),
            proposition.predicates.iter().cloned().collect(),
            proposition.object.clone(),
        ))),
        PredTerm::Alternative(preds) => {
            let matched_preds = proposition
                .predicates
                .iter()
                .filter(|p| preds.contains(p))
                .cloned()
                .collect::<Vec<_>>();
            if !matched_preds.is_empty() {
                Ok(Some((
                    proposition.subject.clone(),
                    matched_preds,
                    proposition.object.clone(),
                )))
            } else {
                Ok(None)
            }
        }
        _ => Err(KipError::invalid_syntax(format!(
            "Predicate must be either Literal or Variable, got: {predicate:?}"
        ))),
    }
}

/// Converts database errors to KIP errors.
///
/// # Arguments
///
/// * `err` - The database error to convert
///
/// # Returns
///
/// Corresponding KIP error type
///
/// # Error Mappings
///
/// * `Schema` → `Parse`
/// * `NotFound` → `NotFound`
/// * `AlreadyExists` → `AlreadyExists`
/// * Others → `Execution`
pub fn db_to_kip_error(err: DBError) -> KipError {
    match &err {
        DBError::Schema { .. } => KipError::invalid_syntax(format!("{err}")),
        DBError::NotFound { .. } => KipError::not_found(format!("{err}")),
        DBError::AlreadyExists { .. } => KipError::duplicate_exists(format!("{err}")),
        _ => KipError::internal_error(format!("{err}")),
    }
}

/// Extension trait for `FilterExpression` to check for unbound variables.
pub trait FilterExpressionExt {
    /// Returns `true` if the expression references any variable NOT already in `bound`.
    fn has_unbound_variables(&self, bound: &FxHashMap<String, EntityID>) -> bool;
}

impl FilterExpressionExt for FilterExpression {
    fn has_unbound_variables(&self, bound: &FxHashMap<String, EntityID>) -> bool {
        match self {
            FilterExpression::Comparison { left, right, .. } => {
                operand_has_unbound(left, bound) || operand_has_unbound(right, bound)
            }
            FilterExpression::Logical { left, right, .. } => {
                left.has_unbound_variables(bound) || right.has_unbound_variables(bound)
            }
            FilterExpression::Not(inner) => inner.has_unbound_variables(bound),
            FilterExpression::Function { args, .. } => {
                args.iter().any(|a| operand_has_unbound(a, bound))
            }
        }
    }
}

fn operand_has_unbound(op: &FilterOperand, bound: &FxHashMap<String, EntityID>) -> bool {
    match op {
        FilterOperand::Variable(dot_path) => !bound.contains_key(&dot_path.var),
        FilterOperand::Literal(_) | FilterOperand::List(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anda_kip::{
        AggregationFunction, ComparisonOperator, DotPathVar, FilterFunction, LogicalOperator,
        OrderByCondition,
    };
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};

    fn concept(id: u64, r#type: &str, name: &str) -> Concept {
        Concept {
            _id: id,
            r#type: r#type.to_string(),
            name: name.to_string(),
            attributes: Map::from_iter([
                ("score".to_string(), json!(id)),
                ("active".to_string(), json!(id % 2 == 1)),
            ]),
            metadata: Map::from_iter([("source".to_string(), json!("unit"))]),
        }
    }

    fn proposition() -> Proposition {
        Proposition {
            _id: 9,
            subject: EntityID::Concept(1),
            object: EntityID::Concept(2),
            predicates: BTreeSet::from(["likes".to_string(), "knows".to_string()]),
            properties: BTreeMap::from([(
                "likes".to_string(),
                Properties {
                    attributes: Map::from_iter([("since".to_string(), json!(2024))]),
                    metadata: Map::from_iter([("source".to_string(), json!("unit"))]),
                },
            )]),
        }
    }

    #[test]
    fn extract_concept_field_value_covers_paths_and_errors() {
        let concept = concept(1, "Person", "Ada");

        assert_eq!(
            extract_concept_field_value(&concept, &[]).unwrap()["id"],
            "C:1"
        );
        assert_eq!(
            extract_concept_field_value(&concept, &["id".to_string()]).unwrap(),
            json!("C:1")
        );
        assert_eq!(
            extract_concept_field_value(&concept, &["type".to_string()]).unwrap(),
            json!("Person")
        );
        assert_eq!(
            extract_concept_field_value(&concept, &["name".to_string()]).unwrap(),
            json!("Ada")
        );
        assert_eq!(
            extract_concept_field_value(&concept, &["attributes".to_string()]).unwrap()["score"],
            json!(1)
        );
        assert_eq!(
            extract_concept_field_value(
                &concept,
                &["attributes".to_string(), "missing".to_string()],
            )
            .unwrap(),
            Json::Null
        );
        assert_eq!(
            extract_concept_field_value(&concept, &["metadata".to_string()]).unwrap()["source"],
            json!("unit")
        );
        assert_eq!(
            extract_concept_field_value(
                &concept,
                &["metadata".to_string(), "missing".to_string()],
            )
            .unwrap(),
            Json::Null
        );
        assert!(extract_concept_field_value(&concept, &["unknown".to_string()]).is_err());
    }

    #[test]
    fn extract_proposition_field_value_covers_paths_defaults_and_errors() {
        let proposition = proposition();

        assert_eq!(
            extract_proposition_field_value(&proposition, "likes", &[]).unwrap()["id"],
            "P:9:likes"
        );
        assert_eq!(
            extract_proposition_field_value(&proposition, "likes", &["id".to_string()]).unwrap(),
            json!("P:9:likes")
        );
        assert_eq!(
            extract_proposition_field_value(&proposition, "likes", &["subject".to_string()])
                .unwrap(),
            json!("C:1")
        );
        assert_eq!(
            extract_proposition_field_value(&proposition, "likes", &["object".to_string()])
                .unwrap(),
            json!("C:2")
        );
        assert_eq!(
            extract_proposition_field_value(&proposition, "likes", &["predicate".to_string()])
                .unwrap(),
            json!("likes")
        );
        assert_eq!(
            extract_proposition_field_value(&proposition, "likes", &["attributes".to_string()])
                .unwrap()["since"],
            json!(2024)
        );
        assert_eq!(
            extract_proposition_field_value(
                &proposition,
                "likes",
                &["attributes".to_string(), "missing".to_string()],
            )
            .unwrap(),
            Json::Null
        );
        assert_eq!(
            extract_proposition_field_value(&proposition, "likes", &["metadata".to_string()])
                .unwrap()["source"],
            json!("unit")
        );
        assert_eq!(
            extract_proposition_field_value(&proposition, "knows", &["attributes".to_string()])
                .unwrap(),
            json!({})
        );
        assert!(
            extract_proposition_field_value(&proposition, "missing", &["id".to_string()]).is_err()
        );
        assert!(
            extract_proposition_field_value(&proposition, "likes", &["unknown".to_string()])
                .is_err()
        );
    }

    #[test]
    fn apply_order_by_handles_numbers_strings_bools_skip_and_descending() {
        let ids = [
            EntityID::Concept(1),
            EntityID::Concept(2),
            EntityID::Concept(3),
        ];
        let values = vec![
            (&ids[0], json!({"name":"Charlie","score":2,"active":true})),
            (&ids[1], json!({"name":"Ada","score":2,"active":false})),
            (&ids[2], json!({"name":"Bob","score":1,"active":true})),
        ];
        let conditions = vec![
            OrderByCondition {
                variable: DotPathVar {
                    var: "other".to_string(),
                    path: vec!["score".to_string()],
                },
                direction: OrderDirection::Asc,
                aggregation: None,
            },
            OrderByCondition {
                variable: DotPathVar {
                    var: "x".to_string(),
                    path: vec!["score".to_string()],
                },
                direction: OrderDirection::Asc,
                aggregation: None,
            },
            OrderByCondition {
                variable: DotPathVar {
                    var: "x".to_string(),
                    path: vec!["active".to_string()],
                },
                direction: OrderDirection::Desc,
                aggregation: None,
            },
            OrderByCondition {
                variable: DotPathVar {
                    var: "x".to_string(),
                    path: vec!["name".to_string()],
                },
                direction: OrderDirection::Asc,
                aggregation: None,
            },
            OrderByCondition {
                variable: DotPathVar {
                    var: "x".to_string(),
                    path: vec!["ignored".to_string()],
                },
                direction: OrderDirection::Desc,
                aggregation: Some(AggregationFunction::Count),
            },
        ];

        let sorted = apply_order_by(values, "x", &conditions);
        assert_eq!(
            sorted
                .into_iter()
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>(),
            vec![
                EntityID::Concept(3),
                EntityID::Concept(1),
                EntityID::Concept(2)
            ]
        );
    }

    #[test]
    fn match_predicate_against_proposition_covers_all_terms() {
        let proposition = proposition();

        let literal = match_predicate_against_proposition(
            &proposition,
            &PredTerm::Literal("likes".to_string()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(literal.1, vec!["likes".to_string()]);

        assert!(
            match_predicate_against_proposition(
                &proposition,
                &PredTerm::Literal("missing".to_string()),
            )
            .unwrap()
            .is_none()
        );

        let variable =
            match_predicate_against_proposition(&proposition, &PredTerm::Variable("p".to_string()))
                .unwrap()
                .unwrap();
        assert_eq!(variable.1.len(), 2);

        let alternative = match_predicate_against_proposition(
            &proposition,
            &PredTerm::Alternative(vec!["missing".to_string(), "knows".to_string()]),
        )
        .unwrap()
        .unwrap();
        assert_eq!(alternative.1, vec!["knows".to_string()]);
        assert!(
            match_predicate_against_proposition(
                &proposition,
                &PredTerm::Alternative(vec!["missing".to_string()]),
            )
            .unwrap()
            .is_none()
        );
        assert!(
            match_predicate_against_proposition(
                &proposition,
                &PredTerm::MultiHop {
                    predicate: "likes".to_string(),
                    min: 1,
                    max: Some(2),
                },
            )
            .is_err()
        );
    }

    #[test]
    fn db_to_kip_error_maps_error_categories() {
        let source = || std::io::Error::other("boom").into();

        assert_eq!(
            db_to_kip_error(DBError::Schema {
                name: "schema".to_string(),
                source: source(),
            })
            .code_str(),
            "KIP_1001"
        );
        assert_eq!(
            db_to_kip_error(DBError::NotFound {
                name: "concept".to_string(),
                path: "x".to_string(),
                source: source(),
                _id: 1,
            })
            .code_str(),
            "KIP_3002"
        );
        assert_eq!(
            db_to_kip_error(DBError::AlreadyExists {
                name: "concept".to_string(),
                path: "x".to_string(),
                source: source(),
                _id: 1,
            })
            .code_str(),
            "KIP_3003"
        );
        assert_eq!(
            db_to_kip_error(DBError::Generic {
                name: "db".to_string(),
                source: source(),
            })
            .code_str(),
            "KIP_4003"
        );
    }

    #[test]
    fn system_metadata_helpers_track_versions_and_reject_reserved_keys() {
        let mut metadata = Map::new();
        // Existing element without `_version` is treated as version 1.
        assert_eq!(system_metadata_version(&metadata), 1);

        init_system_metadata(&mut metadata, 1_750_000_000_123);
        assert_eq!(system_metadata_version(&metadata), 1);
        assert!(metadata["_updated_at"].as_str().unwrap().ends_with(".123Z"));

        bump_system_metadata(&mut metadata, 1_750_000_001_000);
        assert_eq!(system_metadata_version(&metadata), 2);

        assert!(reject_reserved_metadata_keys(std::iter::empty::<&String>()).is_ok());
        assert!(
            reject_reserved_metadata_keys(
                Map::from_iter([("confidence".to_string(), json!(0.9))]).keys()
            )
            .is_ok()
        );
        let err = reject_reserved_metadata_keys(
            Map::from_iter([("_version".to_string(), json!(2))]).keys(),
        )
        .unwrap_err();
        assert_eq!(err.code_str(), "KIP_2002");
    }

    #[test]
    fn unix_ms_to_iso8601_formats_utc_dates() {
        assert_eq!(unix_ms_to_iso8601(0), "1970-01-01T00:00:00.000Z");
        // 2026-06-11T00:00:00Z
        assert_eq!(
            unix_ms_to_iso8601(1_781_136_000_000),
            "2026-06-11T00:00:00.000Z"
        );
        // Leap-year day: 2024-02-29T12:34:56.789Z
        assert_eq!(
            unix_ms_to_iso8601(1_709_210_096_789),
            "2024-02-29T12:34:56.789Z"
        );
    }

    #[test]
    fn filter_expression_ext_detects_unbound_variables_recursively() {
        let bound = FxHashMap::from_iter([("x".to_string(), EntityID::Concept(1))]);
        let x = FilterOperand::Variable(DotPathVar {
            var: "x".to_string(),
            path: vec![],
        });
        let y = FilterOperand::Variable(DotPathVar {
            var: "y".to_string(),
            path: vec!["name".to_string()],
        });

        let comparison = FilterExpression::Comparison {
            left: x.clone(),
            operator: ComparisonOperator::Equal,
            right: FilterOperand::Literal("Ada".into()),
        };
        assert!(!comparison.has_unbound_variables(&bound));

        let logical = FilterExpression::Logical {
            left: Box::new(comparison),
            operator: LogicalOperator::And,
            right: Box::new(FilterExpression::Function {
                func: FilterFunction::Contains,
                args: vec![y.clone(), FilterOperand::List(vec!["Ada".into()])],
            }),
        };
        assert!(logical.has_unbound_variables(&bound));
        assert!(FilterExpression::Not(Box::new(logical)).has_unbound_variables(&bound));
    }
}
