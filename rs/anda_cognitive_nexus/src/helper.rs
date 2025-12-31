//! # Helper Module
//!
//! Utility functions and traits for the Anda Cognitive Nexus system.
//! Provides field extraction, sorting, predicate matching, and error conversion utilities.

use anda_db::error::DBError;
use anda_db_utils::Pipe;
use anda_kip::{
    EntityType, Json, KipError, Map, OrderByCondition, OrderDirection, PredTerm,
    validate_dot_path_var,
};
use std::borrow::Cow;

use crate::entity::{Concept, EntityID, Properties, Proposition};

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
