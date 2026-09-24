//! # Draft vocabulary definitions (Spec §20.16)
//!
//! What a `DEFINE` body may declare, decided on the body alone: a string
//! `description` for both kinds, the members of that kind's package
//! definition and nothing else, and no authority over the data — a draft
//! Predicate claims no closed world and no exclusive-value completeness, and
//! a draft Concept Type has only open, optional attributes of the §9.2 base
//! types.
//!
//! The same rules run twice: statically on a fully literal body, where
//! [`crate::semantics`] refuses the command before it reaches an engine, and
//! in the engine on the body after its parameters are bound — a parameter can
//! carry `open_world: false` as easily as a literal can. What depends on the
//! Schema Environment the symbol joins (a name that already resolves, an
//! endpoint type that does not) is the engine's alone.

use crate::ast::{DefineKind, Json};
use crate::error::KipError;

/// The fixed package every draft symbol belongs to (§20.16).
pub const DRAFT_PACKAGE_ID: &str = "kip://local/draft";
/// Its only version: a draft symbol never changes, so a version would carry
/// no information.
pub const DRAFT_PACKAGE_VERSION: &str = "0.0.0";
/// The exact package reference draft symbols are written under.
pub const DRAFT_PACKAGE_REF: &str = "kip://local/draft@0.0.0";

/// The members a draft Predicate may declare, with §20.15's meanings.
pub const DRAFT_PREDICATE_MEMBERS: &[&str] = &[
    "description",
    "subject",
    "object",
    "functional",
    "functional_by",
    "open_world",
    "complete",
    "boolean_completeness",
    "temporal_conflict",
];

/// The members a draft Concept Type may declare.
pub const DRAFT_TYPE_MEMBERS: &[&str] = &["description", "attributes"];

/// The §9.2 base types a draft attribute may name.
pub const DRAFT_ATTRIBUTE_TYPES: &[&str] = &["string", "number", "boolean", "null"];

/// The exact reference a draft symbol of this name is written under.
pub fn draft_symbol_ref(name: &str) -> String {
    format!("{DRAFT_PACKAGE_REF}/{name}")
}

/// Checks a `DEFINE` body, parameters already bound, against §20.16.
pub fn check_draft_definition(kind: DefineKind, definition: &Json) -> Result<(), KipError> {
    let what = match kind {
        DefineKind::Predicate => "a draft Predicate",
        DefineKind::ConceptType => "a draft Concept Type",
    };
    let reject = |message: String| {
        Err(KipError::constraint_violation(format!(
            "{message} (§20.16)"
        )))
    };
    let Some(members) = definition.as_object() else {
        return reject(format!("{what} is defined by an object"));
    };
    match members.get("description") {
        Some(Json::String(text)) if !text.is_empty() => {}
        Some(Json::String(_)) | None => {
            return reject(format!(
                "{what} declares a description: it is the only meaning a later reader of the \
                 symbol gets"
            ));
        }
        Some(_) => return reject("a draft symbol's description is a string".into()),
    }
    let allowed = match kind {
        DefineKind::Predicate => DRAFT_PREDICATE_MEMBERS,
        DefineKind::ConceptType => DRAFT_TYPE_MEMBERS,
    };
    if let Some(member) = members.keys().find(|k| !allowed.contains(&k.as_str())) {
        return reject(format!(
            "{what} declares no `{member}`; it declares {}",
            allowed.join(", ")
        ));
    }

    if kind == DefineKind::Predicate {
        for member in [
            "functional",
            "open_world",
            "complete",
            "boolean_completeness",
        ] {
            if members.get(member).is_some_and(|value| !value.is_boolean()) {
                return reject(format!("`{member}` is a boolean"));
            }
        }
        if members
            .get("temporal_conflict")
            .is_some_and(|value| !matches!(value.as_str(), Some("overlapping_valid_time" | "none")))
        {
            return reject(
                "temporal_conflict is \"overlapping_valid_time\" or \"none\" (§20.15)".into(),
            );
        }
        for side in ["subject", "object"] {
            if members.get(side).is_some_and(|value| !value.is_object()) {
                return reject(format!("`{side}` is an endpoint object"));
            }
        }
        if members.get("open_world") == Some(&Json::Bool(false)) {
            return reject(
                "a draft Predicate cannot declare open_world: false; a closed-world reading is \
                 authority only an installed package claims"
                    .into(),
            );
        }
        if members.get("complete") == Some(&Json::Bool(true)) {
            return reject(
                "a draft Predicate cannot declare complete: true; exclusive-value completeness \
                 is authority only an installed package claims"
                    .into(),
            );
        }
        if let Some(by) = members.get("functional_by") {
            if by != "object_type" {
                return reject(format!("functional_by is \"object_type\", got {by}"));
            }
            if members.get("functional") == Some(&Json::Bool(true)) {
                return reject("functional_by cannot be combined with functional: true".into());
            }
            // The partition is the object's Concept Type, so the object is
            // declared as Concepts; an omitted one is unconstrained.
            let object = members.get("object");
            let concepts_only = object.and_then(Json::as_object).is_some_and(|object| {
                !object.contains_key("literal_types")
                    && object
                        .get("kinds")
                        .and_then(Json::as_array)
                        .is_none_or(|kinds| kinds.iter().all(|kind| kind == "Concept"))
            });
            if !concepts_only {
                return reject(
                    "functional_by partitions by the object's Concept Type, so the draft \
                     Predicate declares its object as Concepts"
                        .into(),
                );
            }
        }
        return Ok(());
    }

    let Some(attributes) = members.get("attributes") else {
        return Ok(());
    };
    let Some(attributes) = attributes.as_object() else {
        return reject("draft Concept Type attributes are an object {open, fields}".into());
    };
    if let Some(member) = attributes.keys().find(|k| *k != "open" && *k != "fields") {
        return reject(format!(
            "draft Concept Type attributes declare only open and fields, not `{member}`"
        ));
    }
    if attributes.get("open").is_some_and(|open| open != true) {
        return reject("draft Concept Type attributes are open".into());
    }
    let Some(fields) = attributes.get("fields") else {
        return Ok(());
    };
    let Some(fields) = fields.as_object() else {
        return reject("draft Concept Type attribute fields are an object".into());
    };
    for (name, field) in fields {
        let Some(field) = field.as_object() else {
            return reject(format!(
                "draft Concept Type attribute `{name}` is an object {{type, description?}}"
            ));
        };
        if field.contains_key("required") {
            return reject(format!(
                "draft Concept Type attribute `{name}` declares no required member; draft \
                 attributes are always optional"
            ));
        }
        if let Some(member) = field.keys().find(|k| *k != "type" && *k != "description") {
            return reject(format!(
                "draft Concept Type attribute `{name}` declares only type and description, not \
                 `{member}`"
            ));
        }
        if field.get("description").is_some_and(|d| !d.is_string()) {
            return reject(format!(
                "draft Concept Type attribute `{name}` has a string description"
            ));
        }
        let types: Vec<&Json> = match field.get("type") {
            None => {
                return reject(format!(
                    "draft Concept Type attribute `{name}` declares its type"
                ));
            }
            Some(Json::Array(items)) if !items.is_empty() => items.iter().collect(),
            Some(single) => vec![single],
        };
        for ty in types {
            if !ty
                .as_str()
                .is_some_and(|t| DRAFT_ATTRIBUTE_TYPES.contains(&t))
            {
                return reject(format!(
                    "draft Concept Type attribute `{name}` has type {ty}; draft attributes use \
                     the base types {} (§9.2)",
                    DRAFT_ATTRIBUTE_TYPES.join(", ")
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn predicate(body: Json) -> Result<(), KipError> {
        check_draft_definition(DefineKind::Predicate, &body)
    }

    fn concept_type(body: Json) -> Result<(), KipError> {
        check_draft_definition(DefineKind::ConceptType, &body)
    }

    #[test]
    fn a_draft_symbol_needs_a_description_and_no_foreign_member() {
        assert!(predicate(json!({"description": "d"})).is_ok());
        assert!(predicate(json!({"subject": {"concept_types": ["Person"]}})).is_err());
        assert!(predicate(json!({"description": 3})).is_err());
        assert!(predicate(json!({"description": "d", "cardinality": 3})).is_err());
        assert!(predicate(json!({"description": "d", "functional": "yes"})).is_err());
        assert!(predicate(json!({"description": "d", "temporal_conflict": "sometimes"})).is_err());
        assert!(predicate(json!({"description": "d", "subject": "Person"})).is_err());
        assert!(concept_type(json!({"description": "d", "facets": {}})).is_err());
    }

    #[test]
    fn a_draft_predicate_claims_no_authority_over_the_data() {
        assert!(predicate(json!({"description": "d", "open_world": false})).is_err());
        assert!(predicate(json!({"description": "d", "open_world": true})).is_ok());
        assert!(predicate(json!({"description": "d", "complete": true})).is_err());
        assert!(predicate(json!({"description": "d", "complete": false})).is_ok());
        assert!(
            predicate(json!({"description": "d", "functional_by": "object_type"})).is_err(),
            "functional_by needs a Concept object"
        );
        assert!(
            predicate(json!({
                "description": "d", "functional_by": "object_type",
                "object": {"concept_types": ["Instrument"]}
            }))
            .is_ok()
        );
        assert!(
            predicate(json!({
                "description": "d", "functional_by": "object_type",
                "object": {"literal_types": ["string"]}
            }))
            .is_err()
        );
    }

    #[test]
    fn a_draft_concept_type_has_only_open_optional_base_attributes() {
        let ok = json!({"description": "d", "attributes": {"open": true, "fields": {
            "family": {"type": "string", "description": "strings, brass"},
            "count": {"type": ["number", "null"]},
        }}});
        assert!(concept_type(ok).is_ok());
        for bad in [
            json!({"description": "d", "attributes": {"fields": {"a": {"type": "string", "required": true}}}}),
            json!({"description": "d", "attributes": {"fields": {"a": {"type": "string", "required": false}}}}),
            json!({"description": "d", "attributes": {"fields": {"a": {"type": "timestamp"}}}}),
            json!({"description": "d", "attributes": {"fields": {"a": {"description": "x"}}}}),
            json!({"description": "d", "attributes": {"open": false}}),
            json!({"description": "d", "attributes": {"closed": true}}),
        ] {
            assert!(concept_type(bad.clone()).is_err(), "{bad}");
        }
    }
}
