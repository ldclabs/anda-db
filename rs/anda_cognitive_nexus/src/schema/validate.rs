//! # Package validation
//!
//! Core validation and package validation are different layers (Spec §91–§93),
//! and this module is only the second one: attribute shape, Literal datatype,
//! Facet shape, structural cardinality. Element shape, same-Space closure and
//! `_system` protection are Core's, enforced in [`crate::store`], and a package
//! cannot weaken them (§92, §240.32).
//!
//! ## What must *not* be rejected here
//!
//! A `functional` predicate says one subject has at most one true object. It is
//! an **epistemic** statement, so two competing objects are a contested belief —
//! something the Nexus has to be able to store in order to report it (§95,
//! §240.28). Turning it into a write rejection would mean the system could
//! never record disagreement, which is most of what a memory system is for.
//!
//! Likewise `open_world: true` means an absent claim is unknown, not false.
//! Nothing here infers falsity from absence.
//!
//! ## Severity
//!
//! Only deterministic declared constraints produce errors (§98). Model hints
//! are advisory and never become hard validators (§240.34), so nothing in
//! `model_hints` is read by this module at all.

use anda_kip::{Json, KipError, KipErrorCode, Map};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::package::{AttributeSpec, FacetDef, FieldSpec, StructuralFieldDef};

/// How much a violation matters (Spec §98).
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    /// A declared constraint was broken; the write does not proceed.
    Error,
    /// Worth reporting, but not a reason to refuse.
    Warning,
    /// Informational.
    Info,
}

/// One validation finding (Spec §97).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Violation {
    /// A stable code.
    pub code: String,
    /// The symbol whose contract was broken.
    pub schema_ref: String,
    /// Where in the element the problem is, e.g. `attributes.display_name`.
    pub path: String,
    /// What is wrong.
    pub message: String,
    /// How much it matters.
    pub severity: Severity,
}

/// The result of validating one element against its declared schema.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct Validation {
    /// Findings at [`Severity::Error`].
    pub violations: Vec<Violation>,
    /// Findings below that.
    pub warnings: Vec<Violation>,
}

impl Validation {
    /// Whether nothing blocking was found.
    pub fn is_valid(&self) -> bool {
        self.violations.is_empty()
    }

    /// Records a finding, routing it by severity.
    pub fn push(&mut self, violation: Violation) {
        if violation.severity == Severity::Error {
            self.violations.push(violation);
        } else {
            self.warnings.push(violation);
        }
    }

    /// Merges another result into this one.
    pub fn extend(&mut self, other: Validation) {
        self.violations.extend(other.violations);
        self.warnings.extend(other.warnings);
    }

    /// Turns a failed validation into the error a caller sees.
    pub fn into_result(self) -> Result<Self, KipError> {
        if self.is_valid() {
            return Ok(self);
        }
        let summary = self
            .violations
            .iter()
            .map(|v| format!("{}: {}", v.path, v.message))
            .collect::<Vec<_>>()
            .join("; ");
        Err(KipError::new(KipErrorCode::ConstraintViolation, summary)
            .with_details(serde_json::to_value(&self).unwrap_or(Json::Null)))
    }
}

fn error(code: &str, schema_ref: &str, path: &str, message: String) -> Violation {
    Violation {
        code: code.to_string(),
        schema_ref: schema_ref.to_string(),
        path: path.to_string(),
        message,
        severity: Severity::Error,
    }
}

/// Whether a JSON value satisfies one declared validation type (§35).
///
/// The `type` slot is either a name or a list of accepted names, and a list is
/// a union: `["timestamp", "null"]` is how the shipped profile spells a
/// nullable field.
fn matches_type(declared: &Json, value: &Json) -> bool {
    match declared {
        Json::Null => true, // No declared type constrains nothing.
        Json::String(name) => matches_type_name(name, value),
        Json::Array(names) => names.iter().any(|name| matches_type(name, value)),
        _ => false,
    }
}

fn matches_type_name(name: &str, value: &Json) -> bool {
    match name {
        "any" => true,
        "string" => value.is_string(),
        "number" => value.is_number(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "boolean" => value.is_boolean(),
        "array" => value.is_array(),
        "object" => value.is_object(),
        "null" => value.is_null(),
        // A timestamp is carried as a string; its shape is checked where it is
        // normalized, so that one parser decides what a timestamp is.
        "timestamp" => value.is_string(),
        // An unrecognized type name is a package this engine does not fully
        // understand. Accepting the value is the conservative reading: it
        // cannot be validated, and inventing a failure would reject data on
        // the strength of a name the engine simply has not implemented.
        _ => true,
    }
}

fn type_name(declared: &Json) -> String {
    match declared {
        Json::String(name) => name.clone(),
        Json::Array(names) => names.iter().map(type_name).collect::<Vec<_>>().join(" or "),
        other => other.to_string(),
    }
}

/// Validates one declared field's value.
fn validate_field(
    schema_ref: &str,
    path: &str,
    spec: &FieldSpec,
    value: &Json,
    into: &mut Validation,
) {
    if !matches_type(&spec.r#type, value) {
        into.push(error(
            "SCHEMA_TYPE_MISMATCH",
            schema_ref,
            path,
            format!(
                "expected {}, got {}",
                type_name(&spec.r#type),
                json_kind(value)
            ),
        ));
        // A value of the wrong type cannot meaningfully be range-checked.
        return;
    }
    if let Some(number) = value.as_f64() {
        if let Some(min) = spec.minimum
            && number < min
        {
            into.push(error(
                "SCHEMA_RANGE_VIOLATION",
                schema_ref,
                path,
                format!("{number} is below the declared minimum {min}"),
            ));
        }
        if let Some(max) = spec.maximum
            && number > max
        {
            into.push(error(
                "SCHEMA_RANGE_VIOLATION",
                schema_ref,
                path,
                format!("{number} is above the declared maximum {max}"),
            ));
        }
    }
    if let Some(accepted) = &spec.r#enum
        && !accepted.contains(value)
    {
        into.push(error(
            "SCHEMA_VALUE_NOT_ALLOWED",
            schema_ref,
            path,
            format!("{value} is not one of the declared values"),
        ));
    }
}

fn json_kind(value: &Json) -> &'static str {
    match value {
        Json::Null => "null",
        Json::Bool(_) => "boolean",
        Json::Number(_) => "number",
        Json::String(_) => "string",
        Json::Array(_) => "array",
        Json::Object(_) => "object",
    }
}

/// Validates a field map against a declared field set.
fn validate_fields(
    schema_ref: &str,
    prefix: &str,
    open: bool,
    declared: &BTreeMap<String, FieldSpec>,
    values: &Map<String, Json>,
    into: &mut Validation,
) {
    for (name, spec) in declared {
        let path = format!("{prefix}.{name}");
        match values.get(name) {
            Some(value) => validate_field(schema_ref, &path, spec, value, into),
            None if spec.required => into.push(error(
                "SCHEMA_REQUIRED_MISSING",
                schema_ref,
                &path,
                "the schema declares this field required".to_string(),
            )),
            None => {}
        }
    }
    if !open {
        for name in values.keys() {
            if !declared.contains_key(name) {
                into.push(error(
                    "SCHEMA_UNKNOWN_FIELD",
                    schema_ref,
                    &format!("{prefix}.{name}"),
                    "the schema is closed and declares no such field".to_string(),
                ));
            }
        }
    }
}

/// Validates a Concept's attributes against its type (§34–§37).
pub fn validate_attributes(
    schema_ref: &str,
    spec: &AttributeSpec,
    attributes: &Map<String, Json>,
) -> Validation {
    let mut result = Validation::default();
    validate_fields(
        schema_ref,
        "attributes",
        spec.open,
        &spec.fields,
        attributes,
        &mut result,
    );
    result
}

/// Reports attributes that changed despite being declared immutable (§39).
///
/// Needs both states because immutability is a statement about a transition,
/// not about a value: the same attribute map is fine on creation and illegal as
/// an edit.
pub fn validate_attribute_mutability(
    schema_ref: &str,
    spec: &AttributeSpec,
    before: &Map<String, Json>,
    after: &Map<String, Json>,
) -> Validation {
    let mut result = Validation::default();
    for (name, field) in &spec.fields {
        if field.mutable {
            continue;
        }
        let old = before.get(name);
        // Setting an immutable attribute that was never set is establishing
        // it, not changing it; only a change to an existing value is refused.
        if old.is_some() && old != after.get(name) {
            result.push(error(
                "SCHEMA_IMMUTABLE_FIELD",
                schema_ref,
                &format!("attributes.{name}"),
                "the schema declares this attribute immutable; record a new element or a new \
                 Assertion instead of rewriting it"
                    .to_string(),
            ));
        }
    }
    result
}

/// Validates one Facet's members against its definition (§58–§60).
pub fn validate_facet(schema_ref: &str, def: &FacetDef, values: &Map<String, Json>) -> Validation {
    let mut result = Validation::default();
    validate_fields(
        schema_ref,
        "facets",
        !def.closed,
        &def.fields,
        values,
        &mut result,
    );
    result
}

/// Validates one structural field's references (§62–§66).
///
/// `targets` are the referenced elements' equality keys, in the order they were
/// written.
pub fn validate_structural(
    schema_ref: &str,
    def: &StructuralFieldDef,
    targets: &[String],
) -> Validation {
    let mut result = Validation::default();
    let count = targets.len() as u32;
    if count < def.cardinality.min {
        result.push(error(
            "SCHEMA_CARDINALITY_VIOLATION",
            schema_ref,
            "structural",
            format!(
                "the schema requires at least {} reference(s), got {count}",
                def.cardinality.min
            ),
        ));
    }
    if let Some(max) = def.cardinality.max
        && count > max
    {
        result.push(error(
            "SCHEMA_CARDINALITY_VIOLATION",
            schema_ref,
            "structural",
            format!("the schema permits at most {max} reference(s), got {count}"),
        ));
    }
    if def.unique {
        let mut seen = std::collections::BTreeSet::new();
        for target in targets {
            if !seen.insert(target) {
                result.push(error(
                    "SCHEMA_DUPLICATE_REFERENCE",
                    schema_ref,
                    "structural",
                    format!("{target} appears more than once in a field declared unique"),
                ));
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::package::SchemaPackage;
    use serde_json::json;

    const COGNITIVE_MEMORY: &str = include_str!("../../tests/fixtures/cognitive-memory-2.0.0.json");

    fn profile() -> SchemaPackage {
        SchemaPackage::parse(COGNITIVE_MEMORY).unwrap()
    }

    fn map(value: Json) -> Map<String, Json> {
        value.as_object().cloned().unwrap()
    }

    fn codes(result: &Validation) -> Vec<String> {
        result.violations.iter().map(|v| v.code.clone()).collect()
    }

    #[test]
    fn a_closed_facet_rejects_a_member_it_never_declared() {
        // Spec §60 and §240.31: a Facet is a validated namespaced extension,
        // not the untyped metadata bag KIP 1.x had.
        let package = profile();
        let def = package.facet("MnemonicState").unwrap();

        let ok = validate_facet("f", def, &map(json!({"memory_strength": 0.7})));
        assert!(ok.is_valid());

        let smuggled = validate_facet(
            "f",
            def,
            &map(json!({"memory_strength": 0.7, "classification": "public"})),
        );
        assert_eq!(codes(&smuggled), ["SCHEMA_UNKNOWN_FIELD"]);
        assert!(smuggled.violations[0].path.ends_with("classification"));
    }

    #[test]
    fn a_declared_range_is_enforced_and_a_wrong_type_is_reported_once() {
        let package = profile();
        let def = package.facet("MnemonicState").unwrap();

        let out_of_range = validate_facet("f", def, &map(json!({"salience": 1.5})));
        assert_eq!(codes(&out_of_range), ["SCHEMA_RANGE_VIOLATION"]);

        // A string where a number belongs is a type mismatch, and range
        // checking it afterwards would add a second, meaningless complaint.
        let wrong_type = validate_facet("f", def, &map(json!({"salience": "high"})));
        assert_eq!(codes(&wrong_type), ["SCHEMA_TYPE_MISMATCH"]);
        assert!(wrong_type.violations[0].message.contains("expected number"));
    }

    #[test]
    fn a_nullable_field_accepts_both_of_its_declared_types() {
        let package = profile();
        let def = package.facet("MnemonicState").unwrap();
        for value in [json!(null), json!("2026-08-16T00:00:00Z")] {
            let result = validate_facet("f", def, &map(json!({"last_metabolized_at": value})));
            assert!(result.is_valid(), "{result:?}");
        }
        let result = validate_facet("f", def, &map(json!({"last_metabolized_at": 17})));
        assert_eq!(codes(&result), ["SCHEMA_TYPE_MISMATCH"]);
    }

    #[test]
    fn an_open_type_accepts_attributes_it_never_declared() {
        // Spec §37: open attribute sets are the norm for Profile types, so an
        // unknown attribute is not automatically a violation.
        let package = profile();
        let person = package.concept_type("Person").unwrap();
        assert!(person.attributes.open);
        let result = validate_attributes(
            "t",
            &person.attributes,
            &map(json!({"display_name": "Alice", "shoe_size": 42})),
        );
        assert!(result.is_valid());
    }

    #[test]
    fn a_required_attribute_must_be_present() {
        let spec: AttributeSpec = serde_json::from_value(json!({
            "open": false,
            "fields": {"title": {"type": "string", "required": true}}
        }))
        .unwrap();
        assert_eq!(
            codes(&validate_attributes("t", &spec, &map(json!({})))),
            ["SCHEMA_REQUIRED_MISSING"]
        );
        assert!(validate_attributes("t", &spec, &map(json!({"title": "x"}))).is_valid());
    }

    #[test]
    fn an_immutable_attribute_may_be_established_but_not_changed() {
        // Spec §39. Immutability constrains a transition, so first-write and
        // rewrite are different questions about the same value.
        let spec: AttributeSpec = serde_json::from_value(json!({
            "open": true,
            "fields": {"birth_date": {"type": "string", "mutable": false}}
        }))
        .unwrap();

        let established = validate_attribute_mutability(
            "t",
            &spec,
            &map(json!({})),
            &map(json!({"birth_date": "1970-01-01"})),
        );
        assert!(established.is_valid(), "setting it the first time is fine");

        let rewritten = validate_attribute_mutability(
            "t",
            &spec,
            &map(json!({"birth_date": "1970-01-01"})),
            &map(json!({"birth_date": "1971-01-01"})),
        );
        assert_eq!(codes(&rewritten), ["SCHEMA_IMMUTABLE_FIELD"]);

        let untouched = validate_attribute_mutability(
            "t",
            &spec,
            &map(json!({"birth_date": "1970-01-01"})),
            &map(json!({"birth_date": "1970-01-01", "nickname": "A"})),
        );
        assert!(untouched.is_valid());
    }

    #[test]
    fn structural_cardinality_and_uniqueness_are_enforced() {
        let package = profile();
        let has_step = package.structural_field("has_step").unwrap();
        assert!(
            validate_structural("s", has_step, &[]).is_valid(),
            "min is 0"
        );
        assert!(validate_structural("s", has_step, &["C-1".into(), "C-2".into()]).is_valid());
        // Declared unique: the same step twice is a malformed record.
        assert_eq!(
            codes(&validate_structural(
                "s",
                has_step,
                &["C-1".into(), "C-1".into()]
            )),
            ["SCHEMA_DUPLICATE_REFERENCE"]
        );

        let bounded: StructuralFieldDef = serde_json::from_value(json!({
            "kind": "StructuralFieldDefinition",
            "cardinality": {"min": 1, "max": 2}
        }))
        .unwrap();
        assert_eq!(
            codes(&validate_structural("s", &bounded, &[])),
            ["SCHEMA_CARDINALITY_VIOLATION"]
        );
        assert_eq!(
            codes(&validate_structural(
                "s",
                &bounded,
                &["a".into(), "b".into(), "c".into()]
            )),
            ["SCHEMA_CARDINALITY_VIOLATION"]
        );
    }

    #[test]
    fn an_unimplemented_type_name_does_not_invent_a_failure() {
        // A package may declare a validation type this engine does not know.
        // Rejecting the value would fail data on the strength of a name, not
        // of anything actually checked.
        let spec: AttributeSpec = serde_json::from_value(json!({
            "open": false,
            "fields": {"colour": {"type": "kip:colour"}}
        }))
        .unwrap();
        assert!(validate_attributes("t", &spec, &map(json!({"colour": "#fff"}))).is_valid());
    }

    #[test]
    fn a_failed_validation_carries_its_findings_into_the_error() {
        let spec: AttributeSpec = serde_json::from_value(json!({
            "open": false,
            "fields": {"title": {"type": "string", "required": true}}
        }))
        .unwrap();
        let err = validate_attributes("t", &spec, &map(json!({"other": 1})))
            .into_result()
            .unwrap_err();
        assert_eq!(err.name(), "ConstraintViolation");
        let details = err.details.unwrap();
        assert_eq!(details["violations"].as_array().unwrap().len(), 2);
        assert!(err.message.contains("attributes.title"));
    }
}
