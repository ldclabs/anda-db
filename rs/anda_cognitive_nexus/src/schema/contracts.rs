//! Pinned JSON Schema resources. No network, file, or ambient validator cache
//! may satisfy a package's missing transitive dependency (KIP §20.5).
use super::SchemaPackage;
use anda_kip::{Json, KipError, KipErrorCode};
use jsonschema::{Retrieve, Uri, Validator};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{Arc, LazyLock, RwLock},
};

const DOCUMENTS: &[&str] = &[
    anda_kip::PROJECTION_SCHEMA,
    anda_kip::COGNITIVE_RECORDS_SCHEMA,
    anda_kip::ELEMENT_SCHEMA,
    anda_kip::CAPSULE_SCHEMA,
    anda_kip::SCHEMA_PACKAGE_SCHEMA,
    anda_kip::CHANGE_ENVELOPE_SCHEMA,
    anda_kip::MEMORY_SCHEMA,
];

static CATALOG: LazyLock<BTreeMap<String, Json>> = LazyLock::new(|| {
    DOCUMENTS
        .iter()
        .map(|s| {
            let value = anda_kip::parse_canonical_json(s).expect("vendored strict schema");
            (value["$id"].as_str().expect("schema id").to_string(), value)
        })
        .collect()
});

#[derive(Clone)]
struct Locked(BTreeMap<String, Json>);
impl Retrieve for Locked {
    fn retrieve(
        &self,
        uri: &Uri<String>,
    ) -> Result<Json, Box<dyn std::error::Error + Send + Sync>> {
        self.0
            .get(uri.as_str())
            .cloned()
            .ok_or_else(|| format!("unpinned schema resource {uri}").into())
    }
}

pub fn digest(value: &Json) -> Result<String, KipError> {
    Ok(format!(
        "sha256:{}",
        hex::encode(Sha256::digest(
            anda_kip::try_canonical_json(value)?.as_bytes()
        ))
    ))
}

pub fn verify_artifact(value: &Json) -> Result<(), KipError> {
    anda_kip::validate_json(value)?;
    let profile = value["integrity"]["digest_profile"].as_str().unwrap_or("");
    if profile.is_empty() {
        return Ok(());
    }
    if profile != "kip-jcs-safe-v1" {
        return Err(KipError::unsupported_capability(
            "previous draft artifact numeric contracts require explicit migration",
        ));
    }
    let mut covered = value.clone();
    if let Some(object) = covered.as_object_mut() {
        object.remove("integrity");
    }
    if value["integrity"]["content_digest"].as_str() != Some(digest(&covered)?.as_str()) {
        return Err(KipError::new(
            KipErrorCode::DigestMismatch,
            "Schema Package content digest does not match its canonical bytes",
        ));
    }
    Ok(())
}

fn compile(schema: &Json, resources: &BTreeMap<String, Json>) -> Result<Validator, KipError> {
    jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .should_validate_formats(true)
        .with_format("date-time", |value| {
            anda_kip::timestamp::parse(value, "value_schema").is_ok()
        })
        .with_format("timestamp", |value| {
            anda_kip::timestamp::parse(value, "value_schema").is_ok()
        })
        .with_retriever(Locked(resources.clone()))
        .build(schema)
        .map_err(|e| {
            KipError::unsupported_capability(format!("unavailable value_schema contract: {e}"))
        })
}

/// Visit every schema reference, including unused definitions and HTTPS IDs.
fn references(
    value: &Json,
    owner: &str,
    resources: &BTreeMap<String, Json>,
) -> Result<(), KipError> {
    match value {
        Json::Object(map) => {
            if let Some(id) = map.get("$id").and_then(Json::as_str)
                && id != owner
            {
                return Err(KipError::unsupported_capability(
                    "nested schema resource IDs require an explicit resource loader",
                ));
            }
            for key in ["$ref", "$dynamicRef"] {
                if let Some(reference) = map.get(key).and_then(Json::as_str) {
                    let (resource, fragment) = reference.split_once('#').unwrap_or((reference, ""));
                    let resource = if resource.is_empty() { owner } else { resource };
                    let root = resources.get(resource).ok_or_else(|| {
                        KipError::unsupported_capability(format!(
                            "unpinned schema resource {resource}"
                        ))
                    })?;
                    if !fragment.is_empty() && root.pointer(fragment).is_none() {
                        return Err(KipError::unsupported_capability(format!(
                            "unresolved schema reference {reference}"
                        )));
                    }
                }
            }
            for (key, child) in map {
                if !matches!(key.as_str(), "const" | "enum" | "default" | "examples") {
                    references(child, owner, resources)?;
                }
            }
        }
        Json::Array(items) => {
            for child in items {
                references(child, owner, resources)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn value_schemas(value: &Json, into: &mut Vec<Json>) {
    match value {
        Json::Object(map) => {
            for (key, child) in map {
                if key == "value_schema" {
                    into.push(child.clone());
                } else {
                    value_schemas(child, into);
                }
            }
        }
        Json::Array(items) => {
            for child in items {
                value_schemas(child, into);
            }
        }
        _ => {}
    }
}

static VALIDATED_PACKAGES: LazyLock<RwLock<BTreeMap<String, ()>>> =
    LazyLock::new(|| RwLock::new(BTreeMap::new()));

/// Resolve and compile a fresh, closed resource set before activation.
pub fn validate_package(package: &SchemaPackage) -> Result<(), KipError> {
    let artifact = package.artifact()?;
    let key = digest(&artifact)?;
    if VALIDATED_PACKAGES.read().unwrap().contains_key(&key) {
        return Ok(());
    }
    let mut locked: BTreeMap<String, Json> = BTreeMap::new();
    if let Some(pins) = artifact["manifest"].get("validation_schemas") {
        for pin in pins
            .as_array()
            .ok_or_else(|| KipError::type_mismatch("validation_schemas must be a list"))?
        {
            let id = pin["id"]
                .as_str()
                .ok_or_else(|| KipError::type_mismatch("schema pin needs id"))?;
            let schema = CATALOG.get(id).ok_or_else(|| {
                KipError::unsupported_capability(format!("validation schema unavailable: {id}"))
            })?;
            if pin["content_digest"].as_str() != Some(digest(schema)?.as_str()) {
                return Err(KipError::new(
                    KipErrorCode::DigestMismatch,
                    format!("validation schema digest mismatch: {id}"),
                ));
            }
            if locked.insert(id.into(), schema.clone()).is_some() {
                return Err(KipError::constraint_violation(
                    "duplicate validation schema pin",
                ));
            }
        }
    }
    for (id, schema) in &locked {
        references(schema, id, &locked)?;
        compile(schema, &locked)?;
    }
    let mut schemas = Vec::new();
    value_schemas(&artifact["definitions"], &mut schemas);
    for schema in schemas {
        references(&schema, "", &locked)?;
        compile(&schema, &locked)?;
    }
    VALIDATED_PACKAGES.write().unwrap().insert(key, ());
    Ok(())
}

static VALIDATORS: LazyLock<RwLock<BTreeMap<String, Arc<Validator>>>> =
    LazyLock::new(|| RwLock::new(BTreeMap::new()));

/// Called only for contracts whose resource closure passed activation.
pub fn validate_value(schema: &Json, value: &Json) -> Result<(), KipError> {
    let key = anda_kip::canonical_json(schema);
    let cached = VALIDATORS.read().unwrap().get(&key).cloned();
    let validator = match cached {
        Some(v) => v,
        None => {
            let validator = Arc::new(compile(schema, &CATALOG)?);
            VALIDATORS.write().unwrap().insert(key, validator.clone());
            validator
        }
    };
    validator.validate(value).map_err(|e| {
        if timestamp_type_error(&e, schema) {
            KipError::type_mismatch(format!("value_schema: {e}"))
        } else {
            KipError::constraint_violation(format!("value_schema: {e}"))
        }
    })
}

// A nullable timestamp may fail inside anyOf. Identify the actual declared
// schema node, not the input field name: arbitrary payload text is unaffected.
fn timestamp_type_error(error: &jsonschema::ValidationError<'_>, schema: &Json) -> bool {
    use jsonschema::error::ValidationErrorKind;
    if let ValidationErrorKind::AnyOf { context } = error.kind() {
        return context
            .iter()
            .flatten()
            .any(|e| timestamp_type_error(e, schema));
    }
    if !matches!(error.kind(), ValidationErrorKind::Type { .. }) {
        return false;
    }
    let location = error.absolute_keyword_location().map(|uri| uri.as_str());
    let (root, path) = match location.and_then(|s| s.split_once('#')) {
        Some((id, path)) => (CATALOG.get(id).unwrap_or(schema), path.to_string()),
        None => (schema, error.schema_path().to_string()),
    };
    path.rsplit_once('/')
        .and_then(|(parent, _)| root.pointer(parent))
        .and_then(|node| node.get("format"))
        .and_then(Json::as_str)
        .is_some_and(|format| matches!(format, "timestamp" | "date-time"))
}

/// Runtime attachment and immutable record checks run on the final transaction
/// view, so UPDATE, UNSET and Activity class changes cannot bypass creation rules.
pub fn validate_record(
    env: &super::SchemaEnvironment,
    view: &Json,
    before: Option<&Json>,
) -> Result<(), KipError> {
    validate_record_with_intent(env, view, before, super::Intent::Write)
}

pub(crate) fn validate_record_with_intent(
    env: &super::SchemaEnvironment,
    view: &Json,
    before: Option<&Json>,
    intent: super::Intent,
) -> Result<(), KipError> {
    anda_kip::validate_json(view)?;
    let terminal = |v: &Json| {
        matches!(
            v["status"].as_str(),
            Some("completed" | "failed" | "cancelled")
        )
    };
    if let Some(facets) = view["facets"].as_object() {
        for (name, value) in facets {
            if name == "kip://profiles/cognitive-memory@2.1.0/OutcomeRecord"
                && before.is_some_and(|b| {
                    b["facets"].get(name).is_some_and(|old| {
                        anda_kip::canonical_json(old) != anda_kip::canonical_json(value)
                    })
                })
            {
                return Err(KipError::new(
                    KipErrorCode::ImmutableField,
                    "an attached OutcomeRecord is immutable, including previously absent optional members",
                ));
            }
            let symbol = env.resolve_symbol(super::SymbolKind::Facet, name, intent)?;
            let def = env.facet_def(&symbol)?;
            if let Some(schema) = def.extra.get("value_schema") {
                validate_value(schema, value)?;
            }
            if before.is_some_and(|old| terminal(old) && old["facets"].get(name) != Some(value))
                && (def.extra.contains_key("attachment") || name.ends_with("/DependencyBasis"))
            {
                return Err(KipError::new(
                    KipErrorCode::ImmutableField,
                    "terminal record Facets are immutable, including previously absent records",
                ));
            }
            if let Some(attachment) = def.extra.get("attachment") {
                if let Some(classes) = attachment["activity_classes"].as_array()
                    && !classes.contains(&view["activity_class"])
                {
                    return Err(KipError::constraint_violation(format!(
                        "{name} is attached to the wrong Activity class"
                    )));
                }
                if attachment["terminal_only"] == true && !terminal(view) {
                    return Err(KipError::constraint_violation(format!(
                        "{name} requires a terminal Activity"
                    )));
                }
            }
        }
    }
    if let Some(old) = before.filter(|old| terminal(old))
        && let Some(facets) = old["facets"].as_object()
    {
        for (name, value) in facets {
            let symbol = env.resolve_symbol(super::SymbolKind::Facet, name, super::Intent::Read)?;
            let def = env.facet_def(&symbol)?;
            if (def.extra.get("attachment").is_some() || name.ends_with("/DependencyBasis"))
                && view["facets"].get(name) != Some(value)
            {
                return Err(KipError::new(
                    KipErrorCode::ImmutableField,
                    "terminal record Facets are immutable",
                ));
            }
        }
    }
    if let Some(name) = view["schema_ref"]
        .as_str()
        .filter(|r| r.starts_with("kip://profiles/cognitive-memory@2.1.0/"))
        && name.ends_with("/SkillRevision")
    {
        let mut behavior = view["attributes"].clone();
        let supplied = behavior
            .as_object_mut()
            .and_then(|o| o.remove("behavior_digest"));
        if supplied.as_ref().and_then(Json::as_str) != Some(digest(&behavior)?.as_str()) {
            return Err(KipError::new(
                KipErrorCode::DigestMismatch,
                "SkillRevision behavior_digest must cover the immutable behavior fields",
            ));
        }
    }
    Ok(())
}

/// Artifacts requiring a verified dependency contract before automatic use.
pub fn is_derived(element: &crate::store::Element) -> bool {
    match element {
        crate::store::Element::Assertion(row) => row.mode == "inferred",
        crate::store::Element::Concept(row) => {
            ["SkillRevision", "Insight", "WorkingState"]
                .iter()
                .any(|name| row.schema_ref.ends_with(&format!("/{name}")))
                || row
                    .structural
                    .keys()
                    .any(|key| key.ends_with("/derived_from"))
                || row
                    .facets
                    .keys()
                    .any(|key| key.ends_with("/DerivationState"))
        }
        _ => false,
    }
}

/// A DependencyBasis plane uses the same `facets.<local-name>` spelling as KML guards.
pub(crate) fn pinned_plane(planes: &Json, name: &str) -> Option<u64> {
    match name {
        "attributes" | "structural" | "retention" => planes[name].as_u64(),
        _ => name
            .strip_prefix("facets.")
            .filter(|s| !s.is_empty())
            .map(|s| planes["facets"][s].as_u64().unwrap_or(0)),
    }
}

/// KML handles evaluate to `{id}` endpoints. Standard record identity slots
/// serialize as strings, including forward handles in an atomic MUTATE.
/// Only declared reference slots are normalized; arbitrary JSON stays intact.
pub(crate) fn normalize_record_refs(name: &str, members: &mut anda_kip::Map<String, Json>) {
    if !name.starts_with("kip://profiles/cognitive-memory@2.1.0/") {
        return;
    }
    let local = name.rsplit('/').next().unwrap_or("");
    let paths: &[&str] = match local {
        "DependencyBasis" => &["groups.*.pins.*.id", "policy_basis.context_refs.*"],
        "DecisionRecord" => &[
            "retrieved_refs.*",
            "used_refs.*",
            "applied_revisions.*",
            "basis.context_refs.*",
        ],
        "AttemptRecord" => &["decision_ref", "applied_revisions.*", "trial_ref"],
        "OutcomeRecord" => &["attempt_ref"],
        "TrialRecord" => &[
            "revision_refs.*",
            "baseline_attempt_refs.*",
            "baseline_outcome_refs.*",
            "basis.context_refs.*",
        ],
        "EvaluationRecord" => &[
            "trial_ref",
            "revision_refs.*",
            "attempt_refs.*",
            "outcome_refs.*",
            "missing_attempt_refs.*",
            "excluded_samples.*.ref",
        ],
        "TrialState" => &["trial_ref", "revision_ref"],
        "GradingState" => &["revision_ref", "evaluation_ref"],
        "ErasurePlan" => &["source_event_refs.*", "targets.*.ref"],
        _ => &[],
    };
    fn visit(value: &mut Json, path: &[&str]) {
        if let Some((first, rest)) = path.split_first() {
            if *first == "*" {
                if let Some(items) = value.as_array_mut() {
                    for item in items {
                        visit(item, rest);
                    }
                }
            } else if let Some(child) = value.get_mut(*first) {
                visit(child, rest);
            }
        } else if let Some(map) = value.as_object()
            && map.len() == 1
            && map
                .get("id")
                .and_then(Json::as_str)
                .is_some_and(|id| id.parse::<crate::ElementId>().is_ok())
        {
            *value = map["id"].clone();
        }
    }
    let mut value = Json::Object(std::mem::take(members));
    for path in paths {
        visit(&mut value, &path.split('.').collect::<Vec<_>>());
    }
    *members = value.as_object().cloned().unwrap_or_default();
}

#[cfg(test)]
mod timestamp_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn pinned_timestamp_contracts_preserve_error_classes() {
        let schema = json!({"$ref":"urn:kip:2.0:schema:cognitive-records#/$defs/AttemptRecord/properties/started_at"});
        validate_value(&schema, &json!("2024-02-29T00:00:00.123Z")).unwrap();
        for value in [
            json!("2026-01-01T00:00:00Z"),
            json!("2026-02-30T00:00:00.000Z"),
        ] {
            assert_eq!(
                validate_value(&schema, &value).unwrap_err().name(),
                "ConstraintViolation"
            );
        }
        assert_eq!(
            validate_value(&schema, &json!(0)).unwrap_err().name(),
            "TypeMismatch"
        );

        let nullable = json!({"anyOf":[{"type":"string","format":"date-time"},{"type":"null"}]});
        validate_value(&nullable, &Json::Null).unwrap();
        assert_eq!(
            validate_value(&nullable, &json!(0)).unwrap_err().name(),
            "TypeMismatch"
        );
        assert_eq!(
            validate_value(&nullable, &json!("2026-01-01T00:00:00Z"))
                .unwrap_err()
                .name(),
            "ConstraintViolation"
        );
    }
}
