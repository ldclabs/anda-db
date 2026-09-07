//! Deterministic decomposition of the fields KIP 1.x actually recorded.
//! The original row is also kept in LegacyRecord; defaults below describe an
//! unknown/pending item, never a new observation, lease or learning verdict.
use super::stage::LegacyRow;

/// Published v1 Properties stores attributes/metadata as a/m. Also read the
/// verbose form used by older exports and operator-built recovery fixtures.
pub(super) fn metadata(properties: &Json) -> &Json {
    properties.get("m").unwrap_or(&properties["metadata"])
}
use anda_kip::{Json, KipError, Map};
use serde_json::json;

pub(super) fn attributes(
    row: &LegacyRow,
    symbol: &str,
    env: &crate::schema::SchemaEnvironment,
) -> Result<Json, KipError> {
    let mut attrs = row.doc["attributes"]
        .as_object()
        .cloned()
        .unwrap_or_default();
    attrs
        .entry("legacy")
        .or_insert_with(|| json!({"id":row.legacy_id,"metadata":row.doc["metadata"]}));
    if !symbol.starts_with("kip://profiles/cognitive-memory@") {
        return Ok(Json::Object(attrs));
    }
    let kind = row.doc["type"].as_str().unwrap_or("");
    if ["Event", "Insight", "Commitment", "SleepTask"].contains(&kind) {
        let summary = ["summary", "description", "reason"]
            .iter()
            .find_map(|k| attrs.get(*k).and_then(Json::as_str))
            .unwrap_or_else(|| row.doc["name"].as_str().unwrap_or(""))
            .to_string();
        attrs.insert("summary".into(), json!(summary));
    }
    match kind {
        "Event" => {
            alias(&mut attrs, "start_time", "started_at");
            alias(&mut attrs, "end_time", "ended_at");
            if attrs.get("outcome_status").is_some_and(|v| {
                !["success", "partial", "failure", "aborted", "unknown"]
                    .contains(&v.as_str().unwrap_or(""))
            }) {
                attrs.remove("outcome_status");
            }
        }
        "Commitment" => {
            alias(&mut attrs, "fulfilled_at", "completed_at");
            let status = attrs
                .get("status")
                .and_then(Json::as_str)
                .unwrap_or("pending");
            let status =
                if ["pending", "fulfilled", "cancelled", "expired", "blocked"].contains(&status) {
                    status
                } else {
                    "blocked"
                };
            attrs.insert("status".into(), json!(status));
        }
        "SleepTask" => {
            let class = match attrs.get("requested_action").and_then(Json::as_str) {
                Some("consolidate_to_semantic") => "consolidate",
                Some("resolve_contradiction") => "review_conflict",
                Some("merge_duplicates" | "reclassify") => "resolve_identity",
                Some("archive") => "review_retention",
                _ => "review_derived",
            };
            let known = attrs
                .get("task_class")
                .and_then(Json::as_str)
                .is_some_and(|value| {
                    [
                        "consolidate",
                        "review_conflict",
                        "review_skill",
                        "resolve_identity",
                        "review_retention",
                        "review_derived",
                        "refresh_self_model",
                        "inspect_quarantine",
                    ]
                    .contains(&value)
                });
            if !known {
                attrs.insert("task_class".into(), json!(class));
            }
            let status = attrs
                .get("status")
                .and_then(Json::as_str)
                .unwrap_or("pending");
            // An interrupted v1 job holds no authenticated v2 lease.
            let status =
                if ["pending", "completed", "cancelled", "blocked", "failed"].contains(&status) {
                    status
                } else {
                    "blocked"
                };
            attrs.insert("status".into(), json!(status));
        }
        _ => {}
    }
    for name in [
        "started_at",
        "ended_at",
        "created_at",
        "due_at",
        "completed_at",
        "not_before",
        "review_after",
        "first_observed_at",
        "last_observed_at",
    ] {
        if let Some(value) = attrs.get(name).cloned() {
            match timestamp(&value) {
                Some(at) => {
                    attrs.insert(name.into(), json!(at));
                }
                None if value.is_null() => {}
                None => {
                    attrs.remove(name);
                }
            }
        }
    }
    for name in ["priority", "strength", "stability"] {
        if attrs
            .get(name)
            .is_some_and(|v| !v.is_null() && v.as_f64().is_none())
        {
            attrs.remove(name);
        }
    }
    // v1 allowed values the current Profile does not accept (for example a
    // textual preference strength). Keep those in LegacyRecord, not in a
    // native typed field with a fabricated numeric meaning.
    let def = env.concept_type_def(&symbol.parse()?)?;
    for (name, field) in &def.attributes.fields {
        if field.required {
            continue;
        }
        let spec = crate::schema::package::AttributeSpec {
            open: true,
            fields: std::collections::BTreeMap::from([(name.clone(), field.clone())]),
            ..Default::default()
        };
        if !crate::schema::validate_attributes(symbol, &spec, &attrs).is_valid() {
            attrs.remove(name);
        }
    }
    if !def.attributes.open {
        attrs.retain(|name, _| def.attributes.fields.contains_key(name));
    }
    crate::schema::validate_attributes(symbol, &def.attributes, &attrs).into_result()?;
    Ok(Json::Object(attrs))
}

fn alias(attrs: &mut Map<String, Json>, old: &str, new: &str) {
    if !attrs.contains_key(new)
        && let Some(value) = attrs.get(old).cloned()
    {
        attrs.insert(new.into(), value);
    }
}

pub(super) fn timestamp(value: &Json) -> Option<String> {
    value
        .as_str()
        .and_then(|v| crate::time::normalize(v, "legacy timestamp").ok())
}

pub(super) fn retention(metadata: &Json) -> Json {
    let mut retention = Map::new();
    if metadata["pinned"] == true {
        retention.insert("retention_class".into(), json!("pinned"));
    } else if let Some(class) = metadata["retention_class"]
        .as_str()
        .filter(|s| !s.is_empty())
    {
        retention.insert("retention_class".into(), json!(class));
    }
    if let Some(at) = timestamp(&metadata["expires_at"]) {
        retention.insert("expires_at".into(), json!(at));
    }
    if retention.is_empty() {
        Json::Null
    } else {
        Json::Object(retention)
    }
}

pub(super) fn mnemonic(metadata: &Json) -> Option<Json> {
    let mut state = Map::new();
    for field in ["memory_strength", "salience", "utility"] {
        if let Some(value) = metadata[field].as_f64().filter(|v| (0.0..=1.0).contains(v)) {
            state.insert(field.into(), json!(value));
        }
    }
    if state.is_empty() {
        return None;
    }
    if let Some(at) = timestamp(&metadata["last_metabolized_at"]) {
        state.insert("last_metabolized_at".into(), json!(at));
    }
    Some(Json::Object(state))
}

pub(super) fn valid_time(metadata: &Json) -> Json {
    let mut time = Map::new();
    for (old, new) in [("valid_from", "from"), ("valid_until", "until")] {
        if let Some(at) = timestamp(&metadata[old]) {
            time.insert(new.into(), json!(at));
        }
    }
    if let (Some(from), Some(until)) = (time.get("from"), time.get("until"))
        && from.as_str() >= until.as_str()
    {
        return json!({});
    }
    Json::Object(time)
}

/// Unknown lifecycle/time annotations remain auditable, but cannot silently
/// become a current positive belief. Native valid_time keeps valid windows.
pub(super) fn archive(metadata: &Json, now: &str) -> bool {
    let status = metadata["status"].as_str().unwrap_or("");
    metadata["superseded"] == true
        || !["", "active", "reviewed"].contains(&status)
        || timestamp(&metadata["expires_at"]).is_some_and(|at| at.as_str() <= now)
        || ["valid_from", "valid_until", "expires_at"]
            .iter()
            .any(|key| {
                metadata
                    .get(*key)
                    .is_some_and(|v| !v.is_null() && timestamp(v).is_none())
            })
        || {
            let from = timestamp(&metadata["valid_from"]);
            let until = timestamp(&metadata["valid_until"]);
            matches!((from,until), (Some(a),Some(b)) if a>=b)
        }
}
