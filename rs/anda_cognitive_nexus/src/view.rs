//! # The raw Core view
//!
//! Storage rows rendered as the wire shape (Spec §53.1). This is what a KQL
//! dot path reads — `?c.name`, `?a.confidence`, `?x.facets["MnemonicState"]` —
//! so the field names here are part of the query language's surface, not an
//! internal detail.
//!
//! Each row is converted into the corresponding [`anda_kip`] type and then
//! serialized, rather than hand-built as JSON. That is deliberate: the protocol
//! crate owns the wire shape, so a field this engine renamed or dropped becomes
//! a compile error instead of a query that silently returns null.
//!
//! Two deliberate departures, both additive:
//!
//! - `structural` — Profile structural fields have no slot in the Core types
//!   because Core keeps its own topology in typed fields (§103 Q1). The engine
//!   stores them, so the view exposes them under one key rather than
//!   scattering them into `attributes`, where they would be mistaken for
//!   representation-local state.
//! - `merged_into` on a Concept — the forwarding pointer left by a
//!   non-destructive merge (§11.1). A reader that cannot see it cannot tell a
//!   merged-away Concept from a live one.

use anda_kip::{
    Activity, Assertion, AssertionLifecycle, AssertionMode, AssertionStatus, Concept,
    ElementEnvelope, Evidence, EvidenceLifecycle, EvidencePayload, EvidenceRef, GovernanceState,
    Json, Map, Origin, Proposition, Retention, Stance, SystemState, ValidTime,
};

use crate::store::Element;
use crate::store::rows::*;
use crate::store::write::Row;

/// Renders an element in the raw Core view.
pub fn render(element: &Element) -> Json {
    match element {
        Element::Concept(row) => concept(row),
        Element::Proposition(row) => proposition(row),
        Element::Assertion(row) => assertion(row),
        Element::Evidence(row) => evidence(row),
        Element::Activity(row) => activity(row),
    }
}

/// Reads one dot path out of a rendered view, resolving a Facet's local name.
///
/// A Facet is stored under its exact symbol —
/// `kip://profiles/cognitive-memory@2.1.0/MnemonicState` — because a persisted
/// reference must name one version forever (§21). A command writes the local
/// name the environment resolves: `?m.facets["MnemonicState"].salience`. That
/// resolution belongs here, on the read, rather than in a second copy of the
/// Facet map under a name that would go stale the moment the Space activates a
/// different package version.
///
/// A name the environment cannot resolve is left alone, so it reads as `null`
/// like any other missing member instead of failing a whole query.
pub fn read_path_in(
    env: &crate::schema::SchemaEnvironment,
    view: &Json,
    path: &[anda_kip::PathStep],
) -> Json {
    read_path(view, &resolve_facet_path(env, path))
}

/// Rewrites `facets["<local name>"]` to `facets["<exact symbol>"]`.
fn resolve_facet_path<'a>(
    env: &crate::schema::SchemaEnvironment,
    path: &'a [anda_kip::PathStep],
) -> std::borrow::Cow<'a, [anda_kip::PathStep]> {
    use anda_kip::PathStep;
    if path.len() < 2 {
        return std::borrow::Cow::Borrowed(path);
    }
    let (PathStep::Field(head) | PathStep::Key(head)) = &path[0];
    if head != "facets" {
        return std::borrow::Cow::Borrowed(path);
    }
    let (PathStep::Field(name) | PathStep::Key(name)) = &path[1];
    // An exact symbol is already what the row is keyed by.
    if name.starts_with("kip://") {
        return std::borrow::Cow::Borrowed(path);
    }
    let Ok(symbol) = env.resolve_symbol(
        crate::schema::SymbolKind::Facet,
        name,
        crate::schema::Intent::Read,
    ) else {
        return std::borrow::Cow::Borrowed(path);
    };
    let mut resolved = path.to_vec();
    resolved[1] = PathStep::Key(symbol.to_string());
    std::borrow::Cow::Owned(resolved)
}

/// Reads one dot path out of a rendered view.
///
/// A missing member reads as `null` rather than failing. KIP is open-world:
/// "this element has no `birth_date`" is an answer, and turning it into an
/// error would make an `OPTIONAL` block or an `IS_NULL` filter impossible to
/// write (§24 of the Epistemic Model).
pub fn read_path(view: &Json, path: &[anda_kip::PathStep]) -> Json {
    let mut cursor = view;
    for step in path {
        let key = match step {
            anda_kip::PathStep::Field(name) => name.as_str(),
            anda_kip::PathStep::Key(key) => key.as_str(),
        };
        cursor = match cursor.get(key) {
            Some(value) => value,
            None => return Json::Null,
        };
    }
    cursor.clone()
}

/// The Governance block a reader sees (§31.3).
///
/// `authority_class` is filled in rather than left absent: `descriptive` is
/// the class every element has until Governance raises it (§31.4), so
/// answering `null` would make each client reimplement that default — and a
/// client that read the hole as "no ceiling" would have it exactly backwards.
fn governance_state(governance: &Json) -> GovernanceState {
    let mut state =
        serde_json::from_value::<GovernanceState>(governance.clone()).unwrap_or_default();
    if state.authority_class.is_none() {
        state.authority_class = Some(crate::governance::authority::DEFAULT.to_string());
    }
    state
}

fn envelope(row: &impl Row) -> ElementEnvelope {
    let crate::store::write::Envelope {
        kind,
        id,
        space,
        state,
        version,
        plane_versions,
        seq,
        created_at,
        updated_at,
        created_tx,
        updated_tx,
        origin,
        governance,
        retention,
        facets,
        // Named rather than swallowed by `..`: a column added to
        // `envelope_columns!` must fail to compile here, so that whether it
        // belongs on the wire is decided rather than defaulted to "no".
        // `expires_at` is a derived index column and `structural` is attached
        // by `finish` as an additive key (§103 Q1), so neither is an envelope
        // member.
        expires_at: _,
        structural: _,
    } = row.envelope();
    let id = crate::id::ElementId::new(kind, id).to_string();
    let (version, seq) = (*version, *seq);
    ElementEnvelope {
        id,
        kind,
        space_id: some_text(space),
        governance: Some(governance_state(governance)),
        retention: serde_json::from_value::<Retention>(retention.clone()).ok(),
        facets: facets
            .iter()
            .filter_map(|(name, value)| {
                value
                    .as_object()
                    .map(|members| (name.clone(), members.clone()))
            })
            .collect(),
        system: Some(SystemState {
            input_references: origin["_kip_runtime"]["input_references"]
                .as_array()
                .cloned()
                .unwrap_or_default(),
            input_versions: origin["_kip_runtime"]["input_versions"]
                .as_object()
                .cloned(),
            output_versions: origin["_kip_runtime"]["output_versions"]
                .as_object()
                .cloned(),
            dependency_validity: None,
            version: Some(version),
            // Facet counters keyed by local name, so
            // `?x._system.plane_versions.facets["MnemonicState"]` reads the
            // name a command writes (§6.3, §35.1).
            plane_versions: Some(crate::store::planes::to_wire(
                &crate::store::planes::decode(plane_versions),
            )),
            created_at: some_text(created_at),
            updated_at: some_text(updated_at),
            created_tx: some_text(created_tx),
            updated_tx: some_text(updated_tx),
            state: some_text(state),
            space_seq: Some(seq),
            origin: serde_json::from_value::<Origin>(origin.clone()).ok(),
        }),
    }
}

fn some_text(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

/// Serializes a Core type and attaches the engine's additive keys.
fn finish<T: serde::Serialize>(
    value: &T,
    structural: &Map<String, Json>,
    extra: &[(&str, Json)],
) -> Json {
    let mut json = serde_json::to_value(value).unwrap_or(Json::Null);
    if let Some(object) = json.as_object_mut() {
        if !structural.is_empty() {
            object.insert("structural".into(), Json::Object(structural.clone()));
        }
        for (key, value) in extra {
            if !value.is_null() {
                object.insert((*key).to_string(), value.clone());
            }
        }
    }
    json
}

fn concept(row: &ConceptRow) -> Json {
    let value = Concept {
        envelope: envelope(row),
        schema_ref: some_text(&row.schema_ref),
        key: some_text(&row.key),
        name: some_text(&row.name),
        canonical_id: some_text(&row.canonical_id),
        aliases: row.aliases.clone(),
        attributes: row.attributes.clone(),
    };
    finish(
        &value,
        &row.structural,
        &[(
            "merged_into",
            some_text(&row.merged_into)
                .map(Json::String)
                .unwrap_or(Json::Null),
        )],
    )
}

fn proposition(row: &PropositionRow) -> Json {
    let value = Proposition {
        envelope: envelope(row),
        subject: endpoint_view(&row.subject),
        predicate_ref: row.predicate_ref.clone(),
        object: endpoint_view(&row.object),
    };
    finish(&value, &row.structural, &[])
}

/// A stored tuple endpoint as a read returns it.
///
/// A reference is returned as the reference object it is; a Literal as the
/// bare scalar — §9.2's baseline has exactly four datatypes and no tag, so the
/// `{value, datatype}` object the row keeps to stay self-describing adds
/// nothing a reader needs (KIP2-CORE-024), and `?p.object == "dark"` compares
/// the value rather than a wrapper.
pub fn endpoint_view(value: &Json) -> Json {
    match crate::term::Endpoint::from_json(value) {
        Ok(crate::term::Endpoint::Literal(literal)) => literal.value,
        _ => value.clone(),
    }
}

fn assertion(row: &AssertionRow) -> Json {
    let valid_time = ValidTime {
        from: some_text(&row.valid_from),
        until: some_text(&row.valid_until),
    };
    let value = Assertion {
        envelope: envelope(row),
        // The wire form of a reference is an object, never a bare id string
        // (§8.1, §13.2): a string can spell a local id and nothing else, and a
        // reader cannot tell one that was resolved from one that was guessed.
        proposition: serde_json::json!({ "id": row.proposition_id }),
        asserted_by: row.asserted_by.clone(),
        stance: match row.stance.as_str() {
            "support" => Some(Stance::Support),
            "reject" => Some(Stance::Reject),
            "uncertain" => Some(Stance::Uncertain),
            _ => None,
        },
        mode: match row.mode.as_str() {
            "observed" => Some(AssertionMode::Observed),
            "stated" => Some(AssertionMode::Stated),
            "inferred" => Some(AssertionMode::Inferred),
            "predicted" => Some(AssertionMode::Predicted),
            "hypothetical" => Some(AssertionMode::Hypothetical),
            "imported" => Some(AssertionMode::Imported),
            _ => None,
        },
        // The out-of-band sentinel means "the actor stated none", and it must
        // not surface as a real value: a negative number would read as an
        // extraordinarily strong denial rather than as silence. Every negative
        // is read as the sentinel, not only the exact one written today: a row
        // stored before `[0, 1]` was enforced on the way in may hold any of
        // them, and letting one of those through is the one direction this
        // must never be wrong in.
        confidence: (row.confidence >= 0.0).then_some(row.confidence),
        asserted_at: some_text(&row.asserted_at),
        valid_time: (valid_time.from.is_some() || valid_time.until.is_some()).then_some(valid_time),
        // Mapped field by field rather than through `from_value(..).ok()`:
        // dropping a citation that failed to decode would make the Assertion
        // look like it cited less than it did, which is the one direction an
        // evidence list must never be wrong in. A malformed entry surfaces as
        // an empty id instead of disappearing.
        evidence: row
            .evidence_refs
            .iter()
            .map(|value| EvidenceRef {
                id: value
                    .get("id")
                    .and_then(Json::as_str)
                    .unwrap_or_default()
                    .to_string(),
                role: value.get("role").and_then(Json::as_str).map(str::to_string),
            })
            .collect(),
        context_refs: row.context_refs.clone(),
        lifecycle: Some(AssertionLifecycle {
            status: match row.status.as_str() {
                "active" => Some(AssertionStatus::Active),
                "retracted" => Some(AssertionStatus::Retracted),
                "superseded" => Some(AssertionStatus::Superseded),
                "expired" => Some(AssertionStatus::Expired),
                _ => None,
            },
            supersedes: row.supersedes.clone(),
            superseded_by: row.superseded_by.clone(),
            retracted_at: some_text(&row.retracted_at),
        }),
    };
    finish(&value, &row.structural, &[])
}

fn evidence(row: &EvidenceRow) -> Json {
    let value = Evidence {
        envelope: envelope(row),
        evidence_class: row.evidence_class.clone(),
        payload: Some(EvidencePayload {
            mode: some_text(&row.payload_mode),
            inline: (!row.payload_inline.is_null()).then(|| row.payload_inline.clone()),
            content_ref: some_text(&row.content_ref),
        }),
        content_digest: some_text(&row.content_digest),
        media_type: some_text(&row.media_type),
        observed_at: some_text(&row.observed_at),
        source: row.source_refs.clone(),
        generated_by: some_text(&row.generated_by).map(|id| serde_json::json!({"id": id})),
        lifecycle: Some(EvidenceLifecycle {
            status: some_text(&row.status),
            corrects: row.corrects.clone(),
            corrected_by: row.corrected_by.clone(),
        }),
    };
    finish(&value, &row.structural, &[])
}

fn activity(row: &ActivityRow) -> Json {
    let value = Activity {
        envelope: envelope(row),
        activity_class: row.activity_class.clone(),
        started_at: some_text(&row.started_at),
        ended_at: some_text(&row.ended_at),
        inputs: row.inputs.clone(),
        outputs: row.outputs.clone(),
        associated_actors: row.associated_actors.clone(),
        parameters_digest: some_text(&row.parameters_digest),
        status: some_text(&row.status),
    };
    finish(&value, &row.structural, &[])
}

#[cfg(test)]
mod tests {
    use super::*;
    use anda_kip::PathStep;

    fn path(steps: &[&str]) -> Vec<PathStep> {
        steps
            .iter()
            .map(|s| PathStep::Field((*s).to_string()))
            .collect()
    }

    #[test]
    fn a_concept_renders_the_names_a_query_writes() {
        let view = render(&Element::Concept(Box::new(ConceptRow {
            _id: 7,
            space: "s".into(),
            state: state::ACTIVE.into(),
            version: 3,
            seq: 9,
            schema_ref: "kip://p@1.0.0/Person".into(),
            name: "Alice".into(),
            attributes: Map::from_iter([("display_name".into(), Json::from("Alice"))]),
            facets: Map::from_iter([(
                "kip://p@1.0.0/MnemonicState".into(),
                serde_json::json!({"salience": 0.5}),
            )]),
            ..Default::default()
        })));

        assert_eq!(view["id"], "C-7");
        assert_eq!(view["kind"], "concept");
        assert_eq!(read_path(&view, &path(&["name"])), Json::from("Alice"));
        assert_eq!(
            read_path(&view, &path(&["attributes", "display_name"])),
            Json::from("Alice")
        );
        assert_eq!(
            read_path(
                &view,
                &[
                    PathStep::Field("facets".into()),
                    PathStep::Key("kip://p@1.0.0/MnemonicState".into()),
                    PathStep::Field("salience".into()),
                ]
            ),
            Json::from(0.5)
        );
        assert_eq!(
            read_path(&view, &path(&["_system", "version"])),
            Json::from(3)
        );
        assert_eq!(
            read_path(&view, &path(&["_system", "state"])),
            Json::from("active")
        );
    }

    #[test]
    fn a_missing_member_reads_as_null_rather_than_failing() {
        // Open-world: "no such attribute" is an answer, and OPTIONAL and
        // IS_NULL both depend on being able to express it.
        let view = render(&Element::Concept(Box::default()));
        assert_eq!(read_path(&view, &path(&["attributes", "nope"])), Json::Null);
        assert_eq!(read_path(&view, &path(&["nope", "deeper"])), Json::Null);
    }

    #[test]
    fn an_unstated_confidence_does_not_surface_as_a_number() {
        // The storage sentinel is -1; rendering it would read as an
        // extraordinarily strong denial rather than as silence.
        let silent = render(&Element::Assertion(Box::new(AssertionRow {
            _id: 1,
            confidence: crate::kml::clauses::NO_CONFIDENCE,
            stance: "support".into(),
            status: "active".into(),
            ..Default::default()
        })));
        assert_eq!(read_path(&silent, &path(&["confidence"])), Json::Null);

        // A row written before `[0, 1]` was enforced on the way in may hold
        // any negative, and every one of them means the same thing. Rendering
        // one as a number is the failure this guard exists to prevent, so the
        // test is over the whole range rather than over the one value the
        // writer happens to produce today.
        let legacy = render(&Element::Assertion(Box::new(AssertionRow {
            _id: 1,
            confidence: -0.5,
            stance: "support".into(),
            status: "active".into(),
            ..Default::default()
        })));
        assert_eq!(read_path(&legacy, &path(&["confidence"])), Json::Null);

        let stated = render(&Element::Assertion(Box::new(AssertionRow {
            _id: 1,
            confidence: 0.0,
            ..Default::default()
        })));
        // Zero is a real claim of no support, and must survive.
        assert_eq!(read_path(&stated, &path(&["confidence"])), Json::from(0.0));
    }

    #[test]
    fn an_assertion_exposes_its_lifecycle_and_validity() {
        let view = render(&Element::Assertion(Box::new(AssertionRow {
            _id: 4,
            proposition_id: "P-1".into(),
            status: "superseded".into(),
            superseded_by: vec!["A-9".into()],
            valid_from: "2026-01-01T00:00:00.000Z".into(),
            ..Default::default()
        })));
        // §13.2 spells the slot `proposition` and its value as a reference
        // object, so `?a.proposition.id` is what a KQL dot path reads.
        assert_eq!(
            read_path(&view, &path(&["proposition", "id"])),
            Json::from("P-1")
        );
        assert_eq!(view["proposition_id"], Json::Null);
        assert_eq!(
            read_path(&view, &path(&["lifecycle", "status"])),
            Json::from("superseded")
        );
        assert_eq!(
            read_path(&view, &path(&["valid_time", "from"])),
            Json::from("2026-01-01T00:00:00.000Z")
        );
        assert_eq!(
            read_path(&view, &path(&["valid_time", "until"])),
            Json::Null
        );
    }

    #[test]
    fn profile_structural_fields_stay_out_of_attributes() {
        // Putting them in `attributes` would make record topology look like
        // representation-local state, which is the distinction §8 exists for.
        let view = render(&Element::Concept(Box::new(ConceptRow {
            _id: 1,
            structural: Map::from_iter([(
                "kip://p@1.0.0/has_step".into(),
                serde_json::json!([{"id": "C-2"}]),
            )]),
            ..Default::default()
        })));
        assert!(view["attributes"].get("kip://p@1.0.0/has_step").is_none());
        assert_eq!(view["structural"]["kip://p@1.0.0/has_step"][0]["id"], "C-2");
    }
}
