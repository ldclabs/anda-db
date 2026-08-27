//! End-to-end tests for the Specification requirements this engine used to
//! accept and then quietly drop.
//!
//! Every case here failed before the fix, and most of them failed *silently* —
//! a command reported success and the value went nowhere, or an answer came
//! back well-formed and wrong. That is the failure mode these tests exist to
//! keep out, so each one asserts the observable consequence rather than the
//! mechanism.

use anda_cognitive_nexus::{
    CognitiveNexus,
    nexus::{DEFAULT_SPACE, RetentionAction},
    profiles::COGNITIVE_MEMORY,
    schema::{PackageState, SchemaLock, SchemaPackage},
};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{
    Executor, IngestContext, IngestEvidence, Json, Preconditions, Request, TopLevelStatus,
};
use object_store::memory::InMemory;
use serde_json::json;
use std::sync::Arc;

const PROFILE_ID: &str = "kip://profiles/cognitive-memory";

async fn nexus(name: &str) -> CognitiveNexus {
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: name.to_string(),
            description: "conformance fixes".to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let nexus = CognitiveNexus::connect(Arc::new(db)).await.unwrap();
    nexus
        .install_package(&SchemaPackage::parse(COGNITIVE_MEMORY).unwrap(), "test")
        .await
        .unwrap();
    let mut lock = SchemaLock::default();
    lock.packages
        .insert(PROFILE_ID.to_string(), "2.0.0".to_string());
    lock.states
        .insert(PROFILE_ID.to_string(), PackageState::Active);
    nexus.activate_schema(DEFAULT_SPACE, lock).await.unwrap();
    nexus
}

async fn go(nexus: &CognitiveNexus, request: Request) -> anda_kip::Response {
    let parsed = match request.operations[0].parse() {
        Ok(parsed) => parsed,
        Err(err) => return err.into(),
    };
    nexus
        .execute(parsed, &request, &request.operations[0])
        .await
}

fn with(command: &str, params: Json) -> Request {
    serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "operations": [{"command": command, "parameters": params}]
    }))
    .unwrap()
}

async fn ok(nexus: &CognitiveNexus, command: &str, params: Json) -> Json {
    let response = go(nexus, with(command, params)).await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "{command}\n{:#?}",
        response.results
    );
    response.first_result().cloned().unwrap_or(Json::Null)
}

async fn err(nexus: &CognitiveNexus, command: &str, params: Json) -> anda_kip::ErrorObject {
    let response = go(nexus, with(command, params)).await;
    response
        .results
        .into_iter()
        .find_map(|result| result.error)
        .or(response.error)
        .unwrap_or_else(|| panic!("{command} was expected to fail"))
}

fn handle(result: &Json, name: &str) -> String {
    result["handles"][name]
        .as_str()
        .unwrap_or_else(|| panic!("no handle ?{name} in {result}"))
        .to_string()
}

async fn seeded(name: &str) -> (CognitiveNexus, Json) {
    let nexus = nexus(name).await;
    let created = ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark mode" }
            ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
        }"#,
        json!({}),
    )
    .await;
    (nexus, created)
}

// ---------------------------------------------------------------------------
// §20.13 — the Core registries hold for a bound parameter too
// ---------------------------------------------------------------------------

/// A stance the Core Package does not name is refused wherever it is written.
///
/// The protocol layer checks a written literal, but a `:parameter` is bound at
/// execution time — and that is the path that used to store `"maybe"`, read
/// back as `null`, and count in the projection as an actor who engaged.
#[tokio::test]
async fn a_core_registry_holds_against_a_bound_parameter() {
    let (nexus, created) = seeded("registry").await;
    let proposition = handle(&created, "p");
    let alice = handle(&created, "alice");

    for (stance, mode) in [(":bad", r#""stated""#), (r#""support""#, ":bad")] {
        let label = if stance == ":bad" { "stance" } else { "mode" };
        let error = err(
            &nexus,
            &format!(
                r#"CREATE ASSERTION ?a {{ SET FIELDS {{
                     proposition: :p, asserted_by: :who, stance: {stance}, mode: {mode}
                }} }}"#
            ),
            json!({
                "p": {"id": proposition},
                "who": {"id": alice},
                "bad": if label == "stance" { "maybe" } else { "guessed" },
            }),
        )
        .await;
        assert_eq!(error.code, "ConstraintViolation", "{label}");
    }

    // An Evidence citation role is a Core registry too (§56.2): `challenge`
    // and `support` are the difference between dissent and corroboration.
    let evidence = handle(
        &ok(
            &nexus,
            r#"CREATE EVIDENCE ?e { SET FIELDS {evidence_class: "user_statement", payload: "x"} }"#,
            json!({}),
        )
        .await,
        "e",
    );
    let error = err(
        &nexus,
        r#"CREATE ASSERTION ?a {
             SET FIELDS { proposition: :p, asserted_by: :who, stance: "support", mode: "stated" }
             SET STRUCTURAL { ("evidence", :e) {role: :role} }
           }"#,
        json!({"p": {"id": proposition}, "who": {"id": alice}, "e": {"id": evidence}, "role": "vouches"}),
    )
    .await;
    assert_eq!(error.code, "ConstraintViolation");
}

/// Confidence is bounded at both ends, and the lower one is load-bearing.
///
/// `-1` is the stored stand-in for "the actor stated none", so a negative that
/// got through would not be stored wrong — it would be stored as silence.
#[tokio::test]
async fn confidence_is_bounded_at_both_ends() {
    let (nexus, created) = seeded("confidence").await;
    let params = json!({
        "p": {"id": handle(&created, "p")},
        "who": {"id": handle(&created, "alice")},
    });
    for bad in [-0.5, 1.5] {
        let mut params = params.clone();
        params["c"] = json!(bad);
        let error = err(
            &nexus,
            r#"CREATE ASSERTION ?a { SET FIELDS {
                 proposition: :p, asserted_by: :who, stance: "support",
                 mode: "stated", confidence: :c
            } }"#,
            params,
        )
        .await;
        assert_eq!(error.code, "ConstraintViolation", "confidence {bad}");
    }
}

// ---------------------------------------------------------------------------
// §52.1 — CLIENT KEY is retry-safe creation
// ---------------------------------------------------------------------------

/// The same `CLIENT KEY` twice resolves rather than creating twice.
#[tokio::test]
async fn a_client_key_makes_a_resend_a_retry_rather_than_a_second_creation() {
    let nexus = nexus("client_key").await;
    let command = r#"CREATE EVIDENCE ?e {
        CLIENT KEY :k
        SET FIELDS {evidence_class: "user_statement", payload: "I prefer dark mode."}
    }"#;
    let params = json!({"k": "message:42:evidence"});
    let first = ok(&nexus, command, params.clone()).await;
    let second = ok(&nexus, command, params).await;
    assert_eq!(handle(&first, "e"), handle(&second, "e"));

    // And the retry wrote nothing: a spare row left behind would still be a
    // duplicate, just an invisible one.
    let all = ok(&nexus, r#"FIND(?e) WHERE { ?e EVIDENCE {} }"#, json!({})).await;
    assert_eq!(all.as_array().map(Vec::len), Some(1));
}

// ---------------------------------------------------------------------------
// §71.1 / §35.4 / §67 — the request envelope
// ---------------------------------------------------------------------------

/// Ingested Evidence is minted from the envelope and cited as `:key`.
#[tokio::test]
async fn ingested_evidence_reaches_a_command_without_passing_through_its_text() {
    let (nexus, created) = seeded("ingest").await;
    let mut request = with(
        r#"CREATE ASSERTION ?a {
             SET FIELDS { proposition: :p, asserted_by: :who, stance: "support", mode: "observed" }
             SET STRUCTURAL { ("evidence", :msg) {role: "support"} }
           }"#,
        json!({"p": {"id": handle(&created, "p")}, "who": {"id": handle(&created, "alice")}}),
    );
    request.ingest = Some(IngestContext {
        evidence: vec![IngestEvidence {
            key: "msg".into(),
            evidence_class: "user_statement".into(),
            payload: Some(json!("I prefer dark mode.")),
            media_type: Some("text/plain".into()),
            ..Default::default()
        }],
        extensions: None,
    });
    let response = go(&nexus, request).await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "{:#?}",
        response.results
    );

    // The payload arrived byte for byte from the transport, not re-typed by a
    // model into command text (§88.12).
    let payload = ok(
        &nexus,
        r#"FIND(?e.payload.inline) WHERE { ?e EVIDENCE {} }"#,
        json!({}),
    )
    .await;
    assert_eq!(payload, json!(["I prefer dark mode."]));
}

/// A stale precondition stops the write it was written to stop.
#[tokio::test]
async fn a_precondition_the_space_no_longer_meets_refuses_the_command() {
    let nexus = nexus("preconditions").await;
    let mut request = with(
        r#"CREATE CONCEPT ?c { TYPE "Person" NAME "Bob" }"#,
        json!({}),
    );
    request.preconditions = Some(Preconditions {
        space_seq: Some(999_999),
        schema_environment_version: None,
        extensions: None,
    });
    let response = go(&nexus, request).await;
    assert_eq!(
        response.error.as_ref().map(|error| error.code.as_str()),
        Some("PreconditionFailed")
    );
    // And nothing was written: a guard that failed after the write would be
    // decoration.
    let people = ok(&nexus, r#"FIND(?c) WHERE { ?c CONCEPT {} }"#, json!({})).await;
    assert_eq!(people.as_array().map(Vec::len), Some(0));
}

/// A capability requirement this engine does not meet fails fast (§67).
#[tokio::test]
async fn an_unmet_capability_requirement_refuses_before_the_command_runs() {
    let nexus = nexus("requires").await;
    let mut request = with("DESCRIBE PROTOCOL", json!({}));
    request.requires = Some(serde_json::from_value(json!({"semantic_search": true})).unwrap());
    let response = go(&nexus, request).await;
    assert_eq!(
        response.error.as_ref().map(|error| error.code.as_str()),
        Some("UnsupportedCapability")
    );

    // An unknown name is refused too: a fail-fast check that passed because
    // nobody recognized it is worse than no check.
    let mut request = with("DESCRIBE PROTOCOL", json!({}));
    request.requires = Some(serde_json::from_value(json!({"telepathy": true})).unwrap());
    assert!(go(&nexus, request).await.error.is_some());

    // One this engine does implement passes through.
    let mut request = with("DESCRIBE PROTOCOL", json!({}));
    request.requires = Some(serde_json::from_value(json!({"ingest": true})).unwrap());
    assert_eq!(go(&nexus, request).await.status, TopLevelStatus::Succeeded);
}

// ---------------------------------------------------------------------------
// §46.4 / §47.3 — projection answers
// ---------------------------------------------------------------------------

/// A grounded BELIEF over a Proposition nobody created still answers.
///
/// Zero rows would make the Agent infer "unknown" from "the pattern did not
/// match" — the inference §24 exists to prevent, and indistinguishable from a
/// query that was written wrong.
#[tokio::test]
async fn a_grounded_belief_about_a_missing_proposition_is_insufficient_not_empty() {
    let nexus = nexus("ungrounded").await;
    let created = ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark mode" }
        }"#,
        json!({}),
    )
    .await;
    let rows = ok(
        &nexus,
        r#"FIND(?b) WHERE { ?b BELIEF (:a, "prefers", :d) }"#,
        json!({"a": {"id": handle(&created, "alice")}, "d": {"id": handle(&created, "dark")}}),
    )
    .await;
    let rows = rows.as_array().expect("one row");
    assert_eq!(rows.len(), 1, "§46.4 answers rather than returning nothing");
    assert_eq!(rows[0]["status"], "insufficient");
    assert_eq!(rows[0]["proposition_id"], Json::Null);

    // And the read did not create the Proposition to have something to point
    // at.
    let propositions = ok(
        &nexus,
        r#"FIND(?p) WHERE { ?p PROPOSITION (?s, ?v, ?o) }"#,
        json!({}),
    )
    .await;
    assert_eq!(propositions.as_array().map(Vec::len), Some(0));
}

/// A BELIEF SLOT reports its own coordinates and a wire-shaped subject.
#[tokio::test]
async fn an_empty_slot_reports_the_coordinates_it_ran_under() {
    let (nexus, created) = seeded("slot").await;
    let rows = ok(
        &nexus,
        r#"FIND(?s) WHERE { ?s BELIEF SLOT (:a, "prefers") }"#,
        json!({"a": {"id": handle(&created, "alice")}}),
    )
    .await;
    let slot = &rows.as_array().expect("one row")[0];
    assert_eq!(slot["status"], "insufficient");
    assert_eq!(slot["accepted_values"], json!([]));
    // §47.4 is exactly the empty case, so the policy and the coordinate have
    // to come from the slot rather than from a candidate that is not there.
    assert!(slot["policy"]["id"].is_string(), "{slot}");
    assert!(slot["temporal"]["valid_at"].is_string(), "{slot}");
    assert!(slot["explanation"].is_object(), "{slot}");
    // §8: the reference shape, never the engine's internal endpoint key.
    assert_eq!(slot["subject"], json!({"id": handle(&created, "alice")}));
}

/// `explanation: "none"` returns no ledger, not an empty one (§49.1, §49.2).
#[tokio::test]
async fn an_explanation_level_decides_what_comes_back() {
    let (nexus, created) = seeded("explanation").await;
    ok(
        &nexus,
        r#"CREATE ASSERTION ?a { SET FIELDS {
             proposition: :p, asserted_by: :who, stance: "support",
             mode: "stated", confidence: 0.9
        } }"#,
        json!({"p": {"id": handle(&created, "p")}, "who": {"id": handle(&created, "alice")}}),
    )
    .await;

    // `BELIEF (id: …)` names a Proposition by record identity, so the
    // parameter carries the id itself rather than a reference object.
    let params = json!({"p": handle(&created, "p")});
    let full = ok(
        &nexus,
        r#"FIND(?b) WHERE { ?b BELIEF (id: :p) }"#,
        params.clone(),
    )
    .await;
    assert!(full[0]["explanation"].is_object());
    assert_eq!(
        full[0]["support"]["assertion_ids"].as_array().map(Vec::len),
        Some(1)
    );

    let quiet = ok(
        &nexus,
        r#"FIND(?b) WHERE { ?b BELIEF (id: :p) } WITH EPISTEMIC { explanation: "none" }"#,
        params,
    )
    .await;
    assert!(quiet[0]["explanation"].is_null(), "{}", quiet[0]);
    // The Assertion ids *are* the ledger; withholding the ledger and handing
    // them over under another key would be no withholding at all. Absent
    // rather than empty, because an empty list would read as "nothing
    // supports this" — which is a claim, and a false one.
    assert!(
        quiet[0]["support"]["assertion_ids"].is_null(),
        "{}",
        quiet[0]
    );
    assert_eq!(quiet[0]["status"], "accepted", "the answer still answers");
}

/// A setting `WITH EPISTEMIC` does not implement is refused, never ignored.
#[tokio::test]
async fn an_unknown_epistemic_setting_is_refused_rather_than_dropped() {
    let (nexus, created) = seeded("settings").await;
    let error = err(
        &nexus,
        r#"FIND(?b) WHERE { ?b BELIEF (id: :p) } WITH EPISTEMIC { curiosity: 0.5 }"#,
        json!({"p": {"id": handle(&created, "p")}}),
    )
    .await;
    assert_eq!(error.code, "SchemaFieldNotFound");
}

// ---------------------------------------------------------------------------
// §17.4 — ordered structural references
// ---------------------------------------------------------------------------

/// An explicit position is honored, and an impossible one fails validation.
#[tokio::test]
async fn an_ordered_structural_field_honors_the_position_it_was_given() {
    let nexus = nexus("ordered").await;
    let created = ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?exp {
                TYPE "Experience"
                SET ATTRIBUTES {goal: "learn", outcome_status: "success"}
            }
            CREATE CONCEPT ?one { TYPE "ExperienceStep" SET ATTRIBUTES {step_kind: "action", summary: "one"} }
            CREATE CONCEPT ?two { TYPE "ExperienceStep" SET ATTRIBUTES {step_kind: "action", summary: "two"} }
        }"#,
        json!({}),
    )
    .await;
    let params = json!({
        "exp": handle(&created, "exp"),
        "one": {"id": handle(&created, "one")},
        "two": {"id": handle(&created, "two")},
    });

    ok(
        &nexus,
        r#"UPDATE :exp SET STRUCTURAL { ("has_step", :one) }"#,
        params.clone(),
    )
    .await;
    // Position 0 puts the second step *first*, which is the whole point of an
    // explicit index.
    ok(
        &nexus,
        r#"UPDATE :exp SET STRUCTURAL { ("has_step", :two) {index: 0} }"#,
        params.clone(),
    )
    .await;
    let order = ok(
        &nexus,
        r#"FIND(?step.attributes.summary) WHERE { ?e STRUCTURAL (?src, "has_step", ?step) } ORDER BY ?step.attributes.summary"#,
        json!({}),
    )
    .await;
    assert_eq!(order, json!(["one", "two"]));

    // The order itself is readable through the edge binding (§43.7).
    let indexed = ok(
        &nexus,
        r#"FIND(?step.attributes.summary, ?e.index) WHERE { ?e STRUCTURAL (?src, "has_step", ?step) } ORDER BY ?e.index"#,
        json!({}),
    )
    .await;
    assert_eq!(indexed, json!([["two", 0], ["one", 1]]));

    // A position outside the dense range fails validation rather than landing
    // wherever it fits.
    let error = err(
        &nexus,
        r#"UPDATE :exp SET STRUCTURAL { ("has_step", :one) {index: 99} }"#,
        params.clone(),
    )
    .await;
    assert_eq!(error.code, "ConstraintViolation");

    // Two references cannot both be third.
    let error = err(
        &nexus,
        r#"UPDATE :exp SET STRUCTURAL { ("has_step", :one) {index: 0} ("has_step", :two) {index: 0} }"#,
        params,
    )
    .await;
    assert_eq!(error.code, "ConstraintViolation");
}

/// §17.5: a single-cardinality field is replaced, never appended to.
#[tokio::test]
async fn a_single_cardinality_structural_field_is_replaced() {
    let nexus = nexus("single_cardinality").await;
    let created = ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?exp {
                TYPE "Experience"
                SET ATTRIBUTES {goal: "learn", outcome_status: "success"}
            }
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?bob { TYPE "Person" NAME "Bob" }
        }"#,
        json!({}),
    )
    .await;
    let bob = handle(&created, "bob");
    let params = json!({
        "exp": handle(&created, "exp"),
        "alice": {"id": handle(&created, "alice")},
        "bob": {"id": bob},
    });

    ok(
        &nexus,
        r#"UPDATE :exp SET STRUCTURAL { ("experienced_by", :alice) }"#,
        params.clone(),
    )
    .await;
    // Appending here would fail the cardinality check and refuse the one write
    // this form exists for.
    ok(
        &nexus,
        r#"UPDATE :exp SET STRUCTURAL { ("experienced_by", :bob) }"#,
        params.clone(),
    )
    .await;
    let who = ok(
        &nexus,
        r#"FIND(?p.id) WHERE { ?e STRUCTURAL (?src, "experienced_by", ?p) }"#,
        json!({}),
    )
    .await;
    assert_eq!(who, json!([bob]));

    // `experienced_by` declares no order, so a position on it orders nothing
    // and is refused rather than dropped (§17.4).
    let error = err(
        &nexus,
        r#"UPDATE :exp SET STRUCTURAL { ("experienced_by", :alice) {index: 0} }"#,
        params,
    )
    .await;
    assert_eq!(error.code, "ConstraintViolation");
}

// ---------------------------------------------------------------------------
// §44.8 / §88.4 — cursors
// ---------------------------------------------------------------------------

/// A cursor is opaque, snapshot-pinned, and belongs to its own family.
#[tokio::test]
async fn a_page_cursor_is_opaque_and_not_interchangeable() {
    let nexus = nexus("cursors").await;
    for name in ["Alice", "Bob", "Carol"] {
        ok(
            &nexus,
            &format!(r#"CREATE CONCEPT ?c {{ TYPE "Person" NAME "{name}" }}"#),
            json!({}),
        )
        .await;
    }

    let first = go(
        &nexus,
        with(
            r#"FIND(?c.name) WHERE { ?c CONCEPT {} } ORDER BY ?c.name LIMIT 2"#,
            json!({}),
        ),
    )
    .await;
    let cursor = first.next_cursor.clone().expect("more rows remain");
    assert!(
        cursor.parse::<u64>().is_err(),
        "a cursor is opaque, not an offset a caller can invent: {cursor}"
    );
    // §50: the answer says which coordinate it read at.
    assert!(
        first.results[0]
            .context
            .as_ref()
            .unwrap()
            .snapshot_seq
            .is_some()
    );

    let second = ok(
        &nexus,
        &format!(
            r#"FIND(?c.name) WHERE {{ ?c CONCEPT {{}} }} ORDER BY ?c.name LIMIT 2 CURSOR "{cursor}""#
        ),
        json!({}),
    )
    .await;
    assert_eq!(second, json!(["Carol"]));

    // A bare offset is not a cursor this engine issued.
    let error = err(
        &nexus,
        r#"FIND(?c.name) WHERE { ?c CONCEPT {} } LIMIT 2 CURSOR "2""#,
        json!({}),
    )
    .await;
    assert_eq!(error.code, "CursorInvalidated");

    // Neither is one from another operation family (§102.28), even though both
    // count from zero.
    let listed = go(&nexus, with("LIST TYPES LIMIT 1", json!({}))).await;
    let list_cursor = listed.results[0]
        .next_cursor
        .clone()
        .expect("more types remain");
    let error = err(
        &nexus,
        &format!(r#"FIND(?c.name) WHERE {{ ?c CONCEPT {{}} }} LIMIT 2 CURSOR "{list_cursor}""#),
        json!({}),
    )
    .await;
    assert_eq!(error.code, "CursorInvalidated");
}

// ---------------------------------------------------------------------------
// §19 — retention
// ---------------------------------------------------------------------------

/// A retention member outside §19.1's shape is refused, not stored and lost.
#[tokio::test]
async fn a_retention_member_the_hook_does_not_have_is_refused() {
    let (nexus, created) = seeded("retention_shape").await;
    let error = err(
        &nexus,
        r#"SET RETENTION :x {retention_class: "standard", review_at: "2030-01-01T00:00:00Z"}"#,
        json!({"x": handle(&created, "alice")}),
    )
    .await;
    assert_eq!(error.code, "SchemaFieldNotFound");
}

/// An expired record leaves ordinary recall when a sweep runs, and a held one
/// does not.
#[tokio::test]
async fn a_retention_sweep_acts_on_what_lapsed_and_reports_what_it_left() {
    let (nexus, created) = seeded("retention_sweep").await;
    let alice = handle(&created, "alice");
    let dark = handle(&created, "dark");
    ok(
        &nexus,
        r#"SET RETENTION :x {retention_class: "short", expires_at: "2020-01-01T00:00:00Z"}"#,
        json!({"x": alice.clone()}),
    )
    .await;
    ok(
        &nexus,
        r#"SET RETENTION :x {expires_at: "2020-01-01T00:00:00Z", legal_hold: true}"#,
        json!({"x": dark.clone()}),
    )
    .await;

    let report = nexus
        .system_session()
        .sweep_expired(DEFAULT_SPACE, RetentionAction::Archive, 10)
        .await
        .unwrap();
    assert_eq!(report.swept, vec![alice.clone()]);
    // §163: a hold blocks removal for everyone, and the sweep says so rather
    // than reporting a smaller number that reads as the whole truth.
    assert_eq!(report.held, 1);

    // Archived means out of ordinary recall, and still there.
    let recalled = ok(
        &nexus,
        r#"FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} }"#,
        json!({}),
    )
    .await;
    assert_eq!(recalled, json!([]));
    let found = ok(
        &nexus,
        r#"FIND(?c.name) WHERE { ?c CONCEPT {id: :x, state: "archived"} }"#,
        json!({"x": alice}),
    )
    .await;
    assert_eq!(found, json!(["Alice"]));
}

// ---------------------------------------------------------------------------
// §5.6 / §64.2 — Space self identity
// ---------------------------------------------------------------------------

/// The Primer distinguishes the authenticated Principal from the semantic self.
#[tokio::test]
async fn the_primer_tells_the_principal_and_the_self_apart() {
    let (nexus, created) = seeded("self").await;
    let primer = ok(&nexus, "DESCRIBE PRIMER", json!({})).await;
    assert!(primer["execution_context"]["principal"]["id"].is_string());
    // A Space that has designated no self says so rather than guessing at
    // whichever Person Concept looks like the Brain.
    assert_eq!(primer["cognitive_identity"]["self_concept"], Json::Null);

    let alice = handle(&created, "alice");
    nexus
        .system_session()
        .designate_self(DEFAULT_SPACE, Some(alice.parse().unwrap()))
        .await
        .unwrap();
    let primer = ok(&nexus, "DESCRIBE PRIMER", json!({})).await;
    assert_eq!(
        primer["cognitive_identity"]["self_concept"],
        json!({"id": alice})
    );

    // Ordinary KML has no path to it, and a dangling designation is refused
    // rather than stored as a broken link.
    assert!(
        nexus
            .system_session()
            .designate_self(DEFAULT_SPACE, Some("C-9999".parse().unwrap()))
            .await
            .is_err()
    );
}

// ---------------------------------------------------------------------------
// §11.3 — merged references canonicalize on every new write
// ---------------------------------------------------------------------------

/// A claim written after a merge lands on the surviving identity.
#[tokio::test]
async fn a_new_assertion_canonicalizes_its_actor_through_a_merge() {
    let nexus = nexus("merge_actor").await;
    let created = ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?old { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?new { TYPE "Person" NAME "Alice Smith" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark mode" }
        }"#,
        json!({}),
    )
    .await;
    let old = handle(&created, "old");
    let new = handle(&created, "new");
    ok(
        &nexus,
        r#"MERGE CONCEPT :old INTO :new"#,
        json!({"old": old.clone(), "new": new.clone()}),
    )
    .await;

    let assertion = ok(
        &nexus,
        r#"MUTATE {
             ENSURE PROPOSITION ?p (:new, "prefers", :dark)
             CREATE ASSERTION ?a { SET FIELDS {
               proposition: ?p, asserted_by: :old, stance: "support", mode: "stated"
             } }
           }"#,
        json!({"new": {"id": new.clone()}, "dark": {"id": handle(&created, "dark")}, "old": {"id": old}}),
    )
    .await;

    let actor = ok(
        &nexus,
        r#"FIND(?a.asserted_by) WHERE { ?a ASSERTION {id: :id} }"#,
        json!({"id": handle(&assertion, "a")}),
    )
    .await;
    assert_eq!(
        actor,
        json!([{"id": new}]),
        "a merged actor keeps accumulating claims under the surviving identity"
    );
}
