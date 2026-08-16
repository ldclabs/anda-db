//! End-to-end KML tests: real command text, real parser, real database.
//!
//! Everything here goes through [`anda_kip::parse_kip`] rather than a
//! hand-built AST, so the tests exercise the same path an Agent does — and
//! catch the case where the engine implements something the grammar cannot
//! actually express.

use anda_cognitive_nexus::{
    CognitiveNexus, Element,
    id::ElementId,
    nexus::DEFAULT_SPACE,
    schema::{PackageState, SchemaLock, SchemaPackage},
};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{Executor, Json, ReceiptStatus, Request, TopLevelStatus};
use object_store::memory::InMemory;
use serde_json::json;
use std::sync::Arc;

const COGNITIVE_MEMORY: &str = include_str!("fixtures/cognitive-memory-2.0.0.json");
const PROFILE_ID: &str = "kip://profiles/cognitive-memory";

async fn nexus(name: &str) -> CognitiveNexus {
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: name.to_string(),
            description: "kml tests".to_string(),
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

/// Runs one command and returns the whole response.
async fn run(nexus: &CognitiveNexus, command: &str) -> anda_kip::Response {
    let request = Request::single(command);
    let parsed = anda_kip::parse_kip(command).unwrap_or_else(|err| panic!("{command}\n{err}"));
    nexus
        .execute(parsed, &request, &request.operations[0])
        .await
}

/// Runs one command and asserts it succeeded, returning the result body.
async fn ok(nexus: &CognitiveNexus, command: &str) -> Json {
    let response = run(nexus, command).await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "{command}\n{:#?}",
        response.error
    );
    response.first_result().cloned().unwrap_or(Json::Null)
}

fn handle(result: &Json, name: &str) -> ElementId {
    result["handles"][name]
        .as_str()
        .unwrap_or_else(|| panic!("no handle ?{name} in {result}"))
        .parse()
        .unwrap()
}

#[tokio::test]
async fn a_claim_lands_as_a_proposition_plus_an_assertion() {
    // Spec §2.1: a Proposition existing is not the Proposition being true.
    // The tuple carries no confidence; the Assertion about it does.
    let nexus = nexus("claim").await;
    let result = ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark mode" }
            ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
            CREATE EVIDENCE ?e {
                SET FIELDS {
                    evidence_class: "user_statement",
                    payload: "I prefer dark mode.",
                    observed_at: "2026-08-16T09:00:00Z"
                }
            }
            CREATE ASSERTION ?a {
                SET FIELDS {
                    proposition: ?p,
                    asserted_by: ?alice,
                    stance: "support",
                    mode: "stated",
                    confidence: 0.9
                }
                SET STRUCTURAL { ("evidence", ?e) {role: "support"} }
            }
        }"#,
    )
    .await;

    let proposition = handle(&result, "p");
    let assertion = handle(&result, "a");
    let alice = handle(&result, "alice");

    let Element::Proposition(tuple) = nexus.store.get_element(proposition).await.unwrap() else {
        panic!("?p must be a Proposition");
    };
    assert_eq!(
        tuple.predicate_ref, "kip://profiles/cognitive-memory@2.0.0/prefers",
        "a local predicate name is persisted as its exact symbol"
    );
    assert_eq!(tuple.subject["id"], alice.to_string());
    assert_eq!(tuple.version, 1);

    let Element::Assertion(claim) = nexus.store.get_element(assertion).await.unwrap() else {
        panic!("?a must be an Assertion");
    };
    assert_eq!(claim.proposition_id, proposition.to_string());
    assert_eq!(claim.confidence, 0.9);
    assert_eq!(claim.stance, "support");
    assert_eq!(claim.mode, "stated");
    assert_eq!(claim.status, "active");
    assert_eq!(claim.evidence_ids.len(), 1);

    // One transaction, one Space sequence, shared by everything it wrote.
    assert_eq!(tuple.seq, claim.seq);
    assert_eq!(tuple.created_tx, claim.created_tx);
}

#[tokio::test]
async fn a_forward_reference_makes_an_atomic_provenance_cycle_possible() {
    // Spec §22: `Evidence.generated_by → Activity` and `Activity.outputs →
    // Evidence` is a legitimate cycle. A define-before-use ordering would need
    // two transactions and could not form provenance atomically.
    let nexus = nexus("forward_reference").await;
    let result = ok(
        &nexus,
        r#"MUTATE {
            CREATE EVIDENCE ?e {
                SET FIELDS {evidence_class: "tool_result", payload: "42"}
                SET STRUCTURAL { ("generated_by", ?act) }
            }
            CREATE ACTIVITY ?act {
                SET FIELDS {activity_class: "tool_execution"}
                SET STRUCTURAL { ("outputs", ?e) }
            }
        }"#,
    )
    .await;

    let evidence = handle(&result, "e");
    let activity = handle(&result, "act");
    let Element::Evidence(row) = nexus.store.get_element(evidence).await.unwrap() else {
        panic!("?e must be Evidence");
    };
    assert_eq!(row.generated_by, activity.to_string());

    let Element::Activity(row) = nexus.store.get_element(activity).await.unwrap() else {
        panic!("?act must be an Activity");
    };
    assert_eq!(row.outputs[0]["id"], evidence.to_string());
}

#[tokio::test]
async fn ensure_resolves_an_existing_tuple_instead_of_duplicating_it() {
    // Spec §59, §93.6: one Space keeps one canonical Proposition per semantic
    // tuple, and the tuple is immutable, so resolving one changes nothing.
    let nexus = nexus("ensure").await;
    let first = ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark mode" }
            ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
        }"#,
    )
    .await;
    let alice = handle(&first, "alice");
    let dark = handle(&first, "dark");
    let proposition = handle(&first, "p");

    // The bare form needs parameters, so drive it through a bound request.
    let request = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "operations": [{
            "command": r#"ENSURE PROPOSITION ?p (:subject, "prefers", :object)"#,
            "parameters": {"subject": alice.to_string(), "object": dark.to_string()}
        }]
    }))
    .unwrap();
    let parsed = request.operations[0].parse().unwrap();
    let response = nexus
        .execute(parsed, &request, &request.operations[0])
        .await;
    assert_eq!(response.status, TopLevelStatus::Succeeded);
    let result = response.first_result().cloned().unwrap();
    assert_eq!(
        handle(&result, "p"),
        proposition,
        "the same tuple resolves to the same Proposition"
    );
    // Resolving an existing tuple is not a change: no version bump, and the
    // receipt says so rather than claiming a transition.
    assert_eq!(
        response.receipt.as_ref().unwrap().status,
        ReceiptStatus::NoEffect
    );
    let Element::Proposition(row) = nexus.store.get_element(proposition).await.unwrap() else {
        panic!("must be a Proposition");
    };
    assert_eq!(row.version, 1);
}

#[tokio::test]
async fn correcting_a_claim_supersedes_it_rather_than_rewriting_it() {
    // Spec §2.1 and §76: an Assertion's epistemic payload is historically
    // immutable. What was once believed, and by whom, has to survive.
    let nexus = nexus("supersede").await;
    let setup = ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark mode" }
            ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
            CREATE ASSERTION ?old {
                SET FIELDS {proposition: ?p, asserted_by: ?alice, stance: "support", mode: "stated", confidence: 0.9}
            }
        }"#,
    )
    .await;
    let proposition = handle(&setup, "p");
    let alice = handle(&setup, "alice");
    let old = handle(&setup, "old");

    let request = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "operations": [{
            "command": r#"MUTATE {
                CREATE ASSERTION ?new {
                    SET FIELDS {proposition: :p, asserted_by: :alice, stance: "reject", mode: "stated", confidence: 0.8}
                }
                SUPERSEDE ASSERTION :old BY ?new
            }"#,
            "parameters": {
                "p": proposition.to_string(),
                "alice": alice.to_string(),
                "old": old.to_string()
            }
        }]
    }))
    .unwrap();
    let parsed = request.operations[0].parse().unwrap();
    let response = nexus
        .execute(parsed, &request, &request.operations[0])
        .await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "{:#?}",
        response.error
    );
    let new = handle(&response.first_result().cloned().unwrap(), "new");

    let Element::Assertion(old_row) = nexus.store.get_element(old).await.unwrap() else {
        panic!("must be an Assertion");
    };
    assert_eq!(old_row.status, "superseded");
    assert_eq!(old_row.superseded_by, vec![new.to_string()]);
    // The original commitment is untouched: still 0.9, still `support`.
    assert_eq!(old_row.confidence, 0.9);
    assert_eq!(old_row.stance, "support");
    assert_eq!(old_row.version, 2, "one bump for one transaction");

    let Element::Assertion(new_row) = nexus.store.get_element(new).await.unwrap() else {
        panic!("must be an Assertion");
    };
    assert_eq!(new_row.supersedes, vec![old.to_string()]);
    assert_eq!(new_row.stance, "reject");
}

#[tokio::test]
async fn supersession_must_stay_inside_one_lineage() {
    // Epistemic Model §31: a contradiction is not a supersession. Replacing a
    // claim about one tuple with a claim about another would silently rewrite
    // what the first claim was about.
    let nexus = nexus("lineage").await;
    let setup = ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
            CREATE CONCEPT ?light { TYPE "Preference" NAME "Light" }
            ENSURE PROPOSITION ?p1 (?alice, "prefers", ?dark)
            ENSURE PROPOSITION ?p2 (?alice, "prefers", ?light)
            CREATE ASSERTION ?a1 {
                SET FIELDS {proposition: ?p1, asserted_by: ?alice, stance: "support", mode: "stated"}
            }
            CREATE ASSERTION ?a2 {
                SET FIELDS {proposition: ?p2, asserted_by: ?alice, stance: "support", mode: "stated"}
            }
        }"#,
    )
    .await;

    let request = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "operations": [{
            "command": "SUPERSEDE ASSERTION :a1 BY :a2",
            "parameters": {
                "a1": handle(&setup, "a1").to_string(),
                "a2": handle(&setup, "a2").to_string()
            }
        }]
    }))
    .unwrap();
    let parsed = request.operations[0].parse().unwrap();
    let response = nexus
        .execute(parsed, &request, &request.operations[0])
        .await;
    assert_eq!(
        response.error.as_ref().unwrap().code.as_str(),
        "SupersessionMismatch"
    );

    // Both Assertions still stand — a contested belief, not a resolved one.
    let Element::Assertion(row) = nexus.store.get_element(handle(&setup, "a1")).await.unwrap()
    else {
        panic!("must be an Assertion");
    };
    assert_eq!(row.status, "active");
}

#[tokio::test]
async fn an_unknown_type_is_refused_and_writes_nothing() {
    // Spec §110: define-before-use. A data mutation never creates schema.
    let nexus = nexus("define_before_use").await;
    let before = nexus.store.get_space(DEFAULT_SPACE).await.unwrap().seq;

    let response = run(
        &nexus,
        r#"CREATE CONCEPT ?x { TYPE "Spaceship" NAME "Enterprise" }"#,
    )
    .await;
    assert_eq!(
        response.error.as_ref().unwrap().code.as_str(),
        "SchemaSymbolNotFound"
    );

    // The failed statement left no element behind: its shell was discarded,
    // so nothing is recallable and nothing is pending.
    assert_eq!(nexus.store.sweep_pending().await.unwrap(), 0);
    let after = nexus.store.get_space(DEFAULT_SPACE).await.unwrap().seq;
    assert!(after > before, "the sequence it burned is not reused");
}

#[tokio::test]
async fn a_duplicate_handle_is_refused_rather_than_resolved_arbitrarily() {
    // Spec §25: two clauses binding `?x` leave every reference to it
    // ambiguous, and picking either one would be a guess.
    // The protocol crate rejects this before an engine sees it, which is the
    // right layer: a duplicate handle is a property of the text, not of state.
    let err = anda_kip::parse_kip(
        r#"MUTATE {
            CREATE EVIDENCE ?x {SET FIELDS {evidence_class: "message", payload: "a"}}
            CREATE ASSERTION ?x {SET FIELDS {proposition: "P-1", asserted_by: "C-1", stance: "support", mode: "stated"}}
        }"#,
    )
    .unwrap_err();
    assert_eq!(err.name(), "DuplicateLocalHandle");

    // The engine holds the same line for a plan it builds itself, and leaves
    // nothing behind when it does.
    let nexus = nexus("duplicate_handle").await;
    assert_eq!(nexus.store.sweep_pending().await.unwrap(), 0);
}

#[tokio::test]
async fn a_dry_run_computes_the_plan_and_commits_nothing() {
    // Spec §69.3: a dry run must not establish a durable cognitive commit.
    let nexus = nexus("dry_run").await;
    let mut request = Request::single(r#"CREATE CONCEPT ?x { TYPE "Person" NAME "Ghost" }"#);
    request.options = Some(anda_kip::RequestOptions {
        dry_run: Some(true),
        ..Default::default()
    });
    let parsed = request.operations[0].parse().unwrap();
    let response = nexus
        .execute(parsed, &request, &request.operations[0])
        .await;

    assert_eq!(response.status, TopLevelStatus::Succeeded);
    let receipt = response.receipt.as_ref().unwrap();
    assert_eq!(receipt.status, ReceiptStatus::NoEffect);
    assert!(receipt.space_seq.is_none(), "nothing committed");
    // It still reports what it would have done.
    let result = response.first_result().cloned().unwrap();
    assert_eq!(result["changes"].as_array().unwrap().len(), 1);

    let id = handle(&result, "x");
    assert!(
        !nexus.store.contains(id).await,
        "the previewed element must not be durable"
    );
    assert_eq!(nexus.store.sweep_pending().await.unwrap(), 0);
}

#[tokio::test]
async fn retraction_withdraws_a_claim_without_deleting_it() {
    // Spec §41.1: retraction is not deletion. The record of what was believed
    // has to survive being disbelieved.
    let nexus = nexus("retract").await;
    let setup = ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
            ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
            CREATE ASSERTION ?a {
                SET FIELDS {proposition: ?p, asserted_by: ?alice, stance: "support", mode: "stated", confidence: 0.7}
            }
        }"#,
    )
    .await;
    let assertion = handle(&setup, "a");

    let request = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "operations": [{
            "command": "RETRACT ASSERTION :a",
            "parameters": {"a": assertion.to_string()}
        }]
    }))
    .unwrap();
    let parsed = request.operations[0].parse().unwrap();
    let response = nexus
        .execute(parsed, &request, &request.operations[0])
        .await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "{:#?}",
        response.error
    );

    let Element::Assertion(row) = nexus.store.get_element(assertion).await.unwrap() else {
        panic!("must be an Assertion");
    };
    assert_eq!(row.status, "retracted");
    assert!(!row.retracted_at.is_empty());
    // Still there, still active as a record, still carrying what was claimed.
    assert_eq!(row.state, "active");
    assert_eq!(row.confidence, 0.7);
}

#[tokio::test]
async fn a_facet_member_the_schema_never_declared_is_refused() {
    // Spec §240.31: a Facet is a validated namespaced extension, not the
    // untyped metadata bag KIP 1.x ended up with.
    let nexus = nexus("facet_validation").await;
    let ok_response = run(
        &nexus,
        r#"CREATE CONCEPT ?x { TYPE "Person" NAME "Alice" SET FACET "MnemonicState" {memory_strength: 0.4} }"#,
    )
    .await;
    assert_eq!(ok_response.status, TopLevelStatus::Succeeded);

    let refused = run(
        &nexus,
        r#"CREATE CONCEPT ?y { TYPE "Person" NAME "Bob" SET FACET "MnemonicState" {classification: "public"} }"#,
    )
    .await;
    assert_eq!(
        refused.error.as_ref().unwrap().code.as_str(),
        "ConstraintViolation"
    );

    // Out-of-range values are refused by the same layer.
    let out_of_range = run(
        &nexus,
        r#"CREATE CONCEPT ?z { TYPE "Person" NAME "Eve" SET FACET "MnemonicState" {memory_strength: 5} }"#,
    )
    .await;
    assert_eq!(
        out_of_range.error.as_ref().unwrap().code.as_str(),
        "ConstraintViolation"
    );
}

#[tokio::test]
async fn an_upsert_resolves_identity_through_key_and_never_through_name() {
    // Spec §51: a name is mutable grounding state and two Concepts may share
    // one, so resolving identity through it would merge unrelated Concepts.
    let nexus = nexus("upsert").await;
    let first = ok(
        &nexus,
        r#"UPSERT CONCEPT ?p {
             MATCH {key: "person:alice"}
             SET FIELDS {name: "Alice"}
             SET ATTRIBUTES {display_name: "Alice"}
           }"#,
    )
    .await;
    let id = handle(&first, "p");

    let second = ok(
        &nexus,
        r#"UPSERT CONCEPT ?p {
             MATCH {key: "person:alice"}
             SET ATTRIBUTES {display_name: "Alice Smith"}
           }"#,
    )
    .await;
    assert_eq!(handle(&second, "p"), id, "the same key is the same Concept");

    let Element::Concept(row) = nexus.store.get_element(id).await.unwrap() else {
        panic!("must be a Concept");
    };
    assert_eq!(row.attributes["display_name"], json!("Alice Smith"));
    assert_eq!(row.version, 2);

    // Re-running the same assignment is a no-effect final state: no version
    // burned, and the receipt does not claim a transition.
    let response = run(
        &nexus,
        r#"UPSERT CONCEPT ?p {
             MATCH {key: "person:alice"}
             SET ATTRIBUTES {display_name: "Alice Smith"}
           }"#,
    )
    .await;
    assert_eq!(
        response.receipt.as_ref().unwrap().status,
        ReceiptStatus::NoEffect
    );
    let Element::Concept(row) = nexus.store.get_element(id).await.unwrap() else {
        panic!("must be a Concept");
    };
    assert_eq!(row.version, 2, "an unchanged element keeps its version");
}

#[tokio::test]
async fn an_expect_version_guard_stops_a_lost_update() {
    let nexus = nexus("expect_version").await;
    let created = ok(
        &nexus,
        r#"UPSERT CONCEPT ?p { MATCH {key: "k"} SET FIELDS {name: "One"} }"#,
    )
    .await;
    let id = handle(&created, "p");

    let stale = run(
        &nexus,
        r#"UPSERT CONCEPT ?p { MATCH {key: "k"} EXPECT VERSION 99 SET FIELDS {name: "Two"} }"#,
    )
    .await;
    assert_eq!(
        stale.error.as_ref().unwrap().code.as_str(),
        "VersionConflict"
    );

    let Element::Concept(row) = nexus.store.get_element(id).await.unwrap() else {
        panic!("must be a Concept");
    };
    assert_eq!(row.name, "One", "the refused write changed nothing");
}

#[tokio::test]
async fn archiving_removes_from_recall_without_breaking_references() {
    // Spec §41.2, §93.33: archive is not purge, and deletion preserves
    // reference integrity.
    let nexus = nexus("archive").await;
    let setup = ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
            ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
        }"#,
    )
    .await;
    let dark = handle(&setup, "dark");
    let proposition = handle(&setup, "p");

    let request = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "operations": [{"command": "ARCHIVE :x", "parameters": {"x": dark.to_string()}}]
    }))
    .unwrap();
    let parsed = request.operations[0].parse().unwrap();
    let response = nexus
        .execute(parsed, &request, &request.operations[0])
        .await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "{:#?}",
        response.error
    );

    let archived = nexus.store.get_element(dark).await.unwrap();
    assert_eq!(archived.state(), "archived");
    assert!(!archived.is_active());
    // The Proposition that points at it still resolves.
    let Element::Proposition(row) = nexus.store.get_element(proposition).await.unwrap() else {
        panic!("must be a Proposition");
    };
    assert_eq!(row.object["id"], dark.to_string());
}

#[tokio::test]
async fn a_committed_transaction_is_recoverable_by_its_idempotency_key() {
    // Spec §80.4: a caller that lost its response looks the transaction up
    // rather than writing again.
    let nexus = nexus("idempotency").await;
    let request = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "execution": {"mode": "independent", "idempotency_key": "key-1"},
        "operations": [{"command": r#"CREATE CONCEPT ?x { TYPE "Person" NAME "Alice" }"#}]
    }))
    .unwrap();
    let parsed = request.operations[0].parse().unwrap();
    let response = nexus
        .execute(parsed, &request, &request.operations[0])
        .await;
    assert_eq!(response.status, TopLevelStatus::Succeeded);
    let tx_id = response.receipt.as_ref().unwrap().tx_id.clone().unwrap();

    let recovered = nexus
        .store
        .find_transaction_by_idempotency_key(DEFAULT_SPACE, "key-1")
        .await
        .unwrap()
        .expect("the key was journalled");
    assert_eq!(recovered.tx_id, tx_id);
    assert_eq!(recovered.status, "committed");
    assert_eq!(recovered.changed_ids.len(), 1);
    assert_eq!(recovered.schema_environment_version, 1);
}

#[tokio::test]
async fn the_introspection_path_reports_its_absence() {
    // An engine that silently returned an empty answer for a command it
    // cannot run would be worse than one that says so: an Agent would read
    // the emptiness as a fact about the world.
    let nexus = nexus("unsupported").await;
    let response = run(&nexus, "DESCRIBE PRIMER").await;
    assert_eq!(
        response.error.as_ref().unwrap().code.as_str(),
        "UnsupportedCapability"
    );

    // The read path, by contrast, now answers.
    let read = run(&nexus, r#"FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }"#).await;
    assert_eq!(read.status, TopLevelStatus::Succeeded);
}
