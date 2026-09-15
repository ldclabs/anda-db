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

const COGNITIVE_MEMORY: &str = anda_cognitive_nexus::profiles::COGNITIVE_MEMORY;
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
        .insert(PROFILE_ID.to_string(), "2.1.0".to_string());
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

/// The Receipt of a single-operation write.
///
/// §75 puts it on the operation's own result in `sequence` and `independent`
/// execution; the top-level slot is reserved for an `atomic` transaction,
/// which this engine does not run.
fn receipt(response: &anda_kip::Response) -> Option<&anda_kip::Receipt> {
    response.results.first().and_then(|r| r.receipt.as_ref())
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
        tuple.predicate_ref, "kip://profiles/cognitive-memory@2.1.0/prefers",
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
    assert_eq!(receipt(&response).unwrap().status, ReceiptStatus::NoEffect);
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
                TRANSITION :old TO "superseded" BY ?new
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
            "command": "TRANSITION :a1 TO \"superseded\" BY :a2",
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
    // Spec §29: define-before-use. A data mutation never creates schema.
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
    let receipt = receipt(&response).unwrap();
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
            "command": "TRANSITION :a TO \"retracted\"",
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
    // Spec §18.1: a Facet is a validated namespaced extension, not the
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
             MATCH {type: "Person", key: "person:alice"}
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
    assert_eq!(receipt(&response).unwrap().status, ReceiptStatus::NoEffect);
    let Element::Concept(row) = nexus.store.get_element(id).await.unwrap() else {
        panic!("must be a Concept");
    };
    assert_eq!(row.version, 2, "an unchanged element keeps its version");
}

/// `MATCH {type: ...}` used to be parsed and then dropped, which is worse than
/// rejecting it: the upsert created a Concept with an empty `schema_ref`, and
/// `schema_ref` is fixed at creation — so no `{type: ...}` query could ever
/// find it and no later write could repair it.
#[tokio::test]
async fn an_upsert_creates_the_type_its_match_declares() {
    let nexus = nexus("upsert_type").await;
    let created = ok(
        &nexus,
        r#"UPSERT CONCEPT ?p {
             MATCH {type: "Person", key: "person:ada"}
             SET FIELDS {name: "Ada"}
           }"#,
    )
    .await;
    let id = handle(&created, "p");

    let Element::Concept(row) = nexus.store.get_element(id).await.unwrap() else {
        panic!("must be a Concept");
    };
    assert_eq!(
        row.schema_ref,
        "kip://profiles/cognitive-memory@2.1.0/Person"
    );

    // The point of carrying the type: the Concept is reachable by it.
    let found = ok(
        &nexus,
        r#"FIND(?p.name) WHERE { ?p CONCEPT {type: "Person", key: "person:ada"} }"#,
    )
    .await;
    assert_eq!(found, json!(["Ada"]));

    // A Concept whose key nothing else shares, for the bare-key case below.
    let solo = handle(
        &ok(
            &nexus,
            r#"UPSERT CONCEPT ?p {
                 MATCH {type: "Person", key: "person:ada-only"}
                 SET FIELDS {name: "Solo"}
               }"#,
        )
        .await,
        "p",
    );

    // A second upsert resolves the same Concept rather than minting another.
    let again = ok(
        &nexus,
        r#"UPSERT CONCEPT ?p {
             MATCH {type: "Person", key: "person:ada"}
             SET FIELDS {name: "Ada L."}
           }"#,
    )
    .await;
    assert_eq!(handle(&again, "p"), id);

    // §7.3 scopes key uniqueness to (space_id, schema_ref, key), so the same
    // key under another type is a second identity rather than a collision —
    // which is what keeps the 1.x migration of (type, name) identity into a
    // key from merging unrelated Concepts.
    let other = ok(
        &nexus,
        r#"UPSERT CONCEPT ?p {
             MATCH {type: "Preference", key: "person:ada"}
             SET FIELDS {name: "Dark"}
           }"#,
    )
    .await;
    assert_ne!(handle(&other, "p"), id);

    // And now that two Concepts share the key, the key alone no longer names
    // one. Answering with either would be the arbitrary winner §51 forbids for
    // names, reaching the same outcome through `key`.
    let ambiguous = run(
        &nexus,
        r#"UPSERT CONCEPT ?p { MATCH {key: "person:ada"} SET FIELDS {name: "?"} }"#,
    )
    .await;
    assert_eq!(
        ambiguous.error.as_ref().unwrap().code.as_str(),
        "IdentityConflict"
    );

    // An UPSERT with no type cannot create: a Concept whose type nothing can
    // later supply is not something this engine will mint.
    let untyped = run(
        &nexus,
        r#"UPSERT CONCEPT ?p { MATCH {key: "person:grace"} SET FIELDS {name: "Grace"} }"#,
    )
    .await;
    assert_eq!(
        untyped.error.as_ref().unwrap().code.as_str(),
        "SchemaSymbolNotFound"
    );

    // Resolving an unambiguous key still needs no type.
    let resolved = ok(
        &nexus,
        r#"UPSERT CONCEPT ?p { MATCH {key: "person:ada-only"} SET FIELDS {name: "Solo"} }"#,
    )
    .await;
    assert_eq!(handle(&resolved, "p"), solo);

    // An upsert by id may not create, and a type that does not match the id is
    // not a match — reported existence-neutrally either way (§86.4).
    for command in [
        r#"UPSERT CONCEPT ?p { MATCH {id: "C-9999"} SET FIELDS {name: "Nobody"} }"#,
        r#"UPSERT CONCEPT ?p { MATCH {type: "Preference", id: :id} SET FIELDS {name: "Wrong"} }"#,
    ] {
        let request = Request {
            parameters: Some(serde_json::Map::from_iter([(
                "id".to_string(),
                json!(id.to_string()),
            )])),
            ..Request::single(command)
        };
        let parsed = anda_kip::parse_kip(command).unwrap();
        let response = nexus
            .execute(parsed, &request, &request.operations[0])
            .await;
        assert_eq!(
            response.error.as_ref().unwrap().code.as_str(),
            "NotFoundOrNotVisible",
            "{command}"
        );
    }
}

/// §7.3 scopes a logical key's uniqueness to `(space_id, schema_ref, key)`.
/// Both halves matter: two Concepts of one type may not share a key, and two
/// Concepts of different types may.
#[tokio::test]
async fn a_logical_key_is_identity_within_its_type() {
    let nexus = nexus("key_identity").await;
    ok(
        &nexus,
        r#"CREATE CONCEPT ?p { TYPE "Person" NAME "Ada" SET FIELDS {key: "alice"} }"#,
    )
    .await;

    // Same type, same key, second Concept: the key would name two things.
    let clash = run(
        &nexus,
        r#"CREATE CONCEPT ?p { TYPE "Person" NAME "Other" SET FIELDS {key: "alice"} }"#,
    )
    .await;
    assert_eq!(
        clash.error.as_ref().unwrap().code.as_str(),
        "IdentityConflict"
    );

    // Including when both are minted by one transaction, which no store lookup
    // would catch because neither is committed yet.
    let together = run(
        &nexus,
        r#"MUTATE {
             CREATE CONCEPT ?a { TYPE "Preference" NAME "One" SET FIELDS {key: "dark"} }
             CREATE CONCEPT ?b { TYPE "Preference" NAME "Two" SET FIELDS {key: "dark"} }
           }"#,
    )
    .await;
    assert_eq!(
        together.error.as_ref().unwrap().code.as_str(),
        "IdentityConflict"
    );

    // A different type under the same key is a different identity, not a
    // collision — this is what lets 1.x `(type, name)` identity migrate into a
    // key without merging unrelated Concepts.
    ok(
        &nexus,
        r#"CREATE CONCEPT ?p { TYPE "Preference" NAME "Alice" SET FIELDS {key: "alice"} }"#,
    )
    .await;

    // An empty key stores "no logical key" and claims nothing, so any number of
    // Concepts may carry one.
    ok(
        &nexus,
        r#"MUTATE {
             CREATE CONCEPT ?a { TYPE "Person" NAME "Nameless one" }
             CREATE CONCEPT ?b { TYPE "Person" NAME "Nameless two" }
           }"#,
    )
    .await;
}

#[tokio::test]
async fn an_expect_version_guard_stops_a_lost_update() {
    let nexus = nexus("expect_version").await;
    let created = ok(
        &nexus,
        r#"UPSERT CONCEPT ?p { MATCH {type: "Person", key: "k"} SET FIELDS {name: "One"} }"#,
    )
    .await;
    let id = handle(&created, "p");

    let stale = run(
        &nexus,
        r#"UPSERT CONCEPT ?p { MATCH {key: "k"} SET FIELDS {name: "Two"} } EXPECT VERSION 99"#,
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
        "operations": [{"command": "TRANSITION :x TO \"archived\"", "parameters": {"x": dark.to_string()}}]
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
    let tx_id = receipt(&response).unwrap().tx_id.clone().unwrap();

    // The key is journalled under the caller's own scope (§34.2), so the
    // lookup a lost response needs goes through META as the caller.
    let recovered = ok(&nexus, r#"DESCRIBE TRANSACTION BY IDEMPOTENCY KEY "key-1""#).await;
    assert_eq!(recovered["tx_id"], tx_id);
    assert_eq!(recovered["status"], "committed");
    assert_eq!(recovered["changes"].as_array().unwrap().len(), 1);
    // The bare string is not the journal key any more: another Principal
    // reusing it finds nothing of this caller's.
    assert!(
        nexus
            .store
            .find_transaction_by_idempotency_key(DEFAULT_SPACE, "key-1")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn a_resend_under_the_same_key_replays_instead_of_writing_again() {
    // §26, §33: a timeout is not an abort. A client that lost its response
    // resends the same key and gets back the outcome its first attempt
    // produced — the receipt it needs — rather than a second Alice.
    let nexus = nexus("idempotency_resend").await;
    async fn send(nexus: &CognitiveNexus, key: &str) -> anda_kip::Response {
        let request = serde_json::from_value::<Request>(json!({
            "kip": "2.0",
            "execution": {"mode": "independent", "idempotency_key": key},
            "operations": [{"command": r#"CREATE CONCEPT ?x { TYPE "Person" NAME "Alice" }"#}]
        }))
        .unwrap();
        let parsed = request.operations[0].parse().unwrap();
        nexus
            .execute(parsed, &request, &request.operations[0])
            .await
    }

    let first = send(&nexus, "key-1").await;
    assert_eq!(first.status, TopLevelStatus::Succeeded);
    let again = send(&nexus, "key-1").await;
    assert_eq!(again.status, TopLevelStatus::Succeeded);

    // The same receipt, down to the transaction it names: a caller that
    // compares them can tell its write landed.
    let (a, b) = (receipt(&first).unwrap(), receipt(&again).unwrap());
    // The whole receipt, member for member: a replay that reconstructed one
    // field through a second expression is exactly how the two would drift.
    assert_eq!(a, b);
    assert_eq!(first.results[0].result, again.results[0].result);
    // And it says so, because the caller resent precisely to find out.
    assert!(!again.warnings.is_empty());

    // Nothing ran a second time.
    let found = ok(
        &nexus,
        r#"FIND(COUNT(?c)) WHERE { ?c CONCEPT {type: "Person", name: "Alice"} }"#,
    )
    .await;
    assert_eq!(found, json!([1]));

    // A different key is a different write, which is the whole point of the
    // key being the caller's to choose.
    let other = send(&nexus, "key-2").await;
    assert_eq!(other.status, TopLevelStatus::Succeeded);
    assert_ne!(receipt(&other).unwrap().tx_id, a.tx_id);
    let found = ok(
        &nexus,
        r#"FIND(COUNT(?c)) WHERE { ?c CONCEPT {type: "Person", name: "Alice"} }"#,
    )
    .await;
    assert_eq!(found, json!([2]));
}

#[tokio::test]
async fn all_three_command_families_reach_this_engine() {
    // The Executor dispatches on what the command *is*, so a caller does not
    // need to know which family it wrote.
    let nexus = nexus("families").await;
    for command in [
        r#"CREATE CONCEPT ?x { TYPE "Person" NAME "Alice" }"#,
        r#"FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }"#,
        "DESCRIBE PRIMER",
    ] {
        let response = run(&nexus, command).await;
        assert_eq!(
            response.status,
            TopLevelStatus::Succeeded,
            "{command}\n{:#?}",
            response.error
        );
    }
}

// ---------------------------------------------------------------------------
// PURGE PAYLOAD (§60.6)
// ---------------------------------------------------------------------------

/// A Space holding one Evidence record with an inline payload, cited by an
/// Assertion — the shape a payload purge has to survive intact.
async fn with_cited_evidence(name: &str) -> CognitiveNexus {
    let nexus = nexus(name).await;
    ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark mode" }
            ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
            CREATE EVIDENCE ?e {
                SET FIELDS {
                    evidence_class: "user_statement",
                    payload: "I prefer dark mode, and my address is 12 Elm Street.",
                    content_digest: "sha3-256:d1ge5t",
                    media_type: "text/plain",
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
    nexus
}

async fn evidence_view(nexus: &CognitiveNexus, id: &str) -> Json {
    let element = nexus
        .store
        .get_element(id.parse::<ElementId>().unwrap())
        .await
        .unwrap();
    anda_cognitive_nexus::view::render(&element)
}

#[tokio::test]
async fn a_payload_purge_destroys_the_bytes_and_keeps_the_evidence() {
    // §60.6: the data-minimization instrument. A Space can discard observed raw
    // bytes after digesting them without destroying the evidence event, its
    // citations, or its provenance role — which is exactly what makes it usable
    // where element purge is not.
    let nexus = with_cited_evidence("purge_payload").await;
    let before = evidence_view(&nexus, "E-1").await;
    let digest = before["content_digest"].as_str().unwrap().to_string();
    assert_eq!(
        before["payload"]["inline"],
        "I prefer dark mode, and my address is 12 Elm Street."
    );

    let response = run(&nexus, r#"PURGE PAYLOAD "E-1" CONFIRM "PURGE""#).await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "{:#?}",
        response.error
    );

    let after = evidence_view(&nexus, "E-1").await;
    // Gone: the bytes, and nothing but the bytes.
    assert_eq!(after["payload"]["mode"], "purged");
    assert_eq!(after["payload"]["inline"], Json::Null);
    assert_eq!(after["payload"]["content_ref"], Json::Null);
    // Kept: everything §60.6 lists, so corroboration grouping and independence
    // counting keep operating on the surviving digest and provenance (§23).
    assert_eq!(after["content_digest"], json!(digest));
    assert_eq!(after["evidence_class"], "user_statement");
    assert_eq!(after["media_type"], "text/plain");
    assert_eq!(after["observed_at"], "2026-08-16T09:00:00.000Z");
    assert_eq!(after["_system"]["state"], "active", "the record survives");

    // The search index is the other copy of the payload. A purge that left it
    // behind would keep the bytes retrievable by the very words the caller was
    // minimizing away.
    let found = ok(&nexus, r#"SEARCH EVIDENCE "Elm""#).await;
    assert!(
        found["hits"].as_array().unwrap().is_empty(),
        "the purged payload must leave the search index: {found:#?}"
    );

    // The change stream names it `payload_purge`, not `purge`: a follower that
    // could not tell the two apart would read a data-minimization decision as
    // the loss of the record (§36).
    let history = ok(&nexus, r#"HISTORY ELEMENT "E-1""#).await;
    let ops: Vec<&str> = history
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|entry| entry["changes"].as_array().unwrap())
        .filter_map(|change| change["op"].as_str())
        .collect();
    assert_eq!(ops, vec!["create", "payload_purge"], "{history:#?}");

    // The citation still resolves: an Assertion whose Evidence went to a stub
    // would be a history pointing at nothing, which is the failure element
    // purge exists to refuse and this operation never risks.
    let cited = ok(
        &nexus,
        r#"FIND(?a.evidence) WHERE { ?a ASSERTION {id: "A-1"} }"#,
    )
    .await;
    assert_eq!(cited[0][0]["id"], "E-1", "{cited:#?}");
}

#[tokio::test]
async fn a_payload_purge_reaches_the_version_log() {
    // The half that is easy to forget and fatal to skip: every commit appends
    // the whole row it wrote, so a payload cleared only in the current row
    // stays fully readable through `AS OF`.
    let nexus = with_cited_evidence("purge_payload_history").await;
    let seq_before = ok(&nexus, "DESCRIBE SNAPSHOT").await["space_seq"]
        .as_u64()
        .unwrap();
    ok(&nexus, r#"PURGE PAYLOAD "E-1" CONFIRM "PURGE""#).await;

    let historical = ok(
        &nexus,
        &format!(
            r#"FIND(?e.payload) WHERE {{ ?e EVIDENCE {{id: "E-1"}} }} AS OF SEQ {seq_before}"#
        ),
    )
    .await;
    let payload = &historical[0];
    assert_eq!(
        payload["mode"], "purged",
        "a past coordinate must not hand back the bytes: {historical:#?}"
    );
    assert_eq!(payload["inline"], Json::Null);
}

#[tokio::test]
async fn purging_an_already_purged_payload_is_a_no_effect() {
    // §60.6 states it outright, and it matters for a retry: a sweep that ran
    // twice must not burn a second version or emit a second change record.
    let nexus = with_cited_evidence("purge_payload_twice").await;
    ok(&nexus, r#"PURGE PAYLOAD "E-1" CONFIRM "PURGE""#).await;
    let version = evidence_view(&nexus, "E-1").await["_system"]["version"]
        .as_u64()
        .unwrap();

    let again = run(&nexus, r#"PURGE PAYLOAD "E-1" CONFIRM "PURGE""#).await;
    assert_eq!(again.status, TopLevelStatus::Succeeded);
    assert_eq!(
        receipt(&again).map(|r| r.status),
        Some(ReceiptStatus::NoEffect)
    );
    assert_eq!(
        evidence_view(&nexus, "E-1").await["_system"]["version"]
            .as_u64()
            .unwrap(),
        version,
        "a repeat purge must not burn a version"
    );
}

#[tokio::test]
async fn only_evidence_has_a_payload_to_purge() {
    // §60.6: other kinds have no payload. Succeeding vacuously over a set of
    // Concepts would read as "the bytes are gone" when nothing was there.
    let nexus = with_cited_evidence("purge_payload_kind").await;
    let response = run(&nexus, r#"PURGE PAYLOAD "C-1" CONFIRM "PURGE""#).await;
    assert_eq!(response.status, TopLevelStatus::Failed);
    assert_eq!(
        response.error.as_ref().unwrap().code.as_str(),
        "ConstraintViolation"
    );

    // And the refusal erased nothing on the way to refusing.
    assert_eq!(
        evidence_view(&nexus, "E-1").await["payload"]["mode"],
        "inline"
    );
}

#[tokio::test]
async fn a_legal_hold_blocks_a_payload_purge_exactly_as_it_blocks_an_element_purge() {
    // §60.6: a hold is most often placed precisely to preserve the bytes.
    let nexus = with_cited_evidence("purge_payload_hold").await;
    ok(
        &nexus,
        r#"SET RETENTION "E-1" {retention_class: "standard", legal_hold: true}"#,
    )
    .await;
    let response = run(&nexus, r#"PURGE PAYLOAD "E-1" CONFIRM "PURGE""#).await;
    assert_eq!(response.status, TopLevelStatus::Failed);
    assert_eq!(
        response.error.as_ref().unwrap().code.as_str(),
        "LegalHoldConflict"
    );
}

// ---------------------------------------------------------------------------
// §52.5 — one lifecycle statement
// ---------------------------------------------------------------------------

/// A Space with one Experience, one Assertion about Alice, and one Activity.
async fn lifecycle_ground(name: &str) -> CognitiveNexus {
    let nexus = nexus(name).await;
    ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
            CREATE CONCEPT ?exp { TYPE "Experience" NAME "First"
              SET ATTRIBUTES {goal: "learn", outcome_status: "success"} }
            ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
            CREATE ASSERTION ?a {
                SET FIELDS {proposition: ?p, asserted_by: ?alice, stance: "support", mode: "stated", confidence: 0.7}
            }
            CREATE ACTIVITY ?run { SET FIELDS {activity_class: "consolidation"} }
        }"#,
    )
    .await;
    nexus
}

/// §52.5: the quoted state names the move, and the engine validates it against
/// the target's *kind*. Six statements collapsed into one, so the check that
/// used to be carried by the statement's own name — only an Assertion can be
/// retracted, only an Activity can complete — is now the engine's, and it has
/// to be made where the element is loaded rather than where it is parsed.
#[tokio::test]
async fn a_state_that_does_not_fit_the_target_kind_is_an_illegal_move() {
    let nexus = lifecycle_ground("transition_kind").await;

    // A Concept has no claim to withdraw.
    let refused = run(&nexus, r#"TRANSITION "C-1" TO "retracted""#).await;
    assert_eq!(
        refused.error.as_ref().unwrap().code.as_str(),
        "InvalidLifecycleTransition"
    );
    // §35.3 removed EXPECT STATE precisely because this check exists, so the
    // refusal has to carry what a guard would have compared.
    let details = refused.error.as_ref().unwrap().details.as_ref().unwrap();
    assert_eq!(details["from"], "active");
    assert_eq!(details["to"], "retracted");

    // And an Assertion is not an Activity.
    let refused = run(&nexus, r#"TRANSITION "A-1" TO "running""#).await;
    assert_eq!(
        refused.error.as_ref().unwrap().code.as_str(),
        "InvalidLifecycleTransition"
    );
    assert_eq!(
        refused.error.as_ref().unwrap().details.as_ref().unwrap()["to"],
        "running"
    );
}

/// §52.5: a move to the state the target already holds is `no_effect`, not an
/// error and not a second write. A sweep that runs twice — which is what a
/// resend or a retried maintenance job is — must not burn a version.
#[tokio::test]
async fn a_move_to_the_state_already_held_changes_nothing() {
    let nexus = lifecycle_ground("transition_same_state").await;

    let first = run(&nexus, r#"TRANSITION "C-1" TO "archived""#).await;
    assert_eq!(
        receipt(&first).unwrap().status,
        anda_kip::ReceiptStatus::Committed
    );

    let again = run(&nexus, r#"TRANSITION "C-1" TO "archived""#).await;
    assert_eq!(again.status, TopLevelStatus::Succeeded);
    assert_eq!(
        receipt(&again).unwrap().status,
        anda_kip::ReceiptStatus::NoEffect
    );

    let Element::Concept(row) = nexus
        .store
        .get_element("C-1".parse().unwrap())
        .await
        .unwrap()
    else {
        panic!("must be a Concept");
    };
    assert_eq!(row.version, 2, "the replay must not advance the version");
}

/// §52.5: `archived` and `tombstoned` are the only states with a legal path
/// between them, and it runs one way. Tombstoning is logical deletion, so a
/// Space cannot walk an element back out of it by archiving it again.
#[tokio::test]
async fn tombstoning_is_reachable_from_archived_but_not_the_other_way() {
    let nexus = lifecycle_ground("transition_order").await;

    ok(&nexus, r#"TRANSITION "C-1" TO "archived""#).await;
    ok(&nexus, r#"TRANSITION "C-1" TO "tombstoned""#).await;

    let refused = run(&nexus, r#"TRANSITION "C-1" TO "archived""#).await;
    assert_eq!(
        refused.error.as_ref().unwrap().code.as_str(),
        "InvalidLifecycleTransition"
    );
    let details = refused.error.as_ref().unwrap().details.as_ref().unwrap();
    assert_eq!(details["from"], "tombstoned");
    assert_eq!(details["to"], "archived");
}

/// §52.5, §60.1: leaving ordinary recall starts from a state that still holds
/// it.
///
/// A merged-away identity, a quarantined element and a purged stub are engine
/// states with their own exits, and archiving out of one would overwrite the
/// reason the element is where it is. An Activity that has left recall has no
/// lifecycle left to move either: finalizing it would write provenance into an
/// element a reader is no longer meant to reach. The second reference engine
/// refuses the same moves with the same `details`.
#[tokio::test]
async fn removal_starts_from_a_state_that_still_holds_the_element() {
    let nexus = lifecycle_ground("transition_removal_legality").await;
    // A merge requires compatible type lineages; this lifecycle test needs a
    // second Person rather than the Preference in the common setup.
    let duplicate = ok(&nexus, r#"CREATE CONCEPT ?duplicate { TYPE "Person" }"#).await;
    ok(
        &nexus,
        &format!(
            r#"MERGE CONCEPT "C-1" INTO "{}""#,
            handle(&duplicate, "duplicate")
        ),
    )
    .await;
    for state in ["tombstoned", "archived"] {
        let refused = run(&nexus, &format!(r#"TRANSITION "C-1" TO "{state}""#)).await;
        let error = refused.error.as_ref().unwrap();
        assert_eq!(error.code.as_str(), "InvalidLifecycleTransition", "{state}");
        let details = error.details.as_ref().unwrap();
        assert_eq!(details["from"], "merged");
        assert_eq!(details["to"], state);
    }

    ok(&nexus, r#"TRANSITION "X-1" TO "archived""#).await;
    let frozen = run(&nexus, r#"TRANSITION "X-1" TO "running""#).await;
    let error = frozen.error.as_ref().unwrap();
    assert_eq!(error.code.as_str(), "InvalidLifecycleTransition");
    let details = error.details.as_ref().unwrap();
    assert_eq!(details["from"], "archived");
    assert_eq!(details["to"], "running");
}

/// §52.5: `BY` belongs to exactly two states, and `SET FIELDS` / `SET
/// STRUCTURAL` to the Activity states. These are syntax errors rather than
/// runtime refusals, because the grammar can see them: a statement whose
/// clauses do not go with its state means something the engine has no reading
/// for, and guessing which half the author meant is how a lifecycle move
/// silently becomes a different one.
#[tokio::test]
async fn clauses_that_do_not_belong_to_the_state_are_refused_by_the_grammar() {
    for command in [
        r#"TRANSITION "A-1" TO "archived" BY "A-2""#,
        r#"TRANSITION "A-1" TO "retracted" BY "A-2""#,
        r#"TRANSITION "A-1" TO "superseded""#,
        r#"TRANSITION "E-1" TO "corrected""#,
        r#"TRANSITION "C-1" TO "archived" SET FIELDS {name: "X"}"#,
        r#"TRANSITION "A-1" TO "retracted" SET STRUCTURAL { ("evidence", "E-1") {} }"#,
    ] {
        let refused = anda_kip::parse_kip(command)
            .expect_err("a clause that does not go with the state is a syntax error");
        assert_eq!(
            refused.code,
            anda_kip::KipErrorCode::InvalidSyntax,
            "{command}: {refused}"
        );
    }

    // The states themselves are a closed registry, so a plausible-looking word
    // that is not one of the nine is refused rather than stored. That refusal
    // is a `ConstraintViolation` and not a syntax error: the statement is
    // well-formed, and what it violates is a Core registry — the same answer
    // every other closed vocabulary gives, so a client handles them alike.
    let refused = anda_kip::parse_kip(r#"TRANSITION "C-1" TO "deleted""#)
        .expect_err("the state vocabulary is closed");
    assert_eq!(refused.code, anda_kip::KipErrorCode::ConstraintViolation);
}

/// §52.5: an Activity finalizes its terminal fields and its provenance
/// topology in the same statement that moves it, because §16.6 freezes that
/// topology once the Activity is terminal — a second statement would arrive
/// too late to write what the first one made immutable.
#[tokio::test]
async fn an_activity_finalizes_its_fields_in_the_statement_that_completes_it() {
    let nexus = lifecycle_ground("transition_activity").await;

    ok(&nexus, r#"TRANSITION "X-1" TO "running""#).await;
    ok(
        &nexus,
        r#"TRANSITION "X-1" TO "completed"
             SET FIELDS {ended_at: "2026-01-01T00:00:00Z"}
             SET STRUCTURAL { ("outputs", "C-3") {} }"#,
    )
    .await;

    let finalized = ok(
        &nexus,
        r#"FIND(?x.status, ?x.ended_at) WHERE { ?x ACTIVITY {} }"#,
    )
    .await;
    assert_eq!(
        finalized.as_array().unwrap()[0],
        json!(["completed", "2026-01-01T00:00:00.000Z"])
    );

    // §16.6: the topology is frozen now, so a second terminal move is refused
    // by the rule that protects it rather than by the same-state shortcut.
    let refused = run(&nexus, r#"TRANSITION "X-1" TO "failed""#).await;
    assert_eq!(
        refused.error.as_ref().unwrap().code.as_str(),
        "ActivityTerminal"
    );
}

// ---------------------------------------------------------------------------
// §6.3, §35.1 — version planes
// ---------------------------------------------------------------------------

/// §6.3: `_system.version` advances on every committed change; a plane counter
/// advances only when its own plane changes. That difference is the whole
/// point of planes, so it is readable from the element rather than only
/// inferable from which guards happen to pass.
#[tokio::test]
async fn a_plane_counter_advances_only_when_its_own_plane_changes() {
    let nexus = lifecycle_ground("planes_counters").await;

    async fn planes(nexus: &CognitiveNexus) -> Json {
        ok(
            nexus,
            r#"FIND(?c._system.version, ?c._system.plane_versions)
               WHERE { ?c CONCEPT {name: "First"} }"#,
        )
        .await
    }

    // Created with attributes and nothing else: §35.2 wants `EXPECT VERSION 0
    // OF <plane>` to read as "never written" from the first version on.
    let start = planes(&nexus).await;
    assert_eq!(start.as_array().unwrap()[0][0], json!(1));
    assert_eq!(start.as_array().unwrap()[0][1]["attributes"], json!(1));
    assert_eq!(start.as_array().unwrap()[0][1]["structural"], json!(0));
    assert_eq!(start.as_array().unwrap()[0][1]["retention"], json!(0));

    // A Facet write moves the element and that Facet's counter, and leaves the
    // attributes counter exactly where it was.
    ok(
        &nexus,
        r#"UPDATE ?c SET FACET "MnemonicState" {salience: 0.5}
           WHERE { ?c CONCEPT {name: "First"} }"#,
    )
    .await;
    let after = planes(&nexus).await;
    assert_eq!(after.as_array().unwrap()[0][0], json!(2));
    assert_eq!(after.as_array().unwrap()[0][1]["attributes"], json!(1));
    assert_eq!(
        after.as_array().unwrap()[0][1]["facets"]["MnemonicState"],
        json!(1)
    );
}

/// §35.1: a guard on one plane is not spoiled by a concurrent write to
/// another. This is the case the section is written for — a `MnemonicState`
/// decay sweep and a status verdict on the same element, neither invalidating
/// the other — and it is exactly what a bare `EXPECT VERSION` cannot express.
#[tokio::test]
async fn a_plane_guard_survives_a_write_to_a_different_plane() {
    let nexus = lifecycle_ground("planes_guard").await;

    // The decay sweep lands first, moving `_system.version` to 2.
    ok(
        &nexus,
        r#"UPDATE ?c SET FACET "MnemonicState" {memory_strength: 0.4}
           WHERE { ?c CONCEPT {name: "First"} }"#,
    )
    .await;

    // The verdict was planned against attributes version 1 and still commits,
    // because that is the counter it guarded.
    let verdict = run(
        &nexus,
        r#"UPDATE ?c SET ATTRIBUTES {outcome_status: "partial"}
           WHERE { ?c CONCEPT {name: "First"} }
           EXPECT VERSION 1 OF ATTRIBUTES"#,
    )
    .await;
    assert_eq!(
        verdict.status,
        TopLevelStatus::Succeeded,
        "{:#?}",
        verdict.error
    );

    // The bare guard is the one that would have been spoiled: it compares
    // `_system.version`, which every write moves.
    let spoiled = run(
        &nexus,
        r#"UPDATE ?c SET ATTRIBUTES {outcome_status: "failure"}
           WHERE { ?c CONCEPT {name: "First"} }
           EXPECT VERSION 1"#,
    )
    .await;
    assert_eq!(
        spoiled.error.as_ref().unwrap().code.as_str(),
        "VersionConflict"
    );
}

/// §35.1: a mismatch names the plane that mismatched, so a caller that sent
/// several guards learns which precondition it lost rather than only that it
/// lost one.
#[tokio::test]
async fn a_plane_mismatch_names_the_plane_it_mismatched_on() {
    let nexus = lifecycle_ground("planes_details").await;
    ok(
        &nexus,
        r#"UPDATE ?c SET FACET "MnemonicState" {salience: 0.5}
           WHERE { ?c CONCEPT {name: "First"} }"#,
    )
    .await;

    for (guard, plane) in [
        ("EXPECT VERSION 9 OF ATTRIBUTES", "attributes"),
        ("EXPECT VERSION 9 OF STRUCTURAL", "structural"),
        ("EXPECT VERSION 9 OF RETENTION", "retention"),
        (
            r#"EXPECT VERSION 9 OF FACET "MnemonicState""#,
            "facets.MnemonicState",
        ),
    ] {
        let refused = run(
            &nexus,
            &format!(
                r#"UPDATE ?c SET ATTRIBUTES {{outcome_status: "partial"}}
                   WHERE {{ ?c CONCEPT {{name: "First"}} }}
                   {guard}"#
            ),
        )
        .await;
        let error = refused.error.as_ref().unwrap();
        assert_eq!(error.code.as_str(), "VersionConflict", "{guard}");
        assert_eq!(
            error.details.as_ref().unwrap()["plane"],
            json!(plane),
            "{guard}"
        );
    }

    // A bare guard names no plane: it guarded the whole element, and reporting
    // one would say the conflict was narrower than it was.
    let refused = run(
        &nexus,
        r#"UPDATE ?c SET ATTRIBUTES {outcome_status: "partial"}
           WHERE { ?c CONCEPT {name: "First"} }
           EXPECT VERSION 9"#,
    )
    .await;
    let error = refused.error.as_ref().unwrap();
    assert_eq!(error.code.as_str(), "VersionConflict");
    assert!(
        error
            .details
            .as_ref()
            .is_none_or(|details| details.get("plane").is_none())
    );
}

/// §35.1: guards may repeat, one per plane, and naming the same plane twice is
/// a syntax error — two counters cannot both be the one true expectation, and
/// silently keeping the last would make the first guard decorative.
#[tokio::test]
async fn guards_repeat_once_per_plane_and_never_twice_on_one() {
    let both = anda_kip::parse_kip(
        r#"UPDATE "C-1" SET ATTRIBUTES {goal: "x"}
           EXPECT VERSION 1 OF ATTRIBUTES EXPECT VERSION 0 OF STRUCTURAL"#,
    );
    assert!(both.is_ok(), "{both:?}");

    for command in [
        r#"UPDATE "C-1" SET ATTRIBUTES {goal: "x"}
           EXPECT VERSION 1 OF ATTRIBUTES EXPECT VERSION 2 OF ATTRIBUTES"#,
        r#"UPDATE "C-1" SET ATTRIBUTES {goal: "x"}
           EXPECT VERSION 1 EXPECT VERSION 2"#,
        r#"UPDATE "C-1" SET ATTRIBUTES {goal: "x"}
           EXPECT VERSION 1 OF FACET "MnemonicState"
           EXPECT VERSION 2 OF FACET "MnemonicState""#,
    ] {
        let refused = anda_kip::parse_kip(command).expect_err("one plane may be guarded only once");
        assert_eq!(
            refused.code,
            anda_kip::KipErrorCode::InvalidSyntax,
            "{command}"
        );
    }
}

/// §35.2: only the *bare* `EXPECT VERSION 0` is the create-only guard. The
/// plane form at 0 is an ordinary guard saying that plane has never been
/// written, which is a different claim — and reading the two as one would make
/// every never-written plane assert that the element does not exist.
#[tokio::test]
async fn a_plane_guard_at_zero_is_not_the_create_only_guard() {
    let nexus = lifecycle_ground("planes_zero").await;

    // "First" exists and has never had a structural reference written.
    let allowed = run(
        &nexus,
        r#"UPDATE ?c SET ATTRIBUTES {outcome_status: "partial"}
           WHERE { ?c CONCEPT {name: "First"} }
           EXPECT VERSION 0 OF STRUCTURAL"#,
    )
    .await;
    assert_eq!(
        allowed.status,
        TopLevelStatus::Succeeded,
        "{:#?}",
        allowed.error
    );

    // The bare form at 0 says the addressed identity must not already exist.
    ok(
        &nexus,
        r#"CREATE CONCEPT ?p { TYPE "Person" NAME "Bob" SET FIELDS {key: "person:bob"} }"#,
    )
    .await;
    let refused = run(
        &nexus,
        r#"UPSERT CONCEPT ?c { MATCH {type: "Person", key: "person:bob"} SET FIELDS {name: "Robert"} }
           EXPECT VERSION 0"#,
    )
    .await;
    assert_eq!(
        refused.error.as_ref().unwrap().code.as_str(),
        "VersionConflict"
    );

    // The same statement against a key nobody holds is a creation, which is
    // what the create-only guard is for.
    let created = run(
        &nexus,
        r#"UPSERT CONCEPT ?c { MATCH {type: "Person", key: "person:carol"} SET FIELDS {name: "Carol"} }
           EXPECT VERSION 0"#,
    )
    .await;
    assert_eq!(
        created.status,
        TopLevelStatus::Succeeded,
        "{:#?}",
        created.error
    );
}

// ---------------------------------------------------------------------------
// §36.1 — the Change Envelope entry
// ---------------------------------------------------------------------------

/// §36.1: one commit yields one envelope, and every entry carries the members
/// the section makes normative. This is the shape a Watch reads to decide
/// whether a slot, an element or a type moved, so it is asserted member by
/// member rather than by spot-check — and asserted to carry names only, never
/// values, because a follower that may not read the element must still be able
/// to receive the entry.
#[tokio::test]
async fn a_change_entry_carries_the_members_36_1_makes_normative() {
    let nexus = lifecycle_ground("envelope_shape").await;

    // An `update` entry: versions before and after, the paths that changed,
    // and the plane counters after the commit.
    ok(
        &nexus,
        r#"UPDATE ?c SET ATTRIBUTES {outcome_status: "partial"}
                    SET FACET "MnemonicState" {salience: 0.25}
           WHERE { ?c CONCEPT {name: "First"} }"#,
    )
    .await;
    let changes = ok(&nexus, r#"HISTORY ELEMENT "C-3""#).await;
    let entry = changes.as_array().unwrap().last().unwrap()["changes"]
        .as_array()
        .unwrap()[0]
        .clone();
    assert_eq!(entry["op"], "update");
    assert_eq!(entry["kind"], "concept");
    assert_eq!(entry["id"], "C-3");
    assert_eq!(entry["old_version"], json!(1));
    assert_eq!(entry["new_version"], json!(2));
    assert!(
        entry["schema_ref"]
            .as_str()
            .unwrap()
            .ends_with("/Experience"),
        "a Concept entry names the type it was written against: {entry}"
    );
    assert_eq!(
        entry["touched"],
        json!(["attributes.outcome_status", "facets.MnemonicState"])
    );
    assert_eq!(entry["planes"]["attributes"], json!(2));
    assert_eq!(entry["planes"]["facets"]["MnemonicState"], json!(1));
    // Names, never values: the entry says the slot moved, not what to.
    assert!(!entry.to_string().contains("partial"));
    assert!(!entry.to_string().contains("0.25"));

    // A `lifecycle` entry: the move is `state {from, to}`, and an Assertion
    // entry names the Proposition it is about so a follower can find the slot
    // whose belief just changed without reading the Assertion.
    ok(&nexus, r#"TRANSITION "A-1" TO "retracted""#).await;
    let changes = ok(&nexus, r#"HISTORY ELEMENT "A-1""#).await;
    let entry = changes.as_array().unwrap().last().unwrap()["changes"]
        .as_array()
        .unwrap()[0]
        .clone();
    assert_eq!(entry["op"], "lifecycle");
    assert_eq!(entry["kind"], "assertion");
    assert_eq!(entry["state"], json!({"from": "active", "to": "retracted"}));
    assert_eq!(entry["refs"]["proposition"], "P-1");
    // `state` and `touched` answer different questions, so both are there: a
    // consumer watching the belief slot reads the move, and one watching named
    // paths reads the columns it moved. Neither is a plane, though — a
    // lifecycle move advances `_system.version` and no counter — so the entry
    // reports no `planes` at all.
    assert_eq!(
        entry["touched"],
        json!(["fields.retracted_at", "fields.status"])
    );
    assert!(
        entry["planes"].is_null(),
        "a lifecycle move touches no plane"
    );

    // A `create` entry: no `old_version`, and a Proposition entry names its
    // subject and the predicate it was resolved against.
    let changes = ok(&nexus, r#"HISTORY ELEMENT "P-1""#).await;
    let entry = changes.as_array().unwrap()[0]["changes"]
        .as_array()
        .unwrap()[0]
        .clone();
    assert_eq!(entry["op"], "create");
    assert_eq!(entry["kind"], "proposition");
    assert_eq!(entry["new_version"], json!(1));
    assert!(entry["old_version"].is_null());
    assert_eq!(entry["refs"]["subject"], "C-1");
    assert!(
        entry["refs"]["predicate_ref"]
            .as_str()
            .unwrap()
            .ends_with("/prefers")
    );
}

/// §36.1, §36.2: one state-changing commit is one envelope, and everything in
/// it is one cognitive transition. A `MUTATE` block that writes four elements
/// must therefore arrive as four entries under one `space_seq`, not as four
/// envelopes a consumer would have to reassemble.
#[tokio::test]
async fn one_commit_is_one_envelope_however_many_elements_it_touched() {
    let nexus = nexus("envelope_atomicity").await;
    ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
            ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
            CREATE ASSERTION ?a {
                SET FIELDS {proposition: ?p, asserted_by: ?alice, stance: "support", mode: "stated"}
            }
        }"#,
    )
    .await;

    let envelopes = ok(&nexus, "CHANGES AFTER SEQ 0").await;
    let envelopes = envelopes.as_array().unwrap();
    assert_eq!(envelopes.len(), 1, "{envelopes:#?}");
    let envelope = &envelopes[0];
    assert_eq!(envelope["space_seq"], json!(1));
    assert!(envelope["tx_id"].is_string());
    assert!(envelope["committed_at"].is_string());
    assert_eq!(envelope["transaction_class"], "cognitive");

    // §36.3: the deduplication key a consumer is promised.
    assert!(envelope["space_id"].is_string());

    // Four elements, four entries. They are ordered by element id rather than
    // by the order the clauses ran: §36.2 makes the envelope one transition,
    // so nothing inside it happened before anything else, and a stable order
    // is worth more to a consumer than a re-run of the plan.
    let ids: Vec<&str> = envelope["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|change| change["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["A-1", "C-1", "C-2", "P-1"]);
    assert!(
        envelope["changes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|change| change["op"] == "create")
    );
}
