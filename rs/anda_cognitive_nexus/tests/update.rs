//! End-to-end tests for the mutation forms that select what they act on:
//! `UPDATE`, `MERGE CONCEPT`, and the `WHERE`-bearing removal and retention
//! families.
//!
//! Everything goes through the real parser and a real database, because the
//! interesting failures here are exactly the ones a hand-built AST hides: what
//! a selection block binds, what it does *not* see, and what the engine refuses
//! once it knows the element kind the parser could only guess at.

use anda_cognitive_nexus::{
    CognitiveNexus, Element,
    id::ElementId,
    nexus::DEFAULT_SPACE,
    profiles::COGNITIVE_MEMORY,
    schema::{PackageState, SchemaLock, SchemaPackage},
};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{Executor, Json, ReceiptStatus, Request, TopLevelStatus};
use object_store::memory::InMemory;
use serde_json::json;
use std::sync::Arc;

const PROFILE_ID: &str = "kip://profiles/cognitive-memory";

async fn nexus(name: &str) -> CognitiveNexus {
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: name.to_string(),
            description: "update tests".to_string(),
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

async fn run(nexus: &CognitiveNexus, command: &str) -> anda_kip::Response {
    let request = Request::single(command);
    let parsed = anda_kip::parse_kip(command).unwrap_or_else(|err| panic!("{command}\n{err}"));
    nexus
        .execute(parsed, &request, &request.operations[0])
        .await
}

async fn ok(nexus: &CognitiveNexus, command: &str) -> Json {
    let response = run(nexus, command).await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "{command}\n{:#?}",
        response.results
    );
    response.first_result().cloned().unwrap_or(Json::Null)
}

async fn err(nexus: &CognitiveNexus, command: &str) -> anda_kip::ErrorObject {
    let response = run(nexus, command).await;
    response
        .results
        .into_iter()
        .find_map(|result| result.error)
        .or(response.error)
        .unwrap_or_else(|| panic!("{command} was expected to fail"))
}

fn handle(result: &Json, name: &str) -> ElementId {
    result["handles"][name]
        .as_str()
        .unwrap_or_else(|| panic!("no handle ?{name} in {result}"))
        .parse()
        .unwrap()
}

fn rows(result: &Json) -> &Vec<Json> {
    result.as_array().expect("a KQL result is an array")
}

/// Three Experiences with a MnemonicState, one Person, one claim about them.
async fn seeded(name: &str) -> (CognitiveNexus, Json) {
    let nexus = nexus(name).await;
    let created = ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?e1 {
                TYPE "Experience" NAME "First"
                SET ATTRIBUTES {goal: "learn", outcome_status: "success"}
                SET FACET "MnemonicState" {memory_strength: 0.8, salience: 0.5}
            }
            CREATE CONCEPT ?e2 {
                TYPE "Experience" NAME "Second"
                SET ATTRIBUTES {goal: "learn", outcome_status: "failure"}
                SET FACET "MnemonicState" {memory_strength: 0.4, salience: 0.5}
            }
            CREATE CONCEPT ?e3 {
                TYPE "Experience" NAME "Third"
                SET ATTRIBUTES {goal: "rest", outcome_status: "success"}
                SET FACET "MnemonicState" {memory_strength: 0.2, salience: 0.5}
            }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark mode" }
            ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
            CREATE EVIDENCE ?ev {
                SET FIELDS {evidence_class: "user_statement", payload: "I prefer dark mode."}
            }
            CREATE ASSERTION ?a {
                SET FIELDS {
                    proposition: ?p, asserted_by: ?alice, stance: "support",
                    mode: "stated", confidence: 0.9
                }
                SET STRUCTURAL { ("evidence", ?ev) {role: "support"} }
            }
        }"#,
    )
    .await;
    (nexus, created)
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn update_reaches_a_directly_named_concept() {
    let (nexus, created) = seeded("update_direct").await;
    let alice = handle(&created, "alice");

    let request = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "operations": [{
            "command": r#"UPDATE :who
                          SET FIELDS {name: "Alice A."}
                          SET ATTRIBUTES {display_name: "Alice A.", description: "renamed"}"#,
            "parameters": {"who": alice.to_string()}
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
        response.results
    );

    let result = ok(
        &nexus,
        r#"FIND(?c.name, ?c.attributes.display_name) WHERE { ?c CONCEPT {type: "Person"} }"#,
    )
    .await;
    assert_eq!(rows(&result), &vec![json!(["Alice A.", "Alice A."])]);
}

/// The metabolism path: decay a Facet member by reading its own current value.
/// `LIMIT` bounds the sweep, and the order it cuts is documented (ascending
/// element id), so the same sweep twice takes the same elements.
#[tokio::test]
async fn update_sweeps_a_facet_through_an_expression_bounded_by_limit() {
    let (nexus, _) = seeded("update_sweep").await;

    ok(
        &nexus,
        r#"UPDATE ?m
           SET FACET "MnemonicState" {
             memory_strength: MUL(?m.facets["MnemonicState"].memory_strength, 0.5)
           }
           WHERE { ?m CONCEPT {type: "Experience"} }
           LIMIT 2"#,
    )
    .await;

    let result = ok(
        &nexus,
        r#"FIND(?m.name, ?m.facets["MnemonicState"].memory_strength)
           WHERE { ?m CONCEPT {type: "Experience"} }
           ORDER BY ?m.name"#,
    )
    .await;
    // The two lowest ids are the two oldest Experiences: First and Second.
    assert_eq!(
        rows(&result),
        &vec![
            json!(["First", 0.4]),
            json!(["Second", 0.2]),
            json!(["Third", 0.2]),
        ]
    );

    // The other Facet member is untouched: a Facet assignment merges rather
    // than replacing the Facet.
    let salience = ok(
        &nexus,
        r#"FIND(?m.facets["MnemonicState"].salience) WHERE { ?m CONCEPT {name: "First"} }"#,
    )
    .await;
    assert_eq!(rows(&salience), &vec![json!(0.5)]);
}

/// A `FILTER` narrows the sweep, and a sweep that matches nothing is a
/// `no_effect` rather than an error: UPDATE never creates (§52.4).
#[tokio::test]
async fn a_selection_that_matches_nothing_changes_nothing() {
    let (nexus, _) = seeded("update_empty").await;

    let response = run(
        &nexus,
        r#"UPDATE ?m
           SET ATTRIBUTES {outcome_status: "reviewed"}
           WHERE {
             ?m CONCEPT {type: "Experience"}
             FILTER(?m.attributes.goal == "nothing matches this")
           }"#,
    )
    .await;
    assert_eq!(response.status, TopLevelStatus::Succeeded);
    assert_eq!(
        response.receipt.as_ref().map(|receipt| receipt.status),
        Some(ReceiptStatus::NoEffect)
    );

    // And an update that computes the value already stored is equally a
    // no-effect: no version bump for a transition that did not happen.
    let response = run(
        &nexus,
        r#"UPDATE ?m
           SET ATTRIBUTES {goal: "rest"}
           WHERE { ?m CONCEPT {name: "Third"} }"#,
    )
    .await;
    assert_eq!(
        response.receipt.as_ref().map(|receipt| receipt.status),
        Some(ReceiptStatus::NoEffect)
    );
}

/// The engine enforces immutability by the element it loaded, not by what the
/// command looked like: `UPDATE :A-7` names an id, and only the engine knows
/// what kind of element wears it. The parser could not have caught this one.
///
/// A Facet, meanwhile, is representation-local state and none of it is truth —
/// but *which* carriers may wear one is the Schema's decision, and the bundled
/// profile scopes `MnemonicState` to Concepts. So both of these are refused,
/// for two different reasons that must not be confused: one is epistemic, one
/// is schema.
#[tokio::test]
async fn an_assertions_payload_is_immutable_and_its_facets_answer_to_the_schema() {
    let (nexus, created) = seeded("update_assertion").await;
    let assertion = handle(&created, "a");

    let run_with = async |command: &str| {
        let request = serde_json::from_value::<Request>(json!({
            "kip": "2.0",
            "operations": [{
                "command": command,
                "parameters": {"a": assertion.to_string()}
            }]
        }))
        .unwrap();
        let parsed = request.operations[0].parse().unwrap();
        nexus
            .execute(parsed, &request, &request.operations[0])
            .await
    };

    let error = |response: anda_kip::Response| {
        response
            .results
            .into_iter()
            .find_map(|result| result.error)
            .expect("this UPDATE must be refused")
    };

    let epistemic = error(run_with(r#"UPDATE :a SET FIELDS {name: "relabelled"}"#).await);
    assert_eq!(epistemic.code, "EpistemicRevisionRequired", "{epistemic:?}");

    let schema = error(run_with(r#"UPDATE :a SET FACET "MnemonicState" {salience: 0.3}"#).await);
    assert_eq!(schema.code, "ConstraintViolation", "{schema:?}");
    assert!(
        schema.message.contains("Assertion"),
        "the refusal names the carrier the Schema rejected: {}",
        schema.message
    );

    // Nothing moved.
    let result = ok(
        &nexus,
        r#"FIND(?a.confidence, ?a.stance) WHERE { ?a ASSERTION {} }"#,
    )
    .await;
    assert_eq!(rows(&result), &vec![json!([0.9, "support"])]);
}

/// Every refusal names the ritual that is legal instead.
#[tokio::test]
async fn each_immutable_target_is_refused_with_the_code_that_says_what_to_do() {
    let (nexus, created) = seeded("update_refusals").await;

    for (handle_name, command, code) in [
        (
            "ev",
            r#"UPDATE :x SET FIELDS {media_type: "text/plain"}"#,
            "EvidenceCorrectionRequired",
        ),
        (
            "p",
            r#"UPDATE :x SET ATTRIBUTES {note: "about the tuple"}"#,
            // A Proposition's attributes are representation-local, so this one
            // is legal; the tuple itself is what cannot move.
            "",
        ),
        (
            "alice",
            r#"UPDATE :x SET FIELDS {key: "moved"}"#,
            "ImmutableField",
        ),
        (
            "alice",
            r#"UPDATE :x SET FIELDS {retention: {retention_class: "standard"}}"#,
            "ImmutableField",
        ),
    ] {
        let target = handle(&created, handle_name);
        let request = serde_json::from_value::<Request>(json!({
            "kip": "2.0",
            "operations": [{"command": command, "parameters": {"x": target.to_string()}}]
        }))
        .unwrap();
        let parsed = request.operations[0].parse().unwrap();
        let response = nexus
            .execute(parsed, &request, &request.operations[0])
            .await;
        let error = response.results.into_iter().find_map(|result| result.error);
        match code {
            "" => assert!(error.is_none(), "{command} should be legal: {error:?}"),
            code => assert_eq!(
                error.as_ref().map(|error| error.code.as_str()),
                Some(code),
                "{command}"
            ),
        }
    }
}

/// `SET STRUCTURAL` through UPDATE reaches mutable Concept topology, and
/// `UNSET STRUCTURAL` removes one named reference and re-densifies the rest.
#[tokio::test]
async fn update_edits_concept_topology_only() {
    let (nexus, created) = seeded("update_structural").await;
    let first = handle(&created, "e1");
    let second = handle(&created, "e2");
    let third = handle(&created, "e3");

    let run_with = async |command: &str, params: Json| {
        let request = serde_json::from_value::<Request>(json!({
            "kip": "2.0",
            "operations": [{"command": command, "parameters": params}]
        }))
        .unwrap();
        let parsed = request.operations[0].parse().unwrap();
        nexus
            .execute(parsed, &request, &request.operations[0])
            .await
    };

    let response = run_with(
        r#"UPDATE :first SET STRUCTURAL { ("derived_from", :second) ("derived_from", :third) }"#,
        json!({
            "first": first.to_string(),
            "second": second.to_string(),
            "third": third.to_string(),
        }),
    )
    .await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "{:#?}",
        response.results
    );

    let linked = ok(
        &nexus,
        r#"FIND(?target.name)
           WHERE {
             ?source CONCEPT {name: "First"}
             STRUCTURAL (?source, "derived_from", ?target)
           }
           ORDER BY ?target.name"#,
    )
    .await;
    assert_eq!(rows(&linked), &vec![json!("Second"), json!("Third")]);

    let response = run_with(
        r#"UPDATE :first UNSET STRUCTURAL { ("derived_from", :second) }"#,
        json!({"first": first.to_string(), "second": second.to_string()}),
    )
    .await;
    assert_eq!(response.status, TopLevelStatus::Succeeded);

    let linked = ok(
        &nexus,
        r#"FIND(?target.name)
           WHERE {
             ?source CONCEPT {name: "First"}
             STRUCTURAL (?source, "derived_from", ?target)
           }"#,
    )
    .await;
    assert_eq!(rows(&linked), &vec![json!("Third")]);

    // An Assertion's citations are not topology anyone may edit.
    let assertion = handle(&created, "a");
    let evidence = handle(&created, "ev");
    let response = run_with(
        r#"UPDATE :a UNSET STRUCTURAL { ("evidence", :ev) }"#,
        json!({"a": assertion.to_string(), "ev": evidence.to_string()}),
    )
    .await;
    let error = response
        .results
        .into_iter()
        .find_map(|result| result.error)
        .expect("an Assertion's citations are immutable");
    assert_eq!(error.code, "EpistemicRevisionRequired", "{error:?}");
}

// ---------------------------------------------------------------------------
// Selection blocks on the lifecycle families
// ---------------------------------------------------------------------------

#[tokio::test]
async fn archive_and_retract_accept_selection_blocks() {
    let (nexus, _) = seeded("selection_lifecycle").await;

    ok(
        &nexus,
        r#"ARCHIVE ?m WHERE {
             ?m CONCEPT {type: "Experience"}
             FILTER(?m.attributes.outcome_status == "failure")
           }"#,
    )
    .await;
    let remaining = ok(
        &nexus,
        r#"FIND(?m.name) WHERE { ?m CONCEPT {type: "Experience"} } ORDER BY ?m.name"#,
    )
    .await;
    assert_eq!(rows(&remaining), &vec![json!("First"), json!("Third")]);

    ok(
        &nexus,
        r#"RETRACT ASSERTION ?a WHERE { ?a ASSERTION {stance: "support"} } LIMIT 10"#,
    )
    .await;
    let status = ok(
        &nexus,
        r#"FIND(?a.lifecycle.status) WHERE { ?a ASSERTION {} }"#,
    )
    .await;
    assert_eq!(rows(&status), &vec![json!("retracted")]);
}

#[tokio::test]
async fn set_retention_accepts_a_selection_block() {
    let (nexus, _) = seeded("selection_retention").await;

    ok(
        &nexus,
        r#"SET RETENTION ?m {retention_class: "standard", expires_at: "2030-01-01T00:00:00Z"}
           WHERE { ?m CONCEPT {type: "Experience"} }
           LIMIT 1"#,
    )
    .await;

    let result = ok(
        &nexus,
        r#"FIND(?m.name, ?m.retention.retention_class)
           WHERE { ?m CONCEPT {type: "Experience"} }
           ORDER BY ?m.name"#,
    )
    .await;
    assert_eq!(
        rows(&result),
        &vec![
            json!(["First", "standard"]),
            json!(["Second", Json::Null]),
            json!(["Third", Json::Null]),
        ]
    );
}

/// A selection block reads the state the transaction started from. Clause
/// order carries no mutation semantics (§24), so a sweep that could see its own
/// transaction's writes would mean different things depending on where it sat.
#[tokio::test]
async fn a_selection_block_does_not_see_the_transactions_own_writes() {
    let (nexus, _) = seeded("selection_snapshot").await;

    ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?fresh { TYPE "Experience" NAME "Fourth"
              SET ATTRIBUTES {goal: "learn", outcome_status: "success"} }
            ARCHIVE ?m WHERE { ?m CONCEPT {type: "Experience"} }
        }"#,
    )
    .await;

    let remaining = ok(
        &nexus,
        r#"FIND(?m.name) WHERE { ?m CONCEPT {type: "Experience"} }"#,
    )
    .await;
    assert_eq!(
        rows(&remaining),
        &vec![json!("Fourth")],
        "the Concept this transaction created must not be archived by its own sweep"
    );
}

/// One spelling, two meanings, is refused rather than silently resolved.
#[tokio::test]
async fn a_handle_and_a_selection_variable_may_not_share_a_name() {
    let (nexus, _) = seeded("selection_clash").await;

    let error = err(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?m { TYPE "Experience" NAME "Fifth"
              SET ATTRIBUTES {goal: "learn", outcome_status: "success"} }
            ARCHIVE ?m WHERE { ?m CONCEPT {type: "Experience"} }
        }"#,
    )
    .await;
    assert_eq!(error.code, "ReferenceError", "{error:?}");

    // A target the block never binds does not even parse: the grammar knows
    // that much without an engine.
    let unbound =
        anda_kip::parse_kip(r#"ARCHIVE ?missing WHERE { ?m CONCEPT {type: "Experience"} }"#)
            .expect_err("an unbound target is refused");
    assert_eq!(unbound.code, anda_kip::KipErrorCode::ReferenceError);
}

// ---------------------------------------------------------------------------
// MERGE CONCEPT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn merge_is_non_destructive_and_leaves_a_forwarding_pointer() {
    let (nexus, created) = seeded("merge").await;
    let duplicate = ok(
        &nexus,
        r#"CREATE CONCEPT ?dup { TYPE "Person" NAME "Alice" SET ATTRIBUTES {description: "a duplicate record"} }"#,
    )
    .await;
    let dup = handle(&duplicate, "dup");
    let alice = handle(&created, "alice");

    let request = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "operations": [{
            "command": "MERGE CONCEPT ?source INTO ?target WHERE { ?source CONCEPT {id: :dup} ?target CONCEPT {id: :alice} }",
            "parameters": {"dup": dup.to_string(), "alice": alice.to_string()}
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
        response.results
    );

    // The source keeps everything it had, and gains a pointer.
    let Element::Concept(row) = nexus.store.get_element(dup).await.unwrap() else {
        panic!("the merged source is still a Concept");
    };
    assert_eq!(row.merged_into, alice.to_string());
    assert_eq!(row.state, "merged");
    assert_eq!(
        row.name, "Alice",
        "a merge copies nothing and erases nothing"
    );
    assert_eq!(row.attributes["description"], json!("a duplicate record"));

    // The target is untouched: a merge does not invent claims about it.
    let Element::Concept(target) = nexus.store.get_element(alice).await.unwrap() else {
        panic!("the merge target is still a Concept");
    };
    assert_eq!(target.version, 1, "the target is not rewritten by a merge");

    // And the source has left ordinary recall.
    let people = ok(
        &nexus,
        r#"FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} }"#,
    )
    .await;
    assert_eq!(rows(&people).len(), 1);
}

#[tokio::test]
async fn a_merge_that_would_cycle_or_re_point_an_identity_is_refused() {
    let (nexus, created) = seeded("merge_conflicts").await;
    let alice = handle(&created, "alice");
    let second = ok(
        &nexus,
        r#"CREATE CONCEPT ?b { TYPE "Person" NAME "Alicia" }"#,
    )
    .await;
    let b = handle(&second, "b");
    let third = ok(&nexus, r#"CREATE CONCEPT ?c { TYPE "Person" NAME "Ali" }"#).await;
    let c = handle(&third, "c");

    // The operands are named directly here rather than bound by a guard: a
    // merged Concept has left ordinary recall, so no pattern can name it, and
    // every case below involves one.
    let merge = async |source: ElementId, target: ElementId| {
        let request = serde_json::from_value::<Request>(json!({
            "kip": "2.0",
            "operations": [{
                "command": "MERGE CONCEPT :s INTO :t",
                "parameters": {"s": source.to_string(), "t": target.to_string()}
            }]
        }))
        .unwrap();
        let parsed = request.operations[0].parse().unwrap();
        nexus
            .execute(parsed, &request, &request.operations[0])
            .await
    };

    assert_eq!(merge(b, alice).await.status, TopLevelStatus::Succeeded);

    // A cycle would make canonical resolution non-terminating (§11.1).
    let response = merge(alice, b).await;
    let error = response
        .results
        .into_iter()
        .find_map(|result| result.error)
        .expect("a cycle must be refused");
    assert_eq!(error.code, "IdentityMergeConflict", "{error:?}");

    // Re-pointing a merged identity would rewrite a decision later writes have
    // already canonicalized through.
    let response = merge(b, c).await;
    let error = response
        .results
        .into_iter()
        .find_map(|result| result.error)
        .expect("re-pointing a merged identity must be refused");
    assert_eq!(error.code, "IdentityMergeConflict", "{error:?}");

    // Merging something into itself is not a no-op, it is a mistake.
    let response = merge(c, c).await;
    let error = response
        .results
        .into_iter()
        .find_map(|result| result.error)
        .expect("a self-merge must be refused");
    assert_eq!(error.code, "IdentityMergeConflict", "{error:?}");

    // The same merge twice is idempotent, though: the second one changes
    // nothing rather than conflicting with itself.
    let response = merge(b, alice).await;
    assert_eq!(
        response.receipt.as_ref().map(|receipt| receipt.status),
        Some(ReceiptStatus::NoEffect)
    );

    // A guard block naming an operand that has left ordinary recall matches
    // nothing, and a guard that matches nothing is a no-effect rather than an
    // error — the same rule every other selection block follows.
    let request = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "operations": [{
            "command": "MERGE CONCEPT ?source INTO ?target WHERE { ?source CONCEPT {id: :s} ?target CONCEPT {id: :t} }",
            "parameters": {"s": c.to_string(), "t": b.to_string()}
        }]
    }))
    .unwrap();
    let parsed = request.operations[0].parse().unwrap();
    let response = nexus
        .execute(parsed, &request, &request.operations[0])
        .await;
    assert_eq!(response.status, TopLevelStatus::Succeeded);
    assert_eq!(
        response.receipt.as_ref().map(|receipt| receipt.status),
        Some(ReceiptStatus::NoEffect),
        "a guard that cannot name a merged Concept merges nothing"
    );
}

/// §11.3: a new claim about a merged-away Concept is recorded about the
/// identity that survived. Without this the merge would be decorative — later
/// claims would pile up on the identity the merge said was the same one, and
/// the two would never meet.
#[tokio::test]
async fn a_new_write_canonicalizes_a_merged_reference() {
    let (nexus, created) = seeded("merge_canonicalization").await;
    let alice = handle(&created, "alice");
    let dark = handle(&created, "dark");
    let duplicate = ok(
        &nexus,
        r#"CREATE CONCEPT ?dup { TYPE "Person" NAME "Alice" }"#,
    )
    .await;
    let dup = handle(&duplicate, "dup");

    let request = |command: &str, params: Json| {
        serde_json::from_value::<Request>(json!({
            "kip": "2.0",
            "operations": [{"command": command, "parameters": params}]
        }))
        .unwrap()
    };
    let run_with = async |command: &str, params: Json| {
        let request = request(command, params);
        let parsed = request.operations[0].parse().unwrap();
        nexus
            .execute(parsed, &request, &request.operations[0])
            .await
    };

    let merged = run_with(
        "MERGE CONCEPT :s INTO :t",
        json!({"s": dup.to_string(), "t": alice.to_string()}),
    )
    .await;
    assert_eq!(merged.status, TopLevelStatus::Succeeded);

    // The same tuple, written against the merged-away id, resolves to the
    // Proposition that already exists about the survivor.
    let response = run_with(
        r#"ENSURE PROPOSITION ?p (:dup, "prefers", :dark)"#,
        json!({"dup": dup.to_string(), "dark": dark.to_string()}),
    )
    .await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "{:#?}",
        response.results
    );
    let ensured = response.first_result().cloned().unwrap();
    assert_eq!(
        handle(&ensured, "p"),
        handle(&created, "p"),
        "a new write about a merged Concept lands on the surviving identity"
    );

    let count = ok(
        &nexus,
        r#"FIND(COUNT(?p)) WHERE { ?p PROPOSITION (?s, "prefers", ?o) }"#,
    )
    .await;
    assert_eq!(rows(&count), &vec![json!(1)]);
}

/// `UPSERT CONCEPT` accepts `UNSET FACET` and `SET STRUCTURAL`, and both used
/// to parse, validate, and then quietly do nothing. A mutation that is accepted
/// and dropped is worse than one that is refused: the caller has a receipt.
#[tokio::test]
async fn upsert_applies_every_action_it_accepts() {
    let (nexus, created) = seeded("upsert_actions").await;
    let first = handle(&created, "e1");

    let request = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "operations": [{
            "command": r#"UPSERT CONCEPT ?m {
                            MATCH {id: :id}
                            SET FACET "MnemonicState" {salience: 0.9}
                            UNSET FACET "MnemonicState" {memory_strength}
                            SET STRUCTURAL { ("derived_from", :other) }
                          }"#,
            "parameters": {"id": first.to_string(), "other": handle(&created, "e2").to_string()}
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
        response.results
    );

    let facets = ok(
        &nexus,
        r#"FIND(?m.facets["MnemonicState"].salience, ?m.facets["MnemonicState"].memory_strength)
           WHERE { ?m CONCEPT {name: "First"} }"#,
    )
    .await;
    assert_eq!(rows(&facets), &vec![json!([0.9, Json::Null])]);

    let linked = ok(
        &nexus,
        r#"FIND(?target.name)
           WHERE {
             ?source CONCEPT {name: "First"}
             STRUCTURAL (?source, "derived_from", ?target)
           }"#,
    )
    .await;
    assert_eq!(rows(&linked), &vec![json!("Second")]);
}

// ---------------------------------------------------------------------------
// PURGE
// ---------------------------------------------------------------------------

/// The one selection-bearing form that stays refused, and the refusal says why
/// rather than reading as an unimplemented corner.
#[tokio::test]
async fn purge_refuses_by_default_while_the_target_is_still_referenced() {
    // §173, §175: in a cognitive history an Assertion, an Activity or an
    // Experience may point at the target, and erasing the dependency chain
    // falsifies history. KIP 1.x made destructive cascade ordinary; 2.0 does
    // not, and the default reference policy is where that shows.
    let (nexus, created) = seeded("purge").await;
    let alice = handle(&created, "alice");
    let error = err(
        &nexus,
        &format!(r#"PURGE "{alice}" REFERENCE POLICY "deny_if_referenced" CONFIRM "PURGE""#),
    )
    .await;
    assert_eq!(error.code, "PurgeDenied", "{error:?}");
    assert!(
        error.message.contains("reference"),
        "the refusal must say why: {}",
        error.message
    );
}

#[tokio::test]
async fn purging_an_unreferenced_element_leaves_an_identity_stub() {
    // §19.3: a stub so audit and provenance-root identity survive byte
    // destruction. Deleting the row would break every reference to it, and a
    // dangling reference does not say "this was erased" — it says nothing.
    let (nexus, created) = seeded("purge_stub").await;
    let third = handle(&created, "e3");
    let receipt = ok(&nexus, &format!(r#"PURGE "{third}" CONFIRM "PURGE""#)).await;
    assert!(receipt.is_object());

    let stub = rows(
        &ok(
            &nexus,
            r#"FIND(?e.id, ?e.name, ?e.governance.purged) WHERE { ?e CONCEPT {state: "purged"} }"#,
        )
        .await,
    )
    .clone();
    assert_eq!(stub.len(), 1);
    assert_eq!(stub[0][0], serde_json::json!(third.to_string()));
    assert_eq!(stub[0][1], Json::Null, "the content is gone");
    assert_eq!(stub[0][2], serde_json::json!(true));

    // And the history goes with it: a purge that left the version log behind
    // would leave the element fully readable through AS OF.
    let past = rows(
        &ok(
            &nexus,
            r#"FIND(?e.name) WHERE { ?e CONCEPT {type: "Experience"} } AS OF SEQ 1"#,
        )
        .await,
    )
    .clone();
    assert!(
        !past.iter().any(|name| name == "Third"),
        "the erased element must not reappear at a past coordinate: {past:?}"
    );
}

#[tokio::test]
async fn a_legal_hold_stops_a_purge_that_is_otherwise_authorized() {
    let (nexus, created) = seeded("legal_hold").await;
    let third = handle(&created, "e3");
    ok(
        &nexus,
        &format!(r#"SET RETENTION "{third}" {{ legal_hold: true }}"#),
    )
    .await;
    let error = err(&nexus, &format!(r#"PURGE "{third}" CONFIRM "PURGE""#)).await;
    assert_eq!(error.code, "LegalHoldConflict", "{error:?}");
}
