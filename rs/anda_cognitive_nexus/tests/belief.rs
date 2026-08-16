//! Epistemic Projection tests: real claims, real disagreement, real answers.
//!
//! Every fixture here is written through KML, so what the projection reads is
//! what the engine actually stored.

use anda_cognitive_nexus::{
    CognitiveNexus,
    nexus::DEFAULT_SPACE,
    schema::{PackageState, SchemaLock, SchemaPackage},
};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{Executor, Json, Request, TopLevelStatus};
use object_store::memory::InMemory;
use serde_json::json;
use std::sync::Arc;

const COGNITIVE_MEMORY: &str = include_str!("fixtures/cognitive-memory-2.0.0.json");
const PROFILE_ID: &str = "kip://profiles/cognitive-memory";

/// A package with a functional predicate, so conflict expansion has something
/// to expand. The shipped profile has none.
const STATUS_PACKAGE: &str = r#"{
    "format": "KIP-Schema-Package",
    "manifest": {"package_id": "kip://test/status", "version": "1.0.0"},
    "definitions": {
        "concept_types": {
            "Service": {"kind": "ConceptType", "description": "A service."},
            "Status": {"kind": "ConceptType", "description": "A status value."}
        },
        "predicates": {
            "status": {
                "kind": "PredicateType",
                "description": "The service's current status. Single-valued.",
                "functional": true,
                "open_world": true
            },
            "mentions": {
                "kind": "PredicateType",
                "description": "Non-functional reference.",
                "functional": false
            }
        }
    }
}"#;

async fn fresh(name: &str) -> CognitiveNexus {
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: name.to_string(),
            description: "belief tests".to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let nexus = CognitiveNexus::connect(Arc::new(db)).await.unwrap();
    for source in [COGNITIVE_MEMORY, STATUS_PACKAGE] {
        nexus
            .install_package(&SchemaPackage::parse(source).unwrap(), "test")
            .await
            .unwrap();
    }
    let mut lock = SchemaLock::default();
    for (id, version) in [(PROFILE_ID, "2.0.0"), ("kip://test/status", "1.0.0")] {
        lock.packages.insert(id.to_string(), version.to_string());
        lock.states.insert(id.to_string(), PackageState::Active);
    }
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
        response.error
    );
    response.first_result().cloned().unwrap_or(Json::Null)
}

/// Projects the one Proposition `(alice, prefers, dark)` and returns the result.
async fn belief(nexus: &CognitiveNexus, extra: &str) -> Json {
    let command = format!(
        r#"FIND(?b) WHERE {{
             ?s CONCEPT {{name: "Alice"}}
             ?o CONCEPT {{name: "Dark"}}
             ?p PROPOSITION (?s, "prefers", ?o)
             ?b BELIEF (?p)
           }}{extra}"#
    );
    let result = ok(nexus, &command).await;
    result
        .as_array()
        .unwrap()
        .first()
        .cloned()
        .unwrap_or(Json::Null)
}

async fn base(name: &str) -> CognitiveNexus {
    let nexus = fresh(name).await;
    ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?bob { TYPE "Person" NAME "Bob" }
            CREATE CONCEPT ?carol { TYPE "Person" NAME "Carol" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
            ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
        }"#,
    )
    .await;
    nexus
}

/// Adds one Assertion about the seeded Proposition.
async fn assert_claim(nexus: &CognitiveNexus, actor: &str, body: &str) {
    let command = format!(
        r#"MUTATE {{
             CREATE ASSERTION ?a {{
               SET FIELDS {{ proposition: :p, asserted_by: :actor, {body} }}
             }}
           }}"#
    );
    // Resolve the ids, then bind them as parameters.
    let proposition = ok(
        nexus,
        r#"FIND(?p) WHERE { ?p PROPOSITION (?s, "prefers", ?o) }"#,
    )
    .await;
    let proposition = proposition.as_array().unwrap()[0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let actor_id = ok(
        nexus,
        &format!(r#"FIND(?c) WHERE {{ ?c CONCEPT {{name: "{actor}"}} }}"#),
    )
    .await;
    let actor_id = actor_id.as_array().unwrap()[0]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let request = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "operations": [{
            "command": command,
            "parameters": {"p": proposition, "actor": actor_id}
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
        "{command}\n{:#?}",
        response.error
    );
}

#[tokio::test]
async fn nothing_on_record_is_insufficient_and_never_rejected() {
    // Spec §21.5, §24 — the open-world rule. This is the single most important
    // behaviour in the projection: silence is not a "no".
    let nexus = base("insufficient").await;
    let projected = belief(&nexus, "").await;
    assert_eq!(projected["status"], "insufficient");
    assert_eq!(projected["support"]["score"], 0.0);
    assert_eq!(projected["opposition"]["score"], 0.0);
    assert_eq!(
        projected["uncertainty"]["reasons"][0],
        "no eligible Assertion bears on this Proposition"
    );
}

#[tokio::test]
async fn one_confident_source_is_accepted_and_says_it_stands_alone() {
    let nexus = base("accepted").await;
    assert_claim(
        &nexus,
        "Alice",
        r#"stance: "support", mode: "stated", confidence: 0.9"#,
    )
    .await;

    let projected = belief(&nexus, "").await;
    assert_eq!(projected["status"], "accepted");
    assert_eq!(projected["support"]["score"], 0.9);
    assert_eq!(projected["support"]["independent_groups"], 1);
    // Accepted, and still honest about resting on one voice.
    let reasons = projected["uncertainty"]["reasons"].as_array().unwrap();
    assert!(
        reasons
            .iter()
            .any(|r| r.as_str().unwrap().contains("single source")),
        "{reasons:?}"
    );
    // §76: the score must declare what it is, and it is not a probability.
    assert_eq!(
        projected["support"]["score_semantics"],
        "normalized_support_not_probability"
    );
}

#[tokio::test]
async fn repeating_a_claim_does_not_make_it_stronger() {
    // Spec §94. This is how a memory system talks itself into certainty, and
    // the corroboration grouping is what stops it.
    let nexus = base("repetition").await;
    for _ in 0..3 {
        assert_claim(
            &nexus,
            "Alice",
            r#"stance: "support", mode: "stated", confidence: 0.6"#,
        )
        .await;
    }
    let repeated = belief(&nexus, "").await;
    assert_eq!(repeated["support"]["independent_groups"], 1);
    assert_eq!(repeated["support"]["score"], 0.6);
    assert_eq!(repeated["status"], "uncertain", "0.6 is below acceptance");

    // Three different people saying it is a different matter entirely.
    let nexus = base("corroboration").await;
    for actor in ["Alice", "Bob", "Carol"] {
        assert_claim(
            &nexus,
            actor,
            r#"stance: "support", mode: "stated", confidence: 0.6"#,
        )
        .await;
    }
    let corroborated = belief(&nexus, "").await;
    assert_eq!(corroborated["support"]["independent_groups"], 3);
    assert_eq!(corroborated["status"], "accepted");
    assert!(corroborated["support"]["score"].as_f64().unwrap() > 0.9);
}

#[tokio::test]
async fn material_disagreement_is_contested_rather_than_decided() {
    // Spec §71. A memory that reported the leading side as accepted would be
    // hiding the disagreement it was built to record.
    let nexus = base("contested").await;
    assert_claim(
        &nexus,
        "Alice",
        r#"stance: "support", mode: "stated", confidence: 0.9"#,
    )
    .await;
    assert_claim(
        &nexus,
        "Bob",
        r#"stance: "reject", mode: "observed", confidence: 0.8"#,
    )
    .await;

    let projected = belief(&nexus, "").await;
    assert_eq!(projected["status"], "contested");
    assert_eq!(
        projected["support"]["assertion_ids"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        projected["opposition"]["assertion_ids"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(projected["uncertainty"]["level"], "high");
    // Both sides are named, so a caller can go and look at each.
    assert!(projected["support"]["score"].as_f64().unwrap() >= 0.9);
    assert!(projected["opposition"]["score"].as_f64().unwrap() >= 0.8);
}

#[tokio::test]
async fn retracting_a_claim_removes_it_from_belief_but_not_from_the_ledger() {
    // Spec §59: a retracted claim stops supporting and stays explainable.
    let nexus = base("retracted").await;
    assert_claim(
        &nexus,
        "Alice",
        r#"stance: "support", mode: "stated", confidence: 0.9"#,
    )
    .await;
    assert_eq!(belief(&nexus, "").await["status"], "accepted");

    let found = ok(&nexus, r#"FIND(?a) WHERE { ?a ASSERTION {} }"#).await;
    let assertion = found.as_array().unwrap()[0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let request = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "operations": [{"command": "RETRACT ASSERTION :a", "parameters": {"a": assertion}}]
    }))
    .unwrap();
    let parsed = request.operations[0].parse().unwrap();
    nexus
        .execute(parsed, &request, &request.operations[0])
        .await;

    let projected = belief(&nexus, "").await;
    assert_eq!(projected["status"], "insufficient", "withdrawn, not denied");
    let excluded = projected["explanation"]["excluded"].as_array().unwrap();
    assert_eq!(excluded.len(), 1);
    assert_eq!(excluded[0]["reason"], "retracted");
    assert_eq!(excluded[0]["assertion_id"], assertion);
}

#[tokio::test]
async fn a_hypothetical_is_excluded_from_a_factual_projection() {
    // Spec §38: entertained without commitment. Counting it would let a
    // thought experiment become a belief.
    let nexus = base("hypothetical").await;
    assert_claim(
        &nexus,
        "Alice",
        r#"stance: "support", mode: "hypothetical", confidence: 0.95"#,
    )
    .await;

    let projected = belief(&nexus, "").await;
    assert_eq!(projected["status"], "insufficient");
    assert_eq!(
        projected["explanation"]["excluded"][0]["reason"],
        "hypothetical_not_requested"
    );

    // A forecast policy is a different question, asked explicitly.
    let predicted = base("predicted").await;
    assert_claim(
        &predicted,
        "Alice",
        r#"stance: "support", mode: "predicted", confidence: 0.9"#,
    )
    .await;
    assert_eq!(belief(&predicted, "").await["status"], "insufficient");
    let forecast = belief(&predicted, r#" WITH EPISTEMIC {policy: "forecast"}"#).await;
    assert_eq!(forecast["status"], "accepted");
    assert_eq!(forecast["policy"]["id"], "kip:policy:forecast");
}

#[tokio::test]
async fn the_policy_travels_with_the_answer() {
    // Spec §54: "accepted" with no policy attached is not auditable.
    let nexus = base("policy").await;
    assert_claim(
        &nexus,
        "Alice",
        r#"stance: "support", mode: "stated", confidence: 0.8"#,
    )
    .await;

    let default = belief(&nexus, "").await;
    assert_eq!(default["policy"]["id"], "kip:policy:baseline");
    assert_eq!(default["policy"]["version"], 1);
    assert_eq!(default["status"], "accepted");

    // Raising the bar changes the answer — and changes the reported identity,
    // so the two answers cannot be confused for each other.
    let strict = belief(&nexus, r#" WITH EPISTEMIC {accept: 0.95}"#).await;
    assert_eq!(strict["status"], "uncertain");
    assert_ne!(strict["policy"]["id"], "kip:policy:baseline");
    assert!(
        strict["policy"]["id"]
            .as_str()
            .unwrap()
            .starts_with("kip:policy:baseline")
    );

    // The result context reports it too.
    let response = run(
        &nexus,
        r#"FIND(?b) WHERE {
             ?s CONCEPT {name: "Alice"}
             ?o CONCEPT {name: "Dark"}
             ?p PROPOSITION (?s, "prefers", ?o)
             ?b BELIEF (?p)
           }"#,
    )
    .await;
    let context = response.results[0].context.as_ref().unwrap();
    let policy = context.epistemic_policy.as_ref().expect("a projection ran");
    assert_eq!(policy.id.as_deref(), Some("kip:policy:baseline"));
}

#[tokio::test]
async fn a_functional_predicate_makes_rival_values_oppose_each_other() {
    // Spec §34.2 and §58: conflict-set expansion. Nobody rejected "healthy";
    // they asserted "degraded", and the schema says only one can apply.
    let nexus = fresh("functional").await;
    ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?svc { TYPE "Service" NAME "api" }
            CREATE CONCEPT ?healthy { TYPE "Status" NAME "healthy" }
            CREATE CONCEPT ?degraded { TYPE "Status" NAME "degraded" }
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?bob { TYPE "Person" NAME "Bob" }
            ENSURE PROPOSITION ?p1 (?svc, "status", ?healthy)
            ENSURE PROPOSITION ?p2 (?svc, "status", ?degraded)
            CREATE ASSERTION ?a1 {
                SET FIELDS {proposition: ?p1, asserted_by: ?alice, stance: "support", mode: "observed", confidence: 0.8}
            }
            CREATE ASSERTION ?a2 {
                SET FIELDS {proposition: ?p2, asserted_by: ?bob, stance: "support", mode: "observed", confidence: 0.8}
            }
        }"#,
    )
    .await;

    let result = ok(
        &nexus,
        r#"FIND(?b) WHERE {
             ?svc CONCEPT {name: "api"}
             ?v CONCEPT {name: "healthy"}
             ?p PROPOSITION (?svc, "status", ?v)
             ?b BELIEF (?p)
           }"#,
    )
    .await;
    let projected = &result.as_array().unwrap()[0];
    assert_eq!(projected["status"], "contested");
    assert_eq!(
        projected["opposition"]["independent_groups"], 1,
        "the rival value opposes without anyone rejecting"
    );

    // A non-functional predicate has no such rivalry: two values coexist.
    let coexisting = fresh("non_functional").await;
    ok(
        &coexisting,
        r#"MUTATE {
            CREATE CONCEPT ?svc { TYPE "Service" NAME "api" }
            CREATE CONCEPT ?a { TYPE "Status" NAME "one" }
            CREATE CONCEPT ?b { TYPE "Status" NAME "two" }
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            ENSURE PROPOSITION ?p1 (?svc, "mentions", ?a)
            ENSURE PROPOSITION ?p2 (?svc, "mentions", ?b)
            CREATE ASSERTION ?a1 {
                SET FIELDS {proposition: ?p1, asserted_by: ?alice, stance: "support", mode: "stated", confidence: 0.9}
            }
            CREATE ASSERTION ?a2 {
                SET FIELDS {proposition: ?p2, asserted_by: ?alice, stance: "support", mode: "stated", confidence: 0.9}
            }
        }"#,
    )
    .await;
    let result = ok(
        &coexisting,
        r#"FIND(?b) WHERE {
             ?svc CONCEPT {name: "api"}
             ?v CONCEPT {name: "one"}
             ?p PROPOSITION (?svc, "mentions", ?v)
             ?b BELIEF (?p)
           }"#,
    )
    .await;
    assert_eq!(result.as_array().unwrap()[0]["status"], "accepted");
}

#[tokio::test]
async fn a_belief_slot_reports_the_conflict_set_not_a_winner() {
    // Spec §35: a contested slot has a leading side and still no settled
    // answer. Collapsing it to one value would be the engine picking a winner
    // nobody authorized.
    let nexus = fresh("slot").await;
    ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?svc { TYPE "Service" NAME "api" }
            CREATE CONCEPT ?healthy { TYPE "Status" NAME "healthy" }
            CREATE CONCEPT ?degraded { TYPE "Status" NAME "degraded" }
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?bob { TYPE "Person" NAME "Bob" }
            ENSURE PROPOSITION ?p1 (?svc, "status", ?healthy)
            ENSURE PROPOSITION ?p2 (?svc, "status", ?degraded)
            CREATE ASSERTION ?a1 {
                SET FIELDS {proposition: ?p1, asserted_by: ?alice, stance: "support", mode: "observed", confidence: 0.9}
            }
            CREATE ASSERTION ?a2 {
                SET FIELDS {proposition: ?p2, asserted_by: ?bob, stance: "support", mode: "observed", confidence: 0.5}
            }
        }"#,
    )
    .await;

    let svc = ok(&nexus, r#"FIND(?c) WHERE { ?c CONCEPT {name: "api"} }"#).await;
    let svc = svc.as_array().unwrap()[0]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let request = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "operations": [{
            "command": r#"FIND(?slot) WHERE { ?slot BELIEF SLOT (:svc, "status") }"#,
            "parameters": {"svc": svc}
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

    let slot = &response.first_result().unwrap().as_array().unwrap()[0];
    assert_eq!(slot["candidate_projections"].as_array().unwrap().len(), 2);
    assert_eq!(slot["contested"], true);
    // Neither candidate is accepted: each opposes the other through the
    // functional predicate, so the slot has no settled value at all.
    assert!(slot["accepted_values"].as_array().unwrap().is_empty());
    // A leading side exists and is named as *leading*, not as the value.
    assert!(slot["leading"].is_string());
    assert!(slot.get("value").is_none());
}

#[tokio::test]
async fn a_projection_target_that_names_nothing_is_refused() {
    // Projecting every slot in the Space because the subject was unbound would
    // answer a question nobody asked, expensively.
    let nexus = base("unbound").await;
    let response = run(
        &nexus,
        r#"FIND(?slot) WHERE { ?slot BELIEF SLOT (?anything, "prefers") }"#,
    )
    .await;
    assert_eq!(
        response.error.as_ref().unwrap().code.as_str(),
        "ProjectionTargetUnbounded"
    );

    let unbound = run(&nexus, r#"FIND(?b) WHERE { ?b BELIEF (?p) }"#).await;
    assert_eq!(
        unbound.error.as_ref().unwrap().code.as_str(),
        "ProjectionTargetUnbound"
    );
}

#[tokio::test]
async fn a_belief_can_be_filtered_and_projected_by_dot_path() {
    let nexus = base("dot_path").await;
    assert_claim(
        &nexus,
        "Alice",
        r#"stance: "support", mode: "stated", confidence: 0.9"#,
    )
    .await;

    let status = ok(
        &nexus,
        r#"FIND(?b.status) WHERE {
             ?s CONCEPT {name: "Alice"}
             ?o CONCEPT {name: "Dark"}
             ?p PROPOSITION (?s, "prefers", ?o)
             ?b BELIEF (?p)
             FILTER(?b.support.score > 0.5)
           }"#,
    )
    .await;
    assert_eq!(status.as_array().unwrap(), &vec![json!("accepted")]);

    // And a filter that excludes it leaves nothing, rather than erroring.
    let none = ok(
        &nexus,
        r#"FIND(?b.status) WHERE {
             ?s CONCEPT {name: "Alice"}
             ?o CONCEPT {name: "Dark"}
             ?p PROPOSITION (?s, "prefers", ?o)
             ?b BELIEF (?p)
             FILTER(?b.status == "rejected")
           }"#,
    )
    .await;
    assert!(none.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn a_projection_never_claims_a_trust_judgement_it_did_not_make() {
    // The engine has no trust model. An answer that read as trust-weighted
    // when every group counted equally would be worse than none.
    let nexus = base("warnings").await;
    assert_claim(
        &nexus,
        "Alice",
        r#"stance: "support", mode: "stated", confidence: 0.9"#,
    )
    .await;
    let projected = belief(&nexus, "").await;
    let warnings = projected["explanation"]["warnings"].as_array().unwrap();
    assert!(
        warnings
            .iter()
            .any(|w| w.as_str().unwrap().contains("no source trust")),
        "{warnings:?}"
    );
}
