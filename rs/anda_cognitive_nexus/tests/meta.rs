//! META tests: orientation, introspection, grounding, history.

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

const COGNITIVE_MEMORY: &str = anda_cognitive_nexus::profiles::COGNITIVE_MEMORY;
const PROFILE_ID: &str = "kip://profiles/cognitive-memory";

async fn fresh(name: &str) -> CognitiveNexus {
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: name.to_string(),
            description: "meta tests".to_string(),
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
        response.error
    );
    response.first_result().cloned().unwrap_or(Json::Null)
}

async fn seeded(name: &str) -> CognitiveNexus {
    let nexus = fresh(name).await;
    ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice Anderson" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark mode theme" }
            ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
            CREATE ASSERTION ?a {
                SET FIELDS {proposition: ?p, asserted_by: ?alice, stance: "support", mode: "stated", confidence: 0.9}
            }
        }"#,
    )
    .await;
    nexus
}

#[tokio::test]
async fn the_primer_orients_an_agent_before_it_reads_anything() {
    let nexus = seeded("primer").await;
    let primer = ok(&nexus, "DESCRIBE PRIMER").await;

    assert_eq!(primer["space"]["id"], DEFAULT_SPACE);
    assert_eq!(primer["contents"]["concept"], 2);
    assert_eq!(primer["contents"]["proposition"], 1);
    assert_eq!(primer["contents"]["assertion"], 1);
    assert_eq!(primer["schema"]["environment_version"], 1);
    assert!(
        primer["schema"]["types"]
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t.as_str().unwrap().ends_with("/Person"))
    );

    // The invariants a caller will otherwise get wrong are stated, not implied.
    let invariants = primer["safety_invariants"].as_array().unwrap();
    assert!(invariants.iter().any(|i| {
        i.as_str()
            .unwrap()
            .contains("not the Proposition being true")
    }));
    assert!(
        invariants
            .iter()
            .any(|i| i.as_str().unwrap().contains("never 'no'"))
    );

    // Compact omits the bulky sections; full includes them.
    assert!(primer.get("capabilities").is_none());
    let full = ok(&nexus, r#"DESCRIBE PRIMER MODE "full""#).await;
    assert!(full["capabilities"]["supported"].is_object());
    assert_eq!(full["protocol"]["kip"], "2.0");
}

#[tokio::test]
async fn capabilities_report_the_gaps_as_data_not_as_errors() {
    // An Agent that has to discover a gap by triggering an error has wasted a
    // turn; one that never discovers it reads an absent feature as an absent
    // fact.
    let nexus = fresh("capabilities").await;
    let caps = ok(&nexus, "DESCRIBE CAPABILITIES").await;

    assert_eq!(caps["kip"], "2.0");
    assert_eq!(caps["supported"]["search_modes"], json!(["keyword"]));
    assert_eq!(
        caps["supported"]["execution_modes"],
        json!(["independent", "sequence"])
    );

    let unsupported: Vec<&str> = caps["unsupported"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["capability"].as_str().unwrap())
        .collect();
    for expected in [
        "atomic_batch",
        "historical_search",
        "semantic_search",
        "trust_model",
        "governance",
        "capsule_import_modes",
        "capsule_signatures",
    ] {
        assert!(unsupported.contains(&expected), "missing {expected}");
    }
    // Each gap says why, not just that.
    for entry in caps["unsupported"].as_array().unwrap() {
        assert!(!entry["reason"].as_str().unwrap().is_empty());
    }
}

#[tokio::test]
async fn describe_answers_with_canonical_identity_not_the_local_name() {
    // Spec §106. A caller that stored the local name would have stored
    // something whose meaning changes when the Space's schema does.
    let nexus = fresh("canonical").await;
    let described = ok(&nexus, r#"DESCRIBE TYPE "Person""#).await;
    assert_eq!(
        described["ref"],
        "kip://profiles/cognitive-memory@2.0.0/Person"
    );
    assert_eq!(described["local_name"], "Person");
    assert_eq!(
        described["package_ref"],
        "kip://profiles/cognitive-memory@2.0.0"
    );
    assert!(described["definition"]["attributes"]["open"].is_boolean());

    let predicate = ok(&nexus, r#"DESCRIBE PREDICATE "prefers""#).await;
    assert_eq!(predicate["definition"]["functional"], false);

    let facet = ok(&nexus, r#"DESCRIBE FACET "MnemonicState""#).await;
    assert_eq!(facet["definition"]["closed"], true);

    // An unknown symbol is a miss, not an empty answer.
    let missing = run(&nexus, r#"DESCRIBE TYPE "Spaceship""#).await;
    assert_eq!(
        missing.error.as_ref().unwrap().code.as_str(),
        "SchemaSymbolNotFound"
    );
}

#[tokio::test]
async fn list_enumerates_the_schema_environment_and_pages() {
    let nexus = fresh("list").await;
    let types = ok(&nexus, "LIST TYPES").await;
    let names: Vec<&str> = types
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["local_name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"Person"));
    assert!(names.contains(&"Experience"));

    let packages = ok(&nexus, "LIST SCHEMA PACKAGES").await;
    assert_eq!(packages.as_array().unwrap().len(), 1);
    assert_eq!(packages[0]["status"], "active");

    // Filtering by a status nothing has returns nothing, rather than
    // everything.
    let blocked = ok(&nexus, r#"LIST SCHEMA PACKAGES STATUS "blocked""#).await;
    assert!(blocked.as_array().unwrap().is_empty());

    let spaces = ok(&nexus, "LIST SPACES").await;
    assert_eq!(spaces[0]["id"], DEFAULT_SPACE);

    let paged = run(&nexus, "LIST TYPES LIMIT 2").await;
    assert_eq!(paged.first_result().unwrap().as_array().unwrap().len(), 2);
    assert!(paged.next_cursor.is_some());
}

#[tokio::test]
async fn the_error_registry_is_introspectable() {
    // An Agent deciding whether to retry needs the retry class, and reading it
    // from a registry beats parsing prose out of a message.
    let nexus = fresh("errors").await;
    let described = ok(&nexus, r#"DESCRIBE ERROR "VersionConflict""#).await;
    assert_eq!(described["code"], "VersionConflict");
    assert_eq!(described["retry_class"], "requires_refresh");
    assert_eq!(described["category"], "transaction");

    let unknown = run(&nexus, r#"DESCRIBE ERROR "NotAnError""#).await;
    assert_eq!(
        unknown.error.as_ref().unwrap().code.as_str(),
        "NotFoundOrNotVisible"
    );
}

#[tokio::test]
async fn search_grounds_and_says_what_its_score_is_not() {
    // Spec §77: a score is not a confidence and a miss is not an absence.
    let nexus = seeded("search").await;
    let result = ok(&nexus, r#"SEARCH CONCEPT "Anderson""#).await;
    let hits = result["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0]["element"]["name"], "Alice Anderson");
    assert!(hits[0]["score"].as_f64().unwrap() > 0.0);
    // The hit carries a score and never a confidence.
    assert!(hits[0].get("confidence").is_none());
    assert_eq!(
        result["search_context"]["score_semantics"],
        "bm25_relevance_not_confidence"
    );
    assert!(
        result["caveat"]
            .as_str()
            .unwrap()
            .contains("not an absence")
    );
    assert!(result["search_context"]["index_seq"].is_number());

    // Narrowing by type still resolves the local name to its exact symbol.
    let typed = ok(&nexus, r#"SEARCH CONCEPT "Dark" WITH TYPE "Preference""#).await;
    assert_eq!(typed["hits"].as_array().unwrap().len(), 1);
    let mismatched = ok(&nexus, r#"SEARCH CONCEPT "Dark" WITH TYPE "Person""#).await;
    assert!(mismatched["hits"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn an_unavailable_search_mode_is_refused_rather_than_downgraded() {
    // Silently answering a semantic query with keyword results would look like
    // an answer to a question nobody asked.
    let nexus = seeded("search_modes").await;
    let response = run(&nexus, r#"SEARCH CONCEPT "Alice" MODE "semantic""#).await;
    assert_eq!(
        response.error.as_ref().unwrap().code.as_str(),
        "SearchModeUnsupported"
    );

    // And a kind with no text index says so rather than returning nothing.
    let no_index = run(&nexus, r#"SEARCH ASSERTION "anything""#).await;
    assert_eq!(
        no_index.error.as_ref().unwrap().code.as_str(),
        "SearchIndexUnavailable"
    );
}

#[tokio::test]
async fn validate_reports_legality_without_promising_a_commit() {
    let nexus = fresh("validate").await;
    let good = ok(
        &nexus,
        r#"VALIDATE KQL "FIND(?c) WHERE { ?c CONCEPT {type: \"Person\"} }""#,
    )
    .await;
    assert_eq!(good["valid"], true);
    assert!(good["note"].as_str().unwrap().contains("may still fail"));

    let bad = ok(&nexus, r#"VALIDATE KQL "FIND(?c) WHERE {""#).await;
    assert_eq!(bad["valid"], false);
    assert!(!bad["violations"].as_array().unwrap().is_empty());

    // Validating a mutation as a query is a language mismatch, not a pass.
    let mismatched = ok(&nexus, r#"VALIDATE KQL "ARCHIVE \"C-1\"""#).await;
    assert_eq!(mismatched["valid"], false);
    assert_eq!(mismatched["violations"][0]["code"], "LanguageMismatch");
}

#[tokio::test]
async fn preview_computes_an_effect_and_commits_nothing() {
    let nexus = fresh("preview").await;
    let before = ok(&nexus, "DESCRIBE PRIMER").await["contents"]["concept"].clone();

    let previewed = ok(
        &nexus,
        r#"PREVIEW KML "CREATE CONCEPT ?x { TYPE \"Person\" NAME \"Ghost\" }""#,
    )
    .await;
    assert_eq!(previewed["would_commit"], true);
    assert_eq!(previewed["receipt"]["status"], "no_effect");
    assert_eq!(previewed["effect"]["changes"].as_array().unwrap().len(), 1);

    let after = ok(&nexus, "DESCRIBE PRIMER").await["contents"]["concept"].clone();
    assert_eq!(before, after, "a preview establishes no durable state");

    // A preview of something illegal reports the failure instead of hiding it.
    let refused = ok(
        &nexus,
        r#"PREVIEW KML "CREATE CONCEPT ?x { TYPE \"Spaceship\" NAME \"X\" }""#,
    )
    .await;
    assert_eq!(refused["would_commit"], false);
    assert_eq!(refused["error"]["code"], "SchemaSymbolNotFound");
}

#[tokio::test]
async fn verify_refuses_rather_than_reporting_an_unchecked_artifact_as_valid() {
    // Integrity is the one layer a caller trusts to be paranoid on its behalf.
    let nexus = fresh("verify").await;

    // An unreadable artifact is a parse failure, not a pass.
    let garbage = run(&nexus, r#"VERIFY CAPSULE "not a capsule""#).await;
    assert_eq!(
        garbage.error.as_ref().unwrap().code.as_str(),
        "ArtifactParseError"
    );

    // The kinds this engine cannot check say so rather than answering.
    for command in [
        r#"VERIFY RECEIPT "x""#,
        r#"VERIFY BLOB "x""#,
        r#"VERIFY SCHEMA PACKAGE "x""#,
    ] {
        let response = run(&nexus, command).await;
        assert_eq!(
            response.error.as_ref().unwrap().code.as_str(),
            "UnsupportedCapability",
            "for {command}"
        );
        assert!(
            response
                .error
                .as_ref()
                .unwrap()
                .message
                .contains("defeat the purpose")
        );
    }
}

#[tokio::test]
async fn history_and_changes_page_the_transaction_journal() {
    let nexus = seeded("history").await;
    ok(
        &nexus,
        r#"CREATE CONCEPT ?bob { TYPE "Person" NAME "Bob" }"#,
    )
    .await;

    let space_history = ok(&nexus, "HISTORY SPACE").await;
    let entries = space_history.as_array().unwrap();
    assert!(entries.len() >= 2);
    // Chronological, by the coordinate the whole history model rests on.
    let seqs: Vec<u64> = entries
        .iter()
        .map(|e| e["space_seq"].as_u64().unwrap())
        .collect();
    let mut sorted = seqs.clone();
    sorted.sort_unstable();
    assert_eq!(seqs, sorted);

    // One element's chronology is narrowed to that element's changes.
    let found = ok(&nexus, r#"FIND(?c) WHERE { ?c CONCEPT {name: "Bob"} }"#).await;
    let bob = found.as_array().unwrap()[0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let request = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "operations": [{"command": "HISTORY ELEMENT :id", "parameters": {"id": bob.clone()}}]
    }))
    .unwrap();
    let parsed = request.operations[0].parse().unwrap();
    let response = nexus
        .execute(parsed, &request, &request.operations[0])
        .await;
    let element_history = response.first_result().unwrap().as_array().unwrap();
    assert_eq!(element_history.len(), 1);
    assert_eq!(element_history[0]["changes"][0]["id"], bob);

    // CHANGES resumes from a coordinate rather than replaying from the start.
    let all = ok(&nexus, "CHANGES AFTER SEQ 0").await;
    let count = all.as_array().unwrap().len();
    assert!(count >= 2);
    let after_first = ok(&nexus, "CHANGES AFTER SEQ 1").await;
    assert_eq!(after_first.as_array().unwrap().len(), count - 1);
}

#[tokio::test]
async fn a_transaction_is_recoverable_by_id_and_by_idempotency_key() {
    // Spec §80.4: recovering a lost response is a lookup, and a key that never
    // committed must say so rather than looking like a failure to find.
    let nexus = fresh("transactions").await;
    let request = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "execution": {"mode": "independent", "idempotency_key": "formation:1"},
        "operations": [{"command": r#"CREATE CONCEPT ?x { TYPE "Person" NAME "Alice" }"#}]
    }))
    .unwrap();
    let parsed = request.operations[0].parse().unwrap();
    let response = nexus
        .execute(parsed, &request, &request.operations[0])
        .await;
    let tx_id = response.receipt.as_ref().unwrap().tx_id.clone().unwrap();

    let by_id = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "operations": [{"command": "DESCRIBE TRANSACTION :tx", "parameters": {"tx": tx_id.clone()}}]
    }))
    .unwrap();
    let parsed = by_id.operations[0].parse().unwrap();
    let described = nexus.execute(parsed, &by_id, &by_id.operations[0]).await;
    let body = described.first_result().unwrap();
    assert_eq!(body["tx_id"], tx_id);
    assert_eq!(body["status"], "committed");

    let by_key = ok(
        &nexus,
        r#"DESCRIBE TRANSACTION BY IDEMPOTENCY KEY "formation:1""#,
    )
    .await;
    assert_eq!(by_key["tx_id"], tx_id);

    // A key nobody used tells the caller it is safe to send again.
    let unknown = run(
        &nexus,
        r#"DESCRIBE TRANSACTION BY IDEMPOTENCY KEY "never-used""#,
    )
    .await;
    let error = unknown.error.as_ref().unwrap();
    assert_eq!(error.code.as_str(), "TransactionUnknown");
    assert!(error.message.contains("safe to send again"));
}

#[tokio::test]
async fn a_snapshot_reports_a_coordinate_a_later_read_can_bind_to() {
    // A token promises a later read can be bound to this coordinate. The
    // engine keeps that promise now, so it issues one; `tests/history.rs`
    // exercises the binding itself.
    let nexus = seeded("snapshot").await;
    let snapshot = ok(&nexus, "SNAPSHOT").await;
    let seq = snapshot["snapshot_seq"].as_u64().unwrap();
    assert!(seq >= 1);
    assert!(snapshot["snapshot_token"].is_string());

    let historical = ok(&nexus, "SNAPSHOT AS OF SEQ 1").await;
    assert_eq!(historical["snapshot_seq"], serde_json::json!(1));

    // A coordinate the Space has not reached is refused rather than rounded
    // down to the present.
    let ahead = run(&nexus, &format!("SNAPSHOT AS OF SEQ {}", seq + 50)).await;
    assert_eq!(
        ahead
            .results
            .iter()
            .find_map(|result| result.error.as_ref())
            .or(ahead.error.as_ref())
            .unwrap()
            .code
            .as_str(),
        "HistoricalSnapshotUnavailable"
    );
}

#[tokio::test]
async fn trust_and_access_refuse_rather_than_reporting_an_empty_judgement() {
    // An empty trust report reads as "nothing is trusted" and an empty access
    // report as "nothing is permitted". Both are claims this engine cannot
    // make.
    let nexus = fresh("governance").await;
    for command in ["DESCRIBE TRUST", "DESCRIBE ACCESS"] {
        let response = run(&nexus, command).await;
        assert_eq!(
            response.error.as_ref().unwrap().code.as_str(),
            "UnsupportedCapability",
            "for {command}"
        );
    }
}

#[tokio::test]
async fn the_epistemic_policy_is_introspectable_before_it_is_used() {
    let nexus = fresh("policies").await;
    let policies = ok(&nexus, "LIST EPISTEMIC POLICIES").await;
    assert_eq!(policies.as_array().unwrap().len(), 2);

    let baseline = ok(&nexus, r#"DESCRIBE EPISTEMIC POLICY "baseline""#).await;
    assert_eq!(baseline["id"], "kip:policy:baseline");
    assert_eq!(baseline["accept_threshold"], 0.7);
    assert!(
        baseline["eligible_modes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|m| m != "hypothetical")
    );
    assert!(
        baseline["notes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n.as_str().unwrap().contains("does not grant trust"))
    );

    let capability = ok(&nexus, "DESCRIBE PROJECTION CAPABILITY").await;
    assert!(
        capability["missing_stages"]
            .as_array()
            .unwrap()
            .contains(&json!("trust_evaluation"))
    );
}

#[tokio::test]
async fn execution_context_states_what_the_next_read_will_see() {
    let nexus = seeded("execution_context").await;
    let context = ok(&nexus, "DESCRIBE EXECUTION CONTEXT").await;
    assert_eq!(context["space_id"], DEFAULT_SPACE);
    assert!(context["space_seq"].as_u64().unwrap() >= 1);
    assert_eq!(context["schema_environment_version"], 1);
    assert!(
        context["read_basis"]
            .as_str()
            .unwrap()
            .contains("committed state")
    );
}
