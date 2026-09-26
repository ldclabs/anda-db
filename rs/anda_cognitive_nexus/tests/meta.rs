//! META tests: orientation, introspection, grounding, history.

use anda_cognitive_nexus::{
    CognitiveNexus,
    nexus::DEFAULT_SPACE,
    schema::{PackageState, SchemaLock, SchemaPackage},
    store::space::SpaceDraft,
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
    nexus
        .install_package(
            &SchemaPackage::parse(include_str!("support/options.json")).unwrap(),
            "test",
        )
        .await
        .unwrap();
    lock.packages
        .insert("kip://test/options".into(), "1.0.0".into());
    lock.states
        .insert("kip://test/options".into(), PackageState::Active);
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
            CREATE CONCEPT ?dark { TYPE "Option" NAME "Dark mode theme" }
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
        "capsule_restore_mode",
        "capsule_signatures",
        "retention_policy",
    ] {
        assert!(unsupported.contains(&expected), "missing {expected}");
    }
    assert!(
        caps["supported"]["governance"]["default_deny"] == json!(true),
        "and what is enforced is reported as supported"
    );
    assert_eq!(
        caps["supported"]["lifecycle"]["purge"]["default_reference_policy"], "deny_if_referenced",
        "a destructive default has to be the conservative one, and has to say so"
    );
    // Each gap says why, not just that.
    for entry in caps["unsupported"].as_array().unwrap() {
        assert!(!entry["reason"].as_str().unwrap().is_empty());
    }
}

#[tokio::test]
async fn describe_answers_with_canonical_identity_not_the_local_name() {
    // Spec §88.6. A caller that stored the local name would have stored
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
    // The Profile and the test options package.
    assert_eq!(packages.as_array().unwrap().len(), 2);
    assert!(
        packages
            .as_array()
            .unwrap()
            .iter()
            .all(|package| package["status"] == "active")
    );

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
    let typed = ok(&nexus, r#"SEARCH CONCEPT "Dark" WITH TYPE "Option""#).await;
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
    //
    // `UnsupportedCapability`, not `SearchIndexUnavailable`: the second carries
    // the `safe_same_request` retry class, and telling an Agent to re-run a
    // search that can never work is a retry loop wearing a diagnosis.
    let no_index = run(&nexus, r#"SEARCH ASSERTION "anything""#).await;
    assert_eq!(
        no_index.error.as_ref().unwrap().code.as_str(),
        "UnsupportedCapability"
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
    let mismatched = ok(
        &nexus,
        r#"VALIDATE KQL "TRANSITION \"C-1\" TO \"archived\"""#,
    )
    .await;
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

    // The other two kinds are checked too, and an unreadable one is a parse
    // failure rather than a pass.
    for command in [r#"VERIFY RECEIPT "x""#, r#"VERIFY SCHEMA PACKAGE "x""#] {
        let response = run(&nexus, command).await;
        assert_eq!(
            response.error.as_ref().unwrap().code.as_str(),
            "ArtifactParseError",
            "for {command}"
        );
    }
    // §69.1 names three targets; the two the draft once listed beside them
    // are gone from the grammar.
    assert!(anda_kip::parse_kip(r#"VERIFY BLOB "x""#).is_err());
    assert!(anda_kip::parse_kip(r#"VERIFY CHECKPOINT "x""#).is_err());
}

async fn run_with(nexus: &CognitiveNexus, command: &str, params: Json) -> anda_kip::Response {
    let request: Request = serde_json::from_value(json!({
        "kip": "2.0",
        "operations": [{"command": command, "parameters": params}]
    }))
    .unwrap();
    let parsed = request.operations[0].parse().unwrap();
    nexus
        .execute(parsed, &request, &request.operations[0])
        .await
}

#[tokio::test]
async fn verify_receipt_recomputes_the_digest_and_attests_the_journal() {
    // §69.1 / §33.2: intact is one question, "did this runtime commit it" is
    // another, and the answer says which it is answering.
    let nexus = fresh("verify_receipt").await;
    let committed = run(
        &nexus,
        r#"CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }"#,
    )
    .await;
    let receipt = committed.results[0]
        .receipt
        .clone()
        .expect("a state-changing operation carries a Receipt");
    let receipt_json = serde_json::to_value(&receipt).unwrap();

    let verified = run_with(&nexus, "VERIFY RECEIPT :r", json!({"r": receipt_json})).await;
    assert_eq!(
        verified.status,
        TopLevelStatus::Succeeded,
        "{:?}",
        verified.error
    );
    let report = verified.results[0].result.clone().unwrap();
    assert_eq!(report["valid"], true);
    assert_eq!(report["receipt_digest"], receipt_json["receipt_digest"]);
    assert_eq!(report["attestation"]["known"], true);
    assert_eq!(report["attestation"]["matches"], true);
    assert_eq!(report["signature"]["checked"], false);

    // The artifact text form is the same artifact.
    let as_text = run_with(
        &nexus,
        "VERIFY RECEIPT :r",
        json!({"r": receipt_json.to_string()}),
    )
    .await;
    assert_eq!(
        as_text.status,
        TopLevelStatus::Succeeded,
        "{:?}",
        as_text.error
    );

    // Altered after sealing: the content no longer digests to what it declares.
    let mut tampered = receipt_json.clone();
    tampered["space_seq"] = json!(999);
    let refused = run_with(&nexus, "VERIFY RECEIPT :r", json!({"r": tampered})).await;
    assert_eq!(
        refused.error.as_ref().unwrap().code.as_str(),
        "DigestMismatch"
    );

    // Intact, but naming a transaction this journal never saw.
    let other = fresh("verify_receipt_other").await;
    run(&other, r#"CREATE CONCEPT ?c { TYPE "Person" NAME "Bob" }"#).await;
    let second = run(
        &other,
        r#"CREATE CONCEPT ?c { TYPE "Person" NAME "Carol" }"#,
    )
    .await;
    let foreign = serde_json::to_value(second.results[0].receipt.as_ref().unwrap()).unwrap();
    let unattested = run_with(&nexus, "VERIFY RECEIPT :r", json!({"r": foreign})).await;
    let report = unattested.results[0].result.clone().unwrap();
    assert_eq!(report["valid"], true);
    assert_eq!(report["attestation"]["known"], false);
}

#[tokio::test]
async fn verify_schema_package_checks_the_declared_digest_and_the_installed_artifact() {
    let nexus = fresh("verify_package").await;
    let verified = run_with(
        &nexus,
        "VERIFY SCHEMA PACKAGE :p",
        json!({"p": COGNITIVE_MEMORY}),
    )
    .await;
    assert_eq!(
        verified.status,
        TopLevelStatus::Succeeded,
        "{:?}",
        verified.error
    );
    let report = verified.results[0].result.clone().unwrap();
    assert_eq!(report["valid"], true);
    assert_eq!(
        report["package_ref"],
        "kip://profiles/cognitive-memory@2.0.0"
    );
    assert_eq!(report["declared"]["checked"], true);
    assert_eq!(report["installed"]["known"], true);
    assert_eq!(report["installed"]["matches"], true);

    // A byte changed after sealing fails the declared digest (§20.11).
    let tampered = COGNITIVE_MEMORY.replacen("\"description\"", "\"description \"", 1);
    assert_ne!(tampered, COGNITIVE_MEMORY);
    let refused = run_with(&nexus, "VERIFY SCHEMA PACKAGE :p", json!({"p": tampered})).await;
    assert_eq!(
        refused.error.as_ref().unwrap().code.as_str(),
        "DigestMismatch"
    );

    // Intact on its own terms, but not the content installed under that name.
    let mut artifact: Json = serde_json::from_str(COGNITIVE_MEMORY).unwrap();
    artifact.as_object_mut().unwrap().remove("integrity");
    artifact["manifest"]["description"] = json!("a different package under the same reference");
    let differing = run_with(&nexus, "VERIFY SCHEMA PACKAGE :p", json!({"p": artifact})).await;
    assert_eq!(
        differing.status,
        TopLevelStatus::Succeeded,
        "{:?}",
        differing.error
    );
    let report = differing.results[0].result.clone().unwrap();
    assert_eq!(report["declared"]["checked"], false);
    assert_eq!(report["installed"]["known"], true);
    assert_eq!(report["installed"]["matches"], false);
    assert_eq!(report["valid"], false);
}

#[tokio::test]
async fn search_hits_carry_a_snippet_of_the_matched_text() {
    // §66.4: a safe snippet beside the element, windowed around the term.
    let nexus = seeded("search_snippet").await;
    let result = ok(&nexus, r#"SEARCH CONCEPT "Anderson""#).await;
    let hit = &result["hits"][0];
    let snippet = hit["snippet"].as_str().expect("a snippet");
    assert!(snippet.contains("Anderson"), "{snippet}");
    assert!(snippet.chars().count() <= 200);
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
    // §75: a single operation carries its own Receipt; the top-level slot is
    // reserved for an `atomic` transaction, which this engine does not run.
    let tx_id = response.results[0]
        .receipt
        .as_ref()
        .unwrap()
        .tx_id
        .clone()
        .unwrap();

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

    // Unknown and hidden transactions have the same non-disclosing answer.
    let unknown = run(
        &nexus,
        r#"DESCRIBE TRANSACTION BY IDEMPOTENCY KEY "never-used""#,
    )
    .await;
    let error = unknown.error.as_ref().unwrap();
    assert_eq!(error.code.as_str(), "TransactionUnknown");
    // An unavailable lookup must not distinguish hidden history from absence
    // or promise that a fresh mutation is safe after the caller lost visibility.
    assert_eq!(error.message, "transaction unavailable");
}

#[tokio::test]
async fn a_snapshot_reports_a_coordinate_a_later_read_can_bind_to() {
    // §68: the bare `SNAPSHOT` statement became `DESCRIBE SNAPSHOT`, which
    // answers a coordinate — the sequence, the transaction that committed it,
    // its commit time and the schema environment in force. A token promises a
    // later read can be bound to it; `tests/history.rs` exercises the binding.
    let nexus = seeded("snapshot").await;
    let snapshot = ok(&nexus, "DESCRIBE SNAPSHOT").await;
    let seq = snapshot["space_seq"].as_u64().unwrap();
    assert!(seq >= 1);
    assert!(snapshot["snapshot_token"].is_string());
    assert_eq!(snapshot["space_id"], DEFAULT_SPACE);
    assert!(snapshot["tx_id"].is_string());
    assert!(snapshot["committed_at"].is_string());
    assert_eq!(snapshot["schema_environment_version"], 1);

    let historical = ok(&nexus, "DESCRIBE SNAPSHOT AS OF SEQ 1").await;
    assert_eq!(historical["space_seq"], serde_json::json!(1));

    // A coordinate the Space has not reached is refused rather than rounded
    // down to the present.
    let ahead = run(&nexus, &format!("DESCRIBE SNAPSHOT AS OF SEQ {}", seq + 50)).await;
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

/// §48.1 dropped `AS OF TIME` from KQL, so wall-clock time now enters a
/// historical read through one door only: `DESCRIBE SNAPSHOT AT TIME` resolves
/// an instant to the last sequence committed at or before it, and the caller
/// carries that sequence into `AS OF SEQ`. The engine never guesses which of
/// several sequences an instant meant.
#[tokio::test]
async fn an_instant_resolves_to_the_sequence_it_names() {
    let nexus = seeded("snapshot_at_time").await;
    let head = ok(&nexus, "DESCRIBE SNAPSHOT").await;
    let seq = head["space_seq"].as_u64().unwrap();
    let committed_at = head["committed_at"].as_str().unwrap().to_string();

    // The instant of the head commit resolves to the head itself: `at or
    // before` includes the commit that happened exactly then.
    let resolved = ok(
        &nexus,
        &format!(r#"DESCRIBE SNAPSHOT AT TIME "{committed_at}""#),
    )
    .await;
    assert_eq!(resolved["space_seq"], serde_json::json!(seq));

    // An instant later than every commit still names the last sequence
    // committed at or before it, which is the head — not a future coordinate.
    let later = ok(
        &nexus,
        r#"DESCRIBE SNAPSHOT AT TIME "2999-01-01T00:00:00.000Z""#,
    )
    .await;
    assert_eq!(later["space_seq"], serde_json::json!(seq));

    // An instant before the Space existed is sequence 0 — an empty Space, not
    // an error: this engine keeps every version, so no instant falls below a
    // retention floor.
    let before = ok(
        &nexus,
        r#"DESCRIBE SNAPSHOT AT TIME "1990-01-01T00:00:00.000Z""#,
    )
    .await;
    assert_eq!(before["space_seq"], serde_json::json!(0));
}

#[tokio::test]
async fn trust_refuses_rather_than_reporting_an_empty_judgement() {
    // An empty trust report reads as "nothing is trusted", which is a claim
    // this engine cannot make: it evaluates no source trust at all.
    //
    // `DESCRIBE ACCESS` used to refuse for the parallel reason and no longer
    // does, because there is now a Governance plane with something true to say.
    let nexus = fresh("governance").await;
    let response = ok(&nexus, "DESCRIBE TRUST").await;
    assert_eq!(response["model"], "protected-actor-weights-v1");

    let access = ok(&nexus, "DESCRIBE ACCESS").await;
    assert_eq!(access["principal"]["id"], "kip:principal:system");
    assert_eq!(access["is_owner"], true);
    assert!(
        access["permissions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|permission| permission == "read"),
        "an owner holds the read permission"
    );
}

#[tokio::test]
async fn the_epistemic_policy_is_introspectable_before_it_is_used() {
    let nexus = fresh("policies").await;
    let policies = ok(&nexus, "LIST EPISTEMIC POLICIES").await;
    // The baseline, the forecast, the standard memory policy (§21.13) and
    // the structural baseline (§21.10).
    assert_eq!(policies.as_array().unwrap().len(), 4);
    assert!(
        policies
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["id"] == "kip:memory-default")
    );

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

    // §67.4 replaced `DESCRIBE PROJECTION CAPABILITY` with one registry every
    // engine spells the same way, so a client asks the same question of any
    // engine. This one runs the structural baseline (§21.10) and evaluates no
    // source trust, which is `weighted_projection: false` — a stated absence,
    // not a silence a caller has to interpret.
    let registry = ok(&nexus, "DESCRIBE CAPABILITIES").await["supported"]["registry"].clone();
    assert_eq!(registry["weighted_projection"], json!(true));
    // Level requirements are no longer registry entries (§67.4).
    assert!(registry.get("belief_slot").is_none());
    assert!(registry.get("materialized_projection").is_none());
}

/// §67.4 is a closed registry: a `requires` block names an entry from it, and
/// an engine MUST NOT rename one. So every name the section lists is reported,
/// spelled exactly as the section spells it, whether or not this engine has it.
#[tokio::test]
async fn the_capability_registry_names_everything_the_spec_registered() {
    let nexus = fresh("registry").await;
    let registry = ok(&nexus, "DESCRIBE CAPABILITIES").await["supported"]["registry"].clone();
    for name in [
        "serializable_isolation",
        "idempotency_retention",
        "historical_reads",
        "historical_search",
        "semantic_search",
        "hybrid_search",
        "search_index_freshness",
        "weighted_projection",
        "signed_receipts",
        "streaming",
        "artifacts",
        "change_stream",
        "filtered_delivery",
        "watch_evaluation",
        "exposure_log",
        "draft_vocabulary",
        "identity_repair",
        "recording_repair",
        "capsule_export",
        "capsule_import",
        "capsule_signatures",
        "derive_permission",
        "record_outcome_permission",
    ] {
        assert!(
            !registry[name].is_null(),
            "§67.4 registers {name}, and an engine may add entries but not drop one"
        );
    }
    // An entry MAY carry a value rather than a bare flag; §67.4 gives
    // `idempotency_retention` a window as its example.
    assert!(registry["idempotency_retention"].is_object());
    assert!(registry["search_index_freshness"].is_object());
}

/// §68 removed `DESCRIBE EXECUTION CONTEXT`: what the next read will see is a
/// snapshot coordinate, and there is now one statement that reports one. The
/// question survived the statement, so the answer must still carry it — which
/// Space, which sequence, and which schema environment is in force there.
#[tokio::test]
async fn the_head_coordinate_states_what_the_next_read_will_see() {
    let nexus = seeded("execution_context").await;
    let context = ok(&nexus, "DESCRIBE SNAPSHOT").await;
    assert_eq!(context["space_id"], DEFAULT_SPACE);
    assert!(context["space_seq"].as_u64().unwrap() >= 1);
    assert_eq!(context["schema_environment_version"], 1);

    // And it really is the head: a write moves it, so a caller that reads the
    // coordinate twice can tell that the Space advanced under it.
    let before = context["space_seq"].as_u64().unwrap();
    ok(&nexus, r#"CREATE CONCEPT ?x { TYPE "Person" NAME "Dana" }"#).await;
    let after = ok(&nexus, "DESCRIBE SNAPSHOT").await["space_seq"]
        .as_u64()
        .unwrap();
    assert!(after > before, "the head coordinate advances with a commit");
}

// ---------------------------------------------------------------------------
// LIST DEPENDENTS (§63.5)
// ---------------------------------------------------------------------------

/// A Space with two derivation hops recorded as Activity provenance.
///
/// ```text
/// Event ──inputs──▸ consolidation ──outputs──▸ Insight
///                   Insight ──inputs──▸ compilation ──outputs──▸ Skill
/// ```
async fn derived(name: &str) -> CognitiveNexus {
    let nexus = fresh(name).await;
    ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?event {
                TYPE "Event"
                NAME "Migration meeting"
                SET ATTRIBUTES {summary: "The team agreed to migrate on Friday"}
            }
            CREATE CONCEPT ?insight {
                TYPE "Insight"
                NAME "Migrations need a rollback plan"
                SET ATTRIBUTES {summary: "Every migration ships with a rollback"}
            }
            CREATE ACTIVITY ?consolidate {
                SET FIELDS {activity_class: "semantic_consolidation", status: "completed"}
                SET STRUCTURAL {
                    ("inputs", ?event)
                    ("outputs", ?insight)
                }
            }
        }"#,
    )
    .await;
    ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?skill {
                TYPE "Insight"
                NAME "Plan a migration"
                SET ATTRIBUTES {
                    summary: "Write the rollback first"
                }
            }
            CREATE ACTIVITY ?compile {
                SET FIELDS {activity_class: "procedural_consolidation", status: "completed"}
                SET STRUCTURAL {
                    ("inputs", "C-2")
                    ("outputs", ?skill)
                }
            }
        }"#,
    )
    .await;
    nexus
}

#[tokio::test]
async fn dependents_walk_the_provenance_dag_in_the_derived_direction() {
    // §63.5: X ∈ Activity.inputs → that Activity → each element in its outputs.
    // This is the read §57.5 asks a Brain to make after revising a root: the
    // cognition built on the old claim is still active state, and it has to be
    // findable before it can be reviewed.
    let nexus = derived("dependents").await;

    let one = ok(&nexus, r#"LIST DEPENDENTS "C-1""#).await;
    let rows = one.as_array().unwrap();
    assert_eq!(rows.len(), 1, "{rows:#?}");
    assert_eq!(rows[0]["id"], "C-2");
    assert_eq!(rows[0]["kind"], "concept");
    assert_eq!(rows[0]["distance"], 1);
    assert_eq!(
        rows[0]["via"]["activity"], "X-1",
        "the row names the Activity it was reached through"
    );

    // DEPTH is what turns one hop into the closure. Default is one hop, so the
    // Skill two derivations away is out of reach until it is asked for.
    let two = ok(&nexus, r#"LIST DEPENDENTS "C-1" DEPTH 2"#).await;
    let rows = two.as_array().unwrap();
    assert_eq!(rows.len(), 2, "{rows:#?}");
    assert_eq!(rows[1]["id"], "C-3");
    assert_eq!(rows[1]["distance"], 2);
    assert_eq!(rows[1]["via"]["activity"], "X-2");
}

#[tokio::test]
async fn dependents_is_a_read_and_pages_like_every_other_list() {
    let nexus = derived("dependents_paging").await;
    let response = run(&nexus, r#"LIST DEPENDENTS "C-1" DEPTH 2 LIMIT 1"#).await;
    assert_eq!(response.status, TopLevelStatus::Succeeded);
    let page = response.first_result().unwrap().as_array().unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0]["id"], "C-2");

    // Reachability is provenance topology, not judgment (§57.5): nothing about
    // the listed element changed.
    let insight = ok(
        &nexus,
        r#"FIND(?c._system.state, ?c._system.version) WHERE { ?c CONCEPT {id: "C-2"} }"#,
    )
    .await;
    let rows = insight.as_array().unwrap();
    assert_eq!(rows[0][0], "active");
    assert_eq!(rows[0][1], 1);
}

#[tokio::test]
async fn a_transformation_with_no_activity_lineage_is_not_discoverable() {
    // §63.5's own caveat, and the reason the Profile's consolidation guidance
    // insists on citing the inputs you actually relied on: an uncited input is
    // an invisible dependency, and this command cannot invent the edge.
    let nexus = fresh("dependents_unlinked").await;
    ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?event {
                TYPE "Event"
                NAME "Meeting"
                SET ATTRIBUTES {summary: "A meeting happened"}
            }
            CREATE CONCEPT ?insight {
                TYPE "Insight"
                NAME "Undeclared derivation"
                SET ATTRIBUTES {summary: "Derived from the meeting, but nobody said so"}
            }
        }"#,
    )
    .await;
    let rows = ok(&nexus, r#"LIST DEPENDENTS "C-1" DEPTH 4"#).await;
    assert!(rows.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn an_unknown_dependents_root_is_answered_as_an_absent_one() {
    // §30.4: omission is indistinguishable from absence. An error here would
    // turn the command into an existence oracle.
    let nexus = derived("dependents_absent").await;
    let rows = ok(&nexus, r#"LIST DEPENDENTS "C-999""#).await;
    assert!(rows.as_array().unwrap().is_empty());

    // A string that is not an element id at all is a different mistake, and is
    // reported as one.
    let response = run(&nexus, r#"LIST DEPENDENTS "not-an-id""#).await;
    assert_eq!(response.status, TopLevelStatus::Failed);
    assert_eq!(
        response.error.as_ref().unwrap().code.as_str(),
        "InvalidIdentifier"
    );
}

#[tokio::test]
async fn search_thresholds_are_normalized_inclusive_and_typed() {
    let nexus = seeded("search_contract").await;
    let before = nexus.store.get_space(DEFAULT_SPACE).await.unwrap().seq;
    let first = ok(&nexus, r#"SEARCH CONCEPT "Anderson""#).await;
    let score = first["hits"][0]["retrieval"]["score"].as_f64().unwrap();
    assert!(score > 0.0 && score < 1.0);
    assert_eq!(
        first["hits"][0]["score"],
        first["hits"][0]["retrieval"]["score"]
    );
    let mut request = Request::single(r#"SEARCH CONCEPT "Anderson" THRESHOLD :threshold"#);
    for threshold in [
        json!(score),
        json!(1.0),
        json!(-0.1),
        json!(1.1),
        json!("0.5"),
    ] {
        request.parameters = Some(serde_json::Map::from_iter([(
            "threshold".into(),
            threshold.clone(),
        )]));
        let response = nexus
            .execute(
                request.operations[0].parse().unwrap(),
                &request,
                &request.operations[0],
            )
            .await;
        if threshold == json!(score) {
            assert_eq!(response.first_result().unwrap()["hits"], first["hits"]);
        } else if threshold == json!(1.0) {
            assert_eq!(response.first_result().unwrap()["hits"], json!([]));
        } else {
            assert_eq!(response.status, TopLevelStatus::Failed);
        }
    }
    for command in [
        r#"SEARCH CONCEPT "Alice" WITH PREDICATE "prefers""#,
        r#"SEARCH PROPOSITION "prefers" WITH TYPE "Person""#,
        r#"SEARCH CONCEPT "Alice" LIMIT "1""#,
    ] {
        assert_eq!(
            run(&nexus, command).await.status,
            TopLevelStatus::Failed,
            "{command}"
        );
    }
    assert_eq!(
        nexus.store.get_space(DEFAULT_SPACE).await.unwrap().seq,
        before
    );
}

#[tokio::test]
async fn search_pages_refuse_a_changed_index() {
    let nexus = seeded("search_cursor_contract").await;
    ok(
        &nexus,
        r#"CREATE CONCEPT ?c {TYPE "Person" NAME "Alice Again"}"#,
    )
    .await;
    let page = run(&nexus, r#"SEARCH CONCEPT "Alice" LIMIT 1"#).await;
    let cursor = page.results[0].next_cursor.as_ref().unwrap();
    ok(
        &nexus,
        r#"CREATE CONCEPT ?c {TYPE "Person" NAME "Alice New"}"#,
    )
    .await;
    let response = run(
        &nexus,
        &format!(r#"SEARCH CONCEPT "Alice" LIMIT 1 CURSOR "{cursor}""#),
    )
    .await;
    assert_eq!(response.error.unwrap().code.as_str(), "CursorExpired");
}

/// Memory Interface §2: a raw Nexus MUST NOT advertise a binding its
/// connected Brain cannot serve, so `memory_interface`,
/// `durable_brain_runtime` and `receiver_fencing` are `false` until the host
/// declares them — and then `DESCRIBE CAPABILITIES`, the Primer and every
/// `requires` block answer with the declaration.
#[tokio::test]
async fn host_capabilities_are_the_hosts_to_declare() {
    use anda_cognitive_nexus::meta::HostCapabilities;
    use anda_kip::memory::binding::{Budget, Bundle, Descriptor};

    let nexus = fresh("host_capabilities").await;
    let requires = |name: &str| {
        let request: Request = serde_json::from_value(json!({
            "kip": "2.0",
            "requires": {name: true},
            "operations": [{"command": "DESCRIBE SPACE"}]
        }))
        .unwrap();
        let parsed = anda_kip::parse_kip("DESCRIBE SPACE").unwrap();
        let nexus = nexus.clone();
        async move {
            nexus
                .execute(parsed, &request, &request.operations[0])
                .await
                .status
        }
    };
    let registry = ok(&nexus, "DESCRIBE CAPABILITIES").await["supported"]["registry"].clone();
    for name in [
        "memory_interface",
        "durable_brain_runtime",
        "receiver_fencing",
    ] {
        assert_eq!(registry[name], json!(false), "{name}");
    }
    assert_ne!(
        requires("memory_interface").await,
        TopLevelStatus::Succeeded
    );
    assert!(
        ok(&nexus, "DESCRIBE PRIMER")
            .await
            .get("extensions")
            .is_none()
    );

    let descriptor = Descriptor {
        kip_memory: "2.0".into(),
        bundles: vec![Bundle::MemoryBasic],
        default_scope: None,
        default_budget: Budget {
            max_output_tokens: Some(4096),
            deadline_ms: Some(30_000),
            tokenizer: None,
        },
        tokenizer: "o200k_base".into(),
        minimum_response_tokens: 256,
        default_space: None,
    };
    // A level this engine's conformance claim cannot carry is refused.
    let mut experience = descriptor.clone();
    experience.bundles.push(Bundle::MemoryExperience);
    let claims_memory = ok(&nexus, "DESCRIBE CAPABILITIES").await["profiles"]
        .as_array()
        .unwrap()
        .contains(&json!("KIP-CognitiveMemory"));
    assert_eq!(
        nexus
            .set_host_capabilities(HostCapabilities {
                memory_interface: Some(experience),
                ..Default::default()
            })
            .is_ok(),
        claims_memory
    );

    nexus
        .set_host_capabilities(HostCapabilities {
            memory_interface: Some(descriptor),
            durable_brain_runtime: true,
            receiver_fencing: false,
        })
        .unwrap();
    let registry = ok(&nexus, "DESCRIBE CAPABILITIES").await["supported"]["registry"].clone();
    assert_eq!(
        registry["memory_interface"]["bundles"],
        json!(["memory_basic"])
    );
    assert_eq!(registry["durable_brain_runtime"], json!(true));
    assert_eq!(registry["receiver_fencing"], json!(false));
    assert_eq!(
        requires("memory_interface").await,
        TopLevelStatus::Succeeded
    );
    assert_eq!(
        requires("durable_brain_runtime").await,
        TopLevelStatus::Succeeded
    );
    assert_ne!(
        requires("receiver_fencing").await,
        TopLevelStatus::Succeeded
    );
    let primer = ok(&nexus, "DESCRIBE PRIMER").await;
    assert_eq!(
        primer["extensions"]["memory_interface"]["kip_memory"],
        "2.0"
    );
}

#[tokio::test]
async fn search_top_k_preserves_ties_and_invalidates_cached_scopes() {
    let nexus = fresh("search_cached_scope").await;
    let mut command = String::from("MUTATE {\n");
    for i in 0..30 {
        command.push_str(&format!(
            "CREATE CONCEPT ?p{i} {{ TYPE \"Person\" NAME \"sharedword\" }}\n"
        ));
    }
    command.push('}');
    ok(&nexus, &command).await;
    let query = r#"SEARCH CONCEPT "sharedword" WITH TYPE "Person" LIMIT 5"#;
    for _ in 0..2 {
        let result = ok(&nexus, query).await;
        let ids: Vec<_> = result["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|hit| hit["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["C-1", "C-10", "C-11", "C-12", "C-13"]);
    }
    ok(&nexus, r#"UPDATE "C-1" SET FIELDS {name: "differentword"}"#).await;
    let result = ok(&nexus, query).await;
    assert_eq!(result["hits"][0]["id"], "C-10");
    assert_eq!(result["hits"].as_array().unwrap().len(), 5);
}

#[tokio::test]
async fn indexed_search_scopes_survive_writes_in_other_spaces() {
    let nexus = fresh("search_scope_other_space").await;
    ok(
        &nexus,
        r#"CREATE CONCEPT ?p { TYPE "Person" NAME "sharedword" }"#,
    )
    .await;
    let query = r#"SEARCH CONCEPT "sharedword" WITH TYPE "Person" LIMIT 5"#;
    let concepts = nexus.store.elements(anda_kip::ElementKind::Concept);
    let hits = |result: &Json| result["hits"].as_array().unwrap().len();
    assert_eq!(hits(&ok(&nexus, query).await), 1);

    // Another Space filling up with the same word neither invalidates this
    // Space's prepared scope nor reaches its hits.
    nexus
        .store
        .open_or_create_space(SpaceDraft {
            space_id: "kip:space:other".into(),
            owner_principal: "kip:principal:system".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut lock = SchemaLock::default();
    lock.packages
        .insert(PROFILE_ID.to_string(), "2.0.0".to_string());
    lock.states
        .insert(PROFILE_ID.to_string(), PackageState::Active);
    nexus
        .activate_schema("kip:space:other", lock)
        .await
        .unwrap();
    let command = r#"CREATE CONCEPT ?x { TYPE "Person" NAME "sharedword" }"#;
    let mut other = Request::single(command);
    other.space = Some(anda_kip::SpaceSelector {
        id: Some("kip:space:other".into()),
        ..Default::default()
    });
    let response = nexus
        .execute(
            anda_kip::parse_kip(command).unwrap(),
            &other,
            &other.operations[0],
        )
        .await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "{:#?}",
        response.error
    );
    let scans = concepts.stats().search_count;
    assert_eq!(hits(&ok(&nexus, query).await), 1);
    assert_eq!(concepts.stats().search_count, scans, "the scope was reused");

    // A write in this Space moves its sequence and rebuilds the scope.
    ok(
        &nexus,
        r#"CREATE CONCEPT ?q { TYPE "Person" NAME "sharedword" }"#,
    )
    .await;
    let scans = concepts.stats().search_count;
    assert_eq!(hits(&ok(&nexus, query).await), 2);
    assert!(concepts.stats().search_count > scans);
}
