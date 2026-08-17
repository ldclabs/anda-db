//! Reading the Space at a past coordinate: `AS OF`, `SNAPSHOT`, and the
//! `read.snapshot_token` binding.
//!
//! `AS OF` asks what this Brain *held* then. That is not what was *true* then —
//! `FOR TIME` asks that, and the two are independent axes (§36.1). A test that
//! confused them would pass while the engine answered the wrong question, so
//! the difference is exercised here directly.

use anda_cognitive_nexus::{
    CognitiveNexus,
    nexus::DEFAULT_SPACE,
    profiles::COGNITIVE_MEMORY,
    schema::{PackageState, SchemaLock, SchemaPackage},
};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{Executor, Json, Request, TopLevelStatus};
use object_store::memory::InMemory;
use serde_json::json;
use std::sync::Arc;

const PROFILE_ID: &str = "kip://profiles/cognitive-memory";

async fn nexus(name: &str) -> CognitiveNexus {
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: name.to_string(),
            description: "history tests".to_string(),
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

fn rows(result: &Json) -> &Vec<Json> {
    result.as_array().expect("a KQL result is an array")
}

/// Returns the Space sequence a command committed at.
async fn commit(nexus: &CognitiveNexus, command: &str) -> (u64, String) {
    let response = run(nexus, command).await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "{command}\n{:#?}",
        response.results
    );
    let receipt = response.receipt.expect("a write produces a receipt");
    (
        receipt.space_seq.expect("a commit reports its sequence"),
        receipt.tx_id.expect("a commit reports its transaction"),
    )
}

/// Alice is created, then renamed, then archived — three coordinates with
/// three different answers.
async fn evolving(name: &str) -> (CognitiveNexus, u64, u64, String) {
    let nexus = nexus(name).await;
    let (created, tx) = commit(
        &nexus,
        r#"CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" SET ATTRIBUTES {display_name: "Alice"} }"#,
    )
    .await;
    let (renamed, _) = commit(
        &nexus,
        r#"UPDATE ?c SET FIELDS {name: "Alice A."} WHERE { ?c CONCEPT {name: "Alice"} }"#,
    )
    .await;
    (nexus, created, renamed, tx)
}

#[tokio::test]
async fn as_of_seq_reads_the_state_that_coordinate_held() {
    let (nexus, created, renamed, _) = evolving("as_of_seq").await;

    let now = ok(
        &nexus,
        r#"FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} }"#,
    )
    .await;
    assert_eq!(rows(&now), &vec![json!("Alice A.")]);

    let before_rename = ok(
        &nexus,
        &format!(r#"FIND(?c.name) WHERE {{ ?c CONCEPT {{type: "Person"}} }} AS OF SEQ {created}"#),
    )
    .await;
    assert_eq!(
        rows(&before_rename),
        &vec![json!("Alice")],
        "the coordinate before the rename must still say Alice"
    );

    let after_rename = ok(
        &nexus,
        &format!(r#"FIND(?c.name) WHERE {{ ?c CONCEPT {{type: "Person"}} }} AS OF SEQ {renamed}"#),
    )
    .await;
    assert_eq!(rows(&after_rename), &vec![json!("Alice A.")]);

    // Before it existed at all: an element with no version at that coordinate
    // is not there, rather than there in a state it never had.
    let before_creation = ok(
        &nexus,
        r#"FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} } AS OF SEQ 0"#,
    )
    .await;
    assert!(rows(&before_creation).is_empty());
}

#[tokio::test]
async fn as_of_tx_and_time_name_the_same_kind_of_coordinate() {
    let (nexus, created, _, tx) = evolving("as_of_tx").await;

    let by_tx = ok(
        &nexus,
        &format!(r#"FIND(?c.name) WHERE {{ ?c CONCEPT {{}} }} AS OF TX "{tx}""#),
    )
    .await;
    assert_eq!(rows(&by_tx), &vec![json!("Alice")]);

    // `AS OF TIME` resolves to the last transaction committed by then. A time
    // before anything committed is coordinate 0 — an empty Space, not an
    // error.
    let empty = ok(
        &nexus,
        r#"FIND(?c.name) WHERE { ?c CONCEPT {} } AS OF TIME "2000-01-01T00:00:00Z""#,
    )
    .await;
    assert!(rows(&empty).is_empty());

    let now = ok(
        &nexus,
        r#"FIND(?c.name) WHERE { ?c CONCEPT {} } AS OF TIME "2099-01-01T00:00:00Z""#,
    )
    .await;
    assert_eq!(rows(&now), &vec![json!("Alice A.")]);

    let unknown = err(
        &nexus,
        r#"FIND(?c.name) WHERE { ?c CONCEPT {} } AS OF TX "kip:space:default#999""#,
    )
    .await;
    assert_eq!(unknown.code, "TransactionUnknown", "{unknown:?}");
    let _ = created;
}

/// A tuple that was retracted later is still there at the coordinate before
/// it, and an Assertion's payload reads as it was claimed then.
#[tokio::test]
async fn a_past_coordinate_still_holds_what_was_later_removed() {
    let nexus = nexus("as_of_removal").await;
    let (claimed, _) = commit(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
            ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
            CREATE ASSERTION ?a {
                SET FIELDS {proposition: ?p, asserted_by: ?alice, stance: "support", mode: "stated", confidence: 0.9}
            }
        }"#,
    )
    .await;
    commit(&nexus, r#"RETRACT ASSERTION ?a WHERE { ?a ASSERTION {} }"#).await;

    let now = ok(
        &nexus,
        r#"FIND(?a.lifecycle.status) WHERE { ?a ASSERTION {} }"#,
    )
    .await;
    assert_eq!(rows(&now), &vec![json!("retracted")]);

    let then = ok(
        &nexus,
        &format!(
            r#"FIND(?a.lifecycle.status, ?a.confidence) WHERE {{ ?a ASSERTION {{}} }} AS OF SEQ {claimed}"#
        ),
    )
    .await;
    assert_eq!(rows(&then), &vec![json!(["active", 0.9])]);

    // The tuple pattern reads the same coordinate.
    let tuple = ok(
        &nexus,
        &format!(
            r#"FIND(?s.name, ?o.name) WHERE {{ ?p PROPOSITION (?s, "prefers", ?o) }} AS OF SEQ {claimed}"#
        ),
    )
    .await;
    assert_eq!(rows(&tuple), &vec![json!(["Alice", "Dark"])]);
}

/// An archived Concept is out of ordinary recall now, and was in it then.
#[tokio::test]
async fn archiving_changes_what_recall_returns_only_from_that_coordinate_on() {
    let nexus = nexus("as_of_archive").await;
    let (created, _) = commit(
        &nexus,
        r#"CREATE CONCEPT ?note { TYPE "Insight" NAME "Old note" SET ATTRIBUTES {summary: "s"} }"#,
    )
    .await;
    commit(
        &nexus,
        r#"ARCHIVE ?c WHERE { ?c CONCEPT {name: "Old note"} }"#,
    )
    .await;

    let now = ok(&nexus, r#"FIND(COUNT(?c)) WHERE { ?c CONCEPT {} }"#).await;
    assert_eq!(rows(&now), &vec![json!(0)]);

    let then = ok(
        &nexus,
        &format!(r#"FIND(COUNT(?c)) WHERE {{ ?c CONCEPT {{}} }} AS OF SEQ {created}"#),
    )
    .await;
    assert_eq!(rows(&then), &vec![json!(1)]);
}

/// `SNAPSHOT` now issues a token, because the engine can honour what a token
/// promises: a later read carrying it answers at the same coordinate.
#[tokio::test]
async fn a_snapshot_token_binds_a_later_read_to_its_coordinate() {
    let (nexus, created, _, _) = evolving("snapshot_token").await;

    let at_creation = ok(&nexus, &format!("SNAPSHOT AS OF SEQ {created}")).await;
    let token = at_creation["snapshot_token"]
        .as_str()
        .expect("a snapshot issues a token")
        .to_string();
    assert_eq!(at_creation["snapshot_seq"], json!(created));

    let request = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "read": {"snapshot_token": token},
        "operations": [{"command": r#"FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} }"#}]
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
    assert_eq!(
        rows(response.first_result().unwrap()),
        &vec![json!("Alice")],
        "the bound read must answer at the token's coordinate"
    );

    // A token from another Space names a sequence that means something else
    // there.
    let foreign = anda_cognitive_nexus::store::history::Coordinate { seq: created }
        .to_token("kip:space:somewhere-else");
    let request = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "read": {"snapshot_token": foreign},
        "operations": [{"command": r#"FIND(?c.name) WHERE { ?c CONCEPT {} }"#}]
    }))
    .unwrap();
    let parsed = request.operations[0].parse().unwrap();
    let response = nexus
        .execute(parsed, &request, &request.operations[0])
        .await;
    let error = response
        .results
        .into_iter()
        .find_map(|result| result.error)
        .expect("a foreign token must be refused");
    assert_eq!(error.code, "CursorInvalidated", "{error:?}");
}

/// One read answers at one coordinate: a request bound to a snapshot and a
/// command naming a different one cannot both be honoured.
#[tokio::test]
async fn a_bound_request_and_a_disagreeing_command_are_refused() {
    let (nexus, created, renamed, _) = evolving("conflicting_coordinates").await;
    let snapshot = ok(&nexus, &format!("SNAPSHOT AS OF SEQ {created}")).await;
    let token = snapshot["snapshot_token"].as_str().unwrap().to_string();

    let request = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "read": {"snapshot_token": token},
        "operations": [{
            "command": format!(r#"FIND(?c.name) WHERE {{ ?c CONCEPT {{}} }} AS OF SEQ {renamed}"#)
        }]
    }))
    .unwrap();
    let parsed = request.operations[0].parse().unwrap();
    let response = nexus
        .execute(parsed, &request, &request.operations[0])
        .await;
    let error = response
        .results
        .into_iter()
        .find_map(|result| result.error)
        .expect("two coordinates in one read must be refused");
    assert_eq!(error.code, "InvalidRequestEnvelope", "{error:?}");
}

/// A coordinate the Space has not reached is refused rather than answered with
/// the present.
#[tokio::test]
async fn a_future_coordinate_is_refused() {
    let (nexus, _, renamed, _) = evolving("future_coordinate").await;
    let error = err(&nexus, &format!("SNAPSHOT AS OF SEQ {}", renamed + 100)).await;
    assert_eq!(error.code, "HistoricalSnapshotUnavailable", "{error:?}");
}

/// The Schema Environment a historical read resolves through is the one that
/// was in force then.
#[tokio::test]
async fn a_historical_read_resolves_symbols_through_the_schema_of_its_time() {
    let (nexus, created, _, _) = evolving("historical_schema").await;
    let environment = ok(
        &nexus,
        &format!("DESCRIBE SCHEMA ENVIRONMENT AS OF SEQ {created}"),
    )
    .await;
    assert_eq!(environment["snapshot_seq"], json!(created));
    assert_eq!(environment["version"], json!(1));
}

/// Belief is projected from the Assertions on record *at that coordinate*, so
/// a claim made later does not leak into an earlier answer.
#[tokio::test]
async fn a_projection_at_a_coordinate_sees_only_the_claims_of_its_time() {
    let nexus = nexus("historical_belief").await;
    let (first, _) = commit(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
            ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
            CREATE ASSERTION ?a {
                SET FIELDS {proposition: ?p, asserted_by: ?alice, stance: "support", mode: "stated", confidence: 0.6}
            }
        }"#,
    )
    .await;
    commit(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?bob { TYPE "Person" NAME "Bob" }
            CREATE ASSERTION ?a2 {
                SET FIELDS {
                    proposition: :p, asserted_by: ?bob, stance: "reject",
                    mode: "stated", confidence: 0.9
                }
            }
        }"#
        .replace(
            ":p",
            &format!(
                "\"{}\"",
                ok(
                    &nexus,
                    r#"FIND(?p.id) WHERE { ?p PROPOSITION (?s, "prefers", ?o) }"#
                )
                .await
                .as_array()
                .unwrap()[0]
                    .as_str()
                    .unwrap()
            ),
        )
        .as_str(),
    )
    .await;

    let now = ok(
        &nexus,
        r#"FIND(?b.status) WHERE { ?p PROPOSITION (?s, "prefers", ?o) ?b BELIEF (?p) }"#,
    )
    .await;
    let then = ok(
        &nexus,
        &format!(
            r#"FIND(?b.status) WHERE {{ ?p PROPOSITION (?s, "prefers", ?o) ?b BELIEF (?p) }} AS OF SEQ {first}"#
        ),
    )
    .await;
    assert_ne!(
        rows(&now),
        rows(&then),
        "the rejection recorded later must not reach the earlier coordinate"
    );
}
