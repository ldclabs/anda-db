//! A file-backed round trip through the binding's Rust API.
//!
//! Run it with:
//!
//! ```bash
//! cargo run -p anda_cognitive_nexus_py --example test_kip_stateful_execution
//! ```
//!
//! The point is statefulness: the same database is written, read back, closed
//! and re-opened, so this catches a Nexus that only works while its process is
//! alive. It also shows the shape of a KIP 2.0 write — Concepts, a Proposition,
//! and an Assertion that is the only thing claiming anything.

use anda_cognitive_nexus_py::{create_kip_db, execute_kip, AndaDbConfig, StoreLocationType};
use anda_kip::{Json, Map, TopLevelStatus};

/// One attributed claim. The types come from the bundled cognitive-memory
/// profile, which `create_kip_db` activates: in KIP 2.0 a type is an immutable
/// Schema Package symbol, so there is no `$ConceptType` node to write first —
/// and no way for a command to invent a type on its way to using it.
///
/// `:preference_name` is a request parameter, bound structurally into a value
/// position rather than pasted into the text (§74, §88.2).
static RECORD_A_PREFERENCE: &str = r#"
    MUTATE {
        CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
        CREATE CONCEPT ?dark { TYPE "Preference" NAME :preference_name }
        CREATE EVIDENCE ?said {
            SET FIELDS {
                evidence_class: "user_statement",
                payload: "I prefer dark mode.",
                observed_at: "2026-08-16T09:00:00Z"
            }
        }
        ASSERT ?a (?alice, "prefers", ?dark) {
            by: ?alice, mode: "stated", confidence: 0.9, evidence: ?said
        }
    }
    "#;

/// The same actor changed their mind. Nothing is rewritten: this records a
/// second Assertion, because what somebody claimed in the past stays true
/// about the past (§76).
///
/// `?alice` is not available here — a handle is local to the transaction that
/// minted it — so the Person is named by the element id the first write
/// returned, bound as the `:alice` parameter.
static CHANGE_OF_MIND: &str = r#"
    MUTATE {
        CREATE CONCEPT ?light { TYPE "Preference" NAME "Light mode" }
        ASSERT ?a (:alice, "prefers", ?light) {
            by: :alice, mode: "stated", confidence: 0.7
        }
    }
    "#;

#[tokio::main]
async fn main() {
    println!("--- Running Full Stateful KIP Execution Test ---");

    let store_location = "/tmp/anda_cognitive_nexus_py_test_db".to_string();
    let path = std::path::Path::new(&store_location);
    if path.is_file() {
        panic!("store_location exists but is a file, not a directory: {store_location}");
    }
    std::fs::create_dir_all(path).expect("Failed to create store_location directory");

    let db_config = AndaDbConfig {
        store_location_type: StoreLocationType::LocalFile,
        store_location,
        db_name: "test_preferences_db".to_string(),
        db_desc: Some("Local file DB for the KIP binding example".to_string()),
        meta_cache_capacity: Some(10000),
    };

    println!("\n1. Recording an attributed claim...");
    let nexus = create_kip_db(db_config.clone())
        .await
        .expect("Failed to create the local-file Nexus");

    let mut parameters = Map::new();
    parameters.insert(
        "preference_name".to_string(),
        Json::String("Dark mode".to_string()),
    );
    let (_, response) = execute_kip(
        nexus.as_ref(),
        RECORD_A_PREFERENCE.to_string(),
        Some(parameters),
        false,
    )
    .await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "recording the claim failed: {:#?}",
        response.results
    );
    let alice = response
        .first_result()
        .and_then(|result| result["handles"]["alice"].as_str())
        .expect("the write reports the element each handle minted")
        .to_string();
    println!(
        "Recorded {alice}. tx: {:?}",
        response.receipt.and_then(|r| r.tx_id)
    );

    println!("\n2. Recording a change of mind...");
    let mut parameters = Map::new();
    parameters.insert("alice".to_string(), Json::String(alice));
    let (_, response) = execute_kip(
        nexus.as_ref(),
        CHANGE_OF_MIND.to_string(),
        Some(parameters),
        false,
    )
    .await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "recording the second claim failed: {:#?}",
        response.results
    );

    // Close and re-open: everything below reads what storage kept, not what a
    // live process is holding.
    nexus.close().await.expect("Failed to close the database");
    let nexus = create_kip_db(db_config)
        .await
        .expect("Failed to re-open the local-file Nexus");

    println!("\n3. Reading the claims back...");
    // This asks who claimed what, and with how much confidence. It does not
    // ask what is true: belief is projected from Assertions under a policy,
    // and a raw read is not that projection.
    let query = r#"
    FIND(?thing.name, ?a.confidence, ?a.mode)
    WHERE {
        ?p PROPOSITION (?person, "prefers", ?thing)
        ?a ASSERTION {proposition: ?p}
    }
    ORDER BY ?a.confidence DESC
    "#;
    let (_, response) = execute_kip(nexus.as_ref(), query.to_string(), None, false).await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "the read failed: {:#?}",
        response.results
    );

    let result = response.first_result().expect("the read returns a result");
    println!("Query Response: {result:#}");
    let rows = result.as_array().expect("a KQL result is an array");
    assert_eq!(
        rows.len(),
        2,
        "expected both claims to survive the restart, found {}",
        rows.len()
    );
    // Nothing was overwritten by the change of mind: the earlier claim is
    // still on the record, at the confidence it was made with.
    assert_eq!(rows[0][0], Json::from("Dark mode"));
    assert_eq!(rows[0][1], Json::from(0.9));
    assert_eq!(rows[1][0], Json::from("Light mode"));

    nexus.close().await.expect("Failed to close the database");
    println!("\n--- Full Stateful KIP Execution Test Passed ---");
}
