use anda_cognitive_nexus::{CognitiveNexus, nexus::DEFAULT_SPACE, profiles::COGNITIVE_MEMORY};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{IngestContext, IngestEvidence, Request, TopLevelStatus, execute_request};
use object_store::memory::InMemory;
use serde_json::{Value, json};
use std::sync::Arc;

async fn fresh() -> CognitiveNexus {
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: "ingest_identity".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let nexus = CognitiveNexus::connect(Arc::new(db)).await.unwrap();
    nexus
        .install_and_activate(&[("test", COGNITIVE_MEMORY)], DEFAULT_SPACE)
        .await
        .unwrap();
    nexus
}

fn observation() -> IngestEvidence {
    IngestEvidence {
        key: "msg1".into(),
        client_key: Some("thread:submission:message".into()),
        evidence_class: "user_statement".into(),
        payload: Some(json!({"text":"first message"})),
        observed_at: Some("2026-09-07T00:00:00.000Z".into()),
        ..Default::default()
    }
}

fn request(evidence: Vec<IngestEvidence>) -> Request {
    let mut req = Request::single(
        "UPSERT CONCEPT ?c { MATCH {type: \"Person\", key: \"actor\"} SET FIELDS {name: \"Actor\"} }",
    );
    req.ingest = Some(IngestContext {
        evidence,
        extensions: None,
    });
    req
}

async fn evidence_count(nexus: &CognitiveNexus) -> Value {
    execute_request(
        nexus,
        &Request::single("FIND(COUNT(?e)) WHERE { ?e EVIDENCE {} }"),
    )
    .await
    .first_result()
    .unwrap()
    .clone()
}

#[tokio::test]
async fn replay_checks_observation_identity_before_binding_evidence() {
    let nexus = fresh().await;
    let original = observation();
    for _ in 0..2 {
        assert_eq!(
            execute_request(&nexus, &request(vec![original.clone()]))
                .await
                .status,
            TopLevelStatus::Succeeded
        );
    }
    assert_eq!(evidence_count(&nexus).await, json!([1]));
    let mut payload = original.clone();
    payload.payload = Some(json!({"text":"changed message"}));
    let mut class = original.clone();
    class.evidence_class = "tool_result".into();
    let mut time = original.clone();
    time.observed_at = Some("2026-09-08T00:00:00.000Z".into());
    for conflicting in [payload, class, time] {
        let response = execute_request(&nexus, &request(vec![conflicting])).await;
        assert_eq!(
            response.results[0].error.as_ref().unwrap().code,
            "ClientKeyConflict"
        );
    }
    assert_eq!(evidence_count(&nexus).await, json!([1]));
}

#[tokio::test]
async fn one_transaction_deduplicates_identical_keys_and_rolls_back_conflicts() {
    let nexus = fresh().await;
    let a = observation();
    let mut b = a.clone();
    b.key = "msg2".into();
    assert_eq!(
        execute_request(&nexus, &request(vec![a.clone(), b.clone()]))
            .await
            .status,
        TopLevelStatus::Succeeded
    );
    assert_eq!(evidence_count(&nexus).await, json!([1]));
    let mut a = a;
    a.client_key = Some("another-observation".into());
    b.client_key = a.client_key.clone();
    b.payload = Some(json!("different"));
    let response = execute_request(&nexus, &request(vec![a, b])).await;
    assert_eq!(
        response.results[0].error.as_ref().unwrap().code,
        "ClientKeyConflict"
    );
    assert_eq!(evidence_count(&nexus).await, json!([1]));
}

#[tokio::test]
async fn assertions_share_a_tuple_created_in_their_own_transaction() {
    let nexus = fresh().await;
    let response = execute_request(
        &nexus,
        &Request::single(
            r#"MUTATE {
      CREATE CONCEPT ?alice {TYPE "Person" NAME "Alice"}
      CREATE CONCEPT ?bob {TYPE "Person" NAME "Bob"}
      CREATE CONCEPT ?p {TYPE "Preference" NAME "Dark"}
      ASSERT ?a (?alice, "prefers", ?p) {by: ?alice, mode: "stated"}
      ASSERT ?b (?alice, "prefers", ?p) {by: ?bob, mode: "stated"}
    }"#,
        ),
    )
    .await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "{:?}",
        response.results
    );
    assert_eq!(nexus.store.propositions().len(), 1);
    assert_eq!(nexus.store.assertions().len(), 2);
}
