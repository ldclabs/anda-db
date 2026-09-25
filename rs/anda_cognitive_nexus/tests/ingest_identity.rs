use anda_cognitive_nexus::{CognitiveNexus, nexus::DEFAULT_SPACE, profiles::COGNITIVE_MEMORY};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{
    Execution, ExecutionMode, IngestContext, IngestEvidence, Operation, Request, TopLevelStatus,
    execute_request,
};
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
        .install_and_activate(
            &[
                ("test", COGNITIVE_MEMORY),
                ("test", include_str!("support/options.json")),
            ],
            DEFAULT_SPACE,
        )
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
      CREATE CONCEPT ?p {TYPE "Option" NAME "Dark"}
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

#[tokio::test]
async fn explicit_null_payload_survives_ingestion_and_readback() {
    let nexus = fresh().await;
    let mut evidence = observation();
    evidence.payload = Some(Value::Null);
    let request =
        Request::from_value(serde_json::to_value(request(vec![evidence])).unwrap()).unwrap();
    let result = execute_request(&nexus, &request).await;
    assert_eq!(result.status, TopLevelStatus::Succeeded, "{result:?}");
    let result = execute_request(
        &nexus,
        &Request::single("FIND(?e.payload) WHERE { ?e EVIDENCE {} }"),
    )
    .await;
    assert_eq!(
        result.first_result(),
        Some(&json!([{"mode":"inline","inline":null}]))
    );
}

#[tokio::test]
async fn independent_writes_resolve_one_evidence_through_the_client_key() {
    // §71.1: an ingest entry is one Evidence per request. Two `independent`
    // operations are two transactions, and both mint the entry; only its
    // client_key makes the second resolve the first one's Evidence.
    let nexus = fresh().await;
    let batch = |client_key: Option<&str>| {
        let mut req = request(vec![IngestEvidence {
            client_key: client_key.map(str::to_string),
            ..observation()
        }]);
        req.operations.push(Operation::new(
            "UPSERT CONCEPT ?c { MATCH {type: \"Person\", key: \"other\"} SET FIELDS {name: \"Other\"} }",
        ));
        req.execution = Some(Execution::new(ExecutionMode::Independent));
        req
    };
    let people = async || {
        execute_request(
            &nexus,
            &Request::single("FIND(COUNT(?c)) WHERE { ?c CONCEPT {type: \"Person\"} }"),
        )
        .await
        .first_result()
        .unwrap()
        .clone()
    };

    let refused = execute_request(&nexus, &batch(None)).await;
    assert_eq!(
        refused.error.as_ref().unwrap().code,
        "InvalidRequestEnvelope"
    );
    assert_eq!(evidence_count(&nexus).await, json!([0]));
    assert_eq!(people().await, json!([0]), "no operation ran");

    let response = execute_request(&nexus, &batch(Some("thread:batch:message"))).await;
    assert_eq!(response.status, TopLevelStatus::Succeeded, "{response:?}");
    assert_eq!(evidence_count(&nexus).await, json!([1]));
    assert_eq!(people().await, json!([2]));
}
