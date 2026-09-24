//! The exposure log (Spec §66.8), as `KIP2-RT-035` states it: recording
//! `retrieved` and `used` allocates no `space_seq`, emits no Change Envelope
//! entry, creates no element and changes no strength; entries are readable
//! only under `read_audit`, omit elements the reader may not discover, and go
//! with their element when it is erased.

use anda_cognitive_nexus::{
    CognitiveNexus,
    exposure::{ExposureInput, ExposureQuery},
    governance::{
        AuthContext, SYSTEM_PRINCIPAL,
        rows::{AuthorityScope, principal_class},
        store::{GrantDraft, PrincipalDraft},
    },
    nexus::{DEFAULT_SPACE, Session},
    schema::{PackageState, SchemaLock, SchemaPackage},
};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{Executor, Json, Request, TopLevelStatus, cognitive::Exposure};
use object_store::memory::InMemory;
use serde_json::json;
use std::sync::Arc;

const PROFILE_ID: &str = "kip://profiles/cognitive-memory";

async fn fresh(name: &str) -> CognitiveNexus {
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: name.to_string(),
            description: "exposure log tests".to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let nexus = CognitiveNexus::connect(Arc::new(db)).await.unwrap();
    nexus
        .install_package(
            &SchemaPackage::parse(anda_cognitive_nexus::profiles::COGNITIVE_MEMORY).unwrap(),
            "test",
        )
        .await
        .unwrap();
    let mut lock = SchemaLock::default();
    lock.packages.insert(PROFILE_ID.into(), "2.0.0".into());
    lock.states.insert(PROFILE_ID.into(), PackageState::Active);
    nexus.activate_schema(DEFAULT_SPACE, lock).await.unwrap();
    nexus
}

async fn ok(executor: &impl Executor, command: &str, params: Json) -> Json {
    let request = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "operations": [{"command": command, "parameters": params}]
    }))
    .unwrap();
    let parsed = request.operations[0].parse().unwrap();
    let response = executor
        .execute(parsed, &request, &request.operations[0])
        .await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "{command}\n{:#?}",
        response.error
    );
    response.first_result().cloned().unwrap_or(Json::Null)
}

async fn principal(
    nexus: &CognitiveNexus,
    id: &str,
    grants: Vec<(Vec<&str>, AuthorityScope)>,
) -> Session {
    let gov = nexus.governance();
    gov.ensure_principal(PrincipalDraft {
        principal_id: id.into(),
        principal_class: principal_class::AGENT.into(),
        display_name: id.into(),
        auth_provider: "test".into(),
        auth_subject: id.into(),
    })
    .await
    .unwrap();
    for (actions, scope) in grants {
        gov.create_grant(
            GrantDraft {
                space_id: DEFAULT_SPACE.into(),
                grantee_principal: id.into(),
                actions: actions.into_iter().map(str::to_string).collect(),
                scope,
                ..Default::default()
            },
            SYSTEM_PRINCIPAL,
        )
        .await
        .unwrap();
    }
    nexus.session(AuthContext::principal(id))
}

async fn space_seq(executor: &impl Executor) -> u64 {
    ok(executor, "DESCRIBE SPACE", json!({})).await["seq"]
        .as_u64()
        .unwrap()
}

fn retrieved(element: &str, seq: u64) -> ExposureInput {
    ExposureInput {
        element_id: element.into(),
        exposure: Exposure::Retrieved,
        snapshot_seq: seq,
        decision_ref: None,
        recall_ref: Some("recall-1".into()),
    }
}

#[tokio::test]
async fn exposure_is_never_cognition() {
    let nexus = fresh("exposure").await;
    let seeded = ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" SET FACET "MnemonicState" {memory_strength: 0.4} }
            CREATE ACTIVITY ?gate { SET FIELDS {activity_class: "action_gate", status: "completed"} }
        }"#,
        json!({}),
    )
    .await;
    let alice = seeded["handles"]["alice"].as_str().unwrap().to_string();
    let gate = seeded["handles"]["gate"].as_str().unwrap().to_string();
    let seq = space_seq(&nexus).await;
    let before = ok(
        &nexus,
        r#"FIND(?c) WHERE { ?c CONCEPT {id: :id} }"#,
        json!({"id": alice}),
    )
    .await;

    let session = nexus.system_session();
    let recorded = session
        .record_exposures(
            DEFAULT_SPACE,
            vec![
                retrieved(&alice, seq),
                ExposureInput {
                    element_id: alice.clone(),
                    exposure: Exposure::Used,
                    snapshot_seq: seq,
                    decision_ref: Some(gate.clone()),
                    recall_ref: None,
                },
            ],
        )
        .await
        .unwrap();
    assert_eq!(recorded["recorded"], 2);

    // No commit, no change stream entry, no element and no strength change.
    assert_eq!(space_seq(&nexus).await, seq);
    let changes = ok(&nexus, &format!("CHANGES AFTER SEQ {}", seq), json!({})).await;
    assert!(
        changes["changes"].as_array().is_none_or(Vec::is_empty),
        "{changes:#}"
    );
    let after = ok(
        &nexus,
        r#"FIND(?c) WHERE { ?c CONCEPT {id: :id} }"#,
        json!({"id": alice}),
    )
    .await;
    assert_eq!(after, before);

    let page = session
        .read_exposures(DEFAULT_SPACE, ExposureQuery::default())
        .await
        .unwrap();
    let records = page["records"].as_array().unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["exposure"], "retrieved");
    assert_eq!(records[0]["recall_ref"], "recall-1");
    assert_eq!(records[1]["exposure"], "used");
    assert_eq!(records[1]["decision_ref"], json!(gate));
    assert_eq!(records[1]["principal_id"], SYSTEM_PRINCIPAL);
    assert!(page["next_cursor"].is_null());

    // Paged, oldest first.
    let first = session
        .read_exposures(
            DEFAULT_SPACE,
            ExposureQuery {
                limit: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let cursor = first["next_cursor"].as_str().unwrap().to_string();
    let second = session
        .read_exposures(
            DEFAULT_SPACE,
            ExposureQuery {
                cursor: Some(cursor),
                limit: Some(1),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(second["records"][0]["exposure"], "used");

    // A `used` entry names the decision; a read ahead of the Space is refused.
    let unnamed = session
        .record_exposures(
            DEFAULT_SPACE,
            vec![ExposureInput {
                element_id: alice.clone(),
                exposure: Exposure::Used,
                snapshot_seq: seq,
                decision_ref: None,
                recall_ref: None,
            }],
        )
        .await
        .unwrap_err();
    assert_eq!(unnamed.name(), "ConstraintViolation");
    let ahead = session
        .record_exposures(DEFAULT_SPACE, vec![retrieved(&alice, seq + 10)])
        .await
        .unwrap_err();
    assert_eq!(ahead.name(), "ConstraintViolation");

    // Erasure takes the element's entries with it (§60.7).
    ok(
        &nexus,
        &format!(r#"PURGE "{alice}" CONFIRM "PURGE""#),
        json!({}),
    )
    .await;
    let erased = session
        .read_exposures(DEFAULT_SPACE, ExposureQuery::default())
        .await
        .unwrap();
    assert!(
        erased["records"].as_array().unwrap().is_empty(),
        "{erased:#}"
    );
}

#[tokio::test]
async fn the_log_is_audit_and_hides_what_a_reader_cannot_discover() {
    let nexus = fresh("audit").await;
    let seeded = ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?bob { TYPE "Person" NAME "Bob" }
            ASSERT ?a (?alice, "same_as", ?bob) { by: ?alice, mode: "stated", at: "2026-01-01T00:00:00.000Z" }
        }"#,
        json!({}),
    )
    .await;
    let alice = seeded["handles"]["alice"].as_str().unwrap().to_string();
    let assertion = seeded["handles"]["a"].as_str().unwrap().to_string();
    let seq = space_seq(&nexus).await;
    nexus
        .system_session()
        .record_exposures(
            DEFAULT_SPACE,
            vec![retrieved(&alice, seq), retrieved(&assertion, seq)],
        )
        .await
        .unwrap();

    // Reading the Space is not reading who has been reading it.
    let reader = principal(
        &nexus,
        "kip:principal:reader",
        vec![(vec!["discover", "read"], AuthorityScope::default())],
    )
    .await;
    assert_eq!(
        reader
            .read_exposures(DEFAULT_SPACE, ExposureQuery::default())
            .await
            .unwrap_err()
            .name(),
        "NotAuthorized"
    );

    // An auditor who may discover only Concepts sees only their entries.
    let auditor = principal(
        &nexus,
        "kip:principal:auditor",
        vec![
            (vec!["read_audit"], AuthorityScope::default()),
            (
                vec!["discover", "read"],
                AuthorityScope {
                    kinds: vec!["concept".into()],
                    ..Default::default()
                },
            ),
        ],
    )
    .await;
    let page = auditor
        .read_exposures(DEFAULT_SPACE, ExposureQuery::default())
        .await
        .unwrap();
    let elements: Vec<&str> = page["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["element_id"].as_str().unwrap())
        .collect();
    assert_eq!(elements, vec![alice.as_str()]);

    // Nor may it report an exposure of what it cannot read.
    let hidden = auditor
        .record_exposures(DEFAULT_SPACE, vec![retrieved(&assertion, seq)])
        .await
        .unwrap_err();
    assert_eq!(hidden.name(), "NotFoundOrNotVisible");
}
