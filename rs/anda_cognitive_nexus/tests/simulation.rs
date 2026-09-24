#![cfg(feature = "simulation")]

use anda_cognitive_nexus::{
    CognitiveNexus,
    governance::{
        AuthContext, SYSTEM_PRINCIPAL,
        rows::{AuthorityConditions, PolicyStatement, principal_class},
        store::{GrantDraft, PolicyDraft, PrincipalDraft},
    },
    nexus::{DEFAULT_SPACE, RetentionAction, Session},
    time,
};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{Json, Request, TopLevelStatus, execute_request};
use object_store::memory::InMemory;
use serde_json::json;
use std::sync::Arc;

const EXPIRY: &str = "2999-01-01T00:00:00.000Z";
const AFTER: &str = "3000-01-01T00:00:00.000Z";

async fn fresh() -> (Arc<AndaDB>, CognitiveNexus) {
    let db = Arc::new(
        AndaDB::connect(
            Arc::new(InMemory::new()),
            DBConfig {
                name: "session_lifecycle_simulation".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
    );
    let nexus = CognitiveNexus::connect(db.clone()).await.unwrap();
    nexus
        .install_and_activate(
            &[
                ("test", anda_cognitive_nexus::profiles::COGNITIVE_MEMORY),
                ("test", include_str!("support/options.json")),
            ],
            DEFAULT_SPACE,
        )
        .await
        .unwrap();
    (db, nexus)
}

async fn run(session: &Session, command: &str, params: Json) -> Json {
    let request: Request = serde_json::from_value(json!({
        "kip": "2.0", "operations": [{"command": command, "parameters": params}]
    }))
    .unwrap();
    let response = execute_request(session, &request).await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "{command}: {response:?}"
    );
    response.first_result().cloned().unwrap()
}

async fn seed(nexus: &CognitiveNexus) -> Json {
    let session = nexus.system_session();
    let created = run(
        &session,
        r#"MUTATE {
        CREATE CONCEPT ?person {TYPE "Person" NAME "Actor"}
        CREATE CONCEPT ?preference {TYPE "Option" NAME "Dark"}
        CREATE CONCEPT ?retained {TYPE "Person" NAME "Retained"}
        CREATE CONCEPT ?held {TYPE "Person" NAME "Held"}
        ENSURE PROPOSITION ?p (?person, "prefers", ?preference)
        CREATE ASSERTION ?a {SET FIELDS {
            proposition: ?p, asserted_by: ?person, stance: "support", mode: "stated",
            confidence: 0.9, valid_time: {until: :expiry}
        }}
    }"#,
        json!({"expiry": EXPIRY}),
    )
    .await;
    let handles = created["handles"].clone();
    for (name, held) in [("retained", false), ("held", true)] {
        run(
            &session,
            "SET RETENTION :id {expires_at: :expiry, legal_hold: :held}",
            json!({"id": handles[name], "expiry": EXPIRY, "held": held}),
        )
        .await;
    }
    handles
}

async fn view(nexus: &CognitiveNexus, id: &Json) -> Json {
    let element = nexus
        .store
        .get_element(id.as_str().unwrap().parse().unwrap())
        .await
        .unwrap();
    anda_cognitive_nexus::view::render(&element)
}

#[tokio::test]
async fn simulation_sweeps_retention_but_preserves_session_isolation_holds_and_real_timestamps() {
    for action in [RetentionAction::Archive, RetentionAction::Tombstone] {
        let (db, nexus) = fresh().await;
        let handles = seed(&nexus).await;
        let normal = nexus.system_session();
        let simulated = normal.clone().with_simulated_lifecycle_time(AFTER).unwrap();
        // Setting one session's clock never moves the original or another session.
        assert!(
            normal
                .sweep_expired(DEFAULT_SPACE, action, 10)
                .await
                .unwrap()
                .swept
                .is_empty()
        );
        let before = time::now();
        let projection = run(
            &simulated,
            r#"FIND(?b) WHERE {
            ?person CONCEPT {name: "Actor"} ?pref CONCEPT {name: "Dark"}
            ?p (?person, "prefers", ?pref) ?b BELIEF (?p)
        }"#,
            json!({}),
        )
        .await;
        let valid_at = projection[0]["basis"]["valid_at"].as_str().unwrap();
        assert!(valid_at >= before.as_str() && valid_at <= time::now().as_str());
        let sweep = simulated
            .sweep_expired(DEFAULT_SPACE, action, 10)
            .await
            .unwrap();
        assert_eq!(sweep.swept, vec![handles["retained"].as_str().unwrap()]);
        assert_eq!(sweep.held, 1);
        assert_eq!(sweep.refused, 0);
        // `expired` is computed from world time at a read and never stored
        // (§14.3): no clock moves an Assertion's lifecycle.
        let assertion = view(&nexus, &handles["a"]).await;
        assert_eq!(assertion["lifecycle"]["status"], "active");
        let retained = view(&nexus, &handles["retained"]).await;
        assert_eq!(
            retained["_system"]["state"],
            match action {
                RetentionAction::Archive => "archived",
                RetentionAction::Tombstone => "tombstoned",
            }
        );
        assert_eq!(
            view(&nexus, &handles["held"]).await["_system"]["state"],
            "active"
        );
        let after = time::now();
        let updated = retained["_system"]["updated_at"].as_str().unwrap();
        assert!(updated >= before.as_str() && updated <= after.as_str());
        assert_ne!(updated, AFTER);
        let audits = normal.read_audit(DEFAULT_SPACE, 100).await.unwrap();
        let expiries: Vec<_> = audits
            .iter()
            .filter(|row| row.operation == "retention_expiry")
            .collect();
        assert!(!expiries.is_empty());
        assert!(
            expiries
                .iter()
                .all(|row| row.at >= before && row.at <= after)
        );
        db.close().await.unwrap();
    }
}

#[tokio::test]
async fn simulation_is_not_a_request_option_or_an_ordinary_principal_capability() {
    let (db, nexus) = fresh().await;
    for auth in [
        AuthContext::anonymous(),
        AuthContext::principal("kip:principal:agent"),
        AuthContext::principal(SYSTEM_PRINCIPAL),
        AuthContext::system().with_delegation_chain(vec!["delegated".into()]),
    ] {
        let err = nexus
            .session(auth)
            .with_simulated_lifecycle_time(AFTER)
            .unwrap_err();
        assert_eq!(err.name(), "NotAuthorized");
    }
    assert_eq!(
        nexus
            .system_session()
            .with_simulated_lifecycle_time("tomorrow")
            .unwrap_err()
            .name(),
        "ConstraintViolation"
    );
    assert!(
        serde_json::from_value::<Request>(
            json!({"kip": "2.0", "operations": [{"command": "DESCRIBE PRIMER"}],
        "context": {"simulated_lifecycle_time": AFTER}})
        )
        .is_err()
    );
    seed(&nexus).await;
    let request: Request = serde_json::from_value(
        json!({"kip": "2.0", "operations": [{"command": "DESCRIBE PRIMER"}],
        "extensions": {"test/simulated_lifecycle_time": {"at": AFTER}}}),
    )
    .unwrap();
    assert_eq!(
        execute_request(&nexus.system_session(), &request)
            .await
            .status,
        TopLevelStatus::Succeeded
    );
    assert!(
        nexus
            .system_session()
            .sweep_expired(DEFAULT_SPACE, RetentionAction::Archive, 10)
            .await
            .unwrap()
            .swept
            .is_empty()
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn simulation_does_not_bypass_element_policy_denials() {
    let (db, nexus) = fresh().await;
    let handles = seed(&nexus).await;
    nexus
        .governance()
        .publish_policy(
            PolicyDraft {
                policy_id: "kip:policy:simulation-deny".into(),
                space_id: DEFAULT_SPACE.into(),
                statements: vec![PolicyStatement {
                    effect: "deny".into(),
                    principals: vec![SYSTEM_PRINCIPAL.into()],
                    actions: vec!["archive".into(), "tombstone".into(), "maintain".into()],
                    ..Default::default()
                }],
                ..Default::default()
            },
            SYSTEM_PRINCIPAL,
        )
        .await
        .unwrap();
    let mut space = nexus.store.get_space(DEFAULT_SPACE).await.unwrap();
    space.default_policy_id = "kip:policy:simulation-deny".into();
    nexus.store.put_space(&space).await.unwrap();
    let simulated = nexus
        .system_session()
        .with_simulated_lifecycle_time(AFTER)
        .unwrap();
    for action in [RetentionAction::Archive, RetentionAction::Tombstone] {
        let report = simulated
            .sweep_expired(DEFAULT_SPACE, action, 10)
            .await
            .unwrap();
        assert!(report.swept.is_empty());
        assert_eq!(report.refused, 1);
        assert_eq!(report.held, 1);
    }
    assert_eq!(
        view(&nexus, &handles["a"]).await["lifecycle"]["status"],
        "active"
    );
    assert_eq!(
        view(&nexus, &handles["retained"]).await["_system"]["state"],
        "active"
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn rewinding_lifecycle_time_never_revives_an_expired_grant() {
    let (db, nexus) = fresh().await;
    let owner = "kip:principal:simulation-owner";
    nexus
        .governance()
        .ensure_principal(PrincipalDraft {
            principal_id: owner.into(),
            principal_class: principal_class::AGENT.into(),
            display_name: "owner".into(),
            auth_provider: "test".into(),
            auth_subject: "owner".into(),
        })
        .await
        .unwrap();
    nexus
        .governance()
        .create_grant(
            GrantDraft {
                space_id: DEFAULT_SPACE.into(),
                grantee_principal: SYSTEM_PRINCIPAL.into(),
                actions: vec!["manage_retention".into(), "read".into(), "archive".into()],
                conditions: AuthorityConditions {
                    valid_until: "2000-01-01T00:00:00.000Z".into(),
                    ..Default::default()
                },
                ..Default::default()
            },
            SYSTEM_PRINCIPAL,
        )
        .await
        .unwrap();
    let mut space = nexus.store.get_space(DEFAULT_SPACE).await.unwrap();
    space.owner_principal = owner.into();
    space.owners = vec![owner.into()];
    nexus.store.put_space(&space).await.unwrap();
    let simulated = nexus
        .system_session()
        .with_simulated_lifecycle_time("1990-01-01T00:00:00.000Z")
        .unwrap();
    assert!(
        simulated
            .sweep_expired(DEFAULT_SPACE, RetentionAction::Archive, 10)
            .await
            .is_err()
    );
    db.close().await.unwrap();
}

#[tokio::test]
async fn evaluation_simulation_changes_cutoff_eligibility_only() {
    let (db, nexus) = fresh().await;
    let session = nexus.system_session();
    for auth in [
        AuthContext::principal("kip:principal:observer"),
        AuthContext::principal(SYSTEM_PRINCIPAL),
    ] {
        assert!(
            nexus
                .session(auth)
                .with_simulated_evaluation_time(AFTER)
                .is_err()
        );
    }
    assert!(
        session
            .clone()
            .with_simulated_evaluation_time("tomorrow")
            .is_err()
    );
    let behavior = json!({"task_family":"simulation","procedure":"fixture"});
    let digest = anda_cognitive_nexus::content_digest(&behavior).unwrap();
    let seeded=run(&session, &format!(r#"MUTATE {{
        CREATE CONCEPT ?s {{TYPE "Skill" SET ATTRIBUTES {{skill_class:"workflow",summary:"simulation",status:"proposed"}} SET STRUCTURAL {{("current_revision",?r)}}}}
        CREATE CONCEPT ?r {{TYPE "SkillRevision" SET ATTRIBUTES {{task_family:"simulation",procedure:"fixture",behavior_digest:"{digest}"}} SET STRUCTURAL {{("revision_of",?s)}}}}
    }}"#), json!({})).await;
    let skill = seeded["handles"]["s"].as_str().unwrap();
    let revision = seeded["handles"]["r"].as_str().unwrap();
    let comparison = json!({"status":"withdrawal","effect":null,"uncertainty":{"reason":"isolated clock fixture"}});
    let replay = session
        .put_artifact(
            DEFAULT_SPACE,
            json!({"comparison":comparison}),
            vec![revision.into()],
        )
        .await
        .unwrap();
    let evaluation = json!({"trial_ref":null,"revision_refs":[revision],"from_status":"proposed","to_status":"revoked",
        "rule_digest":digest,"parameters_digest":digest,"cutoff":EXPIRY,"attempt_refs":[],"outcome_refs":[],
        "excluded_samples":[],"missing_attempt_refs":[],"comparison":comparison,"replay_artifact":replay});
    let command = format!(
        r#"MUTATE {{
        CREATE ACTIVITY ?v {{SET FIELDS {{activity_class:"lifecycle_verdict",status:"completed"}} SET FACET "EvaluationRecord" {evaluation} SET STRUCTURAL {{("inputs","{revision}") ("outputs","{skill}")}}}}
        UPDATE "{skill}" SET ATTRIBUTES {{status:"revoked"}} SET STRUCTURAL {{("current_evaluation",?v)}} EXPECT VERSION 1
    }}"#
    );
    let mut request = Request::single(command);
    request.parameters =
        Some(serde_json::from_value(json!({"simulated_evaluation_time":AFTER})).unwrap());
    let failed = execute_request(&session, &request).await;
    assert_eq!(
        failed.status,
        TopLevelStatus::Failed,
        "serialized parameters cannot enable simulation: {failed:?}"
    );
    assert!(
        serde_json::to_string(&failed)
            .unwrap()
            .contains("cutoff cannot be in the future")
    );
    let simulated = session
        .clone()
        .with_simulated_evaluation_time(AFTER)
        .unwrap();
    let committed = execute_request(&simulated, &request).await;
    assert_eq!(committed.status, TopLevelStatus::Succeeded, "{committed:?}");
    let verdict = committed.first_result().unwrap()["handles"]["v"]
        .as_str()
        .unwrap();
    let rows = run(
        &session,
        &format!(
            "FIND(?v) WHERE {{?v ACTIVITY {{id:{}}}}}",
            serde_json::to_string(verdict).unwrap()
        ),
        json!({}),
    )
    .await;
    let row = &rows[0];
    assert!(
        row["_system"]["created_at"].as_str().unwrap() < EXPIRY,
        "audit time must remain real"
    );
    assert_eq!(
        row["facets"]["kip://profiles/cognitive-memory@2.0.0/EvaluationRecord"]["cutoff"],
        EXPIRY
    );
    // A simulated cutoff must not silently change normal lifecycle sweeps.
    seed(&nexus).await;
    assert!(
        simulated
            .sweep_expired(DEFAULT_SPACE, RetentionAction::Archive, 100)
            .await
            .unwrap()
            .swept
            .is_empty()
    );
    nexus.close().await.unwrap();
    db.close().await.unwrap();
}
