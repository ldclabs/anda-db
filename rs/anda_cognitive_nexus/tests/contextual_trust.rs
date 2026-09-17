use anda_cognitive_nexus::{
    CognitiveNexus,
    governance::rows::GovernanceAuditRow,
    nexus::DEFAULT_SPACE,
    profiles::COGNITIVE_MEMORY,
    store::Element,
    trust::{ContextualTrustRule, TrustCalibrationProposal, TrustConfiguration},
};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{Json, Request, TopLevelStatus};
use object_store::{ObjectStore, memory::InMemory};
use serde_json::json;
use std::sync::Arc;

#[path = "support/attention_fault.rs"]
mod fault;

async fn run(n: &CognitiveNexus, command: &str, params: Json) -> Json {
    let mut request = Request::single(command);
    request.parameters = params.as_object().cloned();
    let response = anda_kip::execute_request(n, &request).await;
    assert_eq!(response.status, TopLevelStatus::Succeeded, "{response:?}");
    response.first_result().cloned().unwrap()
}

async fn fresh(store: Arc<dyn ObjectStore>) -> CognitiveNexus {
    let db = Arc::new(
        AndaDB::connect(
            store,
            DBConfig {
                name: "contextual_trust".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
    );
    let n = CognitiveNexus::connect(db).await.unwrap();
    n.install_and_activate(&[("brain", COGNITIVE_MEMORY)], DEFAULT_SPACE)
        .await
        .unwrap();
    n
}

async fn seed(n: &CognitiveNexus) -> Json {
    run(n,r#"MUTATE {
      CREATE CONCEPT ?actor {TYPE "Person" NAME "source"}
      CREATE CONCEPT ?object {TYPE "Preference" NAME "tea"}
      CREATE CONCEPT ?work {TYPE "Event" NAME "work" SET ATTRIBUTES {summary:"work context"}}
      CREATE CONCEPT ?home {TYPE "Event" NAME "home" SET ATTRIBUTES {summary:"home context"}}
      ENSURE PROPOSITION ?p (?actor,"prefers",?object)
      CREATE ASSERTION ?a {SET FIELDS {proposition:?p,asserted_by:?actor,mode:"stated",stance:"support",confidence:1}}
      CREATE EVIDENCE ?e {SET FIELDS {evidence_class:"observation",payload:"independent fixture observation"}}
    }"#,Json::Null).await["handles"].clone()
}

fn configuration(ids: &Json, weight: f64) -> TrustConfiguration {
    TrustConfiguration {
        weights: Default::default(),
        default_weight: 1.0,
        rules: vec![ContextualTrustRule {
            id: "work-preference".into(),
            actor_ref: ids["actor"].as_str().unwrap().into(),
            predicate_ref: Some("kip://profiles/cognitive-memory@2.1.0/prefers".into()),
            context_ref: Some(ids["work"].as_str().unwrap().into()),
            weight,
        }],
    }
}

async fn belief(
    n: &CognitiveNexus,
    ids: &Json,
    contexts: Vec<String>,
    history: Option<u64>,
) -> Json {
    let suffix = history
        .map(|v| format!(" AS OF SEQ {v}"))
        .unwrap_or_default();
    run(n,&format!("FIND(?b) WHERE {{?p PROPOSITION(id: :id) ?b BELIEF(?p)}} {suffix} WITH EPISTEMIC {{context_refs: :contexts}}"),json!({"id":ids["p"],"contexts":contexts})).await[0].clone()
}

#[tokio::test]
async fn context_and_predicate_weights_do_not_change_global_trust_or_assertion_confidence() {
    let n = fresh(Arc::new(InMemory::new())).await;
    let ids = seed(&n).await;
    let old_seq = n.store.get_space(DEFAULT_SPACE).await.unwrap().seq;
    let cfg = configuration(&ids, 0.1);
    assert_eq!(
        cfg.weight(
            ids["actor"].as_str().unwrap(),
            "other-predicate",
            &[ids["work"].as_str().unwrap().into()]
        )
        .unwrap(),
        1.0
    );
    n.system_session()
        .set_contextual_trust(DEFAULT_SPACE, 1, cfg.clone())
        .await
        .unwrap();
    assert_eq!(belief(&n, &ids, vec![], None).await["status"], "accepted");
    assert_eq!(
        belief(&n, &ids, vec![ids["home"].as_str().unwrap().into()], None).await["status"],
        "accepted"
    );
    let scoped = belief(&n, &ids, vec![ids["work"].as_str().unwrap().into()], None).await;
    assert_eq!(scoped["status"], "uncertain");
    let old = belief(
        &n,
        &ids,
        vec![ids["work"].as_str().unwrap().into()],
        Some(old_seq),
    )
    .await;
    assert_eq!(old["status"], "accepted");
    assert_ne!(
        old["basis"]["trust_version"],
        scoped["basis"]["trust_version"]
    );
    let Element::Assertion(a) = n
        .store
        .get_element(ids["a"].as_str().unwrap().parse().unwrap())
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(a.confidence, 1.0);
    n.system_session()
        .set_trust(DEFAULT_SPACE, 2, Default::default(), 0.9)
        .await
        .unwrap();
    let saved = n
        .system_session()
        .read_control(DEFAULT_SPACE, "trust", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.value["rules"], json!(cfg.rules));
    assert_eq!(
        belief(&n, &ids, vec![ids["work"].as_str().unwrap().into()], None).await["status"],
        "uncertain"
    );
    n.close().await.unwrap();
}

#[test]
fn ambiguous_scopes_never_depend_on_rule_order_and_specific_overrides_win() {
    let mut c = TrustConfiguration {
        weights: Default::default(),
        default_weight: 1.,
        rules: vec![
            ContextualTrustRule {
                id: "context".into(),
                actor_ref: "C-1".into(),
                predicate_ref: None,
                context_ref: Some("C-2".into()),
                weight: 0.2,
            },
            ContextualTrustRule {
                id: "predicate".into(),
                actor_ref: "C-1".into(),
                predicate_ref: Some("p".into()),
                context_ref: None,
                weight: 0.8,
            },
        ],
    };
    assert!(c.weight("C-1", "p", &["C-2".into()]).is_err());
    c.rules.push(ContextualTrustRule {
        id: "combined".into(),
        actor_ref: "C-1".into(),
        predicate_ref: Some("p".into()),
        context_ref: Some("C-2".into()),
        weight: 0.5,
    });
    assert_eq!(c.weight("C-1", "p", &["C-2".into()]).unwrap(), 0.5);
    c.rules.reverse();
    assert_eq!(c.weight("C-1", "p", &["C-2".into()]).unwrap(), 0.5);
    c.rules[0].weight = f64::NAN;
    assert!(c.validate().is_err());
}

async fn proposal(n: &CognitiveNexus, ids: &Json, weight: f64) -> anda_kip::cognitive::ArtifactPin {
    let s = n.system_session();
    let method = s
        .put_artifact(
            DEFAULT_SPACE,
            json!({"method":"fixture-only-not-empirical-calibration"}),
            vec![],
        )
        .await
        .unwrap();
    let p = TrustCalibrationProposal {
        format: "nexus:trust-calibration-v1".into(),
        space_id: DEFAULT_SPACE.into(),
        expected_version: 1,
        configuration: configuration(ids, weight),
        method,
        evidence_refs: vec![ids["e"].as_str().unwrap().into()],
        uncertainty: json!({"kind":"test fixture"}),
    };
    s.put_artifact(DEFAULT_SPACE, json!(p), p.evidence_refs.clone())
        .await
        .unwrap()
}

async fn audits(n: &CognitiveNexus) -> Vec<GovernanceAuditRow> {
    let rows = n.governance().read_audit(DEFAULT_SPACE, 100).await.unwrap();
    rows.into_iter()
        .filter(|r| r.operation == "apply_trust_calibration")
        .collect()
}

#[tokio::test]
async fn trust_calibration_binds_proposal_audit_and_control_with_idempotent_replay() {
    let n = fresh(Arc::new(InMemory::new())).await;
    let ids = seed(&n).await;
    let p = proposal(&n, &ids, 0.2).await;
    let s = n.system_session();
    let before = n.store.get_space(DEFAULT_SPACE).await.unwrap().seq;
    let result = s
        .apply_trust_calibration(DEFAULT_SPACE, 1, p.clone(), "apply-1")
        .await
        .unwrap();
    assert_eq!(result["receipt"]["transaction_class"], "governance");
    assert_eq!(
        result,
        s.apply_trust_calibration(DEFAULT_SPACE, 1, p.clone(), "apply-1")
            .await
            .unwrap()
    );
    let record = s
        .read_control(DEFAULT_SPACE, "trust", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.version, 2);
    assert_eq!(record.seq, result["receipt"]["space_seq"]);
    assert_eq!(record.value["calibration"]["proposal"], json!(p));
    let audit = audits(&n).await;
    assert_eq!(audit.len(), 1);
    assert_eq!(audit[0].record["after_version"], 2);
    let changes = s.change_page(DEFAULT_SPACE, before, 100).await.unwrap();
    assert!(changes["changes"].as_array().unwrap().iter().any(|e| {
        e["control_changes"]
            .as_array()
            .is_some_and(|c| c.iter().any(|c| c["kind"] == "trust"))
    }));
    let conflicting = proposal(&n, &ids, 0.7).await;
    assert!(
        s.apply_trust_calibration(DEFAULT_SPACE, 1, conflicting, "apply-1")
            .await
            .is_err()
    );
    assert_eq!(audits(&n).await.len(), 1);
    let anonymous = n.session(anda_cognitive_nexus::governance::AuthContext::anonymous());
    assert!(
        anonymous
            .apply_trust_calibration(DEFAULT_SPACE, 1, p, "forged")
            .await
            .is_err()
    );
    n.close().await.unwrap();
}

#[tokio::test]
async fn interrupted_trust_commit_recovers_audit_and_control_together() {
    let store = Arc::new(fault::FaultStore::default());
    let n = fresh(store.clone()).await;
    let ids = seed(&n).await;
    let p = proposal(&n, &ids, 0.2).await;
    store.arm("kip_control_records/");
    {
        let s = n.system_session();
        let operation = s.apply_trust_calibration(DEFAULT_SPACE, 1, p.clone(), "recover");
        tokio::pin!(operation);
        tokio::select! {
            _=store.entered.notified()=>{},
            result=&mut operation=>panic!("fault was not reached: {result:?}"),
            _=tokio::time::sleep(std::time::Duration::from_secs(20))=>panic!("fault watchdog"),
        }
    }
    drop(n);
    let recovered = fresh(store).await;
    let result = recovered
        .system_session()
        .apply_trust_calibration(DEFAULT_SPACE, 1, p, "recover")
        .await
        .unwrap();
    assert_eq!(result["version"], 2);
    assert_eq!(audits(&recovered).await.len(), 1);
    assert_eq!(
        recovered
            .system_session()
            .read_control(DEFAULT_SPACE, "trust", None)
            .await
            .unwrap()
            .unwrap()
            .version,
        2
    );
    recovered.close().await.unwrap();
}

#[tokio::test]
async fn corrected_evidence_cannot_authorize_a_new_calibration() {
    let n = fresh(Arc::new(InMemory::new())).await;
    let ids = seed(&n).await;
    let p = proposal(&n, &ids, 0.2).await;
    let replacement = run(
        &n,
        r#"CREATE EVIDENCE ?e {SET FIELDS {evidence_class:"observation",payload:"correction"}}"#,
        Json::Null,
    )
    .await;
    run(
        &n,
        "TRANSITION :original TO \"corrected\" BY :replacement",
        json!({"original":ids["e"],"replacement":replacement["handles"]["e"]}),
    )
    .await;
    assert!(
        n.system_session()
            .apply_trust_calibration(DEFAULT_SPACE, 1, p, "corrected")
            .await
            .is_err()
    );
    assert_eq!(
        n.system_session()
            .read_control(DEFAULT_SPACE, "trust", None)
            .await
            .unwrap()
            .unwrap()
            .version,
        1
    );
    assert!(audits(&n).await.is_empty());
    n.close().await.unwrap();
}
