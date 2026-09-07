use anda_cognitive_nexus::{
    CognitiveNexus,
    nexus::DEFAULT_SPACE,
    profiles::{COGNITIVE_MEMORY, COGNITIVE_MEMORY_ID, COGNITIVE_MEMORY_VERSION},
    schema::{PackageState, SchemaLock, SchemaPackage, contracts},
    store::{Element, control::CommitPlan, rows::CommitLogRow, space::JournalEntry},
};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{Executor, Json, Request, TopLevelStatus};
use object_store::memory::InMemory;
use serde_json::json;
use std::sync::Arc;

async fn fresh(name: &str) -> (CognitiveNexus, Arc<AndaDB>) {
    let db = Arc::new(
        AndaDB::connect(
            Arc::new(InMemory::new()),
            DBConfig {
                name: name.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
    );
    let nexus = CognitiveNexus::connect(db.clone()).await.unwrap();
    nexus
        .install_package(&SchemaPackage::parse(COGNITIVE_MEMORY).unwrap(), "test")
        .await
        .unwrap();
    let mut lock = SchemaLock::default();
    lock.packages
        .insert(COGNITIVE_MEMORY_ID.into(), COGNITIVE_MEMORY_VERSION.into());
    lock.states
        .insert(COGNITIVE_MEMORY_ID.into(), PackageState::Active);
    nexus.activate_schema(DEFAULT_SPACE, lock).await.unwrap();
    (nexus, db)
}
async fn run(n: &CognitiveNexus, command: &str, parameters: Json) -> anda_kip::Response {
    let mut r = Request::single(command);
    r.parameters = parameters.as_object().cloned();
    n.execute(anda_kip::parse_kip(command).unwrap(), &r, &r.operations[0])
        .await
}
async fn ok(n: &CognitiveNexus, command: &str, p: Json) -> Json {
    let r = run(n, command, p).await;
    assert_eq!(
        r.status,
        TopLevelStatus::Succeeded,
        "{command}: {:?}",
        r.error
    );
    r.first_result().cloned().unwrap_or(Json::Null)
}
const SETUP: &str = r#"MUTATE {
CREATE CONCEPT ?a {TYPE "Person" NAME "Ada"}
CREATE CONCEPT ?b {TYPE "Preference" NAME "tea"}
ENSURE PROPOSITION ?p (?a,"prefers",?b)
CREATE EVIDENCE ?e { SET FIELDS {evidence_class:"observation",payload:"material"} }
}"#;
const BELIEF: &str = r#"FIND(?b) WHERE { ?p PROPOSITION (id:"P-1") ?b BELIEF (?p) }"#;
const DERIVE: &str = r#"MUTATE {
CREATE ASSERTION ?a {SET FIELDS {proposition:"P-1",asserted_by:"C-1",mode:"inferred",stance:"support",confidence:0.9,asserted_at:"2026-01-01T00:00:00Z"}}
CREATE ACTIVITY ?work {SET FIELDS {activity_class:"semantic_consolidation",status:"completed"}
 SET FACET "DependencyBasis" {basis_seq: :seq,groups:[{role:"all_of",pins:[{id:"C-1",version:1}]}],policy_basis: :basis}
 SET STRUCTURAL {("inputs","C-1") ("outputs",?a)} }
}"#;

#[tokio::test]
async fn revalidation_binds_exact_version_and_preserves_assertion_premises() {
    let (n, _) = fresh("revalidation").await;
    ok(&n, SETUP, Json::Null).await;
    let b = ok(&n, BELIEF, Json::Null).await[0]["basis"].clone();
    ok(&n, DERIVE, json!({"seq":b["snapshot_seq"],"basis":b})).await;
    ok(
        &n,
        r#"UPDATE "C-1" SET FIELDS {name:"Ada Lovelace"}"#,
        Json::Null,
    )
    .await;
    let b = ok(&n, BELIEF, Json::Null).await;
    assert_ne!(b[0]["status"], "accepted");
    let b = b[0]["basis"].clone();
    let validation = r#"CREATE ACTIVITY ?v {SET FIELDS {activity_class:"dependency_validation",status:"completed"}
      SET FACET "DependencyBasis" {basis_seq: :seq,groups:[{role:"all_of",pins:[{id: :source,version: :version}]}],policy_basis: :basis}
      SET STRUCTURAL {("inputs",:source) ("outputs","A-1")} }"#;
    let bad = run(
        &n,
        validation,
        json!({"seq":b["snapshot_seq"],"basis":b,"source":"C-2","version":1}),
    )
    .await;
    assert_eq!(bad.status, TopLevelStatus::Failed);
    ok(
        &n,
        validation,
        json!({"seq":b["snapshot_seq"],"basis":b,"source":"C-1","version":2}),
    )
    .await;
    assert_eq!(ok(&n, BELIEF, Json::Null).await[0]["status"], "accepted");
}

#[tokio::test]
async fn historical_projection_uses_retained_trust_and_policy() {
    let (n, _) = fresh("control_history").await;
    ok(&n, SETUP, Json::Null).await;
    ok(&n,r#"CREATE ASSERTION ?a { SET FIELDS {proposition:"P-1",asserted_by:"C-1",mode:"stated",stance:"support",confidence:0.9} }"#,Json::Null).await;
    let seq = n.store.get_space(DEFAULT_SPACE).await.unwrap().seq;
    n.system_session()
        .set_trust(
            DEFAULT_SPACE,
            1,
            std::collections::BTreeMap::from([("C-1".into(), 0.0)]),
            1.0,
        )
        .await
        .unwrap();
    assert_ne!(ok(&n, BELIEF, Json::Null).await[0]["status"], "accepted");
    let past = ok(&n, &format!("{BELIEF} AS OF SEQ {seq}"), Json::Null).await;
    assert_eq!(past[0]["status"], "accepted");
    assert_ne!(
        past[0]["basis"]["trust_version"],
        ok(&n, BELIEF, Json::Null).await[0]["basis"]["trust_version"]
    );
    let page = n
        .system_session()
        .change_page(DEFAULT_SPACE, seq, 100)
        .await
        .unwrap();
    assert_eq!(page["coverage"]["complete"], true);
    assert!(page["changes"].as_array().unwrap().iter().any(|e| {
        e["control_changes"]
            .as_array()
            .is_some_and(|cs| cs.iter().any(|c| c["kind"] == "trust"))
    }));
}

#[tokio::test]
async fn identity_withdrawal_restores_source_and_preserves_history() {
    let (n, _) = fresh("identity_withdrawal").await;
    ok(&n, SETUP, Json::Null).await;
    ok(
        &n,
        r#"CREATE CONCEPT ?alias {TYPE "Person" NAME "A. Lovelace"}"#,
        Json::Null,
    )
    .await;
    ok(&n, r#"MERGE CONCEPT "C-3" INTO "C-1""#, Json::Null).await;
    let seq = n.store.get_space(DEFAULT_SPACE).await.unwrap().seq;
    let decision = format!("identity:{DEFAULT_SPACE}#{seq}:C-3");
    ok(
        &n,
        r#"ASSERT (:a,"prefers",:b) { by: :actor,mode:"stated" }"#,
        json!({"a":"C-3","b":"C-2","actor":"C-1"}),
    )
    .await;
    let repaired = n
        .system_session()
        .withdraw_identity(DEFAULT_SPACE, &decision, seq, vec!["E-1".into()])
        .await
        .unwrap();
    assert!(repaired["identity_version"].as_u64().unwrap() > seq);
    let Element::Concept(source) = n.store.get_element("C-3".parse().unwrap()).await.unwrap()
    else {
        panic!()
    };
    assert!(source.merged_into.is_empty());
    let Element::Concept(past) = n
        .store
        .element_at(DEFAULT_SPACE, "C-3".parse().unwrap(), seq)
        .await
        .unwrap()
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(past.merged_into, "C-1");
    assert_eq!(
        n.system_session()
            .read_control(DEFAULT_SPACE, &decision, Some(seq))
            .await
            .unwrap()
            .unwrap()
            .value["status"],
        "active"
    );
    assert!(
        n.system_session()
            .withdraw_identity(DEFAULT_SPACE, &decision, seq, vec!["E-1".into()])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn revision_selection_and_unguarded_standing_changes() {
    let (n, _) = fresh("skill_revision_contract").await;
    let behavior = json!({"task_family":"test","procedure":"verify before act"});
    let digest = contracts::digest(&behavior).unwrap();
    let create = r#"MUTATE {
      CREATE CONCEPT ?skill {TYPE "Skill" NAME "verify" SET ATTRIBUTES {skill_class:"workflow",summary:"verify",status:"proposed"} SET STRUCTURAL {("current_revision",?revision)} }
      CREATE CONCEPT ?revision {TYPE "SkillRevision" SET ATTRIBUTES {task_family:"test",procedure:"verify before act",behavior_digest: :digest} SET STRUCTURAL {("revision_of",?skill)} }
    }"#;
    ok(&n, create, json!({"digest":digest})).await;
    assert_eq!(
        run(
            &n,
            r#"UPDATE "C-1" SET ATTRIBUTES {status:"adopted"}"#,
            Json::Null
        )
        .await
        .status,
        TopLevelStatus::Failed
    );
    let skill = n.store.get_element("C-1".parse().unwrap()).await.unwrap();
    assert_eq!(
        anda_cognitive_nexus::view::render(&skill)["attributes"]["status"],
        "proposed"
    );
    assert_eq!(
        run(
            &n,
            r#"UPDATE "C-2" SET ATTRIBUTES {procedure:"changed"}"#,
            Json::Null
        )
        .await
        .status,
        TopLevelStatus::Failed
    );
}

#[tokio::test]
async fn trial_entry_and_guarded_verdict_are_usable_without_a_brain_process() {
    use anda_kip::cognitive::{EvaluationPolicy, ObserverControl};
    let (n, _) = fresh("local_learning_flow").await;
    ok(&n, SETUP, Json::Null).await;
    let behavior = json!({"task_family":"test","procedure":"verify"});
    ok(&n,r#"MUTATE {
        CREATE CONCEPT ?s {TYPE "Skill" SET ATTRIBUTES {skill_class:"workflow",summary:"verify",status:"proposed"} SET STRUCTURAL {("current_revision",?r)}}
        CREATE CONCEPT ?r {TYPE "SkillRevision" SET ATTRIBUTES {task_family:"test",procedure:"verify",behavior_digest: :digest} SET STRUCTURAL {("revision_of",?s)}}
    }"#,json!({"digest":contracts::digest(&behavior).unwrap()})).await;
    let s = n.system_session();
    let rule = json!({"engine":"kip:binary-stratified-v1"});
    let parameters = json!({"alpha":0.05});
    let rule_pin = s
        .put_artifact(DEFAULT_SPACE, rule.clone(), vec![])
        .await
        .unwrap();
    let parameter_pin = s
        .put_artifact(DEFAULT_SPACE, parameters.clone(), vec![])
        .await
        .unwrap();
    let observers = vec![ObserverControl {
        principal_id: "independent-instrument".into(),
        configuration_digest: contracts::digest(&json!({"instrument":"test"})).unwrap(),
        control_domain: "independent-operator".into(),
    }];
    let observer_digest = contracts::digest(&serde_json::to_value(&observers).unwrap()).unwrap();
    let policy = s
        .set_evaluation_policy(
            DEFAULT_SPACE,
            0,
            EvaluationPolicy {
                id: "test-learning".into(),
                version: "1".into(),
                allowed_rules: vec![rule_pin.content_digest.clone()],
                allowed_parameters: vec![parameter_pin.content_digest.clone()],
                observers,
                observer_control_digest: observer_digest.clone(),
                minimum_independent_attempts: 2,
                allow_same_principal_observer: false,
                retain_adoption_on_insufficient: true,
            },
        )
        .await
        .unwrap();
    let basis = ok(&n, BELIEF, Json::Null).await[0]["basis"].clone();
    let replay=s.put_artifact(DEFAULT_SPACE,json!({"rule":rule,"parameters":parameters,"basis":basis,"baseline_attempts":{},"baseline_outcomes":{}}),vec!["C-4".into()]).await.unwrap();
    let trial = json!({"revision_refs":["C-4"],"basis":basis,"rule":rule_pin,"parameters":parameter_pin,"baseline_attempt_refs":[],"baseline_outcome_refs":[],"comparability":{"method":"stratified","environment_digest":contracts::digest(&json!({})).unwrap(),"strata_weights":{"all":1.0},"metric":"success","minimum_effect":0.0,"uncertainty_rule":"hoeffding","missingness_policy":"count_as_failure","observer_control_digest":observer_digest,"sampling_unit":"attempt","correlation_policy":"independent"},"quota":2,"observation_window":"one_action","replay_artifact":replay,"evaluation_policy":{"id":"test-learning","version":"1","content_digest":contracts::digest(&policy.value).unwrap()}});
    let result=ok(&n,&format!(r#"CREATE ACTIVITY ?trial {{SET FIELDS {{activity_class:"trial_open",status:"completed"}} SET FACET "TrialRecord" {trial} SET STRUCTURAL {{("inputs","C-4")}}}}"#),Json::Null).await;
    let trial_ref = result["handles"]["trial"].as_str().unwrap().to_string();
    let replay=s.put_artifact(DEFAULT_SPACE,json!({"rule":rule,"parameters":parameters,"trial_record":trial,"attempts":{},"outcomes":{}}),vec!["C-4".into(),trial_ref.clone()]).await.unwrap();
    let evaluation = json!({"trial_ref":trial_ref,"revision_refs":["C-4"],"from_status":"proposed","to_status":"trialed","rule_digest":rule_pin.content_digest,"parameters_digest":parameter_pin.content_digest,"cutoff":anda_cognitive_nexus::time::now(),"attempt_refs":[],"outcome_refs":[],"excluded_samples":[],"missing_attempt_refs":[],"comparison":{"status":"insufficient","effect":null,"uncertainty":{"method":"hoeffding","alpha":0.05}},"replay_artifact":replay});
    let mutation = format!(
        r#"MUTATE {{
        CREATE ACTIVITY ?verdict {{SET FIELDS {{activity_class:"lifecycle_verdict",status:"completed"}} SET FACET "EvaluationRecord" {evaluation} SET STRUCTURAL {{("inputs","C-4") ("inputs","{trial_ref}") ("outputs","C-3")}}}}
        UPDATE "C-3" SET ATTRIBUTES {{status:"trialed"}} SET FACET "TrialState" {{revision_ref:"C-4",trial_ref:"{trial_ref}"}} SET FACET "GradingState" {{revision_ref:"C-4",evaluation_ref:?verdict,success_count:0,failure_count:0,graded_count:0}} EXPECT VERSION 1
    }}"#
    );
    ok(&n, &mutation, Json::Null).await;
    let skill = n.store.get_element("C-3".parse().unwrap()).await.unwrap();
    let v = anda_cognitive_nexus::view::render(&skill);
    assert_eq!(v["attributes"]["status"], "trialed");
    assert!(
        v["facets"]["kip://profiles/cognitive-memory@2.1.0/GradingState"]["evaluation_ref"]
            .as_str()
            .unwrap()
            .starts_with("X-")
    );
    assert_eq!(
        run(
            &n,
            r#"UPDATE "C-3" SET FACET "GradingState" {success_count:99} EXPECT VERSION 2"#,
            Json::Null
        )
        .await
        .status,
        TopLevelStatus::Failed
    );
}

#[tokio::test]
async fn task_leases_and_watch_generations_are_persistent_cas() {
    let (n, db) = fresh("durable_primitives").await;
    ok(&n, SETUP, Json::Null).await;
    ok(&n,r#"MUTATE {
      CREATE CONCEPT ?task {TYPE "SleepTask" SET ATTRIBUTES {task_class:"review_skill",summary:"review",status:"pending"}}
      CREATE CONCEPT ?watch {TYPE "Watch" SET ATTRIBUTES {watch_class:"delta",summary:"name change",condition:{element:"C-1",ops:["update"],touched:["fields.name"]},status:"disarmed"}}
    }"#,Json::Null).await;
    let leased = n
        .system_session()
        .lease_task(DEFAULT_SPACE, "C-3", 1, "2099-01-01T00:00:00Z")
        .await
        .unwrap();
    assert_eq!(leased["lease"]["fencing_token"], 1);
    assert!(
        n.system_session()
            .lease_task(DEFAULT_SPACE, "C-3", 1, "2099-02-01T00:00:00Z")
            .await
            .is_err()
    );
    n.system_session()
        .arm_watch(DEFAULT_SPACE, "C-4", 1)
        .await
        .unwrap();
    ok(
        &n,
        r#"UPDATE "C-1" SET FIELDS {name:"Ada Byron"}"#,
        Json::Null,
    )
    .await;
    let restarted = CognitiveNexus::connect(db).await.unwrap();
    let fired = restarted
        .system_session()
        .advance_watch(DEFAULT_SPACE, "C-4", 2, 1, 100)
        .await
        .unwrap();
    assert_eq!(fired["status"], "fired");
    restarted
        .system_session()
        .arm_watch(DEFAULT_SPACE, "C-4", 3)
        .await
        .unwrap();
    assert!(
        restarted
            .system_session()
            .advance_watch(DEFAULT_SPACE, "C-4", 4, 1, 100)
            .await
            .is_err()
    );
    assert_eq!(
        run(
            &restarted,
            r#"UPDATE "C-3" SET ATTRIBUTES {status:"completed"}"#,
            Json::Null
        )
        .await
        .status,
        TopLevelStatus::Failed
    );
    ok(
        &restarted,
        r#"UPDATE "C-3" SET ATTRIBUTES {status:"completed"} EXPECT VERSION 2"#,
        Json::Null,
    )
    .await;
}

#[tokio::test]
async fn replay_payload_erasure_scrubs_owned_artifacts() {
    let (n, _) = fresh("governed_artifact_erasure").await;
    ok(&n, SETUP, Json::Null).await;
    let pin = n
        .system_session()
        .put_artifact(
            DEFAULT_SPACE,
            json!({"source":"material"}),
            vec!["E-1".into()],
        )
        .await
        .unwrap();
    assert_eq!(
        n.system_session()
            .read_artifact(DEFAULT_SPACE, &pin)
            .await
            .unwrap()["source"],
        "material"
    );
    ok(&n, r#"PURGE PAYLOAD "E-1" CONFIRM "PURGE""#, Json::Null).await;
    assert!(
        n.system_session()
            .read_artifact(DEFAULT_SPACE, &pin)
            .await
            .is_err()
    );
    assert!(
        n.system_session()
            .put_artifact(
                DEFAULT_SPACE,
                json!({"source":"material"}),
                vec!["E-1".into()]
            )
            .await
            .is_err()
    );
    let head = n.store.get_space(DEFAULT_SPACE).await.unwrap().seq;
    let plan = json!({"scope":"payload_only","basis_seq":head,"source_event_refs":["E-1"],"targets":[{"ref":"E-1","surface":"payload","state":"erased"},{"ref":pin.artifact_ref,"surface":"replay","state":"erased"}],"external_exports":[],"status":"completed","receipts":[]});
    n.system_session()
        .validate_erasure_plan(DEFAULT_SPACE, &plan)
        .await
        .unwrap();
    assert_eq!(
        n.store.get_space(DEFAULT_SPACE).await.unwrap().seq,
        head,
        "validation is read-only"
    );
    let mut unverified = plan;
    unverified["targets"]
        .as_array_mut()
        .unwrap()
        .push(json!({"ref":"backup-1","surface":"backup","state":"erased"}));
    assert!(
        n.system_session()
            .validate_erasure_plan(DEFAULT_SPACE, &unverified)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn interrupted_governance_delivery_requires_a_new_coverage_basis() {
    let (n, db) = fresh("control_delivery_recovery").await;
    let before = n.store.get_space(DEFAULT_SPACE).await.unwrap().seq;
    let principal = n
        .governance()
        .find_principal(anda_cognitive_nexus::governance::SYSTEM_PRINCIPAL)
        .await
        .unwrap()
        .unwrap();
    // Simulate a process loss after control data reached storage but before its
    // audit/Space notification. The old checkpoint must not prove silence.
    let table = db
        .open_collection("gov_principals".into(), async |_| Ok(()))
        .await
        .unwrap();
    table
        .update(
            principal._id,
            std::collections::BTreeMap::from([(
                "display_name".into(),
                anda_db_schema::Fv::Text("recovered control edit".into()),
            )]),
        )
        .await
        .unwrap();
    table.flush(1).await.unwrap();
    let recovered = CognitiveNexus::connect(db).await.unwrap();
    let page = recovered
        .system_session()
        .change_page(DEFAULT_SPACE, before, 100)
        .await
        .unwrap();
    assert_eq!(page["resync_required"], true);
    assert_eq!(page["coverage"]["complete"], false);
    let head = recovered.store.get_space(DEFAULT_SPACE).await.unwrap().seq;
    assert_eq!(
        recovered
            .system_session()
            .change_page(DEFAULT_SPACE, head, 100)
            .await
            .unwrap()["coverage"]["complete"],
        true
    );
}

#[tokio::test]
async fn redo_recovers_all_rows_after_partial_application() {
    let (n, db) = fresh("commit_replay").await;
    ok(&n, SETUP, Json::Null).await;
    let cx = n
        .store
        .begin_transaction(DEFAULT_SPACE, json!({"principal_id":"test"}))
        .await
        .unwrap();
    let mut writes = vec![];
    for id in ["C-1", "C-2"] {
        let Element::Concept(mut row) = n.store.get_element(id.parse().unwrap()).await.unwrap()
        else {
            panic!()
        };
        row.name = "recovered".into();
        row.version += 1;
        row.seq = cx.seq;
        row.updated_tx = cx.tx_id.clone();
        row.updated_at = cx.at.clone();
        writes.push((Element::Concept(row), "update".to_string()));
    }
    let plan = CommitPlan {
        cx: cx.clone(),
        journal: JournalEntry {
            status: "committed".into(),
            transaction_class: "cognitive".into(),
            ..Default::default()
        },
        writes: writes.clone(),
        controls: vec![],
        control_replacements: vec![],
        space: None,
        purge_versions: vec![],
        scrub_versions: vec![],
        audits: vec![],
        approvals: vec![],
    };
    n.store.flush(1).await.unwrap();
    n.store
        .commit_log()
        .add_from(&CommitLogRow {
            _id: 0,
            tx_id: cx.tx_id.clone(),
            plan: serde_json::to_value(plan).unwrap(),
        })
        .await
        .unwrap();
    n.store.commit_log().flush(2).await.unwrap();
    if let Element::Concept(row) = &writes[0].0 {
        n.store.put(row.as_ref()).await.unwrap();
    }
    n.store.concepts().flush(3).await.unwrap();
    let recovered = CognitiveNexus::connect(db).await.unwrap();
    for id in ["C-1", "C-2"] {
        let row = recovered
            .store
            .get_element(id.parse().unwrap())
            .await
            .unwrap();
        assert_eq!(
            anda_cognitive_nexus::view::render(&row)["name"],
            "recovered"
        );
    }
    assert!(
        recovered
            .store
            .find_transaction(&cx.tx_id)
            .await
            .unwrap()
            .is_some()
    );
    recovered.store.recover_commits().await.unwrap();
}

#[tokio::test]
async fn dispatch_keeps_external_identity_and_never_upgrades_a_stale_fence() {
    use anda_kip::cognitive::DispatchRequest;
    let (n, _) = fresh("dispatch_recovery_contract").await;
    ok(&n, SETUP, Json::Null).await;
    ok(&n,r#"CREATE CONCEPT ?task {TYPE "SleepTask" SET ATTRIBUTES {task_class:"review_skill",summary:"action",status:"pending"}}"#,Json::Null).await;
    let s = n.system_session();
    s.lease_task(DEFAULT_SPACE, "C-3", 1, "2099-01-01T00:00:00Z")
        .await
        .unwrap();
    let selection = s
        .put_artifact(
            DEFAULT_SPACE,
            json!({"selection":"explicit action without a Skill"}),
            vec![],
        )
        .await
        .unwrap();
    let basis = ok(&n, BELIEF, Json::Null).await[0]["basis"].clone();
    let decision = json!({"decision":"act","retrieved_refs":["E-1"],"used_refs":["E-1"],"applied_revisions":[],"basis":basis});
    let dependency = json!({"basis_seq":basis["snapshot_seq"],"groups":[{"role":"all_of","pins":[{"id":"E-1","version":1}]}],"policy_basis":basis});
    let gate=ok(&n,&format!(r#"CREATE ACTIVITY ?gate {{SET FIELDS {{activity_class:"action_gate",status:"completed"}} SET FACET "DecisionRecord" {decision} SET FACET "DependencyBasis" {dependency} SET STRUCTURAL {{("inputs","E-1")}}}}"#),Json::Null).await["handles"]["gate"].as_str().unwrap().to_string();
    for (key, idempotent) in [("unsafe-effect", false), ("idempotent-effect", true)] {
        let record = json!({"attempt_id":key,"decision_ref":gate,"applied_revisions":[],"trial_ref":null,"context":{},"environment_digest":contracts::digest(&json!({})).unwrap(),"tool_versions":{"test":"1"},"selection_policy":selection,"preconditions_satisfied":"yes","started_at":anda_cognitive_nexus::time::now()});
        let result=ok(&n,&format!(r#"CREATE ACTIVITY ?attempt {{SET FIELDS {{activity_class:"action_attempt",status:"completed"}} SET FACET "AttemptRecord" {record} SET STRUCTURAL {{("inputs","{gate}")}}}}"#),Json::Null).await;
        let attempt = result["handles"]["attempt"].as_str().unwrap().to_string();
        let duplicate=run(&n,&format!(r#"CREATE ACTIVITY ?duplicate {{SET FIELDS {{activity_class:"action_attempt",status:"completed"}} SET FACET "AttemptRecord" {record}}}"#),Json::Null).await;
        assert_eq!(duplicate.status, TopLevelStatus::Failed);
        s.enqueue_dispatch(
            DEFAULT_SPACE,
            DispatchRequest {
                attempt_ref: attempt,
                task_ref: "C-3".into(),
                fencing_token: 1,
                supports_idempotency: idempotent,
                supports_outcome_lookup: false,
            },
        )
        .await
        .unwrap();
        let first = s.begin_dispatch(DEFAULT_SPACE, key, 1, 1).await.unwrap();
        assert_eq!(first["action"], "dispatch");
        assert_eq!(first["idempotency_key"], key);
    }
    assert_eq!(
        s.begin_dispatch(DEFAULT_SPACE, "unsafe-effect", 2, 1)
            .await
            .unwrap()["action"],
        "outcome_unknown"
    );
    ok(
        &n,
        r#"UPDATE "C-3" SET ATTRIBUTES {status:"completed"} EXPECT VERSION 2"#,
        Json::Null,
    )
    .await;
    ok(
        &n,
        r#"UPDATE "C-3" SET ATTRIBUTES {status:"pending"} EXPECT VERSION 3"#,
        Json::Null,
    )
    .await;
    let lease = s
        .lease_task(DEFAULT_SPACE, "C-3", 4, "2099-02-01T00:00:00Z")
        .await
        .unwrap();
    assert_eq!(lease["lease"]["fencing_token"], 2);
    assert!(
        s.begin_dispatch(DEFAULT_SPACE, "idempotent-effect", 2, 1)
            .await
            .is_err()
    );
    let resumed = s
        .begin_dispatch(DEFAULT_SPACE, "idempotent-effect", 2, 2)
        .await
        .unwrap();
    assert_eq!(resumed["action"], "dispatch");
    assert_eq!(resumed["idempotency_key"], "idempotent-effect");
}
