use super::*;
use anda_cognitive_nexus::attention::*;
use std::sync::atomic::{AtomicBool, Ordering};

fn pin(id: &str) -> RuntimePin {
    RuntimePin {
        id: id.into(),
        digest: format!("sha256:{}", "a".repeat(64)),
    }
}

async fn semantic_config(f: &Fixture) {
    let s = f.nexus.system_session();
    let row = s
        .read_control(DEFAULT_SPACE, "attention/config", None)
        .await
        .unwrap()
        .unwrap();
    let mut c: AttentionConfig = serde_json::from_value(row.value).unwrap();
    c.pins.evaluator = Some(pin("semantic-test"));
    s.set_attention_config(DEFAULT_SPACE, row.version, c)
        .await
        .unwrap();
}

async fn semantic_delta() -> Fixture {
    let mut f = Fixture::new().await;
    semantic_config(&f).await;
    let armed = f
        .nexus
        .system_session()
        .rearm_watch(
            DEFAULT_SPACE,
            &f.watch,
            f.version,
            json!({"element":f.target,"text":"a reply"}),
            None,
        )
        .await
        .unwrap();
    f.version = f
        .nexus
        .store
        .get_element(f.watch.parse().unwrap())
        .await
        .unwrap()
        .version();
    f.generation = armed["watch"]["arm_generation"].as_u64().unwrap();
    run(
        &f.nexus,
        r#"UPDATE :target SET FIELDS {name:"new semantic reply"}"#,
        json!({"target":f.target}),
    )
    .await;
    f
}

fn judgments(prepared: &PreparedWatchPage, key: &str, value: WatchMatch) -> WatchEvaluation {
    WatchEvaluation {
        evaluation_key: key.into(),
        evaluator: prepared.evaluator.clone(),
        judgments: prepared
            .candidates
            .iter()
            .map(|c| WatchJudgment {
                candidate_id: c.id.clone(),
                result: value,
                rationale: "Deterministic fixture judgment over the pinned transition".into(),
            })
            .collect(),
    }
}

#[tokio::test]
async fn wake_pages_are_bounded_snapshot_reads_with_empty_intermediate_pages() {
    let f = Fixture::new().await;
    let result = f.advance().await;
    let root = result["wake_ref"].as_str().unwrap();
    let s = f.nexus.system_session();
    s.claim_wake(DEFAULT_SPACE, root, 1, 0, &expiry_in(120))
        .await
        .unwrap();
    s.finish_wake(
        DEFAULT_SPACE,
        root,
        2,
        1,
        "",
        Default::default(),
        (0..4)
            .map(|i| WakeContinuation {
                key: format!("child-{i}"),
                not_before_ms: 0,
            })
            .collect(),
    )
    .await
    .unwrap();
    let before = f.nexus.store.get_space(DEFAULT_SPACE).await.unwrap().seq;
    let first = s.list_wakes(DEFAULT_SPACE, None, 1).await.unwrap();
    assert_eq!(
        before,
        f.nexus.store.get_space(DEFAULT_SPACE).await.unwrap().seq
    );
    assert!(first.items.is_empty() && !first.complete && first.scanned == 1);
    let snapshot = first.snapshot_seq;
    let mut cursor = first.next_cursor;
    let mut refs = std::collections::BTreeSet::new();
    let mut pages = 0;
    while let Some(next) = cursor {
        let page = s.list_wakes(DEFAULT_SPACE, Some(&next), 1).await.unwrap();
        assert_eq!(page.snapshot_seq, snapshot);
        assert!(page.scanned <= 1);
        for item in page.items {
            assert!(refs.insert(item.wake_ref));
        }
        cursor = page.next_cursor;
        pages += 1;
        assert!(pages < 20);
    }
    assert_eq!(refs.len(), 5);
    assert!(s.list_wakes(DEFAULT_SPACE, None, 0).await.is_err());
    let cursor = s
        .list_wakes(DEFAULT_SPACE, None, 1)
        .await
        .unwrap()
        .next_cursor
        .unwrap();
    s.set_trust(DEFAULT_SPACE, 1, Default::default(), 0.9)
        .await
        .unwrap();
    assert!(s.list_wakes(DEFAULT_SPACE, Some(&cursor), 1).await.is_err());
    f.nexus.close().await.unwrap();
}

struct Ready(AtomicBool);
#[async_trait::async_trait]
impl WakeResumeVerifier for Ready {
    async fn verify(&self, input: WakeResumeInput) -> Result<bool, anda_kip::KipError> {
        assert_eq!(input.condition, json!({"ready":"binding"}));
        Ok(self.0.load(Ordering::SeqCst))
    }
}

#[tokio::test]
async fn on_change_resume_requires_registered_code_and_replays_after_success() {
    let f = Fixture::new().await;
    let result = f.advance().await;
    let wake = result["wake_ref"].as_str().unwrap();
    let s = f.nexus.system_session();
    s.claim_wake(DEFAULT_SPACE, wake, 1, 0, &expiry_in(120))
        .await
        .unwrap();
    let condition = json!({"ready":"binding"});
    let digest = anda_cognitive_nexus::content_digest(&condition).unwrap();
    s.block_wake(
        DEFAULT_SPACE,
        wake,
        2,
        1,
        WakeRetry {
            reason: "binding_unavailable".into(),
            resume: WakeResume::OnChange {
                condition_digest: digest.clone(),
            },
        },
    )
    .await
    .unwrap();
    assert!(s.resume_wake(DEFAULT_SPACE, wake, 3, 1).await.is_err());
    let verifier = Arc::new(Ready(AtomicBool::new(false)));
    assert_eq!(
        f.nexus
            .register_wake_resume_verifier(condition.clone(), pin("ready"), verifier.clone())
            .unwrap(),
        digest
    );
    assert!(
        f.nexus
            .register_wake_resume_verifier(condition, pin("other"), verifier.clone())
            .is_err()
    );
    assert!(s.resume_wake(DEFAULT_SPACE, wake, 3, 1).await.is_err());
    assert_eq!(s.read_wake(DEFAULT_SPACE, wake).await.unwrap().version, 3);
    verifier.0.store(true, Ordering::SeqCst);
    let resumed = s.resume_wake(DEFAULT_SPACE, wake, 3, 1).await.unwrap();
    assert_eq!(resumed["wake"]["state"]["stage"], "pending");
    assert_eq!(resumed["resume_verification"]["verifier"]["id"], "ready");
    assert_eq!(
        resumed,
        s.resume_wake(DEFAULT_SPACE, wake, 3, 1).await.unwrap()
    );
    f.nexus.close().await.unwrap();
}

struct Waiting {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl WakeResumeVerifier for Waiting {
    async fn verify(&self, _: WakeResumeInput) -> Result<bool, anda_kip::KipError> {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(true)
    }
}

#[tokio::test]
async fn async_resume_does_not_hold_the_lock_or_override_a_cancellation() {
    let f = Fixture::new().await;
    let result = f.advance().await;
    let wake = result["wake_ref"].as_str().unwrap().to_string();
    let s = f.nexus.system_session();
    s.claim_wake(DEFAULT_SPACE, &wake, 1, 0, &expiry_in(120))
        .await
        .unwrap();
    let verifier = Arc::new(Waiting {
        entered: Default::default(),
        release: Default::default(),
    });
    let condition = f
        .nexus
        .register_wake_resume_verifier(json!({"wait":true}), pin("waiting"), verifier.clone())
        .unwrap();
    s.block_wake(
        DEFAULT_SPACE,
        &wake,
        2,
        1,
        WakeRetry {
            reason: "binding_unavailable".into(),
            resume: WakeResume::OnChange {
                condition_digest: condition,
            },
        },
    )
    .await
    .unwrap();
    let worker = s.clone();
    let target = wake.clone();
    let task = tokio::spawn(async move { worker.resume_wake(DEFAULT_SPACE, &target, 3, 1).await });
    verifier.entered.notified().await;
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        s.read_wake(DEFAULT_SPACE, &wake),
    )
    .await
    .unwrap()
    .unwrap();
    s.cancel_wake(DEFAULT_SPACE, &wake, 3, 1, "cancel while checking")
        .await
        .unwrap();
    verifier.release.notify_one();
    assert!(task.await.unwrap().is_err());
    assert!(matches!(
        s.read_wake(DEFAULT_SPACE, &wake).await.unwrap().state,
        WakeState::Cancelled { .. }
    ));
    f.nexus.close().await.unwrap();
}

#[tokio::test]
async fn semantic_page_rejects_omissions_unknowns_and_false_complete_flags() {
    // Keep fixture setup on a separate task from this long validation path.
    let f = tokio::spawn(semantic_delta()).await.unwrap();
    let s = f.nexus.system_session();
    let result = s
        .prepare_watch_page(
            DEFAULT_SPACE,
            &f.watch,
            f.version,
            f.generation,
            200,
            "page-1",
        )
        .await
        .unwrap();
    let p: PreparedWatchPage = serde_json::from_value(result["prepared"].clone()).unwrap();
    assert_eq!(p.candidates.len(), 1);
    assert_eq!(p.candidates[0].after["name"], "new semantic reply");
    assert_ne!(
        p.candidates[0].before.as_ref().unwrap()["name"],
        "new semantic reply"
    );
    // Later writes must not change the page's event-time material or results.
    run(
        &f.nexus,
        r#"UPDATE :target SET FIELDS {name:"later value"}"#,
        json!({"target":f.target}),
    )
    .await;
    let repeated = s
        .prepare_watch_page(
            DEFAULT_SPACE,
            &f.watch,
            f.version,
            f.generation,
            200,
            "page-1",
        )
        .await
        .unwrap();
    assert_eq!(result, repeated);
    let mut unexplained = judgments(&p, "unexplained", WatchMatch::Match);
    unexplained.judgments[0].rationale.clear();
    assert!(
        s.commit_watch_page(DEFAULT_SPACE, &p.ticket_ref, unexplained)
            .await
            .is_err()
    );
    let mut missing = judgments(&p, "missing", WatchMatch::NoMatch);
    missing.judgments.clear();
    assert!(
        s.commit_watch_page(DEFAULT_SPACE, &p.ticket_ref, missing)
            .await
            .is_err()
    );
    let unknown = s
        .commit_watch_page(
            DEFAULT_SPACE,
            &p.ticket_ref,
            judgments(&p, "unknown", WatchMatch::Unknown),
        )
        .await
        .unwrap();
    assert_eq!(unknown["status"], "deferred");
    assert_eq!(
        f.nexus
            .store
            .get_element(f.watch.parse().unwrap())
            .await
            .unwrap()
            .version(),
        f.version
    );
    let mut forged = serde_json::to_value(judgments(&p, "forged", WatchMatch::Match)).unwrap();
    forged["complete"] = json!(true);
    assert!(serde_json::from_value::<WatchEvaluation>(forged).is_err());
    let done = s
        .commit_watch_page(
            DEFAULT_SPACE,
            &p.ticket_ref,
            judgments(&p, "accepted", WatchMatch::Match),
        )
        .await
        .unwrap();
    assert_eq!(done["status"], "fired");
    let record = s
        .read_control(
            DEFAULT_SPACE,
            done["evaluation_ref"].as_str().unwrap(),
            None,
        )
        .await
        .unwrap()
        .unwrap();
    let material: anda_kip::cognitive::ArtifactPin =
        serde_json::from_value(record.value["material"].clone()).unwrap();
    let reasoning = s.read_artifact(DEFAULT_SPACE, &material).await.unwrap();
    assert!(
        reasoning
            .to_string()
            .contains("Deterministic fixture judgment")
    );
    let journal = f
        .nexus
        .store
        .find_transaction(done["receipt"]["tx_id"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(
        !journal
            .result
            .to_string()
            .contains("Deterministic fixture judgment")
    );
    assert_eq!(
        done,
        s.commit_watch_page(
            DEFAULT_SPACE,
            &p.ticket_ref,
            judgments(&p, "accepted", WatchMatch::Match)
        )
        .await
        .unwrap()
    );
    assert!(
        s.commit_watch_page(
            DEFAULT_SPACE,
            &p.ticket_ref,
            judgments(&p, "accepted", WatchMatch::NoMatch)
        )
        .await
        .is_err()
    );
    run(
        &f.nexus,
        r#"PURGE :target CONFIRM "PURGE""#,
        json!({"target":f.target}),
    )
    .await;
    assert!(s.read_artifact(DEFAULT_SPACE, &material).await.is_err());
    f.nexus.close().await.unwrap();
}

#[tokio::test]
async fn prepared_page_survives_restart_but_old_generation_cannot_apply_it() {
    let f = semantic_delta().await;
    let s = f.nexus.system_session();
    let r = s
        .prepare_watch_page(
            DEFAULT_SPACE,
            &f.watch,
            f.version,
            f.generation,
            200,
            "persistent",
        )
        .await
        .unwrap();
    let p: PreparedWatchPage = serde_json::from_value(r["prepared"].clone()).unwrap();
    let f = f.reopen().await;
    let s = f.nexus.system_session();
    assert_eq!(
        s.read_prepared_watch_page(DEFAULT_SPACE, &p.ticket_ref)
            .await
            .unwrap()
            .page_digest,
        p.page_digest
    );
    s.arm_watch(DEFAULT_SPACE, &f.watch, f.version)
        .await
        .unwrap();
    assert!(
        s.commit_watch_page(
            DEFAULT_SPACE,
            &p.ticket_ref,
            judgments(&p, "late", WatchMatch::Match)
        )
        .await
        .is_err()
    );
    assert_eq!(
        run(
            &f.nexus,
            r#"FIND(?a.id) WHERE {?a ACTIVITY {activity_class:"watch_fire"}}"#,
            Value::Null
        )
        .await,
        json!([])
    );
    f.nexus.close().await.unwrap();
}

#[tokio::test]
async fn prepared_before_deadline_cannot_claim_deadline_coverage_after_waiting() {
    let f = Fixture::new().await;
    semantic_config(&f).await;
    let due = expiry_in(1);
    let made=run(&f.nexus,r#"CREATE CONCEPT ?watch {TYPE "Watch" SET ATTRIBUTES {watch_class:"silence",summary:"deadline",condition:{element: :target,text:"no reply"},due_at: :due,status:"disarmed"}}"#,json!({"target":f.target,"due":due})).await;
    let watch = made["handles"]["watch"].as_str().unwrap();
    let s = f.nexus.system_session();
    s.arm_watch(DEFAULT_SPACE, watch, 1).await.unwrap();
    let first = s
        .prepare_watch_page(DEFAULT_SPACE, watch, 2, 1, 200, "before")
        .await
        .unwrap();
    let p: PreparedWatchPage = serde_json::from_value(first["prepared"].clone()).unwrap();
    assert!(!p.deadline_covered && p.candidates.is_empty());
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    run(
        &f.nexus,
        r#"UPDATE :target SET FIELDS {name:"after deadline"}"#,
        json!({"target":f.target}),
    )
    .await;
    let partial = s
        .commit_watch_page(
            DEFAULT_SPACE,
            &p.ticket_ref,
            judgments(&p, "before-eval", WatchMatch::NoMatch),
        )
        .await
        .unwrap();
    assert_eq!(partial["status"], "armed");
    let version = f
        .nexus
        .store
        .get_element(watch.parse().unwrap())
        .await
        .unwrap()
        .version();
    let next = s
        .prepare_watch_page(DEFAULT_SPACE, watch, version, 1, 200, "after")
        .await
        .unwrap();
    let p: PreparedWatchPage = serde_json::from_value(next["prepared"].clone()).unwrap();
    assert!(p.deadline_covered && p.candidates.is_empty());
    let fired = s
        .commit_watch_page(
            DEFAULT_SPACE,
            &p.ticket_ref,
            judgments(&p, "after-eval", WatchMatch::NoMatch),
        )
        .await
        .unwrap();
    assert_eq!(fired["status"], "fired");
    f.nexus.close().await.unwrap();
}

async fn lookup_fixture(
    register_observer: bool,
) -> (
    Fixture,
    String,
    String,
    anda_cognitive_nexus::nexus::Session,
) {
    use anda_cognitive_nexus::governance::{
        AuthContext,
        store::{GrantDraft, PrincipalDraft},
    };
    let mut f = Fixture::new().await;
    let s = f.nexus.system_session();
    let principal = "kip:principal:lookup-instrument";
    f.nexus
        .governance()
        .ensure_principal(PrincipalDraft {
            principal_id: principal.into(),
            principal_class: "service".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    f.nexus
        .governance()
        .create_grant(
            GrantDraft {
                space_id: DEFAULT_SPACE.into(),
                grantee_principal: principal.into(),
                actions: vec![
                    "read".into(),
                    "record_outcome".into(),
                    "read_governance_history".into(),
                ],
                ..Default::default()
            },
            "kip:principal:system",
        )
        .await
        .unwrap();
    let saved = s
        .read_control(DEFAULT_SPACE, "attention/config", None)
        .await
        .unwrap()
        .unwrap();
    let mut cfg: AttentionConfig = serde_json::from_value(saved.value).unwrap();
    cfg.pins.binding = Some(pin("lookup-executor"));
    s.set_attention_config(DEFAULT_SPACE, saved.version, cfg)
        .await
        .unwrap();
    if register_observer {
        s.set_dispatch_lookup_observer(
            DEFAULT_SPACE,
            0,
            DispatchLookupObserver {
                binding: pin("lookup-executor"),
                principal_id: principal.into(),
                configuration_digest: pin("observer").digest,
            },
        )
        .await
        .unwrap();
    }
    let armed = s
        .arm_watch(DEFAULT_SPACE, &f.watch, f.version)
        .await
        .unwrap();
    f.generation = armed["watch"]["arm_generation"].as_u64().unwrap();
    f.version = f
        .nexus
        .store
        .get_element(f.watch.parse().unwrap())
        .await
        .unwrap()
        .version();
    run(
        &f.nexus,
        r#"UPDATE :target SET FIELDS {name:"wake lookup"}"#,
        json!({"target":f.target}),
    )
    .await;
    let result = f.advance().await;
    let wake = result["wake_ref"].as_str().unwrap().to_string();
    s.claim_wake(DEFAULT_SPACE, &wake, 1, 0, &expiry_in(120))
        .await
        .unwrap();
    let made=run(&f.nexus,r#"MUTATE {CREATE CONCEPT ?preference {TYPE "Preference" NAME "lookup"} ENSURE PROPOSITION ?basis (:target,"prefers",?preference)}"#,json!({"target":f.target})).await;
    let projection = run(
        &f.nexus,
        "FIND(?b) WHERE {?p PROPOSITION(id: :id) ?b BELIEF(?p)}",
        json!({"id":made["handles"]["basis"]}),
    )
    .await;
    let basis = &projection[0]["basis"];
    let selection = s
        .put_artifact(DEFAULT_SPACE, json!({"policy":"lookup-test"}), vec![])
        .await
        .unwrap();
    let result=run(&f.nexus,r#"MUTATE {
      CREATE ACTIVITY ?decision {SET FIELDS {activity_class:"action_gate",status:"completed"}
        SET FACET "DecisionRecord" {decision:"act",retrieved_refs:[:watch],used_refs:[:watch],applied_revisions:[],basis: :basis}
        SET FACET "DependencyBasis" {basis_seq: :seq,policy_basis: :basis,groups:[{role:"context",pins:[{id: :watch,version: :version},{id: :prop,version:1}]}]}
        SET STRUCTURAL {("inputs",:watch) ("inputs",:prop)}}
      CREATE ACTIVITY ?attempt {SET FIELDS {activity_class:"action_attempt",status:"completed"}
        SET FACET "AttemptRecord" {attempt_id:"lookup-test",decision_ref:?decision,applied_revisions:[],trial_ref:null,context:{task_family:"lookup.test"},environment_digest: :environment,tool_versions:{fixture:"v1"},selection_policy: :selection,preconditions_satisfied:"yes",started_at: :started}
        SET STRUCTURAL {("inputs",?decision)}}
    }"#,json!({"watch":f.watch,"version":f.nexus.store.get_element(f.watch.parse().unwrap()).await.unwrap().version(),"basis":basis,"seq":basis["snapshot_seq"],"prop":made["handles"]["basis"],"environment":pin("env").digest,"selection":selection,"started":anda_cognitive_nexus::time::now()})).await;
    let attempt = result["handles"]["attempt"].as_str().unwrap().to_string();
    let mut auth = AuthContext::principal(principal);
    auth.auth_method = "authenticated-test-instrument".into();
    let observer = f.nexus.session(auth);
    (f, wake, attempt, observer)
}

#[tokio::test]
async fn not_started_reconciliation_is_authenticated_cas_guarded_and_not_an_outcome() {
    // Poll the large setup future as a separate task so its stack frame does
    // not nest under the reconciliation test's own future.
    let (f, wake, attempt, observer) = tokio::spawn(lookup_fixture(true)).await.unwrap();
    let s = f.nexus.system_session();
    let first = s
        .begin_wake_dispatch(DEFAULT_SPACE, &wake, 2, 1, &attempt, false, true)
        .await
        .unwrap();
    assert_eq!(first["action"], "dispatch");
    let reference = first["dispatch_ref"].as_str().unwrap();
    assert!(
        observer
            .read_control(DEFAULT_SPACE, reference, None)
            .await
            .is_err()
    );
    let next = s
        .begin_wake_dispatch(DEFAULT_SPACE, &wake, 2, 1, &attempt, false, true)
        .await
        .unwrap();
    assert_eq!(next["action"], "lookup");
    let observation = DispatchLookup {
        observation_key: "not-started-1".into(),
        observed_at: anda_cognitive_nexus::time::now(),
        configuration_digest: pin("observer").digest,
        status: DispatchLookupStatus::NotStarted,
    };
    assert!(
        s.reconcile_wake_lookup(DEFAULT_SPACE, reference, 2, observation.clone())
            .await
            .is_err()
    );
    let ready = observer
        .reconcile_wake_lookup(DEFAULT_SPACE, reference, 2, observation.clone())
        .await
        .unwrap();
    assert_eq!(ready["intent"]["state"], "ready");
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    let resend = s
        .begin_wake_dispatch(DEFAULT_SPACE, &wake, 2, 1, &attempt, false, true)
        .await
        .unwrap();
    assert_eq!(resend["action"], "dispatch");
    assert_eq!(resend["idempotency_key"], first["idempotency_key"]);
    let mut delayed = observation.clone();
    delayed.observation_key = "delayed-not-started".into();
    assert!(
        observer
            .reconcile_wake_lookup(DEFAULT_SPACE, reference, 4, delayed)
            .await
            .is_err()
    );
    let future = DispatchLookup {
        observation_key: "future".into(),
        observed_at: expiry_in(60),
        ..observation.clone()
    };
    assert!(
        observer
            .reconcile_wake_lookup(DEFAULT_SPACE, reference, 4, future)
            .await
            .is_err()
    );
    assert_eq!(
        ready,
        observer
            .reconcile_wake_lookup(DEFAULT_SPACE, reference, 2, observation)
            .await
            .unwrap()
    );
    assert_eq!(
        s.read_control(DEFAULT_SPACE, reference, None)
            .await
            .unwrap()
            .unwrap()
            .value["state"],
        "dispatching"
    );
    let finished = DispatchLookup {
        observation_key: "finished".into(),
        observed_at: anda_cognitive_nexus::time::now(),
        configuration_digest: pin("observer").digest,
        status: DispatchLookupStatus::Finished,
    };
    assert!(
        observer
            .reconcile_wake_lookup(DEFAULT_SPACE, reference, 2, finished.clone())
            .await
            .is_err()
    );
    let state = observer
        .reconcile_wake_lookup(DEFAULT_SPACE, reference, 4, finished)
        .await
        .unwrap();
    assert_eq!(state["intent"]["state"], "dispatching");
    assert_eq!(
        run(
            &f.nexus,
            r#"FIND(?e.id) WHERE {?e EVIDENCE {evidence_class:"outcome"}}"#,
            Value::Null
        )
        .await,
        json!([])
    );
    assert!(
        s.finish_wake(DEFAULT_SPACE, &wake, 2, 1, "", Default::default(), vec![])
            .await
            .is_err()
    );
    f.nexus.close().await.unwrap();
}

#[tokio::test]
async fn lookup_dispatch_requires_a_registered_observer() {
    let (fixture, wake, attempt, _) = tokio::spawn(lookup_fixture(false)).await.unwrap();
    let error = fixture
        .nexus
        .system_session()
        .begin_wake_dispatch(DEFAULT_SPACE, &wake, 2, 1, &attempt, false, true)
        .await
        .unwrap_err();
    assert_eq!(error.name(), "UnsupportedCapability");
    for id in fixture.nexus.store.control_records().ids() {
        let row: anda_cognitive_nexus::store::rows::ControlRecordRow = fixture
            .nexus
            .store
            .control_records()
            .get_as(id)
            .await
            .unwrap();
        assert!(!(row.kind == "dispatch" && row.key.starts_with("dispatch/v1/")));
    }
    fixture.nexus.close().await.unwrap();
}

#[tokio::test]
async fn prepared_material_is_revocable_and_never_copied_into_the_replay_journal() {
    let f = semantic_delta().await;
    let s = f.nexus.system_session();
    let result = s
        .prepare_watch_page(
            DEFAULT_SPACE,
            &f.watch,
            f.version,
            f.generation,
            200,
            "erasable",
        )
        .await
        .unwrap();
    let p: PreparedWatchPage = serde_json::from_value(result["prepared"].clone()).unwrap();
    let transaction = f
        .nexus
        .store
        .find_transaction(result["receipt"]["tx_id"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(!transaction.result.to_string().contains("a reply"));
    run(
        &f.nexus,
        r#"PURGE :target CONFIRM "PURGE""#,
        json!({"target":f.target}),
    )
    .await;
    assert!(
        s.read_prepared_watch_page(DEFAULT_SPACE, &p.ticket_ref)
            .await
            .is_err()
    );
    assert!(
        s.commit_watch_page(
            DEFAULT_SPACE,
            &p.ticket_ref,
            judgments(&p, "erased", WatchMatch::Match)
        )
        .await
        .is_err()
    );
    f.nexus.close().await.unwrap();
}

#[tokio::test]
async fn changing_evaluator_configuration_invalidates_an_uncommitted_page() {
    let f = semantic_delta().await;
    let s = f.nexus.system_session();
    let result = s
        .prepare_watch_page(
            DEFAULT_SPACE,
            &f.watch,
            f.version,
            f.generation,
            200,
            "config-pin",
        )
        .await
        .unwrap();
    let p: PreparedWatchPage = serde_json::from_value(result["prepared"].clone()).unwrap();
    let stored = s
        .read_control(DEFAULT_SPACE, "attention/config", None)
        .await
        .unwrap()
        .unwrap();
    let mut config: AttentionConfig = serde_json::from_value(stored.value).unwrap();
    config.pins.evaluator = Some(pin("replacement"));
    s.set_attention_config(DEFAULT_SPACE, stored.version, config)
        .await
        .unwrap();
    assert!(
        s.commit_watch_page(
            DEFAULT_SPACE,
            &p.ticket_ref,
            judgments(&p, "old-evaluator", WatchMatch::Match)
        )
        .await
        .is_err()
    );
    assert_eq!(
        f.nexus
            .store
            .get_element(f.watch.parse().unwrap())
            .await
            .unwrap()
            .version(),
        f.version
    );
    f.nexus.close().await.unwrap();
}
