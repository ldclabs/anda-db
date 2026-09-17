//! R0 downstream acceptance cases, now enforced by native attention.
use anda_cognitive_nexus::{
    CognitiveNexus,
    nexus::DEFAULT_SPACE,
    profiles::COGNITIVE_MEMORY,
    store::{Element, rows::ControlRecordRow},
};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{Request, TopLevelStatus};
use object_store::{ObjectStore, memory::InMemory};
use serde_json::{Value, json};
use std::sync::Arc;

#[path = "watch_handoff/extensions.rs"]
mod extensions;

#[path = "support/attention_fault.rs"]
mod fault;

struct Fixture {
    store: Arc<dyn ObjectStore>,
    config: DBConfig,
    nexus: CognitiveNexus,
    target: String,
    watch: String,
    version: u64,
    generation: u64,
    matched_seq: u64,
}

async fn run(nexus: &CognitiveNexus, command: &str, parameters: Value) -> Value {
    let mut request = Request::single(command);
    request.parameters = parameters.as_object().cloned();
    let response = anda_kip::execute_request(nexus, &request).await;
    assert_eq!(response.status, TopLevelStatus::Succeeded, "{response:?}");
    response.first_result().cloned().expect("operation result")
}

impl Fixture {
    async fn new() -> Self {
        Self::with_store(Arc::new(InMemory::new())).await
    }

    async fn with_store(store: Arc<dyn ObjectStore>) -> Self {
        let config = DBConfig {
            name: "r0_watch_handoff".into(),
            ..Default::default()
        };
        let db = Arc::new(
            AndaDB::connect(store.clone(), config.clone())
                .await
                .unwrap(),
        );
        let nexus = CognitiveNexus::connect(db).await.unwrap();
        nexus
            .install_and_activate(&[("brain", COGNITIVE_MEMORY)], DEFAULT_SPACE)
            .await
            .unwrap();
        let target = run(
            &nexus,
            r#"CREATE CONCEPT ?target {TYPE "Person" NAME "before"}"#,
            Value::Null,
        )
        .await["handles"]["target"]
            .as_str()
            .unwrap()
            .to_owned();
        let watch = run(&nexus, r#"CREATE CONCEPT ?watch {TYPE "Watch" SET ATTRIBUTES {watch_class:"delta",summary:"track target",condition:{element: :target,ops:["update"]},status:"disarmed"}}"#, json!({"target":target})).await["handles"]["watch"].as_str().unwrap().to_owned();
        let version = nexus
            .store
            .get_element(watch.parse().unwrap())
            .await
            .unwrap()
            .version();
        let armed = nexus
            .system_session()
            .arm_watch(DEFAULT_SPACE, &watch, version)
            .await
            .unwrap();
        let version = nexus
            .store
            .get_element(watch.parse().unwrap())
            .await
            .unwrap()
            .version();
        let generation = armed["watch"]["arm_generation"].as_u64().unwrap();
        run(
            &nexus,
            r#"UPDATE :target SET FIELDS {name:"after"}"#,
            json!({"target":target}),
        )
        .await;
        let matched_seq = nexus.store.get_space(DEFAULT_SPACE).await.unwrap().seq;
        Self {
            store,
            config,
            nexus,
            target,
            watch,
            version,
            generation,
            matched_seq,
        }
    }

    async fn advance(&self) -> Value {
        self.nexus
            .system_session()
            .advance_watch(
                DEFAULT_SPACE,
                &self.watch,
                self.version,
                self.generation,
                200,
            )
            .await
            .unwrap()
    }

    async fn reopen(self) -> Self {
        self.open_again(true).await
    }

    async fn open_again(self, graceful: bool) -> Self {
        let Self {
            store,
            config,
            nexus,
            target,
            watch,
            version,
            generation,
            matched_seq,
        } = self;
        if graceful {
            nexus.close().await.unwrap();
        }
        drop(nexus);
        let db = Arc::new(AndaDB::open(store.clone(), config.clone()).await.unwrap());
        let nexus = CognitiveNexus::connect(db).await.unwrap();
        Self {
            store,
            config,
            nexus,
            target,
            watch,
            version,
            generation,
            matched_seq,
        }
    }

    async fn assert_handoff(&self, result: &Value) {
        assert_eq!(result["status"], "fired");
        let activities = run(
            &self.nexus,
            r#"FIND(?a.id) WHERE {?a ACTIVITY {activity_class:"watch_fire"}} LIMIT 10"#,
            Value::Null,
        )
        .await;
        assert_eq!(
            activities.as_array().unwrap().len(),
            1,
            "R1 requires exactly one watch_fire Activity in the firing transaction; observed {activities}"
        );
        let activity_ref = result["fire_activity_ref"]
            .as_str()
            .expect("R1 fire_activity_ref");
        assert_eq!(activities[0], activity_ref);
        let fire_key = format!(
            "watch_fire:{}:{}:{}",
            self.watch, self.generation, self.matched_seq
        );
        assert_eq!(result["fire_key"], fire_key);
        let wake_ref = result["wake_ref"].as_str().expect("R1 wake_ref");
        assert!(wake_ref.starts_with("wake/v1/"));
        let mut wakes = Vec::new();
        let collection = self.nexus.store.control_records();
        for id in collection.ids() {
            let row: ControlRecordRow = collection.get_as(id).await.unwrap();
            if row.space == DEFAULT_SPACE && row.key.starts_with("wake/v1/") {
                wakes.push(row);
            }
        }
        assert_eq!(
            wakes.len(),
            1,
            "R1 requires exactly one durable wake, not merely a returned id"
        );
        let wake = &wakes[0];
        assert_eq!(wake.key, wake_ref);
        assert_eq!(wake.value["fire_activity_ref"], activity_ref);
        assert_eq!(wake.value["fire"]["watch_ref"], self.watch);
        assert_eq!(wake.value["fire"]["arm_generation"], self.generation);
        let committed_seq = result["receipt"]["space_seq"]
            .as_u64()
            .expect("committed receipt");
        let activity = self
            .nexus
            .store
            .get_element(activity_ref.parse().unwrap())
            .await
            .unwrap();
        let Element::Activity(activity_row) = &activity else {
            panic!("fire_activity_ref must name an Activity");
        };
        assert_eq!(activity_row.client_key, fire_key);
        let watch = self
            .nexus
            .store
            .get_element(self.watch.parse().unwrap())
            .await
            .unwrap();
        assert_eq!(activity.seq(), committed_seq);
        assert_eq!(watch.seq(), committed_seq);
        assert_eq!(
            wake.seq, committed_seq,
            "wake must share the native commit, not be a follow-up write"
        );
    }
}

#[tokio::test]
async fn native_watch_coverage_survives_a_real_database_reopen() {
    let fixture = Fixture::new().await.reopen().await;
    let fired = fixture.advance().await;
    assert_eq!(fired["status"], "fired");
    assert_eq!(fired["watch"]["arm_generation"], fixture.generation);
    assert_eq!(fired["watch"]["matched"], true);
    assert!(fired["watch"]["consumed_seq"].as_u64().unwrap() >= fixture.matched_seq);
    fixture.nexus.close().await.unwrap();
}

#[tokio::test]
async fn fired_watch_atomically_creates_one_activity_and_one_wake() {
    let fixture = Fixture::new().await;
    let fired = fixture.advance().await;
    fixture.assert_handoff(&fired).await;
    fixture.nexus.close().await.unwrap();
}

#[tokio::test]
async fn governance_history_alone_cannot_read_a_wake_record() {
    use anda_cognitive_nexus::governance::{
        AuthContext,
        store::{GrantDraft, PrincipalDraft},
    };
    let fixture = Fixture::new().await;
    let fired = fixture.advance().await;
    let wake = fired["wake_ref"].as_str().unwrap();
    let principal = "kip:principal:attention-auditor";
    fixture
        .nexus
        .governance()
        .ensure_principal(PrincipalDraft {
            principal_id: principal.into(),
            principal_class: "service".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    fixture
        .nexus
        .governance()
        .create_grant(
            GrantDraft {
                space_id: DEFAULT_SPACE.into(),
                grantee_principal: principal.into(),
                actions: vec!["read".into(), "read_governance_history".into()],
                ..Default::default()
            },
            "kip:principal:system",
        )
        .await
        .unwrap();
    let auditor = fixture.nexus.session(AuthContext::principal(principal));
    assert!(
        auditor
            .read_control(DEFAULT_SPACE, wake, None)
            .await
            .is_err()
    );
    assert!(auditor.read_wake(DEFAULT_SPACE, wake).await.is_err());
    let checkpoint = format!("attention/watch/{}/{}", fixture.watch, fixture.generation);
    assert!(
        auditor
            .read_control(DEFAULT_SPACE, &checkpoint, None)
            .await
            .is_err()
    );
    assert!(
        fixture
            .nexus
            .system_session()
            .read_control(DEFAULT_SPACE, wake, None)
            .await
            .unwrap()
            .is_some()
    );
    fixture.nexus.close().await.unwrap();
}

#[tokio::test]
async fn committed_handoff_survives_database_reopen() {
    let fixture = Fixture::new().await;
    let fired = fixture.advance().await;
    let reopened = fixture.reopen().await;
    reopened.assert_handoff(&fired).await;
    reopened.nexus.close().await.unwrap();
}

#[tokio::test]
async fn concurrent_and_ack_lost_advancement_replays_the_same_handoff() {
    let fixture = Fixture::new().await;
    let session = fixture.nexus.system_session();
    let (a, b) = tokio::join!(
        session.advance_watch(
            DEFAULT_SPACE,
            &fixture.watch,
            fixture.version,
            fixture.generation,
            200
        ),
        session.advance_watch(
            DEFAULT_SPACE,
            &fixture.watch,
            fixture.version,
            fixture.generation,
            200
        )
    );
    let a = a.expect("first request must commit or replay");
    let b = b.expect("identical concurrent request must replay");
    assert_eq!(a["fire_key"], b["fire_key"]);
    assert_eq!(a["wake_ref"], b["wake_ref"]);
    let reopened = fixture.reopen().await;
    let replay = reopened.advance().await;
    assert_eq!(a["receipt"], replay["receipt"]);
    reopened.assert_handoff(&replay).await;
    reopened.nexus.close().await.unwrap();
}

fn expiry_in(seconds: i64) -> String {
    (chrono::Utc::now() + chrono::Duration::seconds(seconds))
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

#[tokio::test]
async fn lease_completion_and_continuations_share_a_replayable_commit() {
    use anda_cognitive_nexus::attention::{WakeContinuation, WakeState};
    let fixture = Fixture::new().await;
    let fired = fixture.advance().await;
    let wake_ref = fired["wake_ref"].as_str().unwrap();
    let session = fixture.nexus.system_session();
    let expires = expiry_in(120);
    let claimed = session
        .claim_wake(DEFAULT_SPACE, wake_ref, 1, 0, &expires)
        .await
        .unwrap();
    let replay = session
        .claim_wake(DEFAULT_SPACE, wake_ref, 1, 0, &expires)
        .await
        .unwrap();
    assert_eq!(claimed["receipt"], replay["receipt"]);
    assert_eq!(claimed["wake"]["version"], 2);
    assert!(
        session
            .renew_wake(DEFAULT_SPACE, wake_ref, 2, 0, &expiry_in(130))
            .await
            .is_err()
    );
    let command = r#"CREATE CONCEPT ?output {TYPE "Event" NAME "reviewed" SET ATTRIBUTES {summary:"reviewed"}}"#;
    let children = vec![WakeContinuation {
        key: "next-review".into(),
        not_before_ms: 0,
    }];
    let done = session
        .finish_wake(
            DEFAULT_SPACE,
            wake_ref,
            2,
            1,
            command,
            Default::default(),
            children.clone(),
        )
        .await
        .unwrap();
    let repeated = session
        .finish_wake(
            DEFAULT_SPACE,
            wake_ref,
            2,
            1,
            command,
            Default::default(),
            children,
        )
        .await
        .unwrap();
    assert_eq!(done, repeated);
    assert!(
        session
            .finish_wake(
                DEFAULT_SPACE,
                wake_ref,
                2,
                1,
                "",
                Default::default(),
                vec![]
            )
            .await
            .is_err()
    );
    let parent = session.read_wake(DEFAULT_SPACE, wake_ref).await.unwrap();
    assert!(matches!(parent.state, WakeState::Completed { .. }));
    let seq = done["receipt"]["space_seq"].as_u64().unwrap();
    for output in done["outputs"].as_array().unwrap() {
        let reference = output.as_str().unwrap();
        if reference.starts_with("wake/") {
            let child = session.read_wake(DEFAULT_SPACE, reference).await.unwrap();
            assert_eq!(child.format, "anda-brain:attention-continuation-v1");
            assert_eq!(child.parent_ref.as_deref(), Some(wake_ref));
            assert_eq!(
                fixture
                    .nexus
                    .store
                    .control_at(DEFAULT_SPACE, reference, u64::MAX)
                    .await
                    .unwrap()
                    .unwrap()
                    .seq,
                seq
            );
            let cancelled = session
                .cancel_wake(DEFAULT_SPACE, reference, 1, 0, "no longer needed")
                .await
                .unwrap();
            assert_eq!(cancelled["wake"]["fence"], 1);
            assert!(
                session
                    .claim_wake(DEFAULT_SPACE, reference, 2, 1, &expiry_in(60))
                    .await
                    .is_err()
            );
        } else {
            assert_eq!(
                fixture
                    .nexus
                    .store
                    .get_element(reference.parse().unwrap())
                    .await
                    .unwrap()
                    .seq(),
                seq
            );
        }
    }
    assert_eq!(
        fixture
            .nexus
            .store
            .control_at(DEFAULT_SPACE, wake_ref, u64::MAX)
            .await
            .unwrap()
            .unwrap()
            .seq,
        seq
    );
    fixture.nexus.close().await.unwrap();
}

#[tokio::test]
async fn invalid_completion_cannot_leave_outputs_or_consume_the_lease() {
    use anda_cognitive_nexus::attention::WakeContinuation;
    let fixture = Fixture::new().await;
    let fired = fixture.advance().await;
    let wake = fired["wake_ref"].as_str().unwrap();
    let session = fixture.nexus.system_session();
    session
        .claim_wake(DEFAULT_SPACE, wake, 1, 0, &expiry_in(120))
        .await
        .unwrap();
    let command = r#"CREATE CONCEPT ?output {TYPE "Person" NAME "must-not-survive"}"#;
    let duplicated = WakeContinuation {
        key: "duplicate".into(),
        not_before_ms: 0,
    };
    assert!(
        session
            .finish_wake(
                DEFAULT_SPACE,
                wake,
                2,
                1,
                command,
                Default::default(),
                vec![duplicated.clone(), duplicated]
            )
            .await
            .is_err()
    );
    let rows = run(
        &fixture.nexus,
        r#"FIND(?c.id) WHERE {?c CONCEPT {name:"must-not-survive"}}"#,
        Value::Null,
    )
    .await;
    assert_eq!(rows, json!([]));
    assert_eq!(
        session
            .read_wake(DEFAULT_SPACE, wake)
            .await
            .unwrap()
            .version,
        2
    );
    fixture.nexus.close().await.unwrap();
}

#[tokio::test]
async fn old_generation_and_changed_control_basis_cannot_complete_work() {
    let fixture = Fixture::new().await;
    let fired = fixture.advance().await;
    let wake = fired["wake_ref"].as_str().unwrap();
    let session = fixture.nexus.system_session();
    session
        .claim_wake(DEFAULT_SPACE, wake, 1, 0, &expiry_in(120))
        .await
        .unwrap();
    session
        .set_trust(DEFAULT_SPACE, 1, Default::default(), 0.8)
        .await
        .unwrap();
    assert!(
        session
            .finish_wake(DEFAULT_SPACE, wake, 2, 1, "", Default::default(), vec![])
            .await
            .is_err()
    );
    let version = fixture
        .nexus
        .store
        .get_element(fixture.watch.parse().unwrap())
        .await
        .unwrap()
        .version();
    session
        .arm_watch(DEFAULT_SPACE, &fixture.watch, version)
        .await
        .unwrap();
    assert!(
        session
            .renew_wake(DEFAULT_SPACE, wake, 2, 1, &expiry_in(180))
            .await
            .is_err()
    );
    // Withdrawal remains possible even though execution eligibility was lost.
    session
        .cancel_wake(DEFAULT_SPACE, wake, 2, 1, "basis withdrawn")
        .await
        .unwrap();
    fixture.nexus.close().await.unwrap();
}

#[tokio::test]
async fn wake_lease_expiry_and_takeover_use_real_time_and_increasing_fences() {
    let fixture = Fixture::new().await;
    let fired = fixture.advance().await;
    let wake = fired["wake_ref"].as_str().unwrap();
    let session = fixture.nexus.system_session();
    assert!(
        session
            .claim_wake(DEFAULT_SPACE, wake, 1, 0, &expiry_in(600))
            .await
            .is_err()
    );
    session
        .claim_wake(DEFAULT_SPACE, wake, 1, 0, &expiry_in(1))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    assert!(
        session
            .finish_wake(DEFAULT_SPACE, wake, 2, 1, "", Default::default(), vec![])
            .await
            .is_err()
    );
    let reclaimed = session
        .claim_wake(DEFAULT_SPACE, wake, 2, 1, &expiry_in(120))
        .await
        .unwrap();
    assert_eq!(reclaimed["wake"]["fence"], 2);
    assert!(
        session
            .finish_wake(DEFAULT_SPACE, wake, 3, 1, "", Default::default(), vec![])
            .await
            .is_err()
    );
    session
        .finish_wake(DEFAULT_SPACE, wake, 3, 2, "", Default::default(), vec![])
        .await
        .unwrap();
    fixture.nexus.close().await.unwrap();
}

#[tokio::test]
async fn interrupted_native_writes_recover_all_or_none_then_replay_once() {
    for point in [
        "kip_commit_log/",
        "kip_control_records/",
        "concepts/",
        "activities/",
    ] {
        let storage = Arc::new(fault::FaultStore::default());
        let fixture = Fixture::with_store(storage.clone()).await;
        storage.arm(point);
        {
            let advance = fixture.advance();
            tokio::pin!(advance);
            // Deliberate crash injection: cancel only after a real durable PUT.
            tokio::select! {
                _=storage.entered.notified()=>{},
                result=&mut advance=>panic!("fault {point} was not reached: {result}"),
                _=tokio::time::sleep(std::time::Duration::from_secs(20))=>panic!("fault watchdog {point}"),
            }
        }
        let reopened = fixture.open_again(false).await;
        let row = reopened
            .nexus
            .store
            .get_element(reopened.watch.parse().unwrap())
            .await
            .unwrap();
        let view = anda_cognitive_nexus::view::render(&row);
        let activities = run(
            &reopened.nexus,
            r#"FIND(?a.id) WHERE {?a ACTIVITY {activity_class:"watch_fire"}}"#,
            Value::Null,
        )
        .await;
        match view["attributes"]["status"].as_str().unwrap() {
            "armed" => assert_eq!(activities, json!([]), "{point}"),
            "fired" => assert_eq!(activities.as_array().unwrap().len(), 1, "{point}"),
            status => panic!("unexpected recovered state {status}"),
        }
        let result = reopened.advance().await;
        reopened.assert_handoff(&result).await;
        reopened.nexus.close().await.unwrap();
    }
}

// Controlled historical fixtures, not a clock override exposed by the runtime.
async fn journal_time(nexus: &CognitiveNexus, seq: Option<u64>, at: &str) {
    use anda_cognitive_nexus::store::rows::TransactionRow;
    let table = nexus.store.transactions();
    for id in table.ids() {
        let row: TransactionRow = table.get_as(id).await.unwrap();
        if seq.is_none_or(|seq| row.seq == seq) {
            table
                .update(
                    id,
                    std::collections::BTreeMap::from([(
                        "committed_at".into(),
                        anda_db::schema::Fv::Text(at.into()),
                    )]),
                )
                .await
                .unwrap();
        }
    }
}

async fn silence_fixture(fixture: &Fixture) -> String {
    let result=run(&fixture.nexus,r#"CREATE CONCEPT ?watch {TYPE "Watch" SET ATTRIBUTES {watch_class:"silence",summary:"wait for update",condition:{element: :target,ops:["update"]},due_at:"2000-01-01T00:00:01.000Z",status:"disarmed"}}"#,json!({"target":fixture.target})).await;
    let reference = result["handles"]["watch"].as_str().unwrap().to_string();
    fixture
        .nexus
        .system_session()
        .arm_watch(DEFAULT_SPACE, &reference, 1)
        .await
        .unwrap();
    journal_time(&fixture.nexus, None, "2000-01-01T00:00:00.000Z").await;
    reference
}

#[tokio::test]
async fn silence_distinguishes_before_equal_and_after_deadline() {
    for (at, expected) in [
        ("2000-01-01T00:00:00.999Z", "expired"),
        ("2000-01-01T00:00:01.000Z", "expired"),
        ("2000-01-01T00:00:01.001Z", "fired"),
    ] {
        let fixture = Fixture::new().await;
        let reference = silence_fixture(&fixture).await;
        run(
            &fixture.nexus,
            r#"UPDATE :target SET FIELDS {name:"reply"}"#,
            json!({"target":fixture.target}),
        )
        .await;
        let seq = fixture
            .nexus
            .store
            .get_space(DEFAULT_SPACE)
            .await
            .unwrap()
            .seq;
        journal_time(&fixture.nexus, Some(seq), at).await;
        let result = fixture
            .nexus
            .system_session()
            .advance_watch(DEFAULT_SPACE, &reference, 2, 1, 200)
            .await
            .unwrap();
        assert_eq!(result["status"], expected, "{at}: {result}");
        assert_eq!(result["watch"]["matched"], expected == "expired");
        let activities = run(
            &fixture.nexus,
            r#"FIND(?a.id) WHERE {?a ACTIVITY {activity_class:"watch_fire"}}"#,
            Value::Null,
        )
        .await;
        assert_eq!(
            activities.as_array().unwrap().len(),
            usize::from(expected == "fired")
        );
        if expected == "fired" {
            let wake = fixture
                .nexus
                .system_session()
                .read_wake(DEFAULT_SPACE, result["wake_ref"].as_str().unwrap())
                .await
                .unwrap();
            assert_eq!(
                wake.fire.key().unwrap(),
                format!("watch_fire:{reference}:1:silence:2000-01-01T00:00:01.000Z")
            );
            assert_eq!(result["watch"]["consumed_seq"], seq - 1);
        }
        fixture.nexus.close().await.unwrap();
    }
}

#[tokio::test]
async fn silence_rearm_cannot_clear_its_deadline() {
    let fixture = Fixture::new().await;
    let reference = silence_fixture(&fixture).await;
    let version = fixture
        .nexus
        .store
        .get_element(reference.parse().unwrap())
        .await
        .unwrap()
        .version();
    let result = fixture
        .nexus
        .system_session()
        .rearm_watch(
            DEFAULT_SPACE,
            &reference,
            version,
            json!({"element":fixture.target,"ops":["update"]}),
            None,
        )
        .await;
    assert!(result.is_err());
    let watch = fixture
        .nexus
        .store
        .get_element(reference.parse().unwrap())
        .await
        .unwrap();
    assert_eq!(watch.version(), version);
    assert_eq!(
        anda_cognitive_nexus::view::render(&watch)["attributes"]["due_at"],
        "2000-01-01T00:00:01.000Z"
    );
    fixture.nexus.close().await.unwrap();
}

#[tokio::test]
async fn silence_finishes_fixed_coverage_despite_newer_traffic() {
    let fixture = Fixture::new().await;
    let reference = silence_fixture(&fixture).await;
    for _ in 0..6 {
        run(
            &fixture.nexus,
            r#"CREATE CONCEPT ?noise {TYPE "Person" NAME "before-deadline"}"#,
            Value::Null,
        )
        .await;
        let seq = fixture
            .nexus
            .store
            .get_space(DEFAULT_SPACE)
            .await
            .unwrap()
            .seq;
        journal_time(&fixture.nexus, Some(seq), "2000-01-01T00:00:00.500Z").await;
    }
    let due_seq = fixture
        .nexus
        .store
        .get_space(DEFAULT_SPACE)
        .await
        .unwrap()
        .seq;
    // This would be a match, but it is outside the silence interval.
    run(
        &fixture.nexus,
        r#"UPDATE :target SET FIELDS {name:"late reply"}"#,
        json!({"target":fixture.target}),
    )
    .await;
    let mut final_result = None;
    for _ in 0..10 {
        let version = fixture
            .nexus
            .store
            .get_element(reference.parse().unwrap())
            .await
            .unwrap()
            .version();
        let result = fixture
            .nexus
            .system_session()
            .advance_watch(DEFAULT_SPACE, &reference, version, 1, 1)
            .await
            .unwrap();
        if result["status"] == "fired" {
            final_result = Some(result);
            break;
        }
        assert_eq!(result["status"], "armed");
        assert_eq!(result["coverage"]["complete"], false);
        run(
            &fixture.nexus,
            r#"CREATE CONCEPT ?noise {TYPE "Person" NAME "new traffic"}"#,
            Value::Null,
        )
        .await;
    }
    let result = final_result.expect("fixed due_seq must not follow new traffic forever");
    assert_eq!(result["watch"]["consumed_seq"], due_seq);
    assert_eq!(result["watch"]["matched"], false);
    fixture.nexus.close().await.unwrap();
}

#[tokio::test]
async fn history_gap_and_prose_conditions_cannot_invent_coverage() {
    let fixture = Fixture::new().await;
    let session = fixture.nexus.system_session();
    let error = session
        .rearm_watch(
            DEFAULT_SPACE,
            &fixture.watch,
            fixture.version,
            json!({"element":fixture.target,"text":"meaningful reply"}),
            None,
        )
        .await
        .unwrap();
    let version = fixture
        .nexus
        .store
        .get_element(fixture.watch.parse().unwrap())
        .await
        .unwrap()
        .version();
    let generation = error["watch"]["arm_generation"].as_u64().unwrap();
    let refused = session
        .advance_watch(DEFAULT_SPACE, &fixture.watch, version, generation, 200)
        .await
        .unwrap_err();
    assert_eq!(refused.code, anda_kip::KipErrorCode::UnsupportedCapability);
    assert_eq!(
        fixture
            .nexus
            .store
            .get_element(fixture.watch.parse().unwrap())
            .await
            .unwrap()
            .version(),
        version
    );
    session
        .rearm_watch(
            DEFAULT_SPACE,
            &fixture.watch,
            version,
            json!({"element":fixture.target,"ops":["update"]}),
            None,
        )
        .await
        .unwrap();
    let version = fixture
        .nexus
        .store
        .get_element(fixture.watch.parse().unwrap())
        .await
        .unwrap()
        .version();
    let control = fixture
        .nexus
        .store
        .control_at(DEFAULT_SPACE, "internal/governance", u64::MAX)
        .await
        .unwrap()
        .unwrap();
    let mut value = control.value;
    value["coverage_floor"] = json!(
        fixture
            .nexus
            .store
            .get_space(DEFAULT_SPACE)
            .await
            .unwrap()
            .seq
    );
    fixture
        .nexus
        .store
        .control_records()
        .update(
            control._id,
            std::collections::BTreeMap::from([("value".into(), anda_db::schema::Fv::Json(value))]),
        )
        .await
        .unwrap();
    let refused = session
        .advance_watch(DEFAULT_SPACE, &fixture.watch, version, generation + 1, 200)
        .await
        .unwrap_err();
    assert_eq!(refused.details.unwrap()["attention_reason"], "history_gap");
    assert_eq!(
        fixture
            .nexus
            .store
            .get_element(fixture.watch.parse().unwrap())
            .await
            .unwrap()
            .version(),
        version
    );
    fixture.nexus.close().await.unwrap();
}

#[tokio::test]
async fn wake_dispatch_rechecks_native_attempt_and_never_replays_a_send_permission() {
    use anda_cognitive_nexus::attention::{AttentionConfig, DispatchLookupObserver, RuntimePin};
    for (idempotent, lookup, second_action) in [
        (false, true, "lookup"),
        (false, false, "outcome_unknown"),
        (true, false, "dispatch"),
    ] {
        let mut fixture = Fixture::new().await;
        let session = fixture.nexus.system_session();
        let saved = session
            .read_control(DEFAULT_SPACE, "attention/config", None)
            .await
            .unwrap()
            .unwrap();
        let mut config: AttentionConfig = serde_json::from_value(saved.value).unwrap();
        let binding = RuntimePin {
            id: "test-executor".into(),
            digest: format!("sha256:{}", "a".repeat(64)),
        };
        config.pins.binding = Some(binding.clone());
        session
            .set_attention_config(DEFAULT_SPACE, saved.version, config)
            .await
            .unwrap();
        if lookup {
            session
                .set_dispatch_lookup_observer(
                    DEFAULT_SPACE,
                    0,
                    DispatchLookupObserver {
                        binding,
                        principal_id: "kip:principal:test-lookup".into(),
                        configuration_digest: format!("sha256:{}", "b".repeat(64)),
                    },
                )
                .await
                .unwrap();
        }
        let armed = session
            .arm_watch(DEFAULT_SPACE, &fixture.watch, fixture.version)
            .await
            .unwrap();
        fixture.generation = armed["watch"]["arm_generation"].as_u64().unwrap();
        fixture.version = fixture
            .nexus
            .store
            .get_element(fixture.watch.parse().unwrap())
            .await
            .unwrap()
            .version();
        run(
            &fixture.nexus,
            r#"UPDATE :target SET FIELDS {name:"new reply"}"#,
            json!({"target":fixture.target}),
        )
        .await;
        let fired = fixture.advance().await;
        let wake = fired["wake_ref"].as_str().unwrap();
        session
            .claim_wake(DEFAULT_SPACE, wake, 1, 0, &expiry_in(120))
            .await
            .unwrap();
        let created=run(&fixture.nexus,r#"MUTATE {CREATE CONCEPT ?preference {TYPE "Preference" NAME "delivery"} ENSURE PROPOSITION ?basis (:target,"prefers",?preference)}"#,json!({"target":fixture.target})).await;
        let projection = run(
            &fixture.nexus,
            "FIND(?b) WHERE {?p PROPOSITION(id: :id) ?b BELIEF(?p)}",
            json!({"id":created["handles"]["basis"]}),
        )
        .await;
        let basis = &projection[0]["basis"];
        let selection = session
            .put_artifact(
                DEFAULT_SPACE,
                json!({"policy":"attention-native-test"}),
                vec![],
            )
            .await
            .unwrap();
        let decision=run(&fixture.nexus,r#"MUTATE {
            CREATE ACTIVITY ?decision {SET FIELDS {activity_class:"action_gate",status:"completed"}
                SET FACET "DecisionRecord" {decision:"act",retrieved_refs:[:watch],used_refs:[:watch],applied_revisions:[],basis: :basis}
                SET FACET "DependencyBasis" {basis_seq: :seq,policy_basis: :basis,groups:[{role:"context",pins:[{id: :proposition,version:1},{id: :watch,version: :watch_version}]}]}
                SET STRUCTURAL {("inputs",:watch) ("inputs",:proposition)}}
            CREATE ACTIVITY ?attempt {SET FIELDS {activity_class:"action_attempt",status:"completed"}
                SET FACET "AttemptRecord" {attempt_id:"attention-native-test",decision_ref:?decision,applied_revisions:[],trial_ref:null,context:{task_family:"attention.test"},environment_digest: :environment,tool_versions:{fixture:"v1"},selection_policy: :selection,preconditions_satisfied:"yes",started_at: :started}
                SET STRUCTURAL {("inputs",?decision)}}
        }"#,json!({"watch":fixture.watch,"watch_version":fixture.nexus.store.get_element(fixture.watch.parse().unwrap()).await.unwrap().version(),"basis":basis,"seq":basis["snapshot_seq"],"proposition":created["handles"]["basis"],"environment":format!("sha256:{}","b".repeat(64)),"selection":selection,"started":anda_cognitive_nexus::time::now()})).await;
        let attempt = decision["handles"]["attempt"].as_str().unwrap();
        let first = session
            .begin_wake_dispatch(DEFAULT_SPACE, wake, 2, 1, attempt, idempotent, lookup)
            .await
            .unwrap();
        assert_eq!(first["action"], "dispatch");
        let second = session
            .begin_wake_dispatch(DEFAULT_SPACE, wake, 2, 1, attempt, idempotent, lookup)
            .await
            .unwrap();
        assert_eq!(second["action"], second_action);
        assert_eq!(first["idempotency_key"], second["idempotency_key"]);
        assert_eq!(first["dispatch_ref"], second["dispatch_ref"]);
        assert!(
            session
                .begin_wake_dispatch(DEFAULT_SPACE, wake, 2, 0, attempt, idempotent, lookup)
                .await
                .is_err()
        );
        assert!(
            session
                .finish_wake(DEFAULT_SPACE, wake, 2, 1, "", Default::default(), vec![])
                .await
                .is_err()
        );
        session
            .cancel_wake(DEFAULT_SPACE, wake, 2, 1, "stop new sends")
            .await
            .unwrap();
        assert!(
            session
                .begin_wake_dispatch(DEFAULT_SPACE, wake, 3, 2, attempt, idempotent, lookup)
                .await
                .is_err()
        );
        assert!(
            fixture
                .nexus
                .store
                .control_at(
                    DEFAULT_SPACE,
                    first["dispatch_ref"].as_str().unwrap(),
                    u64::MAX
                )
                .await
                .unwrap()
                .is_some(),
            "cancellation must retain the reconciliation obligation"
        );
        fixture.nexus.close().await.unwrap();
    }
}

#[tokio::test]
async fn ordinary_writes_cannot_preempt_fire_identity_or_change_armed_deadlines() {
    let fixture = Fixture::new().await;
    let key = format!(
        "watch_fire:{}:{}:{}",
        fixture.watch, fixture.generation, fixture.matched_seq
    );
    for (command, params) in [
        (
            r#"CREATE ACTIVITY ?fake {CLIENT KEY :key SET FIELDS {activity_class:"watch_fire",status:"completed"}}"#,
            json!({"key":key}),
        ),
        (
            r#"UPDATE :watch SET ATTRIBUTES {due_at:"2099-01-01T00:00:00Z"} EXPECT VERSION :version"#,
            json!({"watch":fixture.watch,"version":fixture.version}),
        ),
    ] {
        let mut request = Request::single(command);
        request.parameters = params.as_object().cloned();
        let result = anda_kip::execute_request(&fixture.nexus, &request).await;
        assert_eq!(result.status, TopLevelStatus::Failed);
        assert_eq!(
            result.results[0].error.as_ref().unwrap().code,
            "NotAuthorized"
        );
    }
    for condition in [
        json!(null),
        json!(0),
        json!({}),
        json!({"text":null}),
        json!({"unexpected":"selector"}),
    ] {
        assert!(
            fixture
                .nexus
                .system_session()
                .rearm_watch(
                    DEFAULT_SPACE,
                    &fixture.watch,
                    fixture.version,
                    condition,
                    None
                )
                .await
                .is_err()
        );
    }
    let fired = fixture.advance().await;
    fixture.assert_handoff(&fired).await;
    fixture.nexus.close().await.unwrap();
}

#[tokio::test]
async fn configured_semantic_evaluator_failure_preserves_the_original_checkpoint() {
    use anda_cognitive_nexus::attention::{AttentionConfig, RuntimePin};
    let fixture = Fixture::new().await;
    let session = fixture.nexus.system_session();
    let saved = session
        .read_control(DEFAULT_SPACE, "attention/config", None)
        .await
        .unwrap()
        .unwrap();
    let mut cfg: AttentionConfig = serde_json::from_value(saved.value).unwrap();
    cfg.pins.evaluator = Some(RuntimePin {
        id: "explicit-native-test".into(),
        digest: format!("sha256:{}", "d".repeat(64)),
    });
    session
        .set_attention_config(DEFAULT_SPACE, saved.version, cfg)
        .await
        .unwrap();
    let armed = session
        .rearm_watch(
            DEFAULT_SPACE,
            &fixture.watch,
            fixture.version,
            json!({"element":fixture.target,"text":"reply received"}),
            None,
        )
        .await
        .unwrap();
    let version = fixture
        .nexus
        .store
        .get_element(fixture.watch.parse().unwrap())
        .await
        .unwrap()
        .version();
    let generation = armed["watch"]["arm_generation"].as_u64().unwrap();
    run(
        &fixture.nexus,
        r#"UPDATE :target SET FIELDS {name:"semantic reply"}"#,
        json!({"target":fixture.target}),
    )
    .await;
    assert!(
        session
            .advance_watch_with(
                DEFAULT_SPACE,
                &fixture.watch,
                version,
                generation,
                200,
                |_, _| Err(anda_kip::KipError::unsupported_capability(
                    "unknown semantic result"
                ))
            )
            .await
            .is_err()
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .get_element(fixture.watch.parse().unwrap())
            .await
            .unwrap()
            .version(),
        version
    );
    let result = session
        .advance_watch_with(
            DEFAULT_SPACE,
            &fixture.watch,
            version,
            generation,
            200,
            |_, _| Ok(true),
        )
        .await
        .unwrap();
    assert_eq!(result["status"], "fired");
    fixture.nexus.close().await.unwrap();
}
