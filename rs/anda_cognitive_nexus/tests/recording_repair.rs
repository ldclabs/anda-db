//! Recording repair (Spec §57.8): what `KIP2-REL-004` and the replacement
//! variant of `KIP2-MIF-017` ask of the engine. A misrecording is repaired —
//! never written as the actor's retraction, supersession or correction — and
//! only by the recorder, against the exact source it got wrong.

use anda_cognitive_nexus::{
    CognitiveNexus,
    governance::{
        AuthContext, SYSTEM_PRINCIPAL,
        rows::{AuthorityScope, principal_class},
        store::{GrantDraft, PrincipalDraft},
    },
    nexus::DEFAULT_SPACE,
    schema::{PackageState, SchemaLock, SchemaPackage},
};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{
    Executor, Json, KipError, Request, Response, TopLevelStatus,
    cognitive::{RecordingRepair, RepairReason},
};
use object_store::memory::InMemory;
use serde_json::json;
use std::sync::Arc;

const PROFILE_ID: &str = "kip://profiles/cognitive-memory";
const SOURCE_DIGEST: &str =
    "sha256:5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e5ca1ab1e";

async fn fresh(name: &str) -> CognitiveNexus {
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: name.to_string(),
            description: "recording repair tests".to_string(),
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
    ok(
        &nexus,
        r#"DEFINE PREDICATE "diet" {description: "What the subject eats."}"#,
        json!({}),
    )
    .await;
    nexus
}

async fn run(executor: &impl Executor, command: &str, params: Json) -> Response {
    let request = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "operations": [{"command": command, "parameters": params}]
    }))
    .unwrap();
    let parsed = request.operations[0].parse().unwrap();
    executor
        .execute(parsed, &request, &request.operations[0])
        .await
}

async fn ok(executor: &impl Executor, command: &str, params: Json) -> Json {
    let response = run(executor, command, params).await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "{command}\n{:#?}",
        response.error
    );
    response.first_result().cloned().unwrap_or(Json::Null)
}

fn name(error: KipError) -> String {
    error.name().to_string()
}

async fn agent(
    nexus: &CognitiveNexus,
    principal: &str,
    actions: &[&str],
) -> anda_cognitive_nexus::nexus::Session {
    let gov = nexus.governance();
    gov.ensure_principal(PrincipalDraft {
        principal_id: principal.into(),
        principal_class: principal_class::AGENT.into(),
        display_name: principal.into(),
        auth_provider: "test".into(),
        auth_subject: principal.into(),
    })
    .await
    .unwrap();
    gov.create_grant(
        GrantDraft {
            space_id: DEFAULT_SPACE.into(),
            grantee_principal: principal.into(),
            actions: actions.iter().map(|a| a.to_string()).collect(),
            ..Default::default()
        },
        SYSTEM_PRINCIPAL,
    )
    .await
    .unwrap();
    nexus.session(AuthContext::principal(principal))
}

/// Alice said she is not vegetarian; the recorder extracted that she is.
/// Returns (alice, source Evidence, the wrong Assertion).
async fn misrecord(executor: &impl Executor, alice_key: &str) -> (String, String, String) {
    let created = ok(
        executor,
        r#"MUTATE {
            UPSERT CONCEPT ?alice { MATCH {type: "Person", key: :alice_key} SET FIELDS {name: "Alice"} }
            CREATE EVIDENCE ?msg {
                SET FIELDS {
                    evidence_class: "message", payload: :payload,
                    content_digest: :digest, observed_at: "2026-01-01T00:00:00.000Z"
                }
            }
            ASSERT ?wrong (?alice, "diet", "vegetarian") {
                by: ?alice, mode: "stated", at: "2026-01-01T00:00:00.000Z", evidence: ?msg
            }
        }"#,
        json!({
            "alice_key": alice_key,
            "digest": SOURCE_DIGEST,
            "payload": {"role": "user", "content": "I am not vegetarian; I just skipped meat today."},
        }),
    )
    .await;
    let handle = |h: &str| created["handles"][h].as_str().unwrap().to_string();
    (handle("alice"), handle("msg"), handle("wrong"))
}

async fn diet(executor: &impl Executor, alice: &str, at: Option<&str>) -> Json {
    let command = match at {
        Some(at) => {
            format!(r#"FIND(?slot) WHERE {{ ?slot BELIEF SLOT (:alice, "diet") }} FOR TIME "{at}""#)
        }
        None => r#"FIND(?slot) WHERE { ?slot BELIEF SLOT (:alice, "diet") }"#.to_string(),
    };
    let result = ok(executor, &command, json!({"alice": alice})).await;
    result.as_array().unwrap()[0].clone()
}

async fn version(executor: &impl Executor, id: &str) -> u64 {
    ok(
        executor,
        r#"FIND(?a._system.version) WHERE { ?a ASSERTION {id: :id} }"#,
        json!({"id": id}),
    )
    .await[0]
        .as_u64()
        .unwrap()
}

fn repair(source: &str, wrong: &str, version: u64, replacements: &[&str]) -> RecordingRepair {
    RecordingRepair {
        source_ref: source.into(),
        source_digest: SOURCE_DIGEST.into(),
        source_locator: "/content".into(),
        invalidated_refs: vec![wrong.into()],
        replacement_refs: replacements.iter().map(|r| r.to_string()).collect(),
        reason: RepairReason::ExtractionError,
        expected_versions: [(wrong.to_string(), version)].into(),
    }
}

async fn space_seq(executor: &impl Executor) -> u64 {
    ok(executor, "DESCRIBE SPACE", json!({})).await["seq"]
        .as_u64()
        .unwrap()
}

#[tokio::test]
async fn a_misrecording_is_repaired_without_an_actor_withdrawal() {
    let nexus = fresh("repair").await;
    let (alice, source, wrong) = misrecord(&nexus, "alice").await;
    let before = diet(&nexus, &alice, None).await;
    assert_eq!(before["status"], "accepted", "{before:#}");
    let before_seq = space_seq(&nexus).await;
    let original_version = version(&nexus, &wrong).await;

    // What she actually said, as the recorder's own new extraction.
    let replacement = ok(
        &nexus,
        r#"MUTATE {
            ASSERT ?right (:alice, "diet", "vegetarian") {
                by: :alice, mode: "stated", stance: "reject", confidence: 0.9,
                at: "2026-01-01T00:00:00.000Z", evidence: :msg
            }
        }"#,
        json!({"alice": {"id": alice}, "msg": source}),
    )
    .await["handles"]["right"]
        .as_str()
        .unwrap()
        .to_string();

    let session = nexus.system_session();
    let result = session
        .repair_recording(
            DEFAULT_SPACE,
            repair(&source, &wrong, original_version, &[&replacement]),
        )
        .await
        .unwrap();
    let repair_ref = result["repair_ref"].as_str().unwrap().to_string();
    assert!(result["receipt"]["space_seq"].as_u64().unwrap() > before_seq);
    let tx = result["receipt"]["tx_id"].as_str().unwrap();
    let described = ok(
        &nexus,
        &format!(r#"DESCRIBE TRANSACTION "{tx}""#),
        json!({}),
    )
    .await;
    assert!(
        described["control_changes"]
            .as_array()
            .is_some_and(|changes| changes.iter().any(|c| c["kind"] == "recording")),
        "{described:#}"
    );

    // Current recall no longer states the misrecording; it now holds what
    // Alice said: she rejects it.
    let after = diet(&nexus, &alice, None).await;
    assert!(
        after["accepted_values"].as_array().unwrap().is_empty(),
        "{after:#}"
    );
    let candidate = &after["candidate_projections"][0];
    assert_eq!(candidate["candidate_status"], "rejected", "{after:#}");
    assert_eq!(
        candidate["explanation"]["excluded"],
        json!([{"assertion_id": wrong, "reason": "recording_invalidated"}])
    );

    // The original payload and the actor's lifecycle are untouched: nobody
    // is recorded as having withdrawn or corrected anything.
    let raw = ok(
        &nexus,
        r#"FIND(?a) WHERE { ?a ASSERTION {id: :id} }"#,
        json!({"id": wrong}),
    )
    .await[0]
        .clone();
    assert_eq!(raw["stance"], "support");
    assert_eq!(raw["lifecycle"]["status"], "active");
    assert_eq!(raw["_system"]["version"], original_version + 1);
    assert_eq!(
        raw["_system"]["recording_validity"],
        json!({"status": "invalidated", "repair_ref": repair_ref})
    );
    let standing = ok(
        &nexus,
        r#"FIND(?a._system.recording_validity) WHERE { ?a ASSERTION {id: :id} }"#,
        json!({"id": replacement}),
    )
    .await;
    assert_eq!(standing[0], json!({"status": "valid", "repair_ref": null}));

    // The repair is a terminal recording_repair Activity over the source and
    // the wrong extraction, carrying the RecordingRepair record.
    let activity = ok(
        &nexus,
        r#"FIND(?x) WHERE { ?x ACTIVITY {id: :id} }"#,
        json!({"id": repair_ref}),
    )
    .await[0]
        .clone();
    assert_eq!(activity["activity_class"], "recording_repair");
    assert_eq!(activity["status"], "completed");
    assert_eq!(activity["inputs"], json!([{"id": source}, {"id": wrong}]));
    let record = &activity["facets"]["kip://profiles/cognitive-memory@2.0.0/RecordingRepair"];
    assert_eq!(record["replacement_refs"], json!([replacement]));
    assert_eq!(record["reason"], "extraction_error");

    // History reads the repair state at its own snapshot.
    let past = ok(
        &nexus,
        &format!(r#"FIND(?a._system.recording_validity.status) WHERE {{ ?a ASSERTION {{id: :id}} }} AS OF SEQ {before_seq}"#),
        json!({"id": wrong}),
    )
    .await;
    assert_eq!(past[0], "valid");

    // The source bytes are the source bytes.
    let evidence = ok(
        &nexus,
        r#"FIND(?e.content_digest, ?e.lifecycle.status) WHERE { ?e EVIDENCE {id: :id} }"#,
        json!({"id": source}),
    )
    .await;
    assert_eq!(evidence[0], json!([SOURCE_DIGEST, "active"]));

    // The same repair, retried, writes nothing.
    let seq = space_seq(&nexus).await;
    let replay = session
        .repair_recording(
            DEFAULT_SPACE,
            repair(&source, &wrong, original_version, &[&replacement]),
        )
        .await
        .unwrap();
    assert_eq!(replay["repair_ref"], json!(repair_ref));
    assert_eq!(replay["replayed"], true);
    assert_eq!(space_seq(&nexus).await, seq);
}

#[tokio::test]
async fn only_the_recorder_repairs_and_only_against_the_exact_source() {
    let nexus = fresh("guards").await;
    let (_, source, wrong) = misrecord(&nexus, "alice").await;
    let current = version(&nexus, &wrong).await;
    let session = nexus.system_session();

    // The source must be the one the extraction was made from.
    let mut forged = repair(&source, &wrong, current, &[]);
    forged.source_digest =
        "sha256:0000000000000000000000000000000000000000000000000000000000000000".into();
    assert_eq!(
        name(
            session
                .repair_recording(DEFAULT_SPACE, forged)
                .await
                .unwrap_err()
        ),
        "DigestMismatch"
    );
    let mut nowhere = repair(&source, &wrong, current, &[]);
    nowhere.source_locator = "/missing".into();
    assert_eq!(
        name(
            session
                .repair_recording(DEFAULT_SPACE, nowhere)
                .await
                .unwrap_err()
        ),
        "ConstraintViolation"
    );
    assert_eq!(
        name(
            session
                .repair_recording(DEFAULT_SPACE, repair(&source, &wrong, current + 1, &[]))
                .await
                .unwrap_err()
        ),
        "VersionConflict"
    );

    // An unrelated recorder may not repair this recorder's extraction, and a
    // Principal without repair_recording may not repair at all.
    let other = agent(
        &nexus,
        "kip:principal:other",
        &[
            "discover",
            "read",
            "create",
            "update",
            "assert",
            "record_attributed_assertion",
            "repair_recording",
        ],
    )
    .await;
    assert_eq!(
        name(
            other
                .repair_recording(DEFAULT_SPACE, repair(&source, &wrong, current, &[]))
                .await
                .unwrap_err()
        ),
        "NotAuthorized"
    );
    let reader = agent(&nexus, "kip:principal:reader", &["discover", "read"]).await;
    assert_eq!(
        name(
            reader
                .repair_recording(DEFAULT_SPACE, repair(&source, &wrong, current, &[]))
                .await
                .unwrap_err()
        ),
        "NotAuthorized"
    );

    // A replacement the recorder did not write is not its to offer.
    let foreign = ok(
        &other,
        r#"MUTATE {
            UPSERT CONCEPT ?alice { MATCH {type: "Person", key: "alice"} }
            ASSERT ?r (?alice, "diet", "omnivore") {
                by: ?alice, mode: "stated", at: "2026-01-01T00:00:00.000Z", evidence: :msg
            }
        }"#,
        json!({"msg": source}),
    )
    .await["handles"]["r"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        name(
            session
                .repair_recording(DEFAULT_SPACE, repair(&source, &wrong, current, &[&foreign]))
                .await
                .unwrap_err()
        ),
        "NotAuthorized"
    );

    // Nothing above changed the extraction.
    assert_eq!(version(&nexus, &wrong).await, current);

    // Nor can a model pass an Activity off as a repair.
    let response = run(
        &nexus,
        r#"MUTATE { CREATE ACTIVITY ?x { SET FIELDS {activity_class: "recording_repair", status: "completed"} } }"#,
        json!({}),
    )
    .await;
    assert_ne!(response.status, TopLevelStatus::Succeeded);
}

#[tokio::test]
async fn a_recorder_repairs_its_own_extraction() {
    let nexus = fresh("own").await;
    let recorder = agent(
        &nexus,
        "kip:principal:recorder",
        &[
            "discover",
            "read",
            "create",
            "update",
            "assert",
            "record_attributed_assertion",
            "repair_recording",
        ],
    )
    .await;
    let (alice, source, wrong) = misrecord(&recorder, "alice").await;
    let current = version(&nexus, &wrong).await;
    recorder
        .repair_recording(DEFAULT_SPACE, repair(&source, &wrong, current, &[]))
        .await
        .unwrap();
    // Without a replacement, the slot simply has no claim left: insufficient,
    // never "no".
    let slot = diet(&nexus, &alice, None).await;
    assert_eq!(slot["status"], "insufficient", "{slot:#}");
}

#[tokio::test]
async fn repair_references_require_discovery_of_the_activity() {
    let nexus = fresh("repair_visibility").await;
    let (_, source, wrong) = misrecord(&nexus, "alice").await;
    let before = space_seq(&nexus).await;
    let result = nexus
        .system_session()
        .repair_recording(
            DEFAULT_SPACE,
            repair(&source, &wrong, version(&nexus, &wrong).await, &[]),
        )
        .await
        .unwrap();
    let repair_ref = result["repair_ref"].as_str().unwrap();
    let repaired_at = space_seq(&nexus).await;
    let reader = agent(&nexus, "kip:principal:assertion-reader", &["read_history"]).await;
    nexus
        .governance()
        .create_grant(
            GrantDraft {
                space_id: DEFAULT_SPACE.into(),
                grantee_principal: "kip:principal:assertion-reader".into(),
                actions: vec!["read".into()],
                scope: AuthorityScope {
                    kinds: vec!["assertion".into()],
                    ..Default::default()
                },
                ..Default::default()
            },
            SYSTEM_PRINCIPAL,
        )
        .await
        .unwrap();
    let command = r#"FIND(?a._system.recording_validity, ?a.governance.recording_repair) WHERE { ?a ASSERTION {id: :id} }"#;
    for suffix in [String::new(), format!(" AS OF SEQ {repaired_at}")] {
        assert_eq!(
            ok(&reader, &format!("{command}{suffix}"), json!({"id": wrong})).await,
            json!([[{"status": "invalidated", "repair_ref": null}, null]])
        );
    }
    assert_eq!(
        ok(
            &reader,
            &format!("{command} AS OF SEQ {before}"),
            json!({"id": wrong})
        )
        .await,
        json!([[{"status": "valid", "repair_ref": null}, null]])
    );
    assert_eq!(
        ok(
            &reader,
            r#"FIND(?x.id) WHERE { ?x ACTIVITY {id: :id} }"#,
            json!({"id": repair_ref})
        )
        .await,
        json!([])
    );
    assert_eq!(ok(&reader, r#"FIND(?a.id) WHERE { ?a ASSERTION {id: :id} FILTER(?a._system.recording_validity.repair_ref == :repair) }"#,
        json!({"id": wrong, "repair": repair_ref})).await, json!([]));

    // Discovery alone is sufficient to disclose a reference; content remains hidden.
    nexus
        .governance()
        .create_grant(
            GrantDraft {
                space_id: DEFAULT_SPACE.into(),
                grantee_principal: "kip:principal:assertion-reader".into(),
                actions: vec!["discover".into()],
                scope: AuthorityScope {
                    kinds: vec!["activity".into()],
                    ..Default::default()
                },
                ..Default::default()
            },
            SYSTEM_PRINCIPAL,
        )
        .await
        .unwrap();
    assert_eq!(
        ok(&reader, command, json!({"id": wrong})).await,
        json!([[{"status": "invalidated", "repair_ref": repair_ref}, repair_ref]])
    );
}

#[tokio::test]
async fn a_repair_preserves_a_claim_time_from_historical_source_material() {
    let nexus = fresh("historical_source_time").await;
    let seeded = ok(&nexus, r#"MUTATE {
        CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
        CREATE EVIDENCE ?source { SET FIELDS {
            evidence_class: "message", content_digest: :digest,
            payload: {timestamp: "2026-01-01T00:00:00.000Z", content: "I eat meat."},
            observed_at: "2026-09-01T00:00:00.000Z"
        } }
        ASSERT ?wrong (?alice, "diet", "vegetarian") { by: ?alice, mode: "stated", at: "2026-01-01T00:00:00.000Z", evidence: ?source }
        ASSERT ?right (?alice, "diet", "omnivore") { by: ?alice, mode: "stated", at: "2026-01-01T00:00:00.000Z", evidence: ?source }
        ASSERT ?wrong_time (?alice, "diet", "vegetarian") { by: ?alice, mode: "stated", at: "2026-09-01T00:00:00.000Z", evidence: ?source }
        ASSERT ?late_wrong (?alice, "diet", "vegetarian") { by: ?alice, mode: "stated", at: "2026-09-24T00:00:00.000Z", evidence: ?source }
        ASSERT ?late_copy (?alice, "diet", "omnivore") { by: ?alice, mode: "stated", at: "2026-09-24T00:00:00.000Z", evidence: ?source }
    }"#, json!({"digest": SOURCE_DIGEST})).await;
    let h = &seeded["handles"];
    let wrong = h["wrong"].as_str().unwrap();
    let session = nexus.system_session();
    let source = h["source"].as_str().unwrap();
    let late = h["late_wrong"].as_str().unwrap();
    assert_eq!(
        session
            .repair_recording(
                DEFAULT_SPACE,
                repair(
                    source,
                    late,
                    version(&nexus, late).await,
                    &[h["late_copy"].as_str().unwrap()]
                )
            )
            .await
            .unwrap_err()
            .name(),
        "ConstraintViolation",
        "copying an old extraction's recording time must not make it a new claim"
    );
    let wrong_time = h["wrong_time"].as_str().unwrap();
    let mut recover_time = repair(
        source,
        wrong_time,
        version(&nexus, wrong_time).await,
        &[h["right"].as_str().unwrap()],
    );
    assert_eq!(
        session
            .repair_recording(DEFAULT_SPACE, recover_time.clone())
            .await
            .unwrap_err()
            .name(),
        "ConstraintViolation"
    );
    recover_time.source_locator = "/timestamp".into();
    session
        .repair_recording(DEFAULT_SPACE, recover_time)
        .await
        .unwrap();
    nexus
        .system_session()
        .repair_recording(
            DEFAULT_SPACE,
            repair(
                h["source"].as_str().unwrap(),
                wrong,
                version(&nexus, wrong).await,
                &[h["right"].as_str().unwrap()],
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        ok(
            &nexus,
            r#"FIND(?a.asserted_at) WHERE { ?a ASSERTION {id: :id} }"#,
            json!({"id": h["right"]})
        )
        .await,
        json!(["2026-01-01T00:00:00.000Z"])
    );
}

#[tokio::test]
async fn an_extraction_repair_compares_merged_actor_identities() {
    let nexus = fresh("repair_merged_actor").await;
    let (alice, source, wrong) = misrecord(&nexus, "alice").await;
    let created = ok(
        &nexus,
        r#"MUTATE {
        CREATE CONCEPT ?canonical { TYPE "Person" NAME "Alice canonical" }
        CREATE CONCEPT ?other { TYPE "Person" NAME "Bob" }
    }"#,
        json!({}),
    )
    .await;
    let canonical = &created["handles"]["canonical"];
    ok(
        &nexus,
        "MERGE CONCEPT :from INTO :into",
        json!({"from": alice, "into": canonical}),
    )
    .await;
    let assertions = ok(&nexus, r#"MUTATE {
        ASSERT ?right (:alice, "diet", "omnivore") { by: :alice, mode: "stated", at: "2026-01-01T00:00:00.000Z", evidence: :source }
        ASSERT ?other (:alice, "diet", "omnivore") { by: :other, mode: "stated", at: "2026-01-01T00:00:00.000Z", evidence: :source }
    }"#, json!({"alice": {"id": canonical}, "other": {"id": created["handles"]["other"]}, "source": source})).await;
    let current = version(&nexus, &wrong).await;
    let h = &assertions["handles"];
    let session = nexus.system_session();
    assert_eq!(
        session
            .repair_recording(
                DEFAULT_SPACE,
                repair(&source, &wrong, current, &[h["other"].as_str().unwrap()])
            )
            .await
            .unwrap_err()
            .name(),
        "ConstraintViolation"
    );
    session
        .repair_recording(
            DEFAULT_SPACE,
            repair(&source, &wrong, current, &[h["right"].as_str().unwrap()]),
        )
        .await
        .unwrap();
}

/// MIF-017's replacement variant: January's misextraction is repaired in
/// September. The replacement keeps January's claim time, so the genuine
/// September change still ends it.
#[tokio::test]
async fn a_repair_keeps_the_original_claim_time() {
    let nexus = fresh("replacement").await;
    ok(
        &nexus,
        r#"DEFINE CONCEPT TYPE "ColorScheme" {description: "A display color scheme."}"#,
        json!({}),
    )
    .await;
    let seeded = ok(
        &nexus,
        r#"MUTATE {
            UPSERT CONCEPT ?alice { MATCH {type: "Person", key: "alice"} SET FIELDS {name: "Alice"} }
            UPSERT CONCEPT ?dark { MATCH {type: "ColorScheme", key: "dark"} SET FIELDS {name: "Dark"} }
            UPSERT CONCEPT ?blue { MATCH {type: "ColorScheme", key: "blue"} SET FIELDS {name: "Blue"} }
            UPSERT CONCEPT ?light { MATCH {type: "ColorScheme", key: "light"} SET FIELDS {name: "Light"} }
            CREATE EVIDENCE ?jan {
                SET FIELDS {evidence_class: "message", payload: "I prefer dark.",
                            content_digest: :digest, observed_at: "2026-01-01T00:00:00.000Z"}
            }
            CREATE EVIDENCE ?sep {
                SET FIELDS {evidence_class: "message", payload: "I prefer light now.",
                            content_digest: "sha256:5e9", observed_at: "2026-09-01T00:00:00.000Z"}
            }
            ASSERT ?wrong (?alice, "prefers", ?blue) {
                by: ?alice, mode: "stated", at: "2026-01-01T00:00:00.000Z", evidence: ?jan
            }
            ASSERT ?right_light (?alice, "prefers", ?light) {
                by: ?alice, mode: "stated", at: "2026-09-01T00:00:00.000Z",
                valid: {from: "2026-09-01T00:00:00.000Z"}, evidence: ?sep
            }
        }"#,
        json!({"digest": SOURCE_DIGEST}),
    )
    .await;
    let handle = |h: &str| seeded["handles"][h].as_str().unwrap().to_string();
    let (alice, jan, wrong, dark) = (
        handle("alice"),
        handle("jan"),
        handle("wrong"),
        handle("dark"),
    );

    // Repaired in September; a replacement stamped with the repair's own time
    // would be refused, because it would become a new claim.
    let late = ok(
        &nexus,
        r#"MUTATE {
            ASSERT ?r (:alice, "prefers", :dark) {
                by: :alice, mode: "stated", at: "2026-09-24T00:00:00.000Z", evidence: :jan
            }
        }"#,
        json!({"alice": {"id": alice}, "dark": {"id": dark}, "jan": jan}),
    )
    .await["handles"]["r"]
        .as_str()
        .unwrap()
        .to_string();
    let current = version(&nexus, &wrong).await;
    let session = nexus.system_session();
    let mut request = repair(&jan, &wrong, current, &[&late]);
    request.source_locator = "bytes=0-13".into();
    assert_eq!(
        name(
            session
                .repair_recording(DEFAULT_SPACE, request)
                .await
                .unwrap_err()
        ),
        "ConstraintViolation"
    );

    let replacement = ok(
        &nexus,
        r#"MUTATE {
            ASSERT ?r (:alice, "prefers", :dark) {
                by: :alice, mode: "stated", at: "2026-01-01T00:00:00.000Z", evidence: :jan
            }
        }"#,
        json!({"alice": {"id": alice}, "dark": {"id": dark}, "jan": jan}),
    )
    .await["handles"]["r"]
        .as_str()
        .unwrap()
        .to_string();
    let mut request = repair(&jan, &wrong, current, &[&replacement]);
    request.source_locator = "bytes=0-13".into();
    session
        .repair_recording(DEFAULT_SPACE, request)
        .await
        .unwrap();

    // Recording repair touched only the January extraction: the late
    // replacement attempt is still a standing claim of its own, so retract it
    // to read the three-claim history MIF-017 describes.
    ok(
        &nexus,
        r#"TRANSITION :late TO "retracted""#,
        json!({"late": late}),
    )
    .await;

    let proposition = |id: String| {
        let nexus = &nexus;
        async move {
            ok(
                nexus,
                r#"FIND(?a.proposition.id) WHERE { ?a ASSERTION {id: :id} }"#,
                json!({"id": id}),
            )
            .await[0]
                .clone()
        }
    };
    let january = diet_like(&nexus, &alice, "2026-02-01T00:00:00.000Z").await;
    assert_eq!(
        january["accepted_values"],
        json!([proposition(replacement.clone()).await]),
        "{january:#}"
    );
    let now = diet_like(&nexus, &alice, "2026-09-25T00:00:00.000Z").await;
    assert_eq!(
        now["accepted_values"],
        json!([proposition(handle("right_light")).await]),
        "{now:#}"
    );
    let raw = ok(
        &nexus,
        r#"FIND(?a.asserted_at) WHERE { ?a ASSERTION {id: :id} }"#,
        json!({"id": replacement}),
    )
    .await;
    assert_eq!(raw[0], "2026-01-01T00:00:00.000Z");
}

async fn diet_like(executor: &impl Executor, alice: &str, at: &str) -> Json {
    let result = ok(
        executor,
        &format!(
            r#"FIND(?slot) WHERE {{ ?slot BELIEF SLOT (:alice, "prefers") }} FOR TIME "{at}""#
        ),
        json!({"alice": alice}),
    )
    .await;
    result.as_array().unwrap()[0].clone()
}

/// A Capsule carries another Brain's repair history; importing it is not
/// forging a repair here, so it is not refused.
#[tokio::test]
async fn an_exported_repair_imports() {
    let source = fresh("export_source").await;
    let seeded = ok(
        &source,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?bob { TYPE "Person" NAME "Bob" }
            CREATE EVIDENCE ?msg {
                SET FIELDS {evidence_class: "message", payload: "Alice and Bob are colleagues.",
                            content_digest: :digest, observed_at: "2026-01-01T00:00:00.000Z"}
            }
            ASSERT ?wrong (?alice, "same_as", ?bob) {
                by: ?alice, mode: "stated", at: "2026-01-01T00:00:00.000Z", evidence: ?msg
            }
        }"#,
        json!({"digest": SOURCE_DIGEST}),
    )
    .await;
    let (msg, wrong) = (
        seeded["handles"]["msg"].as_str().unwrap().to_string(),
        seeded["handles"]["wrong"].as_str().unwrap().to_string(),
    );
    let mut request = repair(&msg, &wrong, version(&source, &wrong).await, &[]);
    request.source_locator = "bytes=0-4".into();
    source
        .system_session()
        .repair_recording(DEFAULT_SPACE, request)
        .await
        .unwrap();

    let artifact = ok(
        &source,
        r#"EXPORT CAPSULE ?x WHERE { ?x ACTIVITY {} }"#,
        json!({}),
    )
    .await;
    let capsule = anda_cognitive_nexus::capsule::parse(&artifact.to_string()).unwrap();
    let destination = fresh("export_destination").await;
    let report = destination
        .import_capsule(&capsule, DEFAULT_SPACE)
        .await
        .expect("an exported repair imports");
    assert_eq!(report.counts["activity"], 1);
    assert_eq!(report.counts["assertion"], 1);
}
