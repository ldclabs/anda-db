//! The Space's draft vocabulary (Spec §20.16): what the engine suite's
//! `draft-vocabulary.json` cannot drive — Space history, host activation,
//! idempotent retry, the `propose_schema` boundary, promotion and Capsule
//! mapping.

use anda_cognitive_nexus::{
    CognitiveNexus,
    capsule::SymbolMapping,
    governance::{
        AuthContext, SYSTEM_PRINCIPAL,
        rows::principal_class,
        store::{GrantDraft, PrincipalDraft},
    },
    nexus::DEFAULT_SPACE,
    schema::{PackageState, SchemaLock, SchemaPackage, SymbolKind},
};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{Executor, Json, Request, Response, TopLevelStatus};
use object_store::memory::InMemory;
use serde_json::json;
use std::sync::Arc;

const PROFILE_ID: &str = "kip://profiles/cognitive-memory";

/// A package that later names what a Space drafted first.
const MUSIC: &str = r#"{
    "format": "KIP-Schema-Package",
    "format_version": "2.0-draft",
    "manifest": {"package_id": "kip://test/music", "version": "1.0.0"},
    "definitions": {
        "concept_types": {
            "Instrument": {"kind": "ConceptType", "description": "A musical instrument.", "attributes": {"open": true, "fields": {}}}
        },
        "predicates": {
            "mentors": {"kind": "PredicateType", "description": "The subject mentors the object."}
        }
    }
}"#;

fn profile_lock() -> SchemaLock {
    let mut lock = SchemaLock::default();
    lock.packages.insert(PROFILE_ID.into(), "2.0.0".into());
    lock.states.insert(PROFILE_ID.into(), PackageState::Active);
    lock
}

async fn fresh(name: &str) -> CognitiveNexus {
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: name.to_string(),
            description: "draft vocabulary tests".to_string(),
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
    nexus
        .activate_schema(DEFAULT_SPACE, profile_lock())
        .await
        .unwrap();
    nexus
}

async fn run(executor: &impl Executor, command: &str, params: Json, key: Option<&str>) -> Response {
    let mut request = json!({
        "kip": "2.0",
        "operations": [{"command": command, "parameters": params}]
    });
    if let Some(key) = key {
        request["operations"][0]["idempotency_key"] = json!(key);
    }
    let request = serde_json::from_value::<Request>(request).unwrap();
    let parsed = request.operations[0].parse().unwrap();
    executor
        .execute(parsed, &request, &request.operations[0])
        .await
}

async fn ok(executor: &impl Executor, command: &str) -> Json {
    let response = run(executor, command, json!({}), None).await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "{command}\n{:#?}",
        response.error
    );
    response.first_result().cloned().unwrap_or(Json::Null)
}

fn error_code(response: &Response) -> &str {
    response
        .error
        .as_ref()
        .or_else(|| response.results.first().and_then(|r| r.error.as_ref()))
        .map(|error| error.code.as_str())
        .unwrap_or("")
}

async fn err(executor: &impl Executor, command: &str) -> String {
    let response = run(executor, command, json!({}), None).await;
    assert_ne!(response.status, TopLevelStatus::Succeeded, "{command}");
    error_code(&response).to_string()
}

const DEFINE_MENTORS: &str = r#"DEFINE PREDICATE "mentors" {
    description: "The subject mentors the object.",
    subject: {concept_types: ["Person"]},
    object: {concept_types: ["Person"]}
}"#;

#[tokio::test]
async fn a_definition_is_a_schema_transaction_in_the_space_history() {
    let nexus = fresh("history").await;
    let before = ok(&nexus, "DESCRIBE SCHEMA ENVIRONMENT").await;
    let response = run(&nexus, DEFINE_MENTORS, json!({}), None).await;
    assert_eq!(response.status, TopLevelStatus::Succeeded);
    let result = response.first_result().unwrap();
    assert_eq!(result["ref"], "kip://local/draft@0.0.0/mentors");
    let version = result["schema_environment_version"].as_u64().unwrap();
    assert_eq!(version, before["version"].as_u64().unwrap() + 1);

    // A governance transaction that published a `schema` control change.
    let receipt = response.results[0].receipt.as_ref().unwrap();
    assert_eq!(receipt.transaction_class.as_deref(), Some("governance"));
    let tx = receipt.tx_id.clone().unwrap();
    let described = ok(&nexus, &format!(r#"DESCRIBE TRANSACTION "{tx}""#)).await;
    assert!(
        described["control_changes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|change| change["kind"] == "schema"),
        "{described}"
    );

    // The draft package is Space state from here on, with a computed digest,
    // and a read before the definition does not see it.
    let package = ok(&nexus, r#"DESCRIBE PACKAGE "kip://local/draft@0.0.0""#).await;
    assert_eq!(package["status"], "active");
    assert!(
        package["integrity"]["content_digest"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert!(package["definitions"]["predicates"]["mentors"].is_object());
    let seq = receipt.space_seq.unwrap();
    let past = ok(
        &nexus,
        &format!("DESCRIBE SCHEMA ENVIRONMENT AS OF SEQ {}", seq - 1),
    )
    .await;
    assert!(
        past["packages"].get("kip://local/draft").is_none(),
        "{past}"
    );
    let now = ok(&nexus, "DESCRIBE SCHEMA ENVIRONMENT").await;
    assert_eq!(now["packages"]["kip://local/draft"], "0.0.0");

    // Endpoint types persist as exact references.
    let described = ok(&nexus, r#"DESCRIBE PREDICATE "mentors""#).await;
    assert_eq!(
        described["definition"]["subject"]["concept_types"],
        json!(["kip://profiles/cognitive-memory@2.0.0/Person"])
    );
}

#[tokio::test]
async fn activating_other_packages_never_removes_the_draft() {
    let nexus = fresh("activation").await;
    ok(&nexus, DEFINE_MENTORS).await;
    let version = ok(&nexus, "DESCRIBE SCHEMA ENVIRONMENT").await["version"].clone();

    // A host re-asserting its baseline lock on start changes nothing.
    nexus
        .ensure_schema(DEFAULT_SPACE, profile_lock())
        .await
        .unwrap();
    assert_eq!(
        ok(&nexus, "DESCRIBE SCHEMA ENVIRONMENT").await["version"],
        version
    );

    // Activating another package carries the draft forward.
    nexus
        .install_package(&SchemaPackage::parse(MUSIC).unwrap(), "test")
        .await
        .unwrap();
    let mut lock = profile_lock();
    lock.packages
        .insert("kip://test/music".into(), "1.0.0".into());
    lock.states
        .insert("kip://test/music".into(), PackageState::Installed);
    let env = nexus.activate_schema(DEFAULT_SPACE, lock).await.unwrap();
    assert!(env.lock.draft.predicates.contains_key("mentors"));
    assert_eq!(
        env.lock.states.get(anda_kip::DRAFT_PACKAGE_ID),
        Some(&PackageState::Active)
    );

    // And refuses to switch it off.
    let mut lock = env.lock.clone();
    lock.states
        .insert(anda_kip::DRAFT_PACKAGE_ID.into(), PackageState::Deprecated);
    let refused = nexus
        .activate_schema(DEFAULT_SPACE, lock)
        .await
        .unwrap_err();
    assert_eq!(refused.name(), "ConstraintViolation");
}

#[tokio::test]
async fn a_retry_replays_and_a_repeat_conflicts() {
    let nexus = fresh("retry").await;
    let first = run(&nexus, DEFINE_MENTORS, json!({}), Some("define-mentors")).await;
    assert_eq!(first.status, TopLevelStatus::Succeeded);
    let replayed = run(&nexus, DEFINE_MENTORS, json!({}), Some("define-mentors")).await;
    assert_eq!(replayed.status, TopLevelStatus::Succeeded);
    assert_eq!(replayed.first_result(), first.first_result());
    assert_eq!(
        replayed.results[0].receipt.as_ref().unwrap().tx_id,
        first.results[0].receipt.as_ref().unwrap().tx_id
    );
    // Without a key, a repeat is a second definition of a taken name.
    assert_eq!(err(&nexus, DEFINE_MENTORS).await, "SchemaSymbolConflict");
    // A dry run checks and writes nothing.
    let request = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "options": {"dry_run": true},
        "operations": [{"command": r#"DEFINE CONCEPT TYPE "Genre" {description: "A genre."}"#}]
    }))
    .unwrap();
    let parsed = request.operations[0].parse().unwrap();
    let preview = nexus
        .execute(parsed, &request, &request.operations[0])
        .await;
    assert_eq!(
        preview.first_result().unwrap()["ref"],
        "kip://local/draft@0.0.0/Genre"
    );
    let package = ok(&nexus, r#"DESCRIBE PACKAGE "kip://local/draft@0.0.0""#).await;
    assert!(
        package["definitions"]["concept_types"]
            .get("Genre")
            .is_none()
    );
}

#[tokio::test]
async fn propose_schema_confers_drafting_and_nothing_more() {
    let nexus = fresh("authority").await;
    let gov = nexus.governance();
    for (principal, actions) in [
        (
            "kip:principal:drafter",
            vec!["propose_schema", "read", "discover"],
        ),
        ("kip:principal:reader", vec!["read", "discover"]),
    ] {
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
                actions: actions.into_iter().map(str::to_string).collect(),
                ..Default::default()
            },
            SYSTEM_PRINCIPAL,
        )
        .await
        .unwrap();
    }
    let drafter = nexus.session(AuthContext::principal("kip:principal:drafter"));
    let reader = nexus.session(AuthContext::principal("kip:principal:reader"));

    assert_eq!(err(&reader, DEFINE_MENTORS).await, "NotAuthorized");
    ok(&drafter, DEFINE_MENTORS).await;
    // No schema management, no promotion.
    let install = drafter
        .install_package(DEFAULT_SPACE, &SchemaPackage::parse(MUSIC).unwrap(), "test")
        .await
        .unwrap_err();
    assert_eq!(install.name(), "NotAuthorized");
    let promote = drafter
        .promote_draft_symbol(
            DEFAULT_SPACE,
            SymbolKind::PredicateType,
            "mentors",
            "kip://profiles/cognitive-memory@2.0.0/prefers",
        )
        .await
        .unwrap_err();
    assert_eq!(promote.name(), "NotAuthorized");
}

#[tokio::test]
async fn a_promotion_joins_the_lineages_and_keeps_exact_references() {
    let nexus = fresh("promotion").await;
    ok(
        &nexus,
        r#"DEFINE CONCEPT TYPE "Instrument" {description: "A musical instrument."}"#,
    )
    .await;
    ok(&nexus, DEFINE_MENTORS).await;
    ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?violin { TYPE "Instrument" NAME "Violin" }
            CREATE CONCEPT ?ada { TYPE "Person" NAME "Ada" }
            CREATE CONCEPT ?grace { TYPE "Person" NAME "Grace" }
            ASSERT (?ada, "mentors", ?grace) { by: ?ada, mode: "stated", at: "2026-01-01T00:00:00.000Z" }
        }"#,
    )
    .await;

    // A package that names the same symbols makes the local names ambiguous
    // until the drafts are promoted to it (§20.7, §20.16).
    nexus
        .install_package(&SchemaPackage::parse(MUSIC).unwrap(), "test")
        .await
        .unwrap();
    let mut lock = profile_lock();
    lock.packages
        .insert("kip://test/music".into(), "1.0.0".into());
    lock.states
        .insert("kip://test/music".into(), PackageState::Active);
    nexus.activate_schema(DEFAULT_SPACE, lock).await.unwrap();
    assert_eq!(
        err(
            &nexus,
            r#"FIND(?c) WHERE { ?c CONCEPT {type: "Instrument"} }"#
        )
        .await,
        "SchemaSymbolAmbiguous"
    );
    let before = ok(&nexus, "DESCRIBE SCHEMA ENVIRONMENT").await["version"]
        .as_u64()
        .unwrap();

    let unpromoted = ok(&nexus, "DESCRIBE SPACE").await["seq"].as_u64().unwrap();
    let session = nexus.system_session();
    session
        .promote_draft_symbol(
            DEFAULT_SPACE,
            SymbolKind::ConceptType,
            "Instrument",
            "kip://test/music@1.0.0/Instrument",
        )
        .await
        .unwrap();
    session
        .promote_draft_symbol(
            DEFAULT_SPACE,
            SymbolKind::PredicateType,
            "kip://local/draft@0.0.0/mentors",
            "kip://test/music@1.0.0/mentors",
        )
        .await
        .unwrap();
    let again = session
        .promote_draft_symbol(
            DEFAULT_SPACE,
            SymbolKind::ConceptType,
            "Instrument",
            "kip://test/music@1.0.0/Instrument",
        )
        .await
        .unwrap_err();
    assert_eq!(again.name(), "ConstraintViolation");

    let env = ok(&nexus, "DESCRIBE SCHEMA ENVIRONMENT").await;
    assert_eq!(env["version"], before + 2);
    assert!(
        env["lineage_maps"]
            .as_array()
            .unwrap()
            .contains(&json!({"kind": "ConceptType", "from": "kip://local/draft/Instrument", "to": "kip://test/music/Instrument"}))
    );

    // The local name now resolves to the package symbol, and matching by
    // lineage treats the two as one; the element keeps its exact reference.
    ok(
        &nexus,
        r#"MUTATE { CREATE CONCEPT ?cello { TYPE "Instrument" NAME "Cello" } }"#,
    )
    .await;
    let found = ok(
        &nexus,
        r#"FIND(?c.name, ?c.schema_ref) WHERE { ?c CONCEPT {type: "Instrument"} } ORDER BY ?c.name ASC"#,
    )
    .await;
    assert_eq!(
        found,
        json!([
            ["Cello", "kip://test/music@1.0.0/Instrument"],
            ["Violin", "kip://local/draft@0.0.0/Instrument"]
        ])
    );
    let mentored = ok(
        &nexus,
        r#"FIND(?o.name) WHERE { ?s CONCEPT {name: "Ada"} ?p (?s, "mentors", ?o) }"#,
    )
    .await;
    assert_eq!(mentored, json!(["Grace"]));
    // A read before the promotion does not see the mapping.
    let past = ok(
        &nexus,
        &format!("DESCRIBE SCHEMA ENVIRONMENT AS OF SEQ {unpromoted}"),
    )
    .await;
    assert!(
        past["lineage_maps"]
            .as_array()
            .is_none_or(|maps| maps.is_empty()),
        "{past}"
    );
}

/// Installs and activates [`MUSIC`] next to the Profile.
async fn activate_music(nexus: &CognitiveNexus) {
    nexus
        .install_package(&SchemaPackage::parse(MUSIC).unwrap(), "test")
        .await
        .unwrap();
    let mut lock = profile_lock();
    lock.packages
        .insert("kip://test/music".into(), "1.0.0".into());
    lock.states
        .insert("kip://test/music".into(), PackageState::Active);
    nexus.activate_schema(DEFAULT_SPACE, lock).await.unwrap();
}

/// Promotes the drafted `Instrument` and `mentors` to [`MUSIC`]'s symbols.
async fn promote_to_music(nexus: &CognitiveNexus) {
    let session = nexus.system_session();
    for (kind, name) in [
        (SymbolKind::ConceptType, "Instrument"),
        (SymbolKind::PredicateType, "mentors"),
    ] {
        session
            .promote_draft_symbol(
                DEFAULT_SPACE,
                kind,
                name,
                &format!("kip://test/music@1.0.0/{name}"),
            )
            .await
            .unwrap();
    }
}

/// An identity written under a draft symbol is the same identity under the
/// symbol it was promoted to (§20.16): the tuple (§12.3), the logical key
/// (§7.3) and the `CLIENT KEY` retry (§52.1) resolve to the element that
/// exists, `MERGE CONCEPT` treats the two types as one lineage, and a value
/// correction stays in its slot (§14.2).
#[tokio::test]
async fn identities_written_under_a_draft_survive_its_promotion() {
    let nexus = fresh("identity").await;
    ok(
        &nexus,
        r#"DEFINE CONCEPT TYPE "Instrument" {description: "A musical instrument."}"#,
    )
    .await;
    ok(&nexus, DEFINE_MENTORS).await;
    ok(
        &nexus,
        r#"MUTATE {
            UPSERT CONCEPT ?viola { MATCH {type: "Instrument", key: "viola"} SET FIELDS {name: "Viola"} }
            CREATE CONCEPT ?violin { TYPE "Instrument" NAME "Violin" CLIENT KEY "violin" }
            CREATE CONCEPT ?ada { TYPE "Person" NAME "Ada" }
            CREATE CONCEPT ?grace { TYPE "Person" NAME "Grace" }
            ASSERT (?ada, "mentors", ?grace) { by: ?ada, mode: "stated", at: "2026-01-01T00:00:00.000Z" }
        }"#,
    )
    .await;
    let ids = ok(
        &nexus,
        r#"FIND(?ada.id, ?grace.id, ?a.id) WHERE {
            ?ada CONCEPT {name: "Ada"}
            ?grace CONCEPT {name: "Grace"}
            ?p PROPOSITION (?ada, "mentors", ?grace)
            ?a ASSERTION {proposition: ?p}
        }"#,
    )
    .await;
    let (ada, grace, claim) = (ids[0][0].clone(), ids[0][1].clone(), ids[0][2].clone());
    activate_music(&nexus).await;
    promote_to_music(&nexus).await;

    let succeeded = |response: &Response, what: &str| {
        assert_eq!(
            response.status,
            TopLevelStatus::Succeeded,
            "{what}\n{:#?}",
            response.error
        );
    };

    // The tuple, written again under the package Predicate, is bound.
    let ensured = run(
        &nexus,
        r#"MUTATE { ENSURE PROPOSITION ?p (:ada, "mentors", :grace) }"#,
        json!({"ada": ada, "grace": grace}),
        None,
    )
    .await;
    succeeded(&ensured, "ENSURE after promotion");
    let tuples = ok(
        &nexus,
        r#"FIND(?p.id) WHERE { ?s CONCEPT {name: "Ada"} ?p PROPOSITION (?s, "mentors", ?o) }"#,
    )
    .await;
    assert_eq!(tuples.as_array().map(Vec::len), Some(1), "{tuples}");

    // The logical key resolves, and so does the CLIENT KEY retry.
    ok(
        &nexus,
        r#"MUTATE { UPSERT CONCEPT ?v { MATCH {type: "Instrument", key: "viola"} SET FIELDS {name: "Viola da braccio"} } }"#,
    )
    .await;
    ok(
        &nexus,
        r#"MUTATE { CREATE CONCEPT ?violin { TYPE "Instrument" NAME "Violin" CLIENT KEY "violin" } }"#,
    )
    .await;
    let instruments = ok(
        &nexus,
        r#"FIND(?c.name, ?c.schema_ref) WHERE { ?c CONCEPT {type: "Instrument"} } ORDER BY ?c.name ASC"#,
    )
    .await;
    assert_eq!(
        instruments,
        json!([
            ["Viola da braccio", "kip://local/draft@0.0.0/Instrument"],
            ["Violin", "kip://local/draft@0.0.0/Instrument"]
        ])
    );

    // Two Concepts across the promotion are one type lineage for a merge.
    ok(
        &nexus,
        r#"MUTATE { CREATE CONCEPT ?fiddle { TYPE "Instrument" NAME "Fiddle" } }"#,
    )
    .await;
    let pair = ok(
        &nexus,
        r#"FIND(?violin.id, ?fiddle.id) WHERE {
            ?violin CONCEPT {name: "Violin"}
            ?fiddle CONCEPT {name: "Fiddle"}
        }"#,
    )
    .await;
    let merged = run(
        &nexus,
        "MERGE CONCEPT :from INTO :into",
        json!({"from": pair[0][0], "into": pair[0][1]}),
        None,
    )
    .await;
    succeeded(&merged, "MERGE across the promotion");

    // A value correction under the package Predicate stays in the slot of
    // the claim made under the draft.
    let corrected = run(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?linus { TYPE "Person" NAME "Linus" }
            ASSERT (:ada, "mentors", ?linus) { by: :ada, mode: "stated", at: "2026-01-01T00:00:00.000Z" } SUPERSEDING :claim
        }"#,
        json!({"ada": ada, "claim": claim}),
        None,
    )
    .await;
    succeeded(&corrected, "SUPERSEDING across the promotion");
}

#[tokio::test]
async fn a_capsule_maps_source_draft_symbols_or_refuses() {
    let source = fresh("capsule_source").await;
    ok(
        &source,
        r#"DEFINE CONCEPT TYPE "Instrument" {description: "A musical instrument."}"#,
    )
    .await;
    ok(
        &source,
        r#"MUTATE { CREATE CONCEPT ?violin { TYPE "Instrument" NAME "Violin" } }"#,
    )
    .await;
    let capsule = ok(
        &source,
        r#"EXPORT CAPSULE ?c WHERE { ?c CONCEPT {type: "Instrument"} } WITH {include_schema: true}"#,
    )
    .await;
    assert!(
        capsule["payload"]["schema_dependencies"]
            .as_array()
            .unwrap()
            .iter()
            .any(|dependency| dependency["package_ref"] == "kip://local/draft@0.0.0"),
        "{}",
        capsule["payload"]["schema_dependencies"]
    );
    let capsule: anda_kip::Capsule = serde_json::from_value(capsule).unwrap();

    let destination = fresh("capsule_destination").await;
    // The destination's own draft of the same name is never matched by name.
    ok(
        &destination,
        r#"DEFINE CONCEPT TYPE "Instrument" {description: "Something played."}"#,
    )
    .await;
    let session = destination.system_session();
    let refused = session
        .import_capsule(DEFAULT_SPACE, &capsule, false)
        .await
        .unwrap_err();
    assert_eq!(refused.name(), "SchemaPackageUnavailable");
    assert!(
        refused
            .message
            .contains("ConceptType kip://local/draft@0.0.0/Instrument"),
        "{}",
        refused.message
    );

    session
        .import_capsule_mapped(
            DEFAULT_SPACE,
            &capsule,
            false,
            &[SymbolMapping {
                kind: "ConceptType".into(),
                from: "kip://local/draft@0.0.0/Instrument".into(),
                to: "Instrument".into(),
            }],
        )
        .await
        .unwrap();
    let imported = ok(
        &destination,
        r#"FIND(?c.schema_ref) WHERE { ?c CONCEPT {name: "Violin"} }"#,
    )
    .await;
    assert_eq!(imported, json!(["kip://local/draft@0.0.0/Instrument"]));
}
