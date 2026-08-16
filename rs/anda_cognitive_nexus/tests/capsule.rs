//! Cognitive Capsule tests: export, integrity, and what import refuses.

use anda_cognitive_nexus::{
    CognitiveNexus,
    nexus::DEFAULT_SPACE,
    schema::{PackageState, SchemaLock, SchemaPackage},
};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{Executor, Json, Request, TopLevelStatus};
use object_store::memory::InMemory;
use serde_json::json;
use std::sync::Arc;

const COGNITIVE_MEMORY: &str = anda_cognitive_nexus::profiles::COGNITIVE_MEMORY;
const PROFILE_ID: &str = "kip://profiles/cognitive-memory";

async fn fresh(name: &str, with_schema: bool) -> CognitiveNexus {
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: name.to_string(),
            description: "capsule tests".to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let nexus = CognitiveNexus::connect(Arc::new(db)).await.unwrap();
    if with_schema {
        nexus
            .install_package(&SchemaPackage::parse(COGNITIVE_MEMORY).unwrap(), "test")
            .await
            .unwrap();
        let mut lock = SchemaLock::default();
        lock.packages
            .insert(PROFILE_ID.to_string(), "2.0.0".to_string());
        lock.states
            .insert(PROFILE_ID.to_string(), PackageState::Active);
        nexus.activate_schema(DEFAULT_SPACE, lock).await.unwrap();
    }
    nexus
}

async fn run(nexus: &CognitiveNexus, command: &str) -> anda_kip::Response {
    let request = Request::single(command);
    let parsed = anda_kip::parse_kip(command).unwrap_or_else(|err| panic!("{command}\n{err}"));
    nexus
        .execute(parsed, &request, &request.operations[0])
        .await
}

async fn ok(nexus: &CognitiveNexus, command: &str) -> Json {
    let response = run(nexus, command).await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "{command}\n{:#?}",
        response.error
    );
    response.first_result().cloned().unwrap_or(Json::Null)
}

async fn with_param(nexus: &CognitiveNexus, command: &str, params: Json) -> anda_kip::Response {
    let request = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "operations": [{"command": command, "parameters": params}]
    }))
    .unwrap();
    let parsed = request.operations[0].parse().unwrap();
    nexus
        .execute(parsed, &request, &request.operations[0])
        .await
}

/// A small graph: Alice prefers dark mode, because she said so.
async fn seeded(name: &str) -> CognitiveNexus {
    let nexus = fresh(name, true).await;
    ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
            ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
            CREATE EVIDENCE ?e {
                SET FIELDS {evidence_class: "user_statement", payload: "I prefer dark mode."}
            }
            CREATE ASSERTION ?a {
                SET FIELDS {proposition: ?p, asserted_by: ?alice, stance: "support", mode: "stated", confidence: 0.9}
                SET STRUCTURAL { ("evidence", ?e) {role: "support"} }
            }
        }"#,
    )
    .await;
    nexus
}

#[tokio::test]
async fn an_export_carries_the_referential_closure_of_its_roots() {
    // A Capsule that referenced elements it did not carry would import as a
    // graph full of dangling edges.
    let nexus = seeded("closure").await;
    let capsule = ok(&nexus, r#"EXPORT CAPSULE ?a WHERE { ?a ASSERTION {} }"#).await;

    let records = &capsule["payload"]["records"];
    assert_eq!(records["assertions"].as_array().unwrap().len(), 1);
    // The Assertion's Proposition, its Evidence and its assertor all came
    // along, and the Proposition's own endpoints with them.
    assert_eq!(records["propositions"].as_array().unwrap().len(), 1);
    assert_eq!(records["evidence"].as_array().unwrap().len(), 1);
    assert_eq!(records["concepts"].as_array().unwrap().len(), 2);

    // `closure: "none"` exports exactly what was asked for, and says so.
    let bare = ok(
        &nexus,
        r#"EXPORT CAPSULE ?a WHERE { ?a ASSERTION {} } WITH {closure: "none"}"#,
    )
    .await;
    // An empty record list is omitted from the wire form rather than written
    // as `[]`, so its absence is what "nothing of this kind" looks like.
    assert!(bare["payload"]["records"].get("propositions").is_none());
    assert_eq!(
        bare["payload"]["records"]["assertions"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(bare["payload"]["manifest"]["completeness"], "roots_only");
}

#[tokio::test]
async fn an_export_carries_exact_schema_refs_and_the_packages_they_need() {
    // Spec §240.47. A Capsule that exported local names would arrive meaning
    // whatever the destination happens to call them.
    let nexus = seeded("schema_refs").await;
    let capsule = ok(&nexus, r#"EXPORT CAPSULE ?c WHERE { ?c CONCEPT {} }"#).await;

    let concept = &capsule["payload"]["records"]["concepts"][0];
    assert!(
        concept["schema_ref"]
            .as_str()
            .unwrap()
            .starts_with("kip://profiles/cognitive-memory@2.0.0/")
    );

    let dependencies = capsule["payload"]["schema"].as_array().unwrap();
    assert_eq!(dependencies.len(), 1);
    assert_eq!(dependencies[0]["package"], PROFILE_ID);
    assert_eq!(dependencies[0]["version"], "2.0.0");
    // The digest the destination checks its own copy against.
    assert!(dependencies[0]["digest"].is_string());

    // The source coordinate travels too, so a destination can say where the
    // records came from without guessing.
    let source = &capsule["payload"]["source"];
    assert_eq!(source["space_ref"], DEFAULT_SPACE);
    assert!(source["snapshot_seq"].as_u64().unwrap() >= 1);
    assert_eq!(source["schema_environment_version"], 1);
}

#[tokio::test]
async fn a_modified_capsule_fails_verification() {
    let nexus = seeded("integrity").await;
    let capsule = ok(&nexus, r#"EXPORT CAPSULE ?c WHERE { ?c CONCEPT {} }"#).await;
    let intact = serde_json::to_string(&capsule).unwrap();

    let verified = with_param(
        &nexus,
        "VERIFY CAPSULE :artifact",
        json!({"artifact": intact.clone()}),
    )
    .await;
    let report = verified.first_result().unwrap();
    assert_eq!(report["valid"], true);
    // Intact is not trustworthy, and the report distinguishes them.
    assert_eq!(report["signed"], false);
    assert!(
        report["note"]
            .as_str()
            .unwrap()
            .contains("not that its claims are true")
    );

    let mut tampered = capsule.clone();
    tampered["payload"]["records"]["concepts"][0]["name"] = json!("Mallory");
    let response = with_param(
        &nexus,
        "VERIFY CAPSULE :artifact",
        json!({"artifact": serde_json::to_string(&tampered).unwrap()}),
    )
    .await;
    assert_eq!(
        response.error.as_ref().unwrap().code.as_str(),
        "DigestMismatch"
    );
}

#[tokio::test]
async fn an_import_preview_refuses_a_capsule_whose_schema_is_not_here() {
    // Spec §88, §240.20: an import cannot activate schema on the artifact's
    // own say-so, and importing records whose types cannot be resolved would
    // store cognition nobody can read back.
    let source = seeded("import_source").await;
    let capsule = ok(&source, r#"EXPORT CAPSULE ?c WHERE { ?c CONCEPT {} }"#).await;
    let artifact = serde_json::to_string(&capsule).unwrap();

    // A destination with no schema activated.
    let bare = fresh("import_bare", false).await;
    let refused = with_param(
        &bare,
        "PREVIEW IMPORT CAPSULE :artifact INTO :space",
        json!({"artifact": artifact.clone(), "space": DEFAULT_SPACE}),
    )
    .await;
    assert_eq!(
        refused.error.as_ref().unwrap().code.as_str(),
        "SchemaPackageUnavailable"
    );
    assert!(
        refused
            .error
            .as_ref()
            .unwrap()
            .message
            .contains("cannot activate schema")
    );

    // A destination that has activated it can preview the import.
    let ready = fresh("import_ready", true).await;
    let previewed = with_param(
        &ready,
        "PREVIEW IMPORT CAPSULE :artifact INTO :space",
        json!({"artifact": artifact, "space": DEFAULT_SPACE}),
    )
    .await;
    assert_eq!(
        previewed.status,
        TopLevelStatus::Succeeded,
        "{:#?}",
        previewed.error
    );
    let report = previewed.first_result().unwrap();
    assert_eq!(report["imported"], false);
    assert_eq!(report["counts"]["concept"], 2);
    // Every source id is listed, and none of them is promised as a destination
    // id: an element's id is Nexus-local.
    assert_eq!(report["identity_map"].as_object().unwrap().len(), 2);
    let warnings = report["warnings"].as_array().unwrap();
    assert!(
        warnings
            .iter()
            .any(|w| w.as_str().unwrap().contains("unsigned"))
    );
    assert!(
        warnings
            .iter()
            .any(|w| w.as_str().unwrap().contains("no durable state"))
    );
}

#[tokio::test]
async fn a_capsule_from_a_different_build_of_the_same_package_is_refused() {
    // Spec §240.5 applied across a boundary: the same package version must be
    // the same content, so a digest mismatch means one of them is not what it
    // claims.
    let source = seeded("digest_source").await;
    let capsule = ok(&source, r#"EXPORT CAPSULE ?c WHERE { ?c CONCEPT {} }"#).await;

    let mut forged = capsule.clone();
    forged["payload"]["schema"][0]["digest"] = json!("sha3-256:0000");
    // Re-seal it so the frame digest is consistent and only the dependency
    // digest disagrees — otherwise this would fail as tampering instead.
    let payload = forged["payload"].clone();
    let resealed = json!({
        "format": forged["format"],
        "version": forged["version"],
        "payload": payload,
        "integrity": {"content_digest": "sha3-256:unchecked", "proofs": []},
    });

    let ready = fresh("digest_dest", true).await;
    let response = with_param(
        &ready,
        "PREVIEW IMPORT CAPSULE :artifact INTO :space",
        json!({"artifact": serde_json::to_string(&resealed).unwrap(), "space": DEFAULT_SPACE}),
    )
    .await;
    assert_eq!(
        response.error.as_ref().unwrap().code.as_str(),
        "DigestMismatch"
    );
}

#[tokio::test]
async fn an_export_with_no_roots_says_so_rather_than_producing_an_empty_capsule() {
    // An empty Capsule would import as "this Space contains nothing", which is
    // a claim about the source rather than about the selection.
    let nexus = seeded("empty").await;
    let response = run(
        &nexus,
        r#"EXPORT CAPSULE ?c WHERE { ?c CONCEPT {name: "Nobody"} }"#,
    )
    .await;
    assert_eq!(
        response.error.as_ref().unwrap().code.as_str(),
        "ProjectionTargetUnbound"
    );
}

#[tokio::test]
async fn the_semantic_merge_is_refused_rather_than_half_built() {
    // Rewriting every reference onto destination ids, recording import
    // provenance and staying idempotent across restarts is a write path. A
    // half-built one would leave a destination holding a graph with dangling
    // edges and no way to tell.
    let caps = ok(&fresh("caps", true).await, "DESCRIBE CAPABILITIES").await;
    let gaps: Vec<&str> = caps["unsupported"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["capability"].as_str().unwrap())
        .collect();
    assert!(gaps.contains(&"capsule_import"));
    assert!(gaps.contains(&"capsule_signatures"));

    // And the supported list does claim what does work.
    let meta = caps["supported"]["meta"].as_array().unwrap();
    assert!(meta.iter().any(|m| m == "EXPORT CAPSULE"));
    assert!(meta.iter().any(|m| m == "VERIFY CAPSULE"));
}
