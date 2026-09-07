use anda_cognitive_nexus::{
    CognitiveNexus,
    nexus::DEFAULT_SPACE,
    profiles::{COGNITIVE_MEMORY, COGNITIVE_MEMORY_ID, COGNITIVE_MEMORY_VERSION},
    schema::{PackageState, SchemaLock, SchemaPackage, contracts},
};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{Executor, Json, Request, TopLevelStatus};
use object_store::memory::InMemory;
use std::sync::Arc;

async fn fresh(name: &str) -> CognitiveNexus {
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: name.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let nexus = CognitiveNexus::connect(Arc::new(db)).await.unwrap();
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
    nexus
}
async fn run(nexus: &CognitiveNexus, command: &str, parameters: Json) -> anda_kip::Response {
    let mut request = Request::single(command);
    request.parameters = parameters.as_object().cloned();
    nexus
        .execute(
            anda_kip::parse_kip(command).unwrap(),
            &request,
            &request.operations[0],
        )
        .await
}
async fn ok(nexus: &CognitiveNexus, command: &str, parameters: Json) -> Json {
    let response = run(nexus, command, parameters).await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "{command}: {:?}",
        response.error
    );
    response.first_result().cloned().unwrap_or(Json::Null)
}
const SETUP: &str = r#"MUTATE {
 CREATE CONCEPT ?a { TYPE "Person" NAME "Ada" }
 CREATE CONCEPT ?p { TYPE "Preference" NAME "tea" }
 ENSURE PROPOSITION ?claim (?a, "prefers", ?p)
 CREATE EVIDENCE ?source { SET FIELDS {evidence_class: "observation", payload: "source", observed_at: "2026-09-07T00:00:00Z", content_digest: "sha256:41cf6794ba4200b839c53531555f0f3998df4cbb01a4d5cb0b94e3ca5e23947d"} }
}"#;
const BELIEF: &str = r#"FIND(?b) WHERE { ?p PROPOSITION (id: "P-1") ?b BELIEF (?p) }"#;
const DERIVE: &str = r#"MUTATE {
 CREATE ASSERTION ?a { SET FIELDS {proposition: "P-1", asserted_by: "C-1", stance: "support", mode: "inferred", confidence: 0.9, asserted_at: "2026-09-07T00:00:00Z"} SET STRUCTURAL {("evidence", "E-1") {role: "support"}} }
 CREATE ACTIVITY ?work {
  SET FIELDS {activity_class: "semantic_consolidation", status: "completed"}
  SET FACET "DependencyBasis" {basis_seq: :seq, groups: [{role: "all_of", pins: [{id: "E-1", version: :version}]}], policy_basis: :basis}
  SET STRUCTURAL {("inputs", "E-1") ("outputs", ?a)}
 }
}"#;

#[tokio::test]
async fn derived_belief_checks_real_pins_before_maintenance() {
    let nexus = fresh("dependency_contract").await;
    ok(&nexus, SETUP, Json::Null).await;
    let capsule = ok(
        &nexus,
        "EXPORT CAPSULE ?c WHERE { ?c CONCEPT {name: \"Ada\"} }",
        Json::Null,
    )
    .await;
    contracts::validate_value(
        &serde_json::json!({"$ref":"urn:kip:2.0:schema:capsule"}),
        &capsule,
    )
    .unwrap();
    let basis = ok(&nexus, BELIEF, Json::Null).await[0]["basis"].clone();
    contracts::validate_value(
        &serde_json::json!({"$ref":"urn:kip:2.0:schema:projection#/$defs/ProjectionBasis"}),
        &basis,
    )
    .unwrap();
    let parameters = serde_json::json!({"seq":basis["snapshot_seq"], "basis":basis, "version":1});
    ok(&nexus, DERIVE, parameters).await;
    let full_capsule = ok(
        &nexus,
        "EXPORT CAPSULE ?a WHERE { ?a ASSERTION {} }",
        Json::Null,
    )
    .await;
    contracts::validate_value(
        &serde_json::json!({"$ref":"urn:kip:2.0:schema:capsule"}),
        &full_capsule,
    )
    .unwrap();
    let belief = ok(&nexus, BELIEF, Json::Null).await;
    assert_eq!(belief[0]["status"], "accepted");
    let activity = ok(
        &nexus,
        "FIND(?x._system.input_versions, ?x._system.output_versions) WHERE { ?x ACTIVITY {} }",
        Json::Null,
    )
    .await;
    assert_eq!(activity[0][0]["E-1"], 1);
    assert_eq!(activity[0][1]["A-1"], 1);
    ok(&nexus, "CREATE EVIDENCE ?replacement { SET FIELDS {evidence_class: \"observation\", payload: \"corrected\"} }", Json::Null).await;
    ok(
        &nexus,
        "TRANSITION \"E-1\" TO \"corrected\" BY \"E-2\"",
        Json::Null,
    )
    .await;
    let validity = ok(
        &nexus,
        "FIND(?a._system.dependency_validity) WHERE { ?a ASSERTION {} }",
        Json::Null,
    )
    .await;
    assert_eq!(validity[0]["status"], "needs_review");
    assert_eq!(validity[0]["action_eligible"], false);
    assert_ne!(
        ok(&nexus, BELIEF, Json::Null).await[0]["status"],
        "accepted"
    );
}

#[tokio::test]
async fn false_read_pins_are_refused_without_partial_writes() {
    let nexus = fresh("bad_dependency_pin").await;
    ok(&nexus, SETUP, Json::Null).await;
    let basis = ok(&nexus, BELIEF, Json::Null).await[0]["basis"].clone();
    let response = run(
        &nexus,
        DERIVE,
        serde_json::json!({"seq":basis["snapshot_seq"], "basis":basis, "version":99}),
    )
    .await;
    assert_eq!(response.status, TopLevelStatus::Failed);
    assert_eq!(
        ok(&nexus, "FIND(?a) WHERE { ?a ASSERTION {} }", Json::Null).await,
        serde_json::json!([])
    );
}

#[test]
fn activation_verifies_schema_resource_closure_even_after_a_warm_cache() {
    let package = SchemaPackage::parse(COGNITIVE_MEMORY).unwrap();
    contracts::verify_artifact(&package.artifact().unwrap()).unwrap();
    contracts::validate_package(&package).unwrap();
    let mut value: Json = serde_json::from_str(COGNITIVE_MEMORY).unwrap();
    value["manifest"]["validation_schemas"]
        .as_array_mut()
        .unwrap()
        .retain(|p| !p["id"].as_str().unwrap().starts_with("https:"));
    let incomplete = SchemaPackage::parse(&value.to_string()).unwrap();
    assert!(contracts::validate_package(&incomplete).is_err());
    value["manifest"]["validation_schemas"][0]["content_digest"] =
        Json::String(format!("sha256:{}", "0".repeat(64)));
    assert_eq!(
        contracts::validate_package(&SchemaPackage::parse(&value.to_string()).unwrap())
            .unwrap_err()
            .name(),
        "DigestMismatch"
    );
}

#[tokio::test]
async fn an_unchanged_any_of_alternative_keeps_a_derivation_current() {
    let nexus = fresh("any_of_contract").await;
    ok(&nexus, SETUP, Json::Null).await;
    ok(&nexus, "CREATE EVIDENCE ?second { SET FIELDS {evidence_class: \"observation\", payload: \"independent\"} }", Json::Null).await;
    let basis = ok(&nexus, BELIEF, Json::Null).await[0]["basis"].clone();
    let parameters = serde_json::json!({"seq":basis["snapshot_seq"], "basis":basis});
    ok(&nexus, r#"MUTATE {
        CREATE CONCEPT ?summary {TYPE "Insight" SET ATTRIBUTES {summary: "supported by either independent source"}}
        CREATE ACTIVITY ?work {
            SET FIELDS {activity_class: "semantic_consolidation", status: "completed"}
            SET FACET "DependencyBasis" {basis_seq: :seq, policy_basis: :basis, groups: [{role: "any_of", pins: [{id:"E-1",version:1},{id:"E-2",version:1}]}]}
            SET STRUCTURAL {("inputs", "E-1") ("inputs", "E-2") ("outputs", ?summary)}
        }
    }"#, parameters).await;
    ok(&nexus, "CREATE EVIDENCE ?replacement { SET FIELDS {evidence_class: \"observation\", payload: \"correction\"} }", Json::Null).await;
    ok(
        &nexus,
        "TRANSITION \"E-1\" TO \"corrected\" BY \"E-3\"",
        Json::Null,
    )
    .await;
    let status = ok(
        &nexus,
        r#"FIND(?c._system.dependency_validity.status) WHERE {?c CONCEPT {type:"Insight"}}"#,
        Json::Null,
    )
    .await;
    assert_eq!(status, serde_json::json!(["current"]));
}

#[tokio::test]
async fn terminal_audits_cannot_acquire_a_retrospective_read_contract() {
    let nexus = fresh("terminal_dependency").await;
    ok(&nexus, SETUP, Json::Null).await;
    ok(&nexus, r#"CREATE ACTIVITY ?audit {SET FIELDS {activity_class:"semantic_consolidation",status:"completed"} SET STRUCTURAL {("inputs","E-1") ("outputs","C-1")}}"#, Json::Null).await;
    let basis = ok(&nexus, BELIEF, Json::Null).await[0]["basis"].clone();
    let response = run(&nexus, r#"UPDATE "X-1" SET FACET "DependencyBasis" {basis_seq: :seq, policy_basis: :basis, groups: [{role:"all_of",pins:[{id:"E-1",version:1}]}]}"#, serde_json::json!({"seq":basis["snapshot_seq"],"basis":basis})).await;
    assert_eq!(response.error.unwrap().code.as_str(), "ImmutableField");
}
