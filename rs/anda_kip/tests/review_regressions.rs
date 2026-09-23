//! Regressions from the SDK contract review: external wire values, admission
//! parity and transaction recovery metadata, rather than implementation details.
#[path = "support/json_schema.rs"]
mod json_schema;

use anda_kip::*;
use serde_json::json;

fn system() -> Json {
    json!({"version":1,"state":"active","origin":{"principal_id":"p","channel":"host"}})
}

fn capsule_wire() -> Json {
    json!({
        "format":CAPSULE_FORMAT,"format_version":CAPSULE_VERSION,
        "payload":{
            "manifest":{"kind":"delta","closure":"selective","roots":[],"base_seq":0,"target_seq":1},
            "source":{"space_id":"s","snapshot_seq":1},"schema_dependencies":[],
            "records":[
                {"id":"a","kind":"activity","space_id":"s","_system":system(),"activity_class":"tool_execution","status":"completed","inputs":[],"outputs":[]},
                {"id":"c","kind":"concept","space_id":"s","_system":system(),"schema_ref":"kip://example@1.0.0/T"}
            ],
            "external_refs":[],"blobs":{},"handling":{"requirements":[],"source_classification":null,"vendor/note":null},
            "changes":[{"space_id":"s","space_seq":1,"tx_id":"tx1","committed_at":"2026-09-01T00:00:00.000Z","changes":[]}]
        },
        "integrity":{"digest_profile":"kip-jcs-safe-v1","content_digest":format!("sha256:{}","0".repeat(64)),"covers":"artifact","signatures":[{"suite":"custom","vendor/key":"proof"}]}
    })
}

#[test]
fn external_capsules_preserve_digest_inputs_and_delta_changes() {
    let schema = json_schema::Schema::new(CAPSULE_SCHEMA);
    let original = capsule_wire();
    schema.assert_valid("external capsule", &original);
    let capsule: Capsule = serde_json::from_value(original.clone()).unwrap();
    capsule.validate_frame().unwrap();
    assert_eq!(
        capsule
            .payload
            .records
            .by_kind(ElementKind::Concept)
            .count(),
        1
    );
    assert_eq!(capsule.payload.records.0[0]["id"], "a");
    let encoded = serde_json::to_value(&capsule).unwrap();
    assert_eq!(encoded, original);
    let mut covered = original;
    covered.as_object_mut().unwrap().remove("integrity");
    assert_eq!(capsule.canonical_payload(), canonical_json(&covered));
}

#[test]
fn capsule_changes_preserve_absence_and_empty_arrays() {
    for changes in [None, Some(json!([]))] {
        let mut wire = capsule_wire();
        if let Some(changes) = changes {
            wire["payload"]["changes"] = changes;
        } else {
            wire["payload"].as_object_mut().unwrap().remove("changes");
        }
        let decoded: Capsule = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), wire);
    }
}

#[test]
fn portable_core_elements_survive_typed_round_trips() {
    let schema = json_schema::Schema::new(ELEMENT_SCHEMA);
    let concept = json!({"id":"c","kind":"concept","space_id":"s","_system":system(),
        "schema_ref":"kip://example@1.0.0/T","structural":{"has_step":[{"id":"step"}]},"merged_into":{"id":"canonical"}});
    schema.assert_valid("Concept", &concept);
    let decoded: Concept = serde_json::from_value(concept.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), concept);
    let assertion = json!({"id":"a","kind":"assertion","space_id":"s","_system":system(),
        "proposition":{"id":"p"},"asserted_by":{"id":"actor"},"stance":"support","mode":"stated","asserted_at":"2026-09-01T00:00:00.000Z",
        "lifecycle":{"status":"superseded","supersedes":[{"id":"previous"}],"superseded_by":[{"id":"next"}]}});
    schema.assert_valid("Assertion", &assertion);
    let decoded: Assertion = serde_json::from_value(assertion.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), assertion);
    for payload in [
        json!({"status":"purged"}),
        json!({"mode":"inline","inline":null}),
        json!({"mode":"external","content_ref":"blob:1"}),
    ] {
        let evidence = json!({"id":"e","kind":"evidence","space_id":"s","_system":system(),
            "evidence_class":"tool_result","payload":payload,"content_digest":format!("sha256:{}","0".repeat(64)),"observed_at":"2026-09-01T00:00:00.000Z",
            "lifecycle":{"status":"corrected","corrects":[{"id":"old"}],"corrected_by":[{"id":"new"}]}});
        schema.assert_valid("Evidence", &evidence);
        let decoded: Evidence = serde_json::from_value(evidence.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), evidence);
    }
}

#[test]
fn core_registries_do_not_constrain_package_defined_members() {
    for text in [
        r#"CREATE CONCEPT ?c { TYPE "Settings" SET ATTRIBUTES {mode:"dark",stance:"wide",confidence:42} }"#,
        r#"UPDATE :c SET FACET "RenderOptions" {mode:"dark",confidence:42}"#,
        r#"CREATE CONCEPT ?c {SET STRUCTURAL {("evidence", :e) {role:"custom"}}}"#,
    ] {
        parse_kip(text).unwrap_or_else(|e| panic!("{text}: {e}"));
    }
    assert!(parse_kip(r#"CREATE ASSERTION ?a {SET FIELDS {mode:"dark"}}"#).is_err());
    assert!(
        parse_kip(r#"CREATE ASSERTION ?a {SET STRUCTURAL {("evidence", :e) {role:"custom"}}}"#)
            .is_err()
    );
}

#[test]
fn mutation_references_respect_proposition_endpoints_and_not_scope() {
    for text in [
        r#"ENSURE PROPOSITION ?p (?missing, "prefers", :value)"#,
        r#"ASSERT (?missing, "prefers", :value) {by: :me, mode:"stated"}"#,
        r#"ENSURE PROPOSITION ?p (:s, "p", (:nested,"q",?missing))"#,
        r#"UPDATE ?x SET ATTRIBUTES {score:1} WHERE {NOT {?x {}}}"#,
    ] {
        assert_eq!(
            parse_kip(text).unwrap_err().code,
            KipErrorCode::ReferenceError,
            "{text}"
        );
    }
    for text in [
        r#"MUTATE {ENSURE PROPOSITION ?p (?later,"p",:value) CREATE CONCEPT ?later {}}"#,
        r#"UPDATE ?x SET ATTRIBUTES {score:1} WHERE {?x {} NOT {?x {name:"hidden"}}}"#,
        r#"UPDATE ?x SET ATTRIBUTES {score:1} WHERE {NOT {?x {name:"hidden"}} ?x {}}"#,
    ] {
        assert!(parse_kip(text).is_ok(), "{text}");
    }
}

fn ast_operation(command: Command) -> Operation {
    Operation {
        ast: Some(command),
        ..Default::default()
    }
}

#[test]
fn ast_admission_rechecks_query_and_meta_shapes() {
    let snapshot = Command::Meta(MetaCommand::Describe(DescribeTarget::Snapshot {
        as_of: Some(AsOf::Seq(Scalar::Literal(1_u64.into()))),
        at_time: Some(Scalar::Literal("2026-09-01T00:00:00.000Z".into())),
    }));
    assert!(ast_operation(snapshot).parse().is_err());
    assert!(
        parse_kip(r#"DESCRIBE SNAPSHOT AS OF SEQ 1 AT TIME "2026-09-01T00:00:00.000Z""#).is_err()
    );
    let list = Command::Meta(MetaCommand::List(ListCommand {
        target: ListTarget::Dependents,
        status: None,
        element: None,
        depth: None,
        limit: None,
        cursor: None,
    }));
    assert!(ast_operation(list).parse().is_err());
    assert!(parse_kip("LIST DEPENDENTS").is_err());
    let mut q = parse_kql(r#"FIND(?p) WHERE {?p (:s,"p",:o)}"#).unwrap();
    if let WhereClause::Proposition {
        matcher: PropositionMatcher::Tuple(triple),
        ..
    } = &mut q.where_clauses[0]
    {
        triple.subject = Term::Literal("literal subject".into());
    }
    assert!(ast_operation(Command::Kql(q)).parse().is_err());
    assert!(parse_kip(r#"FIND(?p) WHERE {?p ("literal subject","p",:o)}"#).is_err());
    for text in [
        "DESCRIBE SNAPSHOT AS OF SEQ 1",
        "LIST DEPENDENTS :root",
        r#"FIND(?p) WHERE {?p (:s,"p"{1,3},:o)}"#,
    ] {
        let command = parse_kip(text).unwrap();
        assert_eq!(ast_operation(command.clone()).parse().unwrap(), command);
    }
}

#[test]
fn ast_rejects_duplicate_bound_keys_and_invalid_hop_ranges() {
    let mut command = parse_kip(r#"UPDATE :c SET ATTRIBUTES {settings:{theme: :theme}}"#).unwrap();
    let Command::Kml(plan) = &mut command else {
        panic!("KML")
    };
    let MutationClause::Update(update) = &mut plan.clauses[0] else {
        panic!("UPDATE")
    };
    let UpdateAction::SetAttributes(fields) = &mut update.actions[0] else {
        panic!("attributes")
    };
    let MutationValue::Object(entries) = &mut fields[0].1 else {
        panic!("bound object")
    };
    entries.push(entries[0].clone());
    assert!(ast_operation(command).parse().is_err());
    let mut command = parse_kip(r#"FIND(?p) WHERE {?p (:s,"p"{1,3},:o)}"#).unwrap();
    let Command::Kql(q) = &mut command else {
        panic!("KQL")
    };
    let WhereClause::Proposition {
        matcher: PropositionMatcher::Tuple(triple),
        ..
    } = &mut q.where_clauses[0]
    else {
        panic!("tuple")
    };
    let PredTerm::Path(atoms) = &mut triple.predicate else {
        panic!("path")
    };
    atoms[0].hops = Some(HopRange {
        min: 3,
        max: Some(1),
    });
    assert!(ast_operation(command).parse().is_err());
}

#[test]
fn null_payloads_and_results_are_present_values() {
    let wire = json!({"kip":"2.0","operations":[{"command":"CREATE CONCEPT ?c {}"}],
        "ingest":{"evidence":[{"key":"observation","evidence_class":"tool_result","payload":null}]}});
    json_schema::Schema::new(include_str!("../schemas/kip-request.schema.json"))
        .assert_valid("null ingest", &wire);
    let request = Request::from_json(&wire.to_string()).unwrap();
    assert_eq!(
        request.ingest.as_ref().unwrap().evidence[0].payload,
        Some(Json::Null)
    );
    assert_eq!(serde_json::to_value(&request).unwrap(), wire);
    request.parse_operations().unwrap();
    let mut missing = wire;
    missing["ingest"]["evidence"][0]
        .as_object_mut()
        .unwrap()
        .remove("payload");
    assert!(Request::from_value(missing).is_err());
    let response = Response::ok(Json::Null);
    let decoded: Response =
        serde_json::from_value(serde_json::to_value(&response).unwrap()).unwrap();
    assert_eq!(decoded, response);
    assert_eq!(decoded.first_result(), Some(&Json::Null));
}

#[test]
fn extension_values_are_checked_in_each_request_block() {
    for pointer in [
        "/extensions",
        "/execution/extensions",
        "/read/extensions",
        "/preconditions/extensions",
        "/context/extensions",
        "/options/extensions",
        "/operations/0/extensions",
        "/operations/0/options/extensions",
        "/ingest/extensions",
        "/ingest/evidence/0/extensions",
    ] {
        let mut request = json!({"kip":"2.0","operations":[{"command":"CREATE CONCEPT ?c {}","extensions":{},"options":{"extensions":{}}}],
            "execution":{"mode":"independent","extensions":{}},"read":{"extensions":{}},"preconditions":{"extensions":{}},
            "context":{"extensions":{}},"options":{"extensions":{}},"extensions":{},
            "ingest":{"extensions":{},"evidence":[{"key":"source","evidence_class":"document","payload":"x","extensions":{}}]}});
        for value in [
            json!(true),
            json!({"critical":"true"}),
            json!({"critical":null}),
        ] {
            *request.pointer_mut(pointer).unwrap() = json!({"vendor/test":value});
            assert!(Request::from_value(request.clone()).is_err(), "{pointer}");
        }
        *request.pointer_mut(pointer).unwrap() = json!({"vendor/test":{"critical":true}});
        let decoded = Request::from_value(request).unwrap();
        assert_eq!(decoded.critical_extensions(), ["vendor/test"]);
    }
}

#[test]
fn direct_request_validation_still_rejects_nonportable_numbers() {
    let unsafe_number = json!(MAX_SAFE_INTEGER + 1);
    let mut request = Request::single("DESCRIBE PROTOCOL");
    request.parameters = Some(Map::from_iter([("n".into(), unsafe_number.clone())]));
    assert!(request.validate().is_err());
    request.parameters = None;
    request.options = Some(RequestOptions {
        deadline_ms: Some(MAX_SAFE_INTEGER + 1),
        ..Default::default()
    });
    assert!(request.validate().is_err());
    request.options = None;
    request.operations[0].parameters = Some(Map::from_iter([("n".into(), unsafe_number)]));
    assert!(request.validate().is_err());
}

struct Commits;
#[async_trait::async_trait]
impl Executor for Commits {
    async fn execute(&self, _: Command, _: &Request, op: &Operation) -> Response {
        if op.op_id.as_deref() == Some("unknown") {
            return Response::outcome_unknown(KipError::outcome_unknown("response lost"));
        }
        if op.op_id.as_deref() == Some("failed") {
            return Response::failed(KipError::constraint_violation("failed"));
        }
        Response {
            receipt: Some(
                serde_json::from_value(json!({"status":"committed","tx_id":op.op_id})).unwrap(),
            ),
            ..Response::ok(json!(true))
        }
    }
}

#[tokio::test]
async fn every_independent_commit_keeps_its_own_receipt() {
    for mode in [ExecutionMode::Independent, ExecutionMode::Sequence] {
        for end in ["tx2", "failed", "unknown"] {
            let request = Request {
                execution: Some(Execution::new(mode)),
                operations: ["tx1", end, "tx3"]
                    .into_iter()
                    .map(|id| Operation::new("CREATE CONCEPT ?c {}").with_op_id(id))
                    .collect(),
                ..Default::default()
            };
            let result = execute_request(&Commits, &request).await;
            assert!(result.receipt.is_none());
            assert_eq!(
                result.results[0].receipt.as_ref().unwrap().tx_id.as_deref(),
                Some("tx1")
            );
            if end == "tx2" {
                assert_eq!(
                    result.results[1].receipt.as_ref().unwrap().tx_id.as_deref(),
                    Some("tx2")
                );
            } else {
                assert!(result.results[1].receipt.is_none());
            }
            if mode == ExecutionMode::Sequence && end != "tx2" {
                assert_eq!(result.results[2].status, OperationStatus::Skipped);
            } else {
                assert_eq!(
                    result.results[2].receipt.as_ref().unwrap().tx_id.as_deref(),
                    Some("tx3")
                );
            }
            if end == "unknown" {
                assert_eq!(result.status, TopLevelStatus::OutcomeUnknown);
            }
        }
    }
}
