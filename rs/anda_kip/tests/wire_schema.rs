//! Conformance of the Rust envelope types against the vendored wire schemas.
//!
//! `schemas/kip-request.schema.json` and `schemas/kip-response.schema.json` are
//! the normative description of what KIP 2.0 puts on the wire. Nothing checked
//! the Rust types against them, and three divergences had accumulated: a field
//! the schema defines with no Rust counterpart, and two `Default` values that
//! serialize to shapes the schema forbids.
//!
//! Two directions are checked, because they fail differently:
//!
//! - **Rust → wire**: what this crate emits must satisfy the schema. A type
//!   whose every field is optional can serialize to `{}` while the schema
//!   requires a member, which is invisible until something validates it.
//! - **wire → Rust → wire**: a payload using every field the schema defines
//!   must survive the round trip. A field missing from a Rust struct is
//!   silently dropped by serde, so only a field-by-field comparison finds it.

#[path = "support/json_schema.rs"]
mod json_schema;

use anda_kip::{
    ChangeEnvelope, ElementReference, Execution, ExecutionMode, IngestContext, IngestEvidence,
    OnError, Operation, OperationResult, OperationStatus, PolicyIdentity, Preconditions,
    ReadBinding, Receipt, ReceiptStatus, Request, RequestContext, RequestOptions, Response,
    ResponseContext, ResponseExecution, ResultContext, SearchContext, SearchMode, SnapshotContext,
    SpaceSelector, TopLevelStatus, Warning,
};
use json_schema::Schema;
use serde_json::{Value, json};

fn request_schema() -> Schema {
    Schema::new(include_str!("../schemas/kip-request.schema.json"))
}

fn response_schema() -> Schema {
    Schema::new(include_str!("../schemas/kip-response.schema.json"))
}

fn change_envelope_schema() -> Schema {
    Schema::new(include_str!("../schemas/kip-change-envelope.schema.json"))
}

fn encode<T: serde::Serialize>(value: &T) -> Value {
    serde_json::to_value(value).expect("serializes")
}

// ---------------------------------------------------------------------------
// Rust → wire
// ---------------------------------------------------------------------------

#[test]
fn every_request_this_crate_builds_matches_the_wire_schema() {
    let schema = request_schema();

    schema.assert_valid(
        "Request::single",
        &encode(&Request::single("DESCRIBE PROTOCOL")),
    );

    // The first operation is KML because the envelope carries an `ingest`
    // block, and §71.1 mints that Evidence inside the request's transaction:
    // an all-read request has no transaction to mint into.
    let mut batch = Request::single(
        "ASSERT (:alice, \"prefers\", :dark_mode) { by: :alice, mode: \"stated\", evidence: :msg }",
    );
    batch
        .operations
        .push(Operation::new("DESCRIBE PRIMER").with_op_id("primer"));
    batch.execution = Some(Execution {
        mode: ExecutionMode::Sequence,
        on_error: Some(OnError::Stop),
        isolation: Some("serializable".into()),
        idempotency_key: Some("intent-1".into()),
        extensions: None,
    });
    batch.request_id = Some("req-1".into());
    batch.space = Some(SpaceSelector {
        id: Some("space-1".into()),
        uri: Some("kip://space-1".into()),
    });
    batch.compatibility_profile = Some("kip-1-compat".into());
    batch.read = Some(ReadBinding {
        snapshot_token: Some("snap-token".into()),
        extensions: None,
    });
    batch.preconditions = Some(Preconditions {
        space_seq: Some(10),
        schema_environment_version: Some(3),
        extensions: None,
    });
    batch.parameters = Some(
        json!({ "limit": 10 })
            .as_object()
            .expect("object")
            .to_owned(),
    );
    batch.context = Some(RequestContext {
        purpose: Some("answer_user".into()),
        risk: Some("low".into()),
        locale: Some("en".into()),
        client: Some("anda".into()),
        extensions: None,
    });
    batch.requires = Some(
        json!({ "belief_slot": true })
            .as_object()
            .expect("object")
            .to_owned(),
    );
    batch.options = Some(RequestOptions {
        dry_run: Some(true),
        deadline_ms: Some(5_000),
        extensions: None,
    });
    batch.ingest = Some(IngestContext {
        evidence: vec![IngestEvidence {
            key: "msg".into(),
            evidence_class: "user_statement".into(),
            payload: Some(json!({ "text": "hi" })),
            payload_artifact: None,
            media_type: Some("application/json".into()),
            observed_at: Some("2026-01-01T00:00:00Z".into()),
            source_actor: Some(ElementReference::by_id("C-alice")),
            facets: Default::default(),
            client_key: Some("msg-1".into()),
            extensions: None,
        }],
        extensions: None,
    });

    batch.validate().expect("the envelope is valid");
    schema.assert_valid("a fully populated Request", &encode(&batch));
}

#[test]
fn every_response_this_crate_builds_matches_the_wire_schema() {
    let schema = response_schema();

    schema.assert_valid("Response::default", &encode(&Response::default()));
    schema.assert_valid(
        "Response::ok",
        &encode(&Response::ok(json!({ "rows": [] }))),
    );
    schema.assert_valid(
        "Response::failed",
        &encode(&Response::failed(anda_kip::KipError::invalid_syntax("no"))),
    );
    schema.assert_valid(
        "Response::outcome_unknown",
        &encode(&Response::outcome_unknown(
            anda_kip::KipError::outcome_unknown("unknown"),
        )),
    );
    schema.assert_valid(
        "a partial Response",
        &encode(&Response::from_results(vec![
            OperationResult::ok(json!(1)),
            OperationResult::failed(anda_kip::KipError::invalid_syntax("no")),
        ])),
    );
    schema.assert_valid(
        "a no-effect Response",
        &encode(&Response::from_results(vec![OperationResult::no_effect()])),
    );
    schema.assert_valid(
        "a rolled-back Response",
        &encode(&Response::from_results(vec![
            OperationResult::rolled_back(),
            OperationResult::ok(json!(1)),
        ])),
    );
}

/// The minimal construction of every context type must still satisfy the
/// schema.
///
/// `SnapshotContext.snapshot_seq` and `PolicyIdentity.id` are required members,
/// so both are non-`Option` fields rather than optional ones — an
/// all-optional struct serializes to `{}`, which the schema forbids in both
/// places. This pins that: making either field optional again fails here.
#[test]
fn minimally_constructed_contexts_still_match_the_schema() {
    let schema = response_schema();

    let response = Response {
        snapshot: Some(SnapshotContext::at(1500)),
        results: vec![OperationResult {
            context: Some(ResultContext {
                epistemic_policy: Some(PolicyIdentity::new("policy-1")),
                ..Default::default()
            }),
            ..OperationResult::ok(json!(1))
        }],
        ..Default::default()
    };

    schema.assert_valid("a Response carrying minimal contexts", &encode(&response));
}

/// `FOR TIME` and `AS OF` are independent axes (Spec §48.3), so a result must
/// be able to report the world valid-time it was evaluated for alongside the
/// cognitive history it was read from.
#[test]
fn a_result_context_can_report_its_world_valid_time() {
    let context = ResultContext {
        snapshot_seq: Some(1500),
        valid_at: Some("2026-01-01T00:00:00Z".into()),
        ..Default::default()
    };
    assert_eq!(
        encode(&context),
        json!({ "snapshot_seq": 1500, "valid_at": "2026-01-01T00:00:00Z" })
    );
    response_schema().assert_valid(
        "a ResultContext reporting valid_at",
        &encode(&Response {
            results: vec![OperationResult {
                context: Some(context),
                ..OperationResult::ok(json!(1))
            }],
            ..Default::default()
        }),
    );
}

// ---------------------------------------------------------------------------
// wire → Rust → wire
// ---------------------------------------------------------------------------

/// A response using every field the schema defines, so a Rust struct missing
/// one shows up as a dropped field rather than as nothing at all.
fn exhaustive_response() -> Value {
    json!({
      "kip": "2.0",
      "request_id": "req-1",
      "status": "succeeded",
      "execution": {
        "mode": "sequence",
        "on_error": "stop",
        "isolation": "serializable",
        "idempotency_key": "intent-1",
        "extensions": { "vendor/trace": { "critical": false, "id": "t-1" } }
      },
      "results": [
        {
          "op_id": "op-1",
          "status": "succeeded",
          "result": { "rows": [1, 2] },
          "receipt": {
            "tx_id": "tx-899",
            "space_id": "space-1",
            "snapshot_seq": 1498,
            "space_seq": 1499,
            "committed_at": "2026-01-01T00:00:00Z",
            "status": "committed",
            "transaction_class": "cognitive",
            "schema_environment_version": 3
          },
          "context": {
            "space_id": "space-1",
            "snapshot_seq": 1500,
            "schema_environment_version": 3,
            "epistemic_policy": { "id": "policy-1", "version": "2.0" },
            "valid_at": "2026-01-01T00:00:00Z",
            "search": {
              "index_seq": 1490,
              "current_space_seq": 1500,
              "consistency": "eventual",
              "mode": "hybrid",
              "score_semantics": "implementation_specific",
              "extensions": { "vendor/search": { "critical": false } }
            },
            "cursor": "cursor-in",
            "extensions": { "vendor/ctx": { "critical": false } }
          },
          "warnings": [
            "a bare caveat",
            {
              "code": "SearchIndexLag",
              "message": "the index lags the snapshot",
              "details": { "lag": 10 },
              "extensions": { "vendor/warn": { "critical": false } }
            }
          ],
          "next_cursor": "cursor-out",
          "extensions": { "vendor/result": { "critical": false } }
        }
      ],
      "context": {
        "space_id": "space-1",
        "schema_environment_version": 3,
        "compatibility_profile_used": "kip-1-compat",
        "extensions": { "vendor/resp": { "critical": false } }
      },
      "snapshot": {
        "space_id": "space-1",
        "snapshot_seq": 1500,
        "snapshot_token": "snap-token",
        "schema_environment_version": 3,
        "extensions": { "vendor/snap": { "critical": false } }
      },
      "receipt": {
        "tx_id": "tx-900",
        "space_id": "space-1",
        "snapshot_seq": 1499,
        "space_seq": 1500,
        "committed_at": "2026-01-01T00:00:01Z",
        "status": "committed",
        "transaction_class": "cognitive",
        "request_digest": "sha256:aa",
        "semantic_plan_digest": "sha256:bb",
        "result_digest": "sha256:cc",
        "schema_environment_version": 3,
        "change_summary": { "created": 2 },
        "proofs": [{ "type": "signature" }],
        "receipt_digest": "sha256:dd",
        "origin": {
          "principal_id": "principal-1",
          "actor_binding_id": "binding-1",
          "delegation_digest": "sha256:ee"
        },
        "extensions": { "vendor/receipt": { "critical": false } }
      },
      "warnings": ["a request-level caveat"],
      "next_cursor": "request-cursor",
      "extensions": { "vendor/env": { "critical": false } }
    })
}

/// A request using every field the schema defines.
fn exhaustive_request() -> Value {
    json!({
      "kip": "2.0",
      "request_id": "req-1",
      "space": { "id": "space-1", "uri": "kip://space-1" },
      "compatibility_profile": "kip-1-compat",
      "execution": {
        "mode": "atomic",
        "on_error": "stop",
        "isolation": "serializable",
        "idempotency_key": "intent-1",
        "extensions": { "vendor/exec": { "critical": false } }
      },
      "read": {
        "snapshot_token": "snap-token",
        "extensions": { "vendor/read": { "critical": false } }
      },
      "ingest": {
        "evidence": [
          {
            "key": "msg",
            "evidence_class": "user_statement",
            "payload": { "text": "hi" },
            "media_type": "application/json",
            "observed_at": "2026-01-01T00:00:00Z",
            "source_actor": { "id": "C-alice" },
            "client_key": "msg-1",
            "facets": { "OutcomeRecord": { "task_family": "deploy/rollback", "outcome_status": "success" } },
            "extensions": { "vendor/ingest": { "critical": false } }
          }
        ],
        "extensions": { "vendor/ingestctx": { "critical": false } }
      },
      "preconditions": {
        "space_seq": 1499,
        "schema_environment_version": 3,
        "extensions": { "vendor/pre": { "critical": false } }
      },
      "operations": [
        {
          "op_id": "op-1",
          "language": "KQL",
          "command": "FIND(?x) WHERE { ?x {type: \"T\"} }",
          "parameters": { "limit": 10 },
          "idempotency_key": "op-intent-1",
          "options": { "extensions": { "vendor/opopt": { "critical": false } } },
          "extensions": { "vendor/op": { "critical": false } }
        },
        {
          "op_id": "op-2",
          "command": "DESCRIBE PRIMER"
        }
      ],
      "parameters": { "shared": true },
      "context": {
        "purpose": "answer_user",
        "risk": "low",
        "locale": "en",
        "client": "anda",
        "extensions": { "vendor/reqctx": { "critical": false } }
      },
      "requires": { "belief_slot": true },
      "options": {
        "dry_run": true,
        "deadline_ms": 5000,
        "extensions": { "vendor/opts": { "critical": false } }
      },
      "extensions": { "vendor/reqenv": { "critical": false } }
    })
}

/// Collects the JSON pointers present in `expected` but absent from `actual`.
fn dropped_fields(expected: &Value, actual: &Value, path: &str, out: &mut Vec<String>) {
    match (expected, actual) {
        (Value::Object(want), Value::Object(got)) => {
            for (key, value) in want {
                match got.get(key) {
                    Some(seen) => dropped_fields(value, seen, &format!("{path}/{key}"), out),
                    None => out.push(format!("{path}/{key}")),
                }
            }
        }
        (Value::Array(want), Value::Array(got)) => {
            for (index, value) in want.iter().enumerate() {
                match got.get(index) {
                    Some(seen) => dropped_fields(value, seen, &format!("{path}/{index}"), out),
                    None => out.push(format!("{path}/{index}")),
                }
            }
        }
        (want, got) if want != got => {
            out.push(format!("{path} (changed: {want} → {got})"));
        }
        _ => {}
    }
}

#[test]
fn the_exhaustive_wire_payloads_are_themselves_schema_valid() {
    // The round-trip tests below are only meaningful if their inputs are
    // legal to begin with; a typo in the fixture would otherwise look like a
    // Rust-side bug.
    request_schema().assert_valid("the exhaustive request fixture", &exhaustive_request());
    response_schema().assert_valid("the exhaustive response fixture", &exhaustive_response());
}

#[test]
fn a_request_using_every_schema_field_survives_the_rust_types() {
    let wire = exhaustive_request();
    let decoded: Request = serde_json::from_value(wire.clone()).expect("decodes into Request");
    let reencoded = encode(&decoded);

    let mut dropped = Vec::new();
    dropped_fields(&wire, &reencoded, "", &mut dropped);
    assert!(
        dropped.is_empty(),
        "the Rust request types drop wire fields the schema defines:\n  {}",
        dropped.join("\n  ")
    );
}

#[test]
fn a_response_using_every_schema_field_survives_the_rust_types() {
    let wire = exhaustive_response();
    let decoded: Response = serde_json::from_value(wire.clone()).expect("decodes into Response");
    let reencoded = encode(&decoded);

    let mut dropped = Vec::new();
    dropped_fields(&wire, &reencoded, "", &mut dropped);
    assert!(
        dropped.is_empty(),
        "the Rust response types drop wire fields the schema defines:\n  {}",
        dropped.join("\n  ")
    );
}

// ---------------------------------------------------------------------------
// The schema's own cross-field rules
// ---------------------------------------------------------------------------

#[test]
fn the_schema_rejects_what_the_envelope_validator_rejects() {
    let schema = request_schema();

    // A multi-operation request without `execution` (Spec §75.4).
    let mut request = Request::single("DESCRIBE PROTOCOL");
    request.operations.push(Operation::new("DESCRIBE PRIMER"));
    assert!(request.validate().is_err(), "the validator rejects it");
    assert!(
        !schema.validate(&encode(&request)).is_empty(),
        "and so does the schema"
    );

    // Atomic execution cannot continue past an error.
    let mut atomic = Request::single(r#"TRANSITION :x TO "tombstoned""#);
    atomic.execution = Some(Execution {
        mode: ExecutionMode::Atomic,
        on_error: Some(OnError::Continue),
        isolation: None,
        idempotency_key: None,
        extensions: None,
    });
    assert!(atomic.validate().is_err(), "the validator rejects it");
    assert!(
        !schema.validate(&encode(&atomic)).is_empty(),
        "and so does the schema"
    );
}

#[test]
fn a_succeeded_response_carries_no_top_level_error() {
    // The schema forbids the combination; nothing in the Rust type does, so
    // this pins the invariant the constructors maintain.
    let schema = response_schema();
    let mut response = Response::ok(json!(1));
    response.error = Some(anda_kip::KipError::invalid_syntax("no").into());
    assert!(
        !schema.validate(&encode(&response)).is_empty(),
        "a succeeded response with an error must not validate"
    );

    response.status = TopLevelStatus::Failed;
    schema.assert_valid("a failed response with an error", &encode(&response));
}

#[test]
fn every_status_and_mode_spells_itself_the_way_the_schema_does() {
    let schema = response_schema();
    for (status, expected) in [
        (TopLevelStatus::Succeeded, "succeeded"),
        (TopLevelStatus::Failed, "failed"),
        (TopLevelStatus::Partial, "partial"),
        (TopLevelStatus::OutcomeUnknown, "outcome_unknown"),
    ] {
        assert_eq!(encode(&status), json!(expected));
    }
    for (status, expected) in [
        (OperationStatus::Succeeded, "succeeded"),
        (OperationStatus::Failed, "failed"),
        (OperationStatus::Skipped, "skipped"),
        (OperationStatus::RolledBack, "rolled_back"),
        (OperationStatus::NoEffect, "no_effect"),
    ] {
        assert_eq!(encode(&status), json!(expected));
    }
    for (status, expected) in [
        (ReceiptStatus::Committed, "committed"),
        (ReceiptStatus::Aborted, "aborted"),
        (ReceiptStatus::NoEffect, "no_effect"),
        (ReceiptStatus::Pending, "pending"),
        (ReceiptStatus::Unknown, "unknown"),
    ] {
        assert_eq!(encode(&status), json!(expected));
    }
    for (mode, expected) in [
        (SearchMode::Keyword, "keyword"),
        (SearchMode::Semantic, "semantic"),
        (SearchMode::Hybrid, "hybrid"),
    ] {
        assert_eq!(encode(&mode), json!(expected));
    }

    // And the spellings are exercised through a whole response, so a rename
    // fails here rather than in an engine.
    let response = Response {
        status: TopLevelStatus::Partial,
        execution: Some(ResponseExecution {
            mode: ExecutionMode::Independent,
            on_error: Some(OnError::Continue),
            isolation: None,
            idempotency_key: None,
            extensions: None,
        }),
        results: vec![
            OperationResult::ok(json!(1)),
            OperationResult {
                context: Some(ResultContext {
                    search: Some(SearchContext {
                        mode: Some(SearchMode::Keyword),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..OperationResult::rolled_back()
            },
        ],
        context: Some(ResponseContext::default()),
        receipt: Some(Receipt {
            status: ReceiptStatus::NoEffect,
            tx_id: None,
            space_id: None,
            snapshot_seq: None,
            space_seq: None,
            committed_at: None,
            transaction_class: None,
            request_digest: None,
            semantic_plan_digest: None,
            result_digest: None,
            schema_environment_version: None,
            change_summary: None,
            proofs: Vec::new(),
            receipt_digest: None,
            origin: None,
            extensions: None,
        }),
        warnings: vec![Warning::from("a caveat")],
        ..Default::default()
    };
    schema.assert_valid("a response exercising every spelling", &encode(&response));
}

// ---------------------------------------------------------------------------
// The Change Envelope (§36.1)
// ---------------------------------------------------------------------------

/// A Change Envelope using every member the schema defines.
fn exhaustive_change_envelope() -> Value {
    json!({
        "kip": "2.0",
        "space_id": "kip:space:default",
        "space_seq": 42,
        "tx_id": "kip:space:default#42",
        "committed_at": "2026-09-03T00:00:00.000Z",
        "transaction_class": "cognitive",
        "schema_environment_version": 3,
        "changes": [
            {
                "op": "create",
                "kind": "concept",
                "id": "C-1",
                "schema_ref": "kip://profiles/cognitive-memory@2.0.0/Person",
                "new_version": 1,
                "touched": ["attributes.role"],
                "planes": {
                    "attributes": 1,
                    "structural": 0,
                    "retention": 0,
                    "facets": {"MnemonicState": 1}
                },
                "extensions": {"kip-do/entry": {"critical": false}}
            },
            {
                "op": "lifecycle",
                "kind": "assertion",
                "id": "A-2",
                "old_version": 1,
                "new_version": 2,
                "state": {"from": "active", "to": "retracted"},
                "refs": {
                    "proposition": "P-3",
                    "subject": "C-1",
                    "predicate_ref": "kip://profiles/cognitive-memory@2.0.0/prefers",
                    "merged_into": "C-9"
                }
            }
        ],
        "extensions": {"kip-do/envelope": {"critical": true}}
    })
}

#[test]
fn every_change_envelope_this_crate_builds_matches_the_wire_schema() {
    // §36.1 is the one artifact two engines hand the same consumer, and the
    // schema says `additionalProperties: false` — so a member the Rust type
    // carries and the schema does not define is a field one engine emits and
    // the other's consumer rejects.
    let schema = change_envelope_schema();
    schema.assert_valid(
        "an exhaustive Change Envelope",
        &exhaustive_change_envelope(),
    );

    let decoded: ChangeEnvelope =
        serde_json::from_value(exhaustive_change_envelope()).expect("decodes");
    schema.assert_valid("a re-encoded Change Envelope", &encode(&decoded));
}

#[test]
fn a_change_envelope_using_every_schema_field_survives_the_rust_types() {
    let wire = exhaustive_change_envelope();
    let decoded: ChangeEnvelope =
        serde_json::from_value(wire.clone()).expect("decodes into ChangeEnvelope");
    let reencoded = encode(&decoded);

    let mut dropped = Vec::new();
    dropped_fields(&wire, &reencoded, "", &mut dropped);
    assert!(
        dropped.is_empty(),
        "the Rust Change Envelope types drop wire fields the schema defines:\n  {}",
        dropped.join("\n  ")
    );
}
