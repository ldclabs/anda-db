//! Regressions for the 2026-09-25 review: read-path reference audit, commit-time
//! sequences, reference-binding fan-out, transaction size, the indexed SEARCH
//! path, learning-record lookups and read cost.

use anda_cognitive_nexus::{
    CognitiveNexus,
    governance::{
        AuthContext,
        rows::{AuthorityConstraints, AuthorityScope},
        store::{GrantDraft, PrincipalDraft},
    },
    nexus::DEFAULT_SPACE,
    profiles::{COGNITIVE_MEMORY, COGNITIVE_MEMORY_ID, COGNITIVE_MEMORY_VERSION},
    schema::{PackageState, SchemaLock, SchemaPackage, contracts},
    store::{Element, space::SpaceDraft},
};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{Executor, Json, Request, TopLevelStatus};
use object_store::memory::InMemory;
use serde_json::json;
use std::sync::Arc;

async fn fresh(name: &str) -> CognitiveNexus {
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
    let nexus = CognitiveNexus::connect(db).await.unwrap();
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

fn request(command: &str, parameters: Json, dry_run: bool) -> Request {
    let mut request = Request::single(command);
    request.parameters = parameters.as_object().cloned();
    if dry_run {
        request.options = Some(anda_kip::RequestOptions {
            dry_run: Some(true),
            ..Default::default()
        });
    }
    request
}

async fn run(n: &impl Executor, command: &str, parameters: Json) -> anda_kip::Response {
    let request = request(command, parameters, false);
    n.execute(
        anda_kip::parse_kip(command).unwrap(),
        &request,
        &request.operations[0],
    )
    .await
}

async fn ok(n: &impl Executor, command: &str, parameters: Json) -> Json {
    let response = run(n, command, parameters).await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "{command}: {:?}",
        response.error
    );
    response.first_result().cloned().unwrap_or(Json::Null)
}

async fn seq(n: &CognitiveNexus) -> u64 {
    n.store.get_space(DEFAULT_SPACE).await.unwrap().seq
}

async fn reader(
    n: &CognitiveNexus,
    id: &str,
    grant: GrantDraft,
) -> anda_cognitive_nexus::nexus::Session {
    n.governance()
        .ensure_principal(PrincipalDraft {
            principal_id: id.into(),
            principal_class: "agent".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    n.governance()
        .create_grant(
            GrantDraft {
                space_id: DEFAULT_SPACE.into(),
                grantee_principal: id.into(),
                ..grant
            },
            "kip:principal:system",
        )
        .await
        .unwrap();
    n.session(AuthContext::principal(id))
}

#[tokio::test]
async fn an_endpoint_reached_through_a_tuple_keeps_its_reference_audit_filtered() {
    let n = fresh("review_reference_audit").await;
    let created = ok(
        &n,
        r#"MUTATE {
          CREATE CONCEPT ?secret {TYPE "Person" NAME "Secret"}
          CREATE CONCEPT ?ada {TYPE "Person" NAME "Ada"}
          CREATE CONCEPT ?party {TYPE "Event" NAME "Party" SET ATTRIBUTES {summary: "a party"} SET STRUCTURAL {("involves", ?secret)}}
          ENSURE PROPOSITION ?p (?ada, "prefers", ?party)
        }"#,
        Json::Null,
    )
    .await;
    let handle = |name: &str| created["handles"][name].as_str().unwrap().to_string();
    let limited = reader(
        &n,
        "kip:principal:limited",
        GrantDraft {
            actions: vec!["read".into()],
            scope: AuthorityScope {
                elements: vec![handle("ada"), handle("party"), handle("p")],
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await;
    // The Event's view is first built while the Proposition's endpoints are
    // canonicalized; binding it afterwards must still apply the audit filter.
    for command in [
        format!(
            r#"FIND(?o._system.input_references) WHERE {{?o CONCEPT {{id: "{}"}}}}"#,
            handle("party")
        ),
        r#"FIND(?o._system.input_references) WHERE { (?s, "prefers", ?o) }"#.to_string(),
    ] {
        let answer = ok(&limited, &command, Json::Null).await;
        assert!(
            !answer.to_string().contains(&handle("secret")),
            "{command}: {answer}"
        );
    }
}

#[tokio::test]
async fn only_a_commit_takes_a_space_sequence() {
    let n = fresh("review_sequence").await;
    let start = seq(&n).await;

    // A dry run, a refused statement and a no-op reserve nothing (§32.8,
    // §69.3); none of them names the coordinate the next commit takes.
    let dry = request(
        r#"CREATE CONCEPT ?a {TYPE "Person" NAME "Ada"}"#,
        Json::Null,
        true,
    );
    let response = n
        .execute(
            anda_kip::parse_kip(&dry.operations[0].command.clone().unwrap()).unwrap(),
            &dry,
            &dry.operations[0],
        )
        .await;
    assert_eq!(response.status, TopLevelStatus::Succeeded);
    let dry_tx = response.results[0]
        .receipt
        .as_ref()
        .unwrap()
        .tx_id
        .clone()
        .unwrap();
    assert_ne!(dry_tx, format!("{DEFAULT_SPACE}#{}", start + 1));
    assert_eq!(seq(&n).await, start);

    let refused = run(
        &n,
        r#"UPDATE "C-999" SET FIELDS {name: "Nope"}"#,
        Json::Null,
    )
    .await;
    assert_eq!(refused.status, TopLevelStatus::Failed);
    assert_eq!(seq(&n).await, start);

    let created = run(
        &n,
        r#"CREATE CONCEPT ?a {TYPE "Person" NAME "Ada"}"#,
        Json::Null,
    )
    .await;
    let receipt = created.results[0].receipt.clone().unwrap();
    assert_eq!(receipt.space_seq, Some(start + 1));
    assert_eq!(
        receipt.tx_id,
        Some(format!("{DEFAULT_SPACE}#{}", start + 1))
    );
    let id = created.first_result().unwrap()["handles"]["a"]
        .as_str()
        .unwrap()
        .to_string();

    // A no-op under an idempotency key is retained for its resend (§34.3) —
    // without a sequence — and replays as the same receipt.
    let noop = format!(r#"UPDATE "{id}" SET FIELDS {{name: "Ada"}}"#);
    let mut keyed = request(&noop, Json::Null, false);
    keyed.operations[0].idempotency_key = Some("same-name".into());
    let command = anda_kip::parse_kip(&noop).unwrap();
    let first = n
        .execute(command.clone(), &keyed, &keyed.operations[0])
        .await;
    let receipt = first.results[0].receipt.clone().unwrap();
    assert_eq!(receipt.status, anda_kip::ReceiptStatus::NoEffect);
    assert_eq!(receipt.space_seq, None);
    assert_eq!(seq(&n).await, start + 1);
    let again = n.execute(command, &keyed, &keyed.operations[0]).await;
    assert_eq!(
        again.results[0].receipt.as_ref().unwrap().receipt_digest,
        receipt.receipt_digest
    );

    let next = run(
        &n,
        r#"CREATE CONCEPT ?b {TYPE "Person" NAME "Bea"}"#,
        Json::Null,
    )
    .await;
    assert_eq!(
        next.results[0].receipt.as_ref().unwrap().space_seq,
        Some(start + 2)
    );
    // The journal stays in sequence order for readers of it.
    let snapshot = ok(
        &n,
        "DESCRIBE SNAPSHOT AT TIME :t",
        json!({"t": "2999-01-01T00:00:00.000Z"}),
    )
    .await;
    assert_eq!(snapshot["space_seq"], json!(start + 2), "{snapshot}");
}

#[tokio::test]
async fn each_element_audits_each_resolution_once() {
    let n = fresh("review_binding_fanout").await;
    let mut text = String::from("MUTATE {\nCREATE CONCEPT ?ada {TYPE \"Person\" NAME \"Ada\"}\n");
    for i in 0..10 {
        text.push_str(&format!(
            "CREATE CONCEPT ?o{i} {{TYPE \"Person\" NAME \"P{i}\"}}\nENSURE PROPOSITION ?p{i} (?ada, \"prefers\", ?o{i})\n"
        ));
    }
    text.push('}');
    let created = ok(&n, &text, Json::Null).await;
    let p0 = created["handles"]["p0"].as_str().unwrap();
    let audit = ok(
        &n,
        &format!(r#"FIND(?p._system.input_references) WHERE {{ ?p PROPOSITION (id: "{p0}") }}"#),
        Json::Null,
    )
    .await;
    // Subject and object: two resolutions, however many clauses named Ada.
    assert_eq!(audit[0].as_array().unwrap().len(), 2, "{audit}");
}

#[tokio::test]
async fn a_large_statement_commits_and_an_oversized_one_is_refused_cleanly() {
    let n = fresh("review_transaction_size").await;
    let mut text = String::from("MUTATE {\n");
    for i in 0..600 {
        text.push_str(&format!(
            "CREATE CONCEPT ?c{i} {{TYPE \"Person\" NAME \"P{i}\"}}\n"
        ));
    }
    text.push('}');
    let created = run(&n, &text, Json::Null).await;
    assert_eq!(
        created.status,
        TopLevelStatus::Succeeded,
        "{:?}",
        created.error
    );
    let before = seq(&n).await;

    // One value past the per-field budget is refused before anything durable
    // happens: a clear, non-retryable answer and no shells left behind.
    let big: Vec<u64> = (0..20_000).collect();
    let refused = run(
        &n,
        r#"CREATE EVIDENCE ?e {SET FIELDS {evidence_class: "observation", payload: :big}}"#,
        json!({"big": big}),
    )
    .await;
    assert_eq!(refused.status, TopLevelStatus::Failed);
    assert_eq!(
        refused.error.as_ref().unwrap().code.as_str(),
        "ResourceExhausted",
        "{:?}",
        refused.error
    );
    assert_eq!(n.store.sweep_pending().await.unwrap(), 0);
    assert_eq!(seq(&n).await, before);
    assert!(n.store.commit_log().is_empty());
}

#[tokio::test]
async fn an_unnarrowed_search_ranks_like_the_authorized_scan() {
    let n = fresh("review_indexed_search").await;
    ok(
        &n,
        r#"MUTATE {
          CREATE CONCEPT ?a {TYPE "Person" NAME "Alice Liddell" SET FIELDS {aliases: ["Alice"]}}
          CREATE CONCEPT ?b {TYPE "Person" NAME "Alice Cooper"}
          CREATE CONCEPT ?c {TYPE "Person" NAME "Bob" SET ATTRIBUTES {note: "knows Alice"}}
          CREATE CONCEPT ?d {TYPE "Event" NAME "Tea party" SET ATTRIBUTES {summary: "Alice at tea"}}
        }"#,
        Json::Null,
    )
    .await;
    // Another Space full of the same word must not move this Space's scores.
    n.store
        .open_or_create_space(SpaceDraft {
            space_id: "kip:space:other".into(),
            owner_principal: "kip:principal:system".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut other = request(
        r#"CREATE CONCEPT ?x {NAME "Alice Alice Alice"}"#,
        Json::Null,
        false,
    );
    other.space = Some(anda_kip::SpaceSelector {
        id: Some("kip:space:other".into()),
        ..Default::default()
    });
    n.execute(
        anda_kip::parse_kip(r#"CREATE CONCEPT ?x {NAME "Alice Alice Alice"}"#).unwrap(),
        &other,
        &other.operations[0],
    )
    .await;

    // A result cap makes the reader's authority narrowed, so it takes the
    // scan over its authorized corpus — which here is the whole Space.
    let capped = reader(
        &n,
        "kip:principal:capped",
        GrantDraft {
            actions: vec!["read".into(), "search".into()],
            constraints: AuthorityConstraints {
                max_results: Some(100),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await;
    for command in [
        r#"SEARCH CONCEPT "Alice" LIMIT 10"#,
        r#"SEARCH CONCEPT "Alice" WITH TYPE "Person" LIMIT 10"#,
        r#"SEARCH CONCEPT "alice tea" LIMIT 10"#,
    ] {
        let indexed = ok(&n.system_session(), command, Json::Null).await;
        let scanned = ok(&capped, command, Json::Null).await;
        let pairs = |answer: &Json| -> Vec<(String, f64)> {
            answer["hits"]
                .as_array()
                .unwrap()
                .iter()
                .map(|hit| {
                    (
                        hit["id"].as_str().unwrap().to_string(),
                        hit["score"].as_f64().unwrap(),
                    )
                })
                .collect()
        };
        let (indexed, scanned) = (pairs(&indexed), pairs(&scanned));
        assert!(!indexed.is_empty(), "{command}");
        assert_eq!(indexed.len(), scanned.len(), "{command}");
        for ((a, x), (b, y)) in indexed.iter().zip(&scanned) {
            assert_eq!(a, b, "{command}");
            assert!((x - y).abs() < 1e-9, "{command}: {x} vs {y}");
        }
    }

    // Paging reads only the page it returns (plus one to know there is more).
    let before = n.store.concepts().stats().get_count;
    let page = ok(
        &n.system_session(),
        r#"SEARCH CONCEPT "Alice" LIMIT 1"#,
        Json::Null,
    )
    .await;
    assert_eq!(page["hits"].as_array().unwrap().len(), 1);
    assert!(n.store.concepts().stats().get_count - before <= 2);
}

#[tokio::test]
async fn a_tuple_read_pays_no_version_log_or_per_element_control_lookups() {
    let n = fresh("review_read_cost").await;
    let mut text = String::from("MUTATE {\n");
    for i in 0..20 {
        text.push_str(&format!(
            "CREATE CONCEPT ?s{i} {{TYPE \"Person\" NAME \"S{i}\"}}\nCREATE CONCEPT ?o{i} {{TYPE \"Person\" NAME \"O{i}\"}}\nENSURE PROPOSITION (?s{i}, \"prefers\", ?o{i})\n"
        ));
    }
    text.push('}');
    ok(&n, &text, Json::Null).await;
    let versions = n.store.element_versions().stats().search_count;
    let controls = n.store.control_records().stats().search_count;
    let answer = ok(
        &n,
        r#"FIND(?p) WHERE { ?p PROPOSITION (?s, "prefers", ?o) }"#,
        Json::Null,
    )
    .await;
    assert_eq!(answer.as_array().unwrap().len(), 20);
    assert_eq!(n.store.element_versions().stats().search_count, versions);
    // The projection policy and the identity reviews, once each per read.
    assert!(n.store.control_records().stats().search_count - controls <= 3);
}

#[tokio::test]
async fn learning_records_are_indexed_and_unique_by_their_identity() {
    let n = fresh("review_learning_index").await;
    ok(
        &n,
        r#"MUTATE {
          CREATE CONCEPT ?a {TYPE "Person" NAME "Ada"}
          CREATE EVIDENCE ?e { SET FIELDS {evidence_class: "observation", payload: "material"} }
        }"#,
        Json::Null,
    )
    .await;
    let s = n.system_session();
    let selection = s
        .put_artifact(DEFAULT_SPACE, json!({"selection": "explicit"}), vec![])
        .await
        .unwrap();
    ok(
        &n,
        r#"MUTATE {
          CREATE CONCEPT ?b {TYPE "Person" NAME "Bea"}
          ENSURE PROPOSITION ?p (:ada, "prefers", ?b)
          CREATE ASSERTION ?x {SET FIELDS {proposition: ?p, asserted_by: :ada, mode: "stated", stance: "support", confidence: 0.9}}
        }"#,
        json!({"ada": "C-1"}),
    )
    .await;
    let basis = ok(
        &n,
        r#"FIND(?b) WHERE { ?p PROPOSITION (id: "P-1") ?b BELIEF (?p) }"#,
        Json::Null,
    )
    .await[0]["basis"]
        .clone();
    let decision = json!({"decision": "act", "retrieved_refs": ["E-1"], "used_refs": ["E-1"], "applied_revisions": [], "basis": basis});
    let dependency = json!({"basis_seq": basis["snapshot_seq"], "groups": [{"role": "all_of", "pins": [{"id": "E-1", "version": 1}]}], "policy_basis": basis});
    let gate = ok(
        &n,
        &format!(
            r#"CREATE ACTIVITY ?gate {{SET FIELDS {{activity_class: "action_gate", status: "completed"}} SET FACET "DecisionRecord" {decision} SET FACET "DependencyBasis" {dependency} SET STRUCTURAL {{("inputs", "E-1")}}}}"#
        ),
        Json::Null,
    )
    .await["handles"]["gate"]
        .as_str()
        .unwrap()
        .to_string();
    let record = json!({"attempt_id": "attempt-1", "decision_ref": gate, "applied_revisions": [], "trial_ref": null, "context": {}, "environment_digest": contracts::digest(&json!({})).unwrap(), "tool_versions": {"test": "1"}, "selection_policy": selection, "preconditions_satisfied": "yes", "started_at": anda_cognitive_nexus::time::now()});
    let attempt = format!(
        r#"CREATE ACTIVITY ?attempt {{SET FIELDS {{activity_class: "action_attempt", status: "completed"}} SET FACET "AttemptRecord" {record} SET STRUCTURAL {{("inputs", "{gate}")}}}}"#
    );
    let created = ok(&n, &attempt, Json::Null).await;
    let id = created["handles"]["attempt"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let Element::Activity(row) = n.store.get_element(id).await.unwrap() else {
        panic!("an Activity");
    };
    assert_eq!(
        row.record_keys,
        Some(vec![
            "AttemptRecord".into(),
            "AttemptRecord:attempt-1".into()
        ])
    );
    let duplicate = run(&n, &attempt, Json::Null).await;
    assert_eq!(duplicate.status, TopLevelStatus::Failed);
    assert!(
        duplicate
            .error
            .as_ref()
            .unwrap()
            .message
            .contains("attempt_id must be Space-unique"),
        "{:?}",
        duplicate.error
    );
}
