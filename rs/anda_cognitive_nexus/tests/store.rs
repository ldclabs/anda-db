//! Storage-layer integration tests against a real Anda DB.
//!
//! The unit tests in `src/` check the rules in isolation; these check that the
//! rules survive contact with the database — that a unique index really
//! rejects a duplicate tuple, that a reopened handle still has its indexes, and
//! that the sequence a `CHANGES` cursor pages through actually advances.

use anda_cognitive_nexus::{
    Element, Store,
    id::ElementId,
    store::{
        rows::*,
        space::{JournalEntry, SpaceDraft},
        write::WriteContext,
    },
    term::{Endpoint, Literal, tuple_key},
    time,
};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::ElementKind;
use object_store::memory::InMemory;
use serde_json::json;
use std::sync::Arc;

const SPACE: &str = "space-test";

async fn fresh_store(name: &str) -> Store {
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: name.to_string(),
            description: "storage tests".to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let store = Store::open(Arc::new(db)).await.unwrap();
    store
        .open_or_create_space(SpaceDraft {
            space_id: SPACE.to_string(),
            name: "Test Space".to_string(),
            owner_principal: "principal-1".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
    store
}

async fn begin(store: &Store) -> WriteContext {
    store
        .begin_transaction(
            SPACE,
            json!({"principal_id": "principal-1", "channel": "test"}),
        )
        .await
        .unwrap()
}

fn concept(name: &str) -> ConceptRow {
    ConceptRow {
        schema_ref: "kip://core/Concept".to_string(),
        name: name.to_string(),
        ..Default::default()
    }
}

fn proposition(subject: &Endpoint, predicate: &str, object: &Endpoint) -> PropositionRow {
    PropositionRow {
        subject: subject.to_json(),
        subject_key: subject.key(),
        predicate_ref: predicate.to_string(),
        object: object.to_json(),
        object_key: object.key(),
        tuple_key: tuple_key(SPACE, subject, predicate, object),
        ..Default::default()
    }
}

fn local(id: ElementId) -> Endpoint {
    Endpoint::Local(id)
}

#[tokio::test]
async fn a_minimal_epistemic_graph_round_trips() {
    // Spec §95: the smallest useful graph — Alice prefers dark mode, because
    // she said so, observed through one Evidence record, produced by one
    // Activity. Every Core kind, once.
    let store = fresh_store("minimal_graph").await;
    let cx = begin(&store).await;

    let mut alice = concept("Alice");
    let alice_id = store.insert(&cx, &mut alice).await.unwrap();
    let mut dark_mode = concept("DarkMode");
    let dark_id = store.insert(&cx, &mut dark_mode).await.unwrap();

    let mut tuple = proposition(&local(alice_id), "prefers", &local(dark_id));
    let proposition_id = store.insert(&cx, &mut tuple).await.unwrap();

    let mut evidence = EvidenceRow {
        evidence_class: "user_statement".to_string(),
        payload_mode: "inline".to_string(),
        payload_inline: json!("I prefer dark mode."),
        observed_at: time::normalize("2026-08-16T09:00:00Z", "observed_at").unwrap(),
        status: "active".to_string(),
        ..Default::default()
    };
    let evidence_id = store.insert(&cx, &mut evidence).await.unwrap();

    let mut assertion = AssertionRow {
        proposition_id: proposition_id.to_string(),
        asserted_by: local(alice_id).to_json(),
        asserted_by_key: local(alice_id).key(),
        stance: "support".to_string(),
        mode: "stated".to_string(),
        confidence: 0.9,
        status: "active".to_string(),
        evidence_ids: vec![evidence_id.to_string()],
        evidence_refs: vec![json!({"evidence_id": evidence_id.to_string(), "role": "support"})],
        ..Default::default()
    };
    let assertion_id = store.insert(&cx, &mut assertion).await.unwrap();

    let mut activity = ActivityRow {
        activity_class: "extraction".to_string(),
        status: "completed".to_string(),
        inputs: vec![json!({"id": evidence_id.to_string()})],
        input_keys: vec![local(evidence_id).key()],
        outputs: vec![json!({"id": assertion_id.to_string()})],
        output_keys: vec![local(assertion_id).key()],
        ..Default::default()
    };
    let activity_id = store.insert(&cx, &mut activity).await.unwrap();

    // Each id names its own kind, so a reference resolves without a type map.
    assert_eq!(alice_id.kind, ElementKind::Concept);
    assert_eq!(proposition_id.kind, ElementKind::Proposition);
    assert_eq!(assertion_id.kind, ElementKind::Assertion);
    assert_eq!(evidence_id.kind, ElementKind::Evidence);
    assert_eq!(activity_id.kind, ElementKind::Activity);

    for id in [
        alice_id,
        dark_id,
        proposition_id,
        assertion_id,
        evidence_id,
        activity_id,
    ] {
        let element = store.get_element(id).await.unwrap();
        assert_eq!(element.id(), id);
        assert_eq!(element.space(), SPACE);
        assert_eq!(element.version(), 1, "a new element starts at version 1");
        assert_eq!(element.seq(), cx.seq, "one commit, one sequence");
        assert!(element.is_active());
    }

    let Element::Assertion(stored) = store.get_element(assertion_id).await.unwrap() else {
        panic!("A- ids must load as Assertions");
    };
    assert_eq!(stored.confidence, 0.9);
    assert_eq!(stored.proposition_id, proposition_id.to_string());
    assert_eq!(stored.created_tx, cx.tx_id);

    // The claim's confidence lives on the Assertion. The Proposition it is
    // about has no such column at all (§12.8) — which is the whole point of
    // separating them.
    let Element::Proposition(stored) = store.get_element(proposition_id).await.unwrap() else {
        panic!("P- ids must load as Propositions");
    };
    assert_eq!(stored.predicate_ref, "prefers");
    assert_eq!(stored.subject_key, local(alice_id).key());
}

#[tokio::test]
async fn one_space_keeps_one_proposition_per_tuple() {
    // Spec §93.6: a Space has one canonical Proposition per semantic tuple.
    // The unique index is what makes `ENSURE PROPOSITION` resolve instead of
    // racing two writers into a duplicate.
    let store = fresh_store("tuple_uniqueness").await;
    let cx = begin(&store).await;

    let mut alice = concept("Alice");
    let alice_id = store.insert(&cx, &mut alice).await.unwrap();
    let dark = Endpoint::Literal(Literal::from_scalar(json!("dark")).unwrap());

    let mut first = proposition(&local(alice_id), "theme", &dark);
    store.insert(&cx, &mut first).await.unwrap();

    let mut duplicate = proposition(&local(alice_id), "theme", &dark);
    let err = store.insert(&cx, &mut duplicate).await.unwrap_err();
    assert_eq!(err.name(), "IdentityConflict");

    // A different lexical form of the same number is the same Literal, so it
    // is the same tuple too (§9.4).
    let mut with_int = proposition(
        &local(alice_id),
        "retry_count",
        &Endpoint::Literal(Literal::from_scalar(json!(3)).unwrap()),
    );
    store.insert(&cx, &mut with_int).await.unwrap();
    let mut with_float = proposition(
        &local(alice_id),
        "retry_count",
        &Endpoint::Literal(Literal::from_scalar(json!(3.0)).unwrap()),
    );
    let err = store.insert(&cx, &mut with_float).await.unwrap_err();
    assert_eq!(err.name(), "IdentityConflict", "3 and 3.0 are one Literal");

    // A different predicate is a different tuple.
    let mut other = proposition(&local(alice_id), "fallback_theme", &dark);
    store.insert(&cx, &mut other).await.unwrap();
}

#[tokio::test]
async fn an_update_bumps_the_version_expect_version_compares_against() {
    let store = fresh_store("versioning").await;
    let cx = begin(&store).await;
    let mut alice = concept("Alice");
    let id = store.insert(&cx, &mut alice).await.unwrap();
    assert!(Store::expect_version(id, alice.version, 1).is_ok());

    let cx2 = begin(&store).await;
    assert_eq!(cx2.seq, cx.seq + 1, "each commit takes the next sequence");
    alice.name = "Alice Smith".to_string();
    let version = store.update(&cx2, &mut alice).await.unwrap();
    assert_eq!(version, 2);

    let Element::Concept(stored) = store.get_element(id).await.unwrap() else {
        panic!("C- ids must load as Concepts");
    };
    assert_eq!(stored.name, "Alice Smith");
    assert_eq!(stored.version, 2);
    assert_eq!(stored.seq, cx2.seq);
    assert_eq!(stored.updated_tx, cx2.tx_id);
    // The record of when this element entered the Nexus survives the update.
    assert_eq!(stored.created_tx, cx.tx_id);

    let err = Store::expect_version(id, stored.version, 1).unwrap_err();
    assert_eq!(err.name(), "VersionConflict");
}

#[tokio::test]
async fn a_reference_out_of_the_space_is_refused() {
    // Spec §7: baseline Core is same-Space closed. A reference that silently
    // reached into another Space would make a later read depend on authority
    // the reader may not have.
    let store = fresh_store("closure").await;
    store
        .open_or_create_space(SpaceDraft {
            space_id: "space-other".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();

    let cx = begin(&store).await;
    let mut here = concept("Here");
    let here_id = store.insert(&cx, &mut here).await.unwrap();

    let other_cx = store
        .begin_transaction("space-other", json!({}))
        .await
        .unwrap();
    let mut there = concept("There");
    let there_id = store.insert(&other_cx, &mut there).await.unwrap();

    store
        .check_same_space(SPACE, here_id, "subject")
        .await
        .unwrap();
    let err = store
        .check_same_space(SPACE, there_id, "subject")
        .await
        .unwrap_err();
    assert_eq!(err.name(), "StructuralReferenceInvalid");
    assert!(err.message.contains("space-other"));
}

#[tokio::test]
async fn a_lost_response_is_recovered_from_the_journal() {
    // Spec §80.4: recovery from a lost response is a lookup, not a second
    // write. That only works if the key was journaled with the result.
    let store = fresh_store("idempotency").await;
    let cx = begin(&store).await;
    let mut alice = concept("Alice");
    let id = store.insert(&cx, &mut alice).await.unwrap();

    store
        .journal(
            &cx,
            JournalEntry {
                status: "committed".to_string(),
                transaction_class: "cognitive".to_string(),
                idempotency_key: "key-1".to_string(),
                result: json!({"created": [id.to_string()]}),
                changes: vec![json!({
                    "id": id.to_string(),
                    "kind": "concept",
                    "op": "create",
                    "version": 1,
                })],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let replayed = store
        .find_transaction_by_idempotency_key(SPACE, "key-1")
        .await
        .unwrap()
        .expect("the key was journaled");
    assert_eq!(replayed.tx_id, cx.tx_id);
    assert_eq!(replayed.result, json!({"created": [id.to_string()]}));
    assert_eq!(replayed.changed_ids, vec![id.to_string()]);
    assert_eq!(replayed.snapshot_seq, cx.seq - 1);

    assert!(store.find_transaction(&cx.tx_id).await.unwrap().is_some());
    assert!(
        store
            .find_transaction_by_idempotency_key(SPACE, "key-2")
            .await
            .unwrap()
            .is_none()
    );
    // A keyless transaction must not answer to the empty key, or every
    // keyless commit in the Space would look like a replay of the first.
    assert!(
        store
            .find_transaction_by_idempotency_key(SPACE, "")
            .await
            .unwrap()
            .is_none()
    );
    // Idempotency is per Space.
    assert!(
        store
            .find_transaction_by_idempotency_key("space-elsewhere", "key-1")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn reopening_the_store_keeps_its_data_and_its_indexes() {
    // Poison recovery reopens every handle. A reopened handle that lost its
    // indexes or its tokenizer would answer queries wrongly rather than
    // failing, which is the worst way to lose them.
    let store = fresh_store("reopen").await;
    let cx = begin(&store).await;
    let mut alice = concept("Alice");
    let alice_id = store.insert(&cx, &mut alice).await.unwrap();
    let mut tuple = proposition(
        &local(alice_id),
        "theme",
        &Endpoint::Literal(Literal::from_scalar(json!("dark")).unwrap()),
    );
    store.insert(&cx, &mut tuple).await.unwrap();
    store.flush(1_755_000_000_000).await.unwrap();

    assert!(!store.has_poisoned_handle());
    store.reopen().await.unwrap();
    store.reopen().await.unwrap(); // idempotent

    let element = store.get_element(alice_id).await.unwrap();
    assert_eq!(element.space(), SPACE);

    // The unique constraint is index state; if the reopen dropped it, this
    // duplicate would be accepted.
    let cx2 = begin(&store).await;
    let mut duplicate = proposition(
        &local(alice_id),
        "theme",
        &Endpoint::Literal(Literal::from_scalar(json!("dark")).unwrap()),
    );
    assert!(store.insert(&cx2, &mut duplicate).await.is_err());

    // The Space sequence survived the reopen rather than restarting.
    let space = store.get_space(SPACE).await.unwrap();
    assert_eq!(space.seq, cx2.seq);
}

#[tokio::test]
async fn opening_a_space_twice_does_not_reset_it() {
    let store = fresh_store("space_idempotent").await;
    let cx = begin(&store).await;
    assert_eq!(cx.seq, 1, "sequence 0 means nothing has happened yet");

    let again = store
        .open_or_create_space(SpaceDraft {
            space_id: SPACE.to_string(),
            name: "A Different Label".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(again.seq, 1, "reopening must not rewind history");
    assert_eq!(again.name, "Test Space", "nor overwrite the existing Space");

    let missing = store.get_space("space-nope").await.unwrap_err();
    assert_eq!(missing.name(), "NotFoundOrNotVisible");
}
