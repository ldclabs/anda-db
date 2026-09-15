//! Regression coverage for §52.7–61 details shared by both KML engines.
use anda_cognitive_nexus::{
    CognitiveNexus, Element,
    id::ElementId,
    nexus::DEFAULT_SPACE,
    schema::{PackageState, SchemaLock, SchemaPackage},
};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{Executor, Json, ReceiptStatus, Request, TopLevelStatus};
use object_store::memory::InMemory;
use serde_json::json;
use std::sync::Arc;

async fn nexus(name: &str) -> CognitiveNexus {
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: name.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let n = CognitiveNexus::connect(Arc::new(db)).await.unwrap();
    let package = SchemaPackage::parse(&json!({
        "format": "KIP-Schema-Package",
        "manifest": {"package_id": "kip://test/kml-detail", "version": "1.0.0"},
        "definitions": {"concept_types": {
            "Counter": {"kind": "ConceptType", "attributes": {"open": true}},
            "Other": {"kind": "ConceptType"}
        }, "structural_fields": {
            "links": {"kind":"StructuralFieldDefinition", "source":{"kinds":["Concept"]}, "target":{"kinds":["Concept"]}, "ordered":true},
            "primary": {"kind":"StructuralFieldDefinition", "source":{"kinds":["Concept"]}, "target":{"kinds":["Concept"]}, "cardinality":{"max":1}}
        }, "facets": {"Numeric": {"kind":"FacetDefinition", "fields":{"count":{"type":"number"}}}}}
    }).to_string()).unwrap();
    n.install_package(&package, "test").await.unwrap();
    let mut lock = SchemaLock::default();
    lock.packages
        .insert("kip://test/kml-detail".into(), "1.0.0".into());
    lock.states
        .insert("kip://test/kml-detail".into(), PackageState::Active);
    n.activate_schema(DEFAULT_SPACE, lock).await.unwrap();
    n
}

async fn run(n: &CognitiveNexus, command: &str) -> anda_kip::Response {
    let request = Request::single(command);
    n.execute(
        anda_kip::parse_kip(command).unwrap(),
        &request,
        &request.operations[0],
    )
    .await
}
async fn ok(n: &CognitiveNexus, command: &str) -> anda_kip::Response {
    let response = run(n, command).await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "{command}\n{:?}",
        response.error
    );
    response
}
async fn row(n: &CognitiveNexus, id: &str) -> Json {
    let id: ElementId = id.parse().unwrap();
    let Element::Concept(row) = n.store.get_element(id).await.unwrap() else {
        panic!()
    };
    serde_json::to_value(row).unwrap()
}
fn no_effect(r: &anda_kip::Response) {
    assert_eq!(
        r.results[0].receipt.as_ref().unwrap().status,
        ReceiptStatus::NoEffect
    );
}

#[tokio::test]
async fn expressions_skip_invalid_inputs_but_literal_null_and_shallow_merge_are_assignments() {
    let n = nexus("kml_detail_skip").await;
    ok(
        &n,
        r#"CREATE CONCEPT ?c { TYPE "Counter" SET ATTRIBUTES {
        count: 2, text: "hello", keep: true, bag: {old: 1, kept: 2}, list: [1, 2]
    } }"#,
    )
    .await;
    ok(
        &n,
        r#"UPDATE ?c SET ATTRIBUTES {
        missing: ADD(?c.attributes.missing, 1),
        count: ADD(?c.attributes.text, 1),
        text: COALESCE(?c.attributes.text, 9),
        fallback: COALESCE(?c.attributes.missing, 3),
        explicit: null, bag: {new: 3}, list: [4]
    } WHERE { ?c CONCEPT {id: "C-1"} }"#,
    )
    .await;
    assert_eq!(
        row(&n, "C-1").await["attributes"],
        json!({
            "count": 2, "text": "hello", "keep": true, "bag": {"new": 3},
            "list": [4], "fallback": 3, "explicit": null
        })
    );
    ok(&n, r#"UPDATE "C-1" UNSET ATTRIBUTES {explicit}"#).await;
    assert!(row(&n, "C-1").await["attributes"].get("explicit").is_none());
}

#[tokio::test]
async fn targets_are_deduplicated_before_limit_and_assignments_read_one_snapshot() {
    let n = nexus("kml_detail_dedup").await;
    ok(
        &n,
        r#"MUTATE {
        CREATE CONCEPT ?a { TYPE "Counter" SET ATTRIBUTES {count: 2} }
        CREATE CONCEPT ?b { TYPE "Counter" SET ATTRIBUTES {count: 5} }
    }"#,
    )
    .await;
    ok(
        &n,
        r#"UPDATE ?c SET ATTRIBUTES {count: ADD(?c.attributes.count, 1)}
        SET ATTRIBUTES {double: MUL(?c.attributes.count, 2)}
        WHERE { ?c CONCEPT {type: "Counter"} ?join CONCEPT {type: "Counter"} } LIMIT 2"#,
    )
    .await;
    assert_eq!(
        row(&n, "C-1").await["attributes"],
        json!({"count": 3, "double": 4})
    );
    assert_eq!(
        row(&n, "C-2").await["attributes"],
        json!({"count": 6, "double": 10})
    );
    let version = row(&n, "C-1").await["version"].clone();
    no_effect(
        &ok(
            &n,
            r#"UPDATE "C-1" SET FIELDS {name: "changed"}
        WHERE { ?c CONCEPT {type: "Counter"} } LIMIT 0"#,
        )
        .await,
    );
    assert_eq!(row(&n, "C-1").await["version"], version);
}

#[tokio::test]
async fn numeric_errors_roll_back_every_target_and_assignment() {
    let n = nexus("kml_detail_numbers").await;
    ok(
        &n,
        r#"MUTATE {
        CREATE CONCEPT ?a { TYPE "Counter" SET ATTRIBUTES {count: 2} }
        CREATE CONCEPT ?b { TYPE "Counter" SET ATTRIBUTES {count: 9007199254740991} }
    }"#,
    )
    .await;
    let failed = run(
        &n,
        r#"UPDATE ?c SET FIELDS {name: "changed"}
        SET ATTRIBUTES {count: ADD(?c.attributes.count, 1)}
        WHERE { ?c CONCEPT {type: "Counter"} }"#,
    )
    .await;
    assert_eq!(failed.error.unwrap().code.as_str(), "TypeMismatch");
    assert_eq!(row(&n, "C-1").await["attributes"]["count"], 2);
    assert_eq!(row(&n, "C-1").await["name"], "");
    assert_eq!(row(&n, "C-1").await["version"], 1);
    for expression in ["MUL(1e-300, 1e-100)", "CLAMP(1, 3, 2)"] {
        let failed = run(
            &n,
            &format!(
                r#"UPDATE ?c SET ATTRIBUTES {{count: {expression}}}
            WHERE {{ ?c CONCEPT {{id: "C-1"}} }}"#
            ),
        )
        .await;
        assert_eq!(failed.status, TopLevelStatus::Failed);
        assert_eq!(row(&n, "C-1").await["attributes"]["count"], 2);
    }
}

#[tokio::test]
async fn merges_preserve_identity_and_validate_endpoint_cardinality_and_lineage() {
    let n = nexus("kml_detail_merge").await;
    ok(
        &n,
        r#"MUTATE {
        CREATE CONCEPT ?a { TYPE "Counter" }
        CREATE CONCEPT ?b { TYPE "Counter" }
        CREATE CONCEPT ?c { TYPE "Counter" }
        CREATE CONCEPT ?d { TYPE "Other" }
    }"#,
    )
    .await;
    no_effect(&ok(&n, r#"MERGE CONCEPT "C-1" INTO "C-1""#).await);
    for (command, code) in [
        (r#"MERGE CONCEPT "C-1" INTO "C-4""#, "IdentityMergeConflict"),
        (
            r#"MERGE CONCEPT ?source INTO "C-3" WHERE { ?source CONCEPT {type: "Counter"} }"#,
            "IdentityMergeConflict",
        ),
        (
            r#"MERGE CONCEPT ?source INTO "C-3" WHERE { ?source CONCEPT {name: "absent"} }"#,
            "NotFoundOrNotVisible",
        ),
    ] {
        assert_eq!(run(&n, command).await.error.unwrap().code.as_str(), code);
    }
    ok(&n, r#"MERGE CONCEPT "C-1" INTO "C-2""#).await;
    ok(&n, r#"MERGE CONCEPT "C-2" INTO "C-3""#).await;
    let version = row(&n, "C-1").await["version"].clone();
    no_effect(&ok(&n, r#"MERGE CONCEPT "C-1" INTO "C-3""#).await);
    assert_eq!(row(&n, "C-1").await["version"], version);
    assert_eq!(row(&n, "C-1").await["merged_into"], "C-2");
}

#[tokio::test]
async fn upsert_replaces_empty_fields_and_never_falls_back_from_an_id() {
    let n = nexus("kml_detail_upsert").await;
    ok(&n, r#"CREATE CONCEPT ?c { TYPE "Counter" NAME "Before" SET FIELDS {key: "counter", aliases: ["old"]} }"#).await;
    ok(
        &n,
        r#"UPSERT CONCEPT ?c { MATCH {id: "C-1"} SET FIELDS {name: "", aliases: []} }"#,
    )
    .await;
    assert_eq!(row(&n, "C-1").await["name"], "");
    assert_eq!(row(&n, "C-1").await["aliases"], json!([]));
    for selector in [
        r#"id: "C-999", type: "Counter""#,
        r#"id: "C-1", type: "Other""#,
        r#"id: "C-1", name: "wrong""#,
    ] {
        let failed = run(&n, &format!(r#"UPSERT CONCEPT ?c {{ MATCH {{{selector}, key: "counter"}} SET FIELDS {{name: "wrong"}} }}"#)).await;
        assert_eq!(failed.error.unwrap().code.as_str(), "NotFoundOrNotVisible");
    }
    assert_eq!(row(&n, "C-1").await["name"], "");
    let version = row(&n, "C-1").await["version"].clone();
    let conflicted = run(
        &n,
        r#"UPDATE "C-1" SET ATTRIBUTES {temporary: 1} UNSET ATTRIBUTES {temporary}"#,
    )
    .await;
    assert_eq!(conflicted.status, TopLevelStatus::Failed);
    assert_eq!(row(&n, "C-1").await["version"], version);
}

#[tokio::test]
async fn forward_block_output_reads_are_frozen_and_visible_in_not_and_union() {
    let n = nexus("kml_detail_output_scope").await;
    ok(&n, r#"CREATE CONCEPT ?c { TYPE "Counter" }"#).await;
    ok(
        &n,
        r#"MUTATE {
    UPDATE ?gate SET FIELDS {name: "Changed"} WHERE { FILTER(?gate.name == "Gate") }
    UPDATE ?target SET ATTRIBUTES {selected: true} WHERE {
      ?target CONCEPT {id: "C-1"} FILTER(?gate.name == "Gate")
      NOT { FILTER(?gate.name == "Stop") }
    }
    UPDATE ?target SET ATTRIBUTES {union_seen: true} WHERE {
      ?target CONCEPT {id: "C-1"} FILTER(?gate.name == "Wrong")
      UNION { ?target CONCEPT {id: "C-1"} FILTER(?gate.name == "Gate") }
    }
    UPDATE ?target SET ATTRIBUTES {activity_seen: true} WHERE {
      ?target CONCEPT {id: "C-1"} FILTER(?run.activity_class == "consolidation")
    }
    CREATE CONCEPT ?gate { TYPE "Counter" NAME "Gate" }
    CREATE ACTIVITY ?run { SET FIELDS {activity_class: "consolidation"} }
}"#,
    )
    .await;
    assert_eq!(
        row(&n, "C-1").await["attributes"],
        json!({"selected": true, "union_seen": true, "activity_seen": true})
    );
    assert_eq!(row(&n, "C-2").await["name"], "Changed");
    assert!(anda_kip::parse_kip(r#"UPDATE ?gate SET FIELDS {name: "leaked"}"#).is_err());
}

#[tokio::test]
async fn structural_edge_conflicts_are_order_independent_and_replacements_remain_legal() {
    let n = nexus("kml_detail_structural_conflicts").await;
    ok(&n, r#"MUTATE { CREATE CONCEPT ?a {TYPE "Counter"} CREATE CONCEPT ?b {TYPE "Counter"} CREATE CONCEPT ?c {TYPE "Counter"} }"#).await;
    let add = r#"UPDATE "C-1" SET STRUCTURAL { ("links", "C-2") }"#;
    let remove = r#"UPDATE "C-1" UNSET STRUCTURAL { ("links", "C-2") }"#;
    for command in [
        format!("MUTATE {{ {add} {remove} }}"),
        format!("MUTATE {{ {remove} {add} }}"),
    ] {
        let failed = run(&n, &command).await;
        assert_eq!(
            failed.error.unwrap().code.as_str(),
            "DuplicateMutationTarget"
        );
        assert_eq!(row(&n, "C-1").await["structural"], json!({}));
    }
    ok(
        &n,
        r#"MUTATE {
        UPDATE "C-1" SET STRUCTURAL { ("links", "C-2") {index: 0} }
        UPDATE "C-1" SET STRUCTURAL { ("links", "C-2") {index: 0} }
    }"#,
    )
    .await;
    ok(&n, r#"UPDATE "C-1" SET STRUCTURAL { ("primary", "C-2") }"#).await;
    ok(&n, r#"UPDATE "C-1" SET STRUCTURAL { ("primary", "C-3") } UNSET STRUCTURAL { ("primary", "C-2") }"#).await;
    assert_eq!(
        row(&n, "C-1").await["structural"]["kip://test/kml-detail@1.0.0/primary"],
        json!([{"id":"C-3"}])
    );
    let version = row(&n, "C-1").await["version"].clone();
    no_effect(&ok(&n, r#"UPDATE ?c SET FACET "Numeric" {count: ADD(?c.attributes.missing, 1)} WHERE {?c CONCEPT {id: "C-1"}}"#).await);
    assert_eq!(row(&n, "C-1").await["facets"], json!({}));
    assert_eq!(row(&n, "C-1").await["version"], version);
}

#[tokio::test]
async fn a_new_concept_can_be_archived_through_its_forward_output_handle() {
    let n = nexus("kml_detail_new_concept_archive").await;
    ok(&n, r#"CREATE CONCEPT ?old {TYPE "Counter" NAME "Kept"}"#).await;
    ok(
        &n,
        r#"MUTATE {
        TRANSITION ?fresh TO "archived" WHERE {FILTER(?fresh.name == "New")}
        CREATE CONCEPT ?fresh {TYPE "Counter" NAME "New"}
    }"#,
    )
    .await;
    assert_eq!(row(&n, "C-2").await["state"], "archived");
    assert_eq!(row(&n, "C-2").await["version"], 1);
    assert_eq!(row(&n, "C-1").await["state"], "active");
    let active = ok(&n, r#"FIND(?c.name) WHERE {?c CONCEPT {type: "Counter"}}"#).await;
    assert_eq!(active.first_result().unwrap(), &json!(["Kept"]));
    let archived = ok(
        &n,
        r#"FIND(?c.name) WHERE {?c CONCEPT {id: "C-2", state: "archived"}}"#,
    )
    .await;
    assert_eq!(archived.first_result().unwrap(), &json!(["New"]));
}
