//! Migrating a KIP 1.x database into KIP 2.0, on the shape 1.x actually wrote.
//!
//! The fixture is built from the 1.x row layout rather than from a mirrored
//! struct: `concepts` with `type`/`name`/`attributes`/`metadata`, and
//! `propositions` with `subject`/`object`/`predicates`/`properties`, endpoints
//! spelled `C:{id}` and `P:{id}:{predicate}` the way 1.x `EntityID` displayed
//! them. If those strings are wrong the test proves nothing, so they are
//! written out here rather than derived from anything current.

use anda_cognitive_nexus::{
    CognitiveNexus,
    migrate::LEGACY_STAGING,
    nexus::DEFAULT_SPACE,
    schema::{PackageState, SchemaLock, SchemaPackage},
};
use anda_db::{
    collection::CollectionConfig,
    database::{AndaDB, DBConfig},
    schema::{AndaDBSchema, Json},
};
use anda_kip::{Executor, Request, TopLevelStatus};
use object_store::memory::InMemory;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

const COGNITIVE_MEMORY: &str = anda_cognitive_nexus::profiles::COGNITIVE_MEMORY;
const PROFILE_ID: &str = "kip://profiles/cognitive-memory";

/// The 1.x Concept row, as `20a2647` defined it.
#[derive(Clone, Debug, Deserialize, Serialize, AndaDBSchema)]
struct V1Concept {
    _id: u64,
    #[field_type = "Text"]
    r#type: String,
    #[field_type = "Text"]
    name: String,
    #[field_type = "Json"]
    attributes: Json,
    #[field_type = "Json"]
    metadata: Json,
}

/// The 1.x Proposition row: one subject, one object, a *set* of predicates.
#[derive(Clone, Debug, Deserialize, Serialize, AndaDBSchema)]
struct V1Proposition {
    _id: u64,
    #[field_type = "Text"]
    subject: String,
    #[field_type = "Text"]
    object: String,
    #[field_type = "Json"]
    predicates: Json,
    #[field_type = "Json"]
    properties: Json,
}

/// Writes a database in the 1.x layout and returns the shared object store.
async fn write_v1(name: &str) -> Arc<InMemory> {
    let store = Arc::new(InMemory::new());
    let db = AndaDB::connect(
        store.clone(),
        DBConfig {
            name: name.to_string(),
            description: "a KIP 1.x database".to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let concepts = db
        .open_or_create_collection(
            V1Concept::schema().unwrap(),
            CollectionConfig {
                name: "concepts".to_string(),
                description: "Concept nodes".to_string(),
            },
            async |c| {
                c.create_btree_index_nx(&["type"]).await?;
                c.create_btree_index_nx(&["name"]).await?;
                Ok(())
            },
        )
        .await
        .unwrap();

    for concept in [
        V1Concept {
            _id: 0,
            r#type: "Person".to_string(),
            name: "Alice".to_string(),
            attributes: json!({"display_name": "Alice A"}),
            metadata: json!({"access_level": "private", "author": "importer"}),
        },
        V1Concept {
            _id: 0,
            r#type: "Preference".to_string(),
            name: "Dark mode".to_string(),
            attributes: json!({}),
            metadata: json!({}),
        },
        // A type the cognitive-memory profile has never heard of. This is the
        // row that decides whether the legacy package is real.
        V1Concept {
            _id: 0,
            r#type: "Spaceship".to_string(),
            name: "Serenity".to_string(),
            attributes: json!({"crew": 9}),
            metadata: json!({}),
        },
    ] {
        concepts.add_from(&concept).await.unwrap();
    }
    concepts.flush(now_ms()).await.unwrap();

    let propositions = db
        .open_or_create_collection(
            V1Proposition::schema().unwrap(),
            CollectionConfig {
                name: "propositions".to_string(),
                description: "Proposition links".to_string(),
            },
            async |c| {
                c.create_btree_index_nx(&["subject"]).await?;
                Ok(())
            },
        )
        .await
        .unwrap();

    // Two predicates on one row: 1.x's multi-predicate edge, which has to fan
    // out into two independent 2.0 tuples with their own confidences.
    let edge = propositions
        .add_from(&V1Proposition {
            _id: 0,
            subject: "C:1".to_string(),
            object: "C:2".to_string(),
            predicates: json!(["prefers", "mentions"]),
            properties: json!({
                "prefers": {"attributes": {}, "metadata": {"confidence": 0.9}},
                "mentions": {"attributes": {}, "metadata": {"confidence": 0.25}},
            }),
        })
        .await
        .unwrap();
    // A higher-order reference: the subject is one *predicate* of that row,
    // which is the tuple its fan-out produces rather than the row itself.
    propositions
        .add_from(&V1Proposition {
            _id: 0,
            subject: format!("P:{edge}:prefers"),
            object: "C:3".to_string(),
            predicates: json!(["noted_by"]),
            properties: json!({"noted_by": {"attributes": {}, "metadata": {}}}),
        })
        .await
        .unwrap();
    propositions.flush(now_ms()).await.unwrap();
    db.close().await.unwrap();
    store
}

/// Opens the 2.0 engine over an existing object store, as a restart would.
async fn open_v2(store: Arc<InMemory>, name: &str) -> CognitiveNexus {
    let db = AndaDB::connect(
        store,
        DBConfig {
            name: name.to_string(),
            description: "a KIP 2.0 database".to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let nexus = CognitiveNexus::connect(Arc::new(db)).await.unwrap();
    // What a host does on every start; the migration must survive it.
    nexus
        .install_package(&SchemaPackage::parse(COGNITIVE_MEMORY).unwrap(), "test")
        .await
        .unwrap();
    let mut lock = SchemaLock::default();
    lock.packages
        .insert(PROFILE_ID.to_string(), "2.0.0".to_string());
    lock.states
        .insert(PROFILE_ID.to_string(), PackageState::Active);
    nexus.ensure_schema(DEFAULT_SPACE, lock).await.unwrap();
    nexus
}

async fn query(nexus: &CognitiveNexus, command: &str) -> Json {
    let request = Request::single(command);
    let parsed = anda_kip::parse_kip(command).unwrap();
    let response = nexus
        .execute(parsed, &request, &request.operations[0])
        .await;
    assert_eq!(
        response.status,
        TopLevelStatus::Succeeded,
        "{command}\n{:#?}",
        response.error
    );
    response.first_result().cloned().unwrap_or(Json::Null)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[tokio::test]
async fn a_1_x_database_migrates_on_the_first_2_0_start() {
    let store = write_v1("migrate_basic").await;
    let nexus = open_v2(store, "migrate_basic").await;

    // Every Concept came across, including the type the standard profile has
    // never heard of — which is what the generated legacy package is for.
    let names = query(
        &nexus,
        r#"FIND(?c.name) WHERE { ?c CONCEPT {} } ORDER BY ?c.name"#,
    )
    .await;
    let names = names.as_array().unwrap();
    for expected in ["Alice", "Dark mode", "Serenity"] {
        assert!(
            names.iter().any(|n| n == expected),
            "{expected} missing from {names:?}"
        );
    }

    // The 1.x type survived as an exact symbol rather than being dropped.
    let serenity = query(
        &nexus,
        r#"FIND(?c.schema_ref) WHERE { ?c CONCEPT {name: "Serenity"} }"#,
    )
    .await;
    assert_eq!(
        serenity,
        json!(["kip://legacy/nexus@1.1.0/Spaceship"]),
        "the legacy type must resolve to a real package symbol"
    );

    // The Profile has no Preference type (Profile §5.5): a 1.x Preference
    // keeps its own symbol in the legacy package rather than being retyped
    // as a claim the actor never made.
    let dark = query(
        &nexus,
        r#"FIND(?c.schema_ref) WHERE { ?c CONCEPT {name: "Dark mode"} }"#,
    )
    .await;
    assert_eq!(dark, json!(["kip://legacy/nexus@1.1.0/Preference"]));

    // The multi-predicate row fanned out, and each tuple kept its own
    // confidence — the reason the fan-out cannot be collapsed.
    let claims = query(
        &nexus,
        r#"FIND(?a.confidence) WHERE { ?a ASSERTION {} } ORDER BY ?a.confidence"#,
    )
    .await;
    let claims = claims.as_array().unwrap();
    assert!(
        claims.contains(&json!(0.9)) && claims.contains(&json!(0.25)),
        "both legacy confidences must survive: {claims:?}"
    );

    // Carried in from another system, and it says so.
    let modes = query(&nexus, r#"FIND(?a.mode) WHERE { ?a ASSERTION {} }"#).await;
    for mode in modes.as_array().unwrap() {
        assert_eq!(mode, "imported");
    }

    // The higher-order reference resolved to the tuple the fan-out produced,
    // not to the row it came from.
    let higher_order = query(
        &nexus,
        r#"FIND(COUNT(?p)) WHERE { ?p PROPOSITION (?s, "kip://legacy/nexus@1.1.0/noted_by", ?o) }"#,
    )
    .await;
    assert_eq!(higher_order, json!([1]));

    // 1.x metadata was preserved rather than promoted: `access_level`
    // annotated where 2.0 classification enforces, so guessing would either
    // over- or under-protect.
    let legacy = query(
        &nexus,
        r#"FIND(?c.attributes.legacy.metadata.access_level) WHERE { ?c CONCEPT {name: "Alice"} }"#,
    )
    .await;
    assert_eq!(legacy, json!(["private"]));

    // What arrives is a 2.0 element, not a 1.x row wearing a new name: §6.3's
    // version planes have to be there from the first read, or every
    // `EXPECT VERSION ... OF <plane>` a migrated Space serves would compare
    // against a counter that was never initialized. A migrated Concept has
    // attributes and no structural references, so §35.2 wants exactly one
    // plane at 1 and the rest at 0.
    let planes = query(
        &nexus,
        r#"FIND(?c._system.version, ?c._system.plane_versions)
           WHERE { ?c CONCEPT {name: "Alice"} }"#,
    )
    .await;
    let row = &planes.as_array().unwrap()[0];
    assert_eq!(row[0], json!(1), "a migrated element starts at version 1");
    assert_eq!(row[1]["attributes"], json!(1));
    assert_eq!(row[1]["structural"], json!(0));
    assert_eq!(row[1]["retention"], json!(0));

    // And §31.4's floor is reported rather than left as a hole for a client to
    // fill in: nothing carried in from 1.x may influence behaviour until this
    // Space's Governance says so.
    let ceiling = query(
        &nexus,
        r#"FIND(?c.governance.authority_class) WHERE { ?c CONCEPT {name: "Alice"} }"#,
    )
    .await;
    assert_eq!(ceiling, json!(["descriptive"]));
}

#[tokio::test]
async fn a_second_start_migrates_nothing_further() {
    let store = write_v1("migrate_twice").await;
    let nexus = open_v2(store.clone(), "migrate_twice").await;
    let before = query(&nexus, r#"FIND(COUNT(?c)) WHERE { ?c CONCEPT {} }"#).await;
    let claims_before = query(&nexus, r#"FIND(COUNT(?a)) WHERE { ?a ASSERTION {} }"#).await;
    nexus.close().await.unwrap();

    // The restart a host actually performs.
    let nexus = open_v2(store, "migrate_twice").await;
    let after = query(&nexus, r#"FIND(COUNT(?c)) WHERE { ?c CONCEPT {} }"#).await;
    let claims_after = query(&nexus, r#"FIND(COUNT(?a)) WHERE { ?a ASSERTION {} }"#).await;

    assert_eq!(before, after, "a restart must not duplicate Concepts");
    assert_eq!(
        claims_before, claims_after,
        "a restart must not duplicate Assertions"
    );
}

#[tokio::test]
async fn the_1_x_rows_are_kept_after_migrating() {
    let store = write_v1("migrate_keeps").await;
    let nexus = open_v2(store, "migrate_keeps").await;
    // Three concepts, two proposition rows, plus extraction and completion markers: the
    // original is still there to be read in the shape it was stored in.
    let staged = nexus
        .store
        .db
        .open_collection(LEGACY_STAGING.to_string(), async |_| Ok(()))
        .await
        .unwrap();
    assert_eq!(staged.len(), 7);
}

#[tokio::test]
async fn a_fresh_2_0_database_migrates_nothing() {
    let store = Arc::new(InMemory::new());
    let nexus = open_v2(store, "migrate_fresh").await;
    assert!(
        !nexus
            .store
            .db
            .metadata()
            .collections
            .contains(LEGACY_STAGING),
        "nothing should be staged for a database that was never 1.x"
    );
}

#[tokio::test]
async fn an_interrupted_extract_is_redone_rather_than_resumed_half_way() {
    // The dangerous window is between "some rows copied" and "the 1.x
    // collections dropped": the source is still authoritative there, so a
    // partial staging area must be discarded rather than trusted. Simulated by
    // staging a bogus row while the 1.x layout is still in place.
    let store = write_v1("migrate_interrupted").await;
    {
        let db = AndaDB::connect(
            store.clone(),
            DBConfig {
                name: "migrate_interrupted".to_string(),
                description: "interrupted".to_string(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let staging = db
            .open_or_create_collection(
                anda_cognitive_nexus::migrate::LegacyRow::schema().unwrap(),
                CollectionConfig {
                    name: LEGACY_STAGING.to_string(),
                    description: "half-written".to_string(),
                },
                async |c| {
                    c.create_btree_index_nx(&["kind"]).await?;
                    c.create_btree_index_nx(&["legacy_id"]).await?;
                    Ok(())
                },
            )
            .await
            .unwrap();
        staging
            .add_from(&anda_cognitive_nexus::migrate::LegacyRow {
                _id: 0,
                kind: "concept".to_string(),
                legacy_id: 999,
                doc: json!({"_id": 999, "type": "Ghost", "name": "Not a real row"}),
            })
            .await
            .unwrap();
        staging.flush(now_ms()).await.unwrap();
        db.close().await.unwrap();
    }

    let nexus = open_v2(store, "migrate_interrupted").await;
    // The half-written row was discarded with the rest of the staging area, so
    // it never became a Concept.
    let ghost = query(
        &nexus,
        r#"FIND(COUNT(?c)) WHERE { ?c CONCEPT {name: "Not a real row"} }"#,
    )
    .await;
    assert_eq!(ghost, json!([0]));
    // And the real rows still arrived.
    let alice = query(
        &nexus,
        r#"FIND(COUNT(?c)) WHERE { ?c CONCEPT {name: "Alice"} }"#,
    )
    .await;
    assert_eq!(alice, json!([1]));
}

/// Opens the raw database without migrating, as a dry run does.
async fn open_raw(store: Arc<InMemory>, name: &str) -> Arc<AndaDB> {
    Arc::new(
        AndaDB::connect(
            store,
            DBConfig {
                name: name.to_string(),
                description: "dry run".to_string(),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
    )
}

#[tokio::test]
async fn a_dry_run_reports_the_plan_and_writes_nothing() {
    let store = write_v1("migrate_dry_run").await;
    let db = open_raw(store.clone(), "migrate_dry_run").await;

    let plan = anda_cognitive_nexus::migrate::plan(&db)
        .await
        .unwrap()
        .expect("a 1.x database has a plan");

    assert_eq!(plan.concepts, 3);
    assert_eq!(plan.proposition_rows, 2);
    // The fan-out: two predicates on one row plus one on the other.
    assert_eq!(plan.propositions, 3);
    assert_eq!(plan.assertions, 3);
    assert!(plan.concept_types.contains("Spaceship"));
    assert!(plan.predicates.contains("prefers"));
    assert!(plan.predicates.contains("noted_by"));
    assert!(plan.is_runnable(), "blockers: {:?}", plan.blockers);

    // The inventory that makes §13/§21 actionable for *this* deployment.
    let confidence = plan.confidence.expect("the fixture carries confidence");
    assert_eq!(confidence.count, 2);
    assert_eq!(confidence.min, 0.25);
    assert_eq!(confidence.max, 0.9);
    assert_eq!(plan.access_levels.get("private"), Some(&1));

    // The report is readable rather than a struct dump.
    let rendered = plan.to_string();
    assert!(rendered.contains("nothing has been written"));
    assert!(rendered.contains("No blockers"));

    // And it left no trace: no staging, and the 1.x layout is untouched.
    assert!(
        !db.metadata().collections.contains(LEGACY_STAGING),
        "a dry run must not stage"
    );
    let concepts = db
        .open_collection("concepts".to_string(), async |_| Ok(()))
        .await
        .unwrap();
    assert_eq!(concepts.len(), 3);
    assert!(
        concepts.schema().get_field("type").is_some(),
        "the 1.x schema must still be in force after a dry run"
    );
    db.close().await.unwrap();

    // The real migration still runs afterwards, unaffected by having been asked.
    let nexus = open_v2(store, "migrate_dry_run").await;
    let count = query(&nexus, r#"FIND(COUNT(?c)) WHERE { ?c CONCEPT {} }"#).await;
    assert_eq!(count, json!([4]), "3 migrated concepts plus the actor");
}

#[tokio::test]
async fn a_dry_run_names_what_would_block_it() {
    let store = Arc::new(InMemory::new());
    let db = AndaDB::connect(
        store.clone(),
        DBConfig {
            name: "migrate_blocked".to_string(),
            description: "a broken KIP 1.x database".to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let concepts = db
        .open_or_create_collection(
            V1Concept::schema().unwrap(),
            CollectionConfig {
                name: "concepts".to_string(),
                description: "Concept nodes".to_string(),
            },
            async |c| {
                c.create_btree_index_nx(&["type"]).await?;
                Ok(())
            },
        )
        .await
        .unwrap();
    // A row with no type has no symbol to resolve to.
    concepts
        .add_from(&V1Concept {
            _id: 0,
            r#type: String::new(),
            name: "Untyped".to_string(),
            attributes: json!({}),
            metadata: json!({}),
        })
        .await
        .unwrap();
    let propositions = db
        .open_or_create_collection(
            V1Proposition::schema().unwrap(),
            CollectionConfig {
                name: "propositions".to_string(),
                description: "Proposition links".to_string(),
            },
            async |c| {
                c.create_btree_index_nx(&["subject"]).await?;
                Ok(())
            },
        )
        .await
        .unwrap();
    // An endpoint nothing provides.
    propositions
        .add_from(&V1Proposition {
            _id: 0,
            subject: "C:999".to_string(),
            object: "C:1".to_string(),
            predicates: json!(["dangles"]),
            properties: json!({}),
        })
        .await
        .unwrap();
    concepts.flush(now_ms()).await.unwrap();
    propositions.flush(now_ms()).await.unwrap();

    let db = Arc::new(db);
    let plan = anda_cognitive_nexus::migrate::plan(&db)
        .await
        .unwrap()
        .unwrap();
    assert!(!plan.is_runnable());
    let blockers = plan.blockers.join("\n");
    assert!(blockers.contains("has no type"), "{blockers}");
    assert!(blockers.contains("C:999"), "{blockers}");
    assert!(plan.to_string().contains("blocker(s)"));
}

#[tokio::test]
async fn a_dry_run_on_a_migrated_database_has_nothing_to_report() {
    let store = write_v1("migrate_dry_after").await;
    let nexus = open_v2(store.clone(), "migrate_dry_after").await;
    nexus.close().await.unwrap();

    let db = open_raw(store, "migrate_dry_after").await;
    assert!(
        anda_cognitive_nexus::migrate::plan(&db)
            .await
            .unwrap()
            .is_none(),
        "a migrated database has no outstanding plan"
    );
}

#[tokio::test]
async fn an_unambiguous_legacy_author_becomes_the_speaker() {
    // §12: a legacy `author` may be a speaker, a writer application, or
    // bookkeeping. Mapping it is justified only when it names exactly one
    // Concept — then the old system really did record who said it.
    let store = Arc::new(InMemory::new());
    let db = AndaDB::connect(
        store.clone(),
        DBConfig {
            name: "migrate_author".to_string(),
            description: "a KIP 1.x database".to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let concepts = db
        .open_or_create_collection(
            V1Concept::schema().unwrap(),
            CollectionConfig {
                name: "concepts".to_string(),
                description: "Concept nodes".to_string(),
            },
            async |c| {
                c.create_btree_index_nx(&["name"]).await?;
                Ok(())
            },
        )
        .await
        .unwrap();
    for (name, kind) in [("Alice", "Person"), ("Dark", "Preference")] {
        concepts
            .add_from(&V1Concept {
                _id: 0,
                r#type: kind.to_string(),
                name: name.to_string(),
                attributes: json!({}),
                metadata: json!({}),
            })
            .await
            .unwrap();
    }
    let propositions = db
        .open_or_create_collection(
            V1Proposition::schema().unwrap(),
            CollectionConfig {
                name: "propositions".to_string(),
                description: "Proposition links".to_string(),
            },
            async |c| {
                c.create_btree_index_nx(&["subject"]).await?;
                Ok(())
            },
        )
        .await
        .unwrap();
    propositions
        .add_from(&V1Proposition {
            _id: 0,
            subject: "C:1".to_string(),
            object: "C:2".to_string(),
            predicates: json!(["prefers", "mentions"]),
            properties: json!({
                // Names exactly one Concept: a speaker the old system recorded.
                "prefers": {"attributes": {}, "metadata": {"author": "Alice"}},
                // Names nothing: stays the migration actor.
                "mentions": {"attributes": {}, "metadata": {"author": "some-importer-v3"}},
            }),
        })
        .await
        .unwrap();
    concepts.flush(now_ms()).await.unwrap();
    propositions.flush(now_ms()).await.unwrap();
    db.close().await.unwrap();

    let nexus = open_v2(store, "migrate_author").await;
    let speakers = query(
        &nexus,
        r#"FIND(?who.name) WHERE { ?a ASSERTION {asserted_by: ?who} } ORDER BY ?who.name"#,
    )
    .await;
    let speakers = speakers.as_array().unwrap();
    assert!(
        speakers.contains(&json!("Alice")),
        "a resolvable author becomes the speaker: {speakers:?}"
    );
    assert!(
        speakers.contains(&json!("KIP 1.x migration")),
        "an unresolvable one stays the migration actor: {speakers:?}"
    );
}

#[tokio::test]
async fn staged_inventory_includes_more_than_search_limit_per_kind() {
    use anda_cognitive_nexus::migrate::{self, LEGACY_STAGING, LegacyRow};
    use anda_db::{
        collection::{Collection, CollectionConfig},
        database::{AndaDB, DBConfig},
    };
    use object_store::memory::InMemory;
    use serde_json::json;
    use std::sync::Arc;

    let db = Arc::new(
        AndaDB::connect(Arc::new(InMemory::new()), DBConfig::default())
            .await
            .unwrap(),
    );
    let staging = db
        .open_or_create_collection(
            LegacyRow::schema().unwrap(),
            CollectionConfig {
                name: LEGACY_STAGING.into(),
                description: String::new(),
            },
            async |c| {
                c.create_btree_index_nx(&["kind"]).await?;
                Ok(())
            },
        )
        .await
        .unwrap();
    let count = Collection::MAX_SEARCH_LIMIT + 1;
    for i in 1..=count {
        for (kind, doc) in [
            (
                "concept",
                json!({"_id": i, "type": "Person", "name": format!("person-{i}"), "attributes": {}, "metadata": {}}),
            ),
            (
                "proposition",
                json!({"_id": i, "subject": format!("C:{i}"), "object": "C:1", "predicates": ["knows"], "properties": {"knows": {"a": {}, "m": {}}}}),
            ),
        ] {
            staging
                .add_from(&LegacyRow {
                    _id: 0,
                    kind: kind.into(),
                    legacy_id: i as u64,
                    doc,
                })
                .await
                .unwrap();
        }
    }
    staging.flush(1).await.unwrap();
    let plan = migrate::plan(&db).await.unwrap().unwrap();
    assert_eq!(plan.concepts, count);
    assert_eq!(plan.proposition_rows, count);
    assert_eq!(plan.propositions, count);
    assert_eq!(plan.assertions, count);
    assert!(plan.blockers.is_empty(), "{:?}", plan.blockers);
    db.close().await.unwrap();
}

#[tokio::test]
async fn migration_preserves_large_collections_through_restart() {
    let name = "large_legacy";
    let store = write_v1(name).await;
    let db = open_raw(store.clone(), name).await;
    let concepts = db
        .open_collection("concepts".into(), async |_| Ok(()))
        .await
        .unwrap();
    let propositions = db
        .open_collection("propositions".into(), async |_| Ok(()))
        .await
        .unwrap();
    for i in 0..=anda_db::collection::Collection::MAX_SEARCH_LIMIT {
        let id = concepts
            .add_from(&V1Concept {
                _id: 0,
                r#type: "Topic".into(),
                name: format!("bulk-{i}"),
                attributes: json!({}),
                metadata: json!({}),
            })
            .await
            .unwrap();
        propositions
            .add_from(&V1Proposition {
                _id: 0,
                subject: "C:1".into(),
                object: format!("C:{id}"),
                predicates: json!(["bulk_link"]),
                properties: json!({"bulk_link":{"a":{},"m":{}}}),
            })
            .await
            .unwrap();
    }
    let expected = (concepts.len(), propositions.len());
    let tuples = anda_cognitive_nexus::migrate::plan(&db)
        .await
        .unwrap()
        .unwrap()
        .propositions;
    db.close().await.unwrap();
    drop(db);
    let nexus = open_v2(store.clone(), name).await;
    assert_eq!(nexus.store.concepts().len(), expected.0 + 1); // migration actor
    assert_eq!(nexus.store.propositions().len(), tuples);
    assert_eq!(nexus.store.assertions().len(), tuples);
    let staging = nexus
        .store
        .db
        .open_collection(LEGACY_STAGING.into(), async |_| Ok(()))
        .await
        .unwrap();
    assert_eq!(staging.len(), expected.0 + expected.1 + 2); // extracted + complete
    nexus.close().await.unwrap();
    drop(nexus);
    let nexus = open_v2(store, name).await;
    assert_eq!(nexus.store.concepts().len(), expected.0 + 1);
    assert_eq!(nexus.store.propositions().len(), tuples);
    assert_eq!(nexus.store.assertions().len(), tuples);
    nexus.close().await.unwrap();
}

#[tokio::test]
async fn legacy_task_execution_states_are_preserved_without_fabricated_leases() {
    let name = "legacy_task_states";
    let store = write_v1(name).await;
    let db = open_raw(store.clone(), name).await;
    let concepts = db
        .open_collection("concepts".into(), async |_| Ok(()))
        .await
        .unwrap();
    let mut originals = std::collections::BTreeMap::new();
    for status in [
        "pending",
        "running",
        "completed",
        "failed",
        "cancelled",
        "blocked",
    ] {
        let id = concepts.add_from(&V1Concept {
            _id: 0, r#type: "SleepTask".into(), name: format!("task-{status}"),
            attributes: json!({"status":status,"description":"Historical task","result":{"note":"original result"}}),
            metadata: json!({}),
        }).await.unwrap();
        originals.insert(
            format!("kip:migrate:v1:C:{id}"),
            concepts.get_as::<Json>(id).await.unwrap(),
        );
    }
    db.close().await.unwrap();
    drop(db);
    for _ in 0..2 {
        let nexus = open_v2(store.clone(), name).await;
        let mut seen = 0;
        for id in nexus.store.concepts().ids() {
            let row: Json = nexus.store.concepts().get_as(id).await.unwrap();
            let Some(original) = row["client_key"]
                .as_str()
                .and_then(|key| originals.get(key))
            else {
                continue;
            };
            seen += 1;
            let before = original["attributes"]["status"].as_str().unwrap();
            let expected = if ["running", "completed", "failed"].contains(&before) {
                "blocked"
            } else {
                before
            };
            assert_eq!(row["attributes"]["status"], expected);
            assert_eq!(
                &row["facets"]["kip://legacy/nexus@1.1.0/LegacyRecord"]["record"],
                original
            );
            assert!(
                row["facets"]
                    .get("kip://profiles/cognitive-memory@2.0.0/LeaseState")
                    .is_none()
            );
        }
        assert_eq!(seen, originals.len());
        nexus.close().await.unwrap();
    }
}

#[tokio::test]
async fn completed_commitments_migrate_and_old_untouched_rows_are_repaired_once() {
    use anda_db::schema::Fv;
    let name = "completed_commitment_repair";
    let store = write_v1(name).await;
    let db = open_raw(store.clone(), name).await;
    let concepts = db
        .open_collection("concepts".into(), async |_| Ok(()))
        .await
        .unwrap();
    for label in ["untouched", "edited", "archived"] {
        concepts
            .add_from(&V1Concept {
                _id: 0,
                r#type: "Commitment".into(),
                name: label.into(),
                attributes: json!({"status":"completed","description":"Delivered the report"}),
                metadata: if label == "archived" {
                    json!({"expires_at":"2020-01-01T00:00:00Z"})
                } else {
                    json!({})
                },
            })
            .await
            .unwrap();
    }
    db.close().await.unwrap();
    let nexus = open_v2(store.clone(), name).await;
    let mut ids = std::collections::BTreeMap::new();
    for id in nexus.store.concepts().ids() {
        let row: Json = nexus.store.concepts().get_as(id).await.unwrap();
        if !["untouched", "edited", "archived"].contains(&row["name"].as_str().unwrap_or("")) {
            continue;
        }
        assert_eq!(row["attributes"]["status"], "fulfilled");
        assert_eq!(
            row["facets"]["kip://legacy/nexus@1.1.0/LegacyRecord"]["record"]["attributes"]["status"],
            "completed"
        );
        ids.insert(row["name"].as_str().unwrap().to_string(), id);
        // Reproduce 0.13.3's stored state, including the attributes plane version.
        let mut attrs = row["attributes"].clone();
        attrs["status"] = json!("blocked");
        nexus
            .store
            .concepts()
            .update(
                id,
                std::collections::BTreeMap::from([(
                    "attributes".into(),
                    Fv::from(attrs.as_object().unwrap().clone()),
                )]),
            )
            .await
            .unwrap();
    }
    query(
        &nexus,
        &format!(
            r#"UPDATE "C-{}" SET ATTRIBUTES {{summary: "Later user edit"}}"#,
            ids["edited"]
        ),
    )
    .await;
    let staging = nexus
        .store
        .db
        .open_collection(LEGACY_STAGING.into(), async |_| Ok(()))
        .await
        .unwrap();
    staging
        .remove_extension("completed_commitments_repaired_v1")
        .await
        .unwrap();
    nexus.close().await.unwrap();
    drop(nexus);
    for _ in 0..2 {
        let nexus = open_v2(store.clone(), name).await;
        for (name, id) in &ids {
            let row: Json = nexus.store.concepts().get_as(*id).await.unwrap();
            assert_eq!(
                row["attributes"]["status"],
                if name == "edited" {
                    "blocked"
                } else {
                    "fulfilled"
                }
            );
            if name == "archived" {
                assert_eq!(row["state"], "archived");
            }
            if name != "edited" {
                assert_eq!(row["plane_versions"]["attributes"], 2);
            }
        }
        nexus.close().await.unwrap();
    }
}
