//! End-to-end KQL tests: real command text, real parser, real database.
//!
//! The fixture is one small memory written through KML, so every read is a
//! read of something the engine actually stored rather than of a hand-built
//! row.

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

async fn nexus(name: &str) -> CognitiveNexus {
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: name.to_string(),
            description: "kql tests".to_string(),
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
        .insert(PROFILE_ID.to_string(), "2.0.0".to_string());
    lock.states
        .insert(PROFILE_ID.to_string(), PackageState::Active);
    nexus.activate_schema(DEFAULT_SPACE, lock).await.unwrap();
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

/// Two people, two preferences, three claims with different stances.
async fn seeded(name: &str) -> CognitiveNexus {
    let nexus = nexus(name).await;
    ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" SET ATTRIBUTES {display_name: "Alice A"} }
            CREATE CONCEPT ?bob { TYPE "Person" NAME "Bob" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark mode" }
            CREATE CONCEPT ?light { TYPE "Preference" NAME "Light mode" }
            ENSURE PROPOSITION ?p1 (?alice, "prefers", ?dark)
            ENSURE PROPOSITION ?p2 (?bob, "prefers", ?light)
            CREATE ASSERTION ?a1 {
                SET FIELDS {proposition: ?p1, asserted_by: ?alice, stance: "support", mode: "stated", confidence: 0.9}
            }
            CREATE ASSERTION ?a2 {
                SET FIELDS {proposition: ?p1, asserted_by: ?bob, stance: "reject", mode: "inferred", confidence: 0.4}
            }
            CREATE ASSERTION ?a3 {
                SET FIELDS {proposition: ?p2, asserted_by: ?bob, stance: "support", mode: "stated", confidence: 0.6}
            }
        }"#,
    )
    .await;
    nexus
}

fn rows(result: &Json) -> &Vec<Json> {
    result.as_array().expect("a KQL result is an array")
}

#[tokio::test]
async fn a_concept_pattern_finds_by_type_and_projects_dot_paths() {
    let nexus = seeded("concepts").await;
    let result = ok(
        &nexus,
        r#"FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} } ORDER BY ?c.name"#,
    )
    .await;
    assert_eq!(rows(&result), &vec![json!("Alice"), json!("Bob")]);

    // A local type name resolves to the exact symbol before it reaches the
    // index, so writing the canonical form finds the same Concepts.
    let qualified = ok(
        &nexus,
        r#"FIND(?c.name) WHERE { ?c CONCEPT {type: "kip://profiles/cognitive-memory@2.0.0/Person"} }
           ORDER BY ?c.name"#,
    )
    .await;
    assert_eq!(rows(&qualified).len(), 2);

    // An attribute reached by dot path.
    let attribute = ok(
        &nexus,
        r#"FIND(?c.attributes.display_name) WHERE { ?c CONCEPT {name: "Alice"} }"#,
    )
    .await;
    assert_eq!(rows(&attribute), &vec![json!("Alice A")]);
}

#[tokio::test]
async fn a_bare_variable_projects_the_whole_element() {
    // An Agent that writes `FIND(?c)` wants the Concept, not its id string.
    let nexus = seeded("whole_element").await;
    let result = ok(&nexus, r#"FIND(?c) WHERE { ?c CONCEPT {name: "Alice"} }"#).await;
    let concept = &rows(&result)[0];
    assert_eq!(concept["kind"], "concept");
    assert_eq!(concept["name"], "Alice");
    assert_eq!(concept["_system"]["version"], 1);
    assert_eq!(
        concept["schema_ref"],
        "kip://profiles/cognitive-memory@2.0.0/Person"
    );
}

#[tokio::test]
async fn a_tuple_pattern_binds_both_ends_and_joins_on_them() {
    let nexus = seeded("tuples").await;
    let result = ok(
        &nexus,
        r#"FIND(?person.name, ?thing.name)
           WHERE {
             ?p PROPOSITION (?person, "prefers", ?thing)
           }
           ORDER BY ?person.name"#,
    )
    .await;
    assert_eq!(
        rows(&result),
        &vec![json!(["Alice", "Dark mode"]), json!(["Bob", "Light mode"])]
    );

    // Joining a tuple against a Concept pattern narrows through the shared
    // variable rather than cross-producting.
    let joined = ok(
        &nexus,
        r#"FIND(?thing.name)
           WHERE {
             ?person CONCEPT {name: "Alice"}
             ?p PROPOSITION (?person, "prefers", ?thing)
           }"#,
    )
    .await;
    assert_eq!(rows(&joined), &vec![json!("Dark mode")]);
}

#[tokio::test]
async fn an_assertion_pattern_reads_claims_not_beliefs() {
    // Spec §2.1. Three Assertions about two Propositions, including one that
    // rejects what another supports — the engine reports both, because a
    // memory that hides disagreement cannot report it.
    let nexus = seeded("assertions").await;
    let result = ok(
        &nexus,
        r#"FIND(?a.stance, ?a.confidence)
           WHERE { ?a ASSERTION {} }
           ORDER BY ?a.confidence DESC"#,
    )
    .await;
    assert_eq!(
        rows(&result),
        &vec![
            json!(["support", 0.9]),
            json!(["support", 0.6]),
            json!(["reject", 0.4])
        ]
    );

    // The two competing claims about one tuple are both on record.
    let contested = ok(
        &nexus,
        r#"FIND(?a.stance)
           WHERE {
             ?person CONCEPT {name: "Alice"}
             ?p PROPOSITION (?person, "prefers", ?thing)
             ?a ASSERTION {proposition: ?p}
           }
           ORDER BY ?a.stance"#,
    )
    .await;
    assert_eq!(rows(&contested), &vec![json!("reject"), json!("support")]);
}

#[tokio::test]
async fn a_raw_read_and_a_projection_answer_different_questions() {
    // The two competing Assertions about Alice's preference are both on
    // record, so the raw read reports two rows. The projection reports one
    // contested belief. Neither is the other, and presenting a raw row as
    // accepted belief is exactly what this version exists to prevent.
    let nexus = seeded("raw_vs_projected").await;
    let raw = ok(
        &nexus,
        r#"FIND(?a.stance)
           WHERE {
             ?person CONCEPT {name: "Alice"}
             ?p PROPOSITION (?person, "prefers", ?thing)
             ?a ASSERTION {proposition: ?p}
           }"#,
    )
    .await;
    assert_eq!(rows(&raw).len(), 2, "two claims on record");

    let projected = ok(
        &nexus,
        r#"FIND(?b.status)
           WHERE {
             ?person CONCEPT {name: "Alice"}
             ?p PROPOSITION (?person, "prefers", ?thing)
             ?b BELIEF (?p)
           }"#,
    )
    .await;
    assert_eq!(rows(&projected), &vec![json!("contested")]);
}

#[tokio::test]
async fn filters_narrow_and_stay_typed() {
    let nexus = seeded("filters").await;
    let strong = ok(
        &nexus,
        r#"FIND(?a.confidence)
           WHERE { ?a ASSERTION {} FILTER(?a.confidence > 0.5) }
           ORDER BY ?a.confidence"#,
    )
    .await;
    assert_eq!(rows(&strong), &vec![json!(0.6), json!(0.9)]);

    let named = ok(
        &nexus,
        r#"FIND(?c.name)
           WHERE { ?c CONCEPT {type: "Person"} FILTER(CONTAINS(?c.name, "li")) }"#,
    )
    .await;
    assert_eq!(rows(&named), &vec![json!("Alice")]);

    let listed = ok(
        &nexus,
        r#"FIND(?a.stance)
           WHERE { ?a ASSERTION {} FILTER(IN(?a.mode, ["inferred", "observed"])) }"#,
    )
    .await;
    assert_eq!(rows(&listed), &vec![json!("reject")]);

    // A comparison between unlike types decides nothing and drops the row,
    // rather than inventing an order out of representation.
    let mistyped = ok(
        &nexus,
        r#"FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} FILTER(?c.name > 5) }"#,
    )
    .await;
    assert!(rows(&mistyped).is_empty());
}

#[tokio::test]
async fn optional_pads_and_not_excludes() {
    let nexus = seeded("optional_not").await;
    // Only Alice has a display_name attribute; Bob must still appear.
    let optional = ok(
        &nexus,
        r#"FIND(?c.name, ?c.attributes.display_name)
           WHERE {
             ?c CONCEPT {type: "Person"}
             OPTIONAL { ?c CONCEPT {attributes: ?attrs} }
           }
           ORDER BY ?c.name"#,
    )
    .await;
    assert_eq!(rows(&optional).len(), 2);

    // NOT asks about the record, never about the world: "no Assertion rejects
    // this" is not "this is true".
    let not_rejected = ok(
        &nexus,
        r#"FIND(?p)
           WHERE {
             ?p PROPOSITION (?s, "prefers", ?o)
             NOT { ?a ASSERTION {proposition: ?p, stance: "reject"} }
           }"#,
    )
    .await;
    assert_eq!(
        rows(&not_rejected).len(),
        1,
        "only Bob's tuple is unopposed"
    );
}

#[tokio::test]
async fn a_union_widens_rather_than_filtering() {
    let nexus = seeded("union").await;
    let result = ok(
        &nexus,
        r#"FIND(?c.name)
           WHERE {
             ?c CONCEPT {name: "Alice"}
             UNION { ?c CONCEPT {name: "Dark mode"} }
           }
           ORDER BY ?c.name"#,
    )
    .await;
    assert_eq!(rows(&result), &vec![json!("Alice"), json!("Dark mode")]);
}

#[tokio::test]
async fn aggregates_answer_over_the_whole_solution_set() {
    let nexus = seeded("aggregates").await;
    assert_eq!(
        ok(&nexus, r#"FIND(COUNT(?a)) WHERE { ?a ASSERTION {} }"#).await,
        json!([3])
    );
    assert_eq!(
        ok(
            &nexus,
            r#"FIND(COUNT(DISTINCT ?a.stance)) WHERE { ?a ASSERTION {} }"#
        )
        .await,
        json!([2])
    );
    let averaged = ok(
        &nexus,
        r#"FIND(AVG(?a.confidence)) WHERE { ?a ASSERTION {} }"#,
    )
    .await;
    let value = rows(&averaged)[0].as_f64().unwrap();
    assert!((value - 0.633_333_3).abs() < 1e-5, "got {value}");
}

#[tokio::test]
async fn ordering_puts_nulls_last_and_paging_is_stable() {
    let nexus = seeded("paging").await;
    // `display_name` is set on Alice only, so Bob's is null and must sort last
    // under ASC — an unbound value is not a small value.
    let ordered = ok(
        &nexus,
        r#"FIND(?c.name)
           WHERE { ?c CONCEPT {type: "Person"} }
           ORDER BY ?c.attributes.display_name ASC"#,
    )
    .await;
    assert_eq!(rows(&ordered), &vec![json!("Alice"), json!("Bob")]);

    let first = run(
        &nexus,
        r#"FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} } ORDER BY ?c.name LIMIT 1"#,
    )
    .await;
    assert_eq!(rows(first.first_result().unwrap()), &vec![json!("Alice")]);
    let cursor = first.next_cursor.clone().expect("more rows remain");

    let second = run(
        &nexus,
        &format!(
            r#"FIND(?c.name) WHERE {{ ?c CONCEPT {{type: "Person"}} }} ORDER BY ?c.name LIMIT 1 CURSOR "{cursor}""#
        ),
    )
    .await;
    assert_eq!(rows(second.first_result().unwrap()), &vec![json!("Bob")]);
    assert!(second.next_cursor.is_none(), "the last page has no cursor");
}

#[tokio::test]
async fn an_archived_element_leaves_ordinary_recall_but_still_exists() {
    // Spec §41.2. The default pattern stops matching it; asking for the
    // archived state finds it again, and every reference still resolves.
    let nexus = seeded("archived").await;
    let alice: String = {
        let found = ok(&nexus, r#"FIND(?c) WHERE { ?c CONCEPT {name: "Alice"} }"#).await;
        rows(&found)[0]["id"].as_str().unwrap().to_string()
    };

    let request = serde_json::from_value::<Request>(json!({
        "kip": "2.0",
        "operations": [{"command": "ARCHIVE :x", "parameters": {"x": alice}}]
    }))
    .unwrap();
    let parsed = request.operations[0].parse().unwrap();
    nexus
        .execute(parsed, &request, &request.operations[0])
        .await;

    let recalled = ok(
        &nexus,
        r#"FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} } ORDER BY ?c.name"#,
    )
    .await;
    assert_eq!(rows(&recalled), &vec![json!("Bob")]);

    let asked_for = ok(
        &nexus,
        r#"FIND(?c.name) WHERE { ?c CONCEPT {type: "Person", state: "archived"} }"#,
    )
    .await;
    assert_eq!(rows(&asked_for), &vec![json!("Alice")]);

    // And the Proposition that references it still resolves both ends.
    let still_linked = ok(
        &nexus,
        r#"FIND(?p) WHERE { ?p PROPOSITION (?s, "prefers", ?o) }"#,
    )
    .await;
    assert_eq!(rows(&still_linked).len(), 2);
}

#[tokio::test]
async fn for_time_restricts_by_world_validity() {
    // Spec §36.1: `FOR TIME` asks what was applicable then, an axis
    // independent of what the Brain contained then.
    let nexus = nexus("for_time").await;
    ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark" }
            ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
            CREATE ASSERTION ?old {
                SET FIELDS {
                    proposition: ?p, asserted_by: ?alice, stance: "support", mode: "stated",
                    valid_time: {from: "2020-01-01T00:00:00Z", until: "2023-01-01T00:00:00Z"}
                }
            }
            CREATE ASSERTION ?new {
                SET FIELDS {
                    proposition: ?p, asserted_by: ?alice, stance: "support", mode: "stated",
                    valid_time: {from: "2023-01-01T00:00:00Z"}
                }
            }
        }"#,
    )
    .await;

    let then = ok(
        &nexus,
        r#"FIND(?a.valid_time.from) WHERE { ?a ASSERTION {} } FOR TIME "2021-06-01T00:00:00Z""#,
    )
    .await;
    assert_eq!(rows(&then), &vec![json!("2020-01-01T00:00:00.000Z")]);

    let now = ok(
        &nexus,
        r#"FIND(?a.valid_time.from) WHERE { ?a ASSERTION {} } FOR TIME "2026-06-01T00:00:00Z""#,
    )
    .await;
    assert_eq!(rows(&now), &vec![json!("2023-01-01T00:00:00.000Z")]);
}

#[tokio::test]
async fn a_structural_pattern_reads_record_topology() {
    // Spec §17.3: a structural reference is not a semantic Proposition. The
    // pattern reports how records are assembled, nothing about truth.
    let nexus = nexus("structural").await;
    ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?s1 {
                TYPE "ExperienceStep"
                NAME "Step one"
                SET ATTRIBUTES {step_kind: "action", summary: "First"}
            }
            CREATE CONCEPT ?s2 {
                TYPE "ExperienceStep"
                NAME "Step two"
                SET ATTRIBUTES {step_kind: "action", summary: "Second"}
            }
            CREATE CONCEPT ?exp {
                TYPE "Experience"
                NAME "Deploy"
                SET ATTRIBUTES {goal: "ship", outcome_status: "success"}
                SET STRUCTURAL { ("has_step", ?s1) ("has_step", ?s2) }
            }
        }"#,
    )
    .await;

    let steps = ok(
        &nexus,
        r#"FIND(?step.name)
           WHERE {
             ?exp CONCEPT {name: "Deploy"}
             STRUCTURAL (?exp, "has_step", ?step)
           }
           ORDER BY ?step.name"#,
    )
    .await;
    assert_eq!(rows(&steps), &vec![json!("Step one"), json!("Step two")]);
}

#[tokio::test]
async fn unsupported_reads_say_so_instead_of_answering_something_else() {
    let nexus = seeded("unsupported").await;
    // Hop quantifiers moved out of this list when traversal landed; see
    // `tests/traversal.rs`.
    for (command, fragment) in [(
        r#"FIND(?c) WHERE { ?c CONCEPT {type: "Person"} } AS OF SEQ 1"#,
        "historical snapshots",
    )] {
        let response = run(&nexus, command).await;
        let error = response.error.as_ref().unwrap_or_else(|| {
            panic!("{command} should not have succeeded");
        });
        assert_eq!(
            error.code.as_str(),
            "UnsupportedCapability",
            "for {command}"
        );
        assert!(
            error.message.contains(fragment),
            "for {command}: {}",
            error.message
        );
    }
}

#[tokio::test]
async fn an_empty_where_block_is_one_solution_not_zero() {
    let nexus = seeded("unit").await;
    // `?c CONCEPT {}` constrains nothing but the kind, so it finds them all.
    let all = ok(&nexus, r#"FIND(COUNT(?c)) WHERE { ?c CONCEPT {} }"#).await;
    assert_eq!(all, json!([4]));
}
