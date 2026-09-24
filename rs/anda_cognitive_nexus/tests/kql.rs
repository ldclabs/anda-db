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
    nexus
        .install_package(
            &SchemaPackage::parse(include_str!("support/options.json")).unwrap(),
            "test",
        )
        .await
        .unwrap();
    lock.packages
        .insert("kip://test/options".into(), "1.0.0".into());
    lock.states
        .insert("kip://test/options".into(), PackageState::Active);
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

#[tokio::test]
async fn sparse_concept_fields_preserve_absence_and_nonempty_matching() {
    let nexus = nexus("sparse_concept_matching").await;
    ok(&nexus, r#"CREATE CONCEPT ?c { TYPE "Person" }"#).await;
    let all = ok(&nexus, r#"FIND(?c) WHERE { ?c CONCEPT {} }"#).await;
    assert_eq!(all.as_array().unwrap().len(), 1);

    // An absent optional field never matched a literal empty string in the
    // rendered view. Omitting the sentinel from its index must preserve that.
    for field in ["name", "key", "canonical_id"] {
        let found = ok(
            &nexus,
            &format!(r#"FIND(?c) WHERE {{ ?c CONCEPT {{{field}: ""}} }}"#),
        )
        .await;
        assert!(found.as_array().unwrap().is_empty(), "{field}");
    }

    ok(
        &nexus,
        r#"CREATE CONCEPT ?named { TYPE "Person" NAME "Alice" SET FIELDS {key: "alice"} }"#,
    )
    .await;
    for matcher in [r#"name: "Alice""#, r#"key: "alice""#] {
        let found = ok(
            &nexus,
            &format!("FIND(?c) WHERE {{ ?c CONCEPT {{{matcher}}} }}"),
        )
        .await;
        assert_eq!(found.as_array().unwrap().len(), 1, "{matcher}");
    }
}

/// Two people, two preferences, three claims with different stances.
async fn seeded(name: &str) -> CognitiveNexus {
    let nexus = nexus(name).await;
    ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" SET ATTRIBUTES {display_name: "Alice A"} }
            CREATE CONCEPT ?bob { TYPE "Person" NAME "Bob" }
            CREATE CONCEPT ?dark { TYPE "Option" NAME "Dark mode" }
            CREATE CONCEPT ?light { TYPE "Option" NAME "Light mode" }
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
        "operations": [{"command": "TRANSITION :x TO \"archived\"", "parameters": {"x": alice}}]
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
            CREATE CONCEPT ?dark { TYPE "Option" NAME "Dark" }
            ENSURE PROPOSITION ?p (?alice, "prefers", ?dark)
            CREATE ASSERTION ?old {
                SET FIELDS {
                    proposition: ?p, asserted_by: ?alice, stance: "support", mode: "stated",
                    valid_time: {from: "2020-01-01T00:00:00.000Z", until: "2023-01-01T00:00:00.000Z"}
                }
            }
            CREATE ASSERTION ?new {
                SET FIELDS {
                    proposition: ?p, asserted_by: ?alice, stance: "support", mode: "stated",
                    valid_time: {from: "2023-01-01T00:00:00.000Z"}
                }
            }
        }"#,
    )
    .await;

    let then = ok(
        &nexus,
        r#"FIND(?a.valid_time.from) WHERE { ?a ASSERTION {} } FOR TIME "2021-06-01T00:00:00.000Z""#,
    )
    .await;
    assert_eq!(rows(&then), &vec![json!("2020-01-01T00:00:00.000Z")]);

    let now = ok(
        &nexus,
        r#"FIND(?a.valid_time.from) WHERE { ?a ASSERTION {} } FOR TIME "2026-06-01T00:00:00.000Z""#,
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
async fn an_aggregate_groups_by_the_projected_expressions() {
    let nexus = seeded("unit").await;
    // §44.6: grouping is implicit — the non-aggregated projected expressions
    // are the key. A plain variable beside an aggregate is one row per group,
    // not one global row with a variable picked out of whichever solution
    // happened to come first.
    let counts = ok(
        &nexus,
        r#"FIND(?c.name, COUNT(?a))
           WHERE {
             ?c CONCEPT {type: "Person"}
             ?a ASSERTION {asserted_by: ?c}
           }
           ORDER BY ?c.name"#,
    )
    .await;
    let rows = counts.as_array().expect("one row per group").clone();
    assert!(!rows.is_empty());
    for row in &rows {
        let pair = row.as_array().expect("name and count");
        assert!(pair[0].is_string(), "{pair:?}");
        assert!(pair[1].as_u64().is_some_and(|count| count > 0), "{pair:?}");
    }
    // Ascending by name, which is what the ORDER BY asked for.
    let names: Vec<&str> = rows
        .iter()
        .map(|row| row[0].as_str().unwrap_or_default())
        .collect();
    let mut sorted = names.clone();
    sorted.sort_unstable();
    assert_eq!(names, sorted);
}

#[tokio::test]
async fn ordering_by_an_aggregate_orders_the_groups_it_counts() {
    let nexus = seeded("unit").await;
    // ORDER BY counts the same groups as the projected aggregate.
    let ordered = ok(
        &nexus,
        r#"FIND(?c.name, COUNT(?a))
           WHERE {
             ?c CONCEPT {type: "Person"}
             ?a ASSERTION {asserted_by: ?c}
           }
           ORDER BY COUNT(?a) DESC"#,
    )
    .await;
    let counts: Vec<u64> = ordered
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| row[1].as_u64().unwrap_or_default())
        .collect();
    let mut descending = counts.clone();
    descending.sort_unstable_by(|a, b| b.cmp(a));
    assert_eq!(counts, descending);

    // The portable aggregate sort expression must also appear in FIND.
    let names = run(
        &nexus,
        r#"FIND(?c.name)
           WHERE {
             ?c CONCEPT {type: "Person"}
             ?a ASSERTION {asserted_by: ?c}
           }
           ORDER BY COUNT(?a) DESC"#,
    )
    .await;
    assert_eq!(
        names.error.expect("invalid aggregate sort").code,
        "InvalidSyntax"
    );
}

#[tokio::test]
async fn a_sort_key_that_varies_inside_a_group_is_refused() {
    let nexus = seeded("unit").await;
    // Grouping makes the projected expressions the only values a group has, so
    // a key that varies inside one has nothing to sort by — and picking a row
    // to read it from would be inventing an answer.
    let response = run(
        &nexus,
        r#"FIND(?c.name, COUNT(?a))
           WHERE {
             ?c CONCEPT {type: "Person"}
             ?a ASSERTION {asserted_by: ?c}
           }
           ORDER BY ?a.confidence"#,
    )
    .await;
    assert_eq!(response.status, TopLevelStatus::Failed);
    let error = response.error.expect("a refusal carries an error");
    assert_eq!(error.code, "InvalidSyntax");
}

#[tokio::test]
async fn an_empty_where_block_is_one_solution_not_zero() {
    let nexus = seeded("unit").await;
    // `?c CONCEPT {}` constrains nothing but the kind, so it finds them all.
    let all = ok(&nexus, r#"FIND(COUNT(?c)) WHERE { ?c CONCEPT {} }"#).await;
    assert_eq!(all, json!([4]));
}

#[tokio::test]
async fn a_cursor_continues_only_the_traversal_that_issued_it() {
    // §44.8, §88.4: a cursor names a page of one traversal. Handed to another
    // query it is refused, not answered with the first query's page.
    let nexus = seeded("cursor_binding").await;
    let first = run(
        &nexus,
        r#"FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} } ORDER BY ?c.name LIMIT 1"#,
    )
    .await;
    let cursor = first.next_cursor.clone().expect("more rows remain");

    // The same traversal paged wider: the page size is not part of its identity.
    let wider = run(
        &nexus,
        &format!(
            r#"FIND(?c.name) WHERE {{ ?c CONCEPT {{type: "Person"}} }} ORDER BY ?c.name LIMIT 5 CURSOR "{cursor}""#
        ),
    )
    .await;
    assert_eq!(wider.status, TopLevelStatus::Succeeded, "{:?}", wider.error);
    assert_eq!(rows(wider.first_result().unwrap()), &vec![json!("Bob")]);

    let elsewhere = run(
        &nexus,
        &format!(
            r#"FIND(?c.name) WHERE {{ ?c CONCEPT {{type: "Option"}} }} ORDER BY ?c.name LIMIT 1 CURSOR "{cursor}""#
        ),
    )
    .await;
    assert_eq!(
        elsewhere.error.as_ref().map(|err| err.code.as_str()),
        Some("CursorMismatch"),
        "{elsewhere:?}"
    );
}

#[tokio::test]
async fn nested_blocks_correlate_filters_and_preserve_outer_bindings() {
    let nexus = seeded("nested_scope").await;
    let optional = ok(
        &nexus,
        r#"FIND(?p.name, ?o.name) WHERE {
        ?p CONCEPT {type: "Person"}
        OPTIONAL { FILTER(?p.name == "Alice") ?o CONCEPT {name: "Dark mode"} }
    } ORDER BY ?p.name"#,
    )
    .await;
    assert_eq!(optional, json!([["Alice", "Dark mode"], ["Bob", null]]));
    let negated = ok(
        &nexus,
        r#"FIND(?p.name) WHERE {
        ?p CONCEPT {type: "Person"} NOT { FILTER(?p.name == "Alice") }
    }"#,
    )
    .await;
    assert_eq!(negated, json!(["Bob"]));
    let independent_nested = ok(
        &nexus,
        r#"FIND(?p.name, ?x.name) WHERE {
        ?p CONCEPT {type: "Person"}
        OPTIONAL {
            ?x CONCEPT {name: "absent"}
            UNION { ?p CONCEPT {name: "Alice"} ?x CONCEPT {name: "Dark mode"} }
        }
    } ORDER BY ?p.name"#,
    )
    .await;
    assert_eq!(
        independent_nested,
        json!([["Alice", "Dark mode"], ["Bob", null]])
    );
    let nested_not = ok(
        &nexus,
        r#"FIND(?p.name) WHERE {
        ?p CONCEPT {type: "Person"}
        NOT { ?x CONCEPT {name: "absent"} UNION { ?p CONCEPT {name: "Alice"} } }
    }"#,
    )
    .await;
    assert_eq!(nested_not, json!(["Bob"]));
}

#[tokio::test]
async fn optional_failure_does_not_leak_partial_bindings_and_can_rebind() {
    let nexus = seeded("optional_bindings").await;
    let result = ok(
        &nexus,
        r#"FIND(?p.name, ?o.name) WHERE {
        ?p CONCEPT {name: "Alice"}
        OPTIONAL { ?o CONCEPT {name: "Dark mode"} FILTER(?o.name == "missing") }
        ?o CONCEPT {name: "Light mode"}
    }"#,
    )
    .await;
    assert_eq!(result, json!([["Alice", "Light mode"]]));
    let result = ok(
        &nexus,
        r#"FIND(?p.name) WHERE {
        ?p CONCEPT {name: "Alice"}
        OPTIONAL { ?o CONCEPT {name: "missing"} }
        FILTER(?o.name == "anything")
    }"#,
    )
    .await;
    assert_eq!(result, json!([]));
}

#[tokio::test]
async fn union_survives_empty_left_and_deduplicates_complete_bindings() {
    let nexus = seeded("union_sets").await;
    assert_eq!(
        ok(
            &nexus,
            r#"FIND(?p.name, ?o.name) WHERE {
        ?p CONCEPT {name: "missing"} UNION { ?o CONCEPT {name: "Dark mode"} }
    }"#
        )
        .await,
        json!([[null, "Dark mode"]])
    );
    assert_eq!(
        ok(
            &nexus,
            r#"FIND(COUNT(?p)) WHERE {
        ?p CONCEPT {name: "Alice"} UNION { ?p CONCEPT {name: "Alice"} }
    }"#
        )
        .await,
        json!([1])
    );
    assert_eq!(
        ok(
            &nexus,
            r#"FIND(?p.name) WHERE {
        ?p CONCEPT {type: "Person"} ?a ASSERTION {asserted_by: ?p}
    } ORDER BY ?p.name"#
        )
        .await,
        json!(["Alice", "Bob", "Bob"])
    );
}

#[tokio::test]
async fn expression_variables_obey_not_and_union_scope_boundaries() {
    let nexus = seeded("static_scope").await;
    for command in [
        r#"FIND(?local) WHERE { ?p CONCEPT {name: "Alice"} NOT { ?local CONCEPT {name: "absent"} } }"#,
        r#"FIND(?p) WHERE { ?p CONCEPT {name: "Alice"} UNION { FILTER(?p.name == "Alice") } }"#,
        r#"FIND(?p) WHERE { ?p CONCEPT {name: "absent"} FILTER(IS_NULL(?unknown)) }"#,
        r#"FIND(?p) WHERE { ?p CONCEPT {} OPTIONAL { NOT { ?local CONCEPT {} } } } ORDER BY ?local.name"#,
    ] {
        let error = run(&nexus, command).await.error.expect(command);
        assert_eq!(error.code, "InvalidSyntax", "{command}: {error:?}");
    }
    assert_eq!(
        ok(
            &nexus,
            r#"FIND(?local.name) WHERE {
        NOT { ?local CONCEPT {name: "absent"} }
        ?local CONCEPT {name: "Alice"}
    }"#
        )
        .await,
        json!(["Alice"])
    );
}

#[tokio::test]
async fn null_filters_preserve_unknown_through_negation_and_string_tests() {
    let nexus = seeded("filter_unknown").await;
    for condition in [
        "!(?p.attributes.missing == 3)",
        "!CONTAINS(?p.attributes.missing, \"x\")",
        "!STARTS_WITH(?p.attributes.missing, \"\")",
        "!IN(?p.attributes.missing, [null])",
        "!(?p.attributes.missing > 3 || ?p.name == \"missing\")",
    ] {
        let command =
            format!("FIND(?p.name) WHERE {{ ?p CONCEPT {{name: \"Alice\"}} FILTER({condition}) }}");
        assert_eq!(ok(&nexus, &command).await, json!([]), "{condition}");
    }
    assert_eq!(
        ok(
            &nexus,
            r#"FIND(?p.name) WHERE {
        ?p CONCEPT {name: "Alice"}
        FILTER(?p.name == "Alice" || ?p.attributes.missing == 3)
    }"#
        )
        .await,
        json!(["Alice"])
    );
    assert_eq!(
        ok(
            &nexus,
            r#"FIND(?p.name) WHERE {
        ?p CONCEPT {name: "Alice"}
        FILTER(!(?p.name == "missing" && ?p.attributes.missing == 3))
    }"#
        )
        .await,
        json!(["Alice"])
    );
}

#[tokio::test]
async fn static_filter_errors_are_not_hidden_by_absent_rows_or_short_circuit() {
    let nexus = seeded("filter_static_errors").await;
    for command in [
        r#"FIND(?p) WHERE { ?p CONCEPT {name: "absent"} FILTER(REGEX(?p.name, "(")) }"#,
        r#"FIND(?p) WHERE { ?p CONCEPT {name: "Alice"} FILTER(?p.name == "Alice" || REGEX(?p.name, "(")) }"#,
        r#"FIND(?p) WHERE { ?p CONCEPT {name: "absent"} OPTIONAL { FILTER(REGEX(?p.name, "(")) } }"#,
        r#"FIND(?p) WHERE { ?p CONCEPT {name: "Alice"} NOT { FILTER(REGEX(?p.name, "(")) } }"#,
        r#"FIND(?p) WHERE { ?p CONCEPT {name: "absent"} FILTER(IS_NULL(?p, ?p)) }"#,
    ] {
        assert_eq!(
            run(&nexus, command).await.error.expect(command).code,
            "InvalidSyntax",
            "{command}"
        );
    }
    let schema_error = run(
        &nexus,
        r#"FIND(?p) WHERE {
        ?p CONCEPT {name: "absent"} OPTIONAL { ?x CONCEPT {type: "NotASchemaType"} }
    }"#,
    )
    .await;
    assert_eq!(schema_error.status, TopLevelStatus::Failed);
}

#[tokio::test]
async fn aggregates_apply_null_empty_and_scalar_type_contracts() {
    let nexus = seeded("aggregate_contracts").await;
    assert_eq!(ok(&nexus, r#"FIND(COUNT(?p), SUM(?p.attributes.n), AVG(?p.attributes.n), MIN(?p.name), MAX(?p.name))
        WHERE { ?p CONCEPT {name: "absent"} }"#).await, json!([[0, null, null, null, null]]));
    assert_eq!(ok(&nexus, r#"FIND(MIN(?p.name), MAX(?p.name), COUNT(?p.attributes.missing), SUM(?p.attributes.missing))
        WHERE { ?p CONCEPT {type: "Person"} }"#).await, json!([["Alice", "Bob", 0, null]]));
    assert_eq!(
        run(
            &nexus,
            r#"FIND(SUM(?p.name)) WHERE { ?p CONCEPT {type: "Person"} }"#
        )
        .await
        .error
        .unwrap()
        .code,
        "TypeMismatch"
    );
}

#[tokio::test]
async fn repeated_variable_positions_are_constraints() {
    let nexus = seeded("repeated_positions").await;
    assert_eq!(
        ok(&nexus, r#"FIND(?p) WHERE { (?p, "prefers", ?p) }"#).await,
        json!([])
    );
    assert_eq!(
        ok(
            &nexus,
            r#"FIND(?p) WHERE { ?p CONCEPT {type: "Person", name: ?p} }"#
        )
        .await,
        json!([])
    );
}

#[tokio::test]
async fn zero_hop_paths_need_visible_elements_but_no_edges() {
    let nexus = seeded("zero_hop").await;
    assert_eq!(
        ok(
            &nexus,
            r#"FIND(?p.name, ?q.name) WHERE {
        ?p CONCEPT {name: "Dark mode"} (?p, "prefers"{0}, ?q)
    }"#
        )
        .await,
        json!([["Dark mode", "Dark mode"]])
    );
    assert_eq!(ok(&nexus, r#"FIND(?p.name) WHERE { (?p, "prefers"{0}, ?p) ?p CONCEPT {type: "Person"} } ORDER BY ?p.name"#).await, json!(["Alice", "Bob"]));
    assert_eq!(
        ok(
            &nexus,
            r#"FIND(?p) WHERE { ({id: "C-99999999"}, "prefers"{0}, ?p) }"#
        )
        .await,
        json!([])
    );
    assert_eq!(
        run(
            &nexus,
            r#"FIND(?p) WHERE { ?p CONCEPT {name: "absent"} (?p, ?pred{1}, ?o) }"#
        )
        .await
        .error
        .unwrap()
        .code,
        "InvalidSyntax"
    );
}

#[tokio::test]
async fn limit_requires_a_nonnegative_safe_number() {
    let nexus = seeded("safe_limit").await;
    for limit in [json!("1"), json!(-1), json!(0.5)] {
        let command = r#"FIND(?p) WHERE { ?p CONCEPT {} } LIMIT :limit"#;
        let mut request = Request::single(command);
        request.parameters = Some(serde_json::from_value(json!({"limit": limit})).unwrap());
        let response = nexus
            .execute(
                anda_kip::parse_kip(command).unwrap(),
                &request,
                &request.operations[0],
            )
            .await;
        assert_eq!(response.error.expect("invalid limit").code, "TypeMismatch");
    }
    assert_eq!(
        ok(&nexus, r#"FIND(?p) WHERE { ?p CONCEPT {} } LIMIT 0"#).await,
        json!([])
    );
}

#[tokio::test]
async fn nested_object_patterns_are_subsets_with_compatible_field_bindings() {
    let nexus = seeded("nested_field_patterns").await;
    assert_eq!(ok(&nexus, r#"FIND(?p.name, ?version) WHERE {
        ?p CONCEPT {type: "Person", _system: {version: ?version}, attributes: {display_name: "Alice A"}}
    }"#).await, json!([["Alice", 1]]));
    assert_eq!(
        ok(
            &nexus,
            r#"FIND(?p.name, ?display) WHERE {
        ?p CONCEPT {type: "Person", attributes: {display_name: ?display}}
    }"#
        )
        .await,
        json!([["Alice", "Alice A"]])
    );
    assert_eq!(
        ok(
            &nexus,
            r#"FIND(?p) WHERE {
        ?p CONCEPT {type: "Person", attributes: {missing: null}}
    }"#
        )
        .await,
        json!([])
    );
    assert_eq!(
        ok(
            &nexus,
            r#"FIND(?p) WHERE {
        ?p CONCEPT {type: "Person", _system: {version: ?p}}
    }"#
        )
        .await,
        json!([])
    );
}

#[tokio::test]
async fn inline_endpoint_patterns_find_existing_elements_and_export_fields() {
    let nexus = seeded("inline_endpoints").await;
    assert_eq!(
        ok(
            &nexus,
            r#"FIND(?person, ?preference) WHERE {
        ({type: "Person", name: ?person}, "prefers", {type: "Option", name: ?preference})
    } ORDER BY ?person"#
        )
        .await,
        json!([["Alice", "Dark mode"], ["Bob", "Light mode"]])
    );
    assert_eq!(
        ok(
            &nexus,
            r#"FIND(?p.name) WHERE {
        ({type: "Person", name: "Alice"}, "prefers", ?p)
    }"#
        )
        .await,
        json!(["Dark mode"])
    );
    let alice = ok(
        &nexus,
        r#"FIND(?p.id) WHERE { ?p CONCEPT {name: "Alice"} }"#,
    )
    .await[0]
        .as_str()
        .unwrap()
        .to_string();
    let query = format!(r#"FIND(?p) WHERE {{ ?p CONCEPT {{id: "{alice}", name: "Bob"}} }}"#);
    assert_eq!(ok(&nexus, &query).await, json!([]));
    let query = format!(r#"FIND(?p) WHERE {{ ({{id: "{alice}", name: "Bob"}}, "prefers", ?p) }}"#);
    assert_eq!(ok(&nexus, &query).await, json!([]));
    assert_eq!(
        ok(&nexus, r#"FIND(COUNT(?p)) WHERE { ?p CONCEPT {} }"#).await,
        json!([4])
    );
}

#[tokio::test]
async fn order_by_distinct_uses_the_distinct_aggregate_identity() {
    let nexus = seeded("distinct_sort").await;
    let identities = ok(
        &nexus,
        r#"FIND(?a.proposition.id, ?a.asserted_by.id) WHERE { ?a ASSERTION {confidence: 0.9} }"#,
    )
    .await;
    let command = r#"CREATE ASSERTION ?extra { SET FIELDS {
        proposition: :proposition, asserted_by: :actor, stance: "support", mode: "stated", confidence: 0.7
    }}"#;
    let mut request = Request::single(command);
    request.parameters = Some(
        serde_json::from_value(json!({"proposition": identities[0][0], "actor": identities[0][1]}))
            .unwrap(),
    );
    let response = nexus
        .execute(
            anda_kip::parse_kip(command).unwrap(),
            &request,
            &request.operations[0],
        )
        .await;
    assert_eq!(response.status, TopLevelStatus::Succeeded, "{response:?}");
    assert_eq!(
        ok(
            &nexus,
            r#"FIND(?p.name, COUNT(?a.stance), COUNT(DISTINCT ?a.stance)) WHERE {
        ?p CONCEPT {type: "Person"} ?a ASSERTION {asserted_by: ?p}
    } ORDER BY COUNT(DISTINCT ?a.stance) DESC, ?p.name"#
        )
        .await,
        json!([["Bob", 2, 2], ["Alice", 2, 1]])
    );
}

#[tokio::test]
async fn virtual_beliefs_deduplicate_by_target_and_basis() {
    let nexus = seeded("virtual_identity").await;
    let ids = ok(&nexus, r#"FIND(?c.name, ?c.id) WHERE { ?c CONCEPT {} }"#).await;
    let get = |name: &str| {
        ids.as_array()
            .unwrap()
            .iter()
            .find(|row| row[0] == name)
            .unwrap()[1]
            .clone()
    };
    let command = r#"FIND(COUNT(?b), COUNT(DISTINCT ?b)) WHERE {
        ?b BELIEF (:alice, "prefers", :light)
        UNION { ?b BELIEF (:bob, "prefers", :dark) }
        UNION { ?b BELIEF (:alice, "prefers", :light) }
    }"#;
    let mut request = Request::single(command);
    request.parameters = Some(serde_json::from_value(json!({
        "alice": get("Alice"), "bob": get("Bob"), "dark": get("Dark mode"), "light": get("Light mode")
    })).unwrap());
    let response = nexus
        .execute(
            anda_kip::parse_kip(command).unwrap(),
            &request,
            &request.operations[0],
        )
        .await;
    assert_eq!(response.status, TopLevelStatus::Succeeded, "{response:?}");
    assert_eq!(response.first_result(), Some(&json!([[2, 2]])));
}

#[tokio::test]
async fn known_function_argument_types_are_validated_without_rows() {
    let nexus = seeded("known_filter_types").await;
    for expression in ["IN(?p.name, 1)", "IS_KIND(?p, 1)"] {
        let command =
            format!(r#"FIND(?p) WHERE {{ ?p CONCEPT {{name: "absent"}} FILTER({expression}) }}"#);
        let response = run(&nexus, &command).await;
        assert_eq!(
            response.error.expect(&command).code,
            "TypeMismatch",
            "{expression}"
        );
    }
}
