//! Hop-quantified path traversal: `(?a, "knows"{1,3}, ?b)`.
//!
//! A path walks the raw Proposition graph. It reports that the tuples exist —
//! never that the chain is believed, because belief does not compose along a
//! path (§45): two separately credible claims do not make their conclusion
//! credible, and an engine that let a traversal carry confidence would be
//! manufacturing one.

use anda_cognitive_nexus::{
    CognitiveNexus,
    nexus::DEFAULT_SPACE,
    profiles::COGNITIVE_MEMORY,
    schema::{PackageState, SchemaLock, SchemaPackage},
};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{Executor, Json, Request, TopLevelStatus};
use object_store::memory::InMemory;
use serde_json::json;
use std::sync::Arc;

const PROFILE_ID: &str = "kip://profiles/cognitive-memory";

/// A profile whose predicate is legal between two Experiences, so a chain of
/// them is a chain the Schema accepts.
const CHAIN_PROFILE: &str = r#"{
  "format": "KIP-Schema-Package",
  "format_version": "2.0",
  "manifest": {
    "package_id": "kip://test/chain",
    "version": "1.0.0",
    "package_ref": "kip://test/chain@1.0.0",
    "name": "Chain test package"
  },
  "definitions": {
    "predicates": {
      "leads_to": {
        "ref": "kip://test/chain@1.0.0/leads_to",
        "kind": "PredicateType",
        "subject": {"kinds": ["Concept"]},
        "object": {"kinds": ["Concept"]}
      }
    }
  }
}"#;

async fn nexus(name: &str) -> CognitiveNexus {
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: name.to_string(),
            description: "traversal tests".to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let nexus = CognitiveNexus::connect(Arc::new(db)).await.unwrap();
    let mut lock = SchemaLock::default();
    for (id, source) in [
        (PROFILE_ID, COGNITIVE_MEMORY),
        ("kip://test/chain", CHAIN_PROFILE),
    ] {
        nexus
            .install_package(&SchemaPackage::parse(source).unwrap(), "test")
            .await
            .unwrap();
        let version = if id == PROFILE_ID { "2.0.0" } else { "1.0.0" };
        lock.packages.insert(id.to_string(), version.to_string());
        lock.states.insert(id.to_string(), PackageState::Active);
    }
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
        response.results
    );
    response.first_result().cloned().unwrap_or(Json::Null)
}

async fn err(nexus: &CognitiveNexus, command: &str) -> anda_kip::ErrorObject {
    let response = run(nexus, command).await;
    response
        .results
        .into_iter()
        .find_map(|result| result.error)
        .or(response.error)
        .unwrap_or_else(|| panic!("{command} was expected to fail"))
}

fn rows(result: &Json) -> &Vec<Json> {
    result.as_array().expect("a KQL result is an array")
}

/// A → B → C → D, plus a cycle D → B, and a separate one-hop pair.
async fn chained(name: &str) -> CognitiveNexus {
    let nexus = nexus(name).await;
    ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?a { TYPE "Event" NAME "A" SET ATTRIBUTES {summary: "A"} }
            CREATE CONCEPT ?b { TYPE "Event" NAME "B" SET ATTRIBUTES {summary: "B"} }
            CREATE CONCEPT ?c { TYPE "Event" NAME "C" SET ATTRIBUTES {summary: "C"} }
            CREATE CONCEPT ?d { TYPE "Event" NAME "D" SET ATTRIBUTES {summary: "D"} }
            CREATE CONCEPT ?x { TYPE "Event" NAME "X" SET ATTRIBUTES {summary: "X"} }
            CREATE CONCEPT ?y { TYPE "Event" NAME "Y" SET ATTRIBUTES {summary: "Y"} }
            ENSURE PROPOSITION ?ab (?a, "leads_to", ?b)
            ENSURE PROPOSITION ?bc (?b, "leads_to", ?c)
            ENSURE PROPOSITION ?cd (?c, "leads_to", ?d)
            ENSURE PROPOSITION ?db (?d, "leads_to", ?b)
            ENSURE PROPOSITION ?xy (?x, "leads_to", ?y)
        }"#,
    )
    .await;
    nexus
}

#[tokio::test]
async fn a_bounded_walk_reaches_what_the_range_allows() {
    let nexus = chained("bounded").await;

    let one = ok(
        &nexus,
        r#"FIND(?to.name)
           WHERE { ?from CONCEPT {name: "A"} (?from, "leads_to"{1,1}, ?to) }
           ORDER BY ?to.name"#,
    )
    .await;
    assert_eq!(rows(&one), &vec![json!("B")]);

    let two = ok(
        &nexus,
        r#"FIND(?to.name)
           WHERE { ?from CONCEPT {name: "A"} (?from, "leads_to"{1,2}, ?to) }
           ORDER BY ?to.name"#,
    )
    .await;
    assert_eq!(rows(&two), &vec![json!("B"), json!("C")]);

    // The cycle D → B does not make the walk run forever, and does not
    // re-report B.
    let deep = ok(
        &nexus,
        r#"FIND(?to.name)
           WHERE { ?from CONCEPT {name: "A"} (?from, "leads_to"{1,10}, ?to) }
           ORDER BY ?to.name"#,
    )
    .await;
    assert_eq!(rows(&deep), &vec![json!("B"), json!("C"), json!("D")]);

    // An unbounded range is the same walk with no ceiling.
    let unbounded = ok(
        &nexus,
        r#"FIND(?to.name)
           WHERE { ?from CONCEPT {name: "A"} (?from, "leads_to"{1,}, ?to) }
           ORDER BY ?to.name"#,
    )
    .await;
    assert_eq!(rows(&unbounded), &vec![json!("B"), json!("C"), json!("D")]);
}

/// A minimum above one skips the near neighbours; a minimum of zero includes
/// the element itself, because zero hops is where you already are.
#[tokio::test]
async fn the_minimum_decides_where_the_answer_starts() {
    let nexus = chained("minimum").await;

    let far = ok(
        &nexus,
        r#"FIND(?to.name)
           WHERE { ?from CONCEPT {name: "A"} (?from, "leads_to"{2,3}, ?to) }
           ORDER BY ?to.name"#,
    )
    .await;
    assert_eq!(rows(&far), &vec![json!("C"), json!("D")]);

    let including_self = ok(
        &nexus,
        r#"FIND(?to.name)
           WHERE { ?from CONCEPT {name: "A"} (?from, "leads_to"{0,1}, ?to) }
           ORDER BY ?to.name"#,
    )
    .await;
    assert_eq!(rows(&including_self), &vec![json!("A"), json!("B")]);

    let exactly = ok(
        &nexus,
        r#"FIND(?to.name)
           WHERE { ?from CONCEPT {name: "A"} (?from, "leads_to"{3}, ?to) }"#,
    )
    .await;
    assert_eq!(rows(&exactly), &vec![json!("D")]);
}

/// The walk runs backwards when the object is the pinned end, so asking what
/// reaches D costs the same as asking what A reaches.
#[tokio::test]
async fn a_walk_runs_from_whichever_end_is_pinned() {
    let nexus = chained("direction").await;

    let ancestors = ok(
        &nexus,
        r#"FIND(?from.name)
           WHERE { ?to CONCEPT {name: "D"} (?from, "leads_to"{1,5}, ?to) }
           ORDER BY ?from.name"#,
    )
    .await;
    assert_eq!(rows(&ancestors), &vec![json!("A"), json!("B"), json!("C")]);

    // Both ends pinned is a yes/no question about reachability.
    let connected = ok(
        &nexus,
        r#"FIND(?a.name)
           WHERE {
             ?a CONCEPT {name: "A"}
             ?d CONCEPT {name: "D"}
             (?a, "leads_to"{1,5}, ?d)
           }"#,
    )
    .await;
    assert_eq!(rows(&connected), &vec![json!("A")]);

    let unconnected = ok(
        &nexus,
        r#"FIND(?a.name)
           WHERE {
             ?a CONCEPT {name: "A"}
             ?x CONCEPT {name: "X"}
             (?a, "leads_to"{1,5}, ?x)
           }"#,
    )
    .await;
    assert!(rows(&unconnected).is_empty());
}

/// With neither end pinned the walk enumerates every subject of the predicate,
/// which is bounded work — but a zero-hop range would match every element in
/// the Space against itself, and that is refused rather than answered.
#[tokio::test]
async fn an_unpinned_walk_enumerates_subjects_and_refuses_zero_hops() {
    let nexus = chained("unpinned").await;

    let pairs = ok(
        &nexus,
        r#"FIND(?from.name, ?to.name)
           WHERE { (?from, "leads_to"{2,2}, ?to) }
           ORDER BY ?from.name, ?to.name"#,
    )
    .await;
    assert_eq!(
        rows(&pairs),
        &vec![
            json!(["A", "C"]),
            json!(["B", "D"]),
            json!(["C", "B"]),
            json!(["D", "C"]),
        ]
    );

    let error = err(&nexus, r#"FIND(?a) WHERE { (?a, "leads_to"{0,2}, ?b) }"#).await;
    assert_eq!(error.code, "ResourceExhausted", "{error:?}");
}

/// A walk is not a Proposition. Binding a variable to one would name a claim
/// the query never asked about.
#[tokio::test]
async fn a_multi_hop_path_cannot_be_bound_to_a_variable() {
    let nexus = chained("binding").await;

    let error = err(
        &nexus,
        r#"FIND(?p) WHERE { ?from CONCEPT {name: "A"} ?p PROPOSITION (?from, "leads_to"{1,3}, ?to) }"#,
    )
    .await;
    assert_eq!(error.code, "InvalidSyntax", "{error:?}");

    // A single-hop quantifier is an ordinary tuple pattern, so it still binds.
    let bound = ok(
        &nexus,
        r#"FIND(?to.name)
           WHERE { ?from CONCEPT {name: "A"} ?p PROPOSITION (?from, "leads_to"{1,1}, ?to) }"#,
    )
    .await;
    assert_eq!(rows(&bound), &vec![json!("B")]);
}

/// The quantifier binds to the atom it was written on, so `"a"{1,3} | "b"` is
/// two independent walks rather than one walk that alternates predicates.
#[tokio::test]
async fn an_alternation_quantifies_each_alternative_on_its_own() {
    let nexus = nexus("alternation").await;
    ok(
        &nexus,
        r#"MUTATE {
            CREATE CONCEPT ?a { TYPE "Event" NAME "A" SET ATTRIBUTES {summary: "A"} }
            CREATE CONCEPT ?b { TYPE "Event" NAME "B" SET ATTRIBUTES {summary: "B"} }
            CREATE CONCEPT ?c { TYPE "Event" NAME "C" SET ATTRIBUTES {summary: "C"} }
            CREATE CONCEPT ?d { TYPE "Event" NAME "D" SET ATTRIBUTES {summary: "D"} }
            ENSURE PROPOSITION ?ab (?a, "leads_to", ?b)
            ENSURE PROPOSITION ?bc (?b, "leads_to", ?c)
            ENSURE PROPOSITION ?cd (?c, "same_as", ?d)
        }"#,
    )
    .await;

    // Two hops of leads_to, or one hop of same_as. Not "leads_to then same_as".
    let reached = ok(
        &nexus,
        r#"FIND(?to.name)
           WHERE { ?from CONCEPT {name: "A"} (?from, "leads_to"{1,2} | "same_as", ?to) }
           ORDER BY ?to.name"#,
    )
    .await;
    assert_eq!(rows(&reached), &vec![json!("B"), json!("C")]);
}

/// A traversal reports reachability in the raw graph. Whether anybody believes
/// the chain is a different question, and `BELIEF` still refuses to answer it
/// along a path.
#[tokio::test]
async fn a_path_reports_tuples_not_belief() {
    let nexus = chained("belief").await;

    // The tuples exist with no Assertion about any of them at all, and the
    // walk still finds them: a Proposition existing is not a claim.
    let reachable = ok(
        &nexus,
        r#"FIND(COUNT(?to)) WHERE { ?from CONCEPT {name: "A"} (?from, "leads_to"{1,3}, ?to) }"#,
    )
    .await;
    assert_eq!(rows(&reachable), &vec![json!(3)]);

    let claims = ok(&nexus, r#"FIND(COUNT(?a)) WHERE { ?a ASSERTION {} }"#).await;
    assert_eq!(rows(&claims), &vec![json!(0)]);
}
