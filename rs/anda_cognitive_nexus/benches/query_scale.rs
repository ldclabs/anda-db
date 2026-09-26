//! Opt-in million-row current-state query probe. Seed rows from a validated
//! KML template, outside the measured GET-count checks. Cloned filler rows do
//! not model a complete historical log. Run with ANDA_NEXUS_BENCH=1.

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
            &SchemaPackage::parse(include_str!("../tests/support/options.json")).unwrap(),
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

fn main() {
    if std::env::var_os("ANDA_NEXUS_BENCH").is_none() {
        eprintln!("Set ANDA_NEXUS_BENCH=1 to opt into the large fixture.");
        return;
    }
    let documents = std::env::var("ANDA_BENCH_DOCS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1_000_001)
        .max(4);
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let n = nexus("million_current_rows").await;
        let made = ok(&n, r#"MUTATE {
            CREATE CONCEPT ?person {TYPE "Person" NAME "Unique Person"}
            CREATE CONCEPT ?step {TYPE "ExperienceStep" SET ATTRIBUTES {step_kind:"action",summary:"first"}}
            CREATE CONCEPT ?experience {TYPE "Experience" SET ATTRIBUTES {goal:"probe",outcome_status:"success"} SET STRUCTURAL {("has_step",?step)}}
            CREATE CONCEPT ?event {TYPE "Event" SET ATTRIBUTES {summary:""}}
        }"#).await;
        let person=made["handles"]["person"].as_str().unwrap();
        let step=made["handles"]["step"].as_str().unwrap();
        let experience=made["handles"]["experience"].as_str().unwrap();
        let event: anda_cognitive_nexus::ElementId=made["handles"]["event"].as_str().unwrap().parse().unwrap();
        let collection=n.store.concepts();
        let mut template:anda_cognitive_nexus::rows::ConceptRow=collection.get_as(event.seq).await.unwrap();
        template._id=0;
        for i in 4..documents {
            collection.add_from(&template).await.unwrap();
            if i % 250_000 == 0 {println!("{}",json!({"seed_progress":i}));}
        }
        assert_eq!(collection.len(),documents);
        println!("{}",json!({"concepts":collection.len(),"fixture":"current rows from a schema-valid KML template; cloned rows have no version history","latency_claim":false}));
        for (name,query,expected) in [
            ("rare_type",r#"FIND(?c.name) WHERE {?c CONCEPT {type:"Person"}} LIMIT 1"#.to_string(),json!(["Unique Person"])),
            ("filter",r#"FIND(?c.name) WHERE {?c CONCEPT {} FILTER(?c.name == "Unique Person")} LIMIT 1"#.to_string(),json!(["Unique Person"])),
            ("bound",format!(r#"FIND(?c.name) WHERE {{?c CONCEPT {{id:"{person}"}} ?c CONCEPT {{}}}} LIMIT 1"#),json!(["Unique Person"])),
            ("count", "FIND(COUNT(?c)) WHERE {?c CONCEPT {}}".into(),json!([documents])),
            ("reverse_structural",format!(r#"FIND(?s.id) WHERE {{STRUCTURAL (?s,"has_step","{step}")}}"#),json!([experience])),
            ("first_page","FIND(?c.id) WHERE {?c CONCEPT {}} LIMIT 2".into(),json!([person,step])),
        ] {
            let before=collection.stats().get_count;
            let response=run(&n,&query).await;
            println!("{}",json!({"case":name,"status":response.status,"result":response.first_result(),"concept_gets":collection.stats().get_count-before,"error":response.error}));
            assert_eq!(response.status,TopLevelStatus::Succeeded,"{name}");
            assert_eq!(response.first_result().unwrap(),&expected,"{name}");
        }
    });
}
