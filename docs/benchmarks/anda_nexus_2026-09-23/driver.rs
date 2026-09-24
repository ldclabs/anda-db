//! Standalone paired microbenchmarks. Seed data outside the timed intervals.
use anda_cognitive_nexus::{
    CognitiveNexus, WriteContext, content_digest,
    nexus::DEFAULT_SPACE,
    rows::{ActivityRow, ConceptRow, ElementVersionRow, EvidenceRow, TransactionRow},
    schema::{PackageState, SchemaLock, SchemaPackage},
};
use anda_db::{database::{AndaDB, DBConfig}, schema::Fv};
use anda_kip::{Executor, Json, Request, TopLevelStatus};
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc, time::Instant};

async fn execute(n: &CognitiveNexus, text: &str, parameters: Json) -> Json {
    let mut request = Request::single(text);
    request.parameters = parameters.as_object().cloned();
    let response = n.execute(anda_kip::parse_kip(text).unwrap(), &request, &request.operations[0]).await;
    assert_eq!(response.status, TopLevelStatus::Succeeded, "{response:?}");
    response.first_result().unwrap().clone()
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    for size in [1000, 10000] {
        let db = Arc::new(AndaDB::connect(Arc::new(object_store::memory::InMemory::new()), DBConfig {
            name: format!("bench_{size}"), ..Default::default()
        }).await.unwrap());
        let n = CognitiveNexus::connect(db).await.unwrap();
        n.install_package(&SchemaPackage::parse(anda_cognitive_nexus::profiles::COGNITIVE_MEMORY).unwrap(), "benchmark").await.unwrap();
        let mut lock = SchemaLock::default();
        lock.packages.insert("kip://profiles/cognitive-memory".into(), "2.1.0".into());
        lock.states.insert("kip://profiles/cognitive-memory".into(), PackageState::Active);
        n.activate_schema(DEFAULT_SPACE, lock).await.unwrap();
        let cx = WriteContext { space: DEFAULT_SPACE.into(), tx_id: "bench-seed".into(), seq: 1,
            at: "2026-09-23T00:00:00.000Z".into(), origin: json!({}) };
        let mut first = ConceptRow::default();
        for i in 0..size {
            let mut row = ConceptRow { name: format!("memory keyword {i}"), schema_ref: format!("kip://profiles/cognitive-memory@2.0.0/{}", if i % 100 == 0 {"Person"} else {"Preference"}), ..Default::default() };
            n.store.insert(&cx, &mut row).await.unwrap();
            if i == 0 { first = row; }
            n.store.insert(&cx, &mut ActivityRow { activity_class: "semantic_consolidation".into(), status: "completed".into(), ..Default::default() }).await.unwrap();
            n.store.insert(&cx, &mut EvidenceRow { evidence_class: "observation".into(), status: "active".into(), ..Default::default() }).await.unwrap();
        }
        // Preload history directly so the baseline's quadratic append cost is
        // measured once per sample, rather than dominating fixture construction.
        for version in 1..=size as u64 {
            first.version = version;
            first.seq = version;
            n.store.element_versions().add_from(&ElementVersionRow {
                _id: 0, space: DEFAULT_SPACE.into(), element: "C-1".into(), kind: "concept".into(),
                version, seq: version, tx_id: format!("version-{version}"), op: "update".into(), row: serde_json::to_value(&first).unwrap(),
            }).await.unwrap();
            n.store.transactions().add_from(&TransactionRow {
                _id: 0, space: DEFAULT_SPACE.into(), tx_id: format!("journal-{version}"), seq: version,
                committed_at: cx.at.clone(), status: "committed".into(), ..Default::default()
            }).await.unwrap();
        }
        let space = n.store.get_space(DEFAULT_SPACE).await.unwrap();
        n.store.spaces().update(space._id, BTreeMap::from([("seq".into(), Fv::U64(size as u64))])).await.unwrap();
        n.store.flush(0).await.unwrap();
        let mut append_ms = Vec::new();
        let mut page_ms = Vec::new();
        let mut search_ms = Vec::new();
        let mut learning_ms = Vec::new();
        let behavior = json!({"task_family":"test", "procedure":"verify"});
        let digest = content_digest(&behavior).unwrap();
        let skill = r#"MUTATE {
          CREATE CONCEPT ?s { TYPE "Skill" SET ATTRIBUTES {skill_class:"workflow",summary:"verify",status:"proposed"} SET STRUCTURAL {("current_revision",?r)} }
          CREATE CONCEPT ?r { TYPE "SkillRevision" SET ATTRIBUTES {task_family:"test",procedure:"verify",behavior_digest: :digest} SET STRUCTURAL {("revision_of",?s)} }
        }"#;
        for sample in 0..6 {
            let mut at = cx.clone();
            at.tx_id = format!("measured-{sample}");
            at.seq = size as u64 + sample + 1;
            let started = Instant::now();
            n.store.record_version(&at, "C-1".parse().unwrap(), at.seq, "update", &first).await.unwrap();
            let append = started.elapsed().as_secs_f64() * 1000.;
            let started = Instant::now();
            let page = n.system_session().change_page(DEFAULT_SPACE, 0, 100).await.unwrap();
            assert_eq!(page["changes"].as_array().unwrap().len(), 100);
            let paged = started.elapsed().as_secs_f64() * 1000.;
            let started = Instant::now();
            execute(&n, r#"SEARCH CONCEPT "keyword" WITH TYPE "Person" LIMIT 10"#, Json::Null).await;
            let searched = started.elapsed().as_secs_f64() * 1000.;
            let started = Instant::now();
            execute(&n, skill, json!({"digest":digest})).await;
            let learned = started.elapsed().as_secs_f64() * 1000.;
            if sample > 0 {
                append_ms.push(append); page_ms.push(paged); search_ms.push(searched); learning_ms.push(learned);
            }
        }
        let average = |v: &[f64]| v.iter().sum::<f64>() / v.len() as f64;
        println!("{}", json!({"size":size, "iterations":5, "version_append_ms":average(&append_ms), "change_page_ms":average(&page_ms), "typed_search_ms":average(&search_ms), "skill_creation_ms":average(&learning_ms)}));
        n.close().await.unwrap();
    }
}
