//! The cross-engine KIP 2.0 conformance suite.
//!
//! Fixtures live in `fixtures/kip-conformance-2.0/` and are meant to be run by
//! *both* this engine and `ts/kip-do`, so they are plain data: a schema to
//! activate, a setup script, and cases with expected results.
//!
//! ## Why the normalization exists
//!
//! Element ids are engine-assigned and differ between runs and between
//! engines, so a fixture cannot name them. They are rewritten to `C:<1>`,
//! `P:<2>` and so on by order of first appearance, which compares *structure*
//! while still catching a wrong reference. Timestamps and transaction ids are
//! dropped for the same reason.
//!
//! Everything else is compared exactly. A fixture that had to be loose about
//! its expected values would not be pinning behaviour down.

use anda_cognitive_nexus::{
    CognitiveNexus,
    nexus::DEFAULT_SPACE,
    schema::{PackageState, SchemaLock, SchemaPackage},
};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{Executor, Json, Request};
use object_store::memory::InMemory;
use serde::Deserialize;
use serde_json::{Map, json};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

const COGNITIVE_MEMORY: &str = include_str!("fixtures/cognitive-memory-2.0.0.json");

#[derive(Deserialize)]
struct Fixture {
    name: String,
    #[allow(dead_code)]
    description: String,
    /// Extra Schema Package artifacts to install and activate, inline.
    #[serde(default)]
    packages: Vec<Json>,
    #[serde(default)]
    setup: Vec<String>,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    command: String,
    #[serde(default)]
    params: Map<String, Json>,
    expect: Expectation,
    /// Whether the order of a top-level result array is part of the contract.
    #[serde(default)]
    ordered: bool,
}

#[derive(Deserialize)]
struct Expectation {
    #[serde(default)]
    result: Option<Json>,
    /// The registry code this case must fail with.
    #[serde(default)]
    error: Option<String>,
}

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/kip-conformance-2.0")
        .canonicalize()
        .expect("the 2.0 conformance fixtures must be present")
}

async fn open(fixture: &Fixture) -> CognitiveNexus {
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: fixture.name.replace(['-', '.', ' '], "_"),
            description: "conformance".to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let nexus = CognitiveNexus::connect(Arc::new(db)).await.unwrap();

    fn declare(source: &str, lock: &mut SchemaLock) -> SchemaPackage {
        let package = SchemaPackage::parse(source).expect("a fixture package must parse");
        let reference = package.package_ref().unwrap();
        lock.packages
            .insert(reference.package_id.clone(), reference.version.to_string());
        lock.states
            .insert(reference.package_id.clone(), PackageState::Active);
        package
    }

    // The Cognitive Memory Profile is always available; a fixture may add
    // packages of its own, which is how it declares the vocabulary its cases
    // need without depending on what some other fixture installed.
    let mut lock = SchemaLock::default();
    let mut sources = vec![COGNITIVE_MEMORY.to_string()];
    sources.extend(fixture.packages.iter().map(Json::to_string));
    for source in &sources {
        let package = declare(source, &mut lock);
        nexus
            .install_package(&package, "conformance")
            .await
            .unwrap();
    }
    nexus.activate_schema(DEFAULT_SPACE, lock).await.unwrap();
    nexus
}

/// Runs one command and flattens the response into `(result, error_code)`.
async fn execute(
    nexus: &CognitiveNexus,
    command: &str,
    params: &Map<String, Json>,
) -> (Option<Json>, Option<String>) {
    let request: Request = serde_json::from_value(json!({
        "kip": "2.0",
        "operations": [{"command": command, "parameters": params}]
    }))
    .expect("a fixture command must build a request");

    let parsed = match request.operations[0].parse() {
        Ok(parsed) => parsed,
        // A parse failure is a real outcome a fixture may assert on.
        Err(err) => return (None, Some(err.name().to_string())),
    };
    let response = nexus
        .execute(parsed, &request, &request.operations[0])
        .await;
    let error = response
        .error
        .as_ref()
        .map(|error| error.code.clone())
        .or_else(|| {
            response
                .results
                .first()
                .and_then(|result| result.error.as_ref().map(|error| error.code.clone()))
        });
    (response.first_result().cloned(), error)
}

/// Rewrites engine-assigned ids to stable ordinals, and drops volatile fields.
#[derive(Default)]
struct Normalizer {
    seen: BTreeMap<String, String>,
}

impl Normalizer {
    fn id(&mut self, raw: &str) -> String {
        let next = self.seen.len() + 1;
        self.seen
            .entry(raw.to_string())
            .or_insert_with(|| {
                let tag = raw.split('-').next().unwrap_or("?");
                format!("{tag}:<{next}>")
            })
            .clone()
    }

    fn value(&mut self, value: &Json) -> Json {
        match value {
            Json::String(text) => {
                if text.parse::<anda_cognitive_nexus::id::ElementId>().is_ok() {
                    Json::String(self.id(text))
                } else {
                    value.clone()
                }
            }
            Json::Array(items) => Json::Array(items.iter().map(|v| self.value(v)).collect()),
            Json::Object(map) => {
                let mut out = Map::new();
                for (key, item) in map {
                    // Wall-clock times and transaction ids differ every run and
                    // between engines; they are engine truth, not behaviour.
                    if matches!(
                        key.as_str(),
                        "created_at"
                            | "updated_at"
                            | "created_tx"
                            | "updated_tx"
                            | "tx_id"
                            | "committed_at"
                            | "valid_at"
                            | "content_digest"
                            | "score"
                    ) {
                        continue;
                    }
                    out.insert(key.clone(), self.value(item));
                }
                Json::Object(out)
            }
            other => other.clone(),
        }
    }
}

fn canonical(value: &Json) -> String {
    // Sorted keys, so two structurally equal answers compare equal regardless
    // of how either engine ordered its object members.
    fn write(value: &Json, out: &mut String) {
        match value {
            Json::Object(map) => {
                let mut keys: Vec<&String> = map.keys().collect();
                keys.sort_unstable();
                out.push('{');
                for (index, key) in keys.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    out.push_str(&Json::String((*key).clone()).to_string());
                    out.push(':');
                    write(&map[*key], out);
                }
                out.push('}');
            }
            Json::Array(items) => {
                out.push('[');
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    write(item, out);
                }
                out.push(']');
            }
            scalar => out.push_str(&scalar.to_string()),
        }
    }
    let mut out = String::new();
    write(value, &mut out);
    out
}

#[tokio::test]
async fn kip_2_conformance() {
    let dir = fixtures_dir();
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("the fixture directory must be readable")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no fixtures found in {}", dir.display());

    let mut failures: Vec<String> = Vec::new();
    let mut cases = 0usize;

    for path in files {
        let source = std::fs::read_to_string(&path).unwrap();
        let fixture: Fixture =
            serde_json::from_str(&source).unwrap_or_else(|err| panic!("{}: {err}", path.display()));
        let nexus = open(&fixture).await;

        for (index, command) in fixture.setup.iter().enumerate() {
            let (_, error) = execute(&nexus, command, &Map::new()).await;
            if let Some(error) = error {
                failures.push(format!(
                    "{} setup[{index}] failed with {error}:\n{command}",
                    fixture.name
                ));
            }
        }

        for case in &fixture.cases {
            cases += 1;
            let (result, error) = execute(&nexus, &case.command, &case.params).await;

            if let Some(expected) = &case.expect.error {
                if error.as_deref() != Some(expected.as_str()) {
                    failures.push(format!(
                        "{} / {}: expected error {expected}, got {error:?} with result {result:?}",
                        fixture.name, case.name
                    ));
                }
                continue;
            }
            if let Some(error) = error {
                failures.push(format!(
                    "{} / {}: unexpected error {error}",
                    fixture.name, case.name
                ));
                continue;
            }
            let Some(expected) = &case.expect.result else {
                continue;
            };

            let mut normalizer = Normalizer::default();
            let mut actual = normalizer.value(&result.unwrap_or(Json::Null));
            let mut expected = expected.clone();
            if !case.ordered {
                // Only the top level reorders: a nested array is usually a
                // list whose order is part of the value.
                if let (Json::Array(a), Json::Array(b)) = (&mut actual, &mut expected) {
                    a.sort_by_key(canonical);
                    b.sort_by_key(canonical);
                }
            }
            if canonical(&actual) != canonical(&expected) {
                failures.push(format!(
                    "{} / {}:\n  expected {}\n  actual   {}",
                    fixture.name,
                    case.name,
                    serde_json::to_string(&expected).unwrap(),
                    serde_json::to_string(&actual).unwrap(),
                ));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {cases} conformance case(s) failed:\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
