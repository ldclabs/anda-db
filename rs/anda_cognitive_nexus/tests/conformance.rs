//! The KIP 2.0 engine suite, run against this engine.
//!
//! The fixtures in `fixtures/kip-conformance-2.0/` are a byte-for-byte copy of
//! KIP's `conformance/engine-suite/` (`make sync-kip-conformance`), which both
//! reference engines run. This harness reproduces KIP's own runner
//! (`conformance/engine-runner.mjs`): one command per case — or, for a
//! request-level contract, one `operations` batch sent as a single request —
//! flattened to a result or an error code, ids normalized by a sorted-key walk.
//!
//! Three outcomes besides PASS and FAIL, all reported rather than hidden:
//!
//! - an `UnsupportedCapability` the case did not expect is **SKIP**, never a
//!   pass — the capability is optional and this engine says it lacks it;
//! - a fixture marked `"status": "pending_engine"` has been verified by no
//!   engine yet; its failures are listed but do not fail this test, and its
//!   passes are new evidence;
//! - `expect.result_contains` matches deployment-extensible META answers
//!   partially, exactly as the runner does.

use anda_cognitive_nexus::{
    CognitiveNexus,
    nexus::DEFAULT_SPACE,
    schema::{PackageState, SchemaLock, SchemaPackage},
};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{Executor, Json, Request, execute_request};
use object_store::memory::InMemory;
use serde::Deserialize;
use serde_json::{Map, json};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

const COGNITIVE_MEMORY: &str = anda_cognitive_nexus::profiles::COGNITIVE_MEMORY;

#[derive(Deserialize)]
struct Fixture {
    name: String,
    #[allow(dead_code)]
    description: String,
    /// `pending_engine` while no engine has verified the fixture.
    #[serde(default)]
    status: Option<String>,
    /// Extra Schema Package artifacts to install and activate, inline.
    #[serde(default)]
    packages: Vec<Json>,
    #[serde(default)]
    setup: Vec<Setup>,
    cases: Vec<Case>,
}

/// A setup step: a bare command, or one whose raw result is captured into
/// parameters for later steps and cases.
#[derive(Deserialize)]
#[serde(untagged)]
enum Setup {
    Command(String),
    Step {
        command: String,
        #[serde(default)]
        params: Map<String, Json>,
        /// Parameter name → JSON Pointer into the command's raw result.
        #[serde(default)]
        capture: BTreeMap<String, String>,
    },
}

#[derive(Deserialize)]
struct Case {
    name: String,
    /// The one command, or `None` for a batch case.
    #[serde(default)]
    command: Option<String>,
    /// A batch sent as one multi-operation request; its `envelope` declares
    /// the execution mode §75 requires.
    #[serde(default)]
    operations: Option<Vec<BatchOperation>>,
    #[serde(default)]
    params: Map<String, Json>,
    expect: Expectation,
    /// Whether the order of a top-level result array is part of the contract.
    #[serde(default)]
    ordered: bool,
    /// Extra request-envelope members, merged over the ones the harness builds.
    #[serde(default)]
    envelope: Map<String, Json>,
    /// The parent-suite vectors this case pins (`tests/coverage.rs`).
    #[serde(default)]
    #[allow(dead_code)]
    vectors: Vec<String>,
}

#[derive(Deserialize)]
struct BatchOperation {
    command: String,
    #[serde(default)]
    params: Map<String, Json>,
}

#[derive(Deserialize)]
struct Expectation {
    #[serde(default)]
    result: Option<Json>,
    /// A partial match: members and rows the answer must contain.
    #[serde(default)]
    result_contains: Option<Json>,
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
    envelope: &Map<String, Json>,
) -> (Option<Json>, Option<String>) {
    let mut body = json!({
        "kip": "2.0",
        "operations": [{"command": command, "parameters": params}]
    });
    let object = body.as_object_mut().expect("the harness builds an object");
    for (key, value) in envelope {
        object.insert(key.clone(), value.clone());
    }
    // An envelope member of the wrong shape never becomes a Request at all,
    // and that refusal is itself a conformance outcome: §71.1 makes
    // `source_actor` an element reference (`{id}` or `{type, key}`), so a bare
    // string is `InvalidRequestEnvelope` before any command runs — which is
    // exactly what a transport that deserializes the body reports.
    let request = match Request::from_value(body) {
        Ok(request) => request,
        Err(err) => return (None, Some(err.name().to_string())),
    };

    // The structural gate a real transport runs before dispatch
    // (`anda_kip::execute_request`). The harness calls one operation directly,
    // so without this an envelope invariant — an `ingest` block with nothing to
    // mint into, a capability name that is not an identifier — would be
    // enforced in production and invisible here.
    if let Err(err) = request.validate() {
        return (None, Some(err.name().to_string()));
    }

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

/// Runs a batch case as one multi-operation request through the ordinary
/// batch path, and flattens it as KIP's runner does: the top-level error, else
/// the first operation error in order, else the first result.
async fn execute_batch(
    nexus: &CognitiveNexus,
    operations: &[BatchOperation],
    captured: &Map<String, Json>,
    envelope: &Map<String, Json>,
) -> (Option<Json>, Option<String>) {
    let operations: Vec<Json> = operations
        .iter()
        .map(|operation| {
            let mut params = captured.clone();
            params.extend(operation.params.clone());
            json!({"command": operation.command, "parameters": params})
        })
        .collect();
    let mut body = json!({"kip": "2.0", "operations": operations});
    let object = body.as_object_mut().expect("the harness builds an object");
    for (key, value) in envelope {
        object.insert(key.clone(), value.clone());
    }
    let request = match Request::from_value(body) {
        Ok(request) => request,
        Err(err) => return (None, Some(err.name().to_string())),
    };
    let response = execute_request(nexus, &request).await;
    let error = response
        .error
        .as_ref()
        .map(|error| error.code.clone())
        .or_else(|| {
            response
                .results
                .iter()
                .find_map(|result| result.error.as_ref().map(|error| error.code.clone()))
        });
    (response.first_result().cloned(), error)
}

/// Engine truth rather than behaviour: dropped before comparison.
const VOLATILE: &[&str] = &[
    "created_at",
    "updated_at",
    "authorization_view",
    "created_tx",
    "updated_tx",
    "tx_id",
    "committed_at",
    "valid_at",
    "content_digest",
    "score",
];

/// Ids become `C:<1>`, `P:<2>`, … in the order a sorted-key walk reaches them,
/// one counter across every kind (KIP `engine-runner.mjs`).
#[derive(Default)]
struct Normalizer {
    seen: BTreeMap<String, String>,
}

impl Normalizer {
    fn value(&mut self, value: &Json) -> Json {
        match value {
            Json::String(text) => {
                if text.parse::<anda_cognitive_nexus::id::ElementId>().is_ok() {
                    let next = self.seen.len() + 1;
                    let tag = text.split('-').next().unwrap_or("?");
                    Json::String(
                        self.seen
                            .entry(text.clone())
                            .or_insert_with(|| format!("{tag}:<{next}>"))
                            .clone(),
                    )
                } else {
                    value.clone()
                }
            }
            Json::Array(items) => Json::Array(items.iter().map(|v| self.value(v)).collect()),
            Json::Object(map) => {
                let mut keys: Vec<&String> = map.keys().collect();
                keys.sort_unstable();
                let mut out = Map::new();
                for key in keys {
                    if !VOLATILE.contains(&key.as_str()) {
                        out.insert(key.clone(), self.value(&map[key]));
                    }
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

/// `expect.result_contains`: objects match member by member, an expected array
/// needs a matching actual row per expected row, scalars match exactly.
fn contains(actual: &Json, expected: &Json) -> bool {
    match expected {
        Json::Array(rows) => actual.as_array().is_some_and(|actual| {
            rows.iter()
                .all(|row| actual.iter().any(|a| contains(a, row)))
        }),
        Json::Object(members) => actual.as_object().is_some_and(|actual| {
            members
                .iter()
                .all(|(key, value)| actual.get(key).is_some_and(|a| contains(a, value)))
        }),
        scalar => actual == scalar,
    }
}

enum Outcome {
    Pass,
    Skip(String),
    Fail(String),
}

async fn run_case(nexus: &CognitiveNexus, case: &Case, captured: &Map<String, Json>) -> Outcome {
    let (result, error) = match (&case.command, &case.operations) {
        (Some(command), None) => {
            let mut params = captured.clone();
            params.extend(case.params.clone());
            execute(nexus, command, &params, &case.envelope).await
        }
        (None, Some(operations)) => {
            execute_batch(nexus, operations, captured, &case.envelope).await
        }
        _ => return Outcome::Fail("a case carries one command or one operations batch".into()),
    };
    let expected_error = case.expect.error.as_deref();
    if let Some(error) = error {
        return if error == "UnsupportedCapability" && expected_error != Some(error.as_str()) {
            Outcome::Skip(error)
        } else if expected_error == Some(error.as_str()) {
            Outcome::Pass
        } else {
            Outcome::Fail(format!(
                "expected {}, got {error}",
                expected_error.unwrap_or("a result")
            ))
        };
    }
    if let Some(expected) = expected_error {
        return Outcome::Fail(format!("expected {expected}, got {result:?}"));
    }
    let result = result.unwrap_or(Json::Null);
    let mut normalizer = Normalizer::default();
    let mut actual = normalizer.value(&result);
    if let Some(expected) = &case.expect.result {
        let mut expected = expected.clone();
        if !case.ordered
            && let (Json::Array(a), Json::Array(b)) = (&mut actual, &mut expected)
        {
            a.sort_by_key(canonical);
            b.sort_by_key(canonical);
        }
        if canonical(&actual) != canonical(&expected) {
            return Outcome::Fail(format!(
                "\n  expected {}\n  actual   {}",
                serde_json::to_string(&expected).unwrap(),
                serde_json::to_string(&actual).unwrap(),
            ));
        }
    }
    if let Some(expected) = &case.expect.result_contains
        && !contains(&result, expected)
    {
        return Outcome::Fail(format!(
            "\n  expected to contain {}\n  actual {}",
            serde_json::to_string(expected).unwrap(),
            serde_json::to_string(&actual).unwrap(),
        ));
    }
    Outcome::Pass
}

fn pointer(value: &Json, path: &str) -> Option<Json> {
    if path.is_empty() {
        Some(value.clone())
    } else {
        value.pointer(path).cloned()
    }
}

#[tokio::test]
async fn kip_2_conformance() {
    let dir = fixtures_dir();
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("the fixture directory must be readable")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.extension().is_some_and(|ext| ext == "json")
                && path.file_name().is_some_and(|name| name != "manifest.json")
        })
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no fixtures found in {}", dir.display());

    let mut failures: Vec<String> = Vec::new();
    let mut pending: Vec<String> = Vec::new();
    let mut skips: Vec<String> = Vec::new();
    let (mut cases, mut passed) = (0usize, 0usize);

    for path in files {
        let source = std::fs::read_to_string(&path).unwrap();
        let fixture: Fixture =
            serde_json::from_str(&source).unwrap_or_else(|err| panic!("{}: {err}", path.display()));
        let is_pending = fixture.status.as_deref() == Some("pending_engine");
        let report = if is_pending {
            &mut pending
        } else {
            &mut failures
        };
        let nexus = open(&fixture).await;

        let mut captured = Map::new();
        let mut setup_ok = true;
        for (index, step) in fixture.setup.iter().enumerate() {
            let (command, params, capture) = match step {
                Setup::Command(command) => (command, Map::new(), BTreeMap::new()),
                Setup::Step {
                    command,
                    params,
                    capture,
                } => (command, params.clone(), capture.clone()),
            };
            let mut merged = captured.clone();
            merged.extend(params);
            let (result, error) = execute(&nexus, command, &merged, &Map::new()).await;
            if let Some(error) = error {
                report.push(format!(
                    "{} setup[{index}] failed with {error}:\n{command}",
                    fixture.name
                ));
                setup_ok = false;
                break;
            }
            let result = result.unwrap_or(Json::Null);
            for (name, path) in capture {
                match pointer(&result, &path) {
                    Some(value) => {
                        captured.insert(name, value);
                    }
                    None => {
                        report.push(format!(
                            "{} setup[{index}] result is missing {path}",
                            fixture.name
                        ));
                        setup_ok = false;
                    }
                }
            }
            if !setup_ok {
                break;
            }
        }
        if !setup_ok {
            cases += fixture.cases.len();
            continue;
        }

        for case in &fixture.cases {
            cases += 1;
            match run_case(&nexus, case, &captured).await {
                Outcome::Pass => passed += 1,
                Outcome::Skip(code) => {
                    skips.push(format!("{} / {}: {code}", fixture.name, case.name))
                }
                Outcome::Fail(message) => {
                    report.push(format!("{} / {}: {message}", fixture.name, case.name))
                }
            }
        }
    }

    println!(
        "engine suite: {passed} passed, {} skipped, {} failed, {} pending-engine failures, of {cases}",
        skips.len(),
        failures.len(),
        pending.len()
    );
    for skip in &skips {
        println!("SKIP_UNSUPPORTED {skip}");
    }
    for failure in &pending {
        println!("PENDING_ENGINE {failure}");
    }
    assert!(
        failures.is_empty(),
        "{} of {cases} conformance case(s) failed:\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
