//! Cross-engine KIP conformance suite.
//!
//! Runs the fixtures in `fixtures/kip-conformance/` against this engine. The
//! same files drive the TypeScript engine's suite in
//! `js/kip-do/test/conformance.test.ts`; a case that passes in one and fails
//! in the other is a divergence between two implementations of the same
//! protocol, which is what the suite exists to surface.
//!
//! The normalization below is specified in `fixtures/kip-conformance/README.md`
//! and implemented twice, once per language. Keep the two in step: a
//! difference here silently weakens every assertion rather than failing.

use std::{collections::BTreeMap, fs, path::PathBuf, sync::Arc};

use anda_cognitive_nexus::CognitiveNexus;
use anda_db::{
    database::{AndaDB, DBConfig},
    storage::StorageConfig,
};
use anda_kip::{Command, Executor, Response, parse_kip};
use object_store::memory::InMemory;
use serde::Deserialize;
use serde_json::{Map, Value, json};
// ---------------------------------------------------------------------------
// Fixture schema — mirrors `js/kip-do/test/conformance/normalize.ts`.
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct Fixture {
    name: String,
    #[allow(dead_code)]
    #[serde(default)]
    description: String,
    #[serde(default)]
    setup: Vec<String>,
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize)]
struct Case {
    name: String,
    command: String,
    expect: Expectation,
    #[serde(default)]
    ordered: bool,
    #[serde(default)]
    skip: Option<Skip>,
}

#[derive(Debug, Deserialize)]
struct Expectation {
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    next_cursor: Option<Option<String>>,
    #[serde(default)]
    error: Option<ExpectedError>,
}

#[derive(Debug, Deserialize)]
struct ExpectedError {
    code: String,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Skip {
    #[serde(default)]
    rust: Option<String>,
    #[allow(dead_code)]
    #[serde(default)]
    ts: Option<String>,
}

// ---------------------------------------------------------------------------
// Normalization
// ---------------------------------------------------------------------------

/// Wall-clock fields that cannot match across two runs, let alone two engines.
const VOLATILE_METADATA_KEYS: &[&str] = &["_created_at", "_updated_at"];

/// Assigns positional tokens to entity ids in first-appearance order.
///
/// Absolute document ids depend on how many rows the engine created before
/// this one, and this engine seeds bootstrap capsules that the TypeScript one
/// does not. Numbering by first appearance keeps identity *relationships*
/// asserted while dropping the absolute values, which carry no meaning.
#[derive(Default)]
struct IdMapper {
    seen: BTreeMap<u64, usize>,
}

impl IdMapper {
    fn token(&mut self, raw: &str) -> Option<String> {
        if let Some(rest) = raw.strip_prefix("C:") {
            let id: u64 = rest.parse().ok()?;
            return Some(format!("C:<{}>", self.ordinal(id)));
        }
        if let Some(rest) = raw.strip_prefix("P:") {
            // Only the first `:` separates the id from the predicate — a
            // predicate may itself contain `:`.
            let (id_part, predicate) = rest.split_once(':')?;
            let id: u64 = id_part.parse().ok()?;
            return Some(format!("P:<{}>:{}", self.ordinal(id), predicate));
        }
        None
    }

    fn ordinal(&mut self, id: u64) -> usize {
        let next = self.seen.len() + 1;
        *self.seen.entry(id).or_insert(next)
    }
}

fn normalize(value: &Value, ordered: bool) -> Value {
    let mut mapper = IdMapper::default();
    let walked = walk(value, &mut mapper, false);
    if !ordered && let Value::Array(mut items) = walked {
        items.sort_by_cached_key(canonical);
        return Value::Array(items);
    }
    walked
}

fn walk(value: &Value, mapper: &mut IdMapper, in_metadata: bool) -> Value {
    match value {
        Value::String(s) => match mapper.token(s) {
            Some(token) => Value::String(token),
            None => value.clone(),
        },
        // Nested arrays are data: their order is part of the value, so only
        // the top-level result array is ever reordered.
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| walk(item, mapper, in_metadata))
                .collect(),
        ),
        Value::Object(map) => {
            // `serde_json::Map` iterates in sorted order under the default
            // (BTreeMap) feature, which is what makes the id numbering match
            // the TypeScript side's explicit key sort.
            let mut out = Map::new();
            for (key, item) in map {
                if in_metadata && VOLATILE_METADATA_KEYS.contains(&key.as_str()) {
                    continue;
                }
                out.insert(
                    key.clone(),
                    walk(item, mapper, in_metadata || key == "metadata"),
                );
            }
            Value::Object(out)
        }
        _ => value.clone(),
    }
}

/// Canonical encoding with object keys sorted, for stable comparison.
fn canonical(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let parts: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{}:{}", json!(k), canonical(v)))
                .collect();
            format!("{{{}}}", parts.join(","))
        }
        Value::Array(items) => {
            let parts: Vec<String> = items.iter().map(canonical).collect();
            format!("[{}]", parts.join(","))
        }
        other => other.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn fixtures_dir() -> PathBuf {
    // CARGO_MANIFEST_DIR is `<repo>/rs/anda_cognitive_nexus`.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/kip-conformance")
        .canonicalize()
        .expect("fixtures/kip-conformance must exist")
}

fn load_fixtures() -> Vec<(String, Fixture)> {
    let dir = fixtures_dir();
    let mut out = Vec::new();
    let mut entries: Vec<_> = fs::read_dir(&dir)
        .expect("read fixtures dir")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "json"))
        .collect();
    // Stable order so a failure reports the same way on every machine.
    entries.sort();

    for path in entries {
        let raw = fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let fixture: Fixture = serde_json::from_str(&raw)
            .unwrap_or_else(|e| panic!("parse {}: {e}", path.display()));
        out.push((path.display().to_string(), fixture));
    }
    out
}

/// Fresh in-memory database per fixture.
///
/// `InMemory` rather than a temp directory: the suite runs many small
/// databases and the object-store round trips dominate otherwise.
async fn fresh_nexus(name: &str) -> CognitiveNexus {
    let store = Arc::new(InMemory::new());
    // Database names are validated as identifiers; fixture names are
    // kebab-case file names, so map the separator rather than constrain how
    // fixtures may be named.
    let safe_name = name.replace(['-', '.', ' '], "_");
    let db = AndaDB::connect(
        store,
        DBConfig {
            name: format!("conformance_{safe_name}"),
            description: "KIP conformance fixture database".to_string(),
            storage: StorageConfig::default(),
            lock: None,
        },
    )
    .await
    .expect("connect");

    CognitiveNexus::connect(Arc::new(db), async |_| Ok(()))
        .await
        .expect("connect nexus")
}

/// Runs one command and flattens the response into (result, cursor, error).
async fn execute(
    nexus: &CognitiveNexus,
    source: &str,
) -> (Option<Value>, Option<String>, Option<(String, String)>) {
    let command: Command = match parse_kip(source) {
        Ok(command) => command,
        Err(err) => {
            return (None, None, Some((err.code_str().to_string(), err.message)));
        }
    };
    match nexus.execute(command, false).await {
        Response::Ok {
            result,
            next_cursor,
        } => (Some(result), next_cursor, None),
        Response::Err { error, .. } => (None, None, Some((error.code, error.message))),
    }
}

#[tokio::test]
async fn kip_conformance() {
    let fixtures = load_fixtures();
    assert!(
        !fixtures.is_empty(),
        "no conformance fixtures found in {}",
        fixtures_dir().display()
    );

    let mut failures: Vec<String> = Vec::new();
    let mut ran = 0usize;
    let mut skipped = 0usize;

    for (path, fixture) in &fixtures {
        let nexus = fresh_nexus(&fixture.name).await;

        for command in &fixture.setup {
            let (_, _, error) = execute(&nexus, command).await;
            if let Some((code, message)) = error {
                panic!(
                    "setup failed for fixture {} ({path}): {code} {message}\n  {command}",
                    fixture.name
                );
            }
        }

        for case in &fixture.cases {
            // Cases accumulate state, so a skipped case must still execute —
            // only its assertions are dropped. Skipping execution would leave
            // this engine's database in a different state from the other
            // engine's and silently invalidate every later case.
            let (result, next_cursor, error) = execute(&nexus, &case.command).await;

            if let Some(reason) = case.skip.as_ref().and_then(|s| s.rust.as_ref()) {
                eprintln!("skip {}/{}: {reason}", fixture.name, case.name);
                skipped += 1;
                continue;
            }
            ran += 1;

            if let Some(expected) = &case.expect.error {
                match error {
                    None => failures.push(format!(
                        "{}/{}: expected {} but the command succeeded with {}",
                        fixture.name,
                        case.name,
                        expected.code,
                        result.unwrap_or(Value::Null)
                    )),
                    Some((code, message)) => {
                        if code != expected.code {
                            failures.push(format!(
                                "{}/{}: expected {} but got {code}: {message}",
                                fixture.name, case.name, expected.code
                            ));
                        } else if let Some(needle) = &expected.message
                            && !message.contains(needle)
                        {
                            failures.push(format!(
                                "{}/{}: message {message:?} does not contain {needle:?}",
                                fixture.name, case.name
                            ));
                        }
                    }
                }
                continue;
            }

            if let Some((code, message)) = error {
                failures.push(format!(
                    "{}/{}: expected a result but got {code}: {message}",
                    fixture.name, case.name
                ));
                continue;
            }

            let Some(expected_result) = &case.expect.result else {
                failures.push(format!(
                    "{}/{}: case asserts nothing",
                    fixture.name, case.name
                ));
                continue;
            };

            let actual = normalize(&result.unwrap_or(Value::Null), case.ordered);
            let wanted = normalize(expected_result, case.ordered);
            if canonical(&actual) != canonical(&wanted) {
                failures.push(format!(
                    "{}/{}:\n  expected {}\n  actual   {}",
                    fixture.name,
                    case.name,
                    canonical(&wanted),
                    canonical(&actual)
                ));
            }

            if let Some(expected_cursor) = &case.expect.next_cursor
                && next_cursor.as_deref() != expected_cursor.as_deref()
            {
                failures.push(format!(
                    "{}/{}: expected cursor {expected_cursor:?} but got {next_cursor:?}",
                    fixture.name, case.name
                ));
            }
        }

        nexus.db.close().await.ok();
    }

    eprintln!("conformance: {ran} cases run, {skipped} skipped");
    assert!(
        failures.is_empty(),
        "{} conformance failure(s):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}

/// Pins the normalizer against the cases the TypeScript side mirrors.
///
/// The two implementations are independent, so without this the suite could
/// drift into comparing differently-normalized values and quietly pass.
#[test]
fn normalizer_matches_the_specification() {
    // Ids are numbered by first appearance, and repeats reuse their token.
    let value = json!([
        {"id": "C:41", "ref": "C:41"},
        {"id": "P:7:treats", "subject": "C:41", "object": "C:99"}
    ]);
    assert_eq!(
        canonical(&normalize(&value, true)),
        canonical(&json!([
            {"id": "C:<1>", "ref": "C:<1>"},
            {"id": "P:<2>:treats", "subject": "C:<1>", "object": "C:<3>"}
        ]))
    );

    // Volatile metadata is dropped; `_version` survives because
    // `EXPECT VERSION` depends on it.
    let value = json!({
        "metadata": {"_version": 3, "_created_at": "x", "_updated_at": "y", "src": "t"}
    });
    assert_eq!(
        canonical(&normalize(&value, true)),
        canonical(&json!({"metadata": {"_version": 3, "src": "t"}}))
    );

    // A predicate containing a colon survives tokenization intact.
    assert_eq!(
        canonical(&normalize(&json!(["P:5:a:b"]), true)),
        canonical(&json!(["P:<1>:a:b"]))
    );

    // Unordered results sort; nested arrays keep their order.
    assert_eq!(
        canonical(&normalize(&json!([["b", 2], ["a", 1]]), false)),
        canonical(&json!([["a", 1], ["b", 2]]))
    );
    assert_eq!(
        canonical(&normalize(&json!([["b", "a"]]), false)),
        canonical(&json!([["b", "a"]]))
    );
}
