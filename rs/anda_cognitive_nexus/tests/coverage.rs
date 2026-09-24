//! The §27 coverage matrix, computed rather than asserted.
//!
//! `KIP-2.0-Invariants.md` Part A registers the 49 cross-cutting invariants
//! §102 requires of every implementation, and names, for each, the conformance
//! vectors that pin it. The registry is blunt about what that means:
//!
//! > A Core invariant without a vector does not exist.
//!
//! The shared fixtures in `fixtures/kip-conformance-2.0/` are KIP's engine
//! suite, copied byte for byte; a case covers a parent vector only where its
//! authors read both and said so — which is what a case's `vectors` field is.
//! This test turns those declarations into the matrix and holds one line:
//!
//! - **no silent regression.** Coverage may rise; a change that drops a
//!   fixture's declaration has to lower the floor deliberately.
//!
//! Run with `--nocapture` to read the matrix itself, which is the point of the
//! test on any day when it passes.

use serde::Deserialize;
use serde_json::{Map, Value as Json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

/// The number of §102 invariants at least one fixture claims to pin.
///
/// A floor, not a target: it goes up when a fixture declares a vector, and it
/// is lowered only by someone who has decided to stop covering something.
const COVERED_INVARIANTS_FLOOR: usize = 29;

#[derive(Deserialize)]
struct Fixture {
    name: String,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    vectors: Vec<String>,
    #[serde(flatten)]
    #[allow(dead_code)]
    rest: Map<String, Json>,
}

/// One row of the registry: the invariant, and the vectors that pin it.
struct Invariant {
    number: usize,
    statement: String,
    vectors: Vec<String>,
}

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/kip-conformance-2.0")
        .canonicalize()
        .expect("the 2.0 conformance fixtures must be present")
}

/// Part A of the vendored invariant registry.
fn registry() -> Vec<Invariant> {
    let text = include_str!("../../anda_kip/Invariants.md");
    let part_a = text
        .split("## Part A.")
        .nth(1)
        .and_then(|rest| rest.split("## Part B.").next())
        .expect("the registry has a Part A");

    let mut rows = Vec::new();
    for line in part_a.lines() {
        let line = line.trim();
        if !line.starts_with('|') {
            continue;
        }
        let cells: Vec<&str> = line.trim_matches('|').split('|').map(str::trim).collect();
        if cells.len() < 4 {
            continue;
        }
        let Ok(number) = cells[0].parse::<usize>() else {
            continue; // the header and its separator
        };
        rows.push(Invariant {
            number,
            statement: cells[1].to_string(),
            vectors: cells[3]
                .split(',')
                .map(|name| name.trim().to_string())
                .filter(|name| !name.is_empty() && name != "—")
                .collect(),
        });
    }
    rows
}

/// Every vector the shared fixtures declare, and which case declared it.
fn declared() -> BTreeMap<String, Vec<String>> {
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut files: Vec<PathBuf> = std::fs::read_dir(fixtures_dir())
        .expect("the fixture directory is readable")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.extension().is_some_and(|ext| ext == "json")
                && path.file_name().is_some_and(|name| name != "manifest.json")
        })
        .collect();
    files.sort();
    for path in files {
        let text = std::fs::read_to_string(&path).expect("a readable fixture");
        let fixture: Fixture =
            serde_json::from_str(&text).unwrap_or_else(|err| panic!("{}: {err}", path.display()));
        for case in fixture.cases {
            for vector in case.vectors {
                out.entry(vector)
                    .or_default()
                    .push(format!("{} / {}", fixture.name, case.name));
            }
        }
    }
    out
}

#[test]
fn the_invariant_coverage_matrix_is_honest_and_does_not_shrink() {
    let registry = registry();
    assert_eq!(
        registry.len(),
        49,
        "§102 registers 49 Core invariants; the vendored registry parsed {} rows",
        registry.len()
    );

    let known: BTreeSet<&str> = registry
        .iter()
        .flat_map(|row| row.vectors.iter().map(String::as_str))
        .collect();
    let declared = declared();

    // The suite is KIP's own, so its vectors are the parent suite's names;
    // one outside Part A pins a Profile invariant (Part B) or a vector with no
    // §102 row, and is reported rather than counted.
    let outside: Vec<&String> = declared
        .keys()
        .filter(|name| !known.contains(name.as_str()))
        .collect();
    if !outside.is_empty() {
        println!(
            "\nvectors outside §102 Part A: {}",
            outside
                .iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    let mut covered = 0;
    println!("\n§102 invariant coverage, from the shared fixtures' own declarations:\n");
    for row in &registry {
        let hits: Vec<&String> = row
            .vectors
            .iter()
            .filter(|vector| declared.contains_key(*vector))
            .collect();
        if !hits.is_empty() {
            covered += 1;
        }
        println!(
            "  {:>2}. [{}] {:<62} {}",
            row.number,
            if hits.is_empty() { ' ' } else { 'x' },
            truncate(&row.statement, 62),
            if hits.is_empty() {
                format!("needs {}", row.vectors.join(", "))
            } else {
                hits.iter()
                    .map(|name| name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        );
    }
    println!(
        "\n  {covered}/49 invariants pinned by at least one declared vector \
         ({} vectors declared across the suite)\n",
        declared.len()
    );

    assert!(
        covered >= COVERED_INVARIANTS_FLOOR,
        "invariant coverage fell from {COVERED_INVARIANTS_FLOOR} to {covered}; a fixture that \
         stopped declaring a vector stopped pinning an invariant §102 requires"
    );
}

fn truncate(text: &str, width: usize) -> String {
    if text.chars().count() <= width {
        return text.to_string();
    }
    text.chars().take(width - 1).collect::<String>() + "…"
}
