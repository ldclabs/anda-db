//! Fuzz-style property tests for the KIP parsers.
//!
//! The KQL/KML/META parsers are exposed to external input through
//! `anda_db_server` and the cognitive nexus, so they are an attack surface:
//! whatever bytes arrive, parsing must terminate and return a `Result` —
//! never panic, never hang. Two input generators are used:
//!
//! - completely arbitrary unicode strings, and
//! - mutated valid documents (the shipped `.kip` capsules and known-good
//!   statements with random splices, deletions and truncations), which reach
//!   much deeper into the grammar than random noise.
//!
//! A `cargo fuzz` setup with the same targets lives in `rs/anda_kip/fuzz/`
//! for open-ended coverage-guided runs; these proptest cases are the
//! always-on subset executed by `cargo test`.

use anda_kip::{parse_json, parse_kip, parse_kml, parse_kql, parse_meta, quote_str, unquote_str};
use proptest::prelude::*;

/// Valid documents used both as mutation seeds and as parse-Ok regressions.
const KQL_SEEDS: &[&str] = &[
    r#"
    FIND(?drug.name)
    WHERE {
        ?drug {type: "Drug"}
    }
    "#,
    r#"
    FIND(?drug_class, COUNT(?drug))
    WHERE {
        ?drug {type: "Drug"}
        (?drug, "is_class_of", ?drug_class)
    }
    "#,
];

const KML_SEEDS: &[&str] = &[r#"
    UPSERT {
        CONCEPT ?drug {
            { type: "Drug", name: "Aspirin" }
            SET ATTRIBUTES {
                molecular_formula: "C9H8O4",
                risk_level: 2
            }
        }
    }
    "#];

const META_SEEDS: &[&str] = &["DESCRIBE PRIMER", "DESCRIBE CONCEPT TYPE \"Drug\""];

/// Larger real-world documents (knowledge capsules shipped with the crate).
const CAPSULE_SEEDS: &[&str] = &[
    include_str!("../capsules/Person.kip"),
    include_str!("../capsules/Event.kip"),
    include_str!("../capsules/Genesis.kip"),
    include_str!("../capsules/Insight.kip"),
];

/// Snippets spliced into seeds to stress token boundaries.
const SPLICES: &[&str] = &[
    "{",
    "}",
    "(",
    ")",
    "\"",
    "\\",
    "'",
    "?",
    "_",
    ",",
    "@",
    "$",
    ":",
    ";",
    "\u{0}",
    "\u{7f}",
    "🦀",
    "NULL",
    "FIND",
    "UPSERT",
    "DELETE",
    "WHERE",
    "META",
    "ATTRIBUTES",
    "ON",
    "0x",
    "1e999",
    "-0",
    "//",
    "/*",
    "*/",
    "\n",
    "\r\n",
    "\t",
    "?var",
    "\"unterminated",
];

fn all_parsers_terminate(input: &str) {
    let _ = parse_kip(input);
    let _ = parse_kql(input);
    let _ = parse_kml(input);
    let _ = parse_meta(input);
    let _ = parse_json(input);
}

#[derive(Debug, Clone)]
enum Mutation {
    /// Remove a range of characters.
    Delete { at: usize, len: usize },
    /// Insert a splice snippet.
    Insert { at: usize, splice: usize },
    /// Truncate the document.
    Truncate { at: usize },
    /// Duplicate a range of characters.
    Duplicate { at: usize, len: usize },
}

fn mutation_strategy() -> impl Strategy<Value = Mutation> {
    prop_oneof![
        (0usize..10_000, 1usize..64).prop_map(|(at, len)| Mutation::Delete { at, len }),
        (0usize..10_000, 0usize..SPLICES.len())
            .prop_map(|(at, splice)| Mutation::Insert { at, splice }),
        (0usize..10_000).prop_map(|at| Mutation::Truncate { at }),
        (0usize..10_000, 1usize..64).prop_map(|(at, len)| Mutation::Duplicate { at, len }),
    ]
}

fn apply_mutations(seed: &str, mutations: &[Mutation]) -> String {
    // Work on a char vector so every mutation keeps the input valid UTF-8.
    let mut chars: Vec<char> = seed.chars().collect();
    for mutation in mutations {
        if chars.is_empty() {
            break;
        }
        match mutation {
            Mutation::Delete { at, len } => {
                let at = at % chars.len();
                let end = (at + len).min(chars.len());
                chars.drain(at..end);
            }
            Mutation::Insert { at, splice } => {
                let at = at % (chars.len() + 1);
                chars.splice(at..at, SPLICES[*splice].chars());
            }
            Mutation::Truncate { at } => {
                chars.truncate(at % (chars.len() + 1));
            }
            Mutation::Duplicate { at, len } => {
                let at = at % chars.len();
                let end = (at + len).min(chars.len());
                let dup: Vec<char> = chars[at..end].to_vec();
                chars.splice(end..end, dup);
            }
        }
    }
    chars.into_iter().collect()
}

#[test]
fn seed_documents_parse_ok() {
    for seed in KQL_SEEDS {
        parse_kql(seed).unwrap_or_else(|err| panic!("KQL seed failed to parse: {err:?}"));
    }
    for seed in KML_SEEDS {
        parse_kml(seed).unwrap_or_else(|err| panic!("KML seed failed to parse: {err:?}"));
    }
    for seed in META_SEEDS {
        parse_meta(seed).unwrap_or_else(|err| panic!("META seed failed to parse: {err:?}"));
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Arbitrary unicode input must never panic any parser.
    #[test]
    fn arbitrary_input_never_panics(input in "\\PC{0,300}") {
        all_parsers_terminate(&input);
    }

    /// Arbitrary input including control characters and separators.
    #[test]
    fn arbitrary_bytes_never_panic(input in prop::collection::vec(any::<char>(), 0..200)) {
        let input: String = input.into_iter().collect();
        all_parsers_terminate(&input);
    }

    /// Mutated valid statements reach deep grammar paths without panicking.
    #[test]
    fn mutated_statements_never_panic(
        seed in 0usize..(KQL_SEEDS.len() + KML_SEEDS.len() + META_SEEDS.len()),
        mutations in prop::collection::vec(mutation_strategy(), 1..10),
    ) {
        let seeds: Vec<&str> = KQL_SEEDS
            .iter()
            .chain(KML_SEEDS)
            .chain(META_SEEDS)
            .copied()
            .collect();
        let input = apply_mutations(seeds[seed], &mutations);
        all_parsers_terminate(&input);
    }

    /// Mutated knowledge capsules (large, deeply nested documents).
    #[test]
    fn mutated_capsules_never_panic(
        seed in 0usize..CAPSULE_SEEDS.len(),
        mutations in prop::collection::vec(mutation_strategy(), 1..6),
    ) {
        let input = apply_mutations(CAPSULE_SEEDS[seed], &mutations);
        all_parsers_terminate(&input);
    }

    /// `unquote_str` must invert `quote_str` for every string.
    #[test]
    fn quote_unquote_roundtrip(input in "\\PC{0,200}") {
        let quoted = quote_str(&input);
        prop_assert_eq!(unquote_str(&quoted), Some(input));
    }
}
