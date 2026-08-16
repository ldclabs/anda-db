//! Fuzz-style property tests for the KIP 2.0 parsers.
//!
//! The KQL/KML/META parsers are exposed to external input through
//! `anda_db_server` and the cognitive nexus, so they are an attack surface:
//! whatever bytes arrive, parsing must terminate and return a `Result` —
//! never panic, never hang. Two input generators are used:
//!
//! - completely arbitrary unicode strings, and
//! - mutated valid documents (known-good statements with random splices,
//!   deletions and truncations), which reach much deeper into the grammar than
//!   random noise.
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
    FIND(?drug_class, COUNT(DISTINCT ?drug))
    WHERE {
        ?drug CONCEPT {type: "Drug"}
        (?drug, "is_class_of"|"subclass_of"{1,3}, ?drug_class)
        OPTIONAL { ?edge STRUCTURAL (?drug, "has_step", ?step) }
        FILTER(?drug.attributes.risk_level < 3 && !IS_NULL(?drug_class))
    }
    ORDER BY COUNT(?drug) DESC
    LIMIT 10
    "#,
    r#"
    FIND(?b, ?slot)
    WHERE {
        ?b BELIEF (:alice, "timezone", ?tz)
        ?slot BELIEF SLOT (:alice, "timezone")
        ?a ASSERTION {proposition: (id: "P-1"), stance: "support"}
    }
    AS OF SEQ 4200
    FOR TIME :world_time
    WITH EPISTEMIC { explain: "summary" }
    "#,
];

const KML_SEEDS: &[&str] = &[
    r#"
    ASSERT ?a (:alice, "prefers", :dark_mode) {
        by: :alice,
        mode: "stated",
        confidence: 0.9,
        evidence: [:msg, :screenshot]
    } SUPERSEDING :old
    "#,
    r#"
    MUTATE {
        CREATE CONCEPT ?alice { TYPE "Person" CLIENT KEY "person:alice" NAME "Alice" }
        CREATE EVIDENCE ?msg { SET FIELDS { evidence_class: "user_statement" } }
        ENSURE PROPOSITION ?p (?alice, "prefers", :dark_mode)
        CREATE ASSERTION ?claim {
            SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: "support" }
            SET STRUCTURAL { ("evidence", ?msg) {role: "support"} }
        }
        UPSERT CONCEPT ?drug {
            MATCH {key: "drug:aspirin"}
            EXPECT VERSION 3
            SET ATTRIBUTES { risk_level: 2 }
            UNSET ATTRIBUTES { deprecated_note }
            SET FACET "MnemonicState" { salience: 0.4 }
        }
        UPDATE ?c
            SET FACET "MnemonicState" { memory_strength: MUL(?c.facets["MnemonicState"].memory_strength, 0.99) }
            WHERE { ?c CONCEPT {type: "Experience"} }
            LIMIT 100
        TRANSITION ACTIVITY :act TO "succeeded" SET FIELDS { ended_at: :now }
        PURGE :leak REFERENCE POLICY "detach" CONFIRM "PURGE"
    }
    "#,
];

const META_SEEDS: &[&str] = &[
    "DESCRIBE PRIMER MODE \"compact\"",
    "DESCRIBE TYPE \"Person\"",
    "LIST SCHEMA PACKAGES STATUS \"active\" LIMIT 20 CURSOR :page",
    "SEARCH COGNITION \"dark mode\" WITH TYPE \"Preference\" MODE \"hybrid\" THRESHOLD 0.7 LIMIT 5",
    "HISTORY ELEMENT \"C-1\" FROM SEQ 1 TO SEQ 99 LIMIT 10",
    "EXPORT CAPSULE :out WHERE { ?c CONCEPT {type: \"Experience\"} } WITH { redact: true } AS OF SEQ 7",
];

/// Snippets spliced into seeds to stress token boundaries.
const SPLICES: &[&str] = &[
    "{",
    "}",
    "(",
    ")",
    "[",
    "]",
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
    "|",
    "\u{0}",
    "\u{7f}",
    "🦀",
    "NULL",
    "FIND",
    "MUTATE",
    "ASSERT",
    "BELIEF",
    "SLOT",
    "SUPERSEDING",
    "PURGE",
    "CONFIRM",
    "WHERE",
    "META",
    "ATTRIBUTES",
    "STRUCTURAL",
    "EXPECT",
    "AS OF",
    "0x",
    "1e999",
    "-0",
    "{1,}",
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

    /// Mutated META commands, which have the widest keyword dispatch surface.
    #[test]
    fn mutated_meta_never_panics(
        seed in 0usize..META_SEEDS.len(),
        mutations in prop::collection::vec(mutation_strategy(), 1..6),
    ) {
        let input = apply_mutations(META_SEEDS[seed], &mutations);
        all_parsers_terminate(&input);
    }

    /// `unquote_str` must invert `quote_str` for every string.
    #[test]
    fn quote_unquote_roundtrip(input in "\\PC{0,200}") {
        let quoted = quote_str(&input);
        prop_assert_eq!(unquote_str(&quoted), Some(input));
    }
}
