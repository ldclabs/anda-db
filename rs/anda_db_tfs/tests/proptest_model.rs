//! Model-based property tests for [`BM25Index`].
//!
//! Random insert/remove sequences run against both the real index and a
//! naive, trivially-correct inverted index. BM25 assigns scores, but the
//! *retrieval set* is exact: a single-term query must return precisely the
//! live documents containing that term, and boolean queries must return the
//! corresponding set algebra. Scores must be finite, positive and sorted.
//! All checks are repeated after a flush/load round-trip with tiny buckets so
//! bucket splitting is exercised.

#![cfg(feature = "tantivy")]

use anda_db_tfs::{
    BM25Config, BM25Error, BM25Index, BucketObject, TokenizerChain, default_tokenizer,
};
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};

/// Token alphabet chosen so the Porter stemmer maps every word to itself,
/// keeping the reference model independent of stemming.
const WORDS: &[&str] = &[
    "red", "blue", "fox", "dog", "sun", "moon", "rock", "wind", "salt", "gold", "iron", "wolf",
];

#[derive(Debug, Clone)]
enum Op {
    /// Insert a new document containing these (possibly repeated) words.
    Insert(Vec<usize>),
    /// Remove a live document, selected by index into the live set.
    RemoveLive(usize),
    /// Remove a document id that is not live; must be a no-op.
    RemoveMissing(u64),
    /// Re-insert a live document id; must fail with `AlreadyExists`.
    ReinsertLive(usize),
    Purge(Vec<usize>),
    Compact,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => prop::collection::vec(0usize..WORDS.len(), 1..8).prop_map(Op::Insert),
        2 => (0usize..64).prop_map(Op::RemoveLive),
        1 => (1_000_000u64..1_000_010).prop_map(Op::RemoveMissing),
        1 => (0usize..64).prop_map(Op::ReinsertLive),
        1 => prop::collection::vec(0usize..64, 0..8).prop_map(Op::Purge),
        1 => Just(Op::Compact),
    ]
}

/// Reference model: live documents and their word lists.
#[derive(Debug, Default)]
struct Model {
    docs: BTreeMap<u64, Vec<&'static str>>,
    next_id: u64,
}

impl Model {
    fn docs_containing(&self, word: &str) -> BTreeSet<u64> {
        self.docs
            .iter()
            .filter(|(_, words)| words.contains(&word))
            .map(|(id, _)| *id)
            .collect()
    }
}

fn doc_text(word_idxs: &[usize]) -> (Vec<&'static str>, String) {
    let words: Vec<&'static str> = word_idxs.iter().map(|i| WORDS[*i]).collect();
    let text = words.join(" ");
    (words, text)
}

/// Result ids of a search, with score sanity checks applied.
fn search_ids(index: &BM25Index<TokenizerChain>, query: &str, advanced: bool) -> BTreeSet<u64> {
    let results = if advanced {
        index.search_advanced(query, 10_000, None)
    } else {
        index.search(query, 10_000, None)
    };
    let mut prev = f32::INFINITY;
    for (id, score) in &results {
        assert!(
            score.is_finite() && *score > 0.0,
            "query {query:?}: doc {id} has invalid score {score}"
        );
        assert!(
            *score <= prev,
            "query {query:?}: results not sorted by descending score"
        );
        prev = *score;
    }
    results.into_iter().map(|(id, _)| id).collect()
}

fn assert_search_matches_model(index: &BM25Index<TokenizerChain>, model: &Model, context: &str) {
    assert_eq!(index.len(), model.docs.len(), "{context}: live doc count");

    for word in WORDS {
        let expected = model.docs_containing(word);
        let got = search_ids(index, word, false);
        assert_eq!(got, expected, "{context}: term query {word:?} diverged");
        // Independent scoring oracle from the document model, without using
        // index counters or posting lists. In particular DF must remain global.
        let total: usize = model.docs.values().map(Vec::len).sum();
        for (id, score) in index.search(word, 10_000, None) {
            let words = &model.docs[&id];
            let tf = words.iter().filter(|term| *term == word).count() as f64;
            let n = model.docs.len() as f64;
            let df = expected.len() as f64;
            let avg = total as f64 / n;
            let reference = ((n - df + 0.5) / (df + 0.5)).ln_1p() * tf * 2.2
                / (tf + 1.2 * (0.25 + 0.75 * words.len() as f64 / avg));
            assert!(
                (score as f64 - reference).abs() < 1e-5 * (1.0 + reference),
                "{context}: score for {word}/{id}"
            );
        }
    }

    // Boolean queries over a few word pairs: intersection, union, difference.
    for (a, b) in [("red", "blue"), ("fox", "moon"), ("salt", "gold")] {
        let in_a = model.docs_containing(a);
        let in_b = model.docs_containing(b);

        let got = search_ids(index, &format!("{a} AND {b}"), true);
        let expected: BTreeSet<u64> = in_a.intersection(&in_b).copied().collect();
        assert_eq!(got, expected, "{context}: {a} AND {b} diverged");

        let got = search_ids(index, &format!("{a} OR {b}"), true);
        let expected: BTreeSet<u64> = in_a.union(&in_b).copied().collect();
        assert_eq!(got, expected, "{context}: {a} OR {b} diverged");

        let got = search_ids(index, &format!("{a} AND NOT {b}"), true);
        let expected: BTreeSet<u64> = in_a.difference(&in_b).copied().collect();
        assert_eq!(got, expected, "{context}: {a} AND NOT {b} diverged");
    }
}

fn flush_and_reload(index: &BM25Index<TokenizerChain>) -> BM25Index<TokenizerChain> {
    let mut metadata = Vec::new();
    let mut buckets: BTreeMap<BucketObject, Vec<u8>> = BTreeMap::new();
    let outcome = futures::executor::block_on(index.flush(&mut metadata, 1_000, |object, data| {
        buckets.insert(object, data);
        std::future::ready(Ok(()))
    }))
    .expect("flush failed");
    // Mirror the production adapter: retire objects the manifest replaced.
    for object in &outcome.obsolete {
        buckets.remove(object);
    }

    futures::executor::block_on(BM25Index::load_all(
        default_tokenizer(),
        metadata.as_slice(),
        async |object| Ok(buckets.get(&object).cloned()),
    ))
    .expect("load_all failed")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn random_ops_match_reference_model(ops in prop::collection::vec(op_strategy(), 1..80)) {
        let index = BM25Index::new(
            "prop".to_string(),
            default_tokenizer(),
            Some(BM25Config {
                // Force frequent bucket splits so persistence covers many buckets.
                bucket_overload_size: 256,
                ..Default::default()
            }),
        );
        let mut model = Model::default();

        for (step, op) in ops.iter().enumerate() {
            let now_ms = step as u64;
            match op {
                Op::Insert(word_idxs) => {
                    model.next_id += 1;
                    let id = model.next_id;
                    let (words, text) = doc_text(word_idxs);
                    index.insert(id, &text, now_ms).expect("insert failed");
                    model.docs.insert(id, words);
                }
                Op::RemoveLive(selector) => {
                    if model.docs.is_empty() {
                        continue;
                    }
                    let id = *model
                        .docs
                        .keys()
                        .nth(selector % model.docs.len())
                        .expect("selector in range");
                    let words = model.docs.remove(&id).expect("doc is live");
                    let removed = index.remove(id, &words.join(" "), now_ms);
                    prop_assert!(removed, "remove of live doc {} returned false", id);
                }
                Op::RemoveMissing(id) => {
                    let removed = index.remove(*id, "red blue", now_ms);
                    prop_assert!(!removed, "remove of missing doc {} returned true", id);
                }
                Op::ReinsertLive(selector) => {
                    if model.docs.is_empty() {
                        continue;
                    }
                    let id = *model
                        .docs
                        .keys()
                        .nth(selector % model.docs.len())
                        .expect("selector in range");
                    let err = index.insert(id, "red blue", now_ms).unwrap_err();
                    prop_assert!(
                        matches!(err, BM25Error::AlreadyExists { .. }),
                        "expected AlreadyExists, got {:?}", err
                    );
                }
                Op::Purge(selectors) => {
                    let live: Vec<_> = model.docs.keys().copied().collect();
                    let ids: BTreeSet<_> = if live.is_empty() {
                        BTreeSet::new()
                    } else {
                        selectors.iter().map(|n| live[n % live.len()]).collect()
                    };
                    prop_assert_eq!(index.purge_ids(&ids, now_ms), ids.len());
                    for id in ids { model.docs.remove(&id); }
                }
                Op::Compact => { index.compact_buckets(); }
            }
        }

        assert_search_matches_model(&index, &model, "after ops");

        let reloaded = flush_and_reload(&index);
        assert_search_matches_model(&reloaded, &model, "after flush/load round-trip");
    }
}

#[derive(Clone, Debug)]
enum BooleanExpr {
    Term(usize),
    Not(Box<Self>),
    And(Vec<Self>),
    Or(Vec<Self>),
}

impl BooleanExpr {
    fn render(&self) -> String {
        match self {
            Self::Term(i) => WORDS[*i].into(),
            Self::Not(inner) => format!("NOT ({})", inner.render()),
            Self::And(children) | Self::Or(children) => {
                let operator = if matches!(self, Self::And(_)) {
                    " AND "
                } else {
                    " OR "
                };
                children
                    .iter()
                    .map(|q| format!("({})", q.render()))
                    .collect::<Vec<_>>()
                    .join(operator)
            }
        }
    }

    fn matches(&self, id: u64) -> bool {
        match self {
            Self::Term(i) => id & (1 << i) != 0,
            Self::Not(inner) => !inner.matches(id),
            Self::And(children) => children.iter().all(|q| q.matches(id)),
            Self::Or(children) => children.iter().any(|q| q.matches(id)),
        }
    }
}

fn boolean_expr() -> impl Strategy<Value = BooleanExpr> {
    (0usize..5)
        .prop_map(BooleanExpr::Term)
        .prop_recursive(4, 32, 3, |inner| {
            prop_oneof![
                inner.clone().prop_map(|q| BooleanExpr::Not(Box::new(q))),
                prop::collection::vec(inner.clone(), 2..4).prop_map(BooleanExpr::And),
                prop::collection::vec(inner, 2..4).prop_map(BooleanExpr::Or),
            ]
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    #[test]
    fn nested_boolean_queries_match_truth_model(
        expr in boolean_expr(),
        scope in prop::collection::vec(0u64..40, 0..32),
    ) {
        let index = BM25Index::new("boolean-model".into(), default_tokenizer(), None);
        // All combinations of five words, including a document matching none.
        for id in 0..32 {
            let mut text = String::from("anchor");
            for (i, word) in WORDS.iter().take(5).enumerate() {
                if id & (1 << i) != 0 { text.push(' '); text.push_str(word); }
            }
            index.insert(id, &text, 0).unwrap();
        }
        let query = expr.render();
        let full = index.try_search_advanced(&query, 100, None).unwrap();
        let expected: BTreeSet<_> = (0..32).filter(|id| expr.matches(*id)).collect();
        prop_assert_eq!(full.iter().map(|(id, _)| *id).collect::<BTreeSet<_>>(), expected);
        let scoped = index.try_search_in_ids(&query, 100, None, &scope, true).unwrap();
        let expected_scoped: Vec<_> = full.into_iter().filter(|(id, _)| scope.contains(id)).collect();
        prop_assert_eq!(scoped, expected_scoped);
    }
}
