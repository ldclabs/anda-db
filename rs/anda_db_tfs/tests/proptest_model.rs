//! Model-based property tests for [`BM25Index`].
//!
//! Random insert/remove sequences run against both the real index and a
//! naive, trivially-correct inverted index. BM25 assigns scores, but the
//! *retrieval set* is exact: a single-term query must return precisely the
//! live documents containing that term, and boolean queries must return the
//! corresponding set algebra. Scores must be finite, positive and sorted.
//! All checks are repeated after a flush/load round-trip with tiny buckets so
//! bucket splitting is exercised.

use anda_db_tfs::{BM25Config, BM25Error, BM25Index, TokenizerChain, default_tokenizer};
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
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => prop::collection::vec(0usize..WORDS.len(), 1..8).prop_map(Op::Insert),
        2 => (0usize..64).prop_map(Op::RemoveLive),
        1 => (1_000_000u64..1_000_010).prop_map(Op::RemoveMissing),
        1 => (0usize..64).prop_map(Op::ReinsertLive),
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
    let mut buckets: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
    futures::executor::block_on(index.flush(&mut metadata, 1_000, async |bucket_id, data| {
        buckets.insert(bucket_id, data.to_vec());
        Ok(true)
    }))
    .expect("flush failed");

    futures::executor::block_on(BM25Index::load_all(
        default_tokenizer(),
        metadata.as_slice(),
        async |bucket_id| Ok(buckets.get(&bucket_id).cloned()),
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
            }
        }

        assert_search_matches_model(&index, &model, "after ops");

        let reloaded = flush_and_reload(&index);
        assert_search_matches_model(&reloaded, &model, "after flush/load round-trip");
    }
}
