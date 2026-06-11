//! Model-based property tests for [`BTreeIndex`].
//!
//! Random operation sequences are applied both to the real index and to a
//! trivially-correct reference model (`std::collections::BTreeMap`); every
//! observable behaviour — mutation results, point queries, arbitrary nested
//! range queries, key listing, and a full flush/load round-trip — must match
//! the model exactly. Buckets are kept tiny so that bucket splitting is
//! exercised constantly.

use anda_db_btree::{BTreeConfig, BTreeError, BTreeIndex, RangeQuery};
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};

/// A mutation in the workload.
#[derive(Debug, Clone)]
enum Op {
    Insert(u64, u64),
    Remove(u64, u64),
}

/// A query specification with semantics defined independently from the
/// implementation (see [`matches`]).
#[derive(Debug, Clone)]
enum Spec {
    Eq(u64),
    Gt(u64),
    Ge(u64),
    Lt(u64),
    Le(u64),
    Between(u64, u64),
    Include(Vec<u64>),
    And(Vec<Spec>),
    Or(Vec<Spec>),
    Not(Box<Spec>),
}

impl Spec {
    fn to_query(&self) -> RangeQuery<u64> {
        match self {
            Spec::Eq(v) => RangeQuery::Eq(*v),
            Spec::Gt(v) => RangeQuery::Gt(*v),
            Spec::Ge(v) => RangeQuery::Ge(*v),
            Spec::Lt(v) => RangeQuery::Lt(*v),
            Spec::Le(v) => RangeQuery::Le(*v),
            Spec::Between(a, b) => RangeQuery::Between(*a, *b),
            Spec::Include(vs) => RangeQuery::Include(vs.clone()),
            Spec::And(specs) => {
                RangeQuery::And(specs.iter().map(|s| Box::new(s.to_query())).collect())
            }
            Spec::Or(specs) => {
                RangeQuery::Or(specs.iter().map(|s| Box::new(s.to_query())).collect())
            }
            Spec::Not(spec) => RangeQuery::Not(Box::new(spec.to_query())),
        }
    }

    /// Reference semantics, written independently of the index internals.
    fn matches(&self, key: u64) -> bool {
        match self {
            Spec::Eq(v) => key == *v,
            Spec::Gt(v) => key > *v,
            Spec::Ge(v) => key >= *v,
            Spec::Lt(v) => key < *v,
            Spec::Le(v) => key <= *v,
            Spec::Between(a, b) => *a <= *b && key >= *a && key <= *b,
            Spec::Include(vs) => vs.contains(&key),
            Spec::And(specs) => !specs.is_empty() && specs.iter().all(|s| s.matches(key)),
            Spec::Or(specs) => specs.iter().any(|s| s.matches(key)),
            Spec::Not(spec) => !spec.matches(key),
        }
    }
}

fn op_strategy() -> impl Strategy<Value = Op> {
    // Small domains so that inserts, removals and queries collide constantly.
    let pk = 0u64..40;
    let fv = 0u64..16;
    prop_oneof![
        3 => (pk.clone(), fv.clone()).prop_map(|(p, f)| Op::Insert(p, f)),
        1 => (pk, fv).prop_map(|(p, f)| Op::Remove(p, f)),
    ]
}

fn spec_strategy() -> impl Strategy<Value = Spec> {
    let leaf = prop_oneof![
        (0u64..18).prop_map(Spec::Eq),
        (0u64..18).prop_map(Spec::Gt),
        (0u64..18).prop_map(Spec::Ge),
        (0u64..18).prop_map(Spec::Lt),
        (0u64..18).prop_map(Spec::Le),
        (0u64..18, 0u64..18).prop_map(|(a, b)| Spec::Between(a, b)),
        prop::collection::vec(0u64..18, 0..4).prop_map(Spec::Include),
    ];
    leaf.prop_recursive(2, 8, 3, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..3).prop_map(Spec::And),
            prop::collection::vec(inner.clone(), 0..3).prop_map(Spec::Or),
            inner.prop_map(|s| Spec::Not(Box::new(s))),
        ]
    })
}

type Model = BTreeMap<u64, BTreeSet<u64>>;

fn model_insert(model: &mut Model, pk: u64, fv: u64) -> bool {
    model.entry(fv).or_default().insert(pk)
}

fn model_remove(model: &mut Model, pk: u64, fv: u64) -> bool {
    if let Some(set) = model.get_mut(&fv) {
        let removed = set.remove(&pk);
        if set.is_empty() {
            model.remove(&fv);
        }
        removed
    } else {
        false
    }
}

fn tiny_bucket_config() -> BTreeConfig {
    BTreeConfig {
        // Force frequent bucket splits so persistence covers many buckets.
        bucket_overload_size: 128,
        allow_duplicates: true,
    }
}

/// Collects the index's answer to a range query as `field value -> doc ids`.
fn index_range_query(index: &BTreeIndex<u64, u64>, query: RangeQuery<u64>) -> Model {
    let mut result: Model = BTreeMap::new();
    for (fv, pks) in index.range_query_with(query, |fv, pks| (true, vec![(*fv, pks.clone())])) {
        result.entry(fv).or_default().extend(pks);
    }
    result
}

fn model_range_query(model: &Model, spec: &Spec) -> Model {
    model
        .iter()
        .filter(|(fv, _)| spec.matches(**fv))
        .map(|(fv, pks)| (*fv, pks.clone()))
        .collect()
}

/// Full observable state comparison between index and model.
fn assert_index_matches_model(index: &BTreeIndex<u64, u64>, model: &Model, context: &str) {
    assert_eq!(
        index.len(),
        model.len(),
        "{context}: number of distinct field values diverged"
    );
    let keys: BTreeSet<u64> = index.keys(None, None).into_iter().collect();
    let model_keys: BTreeSet<u64> = model.keys().copied().collect();
    assert_eq!(keys, model_keys, "{context}: key sets diverged");

    for (fv, expected) in model {
        let got: Option<BTreeSet<u64>> =
            index.query_with(fv, |pks| Some(pks.iter().copied().collect()));
        assert_eq!(
            got.as_ref(),
            Some(expected),
            "{context}: posting for field value {fv} diverged"
        );
    }
}

/// Persists the index to in-memory buffers and loads it back.
fn flush_and_reload(index: &BTreeIndex<u64, u64>) -> BTreeIndex<u64, u64> {
    let mut metadata = Vec::new();
    let mut buckets: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
    futures::executor::block_on(index.flush(&mut metadata, 1_000, async |bucket_id, data| {
        buckets.insert(bucket_id, data.to_vec());
        Ok(true)
    }))
    .expect("flush failed");

    futures::executor::block_on(BTreeIndex::<u64, u64>::load_all(
        metadata.as_slice(),
        async |bucket_id| Ok(buckets.get(&bucket_id).cloned()),
    ))
    .expect("load_all failed")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Random insert/remove sequences keep the index equivalent to the model,
    /// across mutation results, point queries, key listing and reload.
    #[test]
    fn random_ops_match_reference_model(ops in prop::collection::vec(op_strategy(), 1..200)) {
        let index = BTreeIndex::<u64, u64>::new("prop".to_string(), Some(tiny_bucket_config()));
        let mut model: Model = BTreeMap::new();

        for (step, op) in ops.iter().enumerate() {
            match *op {
                Op::Insert(pk, fv) => {
                    let expected = model_insert(&mut model, pk, fv);
                    let got = index.insert(pk, fv, step as u64).expect("insert failed");
                    prop_assert_eq!(got, expected, "insert({}, {}) at step {}", pk, fv, step);
                }
                Op::Remove(pk, fv) => {
                    let expected = model_remove(&mut model, pk, fv);
                    let got = index.remove(pk, fv, step as u64);
                    prop_assert_eq!(got, expected, "remove({}, {}) at step {}", pk, fv, step);
                }
            }
        }

        assert_index_matches_model(&index, &model, "after ops");

        // Persistence round-trip must preserve the full observable state.
        let reloaded = flush_and_reload(&index);
        assert_index_matches_model(&reloaded, &model, "after flush/load round-trip");
    }

    /// Arbitrary (nested) range queries return exactly what the reference
    /// model predicts, before and after a persistence round-trip.
    #[test]
    fn range_queries_match_reference_model(
        ops in prop::collection::vec(op_strategy(), 1..150),
        specs in prop::collection::vec(spec_strategy(), 1..12),
    ) {
        let index = BTreeIndex::<u64, u64>::new("prop".to_string(), Some(tiny_bucket_config()));
        let mut model: Model = BTreeMap::new();
        for (step, op) in ops.iter().enumerate() {
            match *op {
                Op::Insert(pk, fv) => {
                    model_insert(&mut model, pk, fv);
                    index.insert(pk, fv, step as u64).expect("insert failed");
                }
                Op::Remove(pk, fv) => {
                    model_remove(&mut model, pk, fv);
                    index.remove(pk, fv, step as u64);
                }
            }
        }

        let reloaded = flush_and_reload(&index);
        for spec in &specs {
            let expected = model_range_query(&model, spec);
            let got = index_range_query(&index, spec.to_query());
            prop_assert_eq!(&got, &expected, "query {:?} diverged", spec);
            let got_reloaded = index_range_query(&reloaded, spec.to_query());
            prop_assert_eq!(&got_reloaded, &expected, "query {:?} diverged after reload", spec);
        }
    }

    /// Unique indexes (`allow_duplicates = false`) accept exactly one primary
    /// key per field value, idempotently, and reject the rest.
    #[test]
    fn unique_index_rejects_conflicting_inserts(
        pairs in prop::collection::vec((0u64..40, 0u64..12), 1..80),
    ) {
        let index = BTreeIndex::<u64, u64>::new(
            "prop_unique".to_string(),
            Some(BTreeConfig {
                bucket_overload_size: 128,
                allow_duplicates: false,
            }),
        );
        let mut model: BTreeMap<u64, u64> = BTreeMap::new();

        for (step, (pk, fv)) in pairs.iter().enumerate() {
            match model.get(fv) {
                None => {
                    index.insert(*pk, *fv, step as u64).expect("first insert failed");
                    model.insert(*fv, *pk);
                }
                Some(owner) if owner == pk => {
                    // Idempotent re-insert of the same pair is allowed.
                    index.insert(*pk, *fv, step as u64).expect("idempotent insert failed");
                }
                Some(_) => {
                    let err = index.insert(*pk, *fv, step as u64).unwrap_err();
                    prop_assert!(
                        matches!(err, BTreeError::AlreadyExists { .. }),
                        "expected AlreadyExists, got {:?}", err
                    );
                }
            }
        }

        for (fv, pk) in &model {
            let got: Option<Vec<u64>> = index.query_with(fv, |pks| Some(pks.clone()));
            prop_assert_eq!(got, Some(vec![*pk]), "unique posting for {} diverged", fv);
        }
    }
}
