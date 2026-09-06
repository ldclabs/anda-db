use anda_db_btree::{BTreeConfig, BTreeError, BTreeIndex, BucketObject, LoadState, RangeQuery};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt::Debug,
    hash::Hash,
    sync::atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct Store {
    metadata: Vec<u8>,
    buckets: BTreeMap<BucketObject, Vec<u8>>,
}
fn save<PK, FV>(index: &BTreeIndex<PK, FV>, store: &mut Store)
where
    PK: Ord + Eq + Hash + Debug + Clone + Serialize + serde::de::DeserializeOwned,
    FV: Ord + Eq + Hash + Debug + Clone + Serialize + serde::de::DeserializeOwned,
{
    let outcome = block_on(index.flush_owned_with(
        10,
        |data| {
            store.metadata = data;
            std::future::ready(Ok(()))
        },
        |obj, data| {
            store.buckets.insert(obj, data);
            std::future::ready(Ok(()))
        },
    ))
    .unwrap();
    for obj in outcome.obsolete {
        store.buckets.remove(&obj);
    }
}
fn load(store: &Store) -> Result<BTreeIndex<u64, u64>, BTreeError> {
    block_on(BTreeIndex::load_all(
        store.metadata.as_slice(),
        async |obj| Ok(store.buckets.get(&obj).cloned()),
    ))
}
#[test]
fn new_empty_manifest_never_probes_legacy_orphans() {
    let index = BTreeIndex::<u64, u64>::new("same".into(), None);
    let mut store = Store::default();
    save(&index, &mut store);
    let mut probes = 0;
    let loaded = block_on(BTreeIndex::<u64, u64>::load_all(
        store.metadata.as_slice(),
        async |_| {
            probes += 1;
            Ok(Some(vec![0xff]))
        },
    ))
    .unwrap();
    assert!(loaded.is_empty());
    assert_eq!(probes, 0);
}
#[test]
fn missing_manifest_object_is_an_error_and_partial_load_is_read_only() {
    let index = BTreeIndex::new(
        "missing".into(),
        Some(BTreeConfig {
            bucket_overload_size: 64,
            allow_duplicates: true,
        }),
    );
    for k in 0u64..40 {
        index.insert(k, k, 1).unwrap();
    }
    let mut store = Store::default();
    save(&index, &mut store);
    let object = *store.buckets.keys().next_back().unwrap();
    let restored = store.buckets.remove(&object).unwrap();
    assert!(load(&store).is_err());
    let mut partial = BTreeIndex::<u64, u64>::load_metadata(store.metadata.as_slice()).unwrap();
    let missing =
        block_on(partial.load_buckets_partial(async |obj| Ok(store.buckets.get(&obj).cloned())))
            .unwrap();
    assert_eq!(missing, vec![object]);
    assert_eq!(partial.load_state(), LoadState::Partial);
    let before = partial.stats();
    assert!(partial.insert(99, 99, 2).is_err());
    assert!(partial.insert_array(99, vec![99], 2).is_err());
    assert!(partial.batch_update(1, vec![1], vec![99], 2).is_err());
    assert!(!partial.remove(1, 1, 2));
    assert_eq!(partial.remove_array(1, vec![1], 2), 0);
    assert!(!partial.compact_buckets_with_outcome().changed);
    assert!(block_on(partial.flush(Vec::new(), 2, |_, _| std::future::ready(Ok(())))).is_err());
    assert_eq!(partial.stats(), before);
    store.buckets.insert(object, restored);
    block_on(partial.load_buckets(async |obj| Ok(store.buckets.get(&obj).cloned()))).unwrap();
    assert_eq!(partial.load_state(), LoadState::Ready);
    assert_eq!(partial.len(), 40);
    assert!(partial.insert(99, 99, 3).unwrap());
}
#[test]
fn cancelled_load_can_be_retried_without_mutating_partial_state() {
    let index = BTreeIndex::new(
        "cancel_load".into(),
        Some(BTreeConfig {
            bucket_overload_size: 64,
            allow_duplicates: true,
        }),
    );
    for k in 0u64..40 {
        index.insert(k, k, 1).unwrap();
    }
    let mut store = Store::default();
    save(&index, &mut store);
    let last = *store.buckets.keys().next_back().unwrap();
    let mut loaded = BTreeIndex::<u64, u64>::load_metadata(store.metadata.as_slice()).unwrap();
    block_on(async {
        let future = loaded.load_buckets(async |obj| {
            if obj == last {
                std::future::pending::<()>().await;
            }
            Ok(store.buckets.get(&obj).cloned())
        });
        futures::pin_mut!(future);
        assert!(matches!(futures::poll!(future), std::task::Poll::Pending));
    });
    assert_eq!(loaded.load_state(), LoadState::Partial);
    assert!(loaded.insert(99, 99, 2).is_err());
    block_on(loaded.load_buckets(async |obj| Ok(store.buckets.get(&obj).cloned()))).unwrap();
    assert_eq!(loaded.keys(None, None), (0..40).collect::<Vec<_>>());
}
#[test]
fn same_count_rebuild_is_reported_and_canonical_compaction_is_a_no_op() {
    let index = BTreeIndex::new(
        "compact".into(),
        Some(BTreeConfig {
            bucket_overload_size: 64,
            allow_duplicates: true,
        }),
    );
    for k in (0u64..4).rev() {
        index
            .insert(k, format!("{k}-{}", "x".repeat(100)), 1)
            .unwrap();
    }
    let mut store = Store::default();
    save(&index, &mut store);
    let changed = index.compact_buckets_with_outcome();
    assert_eq!((changed.old_bucket_count, changed.new_bucket_count), (4, 4));
    assert!(changed.changed);
    save(&index, &mut store);
    let before = index.metadata();
    assert!(!index.compact_buckets_with_outcome().changed);
    assert!(!index.has_dirty_buckets());
    assert_eq!(index.metadata(), before);
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
struct Converted(u64);
impl TryFrom<u64> for Converted {
    type Error = anda_db_btree::BoxError;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Ok(Self(value))
    }
}
fn deep_query() -> RangeQuery<u64> {
    (0..100_000).fold(RangeQuery::Eq(1), |q, _| RangeQuery::Not(Box::new(q)))
}
#[test]
fn deep_queries_are_rejected_without_recursive_drop() {
    if std::env::var_os("ANDA_BTREE_DEEP_CHILD").is_some() {
        let index = BTreeIndex::<u64, u64>::new("deep".into(), None);
        assert!(
            index
                .try_range_query_with(deep_query(), |_, _| (true, Vec::<u64>::new()))
                .is_err()
        );
        index.insert(1, 1, 1).unwrap();
        assert!(
            index
                .range_query_with(deep_query(), |_, _| (true, Vec::<u64>::new()))
                .is_empty()
        );
        assert!(
            index
                .try_range_query_rev_with(deep_query(), |_, _| (true, Vec::<u64>::new()))
                .is_err()
        );
        assert!(RangeQuery::<Converted>::try_convert_from(deep_query()).is_err());
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "deep_queries_are_rejected_without_recursive_drop",
            "--nocapture",
        ])
        .env("ANDA_BTREE_DEEP_CHILD", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
#[test]
fn query_json_and_cbor_keep_the_derived_wire_format_and_enforce_budgets() {
    use RangeQuery::*;
    let queries = [
        Eq(1u64),
        Gt(1),
        Ge(1),
        Lt(1),
        Le(1),
        Between(1, 3),
        Include(vec![1, 1, 3]),
        And(vec![Box::new(Ge(1)), Box::new(Le(3))]),
        Or(vec![Box::new(Eq(1))]),
        Not(Box::new(Include(vec![]))),
        And(vec![]),
        Or(vec![]),
    ];
    for query in queries {
        let json = serde_json::to_string(&query).unwrap();
        let decoded: RangeQuery<u64> = serde_json::from_str(&json).unwrap();
        assert_eq!(serde_json::to_string(&decoded).unwrap(), json);
        let mut cbor = Vec::new();
        cbor2::to_writer(&query, &mut cbor).unwrap();
        let decoded: RangeQuery<u64> = cbor2::from_reader(cbor.as_slice()).unwrap();
        assert_eq!(serde_json::to_string(&decoded).unwrap(), json);
    }
    assert_eq!(
        serde_json::to_string(&Between(1u64, 3)).unwrap(),
        r#"{"Between":[1,3]}"#
    );
    let deep = format!(
        "{}{{\"Eq\":1}}{}",
        r#"{"Not":"#.repeat(1000),
        "}".repeat(1000)
    );
    assert!(serde_json::from_str::<RangeQuery<u64>>(&deep).is_err());
    let wide = Or((0..RangeQuery::<u64>::MAX_NODES)
        .map(|_| Box::new(Eq(1)))
        .collect());
    assert!(wide.validate().is_err());
    let json = serde_json::to_string(&wide).unwrap();
    assert!(serde_json::from_str::<RangeQuery<u64>>(&json).is_err());
    let include = Include(vec![1u64; RangeQuery::<u64>::MAX_INCLUDE_KEYS + 1]);
    assert!(include.validate().is_err());
    let mut cbor = Vec::new();
    cbor2::to_writer(&include, &mut cbor).unwrap();
    assert!(cbor2::from_reader::<RangeQuery<u64>, _>(&cbor[..]).is_err());
}

static CLONES: AtomicUsize = AtomicUsize::new(0);
static EQUALS: AtomicUsize = AtomicUsize::new(0);
#[derive(Debug, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Count(u64);
impl Hash for Count {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}
impl Clone for Count {
    fn clone(&self) -> Self {
        CLONES.fetch_add(1, Ordering::Relaxed);
        Self(self.0)
    }
}
impl PartialEq for Count {
    fn eq(&self, other: &Self) -> bool {
        EQUALS.fetch_add(1, Ordering::Relaxed);
        self.0 == other.0
    }
}
#[test]
fn paged_queries_and_flush_do_not_clone_postings_and_delete_does_not_scan_them() {
    let index = BTreeIndex::<u64, Count>::new("query_cost".into(), None);
    for k in 0..10_000 {
        index.insert(k, Count(k), 1).unwrap();
    }
    use RangeQuery::*;
    for query in [
        And(vec![Box::new(Ge(Count(0)))]),
        Or(vec![Box::new(Ge(Count(0)))]),
        Not(Box::new(Lt(Count(9_999)))),
        And(vec![
            Box::new(Ge(Count(0))),
            Box::new(Not(Box::new(Include(vec![Count(0)])))),
        ]),
    ] {
        CLONES.store(0, Ordering::Relaxed);
        let result = index.range_query_with(query, |key, _| (false, vec![key.0]));
        assert_eq!(result.len(), 1);
        assert_eq!(CLONES.load(Ordering::Relaxed), 0);
    }
    let index = BTreeIndex::<Count, u64>::new("posting_cost".into(), None);
    for k in 0..10_000 {
        index.insert(Count(k), 0, 1).unwrap();
    }
    EQUALS.store(0, Ordering::Relaxed);
    assert!(!index.remove(Count(20_000), 0, 2));
    assert!(index.remove(Count(9_999), 0, 2));
    assert!(EQUALS.load(Ordering::Relaxed) < 20);
    CLONES.store(0, Ordering::Relaxed);
    let mut store = Store::default();
    save(&index, &mut store);
    assert_eq!(CLONES.load(Ordering::Relaxed), 0);
    let reloaded = block_on(BTreeIndex::<Count, u64>::load_all(
        &store.metadata[..],
        async |obj| Ok(store.buckets.get(&obj).cloned()),
    ))
    .unwrap();
    for k in (0..9_999).rev() {
        assert!(reloaded.remove(Count(k), 0, 3));
    }
    assert!(reloaded.is_empty());
}
