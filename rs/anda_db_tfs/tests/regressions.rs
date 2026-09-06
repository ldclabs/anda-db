use anda_db_tfs::{BM25Config, BM25Index, BoxError, BucketObject, QueryType, Tokenizer};
use futures::executor::block_on;
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, Write},
};
use tantivy::tokenizer::RawTokenizer;

#[path = "../examples/support/atomic_file.rs"]
mod atomic_file;

#[derive(Default)]
struct Store {
    metadata: Vec<u8>,
    buckets: BTreeMap<BucketObject, Vec<u8>>,
}

fn save<T: Tokenizer>(index: &BM25Index<T>, store: &mut Store) {
    let mut metadata = Vec::new();
    let out = block_on(index.flush(&mut metadata, 1, |key, data| {
        store.buckets.insert(key, data);
        std::future::ready(Ok(()))
    }))
    .unwrap();
    if out.saved {
        store.metadata = metadata;
        for key in out.obsolete {
            store.buckets.remove(&key);
        }
    }
}

fn ids(hits: Vec<(u64, f32)>) -> BTreeSet<u64> {
    hits.into_iter().map(|(id, _)| id).collect()
}

#[test]
fn boolean_operands_preserve_tokenizer_semantics() {
    let index = BM25Index::new("raw".into(), RawTokenizer::default(), None);
    for (id, text) in [(1, "Rust"), (2, "alpha"), (3, "beta"), (4, "alpha beta")] {
        index.insert(id, text, 0).unwrap();
    }
    assert_eq!(
        index.search("Rust", 10, None),
        index.search_advanced("Rust", 10, None)
    );
    assert_eq!(
        ids(index.search_advanced("alpha OR beta", 10, None)),
        BTreeSet::from([2, 3])
    );
    assert_eq!(
        ids(index.search_advanced("Rust AND NOT alpha", 10, None)),
        BTreeSet::from([1])
    );
    assert_eq!(
        ids(index.search_advanced("NOT Rust", 10, None)),
        BTreeSet::from([2, 3, 4])
    );
}

#[test]
fn guarded_parser_never_silently_changes_negation() {
    for parentheses in 0..=64 {
        for negations in 0..=65 {
            let input = format!(
                "{}{}fox{}",
                "(".repeat(parentheses),
                "NOT ".repeat(negations),
                ")".repeat(parentheses)
            );
            let parsed = QueryType::try_parse(&input);
            if parentheses + negations > 64 {
                assert!(parsed.is_err(), "{input}");
            } else {
                let mut expected = QueryType::Term("fox".into());
                for _ in 0..negations {
                    expected = QueryType::Not(Box::new(expected));
                }
                assert_eq!(parsed.unwrap(), expected, "{input}");
            }
        }
    }
}

struct FailingWriter;
impl Write for FailingWriter {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("injected storage failure"))
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn buffered_failure_preserves_dirty_state_and_can_retry() {
    let index = BM25Index::new("buffered".into(), RawTokenizer::default(), None);
    index.insert(1, "alpha", 0).unwrap();
    let out = block_on(index.flush(
        io::BufWriter::with_capacity(65536, FailingWriter),
        1,
        |_, _| std::future::ready(Ok(())),
    ));
    assert!(out.is_err());
    assert!(index.has_dirty_buckets());
    let mut store = Store::default();
    save(&index, &mut store);
    let loaded = block_on(BM25Index::load_all_strict(
        RawTokenizer::default(),
        store.metadata.as_slice(),
        async |key| Ok(store.buckets.get(&key).cloned()),
    ))
    .unwrap();
    assert_eq!(loaded.len(), 1);
}

fn two_buckets() -> Store {
    let index = BM25Index::new(
        "partial".into(),
        RawTokenizer::default(),
        Some(BM25Config {
            bucket_overload_size: 1,
            ..Default::default()
        }),
    );
    index.insert(1, "alpha", 0).unwrap();
    index.insert(2, "beta", 0).unwrap();
    let mut store = Store::default();
    save(&index, &mut store);
    assert_eq!(store.buckets.len(), 2);
    store
}

#[test]
fn partial_load_is_read_only_and_can_be_completed() {
    let store = two_buckets();
    let mut partial = block_on(BM25Index::load_all(
        RawTokenizer::default(),
        store.metadata.as_slice(),
        async |key| {
            Ok(if key.bucket_id == 0 {
                store.buckets.get(&key).cloned()
            } else {
                None
            })
        },
    ))
    .unwrap();
    assert!(!partial.is_fully_loaded());
    let before = partial.metadata().buckets;
    assert!(partial.insert(9, "new", 0).is_err());
    assert!(!partial.remove(1, "alpha", 0));
    assert_eq!(partial.purge_ids(&BTreeSet::from([1]), 0), 0);
    assert_eq!(partial.compact_buckets(), (2, 2));
    assert!(
        block_on(partial.flush_with(
            1,
            |_| std::future::ready(Ok(())),
            |_, _| std::future::ready(Ok(()))
        ))
        .is_err()
    );
    assert_eq!(partial.metadata().buckets, before);
    block_on(partial.load_buckets(async |key| {
        Ok(if key.bucket_id == 1 {
            store.buckets.get(&key).cloned()
        } else {
            None
        })
    }))
    .unwrap();
    assert!(partial.is_fully_loaded());
    assert_eq!(partial.len(), 2);
    assert_eq!(partial.search("alpha", 10, None).len(), 1);
    assert_eq!(partial.search("beta", 10, None).len(), 1);
}

#[test]
fn strict_load_rejects_missing_objects() {
    let store = two_buckets();
    let result = block_on(BM25Index::load_all_strict(
        RawTokenizer::default(),
        store.metadata.as_slice(),
        async |key| {
            Ok(if key.bucket_id == 0 {
                store.buckets.get(&key).cloned()
            } else {
                None
            })
        },
    ));
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains("missing referenced bucket")
    );
}

#[test]
fn failed_load_can_resume_without_losing_already_loaded_lengths() {
    let store = two_buckets();
    let mut index =
        BM25Index::load_metadata(RawTokenizer::default(), store.metadata.as_slice()).unwrap();
    let result = block_on(index.load_buckets(async |key| {
        if key.bucket_id == 0 {
            Ok(store.buckets.get(&key).cloned())
        } else {
            Err::<Option<Vec<u8>>, BoxError>("transient read failure".into())
        }
    }));
    assert!(result.is_err());
    assert!(!index.is_fully_loaded());
    block_on(index.load_buckets(async |key| {
        Ok(if key.bucket_id == 1 {
            store.buckets.get(&key).cloned()
        } else {
            None
        })
    }))
    .unwrap();
    assert!(index.is_fully_loaded());
    assert_eq!(index.len(), 2);
    assert_eq!(index.search("alpha", 10, None).len(), 1);
    assert_eq!(index.stats().avg_doc_tokens, 1.0);
}

#[test]
fn candidate_scoring_keeps_global_idf_and_nested_boolean_semantics() {
    let index = BM25Index::new(
        "candidate".into(),
        tantivy::tokenizer::SimpleTokenizer::default(),
        None,
    );
    for id in 0..200 {
        let text = format!(
            "common {} {}",
            if id % 17 == 0 { "rare rare" } else { "normal" },
            if id % 3 == 0 { "blue" } else { "red" }
        );
        index.insert(id, &text, 0).unwrap();
    }
    let common: BTreeMap<_, _> = index.search("common", 300, None).into_iter().collect();
    let rare: BTreeMap<_, _> = index.search("rare", 300, None).into_iter().collect();
    let matched = index.search_advanced("common AND rare", 300, None);
    assert_eq!(matched.len(), rare.len());
    for (id, score) in matched {
        assert!((score - common[&id] - rare[&id]).abs() < 1e-5);
    }
    for (query, expected) in [
        (
            "common AND NOT (blue OR NOT rare)",
            (0..200).filter(|id| id % 3 != 0 && id % 17 == 0).collect(),
        ),
        (
            "(rare OR blue) AND NOT NOT red",
            (0..200)
                .filter(|id| (id % 17 == 0 || id % 3 == 0) && id % 3 != 0)
                .collect(),
        ),
        (
            "NOT (rare AND NOT blue)",
            (0..200).filter(|id| id % 17 != 0 || id % 3 == 0).collect(),
        ),
    ] {
        assert_eq!(
            ids(index.search_advanced(query, 300, None)),
            expected,
            "{query}"
        );
    }
}

#[test]
fn large_candidate_not_queries_keep_the_complement_budget() {
    const DOCUMENTS: u64 = 10_001;
    let index = BM25Index::new(
        "not-budget".into(),
        tantivy::tokenizer::SimpleTokenizer::default(),
        None,
    );
    for id in 0..DOCUMENTS {
        index.insert(id, "common present", 0).unwrap();
    }

    // Simple negative postings are applied directly. Missing terms therefore
    // do not copy or scan the full candidate set once per NOT clause.
    let query = format!(
        "common{}",
        (0..64)
            .map(|id| format!(" AND NOT missing{id}"))
            .collect::<String>()
    );
    assert_eq!(
        index.try_search_advanced(&query, 10, None).unwrap().len(),
        10
    );

    // Leading pairs cancel without constructing a complement, even when they
    // occur inside another boolean branch.
    assert_eq!(
        index
            .try_search_advanced("common AND (present OR NOT NOT present)", 10, None)
            .unwrap()
            .len(),
        10
    );
    assert_eq!(
        index
            .try_search_advanced("NOT NOT common AND NOT missing", 10, None)
            .unwrap()
            .len(),
        10
    );

    // These shapes really do need a candidate-relative complement. A common
    // positive clause can be the whole corpus, so it must not bypass the same
    // 10,000-document guard that protects top-level NOT.
    for query in [
        "common AND NOT (present AND NOT missing)",
        "common AND (present OR NOT missing)",
    ] {
        let err = index.try_search_advanced(query, 10, None).unwrap_err();
        assert!(err.to_string().contains("logical NOT complement"), "{err}");
    }
}

#[test]
fn metadata_shell_retains_persisted_statistics() {
    let store = two_buckets();
    let shell =
        BM25Index::load_metadata(RawTokenizer::default(), store.metadata.as_slice()).unwrap();
    assert_eq!(shell.len(), 0);
    assert_eq!(shell.metadata().stats.num_elements, 2);
    assert_eq!(shell.stats().avg_doc_tokens, 1.0);
    let searches = shell.stats().search_count;
    assert!(shell.search("alpha", 10, None).is_empty());
    assert_eq!(shell.stats().search_count, searches + 1);
    assert_eq!(shell.metadata().stats.search_count, searches + 1);
    assert_eq!(shell.stats().num_elements, 2);
    assert_eq!(shell.stats().avg_doc_tokens, 1.0);
}

#[test]
fn atomic_file_adapter_preserves_metadata_on_noop_and_bucket_failure() {
    let dir = std::env::temp_dir().join(format!("tfs-file-regression-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("metadata.cbor");
    let index = BM25Index::new("file".into(), RawTokenizer::default(), None);
    index.insert(1, "alpha", 0).unwrap();
    let mut store = Store::default();
    save(&index, &mut store);
    atomic_file::write_atomic(&path, &store.metadata).unwrap();
    let metadata = |data: Vec<u8>| {
        std::future::ready(atomic_file::write_atomic(&path, &data).map_err(Into::into))
    };
    assert!(
        !block_on(index.flush_with(2, metadata, |_, _| std::future::ready(Ok(()))))
            .unwrap()
            .saved
    );
    assert_eq!(std::fs::read(&path).unwrap(), store.metadata);
    index.insert(2, "beta", 3).unwrap();
    let failure = block_on(index.flush_with(4, metadata, |_, _| {
        std::future::ready(Err::<(), BoxError>("bucket failure".into()))
    }));
    assert!(failure.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), store.metadata);
    std::fs::remove_dir_all(dir).unwrap();
}
