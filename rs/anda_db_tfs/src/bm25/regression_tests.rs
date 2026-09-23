use super::*;
use std::sync::{Arc, Barrier, atomic::AtomicBool};
use tantivy::tokenizer::RawTokenizer;

#[derive(Clone)]
struct PausingTokenizer {
    inner: RawTokenizer,
    armed: Arc<AtomicBool>,
    entered: Arc<Barrier>,
    resume: Arc<Barrier>,
}

impl Tokenizer for PausingTokenizer {
    type TokenStream<'a> = BoxTokenStream<'a>;
    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.entered.wait();
            self.resume.wait();
        }
        BoxTokenStream::new(self.inner.token_stream(text))
    }
}

#[test]
fn remove_tokenizes_before_acquiring_mutation_locks() {
    let tokenizer = PausingTokenizer {
        inner: RawTokenizer::default(),
        armed: Arc::new(AtomicBool::new(false)),
        entered: Arc::new(Barrier::new(2)),
        resume: Arc::new(Barrier::new(2)),
    };
    let index = Arc::new(BM25Index::new(
        "tokenize-first".into(),
        tokenizer.clone(),
        None,
    ));
    index.insert(1, "alpha", 0).unwrap();
    tokenizer.armed.store(true, Ordering::SeqCst);
    let removing = {
        let index = index.clone();
        std::thread::spawn(move || index.remove(1, "alpha", 1))
    };
    tokenizer.entered.wait();
    let unlocked = index.doc_locks[BM25Index::<PausingTokenizer>::doc_stripe(1)]
        .try_lock()
        .is_some();
    let present = index.get_doc_tokens(1);
    tokenizer.resume.wait();
    assert!(removing.join().unwrap());
    assert!(unlocked, "tokenization must not hold a document stripe");
    assert_eq!(
        present,
        Some(1),
        "tokenization must precede membership removal"
    );
}

#[test]
fn same_id_reinsert_waits_for_the_entire_remove() {
    use std::time::{Duration, Instant};
    let index = Arc::new(BM25Index::new(
        "same-id".into(),
        RawTokenizer::default(),
        None,
    ));
    index.insert(1, "alpha", 0).unwrap();
    // Park removal after membership removal but before posting cleanup.
    let posting = index.postings.get_mut("alpha").unwrap();
    let removing = {
        let index = index.clone();
        std::thread::spawn(move || index.remove(1, "alpha", 1))
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while index.doc_tokens.contains_key(&1) {
        assert!(
            Instant::now() < deadline,
            "remove never entered its critical section"
        );
        std::thread::yield_now();
    }
    assert!(
        index.doc_locks[BM25Index::<RawTokenizer>::doc_stripe(1)]
            .try_lock()
            .is_none()
    );
    let inserting = {
        let index = index.clone();
        std::thread::spawn(move || index.insert(1, "alpha", 2))
    };
    drop(posting);
    assert!(removing.join().unwrap());
    inserting.join().unwrap().unwrap();
    assert_eq!(index.len(), 1);
    assert_eq!(index.search("alpha", 10, None)[0].0, 1);
    assert_eq!(index.total_tokens.load(Ordering::Relaxed), 1);
    assert_roundtrip(&index, 1);
}

#[test]
fn empty_candidates_do_not_read_postings() {
    use std::{sync::mpsc, time::Duration};
    let index = Arc::new(BM25Index::new(
        "empty-scope".into(),
        RawTokenizer::default(),
        None,
    ));
    index.insert(1, "common", 0).unwrap();
    for (query, logical) in [
        ("common", false),
        ("common", true),
        ("common AND present", true),
        ("common OR NOT absent", true),
    ] {
        let posting = index.postings.get_mut("common").unwrap();
        let (send, receive) = mpsc::channel();
        let searching = {
            let index = index.clone();
            std::thread::spawn(move || {
                send.send(index.try_search_in_ids(query, 10, None, &[], logical))
                    .unwrap();
            })
        };
        let result = receive.recv_timeout(Duration::from_secs(5));
        drop(posting);
        searching.join().unwrap();
        assert!(
            result
                .expect("empty scope tried to lock postings")
                .unwrap()
                .is_empty()
        );
    }
    assert_eq!(index.stats().search_count, 4);
    assert!(
        index
            .try_search_in_ids(&"x".repeat(9000), 10, None, &[], true)
            .is_err()
    );
    assert_eq!(index.stats().search_count, 4);
}

#[test]
fn unlisting_rechecks_recreated_postings_under_the_bucket_lock() {
    let index = Arc::new(BM25Index::new(
        "unlist".into(),
        RawTokenizer::default(),
        None,
    ));
    index.insert(1, "alpha", 0).unwrap();
    // A purge already removed the last posting and recorded this token for
    // unlisting. A different document completes recreation before unlisting.
    index.postings.remove("alpha");
    index.doc_tokens.remove(&1);
    index.total_tokens.store(0, Ordering::Relaxed);
    index.insert(2, "alpha", 0).unwrap();
    index.unlist_if_unowned(0, "alpha");
    assert!(index.buckets.get(&0).unwrap().tokens.contains("alpha"));
    assert_roundtrip(&index, 2);
}

#[test]
fn purge_and_insert_of_distinct_ids_keep_every_live_term_persistable() {
    let index = Arc::new(BM25Index::new(
        "purge-concurrent".into(),
        default_tokenizer(),
        Some(BM25Config {
            bucket_overload_size: 64,
            ..Default::default()
        }),
    ));
    let ready = Arc::new(Barrier::new(3));
    let writer = {
        let index = index.clone();
        let ready = ready.clone();
        std::thread::spawn(move || {
            ready.wait();
            for id in 0..300 {
                index
                    .insert(id * 2, &format!("shared term{id}"), 0)
                    .unwrap();
            }
        })
    };
    let purger = {
        let index = index.clone();
        let ready = ready.clone();
        std::thread::spawn(move || {
            ready.wait();
            for id in 0..300 {
                index
                    .insert(id * 2 + 1, &format!("shared term{id}"), 0)
                    .unwrap();
                assert_eq!(index.purge_ids(&BTreeSet::from([id * 2 + 1]), 0), 1);
            }
        })
    };
    ready.wait();
    writer.join().unwrap();
    purger.join().unwrap();
    for posting in index.postings.iter() {
        assert!(
            index
                .buckets
                .get(&posting.0)
                .unwrap()
                .tokens
                .contains(posting.key())
        );
    }
    let mut metadata = Vec::new();
    let mut buckets = BTreeMap::new();
    futures::executor::block_on(index.flush(&mut metadata, 1, |key, data| {
        buckets.insert(key, data);
        std::future::ready(Ok(()))
    }))
    .unwrap();
    let loaded = futures::executor::block_on(BM25Index::load_all_strict(
        default_tokenizer(),
        metadata.as_slice(),
        async |key| Ok(buckets.get(&key).cloned()),
    ))
    .unwrap();
    assert_eq!(loaded.len(), 300);
    for id in 0..300 {
        assert_eq!(loaded.search(&format!("term{id}"), 10, None)[0].0, id * 2);
    }
}

fn assert_roundtrip<T: Tokenizer>(index: &BM25Index<T>, expected_id: u64) {
    let mut metadata = Vec::new();
    let mut buckets = BTreeMap::new();
    futures::executor::block_on(index.flush(&mut metadata, 1, |key, data| {
        buckets.insert(key, data);
        std::future::ready(Ok(()))
    }))
    .unwrap();
    let loaded = futures::executor::block_on(BM25Index::load_all_strict(
        RawTokenizer::default(),
        metadata.as_slice(),
        async |key| Ok(buckets.get(&key).cloned()),
    ))
    .unwrap();
    assert_eq!(loaded.search("alpha", 10, None)[0].0, expected_id);
}
