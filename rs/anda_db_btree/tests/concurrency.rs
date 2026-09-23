use anda_db_btree::{BTreeConfig, BTreeIndex, BucketObject};
use futures::executor::block_on;
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    collections::BTreeMap,
    hash::{Hash, Hasher},
    sync::{Arc, Barrier, mpsc},
    time::Duration,
};

// Pause a remover at a public operation boundary without production test hooks.
// A single remove pauses at its second lookup; a batch pauses before key 2.
struct HashPause {
    key: u64,
    remaining: usize,
    entered: mpsc::Sender<()>,
    resume: mpsc::Receiver<()>,
}
thread_local! {
    static HASH_PAUSE: RefCell<Option<HashPause>> = const { RefCell::new(None) };
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct PausingKey(u64);
impl Hash for PausingKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let pause = HASH_PAUSE.with(|slot| {
            let mut slot = slot.borrow_mut();
            if let Some(pause) = slot.as_mut()
                && pause.key == self.0
            {
                pause.remaining -= 1;
                if pause.remaining == 0 {
                    return slot.take();
                }
            }
            None
        });
        if let Some(pause) = pause {
            pause.entered.send(()).unwrap();
            pause.resume.recv_timeout(Duration::from_secs(5)).unwrap();
        }
        self.0.hash(state);
    }
}

#[test]
fn last_id_removal_never_exposes_empty_postings_or_rejects_same_id_reinsertion() {
    for batch_remove in [false, true] {
        for batch_insert in [false, true] {
            let index = Arc::new(BTreeIndex::new(
                "unique_removal".into(),
                Some(BTreeConfig {
                    allow_duplicates: false,
                    ..BTreeConfig::default()
                }),
            ));
            index.insert(1u64, PausingKey(1), 1).unwrap();
            if batch_remove {
                index.insert(1, PausingKey(2), 1).unwrap();
            }
            let (entered_tx, entered_rx) = mpsc::channel();
            let (resume_tx, resume_rx) = mpsc::channel();
            let remover_index = index.clone();
            let remover = std::thread::spawn(move || {
                HASH_PAUSE.with(|slot| {
                    *slot.borrow_mut() = Some(HashPause {
                        key: if batch_remove { 2 } else { 1 },
                        remaining: if batch_remove { 1 } else { 2 },
                        entered: entered_tx,
                        resume: resume_rx,
                    })
                });
                if batch_remove {
                    assert_eq!(
                        remover_index.remove_array(1, vec![PausingKey(1), PausingKey(2)], 2),
                        2
                    );
                } else {
                    assert!(remover_index.remove(1, PausingKey(1), 2));
                }
            });
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let visible_during_cleanup = index.query_with(&PausingKey(1), |ids| Some(ids.clone()));
            let reinsert = || {
                if batch_insert {
                    index.insert_array(1, vec![PausingKey(1)], 3)
                } else {
                    index.insert(1, PausingKey(1), 3).map(usize::from)
                }
            };
            // The single remover may hold the ordered-key lock during cleanup.
            // Batches have not taken it yet, so reinsert before resuming them.
            let inserted = if batch_remove {
                let result = reinsert();
                resume_tx.send(()).unwrap();
                result
            } else {
                resume_tx.send(()).unwrap();
                reinsert()
            };
            remover.join().unwrap();
            assert_eq!(
                visible_during_cleanup, None,
                "empty postings must be removed under the posting lock"
            );
            assert_eq!(inserted.unwrap(), 1);
            assert_eq!(index.keys(None, None), vec![PausingKey(1)]);
            assert!(index.insert(2, PausingKey(1), 4).is_err());

            let mut metadata = Vec::new();
            let mut buckets = BTreeMap::new();
            block_on(index.flush(&mut metadata, 5, |object, data| {
                buckets.insert(object, data);
                std::future::ready(Ok(()))
            }))
            .unwrap();
            let loaded = block_on(BTreeIndex::<u64, PausingKey>::load_all(
                &metadata[..],
                async |object| Ok(buckets.get(&object).cloned()),
            ))
            .unwrap();
            assert_eq!(loaded.keys(None, None), vec![PausingKey(1)]);
            assert_eq!(
                loaded.query_with(&PausingKey(1), |ids| Some(ids.clone())),
                Some(vec![1])
            );
        }
    }
}

#[test]
fn overlapping_batches_and_last_id_removal_survive_repeated_checkpoints() {
    for round in 0..100 {
        let index = Arc::new(BTreeIndex::<u64, u64>::new(
            "shared".into(),
            Some(BTreeConfig {
                bucket_overload_size: 128,
                allow_duplicates: true,
            }),
        ));
        let barrier = Arc::new(Barrier::new(4));
        std::thread::scope(|scope| {
            for worker in 0..4u64 {
                let index = index.clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    for batch in 0..8u64 {
                        let id = worker * 100 + batch;
                        let values: Vec<_> = (0..64).map(|n| (n + worker + batch) % 32).collect(); // duplicate inputs
                        index.insert_array(id, values.clone(), id).unwrap();
                        index.remove_array(
                            id,
                            values
                                .into_iter()
                                .filter(|v| (v + worker + batch) % 3 != 0)
                                .collect(),
                            id,
                        );
                    }
                });
            }
        });
        let expected: Vec<_> = (0..32u64)
            .map(|key| {
                let mut ids = Vec::new();
                for worker in 0..4u64 {
                    for batch in 0..8u64 {
                        if (key + worker + batch) % 3 == 0 {
                            ids.push(worker * 100 + batch);
                        }
                    }
                }
                (key, ids)
            })
            .collect();
        let mut metadata = Vec::new();
        let mut buckets = BTreeMap::<BucketObject, Vec<u8>>::new();
        block_on(index.flush(&mut metadata, 1, |obj, data| {
            buckets.insert(obj, data);
            std::future::ready(Ok(()))
        }))
        .unwrap();
        let loaded = block_on(BTreeIndex::<u64, u64>::load_all(
            &metadata[..],
            async |obj| Ok(buckets.get(&obj).cloned()),
        ))
        .unwrap();
        for (key, ids) in expected {
            let found = loaded.query_with(&key, |ids| {
                let mut sorted = ids.clone();
                sorted.sort_unstable();
                Some(sorted)
            });
            assert_eq!(found, Some(ids), "round {round}, key {key}");
        }
        // Exercise position-map shrink and complete posting removal after load.
        for worker in 0..4u64 {
            for batch in 0..8u64 {
                loaded.remove_array(worker * 100 + batch, (0..32).collect(), 2);
            }
        }
        let mut empty_meta = Vec::new();
        block_on(loaded.flush(&mut empty_meta, 2, |obj, data| {
            buckets.insert(obj, data);
            std::future::ready(Ok(()))
        }))
        .unwrap();
        let empty = block_on(BTreeIndex::<u64, u64>::load_all(
            &empty_meta[..],
            async |obj| Ok(buckets.get(&obj).cloned()),
        ))
        .unwrap();
        assert!(empty.is_empty());
    }
}
