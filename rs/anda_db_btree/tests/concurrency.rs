use anda_db_btree::{BTreeConfig, BTreeIndex, BucketObject};
use futures::executor::block_on;
use std::{
    collections::BTreeMap,
    sync::{Arc, Barrier},
};

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
