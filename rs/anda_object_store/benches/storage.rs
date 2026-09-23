use anda_object_store::{EncryptedStoreBuilder, FaultOp, FaultStore, MetaStoreBuilder};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::{
    ObjectStore, ObjectStoreExt, local::LocalFileSystem, memory::InMemory, path::Path,
};
use std::{hint::black_box, time::Duration};
mod support;
fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let path = Path::from("benchmark/object");
    support::header();
    for size in [4096, 1024 * 1024, 16 * 1024 * 1024] {
        let payload = Bytes::from(vec![7; size]);
        let plain = MetaStoreBuilder::new(InMemory::new(), 100).build();
        let encrypted = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0; 32]).build();
        support::measure(
            &format!("meta/put/{size}"),
            || {},
            || {
                runtime
                    .block_on(plain.put(&path, payload.clone().into()))
                    .unwrap();
            },
        );
        support::measure(
            &format!("encrypted/put/{size}"),
            || {},
            || {
                runtime
                    .block_on(encrypted.put(&path, payload.clone().into()))
                    .unwrap();
            },
        );
    }
    let backend = InMemory::new();
    let store = EncryptedStoreBuilder::with_secret(backend.clone(), 100, [0; 32])
        .with_chunk_size(1024)
        .build();
    let payload = Bytes::from(vec![7; 4 * 1024 * 1024]);
    runtime
        .block_on(store.put(&path, payload.clone().into()))
        .unwrap();
    support::measure(
        "encrypted/head/hot",
        || {},
        || {
            black_box(runtime.block_on(store.head(&path)).unwrap());
        },
    );
    support::measure(
        "encrypted/range/hot",
        || {},
        || {
            black_box(runtime.block_on(store.get_range(&path, 5..6)).unwrap());
        },
    );
    let cold = EncryptedStoreBuilder::with_secret(backend, 0, [0; 32])
        .with_chunk_size(1024)
        .build();
    support::measure(
        "encrypted/head/cold",
        || {},
        || {
            black_box(runtime.block_on(cold.head(&path)).unwrap());
        },
    );
    let ranges: Vec<_> = (0..64)
        .map(|i| if i % 2 == 0 { 0..1 } else { 8192..8193 })
        .collect();
    support::measure(
        "encrypted/ranges/interleaved",
        || {},
        || {
            black_box(runtime.block_on(store.get_ranges(&path, &ranges)).unwrap());
        },
    );
    support::measure(
        "encrypted/read/full",
        || {},
        || {
            runtime.block_on(async {
                black_box(store.get(&path).await.unwrap().bytes().await.unwrap());
            });
        },
    );
    support::measure(
        "encrypted/stream/first_chunk",
        || {},
        || {
            runtime.block_on(async {
                let mut stream = store.get(&path).await.unwrap().into_stream();
                black_box(stream.next().await.unwrap().unwrap());
            });
        },
    );
    let part = Bytes::from(vec![4; 5 * 1024 * 1024 + 384 * 1024]);
    let multipart = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0; 32]).build();
    support::measure(
        "encrypted/multipart/unaligned",
        || {},
        || {
            runtime.block_on(async {
                let mut upload = multipart.put_multipart(&path).await.unwrap();
                let a = upload.put_part(part.clone().into());
                let b = upload.put_part(part.clone().into());
                let c = upload.put_part(part.clone().into());
                futures::try_join!(a, b, c).unwrap();
                upload.complete().await.unwrap();
            });
        },
    );
    for sync in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let local = MetaStoreBuilder::new(
            LocalFileSystem::new_with_prefix(dir.path())
                .unwrap()
                .with_fsync(sync),
            100,
        )
        .build();
        support::measure(
            &format!("local/put/fsync={sync}"),
            || {},
            || {
                runtime
                    .block_on(local.put(&path, Bytes::from_static(b"durable value").into()))
                    .unwrap();
            },
        );
    }
    let backend = InMemory::new();
    let collector = MetaStoreBuilder::new(backend.clone(), 1000).build();
    runtime.block_on(async {
        for i in 0..100 {
            collector
                .put(
                    &Path::from(format!("gc/{i}")),
                    Bytes::from_static(b"live").into(),
                )
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    });
    support::measure(
        "gc/100_keys_1000_orphans",
        || {
            runtime.block_on(async {
                for i in 0..100 {
                    for n in 0..10 {
                        backend
                            .put(
                                &Path::from(format!("gen/gc/{i}/0000000000000001-{n:08x}")),
                                Bytes::from_static(b"orphan").into(),
                            )
                            .await
                            .unwrap();
                    }
                }
            });
        },
        || {
            assert_eq!(runtime.block_on(collector.collect_garbage()).unwrap(), 1000);
        },
    );
    // Listing order/cold path workload.
    support::measure(
        "list/warm/100_keys",
        || {},
        || {
            runtime.block_on(async {
                black_box(collector.list(None).try_collect::<Vec<_>>().await.unwrap());
            });
        },
    );

    let tiny = EncryptedStoreBuilder::with_secret(InMemory::new(), 100, [0; 32]).build();
    runtime
        .block_on(tiny.put(&path, vec![7; 1024 * 1024].into()))
        .unwrap();
    support::measure(
        "encrypted/range/default_chunk/one_byte",
        || {},
        || {
            black_box(runtime.block_on(tiny.get_range(&path, 100..101)).unwrap());
        },
    );
    for offset in [100, 256 * 1024 - 1] {
        support::retained(
            &format!("encrypted/range/default_chunk/offset={offset}"),
            || {
                runtime
                    .block_on(tiny.get_range(&path, offset..offset + 1))
                    .unwrap()
            },
        );
    }

    let cache = moka::future::Cache::builder().max_capacity(1000).build();
    let listing_backend = InMemory::new();
    let writer = EncryptedStoreBuilder::with_secret(listing_backend.clone(), 1000, [0; 32]).build();
    runtime.block_on(async {
        for i in 0..100 {
            writer
                .put(
                    &Path::from(format!("listing/{i}")),
                    Bytes::from_static(b"data").into(),
                )
                .await
                .unwrap();
        }
    });
    drop(writer);
    let reader = EncryptedStoreBuilder::with_secret(listing_backend, 1000, [0; 32])
        .with_meta_cache(cache.clone())
        .build();
    support::measure(
        "encrypted/list/cold/100_keys",
        || cache.invalidate_all(),
        || {
            black_box(
                runtime
                    .block_on(reader.list(None).try_collect::<Vec<_>>())
                    .unwrap(),
            );
        },
    );
    support::measure(
        "encrypted/list/repeated_after_cold/100_keys",
        || {},
        || {
            black_box(
                runtime
                    .block_on(reader.list(None).try_collect::<Vec<_>>())
                    .unwrap(),
            );
        },
    );

    let copy_backend = InMemory::new();
    let (fault, handle) = FaultStore::wrap(copy_backend.clone());
    let copy_store = MetaStoreBuilder::new(fault, 100).build();
    let source = Path::from("copy/source");
    let target = Path::from("copy/target");
    runtime.block_on(async {
        copy_store
            .put(&source, vec![7; 1024 * 1024].into())
            .await
            .unwrap();
        copy_store
            .put(&target, Bytes::from_static(b"exists").into())
            .await
            .unwrap();
    });
    handle.reset();
    support::measure(
        "meta/copy_if_not_exists/existing_target",
        || {},
        || {
            assert!(
                runtime
                    .block_on(copy_store.copy_if_not_exists(&source, &target))
                    .is_err()
            );
        },
    );
    let copies = handle
        .mutation_log()
        .iter()
        .filter(|(op, _)| *op == FaultOp::Copy)
        .count();
    let generations = runtime
        .block_on(
            copy_backend
                .list(Some(&Path::from("gen/copy/target")))
                .try_collect::<Vec<_>>(),
        )
        .unwrap();
    eprintln!(
        "conditional_copy: attempts=17, backend_copies={copies}, target_generations={}, target_payload_bytes={}",
        generations.len(),
        generations.iter().map(|meta| meta.size).sum::<u64>()
    );
}
