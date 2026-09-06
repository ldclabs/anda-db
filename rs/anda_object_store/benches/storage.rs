use anda_object_store::{EncryptedStoreBuilder, MetaStoreBuilder};
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
}
