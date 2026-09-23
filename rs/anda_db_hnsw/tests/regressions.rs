//! Regression coverage for the 2026-09 HNSW review. These assert corrected
//! behavior, including the caller-serialized persistence contract.

#[tokio::test]
async fn coherent_sparse_snapshots_preserve_their_original_graph() {
    let index = index();
    for id in 0..20 {
        index.insert_f32(id, vec![0.0, 0.0], 0).unwrap();
    }
    let before = index.search_f32(&[0.0, 0.0], 20).unwrap();
    assert!(
        before.len() < index.len(),
        "fixture must exercise disconnected approximate pruning"
    );
    let disk = Disk::default();
    disk.flush(&index).await;
    let loaded = disk.load().await;
    assert!(!loaded.recovery_report().rebuilt);
    assert!(!loaded.has_dirty_nodes());
    assert_eq!(loaded.search_f32(&[0.0, 0.0], 20).unwrap(), before);
    for id in index.node_ids() {
        assert_eq!(
            index.get_node_with(id, serialize_node).unwrap(),
            loaded.get_node_with(id, serialize_node).unwrap()
        );
    }
}

#[tokio::test]
async fn byte_budget_and_parallel_cancellation_preserve_retry_state() {
    let index = Arc::new(index());
    for id in 0..8 {
        index.insert_f32(id, vec![id as f32, 0.0], 0).unwrap();
    }
    for options in [
        FlushOptions {
            node_concurrency: 0,
            max_in_flight_bytes: 1024,
        },
        FlushOptions {
            node_concurrency: 65,
            max_in_flight_bytes: 1024,
        },
        FlushOptions {
            node_concurrency: 2,
            max_in_flight_bytes: 1,
        },
    ] {
        let started = AtomicUsize::new(0);
        assert!(
            index
                .flush_with_options(
                    1,
                    options,
                    |_, _| {
                        started.fetch_add(1, Ordering::Relaxed);
                        ready(Ok::<bool, BoxError>(true))
                    },
                    |_| ready(Ok::<(), BoxError>(())),
                    |_| ready(Ok::<(), BoxError>(()))
                )
                .await
                .is_err()
        );
        assert!(index.has_dirty_nodes());
        assert_eq!(started.load(Ordering::Relaxed), 0);
    }
    let entered = Arc::new(tokio::sync::Notify::new());
    let task_index = index.clone();
    let signal = entered.clone();
    let task = tokio::spawn(async move {
        task_index
            .flush_with_options(
                1,
                FlushOptions {
                    node_concurrency: 2,
                    ..Default::default()
                },
                move |_, _| {
                    let signal = signal.clone();
                    async move {
                        signal.notify_one();
                        std::future::pending::<Result<bool, BoxError>>().await
                    }
                },
                |_| ready(Ok::<(), BoxError>(())),
                |_| ready(Ok::<(), BoxError>(())),
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(index.has_dirty_nodes() && index.has_pending_metadata_flush());
    let disk = Disk::default();
    disk.flush(&index).await;
    assert_eq!(disk.load().await.len(), 8);
}

#[tokio::test]
async fn snapshot_acknowledgement_survives_same_id_same_counter_aba() {
    let index = index();
    index.insert_f32(1, vec![0.0, 0.0], 0).unwrap();
    let original = index.get_node_with(1, |n| n.version).unwrap();
    index
        .flush(Vec::new(), Vec::new(), 1, async |_, _| {
            assert!(index.remove(1, 2));
            index.insert_f32(1, vec![9.0, 0.0], 3).unwrap();
            Ok(true)
        })
        .await
        .unwrap();
    assert_eq!(index.get_node_with(1, |n| n.version).unwrap(), original);
    assert!(index.has_dirty_nodes());
    let disk = Disk::default();
    disk.flush(&index).await;
    assert_eq!(
        disk.load().await.search_f32(&[9.0, 0.0], 1).unwrap(),
        vec![(1, 0.0)]
    );
}

#[test]
fn euclidean_tiny_distances_do_not_underflow_to_zero() {
    for value in [1e-30f32, 1e-20, f32::MIN_POSITIVE, f32::from_bits(1)] {
        let distance = DistanceMetric::Euclidean
            .compute_f32(&[value], &[0.0])
            .unwrap();
        assert_eq!(distance, value);
    }
}

use anda_db_hnsw::{
    BoxError, DistanceMetric, FlushOptions, FlushOutcome, HnswConfig, HnswIndex, HnswMetadata,
    HnswNode, SearchOptions, SearchWorkspace, SelectNeighborsStrategy, half::bf16, serialize_node,
};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    future::ready,
    io::{self, BufWriter, Read, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

#[tokio::test]
async fn duplicate_vectors_do_not_isolate_a_distinct_insert() {
    for metric in [DistanceMetric::Euclidean, DistanceMetric::Cosine] {
        for count in [77, 90] {
            let index = HnswIndex::try_new_seeded(
                "duplicate_neighbors".into(),
                Some(HnswConfig {
                    dimension: 128,
                    distance_metric: metric,
                    ..Default::default()
                }),
                42,
            )
            .unwrap();
            for id in 0..count {
                index.insert_f32(id, vec![1.0; 128], 0).unwrap();
            }
            let mut query = vec![1.0; 128];
            query[0] = 2.0;
            index.insert_f32(count, query.clone(), 1).unwrap();
            assert_eq!(index.get_node_with(count, |n| n.layer).unwrap(), 0);
            let incoming = index.node_ids().into_iter().any(|id| {
                index
                    .get_node_with(id, |n| n.neighbors[0].iter().any(|e| e.0 == count))
                    .unwrap()
            });
            assert!(incoming, "{metric:?}: distinct node has no incoming edge");
            let disk = Disk::default();
            disk.flush(&index).await;
            let loaded = disk.load().await;
            for index in [&index, &loaded] {
                for ef in [50, 4096] {
                    let hits = index
                        .search_f32_with_options(
                            &query,
                            10,
                            SearchOptions {
                                ef_search: Some(ef),
                            },
                        )
                        .unwrap();
                    assert_eq!(hits[0].0, count, "{metric:?}, ef={ef}");
                    assert!(hits[0].1.abs() < 1e-6);
                }
                assert_graph(index);
            }
        }
    }
}

struct NoEofReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl Read for NoEofReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.offset == self.bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "the framed CBOR item ended without closing the stream",
            ));
        }
        let count = buffer.len().min(self.bytes.len() - self.offset);
        buffer[..count].copy_from_slice(&self.bytes[self.offset..self.offset + count]);
        self.offset += count;
        Ok(count)
    }
}

fn config() -> HnswConfig {
    HnswConfig {
        dimension: 2,
        max_layers: 1,
        max_connections: 2,
        ef_construction: 16,
        ef_search: 16,
        ..Default::default()
    }
}
fn index() -> HnswIndex {
    HnswIndex::try_new_seeded("regression".into(), Some(config()), 7).unwrap()
}

#[derive(Default)]
struct Disk {
    metadata: Mutex<Vec<u8>>,
    ids: Mutex<Vec<u8>>,
    nodes: Mutex<BTreeMap<u64, Vec<u8>>>,
}
impl Disk {
    async fn flush(&self, index: &HnswIndex) {
        index
            .flush_with(
                10,
                |id, bytes| {
                    self.nodes.lock().unwrap().insert(id, bytes);
                    ready(Ok::<_, BoxError>(true))
                },
                |bytes| {
                    *self.ids.lock().unwrap() = bytes;
                    ready(Ok::<_, BoxError>(()))
                },
                |bytes| {
                    *self.metadata.lock().unwrap() = bytes;
                    ready(Ok::<_, BoxError>(()))
                },
            )
            .await
            .unwrap();
    }
    async fn load(&self) -> HnswIndex {
        let metadata = self.metadata.lock().unwrap().clone();
        let ids = self.ids.lock().unwrap().clone();
        HnswIndex::load_all(metadata.as_slice(), ids.as_slice(), async |id| {
            Ok(self.nodes.lock().unwrap().get(&id).cloned())
        })
        .await
        .unwrap()
    }
}

fn assert_graph(index: &HnswIndex) {
    let nodes: BTreeMap<_, _> = index
        .node_ids()
        .into_iter()
        .map(|id| (id, index.get_node_with(id, Clone::clone).unwrap()))
        .collect();
    assert_eq!(index.len(), nodes.len());
    let config = index.metadata().config;
    for (&id, node) in &nodes {
        assert_eq!(node.neighbors.len(), node.layer as usize + 1);
        for (layer, neighbors) in node.neighbors.iter().enumerate() {
            let mut seen = BTreeSet::new();
            let capacity = config.max_connections as usize * if layer == 0 { 2 } else { 1 };
            assert!(neighbors.len() <= capacity + capacity / 5);
            for &(target, distance) in neighbors {
                assert_ne!(id, target);
                assert!(seen.insert(target), "duplicate edge {id}->{target}");
                let other = &nodes[&target];
                assert!(other.layer as usize >= layer);
                let expected = config
                    .distance_metric
                    .compute(&node.vector, &other.vector)
                    .unwrap();
                assert!(distance.is_finite());
                assert!(
                    (distance.to_f32() - expected).abs() <= expected.abs() * 0.01 + 1e-5,
                    "stale distance {id}->{target}: {} vs {expected}",
                    distance.to_f32()
                );
            }
        }
    }
}

#[tokio::test]
async fn same_id_updates_keep_edges_unique_and_current() {
    for strategy in [
        SelectNeighborsStrategy::Simple,
        SelectNeighborsStrategy::Heuristic,
    ] {
        for reconnect in [false, true] {
            let index = HnswIndex::try_new_seeded(
                "updates".into(),
                Some(HnswConfig {
                    select_neighbors_strategy: strategy,
                    reconnect_on_delete: reconnect,
                    ..config()
                }),
                17,
            )
            .unwrap();
            for id in 1..=40 {
                index.insert_f32(id, vec![id as f32, 0.0], 0).unwrap();
            }
            let disk = Disk::default();
            disk.flush(&index).await;
            let index = disk.load().await;
            for x in [1000.0, 1.5, 4.0, -1000.0, 1.5] {
                assert!(index.remove(4, 1));
                assert_graph(&index);
                index.insert_f32(4, vec![x, 0.0], 2).unwrap();
                assert_graph(&index);
            }
            disk.flush(&index).await;
            assert_graph(&disk.load().await);
        }
    }
}

#[tokio::test]
async fn accepted_large_vectors_round_trip_and_out_of_range_inserts_are_atomic() {
    for metric in [
        DistanceMetric::Euclidean,
        DistanceMetric::Cosine,
        DistanceMetric::InnerProduct,
        DistanceMetric::Manhattan,
    ] {
        let index = HnswIndex::try_new_seeded(
            "numbers".into(),
            Some(HnswConfig {
                distance_metric: metric,
                ..config()
            }),
            7,
        )
        .unwrap();
        let value = if metric == DistanceMetric::InnerProduct {
            1e18
        } else {
            1e20
        };
        index.insert_f32(1, vec![value, 0.0], 0).unwrap();
        index.insert_f32(2, vec![-value, 0.0], 0).unwrap();
        assert_graph(&index);
        let results = index.search_f32(&[value, 0.0], 2).unwrap();
        assert!(results.iter().all(|(_, d)| d.is_finite()));
        let disk = Disk::default();
        disk.flush(&index).await;
        assert_graph(&disk.load().await);
        let before = index.metadata();
        if metric != DistanceMetric::Cosine {
            assert!(index.insert_f32(3, vec![2e38, 2e38], 0).is_err());
            assert_eq!(before, index.metadata());
            assert!(!index.has_dirty_nodes());
        }
        assert!(index.insert_f32(3, vec![f32::NAN, 0.0], 0).is_err());
        assert_eq!(before, index.metadata());
    }
    assert!(
        DistanceMetric::InnerProduct
            .compute_f32(&[1e20], &[1e20])
            .is_err()
    );
    assert!(
        DistanceMetric::Manhattan
            .compute_f32(&[2e38], &[-2e38])
            .is_err()
    );
    let euclidean = DistanceMetric::Euclidean
        .compute_f32(&[1e20], &[-1e20])
        .unwrap();
    assert!((euclidean as f64 / 2e20 - 1.0).abs() < 1e-6);
    assert!(
        DistanceMetric::Cosine
            .compute_f32(&[1e20, 1e20], &[1.0, 1.0])
            .unwrap()
            .abs()
            < 1e-6
    );
    assert!(
        DistanceMetric::Cosine
            .compute_f32(&[f32::INFINITY, 0.0], &[0.0, 0.0])
            .is_err()
    );
}

#[tokio::test]
async fn legacy_finite_vectors_outside_new_insert_bounds_still_load() {
    let seed = index();
    seed.insert_f32(1, vec![1.0, 0.0], 0).unwrap();
    let metadata = seed.metadata_bytes().unwrap();
    let mut ids = Vec::new();
    seed.store_ids(&mut ids).unwrap();
    let mut legacy = node(1, 0, vec![vec![]]);
    legacy.vector = vec![bf16::from_f32(1e38), bf16::ZERO];
    let blob = serialize_node(&legacy);

    let loaded = HnswIndex::load_all(metadata.as_slice(), ids.as_slice(), async |id| {
        assert_eq!(id, 1);
        Ok(Some(blob.clone()))
    })
    .await
    .unwrap();

    assert_eq!(loaded.node_ids(), vec![1]);
    let result = loaded.search_f32(&[1e38, 0.0], 1).unwrap();
    assert_eq!(result[0].0, 1);
    assert!(result[0].1.is_finite());
    assert!(!loaded.recovery_report().rebuilt);

    // A later graph mutation can rewrite the legacy node with a modern pass
    // marker. That must not revoke its read compatibility on the next open.
    loaded.insert_f32(2, vec![0.0, 0.0], 1).unwrap();
    let disk = Disk::default();
    disk.flush(&loaded).await;
    assert_eq!(disk.load().await.node_ids(), vec![1, 2]);
}

#[tokio::test]
async fn valid_disconnected_legacy_graph_does_not_trigger_full_rebuild() {
    let seed = index();
    seed.insert_f32(1, vec![1.0, 0.0], 0).unwrap();
    seed.insert_f32(2, vec![2.0, 0.0], 0).unwrap();
    let metadata = seed.metadata_bytes().unwrap();
    let mut ids = Vec::new();
    seed.store_ids(&mut ids).unwrap();
    let blobs = BTreeMap::from([
        (1, serialize_node(&node(1, 0, vec![vec![]]))),
        (2, serialize_node(&node(2, 0, vec![vec![]]))),
    ]);

    let loaded = HnswIndex::load_all(metadata.as_slice(), ids.as_slice(), async |id| {
        Ok(blobs.get(&id).cloned())
    })
    .await
    .unwrap();

    assert_eq!(loaded.node_ids(), vec![1, 2]);
    assert!(!loaded.recovery_report().rebuilt);
    assert!(!loaded.has_dirty_nodes());
}

#[test]
fn ids_are_one_framed_cbor_item_and_loading_does_not_wait_for_eof() {
    let source = index();
    source.insert_f32(1, vec![1.0, 0.0], 0).unwrap();
    let mut ids = Vec::new();
    source.store_ids(&mut ids).unwrap();
    cbor2::validate_slice(&ids).unwrap();

    let mut target = index();
    target
        .load_ids(NoEofReader {
            bytes: &ids,
            offset: 0,
        })
        .unwrap();
}

#[tokio::test]
async fn flush_waits_for_started_node_callbacks_after_an_error() {
    let index = index();
    for id in 0..4 {
        index.insert_f32(id, vec![id as f32, 0.0], 0).unwrap();
    }
    let started = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let ids_called = AtomicBool::new(false);
    let metadata_called = AtomicBool::new(false);
    let result = index
        .flush_with_options(
            1,
            FlushOptions {
                node_concurrency: 4,
                ..Default::default()
            },
            |id, _| {
                let started = started.clone();
                let completed = completed.clone();
                async move {
                    started.fetch_add(1, Ordering::SeqCst);
                    if id == 0 {
                        return Err::<bool, BoxError>("injected node error".into());
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    completed.fetch_add(1, Ordering::SeqCst);
                    Ok(true)
                }
            },
            |_| {
                ids_called.store(true, Ordering::SeqCst);
                ready(Ok::<(), BoxError>(()))
            },
            |_| {
                metadata_called.store(true, Ordering::SeqCst);
                ready(Ok::<(), BoxError>(()))
            },
        )
        .await;
    assert!(result.is_err());
    assert_eq!(started.load(Ordering::SeqCst), 4);
    assert_eq!(completed.load(Ordering::SeqCst), 3);
    assert!(!ids_called.load(Ordering::SeqCst));
    assert!(!metadata_called.load(Ordering::SeqCst));
    assert!(index.has_pending_flush());
}

#[tokio::test]
async fn byte_budget_covers_ids_and_metadata_payloads() {
    let many = index();
    for id in 0..64 {
        many.insert_f32(id * (u32::MAX as u64 + 1), vec![id as f32, 0.0], 0)
            .unwrap();
    }
    let mut ids = Vec::new();
    many.store_ids(&mut ids).unwrap();
    let nodes_called = AtomicBool::new(false);
    let ids_called = AtomicBool::new(false);
    let metadata_called = AtomicBool::new(false);
    let result = many
        .flush_with_options(
            1,
            FlushOptions {
                node_concurrency: 1,
                max_in_flight_bytes: ids.len() - 1,
            },
            |_, _| {
                nodes_called.store(true, Ordering::SeqCst);
                ready(Ok::<bool, BoxError>(true))
            },
            |_| {
                ids_called.store(true, Ordering::SeqCst);
                ready(Ok::<(), BoxError>(()))
            },
            |_| {
                metadata_called.store(true, Ordering::SeqCst);
                ready(Ok::<(), BoxError>(()))
            },
        )
        .await;
    assert!(result.is_err());
    assert!(!nodes_called.load(Ordering::SeqCst));
    assert!(!ids_called.load(Ordering::SeqCst));
    assert!(!metadata_called.load(Ordering::SeqCst));

    let empty = index();
    let mut empty_ids = Vec::new();
    empty.store_ids(&mut empty_ids).unwrap();
    assert!(empty.metadata_bytes().unwrap().len() > empty_ids.len());
    let ids_called = AtomicBool::new(false);
    let metadata_called = AtomicBool::new(false);
    let nodes_called = AtomicBool::new(false);
    let result = empty
        .flush_with_options(
            1,
            FlushOptions {
                node_concurrency: 1,
                max_in_flight_bytes: empty_ids.len(),
            },
            |_, _| {
                nodes_called.store(true, Ordering::SeqCst);
                ready(Ok::<bool, BoxError>(true))
            },
            |_| {
                ids_called.store(true, Ordering::SeqCst);
                ready(Ok::<(), BoxError>(()))
            },
            |_| {
                metadata_called.store(true, Ordering::SeqCst);
                ready(Ok::<(), BoxError>(()))
            },
        )
        .await;
    assert!(result.is_err());
    assert!(!nodes_called.load(Ordering::SeqCst));
    assert!(!ids_called.load(Ordering::SeqCst));
    assert!(!metadata_called.load(Ordering::SeqCst));
}

struct FailingWriter {
    fail_write: bool,
    fail_flush: bool,
}
impl Write for FailingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.fail_write {
            Err(io::Error::other("write failure"))
        } else {
            Ok(bytes.len())
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        if self.fail_flush {
            Err(io::Error::other("flush failure"))
        } else {
            Ok(())
        }
    }
}
fn writer(fail_write: bool, fail_flush: bool) -> BufWriter<FailingWriter> {
    BufWriter::with_capacity(
        8192,
        FailingWriter {
            fail_write,
            fail_flush,
        },
    )
}
#[tokio::test]
async fn buffered_writer_failures_never_acknowledge_a_snapshot() {
    for fail_metadata in [false, true] {
        for fail_write in [false, true] {
            let index = index();
            index.insert_f32(1, vec![0.0, 0.0], 0).unwrap();
            let bad = writer(fail_write, !fail_write);
            let good = writer(false, false);
            let result = if fail_metadata {
                index.flush(bad, good, 1, async |_, _| Ok(true)).await
            } else {
                index.flush(good, bad, 1, async |_, _| Ok(true)).await
            };
            assert!(result.is_err());
            assert!(index.has_dirty_nodes() && index.has_pending_metadata_flush());
            let disk = Disk::default();
            disk.flush(&index).await;
            assert_eq!(disk.load().await.len(), 1);
        }
    }
    let index = index();
    assert!(index.store_metadata(writer(false, true), 1).is_err());
    assert!(index.has_pending_metadata_flush());
}

#[tokio::test]
async fn stopped_and_metadata_only_flushes_cannot_authorize_purge() {
    let index = index();
    index.insert_f32(1, vec![0.0, 0.0], 0).unwrap();
    index.insert_f32(2, vec![1.0, 0.0], 0).unwrap();
    let disk = Disk::default();
    disk.flush(&index).await;
    index.remove(2, 1);
    assert_eq!(
        index
            .flush_outcome(Vec::new(), Vec::new(), 2, async |_, _| Ok(false))
            .await
            .unwrap(),
        FlushOutcome::Stopped
    );
    assert!(
        index
            .flush(Vec::new(), Vec::new(), 2, async |_, _| Ok(false))
            .await
            .is_err()
    );
    index
        .store_metadata_with(2, async |_| Ok(()))
        .await
        .unwrap();
    index
        .purge_removed_nodes(async |_| panic!("uncommitted removal must not be purged"))
        .await
        .unwrap();
    assert!(disk.nodes.lock().unwrap().contains_key(&2));
    assert!(index.has_pending_flush());
    disk.flush(&index).await;
    index
        .purge_removed_nodes(async |id| {
            disk.nodes.lock().unwrap().remove(&id);
            Ok(true)
        })
        .await
        .unwrap();
    assert!(!disk.nodes.lock().unwrap().contains_key(&2));
}

#[tokio::test]
async fn removal_during_flush_waits_for_its_own_commit() {
    let index = index();
    for id in 1..=3 {
        index.insert_f32(id, vec![id as f32, 0.0], 0).unwrap();
    }
    let disk = Disk::default();
    let mut removed = false;
    index
        .flush_with(
            1,
            |id, data| {
                disk.nodes.lock().unwrap().insert(id, data);
                if !removed {
                    index.remove(2, 2);
                    removed = true;
                }
                ready(Ok::<_, BoxError>(true))
            },
            |data| {
                *disk.ids.lock().unwrap() = data;
                ready(Ok::<_, BoxError>(()))
            },
            |data| {
                *disk.metadata.lock().unwrap() = data;
                ready(Ok::<_, BoxError>(()))
            },
        )
        .await
        .unwrap();
    index
        .purge_removed_nodes(async |_| panic!("later removal is not committed"))
        .await
        .unwrap();
    assert_eq!(disk.load().await.node_ids(), vec![1, 2, 3]);
    disk.flush(&index).await;
    index
        .purge_removed_nodes(async |id| {
            disk.nodes.lock().unwrap().remove(&id);
            Ok(true)
        })
        .await
        .unwrap();
    assert_eq!(disk.load().await.node_ids(), vec![1, 3]);
}

#[tokio::test]
async fn purge_acknowledgement_does_not_consume_a_new_removal() {
    let index = index();
    index.insert_f32(1, vec![0.0, 0.0], 0).unwrap();
    let disk = Disk::default();
    disk.flush(&index).await;
    index.remove(1, 1);
    disk.flush(&index).await;
    index
        .purge_removed_nodes(async |id| {
            index.insert_f32(id, vec![2.0, 0.0], 2).unwrap();
            index.remove(id, 3);
            disk.nodes.lock().unwrap().remove(&id);
            Ok(true)
        })
        .await
        .unwrap();
    assert!(index.has_removed_nodes());
    index
        .purge_removed_nodes(async |_| panic!("new removal needs a flush"))
        .await
        .unwrap();
    disk.flush(&index).await;
    index.purge_removed_nodes(async |_| Ok(true)).await.unwrap();
    assert!(!index.has_removed_nodes());
}

#[tokio::test]
async fn every_persistence_boundary_reopens_a_searchable_image() {
    for boundary in 0..=4 {
        for fail_after in [false, true] {
            let index = index();
            for id in 1..=8 {
                index.insert_f32(id, vec![id as f32, 0.0], 0).unwrap();
            }
            let disk = Disk::default();
            disk.flush(&index).await;
            index.remove(2, 2);
            index.insert_f32(9, vec![9.0, 0.0], 2).unwrap();
            let calls = AtomicUsize::new(0);
            let mut ids_written = false;
            let result = index
                .flush_with(
                    3,
                    |id, data| {
                        let position = calls.fetch_add(1, Ordering::Relaxed);
                        let fail =
                            (boundary == 0 && position == 0) || (boundary == 1 && position == 1);
                        if !fail || fail_after {
                            disk.nodes.lock().unwrap().insert(id, data);
                        }
                        ready(if fail {
                            Err::<bool, BoxError>("node fault".into())
                        } else {
                            Ok(true)
                        })
                    },
                    |data| {
                        if boundary != 2 || fail_after {
                            *disk.ids.lock().unwrap() = data;
                            ids_written = true;
                        }
                        ready(if boundary == 2 {
                            Err::<(), BoxError>("IDs fault".into())
                        } else {
                            Ok(())
                        })
                    },
                    |data| {
                        if boundary != 3 || fail_after {
                            *disk.metadata.lock().unwrap() = data;
                        }
                        ready(if boundary == 3 {
                            Err::<(), BoxError>("metadata fault".into())
                        } else {
                            Ok(())
                        })
                    },
                )
                .await;
            if boundary < 4 {
                assert!(result.is_err());
            }
            drop(index);
            let recovered = disk.load().await;
            let expected: Vec<_> = if ids_written {
                vec![1, 3, 4, 5, 6, 7, 8, 9]
            } else {
                (1..=8).collect()
            };
            assert_eq!(recovered.node_ids(), expected);
            assert_graph(&recovered);
            assert_eq!(
                recovered.search_f32(&[4.0, 0.0], 16).unwrap().len(),
                expected.len(),
                "boundary={boundary}, after={fail_after}"
            );
            disk.flush(&recovered).await;
            let stable = disk.load().await;
            assert!(!stable.recovery_report().rebuilt);
            assert!(!stable.has_dirty_nodes() && !stable.has_pending_metadata_flush());
        }
    }
}

#[tokio::test]
async fn failed_bootstrap_is_transactional_and_retry_drops_missing_nodes() {
    let source = index();
    source.insert_f32(1, vec![0.0, 0.0], 0).unwrap();
    source.insert_f32(2, vec![1.0, 0.0], 0).unwrap();
    let disk = Disk::default();
    disk.flush(&source).await;
    let metadata = disk.metadata.lock().unwrap().clone();
    let ids = disk.ids.lock().unwrap().clone();
    let mut staged = HnswIndex::load_metadata(metadata.as_slice()).unwrap();
    staged.load_ids(ids.as_slice()).unwrap();
    assert!(
        staged
            .load_nodes(async |id| if id == 1 {
                Ok(disk.nodes.lock().unwrap().get(&id).cloned())
            } else {
                Err("I/O".into())
            })
            .await
            .is_err()
    );
    assert_eq!(staged.len(), 0);
    assert!(staged.node_ids().is_empty());
    staged
        .load_nodes(async |id| {
            if id == 1 {
                Ok(None)
            } else {
                Ok(disk.nodes.lock().unwrap().get(&id).cloned())
            }
        })
        .await
        .unwrap();
    assert_eq!(staged.len(), 1);
    assert_eq!(staged.node_ids(), vec![2]);
    assert_eq!(staged.search_f32(&[0.0, 0.0], 10).unwrap(), vec![(2, 1.0)]);
}

#[derive(Serialize)]
struct MetadataImage {
    entry_point: (u64, u8),
    metadata: HnswMetadata,
    removed_nodes: Vec<u64>,
}
fn node(id: u64, layer: u8, edges: Vec<Vec<(u64, f32)>>) -> HnswNode {
    HnswNode {
        id,
        layer,
        vector: vec![bf16::from_f32(id as f32), bf16::ZERO],
        neighbors: edges
            .into_iter()
            .map(|e| {
                e.into_iter()
                    .map(|(id, d)| (id, bf16::from_f32(d)))
                    .collect()
            })
            .collect(),
        version: 1,
    }
}
#[tokio::test]
async fn legacy_topology_and_entry_layers_are_repaired() {
    let cfg = HnswConfig {
        max_connections: 4,
        max_layers: 2,
        ..config()
    };
    let seed = HnswIndex::try_new_seeded("legacy".into(), Some(cfg), 5).unwrap();
    seed.insert_f32(1, vec![1.0, 0.0], 0).unwrap();
    seed.insert_f32(2, vec![2.0, 0.0], 0).unwrap();
    let mut metadata = Vec::new();
    cbor2::to_writer(
        &MetadataImage {
            entry_point: (1, 1),
            metadata: seed.metadata(),
            removed_nodes: vec![],
        },
        &mut metadata,
    )
    .unwrap();
    let mut ids = Vec::new();
    seed.store_ids(&mut ids).unwrap();
    cbor2::validate_slice(&ids).unwrap();
    let blobs = BTreeMap::from([
        (
            1,
            serialize_node(&node(
                1,
                0,
                vec![vec![(1, 0.0), (2, 1.0), (2, 1.0), (99, 1.0)]],
            )),
        ),
        (
            2,
            serialize_node(&node(2, 1, vec![vec![(1, 1.0)], vec![(1, 1.0)]])),
        ),
    ]);
    let index = HnswIndex::load_all(metadata.as_slice(), ids.as_slice(), async |id| {
        Ok(blobs.get(&id).cloned())
    })
    .await
    .unwrap();
    assert_graph(&index);
    assert!(index.has_pending_metadata_flush());
    assert_eq!(index.stats().max_layer, 1);
    index.insert_f32(3, vec![3.0, 0.0], 1).unwrap();
    assert_graph(&index);
}

#[tokio::test]
async fn concurrent_uploads_obey_order_and_memory_budget() {
    let index = index();
    for id in 1..=20 {
        index.insert_f32(id, vec![id as f32, 0.0], 0).unwrap();
    }
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let byte_count = Arc::new(AtomicUsize::new(0));
    let peak_bytes = Arc::new(AtomicUsize::new(0));
    let written = Arc::new(AtomicUsize::new(0));
    let ids_written = AtomicBool::new(false);
    let budget = 1024;
    let result = index
        .flush_with_options(
            1,
            FlushOptions {
                node_concurrency: 4,
                max_in_flight_bytes: budget,
            },
            |_, bytes| {
                let active = active.clone();
                let peak = peak.clone();
                let count = byte_count.clone();
                let peak_bytes = peak_bytes.clone();
                let written = written.clone();
                async move {
                    let live = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(live, Ordering::SeqCst);
                    let size = bytes.len();
                    let total = count.fetch_add(size, Ordering::SeqCst) + size;
                    peak_bytes.fetch_max(total, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    written.fetch_add(1, Ordering::SeqCst);
                    count.fetch_sub(size, Ordering::SeqCst);
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(true)
                }
            },
            |_| {
                assert_eq!(active.load(Ordering::SeqCst), 0);
                assert_eq!(written.load(Ordering::SeqCst), 20);
                ids_written.store(true, Ordering::SeqCst);
                ready(Ok::<_, BoxError>(()))
            },
            |_| {
                assert!(ids_written.load(Ordering::SeqCst));
                ready(Ok::<_, BoxError>(()))
            },
        )
        .await
        .unwrap();
    assert_eq!(result, FlushOutcome::Committed);
    assert!((2..=4).contains(&peak.load(Ordering::SeqCst)));
    assert!(peak_bytes.load(Ordering::SeqCst) <= budget);
}

#[tokio::test]
async fn unrelated_mutation_does_not_keep_the_whole_snapshot_dirty() {
    let index = index();
    for id in 1..=100 {
        index.insert_f32(id, vec![id as f32, 0.0], 0).unwrap();
    }
    let before: BTreeMap<_, _> = index
        .node_ids()
        .into_iter()
        .map(|id| (id, index.get_node_with(id, |n| n.version).unwrap()))
        .collect();
    let mut first = true;
    index
        .flush(Vec::new(), Vec::new(), 1, async |_, _| {
            if first {
                index.insert_f32(1000, vec![1000.0, 0.0], 2).unwrap();
                first = false;
            }
            Ok(true)
        })
        .await
        .unwrap();
    let expected: BTreeSet<_> = index
        .node_ids()
        .into_iter()
        .filter(|id| before.get(id) != Some(&index.get_node_with(*id, |n| n.version).unwrap()))
        .collect();
    assert!(expected.len() < 20);
    let mut written = BTreeSet::new();
    index
        .flush(Vec::new(), Vec::new(), 3, async |id, _| {
            written.insert(id);
            Ok(true)
        })
        .await
        .unwrap();
    assert_eq!(written, expected);
    assert!(!index.has_pending_flush());
}

#[test]
fn query_limits_and_zero_k_are_consistent() {
    let index = index();
    index.insert_f32(1, vec![0.0, 0.0], 0).unwrap();
    assert!(index.search_f32(&[f32::NAN], 0).unwrap().is_empty());
    assert!(index.search(&[bf16::NAN], 0).unwrap().is_empty());
    for k in [4097, usize::MAX] {
        assert!(index.search_f32(&[0.0, 0.0], k).is_err());
        assert!(index.search(&[bf16::ZERO, bf16::ZERO], k).is_err());
    }
    for k in [1, 4096] {
        assert_eq!(index.search_f32(&[0.0, 0.0], k).unwrap().len(), 1);
    }
    let options = SearchOptions { ef_search: Some(1) };
    let mut workspace = SearchWorkspace::default();
    assert_eq!(
        index
            .search_f32_with_workspace(&[0.0, 0.0], 1, options, &mut workspace)
            .unwrap(),
        vec![(1, 0.0)]
    );
    assert!(
        index
            .search_f32_with_options(&[0.0, 0.0], 1, SearchOptions { ef_search: Some(0) })
            .is_err()
    );
}

#[test]
fn seeded_construction_is_reproducible() {
    let cfg = HnswConfig {
        max_layers: 2,
        ..config()
    };
    let a = HnswIndex::try_new_seeded("seed".into(), Some(cfg.clone()), 123).unwrap();
    let b = HnswIndex::try_new_seeded("seed".into(), Some(cfg), 123).unwrap();
    for id in 0..100 {
        let vector = vec![(id % 7) as f32, (id / 7) as f32];
        a.insert_f32(id, vector.clone(), 0).unwrap();
        b.insert_f32(id, vector, 0).unwrap();
    }
    // A deleted top-layer entry has many equally high replacements. Tie-break
    // by ID, rather than the concurrent hash table's randomized iteration order.
    let victim = a
        .node_ids()
        .into_iter()
        .max_by_key(|id| {
            (
                a.get_node_with(*id, |node| node.layer).unwrap(),
                std::cmp::Reverse(*id),
            )
        })
        .unwrap();
    assert!(a.remove(victim, 1));
    assert!(b.remove(victim, 1));
    assert_eq!(a.metadata_bytes().unwrap(), b.metadata_bytes().unwrap());
    for id in a.node_ids() {
        assert_eq!(
            a.get_node_with(id, serialize_node).unwrap(),
            b.get_node_with(id, serialize_node).unwrap()
        );
    }
}
