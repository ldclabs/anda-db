use super::*;
use crate::serialize_node;
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, atomic::AtomicBool};

struct FailingWriter;

impl Write for FailingWriter {
    fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
        Err(io::Error::other("writer failed"))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn test_config() -> HnswConfig {
    HnswConfig {
        dimension: 2,
        max_layers: 3,
        max_connections: 4,
        ef_construction: 8,
        ef_search: 8,
        ..Default::default()
    }
}

fn valid_node(id: u64) -> HnswNode {
    HnswNode {
        id,
        layer: 0,
        vector: vec![bf16::from_f32(id as f32), bf16::from_f32(id as f32 + 0.5)],
        neighbors: vec![SmallVec::new()],
        version: 1,
    }
}

fn metadata_bytes(metadata: &HnswMetadata, entry_point: (u64, u8)) -> Vec<u8> {
    let mut buf = Vec::new();
    cbor2::to_writer(
        &HnswIndexRef {
            entry_point,
            metadata,
            removed_nodes: Vec::new(),
        },
        &mut buf,
    )
    .unwrap();
    buf
}

#[test]
fn test_try_new_rejects_invalid_config() {
    let result = HnswIndex::try_new(
        "anda_db_hnsw".to_string(),
        Some(HnswConfig {
            dimension: 0,
            ..Default::default()
        }),
    );
    assert!(matches!(result, Err(HnswError::Generic { .. })));
}

#[test]
fn test_config_validation_normalization_and_accessors() {
    for config in [
        HnswConfig {
            max_layers: 0,
            dimension: 2,
            ..Default::default()
        },
        HnswConfig {
            max_connections: 1,
            dimension: 2,
            ..Default::default()
        },
        HnswConfig {
            ef_construction: 0,
            dimension: 2,
            ..Default::default()
        },
        HnswConfig {
            ef_search: 0,
            dimension: 2,
            ..Default::default()
        },
        HnswConfig {
            dimension: HnswConfig::MAX_DIMENSION + 1,
            ..Default::default()
        },
        HnswConfig {
            max_layers: HnswConfig::MAX_MAX_LAYERS + 1,
            dimension: 2,
            ..Default::default()
        },
        HnswConfig {
            max_connections: HnswConfig::MAX_MAX_CONNECTIONS + 1,
            dimension: 2,
            ..Default::default()
        },
        HnswConfig {
            ef_construction: HnswConfig::MAX_EF_CONSTRUCTION + 1,
            dimension: 2,
            ..Default::default()
        },
        HnswConfig {
            ef_search: HnswConfig::MAX_EF_SEARCH + 1,
            dimension: 2,
            ..Default::default()
        },
        HnswConfig {
            scale_factor: Some(0.0),
            dimension: 2,
            ..Default::default()
        },
        HnswConfig {
            scale_factor: Some(f64::INFINITY),
            dimension: 2,
            ..Default::default()
        },
    ] {
        assert!(config.validate("invalid").is_err());
    }

    let normalized = HnswConfig {
        max_layers: 0,
        max_connections: 1,
        ef_construction: 0,
        ef_search: 0,
        scale_factor: Some(f64::NEG_INFINITY),
        dimension: 2,
        ..Default::default()
    }
    .normalized();
    assert_eq!(normalized.max_layers, HnswConfig::MIN_MAX_LAYERS);
    assert_eq!(normalized.max_connections, HnswConfig::MIN_MAX_CONNECTIONS);
    assert_eq!(normalized.ef_construction, 1);
    assert_eq!(normalized.ef_search, 1);
    assert_eq!(normalized.scale_factor, None);

    let normalized = HnswConfig {
        dimension: HnswConfig::MAX_DIMENSION + 1,
        max_layers: HnswConfig::MAX_MAX_LAYERS + 1,
        max_connections: HnswConfig::MAX_MAX_CONNECTIONS + 1,
        ef_construction: HnswConfig::MAX_EF_CONSTRUCTION + 1,
        ef_search: HnswConfig::MAX_EF_SEARCH + 1,
        ..Default::default()
    }
    .normalized();
    assert_eq!(normalized.dimension, HnswConfig::MAX_DIMENSION);
    assert_eq!(normalized.max_layers, HnswConfig::MAX_MAX_LAYERS);
    assert_eq!(normalized.max_connections, HnswConfig::MAX_MAX_CONNECTIONS);
    assert_eq!(normalized.ef_construction, HnswConfig::MAX_EF_CONSTRUCTION);
    assert_eq!(normalized.ef_search, HnswConfig::MAX_EF_SEARCH);

    let config = HnswConfig {
        dimension: 2,
        scale_factor: Some(1.5),
        ..Default::default()
    };
    config.validate("valid").unwrap();
    let _ = config.layer_gen();

    let index = HnswIndex::new("getters".to_string(), Some(test_config()));
    assert!(index.is_empty());
    assert_eq!(index.name(), "getters");
    assert_eq!(index.dimension(), 2);
    assert_eq!(index.node_ids(), Vec::<u64>::new());
    assert!(matches!(
        index.get_node_with(42, |node| node.id),
        Err(HnswError::NotFound { id: 42, .. })
    ));
    assert_eq!(
        index.search_f32(&[0.0, 0.0], 3).unwrap(),
        Vec::<(u64, f32)>::new()
    );
}

#[test]
fn test_new_normalizes_runtime_config() {
    let index = HnswIndex::new(
        "anda_db_hnsw".to_string(),
        Some(HnswConfig {
            max_layers: 0,
            max_connections: 1,
            ef_construction: 0,
            ef_search: 0,
            scale_factor: Some(f64::NAN),
            dimension: 2,
            ..Default::default()
        }),
    );

    let metadata = index.metadata();
    assert_eq!(metadata.config.max_layers, 1);
    assert_eq!(metadata.config.max_connections, 2);
    assert_eq!(metadata.config.ef_construction, 1);
    assert_eq!(metadata.config.ef_search, 1);
    assert_eq!(metadata.config.scale_factor, None);
    index.insert_f32(1, vec![1.0, 1.0], 0).unwrap();
    assert_eq!(index.search_f32(&[1.0, 1.0], 1).unwrap().len(), 1);
}

#[test]
fn test_first_insert_advances_saved_empty_metadata_version() {
    let config = HnswConfig {
        dimension: 2,
        ..Default::default()
    };
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));

    let mut metadata = Vec::new();
    assert!(index.store_metadata(&mut metadata, 0).unwrap());
    assert!(!index.store_metadata(Vec::new(), 0).unwrap());

    index.insert_f32(1, vec![1.0, 1.0], 1).unwrap();

    let mut metadata_after_insert = Vec::new();
    assert!(index.store_metadata(&mut metadata_after_insert, 1).unwrap());
    let loaded = HnswIndex::load_metadata(&metadata_after_insert[..]).unwrap();
    assert_eq!(loaded.stats().version, 2);
}

#[tokio::test]
async fn test_load_nodes_removes_missing_ids() {
    let config = HnswConfig {
        dimension: 2,
        ..Default::default()
    };
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));
    index.insert_f32(1, vec![1.0, 1.0], 0).unwrap();
    index.insert_f32(2, vec![2.0, 2.0], 0).unwrap();

    let mut metadata = Vec::new();
    let mut ids = Vec::new();
    let mut nodes: HashMap<u64, Vec<u8>> = HashMap::new();
    index
        .flush(&mut metadata, &mut ids, 0, async |id, data| {
            nodes.insert(id, data.to_vec());
            Ok(true)
        })
        .await
        .unwrap();
    nodes.remove(&2);

    let loaded = HnswIndex::load_all(&metadata[..], &ids[..], async |id| {
        Ok(nodes.get(&id).map(|data| data.to_vec()))
    })
    .await
    .unwrap();

    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded.node_ids(), vec![1]);
    assert!(
        loaded
            .get_node_with(1, |node| node
                .neighbors
                .iter()
                .all(|neighbors| neighbors.iter().all(|(id, _)| *id != 2)))
            .unwrap(),
        "load should prune stale edges to missing node blobs"
    );
    assert!(
        loaded.has_dirty_nodes(),
        "pruned stale edges should be persisted on the next flush"
    );
    assert_eq!(loaded.search_f32(&[1.5, 1.5], 10).unwrap().len(), 1);
    assert!(loaded.stats().version > index.stats().version);
}

#[tokio::test]
async fn test_purge_removed_nodes_deletes_tombstones_and_skips_reinserts() {
    let config = HnswConfig {
        dimension: 2,
        ..Default::default()
    };
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));
    index.insert_f32(1, vec![1.0, 1.0], 0).unwrap();
    index.insert_f32(2, vec![2.0, 2.0], 0).unwrap();

    assert!(!index.has_removed_nodes());
    assert!(index.remove(2, 1));
    assert!(index.has_removed_nodes());

    index
        .flush(Vec::new(), Vec::new(), 1, async |_, _| Ok(true))
        .await
        .unwrap();
    // Purge hands a committed tombstone to the delete callback exactly once.
    let mut purged = Vec::new();
    index
        .purge_removed_nodes(async |id| {
            purged.push(id);
            Ok(true)
        })
        .await
        .unwrap();
    assert_eq!(purged, vec![2]);
    assert!(!index.has_removed_nodes());

    // A re-inserted id must not be purged: its blob belongs to the new node.
    assert!(index.remove(1, 2));
    index.insert_f32(1, vec![1.5, 1.5], 3).unwrap();
    let mut purged = Vec::new();
    index
        .purge_removed_nodes(async |id| {
            purged.push(id);
            Ok(true)
        })
        .await
        .unwrap();
    assert!(purged.is_empty());

    // On callback error the tombstone is refunded and retried later.
    assert!(index.remove(1, 4));
    index
        .flush(Vec::new(), Vec::new(), 4, async |_, _| Ok(true))
        .await
        .unwrap();
    let err = index
        .purge_removed_nodes(async |_| Err("boom".into()))
        .await
        .unwrap_err();
    assert!(matches!(err, HnswError::Generic { .. }));
    assert!(index.has_removed_nodes());
    index.purge_removed_nodes(async |_| Ok(true)).await.unwrap();
    assert!(!index.has_removed_nodes());
}

#[tokio::test]
async fn test_purge_removed_nodes_stop_keeps_current_tombstone_retryable() {
    let index = HnswIndex::new(
        "purge_stop".to_string(),
        Some(HnswConfig {
            dimension: 2,
            ..Default::default()
        }),
    );
    index.insert_f32(1, vec![1.0, 1.0], 0).unwrap();
    assert!(index.remove(1, 1));

    index
        .flush(Vec::new(), Vec::new(), 1, async |_, _| Ok(true))
        .await
        .unwrap();
    let mut attempted = Vec::new();
    index
        .purge_removed_nodes(async |id| {
            attempted.push(id);
            Ok(false)
        })
        .await
        .unwrap();
    assert_eq!(attempted, vec![1]);
    assert!(index.has_removed_nodes());

    let mut retried = Vec::new();
    index
        .purge_removed_nodes(async |id| {
            retried.push(id);
            Ok(true)
        })
        .await
        .unwrap();
    assert_eq!(retried, vec![1]);
    assert!(!index.has_removed_nodes());
}

#[tokio::test]
async fn test_purge_bumps_metadata_version_so_flush_persists_cleared_tombstones() {
    let index = HnswIndex::new("purge_flush".to_string(), Some(test_config()));
    index.insert_f32(1, vec![1.0, 1.0], 0).unwrap();
    index.insert_f32(2, vec![2.0, 2.0], 0).unwrap();
    assert!(index.remove(2, 1));

    // Flush #1: the tombstone for node 2 is persisted with the metadata.
    let mut metadata = Vec::new();
    let mut ids = Vec::new();
    let mut blobs: HashMap<u64, Vec<u8>> = HashMap::new();
    index
        .flush(&mut metadata, &mut ids, 2, async |id, data| {
            blobs.insert(id, data.to_vec());
            Ok(true)
        })
        .await
        .unwrap();
    assert!(!index.has_pending_metadata_flush());

    // Purge deletes the blob and must mark the metadata dirty again so
    // the cleared tombstone set reaches disk on the next flush.
    let mut purged = Vec::new();
    index
        .purge_removed_nodes(async |id| {
            purged.push(id);
            blobs.remove(&id);
            Ok(true)
        })
        .await
        .unwrap();
    assert_eq!(purged, vec![2]);
    assert!(!index.has_removed_nodes());
    assert!(
        index.has_pending_metadata_flush(),
        "purge must bump the metadata version so the next flush persists \
         the cleared tombstone set"
    );

    // Flush #2 persists the post-purge state.
    let mut metadata = Vec::new();
    let mut ids = Vec::new();
    assert!(
        index
            .flush(&mut metadata, &mut ids, 3, async |id, data| {
                blobs.insert(id, data.to_vec());
                Ok(true)
            })
            .await
            .unwrap()
    );
    assert!(!index.has_pending_metadata_flush());

    // Reload: the purged tombstone must NOT be replayed.
    let reloaded = HnswIndex::load_all(metadata.as_slice(), ids.as_slice(), async |id| {
        Ok(blobs.get(&id).cloned())
    })
    .await
    .unwrap();
    assert!(
        !reloaded.has_removed_nodes(),
        "purged tombstones must not be re-queued after reload"
    );
    let mut replayed = Vec::new();
    reloaded
        .purge_removed_nodes(async |id| {
            replayed.push(id);
            Ok(true)
        })
        .await
        .unwrap();
    assert!(replayed.is_empty(), "no deletions should be replayed");

    // An empty purge is a no-op and must not force metadata churn.
    assert!(!reloaded.has_pending_metadata_flush());
}

/// Builds a deterministic single-layer line graph (`max_layers == 1`
/// removes the layer randomness) so removals can be compared
/// structurally between the two `reconnect_on_delete` modes.
fn build_line_index(reconnect_on_delete: bool, n: u64) -> HnswIndex {
    let config = HnswConfig {
        dimension: 2,
        max_layers: 1,
        max_connections: 2,
        ef_construction: 8,
        ef_search: 8,
        reconnect_on_delete,
        ..Default::default()
    };
    let index = HnswIndex::new("reconnect_toggle".to_string(), Some(config));
    for id in 1..=n {
        index.insert_f32(id, vec![id as f32, 0.0], id).unwrap();
    }
    index
}

fn layer0_adjacency(index: &HnswIndex, n: u64) -> HashMap<u64, BTreeSet<u64>> {
    let mut adjacency = HashMap::new();
    for id in 1..=n {
        if let Ok(neighbors) = index.get_node_with(id, |node| {
            node.neighbors[0]
                .iter()
                .map(|&(nid, _)| nid)
                .collect::<BTreeSet<u64>>()
        }) {
            adjacency.insert(id, neighbors);
        }
    }
    adjacency
}

#[test]
fn test_remove_reconnect_on_delete_toggle() {
    const N: u64 = 30;
    const VICTIM: u64 = 15;

    let fast = build_line_index(false, N);
    let repairing = build_line_index(true, N);

    // The flag does not affect construction: both graphs are identical.
    let before = layer0_adjacency(&fast, N);
    assert_eq!(
        before,
        layer0_adjacency(&repairing, N),
        "construction must not depend on reconnect_on_delete"
    );

    assert!(fast.remove(VICTIM, 100));
    assert!(repairing.remove(VICTIM, 100));

    let after_fast = layer0_adjacency(&fast, N);
    let after_repairing = layer0_adjacency(&repairing, N);

    for (id, neighbors) in &after_fast {
        assert!(
            !neighbors.contains(&VICTIM),
            "node {id} still links to the removed node"
        );
        // reconnect_on_delete == false: edges can only be pruned, never
        // added, so every surviving list is a subset of its old list.
        assert!(
            neighbors.is_subset(&before[id]),
            "fast-path removal must not add edges: node {id} had {:?}, now {:?}",
            before[id],
            neighbors
        );
    }

    // reconnect_on_delete == true: the removed node's former neighbors
    // are re-linked through its remaining neighbors, so at least one
    // survivor gains an edge it did not have before.
    let mut gained = false;
    for (id, neighbors) in &after_repairing {
        assert!(
            !neighbors.contains(&VICTIM),
            "node {id} still links to the removed node"
        );
        if !neighbors.is_subset(&before[id]) {
            gained = true;
        }
    }
    assert!(
        gained,
        "reconnect mode must re-link at least one survivor to a new neighbor"
    );

    // Both modes keep the index searchable.
    assert_eq!(fast.search_f32(&[VICTIM as f32, 0.0], 3).unwrap().len(), 3);
    assert_eq!(
        repairing
            .search_f32(&[VICTIM as f32, 0.0], 3)
            .unwrap()
            .len(),
        3
    );
}

#[test]
fn test_config_reconnect_on_delete_defaults_to_false_for_legacy_metadata() {
    // Metadata persisted before the field existed must deserialize with
    // the repair disabled: those indexes were built without neighbor
    // repair, and 0.10.0 makes that the default again.
    #[derive(Serialize)]
    struct LegacyConfig {
        dimension: usize,
        max_layers: u8,
        max_connections: u8,
        ef_construction: usize,
        ef_search: usize,
        distance_metric: DistanceMetric,
        scale_factor: Option<f64>,
        select_neighbors_strategy: SelectNeighborsStrategy,
    }

    let legacy = LegacyConfig {
        dimension: 2,
        max_layers: 3,
        max_connections: 4,
        ef_construction: 8,
        ef_search: 8,
        distance_metric: DistanceMetric::Euclidean,
        scale_factor: None,
        select_neighbors_strategy: SelectNeighborsStrategy::Heuristic,
    };
    let mut buf = Vec::new();
    cbor2::to_writer(&legacy, &mut buf).unwrap();
    let config: HnswConfig = cbor2::from_reader(&buf[..]).unwrap();
    assert!(!config.reconnect_on_delete);

    // And a round-trip of the current config preserves an explicit true.
    let current = HnswConfig {
        reconnect_on_delete: true,
        ..Default::default()
    };
    let mut buf = Vec::new();
    cbor2::to_writer(&current, &mut buf).unwrap();
    let config: HnswConfig = cbor2::from_reader(&buf[..]).unwrap();
    assert!(config.reconnect_on_delete);
}

#[tokio::test]
async fn test_removed_tombstones_survive_flush_and_reload() {
    let index = HnswIndex::new("tombstones".to_string(), Some(test_config()));
    index.insert_f32(1, vec![1.0, 1.0], 0).unwrap();
    index.insert_f32(2, vec![2.0, 2.0], 0).unwrap();

    let mut metadata = Vec::new();
    let mut ids = Vec::new();
    let mut blobs: HashMap<u64, Vec<u8>> = HashMap::new();
    index
        .flush(&mut metadata, &mut ids, 1, async |id, data| {
            blobs.insert(id, data.to_vec());
            Ok(true)
        })
        .await
        .unwrap();

    // Remove a node and flush again, but crash BEFORE purge: the
    // tombstone must be persisted with the metadata.
    assert!(index.remove(2, 2));
    let mut metadata = Vec::new();
    let mut ids = Vec::new();
    index
        .flush(&mut metadata, &mut ids, 3, async |id, data| {
            blobs.insert(id, data.to_vec());
            Ok(true)
        })
        .await
        .unwrap();

    // Reload: the pending deletion is re-queued and handed to purge, so
    // the orphaned blob does not leak.
    let reloaded = HnswIndex::load_all(metadata.as_slice(), ids.as_slice(), async |id| {
        Ok(blobs.get(&id).cloned())
    })
    .await
    .unwrap();
    assert_eq!(reloaded.len(), 1);
    assert!(reloaded.has_removed_nodes());
    let mut purged = Vec::new();
    reloaded
        .purge_removed_nodes(async |id| {
            purged.push(id);
            Ok(true)
        })
        .await
        .unwrap();
    assert_eq!(purged, vec![2]);

    // Metadata written by older versions (without the tombstone field)
    // must still load.
    #[derive(Serialize)]
    struct LegacyIndexRef<'a> {
        entry_point: (u64, u8),
        metadata: &'a HnswMetadata,
    }
    let mut legacy = Vec::new();
    cbor2::to_writer(
        &LegacyIndexRef {
            entry_point: (1, 0),
            metadata: &index.metadata(),
        },
        &mut legacy,
    )
    .unwrap();
    let legacy_index = HnswIndex::load_metadata(legacy.as_slice()).unwrap();
    assert!(!legacy_index.has_removed_nodes());
}

#[tokio::test]
async fn test_flush_does_not_commit_metadata_when_node_write_fails() {
    let index = HnswIndex::new("flush_order".to_string(), Some(test_config()));
    index.insert_f32(1, vec![1.0, 1.0], 0).unwrap();

    // Node persistence fails: neither ids nor the metadata commit record
    // may be published, and the version watermark must remain pending.
    let mut metadata = Vec::new();
    let mut ids = Vec::new();
    let err = index
        .flush(&mut metadata, &mut ids, 1, async |_, _| {
            Err::<bool, _>("node write failed".into())
        })
        .await
        .unwrap_err();
    assert!(matches!(err, HnswError::Generic { .. }));
    assert!(metadata.is_empty());
    assert!(ids.is_empty());
    assert!(index.has_pending_metadata_flush());
    assert!(index.has_dirty_nodes());

    // The next flush retries nodes, ids and metadata together.
    let mut metadata = Vec::new();
    let mut ids = Vec::new();
    let mut blobs: HashMap<u64, Vec<u8>> = HashMap::new();
    assert!(
        index
            .flush(&mut metadata, &mut ids, 2, async |id, data| {
                blobs.insert(id, data.to_vec());
                Ok(true)
            })
            .await
            .unwrap()
    );
    assert!(!metadata.is_empty());
    assert!(!index.has_pending_metadata_flush());
    assert!(!index.has_dirty_nodes());

    let reloaded = HnswIndex::load_all(metadata.as_slice(), ids.as_slice(), async |id| {
        Ok(blobs.get(&id).cloned())
    })
    .await
    .unwrap();
    assert_eq!(reloaded.len(), 1);
    assert_eq!(reloaded.search_f32(&[1.0, 1.0], 1).unwrap()[0].0, 1);
}

#[tokio::test]
async fn test_flush_snapshot_excludes_mutation_crossing_node_put() {
    let index = HnswIndex::new("flush_snapshot".to_string(), Some(test_config()));
    index.insert_f32(1, vec![1.0, 1.0], 0).unwrap();

    let persisted_nodes = Arc::new(Mutex::new(HashMap::<u64, Vec<u8>>::new()));
    let persisted_ids = Arc::new(Mutex::new(Vec::new()));
    let persisted_metadata = Arc::new(Mutex::new(Vec::new()));
    let inserted_during_io = Arc::new(AtomicBool::new(false));

    assert!(
        index
            .flush_with(
                1,
                |id, data| {
                    persisted_nodes.lock().insert(id, data);
                    if !inserted_during_io.swap(true, Ordering::AcqRel) {
                        // The immutable snapshot was already captured. This
                        // mutation must stay wholly in the next generation,
                        // even though it crosses the node-write callback.
                        index.insert_f32(2, vec![2.0, 2.0], 2).unwrap();
                    }
                    std::future::ready(Ok::<bool, BoxError>(true))
                },
                |data| {
                    *persisted_ids.lock() = data;
                    std::future::ready(Ok::<(), BoxError>(()))
                },
                |data| {
                    *persisted_metadata.lock() = data;
                    std::future::ready(Ok::<(), BoxError>(()))
                },
            )
            .await
            .unwrap()
    );

    // The committed image is generation 1 only; generation 2 remains
    // pending rather than leaking into ids or metadata.
    let first_metadata = persisted_metadata.lock().clone();
    let first_ids = persisted_ids.lock().clone();
    let first_nodes = persisted_nodes.clone();
    let first = HnswIndex::load_all(
        first_metadata.as_slice(),
        first_ids.as_slice(),
        async move |id| Ok(first_nodes.lock().get(&id).cloned()),
    )
    .await
    .unwrap();
    assert_eq!(first.node_ids(), vec![1]);
    assert!(index.has_pending_metadata_flush());
    assert!(index.has_dirty_nodes());

    // A retry snapshots and commits the later mutation, including the
    // neighbor rewrite of node 1 that happened during the first I/O pass.
    index
        .flush_with(
            3,
            |id, data| {
                persisted_nodes.lock().insert(id, data);
                std::future::ready(Ok::<bool, BoxError>(true))
            },
            |data| {
                *persisted_ids.lock() = data;
                std::future::ready(Ok::<(), BoxError>(()))
            },
            |data| {
                *persisted_metadata.lock() = data;
                std::future::ready(Ok::<(), BoxError>(()))
            },
        )
        .await
        .unwrap();
    assert!(!index.has_pending_metadata_flush());
    assert!(!index.has_dirty_nodes());

    let final_metadata = persisted_metadata.lock().clone();
    let final_ids = persisted_ids.lock().clone();
    let final_nodes = persisted_nodes.clone();
    let final_index = HnswIndex::load_all(
        final_metadata.as_slice(),
        final_ids.as_slice(),
        async move |id| Ok(final_nodes.lock().get(&id).cloned()),
    )
    .await
    .unwrap();
    assert_eq!(final_index.node_ids(), vec![1, 2]);
}

#[tokio::test]
async fn test_store_metadata_with_reverts_claim_on_callback_error() {
    let config = HnswConfig {
        dimension: 2,
        ..Default::default()
    };
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));
    index.insert_f32(1, vec![1.0, 1.0], 0).unwrap();

    // A failing persist callback must not consume the version claim.
    let err = index
        .store_metadata_with(1, async |_| Err("io".into()))
        .await
        .unwrap_err();
    assert!(matches!(err, HnswError::Generic { .. }));

    // The retry must still serialize this version.
    let mut persisted = Vec::new();
    assert!(
        index
            .store_metadata_with(2, async |data| {
                persisted.extend_from_slice(data);
                Ok(())
            })
            .await
            .unwrap()
    );
    assert!(!persisted.is_empty());
    // And now it is a no-op.
    assert!(
        !index
            .store_metadata_with(3, async |_| Ok(()))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_store_metadata_with_cancellation_keeps_generation_pending() {
    let index = Arc::new(HnswIndex::new(
        "metadata_cancel".to_string(),
        Some(test_config()),
    ));
    index.insert_f32(1, vec![1.0, 1.0], 0).unwrap();

    let entered = Arc::new(tokio::sync::Notify::new());
    let task_index = index.clone();
    let task_entered = entered.clone();
    let task = tokio::spawn(async move {
        task_index
            .store_metadata_with(1, async move |_| {
                task_entered.notify_one();
                std::future::pending::<Result<(), BoxError>>().await
            })
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .expect("callback did not start");
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());

    assert!(index.has_pending_metadata_flush());
    assert!(
        index
            .store_metadata_with(2, async |_| Ok(()))
            .await
            .unwrap()
    );
    assert!(!index.has_pending_metadata_flush());
}

#[tokio::test]
async fn test_store_dirty_nodes_cancellation_keeps_current_node_dirty() {
    let index = Arc::new(HnswIndex::new(
        "dirty_cancel".to_string(),
        Some(test_config()),
    ));
    index.insert_f32(1, vec![1.0, 1.0], 0).unwrap();

    let entered = Arc::new(tokio::sync::Notify::new());
    let task_index = index.clone();
    let task_entered = entered.clone();
    let task = tokio::spawn(async move {
        task_index
            .store_dirty_nodes(async move |_, _| {
                task_entered.notify_one();
                std::future::pending::<Result<bool, BoxError>>().await
            })
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .expect("callback did not start");
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());

    assert!(index.has_dirty_nodes());
    let persisted = Arc::new(Mutex::new(Vec::new()));
    let output = persisted.clone();
    index
        .store_dirty_nodes(async move |id, _| {
            output.lock().push(id);
            Ok(true)
        })
        .await
        .unwrap();
    assert_eq!(*persisted.lock(), vec![1]);
    assert!(!index.has_dirty_nodes());
}

/// Regression: `Ok(false)` means "stop", not "this id is persisted".
/// `store_dirty_nodes` used to retire the dirty mark *before* it looked at
/// the callback's answer, so a caller writing one callback style for both
/// this API and `purge_removed_nodes` silently lost that node's rewritten
/// adjacency list: `flush_with` keys its snapshot off `dirty_nodes`, so
/// nothing was pending afterwards and no later flush rewrote the blob.
#[tokio::test]
async fn test_store_dirty_nodes_stop_keeps_current_node_dirty() {
    let index = HnswIndex::new("stop_keeps_dirty".to_string(), Some(test_config()));
    index.insert_f32(1, vec![1.0, 1.0], 1).unwrap();
    index.insert_f32(2, vec![2.0, 2.0], 1).unwrap();

    // Stop on the very first id, acknowledging nothing.
    let visited = Arc::new(Mutex::new(Vec::new()));
    let first = visited.clone();
    index
        .store_dirty_nodes(async move |id, _| {
            first.lock().push(id);
            Ok(false)
        })
        .await
        .unwrap();
    assert_eq!(*visited.lock(), vec![1]);

    // The next call must offer the stopped-on id again, together with the
    // ids it never reached.
    let retried = Arc::new(Mutex::new(Vec::new()));
    let second = retried.clone();
    index
        .store_dirty_nodes(async move |id, _| {
            second.lock().push(id);
            Ok(true)
        })
        .await
        .unwrap();
    assert_eq!(
        *retried.lock(),
        vec![1, 2],
        "the id the callback stopped on must stay retryable"
    );
    assert!(!index.has_dirty_nodes());
}

#[tokio::test]
async fn test_purge_removed_nodes_cancellation_keeps_tombstone_retryable() {
    let index = Arc::new(HnswIndex::new(
        "purge_cancel".to_string(),
        Some(test_config()),
    ));
    index.insert_f32(1, vec![1.0, 1.0], 0).unwrap();
    index
        .store_dirty_nodes(async |_, _| Ok(true))
        .await
        .unwrap();
    assert!(index.remove(1, 1));
    index
        .store_metadata_with(2, async |_| Ok(()))
        .await
        .unwrap();
    assert!(!index.has_pending_metadata_flush());

    index
        .flush(Vec::new(), Vec::new(), 2, async |_, _| Ok(true))
        .await
        .unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let task_index = index.clone();
    let task_entered = entered.clone();
    let task = tokio::spawn(async move {
        task_index
            .purge_removed_nodes(async move |_| {
                task_entered.notify_one();
                std::future::pending::<Result<bool, BoxError>>().await
            })
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .expect("callback did not start");
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());

    assert!(index.has_removed_nodes());
    index.purge_removed_nodes(async |_| Ok(true)).await.unwrap();
    assert!(!index.has_removed_nodes());
    assert!(index.has_pending_metadata_flush());
}

#[test]
fn test_search_top_k_zero_is_fast_empty_result() {
    let config = HnswConfig {
        dimension: 2,
        ..Default::default()
    };
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));
    index.insert_f32(1, vec![1.0, 1.0], 0).unwrap();

    assert!(index.search_f32(&[1.0, 1.0], 0).unwrap().is_empty());
    assert_eq!(index.stats().search_count, 0);
}

#[test]
fn test_search_rejects_non_finite_query_values() {
    let config = HnswConfig {
        dimension: 2,
        ..Default::default()
    };
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));
    index.insert_f32(1, vec![1.0, 1.0], 0).unwrap();

    let result = index.search_f32(&[f32::NAN, 1.0], 1);
    assert!(matches!(result, Err(HnswError::Generic { .. })));

    let result = index.search(&[bf16::from_f32(f32::INFINITY), bf16::from_f32(1.0)], 1);
    assert!(matches!(result, Err(HnswError::Generic { .. })));

    assert!(
        index
            .search_f32(&[f32::NAN, f32::INFINITY], 0)
            .unwrap()
            .is_empty()
    );
}

/// Regression: an edge recorded at layer `L` must point at a node that
/// exists at layer `L`. Every node that raised the max layer used to break
/// that invariant — at the brand-new top layer `search_layer` returns the
/// (lower-layer) entry point unexpanded, `select_neighbors` passes it
/// through, and the forward edge was recorded even though the reverse-edge
/// guard correctly refused the mirror. The new top layer then held exactly
/// one node whose only edge was a dead end, and the greedy descent
/// followed it into a layer the target does not belong to.
#[test]
fn test_forward_edges_never_point_below_their_layer() {
    let config = HnswConfig {
        dimension: 2,
        max_layers: 6,
        max_connections: 4,
        ef_construction: 16,
        ef_search: 16,
        // Dense upper layers, so many inserts raise the max layer — the
        // only situation that produced the asymmetric edge.
        scale_factor: Some(3.0),
        ..Default::default()
    };
    let index = HnswIndex::new("layer_edges".to_string(), Some(config));
    for id in 0..300u64 {
        index
            .insert_f32(id, vec![(id % 17) as f32, (id / 17) as f32], 0)
            .unwrap();
    }
    assert!(
        index.stats().max_layer > 0,
        "the corpus never raised the max layer, so nothing was exercised"
    );

    let nodes = index.nodes.pin();
    for (_, node) in nodes.iter() {
        for (layer, neighbors) in node.neighbors.iter().enumerate() {
            for &(neighbor_id, _) in neighbors.iter() {
                let neighbor = nodes
                    .get(&neighbor_id)
                    .unwrap_or_else(|| panic!("edge to unknown node {neighbor_id}"));
                assert!(
                    neighbor.layer as usize >= layer,
                    "node {} has a layer-{layer} edge to node {neighbor_id}, \
                     which only exists up to layer {}",
                    node.id,
                    neighbor.layer
                );
            }
        }
    }
}

#[test]
fn test_remove_repairs_entry_point_and_max_layer() {
    let config = HnswConfig {
        dimension: 2,
        ..Default::default()
    };
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));
    let nodes = index.nodes.pin();
    nodes.insert(
        1,
        Arc::new(GraphNode::from(HnswNode {
            id: 1,
            layer: 3,
            vector: vec![bf16::from_f32(1.0), bf16::from_f32(1.0)],
            neighbors: vec![
                SmallVec::new(),
                SmallVec::new(),
                SmallVec::new(),
                SmallVec::new(),
            ],
            version: 1,
        })),
    );
    nodes.insert(
        2,
        Arc::new(GraphNode::from(HnswNode {
            id: 2,
            layer: 1,
            vector: vec![bf16::from_f32(2.0), bf16::from_f32(2.0)],
            neighbors: vec![SmallVec::new(), SmallVec::new()],
            version: 1,
        })),
    );
    index.ids.write().add(1);
    index.ids.write().add(2);
    *index.entry_point.write() = (1, 3);
    index.update_metadata(|metadata| metadata.stats.max_layer = 3);

    assert!(index.remove(1, 0));
    assert_eq!(*index.entry_point.read(), (2, 1));
    assert_eq!(index.stats().max_layer, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_duplicate_insert_only_one_succeeds() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Barrier;

    let config = HnswConfig {
        dimension: 2,
        ..Default::default()
    };
    let index = Arc::new(HnswIndex::new("anda_db_hnsw".to_string(), Some(config)));
    let barrier = Arc::new(Barrier::new(16));
    let successes = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for _ in 0..16 {
        let index = Arc::clone(&index);
        let barrier = Arc::clone(&barrier);
        let successes = Arc::clone(&successes);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            if index.insert_f32(1, vec![1.0, 1.0], 0).is_ok() {
                successes.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for result in futures::future::join_all(handles).await {
        result.unwrap();
    }
    assert_eq!(successes.load(Ordering::Relaxed), 1);
    assert_eq!(index.len(), 1);
    assert_eq!(index.stats().insert_count, 1);
}

#[tokio::test]
async fn test_hnsw_basic() {
    let config = HnswConfig {
        dimension: 2,
        ..Default::default()
    };
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));

    // Add some 2D vectors.
    index.insert_f32(1, vec![1.0, 1.0], 0).unwrap();
    index.insert_f32(2, vec![1.0, 2.0], 0).unwrap();
    index.insert_f32(4, vec![2.0, 2.0], 0).unwrap();
    index.insert_f32(3, vec![2.0, 1.0], 0).unwrap();
    index.insert_f32(5, vec![3.0, 3.0], 0).unwrap();
    println!("Added vectors to index.");

    let ids = index.node_ids();
    assert_eq!(ids.into_iter().collect::<Vec<_>>(), vec![1, 2, 3, 4, 5]);

    let data = index.get_node_with(1, serialize_node).unwrap();
    let node: HnswNode = cbor2::from_reader(&data[..]).unwrap();
    println!("Node data: {node:?}");
    assert_eq!(node.vector, vec![bf16::from_f32(1.0), bf16::from_f32(1.0)]);
    assert!(!node.neighbors[0].is_empty());

    // Search for the nearest neighbors.
    let results = index.search_f32(&[1.1, 1.1], 2).unwrap();
    assert_eq!(results.len(), 2);
    assert!(results[0].1 < results[1].1);
    println!("Search results: {results:?}");

    // Round-trip through persistence.
    let mut metadata = Vec::new();
    let mut ids = Vec::new();
    let mut nodes: HashMap<u64, Vec<u8>> = HashMap::new();
    index
        .flush(&mut metadata, &mut ids, 0, async |id, data| {
            nodes.insert(id, data.to_vec());
            Ok(true)
        })
        .await
        .unwrap();

    let loaded_index = HnswIndex::load_all(&metadata[..], &ids[..], async |id| {
        Ok(nodes.get(&id).map(|v| v.to_vec()))
    })
    .await
    .unwrap();

    println!("Loaded index stats: {:?}", loaded_index.stats());
    let loaded_results = loaded_index.search_f32(&[1.1, 1.1], 2).unwrap();
    assert_eq!(results, loaded_results);
}

#[test]
#[allow(clippy::approx_constant)]
fn test_distance_metrics() {
    let v1 = vec![1.0, 0.0];
    let v2 = vec![0.0, 1.0];

    // Euclidean.
    let config = HnswConfig {
        dimension: 2,
        distance_metric: DistanceMetric::Euclidean,
        ..Default::default()
    };
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));
    index.insert_f32(1, v1.clone(), 0).unwrap();
    let results = index.search_f32(&v2, 1).unwrap();
    assert!((results[0].1 - 1.4142135).abs() < 1e-6);

    // Cosine.
    let config = HnswConfig {
        dimension: 2,
        distance_metric: DistanceMetric::Cosine,
        ..Default::default()
    };
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));
    index.insert_f32(1, v1.clone(), 0).unwrap();
    let results = index.search_f32(&v2, 1).unwrap();
    assert!((results[0].1 - 1.0).abs() < 1e-6);

    // Inner product.
    let config = HnswConfig {
        dimension: 2,
        distance_metric: DistanceMetric::InnerProduct,
        ..Default::default()
    };
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));
    index.insert_f32(1, v1.clone(), 0).unwrap();
    let results = index.search_f32(&v2, 1).unwrap();
    assert!((results[0].1 - 0.0).abs() < 1e-6);
}

#[test]
fn test_manhattan_distance() {
    let v1 = vec![1.0, 0.0];
    let v2 = vec![0.0, 1.0];

    // Manhattan.
    let config = HnswConfig {
        dimension: 2,
        distance_metric: DistanceMetric::Manhattan,
        ..Default::default()
    };
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));
    index.insert_f32(1, v1.clone(), 0).unwrap();
    let results = index.search_f32(&v2, 1).unwrap();
    assert!((results[0].1 - 2.0).abs() < 1e-6);
}

#[test]
fn test_dimension_mismatch() {
    let config = HnswConfig {
        dimension: 3,
        ..Default::default()
    };
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));

    // Inserting a vector whose dimensionality disagrees with the config.
    let result = index.insert_f32(1, vec![1.0, 2.0], 0);
    assert!(matches!(
        result,
        Err(HnswError::DimensionMismatch {
            expected: 3,
            got: 2,
            ..
        })
    ));

    // Inserting a correctly-shaped vector succeeds.
    index.insert_f32(1, vec![1.0, 2.0, 3.0], 0).unwrap();

    // Searching with a mismatched query is rejected.
    let result = index.search_f32(&[1.0, 2.0], 5);
    assert!(matches!(
        result,
        Err(HnswError::DimensionMismatch {
            expected: 3,
            got: 2,
            ..
        })
    ));
}

#[test]
fn test_duplicate_insert() {
    let config = HnswConfig {
        dimension: 2,
        ..Default::default()
    };
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));

    // First insert succeeds.
    index.insert_f32(1, vec![1.0, 2.0], 0).unwrap();

    // Re-inserting the same id must fail.
    let result = index.insert_f32(1, vec![3.0, 4.0], 0);
    assert!(matches!(
        result,
        Err(HnswError::AlreadyExists { id: 1, .. })
    ));
}

#[test]
fn test_remove() {
    let config = HnswConfig {
        dimension: 2,
        ..Default::default()
    };
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));

    // Populate.
    index.insert_f32(1, vec![1.0, 1.0], 0).unwrap();
    index.insert_f32(2, vec![2.0, 2.0], 0).unwrap();
    index.insert_f32(3, vec![3.0, 3.0], 0).unwrap();

    assert_eq!(index.len(), 3);

    // Remove an existing id.
    let deleted = index.remove(2, 0);
    assert!(deleted);
    assert_eq!(index.len(), 2);

    // Removing a missing id is a no-op.
    let deleted = index.remove(4, 0);
    assert!(!deleted);

    // Searches must only see the survivors.
    let results = index.search_f32(&[1.5, 1.5], 5).unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.iter().all(|(id, _)| *id == 1 || *id == 3));
}

#[test]
fn test_select_neighbors_strategies() {
    // Simple strategy.
    let config = HnswConfig {
        dimension: 2,
        select_neighbors_strategy: SelectNeighborsStrategy::Simple,
        ..Default::default()
    };
    let simple_index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));

    // Heuristic strategy.
    let config = HnswConfig {
        dimension: 2,
        select_neighbors_strategy: SelectNeighborsStrategy::Heuristic,
        ..Default::default()
    };
    let heuristic_index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));

    // Insert the same points into both indexes.
    for i in 0..20 {
        let x = (i % 5) as f32;
        let y = (i / 5) as f32;
        simple_index.insert_f32(i, vec![x, y], 0).unwrap();
        heuristic_index.insert_f32(i, vec![x, y], 0).unwrap();
    }

    // Both strategies must return the requested top-k.
    let simple_results = simple_index.search_f32(&[2.5, 2.5], 5).unwrap();
    let heuristic_results = heuristic_index.search_f32(&[2.5, 2.5], 5).unwrap();

    // Both strategies should return 5 results.
    assert_eq!(simple_results.len(), 5);
    assert_eq!(heuristic_results.len(), 5);
}

#[test]
fn test_select_neighbors_invalid_bf16_and_pending_metadata_flush() {
    let config = HnswConfig {
        dimension: 2,
        ..Default::default()
    };
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));
    assert!(index.has_pending_metadata_flush());

    assert!(matches!(
        index.insert(99, vec![bf16::from_f32(f32::NAN), bf16::from_f32(1.0)], 0,),
        Err(HnswError::Generic { .. })
    ));

    index.insert_f32(1, vec![0.0, 0.0], 1).unwrap();
    index.insert_f32(2, vec![1.0, 0.0], 2).unwrap();
    index.insert_f32(3, vec![0.0, 1.0], 3).unwrap();
    assert!(index.has_pending_metadata_flush());

    let mut layer_cache = SearchWorkspace::default();
    assert!(matches!(
        index.search_layer(
            &crate::distance::PreparedQuery::new(DistanceMetric::Euclidean, &[0.0, 0.0]),
            u64::MAX,
            0,
            0,
            0,
            &mut layer_cache
        ),
        Err(HnswError::NotFound { id: u64::MAX, .. })
    ));

    let mut pair_cache = FxHashMap::default();
    let simple = index
        .select_neighbors(
            vec![(3, 0.3, 0), (1, 0.1, 0), (2, 0.2, 0)],
            2,
            SelectNeighborsStrategy::Simple,
            &mut pair_cache,
        )
        .unwrap();
    assert_eq!(simple, vec![(1, 0.1, 0), (2, 0.2, 0)]);

    let empty_index = HnswIndex::new(
        "empty_hnsw".to_string(),
        Some(HnswConfig {
            dimension: 2,
            ..Default::default()
        }),
    );
    assert_eq!(empty_index.repair_entry_point(), 0);

    let mut writer = FailingWriter;
    assert!(writer.flush().is_ok());
}

#[test]
fn test_select_neighbors_heuristic_diversity_and_backfill() {
    let config = HnswConfig {
        dimension: 2,
        ..Default::default()
    };
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));
    // Query point is the origin. Node 2 is "shadowed" by node 1 (it is
    // closer to node 1 than to the query), nodes 3 and 4 span other
    // directions.
    index.insert_f32(1, vec![1.0, 0.0], 0).unwrap();
    index.insert_f32(2, vec![1.2, 0.0], 0).unwrap();
    index.insert_f32(3, vec![0.0, 1.4], 0).unwrap();
    index.insert_f32(4, vec![-1.6, 0.0], 0).unwrap();

    let candidates = vec![(1, 1.0, 0), (2, 1.2, 0), (3, 1.4, 0), (4, 1.6, 0)];

    // With m = 2, the diversity rule must skip node 2 (dist(2, 1) = 0.2 <
    // dist(2, query) = 1.2) and pick node 3 instead.
    let mut cache = FxHashMap::default();
    let selected = index
        .select_neighbors(
            candidates.clone(),
            2,
            SelectNeighborsStrategy::Heuristic,
            &mut cache,
        )
        .unwrap();
    assert_eq!(selected.len(), 2);
    assert_eq!(selected[0].0, 1);
    assert_eq!(selected[1].0, 3);
    assert!(!cache.is_empty());

    // With m = 3, the third diverse pick is node 4; node 2 stays pruned.
    let selected = index
        .select_neighbors(
            candidates.clone(),
            3,
            SelectNeighborsStrategy::Heuristic,
            &mut cache,
        )
        .unwrap();
    assert_eq!(
        selected.iter().map(|(id, ..)| *id).collect::<Vec<_>>(),
        vec![1, 3, 4]
    );

    // Candidate count ≤ m passes through unchanged.
    let passthrough = index
        .select_neighbors(
            candidates.clone(),
            4,
            SelectNeighborsStrategy::Heuristic,
            &mut cache,
        )
        .unwrap();
    assert_eq!(passthrough, candidates);

    // All shadowed by node 1: backfill must still deliver exactly m edges,
    // closest pruned candidates first.
    index.insert_f32(5, vec![1.1, 0.1], 0).unwrap();
    let clustered = vec![(1, 1.0, 0), (2, 1.2, 0), (5, 1.3, 0)];
    let selected = index
        .select_neighbors(clustered, 2, SelectNeighborsStrategy::Heuristic, &mut cache)
        .unwrap();
    assert_eq!(
        selected.iter().map(|(id, ..)| *id).collect::<Vec<_>>(),
        vec![1, 2]
    );

    // m == 0 yields no neighbors.
    let empty = index
        .select_neighbors(
            vec![(1, 1.0, 0), (2, 1.2, 0)],
            0,
            SelectNeighborsStrategy::Heuristic,
            &mut cache,
        )
        .unwrap();
    assert!(empty.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_search_is_robust_while_entry_point_is_removed() {
    use std::sync::Arc;

    let config = HnswConfig {
        dimension: 2,
        ..Default::default()
    };
    let index = Arc::new(HnswIndex::new("anda_db_hnsw".to_string(), Some(config)));
    for i in 0..64u64 {
        index
            .insert_f32(i, vec![(i % 8) as f32, (i / 8) as f32], 0)
            .unwrap();
    }

    // Writer: keep removing and re-inserting the current entry point so
    // searches race against entry-point invalidation.
    let writer = {
        let index = Arc::clone(&index);
        tokio::task::spawn_blocking(move || {
            for _ in 0..300 {
                let (entry_id, _) = *index.entry_point.read();
                if index.remove(entry_id, 0) {
                    index
                        .insert_f32(
                            entry_id,
                            vec![(entry_id % 8) as f32, (entry_id / 8) as f32],
                            0,
                        )
                        .unwrap();
                }
            }
        })
    };

    let mut readers = Vec::new();
    for t in 0..4u64 {
        let index = Arc::clone(&index);
        readers.push(tokio::task::spawn_blocking(move || {
            for i in 0..500u64 {
                let q = [((t + i) % 8) as f32, (i % 8) as f32];
                // Transient entry-point removal must never surface as an
                // error: search retries from the repaired entry point.
                let results = index.search_f32(&q, 5).unwrap();
                assert!(!results.is_empty());
            }
        }));
    }

    writer.await.unwrap();
    for reader in readers {
        reader.await.unwrap();
    }
    assert_eq!(index.len(), 64);
}

#[test]
fn test_corrupt_entry_point_fails_search_but_insert_self_heals() {
    let config = HnswConfig {
        dimension: 2,
        ..Default::default()
    };
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));
    index.insert_f32(1, vec![1.0, 1.0], 0).unwrap();

    // Simulate a corrupted entry point referencing a missing node.
    *index.entry_point.write() = (404, 0);

    // Search retries and then surfaces the corruption.
    let result = index.search_f32(&[1.0, 1.0], 1);
    assert!(matches!(result, Err(HnswError::NotFound { id: 404, .. })));
    assert_eq!(index.stats().search_count, 0);

    // The next insert self-heals the entry point and search recovers.
    index.insert_f32(2, vec![2.0, 2.0], 0).unwrap();
    let entry_id = index.entry_point.read().0;
    assert!(index.nodes.pin().contains_key(&entry_id));
    let results = index.search_f32(&[1.0, 1.0], 2).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, 1);
}

#[tokio::test]
async fn test_file_persistence() {
    let mut metadata = Vec::new();
    let mut ids = Vec::new();
    let mut nodes: HashMap<u64, Vec<u8>> = HashMap::new();

    // Build and populate the index.
    {
        let config = HnswConfig {
            dimension: 3,
            ..Default::default()
        };
        let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));

        for i in 0..100 {
            let x = (i % 10) as f32;
            let y = ((i / 10) % 10) as f32;
            let z = (i / 100) as f32;
            index.insert_f32(i, vec![x, y, z], 0).unwrap();
        }

        index
            .flush(&mut metadata, &mut ids, 0, async |id, data| {
                nodes.insert(id, data.to_vec());
                Ok(true)
            })
            .await
            .unwrap();
    }

    {
        let loaded_index = HnswIndex::load_all(&metadata[..], &ids[..], async |id| {
            Ok(nodes.get(&id).map(|v| v.to_vec()))
        })
        .await
        .unwrap();

        // Verify element count after reload.
        assert_eq!(loaded_index.len(), 100);

        // Verify that search still works.
        let results = loaded_index.search_f32(&[5.0, 5.0, 0.0], 10).unwrap();
        assert_eq!(results.len(), 10);
    }
}

#[tokio::test]
async fn test_flush_persists_dirty_nodes_even_if_metadata_already_saved() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let config = HnswConfig {
        dimension: 2,
        ..Default::default()
    };
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));

    index.insert_f32(1, vec![1.0, 1.0], 0).unwrap();
    assert!(index.has_dirty_nodes());

    // Save metadata first (simulate metadata already persisted, nodes pending).
    let mut metadata = Vec::new();
    assert!(index.store_metadata(&mut metadata, 0).unwrap());
    assert!(index.has_dirty_nodes());

    // flush should still persist dirty nodes even when metadata version is unchanged.
    let writes = Arc::new(AtomicUsize::new(0));
    let writes_clone = Arc::clone(&writes);
    let mut metadata2 = Vec::new();
    let mut ids = Vec::new();
    let saved = index
        .flush(&mut metadata2, &mut ids, 0, async move |_, _| {
            writes_clone.fetch_add(1, Ordering::Relaxed);
            Ok(true)
        })
        .await
        .unwrap();

    assert!(saved);
    assert_eq!(writes.load(Ordering::Relaxed), 1);
    assert!(!index.has_dirty_nodes());
}

#[test]
fn test_stats() {
    let config = HnswConfig {
        dimension: 2,
        ..Default::default()
    };
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));

    // Initial state.
    let stats = index.stats();
    assert_eq!(stats.num_elements, 0);
    assert_eq!(stats.insert_count, 0);
    assert_eq!(stats.search_count, 0);
    assert_eq!(stats.delete_count, 0);

    // Populate.
    for i in 0..10 {
        index.insert_f32(i, vec![i as f32, i as f32], 0).unwrap();
    }

    let stats = index.stats();
    assert_eq!(stats.num_elements, 10);
    assert_eq!(stats.insert_count, 10);

    // Issue some searches.
    for _ in 0..5 {
        index.search_f32(&[5.0, 5.0], 3).unwrap();
    }

    let stats = index.stats();
    assert_eq!(stats.search_count, 5);

    // Delete.
    index.remove(5, 0);
    index.remove(6, 0);

    let stats = index.stats();
    assert_eq!(stats.num_elements, 8);
    assert_eq!(stats.delete_count, 2);
}

#[test]
fn test_bf16_conversion() {
    // Check f32 → bf16 round-trip precision.
    let original = [1.234f32, 5.678f32, 9.012f32];
    let bf16_vec: Vec<bf16> = original.iter().map(|&x| bf16::from_f32(x)).collect();
    let back_to_f32: Vec<f32> = bf16_vec.iter().map(|x| x.to_f32()).collect();

    // bf16 has limited precision; tolerate a small rounding error.
    for (i, (orig, converted)) in original.iter().zip(back_to_f32.iter()).enumerate() {
        println!(
            "Original: {}, Converted: {}, Diff: {}",
            orig,
            converted,
            (orig - converted).abs()
        );
        // Allow some bounded error.
        assert!(
            (orig - converted).abs() < 0.1,
            "Too much precision loss at index {i}"
        );
    }
}

#[test]
fn test_large_dimension() {
    // Exercise high-dimensional vectors.
    let dim = 128;
    let config = HnswConfig {
        dimension: dim,
        ..Default::default()
    };
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));

    // Populate with several high-dim vectors.
    for i in 0..10 {
        let vec = vec![i as f32 / 10.0; dim];
        index.insert_f32(i, vec, 0).unwrap();
    }

    // Search.
    let query = vec![0.35; dim];
    let results = index.search_f32(&query, 3).unwrap();

    assert_eq!(results.len(), 3);
    // The closest vector should be the one for 0.3 or 0.4.
    assert!(results[0].0 == 3 || results[0].0 == 4);
}

#[test]
fn test_entry_point_update() {
    let config = HnswConfig {
        dimension: 2,
        ..Default::default()
    };
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));

    // Seed the index.
    index.insert_f32(1, vec![1.0, 1.0], 0).unwrap();

    // Observe the current entry point.
    let (entry_id, _) = *index.entry_point.read();
    assert_eq!(entry_id, 1);

    // Delete the entry-point node.
    index.remove(entry_id, 0);

    // A subsequent insert must become the new entry point.
    index.insert_f32(2, vec![2.0, 2.0], 0).unwrap();

    let (new_entry_id, _) = *index.entry_point.read();
    assert_eq!(new_entry_id, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_operations() {
    use std::sync::Arc;
    use tokio::sync::Barrier;

    let config = HnswConfig {
        dimension: 3,
        ..Default::default()
    };
    let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));
    let index = Arc::new(index);
    let barrier = Arc::new(Barrier::new(10));
    let mut handles: Vec<tokio::task::JoinHandle<Result<(), HnswError>>> = Vec::with_capacity(10);

    // Seed the index.
    for i in 0..20 {
        index
            .insert_f32(i, vec![i as f32, i as f32, i as f32], 0)
            .unwrap();
    }

    for t in 0..10 {
        let b = barrier.clone();
        let index_clone = Arc::clone(&index);
        // The same messages will be printed together.
        // You will NOT see any interleaving.
        handles.push(tokio::spawn(async move {
            b.wait().await;

            // Each task performs a different mix of operations.
            let base_id = 100 + t * 100;

            // Inserts.
            for i in 0..20 {
                let id = base_id + i;
                index_clone.insert_f32(id as u64, vec![id as f32, id as f32, id as f32], 0)?;
            }

            // Searches.
            for _ in 0..5 {
                let _ = index_clone.search_f32(&[t as f32, t as f32, t as f32], 5)?;
            }

            // Deletes.
            for i in 0..5 {
                let id = base_id + i;
                let _ = index_clone.remove(id as u64, 0);
            }
            Ok(())
        }));
    }

    for result in futures::future::try_join_all(handles).await.unwrap() {
        result.unwrap();
    }
    assert_eq!(index.len(), 170);
}

#[test]
fn test_load_metadata_and_ids_error_paths_and_entry_clamp() {
    match HnswIndex::load_metadata(&b"not cbor"[..]) {
        Err(HnswError::Serialization { .. }) => {}
        Err(other) => panic!("expected metadata serialization error, got {other:?}"),
        Ok(_) => panic!("expected metadata serialization error"),
    }

    let mut metadata = HnswMetadata {
        name: "load_metadata".to_string(),
        config: HnswConfig {
            dimension: 2,
            max_layers: 1,
            max_connections: 1,
            ef_construction: 0,
            ef_search: 0,
            scale_factor: Some(f64::NAN),
            ..Default::default()
        },
        stats: HnswStats {
            version: 9,
            search_count: 4,
            ..Default::default()
        },
    };
    let bytes = metadata_bytes(&metadata, (99, 9));
    assert!(
        HnswIndex::load_metadata(&bytes[..]).is_err(),
        "invalid persisted config must not be silently normalized"
    );
    metadata.config = test_config();
    let bytes = metadata_bytes(&metadata, (99, 9));
    let mut loaded = HnswIndex::load_metadata(&bytes[..]).unwrap();
    // Entry-point topology is repaired transactionally once nodes are available.
    assert_eq!(*loaded.entry_point.read(), (99, 9));
    assert_eq!(loaded.stats().search_count, 4);

    assert!(matches!(
        loaded.load_ids(&b"not cbor"[..]),
        Err(HnswError::Serialization { .. })
    ));

    let mut invalid_bitmap = Vec::new();
    cbor2::to_writer(&cbor2::Value::Bytes(vec![1, 2, 3]), &mut invalid_bitmap).unwrap();
    assert!(matches!(
        loaded.load_ids(&invalid_bitmap[..]),
        Err(HnswError::Generic { .. })
    ));
}

#[tokio::test]
async fn test_load_nodes_validation_and_loader_errors() {
    let mut empty = HnswIndex::new("empty_load".to_string(), Some(test_config()));
    empty.load_nodes(async |_| Ok(Some(vec![]))).await.unwrap();

    let mut generic = HnswIndex::new("generic_load".to_string(), Some(test_config()));
    generic.ids.write().add(1);
    let err = generic
        .load_nodes(async |_| Err::<Option<Vec<u8>>, _>("load failed".into()))
        .await
        .unwrap_err();
    assert!(matches!(err, HnswError::Generic { .. }));

    let mut bad_cbor = HnswIndex::new("bad_cbor_load".to_string(), Some(test_config()));
    bad_cbor.ids.write().add(1);
    let err = bad_cbor
        .load_nodes(async |_| Ok(Some(b"not a node".to_vec())))
        .await
        .unwrap_err();
    assert!(matches!(err, HnswError::Serialization { .. }));

    let cases = vec![
        {
            let mut node = valid_node(2);
            node.id = 99;
            node
        },
        {
            let mut node = valid_node(2);
            node.vector.pop();
            node
        },
        {
            let mut node = valid_node(2);
            node.layer = 3;
            node.neighbors = vec![
                SmallVec::new(),
                SmallVec::new(),
                SmallVec::new(),
                SmallVec::new(),
            ];
            node
        },
        {
            let mut node = valid_node(2);
            node.neighbors.clear();
            node
        },
        {
            let mut node = valid_node(2);
            node.vector[0] = bf16::from_f32(f32::NAN);
            node
        },
        {
            let mut node = valid_node(2);
            node.neighbors[0].push((1, bf16::from_f32(f32::INFINITY)));
            node
        },
    ];

    for node in cases {
        let mut index = HnswIndex::new("validation_load".to_string(), Some(test_config()));
        index.ids.write().add(2);
        let data = serialize_node(&node);
        let err = index
            .load_nodes(async |_| Ok(Some(data.clone())))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            HnswError::Generic { .. } | HnswError::DimensionMismatch { .. }
        ));
    }
}

#[tokio::test]
async fn test_load_nodes_repairs_missing_entry_point_without_missing_ids() {
    let mut index = HnswIndex::new("entry_repair_load".to_string(), Some(test_config()));
    index.ids.write().add(1);
    *index.entry_point.write() = (99, 0);
    let node = valid_node(1);
    let data = serialize_node(&node);

    index
        .load_nodes(async |_| Ok(Some(data.clone())))
        .await
        .unwrap();

    assert_eq!(*index.entry_point.read(), (1, 0));
    assert_eq!(index.len(), 1);
    assert!(index.stats().version > 1);
}

#[tokio::test]
async fn test_load_nodes_repairs_zero_entry_point_when_node_zero_absent() {
    // The entry point defaults to `(0, 0)`. If the persisted graph does not
    // contain node 0 (e.g. partial write / corruption), the dangling entry
    // must still be repaired — id 0 is a valid node, not an "unset" sentinel.
    let mut index = HnswIndex::new("zero_entry_repair".to_string(), Some(test_config()));
    index.ids.write().add(5);
    let node = valid_node(5);
    let data = serialize_node(&node);

    index
        .load_nodes(async |_| Ok(Some(data.clone())))
        .await
        .unwrap();

    assert_eq!(*index.entry_point.read(), (5, 0));
    assert_eq!(index.len(), 1);
    assert!(index.stats().version > 1);
    // Search must resolve via the repaired entry point instead of failing
    // with `NotFound { id: 0 }`.
    assert_eq!(index.search_f32(&[5.0, 5.5], 1).unwrap().len(), 1);
}

#[tokio::test]
async fn test_store_metadata_ids_dirty_nodes_and_flush_error_paths() {
    let index = HnswIndex::new("store_errors".to_string(), Some(test_config()));
    let err = index.store_metadata(FailingWriter, 1).unwrap_err();
    assert!(matches!(err, HnswError::Serialization { .. }));

    assert!(index.store_metadata(Vec::new(), 1).unwrap());
    assert!(!index.store_metadata(Vec::new(), 2).unwrap());
    assert!(matches!(
        index.store_ids(FailingWriter),
        Err(HnswError::Serialization { .. })
    ));

    let clean = HnswIndex::new("clean_flush".to_string(), Some(test_config()));
    assert!(
        clean
            .flush(Vec::new(), Vec::new(), 1, async |_, _| Ok(true))
            .await
            .unwrap()
    );
    assert!(
        !clean
            .flush(Vec::new(), Vec::new(), 2, async |_, _| Ok(true))
            .await
            .unwrap()
    );

    let dirty = HnswIndex::new("dirty_store".to_string(), Some(test_config()));
    dirty.insert_f32(1, vec![1.0, 1.0], 1).unwrap();
    let err = dirty
        .store_dirty_nodes(async |_, _| Err::<bool, _>("node write failed".into()))
        .await
        .unwrap_err();
    assert!(matches!(err, HnswError::Generic { .. }));
    assert!(dirty.has_dirty_nodes());

    let stop = HnswIndex::new("stop_store".to_string(), Some(test_config()));
    stop.insert_f32(1, vec![1.0, 1.0], 1).unwrap();
    stop.insert_f32(2, vec![2.0, 2.0], 1).unwrap();
    stop.store_dirty_nodes(async |_, _| Ok(false))
        .await
        .unwrap();
    assert!(stop.has_dirty_nodes());

    stop.store_dirty_nodes(async |_, _| Ok(true)).await.unwrap();
    assert!(!stop.has_dirty_nodes());

    let stale_dirty = HnswIndex::new("stale_dirty".to_string(), Some(test_config()));
    stale_dirty.dirty_nodes.write().insert(999);
    stale_dirty
        .store_dirty_nodes(async |_, _| panic!("missing dirty node must be skipped"))
        .await
        .unwrap();
    assert!(!stale_dirty.has_dirty_nodes());
}
