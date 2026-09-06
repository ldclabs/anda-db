//! # Anda-DB HNSW Vector Search Library
//!
//! Approximate nearest-neighbor (ANN) search over high-dimensional vectors based
//! on the Hierarchical Navigable Small World graph (Malkov & Yashunin, 2018).
//!
//! The [`HnswIndex`] type is the user-facing entry point. It owns:
//!
//! * a concurrent id → [`HnswNode`] map (for reads and in-place updates),
//! * a compact [`croaring::Treemap`] of live ids (fast cardinality / iteration),
//! * a dirty-node set and a saved-version watermark for incremental persistence.
//!
//! Vector elements use two-byte [`bf16`] storage. Distances use f32 kernels
//! with wider fallbacks at numeric boundaries; recall is measured separately.
//!
//! See the crate-level [`DistanceMetric`] and [`LayerGen`] for the math used in
//! the graph construction and query layers.

use croaring::{Portable, Treemap};
use half::bf16;
use ordered_float::OrderedFloat;
use papaya::HashMap as CoHashMap;
use parking_lot::{Mutex, RwLock};
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::{
    cmp::{self, Reverse},
    collections::{BTreeMap, BTreeSet, BinaryHeap, hash_map::Entry},
    future::Future,
    io::{Read, Write},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

mod persistence;
pub use persistence::{FlushOptions, FlushOutcome};
mod search;
pub use search::{SearchOptions, SearchWorkspace};

use crate::node::{GraphNode, PersistedNode};
pub use half;
use rand::SeedableRng;

use crate::{
    DistanceMetric, HnswConfig, HnswNode, LayerGen, SelectNeighborsStrategy,
    error::{BoxError, HnswError},
};

/// Concurrent, persistable HNSW index for approximate nearest-neighbor search.
///
/// Structural writes are serialized internally; queries may overlap with them.
/// Search briefly reads the entry-point RwLock and is not a transactional snapshot.
/// The caller must serialize all flush/store/purge calls. Bootstrap loaders require
/// exclusive ownership and publish a replacement graph only after validation.
///
/// Persistence separates metadata, an IDs bitmap, and generation-marked node blobs.
/// Generation markers detect partial progress; bootstrap recovers mixed images.
/// See [`Self::flush_with_options`] for callback, byte-budget and commit semantics.
pub struct HnswIndex {
    /// Human-readable name of the index; propagated into error variants.
    name: String,

    /// Frozen copy of the configuration used to build the graph.
    config: HnswConfig,

    /// Layer generator that assigns a layer to each new node.
    layer_gen: LayerGen,

    /// Serializes structural graph mutations.
    ///
    /// Search does not acquire this mutex, but insert/remove both clone
    /// and rewrite adjacency lists. Without this mutex, concurrent writers can
    /// overwrite each other's neighbor-list updates.
    structural_lock: Mutex<()>,

    /// Lock-free id → node map backing the graph.
    ///
    /// Uses [`papaya::HashMap`] for wait-free reads on the hot search path.
    /// Updates are performed with clone-then-`insert` (papaya has no in-place
    /// update API). The returned pin guard is `!Send` and must **not** be held
    /// across `.await` points.
    nodes: CoHashMap<u64, Arc<GraphNode>>,

    /// Exact incoming references, including asymmetric edges; writers hold structural_lock.
    incoming: Mutex<FxHashMap<(u64, u8), FxHashSet<u64>>>,

    /// Optional deterministic construction stream. Not part of the wire configuration.
    layer_rng: Mutex<Option<rand::rngs::StdRng>>,

    pending_ids: Option<Treemap>,
    full_saved_version: AtomicU64,

    /// Repairs performed at the last bootstrap.
    recovery: RecoveryReport,

    /// Current entry point for top-down search: `(node_id, layer)`.
    entry_point: RwLock<(u64, u8)>,

    /// Metadata (name, config, live stats) — cloned by [`Self::metadata`] /
    /// [`Self::stats`] for read-only snapshots.
    metadata: RwLock<HnswMetadata>,

    /// Ids that have been mutated since the last successful flush. Consumed
    /// by [`Self::store_dirty_nodes`].
    dirty_nodes: RwLock<BTreeSet<u64>>,

    /// Ids removed since the last successful purge. Consumed by
    /// [`Self::purge_removed_nodes`] so the caller can delete the
    /// corresponding persisted node blobs; without this, removed node files
    /// would accumulate forever. Also persisted alongside the metadata so a
    /// crash between a flush and the next purge re-queues the pending
    /// deletions on reload.
    removed_nodes: RwLock<BTreeMap<u64, Tombstone>>,

    /// Roaring-bitmap index of live node ids. Staged loader IDs are separate.
    ids: RwLock<Treemap>,

    /// Total number of queries served (exposed via `stats()`).
    search_count: AtomicU64,

    /// Highest metadata version already flushed to disk. Used to short-circuit
    /// no-op calls to [`Self::store_metadata`] and to make flushes idempotent
    /// when persistence calls are serialized by the caller.
    last_saved_version: AtomicU64,
}

/// Index metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HnswMetadata {
    /// Index name
    pub name: String,

    /// Index configuration.
    pub config: HnswConfig,

    /// Index statistics.
    pub stats: HnswStats,
}

/// Runtime statistics exported alongside the metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct HnswStats {
    /// Timestamp (unix ms) of the most recent `insert`.
    pub last_inserted: u64,

    /// Timestamp (unix ms) of the most recent `remove`.
    pub last_deleted: u64,

    /// Timestamp (unix ms) of the most recent successful metadata flush.
    pub last_saved: u64,

    /// Monotonic index version. Incremented on every structural change
    /// (insert or delete).
    pub version: u64,

    /// Current number of live nodes.
    pub num_elements: u64,

    /// Total search queries served since process start / load.
    pub search_count: u64,

    /// Total successful inserts since process start / load.
    pub insert_count: u64,

    /// Total successful deletes since process start / load.
    pub delete_count: u64,

    /// Highest layer currently present in the graph.
    pub max_layer: u8,
}

/// Serializable HNSW index structure (owned version).
#[derive(Clone, PartialEq, Serialize, Deserialize)]
struct HnswIndexOwned {
    pub entry_point: (u64, u8),
    pub metadata: HnswMetadata,
    /// Tombstones of removed nodes whose persisted blobs have not been purged
    /// yet. Persisted with the metadata so a crash between a flush and the
    /// next [`HnswIndex::purge_removed_nodes`] re-queues the deletions on
    /// reload instead of leaking the blobs forever. Missing in metadata
    /// written by older versions, hence the default.
    #[serde(default)]
    pub removed_nodes: Vec<u64>,
}

/// Serializable HNSW index structure (reference version).
#[derive(Clone, Serialize)]
struct HnswIndexRef<'a> {
    entry_point: (u64, u8),
    metadata: &'a HnswMetadata,
    removed_nodes: Vec<u64>,
}

impl HnswIndex {
    /// Maximum number of in-flight node loads used by [`Self::load_nodes`].
    pub const LOAD_NODES_CONCURRENCY: usize = 32;

    /// Maximum number of attempts a search makes when nodes on its path are
    /// being removed concurrently (each retry re-reads the repaired entry
    /// point).
    pub const SEARCH_MAX_ATTEMPTS: usize = 3;

    /// Pending removed-node tombstone count at which [`Self::remove`] starts
    /// warning (once per further multiple) that
    /// [`Self::purge_removed_nodes`] should be called.
    pub const REMOVED_NODES_WARN_THRESHOLD: usize = 10_000;

    /// Creates a new HNSW index.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the index
    /// * `config` - Optional HNSW configuration parameters
    ///
    /// # Returns
    ///
    /// * `HnswIndex` - New HNSW index instance
    pub fn new(name: String, config: Option<HnswConfig>) -> Self {
        let config = config.unwrap_or_default().normalized();
        Self::new_with_config(name, config)
    }

    /// Creates a new HNSW index after strictly validating the configuration.
    pub fn try_new(name: String, config: Option<HnswConfig>) -> Result<Self, HnswError> {
        let config = config.unwrap_or_default();
        config.validate(&name)?;
        Ok(Self::new_with_config(name, config))
    }

    fn new_with_config(name: String, config: HnswConfig) -> Self {
        let layer_gen = config.layer_gen();
        let stats = HnswStats {
            version: 1,
            ..Default::default()
        };
        Self {
            name: name.clone(),
            config: config.clone(),
            layer_gen,
            structural_lock: Mutex::new(()),
            nodes: CoHashMap::new(),
            incoming: Mutex::new(FxHashMap::default()),
            layer_rng: Mutex::new(None),
            pending_ids: None,
            full_saved_version: AtomicU64::new(0),
            recovery: RecoveryReport::default(),
            entry_point: RwLock::new((0, 0)),
            metadata: RwLock::new(HnswMetadata {
                name,
                config,
                stats,
            }),
            dirty_nodes: RwLock::new(BTreeSet::new()),
            removed_nodes: RwLock::new(BTreeMap::new()),
            ids: RwLock::new(Treemap::new()),
            search_count: AtomicU64::new(0),
            last_saved_version: AtomicU64::new(0),
        }
    }

    /// Returns the number of vectors in the index.
    ///
    /// # Returns
    ///
    /// * `usize` - Number of vectors
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Checks if the index is empty
    ///
    /// # Returns
    ///
    /// * `bool` - True if the index contains no vectors
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Returns the index name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the dimensionality of vectors in the index
    ///
    /// # Returns
    ///
    /// * `usize` - Vector dimension
    pub fn dimension(&self) -> usize {
        self.config.dimension
    }

    /// Returns the index metadata
    pub fn metadata(&self) -> HnswMetadata {
        let mut metadata = { self.metadata.read().clone() };
        self.refresh_live_stats(&mut metadata.stats);
        metadata
    }

    /// Gets current statistics about the index
    ///
    /// # Returns
    ///
    /// * `IndexStats` - Current statistics
    pub fn stats(&self) -> HnswStats {
        let mut stats = { self.metadata.read().stats.clone() };
        self.refresh_live_stats(&mut stats);
        stats
    }

    /// Overlays the live atomic/runtime counters onto a snapshot of the
    /// persisted statistics so callers always observe up-to-date values.
    fn refresh_live_stats(&self, stats: &mut HnswStats) {
        stats.num_elements = self.nodes.len() as u64;
        stats.search_count = self.search_count.load(Ordering::Relaxed);
    }

    /// Gets all node IDs in the index.
    pub fn node_ids(&self) -> Vec<u64> {
        self.ids.read().iter().collect()
    }

    /// Gets a node by ID and applies a function to it.
    pub fn get_node_with<R, F>(&self, id: u64, f: F) -> Result<R, HnswError>
    where
        F: FnOnce(&HnswNode) -> R,
    {
        self.nodes
            .pin()
            .get(&id)
            .map(|node| f(&node.to_public()))
            .ok_or_else(|| HnswError::NotFound {
                name: self.name.clone(),
                id,
            })
    }

    /// Borrows a node's stored vector and applies `f` without materializing a
    /// public node or cloning its vector and neighbor layers.
    pub fn get_vector_with<R, F>(&self, id: u64, f: F) -> Result<R, HnswError>
    where
        F: FnOnce(&[bf16]) -> R,
    {
        self.nodes
            .pin()
            .get(&id)
            .map(|node| f(node.vector.as_ref()))
            .ok_or_else(|| HnswError::NotFound {
                name: self.name.clone(),
                id,
            })
    }

    /// Inserts a vector.
    ///
    /// Complexity: O(log N) expected; the exact cost is dominated by
    /// `ef_construction` distance computations at each visited layer.
    ///
    /// Implementation outline:
    /// 1. Randomly draw the node's layer from the exponential distribution.
    /// 2. Descend from the top layer, using beam-1 search to pick a good
    ///    entry point for the target layer.
    /// 3. For every layer `≤` the node's layer, run layer-local ANN search
    ///    (beam width = `ef_construction`) and pick the best neighbors via
    ///    the configured [`SelectNeighborsStrategy`].
    /// 4. Publish the new node, queue reverse-edge updates on the selected
    ///    neighbors, then apply those updates in place — pruning any node
    ///    whose connection list exceeded `1.2 * max_connections`.
    ///
    /// # Errors
    /// * [`HnswError::DimensionMismatch`] if `vector.len() != config.dimension`.
    /// * [`HnswError::Generic`] for non-finite values or excessive vector magnitude.
    /// * [`HnswError::AlreadyExists`] if `id` is already present.
    pub fn insert(&self, id: u64, vector: Vec<bf16>, now_ms: u64) -> Result<(), HnswError> {
        if vector.len() != self.config.dimension {
            return Err(HnswError::DimensionMismatch {
                name: self.name.clone(),
                expected: self.config.dimension,
                got: vector.len(),
            });
        }

        self.config
            .distance_metric
            .validate_stored(&vector, &self.name)?;

        let _structural_guard = self.structural_lock.lock();
        let nodes = self.nodes.pin();
        // Check if ID already exists.
        if nodes.contains_key(&id) {
            return Err(HnswError::AlreadyExists {
                name: self.name.clone(),
                id,
            });
        }

        let (initial_entry_point_node, current_max_layer) = { *self.entry_point.read() };
        // Self-heal a stale entry point (e.g. left behind by interrupted
        // bootstrap or external state corruption). Without this, every insert
        // and search would keep failing with `NotFound`. Safe here because the
        // structural lock is held.
        let (initial_entry_point_node, current_max_layer) =
            if !nodes.is_empty() && !nodes.contains_key(&initial_entry_point_node) {
                self.repair_entry_point();
                *self.entry_point.read()
            } else {
                (initial_entry_point_node, current_max_layer)
            };
        // Randomly determine the node's layer
        let layer = match self.layer_rng.lock().as_mut() {
            Some(rng) => self.layer_gen.generate_with(current_max_layer, rng),
            None => self.layer_gen.generate(current_max_layer),
        };
        let mut node_neighbors: Vec<Vec<(u64, bf16)>> = (0..=layer)
            .map(|layer| Vec::with_capacity(self.config.layer_limit(layer) + 1))
            .collect();

        // If this is the first node, set it as the entry point
        if nodes.is_empty() {
            self.put_node(GraphNode::new(id, layer, vector, node_neighbors));
            self.ids.write().add(id);
            *self.entry_point.write() = (id, layer);
            self.dirty_nodes.write().insert(id); // Mark the node as dirty for persistence
            // A re-inserted id must not have its (new) blob purged by a
            // pending tombstone from an earlier remove().
            self.removed_nodes.write().remove(&id);

            self.update_metadata(|m| {
                m.stats.version += 1;
                m.stats.last_inserted = now_ms;
                m.stats.max_layer = layer;
                m.stats.insert_count += 1;
            });

            return Ok(());
        }

        // --- Phase 1: descend the layers to gather search state ---
        // The new vector is exactly representable in f32, so searching with the
        // f32 copy yields bit-identical distances while skipping the per-element
        // bf16 promotion of the query inside every distance computation.
        let vector_f32: Vec<f32> = vector.iter().map(|v| v.to_f32()).collect();
        let mut workspace = SearchWorkspace::default();
        let query = crate::distance::PreparedQuery::new(self.config.distance_metric, &vector_f32);
        let mut entry_point_node = initial_entry_point_node;
        let mut entry_point_layer = current_max_layer;
        let mut entry_point_dist = f32::MAX;

        // Search from top layer down to find the best entry point
        for current_layer_search in (layer + 1..=current_max_layer).rev() {
            let nearest = self.search_layer(
                &query,
                entry_point_node,
                entry_point_layer,
                current_layer_search,
                1, // Only need the closest one for entry point search
                &mut workspace,
            )?;
            if let Some(&(nearest_id, nearest_dist, nearest_layer)) = nearest.first()
                && nearest_dist < entry_point_dist
            {
                entry_point_node = nearest_id;
                entry_point_layer = nearest_layer;
                entry_point_dist = nearest_dist;
            }
        }

        // Inter-node distance cache shared across calls to `select_neighbors`.
        #[allow(clippy::type_complexity)]
        let mut multi_distance_cache: FxHashMap<(u64, u64), f32> = FxHashMap::default();

        // Pending reverse-edge updates: `neighbor_id -> [(layer, (new_id, dist))]`.
        //
        // Edges at layer L require both endpoints to exist at layer L.
        #[allow(clippy::type_complexity)]
        let mut neighbor_updates_required: FxHashMap<
            u64,
            SmallVec<[(u8, (u64, bf16)); 8]>,
        > = FxHashMap::default();

        // Build connections
        for current_layer_build in (0..=layer).rev() {
            let max_connections = self.config.layer_capacity(current_layer_build);

            let nearest = self.search_layer(
                &query,
                entry_point_node, // Use the best entry point found so far
                entry_point_layer,
                current_layer_build,
                self.config.ef_construction,
                &mut workspace,
            )?;

            let selected_neighbors = self.select_neighbors(
                nearest,
                max_connections,
                self.config.select_neighbors_strategy,
                &mut multi_distance_cache,
            )?;

            // Use the best candidate on this layer as the entry point for the next
            // iteration if it improves on the running minimum distance.
            if let Some(closest_in_layer) = selected_neighbors
                .iter()
                .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(cmp::Ordering::Equal))
                && closest_in_layer.1 < entry_point_dist
            {
                entry_point_node = closest_in_layer.0;
                entry_point_dist = closest_in_layer.1;
                // Keep the layer metadata in sync with the new entry node;
                // `search_layer` propagates it into its results, and the
                // reverse-edge guard below relies on it being accurate.
                entry_point_layer = closest_in_layer.2;
            }

            // Record forward edges on the new node and queue reverse edges.
            for (neighbor_id, dist, neighbor_layer) in selected_neighbors {
                if neighbor_id == id {
                    // Skip self-loops.
                    continue;
                }

                if neighbor_layer < current_layer_build {
                    // The candidate does not exist at this layer, so this
                    // layer's graph must not link to it. This happens whenever
                    // the new node raises the max layer: `search_layer` returns
                    // the entry point unexpanded at a layer it does not belong
                    // to and `select_neighbors` passes it through. Recording
                    // the forward edge anyway would leave a permanently
                    // asymmetric dead end — the reverse edge is (correctly)
                    // refused below, and the descent would then follow an edge
                    // to a node that is not on the layer it is descending.
                    continue;
                }

                let dist_bf16 = bf16::from_f32(dist);
                // (1) Forward edge on the new node.
                node_neighbors[current_layer_build as usize].push((neighbor_id, dist_bf16));

                // (2) Reverse edge on the existing node; guaranteed valid here
                //     because the target exists at this layer.
                neighbor_updates_required
                    .entry(neighbor_id)
                    .or_default()
                    .push((current_layer_build, (id, dist_bf16)));
            }
        }

        // --- Phase 2: publish the new node ---
        self.put_node(GraphNode::new(id, layer, vector, node_neighbors));
        self.ids.write().add(id);
        // A re-inserted id must not have its (new) blob purged by a pending
        // tombstone from an earlier remove().
        self.removed_nodes.write().remove(&id);

        let mut local_dirty_nodes = BTreeSet::new();
        local_dirty_nodes.insert(id);

        {
            // Promote the new node if it raises the graph's maximum layer.
            let mut entry_point_guard = self.entry_point.write();
            if layer > entry_point_guard.1 || !nodes.contains_key(&entry_point_guard.0) {
                *entry_point_guard = (id, layer);
            }
            // The guard is dropped here to avoid holding two locks at once.
        }

        self.update_metadata(|m| {
            m.stats.version += 1; // Increment index version
            m.stats.last_inserted = now_ms;
            if layer > m.stats.max_layer {
                m.stats.max_layer = layer;
            }
            m.stats.insert_count += 1;
        });

        // --- Phase 3: apply reverse edges + in-place pruning in a single pass ---
        //
        // Each affected neighbor is cloned exactly once: reverse-edge inserts and
        // (if needed) pruning via `select_neighbors` both mutate the local copy
        // before a single `nodes.insert` writes it back.
        for (neighbor_id, updates) in neighbor_updates_required {
            // Clone only adjacency; immutable vector storage remains shared.
            let mut neighbor_node = match nodes.get(&neighbor_id) {
                Some(n) => (**n).clone(),
                None => continue,
            };

            for (update_layer, connection) in updates {
                let Some(n_layer_list) = neighbor_node.neighbors.get_mut(update_layer as usize)
                else {
                    continue;
                };

                let max_conns = self.config.layer_capacity(update_layer);
                let should_truncate = self.config.layer_limit(update_layer);
                if let Some(edge) = n_layer_list.iter_mut().find(|edge| edge.0 == connection.0) {
                    *edge = connection;
                } else {
                    n_layer_list.push(connection);
                }
                if n_layer_list.len() > should_truncate {
                    // Prune in place: re-run the neighbor-selection strategy over
                    // the current connection list and keep only the best `max_conns`.
                    let candidates: Vec<(u64, f32, u8)> = n_layer_list
                        .iter()
                        .map(|&(cid, dist)| (cid, dist.to_f32(), 0)) // layer unused here
                        .collect();
                    if let Ok(selected) = self.select_neighbors(
                        candidates,
                        max_conns,
                        self.config.select_neighbors_strategy,
                        &mut multi_distance_cache,
                    ) {
                        n_layer_list.clear();
                        n_layer_list.extend(
                            selected
                                .into_iter()
                                .map(|(id, dist, _)| (id, bf16::from_f32(dist))),
                        );
                    }
                }
            }

            neighbor_node.version += 1;
            local_dirty_nodes.insert(neighbor_id);
            self.put_node(neighbor_node);
        }

        // --- Phase 4: commit the dirty set ---
        self.dirty_nodes.write().append(&mut local_dirty_nodes);

        Ok(())
    }

    /// Inserts a vector with f32 values into the index
    ///
    /// Automatically converts f32 values to bf16 for storage efficiency
    ///
    /// # Arguments
    ///
    /// * `id` - Unique identifier for the vector
    /// * `vector` - Vector data as f32 values
    /// * `now_ms` - Current timestamp in milliseconds
    ///
    /// # Returns
    ///
    /// * `Result<(), HnswError>` - Ok(()) if successful, or an error.
    pub fn insert_f32(&self, id: u64, vector: Vec<f32>, now_ms: u64) -> Result<(), HnswError> {
        if vector.len() != self.config.dimension {
            return Err(HnswError::DimensionMismatch {
                name: self.name.clone(),
                expected: self.config.dimension,
                got: vector.len(),
            });
        }
        self.insert(id, vector.into_iter().map(bf16::from_f32).collect(), now_ms)
    }

    /// Removes a node and prunes the reverse edges that point to it,
    /// optionally re-linking its former neighbors to each other.
    ///
    /// This method only mutates the in-memory graph. The id is recorded as a
    /// tombstone; call [`Self::purge_removed_nodes`] after flushing so the
    /// persistence layer deletes the corresponding on-disk node blob,
    /// otherwise removed node files accumulate forever.
    ///
    /// Exact reverse references include asymmetric edges created by pruning,
    /// so a removed ID can safely be reused for a different vector. The work is
    /// proportional to its incoming references and affected adjacency lists.
    /// Only deleting the entry point needs an O(N) replacement scan.
    ///
    /// With `reconnect_on_delete`, affected neighbors also consider the deleted
    /// node's peers. This can improve recall under churn, at the cost of extra
    /// distance work under the structural lock; it does not guarantee graph
    /// connectivity for arbitrary data and deletion sequences.
    ///
    /// # Returns
    /// * `true` if a node with `id` existed and was removed.
    /// * `false` otherwise.
    pub fn remove(&self, id: u64, now_ms: u64) -> bool {
        let _structural_guard = self.structural_lock.lock();
        let nodes = self.nodes.pin();
        let Some(node) = nodes.get(&id).cloned() else {
            return false;
        };

        let entry_was_removed = self.entry_point.read().0 == id;
        let replacement_entry = if entry_was_removed {
            // O(N) scan for the live node on the highest layer. Entry-point
            // deletions are rare, and the scan avoids maintaining a mirror
            // per-layer tracker that must stay synchronized with `nodes`.
            nodes
                .iter()
                .filter(|(node_id, _)| **node_id != id)
                .max_by_key(|(_, node)| (node.layer, Reverse(node.id)))
                .map(|(_, node)| (node.id, node.layer))
        } else {
            None
        };
        if entry_was_removed {
            *self.entry_point.write() = replacement_entry.unwrap_or((0, 0));
        }

        nodes.remove(&id);

        self.ids.write().remove(id);
        // A dirty mark for a node that no longer exists is pointless; record
        // the tombstone instead so `purge_removed_nodes` can delete the
        // persisted blob.
        self.dirty_nodes.write().remove(&id);
        let pending_tombstones = {
            let mut removed_nodes = self.removed_nodes.write();
            removed_nodes.insert(id, Tombstone::new(false));
            removed_nodes.len()
        };
        // Tombstones are only drained by `purge_removed_nodes`; remind the
        // caller periodically so the set (and the persisted metadata that
        // snapshots it) cannot grow without bound.
        if pending_tombstones >= Self::REMOVED_NODES_WARN_THRESHOLD
            && pending_tombstones.is_multiple_of(Self::REMOVED_NODES_WARN_THRESHOLD)
        {
            log::warn!(
                action = "remove",
                index = self.name.as_str(),
                pending_tombstones = pending_tombstones;
                "HnswIndex '{}': {} removed-node tombstones are pending purge; \
                 call purge_removed_nodes (then flush) to delete the persisted \
                 blobs and stop the tombstone set from growing unboundedly",
                self.name,
                pending_tombstones,
            );
        }
        let recalculated_max_layer = if entry_was_removed {
            Some(replacement_entry.map_or(0, |(_, layer)| layer))
        } else {
            None
        };
        self.update_metadata(|m| {
            m.stats.version += 1;
            m.stats.last_deleted = now_ms;
            m.stats.delete_count += 1;
            if let Some(max_layer) = recalculated_max_layer {
                m.stats.max_layer = max_layer;
            }
        });

        // Track incoming references independently of outgoing edges. Pruning
        // makes the graph asymmetric, so walking node.neighbors is insufficient.
        let neighbor_ids = {
            let mut incoming = self.incoming.lock();
            let mut affected = FxHashSet::default();
            for layer in 0..=node.layer {
                if let Some(sources) = incoming.remove(&(id, layer)) {
                    affected.extend(sources);
                }
                for &(target, _) in &node.neighbors[layer as usize] {
                    let key = (target, layer);
                    if let Some(sources) = incoming.get_mut(&key) {
                        sources.remove(&id);
                        if sources.is_empty() {
                            incoming.remove(&key);
                        }
                    }
                }
            }
            affected
        };

        // Distance cache shared by the re-link candidates and `select_neighbors`.
        let mut pair_distance_cache: FxHashMap<(u64, u64), f32> = FxHashMap::default();
        let mut dirty_nodes = BTreeSet::new();
        for &neighbor_id in &neighbor_ids {
            if let Some(n) = nodes.get(&neighbor_id) {
                let mut updated = false;
                let mut o = (**n).clone();
                for layer in 0..=(n.layer as usize) {
                    let Some(pos) = n.neighbors[layer].iter().position(|&(idx, _)| idx == id)
                    else {
                        continue;
                    };
                    o.neighbors[layer].swap_remove(pos);
                    o.neighbors[layer].retain(|&(target, _)| target != id);
                    updated = true;

                    // Fast path: with reconnect_on_delete disabled, deletion
                    // only prunes the reverse edge (see the config docs for
                    // the recall trade-off).
                    if !self.config.reconnect_on_delete {
                        continue;
                    }

                    // Re-link: merge the deleted node's other neighbors at
                    // this layer into this node's candidate set and re-select
                    // the best available edges.
                    let Some(peers) = node.neighbors.get(layer) else {
                        continue;
                    };
                    let current_list = &o.neighbors[layer];
                    let mut candidate_ids: FxHashSet<u64> =
                        current_list.iter().map(|&(cid, _)| cid).collect();
                    let mut candidates: Vec<(u64, f32, u8)> = current_list
                        .iter()
                        .map(|&(cid, dist)| (cid, dist.to_f32(), 0)) // layer unused here
                        .collect();
                    let existing_len = candidates.len();
                    for &(peer, _) in peers {
                        if peer == neighbor_id || peer == id || !candidate_ids.insert(peer) {
                            continue;
                        }
                        let Some(peer_node) = nodes.get(&peer) else {
                            continue;
                        };
                        if (peer_node.layer as usize) < layer {
                            // The peer does not exist at this layer.
                            continue;
                        }
                        let cache_key = if neighbor_id < peer {
                            (neighbor_id, peer)
                        } else {
                            (peer, neighbor_id)
                        };
                        let dist = match pair_distance_cache.entry(cache_key) {
                            Entry::Occupied(entry) => *entry.get(),
                            Entry::Vacant(entry) => {
                                match self
                                    .config
                                    .distance_metric
                                    .compute(&n.vector, &peer_node.vector)
                                {
                                    Ok(dist) => {
                                        entry.insert(dist);
                                        dist
                                    }
                                    // Defensive: vectors are validated on
                                    // insert/load, so this is unreachable.
                                    Err(_) => continue,
                                }
                            }
                        };
                        candidates.push((peer, dist, 0));
                    }

                    if candidates.len() > existing_len {
                        let max_conns = self.config.layer_capacity(layer as u8);
                        if let Ok(selected) = self.select_neighbors(
                            candidates,
                            max_conns,
                            self.config.select_neighbors_strategy,
                            &mut pair_distance_cache,
                        ) {
                            let layer_list = &mut o.neighbors[layer];
                            layer_list.clear();
                            layer_list.extend(
                                selected
                                    .into_iter()
                                    .map(|(cid, dist, _)| (cid, bf16::from_f32(dist))),
                            );
                        }
                    }
                }
                if updated {
                    o.version += 1;
                    dirty_nodes.insert(neighbor_id);
                    self.put_node(o);
                }
            }
        }

        if !dirty_nodes.is_empty() {
            self.dirty_nodes.write().extend(dirty_nodes);
        }

        true
    }

    /// Selects the best neighbors for a node based on the configured strategy
    ///
    /// # Arguments
    ///
    /// * `candidates` - List of candidate nodes with their distances
    /// * `m` - Maximum number of neighbors to select
    /// * `strategy` - Strategy to use for selection (Simple or Heuristic)
    /// * `distance_cache` - Cache of previously computed distances between nodes
    ///
    /// # Returns
    ///
    /// * `Result<Vec<(u64, f32, u8)>, HnswError>` - Selected neighbors with their distances
    fn select_neighbors(
        &self,
        mut candidates: Vec<(u64, f32, u8)>,
        m: usize,
        strategy: SelectNeighborsStrategy,
        distance_cache: &mut FxHashMap<(u64, u64), f32>,
    ) -> Result<Vec<(u64, f32, u8)>, HnswError> {
        if m == 0 {
            return Ok(Vec::new());
        }
        let nodes = self.nodes.pin();
        let mut seen = FxHashSet::default();
        candidates.retain(|(id, distance, _)| {
            distance.is_finite() && nodes.contains_key(id) && seen.insert(*id)
        });
        if candidates.len() <= m {
            return Ok(candidates);
        }

        match strategy {
            SelectNeighborsStrategy::Simple => {
                // Simple strategy: select m closest neighbors
                let mut selected = candidates;
                selected
                    .sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(cmp::Ordering::Equal));
                selected.truncate(m);
                Ok(selected)
            }
            SelectNeighborsStrategy::Heuristic => {
                // Algorithm 4 from the HNSW paper: scan candidates from nearest
                // to farthest and keep one only if it is closer to the query
                // point than to every neighbor selected so far. This favors
                // edges that span different directions ("diversity") over
                // tightly clustered ones, and needs at most `c * m` pairwise
                // distances with an early exit on the first conflict.
                let mut remaining = candidates;
                remaining
                    .sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(cmp::Ordering::Equal));

                let mut selected: Vec<(u64, f32, u8)> = Vec::with_capacity(m);
                // Candidates pruned by the diversity rule, kept in ascending
                // distance order as backfill (`keepPrunedConnections`) so the
                // node still ends up with exactly `m` edges.
                let mut discarded: Vec<(u64, f32, u8)> = Vec::new();

                for candidate in remaining {
                    if selected.len() >= m {
                        break;
                    }

                    let (cand_id, cand_dist, _) = candidate;
                    if !nodes.contains_key(&cand_id) {
                        // The node is gone (removed concurrently or left over
                        // as a stale neighbor id). It can never be a useful
                        // edge, so drop it instead of letting it occupy — or
                        // backfill — a slot ahead of live candidates.
                        continue;
                    }

                    let mut keep = true;
                    for &(sel_id, _, _) in &selected {
                        let cache_key = if cand_id < sel_id {
                            (cand_id, sel_id)
                        } else {
                            (sel_id, cand_id)
                        };

                        let dist = match distance_cache.entry(cache_key) {
                            Entry::Occupied(entry) => *entry.get(),
                            Entry::Vacant(entry) => {
                                if let (Some(cand_node), Some(sel_node)) =
                                    (nodes.get(&cand_id), nodes.get(&sel_id))
                                {
                                    let dist = self
                                        .config
                                        .distance_metric
                                        .stored(&cand_node.vector, &sel_node.vector)?;
                                    entry.insert(dist);
                                    dist
                                } else {
                                    // The candidate was checked above, so only
                                    // a concurrently removed `sel_id` reaches
                                    // here (defensive): skip this pair and keep
                                    // testing the candidate against the rest.
                                    continue;
                                }
                            }
                        };

                        if dist < cand_dist {
                            keep = false;
                            break;
                        }
                    }

                    if keep {
                        selected.push(candidate);
                    } else {
                        discarded.push(candidate);
                    }
                }

                // Backfill with the closest pruned candidates.
                let mut discarded = discarded.into_iter();
                while selected.len() < m {
                    match discarded.next() {
                        Some(candidate) => selected.push(candidate),
                        None => break,
                    }
                }

                Ok(selected)
            }
        }
    }

    /// Repairs the entry point by selecting the live node with the highest layer.
    ///
    fn repair_entry_point(&self) -> u8 {
        let nodes = self.nodes.pin();
        let max_layer = if let Some((_, node)) = nodes
            .iter()
            .max_by_key(|(_, node)| (node.layer, Reverse(node.id)))
        {
            *self.entry_point.write() = (node.id, node.layer);
            node.layer
        } else {
            *self.entry_point.write() = (0, 0);
            0
        };

        if log::log_enabled!(log::Level::Debug) {
            let entry_point = self.entry_point.read();
            log::debug!(
                "Updated entry point to {} at layer {}",
                entry_point.0,
                entry_point.1
            );
        }

        max_layer
    }

    /// Updates the index metadata
    ///
    /// # Arguments
    ///
    /// * `f` - Function that modifies the metadata
    fn update_metadata<F>(&self, f: F)
    where
        F: FnOnce(&mut HnswMetadata),
    {
        let mut metadata = self.metadata.write();
        f(&mut metadata);
    }
}

#[cfg(test)]
mod tests;

/// Bootstrap repairs, separate from persisted operation counters.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Referenced node blobs that were absent.
    pub missing_nodes: usize,
    /// Loaded adjacency lists or entry-point metadata repaired.
    pub repaired_nodes: usize,
    /// A mixed-generation image was rebuilt from its vectors.
    pub rebuilt: bool,
}

#[derive(Clone)]
struct Tombstone {
    token: Arc<()>,
    committed: bool,
}
impl Tombstone {
    fn new(committed: bool) -> Self {
        Self {
            token: Arc::new(()),
            committed,
        }
    }
}

impl HnswIndex {
    /// Strict constructor with repeatable graph layer assignment.
    pub fn try_new_seeded(
        name: String,
        config: Option<HnswConfig>,
        seed: u64,
    ) -> Result<Self, HnswError> {
        let mut index = Self::try_new(name, config)?;
        *index.layer_rng.get_mut() = Some(rand::rngs::StdRng::seed_from_u64(seed));
        Ok(index)
    }

    /// Reports repairs performed by the latest successful node load.
    pub fn recovery_report(&self) -> &RecoveryReport {
        &self.recovery
    }

    /// Publishes one immutable node and maintains the exact reverse references.
    /// The caller holds structural_lock (or exclusive bootstrap ownership).
    fn put_node(&self, mut node: GraphNode) {
        for (layer, neighbors) in node.neighbors.iter_mut().enumerate() {
            neighbors.sort_unstable_by_key(|edge| edge.0);
            neighbors.dedup_by_key(|edge| edge.0);
            neighbors.shrink_to(self.config.layer_limit(layer as u8) + 1);
        }
        let nodes = self.nodes.pin();
        let previous = nodes.get(&node.id);
        let mut incoming = self.incoming.lock();
        for layer in 0..node
            .neighbors
            .len()
            .max(previous.map_or(0, |n| n.neighbors.len()))
        {
            let old = previous
                .and_then(|n| n.neighbors.get(layer))
                .map_or(&[][..], |n| n.as_slice());
            let new = node.neighbors.get(layer).map_or(&[][..], |n| n.as_slice());
            let mut old = old.iter().peekable();
            let mut new = new.iter().peekable();
            loop {
                match (old.peek(), new.peek()) {
                    (Some(a), Some(b)) if a.0 == b.0 => {
                        old.next();
                        new.next();
                    }
                    (Some(a), b) if b.is_none_or(|b| a.0 < b.0) => {
                        let key = (a.0, layer as u8);
                        if let Some(sources) = incoming.get_mut(&key) {
                            sources.remove(&node.id);
                            if sources.is_empty() {
                                incoming.remove(&key);
                            }
                        }
                        old.next();
                    }
                    (_, Some(b)) => {
                        incoming
                            .entry((b.0, layer as u8))
                            .or_default()
                            .insert(node.id);
                        new.next();
                    }
                    _ => break,
                }
            }
        }
        nodes.insert(node.id, Arc::new(node));
    }

    fn rebuild_incoming(&self) {
        let mut incoming = self.incoming.lock();
        incoming.clear();
        for (id, node) in self.nodes.pin().iter() {
            for (layer, neighbors) in node.neighbors.iter().enumerate() {
                for &(target, _) in neighbors {
                    incoming
                        .entry((target, layer as u8))
                        .or_default()
                        .insert(*id);
                }
            }
        }
    }
}
