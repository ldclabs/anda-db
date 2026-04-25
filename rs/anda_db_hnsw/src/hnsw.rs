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
//! Vectors are stored in [`bf16`] to cut memory by ~50% with negligible impact
//! on ANN recall. Distance computation is performed in `f32` internally.
//!
//! See the crate-level [`DistanceMetric`] and [`LayerGen`] for the math used in
//! the graph construction and query layers.

use croaring::{Portable, Treemap};
use half::bf16;
use ordered_float::OrderedFloat;
use papaya::HashMap as CoHashMap;
use parking_lot::RwLock;
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::{
    borrow::Cow,
    cmp::{self, Reverse},
    collections::{BTreeSet, BinaryHeap},
    io::{Read, Write},
    sync::atomic::{AtomicU64, Ordering},
};

pub use half;

use crate::{
    DistanceMetric, LayerGen,
    error::{BoxError, HnswError},
};

/// Concurrent, persistable HNSW index for approximate nearest-neighbor search.
///
/// `HnswIndex` is thread-safe: `insert`, `remove`, `search` and the `store_*`
/// methods may be called from multiple tasks / threads simultaneously. The
/// only `&mut self` methods are the bootstrap loaders ([`Self::load_metadata`],
/// [`Self::load_ids`], [`Self::load_nodes`]).
///
/// Persistence is split into three artifacts so a writer can fsync each one
/// independently:
///
/// * **metadata** — a small CBOR blob with the [`HnswMetadata`] and the current
///   entry point; versioned via [`HnswStats::version`].
/// * **ids** — a Roaring bitmap (`croaring::Treemap`) of live node ids.
/// * **nodes** — per-id CBOR blobs emitted via [`Self::store_dirty_nodes`].
pub struct HnswIndex {
    /// Human-readable name of the index; propagated into error variants.
    name: String,

    /// Frozen copy of the configuration used to build the graph.
    config: HnswConfig,

    /// Layer generator that assigns a layer to each new node.
    layer_gen: LayerGen,

    /// Lock-free id → node map backing the graph.
    ///
    /// Uses [`papaya::HashMap`] for wait-free reads on the hot search path.
    /// Updates are performed with clone-then-`insert` (papaya has no in-place
    /// update API). The returned pin guard is `!Send` and must **not** be held
    /// across `.await` points.
    nodes: CoHashMap<u64, HnswNode>,

    /// Current entry point for top-down search: `(node_id, layer)`.
    entry_point: RwLock<(u64, u8)>,

    /// Metadata (name, config, live stats) — cloned by [`Self::metadata`] /
    /// [`Self::stats`] for read-only snapshots.
    metadata: RwLock<HnswMetadata>,

    /// Ids that have been mutated since the last successful flush. Consumed
    /// by [`Self::store_dirty_nodes`].
    dirty_nodes: RwLock<BTreeSet<u64>>,

    /// Roaring-bitmap index of live node ids. Kept in sync with `nodes`.
    ids: RwLock<Treemap>,

    /// Total number of queries served (exposed via `stats()`).
    search_count: AtomicU64,

    /// Highest metadata version already flushed to disk. Used to short-circuit
    /// no-op calls to [`Self::store_metadata`] and to make flushes idempotent
    /// under concurrent writers.
    last_saved_version: AtomicU64,
}

/// Tunable HNSW parameters. Defaults are suitable for 384–768-dim sentence
/// embeddings; see the crate-level docs for guidance on tuning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HnswConfig {
    /// Required vector dimensionality. Every `insert` / `search` validates this.
    pub dimension: usize,

    /// Maximum number of layers. Default `16`. Layer ids are `u8`, so the hard
    /// ceiling is 255.
    pub max_layers: u8,

    /// Maximum connections per node (M). Layer 0 uses `2 * M` by convention.
    /// Default `32`.
    pub max_connections: u8,

    /// Candidate-list breadth during construction (`efConstruction`).
    /// Higher = better graph quality, slower inserts. Default `200`.
    pub ef_construction: usize,

    /// Candidate-list breadth during search (`efSearch`). Must be ≥ `top_k`;
    /// [`HnswIndex::search`] enforces this at query time. Default `50`.
    pub ef_search: usize,

    /// Distance metric used for similarity. Default [`DistanceMetric::Euclidean`].
    pub distance_metric: DistanceMetric,

    /// Scale factor applied to the exponential layer distribution. `> 1.0`
    /// makes upper layers denser; `< 1.0` makes them sparser. Default `1.0`.
    pub scale_factor: Option<f64>,

    /// Neighbor selection strategy. Default [`SelectNeighborsStrategy::Heuristic`].
    pub select_neighbors_strategy: SelectNeighborsStrategy,
}

impl HnswConfig {
    /// Creates a layer generator based on the configuration.
    ///
    /// # Returns
    ///
    /// * `LayerGen` - A layer generator with the configured parameters.
    pub fn layer_gen(&self) -> LayerGen {
        LayerGen::new_with_scale(
            self.max_connections,
            self.scale_factor.unwrap_or(1.0),
            self.max_layers,
        )
    }
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            dimension: 512,
            max_layers: 16,
            max_connections: 32,
            ef_construction: 200,
            ef_search: 50,
            distance_metric: DistanceMetric::Euclidean,
            scale_factor: None,
            select_neighbors_strategy: SelectNeighborsStrategy::Heuristic,
        }
    }
}

/// Neighbor selection strategies used both during graph construction and when
/// pruning over-connected nodes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SelectNeighborsStrategy {
    /// Greedy top-k by distance. Fastest to build; lower recall on hard data.
    Simple,

    /// Algorithm 4 from the HNSW paper (approximate diverse selection).
    /// Slower construction, better recall, especially on clustered data.
    Heuristic,
}

/// One node of the HNSW graph.
///
/// A node records its highest layer, its stored vector and, for every layer
/// from 0 up to [`HnswNode::layer`], the list of outgoing edges `(id, dist)`.
/// Distances are cached in `bf16` purely to shrink the persisted form; all
/// computation is in `f32`.
///
/// Serde field renames keep the on-disk CBOR compact.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct HnswNode {
    /// Unique node identifier, assigned by the caller.
    #[serde(rename = "i")]
    pub id: u64,

    /// Highest layer index at which this node is present.
    #[serde(rename = "l")]
    pub layer: u8,

    /// Stored vector in `bf16` format.
    #[serde(rename = "vec")]
    pub vector: Vec<bf16>,

    /// Adjacency lists indexed by layer (`neighbors[l]` = edges at layer `l`).
    #[serde(rename = "n")]
    pub neighbors: Vec<SmallVec<[(u64, bf16); 64]>>,

    /// Monotonically increasing write counter. Incremented on every mutation
    /// so that persistence layers can implement last-writer-wins / conflict
    /// detection.
    #[serde(rename = "v")]
    pub version: u64,
}

/// Serializes a node to CBOR. Used by [`HnswIndex::store_dirty_nodes`] and by
/// external tools that snapshot individual nodes.
pub fn serialize_node(node: &HnswNode) -> Vec<u8> {
    let mut buf = Vec::new();
    ciborium::into_writer(node, &mut buf).expect("Failed to serialize node");
    buf
}

/// Index metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HnswMetadata {
    /// Index name
    pub name: String,

    /// Index configuration.
    pub config: HnswConfig,

    /// Index statistics.
    pub stats: HnswStats,
}

/// Runtime statistics exported alongside the metadata.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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
#[derive(Clone, Serialize, Deserialize)]
struct HnswIndexOwned {
    pub entry_point: (u64, u8),
    pub metadata: HnswMetadata,
}

/// Serializable HNSW index structure (reference version).
#[derive(Clone, Serialize)]
struct HnswIndexRef<'a> {
    entry_point: (u64, u8),
    metadata: &'a HnswMetadata,
}

impl HnswIndex {
    /// Maximum number of in-flight node loads used by [`Self::load_nodes`].
    pub const LOAD_NODES_CONCURRENCY: usize = 32;

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
        let config = config.unwrap_or_default();
        let layer_gen = config.layer_gen();
        let stats = HnswStats {
            version: 1,
            ..Default::default()
        };
        Self {
            name: name.clone(),
            config: config.clone(),
            layer_gen,
            nodes: CoHashMap::new(),
            entry_point: RwLock::new((0, 0)),
            metadata: RwLock::new(HnswMetadata {
                name,
                config,
                stats,
            }),
            dirty_nodes: RwLock::new(BTreeSet::new()),
            ids: RwLock::new(Treemap::new()),
            search_count: AtomicU64::new(0),
            last_saved_version: AtomicU64::new(0),
        }
    }

    /// Loads an index from metadata reader, ids reader and a closure for loading nodes.
    ///
    /// # Arguments
    ///
    /// * `metadata` - Metadata reader
    /// * `ids` - IDs reader
    /// * `f` - Closure for loading nodes
    ///
    /// # Returns
    ///
    /// * `Result<Self, HnswError>` - Loaded index or error.
    pub async fn load_all<R: Read, F>(metadata: R, ids: R, f: F) -> Result<Self, HnswError>
    where
        F: AsyncFn(u64) -> Result<Option<Vec<u8>>, BoxError>,
    {
        let mut index = Self::load_metadata(metadata)?;
        index.load_ids(ids)?;
        index.load_nodes(f).await?;
        Ok(index)
    }

    /// Loads an index from a sync [`Read`].
    ///
    /// Deserializes the index from CBOR format.
    ///
    /// # Arguments
    ///
    /// * `r` - Any type implementing the [`Read`] trait
    ///
    /// # Returns
    ///
    /// * `Result<Self, HnswError>` - Loaded index or error.
    pub fn load_metadata<R: Read>(r: R) -> Result<Self, HnswError> {
        let index: HnswIndexOwned =
            ciborium::from_reader(r).map_err(|err| HnswError::Serialization {
                name: "unknown".to_string(),
                source: err.into(),
            })?;
        let layer_gen = index.metadata.config.layer_gen();
        let search_count = AtomicU64::new(index.metadata.stats.search_count);
        let last_saved_version = AtomicU64::new(index.metadata.stats.version);

        Ok(HnswIndex {
            name: index.metadata.name.clone(),
            config: index.metadata.config.clone(),
            layer_gen,
            nodes: CoHashMap::new(),
            entry_point: RwLock::new(index.entry_point),
            metadata: RwLock::new(index.metadata),
            dirty_nodes: RwLock::new(BTreeSet::new()),
            ids: RwLock::new(Treemap::new()),
            search_count,
            last_saved_version,
        })
    }

    /// Loads IDs from a sync [`Read`].
    ///
    /// Deserializes the IDs from CBOR format.
    ///
    /// # Arguments
    ///
    /// * `r` - Any type implementing the [`Read`] trait
    ///
    /// # Returns
    ///
    /// * `Result<(), HnswError>` - Ok(()) if successful, or an error.
    pub fn load_ids<R: Read>(&mut self, r: R) -> Result<(), HnswError> {
        let ids: Vec<u8> = ciborium::from_reader(r).map_err(|err| HnswError::Serialization {
            name: "unknown".to_string(),
            source: err.into(),
        })?;
        let treemap =
            Treemap::try_deserialize::<Portable>(&ids).ok_or_else(|| HnswError::Generic {
                name: self.name.clone(),
                source: "Failed to deserialize ids".into(),
            })?;
        *self.ids.write() = treemap;
        Ok(())
    }

    /// Loads node payloads via the provided async loader.
    ///
    /// Loader invocations and CBOR deserialization run concurrently (up to
    /// [`Self::LOAD_NODES_CONCURRENCY`] in flight) to hide storage latency on cold
    /// start. Results are applied to `self.nodes` as they complete.
    ///
    /// This method is only used to bootstrap the index from persistent storage.
    ///
    /// # Arguments
    ///
    /// * `f` - Async function that loads the raw node bytes for a given id.
    ///   It must be callable concurrently (`AsyncFn`), which is trivially satisfied
    ///   by closures that only read captured `Arc`/`Clone` resources.
    ///
    /// # Returns
    ///
    /// * `Result<(), HnswError>` - Ok(()) if successful, or an error.
    pub async fn load_nodes<F>(&mut self, f: F) -> Result<(), HnswError>
    where
        F: AsyncFn(u64) -> Result<Option<Vec<u8>>, BoxError>,
    {
        use futures::stream::{self, StreamExt, TryStreamExt};

        let ids: Vec<u64> = self.ids.read().iter().collect();
        if ids.is_empty() {
            return Ok(());
        }

        let name = &self.name;
        let f_ref = &f;
        let mut stream = stream::iter(ids.into_iter())
            .map(|id| async move {
                match f_ref(id).await {
                    Ok(Some(data)) => {
                        let node: HnswNode = ciborium::from_reader(&data[..]).map_err(|err| {
                            HnswError::Serialization {
                                name: name.clone(),
                                source: err.into(),
                            }
                        })?;
                        Ok::<_, HnswError>(Some((id, node)))
                    }
                    Ok(None) => Ok(None),
                    Err(err) => Err(HnswError::Generic {
                        name: name.clone(),
                        source: err,
                    }),
                }
            })
            .buffer_unordered(Self::LOAD_NODES_CONCURRENCY);

        let nodes = &self.nodes;
        while let Some(item) = stream.try_next().await? {
            if let Some((id, node)) = item {
                // Re-acquire the pin guard per item: papaya's LocalGuard contains
                // raw pointers and is !Send, so it must not be held across .await.
                nodes.pin().insert(id, node);
            }
        }
        Ok(())
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
        metadata.stats.num_elements = self.nodes.len() as u64;
        metadata.stats.search_count = self.search_count.load(Ordering::Relaxed);

        metadata
    }

    /// Gets current statistics about the index
    ///
    /// # Returns
    ///
    /// * `IndexStats` - Current statistics
    pub fn stats(&self) -> HnswStats {
        let mut stats = { self.metadata.read().stats.clone() };
        stats.num_elements = self.nodes.len() as u64;
        stats.search_count = self.search_count.load(Ordering::Relaxed);

        stats
    }

    /// Gets all node IDs in the index.
    pub fn node_ids(&self) -> Vec<u64> {
        self.ids.read().iter().collect()
    }

    /// Gets a node by ID and applies a function to it.
    pub fn get_node_with<R, F>(&self, id: u64, f: F) -> Result<R, HnswError>
    where
        F: FnMut(&HnswNode) -> R,
    {
        self.nodes
            .pin()
            .get(&id)
            .map(f)
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
    /// * [`HnswError::Generic`] if the vector contains `NaN` / `±∞`.
    /// * [`HnswError::AlreadyExists`] if `id` is already present.
    pub fn insert(&self, id: u64, vector: Vec<bf16>, now_ms: u64) -> Result<(), HnswError> {
        if vector.len() != self.config.dimension {
            return Err(HnswError::DimensionMismatch {
                name: self.name.clone(),
                expected: self.config.dimension,
                got: vector.len(),
            });
        }

        if vector.iter().any(|v| !v.is_finite()) {
            return Err(HnswError::Generic {
                name: self.name.clone(),
                source: "Vector contains invalid values (NaN or infinity)".into(),
            });
        }

        let nodes = self.nodes.pin();
        // Check if ID already exists.
        if nodes.contains_key(&id) {
            return Err(HnswError::AlreadyExists {
                name: self.name.clone(),
                id,
            });
        }

        let (initial_entry_point_node, current_max_layer) = { *self.entry_point.read() };
        // Randomly determine the node's layer
        let layer = self.layer_gen.generate(current_max_layer);
        let mut node_neighbors: Vec<SmallVec<[(u64, bf16); 64]>> =
            vec![
                SmallVec::with_capacity(self.config.max_connections as usize * 2);
                layer as usize + 1
            ];

        // If this is the first node, set it as the entry point
        if nodes.is_empty() {
            nodes.insert(
                id,
                HnswNode {
                    id,
                    layer,
                    vector,
                    neighbors: node_neighbors,
                    version: 1,
                },
            );
            self.ids.write().add(id);
            *self.entry_point.write() = (id, layer);
            self.dirty_nodes.write().insert(id); // Mark the node as dirty for persistence

            self.update_metadata(|m| {
                m.stats.version = 1;
                m.stats.last_inserted = now_ms;
                m.stats.max_layer = layer;
                m.stats.insert_count += 1;
            });

            return Ok(());
        }

        // --- Phase 1: descend the layers to gather search state ---
        let mut distance_cache = FxHashMap::default();
        let mut entry_point_node = initial_entry_point_node;
        let mut entry_point_layer = current_max_layer;
        let mut entry_point_dist = f32::MAX;

        // Search from top layer down to find the best entry point
        for current_layer_search in (current_max_layer.min(layer + 1)..=current_max_layer).rev() {
            let nearest = self.search_layer(
                &vector,
                entry_point_node,
                entry_point_layer,
                current_layer_search,
                1, // Only need the closest one for entry point search
                &mut distance_cache,
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
        // HNSW only adds reverse edges at layers where both endpoints exist
        // (a lower-layer node may appear in a higher-layer node's neighbor list,
        // but not vice versa).
        #[allow(clippy::type_complexity)]
        let mut neighbor_updates_required: FxHashMap<
            u64,
            SmallVec<[(u8, (u64, bf16)); 8]>,
        > = FxHashMap::default();

        // Build connections
        for current_layer_build in (0..=layer).rev() {
            let max_connections = if current_layer_build > 0 {
                self.config.max_connections as usize
            } else {
                // Layer 0 typically has double connections
                self.config.max_connections as usize * 2
            };

            let nearest = self.search_layer(
                &vector,
                entry_point_node, // Use the best entry point found so far
                entry_point_layer,
                current_layer_build,
                self.config.ef_construction,
                &mut distance_cache,
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
            }

            // Record forward edges on the new node and queue reverse edges.
            for (neighbor_id, dist, layer) in selected_neighbors {
                if neighbor_id == id {
                    // Skip self-loops.
                    continue;
                }

                let dist_bf16 = bf16::from_f32(dist);
                // (1) Forward edge on the new node.
                node_neighbors[current_layer_build as usize].push((neighbor_id, dist_bf16));

                // (2) Reverse edge on the existing node, only if the target node
                //     actually exists at this layer.
                if layer >= current_layer_build {
                    neighbor_updates_required
                        .entry(neighbor_id)
                        .or_default()
                        .push((current_layer_build, (id, dist_bf16)));
                }
            }
        }

        // --- Phase 2: publish the new node ---
        let new_node = HnswNode {
            id,
            layer,
            vector,
            neighbors: node_neighbors,
            version: 1,
        };

        nodes.insert(id, new_node);
        self.ids.write().add(id);

        let mut local_dirty_nodes = BTreeSet::new();
        local_dirty_nodes.insert(id);

        {
            // Promote the new node to entry point if it lives on a higher layer,
            // or if the previous entry point disappeared concurrently.
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
            // Snapshot the neighbor. `papaya` has no in-place update API, so we
            // clone-then-insert. If the node was concurrently removed, skip it.
            let mut neighbor_node = match nodes.get(&neighbor_id) {
                Some(n) => n.clone(),
                None => continue,
            };

            for (update_layer, connection) in updates {
                let Some(n_layer_list) = neighbor_node.neighbors.get_mut(update_layer as usize)
                else {
                    continue;
                };

                let max_conns = if update_layer > 0 {
                    self.config.max_connections as usize
                } else {
                    // Layer 0 uses 2×M connections (standard HNSW convention).
                    self.config.max_connections as usize * 2
                };
                // Allow 20% slack before paying for a prune, to amortize the
                // cost of `select_neighbors` over multiple inserts.
                let should_truncate = max_conns + max_conns / 5;

                n_layer_list.push(connection);
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
            nodes.insert(neighbor_id, neighbor_node);
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
        self.insert(id, vector.into_iter().map(bf16::from_f32).collect(), now_ms)
    }

    /// Removes a node and prunes the reverse edges that point to it.
    ///
    /// This method only mutates the in-memory graph. The corresponding on-disk
    /// node blob (if any) must be deleted by the caller — see the `flush`
    /// callback documentation.
    ///
    /// The implementation walks the deleted node's own neighbor list rather
    /// than scanning the whole map, reducing cost from O(N) to O(M*L).
    /// Stale back-references from nodes that were not in the deleted node's
    /// neighbor list (e.g. after a prior prune) are harmless: they are skipped
    /// at search time when `nodes.get()` returns `None`.
    ///
    /// # Returns
    /// * `true` if a node with `id` existed and was removed.
    /// * `false` otherwise.
    pub fn remove(&self, id: u64, now_ms: u64) -> bool {
        let nodes = self.nodes.pin();
        let Some(node) = nodes.remove(&id) else {
            return false;
        };

        self.ids.write().remove(id);
        self.try_update_entry_point(node);
        self.update_metadata(|m| {
            m.stats.version += 1;
            m.stats.last_deleted = now_ms;
            m.stats.delete_count += 1;
        });

        // Only iterate the deleted node's known neighbors instead of scanning ALL nodes.
        // This reduces complexity from O(N) to O(K*L) where K=max_connections, L=max_layers.
        // Note: nodes that reference the deleted node but are NOT in the deleted node's
        // neighbor list (due to pruning) will retain stale references. These stale references
        // are harmlessly skipped during search (nodes.get() returns None).
        let mut neighbor_ids: FxHashSet<u64> = FxHashSet::with_capacity_and_hasher(
            node.neighbors.iter().map(|l| l.len()).sum(),
            FxBuildHasher,
        );
        for layer_neighbors in &node.neighbors {
            for &(nid, _) in layer_neighbors {
                neighbor_ids.insert(nid);
            }
        }

        let mut dirty_nodes = BTreeSet::new();
        for &neighbor_id in &neighbor_ids {
            if let Some(n) = nodes.get(&neighbor_id) {
                let mut updated = false;
                let mut o = Cow::Borrowed(n);
                for layer in 0..=(n.layer as usize) {
                    if let Some(pos) = n.neighbors[layer].iter().position(|&(idx, _)| idx == id) {
                        o.to_mut().neighbors[layer].swap_remove(pos);
                        updated = true;
                    }
                }
                if updated {
                    o.to_mut().version += 1;
                    dirty_nodes.insert(neighbor_id);
                    nodes.insert(neighbor_id, o.into_owned());
                }
            }
        }

        if !dirty_nodes.is_empty() {
            self.dirty_nodes.write().extend(dirty_nodes);
        }

        true
    }

    /// Returns the `top_k` nearest neighbors to `query`, sorted by ascending
    /// distance.
    ///
    /// Standard two-phase HNSW search:
    /// 1. Greedy descent from the top layer down to layer 1 with beam width 1
    ///    to refine the entry point.
    /// 2. Layer-0 beam search with width `max(ef_search, top_k)`, then truncate
    ///    to `top_k`.
    ///
    /// # Errors
    /// * [`HnswError::DimensionMismatch`] on dimension mismatch.
    /// * [`HnswError::NotFound`] if the current entry point has been removed
    ///   concurrently and has not yet been repaired by a subsequent mutation.
    pub fn search(&self, query: &[bf16], top_k: usize) -> Result<Vec<(u64, f32)>, HnswError> {
        if query.len() != self.config.dimension {
            return Err(HnswError::DimensionMismatch {
                name: self.name.clone(),
                expected: self.config.dimension,
                got: query.len(),
            });
        }

        if self.nodes.is_empty() {
            return Ok(vec![]);
        }

        let mut distance_cache = FxHashMap::default();
        let mut current_dist = f32::MAX;
        let (mut current_node, mut current_node_layer) = { *self.entry_point.read() };
        // Greedy descent from the top layer to refine the entry point.
        for current_layer in (1..=current_node_layer).rev() {
            let nearest = self.search_layer(
                query,
                current_node,
                current_node_layer,
                current_layer,
                1,
                &mut distance_cache,
            )?;
            if let Some(node) = nearest.first()
                && node.1 < current_dist
            {
                current_dist = node.1;
                current_node = node.0;
                current_node_layer = node.2;
            }
        }

        // Layer 0 is fully searched with the user-requested breadth.
        let ef = self.config.ef_search.max(top_k);
        let mut results = self.search_layer(
            query,
            current_node,
            current_node_layer,
            0,
            ef,
            &mut distance_cache,
        )?;
        results.truncate(top_k);

        self.search_count.fetch_add(1, Ordering::Relaxed);

        Ok(results
            .into_iter()
            .map(|(id, dist, _)| (id, dist))
            .collect())
    }

    /// Searches for nearest neighbors using f32 query vector
    ///
    /// Automatically converts f32 values to bf16 for distance calculations
    ///
    /// # Arguments
    ///
    /// * `query` - Query vector as f32 values
    /// * `top_k` - Number of nearest neighbors to return
    ///
    /// # Returns
    ///
    /// * `Result<Vec<(u64, f32)>, HnswError>` - Vector of (id, distance) pairs sorted by ascending distance
    pub fn search_f32(&self, query: &[f32], top_k: usize) -> Result<Vec<(u64, f32)>, HnswError> {
        self.search(
            &query.iter().map(|v| bf16::from_f32(*v)).collect::<Vec<_>>(),
            top_k,
        )
    }

    /// Searches for nearest neighbors within a specific layer
    ///
    /// This is an internal method used by both insert and search operations
    /// to find nearest neighbors at a specific layer of the graph.
    ///
    /// # Arguments
    ///
    /// * `query` - Query vector
    /// * `entry_point` - Starting node ID for the search
    /// * `entry_point_layer` - Layer of the entry point node
    /// * `layer` - Layer to search in
    /// * `ef` - Expansion factor (number of candidates to consider)
    /// * `distance_cache` - Cache of previously computed distances
    ///
    /// # Returns
    ///
    /// * `Result<Vec<(u64, f32, u8)>, HnswError>` - Vector of (id, distance, node layer) pairs sorted by ascending distance
    fn search_layer(
        &self,
        query: &[bf16],
        entry_point: u64,
        entry_point_layer: u8,
        layer: u8,
        ef: usize,
        distance_cache: &mut FxHashMap<u64, f32>,
    ) -> Result<Vec<(u64, f32, u8)>, HnswError> {
        let mut visited: FxHashSet<u64> =
            FxHashSet::with_capacity_and_hasher(ef * 2, FxBuildHasher);
        let mut candidates: BinaryHeap<(Reverse<OrderedFloat<f32>>, u64, u8)> =
            BinaryHeap::with_capacity(ef * 2);
        let mut results: BinaryHeap<(OrderedFloat<f32>, u64, u8)> =
            BinaryHeap::with_capacity(ef * 2);

        let nodes = self.nodes.pin();
        // Calculate distance to entry point
        let entry_dist = match nodes.get(&entry_point) {
            Some(node) => self.get_distance_with_cache(distance_cache, query, node)?,
            None => {
                return Err(HnswError::NotFound {
                    name: self.name.clone(),
                    id: entry_point,
                });
            }
        };

        // Initialize candidate list
        visited.insert(entry_point);
        candidates.push((
            Reverse(OrderedFloat(entry_dist)),
            entry_point,
            entry_point_layer,
        ));
        results.push((OrderedFloat(entry_dist), entry_point, entry_point_layer));

        // Get nearest candidates
        while let Some((Reverse(OrderedFloat(dist)), point, _)) = candidates.pop() {
            if let Some((OrderedFloat(max_dist), _, _)) = results.peek()
                && &dist > max_dist
                && results.len() >= ef
            {
                break;
            };

            // Check neighbors of current node
            if let Some(node) = nodes.get(&point)
                && let Some(neighbors) = node.neighbors.get(layer as usize)
            {
                for &(neighbor, _) in neighbors {
                    if !visited.contains(&neighbor) {
                        visited.insert(neighbor);
                        if let Some(neighbor_node) = nodes.get(&neighbor) {
                            match self.get_distance_with_cache(distance_cache, query, neighbor_node)
                            {
                                Ok(dist) => {
                                    // results always has ≥1 element (the entry point),
                                    // so peek() always returns Some here.
                                    if let Some((OrderedFloat(max_dist), _, _)) = results.peek()
                                        && (&dist < max_dist || results.len() < ef)
                                    {
                                        candidates.push((
                                            Reverse(OrderedFloat(dist)),
                                            neighbor,
                                            neighbor_node.layer,
                                        ));
                                        results.push((
                                            OrderedFloat(dist),
                                            neighbor,
                                            neighbor_node.layer,
                                        ));

                                        // Prune distant results
                                        if results.len() > ef {
                                            results.pop();
                                        }
                                    }
                                }
                                Err(e) => {
                                    log::warn!("Distance calculation error: {e:?}");
                                    distance_cache.insert(neighbor, f32::MAX);
                                }
                            };
                        }
                    }
                }
            }
        }

        Ok(results
            .into_sorted_vec()
            .into_iter()
            .map(|(d, id, l)| (id, d.0, l))
            .collect())
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
        candidates: Vec<(u64, f32, u8)>,
        m: usize,
        strategy: SelectNeighborsStrategy,
        distance_cache: &mut FxHashMap<(u64, u64), f32>,
    ) -> Result<Vec<(u64, f32, u8)>, HnswError> {
        if candidates.len() <= m {
            return Ok(candidates);
        }

        let nodes = self.nodes.pin();
        match strategy {
            SelectNeighborsStrategy::Simple => {
                // Simple strategy: select m closest neighbors
                let mut selected = candidates;
                selected.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(cmp::Ordering::Equal));
                selected.truncate(m);
                Ok(selected)
            }
            SelectNeighborsStrategy::Heuristic => {
                // Heuristic strategy: balance distance and connection diversity
                // Create candidate and result sets
                let mut selected: Vec<(u64, f32, u8)> = Vec::with_capacity(m);
                let mut remaining = candidates;
                remaining.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(cmp::Ordering::Equal));

                // Add the first nearest neighbor
                if !remaining.is_empty() {
                    selected.push(remaining.remove(0));
                }

                // Greedily add remaining nodes while considering diversity
                while selected.len() < m && !remaining.is_empty() {
                    let mut best_candidate_idx = 0;
                    let mut best_distance_improvement = f32::MIN;

                    for (i, &(cand_id, cand_dist, _)) in remaining.iter().enumerate() {
                        let mut min_dist_to_selected = f32::MAX;
                        for &(sel_id, _, _) in &selected {
                            let cache_key = if cand_id < sel_id {
                                (cand_id, sel_id)
                            } else {
                                (sel_id, cand_id)
                            };

                            let dist = if let Some(&cached_dist) = distance_cache.get(&cache_key) {
                                cached_dist
                            } else if let (Some(cand_node), Some(sel_node)) =
                                (nodes.get(&cand_id), nodes.get(&sel_id))
                            {
                                let new_dist = self
                                    .config
                                    .distance_metric
                                    .compute(&cand_node.vector, &sel_node.vector)?;
                                distance_cache.insert(cache_key, new_dist);
                                new_dist
                            } else {
                                continue;
                            };

                            min_dist_to_selected = min_dist_to_selected.min(dist);
                        }

                        // Balance: proximity to the query vs. diversity w.r.t.
                        // the already-selected set.
                        let improvement = min_dist_to_selected - cand_dist;
                        if improvement > best_distance_improvement {
                            best_distance_improvement = improvement;
                            best_candidate_idx = i;
                        }
                    }

                    // Commit the best candidate (bounds-checked to avoid a
                    // pathological infinite loop on degenerate inputs).
                    if best_candidate_idx < remaining.len() {
                        selected.push(remaining.swap_remove(best_candidate_idx));
                    } else if !remaining.is_empty() {
                        // Fall back to the simple strategy.
                        selected.push(remaining.remove(0));
                    } else {
                        break;
                    }
                }

                Ok(selected)
            }
        }
    }

    // TODO: use improved version
    #[allow(dead_code)]
    fn select_neighbors_heuristic(
        &self,
        candidates: Vec<(u64, f32, u8)>,
        m: usize,
        distance_cache: &mut FxHashMap<(u64, u64), f32>,
    ) -> Result<Vec<(u64, f32, u8)>, HnswError> {
        if candidates.len() <= m {
            return Ok(candidates);
        }

        let nodes = self.nodes.pin();
        let mut selected: Vec<(u64, f32, u8)> = Vec::with_capacity(m);
        let mut remaining = candidates;
        remaining.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(cmp::Ordering::Equal));

        // Always seed with the single closest candidate.
        if !remaining.is_empty() {
            selected.push(remaining.remove(0));
        }

        // Refined diversity metric: average distance to the selected set.
        while selected.len() < m && !remaining.is_empty() {
            let mut best_idx = 0;
            let mut best_score = f32::MIN;

            for (i, &(cand_id, cand_dist, _)) in remaining.iter().enumerate() {
                let mut diversity_score = 0.0;
                let mut valid_comparisons = 0;

                for &(sel_id, _, _) in &selected {
                    let cache_key = if cand_id < sel_id {
                        (cand_id, sel_id)
                    } else {
                        (sel_id, cand_id)
                    };

                    if let Some(&cached_dist) = distance_cache.get(&cache_key) {
                        diversity_score += cached_dist;
                        valid_comparisons += 1;
                    } else if let (Some(cand_node), Some(sel_node)) =
                        (nodes.get(&cand_id), nodes.get(&sel_id))
                    {
                        let dist = self
                            .config
                            .distance_metric
                            .compute(&cand_node.vector, &sel_node.vector)?;
                        distance_cache.insert(cache_key, dist);
                        diversity_score += dist;
                        valid_comparisons += 1;
                    }
                }

                if valid_comparisons > 0 {
                    diversity_score /= valid_comparisons as f32;
                    // Combine diversity (higher is better) with proximity to the
                    // query (lower `cand_dist` is better); λ = 0.5.
                    let combined_score = diversity_score - cand_dist * 0.5;

                    if combined_score > best_score {
                        best_score = combined_score;
                        best_idx = i;
                    }
                }
            }

            if best_idx < remaining.len() {
                selected.push(remaining.swap_remove(best_idx));
            } else {
                break;
            }
        }

        Ok(selected)
    }

    /// Gets the distance between a query vector and a node, using cache when available
    ///
    /// # Arguments
    ///
    /// * `cache` - Cache of previously computed distances
    /// * `query` - Query vector
    /// * `neighbor` - Node to compute distance to
    ///
    /// # Returns
    ///
    /// * `Result<f32, HnswError>` - Computed distance
    fn get_distance_with_cache(
        &self,
        cache: &mut FxHashMap<u64, f32>,
        query: &[bf16],
        neighbor: &HnswNode,
    ) -> Result<f32, HnswError> {
        match cache.get(&neighbor.id) {
            Some(&dist) => Ok(dist),
            None => {
                let dist = self
                    .config
                    .distance_metric
                    .compute(query, &neighbor.vector)?;
                cache.insert(neighbor.id, dist);
                Ok(dist)
            }
        }
    }

    /// Persists metadata, ids and dirty nodes in one coordinated pass.
    ///
    /// The sequence is:
    /// 1. [`Self::store_metadata`] — a no-op if the version hasn't advanced.
    /// 2. If either the metadata actually changed **or** dirty nodes are pending,
    ///    call [`Self::store_ids`] and [`Self::store_dirty_nodes`].
    ///
    /// `f` receives `(id, &cbor_bytes)` and returns:
    /// * `Ok(true)` — continue with the next dirty node.
    /// * `Ok(false)` — stop; unprocessed ids are placed back on the dirty set.
    /// * `Err(_)` — stop with an error; the failing id is also requeued.
    ///
    /// Returns `true` iff any work was actually committed.
    pub async fn flush<W: Write, F>(
        &self,
        metadata: W,
        ids: W,
        now_ms: u64,
        f: F,
    ) -> Result<bool, HnswError>
    where
        F: AsyncFnMut(u64, &[u8]) -> Result<bool, BoxError>,
    {
        let meta_saved = self.store_metadata(metadata, now_ms)?;
        let had_dirty = self.has_dirty_nodes();
        if !meta_saved && !had_dirty {
            return Ok(false);
        }

        self.store_ids(ids)?;
        self.store_dirty_nodes(f).await?;
        Ok(meta_saved || had_dirty)
    }

    /// Returns whether there are dirty nodes pending persistence.
    pub fn has_dirty_nodes(&self) -> bool {
        !self.dirty_nodes.read().is_empty()
    }

    /// Stores the index metadata to a writer in CBOR format.
    ///
    /// # Arguments
    ///
    /// * `w` - Any type implementing the [`Write`] trait
    /// * `now_ms` - Current timestamp in milliseconds.
    ///
    /// # Returns
    ///
    /// * `Result<bool, HnswError>` - true if the metadata was saved, false if the version was not updated
    pub fn store_metadata<W: Write>(&self, w: W, now_ms: u64) -> Result<bool, HnswError> {
        // Fast path: if the version is already saved, avoid cloning metadata.
        let current_version = { self.metadata.read().stats.version };
        if self.last_saved_version.load(Ordering::Relaxed) >= current_version {
            return Ok(false);
        }

        let mut meta = self.metadata();
        // Atomically claim the right to serialize this version.
        // Only one concurrent caller will see prev < meta.stats.version and proceed.
        let prev_saved_version = self
            .last_saved_version
            .fetch_max(meta.stats.version, Ordering::Relaxed);
        if prev_saved_version >= meta.stats.version {
            // No need to save if the version is not updated
            return Ok(false);
        }

        meta.stats.last_saved = now_ms.max(meta.stats.last_saved);
        if let Err(err) = ciborium::into_writer(
            &HnswIndexRef {
                entry_point: *self.entry_point.read(),
                metadata: &meta,
            },
            w,
        ) {
            // Serialization failed: try to revert only if no other writer has already
            // advanced this atomic to a newer version.
            let _ = self.last_saved_version.compare_exchange(
                meta.stats.version,
                prev_saved_version,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
            return Err(HnswError::Serialization {
                name: self.name.clone(),
                source: err.into(),
            });
        }

        self.update_metadata(|m| {
            m.stats.last_saved = meta.stats.last_saved.max(m.stats.last_saved);
        });

        Ok(true)
    }

    /// Stores the index ids to a writer in CBOR format.
    ///
    /// # Arguments
    ///
    /// * `w` - Any type implementing the [`Write`] trait
    ///
    /// # Returns
    ///
    /// * `Result<(), HnswError>` - Success or error.
    pub fn store_ids<W: Write>(&self, w: W) -> Result<(), HnswError> {
        let data = {
            let mut ids = self.ids.read().clone();
            ids.run_optimize();
            ids.serialize::<Portable>()
        };

        ciborium::into_writer(&ciborium::Value::Bytes(data), w).map_err(|err| {
            HnswError::Serialization {
                name: self.name.clone(),
                source: err.into(),
            }
        })
    }

    /// Stores dirty nodes to persistent storage using the provided async function
    ///
    /// This method iterates through dirty nodes.
    ///
    /// # Arguments
    ///
    /// * `f` - Async function that writes a node data to persistent storage
    ///   The function takes a node ID and serialized data, and returns whether to continue
    ///
    /// # Returns
    ///
    /// * `Result<(), HnswError>` - Success or error.
    pub async fn store_dirty_nodes<F>(&self, mut f: F) -> Result<(), HnswError>
    where
        F: AsyncFnMut(u64, &[u8]) -> Result<bool, BoxError>,
    {
        let mut dirty_nodes = {
            // move the dirty nodes into a temporary variable
            // and release the lock
            let mut guard = self.dirty_nodes.write();
            std::mem::take(&mut *guard)
        };

        let mut buf = Vec::with_capacity(4096);
        while let Some(id) = dirty_nodes.pop_first() {
            // Hold the `papaya` pin guard only while serializing; it is `!Send`
            // and must be dropped before the `.await` below.
            let has_node = {
                let nodes = self.nodes.pin();
                if let Some(node) = nodes.get(&id) {
                    buf.clear();
                    ciborium::into_writer(&node, &mut buf).map_err(|err| {
                        HnswError::Serialization {
                            name: self.name.clone(),
                            source: err.into(),
                        }
                    })?;
                    true
                } else {
                    false
                }
            };

            if has_node {
                match f(id, &buf).await {
                    Ok(true) => {
                        // continue
                    }
                    Ok(false) => {
                        // stop and refund the unprocessed dirty nodes
                        self.dirty_nodes.write().append(&mut dirty_nodes);
                        return Ok(());
                    }
                    Err(err) => {
                        // refund the unprocessed dirty nodes
                        dirty_nodes.insert(id);
                        self.dirty_nodes.write().append(&mut dirty_nodes);
                        return Err(HnswError::Generic {
                            name: self.name.clone(),
                            source: err,
                        });
                    }
                }
            }
        }

        Ok(())
    }

    /// Updates the entry point after a node deletion
    ///
    /// # Arguments
    ///
    /// * `deleted_node` - The node that was deleted
    ///
    /// # Returns
    ///
    /// * `Result<(), HnswError>` - Success or error
    fn try_update_entry_point(&self, deleted_node: &HnswNode) {
        let (_, mut max_layer) = {
            let point = self.entry_point.read();
            if point.0 != deleted_node.id {
                return;
            }
            *point
        };

        let nodes = self.nodes.pin();
        loop {
            if let Some(neighbors) = deleted_node.neighbors.get(max_layer as usize) {
                for &(neighbor, _) in neighbors {
                    if let Some(neighbor_node) = nodes.get(&neighbor) {
                        *self.entry_point.write() = (neighbor, neighbor_node.layer);
                        return;
                    }
                }
            }

            if max_layer == 0 {
                break;
            }
            max_layer -= 1;
        }

        if let Some((_, node)) = nodes.iter().next() {
            *self.entry_point.write() = (node.id, node.layer);
        } else {
            *self.entry_point.write() = (0, 0);
        }

        if log::log_enabled!(log::Level::Debug) {
            let entry_point = self.entry_point.read();
            log::debug!(
                "Updated entry point to {} at layer {}",
                entry_point.0,
                entry_point.1
            );
        }
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
mod tests {
    use super::*;
    use std::collections::HashMap;

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
        let node: HnswNode = ciborium::from_reader(&data[..]).unwrap();
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
        let mut handles: Vec<tokio::task::JoinHandle<Result<(), HnswError>>> =
            Vec::with_capacity(10);

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

        futures::future::try_join_all(handles).await.unwrap();
    }
}
