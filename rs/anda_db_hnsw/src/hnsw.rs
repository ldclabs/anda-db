//! # Anda-DB HNSW Vector Search Library

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
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    io::{Read, Write},
    sync::atomic::{AtomicU64, Ordering},
};

pub use half;

use crate::{
    DistanceMetric, LayerGen,
    error::{BoxError, HnswError},
};

/// HNSW index for approximate nearest neighbor search.
pub struct HnswIndex {
    /// Index name
    name: String,

    /// Index configuration.
    config: HnswConfig,

    /// Layer generator for assigning layers to new nodes.
    layer_gen: LayerGen,

    /// Map of node IDs to nodes.
    nodes: CoHashMap<u64, HnswNode>,

    /// Entry point for search (node_id, layer)
    entry_point: RwLock<(u64, u8)>,

    /// Index metadata.
    metadata: RwLock<HnswMetadata>,

    dirty_nodes: RwLock<BTreeSet<u64>>,

    ids: RwLock<Treemap>,

    /// Number of search operations performed.
    search_count: AtomicU64,

    /// Last saved version of the index
    last_saved_version: AtomicU64,
}

/// HNSW configuration parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HnswConfig {
    /// Dimensionality of vectors in the index.
    pub dimension: usize,

    /// Maximum number of layers in the graph. Default is 16.
    pub max_layers: u8,

    /// Maximum number of connections per node in each layer. Default is 32.
    pub max_connections: u8,

    /// Expansion factor during index construction. Default is 200.
    pub ef_construction: usize,

    /// Number of candidates to consider during search. Default is 50.
    pub ef_search: usize,

    /// Distance metric to use for similarity calculations. Default is Euclidean.
    pub distance_metric: DistanceMetric,

    /// Scale factor for adjusting layer distribution. Default is 1.0.
    pub scale_factor: Option<f64>,

    /// Strategy for selecting neighbors. Default is Heuristic.
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

/// Neighbor selection strategies.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SelectNeighborsStrategy {
    /// Simple greedy strategy that selects the closest nodes.
    Simple,

    /// Heuristic neighbor selection algorithm (NN-descent) that considers diversity.
    /// It will consume more time to build the index, but will improve search performance.
    Heuristic,
}

/// HNSW graph node.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct HnswNode {
    /// Unique identifier for the node.
    #[serde(rename = "i")]
    pub id: u64,

    /// The highest layer this node appears in.
    #[serde(rename = "l")]
    pub layer: u8,

    /// Vector data stored in bf16 format.
    #[serde(rename = "vec")]
    pub vector: Vec<bf16>,

    /// Neighbors at each layer (layer -> [(id, distance)]).
    #[serde(rename = "n")]
    pub neighbors: Vec<SmallVec<[(u64, bf16); 64]>>,

    /// Updated version for the node. It will be incremented when the node is updated.
    #[serde(rename = "v")]
    pub version: u64,
}

/// Serializes a node to binary in CBOR format.
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

/// Index statistics.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HnswStats {
    /// Last insertion timestamp (unix ms).
    pub last_inserted: u64,

    /// Last deletion timestamp (unix ms).
    pub last_deleted: u64,

    /// Last saved timestamp (unix ms).
    pub last_saved: u64,

    /// Updated version for the index. It will be incremented when the index is updated.
    pub version: u64,

    /// Number of nodes in the index.
    pub num_elements: u64,

    /// Number of search operations performed.
    pub search_count: u64,

    /// Number of insert operations performed.
    pub insert_count: u64,

    /// Number of delete operations performed.
    pub delete_count: u64,

    /// Maximum layer in the index.
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
        F: AsyncFnMut(u64) -> Result<Option<Vec<u8>>, BoxError>,
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

    /// Sets the node if it is not already present or if the version is newer.
    /// This method is only used to bootstrap the index from persistent storage.
    ///
    /// # Arguments
    ///
    /// * `f` - Function to load the node data
    ///
    /// # Returns
    ///
    /// * `Result<(), HnswError>` - Ok(()) if successful, or an error.
    pub async fn load_nodes<F>(&mut self, mut f: F) -> Result<(), HnswError>
    where
        F: AsyncFnMut(u64) -> Result<Option<Vec<u8>>, BoxError>,
    {
        // TODO: concurrent loading
        let ids = self.ids.read().clone();
        for id in ids.iter() {
            let data = f(id).await.map_err(|err| HnswError::Generic {
                name: self.name.clone(),
                source: err,
            })?;
            if let Some(data) = data {
                let node: HnswNode =
                    ciborium::from_reader(&data[..]).map_err(|err| HnswError::Serialization {
                        name: self.name.clone(),
                        source: err.into(),
                    })?;

                self.nodes.pin().insert(id, node);
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

    /// Inserts a vector into the index
    ///
    /// This is the core insertion method that implements the HNSW algorithm:
    /// 1. Randomly assigns a layer to the new node
    /// 2. Finds the nearest neighbors in each layer
    /// 3. Creates connections to selected neighbors
    /// 4. Updates the entry point if necessary
    ///
    /// # Arguments
    ///
    /// * `id` - Unique identifier for the vector
    /// * `vector` - Vector data as bf16 values
    /// * `now_ms` - Current timestamp in milliseconds
    ///
    /// # Returns
    ///
    /// * `Result<(), HnswError>` - Ok(()) if successful, or an error.
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
                    neighbors: node_neighbors, // 使用收集好的邻居列表
                    version: 1,                // Initial version
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

        // --- 阶段一：搜索与信息收集 ---
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

        #[allow(clippy::type_complexity)]
        let mut multi_distance_cache: FxHashMap<(u64, u64), f32> = FxHashMap::default(); // For select_neighbors heuristic
        // 存储邻居节点需要添加的反向连接信息: neighbor_id -> [(layer, (new_node_id, dist))]
        #[allow(clippy::type_complexity)]
        let mut neighbor_updates_required: BTreeMap<u64, Vec<(u8, (u64, bf16))>> = BTreeMap::new();

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

            // Update entry_point_node for the next layer's search based on this layer's findings
            // Find the closest node among the selected neighbors in this layer to potentially
            // improve the entry point for the next layer up.
            if let Some(closest_in_layer) = selected_neighbors
                .iter()
                .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(cmp::Ordering::Equal))
                && closest_in_layer.1 < entry_point_dist
            {
                entry_point_node = closest_in_layer.0;
                entry_point_dist = closest_in_layer.1;
            }

            // 记录新节点需要连接的邻居，并收集反向连接信息
            for (neighbor_id, dist, layer) in selected_neighbors {
                // Don't connect to self
                if neighbor_id == id {
                    continue;
                }

                let dist_bf16 = bf16::from_f32(dist);
                // 1. 添加到新节点的邻居列表
                node_neighbors[current_layer_build as usize].push((neighbor_id, dist_bf16));

                // 2. 记录邻居节点需要添加的反向连接。HNSW 算法允许“低层节点作为高层节点的邻居”，但不允许反向，即不能给低层节点加高层 neighbors。
                if layer >= current_layer_build {
                    neighbor_updates_required
                        .entry(neighbor_id)
                        .or_default()
                        .push((current_layer_build, (id, dist_bf16)));
                }
            }
        }

        // --- 阶段二：插入新节点 ---
        let new_node = HnswNode {
            id,
            layer,
            vector,
            neighbors: node_neighbors, // 使用收集好的邻居列表
            version: 1,                // Initial version
        };

        nodes.insert(id, new_node);
        self.ids.write().add(id);

        let mut local_dirty_nodes = BTreeSet::new();
        local_dirty_nodes.insert(id);

        {
            // 更新入口点（如果需要）和元数据
            let mut entry_point_guard = self.entry_point.write();
            if layer > entry_point_guard.1 || !nodes.contains_key(&entry_point_guard.0) {
                // Update if new node is at a higher layer OR if the current entry point
                // no longer exists (e.g., was concurrently deleted).
                *entry_point_guard = (id, layer);
            }
            // Release entry point lock before metadata lock
        }

        self.update_metadata(|m| {
            m.stats.version += 1; // Increment index version
            m.stats.last_inserted = now_ms;
            if layer > m.stats.max_layer {
                m.stats.max_layer = layer;
            }
            m.stats.insert_count += 1;
        });

        // --- 阶段三：顺序化更新现有邻居 ---
        #[allow(clippy::type_complexity)]
        let mut neighbors_to_truncate: Vec<(u64, Vec<(u8, SmallVec<[(u64, bf16); 64]>)>)> =
            Vec::with_capacity(64); // id, layer, connections
        for (neighbor_id, updates) in neighbor_updates_required {
            // 获取邻居节点的可变引用（锁定对应分片）
            if let Some(neighbor_node) = nodes.get(&neighbor_id) {
                let mut neighbor_node = neighbor_node.clone();
                // 1. 添加反向连接
                #[allow(clippy::type_complexity)]
                let mut to_truncate: Vec<(u8, SmallVec<[(u64, bf16); 64]>)> = Vec::new();
                for (update_layer, connection) in updates {
                    if let Some(n_layer_list) =
                        neighbor_node.neighbors.get_mut(update_layer as usize)
                    {
                        let max_conns = if update_layer > 0 {
                            self.config.max_connections as usize
                        } else {
                            self.config.max_connections as usize * 2
                        };
                        let should_truncate = (max_conns as f64 * 1.2) as usize;
                        n_layer_list.push(connection);
                        if n_layer_list.len() > should_truncate {
                            to_truncate.push((update_layer, n_layer_list.clone()));
                        }
                    }
                }

                if !to_truncate.is_empty() {
                    neighbors_to_truncate.push((neighbor_id, to_truncate));
                }

                // 3. 更新版本号并标记为脏节点
                neighbor_node.version += 1;
                local_dirty_nodes.insert(neighbor_id);
                nodes.insert(neighbor_id, neighbor_node);
            };
        }

        // --- 阶段四：修剪节点 ---
        for (neighbor_id, to_truncate) in neighbors_to_truncate {
            for (layer, connections) in to_truncate {
                let max_conns = if layer > 0 {
                    self.config.max_connections as usize
                } else {
                    self.config.max_connections as usize * 2
                };
                let candidates: Vec<(u64, f32, u8)> = connections
                    .into_iter()
                    .map(|(id, dist)| (id, dist.to_f32(), 0)) // use 0 as a placeholder for layer
                    .collect();

                if let Ok(selected) = self.select_neighbors(
                    candidates,
                    max_conns,
                    self.config.select_neighbors_strategy,
                    &mut multi_distance_cache,
                ) {
                    // select_neighbors 会读取 self.nodes
                    // 修改 self.nodes 必须在 select_neighbors 之后
                    if let Some(node) = nodes.get(&neighbor_id)
                        && node.neighbors.len() > layer as usize
                    {
                        // Update neighbor connections
                        let mut node = node.clone();
                        node.neighbors[layer as usize] = selected
                            .into_iter()
                            .map(|(id, dist, _)| (id, bf16::from_f32(dist)))
                            .collect();
                        nodes.insert(neighbor_id, node);
                    };
                }
            }
        }

        // --- 阶段五：提交脏节点 ---
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

    /// Removes a vector from the index.
    /// It will not remove the node data from the persistent storage. You need to do it manually.
    ///
    /// # Arguments
    ///
    /// * `id` - ID of the vector to remove
    /// * `now_ms` - Current timestamp in milliseconds
    ///
    /// # Returns
    ///
    /// * `bool` - True if the node was deleted, false otherwise
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

    /// Searches for the k nearest neighbors to the query vector
    ///
    /// Results are sorted by ascending distance (closest first)
    ///
    /// # Arguments
    ///
    /// * `query` - Query vector
    /// * `top_k` - Number of nearest neighbors to return
    ///
    /// # Returns
    ///
    /// * `Result<Vec<(u64, f32)>, HnswError>` - Vector of (id, distance) pairs
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
        // 从最高层向下搜索入口点
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

        // 在底层搜索最近的邻居
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

                        // 平衡因子 = 距离查询点的近似程度 + 与已选集合的多样性
                        let improvement = min_dist_to_selected - cand_dist;
                        if improvement > best_distance_improvement {
                            best_distance_improvement = improvement;
                            best_candidate_idx = i;
                        }
                    }

                    // 添加最佳候选点
                    // 添加最佳候选点（防止索引越界）
                    if best_candidate_idx < remaining.len() {
                        selected.push(remaining.swap_remove(best_candidate_idx));
                    } else if !remaining.is_empty() {
                        // 退化为简单策略，避免可能的死循环
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

        // 首先选择最近的节点
        if !remaining.is_empty() {
            selected.push(remaining.remove(0));
        }

        // 使用改进的多样性度量
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
                    // 平衡距离和多样性：更高的多样性分数和更低的查询距离
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

    /// Stores the index metadata, IDs and nodes to persistent storage.
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
        if !self.store_metadata(metadata, now_ms)? {
            return Ok(false);
        }
        self.store_ids(ids)?;
        self.store_dirty_nodes(f).await?;
        Ok(true)
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
        let mut meta = self.metadata();
        let prev_saved_version = self
            .last_saved_version
            .fetch_max(meta.stats.version, Ordering::Release);
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
            // Serialization failed: revert last_saved_version so the next call can retry.
            self.last_saved_version
                .fetch_min(prev_saved_version, Ordering::Relaxed);
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
            // 只在序列化阶段短暂持有 map 的 guard；await 前必须释放。
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

        // 添加一些二维向量
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

        // 搜索最近的邻居
        let results = index.search_f32(&[1.1, 1.1], 2).unwrap();
        assert_eq!(results.len(), 2);
        assert!(results[0].1 < results[1].1);
        println!("Search results: {results:?}");

        // 测试持久化
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

        // 欧氏距离
        let config = HnswConfig {
            dimension: 2,
            distance_metric: DistanceMetric::Euclidean,
            ..Default::default()
        };
        let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));
        index.insert_f32(1, v1.clone(), 0).unwrap();
        let results = index.search_f32(&v2, 1).unwrap();
        assert!((results[0].1 - 1.4142135).abs() < 1e-6);

        // 余弦距离
        let config = HnswConfig {
            dimension: 2,
            distance_metric: DistanceMetric::Cosine,
            ..Default::default()
        };
        let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));
        index.insert_f32(1, v1.clone(), 0).unwrap();
        let results = index.search_f32(&v2, 1).unwrap();
        assert!((results[0].1 - 1.0).abs() < 1e-6);

        // 内积
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

        // 曼哈顿距离
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

        // 插入维度不匹配的向量
        let result = index.insert_f32(1, vec![1.0, 2.0], 0);
        assert!(matches!(
            result,
            Err(HnswError::DimensionMismatch {
                expected: 3,
                got: 2,
                ..
            })
        ));

        // 插入正确维度的向量
        index.insert_f32(1, vec![1.0, 2.0, 3.0], 0).unwrap();

        // 搜索维度不匹配的向量
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

        // 首次插入成功
        index.insert_f32(1, vec![1.0, 2.0], 0).unwrap();

        // 重复插入同一ID应该失败
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

        // 添加向量
        index.insert_f32(1, vec![1.0, 1.0], 0).unwrap();
        index.insert_f32(2, vec![2.0, 2.0], 0).unwrap();
        index.insert_f32(3, vec![3.0, 3.0], 0).unwrap();

        assert_eq!(index.len(), 3);

        // 删除存在的向量
        let deleted = index.remove(2, 0);
        assert!(deleted);
        assert_eq!(index.len(), 2);

        // 删除不存在的向量
        let deleted = index.remove(4, 0);
        assert!(!deleted);

        // 搜索应该只返回剩余的向量
        let results = index.search_f32(&[1.5, 1.5], 5).unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|(id, _)| *id == 1 || *id == 3));
    }

    #[test]
    fn test_select_neighbors_strategies() {
        // 测试简单策略
        let config = HnswConfig {
            dimension: 2,
            select_neighbors_strategy: SelectNeighborsStrategy::Simple,
            ..Default::default()
        };
        let simple_index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));

        // 测试启发式策略
        let config = HnswConfig {
            dimension: 2,
            select_neighbors_strategy: SelectNeighborsStrategy::Heuristic,
            ..Default::default()
        };
        let heuristic_index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));

        // 添加相同的向量到两个索引
        for i in 0..20 {
            let x = (i % 5) as f32;
            let y = (i / 5) as f32;
            simple_index.insert_f32(i, vec![x, y], 0).unwrap();
            heuristic_index.insert_f32(i, vec![x, y], 0).unwrap();
        }

        // 两种策略都应该能找到最近邻
        let simple_results = simple_index.search_f32(&[2.5, 2.5], 5).unwrap();
        let heuristic_results = heuristic_index.search_f32(&[2.5, 2.5], 5).unwrap();

        // 两种策略都应该返回5个结果
        assert_eq!(simple_results.len(), 5);
        assert_eq!(heuristic_results.len(), 5);
    }

    #[tokio::test]
    async fn test_file_persistence() {
        let mut metadata = Vec::new();
        let mut ids = Vec::new();
        let mut nodes: HashMap<u64, Vec<u8>> = HashMap::new();

        // 创建并填充索引
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

            // 验证索引大小
            assert_eq!(loaded_index.len(), 100);

            // 验证搜索功能
            let results = loaded_index.search_f32(&[5.0, 5.0, 0.0], 10).unwrap();
            assert_eq!(results.len(), 10);
        }
    }

    #[test]
    fn test_stats() {
        let config = HnswConfig {
            dimension: 2,
            ..Default::default()
        };
        let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));

        // 初始状态
        let stats = index.stats();
        assert_eq!(stats.num_elements, 0);
        assert_eq!(stats.insert_count, 0);
        assert_eq!(stats.search_count, 0);
        assert_eq!(stats.delete_count, 0);

        // 添加向量
        for i in 0..10 {
            index.insert_f32(i, vec![i as f32, i as f32], 0).unwrap();
        }

        let stats = index.stats();
        assert_eq!(stats.num_elements, 10);
        assert_eq!(stats.insert_count, 10);

        // 执行搜索
        for _ in 0..5 {
            index.search_f32(&[5.0, 5.0], 3).unwrap();
        }

        let stats = index.stats();
        assert_eq!(stats.search_count, 5);

        // 删除向量
        index.remove(5, 0);
        index.remove(6, 0);

        let stats = index.stats();
        assert_eq!(stats.num_elements, 8);
        assert_eq!(stats.delete_count, 2);
    }

    #[test]
    fn test_bf16_conversion() {
        // 测试f32到bf16的转换精度
        let original = [1.234f32, 5.678f32, 9.012f32];
        let bf16_vec: Vec<bf16> = original.iter().map(|&x| bf16::from_f32(x)).collect();
        let back_to_f32: Vec<f32> = bf16_vec.iter().map(|x| x.to_f32()).collect();

        // bf16有限的精度会导致一些舍入误差
        for (i, (orig, converted)) in original.iter().zip(back_to_f32.iter()).enumerate() {
            println!(
                "Original: {}, Converted: {}, Diff: {}",
                orig,
                converted,
                (orig - converted).abs()
            );
            // 允许一定的误差范围
            assert!(
                (orig - converted).abs() < 0.1,
                "Too much precision loss at index {i}"
            );
        }
    }

    #[test]
    fn test_large_dimension() {
        // 测试高维向量
        let dim = 128;
        let config = HnswConfig {
            dimension: dim,
            ..Default::default()
        };
        let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));

        // 创建一些高维向量
        for i in 0..10 {
            let vec = vec![i as f32 / 10.0; dim];
            index.insert_f32(i, vec, 0).unwrap();
        }

        // 搜索
        let query = vec![0.35; dim];
        let results = index.search_f32(&query, 3).unwrap();

        assert_eq!(results.len(), 3);
        // 最近的应该是0.3或0.4
        assert!(results[0].0 == 3 || results[0].0 == 4);
    }

    #[test]
    fn test_entry_point_update() {
        let config = HnswConfig {
            dimension: 2,
            ..Default::default()
        };
        let index = HnswIndex::new("anda_db_hnsw".to_string(), Some(config));

        // 添加向量
        index.insert_f32(1, vec![1.0, 1.0], 0).unwrap();

        // 获取当前入口点
        let (entry_id, _) = *index.entry_point.read();
        assert_eq!(entry_id, 1);

        // 删除入口点
        index.remove(entry_id, 0);

        // 添加新向量，应该成为新的入口点
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

        // 添加一些初始数据
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

                // 每个线程执行不同的操作
                let base_id = 100 + t * 100;

                // 插入操作
                for i in 0..20 {
                    let id = base_id + i;
                    index_clone.insert_f32(id as u64, vec![id as f32, id as f32, id as f32], 0)?;
                }

                // 搜索操作
                for _ in 0..5 {
                    let _ = index_clone.search_f32(&[t as f32, t as f32, t as f32], 5)?;
                }

                // 删除操作
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
