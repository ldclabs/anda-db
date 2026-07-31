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
use parking_lot::{Mutex, RwLock};
use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::{
    borrow::Cow,
    cmp::{self, Reverse},
    collections::{BTreeSet, BinaryHeap, hash_map::Entry},
    future::Future,
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

    /// Serializes structural graph mutations.
    ///
    /// Search remains lock-free on the hot path, but insert/remove both clone
    /// and rewrite adjacency lists. Without this mutex, concurrent writers can
    /// overwrite each other's neighbor-list updates.
    structural_lock: Mutex<()>,

    /// Serializes complete persistence passes.
    ///
    /// A flush snapshots the graph under [`Self::structural_lock`], releases
    /// that synchronous lock before doing I/O, and then persists the immutable
    /// snapshot. Serializing those passes prevents an older snapshot from
    /// overwriting node or id objects after a newer snapshot has committed.

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

    /// Ids removed since the last successful purge. Consumed by
    /// [`Self::purge_removed_nodes`] so the caller can delete the
    /// corresponding persisted node blobs; without this, removed node files
    /// would accumulate forever. Also persisted alongside the metadata so a
    /// crash between a flush and the next purge re-queues the pending
    /// deletions on reload.
    removed_nodes: RwLock<BTreeSet<u64>>,

    /// Roaring-bitmap index of live node ids. Kept in sync with `nodes`.
    ids: RwLock<Treemap>,

    /// Live node ids grouped by their highest layer. Kept in sync with
    /// `nodes` (all mutations happen under `structural_lock`; the loaders
    /// rebuild it) so that removing the entry point or a top-layer node can
    /// find a replacement without an O(N) scan over all nodes.

    /// Total number of queries served (exposed via `stats()`).
    search_count: AtomicU64,

    /// Highest metadata version already flushed to disk. Used to short-circuit
    /// no-op calls to [`Self::store_metadata`] and to make flushes idempotent
    /// under concurrent writers.
    last_saved_version: AtomicU64,
}

/// Tunable HNSW parameters. Defaults are suitable for 384–768-dim sentence
/// embeddings; see the crate-level docs for guidance on tuning.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

    /// Whether [`HnswIndex::remove`] repairs the graph around a deleted node
    /// by re-linking its former neighbors to each other. Default `false`.
    ///
    /// * `false` (default) — deletion only drops the reverse edges (a cheap
    ///   `swap_remove` per affected neighbor) and returns immediately. Bulk
    ///   deletions are fast, but every deletion strictly reduces the
    ///   survivors' connectivity: under delete-heavy workloads recall can
    ///   degrade and a cluster reachable only through deleted nodes can
    ///   become unreachable. Compensate with periodic rebuilds (or
    ///   re-inserts of affected regions) when deletions dominate.
    /// * `true` — after pruning the reverse edges, the deleted node's
    ///   remaining neighbors are merged into each affected neighbor's
    ///   candidate set and the configured [`SelectNeighborsStrategy`]
    ///   re-selects its edges. This keeps the local subgraph connected, so
    ///   recall stays stable even under heavy deletion workloads — but the
    ///   repair runs `O(M²·L)` distance computations **while holding the
    ///   index's structural write lock**, which can slow bulk deletions by
    ///   orders of magnitude.
    ///
    /// Metadata persisted without this field deserializes to `false`, which is
    /// the behavior those indexes were actually built with: every published
    /// release up to and including 0.9.1 pruned the reverse edges and stopped
    /// there (`remove()` had no re-link step at all). The unconditional
    /// re-link existed only in the unpublished 0.9.2 line; 0.10.0 and later
    /// always serialize this field explicitly, so an index missing it can only
    /// come from a release that never repaired on delete.
    #[serde(default)]
    pub reconnect_on_delete: bool,
}

impl HnswConfig {
    /// Minimum useful value for `max_layers`.
    pub const MIN_MAX_LAYERS: u8 = 1;

    /// Minimum useful value for `max_connections`.
    pub const MIN_MAX_CONNECTIONS: u8 = 2;

    /// Maximum vector dimensionality accepted by public configuration.
    pub const MAX_DIMENSION: usize = 16_384;

    /// Maximum layer count accepted by public configuration.
    pub const MAX_MAX_LAYERS: u8 = 64;

    /// Maximum neighbor connections accepted by public configuration.
    pub const MAX_MAX_CONNECTIONS: u8 = 128;

    /// Maximum construction candidate-list breadth.
    pub const MAX_EF_CONSTRUCTION: usize = 4_096;

    /// Maximum search candidate-list breadth.
    pub const MAX_EF_SEARCH: usize = 4_096;

    /// Creates a layer generator based on the configuration.
    ///
    /// # Returns
    ///
    /// * `LayerGen` - A layer generator with the configured parameters.
    pub fn layer_gen(&self) -> LayerGen {
        let config = self.clone().normalized();
        LayerGen::new_with_scale(
            config.max_connections,
            config.scale_factor.unwrap_or(1.0),
            config.max_layers,
        )
    }

    /// Returns a runtime-safe copy of the config.
    ///
    /// This keeps the infallible [`HnswIndex::new`] constructor backward
    /// compatible while preventing invalid public config values from causing
    /// panics in layer generation or zero-width searches.
    pub fn normalized(mut self) -> Self {
        self.dimension = self.dimension.clamp(1, Self::MAX_DIMENSION);
        self.max_layers = self
            .max_layers
            .clamp(Self::MIN_MAX_LAYERS, Self::MAX_MAX_LAYERS);
        self.max_connections = self
            .max_connections
            .clamp(Self::MIN_MAX_CONNECTIONS, Self::MAX_MAX_CONNECTIONS);
        self.ef_construction = self.ef_construction.clamp(1, Self::MAX_EF_CONSTRUCTION);
        self.ef_search = self.ef_search.clamp(1, Self::MAX_EF_SEARCH);
        if !matches!(self.scale_factor, Some(scale_factor) if scale_factor.is_finite() && scale_factor > 0.0)
        {
            self.scale_factor = None;
        }
        self
    }

    /// Strictly validates the config without normalization.
    pub fn validate(&self, name: &str) -> Result<(), HnswError> {
        if self.dimension == 0 {
            return Err(Self::invalid_config(
                name,
                "dimension must be greater than 0",
            ));
        }
        if self.dimension > Self::MAX_DIMENSION {
            return Err(Self::invalid_config(
                name,
                format!("dimension must be at most {}", Self::MAX_DIMENSION),
            ));
        }
        if self.max_layers < Self::MIN_MAX_LAYERS {
            return Err(Self::invalid_config(name, "max_layers must be at least 1"));
        }
        if self.max_layers > Self::MAX_MAX_LAYERS {
            return Err(Self::invalid_config(
                name,
                format!("max_layers must be at most {}", Self::MAX_MAX_LAYERS),
            ));
        }
        if self.max_connections < Self::MIN_MAX_CONNECTIONS {
            return Err(Self::invalid_config(
                name,
                "max_connections must be at least 2",
            ));
        }
        if self.max_connections > Self::MAX_MAX_CONNECTIONS {
            return Err(Self::invalid_config(
                name,
                format!(
                    "max_connections must be at most {}",
                    Self::MAX_MAX_CONNECTIONS
                ),
            ));
        }
        if self.ef_construction == 0 {
            return Err(Self::invalid_config(
                name,
                "ef_construction must be greater than 0",
            ));
        }
        if self.ef_construction > Self::MAX_EF_CONSTRUCTION {
            return Err(Self::invalid_config(
                name,
                format!(
                    "ef_construction must be at most {}",
                    Self::MAX_EF_CONSTRUCTION
                ),
            ));
        }
        if self.ef_search == 0 {
            return Err(Self::invalid_config(
                name,
                "ef_search must be greater than 0",
            ));
        }
        if self.ef_search > Self::MAX_EF_SEARCH {
            return Err(Self::invalid_config(
                name,
                format!("ef_search must be at most {}", Self::MAX_EF_SEARCH),
            ));
        }
        if let Some(scale_factor) = self.scale_factor
            && (!scale_factor.is_finite() || scale_factor <= 0.0)
        {
            return Err(Self::invalid_config(
                name,
                "scale_factor must be finite and greater than 0",
            ));
        }
        Ok(())
    }

    fn invalid_config(name: &str, message: impl Into<String>) -> HnswError {
        HnswError::Generic {
            name: name.to_string(),
            source: format!("Invalid config: {}", message.into()).into(),
        }
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
            reconnect_on_delete: false,
        }
    }
}

/// Neighbor selection strategies used both during graph construction and when
/// pruning over-connected nodes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SelectNeighborsStrategy {
    /// Greedy top-k by distance. Fastest to build; lower recall on hard data.
    Simple,

    /// Algorithm 4 from the HNSW paper with `keepPrunedConnections`: keeps a
    /// candidate only if it is closer to the query than to every neighbor
    /// already selected, then backfills with the closest pruned candidates.
    /// Better recall than [`SelectNeighborsStrategy::Simple`], especially on
    /// clustered data.
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
///
/// # Panics
///
/// Panics if CBOR encoding fails. Encoding into a `Vec` cannot fail for any
/// well-formed [`HnswNode`] (plain integers, `bf16` vectors and adjacency
/// lists), so this is unreachable in practice; the signature stays infallible
/// for backward compatibility.
pub fn serialize_node(node: &HnswNode) -> Vec<u8> {
    let mut buf = Vec::new();
    cbor2::to_writer(node, &mut buf).expect("Failed to serialize node");
    buf
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

/// Immutable, single-generation persistence image captured while structural
/// mutations are excluded.
struct HnswFlushSnapshot {
    version: u64,
    last_saved: u64,
    dirty_ids: Vec<u64>,
    nodes: Vec<(u64, Vec<u8>)>,
    ids: Vec<u8>,
    metadata: Vec<u8>,
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
            entry_point: RwLock::new((0, 0)),
            metadata: RwLock::new(HnswMetadata {
                name,
                config,
                stats,
            }),
            dirty_nodes: RwLock::new(BTreeSet::new()),
            removed_nodes: RwLock::new(BTreeSet::new()),
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
        let mut index: HnswIndexOwned =
            cbor2::from_reader(r).map_err(|err| HnswError::Serialization {
                name: "unknown".to_string(),
                source: err.into(),
            })?;
        index.metadata.config = index.metadata.config.normalized();
        let layer_gen = index.metadata.config.layer_gen();
        let search_count = AtomicU64::new(index.metadata.stats.search_count);
        let last_saved_version = AtomicU64::new(index.metadata.stats.version);
        let entry_point = (
            index.entry_point.0,
            index
                .entry_point
                .1
                .min(index.metadata.config.max_layers.saturating_sub(1)),
        );

        Ok(HnswIndex {
            name: index.metadata.name.clone(),
            config: index.metadata.config.clone(),
            layer_gen,
            structural_lock: Mutex::new(()),
            nodes: CoHashMap::new(),
            entry_point: RwLock::new(entry_point),
            metadata: RwLock::new(index.metadata),
            dirty_nodes: RwLock::new(BTreeSet::new()),
            removed_nodes: RwLock::new(index.removed_nodes.into_iter().collect()),
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
        let ids: Vec<u8> = cbor2::from_reader(r).map_err(|err| HnswError::Serialization {
            name: self.name.clone(),
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

        enum LoadedNode {
            Loaded(u64, HnswNode),
            Missing(u64),
        }

        let name = &self.name;
        let dimension = self.config.dimension;
        let max_layers = self.config.max_layers;
        let f_ref = &f;
        let mut stream = stream::iter(ids)
            .map(|id| async move {
                match f_ref(id).await {
                    Ok(Some(data)) => {
                        let node: HnswNode = cbor2::from_reader(&data[..]).map_err(|err| {
                            HnswError::Serialization {
                                name: name.clone(),
                                source: err.into(),
                            }
                        })?;
                        Self::validate_loaded_node(name, id, dimension, max_layers, &node)?;
                        Ok::<_, HnswError>(LoadedNode::Loaded(id, node))
                    }
                    Ok(None) => Ok(LoadedNode::Missing(id)),
                    Err(err) => Err(HnswError::Generic {
                        name: name.clone(),
                        source: err,
                    }),
                }
            })
            .buffer_unordered(Self::LOAD_NODES_CONCURRENCY);

        let nodes = &self.nodes;
        let mut missing_ids = Vec::new();
        while let Some(item) = stream.try_next().await? {
            match item {
                LoadedNode::Loaded(id, node) => {
                    // Re-acquire the pin guard per item: papaya's LocalGuard contains
                    // raw pointers and is !Send, so it must not be held across .await.
                    nodes.pin().insert(id, node);
                }
                LoadedNode::Missing(id) => missing_ids.push(id),
            }
        }

        if !missing_ids.is_empty() {
            {
                let mut ids = self.ids.write();
                for id in &missing_ids {
                    ids.remove(*id);
                }
            }
            let repaired_nodes = self.prune_missing_node_edges(&missing_ids);
            let max_layer = self.repair_entry_point();
            self.update_metadata(|metadata| {
                metadata.stats.version = metadata.stats.version.saturating_add(1);
                metadata.stats.delete_count = metadata
                    .stats
                    .delete_count
                    .saturating_add(missing_ids.len() as u64);
                metadata.stats.max_layer = max_layer;
            });
            if repaired_nodes > 0 {
                log::debug!(
                    "Removed stale edges to {} missing HNSW nodes from {} loaded nodes",
                    missing_ids.len(),
                    repaired_nodes
                );
            }
        } else {
            // No node blobs were missing, but the persisted entry point may
            // still dangle (corruption / partial write). Repair whenever the
            // entry node is absent from the loaded graph. We must not use
            // `entry_point != 0` as an "unset" sentinel: id 0 is a valid node,
            // so that guard would skip repairing a dangling `(0, _)` entry and
            // leave every search failing with `NotFound { id: 0 }`. Reaching
            // this branch implies `self.nodes` is non-empty (some id loaded).
            let (entry_point, _) = *self.entry_point.read();
            if self.nodes.pin().get(&entry_point).is_none() {
                let max_layer = self.repair_entry_point();
                self.update_metadata(|metadata| {
                    metadata.stats.version = metadata.stats.version.saturating_add(1);
                    metadata.stats.max_layer = max_layer;
                });
            }
        }
        Ok(())
    }

    fn validate_loaded_node(
        name: &str,
        expected_id: u64,
        dimension: usize,
        max_layers: u8,
        node: &HnswNode,
    ) -> Result<(), HnswError> {
        if node.id != expected_id {
            return Err(HnswError::Generic {
                name: name.to_string(),
                source: format!(
                    "Loaded node id mismatch, expected {expected_id}, got {}",
                    node.id
                )
                .into(),
            });
        }
        if node.vector.len() != dimension {
            return Err(HnswError::DimensionMismatch {
                name: name.to_string(),
                expected: dimension,
                got: node.vector.len(),
            });
        }
        if node.layer >= max_layers {
            return Err(HnswError::Generic {
                name: name.to_string(),
                source: format!(
                    "Loaded node {expected_id} has layer {} outside configured max_layers {max_layers}",
                    node.layer
                )
                .into(),
            });
        }
        let expected_neighbors = node.layer as usize + 1;
        if node.neighbors.len() != expected_neighbors {
            return Err(HnswError::Generic {
                name: name.to_string(),
                source: format!(
                    "Loaded node {expected_id} has {} neighbor layers, expected {expected_neighbors}",
                    node.neighbors.len()
                )
                .into(),
            });
        }
        if node.vector.iter().any(|value| !value.is_finite()) {
            return Err(HnswError::Generic {
                name: name.to_string(),
                source: format!("Loaded node {expected_id} contains NaN or infinity").into(),
            });
        }
        if node
            .neighbors
            .iter()
            .flatten()
            .any(|(_, distance)| !distance.is_finite())
        {
            return Err(HnswError::Generic {
                name: name.to_string(),
                source: format!("Loaded node {expected_id} contains non-finite edge distance")
                    .into(),
            });
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
        let mut distance_cache = FxHashMap::default();
        let mut entry_point_node = initial_entry_point_node;
        let mut entry_point_layer = current_max_layer;
        let mut entry_point_dist = f32::MAX;

        // Search from top layer down to find the best entry point
        for current_layer_search in (current_max_layer.min(layer + 1)..=current_max_layer).rev() {
            let nearest = self.search_layer(
                &vector_f32,
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
                &vector_f32,
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
        let new_node = HnswNode {
            id,
            layer,
            vector,
            neighbors: node_neighbors,
            version: 1,
        };

        nodes.insert(id, new_node);
        self.ids.write().add(id);
        // A re-inserted id must not have its (new) blob purged by a pending
        // tombstone from an earlier remove().
        self.removed_nodes.write().remove(&id);

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

    /// Removes a node and prunes the reverse edges that point to it,
    /// optionally re-linking its former neighbors to each other.
    ///
    /// This method only mutates the in-memory graph. The id is recorded as a
    /// tombstone; call [`Self::purge_removed_nodes`] after flushing so the
    /// persistence layer deletes the corresponding on-disk node blob,
    /// otherwise removed node files accumulate forever.
    ///
    /// The implementation walks the deleted node's own neighbor list rather
    /// than scanning the whole map, reducing cost from O(N) to O(M²·L).
    /// Stale back-references from nodes that were not in the deleted node's
    /// neighbor list (e.g. after a prior prune) are harmless: they are skipped
    /// at search time when `nodes.get()` returns `None`.
    ///
    /// Only when [`HnswConfig::reconnect_on_delete`] is `true` — it is `false`
    /// by default — the graph is additionally repaired: for every layer where
    /// a neighbor lost its edge to the deleted node, the deleted node's
    /// remaining neighbors at that layer are merged into the neighbor's
    /// candidate set and the configured [`SelectNeighborsStrategy`] re-selects
    /// its edges (the local repair used by hnswlib). The repair costs
    /// `O(M²·L)` distance computations while the structural write lock is
    /// held, which is why it is opt-in.
    ///
    /// With the default `false`, a deletion only `swap_remove`s the reverse
    /// edges, so every deletion strictly reduces the survivors' connectivity:
    /// recall degrades as deletions accumulate and a cluster reachable only
    /// through the deleted node can become unreachable entirely. Enable
    /// [`HnswConfig::reconnect_on_delete`] (or rebuild periodically) when
    /// recall stability under delete-heavy workloads matters more than
    /// deletion throughput; see the config field's documentation for the
    /// trade-offs.
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

        let previous_max_layer = self.metadata.read().stats.max_layer;
        let entry_was_removed = self.entry_point.read().0 == id;
        let replacement_entry = if entry_was_removed {
            // O(N) scan for the live node on the highest layer. Entry-point
            // deletions are rare, and the scan avoids maintaining a mirror
            // per-layer tracker that must stay synchronized with `nodes`.
            nodes
                .iter()
                .filter(|(node_id, _)| **node_id != id)
                .max_by_key(|(_, node)| node.layer)
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
            removed_nodes.insert(id);
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
        } else if node.layer >= previous_max_layer {
            // Removing a node at the current max layer: recompute with a
            // scan (`id` was already removed from `nodes` above).
            Some(nodes.iter().map(|(_, node)| node.layer).max().unwrap_or(0))
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

        // Only iterate the deleted node's known neighbors instead of scanning ALL nodes.
        // Note: nodes that reference the deleted node but are NOT in the deleted node's
        // neighbor list (due to pruning) will retain stale references. These stale references
        // are harmlessly skipped during search (nodes.get() returns None).
        let mut neighbor_ids: FxHashSet<u64> = FxHashSet::with_capacity_and_hasher(
            node.neighbors.iter().map(|l| l.len()).sum(),
            FxBuildHasher,
        );
        for layer_neighbors in &node.neighbors {
            for &(nid, _) in layer_neighbors {
                if nid != id {
                    neighbor_ids.insert(nid);
                }
            }
        }

        // Distance cache shared by the re-link candidates and `select_neighbors`.
        let mut pair_distance_cache: FxHashMap<(u64, u64), f32> = FxHashMap::default();
        let mut dirty_nodes = BTreeSet::new();
        for &neighbor_id in &neighbor_ids {
            if let Some(n) = nodes.get(&neighbor_id) {
                let mut updated = false;
                let mut o = Cow::Borrowed(n);
                for layer in 0..=(n.layer as usize) {
                    let Some(pos) = n.neighbors[layer].iter().position(|&(idx, _)| idx == id)
                    else {
                        continue;
                    };
                    o.to_mut().neighbors[layer].swap_remove(pos);
                    updated = true;

                    // Fast path: with reconnect_on_delete disabled, deletion
                    // only prunes the reverse edge (see the config docs for
                    // the recall trade-off).
                    if !self.config.reconnect_on_delete {
                        continue;
                    }

                    // Re-link: merge the deleted node's other neighbors at
                    // this layer into this node's candidate set and re-select
                    // the best edges, so the local subgraph stays connected.
                    let Some(peers) = node.neighbors.get(layer) else {
                        continue;
                    };
                    let current_list = &o.to_mut().neighbors[layer];
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
                        let max_conns = if layer > 0 {
                            self.config.max_connections as usize
                        } else {
                            // Layer 0 uses 2×M connections (standard HNSW convention).
                            self.config.max_connections as usize * 2
                        };
                        if let Ok(selected) = self.select_neighbors(
                            candidates,
                            max_conns,
                            self.config.select_neighbors_strategy,
                            &mut pair_distance_cache,
                        ) {
                            let layer_list = &mut o.to_mut().neighbors[layer];
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
    /// If a node on the search path is removed concurrently, the search is
    /// transparently retried (up to [`Self::SEARCH_MAX_ATTEMPTS`] times) from
    /// the repaired entry point.
    ///
    /// # Errors
    /// * [`HnswError::DimensionMismatch`] on dimension mismatch.
    /// * [`HnswError::NotFound`] if the entry point could not be resolved even
    ///   after retrying (e.g. persistent index corruption).
    pub fn search(&self, query: &[bf16], top_k: usize) -> Result<Vec<(u64, f32)>, HnswError> {
        if query.len() != self.config.dimension {
            return Err(HnswError::DimensionMismatch {
                name: self.name.clone(),
                expected: self.config.dimension,
                got: query.len(),
            });
        }

        if query.iter().any(|v| !v.is_finite()) {
            return Err(HnswError::Generic {
                name: self.name.clone(),
                source: "Query vector contains invalid values (NaN or infinity)".into(),
            });
        }

        if top_k == 0 {
            return Ok(Vec::new());
        }

        let query_f32: Vec<f32> = query.iter().map(|v| v.to_f32()).collect();
        self.search_inner(&query_f32, top_k)
    }

    /// Searches for nearest neighbors using an `f32` query vector.
    ///
    /// The query stays in `f32` for all distance computations (only the stored
    /// vectors are `bf16`), so no query precision is lost to quantization.
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
        if top_k == 0 {
            return Ok(Vec::new());
        }

        if query.iter().any(|v| !v.is_finite()) {
            return Err(HnswError::Generic {
                name: self.name.clone(),
                source: "Query vector contains invalid values (NaN or infinity)".into(),
            });
        }

        if query.len() != self.config.dimension {
            return Err(HnswError::DimensionMismatch {
                name: self.name.clone(),
                expected: self.config.dimension,
                got: query.len(),
            });
        }

        self.search_inner(query, top_k)
    }

    /// Runs the two-phase search, retrying when a concurrently removed node
    /// surfaces as a transient [`HnswError::NotFound`].
    ///
    /// `remove` repairs the entry point under the structural lock before it
    /// returns, so re-reading the entry point on the next attempt resolves the
    /// race; the retry bound only guards against persistent corruption.
    fn search_inner(&self, query: &[f32], top_k: usize) -> Result<Vec<(u64, f32)>, HnswError> {
        let mut attempt = 0;
        loop {
            attempt += 1;
            if self.nodes.is_empty() {
                return Ok(Vec::new());
            }

            match self.search_attempt(query, top_k) {
                Ok(results) => {
                    self.search_count.fetch_add(1, Ordering::Relaxed);
                    return Ok(results);
                }
                Err(HnswError::NotFound { .. }) if attempt < Self::SEARCH_MAX_ATTEMPTS => continue,
                Err(err) => return Err(err),
            }
        }
    }

    fn search_attempt(&self, query: &[f32], top_k: usize) -> Result<Vec<(u64, f32)>, HnswError> {
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

        // Layer 0 is fully searched with the user-requested breadth. A huge
        // caller-supplied `top_k` is capped at `MAX_EF_SEARCH` so it cannot
        // force an arbitrarily expensive beam; in that case fewer than
        // `top_k` results may be returned.
        let ef = self
            .config
            .ef_search
            .max(top_k.min(HnswConfig::MAX_EF_SEARCH));
        let mut results = self.search_layer(
            query,
            current_node,
            current_node_layer,
            0,
            ef,
            &mut distance_cache,
        )?;
        results.truncate(top_k);

        Ok(results
            .into_iter()
            .map(|(id, dist, _)| (id, dist))
            .collect())
    }

    /// Searches for nearest neighbors within a specific layer
    ///
    /// This is an internal method used by both insert and search operations
    /// to find nearest neighbors at a specific layer of the graph.
    ///
    /// # Arguments
    ///
    /// * `query` - Query vector (`f32`; stored vectors stay `bf16`)
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
        query: &[f32],
        entry_point: u64,
        entry_point_layer: u8,
        layer: u8,
        ef: usize,
        distance_cache: &mut FxHashMap<u64, f32>,
    ) -> Result<Vec<(u64, f32, u8)>, HnswError> {
        let ef = ef.max(1);
        let heap_capacity = ef.saturating_mul(2);
        let mut visited: FxHashSet<u64> =
            FxHashSet::with_capacity_and_hasher(heap_capacity, FxBuildHasher);
        let mut candidates: BinaryHeap<(Reverse<OrderedFloat<f32>>, u64, u8)> =
            BinaryHeap::with_capacity(heap_capacity);
        let mut results: BinaryHeap<(OrderedFloat<f32>, u64, u8)> =
            BinaryHeap::with_capacity(heap_capacity);

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
                    if visited.insert(neighbor)
                        && let Some(neighbor_node) = nodes.get(&neighbor)
                    {
                        match self.get_distance_with_cache(distance_cache, query, neighbor_node) {
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
                                // Defensive: `compute_mixed` only fails on a
                                // dimension mismatch, which cannot happen for
                                // vectors validated at insert/load time. Skip
                                // the neighbor for this search (it is already
                                // marked visited) WITHOUT caching a fake
                                // distance: a cached `f32::MAX` would keep
                                // mis-ranking the node as "farthest" for the
                                // rest of the query.
                                log::warn!("Distance calculation error: {e:?}");
                            }
                        };
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
        if m == 0 {
            return Ok(Vec::new());
        }
        if candidates.len() <= m {
            return Ok(candidates);
        }

        let nodes = self.nodes.pin();
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
                                        .compute(&cand_node.vector, &sel_node.vector)?;
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

    /// Gets the distance between a query vector and a node, using cache when available
    ///
    /// # Arguments
    ///
    /// * `cache` - Cache of previously computed distances
    /// * `query` - Query vector (`f32`; stored vectors stay `bf16`)
    /// * `neighbor` - Node to compute distance to
    ///
    /// # Returns
    ///
    /// * `Result<f32, HnswError>` - Computed distance
    fn get_distance_with_cache(
        &self,
        cache: &mut FxHashMap<u64, f32>,
        query: &[f32],
        neighbor: &HnswNode,
    ) -> Result<f32, HnswError> {
        match cache.entry(neighbor.id) {
            Entry::Occupied(entry) => Ok(*entry.get()),
            Entry::Vacant(entry) => {
                let dist = self
                    .config
                    .distance_metric
                    .compute_mixed(query, &neighbor.vector)?;
                entry.insert(dist);
                Ok(dist)
            }
        }
    }

    /// Captures metadata, ids, and dirty nodes from one structural generation.
    ///
    /// The synchronous structural lock is intentionally released before any
    /// object-store callback is awaited. Mutations can therefore continue
    /// while I/O is in flight, but they cannot leak into this immutable image.
    fn capture_flush_snapshot(&self, now_ms: u64) -> Result<Option<HnswFlushSnapshot>, HnswError> {
        let _structural_guard = self.structural_lock.lock();
        let current_version = self.metadata.read().stats.version;
        let dirty_ids: Vec<u64> = self.dirty_nodes.read().iter().copied().collect();
        if self.last_saved_version.load(Ordering::Acquire) >= current_version
            && dirty_ids.is_empty()
        {
            return Ok(None);
        }

        let mut metadata = self.metadata();
        metadata.stats.last_saved = now_ms.max(metadata.stats.last_saved);
        let version = metadata.stats.version;
        let last_saved = metadata.stats.last_saved;

        let mut metadata_buf = Vec::with_capacity(256);
        cbor2::to_writer(
            &HnswIndexRef {
                entry_point: *self.entry_point.read(),
                metadata: &metadata,
                removed_nodes: self.removed_nodes.read().iter().copied().collect(),
            },
            &mut metadata_buf,
        )
        .map_err(|err| HnswError::Serialization {
            name: self.name.clone(),
            source: err.into(),
        })?;

        let ids_data = {
            let mut ids = self.ids.read().clone();
            ids.run_optimize();
            ids.serialize::<Portable>()
        };
        let mut ids_buf = Vec::with_capacity(ids_data.len().saturating_add(16));
        cbor2::to_writer(&cbor2::Value::Bytes(ids_data), &mut ids_buf).map_err(|err| {
            HnswError::Serialization {
                name: self.name.clone(),
                source: err.into(),
            }
        })?;

        let nodes = self.nodes.pin();
        let mut node_bufs = Vec::with_capacity(dirty_ids.len());
        for id in &dirty_ids {
            let Some(node) = nodes.get(id) else {
                // A stale dirty mark has no live blob to persist. It is still
                // part of `dirty_ids` so a successful commit can retire it.
                continue;
            };
            let mut buf = Vec::with_capacity(4096);
            cbor2::to_writer(node, &mut buf).map_err(|err| HnswError::Serialization {
                name: self.name.clone(),
                source: err.into(),
            })?;
            node_bufs.push((*id, buf));
        }

        Ok(Some(HnswFlushSnapshot {
            version,
            last_saved,
            dirty_ids,
            nodes: node_bufs,
            ids: ids_buf,
            metadata: metadata_buf,
        }))
    }

    /// Commits the in-memory persistence watermark for a durable snapshot.
    fn commit_flush_snapshot(&self, snapshot: &HnswFlushSnapshot) {
        let _structural_guard = self.structural_lock.lock();

        // If a mutation crossed the I/O window, leave every snapshotted dirty
        // id pending. Rewriting a few unchanged nodes on the next pass is
        // preferable to accidentally clearing a remove+reinsert whose node
        // version happened to wrap or restart at the same value.
        if self.metadata.read().stats.version == snapshot.version {
            let mut dirty = self.dirty_nodes.write();
            for id in &snapshot.dirty_ids {
                dirty.remove(id);
            }
        }

        self.last_saved_version
            .fetch_max(snapshot.version, Ordering::Release);
        self.update_metadata(|metadata| {
            metadata.stats.last_saved = snapshot.last_saved.max(metadata.stats.last_saved);
        });
    }

    /// Persists one coherent graph generation through async callbacks.
    ///
    /// The durable order is **nodes → ids → metadata**. Metadata is the commit
    /// record: its callback must use compare-and-swap (or an equivalent atomic
    /// conditional update) in production. A failure or cooperative stop before
    /// that callback leaves both the saved-version watermark and every dirty
    /// node pending for retry. Concurrent mutations are captured by the next
    /// generation.
    ///
    /// # Caller contract
    ///
    /// Persistence calls (`flush*`, `purge_removed_nodes`, `store_*`) must be
    /// serialized by the caller: overlapping flushes could upload an older
    /// snapshot over a newer one. `anda_db`'s `Collection` already guarantees
    /// this by holding its exclusive operation gate across every flush.
    ///
    /// Removed-node blobs are not deleted here. Call
    /// [`Self::purge_removed_nodes`] only after this method returns success.
    pub async fn flush_with<N, NFut, I, IFut, M, MFut>(
        &self,
        now_ms: u64,
        mut node_f: N,
        ids_f: I,
        metadata_f: M,
    ) -> Result<bool, HnswError>
    where
        N: FnMut(u64, Vec<u8>) -> NFut,
        NFut: Future<Output = Result<bool, BoxError>>,
        I: FnOnce(Vec<u8>) -> IFut,
        IFut: Future<Output = Result<(), BoxError>>,
        M: FnOnce(Vec<u8>) -> MFut,
        MFut: Future<Output = Result<(), BoxError>>,
    {
        let Some(snapshot) = self.capture_flush_snapshot(now_ms)? else {
            return Ok(false);
        };

        for (id, data) in &snapshot.nodes {
            let keep_going = node_f(*id, data.clone())
                .await
                .map_err(|err| HnswError::Generic {
                    name: self.name.clone(),
                    source: err,
                })?;
            if !keep_going {
                return Ok(true);
            }
        }

        ids_f(snapshot.ids.clone())
            .await
            .map_err(|err| HnswError::Generic {
                name: self.name.clone(),
                source: err,
            })?;
        metadata_f(snapshot.metadata.clone())
            .await
            .map_err(|err| HnswError::Generic {
                name: self.name.clone(),
                source: err,
            })?;

        self.commit_flush_snapshot(&snapshot);
        Ok(true)
    }

    /// Persists metadata, ids and dirty nodes in one coordinated pass.
    ///
    /// This writer-oriented compatibility API uses the same coherent snapshot
    /// and nodes → ids → metadata commit order as [`Self::flush_with`].
    /// `f` receives `(id, &cbor_bytes)` and may return `Ok(false)` to stop
    /// cooperatively before ids or metadata are committed.
    pub async fn flush<W: Write, F>(
        &self,
        mut metadata: W,
        mut ids: W,
        now_ms: u64,
        mut f: F,
    ) -> Result<bool, HnswError>
    where
        F: AsyncFnMut(u64, &[u8]) -> Result<bool, BoxError>,
    {
        let Some(snapshot) = self.capture_flush_snapshot(now_ms)? else {
            return Ok(false);
        };

        for (id, data) in &snapshot.nodes {
            let keep_going = f(*id, data).await.map_err(|err| HnswError::Generic {
                name: self.name.clone(),
                source: err,
            })?;
            if !keep_going {
                return Ok(true);
            }
        }

        ids.write_all(&snapshot.ids)
            .map_err(|err| HnswError::Serialization {
                name: self.name.clone(),
                source: err.into(),
            })?;
        metadata
            .write_all(&snapshot.metadata)
            .map_err(|err| HnswError::Serialization {
                name: self.name.clone(),
                source: err.into(),
            })?;

        self.commit_flush_snapshot(&snapshot);
        Ok(true)
    }

    /// Returns whether there are dirty nodes pending persistence.
    pub fn has_dirty_nodes(&self) -> bool {
        !self.dirty_nodes.read().is_empty()
    }

    /// Returns whether there are removed-node tombstones pending purge.
    pub fn has_removed_nodes(&self) -> bool {
        !self.removed_nodes.read().is_empty()
    }

    /// Returns the removed-node tombstones whose persisted blobs have not
    /// been purged yet. Callers sweeping storage for orphan blobs must treat
    /// these ids as referenced: their deletion belongs to
    /// [`Self::purge_removed_nodes`].
    pub fn removed_node_ids(&self) -> Vec<u64> {
        self.removed_nodes.read().iter().copied().collect()
    }

    /// Hands every removed node id to the caller so it can delete the
    /// corresponding persisted node blob.
    ///
    /// [`Self::remove`] only mutates the in-memory graph; the on-disk blob of
    /// a removed node must be deleted by the persistence layer or it leaks
    /// forever. Call this after [`Self::store_dirty_nodes`] on each flush.
    ///
    /// Ids whose node has been re-inserted in the meantime are skipped. The
    /// callback returns `Ok(true)` to acknowledge the current deletion and
    /// continue. `Ok(false)` stops early without consuming the current id, so
    /// it and all later unprocessed ids remain retryable; on `Err`, the failing
    /// id likewise remains retryable. Treat "blob not found" as success in the
    /// callback: a crash between a purge and the next flush simply retries
    /// deletions that already happened, and reloaded metadata may re-queue
    /// tombstones whose blobs were already deleted.
    ///
    /// When at least one tombstone is consumed (deleted or skipped as
    /// re-inserted), the metadata version is bumped so the **next flush
    /// persists the shrunken tombstone set**. Without the bump, the persisted
    /// metadata would keep the stale tombstones forever and every reload
    /// would replay the same deletions. Flush after purging (flush → purge →
    /// flush, or simply purge before the next periodic flush) to make the
    /// purge durable.
    pub async fn purge_removed_nodes<F>(&self, mut f: F) -> Result<(), HnswError>
    where
        F: AsyncFnMut(u64) -> Result<bool, BoxError>,
    {
        // Never move tombstones out of the authoritative set before an await:
        // dropping this future at any callback boundary must leave the current
        // and remaining ids retryable. Each successful callback retires its id
        // synchronously in the same poll that observes success.
        let removed: Vec<u64> = self.removed_nodes.read().iter().copied().collect();

        for id in removed {
            // Re-check under the structural gate. An id re-inserted after the
            // snapshot owns a new live blob and its old tombstone is obsolete.
            let reinserted = {
                let _structural_guard = self.structural_lock.lock();
                let nodes = self.nodes.pin();
                if nodes.contains_key(&id) {
                    let retired = self.removed_nodes.write().remove(&id);
                    drop(nodes);
                    if retired {
                        self.update_metadata(|m| m.stats.version += 1);
                    }
                    true
                } else {
                    false
                }
            };
            if reinserted {
                continue;
            }

            let keep_going = f(id).await.map_err(|source| HnswError::Generic {
                name: self.name.clone(),
                source,
            })?;

            // `Ok(false)` is cooperative stop, not acknowledgement that this
            // id's persisted blob was deleted. Leave the current tombstone in
            // the authoritative set so the next purge retries it.
            if !keep_going {
                return Ok(());
            }

            // The delete is durable. Retire the tombstone only if the id is
            // still absent; a concurrent re-insert already removed it and
            // marked the new node dirty. Bump metadata immediately so future
            // cancellation cannot lose evidence that the tombstone set shrank.
            {
                let _structural_guard = self.structural_lock.lock();
                let nodes = self.nodes.pin();
                if !nodes.contains_key(&id) {
                    let retired = self.removed_nodes.write().remove(&id);
                    drop(nodes);
                    if retired {
                        self.update_metadata(|m| m.stats.version += 1);
                    }
                }
            }
        }

        Ok(())
    }

    /// Returns whether metadata has a newer logical version than the last
    /// serialized metadata snapshot.
    pub fn has_pending_metadata_flush(&self) -> bool {
        let current_version = { self.metadata.read().stats.version };
        self.last_saved_version.load(Ordering::Acquire) < current_version
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
        if let Err(err) = cbor2::to_writer(
            &HnswIndexRef {
                entry_point: *self.entry_point.read(),
                metadata: &meta,
                removed_nodes: self.removed_nodes.read().iter().copied().collect(),
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

    /// Like [`Self::store_metadata`], but hands the serialized metadata to an
    /// async persist callback and only commits the saved-version watermark
    /// when the callback succeeds.
    ///
    /// Use this instead of `store_metadata` with an in-memory buffer when the
    /// actual persistence step can fail (e.g. an object-store write): with
    /// the plain variant, a failed external write would leave the watermark
    /// advanced and this metadata version would never be retried, so a later
    /// crash could load stale metadata that hides state written afterwards.
    ///
    /// # Returns
    ///
    /// * `Ok(true)` if metadata was serialized and persisted.
    /// * `Ok(false)` if the version was already saved.
    pub async fn store_metadata_with<F>(&self, now_ms: u64, f: F) -> Result<bool, HnswError>
    where
        F: AsyncFnOnce(&[u8]) -> Result<(), BoxError>,
    {
        let (version, last_saved, buf) = {
            // Keep entry-point, tombstones and metadata in one structural
            // generation while building the immutable payload. The guard is
            // released before the async callback.
            let _structural_guard = self.structural_lock.lock();
            let mut meta = self.metadata();
            if self.last_saved_version.load(Ordering::Acquire) >= meta.stats.version {
                return Ok(false);
            }

            meta.stats.last_saved = now_ms.max(meta.stats.last_saved);
            let version = meta.stats.version;
            let last_saved = meta.stats.last_saved;
            let mut buf = Vec::with_capacity(256);
            cbor2::to_writer(
                &HnswIndexRef {
                    entry_point: *self.entry_point.read(),
                    metadata: &meta,
                    removed_nodes: self.removed_nodes.read().iter().copied().collect(),
                },
                &mut buf,
            )
            .map_err(|err| HnswError::Serialization {
                name: self.name.clone(),
                source: err.into(),
            })?;
            (version, last_saved, buf)
        };

        if let Err(err) = f(&buf).await {
            return Err(HnswError::Generic {
                name: self.name.clone(),
                source: err,
            });
        }

        // Publish only after the callback confirms durability. Cancellation
        // at any earlier await leaves this generation pending for retry.
        self.last_saved_version
            .fetch_max(version, Ordering::Release);
        self.update_metadata(|m| {
            m.stats.last_saved = last_saved.max(m.stats.last_saved);
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

        cbor2::to_writer(&cbor2::Value::Bytes(data), w).map_err(|err| HnswError::Serialization {
            name: self.name.clone(),
            source: err.into(),
        })
    }

    /// Stores dirty nodes to persistent storage using the provided async function
    ///
    /// This method iterates through dirty nodes.
    ///
    /// # Arguments
    ///
    /// * `f` - Async function that writes a node's data to persistent storage.
    ///   It takes a node ID and the serialized payload. Return `Ok(true)` to
    ///   acknowledge the write and continue. `Ok(false)` stops early *without*
    ///   acknowledging the current id, so it and all later unprocessed ids
    ///   stay dirty and are retried by the next call; on `Err`, the failing id
    ///   likewise stays dirty. This mirrors
    ///   [`purge_removed_nodes`](Self::purge_removed_nodes), so one callback
    ///   style is correct for both APIs.
    ///
    /// # Returns
    ///
    /// * `Result<(), HnswError>` - Success or error.
    pub async fn store_dirty_nodes<F>(&self, mut f: F) -> Result<(), HnswError>
    where
        F: AsyncFnMut(u64, &[u8]) -> Result<bool, BoxError>,
    {
        // Iterate an immutable id snapshot. Dirty evidence remains in the
        // authoritative set until the corresponding callback succeeds, so
        // dropping this future at an await boundary is inherently retryable.
        let dirty_ids: Vec<u64> = self.dirty_nodes.read().iter().copied().collect();

        for id in dirty_ids {
            let snapshot = {
                // Mutations increment the global metadata version while
                // holding this gate. If that generation changes during I/O,
                // conservatively leave the id dirty even when this particular
                // node's serialized bytes happen to look unchanged.
                let _structural_guard = self.structural_lock.lock();
                if !self.dirty_nodes.read().contains(&id) {
                    None
                } else {
                    let generation = self.metadata.read().stats.version;
                    let nodes = self.nodes.pin();
                    if let Some(node) = nodes.get(&id) {
                        let mut data = Vec::with_capacity(4096);
                        cbor2::to_writer(node, &mut data).map_err(|err| {
                            HnswError::Serialization {
                                name: self.name.clone(),
                                source: err.into(),
                            }
                        })?;
                        Some((generation, data))
                    } else {
                        // A stale mark without a live node needs no external
                        // write. Retire it synchronously while mutations are
                        // excluded.
                        drop(nodes);
                        self.dirty_nodes.write().remove(&id);
                        None
                    }
                }
            };

            let Some((generation, data)) = snapshot else {
                continue;
            };
            let keep_going = f(id, &data).await.map_err(|source| HnswError::Generic {
                name: self.name.clone(),
                source,
            })?;

            // `Ok(false)` is a cooperative stop, not an acknowledgement that
            // this node's bytes were persisted. Leave the id dirty — as
            // `purge_removed_nodes` leaves its tombstone — so the next flush
            // retries it; clearing the mark first would drop the rewritten
            // adjacency list permanently, since `flush_with`'s snapshot logic
            // keys off `dirty_nodes`.
            if !keep_going {
                return Ok(());
            }

            {
                let _structural_guard = self.structural_lock.lock();
                if self.metadata.read().stats.version == generation {
                    self.dirty_nodes.write().remove(&id);
                }
            }
        }

        Ok(())
    }

    /// Repairs the entry point by selecting the live node with the highest layer.
    ///
    /// Also resynchronizes the per-layer id tracker from the authoritative
    /// node map, since this method is the self-heal path for corrupted state.
    fn repair_entry_point(&self) -> u8 {
        let nodes = self.nodes.pin();
        let max_layer = if let Some((_, node)) = nodes.iter().max_by_key(|(_, node)| node.layer) {
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

    /// Removes all edges that point to node blobs missing during bootstrap.
    fn prune_missing_node_edges(&self, missing_ids: &[u64]) -> usize {
        if missing_ids.is_empty() {
            return 0;
        }

        let missing: FxHashSet<u64> = FxHashSet::from_iter(missing_ids.iter().copied());
        let nodes = self.nodes.pin();
        let mut repaired_nodes = BTreeSet::new();

        for (id, node) in nodes.iter() {
            let mut repaired = None;
            for (layer, neighbors) in node.neighbors.iter().enumerate() {
                if neighbors
                    .iter()
                    .any(|(neighbor_id, _)| missing.contains(neighbor_id))
                {
                    let node = repaired.get_or_insert_with(|| node.clone());
                    node.neighbors[layer].retain(|(neighbor_id, _)| !missing.contains(neighbor_id));
                }
            }

            if let Some(mut node) = repaired {
                node.version = node.version.saturating_add(1);
                repaired_nodes.insert(*id);
                nodes.insert(*id, node);
            }
        }

        let repaired_count = repaired_nodes.len();
        if repaired_count > 0 {
            self.dirty_nodes.write().extend(repaired_nodes);
        }
        repaired_count
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

        // Purge hands the tombstone to the delete callback exactly once.
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
        entered.notified().await;
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
        entered.notified().await;
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
        entered.notified().await;
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
            HnswNode {
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
            },
        );
        nodes.insert(
            2,
            HnswNode {
                id: 2,
                layer: 1,
                vector: vec![bf16::from_f32(2.0), bf16::from_f32(2.0)],
                neighbors: vec![SmallVec::new(), SmallVec::new()],
                version: 1,
            },
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

        let mut layer_cache = FxHashMap::default();
        assert!(matches!(
            index.search_layer(&[0.0, 0.0], u64::MAX, 0, 0, 0, &mut layer_cache),
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

    #[test]
    fn test_load_metadata_and_ids_error_paths_and_entry_clamp() {
        match HnswIndex::load_metadata(&b"not cbor"[..]) {
            Err(HnswError::Serialization { .. }) => {}
            Err(other) => panic!("expected metadata serialization error, got {other:?}"),
            Ok(_) => panic!("expected metadata serialization error"),
        }

        let metadata = HnswMetadata {
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
        let mut loaded = HnswIndex::load_metadata(&bytes[..]).unwrap();
        assert_eq!(*loaded.entry_point.read(), (99, 0));
        assert_eq!(loaded.metadata().config.max_layers, 1);
        assert_eq!(loaded.metadata().config.max_connections, 2);
        assert_eq!(loaded.metadata().config.ef_construction, 1);
        assert_eq!(loaded.metadata().config.ef_search, 1);
        assert_eq!(loaded.metadata().config.scale_factor, None);
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
}
