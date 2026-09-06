use crate::{DistanceMetric, HnswError, LayerGen};
use serde::{Deserialize, Serialize};

/// Tunable HNSW parameters. Defaults are suitable for 384–768-dim sentence
/// embeddings; see the crate-level docs for guidance on tuning.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HnswConfig {
    /// Required vector dimensionality. Every `insert` / `search` validates this.
    pub dimension: usize,

    /// Maximum number of layers, from 1 to 64. Default `16`.
    pub max_layers: u8,

    /// Maximum connections per node (M). Layer 0 uses `2 * M` by convention.
    /// Default `32`.
    pub max_connections: u8,

    /// Candidate-list breadth during construction (`efConstruction`).
    /// Higher = better graph quality, slower inserts. Default `200`.
    pub ef_construction: usize,

    /// Candidate-list breadth during search (`efSearch`). Must be ≥ `top_k`;
    /// [`crate::HnswIndex::search`] enforces this at query time. Default `50`.
    pub ef_search: usize,

    /// Distance metric used for similarity. Default [`DistanceMetric::Euclidean`].
    pub distance_metric: DistanceMetric,

    /// Scale factor applied to the exponential layer distribution. `> 1.0`
    /// makes upper layers denser; `< 1.0` makes them sparser. Default `1.0`.
    pub scale_factor: Option<f64>,

    /// Neighbor selection strategy. Default [`SelectNeighborsStrategy::Heuristic`].
    pub select_neighbors_strategy: SelectNeighborsStrategy,

    /// Whether [`crate::HnswIndex::remove`] attempts local neighbor reconnection.
    /// Default `false`: delete all incoming edges without adding replacement
    /// links. This is cheaper but recall can degrade under heavy deletion.
    /// `true` merges peers into affected neighbors' candidate sets and reselects
    /// edges. It costs additional distance work under the structural lock and
    /// improves resilience without guaranteeing connectivity for every graph.
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
    /// Target out-degree at a layer. Layer zero uses twice M.
    pub(crate) fn layer_capacity(&self, layer: u8) -> usize {
        self.max_connections as usize * if layer == 0 { 2 } else { 1 }
    }

    /// Hard out-degree bound, including the amortized pruning allowance.
    pub(crate) fn layer_limit(&self, layer: u8) -> usize {
        let capacity = self.layer_capacity(layer);
        capacity + capacity / 5
    }
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
    /// This keeps the infallible [`crate::HnswIndex::new`] constructor backward
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
