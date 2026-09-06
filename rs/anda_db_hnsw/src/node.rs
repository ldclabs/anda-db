use half::bf16;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::sync::Arc;

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

    /// Mutation counter for one node instance. An ID can be removed and reused,
    /// so this is not an incarnation identifier. Index persistence additionally
    /// uses immutable snapshot identity and pass generations.
    #[serde(rename = "v")]
    pub version: u64,
}

/// Serializes a public node to legacy-compatible CBOR for inspection/tooling.
/// For complete index persistence, use [`crate::HnswIndex::flush_with_options`],
/// which also records pass generations and acknowledges immutable snapshots.
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

/// Immutable runtime node. Public/wire nodes retain the original Vec/SmallVec
/// representation; hot graph updates share vector storage and clone only edges.
#[derive(Debug)]
pub(crate) struct GraphNode {
    pub id: u64,
    pub layer: u8,
    pub vector: Arc<[bf16]>,
    pub norm: f64,
    pub neighbors: Vec<Vec<(u64, bf16)>>,
    pub version: u64,
}

impl GraphNode {
    pub fn new(id: u64, layer: u8, vector: Vec<bf16>, neighbors: Vec<Vec<(u64, bf16)>>) -> Self {
        let norm = crate::distance::norm(&vector);
        Self {
            id,
            layer,
            vector: vector.into(),
            norm,
            neighbors,
            version: 1,
        }
    }

    pub fn to_public(&self) -> HnswNode {
        HnswNode {
            id: self.id,
            layer: self.layer,
            vector: self.vector.to_vec(),
            neighbors: self
                .neighbors
                .iter()
                .map(|layer| layer.iter().copied().collect())
                .collect(),
            version: self.version,
        }
    }

    fn wire(&self, generation: u64) -> NodeRef<'_> {
        NodeRef {
            i: self.id,
            l: self.layer,
            vec: &self.vector,
            n: &self.neighbors,
            v: self.version,
            g: generation,
        }
    }

    pub fn encoded_size(&self, generation: u64) -> Result<usize, crate::HnswError> {
        cbor2::serialized_size(&self.wire(generation))
            .map_err(|source| crate::HnswError::Serialization {
                name: "node".into(),
                source: source.into(),
            })
            .and_then(|size| {
                usize::try_from(size).map_err(|source| crate::HnswError::Serialization {
                    name: "node".into(),
                    source: source.into(),
                })
            })
    }

    pub fn encode(&self, generation: u64) -> Result<Vec<u8>, crate::HnswError> {
        self.encode_sized(generation, self.encoded_size(generation)?)
    }

    pub fn encode_sized(&self, generation: u64, size: usize) -> Result<Vec<u8>, crate::HnswError> {
        let mut bytes = Vec::with_capacity(size);
        cbor2::to_writer(&self.wire(generation), &mut bytes).map_err(|source| {
            crate::HnswError::Serialization {
                name: "node".into(),
                source: source.into(),
            }
        })?;
        Ok(bytes)
    }
}

impl From<HnswNode> for GraphNode {
    fn from(node: HnswNode) -> Self {
        let mut graph = Self::new(
            node.id,
            node.layer,
            node.vector,
            node.neighbors
                .into_iter()
                .map(|n| {
                    let mut n = n.into_vec();
                    n.shrink_to_fit();
                    n
                })
                .collect(),
        );
        graph.version = node.version;
        graph
    }
}

/// Direct wire decoding avoids Serde flatten's intermediate value tree and
/// decodes adjacency straight into its compact runtime representation.
#[derive(Deserialize)]
pub(crate) struct PersistedNode {
    #[serde(rename = "i")]
    pub id: u64,
    #[serde(rename = "l")]
    pub layer: u8,
    #[serde(rename = "vec")]
    pub vector: Vec<bf16>,
    #[serde(rename = "n")]
    pub neighbors: Vec<Vec<(u64, bf16)>>,
    #[serde(rename = "v")]
    pub version: u64,
    #[serde(default, rename = "g")]
    pub generation: u64,
}

impl PersistedNode {
    pub fn into_graph(self) -> GraphNode {
        let mut node = GraphNode::new(self.id, self.layer, self.vector, self.neighbors);
        node.version = self.version;
        node
    }
}

#[derive(Serialize)]
struct NodeRef<'a> {
    i: u64,
    l: u8,
    vec: &'a [bf16],
    n: &'a [Vec<(u64, bf16)>],
    v: u64,
    g: u64,
}

impl Clone for GraphNode {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            layer: self.layer,
            vector: self.vector.clone(),
            norm: self.norm,
            version: self.version,
            neighbors: self
                .neighbors
                .iter()
                .map(|edges| {
                    let mut copy = Vec::with_capacity(edges.capacity());
                    copy.extend_from_slice(edges);
                    copy
                })
                .collect(),
        }
    }
}
