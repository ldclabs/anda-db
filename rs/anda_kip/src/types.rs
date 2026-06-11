//! # KIP Entity Types
//!
use serde::{Deserialize, Serialize};

use crate::{
    KipError,
    ast::{Json, Map},
};

/// Reserved system metadata namespace prefix.
///
/// Metadata keys beginning with `_` are maintained exclusively by the engine:
/// KML statements cannot set or delete them (`KIP_2002`), while KQL reads them
/// like ordinary metadata (e.g., `?x.metadata._version`).
pub static RESERVED_METADATA_PREFIX: &str = "_";

/// Monotonic mutation counter (starts at 1); target of `EXPECT VERSION`. REQUIRED.
pub static METADATA_VERSION: &str = "_version";

/// ISO 8601 timestamp of the element's last mutation (engine truth). RECOMMENDED.
pub static METADATA_UPDATED_AT: &str = "_updated_at";

/// Transient normalized `SEARCH` relevance score `[0, 1]`; never persisted. OPTIONAL.
pub static METADATA_SCORE: &str = "_score";

/// Provenance trail (`"<Type>:<name>"` entries) left on the target by `MERGE`.
pub static METADATA_MERGED_FROM: &str = "_merged_from";

/// Returns true if the metadata key belongs to the reserved `_` namespace
/// (engine-maintained, read-only to KML).
pub fn is_reserved_metadata_key(key: &str) -> bool {
    key.starts_with(RESERVED_METADATA_PREFIX)
}

/// Enumeration of entity types for type checking and validation.
///
/// This enum is used to distinguish between different entity types
/// in contexts where type information is needed without the full entity data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntityType {
    /// Represents a concept node entity type.
    ConceptNode,
    /// Represents a proposition link entity type.
    PropositionLink,
}

/// Represents a knowledge entity that can be either a concept node or a proposition link.
///
/// This enum serves as the main container for all knowledge entities in the system,
/// supporting both conceptual entities (nodes) and relational entities (links).
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "_type")]
pub enum Entity {
    /// A concept node representing a knowledge concept.
    ConceptNode(ConceptNode),
    /// A proposition link representing a relationship between concepts.
    PropositionLink(PropositionLink),
}

/// A borrowed reference version of [`Entity`] for efficient serialization without cloning.
///
/// This enum provides the same variants as [`Entity`] but uses borrowed references
/// to avoid unnecessary memory allocations during serialization.
#[derive(Debug, Serialize)]
#[serde(tag = "_type")]
pub enum EntityRef<'a> {
    /// A borrowed reference to a concept node.
    ConceptNode(ConceptNodeRef<'a>),
    /// A borrowed reference to a proposition link.
    PropositionLink(PropositionLinkRef<'a>),
}

/// Represents a concept node in the knowledge graph.
///
/// A concept node is a fundamental unit of knowledge that represents
/// a specific concept, entity, or idea. It contains identifying information,
/// type classification, and optional attributes and metadata.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ConceptNode {
    /// Unique identifier for the concept node.
    pub id: String,
    /// The type classification of this concept.
    pub r#type: String,
    /// Human-readable name of the concept.
    pub name: String,

    /// Additional attributes associated with this concept.
    /// Skipped during serialization if empty.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub attributes: Map<String, Json>,

    /// Metadata information about this concept.
    /// Skipped during serialization if empty.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub metadata: Map<String, Json>,
}

/// A borrowed reference version of [`ConceptNode`] for efficient serialization.
///
/// This struct provides the same fields as [`ConceptNode`] but uses borrowed references
/// to avoid unnecessary memory allocations during serialization operations.
#[derive(Debug, Serialize)]
pub struct ConceptNodeRef<'a> {
    /// Reference to the unique identifier.
    pub id: &'a str,
    /// Reference to the type classification.
    pub r#type: &'a str,
    /// Reference to the human-readable name.
    pub name: &'a str,
    /// Reference to the additional attributes.
    #[serde(skip_serializing_if = "Map::is_empty")]
    pub attributes: &'a Map<String, Json>,

    /// Reference to the metadata information.
    #[serde(skip_serializing_if = "Map::is_empty")]
    pub metadata: &'a Map<String, Json>,
}

/// Represents a proposition link in the knowledge graph.
///
/// A proposition link defines a relationship between two concepts (subject and object)
/// through a predicate. It forms the relational backbone of the knowledge graph
/// by connecting concepts with meaningful relationships.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct PropositionLink {
    /// Unique identifier for the proposition link.
    pub id: String,
    /// The subject concept ID in the relationship.
    pub subject: String,
    /// The object concept ID in the relationship.
    pub object: String,
    /// The predicate defining the type of relationship.
    pub predicate: String,

    /// Additional attributes associated with this relationship.
    /// Skipped during serialization if empty.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub attributes: Map<String, Json>,

    /// Metadata information about this relationship.
    /// Skipped during serialization if empty.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub metadata: Map<String, Json>,
}

/// A borrowed reference version of [`PropositionLink`] for efficient serialization.
///
/// This struct provides the same fields as [`PropositionLink`] but uses borrowed references
/// to avoid unnecessary memory allocations during serialization operations.
#[derive(Debug, Serialize)]
pub struct PropositionLinkRef<'a> {
    /// Reference to the unique identifier.
    pub id: &'a str,
    /// Reference to the subject concept ID.
    pub subject: &'a str,
    /// Reference to the object concept ID.
    pub object: &'a str,
    /// Reference to the predicate.
    pub predicate: &'a str,

    /// Reference to the additional attributes.
    #[serde(skip_serializing_if = "Map::is_empty")]
    pub attributes: &'a Map<String, Json>,

    /// Reference to the metadata information.
    #[serde(skip_serializing_if = "Map::is_empty")]
    pub metadata: &'a Map<String, Json>,
}

/// The result of an upsert operation in the knowledge graph.
///
/// This struct represents the outcome of an upsert operation, which includes
/// the number of blocks affected, the concept nodes upserted, and the proposition links upserted.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct UpsertResult {
    /// The number of blocks affected by the upsert operation.
    pub blocks: usize,
    /// The concept node IDs upserted during the operation.
    pub upsert_concept_nodes: Vec<String>,
    /// The proposition link IDs upserted during the operation.
    pub upsert_proposition_links: Vec<String>,
}

/// Validates a dot notation path for accessing entity properties.
///
/// This function ensures that the provided path is valid for the given entity type.
/// It supports accessing top-level properties and nested properties within
/// attributes and metadata maps.
///
/// # Arguments
///
/// * `path` - A slice of strings representing the dot notation path components
/// * `et` - The entity type to validate the path against
///
/// # Returns
///
/// * `Ok(())` if the path is valid for the given entity type
/// * `Err(KipError)` with InvalidSyntax code if the path is invalid
///
/// # Examples
///
/// Valid paths for ConceptNode:
/// - `[]` (empty path)
/// - `["id"]`, `["type"]`, `["name"]`, `["attributes"]`, `["metadata"]`
/// - `["attributes", "some_key"]`, `["metadata", "some_key"]`
///
/// Valid paths for PropositionLink:
/// - `[]` (empty path)
/// - `["id"]`, `["subject"]`, `["object"]`, `["predicate"]`, `["attributes"]`, `["metadata"]`
/// - `["attributes", "some_key"]`, `["metadata", "some_key"]`
pub fn validate_dot_path_var(path: &[String], et: EntityType) -> Result<(), KipError> {
    match path.len() {
        0 => Ok(()),
        1 => match path[0].as_str() {
            "id" | "attributes" | "metadata" => Ok(()),
            "type" | "name" if et == EntityType::ConceptNode => Ok(()),
            "subject" | "predicate" | "object" if et == EntityType::PropositionLink => Ok(()),
            _ => Err(KipError::invalid_syntax(format!(
                "Dot notation path: invalid path component {:?}",
                path[0]
            ))),
        },
        2 => match (path[0].as_str(), path[1].as_str()) {
            ("attributes", _) | ("metadata", _) => Ok(()),
            _ => Err(KipError::invalid_syntax(format!(
                "Dot notation path: invalid path components {:?}",
                path.join("."),
            ))),
        },
        _ => Err(KipError::invalid_syntax(format!(
            "Dot notation path: too many components in path {:?}",
            path.join(".")
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entity() {
        let concept = Entity::ConceptNode(ConceptNode {
            id: "C:123".to_string(),
            r#type: "Drug".to_string(),
            name: "Test".to_string(),
            attributes: Map::new(),
            metadata: Map::new(),
        });
        let rt = serde_json::to_string(&concept).unwrap();
        assert_eq!(
            rt,
            r#"{"_type":"ConceptNode","id":"C:123","type":"Drug","name":"Test"}"#
        );
        let rt: Entity = serde_json::from_str(&rt).unwrap();
        assert_eq!(rt, concept);

        let prop = Entity::PropositionLink(PropositionLink {
            id: "P:123:treats".to_string(),
            subject: "C:123".to_string(),
            object: "C:456".to_string(),
            predicate: "treats".to_string(),
            attributes: Map::new(),
            metadata: Map::new(),
        });
        let rt = serde_json::to_string(&prop).unwrap();
        assert_eq!(
            rt,
            r#"{"_type":"PropositionLink","id":"P:123:treats","subject":"C:123","object":"C:456","predicate":"treats"}"#
        );
        let rt: Entity = serde_json::from_str(&rt).unwrap();
        assert_eq!(rt, prop);
    }

    #[test]
    fn test_entity_refs_and_dot_path_validation() {
        let mut attributes = Map::new();
        attributes.insert("score".to_string(), serde_json::json!(7));
        let metadata = Map::new();

        let concept_ref = EntityRef::ConceptNode(ConceptNodeRef {
            id: "C:1",
            r#type: "Drug",
            name: "Aspirin",
            attributes: &attributes,
            metadata: &metadata,
        });
        let concept_json = serde_json::to_value(concept_ref).unwrap();
        assert_eq!(concept_json["_type"], "ConceptNode");
        assert_eq!(concept_json["attributes"]["score"], 7);

        let prop_ref = EntityRef::PropositionLink(PropositionLinkRef {
            id: "P:1:treats",
            subject: "C:1",
            object: "C:2",
            predicate: "treats",
            attributes: &metadata,
            metadata: &metadata,
        });
        let prop_json = serde_json::to_value(prop_ref).unwrap();
        assert_eq!(prop_json["_type"], "PropositionLink");
        assert_eq!(prop_json["predicate"], "treats");

        assert!(validate_dot_path_var(&[], EntityType::ConceptNode).is_ok());
        assert!(validate_dot_path_var(&["name".to_string()], EntityType::ConceptNode).is_ok());
        assert!(
            validate_dot_path_var(&["subject".to_string()], EntityType::PropositionLink).is_ok()
        );
        assert!(
            validate_dot_path_var(
                &["attributes".to_string(), "score".to_string()],
                EntityType::ConceptNode,
            )
            .is_ok()
        );
        assert!(validate_dot_path_var(&["subject".to_string()], EntityType::ConceptNode).is_err());
        assert!(
            validate_dot_path_var(
                &["name".to_string(), "first".to_string()],
                EntityType::ConceptNode,
            )
            .is_err()
        );
        assert!(
            validate_dot_path_var(
                &[
                    "attributes".to_string(),
                    "score".to_string(),
                    "extra".to_string()
                ],
                EntityType::ConceptNode,
            )
            .is_err()
        );
    }
}
