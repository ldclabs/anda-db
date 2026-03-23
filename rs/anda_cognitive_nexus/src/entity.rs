//! # Entity Module
//!
//! This module defines the core entity types for the Anda Cognitive Nexus system.
//! It provides data structures and operations for representing knowledge as concepts
//! and propositions in a semantic knowledge graph.
//!
//! ## Overview
//!
//! The cognitive nexus represents knowledge through two primary entity types:
//! - **Concepts**: Fundamental units representing ideas, objects, or categories
//! - **Propositions**: Relationships between concepts through predicates
//!
//! ## Entity Identification
//!
//! All entities are uniquely identified using the `EntityID` enum, which provides
//! type-safe references with human-readable string representations:
//! - Concepts: `"C:{id}"` (e.g., "C:123")
//! - Propositions: `"P:{id}:{predicate}"` (e.g., "P:456:hasProperty")
//!

use anda_db_schema::{
    AndaDBSchema, FieldEntry, FieldKey, FieldType, FieldTyped, FieldValue, Json, Map, Schema,
    SchemaError,
};
use anda_kip::{ConceptNode, ConceptNodeRef, EntityRef, PropositionLinkRef};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    fmt,
    str::FromStr,
};

/// Represents a concept entity in the cognitive nexus.
///
/// A concept is a fundamental unit of knowledge that represents an idea, object, or category.
/// It serves as a node in the knowledge graph and can be connected to other concepts
/// through propositions. Each concept has a unique identifier, type classification,
/// human-readable name, and associated data stored as attributes and metadata.
///
/// ## Structure
///
/// - **ID**: Unique 64-bit identifier for database storage and referencing
/// - **Type**: Classification string (e.g., "Person", "Location", "Abstract")
/// - **Name**: Human-readable label for display and search
/// - **Attributes**: Domain-specific properties and characteristics
/// - **Metadata**: System-level information (timestamps, provenance, etc.)
///
#[derive(Debug, Clone, Default, Deserialize, Serialize, AndaDBSchema)]
pub struct Concept {
    /// Unique identifier for the concept.
    ///
    /// This is a 64-bit unsigned integer that serves as the primary key
    /// for database storage and cross-references. It should be unique
    /// across all concepts in the system.
    pub _id: u64,

    /// Type classification of the concept.
    ///
    /// This string categorizes the concept into a semantic type or class.
    /// Common examples include "Person", "Organization", "Location", "Event",
    /// "Abstract", etc. The type helps with reasoning and query optimization.
    pub r#type: String,

    /// Human-readable name of the concept.
    ///
    /// This is the primary label used for display, search, and human interaction.
    /// It should be descriptive and unique within its type when possible.
    /// Examples: "Albert Einstein", "Golden Gate Bridge", "Machine Learning".
    pub name: String,

    /// Key-value attributes associated with the concept.
    ///
    /// Domain-specific properties that define the characteristics of this concept.
    /// The structure is flexible to accommodate different domains and use cases.
    /// Examples: {"birth_date": "1879-03-14", "nationality": "German"}.
    pub attributes: Map<String, Json>,

    /// Additional metadata for the concept.
    ///
    /// System-level information such as creation timestamps, data provenance,
    /// confidence scores, or processing flags. This data is typically used
    /// by the system rather than domain applications.
    pub metadata: Map<String, Json>,
}

impl Concept {
    /// Returns the entity ID for this concept.
    ///
    /// Creates an `EntityID::Concept` variant that can be used for referencing
    /// this concept in propositions, queries, and other operations.
    ///
    /// # Returns
    ///
    /// An `EntityID::Concept` variant containing the concept's unique ID.
    ///
    pub fn entity_id(&self) -> EntityID {
        EntityID::Concept(self._id)
    }

    /// Converts the concept to a JSON representation as a concept node reference.
    ///
    /// Creates a JSON object that represents this concept as a node in the knowledge
    /// graph, using borrowed references to avoid unnecessary cloning. This is useful
    /// for serialization and API responses where the original concept data is still needed.
    ///
    /// # Returns
    ///
    /// A JSON object containing the concept node with string references to its fields.
    /// The structure follows the [`ConceptNodeRef`] format from the KIP specification.
    ///
    /// # Examples
    ///
    pub fn to_concept_node(&self) -> Json {
        json!(EntityRef::ConceptNode(ConceptNodeRef {
            id: self.entity_id().to_string().as_str(),
            r#type: &self.r#type,
            name: &self.name,
            attributes: &self.attributes,
            metadata: &self.metadata,
        }))
    }

    /// Consumes the concept and converts it into a concept node.
    ///
    /// Takes ownership of the concept and transforms it into a `ConceptNode`
    /// with owned data. This is useful when the original concept is no longer
    /// needed and you want to avoid cloning large attribute maps.
    ///
    /// # Returns
    ///
    /// A [`ConceptNode`] containing the owned data from this concept.
    /// The node can be used independently without references to the original concept.
    ///
    pub fn into_concept_node(self) -> ConceptNode {
        ConceptNode {
            id: self.entity_id().to_string(),
            r#type: self.r#type,
            name: self.name,
            attributes: self.attributes,
            metadata: self.metadata,
        }
    }
}

/// Represents a proposition entity that defines relationships between concepts.
///
/// A proposition establishes semantic connections between a subject and object through
/// one or more predicates, forming the edges in the knowledge graph. Each proposition
/// can represent multiple relationship types simultaneously and includes properties
/// specific to each predicate.
///
/// ## Structure
///
/// - **Subject**: The source entity in the relationship (EntityID)
/// - **Object**: The target entity in the relationship (EntityID)
/// - **Predicates**: Set of relationship types (e.g., "hasProperty", "isA", "locatedIn")
/// - **Properties**: Predicate-specific attributes and metadata
///
/// ## Multi-Predicate Support
///
/// A single proposition can represent multiple relationships between the same
/// subject-object pair, each with its own properties. This reduces storage overhead
/// and maintains semantic coherence.
///
#[derive(Debug, Clone, Deserialize, Serialize, AndaDBSchema)]
pub struct Proposition {
    /// Unique identifier for the proposition.
    ///
    /// This 64-bit unsigned integer serves as the primary key for database storage.
    /// Multiple predicate-specific entity IDs can be derived from this base ID.
    pub _id: u64,

    /// The subject entity ID in the proposition relationship.
    ///
    /// This is the source or "from" entity in the directed relationship.
    /// It can reference either a concept or another proposition, allowing
    /// for complex nested relationships and meta-statements.
    #[field_type = "Text"]
    pub subject: EntityID,

    /// The object entity ID in the proposition relationship.
    ///
    /// This is the target or "to" entity in the directed relationship.
    /// Like the subject, it can reference concepts or propositions for
    /// building complex knowledge structures.
    #[field_type = "Text"]
    pub object: EntityID,

    /// Set of predicates that define the relationship types.
    ///
    /// Each predicate represents a specific type of relationship between
    /// the subject and object. Using a set ensures uniqueness and allows
    /// for efficient membership testing. Common predicates include:
    /// - "is_a" (type relationships)
    /// - "has_property" (attribute relationships)
    /// - "located_in" (spatial relationships)
    /// - "occurred_at" (temporal relationships)
    pub predicates: BTreeSet<String>,

    /// Properties associated with each predicate.
    ///
    /// Maps predicate names to their specific properties, allowing each
    /// relationship type to have its own attributes and metadata.
    /// Predicates without explicit properties use default empty values.
    pub properties: BTreeMap<String, Properties>,
}

impl Proposition {
    /// Returns the entity ID for this proposition with a specific predicate.
    ///
    /// Creates a predicate-specific entity ID that uniquely identifies this
    /// proposition-predicate combination. This allows the same proposition
    /// to be referenced differently for each of its relationship types.
    ///
    /// # Arguments
    ///
    /// * `predicate` - The predicate string to include in the entity ID
    ///
    /// # Returns
    ///
    /// An `EntityID::Proposition` variant containing the proposition ID and predicate.
    ///
    pub fn entity_id(&self, predicate: String) -> EntityID {
        EntityID::Proposition(self._id, predicate)
    }

    /// Converts the proposition to a JSON representation as a proposition link for a specific predicate.
    ///
    /// Creates a JSON object representing this proposition as a link in the knowledge
    /// graph for the specified predicate. If the predicate doesn't exist in this
    /// proposition, returns `None`. Uses borrowed references when possible to avoid
    /// unnecessary data copying.
    ///
    /// # Arguments
    ///
    /// * `predicate` - The predicate to create the link for
    ///
    /// # Returns
    ///
    /// An optional JSON object representing the proposition link, or `None` if
    /// the predicate doesn't exist in this proposition's predicate set.
    ///
    pub fn to_proposition_link(&self, predicate: &str) -> Option<Json> {
        match self.predicates.get(predicate) {
            Some(predicate) => {
                let prop = self
                    .properties
                    .get(predicate)
                    .map(Cow::Borrowed)
                    .unwrap_or_else(|| {
                        Cow::Owned(Properties {
                            attributes: Map::new(),
                            metadata: Map::new(),
                        })
                    });

                Some(json!(EntityRef::PropositionLink(PropositionLinkRef {
                    id: self.entity_id(predicate.to_string()).to_string().as_str(),
                    subject: self.subject.to_string().as_str(),
                    predicate,
                    object: self.object.to_string().as_str(),
                    attributes: &prop.attributes,
                    metadata: &prop.metadata,
                })))
            }
            None => None,
        }
    }

    pub fn to_info(&self, predicate: &str) -> Option<PropositionInfo> {
        self.properties.get(predicate).map(|v| PropositionInfo {
            id: self.entity_id(predicate.to_string()).to_string(),
            subject: self.subject.to_string(),
            object: self.object.to_string(),
            predicate: predicate.to_string(),
            attributes: v.attributes.clone(),
        })
    }
}

/// Properties container for storing attributes and metadata.
///
/// This structure provides a standardized way to store key-value pairs for both
/// domain-specific attributes and system-level metadata. It uses compact field
/// names ("a" for attributes, "m" for metadata) to reduce serialization overhead
/// while maintaining clear semantic separation.
///
#[derive(Debug, Clone, Default, Deserialize, Serialize, FieldTyped)]
pub struct Properties {
    /// Domain-specific attributes for the relationship or entity.
    #[serde(rename = "a")]
    pub attributes: Map<String, Json>,

    /// System-level metadata for tracking and management.
    #[serde(rename = "m")]
    pub metadata: Map<String, Json>,
}

impl From<Properties> for FieldValue {
    /// Converts Properties into a FieldValue for database storage.
    fn from(value: Properties) -> Self {
        FieldValue::Map(BTreeMap::from([
            ("a".into(), value.attributes.into()),
            ("m".into(), value.metadata.into()),
        ]))
    }
}

/// Unique identifier for entities in the cognitive nexus.
///
/// EntityID provides a type-safe way to reference different types of entities
/// within the knowledge graph. It supports two main entity types with distinct
/// string representations for human readability and debugging.
///
/// ## Format Specification
///
/// - **Concepts**: `"C:{id}"` where `{id}` is a 64-bit unsigned integer
/// - **Propositions**: `"P:{id}:{predicate}"` where `{id}` is the proposition ID
///   and `{predicate}` is the specific relationship type
///
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EntityID {
    /// Identifier for a concept entity.
    ///
    /// Contains the unique 64-bit identifier for the concept.
    /// Concepts represent nodes in the knowledge graph.
    Concept(u64),

    /// Identifier for a proposition entity with a specific predicate.
    ///
    /// Contains both the proposition's unique ID and the specific predicate
    /// name, allowing the same proposition to be referenced differently
    /// for each of its relationship types.
    Proposition(u64, String),
}

impl fmt::Display for EntityID {
    /// Formats the EntityID as a string.
    ///
    /// # Format
    ///
    /// * Concepts: "C:{id}"
    /// * Propositions: "P:{id}:{predicate}"
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EntityID::Concept(id) => write!(f, "C:{}", id),
            EntityID::Proposition(id, pred) => write!(f, "P:{}:{}", id, pred),
        }
    }
}

impl FromStr for EntityID {
    type Err = String;

    /// Parses a string into an EntityID.
    ///
    /// # Arguments
    ///
    /// * `s` - String to parse, expected format: "C:{id}" or "P:{id}:{predicate}"
    ///
    /// # Returns
    ///
    /// Result containing the parsed EntityID or an error message.
    ///
    /// # Errors
    ///
    /// Returns an error if the string doesn't match the expected format.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(id) = s.strip_prefix("C:") {
            id.parse::<u64>()
                .map(EntityID::Concept)
                .map_err(|_| format!("Invalid Concept ID, expected format 'C:<u64>', got {s:?}"))
        } else if let Some(id) = s.strip_prefix("P:") {
            let parts: Vec<&str> = id.split(':').collect();
            if parts.len() != 2 {
                return Err(format!(
                    "Invalid Proposition ID, expected format 'P:<u64>:<predicate>', got {s:?}"
                ));
            }

            let id = parts[0].parse::<u64>().map_err(|_| {
                format!("Invalid Proposition ID, expected format 'P:<u64>:<predicate>', got {s:?}")
            })?;

            Ok(EntityID::Proposition(id, parts[1].to_string()))
        } else {
            Err(format!("EntityID must start with 'C:' or 'P:', got {s:?}"))
        }
    }
}

impl TryFrom<&str> for EntityID {
    type Error = String;

    /// Attempts to convert a string slice into an EntityID.
    ///
    /// # Arguments
    ///
    /// * `value` - String slice to convert
    ///
    /// # Returns
    ///
    /// Result containing the parsed EntityID or an error message.
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        EntityID::from_str(value)
    }
}

impl Serialize for EntityID {
    /// Serializes the EntityID as a string.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

/// Visitor for deserializing EntityID from strings.
struct EntityIDVisitor;

impl serde::de::Visitor<'_> for EntityIDVisitor {
    type Value = EntityID;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a string")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        EntityID::from_str(s).map_err(|err| E::custom(err))
    }
}

impl<'de> Deserialize<'de> for EntityID {
    /// Deserializes an EntityID from a string.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(EntityIDVisitor)
    }
}

/// Information about a knowledge domain.
///
/// Contains comprehensive metadata about a knowledge domain, including its
/// conceptual structure and relationship types. This information is used for
/// domain-specific reasoning, query optimization, and knowledge organization.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct DomainInfo {
    pub name: String,
    pub description: String,

    /// Key concept types in this domain.
    pub key_concept_types: Vec<String>,

    /// Key proposition types in this domain.
    pub key_proposition_types: Vec<String>,
}

impl From<&Concept> for DomainInfo {
    fn from(concept: &Concept) -> Self {
        Self {
            name: concept.name.clone(),
            description: concept
                .attributes
                .get("description")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            key_concept_types: Vec::new(),
            key_proposition_types: Vec::new(),
        }
    }
}

impl From<Concept> for DomainInfo {
    fn from(concept: Concept) -> Self {
        Self {
            name: concept.name,
            description: concept
                .attributes
                .get("description")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            key_concept_types: Vec::new(),
            key_proposition_types: Vec::new(),
        }
    }
}

/// Simplified information about a concept.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ConceptInfo {
    pub id: String,
    pub r#type: String,
    pub name: String,
    pub attributes: Map<String, Json>,
}

impl From<&Concept> for ConceptInfo {
    fn from(concept: &Concept) -> Self {
        Self {
            id: concept.entity_id().to_string(),
            r#type: concept.r#type.clone(),
            name: concept.name.clone(),
            attributes: concept.attributes.clone(),
        }
    }
}

impl From<Concept> for ConceptInfo {
    fn from(concept: Concept) -> Self {
        Self {
            id: concept.entity_id().to_string(),
            r#type: concept.r#type,
            name: concept.name,
            attributes: concept.attributes,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PropositionInfo {
    pub id: String,
    pub subject: String,
    pub object: String,
    pub predicate: String,
    pub attributes: Map<String, Json>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entity() {}

    #[test]
    fn test_concept_node_schema() {
        let schema = Concept::schema().unwrap();
        assert_eq!(schema.len(), 5);
        assert_eq!(schema.get_field("_id").unwrap().r#type(), &FieldType::U64);
        assert_eq!(schema.get_field("type").unwrap().r#type(), &FieldType::Text);
        assert_eq!(schema.get_field("name").unwrap().r#type(), &FieldType::Text);
        assert_eq!(
            schema.get_field("attributes").unwrap().r#type(),
            &FieldType::Map(std::collections::BTreeMap::from([(
                "*".into(),
                FieldType::Json
            )]))
        );
        assert_eq!(
            schema.get_field("metadata").unwrap().r#type(),
            &FieldType::Map(std::collections::BTreeMap::from([(
                "*".into(),
                FieldType::Json
            )]))
        );
    }

    #[test]
    fn test_proposition_link_schema() {
        let schema = Proposition::schema().unwrap();
        assert_eq!(schema.len(), 5);
        assert_eq!(schema.get_field("_id").unwrap().r#type(), &FieldType::U64);
        assert_eq!(
            schema.get_field("subject").unwrap().r#type(),
            &FieldType::Text
        );
        assert_eq!(
            schema.get_field("object").unwrap().r#type(),
            &FieldType::Text
        );
        assert_eq!(
            schema.get_field("predicates").unwrap().r#type(),
            &FieldType::Array(vec![FieldType::Text])
        );
        assert_eq!(
            schema.get_field("properties").unwrap().r#type(),
            &FieldType::Map(std::collections::BTreeMap::from([(
                "*".into(),
                FieldType::Map(std::collections::BTreeMap::from([
                    (
                        "a".into(),
                        FieldType::Map(std::collections::BTreeMap::from([(
                            "*".into(),
                            FieldType::Json
                        )]))
                    ),
                    (
                        "m".into(),
                        FieldType::Map(std::collections::BTreeMap::from([(
                            "*".into(),
                            FieldType::Json
                        )]))
                    )
                ]))
            )]))
        );
    }
}
