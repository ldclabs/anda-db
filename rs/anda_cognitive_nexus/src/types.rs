//! # Types Module
//!
//! This module defines core types and data structures for the cognitive nexus system,
//! including primary keys, query contexts, and result structures for managing
//! concepts and propositions in the knowledge graph.
//!
//! ## Key Components
//!
//! - **Primary Keys**: `ConceptPK`, `PropositionPK`, and `EntityPK` for entity identification
//! - **Query System**: `QueryContext` and `QueryCache` for query execution and caching
//! - **Result Types**: `PropositionsMatchResult` and `GraphPath` for query results
//! - **Target Types**: `TargetEntities` for specifying query targets

use anda_db_utils::UniqueVec;
use anda_kip::*;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use std::{fmt, hash::Hash, str::FromStr, sync::Arc};

use crate::entity::*;

/// Primary key for identifying concepts in the cognitive nexus.
///
/// Concepts can be identified either by their numeric ID or by their type and name.
/// This enum provides a unified way to reference concepts across the system.
///
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConceptPK {
    /// Concept identified by its numeric ID
    ID(u64),
    /// Concept identified by its type and name
    Object { r#type: String, name: String },
}

impl fmt::Display for ConceptPK {
    /// Formats the concept primary key for display.
    ///
    /// # Format
    /// - ID variant: `{id: "concept:<id>"}`
    /// - Object variant: `{type: "<type>", name: "<name>"}`
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // `{id: "<id>"}`
            ConceptPK::ID(id) => write!(f, "{{id: {:?}}}", EntityID::Concept(*id)),
            // `{type: "<type>", name: "<name>"}`
            ConceptPK::Object { r#type, name } => {
                write!(f, "{{type: {:?}, name: {:?}}}", r#type, name)
            }
        }
    }
}

impl TryFrom<ConceptMatcher> for ConceptPK {
    type Error = KipError;

    /// Converts a `ConceptMatcher` from the KIP protocol into a `ConceptPK`.
    ///
    /// # Arguments
    /// * `value` - The concept matcher to convert
    ///
    /// # Returns
    /// * `Ok(ConceptPK)` - Successfully converted primary key
    /// * `Err(KipError)` - If the matcher is invalid or unsupported
    ///
    /// # Errors
    /// - `KipErrorCode::InvalidSyntax` - If the ID string cannot be parsed or the matcher type is unsupported
    fn try_from(value: ConceptMatcher) -> Result<Self, Self::Error> {
        match value {
            ConceptMatcher::ID(id) => {
                let id = EntityID::from_str(&id).map_err(KipError::invalid_syntax)?;
                match id {
                    EntityID::Concept(id) => Ok(ConceptPK::ID(id)),
                    _ => Err(KipError::invalid_syntax(format!(
                        "ConceptMatcher::ID must be a Concept ID, got: {id:?}"
                    ))),
                }
            }
            ConceptMatcher::Object { r#type, name } => Ok(ConceptPK::Object { r#type, name }),
            _ => Err(KipError::invalid_syntax(format!(
                "ConceptMatcher must be either ID or Object, got: {value:?}"
            ))),
        }
    }
}

/// Primary key for identifying propositions in the cognitive nexus.
///
/// Propositions represent relationships between entities and can be identified
/// either by their ID and predicate, or by their subject-predicate-object structure.
///
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PropositionPK {
    /// Proposition identified by its numeric ID and predicate
    ID(u64, String),
    /// Proposition identified by its subject, predicate, and object
    Object {
        subject: Box<EntityPK>,
        predicate: String,
        object: Box<EntityPK>,
    },
}

impl fmt::Display for PropositionPK {
    /// Formats the proposition primary key for display.
    ///
    /// # Format
    /// - ID variant: `(id: "proposition:<id>:<predicate>")`
    /// - Object variant: `(<subject>, "<predicate>", <object>)`
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // `(id: "<link_id>")`
            PropositionPK::ID(id, predicate) => write!(
                f,
                "(id: {:?})",
                EntityID::Proposition(*id, predicate.clone()),
            ),
            // `(?subject, "<predicate>", ?object)`
            PropositionPK::Object {
                subject,
                predicate,
                object,
            } => write!(f, "({}, {:?}, {})", subject, predicate, object),
        }
    }
}

impl TryFrom<PropositionMatcher> for PropositionPK {
    type Error = KipError;

    /// Converts a `PropositionMatcher` from the KIP protocol into a `PropositionPK`.
    ///
    /// # Arguments
    /// * `value` - The proposition matcher to convert
    ///
    /// # Returns
    /// * `Ok(PropositionPK)` - Successfully converted primary key
    /// * `Err(KipError)` - If the matcher is invalid or unsupported
    ///
    /// # Errors
    /// - `KipErrorCode::InvalidSyntax` - If the ID string cannot be parsed, matcher type is unsupported, or predicate is not literal
    fn try_from(value: PropositionMatcher) -> Result<Self, Self::Error> {
        match value {
            PropositionMatcher::ID(id) => {
                let id = EntityID::from_str(&id).map_err(KipError::invalid_syntax)?;
                match id {
                    EntityID::Proposition(id, predicate) => Ok(PropositionPK::ID(id, predicate)),
                    _ => Err(KipError::invalid_syntax(format!(
                        "PropositionMatcher::ID must be a Proposition ID, got: {id:?}"
                    ))),
                }
            }
            PropositionMatcher::Object {
                subject,
                predicate,
                object,
            } => {
                let subject = Box::new(EntityPK::try_from(subject)?);
                let object = Box::new(EntityPK::try_from(object)?);
                let predicate = match predicate {
                    PredTerm::Literal(value) => value,
                    val => {
                        return Err(KipError::invalid_syntax(format!(
                            "PropositionMatcher::Object's predicate must be a literal string, got: {val:?}"
                        )));
                    }
                };

                Ok(PropositionPK::Object {
                    subject,
                    predicate,
                    object,
                })
            }
        }
    }
}

/// Unified primary key for any entity in the cognitive nexus.
///
/// This enum provides a common interface for working with both concepts and propositions,
/// enabling polymorphic operations across different entity types.
///
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EntityPK {
    /// A concept entity
    Concept(ConceptPK),
    /// A proposition entity
    Proposition(PropositionPK),
}

impl fmt::Display for EntityPK {
    /// Formats the entity primary key by delegating to the underlying type's display implementation.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EntityPK::Concept(pk) => write!(f, "{}", pk),
            EntityPK::Proposition(pk) => write!(f, "{}", pk),
        }
    }
}

impl TryFrom<TargetTerm> for EntityPK {
    type Error = KipError;

    /// Converts a `TargetTerm` from the KIP protocol into an `EntityPK`.
    ///
    /// # Arguments
    /// * `value` - The target term to convert
    ///
    /// # Returns
    /// * `Ok(EntityPK)` - Successfully converted entity primary key
    /// * `Err(KipError)` - If the target term is invalid or unsupported
    fn try_from(value: TargetTerm) -> Result<Self, Self::Error> {
        match value {
            TargetTerm::Concept(value) => Ok(EntityPK::Concept(ConceptPK::try_from(value)?)),
            TargetTerm::Proposition(value) => {
                Ok(EntityPK::Proposition(PropositionPK::try_from(*value)?))
            }
            _ => Err(KipError::invalid_syntax(format!(
                "TargetTerm must be either Concept or Proposition, got: {value:?}"
            ))),
        }
    }
}

impl From<EntityID> for EntityPK {
    /// Converts an `EntityID` into an `EntityPK`.
    ///
    /// This conversion always succeeds as every `EntityID` has a corresponding `EntityPK` representation.
    fn from(value: EntityID) -> Self {
        match value {
            EntityID::Concept(id) => EntityPK::Concept(ConceptPK::ID(id)),
            EntityID::Proposition(id, pred) => EntityPK::Proposition(PropositionPK::ID(id, pred)),
        }
    }
}

/// Query execution context for managing variable bindings and caching.
///
/// This structure maintains the state during query execution, including:
/// - Variable bindings for entities and predicates
/// - Shared cache for loaded entities to avoid redundant database access
///
/// # Usage
///
/// The query context is passed through query execution pipelines to maintain
/// consistency and performance through caching.
#[derive(Clone, Debug, Default)]
pub struct QueryContext {
    /// Variable name to entity ID mappings
    ///
    /// Maps variable names (e.g., "?person", "?location") to lists of entity IDs
    /// that match the variable's constraints in the current query context.
    pub entities: FxHashMap<String, UniqueVec<EntityID>>,

    /// Variable name to predicate mappings
    ///
    /// Maps variable names to lists of predicate strings that match
    /// the variable's constraints in the current query context.
    pub predicates: FxHashMap<String, UniqueVec<String>>,

    /// Shared cache for loaded entities
    ///
    /// Provides thread-safe caching of concepts and propositions to avoid
    /// redundant database queries during query execution.
    pub cache: Arc<QueryCache>,

    /// Compiled regex cache for FILTER(REGEX(...)) evaluation.
    ///
    /// This avoids recompiling the same regex pattern for each row
    /// during a single query execution.
    pub regex_cache: FxHashMap<String, regex::Regex>,
}

/// Thread-safe cache for storing loaded entities during query execution.
///
/// This cache improves performance by avoiding redundant database queries
/// for the same entities within a query execution context.
///
/// # Thread Safety
///
/// Uses `RwLock` to allow concurrent reads while ensuring exclusive writes,
/// making it safe to use across multiple threads during parallel query execution.
#[derive(Debug, Default)]
pub struct QueryCache {
    /// Cache for loaded concept entities
    ///
    /// Maps concept IDs to their loaded `Concept` instances.
    pub concepts: RwLock<FxHashMap<u64, Concept>>,

    /// Cache for loaded proposition entities
    ///
    /// Maps proposition IDs to their loaded `Proposition` instances.
    pub propositions: RwLock<FxHashMap<u64, Proposition>>,
}

/// Specifies the target entities for query operations.
///
/// This enum allows queries to target different subsets of entities
/// in the knowledge graph, enabling efficient query planning and execution.
///
/// # Variants
///
/// - `Any`: Target all entities (concepts and propositions)
/// - `AnyPropositions`: Target only proposition entities
/// - `IDs`: Target specific entities by their IDs
#[derive(Debug)]
pub enum TargetEntities {
    /// Target all entities in the knowledge graph
    Any,
    /// Target only proposition entities
    AnyPropositions,
    /// Target specific entities identified by their IDs
    IDs(Vec<EntityID>),
}

/// Result structure for proposition matching operations.
///
/// Collects all entities and predicates that match during proposition queries,
/// providing comprehensive information about the matching results.
///
/// # Usage
///
/// This structure is typically populated during query execution and provides
/// access to all matched components of propositions for further processing.
#[derive(Default)]
pub struct PropositionsMatchResult {
    /// List of matched proposition entity IDs
    pub matched_propositions: UniqueVec<EntityID>,
    /// List of matched subject entity IDs
    pub matched_subjects: UniqueVec<EntityID>,
    /// List of matched object entity IDs
    pub matched_objects: UniqueVec<EntityID>,
    /// List of matched predicate strings
    pub matched_predicates: UniqueVec<String>,
}

impl PropositionsMatchResult {
    /// Adds a matching proposition and its components to the result.
    ///
    /// This method ensures that duplicate entries are not added to the result collections
    /// by using the `push_nx` helper function.
    ///
    /// # Arguments
    ///
    /// * `subject` - The subject entity ID of the matched proposition
    /// * `object` - The object entity ID of the matched proposition
    /// * `predicates` - List of predicates for this proposition
    /// * `proposition_id` - The numeric ID of the proposition
    ///
    /// # Behavior
    ///
    /// - Adds subject and object to their respective collections (if not already present)
    /// - Creates proposition entity IDs for each predicate and adds them
    /// - Adds each predicate string to the predicates collection
    pub fn add_match(
        &mut self,
        subject: EntityID,
        object: EntityID,
        predicates: Vec<String>,
        proposition_id: u64,
    ) {
        self.matched_subjects.push(subject);
        self.matched_objects.push(object);

        for pred in predicates {
            let id = EntityID::Proposition(proposition_id, pred.clone());
            self.matched_propositions.push(id);
            self.matched_predicates.push(pred);
        }
    }
}

/// Represents a path through the knowledge graph.
///
/// A graph path connects two entities through a series of propositions,
/// providing information about the relationship chain and path length.
///
/// # Usage
///
/// Graph paths are typically used in:
/// - Path finding algorithms
/// - Relationship analysis
/// - Graph traversal operations
/// - Shortest path queries
///
#[derive(Clone, Debug)]
pub struct GraphPath {
    /// The starting entity of the path
    pub start: EntityID,
    /// The ending entity of the path
    pub end: EntityID,
    /// The sequence of propositions that form the path
    ///
    /// Each proposition represents an edge in the path from start to end.
    /// The order of propositions matters as it represents the traversal sequence.
    pub propositions: UniqueVec<EntityID>,
    /// The number of hops (edges) in the path
    ///
    /// This should equal the length of the `propositions` vector.
    /// Useful for path length comparisons and shortest path algorithms.
    pub hops: u16,
}
