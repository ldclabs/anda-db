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
use rustc_hash::{FxHashMap, FxHashSet};
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
            TargetTerm::Concept { matcher, .. } => {
                Ok(EntityPK::Concept(ConceptPK::try_from(matcher)?))
            }
            TargetTerm::Proposition { matcher, .. } => {
                Ok(EntityPK::Proposition(PropositionPK::try_from(*matcher)?))
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

    /// Group relationships between variables.
    ///
    /// Key: `(group_var, member_var)` — e.g., `("d", "n")` for the pattern
    /// `(?n, "belongs_to_domain", ?d)` where each domain ?d groups its members ?n.
    /// Value: maps each group entity ID to its related member entity IDs.
    ///
    /// This enables per-group aggregation in FIND clauses like `FIND(?d.name, COUNT(?n))`.
    pub groups: FxHashMap<(String, String), FxHashMap<EntityID, UniqueVec<EntityID>>>,

    /// Variables whose field-level values participate in row-sensitive filtering.
    pub row_sensitive_vars: FxHashSet<String>,

    /// Row-level bindings produced by proposition clauses.
    ///
    /// These preserve the tuple relationship between a proposition link and its
    /// subject/object bindings for result materialization that needs row-level
    /// ordering or filtering across variables.
    pub relations: Vec<QueryRelationBinding>,

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

/// Variables attached to the rows emitted by one proposition clause.
#[derive(Clone, Debug)]
pub struct QueryRelationBinding {
    pub proposition_var: Option<String>,
    pub subject_var: Option<String>,
    pub predicate_var: Option<String>,
    pub object_var: Option<String>,
    pub rows: Vec<QueryRelationRow>,
}

/// One concrete proposition-clause match.
#[derive(Clone, Debug)]
pub struct QueryRelationRow {
    pub proposition: EntityID,
    pub subject: EntityID,
    pub predicate: String,
    pub object: EntityID,
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
    /// Per-subject grouping: maps each subject to its matched objects
    pub subject_to_objects: FxHashMap<EntityID, UniqueVec<EntityID>>,
    /// Per-object grouping: maps each object to its matched subjects
    pub object_to_subjects: FxHashMap<EntityID, UniqueVec<EntityID>>,
    /// Concrete row matches preserving subject-predicate-object alignment.
    pub rows: Vec<QueryRelationRow>,
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
        self.matched_subjects.push(subject.clone());
        self.matched_objects.push(object.clone());

        // Track per-entity groupings
        self.subject_to_objects
            .entry(subject.clone())
            .or_default()
            .push(object.clone());
        self.object_to_subjects
            .entry(object.clone())
            .or_default()
            .push(subject.clone());

        for pred in predicates {
            let proposition = EntityID::Proposition(proposition_id, pred.clone());
            self.matched_propositions.push(proposition.clone());
            self.matched_predicates.push(pred.clone());
            self.rows.push(QueryRelationRow {
                proposition,
                subject: subject.clone(),
                predicate: pred,
                object: object.clone(),
            });
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};

    fn concept_ref(id: u64) -> EntityID {
        EntityID::Concept(id)
    }

    #[test]
    fn concept_pk_display_and_try_from_matcher_cover_success_and_errors() {
        let by_id = ConceptPK::ID(7);
        assert_eq!(by_id.to_string(), "{id: Concept(7)}");

        let by_object = ConceptPK::Object {
            r#type: "Person".to_string(),
            name: "Ada".to_string(),
        };
        assert_eq!(by_object.to_string(), r#"{type: "Person", name: "Ada"}"#);

        assert_eq!(
            ConceptPK::try_from(ConceptMatcher::ID("C:7".to_string())).unwrap(),
            by_id
        );
        assert_eq!(
            ConceptPK::try_from(ConceptMatcher::Object {
                r#type: "Person".to_string(),
                name: "Ada".to_string(),
            })
            .unwrap(),
            by_object
        );

        assert!(ConceptPK::try_from(ConceptMatcher::ID("P:1:likes".to_string())).is_err());
        assert!(ConceptPK::try_from(ConceptMatcher::Type("Person".to_string())).is_err());
        assert!(ConceptPK::try_from(ConceptMatcher::ID("bad-id".to_string())).is_err());
    }

    #[test]
    fn proposition_and_entity_pk_conversions_cover_nested_targets() {
        let proposition = PropositionPK::ID(9, "likes".to_string());
        assert_eq!(proposition.to_string(), r#"(id: Proposition(9, "likes"))"#);
        assert_eq!(
            PropositionPK::try_from(PropositionMatcher::ID("P:9:likes".to_string())).unwrap(),
            proposition
        );

        let matcher = PropositionMatcher::Object {
            subject: TargetTerm::Concept {
                variable: Some("s".to_string()),
                matcher: ConceptMatcher::Object {
                    r#type: "Person".to_string(),
                    name: "Ada".to_string(),
                },
            },
            predicate: PredTerm::Literal("likes".to_string()),
            object: TargetTerm::Concept {
                variable: None,
                matcher: ConceptMatcher::ID("C:2".to_string()),
            },
        };
        let object_pk = PropositionPK::try_from(matcher).unwrap();
        assert_eq!(
            object_pk.to_string(),
            r#"({type: "Person", name: "Ada"}, "likes", {id: Concept(2)})"#
        );

        assert!(PropositionPK::try_from(PropositionMatcher::ID("C:9".to_string())).is_err());
        assert!(
            PropositionPK::try_from(PropositionMatcher::Object {
                subject: TargetTerm::Variable("s".to_string()),
                predicate: PredTerm::Variable("p".to_string()),
                object: TargetTerm::Concept {
                    variable: None,
                    matcher: ConceptMatcher::ID("C:1".to_string()),
                },
            })
            .is_err()
        );

        assert_eq!(
            EntityPK::try_from(TargetTerm::Concept {
                variable: None,
                matcher: ConceptMatcher::ID("C:7".to_string()),
            })
            .unwrap(),
            EntityPK::Concept(ConceptPK::ID(7))
        );
        assert_eq!(
            EntityPK::try_from(TargetTerm::Proposition {
                variable: None,
                matcher: Box::new(PropositionMatcher::ID("P:9:likes".to_string())),
            })
            .unwrap(),
            EntityPK::Proposition(PropositionPK::ID(9, "likes".to_string()))
        );
        assert!(EntityPK::try_from(TargetTerm::Variable("x".to_string())).is_err());

        assert_eq!(
            EntityPK::from(EntityID::Proposition(9, "likes".to_string())),
            EntityPK::Proposition(PropositionPK::ID(9, "likes".to_string()))
        );
    }

    #[test]
    fn proposition_match_result_deduplicates_and_keeps_rows() {
        let mut result = PropositionsMatchResult::default();
        result.add_match(
            concept_ref(1),
            concept_ref(2),
            vec!["likes".to_string(), "knows".to_string()],
            10,
        );
        result.add_match(
            concept_ref(1),
            concept_ref(2),
            vec!["likes".to_string()],
            10,
        );

        assert_eq!(result.matched_subjects.len(), 1);
        assert_eq!(result.matched_objects.len(), 1);
        assert_eq!(result.matched_predicates.len(), 2);
        assert_eq!(result.matched_propositions.len(), 2);
        assert_eq!(result.rows.len(), 3);
        assert_eq!(
            result
                .subject_to_objects
                .get(&concept_ref(1))
                .unwrap()
                .to_vec(),
            vec![concept_ref(2)]
        );
        assert_eq!(
            result
                .object_to_subjects
                .get(&concept_ref(2))
                .unwrap()
                .to_vec(),
            vec![concept_ref(1)]
        );
    }

    #[test]
    fn query_context_cache_relation_and_target_structs_are_exercised() {
        let ctx = QueryContext::default();
        ctx.cache.concepts.write().insert(
            1,
            Concept {
                _id: 1,
                r#type: "Person".to_string(),
                name: "Ada".to_string(),
                attributes: Map::from_iter([("age".to_string(), json!(42))]),
                metadata: Map::new(),
            },
        );
        ctx.cache.propositions.write().insert(
            2,
            Proposition {
                _id: 2,
                subject: concept_ref(1),
                object: concept_ref(3),
                predicates: BTreeSet::from(["likes".to_string()]),
                properties: BTreeMap::new(),
            },
        );

        assert_eq!(ctx.cache.concepts.read().get(&1).unwrap().name, "Ada");
        assert_eq!(
            ctx.cache.propositions.read().get(&2).unwrap().subject,
            concept_ref(1)
        );

        let row = QueryRelationRow {
            proposition: EntityID::Proposition(2, "likes".to_string()),
            subject: concept_ref(1),
            predicate: "likes".to_string(),
            object: concept_ref(3),
        };
        let binding = QueryRelationBinding {
            proposition_var: Some("p".to_string()),
            subject_var: Some("s".to_string()),
            predicate_var: Some("pred".to_string()),
            object_var: Some("o".to_string()),
            rows: vec![row.clone()],
        };
        assert_eq!(binding.rows[0].predicate, row.predicate);

        let targets = [
            TargetEntities::Any,
            TargetEntities::AnyPropositions,
            TargetEntities::IDs(vec![concept_ref(1)]),
        ];
        assert!(matches!(targets[0], TargetEntities::Any));
        assert!(matches!(targets[1], TargetEntities::AnyPropositions));
        assert!(matches!(&targets[2], TargetEntities::IDs(ids) if ids == &vec![concept_ref(1)]));

        let path = GraphPath {
            start: concept_ref(1),
            end: concept_ref(3),
            propositions: vec![EntityID::Proposition(2, "likes".to_string())].into(),
            hops: 1,
        };
        assert_eq!(path.hops, path.propositions.len() as u16);
    }
}
