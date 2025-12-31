//! # Abstract Syntax Tree definitions for all KIP constructs
//!
//! This module defines the Abstract Syntax Tree (AST) structures for the Knowledge Interaction Protocol (KIP),
//! a knowledge memory interaction protocol designed for Large Language Models (LLMs) to build sustainable
//! learning and self-evolving knowledge memory systems.
//!
//! KIP defines a complete interaction pattern for efficient, reliable, bidirectional knowledge exchange
//! between the neural core (LLM) and the symbolic core (Cognitive Nexus).
//!
//! The AST is organized into three main command categories:
//! - **KQL (Knowledge Query Language)**: For knowledge retrieval and reasoning
//! - **KML (Knowledge Manipulation Language)**: For knowledge evolution and updates
//! - **META**: For knowledge exploration and grounding

use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::HashSet, fmt, str::FromStr};

pub use serde_json::{Map, Number};

/// Alias for serde_json::Value. It is KIP's value type for JSON-like structures.
/// Such as attributes, metadata.
pub type Json = serde_json::Value;

/// Represents a primitive value in the KIP system.
/// This is the fundamental data type used throughout KIP for attributes, metadata, and literals.
#[derive(Clone, Debug, Default, Deserialize, Serialize, Eq, PartialEq, Hash)]
pub enum Value {
    /// Represents a null value
    #[default]
    Null,
    /// Boolean value (true/false)
    Bool(bool),
    /// Numeric value (integer or floating-point)
    Number(Number),
    /// String value
    String(String),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Number(n) => write!(f, "{n}"),
            // format as JSON string (format_escaped_str)
            Value::String(s) => write!(f, "{}", Json::String(s.clone())),
        }
    }
}

impl From<Value> for Json {
    fn from(value: Value) -> Self {
        match value {
            Value::Null => Json::Null,
            Value::Bool(b) => Json::Bool(b),
            Value::Number(n) => Json::Number(n),
            Value::String(s) => Json::String(s),
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.to_string())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

impl From<Number> for Value {
    fn from(value: Number) -> Self {
        Value::Number(value)
    }
}

impl TryFrom<Json> for Value {
    type Error = String;

    fn try_from(value: Json) -> Result<Self, Self::Error> {
        match value {
            Json::Null => Ok(Value::Null),
            Json::Bool(b) => Ok(Value::Bool(b)),
            Json::Number(n) => Ok(Value::Number(n)),
            Json::String(s) => Ok(Value::String(s)),
            _ => Err(format!("Unsupported JSON type: {value:?}")),
        }
    }
}

impl Value {
    /// Extracts a string from the Value, returning an error if the type is incorrect.
    pub fn into_opt_string(self) -> Result<Option<String>, String> {
        match self {
            Value::String(s) => Ok(Some(s)),
            Value::Null => Ok(None),
            v => Err(format!("Expected a string or null, found: {v:?}")),
        }
    }

    /// Extracts a number from the Value, returning an error if the type is incorrect.
    pub fn into_opt_number(self) -> Result<Option<Number>, String> {
        match self {
            Value::Number(n) => Ok(Some(n)),
            Value::Null => Ok(None),
            v => Err(format!("Expected a number or null, found: {v:?}")),
        }
    }

    /// Extracts a boolean from the Value, returning an error if the type is incorrect.
    pub fn into_opt_bool(self) -> Result<Option<bool>, String> {
        match self {
            Value::Bool(b) => Ok(Some(b)),
            Value::Null => Ok(None),
            v => Err(format!("Expected a boolean or null, found: {v:?}")),
        }
    }

    /// Extracts a string from the Value, returning None if the type is incorrect.
    pub fn as_string(self) -> Option<String> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Extracts a number from the Value, returning None if the type is incorrect.
    pub fn as_number(self) -> Option<Number> {
        match self {
            Value::Number(n) => Some(n),
            _ => None,
        }
    }

    /// Extracts a boolean from the Value, returning None if the type is incorrect.
    pub fn as_bool(self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(b),
            _ => None,
        }
    }

    /// Checks if the Value is a string.
    pub fn is_string(&self) -> bool {
        matches!(self, Value::String(_))
    }

    /// Checks if the Value is a number.
    pub fn is_number(&self) -> bool {
        matches!(self, Value::Number(_))
    }

    /// Checks if the Value is a boolean.
    pub fn is_bool(&self) -> bool {
        matches!(self, Value::Bool(_))
    }

    /// Checks if the Value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandType {
    /// KQL (Knowledge Query Language) - for knowledge retrieval and reasoning
    Kql,
    /// KML (Knowledge Manipulation Language) - for knowledge evolution and updates
    Kml,
    /// META commands - for knowledge exploration and grounding
    Meta,
    /// Unknown command type
    Unknown,
}

impl CommandType {
    pub fn from(val: &Command) -> CommandType {
        match val {
            Command::Kql(_) => CommandType::Kql,
            Command::Kml(_) => CommandType::Kml,
            Command::Meta(_) => CommandType::Meta,
        }
    }
}

impl fmt::Display for CommandType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandType::Kql => write!(f, "KQL"),
            CommandType::Kml => write!(f, "KML"),
            CommandType::Meta => write!(f, "META"),
            CommandType::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

impl FromStr for CommandType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "KQL" => Ok(CommandType::Kql),
            "KML" => Ok(CommandType::Kml),
            "META" => Ok(CommandType::Meta),
            _ => Ok(CommandType::Unknown),
        }
    }
}

impl Serialize for CommandType {
    /// Serializes the CommandType as a string.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

/// Visitor for deserializing CommandType from strings.
struct CommandTypeVisitor;

impl serde::de::Visitor<'_> for CommandTypeVisitor {
    type Value = CommandType;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a string")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        CommandType::from_str(s).map_err(|err| E::custom(err))
    }
}

impl<'de> Deserialize<'de> for CommandType {
    /// Deserializes a CommandType from a string.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(CommandTypeVisitor)
    }
}

/// Top-level command enum representing the three main KIP instruction sets.
/// Each command type serves a specific purpose in the knowledge interaction workflow.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum Command {
    /// KQL (Knowledge Query Language) - for knowledge retrieval and reasoning
    Kql(KqlQuery),
    /// KML (Knowledge Manipulation Language) - for knowledge evolution and updates
    Kml(KmlStatement),
    /// META commands - for knowledge exploration and grounding
    Meta(MetaCommand),
}

// --- Common AST Nodes ---

/// Represents a key-value pair used in various contexts throughout KIP.
/// Used for attributes, metadata, constraints, and unique key specifications.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct KeyValue {
    /// The key name
    pub key: String,
    /// The associated value
    pub value: Value,
}

/// Represents a concept clause used for concept identification and grounding.
/// Syntax: `?node_var {id: "<id>"}`, `?node_var {type: "<type>", name: "<name>"}`, `?node_var {type: "<type>"}`，`?node_var {name: "<name>"}`
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct ConceptClause {
    /// The matcher for concept, which can be a combination of `id`, `type`, and `name`
    pub matcher: ConceptMatcher,
    /// A variable (e.g., `?drug`)
    pub variable: String,
}

/// Represents a identifier for a concept node.
/// This identifier can be constructed from various attributes like `id`, `type`, and `name`.
/// It is used to uniquely identify a concept within the knowledge graph, or to match concepts
/// based on type or name.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum ConceptMatcher {
    /// Syntax: `{id: "<id>"}`
    ID(String),
    /// Syntax: `{type: "<type>"}`
    Type(String),
    /// Syntax: `{name: "<name>"}`
    Name(String),
    /// Syntax: `{type: "<type>", name: "<name>"}`
    Object { r#type: String, name: String },
}

impl fmt::Display for ConceptMatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConceptMatcher::ID(val) => write!(f, "{{id: {val:?}}}"),
            ConceptMatcher::Type(val) => write!(f, "{{type: {val:?}}}"),
            ConceptMatcher::Name(val) => write!(f, "{{name: {val:?}}}"),
            ConceptMatcher::Object {
                r#type: val_type,
                name: val_name,
            } => {
                write!(f, "{{type: {val_type:?}, name: {val_name:?}}}")
            }
        }
    }
}

/// Implements conversion from a vector of KeyValue pairs to a ConceptMatcher.
impl TryFrom<Vec<KeyValue>> for ConceptMatcher {
    type Error = String;

    fn try_from(values: Vec<KeyValue>) -> Result<Self, Self::Error> {
        let mut id: Option<String> = None;
        let mut r#type: Option<String> = None;
        let mut name: Option<String> = None;

        for val in values {
            match val.key.as_str() {
                "id" => id = val.value.into_opt_string()?,
                "type" => r#type = val.value.into_opt_string()?,
                "name" => name = val.value.into_opt_string()?,
                key => {
                    return Err(format!("Invalid key in Concept clause: {}", key));
                }
            }
        }

        match (id, r#type, name) {
            (Some(id_val), None, None) => Ok(ConceptMatcher::ID(id_val)),
            (None, Some(type_val), None) => Ok(ConceptMatcher::Type(type_val)),
            (None, None, Some(name_val)) => Ok(ConceptMatcher::Name(name_val)),
            (None, Some(type_val), Some(name_val)) => Ok(ConceptMatcher::Object {
                r#type: type_val,
                name: name_val,
            }),
            (Some(_), Some(_), _) | (Some(_), _, Some(_)) => {
                Err("ConceptMatcher cannot have both id and other attributes".to_string())
            }
            (None, None, None) => Err(
                "ConceptMatcher must have at least one identifying attribute: id, type, or name"
                    .to_string(),
            ),
        }
    }
}

impl ConceptMatcher {
    /// Checks if the ConceptMatcher is unique based on its attributes.
    /// A ConceptMatcher is considered unique if it has an ID, or both type and name are specified.
    pub fn is_unique(&self) -> bool {
        matches!(self, ConceptMatcher::ID(_) | ConceptMatcher::Object { .. })
    }
}

/// Represents a proposition clause used for proposition identification and grounding.
/// Syntax: `?link_var (id: "<link_id>")`, `?link_var (?subject, "<predicate>", ?object)`
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct PropositionClause {
    /// The matcher for proposition, which can be a combination of `subject`, `predicate`, and `object`
    pub matcher: PropositionMatcher,
    /// A variable (e.g., `?relationship`)
    pub variable: Option<String>,
}

/// Represents a proposition matcher that identifies a specific relationship between concepts or propositions.
/// It consists of a subject, predicate, and object, which can be variables, concept references or proposition references.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum PropositionMatcher {
    /// Syntax: `(id: "<link_id>")`
    ID(String),
    /// `(?subject, "<predicate>", ?object)`
    Object {
        subject: TargetTerm,
        predicate: PredTerm,
        object: TargetTerm,
    },
}

/// Represents a term that can be a variable, node reference, or nested proposition.
/// Used for both subject and object positions in proposition patterns.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum TargetTerm {
    /// A variable (e.g., `?drug`)
    Variable(String),
    /// A reference to an existing concept node (via ConceptClause)
    Concept(ConceptMatcher),
    /// A nested proposition clause
    Proposition(Box<PropositionMatcher>),
}

/// Represents a predicate term in a proposition.
/// Can be either a variable or a literal string.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum PredTerm {
    /// A variable predicate (e.g., `?relationship`)
    Variable(String),
    /// A literal predicate string (e.g., `"treats"`)
    Literal(String),
    /// A list of literal predicates (e.g., `"treats" | "causes"`)
    Alternative(Vec<String>),
    /// A multi-hop predicate (e.g., `"is_subclass_of"{0,5}`)
    MultiHop {
        predicate: String,
        min: u16,
        max: Option<u16>,
    },
}

// --- KQL AST ---

/// Represents a complete KQL (Knowledge Query Language) query.
/// KQL is responsible for knowledge retrieval and reasoning within the Cognitive Nexus.
///
/// Structure: `FIND(...) WHERE { ... } ORDER BY ... LIMIT N CURSOR "<token>"`
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct KqlQuery {
    /// The FIND clause specifying what to return
    pub find_clause: FindClause,
    /// WHERE clauses containing graph patterns and filters (all ANDed together)
    pub where_clauses: Vec<WhereClause>,
    /// Optional ORDER BY conditions for result sorting
    pub order_by: Option<Vec<OrderByCondition>>,
    /// Optional LIMIT for result count restriction
    pub limit: Option<usize>,
    /// Optional CURSOR for result pagination
    pub cursor: Option<String>,
}

/// Represents the FIND clause of a KQL query.
/// Declares the final output of the query, supporting both simple variables and aggregations.
/// Syntax: `FIND(?var1, ?agg_func(?var2))`
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct FindClause {
    /// List of expressions to be returned (variables or aggregations)
    pub expressions: Vec<FindExpression>,
}

/// Represents an expression in the FIND clause.
/// Can be either a simple variable or an aggregation function with alias.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum FindExpression {
    /// A dot notation path (e.g., `?drug.name`, `?drug.attributes.risk_level`)
    Variable(DotPathVar),
    /// An aggregation function (e.g., `COUNT(?drug)`)
    Aggregation {
        /// The aggregation function to apply
        func: AggregationFunction,
        /// The variable to aggregate
        var: DotPathVar,
        /// Whether to use DISTINCT
        distinct: bool,
    },
}

/// Represents a dot notation path for accessing nested data.
/// Syntax: `?var.field` or `?var.attributes.key` or `?var.metadata.key`
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct DotPathVar {
    /// The base variable (e.g., `?drug`)
    pub var: String,
    /// The path components (e.g., ["attributes", "risk_level"])
    pub path: Vec<String>,
}

impl DotPathVar {
    /// Converts the DotPathVar to a JSON Pointer string.
    pub fn to_pointer(&self) -> String {
        if self.path.is_empty() {
            return "".to_string(); // the whole document
        }

        // 构建完整的 JSON Pointer 路径
        let mut pointer = String::new();
        for component in &self.path {
            pointer.push('/');
            pointer.push_str(&escape_json_pointer_token(component));
        }
        pointer
    }

    /// Returns the JSON Pointer string or the specified field if the path is empty.
    pub fn to_pointer_or(&self, field: &str) -> String {
        if self.path.is_empty() {
            return format!("/{}", escape_json_pointer_token(field));
        }

        self.to_pointer()
    }
}

fn escape_json_pointer_token(token: &str) -> String {
    token
        .replace('~', "~0") // 首先替换 '~' 为 '~0'
        .replace('/', "~1") // 然后替换 '/' 为 '~1'
}

/// Supported aggregation functions in KQL.
/// These functions operate on grouped data to produce summary statistics.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum AggregationFunction {
    /// COUNT(?var) - counts the number of bindings
    Count,
    /// SUM(?var) - sums numeric values
    Sum,
    /// AVG(?var) - calculates average of numeric values
    Avg,
    /// MIN(?var) - finds minimum value
    Min,
    /// MAX(?var) - finds maximum value
    Max,
}

impl AggregationFunction {
    pub fn calculate(&self, values: &Vec<Json>, distinct: bool) -> Json {
        match self {
            AggregationFunction::Count => {
                if distinct {
                    let vals: HashSet<&Json> = HashSet::from_iter(values);
                    vals.len().into()
                } else {
                    values.len().into()
                }
            }
            AggregationFunction::Sum => {
                let sum: f64 = values.iter().filter_map(|v| v.as_f64()).sum();
                Number::from_f64(sum).map(|v| v.into()).unwrap_or_default()
            }
            AggregationFunction::Avg => {
                let nums: Vec<f64> = values.iter().filter_map(|v| v.as_f64()).collect();
                if nums.is_empty() {
                    Json::Null
                } else {
                    let avg = nums.iter().sum::<f64>() / nums.len() as f64;
                    Number::from_f64(avg).map(|v| v.into()).unwrap_or_default()
                }
            }
            AggregationFunction::Min => values
                .iter()
                .filter_map(|v| v.as_f64())
                .min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
                .map(|min| Number::from_f64(min).map(|v| v.into()).unwrap_or_default())
                .unwrap_or(Json::Null),
            AggregationFunction::Max => values
                .iter()
                .filter_map(|v| v.as_f64())
                .max_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
                .map(|max| Number::from_f64(max).map(|v| v.into()).unwrap_or_default())
                .unwrap_or(Json::Null),
        }
    }
}

/// Represents different types of clauses in the WHERE section of a KQL query.
/// All clauses are combined with logical AND by default.
/// Syntax: `WHERE { ... }`
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum WhereClause {
    /// Concept clause: `?node_var {type: "<type>", name: "<name>", id: "<id>"}`
    Concept(ConceptClause),
    /// Proposition clause: `?link_var (?subject, "<predicate>", ?object)`
    Proposition(PropositionClause),
    /// Filter condition: `FILTER(boolean_expression)`
    Filter(FilterClause),
    /// Negation: `NOT { ... }`
    Not(Vec<WhereClause>),
    /// Optional matching: `OPTIONAL { ... }`
    Optional(Vec<WhereClause>),
    /// Union (logical OR): `UNION { ... }`
    Union(Vec<WhereClause>),
}

/// Represents a filter condition with optional subquery.
/// Applies complex filtering logic to bound variables.
/// Syntax: `FILTER(boolean_expression)`
/// Example: `FILTER(?risk < 3)` or `FILTER(?count > 5)`
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct FilterClause {
    /// The main filter expression
    pub expression: FilterExpression,
}

/// Represents different types of filter expressions.
/// Supports comparisons, logical operations, negation, and function calls.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum FilterExpression {
    /// Comparison operations (==, !=, <, >, <=, >=)
    Comparison {
        left: FilterOperand,
        operator: ComparisonOperator,
        right: FilterOperand,
    },
    /// Logical operations (&&, ||)
    Logical {
        left: Box<FilterExpression>,
        operator: LogicalOperator,
        right: Box<FilterExpression>,
    },
    /// Unary negation (!)
    Not(Box<FilterExpression>),
    /// Function calls (CONTAINS, STARTS_WITH, etc.)
    Function {
        func: FilterFunction,
        args: Vec<FilterOperand>,
    },
}

/// Represents an operand in a filter expression.
/// Can be either a variable reference, dot notation path, or a literal value.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum FilterOperand {
    /// A dot notation path (e.g., `?risk`, `?drug.attributes.risk_level`)
    Variable(DotPathVar),
    /// A literal value
    Literal(Value),
}

/// Comparison operators supported in filter expressions.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum ComparisonOperator {
    /// Equality (==)
    Equal,
    /// Inequality (!=)
    NotEqual,
    /// Less than (<)
    LessThan,
    /// Greater than (>)
    GreaterThan,
    /// Less than or equal (<=)
    LessEqual,
    /// Greater than or equal (>=)
    GreaterEqual,
}

impl ComparisonOperator {
    pub fn compare(&self, left: &Json, right: &Json) -> bool {
        match self {
            ComparisonOperator::Equal => left == right,
            ComparisonOperator::NotEqual => left != right,
            ComparisonOperator::LessThan => compare_json(left, right)
                .map(|o| o == Ordering::Less)
                .unwrap_or(false),
            ComparisonOperator::GreaterThan => compare_json(left, right)
                .map(|o| o == Ordering::Greater)
                .unwrap_or(false),
            ComparisonOperator::LessEqual => compare_json(left, right)
                .map(|o| o != Ordering::Greater)
                .unwrap_or(false),
            ComparisonOperator::GreaterEqual => compare_json(left, right)
                .map(|o| o != Ordering::Less)
                .unwrap_or(false),
        }
    }
}

/// Logical operators for combining filter expressions.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum LogicalOperator {
    /// Logical AND (&&)
    And,
    /// Logical OR (||)
    Or,
}

/// String manipulation and pattern matching functions for filters.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum FilterFunction {
    /// CONTAINS(?str, "substring") - checks if string contains substring
    Contains,
    /// STARTS_WITH(?str, "prefix") - checks if string starts with prefix
    StartsWith,
    /// ENDS_WITH(?str, "suffix") - checks if string ends with suffix
    EndsWith,
    /// REGEX(?str, "pattern") - checks if string matches regex pattern
    Regex,
}

/// Represents an ORDER BY condition for result sorting.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct OrderByCondition {
    /// The variable to sort by
    pub variable: DotPathVar,
    /// Sort direction (ascending or descending)
    pub direction: OrderDirection,
}

/// Sort direction for ORDER BY clauses.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum OrderDirection {
    /// Ascending order
    Asc,
    /// Descending order
    Desc,
}

// --- KML AST ---

/// Represents a KML (Knowledge Manipulation Language) statement.
/// KML is responsible for knowledge evolution and is the core tool for Agent learning.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum KmlStatement {
    /// UPSERT statement for atomic knowledge creation/updates
    Upsert(Vec<UpsertBlock>),
    /// DELETE statement for knowledge removal
    Delete(DeleteStatement),
}

/// Represents an UPSERT block - the primary vehicle for "Knowledge Capsules".
/// Provides atomic creation or update of knowledge, ensuring idempotent operations.
/// Structure: `UPSERT { CONCEPT ?handle { ... } } WITH METADATA { ... }`
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct UpsertBlock {
    /// List of concepts and propositions to upsert
    pub items: Vec<UpsertItem>,
    /// Global metadata for the entire upsert operation
    pub metadata: Option<Map<String, Json>>,
}

/// Represents an item within an UPSERT block.
/// Can be either a concept definition or a standalone proposition.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum UpsertItem {
    /// A concept block defining a concept node
    Concept(ConceptBlock),
    /// A proposition block defining a standalone proposition
    Proposition(PropositionBlock),
}

/// Represents a concept definition within an UPSERT block.
/// Defines a concept node with its attributes and outgoing propositions.
/// Structure: `CONCEPT ?handle { { ... } SET ATTRIBUTES { ... } SET PROPOSITIONS { ... } } WITH METADATA { ... }`
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct ConceptBlock {
    /// Local handle for referencing within the transaction (starts with ?)
    pub handle: String,
    /// Concept clause for matching the existing concept or creating new one
    pub concept: ConceptMatcher,
    /// Optional attributes to set on the concept
    pub set_attributes: Option<Map<String, Json>>,
    /// Optional propositions emanating from this concept
    pub set_propositions: Option<Vec<SetProposition>>,
    /// Optional metadata for this concept
    pub metadata: Option<Map<String, Json>>,
}

/// Represents a proposition to be set from a concept.
/// Used within the SET PROPOSITIONS block of a concept definition.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct SetProposition {
    /// The predicate (relationship type)
    pub predicate: String,
    /// The object of the proposition (node or local handle)
    pub object: TargetTerm,
    /// Optional metadata for this specific proposition
    pub metadata: Option<Map<String, Json>>,
}

/// Represents a standalone proposition definition within an UPSERT block.
/// Used for creating complex relationships that don't naturally belong to a single concept.
/// Structure: `PROPOSITION ?handle { ({ ... }, "predicate", { ... }) SET ATTRIBUTES { ... } } WITH METADATA { ... }`
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct PropositionBlock {
    /// Local handle for referencing within the transaction (starts with ?)
    pub handle: String,
    /// Proposition clause for matching the existing proposition or creating new one
    pub proposition: PropositionMatcher,
    /// Optional attributes to set on the concept
    pub set_attributes: Option<Map<String, Json>>,
    /// Optional metadata for this proposition
    pub metadata: Option<Map<String, Json>>,
}

/// Represents different types of DELETE statements in KML.
/// Provides targeted removal of knowledge components from the Cognitive Nexus.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum DeleteStatement {
    /// Delete specific attributes from concepts or proposition where conditions match
    /// Syntax: `DELETE ATTRIBUTES { "attribute_name", ... } FROM ?target WHERE { ... }`
    DeleteAttributes {
        /// List of attribute names to delete
        attributes: Vec<String>,
        /// The target node or link to delete attributes from
        target: String,
        /// WHERE clauses containing graph patterns and filters
        where_clauses: Vec<WhereClause>,
    },
    /// Syntax: `DELETE METADATA { "key_name", ... } FROM ?target WHERE { ... }`
    DeleteMetadata {
        /// List of keys to delete
        keys: Vec<String>,
        /// The target node or link to delete attributes from
        target: String,
        /// WHERE clauses containing graph patterns and filters
        where_clauses: Vec<WhereClause>,
    },
    /// Delete propositions where conditions match
    /// Syntax: `DELETE PROPOSITIONS ?target_link WHERE { ... }`
    DeletePropositions {
        /// The target links
        target: String,
        /// WHERE clauses containing graph patterns and filters
        where_clauses: Vec<WhereClause>,
    },
    /// Delete an entire concept and all its relationships
    /// Syntax: `DELETE CONCEPT ?target_node DETACH WHERE { ... }`
    DeleteConcept {
        /// The target concept node
        target: String,
        /// WHERE clauses containing graph patterns and filters
        where_clauses: Vec<WhereClause>,
    },
}

// --- META AST ---

/// Represents META commands for knowledge exploration and grounding.
/// META is a lightweight subset focused on introspection and disambiguation.
/// These are fast, metadata-driven commands that don't involve complex graph traversal.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum MetaCommand {
    /// DESCRIBE commands for schema information and cognitive primers
    Describe(DescribeTarget),
    /// SEARCH commands for concept disambiguation
    Search(SearchCommand),
}

/// Represents different targets for DESCRIBE commands.
/// Used to query the "schema" information of the Cognitive Nexus.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum DescribeTarget {
    /// DESCRIBE PRIMER - gets the "Cognitive Primer" for LLM guidance
    Primer,
    /// DESCRIBE DOMAINS - lists all knowledge domains
    Domains,
    /// DESCRIBE CONCEPT_TYPES - lists all concept types
    ConceptTypes {
        /// Optional LIMIT for result count restriction
        limit: Option<usize>,
        /// Optional CURSOR for result pagination
        cursor: Option<String>,
    },
    /// DESCRIBE CONCEPT_TYPE "TypeName" - details about a specific concept type
    ConceptType(String),
    /// DESCRIBE PROPOSITION_TYPES - lists all proposition types
    PropositionTypes {
        /// Optional LIMIT for result count restriction
        limit: Option<usize>,
        /// Optional CURSOR for result pagination
        cursor: Option<String>,
    },
    /// DESCRIBE PROPOSITION_TYPE "TypeName" - details about a specific proposition type
    PropositionType(String),
}

/// Represents a SEARCH command for concept disambiguation.
/// Helps LLMs find and identify concepts or propositions when exact matches are unclear.
/// Syntax: `SEARCH [CONCEPT|PROPOSITION] "<search_term>" WITH TYPE "<type_name>" LIMIT N`
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct SearchCommand {
    pub target: SearchTarget,
    /// The search term
    pub term: String,
    /// Optional type constraint for the search
    pub in_type: Option<String>,
    /// Optional limit on the number of results
    pub limit: Option<usize>,
}

/// Represents the target of a search command.
/// Indicates whether the search is for concepts or propositions.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum SearchTarget {
    /// Searching for concepts
    Concept,
    /// Searching for propositions
    Proposition,
}

pub fn compare_json(left: &Json, right: &Json) -> Option<Ordering> {
    match (left, right) {
        (Json::Number(a), Json::Number(b)) => a
            .as_f64()
            .unwrap_or(0.0)
            .partial_cmp(&b.as_f64().unwrap_or(0.0)),
        (Json::Bool(a), Json::Bool(b)) => Some(a.cmp(b)),
        (Json::Null, Json::Null) => Some(Ordering::Equal),
        (Json::String(a), Json::String(b)) => {
            // try to compare as number
            if let Ok(a) = Number::from_str(a)
                && let Ok(b) = Number::from_str(b)
            {
                return a
                    .as_f64()
                    .unwrap_or(0.0)
                    .partial_cmp(&b.as_f64().unwrap_or(0.0));
            }
            // try to compare as datetime
            if let Ok(a) = DateTime::parse_from_rfc3339(a)
                && let Ok(b) = DateTime::parse_from_rfc3339(b)
            {
                return Some(a.cmp(&b));
            }
            if let Ok(a) = DateTime::parse_from_rfc2822(a)
                && let Ok(b) = DateTime::parse_from_rfc2822(b)
            {
                return Some(a.cmp(&b));
            }

            Some(a.cmp(b))
        }
        _ => None,
    }
}
