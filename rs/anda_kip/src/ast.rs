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

/// High-level language family of a parsed KIP command.
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
    /// Returns the command family for a parsed [`Command`].
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
    Object {
        /// Concept type name.
        r#type: String,
        /// Concept display name within the type.
        name: String,
    },
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
        // Duplicate keys in an LLM-generated matcher are almost always a
        // generation error; reject them instead of silently keeping the last.
        let mut seen = [false; 3];

        for val in values {
            let idx = match val.key.as_str() {
                "id" => 0,
                "type" => 1,
                "name" => 2,
                key => {
                    return Err(format!("Invalid key in Concept clause: {}", key));
                }
            };
            if seen[idx] {
                return Err(format!("Duplicate key in Concept clause: {}", val.key));
            }
            seen[idx] = true;
            match idx {
                0 => id = val.value.into_opt_string()?,
                1 => r#type = val.value.into_opt_string()?,
                _ => name = val.value.into_opt_string()?,
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
        /// Subject endpoint of the proposition pattern.
        subject: TargetTerm,
        /// Predicate matcher between subject and object.
        predicate: PredTerm,
        /// Object endpoint of the proposition pattern.
        object: TargetTerm,
    },
}

/// Represents a term that can be a variable, node reference, or nested proposition.
/// Used for both subject and object positions in proposition patterns.
///
/// Per the KIP specification, an embedded endpoint clause must be **unnamed**:
/// to bind an endpoint to a variable, declare it in a separate clause first and
/// reference the variable here.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum TargetTerm {
    /// A variable (e.g., `?drug`)
    Variable(String),
    /// An unnamed concept clause referencing an existing concept node
    /// (e.g., `{type: "Person", name: "Yan"}`).
    Concept(ConceptMatcher),
    /// An unnamed nested proposition clause (e.g., `(?s, "p", ?o)`).
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
        /// Predicate name to traverse repeatedly.
        predicate: String,
        /// Minimum number of hops in the traversal.
        min: u16,
        /// Optional maximum number of hops; `None` means unbounded.
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

impl fmt::Display for FindExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FindExpression::Variable(var) => write!(f, "{}", var),
            FindExpression::Aggregation {
                func,
                var,
                distinct,
            } => {
                let func_name = match func {
                    AggregationFunction::Count => "COUNT",
                    AggregationFunction::Sum => "SUM",
                    AggregationFunction::Avg => "AVG",
                    AggregationFunction::Min => "MIN",
                    AggregationFunction::Max => "MAX",
                };

                if *distinct {
                    write!(f, "{}(DISTINCT {})", func_name, var)
                } else {
                    write!(f, "{}({})", func_name, var)
                }
            }
        }
    }
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

        // Build the full JSON Pointer path
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

impl fmt::Display for DotPathVar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.path.is_empty() {
            write!(f, "?{}", self.var)
        } else {
            write!(f, "?{}.{}", self.var, self.path.join("."))
        }
    }
}

fn escape_json_pointer_token(token: &str) -> String {
    token
        .replace('~', "~0") // First replace '~' with '~0'
        .replace('/', "~1") // Then replace '/' with '~1'
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
    /// Applies this aggregation to a list of JSON values.
    ///
    /// Non-numeric values are ignored by numeric aggregations. `distinct`
    /// de-duplicates values before `COUNT`, matching KQL `DISTINCT` behavior.
    ///
    /// Boundary conventions:
    /// - `SUM` of all-integer inputs accumulates in `i128` and returns an
    ///   integer (no f64 precision loss above 2^53); mixed/float inputs — or
    ///   integer sums outside the `i64`/`u64` range — fall back to `f64`.
    ///   `SUM` of an empty/non-numeric input is integer `0` (the additive
    ///   identity); a non-finite `f64` sum (overflow) yields `Json::Null`.
    /// - `AVG` is always `f64`; empty/non-numeric input yields `Json::Null`.
    /// - `MIN`/`MAX` return the original input value (integers stay
    ///   integers); empty/non-numeric input yields `Json::Null`.
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
                let numeric: Vec<&Json> = values.iter().filter(|v| v.is_number()).collect();
                let int_sum = numeric
                    .iter()
                    .try_fold(0i128, |acc, v| as_i128(v).and_then(|i| acc.checked_add(i)))
                    .and_then(i128_to_number);
                match int_sum {
                    Some(sum) => Json::Number(sum),
                    None => {
                        let sum: f64 = numeric.iter().filter_map(|v| v.as_f64()).sum();
                        Number::from_f64(sum)
                            .map(|v| v.into())
                            .unwrap_or(Json::Null)
                    }
                }
            }
            AggregationFunction::Avg => {
                let nums: Vec<f64> = values.iter().filter_map(|v| v.as_f64()).collect();
                if nums.is_empty() {
                    Json::Null
                } else {
                    let avg = nums.iter().sum::<f64>() / nums.len() as f64;
                    Number::from_f64(avg)
                        .map(|v| v.into())
                        .unwrap_or(Json::Null)
                }
            }
            AggregationFunction::Min => values
                .iter()
                .filter(|v| v.is_number())
                .min_by(|a, b| compare_json(a, b).unwrap_or(Ordering::Equal))
                .cloned()
                .unwrap_or(Json::Null),
            AggregationFunction::Max => values
                .iter()
                .filter(|v| v.is_number())
                .max_by(|a, b| compare_json(a, b).unwrap_or(Ordering::Equal))
                .cloned()
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
        /// Left operand of the comparison.
        left: FilterOperand,
        /// Comparison operator to apply.
        operator: ComparisonOperator,
        /// Right operand of the comparison.
        right: FilterOperand,
    },
    /// Logical operations (&&, ||)
    Logical {
        /// Left boolean expression.
        left: Box<FilterExpression>,
        /// Logical operator joining the expressions.
        operator: LogicalOperator,
        /// Right boolean expression.
        right: Box<FilterExpression>,
    },
    /// Unary negation (!)
    Not(Box<FilterExpression>),
    /// Function calls (CONTAINS, STARTS_WITH, etc.)
    Function {
        /// Built-in filter function to invoke.
        func: FilterFunction,
        /// Function arguments in source order.
        args: Vec<FilterOperand>,
    },
}

/// Represents an operand in a filter expression.
/// Can be either a variable reference, dot notation path, a literal value, or a list of values.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum FilterOperand {
    /// A dot notation path (e.g., `?risk`, `?drug.attributes.risk_level`)
    Variable(DotPathVar),
    /// A literal value
    Literal(Value),
    /// A list of literal values (e.g., `["a", "b", "c"]`)
    List(Vec<Value>),
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
    /// Compares two JSON values according to this operator.
    ///
    /// `==` / `!=` use [`loose_equal`] and are exact negations of each other:
    /// numbers compare exactly (`3.0 == 3` is `true`, and integers keep full
    /// precision beyond 2^53), strings are equal when identical or when both
    /// denote the same datetime instant (RFC 3339 / RFC 2822, per side), a
    /// number equals a string that spells the same numeric value, and
    /// arrays/objects compare by structural equality.
    ///
    /// The ordering operators use [`compare_json`] and return `false` for
    /// value pairs without a defined ordering (arrays, objects, and mixed
    /// types other than number/numeric-string; see KIP spec §2.7). For every
    /// ordered pair, `a == b` implies `a <= b` and `a >= b`. The ordering is
    /// deliberately looser than equality for numeric strings: `"1.10"` and
    /// `"1.1"` order as numerically equal (`<=` and `>=` both hold) but are
    /// not `==`.
    pub fn compare(&self, left: &Json, right: &Json) -> bool {
        match self {
            ComparisonOperator::Equal => loose_equal(left, right),
            ComparisonOperator::NotEqual => !loose_equal(left, right),
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
    /// IN(?expr, [value1, value2, ...]) - checks if value is in the given list
    In,
    /// IS_NULL(?expr) - checks if value is null or undefined
    IsNull,
    /// IS_NOT_NULL(?expr) - checks if value is not null or undefined
    IsNotNull,
}

/// Represents an ORDER BY condition for result sorting.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct OrderByCondition {
    /// The variable to sort by (also used for aggregation variable)
    pub variable: DotPathVar,
    /// Sort direction (ascending or descending)
    pub direction: OrderDirection,
    /// Optional aggregation function for ORDER BY aggregation expressions
    /// e.g., `ORDER BY COUNT(?n) ASC`
    pub aggregation: Option<AggregationFunction>,
}

impl OrderByCondition {
    /// Returns true if this ORDER BY condition sorts by an aggregation expression.
    pub fn is_aggregation(&self) -> bool {
        self.aggregation.is_some()
    }
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
/// It comprises four statements: `UPSERT` (identity-addressed create-or-update),
/// `UPDATE` (pattern-matched bulk mutation), `MERGE` (atomic entity consolidation),
/// and `DELETE` (targeted removal).
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum KmlStatement {
    /// UPSERT statement for atomic knowledge creation/updates
    Upsert(Vec<UpsertBlock>),
    /// UPDATE statement for pattern-matched bulk mutation (never creates)
    Update(UpdateStatement),
    /// MERGE statement for atomic entity consolidation
    Merge(MergeStatement),
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
    pub handle: Option<String>,
    /// Concept clause for matching the existing concept or creating new one
    pub concept: ConceptMatcher,
    /// Optional optimistic-concurrency guard (`EXPECT VERSION <n>`).
    /// The block executes only if the matched element's `_version` equals this value;
    /// `0` asserts the element does not exist yet (create-only). On mismatch the
    /// entire UPSERT aborts atomically with `KIP_3005` (VersionConflict).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expect_version: Option<u64>,
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
    pub handle: Option<String>,
    /// Proposition clause for matching the existing proposition or creating new one
    pub proposition: PropositionMatcher,
    /// Optional optimistic-concurrency guard (`EXPECT VERSION <n>`).
    /// See [`ConceptBlock::expect_version`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expect_version: Option<u64>,
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

/// Represents an UPDATE statement for pattern-matched bulk mutation.
/// Where UPSERT addresses elements one at a time by identity, UPDATE mutates
/// every element matched by a WHERE pattern in a single atomic statement.
/// It never creates elements.
///
/// Syntax:
/// ```prolog
/// UPDATE ?target
/// SET ATTRIBUTES { <key>: <value_or_expr>, ... }
/// SET METADATA { <key>: <value_or_expr>, ... }
/// WHERE { ... }
/// LIMIT N
/// ```
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct UpdateStatement {
    /// The target variable bound in the WHERE clause (concept nodes or proposition links)
    pub target: String,
    /// Attributes to set (shallow merge); values may be JSON or update expressions
    pub set_attributes: Option<Vec<(String, UpdateValue)>>,
    /// Author-asserted metadata to set (shallow merge); reserved `_` keys are
    /// rejected by executors with `KIP_2002`
    pub set_metadata: Option<Vec<(String, UpdateValue)>>,
    /// WHERE clauses containing graph patterns and filters binding the target
    pub where_clauses: Vec<WhereClause>,
    /// Optional safety cap on the number of elements updated in one statement
    pub limit: Option<usize>,
}

/// A value position inside an UPDATE `SET ATTRIBUTES` / `SET METADATA` block.
/// Either a plain JSON value (same semantics as UPSERT) or a numeric update
/// expression computed per element from the target's own current state.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum UpdateValue {
    /// A plain JSON value
    Json(Json),
    /// A numeric update expression (e.g., `ADD(?t.attributes.count, 1)`)
    Expr(UpdateExpr),
}

/// Numeric update expression functions available in UPDATE statements.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum UpdateFunction {
    /// ADD(a, b) - `a + b` (use a negative `b` to subtract)
    Add,
    /// MUL(a, b) - `a × b`
    Mul,
    /// CLAMP(x, lo, hi) - constrains `x` into `[lo, hi]`
    Clamp,
    /// COALESCE(x, default) - `x` if non-null, else `default`
    Coalesce,
}

impl UpdateFunction {
    /// Applies the function to pre-evaluated operand values.
    ///
    /// `ADD` / `MUL` / `CLAMP` operate on numbers and preserve integer arithmetic
    /// when all operands are integers; any `null` or non-number operand yields
    /// `Json::Null` (the executor then skips that key for that element).
    /// `COALESCE` returns the first operand when it is non-null, else the second.
    pub fn calculate(&self, args: &[Json]) -> Json {
        fn binary_number_op(
            a: &Json,
            b: &Json,
            int_op: impl Fn(i128, i128) -> Option<i128>,
            float_op: impl Fn(f64, f64) -> f64,
        ) -> Json {
            match (as_i128(a), as_i128(b)) {
                (Some(x), Some(y)) => int_op(x, y)
                    .and_then(i128_to_number)
                    .map(Json::Number)
                    .unwrap_or_else(|| float_number(float_op(x as f64, y as f64))),
                _ => match (a.as_f64(), b.as_f64()) {
                    (Some(x), Some(y)) => float_number(float_op(x, y)),
                    _ => Json::Null,
                },
            }
        }

        match self {
            UpdateFunction::Add => match args {
                [a, b] => binary_number_op(a, b, |x, y| x.checked_add(y), |x, y| x + y),
                _ => Json::Null,
            },
            UpdateFunction::Mul => match args {
                [a, b] => binary_number_op(a, b, |x, y| x.checked_mul(y), |x, y| x * y),
                _ => Json::Null,
            },
            UpdateFunction::Clamp => match args {
                [x, lo, hi] => match (as_i128(x), as_i128(lo), as_i128(hi)) {
                    (Some(x), Some(lo), Some(hi)) if lo <= hi => i128_to_number(x.clamp(lo, hi))
                        .map(Json::Number)
                        .unwrap_or(Json::Null),
                    _ => match (x.as_f64(), lo.as_f64(), hi.as_f64()) {
                        (Some(x), Some(lo), Some(hi)) if lo <= hi => float_number(x.clamp(lo, hi)),
                        _ => Json::Null,
                    },
                },
                _ => Json::Null,
            },
            UpdateFunction::Coalesce => match args {
                [x, default] => {
                    if x.is_null() {
                        default.clone()
                    } else {
                        x.clone()
                    }
                }
                _ => Json::Null,
            },
        }
    }
}

fn as_i128(value: &Json) -> Option<i128> {
    match value {
        Json::Number(n) => number_as_i128(n),
        _ => None,
    }
}

fn number_as_i128(n: &Number) -> Option<i128> {
    if let Some(i) = n.as_i64() {
        Some(i as i128)
    } else {
        n.as_u64().map(|u| u as i128)
    }
}

fn i128_to_number(value: i128) -> Option<Number> {
    if let Ok(i) = i64::try_from(value) {
        Some(Number::from(i))
    } else {
        u64::try_from(value).ok().map(Number::from)
    }
}

fn float_number(value: f64) -> Json {
    Number::from_f64(value)
        .map(Json::Number)
        .unwrap_or_default()
}

/// An operand/expression tree for UPDATE value computation.
/// Operands may be number literals, dot-notation paths on the UPDATE target
/// itself, or nested update expressions.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum UpdateExpr {
    /// A number literal
    Number(Number),
    /// A dot-notation path on the UPDATE target itself
    /// (e.g., `?target.metadata.confidence`); paths on other variables are invalid
    Variable(DotPathVar),
    /// A function application (e.g., `CLAMP(MUL(?t.metadata.confidence, 0.9), 0.0, 1.0)`)
    Function {
        /// The update function to apply
        func: UpdateFunction,
        /// The function arguments
        args: Vec<UpdateExpr>,
    },
}

impl UpdateExpr {
    /// Evaluates the expression for one element.
    ///
    /// `resolve` maps a dot-notation path on the target to the element's current
    /// JSON value (returning `Json::Null` for missing paths). A `null` or
    /// non-number result means the executor skips that key for that element.
    pub fn evaluate<F>(&self, resolve: &F) -> Json
    where
        F: Fn(&DotPathVar) -> Json,
    {
        match self {
            UpdateExpr::Number(n) => Json::Number(n.clone()),
            UpdateExpr::Variable(path) => resolve(path),
            UpdateExpr::Function { func, args } => {
                let args: Vec<Json> = args.iter().map(|arg| arg.evaluate(resolve)).collect();
                func.calculate(&args)
            }
        }
    }

    /// Returns all dot-notation paths referenced by this expression.
    pub fn referenced_paths(&self) -> Vec<&DotPathVar> {
        match self {
            UpdateExpr::Number(_) => vec![],
            UpdateExpr::Variable(path) => vec![path],
            UpdateExpr::Function { args, .. } => {
                args.iter().flat_map(|arg| arg.referenced_paths()).collect()
            }
        }
    }
}

/// Represents a MERGE statement for atomic entity consolidation.
/// Declares that two concept nodes denote the same entity and merges the
/// source into the target: repoints all links, fills missing attributes
/// (target wins; `aliases` unioned), deletes the source, and records
/// `_merged_from` provenance.
///
/// Syntax:
/// ```prolog
/// MERGE CONCEPT ?source INTO ?target
/// WHERE { ... }
/// ```
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct MergeStatement {
    /// The source variable (must bind exactly one concept node)
    pub source: String,
    /// The target variable (must bind exactly one concept node of the same type)
    pub target: String,
    /// WHERE clauses binding both variables
    pub where_clauses: Vec<WhereClause>,
}

// --- META AST ---

/// Represents META commands for knowledge exploration and grounding.
/// META is a lightweight subset focused on introspection, disambiguation,
/// and capsule round-trips.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum MetaCommand {
    /// DESCRIBE commands for schema information and cognitive primers
    Describe(DescribeTarget),
    /// SEARCH commands for index-driven grounding and associative retrieval
    Search(SearchCommand),
    /// EXPORT command for serializing knowledge into an idempotent UPSERT capsule
    Export(ExportCommand),
}

/// Represents different targets for DESCRIBE commands.
/// Used to query the "schema" information of the Cognitive Nexus.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum DescribeTarget {
    /// DESCRIBE PRIMER - gets the "Cognitive Primer" for LLM guidance
    Primer,
    /// DESCRIBE DOMAINS - lists all knowledge domains
    Domains,
    /// DESCRIBE CONCEPT TYPES - lists all concept types
    ConceptTypes {
        /// Optional LIMIT for result count restriction
        limit: Option<usize>,
        /// Optional CURSOR for result pagination
        cursor: Option<String>,
    },
    /// DESCRIBE CONCEPT TYPE "TypeName" - details about a specific concept type
    ConceptType(String),
    /// DESCRIBE PROPOSITION TYPES - lists all proposition types
    PropositionTypes {
        /// Optional LIMIT for result count restriction
        limit: Option<usize>,
        /// Optional CURSOR for result pagination
        cursor: Option<String>,
    },
    /// DESCRIBE PROPOSITION TYPE "predicate" - details about a specific proposition type
    PropositionType(String),
}

/// Represents a SEARCH command for index-driven grounding and associative retrieval.
/// Helps LLMs find and identify concepts or propositions when exact matches are unclear.
/// Syntax:
/// `SEARCH CONCEPT|PROPOSITION "<term>" [WITH TYPE "<Type>"] [MODE "keyword"|"semantic"|"hybrid"] [THRESHOLD <0.0-1.0>] [LIMIT N]`
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct SearchCommand {
    /// Entity class to search.
    pub target: SearchTarget,
    /// The search term
    pub term: String,
    /// Optional type constraint for the search
    pub in_type: Option<String>,
    /// Optional retrieval mode. When omitted, the engine uses `hybrid` if it
    /// supports semantic retrieval, otherwise `keyword`. Engines without
    /// semantic capability MUST treat `semantic` / `hybrid` as `keyword`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<SearchMode>,
    /// Optional relevance threshold in `[0, 1]`: hits whose transient
    /// `metadata._score` falls below it are dropped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub threshold: Option<Number>,
    /// Optional limit on the number of results
    pub limit: Option<usize>,
}

/// Retrieval mode for SEARCH commands.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum SearchMode {
    /// Lexical match over the grounding fields (text index)
    Keyword,
    /// Meaning-based similarity over the grounding fields (engine owns embeddings)
    Semantic,
    /// Fused lexical + semantic ranking (recommended default where supported)
    Hybrid,
}

impl fmt::Display for SearchMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SearchMode::Keyword => write!(f, "keyword"),
            SearchMode::Semantic => write!(f, "semantic"),
            SearchMode::Hybrid => write!(f, "hybrid"),
        }
    }
}

impl FromStr for SearchMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "keyword" => Ok(SearchMode::Keyword),
            "semantic" => Ok(SearchMode::Semantic),
            "hybrid" => Ok(SearchMode::Hybrid),
            _ => Err(format!(
                "Invalid SEARCH mode: {s:?}, expected \"keyword\", \"semantic\", or \"hybrid\""
            )),
        }
    }
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

/// Represents an EXPORT command that serializes matched concepts/propositions
/// into an idempotent UPSERT capsule for backup, migration, and agent-to-agent
/// knowledge exchange. Read-only.
///
/// Syntax: `EXPORT ?target WHERE { ... } [LIMIT N] [CURSOR "<token>"]`
#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub struct ExportCommand {
    /// The target variable bound in the WHERE clause (concept nodes and/or proposition links)
    pub target: String,
    /// WHERE clauses containing graph patterns and filters binding the target
    pub where_clauses: Vec<WhereClause>,
    /// Optional limit on the number of exported elements
    pub limit: Option<usize>,
    /// Optional CURSOR token to continue a paginated export where the
    /// previous page ended (KIP §5.3); each page is an independently valid,
    /// idempotent capsule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// Compares JSON scalar values using KIP filter ordering rules.
///
/// - Numbers compare numerically and exactly: integer/integer pairs compare
///   in `i128` (no `f64` collapse above 2^53) and integer/float pairs
///   compare without rounding the integer side.
/// - Booleans compare by boolean order; `null` compares only to `null`.
/// - Two strings first try numeric comparison, then datetime comparison
///   (each side parsed independently as RFC 3339 or RFC 2822, so the same
///   instant written in the two formats compares `Equal`), then fall back
///   to lexical ordering.
/// - A number and a string compare numerically when the string parses as a
///   JSON number, mirroring [`loose_equal`]'s numeric coercion.
/// - Every other pair (arrays, objects, remaining mixed types) has no
///   defined ordering and yields `None`.
///
/// Note this ordering is deliberately looser than [`loose_equal`] for
/// numeric strings: different spellings of the same numeric value (e.g.
/// `"1.10"` vs `"1.1"`) are `Ordering::Equal` here but not loosely equal.
pub fn compare_json(left: &Json, right: &Json) -> Option<Ordering> {
    match (left, right) {
        (Json::Number(a), Json::Number(b)) => compare_numbers(a, b),
        (Json::Bool(a), Json::Bool(b)) => Some(a.cmp(b)),
        (Json::Null, Json::Null) => Some(Ordering::Equal),
        (Json::String(a), Json::String(b)) => {
            // try to compare as numbers
            if let Ok(a) = Number::from_str(a)
                && let Ok(b) = Number::from_str(b)
            {
                return compare_numbers(&a, &b);
            }
            // try to compare as datetimes; each side is parsed independently
            // (RFC 3339 first, then RFC 2822) so the two formats compare
            // against each other by instant
            if let Some(a) = parse_datetime(a)
                && let Some(b) = parse_datetime(b)
            {
                return Some(a.cmp(&b));
            }

            Some(a.cmp(b))
        }
        (Json::Number(a), Json::String(b)) => {
            let b = Number::from_str(b).ok()?;
            compare_numbers(a, &b)
        }
        (Json::String(a), Json::Number(b)) => {
            let a = Number::from_str(a).ok()?;
            compare_numbers(&a, b)
        }
        _ => None,
    }
}

/// KIP loose equality: the semantics of the `==` / `!=` filter operators and
/// of list-membership tests such as the `IN` filter function.
///
/// Rules:
/// - **Number vs Number** — exact numeric equality. Integer/integer pairs
///   compare in `i128`, so `9007199254740993 == 9007199254740992` is `false`
///   even though both collapse to the same `f64`; integer/float pairs
///   compare without rounding the integer side (`3 == 3.0` is `true`).
/// - **String vs String** — equal when the strings are identical, or when
///   both parse as datetimes (RFC 3339 or RFC 2822, independently per side)
///   denoting the same instant. Numeric-looking strings are *not* loosely
///   equal (`"1.10" == "1.1"` and `"1e3" == "1000"` are `false`), though
///   they still *order* numerically via [`compare_json`].
/// - **Number vs String** — equal when the string parses as a JSON number
///   with the same exact numeric value (`3 == "3.0"` is `true`).
/// - **Bool vs Bool / Null vs Null** — standard equality; `null` equals only
///   `null`.
/// - **Array/Object vs Array/Object** — structural (`serde_json`) equality.
///   Non-scalars still have no ordering (KIP spec §2.7), so `[1] == [1]`
///   holds while `[1] <= [1]` does not.
/// - Any other mixed pair is not equal.
///
/// `ComparisonOperator::NotEqual` is always the exact negation of this
/// function.
pub fn loose_equal(left: &Json, right: &Json) -> bool {
    match (left, right) {
        (Json::Number(a), Json::Number(b)) => compare_numbers(a, b) == Some(Ordering::Equal),
        (Json::Bool(a), Json::Bool(b)) => a == b,
        (Json::Null, Json::Null) => true,
        (Json::String(a), Json::String(b)) => {
            if a == b {
                return true;
            }
            // Different spellings of the same instant are equal.
            if let Some(a) = parse_datetime(a)
                && let Some(b) = parse_datetime(b)
            {
                return a == b;
            }
            false
        }
        (Json::Number(a), Json::String(b)) => Number::from_str(b)
            .ok()
            .is_some_and(|b| compare_numbers(a, &b) == Some(Ordering::Equal)),
        (Json::String(a), Json::Number(b)) => Number::from_str(a)
            .ok()
            .is_some_and(|a| compare_numbers(&a, b) == Some(Ordering::Equal)),
        (Json::Array(_) | Json::Object(_), Json::Array(_) | Json::Object(_)) => left == right,
        _ => false,
    }
}

/// Compares two JSON numbers exactly.
///
/// Integer/integer pairs (any mix of `i64` / `u64` representations) compare
/// in `i128`; integer/float pairs compare via [`compare_i128_f64`] without
/// rounding the integer side; float/float pairs compare as `f64`.
fn compare_numbers(a: &Number, b: &Number) -> Option<Ordering> {
    match (number_as_i128(a), number_as_i128(b)) {
        (Some(x), Some(y)) => Some(x.cmp(&y)),
        (Some(x), None) => compare_i128_f64(x, b.as_f64()?),
        (None, Some(y)) => compare_i128_f64(y, a.as_f64()?).map(Ordering::reverse),
        (None, None) => a.as_f64()?.partial_cmp(&b.as_f64()?),
    }
}

/// Compares an integer with an `f64` without converting the integer to `f64`
/// (which would collapse distinct integers above 2^53).
fn compare_i128_f64(x: i128, y: f64) -> Option<Ordering> {
    if y.is_nan() {
        return None;
    }
    // 2^127 as f64. Finite doubles at or beyond ±2^127 lie outside the i128
    // range (and a fortiori outside the i64/u64 range `x` comes from), so
    // the sign decides; this also covers ±infinity.
    const I128_LIMIT: f64 = 170141183460469231731687303715884105728.0;
    if y >= I128_LIMIT {
        return Some(Ordering::Less);
    }
    if y < -I128_LIMIT {
        return Some(Ordering::Greater);
    }
    // `y` is finite with |y| <= 2^127, so `floor(y)` converts to i128 exactly.
    let floor = y.floor();
    match x.cmp(&(floor as i128)) {
        // x == floor(y): x < y iff y has a fractional part.
        Ordering::Equal if y > floor => Some(Ordering::Less),
        ord => Some(ord),
    }
}

/// Parses a datetime string as RFC 3339, falling back to RFC 2822.
///
/// Both formats are accepted on either side of a comparison, so the same
/// instant written as `"2025-01-01T00:00:00Z"` and
/// `"Wed, 01 Jan 2025 00:00:00 +0000"` compares equal.
fn parse_datetime(s: &str) -> Option<DateTime<FixedOffset>> {
    DateTime::parse_from_rfc3339(s)
        .or_else(|_| DateTime::parse_from_rfc2822(s))
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{cmp::Ordering, str::FromStr};

    #[test]
    fn find_expression_display_variable() {
        let expr = FindExpression::Variable(DotPathVar {
            var: "drug".to_string(),
            path: vec!["attributes".to_string(), "risk_level".to_string()],
        });

        assert_eq!(expr.to_string(), "?drug.attributes.risk_level");
    }

    #[test]
    fn find_expression_display_aggregation_without_distinct() {
        let expr = FindExpression::Aggregation {
            func: AggregationFunction::Count,
            var: DotPathVar {
                var: "drug".to_string(),
                path: vec![],
            },
            distinct: false,
        };

        assert_eq!(expr.to_string(), "COUNT(?drug)");
    }

    #[test]
    fn find_expression_display_aggregation_with_distinct() {
        let expr = FindExpression::Aggregation {
            func: AggregationFunction::Sum,
            var: DotPathVar {
                var: "drug".to_string(),
                path: vec!["score".to_string()],
            },
            distinct: true,
        };

        assert_eq!(expr.to_string(), "SUM(DISTINCT ?drug.score)");
    }

    #[test]
    fn value_conversions_display_and_accessors_cover_all_variants() {
        assert_eq!(Value::Null.to_string(), "null");
        assert_eq!(Value::Bool(true).to_string(), "true");
        assert_eq!(Value::Number(Number::from(7)).to_string(), "7");
        assert_eq!(Value::String("a\"b".to_string()).to_string(), r#""a\"b""#);

        assert_eq!(Json::from(Value::Null), Json::Null);
        assert_eq!(Json::from(Value::Bool(false)), Json::Bool(false));
        assert_eq!(Json::from(Value::Number(Number::from(3))), json!(3));
        assert_eq!(Json::from(Value::String("x".to_string())), json!("x"));

        assert_eq!(Value::from("borrowed"), Value::String("borrowed".into()));
        assert_eq!(
            Value::from("owned".to_string()),
            Value::String("owned".into())
        );
        assert_eq!(Value::from(true), Value::Bool(true));
        assert_eq!(
            Value::from(Number::from(42)),
            Value::Number(Number::from(42))
        );

        assert_eq!(Value::try_from(Json::Null).unwrap(), Value::Null);
        assert_eq!(Value::try_from(json!(true)).unwrap(), Value::Bool(true));
        assert_eq!(
            Value::try_from(json!(11)).unwrap(),
            Value::Number(Number::from(11))
        );
        assert_eq!(
            Value::try_from(json!("text")).unwrap(),
            Value::String("text".into())
        );
        assert!(
            Value::try_from(json!([1]))
                .unwrap_err()
                .contains("Unsupported")
        );

        assert_eq!(
            Value::String("s".into()).into_opt_string().unwrap(),
            Some("s".into())
        );
        assert_eq!(Value::Null.into_opt_string().unwrap(), None);
        assert!(Value::Bool(true).into_opt_string().is_err());

        assert_eq!(
            Value::Number(Number::from(5)).into_opt_number().unwrap(),
            Some(Number::from(5))
        );
        assert_eq!(Value::Null.into_opt_number().unwrap(), None);
        assert!(Value::String("bad".into()).into_opt_number().is_err());

        assert_eq!(Value::Bool(false).into_opt_bool().unwrap(), Some(false));
        assert_eq!(Value::Null.into_opt_bool().unwrap(), None);
        assert!(Value::Number(Number::from(1)).into_opt_bool().is_err());

        assert_eq!(Value::String("s".into()).as_string(), Some("s".into()));
        assert_eq!(Value::Null.as_string(), None);
        assert_eq!(
            Value::Number(Number::from(9)).as_number(),
            Some(Number::from(9))
        );
        assert_eq!(Value::Null.as_number(), None);
        assert_eq!(Value::Bool(true).as_bool(), Some(true));
        assert_eq!(Value::Null.as_bool(), None);

        assert!(Value::String("s".into()).is_string());
        assert!(Value::Number(Number::from(1)).is_number());
        assert!(Value::Bool(true).is_bool());
        assert!(Value::Null.is_null());
    }

    #[test]
    fn command_type_display_parse_serde_and_from_command() {
        assert_eq!(CommandType::Kql.to_string(), "KQL");
        assert_eq!(CommandType::Kml.to_string(), "KML");
        assert_eq!(CommandType::Meta.to_string(), "META");
        assert_eq!(CommandType::Unknown.to_string(), "UNKNOWN");

        assert_eq!(CommandType::from_str("kql").unwrap(), CommandType::Kql);
        assert_eq!(CommandType::from_str("KML").unwrap(), CommandType::Kml);
        assert_eq!(CommandType::from_str("meta").unwrap(), CommandType::Meta);
        assert_eq!(
            CommandType::from_str("other").unwrap(),
            CommandType::Unknown
        );

        let serialized = serde_json::to_string(&CommandType::Kml).unwrap();
        assert_eq!(serialized, r#""KML""#);
        assert_eq!(
            serde_json::from_str::<CommandType>(&serialized).unwrap(),
            CommandType::Kml
        );

        let kql = Command::Kql(KqlQuery {
            find_clause: FindClause {
                expressions: vec![],
            },
            where_clauses: vec![],
            order_by: None,
            limit: None,
            cursor: None,
        });
        let kml = Command::Kml(KmlStatement::Upsert(vec![]));
        let meta = Command::Meta(MetaCommand::Describe(DescribeTarget::Primer));
        assert_eq!(CommandType::from(&kql), CommandType::Kql);
        assert_eq!(CommandType::from(&kml), CommandType::Kml);
        assert_eq!(CommandType::from(&meta), CommandType::Meta);
    }

    #[test]
    fn concept_matcher_dot_path_and_comparison_helpers_cover_edges() {
        assert_eq!(
            ConceptMatcher::ID("id1".into()).to_string(),
            r#"{id: "id1"}"#
        );
        assert_eq!(
            ConceptMatcher::Type("Drug".into()).to_string(),
            r#"{type: "Drug"}"#
        );
        assert_eq!(
            ConceptMatcher::Name("Aspirin".into()).to_string(),
            r#"{name: "Aspirin"}"#
        );
        assert_eq!(
            ConceptMatcher::Object {
                r#type: "Drug".into(),
                name: "Aspirin".into(),
            }
            .to_string(),
            r#"{type: "Drug", name: "Aspirin"}"#
        );

        assert!(ConceptMatcher::ID("id1".into()).is_unique());
        assert!(
            ConceptMatcher::Object {
                r#type: "Drug".into(),
                name: "Aspirin".into(),
            }
            .is_unique()
        );
        assert!(!ConceptMatcher::Type("Drug".into()).is_unique());

        let invalid = ConceptMatcher::try_from(vec![
            KeyValue {
                key: "id".into(),
                value: "id1".into(),
            },
            KeyValue {
                key: "type".into(),
                value: "Drug".into(),
            },
        ])
        .unwrap_err();
        assert!(invalid.contains("cannot have both id"));
        assert!(
            ConceptMatcher::try_from(vec![KeyValue {
                key: "name".into(),
                value: Value::Null,
            }])
            .unwrap_err()
            .contains("must have at least one")
        );

        let escaped = DotPathVar {
            var: "node".into(),
            path: vec!["a/b".into(), "c~d".into()],
        };
        assert_eq!(escaped.to_pointer(), "/a~1b/c~0d");
        assert_eq!(escaped.to_pointer_or("ignored"), "/a~1b/c~0d");
        let whole_doc = DotPathVar {
            var: "node".into(),
            path: vec![],
        };
        assert_eq!(whole_doc.to_pointer(), "");
        assert_eq!(whole_doc.to_pointer_or("a/b"), "/a~1b");

        let value = json!(2);
        assert!(ComparisonOperator::Equal.compare(&value, &json!(2)));
        assert!(ComparisonOperator::GreaterEqual.compare(&value, &json!(2)));
        assert!(!ComparisonOperator::GreaterEqual.compare(&json!(1), &json!(2)));
        assert!(!ComparisonOperator::LessThan.compare(&json!("x"), &json!(2)));
    }

    #[test]
    fn update_function_calculate_covers_numeric_and_null_semantics() {
        // Integer arithmetic is preserved when all operands are integers.
        assert_eq!(
            UpdateFunction::Add.calculate(&[json!(5), json!(1)]),
            json!(6)
        );
        assert_eq!(
            UpdateFunction::Add.calculate(&[json!(5), json!(-2)]),
            json!(3)
        );
        assert_eq!(
            UpdateFunction::Mul.calculate(&[json!(4), json!(3)]),
            json!(12)
        );
        // Mixed integer/float falls back to float arithmetic.
        assert_eq!(
            UpdateFunction::Mul.calculate(&[json!(0.5), json!(4)]),
            json!(2.0)
        );
        // CLAMP constrains into [lo, hi], integer-preserving when possible.
        assert_eq!(
            UpdateFunction::Clamp.calculate(&[json!(15), json!(0), json!(10)]),
            json!(10)
        );
        assert_eq!(
            UpdateFunction::Clamp.calculate(&[json!(1.2), json!(0.0), json!(1.0)]),
            json!(1.0)
        );
        // COALESCE returns the first non-null operand.
        assert_eq!(
            UpdateFunction::Coalesce.calculate(&[Json::Null, json!(0)]),
            json!(0)
        );
        assert_eq!(
            UpdateFunction::Coalesce.calculate(&[json!(7), json!(0)]),
            json!(7)
        );
        // A null or non-number operand yields null (the key is then skipped).
        assert_eq!(
            UpdateFunction::Add.calculate(&[Json::Null, json!(1)]),
            Json::Null
        );
        assert_eq!(
            UpdateFunction::Mul.calculate(&[json!("text"), json!(2)]),
            Json::Null
        );
        assert_eq!(
            UpdateFunction::Clamp.calculate(&[json!(1), json!(10), json!(0)]),
            Json::Null // lo > hi
        );
    }

    #[test]
    fn update_expr_evaluate_resolves_target_paths() {
        // ADD(COALESCE(?t.attributes.count, 0), 1) — the reinforcement idiom.
        let expr = UpdateExpr::Function {
            func: UpdateFunction::Add,
            args: vec![
                UpdateExpr::Function {
                    func: UpdateFunction::Coalesce,
                    args: vec![
                        UpdateExpr::Variable(DotPathVar {
                            var: "t".to_string(),
                            path: vec!["attributes".to_string(), "count".to_string()],
                        }),
                        UpdateExpr::Number(Number::from(0)),
                    ],
                },
                UpdateExpr::Number(Number::from(1)),
            ],
        };

        // Missing counter initializes via COALESCE.
        assert_eq!(expr.evaluate(&|_| Json::Null), json!(1));
        // Existing integer counter increments without losing integerness.
        assert_eq!(expr.evaluate(&|_| json!(41)), json!(42));
        // Non-numeric state yields null (key skipped).
        assert_eq!(expr.evaluate(&|_| json!("not a number")), Json::Null);

        assert_eq!(
            expr.referenced_paths()
                .into_iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>(),
            vec!["?t.attributes.count".to_string()]
        );

        // CLAMP(MUL(?t.metadata.confidence, 0.9), 0.0, 1.0) — the decay idiom.
        let decay = UpdateExpr::Function {
            func: UpdateFunction::Clamp,
            args: vec![
                UpdateExpr::Function {
                    func: UpdateFunction::Mul,
                    args: vec![
                        UpdateExpr::Variable(DotPathVar {
                            var: "t".to_string(),
                            path: vec!["metadata".to_string(), "confidence".to_string()],
                        }),
                        UpdateExpr::Number(Number::from_f64(0.9).unwrap()),
                    ],
                },
                UpdateExpr::Number(Number::from_f64(0.0).unwrap()),
                UpdateExpr::Number(Number::from_f64(1.0).unwrap()),
            ],
        };
        assert_eq!(decay.evaluate(&|_| json!(0.5)), json!(0.45));
        assert_eq!(decay.evaluate(&|_| json!(2.0)), json!(1.0));
        assert_eq!(decay.evaluate(&|_| Json::Null), Json::Null);
    }

    #[test]
    fn search_mode_display_and_from_str_roundtrip() {
        for (mode, s) in [
            (SearchMode::Keyword, "keyword"),
            (SearchMode::Semantic, "semantic"),
            (SearchMode::Hybrid, "hybrid"),
        ] {
            assert_eq!(mode.to_string(), s);
            assert_eq!(SearchMode::from_str(s).unwrap(), mode);
            assert_eq!(SearchMode::from_str(&s.to_ascii_uppercase()).unwrap(), mode);
        }
        assert!(SearchMode::from_str("fuzzy").is_err());
    }

    #[test]
    fn aggregation_display_and_json_comparison_cover_remaining_branches() {
        let var = DotPathVar {
            var: "drug".into(),
            path: vec![],
        };
        for (func, expected) in [
            (AggregationFunction::Avg, "AVG(?drug)"),
            (AggregationFunction::Min, "MIN(?drug)"),
            (AggregationFunction::Max, "MAX(?drug)"),
        ] {
            let expr = FindExpression::Aggregation {
                func,
                var: var.clone(),
                distinct: false,
            };
            assert_eq!(expr.to_string(), expected);
        }

        let values = vec![json!(1), json!(2), json!(2), json!("skip")];
        assert_eq!(
            AggregationFunction::Count.calculate(&values, true),
            json!(3)
        );
        assert_eq!(
            AggregationFunction::Avg.calculate(&values, false),
            json!(5.0 / 3.0)
        );
        // MIN/MAX return the original input value: integers stay integers.
        assert_eq!(AggregationFunction::Min.calculate(&values, false), json!(1));
        assert_eq!(AggregationFunction::Max.calculate(&values, false), json!(2));
        assert_eq!(
            AggregationFunction::Avg.calculate(&vec![json!("x")], false),
            Json::Null
        );

        assert_eq!(
            compare_json(&json!(false), &json!(true)),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare_json(&Json::Null, &Json::Null),
            Some(Ordering::Equal)
        );
        assert_eq!(
            compare_json(&json!("9"), &json!("10")),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare_json(
                &json!("2025-01-01T00:00:00Z"),
                &json!("2025-01-02T00:00:00Z")
            ),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare_json(
                &json!("Tue, 1 Jul 2003 10:52:37 +0200"),
                &json!("Tue, 1 Jul 2003 10:53:37 +0200")
            ),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare_json(&json!("abc"), &json!("abd")),
            Some(Ordering::Less)
        );
        assert_eq!(compare_json(&json!("abc"), &json!(1)), None);
    }

    #[test]
    fn comparison_equality_and_ordering_are_consistent() {
        use ComparisonOperator::*;

        // Numbers compare numerically regardless of int/float representation:
        // equality can never contradict the ordering operators.
        assert!(Equal.compare(&json!(3.0), &json!(3)));
        assert!(!NotEqual.compare(&json!(3.0), &json!(3)));
        assert!(LessEqual.compare(&json!(3.0), &json!(3)));
        assert!(GreaterEqual.compare(&json!(3.0), &json!(3)));
        assert!(!LessThan.compare(&json!(3.0), &json!(3)));
        assert!(!GreaterThan.compare(&json!(3.0), &json!(3)));
        assert!(Equal.compare(&json!(1), &json!(1)));
        assert!(NotEqual.compare(&json!(1), &json!(2)));

        // String-string equality is exact: different spellings of the same
        // numeric value are NOT equal (no numeric loose equality), but they
        // still *order* as numerically equal via compare_json.
        assert!(!Equal.compare(&json!("3"), &json!("3.0")));
        assert!(NotEqual.compare(&json!("3"), &json!("3.0")));
        assert!(LessEqual.compare(&json!("3"), &json!("3.0")));
        assert!(GreaterEqual.compare(&json!("3"), &json!("3.0")));
        assert!(!Equal.compare(&json!("1.10"), &json!("1.1")));
        assert!(NotEqual.compare(&json!("1.10"), &json!("1.1")));
        assert!(!Equal.compare(&json!("1e3"), &json!("1000")));
        assert!(NotEqual.compare(&json!("1e3"), &json!("1000")));
        // Identical strings are equal, of course.
        assert!(Equal.compare(&json!("3.0"), &json!("3.0")));
        assert!(!NotEqual.compare(&json!("3.0"), &json!("3.0")));

        // Different spellings of the same instant are equal, like `<=`/`>=`.
        let z = json!("2025-01-01T00:00:00Z");
        let offset = json!("2025-01-01T00:00:00+00:00");
        assert!(Equal.compare(&z, &offset));
        assert!(!NotEqual.compare(&z, &offset));
        assert!(LessEqual.compare(&z, &offset));
        assert!(!LessThan.compare(&z, &offset));

        // Number vs numeric string coerces numerically for both equality and
        // ordering (a == b implies a <= b and a >= b).
        assert!(Equal.compare(&json!("3"), &json!(3)));
        assert!(!NotEqual.compare(&json!("3"), &json!(3)));
        assert!(LessEqual.compare(&json!("3"), &json!(3)));
        assert!(GreaterEqual.compare(&json!("3"), &json!(3)));
        assert!(Equal.compare(&json!(3), &json!("3.0")));
        assert!(LessThan.compare(&json!(2), &json!("3")));
        assert!(GreaterThan.compare(&json!("10"), &json!(9)));
        // A non-numeric string never coerces.
        assert!(!Equal.compare(&json!("abc"), &json!(3)));
        assert!(NotEqual.compare(&json!("abc"), &json!(3)));
        assert!(!LessEqual.compare(&json!("abc"), &json!(3)));

        // Other mixed types have no defined ordering: never Equal, never
        // ordered, only NotEqual is true.
        assert!(!Equal.compare(&json!(true), &json!(1)));
        assert!(NotEqual.compare(&json!(true), &json!(1)));
        assert!(!LessEqual.compare(&json!(true), &json!(1)));

        // Arrays/objects compare by structural equality for `==`/`!=` but
        // stay unordered in FILTER (KIP spec §2.7).
        assert!(Equal.compare(&json!([1]), &json!([1])));
        assert!(!NotEqual.compare(&json!([1]), &json!([1])));
        assert!(!Equal.compare(&json!([1]), &json!([2])));
        assert!(NotEqual.compare(&json!([1]), &json!([2])));
        assert!(Equal.compare(&json!({"a": 1}), &json!({"a": 1})));
        assert!(!NotEqual.compare(&json!({"a": 1}), &json!({"a": 1})));
        assert!(!Equal.compare(&json!({"a": 1}), &json!({"a": 2})));
        assert!(!LessEqual.compare(&json!([1]), &json!([1])));
        assert!(!GreaterEqual.compare(&json!([1]), &json!([1])));
        assert!(!LessThan.compare(&json!([1]), &json!([2])));
        // A scalar never equals a non-scalar.
        assert!(!Equal.compare(&json!([1]), &json!(1)));
        assert!(NotEqual.compare(&json!([1]), &json!(1)));

        // Null equals only null.
        assert!(Equal.compare(&Json::Null, &Json::Null));
        assert!(NotEqual.compare(&Json::Null, &json!(0)));
    }

    #[test]
    fn number_comparison_is_exact_beyond_f64_precision() {
        use ComparisonOperator::*;

        // 2^53 and 2^53 + 1 collapse to the same f64; they must not compare
        // equal (regression for the as_f64-based comparison).
        let a = json!(9_007_199_254_740_993i64); // 2^53 + 1
        let b = json!(9_007_199_254_740_992i64); // 2^53
        assert!(!Equal.compare(&a, &b));
        assert!(NotEqual.compare(&a, &b));
        assert!(GreaterThan.compare(&a, &b));
        assert!(!LessEqual.compare(&a, &b));
        assert_eq!(compare_json(&a, &b), Some(Ordering::Greater));

        // u64 boundary: distinct values near u64::MAX stay distinct.
        let hi = json!(u64::MAX);
        let lo = json!(u64::MAX - 1);
        assert!(!Equal.compare(&hi, &lo));
        assert!(GreaterThan.compare(&hi, &lo));
        assert!(Equal.compare(&hi, &json!(u64::MAX)));
        // Cross-signedness: i64::MIN < u64::MAX (compared in i128).
        assert_eq!(
            compare_json(&json!(i64::MIN), &json!(u64::MAX)),
            Some(Ordering::Less)
        );

        // Integer vs float compares without rounding the integer side:
        // 2^53 + 1 (int) > 2^53 (f64), even though (2^53 + 1) as f64 == 2^53.
        let f = json!(9_007_199_254_740_992.0f64);
        assert!(GreaterThan.compare(&a, &f));
        assert!(!Equal.compare(&a, &f));
        assert!(Equal.compare(&b, &f));
        // Small values keep the intuitive behavior.
        assert!(Equal.compare(&json!(3), &json!(3.0)));
        assert!(LessThan.compare(&json!(3), &json!(3.5)));
        assert!(GreaterThan.compare(&json!(4), &json!(3.5)));
        assert!(LessThan.compare(&json!(-4), &json!(-3.5)));
        // Huge floats order by sign against any integer.
        assert!(LessThan.compare(&json!(u64::MAX), &json!(1e300)));
        assert!(GreaterThan.compare(&json!(i64::MIN), &json!(-1e300)));

        // MIN/MAX pick the true extremum (regression: the f64-collapsing
        // comparator reported Equal and returned the wrong element).
        let values = vec![b.clone(), a.clone(), json!(1)];
        assert_eq!(AggregationFunction::Min.calculate(&values, false), json!(1));
        assert_eq!(AggregationFunction::Max.calculate(&values, false), a);
        let values = vec![a.clone(), b.clone()];
        assert_eq!(AggregationFunction::Min.calculate(&values, false), b);
        assert_eq!(AggregationFunction::Max.calculate(&values, false), a);
    }

    #[test]
    fn loose_equal_covers_strings_numbers_and_non_scalars() {
        // Number/number: exact.
        assert!(loose_equal(&json!(3), &json!(3.0)));
        assert!(!loose_equal(
            &json!(9_007_199_254_740_993i64),
            &json!(9_007_199_254_740_992i64)
        ));

        // String/string: exact, except datetime instants.
        assert!(loose_equal(&json!("abc"), &json!("abc")));
        assert!(!loose_equal(&json!("1.10"), &json!("1.1")));
        assert!(!loose_equal(&json!("1e3"), &json!("1000")));
        assert!(loose_equal(
            &json!("2025-01-01T00:00:00Z"),
            &json!("2025-01-01T00:00:00+00:00")
        ));

        // Number/string numeric coercion.
        assert!(loose_equal(&json!(1000), &json!("1e3")));
        assert!(loose_equal(&json!("3.0"), &json!(3)));
        assert!(!loose_equal(&json!("3.5"), &json!(3)));
        assert!(!loose_equal(&json!("abc"), &json!(3)));

        // Non-scalars: structural equality, in both nesting shapes.
        assert!(loose_equal(&json!([1, {"a": 2}]), &json!([1, {"a": 2}])));
        assert!(!loose_equal(&json!([1, 2]), &json!([2, 1])));
        assert!(loose_equal(&json!({"a": [1]}), &json!({"a": [1]})));
        assert!(!loose_equal(&json!([1]), &json!({"a": 1})));

        // Mixed scalar kinds are never equal.
        assert!(!loose_equal(&json!(true), &json!(1)));
        assert!(!loose_equal(&json!(true), &json!("true")));
        assert!(!loose_equal(&Json::Null, &json!("null")));
        assert!(!loose_equal(&json!([1]), &json!(1)));
    }

    #[test]
    fn datetime_comparison_works_across_rfc_formats() {
        use ComparisonOperator::*;

        // The same instant written as RFC 3339 and RFC 2822 is equal.
        let rfc3339 = json!("2025-01-01T00:00:00Z");
        let rfc2822 = json!("Wed, 01 Jan 2025 00:00:00 +0000");
        assert!(Equal.compare(&rfc3339, &rfc2822));
        assert!(!NotEqual.compare(&rfc3339, &rfc2822));
        assert!(LessEqual.compare(&rfc3339, &rfc2822));
        assert!(GreaterEqual.compare(&rfc3339, &rfc2822));
        assert!(!LessThan.compare(&rfc3339, &rfc2822));
        assert_eq!(compare_json(&rfc3339, &rfc2822), Some(Ordering::Equal));

        // Distinct instants order correctly across formats, both directions.
        let later_3339 = json!("2025-01-02T00:00:00Z");
        let later_2822 = json!("Thu, 02 Jan 2025 00:00:00 +0000");
        assert!(LessThan.compare(&rfc2822, &later_3339));
        assert!(LessThan.compare(&rfc3339, &later_2822));
        assert!(GreaterThan.compare(&later_2822, &rfc3339));
        assert_eq!(
            compare_json(&rfc2822, &later_3339),
            Some(Ordering::Less)
        );

        // Offsets are honoured: 01:00+01:00 is the same instant as 00:00Z.
        let offset_2822 = json!("Wed, 01 Jan 2025 01:00:00 +0100");
        assert!(Equal.compare(&rfc3339, &offset_2822));
    }

    #[test]
    fn concept_matcher_rejects_duplicate_keys() {
        let kv = |k: &str, v: &str| KeyValue {
            key: k.to_string(),
            value: Value::String(v.to_string()),
        };

        let err = ConceptMatcher::try_from(vec![kv("type", "A"), kv("type", "B")]).unwrap_err();
        assert!(err.contains("Duplicate key"), "unexpected error: {err}");

        let err = ConceptMatcher::try_from(vec![kv("id", "1"), kv("id", "2")]).unwrap_err();
        assert!(err.contains("Duplicate key"), "unexpected error: {err}");

        // Non-duplicate combinations still work.
        assert_eq!(
            ConceptMatcher::try_from(vec![kv("type", "A"), kv("name", "B")]).unwrap(),
            ConceptMatcher::Object {
                r#type: "A".to_string(),
                name: "B".to_string()
            }
        );
    }

    #[test]
    fn aggregation_sum_avg_boundary_semantics() {
        // All-integer inputs keep integer precision beyond 2^53.
        let big = 9_007_199_254_740_993i64; // 2^53 + 1, not representable in f64
        let values = vec![json!(big), json!(1), json!("skip")];
        assert_eq!(
            AggregationFunction::Sum.calculate(&values, false),
            json!(big + 1)
        );

        // Empty (or non-numeric-only) input sums to integer 0.
        assert_eq!(AggregationFunction::Sum.calculate(&vec![], false), json!(0));
        assert_eq!(
            AggregationFunction::Sum.calculate(&vec![json!("x")], false),
            json!(0)
        );

        // Mixed int/float input falls back to f64.
        assert_eq!(
            AggregationFunction::Sum.calculate(&vec![json!(1), json!(0.5)], false),
            json!(1.5)
        );

        // f64 overflow (non-finite sum) yields Null instead of a bogus number.
        assert_eq!(
            AggregationFunction::Sum.calculate(&vec![json!(f64::MAX), json!(f64::MAX)], false),
            Json::Null
        );

        // AVG: empty input is Null, otherwise f64.
        assert_eq!(
            AggregationFunction::Avg.calculate(&vec![], false),
            Json::Null
        );
        assert_eq!(
            AggregationFunction::Avg.calculate(&vec![json!(1), json!(2)], false),
            json!(1.5)
        );

        // MIN/MAX: empty input is Null; original values are preserved.
        assert_eq!(
            AggregationFunction::Min.calculate(&vec![], false),
            Json::Null
        );
        assert_eq!(
            AggregationFunction::Max.calculate(&vec![json!(big), json!(1)], false),
            json!(big)
        );
    }
}
