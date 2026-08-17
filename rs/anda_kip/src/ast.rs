//! # The executable KIP 2.0 AST
//!
//! This is the tree a KIP engine consumes: every construct is already collapsed
//! to the one shape it means, with the open-ended parts of the grammar closed.
//!
//! - a predicate is an atom or a path, not a nested alternation/quantifier tree;
//! - a filter is a comparison, a logical node, a negation, or a call to one of
//!   the registered functions, not a general expression tree;
//! - a variable is a name and a path of steps, not a chain of member accesses;
//! - `ASSERT` is gone: the parser desugars it into the parts it is defined as.
//!
//! A consumer matching on these enums is total: there is no "some other function
//! name" case to defend against, because the parser rejected it.
//!
//! The encoding is serde's default externally-tagged enum representation, which
//! makes this tree field-for-field identical to the `exec-ast.ts` contract of the
//! reference toolkit `@ldclabs/kip-lang`, so the two implementations can be
//! differentially tested against one another.

use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt, str::FromStr};

pub use serde_json::{Map, Number};

/// Alias for [`serde_json::Value`], used wherever KIP carries opaque JSON.
pub type Json = serde_json::Value;

/// An `object_pattern` — an open, schema-validated field map.
///
/// Unlike KIP 1.x, v2 does not close this to a fixed set of identity forms:
/// which fields identify an element is Schema's decision, not the grammar's.
pub type ObjectMatcher = BTreeMap<String, MatchValue>;

/// Assignment pairs, kept ordered so lowering stays deterministic.
pub type Assignments = Vec<(String, MutationValue)>;

/// A `{...}` block whose values may still contain unbound parameters.
pub type BoundObject = BTreeMap<String, BoundValue>;

// ---------------------------------------------------------------------------
// Values
// ---------------------------------------------------------------------------

/// A KIP literal.
///
/// Arrays and objects are not baseline Core Literals (Spec §9.2); they appear
/// here only as the option/assignment payloads that the grammar admits.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub enum KipValue {
    /// The absence of a value (Spec §9.5).
    #[default]
    Null,
    /// A boolean.
    Bool(bool),
    /// A finite JSON number (Spec §9.3).
    Number(Number),
    /// A UTF-8 string.
    String(String),
    /// A JSON array, legal only in payload positions.
    Array(Vec<KipValue>),
    /// A JSON object, legal only in payload positions.
    Object(BTreeMap<String, KipValue>),
}

impl fmt::Display for KipValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", Json::from(self.clone()))
    }
}

impl From<KipValue> for Json {
    fn from(value: KipValue) -> Self {
        match value {
            KipValue::Null => Json::Null,
            KipValue::Bool(b) => Json::Bool(b),
            KipValue::Number(n) => Json::Number(n),
            KipValue::String(s) => Json::String(s),
            KipValue::Array(items) => Json::Array(items.into_iter().map(Json::from).collect()),
            KipValue::Object(fields) => Json::Object(
                fields
                    .into_iter()
                    .map(|(k, v)| (k, Json::from(v)))
                    .collect(),
            ),
        }
    }
}

impl TryFrom<Json> for KipValue {
    type Error = String;

    fn try_from(value: Json) -> Result<Self, Self::Error> {
        Ok(match value {
            Json::Null => KipValue::Null,
            Json::Bool(b) => KipValue::Bool(b),
            Json::Number(n) => {
                // Only finite numbers are valid KIP literals (Spec §9.3);
                // serde_json cannot represent a non-finite number, but an
                // arbitrary-precision build can, so the check is not vacuous.
                if n.as_f64().is_some_and(|f| !f.is_finite()) {
                    return Err(format!("{n} is not a finite KIP number"));
                }
                KipValue::Number(n)
            }
            Json::String(s) => KipValue::String(s),
            Json::Array(items) => KipValue::Array(
                items
                    .into_iter()
                    .map(KipValue::try_from)
                    .collect::<Result<_, _>>()?,
            ),
            Json::Object(fields) => KipValue::Object(
                fields
                    .into_iter()
                    .map(|(k, v)| KipValue::try_from(v).map(|v| (k, v)))
                    .collect::<Result<_, _>>()?,
            ),
        })
    }
}

impl From<&str> for KipValue {
    fn from(s: &str) -> Self {
        KipValue::String(s.to_string())
    }
}

impl From<String> for KipValue {
    fn from(s: String) -> Self {
        KipValue::String(s)
    }
}

impl From<bool> for KipValue {
    fn from(b: bool) -> Self {
        KipValue::Bool(b)
    }
}

impl From<i64> for KipValue {
    fn from(n: i64) -> Self {
        KipValue::Number(Number::from(n))
    }
}

impl From<u64> for KipValue {
    fn from(n: u64) -> Self {
        KipValue::Number(Number::from(n))
    }
}

/// A `data_value`: a value that may still contain unbound parameters.
///
/// The grammar admits `parameter` at every depth of an array or object, so no
/// assignment, option block or epistemic setting is plain JSON. A subtree with
/// nothing left to bind collapses to [`BoundValue::Value`]; anything else keeps
/// its shape so the runtime envelope can fill the holes without touching text.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum BoundValue {
    /// A fully literal subtree.
    Value(KipValue),
    /// A `:parameter`, bound from the request envelope.
    Param(String),
    /// A `?handle` naming an element created by this mutation plan.
    Handle(String),
    /// A read of the target element's own field.
    Variable(DotPathVar),
    /// An array with at least one unbound element.
    Array(Vec<BoundValue>),
    /// An object with at least one unbound member, kept ordered.
    Object(Vec<(String, BoundValue)>),
}

/// A value slot the grammar spells `parameter | literal`.
///
/// KIP 2.0 parameters are structurally bound data, never string-spliced, so an
/// unbound `:name` survives lowering as a [`Scalar::Param`] for the runtime
/// envelope to fill — it is not an error and never becomes text.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum Scalar {
    /// A literal written inline.
    Literal(KipValue),
    /// A `:parameter` to be bound at execution time.
    Param(String),
}

/// A schema symbol: `string_literal | parameter`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum SymbolRef {
    /// A quoted schema symbol, resolved against the Schema Environment.
    Name(String),
    /// A `:parameter` standing for a schema symbol.
    Param(String),
}

/// A mutation target: `variable | parameter | string_literal`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum ElementRef {
    /// A `?handle` bound by this mutation plan or by the statement's WHERE.
    Handle(String),
    /// A `:parameter` carrying an element reference.
    Param(String),
    /// A literal element id.
    Id(String),
}

/// `?var` plus a resolved path, e.g. `?x.facets["MnemonicState"].salience`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct DotPathVar {
    /// The bare variable name, without the `?` sigil.
    pub var: String,
    /// The resolved access path.
    pub path: Vec<PathStep>,
}

impl fmt::Display for DotPathVar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "?{}", self.var)?;
        for step in &self.path {
            match step {
                PathStep::Field(name) => write!(f, ".{name}")?,
                PathStep::Key(key) => write!(f, "[{}]", Json::String(key.clone()))?,
            }
        }
        Ok(())
    }
}

/// A dot step names a field; an index step keys into a map-valued field.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum PathStep {
    /// `.name`
    Field(String),
    /// `["key"]`
    Key(String),
}

// ---------------------------------------------------------------------------
// Shared terms
// ---------------------------------------------------------------------------

/// A `predicate_atom` — the exact predicate slot.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum PredAtom {
    /// A `?variable` predicate, legal only in read patterns.
    Variable(String),
    /// A quoted predicate symbol.
    Literal(String),
    /// A `:parameter` standing for a predicate.
    Param(String),
}

/// A hop range written as `{n}` / `{n,}` / `{n,m}`.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct HopRange {
    /// Minimum hop count.
    pub min: u32,
    /// Maximum hop count; `None` means unbounded.
    pub max: Option<u32>,
}

/// One atom of a raw predicate path, with its optional quantifier.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub struct PredPathAtom {
    /// The predicate to traverse.
    pub predicate: PredAtom,
    /// The hop quantifier, when one was written.
    pub hops: Option<HopRange>,
}

/// The predicate slot of a Proposition expression.
///
/// [`PredTerm::Atom`] is the plain predicate every language accepts.
/// [`PredTerm::Path`] carries the KQL-only traversal forms — alternation and hop
/// quantifiers — which never propagate belief and are rejected in KML and
/// EXPORT selections (Spec §45).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum PredTerm {
    /// One exact predicate.
    Atom(PredAtom),
    /// A traversal path of one or more alternatives.
    Path(Vec<PredPathAtom>),
}

/// One endpoint of a tuple.
///
/// A term may itself be a tuple: KIP states things about statements.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum Term {
    /// A `?variable` binding.
    Variable(String),
    /// A `:parameter`.
    Param(String),
    /// A literal value.
    Literal(KipValue),
    /// An inline `{field: value}` matcher.
    Match(ObjectMatcher),
    /// A nested Proposition expression.
    ///
    /// Boxed to break the `Term → Proposition → Triple → Term` cycle: KIP states
    /// things about statements, so the recursion is the point, not an accident.
    /// `Box` is transparent to serde, so the encoding is unchanged.
    Proposition(Box<PropositionMatcher>),
}

/// A value inside an [`ObjectMatcher`].
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum MatchValue {
    /// A `?variable` binding.
    Variable(String),
    /// A `:parameter`.
    Param(String),
    /// A literal value.
    Literal(KipValue),
    /// An array of match values.
    Array(Vec<MatchValue>),
    /// A nested matcher.
    Match(ObjectMatcher),
    /// A nested Proposition expression.
    Proposition(PropositionMatcher),
}

/// A `(subject, predicate, object)` tuple.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct PropositionTriple {
    /// The subject endpoint, always an Element reference.
    pub subject: Term,
    /// The predicate.
    pub predicate: PredTerm,
    /// The object endpoint.
    pub object: Term,
}

/// The Proposition expression slot (Spec §43.2).
///
/// [`PropositionMatcher::Tuple`] addresses a Proposition by structure,
/// [`PropositionMatcher::Id`] by record identity. Both live in the same slot,
/// which is why an id reference works everywhere a triple does — including as a
/// [`Term`] endpoint. `Id` is match-only: it never resolves-or-creates, so the
/// parser rejects it in `ENSURE PROPOSITION` and in the `ASSERT` sugar that
/// desugars through it.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum PropositionMatcher {
    /// Addressed by structure.
    Tuple(PropositionTriple),
    /// Addressed by record identity.
    Id(Scalar),
}

// ---------------------------------------------------------------------------
// Command
// ---------------------------------------------------------------------------

/// One parsed KIP command.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum Command {
    /// A KQL read.
    Kql(KqlQuery),
    /// A KML mutation transaction.
    Kml(KmlStatement),
    /// A META introspection command.
    Meta(MetaCommand),
}

impl Command {
    /// Whether executing this command can change durable state.
    ///
    /// The runtime classifies actual semantics rather than trusting a
    /// caller-supplied language label (Spec §73.1).
    pub fn is_mutation(&self) -> bool {
        matches!(self, Command::Kml(_))
    }
}

/// The language family a command belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommandType {
    /// KQL — the read language.
    Kql,
    /// KML — the cognitive mutation language.
    Kml,
    /// META — introspection, grounding, verification, history, export.
    Meta,
    /// The command failed to parse, so its family is unknown.
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
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for CommandType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        CommandType::from_str(&s).map_err(serde::de::Error::custom)
    }
}

// ---------------------------------------------------------------------------
// KQL
// ---------------------------------------------------------------------------

/// A `FIND ... WHERE ...` query.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct KqlQuery {
    /// The projection list.
    pub find_clause: FindClause,
    /// The solution patterns.
    pub where_clauses: Vec<WhereClause>,
    /// Cognitive history basis — what the Brain contained/believed then.
    pub as_of: Option<AsOf>,
    /// World-valid time — what was applicable then. An independent axis.
    pub for_time: Option<Scalar>,
    /// `WITH EPISTEMIC {...}` projection settings.
    pub epistemic: Option<BoundObject>,
    /// The sort keys.
    pub order_by: Option<Vec<OrderByItem>>,
    /// The result cap.
    pub limit: Option<Scalar>,
    /// The pagination cursor.
    pub cursor: Option<Scalar>,
}

/// `AS OF SEQ|TX|TIME` — which cognitive history the read runs against.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum AsOf {
    /// A Space sequence coordinate.
    Seq(Scalar),
    /// A transaction coordinate.
    Tx(Scalar),
    /// An engine-time coordinate.
    Time(Scalar),
}

/// The projection list of a query.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct FindClause {
    /// One entry per projected column.
    pub expressions: Vec<FindExpression>,
}

/// One projected column.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum FindExpression {
    /// A variable or dot path.
    Variable(DotPathVar),
    /// An aggregate over a variable or dot path.
    Aggregation {
        /// The aggregate function.
        func: AggregationFunction,
        /// The aggregated variable.
        var: DotPathVar,
        /// Whether `DISTINCT` was written.
        distinct: bool,
    },
}

/// The aggregate functions KQL registers (Spec §44.6).
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum AggregationFunction {
    /// `COUNT`
    Count,
    /// `SUM`
    Sum,
    /// `AVG`
    Avg,
    /// `MIN`
    Min,
    /// `MAX`
    Max,
}

/// One `ORDER BY` key.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct OrderByItem {
    /// The sorted variable or dot path.
    pub variable: DotPathVar,
    /// The sort direction; `ASC` when unwritten.
    pub direction: OrderDirection,
    /// The aggregate applied before sorting, when the key is an aggregate.
    pub aggregation: Option<AggregationFunction>,
}

/// Sort direction.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum OrderDirection {
    /// Ascending, the default.
    #[default]
    Asc,
    /// Descending.
    Desc,
}

/// One pattern inside a `WHERE` block.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum WhereClause {
    /// `?c CONCEPT {...}` — a Concept by its fields.
    Concept {
        /// The bound variable.
        variable: String,
        /// The field matcher.
        matcher: ObjectMatcher,
    },
    /// `?p PROPOSITION (s, p, o)` — a raw Proposition, truth-neutral.
    Proposition {
        /// The bound variable, when one was written.
        variable: Option<String>,
        /// The Proposition expression.
        matcher: PropositionMatcher,
    },
    /// `?a ASSERTION {...}` — one actor's epistemic commitment.
    Assertion {
        /// The bound variable.
        variable: String,
        /// The field matcher.
        matcher: ObjectMatcher,
    },
    /// `?e EVIDENCE {...}` — an observation record.
    Evidence {
        /// The bound variable.
        variable: String,
        /// The field matcher.
        matcher: ObjectMatcher,
    },
    /// `?act ACTIVITY {...}` — a provenance record.
    Activity {
        /// The bound variable.
        variable: String,
        /// The field matcher.
        matcher: ObjectMatcher,
    },
    /// `?edge STRUCTURAL (?src, "has_step", ?dst)` — record topology.
    ///
    /// Never a semantic Proposition: a claim *about* a structural relation is a
    /// separate Proposition + Assertion (Spec §17.3).
    Structural {
        /// The bound variable, when one was written.
        variable: Option<String>,
        /// The referencing element.
        subject: Term,
        /// The structural field.
        field: SymbolRef,
        /// The referenced element.
        object: Term,
    },
    /// `?b BELIEF (...)` — an Epistemic Projection, virtual and read-only.
    Belief {
        /// The bound variable.
        variable: String,
        /// What is projected.
        target: BeliefTarget,
    },
    /// `?slot BELIEF SLOT (?s, "pred")` — candidates and conflicts for one slot.
    BeliefSlot {
        /// The bound variable.
        variable: String,
        /// The slot subject.
        subject: Term,
        /// The slot predicate.
        predicate: PredAtom,
    },
    /// `FILTER (...)`
    Filter {
        /// The filter expression.
        expression: FilterExpression,
    },
    /// `NOT { ... }`
    Not(Vec<WhereClause>),
    /// `OPTIONAL { ... }`
    Optional(Vec<WhereClause>),
    /// `UNION { ... }`
    Union(Vec<WhereClause>),
}

/// What a `BELIEF` projects.
///
/// `BELIEF (...)` is the Proposition expression slot, so the id form that names
/// a Proposition in a pattern names it here too (Spec §43.2 / §46.1). The inline
/// tuple always carries an exact predicate: projection never walks a raw path
/// (Spec §45).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum BeliefTarget {
    /// An already-bound Proposition variable.
    Proposition(String),
    /// A Proposition named by id.
    Id(Scalar),
    /// A tuple stated inline.
    Tuple(PropositionTriple),
}

/// A `FILTER` expression.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum FilterExpression {
    /// A binary comparison.
    Comparison {
        /// Left operand.
        left: FilterOperand,
        /// The operator.
        operator: ComparisonOperator,
        /// Right operand.
        right: FilterOperand,
    },
    /// A logical combination.
    Logical {
        /// Left branch.
        left: Box<FilterExpression>,
        /// The operator.
        operator: LogicalOperator,
        /// Right branch.
        right: Box<FilterExpression>,
    },
    /// `!expr`
    Not(Box<FilterExpression>),
    /// A call to a registered filter function.
    Function {
        /// The function.
        func: FilterFunction,
        /// The arguments.
        args: Vec<FilterOperand>,
    },
}

/// An operand of a filter comparison or function call.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum FilterOperand {
    /// A variable or dot path.
    Variable(DotPathVar),
    /// A literal value.
    Literal(KipValue),
    /// A `:parameter`.
    Param(String),
    /// A bracketed list, e.g. the second argument of `IN`.
    List(Vec<FilterOperand>),
    /// A negated operand.
    Negate(Box<FilterOperand>),
}

/// Comparison operators.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum ComparisonOperator {
    /// `==`
    Equal,
    /// `!=`
    NotEqual,
    /// `<`
    LessThan,
    /// `>`
    GreaterThan,
    /// `<=`
    LessEqual,
    /// `>=`
    GreaterEqual,
}

/// Logical operators.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum LogicalOperator {
    /// `&&`
    And,
    /// `||`
    Or,
}

/// The filter functions KIP 2.0 registers by name.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum FilterFunction {
    /// `CONTAINS(?x, "sub")`
    Contains,
    /// `STARTS_WITH(?x, "pre")`
    StartsWith,
    /// `ENDS_WITH(?x, "suf")`
    EndsWith,
    /// `REGEX(?x, "pattern")`
    Regex,
    /// `IN(?expr, [a, b])` — membership. A function, not a comparison operator.
    In,
    /// `IS_NULL(?x)`
    IsNull,
    /// `IS_NOT_NULL(?x)`
    IsNotNull,
    /// `IS_LITERAL(?x)`
    IsLiteral,
    /// `IS_ELEMENT(?x)`
    IsElement,
    /// `IS_KIND(?x, "Concept")`
    IsKind,
    /// `LITERAL_TYPE(?x)`
    LiteralType,
}

// ---------------------------------------------------------------------------
// KML
// ---------------------------------------------------------------------------

/// One atomic cognitive transition.
///
/// A KML mutation becomes durable only via a Transaction, so a statement written
/// on its own is still a one-clause transaction. `explicit_transaction` records
/// which spelling the source used without changing that meaning.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct KmlStatement {
    /// Whether the source wrote `MUTATE { ... }`.
    pub explicit_transaction: bool,
    /// The mutations, in source order.
    pub clauses: Vec<MutationClause>,
}

/// One mutation inside a transaction.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum MutationClause {
    /// `CREATE CONCEPT ?h { ... }`
    CreateConcept(ConceptCreate),
    /// `UPSERT CONCEPT ?h { ... }`
    UpsertConcept(ConceptUpsert),
    /// `ENSURE PROPOSITION [?h] (s, p, o)`
    EnsureProposition(EnsureProposition),
    /// `CREATE EVIDENCE ?h { ... }`
    CreateEvidence(RecordCreate),
    /// `CREATE ASSERTION ?h { ... }`
    CreateAssertion(RecordCreate),
    /// `CREATE ACTIVITY ?h { ... }`
    CreateActivity(RecordCreate),
    /// `UPDATE target ...`
    Update(UpdateStatement),
    /// `RETRACT ASSERTION target`
    RetractAssertion(RetractAssertion),
    /// `SUPERSEDE ASSERTION old BY new`
    SupersedeAssertion(SupersedeAssertion),
    /// `CORRECT EVIDENCE old BY new`
    CorrectEvidence(CorrectEvidence),
    /// `TRANSITION ACTIVITY target TO state`
    TransitionActivity(TransitionActivity),
    /// `SET RETENTION target { ... }`
    SetRetention(SetRetention),
    /// `ARCHIVE target`
    Archive(RemovalStatement),
    /// `TOMBSTONE target`
    Tombstone(RemovalStatement),
    /// `PURGE target ... CONFIRM "PURGE"`
    Purge(PurgeStatement),
    /// `MERGE CONCEPT source INTO target`
    MergeConcept(MergeConcept),
}

impl MutationClause {
    /// The local handle this clause binds, when it binds one.
    pub fn handle(&self) -> Option<&str> {
        match self {
            MutationClause::CreateConcept(c) => Some(c.handle.as_str()),
            MutationClause::UpsertConcept(c) => Some(c.handle.as_str()),
            MutationClause::CreateEvidence(c)
            | MutationClause::CreateAssertion(c)
            | MutationClause::CreateActivity(c) => Some(c.handle.as_str()),
            MutationClause::EnsureProposition(c) => c.handle.as_deref(),
            _ => None,
        }
    }
}

/// `CREATE CONCEPT` — a new Concept with engine-minted identity.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ConceptCreate {
    /// The block-local handle this clause binds.
    pub handle: String,
    /// `TYPE "..."`
    pub r#type: Option<SymbolRef>,
    /// `CLIENT KEY ...` — retry-safe logical identity.
    pub client_key: Option<Scalar>,
    /// `NAME ...` — mutable grounding state, never identity.
    pub name: Option<Scalar>,
    /// `SET FIELDS { ... }`
    pub set_fields: Option<Assignments>,
    /// `SET ATTRIBUTES { ... }`
    pub set_attributes: Option<Assignments>,
    /// `SET FACET "..." { ... }`, one entry per facet.
    pub set_facets: Vec<FacetAssignment>,
    /// `SET STRUCTURAL { ... }`
    pub set_structural: Option<Vec<StructuralEdge>>,
}

/// `UPSERT CONCEPT` — resolve-or-create against a stable identity.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ConceptUpsert {
    /// The block-local handle this clause binds.
    pub handle: String,
    /// `MATCH { ... }` — must carry `id` or `key`.
    pub r#match: Option<ObjectMatcher>,
    /// `EXPECT VERSION ...`
    pub expect_version: Option<Scalar>,
    /// `SET FIELDS { ... }`
    pub set_fields: Option<Assignments>,
    /// `SET ATTRIBUTES { ... }`
    pub set_attributes: Option<Assignments>,
    /// `SET FACET "..." { ... }`
    pub set_facets: Vec<FacetAssignment>,
    /// `UNSET ATTRIBUTES { ... }`
    pub unset_attributes: Option<Vec<String>>,
    /// `UNSET FACET "..." { ... }`
    pub unset_facets: Vec<FacetUnset>,
    /// `SET STRUCTURAL { ... }`
    pub set_structural: Option<Vec<StructuralEdge>>,
    /// `UNSET STRUCTURAL { ... }`
    pub unset_structural: Option<Vec<StructuralRemoval>>,
}

/// `CREATE EVIDENCE` / `ASSERTION` / `ACTIVITY` share one shape.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct RecordCreate {
    /// The block-local handle this clause binds.
    pub handle: String,
    /// `CLIENT KEY ...`
    pub client_key: Option<Scalar>,
    /// `SET FIELDS { ... }`
    pub set_fields: Option<Assignments>,
    /// `SET FACET "..." { ... }`
    pub set_facets: Vec<FacetAssignment>,
    /// `SET STRUCTURAL { ... }`
    pub set_structural: Option<Vec<StructuralEdge>>,
}

/// `ENSURE PROPOSITION` — resolve-or-create a truth-neutral tuple.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct EnsureProposition {
    /// The block-local handle, when one was written.
    pub handle: Option<String>,
    /// The subject endpoint.
    pub subject: Term,
    /// The exact predicate.
    pub predicate: PredAtom,
    /// The object endpoint.
    pub object: Term,
    /// `EXPECT VERSION ...`
    pub expect_version: Option<Scalar>,
}

/// One `SET FACET` clause.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct FacetAssignment {
    /// The facet symbol.
    pub facet: SymbolRef,
    /// The assigned members.
    pub values: Assignments,
}

/// One `UNSET FACET` clause.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct FacetUnset {
    /// The facet symbol.
    pub facet: SymbolRef,
    /// The removed member names.
    pub fields: Vec<String>,
}

/// One structural edge, optionally placed.
///
/// The options object carries edge options; `index` is meaningful only on a
/// field declared ordered, and index order is never causality (Spec §17.4).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct StructuralEdge {
    /// The structural field.
    pub field: SymbolRef,
    /// The referenced element.
    pub value: MutationValue,
    /// Edge options, when written.
    pub options: Option<BoundObject>,
}

/// `UNSET STRUCTURAL { (field, target) }` — one reference to remove.
///
/// The `SET STRUCTURAL` edge without options: removal is per reference, ordered
/// fields re-densify, cardinality is validated at commit (Spec §17.5).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct StructuralRemoval {
    /// The structural field.
    pub field: SymbolRef,
    /// The referenced element to remove.
    pub value: MutationValue,
}

/// A KML right-hand side: a bound value, or arithmetic over the target's *own*
/// fields.
///
/// References to any other variable are rejected during lowering, which is what
/// lets each matched element be updated from its own row without a join.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum MutationValue {
    /// A fully literal subtree.
    Value(KipValue),
    /// A `:parameter`.
    Param(String),
    /// A `?handle` naming an element created by this mutation plan.
    Handle(String),
    /// A read of the target element's own field.
    Variable(DotPathVar),
    /// An array with at least one unbound element.
    Array(Vec<BoundValue>),
    /// An object with at least one unbound member.
    Object(Vec<(String, BoundValue)>),
    /// A deterministic update expression.
    Expr(UpdateExpr),
}

impl From<BoundValue> for MutationValue {
    fn from(value: BoundValue) -> Self {
        match value {
            BoundValue::Value(v) => MutationValue::Value(v),
            BoundValue::Param(p) => MutationValue::Param(p),
            BoundValue::Handle(h) => MutationValue::Handle(h),
            BoundValue::Variable(v) => MutationValue::Variable(v),
            BoundValue::Array(items) => MutationValue::Array(items),
            BoundValue::Object(fields) => MutationValue::Object(fields),
        }
    }
}

/// A deterministic arithmetic expression over the target's own fields.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum UpdateExpr {
    /// A read of the target element's own field.
    Variable(DotPathVar),
    /// A numeric literal.
    Number(Number),
    /// A `:parameter`.
    Param(String),
    /// A call to a registered update function.
    Function {
        /// The function.
        func: UpdateFunction,
        /// The arguments.
        args: Vec<UpdateExpr>,
    },
}

/// The deterministic update functions KIP 2.0 registers (Spec §59).
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum UpdateFunction {
    /// `ADD(a, b)`
    Add,
    /// `MUL(a, b)`
    Mul,
    /// `CLAMP(x, lo, hi)`
    Clamp,
    /// `COALESCE(a, b)`
    Coalesce,
}

impl UpdateFunction {
    /// The exact number of arguments this function takes.
    pub fn arity(&self) -> usize {
        match self {
            UpdateFunction::Add | UpdateFunction::Mul | UpdateFunction::Coalesce => 2,
            UpdateFunction::Clamp => 3,
        }
    }
}

/// `UPDATE` reaches mutable state only.
///
/// Proposition tuples, Assertion epistemic payload, Evidence payload, terminal
/// Activity topology, `_system` and Governance are all out of reach; the parser
/// rejects those targets rather than letting an engine discover them.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct UpdateStatement {
    /// The element to update.
    pub target: ElementRef,
    /// `EXPECT VERSION ...`
    pub expect_version: Option<Scalar>,
    /// The actions, in source order.
    pub actions: Vec<UpdateAction>,
    /// `None` when the statement names its target directly and omits WHERE —
    /// the same shape as the removal family (Spec §58).
    pub where_clauses: Option<Vec<WhereClause>>,
    /// The bound on how many matched elements may be updated.
    pub limit: Option<Scalar>,
}

/// One action of an `UPDATE`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum UpdateAction {
    /// `SET FIELDS { ... }`
    SetFields(Assignments),
    /// `SET ATTRIBUTES { ... }`
    SetAttributes(Assignments),
    /// `SET FACET "..." { ... }`
    SetFacet(FacetAssignment),
    /// `UNSET ATTRIBUTES { ... }`
    UnsetAttributes(Vec<String>),
    /// `UNSET FACET "..." { ... }`
    UnsetFacet(FacetUnset),
    /// `SET STRUCTURAL { ... }`
    SetStructural(Vec<StructuralEdge>),
    /// `UNSET STRUCTURAL { ... }`
    UnsetStructural(Vec<StructuralRemoval>),
}

/// `RETRACT ASSERTION` — the assertor withdraws their commitment (Spec §57.3).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct RetractAssertion {
    /// The Assertion to retract.
    pub target: ElementRef,
    /// The selection block, when the target is bound by one.
    pub where_clauses: Option<Vec<WhereClause>>,
    /// The bound on how many matched Assertions may be retracted.
    pub limit: Option<Scalar>,
    /// `EXPECT STATE ...`
    pub expect_state: Option<Scalar>,
}

/// `SUPERSEDE ASSERTION old BY new` — belief revision (Spec §57.4).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct SupersedeAssertion {
    /// The superseded Assertion.
    pub target: ElementRef,
    /// The superseding Assertion.
    pub by: ElementRef,
    /// `EXPECT STATE ...`
    pub expect_state: Option<Scalar>,
}

/// `CORRECT EVIDENCE old BY new` — Evidence correction (Spec §57.2).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct CorrectEvidence {
    /// The corrected Evidence.
    pub target: ElementRef,
    /// The correcting Evidence.
    pub by: ElementRef,
    /// `EXPECT STATE ...`
    pub expect_state: Option<Scalar>,
}

/// `TRANSITION ACTIVITY` — advance an Activity's lifecycle, finalizing atomically.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct TransitionActivity {
    /// The Activity to transition.
    pub target: ElementRef,
    /// The target state.
    pub to: Scalar,
    /// Terminal outputs finalized in the same transition.
    pub set_fields: Option<Assignments>,
    /// Terminal topology finalized in the same transition.
    pub set_structural: Option<Vec<StructuralEdge>>,
    /// `EXPECT STATE ...`
    pub expect_state: Option<Scalar>,
}

/// `SET RETENTION` — storage lifecycle, never valid time (Spec §19).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct SetRetention {
    /// The element whose retention changes.
    pub target: ElementRef,
    /// The retention members.
    pub values: Assignments,
    /// The selection block, when the target is bound by one.
    pub where_clauses: Option<Vec<WhereClause>>,
    /// The bound on how many matched elements may be changed.
    pub limit: Option<Scalar>,
    /// `EXPECT VERSION ...`
    pub expect_version: Option<Scalar>,
}

/// `ARCHIVE` / `TOMBSTONE` share one shape.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct RemovalStatement {
    /// The element to remove.
    pub target: ElementRef,
    /// The selection block, when the target is bound by one.
    pub where_clauses: Option<Vec<WhereClause>>,
    /// The bound on how many matched elements may be removed.
    pub limit: Option<Scalar>,
    /// `EXPECT STATE ...`
    pub expect_state: Option<Scalar>,
}

/// `PURGE` — physical erasure. The grammar freezes the confirmation spelling.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct PurgeStatement {
    /// The element to erase.
    pub target: ElementRef,
    /// The selection block, when the target is bound by one.
    pub where_clauses: Option<Vec<WhereClause>>,
    /// The bound on how many matched elements may be erased.
    pub limit: Option<Scalar>,
    /// `REFERENCE POLICY ...`
    pub reference_policy: Option<Scalar>,
    /// Always the literal `PURGE`; the grammar freezes the spelling.
    pub confirm: String,
}

/// `MERGE CONCEPT` — non-destructive: the source stays addressable as merged
/// history (Spec §11.1).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct MergeConcept {
    /// The merged-away Concept.
    pub source: ElementRef,
    /// The surviving canonical Concept.
    pub into: ElementRef,
    /// A guard block; MERGE never selects its operands by pattern.
    pub where_clauses: Option<Vec<WhereClause>>,
    /// `EXPECT VERSION ...`
    pub expect_version: Option<Scalar>,
}

// ---------------------------------------------------------------------------
// META
// ---------------------------------------------------------------------------

/// A META command. META is semantically read-only (Spec §63.2).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum MetaCommand {
    /// `DESCRIBE ...`
    Describe(DescribeTarget),
    /// `LIST ...`
    List(ListCommand),
    /// `SEARCH ...`
    Search(SearchCommand),
    /// `VERIFY ...`
    Verify {
        /// What kind of artifact is verified.
        target: VerifyTarget,
        /// The artifact operand.
        value: Scalar,
    },
    /// `VALIDATE ...`
    Validate(ValidateCommand),
    /// `PREVIEW ...`
    Preview(PreviewCommand),
    /// `HISTORY ...`
    History(HistoryCommand),
    /// `CHANGES ...`
    Changes(ChangesCommand),
    /// `SNAPSHOT [AS OF ...]`
    Snapshot {
        /// The history coordinate, when written.
        as_of: Option<AsOf>,
    },
    /// `EXPORT CAPSULE ...`
    ExportCapsule(ExportCapsuleCommand),
}

/// What a `DESCRIBE` introspects.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum DescribeTarget {
    /// `DESCRIBE PRIMER [MODE ...]`
    Primer {
        /// `"compact"` or `"full"`.
        mode: Option<Scalar>,
    },
    /// `DESCRIBE PROTOCOL`
    Protocol,
    /// `DESCRIBE EXECUTION CONTEXT`
    ExecutionContext,
    /// `DESCRIBE CAPABILITIES`
    Capabilities,
    /// `DESCRIBE SPACE [...]`
    Space {
        /// The Space id; the current Space when absent.
        value: Option<Scalar>,
    },
    /// `DESCRIBE SCHEMA ENVIRONMENT [AS OF ...]`
    SchemaEnvironment {
        /// The history coordinate, when written.
        as_of: Option<AsOf>,
    },
    /// `DESCRIBE PACKAGE ...`
    Package(Scalar),
    /// `DESCRIBE TYPE ...`
    Type(Scalar),
    /// `DESCRIBE PREDICATE ...`
    Predicate(Scalar),
    /// `DESCRIBE FACET ...`
    Facet(Scalar),
    /// `DESCRIBE STRUCTURAL FIELD ...`
    StructuralField(Scalar),
    /// `DESCRIBE COMPATIBILITY FROM ... TO ...`
    Compatibility {
        /// The source version.
        from: Scalar,
        /// The target version.
        to: Scalar,
    },
    /// `DESCRIBE ERROR ...`
    Error(Scalar),
    /// `DESCRIBE TRANSACTION ...`
    Transaction(Scalar),
    /// `DESCRIBE TRANSACTION BY IDEMPOTENCY KEY ...`
    TransactionByIdempotencyKey(Scalar),
    /// `DESCRIBE SNAPSHOT [AS OF ...]`
    Snapshot {
        /// The history coordinate, when written.
        as_of: Option<AsOf>,
    },
    /// `DESCRIBE CAPSULE ...`
    Capsule(Scalar),
    /// `DESCRIBE EPISTEMIC POLICY [...]`
    EpistemicPolicy {
        /// The policy name; the active policy when absent.
        value: Option<Scalar>,
    },
    /// `DESCRIBE PROJECTION CAPABILITY`
    ProjectionCapability,
    /// `DESCRIBE TRUST [...]`
    Trust {
        /// The trust subject; the whole trust state when absent.
        value: Option<Scalar>,
    },
    /// `DESCRIBE ACCESS [WITH {...}]`
    Access {
        /// The operation/resource/purpose input block.
        with: Option<BoundObject>,
    },
}

/// `LIST ...`
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ListCommand {
    /// What is listed.
    pub target: ListTarget,
    /// `LIST SCHEMA PACKAGES STATUS ...` only.
    pub status: Option<Scalar>,
    /// The page size.
    pub limit: Option<Scalar>,
    /// The page cursor.
    pub cursor: Option<Scalar>,
}

/// What a `LIST` enumerates.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum ListTarget {
    /// `LIST SPACES`
    Spaces,
    /// `LIST SCHEMA PACKAGES`
    SchemaPackages,
    /// `LIST TYPES`
    Types,
    /// `LIST PREDICATES`
    Predicates,
    /// `LIST FACETS`
    Facets,
    /// `LIST STRUCTURAL FIELDS`
    StructuralFields,
    /// `LIST EPISTEMIC POLICIES`
    EpistemicPolicies,
}

/// `SEARCH ...`
///
/// Grounding only: a SEARCH score is not confidence, and a miss is not absence.
/// The golden path is SEARCH → exact id → BELIEF/FIND.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct SearchCommand {
    /// What kind of element is searched.
    pub target: SearchTarget,
    /// The search term.
    pub term: Scalar,
    /// `WITH TYPE ...`
    pub with_type: Option<Scalar>,
    /// `WITH PREDICATE ...`
    pub with_predicate: Option<Scalar>,
    /// `MODE "keyword" | "semantic" | "hybrid"`
    pub mode: Option<Scalar>,
    /// `THRESHOLD ...`
    pub threshold: Option<Scalar>,
    /// Historical index basis, `AS OF SEQ`.
    pub as_of_seq: Option<Scalar>,
    /// The page size.
    pub limit: Option<Scalar>,
    /// The page cursor.
    pub cursor: Option<Scalar>,
}

/// What a `SEARCH` looks through.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum SearchTarget {
    /// `SEARCH CONCEPT`
    Concept,
    /// `SEARCH PROPOSITION`
    Proposition,
    /// `SEARCH ASSERTION`
    Assertion,
    /// `SEARCH EVIDENCE`
    Evidence,
    /// `SEARCH ACTIVITY`
    Activity,
    /// `SEARCH COGNITION`
    Cognition,
}

/// What a `VERIFY` checks.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum VerifyTarget {
    /// `VERIFY CAPSULE`
    Capsule,
    /// `VERIFY SCHEMA PACKAGE`
    SchemaPackage,
    /// `VERIFY RECEIPT`
    Receipt,
    /// `VERIFY BLOB`
    Blob,
    /// `VERIFY CHECKPOINT`
    Checkpoint,
}

/// `VALIDATE ...`
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ValidateCommand {
    /// What kind of input is validated.
    pub target: ValidateTarget,
    /// The input operand.
    pub value: Scalar,
    /// `WITH { ... }` validation options.
    pub options: Option<BoundObject>,
}

/// What a `VALIDATE` checks.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum ValidateTarget {
    /// `VALIDATE KQL`
    Kql,
    /// `VALIDATE KML`
    Kml,
    /// `VALIDATE CAPSULE`
    Capsule,
    /// `VALIDATE SCHEMA PACKAGE`
    SchemaPackage,
    /// `VALIDATE IMPORT PLAN`
    ImportPlan,
}

/// `PREVIEW ...` — computes an effect plan without committing (Spec §69.3).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum PreviewCommand {
    /// `PREVIEW KML ...`
    Kml(Scalar),
    /// `PREVIEW IMPORT CAPSULE ... INTO ...`
    ImportCapsule {
        /// The capsule artifact.
        capsule: Scalar,
        /// The destination Space.
        into: Scalar,
    },
}

/// `HISTORY ...`
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum HistoryCommand {
    /// `HISTORY ELEMENT ...`
    Element {
        /// The element id.
        value: Scalar,
        /// `FROM SEQ ...`
        from_seq: Option<Scalar>,
        /// `TO SEQ ...`
        to_seq: Option<Scalar>,
        /// The page size.
        limit: Option<Scalar>,
        /// The page cursor.
        cursor: Option<Scalar>,
    },
    /// `HISTORY SPACE ...`
    Space {
        /// `FROM SEQ ...`
        from_seq: Option<Scalar>,
        /// `TO SEQ ...`
        to_seq: Option<Scalar>,
        /// The page size.
        limit: Option<Scalar>,
        /// The page cursor.
        cursor: Option<Scalar>,
    },
}

/// `CHANGES ...`
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum ChangesCommand {
    /// `CHANGES SINCE ...`
    Since {
        /// The change cursor.
        cursor: Scalar,
        /// The page size.
        limit: Option<Scalar>,
    },
    /// `CHANGES AFTER SEQ ...`
    AfterSeq {
        /// The Space sequence.
        seq: Scalar,
        /// The page size.
        limit: Option<Scalar>,
    },
}

/// `EXPORT CAPSULE ...`
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ExportCapsuleCommand {
    /// The capsule target reference.
    pub target: ElementRef,
    /// The selection block.
    pub where_clauses: Vec<WhereClause>,
    /// `WITH { ... }` export options.
    pub options: Option<BoundObject>,
    /// The history coordinate, when written.
    pub as_of: Option<AsOf>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kip_value_encodes_externally_tagged() {
        assert_eq!(serde_json::to_string(&KipValue::Null).unwrap(), r#""Null""#);
        assert_eq!(
            serde_json::to_string(&KipValue::Bool(true)).unwrap(),
            r#"{"Bool":true}"#
        );
        assert_eq!(
            serde_json::to_string(&KipValue::String("a".into())).unwrap(),
            r#"{"String":"a"}"#
        );
        assert_eq!(
            serde_json::to_string(&KipValue::Number(Number::from(3))).unwrap(),
            r#"{"Number":3}"#
        );
    }

    #[test]
    fn scalar_and_refs_match_the_reference_encoding() {
        assert_eq!(
            serde_json::to_string(&Scalar::Param("limit".into())).unwrap(),
            r#"{"Param":"limit"}"#
        );
        assert_eq!(
            serde_json::to_string(&Scalar::Literal(KipValue::Number(Number::from(10)))).unwrap(),
            r#"{"Literal":{"Number":10}}"#
        );
        assert_eq!(
            serde_json::to_string(&SymbolRef::Name("has_step".into())).unwrap(),
            r#"{"Name":"has_step"}"#
        );
        assert_eq!(
            serde_json::to_string(&ElementRef::Id("E-1".into())).unwrap(),
            r#"{"Id":"E-1"}"#
        );
    }

    #[test]
    fn unit_enums_encode_as_bare_strings() {
        assert_eq!(
            serde_json::to_string(&DescribeTarget::Protocol).unwrap(),
            r#""Protocol""#
        );
        assert_eq!(
            serde_json::to_string(&AggregationFunction::Count).unwrap(),
            r#""Count""#
        );
        assert_eq!(
            serde_json::to_string(&OrderDirection::Desc).unwrap(),
            r#""Desc""#
        );
    }

    #[test]
    fn assignments_encode_as_ordered_pairs() {
        let assignments: Assignments = vec![
            (
                "stance".to_string(),
                MutationValue::Value(KipValue::String("support".into())),
            ),
            ("evidence".to_string(), MutationValue::Handle("e1".into())),
        ];
        assert_eq!(
            serde_json::to_string(&assignments).unwrap(),
            r#"[["stance",{"Value":{"String":"support"}}],["evidence",{"Handle":"e1"}]]"#
        );
    }

    #[test]
    fn command_round_trips_through_json() {
        let command = Command::Kml(KmlStatement {
            explicit_transaction: true,
            clauses: vec![MutationClause::EnsureProposition(EnsureProposition {
                handle: Some("p".into()),
                subject: Term::Param("alice".into()),
                predicate: PredAtom::Literal("prefers".into()),
                object: Term::Param("dark_mode".into()),
                expect_version: None,
            })],
        });
        let encoded = serde_json::to_string(&command).unwrap();
        let decoded: Command = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, command);
        assert!(decoded.is_mutation());
    }

    #[test]
    fn kip_value_rejects_nothing_finite_and_converts_to_json() {
        let value = KipValue::try_from(serde_json::json!({"a": [1, "b", null]})).unwrap();
        assert_eq!(Json::from(value), serde_json::json!({"a": [1, "b", null]}));
    }

    #[test]
    fn dot_path_var_displays_both_step_kinds() {
        let path = DotPathVar {
            var: "x".into(),
            path: vec![
                PathStep::Field("facets".into()),
                PathStep::Key("MnemonicState".into()),
                PathStep::Field("salience".into()),
            ],
        };
        assert_eq!(path.to_string(), r#"?x.facets["MnemonicState"].salience"#);
    }
}
