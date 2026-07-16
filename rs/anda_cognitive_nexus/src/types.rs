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
    Object {
        /// Concept type name.
        r#type: String,
        /// Concept display name within the type.
        name: String,
    },
}

impl fmt::Display for ConceptPK {
    /// Formats the concept primary key for display.
    ///
    /// # Format
    /// - ID variant: `{id: "concept:<id>"}`
    /// - Object variant: `{type: "<type>", name: "<name>"}`
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // `{id: "<id>"}` — valid KIP syntax so error messages remain
            // directly reusable by self-correcting agents.
            ConceptPK::ID(id) => write!(f, "{{id: \"{}\"}}", EntityID::Concept(*id)),
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
        /// Subject entity primary key.
        subject: Box<EntityPK>,
        /// Predicate name connecting subject and object.
        predicate: String,
        /// Object entity primary key.
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
            // `(id: "<link_id>")` — valid KIP syntax so error messages remain
            // directly reusable by self-correcting agents.
            PropositionPK::ID(id, predicate) => write!(
                f,
                "(id: \"{}\")",
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
            TargetTerm::Concept(matcher) => Ok(EntityPK::Concept(ConceptPK::try_from(matcher)?)),
            TargetTerm::Proposition(matcher) => {
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

/// Engine cap on the number of solution rows a single operation
/// materializes: natural joins across disconnected variables, UNION block
/// materialization and the unconstrained `(?s, ?p, ?o)` full scan. Beyond
/// it the command fails with `KIP_4002` — connect the variables through
/// graph patterns or narrow them first (the spec explicitly allows the
/// engine to reject such queries, KIP §3.4.2).
pub const MAX_SOLUTION_COMBINATIONS: usize = 65_536;

/// One variable's binding inside a solution row.
///
/// This is the single value representation shared by the whole KQL solver:
/// solution tables, FILTER evaluation and FIND projection all speak
/// `BindingValue`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum BindingValue {
    /// The variable binds a concept node or proposition link.
    Entity(EntityID),
    /// The variable binds a predicate name (KIP §3.4.2 — a string).
    Predicate(String),
    /// Absent binding: `OPTIONAL` misses and `UNION` branches that do not
    /// bind the variable project `null` (KIP §3.4.7.2 / §3.4.7.3).
    Null,
}

impl BindingValue {
    /// Returns `true` for the [`BindingValue::Null`] (absent) binding.
    pub fn is_null(&self) -> bool {
        matches!(self, BindingValue::Null)
    }
}

/// A set of candidate solutions over a fixed set of variables, stored in a
/// column-layout row table (`vars` is the header, every row has exactly
/// `vars.len()` cells).
///
/// The KQL solver keeps the `WHERE` state as a *forest* of these tables
/// (see [`QueryContext::tables`]): each variable lives in exactly one
/// table, and tables are pairwise disjoint. Disconnected variable groups
/// stay in separate tables so their cross product is never materialized
/// until a clause or projection actually needs it.
#[derive(Clone, Debug, Default)]
pub struct SolutionTable {
    /// Column header: the variables this table binds.
    pub vars: Vec<String>,
    /// Solution rows; every row has exactly `vars.len()` cells.
    pub rows: Vec<Vec<BindingValue>>,
}

impl SolutionTable {
    /// A table over one variable with one row per value.
    pub fn single_column(var: String, values: Vec<BindingValue>) -> Self {
        SolutionTable {
            vars: vec![var],
            rows: values.into_iter().map(|value| vec![value]).collect(),
        }
    }

    /// Column index of `var`, if this table binds it.
    pub fn column(&self, var: &str) -> Option<usize> {
        self.vars.iter().position(|v| v == var)
    }

    /// Whether this table binds `var`.
    pub fn covers(&self, var: &str) -> bool {
        self.vars.iter().any(|v| v == var)
    }

    /// Distinct non-null values of `var`'s column, in first-occurrence
    /// (row) order. Empty when the variable is not covered.
    pub fn distinct_values(&self, var: &str) -> Vec<BindingValue> {
        let Some(col) = self.column(var) else {
            return Vec::new();
        };
        let mut seen: FxHashSet<&BindingValue> = FxHashSet::default();
        let mut out = Vec::new();
        for row in &self.rows {
            let value = &row[col];
            if !value.is_null() && seen.insert(value) {
                out.push(value.clone());
            }
        }
        out
    }

    /// Natural join (KIP §3.4: `WHERE` clauses are conjunctive).
    ///
    /// Shared variables must agree; a [`BindingValue::Null`] cell is
    /// compatible with anything (SPARQL-style unbound semantics — padded
    /// `OPTIONAL` / `UNION` rows never block a later pattern) and the
    /// merged cell takes the non-null side. With no shared variables this
    /// is the cross product. The output is capped at
    /// [`MAX_SOLUTION_COMBINATIONS`] (`KIP_4002`).
    pub fn natural_join(&self, right: &SolutionTable) -> Result<SolutionTable, KipError> {
        // (left column, right column) pairs of the shared variables.
        let shared: Vec<(usize, usize)> = self
            .vars
            .iter()
            .enumerate()
            .filter_map(|(li, var)| right.column(var).map(|ri| (li, ri)))
            .collect();
        let right_extra: Vec<usize> = (0..right.vars.len())
            .filter(|ri| !shared.iter().any(|(_, r)| r == ri))
            .collect();

        let mut vars = self.vars.clone();
        vars.extend(right_extra.iter().map(|&ri| right.vars[ri].clone()));

        // Hash the right rows on their shared-variable key; rows with a
        // null in the key are wildcards and must be probed per left row.
        let mut keyed: FxHashMap<Vec<&BindingValue>, Vec<usize>> = FxHashMap::default();
        let mut wild: Vec<usize> = Vec::new();
        for (ri, row) in right.rows.iter().enumerate() {
            let key: Vec<&BindingValue> = shared.iter().map(|&(_, r)| &row[r]).collect();
            if key.iter().any(|value| value.is_null()) {
                wild.push(ri);
            } else {
                keyed.entry(key).or_default().push(ri);
            }
        }

        let compatible = |left: &[BindingValue], right_row: &[BindingValue]| {
            shared.iter().all(|&(l, r)| {
                left[l].is_null() || right_row[r].is_null() || left[l] == right_row[r]
            })
        };
        let emit = |left: &[BindingValue], right_row: &[BindingValue]| -> Vec<BindingValue> {
            let mut row = left.to_vec();
            for &(l, r) in &shared {
                if row[l].is_null() {
                    row[l] = right_row[r].clone();
                }
            }
            row.extend(right_extra.iter().map(|&ri| right_row[ri].clone()));
            row
        };

        let mut rows: Vec<Vec<BindingValue>> = Vec::new();
        for left in &self.rows {
            let key: Vec<&BindingValue> = shared.iter().map(|&(l, _)| &left[l]).collect();
            if key.iter().any(|value| value.is_null()) {
                // Wildcard on the left: probe every right row.
                for right_row in &right.rows {
                    if compatible(left, right_row) {
                        rows.push(emit(left, right_row));
                    }
                }
            } else {
                if let Some(matches) = keyed.get(&key) {
                    for &ri in matches {
                        rows.push(emit(left, &right.rows[ri]));
                    }
                }
                for &ri in &wild {
                    if compatible(left, &right.rows[ri]) {
                        rows.push(emit(left, &right.rows[ri]));
                    }
                }
            }
            if rows.len() > MAX_SOLUTION_COMBINATIONS {
                return Err(KipError::resource_exhausted(format!(
                    "joining graph patterns materializes more than {MAX_SOLUTION_COMBINATIONS} \
                     solution rows; narrow the patterns or connect the variables before \
                     combining them"
                )));
            }
        }

        Ok(SolutionTable { vars, rows })
    }

    /// Left join (KIP §3.4.7.2, `OPTIONAL`): every left row is kept; the
    /// right table's extra variables extend matching rows and pad `null`
    /// on misses. Shared-variable compatibility follows
    /// [`SolutionTable::natural_join`].
    pub fn left_join(&self, right: &SolutionTable) -> Result<SolutionTable, KipError> {
        let shared: Vec<(usize, usize)> = self
            .vars
            .iter()
            .enumerate()
            .filter_map(|(li, var)| right.column(var).map(|ri| (li, ri)))
            .collect();
        let right_extra: Vec<usize> = (0..right.vars.len())
            .filter(|ri| !shared.iter().any(|(_, r)| r == ri))
            .collect();

        let mut vars = self.vars.clone();
        vars.extend(right_extra.iter().map(|&ri| right.vars[ri].clone()));

        let compatible = |left: &[BindingValue], right_row: &[BindingValue]| {
            shared.iter().all(|&(l, r)| {
                left[l].is_null() || right_row[r].is_null() || left[l] == right_row[r]
            })
        };
        let mut rows: Vec<Vec<BindingValue>> = Vec::new();
        for left in &self.rows {
            let mut matched = false;
            for right_row in &right.rows {
                if compatible(left, right_row) {
                    matched = true;
                    let mut row = left.to_vec();
                    for &(l, r) in &shared {
                        if row[l].is_null() {
                            row[l] = right_row[r].clone();
                        }
                    }
                    row.extend(right_extra.iter().map(|&ri| right_row[ri].clone()));
                    rows.push(row);
                }
            }
            if !matched {
                let mut row = left.to_vec();
                row.extend(right_extra.iter().map(|_| BindingValue::Null));
                rows.push(row);
            }
            if rows.len() > MAX_SOLUTION_COMBINATIONS {
                return Err(KipError::resource_exhausted(format!(
                    "OPTIONAL join materializes more than {MAX_SOLUTION_COMBINATIONS} solution \
                     rows; narrow the patterns before combining them"
                )));
            }
        }

        Ok(SolutionTable { vars, rows })
    }

    /// Row-wise union with null padding (KIP §3.4.7.3): the result covers
    /// the union of both variable sets, `self`'s rows come first, each side
    /// padded with `null` for the other's missing variables, and identical
    /// full rows are deduplicated (§3.3 solution set semantics).
    pub fn union_padded(&self, right: &SolutionTable) -> Result<SolutionTable, KipError> {
        let mut vars = self.vars.clone();
        for var in &right.vars {
            if !self.covers(var) {
                vars.push(var.clone());
            }
        }
        if self.rows.len().saturating_add(right.rows.len()) > MAX_SOLUTION_COMBINATIONS {
            return Err(KipError::resource_exhausted(format!(
                "UNION materializes more than {MAX_SOLUTION_COMBINATIONS} solution rows; \
                 narrow the branches before merging them"
            )));
        }

        let mut rows: Vec<Vec<BindingValue>> = Vec::with_capacity(self.rows.len());
        let mut seen: FxHashSet<Vec<BindingValue>> = FxHashSet::default();
        for row in &self.rows {
            let mut padded = row.clone();
            padded.extend(vars[self.vars.len()..].iter().map(|_| BindingValue::Null));
            if seen.insert(padded.clone()) {
                rows.push(padded);
            }
        }
        for row in &right.rows {
            let padded: Vec<BindingValue> = vars
                .iter()
                .map(|var| match right.column(var) {
                    Some(ri) => row[ri].clone(),
                    None => BindingValue::Null,
                })
                .collect();
            if seen.insert(padded.clone()) {
                rows.push(padded);
            }
        }

        Ok(SolutionTable { vars, rows })
    }
}

/// Query execution context: the row-oriented solution state of a `WHERE`
/// evaluation plus the shared entity cache.
///
/// # Solution model
///
/// The candidate solution set is a forest of [`SolutionTable`]s with
/// pairwise-disjoint variable sets. Every `WHERE` clause is a relational
/// operator over this forest: graph patterns natural-join their match rows
/// in, `FILTER` keeps satisfying rows, `NOT` anti-joins, `OPTIONAL`
/// left-joins with null padding and `UNION` merges row-wise with null
/// padding. `FIND` projects columns from the (joined) tables, so
/// multi-variable results stay index-aligned across solutions by
/// construction (KIP §6.2.2).
#[derive(Clone, Debug, Default)]
pub struct QueryContext {
    /// The solution forest. Invariant: every variable is bound by at most
    /// one table.
    pub tables: Vec<SolutionTable>,

    /// `(group_var, member_var)` pairs eligible for grouped aggregation:
    /// recorded for the subject/object endpoints of each matched
    /// proposition pattern (both directions). `FIND(?g.name, COUNT(?m))`
    /// aggregates per group only when such a pair exists — mirroring the
    /// spec's implicit-grouping examples while keeping plain
    /// `FIND(?x, COUNT(?y))` a global aggregate for unrelated variables.
    pub group_pairs: FxHashSet<(String, String)>,

    /// When `true`, "dangling id / entity not found" grounding errors
    /// (`KIP_3002`) degrade to an **empty match** instead of failing the
    /// whole query. Set for `NOT` / `OPTIONAL` / `UNION` sub-block contexts
    /// (KIP §3.4.7): a sub-pattern that cannot match makes the `NOT` clause
    /// succeed, the `OPTIONAL` block pad with `null`, or the `UNION` branch
    /// contribute nothing — it must not abort the query. Only the precise
    /// `KIP_3002` grounding checks are degraded; storage-level failures
    /// still propagate. The main (top-level) pattern keeps strict `KIP_3002`
    /// semantics.
    pub lenient_grounding: bool,

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

impl QueryContext {
    /// Whether `var` is bound by some table (possibly with zero rows).
    pub fn is_bound(&self, var: &str) -> bool {
        self.tables.iter().any(|table| table.covers(var))
    }

    /// The table binding `var`, if any.
    pub fn table_of(&self, var: &str) -> Option<&SolutionTable> {
        self.tables.iter().find(|table| table.covers(var))
    }

    /// Distinct non-null values of `var` across its table, in row order.
    /// `None` when the variable is unbound.
    pub fn distinct_values(&self, var: &str) -> Option<Vec<BindingValue>> {
        self.table_of(var).map(|table| table.distinct_values(var))
    }

    /// Distinct entity bindings of `var` in row order. `None` when the
    /// variable is unbound; `Some(empty)` when it is bound with no
    /// surviving entity value.
    pub fn entity_values(&self, var: &str) -> Option<UniqueVec<EntityID>> {
        let values = self.distinct_values(var)?;
        let mut out = UniqueVec::new();
        for value in values {
            if let BindingValue::Entity(id) = value {
                out.push(id);
            }
        }
        Some(out)
    }

    /// Merges a clause's match table into the forest: every existing table
    /// sharing a variable is natural-joined with it (conjunctive `WHERE`
    /// semantics); a fully disjoint table starts a new tree.
    pub fn merge_table(&mut self, table: SolutionTable) -> Result<(), KipError> {
        if table.vars.is_empty() {
            return Ok(());
        }
        let mut acc = table;
        // Walk existing tables in order; joining through `acc` keeps every
        // step keyed on at least one shared variable, so two existing
        // tables are never cross-joined unless the new table bridges them.
        let mut idx = 0;
        let mut insert_at: Option<usize> = None;
        while idx < self.tables.len() {
            if acc.vars.iter().any(|var| self.tables[idx].covers(var)) {
                let existing = self.tables.remove(idx);
                // Existing table on the left: its row order (the earlier
                // clauses' order) dominates the merged row order.
                acc = existing.natural_join(&acc)?;
                insert_at.get_or_insert(idx);
            } else {
                idx += 1;
            }
        }
        let at = insert_at.unwrap_or(self.tables.len());
        self.tables.insert(at, acc);
        Ok(())
    }

    /// Joins every table covering any of `vars` into a single table and
    /// returns its forest index. Disconnected tables cross-join under the
    /// [`MAX_SOLUTION_COMBINATIONS`] cap. Returns `None` when no table
    /// covers any of the variables.
    pub fn join_tables_covering<S: AsRef<str>>(
        &mut self,
        vars: &[S],
    ) -> Result<Option<usize>, KipError> {
        let mut indices: Vec<usize> = self
            .tables
            .iter()
            .enumerate()
            .filter(|(_, table)| vars.iter().any(|var| table.covers(var.as_ref())))
            .map(|(idx, _)| idx)
            .collect();
        let Some(&first) = indices.first() else {
            return Ok(None);
        };
        // Remove back-to-front so indices stay valid, then fold in forest
        // order (first table's row order dominates).
        indices.reverse();
        let mut removed: Vec<SolutionTable> = indices
            .into_iter()
            .map(|idx| self.tables.remove(idx))
            .collect();
        removed.reverse();
        let mut iter = removed.into_iter();
        let mut acc = iter.next().expect("at least one table");
        for table in iter {
            acc = acc.natural_join(&table)?;
        }
        self.tables.insert(first, acc);
        Ok(Some(first))
    }

    /// Child context for `NOT` / `OPTIONAL` sub-blocks: shares the entity
    /// cache and seeds one single-column domain table per outer-bound
    /// variable in `seed_vars` (KIP §3.4.7.1/§3.4.7.2 — outer bindings are
    /// visible inside the block). Row correlations are *not* copied: the
    /// block's own patterns re-establish them, and the parent combines the
    /// block's solutions back per its own semantics (anti-join/left join).
    pub fn scoped_child<S: AsRef<str>>(&self, seed_vars: &[S]) -> Self {
        let mut sorted: Vec<&str> = seed_vars.iter().map(|var| var.as_ref()).collect();
        sorted.sort_unstable();
        sorted.dedup();
        let tables = sorted
            .into_iter()
            .filter_map(|var| {
                self.distinct_values(var)
                    .map(|values| SolutionTable::single_column(var.to_string(), values))
            })
            .collect();
        QueryContext {
            tables,
            cache: self.cache.clone(),
            // A child of a lenient sub-block stays lenient (e.g. a `NOT`
            // nested inside an `OPTIONAL`).
            lenient_grounding: self.lenient_grounding,
            ..Default::default()
        }
    }
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

/// One concrete proposition-pattern match row.
///
/// `proposition` is `None` for multi-hop path matches — a path is not a
/// single link — while the `(subject, predicate, object)` triple keeps the
/// solution row aligned (KIP §6.2.2).
#[derive(Clone, Debug)]
pub struct PropositionMatchRow {
    /// Matched proposition id (`None` for a multi-hop path match).
    pub proposition: Option<EntityID>,
    /// Matched subject entity id.
    pub subject: EntityID,
    /// Matched predicate name.
    pub predicate: String,
    /// Matched object entity id.
    pub object: EntityID,
}

/// Result of one proposition-pattern matching operation: the concrete
/// solution rows plus the matched proposition link ids (the pattern's value
/// when it is used as a nested endpoint target).
#[derive(Default)]
pub struct PropositionsMatchResult {
    /// List of matched proposition entity IDs
    pub matched_propositions: UniqueVec<EntityID>,
    /// Concrete row matches preserving subject-predicate-object alignment.
    pub rows: Vec<PropositionMatchRow>,
}

impl PropositionsMatchResult {
    /// Adds a matching proposition: one row (and one matched link id) per
    /// matched predicate.
    pub fn add_match(
        &mut self,
        subject: EntityID,
        object: EntityID,
        predicates: Vec<String>,
        proposition_id: u64,
    ) {
        for pred in predicates {
            let proposition = EntityID::Proposition(proposition_id, pred.clone());
            self.matched_propositions.push(proposition.clone());
            self.rows.push(PropositionMatchRow {
                proposition: Some(proposition),
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
        assert_eq!(by_id.to_string(), r#"{id: "C:7"}"#);

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
        assert_eq!(proposition.to_string(), r#"(id: "P:9:likes")"#);
        assert_eq!(
            PropositionPK::try_from(PropositionMatcher::ID("P:9:likes".to_string())).unwrap(),
            proposition
        );

        let matcher = PropositionMatcher::Object {
            subject: TargetTerm::Concept(ConceptMatcher::Object {
                r#type: "Person".to_string(),
                name: "Ada".to_string(),
            }),
            predicate: PredTerm::Literal("likes".to_string()),
            object: TargetTerm::Concept(ConceptMatcher::ID("C:2".to_string())),
        };
        let object_pk = PropositionPK::try_from(matcher).unwrap();
        assert_eq!(
            object_pk.to_string(),
            r#"({type: "Person", name: "Ada"}, "likes", {id: "C:2"})"#
        );

        assert!(PropositionPK::try_from(PropositionMatcher::ID("C:9".to_string())).is_err());
        assert!(
            PropositionPK::try_from(PropositionMatcher::Object {
                subject: TargetTerm::Variable("s".to_string()),
                predicate: PredTerm::Variable("p".to_string()),
                object: TargetTerm::Concept(ConceptMatcher::ID("C:1".to_string())),
            })
            .is_err()
        );

        assert_eq!(
            EntityPK::try_from(TargetTerm::Concept(ConceptMatcher::ID("C:7".to_string()))).unwrap(),
            EntityPK::Concept(ConceptPK::ID(7))
        );
        assert_eq!(
            EntityPK::try_from(TargetTerm::Proposition(Box::new(PropositionMatcher::ID(
                "P:9:likes".to_string()
            ))))
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
    fn proposition_match_result_keeps_rows_and_link_ids() {
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

        // Link ids deduplicate; rows keep every emitted match row.
        assert_eq!(result.matched_propositions.len(), 2);
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0].subject, concept_ref(1));
        assert_eq!(result.rows[0].object, concept_ref(2));
        assert_eq!(result.rows[0].predicate, "likes");
        assert_eq!(
            result.rows[0].proposition,
            Some(EntityID::Proposition(10, "likes".to_string()))
        );
    }

    fn table(vars: &[&str], rows: &[&[BindingValue]]) -> SolutionTable {
        SolutionTable {
            vars: vars.iter().map(|v| v.to_string()).collect(),
            rows: rows.iter().map(|row| row.to_vec()).collect(),
        }
    }

    fn ent(id: u64) -> BindingValue {
        BindingValue::Entity(concept_ref(id))
    }

    #[test]
    fn solution_table_natural_join_covers_shared_null_and_cross() {
        // Shared-variable equi-join.
        let left = table(&["a", "b"], &[&[ent(1), ent(2)], &[ent(3), ent(4)]]);
        let right = table(&["b", "c"], &[&[ent(2), ent(5)], &[ent(9), ent(6)]]);
        let joined = left.natural_join(&right).unwrap();
        assert_eq!(joined.vars, vec!["a", "b", "c"]);
        assert_eq!(joined.rows, vec![vec![ent(1), ent(2), ent(5)]]);

        // Null is a wildcard and adopts the other side's value.
        let padded = table(&["a", "b"], &[&[ent(1), BindingValue::Null]]);
        let joined = padded.natural_join(&right).unwrap();
        assert_eq!(
            joined.rows,
            vec![vec![ent(1), ent(2), ent(5)], vec![ent(1), ent(9), ent(6)],]
        );

        // No shared variables: cross product.
        let loose = table(&["x"], &[&[ent(7)], &[ent(8)]]);
        let crossed = left.natural_join(&loose).unwrap();
        assert_eq!(crossed.rows.len(), 4);
    }

    #[test]
    fn solution_table_left_join_pads_misses_with_null() {
        let left = table(&["a"], &[&[ent(1)], &[ent(2)]]);
        let right = table(&["a", "b"], &[&[ent(1), ent(5)]]);
        let joined = left.left_join(&right).unwrap();
        assert_eq!(joined.vars, vec!["a", "b"]);
        assert_eq!(
            joined.rows,
            vec![vec![ent(1), ent(5)], vec![ent(2), BindingValue::Null],]
        );
    }

    #[test]
    fn solution_table_union_pads_and_deduplicates() {
        let left = table(&["a"], &[&[ent(1)]]);
        let right = table(&["a", "b"], &[&[ent(1), ent(5)], &[ent(1), ent(5)]]);
        let merged = left.union_padded(&right).unwrap();
        assert_eq!(merged.vars, vec!["a", "b"]);
        assert_eq!(
            merged.rows,
            vec![vec![ent(1), BindingValue::Null], vec![ent(1), ent(5)],]
        );
    }

    #[test]
    fn query_context_forest_merge_and_domains() {
        let mut ctx = QueryContext::default();
        ctx.merge_table(table(&["a"], &[&[ent(1)], &[ent(2)]]))
            .unwrap();
        ctx.merge_table(table(&["b"], &[&[ent(3)]])).unwrap();
        assert_eq!(ctx.tables.len(), 2, "disjoint tables stay separate");

        // A bridging table joins both trees into one.
        ctx.merge_table(table(&["a", "b"], &[&[ent(1), ent(3)]]))
            .unwrap();
        assert_eq!(ctx.tables.len(), 1);
        assert_eq!(ctx.tables[0].rows.len(), 1);

        assert!(ctx.is_bound("a"));
        assert!(!ctx.is_bound("missing"));
        assert_eq!(
            ctx.entity_values("a").unwrap().to_vec(),
            vec![concept_ref(1)]
        );
        assert!(ctx.entity_values("missing").is_none());

        // Child contexts seed single-column domain tables.
        let child = ctx.scoped_child(&["a", "missing"]);
        assert_eq!(child.tables.len(), 1);
        assert_eq!(child.tables[0].vars, vec!["a"]);
    }

    #[test]
    fn query_context_cache_and_target_structs_are_exercised() {
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
