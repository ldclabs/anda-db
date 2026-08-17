//! # Solutions
//!
//! A `WHERE` block is a conjunction of patterns over shared variables, so
//! evaluating it is relational algebra: each pattern produces a table, and the
//! tables natural-join on the variables they have in common.
//!
//! The one thing worth stating up front is what a variable can hold. A KQL
//! variable is not always an element:
//!
//! ```text
//! ?c   in  ?c CONCEPT {...}              an element
//! ?tz  in  (:alice, "timezone", ?tz)     a Literal — the object of a tuple
//! ?p   in  (?s, ?p, ?o)                  a predicate symbol
//! ```
//!
//! Collapsing those into "some JSON value" would lose the distinction between
//! the *element* `C-1` and the *string* `"C-1"`, which is exactly the
//! distinction a filter like `IS_ELEMENT(?x)` exists to ask about.

use anda_kip::{ElementKind, Json};
use std::collections::BTreeMap;

use crate::id::ElementId;

/// What one variable is bound to in one solution.
#[derive(Clone, Debug, PartialEq)]
pub enum Binding {
    /// A Cognitive Element.
    Element(ElementId),
    /// A Literal value — the object endpoint of a tuple, or a read field.
    Literal(Json),
    /// A schema symbol, when a predicate slot was written as a variable.
    Symbol(String),
    /// Unbound. Produced only by `OPTIONAL`, which pads the rows it could not
    /// extend rather than dropping them.
    Null,
}

impl Binding {
    /// The value a projection or a filter sees.
    pub fn to_json(&self) -> Json {
        match self {
            Binding::Element(id) => Json::String(id.to_string()),
            Binding::Literal(value) => value.clone(),
            Binding::Symbol(symbol) => Json::String(symbol.clone()),
            Binding::Null => Json::Null,
        }
    }

    /// The element this binding names, if it names one.
    pub fn element(&self) -> Option<ElementId> {
        match self {
            Binding::Element(id) => Some(*id),
            _ => None,
        }
    }

    /// Whether this is an element reference rather than a value.
    pub fn is_element(&self) -> bool {
        matches!(self, Binding::Element(_))
    }

    /// Whether this is a Literal value rather than a reference.
    pub fn is_literal(&self) -> bool {
        matches!(self, Binding::Literal(_))
    }

    /// The Core kind name, when this binding names an element.
    pub fn kind(&self) -> Option<ElementKind> {
        self.element().map(|id| id.kind)
    }
}

/// A set of solutions over a shared set of variables.
#[derive(Clone, Debug, Default)]
pub struct Solutions {
    /// The variable names, in column order.
    pub vars: Vec<String>,
    /// One row per solution.
    pub rows: Vec<Vec<Binding>>,
}

impl Solutions {
    /// The single solution that binds nothing.
    ///
    /// Not the same as [`Solutions::empty`]: this one satisfies every pattern
    /// that constrains no variable, which is what makes an empty `WHERE` block
    /// return one row rather than none.
    pub fn unit() -> Self {
        Self {
            vars: vec![],
            rows: vec![vec![]],
        }
    }

    /// No solutions at all.
    pub fn empty() -> Self {
        Self {
            vars: vec![],
            rows: vec![],
        }
    }

    /// A one-column table.
    pub fn column(var: &str, values: Vec<Binding>) -> Self {
        Self {
            vars: vec![var.to_string()],
            rows: values.into_iter().map(|value| vec![value]).collect(),
        }
    }

    /// A table over several variables.
    pub fn table(vars: Vec<String>, rows: Vec<Vec<Binding>>) -> Self {
        Self { vars, rows }
    }

    /// Whether any solution survives.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Whether a variable is bound here.
    pub fn binds(&self, var: &str) -> bool {
        self.vars.iter().any(|name| name == var)
    }

    fn index_of(&self, var: &str) -> Option<usize> {
        self.vars.iter().position(|name| name == var)
    }

    /// Reads one variable out of one row.
    pub fn get<'a>(&'a self, row: &'a [Binding], var: &str) -> Option<&'a Binding> {
        self.index_of(var).and_then(|index| row.get(index))
    }

    /// The distinct values one variable takes.
    pub fn values_of(&self, var: &str) -> Vec<Binding> {
        let Some(index) = self.index_of(var) else {
            return vec![];
        };
        let mut seen: Vec<Binding> = Vec::new();
        for row in &self.rows {
            if let Some(value) = row.get(index)
                && !seen.contains(value)
            {
                seen.push(value.clone());
            }
        }
        seen
    }

    /// The distinct elements one variable takes.
    pub fn elements_of(&self, var: &str) -> Vec<ElementId> {
        self.values_of(var)
            .into_iter()
            .filter_map(|binding| binding.element())
            .collect()
    }

    /// Natural join: rows agree on every shared variable.
    ///
    /// With no shared variable this is a cross product, which is the correct
    /// reading of two independent patterns in one `WHERE` block — and also why
    /// the caller caps result size rather than trusting the query to be small.
    pub fn join(self, other: Solutions) -> Solutions {
        if self.vars.is_empty() && self.rows.len() == 1 {
            return other;
        }
        if other.vars.is_empty() && other.rows.len() == 1 {
            return self;
        }
        if self.is_empty() || other.is_empty() {
            // One unsatisfiable conjunct makes the conjunction unsatisfiable,
            // but the variable set still has to grow: a later `OPTIONAL` needs
            // to know which columns exist.
            return Solutions {
                vars: union_vars(&self.vars, &other.vars),
                rows: vec![],
            };
        }

        let shared: Vec<(usize, usize)> = self
            .vars
            .iter()
            .enumerate()
            .filter_map(|(left, name)| {
                other
                    .vars
                    .iter()
                    .position(|other_name| other_name == name)
                    .map(|right| (left, right))
            })
            .collect();
        let carried: Vec<usize> = other
            .vars
            .iter()
            .enumerate()
            .filter(|(index, _)| !shared.iter().any(|(_, right)| right == index))
            .map(|(index, _)| index)
            .collect();

        let mut vars = self.vars.clone();
        vars.extend(carried.iter().map(|index| other.vars[*index].clone()));

        let mut rows = Vec::new();
        for left in &self.rows {
            for right in &other.rows {
                if !shared
                    .iter()
                    .all(|(l, r)| compatible(&left[*l], &right[*r]))
                {
                    continue;
                }
                let mut row = left.clone();
                for index in &carried {
                    row.push(right[*index].clone());
                }
                rows.push(row);
            }
        }
        Solutions { vars, rows }
    }

    /// Left join: every left row survives, padded when nothing matched.
    ///
    /// This is `OPTIONAL` (§46). Dropping the unmatched rows instead would
    /// turn "we have no birth date for Bob" into "Bob does not exist", which
    /// is the open-world mistake in miniature.
    pub fn left_join(self, other: Solutions) -> Solutions {
        if other.is_empty() && other.vars.is_empty() {
            return self;
        }
        let extended = self.clone().join(other.clone());
        let added: Vec<String> = other
            .vars
            .iter()
            .filter(|name| !self.binds(name))
            .cloned()
            .collect();
        if added.is_empty() {
            // The optional block bound nothing new, so it can only filter —
            // and an OPTIONAL must never filter. Keep the left side.
            return self;
        }

        let mut vars = self.vars.clone();
        vars.extend(added.clone());
        let mut rows: Vec<Vec<Binding>> = Vec::new();
        for left in &self.rows {
            let matches: Vec<&Vec<Binding>> = extended
                .rows
                .iter()
                .filter(|row| row[..left.len()] == left[..])
                .collect();
            if matches.is_empty() {
                let mut padded = left.clone();
                padded.extend(std::iter::repeat_n(Binding::Null, added.len()));
                rows.push(padded);
            } else {
                for row in matches {
                    rows.push(row.clone());
                }
            }
        }
        Solutions { vars, rows }
    }

    /// Anti-join: keep the left rows that the right side cannot extend.
    ///
    /// This is `NOT { ... }`. It asks about the *recorded* graph, never about
    /// the world: "no Assertion says so" is not "it is false" (§24).
    pub fn anti_join(self, other: Solutions) -> Solutions {
        if other.is_empty() {
            return self;
        }
        let extended = self.clone().join(other);
        let vars = self.vars.clone();
        let rows = self
            .rows
            .into_iter()
            .filter(|left| {
                !extended
                    .rows
                    .iter()
                    .any(|row| row[..left.len()] == left[..])
            })
            .collect();
        Solutions { vars, rows }
    }

    /// Union: both sides' rows, widened to the same columns.
    ///
    /// A branch that does not bind a column pads it with `Null`, so a `UNION`
    /// of differently-shaped branches stays one table rather than silently
    /// dropping the narrower side.
    pub fn union(self, other: Solutions) -> Solutions {
        let vars = union_vars(&self.vars, &other.vars);
        let mut rows = Vec::with_capacity(self.rows.len() + other.rows.len());
        for source in [&self, &other] {
            for row in &source.rows {
                rows.push(
                    vars.iter()
                        .map(|name| source.get(row, name).cloned().unwrap_or(Binding::Null))
                        .collect(),
                );
            }
        }
        Solutions { vars, rows }
    }

    /// Restricts to rows a predicate accepts.
    pub fn retain(&mut self, mut keep: impl FnMut(&Solutions, &[Binding]) -> bool) {
        let snapshot = self.clone();
        self.rows.retain(|row| keep(&snapshot, row));
    }

    /// One row as a variable map, for projection and ordering.
    pub fn row_map(&self, row: &[Binding]) -> BTreeMap<String, Binding> {
        self.vars.iter().cloned().zip(row.iter().cloned()).collect()
    }
}

/// Whether two bindings of the same variable agree.
///
/// `Null` agrees with anything: it is the padding an `OPTIONAL` left behind,
/// meaning "this pattern had nothing to say here", not "this pattern says the
/// value is null".
fn compatible(left: &Binding, right: &Binding) -> bool {
    matches!(left, Binding::Null) || matches!(right, Binding::Null) || left == right
}

fn union_vars(left: &[String], right: &[String]) -> Vec<String> {
    let mut vars = left.to_vec();
    for name in right {
        if !vars.contains(name) {
            vars.push(name.clone());
        }
    }
    vars
}

#[cfg(test)]
mod tests {
    use super::*;

    fn concept(seq: u64) -> Binding {
        Binding::Element(ElementId::new(ElementKind::Concept, seq))
    }

    fn table(vars: &[&str], rows: Vec<Vec<Binding>>) -> Solutions {
        Solutions::table(vars.iter().map(|s| s.to_string()).collect(), rows)
    }

    #[test]
    fn an_element_and_its_id_string_are_different_bindings() {
        // `IS_ELEMENT(?x)` exists to ask this question, so the two must not
        // collapse into one representation.
        let element = concept(1);
        let text = Binding::Literal(Json::from("C-1"));
        assert_eq!(element.to_json(), text.to_json());
        assert_ne!(element, text);
        assert!(element.is_element() && !element.is_literal());
        assert!(text.is_literal() && !text.is_element());
    }

    #[test]
    fn a_join_agrees_on_shared_variables_and_crosses_the_rest() {
        let left = table(&["a"], vec![vec![concept(1)], vec![concept(2)]]);
        let right = table(
            &["a", "b"],
            vec![vec![concept(1), concept(10)], vec![concept(3), concept(30)]],
        );
        let joined = left.clone().join(right);
        assert_eq!(joined.vars, vec!["a", "b"]);
        assert_eq!(joined.rows.len(), 1);
        assert_eq!(joined.rows[0], vec![concept(1), concept(10)]);

        // No shared variable is a cross product — two independent patterns.
        let independent = table(&["z"], vec![vec![concept(9)], vec![concept(8)]]);
        assert_eq!(left.join(independent).rows.len(), 4);
    }

    #[test]
    fn the_unit_solution_is_the_identity_of_join() {
        let some = table(&["a"], vec![vec![concept(1)]]);
        assert_eq!(Solutions::unit().join(some.clone()).rows.len(), 1);
        assert_eq!(some.join(Solutions::unit()).rows.len(), 1);
        // And an empty `WHERE` produces exactly one solution, not zero.
        assert_eq!(Solutions::unit().rows.len(), 1);
        assert!(Solutions::empty().is_empty());
    }

    #[test]
    fn an_optional_pads_rather_than_drops() {
        // Spec §46: dropping the unmatched row would turn "no birth date on
        // record" into "this person does not exist".
        let people = table(&["p"], vec![vec![concept(1)], vec![concept(2)]]);
        let dates = table(
            &["p", "d"],
            vec![vec![concept(1), Binding::Literal(Json::from("1970"))]],
        );
        let result = people.left_join(dates);
        assert_eq!(result.vars, vec!["p", "d"]);
        assert_eq!(result.rows.len(), 2);
        let padded = result.rows.iter().find(|row| row[0] == concept(2)).unwrap();
        assert_eq!(padded[1], Binding::Null);
    }

    #[test]
    fn an_anti_join_keeps_what_the_pattern_could_not_extend() {
        let people = table(&["p"], vec![vec![concept(1)], vec![concept(2)]]);
        let has_date = table(&["p"], vec![vec![concept(1)]]);
        let result = people.anti_join(has_date);
        assert_eq!(result.rows, vec![vec![concept(2)]]);
    }

    #[test]
    fn a_union_widens_both_branches_to_one_shape() {
        let left = table(&["a"], vec![vec![concept(1)]]);
        let right = table(&["b"], vec![vec![concept(2)]]);
        let result = left.union(right);
        assert_eq!(result.vars, vec!["a", "b"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0], vec![concept(1), Binding::Null]);
        assert_eq!(result.rows[1], vec![Binding::Null, concept(2)]);
    }

    #[test]
    fn optional_padding_does_not_block_a_later_join() {
        // A padded column means "this branch had nothing to say", so a later
        // pattern that does bind it must still be able to.
        let padded = table(&["a", "b"], vec![vec![concept(1), Binding::Null]]);
        let bound = table(&["b"], vec![vec![concept(5)]]);
        assert_eq!(padded.join(bound).rows.len(), 1);
    }
}
