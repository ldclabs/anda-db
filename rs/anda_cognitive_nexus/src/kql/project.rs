//! # `FIND` — projection, ordering, aggregation, paging
//!
//! Two decisions here are semantic rather than presentational.
//!
//! **Nulls sort last.** An unbound value is not a small value; putting it
//! first under `ASC` would make "nothing recorded" look like the minimum.
//!
//! **The cursor pins a snapshot over a deterministic order.** Paging without a
//! total order returns overlapping or missing rows, so a query that pages gets
//! `id` appended as the final sort key whether or not it asked for one — and
//! the cursor carries the coordinate the traversal began at, because a total
//! order over a moving set is still not one set (§44.8).

use anda_kip::{
    AggregationFunction, FindClause, FindExpression, Json, KipError, OrderByItem, OrderDirection,
};
use std::{cmp::Ordering, collections::HashMap};

use super::Context;
use super::binding::{Binding, Solutions};

/// The result of a projection: rows plus the cursor for the next page.
pub struct Projected {
    /// One JSON value per solution, or per column when a single one is asked.
    pub rows: Vec<Json>,
    /// The cursor a caller passes back to continue, when more rows remain.
    pub next_cursor: Option<String>,
}

impl Context<'_> {
    /// Projects solutions into the result a `FIND` returns.
    pub fn project(
        &mut self,
        mut solutions: Solutions,
        find: &FindClause,
        order_by: Option<&Vec<OrderByItem>>,
        limit: Option<usize>,
        offset: Option<usize>,
        pinned_seq: u64,
    ) -> Result<Projected, KipError> {
        solutions.deduplicate();
        // Every row this engine returns leaves through here, so the cap an
        // authority carries is merged in at this one place rather than by each
        // output path — an aggregate is still output, and a `max_results: 0`
        // authority caps it at nothing just as it caps a row list.
        let limit = match (limit, self.governed_limit()) {
            (Some(requested), Some(governed)) => Some(requested.min(governed)),
            (requested, governed) => requested.or(governed),
        };

        // §44.6: an aggregate anywhere makes this a grouped projection, and
        // the non-aggregated `FIND` expressions are the grouping key. `ORDER
        // BY COUNT(?a)` counts too: it orders groups by an aggregate the
        // caller did not project, and sorting by the bare variable instead
        // would answer a question nobody asked.
        let grouped = find
            .expressions
            .iter()
            .any(|e| matches!(e, FindExpression::Aggregation { .. }))
            || order_by.is_some_and(|items| items.iter().any(|item| item.aggregation.is_some()));
        if grouped {
            return self.project_grouped(solutions, find, order_by, limit, offset, pinned_seq);
        }

        let total = solutions.rows.len();
        let offset = offset.unwrap_or(0);
        self.sort(
            &mut solutions,
            order_by,
            limit.map(|limit| offset.saturating_add(limit)),
        )?;
        let window: Vec<Vec<Binding>> = solutions
            .rows
            .iter()
            .skip(offset)
            .take(limit.unwrap_or(usize::MAX))
            .cloned()
            .collect();
        let consumed = offset + window.len();
        // The cursor carries the coordinate this page was read at, so the
        // next one continues over the same canonical snapshot rather than over
        // whatever the Space holds by then (§44.8).
        let next_cursor = (limit.is_some() && consumed < total).then(|| {
            crate::store::history::PageCursor {
                family: crate::store::history::CursorFamily::Query,
                snapshot_seq: pinned_seq,
                offset: self.page_offset_base.saturating_add(consumed),
                traversal: self.traversal.clone(),
            }
            .issue(self.store, &self.space, &self.auth.principal_id)
        });

        if self.element_page
            && let Some(token) = &next_cursor
            && let Some(id) = window
                .last()
                .and_then(|row| row.first())
                .and_then(Binding::element)
        {
            let mut seeks = self.store.query_seeks.lock();
            if seeks.len() >= 2048 {
                seeks.pop_front();
            }
            seeks.push_back((self.auth.principal_id.clone(), token.clone(), id.seq));
        }
        let mut rows = Vec::with_capacity(window.len());
        for row in &window {
            let mut projected = Vec::with_capacity(find.expressions.len());
            for expression in &find.expressions {
                let FindExpression::Variable(path) = expression else {
                    unreachable!("aggregates handled above");
                };
                projected.push(self.read_variable(&solutions, row, path));
            }
            // One projected column returns bare values rather than
            // single-element arrays: `FIND(?name)` should read as a list of
            // names, not a list of one-name lists.
            rows.push(if projected.len() == 1 {
                projected.remove(0)
            } else {
                Json::Array(projected)
            });
        }
        Ok(Projected { rows, next_cursor })
    }

    /// Reads one projected column out of one solution.
    pub fn read_variable(
        &self,
        solutions: &Solutions,
        row: &[Binding],
        path: &anda_kip::DotPathVar,
    ) -> Json {
        let binding = solutions
            .get(row, &path.var)
            .cloned()
            .unwrap_or(Binding::Null);
        if path.path.is_empty() {
            // A bare element variable projects the whole element, because an
            // Agent asking for `?c` wants the Concept, not its id string.
            return match binding.element().and_then(|id| self.cached_view(id)) {
                Some(view) => view.as_ref().clone(),
                None => binding.to_json(),
            };
        }
        match &binding {
            // A projection result is a value, not an element, and `?b.status`
            // has to read out of it exactly as `?c.name` reads out of a
            // Concept.
            Binding::Literal(value) | Binding::Virtual { value, .. } => {
                crate::view::read_path_in(&self.env, value, &path.path)
            }
            _ => match binding.element().and_then(|id| self.cached_view(id)) {
                Some(view) => crate::view::read_path_in(&self.env, &view, &path.path),
                None => Json::Null,
            },
        }
    }

    fn sort(
        &self,
        solutions: &mut Solutions,
        order_by: Option<&Vec<OrderByItem>>,
        keep: Option<usize>,
    ) -> Result<(), KipError> {
        let snapshot = solutions.header();
        let keys: Vec<(anda_kip::DotPathVar, OrderDirection)> = order_by
            .map(|items| {
                items
                    .iter()
                    .map(|item| (item.variable.clone(), item.direction))
                    .collect()
            })
            .unwrap_or_default();

        // Each row's sort keys are resolved once, not once per comparison:
        // reading one is a view lookup plus a dot-path walk, and a comparison
        // sort asks for them O(n log n) times.
        let mut decorated: Vec<(Vec<Json>, Vec<Binding>)> = solutions
            .rows
            .drain(..)
            .map(|row| {
                let resolved = keys
                    .iter()
                    .map(|(path, _)| self.read_variable(&snapshot, &row, path))
                    .collect();
                (resolved, row)
            })
            .collect();

        for (keys, _) in &decorated {
            if keys.iter().any(|key| key.is_object() || key.is_array()) {
                return Err(KipError::type_mismatch(
                    "ORDER BY requires comparable scalar values; use an Element field path",
                ));
            }
        }

        let compare = |(left_keys, left): &(Vec<Json>, Vec<Binding>),
                       (right_keys, right): &(Vec<Json>, Vec<Binding>)| {
            for (index, (_, direction)) in keys.iter().enumerate() {
                let (left_key, right_key) = (&left_keys[index], &right_keys[index]);
                // Null sorts last in *both* directions (§44.7). Reversing it
                // with the rest would put the unbound rows first under `DESC`,
                // and an absent value is not a large value any more than it is
                // a small one — it is the answer "this row has nothing here",
                // which belongs at the end whichever way the key runs.
                if let Some(ordering) = null_order(left_key, right_key) {
                    if ordering != Ordering::Equal {
                        return ordering;
                    }
                    continue;
                }
                let ordering = compare_json(left_key, right_key);
                if ordering != Ordering::Equal {
                    return match direction {
                        OrderDirection::Asc => ordering,
                        OrderDirection::Desc => ordering.reverse(),
                    };
                }
            }
            // The tiebreaker that makes paging safe: without a total order,
            // two pages of the same query can overlap or skip rows.
            compare_rows(left, right)
        };
        if let Some(keep) = keep.filter(|keep| *keep < decorated.len()) {
            if keep > 0 {
                decorated.select_nth_unstable_by(keep - 1, &compare);
            }
            decorated.truncate(keep);
        }
        decorated.sort_by(compare);

        solutions.rows = decorated.into_iter().map(|(_, row)| row).collect();
        Ok(())
    }

    /// Projects a `FIND` that carries at least one aggregate (§44.6).
    ///
    /// Grouping is implicit: the non-aggregated projected expressions are the
    /// grouping key, and a `FIND` of aggregates alone is one global group. The
    /// group order is the key's own ascending order unless `ORDER BY` says
    /// otherwise, so a paged aggregate reads the same way twice.
    fn project_grouped(
        &mut self,
        solutions: Solutions,
        find: &FindClause,
        order_by: Option<&Vec<OrderByItem>>,
        limit: Option<usize>,
        offset: Option<usize>,
        pinned_seq: u64,
    ) -> Result<Projected, KipError> {
        let keys: Vec<&anda_kip::DotPathVar> = find
            .expressions
            .iter()
            .filter_map(|e| match e {
                FindExpression::Variable(path) => Some(path),
                FindExpression::Aggregation { .. } => None,
            })
            .collect();

        // One group per distinct key tuple, in first-appearance order until
        // the sort below fixes it. A `FIND` of aggregates alone has an empty
        // key, which is one group over every solution — the global aggregate.
        //
        // Indexed by the key's canonical text rather than scanned for: a
        // linear scan per solution is quadratic in the result size, and an
        // aggregate is exactly the query someone runs over everything.
        let mut groups: Vec<(Vec<Json>, Vec<usize>)> = Vec::new();
        let mut index: HashMap<String, usize> = HashMap::new();
        for (row_index, row) in solutions.rows.iter().enumerate() {
            let key: Vec<Json> = keys
                .iter()
                .map(|path| self.read_variable(&solutions, row, path))
                .collect();
            let token = super::binding::value_key(&Json::Array(key.clone()));
            match index.get(&token) {
                Some(at) => groups[*at].1.push(row_index),
                None => {
                    index.insert(token, groups.len());
                    groups.push((key, vec![row_index]));
                }
            }
        }
        // `COUNT` over an empty result is `0`, not an empty answer (§44.6):
        // the global group exists even when nothing matched.
        if groups.is_empty() && keys.is_empty() {
            groups.push((Vec::new(), Vec::new()));
        }

        // Every aggregate this projection needs, in one plan: the ones the
        // caller projected, then the ones only `ORDER BY` asks for.
        let mut plan: Vec<(AggregationFunction, &anda_kip::DotPathVar, bool)> = find
            .expressions
            .iter()
            .filter_map(|e| match e {
                FindExpression::Aggregation {
                    func,
                    var,
                    distinct,
                } => Some((*func, var, *distinct)),
                FindExpression::Variable(_) => None,
            })
            .collect();
        let projected_aggregates = plan.len();
        if let Some(items) = order_by {
            for item in items {
                let Some(func) = item.aggregation else {
                    continue;
                };
                if !plan.iter().any(|(applied, path, distinct)| {
                    *applied == func
                        && *distinct == item.distinct
                        && same_path(path, &item.variable)
                }) {
                    plan.push((func, &item.variable, item.distinct));
                }
            }
        }

        // Resolved once per group — the sort reads them, and so does the row.
        let mut resolved: Vec<(Vec<Json>, Vec<Json>)> = Vec::with_capacity(groups.len());
        for (key, rows) in groups {
            let mut aggregates = Vec::with_capacity(plan.len());
            for (func, var, distinct) in &plan {
                aggregates.push(self.aggregate_column(&solutions, &rows, *func, var, *distinct)?);
            }
            resolved.push((key, aggregates));
        }

        self.sort_groups(&mut resolved, &plan, &keys, order_by)?;

        let offset = offset.unwrap_or(0);
        let total = resolved.len();
        let window: Vec<(Vec<Json>, Vec<Json>)> = resolved
            .into_iter()
            .skip(offset)
            .take(limit.unwrap_or(usize::MAX))
            .collect();
        let consumed = offset + window.len();
        let next_cursor = (limit.is_some() && consumed < total).then(|| {
            crate::store::history::PageCursor {
                family: crate::store::history::CursorFamily::Query,
                snapshot_seq: pinned_seq,
                offset: consumed,
                traversal: self.traversal.clone(),
            }
            .issue(self.store, &self.space, &self.auth.principal_id)
        });

        let mut rows = Vec::with_capacity(window.len());
        for (key, aggregates) in window {
            // Back into the order the caller wrote them in: the two lists were
            // split apart to be computed, and a row that reported them in
            // computation order would not line up with the `FIND` list.
            let (mut keys_left, mut aggregates_left) = (
                key.into_iter(),
                aggregates.into_iter().take(projected_aggregates),
            );
            let mut projected: Vec<Json> = Vec::with_capacity(find.expressions.len());
            for expression in &find.expressions {
                projected.push(match expression {
                    FindExpression::Variable(_) => keys_left.next().unwrap_or(Json::Null),
                    FindExpression::Aggregation { .. } => {
                        aggregates_left.next().unwrap_or(Json::Null)
                    }
                });
            }
            rows.push(if projected.len() == 1 {
                projected.remove(0)
            } else {
                Json::Array(projected)
            });
        }
        Ok(Projected { rows, next_cursor })
    }

    /// Orders groups by the projected columns `ORDER BY` names (§44.7).
    ///
    /// A key that is neither a projected variable nor a projected aggregate
    /// has no value per group — it varies *inside* one — so it is refused
    /// rather than resolved to whichever row happened to come first.
    fn sort_groups(
        &self,
        groups: &mut [(Vec<Json>, Vec<Json>)],
        aggregates: &[(AggregationFunction, &anda_kip::DotPathVar, bool)],
        keys: &[&anda_kip::DotPathVar],
        order_by: Option<&Vec<OrderByItem>>,
    ) -> Result<(), KipError> {
        // `ORDER BY` absent: the grouping key's own ascending order, so a
        // paged aggregate reads the same way twice.
        let mut plan: Vec<(usize, bool, OrderDirection)> = Vec::new();
        match order_by {
            None => {
                for index in 0..keys.len() {
                    plan.push((index, false, OrderDirection::Asc));
                }
            }
            Some(items) => {
                for item in items {
                    let position = match item.aggregation {
                        // DISTINCT is part of the aggregate identity: the
                        // distinct and ordinary counts can order differently.
                        Some(func) => aggregates
                            .iter()
                            .position(|(applied, path, distinct)| {
                                *applied == func
                                    && *distinct == item.distinct
                                    && same_path(path, &item.variable)
                            })
                            .map(|index| (index, true)),
                        None => keys
                            .iter()
                            .position(|path| same_path(path, &item.variable))
                            .map(|index| (index, false)),
                    };
                    let Some((index, is_aggregate)) = position else {
                        return Err(KipError::constraint_violation(format!(
                            "ORDER BY ?{} is not one of the projected columns; grouping makes \
                             the projected expressions the only values a group has, so a sort \
                             key that varies inside a group has no value to sort by",
                            item.variable.var
                        )));
                    };
                    plan.push((index, is_aggregate, item.direction));
                }
                // The tie-breaker that makes a paged aggregate safe, for the
                // same reason `sort` appends one: without a total order, two
                // pages of one query overlap or skip. The grouping key is
                // unique per group, so appending it in ascending order is one
                // — and where `ORDER BY` already decided, it changes nothing.
                for index in 0..keys.len() {
                    if !plan
                        .iter()
                        .any(|(at, is_aggregate, _)| !*is_aggregate && *at == index)
                    {
                        plan.push((index, false, OrderDirection::Asc));
                    }
                }
            }
        }

        groups.sort_by(
            |(left_keys, left_aggregates), (right_keys, right_aggregates)| {
                for (index, is_aggregate, direction) in &plan {
                    let (left, right) = if *is_aggregate {
                        (&left_aggregates[*index], &right_aggregates[*index])
                    } else {
                        (&left_keys[*index], &right_keys[*index])
                    };
                    if let Some(ordering) = null_order(left, right) {
                        if ordering != Ordering::Equal {
                            return ordering;
                        }
                        continue;
                    }
                    let ordering = compare_json(left, right);
                    if ordering != Ordering::Equal {
                        return match direction {
                            OrderDirection::Asc => ordering,
                            OrderDirection::Desc => ordering.reverse(),
                        };
                    }
                }
                Ordering::Equal
            },
        );
        Ok(())
    }

    /// One aggregate over one group's rows.
    fn aggregate_column(
        &self,
        group: &Solutions,
        row_indices: &[usize],
        func: AggregationFunction,
        var: &anda_kip::DotPathVar,
        distinct: bool,
    ) -> Result<Json, KipError> {
        let mut seen = std::collections::HashSet::new();
        let mut column = Vec::new();
        let mut count = 0usize;
        for &index in row_indices {
            let row = &group.rows[index];
            let value = if func == AggregationFunction::Count && var.path.is_empty() {
                group
                    .get(row, &var.var)
                    .map(Binding::to_json)
                    .unwrap_or(Json::Null)
            } else {
                self.read_variable(group, row, var)
            };
            if distinct {
                let key = if var.path.is_empty() {
                    group
                        .get(row, &var.var)
                        .unwrap_or(&Binding::Null)
                        .identity_key()
                } else {
                    super::binding::value_key(&value)
                };
                if !seen.insert(key) {
                    continue;
                }
            }
            if func == AggregationFunction::Count {
                count += usize::from(!value.is_null());
            } else {
                column.push(value);
            }
        }
        if func == AggregationFunction::Count {
            Ok(Json::from(count))
        } else {
            aggregate(func, &column)
        }
    }
}

/// Whether two projected paths name the same value.
fn same_path(left: &anda_kip::DotPathVar, right: &anda_kip::DotPathVar) -> bool {
    left.var == right.var && left.path == right.path
}

/// Apply an aggregate to the non-null values, rejecting incompatible inputs.
fn aggregate(func: AggregationFunction, column: &[Json]) -> Result<Json, KipError> {
    let values: Vec<&Json> = column.iter().filter(|value| !value.is_null()).collect();
    if func == AggregationFunction::Count {
        return Ok(Json::from(values.len()));
    }
    if values.is_empty() {
        return Ok(Json::Null);
    }
    let finish = |value: f64| -> Result<Json, KipError> {
        if !value.is_finite() || (value.fract() == 0.0 && value.abs() > 9_007_199_254_740_991.0) {
            return Err(KipError::type_mismatch(
                "aggregate result must be a finite portable number with safe integer precision",
            ));
        }
        Ok(Json::Number(
            serde_json::Number::from_f64(value).expect("finite number"),
        ))
    };
    match func {
        AggregationFunction::Sum | AggregationFunction::Avg => {
            let numbers = values
                .iter()
                .map(|value| {
                    value.as_f64().ok_or_else(|| {
                        KipError::type_mismatch("SUM and AVG require numeric inputs")
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let sum: f64 = numbers.iter().sum();
            finish(if func == AggregationFunction::Avg {
                sum / numbers.len() as f64
            } else {
                sum
            })
        }
        AggregationFunction::Min | AggregationFunction::Max => {
            let first = values[0];
            if !first.is_boolean() && !first.is_number() && !first.is_string()
                || values
                    .iter()
                    .any(|value| type_rank(value) != type_rank(first))
            {
                return Err(KipError::type_mismatch(
                    "MIN and MAX require mutually comparable scalar inputs",
                ));
            }
            let selected = values
                .into_iter()
                .reduce(|left, right| {
                    let order = compare_json(left, right);
                    if (func == AggregationFunction::Min && order.is_gt())
                        || (func == AggregationFunction::Max && order.is_lt())
                    {
                        right
                    } else {
                        left
                    }
                })
                .expect("nonempty column");
            Ok(selected.clone())
        }
        AggregationFunction::Count => unreachable!(),
    }
}

/// Total order over projected values, with nulls last.
/// The order between two sort keys when either is null, direction-independent.
///
/// `None` means neither is null and the ordinary comparison applies. Kept apart
/// from [`compare_json`] because that one is also the tie-breaker's comparator,
/// where a total order over *every* value — nulls included — is what makes
/// paging safe; here the question is the different one §44.7 answers.
fn null_order(left: &Json, right: &Json) -> Option<Ordering> {
    match (left.is_null(), right.is_null()) {
        (false, false) => None,
        (true, true) => Some(Ordering::Equal),
        (true, false) => Some(Ordering::Greater),
        (false, true) => Some(Ordering::Less),
    }
}

pub fn compare_json(left: &Json, right: &Json) -> Ordering {
    match (left, right) {
        (Json::Null, Json::Null) => Ordering::Equal,
        // Nulls last under ASC: an unbound value is not a small value.
        (Json::Null, _) => Ordering::Greater,
        (_, Json::Null) => Ordering::Less,
        (Json::Number(a), Json::Number(b)) => a
            .as_f64()
            .zip(b.as_f64())
            .and_then(|(a, b)| a.partial_cmp(&b))
            .unwrap_or(Ordering::Equal),
        (Json::String(a), Json::String(b)) => a.cmp(b),
        (Json::Bool(a), Json::Bool(b)) => a.cmp(b),
        // Unlike types get a stable but arbitrary order by their type name, so
        // a mixed column still sorts deterministically instead of depending on
        // which rows the storage happened to return first.
        _ => type_rank(left).cmp(&type_rank(right)),
    }
}

fn type_rank(value: &Json) -> u8 {
    match value {
        Json::Bool(_) => 0,
        Json::Number(_) => 1,
        Json::String(_) => 2,
        Json::Array(_) => 3,
        Json::Object(_) => 4,
        Json::Null => 5,
    }
}

fn compare_rows(left: &[Binding], right: &[Binding]) -> Ordering {
    for (a, b) in left.iter().zip(right.iter()) {
        let ordering = match (a, b) {
            (Binding::Element(a), Binding::Element(b)) => a.cmp(b),
            _ => compare_json(&a.to_json(), &b.to_json()),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
        let identity = a.identity_key().cmp(&b.identity_key());
        if identity != Ordering::Equal {
            return identity;
        }
    }
    left.len().cmp(&right.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nulls_sort_last_in_both_directions() {
        // Spec-adjacent but load-bearing: under ASC a null must not look like
        // the minimum, or "nothing recorded" ranks above every real value.
        let mut values = vec![Json::Null, Json::from(2), Json::from(1)];
        values.sort_by(compare_json);
        assert_eq!(values, vec![Json::from(1), Json::from(2), Json::Null]);
    }

    #[test]
    fn a_mixed_column_still_sorts_deterministically() {
        let mut values = vec![
            Json::from("b"),
            Json::from(1),
            Json::from(true),
            Json::from("a"),
        ];
        values.sort_by(compare_json);
        assert_eq!(
            values,
            vec![
                Json::from(true),
                Json::from(1),
                Json::from("a"),
                Json::from("b")
            ]
        );
    }

    #[test]
    fn the_average_of_nothing_is_unknown_not_zero() {
        assert_eq!(
            aggregate(AggregationFunction::Avg, &[]).unwrap(),
            Json::Null
        );
        assert_eq!(
            aggregate(AggregationFunction::Min, &[]).unwrap(),
            Json::Null
        );
        // SUM follows the same empty-group null rule as AVG/MIN/MAX.
        assert_eq!(
            aggregate(AggregationFunction::Sum, &[]).unwrap(),
            Json::Null
        );
    }

    #[test]
    fn count_counts_values_not_rows() {
        let column = [Json::from(1), Json::Null, Json::from(3)];
        assert_eq!(
            aggregate(AggregationFunction::Count, &column).unwrap(),
            Json::from(2)
        );
        assert_eq!(
            aggregate(AggregationFunction::Avg, &column).unwrap(),
            Json::from(2.0)
        );
    }
}
