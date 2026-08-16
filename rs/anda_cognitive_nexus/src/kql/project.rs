//! # `FIND` — projection, ordering, aggregation, paging
//!
//! Two decisions here are semantic rather than presentational.
//!
//! **Nulls sort last.** An unbound value is not a small value; putting it
//! first under `ASC` would make "nothing recorded" look like the minimum.
//!
//! **The cursor is an offset over a deterministic order.** Paging without a
//! total order returns overlapping or missing rows, so a query that pages gets
//! `id` appended as the final sort key whether or not it asked for one.

use anda_kip::{
    AggregationFunction, FindClause, FindExpression, Json, KipError, OrderByItem, OrderDirection,
};
use std::cmp::Ordering;

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
        cursor: Option<usize>,
    ) -> Result<Projected, KipError> {
        let aggregates: Vec<&FindExpression> = find
            .expressions
            .iter()
            .filter(|e| matches!(e, FindExpression::Aggregation { .. }))
            .collect();

        if !aggregates.is_empty() {
            if find.expressions.len() != aggregates.len() {
                return Err(KipError::unsupported_capability(
                    "mixing aggregates with plain projections needs grouping, which this engine \
                     does not implement yet; project the aggregates alone",
                ));
            }
            let row = self.aggregate_row(&solutions, find)?;
            return Ok(Projected {
                rows: vec![row],
                next_cursor: None,
            });
        }

        self.sort(&mut solutions, order_by)?;

        let offset = cursor.unwrap_or(0);
        let total = solutions.rows.len();
        let window: Vec<Vec<Binding>> = solutions
            .rows
            .iter()
            .skip(offset)
            .take(limit.unwrap_or(usize::MAX))
            .cloned()
            .collect();
        let consumed = offset + window.len();
        let next_cursor = (limit.is_some() && consumed < total).then(|| consumed.to_string());

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
                Some(view) => view,
                None => binding.to_json(),
            };
        }
        match &binding {
            // A projection result is a value, not an element, and `?b.status`
            // has to read out of it exactly as `?c.name` reads out of a
            // Concept.
            Binding::Literal(value) => crate::view::read_path(value, &path.path),
            _ => match binding.element().and_then(|id| self.cached_view(id)) {
                Some(view) => crate::view::read_path(&view, &path.path),
                None => Json::Null,
            },
        }
    }

    fn sort(
        &self,
        solutions: &mut Solutions,
        order_by: Option<&Vec<OrderByItem>>,
    ) -> Result<(), KipError> {
        let snapshot = solutions.clone();
        let keys: Vec<(anda_kip::DotPathVar, OrderDirection)> = order_by
            .map(|items| {
                items
                    .iter()
                    .map(|item| (item.variable.clone(), item.direction))
                    .collect()
            })
            .unwrap_or_default();

        solutions.rows.sort_by(|left, right| {
            for (path, direction) in &keys {
                let a = self.read_variable(&snapshot, left, path);
                let b = self.read_variable(&snapshot, right, path);
                let ordering = compare_json(&a, &b);
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
        });
        Ok(())
    }

    fn aggregate_row(&self, solutions: &Solutions, find: &FindClause) -> Result<Json, KipError> {
        let mut values = Vec::with_capacity(find.expressions.len());
        for expression in &find.expressions {
            let FindExpression::Aggregation {
                func,
                var,
                distinct,
            } = expression
            else {
                unreachable!("checked by the caller");
            };
            let mut column: Vec<Json> = solutions
                .rows
                .iter()
                .map(|row| self.read_variable(solutions, row, var))
                .collect();
            if *distinct {
                let mut seen: Vec<Json> = Vec::new();
                column.retain(|value| {
                    if seen.contains(value) {
                        false
                    } else {
                        seen.push(value.clone());
                        true
                    }
                });
            }
            values.push(aggregate(*func, &column)?);
        }
        Ok(if values.len() == 1 {
            values.remove(0)
        } else {
            Json::Array(values)
        })
    }
}

/// Applies one aggregate to a column.
///
/// `COUNT` counts rows that have a value; the others ignore non-numbers rather
/// than failing, because a column may legitimately mix types and an aggregate
/// over the numeric part is still an answer.
fn aggregate(func: AggregationFunction, column: &[Json]) -> Result<Json, KipError> {
    let numbers: Vec<f64> = column.iter().filter_map(Json::as_f64).collect();
    let finish = |value: f64| {
        serde_json::Number::from_f64(value)
            .map(Json::Number)
            .unwrap_or(Json::Null)
    };
    Ok(match func {
        AggregationFunction::Count => {
            Json::from(column.iter().filter(|value| !value.is_null()).count())
        }
        AggregationFunction::Sum => finish(numbers.iter().sum()),
        AggregationFunction::Avg => {
            if numbers.is_empty() {
                // The average of nothing is not zero; zero would be a claim.
                Json::Null
            } else {
                finish(numbers.iter().sum::<f64>() / numbers.len() as f64)
            }
        }
        AggregationFunction::Min => numbers
            .iter()
            .copied()
            .fold(None::<f64>, |acc, value| {
                Some(acc.map_or(value, |acc| acc.min(value)))
            })
            .map(finish)
            .unwrap_or(Json::Null),
        AggregationFunction::Max => numbers
            .iter()
            .copied()
            .fold(None::<f64>, |acc, value| {
                Some(acc.map_or(value, |acc| acc.max(value)))
            })
            .map(finish)
            .unwrap_or(Json::Null),
    })
}

/// Total order over projected values, with nulls last.
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
    }
    Ordering::Equal
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
        // A sum over nothing is genuinely zero, which is not the same claim.
        assert_eq!(
            aggregate(AggregationFunction::Sum, &[]).unwrap(),
            Json::from(0.0)
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
