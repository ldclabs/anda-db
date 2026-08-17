//! # `FILTER` evaluation
//!
//! Filters run over solutions that already exist, so they narrow rather than
//! find. Two consequences worth naming:
//!
//! **A filter over an unbound variable removes the row.** `?x` padded by an
//! `OPTIONAL` is not a value, and comparing it as one would silently treat
//! "we have nothing recorded" as a number.
//!
//! **Comparison is typed.** A string never compares less than a number; the
//! comparison simply fails, and the row goes. Ordering unlike types would
//! invent an answer out of representation.

use anda_kip::{
    ComparisonOperator, FilterExpression, FilterFunction, FilterOperand, Json, KipError,
    LogicalOperator,
};

use super::Context;
use super::binding::{Binding, Solutions};

/// One operand's value in one row.
enum Value {
    /// A single value.
    One(Binding),
    /// A bracketed list, the second argument of `IN`.
    List(Vec<Binding>),
}

impl Context<'_> {
    /// Applies one `FILTER` to a set of solutions.
    pub fn apply_filter(
        &mut self,
        solutions: &mut Solutions,
        expression: &FilterExpression,
    ) -> Result<(), KipError> {
        // Errors are collected rather than returned per row: a filter that
        // cannot be evaluated at all is a query bug, and reporting it once is
        // more useful than failing on whichever row happened to be first.
        let mut failure: Option<KipError> = None;
        let snapshot = solutions.clone();
        solutions.rows.retain(|row| {
            if failure.is_some() {
                return false;
            }
            match evaluate(self, &snapshot, row, expression) {
                Ok(keep) => keep,
                Err(err) => {
                    failure = Some(err);
                    false
                }
            }
        });
        match failure {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

fn evaluate(
    cx: &Context<'_>,
    solutions: &Solutions,
    row: &[Binding],
    expression: &FilterExpression,
) -> Result<bool, KipError> {
    Ok(match expression {
        FilterExpression::Not(inner) => !evaluate(cx, solutions, row, inner)?,
        FilterExpression::Logical {
            left,
            operator,
            right,
        } => match operator {
            // Short-circuiting is not an optimization here: the right branch
            // may be unevaluable for a row the left branch already decided.
            LogicalOperator::And => {
                evaluate(cx, solutions, row, left)? && evaluate(cx, solutions, row, right)?
            }
            LogicalOperator::Or => {
                evaluate(cx, solutions, row, left)? || evaluate(cx, solutions, row, right)?
            }
        },
        FilterExpression::Comparison {
            left,
            operator,
            right,
        } => {
            let (Value::One(left), Value::One(right)) = (
                operand(cx, solutions, row, left)?,
                operand(cx, solutions, row, right)?,
            ) else {
                return Err(KipError::type_mismatch(
                    "a comparison takes single values, not lists",
                ));
            };
            compare(&left, &right, *operator)
        }
        FilterExpression::Function { func, args } => {
            let values: Vec<Value> = args
                .iter()
                .map(|arg| operand(cx, solutions, row, arg))
                .collect::<Result<_, _>>()?;
            call(*func, &values)?
        }
    })
}

fn operand(
    cx: &Context<'_>,
    solutions: &Solutions,
    row: &[Binding],
    operand: &FilterOperand,
) -> Result<Value, KipError> {
    Ok(match operand {
        FilterOperand::Literal(literal) => {
            Value::One(Binding::Literal(Json::from(literal.clone())))
        }
        FilterOperand::Param(name) => Value::One(Binding::Literal(cx.param_ref(name)?)),
        FilterOperand::Variable(path) => {
            let binding = solutions
                .get(row, &path.var)
                .cloned()
                .unwrap_or(Binding::Null);
            if path.path.is_empty() {
                return Ok(Value::One(binding));
            }
            // A dot path reads either a projection result — which is already
            // a value — or an element's rendered view, which is why the
            // element has to be loaded before a filter can mention it.
            if let Binding::Literal(value) = &binding {
                return Ok(Value::One(Binding::Literal(crate::view::read_path_in(
                    &cx.env, value, &path.path,
                ))));
            }
            let Some(id) = binding.element() else {
                return Ok(Value::One(Binding::Null));
            };
            let Some(view) = cx.cached_view(id) else {
                return Ok(Value::One(Binding::Null));
            };
            Value::One(Binding::Literal(crate::view::read_path_in(
                &cx.env, &view, &path.path,
            )))
        }
        FilterOperand::List(items) => {
            let mut values = Vec::new();
            for item in items {
                match operand_value(cx, solutions, row, item)? {
                    Value::One(value) => values.push(value),
                    Value::List(mut nested) => values.append(&mut nested),
                }
            }
            Value::List(values)
        }
        FilterOperand::Negate(inner) => match operand_value(cx, solutions, row, inner)? {
            Value::One(Binding::Literal(Json::Number(n))) => {
                let negated = -n.as_f64().unwrap_or(f64::NAN);
                Value::One(Binding::Literal(
                    serde_json::Number::from_f64(negated)
                        .map(Json::Number)
                        .unwrap_or(Json::Null),
                ))
            }
            _ => Value::One(Binding::Null),
        },
    })
}

fn operand_value(
    cx: &Context<'_>,
    solutions: &Solutions,
    row: &[Binding],
    item: &FilterOperand,
) -> Result<Value, KipError> {
    operand(cx, solutions, row, item)
}

/// Typed comparison.
///
/// Returns `false` rather than an error when the operands are of unlike types:
/// two things that cannot be ordered are simply not in that order, and the row
/// is filtered out. Erroring would make one badly-typed row fail a whole query
/// that is otherwise answerable.
fn compare(left: &Binding, right: &Binding, operator: ComparisonOperator) -> bool {
    if matches!(left, Binding::Null) || matches!(right, Binding::Null) {
        // An unbound variable has no value to compare, so only an explicit
        // `IS_NULL` can ask about it.
        return false;
    }
    let (a, b) = (left.to_json(), right.to_json());
    match operator {
        ComparisonOperator::Equal => a == b,
        ComparisonOperator::NotEqual => a != b,
        _ => {
            let ordering = match (&a, &b) {
                (Json::Number(x), Json::Number(y)) => x
                    .as_f64()
                    .zip(y.as_f64())
                    .and_then(|(x, y)| x.partial_cmp(&y)),
                (Json::String(x), Json::String(y)) => Some(x.cmp(y)),
                (Json::Bool(x), Json::Bool(y)) => Some(x.cmp(y)),
                _ => None,
            };
            match ordering {
                None => false,
                Some(ordering) => match operator {
                    ComparisonOperator::LessThan => ordering.is_lt(),
                    ComparisonOperator::LessEqual => ordering.is_le(),
                    ComparisonOperator::GreaterThan => ordering.is_gt(),
                    ComparisonOperator::GreaterEqual => ordering.is_ge(),
                    _ => unreachable!("equality handled above"),
                },
            }
        }
    }
}

fn call(func: FilterFunction, args: &[Value]) -> Result<bool, KipError> {
    let single = |index: usize| -> Result<&Binding, KipError> {
        match args.get(index) {
            Some(Value::One(binding)) => Ok(binding),
            _ => Err(KipError::type_mismatch(format!(
                "{func:?} takes single values in position {}",
                index + 1
            ))),
        }
    };
    let text = |index: usize| -> Result<String, KipError> {
        Ok(match single(index)?.to_json() {
            Json::String(text) => text,
            other => other.to_string(),
        })
    };

    Ok(match func {
        FilterFunction::IsNull => matches!(single(0)?, Binding::Null),
        FilterFunction::IsNotNull => !matches!(single(0)?, Binding::Null),
        FilterFunction::IsElement => single(0)?.is_element(),
        FilterFunction::IsLiteral => single(0)?.is_literal(),
        FilterFunction::IsKind => {
            let expected = text(1)?;
            single(0)?
                .kind()
                .is_some_and(|kind| kind_name(kind).eq_ignore_ascii_case(&expected))
        }
        FilterFunction::LiteralType => {
            // Registered as a function rather than an operator, and it answers
            // a question about representation: what datatype family a value is.
            let expected = text(1).unwrap_or_default();
            let actual = literal_type(single(0)?);
            if expected.is_empty() {
                !actual.is_empty()
            } else {
                actual == expected
            }
        }
        FilterFunction::Contains => matches!(single(0)?, Binding::Null)
            .then_some(false)
            .unwrap_or_else(|| {
                text(0)
                    .unwrap_or_default()
                    .contains(&text(1).unwrap_or_default())
            }),
        FilterFunction::StartsWith => text(0)?.starts_with(&text(1)?),
        FilterFunction::EndsWith => text(0)?.ends_with(&text(1)?),
        FilterFunction::Regex => {
            let pattern = text(1)?;
            let regex = regex::Regex::new(&pattern).map_err(|err| {
                KipError::invalid_syntax(format!("REGEX pattern {pattern:?} is invalid: {err}"))
            })?;
            regex.is_match(&text(0)?)
        }
        FilterFunction::In => {
            let needle = single(0)?.to_json();
            match args.get(1) {
                Some(Value::List(items)) => items.iter().any(|item| item.to_json() == needle),
                Some(Value::One(item)) => item.to_json() == needle,
                None => false,
            }
        }
    })
}

fn kind_name(kind: anda_kip::ElementKind) -> &'static str {
    match kind {
        anda_kip::ElementKind::Concept => "Concept",
        anda_kip::ElementKind::Proposition => "Proposition",
        anda_kip::ElementKind::Assertion => "Assertion",
        anda_kip::ElementKind::Evidence => "Evidence",
        anda_kip::ElementKind::Activity => "Activity",
    }
}

fn literal_type(binding: &Binding) -> String {
    match binding {
        Binding::Literal(Json::String(_)) => crate::term::DT_STRING.to_string(),
        Binding::Literal(Json::Number(_)) => crate::term::DT_NUMBER.to_string(),
        Binding::Literal(Json::Bool(_)) => crate::term::DT_BOOLEAN.to_string(),
        Binding::Literal(Json::Null) => crate::term::DT_NULL.to_string(),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anda_kip::Number;

    fn number(value: f64) -> Binding {
        Binding::Literal(Json::Number(Number::from_f64(value).unwrap()))
    }

    fn text(value: &str) -> Binding {
        Binding::Literal(Json::from(value))
    }

    #[test]
    fn unlike_types_are_not_ordered() {
        // Ordering a string against a number would invent an answer out of
        // representation rather than out of meaning.
        assert!(!compare(
            &text("2"),
            &number(10.0),
            ComparisonOperator::LessThan
        ));
        assert!(!compare(
            &number(10.0),
            &text("2"),
            ComparisonOperator::GreaterThan
        ));
        // Equality across types is simply false, which is well-defined.
        assert!(compare(
            &text("2"),
            &number(2.0),
            ComparisonOperator::NotEqual
        ));
    }

    #[test]
    fn an_unbound_variable_compares_to_nothing() {
        for operator in [
            ComparisonOperator::Equal,
            ComparisonOperator::NotEqual,
            ComparisonOperator::LessThan,
            ComparisonOperator::GreaterEqual,
        ] {
            assert!(
                !compare(&Binding::Null, &number(1.0), operator),
                "{operator:?} should not decide against an unbound variable"
            );
        }
    }

    #[test]
    fn numbers_order_by_value_and_strings_by_code_point() {
        assert!(compare(
            &number(2.0),
            &number(10.0),
            ComparisonOperator::LessThan
        ));
        assert!(compare(
            &text("a"),
            &text("b"),
            ComparisonOperator::LessThan
        ));
        assert!(compare(
            &number(1.0),
            &number(1.0),
            ComparisonOperator::LessEqual
        ));
    }

    #[test]
    fn is_element_and_is_literal_ask_different_questions() {
        let element =
            Binding::Element(crate::id::ElementId::new(anda_kip::ElementKind::Concept, 1));
        let looks_the_same = text("C-1");
        assert!(call(FilterFunction::IsElement, &[Value::One(element.clone())]).unwrap());
        assert!(
            !call(
                FilterFunction::IsElement,
                &[Value::One(looks_the_same.clone())]
            )
            .unwrap()
        );
        assert!(call(FilterFunction::IsLiteral, &[Value::One(looks_the_same)]).unwrap());
        assert!(
            call(
                FilterFunction::IsKind,
                &[Value::One(element), Value::One(text("Concept"))]
            )
            .unwrap()
        );
    }

    #[test]
    fn in_is_membership_over_a_bracketed_list() {
        let args = [
            Value::One(number(2.0)),
            Value::List(vec![number(1.0), number(2.0)]),
        ];
        assert!(call(FilterFunction::In, &args).unwrap());
        let missing = [
            Value::One(number(9.0)),
            Value::List(vec![number(1.0), number(2.0)]),
        ];
        assert!(!call(FilterFunction::In, &missing).unwrap());
    }

    #[test]
    fn an_invalid_regex_is_reported_rather_than_treated_as_no_match() {
        let args = [Value::One(text("abc")), Value::One(text("("))];
        let err = call(FilterFunction::Regex, &args).unwrap_err();
        assert_eq!(err.name(), "InvalidSyntax");
    }
}
