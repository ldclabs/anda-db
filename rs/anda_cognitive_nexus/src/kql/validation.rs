//! Static variable scopes and expression checks, shared by KQL and raw WHERE.
use super::Context;
use anda_kip::{
    BeliefTarget, FilterExpression, FilterOperand, FindExpression, KipError, KqlQuery, MatchValue,
    ObjectMatcher, PredAtom, PredTerm, PropositionMatcher, Term, WhereClause,
};
use std::collections::BTreeSet;

type Scope = BTreeSet<String>;

fn visible(name: &str, scope: &Scope) -> Result<(), KipError> {
    if scope.contains(name) {
        Ok(())
    } else {
        Err(KipError::invalid_syntax(format!(
            "?{name} has no visible pattern binding site in this scope"
        )))
    }
}

fn operand(cx: &Context<'_>, arg: &FilterOperand, scope: &Scope) -> Result<(), KipError> {
    match arg {
        FilterOperand::Variable(path) => {
            visible(&path.var, scope)?;
            validate_path(cx, path)
        }
        FilterOperand::List(items) => items.iter().try_for_each(|item| operand(cx, item, scope)),
        FilterOperand::Negate(inner) => operand(cx, inner, scope),
        _ => Ok(()),
    }
}

fn expression(cx: &Context<'_>, expr: &FilterExpression, scope: &Scope) -> Result<(), KipError> {
    match expr {
        FilterExpression::Comparison { left, right, .. } => {
            operand(cx, left, scope)?;
            operand(cx, right, scope)
        }
        FilterExpression::Logical { left, right, .. } => {
            expression(cx, left, scope)?;
            expression(cx, right, scope)
        }
        FilterExpression::Not(inner) => expression(cx, inner, scope),
        FilterExpression::Function { args, .. } => {
            args.iter().try_for_each(|arg| operand(cx, arg, scope))
        }
    }
}

fn matcher(matcher: &ObjectMatcher, scope: &mut Scope) {
    fn value(item: &MatchValue, scope: &mut Scope) {
        match item {
            MatchValue::Variable(name) => {
                scope.insert(name.clone());
            }
            MatchValue::Match(inner) => self::matcher(inner, scope),
            MatchValue::Proposition(inner) => proposition(inner, scope),
            MatchValue::Array(items) => {
                for item in items {
                    value(item, scope);
                }
            }
            _ => {}
        }
    }
    for field in matcher.values() {
        value(field, scope);
    }
}

fn term(term: &Term, scope: &mut Scope) {
    match term {
        Term::Variable(name) => {
            scope.insert(name.clone());
        }
        Term::Match(inner) => matcher(inner, scope),
        Term::Proposition(inner) => proposition(inner, scope),
        _ => {}
    }
}

fn proposition(prop: &PropositionMatcher, scope: &mut Scope) {
    if let PropositionMatcher::Tuple(triple) = prop {
        term(&triple.subject, scope);
        term(&triple.object, scope);
        if let PredTerm::Atom(PredAtom::Variable(name)) = &triple.predicate {
            scope.insert(name.clone());
        }
    }
}

pub(super) fn validate_block(
    cx: &mut Context<'_>,
    clauses: &[WhereClause],
    incoming: &Scope,
) -> Result<Scope, KipError> {
    let mut scope = incoming.clone();
    for clause in clauses {
        cx.validate_pattern(clause)?;
        match clause {
            WhereClause::Concept {
                variable,
                matcher: fields,
            }
            | WhereClause::Assertion {
                variable,
                matcher: fields,
            }
            | WhereClause::Evidence {
                variable,
                matcher: fields,
            }
            | WhereClause::Activity {
                variable,
                matcher: fields,
            } => {
                scope.insert(variable.clone());
                matcher(fields, &mut scope);
            }
            WhereClause::Proposition { variable, matcher } => {
                scope.extend(variable.iter().cloned());
                proposition(matcher, &mut scope);
            }
            WhereClause::Structural {
                variable,
                subject,
                object,
                ..
            } => {
                scope.extend(variable.iter().cloned());
                term(subject, &mut scope);
                term(object, &mut scope);
            }
            WhereClause::Belief { variable, target } => {
                if let BeliefTarget::Tuple(triple) = target {
                    proposition(&PropositionMatcher::Tuple(triple.clone()), &mut scope);
                }
                scope.insert(variable.clone());
            }
            WhereClause::BeliefSlot {
                variable,
                subject,
                predicate,
            } => {
                term(subject, &mut scope);
                if let PredAtom::Variable(name) = predicate {
                    scope.insert(name.clone());
                }
                scope.insert(variable.clone());
            }
            WhereClause::Filter { expression: expr } => {
                expression(cx, expr, &scope)?;
                super::filter::validate_expression(cx, expr)?;
            }
            WhereClause::Not(inner) => {
                validate_block(cx, inner, &scope)?;
            }
            WhereClause::Optional(inner) => {
                scope.extend(validate_block(cx, inner, &scope)?);
            }
            WhereClause::Union(inner) => {
                let ambient = cx.ambient.vars.iter().cloned().collect();
                scope.extend(validate_block(cx, inner, &ambient)?);
            }
        }
    }
    Ok(scope)
}

pub(super) fn validate_projection(
    cx: &Context<'_>,
    query: &KqlQuery,
    scope: &Scope,
) -> Result<(), KipError> {
    let grouped = query
        .find_clause
        .expressions
        .iter()
        .any(|expr| matches!(expr, FindExpression::Aggregation { .. }));
    for expr in &query.find_clause.expressions {
        let path = match expr {
            FindExpression::Variable(path) => path,
            FindExpression::Aggregation { var, .. } => var,
        };
        visible(&path.var, scope)?;
        validate_path(cx, path)?;
    }
    for item in query.order_by.iter().flatten() {
        visible(&item.variable.var, scope)?;
        validate_path(cx, &item.variable)?;
        if let Some(func) = item.aggregation {
            if !query.find_clause.expressions.iter().any(|expr| matches!(expr, FindExpression::Aggregation { func: found, var, distinct } if *found == func && *distinct == item.distinct && var == &item.variable)) {
                return Err(KipError::invalid_syntax("an ORDER BY aggregate must also appear in FIND"));
            }
        } else if grouped
            && !query.find_clause.expressions.iter().any(
                |expr| matches!(expr, FindExpression::Variable(path) if path == &item.variable),
            )
        {
            return Err(KipError::invalid_syntax(
                "a grouped ORDER BY value must be a grouping expression in FIND",
            ));
        }
    }
    Ok(())
}

/// A missing Facet value is null; an unknown Schema symbol is a command error.
fn validate_path(cx: &Context<'_>, path: &anda_kip::DotPathVar) -> Result<(), KipError> {
    use anda_kip::PathStep;
    if let [
        PathStep::Field(head) | PathStep::Key(head),
        PathStep::Field(name) | PathStep::Key(name),
        ..,
    ] = path.path.as_slice()
        && head == "facets"
    {
        cx.env.resolve_symbol(
            crate::schema::SymbolKind::Facet,
            name,
            crate::schema::Intent::Read,
        )?;
    }
    Ok(())
}
