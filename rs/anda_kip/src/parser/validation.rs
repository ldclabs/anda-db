//! Shape checks shared by text parsing and transported executable ASTs.
use std::collections::BTreeSet;

use super::common::Flavor;
use crate::KipError;
use crate::ast::*;

pub(super) fn scalar(value: &Scalar) -> Result<(), KipError> {
    if matches!(
        value,
        Scalar::Literal(KipValue::Array(_) | KipValue::Object(_))
    ) {
        return Err(KipError::invalid_syntax(
            "a scalar literal cannot be an array or object",
        ));
    }
    Ok(())
}

fn scalars<'a>(values: impl IntoIterator<Item = &'a Scalar>) -> Result<(), KipError> {
    values.into_iter().try_for_each(scalar)
}

pub(super) fn bound(value: &BoundValue) -> Result<(), KipError> {
    match value {
        BoundValue::Array(items) => items.iter().try_for_each(bound),
        BoundValue::Object(entries) => entries_unique(entries),
        _ => Ok(()),
    }
}

pub(super) fn entries_unique(entries: &[(String, BoundValue)]) -> Result<(), KipError> {
    let mut seen = BTreeSet::new();
    for (key, value) in entries {
        if !seen.insert(key) {
            return Err(KipError::invalid_syntax(format!(
                "duplicate object key {key:?}"
            )));
        }
        bound(value)?;
    }
    Ok(())
}

pub(super) fn bound_object(object: &BoundObject) -> Result<(), KipError> {
    object.values().try_for_each(bound)
}

pub(super) fn query(query: &KqlQuery) -> Result<(), KipError> {
    if query.find_clause.expressions.is_empty() {
        return Err(KipError::invalid_syntax(
            "FIND needs at least one projection",
        ));
    }
    if query.order_by.as_ref().is_some_and(Vec::is_empty) {
        return Err(KipError::invalid_syntax(
            "ORDER BY needs at least one sort key",
        ));
    }
    if query
        .order_by
        .iter()
        .flatten()
        .any(|item| item.distinct && item.aggregation.is_none())
    {
        return Err(KipError::invalid_syntax(
            "DISTINCT requires an aggregate sort key",
        ));
    }
    if let Some(AsOf::Seq(value)) = &query.as_of {
        scalar(value)?;
    }
    scalars(
        [&query.for_time, &query.limit, &query.cursor]
            .into_iter()
            .flatten(),
    )?;
    if let Some(object) = &query.epistemic {
        bound_object(object)?;
    }
    patterns(&query.where_clauses, Flavor::Kql)
}

pub(super) fn patterns(clauses: &[WhereClause], flavor: Flavor) -> Result<(), KipError> {
    for clause in clauses {
        match clause {
            WhereClause::Concept { matcher, .. }
            | WhereClause::Assertion { matcher, .. }
            | WhereClause::Evidence { matcher, .. }
            | WhereClause::Activity { matcher, .. } => object_matcher(matcher, flavor)?,
            WhereClause::Proposition { matcher, .. } => proposition(matcher, flavor)?,
            WhereClause::Structural {
                subject, object, ..
            } => {
                term(subject, flavor)?;
                term(object, flavor)?;
            }
            WhereClause::Not(inner) | WhereClause::Optional(inner) | WhereClause::Union(inner) => {
                patterns(inner, flavor)?
            }
            WhereClause::Belief { .. } | WhereClause::BeliefSlot { .. }
                if flavor == Flavor::Exact =>
            {
                return Err(KipError::invalid_syntax(
                    "BELIEF cannot be a mutation target or an export selector",
                ));
            }
            WhereClause::Belief { target, .. } => match target {
                BeliefTarget::Tuple(triple) => {
                    if !matches!(triple.predicate, PredTerm::Atom(_)) {
                        return Err(KipError::invalid_syntax("BELIEF needs one exact predicate"));
                    }
                    triple_shape(triple, flavor)?;
                }
                BeliefTarget::Id(value) => scalar(value)?,
                BeliefTarget::Proposition(_) => {}
            },
            WhereClause::BeliefSlot { subject, .. } => proposition_subject(subject, flavor)?,
            WhereClause::Filter { .. } => {}
        }
    }
    Ok(())
}

pub(super) fn proposition_subject(value: &Term, flavor: Flavor) -> Result<(), KipError> {
    if matches!(value, Term::Literal(_)) {
        return Err(KipError::invalid_syntax(
            "a Proposition subject must be an Element reference, never a Literal",
        ));
    }
    term(value, flavor)
}

fn proposition(value: &PropositionMatcher, flavor: Flavor) -> Result<(), KipError> {
    match value {
        PropositionMatcher::Id(value) => scalar(value),
        PropositionMatcher::Tuple(triple) => triple_shape(triple, flavor),
    }
}

fn triple_shape(triple: &PropositionTriple, flavor: Flavor) -> Result<(), KipError> {
    if let PredTerm::Path(atoms) = &triple.predicate {
        if flavor == Flavor::Exact {
            return Err(KipError::invalid_syntax(
                "predicate paths are KQL traversal syntax only",
            ));
        }
        if atoms.is_empty()
            || atoms.iter().any(|atom| {
                atom.hops
                    .is_some_and(|h| h.max.is_some_and(|max| max < h.min))
            })
        {
            return Err(KipError::invalid_syntax(
                "a predicate path needs atoms with ordered hop bounds",
            ));
        }
    }
    proposition_subject(&triple.subject, flavor)?;
    term(&triple.object, flavor)
}

pub(super) fn term(value: &Term, flavor: Flavor) -> Result<(), KipError> {
    match value {
        Term::Match(matcher) => object_matcher(matcher, flavor),
        Term::Proposition(matcher) => proposition(matcher, flavor),
        Term::Literal(KipValue::Array(_) | KipValue::Object(_)) => {
            Err(KipError::invalid_syntax("a tuple literal must be scalar"))
        }
        _ => Ok(()),
    }
}

pub(super) fn object_matcher(matcher: &ObjectMatcher, flavor: Flavor) -> Result<(), KipError> {
    matcher
        .values()
        .try_for_each(|value| match_value(value, flavor))
}

fn match_value(value: &MatchValue, flavor: Flavor) -> Result<(), KipError> {
    match value {
        MatchValue::Array(items) => items
            .iter()
            .try_for_each(|value| match_value(value, flavor)),
        MatchValue::Match(matcher) => object_matcher(matcher, flavor),
        MatchValue::Proposition(matcher) => proposition(matcher, flavor),
        _ => Ok(()),
    }
}

pub(super) fn meta(command: &MetaCommand) -> Result<(), KipError> {
    match command {
        MetaCommand::Describe(target) => match target {
            DescribeTarget::Primer { mode } => scalars(mode)?,
            DescribeTarget::Space { value }
            | DescribeTarget::Trust { value }
            | DescribeTarget::EpistemicPolicy { value } => scalars(value)?,
            DescribeTarget::SchemaEnvironment { as_of } => {
                if let Some(AsOf::Seq(value)) = as_of {
                    scalar(value)?;
                }
            }
            DescribeTarget::Snapshot { as_of, at_time } => {
                if as_of.is_some() && at_time.is_some() {
                    return Err(KipError::invalid_syntax(
                        "DESCRIBE SNAPSHOT takes AS OF SEQ or AT TIME, never both",
                    ));
                }
                if let Some(AsOf::Seq(value)) = as_of {
                    scalar(value)?;
                }
                scalars(at_time)?;
            }
            DescribeTarget::Package(value)
            | DescribeTarget::Type(value)
            | DescribeTarget::Predicate(value)
            | DescribeTarget::Facet(value)
            | DescribeTarget::StructuralField(value)
            | DescribeTarget::Error(value)
            | DescribeTarget::Transaction(value)
            | DescribeTarget::TransactionByIdempotencyKey(value)
            | DescribeTarget::Capsule(value) => scalar(value)?,
            DescribeTarget::Compatibility { from, to } => scalars([from, to])?,
            DescribeTarget::Access { with } => {
                if let Some(object) = with {
                    bound_object(object)?;
                }
            }
            DescribeTarget::Protocol | DescribeTarget::Capabilities => {}
        },
        MetaCommand::List(list) => {
            if (list.target == ListTarget::Dependents) != list.element.is_some()
                || (list.depth.is_some() && list.target != ListTarget::Dependents)
                || (list.status.is_some() && list.target != ListTarget::SchemaPackages)
            {
                return Err(KipError::invalid_syntax(
                    "LIST operands must match its target; DEPENDENTS requires a root",
                ));
            }
            scalars(
                [
                    &list.status,
                    &list.element,
                    &list.depth,
                    &list.limit,
                    &list.cursor,
                ]
                .into_iter()
                .flatten(),
            )?;
        }
        MetaCommand::Search(search) => {
            scalar(&search.term)?;
            scalars(
                [
                    &search.with_type,
                    &search.with_predicate,
                    &search.mode,
                    &search.threshold,
                    &search.as_of_seq,
                    &search.limit,
                    &search.cursor,
                ]
                .into_iter()
                .flatten(),
            )?;
        }
        MetaCommand::Verify { value, .. } | MetaCommand::Preview(PreviewCommand::Kml(value)) => {
            scalar(value)?
        }
        MetaCommand::Validate(command) => {
            scalar(&command.value)?;
            if let Some(object) = &command.options {
                bound_object(object)?;
            }
        }
        MetaCommand::Preview(PreviewCommand::ImportCapsule { capsule, into }) => {
            scalars([capsule, into])?
        }
        MetaCommand::History(history) => match history {
            HistoryCommand::Element {
                value,
                from_seq,
                to_seq,
                limit,
                cursor,
            } => {
                scalar(value)?;
                scalars([from_seq, to_seq, limit, cursor].into_iter().flatten())?;
            }
            HistoryCommand::Space {
                from_seq,
                to_seq,
                limit,
                cursor,
            } => scalars([from_seq, to_seq, limit, cursor].into_iter().flatten())?,
        },
        MetaCommand::Changes(
            ChangesCommand::AfterSeq { seq: value, limit }
            | ChangesCommand::Since {
                cursor: value,
                limit,
            },
        ) => {
            scalar(value)?;
            scalars(limit)?;
        }
        MetaCommand::ExportCapsule(export) => {
            if export.where_clauses.is_empty() {
                return Err(KipError::invalid_syntax(
                    "EXPORT CAPSULE needs at least one selection pattern",
                ));
            }
            patterns(&export.where_clauses, Flavor::Exact)?;
            if let Some(AsOf::Seq(value)) = &export.as_of {
                scalar(value)?;
            }
            if let Some(object) = &export.options {
                bound_object(object)?;
            }
        }
    }
    Ok(())
}
