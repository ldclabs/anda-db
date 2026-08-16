//! KQL — the Cognitive Query Language (Spec §42–§50).
//!
//! KQL reads raw cognitive state by default. `BELIEF` is the only clause that
//! interprets it, and it is a read-only Projection: never a mutation target.

use nom::{
    Parser,
    branch::alt,
    character::complete::char,
    combinator::{cut, map, opt, value},
    multi::separated_list1,
    sequence::preceded,
};

use super::common::{
    Flavor, VResult, bound_object, dot_path_var, fail, identifier, opt_after, parenthesized,
    scalar, where_block, word, words, ws,
};
use crate::ast::{
    AggregationFunction, AsOf, DotPathVar, FindClause, FindExpression, KqlQuery, OrderByItem,
    OrderDirection,
};

/// Parses a whole `FIND ... WHERE ...` query.
pub fn parse_kql_query(input: &str) -> VResult<'_, KqlQuery> {
    let (input, _) = ws(word("FIND")).parse(input)?;
    let (input, expressions) = cut(parenthesized(separated_list1(
        ws(char(',')),
        ws(find_expression),
    )))
    .parse(input)?;

    let (input, _) = cut(ws(word("WHERE"))).parse(input)?;
    let (input, where_clauses) = cut(|i| where_block(i, Flavor::Kql)).parse(input)?;

    let (input, as_of) = opt(ws(as_of_clause)).parse(input)?;
    let (input, for_time) = opt_after(&["FOR", "TIME"], ws(scalar)).parse(input)?;
    let (input, epistemic) = opt_after(&["WITH", "EPISTEMIC"], ws(bound_object)).parse(input)?;
    let (input, order_by) = opt_after(
        &["ORDER", "BY"],
        separated_list1(ws(char(',')), ws(order_item)),
    )
    .parse(input)?;
    let (input, limit) = opt_after(&["LIMIT"], ws(scalar)).parse(input)?;
    let (input, cursor) = opt_after(&["CURSOR"], ws(scalar)).parse(input)?;

    Ok((
        input,
        KqlQuery {
            find_clause: FindClause { expressions },
            where_clauses,
            as_of,
            for_time,
            epistemic,
            order_by,
            limit,
            cursor,
        },
    ))
}

/// `AS OF SEQ|TX|TIME ...` — the cognitive history the read runs against.
///
/// Shared with META, which uses the same clause on `SNAPSHOT`, `DESCRIBE
/// SNAPSHOT`, `DESCRIBE SCHEMA ENVIRONMENT` and `EXPORT CAPSULE`.
pub fn as_of_clause(input: &str) -> VResult<'_, AsOf> {
    let (input, _) = ws(words(&["AS", "OF"])).parse(input)?;
    cut(alt((
        map(preceded(ws(word("SEQ")), cut(ws(scalar))), AsOf::Seq),
        map(preceded(ws(word("TX")), cut(ws(scalar))), AsOf::Tx),
        map(preceded(ws(word("TIME")), cut(ws(scalar))), AsOf::Time),
    )))
    .parse(input)
}

/// `projection_expression = aggregate_expression | expression`
///
/// Both spellings must resolve to one variable plus a path: a projection names
/// a column, and an arbitrary expression has no column to name.
fn find_expression(input: &str) -> VResult<'_, FindExpression> {
    if let Ok((rest, (func, distinct, var))) = aggregate_call(input) {
        return Ok((
            rest,
            FindExpression::Aggregation {
                func,
                var,
                distinct,
            },
        ));
    }
    map(dot_path_var, FindExpression::Variable).parse(input)
}

/// `order_item = projection_expression [ "ASC" | "DESC" ]`
fn order_item(input: &str) -> VResult<'_, OrderByItem> {
    let (input, (variable, aggregation)) = alt((
        map(aggregate_call, |(func, _, var)| (var, Some(func))),
        map(dot_path_var, |var| (var, None)),
    ))
    .parse(input)?;
    let (input, direction) = opt(ws(alt((
        value(OrderDirection::Asc, word("ASC")),
        value(OrderDirection::Desc, word("DESC")),
    ))))
    .parse(input)?;

    Ok((
        input,
        OrderByItem {
            variable,
            direction: direction.unwrap_or_default(),
            aggregation,
        },
    ))
}

/// `aggregate_expression = aggregate_name "(" [ "DISTINCT" ] expression ")"`
fn aggregate_call(input: &str) -> VResult<'_, (AggregationFunction, bool, DotPathVar)> {
    let (rest, name) = identifier(input)?;
    let func = match name.to_ascii_uppercase().as_str() {
        "COUNT" => AggregationFunction::Count,
        "SUM" => AggregationFunction::Sum,
        "AVG" => AggregationFunction::Avg,
        "MIN" => AggregationFunction::Min,
        "MAX" => AggregationFunction::Max,
        _ => return fail(input, "an aggregate: COUNT, SUM, AVG, MIN or MAX"),
    };
    let (rest, (distinct, var)) = cut(parenthesized((
        map(opt(ws(word("DISTINCT"))), |d| d.is_some()),
        ws(dot_path_var),
    )))
    .parse(rest)?;
    Ok((rest, (func, distinct, var)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Scalar, WhereClause};

    fn kql(input: &str) -> KqlQuery {
        let (rest, query) =
            parse_kql_query(input).unwrap_or_else(|e| panic!("failed to parse:\n{input}\n{e}"));
        assert!(rest.trim().is_empty(), "unconsumed input {rest:?}");
        query
    }

    #[test]
    fn parses_the_full_query_skeleton() {
        let query = kql(r#"
            FIND(?drug.name, COUNT(DISTINCT ?trial))
            WHERE {
                ?drug CONCEPT {type: "Drug"}
                (?drug, "studied_in", ?trial)
                FILTER(?drug.attributes.risk_level < 3)
            }
            AS OF SEQ 4200
            FOR TIME "2026-01-01T00:00:00Z"
            WITH EPISTEMIC { explain: "summary" }
            ORDER BY COUNT(?trial) DESC, ?drug.name
            LIMIT 10
            CURSOR :page
            "#);

        assert_eq!(query.find_clause.expressions.len(), 2);
        assert!(matches!(
            query.find_clause.expressions[1],
            FindExpression::Aggregation {
                func: AggregationFunction::Count,
                distinct: true,
                ..
            }
        ));
        assert!(matches!(query.as_of, Some(AsOf::Seq(_))));
        assert!(query.for_time.is_some());
        assert!(query.epistemic.is_some());

        let order = query.order_by.expect("ORDER BY");
        assert_eq!(order[0].direction, OrderDirection::Desc);
        assert_eq!(order[0].aggregation, Some(AggregationFunction::Count));
        // An unwritten direction is ascending.
        assert_eq!(order[1].direction, OrderDirection::Asc);
        assert_eq!(order[1].aggregation, None);

        assert!(matches!(query.cursor, Some(Scalar::Param(_))));
    }

    #[test]
    fn as_of_and_for_time_are_independent_axes() {
        // Spec §48.3: history basis and world-valid time never imply each other.
        let query = kql(r#"FIND(?x) WHERE { ?x {type: "T"} } AS OF TX :tx FOR TIME :t"#);
        assert!(matches!(query.as_of, Some(AsOf::Tx(_))));
        assert!(query.for_time.is_some());

        let only_time = kql(r#"FIND(?x) WHERE { ?x {type: "T"} } FOR TIME :t"#);
        assert!(only_time.as_of.is_none());
        assert!(only_time.for_time.is_some());
    }

    #[test]
    fn keywords_are_case_insensitive() {
        let query = kql(r#"find(?x) where { ?x {type: "T"} } limit 5"#);
        assert_eq!(query.where_clauses.len(), 1);
        assert!(query.limit.is_some());
    }

    #[test]
    fn a_projection_must_name_a_column() {
        assert!(parse_kql_query(r#"FIND("literal") WHERE { ?x {a: 1} }"#).is_err());
        assert!(parse_kql_query(r#"FIND(1 + 1) WHERE { ?x {a: 1} }"#).is_err());
        assert!(parse_kql_query(r#"FIND() WHERE { ?x {a: 1} }"#).is_err());
    }

    #[test]
    fn union_is_a_block_not_a_binary_operator() {
        let query = kql(r#"FIND(?x) WHERE { ?x {type: "A"} UNION { ?x {type: "B"} } }"#);
        assert_eq!(query.where_clauses.len(), 2);
        assert!(matches!(query.where_clauses[1], WhereClause::Union(_)));
    }

    #[test]
    fn comments_are_ignored_between_clauses() {
        let query = kql(r#"
            // pick the drug
            FIND(?x) // just the binding
            WHERE {
                // any drug will do
                ?x {type: "Drug"}
            }
            LIMIT 1
            "#);
        assert_eq!(query.where_clauses.len(), 1);
        assert!(query.limit.is_some());
    }
}
