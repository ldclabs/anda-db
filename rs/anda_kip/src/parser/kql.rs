use nom::{
    Parser,
    branch::alt,
    bytes::complete::tag,
    character::complete::{char, multispace1},
    combinator::{cut, map, map_res, opt},
    error::context,
    multi::{fold, many1, separated_list1},
    sequence::{delimited, pair, preceded, separated_pair, terminated},
};

use super::common::*;
use crate::ast::*;

// --- Top Level KQL Parser ---

pub fn parse_kql_query(input: &str) -> VResult<'_, KqlQuery> {
    context(
        "KQL query: FIND(...) WHERE { ... } [ORDER BY ...] [LIMIT N] [CURSOR \"...\"]",
        map(
            (
                ws(parse_find_clause),
                cut(ws(parse_where_block)),
                opt(ws(parse_order_by_clause)),
                opt(ws(parse_limit_clause)),
                opt(ws(parse_cursor_clause)),
            ),
            |(find_clause, where_clauses, order_by, limit, cursor)| KqlQuery {
                find_clause,
                where_clauses,
                order_by,
                limit,
                cursor,
            },
        ),
    )
    .parse(input)
}

// --- FIND Clause ---

fn parse_find_clause(input: &str) -> VResult<'_, FindClause> {
    context(
        "FIND( ... )",
        map(
            preceded(
                ws(tag("FIND")),
                parenthesized_block(separated_list1(ws(char(',')), parse_find_expression)),
            ),
            |expressions| FindClause { expressions },
        ),
    )
    .parse(input)
}

fn parse_find_expression(input: &str) -> VResult<'_, FindExpression> {
    context(
        "FIND expression: ?variable or COUNT(?var), SUM(?var), AVG(?var), MIN(?var), MAX(?var)",
        alt((parse_aggregation_expression, parse_find_variable)),
    )
    .parse(input)
}

fn parse_find_variable(input: &str) -> VResult<'_, FindExpression> {
    map(dot_path_var, FindExpression::Variable).parse(input)
}

fn parse_aggregation_expression(input: &str) -> VResult<'_, FindExpression> {
    context(
        "KQL aggregation expression",
        map(
            (
                parse_aggregation_function,
                parenthesized_block((opt(terminated(tag("DISTINCT"), multispace1)), dot_path_var)),
            ),
            |(func, (distinct, var))| FindExpression::Aggregation {
                func,
                var,
                distinct: distinct.is_some(),
            },
        ),
    )
    .parse(input)
}

fn parse_aggregation_function(input: &str) -> VResult<'_, AggregationFunction> {
    alt((
        map(tag("COUNT"), |_| AggregationFunction::Count),
        map(tag("SUM"), |_| AggregationFunction::Sum),
        map(tag("AVG"), |_| AggregationFunction::Avg),
        map(tag("MIN"), |_| AggregationFunction::Min),
        map(tag("MAX"), |_| AggregationFunction::Max),
    ))
    .parse(input)
}

// --- WHERE Clause ---

pub fn parse_where_block(input: &str) -> VResult<'_, Vec<WhereClause>> {
    context(
        "WHERE { ... }",
        preceded(ws(tag("WHERE")), parse_where_group),
    )
    .parse(input)
}

fn parse_where_group(input: &str) -> VResult<'_, Vec<WhereClause>> {
    braced_block(many1(ws(parse_single_where_clause))).parse(input)
}

fn parse_single_where_clause(input: &str) -> VResult<'_, WhereClause> {
    context(
        "WHERE clause item: ?var {matcher} | (?s, \"pred\", ?o) | FILTER(...) | OPTIONAL {...} | NOT {...} | UNION {...}",
        alt((
            map(parse_optional_clause, WhereClause::Optional),
            map(parse_not_clause, WhereClause::Not),
            map(parse_union_expression, WhereClause::Union),
            map(parse_filter_clause, WhereClause::Filter),
            map(parse_concept_clause, WhereClause::Concept),
            map(parse_prop_clause, WhereClause::Proposition),
        )),
    )
    .parse(input)
}

// --- WHERE Clause Sub-parsers ---

pub fn parse_concept_matcher(input: &str) -> VResult<'_, ConceptMatcher> {
    context(
        "KQL concept matcher",
        map_res(
            braced_block(cut(separated_list1(ws(char(',')), key_value_pair))),
            ConceptMatcher::try_from,
        ),
    )
    .parse(input)
}

fn parse_concept_clause(input: &str) -> VResult<'_, ConceptClause> {
    context(
        "KQL concept clause",
        map((variable, parse_concept_matcher), |(variable, matcher)| {
            ConceptClause { variable, matcher }
        }),
    )
    .parse(input)
}

pub fn parse_target_term(input: &str) -> VResult<'_, TargetTerm> {
    context(
        "KQL target term",
        alt((
            map(parse_concept_matcher, TargetTerm::Concept),
            map(parse_prop_mather, |p| TargetTerm::Proposition(Box::new(p))),
            map(variable, TargetTerm::Variable),
        )),
    )
    .parse(input)
}

fn parse_predicate_path(input: &str) -> VResult<'_, PredTerm> {
    let (input, first) = parse_multi_hop_predicate(input)?;

    match first {
        PredTerm::Literal(predicate) if input.contains('|') => map(
            fold(
                0..,
                preceded(ws(char('|')), quoted_string),
                move || vec![predicate.clone()],
                |mut acc, next| {
                    acc.push(next);
                    acc
                },
            ),
            PredTerm::Alternative,
        )
        .parse(input),
        _ => Ok((input, first)),
    }
}

fn parse_multi_hop_predicate(input: &str) -> VResult<'_, PredTerm> {
    alt((
        map(
            (quoted_string, parse_predicate_quantifier),
            |(predicate, (min, max))| PredTerm::MultiHop {
                predicate,
                min,
                max,
            },
        ),
        map(quoted_string, PredTerm::Literal),
    ))
    .parse(input)
}

// parse: {m,n} | {m,} | {m}
fn parse_predicate_quantifier(input: &str) -> VResult<'_, (u16, Option<u16>)> {
    braced_block(ws(cut(alt((
        // {m,n} 格式
        map_res(
            (
                nom::character::complete::u16,
                ws(char(',')),
                nom::character::complete::u16,
            ),
            |(min, _, max)| {
                if max >= min {
                    Ok((min, Some(max)))
                } else {
                    Err(format!(
                        "invalid multi-hop predicate: min {min} cannot be greater than max {max}"
                    ))
                }
            },
        ),
        // {m,} 格式（无上限）
        map(
            (nom::character::complete::u16, ws(char(','))),
            |(min, _)| (min, None),
        ),
        // {m} 格式（精确匹配）
        map(nom::character::complete::u16, |min| (min, Some(min))),
    )))))
    .parse(input)
}

fn parse_pred_term(input: &str) -> VResult<'_, PredTerm> {
    context(
        "KQL predicate term",
        alt((map(variable, PredTerm::Variable), parse_predicate_path)),
    )
    .parse(input)
}

pub fn parse_prop_mather(input: &str) -> VResult<'_, PropositionMatcher> {
    context(
        "KQL proposition matcher",
        parenthesized_block(cut(alt((
            map(
                separated_pair(ws(tag("id")), ws(char(':')), ws(quoted_string)),
                |(_, id)| PropositionMatcher::ID(id),
            ),
            map(
                (
                    ws(parse_target_term),
                    ws(char(',')),
                    ws(parse_pred_term),
                    ws(char(',')),
                    ws(parse_target_term),
                ),
                |(subject, _, predicate, _, object)| PropositionMatcher::Object {
                    subject,
                    predicate,
                    object,
                },
            ),
        )))),
    )
    .parse(input)
}

fn parse_prop_clause(input: &str) -> VResult<'_, PropositionClause> {
    context(
        "KQL proposition clause",
        map((opt(variable), parse_prop_mather), |(variable, matcher)| {
            PropositionClause { matcher, variable }
        }),
    )
    .parse(input)
}

fn parse_filter_clause(input: &str) -> VResult<'_, FilterClause> {
    context(
        "KQL FILTER clause",
        map(
            preceded(
                ws(tag("FILTER")),
                cut(parenthesized_block(parse_filter_expression)),
            ),
            |expression| FilterClause { expression },
        ),
    )
    .parse(input)
}

fn parse_filter_expression(input: &str) -> VResult<'_, FilterExpression> {
    parse_logical_or_expression(input)
}

// 解析逻辑 OR 表达式（最低优先级）
fn parse_logical_or_expression(input: &str) -> VResult<'_, FilterExpression> {
    let (input, left) = parse_logical_and_expression(input)?;

    fold(
        0..,
        preceded(ws(tag("||")), parse_logical_and_expression),
        move || left.clone(),
        |acc, right| FilterExpression::Logical {
            left: Box::new(acc),
            operator: LogicalOperator::Or,
            right: Box::new(right),
        },
    )
    .parse(input)
}

// 解析逻辑 AND 表达式
fn parse_logical_and_expression(input: &str) -> VResult<'_, FilterExpression> {
    let (input, left) = parse_unary_expression(input)?;

    fold(
        0..,
        preceded(ws(tag("&&")), parse_unary_expression),
        move || left.clone(),
        |acc, right| FilterExpression::Logical {
            left: Box::new(acc),
            operator: LogicalOperator::And,
            right: Box::new(right),
        },
    )
    .parse(input)
}

// 解析一元表达式（NOT）
fn parse_unary_expression(input: &str) -> VResult<'_, FilterExpression> {
    alt((
        map(preceded(ws(char('!')), parse_primary_expression), |expr| {
            FilterExpression::Not(Box::new(expr))
        }),
        parse_primary_expression,
    ))
    .parse(input)
}

// 解析基本表达式（比较、函数、括号）
fn parse_primary_expression(input: &str) -> VResult<'_, FilterExpression> {
    alt((
        // 括号表达式
        parenthesized_block(parse_filter_expression),
        // 函数调用
        parse_function_expression,
        // 比较表达式
        parse_comparison_expression,
    ))
    .parse(input)
}

// 解析比较表达式
fn parse_comparison_expression(input: &str) -> VResult<'_, FilterExpression> {
    context(
        "FILTER comparison: ?var == value, ?var != value, ?var < value, etc.",
        map(
            (
                parse_filter_operand,
                ws(parse_comparison_operator),
                cut(parse_filter_operand),
            ),
            |(left, operator, right)| FilterExpression::Comparison {
                left,
                operator,
                right,
            },
        ),
    )
    .parse(input)
}

// 解析函数表达式
fn parse_function_expression(input: &str) -> VResult<'_, FilterExpression> {
    map(
        (
            parse_filter_function,
            parenthesized_block(separated_list1(ws(char(',')), parse_filter_operand)),
        ),
        |(func, args)| FilterExpression::Function { func, args },
    )
    .parse(input)
}

// 解析过滤器操作数
fn parse_filter_operand(input: &str) -> VResult<'_, FilterOperand> {
    context(
        "FILTER operand: ?variable.path, literal value, or [value, ...] list",
        alt((
            map(dot_path_var, FilterOperand::Variable),
            map(
                delimited(
                    ws(char('[')),
                    separated_list1(ws(char(',')), kip_value),
                    ws(char(']')),
                ),
                FilterOperand::List,
            ),
            map(kip_value, FilterOperand::Literal),
        )),
    )
    .parse(input)
}

// 解析比较运算符
fn parse_comparison_operator(input: &str) -> VResult<'_, ComparisonOperator> {
    alt((
        map(tag("=="), |_| ComparisonOperator::Equal),
        map(tag("!="), |_| ComparisonOperator::NotEqual),
        map(tag("<="), |_| ComparisonOperator::LessEqual),
        map(tag(">="), |_| ComparisonOperator::GreaterEqual),
        map(tag("<"), |_| ComparisonOperator::LessThan),
        map(tag(">"), |_| ComparisonOperator::GreaterThan),
    ))
    .parse(input)
}

// 解析过滤器函数
fn parse_filter_function(input: &str) -> VResult<'_, FilterFunction> {
    alt((
        map(tag("CONTAINS"), |_| FilterFunction::Contains),
        map(tag("STARTS_WITH"), |_| FilterFunction::StartsWith),
        map(tag("ENDS_WITH"), |_| FilterFunction::EndsWith),
        map(tag("REGEX"), |_| FilterFunction::Regex),
        map(tag("IS_NOT_NULL"), |_| FilterFunction::IsNotNull),
        map(tag("IS_NULL"), |_| FilterFunction::IsNull),
        map(tag("IN"), |_| FilterFunction::In),
    ))
    .parse(input)
}

fn parse_optional_clause(input: &str) -> VResult<'_, Vec<WhereClause>> {
    context(
        "KQL OPTIONAL clause",
        preceded(
            ws(tag("OPTIONAL")),
            cut(braced_block(many1(ws(parse_single_where_clause)))),
        ),
    )
    .parse(input)
}

fn parse_not_clause(input: &str) -> VResult<'_, Vec<WhereClause>> {
    context(
        "KQL NOT clause",
        preceded(
            ws(tag("NOT")),
            cut(braced_block(many1(ws(parse_single_where_clause)))),
        ),
    )
    .parse(input)
}

fn parse_union_expression(input: &str) -> VResult<'_, Vec<WhereClause>> {
    context(
        "KQL UNION clause",
        preceded(
            ws(tag("UNION")),
            cut(braced_block(many1(ws(parse_single_where_clause)))),
        ),
    )
    .parse(input)
}

// --- Solution Modifiers ---

fn parse_order_by_clause(input: &str) -> VResult<'_, Vec<OrderByCondition>> {
    context(
        "ORDER BY ?variable [ASC|DESC], ...",
        preceded(
            ws(tag("ORDER ")),
            preceded(
                ws(tag("BY ")),
                cut(separated_list1(ws(char(',')), parse_order_by_condition)),
            ),
        ),
    )
    .parse(input)
}

fn parse_order_by_condition(input: &str) -> VResult<'_, OrderByCondition> {
    map(
        pair(
            dot_path_var,
            opt(alt((
                map(ws(tag("ASC")), |_| OrderDirection::Asc),
                map(ws(tag("DESC")), |_| OrderDirection::Desc),
            ))),
        ),
        |(variable, direction)| OrderByCondition {
            variable,
            direction: direction.unwrap_or(OrderDirection::Asc),
        },
    )
    .parse(input)
}

pub fn parse_limit_clause(input: &str) -> VResult<'_, usize> {
    context(
        "KQL LIMIT clause",
        preceded(
            ws(tag("LIMIT ")),
            map(nom::character::complete::usize, |n| n),
        ),
    )
    .parse(input)
}

pub fn parse_cursor_clause(input: &str) -> VResult<'_, String> {
    preceded(ws(tag("CURSOR ")), quoted_string).parse(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_find_query() {
        let input = r#"
            FIND(?drug.name)
            WHERE {
                ?drug {type: "Drug"}
            }
        "#;

        let result = parse_kql_query(input);
        assert!(result.is_ok());

        let (_, query) = result.unwrap();
        assert_eq!(query.find_clause.expressions.len(), 1);
        match &query.find_clause.expressions[0] {
            FindExpression::Variable(var) => {
                assert_eq!(var.var, "drug");
                assert_eq!(var.path, vec!["name".to_string()]);
            }
            _ => panic!("Expected variable expression"),
        }
        assert_eq!(query.where_clauses.len(), 1);
    }

    #[test]
    fn test_parse_aggregation_find() {
        let input = r#"
            FIND(?drug_class, COUNT(?drug))
            WHERE {
                ?drug {type: "Drug"}
                (?drug, "is_class_of", ?drug_class)
            }
        "#;

        let result = parse_kql_query(input);
        assert!(result.is_ok());

        let (_, query) = result.unwrap();
        assert_eq!(query.find_clause.expressions.len(), 2);

        match &query.find_clause.expressions[0] {
            FindExpression::Variable(var) => {
                assert_eq!(var.var, "drug_class");
                assert!(var.path.is_empty());
            }
            _ => panic!("Expected variable expression"),
        }

        match &query.find_clause.expressions[1] {
            FindExpression::Aggregation {
                func,
                var,
                distinct,
            } => {
                assert_eq!(*func, AggregationFunction::Count);
                assert_eq!(var.var, "drug");
                assert!(var.path.is_empty());
                assert!(!distinct);
            }
            _ => panic!("Expected aggregation expression"),
        }
    }

    #[test]
    fn test_parse_aggregation_with_distinct() {
        let input = r#"
            FIND(COUNT(DISTINCT ?symptom))
            WHERE {
                (?drug, "treats", ?symptom)
            }
        "#;

        let result = parse_kql_query(input);
        assert!(result.is_ok());

        let (_, query) = result.unwrap();
        match &query.find_clause.expressions[0] {
            FindExpression::Aggregation {
                func,
                var,
                distinct,
            } => {
                assert_eq!(*func, AggregationFunction::Count);
                assert_eq!(var.var, "symptom");
                assert!(var.path.is_empty());
                assert!(*distinct);
            }
            _ => panic!("Expected aggregation expression"),
        }
    }

    #[test]
    fn test_parse_all_aggregation_functions() {
        let functions = vec![
            ("COUNT(?var)", AggregationFunction::Count),
            ("SUM(?var)", AggregationFunction::Sum),
            ("AVG(?var)", AggregationFunction::Avg),
            ("MIN(?var)", AggregationFunction::Min),
            ("MAX(?var)", AggregationFunction::Max),
        ];

        for (func_str, expected_func) in functions {
            let input = format!("FIND({func_str}) WHERE {{ ?x{{type: \"Test\"}} }}");
            let result = parse_kql_query(&input);
            assert!(result.is_ok(), "Failed to parse: {func_str}");

            let (_, query) = result.unwrap();
            match &query.find_clause.expressions[0] {
                FindExpression::Aggregation { func, .. } => {
                    assert_eq!(*func, expected_func);
                }
                _ => panic!("Expected aggregation for: {func_str}"),
            }
        }
    }

    #[test]
    fn test_parse_concept_clause() {
        let input = r#"
            FIND(?drug.name)
            WHERE {
                ?drug {type: "Drug", name: "Aspirin"}
            }
        "#;

        let result = parse_kql_query(input);
        assert!(result.is_ok());

        let (_, query) = result.unwrap();
        match &query.where_clauses[0] {
            WhereClause::Concept(clause) => {
                assert_eq!(clause.variable, "drug".to_string());
                assert_eq!(
                    clause.matcher,
                    ConceptMatcher::Object {
                        r#type: "Drug".to_string(),
                        name: "Aspirin".to_string(),
                    }
                );
            }
            _ => panic!("Expected concept clause"),
        }
    }

    #[test]
    fn test_parse_proposition_clause() {
        let input = r#"
            FIND(?drug.name, ?symptom.name)
            WHERE {
                (?drug, "treats", ?symptom)
            }
        "#;

        let result = parse_kql_query(input);
        assert!(result.is_ok());

        let (_, query) = result.unwrap();
        match &query.where_clauses[0] {
            WhereClause::Proposition(clause) => {
                assert_eq!(clause.variable, None);
                assert_eq!(
                    clause.matcher,
                    PropositionMatcher::Object {
                        subject: TargetTerm::Variable("drug".to_string()),
                        predicate: PredTerm::Literal("treats".to_string()),
                        object: TargetTerm::Variable("symptom".to_string()),
                    }
                );
            }
            _ => panic!("Expected proposition clause"),
        }
    }

    #[test]
    fn test_parse_proposition_with_concept_clause() {
        let input = r#"
            FIND(?drug.name)
            WHERE {
                (?drug, "treats", { type: "Symptom", name: "Headache" })
            }
        "#;

        let result = parse_kql_query(input);
        assert!(result.is_ok());

        let (_, query) = result.unwrap();
        match &query.where_clauses[0] {
            WhereClause::Proposition(clause) => {
                assert_eq!(clause.variable, None);
                assert_eq!(
                    clause.matcher,
                    PropositionMatcher::Object {
                        subject: TargetTerm::Variable("drug".to_string()),
                        predicate: PredTerm::Literal("treats".to_string()),
                        object: TargetTerm::Concept(ConceptMatcher::Object {
                            r#type: "Symptom".to_string(),
                            name: "Headache".to_string(),
                        }),
                    }
                );
            }
            _ => panic!("Expected proposition clause"),
        }
    }

    #[test]
    fn test_parse_proposition_with_id_matcher() {
        let input = r#"
            FIND(?link)
            WHERE {
                ?link (id: "P:12345:connect")
            }
        "#;

        let result = parse_kql_query(input);
        assert!(result.is_ok());

        let (_, query) = result.unwrap();
        assert_eq!(query.find_clause.expressions.len(), 1);
        assert_eq!(query.where_clauses.len(), 1);

        match &query.where_clauses[0] {
            WhereClause::Proposition(clause) => {
                assert_eq!(clause.variable, Some("link".to_string()));
                assert_eq!(
                    clause.matcher,
                    PropositionMatcher::ID("P:12345:connect".to_string())
                );
            }
            _ => panic!("Expected proposition clause"),
        }
    }

    #[test]
    fn test_parse_nested_proposition() {
        let input = r#"
            FIND(?paper.doi, ?drug.name)
            WHERE {
                ({type: "Person", name: "张三"}, "stated", (?paper, "cites_as_evidence", (?drug, "treats", ?symptom)))
            }
        "#;

        let result = parse_kql_query(input);
        assert!(result.is_ok());

        let (_, query) = result.unwrap();
        match &query.where_clauses[0] {
            WhereClause::Proposition(clause) => {
                assert_eq!(clause.variable, None);
                assert_eq!(
                    clause.matcher,
                    PropositionMatcher::Object {
                        subject: TargetTerm::Concept(ConceptMatcher::Object {
                            r#type: "Person".to_string(),
                            name: "张三".to_string(),
                        }),
                        predicate: PredTerm::Literal("stated".to_string()),
                        object: TargetTerm::Proposition(Box::new(PropositionMatcher::Object {
                            subject: TargetTerm::Variable("paper".to_string()),
                            predicate: PredTerm::Literal("cites_as_evidence".to_string()),
                            object: TargetTerm::Proposition(Box::new(PropositionMatcher::Object {
                                subject: TargetTerm::Variable("drug".to_string()),
                                predicate: PredTerm::Literal("treats".to_string()),
                                object: TargetTerm::Variable("symptom".to_string()),
                            })),
                        })),
                    }
                );
            }
            _ => panic!("Expected proposition clause"),
        }
    }

    #[test]
    fn test_parse_quantified_predicate_path() {
        let test_cases = vec![
            // {2,5} - 2到5跳
            (r#"(?a, "follows"{2,5}, ?b)"#, 2, Some(5)),
            // {3,} - 至少3跳
            (r#"(?a, "follows"{3,}, ?b)"#, 3, None),
            // {4} - 精确4跳
            (r#"(?a, "follows"{4}, ?b)"#, 4, Some(4)),
        ];

        for (input, expected_min, expected_max) in test_cases {
            let result = parse_prop_mather(input);
            assert!(result.is_ok(), "Failed to parse: {}", input);

            let (_, matcher) = result.unwrap();
            match &matcher {
                PropositionMatcher::Object {
                    subject,
                    predicate,
                    object,
                } => {
                    assert_eq!(subject, &TargetTerm::Variable("a".to_string()));
                    assert_eq!(object, &TargetTerm::Variable("b".to_string()));
                    match &predicate {
                        PredTerm::MultiHop {
                            predicate,
                            min,
                            max,
                        } => {
                            assert_eq!(predicate, "follows");
                            assert_eq!(*min, expected_min);
                            assert_eq!(*max, expected_max);
                        }
                        _ => panic!("Expected proposition for object"),
                    }
                }
                _ => panic!("Expected quantified predicate path for: {}", input),
            }
        }
    }

    #[test]
    fn test_parse_alternative_predicate_path() {
        let input = r#"(?a, "follows" | "connected_to"| "mark", ?b)"#;
        let result = parse_prop_mather(input);
        assert!(result.is_ok());

        let (_, matcher) = result.unwrap();
        match matcher {
            PropositionMatcher::Object {
                predicate: PredTerm::Alternative(paths),
                ..
            } => {
                assert_eq!(paths.len(), 3);
                assert_eq!(paths[0], "follows");
                assert_eq!(paths[1], "connected_to");
                assert_eq!(paths[2], "mark");
            }
            _ => panic!("Expected alternative predicate path"),
        }
    }

    #[test]
    fn test_parse_predicate_path_error_cases() {
        let invalid_inputs = vec![
            r#"(?a, "follows"{}, ?b)"#,    // 空量词
            r#"(?a, "follows"{a,b}, ?b)"#, // 非数字量词
            r#"(?a, "follows"{5,2}, ?b)"#, // min > max
            r#"(?a, "follows"{ , }, ?b)"#, // 缺少数字
            r#"(?a, | "follows", ?b)"#,    // 以 | 开始
            r#"(?a, "follows" |, ?b)"#,    // 以 | 结束
        ];

        for input in invalid_inputs {
            let result = parse_prop_mather(input);
            assert!(result.is_err(), "Should fail to parse: {}", input);
        }
    }

    #[test]
    fn test_parse_simple_comparison_filter() {
        let input = "FILTER(?risk < 3)";
        let result = parse_filter_clause(input);
        assert!(result.is_ok());

        let (_, filter) = result.unwrap();
        match filter.expression {
            FilterExpression::Comparison {
                left,
                operator,
                right,
            } => {
                match left {
                    FilterOperand::Variable(var) => assert_eq!(var.var, "risk"),
                    _ => panic!("Expected variable operand"),
                }
                assert_eq!(operator, ComparisonOperator::LessThan);
                match right {
                    FilterOperand::Literal(Value::Number(_)) => {}
                    _ => panic!("Expected number literal"),
                }
            }
            _ => panic!("Expected comparison expression"),
        }
    }

    #[test]
    fn test_parse_all_comparison_operators() {
        let test_cases = vec![
            ("FILTER(?a == ?b)", ComparisonOperator::Equal),
            ("FILTER(?a != ?b)", ComparisonOperator::NotEqual),
            ("FILTER(?a < ?b)", ComparisonOperator::LessThan),
            ("FILTER(?a > ?b)", ComparisonOperator::GreaterThan),
            ("FILTER(?a <= ?b)", ComparisonOperator::LessEqual),
            ("FILTER(?a >= ?b)", ComparisonOperator::GreaterEqual),
        ];

        for (input, expected_op) in test_cases {
            let result = parse_filter_clause(input);
            assert!(result.is_ok(), "Failed to parse: {input}");

            let (_, filter) = result.unwrap();
            match filter.expression {
                FilterExpression::Comparison { operator, .. } => {
                    assert_eq!(operator, expected_op);
                }
                _ => panic!("Expected comparison expression for: {input}"),
            }
        }
    }

    #[test]
    fn test_parse_logical_and_filter() {
        let input = "FILTER(?risk < 3 && ?score > 0.5)";
        let result = parse_filter_clause(input);
        assert!(result.is_ok());

        let (_, filter) = result.unwrap();
        match filter.expression {
            FilterExpression::Logical {
                left,
                operator,
                right,
            } => {
                assert_eq!(operator, LogicalOperator::And);
                match left.as_ref() {
                    FilterExpression::Comparison { operator, .. } => {
                        assert_eq!(*operator, ComparisonOperator::LessThan);
                    }
                    _ => panic!("Expected comparison in left side"),
                }
                match right.as_ref() {
                    FilterExpression::Comparison { operator, .. } => {
                        assert_eq!(*operator, ComparisonOperator::GreaterThan);
                    }
                    _ => panic!("Expected comparison in right side"),
                }
            }
            _ => panic!("Expected logical expression"),
        }
    }

    #[test]
    fn test_parse_logical_or_filter() {
        let input = "FILTER(?type == \"Drug\" || ?type == \"Medicine\")";
        let result = parse_filter_clause(input);
        assert!(result.is_ok());

        let (_, filter) = result.unwrap();
        match filter.expression {
            FilterExpression::Logical { operator, .. } => {
                assert_eq!(operator, LogicalOperator::Or);
            }
            _ => panic!("Expected logical OR expression"),
        }
    }

    #[test]
    fn test_parse_not_filter() {
        let input = "FILTER(!(?risk > 5))";
        let result = parse_filter_clause(input);
        assert!(result.is_ok());

        let (_, filter) = result.unwrap();
        match filter.expression {
            FilterExpression::Not(inner) => match inner.as_ref() {
                FilterExpression::Comparison { operator, .. } => {
                    assert_eq!(*operator, ComparisonOperator::GreaterThan);
                }
                _ => panic!("Expected comparison inside NOT"),
            },
            _ => panic!("Expected NOT expression"),
        }
    }

    #[test]
    fn test_parse_function_filter() {
        let test_cases = vec![
            (
                "FILTER(CONTAINS(?name, \"acid\"))",
                FilterFunction::Contains,
            ),
            (
                "FILTER(STARTS_WITH(?name, \"pre\"))",
                FilterFunction::StartsWith,
            ),
            (
                "FILTER(ENDS_WITH(?name, \"ine\"))",
                FilterFunction::EndsWith,
            ),
            ("FILTER(REGEX(?name, \"[A-Z]+\"))", FilterFunction::Regex),
        ];

        for (input, expected_func) in test_cases {
            let result = parse_filter_clause(input);
            assert!(result.is_ok(), "Failed to parse: {input}");

            let (_, filter) = result.unwrap();
            match filter.expression {
                FilterExpression::Function { func, args } => {
                    assert_eq!(func, expected_func);
                    assert_eq!(args.len(), 2);
                    match &args[0] {
                        FilterOperand::Variable(_) => {}
                        _ => panic!("Expected variable as first argument"),
                    }
                    match &args[1] {
                        FilterOperand::Literal(Value::String(_)) => {}
                        _ => panic!("Expected string literal as second argument"),
                    }
                }
                _ => panic!("Expected function expression for: {input}"),
            }
        }
    }

    #[test]
    fn test_parse_in_filter() {
        let input = r#"FILTER(IN(?status, ["active", "pending", "review"]))"#;
        let result = parse_filter_clause(input);
        assert!(result.is_ok(), "Failed to parse IN filter");

        let (_, filter) = result.unwrap();
        match filter.expression {
            FilterExpression::Function { func, args } => {
                assert_eq!(func, FilterFunction::In);
                assert_eq!(args.len(), 2);
                match &args[0] {
                    FilterOperand::Variable(var) => assert_eq!(var.var, "status"),
                    _ => panic!("Expected variable as first argument"),
                }
                match &args[1] {
                    FilterOperand::List(values) => {
                        assert_eq!(values.len(), 3);
                        assert_eq!(values[0], Value::String("active".to_string()));
                        assert_eq!(values[1], Value::String("pending".to_string()));
                        assert_eq!(values[2], Value::String("review".to_string()));
                    }
                    _ => panic!("Expected list as second argument"),
                }
            }
            _ => panic!("Expected function expression"),
        }
    }

    #[test]
    fn test_parse_in_filter_with_mixed_values() {
        let input = r#"FILTER(IN(?score, [1, 2, 3]))"#;
        let result = parse_filter_clause(input);
        assert!(result.is_ok(), "Failed to parse IN filter with numbers");

        let (_, filter) = result.unwrap();
        match filter.expression {
            FilterExpression::Function { func, args } => {
                assert_eq!(func, FilterFunction::In);
                assert_eq!(args.len(), 2);
                match &args[1] {
                    FilterOperand::List(values) => {
                        assert_eq!(values.len(), 3);
                    }
                    _ => panic!("Expected list as second argument"),
                }
            }
            _ => panic!("Expected function expression"),
        }
    }

    #[test]
    fn test_parse_is_null_filter() {
        let input = "FILTER(IS_NULL(?description))";
        let result = parse_filter_clause(input);
        assert!(result.is_ok(), "Failed to parse IS_NULL filter");

        let (_, filter) = result.unwrap();
        match filter.expression {
            FilterExpression::Function { func, args } => {
                assert_eq!(func, FilterFunction::IsNull);
                assert_eq!(args.len(), 1);
                match &args[0] {
                    FilterOperand::Variable(var) => assert_eq!(var.var, "description"),
                    _ => panic!("Expected variable as argument"),
                }
            }
            _ => panic!("Expected function expression"),
        }
    }

    #[test]
    fn test_parse_is_not_null_filter() {
        let input = "FILTER(IS_NOT_NULL(?drug.name))";
        let result = parse_filter_clause(input);
        assert!(result.is_ok(), "Failed to parse IS_NOT_NULL filter");

        let (_, filter) = result.unwrap();
        match filter.expression {
            FilterExpression::Function { func, args } => {
                assert_eq!(func, FilterFunction::IsNotNull);
                assert_eq!(args.len(), 1);
                match &args[0] {
                    FilterOperand::Variable(var) => {
                        assert_eq!(var.var, "drug");
                        assert_eq!(var.path, vec!["name".to_string()]);
                    }
                    _ => panic!("Expected variable as argument"),
                }
            }
            _ => panic!("Expected function expression"),
        }
    }

    #[test]
    fn test_parse_new_functions_in_complex_filter() {
        let input =
            r#"FILTER(IS_NOT_NULL(?drug.name) && IN(?drug.status, ["active", "approved"]))"#;
        let result = parse_filter_clause(input);
        assert!(
            result.is_ok(),
            "Failed to parse complex filter with new functions"
        );

        let (_, filter) = result.unwrap();
        match filter.expression {
            FilterExpression::Logical {
                operator,
                left,
                right,
            } => {
                assert_eq!(operator, LogicalOperator::And);
                match left.as_ref() {
                    FilterExpression::Function { func, .. } => {
                        assert_eq!(*func, FilterFunction::IsNotNull);
                    }
                    _ => panic!("Expected IS_NOT_NULL on left"),
                }
                match right.as_ref() {
                    FilterExpression::Function { func, .. } => {
                        assert_eq!(*func, FilterFunction::In);
                    }
                    _ => panic!("Expected IN on right"),
                }
            }
            _ => panic!("Expected logical AND expression"),
        }
    }

    #[test]
    fn test_parse_complex_logical_filter() {
        let input = "FILTER(?risk < 3 && (CONTAINS(?name, \"acid\") || ?score > 0.8))";
        let result = parse_filter_clause(input);
        assert!(result.is_ok());

        let (_, filter) = result.unwrap();
        match filter.expression {
            FilterExpression::Logical {
                left,
                operator,
                right,
            } => {
                assert_eq!(operator, LogicalOperator::And);
                // Left side should be a comparison
                match left.as_ref() {
                    FilterExpression::Comparison { .. } => {}
                    _ => panic!("Expected comparison on left side"),
                }
                // Right side should be a logical OR
                match right.as_ref() {
                    FilterExpression::Logical { operator, .. } => {
                        assert_eq!(*operator, LogicalOperator::Or);
                    }
                    _ => panic!("Expected logical OR on right side"),
                }
            }
            _ => panic!("Expected logical AND expression"),
        }
    }

    #[test]
    fn test_parse_filter_with_different_operand_types() {
        let input = "FILTER(?active == true && ?count != null && ?score >= 3.14)";
        let result = parse_filter_clause(input);
        assert!(result.is_ok());

        let (_, filter) = result.unwrap();
        // This tests that we can parse different value types (boolean, null, float)
        match filter.expression {
            FilterExpression::Logical { .. } => {}
            _ => panic!("Expected logical expression"),
        }
    }

    #[test]
    fn test_parse_operator_precedence() {
        // Test that AND has higher precedence than OR
        let input = "FILTER(?a == 1 || ?b == 2 && ?c == 3)";
        let result = parse_filter_clause(input);
        assert!(result.is_ok());

        let (_, filter) = result.unwrap();
        match filter.expression {
            FilterExpression::Logical {
                operator,
                left,
                right,
            } => {
                // Should be parsed as: (?a == 1) || (?b == 2 && ?c == 3)
                assert_eq!(operator, LogicalOperator::Or);
                match left.as_ref() {
                    FilterExpression::Comparison { .. } => {}
                    _ => panic!("Expected comparison on left"),
                }
                match right.as_ref() {
                    FilterExpression::Logical { operator, .. } => {
                        assert_eq!(*operator, LogicalOperator::And);
                    }
                    _ => panic!("Expected AND expression on right"),
                }
            }
            _ => panic!("Expected OR expression at top level"),
        }
    }

    #[test]
    fn test_parse_parentheses_override_precedence() {
        // Test that parentheses can override operator precedence
        let input = "FILTER((?a == 1 || ?b == 2) && ?c == 3)";
        let result = parse_filter_clause(input);
        assert!(result.is_ok());

        let (_, filter) = result.unwrap();
        match filter.expression {
            FilterExpression::Logical {
                operator,
                left,
                right,
            } => {
                // Should be parsed as: (?a == 1 || ?b == 2) && (?c == 3)
                assert_eq!(operator, LogicalOperator::And);
                match left.as_ref() {
                    FilterExpression::Logical { operator, .. } => {
                        assert_eq!(*operator, LogicalOperator::Or);
                    }
                    _ => panic!("Expected OR expression on left"),
                }
                match right.as_ref() {
                    FilterExpression::Comparison { .. } => {}
                    _ => panic!("Expected comparison on right"),
                }
            }
            _ => panic!("Expected AND expression at top level"),
        }
    }

    #[test]
    fn test_parse_filter_error_cases() {
        let invalid_inputs = vec![
            "FILTER()",                 // Empty filter
            "FILTER(?a <)",             // Incomplete comparison
            "FILTER(?a == && ?b)",      // Invalid logical expression
            "FILTER(UNKNOWN_FUNC(?a))", // Unknown function
            "FILTER(?a ===== ?b)",      // Invalid operator
            "FILTER(!)",                // NOT without expression
        ];

        for input in invalid_inputs {
            let result = parse_filter_clause(input);
            assert!(result.is_err(), "Should fail to parse: {input}");
        }
    }

    #[test]
    fn test_parse_filter_whitespace_handling() {
        let input = "FILTER  (  ?risk   <   3   &&   CONTAINS  (  ?name  ,  \"acid\"  )  )";
        let result = parse_filter_clause(input);
        assert!(result.is_ok());

        let (_, filter) = result.unwrap();
        match filter.expression {
            FilterExpression::Logical { operator, .. } => {
                assert_eq!(operator, LogicalOperator::And);
            }
            _ => panic!("Expected logical expression"),
        }
    }

    #[test]
    fn test_parse_filter_clause() {
        let input = r#"
            FIND(?drug.name)
            WHERE {
                ?drug {type: "Drug"}
                FILTER(?drug.attributes.risk_level < 3)
            }
        "#;

        let result = parse_kql_query(input);
        assert!(result.is_ok());

        let (_, query) = result.unwrap();
        match &query.where_clauses[1] {
            WhereClause::Filter(filter) => match &filter.expression {
                FilterExpression::Comparison {
                    left,
                    operator,
                    right,
                } => {
                    match left {
                        FilterOperand::Variable(var) => {
                            assert_eq!(var.var, "drug");
                            assert_eq!(
                                var.path,
                                vec!["attributes".to_string(), "risk_level".to_string()]
                            );
                        }
                        _ => panic!("Expected variable operand"),
                    }
                    assert_eq!(*operator, ComparisonOperator::LessThan);
                    match right {
                        FilterOperand::Literal(Value::Number(_)) => {}
                        _ => panic!("Expected number literal"),
                    }
                }
                _ => panic!("Expected comparison expression"),
            },
            _ => panic!("Expected filter clause"),
        }
    }

    #[test]
    fn test_parse_complex_filter() {
        let input = r#"
        FIND(?drug.name)
        WHERE {
            ?drug {type: "Drug"}
            FILTER(CONTAINS(?drug.name, "acid") && ?drug.attributes.risk_level < 3)
        }
    "#;

        let result = parse_kql_query(input);
        assert!(result.is_ok());

        let (_, query) = result.unwrap();
        match &query.where_clauses[1] {
            WhereClause::Filter(filter) => {
                // 检查这是一个逻辑 AND 表达式
                match &filter.expression {
                    FilterExpression::Logical {
                        left,
                        operator,
                        right,
                    } => {
                        assert_eq!(*operator, LogicalOperator::And);

                        // 左边应该是 CONTAINS 函数
                        match left.as_ref() {
                            FilterExpression::Function { func, args } => {
                                assert_eq!(*func, FilterFunction::Contains);
                                assert_eq!(args.len(), 2);
                                match &args[0] {
                                    FilterOperand::Variable(var) => {
                                        assert_eq!(var.var, "drug");
                                        assert_eq!(var.path, vec!["name".to_string()]);
                                    }
                                    _ => panic!("Expected variable as first argument"),
                                }
                                match &args[1] {
                                    FilterOperand::Literal(Value::String(s)) => {
                                        assert_eq!(s, "acid")
                                    }
                                    _ => panic!("Expected string literal as second argument"),
                                }
                            }
                            _ => panic!("Expected function expression on left side"),
                        }

                        // 右边应该是比较表达式
                        match right.as_ref() {
                            FilterExpression::Comparison {
                                left,
                                operator,
                                right,
                            } => {
                                match left {
                                    FilterOperand::Variable(var) => {
                                        assert_eq!(var.var, "drug");
                                        assert_eq!(
                                            var.path,
                                            vec![
                                                "attributes".to_string(),
                                                "risk_level".to_string()
                                            ]
                                        );
                                    }
                                    _ => panic!("Expected variable operand"),
                                }
                                assert_eq!(*operator, ComparisonOperator::LessThan);
                                match right {
                                    FilterOperand::Literal(Value::Number(_)) => {}
                                    _ => panic!("Expected number literal"),
                                }
                            }
                            _ => panic!("Expected comparison expression on right side"),
                        }
                    }
                    _ => panic!("Expected logical AND expression"),
                }
            }
            _ => panic!("Expected filter clause"),
        }
    }

    #[test]
    fn test_parse_optional_clause() {
        let input = r#"
            FIND(?drug.name, ?side_effect.name)
            WHERE {
                ?drug {type: "Drug" }
                OPTIONAL {
                    (?drug, "has_side_effect", ?side_effect)
                }
            }
        "#;

        let result = parse_kql_query(input);
        assert!(result.is_ok());

        let (_, query) = result.unwrap();
        match &query.where_clauses[1] {
            WhereClause::Optional(clauses) => {
                assert_eq!(clauses.len(), 1);
                match &clauses[0] {
                    WhereClause::Proposition(clause) => match &clause.matcher {
                        PropositionMatcher::Object {
                            subject,
                            predicate,
                            object,
                        } => {
                            assert_eq!(subject, &TargetTerm::Variable("drug".to_string()));
                            assert_eq!(
                                predicate,
                                &PredTerm::Literal("has_side_effect".to_string())
                            );
                            assert_eq!(object, &TargetTerm::Variable("side_effect".to_string()));
                        }
                        _ => panic!("Expected object matcher"),
                    },
                    _ => panic!("Expected proposition in optional"),
                }
            }
            _ => panic!("Expected optional clause"),
        }
    }

    #[test]
    fn test_parse_not_clause() {
        let input = r#"
            FIND(?drug.name)
            WHERE {
                ?drug {type: "Drug"}
                NOT {
                    (?drug, "is_class_of", {name: "NSAID"})
                }
            }
        "#;

        let result = parse_kql_query(input);
        assert!(result.is_ok());

        let (_, query) = result.unwrap();
        match &query.where_clauses[1] {
            WhereClause::Not(clauses) => {
                assert_eq!(clauses.len(), 1);
                match &clauses[0] {
                    WhereClause::Proposition(clause) => {
                        assert_eq!(
                            clause.matcher,
                            PropositionMatcher::Object {
                                subject: TargetTerm::Variable("drug".to_string()),
                                predicate: PredTerm::Literal("is_class_of".to_string()),
                                object: TargetTerm::Concept(ConceptMatcher::Name(
                                    "NSAID".to_string()
                                )),
                            }
                        );
                    }
                    _ => panic!("Expected proposition in NOT clause"),
                }
            }
            _ => panic!("Expected NOT clause"),
        }
    }

    #[test]
    fn test_parse_union_clause() {
        let input = r#"
            FIND(?drug.name)
            WHERE {
                ?headache {name: "Headache"}
                (?drug, "treats", ?headache)

                UNION {
                    (?drug, "treats", {name: "Fever"})
                }
            }
        "#;

        let result = parse_kql_query(input);
        assert!(result.is_ok());

        let (_, query) = result.unwrap();
        assert_eq!(query.where_clauses.len(), 3);
        match &query.where_clauses[2] {
            WhereClause::Union(clauses) => {
                assert_eq!(clauses.len(), 1);
                match &clauses[0] {
                    WhereClause::Proposition(clause) => {
                        assert_eq!(
                            clause.matcher,
                            PropositionMatcher::Object {
                                subject: TargetTerm::Variable("drug".to_string()),
                                predicate: PredTerm::Literal("treats".to_string()),
                                object: TargetTerm::Concept(ConceptMatcher::Name(
                                    "Fever".to_string()
                                ))
                            }
                        );
                    }
                    _ => panic!("Expected proposition in UNION clause"),
                }
            }
            _ => panic!("Expected UNION clause"),
        }
    }

    #[test]
    fn test_parse_order_by_clause() {
        let input = r#"
            FIND(?drug.name, ?drug.attributes.risk_level)
            WHERE {
                ?drug {type: "Drug"}
            }
            ORDER BY ?drug.attributes.risk_level ASC, ?drug.name DESC
        "#;

        let result = parse_kql_query(input);
        assert!(result.is_ok());

        let (_, query) = result.unwrap();
        assert!(query.order_by.is_some());
        let order_by = query.order_by.unwrap();
        assert_eq!(order_by.len(), 2);
        assert_eq!(order_by[0].variable.var, "drug");
        assert_eq!(
            order_by[0].variable.path,
            vec!["attributes".to_string(), "risk_level".to_string()]
        );
        assert_eq!(order_by[0].direction, OrderDirection::Asc);
        assert_eq!(order_by[1].variable.var, "drug");
        assert_eq!(order_by[1].variable.path, vec!["name".to_string()]);
        assert_eq!(order_by[1].direction, OrderDirection::Desc);
    }

    #[test]
    fn test_parse_order_by_default_asc() {
        let input = r#"
            FIND(?drug.name)
            WHERE {
                ?drug {type: "Drug"}
            }
            ORDER BY ?drug.name
        "#;

        let result = parse_kql_query(input);
        assert!(result.is_ok());

        let (_, query) = result.unwrap();
        assert!(query.order_by.is_some());
        let order_by = query.order_by.unwrap();
        assert_eq!(order_by.len(), 1);
        assert_eq!(order_by[0].direction, OrderDirection::Asc);
    }

    #[test]
    fn test_parse_limit_and_cursor() {
        let input = r#"
            FIND(?drug.name)
            WHERE {
                ?drug {type: "Drug"}
            }
            ORDER BY ?drug.name
            LIMIT 20
            CURSOR "abcdef"
        "#;

        let result = parse_kql_query(input);
        assert!(result.is_ok());

        let (_, query) = result.unwrap();
        assert_eq!(query.limit, Some(20));
        assert_eq!(query.cursor, Some("abcdef".to_string()));
    }

    #[test]
    fn test_parse_complex_query() {
        let input = r#"
            FIND(?drug.name, ?drug.attributes.risk_level)
            WHERE {
                ?drug {type: "Drug"}
                ?headache{name: "Headache"}
                ?nsaid_class  {name: "NSAID"}

                (?drug, "treats", ?headache)

                NOT {
                    (?drug, "is_class_of", ?nsaid_class)
                }

                FILTER(?drug.attributes.risk_level < 4)
            }
            ORDER BY ?drug.attributes.risk_level ASC
            LIMIT 20
        "#;

        let result = parse_kql_query(input);
        assert!(result.is_ok());

        let (_, query) = result.unwrap();
        assert_eq!(query.find_clause.expressions.len(), 2);
        assert_eq!(query.where_clauses.len(), 6); // 3 groundings + 1 prop + 1 not + 1 filter
        assert!(query.order_by.is_some());
        assert_eq!(query.limit, Some(20));
        assert!(query.cursor.is_none());
    }

    #[test]
    fn test_parse_aggregation_with_grouping() {
        let input = r#"
            FIND(?class.name, COUNT(?drug.name))
            WHERE {
                ?class {type: "DrugClass"}
                ?drug {type: "Drug"}
                (?drug, "is_class_of", ?class)
            }
            ORDER BY ?class.name
        "#;

        let result = parse_kql_query(input);
        assert!(result.is_ok());

        let (_, query) = result.unwrap();
        assert_eq!(query.find_clause.expressions.len(), 2);
        match &query.find_clause.expressions[1] {
            FindExpression::Aggregation { func, var, .. } => {
                assert_eq!(*func, AggregationFunction::Count);
                assert_eq!(var.var, "drug");
                assert_eq!(var.path, vec!["name".to_string()]);
            }
            _ => panic!("Expected aggregation expression"),
        }
    }

    #[test]
    fn test_parse_error_cases() {
        let invalid_inputs = vec![
            "FIND() WHERE {}",           // Empty FIND
            "FIND(?var WHERE {}",        // Missing closing parenthesis
            "FIND(?var) WHERE",          // Missing WHERE block
            "FIND(?var) WHERE { ?var }", // Invalid grounding syntax
        ];

        for input in invalid_inputs {
            let result = parse_kql_query(input);
            assert!(result.is_err(), "Should fail to parse: {input}");
        }
    }

    #[test]
    fn test_parse_whitespace_handling() {
        let input_with_extra_whitespace = r#"
            FIND  (  ?drug_name  ,  ?symptom_name  )
            WHERE   {
                ?drug  {  type:  "Drug"  }
                ?prop  (  ?drug  ,  "treats"  ,  ?symptom  )
            }
            ORDER   BY   ?drug_name   ASC
            LIMIT   10
        "#;

        let result = parse_kql_query(input_with_extra_whitespace);

        assert!(result.is_ok());

        let (_, query) = result.unwrap();
        assert_eq!(query.find_clause.expressions.len(), 2);
        assert_eq!(query.where_clauses.len(), 2);
        assert_eq!(query.limit, Some(10));
    }
}
