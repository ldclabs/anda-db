use nom::{
    Parser,
    branch::alt,
    bytes::complete::tag,
    combinator::{cut, map, map_res, opt, value},
    error::context,
    sequence::preceded,
};
use std::str::FromStr;

use super::common::*;
use super::json::parse_number;
use super::kql::{parse_cursor_clause, parse_limit_clause, parse_where_block};
use crate::ast::*;

// --- Top Level META Parser ---

pub fn parse_meta_command(input: &str) -> VResult<'_, MetaCommand> {
    context(
        "META command: DESCRIBE ... | SEARCH ... | EXPORT ...",
        alt((
            map(parse_describe_command, MetaCommand::Describe),
            map(parse_search_command, MetaCommand::Search),
            map(parse_export_command, MetaCommand::Export),
        )),
    )
    .parse(input)
}

// --- DESCRIBE ---

fn parse_describe_command(input: &str) -> VResult<'_, DescribeTarget> {
    preceded(
        ws(keyword("DESCRIBE")),
        ws(alt((
            context(
                "DESCRIBE PRIMER",
                value(DescribeTarget::Primer, ws(tag("PRIMER"))),
            ),
            context(
                "DESCRIBE DOMAINS",
                value(DescribeTarget::Domains, ws(tag("DOMAINS"))),
            ),
            context(
                "DESCRIBE CONCEPT TYPES",
                map(
                    preceded(
                        ws(keywords(&["CONCEPT", "TYPES"])),
                        (opt(ws(parse_limit_clause)), opt(ws(parse_cursor_clause))),
                    ),
                    |(limit, cursor)| DescribeTarget::ConceptTypes { limit, cursor },
                ),
            ),
            context(
                "DESCRIBE CONCEPT TYPE \"<TypeName>\"",
                map(
                    preceded(keywords(&["CONCEPT", "TYPE"]), ws(quoted_string)),
                    DescribeTarget::ConceptType,
                ),
            ),
            context(
                "DESCRIBE PROPOSITION TYPES",
                map(
                    preceded(
                        ws(keywords(&["PROPOSITION", "TYPES"])),
                        (opt(ws(parse_limit_clause)), opt(ws(parse_cursor_clause))),
                    ),
                    |(limit, cursor)| DescribeTarget::PropositionTypes { limit, cursor },
                ),
            ),
            context(
                "DESCRIBE PROPOSITION TYPE \"<predicate>\"",
                map(
                    preceded(keywords(&["PROPOSITION", "TYPE"]), ws(quoted_string)),
                    DescribeTarget::PropositionType,
                ),
            ),
        ))),
    )
    .parse(input)
}

// --- SEARCH ---
fn parse_search_command(input: &str) -> VResult<'_, SearchCommand> {
    context(
        "SEARCH CONCEPT|PROPOSITION \"<term>\" [WITH TYPE \"<Type>\"] [MODE \"keyword\"|\"semantic\"|\"hybrid\"] [THRESHOLD <0.0-1.0>] [LIMIT N]",
        map(
            preceded(
                ws(keyword("SEARCH")),
                (
                    ws(alt((
                        value(SearchTarget::Concept, keyword("CONCEPT")),
                        value(SearchTarget::Proposition, keyword("PROPOSITION")),
                    ))),
                    ws(quoted_string),
                    opt(preceded(keywords(&["WITH", "TYPE"]), ws(quoted_string))),
                    opt(preceded(keyword("MODE"), cut(ws(parse_search_mode)))),
                    opt(preceded(
                        keyword("THRESHOLD"),
                        cut(ws(parse_search_threshold)),
                    )),
                    opt(preceded(
                        keyword("LIMIT"),
                        ws(nom::character::complete::usize),
                    )),
                ),
            ),
            |(target, term, in_type, mode, threshold, limit)| SearchCommand {
                target,
                term,
                in_type,
                mode,
                threshold,
                limit,
            },
        ),
    )
    .parse(input)
}

fn parse_search_mode(input: &str) -> VResult<'_, SearchMode> {
    context(
        "SEARCH MODE: \"keyword\" | \"semantic\" | \"hybrid\"",
        map_res(quoted_string, |s| SearchMode::from_str(&s)),
    )
    .parse(input)
}

fn parse_search_threshold(input: &str) -> VResult<'_, Number> {
    context(
        "SEARCH THRESHOLD: a number between 0.0 and 1.0",
        map_res(parse_number, |n| {
            let v = n.as_f64().unwrap_or(-1.0);
            if (0.0..=1.0).contains(&v) {
                Ok(n)
            } else {
                Err(format!("THRESHOLD must be between 0.0 and 1.0, got {n}"))
            }
        }),
    )
    .parse(input)
}

// --- EXPORT ---
fn parse_export_command(input: &str) -> VResult<'_, ExportCommand> {
    context(
        "EXPORT ?target WHERE { ... } [LIMIT N]",
        map(
            preceded(
                ws(keyword("EXPORT")),
                cut((
                    ws(variable),
                    parse_where_block,
                    opt(ws(parse_limit_clause)),
                )),
            ),
            |(target, where_clauses, limit)| ExportCommand {
                target,
                where_clauses,
                limit,
            },
        ),
    )
    .parse(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_meta_command() {
        // Test DESCRIBE commands
        assert_eq!(
            parse_meta_command("DESCRIBE PRIMER"),
            Ok(("", MetaCommand::Describe(DescribeTarget::Primer)))
        );
        assert_eq!(
            parse_meta_command("DESCRIBE DOMAINS"),
            Ok(("", MetaCommand::Describe(DescribeTarget::Domains)))
        );

        // Test SEARCH commands
        assert_eq!(
            parse_meta_command("SEARCH CONCEPT \"aspirin\""),
            Ok((
                "",
                MetaCommand::Search(SearchCommand {
                    target: SearchTarget::Concept,
                    term: "aspirin".to_string(),
                    in_type: None,
                    mode: None,
                    threshold: None,
                    limit: None,
                })
            ))
        );

        // Test with whitespace
        assert_eq!(
            parse_meta_command("  DESCRIBE   PRIMER  "),
            Ok(("", MetaCommand::Describe(DescribeTarget::Primer)))
        );

        // Test invalid command
        assert!(parse_meta_command("INVALID COMMAND").is_err());
    }

    #[test]
    fn test_parse_describe_command() {
        // Test all DESCRIBE targets
        assert_eq!(
            parse_describe_command("DESCRIBE PRIMER"),
            Ok(("", DescribeTarget::Primer))
        );
        assert_eq!(
            parse_describe_command("DESCRIBE DOMAINS"),
            Ok(("", DescribeTarget::Domains))
        );
        assert_eq!(
            parse_describe_command("DESCRIBE CONCEPT TYPES"),
            Ok((
                "",
                DescribeTarget::ConceptTypes {
                    limit: None,
                    cursor: None
                }
            ))
        );
        assert_eq!(
            parse_describe_command("DESCRIBE CONCEPT TYPES LIMIT 5"),
            Ok((
                "",
                DescribeTarget::ConceptTypes {
                    limit: Some(5),
                    cursor: None
                }
            ))
        );
        assert_eq!(
            parse_describe_command("DESCRIBE CONCEPT TYPES LIMIT 5 CURSOR \"abcdef\""),
            Ok((
                "",
                DescribeTarget::ConceptTypes {
                    limit: Some(5),
                    cursor: Some("abcdef".to_string())
                }
            ))
        );
        assert_eq!(
            parse_describe_command("DESCRIBE PROPOSITION TYPES"),
            Ok((
                "",
                DescribeTarget::PropositionTypes {
                    limit: None,
                    cursor: None
                }
            ))
        );
        assert_eq!(
            parse_describe_command("DESCRIBE PROPOSITION TYPES LIMIT 5"),
            Ok((
                "",
                DescribeTarget::PropositionTypes {
                    limit: Some(5),
                    cursor: None
                }
            ))
        );
        assert_eq!(
            parse_describe_command("DESCRIBE PROPOSITION TYPES LIMIT 5 CURSOR \"abcdef\""),
            Ok((
                "",
                DescribeTarget::PropositionTypes {
                    limit: Some(5),
                    cursor: Some("abcdef".to_string())
                }
            ))
        );
        assert_eq!(
            parse_describe_command("DESCRIBE CONCEPT TYPE \"Drug\""),
            Ok(("", DescribeTarget::ConceptType("Drug".to_string())))
        );
        assert_eq!(
            parse_describe_command("DESCRIBE PROPOSITION TYPE \"treats\""),
            Ok(("", DescribeTarget::PropositionType("treats".to_string())))
        );

        // Test with whitespace
        assert_eq!(
            parse_describe_command("  DESCRIBE   PRIMER  "),
            Ok(("", DescribeTarget::Primer))
        );

        // Test invalid DESCRIBE command
        assert!(parse_describe_command("DESCRIBE INVALID").is_err());
    }

    #[test]
    fn test_parse_search_command() {
        // Basic search
        assert_eq!(
            parse_search_command("SEARCH CONCEPT \"aspirin\""),
            Ok((
                "",
                SearchCommand {
                    target: SearchTarget::Concept,
                    term: "aspirin".to_string(),
                    in_type: None,
                    mode: None,
                    threshold: None,
                    limit: None,
                }
            ))
        );

        // Search with type
        assert_eq!(
            parse_search_command("SEARCH CONCEPT \"aspirin\" \n\n\nWITH TYPE \"Drug\" \nLIMIT  5"),
            Ok((
                "",
                SearchCommand {
                    target: SearchTarget::Concept,
                    term: "aspirin".to_string(),
                    in_type: Some("Drug".to_string()),
                    mode: None,
                    threshold: None,
                    limit: Some(5),
                }
            ))
        );

        // Search with limit
        assert_eq!(
            parse_search_command("SEARCH PROPOSITION \"aspirin\" LIMIT 5"),
            Ok((
                "",
                SearchCommand {
                    target: SearchTarget::Proposition,
                    term: "aspirin".to_string(),
                    in_type: None,
                    mode: None,
                    threshold: None,
                    limit: Some(5),
                }
            ))
        );

        // Search with type and limit
        assert_eq!(
            parse_search_command("SEARCH CONCEPT \"aspirin\" WITH TYPE \"Drug\" LIMIT 5"),
            Ok((
                "",
                SearchCommand {
                    target: SearchTarget::Concept,
                    term: "aspirin".to_string(),
                    in_type: Some("Drug".to_string()),
                    mode: None,
                    threshold: None,
                    limit: Some(5),
                }
            ))
        );

        // Test with whitespace
        assert_eq!(
            parse_search_command("  SEARCH   CONCEPT   \"aspirin\"  "),
            Ok((
                "",
                SearchCommand {
                    target: SearchTarget::Concept,
                    term: "aspirin".to_string(),
                    in_type: None,
                    mode: None,
                    threshold: None,
                    limit: None,
                }
            ))
        );

        // Test with special characters in search term
        assert_eq!(
            parse_search_command("SEARCH CONCEPT \"阿司匹林\""),
            Ok((
                "",
                SearchCommand {
                    target: SearchTarget::Concept,
                    term: "阿司匹林".to_string(),
                    in_type: None,
                    mode: None,
                    threshold: None,
                    limit: None,
                }
            ))
        );

        // Test invalid search command
        assert!(parse_search_command("SEARCH INVALID").is_err());
    }

    #[test]
    fn test_parse_search_with_mode_and_threshold() {
        // Associative recall probe from the spec
        assert_eq!(
            parse_search_command(
                r#"SEARCH CONCEPT "headache relief" MODE "semantic" THRESHOLD 0.75 LIMIT 10"#
            ),
            Ok((
                "",
                SearchCommand {
                    target: SearchTarget::Concept,
                    term: "headache relief".to_string(),
                    in_type: None,
                    mode: Some(SearchMode::Semantic),
                    threshold: Some(Number::from_f64(0.75).unwrap()),
                    limit: Some(10),
                }
            ))
        );

        // Full option set with WITH TYPE
        assert_eq!(
            parse_search_command(
                r#"SEARCH PROPOSITION "treats" WITH TYPE "treats" MODE "hybrid" THRESHOLD 0.5 LIMIT 5"#
            ),
            Ok((
                "",
                SearchCommand {
                    target: SearchTarget::Proposition,
                    term: "treats".to_string(),
                    in_type: Some("treats".to_string()),
                    mode: Some(SearchMode::Hybrid),
                    threshold: Some(Number::from_f64(0.5).unwrap()),
                    limit: Some(5),
                }
            ))
        );

        // Keyword mode without threshold
        assert_eq!(
            parse_search_command(r#"SEARCH CONCEPT "aspirin" MODE "keyword""#),
            Ok((
                "",
                SearchCommand {
                    target: SearchTarget::Concept,
                    term: "aspirin".to_string(),
                    in_type: None,
                    mode: Some(SearchMode::Keyword),
                    threshold: None,
                    limit: None,
                }
            ))
        );

        // Invalid mode value
        assert!(parse_meta_command(r#"SEARCH CONCEPT "x" MODE "fuzzy""#).is_err());
        // Threshold out of range
        assert!(parse_meta_command(r#"SEARCH CONCEPT "x" THRESHOLD 1.5"#).is_err());
        assert!(parse_meta_command(r#"SEARCH CONCEPT "x" THRESHOLD -0.1"#).is_err());
    }

    #[test]
    fn test_parse_export_command() {
        let input = r#"
        EXPORT ?n
        WHERE {
            (?n, "belongs_to_domain", {type: "Domain", name: "Medical"})
        }
        LIMIT 500
        "#;

        let result = parse_meta_command(input);
        assert!(result.is_ok(), "Failed to parse: {result:?}");
        let (_, command) = result.unwrap();
        match command {
            MetaCommand::Export(export) => {
                assert_eq!(export.target, "n");
                assert_eq!(export.where_clauses.len(), 1);
                assert_eq!(export.limit, Some(500));
            }
            _ => panic!("Expected ExportCommand"),
        }

        // EXPORT without LIMIT
        let (_, command) = parse_meta_command(
            r#"EXPORT ?x WHERE { ?x {type: "Drug"} }"#,
        )
        .unwrap();
        match command {
            MetaCommand::Export(export) => {
                assert_eq!(export.target, "x");
                assert_eq!(export.limit, None);
            }
            _ => panic!("Expected ExportCommand"),
        }

        // EXPORT requires a WHERE block
        assert!(parse_meta_command("EXPORT ?x LIMIT 10").is_err());
        // EXPORT requires a target variable
        assert!(parse_meta_command(r#"EXPORT WHERE { ?x {type: "Drug"} }"#).is_err());
    }

    #[test]
    fn test_keywords_accept_arbitrary_whitespace() {
        // Newlines and tabs between multi-word keywords (DESCRIBE / CONCEPT TYPES / WITH TYPE)
        // must be accepted just like a literal space.
        assert_eq!(
            parse_describe_command("DESCRIBE\n  CONCEPT\tTYPES   LIMIT 10"),
            Ok((
                "",
                DescribeTarget::ConceptTypes {
                    limit: Some(10),
                    cursor: None,
                }
            ))
        );

        assert_eq!(
            parse_describe_command("DESCRIBE\nPROPOSITION\nTYPE\n\"treats\""),
            Ok(("", DescribeTarget::PropositionType("treats".to_string())))
        );

        assert_eq!(
            parse_search_command("SEARCH\nCONCEPT\n\"aspirin\"\nWITH\nTYPE\n\"Drug\"\nLIMIT\n5"),
            Ok((
                "",
                SearchCommand {
                    target: SearchTarget::Concept,
                    term: "aspirin".to_string(),
                    in_type: Some("Drug".to_string()),
                    mode: None,
                    threshold: None,
                    limit: Some(5),
                }
            ))
        );
    }
}
