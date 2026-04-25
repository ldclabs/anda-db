use nom::{
    Parser,
    branch::alt,
    bytes::complete::tag,
    combinator::{map, opt, value},
    error::context,
    sequence::preceded,
};

use super::common::*;
use super::kql::{parse_cursor_clause, parse_limit_clause};
use crate::ast::*;

// --- Top Level META Parser ---

pub fn parse_meta_command(input: &str) -> VResult<'_, MetaCommand> {
    context(
        "META command: DESCRIBE ... or SEARCH ...",
        alt((
            map(parse_describe_command, MetaCommand::Describe),
            map(parse_search_command, MetaCommand::Search),
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
        "SEARCH CONCEPT|PROPOSITION \"<term>\" [WITH TYPE \"<Type>\"] [LIMIT N]",
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
                    opt(preceded(
                        keyword("LIMIT"),
                        ws(nom::character::complete::usize),
                    )),
                ),
            ),
            |(target, term, in_type, limit)| SearchCommand {
                target,
                term,
                in_type,
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
                    limit: None,
                }
            ))
        );

        // Test invalid search command
        assert!(parse_search_command("SEARCH INVALID").is_err());
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
                    limit: Some(5),
                }
            ))
        );
    }
}
