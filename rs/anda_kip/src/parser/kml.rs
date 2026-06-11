use nom::{
    Parser,
    branch::alt,
    bytes::complete::tag,
    character::complete::{char, multispace1},
    combinator::{cut, map, map_res, opt},
    error::context,
    multi::{many1, separated_list1},
    sequence::{preceded, separated_pair, terminated},
};

use super::common::*;
use super::json::{json_value, parse_number};
use super::kql::{
    parse_concept_matcher, parse_limit_clause, parse_prop_mather, parse_target_term,
    parse_where_block,
};
use crate::ast::*;

// --- Top Level KML Parser ---

pub fn parse_kml_statement(input: &str) -> VResult<'_, KmlStatement> {
    context(
        "KML statement: UPSERT { ... } | UPDATE ... | MERGE ... | DELETE ...",
        alt((
            map(parse_upsert_blocks, KmlStatement::Upsert),
            map(parse_update_statement, KmlStatement::Update),
            map(parse_merge_statement, KmlStatement::Merge),
            map(parse_delete_statement, KmlStatement::Delete),
        )),
    )
    .parse(input)
}

// --- UPSERT ---

fn parse_with_metadata(input: &str) -> VResult<'_, Map<String, Json>> {
    context(
        "WITH METADATA { ... }",
        preceded(ws(keywords(&["WITH", "METADATA"])), json_value_map),
    )
    .parse(input)
}

fn parse_upsert_blocks(input: &str) -> VResult<'_, Vec<UpsertBlock>> {
    context(
        "UPSERT { ... }",
        many1(map(
            preceded(
                ws(keyword("UPSERT")),
                cut((
                    braced_block(many1(ws(parse_upsert_item))),
                    opt(parse_with_metadata),
                )),
            ),
            |(items, metadata)| UpsertBlock { items, metadata },
        )),
    )
    .parse(input)
}

fn parse_upsert_item(input: &str) -> VResult<'_, UpsertItem> {
    context(
        "UPSERT item: CONCEPT ... or PROPOSITION ...",
        alt((
            map(parse_concept_block, UpsertItem::Concept),
            map(parse_proposition_block, UpsertItem::Proposition),
        )),
    )
    .parse(input)
}

/// Parses the optional optimistic-concurrency guard: `EXPECT VERSION <n>`.
fn parse_expect_version(input: &str) -> VResult<'_, u64> {
    context(
        "EXPECT VERSION <n>",
        preceded(
            (keywords(&["EXPECT", "VERSION"]), multispace1),
            cut(nom::character::complete::u64),
        ),
    )
    .parse(input)
}

fn parse_concept_block(input: &str) -> VResult<'_, ConceptBlock> {
    context(
        "CONCEPT [?local_handle] { ... }",
        map(
            (
                preceded(ws(keyword("CONCEPT")), opt(ws(variable))),
                cut(braced_block((
                    ws(parse_concept_matcher),
                    opt(ws(parse_expect_version)),
                    opt(context(
                        "SET ATTRIBUTES { ... }",
                        ws(preceded(keywords(&["SET", "ATTRIBUTES"]), json_value_map)),
                    )),
                    opt(context(
                        "SET PROPOSITIONS { ... }",
                        ws(preceded(
                            keywords(&["SET", "PROPOSITIONS"]),
                            braced_block(many1(ws(parse_set_proposition))),
                        )),
                    )),
                ))),
                opt(ws(parse_with_metadata)),
            ),
            |(handle, (concept, expect_version, set_attributes, set_propositions), metadata)| {
                ConceptBlock {
                    handle,
                    concept,
                    expect_version,
                    set_attributes,
                    set_propositions,
                    metadata,
                }
            },
        ),
    )
    .parse(input)
}

fn parse_set_proposition(input: &str) -> VResult<'_, SetProposition> {
    map(
        terminated(
            (
                parenthesized_block(separated_pair(
                    quoted_string,
                    ws(char(',')),
                    parse_target_term,
                )),
                opt(parse_with_metadata),
            ),
            opt(ws(char(','))), // Allow atypical trailing comma
        ),
        |((predicate, object), metadata)| SetProposition {
            predicate,
            object,
            metadata,
        },
    )
    .parse(input)
}

fn parse_proposition_block(input: &str) -> VResult<'_, PropositionBlock> {
    context(
        "PROPOSITION [?local_handle] { ... }",
        map(
            (
                preceded(ws(keyword("PROPOSITION")), opt(ws(variable))),
                cut(braced_block((
                    ws(parse_prop_mather),
                    opt(ws(parse_expect_version)),
                    opt(context(
                        "SET ATTRIBUTES { ... }",
                        ws(preceded(keywords(&["SET", "ATTRIBUTES"]), json_value_map)),
                    )),
                ))),
                opt(ws(parse_with_metadata)),
            ),
            |(handle, (proposition, expect_version, set_attributes), metadata)| PropositionBlock {
                handle,
                proposition,
                expect_version,
                set_attributes,
                metadata,
            },
        ),
    )
    .parse(input)
}

// --- UPDATE ---

fn parse_update_statement(input: &str) -> VResult<'_, UpdateStatement> {
    context(
        "UPDATE ?target SET ATTRIBUTES { ... } / SET METADATA { ... } WHERE { ... } [LIMIT N]",
        map_res(
            preceded(
                ws(keyword("UPDATE")),
                cut((
                    ws(variable),
                    opt(context(
                        "SET ATTRIBUTES { ... }",
                        ws(preceded(
                            keywords(&["SET", "ATTRIBUTES"]),
                            parse_update_value_map,
                        )),
                    )),
                    opt(context(
                        "SET METADATA { ... }",
                        ws(preceded(keywords(&["SET", "METADATA"]), parse_update_value_map)),
                    )),
                    parse_where_block,
                    opt(ws(parse_limit_clause)),
                )),
            ),
            |(target, set_attributes, set_metadata, where_clauses, limit)| {
                if set_attributes.is_none() && set_metadata.is_none() {
                    return Err(
                        "UPDATE requires at least one SET ATTRIBUTES or SET METADATA block"
                            .to_string(),
                    );
                }
                Ok(UpdateStatement {
                    target,
                    set_attributes,
                    set_metadata,
                    where_clauses,
                    limit,
                })
            },
        ),
    )
    .parse(input)
}

/// Parses an UPDATE SET map whose values may be JSON values or update expressions.
fn parse_update_value_map(input: &str) -> VResult<'_, Vec<(String, UpdateValue)>> {
    map(
        context(
            "UPDATE SET map",
            preceded(
                ws(char('{')),
                cut(terminated(
                    opt(terminated(
                        separated_list1(ws(char(',')), parse_update_key_value),
                        opt(ws(char(','))), // Allow trailing comma
                    )),
                    ws(char('}')),
                )),
            ),
        ),
        |kvs| kvs.unwrap_or_default(),
    )
    .parse(input)
}

fn parse_update_key_value(input: &str) -> VResult<'_, (String, UpdateValue)> {
    context(
        "key-value pair",
        separated_pair(
            alt((quoted_string, map(identifier, |s| s.to_string()))),
            cut(ws(char(':'))),
            cut(parse_update_value),
        ),
    )
    .parse(input)
}

fn parse_update_value(input: &str) -> VResult<'_, UpdateValue> {
    context(
        "UPDATE value: JSON value or ADD/MUL/CLAMP/COALESCE(...) expression",
        alt((
            map(parse_update_expr_function, UpdateValue::Expr),
            map(json_value(), UpdateValue::Json),
        )),
    )
    .parse(input)
}

fn parse_update_expr(input: &str) -> VResult<'_, UpdateExpr> {
    context(
        "UPDATE expression operand: number, ?target dot-path, or nested expression",
        alt((
            parse_update_expr_function,
            map(dot_path_var, UpdateExpr::Variable),
            map(parse_number, UpdateExpr::Number),
        )),
    )
    .parse(input)
}

fn parse_update_expr_function(input: &str) -> VResult<'_, UpdateExpr> {
    context(
        "UPDATE expression: ADD(a, b) | MUL(a, b) | CLAMP(x, lo, hi) | COALESCE(x, default)",
        map_res(
            (
                parse_update_function,
                parenthesized_block(separated_list1(ws(char(',')), ws(parse_update_expr))),
            ),
            |(func, args)| validate_update_function_args(func, args),
        ),
    )
    .parse(input)
}

fn parse_update_function(input: &str) -> VResult<'_, UpdateFunction> {
    alt((
        map(tag("ADD"), |_| UpdateFunction::Add),
        map(tag("MUL"), |_| UpdateFunction::Mul),
        map(tag("CLAMP"), |_| UpdateFunction::Clamp),
        map(tag("COALESCE"), |_| UpdateFunction::Coalesce),
    ))
    .parse(input)
}

fn validate_update_function_args(
    func: UpdateFunction,
    args: Vec<UpdateExpr>,
) -> Result<UpdateExpr, String> {
    let (name, expected) = match func {
        UpdateFunction::Add => ("ADD", 2),
        UpdateFunction::Mul => ("MUL", 2),
        UpdateFunction::Coalesce => ("COALESCE", 2),
        UpdateFunction::Clamp => ("CLAMP", 3),
    };
    if args.len() != expected {
        return Err(format!(
            "{name} requires exactly {expected} arguments, got {}",
            args.len()
        ));
    }
    Ok(UpdateExpr::Function { func, args })
}

// --- MERGE ---

fn parse_merge_statement(input: &str) -> VResult<'_, MergeStatement> {
    context(
        "MERGE CONCEPT ?source INTO ?target WHERE { ... }",
        map(
            preceded(
                ws(keywords(&["MERGE", "CONCEPT"])),
                cut((
                    ws(variable),
                    preceded(ws(tag("INTO")), ws(variable)),
                    parse_where_block,
                )),
            ),
            |(source, target, where_clauses)| MergeStatement {
                source,
                target,
                where_clauses,
            },
        ),
    )
    .parse(input)
}

// --- DELETE ---

fn parse_delete_statement(input: &str) -> VResult<'_, DeleteStatement> {
    preceded(
        ws(keyword("DELETE")),
        cut(context(
            "DELETE target: ATTRIBUTES | METADATA | PROPOSITIONS | CONCEPT",
            alt((
                parse_delete_attributes,
                parse_delete_metadata,
                parse_delete_propositions,
                parse_delete_concept,
            )),
        )),
    )
    .parse(input)
}

fn parse_delete_attributes(input: &str) -> VResult<'_, DeleteStatement> {
    context(
        "DELETE ATTRIBUTES ...",
        map(
            preceded(
                ws(tag("ATTRIBUTES")),
                cut((
                    braced_block(separated_list1(ws(char(',')), quoted_string)),
                    preceded(ws(tag("FROM")), variable),
                    parse_where_block,
                )),
            ),
            |(attributes, target, where_clauses)| DeleteStatement::DeleteAttributes {
                attributes,
                target,
                where_clauses,
            },
        ),
    )
    .parse(input)
}

fn parse_delete_metadata(input: &str) -> VResult<'_, DeleteStatement> {
    context(
        "DELETE METADATA ...",
        map(
            preceded(
                ws(tag("METADATA")),
                cut((
                    braced_block(separated_list1(ws(char(',')), quoted_string)),
                    preceded(ws(tag("FROM")), variable),
                    parse_where_block,
                )),
            ),
            |(keys, target, where_clauses)| DeleteStatement::DeleteMetadata {
                keys,
                target,
                where_clauses,
            },
        ),
    )
    .parse(input)
}

fn parse_delete_propositions(input: &str) -> VResult<'_, DeleteStatement> {
    context(
        "DELETE PROPOSITIONS ...",
        map(
            preceded(
                ws(tag("PROPOSITIONS")),
                cut((ws(variable), parse_where_block)),
            ),
            |(target, where_clauses)| DeleteStatement::DeletePropositions {
                target,
                where_clauses,
            },
        ),
    )
    .parse(input)
}

fn parse_delete_concept(input: &str) -> VResult<'_, DeleteStatement> {
    context(
        "DELETE CONCEPT ...",
        map(
            preceded(
                ws(tag("CONCEPT")),
                cut((terminated(variable, ws(tag("DETACH"))), parse_where_block)),
            ),
            |(target, where_clauses)| DeleteStatement::DeleteConcept {
                target,
                where_clauses,
            },
        ),
    )
    .parse(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Number;

    #[test]
    fn test_parse_simple_upsert_concept() {
        let input = r#"
        UPSERT {
            CONCEPT ?drug {
                { type: "Drug", name: "Aspirin" }
                SET ATTRIBUTES {
                    molecular_formula: "C9H8O4",
                    risk_level: 2
                }
            }
        }
        "#;

        let result = parse_kml_statement(input);
        assert!(result.is_ok());

        let (_, statement) = result.unwrap();
        match statement {
            KmlStatement::Upsert(upserts) => {
                assert_eq!(upserts.len(), 1);
                let upsert = &upserts[0];
                assert_eq!(upsert.items.len(), 1);
                assert!(upsert.metadata.is_none());

                match &upsert.items[0] {
                    UpsertItem::Concept(concept) => {
                        assert_eq!(concept.handle, Some("drug".to_string()));
                        assert_eq!(
                            concept.concept,
                            ConceptMatcher::Object {
                                r#type: "Drug".to_string(),
                                name: "Aspirin".to_string(),
                            }
                        );

                        let attrs = concept.set_attributes.as_ref().unwrap();
                        assert_eq!(attrs.len(), 2);
                        assert_eq!(
                            attrs["molecular_formula"],
                            Json::String("C9H8O4".to_string())
                        );
                        assert_eq!(attrs["risk_level"], Json::Number(Number::from(2)));
                    }
                    _ => panic!("Expected ConceptBlock"),
                }
            }
            _ => panic!("Expected UpsertBlock"),
        }
    }

    #[test]
    fn test_parse_simple_upsert_concept_with_metadata() {
        let input = r#"
        UPSERT {
            CONCEPT ?drug {
                {type: "Drug", name: "TestDrug"}
            }
            WITH METADATA {
                "confidence":0.95,
                "source":"clinical_trial"
            }
        }
        "#;

        let result = parse_kml_statement(input);
        assert!(result.is_ok());
        let (_, statement) = result.unwrap();
        match statement {
            KmlStatement::Upsert(upserts) => {
                assert_eq!(upserts.len(), 1);
                let upsert = &upserts[0];
                assert_eq!(upsert.items.len(), 1);
                assert!(upsert.metadata.is_none());

                match &upsert.items[0] {
                    UpsertItem::Concept(concept) => {
                        assert_eq!(concept.handle, Some("drug".to_string()));
                        assert_eq!(
                            concept.concept,
                            ConceptMatcher::Object {
                                r#type: "Drug".to_string(),
                                name: "TestDrug".to_string(),
                            }
                        );

                        let metadata = concept.metadata.as_ref().unwrap();
                        assert_eq!(
                            metadata["confidence"],
                            Json::Number(Number::from_f64(0.95).unwrap())
                        );
                        assert_eq!(
                            metadata["source"],
                            Json::String("clinical_trial".to_string())
                        );
                    }
                    _ => panic!("Expected ConceptBlock"),
                }
            }
            _ => panic!("Expected UpsertBlock"),
        }
    }

    #[test]
    fn test_parse_upsert_with_propositions() {
        let input = r#"
        UPSERT {
            CONCEPT ?cognizine {
                { type: "Drug", name: "Cognizine" }
                SET ATTRIBUTES {
                    molecular_formula: "C12H15N5O3",
                    risk_level: 2
                }
                SET PROPOSITIONS {
                    ("is_class_of", { type: "DrugClass", name: "Nootropic" })
                    ("treats", { type: "Symptom", name: "Brain Fog" })
                    ("has_side_effect", ?neural_bloom) WITH METADATA {
                        confidence: 0.75,
                        source: "Clinical Trial"
                    }
                }
            }

            CONCEPT ?neural_bloom {
                { type: "Symptom", name: "Neural Bloom" }
                SET ATTRIBUTES {
                    description: "A rare side effect"
                }
            }
        }
        WITH METADATA {
            source: "KnowledgeCapsule:Nootropics_v1.0",
            author: "LDC Labs",
            confidence: 0.95
        }

        // Test with multiple upsert blocks
        UPSERT {
            CONCEPT ?drug {
                {type: "Drug", name: "TestDrug"}
            }
            WITH METADATA {
                "confidence":0.95,
                "source":"clinical_trial"
            }
        }
        "#;

        let result = parse_kml_statement(input);
        assert!(result.is_ok());

        let (_, statement) = result.unwrap();
        match statement {
            KmlStatement::Upsert(upserts) => {
                assert_eq!(upserts.len(), 2);
                let upsert = &upserts[0];
                assert_eq!(upsert.items.len(), 2);
                assert!(upsert.metadata.is_some());

                let metadata = upsert.metadata.as_ref().unwrap();
                assert_eq!(
                    metadata["source"],
                    Json::String("KnowledgeCapsule:Nootropics_v1.0".to_string())
                );
                assert_eq!(metadata["author"], Json::String("LDC Labs".to_string()));
                assert_eq!(
                    metadata["confidence"],
                    Json::Number(Number::from_f64(0.95).unwrap())
                );

                // Check first concept with propositions
                match &upsert.items[0] {
                    UpsertItem::Concept(concept) => {
                        assert_eq!(concept.handle, Some("cognizine".to_string()));
                        assert_eq!(
                            concept.concept,
                            ConceptMatcher::Object {
                                r#type: "Drug".to_string(),
                                name: "Cognizine".to_string(),
                            }
                        );
                        let props = concept.set_propositions.as_ref().unwrap();
                        assert_eq!(props.len(), 3);

                        assert_eq!(props[0].predicate, "is_class_of");
                        assert_eq!(
                            props[0].object,
                            TargetTerm::Concept(ConceptMatcher::Object {
                                    r#type: "DrugClass".to_string(),
                                    name: "Nootropic".to_string(),
                                })
                        );

                        assert_eq!(props[1].predicate, "treats");
                        assert_eq!(
                            props[1].object,
                            TargetTerm::Concept(ConceptMatcher::Object {
                                    r#type: "Symptom".to_string(),
                                    name: "Brain Fog".to_string(),
                                })
                        );

                        assert_eq!(props[2].predicate, "has_side_effect");
                        assert_eq!(
                            props[2].object,
                            TargetTerm::Variable("neural_bloom".to_string()),
                        );

                        let prop_metadata = props[2].metadata.as_ref().unwrap();
                        assert_eq!(
                            prop_metadata["confidence"],
                            Json::Number(Number::from_f64(0.75).unwrap())
                        );
                    }
                    _ => panic!("Expected ConceptBlock"),
                }

                let upsert = &upserts[1];
                assert_eq!(upsert.items.len(), 1);
                assert!(upsert.metadata.is_none());

                match &upsert.items[0] {
                    UpsertItem::Concept(concept) => {
                        assert_eq!(concept.handle, Some("drug".to_string()));
                        assert_eq!(
                            concept.concept,
                            ConceptMatcher::Object {
                                r#type: "Drug".to_string(),
                                name: "TestDrug".to_string(),
                            }
                        );

                        let metadata = concept.metadata.as_ref().unwrap();
                        assert_eq!(
                            metadata["confidence"],
                            Json::Number(Number::from_f64(0.95).unwrap())
                        );
                        assert_eq!(
                            metadata["source"],
                            Json::String("clinical_trial".to_string())
                        );
                    }
                    _ => panic!("Expected ConceptBlock"),
                }
            }
            _ => panic!("Expected UpsertBlock"),
        }
    }

    #[test]
    fn test_parse_proposition_block() {
        let input = r#"
        UPSERT {
            PROPOSITION ?stmt {
                ( { name: "Zhang San" }, "stated", { type: "Paper", name: "paper_doi" } )
                SET ATTRIBUTES {
                    doi: "10.1000/xyz",
                    created_at: "2023-11-10T14:20:10Z"
                }
            }
            WITH METADATA {
                confidence: 0.9
            }
        }
        "#;

        let result = parse_kml_statement(input);
        assert!(result.is_ok());

        let (_, statement) = result.unwrap();
        match statement {
            KmlStatement::Upsert(upserts) => {
                assert_eq!(upserts.len(), 1);
                let upsert = &upserts[0];
                assert_eq!(upsert.items.len(), 1);

                match &upsert.items[0] {
                    UpsertItem::Proposition(prop) => {
                        assert_eq!(prop.handle, Some("stmt".to_string()));
                        assert_eq!(
                            prop.proposition,
                            PropositionMatcher::Object {
                                subject: TargetTerm::Concept(ConceptMatcher::Name("Zhang San".to_string())),
                                predicate: PredTerm::Literal("stated".to_string()),
                                object: TargetTerm::Concept(ConceptMatcher::Object {
                                        r#type: "Paper".to_string(),
                                        name: "paper_doi".to_string(),
                                    }),
                            }
                        );

                        let set_attributes = prop.set_attributes.as_ref().unwrap();
                        assert_eq!(set_attributes.len(), 2);
                        assert_eq!(set_attributes["doi"], "10.1000/xyz");
                        assert_eq!(set_attributes["created_at"], "2023-11-10T14:20:10Z");

                        // Check metadata
                        let metadata = prop.metadata.as_ref().unwrap();
                        assert_eq!(metadata.len(), 1);
                        assert_eq!(
                            metadata["confidence"],
                            Json::Number(Number::from_f64(0.9).unwrap())
                        );
                    }
                    _ => panic!("Expected PropositionBlock"),
                }
            }
            _ => panic!("Expected UpsertBlock"),
        }
    }

    #[test]
    fn test_parse_delete_attributes() {
        let input = r#"
        DELETE ATTRIBUTES { "risk_category", "old_name" } FROM ?drug
        WHERE {?drug{ type: "Drug", name: "Aspirin" }}
        "#;

        let result = parse_kml_statement(input);
        assert!(result.is_ok());

        let (_, statement) = result.unwrap();
        match statement {
            KmlStatement::Delete(DeleteStatement::DeleteAttributes {
                attributes,
                target,
                where_clauses,
            }) => {
                assert_eq!(attributes.len(), 2);
                assert_eq!(attributes[0], "risk_category");
                assert_eq!(attributes[1], "old_name");
                assert_eq!(target, "drug");

                assert_eq!(where_clauses.len(), 1);
                assert_eq!(
                    where_clauses[0],
                    WhereClause::Concept(ConceptClause {
                        matcher: ConceptMatcher::Object {
                            r#type: "Drug".to_string(),
                            name: "Aspirin".to_string(),
                        },
                        variable: "drug".to_string(),
                    })
                );
            }
            _ => panic!("Expected DeleteAttributes"),
        }
    }

    #[test]
    fn test_parse_delete_propositions_where() {
        let input = r#"
        DELETE PROPOSITIONS ?link
        WHERE {
            ?link (?s, ?p, ?o)
            FILTER(?link.metadata.source == "untrusted_source")
        }
        "#;

        let result = parse_kml_statement(input);
        assert!(result.is_ok());

        let (_, statement) = result.unwrap();
        match statement {
            KmlStatement::Delete(DeleteStatement::DeletePropositions {
                target,
                where_clauses,
            }) => {
                assert_eq!(target, "link");
                assert_eq!(where_clauses.len(), 2);
                assert_eq!(
                    where_clauses[0],
                    WhereClause::Proposition(PropositionClause {
                        matcher: PropositionMatcher::Object {
                            subject: TargetTerm::Variable("s".to_string()),
                            predicate: PredTerm::Variable("p".to_string()),
                            object: TargetTerm::Variable("o".to_string()),
                        },
                        variable: Some("link".to_string()),
                    })
                );
                assert_eq!(
                    where_clauses[1],
                    WhereClause::Filter(FilterClause {
                        expression: FilterExpression::Comparison {
                            left: FilterOperand::Variable(DotPathVar {
                                var: "link".to_string(),
                                path: vec!["metadata".to_string(), "source".to_string()],
                            }),
                            operator: ComparisonOperator::Equal,
                            right: FilterOperand::Literal("untrusted_source".into()),
                        },
                    })
                );
            }
            _ => panic!("Expected DeletePropositionsWhere"),
        }
    }

    #[test]
    fn test_parse_delete_concept() {
        let input = r#"
        DELETE CONCEPT ?drug DETACH
        WHERE {
            ?drug { type: "Drug", name: "OutdatedDrug" }
        }
        "#;

        let result = parse_kml_statement(input);
        assert!(result.is_ok());

        let (_, statement) = result.unwrap();
        match statement {
            KmlStatement::Delete(DeleteStatement::DeleteConcept {
                target,
                where_clauses,
            }) => {
                assert_eq!(target, "drug");
                assert_eq!(where_clauses.len(), 1);
                assert_eq!(
                    where_clauses[0],
                    WhereClause::Concept(ConceptClause {
                        matcher: ConceptMatcher::Object {
                            r#type: "Drug".to_string(),
                            name: "OutdatedDrug".to_string(),
                        },
                        variable: "drug".to_string(),
                    })
                );
            }
            _ => panic!("Expected DeleteConcept"),
        }
    }

    #[test]
    fn test_parse_complex_upsert_with_mixed_items() {
        let input = r#"
        UPSERT {
            CONCEPT ?drug {
                { id: "drug_001" }
                SET ATTRIBUTES {
                    name: "TestDrug",
                    active: true,
                    dosage: null
                }
            }

            PROPOSITION ?relation {
                ( { id: "drug_001" }, "interacts_with", { id: "drug_002" } )
            }
            WITH METADATA {
                interaction_type: "synergistic"
            }

            CONCEPT ?target {
                { id: "drug_002" }
                SET PROPOSITIONS {
                    ("belongs_to", ?relation)
                }
            }
        }
        WITH METADATA {
            batch_id: "batch_123",
            timestamp: "2024-01-01T00:00:00Z"
        }
        "#;

        let result = parse_kml_statement(input);
        assert!(result.is_ok());

        let (_, statement) = result.unwrap();
        match statement {
            KmlStatement::Upsert(upserts) => {
                assert_eq!(upserts.len(), 1);
                let upsert = &upserts[0];
                assert_eq!(upsert.items.len(), 3);
                assert!(upsert.metadata.is_some());

                // Check global metadata
                let global_metadata = upsert.metadata.as_ref().unwrap();
                assert_eq!(
                    global_metadata["batch_id"],
                    Json::String("batch_123".to_string())
                );
                assert_eq!(
                    global_metadata["timestamp"],
                    Json::String("2024-01-01T00:00:00Z".to_string())
                );

                // Check first item (concept)
                match &upsert.items[0] {
                    UpsertItem::Concept(concept) => {
                        assert_eq!(concept.handle, Some("drug".to_string()));
                        assert_eq!(concept.concept, ConceptMatcher::ID("drug_001".to_string()));
                        let attrs = concept.set_attributes.as_ref().unwrap();
                        assert_eq!(attrs["name"], Json::String("TestDrug".to_string()));
                        assert_eq!(attrs["active"], Json::Bool(true));
                        assert_eq!(attrs["dosage"], Json::Null);
                    }
                    _ => panic!("Expected ConceptBlock"),
                }

                // Check second item (proposition)
                match &upsert.items[1] {
                    UpsertItem::Proposition(prop) => {
                        assert_eq!(prop.handle, Some("relation".to_string()));
                        assert_eq!(
                            prop.proposition,
                            PropositionMatcher::Object {
                                subject: TargetTerm::Concept(ConceptMatcher::ID("drug_001".to_string())),
                                predicate: PredTerm::Literal("interacts_with".to_string()),
                                object: TargetTerm::Concept(ConceptMatcher::ID("drug_002".to_string())),
                            }
                        );
                        let metadata = prop.metadata.as_ref().unwrap();
                        assert_eq!(
                            metadata["interaction_type"],
                            Json::String("synergistic".to_string())
                        );
                    }
                    _ => panic!("Expected PropositionBlock"),
                }

                // Check third item (concept with proposition referencing local handle)
                match &upsert.items[2] {
                    UpsertItem::Concept(concept) => {
                        assert_eq!(concept.handle, Some("target".to_string()));
                        assert_eq!(concept.concept, ConceptMatcher::ID("drug_002".to_string()));
                        let props = concept.set_propositions.as_ref().unwrap();
                        assert_eq!(props.len(), 1);
                        assert_eq!(
                            props[0],
                            SetProposition {
                                predicate: "belongs_to".to_string(),
                                object: TargetTerm::Variable("relation".to_string()),
                                metadata: None,
                            }
                        );
                    }
                    _ => panic!("Expected ConceptBlock"),
                }
            }
            _ => panic!("Expected UpsertBlock"),
        }
    }

    #[test]
    fn test_parse_proposition_block_without_handle() {
        let input = r#"
        UPSERT {
            PROPOSITION {
                ( { name: "Zhang San" }, "stated", { type: "Paper", name: "paper_doi" } )
            }
        }
        "#;

        let result = parse_kml_statement(input);
        assert!(result.is_ok());

        let (_, statement) = result.unwrap();
        match statement {
            KmlStatement::Upsert(upserts) => {
                assert_eq!(upserts.len(), 1);
                let upsert = &upserts[0];
                assert_eq!(upsert.items.len(), 1);

                match &upsert.items[0] {
                    UpsertItem::Proposition(prop) => {
                        assert_eq!(prop.handle, None);
                    }
                    _ => panic!("Expected PropositionBlock"),
                }
            }
            _ => panic!("Expected UpsertBlock"),
        }
    }

    #[test]
    fn test_parse_minimal_concept() {
        let input = r#"
        UPSERT {
            CONCEPT ?minimal {
                { id: "test_001" }
            }
        }
        "#;

        let result = parse_kml_statement(input);
        assert!(result.is_ok());

        let (_, statement) = result.unwrap();
        match statement {
            KmlStatement::Upsert(upserts) => {
                assert_eq!(upserts.len(), 1);
                let upsert = &upserts[0];
                assert_eq!(upsert.items.len(), 1);
                match &upsert.items[0] {
                    UpsertItem::Concept(concept) => {
                        assert_eq!(concept.handle, Some("minimal".to_string()));
                        assert_eq!(concept.concept, ConceptMatcher::ID("test_001".to_string()));
                        assert!(concept.set_attributes.is_none());
                        assert!(concept.set_propositions.is_none());
                        assert!(concept.metadata.is_none());
                    }
                    _ => panic!("Expected ConceptBlock"),
                }
            }
            _ => panic!("Expected UpsertBlock"),
        }
    }

    #[test]
    fn test_parse_concept_block_without_handle() {
        let input = r#"
        UPSERT {
            CONCEPT {
                { id: "test_001" }
            }
        }
        "#;

        let result = parse_kml_statement(input);
        assert!(result.is_ok());

        let (_, statement) = result.unwrap();
        match statement {
            KmlStatement::Upsert(upserts) => {
                assert_eq!(upserts.len(), 1);
                let upsert = &upserts[0];
                assert_eq!(upsert.items.len(), 1);
                match &upsert.items[0] {
                    UpsertItem::Concept(concept) => {
                        assert_eq!(concept.handle, None);
                        assert_eq!(concept.concept, ConceptMatcher::ID("test_001".to_string()));
                    }
                    _ => panic!("Expected ConceptBlock"),
                }
            }
            _ => panic!("Expected UpsertBlock"),
        }
    }

    #[test]
    fn test_parse_error_cases() {
        // Missing DETACH in DELETE CONCEPT
        let input1 = r#"
        DELETE CONCEPT
        { type: "Drug", name: "Test" }
        "#;
        assert!(parse_kml_statement(input1).is_err());

        // Invalid local handle (missing ?)
        let input2 = r#"
        UPSERT {
            CONCEPT drug {
                { id: "test" }
            }
        }
        "#;
        assert!(parse_kml_statement(input2).is_err());

        // Missing concept clause in concept
        let input3 = r#"
        UPSERT {
            CONCEPT ?drug {
                SET ATTRIBUTES { name: "Test" }
            }
        }
        "#;
        assert!(parse_kml_statement(input3).is_err());
    }

    #[test]
    fn test_keywords_accept_arbitrary_whitespace() {
        // Newlines/tabs separating multi-word keywords (WITH METADATA, SET ATTRIBUTES,
        // SET PROPOSITIONS) must parse identically to a literal space.
        let input = "UPSERT {\n  CONCEPT ?d {\n    {type: \"Drug\", name: \"X\"}\n    SET\n    ATTRIBUTES { a: 1 }\n    SET\nPROPOSITIONS {\n      (\"is_a\", {type: \"DrugClass\", name: \"NSAID\"})\n    }\n  }\n  WITH\n\tMETADATA { source: \"t\" }\n}\nWITH\n  METADATA { author: \"u\" }\n";
        let result = parse_kml_statement(input).expect("parses with newlines between keywords");
        match result.1 {
            KmlStatement::Upsert(blocks) => {
                assert_eq!(blocks.len(), 1);
                assert!(blocks[0].metadata.is_some());
                assert_eq!(blocks[0].items.len(), 1);
            }
            _ => panic!("expected UPSERT"),
        }
    }

    #[test]
    fn test_parse_upsert_with_expect_version() {
        let input = r#"
        UPSERT {
            CONCEPT ?self {
                {type: "Person", name: "$self"}
                EXPECT VERSION 42
                SET ATTRIBUTES { behavior_preferences: ["concise"] }
            }
            PROPOSITION ?fact {
                (?self, "prefers", {type: "Preference", name: "concise_style"})
                EXPECT VERSION 0
                SET ATTRIBUTES { note: "create-only" }
            }
        }
        WITH METADATA { source: "test", author: "$self", confidence: 1.0 }
        "#;

        let (_, statement) = parse_kml_statement(input).unwrap();
        match statement {
            KmlStatement::Upsert(blocks) => {
                assert_eq!(blocks.len(), 1);
                match &blocks[0].items[0] {
                    UpsertItem::Concept(concept) => {
                        assert_eq!(concept.expect_version, Some(42));
                        assert!(concept.set_attributes.is_some());
                    }
                    _ => panic!("Expected ConceptBlock"),
                }
                match &blocks[0].items[1] {
                    UpsertItem::Proposition(prop) => {
                        assert_eq!(prop.expect_version, Some(0));
                        assert!(prop.set_attributes.is_some());
                    }
                    _ => panic!("Expected PropositionBlock"),
                }
            }
            _ => panic!("Expected UpsertBlock"),
        }

        // EXPECT VERSION must be a non-negative integer
        let invalid = r#"
        UPSERT {
            CONCEPT ?self {
                {type: "Person", name: "$self"}
                EXPECT VERSION -1
            }
        }
        "#;
        assert!(parse_kml_statement(invalid).is_err());
    }

    #[test]
    fn test_parse_update_statement() {
        // The memory-metabolism workhorse from the spec: confidence decay.
        let input = r#"
        UPDATE ?link
        SET METADATA {
            confidence: CLAMP(MUL(?link.metadata.confidence, 0.9), 0.0, 1.0),
            decay_applied_at: "2026-06-11T00:00:00Z"
        }
        WHERE {
            ?link (?s, ?p, ?o)
            FILTER(IS_NULL(?link.metadata.superseded) || ?link.metadata.superseded != true)
            FILTER(?link.metadata.created_at < "2026-05-11T00:00:00Z" && ?link.metadata.confidence > 0.3)
        }
        LIMIT 500
        "#;

        let (_, statement) = parse_kml_statement(input).unwrap();
        match statement {
            KmlStatement::Update(update) => {
                assert_eq!(update.target, "link");
                assert!(update.set_attributes.is_none());
                assert_eq!(update.limit, Some(500));
                assert_eq!(update.where_clauses.len(), 3);

                let metadata = update.set_metadata.as_ref().unwrap();
                assert_eq!(metadata.len(), 2);
                assert_eq!(metadata[0].0, "confidence");
                assert_eq!(
                    metadata[0].1,
                    UpdateValue::Expr(UpdateExpr::Function {
                        func: UpdateFunction::Clamp,
                        args: vec![
                            UpdateExpr::Function {
                                func: UpdateFunction::Mul,
                                args: vec![
                                    UpdateExpr::Variable(DotPathVar {
                                        var: "link".to_string(),
                                        path: vec![
                                            "metadata".to_string(),
                                            "confidence".to_string()
                                        ],
                                    }),
                                    UpdateExpr::Number(Number::from_f64(0.9).unwrap()),
                                ],
                            },
                            UpdateExpr::Number(Number::from_f64(0.0).unwrap()),
                            UpdateExpr::Number(Number::from_f64(1.0).unwrap()),
                        ],
                    })
                );
                assert_eq!(metadata[1].0, "decay_applied_at");
                assert_eq!(
                    metadata[1].1,
                    UpdateValue::Json(Json::String("2026-06-11T00:00:00Z".to_string()))
                );
            }
            _ => panic!("Expected UpdateStatement"),
        }
    }

    #[test]
    fn test_parse_update_statement_reinforcement() {
        // Reinforce without read-modify-write: ADD + COALESCE, both SET blocks.
        let input = r#"
        UPDATE ?pref
        SET ATTRIBUTES {
            evidence_count: ADD(COALESCE(?pref.attributes.evidence_count, 0), 1),
            last_observed: "2026-06-11T00:00:00Z"
        }
        SET METADATA { observed_at: "2026-06-11T00:00:00Z" }
        WHERE {
            ?pref {type: "Preference", name: "concise_style"}
        }
        "#;

        let (_, statement) = parse_kml_statement(input).unwrap();
        match statement {
            KmlStatement::Update(update) => {
                assert_eq!(update.target, "pref");
                assert_eq!(update.limit, None);

                let attrs = update.set_attributes.as_ref().unwrap();
                assert_eq!(attrs.len(), 2);
                assert_eq!(attrs[0].0, "evidence_count");
                assert_eq!(
                    attrs[0].1,
                    UpdateValue::Expr(UpdateExpr::Function {
                        func: UpdateFunction::Add,
                        args: vec![
                            UpdateExpr::Function {
                                func: UpdateFunction::Coalesce,
                                args: vec![
                                    UpdateExpr::Variable(DotPathVar {
                                        var: "pref".to_string(),
                                        path: vec![
                                            "attributes".to_string(),
                                            "evidence_count".to_string()
                                        ],
                                    }),
                                    UpdateExpr::Number(Number::from(0)),
                                ],
                            },
                            UpdateExpr::Number(Number::from(1)),
                        ],
                    })
                );

                let metadata = update.set_metadata.as_ref().unwrap();
                assert_eq!(metadata.len(), 1);
                assert_eq!(metadata[0].0, "observed_at");
            }
            _ => panic!("Expected UpdateStatement"),
        }
    }

    #[test]
    fn test_parse_update_error_cases() {
        // No SET block at all
        let no_set = r#"
        UPDATE ?link
        WHERE { ?link (?s, ?p, ?o) }
        "#;
        assert!(parse_kml_statement(no_set).is_err());

        // Missing WHERE block
        let no_where = r#"
        UPDATE ?link
        SET METADATA { confidence: 0.5 }
        "#;
        assert!(parse_kml_statement(no_where).is_err());

        // Wrong arity: CLAMP requires 3 arguments
        let bad_arity = r#"
        UPDATE ?link
        SET METADATA { confidence: CLAMP(0.5, 1.0) }
        WHERE { ?link (?s, ?p, ?o) }
        "#;
        assert!(parse_kml_statement(bad_arity).is_err());

        // Wrong arity: ADD requires 2 arguments
        let bad_add = r#"
        UPDATE ?link
        SET METADATA { confidence: ADD(1) }
        WHERE { ?link (?s, ?p, ?o) }
        "#;
        assert!(parse_kml_statement(bad_add).is_err());
    }

    #[test]
    fn test_parse_merge_statement() {
        let input = r#"
        MERGE CONCEPT ?dup INTO ?canonical
        WHERE {
            ?dup {type: "SkillTopic", name: "JS"}
            ?canonical {type: "SkillTopic", name: "JavaScript"}
        }
        "#;

        let (_, statement) = parse_kml_statement(input).unwrap();
        match statement {
            KmlStatement::Merge(merge) => {
                assert_eq!(merge.source, "dup");
                assert_eq!(merge.target, "canonical");
                assert_eq!(merge.where_clauses.len(), 2);
                assert_eq!(
                    merge.where_clauses[0],
                    WhereClause::Concept(ConceptClause {
                        matcher: ConceptMatcher::Object {
                            r#type: "SkillTopic".to_string(),
                            name: "JS".to_string(),
                        },
                        variable: "dup".to_string(),
                    })
                );
            }
            _ => panic!("Expected MergeStatement"),
        }

        // MERGE without INTO is invalid
        assert!(
            parse_kml_statement(r#"MERGE CONCEPT ?dup WHERE { ?dup {type: "T", name: "x"} }"#)
                .is_err()
        );
        // MERGE without WHERE is invalid
        assert!(parse_kml_statement("MERGE CONCEPT ?dup INTO ?canonical").is_err());
    }
}
