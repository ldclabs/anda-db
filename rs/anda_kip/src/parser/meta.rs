//! META — introspection, grounding, verification, history and export
//! (Spec §63–§69).
//!
//! META is semantically read-only: syntax acceptance never implies access, and
//! nothing here can change durable state.

use nom::{
    Parser,
    branch::alt,
    combinator::{cut, map, opt, value},
    sequence::preceded,
};

use super::common::{
    Flavor, VResult, bound_object, element_ref, fail, opt_after, scalar, where_block, word, words,
    ws,
};
use super::kql::as_of_clause;
use crate::ast::{
    ChangesCommand, DescribeTarget, ExportCapsuleCommand, HistoryCommand, ListCommand, ListTarget,
    MetaCommand, PreviewCommand, Scalar, SearchCommand, SearchTarget, ValidateCommand,
    ValidateTarget, VerifyTarget,
};

/// Parses one META command.
pub fn parse_meta_command(input: &str) -> VResult<'_, MetaCommand> {
    alt((
        map(describe, MetaCommand::Describe),
        map(list, MetaCommand::List),
        map(search, MetaCommand::Search),
        verify,
        map(validate, MetaCommand::Validate),
        map(preview, MetaCommand::Preview),
        map(history, MetaCommand::History),
        map(changes, MetaCommand::Changes),
        snapshot,
        map(export_capsule, MetaCommand::ExportCapsule),
    ))
    .parse(input)
}

// ---------------------------------------------------------------------------
// DESCRIBE
// ---------------------------------------------------------------------------

fn describe(input: &str) -> VResult<'_, DescribeTarget> {
    let (input, _) = ws(word("DESCRIBE")).parse(input)?;
    cut(alt((
        // Multi-word targets first: `SCHEMA ENVIRONMENT` and `STRUCTURAL FIELD`
        // would otherwise be read as a one-word target with a stray operand.
        map(
            preceded(ws(words(&["SCHEMA", "ENVIRONMENT"])), opt(ws(as_of_clause))),
            |as_of| DescribeTarget::SchemaEnvironment { as_of },
        ),
        map(
            preceded(ws(words(&["EXECUTION", "CONTEXT"])), nothing),
            |_| DescribeTarget::ExecutionContext,
        ),
        map(
            preceded(ws(words(&["STRUCTURAL", "FIELD"])), cut(ws(scalar))),
            DescribeTarget::StructuralField,
        ),
        map(
            preceded(ws(words(&["EPISTEMIC", "POLICY"])), opt(ws(scalar))),
            |value| DescribeTarget::EpistemicPolicy { value },
        ),
        map(
            preceded(ws(words(&["PROJECTION", "CAPABILITY"])), nothing),
            |_| DescribeTarget::ProjectionCapability,
        ),
        map(
            preceded(ws(word("PRIMER")), opt_after(&["MODE"], ws(scalar))),
            |mode| DescribeTarget::Primer { mode },
        ),
        value(DescribeTarget::Protocol, ws(word("PROTOCOL"))),
        value(DescribeTarget::Capabilities, ws(word("CAPABILITIES"))),
        map(preceded(ws(word("SPACE")), opt(ws(scalar))), |value| {
            DescribeTarget::Space { value }
        }),
        map(
            preceded(ws(word("PACKAGE")), cut(ws(scalar))),
            DescribeTarget::Package,
        ),
        map(
            preceded(ws(word("TYPE")), cut(ws(scalar))),
            DescribeTarget::Type,
        ),
        map(
            preceded(ws(word("PREDICATE")), cut(ws(scalar))),
            DescribeTarget::Predicate,
        ),
        map(
            preceded(ws(word("FACET")), cut(ws(scalar))),
            DescribeTarget::Facet,
        ),
        map(
            preceded(
                ws(word("COMPATIBILITY")),
                cut((
                    preceded(ws(word("FROM")), cut(ws(scalar))),
                    preceded(ws(word("TO")), cut(ws(scalar))),
                )),
            ),
            |(from, to)| DescribeTarget::Compatibility { from, to },
        ),
        map(
            preceded(ws(word("ERROR")), cut(ws(scalar))),
            DescribeTarget::Error,
        ),
        describe_transaction,
        map(
            preceded(ws(word("SNAPSHOT")), opt(ws(as_of_clause))),
            |as_of| DescribeTarget::Snapshot { as_of },
        ),
        map(
            preceded(ws(word("CAPSULE")), cut(ws(scalar))),
            DescribeTarget::Capsule,
        ),
        map(preceded(ws(word("TRUST")), opt(ws(scalar))), |value| {
            DescribeTarget::Trust { value }
        }),
        map(
            preceded(ws(word("ACCESS")), opt_after(&["WITH"], ws(bound_object))),
            |with| DescribeTarget::Access { with },
        ),
    )))
    .parse(input)
}

fn describe_transaction(input: &str) -> VResult<'_, DescribeTarget> {
    let (input, _) = ws(word("TRANSACTION")).parse(input)?;
    cut(alt((
        map(
            preceded(ws(words(&["BY", "IDEMPOTENCY", "KEY"])), cut(ws(scalar))),
            DescribeTarget::TransactionByIdempotencyKey,
        ),
        map(ws(scalar), DescribeTarget::Transaction),
    )))
    .parse(input)
}

/// A target that takes no operand still has to consume nothing successfully.
fn nothing(input: &str) -> VResult<'_, ()> {
    Ok((input, ()))
}

// ---------------------------------------------------------------------------
// LIST
// ---------------------------------------------------------------------------

fn list(input: &str) -> VResult<'_, ListCommand> {
    let (input, _) = ws(word("LIST")).parse(input)?;
    let (input, (target, status)) = cut(alt((
        map(
            preceded(
                ws(words(&["SCHEMA", "PACKAGES"])),
                opt_after(&["STATUS"], ws(scalar)),
            ),
            |status| (ListTarget::SchemaPackages, status),
        ),
        map(ws(words(&["STRUCTURAL", "FIELDS"])), |_| {
            (ListTarget::StructuralFields, None)
        }),
        map(ws(words(&["EPISTEMIC", "POLICIES"])), |_| {
            (ListTarget::EpistemicPolicies, None)
        }),
        map(ws(word("SPACES")), |_| (ListTarget::Spaces, None)),
        map(ws(word("TYPES")), |_| (ListTarget::Types, None)),
        map(ws(word("PREDICATES")), |_| (ListTarget::Predicates, None)),
        map(ws(word("FACETS")), |_| (ListTarget::Facets, None)),
    )))
    .parse(input)?;

    let (input, (limit, cursor)) = paging(input)?;
    Ok((
        input,
        ListCommand {
            target,
            status,
            limit,
            cursor,
        },
    ))
}

/// `paging_clauses = LIMIT v [ CURSOR v ] | CURSOR v`
fn paging(input: &str) -> VResult<'_, (Option<Scalar>, Option<Scalar>)> {
    let (input, limit) = opt_after(&["LIMIT"], ws(scalar)).parse(input)?;
    let (input, cursor) = opt_after(&["CURSOR"], ws(scalar)).parse(input)?;
    Ok((input, (limit, cursor)))
}

// ---------------------------------------------------------------------------
// SEARCH
// ---------------------------------------------------------------------------

fn search(input: &str) -> VResult<'_, SearchCommand> {
    let (input, _) = ws(word("SEARCH")).parse(input)?;
    let (input, target) = cut(ws(alt((
        value(SearchTarget::Concept, word("CONCEPT")),
        value(SearchTarget::Proposition, word("PROPOSITION")),
        value(SearchTarget::Assertion, word("ASSERTION")),
        value(SearchTarget::Evidence, word("EVIDENCE")),
        value(SearchTarget::Activity, word("ACTIVITY")),
        value(SearchTarget::Cognition, word("COGNITION")),
    ))))
    .parse(input)?;

    let (input, term) = cut(ws(scalar)).parse(input)?;
    let (input, with_type) = opt_after(&["WITH", "TYPE"], ws(scalar)).parse(input)?;
    let (input, with_predicate) = opt_after(&["WITH", "PREDICATE"], ws(scalar)).parse(input)?;
    let (input, mode) = opt_after(&["MODE"], ws(scalar)).parse(input)?;
    let (input, threshold) = opt_after(&["THRESHOLD"], ws(scalar)).parse(input)?;
    let (input, as_of_seq) = opt_after(&["AS", "OF", "SEQ"], ws(scalar)).parse(input)?;
    let (input, (limit, cursor)) = paging(input)?;

    Ok((
        input,
        SearchCommand {
            target,
            term,
            with_type,
            with_predicate,
            mode,
            threshold,
            as_of_seq,
            limit,
            cursor,
        },
    ))
}

// ---------------------------------------------------------------------------
// VERIFY / VALIDATE / PREVIEW
// ---------------------------------------------------------------------------

fn verify(input: &str) -> VResult<'_, MetaCommand> {
    let (input, _) = ws(word("VERIFY")).parse(input)?;
    let (input, target) = cut(ws(alt((
        value(VerifyTarget::SchemaPackage, words(&["SCHEMA", "PACKAGE"])),
        value(VerifyTarget::Capsule, map(word("CAPSULE"), |_| ())),
        value(VerifyTarget::Receipt, map(word("RECEIPT"), |_| ())),
        value(VerifyTarget::Blob, map(word("BLOB"), |_| ())),
        value(VerifyTarget::Checkpoint, map(word("CHECKPOINT"), |_| ())),
    ))))
    .parse(input)?;
    let (input, value) = cut(ws(scalar)).parse(input)?;
    Ok((input, MetaCommand::Verify { target, value }))
}

fn validate(input: &str) -> VResult<'_, ValidateCommand> {
    let (input, _) = ws(word("VALIDATE")).parse(input)?;
    let (input, target) = cut(ws(alt((
        value(ValidateTarget::SchemaPackage, words(&["SCHEMA", "PACKAGE"])),
        value(ValidateTarget::ImportPlan, words(&["IMPORT", "PLAN"])),
        value(ValidateTarget::Kql, map(word("KQL"), |_| ())),
        value(ValidateTarget::Kml, map(word("KML"), |_| ())),
        value(ValidateTarget::Capsule, map(word("CAPSULE"), |_| ())),
    ))))
    .parse(input)?;
    let (input, value) = cut(ws(scalar)).parse(input)?;
    let (input, options) = opt_after(&["WITH"], ws(bound_object)).parse(input)?;
    Ok((
        input,
        ValidateCommand {
            target,
            value,
            options,
        },
    ))
}

fn preview(input: &str) -> VResult<'_, PreviewCommand> {
    let (input, _) = ws(word("PREVIEW")).parse(input)?;
    cut(alt((
        map(
            preceded(
                ws(words(&["IMPORT", "CAPSULE"])),
                cut((ws(scalar), preceded(ws(word("INTO")), cut(ws(scalar))))),
            ),
            |(capsule, into)| PreviewCommand::ImportCapsule { capsule, into },
        ),
        map(
            preceded(ws(word("KML")), cut(ws(scalar))),
            PreviewCommand::Kml,
        ),
    )))
    .parse(input)
}

// ---------------------------------------------------------------------------
// HISTORY / CHANGES / SNAPSHOT
// ---------------------------------------------------------------------------

fn history(input: &str) -> VResult<'_, HistoryCommand> {
    let (input, _) = ws(word("HISTORY")).parse(input)?;

    if let Ok((input, _)) = ws(word("SPACE")).parse(input) {
        let (input, (from_seq, to_seq, limit, cursor)) = history_range(input)?;
        return Ok((
            input,
            HistoryCommand::Space {
                from_seq,
                to_seq,
                limit,
                cursor,
            },
        ));
    }

    let (input, _) = cut(ws(word("ELEMENT"))).parse(input)?;
    let (input, value) = cut(ws(scalar)).parse(input)?;
    let (input, (from_seq, to_seq, limit, cursor)) = history_range(input)?;
    Ok((
        input,
        HistoryCommand::Element {
            value,
            from_seq,
            to_seq,
            limit,
            cursor,
        },
    ))
}

type HistoryRange = (
    Option<Scalar>,
    Option<Scalar>,
    Option<Scalar>,
    Option<Scalar>,
);

fn history_range(input: &str) -> VResult<'_, HistoryRange> {
    let (input, from_seq) = opt_after(&["FROM", "SEQ"], ws(scalar)).parse(input)?;
    let (input, to_seq) = opt_after(&["TO", "SEQ"], ws(scalar)).parse(input)?;
    let (input, (limit, cursor)) = paging(input)?;
    Ok((input, (from_seq, to_seq, limit, cursor)))
}

fn changes(input: &str) -> VResult<'_, ChangesCommand> {
    let (input, _) = ws(word("CHANGES")).parse(input)?;
    cut(alt((
        map(
            (
                preceded(ws(words(&["AFTER", "SEQ"])), cut(ws(scalar))),
                opt_after(&["LIMIT"], ws(scalar)),
            ),
            |(seq, limit)| ChangesCommand::AfterSeq { seq, limit },
        ),
        map(
            (
                preceded(ws(word("SINCE")), cut(ws(scalar))),
                opt_after(&["LIMIT"], ws(scalar)),
            ),
            |(cursor, limit)| ChangesCommand::Since { cursor, limit },
        ),
    )))
    .parse(input)
}

fn snapshot(input: &str) -> VResult<'_, MetaCommand> {
    let (input, _) = ws(word("SNAPSHOT")).parse(input)?;
    let (input, as_of) = opt(ws(as_of_clause)).parse(input)?;
    Ok((input, MetaCommand::Snapshot { as_of }))
}

// ---------------------------------------------------------------------------
// EXPORT CAPSULE
// ---------------------------------------------------------------------------

fn export_capsule(input: &str) -> VResult<'_, ExportCapsuleCommand> {
    let (input, _) = ws(words(&["EXPORT", "CAPSULE"])).parse(input)?;
    let (input, target) = cut(ws(element_ref)).parse(input)?;
    let (input, _) = cut(ws(word("WHERE"))).parse(input)?;
    // BELIEF stays an interpretation primitive, not an export selector, so the
    // selection uses the raw pattern flavor.
    let (input, where_clauses) = cut(|i| where_block(i, Flavor::Exact)).parse(input)?;
    let (input, options) = opt_after(&["WITH"], ws(bound_object)).parse(input)?;
    let (input, as_of) = opt(ws(as_of_clause)).parse(input)?;

    if where_clauses.is_empty() {
        return fail(
            input,
            "at least one selection pattern: an unbounded EXPORT is not a Capsule",
        );
    }

    Ok((
        input,
        ExportCapsuleCommand {
            target,
            where_clauses,
            options,
            as_of,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::AsOf;

    fn meta(input: &str) -> MetaCommand {
        let (rest, command) =
            parse_meta_command(input).unwrap_or_else(|e| panic!("failed to parse {input:?}: {e}"));
        assert!(rest.trim().is_empty(), "unconsumed input {rest:?}");
        command
    }

    #[test]
    fn describes_every_operandless_target() {
        assert_eq!(
            meta("DESCRIBE PROTOCOL"),
            MetaCommand::Describe(DescribeTarget::Protocol)
        );
        assert_eq!(
            meta("DESCRIBE EXECUTION CONTEXT"),
            MetaCommand::Describe(DescribeTarget::ExecutionContext)
        );
        assert_eq!(
            meta("DESCRIBE CAPABILITIES"),
            MetaCommand::Describe(DescribeTarget::Capabilities)
        );
        assert_eq!(
            meta("DESCRIBE PROJECTION CAPABILITY"),
            MetaCommand::Describe(DescribeTarget::ProjectionCapability)
        );
    }

    #[test]
    fn optional_operands_stay_optional() {
        assert_eq!(
            meta("DESCRIBE SPACE"),
            MetaCommand::Describe(DescribeTarget::Space { value: None })
        );
        assert!(matches!(
            meta(r#"DESCRIBE SPACE "space-1""#),
            MetaCommand::Describe(DescribeTarget::Space { value: Some(_) })
        ));
        assert!(matches!(
            meta("DESCRIBE TRUST"),
            MetaCommand::Describe(DescribeTarget::Trust { value: None })
        ));
        assert!(matches!(
            meta("DESCRIBE EPISTEMIC POLICY"),
            MetaCommand::Describe(DescribeTarget::EpistemicPolicy { value: None })
        ));
    }

    #[test]
    fn multi_word_targets_win_over_their_prefixes() {
        assert!(matches!(
            meta("DESCRIBE SCHEMA ENVIRONMENT AS OF SEQ 42"),
            MetaCommand::Describe(DescribeTarget::SchemaEnvironment {
                as_of: Some(AsOf::Seq(_))
            })
        ));
        assert!(matches!(
            meta(r#"DESCRIBE STRUCTURAL FIELD "has_step""#),
            MetaCommand::Describe(DescribeTarget::StructuralField(_))
        ));
    }

    #[test]
    fn a_transaction_is_named_directly_or_by_idempotency_key() {
        assert!(matches!(
            meta(r#"DESCRIBE TRANSACTION "tx-1""#),
            MetaCommand::Describe(DescribeTarget::Transaction(_))
        ));
        assert!(matches!(
            meta(r#"DESCRIBE TRANSACTION BY IDEMPOTENCY KEY "write-1""#),
            MetaCommand::Describe(DescribeTarget::TransactionByIdempotencyKey(_))
        ));
    }

    #[test]
    fn describe_access_takes_an_input_block() {
        let command = meta(r#"DESCRIBE ACCESS WITH { operation: "update", purpose: :why }"#);
        let MetaCommand::Describe(DescribeTarget::Access { with: Some(with) }) = command else {
            panic!("expected DESCRIBE ACCESS WITH");
        };
        assert_eq!(with.len(), 2);
    }

    #[test]
    fn list_paging_accepts_either_spelling() {
        let MetaCommand::List(command) = meta("LIST SCHEMA PACKAGES STATUS :s LIMIT 10 CURSOR :c")
        else {
            panic!("expected LIST");
        };
        assert_eq!(command.target, ListTarget::SchemaPackages);
        assert!(command.status.is_some());
        assert!(command.limit.is_some());
        assert!(command.cursor.is_some());

        let MetaCommand::List(cursor_only) = meta("LIST TYPES CURSOR :c") else {
            panic!("expected LIST");
        };
        assert!(cursor_only.limit.is_none());
        assert!(cursor_only.cursor.is_some());
    }

    #[test]
    fn search_carries_its_whole_option_set() {
        let MetaCommand::Search(command) = meta(
            r#"SEARCH COGNITION "dark mode" WITH TYPE "Preference" WITH PREDICATE "prefers"
               MODE "hybrid" THRESHOLD 0.7 AS OF SEQ 100 LIMIT 5 CURSOR :c"#,
        ) else {
            panic!("expected SEARCH");
        };
        assert_eq!(command.target, SearchTarget::Cognition);
        assert!(command.with_type.is_some());
        assert!(command.with_predicate.is_some());
        assert!(command.mode.is_some());
        assert!(command.threshold.is_some());
        assert!(command.as_of_seq.is_some());
    }

    #[test]
    fn verify_and_validate_keep_their_two_word_targets() {
        assert!(matches!(
            meta(r#"VERIFY SCHEMA PACKAGE :pkg"#),
            MetaCommand::Verify {
                target: VerifyTarget::SchemaPackage,
                ..
            }
        ));
        let MetaCommand::Validate(command) = meta(r#"VALIDATE IMPORT PLAN :plan"#) else {
            panic!("expected VALIDATE");
        };
        assert_eq!(command.target, ValidateTarget::ImportPlan);

        let MetaCommand::Validate(with_options) =
            meta(r#"VALIDATE KML :cmd WITH { strict: true }"#)
        else {
            panic!("expected VALIDATE");
        };
        assert!(with_options.options.is_some());
    }

    #[test]
    fn preview_is_frozen_to_its_two_operand_forms() {
        assert!(matches!(
            meta(r#"PREVIEW KML :cmd"#),
            MetaCommand::Preview(PreviewCommand::Kml(_))
        ));
        assert!(matches!(
            meta(r#"PREVIEW IMPORT CAPSULE :c INTO "space-1""#),
            MetaCommand::Preview(PreviewCommand::ImportCapsule { .. })
        ));
        assert!(parse_meta_command(r#"PREVIEW MERGE :a INTO :b"#).is_err());
    }

    #[test]
    fn history_and_changes_page_independently() {
        assert!(matches!(
            meta(r#"HISTORY ELEMENT "C-1" FROM SEQ 1 TO SEQ 9 LIMIT 5"#),
            MetaCommand::History(HistoryCommand::Element { .. })
        ));
        assert!(matches!(
            meta("HISTORY SPACE LIMIT 5"),
            MetaCommand::History(HistoryCommand::Space { .. })
        ));
        assert!(matches!(
            meta("CHANGES SINCE :cursor LIMIT 100"),
            MetaCommand::Changes(ChangesCommand::Since { .. })
        ));
        assert!(matches!(
            meta("CHANGES AFTER SEQ 42"),
            MetaCommand::Changes(ChangesCommand::AfterSeq { .. })
        ));
    }

    #[test]
    fn snapshot_takes_the_shared_history_coordinate() {
        assert!(matches!(
            meta("SNAPSHOT"),
            MetaCommand::Snapshot { as_of: None }
        ));
        assert!(matches!(
            meta(r#"SNAPSHOT AS OF TIME "2026-01-01T00:00:00Z""#),
            MetaCommand::Snapshot {
                as_of: Some(AsOf::Time(_))
            }
        ));
    }

    #[test]
    fn export_selects_raw_state_and_never_belief() {
        let MetaCommand::ExportCapsule(command) = meta(
            r#"EXPORT CAPSULE :out WHERE { ?c CONCEPT {type: "Experience"} } WITH { redact: true } AS OF SEQ 7"#,
        ) else {
            panic!("expected EXPORT CAPSULE");
        };
        assert_eq!(command.where_clauses.len(), 1);
        assert!(command.options.is_some());
        assert!(command.as_of.is_some());

        assert!(parse_meta_command(r#"EXPORT CAPSULE :out WHERE { ?b BELIEF (?p) }"#).is_err());
        assert!(parse_meta_command(r#"EXPORT CAPSULE :out WHERE { }"#).is_err());
    }

    #[test]
    fn meta_keywords_are_case_insensitive() {
        assert_eq!(
            meta("describe primer mode \"compact\""),
            MetaCommand::Describe(DescribeTarget::Primer {
                mode: Some(Scalar::Literal(crate::ast::KipValue::String(
                    "compact".into()
                )))
            })
        );
    }
}
