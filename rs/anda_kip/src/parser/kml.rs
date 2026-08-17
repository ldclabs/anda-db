//! KML — the Cognitive Mutation Language (Spec §51–§62).
//!
//! Two things happen here that a plain grammar would not do:
//!
//! - `ASSERT` is desugared into exactly what §55.1 defines it as — `ENSURE
//!   PROPOSITION` + `CREATE ASSERTION` (+ `SUPERSEDE`) — and nothing beyond
//!   those parts is fabricated;
//! - the mutation guards run, so a command that would ask an engine to rewrite
//!   immutable epistemic payload is rejected before it reaches one.

use nom::{
    Parser,
    branch::alt,
    character::complete::char,
    combinator::{cut, map, opt},
    multi::many0,
    sequence::preceded,
};
use std::collections::BTreeSet;

use super::common::{
    Flavor, VResult, assignments, braced, collect_bound_object_handles,
    collect_mutation_value_handles, collect_mutation_value_paths, collect_where_variables,
    element_ref, fail, handle, mutation_value, object_matcher, opt_after, parenthesized,
    proposition_matcher, quoted_string, scalar, spanned, symbol_ref, unset_field_set, where_block,
    word, words, ws,
};
use crate::ast::{
    Assignments, BoundValue, ConceptCreate, ConceptUpsert, CorrectEvidence, DotPathVar, ElementRef,
    EnsureProposition, FacetAssignment, FacetUnset, KipValue, KmlStatement, MatchValue,
    MergeConcept, MutationClause, MutationValue, ObjectMatcher, PredAtom, PropositionMatcher,
    PropositionTriple, RecordCreate, RemovalStatement, RetractAssertion, Scalar, SetRetention,
    StructuralEdge, StructuralRemoval, SupersedeAssertion, SymbolRef, Term, TransitionActivity,
    UpdateAction, UpdateExpr, UpdateStatement, WhereClause,
};
use crate::error::KipError;

/// Assertion payload that is immutable after creation (Spec §13.7).
const ASSERTION_IMMUTABLE: &[&str] = &[
    "proposition_id",
    "proposition",
    "asserted_by",
    "stance",
    "mode",
    "confidence",
    "asserted_at",
    "valid_time",
    "evidence",
    "evidence_refs",
];

/// Evidence payload and observation identity are immutable (Spec §15.5).
const EVIDENCE_IMMUTABLE: &[&str] = &[
    "evidence_class",
    "payload",
    "content_digest",
    "media_type",
    "observed_at",
];

/// A Proposition tuple is immutable after creation (Spec §12.5).
const PROPOSITION_IMMUTABLE: &[&str] = &["subject", "predicate", "object"];

/// The ASSERT members §55.1 defines.
const ASSERT_MEMBERS: &[&str] = &[
    "by",
    "mode",
    "stance",
    "confidence",
    "at",
    "valid",
    "evidence",
    "key",
];

/// Parses a KML statement: a `MUTATE` block, or a single mutation that is still
/// a one-clause transaction.
pub fn parse_kml_statement(input: &str) -> VResult<'_, KmlStatement> {
    if let Ok((rest, _)) = ws(word("MUTATE")).parse(input) {
        let (rest, groups) = cut(braced(many0(ws(spanned(mutation_clause))))).parse(rest)?;
        if groups.is_empty() {
            return fail(input, "at least one mutation inside MUTATE { ... }");
        }
        let clauses = flatten(groups);
        return Ok((
            rest,
            KmlStatement {
                explicit_transaction: true,
                clauses,
            },
        ));
    }

    let (rest, (position, group)) = ws(spanned(mutation_clause)).parse(input)?;
    Ok((
        rest,
        KmlStatement {
            explicit_transaction: false,
            clauses: flatten(vec![(position, group)]),
        },
    ))
}

/// One source statement may lower to several clauses; `ASSERT` is the case.
///
/// The clause's position in its plan keeps synthetic handles distinct between
/// two handle-less `ASSERT`s in the same transaction.
type ClauseGroup = Box<dyn Fn(usize) -> Vec<MutationClause>>;

fn flatten(groups: Vec<(&str, ClauseGroup)>) -> Vec<MutationClause> {
    groups
        .into_iter()
        .enumerate()
        .flat_map(|(seq, (_, build))| build(seq))
        .collect()
}

fn single(clause: MutationClause) -> ClauseGroup {
    Box::new(move |_| vec![clause.clone()])
}

fn mutation_clause(input: &str) -> VResult<'_, ClauseGroup> {
    alt((
        map(create_concept, |c| single(MutationClause::CreateConcept(c))),
        map(upsert_concept, |c| single(MutationClause::UpsertConcept(c))),
        map(ensure_proposition, |c| {
            single(MutationClause::EnsureProposition(c))
        }),
        assert_statement,
        map(create_record, |(kind, record)| {
            single(match kind {
                RecordKind::Evidence => MutationClause::CreateEvidence(record),
                RecordKind::Assertion => MutationClause::CreateAssertion(record),
                RecordKind::Activity => MutationClause::CreateActivity(record),
            })
        }),
        map(update_statement, |c| single(MutationClause::Update(c))),
        map(retract_assertion, |c| {
            single(MutationClause::RetractAssertion(c))
        }),
        map(supersede_assertion, |c| {
            single(MutationClause::SupersedeAssertion(c))
        }),
        map(correct_evidence, |c| {
            single(MutationClause::CorrectEvidence(c))
        }),
        map(transition_activity, |c| {
            single(MutationClause::TransitionActivity(c))
        }),
        map(set_retention, |c| single(MutationClause::SetRetention(c))),
        map(
            |i| removal("ARCHIVE", i),
            |c| single(MutationClause::Archive(c)),
        ),
        map(
            |i| removal("TOMBSTONE", i),
            |c| single(MutationClause::Tombstone(c)),
        ),
        map(purge_statement, |c| single(MutationClause::Purge(c))),
        map(merge_concept, |c| single(MutationClause::MergeConcept(c))),
    ))
    .parse(input)
}

// ---------------------------------------------------------------------------
// Shared mutation clauses
// ---------------------------------------------------------------------------

/// One clause of a `{ ... }` mutation body, before it is folded into a struct.
enum BodyClause {
    Type(SymbolRef),
    ClientKey(Scalar),
    Name(Scalar),
    Match(crate::ast::ObjectMatcher),
    ExpectVersion(Scalar),
    SetFields(Assignments),
    SetAttributes(Assignments),
    SetFacet(FacetAssignment),
    UnsetAttributes(Vec<String>),
    UnsetFacet(FacetUnset),
    SetStructural(Vec<StructuralEdge>),
    UnsetStructural(Vec<StructuralRemoval>),
}

fn body_clause(input: &str) -> VResult<'_, BodyClause> {
    alt((
        map(
            preceded(ws(words(&["CLIENT", "KEY"])), cut(ws(scalar))),
            BodyClause::ClientKey,
        ),
        map(
            preceded(ws(words(&["SET", "FIELDS"])), cut(ws(assignments))),
            BodyClause::SetFields,
        ),
        map(
            preceded(ws(words(&["SET", "ATTRIBUTES"])), cut(ws(assignments))),
            BodyClause::SetAttributes,
        ),
        map(
            preceded(
                ws(words(&["SET", "FACET"])),
                cut((ws(symbol_ref), ws(assignments))),
            ),
            |(facet, values)| BodyClause::SetFacet(FacetAssignment { facet, values }),
        ),
        map(
            preceded(ws(words(&["SET", "STRUCTURAL"])), cut(structural_edges)),
            BodyClause::SetStructural,
        ),
        map(
            preceded(
                ws(words(&["UNSET", "ATTRIBUTES"])),
                cut(ws(unset_field_set)),
            ),
            BodyClause::UnsetAttributes,
        ),
        map(
            preceded(
                ws(words(&["UNSET", "FACET"])),
                cut((ws(symbol_ref), ws(unset_field_set))),
            ),
            |(facet, fields)| BodyClause::UnsetFacet(FacetUnset { facet, fields }),
        ),
        map(
            preceded(
                ws(words(&["UNSET", "STRUCTURAL"])),
                cut(structural_removals),
            ),
            BodyClause::UnsetStructural,
        ),
        map(
            preceded(ws(words(&["EXPECT", "VERSION"])), cut(ws(scalar))),
            BodyClause::ExpectVersion,
        ),
        map(
            preceded(ws(word("TYPE")), cut(ws(symbol_ref))),
            BodyClause::Type,
        ),
        map(
            preceded(ws(word("NAME")), cut(ws(scalar))),
            BodyClause::Name,
        ),
        map(
            preceded(
                ws(word("MATCH")),
                cut(ws(|i| object_matcher(i, Flavor::Exact))),
            ),
            BodyClause::Match,
        ),
    ))
    .parse(input)
}

/// `SET STRUCTURAL { ("field", target) {options} ... }`
fn structural_edges(input: &str) -> VResult<'_, Vec<StructuralEdge>> {
    braced(many0(ws(map(
        (
            parenthesized((ws(symbol_ref), preceded(ws(char(',')), ws(mutation_value)))),
            opt(ws(super::common::bound_object)),
        ),
        |((field, value), options)| StructuralEdge {
            field,
            value,
            options,
        },
    ))))
    .parse(input)
}

/// `UNSET STRUCTURAL { ("field", target) ... }`
///
/// An entry is the `SET STRUCTURAL` entry without its options object: removal is
/// per reference, ordered fields re-densify, and cardinality is validated at
/// commit (Spec §17.5).
fn structural_removals(input: &str) -> VResult<'_, Vec<StructuralRemoval>> {
    let (rest, removals) = braced(many0(ws(map(
        parenthesized((ws(symbol_ref), preceded(ws(char(',')), ws(mutation_value)))),
        |(field, value)| StructuralRemoval { field, value },
    ))))
    .parse(input)?;
    if removals.is_empty() {
        return fail(
            input,
            "at least one (field, target) entry: UNSET STRUCTURAL removes named references",
        );
    }
    Ok((rest, removals))
}

/// Folds a body into typed slots, rejecting a second clause for a single slot.
struct Body {
    r#type: Option<SymbolRef>,
    client_key: Option<Scalar>,
    name: Option<Scalar>,
    r#match: Option<crate::ast::ObjectMatcher>,
    expect_version: Option<Scalar>,
    set_fields: Option<Assignments>,
    set_attributes: Option<Assignments>,
    set_facets: Vec<FacetAssignment>,
    unset_attributes: Option<Vec<String>>,
    unset_facets: Vec<FacetUnset>,
    set_structural: Option<Vec<StructuralEdge>>,
    unset_structural: Option<Vec<StructuralRemoval>>,
}

fn parse_body<'a>(input: &'a str, allowed: &'static [&'static str]) -> VResult<'a, Body> {
    let (rest, clauses) = braced(many0(ws(spanned(body_clause)))).parse(input)?;

    let mut body = Body {
        r#type: None,
        client_key: None,
        name: None,
        r#match: None,
        expect_version: None,
        set_fields: None,
        set_attributes: None,
        set_facets: Vec::new(),
        unset_attributes: None,
        unset_facets: Vec::new(),
        set_structural: None,
        unset_structural: None,
    };

    for (position, clause) in clauses {
        let (label, duplicate) = match clause {
            BodyClause::Type(v) => ("TYPE", body.r#type.replace(v).is_some()),
            BodyClause::ClientKey(v) => ("CLIENT KEY", body.client_key.replace(v).is_some()),
            BodyClause::Name(v) => ("NAME", body.name.replace(v).is_some()),
            BodyClause::Match(v) => ("MATCH", body.r#match.replace(v).is_some()),
            BodyClause::ExpectVersion(v) => {
                ("EXPECT VERSION", body.expect_version.replace(v).is_some())
            }
            BodyClause::SetFields(v) => ("SET FIELDS", body.set_fields.replace(v).is_some()),
            BodyClause::SetAttributes(v) => {
                ("SET ATTRIBUTES", body.set_attributes.replace(v).is_some())
            }
            BodyClause::SetFacet(v) => {
                body.set_facets.push(v);
                ("SET FACET", false)
            }
            BodyClause::UnsetAttributes(v) => (
                "UNSET ATTRIBUTES",
                body.unset_attributes.replace(v).is_some(),
            ),
            BodyClause::UnsetFacet(v) => {
                body.unset_facets.push(v);
                ("UNSET FACET", false)
            }
            BodyClause::SetStructural(v) => {
                ("SET STRUCTURAL", body.set_structural.replace(v).is_some())
            }
            BodyClause::UnsetStructural(v) => (
                "UNSET STRUCTURAL",
                body.unset_structural.replace(v).is_some(),
            ),
        };

        if duplicate {
            return fail(
                position,
                "at most one clause of this kind in a mutation body",
            );
        }
        if !allowed.contains(&label) {
            return fail(position, "a clause this mutation admits");
        }
    }

    Ok((rest, body))
}

// ---------------------------------------------------------------------------
// CREATE / UPSERT / ENSURE
// ---------------------------------------------------------------------------

const CONCEPT_CREATE_CLAUSES: &[&str] = &[
    "TYPE",
    "CLIENT KEY",
    "NAME",
    "SET FIELDS",
    "SET ATTRIBUTES",
    "SET FACET",
    "SET STRUCTURAL",
];

const CONCEPT_UPSERT_CLAUSES: &[&str] = &[
    "MATCH",
    "EXPECT VERSION",
    "SET FIELDS",
    "SET ATTRIBUTES",
    "SET FACET",
    "UNSET ATTRIBUTES",
    "UNSET FACET",
    "SET STRUCTURAL",
    "UNSET STRUCTURAL",
];

const RECORD_CREATE_CLAUSES: &[&str] = &["CLIENT KEY", "SET FIELDS", "SET FACET", "SET STRUCTURAL"];

fn create_concept(input: &str) -> VResult<'_, ConceptCreate> {
    let (input, _) = ws(words(&["CREATE", "CONCEPT"])).parse(input)?;
    let (input, handle) = cut(ws(handle)).parse(input)?;
    let (input, body) = cut(|i| parse_body(i, CONCEPT_CREATE_CLAUSES)).parse(input)?;

    Ok((
        input,
        ConceptCreate {
            handle,
            r#type: body.r#type,
            client_key: body.client_key,
            name: body.name,
            set_fields: body.set_fields,
            set_attributes: body.set_attributes,
            set_facets: body.set_facets,
            set_structural: body.set_structural,
        },
    ))
}

fn upsert_concept(input: &str) -> VResult<'_, ConceptUpsert> {
    let (input, _) = ws(words(&["UPSERT", "CONCEPT"])).parse(input)?;
    let (start, handle) = cut(ws(handle)).parse(input)?;
    let (rest, body) = cut(|i| parse_body(i, CONCEPT_UPSERT_CLAUSES)).parse(start)?;

    // Identity for an upsert is `id` or `key`. A name-only match is forbidden
    // because names are mutable grounding state with duplicates allowed, so
    // "the Concept named X" can silently address a different node over time.
    if body
        .r#match
        .as_ref()
        .is_none_or(|matcher| !upsert_has_stable_identity_selector(matcher))
    {
        return fail(
            start,
            "a required MATCH on a stable identity — {id: <literal-or-parameter>} or \
             {key: <literal-or-parameter>}; name is mutable grounding state and never identifies \
             a Concept",
        );
    }

    Ok((
        rest,
        ConceptUpsert {
            handle,
            r#match: body.r#match,
            expect_version: body.expect_version,
            set_fields: body.set_fields,
            set_attributes: body.set_attributes,
            set_facets: body.set_facets,
            unset_attributes: body.unset_attributes,
            unset_facets: body.unset_facets,
            set_structural: body.set_structural,
            unset_structural: body.unset_structural,
        },
    ))
}

fn upsert_has_stable_identity_selector(matcher: &ObjectMatcher) -> bool {
    ["id", "key"].iter().any(|field| {
        matches!(
            matcher.get(*field),
            Some(MatchValue::Literal(_) | MatchValue::Param(_))
        )
    })
}

enum RecordKind {
    Evidence,
    Assertion,
    Activity,
}

fn create_record(input: &str) -> VResult<'_, (RecordKind, RecordCreate)> {
    let (input, _) = ws(word("CREATE")).parse(input)?;
    let (input, kind) = ws(alt((
        map(word("EVIDENCE"), |_| RecordKind::Evidence),
        map(word("ASSERTION"), |_| RecordKind::Assertion),
        map(word("ACTIVITY"), |_| RecordKind::Activity),
    )))
    .parse(input)?;
    let (input, handle) = cut(ws(handle)).parse(input)?;
    let (input, body) = cut(|i| parse_body(i, RECORD_CREATE_CLAUSES)).parse(input)?;

    Ok((
        input,
        (
            kind,
            RecordCreate {
                handle,
                client_key: body.client_key,
                set_fields: body.set_fields,
                set_facets: body.set_facets,
                set_structural: body.set_structural,
            },
        ),
    ))
}

fn ensure_proposition(input: &str) -> VResult<'_, EnsureProposition> {
    let (input, _) = ws(words(&["ENSURE", "PROPOSITION"])).parse(input)?;
    let (input, handle) = opt(ws(handle)).parse(input)?;
    let (tuple_at, matcher) = cut(ws(|i| proposition_matcher(i, Flavor::Exact))).parse(input)?;
    let triple = match structural_tuple(matcher, input) {
        Ok(triple) => triple,
        Err(ctx) => return fail(input, ctx),
    };
    let (rest, expect_version) = opt_after(&["EXPECT", "VERSION"], ws(scalar)).parse(tuple_at)?;

    Ok((
        rest,
        EnsureProposition {
            handle,
            subject: triple.0,
            predicate: triple.1,
            object: triple.2,
            expect_version,
        },
    ))
}

/// Resolves the tuple a resolve-or-create statement needs.
///
/// `(id: ...)` is match-only: it names a Proposition that must already exist, so
/// it cannot drive `ENSURE PROPOSITION` — or the `ASSERT` sugar that desugars
/// through it — whose job is to create the tuple when it is absent.
fn structural_tuple(
    matcher: PropositionMatcher,
    _at: &str,
) -> Result<(Term, PredAtom, Term), &'static str> {
    let triple: PropositionTriple = match matcher {
        PropositionMatcher::Id(_) => {
            return Err(
                "a (subject, predicate, object) tuple: (id: ...) only matches an existing \
                 Proposition, and no structure can be created from an id",
            );
        }
        PropositionMatcher::Tuple(triple) => triple,
    };
    let atom = match triple.predicate {
        crate::ast::PredTerm::Atom(atom) => atom,
        crate::ast::PredTerm::Path(_) => {
            return Err(
                "one exact predicate: alternation and hop quantifiers are KQL traversal forms",
            );
        }
    };
    if matches!(atom, PredAtom::Variable(_)) {
        return Err(
            "an exact quoted predicate or :parameter; ?variables are KQL read-pattern syntax",
        );
    }
    Ok((triple.subject, atom, triple.object))
}

// ---------------------------------------------------------------------------
// ASSERT — normative sugar (Spec §55.1)
// ---------------------------------------------------------------------------

fn assert_statement(input: &str) -> VResult<'_, ClauseGroup> {
    let (input, _) = ws(word("ASSERT")).parse(input)?;
    let (input, written_handle) = opt(ws(handle)).parse(input)?;
    let (members_at, matcher) = cut(ws(|i| proposition_matcher(i, Flavor::Exact))).parse(input)?;
    let triple = match structural_tuple(matcher, input) {
        Ok(triple) => triple,
        Err(ctx) => return fail(input, ctx),
    };

    let (rest, members) = cut(ws(assignments)).parse(members_at)?;
    for (key, _) in &members {
        if !ASSERT_MEMBERS.contains(&key.as_str()) {
            return fail(
                members_at,
                "an ASSERT member: by, mode, stance, confidence, at, valid, evidence or key",
            );
        }
    }

    let lookup = |name: &str| members.iter().find(|(k, _)| k == name).map(|(_, v)| v);

    // `by` names whose stance this is, and `mode` says how it was arrived at.
    // Neither has a safe default: guessing the actor would forge attribution,
    // and guessing the mode would turn hearsay into observation.
    let Some(by) = lookup("by").cloned() else {
        return fail(
            members_at,
            "by: <semantic actor> — an Assertion without an assertor has no epistemic owner",
        );
    };
    let Some(mode) = lookup("mode").cloned() else {
        return fail(
            members_at,
            "mode: one of observed, stated, inferred, predicted, hypothetical or imported",
        );
    };

    let stance = lookup("stance")
        .cloned()
        .unwrap_or(MutationValue::Value(KipValue::String("support".into())));
    let confidence = lookup("confidence").cloned();
    let asserted_at = lookup("at").cloned();
    let valid_time = lookup("valid").cloned();
    let evidence = lookup("evidence").cloned();

    let client_key = match lookup("key") {
        Some(MutationValue::Param(name)) => Some(Scalar::Param(name.clone())),
        Some(MutationValue::Value(value @ (KipValue::String(_) | KipValue::Number(_)))) => {
            Some(Scalar::Literal(value.clone()))
        }
        Some(MutationValue::Value(value @ (KipValue::Bool(_) | KipValue::Null))) => {
            Some(Scalar::Literal(value.clone()))
        }
        Some(_) => {
            return fail(
                members_at,
                "a literal or :parameter for the ASSERT key member",
            );
        }
        None => None,
    };

    let (rest, superseding) = opt_after(&["SUPERSEDING"], ws(element_ref)).parse(rest)?;

    Ok((
        rest,
        Box::new(move |seq: usize| {
            // The Proposition handle is synthesized, so it must collide with
            // neither a user handle nor another ASSERT in the same plan. `#`
            // cannot occur in a KIP identifier, which rules out the first; the
            // clause position rules out the second.
            let assertion_handle = written_handle
                .clone()
                .unwrap_or_else(|| format!("#assert{seq}"));
            let proposition_handle = format!("{assertion_handle}#proposition");

            let mut set_fields: Assignments = vec![
                (
                    "proposition".into(),
                    MutationValue::Handle(proposition_handle.clone()),
                ),
                ("asserted_by".into(), by.clone()),
                ("mode".into(), mode.clone()),
                // The normative expansion carries a stance even when the source
                // omitted one, so the default is materialized here rather than
                // left for the engine to re-derive.
                ("stance".into(), stance.clone()),
            ];
            if let Some(value) = &confidence {
                set_fields.push(("confidence".into(), value.clone()));
            }
            if let Some(value) = &asserted_at {
                set_fields.push(("asserted_at".into(), value.clone()));
            }
            if let Some(value) = &valid_time {
                set_fields.push(("valid_time".into(), value.clone()));
            }

            // `evidence` is a reserved Core *structural* field, not a plain one:
            // the normative desugaring emits `("evidence", ref) {role: "support"}`.
            // An array cites several artifacts, so it becomes one role-qualified
            // edge each.
            let edges: Vec<StructuralEdge> = evidence
                .as_ref()
                .map(|value| {
                    evidence_refs(value)
                        .into_iter()
                        .map(|value| StructuralEdge {
                            field: SymbolRef::Name("evidence".into()),
                            value,
                            options: Some(
                                [(
                                    "role".to_string(),
                                    BoundValue::Value(KipValue::String("support".into())),
                                )]
                                .into_iter()
                                .collect(),
                            ),
                        })
                        .collect()
                })
                .unwrap_or_default();

            let mut clauses = vec![
                MutationClause::EnsureProposition(EnsureProposition {
                    handle: Some(proposition_handle),
                    subject: triple.0.clone(),
                    predicate: triple.1.clone(),
                    object: triple.2.clone(),
                    expect_version: None,
                }),
                MutationClause::CreateAssertion(RecordCreate {
                    handle: assertion_handle.clone(),
                    client_key: client_key.clone(),
                    set_fields: Some(set_fields),
                    set_facets: Vec::new(),
                    set_structural: (!edges.is_empty()).then_some(edges),
                }),
            ];

            if let Some(target) = &superseding {
                clauses.push(MutationClause::SupersedeAssertion(SupersedeAssertion {
                    target: target.clone(),
                    by: ElementRef::Handle(assertion_handle),
                    expect_state: None,
                }));
            }
            clauses
        }),
    ))
}

/// Splits an `evidence:` member into one citation per artifact.
fn evidence_refs(value: &MutationValue) -> Vec<MutationValue> {
    match value {
        MutationValue::Array(items) => items.iter().cloned().map(MutationValue::from).collect(),
        // A wholly literal array collapsed on the way in; it still cites one
        // artifact per element.
        MutationValue::Value(KipValue::Array(items)) => {
            items.iter().cloned().map(MutationValue::Value).collect()
        }
        other => vec![other.clone()],
    }
}

// ---------------------------------------------------------------------------
// UPDATE and the lifecycle family
// ---------------------------------------------------------------------------

fn update_statement(input: &str) -> VResult<'_, UpdateStatement> {
    let (input, _) = ws(word("UPDATE")).parse(input)?;
    let (start, target) = cut(ws(element_ref)).parse(input)?;
    let (rest, expect_version) = opt_after(&["EXPECT", "VERSION"], ws(scalar)).parse(start)?;
    let (rest, actions) = many0(ws(update_action)).parse(rest)?;
    if actions.is_empty() {
        return fail(start, "at least one SET or UNSET action");
    }
    let (rest, where_clauses) =
        opt_after(&["WHERE"], |i| where_block(i, Flavor::Exact)).parse(rest)?;
    let (rest, limit) = opt_after(&["LIMIT"], ws(scalar)).parse(rest)?;

    let statement = UpdateStatement {
        target,
        expect_version,
        actions,
        where_clauses,
        limit,
    };
    if let Err(ctx) = guard_update(&statement) {
        return fail(start, ctx);
    }
    Ok((rest, statement))
}

fn update_action(input: &str) -> VResult<'_, UpdateAction> {
    map(body_clause, |clause| match clause {
        BodyClause::SetFields(v) => Some(UpdateAction::SetFields(v)),
        BodyClause::SetAttributes(v) => Some(UpdateAction::SetAttributes(v)),
        BodyClause::SetFacet(v) => Some(UpdateAction::SetFacet(v)),
        BodyClause::UnsetAttributes(v) => Some(UpdateAction::UnsetAttributes(v)),
        BodyClause::UnsetFacet(v) => Some(UpdateAction::UnsetFacet(v)),
        BodyClause::SetStructural(v) => Some(UpdateAction::SetStructural(v)),
        BodyClause::UnsetStructural(v) => Some(UpdateAction::UnsetStructural(v)),
        _ => None,
    })
    .parse(input)
    .and_then(|(rest, action)| match action {
        Some(action) => Ok((rest, action)),
        None => fail(input, "a SET or UNSET action"),
    })
}

/// Which Core kind the UPDATE target is bound to, when the WHERE block says.
#[derive(Clone, Copy, PartialEq, Eq)]
enum BoundKind {
    Assertion,
    Evidence,
    Proposition,
    Concept,
    Activity,
}

fn bound_kind_of(variable: &str, clauses: &[WhereClause]) -> Option<BoundKind> {
    for clause in clauses {
        let found = match clause {
            WhereClause::Assertion { variable: v, .. } if v == variable => {
                Some(BoundKind::Assertion)
            }
            WhereClause::Evidence { variable: v, .. } if v == variable => Some(BoundKind::Evidence),
            WhereClause::Activity { variable: v, .. } if v == variable => Some(BoundKind::Activity),
            WhereClause::Concept { variable: v, .. } if v == variable => Some(BoundKind::Concept),
            WhereClause::Proposition {
                variable: Some(v), ..
            } if v == variable => Some(BoundKind::Proposition),
            WhereClause::Not(inner) | WhereClause::Optional(inner) | WhereClause::Union(inner) => {
                bound_kind_of(variable, inner)
            }
            _ => None,
        };
        if found.is_some() {
            return found;
        }
    }
    None
}

/// Rejects the UPDATEs an engine must never be asked to perform.
fn guard_update(statement: &UpdateStatement) -> Result<(), &'static str> {
    let target_var = match &statement.target {
        ElementRef::Handle(name) => Some(name.as_str()),
        _ => None,
    };
    let kind = match (target_var, &statement.where_clauses) {
        (Some(var), Some(clauses)) => bound_kind_of(var, clauses),
        _ => None,
    };

    for action in &statement.actions {
        match action {
            UpdateAction::SetFields(assignments) => {
                for (field, _) in assignments {
                    guard_immutable_field(field, kind)?;
                }
            }
            UpdateAction::SetStructural(_) | UpdateAction::UnsetStructural(_) => {
                guard_structural_mutation(kind)?
            }
            _ => {}
        }
    }

    // An update expression may read only the element being updated: reading
    // another variable would make the result depend on a join the statement
    // never declared.
    if let Some(target_var) = target_var {
        let mut paths: Vec<&DotPathVar> = Vec::new();
        for action in &statement.actions {
            match action {
                UpdateAction::SetFields(a) | UpdateAction::SetAttributes(a) => {
                    for (_, value) in a {
                        collect_mutation_value_paths(value, &mut paths);
                    }
                }
                UpdateAction::SetFacet(facet) => {
                    for (_, value) in &facet.values {
                        collect_mutation_value_paths(value, &mut paths);
                    }
                }
                UpdateAction::SetStructural(edges) => {
                    for edge in edges {
                        collect_mutation_value_paths(&edge.value, &mut paths);
                    }
                }
                UpdateAction::UnsetStructural(removals) => {
                    for removal in removals {
                        collect_mutation_value_paths(&removal.value, &mut paths);
                    }
                }
                UpdateAction::UnsetAttributes(_) | UpdateAction::UnsetFacet(_) => {}
            }
        }
        if paths.iter().any(|path| path.var != target_var) {
            return Err("an update expression that reads only the target element's own fields");
        }
    }

    Ok(())
}

/// Structural mutation reaches mutable Concept topology only (Spec §17.5).
fn guard_structural_mutation(kind: Option<BoundKind>) -> Result<(), &'static str> {
    match kind {
        Some(BoundKind::Assertion) => Err(
            "a mutable target: an Assertion's citations are immutable payload — record a new \
             Assertion with SUPERSEDING",
        ),
        Some(BoundKind::Evidence) => {
            Err("a mutable target: correct Evidence topology with CORRECT EVIDENCE :old BY :new")
        }
        Some(BoundKind::Proposition) => {
            Err("a target with structural fields: a Proposition is its tuple and carries none")
        }
        Some(BoundKind::Activity) => Err(
            "a mutable target: finalize a pending Activity with TRANSITION ACTIVITY ... SET \
             STRUCTURAL; a terminal Activity is immutable",
        ),
        _ => Ok(()),
    }
}

fn guard_immutable_field(field: &str, kind: Option<BoundKind>) -> Result<(), &'static str> {
    match kind {
        Some(BoundKind::Assertion) if ASSERTION_IMMUTABLE.contains(&field) => Err(
            "a mutable field: immutable Assertion payload changes by recording a new Assertion \
             with SUPERSEDING, never by rewriting the old one",
        ),
        Some(BoundKind::Evidence) if EVIDENCE_IMMUTABLE.contains(&field) => Err(
            "a mutable field: immutable Evidence payload is corrected with CORRECT EVIDENCE \
             :old BY :new",
        ),
        Some(BoundKind::Proposition) if PROPOSITION_IMMUTABLE.contains(&field) => Err(
            "a mutable field: the Proposition tuple is immutable — a different tuple is a \
             different Proposition",
        ),
        _ => Ok(()),
    }
}

fn retract_assertion(input: &str) -> VResult<'_, RetractAssertion> {
    let (input, _) = ws(words(&["RETRACT", "ASSERTION"])).parse(input)?;
    let (input, target) = cut(ws(element_ref)).parse(input)?;
    let (input, where_clauses) =
        opt_after(&["WHERE"], |i| where_block(i, Flavor::Exact)).parse(input)?;
    let (input, limit) = opt_after(&["LIMIT"], ws(scalar)).parse(input)?;
    let (input, expect_state) = opt_after(&["EXPECT", "STATE"], ws(scalar)).parse(input)?;
    Ok((
        input,
        RetractAssertion {
            target,
            where_clauses,
            limit,
            expect_state,
        },
    ))
}

fn supersede_assertion(input: &str) -> VResult<'_, SupersedeAssertion> {
    let (input, _) = ws(words(&["SUPERSEDE", "ASSERTION"])).parse(input)?;
    let (input, target) = cut(ws(element_ref)).parse(input)?;
    let (input, _) = cut(ws(word("BY"))).parse(input)?;
    let (input, by) = cut(ws(element_ref)).parse(input)?;
    let (input, expect_state) = opt_after(&["EXPECT", "STATE"], ws(scalar)).parse(input)?;
    Ok((
        input,
        SupersedeAssertion {
            target,
            by,
            expect_state,
        },
    ))
}

fn correct_evidence(input: &str) -> VResult<'_, CorrectEvidence> {
    let (input, _) = ws(words(&["CORRECT", "EVIDENCE"])).parse(input)?;
    let (input, target) = cut(ws(element_ref)).parse(input)?;
    let (input, _) = cut(ws(word("BY"))).parse(input)?;
    let (input, by) = cut(ws(element_ref)).parse(input)?;
    let (input, expect_state) = opt_after(&["EXPECT", "STATE"], ws(scalar)).parse(input)?;
    Ok((
        input,
        CorrectEvidence {
            target,
            by,
            expect_state,
        },
    ))
}

fn transition_activity(input: &str) -> VResult<'_, TransitionActivity> {
    let (input, _) = ws(words(&["TRANSITION", "ACTIVITY"])).parse(input)?;
    let (input, target) = cut(ws(element_ref)).parse(input)?;
    let (input, _) = cut(ws(word("TO"))).parse(input)?;
    let (start, to) = cut(ws(scalar)).parse(input)?;

    let (rest, finalize) = many0(ws(spanned(alt((
        map(
            preceded(ws(words(&["SET", "FIELDS"])), cut(ws(assignments))),
            |a| (true, Some(a), None),
        ),
        map(
            preceded(ws(words(&["SET", "STRUCTURAL"])), cut(structural_edges)),
            |edges| (false, None, Some(edges)),
        ),
    )))))
    .parse(start)?;

    let mut set_fields = None;
    let mut set_structural = None;
    for (position, (is_fields, fields, structural)) in finalize {
        if is_fields {
            if set_fields.replace(fields.expect("SET FIELDS")).is_some() {
                return fail(position, "at most one SET FIELDS clause");
            }
        } else if set_structural
            .replace(structural.expect("SET STRUCTURAL"))
            .is_some()
        {
            return fail(position, "at most one SET STRUCTURAL clause");
        }
    }

    let (rest, expect_state) = opt_after(&["EXPECT", "STATE"], ws(scalar)).parse(rest)?;
    Ok((
        rest,
        TransitionActivity {
            target,
            to,
            set_fields,
            set_structural,
            expect_state,
        },
    ))
}

fn set_retention(input: &str) -> VResult<'_, SetRetention> {
    let (input, _) = ws(words(&["SET", "RETENTION"])).parse(input)?;
    let (input, target) = cut(ws(element_ref)).parse(input)?;
    let (input, values) = cut(ws(assignments)).parse(input)?;
    let (input, where_clauses) =
        opt_after(&["WHERE"], |i| where_block(i, Flavor::Exact)).parse(input)?;
    let (input, limit) = opt_after(&["LIMIT"], ws(scalar)).parse(input)?;
    let (input, expect_version) = opt_after(&["EXPECT", "VERSION"], ws(scalar)).parse(input)?;
    Ok((
        input,
        SetRetention {
            target,
            values,
            where_clauses,
            limit,
            expect_version,
        },
    ))
}

/// `ARCHIVE` and `TOMBSTONE` differ only in their verb.
fn removal<'a>(verb: &'static str, input: &'a str) -> VResult<'a, RemovalStatement> {
    let (input, _) = ws(word(verb)).parse(input)?;
    let (input, target) = cut(ws(element_ref)).parse(input)?;
    let (input, where_clauses) =
        opt_after(&["WHERE"], |i| where_block(i, Flavor::Exact)).parse(input)?;
    let (input, limit) = opt_after(&["LIMIT"], ws(scalar)).parse(input)?;
    let (input, expect_state) = opt_after(&["EXPECT", "STATE"], ws(scalar)).parse(input)?;
    Ok((
        input,
        RemovalStatement {
            target,
            where_clauses,
            limit,
            expect_state,
        },
    ))
}

fn purge_statement(input: &str) -> VResult<'_, crate::ast::PurgeStatement> {
    let (input, _) = ws(word("PURGE")).parse(input)?;
    let (input, target) = cut(ws(element_ref)).parse(input)?;
    let (input, where_clauses) =
        opt_after(&["WHERE"], |i| where_block(i, Flavor::Exact)).parse(input)?;
    let (input, limit) = opt_after(&["LIMIT"], ws(scalar)).parse(input)?;
    let (input, reference_policy) = opt_after(&["REFERENCE", "POLICY"], ws(scalar)).parse(input)?;
    let (input, _) = cut(ws(word("CONFIRM"))).parse(input)?;
    let (rest, confirm) = cut(ws(quoted_string)).parse(input)?;
    if confirm != "PURGE" {
        return fail(input, "the exact confirmation literal \"PURGE\"");
    }

    Ok((
        rest,
        crate::ast::PurgeStatement {
            target,
            where_clauses,
            limit,
            reference_policy,
            confirm,
        },
    ))
}

fn merge_concept(input: &str) -> VResult<'_, MergeConcept> {
    let (input, _) = ws(words(&["MERGE", "CONCEPT"])).parse(input)?;
    let (input, source) = cut(ws(element_ref)).parse(input)?;
    let (input, _) = cut(ws(word("INTO"))).parse(input)?;
    let (input, into) = cut(ws(element_ref)).parse(input)?;
    let (input, where_clauses) =
        opt_after(&["WHERE"], |i| where_block(i, Flavor::Exact)).parse(input)?;
    let (input, expect_version) = opt_after(&["EXPECT", "VERSION"], ws(scalar)).parse(input)?;
    Ok((
        input,
        MergeConcept {
            source,
            into,
            where_clauses,
            expect_version,
        },
    ))
}

// ---------------------------------------------------------------------------
// Whole-plan validation
// ---------------------------------------------------------------------------

/// Checks the closed update-expression vocabulary on an AST that did not pass
/// through [`update_function_call`].
fn validate_update_expr(expr: &UpdateExpr) -> Result<(), KipError> {
    if let UpdateExpr::Function { func, args } = expr {
        if args.len() != func.arity() {
            return Err(KipError::invalid_syntax(format!(
                "{func:?} expects {} arguments, found {}",
                func.arity(),
                args.len()
            )));
        }
        for arg in args {
            validate_update_expr(arg)?;
        }
    }
    Ok(())
}

fn validate_mutation_value(value: &MutationValue) -> Result<(), KipError> {
    if let MutationValue::Expr(expr) = value {
        validate_update_expr(expr)?;
    }
    Ok(())
}

fn validate_structural_edges(edges: &[StructuralEdge]) -> Result<(), KipError> {
    for edge in edges {
        validate_mutation_value(&edge.value)?;
    }
    Ok(())
}

/// Re-checks the schema-independent rules the grammar enforces as it parses.
///
/// The text parser rejects these while reading, so on that path this pass never
/// fires. It exists for the *other* path: an operation may carry a pre-parsed
/// `ast` instead of `command` text (Spec §73), and a tree that never went
/// through this parser has had none of the guards applied to it. Without this,
/// the crate's central claim — that a command asking an engine to corrupt the
/// epistemic record never reaches one — would hold only for text.
fn validate_clause(clause: &MutationClause) -> Result<(), KipError> {
    let bad = |ctx: &str| Err(KipError::invalid_syntax(ctx));

    let check_assignments = |a: &Assignments| -> Result<(), KipError> {
        let mut seen = BTreeSet::new();
        for (key, _) in a {
            if super::common::is_protected_field(key) {
                return Err(KipError::invalid_syntax(format!(
                    "{key} is engine-maintained state and cannot be written by a mutation"
                )));
            }
            if !seen.insert(key.as_str()) {
                return Err(KipError::invalid_syntax(format!(
                    "{key} is assigned twice in one block"
                )));
            }
        }
        for (_, value) in a {
            validate_mutation_value(value)?;
        }
        Ok(())
    };
    let check_unset = |fields: &[String]| -> Result<(), KipError> {
        let mut seen = BTreeSet::new();
        for key in fields {
            if super::common::is_protected_field(key) {
                return Err(KipError::invalid_syntax(format!(
                    "{key} is engine-maintained state and cannot be unset by a mutation"
                )));
            }
            if !seen.insert(key.as_str()) {
                return Err(KipError::invalid_syntax(format!(
                    "{key} is listed twice in one block"
                )));
            }
        }
        Ok(())
    };
    let check_facets = |facets: &[FacetAssignment]| -> Result<(), KipError> {
        facets.iter().try_for_each(|f| check_assignments(&f.values))
    };

    if let Some(where_clauses) = clause_where(clause) {
        validate_exact_patterns(where_clauses)?;
    }

    match clause {
        MutationClause::CreateConcept(c) => {
            c.set_fields.as_ref().map_or(Ok(()), &check_assignments)?;
            c.set_attributes
                .as_ref()
                .map_or(Ok(()), &check_assignments)?;
            check_facets(&c.set_facets)?;
            c.set_structural
                .as_deref()
                .map_or(Ok(()), validate_structural_edges)?;
        }
        MutationClause::UpsertConcept(c) => {
            c.set_fields.as_ref().map_or(Ok(()), &check_assignments)?;
            c.set_attributes
                .as_ref()
                .map_or(Ok(()), &check_assignments)?;
            check_facets(&c.set_facets)?;
            c.unset_attributes.as_deref().map_or(Ok(()), &check_unset)?;
            for facet in &c.unset_facets {
                check_unset(&facet.fields)?;
            }
            c.set_structural
                .as_deref()
                .map_or(Ok(()), validate_structural_edges)?;
            // Names are mutable grounding state with duplicates allowed, so
            // "the Concept named X" can silently address a different node.
            if c.r#match
                .as_ref()
                .is_none_or(|matcher| !upsert_has_stable_identity_selector(matcher))
            {
                return bad("UPSERT CONCEPT must MATCH a stable identity — \
                     {id: <literal-or-parameter>} or {key: <literal-or-parameter>}");
            }
            if let Some(matcher) = &c.r#match {
                validate_exact_object_matcher(matcher)?;
            }
            if c.unset_structural.as_ref().is_some_and(Vec::is_empty) {
                return bad("UNSET STRUCTURAL removes named references; list at least one");
            }
        }
        MutationClause::CreateEvidence(c)
        | MutationClause::CreateAssertion(c)
        | MutationClause::CreateActivity(c) => {
            c.set_fields.as_ref().map_or(Ok(()), &check_assignments)?;
            check_facets(&c.set_facets)?;
            c.set_structural
                .as_deref()
                .map_or(Ok(()), validate_structural_edges)?;
        }
        MutationClause::EnsureProposition(c) => {
            if matches!(c.predicate, PredAtom::Variable(_)) {
                return bad(
                    "ENSURE PROPOSITION needs an exact quoted predicate or :parameter; \
                    ?variables are KQL read-pattern syntax",
                );
            }
            validate_proposition_subject(&c.subject)?;
            validate_exact_term(&c.object)?;
        }
        MutationClause::Update(c) => {
            for action in &c.actions {
                match action {
                    UpdateAction::SetFields(a) | UpdateAction::SetAttributes(a) => {
                        check_assignments(a)?
                    }
                    UpdateAction::SetFacet(f) => check_assignments(&f.values)?,
                    UpdateAction::UnsetAttributes(f) => check_unset(f)?,
                    UpdateAction::UnsetFacet(f) => check_unset(&f.fields)?,
                    UpdateAction::UnsetStructural(removals) if removals.is_empty() => {
                        return bad("UNSET STRUCTURAL removes named references; list at least one");
                    }
                    UpdateAction::SetStructural(edges) => validate_structural_edges(edges)?,
                    UpdateAction::UnsetStructural(removals) => {
                        for removal in removals {
                            validate_mutation_value(&removal.value)?;
                        }
                    }
                }
            }
            if c.actions.is_empty() {
                return bad("UPDATE requires at least one SET or UNSET action");
            }
            if let Err(ctx) = guard_update(c) {
                return bad(ctx);
            }
        }
        MutationClause::TransitionActivity(c) => {
            c.set_fields.as_ref().map_or(Ok(()), &check_assignments)?;
            c.set_structural
                .as_deref()
                .map_or(Ok(()), validate_structural_edges)?;
        }
        MutationClause::SetRetention(c) => check_assignments(&c.values)?,
        // The grammar freezes the spelling so a purge is never the result of a
        // near-miss confirmation.
        MutationClause::Purge(c) if c.confirm != "PURGE" => {
            return bad("PURGE must be confirmed with the exact literal \"PURGE\"");
        }
        _ => {}
    }
    Ok(())
}

/// KML and META selection blocks parse in the exact flavor: no `BELIEF`, and no
/// raw predicate paths. A virtual Projection is never a mutation target or an
/// export selector, and a path never resolves to one Proposition to write.
pub fn validate_exact_patterns(clauses: &[WhereClause]) -> Result<(), KipError> {
    for clause in clauses {
        match clause {
            WhereClause::Belief { .. } | WhereClause::BeliefSlot { .. } => {
                return Err(KipError::invalid_syntax(
                    "BELIEF is a read-only Projection and can never be a mutation target or an \
                     export selector",
                ));
            }
            WhereClause::Concept { matcher, .. }
            | WhereClause::Assertion { matcher, .. }
            | WhereClause::Evidence { matcher, .. }
            | WhereClause::Activity { matcher, .. } => validate_exact_object_matcher(matcher)?,
            WhereClause::Proposition { matcher, .. } => validate_exact_proposition(matcher)?,
            WhereClause::Structural {
                subject, object, ..
            } => {
                validate_exact_term(subject)?;
                validate_exact_term(object)?;
            }
            WhereClause::Not(inner) | WhereClause::Optional(inner) | WhereClause::Union(inner) => {
                validate_exact_patterns(inner)?
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_proposition_subject(subject: &Term) -> Result<(), KipError> {
    if matches!(subject, Term::Literal(_)) {
        return Err(KipError::invalid_syntax(
            "a Proposition subject must be an Element reference, never a Literal",
        ));
    }
    validate_exact_term(subject)
}

fn validate_exact_proposition(matcher: &PropositionMatcher) -> Result<(), KipError> {
    let PropositionMatcher::Tuple(triple) = matcher else {
        return Ok(());
    };
    if matches!(triple.predicate, crate::ast::PredTerm::Path(_)) {
        return Err(KipError::invalid_syntax(
            "alternation and hop quantifiers are KQL traversal forms and are not selection \
             syntax here",
        ));
    }
    validate_proposition_subject(&triple.subject)?;
    validate_exact_term(&triple.object)
}

fn validate_exact_term(term: &Term) -> Result<(), KipError> {
    match term {
        Term::Match(matcher) => validate_exact_object_matcher(matcher),
        Term::Proposition(matcher) => validate_exact_proposition(matcher),
        Term::Variable(_) | Term::Param(_) | Term::Literal(_) => Ok(()),
    }
}

fn validate_exact_object_matcher(matcher: &crate::ast::ObjectMatcher) -> Result<(), KipError> {
    for value in matcher.values() {
        validate_exact_match_value(value)?;
    }
    Ok(())
}

fn validate_exact_match_value(value: &crate::ast::MatchValue) -> Result<(), KipError> {
    match value {
        crate::ast::MatchValue::Array(items) => {
            for item in items {
                validate_exact_match_value(item)?;
            }
            Ok(())
        }
        crate::ast::MatchValue::Match(matcher) => validate_exact_object_matcher(matcher),
        crate::ast::MatchValue::Proposition(matcher) => validate_exact_proposition(matcher),
        crate::ast::MatchValue::Variable(_)
        | crate::ast::MatchValue::Param(_)
        | crate::ast::MatchValue::Literal(_) => Ok(()),
    }
}

/// Checks the invariants that only the whole mutation plan can decide.
pub fn validate_plan(statement: &KmlStatement) -> Result<(), KipError> {
    if statement.clauses.is_empty() {
        return Err(KipError::invalid_syntax(
            "a KML transaction must carry at least one mutation",
        ));
    }
    for clause in &statement.clauses {
        validate_clause(clause)?;
    }

    // Handles are block-local names. Two clauses claiming the same handle make
    // every forward reference to it ambiguous, so the whole plan is rejected
    // rather than resolved by position.
    let mut plan_handles: BTreeSet<String> = BTreeSet::new();
    for clause in &statement.clauses {
        if let Some(name) = clause.handle()
            && !plan_handles.insert(name.to_string())
        {
            return Err(KipError::duplicate_local_handle(format!(
                "?{name} is claimed by two clauses in one mutation plan"
            )));
        }
    }

    // Every executable handle must be created by this plan or bound by that
    // clause's own WHERE. Parameters remain runtime bindings and are unaffected.
    for clause in &statement.clauses {
        let mut allowed = plan_handles.clone();
        if let Some(where_clauses) = clause_where(clause) {
            collect_where_variables(where_clauses, &mut allowed);
        }
        let mut referenced = BTreeSet::new();
        collect_clause_handles(clause, &mut referenced);
        for name in referenced {
            if !allowed.contains(&name) {
                return Err(KipError::reference_error(format!(
                    "?{name} is not bound by this command's mutation outputs or WHERE clause"
                )));
            }
        }
    }

    Ok(())
}

fn clause_where(clause: &MutationClause) -> Option<&Vec<WhereClause>> {
    match clause {
        MutationClause::Update(c) => c.where_clauses.as_ref(),
        MutationClause::RetractAssertion(c) => c.where_clauses.as_ref(),
        MutationClause::SetRetention(c) => c.where_clauses.as_ref(),
        MutationClause::Archive(c) | MutationClause::Tombstone(c) => c.where_clauses.as_ref(),
        MutationClause::Purge(c) => c.where_clauses.as_ref(),
        MutationClause::MergeConcept(c) => c.where_clauses.as_ref(),
        _ => None,
    }
}

fn collect_clause_handles(clause: &MutationClause, out: &mut BTreeSet<String>) {
    let mut element = |r: &ElementRef| {
        if let ElementRef::Handle(name) = r {
            out.insert(name.clone());
        }
    };

    match clause {
        MutationClause::CreateConcept(c) => {
            collect_assignments_handles(c.set_fields.as_ref(), out);
            collect_assignments_handles(c.set_attributes.as_ref(), out);
            collect_facets_handles(&c.set_facets, out);
            collect_edges_handles(c.set_structural.as_ref(), out);
        }
        MutationClause::UpsertConcept(c) => {
            collect_assignments_handles(c.set_fields.as_ref(), out);
            collect_assignments_handles(c.set_attributes.as_ref(), out);
            collect_facets_handles(&c.set_facets, out);
            collect_edges_handles(c.set_structural.as_ref(), out);
            if let Some(removals) = &c.unset_structural {
                for removal in removals {
                    collect_mutation_value_handles(&removal.value, out);
                }
            }
        }
        MutationClause::CreateEvidence(c)
        | MutationClause::CreateAssertion(c)
        | MutationClause::CreateActivity(c) => {
            collect_assignments_handles(c.set_fields.as_ref(), out);
            collect_facets_handles(&c.set_facets, out);
            collect_edges_handles(c.set_structural.as_ref(), out);
        }
        MutationClause::EnsureProposition(_) => {}
        MutationClause::Update(c) => {
            element(&c.target);
            for action in &c.actions {
                match action {
                    UpdateAction::SetFields(a) | UpdateAction::SetAttributes(a) => {
                        collect_assignments_handles(Some(a), out)
                    }
                    UpdateAction::SetFacet(f) => collect_assignments_handles(Some(&f.values), out),
                    UpdateAction::SetStructural(edges) => collect_edges_handles(Some(edges), out),
                    UpdateAction::UnsetStructural(removals) => {
                        for removal in removals {
                            collect_mutation_value_handles(&removal.value, out);
                        }
                    }
                    UpdateAction::UnsetAttributes(_) | UpdateAction::UnsetFacet(_) => {}
                }
            }
        }
        MutationClause::RetractAssertion(c) => element(&c.target),
        MutationClause::SupersedeAssertion(c) => {
            element(&c.target);
            element(&c.by);
        }
        MutationClause::CorrectEvidence(c) => {
            element(&c.target);
            element(&c.by);
        }
        MutationClause::TransitionActivity(c) => {
            element(&c.target);
            collect_assignments_handles(c.set_fields.as_ref(), out);
            collect_edges_handles(c.set_structural.as_ref(), out);
        }
        MutationClause::SetRetention(c) => {
            element(&c.target);
            collect_assignments_handles(Some(&c.values), out);
        }
        MutationClause::Archive(c) | MutationClause::Tombstone(c) => element(&c.target),
        MutationClause::Purge(c) => element(&c.target),
        MutationClause::MergeConcept(c) => {
            element(&c.source);
            element(&c.into);
        }
    }
}

fn collect_assignments_handles(assignments: Option<&Assignments>, out: &mut BTreeSet<String>) {
    for (_, value) in assignments.into_iter().flatten() {
        collect_mutation_value_handles(value, out);
    }
}

fn collect_facets_handles(facets: &[FacetAssignment], out: &mut BTreeSet<String>) {
    for facet in facets {
        collect_assignments_handles(Some(&facet.values), out);
    }
}

fn collect_edges_handles(edges: Option<&Vec<StructuralEdge>>, out: &mut BTreeSet<String>) {
    for edge in edges.into_iter().flatten() {
        collect_mutation_value_handles(&edge.value, out);
        if let Some(options) = &edge.options {
            collect_bound_object_handles(options, out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kml(input: &str) -> KmlStatement {
        let (rest, statement) =
            parse_kml_statement(input).unwrap_or_else(|e| panic!("failed to parse:\n{input}\n{e}"));
        assert!(rest.trim().is_empty(), "unconsumed input {rest:?}");
        validate_plan(&statement).expect("plan validation");
        statement
    }

    #[test]
    fn a_lone_mutation_is_still_a_transaction() {
        let statement = kml(r#"ARCHIVE :old"#);
        assert!(!statement.explicit_transaction);
        assert_eq!(statement.clauses.len(), 1);

        let explicit = kml(r#"MUTATE { ARCHIVE :old }"#);
        assert!(explicit.explicit_transaction);
    }

    #[test]
    fn mutate_needs_at_least_one_mutation() {
        assert!(parse_kml_statement("MUTATE { }").is_err());
    }

    #[test]
    fn assert_desugars_to_exactly_its_definition() {
        let statement = kml(r#"ASSERT ?a (:alice, "prefers", :dark_mode) {
                by: :alice,
                mode: "stated",
                confidence: 0.9,
                evidence: :msg
            }"#);
        assert_eq!(statement.clauses.len(), 2);

        let MutationClause::EnsureProposition(ensure) = &statement.clauses[0] else {
            panic!("expected ENSURE PROPOSITION first");
        };
        assert_eq!(ensure.handle.as_deref(), Some("a#proposition"));

        let MutationClause::CreateAssertion(assertion) = &statement.clauses[1] else {
            panic!("expected CREATE ASSERTION second");
        };
        let fields = assertion.set_fields.as_ref().expect("fields");
        let names: Vec<&str> = fields.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(
            names,
            vec!["proposition", "asserted_by", "mode", "stance", "confidence"]
        );
        // The stance default is materialized, not left for the engine to guess.
        assert_eq!(
            fields[3].1,
            MutationValue::Value(KipValue::String("support".into()))
        );
        // `evidence` is a structural field carrying a role, not a plain field.
        let edges = assertion.set_structural.as_ref().expect("evidence edge");
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].field, SymbolRef::Name("evidence".into()));
        assert!(edges[0].options.as_ref().unwrap().contains_key("role"));
    }

    #[test]
    fn assert_cites_one_edge_per_artifact() {
        let statement =
            kml(r#"ASSERT (:a, "p", :b) { by: :me, mode: "stated", evidence: [:e1, :e2] }"#);
        let MutationClause::CreateAssertion(assertion) = &statement.clauses[1] else {
            panic!("expected CREATE ASSERTION");
        };
        assert_eq!(assertion.set_structural.as_ref().unwrap().len(), 2);

        // A wholly literal array collapses on the way in and must still split.
        let literal =
            kml(r#"ASSERT (:a, "p", :b) { by: :me, mode: "stated", evidence: ["E-1", "E-2"] }"#);
        let MutationClause::CreateAssertion(assertion) = &literal.clauses[1] else {
            panic!("expected CREATE ASSERTION");
        };
        assert_eq!(assertion.set_structural.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn assert_has_no_safe_default_for_actor_or_mode() {
        assert!(parse_kml_statement(r#"ASSERT (:a, "p", :b) { mode: "stated" }"#).is_err());
        assert!(parse_kml_statement(r#"ASSERT (:a, "p", :b) { by: :me }"#).is_err());
        assert!(
            parse_kml_statement(r#"ASSERT (:a, "p", :b) { by: :me, mode: "stated", oops: 1 }"#)
                .is_err()
        );
    }

    #[test]
    fn two_handle_less_asserts_do_not_collide() {
        let statement = kml(r#"MUTATE {
                ASSERT (:a, "p", :b) { by: :me, mode: "stated" }
                ASSERT (:c, "q", :d) { by: :me, mode: "stated" }
            }"#);
        assert_eq!(statement.clauses.len(), 4);
        let handles: Vec<&str> = statement
            .clauses
            .iter()
            .filter_map(|c| c.handle())
            .collect();
        assert_eq!(
            handles,
            vec![
                "#assert0#proposition",
                "#assert0",
                "#assert1#proposition",
                "#assert1"
            ]
        );
    }

    #[test]
    fn assert_superseding_points_at_the_new_assertion() {
        let statement =
            kml(r#"ASSERT ?new (:a, "p", :b) { by: :me, mode: "stated" } SUPERSEDING :old"#);
        assert_eq!(statement.clauses.len(), 3);
        let MutationClause::SupersedeAssertion(supersede) = &statement.clauses[2] else {
            panic!("expected SUPERSEDE");
        };
        assert_eq!(supersede.by, ElementRef::Handle("new".into()));
        assert_eq!(supersede.target, ElementRef::Param("old".into()));
    }

    #[test]
    fn resolve_or_create_rejects_the_id_spelling() {
        // `(id: ...)` is match-only: no structure can be created from an id.
        assert!(parse_kml_statement(r#"ENSURE PROPOSITION (id: "P-1")"#).is_err());
        assert!(parse_kml_statement(r#"ASSERT (id: "P-1") { by: :me, mode: "stated" }"#).is_err());
        // A ?variable predicate is read-pattern syntax, not a creatable tuple.
        assert!(parse_kml_statement(r#"ENSURE PROPOSITION (:a, ?p, :b)"#).is_err());
    }

    #[test]
    fn upsert_must_match_a_stable_identity() {
        assert!(
            parse_kml_statement(r#"UPSERT CONCEPT ?c { SET FIELDS {name: "Alice"} }"#).is_err()
        );
        assert!(parse_kml_statement(r#"UPSERT CONCEPT ?c { MATCH {name: "Alice"} }"#).is_err());
        assert!(parse_kml_statement(r#"UPSERT CONCEPT ?c { MATCH {id: ?anything} }"#).is_err());
        assert!(parse_kml_statement(r#"UPSERT CONCEPT ?c { MATCH {id: "C-1"} }"#).is_ok());
        assert!(parse_kml_statement(r#"UPSERT CONCEPT ?c { MATCH {id: :concept_id} }"#).is_ok());
        assert!(
            parse_kml_statement(r#"UPSERT CONCEPT ?c { MATCH {key: "person:alice"} }"#).is_ok()
        );
    }

    #[test]
    fn pre_parsed_trees_get_the_grammar_only_exactness_guards() {
        let mut update = kml(r#"UPDATE :c SET FIELDS { n: ADD(1, 2) }"#);
        let MutationClause::Update(update_clause) = &mut update.clauses[0] else {
            unreachable!()
        };
        let UpdateAction::SetFields(assignments) = &mut update_clause.actions[0] else {
            unreachable!()
        };
        let MutationValue::Expr(UpdateExpr::Function { args, .. }) = &mut assignments[0].1 else {
            unreachable!()
        };
        args.pop();
        assert!(validate_plan(&update).is_err(), "invalid function arity");

        let mut ensure = kml(r#"ENSURE PROPOSITION (:a, "related_to", :b)"#);
        let MutationClause::EnsureProposition(ensure_clause) = &mut ensure.clauses[0] else {
            unreachable!()
        };
        ensure_clause.subject = Term::Literal(KipValue::String("not-an-element".into()));
        assert!(
            validate_plan(&ensure).is_err(),
            "literal proposition subject"
        );

        let crate::ast::Command::Kql(query) = crate::parser::parse_kip(
            r#"FIND(?p) WHERE { ?p PROPOSITION (:a, "related_to"|"knows"{1,3}, :b) }"#,
        )
        .unwrap() else {
            unreachable!()
        };
        let mut archive =
            kml(r#"ARCHIVE ?p WHERE { ?p PROPOSITION (:a, "related_to", :b) } LIMIT 1"#);
        let MutationClause::Archive(archive_clause) = &mut archive.clauses[0] else {
            unreachable!()
        };
        archive_clause.where_clauses = Some(query.where_clauses);
        assert!(validate_plan(&archive).is_err(), "raw predicate path");
    }

    #[test]
    fn update_cannot_rewrite_immutable_epistemic_payload() {
        let bad = r#"UPDATE ?a SET FIELDS { confidence: 0.1 } WHERE { ?a ASSERTION {id: "A-1"} }"#;
        assert!(parse_kml_statement(bad).is_err());
        let bad_evidence_alias =
            r#"UPDATE ?a SET FIELDS { evidence: :e } WHERE { ?a ASSERTION {id: "A-1"} }"#;
        assert!(parse_kml_statement(bad_evidence_alias).is_err());

        let bad_evidence =
            r#"UPDATE ?e SET FIELDS { payload: "x" } WHERE { ?e EVIDENCE {id: "E-1"} }"#;
        assert!(parse_kml_statement(bad_evidence).is_err());

        let bad_tuple =
            r#"UPDATE ?p SET FIELDS { subject: :x } WHERE { ?p PROPOSITION (?s, "q", ?o) }"#;
        assert!(parse_kml_statement(bad_tuple).is_err());

        // The same field on a Concept is ordinary mutable state.
        let ok = r#"UPDATE ?c SET FIELDS { confidence: 0.1 } WHERE { ?c CONCEPT {id: "C-1"} }"#;
        assert!(parse_kml_statement(ok).is_ok());
    }

    #[test]
    fn structural_mutation_is_concept_topology_only() {
        assert!(
            parse_kml_statement(
                r#"UPDATE ?a SET STRUCTURAL { ("evidence", :e) } WHERE { ?a ASSERTION {id: "A-1"} }"#
            )
            .is_err()
        );
        assert!(
            parse_kml_statement(
                r#"UPDATE ?c SET STRUCTURAL { ("has_step", :s) } WHERE { ?c CONCEPT {id: "C-1"} }"#
            )
            .is_ok()
        );
    }

    #[test]
    fn an_update_expression_reads_only_its_own_target() {
        let ok = r#"UPDATE ?c SET FACET "MnemonicState" { memory_strength: MUL(?c.facets["MnemonicState"].memory_strength, 0.99) } WHERE { ?c CONCEPT {id: "C-1"} }"#;
        assert!(parse_kml_statement(ok).is_ok());

        let joined =
            r#"UPDATE ?c SET FIELDS { n: ADD(?other.n, 1) } WHERE { ?c CONCEPT {id: "C-1"} }"#;
        assert!(parse_kml_statement(joined).is_err());
    }

    #[test]
    fn update_needs_an_action() {
        assert!(parse_kml_statement(r#"UPDATE :c WHERE { ?c CONCEPT {id: "C-1"} }"#).is_err());
    }

    #[test]
    fn purge_freezes_its_confirmation_spelling() {
        assert!(parse_kml_statement(r#"PURGE :e CONFIRM "PURGE""#).is_ok());
        assert!(parse_kml_statement(r#"PURGE :e CONFIRM "purge""#).is_err());
        assert!(parse_kml_statement(r#"PURGE :e"#).is_err());
    }

    #[test]
    fn unset_structural_removes_named_references() {
        assert!(parse_kml_statement(r#"UPDATE :c UNSET STRUCTURAL { ("has_step", :s) }"#).is_ok());
        assert!(parse_kml_statement(r#"UPDATE :c UNSET STRUCTURAL { }"#).is_err());
    }

    #[test]
    fn duplicate_handles_are_rejected_across_the_plan() {
        let statement = parse_kml_statement(
            r#"MUTATE {
                CREATE CONCEPT ?c { TYPE "Person" }
                CREATE CONCEPT ?c { TYPE "Drug" }
            }"#,
        )
        .expect("parses")
        .1;
        assert!(validate_plan(&statement).is_err());
    }

    #[test]
    fn forward_references_resolve_within_the_plan() {
        let statement = kml(r#"MUTATE {
                CREATE EVIDENCE ?msg { SET FIELDS { evidence_class: "user_statement" } }
                CREATE ASSERTION ?a {
                    SET FIELDS { asserted_by: :alice }
                    SET STRUCTURAL { ("evidence", ?msg) {role: "support"} }
                }
            }"#);
        assert_eq!(statement.clauses.len(), 2);
    }

    #[test]
    fn an_unbound_handle_is_a_reference_error() {
        let statement = parse_kml_statement(
            r#"CREATE ASSERTION ?a { SET STRUCTURAL { ("evidence", ?nowhere) } }"#,
        )
        .expect("parses")
        .1;
        let err = validate_plan(&statement).expect_err("unbound handle");
        assert_eq!(err.code, crate::error::KipErrorCode::ReferenceError);
    }

    #[test]
    fn a_where_bound_variable_counts_as_bound() {
        let statement = kml(
            r#"UPDATE ?c SET STRUCTURAL { ("has_step", ?c) } WHERE { ?c CONCEPT {id: "C-1"} }"#,
        );
        assert_eq!(statement.clauses.len(), 1);
    }

    #[test]
    fn transition_finalizes_at_most_once_per_clause_kind() {
        assert!(
            parse_kml_statement(
                r#"TRANSITION ACTIVITY :act TO "succeeded" SET FIELDS { ended_at: :now }"#
            )
            .is_ok()
        );
        assert!(
            parse_kml_statement(
                r#"TRANSITION ACTIVITY :act TO "succeeded" SET FIELDS { a: 1 } SET FIELDS { b: 2 }"#
            )
            .is_err()
        );
    }

    #[test]
    fn a_mutation_body_rejects_a_clause_it_does_not_admit() {
        // UNSET belongs to UPSERT and UPDATE; CREATE has nothing to remove.
        assert!(parse_kml_statement(r#"CREATE CONCEPT ?c { UNSET ATTRIBUTES { a } }"#).is_err());
        // MATCH identifies an existing element, which CREATE never does.
        assert!(parse_kml_statement(r#"CREATE CONCEPT ?c { MATCH {id: "C-1"} }"#).is_err());
    }

    #[test]
    fn merge_is_non_destructive_and_names_both_operands() {
        let statement = kml(r#"MERGE CONCEPT :js INTO :javascript EXPECT VERSION 3"#);
        let MutationClause::MergeConcept(merge) = &statement.clauses[0] else {
            panic!("expected MERGE");
        };
        assert_eq!(merge.source, ElementRef::Param("js".into()));
        assert!(merge.expect_version.is_some());
        // MERGE takes no LIMIT: both operands are already named, and its WHERE
        // only guards. The trailing clause is simply not part of the statement.
        assert!(crate::parser::parse_kml(r#"MERGE CONCEPT :a INTO :b LIMIT 1"#).is_err());
    }
}
