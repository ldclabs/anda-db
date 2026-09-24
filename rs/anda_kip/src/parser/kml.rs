//! KML — the Cognitive Mutation Language (Spec §51–§62).
//!
//! Two things happen here that a plain grammar would not do:
//!
//! - `ASSERT` is desugared into exactly what §55.1 defines it as — `ENSURE
//!   PROPOSITION` + `CREATE ASSERTION` (+ `TRANSITION ... TO "superseded"`) —
//!   and nothing beyond those parts is fabricated;
//! - the mutation guards run, so a command that would ask an engine to rewrite
//!   immutable epistemic payload is rejected before it reaches one.

use nom::{
    Parser,
    branch::alt,
    character::complete::char,
    combinator::{cut, map, opt, value},
    multi::many0,
    sequence::preceded,
};
use std::collections::BTreeSet;

use super::common::{
    Flavor, KeyFault, VResult, assignments, bound_object, braced, collect_bound_object_handles,
    collect_mutation_value_handles, collect_mutation_value_paths, collect_where_variables,
    element_ref, fail, handle, key_fault, mutation_value, object_matcher, opt_after, parenthesized,
    proposition_matcher, quoted_string, scalar, spanned, symbol_ref, unset_field_set, where_block,
    word, words, ws,
};
use crate::ast::{
    Assignments, BoundValue, ConceptCreate, ConceptUpsert, DefineCommand, DefineKind, DotPathVar,
    ElementRef, EnsureProposition, ExpectVersion, FacetAssignment, FacetUnset, KipValue,
    KmlStatement, MatchValue, MergeConcept, MutationClause, MutationValue, ObjectMatcher, PredAtom,
    PropositionMatcher, PropositionTriple, RecordCreate, Scalar, SetRetention, StructuralEdge,
    StructuralRemoval, SymbolRef, Term, Transition, UpdateAction, UpdateExpr, UpdateStatement,
    VersionPlane, WhereClause, transition_state,
};
use crate::error::KipError;

/// Assertion payload that is immutable after creation (Spec §13.7).
///
/// These are the field names §13.2 gives the slots, which is also what KML
/// source writes and what the wire view renders — one spelling per slot, so
/// there is no second name that reaches the same state unguarded.
///
/// **Known divergence from `@ldclabs/kip-lang`**, whose list carries
/// `evidence_refs` — a name from an older draft of the wire shape — but not
/// `evidence`: on that side,
/// `UPDATE ?a SET FIELDS { evidence: ... } WHERE { ?a ASSERTION {...} }`
/// parses. §13.7 lists "initial Evidence citations" among the immutable
/// payload, so rewriting them is exactly the epistemic rewrite it forbids.
/// The differential fixture is a corpus of commands the reference *accepts*,
/// which is why the two lists can drift without the parity test noticing;
/// `an_assertions_citations_are_immutable` below is the basis for it.
const ASSERTION_IMMUTABLE: &[&str] = &[
    "proposition",
    "asserted_by",
    "stance",
    "mode",
    "confidence",
    "asserted_at",
    "valid_time",
    "context_refs",
    "evidence",
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

/// Parses a KML statement: a `MUTATE` block, or a single mutation that is still
/// a one-clause transaction.
pub(crate) fn parse_kml_statement(input: &str) -> VResult<'_, KmlStatement> {
    // DEFINE changes the Schema Environment every later clause resolves
    // against, so it is a statement of its own (Spec §20.16).
    if let Ok((rest, _)) = ws(word("DEFINE")).parse(input) {
        let (rest, define) = cut(define_body).parse(rest)?;
        return Ok((
            rest,
            KmlStatement {
                explicit_transaction: false,
                clauses: vec![MutationClause::Define(define)],
            },
        ));
    }
    if let Ok((rest, _)) = ws(word("MUTATE")).parse(input) {
        let (rest, groups) = cut(braced(many0(ws(mutation_clause)))).parse(rest)?;
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

    let (rest, group) = ws(mutation_clause).parse(input)?;
    Ok((
        rest,
        KmlStatement {
            explicit_transaction: false,
            clauses: flatten(vec![group]),
        },
    ))
}

/// Only ASSERT needs deferred lowering to assign its synthetic handle.
enum ClauseGroup {
    Single(MutationClause),
    Assert {
        written_handle: Option<String>,
        triple: (Term, PredAtom, Term),
        members: Box<AssertMembers>,
        superseding: Option<ElementRef>,
    },
}

fn flatten(groups: Vec<ClauseGroup>) -> Vec<MutationClause> {
    let mut clauses = Vec::with_capacity(groups.len());
    for (seq, group) in groups.into_iter().enumerate() {
        match group {
            ClauseGroup::Single(clause) => clauses.push(clause),
            ClauseGroup::Assert {
                written_handle,
                triple,
                members,
                superseding,
            } => {
                // `#` cannot occur in a source identifier; seq separates the
                // generated handles of otherwise anonymous ASSERT clauses.
                let assertion_handle = written_handle.unwrap_or_else(|| format!("#assert{seq}"));
                let proposition_handle = format!("{assertion_handle}#proposition");
                clauses.push(MutationClause::EnsureProposition(EnsureProposition {
                    handle: Some(proposition_handle.clone()),
                    subject: triple.0,
                    predicate: triple.1,
                    object: triple.2,
                    expect_versions: Vec::new(),
                }));
                clauses.push(MutationClause::CreateAssertion(
                    members.into_record(assertion_handle.clone(), proposition_handle),
                ));
                if let Some(target) = superseding {
                    clauses.push(MutationClause::Transition(Transition {
                        target,
                        to: Scalar::Literal(KipValue::String(transition_state::SUPERSEDED.into())),
                        by: Some(ElementRef::Handle(assertion_handle)),
                        set_fields: None,
                        set_structural: None,
                        where_clauses: None,
                        limit: None,
                        expect_versions: Vec::new(),
                    }));
                }
            }
        }
    }
    clauses
}

fn single(clause: MutationClause) -> ClauseGroup {
    ClauseGroup::Single(clause)
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
        map(transition_statement, |c| {
            single(MutationClause::Transition(c))
        }),
        map(set_retention, |c| single(MutationClause::SetRetention(c))),
        // Before `purge_statement`, which cuts after its verb: `PURGE PAYLOAD`
        // would otherwise reach `cut(element_ref)` on the word `PAYLOAD` and
        // abort the whole alternation instead of falling through.
        map(purge_payload_statement, |c| {
            single(MutationClause::PurgePayload(c))
        }),
        map(purge_statement, |c| single(MutationClause::Purge(c))),
        map(merge_concept, |c| single(MutationClause::MergeConcept(c))),
        // Only reachable inside MUTATE: a standalone DEFINE is taken first.
        preceded(ws(word("DEFINE")), |i| {
            fail(
                i,
                "a mutation: DEFINE is a standalone operation and cannot appear inside MUTATE",
            )
        }),
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

/// `EXPECT VERSION n [OF ATTRIBUTES | STRUCTURAL | RETENTION | FACET "X"]`
fn expect_version_clause(input: &str) -> VResult<'_, ExpectVersion> {
    let (input, _) = ws(words(&["EXPECT", "VERSION"])).parse(input)?;
    let (input, version) = cut(ws(scalar)).parse(input)?;
    let (input, plane) = opt_after(&["OF"], ws(version_plane)).parse(input)?;
    Ok((input, ExpectVersion { version, plane }))
}

fn version_plane(input: &str) -> VResult<'_, VersionPlane> {
    alt((
        value(VersionPlane::Attributes, word("ATTRIBUTES")),
        value(VersionPlane::Structural, word("STRUCTURAL")),
        value(VersionPlane::Retention, word("RETENTION")),
        map(
            preceded(ws(word("FACET")), cut(ws(symbol_ref))),
            VersionPlane::Facet,
        ),
    ))
    .parse(input)
}

/// `{ expect_version_clause }` — the trailing guards every mutation ends with
/// (Spec §52.8), at most one per plane (§35.1).
fn expect_version_clauses(input: &str) -> VResult<'_, Vec<ExpectVersion>> {
    let (rest, guards) = many0(ws(spanned(expect_version_clause))).parse(input)?;
    let mut seen = BTreeSet::new();
    let mut out = Vec::with_capacity(guards.len());
    for (position, guard) in guards {
        if !seen.insert(guard.plane_key()) {
            return fail(
                position,
                "one EXPECT VERSION guard per plane: two guards on one plane cannot both be meant",
            );
        }
        out.push(guard);
    }
    Ok((rest, out))
}

/// Re-checks the one-guard-per-plane rule on a tree that did not come through
/// [`expect_version_clauses`].
fn check_guards(guards: &[ExpectVersion]) -> Result<(), KipError> {
    let mut seen = BTreeSet::new();
    for guard in guards {
        super::validation::scalar(&guard.version)?;
        if !seen.insert(guard.plane_key()) {
            return Err(KipError::invalid_syntax(format!(
                "EXPECT VERSION guards the {} plane twice; one guard per plane",
                guard.plane_key()
            )));
        }
    }
    Ok(())
}

/// Folds a body into typed slots, rejecting a second clause for a single slot.
struct Body {
    r#type: Option<SymbolRef>,
    client_key: Option<Scalar>,
    name: Option<Scalar>,
    r#match: Option<crate::ast::ObjectMatcher>,
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
    // §52.8: the guard follows the closing brace, where every other mutation
    // keeps its preconditions.
    let (rest, expect_versions) = expect_version_clauses(rest)?;

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
            expect_versions,
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
    let triple = match creatable_tuple(matcher) {
        Ok(triple) => triple,
        Err(ctx) => return fail(input, ctx),
    };
    let (rest, expect_versions) = expect_version_clauses(tuple_at)?;

    Ok((
        rest,
        EnsureProposition {
            handle,
            subject: triple.0,
            predicate: triple.1,
            object: triple.2,
            expect_versions,
        },
    ))
}

/// Resolves the tuple a resolve-or-create statement needs.
///
/// `(id: ...)` is match-only: it names a Proposition that must already exist, so
/// it cannot drive `ENSURE PROPOSITION` — or the `ASSERT` sugar that desugars
/// through it — whose job is to create the tuple when it is absent.
fn creatable_tuple(matcher: PropositionMatcher) -> Result<(Term, PredAtom, Term), &'static str> {
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
    let triple = match creatable_tuple(matcher) {
        Ok(triple) => triple,
        Err(ctx) => return fail(input, ctx),
    };
    let (rest, members) = cut(ws(assignments)).parse(members_at)?;
    let members = match AssertMembers::read(members) {
        Ok(members) => members,
        Err(expected) => return fail(members_at, expected),
    };
    let (rest, superseding) = opt_after(&["SUPERSEDING"], ws(element_ref)).parse(rest)?;
    Ok((
        rest,
        ClauseGroup::Assert {
            written_handle,
            triple,
            members: Box::new(members),
            superseding,
        },
    ))
}

/// The members an `ASSERT` block may carry (§55.1), read once.
struct AssertMembers {
    by: MutationValue,
    mode: MutationValue,
    stance: MutationValue,
    confidence: Option<MutationValue>,
    asserted_at: Option<MutationValue>,
    valid_time: Option<MutationValue>,
    context: Option<MutationValue>,
    evidence: Option<MutationValue>,
    client_key: Option<Scalar>,
}

impl AssertMembers {
    fn read(members: Assignments) -> Result<Self, &'static str> {
        let (mut by, mut mode, mut stance, mut confidence) = (None, None, None, None);
        let (mut asserted_at, mut valid_time, mut evidence, mut key) = (None, None, None, None);
        let mut context = None;
        for (name, value) in members {
            let slot = match name.as_str() {
                "by" => &mut by,
                "mode" => &mut mode,
                "stance" => &mut stance,
                "confidence" => &mut confidence,
                "at" => &mut asserted_at,
                "valid" => &mut valid_time,
                "context" => &mut context,
                "evidence" => &mut evidence,
                "key" => &mut key,
                _ => {
                    return Err(
                        "an ASSERT member: by, mode, stance, confidence, at, valid, context, \
                         evidence or key",
                    );
                }
            };
            *slot = Some(value);
        }
        let client_key = match key {
            None => None,
            Some(MutationValue::Param(name)) => Some(Scalar::Param(name)),
            Some(MutationValue::Value(
                value @ (KipValue::String(_)
                | KipValue::Number(_)
                | KipValue::Bool(_)
                | KipValue::Null),
            )) => Some(Scalar::Literal(value)),
            Some(_) => return Err("a literal or :parameter for the ASSERT key member"),
        };
        Ok(Self {
            by: by.ok_or("by: <semantic actor> — an Assertion needs an assertor")?,
            mode: mode
                .ok_or("mode: observed, stated, inferred, predicted, hypothetical or imported")?,
            stance: stance.unwrap_or(MutationValue::Value(KipValue::String("support".into()))),
            confidence,
            asserted_at,
            valid_time,
            context,
            evidence,
            client_key,
        })
    }

    fn into_record(self, handle: String, proposition_handle: String) -> RecordCreate {
        let mut fields = vec![
            (
                "proposition".into(),
                MutationValue::Handle(proposition_handle),
            ),
            ("asserted_by".into(), self.by),
            ("mode".into(), self.mode),
            ("stance".into(), self.stance),
        ];
        for (name, value) in [
            ("confidence", self.confidence),
            ("asserted_at", self.asserted_at),
            ("valid_time", self.valid_time),
            // The scope the stance holds under (§13.3), immutable payload.
            ("context_refs", self.context),
        ] {
            if let Some(value) = value {
                fields.push((name.into(), value));
            }
        }
        // Every sugar citation is a role-qualified Core structural edge.
        let edges: Vec<_> = self
            .evidence
            .into_iter()
            .flat_map(evidence_refs)
            .map(|value| StructuralEdge {
                field: SymbolRef::Name("evidence".into()),
                value,
                options: Some(
                    [(
                        "role".into(),
                        BoundValue::Value(KipValue::String("support".into())),
                    )]
                    .into_iter()
                    .collect(),
                ),
            })
            .collect();
        RecordCreate {
            handle,
            client_key: self.client_key,
            set_fields: Some(fields),
            set_facets: Vec::new(),
            set_structural: (!edges.is_empty()).then_some(edges),
        }
    }
}

/// Splits an `evidence:` member into one citation per artifact.
fn evidence_refs(value: MutationValue) -> Vec<MutationValue> {
    match value {
        MutationValue::Array(items) => items.into_iter().map(MutationValue::from).collect(),
        // A wholly literal array collapsed on the way in; it still cites one
        // artifact per element.
        MutationValue::Value(KipValue::Array(items)) => {
            items.into_iter().map(MutationValue::Value).collect()
        }
        other => vec![other],
    }
}

// ---------------------------------------------------------------------------
// UPDATE and the lifecycle family
// ---------------------------------------------------------------------------

/// A statement's selection block, the bound on its match set and its guards.
type Selection = (Option<Vec<WhereClause>>, Option<Scalar>, Vec<ExpectVersion>);

/// `[WHERE {...}] [LIMIT n] {EXPECT VERSION ...}` — how every mutation that can
/// select by pattern ends: the selection, then its bound (§52.7), then the
/// guards, last (§52.8).
fn selection(input: &str) -> VResult<'_, Selection> {
    let (input, where_clauses) =
        opt_after(&["WHERE"], |i| where_block(i, Flavor::Exact)).parse(input)?;
    let (input, limit) = opt_after(&["LIMIT"], ws(scalar)).parse(input)?;
    let (input, expect_versions) = expect_version_clauses(input)?;
    Ok((input, (where_clauses, limit, expect_versions)))
}

fn update_statement(input: &str) -> VResult<'_, UpdateStatement> {
    let (input, _) = ws(word("UPDATE")).parse(input)?;
    let (start, target) = cut(ws(element_ref)).parse(input)?;
    let (rest, actions) = many0(ws(update_action)).parse(start)?;
    if actions.is_empty() {
        // §52.8: a guard never sits between the target and the actions, so
        // `UPDATE :x EXPECT VERSION :v SET ...` lands here, with no action
        // read — and is refused for the reason it is wrong.
        if ws(words(&["EXPECT", "VERSION"])).parse(start).is_ok() {
            return fail(
                start,
                "the SET / UNSET actions before EXPECT VERSION: a guard is the trailing clause of a \
                 mutation, after WHERE and LIMIT",
            );
        }
        return fail(start, "at least one SET or UNSET action");
    }
    let (rest, (where_clauses, limit, expect_versions)) = selection(rest)?;

    let statement = UpdateStatement {
        target,
        actions,
        where_clauses,
        limit,
        expect_versions,
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
             Assertion instead: a changed world is a new Assertion from the time of the change \
             (§25.4); add SUPERSEDING only when the old Assertion was wrong (§14.2)",
        ),
        Some(BoundKind::Evidence) => Err(
            "a mutable target: correct Evidence topology with TRANSITION :old TO \"corrected\" \
             BY :new",
        ),
        Some(BoundKind::Proposition) => {
            Err("a target with structural fields: a Proposition is its tuple and carries none")
        }
        Some(BoundKind::Activity) => Err(
            "a mutable target: finalize a pending Activity with TRANSITION ... TO \"completed\" \
             SET STRUCTURAL; a terminal Activity is immutable",
        ),
        _ => Ok(()),
    }
}

fn guard_immutable_field(field: &str, kind: Option<BoundKind>) -> Result<(), &'static str> {
    match kind {
        Some(BoundKind::Assertion) if ASSERTION_IMMUTABLE.contains(&field) => Err(
            "a mutable field: immutable Assertion payload is never rewritten — record a new \
             Assertion instead: a changed world is a new Assertion from the time of the change \
             (§25.4); add SUPERSEDING only when the old Assertion was wrong (§14.2)",
        ),
        Some(BoundKind::Evidence) if EVIDENCE_IMMUTABLE.contains(&field) => Err(
            "a mutable field: immutable Evidence payload is corrected with TRANSITION :old TO \
             \"corrected\" BY :new",
        ),
        Some(BoundKind::Proposition) if PROPOSITION_IMMUTABLE.contains(&field) => Err(
            "a mutable field: the Proposition tuple is immutable — a different tuple is a \
             different Proposition",
        ),
        _ => Ok(()),
    }
}

/// `TRANSITION target TO "state" [BY ref] {SET FIELDS | SET STRUCTURAL}
/// [WHERE] [LIMIT] {EXPECT VERSION}` — the one lifecycle statement (Spec §52.5).
///
/// The state names the move; which states fit which target kind, and which
/// current state a move is legal from, is the engine's check. What the grammar
/// fixes is the shape: `BY` exactly for `superseded` / `corrected`, and a
/// finalizing `SET` only on an Activity state. There is no `EXPECT STATE` —
/// the transition validates the current state itself (§35.3).
fn transition_statement(input: &str) -> VResult<'_, Transition> {
    let (input, _) = ws(word("TRANSITION")).parse(input)?;
    let (input, target) = cut(ws(element_ref)).parse(input)?;
    let (input, _) = cut(ws(word("TO"))).parse(input)?;
    let (start, to) = cut(ws(scalar)).parse(input)?;
    let (rest, by) = opt_after(&["BY"], ws(element_ref)).parse(start)?;

    /// What a transition may finalize in the same statement.
    enum Finalize {
        Fields(Assignments),
        Structural(Vec<StructuralEdge>),
    }
    let (rest, finalize) = many0(ws(spanned(alt((
        map(
            preceded(ws(words(&["SET", "FIELDS"])), cut(ws(assignments))),
            Finalize::Fields,
        ),
        map(
            preceded(ws(words(&["SET", "STRUCTURAL"])), cut(structural_edges)),
            Finalize::Structural,
        ),
    )))))
    .parse(rest)?;

    let mut set_fields = None;
    let mut set_structural = None;
    for (position, clause) in finalize {
        let repeated = match clause {
            Finalize::Fields(fields) => set_fields
                .replace(fields)
                .is_some()
                .then_some("at most one SET FIELDS clause"),
            Finalize::Structural(edges) => set_structural
                .replace(edges)
                .is_some()
                .then_some("at most one SET STRUCTURAL clause"),
        };
        if let Some(expected) = repeated {
            return fail(position, expected);
        }
    }

    if ws(words(&["EXPECT", "STATE"])).parse(rest).is_ok() {
        return fail(
            rest,
            "no EXPECT STATE: TRANSITION validates the current lifecycle state itself and fails \
             InvalidLifecycleTransition from the wrong one; guard the version instead",
        );
    }
    let (rest, (where_clauses, limit, expect_versions)) = selection(rest)?;

    let statement = Transition {
        target,
        to,
        by,
        set_fields,
        set_structural,
        where_clauses,
        limit,
        expect_versions,
    };
    if let Err(ctx) = check_transition_shape(&statement) {
        return fail(start, ctx);
    }
    Ok((rest, statement))
}

/// The shape rules §52.5 calls syntax errors, decidable from the literal state.
///
/// A `:parameter` state is bound at execution time, so nothing here can judge
/// it; the engine applies the same rules once it knows the state.
fn check_transition_shape(statement: &Transition) -> Result<(), &'static str> {
    let Some(state) = statement.state() else {
        return Ok(());
    };
    let with_by = transition_state::WITH_BY.contains(&state);
    if with_by && statement.by.is_none() {
        return Err(
            "BY <the replacing element>: TRANSITION TO \"superseded\" names the newer Assertion \
             and TO \"corrected\" the new Evidence",
        );
    }
    if !with_by && statement.by.is_some() {
        return Err(
            "no BY: only TRANSITION TO \"superseded\" / \"corrected\" names a replacing element",
        );
    }
    if statement.finalizes() && !transition_state::ACTIVITY.contains(&state) {
        return Err(
            "no SET FIELDS / SET STRUCTURAL: only a move to an Activity state (running, \
             completed, failed, cancelled) finalizes fields or topology",
        );
    }
    Ok(())
}

fn set_retention(input: &str) -> VResult<'_, SetRetention> {
    let (input, _) = ws(words(&["SET", "RETENTION"])).parse(input)?;
    let (input, target) = cut(ws(element_ref)).parse(input)?;
    let (input, values) = cut(ws(assignments)).parse(input)?;
    let (input, (where_clauses, limit, expect_versions)) = selection(input)?;
    Ok((
        input,
        SetRetention {
            target,
            values,
            where_clauses,
            limit,
            expect_versions,
        },
    ))
}

/// `CONFIRM "PURGE"`, the one spelling both erasures take, frozen so an
/// erasure is never the result of a near-miss confirmation.
fn purge_confirmation(input: &str) -> VResult<'_, String> {
    let (input, _) = cut(ws(word("CONFIRM"))).parse(input)?;
    let (rest, confirm) = cut(ws(quoted_string)).parse(input)?;
    if confirm != "PURGE" {
        return fail(input, "the exact confirmation literal \"PURGE\"");
    }
    Ok((rest, confirm))
}

fn purge_statement(input: &str) -> VResult<'_, crate::ast::PurgeStatement> {
    let (input, _) = ws(word("PURGE")).parse(input)?;
    let (input, target) = cut(ws(element_ref)).parse(input)?;
    // §52.8: the guards, then the statement's own trailing words.
    let (input, (where_clauses, limit, expect_versions)) = selection(input)?;
    let (input, reference_policy) = opt_after(&["REFERENCE", "POLICY"], ws(scalar)).parse(input)?;
    let (rest, confirm) = purge_confirmation(input)?;

    Ok((
        rest,
        crate::ast::PurgeStatement {
            target,
            where_clauses,
            limit,
            expect_versions,
            reference_policy,
            confirm,
        },
    ))
}

/// `PURGE PAYLOAD` — Evidence bytes only (Spec §60.6).
///
/// No `REFERENCE POLICY`: the Evidence record survives a payload purge, so no
/// reference can be left dangling and there is nothing for a policy to decide.
fn purge_payload_statement(input: &str) -> VResult<'_, crate::ast::PurgePayloadStatement> {
    let (input, _) = ws(words(&["PURGE", "PAYLOAD"])).parse(input)?;
    let (input, target) = cut(ws(element_ref)).parse(input)?;
    let (input, (where_clauses, limit, expect_versions)) = selection(input)?;
    let (rest, confirm) = purge_confirmation(input)?;

    Ok((
        rest,
        crate::ast::PurgePayloadStatement {
            target,
            where_clauses,
            limit,
            expect_versions,
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
    let (input, expect_versions) = expect_version_clauses(input)?;
    Ok((
        input,
        MergeConcept {
            source,
            into,
            where_clauses,
            expect_versions,
        },
    ))
}

/// `DEFINE ( PREDICATE | CONCEPT TYPE ) schema_symbol object_literal`, after
/// the `DEFINE` word (Spec §20.16).
fn define_body(input: &str) -> VResult<'_, DefineCommand> {
    let (input, kind) = ws(alt((
        value(DefineKind::Predicate, word("PREDICATE")),
        value(DefineKind::ConceptType, words(&["CONCEPT", "TYPE"])),
    )))
    .parse(input)?;
    let (input, name) = ws(symbol_ref).parse(input)?;
    let (input, definition) = ws(bound_object).parse(input)?;
    Ok((
        input,
        DefineCommand {
            kind,
            name,
            definition,
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
    match value {
        MutationValue::Expr(expr) => validate_update_expr(expr),
        MutationValue::Array(items) => items.iter().try_for_each(super::validation::bound),
        MutationValue::Object(entries) => super::validation::entries_unique(entries),
        _ => Ok(()),
    }
}

fn validate_structural_edges(edges: &[StructuralEdge]) -> Result<(), KipError> {
    for edge in edges {
        validate_mutation_value(&edge.value)?;
        if let Some(options) = &edge.options {
            super::validation::bound_object(options)?;
        }
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
        if let Some((index, fault)) = key_fault(a.iter().map(|(key, _)| key.as_str()), true) {
            let key = &a[index].0;
            return Err(KipError::invalid_syntax(match fault {
                KeyFault::Protected => {
                    format!("{key} is engine-maintained state and cannot be written by a mutation")
                }
                KeyFault::Duplicate => format!("{key} is assigned twice in one block"),
            }));
        }
        a.iter()
            .try_for_each(|(_, value)| validate_mutation_value(value))
    };
    let check_unset = |fields: &[String]| -> Result<(), KipError> {
        if let Some((index, fault)) = key_fault(fields.iter().map(String::as_str), true) {
            let key = &fields[index];
            return Err(KipError::invalid_syntax(match fault {
                KeyFault::Protected => {
                    format!("{key} is engine-maintained state and cannot be unset by a mutation")
                }
                KeyFault::Duplicate => format!("{key} is listed twice in one block"),
            }));
        }
        Ok(())
    };
    let check_facets = |facets: &[FacetAssignment]| -> Result<(), KipError> {
        facets.iter().try_for_each(|f| check_assignments(&f.values))
    };

    if let Some(where_clauses) = clause_where(clause) {
        super::validation::patterns(where_clauses, Flavor::Exact)?;
    }

    match clause {
        MutationClause::CreateConcept(c) => {
            for value in [&c.client_key, &c.name].into_iter().flatten() {
                super::validation::scalar(value)?;
            }
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
            check_guards(&c.expect_versions)?;
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
                super::validation::object_matcher(matcher, Flavor::Exact)?;
            }
            if c.unset_structural.as_ref().is_some_and(Vec::is_empty) {
                return bad("UNSET STRUCTURAL removes named references; list at least one");
            }
        }
        MutationClause::CreateEvidence(c)
        | MutationClause::CreateAssertion(c)
        | MutationClause::CreateActivity(c) => {
            if let Some(value) = &c.client_key {
                super::validation::scalar(value)?;
            }
            c.set_fields.as_ref().map_or(Ok(()), &check_assignments)?;
            check_facets(&c.set_facets)?;
            c.set_structural
                .as_deref()
                .map_or(Ok(()), validate_structural_edges)?;
        }
        MutationClause::EnsureProposition(c) => {
            check_guards(&c.expect_versions)?;
            if matches!(c.predicate, PredAtom::Variable(_)) {
                return bad(
                    "ENSURE PROPOSITION needs an exact quoted predicate or :parameter; \
                    ?variables are KQL read-pattern syntax",
                );
            }
            super::validation::proposition_subject(&c.subject, Flavor::Exact)?;
            super::validation::term(&c.object, Flavor::Exact)?;
        }
        MutationClause::Update(c) => {
            if let Some(value) = &c.limit {
                super::validation::scalar(value)?;
            }
            check_guards(&c.expect_versions)?;
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
        MutationClause::Transition(c) => {
            super::validation::scalar(&c.to)?;
            if let Some(value) = &c.limit {
                super::validation::scalar(value)?;
            }
            check_guards(&c.expect_versions)?;
            if let Err(ctx) = check_transition_shape(c) {
                return Err(KipError::invalid_syntax(format!(
                    "TRANSITION expects {ctx}"
                )));
            }
            c.set_fields.as_ref().map_or(Ok(()), &check_assignments)?;
            c.set_structural
                .as_deref()
                .map_or(Ok(()), validate_structural_edges)?;
        }
        MutationClause::SetRetention(c) => {
            if let Some(value) = &c.limit {
                super::validation::scalar(value)?;
            }
            check_guards(&c.expect_versions)?;
            check_assignments(&c.values)?
        }
        // The grammar freezes the spelling so a purge is never the result of a
        // near-miss confirmation.
        MutationClause::Purge(c) => {
            if let Some(value) = &c.reference_policy {
                super::validation::scalar(value)?;
            }
            if let Some(value) = &c.limit {
                super::validation::scalar(value)?;
            }
            check_guards(&c.expect_versions)?;
            if c.confirm != "PURGE" {
                return bad("PURGE must be confirmed with the exact literal \"PURGE\"");
            }
        }
        MutationClause::PurgePayload(c) => {
            if let Some(value) = &c.limit {
                super::validation::scalar(value)?;
            }
            check_guards(&c.expect_versions)?;
            if c.confirm != "PURGE" {
                return bad("PURGE PAYLOAD must be confirmed with the exact literal \"PURGE\"");
            }
        }
        MutationClause::MergeConcept(c) => check_guards(&c.expect_versions)?,
        MutationClause::Define(c) => super::validation::bound_object(&c.definition)?,
    }
    Ok(())
}

/// KML and META selection blocks parse in the exact flavor: no `BELIEF`, and no
/// raw predicate paths. A virtual Projection is never a mutation target or an
/// export selector, and a path never resolves to one Proposition to write.
/// Checks the invariants that only the whole mutation plan can decide.
pub(crate) fn validate_plan(statement: &KmlStatement) -> Result<(), KipError> {
    if statement.clauses.is_empty() {
        return Err(KipError::invalid_syntax(
            "a KML transaction must carry at least one mutation",
        ));
    }
    for clause in &statement.clauses {
        validate_clause(clause)?;
    }
    if (statement.explicit_transaction || statement.clauses.len() > 1)
        && statement
            .clauses
            .iter()
            .any(|clause| matches!(clause, MutationClause::Define(_)))
    {
        return Err(KipError::invalid_syntax(
            "DEFINE is a standalone operation and cannot appear inside MUTATE (Spec §20.16)",
        ));
    }

    // Handles are block-local names. Two clauses claiming the same handle make
    // every forward reference to it ambiguous, so the whole plan is rejected
    // rather than resolved by position.
    let mut plan_handles: BTreeSet<&str> = BTreeSet::new();
    for clause in &statement.clauses {
        if let Some(name) = clause.handle()
            && !plan_handles.insert(name)
        {
            return Err(KipError::duplicate_local_handle(format!(
                "?{name} is claimed by two clauses in one mutation plan"
            )));
        }
    }

    // Every executable handle must be created by this plan or bound by that
    // clause's own WHERE. Parameters remain runtime bindings and are unaffected.
    for clause in &statement.clauses {
        let mut allowed = BTreeSet::new();
        if let Some(where_clauses) = clause_where(clause) {
            collect_where_variables(where_clauses, &mut allowed);
        }
        let mut referenced = BTreeSet::new();
        collect_clause_handles(clause, &mut referenced);
        for name in referenced {
            if !plan_handles.contains(name.as_str()) && !allowed.contains(&name) {
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
        MutationClause::Transition(c) => c.where_clauses.as_ref(),
        MutationClause::SetRetention(c) => c.where_clauses.as_ref(),
        MutationClause::Purge(c) => c.where_clauses.as_ref(),
        MutationClause::PurgePayload(c) => c.where_clauses.as_ref(),
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
        MutationClause::EnsureProposition(c) => {
            super::common::collect_term_variables(&c.subject, out);
            super::common::collect_term_variables(&c.object, out);
        }
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
        MutationClause::Transition(c) => {
            element(&c.target);
            if let Some(by) = &c.by {
                element(by);
            }
            collect_assignments_handles(c.set_fields.as_ref(), out);
            collect_edges_handles(c.set_structural.as_ref(), out);
        }
        MutationClause::SetRetention(c) => {
            element(&c.target);
            collect_assignments_handles(Some(&c.values), out);
        }
        MutationClause::Purge(c) => element(&c.target),
        MutationClause::PurgePayload(c) => element(&c.target),
        MutationClause::MergeConcept(c) => {
            element(&c.source);
            element(&c.into);
        }
        MutationClause::Define(c) => collect_bound_object_handles(&c.definition, out),
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
        let statement = kml(r#"TRANSITION :old TO "archived""#);
        assert!(!statement.explicit_transaction);
        assert_eq!(statement.clauses.len(), 1);

        let explicit = kml(r#"MUTATE { TRANSITION :old TO "archived" }"#);
        assert!(explicit.explicit_transaction);
    }

    #[test]
    fn one_transition_statement_names_every_lifecycle_move() {
        // §52.5: the quoted state names the move. The old per-move verbs are
        // ordinary identifiers now and no longer start a statement.
        for source in [
            r#"TRANSITION :a TO "retracted""#,
            r#"TRANSITION :old TO "superseded" BY ?new"#,
            r#"TRANSITION :e TO "corrected" BY :fixed"#,
            r#"TRANSITION :act TO "running""#,
            r#"TRANSITION :act TO "completed" SET FIELDS { ended_at: :t } SET STRUCTURAL { ("outputs", :o) }"#,
            r#"TRANSITION :x TO "archived" WHERE { ?x CONCEPT {type: "Event"} } LIMIT 10"#,
            r#"TRANSITION ?x TO "tombstoned" WHERE { ?x CONCEPT {type: "Event"} } LIMIT 10 EXPECT VERSION 3"#,
            r#"TRANSITION :a TO :state"#,
        ] {
            assert!(parse_kml_statement(source).is_ok(), "{source}");
        }
        for source in [
            r#"RETRACT ASSERTION :a"#,
            r#"SUPERSEDE ASSERTION :old BY :new"#,
            r#"CORRECT EVIDENCE :old BY :new"#,
            r#"TRANSITION ACTIVITY :act TO "completed""#,
            r#"ARCHIVE :x"#,
            r#"TOMBSTONE :x"#,
        ] {
            assert!(parse_kml_statement(source).is_err(), "{source}");
        }
    }

    #[test]
    fn a_transition_carries_by_exactly_where_the_spec_says() {
        // §52.5: BY for superseded / corrected, and only there.
        assert!(parse_kml_statement(r#"TRANSITION :a TO "superseded""#).is_err());
        assert!(parse_kml_statement(r#"TRANSITION :e TO "corrected""#).is_err());
        assert!(parse_kml_statement(r#"TRANSITION :a TO "retracted" BY :b"#).is_err());
        assert!(parse_kml_statement(r#"TRANSITION :x TO "archived" BY :b"#).is_err());
        // SET clauses finalize an Activity; nothing else has terminal fields.
        assert!(
            parse_kml_statement(r#"TRANSITION :a TO "retracted" SET FIELDS { ended_at: :t }"#)
                .is_err()
        );
        assert!(
            parse_kml_statement(
                r#"TRANSITION :x TO "archived" SET STRUCTURAL { ("outputs", :o) }"#
            )
            .is_err()
        );
        // A parameter state is bound at execution time; the engine applies the
        // same rules once it knows it.
        assert!(parse_kml_statement(r#"TRANSITION :a TO :state BY :b"#).is_ok());
    }

    #[test]
    fn there_is_no_expect_state_guard() {
        // §35.3: the transition validates the current state itself.
        let err = crate::parse_kml(r#"TRANSITION :a TO "retracted" EXPECT STATE "active""#)
            .expect_err("EXPECT STATE is gone");
        assert!(err.message.contains("EXPECT STATE"), "{err}");
        assert!(
            crate::parse_kml(r#"TRANSITION :x TO "archived" WHERE { ?x CONCEPT {id: "C-1"} } EXPECT STATE "active""#)
                .is_err()
        );
    }

    #[test]
    fn expect_version_is_always_the_trailing_clause() {
        // §52.8: after WHERE and LIMIT, after UPSERT's brace, after ENSURE
        // PROPOSITION's tuple — and never between the target and the actions.
        let update = kml(
            r#"UPDATE ?m SET FACET "MnemonicState" {salience: 0.5} WHERE { ?m CONCEPT {id: "C-1"} } LIMIT 1 EXPECT VERSION :v"#,
        );
        let MutationClause::Update(update) = &update.clauses[0] else {
            panic!("expected UPDATE");
        };
        assert_eq!(update.expect_versions.len(), 1);
        assert!(update.expect_versions[0].plane.is_none());

        let err = crate::parse_kml(r#"UPDATE :m EXPECT VERSION :v SET ATTRIBUTES {status: "x"}"#)
            .expect_err("a guard before the actions");
        assert!(err.message.contains("EXPECT VERSION"), "{err}");

        let upsert = kml(
            r#"UPSERT CONCEPT ?c { MATCH {key: "k"} SET FIELDS {name: "n"} } EXPECT VERSION 0"#,
        );
        let MutationClause::UpsertConcept(upsert) = &upsert.clauses[0] else {
            panic!("expected UPSERT");
        };
        assert_eq!(upsert.expect_versions.len(), 1);
        assert!(
            parse_kml_statement(r#"UPSERT CONCEPT ?c { MATCH {key: "k"} EXPECT VERSION 0 }"#)
                .is_err()
        );

        let ensure = kml(r#"ENSURE PROPOSITION ?p (:a, "p", :b) EXPECT VERSION 0"#);
        let MutationClause::EnsureProposition(ensure) = &ensure.clauses[0] else {
            panic!("expected ENSURE");
        };
        assert_eq!(ensure.expect_versions.len(), 1);

        // PURGE keeps its own trailing words after the guards.
        assert!(
            parse_kml_statement(
                r#"PURGE :e WHERE { ?e EVIDENCE {id: "E-1"} } LIMIT 1 EXPECT VERSION 2 REFERENCE POLICY "deny_if_referenced" CONFIRM "PURGE""#
            )
            .is_ok()
        );
        assert!(
            parse_kml_statement(r#"PURGE PAYLOAD :e EXPECT VERSION 2 CONFIRM "PURGE""#).is_ok()
        );
        assert!(
            parse_kml_statement(
                r#"SET RETENTION :e {retention_class: "standard"} EXPECT VERSION 2"#
            )
            .is_ok()
        );
    }

    #[test]
    fn a_guard_may_name_a_version_plane_once() {
        // §35.1: one guard per plane; the bare guard is the element's version.
        let statement = kml(
            r#"UPDATE :skill SET ATTRIBUTES {status: "adopted"} SET FACET "GradingState" {graded_count: 12}
               EXPECT VERSION :va OF ATTRIBUTES EXPECT VERSION 0 OF FACET "GradingState" EXPECT VERSION :vs OF STRUCTURAL EXPECT VERSION :vr OF RETENTION EXPECT VERSION :v"#,
        );
        let MutationClause::Update(update) = &statement.clauses[0] else {
            panic!("expected UPDATE");
        };
        let planes: Vec<Option<VersionPlane>> = update
            .expect_versions
            .iter()
            .map(|g| g.plane.clone())
            .collect();
        assert_eq!(
            planes,
            vec![
                Some(VersionPlane::Attributes),
                Some(VersionPlane::Facet(SymbolRef::Name("GradingState".into()))),
                Some(VersionPlane::Structural),
                Some(VersionPlane::Retention),
                None,
            ]
        );

        for source in [
            r#"UPDATE :e SET ATTRIBUTES {a: 1} EXPECT VERSION :a OF ATTRIBUTES EXPECT VERSION :b OF ATTRIBUTES"#,
            r#"UPDATE :e SET ATTRIBUTES {a: 1} EXPECT VERSION 1 EXPECT VERSION 2"#,
            r#"UPDATE :e SET FACET "F" {a: 1} EXPECT VERSION 1 OF FACET "F" EXPECT VERSION 2 OF FACET "F""#,
            r#"UPDATE :e SET ATTRIBUTES {a: 1} EXPECT VERSION 1 OF NOWHERE"#,
        ] {
            assert!(parse_kml_statement(source).is_err(), "{source}");
        }

        // The same rule holds for a pre-parsed tree (§73).
        let mut duplicated = kml(r#"UPDATE :e SET ATTRIBUTES {a: 1} EXPECT VERSION 1"#);
        let MutationClause::Update(update) = &mut duplicated.clauses[0] else {
            unreachable!()
        };
        let guard = update.expect_versions[0].clone();
        update.expect_versions.push(guard);
        assert!(validate_plan(&duplicated).is_err());
    }

    #[test]
    fn mutate_needs_at_least_one_mutation() {
        assert!(parse_kml_statement("MUTATE { }").is_err());
    }

    #[test]
    fn purge_payload_is_its_own_statement_not_a_purge_of_something_named_payload() {
        // §60.6: `PURGE PAYLOAD` erases Evidence bytes while the element
        // survives. The verb overlaps with element purge, so the two are one
        // lookahead apart — and getting that wrong would turn a payload purge
        // into a syntax error, or worse, into an element purge.
        let statement = kml(r#"PURGE PAYLOAD :e CONFIRM "PURGE""#);
        assert!(matches!(
            statement.clauses.as_slice(),
            [MutationClause::PurgePayload(_)]
        ));

        let element = kml(r#"PURGE :e CONFIRM "PURGE""#);
        assert!(matches!(
            element.clauses.as_slice(),
            [MutationClause::Purge(_)]
        ));
    }

    #[test]
    fn purge_payload_takes_no_reference_policy() {
        // The Evidence record survives, so no reference can dangle and there
        // is nothing for a policy to decide (§60.6).
        assert!(
            parse_kml_statement(
                r#"PURGE PAYLOAD :e REFERENCE POLICY "tombstone_reference" CONFIRM "PURGE""#
            )
            .is_err()
        );
    }

    #[test]
    fn purge_payload_freezes_its_confirmation_spelling() {
        assert!(parse_kml_statement(r#"PURGE PAYLOAD :e CONFIRM "purge""#).is_err());
        assert!(parse_kml_statement(r#"PURGE PAYLOAD :e"#).is_err());
    }

    #[test]
    fn an_assertions_citations_are_immutable() {
        // §13.7 makes the initial Evidence citations immutable payload, so an
        // UPDATE naming the slot is a forbidden rewrite — record a new
        // Assertion with SUPERSEDING instead.
        //
        // This is the basis for a known divergence from `@ldclabs/kip-lang`,
        // which guards `evidence_refs` — a name from an older draft of the
        // wire shape — and lets `evidence` through. The parity fixture is a
        // corpus of commands the reference accepts, so nothing there would
        // catch the two lists drifting apart.
        for field in ASSERTION_IMMUTABLE {
            let source = format!(
                r#"UPDATE ?a SET FIELDS {{ {field}: :v }} WHERE {{ ?a ASSERTION {{id: "A-1"}} }}"#
            );
            assert!(
                parse_kml_statement(&source).is_err(),
                "an UPDATE rewriting {field} must not parse"
            );
        }

        // The rule is about the Assertion's own citations. A Concept field
        // that happens to be called `evidence` is ordinary mutable state.
        assert!(
            parse_kml_statement(
                r#"UPDATE ?c SET FIELDS { evidence: :e } WHERE { ?c CONCEPT {id: "C-1"} }"#
            )
            .is_ok()
        );
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
        // §55.1: `SUPERSEDING :old` desugars to the one lifecycle statement,
        // `TRANSITION :old TO "superseded" BY <the new Assertion>`.
        let statement =
            kml(r#"ASSERT ?new (:a, "p", :b) { by: :me, mode: "stated" } SUPERSEDING :old"#);
        assert_eq!(statement.clauses.len(), 3);
        let MutationClause::Transition(supersede) = &statement.clauses[2] else {
            panic!("expected TRANSITION");
        };
        assert_eq!(supersede.state(), Some("superseded"));
        assert_eq!(supersede.by, Some(ElementRef::Handle("new".into())));
        assert_eq!(supersede.target, ElementRef::Param("old".into()));
        assert!(supersede.where_clauses.is_none());
        assert!(supersede.expect_versions.is_empty());
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
        let mut archive = kml(
            r#"TRANSITION ?p TO "archived" WHERE { ?p PROPOSITION (:a, "related_to", :b) } LIMIT 1"#,
        );
        let MutationClause::Transition(archive_clause) = &mut archive.clauses[0] else {
            unreachable!()
        };
        archive_clause.where_clauses = Some(query.where_clauses);
        assert!(validate_plan(&archive).is_err(), "raw predicate path");

        // §52.5's shape rules hold for a transported tree too: a BY on a
        // state that takes none, or a finalizing SET on a non-Activity state.
        let mut retract = kml(r#"TRANSITION :a TO "retracted""#);
        let MutationClause::Transition(clause) = &mut retract.clauses[0] else {
            unreachable!()
        };
        clause.by = Some(ElementRef::Param("b".into()));
        assert!(validate_plan(&retract).is_err(), "BY on retracted");
        let mut archive = kml(r#"TRANSITION :x TO "archived""#);
        let MutationClause::Transition(clause) = &mut archive.clauses[0] else {
            unreachable!()
        };
        clause.set_fields = Some(vec![("ended_at".into(), MutationValue::Param("t".into()))]);
        assert!(validate_plan(&archive).is_err(), "SET on archived");
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
            parse_kml_statement(r#"TRANSITION :act TO "completed" SET FIELDS { ended_at: :now }"#)
                .is_ok()
        );
        assert!(
            parse_kml_statement(
                r#"TRANSITION :act TO "completed" SET FIELDS { a: 1 } SET FIELDS { b: 2 }"#
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
        assert_eq!(merge.expect_versions.len(), 1);
        // MERGE takes no LIMIT: both operands are already named, and its WHERE
        // only guards. The trailing clause is simply not part of the statement.
        assert!(crate::parser::parse_kml(r#"MERGE CONCEPT :a INTO :b LIMIT 1"#).is_err());
    }
}
