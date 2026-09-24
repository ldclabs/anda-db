//! # Static checks the Core Package decides on its own (Spec §20.13)
//!
//! `kip://core@2.0.0` is a virtual Schema Package the Specification defines
//! itself: implicitly active in every Schema Environment, never deactivated,
//! never shadowed. Its registries therefore hold no matter which packages a
//! Space has installed, which makes them decidable *here* — before an engine,
//! before a Schema Environment, before a transaction opens.
//!
//! ```text
//! stance                support | reject | uncertain
//! mode                  observed | stated | inferred | predicted | hypothetical | imported
//! Assertion lifecycle   active | retracted | superseded | expired (computed, §14.3)
//! Evidence lifecycle    active | corrected
//! Evidence role         support | challenge | context
//! Activity status       pending | running | completed | failed | cancelled
//! Activity terminal     completed | failed | cancelled
//! belief status         accepted | rejected | contested | uncertain | insufficient
//! TRANSITION states     retracted | superseded | corrected | running | completed |
//!                       failed | cancelled | archived | tombstoned          (§52.5)
//! ```
//!
//! Two boundaries this module holds to, because crossing either turns a
//! useful check into a wrong one:
//!
//! - **Only written literals are checked.** A `:parameter` is bound from the
//!   request envelope at execution time, so nothing here can know its value;
//!   guessing would reject valid commands.
//! - **Only protocol-fixed vocabulary is checked.** `confidence` is `[0,1]`
//!   because §13.6 says so. The Cognitive Memory Profile fixes
//!   `memory_strength`, `salience` and `utility` to the same interval, but
//!   those belong to a *package* — a Space running a different Profile may
//!   legitimately mean something else by them, and hard-coding package
//!   semantics into the protocol layer would reject commands the
//!   Specification admits. Those stay with the engine, which is the only party
//!   that knows the active Schema Environment.
//!
//! Two things are deliberately *not* checked here:
//!
//! - which `TRANSITION` state fits which target kind, and which current state
//!   a move is legal from. §52.5 makes that the engine's check
//!   (`InvalidLifecycleTransition`); only the state *vocabulary* is fixed by
//!   the language, and only that is checked.
//! - `SEARCH ... THRESHOLD`. The engine checks the `[0,1]` retrieval-score
//!   contract in §66.4 after binding, identically for literal and parameter
//!   operands. Syntactic acceptance here is not execution-time validity.

use std::fmt;

use crate::ast::{
    BoundValue, Command, KipValue, KmlStatement, KqlQuery, MetaCommand, MutationClause,
    MutationValue, Scalar, StructuralEdge, UpdateAction,
};
use crate::error::{KipError, KipErrorCode};

// The registries below are views, not copies. A vocabulary this crate already
// types is spelled once, by the enum that carries it, and read back here
// through `NAMES` — so a variant added to `Stance` cannot leave the checker
// refusing a value the engine can produce. Only the vocabularies no enum
// models — the ones an element carries as a plain `String` — are written out.

/// `stance` — what an Assertion does with its Proposition (§13.4).
pub const STANCES: &[&str] = crate::types::Stance::NAMES;

/// `mode` — how an Assertion was arrived at (§13.5).
pub const ASSERTION_MODES: &[&str] = crate::types::AssertionMode::NAMES;

/// The Assertion lifecycle states (§14).
///
/// `expired` is computed, never stored (§14.3): no statement produces it, and
/// the stored status stays `active`, `retracted` or `superseded`.
pub const ASSERTION_LIFECYCLE: &[&str] = crate::types::AssertionStatus::NAMES;

/// The Evidence lifecycle states (§57.2).
pub const EVIDENCE_LIFECYCLE: &[&str] = &["active", "corrected"];

/// What an Evidence citation does for a claim (§56.2).
pub const EVIDENCE_ROLES: &[&str] = &["support", "challenge", "context"];

/// The Activity statuses (§16).
pub const ACTIVITY_STATUS: &[&str] = &["pending", "running", "completed", "failed", "cancelled"];

/// The Activity terminal states (§16.6).
pub const ACTIVITY_TERMINAL: &[&str] = &["completed", "failed", "cancelled"];

/// The states `TRANSITION ... TO` may name (§52.5).
pub const TRANSITION_STATES: &[&str] = crate::ast::transition_state::ALL;

/// The belief statuses an Epistemic Projection can return (§21.3).
pub const BELIEF_STATUSES: &[&str] = crate::types::BeliefStatus::NAMES;

/// The baseline SEARCH modes (§66.3).
pub const SEARCH_MODES: &[&str] = crate::request::SearchMode::NAMES;

/// The Core element kinds `kip://core` exports (§20.13).
pub const CORE_ELEMENT_KINDS: &[&str] = &[
    "Concept",
    "Proposition",
    "Assertion",
    "Evidence",
    "Activity",
];

/// The reserved Core structural fields `kip://core` exports, each with the
/// Core kind that owns it (§20.13).
///
/// Resolved by the source element's Core kind, never through a package alias
/// — which is why a package field of the same name, carried by the *same*
/// kind, would change what an Assertion cites without changing any command
/// that reads it. On a different kind there is nothing to shadow: a Concept
/// owns no Core structural field, so a Profile `evidence` on a Concept is a
/// separate plane, addressed the same way every Profile field is.
pub const CORE_STRUCTURAL_FIELDS: &[(&str, &str)] = &[
    ("evidence", "Assertion"),
    ("context", "Assertion"),
    ("source", "Evidence"),
    ("generated_by", "Evidence"),
    ("inputs", "Activity"),
    ("outputs", "Activity"),
    ("associated_actors", "Activity"),
];

/// The Core kind that owns a reserved structural field name, if one does.
pub fn core_structural_owner(name: &str) -> Option<&'static str> {
    CORE_STRUCTURAL_FIELDS
        .iter()
        .find(|(field, _)| *field == name)
        .map(|(_, owner)| *owner)
}

// §20.13: "A Schema Package MUST NOT define or alias a symbol that shadows a
// reserved Core symbol name in its resolution scope." The two namespaces are
// checked apart by the engine installing the package: `Assertion` is a shadow
// as a type name, `inputs` as a structural field. A Predicate called `source`
// is a claim about origin and shadows nothing.

/// The `DESCRIBE PRIMER` modes (§64).
pub const PRIMER_MODES: &[&str] = &["compact", "full"];

/// The explanation levels `WITH EPISTEMIC` accepts (§49.1).
pub const EXPLANATION_LEVELS: &[&str] = &["none", "summary", "ledger"];

/// How much a diagnostic matters.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Severity {
    /// The command violates a rule the Core Package fixes. [`check`] turns
    /// these into errors, so a command carrying one never reaches an engine.
    Error,
    /// The command is legal but is the shape a mistake usually takes.
    Warning,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Severity::Error => f.write_str("error"),
            Severity::Warning => f.write_str("warning"),
        }
    }
}

/// One finding about a parsed command.
#[derive(Clone, Debug, PartialEq)]
pub struct Diagnostic {
    /// How much it matters.
    pub severity: Severity,
    /// The registry code it would be reported under.
    pub code: KipErrorCode,
    /// What is wrong, and what would be right.
    pub message: String,
}

impl Diagnostic {
    fn error(code: KipErrorCode, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Error,
            code,
            message: message.into(),
        }
    }

    fn warning(code: KipErrorCode, message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Warning,
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.severity, self.message)
    }
}

impl From<Diagnostic> for KipError {
    fn from(diagnostic: Diagnostic) -> Self {
        KipError::new(diagnostic.code, diagnostic.message)
    }
}

/// Reports everything the Core Package can decide about a parsed command.
///
/// Warnings are included, so this is the entry point for a tool that shows
/// findings rather than rejecting; [`check`] is the one that rejects.
///
/// # Examples
///
/// ```rust
/// use anda_kip::{Severity, analyze, parse_kip};
///
/// let command = parse_kip(
///     r#"CREATE ASSERTION ?a { SET FIELDS { asserted_by: :me, mode: "observed" } }"#,
/// )
/// .unwrap();
/// let findings = analyze(&command);
/// // An observation that cites nothing is legal, but it is the shape a
/// // missing citation takes.
/// assert!(findings.iter().any(|d| d.severity == Severity::Warning));
/// ```
pub fn analyze(command: &Command) -> Vec<Diagnostic> {
    let mut out = Vec::new();
    match command {
        Command::Kql(query) => analyze_kql(query, &mut out),
        Command::Kml(statement) => analyze_kml(statement, &mut out),
        Command::Meta(meta) => analyze_meta(meta, &mut out),
    }
    out
}

/// [`check`] for a query that was parsed on its own.
pub(crate) fn check_kql(query: &KqlQuery) -> Result<(), KipError> {
    let mut out = Vec::new();
    analyze_kql(query, &mut out);
    first_error(out)
}

/// [`check`] for a mutation that was parsed on its own.
pub(crate) fn check_kml(statement: &KmlStatement) -> Result<(), KipError> {
    let mut out = Vec::new();
    analyze_kml(statement, &mut out);
    first_error(out)
}

/// [`check`] for a META command that was parsed on its own.
pub(crate) fn check_meta(meta: &MetaCommand) -> Result<(), KipError> {
    let mut out = Vec::new();
    analyze_meta(meta, &mut out);
    first_error(out)
}

fn first_error(diagnostics: Vec<Diagnostic>) -> Result<(), KipError> {
    match diagnostics
        .into_iter()
        .find(|d| d.severity == Severity::Error)
    {
        Some(diagnostic) => Err(diagnostic.into()),
        None => Ok(()),
    }
}

// ---------------------------------------------------------------------------
// Value inspection
// ---------------------------------------------------------------------------

/// The written string in a value position, or `None` when there is nothing to
/// check: a `:parameter` is bound at execution time, and a non-string value is
/// a type error the Schema layer reports.
fn literal_str(value: &MutationValue) -> Option<&str> {
    match value {
        MutationValue::Value(KipValue::String(text)) => Some(text),
        _ => None,
    }
}

fn literal_f64(value: &MutationValue) -> Option<f64> {
    match value {
        MutationValue::Value(KipValue::Number(number)) => number.as_f64(),
        _ => None,
    }
}

fn scalar_str(scalar: &Scalar) -> Option<&str> {
    match scalar {
        Scalar::Literal(KipValue::String(text)) => Some(text),
        _ => None,
    }
}

fn bound_str(value: &BoundValue) -> Option<&str> {
    match value {
        BoundValue::Value(KipValue::String(text)) => Some(text),
        _ => None,
    }
}

fn check_enum(written: Option<&str>, allowed: &[&str], label: &str, out: &mut Vec<Diagnostic>) {
    let Some(written) = written else { return };
    if allowed.contains(&written) {
        return;
    }
    out.push(Diagnostic::error(
        KipErrorCode::ConstraintViolation,
        format!(
            "{label} must be one of {}, found {written:?}",
            allowed.join(" | ")
        ),
    ));
}

fn check_unit_interval(value: Option<f64>, label: &str, out: &mut Vec<Diagnostic>) {
    let Some(value) = value else { return };
    if (0.0..=1.0).contains(&value) {
        return;
    }
    out.push(Diagnostic::error(
        KipErrorCode::ConstraintViolation,
        format!("{label} must be within [0, 1], found {value}"),
    ));
}

// ---------------------------------------------------------------------------
// KML
// ---------------------------------------------------------------------------

fn analyze_kml(statement: &KmlStatement, out: &mut Vec<Diagnostic>) {
    for clause in &statement.clauses {
        analyze_clause(clause, out);
    }
}

fn analyze_clause(clause: &MutationClause, out: &mut Vec<Diagnostic>) {
    match clause {
        // `ASSERT` has already been desugared into this by the parser, so
        // checking the created Assertion covers the sugar form too.
        MutationClause::CreateAssertion(record) => {
            if let Some(fields) = &record.set_fields {
                analyze_assignments(fields, out);
                analyze_assertion_shape(fields, record.set_structural.as_deref(), out);
            }
            analyze_structural(record.set_structural.as_deref(), out);
        }
        // Attributes and Facets are package-defined; their member names do
        // not acquire Assertion semantics by coinciding with Core field names.
        MutationClause::CreateEvidence(_)
        | MutationClause::CreateActivity(_)
        | MutationClause::CreateConcept(_)
        | MutationClause::UpsertConcept(_) => {}
        MutationClause::Update(update) => {
            for action in &update.actions {
                if let UpdateAction::SetFields(a) = action {
                    analyze_assignments(a, out);
                }
            }
            warn_unbounded(
                "UPDATE",
                update.where_clauses.is_some(),
                update.limit.is_some(),
                out,
            );
        }
        // §52.5 fixes the state vocabulary the statement may name; which
        // states fit which target kind, and which current state a move is
        // legal from, is the engine's check (`InvalidLifecycleTransition`).
        MutationClause::Transition(transition) => {
            check_enum(
                transition.state(),
                TRANSITION_STATES,
                "TRANSITION ... TO",
                out,
            );
            warn_unbounded(
                "TRANSITION",
                transition.where_clauses.is_some(),
                transition.limit.is_some(),
                out,
            );
        }
        MutationClause::SetRetention(retention) => {
            warn_unbounded(
                "SET RETENTION",
                retention.where_clauses.is_some(),
                retention.limit.is_some(),
                out,
            );
        }
        MutationClause::Purge(purge) => warn_unbounded(
            "PURGE",
            purge.where_clauses.is_some(),
            purge.limit.is_some(),
            out,
        ),
        MutationClause::PurgePayload(purge) => warn_unbounded(
            "PURGE PAYLOAD",
            purge.where_clauses.is_some(),
            purge.limit.is_some(),
            out,
        ),
        // DEFINE's draft rules (§20.16) depend on the Schema Environment the
        // symbol joins, so the engine owns them.
        MutationClause::EnsureProposition(_)
        | MutationClause::MergeConcept(_)
        | MutationClause::Define(_) => {}
    }
}

/// Check Core fields only, never members of attributes or Facets.
fn analyze_assignments(assignments: &crate::ast::Assignments, out: &mut Vec<Diagnostic>) {
    for (field, value) in assignments {
        match field.as_str() {
            "stance" => check_enum(literal_str(value), STANCES, "stance", out),
            "mode" => check_enum(literal_str(value), ASSERTION_MODES, "mode", out),
            "confidence" => check_unit_interval(literal_f64(value), "confidence", out),
            _ => {}
        }
    }
}

/// `role` on an `("evidence", ...)` citation comes from the Core registry
/// (§56.2). Options on any other structural field are package-defined, and
/// only the Schema Environment can judge those.
fn analyze_structural(edges: Option<&[StructuralEdge]>, out: &mut Vec<Diagnostic>) {
    for edge in edges.into_iter().flatten() {
        let crate::ast::SymbolRef::Name(field) = &edge.field else {
            continue;
        };
        if field != "evidence" {
            continue;
        }
        let Some(options) = &edge.options else {
            continue;
        };
        if let Some(role) = options.get("role") {
            check_enum(
                bound_str(role),
                EVIDENCE_ROLES,
                "an Evidence citation role",
                out,
            );
        }
    }
}

/// An observation that cites nothing is a valid Assertion, but it is the shape
/// a forgotten citation takes: `mode: "observed"` claims the actor saw it, and
/// what they saw is exactly what Evidence records.
fn analyze_assertion_shape(
    fields: &crate::ast::Assignments,
    structural: Option<&[StructuralEdge]>,
    out: &mut Vec<Diagnostic>,
) {
    let observed = fields
        .iter()
        .any(|(name, value)| name == "mode" && literal_str(value) == Some("observed"));
    if !observed {
        return;
    }
    let cites_evidence = structural.into_iter().flatten().any(
        |edge| matches!(&edge.field, crate::ast::SymbolRef::Name(field) if field == "evidence"),
    );
    if !cites_evidence {
        out.push(Diagnostic::warning(
            KipErrorCode::ConstraintViolation,
            "mode: \"observed\" without evidence: an observation normally cites the artifact it \
             was observed from",
        ));
    }
}

/// Spec §52.7 names the statements whose `WHERE` can select an unbounded set.
/// A statement that names its target directly is already bounded to one
/// element, so only the pattern-selecting form is worth warning about.
fn warn_unbounded(statement: &str, has_where: bool, has_limit: bool, out: &mut Vec<Diagnostic>) {
    if has_where && !has_limit {
        out.push(Diagnostic::warning(
            KipErrorCode::ResultLimitExceeded,
            format!(
                "{statement} selects by pattern without a LIMIT: the match set is unbounded, and \
                 an over-broad one cannot be undone"
            ),
        ));
    }
}

// ---------------------------------------------------------------------------
// KQL
// ---------------------------------------------------------------------------

fn analyze_kql(query: &KqlQuery, out: &mut Vec<Diagnostic>) {
    if let Some(epistemic) = &query.epistemic
        && let Some(explanation) = epistemic.get("explanation")
    {
        check_enum(
            bound_str(explanation),
            EXPLANATION_LEVELS,
            "WITH EPISTEMIC explanation",
            out,
        );
    }
    if query.limit.is_none() {
        out.push(Diagnostic::warning(
            KipErrorCode::ResultLimitExceeded,
            "FIND without a LIMIT: an unbounded recall returns whatever the Space happens to hold",
        ));
    }
}

// ---------------------------------------------------------------------------
// META
// ---------------------------------------------------------------------------

fn analyze_meta(meta: &MetaCommand, out: &mut Vec<Diagnostic>) {
    match meta {
        // The engine checks literal and parameter thresholds together after
        // binding against the normalized retrieval-score range in §66.4.
        MetaCommand::Search(search) => check_enum(
            search.mode.as_ref().and_then(scalar_str),
            SEARCH_MODES,
            "SEARCH MODE",
            out,
        ),
        MetaCommand::Describe(crate::ast::DescribeTarget::Primer { mode }) => check_enum(
            mode.as_ref().and_then(scalar_str),
            PRIMER_MODES,
            "DESCRIBE PRIMER MODE",
            out,
        ),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{parse_kip, parse_kml};

    #[test]
    fn core_registry_values_are_checked_wherever_they_are_written() {
        for input in [
            r#"ASSERT (:a, "p", :b) { by: :me, mode: "guessed" }"#,
            r#"ASSERT (:a, "p", :b) { by: :me, mode: "stated", stance: "maybe" }"#,
            r#"CREATE ASSERTION ?a { SET FIELDS { stance: "nope" } }"#,
            r#"UPDATE :c SET FIELDS { mode: "wat" }"#,
            r#"CREATE ASSERTION ?a { SET STRUCTURAL { ("evidence", :e) { role: "bogus" } } }"#,
            r#"RETRACT ASSERTION :a EXPECT STATE "banana""#,
            r#"SUPERSEDE ASSERTION :a BY :b EXPECT STATE "banana""#,
            r#"SEARCH CONCEPT "x" MODE "fuzzy""#,
            r#"DESCRIBE PRIMER MODE "verbose""#,
        ] {
            assert!(
                parse_kip(input).is_err(),
                "a Core registry violation must not parse: {input}"
            );
        }
    }

    #[test]
    fn the_unit_interval_is_enforced_where_the_protocol_fixes_it() {
        for input in [
            r#"ASSERT (:a, "p", :b) { by: :me, mode: "stated", confidence: 5 }"#,
            r#"ASSERT (:a, "p", :b) { by: :me, mode: "stated", confidence: -0.5 }"#,
            r#"CREATE ASSERTION ?a { SET FIELDS { confidence: 1.5 } }"#,
        ] {
            assert!(parse_kip(input).is_err(), "out of [0,1]: {input}");
        }
        for input in [
            r#"ASSERT (:a, "p", :b) { by: :me, mode: "stated", confidence: 0 }"#,
            r#"ASSERT (:a, "p", :b) { by: :me, mode: "stated", confidence: 1 }"#,
            r#"ASSERT (:a, "p", :b) { by: :me, mode: "stated", confidence: 0.5 }"#,
        ] {
            assert!(parse_kip(input).is_ok(), "inside [0,1]: {input}");
        }
    }

    #[test]
    fn search_threshold_execution_validation_belongs_to_the_engine() {
        // The syntax layer preserves numeric operands; engine tests reject
        // out-of-range values under §66.4 after all operands are bound.
        for input in [
            r#"SEARCH CONCEPT "x" THRESHOLD 0.5"#,
            r#"SEARCH CONCEPT "x" THRESHOLD 1000"#,
            r#"SEARCH CONCEPT "x" THRESHOLD -3"#,
        ] {
            assert!(parse_kip(input).is_ok(), "{input}");
        }
    }

    #[test]
    fn a_parameter_is_never_second_guessed() {
        // Its value arrives with the envelope; rejecting it here would reject
        // a command that is going to be perfectly legal.
        for input in [
            r#"ASSERT (:a, "p", :b) { by: :me, mode: :mode, stance: :stance, confidence: :c }"#,
            r#"SEARCH CONCEPT "x" MODE :mode THRESHOLD :threshold"#,
            r#"DESCRIBE PRIMER MODE :mode"#,
            r#"TRANSITION :a TO :state"#,
        ] {
            assert!(parse_kip(input).is_ok(), "a parameter must pass: {input}");
        }
    }

    #[test]
    fn package_defined_signals_are_left_to_the_engine() {
        // The Cognitive Memory Profile fixes these to [0,1], but a Space
        // running a different Profile may mean something else by them. Only
        // the active Schema Environment can decide, so the protocol layer
        // does not.
        assert!(parse_kip(r#"UPDATE :c SET FACET "MnemonicState" { salience: 42 }"#).is_ok());
        assert!(parse_kip(r#"UPDATE :c SET FACET "Skill" { utility: 42 }"#).is_ok());
    }

    #[test]
    fn a_transition_names_a_state_from_the_registry() {
        // §52.5 fixes the vocabulary; which state fits which kind, and which
        // current state the move is legal from, is the engine's check.
        for state in TRANSITION_STATES {
            let by = if crate::ast::transition_state::WITH_BY.contains(state) {
                " BY :b"
            } else {
                ""
            };
            assert!(
                parse_kip(&format!(r#"TRANSITION :a TO "{state}"{by}"#)).is_ok(),
                "{state}"
            );
        }
        let err = parse_kip(r#"TRANSITION :a TO "succeeded""#).expect_err("not a state");
        assert_eq!(err.code, KipErrorCode::ConstraintViolation);
        assert!(parse_kip(r#"TRANSITION :a TO "active""#).is_err());
        assert!(parse_kip(r#"TRANSITION :a TO "quarantined""#).is_err());
    }

    #[test]
    fn unbounded_pattern_mutations_warn_but_still_parse() {
        let statement = parse_kml(r#"PURGE ?x WHERE { ?x {type: "T"} } CONFIRM "PURGE""#)
            .expect("a legal command");
        let mut diagnostics = Vec::new();
        analyze_kml(&statement, &mut diagnostics);
        assert!(
            diagnostics
                .iter()
                .any(|d| d.severity == Severity::Warning && d.message.contains("LIMIT")),
            "an unbounded PURGE must warn: {diagnostics:?}"
        );

        // Naming the target directly is already bounded to one element.
        let bounded = parse_kml(r#"PURGE :x CONFIRM "PURGE""#).expect("a legal command");
        let mut none = Vec::new();
        analyze_kml(&bounded, &mut none);
        assert!(none.is_empty(), "a targeted PURGE must not warn: {none:?}");

        // §60.5 names PURGE PAYLOAD alongside PURGE: byte destruction over an
        // unbounded WHERE is exactly the sweep that must not run by accident.
        let payload =
            parse_kml(r#"PURGE PAYLOAD ?x WHERE { ?x EVIDENCE {evidence_class: "document"} } CONFIRM "PURGE""#)
                .expect("a legal command");
        let mut diagnostics = Vec::new();
        analyze_kml(&payload, &mut diagnostics);
        assert!(
            diagnostics
                .iter()
                .any(|d| d.severity == Severity::Warning && d.message.contains("LIMIT")),
            "an unbounded PURGE PAYLOAD must warn: {diagnostics:?}"
        );
    }

    #[test]
    fn an_observation_without_evidence_is_a_warning_not_a_rejection() {
        let command =
            parse_kip(r#"ASSERT (:a, "p", :b) { by: :me, mode: "observed" }"#).expect("legal");
        let diagnostics = analyze(&command);
        assert!(diagnostics.iter().all(|d| d.severity == Severity::Warning));
        assert!(diagnostics.iter().any(|d| d.message.contains("observed")));

        // Citing one clears it.
        let cited =
            parse_kip(r#"ASSERT (:a, "p", :b) { by: :me, mode: "observed", evidence: :e }"#)
                .expect("legal");
        assert!(analyze(&cited).is_empty());
    }

    #[test]
    fn the_explanation_level_comes_from_the_registry() {
        assert!(
            parse_kip(r#"FIND(?x) WHERE { ?x {a: 1} } WITH EPISTEMIC { explanation: "verbose" }"#)
                .is_err()
        );
        assert!(
            parse_kip(
                r#"FIND(?x) WHERE { ?x {a: 1} } WITH EPISTEMIC { explanation: "ledger" } LIMIT 5"#
            )
            .is_ok()
        );
    }

    #[test]
    fn diagnostics_carry_the_registry_code_they_would_be_reported_under() {
        let err = parse_kip(r#"ASSERT (:a, "p", :b) { by: :me, mode: "guessed" }"#)
            .expect_err("rejected");
        assert_eq!(err.code, KipErrorCode::ConstraintViolation);
        assert!(err.message.contains("observed | stated"), "{}", err.message);
    }
}
