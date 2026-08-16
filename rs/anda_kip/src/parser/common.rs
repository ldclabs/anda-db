//! Lexical layer and the rules the three KIP 2.0 grammars share.
//!
//! Two rules deliberately diverge between the surfaces, and both divergences
//! are owned by KQL (see the EBNF preambles):
//!
//! - `proposition_tuple` admits raw predicate *paths* in KQL, but only an exact
//!   `predicate_atom` in KML and META;
//! - `where_clause` admits `BELIEF` / `BELIEF SLOT` in KQL only — a virtual
//!   Projection can never be a mutation target or an export selector.
//!
//! Both are threaded through as [`Flavor`] rather than duplicated, so the rest
//! of the shared grammar has exactly one definition.

use nom::{
    Parser,
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{anychar, char, multispace1},
    combinator::{cut, map, not, opt, peek, value, verify},
    error::context,
    multi::{many0, separated_list0, separated_list1},
    sequence::{delimited, preceded, separated_pair, terminated},
};
use nom_language::error::{VerboseError, VerboseErrorKind};
use std::collections::BTreeSet;

use super::MAX_KIP_NESTING_DEPTH;
use super::json::{identifier as json_identifier, parse_number, skip_ws_and_comments};
use crate::ast::{
    Assignments, BoundObject, BoundValue, DotPathVar, ElementRef, FilterExpression, FilterFunction,
    FilterOperand, HopRange, KipValue, MatchValue, MutationValue, Number, ObjectMatcher, PathStep,
    PredAtom, PredPathAtom, PredTerm, PropositionMatcher, PropositionTriple, Scalar, SymbolRef,
    Term, UpdateExpr, UpdateFunction, WhereClause,
};

pub use super::json::{quoted_string, ws};

/// The parser's result type over `&str` with source-anchored errors.
pub type VResult<'a, T> = nom::IResult<&'a str, T, VerboseError<&'a str>>;

/// Which surface a shared rule is being parsed for.
///
/// The two reviewed divergences travel together: only KQL walks raw predicate
/// paths, and only KQL projects belief.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Flavor {
    /// KQL — raw predicate paths and BELIEF patterns are admitted.
    Kql,
    /// KML and META — exact predicates only, no BELIEF.
    Exact,
}

impl Flavor {
    fn raw_paths(self) -> bool {
        matches!(self, Flavor::Kql)
    }
}

/// Engine-owned state no cognitive mutation may write (Spec §6.3, §2.11).
///
/// Checked on every assignment, not only on `UPDATE`: author content that could
/// rewrite engine truth or its own authority is exactly what "external
/// cognition cannot self-escalate authority" forbids.
pub const PROTECTED_FIELDS: &[&str] = &["_system", "governance", "space_id", "space_seq"];

/// True when a mutation may not write this field name.
pub fn is_protected_field(name: &str) -> bool {
    PROTECTED_FIELDS.contains(&name)
}

// ---------------------------------------------------------------------------
// Failure helpers
// ---------------------------------------------------------------------------

/// Aborts the parse at `input` with a static explanation.
///
/// `Failure` rather than `Error`: these are rule violations, not a wrong branch
/// of an `alt`, so backtracking into a worse-fitting rule would only bury the
/// real diagnosis.
pub fn fail<'a, T>(input: &'a str, ctx: &'static str) -> VResult<'a, T> {
    Err(nom::Err::Failure(VerboseError {
        errors: vec![(input, VerboseErrorKind::Context(ctx))],
    }))
}

// ---------------------------------------------------------------------------
// Keywords
// ---------------------------------------------------------------------------

/// Asserts that the input is at a keyword boundary, consuming nothing.
///
/// A keyword ends where the next token begins, so it must not be glued to an
/// identifier character (`FIND` must not match the prefix of `FINDX`), to a
/// variable (`INTO?b`), or to a quoted string (`WITH TYPE"Drug"`) — each of
/// those is two tokens in the grammar.
pub fn word_boundary<'a>() -> impl Parser<&'a str, Output = (), Error = VerboseError<&'a str>> {
    not(verify(anychar, |c: &char| {
        c.is_alphanumeric() || matches!(c, '_' | '?' | '"')
    }))
}

/// Matches one protocol keyword, ASCII case-insensitively, at a word boundary.
///
/// KIP 2.0 keywords are case-insensitive with uppercase as the canonical
/// rendering, and they are *contextual*: `by`, `mode`, `key`, `name`, `type` and
/// `status` all appear as ordinary field names in the Spec's own examples. That
/// falls out of scannerless parsing — a keyword is only a keyword where a rule
/// asks for one.
pub fn word<'a>(
    w: &'static str,
) -> impl Parser<&'a str, Output = &'a str, Error = VerboseError<&'a str>> {
    terminated(tag_no_case(w), word_boundary())
}

/// Consumes at least one whitespace character or a line comment, then any
/// further trivia.
fn trivia1(input: &str) -> VResult<'_, ()> {
    let (rest, _) = alt((map(multispace1, |_| ()), map(peek(tag("//")), |_| ()))).parse(input)?;
    skip_ws_and_comments(rest)
}

/// Matches a sequence of keywords separated by mandatory whitespace or comments,
/// e.g. `ORDER BY`, `LIST STRUCTURAL FIELDS`.
pub fn words<'a>(
    kws: &'static [&'static str],
) -> impl Parser<&'a str, Output = (), Error = VerboseError<&'a str>> {
    move |mut input: &'a str| {
        for (idx, w) in kws.iter().enumerate() {
            if idx > 0 {
                // Between two keywords a separator is mandatory, but a comment
                // counts as one: `DESCRIBE // why\n PRIMER` is one command.
                let (rest, _) = trivia1(input)?;
                input = rest;
            }
            let (rest, _) = tag_no_case(*w).parse(input)?;
            input = rest;
        }
        let (input, _) = word_boundary().parse(input)?;
        Ok((input, ()))
    }
}

/// Runs `f` only when the keyword sequence is present, consuming neither on miss.
pub fn opt_after<'a, O, F>(
    kws: &'static [&'static str],
    f: F,
) -> impl Parser<&'a str, Output = Option<O>, Error = VerboseError<&'a str>>
where
    F: Parser<&'a str, Output = O, Error = VerboseError<&'a str>>,
{
    opt(preceded(ws(words(kws)), cut(f)))
}

// ---------------------------------------------------------------------------
// Blocks
// ---------------------------------------------------------------------------

/// Parses `{ f }`.
pub fn braced<'a, O, F>(f: F) -> impl Parser<&'a str, Output = O, Error = VerboseError<&'a str>>
where
    F: Parser<&'a str, Output = O, Error = VerboseError<&'a str>>,
{
    delimited(ws(char('{')), f, ws(char('}')))
}

/// Parses `( f )`.
pub fn parenthesized<'a, O, F>(
    f: F,
) -> impl Parser<&'a str, Output = O, Error = VerboseError<&'a str>>
where
    F: Parser<&'a str, Output = O, Error = VerboseError<&'a str>>,
{
    delimited(ws(char('(')), f, ws(char(')')))
}

// ---------------------------------------------------------------------------
// Lexical atoms
// ---------------------------------------------------------------------------

/// `identifier = identifier_start, { identifier_continue }`
pub fn identifier(input: &str) -> VResult<'_, &str> {
    context(
        "an identifier (a letter or underscore, then letters, digits, underscores)",
        json_identifier(),
    )
    .parse(input)
}

/// `variable = "?", identifier` — returns the bare name, without the sigil.
pub fn variable(input: &str) -> VResult<'_, String> {
    context(
        "a variable such as ?name",
        map(preceded(char('?'), cut(identifier)), |s| s.to_string()),
    )
    .parse(input)
}

/// `parameter = ":", identifier` — returns the bare name, without the sigil.
pub fn parameter(input: &str) -> VResult<'_, String> {
    context(
        "a parameter such as :name",
        map(preceded(char(':'), cut(identifier)), |s| s.to_string()),
    )
    .parse(input)
}

/// `field_name = identifier | string_literal`
pub fn field_name(input: &str) -> VResult<'_, String> {
    alt((quoted_string, map(identifier, |s| s.to_string()))).parse(input)
}

/// `literal = string | number | boolean | null`
///
/// `true` / `false` / `null` are JSON literals, not protocol keywords, so unlike
/// keywords they stay case-sensitive.
pub fn literal(input: &str) -> VResult<'_, KipValue> {
    context(
        "a literal: a string, a number, true, false or null",
        alt((
            value(KipValue::Null, terminated(tag("null"), word_boundary())),
            value(
                KipValue::Bool(true),
                terminated(tag("true"), word_boundary()),
            ),
            value(
                KipValue::Bool(false),
                terminated(tag("false"), word_boundary()),
            ),
            map(quoted_string, KipValue::String),
            map(parse_number, KipValue::Number),
        )),
    )
    .parse(input)
}

/// `scalar_value` / `scalar_or_parameter` / `meta_value` = `parameter | literal`
pub fn scalar(input: &str) -> VResult<'_, Scalar> {
    context(
        "a literal or a :parameter",
        alt((map(parameter, Scalar::Param), map(literal, Scalar::Literal))),
    )
    .parse(input)
}

/// `schema_symbol = string_literal | parameter`
pub fn symbol_ref(input: &str) -> VResult<'_, SymbolRef> {
    context(
        "a quoted schema symbol or a :parameter",
        alt((
            map(quoted_string, SymbolRef::Name),
            map(parameter, SymbolRef::Param),
        )),
    )
    .parse(input)
}

/// `target_ref = variable | parameter | string_literal`
pub fn element_ref(input: &str) -> VResult<'_, ElementRef> {
    context(
        "a ?handle, a :parameter or a quoted element id",
        alt((
            map(variable, ElementRef::Handle),
            map(parameter, ElementRef::Param),
            map(quoted_string, ElementRef::Id),
        )),
    )
    .parse(input)
}

/// `handle = variable`
pub fn handle(input: &str) -> VResult<'_, String> {
    variable(input)
}

/// `field_access = variable, { field_step }`
pub fn dot_path_var(input: &str) -> VResult<'_, DotPathVar> {
    let (input, var) = variable(input)?;
    let (input, path) = many0(alt((
        map(preceded(char('.'), cut(identifier)), |s| {
            PathStep::Field(s.to_string())
        }),
        map(
            delimited(char('['), cut(ws(quoted_string)), cut(char(']'))),
            PathStep::Key,
        ),
    )))
    .parse(input)?;

    // `?var.` is a truncated path, not a variable followed by a stray dot.
    if input.starts_with('.') {
        return fail(input, "a field name after `.`");
    }
    Ok((input, DotPathVar { var, path }))
}

// ---------------------------------------------------------------------------
// Predicates
// ---------------------------------------------------------------------------

/// `predicate_atom = string_literal | parameter | variable`
pub fn pred_atom(input: &str) -> VResult<'_, PredAtom> {
    context(
        "a quoted predicate, a :parameter or a ?variable",
        alt((
            map(quoted_string, PredAtom::Literal),
            map(parameter, PredAtom::Param),
            map(variable, PredAtom::Variable),
        )),
    )
    .parse(input)
}

/// `path_quantifier = "{" unsigned [ "," [ unsigned ] ] "}"`
fn path_quantifier(input: &str) -> VResult<'_, HopRange> {
    let (input, _) = char('{').parse(input)?;
    let (input, min) = cut(ws(unsigned_integer)).parse(input)?;
    let (input, comma) = opt(ws(char(','))).parse(input)?;
    let (input, max) = if comma.is_some() {
        let (input, max) = opt(ws(unsigned_integer)).parse(input)?;
        (input, max)
    } else {
        (input, Some(min))
    };
    let (input, _) = cut(ws(char('}'))).parse(input)?;

    if let Some(max) = max
        && max < min
    {
        return fail(
            input,
            "a hop quantifier whose maximum is at least its minimum",
        );
    }
    Ok((input, HopRange { min, max }))
}

fn unsigned_integer(input: &str) -> VResult<'_, u32> {
    let (rest, number) = parse_number(input)?;
    match number.as_u64() {
        Some(n) if n <= u32::MAX as u64 => Ok((rest, n as u32)),
        _ => fail(input, "a non-negative whole number of hops"),
    }
}

/// `raw_predicate_expression = predicate_path_atom { "|" predicate_path_atom }`
///
/// Collapses to [`PredTerm::Atom`] when a single unquantified atom was written,
/// which is the only shape KML and META accept.
fn raw_predicate(input: &str) -> VResult<'_, PredTerm> {
    let (input, atoms) = separated_list1(
        ws(char('|')),
        (pred_atom, opt(path_quantifier)).map(|(predicate, hops)| PredPathAtom { predicate, hops }),
    )
    .parse(input)?;

    if atoms.len() == 1 && atoms[0].hops.is_none() {
        let only = atoms.into_iter().next().expect("one atom");
        return Ok((input, PredTerm::Atom(only.predicate)));
    }
    Ok((input, PredTerm::Path(atoms)))
}

fn predicate_term(input: &str, flavor: Flavor) -> VResult<'_, PredTerm> {
    if flavor.raw_paths() {
        raw_predicate(input)
    } else {
        map(pred_atom, PredTerm::Atom).parse(input)
    }
}

// ---------------------------------------------------------------------------
// Proposition expressions, terms and matchers
// ---------------------------------------------------------------------------

/// The `id:` head of the identity spelling of a Proposition expression.
///
/// `id` is spelled lowercase: it is an ordinary identifier in the grammar, not a
/// protocol keyword, so unlike keywords it is case-sensitive.
fn id_key(input: &str) -> VResult<'_, ()> {
    let (rest, _) = ws(terminated(tag("id"), word_boundary())).parse(input)?;
    let (rest, _) = ws(char(':')).parse(rest)?;
    Ok((rest, ()))
}

/// `proposition_tuple = "(" term "," predicate "," term ")" | "(" "id" ":" scalar ")"`
pub fn proposition_matcher(input: &str, flavor: Flavor) -> VResult<'_, PropositionMatcher> {
    let (input, _) = ws(char('(')).parse(input)?;

    // `(id: ...)` and `(subject, ...)` are told apart by the literal `id:`.
    if let Ok((rest, _)) = id_key(input) {
        let (rest, id) = cut(ws(scalar)).parse(rest)?;
        let (rest, _) = cut(ws(char(')'))).parse(rest)?;
        return Ok((rest, PropositionMatcher::Id(id)));
    }

    let (input, subject) = cut(ws(|i| proposition_subject(i, flavor))).parse(input)?;
    let (input, _) = cut(ws(char(','))).parse(input)?;
    let (input, predicate) = cut(ws(|i| predicate_term(i, flavor))).parse(input)?;
    let (input, _) = cut(ws(char(','))).parse(input)?;
    let (input, object) = cut(ws(|i| term(i, flavor))).parse(input)?;
    let (input, _) = cut(ws(char(')'))).parse(input)?;

    Ok((
        input,
        PropositionMatcher::Tuple(PropositionTriple {
            subject,
            predicate,
            object,
        }),
    ))
}

/// `term = variable | parameter | literal | object_pattern | proposition_tuple`
pub fn term(input: &str, flavor: Flavor) -> VResult<'_, Term> {
    alt((
        map(variable, Term::Variable),
        map(parameter, Term::Param),
        map(literal, Term::Literal),
        map(|i| object_matcher(i, flavor), Term::Match),
        map(
            |i| proposition_matcher(i, flavor),
            |matcher| Term::Proposition(Box::new(matcher)),
        ),
    ))
    .parse(input)
}

/// A Proposition subject is always an Element reference, never a Literal.
pub fn proposition_subject(input: &str, flavor: Flavor) -> VResult<'_, Term> {
    let (rest, subject) = term(input, flavor)?;
    if matches!(subject, Term::Literal(_)) {
        return fail(
            input,
            "a Proposition subject that is a local Element reference, never a Literal",
        );
    }
    Ok((rest, subject))
}

/// `object_pattern = "{" [ pattern_member { "," pattern_member } ] "}"`
pub fn object_matcher(input: &str, flavor: Flavor) -> VResult<'_, ObjectMatcher> {
    let (rest, entries) = delimited(
        ws(char('{')),
        terminated(
            separated_list0(
                ws(char(',')),
                separated_pair(
                    ws(spanned(field_name)),
                    cut(ws(char(':'))),
                    cut(ws(|i| match_value(i, flavor))),
                ),
            ),
            opt(ws(char(','))),
        ),
        ws(char('}')),
    )
    .parse(input)?;

    let mut matcher = ObjectMatcher::new();
    for ((position, key), value) in entries {
        if matcher.insert(key, value).is_some() {
            return fail(
                position,
                "a match field that is not already set in this block",
            );
        }
    }
    Ok((rest, matcher))
}

/// `pattern_value = variable | parameter | literal | array_pattern | object_pattern | proposition_tuple`
fn match_value(input: &str, flavor: Flavor) -> VResult<'_, MatchValue> {
    alt((
        map(variable, MatchValue::Variable),
        map(parameter, MatchValue::Param),
        map(literal, MatchValue::Literal),
        map(
            delimited(
                ws(char('[')),
                terminated(
                    separated_list0(ws(char(',')), ws(|i| match_value(i, flavor))),
                    opt(ws(char(','))),
                ),
                ws(char(']')),
            ),
            MatchValue::Array,
        ),
        map(|i| object_matcher(i, flavor), MatchValue::Match),
        map(|i| proposition_matcher(i, flavor), MatchValue::Proposition),
    ))
    .parse(input)
}

// ---------------------------------------------------------------------------
// Data values, assignments and update expressions
// ---------------------------------------------------------------------------

/// `data_value` — anything that may still carry unbound parameters.
///
/// A bare `?x` is a **handle** here: in a value position a variable names an
/// element the plan created. `?x.field` is a read of that element's own field,
/// which is a different thing and keeps its path.
pub fn bound_value(input: &str) -> VResult<'_, BoundValue> {
    alt((
        map(parameter, BoundValue::Param),
        map(dot_path_var, |path| {
            if path.path.is_empty() {
                BoundValue::Handle(path.var)
            } else {
                BoundValue::Variable(path)
            }
        }),
        map(literal, BoundValue::Value),
        bound_array,
        bound_object_value,
    ))
    .parse(input)
}

fn bound_array(input: &str) -> VResult<'_, BoundValue> {
    let (rest, items) = delimited(
        ws(char('[')),
        terminated(
            separated_list0(ws(char(',')), ws(bound_value)),
            opt(ws(char(','))),
        ),
        ws(char(']')),
    )
    .parse(input)?;
    Ok((rest, collapse_array(items)))
}

fn bound_object_value(input: &str) -> VResult<'_, BoundValue> {
    let (rest, entries) = bound_entries(input)?;
    Ok((rest, collapse_object(entries)))
}

fn bound_entries(input: &str) -> VResult<'_, Vec<(String, BoundValue)>> {
    let (rest, entries) = delimited(
        ws(char('{')),
        terminated(
            separated_list0(
                ws(char(',')),
                separated_pair(
                    ws(spanned(field_name)),
                    cut(ws(char(':'))),
                    cut(ws(bound_value)),
                ),
            ),
            opt(ws(char(','))),
        ),
        ws(char('}')),
    )
    .parse(input)?;

    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut out = Vec::with_capacity(entries.len());
    for ((position, key), value) in entries {
        // Spec §70.2: duplicate object keys are rejected rather than
        // last-write-wins, which would silently swallow a generation slip.
        if !seen.insert(key.clone()) {
            return fail(position, "a key that is not already set in this object");
        }
        out.push((key, value));
    }
    Ok((rest, out))
}

/// `object_literal` in a position that wants a keyed block, e.g. `WITH {...}`.
pub fn bound_object(input: &str) -> VResult<'_, BoundObject> {
    let (rest, entries) = bound_entries(input)?;
    Ok((rest, entries.into_iter().collect()))
}

/// Collapses a fully literal array so an engine with nothing to bind never
/// walks a binding tree.
fn collapse_array(items: Vec<BoundValue>) -> BoundValue {
    if items.iter().all(|v| matches!(v, BoundValue::Value(_))) {
        BoundValue::Value(KipValue::Array(
            items
                .into_iter()
                .map(|v| match v {
                    BoundValue::Value(value) => value,
                    _ => unreachable!("checked above"),
                })
                .collect(),
        ))
    } else {
        BoundValue::Array(items)
    }
}

fn collapse_object(entries: Vec<(String, BoundValue)>) -> BoundValue {
    if entries
        .iter()
        .all(|(_, v)| matches!(v, BoundValue::Value(_)))
    {
        BoundValue::Value(KipValue::Object(
            entries
                .into_iter()
                .map(|(k, v)| match v {
                    BoundValue::Value(value) => (k, value),
                    _ => unreachable!("checked above"),
                })
                .collect(),
        ))
    } else {
        BoundValue::Object(entries)
    }
}

/// `mutation_value` — a `data_value`, or a deterministic update expression.
pub fn mutation_value(input: &str) -> VResult<'_, MutationValue> {
    alt((
        map(update_function_call, MutationValue::Expr),
        map(bound_value, MutationValue::from),
    ))
    .parse(input)
}

/// A call to one of the registered update functions (Spec §59).
fn update_function_call(input: &str) -> VResult<'_, UpdateExpr> {
    let (rest, name) = terminated(identifier, peek(ws(char('(')))).parse(input)?;
    let func = match name.to_ascii_uppercase().as_str() {
        "ADD" => UpdateFunction::Add,
        "MUL" => UpdateFunction::Mul,
        "CLAMP" => UpdateFunction::Clamp,
        "COALESCE" => UpdateFunction::Coalesce,
        _ => {
            return fail(
                input,
                "a registered update function: ADD, MUL, CLAMP or COALESCE",
            );
        }
    };
    let (rest, args) = cut(parenthesized(terminated(
        separated_list0(ws(char(',')), ws(update_expr)),
        opt(ws(char(','))),
    )))
    .parse(rest)?;

    if args.len() != func.arity() {
        return fail(input, "an update function called with its declared arity");
    }
    Ok((rest, UpdateExpr::Function { func, args }))
}

/// An update expression may read only the element being updated; which element
/// that is depends on the statement, so the check happens once the whole
/// `UPDATE` is known (see `kml::validate`).
pub fn update_expr(input: &str) -> VResult<'_, UpdateExpr> {
    alt((
        update_function_call,
        map(parameter, UpdateExpr::Param),
        map(negative_number, UpdateExpr::Number),
        map(dot_path_var, UpdateExpr::Variable),
    ))
    .parse(input)
}

/// A number, with the unary-minus spelling folded into the literal.
fn negative_number(input: &str) -> VResult<'_, Number> {
    alt((parse_number, preceded(ws(char('-')), cut(negated_number)))).parse(input)
}

fn negated_number(input: &str) -> VResult<'_, Number> {
    let (rest, number) = parse_number(input)?;
    let negated = if let Some(n) = number.as_i64() {
        // `-i64::MIN` is not an `i64`, but it is exactly 2^63, which is a `u64`.
        // Computing it as `-n` panics in a debug build and wraps in a release
        // one, so `--9223372036854775808` used to abort the parser.
        match n.checked_neg() {
            Some(n) => Number::from(n),
            None => Number::from(1u64 << 63),
        }
    } else if let Some(u) = number.as_u64() {
        // An integer above `i64::MAX`; only 2^63 negates to an exact `i64`.
        // Degrading the rest to `f64` would store a different number than the
        // one written, which is what `parse_number` already refuses to do.
        if u == 1u64 << 63 {
            Number::from(i64::MIN)
        } else {
            return fail(input, "an integer whose negation is exactly representable");
        }
    } else if let Some(f) = number.as_f64() {
        match Number::from_f64(-f) {
            Some(n) => n,
            None => return fail(input, "a finite number"),
        }
    } else {
        return fail(input, "a number this build can negate");
    };
    Ok((rest, negated))
}

/// `assignment_object = "{" [ assignment_member { "," assignment_member } ] "}"`
///
/// Rejects duplicate keys and engine-owned field names wherever assignments
/// appear, not only inside `UPDATE`.
pub fn assignments(input: &str) -> VResult<'_, Assignments> {
    let (rest, entries) = delimited(
        ws(char('{')),
        terminated(
            separated_list0(
                ws(char(',')),
                separated_pair(
                    ws(spanned(field_name)),
                    cut(ws(char(':'))),
                    cut(ws(mutation_value)),
                ),
            ),
            opt(ws(char(','))),
        ),
        ws(char('}')),
    )
    .parse(input)?;

    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut out: Assignments = Vec::with_capacity(entries.len());
    for ((position, key), value) in entries {
        if is_protected_field(&key) {
            return fail(
                position,
                "a writable field: _system, governance, space_id and space_seq are engine-maintained",
            );
        }
        if !seen.insert(key.clone()) {
            return fail(
                position,
                "a field that is not already assigned in this block",
            );
        }
        out.push((key, value));
    }
    Ok((rest, out))
}

/// `unset_field_set = "{" [ unset_field { "," unset_field } ] "}"`
pub fn unset_field_set(input: &str) -> VResult<'_, Vec<String>> {
    let (rest, entries) = delimited(
        ws(char('{')),
        terminated(
            separated_list0(ws(char(',')), ws(spanned(field_name))),
            opt(ws(char(','))),
        ),
        ws(char('}')),
    )
    .parse(input)?;

    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut out = Vec::with_capacity(entries.len());
    for (position, key) in entries {
        if is_protected_field(&key) {
            return fail(
                position,
                "a writable field: _system, governance, space_id and space_seq are engine-maintained",
            );
        }
        if !seen.insert(key.clone()) {
            return fail(position, "a field that is not already listed in this block");
        }
        out.push(key);
    }
    Ok((rest, out))
}

// ---------------------------------------------------------------------------
// FILTER expressions
// ---------------------------------------------------------------------------

/// The error a filter that outgrows the nesting budget is rejected with.
const FILTER_TOO_DEEP: &str = "a filter expression within the nesting budget: `!`, unary `-`, `&&` and `||` each nest one \
     level deeper without opening a bracket";

/// `expression` in a `FILTER` position, closed to what a filter can mean.
///
/// Every level below carries an explicit depth, because
/// [`super::MAX_KIP_NESTING_DEPTH`] is enforced by counting brackets and these
/// operators open none: `!!!!…`, `----…` and `a && a && a …` all build an AST
/// exactly as deep as they are long. Without a ceiling the recursive descent —
/// and, for the iterative `&&`/`||` chains, the recursive `Drop` of the tree
/// they build — exhausts the stack, which aborts the process rather than
/// failing the one request.
pub fn filter_expression(input: &str) -> VResult<'_, FilterExpression> {
    filter_or(input, 0)
}

fn filter_or(input: &str, depth: usize) -> VResult<'_, FilterExpression> {
    let (mut rest, mut left) = filter_and(input, depth)?;
    let mut terms = depth;
    loop {
        match ws(tag("||")).parse(rest) {
            Ok((next, _)) => {
                terms += 1;
                if terms > MAX_KIP_NESTING_DEPTH {
                    return fail(next, FILTER_TOO_DEEP);
                }
                let (next, right) = cut(|i| filter_and(i, depth)).parse(next)?;
                left = FilterExpression::Logical {
                    left: Box::new(left),
                    operator: crate::ast::LogicalOperator::Or,
                    right: Box::new(right),
                };
                rest = next;
            }
            Err(_) => return Ok((rest, left)),
        }
    }
}

fn filter_and(input: &str, depth: usize) -> VResult<'_, FilterExpression> {
    let (mut rest, mut left) = filter_unary(input, depth)?;
    let mut terms = depth;
    loop {
        match ws(tag("&&")).parse(rest) {
            Ok((next, _)) => {
                terms += 1;
                if terms > MAX_KIP_NESTING_DEPTH {
                    return fail(next, FILTER_TOO_DEEP);
                }
                let (next, right) = cut(|i| filter_unary(i, depth)).parse(next)?;
                left = FilterExpression::Logical {
                    left: Box::new(left),
                    operator: crate::ast::LogicalOperator::And,
                    right: Box::new(right),
                };
                rest = next;
            }
            Err(_) => return Ok((rest, left)),
        }
    }
}

fn filter_unary(input: &str, depth: usize) -> VResult<'_, FilterExpression> {
    if let Ok((rest, _)) = ws(char::<&str, VerboseError<&str>>('!')).parse(input) {
        // `!=` is one token; `!` here is negation, so refuse to split it.
        if !rest.starts_with('=') {
            if depth >= MAX_KIP_NESTING_DEPTH {
                return fail(input, FILTER_TOO_DEEP);
            }
            let (rest, inner) = cut(|i| filter_unary(i, depth + 1)).parse(rest)?;
            return Ok((rest, FilterExpression::Not(Box::new(inner))));
        }
    }
    filter_primary(input, depth)
}

fn filter_primary(input: &str, depth: usize) -> VResult<'_, FilterExpression> {
    if depth >= MAX_KIP_NESTING_DEPTH {
        return fail(input, FILTER_TOO_DEEP);
    }

    // A parenthesized filter regroups; a parenthesized *operand* is handled by
    // `filter_operand`, so try the grouping reading first and fall through.
    if let Ok((rest, inner)) = parenthesized(|i| filter_or(i, depth + 1)).parse(input) {
        return Ok((rest, inner));
    }

    if let Ok((rest, call)) = filter_function_call(input, depth) {
        return Ok((rest, call));
    }

    let (rest, left) = filter_operand(input, depth)?;
    let Ok((rest, operator)) = ws(comparison_operator).parse(rest) else {
        return fail(
            input,
            "a comparison, a logical combination, a negation or a filter function call",
        );
    };
    let (rest, right) = cut(ws(|i| filter_operand(i, depth))).parse(rest)?;
    Ok((
        rest,
        FilterExpression::Comparison {
            left,
            operator,
            right,
        },
    ))
}

fn comparison_operator(input: &str) -> VResult<'_, crate::ast::ComparisonOperator> {
    use crate::ast::ComparisonOperator::*;
    alt((
        value(Equal, tag("==")),
        value(NotEqual, tag("!=")),
        value(LessEqual, tag("<=")),
        value(GreaterEqual, tag(">=")),
        value(LessThan, tag("<")),
        value(GreaterThan, tag(">")),
    ))
    .parse(input)
}

fn filter_function_call(input: &str, depth: usize) -> VResult<'_, FilterExpression> {
    let (rest, name) = terminated(identifier, peek(ws(char('(')))).parse(input)?;
    let func = match filter_function(name) {
        Some(func) => func,
        None => return fail(input, "a registered KIP filter function"),
    };
    let (rest, args) = cut(parenthesized(terminated(
        separated_list0(ws(char(',')), ws(|i| filter_operand(i, depth + 1))),
        opt(ws(char(','))),
    )))
    .parse(rest)?;
    Ok((rest, FilterExpression::Function { func, args }))
}

fn filter_function(name: &str) -> Option<FilterFunction> {
    Some(match name.to_ascii_uppercase().as_str() {
        "CONTAINS" => FilterFunction::Contains,
        "STARTS_WITH" => FilterFunction::StartsWith,
        "ENDS_WITH" => FilterFunction::EndsWith,
        "REGEX" => FilterFunction::Regex,
        "IN" => FilterFunction::In,
        "IS_NULL" => FilterFunction::IsNull,
        "IS_NOT_NULL" => FilterFunction::IsNotNull,
        "IS_LITERAL" => FilterFunction::IsLiteral,
        "IS_ELEMENT" => FilterFunction::IsElement,
        "IS_KIND" => FilterFunction::IsKind,
        "LITERAL_TYPE" => FilterFunction::LiteralType,
        _ => return None,
    })
}

/// One side of a comparison, or one argument of a filter function.
fn filter_operand(input: &str, depth: usize) -> VResult<'_, FilterOperand> {
    if depth >= MAX_KIP_NESTING_DEPTH {
        return fail(input, FILTER_TOO_DEEP);
    }
    alt((
        map(parameter, FilterOperand::Param),
        map(dot_path_var, FilterOperand::Variable),
        map(literal, FilterOperand::Literal),
        |i| filter_list(i, depth),
        map(
            preceded(ws(char('-')), cut(|i| filter_operand(i, depth + 1))),
            |operand| FilterOperand::Negate(Box::new(operand)),
        ),
        map(bound_object_literal_only, FilterOperand::Literal),
        parenthesized(|i| filter_operand(i, depth + 1)),
    ))
    .parse(input)
}

/// A bracketed operand list, e.g. the second argument of `IN`.
///
/// A trailing comma is legal in a JSON-value position but not here: a filter
/// list is an operand list, and only the source says which was written.
fn filter_list(input: &str, depth: usize) -> VResult<'_, FilterOperand> {
    let (rest, items) = delimited(
        ws(char('[')),
        separated_list0(ws(char(',')), ws(|i| filter_operand(i, depth + 1))),
        ws(char(']')),
    )
    .parse(input)?;
    Ok((rest, FilterOperand::List(items)))
}

/// An object literal used as a filter operand, which must be wholly literal.
fn bound_object_literal_only(input: &str) -> VResult<'_, KipValue> {
    let (rest, object) = bound_object_value(input)?;
    match object {
        BoundValue::Value(value) => Ok((rest, value)),
        _ => fail(input, "a filter operand with no unbound parameters"),
    }
}

// ---------------------------------------------------------------------------
// WHERE blocks
// ---------------------------------------------------------------------------

/// `where_block = "{" { where_clause } "}"`
pub fn where_block(input: &str, flavor: Flavor) -> VResult<'_, Vec<WhereClause>> {
    braced(many0(ws(|i| where_clause(i, flavor)))).parse(input)
}

fn where_clause(input: &str, flavor: Flavor) -> VResult<'_, WhereClause> {
    alt((
        // Keyword-led clauses first: they are unambiguous and give the best
        // errors when the body is wrong.
        preceded(
            ws(word("FILTER")),
            cut(map(parenthesized(filter_expression), |expression| {
                WhereClause::Filter { expression }
            })),
        ),
        preceded(
            ws(word("NOT")),
            cut(map(|i| where_block(i, flavor), WhereClause::Not)),
        ),
        preceded(
            ws(word("OPTIONAL")),
            cut(map(|i| where_block(i, flavor), WhereClause::Optional)),
        ),
        preceded(
            ws(word("UNION")),
            cut(map(|i| where_block(i, flavor), WhereClause::Union)),
        ),
        |i| variable_led_clause(i, flavor),
        // No variable: `PROPOSITION (...)`, `STRUCTURAL (...)`, or a bare tuple.
        preceded(
            ws(word("PROPOSITION")),
            cut(map(
                |i| proposition_matcher(i, flavor),
                |matcher| WhereClause::Proposition {
                    variable: None,
                    matcher,
                },
            )),
        ),
        preceded(
            ws(word("STRUCTURAL")),
            cut(map(
                |i| structural_tuple(i, flavor),
                |(subject, field, object)| WhereClause::Structural {
                    variable: None,
                    subject,
                    field,
                    object,
                },
            )),
        ),
        map(
            |i| proposition_matcher(i, flavor),
            |matcher| WhereClause::Proposition {
                variable: None,
                matcher,
            },
        ),
    ))
    .parse(input)
}

fn variable_led_clause(input: &str, flavor: Flavor) -> VResult<'_, WhereClause> {
    let (rest, var) = ws(variable).parse(input)?;

    // `?v CONCEPT {...}` and `?v {...}` mean the same thing; the keyword is
    // optional exactly here (Spec §43.1).
    if let Ok((rest, _)) = ws(word("CONCEPT")).parse(rest) {
        let (rest, matcher) = cut(ws(|i| object_matcher(i, flavor))).parse(rest)?;
        return Ok((
            rest,
            WhereClause::Concept {
                variable: var,
                matcher,
            },
        ));
    }
    if let Ok((rest, _)) = ws(word("ASSERTION")).parse(rest) {
        let (rest, matcher) = cut(ws(|i| object_matcher(i, flavor))).parse(rest)?;
        return Ok((
            rest,
            WhereClause::Assertion {
                variable: var,
                matcher,
            },
        ));
    }
    if let Ok((rest, _)) = ws(word("EVIDENCE")).parse(rest) {
        let (rest, matcher) = cut(ws(|i| object_matcher(i, flavor))).parse(rest)?;
        return Ok((
            rest,
            WhereClause::Evidence {
                variable: var,
                matcher,
            },
        ));
    }
    if let Ok((rest, _)) = ws(word("ACTIVITY")).parse(rest) {
        let (rest, matcher) = cut(ws(|i| object_matcher(i, flavor))).parse(rest)?;
        return Ok((
            rest,
            WhereClause::Activity {
                variable: var,
                matcher,
            },
        ));
    }
    if let Ok((rest, _)) = ws(word("STRUCTURAL")).parse(rest) {
        let (rest, (subject, field, object)) = cut(|i| structural_tuple(i, flavor)).parse(rest)?;
        return Ok((
            rest,
            WhereClause::Structural {
                variable: Some(var),
                subject,
                field,
                object,
            },
        ));
    }
    if let Ok((rest, _)) = ws(word("BELIEF")).parse(rest) {
        if flavor != Flavor::Kql {
            return fail(
                input,
                "a mutable pattern: BELIEF is a read-only Projection and can never be a mutation \
                 target or an export selector",
            );
        }
        if let Ok((rest, _)) = ws(word("SLOT")).parse(rest) {
            let (rest, (subject, predicate)) = cut(parenthesized(separated_pair(
                ws(|i| proposition_subject(i, flavor)),
                ws(char(',')),
                ws(pred_atom),
            )))
            .parse(rest)?;
            return Ok((
                rest,
                WhereClause::BeliefSlot {
                    variable: var,
                    subject,
                    predicate,
                },
            ));
        }
        let (rest, target) = cut(|i| belief_target(i, flavor)).parse(rest)?;
        return Ok((
            rest,
            WhereClause::Belief {
                variable: var,
                target,
            },
        ));
    }
    if let Ok((rest, _)) = ws(word("PROPOSITION")).parse(rest) {
        let (rest, matcher) = cut(ws(|i| proposition_matcher(i, flavor))).parse(rest)?;
        return Ok((
            rest,
            WhereClause::Proposition {
                variable: Some(var),
                matcher,
            },
        ));
    }

    // No kind keyword: the bracket decides, and once it has decided the body is
    // committed — backtracking here would report "expected (" for a malformed
    // Concept block, which points at the wrong mistake.
    let (rest, _) = skip_ws_and_comments(rest)?;
    if rest.starts_with('{') {
        let (rest, matcher) = cut(ws(|i| object_matcher(i, flavor))).parse(rest)?;
        return Ok((
            rest,
            WhereClause::Concept {
                variable: var,
                matcher,
            },
        ));
    }
    let (rest, matcher) = ws(|i| proposition_matcher(i, flavor)).parse(rest)?;
    Ok((
        rest,
        WhereClause::Proposition {
            variable: Some(var),
            matcher,
        },
    ))
}

fn structural_tuple(input: &str, flavor: Flavor) -> VResult<'_, (Term, SymbolRef, Term)> {
    parenthesized((
        ws(|i| term(i, flavor)),
        preceded(ws(char(',')), ws(symbol_ref)),
        preceded(ws(char(',')), ws(|i| term(i, flavor))),
    ))
    .parse(input)
}

/// The operand of a `BELIEF`, which is the Proposition expression slot.
fn belief_target(input: &str, flavor: Flavor) -> VResult<'_, crate::ast::BeliefTarget> {
    use crate::ast::BeliefTarget;

    let (rest, _) = ws(char('(')).parse(input)?;

    if let Ok((rest, _)) = id_key(rest) {
        let (rest, id) = cut(ws(scalar)).parse(rest)?;
        let (rest, _) = cut(ws(char(')'))).parse(rest)?;
        return Ok((rest, BeliefTarget::Id(id)));
    }

    // `BELIEF (?p)` names an already-bound Proposition; the closing paren is
    // what tells it apart from a tuple whose subject happens to be a variable.
    if let Ok((rest, var)) = terminated(ws(variable), peek(ws(char(')')))).parse(rest) {
        let (rest, _) = cut(ws(char(')'))).parse(rest)?;
        return Ok((rest, BeliefTarget::Proposition(var)));
    }

    let (rest, subject) = cut(ws(|i| proposition_subject(i, flavor))).parse(rest)?;
    let (rest, _) = cut(ws(char(','))).parse(rest)?;
    // Projection must not propagate belief along a path (Spec §45), so the
    // predicate here is always exact, even inside KQL.
    let (rest, predicate) = cut(ws(pred_atom)).parse(rest)?;
    let (rest, _) = cut(ws(char(','))).parse(rest)?;
    let (rest, object) = cut(ws(|i| term(i, flavor))).parse(rest)?;
    let (rest, _) = cut(ws(char(')'))).parse(rest)?;

    Ok((
        rest,
        BeliefTarget::Tuple(PropositionTriple {
            subject,
            predicate: PredTerm::Atom(predicate),
            object,
        }),
    ))
}

// ---------------------------------------------------------------------------
// Handle and variable collection, shared by the KML validators
// ---------------------------------------------------------------------------

/// Collects every variable a WHERE block binds.
///
/// Filter operands carry a [`DotPathVar`] rather than a bare name and are
/// deliberately skipped: reading `?x.score` inside a FILTER does not bind `?x`.
pub fn collect_where_variables(clauses: &[WhereClause], out: &mut BTreeSet<String>) {
    for clause in clauses {
        match clause {
            WhereClause::Concept { variable, matcher }
            | WhereClause::Assertion { variable, matcher }
            | WhereClause::Evidence { variable, matcher }
            | WhereClause::Activity { variable, matcher } => {
                out.insert(variable.clone());
                collect_matcher_variables(matcher, out);
            }
            WhereClause::Proposition { variable, matcher } => {
                if let Some(variable) = variable {
                    out.insert(variable.clone());
                }
                collect_proposition_variables(matcher, out);
            }
            WhereClause::Structural {
                variable,
                subject,
                object,
                ..
            } => {
                if let Some(variable) = variable {
                    out.insert(variable.clone());
                }
                collect_term_variables(subject, out);
                collect_term_variables(object, out);
            }
            WhereClause::Belief { variable, target } => {
                out.insert(variable.clone());
                match target {
                    crate::ast::BeliefTarget::Proposition(name) => {
                        out.insert(name.clone());
                    }
                    crate::ast::BeliefTarget::Id(_) => {}
                    crate::ast::BeliefTarget::Tuple(triple) => {
                        collect_triple_variables(triple, out)
                    }
                }
            }
            WhereClause::BeliefSlot {
                variable,
                subject,
                predicate,
            } => {
                out.insert(variable.clone());
                collect_term_variables(subject, out);
                collect_pred_atom_variable(predicate, out);
            }
            WhereClause::Filter { .. } => {}
            WhereClause::Not(inner) | WhereClause::Optional(inner) | WhereClause::Union(inner) => {
                collect_where_variables(inner, out)
            }
        }
    }
}

fn collect_matcher_variables(matcher: &ObjectMatcher, out: &mut BTreeSet<String>) {
    for value in matcher.values() {
        match value {
            MatchValue::Variable(name) => {
                out.insert(name.clone());
            }
            MatchValue::Array(items) => {
                for item in items {
                    collect_match_value_variables(item, out);
                }
            }
            MatchValue::Match(inner) => collect_matcher_variables(inner, out),
            MatchValue::Proposition(inner) => collect_proposition_variables(inner, out),
            MatchValue::Param(_) | MatchValue::Literal(_) => {}
        }
    }
}

fn collect_match_value_variables(value: &MatchValue, out: &mut BTreeSet<String>) {
    match value {
        MatchValue::Variable(name) => {
            out.insert(name.clone());
        }
        MatchValue::Array(items) => {
            for item in items {
                collect_match_value_variables(item, out);
            }
        }
        MatchValue::Match(inner) => collect_matcher_variables(inner, out),
        MatchValue::Proposition(inner) => collect_proposition_variables(inner, out),
        MatchValue::Param(_) | MatchValue::Literal(_) => {}
    }
}

fn collect_proposition_variables(matcher: &PropositionMatcher, out: &mut BTreeSet<String>) {
    match matcher {
        PropositionMatcher::Id(_) => {}
        PropositionMatcher::Tuple(triple) => collect_triple_variables(triple, out),
    }
}

fn collect_triple_variables(triple: &PropositionTriple, out: &mut BTreeSet<String>) {
    collect_term_variables(&triple.subject, out);
    collect_term_variables(&triple.object, out);
    match &triple.predicate {
        PredTerm::Atom(atom) => collect_pred_atom_variable(atom, out),
        PredTerm::Path(atoms) => {
            for atom in atoms {
                collect_pred_atom_variable(&atom.predicate, out);
            }
        }
    }
}

fn collect_term_variables(term: &Term, out: &mut BTreeSet<String>) {
    match term {
        Term::Variable(name) => {
            out.insert(name.clone());
        }
        Term::Match(matcher) => collect_matcher_variables(matcher, out),
        Term::Proposition(matcher) => collect_proposition_variables(matcher, out),
        Term::Param(_) | Term::Literal(_) => {}
    }
}

fn collect_pred_atom_variable(atom: &PredAtom, out: &mut BTreeSet<String>) {
    if let PredAtom::Variable(name) = atom {
        out.insert(name.clone());
    }
}

/// Collects every `?handle` a value tree references.
pub fn collect_bound_value_handles(value: &BoundValue, out: &mut BTreeSet<String>) {
    match value {
        BoundValue::Handle(name) => {
            out.insert(name.clone());
        }
        BoundValue::Array(items) => {
            for item in items {
                collect_bound_value_handles(item, out);
            }
        }
        BoundValue::Object(entries) => {
            for (_, item) in entries {
                collect_bound_value_handles(item, out);
            }
        }
        BoundValue::Value(_) | BoundValue::Param(_) | BoundValue::Variable(_) => {}
    }
}

/// Collects every `?handle` a mutation right-hand side references.
pub fn collect_mutation_value_handles(value: &MutationValue, out: &mut BTreeSet<String>) {
    match value {
        MutationValue::Handle(name) => {
            out.insert(name.clone());
        }
        MutationValue::Array(items) => {
            for item in items {
                collect_bound_value_handles(item, out);
            }
        }
        MutationValue::Object(entries) => {
            for (_, item) in entries {
                collect_bound_value_handles(item, out);
            }
        }
        MutationValue::Value(_)
        | MutationValue::Param(_)
        | MutationValue::Variable(_)
        | MutationValue::Expr(_) => {}
    }
}

/// Collects every `?handle` an option block references.
pub fn collect_bound_object_handles(object: &BoundObject, out: &mut BTreeSet<String>) {
    for value in object.values() {
        collect_bound_value_handles(value, out);
    }
}

/// Collects the variables an update expression reads.
pub fn collect_update_expr_paths<'a>(expr: &'a UpdateExpr, out: &mut Vec<&'a DotPathVar>) {
    match expr {
        UpdateExpr::Variable(path) => out.push(path),
        UpdateExpr::Function { args, .. } => {
            for arg in args {
                collect_update_expr_paths(arg, out);
            }
        }
        UpdateExpr::Number(_) | UpdateExpr::Param(_) => {}
    }
}

/// Collects the variables a mutation right-hand side reads.
pub fn collect_mutation_value_paths<'a>(value: &'a MutationValue, out: &mut Vec<&'a DotPathVar>) {
    match value {
        MutationValue::Variable(path) => out.push(path),
        MutationValue::Expr(expr) => collect_update_expr_paths(expr, out),
        MutationValue::Array(items) => {
            for item in items {
                collect_bound_value_paths(item, out);
            }
        }
        MutationValue::Object(entries) => {
            for (_, item) in entries {
                collect_bound_value_paths(item, out);
            }
        }
        MutationValue::Value(_) | MutationValue::Param(_) | MutationValue::Handle(_) => {}
    }
}

fn collect_bound_value_paths<'a>(value: &'a BoundValue, out: &mut Vec<&'a DotPathVar>) {
    match value {
        BoundValue::Variable(path) => out.push(path),
        BoundValue::Array(items) => {
            for item in items {
                collect_bound_value_paths(item, out);
            }
        }
        BoundValue::Object(entries) => {
            for (_, item) in entries {
                collect_bound_value_paths(item, out);
            }
        }
        BoundValue::Value(_) | BoundValue::Param(_) | BoundValue::Handle(_) => {}
    }
}

// ---------------------------------------------------------------------------
// Small utilities
// ---------------------------------------------------------------------------

/// Wraps a parser so it also yields the input slice where its match starts,
/// which is what anchors a duplicate-key error at the offending key.
pub fn spanned<'a, O, F>(
    mut f: F,
) -> impl Parser<&'a str, Output = (&'a str, O), Error = VerboseError<&'a str>>
where
    F: Parser<&'a str, Output = O, Error = VerboseError<&'a str>>,
{
    move |input: &'a str| {
        let (rest, value) = f.parse(input)?;
        Ok((rest, (input, value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse<T>(f: impl Fn(&str) -> VResult<'_, T>, input: &str) -> T {
        let (rest, value) = f(input).unwrap_or_else(|e| panic!("failed to parse {input:?}: {e}"));
        assert!(rest.trim().is_empty(), "unconsumed input {rest:?}");
        value
    }

    #[test]
    fn keywords_are_case_insensitive_but_literals_are_not() {
        assert!(word("FIND").parse("find(").is_ok());
        assert!(word("FIND").parse("FiNd(").is_ok());
        // Glued to an identifier character it is not a keyword at all.
        assert!(word("FIND").parse("FINDX").is_err());

        assert_eq!(parse(literal, "true"), KipValue::Bool(true));
        assert!(literal("TRUE").is_err());
        assert!(literal("Null").is_err());
    }

    #[test]
    fn keywords_double_as_field_names() {
        // Spec examples use `by`, `mode`, `type` and `status` as object keys;
        // rejecting a keyword there would make them unparseable.
        let matcher = parse(
            |i| object_matcher(i, Flavor::Exact),
            r#"{ type: "Person", status: "open", by: ?actor }"#,
        );
        assert_eq!(matcher.len(), 3);
        assert!(matches!(matcher["by"], MatchValue::Variable(_)));
    }

    #[test]
    fn dot_paths_carry_both_step_kinds() {
        let path = parse(dot_path_var, r#"?x.facets["MnemonicState"].salience"#);
        assert_eq!(path.var, "x");
        assert_eq!(
            path.path,
            vec![
                PathStep::Field("facets".into()),
                PathStep::Key("MnemonicState".into()),
                PathStep::Field("salience".into()),
            ]
        );
        assert!(dot_path_var("?x.").is_err());
    }

    #[test]
    fn a_bare_variable_in_a_value_position_is_a_handle() {
        assert_eq!(
            parse(bound_value, "?evidence"),
            BoundValue::Handle("evidence".into())
        );
        assert!(matches!(
            parse(bound_value, "?evidence.observed_at"),
            BoundValue::Variable(_)
        ));
    }

    #[test]
    fn fully_literal_subtrees_collapse_to_one_value() {
        assert_eq!(
            parse(bound_value, r#"[1, "a"]"#),
            BoundValue::Value(KipValue::Array(vec![
                KipValue::Number(Number::from(1)),
                KipValue::String("a".into())
            ]))
        );
        // One unbound leaf keeps the whole shape.
        assert!(matches!(
            parse(bound_value, r#"[1, :p]"#),
            BoundValue::Array(_)
        ));
    }

    #[test]
    fn assignments_reject_engine_owned_and_duplicate_fields() {
        assert!(assignments(r#"{ _system: 1 }"#).is_err());
        assert!(assignments(r#"{ governance: {} }"#).is_err());
        assert!(assignments(r#"{ space_seq: 1 }"#).is_err());
        assert!(assignments(r#"{ a: 1, a: 2 }"#).is_err());
        assert_eq!(parse(assignments, r#"{ a: 1, b: 2 }"#).len(), 2);
    }

    #[test]
    fn duplicate_keys_are_rejected_in_every_block_kind() {
        assert!(object_matcher(r#"{ a: 1, a: 2 }"#, Flavor::Exact).is_err());
        assert!(bound_object(r#"{ a: 1, "a": 2 }"#).is_err());
        assert!(unset_field_set(r#"{ a, a }"#).is_err());
    }

    #[test]
    fn a_single_unquantified_atom_collapses_to_an_exact_predicate() {
        assert_eq!(
            parse(|i| predicate_term(i, Flavor::Kql), r#""treats""#),
            PredTerm::Atom(PredAtom::Literal("treats".into()))
        );
        let path = parse(|i| predicate_term(i, Flavor::Kql), r#""part_of"{1,3}"#);
        assert_eq!(
            path,
            PredTerm::Path(vec![PredPathAtom {
                predicate: PredAtom::Literal("part_of".into()),
                hops: Some(HopRange {
                    min: 1,
                    max: Some(3)
                })
            }])
        );
        // `{2}` and `{2,}` are different quantifiers.
        let exact = parse(|i| predicate_term(i, Flavor::Kql), r#""p"{2}"#);
        let open = parse(|i| predicate_term(i, Flavor::Kql), r#""p"{2,}"#);
        assert_ne!(exact, open);
    }

    #[test]
    fn raw_paths_are_kql_only() {
        assert!(predicate_term(r#""a"|"b""#, Flavor::Kql).is_ok());
        // In the mutation flavor the alternation is simply not part of the
        // predicate, so the `|` is left unconsumed for the caller to reject.
        let (rest, _) = predicate_term(r#""a"|"b""#, Flavor::Exact).unwrap();
        assert_eq!(rest, r#"|"b""#);
    }

    #[test]
    fn proposition_tuples_accept_both_spellings_of_the_slot() {
        assert!(matches!(
            parse(|i| proposition_matcher(i, Flavor::Kql), r#"(id: "P-1")"#),
            PropositionMatcher::Id(_)
        ));
        assert!(matches!(
            parse(
                |i| proposition_matcher(i, Flavor::Kql),
                r#"(?s, "pred", ?o)"#
            ),
            PropositionMatcher::Tuple(_)
        ));
        // `id` is an ordinary identifier, so it stays case-sensitive.
        assert!(proposition_matcher(r#"(ID: "P-1")"#, Flavor::Kql).is_err());
    }

    #[test]
    fn a_proposition_subject_is_never_a_literal() {
        assert!(proposition_subject(r#""Alice""#, Flavor::Kql).is_err());
        assert!(proposition_subject("?alice", Flavor::Kql).is_ok());
    }

    #[test]
    fn where_clauses_dispatch_on_keyword_then_bracket() {
        let clauses = parse(
            |i| where_block(i, Flavor::Kql),
            r#"{
                ?drug {type: "Drug"}
                ?p CONCEPT {name: "x"}
                (?drug, "treats", ?p)
                ?prop PROPOSITION (?drug, "treats", ?p)
                ?a ASSERTION {stance: "support"}
                ?e EVIDENCE {evidence_class: "user_statement"}
                ?act ACTIVITY {activity_class: "import"}
                ?edge STRUCTURAL (?x, "has_step", ?y)
                ?b BELIEF (?prop)
                ?slot BELIEF SLOT (?drug, "treats")
                FILTER(?drug.risk < 3)
                NOT { ?x {type: "Y"} }
                OPTIONAL { ?x {type: "Y"} }
                UNION { ?x {type: "Y"} }
            }"#,
        );
        assert_eq!(clauses.len(), 14);
        assert!(matches!(clauses[0], WhereClause::Concept { .. }));
        assert!(matches!(
            clauses[2],
            WhereClause::Proposition { variable: None, .. }
        ));
        assert!(matches!(clauses[8], WhereClause::Belief { .. }));
        assert!(matches!(clauses[9], WhereClause::BeliefSlot { .. }));
    }

    #[test]
    fn belief_is_rejected_outside_kql() {
        assert!(where_block(r#"{ ?b BELIEF (?p) }"#, Flavor::Kql).is_ok());
        assert!(where_block(r#"{ ?b BELIEF (?p) }"#, Flavor::Exact).is_err());
    }

    #[test]
    fn belief_accepts_all_three_operand_forms() {
        use crate::ast::BeliefTarget;
        let block = parse(
            |i| where_block(i, Flavor::Kql),
            r#"{ ?b BELIEF (?p) ?c BELIEF (id: "P-1") ?d BELIEF (?s, "pred", ?o) }"#,
        );
        assert!(matches!(
            &block[0],
            WhereClause::Belief {
                target: BeliefTarget::Proposition(_),
                ..
            }
        ));
        assert!(matches!(
            &block[1],
            WhereClause::Belief {
                target: BeliefTarget::Id(_),
                ..
            }
        ));
        assert!(matches!(
            &block[2],
            WhereClause::Belief {
                target: BeliefTarget::Tuple(_),
                ..
            }
        ));
    }

    #[test]
    fn belief_never_walks_a_raw_path() {
        // Spec §45: projection must not propagate belief along a path.
        assert!(where_block(r#"{ ?b BELIEF (?s, "a"|"b", ?o) }"#, Flavor::Kql).is_err());
        assert!(where_block(r#"{ ?b BELIEF (?s, "a"{1,2}, ?o) }"#, Flavor::Kql).is_err());
    }

    #[test]
    fn filters_build_the_closed_expression_tree() {
        use crate::ast::{ComparisonOperator, LogicalOperator};
        let expr = parse(filter_expression, r#"?a.x > 1 && !IS_NULL(?b)"#);
        match expr {
            FilterExpression::Logical {
                operator: LogicalOperator::And,
                left,
                right,
            } => {
                assert!(matches!(
                    *left,
                    FilterExpression::Comparison {
                        operator: ComparisonOperator::GreaterThan,
                        ..
                    }
                ));
                assert!(matches!(*right, FilterExpression::Not(_)));
            }
            other => panic!("unexpected {other:?}"),
        }

        // `!=` must not be split into `!` and `=`.
        assert!(matches!(
            parse(filter_expression, r#"?a != 1"#),
            FilterExpression::Comparison {
                operator: ComparisonOperator::NotEqual,
                ..
            }
        ));
    }

    #[test]
    fn a_filter_list_rejects_the_trailing_comma_a_json_array_allows() {
        assert!(filter_expression(r#"IN(?x, [1, 2])"#).is_ok());
        assert!(filter_expression(r#"IN(?x, [1, 2,])"#).is_err());
        // The same bytes are fine in a JSON-value position.
        assert!(bound_value(r#"[1, 2,]"#).is_ok());
    }

    #[test]
    fn an_unknown_function_is_not_a_filter() {
        assert!(filter_expression(r#"NOT_A_FUNCTION(?x)"#).is_err());
        // Aggregates summarize a solution set; a filter runs per candidate row.
        assert!(filter_expression(r#"COUNT(?x) > 1"#).is_err());
    }

    #[test]
    fn update_functions_check_their_arity() {
        assert!(mutation_value("ADD(?x.n, 1)").is_ok());
        assert!(mutation_value("ADD(?x.n)").is_err());
        assert!(mutation_value("CLAMP(?x.n, 0, 1)").is_ok());
        assert!(mutation_value("CLAMP(?x.n, 0)").is_err());
        assert!(mutation_value("NOPE(?x.n, 1)").is_err());
        // Case-insensitive, like every other registered name.
        assert!(mutation_value("mul(?x.n, 0.9)").is_ok());
    }

    #[test]
    fn negating_the_i64_floor_does_not_overflow() {
        // `-i64::MIN` is not an i64; computing it as `-n` aborted the parser.
        assert_eq!(
            parse(update_expr, "--9223372036854775808"),
            UpdateExpr::Number(Number::from(9_223_372_036_854_775_808u64))
        );
        assert_eq!(
            parse(update_expr, "-9223372036854775808"),
            UpdateExpr::Number(Number::from(i64::MIN))
        );
        // An integer above i64::MAX has no exact negation, so it is refused
        // rather than silently stored as a different (f64) number.
        assert!(update_expr("-18446744073709551615").is_err());
    }

    #[test]
    fn operators_that_open_no_bracket_still_hit_the_nesting_budget() {
        // The input budget counts brackets; `!`, unary `-`, `&&` and `||` nest
        // one level each while opening none, so an unbounded run used to walk
        // the recursive-descent parser (or the tree's Drop) off the stack.
        let deep_not = format!("{}1 == 1", "!".repeat(MAX_KIP_NESTING_DEPTH + 2));
        assert!(filter_expression(&deep_not).is_err());

        let deep_neg = format!("?x > {}1", "-".repeat(MAX_KIP_NESTING_DEPTH + 2));
        assert!(filter_expression(&deep_neg).is_err());

        let long_chain = std::iter::repeat_n("1 == 1", MAX_KIP_NESTING_DEPTH + 2)
            .collect::<Vec<_>>()
            .join(" && ");
        assert!(filter_expression(&long_chain).is_err());

        let long_or = std::iter::repeat_n("1 == 1", MAX_KIP_NESTING_DEPTH + 2)
            .collect::<Vec<_>>()
            .join(" || ");
        assert!(filter_expression(&long_or).is_err());

        // Ordinary filters are nowhere near the ceiling.
        assert!(filter_expression(r#"!(?a.x > 1 && ?b < 2 || IS_NULL(?c))"#).is_ok());
        let ok_chain = std::iter::repeat_n("1 == 1", MAX_KIP_NESTING_DEPTH - 1)
            .collect::<Vec<_>>()
            .join(" && ");
        assert!(filter_expression(&ok_chain).is_ok());
    }

    #[test]
    fn hop_quantifiers_must_be_well_ordered() {
        assert!(predicate_term(r#""p"{3,1}"#, Flavor::Kql).is_err());
        assert!(predicate_term(r#""p"{1,3}"#, Flavor::Kql).is_ok());
    }

    #[test]
    fn comments_separate_keywords() {
        assert!(
            words(&["ORDER", "BY"])
                .parse("ORDER // pick one\n BY")
                .is_ok()
        );
        assert!(words(&["ORDER", "BY"]).parse("ORDERBY").is_err());
    }
}
