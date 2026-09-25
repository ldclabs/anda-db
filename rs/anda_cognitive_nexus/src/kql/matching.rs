//! # Matching element patterns
//!
//! Each `WHERE` pattern turns into a set of solutions. The shape is always the
//! same: narrow with the indexes that exist, load what survives, then decide
//! the rest against the rendered view.
//!
//! ## Archived elements are out of ordinary recall
//!
//! A pattern that does not mention `state` matches active elements only. That
//! is what archiving *means* (§41.2) — the element still exists and every
//! reference to it still resolves, but it is no longer recalled by default. A
//! query that wants the archived ones asks for them by writing `{state:
//! "archived"}`.
//!
//! ## What a raw pattern is, and is not
//!
//! `?p PROPOSITION (...)` and `?a ASSERTION {...}` read the graph as recorded.
//! They report that a tuple exists and that somebody claimed something; they
//! never report that it is true. Belief is projected, and lives in `BELIEF`.

use anda_db::query::{Filter, RangeQuery};
use anda_db_schema::Fv;
use anda_kip::ElementKind;
use anda_kip::{
    Json, KipError, MatchValue, ObjectMatcher, PredAtom, PredTerm, PropositionMatcher,
    PropositionTriple, Scalar, SymbolRef as AstSymbolRef, Term,
};

use super::Context;
use super::binding::{Binding, Solutions};
use crate::id::ElementId;
use crate::schema::{Intent, SymbolKind};
use crate::store::{eq_field, key_filter};
use crate::term::Endpoint;

/// Whether a tuple endpoint is an inline pattern — a nested Proposition or a
/// matcher that searches rather than names — which is expanded into its own
/// solutions before the tuple is matched.
fn is_inline_pattern(term: &Term) -> bool {
    matches!(term, Term::Proposition(_))
        || matches!(term, Term::Match(matcher) if !identity_matcher(matcher))
}

/// The internal handle a `BELIEF (triple)` binds its resolved Proposition to.
///
/// Named rather than spelled inline because it is a variable name that must not
/// collide with a caller's: it is stripped from the result before the join, and
/// a query that happened to use the same spelling would lose its own column.
const BELIEF_TARGET: &str = "\0belief_target";

/// One matcher entry, once its value has been classified.
enum Slot {
    /// A concrete value the pattern constrains.
    Value(Json),
    /// A nested subset pattern or an array with binding sites.
    Pattern,
}

/// How a field's JSON representation participates in binding and matching.
#[derive(Clone, Copy, Default)]
struct FieldSemantics {
    is_reference: bool,
    is_symbol: bool,
    facet_map: bool,
}

type EndpointExpansion = (Term, Solutions, Option<String>);
type EndpointExpansionFuture<'a> =
    std::pin::Pin<Box<dyn Future<Output = Result<EndpointExpansion, KipError>> + Send + 'a>>;

/// The columns a matcher key selects on, per element kind.
///
/// Only the keys with an index are listed. Anything else falls through to the
/// rendered view, where it is compared by dot path — correct, just not
/// index-accelerated.
fn column_of(kind: ElementKind, key: &str) -> Option<&'static str> {
    match (kind, key) {
        (_, "id") => Some("__id"),
        (_, "state") => Some("state"),
        (ElementKind::Concept, "type") => Some("schema_ref"),
        (ElementKind::Concept, "schema_ref") => Some("schema_ref"),
        (ElementKind::Concept, "key") => Some("key"),
        (ElementKind::Concept, "name") => Some("name"),
        (ElementKind::Concept, "canonical_id") => Some("canonical_id"),
        (ElementKind::Assertion, "proposition") => Some("proposition_id"),
        (ElementKind::Assertion, "stance") => Some("stance"),
        (ElementKind::Assertion, "mode") => Some("mode"),
        (ElementKind::Assertion, "status") => Some("status"),
        (ElementKind::Evidence, "evidence_class") => Some("evidence_class"),
        (ElementKind::Evidence, "class") => Some("evidence_class"),
        (ElementKind::Evidence, "content_digest") => Some("content_digest"),
        (ElementKind::Evidence, "status") => Some("status"),
        (ElementKind::Activity, "activity_class") => Some("activity_class"),
        (ElementKind::Activity, "class") => Some("activity_class"),
        (ElementKind::Activity, "status") => Some("status"),
        _ => None,
    }
}

/// The dot path a matcher key reads in the rendered view.
///
/// What is left here is sugar — a short spelling the Specification's own
/// examples use — rather than translation. `proposition` used to be renamed to
/// `proposition_id` on the way in because the view spelled it that way; the
/// view now spells it as §13.2 does, so the entry is gone and
/// `FIND(?a.proposition)` reads the same field the pattern matched on.
fn view_key(kind: ElementKind, key: &str) -> String {
    match (kind, key) {
        (_, "state") => "_system.state".to_string(),
        (ElementKind::Concept, "type") => "schema_ref".to_string(),
        (ElementKind::Evidence, "class") => "evidence_class".to_string(),
        (ElementKind::Activity, "class") => "activity_class".to_string(),
        (ElementKind::Assertion, "by") => "asserted_by".to_string(),
        (ElementKind::Assertion, "status") => "lifecycle.status".to_string(),
        (ElementKind::Evidence, "status") => "lifecycle.status".to_string(),
        _ => key.to_string(),
    }
}

/// Whether a matcher key names a reference to another element.
///
/// A reference field holds an id, so binding it must produce an *element*
/// rather than the string that spells one — otherwise `?a ASSERTION
/// {proposition: ?p}` cannot join against `?p PROPOSITION (...)`, because a
/// string never equals an element.
fn is_reference_key(kind: ElementKind, key: &str) -> bool {
    matches!(
        (kind, key),
        (ElementKind::Assertion, "proposition")
            | (ElementKind::Assertion, "by")
            | (ElementKind::Assertion, "asserted_by")
            | (ElementKind::Evidence, "generated_by")
    )
}

/// Whether a matcher key names a schema symbol, which must be resolved.
fn is_symbol_key(kind: ElementKind, key: &str) -> bool {
    matches!(
        (kind, key),
        (ElementKind::Concept, "type") | (ElementKind::Concept, "schema_ref")
    )
}

impl Context<'_> {
    /// Resolve command-determined pattern errors even when no row reaches it.
    pub(super) fn validate_pattern(
        &mut self,
        clause: &anda_kip::WhereClause,
    ) -> Result<(), KipError> {
        use anda_kip::WhereClause;
        match clause {
            WhereClause::Concept { matcher, .. }
            | WhereClause::Assertion { matcher, .. }
            | WhereClause::Evidence { matcher, .. }
            | WhereClause::Activity { matcher, .. } => {
                let kind = match clause {
                    WhereClause::Concept { .. } => ElementKind::Concept,
                    WhereClause::Assertion { .. } => ElementKind::Assertion,
                    WhereClause::Evidence { .. } => ElementKind::Evidence,
                    _ => ElementKind::Activity,
                };
                for (key, value) in matcher {
                    if let Slot::Value(value) = self.classify(value)? {
                        if key == "id" {
                            let text = value.as_str().ok_or_else(|| {
                                KipError::type_mismatch("id must be an element id string")
                            })?;
                            ElementId::parse_kind(text, kind)?;
                        } else if is_symbol_key(kind, key) || column_of(kind, key).is_some() {
                            self.matcher_text(kind, key, &value)?;
                        }
                    }
                }
            }
            WhereClause::Proposition { variable, matcher } => {
                self.validate_proposition(variable.as_deref(), matcher)?
            }
            WhereClause::Belief { target, .. } => match target {
                anda_kip::BeliefTarget::Id(id) => {
                    self.scalar_element(id, ElementKind::Proposition)?;
                }
                anda_kip::BeliefTarget::Tuple(triple) => {
                    self.validate_proposition(None, &PropositionMatcher::Tuple(triple.clone()))?
                }
                _ => {}
            },
            WhereClause::BeliefSlot {
                subject, predicate, ..
            } => {
                self.endpoint_slot(subject)?;
                self.predicate_atom(predicate)?;
            }
            WhereClause::Structural {
                subject,
                field,
                object,
                ..
            } => {
                self.validate_endpoint(subject)?;
                self.validate_endpoint(object)?;
                let name = match field {
                    AstSymbolRef::Name(name) => name.clone(),
                    AstSymbolRef::Param(name) => self
                        .param_ref(name)?
                        .as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| {
                            KipError::type_mismatch("a structural field must be a symbol string")
                        })?,
                };
                if core_structural_field(&name).is_none() {
                    self.env
                        .resolve_symbol(SymbolKind::StructuralField, &name, Intent::Read)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn validate_proposition(
        &mut self,
        variable: Option<&str>,
        matcher: &PropositionMatcher,
    ) -> Result<(), KipError> {
        match matcher {
            PropositionMatcher::Id(id) => {
                self.scalar_element(id, ElementKind::Proposition)?;
            }
            PropositionMatcher::Tuple(triple) => {
                self.validate_endpoint(&triple.subject)?;
                self.validate_endpoint(&triple.object)?;
                if self.traversal_of(&triple.predicate)?.is_some() {
                    if variable.is_some() {
                        return Err(KipError::invalid_syntax(
                            "a raw path cannot bind a Proposition variable",
                        ));
                    }
                } else {
                    self.predicate_slot(&triple.predicate)?;
                }
            }
        }
        Ok(())
    }

    /// Match every supplied field against the same authorized element view.
    /// Index predicates narrow candidates; the complete matcher is still
    /// checked when an explicit id bypasses those indexes.
    pub async fn match_element(
        &mut self,
        kind: ElementKind,
        variable: &str,
        matcher: &ObjectMatcher,
    ) -> Result<Solutions, KipError> {
        let mut filters = vec![eq_field("space", Fv::Text(self.space.clone()))];
        let mut by_id = None;
        let mut type_lineages = Vec::new();
        let historical = self.is_historical();
        for (key, value) in matcher {
            let Slot::Value(value) = self.classify(value)? else {
                continue;
            };
            if is_symbol_key(kind, key) {
                let symbol = self.matcher_text(kind, key, &value)?;
                if !historical {
                    let ranges: Vec<Box<Filter>> = self
                        .env
                        .lineage_ranges(crate::schema::SymbolKind::ConceptType, &symbol)
                        .into_iter()
                        .map(|(low, high)| {
                            Box::new(Filter::Field((
                                "schema_ref".into(),
                                RangeQuery::Between(Fv::Text(low), Fv::Text(high)),
                            )))
                        })
                        .collect();
                    match ranges.len() {
                        0 => {}
                        1 => filters.extend(ranges.into_iter().map(|range| *range)),
                        _ => filters.push(Filter::Or(ranges)),
                    }
                }
                type_lineages.push(symbol);
            } else if key == "id" {
                let text = value
                    .as_str()
                    .ok_or_else(|| KipError::type_mismatch("id must be an element id string"))?;
                by_id = Some(ElementId::parse_kind(text, kind)?);
            } else if !historical && let Some(column) = column_of(kind, key) {
                filters.push(eq_field(
                    column,
                    Fv::Text(self.matcher_text(kind, key, &value)?),
                ));
            }
        }
        let constrains_state = matcher.contains_key("state");
        if !constrains_state {
            filters.push(eq_field(
                "state",
                Fv::Text(crate::store::rows::state::ACTIVE.into()),
            ));
        }
        let ids = match by_id {
            Some(id) => vec![id],
            None => {
                self.candidates(
                    kind,
                    Some(Filter::And(filters.into_iter().map(Box::new).collect())),
                )
                .await?
            }
        };
        self.charge(ids.len())?;
        let mut vars = vec![variable.to_string()];
        collect_matcher_vars(matcher, &mut vars);
        let mut rows = Vec::new();
        'candidates: for id in ids {
            let Some(element) = self.load(id).await? else {
                continue;
            };
            if element.space() != self.space || (!constrains_state && !element.is_active()) {
                continue;
            }
            if !type_lineages.iter().all(|symbol| {
                self.env.same_lineage(
                    crate::schema::SymbolKind::ConceptType,
                    element.schema_ref(),
                    symbol,
                )
            }) {
                continue;
            }
            let rendered = self.view_of(id);
            let mut row = vec![Binding::Null; vars.len()];
            set(&vars, &mut row, variable, Binding::Element(id));
            for (key, value) in matcher {
                if is_symbol_key(kind, key) && matches!(self.classify(value)?, Slot::Value(_)) {
                    continue;
                }
                let Some(read) = read_view_ref(&rendered, &view_key(kind, key)) else {
                    // Absence neither binds a field variable nor satisfies a
                    // literal-null constraint. OPTIONAL supplies missing rows.
                    continue 'candidates;
                };
                if !self.match_field_value(
                    value,
                    read,
                    FieldSemantics {
                        is_reference: is_reference_key(kind, key),
                        is_symbol: is_symbol_key(kind, key),
                        facet_map: key == "facets",
                    },
                    &vars,
                    &mut row,
                )? {
                    continue 'candidates;
                }
            }
            rows.push(row);
        }
        Ok(Solutions::table(vars, rows))
    }

    /// Apply a complete matcher to an already authorized view, without the
    /// ordinary query's implicit active-state restriction (used by UPSERT).
    pub(crate) fn matches_element_view(
        &mut self,
        kind: ElementKind,
        view: &Json,
        matcher: &ObjectMatcher,
    ) -> Result<bool, KipError> {
        let mut vars = Vec::new();
        collect_matcher_vars(matcher, &mut vars);
        let mut row = vec![Binding::Null; vars.len()];
        for (key, matcher) in matcher {
            let Some(value) = read_view_ref(view, &view_key(kind, key)) else {
                return Ok(false);
            };
            if is_symbol_key(kind, key)
                && let Slot::Value(expected) = self.classify(matcher)?
            {
                let symbol = self.matcher_text(kind, key, &expected)?;
                if !value.as_str().is_some_and(|stored| {
                    self.env
                        .same_lineage(crate::schema::SymbolKind::ConceptType, stored, &symbol)
                }) {
                    return Ok(false);
                }
            } else if !self.match_field_value(
                matcher,
                value,
                FieldSemantics {
                    is_reference: is_reference_key(kind, key),
                    is_symbol: is_symbol_key(kind, key),
                    facet_map: key == "facets",
                },
                &vars,
                &mut row,
            )? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn match_field_value(
        &mut self,
        matcher: &MatchValue,
        value: &Json,
        semantics: FieldSemantics,
        vars: &[String],
        row: &mut [Binding],
    ) -> Result<bool, KipError> {
        Ok(match matcher {
            MatchValue::Variable(name) => {
                let binding = if semantics.is_symbol
                    && let Json::String(symbol) = value
                {
                    Binding::Symbol(symbol.clone())
                } else {
                    value_binding(value.clone(), semantics.is_reference)
                };
                set(vars, row, name, binding)
            }
            MatchValue::Literal(literal) => matches_value(value, &Json::from(literal.clone())),
            MatchValue::Param(name) => matches_value(value, &self.param_ref(name)?),
            MatchValue::Match(fields) => {
                if !value.is_object() {
                    return Ok(false);
                }
                for (key, matcher) in fields {
                    let key = if semantics.facet_map {
                        self.env
                            .resolve_symbol(SymbolKind::Facet, key, Intent::Read)?
                            .to_string()
                    } else {
                        key.clone()
                    };
                    let Some(read) = value.get(&key) else {
                        return Ok(false);
                    };
                    if !self.match_field_value(
                        matcher,
                        read,
                        FieldSemantics::default(),
                        vars,
                        row,
                    )? {
                        return Ok(false);
                    }
                }
                true
            }
            MatchValue::Array(items) => {
                let Json::Array(values) = value else {
                    return Ok(false);
                };
                if items.len() != values.len() {
                    return Ok(false);
                }
                for (matcher, value) in items.iter().zip(values) {
                    if !self.match_field_value(
                        matcher,
                        value,
                        FieldSemantics::default(),
                        vars,
                        row,
                    )? {
                        return Ok(false);
                    }
                }
                true
            }
            MatchValue::Proposition(_) => {
                return Err(KipError::unsupported_capability(
                    "a nested Proposition in a field matcher must be bound in a separate Proposition pattern",
                ));
            }
        })
    }

    /// Matches `?p PROPOSITION (s, p, o)` or `?p PROPOSITION "P-1"`.
    pub async fn match_proposition(
        &mut self,
        variable: Option<&str>,
        matcher: &PropositionMatcher,
        known: &Solutions,
    ) -> Result<Solutions, KipError> {
        match matcher {
            PropositionMatcher::Id(scalar) => {
                let id = self.scalar_element(scalar, ElementKind::Proposition)?;
                let found = self
                    .load(id)
                    .await?
                    .filter(|element| element.space() == self.space && element.is_active());
                Ok(match (variable, found) {
                    (Some(var), Some(_)) => Solutions::column(var, vec![Binding::Element(id)]),
                    (None, Some(_)) => Solutions::unit(),
                    (_, None) => Solutions::empty(),
                })
            }
            PropositionMatcher::Tuple(triple) => self.match_tuple(variable, triple, known).await,
        }
    }

    fn validate_endpoint(&mut self, term: &Term) -> Result<(), KipError> {
        match term {
            Term::Match(matcher) if !identity_matcher(matcher) => {
                self.validate_pattern(&anda_kip::WhereClause::Concept {
                    variable: String::new(),
                    matcher: matcher.clone(),
                })
            }
            Term::Proposition(matcher) => self.validate_proposition(None, matcher),
            _ => {
                self.endpoint_slot(term)?;
                Ok(())
            }
        }
    }

    fn expand_endpoint<'s>(
        &'s mut self,
        term: &'s Term,
        known: &'s Solutions,
    ) -> EndpointExpansionFuture<'s> {
        Box::pin(async move {
            if !is_inline_pattern(term) {
                return Ok((term.clone(), Solutions::unit(), None));
            }
            let variable = format!("\0inline{}", self.next_internal_variable);
            self.next_internal_variable += 1;
            let mut table = match term {
                Term::Match(matcher) => {
                    self.match_element(ElementKind::Concept, &variable, matcher)
                        .await?
                }
                Term::Proposition(matcher) => {
                    self.match_proposition(Some(&variable), matcher, known)
                        .await?
                }
                _ => unreachable!(),
            };
            let index = table.vars.iter().position(|name| name == &variable);
            if let Some(index) = index {
                for row in &mut table.rows {
                    if let Some(id) = row[index].element() {
                        row[index] = Binding::Element(self.canonical_of(id).await?);
                    }
                }
            }
            Ok((Term::Variable(variable.clone()), table, Some(variable)))
        })
    }

    async fn match_tuple(
        &mut self,
        variable: Option<&str>,
        triple: &PropositionTriple,
        known: &Solutions,
    ) -> Result<Solutions, KipError> {
        // No inline pattern at either end: nothing to expand or join back.
        if !is_inline_pattern(&triple.subject) && !is_inline_pattern(&triple.object) {
            return self.match_tuple_simple(variable, triple, known).await;
        }
        let (subject, left, left_var) = self.expand_endpoint(&triple.subject, known).await?;
        let scope = self.join(known.clone(), left.clone())?;
        let (object, right, right_var) = self.expand_endpoint(&triple.object, &scope).await?;
        let expanded = self.join(left, right)?;
        let scoped = self.join(known.clone(), expanded.clone())?;
        let triple = PropositionTriple {
            subject,
            predicate: triple.predicate.clone(),
            object,
        };
        let matched = self.match_tuple_simple(variable, &triple, &scoped).await?;
        let mut result = self.join(matched, expanded)?;
        remove_internal(&mut result, left_var.iter().chain(right_var.iter()));
        Ok(result)
    }

    async fn match_tuple_simple(
        &mut self,
        variable: Option<&str>,
        triple: &PropositionTriple,
        known: &Solutions,
    ) -> Result<Solutions, KipError> {
        let subject = self.endpoint_slot(&triple.subject)?;
        let object = self.endpoint_slot(&triple.object)?;

        // A quantified path is a traversal, not a tuple: `(?a, "knows"{1,3},
        // ?b)` asks whether ?b is reachable, and the walk that answers it is
        // not itself a Proposition anybody stated.
        if let Some(walks) = self.traversal_of(&triple.predicate)? {
            if let Some(var) = variable {
                return Err(KipError::invalid_syntax(format!(
                    "?{var} cannot bind a hop-quantified path: a multi-hop walk is not a \
                     Proposition, and binding one of the tuples it crossed would name a claim the \
                     query never asked about. Drop the variable, or write an exact predicate"
                )));
            }
            return self.match_traversal(subject, object, &walks, known).await;
        }

        let predicates = self.predicate_slot(&triple.predicate)?;

        // §12.3, §43.2: matching is canonical. A fixed Concept endpoint
        // matches every stored endpoint in its merge class, so a term naming
        // the surviving identity finds the tuples recorded on what was merged
        // into it, and a term naming the merged-away Concept still finds them.
        let subject_keys = match &subject {
            EndpointSlot::Fixed(endpoint) => Some(self.endpoint_keys(endpoint).await?),
            EndpointSlot::Bind(_) => None,
        };
        let object_keys = match &object {
            EndpointSlot::Fixed(endpoint) => Some(self.endpoint_keys(endpoint).await?),
            EndpointSlot::Bind(_) => None,
        };

        let mut filters = vec![
            eq_field("space", Fv::Text(self.space.clone())),
            eq_field("state", Fv::Text(crate::store::rows::state::ACTIVE.into())),
        ];
        if let Some(keys) = &subject_keys {
            filters.push(key_filter("subject_key", keys));
        }
        if let Some(keys) = &object_keys {
            filters.push(key_filter("object_key", keys));
        }
        if let PredicateSlot::Fixed(symbols) = &predicates {
            filters.push(predicate_filter(&self.env, symbols));
        }

        let historical = self.is_historical();
        let ids = self
            .candidates(
                ElementKind::Proposition,
                Some(Filter::And(filters.into_iter().map(Box::new).collect())),
            )
            .await?;
        self.charge(ids.len())?;

        let mut vars: Vec<String> = Vec::new();
        if let Some(var) = variable {
            vars.push(var.to_string());
        }
        for slot in [&subject, &object] {
            if let EndpointSlot::Bind(name) = slot
                && !vars.contains(name)
            {
                vars.push(name.clone());
            }
        }
        if let PredicateSlot::Bind(name) = &predicates
            && !vars.contains(name)
        {
            vars.push(name.clone());
        }

        let mut rows = Vec::new();
        'candidates: for id in ids {
            let Some(crate::store::Element::Proposition(row)) = self.load(id).await? else {
                continue;
            };
            if !self.readable_tuple(id) {
                continue;
            }
            // The predicate index was ranged over a lineage, so the symbol is
            // checked here; at a past coordinate no filter could be pushed
            // down at all, so the whole tuple is.
            if !tuple_matches(
                &self.env,
                &row,
                subject_keys.as_deref(),
                object_keys.as_deref(),
                &predicates,
                historical,
            ) {
                continue;
            }
            let mut solution = vec![Binding::Null; vars.len()];
            if let Some(var) = variable
                && !set(&vars, &mut solution, var, Binding::Element(id))
            {
                continue 'candidates;
            }
            if let EndpointSlot::Bind(name) = &subject {
                let endpoint = self.canonical_endpoint(&row.subject).await?;
                if !set(&vars, &mut solution, name, endpoint_binding(&endpoint)) {
                    continue 'candidates;
                }
            }
            if let EndpointSlot::Bind(name) = &object {
                let endpoint = self.canonical_endpoint(&row.object).await?;
                if !set(&vars, &mut solution, name, endpoint_binding(&endpoint)) {
                    continue 'candidates;
                }
            }
            if let PredicateSlot::Bind(name) = &predicates
                && !set(
                    &vars,
                    &mut solution,
                    name,
                    Binding::Symbol(row.predicate_ref.clone()),
                )
            {
                continue 'candidates;
            }
            rows.push(solution);
        }
        Ok(Solutions::table(vars, rows))
    }

    /// Matches `?edge STRUCTURAL (?src, "field", ?dst)` — record topology.
    ///
    /// Both planes, addressed the same way: a Profile structural field resolves
    /// through the Schema Environment to a symbol, and a Core one (§8.2) is
    /// named plainly. So "which Assertions cite this Evidence" is
    /// `STRUCTURAL (?a, "evidence", :e)`.
    ///
    /// The two are never merged. A name is looked up on each plane
    /// independently, and `?edge.field` carries the full symbol for a Profile
    /// field and the plain name for a Core one, so a result says which plane
    /// it came from. A Profile that declares a field named `evidence`
    /// therefore adds edges rather than changing what an Assertion cites — and
    /// a caller that wants only the Profile one addresses it by its full
    /// symbol.
    ///
    /// A structural reference is never a semantic Proposition (§17.3): this
    /// pattern reports how records are assembled, and a claim *about* that
    /// relation would be a separate Proposition plus Assertion.
    pub async fn match_structural(
        &mut self,
        edge: Option<&str>,
        subject: &Term,
        field: &AstSymbolRef,
        object: &Term,
    ) -> Result<Solutions, KipError> {
        let (subject, left, left_var) = self.expand_endpoint(subject, &Solutions::unit()).await?;
        let (object, right, right_var) = self.expand_endpoint(object, &left).await?;
        let matched = self
            .match_structural_simple(edge, &subject, field, &object)
            .await?;
        let expanded = self.join(left, right)?;
        let mut result = self.join(matched, expanded)?;
        remove_internal(&mut result, left_var.iter().chain(right_var.iter()));
        Ok(result)
    }

    async fn match_structural_simple(
        &mut self,
        edge: Option<&str>,
        subject: &Term,
        field: &AstSymbolRef,
        object: &Term,
    ) -> Result<Solutions, KipError> {
        let name = match field {
            AstSymbolRef::Name(name) => name.clone(),
            AstSymbolRef::Param(param) => match self.param_ref(param)? {
                Json::String(text) => text,
                other => {
                    return Err(KipError::type_mismatch(format!(
                        "the parameter :{param} must carry a structural field symbol, got {other}"
                    )));
                }
            },
        };
        // Both planes are consulted, and a name resolves on each independently.
        // In the ordinary case exactly one answers, so this reads as one
        // lookup. When both do — a Profile that declared a field named
        // `source` — the pattern reports the edges of both rather than picking
        // a winner. Silently preferring either would be the failure this
        // routing exists to prevent, in one direction or the other.
        let core = core_structural_field(&name);
        let resolved = self
            .env
            .resolve_symbol(SymbolKind::StructuralField, &name, Intent::Read);
        // A name that answers on neither plane is the caller's mistake, and the
        // schema layer's message is the one that says what to do about it.
        let symbol = match resolved {
            Ok(symbol) => Some(symbol),
            Err(err) if core.is_none() => return Err(err),
            Err(_) => None,
        };

        let mut planes: Vec<StructuralPlane> = Vec::new();
        if let Some((kind, member)) = core {
            planes.push(StructuralPlane {
                field: name.clone(),
                member: member.to_string(),
                in_structural_map: false,
                // A Core field declares no order — the write path appends
                // rather than honoring `AT` — so it reports none, and a caller
                // is not invited to treat storage order as a position.
                ordered: false,
                holder: kind,
                exclusive: true,
            });
        }
        if let Some(symbol) = &symbol {
            // §17.4: an ordered field exposes each reference's current position
            // as `?edge.index`; an unordered one exposes no index at all, so
            // the member reads null there rather than reporting a position the
            // field does not have.
            let ordered = self
                .env
                .structural_field_def(symbol)
                .map(|def| def.ordered)
                .unwrap_or(false);
            planes.push(StructuralPlane {
                field: symbol.to_string(),
                member: symbol.to_string(),
                in_structural_map: true,
                ordered,
                // Every element carries the generic map, but only a Concept is
                // ever a Profile field's source in practice, and scanning one
                // kind is what keeps an unbound source from being a whole-Space
                // walk.
                holder: ElementKind::Concept,
                exclusive: false,
            });
        }

        let source = self.endpoint_slot(subject)?;
        let target = self.endpoint_slot(object)?;

        let mut vars: Vec<String> = Vec::new();
        if let Some(edge) = edge {
            vars.push(edge.to_string());
        }
        for slot in [&source, &target] {
            if let EndpointSlot::Bind(name) = slot
                && !vars.contains(name)
            {
                vars.push(name.clone());
            }
        }

        let mut rows = Vec::new();
        for plane in &planes {
            // Structural fields live on one element each, so the source side is
            // the only one an index narrows; an unbound source means scanning
            // the kind that could carry the field.
            let sources: Vec<ElementId> = match &source {
                EndpointSlot::Fixed(Endpoint::Local(id)) => {
                    if plane.exclusive && id.kind != plane.holder {
                        vec![]
                    } else {
                        vec![*id]
                    }
                }
                _ => self.active_of(plane.holder).await?,
            };
            self.charge(sources.len())?;

            for id in sources {
                let Some(element) = self.load(id).await? else {
                    continue;
                };
                if element.space() != self.space || !element.is_active() {
                    continue;
                }
                let rendered = self.view_of(id);
                // A Core field is a column of its own; a Profile one lives in
                // the generic map under its resolved symbol.
                let carried = if plane.in_structural_map {
                    rendered
                        .get("structural")
                        .and_then(|value| value.get(&plane.member))
                } else {
                    rendered.get(&plane.member)
                };
                // `generated_by` is single-cardinality and renders as one
                // reference rather than a list; a field with at most one edge
                // is still a field.
                let refs: Vec<Json> = match carried {
                    Some(Json::Array(items)) => items.clone(),
                    Some(value @ Json::Object(_)) => vec![value.clone()],
                    _ => continue,
                };
                'references: for (position, reference) in refs.iter().enumerate() {
                    let bound = endpoint_binding(reference);
                    if let EndpointSlot::Fixed(expected) = &target
                        && endpoint_binding(&expected.to_json()) != bound
                    {
                        continue;
                    }
                    let mut solution = vec![Binding::Null; vars.len()];
                    if let Some(edge) = edge {
                        // §43.7: the bound edge is *virtual* structural query
                        // state, explicitly "not necessarily a durable
                        // Cognitive Element" — so it binds as the value it is,
                        // describing the reference rather than standing in for
                        // a record Core does not keep.
                        let mut value = serde_json::json!({
                            "source": {"id": id.to_string()},
                            "field": plane.field.clone(),
                            "target": reference.clone(),
                        });
                        if plane.ordered {
                            value["index"] = Json::from(position);
                        }
                        let identity = super::binding::value_key(&serde_json::json!([
                            "structural",
                            value["source"],
                            crate::schema::lineage_of(&plane.field),
                            value["target"],
                            value.get("index")
                        ]));
                        if !set(
                            &vars,
                            &mut solution,
                            edge,
                            Binding::Virtual { identity, value },
                        ) {
                            continue 'references;
                        }
                    }
                    if let EndpointSlot::Bind(name) = &source
                        && !set(&vars, &mut solution, name, Binding::Element(id))
                    {
                        continue 'references;
                    }
                    if let EndpointSlot::Bind(name) = &target
                        && !set(&vars, &mut solution, name, bound)
                    {
                        continue 'references;
                    }
                    rows.push(solution);
                }
            }
        }
        if vars.is_empty() {
            // Both ends were fixed, so the pattern is a yes/no question.
            return Ok(if rows.is_empty() {
                Solutions::empty()
            } else {
                Solutions::unit()
            });
        }
        Ok(Solutions::table(vars, rows))
    }

    // -- slot classification ------------------------------------------------

    fn classify(&mut self, value: &MatchValue) -> Result<Slot, KipError> {
        Ok(match value {
            MatchValue::Variable(_) => Slot::Pattern,
            MatchValue::Param(name) => Slot::Value(self.param_ref(name)?),
            MatchValue::Literal(literal) => Slot::Value(Json::from(literal.clone())),
            MatchValue::Array(items) => {
                for item in items {
                    self.classify(item)?;
                }
                Slot::Pattern
            }
            MatchValue::Match(fields) => {
                for value in fields.values() {
                    self.classify(value)?;
                }
                Slot::Pattern
            }
            MatchValue::Proposition(_) => {
                return Err(KipError::unsupported_capability(
                    "a nested Proposition in a field matcher must be bound in a separate Proposition pattern",
                ));
            }
        })
    }

    /// Resolves a matcher value that names a schema symbol.
    fn matcher_text(&self, kind: ElementKind, key: &str, value: &Json) -> Result<String, KipError> {
        let Json::String(text) = value else {
            return Err(KipError::type_mismatch(format!(
                "`{key}` must be a string, got {value}"
            )));
        };
        if is_symbol_key(kind, key) {
            // A model writes `{type: "Person"}`; storage holds the exact
            // symbol, so the local name is resolved before it reaches the
            // index rather than compared as text.
            return Ok(self
                .env
                .resolve_symbol(SymbolKind::ConceptType, text, Intent::Read)?
                .to_string());
        }
        Ok(text.clone())
    }

    /// Whether every position of a tuple names something exactly (§46.3).
    ///
    /// This is what separates "the Proposition does not exist" from "the
    /// pattern did not match": only a fully grounded tuple asks about one
    /// Proposition, so only a fully grounded tuple can be answered with one
    /// belief about the Proposition that is missing.
    fn tuple_is_grounded(&mut self, triple: &PropositionTriple) -> Result<bool, KipError> {
        if [&triple.subject, &triple.object].iter().any(|term| {
            matches!(term, Term::Proposition(_))
                || matches!(term, Term::Match(matcher) if !identity_matcher(matcher))
        }) {
            return Ok(false);
        }
        let ends = [
            self.endpoint_slot(&triple.subject)?,
            self.endpoint_slot(&triple.object)?,
        ];
        if ends
            .iter()
            .any(|slot| matches!(slot, EndpointSlot::Bind(_)))
        {
            return Ok(false);
        }
        // A traversal path is never grounded in this sense: §46.1 refuses a
        // raw path under BELIEF precisely because projection must not
        // propagate belief along one.
        let PredTerm::Atom(atom) = &triple.predicate else {
            return Ok(false);
        };
        Ok(
            matches!(self.predicate_atom(atom)?, PredicateSlot::Fixed(symbols) if symbols.len() == 1),
        )
    }

    fn endpoint_slot(&mut self, term: &Term) -> Result<EndpointSlot, KipError> {
        Ok(match term {
            // Narrowing on what an earlier pattern pinned is the `known`
            // solution set's job, applied by `seeds`; a variable is always an
            // open slot at this point.
            Term::Variable(name) => EndpointSlot::Bind(name.clone()),
            Term::Param(name) => {
                let value = self.param_ref(name)?;
                EndpointSlot::Fixed(endpoint_of(&value)?)
            }
            Term::Literal(literal) => {
                EndpointSlot::Fixed(endpoint_of(&Json::from(literal.clone()))?)
            }
            // An inline matcher is an endpoint only when it *names* one; the
            // rest are searches, and `matcher_endpoint` says so rather than
            // matching everything (§8.1, §8.2).
            Term::Match(matcher) => EndpointSlot::Fixed(crate::term::matcher_endpoint(
                matcher,
                "a tuple endpoint",
                |name| self.param_ref(name),
            )?),
            // §43.2 blesses `(id: ...)` as a `term`, which is how a statement
            // about a statement names an existing Proposition. Refused rather
            // than ignored: an unconstrained endpoint would silently match
            // every tuple under the predicate.
            Term::Proposition(_) => {
                return Err(KipError::unsupported_capability(
                    "a nested Proposition in a tuple endpoint (§43.2) is not supported by this \
                     engine yet; bind the Proposition with its own pattern and pass the variable",
                ));
            }
        })
    }

    /// The walks a predicate slot describes, when any of them is quantified.
    ///
    /// `None` means every alternative is exactly one hop, which is the ordinary
    /// tuple pattern — one Proposition, matched by index.
    ///
    /// The quantifier binds to the atom it was written on (`predicate_path_atom
    /// = predicate_atom, [path_quantifier]`), so `"a"{1,3} | "b"` is *(1 to 3
    /// hops of `a`)* or *(1 hop of `b`)*. A walk that alternates predicates
    /// hop by hop is not something this grammar can express, and inventing it
    /// here would answer a question nobody asked.
    fn traversal_of(&mut self, predicate: &PredTerm) -> Result<Option<Vec<Walk>>, KipError> {
        let PredTerm::Path(atoms) = predicate else {
            return Ok(None);
        };
        if atoms
            .iter()
            .any(|atom| matches!(atom.predicate, PredAtom::Variable(_)))
        {
            return Err(KipError::invalid_syntax(
                "predicate variables cannot have quantifiers or alternatives",
            ));
        }
        if !atoms.iter().any(|atom| {
            atom.hops
                .is_some_and(|hops| hops.min != 1 || hops.max != Some(1))
        }) {
            return Ok(None);
        }

        let mut walks = Vec::with_capacity(atoms.len());
        for atom in atoms {
            let hops = atom.hops.unwrap_or(anda_kip::HopRange {
                min: 1,
                max: Some(1),
            });
            if let Some(max) = hops.max
                && max < hops.min
            {
                return Err(KipError::invalid_syntax(format!(
                    "a hop range must not count down: {{{},{max}}}",
                    hops.min
                )));
            }
            let PredicateSlot::Fixed(symbols) = self.predicate_atom(&atom.predicate)? else {
                return Err(KipError::unsupported_capability(
                    "a variable predicate inside a quantified path is not supported: the walk \
                     would have to try every predicate in the Space",
                ));
            };
            walks.push(Walk { symbols, hops });
        }
        Ok(Some(walks))
    }

    /// Walks the raw Proposition graph.
    ///
    /// Raw, and only raw: a path reports that the tuples exist, never that the
    /// chain is believed (§45). Belief does not compose along a path — two
    /// separately credible claims do not make their conclusion credible — so a
    /// traversal answers reachability and leaves the epistemics to `BELIEF`.
    async fn match_traversal(
        &mut self,
        subject: EndpointSlot,
        object: EndpointSlot,
        walks: &[Walk],
        known: &Solutions,
    ) -> Result<Solutions, KipError> {
        let subject = match subject {
            EndpointSlot::Fixed(endpoint) => EndpointSlot::Fixed(Endpoint::from_json(
                &self.canonical_endpoint(&endpoint.to_json()).await?,
            )?),
            other => other,
        };
        let object = match object {
            EndpointSlot::Fixed(endpoint) => EndpointSlot::Fixed(Endpoint::from_json(
                &self.canonical_endpoint(&endpoint.to_json()).await?,
            )?),
            other => other,
        };
        let zero_hop = walks.iter().any(|walk| walk.hops.min == 0);
        // Walking backwards from a fixed object costs the same as forwards
        // from a fixed subject, and either beats enumerating every subject in
        // the Space, so the direction follows whichever end is pinned — by the
        // pattern itself, or by an earlier pattern in the same block.
        let (starts, forward) = match (&subject, &object) {
            (EndpointSlot::Fixed(from), _) => (vec![from.clone()], true),
            (EndpointSlot::Bind(_), EndpointSlot::Fixed(to)) => (vec![to.clone()], false),
            (EndpointSlot::Bind(name), _)
                if known.binds(name) && !known.values_of(name).contains(&Binding::Null) =>
            {
                (seeds(known, name), true)
            }
            (_, EndpointSlot::Bind(name))
                if known.binds(name) && !known.values_of(name).contains(&Binding::Null) =>
            {
                (seeds(known, name), false)
            }
            _ => {
                if zero_hop {
                    let mut elements = Vec::new();
                    for kind in [
                        ElementKind::Concept,
                        ElementKind::Proposition,
                        ElementKind::Assertion,
                        ElementKind::Evidence,
                        ElementKind::Activity,
                    ] {
                        elements
                            .extend(self.active_of(kind).await?.into_iter().map(Endpoint::Local));
                    }
                    self.charge(elements.len())?;
                    (elements, true)
                } else {
                    let symbols: Vec<String> = walks
                        .iter()
                        .flat_map(|walk| walk.symbols.iter().cloned())
                        .collect();
                    (self.tuple_subjects(&symbols).await?, true)
                }
            }
        };

        let mut pairs: Vec<(Endpoint, Endpoint)> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for start in starts {
            if let Endpoint::Local(id) = &start {
                if self
                    .load(*id)
                    .await?
                    .is_none_or(|element| element.space() != self.space || !element.is_active())
                {
                    continue;
                }
            } else if forward {
                continue;
            }
            let start = Endpoint::from_json(&self.canonical_endpoint(&start.to_json()).await?)?;
            for walk in walks {
                let reached = self.walk_from(&start, walk, forward).await?;
                for endpoint in reached {
                    let pair = if forward {
                        (start.clone(), endpoint)
                    } else {
                        (endpoint, start.clone())
                    };
                    if seen.insert((pair.0.key(), pair.1.key())) {
                        pairs.push(pair);
                    }
                }
            }
        }

        // A fixed endpoint is a constraint, not a binding: keep only the pairs
        // that agree with it.
        if let EndpointSlot::Fixed(to) = &object {
            pairs.retain(|(_, reached)| reached == to);
        }
        if let EndpointSlot::Fixed(from) = &subject {
            pairs.retain(|(origin, _)| origin == from);
        }

        let mut vars: Vec<String> = Vec::new();
        for slot in [&subject, &object] {
            if let EndpointSlot::Bind(name) = slot
                && !vars.contains(name)
            {
                vars.push(name.clone());
            }
        }
        if vars.is_empty() {
            // Both ends pinned: the pattern is a yes/no question about
            // reachability, and `unit` is the yes.
            return Ok(if pairs.is_empty() {
                Solutions::empty()
            } else {
                Solutions::unit()
            });
        }

        let mut rows = Vec::new();
        'pairs: for (from, to) in pairs {
            let mut solution = vec![Binding::Null; vars.len()];
            if let EndpointSlot::Bind(name) = &subject
                && !set(&vars, &mut solution, name, endpoint_to_binding(&from))
            {
                continue 'pairs;
            }
            if let EndpointSlot::Bind(name) = &object
                && !set(&vars, &mut solution, name, endpoint_to_binding(&to))
            {
                continue 'pairs;
            }
            rows.push(solution);
        }
        Ok(Solutions::table(vars, rows))
    }

    /// Breadth-first walk from one endpoint, collecting what it reaches within
    /// the hop range.
    ///
    /// The visited set is what makes a cyclic graph terminate; the budget is
    /// what makes an acyclic but enormous one refuse rather than run forever.
    async fn walk_from(
        &mut self,
        start: &Endpoint,
        walk: &Walk,
        forward: bool,
    ) -> Result<Vec<Endpoint>, KipError> {
        let mut reached: Vec<Endpoint> = Vec::new();
        let mut reached_keys = std::collections::HashSet::new();
        if walk.hops.min == 0 && matches!(start, Endpoint::Local(_)) {
            reached_keys.insert(start.key());
            reached.push(start.clone());
        }
        let mut visited = std::collections::HashSet::new();
        visited.insert((start.key(), 0));
        let mut frontier: Vec<Endpoint> = vec![start.clone()];
        let mut depth = 0u32;
        while !frontier.is_empty() {
            if walk.hops.max.is_some_and(|max| depth >= max) {
                break;
            }
            depth = depth
                .checked_add(1)
                .ok_or_else(|| KipError::resource_exhausted("raw path hop limit exceeded"))?;
            let mut next: Vec<Endpoint> = Vec::new();
            for endpoint in &frontier {
                // Forward traversal ends at a Literal. Reverse traversal may
                // still follow an edge whose object is that Literal.
                if forward && !matches!(endpoint, Endpoint::Local(_)) {
                    continue;
                }
                for neighbour in self.neighbours(endpoint, &walk.symbols, forward).await? {
                    // Bounded paths retain depth, because reaching a node at
                    // one hop does not answer whether it is reachable at two.
                    // Unbounded paths need only distinguish depths below min;
                    // afterwards ordinary reachability makes cycles terminate.
                    let state_depth = if walk.hops.max.is_some() {
                        depth
                    } else {
                        depth.min(walk.hops.min)
                    };
                    if !visited.insert((neighbour.key(), state_depth)) {
                        continue;
                    }
                    if depth >= walk.hops.min && reached_keys.insert(neighbour.key()) {
                        reached.push(neighbour.clone());
                    }
                    next.push(neighbour);
                }
            }
            self.charge(next.len())?;
            frontier = next;
        }
        Ok(reached)
    }

    /// One hop out of (or into) an endpoint, along any of the predicates.
    async fn neighbours(
        &mut self,
        from: &Endpoint,
        symbols: &[String],
        forward: bool,
    ) -> Result<Vec<Endpoint>, KipError> {
        let (anchor, opposite) = if forward {
            ("subject_key", "object")
        } else {
            ("object_key", "subject")
        };
        // A walk anchors canonically too (§43.2): the hop out of B leaves
        // from every tuple recorded on a Concept merged into B.
        let anchor_keys = self.endpoint_keys(from).await?;
        let ids = self
            .candidates(
                ElementKind::Proposition,
                Some(Filter::And(vec![
                    Box::new(eq_field("space", Fv::Text(self.space.clone()))),
                    Box::new(eq_field(
                        "state",
                        Fv::Text(crate::store::rows::state::ACTIVE.into()),
                    )),
                    Box::new(key_filter(anchor, &anchor_keys)),
                    Box::new(predicate_filter(&self.env, symbols)),
                ])),
            )
            .await?;
        self.charge(ids.len())?;

        let historical = self.is_historical();
        let mut out = Vec::new();
        for id in ids {
            let Some(crate::store::Element::Proposition(row)) = self.load(id).await? else {
                continue;
            };
            if !self.readable_tuple(id) {
                continue;
            }
            if !predicate_matches(&self.env, &row.predicate_ref, symbols) {
                continue;
            }
            if historical {
                let matches_anchor = if forward {
                    anchor_keys.contains(&row.subject_key)
                } else {
                    anchor_keys.contains(&row.object_key)
                };
                if !matches_anchor || row.state != "active" {
                    continue;
                }
            }
            let value = if opposite == "object" {
                &row.object
            } else {
                &row.subject
            };
            let canonical = self.canonical_endpoint(value).await?;
            if let Ok(endpoint) = Endpoint::from_json(&canonical) {
                out.push(endpoint);
            }
        }
        Ok(out)
    }

    /// Every distinct subject of the given predicates, for a walk with both
    /// ends unbound.
    async fn tuple_subjects(&mut self, symbols: &[String]) -> Result<Vec<Endpoint>, KipError> {
        let ids = self
            .candidates(
                ElementKind::Proposition,
                Some(Filter::And(vec![
                    Box::new(eq_field("space", Fv::Text(self.space.clone()))),
                    Box::new(eq_field(
                        "state",
                        Fv::Text(crate::store::rows::state::ACTIVE.into()),
                    )),
                    Box::new(predicate_filter(&self.env, symbols)),
                ])),
            )
            .await?;
        self.charge(ids.len())?;

        let historical = self.is_historical();
        let mut subjects: Vec<Endpoint> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for id in ids {
            let Some(crate::store::Element::Proposition(row)) = self.load(id).await? else {
                continue;
            };
            if !self.readable_tuple(id) {
                continue;
            }
            if !predicate_matches(&self.env, &row.predicate_ref, symbols)
                || (historical && row.state != "active")
            {
                continue;
            }
            let canonical = self.canonical_endpoint(&row.subject).await?;
            if let Ok(endpoint) = Endpoint::from_json(&canonical)
                && seen.insert(endpoint.key())
            {
                subjects.push(endpoint);
            }
        }
        Ok(subjects)
    }

    fn predicate_slot(&mut self, predicate: &PredTerm) -> Result<PredicateSlot, KipError> {
        match predicate {
            PredTerm::Atom(atom) => self.predicate_atom(atom),
            PredTerm::Path(atoms) => {
                // Anything quantified beyond a single hop was already routed to
                // the traversal; what reaches here is one hop, spelled either
                // bare or as `{1}` / `{1,1}`.
                debug_assert!(
                    atoms.iter().all(|atom| atom
                        .hops
                        .is_none_or(|hops| hops.min == 1 && hops.max == Some(1))),
                    "a quantified path must be matched as a traversal"
                );
                // Every atom a single hop is an alternation: one hop, several
                // acceptable predicates.
                let mut symbols = Vec::new();
                for atom in atoms {
                    match self.predicate_atom(&atom.predicate)? {
                        PredicateSlot::Fixed(mut names) => symbols.append(&mut names),
                        PredicateSlot::Bind(_) => {
                            return Err(KipError::unsupported_capability(
                                "a variable inside a predicate alternation is not supported",
                            ));
                        }
                    }
                }
                Ok(PredicateSlot::Fixed(symbols))
            }
        }
    }

    fn predicate_atom(&mut self, atom: &PredAtom) -> Result<PredicateSlot, KipError> {
        Ok(match atom {
            PredAtom::Variable(name) => PredicateSlot::Bind(name.clone()),
            PredAtom::Literal(name) => PredicateSlot::Fixed(vec![
                self.env
                    .resolve_symbol(SymbolKind::PredicateType, name, Intent::Read)?
                    .to_string(),
            ]),
            PredAtom::Param(name) => match self.param_ref(name)? {
                Json::String(text) => PredicateSlot::Fixed(vec![
                    self.env
                        .resolve_symbol(SymbolKind::PredicateType, &text, Intent::Read)?
                        .to_string(),
                ]),
                other => {
                    return Err(KipError::type_mismatch(format!(
                        "the parameter :{name} must carry a predicate symbol, got {other}"
                    )));
                }
            },
        })
    }

    fn scalar_element(
        &mut self,
        scalar: &Scalar,
        kind: ElementKind,
    ) -> Result<ElementId, KipError> {
        let value = match scalar {
            Scalar::Literal(literal) => Json::from(literal.clone()),
            Scalar::Param(name) => self.param_ref(name)?,
        };
        match value {
            Json::String(text) => ElementId::parse_kind(&text, kind),
            other => Err(KipError::type_mismatch(format!(
                "expected an element id string, got {other}"
            ))),
        }
    }
}

/// A tuple endpoint, once resolved against what is already bound.
enum EndpointSlot {
    Fixed(Endpoint),
    Bind(String),
}

/// Whether a Proposition row satisfies a tuple pattern.
///
/// The predicate is always checked here, because the index was ranged over
/// its lineage rather than its exact symbol; the endpoints and the state only
/// at a past coordinate, where nothing could be pushed into the index.
fn tuple_matches(
    env: &crate::schema::SchemaEnvironment,
    row: &crate::store::rows::PropositionRow,
    subject_keys: Option<&[String]>,
    object_keys: Option<&[String]>,
    predicates: &PredicateSlot,
    historical: bool,
) -> bool {
    if let PredicateSlot::Fixed(symbols) = predicates
        && !predicate_matches(env, &row.predicate_ref, symbols)
    {
        return false;
    }
    if !historical {
        return true;
    }
    if row.state != "active" {
        return false;
    }
    if let Some(keys) = subject_keys
        && !keys.contains(&row.subject_key)
    {
        return false;
    }
    if let Some(keys) = object_keys
        && !keys.contains(&row.object_key)
    {
        return false;
    }
    true
}

/// An index filter over `predicate_ref` for every version of each lineage
/// (§20.14): the package is ranged over, and [`predicate_matches`] settles
/// the symbol afterwards.
///
/// A promoted draft symbol (§20.16) adds the draft package's range, so the
/// lineage a promotion joined is read as one.
fn predicate_filter(env: &crate::schema::SchemaEnvironment, symbols: &[String]) -> Filter {
    let mut ranges: Vec<Box<Filter>> = Vec::with_capacity(symbols.len());
    for symbol in symbols {
        let lineage = env.lineage_ranges(crate::schema::SymbolKind::PredicateType, symbol);
        if lineage.is_empty() {
            ranges.push(Box::new(eq_field(
                "predicate_ref",
                Fv::Text(symbol.clone()),
            )));
        }
        for (low, high) in lineage {
            ranges.push(Box::new(Filter::Field((
                "predicate_ref".to_string(),
                RangeQuery::Between(Fv::Text(low), Fv::Text(high)),
            ))));
        }
    }
    match ranges.len() {
        1 => *ranges.pop().expect("one range"),
        _ => Filter::Or(ranges),
    }
}

/// Whether a stored predicate belongs to one of the lineages a pattern named,
/// promotions included.
fn predicate_matches(
    env: &crate::schema::SchemaEnvironment,
    stored: &str,
    symbols: &[String],
) -> bool {
    symbols
        .iter()
        .any(|symbol| env.same_lineage(crate::schema::SymbolKind::PredicateType, stored, symbol))
}

/// One quantified alternative of a raw predicate path.
struct Walk {
    /// The predicate symbols this alternative traverses.
    symbols: Vec<String>,
    /// How many hops of them are acceptable.
    hops: anda_kip::HopRange,
}

/// The endpoints a variable is already bound to by the solutions so far.
///
/// Only elements: a Literal has no outgoing tuples, so seeding a walk with one
/// would add a start that can never move.
fn seeds(known: &Solutions, name: &str) -> Vec<Endpoint> {
    known
        .elements_of(name)
        .into_iter()
        .map(Endpoint::Local)
        .collect()
}

/// The binding one endpoint of a walk produces.
fn endpoint_to_binding(endpoint: &Endpoint) -> Binding {
    match endpoint {
        Endpoint::Local(id) => Binding::Element(*id),
        Endpoint::Literal(literal) => Binding::Literal(literal.value.clone()),
        other => Binding::Literal(other.to_json()),
    }
}

/// A predicate slot, once resolved.
enum PredicateSlot {
    Fixed(Vec<String>),
    Bind(String),
}

fn set(vars: &[String], row: &mut [Binding], name: &str, value: Binding) -> bool {
    let index = vars
        .iter()
        .position(|var| var == name)
        .expect("declared binding");
    if !matches!(row[index], Binding::Null) && row[index] != value {
        return false;
    }
    row[index] = value;
    true
}

fn endpoint_of(value: &Json) -> Result<Endpoint, KipError> {
    if let Json::String(text) = value
        && let Ok(id) = text.parse::<ElementId>()
    {
        return Ok(Endpoint::Local(id));
    }
    Endpoint::from_json(value)
}

/// The binding a stored endpoint produces.
fn endpoint_binding(value: &Json) -> Binding {
    match Endpoint::from_json(value) {
        Ok(Endpoint::Local(id)) => Binding::Element(id),
        Ok(Endpoint::Literal(literal)) => Binding::Literal(literal.value),
        _ => Binding::Literal(value.clone()),
    }
}

/// The binding a field read produces.
///
/// A field holding `{"id": "C-1"}` is a reference, so it binds as an element;
/// anything else is a value. Getting this wrong would make `IS_ELEMENT` lie
/// about half the graph.
fn value_binding(value: Json, is_reference: bool) -> Binding {
    if is_reference
        && let Some(id) = value
            .get("id")
            .and_then(Json::as_str)
            .and_then(|text| text.parse::<ElementId>().ok())
    {
        return Binding::Element(id);
    }
    // A bare id string is an element only where the field is declared to hold
    // a reference. Everywhere else `"C-1"` is exactly the string it looks
    // like, and guessing otherwise would make `IS_LITERAL` lie.
    if is_reference
        && let Json::String(text) = &value
        && let Ok(id) = text.parse::<ElementId>()
    {
        return Binding::Element(id);
    }
    Binding::Literal(value)
}

fn belief_binding(target: Json, value: Json) -> Binding {
    let identity =
        super::binding::value_key(&serde_json::json!(["belief", target, value.get("basis")]));
    Binding::Virtual { identity, value }
}

fn identity_matcher(matcher: &ObjectMatcher) -> bool {
    matcher.len() == 1
        && ["id", "canonical_id"].iter().any(|key| {
            matches!(
                matcher.get(*key),
                Some(MatchValue::Literal(_) | MatchValue::Param(_))
            )
        })
}

fn collect_matcher_vars(matcher: &ObjectMatcher, vars: &mut Vec<String>) {
    fn collect(value: &MatchValue, vars: &mut Vec<String>) {
        match value {
            MatchValue::Variable(name) => {
                if !vars.contains(name) {
                    vars.push(name.clone());
                }
            }
            MatchValue::Match(matcher) => collect_matcher_vars(matcher, vars),
            MatchValue::Array(items) => {
                for item in items {
                    collect(item, vars);
                }
            }
            _ => {}
        }
    }
    for value in matcher.values() {
        collect(value, vars);
    }
}

fn remove_internal<'a>(solutions: &mut Solutions, vars: impl Iterator<Item = &'a String>) {
    for name in vars {
        if let Some(index) = solutions.vars.iter().position(|var| var == name) {
            solutions.vars.remove(index);
            for row in &mut solutions.rows {
                row.remove(index);
            }
        }
    }
}

fn read_view_ref<'a>(view: &'a Json, path: &str) -> Option<&'a Json> {
    path.split('.')
        .try_fold(view, |value, step| value.get(step))
}

/// Whether a read value satisfies a matcher constraint.
fn matches_value(read: &Json, expected: &Json) -> bool {
    if super::binding::value_key(read) == super::binding::value_key(expected) {
        return true;
    }
    // A reference compares equal to the id it carries, so `{by: "C-1"}` works
    // without the caller having to spell the reference object.
    if let Some(id) = read.get("id") {
        return id == expected;
    }
    false
}

impl Context<'_> {
    /// Matches `?b BELIEF (...)` — an Epistemic Projection, not a read.
    ///
    /// The bound value is the projection output object, so `?b.status` and
    /// `?b.support.score` read out of it. It is virtual and read-only by
    /// construction: nothing here writes, and there is no element id to bind,
    /// because a belief is not a thing the Nexus stores (§48).
    pub async fn match_belief(
        &mut self,
        variable: &str,
        target: &anda_kip::BeliefTarget,
        solutions: &Solutions,
    ) -> Result<Solutions, KipError> {
        self.projected = true;
        let policy = self.policy.clone();
        let at = self.at.clone();

        // When the target is a variable an earlier pattern bound, the result
        // has to carry that variable too, or the join would cross-product every
        // projection against every Proposition.
        let (carried, propositions): (Option<String>, Vec<ElementId>) = match target {
            anda_kip::BeliefTarget::Id(scalar) => (
                None,
                vec![self.scalar_element(scalar, ElementKind::Proposition)?],
            ),
            anda_kip::BeliefTarget::Proposition(name) => {
                if !solutions.binds(name) {
                    return Err(KipError::projection_target_unbound(format!(
                        "?{name} is not bound to a Proposition; BELIEF projects about a \
                         Proposition, so one has to be identified before it can be projected"
                    )));
                }
                let ids: Vec<ElementId> = solutions
                    .elements_of(name)
                    .into_iter()
                    .filter(|id| id.kind == ElementKind::Proposition)
                    .collect();
                (Some(name.clone()), ids)
            }
            anda_kip::BeliefTarget::Tuple(triple) => {
                // A tuple names the Proposition structurally, and a Space keeps
                // one canonical Proposition per semantic tuple.
                //
                // Matched against the solutions the block has already produced,
                // exactly as a Proposition pattern would be (§46.3): a subject
                // an earlier pattern bound narrows the projection *and* stays
                // joined to it. Against a bare unit table instead, the tuple
                // would range over the whole Space and the resulting beliefs
                // would cross-product with every row the block had — an answer
                // about tuples the caller never named.
                let found = self
                    .match_tuple(Some(BELIEF_TARGET), triple, solutions)
                    .await?;
                // Only a *fully grounded* tuple earns the §46.4 answer. A
                // tuple with an unbound end asked about a family of slots, and
                // "no Proposition" there is an empty match, not one belief
                // about nothing. A grounded tuple binds no variable, so the
                // one answer applies to every row the block already had.
                if found.is_empty() && self.tuple_is_grounded(triple)? {
                    let belief = self.ungrounded_belief(&policy, &at)?;
                    let EndpointSlot::Fixed(subject) = self.endpoint_slot(&triple.subject)? else {
                        unreachable!()
                    };
                    let EndpointSlot::Fixed(object) = self.endpoint_slot(&triple.object)? else {
                        unreachable!()
                    };
                    let PredicateSlot::Fixed(predicates) =
                        self.predicate_slot(&triple.predicate)?
                    else {
                        unreachable!()
                    };
                    let subject = self.canonical_endpoint(&subject.to_json()).await?;
                    let object = self.canonical_endpoint(&object.to_json()).await?;
                    let target = serde_json::json!([
                        subject,
                        crate::schema::lineage_of(&predicates[0]),
                        object
                    ]);
                    return Ok(Solutions::column(
                        variable,
                        vec![belief_binding(target, belief.to_json())],
                    ));
                }
                return self.project_table(variable, found, &policy, &at).await;
            }
        };

        let mut vars = vec![variable.to_string()];
        if let Some(name) = &carried {
            vars.push(name.clone());
        }
        let mut rows = Vec::with_capacity(propositions.len());
        for id in propositions {
            let belief = self.project_belief(id, &policy, &at).await?;
            let mut row = vec![belief_binding(
                Json::String(id.to_string()),
                belief.to_json(),
            )];
            if carried.is_some() {
                row.push(Binding::Element(id));
            }
            rows.push(row);
        }
        Ok(Solutions::table(vars, rows))
    }

    /// Projects every row of a tuple match, keeping the bindings it carried.
    ///
    /// The tuple's own variables — its endpoints, and the handle on the
    /// Proposition — travel with the belief, so the caller's join re-attaches
    /// each projection to the row it came from instead of crossing all of them
    /// with all of it.
    async fn project_table(
        &mut self,
        variable: &str,
        found: Solutions,
        policy: &crate::projection::Policy,
        at: &str,
    ) -> Result<Solutions, KipError> {
        let header = found.header();
        let mut vars: Vec<String> = vec![variable.to_string()];
        vars.extend(
            found
                .vars
                .iter()
                .filter(|name| name.as_str() != BELIEF_TARGET)
                .cloned(),
        );

        let mut rows = Vec::with_capacity(found.rows.len());
        for row in &found.rows {
            let Some(id) = header
                .get(row, BELIEF_TARGET)
                .and_then(|binding| binding.element())
            else {
                continue;
            };
            let belief = self.project_belief(id, policy, at).await?;
            let mut out = vec![belief_binding(
                Json::String(id.to_string()),
                belief.to_json(),
            )];
            for (index, name) in found.vars.iter().enumerate() {
                if name.as_str() != BELIEF_TARGET {
                    out.push(row.get(index).cloned().unwrap_or(Binding::Null));
                }
            }
            rows.push(out);
        }
        Ok(Solutions::table(vars, rows))
    }

    /// Matches `?slot BELIEF SLOT (?s, "pred")` — the conflict set of one slot.
    ///
    /// Answers with every candidate value and their projected states, rather
    /// than with "the" value: a functional slot with two accepted candidates is
    /// contested, and collapsing it to one would be the engine picking a
    /// winner nobody authorized (§35).
    pub async fn match_belief_slot(
        &mut self,
        variable: &str,
        subject: &Term,
        predicate: &PredAtom,
        solutions: &Solutions,
    ) -> Result<Solutions, KipError> {
        self.projected = true;
        let policy = self.policy.clone();
        let at = self.at.clone();

        let predicate_ref = match self.predicate_atom(predicate)? {
            PredicateSlot::Fixed(mut symbols) if symbols.len() == 1 => symbols.remove(0),
            _ => {
                return Err(KipError::projection_target_unbounded(
                    "BELIEF SLOT needs one exact predicate; projection never walks a raw path",
                ));
            }
        };

        // §46.3 asks the subject to be *groundable or bound*, and §47.1 writes
        // it as `?subject` — so a variable an earlier pattern pinned is a
        // legal subject, not an unbounded projection. What is refused is a
        // variable nothing bound, which would range over every slot in the
        // Space.
        // Each subject travels with the binding it was read from, never with
        // one re-derived from its endpoint. A Literal subject round-trips
        // through `Endpoint::Literal` into the canonical `{value, datatype}`
        // object, which is not what the incoming row holds — so re-deriving it
        // would make the caller's join drop exactly the rows this branch exists
        // to answer.
        let (carried, subjects): (Option<String>, Vec<(Endpoint, Option<Binding>)>) =
            match self.endpoint_slot(subject)? {
                EndpointSlot::Fixed(endpoint) => (None, vec![(endpoint, None)]),
                EndpointSlot::Bind(name) => {
                    if !solutions.binds(&name) {
                        return Err(KipError::projection_target_unbounded(format!(
                            "?{name} is unbound, so this would project every slot in the Space; \
                             bind the subject with a pattern first"
                        )));
                    }
                    let mut subjects = Vec::new();
                    for binding in solutions.values_of(&name) {
                        // A Literal keys to a slot no Proposition can have —
                        // a subject is never a Literal (§8.4) — and that slot
                        // projects `insufficient`, which is the honest answer
                        // rather than a dropped row.
                        let endpoint = endpoint_of(&binding.to_json())?;
                        subjects.push((endpoint, Some(binding)));
                    }
                    (Some(name), subjects)
                }
            };

        // One projection per distinct subject, carrying the subject variable so
        // the caller's join re-attaches each slot to the row it came from.
        let mut vars = vec![variable.to_string()];
        if let Some(name) = &carried {
            vars.push(name.clone());
        }
        let mut rows = Vec::with_capacity(subjects.len());
        for (endpoint, binding) in subjects {
            let slot = self
                .project_slot(&endpoint, &predicate_ref, &policy, &at)
                .await?;
            let rendered = crate::projection::slot_to_json(&endpoint, &predicate_ref, &slot);
            let target = serde_json::json!([
                "slot",
                endpoint.to_json(),
                crate::schema::lineage_of(&predicate_ref)
            ]);
            let mut row = vec![belief_binding(target, rendered)];
            if let Some(binding) = binding {
                row.push(binding);
            }
            rows.push(row);
        }
        Ok(Solutions::table(vars, rows))
    }
}

/// The Core structural fields (§8.2), by the kind that owns each and the view
/// member that carries it.
///
/// These are the fields the protocol defines rather than a Profile: they live
/// in typed columns, KML routes them apart from the generic `structural` map,
/// and `STRUCTURAL` reaches them by the same plain names the write path uses.
const CORE_STRUCTURAL_FIELDS: &[(&str, ElementKind, &str)] = &[
    ("evidence", ElementKind::Assertion, "evidence"),
    ("context", ElementKind::Assertion, "context_refs"),
    ("source", ElementKind::Evidence, "source"),
    ("generated_by", ElementKind::Evidence, "generated_by"),
    ("inputs", ElementKind::Activity, "inputs"),
    ("outputs", ElementKind::Activity, "outputs"),
    (
        "associated_actors",
        ElementKind::Activity,
        "associated_actors",
    ),
];

/// Which kind owns a Core structural field, and where its view keeps it.
fn core_structural_field(name: &str) -> Option<(ElementKind, &'static str)> {
    CORE_STRUCTURAL_FIELDS
        .iter()
        .find(|(field, _, _)| *field == name)
        .map(|(_, kind, member)| (*kind, *member))
}

/// One structural plane a `STRUCTURAL` pattern reads, and how to read it.
struct StructuralPlane {
    /// What `?edge.field` reports: a plain Core name or a full symbol.
    field: String,
    /// Where the rendered view keeps it.
    member: String,
    /// Whether that member sits inside the generic `structural` map.
    in_structural_map: bool,
    /// Whether §17.4 gives this field a declared position.
    ordered: bool,
    /// The kind that can carry it.
    ///
    /// Two different uses, and they are not the same question. For an unbound
    /// source it is the kind to scan — Profile fields are scanned over Concepts
    /// because that is where they are declared in practice, and scanning one
    /// kind is what keeps an unbound source from walking the whole Space. For a
    /// *bound* source it filters only when `exclusive`: a Core field lives in a
    /// column its owning kind alone has, but every element carries the generic
    /// `structural` map, so an Assertion with a Profile field is a real answer.
    holder: ElementKind,
    /// Whether `holder` is the only kind that can carry this field at all.
    exclusive: bool,
}
