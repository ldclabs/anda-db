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
use crate::store::eq_field;
use crate::term::Endpoint;
use crate::view;

/// One matcher entry, once its value has been classified.
enum Slot {
    /// A concrete value the pattern constrains.
    Value(Json),
    /// A variable the pattern binds.
    Bind(String),
}

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
fn view_key(kind: ElementKind, key: &str) -> String {
    match (kind, key) {
        (ElementKind::Concept, "type") => "schema_ref".to_string(),
        (ElementKind::Evidence, "class") => "evidence_class".to_string(),
        (ElementKind::Activity, "class") => "activity_class".to_string(),
        (ElementKind::Assertion, "proposition") => "proposition_id".to_string(),
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
            | (ElementKind::Assertion, "proposition_id")
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
    /// Matches a typed element pattern: `?c CONCEPT {...}` and its siblings.
    pub async fn match_element(
        &mut self,
        kind: ElementKind,
        variable: &str,
        matcher: &ObjectMatcher,
    ) -> Result<Solutions, KipError> {
        let mut filters = vec![eq_field("space", Fv::Text(self.space.clone()))];
        let mut post: Vec<(String, Slot)> = Vec::new();
        let mut constrains_state = false;
        let mut by_id: Option<ElementId> = None;

        let historical = self.is_historical();
        for (key, value) in matcher {
            let slot = self.classify(value)?;
            if key == "state" {
                constrains_state = true;
            }
            // At a past coordinate the indexes describe the present, so every
            // constraint is decided against the historical element instead —
            // after the same normalization the index path applies, or a local
            // type name would be compared against the exact symbol it
            // resolves to and never match.
            if historical && !matches!(column_of(kind, key), Some("__id")) {
                let slot = match slot {
                    Slot::Value(value) => {
                        Slot::Value(Json::String(self.matcher_text(kind, key, &value)?))
                    }
                    bind => bind,
                };
                post.push((key.clone(), slot));
                continue;
            }
            match (&slot, column_of(kind, key)) {
                (Slot::Value(value), Some("__id")) => {
                    let Json::String(text) = value else {
                        return Err(KipError::type_mismatch("`id` must be an element id string"));
                    };
                    by_id = Some(ElementId::parse_kind(text, kind)?);
                }
                (Slot::Value(value), Some(column)) => {
                    let text = self.matcher_text(kind, key, value)?;
                    filters.push(eq_field(column, Fv::Text(text)));
                }
                _ => post.push((key.clone(), slot)),
            }
        }
        if !constrains_state {
            filters.push(eq_field("state", Fv::Text("active".to_string())));
        }

        let ids: Vec<ElementId> = match by_id {
            // Naming an id is the narrowest possible pattern, so it skips the
            // index entirely — but the remaining constraints still apply, or
            // `{id: "C-1", state: "archived"}` would match an active element.
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
        for (_, slot) in &post {
            if let Slot::Bind(name) = slot
                && !vars.contains(name)
            {
                vars.push(name.clone());
            }
        }

        let mut rows = Vec::new();
        'candidates: for id in ids {
            let Some(element) = self.load(id).await? else {
                continue;
            };
            if by_id.is_some() || historical {
                // The id path bypassed the filters, so re-check the ones that
                // would have been applied.
                if element.space() != self.space {
                    continue;
                }
                if !constrains_state && !element.is_active() {
                    continue;
                }
            }
            let rendered = view::render(&element);
            let mut row = vec![Binding::Element(id)];
            row.resize(vars.len(), Binding::Null);

            for (key, slot) in &post {
                let read = read_view(&rendered, &view_key(kind, key));
                match slot {
                    Slot::Value(expected) => {
                        if !matches_value(&read, expected) {
                            continue 'candidates;
                        }
                    }
                    Slot::Bind(name) => {
                        let index = vars.iter().position(|v| v == name).expect("declared above");
                        row[index] = value_binding(read, is_reference_key(kind, key));
                    }
                }
            }
            rows.push(row);
        }
        Ok(Solutions::table(vars, rows))
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

    async fn match_tuple(
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

        let mut filters = vec![
            eq_field("space", Fv::Text(self.space.clone())),
            eq_field("state", Fv::Text("active".to_string())),
        ];
        if let EndpointSlot::Fixed(endpoint) = &subject {
            filters.push(eq_field("subject_key", Fv::Text(endpoint.key())));
        }
        if let EndpointSlot::Fixed(endpoint) = &object {
            filters.push(eq_field("object_key", Fv::Text(endpoint.key())));
        }
        if let PredicateSlot::Fixed(symbols) = &predicates {
            filters.push(Filter::Field((
                "predicate_ref".to_string(),
                RangeQuery::Include(symbols.iter().map(|s| Fv::Text(s.clone())).collect()),
            )));
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
        for id in ids {
            let Some(crate::store::Element::Proposition(row)) = self.load(id).await? else {
                continue;
            };
            // At a coordinate the filters could not be pushed down, so the
            // tuple is matched here against the historical row.
            if historical && !tuple_matches(&row, &subject, &object, &predicates) {
                continue;
            }
            let mut solution = vec![Binding::Null; vars.len()];
            if let Some(var) = variable {
                set(&vars, &mut solution, var, Binding::Element(id));
            }
            if let EndpointSlot::Bind(name) = &subject {
                set(&vars, &mut solution, name, endpoint_binding(&row.subject));
            }
            if let EndpointSlot::Bind(name) = &object {
                set(&vars, &mut solution, name, endpoint_binding(&row.object));
            }
            if let PredicateSlot::Bind(name) = &predicates {
                set(
                    &vars,
                    &mut solution,
                    name,
                    Binding::Symbol(row.predicate_ref.clone()),
                );
            }
            rows.push(solution);
        }
        Ok(Solutions::table(vars, rows))
    }

    /// Matches `?edge STRUCTURAL (?src, "field", ?dst)` — record topology.
    ///
    /// A structural reference is never a semantic Proposition (§17.3): this
    /// pattern reports how records are assembled, and a claim *about* that
    /// relation would be a separate Proposition plus Assertion.
    pub async fn match_structural(
        &mut self,
        subject: &Term,
        field: &AstSymbolRef,
        object: &Term,
    ) -> Result<Solutions, KipError> {
        let name = match field {
            AstSymbolRef::Name(name) => name.clone(),
            AstSymbolRef::Param(param) => match self.param(param)? {
                Json::String(text) => text,
                other => {
                    return Err(KipError::type_mismatch(format!(
                        "the parameter :{param} must carry a structural field symbol, got {other}"
                    )));
                }
            },
        };
        let symbol = self
            .env
            .resolve_symbol(SymbolKind::StructuralField, &name, Intent::Read)?;

        let source = self.endpoint_slot(subject)?;
        let target = self.endpoint_slot(object)?;

        // Structural fields live in one map per element, so the source side is
        // the only one an index narrows; an unbound source means scanning the
        // Space's Concepts.
        let sources: Vec<ElementId> = match &source {
            EndpointSlot::Fixed(Endpoint::Local(id)) => vec![*id],
            _ => self.active_concepts().await?,
        };
        self.charge(sources.len())?;

        let mut vars: Vec<String> = Vec::new();
        for slot in [&source, &target] {
            if let EndpointSlot::Bind(name) = slot
                && !vars.contains(name)
            {
                vars.push(name.clone());
            }
        }

        let mut rows = Vec::new();
        for id in sources {
            let Some(element) = self.load(id).await? else {
                continue;
            };
            if element.space() != self.space || !element.is_active() {
                continue;
            }
            let rendered = view::render(&element);
            let Some(refs) = rendered
                .get("structural")
                .and_then(|value| value.get(symbol.to_string()))
                .and_then(Json::as_array)
            else {
                continue;
            };
            for reference in refs {
                let bound = endpoint_binding(reference);
                if let EndpointSlot::Fixed(expected) = &target
                    && endpoint_binding(&expected.to_json()) != bound
                {
                    continue;
                }
                let mut solution = vec![Binding::Null; vars.len()];
                if let EndpointSlot::Bind(name) = &source {
                    set(&vars, &mut solution, name, Binding::Element(id));
                }
                if let EndpointSlot::Bind(name) = &target {
                    set(&vars, &mut solution, name, bound);
                }
                rows.push(solution);
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
            MatchValue::Variable(name) => Slot::Bind(name.clone()),
            MatchValue::Param(name) => Slot::Value(self.param(name)?),
            MatchValue::Literal(literal) => Slot::Value(Json::from(literal.clone())),
            MatchValue::Array(items) => {
                let mut values = Vec::new();
                for item in items {
                    match self.classify(item)? {
                        Slot::Value(value) => values.push(value),
                        Slot::Bind(_) => {
                            return Err(KipError::unsupported_capability(
                                "a variable inside a matcher array is not supported",
                            ));
                        }
                    }
                }
                Slot::Value(Json::Array(values))
            }
            MatchValue::Match(_) | MatchValue::Proposition(_) => {
                return Err(KipError::unsupported_capability(
                    "a nested matcher inside a field matcher is not supported by this engine yet",
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

    fn endpoint_slot(&mut self, term: &Term) -> Result<EndpointSlot, KipError> {
        Ok(match term {
            Term::Variable(name) => match self.bound_endpoint(name) {
                Some(endpoint) => EndpointSlot::Fixed(endpoint),
                None => EndpointSlot::Bind(name.clone()),
            },
            Term::Param(name) => {
                let value = self.param(name)?;
                EndpointSlot::Fixed(endpoint_of(&value)?)
            }
            Term::Literal(literal) => {
                EndpointSlot::Fixed(endpoint_of(&Json::from(literal.clone()))?)
            }
            Term::Match(_) | Term::Proposition(_) => {
                return Err(KipError::unsupported_capability(
                    "an inline matcher or a nested Proposition in a tuple endpoint is not \
                     supported by this engine yet",
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
        let zero_hop = walks.iter().any(|walk| walk.hops.min == 0);
        // Walking backwards from a fixed object costs the same as forwards
        // from a fixed subject, and either beats enumerating every subject in
        // the Space, so the direction follows whichever end is pinned — by the
        // pattern itself, or by an earlier pattern in the same block.
        let (starts, forward) = match (&subject, &object) {
            (EndpointSlot::Fixed(from), _) => (vec![from.clone()], true),
            (EndpointSlot::Bind(_), EndpointSlot::Fixed(to)) => (vec![to.clone()], false),
            (EndpointSlot::Bind(name), _) if known.binds(name) => (seeds(known, name), true),
            (_, EndpointSlot::Bind(name)) if known.binds(name) => (seeds(known, name), false),
            _ => {
                if zero_hop {
                    // Every element in the Space matches itself at zero hops.
                    return Err(KipError::resource_exhausted(
                        "a path whose minimum is 0 hops matches every element against itself; \
                         bind one endpoint before asking for it",
                    ));
                }
                let symbols: Vec<String> = walks
                    .iter()
                    .flat_map(|walk| walk.symbols.iter().cloned())
                    .collect();
                (self.tuple_subjects(&symbols).await?, true)
            }
        };

        let mut pairs: Vec<(Endpoint, Endpoint)> = Vec::new();
        for start in starts {
            for walk in walks {
                let reached = self.walk_from(&start, walk, forward).await?;
                for endpoint in reached {
                    let pair = if forward {
                        (start.clone(), endpoint)
                    } else {
                        (endpoint, start.clone())
                    };
                    if !pairs.contains(&pair) {
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
        for (from, to) in pairs {
            let mut solution = vec![Binding::Null; vars.len()];
            if let EndpointSlot::Bind(name) = &subject {
                set(&vars, &mut solution, name, endpoint_to_binding(&from));
            }
            if let EndpointSlot::Bind(name) = &object {
                set(&vars, &mut solution, name, endpoint_to_binding(&to));
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
        if walk.hops.min == 0 {
            reached.push(start.clone());
        }
        let mut visited: Vec<Endpoint> = vec![start.clone()];
        let mut frontier: Vec<Endpoint> = vec![start.clone()];

        let mut depth = 0u32;
        while !frontier.is_empty() {
            depth += 1;
            if walk.hops.max.is_some_and(|max| depth > max) {
                break;
            }
            let mut next: Vec<Endpoint> = Vec::new();
            for endpoint in &frontier {
                // A literal has no outgoing tuples: `"+08:00"` is a value, not
                // a place the walk can continue from.
                let Endpoint::Local(_) = endpoint else {
                    continue;
                };
                for neighbour in self.neighbours(endpoint, &walk.symbols, forward).await? {
                    if visited.contains(&neighbour) {
                        continue;
                    }
                    visited.push(neighbour.clone());
                    next.push(neighbour.clone());
                    if depth >= walk.hops.min {
                        reached.push(neighbour);
                    }
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
        let anchor_key = from.key();
        let ids = self
            .candidates(
                ElementKind::Proposition,
                Some(Filter::And(vec![
                    Box::new(eq_field("space", Fv::Text(self.space.clone()))),
                    Box::new(eq_field("state", Fv::Text("active".to_string()))),
                    Box::new(eq_field(anchor, Fv::Text(anchor_key.clone()))),
                    Box::new(Filter::Field((
                        "predicate_ref".to_string(),
                        RangeQuery::Include(symbols.iter().map(|s| Fv::Text(s.clone())).collect()),
                    ))),
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
            if historical {
                let matches_anchor = if forward {
                    row.subject_key == anchor_key
                } else {
                    row.object_key == anchor_key
                };
                if !matches_anchor || row.state != "active" || !symbols.contains(&row.predicate_ref)
                {
                    continue;
                }
            }
            let value = if opposite == "object" {
                &row.object
            } else {
                &row.subject
            };
            if let Ok(endpoint) = Endpoint::from_json(value) {
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
                    Box::new(eq_field("state", Fv::Text("active".to_string()))),
                    Box::new(Filter::Field((
                        "predicate_ref".to_string(),
                        RangeQuery::Include(symbols.iter().map(|s| Fv::Text(s.clone())).collect()),
                    ))),
                ])),
            )
            .await?;
        self.charge(ids.len())?;

        let historical = self.is_historical();
        let mut subjects: Vec<Endpoint> = Vec::new();
        for id in ids {
            let Some(crate::store::Element::Proposition(row)) = self.load(id).await? else {
                continue;
            };
            if historical && (row.state != "active" || !symbols.contains(&row.predicate_ref)) {
                continue;
            }
            if let Ok(endpoint) = Endpoint::from_json(&row.subject)
                && !subjects.contains(&endpoint)
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
            PredAtom::Param(name) => match self.param(name)? {
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
            Scalar::Param(name) => self.param(name)?,
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
/// Only the historical path needs this: a present-day read pushes the same
/// three constraints into the index.
fn tuple_matches(
    row: &crate::store::rows::PropositionRow,
    subject: &EndpointSlot,
    object: &EndpointSlot,
    predicates: &PredicateSlot,
) -> bool {
    if row.state != "active" {
        return false;
    }
    if let EndpointSlot::Fixed(endpoint) = subject
        && row.subject_key != endpoint.key()
    {
        return false;
    }
    if let EndpointSlot::Fixed(endpoint) = object
        && row.object_key != endpoint.key()
    {
        return false;
    }
    if let PredicateSlot::Fixed(symbols) = predicates
        && !symbols.contains(&row.predicate_ref)
    {
        return false;
    }
    true
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

fn set(vars: &[String], row: &mut [Binding], name: &str, value: Binding) {
    if let Some(index) = vars.iter().position(|v| v == name) {
        row[index] = value;
    }
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
    if let Some(id) = value
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

fn read_view(view: &Json, path: &str) -> Json {
    let mut cursor = view;
    for step in path.split('.') {
        cursor = match cursor.get(step) {
            Some(value) => value,
            None => return Json::Null,
        };
    }
    cursor.clone()
}

/// Whether a read value satisfies a matcher constraint.
fn matches_value(read: &Json, expected: &Json) -> bool {
    if read == expected {
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
                let found = self
                    .match_tuple(Some("__belief_target"), triple, &Solutions::unit())
                    .await?;
                (None, found.elements_of("__belief_target"))
            }
        };

        let mut vars = vec![variable.to_string()];
        if let Some(name) = &carried {
            vars.push(name.clone());
        }
        let mut rows = Vec::with_capacity(propositions.len());
        for id in propositions {
            let belief = self.project_belief(id, &policy, &at).await?;
            let mut row = vec![Binding::Literal(belief.to_json())];
            if carried.is_some() {
                row.push(Binding::Element(id));
            }
            rows.push(row);
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
    ) -> Result<Solutions, KipError> {
        self.projected = true;
        let policy = self.policy.clone();
        let at = self.at.clone();

        let subject_key = match self.endpoint_slot(subject)? {
            EndpointSlot::Fixed(endpoint) => endpoint.key(),
            EndpointSlot::Bind(name) => {
                return Err(KipError::projection_target_unbounded(format!(
                    "?{name} is unbound, so this would project every slot in the Space; identify \
                     the subject first"
                )));
            }
        };
        let predicate_ref = match self.predicate_atom(predicate)? {
            PredicateSlot::Fixed(mut symbols) if symbols.len() == 1 => symbols.remove(0),
            _ => {
                return Err(KipError::projection_target_unbounded(
                    "BELIEF SLOT needs one exact predicate; projection never walks a raw path",
                ));
            }
        };

        let beliefs = self
            .project_slot(&subject_key, &predicate_ref, &policy, &at)
            .await?;
        let rendered = crate::projection::slot_to_json(&subject_key, &predicate_ref, &beliefs);
        Ok(Solutions::table(
            vec![variable.to_string()],
            vec![vec![Binding::Literal(rendered)]],
        ))
    }
}
