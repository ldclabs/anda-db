//! # Executing KQL
//!
//! A query is a `WHERE` block joined into one set of solutions, then projected.
//!
//! ## What a KQL read is
//!
//! The raw Core view (Spec §53.1). `?p PROPOSITION (...)` reports that a tuple
//! exists; `?a ASSERTION {...}` reports that somebody claimed something. What
//! is *currently believed* is a different question, answered by `BELIEF`, which
//! projects rather than reads — and which this engine does not implement yet.
//! It says so rather than answering the raw question instead, because "Alice
//! asserted X" and "X is believed" are exactly the two things KIP 2.0 exists to
//! keep apart.
//!
//! ## Bounded by construction
//!
//! Two patterns with no shared variable cross-product, so a query can ask for
//! more rows than exist elements. Every candidate a pattern loads is charged
//! against one budget, and exhausting it is a
//! [`ResourceExhausted`](anda_kip::KipErrorCode::ResourceExhausted) — an
//! explicit refusal rather than an engine that stops responding.

pub mod binding;
pub mod filter;
pub mod matching;
pub mod project;

use anda_db_schema::Fv;
use anda_kip::{
    ElementKind, Json, KipError, KqlQuery, Map, Operation, Request, Response, ResponseContext,
    ResultContext, Scalar, WhereClause,
};
use std::collections::BTreeMap;

use crate::error::db_error;
use crate::id::ElementId;
use crate::schema::SchemaEnvironment;
use crate::store::{Element, Store, eq_field};
use binding::Solutions;
use project::Projected;

/// How many candidate elements one query may load.
///
/// A cap rather than a timeout: a timeout makes the same query succeed or fail
/// depending on machine load, which is a worse contract than a limit a caller
/// can reason about.
pub const MAX_CANDIDATES: usize = 100_000;

/// The state one query execution carries.
pub struct Context<'a> {
    /// The storage layer.
    pub store: &'a Store,
    /// The Space this query reads.
    pub space: String,
    /// The Schema Environment local names resolve against.
    pub env: SchemaEnvironment,
    /// Request-level parameters.
    pub request: Option<&'a Map<String, Json>>,
    /// Operation-level parameters, which shadow request-level ones.
    pub operation: Option<&'a Map<String, Json>>,
    /// Elements loaded so far, so one query reads each row once.
    loaded: BTreeMap<ElementId, Option<Element>>,
    views: BTreeMap<ElementId, Json>,
    /// Variables already bound, so a later pattern can narrow on them.
    pub bound: BTreeMap<String, crate::term::Endpoint>,
    /// The policy `BELIEF` projects under.
    pub policy: crate::projection::Policy,
    /// The world time a projection is evaluated at.
    pub at: String,
    /// Whether any clause actually projected, so the answer can report the
    /// policy it ran under — and stay silent about one it never used.
    pub projected: bool,
    /// The past coordinate this read is bound to, when it is bound to one.
    ///
    /// `None` means now. Every read in this context answers at the same
    /// coordinate: a query whose patterns disagreed about *when* they were
    /// reading would join two different Brains together.
    pub as_of: Option<u64>,
    budget: usize,
}

impl<'a> Context<'a> {
    /// Opens a query context.
    pub async fn open(
        store: &'a Store,
        space: &str,
        request: Option<&'a Map<String, Json>>,
        operation: Option<&'a Map<String, Json>>,
    ) -> Result<Self, KipError> {
        Ok(Self {
            env: store.schema_environment(space).await?,
            store,
            space: space.to_string(),
            request,
            operation,
            loaded: BTreeMap::new(),
            views: BTreeMap::new(),
            bound: BTreeMap::new(),
            policy: crate::projection::Policy::baseline(),
            at: crate::time::now(),
            projected: false,
            as_of: None,
            budget: MAX_CANDIDATES,
        })
    }

    /// Looks up a `:parameter`.
    pub fn param(&self, name: &str) -> Result<Json, KipError> {
        self.param_ref(name)
    }

    /// Looks up a `:parameter` without needing a mutable borrow.
    pub fn param_ref(&self, name: &str) -> Result<Json, KipError> {
        self.operation
            .and_then(|map| map.get(name))
            .or_else(|| self.request.and_then(|map| map.get(name)))
            .cloned()
            .ok_or_else(|| {
                KipError::invalid_request_envelope(format!(
                    "the query uses the parameter :{name}, which the request does not bind"
                ))
            })
    }

    /// Loads an element once per query, caching both the row and its view.
    ///
    /// A bound read loads the version that was current at its coordinate, so
    /// every dot path, filter and sort key downstream reads the same past.
    pub async fn load(&mut self, id: ElementId) -> Result<Option<Element>, KipError> {
        if let Some(cached) = self.loaded.get(&id) {
            return Ok(cached.clone());
        }
        let element = match self.as_of {
            Some(seq) => self.store.element_at(&self.space, id, seq).await?,
            None => self.store.get_element(id).await.ok(),
        };
        if let Some(element) = &element {
            self.views.insert(id, crate::view::render(element));
        }
        self.loaded.insert(id, element.clone());
        Ok(element)
    }

    /// The rendered view of an already-loaded element.
    pub fn cached_view(&self, id: ElementId) -> Option<Json> {
        self.views.get(&id).cloned()
    }

    /// Loads every element a solution set mentions.
    ///
    /// A pattern binds an endpoint it never loaded — `(?person, "prefers",
    /// ?thing)` learns both ids from the tuple row without reading either
    /// element — and a later dot path, filter or sort key needs the view. This
    /// is where that debt is paid, once, before anything reads a field.
    pub async fn warm(&mut self, solutions: &Solutions) -> Result<(), KipError> {
        let ids: Vec<ElementId> = solutions
            .rows
            .iter()
            .flat_map(|row| row.iter())
            .filter_map(binding::Binding::element)
            .filter(|id| !self.views.contains_key(id))
            .collect();
        self.charge(ids.len())?;
        for id in ids {
            self.load(id).await?;
        }
        Ok(())
    }

    /// Charges candidates against the query budget.
    pub fn charge(&mut self, count: usize) -> Result<(), KipError> {
        self.budget = self.budget.checked_sub(count).ok_or_else(|| {
            KipError::resource_exhausted(format!(
                "this query would examine more than {MAX_CANDIDATES} elements; narrow it with a \
                 more selective pattern or a FILTER"
            ))
        })?;
        Ok(())
    }

    /// An endpoint a variable is already fixed to, when one is.
    pub fn bound_endpoint(&self, name: &str) -> Option<crate::term::Endpoint> {
        self.bound.get(name).cloned()
    }

    /// Every active Concept in the Space, for patterns no index narrows.
    pub async fn active_concepts(&mut self) -> Result<Vec<ElementId>, KipError> {
        if self.as_of.is_some() {
            return Ok(self
                .candidates(ElementKind::Concept, None)
                .await?
                .into_iter()
                .collect());
        }
        let ids = self
            .store
            .concepts()
            .query_all_ids(anda_db::query::Filter::And(vec![
                Box::new(eq_field("space", Fv::Text(self.space.clone()))),
                Box::new(eq_field("state", Fv::Text("active".to_string()))),
            ]))
            .await
            .map_err(db_error)?;
        Ok(ids
            .into_iter()
            .map(|seq| ElementId::new(ElementKind::Concept, seq))
            .collect())
    }

    /// The candidate elements one pattern starts from.
    ///
    /// Now: the index narrows, and `filters` is what it narrows by. At a past
    /// coordinate: the indexes describe the present and say nothing about what
    /// was there then, so the version log is reconstructed instead and every
    /// constraint is re-checked against the loaded element. Same answers,
    /// different cost — which is why it is charged to the same budget.
    pub async fn candidates(
        &mut self,
        kind: ElementKind,
        filters: Option<anda_db::query::Filter>,
    ) -> Result<Vec<ElementId>, KipError> {
        if let Some(seq) = self.as_of {
            let elements = self.store.elements_at(&self.space, kind, seq).await?;
            self.charge(elements.len())?;
            let mut ids = Vec::with_capacity(elements.len());
            for element in elements {
                let id = element.id();
                // Seed the cache: the historical row was just read, and
                // re-reading it through `load` would answer from the present.
                self.views.insert(id, crate::view::render(&element));
                self.loaded.insert(id, Some(element));
                ids.push(id);
            }
            return Ok(ids);
        }
        let ids = match filters {
            Some(filters) => self
                .store
                .elements(kind)
                .query_all_ids(filters)
                .await
                .map_err(db_error)?,
            None => self
                .store
                .elements(kind)
                .query_all_ids(anda_db::query::Filter::Field((
                    "space".to_string(),
                    anda_db::query::RangeQuery::Eq(Fv::Text(self.space.clone())),
                )))
                .await
                .map_err(db_error)?,
        };
        Ok(ids
            .into_iter()
            .map(|seq| ElementId::new(kind, seq))
            .collect())
    }

    /// Whether this read is bound to a past coordinate.
    pub fn is_historical(&self) -> bool {
        self.as_of.is_some()
    }

    /// Binds this read to a coordinate, from `AS OF` or from the envelope.
    ///
    /// Both may not disagree: a request that pinned one coordinate and a
    /// command that named another would leave the answer's own `snapshot_seq`
    /// unable to say which one it means.
    pub async fn bind_read(
        &mut self,
        as_of: Option<&anda_kip::AsOf>,
        request: &Request,
    ) -> Result<(), KipError> {
        let from_token = match request
            .read
            .as_ref()
            .and_then(|read| read.snapshot_token.as_ref())
        {
            Some(token) => {
                Some(crate::store::history::Coordinate::from_token(token, &self.space)?.seq)
            }
            None => None,
        };
        let from_command = match as_of {
            Some(as_of) => Some(self.resolve_as_of(as_of).await?),
            None => None,
        };
        match (from_token, from_command) {
            (Some(bound), Some(named)) if bound != named => {
                return Err(KipError::invalid_request_envelope(format!(
                    "this request is bound to snapshot {bound} and its command reads AS OF                      {named}; one read answers at one coordinate"
                )));
            }
            _ => {}
        }
        self.as_of = from_command.or(from_token);
        // The Schema that was in force then is what a historical read resolves
        // symbols through: reconstructing the past under today's schema would
        // answer a question nobody asked (§144).
        if let Some(seq) = self.as_of {
            let version = self.store.schema_version_at(&self.space, seq).await?;
            self.env = self
                .store
                .schema_environment_at(&self.space, version)
                .await?;
        }
        Ok(())
    }

    /// Resolves an `AS OF` coordinate to a Space sequence.
    pub async fn resolve_as_of(&mut self, as_of: &anda_kip::AsOf) -> Result<u64, KipError> {
        let (scalar, kind) = match as_of {
            anda_kip::AsOf::Seq(scalar) => (scalar, "SEQ"),
            anda_kip::AsOf::Tx(scalar) => (scalar, "TX"),
            anda_kip::AsOf::Time(scalar) => (scalar, "TIME"),
        };
        let value = match scalar {
            Scalar::Literal(literal) => Json::from(literal.clone()),
            Scalar::Param(name) => self.param_ref(name)?,
        };
        match (kind, value) {
            ("SEQ", Json::Number(number)) => number
                .as_u64()
                .ok_or_else(|| KipError::type_mismatch("AS OF SEQ takes a non-negative sequence")),
            ("TX", Json::String(tx_id)) => self.store.seq_of_transaction(&self.space, &tx_id).await,
            ("TIME", Json::String(at)) => {
                let at = crate::time::normalize(&at, "AS OF TIME")?;
                self.store.seq_at_time(&self.space, &at).await
            }
            (kind, other) => Err(KipError::type_mismatch(format!(
                "AS OF {kind} does not take {other}"
            ))),
        }
    }

    /// Evaluates a `WHERE` block into one set of solutions.
    ///
    /// Boxed because `NOT`, `OPTIONAL` and `UNION` nest blocks inside blocks,
    /// and a self-referential `async fn` needs a heap-allocated future.
    pub fn solve<'s>(
        &'s mut self,
        clauses: &'s [WhereClause],
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Solutions, KipError>> + Send + 's>> {
        Box::pin(async move {
            let mut solutions = Solutions::unit();
            for clause in clauses {
                solutions = self.apply_clause(solutions, clause).await?;
            }
            Ok(solutions)
        })
    }

    fn apply_clause<'s>(
        &'s mut self,
        solutions: Solutions,
        clause: &'s WhereClause,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Solutions, KipError>> + Send + 's>> {
        Box::pin(async move { self.apply_clause_inner(solutions, clause).await })
    }

    async fn apply_clause_inner(
        &mut self,
        solutions: Solutions,
        clause: &WhereClause,
    ) -> Result<Solutions, KipError> {
        Ok(match clause {
            WhereClause::Concept { variable, matcher } => {
                let table = self
                    .match_element(ElementKind::Concept, variable, matcher)
                    .await?;
                solutions.join(table)
            }
            WhereClause::Assertion { variable, matcher } => {
                let table = self
                    .match_element(ElementKind::Assertion, variable, matcher)
                    .await?;
                solutions.join(table)
            }
            WhereClause::Evidence { variable, matcher } => {
                let table = self
                    .match_element(ElementKind::Evidence, variable, matcher)
                    .await?;
                solutions.join(table)
            }
            WhereClause::Activity { variable, matcher } => {
                let table = self
                    .match_element(ElementKind::Activity, variable, matcher)
                    .await?;
                solutions.join(table)
            }
            WhereClause::Proposition { variable, matcher } => {
                // The solutions so far are passed in so a traversal can start
                // from what an earlier pattern already pinned: `?a CONCEPT
                // {name: "A"} (?a, "leads_to"{1,3}, ?b)` should walk from A,
                // not walk the whole Space and then throw most of it away in
                // the join.
                let table = self
                    .match_proposition(variable.as_deref(), matcher, &solutions)
                    .await?;
                solutions.join(table)
            }
            WhereClause::Structural {
                variable,
                subject,
                field,
                object,
            } => {
                if variable.is_some() {
                    // Binding the edge itself would need a durable edge record,
                    // and Core deliberately has none (§103 Q1).
                    return Err(KipError::unsupported_capability(
                        "binding a variable to a structural edge is not supported: Core stores \
                         structural references as typed fields, not as addressable edge records",
                    ));
                }
                let table = self.match_structural(subject, field, object).await?;
                solutions.join(table)
            }
            WhereClause::Filter { expression } => {
                let mut solutions = solutions;
                // A filter may read a dot path off any bound element, so the
                // views have to exist before it runs.
                self.warm(&solutions).await?;
                self.apply_filter(&mut solutions, expression)?;
                solutions
            }
            WhereClause::Not(inner) => {
                let table = self.solve(inner).await?;
                solutions.anti_join(table)
            }
            WhereClause::Optional(inner) => {
                let table = self.solve(inner).await?;
                solutions.left_join(table)
            }
            WhereClause::Union(inner) => {
                // An alternative branch with its own scope: its solutions are
                // added to what came before rather than intersected with it,
                // so a branch binding different variables widens the result
                // instead of filtering the other side away.
                let branch = self.solve(inner).await?;
                solutions.union(branch)
            }
            WhereClause::Belief { variable, target } => {
                let table = self.match_belief(variable, target, &solutions).await?;
                solutions.join(table)
            }
            WhereClause::BeliefSlot {
                variable,
                subject,
                predicate,
            } => {
                let table = self.match_belief_slot(variable, subject, predicate).await?;
                solutions.join(table)
            }
        })
    }
}

/// Runs one KQL query.
pub async fn execute(
    store: &Store,
    space: &str,
    query: &KqlQuery,
    request: &Request,
    operation: &Operation,
) -> Response {
    match run(store, space, query, request, operation).await {
        Ok((projected, schema_environment_version, epistemic_policy)) => {
            let result = Json::Array(projected.rows);
            Response {
                context: Some(ResponseContext {
                    space_id: Some(space.to_string()),
                    schema_environment_version: Some(schema_environment_version),
                    compatibility_profile_used: None,
                    extensions: None,
                }),
                next_cursor: projected.next_cursor.clone(),
                results: vec![anda_kip::OperationResult {
                    context: Some(ResultContext {
                        space_id: Some(space.to_string()),
                        schema_environment_version: Some(schema_environment_version),
                        // Spec §54: a belief reported without the policy it was
                        // projected under is not auditable.
                        epistemic_policy,
                        ..Default::default()
                    }),
                    next_cursor: projected.next_cursor,
                    ..anda_kip::OperationResult::ok(result)
                }],
                ..Default::default()
            }
        }
        Err(err) => Response::from(err),
    }
}

type Answer = (Projected, u64, Option<anda_kip::PolicyIdentity>);

async fn run(
    store: &Store,
    space: &str,
    query: &KqlQuery,
    request: &Request,
    operation: &Operation,
) -> Result<Answer, KipError> {
    let mut cx = Context::open(
        store,
        space,
        request.parameters.as_ref(),
        operation.parameters.as_ref(),
    )
    .await?;
    cx.bind_read(query.as_of.as_ref(), request).await?;
    let environment_version = cx.env.version;

    if let Some(block) = &query.epistemic {
        let settings = crate::projection::settings_of(block, |name| cx.param_ref(name))?;
        cx.policy = crate::projection::Policy::from_settings(&settings)?;
    }
    // `FOR TIME` names the world time a claim has to apply at, so a projection
    // in the same query answers about that instant rather than about now.
    if let Some(for_time) = &query.for_time {
        let at = match for_time {
            Scalar::Literal(literal) => Json::from(literal.clone()),
            Scalar::Param(name) => cx.param_ref(name)?,
        };
        if let Json::String(at) = at {
            cx.at = crate::time::normalize(&at, "FOR TIME")?;
        }
    }

    let mut solutions = cx.solve(&query.where_clauses).await?;
    if let Some(for_time) = &query.for_time {
        let at = match &for_time {
            Scalar::Literal(literal) => Json::from(literal.clone()),
            Scalar::Param(name) => cx.param(name)?,
        };
        let Json::String(at) = at else {
            return Err(KipError::type_mismatch(
                "FOR TIME takes an RFC 3339 timestamp",
            ));
        };
        let at = crate::time::normalize(&at, "FOR TIME")?;
        cx.warm(&solutions).await?;
        restrict_to_valid_time(&mut cx, &mut solutions, &at);
    }

    let limit = query
        .limit
        .as_ref()
        .map(|scalar| scalar_usize(&cx, scalar, "LIMIT"))
        .transpose()?;
    let cursor = query
        .cursor
        .as_ref()
        .map(|scalar| scalar_usize(&cx, scalar, "CURSOR"))
        .transpose()?;

    // ORDER BY and the projection both read fields off bound elements.
    cx.warm(&solutions).await?;
    let policy = cx.projected.then(|| cx.policy.identity());
    let projected = cx.project(
        solutions,
        &query.find_clause,
        query.order_by.as_ref(),
        limit,
        cursor,
    )?;
    Ok((projected, environment_version, policy))
}

/// Drops solutions whose Assertions did not apply at the given world time.
///
/// `FOR TIME` is world-valid time, an axis independent of `AS OF` (Spec §36.1):
/// it asks what was *applicable* then, not what the Brain contained then.
/// Only Assertions carry validity, so only Assertion-bound columns are
/// restricted; an element with no validity window is unaffected rather than
/// excluded, because having no window means "always", not "never".
fn restrict_to_valid_time(cx: &mut Context<'_>, solutions: &mut Solutions, at: &str) {
    let assertion_vars: Vec<String> = solutions
        .vars
        .iter()
        .filter(|var| {
            solutions
                .values_of(var)
                .iter()
                .any(|binding| binding.kind() == Some(ElementKind::Assertion))
        })
        .cloned()
        .collect();
    if assertion_vars.is_empty() {
        return;
    }
    let views: BTreeMap<ElementId, Json> = solutions
        .rows
        .iter()
        .flat_map(|row| row.iter())
        .filter_map(|binding| binding.element())
        .filter(|id| id.kind == ElementKind::Assertion)
        .filter_map(|id| cx.cached_view(id).map(|view| (id, view)))
        .collect();

    let snapshot = solutions.clone();
    solutions.rows.retain(|row| {
        assertion_vars.iter().all(|var| {
            let Some(id) = snapshot.get(row, var).and_then(binding::Binding::element) else {
                return true;
            };
            let Some(view) = views.get(&id) else {
                return true;
            };
            let from = view["valid_time"]["from"].as_str().unwrap_or("");
            let until = view["valid_time"]["until"].as_str().unwrap_or("");
            (from.is_empty() || from <= at) && (until.is_empty() || at < until)
        })
    });
}

fn scalar_usize(cx: &Context<'_>, scalar: &Scalar, what: &str) -> Result<usize, KipError> {
    let value = match scalar {
        Scalar::Literal(literal) => Json::from(literal.clone()),
        Scalar::Param(name) => cx.param_ref(name)?,
    };
    match &value {
        Json::Number(n) => n.as_u64().map(|n| n as usize).ok_or_else(|| {
            KipError::type_mismatch(format!("{what} must be a non-negative integer, got {n}"))
        }),
        // A cursor round-trips as the opaque string this engine emitted.
        Json::String(text) => text.parse().map_err(|_| {
            KipError::new(
                anda_kip::KipErrorCode::CursorInvalidated,
                format!("{what} is not a cursor this engine issued: {text:?}"),
            )
        }),
        other => Err(KipError::type_mismatch(format!(
            "{what} must be a non-negative integer, got {other}"
        ))),
    }
}
