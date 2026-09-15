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
mod validation;

use anda_db_schema::Fv;
use anda_kip::{
    ElementKind, Json, KipError, KqlQuery, Map, Operation, Request, Response, ResponseContext,
    ResultContext, Scalar, WhereClause,
};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::db_error;
use crate::governance::{AuthContext, EffectiveAuthority, Permission, ResourceContext};
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
    views: BTreeMap<ElementId, Arc<Json>>,
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
    /// The coordinate this read answers at, historical or not.
    ///
    /// Always known, unlike [`Context::as_of`], which is `None` for a read of
    /// the present. §50 asks a KQL answer to identify its `snapshot_seq`, and
    /// §44.8 makes a page cursor carry it so the next page continues over the
    /// same canonical snapshot rather than over whatever the Space holds by
    /// then.
    pub pinned_seq: u64,
    /// The identity of this traversal, for the cursors it reads and issues.
    pub traversal: String,
    /// What the caller may see here, resolved once for the whole read.
    pub authority: &'a EffectiveAuthority,
    /// Who the caller is.
    pub auth: &'a AuthContext,
    /// Whether `_system.origin` may be returned at all (§29).
    ///
    /// Space-scoped and decided once: engine origin is operational information
    /// about the deployment rather than about any one element, so a caller
    /// either may see who writes here or may not.
    pub read_origin: bool,
    governed_limit: Option<usize>,
    budget: usize,
    /// Explicit mutation output handles are ambient values, unlike query
    /// variables introduced by a WHERE branch. Standalone KQL has none.
    ambient: Solutions,
    next_internal_variable: u64,
}

impl<'a> Context<'a> {
    /// Opens a query context.
    pub async fn open(
        store: &'a Store,
        space: &str,
        request: Option<&'a Map<String, Json>>,
        operation: Option<&'a Map<String, Json>>,
        authority: &'a EffectiveAuthority,
        auth: &'a AuthContext,
    ) -> Result<Self, KipError> {
        Ok(Self {
            env: store.schema_environment(space).await?,
            store,
            space: space.to_string(),
            request,
            operation,
            loaded: BTreeMap::new(),
            views: BTreeMap::new(),
            policy: store
                .projection_policy_at(space, u64::MAX, &Map::new())
                .await?,
            at: crate::time::now(),
            projected: false,
            as_of: None,
            pinned_seq: store.get_space(space).await?.seq,
            traversal: String::new(),
            authority,
            auth,
            read_origin: authority
                .authorize(
                    crate::governance::Permission::ReadRawOrigin,
                    &crate::governance::ResourceContext::default(),
                    auth,
                )
                .is_permitted(),
            governed_limit: authority
                .authorize(Permission::Read, &ResourceContext::default(), auth)
                .constraints
                .max_results
                .map(|limit| limit as usize),
            budget: MAX_CANDIDATES,
            ambient: Solutions::unit(),
            next_internal_variable: 0,
        })
    }

    /// Looks up a `:parameter`.
    pub fn param_ref(&self, name: &str) -> Result<Json, KipError> {
        self.operation
            .and_then(|map| map.get(name))
            .or_else(|| self.request.and_then(|map| map.get(name)))
            .cloned()
            .ok_or_else(|| {
                KipError::reference_error(format!(
                    "the query uses the parameter :{name}, which the request does not bind"
                ))
            })
    }

    /// Loads an element once per query, caching both the row and its view.
    ///
    /// A bound read loads the version that was current at its coordinate, so
    /// every dot path, filter and sort key downstream reads the same past.
    ///
    /// **This is the read path's authorization choke point.** Every pattern in
    /// [`matching`] reaches an element through here and skips what comes back
    /// `None`, so an element this caller may not read is outside the query
    /// universe for the whole query — not matched, not counted, not ranked, not
    /// paginated over (§104). Putting the check anywhere later would mean each
    /// new pattern had to remember to apply it.
    pub async fn load(&mut self, id: ElementId) -> Result<Option<Element>, KipError> {
        let element = self.load_unattached(id).await?;
        if let Some(row) = &element
            && (crate::schema::contracts::is_derived(row)
                || self
                    .store
                    .control_at(
                        &self.space,
                        &format!("identity_review/{id}"),
                        self.pinned_seq,
                    )
                    .await?
                    .is_some())
        {
            let policy = self.policy.clone();
            let at = self.at.clone();
            let validity = self.dependency_validity(row, &policy, &at).await?;
            if self
                .authority
                .may_read(row, self.auth)
                .is_some_and(|v| v.content)
                && let Some(view) = self.views.get_mut(&id)
            {
                let mut json = (**view).clone();
                if let Some(system) = json.get_mut("_system").and_then(Json::as_object_mut) {
                    system.insert("dependency_validity".into(), validity);
                }
                *view = Arc::new(json);
            }
        }
        // §43.2: a Proposition's view keeps both readings of each endpoint —
        // `subject` / `object` as stored, `canonical_subject` /
        // `canonical_object` merge-resolved at this read's coordinate.
        if let Some(Element::Proposition(row)) = &element {
            let subject = self.canonical_endpoint(&row.subject).await?;
            let object = self.canonical_endpoint(&row.object).await?;
            if let Some(view) = self.views.get(&id) {
                let mut view = (**view).clone();
                if let Some(object_view) = view.as_object_mut() {
                    object_view.insert("canonical_subject".to_string(), subject);
                    object_view.insert("canonical_object".to_string(), object);
                }
                self.views.insert(id, Arc::new(view));
            }
        }
        self.filter_reference_audit(id).await?;
        Ok(element)
    }

    /// Runtime reference audit is useful only for inputs this caller may read.
    /// Filtering both spellings prevents a visible canonical target from
    /// disclosing a hidden alias that another write originally supplied.
    async fn filter_reference_audit(&mut self, id: ElementId) -> Result<(), KipError> {
        let Some(bindings) = self
            .views
            .get(&id)
            .and_then(|view| view["_system"]["input_references"].as_array())
            .cloned()
        else {
            return Ok(());
        };
        let mut visible = Vec::new();
        for binding in bindings {
            let mut readable = true;
            for key in ["supplied", "resolved"] {
                let Some(reference) = binding[key]
                    .as_str()
                    .and_then(|value| value.parse::<ElementId>().ok())
                else {
                    readable = false;
                    break;
                };
                let source = self
                    .store
                    .element_at(&self.space, reference, self.pinned_seq)
                    .await?;
                if !source.as_ref().is_some_and(|row| {
                    self.authority
                        .may_read(row, self.auth)
                        .is_some_and(|visibility| visibility.content)
                }) {
                    readable = false;
                    break;
                }
            }
            if readable {
                visible.push(binding);
            }
        }
        if let Some(view) = self.views.get_mut(&id) {
            let mut json = (**view).clone();
            if let Some(system) = json.get_mut("_system").and_then(Json::as_object_mut) {
                system.insert("input_references".into(), Json::Array(visible));
            }
            *view = Arc::new(json);
        }
        Ok(())
    }

    /// Loads and admits one element without attaching the canonical
    /// endpoints a Proposition's view carries.
    ///
    /// The merge chain a Proposition's endpoints resolve through is walked
    /// with this, so following a pointer never re-enters the attachment that
    /// asked for it.
    pub(crate) async fn load_unattached(
        &mut self,
        id: ElementId,
    ) -> Result<Option<Element>, KipError> {
        if let Some(cached) = self.loaded.get(&id) {
            return Ok(cached.clone());
        }
        let element = match self.as_of {
            Some(seq) => self.store.element_at(&self.space, id, seq).await?,
            None => self.store.get_element(id).await.ok(),
        };
        let element = self.admit(element);
        self.loaded.insert(id, element.clone());
        Ok(element)
    }

    /// Admit an immutable, explicitly declared transaction output for a raw
    /// WHERE read. The caller supplies the transaction Space on the snapshot;
    /// the same authorization/redaction path as storage reads still applies.
    pub(crate) fn seed_element(&mut self, id: ElementId, element: Element) -> bool {
        let admitted = self.admit(Some(element));
        let visible = admitted.is_some();
        self.loaded.insert(id, admitted);
        visible
    }

    /// Follows a Concept's `merged_into` chain to the identity that survived
    /// (§11.1, §12.3), at this read's coordinate.
    ///
    /// Walked through the read path's own admission, so a Concept the caller
    /// may not discover ends the chain rather than being named through it
    /// (§30.4). Bounded independently of the cycle check the write path keeps,
    /// because a corrupt chain must refuse rather than spin.
    pub async fn canonical_of(&mut self, id: ElementId) -> Result<ElementId, KipError> {
        const MAX_HOPS: usize = 64;
        let mut cursor = id;
        for _ in 0..MAX_HOPS {
            if cursor.kind != ElementKind::Concept {
                return Ok(cursor);
            }
            let next = match self.load_unattached(cursor).await? {
                Some(Element::Concept(row)) if !row.merged_into.is_empty() => {
                    row.merged_into.parse::<ElementId>()?
                }
                _ => return Ok(cursor),
            };
            if next == cursor {
                return Ok(cursor);
            }
            cursor = next;
        }
        Err(KipError::internal_error(format!(
            "the merged_into chain above {id} is longer than {MAX_HOPS} hops"
        )))
    }

    /// Every Concept whose `merged_into` chain resolves to the same canonical
    /// identity as `id` — the class a canonical term matches (§12.3, §43.2).
    ///
    /// After `MERGE CONCEPT :alicia INTO :alice` the class of either is
    /// `{alice, alicia}`, so a term naming B finds the tuples recorded on an A
    /// merged into B, and a term naming A still finds them. Sorted, so two
    /// engines walk the same class in the same order.
    pub async fn merge_class(&mut self, id: ElementId) -> Result<Vec<ElementId>, KipError> {
        if id.kind != ElementKind::Concept {
            return Ok(vec![id]);
        }
        let canonical = self.canonical_of(id).await?;
        let mut class = vec![canonical];
        let mut frontier = vec![canonical];
        while let Some(target) = frontier.pop() {
            for source in self.merged_sources(target).await? {
                if !class.contains(&source) {
                    class.push(source);
                    frontier.push(source);
                }
            }
        }
        if !class.contains(&id) {
            class.push(id);
        }
        class.sort();
        Ok(class)
    }

    /// The Concepts merged directly into one, at this read's coordinate.
    async fn merged_sources(&mut self, target: ElementId) -> Result<Vec<ElementId>, KipError> {
        let ids = self
            .candidates(
                ElementKind::Concept,
                Some(anda_db::query::Filter::And(vec![
                    Box::new(eq_field("space", Fv::Text(self.space.clone()))),
                    Box::new(eq_field("merged_into", Fv::Text(target.to_string()))),
                ])),
            )
            .await?;
        self.charge(ids.len())?;
        let mut out = Vec::new();
        for id in ids {
            // At a past coordinate the index could not narrow, so the pointer
            // is checked on the row that was current then.
            if let Some(Element::Concept(row)) = self.load_unattached(id).await?
                && row.merged_into == target.to_string()
            {
                out.push(id);
            }
        }
        Ok(out)
    }

    /// The equality keys a fixed tuple endpoint matches: the merge class of a
    /// Concept, the endpoint itself otherwise.
    pub async fn endpoint_keys(
        &mut self,
        endpoint: &crate::term::Endpoint,
    ) -> Result<Vec<String>, KipError> {
        Ok(match endpoint {
            crate::term::Endpoint::Local(id) if id.kind == ElementKind::Concept => self
                .merge_class(*id)
                .await?
                .into_iter()
                .map(|member| crate::term::Endpoint::Local(member).key())
                .collect(),
            other => vec![other.key()],
        })
    }

    /// A stored endpoint, merge-resolved, as a read returns it.
    pub(crate) async fn canonical_endpoint(&mut self, value: &Json) -> Result<Json, KipError> {
        match crate::term::Endpoint::from_json(value) {
            Ok(crate::term::Endpoint::Local(id)) if id.kind == ElementKind::Concept => {
                let canonical = self.canonical_of(id).await?;
                Ok(crate::term::Endpoint::Local(canonical).to_json())
            }
            _ => Ok(crate::view::endpoint_view(value)),
        }
    }

    /// Applies the read decision to one loaded element, caching its view.
    ///
    /// Returns `None` for an element this caller may not *discover*, and caches
    /// the **redacted** view for one it may — so a `FILTER` or an `ORDER BY` on
    /// a masked field sees what the projection would, rather than being able to
    /// probe the value through row membership (§29.2).
    ///
    /// An element the caller may discover but not read comes back with its
    /// identity and nothing else (§29.2). It still exists, is still counted and
    /// can still be cited; what it says stays closed.
    pub(crate) fn admit(&mut self, element: Option<Element>) -> Option<Element> {
        let element = element?;
        let visibility = self.authority.may_read(&element, self.auth)?;
        if let Some(limit) = visibility
            .constraints
            .max_results
            .map(|limit| limit as usize)
        {
            self.governed_limit = Some(
                self.governed_limit
                    .map_or(limit, |current| current.min(limit)),
            );
        }
        let mut view = crate::view::render(&element);
        if crate::schema::contracts::is_derived(&element) {
            view["_system"]["dependency_validity"] = serde_json::json!({
                "status": "unverifiable", "action_eligible": false,
                "reasons": ["recursive dependency validation is unavailable"],
                "basis": self.projection_basis(&self.policy, &self.at, None),
            });
        }
        if visibility.content {
            crate::governance::redact::apply(&mut view, &visibility.constraints, self.read_origin);
        } else {
            // `discover` without `read` (§29.1, §29.2): the element is in the
            // query universe and its contents are not. Everything but identity
            // goes, and the view says so rather than looking like an element
            // that happens to have no fields.
            crate::governance::redact::to_identity_only(&mut view);
        }
        self.views.insert(element.id(), Arc::new(view));
        Some(element)
    }

    /// The rendered view of an already-loaded element.
    ///
    /// Shared rather than copied: a view is the whole rendered element, and the
    /// read path asks for one per row per filter operand, per sort key and per
    /// projected column. Deep-copying on every hit made the cache cost about
    /// what not having it did.
    pub fn cached_view(&self, id: ElementId) -> Option<Arc<Json>> {
        self.views.get(&id).cloned()
    }

    /// The rendered view of an element this read has admitted.
    ///
    /// [`Context::admit`] caches a view for every element it lets through, so
    /// the empty object is unreachable for anything that came back from
    /// [`Context::load`]. It exists so the read paths do not each invent their
    /// own answer to a question that has one — which is how one of them once
    /// came to fall back on the *unredacted* renderer, three lines under a
    /// comment saying why the redacted view was required.
    pub fn view_of(&self, id: ElementId) -> Arc<Json> {
        self.cached_view(id)
            .unwrap_or_else(|| Arc::new(Json::Object(Default::default())))
    }

    /// The tightest result cap carried by any authority used by this read.
    pub fn governed_limit(&self) -> Option<usize> {
        self.governed_limit
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

    /// Every active Concept in the Space, for patterns no index narrows.
    pub async fn active_concepts(&mut self) -> Result<Vec<ElementId>, KipError> {
        self.active_of(ElementKind::Concept).await
    }

    /// Every active element of one kind in this Space.
    ///
    /// The generalization `active_concepts` used to be: a Core structural field
    /// belongs to an Assertion, an Evidence record or an Activity, so a
    /// `STRUCTURAL` pattern with an unbound source has to be able to start from
    /// the kind that could carry the field rather than always from Concepts.
    pub async fn active_of(&mut self, kind: ElementKind) -> Result<Vec<ElementId>, KipError> {
        if self.as_of.is_some() {
            return Ok(self.candidates(kind, None).await?.into_iter().collect());
        }
        let ids = self
            .store
            .elements(kind)
            .query_all_ids(anda_db::query::Filter::And(vec![
                Box::new(eq_field("space", Fv::Text(self.space.clone()))),
                Box::new(eq_field("state", Fv::Text("active".to_string()))),
            ]))
            .await
            .map_err(db_error)?;
        Ok(ids
            .into_iter()
            .map(|seq| ElementId::new(kind, seq))
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
                // It still goes through `admit`, because a past coordinate is
                // not a way around the present's authorization — the read is
                // happening now, by this caller.
                let admitted = self.admit(Some(element));
                self.loaded.insert(id, admitted.clone());
                if admitted.is_some() {
                    ids.push(id);
                }
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
        cursor: Option<crate::store::history::PageCursor>,
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
                    "this request is bound to snapshot {bound} and its command reads AS OF \
                     {named}; one read answers at one coordinate"
                )));
            }
            _ => {}
        }
        // A continuation is pinned to the coordinate its own first page read
        // at (§44.8). It does not ask for `read_history`: the caller is
        // resuming a traversal it already began, over the same rows page one
        // returned, so requiring a permission page one did not need would make
        // paging a privilege rather than a mechanic.
        //
        // Reconstruction is only engaged when the Space has actually moved on.
        // At the current coordinate the version log would rebuild exactly what
        // the live indexes already hold, at the cost of scanning it.
        let from_cursor = cursor.as_ref().and_then(|cursor| {
            (cursor.snapshot_seq < self.pinned_seq).then_some(cursor.snapshot_seq)
        });
        if let Some(cursor) = cursor {
            match from_command.or(from_token) {
                Some(named) if named != cursor.snapshot_seq => {
                    return Err(KipError::invalid_request_envelope(format!(
                        "this cursor continues a traversal pinned to snapshot {}, and this read \
                         names {named}; one traversal answers at one coordinate",
                        cursor.snapshot_seq
                    )));
                }
                _ => {}
            }
            self.pinned_seq = cursor.snapshot_seq;
        }
        self.as_of = from_command.or(from_token).or(from_cursor);
        if let Some(seq) = self.as_of {
            self.pinned_seq = seq;
        }
        if let Some(seq) = self.as_of {
            self.policy = self
                .store
                .projection_policy_at(&self.space, seq, &Map::new())
                .await
                .unwrap_or_else(|_| {
                    let mut p = crate::projection::Policy::baseline();
                    p.trust_version = "unavailable".into();
                    p
                });
        }
        // The Schema that was in force then is what a historical read resolves
        // symbols through: reconstructing the past under today's schema would
        // answer a question nobody asked (§20.9).
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
    ///
    /// `AS OF SEQ` is the only historical axis (§48.1): a transaction id
    /// resolves to its sequence through `DESCRIBE TRANSACTION`, and an instant
    /// through `DESCRIBE SNAPSHOT AT TIME`, so a historical read always names
    /// the exact coordinate it was served from.
    pub async fn resolve_as_of(&mut self, as_of: &anda_kip::AsOf) -> Result<u64, KipError> {
        let anda_kip::AsOf::Seq(scalar) = as_of;
        let value = match scalar {
            Scalar::Literal(literal) => Json::from(literal.clone()),
            Scalar::Param(name) => self.param_ref(name)?,
        };
        match value {
            Json::Number(number) => number
                .as_u64()
                .ok_or_else(|| KipError::type_mismatch("AS OF SEQ takes a non-negative sequence")),
            other => Err(KipError::type_mismatch(format!(
                "AS OF SEQ does not take {other}"
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
        self.solve_seeded(clauses, Solutions::unit())
    }

    /// Evaluate raw WHERE with explicit enclosing mutation output handles.
    pub fn solve_seeded<'s>(
        &'s mut self,
        clauses: &'s [WhereClause],
        incoming: Solutions,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Solutions, KipError>> + Send + 's>> {
        Box::pin(async move {
            let previous = std::mem::replace(&mut self.ambient, incoming.clone());
            let result = async {
                let scope = incoming.vars.iter().cloned().collect();
                let vars = validation::validate_block(self, clauses, &scope)?;
                let mut solutions = self.solve_with(clauses, incoming).await?;
                // Keep sites even when an empty input skipped a nested block.
                solutions = solutions.union(Solutions::table(vars.into_iter().collect(), vec![]));
                solutions.deduplicate();
                Ok(solutions)
            }
            .await;
            self.ambient = previous;
            result
        })
    }

    fn solve_with<'s>(
        &'s mut self,
        clauses: &'s [WhereClause],
        mut solutions: Solutions,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Solutions, KipError>> + Send + 's>> {
        Box::pin(async move {
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
                let table = self
                    .match_structural(variable.as_deref(), subject, field, object)
                    .await?;
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
                let mut out = solutions.header();
                for row in &solutions.rows {
                    let incoming = solutions.with_rows(vec![row.clone()]);
                    let table = self.solve_with(inner, incoming.clone()).await?;
                    if incoming.clone().join(table).is_empty() {
                        out = out.union(incoming);
                    }
                }
                out
            }
            WhereClause::Optional(inner) => {
                let mut out = solutions.header();
                for row in &solutions.rows {
                    let incoming = solutions.with_rows(vec![row.clone()]);
                    let table = self.solve_with(inner, incoming.clone()).await?;
                    out = out.union(incoming.left_join(table));
                }
                out
            }
            WhereClause::Union(inner) => {
                // An alternative branch with its own scope: its solutions are
                // added to what came before rather than intersected with it,
                // so a branch binding different variables widens the result
                // instead of filtering the other side away.
                let branch = self.solve_with(inner, self.ambient.clone()).await?;
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
                let table = self
                    .match_belief_slot(variable, subject, predicate, &solutions)
                    .await?;
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
    authority: &EffectiveAuthority,
    auth: &AuthContext,
) -> Response {
    match run(store, space, query, request, operation, authority, auth).await {
        Ok(Answer {
            projected,
            schema_environment_version,
            epistemic_policy,
            snapshot_seq,
            valid_at,
        }) => {
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
                        // §50: an answer that cannot say which coordinate it
                        // read is an answer a caller cannot reproduce, and it
                        // is the same field a page cursor pins.
                        snapshot_seq: Some(snapshot_seq),
                        schema_environment_version: Some(schema_environment_version),
                        // Spec §54: a belief reported without the policy it was
                        // projected under is not auditable.
                        epistemic_policy,
                        // The world-time basis, when `FOR TIME` named one.
                        // §48.3 makes it an axis independent of the snapshot:
                        // reporting one without the other leaves a caller
                        // unable to tell a stale answer from a deliberately
                        // historical one.
                        valid_at: valid_at.clone(),
                        cursor: query.cursor.as_ref().and_then(|scalar| match scalar {
                            Scalar::Literal(anda_kip::KipValue::String(text)) => Some(text.clone()),
                            _ => None,
                        }),
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

/// One KQL answer, with the coordinates and policies it was produced under.
///
/// A struct rather than a tuple because §50 keeps adding to it, and a
/// five-element tuple is where `snapshot_seq` and `schema_environment_version`
/// swap places without a compiler complaint.
struct Answer {
    projected: Projected,
    schema_environment_version: u64,
    epistemic_policy: Option<anda_kip::PolicyIdentity>,
    snapshot_seq: u64,
    /// The world-time basis, when `FOR TIME` named one.
    valid_at: Option<String>,
}

/// Reads a `CURSOR` slot as the opaque token this engine issues.
fn page_cursor(
    cx: &Context<'_>,
    scalar: &Scalar,
    space: &str,
) -> Result<crate::store::history::PageCursor, KipError> {
    let token = match scalar {
        Scalar::Literal(literal) => Json::from(literal.clone()),
        Scalar::Param(name) => cx.param_ref(name)?,
    };
    let Json::String(token) = token else {
        return Err(KipError::cursor_invalid(
            crate::store::history::CursorFamily::Query.tag(),
            "malformed",
            format!("CURSOR takes the opaque token this engine issued, got {token}"),
        ));
    };
    crate::store::history::PageCursor::from_token(
        &token,
        space,
        crate::store::history::CursorFamily::Query,
        &cx.traversal,
    )
}

async fn run(
    store: &Store,
    space: &str,
    query: &KqlQuery,
    request: &Request,
    operation: &Operation,
    authority: &EffectiveAuthority,
    auth: &AuthContext,
) -> Result<Answer, KipError> {
    let mut cx = Context::open(
        store,
        space,
        request.parameters.as_ref(),
        operation.parameters.as_ref(),
        authority,
        auth,
    )
    .await?;
    cx.traversal = crate::store::history::traversal_of(
        query,
        request.parameters.as_ref(),
        operation.parameters.as_ref(),
    );
    // The cursor is read before the coordinate is bound, because it *is* one
    // of the things that decides the coordinate.
    let cursor = match &query.cursor {
        Some(scalar) => Some(page_cursor(&cx, scalar, space)?),
        None => None,
    };
    cx.bind_read(query.as_of.as_ref(), request, cursor.clone())
        .await?;
    let environment_version = cx.env.version;

    let settings = match &query.epistemic {
        Some(block) => crate::projection::settings_of(block, |name| cx.param_ref(name))?,
        None => Map::new(),
    };
    cx.policy = match store
        .projection_policy_at(space, cx.pinned_seq, &settings)
        .await
    {
        Ok(policy) => policy,
        Err(e) if e.code == anda_kip::KipErrorCode::HistoricalSnapshotUnavailable => {
            let mut p = crate::projection::Policy::from_settings(&settings)?;
            p.trust_version = "unavailable".into();
            p
        }
        Err(e) => return Err(e),
    };
    let mut policy = cx.policy.clone();
    cx.resolve_projection_context(&mut policy).await?;
    cx.policy = policy;
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

    let visible = validation::validate_block(&mut cx, &query.where_clauses, &Default::default())?;
    validation::validate_projection(&cx, query, &visible)?;
    let mut solutions = cx.solve(&query.where_clauses).await?;
    let mut valid_at = None;
    if let Some(for_time) = &query.for_time {
        let at = match &for_time {
            Scalar::Literal(literal) => Json::from(literal.clone()),
            Scalar::Param(name) => cx.param_ref(name)?,
        };
        let Json::String(at) = at else {
            return Err(KipError::type_mismatch(
                "FOR TIME takes an RFC 3339 timestamp",
            ));
        };
        let at = crate::time::normalize(&at, "FOR TIME")?;
        cx.warm(&solutions).await?;
        restrict_to_valid_time(&mut cx, &mut solutions, &at);
        valid_at = Some(at);
    }

    let limit = query
        .limit
        .as_ref()
        .map(|scalar| scalar_usize(&cx, scalar, "LIMIT"))
        .transpose()?;
    // ORDER BY and the projection both read fields off bound elements. The
    // governed cap is merged in by `project`, after this — every element that
    // could tighten it has been admitted by then.
    cx.warm(&solutions).await?;
    let policy = cx.projected.then(|| cx.policy.identity());
    let pinned_seq = cx.pinned_seq;
    let projected = cx.project(
        solutions,
        &query.find_clause,
        query.order_by.as_ref(),
        limit,
        cursor.as_ref().map(|cursor| cursor.offset),
        pinned_seq,
    )?;
    Ok(Answer {
        projected,
        schema_environment_version: environment_version,
        epistemic_policy: policy,
        snapshot_seq: pinned_seq,
        valid_at,
    })
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
    let views: BTreeMap<ElementId, Arc<Json>> = solutions
        .rows
        .iter()
        .flat_map(|row| row.iter())
        .filter_map(|binding| binding.element())
        .filter(|id| id.kind == ElementKind::Assertion)
        .filter_map(|id| cx.cached_view(id).map(|view| (id, view)))
        .collect();

    let snapshot = solutions.header();
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
        Json::Number(n) => n
            .as_f64()
            .filter(|n| {
                n.is_finite() && *n >= 0.0 && *n <= 9_007_199_254_740_991.0 && n.fract() == 0.0
            })
            .and_then(|n| usize::try_from(n as u64).ok())
            .ok_or_else(|| {
                KipError::type_mismatch(format!(
                    "{what} must be a non-negative safe integer, got {n}"
                ))
            }),
        other => Err(KipError::type_mismatch(format!(
            "{what} must be a non-negative safe integer, got {other}"
        ))),
    }
}
