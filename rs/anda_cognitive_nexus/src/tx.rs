//! # Transactions
//!
//! A KML statement is one atomic cognitive transition (Spec §2), and three of
//! its properties are the reason this module exists rather than each clause
//! writing directly.
//!
//! **A mutation block is declarative, not sequential** (§21–§24). Forward
//! references are legal, and they have to be: `Evidence.generated_by → Activity`
//! and `Activity.outputs → Evidence` is a legitimate structural cycle, so a
//! define-before-use ordering would make atomic provenance formation
//! impossible. Planning therefore happens in two phases — declare every handle,
//! then interpret every clause with all handles known.
//!
//! **An element's version increments once per transaction** (§35.5), no matter
//! how many clauses touched it, and each version plane advances once when the
//! transaction changed that plane (§6.3). A transaction is one externally
//! visible state transition, and `EXPECT VERSION`, audit and the change stream
//! all read those counters. So versions are assigned here, at commit, from a
//! diff of what was loaded against what is written — not by each clause.
//!
//! **A no-effect final state changes nothing.** Writing the same value back
//! would burn a version and emit a change record for a transition that did not
//! happen.
//!
//! ## What this engine does and does not give you
//!
//! Within one process, the Nexus serializes mutations behind a write lock that
//! readers also take, so no reader observes a half-applied transaction. What it
//! does not have is a write-ahead log: a crash *during* commit can leave
//! elements written. Those are minted in the `pending` state and belong to no
//! journalled transaction, so [`Store::sweep_pending`] removes them on open —
//! recovery by construction rather than by replay.

use anda_kip::{
    ChangeEntry, ChangeOp, ChangeRefs, ChangeState, ElementKind, Json, KipError, KipErrorCode, Map,
    PlaneVersions, Receipt, ReceiptOrigin, ReceiptStatus,
};
use std::collections::{BTreeMap, BTreeSet};

use crate::error::db_error;
use crate::governance::approval::Approved;
use crate::governance::store::MutationEntry;
use crate::governance::{AuthContext, EffectiveAuthority, Permission, ResourceContext};
use crate::id::ElementId;
use crate::schema::SchemaEnvironment;
use crate::store::planes::{self, PlaneKey, Touched};
use crate::store::rows::*;
use crate::store::space::JournalEntry;
use crate::store::write::{Row, WriteContext};
use crate::store::{Element, Store};

/// The engine state one KML statement runs against.
pub struct Transaction {
    store: Store,
    /// The engine truth stamped on everything this transaction writes.
    pub cx: WriteContext,
    /// The Schema Environment this transaction is bound to.
    ///
    /// Captured once at the start: a transaction evaluates against one
    /// consistent environment snapshot (§32.5), so an activation racing
    /// alongside cannot change what half of it means.
    pub env: SchemaEnvironment,
    /// Whether this run may become durable.
    pub dry_run: bool,
    /// What the caller may do here, resolved before the transaction opened.
    ///
    /// Cloned rather than borrowed: a transaction outlives the borrow that
    /// produced it, and a stale authority is not a risk here — it was resolved
    /// under the same write lock this transaction holds, so nothing can have
    /// revoked anything in between (§28.6).
    pub authority: EffectiveAuthority,
    /// Who the caller is.
    pub auth: AuthContext,
    reference_bindings: Vec<Json>,
    handles: BTreeMap<String, ElementId>,
    staged: BTreeMap<ElementId, Staged>,
    shells: Vec<ElementId>,
    warnings: Vec<String>,
    /// The version rows each staged purge will destroy at commit, read when the
    /// stub was staged so the receipt and the erasure cannot disagree.
    purges: BTreeMap<ElementId, Vec<u64>>,
    /// Elements whose recorded versions lose their Evidence payload at commit.
    payload_purges: BTreeMap<ElementId, Vec<u64>>,
    approval_decisions: Vec<Approved>,
    governance_audit: Vec<MutationEntry>,
    /// The explicit positions this mutation plan has already claimed, per
    /// element and ordered structural field (§17.4).
    ///
    /// Plan-wide rather than clause-wide, because §17.4 forbids *conflicting
    /// explicit positions in one mutation plan* and a plan is free to spread
    /// them across clauses.
    structural_positions: BTreeMap<(ElementId, String), BTreeSet<usize>>,
    /// The final value this mutation plan has already specified, per element
    /// and path (§53.4).
    ///
    /// Plan-wide for the same reason positions are: a `MUTATE` block is
    /// declarative, so two clauses may name one target, and clause order
    /// carries no mutation semantics. Two clauses that agree are fine; two
    /// that disagree have no answer that is not the engine choosing one.
    assignments: BTreeMap<(ElementId, String), Json>,
    /// The ActorBinding this transaction exercised, when an Assertion was
    /// written under one (§28.3); reported in the Receipt's `origin` (§33.2).
    exercised_binding: Option<String>,
}

/// One element this transaction will write.
struct Staged {
    row: Element,
    /// The row as the transaction loaded it, for an element that existed.
    ///
    /// This is what every guard compares against and what the change entry is
    /// diffed from: a guard is a statement about what the caller believed, and
    /// the caller could not have seen a version this transaction produced.
    before: Option<Element>,
    is_new: bool,
    /// Whether the final state differs from what was there before.
    changed: bool,
    /// What the change entry calls this (§36.1).
    op: ChangeOp,
    /// Whether the row carries its own envelope through the write.
    ///
    /// Ordinarily the writer stamps who the runtime observed (§26). A purge
    /// stub is the exception, and says so here rather than having the generic
    /// writer recognize it by the name of its operation.
    keep_origin: bool,
}

/// One `EXPECT VERSION` guard, resolved (§35.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Guard {
    /// The counter the caller believes the plane is at.
    pub version: u64,
    /// The plane the guard names; the whole element when it names none.
    pub plane: PlaneKey,
}

impl Transaction {
    /// Opens a transaction, allocating its Space sequence.
    pub async fn begin(
        store: &Store,
        space_id: &str,
        origin: Json,
        dry_run: bool,
        authority: EffectiveAuthority,
        auth: AuthContext,
    ) -> Result<Self, KipError> {
        let env = store.schema_environment(space_id).await?;
        let cx = store.begin_transaction(space_id, origin).await?;
        Ok(Self {
            store: store.clone(),
            cx,
            env,
            dry_run,
            authority,
            auth,
            handles: BTreeMap::new(),
            reference_bindings: Vec::new(),
            staged: BTreeMap::new(),
            shells: Vec::new(),
            warnings: Vec::new(),
            purges: BTreeMap::new(),
            payload_purges: BTreeMap::new(),
            approval_decisions: Vec::new(),
            governance_audit: Vec::new(),
            structural_positions: BTreeMap::new(),
            assignments: BTreeMap::new(),
            exercised_binding: None,
        })
    }

    /// The handles bound so far.
    pub fn handles(&self) -> &BTreeMap<String, ElementId> {
        &self.handles
    }

    /// Records a non-fatal caveat.
    pub fn warn(&mut self, message: impl Into<String>) {
        self.warnings.push(message.into());
    }

    /// Records that an Assertion was written under one of the caller's
    /// ActorBindings, for the Receipt's `origin` (§33.2).
    ///
    /// The first binding exercised is the one reported: a Receipt has one
    /// `actor_binding_id` slot, and a statement that spoke as two actors is
    /// still one commit attributed to one Principal.
    pub fn note_binding(&mut self, binding_id: String) {
        if self.exercised_binding.is_none() {
            self.exercised_binding = Some(binding_id);
        }
    }

    /// Declares a handle and mints the element it will name.
    ///
    /// The id has to exist before any clause is interpreted, because a clause
    /// may reference a handle a later clause declares. `anda_db` assigns ids at
    /// insert time and offers no way to reserve one, so the element is inserted
    /// now as a `pending` shell and filled in at commit.
    ///
    /// A handle may be declared exactly once (§53.2): two clauses binding `?x`
    /// leave every reference to it ambiguous, and picking either one would be
    /// a guess.
    pub async fn declare(
        &mut self,
        handle: &str,
        kind: ElementKind,
    ) -> Result<ElementId, KipError> {
        if self.handles.contains_key(handle) {
            return Err(KipError::duplicate_local_handle(format!(
                "?{handle} is declared more than once in this mutation block"
            )));
        }
        let id = self.mint_shell(kind).await?;
        self.handles.insert(handle.to_string(), id);
        Ok(id)
    }

    /// Claims one explicit position in an ordered structural field.
    ///
    /// Returns `false` when this plan already claimed it — two references
    /// cannot both be third, and picking one would be the engine choosing.
    pub fn claim_position(&mut self, id: ElementId, field: &str, index: usize) -> bool {
        self.structural_positions
            .entry((id, field.to_string()))
            .or_default()
            .insert(index)
    }

    /// Records one path's final value, refusing a plan that specifies two.
    ///
    /// §53.4: "Conflicting final mutation specifications for the same existing
    /// target SHOULD fail." Clause source order is not a hidden
    /// last-write-wins, and the alternative to failing is exactly that — the
    /// caller reads a success and the value they wrote second, or first,
    /// depending on an ordering the language does not give them.
    ///
    /// Two clauses writing the *same* value are not in conflict: a plan
    /// assembled from parts may legitimately say a thing twice.
    pub fn claim_assignment(
        &mut self,
        id: ElementId,
        path: String,
        value: &Json,
    ) -> Result<(), KipError> {
        match self.assignments.entry((id, path.clone())) {
            std::collections::btree_map::Entry::Occupied(entry) => {
                if entry.get() != value {
                    return Err(KipError::new(
                        KipErrorCode::DuplicateMutationTarget,
                        format!(
                            "this mutation block gives {id}'s `{path}` two different final \
                             values; clause order is not a tie-break (§53.4), so say it once"
                        ),
                    ));
                }
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(value.clone());
            }
        }
        Ok(())
    }

    /// Re-points a declared handle at an element that already exists.
    ///
    /// The shell minted for the handle stays unstaged and is discarded at
    /// commit, so a resolved retry writes nothing at all — which is the point:
    /// §52.1 says a `client_key` proves a *retry*, and a retry that left a
    /// spare row behind would still be a duplicate, just an invisible one.
    pub fn rebind(&mut self, handle: &str, id: ElementId) {
        self.handles.insert(handle.to_string(), id);
    }

    /// Mints an element with no handle — an anonymous `ENSURE PROPOSITION`.
    pub async fn mint(&mut self, kind: ElementKind) -> Result<ElementId, KipError> {
        self.mint_shell(kind).await
    }

    async fn mint_shell(&mut self, kind: ElementKind) -> Result<ElementId, KipError> {
        let ordinal = self.shells.len();
        let id = match kind {
            ElementKind::Concept => self.insert_shell(ConceptRow::default()).await?,
            ElementKind::Proposition => {
                // `tuple_key` is unique-indexed, so two default shells would
                // collide on the empty string before either had a real tuple.
                self.insert_shell(PropositionRow {
                    tuple_key: format!("pending:{}:{ordinal}", self.cx.tx_id),
                    ..Default::default()
                })
                .await?
            }
            ElementKind::Assertion => self.insert_shell(AssertionRow::default()).await?,
            ElementKind::Evidence => self.insert_shell(EvidenceRow::default()).await?,
            ElementKind::Activity => self.insert_shell(ActivityRow::default()).await?,
        };
        self.shells.push(id);
        Ok(id)
    }

    async fn insert_shell<R: Row>(&self, mut row: R) -> Result<ElementId, KipError> {
        // `pending` is the marker that makes crash recovery possible: nothing
        // reads it, and anything still wearing it belongs to no committed
        // transaction.
        *row.envelope_mut().state = state::PENDING.to_string();
        self.store.insert(&self.cx, &mut row).await
    }

    /// Binds a handle to an element that already has an id.
    ///
    /// Used by the clauses whose target cannot be minted up front — `UPSERT`
    /// may resolve to an existing Concept, and `ENSURE` may resolve to an
    /// existing tuple — so their handles are bound in phase 2.
    pub fn bind_existing(&mut self, handle: &str, id: ElementId) -> Result<(), KipError> {
        if self.handles.contains_key(handle) {
            return Err(KipError::duplicate_local_handle(format!(
                "?{handle} is declared more than once in this mutation block"
            )));
        }
        self.handles.insert(handle.to_string(), id);
        Ok(())
    }

    /// The Concept type of a staged element, when this transaction staged one.
    ///
    /// Endpoint validation has to see the transaction's own writes: a
    /// Proposition whose subject was created by an earlier clause of the same
    /// block would otherwise look untyped.
    pub fn staged_concept_type(&self, id: ElementId) -> Option<String> {
        match self.staged.get(&id).map(|staged| &staged.row) {
            Some(Element::Concept(row)) if !row.schema_ref.is_empty() => {
                Some(row.schema_ref.clone())
            }
            _ => None,
        }
    }

    /// Stages a newly created element's final row.
    pub fn stage_new(&mut self, id: ElementId, row: Element, op: ChangeOp) {
        self.staged.insert(
            id,
            Staged {
                row,
                before: None,
                is_new: true,
                changed: true,
                op,
                keep_origin: false,
            },
        );
    }

    /// Stages an identity stub and defers destruction of its old versions.
    ///
    /// Returns how many versions the commit will destroy, read here rather than
    /// again at commit so the number a purge receipt reports is the number of
    /// rows actually erased.
    pub async fn stage_purge(&mut self, id: ElementId, row: Element) -> Result<usize, KipError> {
        self.load(id).await?;
        let versions = self.store.version_ids(&self.cx.space, id).await?;
        let staged = self.staged.get_mut(&id).expect("loaded above");
        staged.row = row;
        staged.changed = true;
        staged.op = ChangeOp::Purge;
        staged.keep_origin = true;
        let destroyed = versions.len();
        self.purges.insert(id, versions);
        Ok(destroyed)
    }

    /// Stages a payload purge and defers scrubbing its recorded versions.
    ///
    /// Returns how many version rows the commit will scrub, read here rather
    /// than again at commit so the number a receipt reports and the number of
    /// rows actually rewritten cannot come apart — the same contract
    /// [`Self::stage_purge`] keeps for destruction.
    ///
    /// Scrubbed, not destroyed: an Evidence record survives a payload purge, so
    /// its lifecycle history survives with it. Only the payload columns go
    /// (§60.6).
    pub async fn stage_payload_purge(&mut self, id: ElementId) -> Result<usize, KipError> {
        self.load(id).await?;
        let versions = self.store.version_ids(&self.cx.space, id).await?;
        let scrubbed = versions.len();
        self.payload_purges.insert(id, versions);
        Ok(scrubbed)
    }

    /// Defers spending an approval until this transaction commits successfully.
    ///
    /// Reached through [`Approved::defer`](crate::governance::approval::Approved::defer),
    /// which is the half of the contract the caller states.
    pub(crate) fn defer_approval(&mut self, approved: Approved) {
        self.approval_decisions.push(approved);
    }

    /// Defers a Governance audit entry until this transaction commits.
    pub fn defer_governance_audit(&mut self, entry: MutationEntry) {
        self.governance_audit.push(entry);
    }

    /// Rejects any staged reference that leaves this transaction's Space (§5.3).
    ///
    /// Checked here, once over the staged rows, rather than by each clause that
    /// happens to write a reference. [`Element::references`] is the complete
    /// set — including the paths no index covers — so one pass covers `ENSURE`,
    /// `UPSERT`, Profile structural fields and every clause added later, none
    /// of which has to remember the rule. It runs before the first write, so a
    /// violation refuses the whole transaction rather than half of it.
    ///
    /// An element this same transaction is staging needs no lookup: a
    /// transaction writes into one Space, so anything it mints is in it.
    async fn check_reference_closure(&self) -> Result<(), KipError> {
        for (id, staged) in &self.staged {
            if !staged.changed {
                continue;
            }
            for referenced in staged.row.references() {
                if self.staged.contains_key(&referenced) {
                    continue;
                }
                self.store
                    .check_same_space(&self.cx.space, *id, referenced)
                    .await?;
            }
        }
        Ok(())
    }

    /// Rejects a staged Concept claiming a logical key another Concept of the
    /// same type already holds (§7.3).
    ///
    /// One pass over the staged rows rather than a check inside each clause:
    /// `CREATE CONCEPT`, the create half of `UPSERT` and Capsule import all
    /// mint Concepts, and a rule each of them has to remember is a rule one of
    /// them will forget.
    ///
    /// The scope is `(space_id, type lineage, key)` (§7.3, §20.14): a Person
    /// and a Preference may both be keyed `"alice"` — which is what lets a 1.x
    /// database whose identity was `(type, name)` migrate those names into keys
    /// without merging unrelated Concepts — while a Person written under an
    /// earlier version of the same package is the same population. An empty
    /// key stores "no logical key" and claims nothing.
    async fn check_concept_key_identity(&self) -> Result<(), KipError> {
        let mut claimed: Vec<(String, &str)> = Vec::new();
        for (id, staged) in &self.staged {
            let Element::Concept(row) = &staged.row else {
                continue;
            };
            if !staged.changed || row.key.is_empty() {
                continue;
            }
            let claim = (crate::schema::lineage_of(&row.schema_ref), row.key.as_str());
            let conflict = |holder: &str| {
                KipError::new(
                    KipErrorCode::IdentityConflict,
                    format!(
                        "the key {:?} already identifies {holder} of type {}; a logical key is \
                         identity within its type, not a label two Concepts may share",
                        row.key, row.schema_ref
                    ),
                )
            };
            if claimed.contains(&claim) {
                return Err(conflict("another Concept in this transaction"));
            }
            claimed.push(claim);
            if let Some(found) = self
                .store
                .find_concept_by_key(&self.cx.space, Some(&row.schema_ref), &row.key)
                .await?
                && found._id != id.seq
            {
                return Err(conflict(
                    &ElementId::new(ElementKind::Concept, found._id).to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Loads an existing element for modification, or returns the staged copy.
    ///
    /// Read-your-writes inside the transaction (§32.6): a clause that reads an
    /// element another clause already changed sees the change, because both are
    /// the same staged row.
    pub async fn load(&mut self, id: ElementId) -> Result<&mut Element, KipError> {
        if !self.staged.contains_key(&id) {
            let row = self.store.get_element(id).await?;
            if row.space() != self.cx.space {
                return Err(KipError::not_found_or_not_visible(format!(
                    "{id} lives in another MemorySpace"
                )));
            }
            self.staged.insert(
                id,
                Staged {
                    before: Some(row.clone()),
                    row,
                    is_new: false,
                    changed: false,
                    op: ChangeOp::Update,
                    keep_origin: false,
                },
            );
        }
        Ok(&mut self.staged.get_mut(&id).expect("just inserted").row)
    }

    /// Authorizes one permission over an element this transaction will touch.
    ///
    /// The command gate already asked whether the caller may do this *here*;
    /// this asks whether it may do it to *that*. The two are different questions
    /// whenever a Grant is scoped to a kind, a type or a classification, and
    /// answering only the first is how a narrowed Grant turns into an
    /// unnarrowed one.
    ///
    /// Reads the element from storage rather than from the staging map, because
    /// what matters is the state the caller is acting on — an element this
    /// transaction has already edited is still governed by the classification it
    /// had when the transaction started.
    pub async fn authorize_element(
        &mut self,
        id: ElementId,
        permission: Permission,
    ) -> Result<(), KipError> {
        // `of_element` returns owned strings, so the borrow of `self` ends with
        // this statement and no clone of the row is needed to release it.
        let resource = ResourceContext::of_element(self.load(id).await?);
        self.authority
            .authorize(permission, &resource, &self.auth)
            .into_result()
            .map(|_| ())
    }

    /// Authorizes one permission over an element that does not exist yet.
    ///
    /// A creation has no element to read a classification off, so it is judged
    /// at the Space default — which is what the element will carry. A Grant
    /// narrowed to Concepts must not be a way to create Evidence.
    pub fn authorize_new(
        &self,
        kind: anda_kip::ElementKind,
        schema_ref: &str,
        permission: Permission,
    ) -> Result<(), KipError> {
        let resource = ResourceContext {
            kind: kind.to_string(),
            schema_ref: schema_ref.to_string(),
            classification: self.authority.default_classification().to_string(),
            element_id: String::new(),
        };
        self.authority
            .authorize(permission, &resource, &self.auth)
            .into_result()
            .map(|_| ())
    }

    /// Authorizes a permission over an element this transaction is about to
    /// create, judged on the element as it will be written.
    pub fn authorize_created(
        &self,
        element: &Element,
        permission: Permission,
    ) -> Result<(), KipError> {
        let mut resource = ResourceContext::of_element(element);
        if resource.classification.is_empty() {
            resource.classification = self.authority.default_classification().to_string();
        }
        // The id is the one this transaction minted and nothing has committed
        // yet, so a Grant narrowed to specific elements cannot be satisfied by
        // an element that does not exist. Judging on kind and type is what
        // such a Grant can actually be about.
        resource.element_id = String::new();
        self.authority
            .authorize(permission, &resource, &self.auth)
            .into_result()
            .map(|_| ())
    }

    /// Authorizes a permission that is about the Space rather than an element.
    pub fn require(&self, permission: Permission) -> Result<(), KipError> {
        self.authority
            .authorize(permission, &ResourceContext::default(), &self.auth)
            .into_result()
            .map(|_| ())
    }

    /// Whether this caller may withdraw or supersede one Assertion (§57.3,
    /// §57.4).
    ///
    /// Two ways to hold that authority, and administrative dislike is neither:
    ///
    /// ```text
    /// the caller wrote it            withdrawing one's own record
    /// the caller represents the actor  ActorBinding says so
    /// ```
    ///
    /// A moderator who holds neither may exclude the Assertion from recall, but
    /// must not record it as *the source having retracted* — that would be the
    /// engine stating something about the source that never happened, which is
    /// the dishonesty §14.1 exists to forbid.
    pub fn may_represent_assertion(&self, row: &AssertionRow) -> bool {
        let wrote_it = row
            .origin
            .get("principal_id")
            .and_then(Json::as_str)
            .is_some_and(|principal| principal == self.auth.principal_id);
        wrote_it || self.authority.is_bound_to_actor(&row.asserted_by_key)
    }

    /// Carries classification and authority lineage onto derived elements.
    ///
    /// Runs at commit rather than per clause because a mutation block is
    /// declarative: an Activity may list its outputs after the clause that
    /// created them, so the derivation links only all exist once planning is
    /// finished (§21–§24).
    ///
    /// Two things travel along those links, in opposite directions:
    ///
    /// ```text
    /// classification   joins upward    the output is at least as restricted
    /// authority        recorded        the ceiling it may later be raised to
    /// ```
    ///
    /// Classification is applied here because it must be right the moment the
    /// element becomes readable — *read secret Evidence, summarize, write
    /// public summary* is an exfiltration path if the summary lands public even
    /// briefly (§31.2). Authority is only *recorded* here,
    /// because everything is created at the bottom of the ladder and cannot
    /// exceed anything; the lineage is what
    /// [`elevate_authority`](crate::governance::element::elevate_authority)
    /// reads when somebody asks to raise it.
    async fn propagate_governance(&mut self) -> Result<(), KipError> {
        let sources = self.material_inputs();
        for (id, inputs) in sources {
            let Some(staged) = self.staged.get(&id) else {
                continue;
            };
            if !staged.is_new {
                continue;
            }
            let inherited = self.join_classification(&inputs).await?;
            let Some(staged) = self.staged.get(&id) else {
                continue;
            };
            let own = staged.row.classification().to_string();
            let default = self.authority.default_classification().to_string();
            let effective = if own.is_empty() { &default } else { &own };
            let raised = crate::governance::classification::join(effective, &inherited).to_string();

            let Some(staged) = self.staged.get_mut(&id) else {
                continue;
            };
            let envelope = staged.row.governance_mut();
            let mut block = envelope
                .as_object()
                .cloned()
                .unwrap_or_else(serde_json::Map::new);
            if raised != default {
                block.insert("classification".to_string(), Json::from(raised.as_str()));
            }
            if !inputs.is_empty() {
                block.insert(
                    crate::governance::element::LINEAGE_KEY.to_string(),
                    Json::Array(inputs.iter().map(|id| Json::from(id.to_string())).collect()),
                );
            }
            if !block.is_empty() {
                *envelope = Json::Object(block);
            }
        }
        Ok(())
    }

    /// The material inputs of every new element this transaction stages (§31.2).
    ///
    /// Deliberately conservative about what counts: an Assertion's cited
    /// Evidence and context, an Evidence record's sources, an Activity's
    /// inputs, and — walking the other way — the inputs of any Activity that
    /// lists the element as an output. §31.2 allows a policy to distinguish a
    /// material content dependency from a control input, and says that when it
    /// is uncertain the restrictive reading wins. This engine has no such
    /// policy, so it takes the restrictive reading throughout.
    fn material_inputs(&self) -> Vec<(ElementId, Vec<ElementId>)> {
        let local = |value: &Json| -> Option<ElementId> {
            crate::term::Endpoint::from_json(value)
                .ok()
                .and_then(|endpoint| endpoint.local())
        };
        let mut by_output: BTreeMap<ElementId, BTreeSet<ElementId>> = BTreeMap::new();
        for (id, staged) in &self.staged {
            let mut inputs: BTreeSet<ElementId> = BTreeSet::new();
            match &staged.row {
                Element::Assertion(row) => {
                    for reference in &row.evidence_ids {
                        if let Ok(id) = reference.parse::<ElementId>() {
                            inputs.insert(id);
                        }
                    }
                    inputs.extend(row.context_refs.iter().filter_map(local));
                }
                Element::Evidence(row) => {
                    inputs.extend(row.source_refs.iter().filter_map(local));
                }
                Element::Activity(row) => {
                    inputs.extend(row.inputs.iter().filter_map(local));
                }
                _ => {}
            }
            by_output.entry(*id).or_default().extend(inputs);
        }
        // An Activity's outputs inherit from its inputs, which is the general
        // shape of "this was produced from that" — the link a summarizer or a
        // consolidation actually leaves behind.
        for staged in self.staged.values() {
            if let Element::Activity(row) = &staged.row {
                let inputs: Vec<ElementId> = row.inputs.iter().filter_map(local).collect();
                for output in row.outputs.iter().filter_map(local) {
                    by_output
                        .entry(output)
                        .or_default()
                        .extend(inputs.iter().copied());
                }
            }
        }
        by_output
            .into_iter()
            .map(|(id, inputs)| {
                let mut inputs: Vec<ElementId> = inputs.into_iter().collect();
                inputs.retain(|input| *input != id);
                (id, inputs)
            })
            .filter(|(_, inputs)| !inputs.is_empty())
            .collect()
    }

    /// The join of the classifications of a set of inputs.
    ///
    /// Inputs come from two places and both matter: an element this same
    /// transaction staged, and one that was already committed. Reading only the
    /// staged ones would make propagation work inside a single `MUTATE` and
    /// silently stop working the moment the Evidence was written earlier — which
    /// is the ordinary case.
    ///
    /// An input that is not there at all is read as the Space default rather
    /// than as unclassified: §31.1 forbids letting absence mean `public`.
    async fn join_classification(&self, inputs: &[ElementId]) -> Result<String, KipError> {
        let default = self.authority.default_classification().to_string();
        let mut joined = default.clone();
        for input in inputs {
            let label = match self.staged.get(input) {
                Some(staged) => staged.row.classification().to_string(),
                None => match self.store.get_element(*input).await {
                    Ok(element) => element.classification().to_string(),
                    Err(_) => String::new(),
                },
            };
            let label = if label.is_empty() {
                default.clone()
            } else {
                label
            };
            joined = crate::governance::classification::join(&joined, &label).to_string();
        }
        Ok(joined)
    }

    /// Marks a staged element as actually changed.
    ///
    /// Separate from [`Self::load`] because loading is not modifying: a clause
    /// that reads an element and decides to do nothing must not burn a version.
    pub fn mark_changed(&mut self, id: ElementId, op: ChangeOp) {
        if let Some(staged) = self.staged.get_mut(&id) {
            staged.changed = true;
            if !staged.is_new {
                staged.op = op;
            }
        }
    }

    /// Checks every `EXPECT VERSION` guard of one statement against the
    /// pre-transaction counters (§35.1).
    ///
    /// The comparison is against the version the element had when the
    /// transaction started, not a value this transaction produced: a guard is
    /// a statement about what the caller believed, and the caller could not
    /// have seen a version that does not exist yet. A plane guard reads that
    /// plane's own counter, so a write to another plane of the same element
    /// does not spoil it; `EXPECT VERSION 0 OF <plane>` therefore passes
    /// exactly when the plane has never been written (§35.2). A mismatch names
    /// the plane in `details.plane`.
    pub async fn expect_versions(
        &mut self,
        id: ElementId,
        guards: &[Guard],
    ) -> Result<(), KipError> {
        if guards.is_empty() {
            return Ok(());
        }
        self.load(id).await?;
        let staged = self.staged.get(&id).expect("loaded above");
        let (version, planes) = match &staged.before {
            Some(before) => (before.version(), before.plane_versions()),
            None => (0, PlaneVersions::default()),
        };
        for guard in guards {
            let actual = guard.plane.counter(version, &planes);
            if actual == guard.version {
                continue;
            }
            return Err(match &guard.plane {
                PlaneKey::Element => KipError::version_conflict(format!(
                    "{id} is at version {actual}, not the expected {}",
                    guard.version
                )),
                plane => KipError::version_conflict_on_plane(
                    &plane.name(),
                    format!(
                        "{id}'s {} plane is at version {actual}, not the expected {}",
                        plane.name(),
                        guard.version
                    ),
                ),
            });
        }
        Ok(())
    }

    /// Commits everything staged, or reports what a dry run would have done.
    ///
    /// A dry run never establishes a durable cognitive commit (§69.3), so it
    /// removes its own shells and journals nothing.
    pub async fn commit(mut self, entry: JournalEntry) -> Result<Outcome, KipError> {
        if let Err(error) = self.capture_cognitive_contracts().await {
            self.discard_shells().await;
            return Err(error);
        }
        if self.dry_run {
            let changes: Vec<Json> = self
                .prepared_changes()
                .into_iter()
                .map(|(_, prepared)| entry_json(&prepared.entry))
                .collect();
            let change_summary = summarize(&changes);
            self.discard_shells().await;
            let receipt = Receipt {
                status: ReceiptStatus::NoEffect,
                tx_id: Some(self.cx.tx_id.clone()),
                space_id: Some(self.cx.space.clone()),
                snapshot_seq: Some(self.cx.seq.saturating_sub(1)),
                space_seq: None,
                committed_at: None,
                transaction_class: Some("cognitive".into()),
                request_digest: None,
                semantic_plan_digest: None,
                result_digest: None,
                schema_environment_version: Some(self.env.version),
                change_summary: Some(change_summary),
                proofs: vec![],
                receipt_digest: None,
                origin: Some(self.receipt_origin()),
                extensions: None,
            };
            return Ok(Outcome {
                receipt,
                handles: self.handles,
                changes,
                warnings: self.warnings,
            });
        }

        self.propagate_governance().await?;
        self.check_reference_closure().await?;
        self.check_concept_key_identity().await?;

        // Nothing this transaction touched keeps its shell state, and the
        // version rule is applied here so that a clause touching one element
        // five times still produces one increment.
        let identity_changed = self
            .staged
            .values()
            .any(|s| s.changed && s.op == ChangeOp::Merge);
        if identity_changed {
            let mut space = self.store.get_space(&self.cx.space).await?;
            let mut versions = space.policies["_kip_identity_changes"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            versions.push(Json::from(self.cx.seq));
            if !space.policies.is_object() {
                space.policies = serde_json::json!({});
            }
            space.policies["_kip_identity_changes"] = Json::Array(versions);
            self.store.put_space(&space).await?;
        }
        let prepared = self.prepared_changes();
        let mut changes = Vec::with_capacity(prepared.len());
        let mut written = 0usize;
        for (id, prepared) in prepared {
            let staged = self.staged.remove(&id).expect("prepared from staged");
            let mut row = staged.row;
            row.set_plane_versions(&prepared.planes);
            if let Some(versions) = self.purges.get(&id) {
                self.store.remove_versions(versions).await?;
            }
            if let Some(versions) = self.payload_purges.get(&id) {
                self.store.scrub_payload_versions(versions).await?;
            }
            self.write(
                id,
                row,
                prepared.entry.new_version,
                prepared.entry.op,
                staged.is_new,
                staged.keep_origin,
            )
            .await?;
            changes.push(entry_json(&prepared.entry));
            written += 1;
        }
        self.staged.clear();

        // A shell nobody staged is a handle that was declared and never
        // filled in — a planning bug rather than data, so it is removed
        // instead of committed half-formed.
        self.discard_unstaged_shells(&changes).await;

        let status = if written == 0 {
            ReceiptStatus::NoEffect
        } else {
            ReceiptStatus::Committed
        };
        // The response body, journalled rather than only returned: it is what
        // a resend under the same idempotency key replays, and a journal that
        // recorded the key but not the answer would let a caller find its
        // transaction and still not learn what it bound (§34, §33).
        let result = result_body(&self.handles, &changes);
        let journalled = self
            .store
            .journal(
                &self.cx,
                JournalEntry {
                    status: receipt_status_name(status).to_string(),
                    transaction_class: "cognitive".to_string(),
                    schema_environment_version: self.env.version,
                    changes: changes.clone(),
                    result,
                    // §33.2, §80.4: journalled so a resend replays the Receipt
                    // the first attempt produced rather than one rebuilt from
                    // whoever resent it.
                    origin: serde_json::to_value(self.receipt_origin()).unwrap_or(Json::Null),
                    ..entry
                },
            )
            .await?;
        for entry in std::mem::take(&mut self.governance_audit) {
            self.store.governance.record_mutation(entry).await?;
        }
        for approved in std::mem::take(&mut self.approval_decisions) {
            approved.spend(&self.store).await?;
        }
        self.store.flush(now_ms()).await?;

        // §32.8: a transaction that changed nothing reports no cognitive
        // sequence, however the journal records that it ran.
        let committed = status == ReceiptStatus::Committed;
        let receipt = Receipt {
            status,
            tx_id: Some(journalled.tx_id),
            space_id: Some(self.cx.space.clone()),
            snapshot_seq: Some(journalled.snapshot_seq),
            space_seq: committed.then_some(journalled.seq),
            committed_at: Some(journalled.committed_at),
            transaction_class: Some(journalled.transaction_class),
            request_digest: none_if_empty(journalled.request_digest),
            semantic_plan_digest: none_if_empty(journalled.semantic_plan_digest),
            result_digest: none_if_empty(journalled.result_digest),
            schema_environment_version: Some(self.env.version),
            change_summary: Some(summarize(&changes)),
            proofs: vec![],
            receipt_digest: None,
            origin: Some(self.receipt_origin()),
            extensions: None,
        };

        Ok(Outcome {
            receipt,
            handles: self.handles,
            changes,
            warnings: self.warnings,
        })
    }

    pub(crate) fn record_reference(&mut self, supplied: &str, resolved: &str) {
        let identity_version = self.authority.space.policies["_kip_identity_changes"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Json::as_u64)
            .max()
            .unwrap_or(0);
        self.reference_bindings.push(serde_json::json!({
            "supplied": supplied, "resolved": resolved, "identity_version": identity_version,
            "path": format!("references[{}]", self.reference_bindings.len()), "op_id": self.cx.tx_id,
        }));
    }

    async fn capture_cognitive_contracts(&mut self) -> Result<(), KipError> {
        for staged in self
            .staged
            .values_mut()
            .filter(|s| s.changed && s.op != ChangeOp::Purge)
        {
            if !self.reference_bindings.is_empty() {
                let origin = staged.row.envelope_mut().origin;
                if !origin.is_object() {
                    *origin = serde_json::json!({});
                }
                if !origin["_kip_runtime"].is_object() {
                    origin["_kip_runtime"] = serde_json::json!({});
                }
                origin["_kip_runtime"]["input_references"] =
                    Json::Array(self.reference_bindings.clone());
            }
        }
        let pending: Vec<_> = self
            .staged
            .iter()
            .filter(|(_, s)| s.changed)
            .map(|(id, s)| (*id, s.row.clone(), s.before.clone()))
            .collect();
        for (id, element, before) in pending {
            if element.state() == state::PURGED {
                continue;
            }
            let view = crate::view::render(&element);
            let old_view = before.as_ref().map(crate::view::render);
            crate::schema::contracts::validate_record(&self.env, &view, old_view.as_ref())?;
            let Element::Activity(activity) = element else {
                continue;
            };
            if !matches!(
                activity.status.as_str(),
                "completed" | "failed" | "cancelled"
            ) {
                continue;
            }
            if matches!(before, Some(Element::Activity(ref row)) if matches!(row.status.as_str(), "completed" | "failed" | "cancelled"))
            {
                continue;
            }
            let contract = activity
                .facets
                .iter()
                .find(|(name, _)| name.ends_with("/DependencyBasis"))
                .map(|(_, value)| value);
            let mut inputs = Map::new();
            if let Some(contract) = contract {
                let seq = contract["basis_seq"].as_u64().ok_or_else(|| {
                    KipError::constraint_violation("DependencyBasis needs basis_seq")
                })?;
                if seq > self.cx.seq.saturating_sub(1) {
                    return Err(KipError::constraint_violation(
                        "DependencyBasis cannot name a future snapshot",
                    ));
                }
                for group in contract["groups"].as_array().into_iter().flatten() {
                    for pin in group["pins"].as_array().into_iter().flatten() {
                        let source = pin["id"].as_str().unwrap_or("").parse::<ElementId>()?;
                        let expected = pin["version"].as_u64().unwrap_or(0);
                        let retained =
                            if let Some(staged) = self.staged.get(&source).filter(|s| s.is_new) {
                                let mut value = crate::view::render(&staged.row);
                                value["_system"]["version"] = Json::from(1);
                                value["_system"]["plane_versions"] =
                                    serde_json::to_value(planes::initial(&staged.row)).unwrap();
                                value
                            } else {
                                let row = self
                                    .store
                                    .element_at(&self.cx.space, source, seq)
                                    .await?
                                    .ok_or_else(|| {
                                        KipError::constraint_violation(
                                            "DependencyBasis source version is unavailable",
                                        )
                                    })?;
                                self.authority
                                    .authorize(
                                        Permission::Read,
                                        &crate::governance::ResourceContext::of_element(&row),
                                        &self.auth,
                                    )
                                    .into_result()?;
                                crate::view::render(&row)
                            };
                        if retained["_system"]["version"].as_u64() != Some(expected) {
                            return Err(KipError::version_conflict(
                                "DependencyBasis must pin the version actually read",
                            ));
                        }
                        if let Some(pins) = pin["planes"].as_object() {
                            for (plane, version) in pins {
                                if crate::schema::contracts::pinned_plane(
                                    &retained["_system"]["plane_versions"],
                                    plane,
                                ) != version.as_u64()
                                {
                                    return Err(KipError::version_conflict(
                                        "DependencyBasis plane pin does not match retained input",
                                    ));
                                }
                            }
                        }
                        if let Some(old) = inputs.insert(source.to_string(), Json::from(expected))
                            && old != expected
                        {
                            return Err(KipError::constraint_violation(
                                "conflicting DependencyBasis pins",
                            ));
                        }
                    }
                }
            }
            for reference in &activity.inputs {
                let Some(source) = reference_id(reference) else {
                    continue;
                };
                if inputs.contains_key(&source.to_string()) {
                    continue;
                }
                if contract.is_some() {
                    return Err(KipError::constraint_violation(
                        "derived Activity input is missing its read pin",
                    ));
                }
                let version = if let Some(staged) = self.staged.get(&source) {
                    staged.before.as_ref().map_or(1, Element::version)
                } else {
                    self.store.get_element(source).await?.version()
                };
                inputs.insert(source.to_string(), Json::from(version));
            }
            let mut outputs = Map::new();
            for reference in &activity.outputs {
                let Some(target) = reference_id(reference) else {
                    continue;
                };
                let version = if let Some(staged) = self.staged.get(&target).filter(|s| s.changed) {
                    prepare(target, staged).entry.new_version
                } else {
                    self.store.get_element(target).await?.version()
                };
                outputs.insert(target.to_string(), Json::from(version));
            }
            if let Element::Activity(row) = &mut self.staged.get_mut(&id).unwrap().row {
                if !row.origin.is_object() {
                    row.origin = serde_json::json!({});
                }
                if !row.origin["_kip_runtime"].is_object() {
                    row.origin["_kip_runtime"] = serde_json::json!({});
                }
                row.origin["_kip_runtime"]["input_versions"] = Json::Object(inputs);
                row.origin["_kip_runtime"]["output_versions"] = Json::Object(outputs);
            }
        }
        Ok(())
    }

    /// Who this commit is attributed to (§33.2).
    fn receipt_origin(&self) -> ReceiptOrigin {
        ReceiptOrigin {
            principal_id: self.auth.principal_id.clone(),
            actor_binding_id: self.exercised_binding.clone(),
            delegation_digest: delegation_digest(&self.auth.delegation_chain),
        }
    }

    /// Abandons everything staged, removing the shells this run minted.
    ///
    /// Not a rollback in the durable sense — there is no log to unwind — but
    /// the only durable thing a failed statement wrote is its shells, and they
    /// were never visible.
    pub async fn abort(mut self) {
        self.discard_shells().await;
    }

    /// Writes one staged row, stamping the engine truth the transaction owns.
    ///
    /// A staged row for a *new* element was built from scratch by a clause, so
    /// it carries none of the envelope; a staged row for an existing element
    /// was loaded and already has its creation coordinates, which must not be
    /// refreshed (they are the only engine-side record of when the element
    /// entered the Nexus).
    async fn write(
        &self,
        id: ElementId,
        row: Element,
        version: u64,
        op: ChangeOp,
        is_new: bool,
        keep_origin: bool,
    ) -> Result<(), KipError> {
        let op = op_name(op);
        macro_rules! put {
            ($row:expr) => {{
                let mut row = *$row;
                row._id = id.seq;
                row.space = self.cx.space.clone();
                row.version = version;
                row.seq = self.cx.seq;
                row.updated_at = self.cx.at.clone();
                row.updated_tx = self.cx.tx_id.clone();
                // A purge keeps the origin it had. Every other write records
                // who the runtime observed (§2.5), but the whole point of an
                // identity stub is that an auditor can still say something was
                // here and who wrote it — and the version log that would
                // otherwise answer that has just been destroyed.
                if !keep_origin {
                    let runtime = row.origin.get("_kip_runtime").cloned();
                    row.origin = self.cx.origin.clone();
                    if let Some(runtime) = runtime {
                        row.origin["_kip_runtime"] = runtime;
                    }
                }
                if is_new {
                    row.created_at = self.cx.at.clone();
                    row.created_tx = self.cx.tx_id.clone();
                }
                if row.state.is_empty() || row.state == state::PENDING {
                    row.state = state::ACTIVE.to_string();
                }
                self.store.put(&row).await?;
                // The version log is appended in the same commit as the row it
                // records. A history written afterwards can be missing the
                // last write a crash interrupted, and a history with a hole in
                // it answers `AS OF` wrongly rather than refusing.
                self.store
                    .record_version(&self.cx, id, version, op, &row)
                    .await?;
            }};
        }
        match row {
            Element::Concept(row) => put!(row),
            Element::Proposition(row) => put!(row),
            Element::Assertion(row) => put!(row),
            Element::Evidence(row) => put!(row),
            Element::Activity(row) => put!(row),
        }
        Ok(())
    }

    async fn discard_shells(&mut self) {
        for id in std::mem::take(&mut self.shells) {
            // Best effort: a shell that survives is inert and swept on open.
            let _ = self.store.elements(id.kind).remove(id.seq).await;
        }
    }

    async fn discard_unstaged_shells(&mut self, changes: &[Json]) {
        let written: BTreeSet<String> = changes
            .iter()
            .filter_map(|change| change.get("id")?.as_str().map(str::to_string))
            .collect();
        let shells = std::mem::take(&mut self.shells);
        for id in shells {
            if !written.contains(&id.to_string()) {
                let _ = self.store.elements(id.kind).remove(id.seq).await;
            }
        }
    }

    /// The change entry and counters each changed element will commit with.
    ///
    /// One place computes both, from the same diff, so the counter a later
    /// guard compares against and the `planes` a Watch reads off the envelope
    /// cannot disagree.
    fn prepared_changes(&self) -> Vec<(ElementId, Prepared)> {
        self.staged
            .iter()
            .filter(|(_, staged)| staged.changed)
            .map(|(id, staged)| (*id, prepare(*id, staged)))
            .collect()
    }
}

/// One element's change, ready to commit.
struct Prepared {
    entry: ChangeEntry,
    planes: PlaneVersions,
}

/// Diffs one staged element into its Change Envelope entry and its counters
/// after the commit (§6.3, §36.1).
fn prepare(id: ElementId, staged: &Staged) -> Prepared {
    let (old_version, touched, planes) = match (&staged.before, staged.is_new) {
        (Some(before), false) => {
            let touched = planes::diff(before, &staged.row);
            let planes = touched.advance(before.plane_versions());
            (Some(before.version()), touched, planes)
        }
        _ => (None, Touched::default(), planes::initial(&staged.row)),
    };
    let new_version = old_version.map_or(1, |version| version.saturating_add(1));

    let state = match staged.op {
        ChangeOp::Lifecycle => Some(ChangeState {
            from: staged
                .before
                .as_ref()
                .map(planes::lifecycle_state)
                .unwrap_or_else(|| state::ACTIVE.to_string()),
            to: planes::lifecycle_state(&staged.row),
        }),
        _ => None,
    };

    let mut refs = ChangeRefs::default();
    let mut schema_ref = None;
    match &staged.row {
        Element::Concept(row) => {
            if !row.schema_ref.is_empty() {
                schema_ref = Some(row.schema_ref.clone());
            }
            if staged.op == ChangeOp::Merge && !row.merged_into.is_empty() {
                refs.merged_into = Some(row.merged_into.clone());
            }
        }
        Element::Proposition(row) => {
            refs.subject = row
                .subject
                .get("id")
                .and_then(Json::as_str)
                .map(str::to_string);
            if !row.predicate_ref.is_empty() {
                refs.predicate_ref = Some(row.predicate_ref.clone());
            }
        }
        Element::Assertion(row) => {
            if !row.proposition_id.is_empty() {
                refs.proposition = Some(row.proposition_id.clone());
            }
        }
        Element::Evidence(_) | Element::Activity(_) => {}
    }
    let has_refs = refs.proposition.is_some()
        || refs.subject.is_some()
        || refs.predicate_ref.is_some()
        || refs.merged_into.is_some();

    // §36.1 asks for the counters of each plane the entry touched. The wire
    // type carries the three named planes unconditionally, so the entry
    // reports the element's complete counters after the commit whenever any
    // plane moved, and `touched` says which; a creation reports none, its
    // counters being implied by the content it was created with.
    let report_planes = staged.op != ChangeOp::Create && touched.any_plane();
    let entry = ChangeEntry {
        op: staged.op,
        kind: id.kind,
        id: id.to_string(),
        schema_ref,
        old_version,
        new_version,
        state,
        refs: has_refs.then_some(refs),
        touched: touched.paths.clone(),
        planes: report_planes.then(|| planes::to_wire(&planes)),
        extensions: None,
    };
    Prepared { entry, planes }
}

/// A change entry as the journal and the result body carry it.
pub(crate) fn entry_json(entry: &ChangeEntry) -> Json {
    serde_json::to_value(entry).unwrap_or(Json::Null)
}

/// The version-log spelling of an operation.
pub(crate) fn op_name(op: ChangeOp) -> &'static str {
    match op {
        ChangeOp::Create => "create",
        ChangeOp::Update => "update",
        ChangeOp::Lifecycle => "lifecycle",
        ChangeOp::Retention => "retention",
        ChangeOp::Merge => "merge",
        ChangeOp::Purge => "purge",
        ChangeOp::PayloadPurge => "payload_purge",
    }
}

/// Seals a Receipt with its canonical digest (§33.2).
///
/// The digest covers the members §33.2 names — the Receipt without
/// `receipt_digest`, `proofs` and the namespaced `extensions` — over the same
/// RFC 8785 canonical JSON and sha3-256 a Capsule digests under (§37.7), so a
/// signed Receipt (§33.3) has one thing to sign.
///
/// `extensions` is outside the digest on purpose, and it is what makes sealing
/// order-independent: the governance provenance this engine attaches to an
/// audited commit is not journalled, so a replay under the same idempotency
/// key cannot rebuild it (§80.4). A digest that covered it would make the
/// recovered Receipt differ from the sealed one for exactly the transactions
/// — purge, tombstone, every always-audited permission — where an auditor
/// most needs the two to agree.
pub fn seal_receipt(mut receipt: Receipt) -> Receipt {
    receipt.receipt_digest = Some(receipt_digest(&receipt));
    receipt
}

/// The digest a Receipt is sealed with, and `VERIFY RECEIPT` recomputes.
pub(crate) fn receipt_digest(receipt: &Receipt) -> String {
    let mut bare = receipt.clone();
    bare.receipt_digest = None;
    bare.proofs.clear();
    bare.extensions = None;
    let value = serde_json::to_value(&bare).unwrap_or(Json::Null);
    digest_of(&value)
}

/// The digest of a delegation chain, when the request ran under one (§28.5).
fn delegation_digest(chain: &[String]) -> Option<String> {
    if chain.is_empty() {
        return None;
    }
    Some(digest_of(&Json::Array(
        chain
            .iter()
            .map(|link| Json::String(link.clone()))
            .collect(),
    )))
}

/// sha3-256 over RFC 8785 canonical JSON, spelled as the Capsule digest is.
pub(crate) fn digest_of(value: &Json) -> String {
    use sha3::{Digest, Sha3_256};
    let canonical = anda_kip::canonical_json(value);
    format!(
        "{}:{}",
        "sha3-256",
        hex::encode(Sha3_256::digest(canonical.as_bytes()))
    )
}

/// What a committed (or previewed) transaction produced.
pub struct Outcome {
    /// The receipt a caller uses to recover a lost response.
    pub receipt: Receipt,
    /// Every handle this mutation bound, mapped to the element it named.
    pub handles: BTreeMap<String, ElementId>,
    /// One Change Envelope entry per changed element (§36.1).
    pub changes: Vec<Json>,
    /// Non-fatal caveats.
    pub warnings: Vec<String>,
}

impl Outcome {
    /// The result body a KML response carries.
    pub fn result(&self) -> Json {
        result_body(&self.handles, &self.changes)
    }
}

/// The result body a KML response carries, and the journal records.
///
/// One function, so the answer a caller gets and the answer a replay gets are
/// the same shape by construction rather than by two call sites agreeing.
fn result_body(handles: &BTreeMap<String, ElementId>, changes: &[Json]) -> Json {
    let mut bound = Map::new();
    for (handle, id) in handles {
        bound.insert(handle.clone(), Json::String(id.to_string()));
    }
    serde_json::json!({
        "handles": bound,
        "changes": changes,
    })
}

/// The `change_summary` a receipt carries.
///
/// `pub(crate)` because a replayed receipt has to carry the same summary the
/// original did — reconstructing it from the journal through a second
/// expression is how the two would come to differ.
pub(crate) fn summarize(changes: &[Json]) -> Json {
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    for change in changes {
        if let Some(op) = change.get("op").and_then(Json::as_str) {
            *counts.entry(op.to_string()).or_default() += 1;
        }
    }
    serde_json::json!({"elements": changes.len(), "by_op": counts})
}

fn receipt_status_name(status: ReceiptStatus) -> &'static str {
    match status {
        ReceiptStatus::Committed => "committed",
        ReceiptStatus::Aborted => "aborted",
        ReceiptStatus::NoEffect => "no_effect",
        ReceiptStatus::Pending => "pending",
        ReceiptStatus::Unknown => "unknown",
    }
}

pub(crate) fn none_if_empty(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

fn now_ms() -> u64 {
    anda_db::unix_ms()
}

impl Store {
    /// Writes a row verbatim, without touching its version.
    ///
    /// The transaction owns version assignment, so the ordinary
    /// [`Store::update`] — which bumps — is the wrong primitive at commit time.
    pub async fn put<R: Row>(&self, row: &R) -> Result<(), KipError> {
        self.put_row(row).await
    }

    /// Removes every element still wearing the `pending` state.
    ///
    /// A pending element belongs to no committed transaction: it was minted as
    /// a shell by a run that crashed before commit. Nothing ever read it, so
    /// removing it is the whole of the recovery.
    pub async fn sweep_pending(&self) -> Result<usize, KipError> {
        let mut removed = 0;
        for kind in [
            ElementKind::Concept,
            ElementKind::Proposition,
            ElementKind::Assertion,
            ElementKind::Evidence,
            ElementKind::Activity,
        ] {
            let collection = self.elements(kind);
            let ids = collection
                .query_all_ids(crate::store::eq_field(
                    "state",
                    anda_db_schema::Fv::Text(state::PENDING.to_string()),
                ))
                .await
                .map_err(db_error)?;
            for id in ids {
                collection.remove(id).await.map_err(db_error)?;
                removed += 1;
            }
        }
        Ok(removed)
    }
}

fn reference_id(value: &Json) -> Option<ElementId> {
    value
        .as_str()
        .or_else(|| value.get("id").and_then(Json::as_str))
        .and_then(|id| id.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_receipt_digest_ignores_its_own_slot_and_the_proofs() {
        let receipt = Receipt {
            status: ReceiptStatus::Committed,
            tx_id: Some("kip:space:default#3".into()),
            space_id: Some("kip:space:default".into()),
            snapshot_seq: Some(2),
            space_seq: Some(3),
            committed_at: Some("2026-09-03T00:00:00.000Z".into()),
            transaction_class: Some("cognitive".into()),
            request_digest: None,
            semantic_plan_digest: None,
            result_digest: None,
            schema_environment_version: Some(1),
            change_summary: None,
            proofs: vec![],
            receipt_digest: None,
            origin: Some(ReceiptOrigin {
                principal_id: "kip:principal:system".into(),
                actor_binding_id: None,
                delegation_digest: None,
            }),
            extensions: None,
        };
        let sealed = seal_receipt(receipt.clone());
        let digest = sealed.receipt_digest.clone().expect("a digest");
        assert!(digest.starts_with("sha3-256:"));
        // Sealing twice, or adding a proof, does not move the digest.
        let mut signed = sealed.clone();
        signed
            .proofs
            .push(serde_json::json!({"proof_type": "signature"}));
        assert_eq!(seal_receipt(signed).receipt_digest, Some(digest.clone()));
        assert_eq!(
            seal_receipt(sealed.clone()).receipt_digest,
            Some(digest.clone())
        );
        // Nor does the governance provenance an audited commit carries: a
        // replay cannot rebuild it, and a digest that moved with it would make
        // the recovered Receipt differ from the one that was sealed.
        let mut audited = sealed;
        audited.extensions.get_or_insert_with(Map::new).insert(
            "governance".to_string(),
            serde_json::json!({"principal_id": "kip:principal:system"}),
        );
        assert_eq!(seal_receipt(audited).receipt_digest, Some(digest));
    }

    #[test]
    fn a_delegation_chain_digests_only_when_there_is_one() {
        assert_eq!(delegation_digest(&[]), None);
        let one = delegation_digest(&["kip:delegation:1".into()]).unwrap();
        let two =
            delegation_digest(&["kip:delegation:1".into(), "kip:delegation:2".into()]).unwrap();
        assert_ne!(one, two);
    }
}
