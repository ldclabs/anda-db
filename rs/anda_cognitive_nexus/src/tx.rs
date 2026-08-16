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
//! **An element's version increments once per transaction** (§44), no matter
//! how many clauses touched it. A transaction is one externally visible state
//! transition, and `EXPECT VERSION`, audit and the change stream all read that
//! counter. So versions are assigned here, at commit, not by each write.
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

use anda_kip::{ElementKind, Json, KipError, Map, Receipt, ReceiptStatus};
use std::collections::{BTreeMap, BTreeSet};

use crate::error::db_error;
use crate::id::ElementId;
use crate::schema::SchemaEnvironment;
use crate::store::rows::*;
use crate::store::space::JournalEntry;
use crate::store::write::{Row, WriteContext};
use crate::store::{Element, Store};
use crate::time;

/// The engine state one KML statement runs against.
pub struct Transaction {
    store: Store,
    /// The engine truth stamped on everything this transaction writes.
    pub cx: WriteContext,
    /// The Schema Environment this transaction is bound to.
    ///
    /// Captured once at the start: a transaction evaluates against one
    /// consistent environment snapshot (§240.45), so an activation racing
    /// alongside cannot change what half of it means.
    pub env: SchemaEnvironment,
    /// Whether this run may become durable.
    pub dry_run: bool,
    handles: BTreeMap<String, ElementId>,
    staged: BTreeMap<ElementId, Staged>,
    shells: Vec<ElementId>,
    warnings: Vec<String>,
}

/// One element this transaction will write.
struct Staged {
    row: Element,
    is_new: bool,
    /// Whether the final state differs from what was there before.
    changed: bool,
    /// What the change record calls this.
    op: &'static str,
}

impl Transaction {
    /// Opens a transaction, allocating its Space sequence.
    pub async fn begin(
        store: &Store,
        space_id: &str,
        origin: Json,
        dry_run: bool,
    ) -> Result<Self, KipError> {
        let env = store.schema_environment(space_id).await?;
        let cx = store.begin_transaction(space_id, origin).await?;
        Ok(Self {
            store: store.clone(),
            cx,
            env,
            dry_run,
            handles: BTreeMap::new(),
            staged: BTreeMap::new(),
            shells: Vec::new(),
            warnings: Vec::new(),
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

    /// Declares a handle and mints the element it will name.
    ///
    /// The id has to exist before any clause is interpreted, because a clause
    /// may reference a handle a later clause declares. `anda_db` assigns ids at
    /// insert time and offers no way to reserve one, so the element is inserted
    /// now as a `pending` shell and filled in at commit.
    ///
    /// A handle may be declared exactly once (§25): two clauses binding `?x`
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

    /// Checks an `EXPECT STATE` guard against an Assertion's lifecycle status.
    ///
    /// Distinct from [`Self::expect_state`], which reads the *engine* state:
    /// an Assertion can be epistemically retracted while its record is
    /// perfectly active, and confusing the two would let a guard pass on the
    /// wrong question.
    pub async fn expect_assertion_status(
        &mut self,
        id: ElementId,
        expected: &str,
    ) -> Result<(), KipError> {
        let actual = match self.load(id).await? {
            Element::Assertion(row) => row.status.clone(),
            _ => {
                return Err(KipError::structural_reference_invalid(format!(
                    "{id} is not an Assertion"
                )));
            }
        };
        if actual != expected {
            return Err(KipError::precondition_failed(format!(
                "{id} is {actual:?}, not the expected {expected:?}"
            )));
        }
        Ok(())
    }

    /// Stages a newly created element's final row.
    pub fn stage_new(&mut self, id: ElementId, row: Element, op: &'static str) {
        self.staged.insert(
            id,
            Staged {
                row,
                is_new: true,
                changed: true,
                op,
            },
        );
    }

    /// Loads an existing element for modification, or returns the staged copy.
    ///
    /// Read-your-writes inside the transaction (§27): a clause that reads an
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
                    row,
                    is_new: false,
                    changed: false,
                    op: "update",
                },
            );
        }
        Ok(&mut self.staged.get_mut(&id).expect("just inserted").row)
    }

    /// Marks a staged element as actually changed.
    ///
    /// Separate from [`Self::load`] because loading is not modifying: a clause
    /// that reads an element and decides to do nothing must not burn a version.
    pub fn mark_changed(&mut self, id: ElementId, op: &'static str) {
        if let Some(staged) = self.staged.get_mut(&id) {
            staged.changed = true;
            if !staged.is_new {
                staged.op = op;
            }
        }
    }

    /// The version an element is at right now, for `EXPECT VERSION`.
    pub async fn current_version(&mut self, id: ElementId) -> Result<u64, KipError> {
        Ok(self.load(id).await?.version())
    }

    /// Checks an `EXPECT VERSION` guard against the pre-transaction version.
    ///
    /// The comparison is against the version the element had when the
    /// transaction started, not a value this transaction produced: a guard is
    /// a statement about what the caller believed, and the caller could not
    /// have seen a version that does not exist yet.
    pub async fn expect_version(&mut self, id: ElementId, expected: u64) -> Result<(), KipError> {
        let actual = self.current_version(id).await?;
        Store::expect_version(id, actual, expected)
    }

    /// Checks an `EXPECT STATE` guard.
    pub async fn expect_state(&mut self, id: ElementId, expected: &str) -> Result<(), KipError> {
        let actual = self.load(id).await?.state().to_string();
        if actual != expected {
            return Err(KipError::precondition_failed(format!(
                "{id} is in state {actual:?}, not the expected {expected:?}"
            )));
        }
        Ok(())
    }

    /// Commits everything staged, or reports what a dry run would have done.
    ///
    /// A dry run never establishes a durable cognitive commit (§69.3), so it
    /// removes its own shells and journals nothing.
    pub async fn commit(mut self, entry: JournalEntry) -> Result<Outcome, KipError> {
        if self.dry_run {
            let changes = self.change_records();
            let change_summary = summarize(&changes);
            self.discard_shells().await;
            return Ok(Outcome {
                receipt: Receipt {
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
                    extensions: None,
                },
                handles: self.handles,
                changes,
                warnings: self.warnings,
            });
        }

        // Nothing this transaction touched keeps its shell state, and the
        // version rule is applied here so that a clause touching one element
        // five times still produces one increment.
        let mut changes = Vec::new();
        let mut written = 0usize;
        for (id, staged) in std::mem::take(&mut self.staged) {
            if !staged.changed {
                continue;
            }
            let version = if staged.is_new {
                1
            } else {
                staged.row.version().saturating_add(1)
            };
            self.write(id, staged.row, version, staged.op, staged.is_new)
                .await?;
            changes.push(change_record(id, staged.op, version));
            written += 1;
        }

        // A shell nobody staged is a handle that was declared and never
        // filled in — a planning bug rather than data, so it is removed
        // instead of committed half-formed.
        self.discard_unstaged_shells(&changes).await;

        let status = if written == 0 {
            ReceiptStatus::NoEffect
        } else {
            ReceiptStatus::Committed
        };
        let journalled = self
            .store
            .journal(
                &self.cx,
                JournalEntry {
                    status: receipt_status_name(status).to_string(),
                    transaction_class: "cognitive".to_string(),
                    schema_environment_version: self.env.version,
                    changes: changes.clone(),
                    ..entry
                },
            )
            .await?;
        self.store.flush(now_ms()).await?;

        Ok(Outcome {
            receipt: Receipt {
                status,
                tx_id: Some(journalled.tx_id),
                space_id: Some(self.cx.space.clone()),
                snapshot_seq: Some(journalled.snapshot_seq),
                space_seq: Some(journalled.seq),
                committed_at: Some(journalled.committed_at),
                transaction_class: Some(journalled.transaction_class),
                request_digest: none_if_empty(journalled.request_digest),
                semantic_plan_digest: none_if_empty(journalled.semantic_plan_digest),
                result_digest: none_if_empty(journalled.result_digest),
                schema_environment_version: Some(self.env.version),
                change_summary: Some(summarize(&changes)),
                proofs: vec![],
                extensions: None,
            },
            handles: self.handles,
            changes,
            warnings: self.warnings,
        })
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
        op: &str,
        is_new: bool,
    ) -> Result<(), KipError> {
        macro_rules! put {
            ($row:expr) => {{
                let mut row = *$row;
                row._id = id.seq;
                row.space = self.cx.space.clone();
                row.version = version;
                row.seq = self.cx.seq;
                row.updated_at = self.cx.at.clone();
                row.updated_tx = self.cx.tx_id.clone();
                row.origin = self.cx.origin.clone();
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

    fn change_records(&self) -> Vec<Json> {
        self.staged
            .iter()
            .filter(|(_, staged)| staged.changed)
            .map(|(id, staged)| {
                let version = if staged.is_new {
                    1
                } else {
                    staged.row.version().saturating_add(1)
                };
                change_record(*id, staged.op, version)
            })
            .collect()
    }
}

/// What a committed (or previewed) transaction produced.
pub struct Outcome {
    /// The receipt a caller uses to recover a lost response.
    pub receipt: Receipt,
    /// Every handle this mutation bound, mapped to the element it named.
    pub handles: BTreeMap<String, ElementId>,
    /// One record per changed element.
    pub changes: Vec<Json>,
    /// Non-fatal caveats.
    pub warnings: Vec<String>,
}

impl Outcome {
    /// The result body a KML response carries.
    pub fn result(&self) -> Json {
        let mut handles = Map::new();
        for (handle, id) in &self.handles {
            handles.insert(handle.clone(), Json::String(id.to_string()));
        }
        serde_json::json!({
            "handles": handles,
            "changes": self.changes,
        })
    }
}

fn change_record(id: ElementId, op: &str, version: u64) -> Json {
    serde_json::json!({
        "id": id.to_string(),
        "kind": id.kind.to_string(),
        "op": op,
        "version": version,
    })
}

fn summarize(changes: &[Json]) -> Json {
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

fn none_if_empty(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

fn now_ms() -> u64 {
    time::parse(&time::now())
        .map(|at| at.timestamp_millis().max(0) as u64)
        .unwrap_or_default()
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
