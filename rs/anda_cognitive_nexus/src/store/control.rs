//! Protected state and crash recovery. The redo log is flushed before any
//! after-image is applied. Recovery is idempotent and runs under the Nexus lock.
use super::{Element, Store, eq_fields, rows::*, space::JournalEntry, write::WriteContext};
use crate::error::db_error;
use anda_db_schema::Fv;
use anda_kip::{Json, KipError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitPlan {
    pub cx: WriteContext,
    pub journal: JournalEntry,
    pub writes: Vec<(Element, String)>,
    pub controls: Vec<ControlRecordRow>,
    #[serde(default)]
    pub control_replacements: Vec<ControlRecordRow>,
    pub space: Option<SpaceRow>,
    pub purge_versions: Vec<u64>,
    pub scrub_versions: Vec<u64>,
    pub audits: Vec<crate::governance::rows::GovernanceAuditRow>,
    pub approvals: Vec<u64>,
}

impl CommitPlan {
    /// Whether applying this plan changes anything durable beyond its rows:
    /// what a no-op transaction must still write, or it writes nothing.
    pub fn has_durable_effect(&self) -> bool {
        !self.writes.is_empty()
            || !self.controls.is_empty()
            || !self.control_replacements.is_empty()
            || !self.purge_versions.is_empty()
            || !self.scrub_versions.is_empty()
            || !self.audits.is_empty()
            || !self.approvals.is_empty()
            || !self.journal.idempotency_key.is_empty()
    }
}

impl Store {
    pub async fn control_at(
        &self,
        space: &str,
        key: &str,
        seq: u64,
    ) -> Result<Option<ControlRecordRow>, KipError> {
        let table = self.control_records();
        let ids = table
            .query_all_ids(eq_fields(&[
                ("space", Fv::Text(space.into())),
                ("key", Fv::Text(key.into())),
            ]))
            .await
            .map_err(db_error)?;
        let mut found: Option<ControlRecordRow> = None;
        for id in ids {
            let row: ControlRecordRow = table.get_as(id).await.map_err(db_error)?;
            if row.seq <= seq
                && found
                    .as_ref()
                    .is_none_or(|old| (row.seq, row.version) > (old.seq, old.version))
            {
                found = Some(row);
            }
        }
        Ok(found)
    }

    /// The elements an identity withdrawal left under review at a coordinate
    /// (§11), keyed by element id.
    ///
    /// One ranged read of the review keys, so a read asks once instead of
    /// once per element — reviews are rare, and most Spaces have none.
    pub async fn identity_reviews(
        &self,
        space: &str,
        seq: u64,
    ) -> Result<std::collections::BTreeSet<String>, KipError> {
        const PREFIX: &str = "identity_review/";
        let table = self.control_records();
        let ids = table
            .query_all_ids(anda_db::query::Filter::And(vec![
                Box::new(super::eq_field("space", Fv::Text(space.into()))),
                Box::new(anda_db::query::Filter::Field((
                    "key".into(),
                    anda_db::query::RangeQuery::Between(
                        Fv::Text(PREFIX.into()),
                        Fv::Text(format!("{PREFIX}\u{10FFFF}")),
                    ),
                ))),
                Box::new(anda_db::query::Filter::Field((
                    "seq".into(),
                    anda_db::query::RangeQuery::Le(Fv::U64(seq)),
                ))),
            ]))
            .await
            .map_err(db_error)?;
        let mut reviews = std::collections::BTreeSet::new();
        for id in ids {
            let row: ControlRecordRow = table.get_as(id).await.map_err(db_error)?;
            if let Some(element) = row.key.strip_prefix(PREFIX) {
                reviews.insert(element.to_string());
            }
        }
        Ok(reviews)
    }

    pub async fn put_control(&self, row: &ControlRecordRow) -> Result<(), KipError> {
        let table = self.control_records();
        if table
            .query_all_ids(super::eq_field(
                "record_id",
                Fv::Text(row.record_id.clone()),
            ))
            .await
            .map_err(db_error)?
            .is_empty()
        {
            table.add_from(row).await.map_err(db_error)?;
        }
        Ok(())
    }

    /// Persist exactly one logical transaction, including its control effects.
    ///
    /// Every row the plan will write is validated first, so a plan that cannot
    /// be stored fails before anything durable happens rather than as a redo
    /// intent recovery could never finish. `owned` turns true once the intent
    /// row exists: from then on recovery, not the caller, owns the shells.
    pub async fn commit_plan(
        &self,
        plan: CommitPlan,
        owned: &mut bool,
    ) -> Result<TransactionRow, KipError> {
        self.preflight(&plan)?;
        // Reserved element ids must survive before a redo intent can name them.
        self.flush(crate::tx::now_ms()).await?;
        // Stored as JSON text: one value, however many rows the plan carries,
        // so the field's structural budget never bounds a transaction.
        let text =
            serde_json::to_string(&plan).map_err(|e| KipError::internal_error(e.to_string()))?;
        let id = self
            .commit_log()
            .add_from(&CommitLogRow {
                _id: 0,
                tx_id: plan.cx.tx_id.clone(),
                plan: Json::String(text),
            })
            .await
            .map_err(db_error)?;
        *owned = true;
        self.commit_log()
            .flush(crate::tx::now_ms())
            .await
            .map_err(db_error)?;
        self.apply_commit(&plan, false).await?;
        self.flush(crate::tx::now_ms()).await?;
        self.commit_log().remove(id).await.map_err(db_error)?;
        self.commit_log()
            .flush(crate::tx::now_ms())
            .await
            .map_err(db_error)?;
        self.find_transaction(&plan.cx.tx_id)
            .await?
            .ok_or_else(|| KipError::internal_error("committed journal missing"))
    }

    /// Checks every row `apply_commit` would write against its collection's
    /// schema, including the structural budget each field value carries.
    fn preflight(&self, plan: &CommitPlan) -> Result<(), KipError> {
        fn check<T: serde::Serialize>(
            collection: &anda_db::collection::Collection,
            row: &T,
            what: impl std::fmt::Display,
        ) -> Result<(), KipError> {
            anda_db_schema::Document::try_from(collection.schema(), row)
                .map(|_| ())
                .map_err(|err| {
                    KipError::resource_exhausted(format!(
                        "{what} is too large to store in one transaction ({err}); split the \
                         statement or the Capsule into smaller ones"
                    ))
                })
        }
        let versions = self.element_versions();
        for (element, op) in &plan.writes {
            let id = element.id();
            let collection = self.elements(id.kind);
            macro_rules! row {
                ($row:expr) => {{
                    check(&collection, $row.as_ref(), id)?;
                    let version = super::history::version_row(
                        &plan.cx,
                        id,
                        element.version(),
                        op,
                        $row.as_ref(),
                    )?;
                    check(&versions, &version, format_args!("the version of {id}"))?;
                }};
            }
            match element {
                Element::Concept(row) => row!(row),
                Element::Proposition(row) => row!(row),
                Element::Assertion(row) => row!(row),
                Element::Evidence(row) => row!(row),
                Element::Activity(row) => row!(row),
            }
        }
        let controls = self.control_records();
        for row in plan.controls.iter().chain(&plan.control_replacements) {
            check(&controls, row, format_args!("control record {}", row.key))?;
        }
        let (journal, _) = super::space::journal_row(&plan.cx, plan.journal.clone());
        check(
            &self.transactions(),
            &journal,
            "the transaction journal entry",
        )
    }

    pub async fn recover_commits(&self) -> Result<(), KipError> {
        let table = self.commit_log();
        let ids = table.ids();
        for id in ids {
            let row: CommitLogRow = table.get_as(id).await.map_err(db_error)?;
            // Written as JSON text; an intent from before that is the object.
            let plan: CommitPlan = match row.plan {
                Json::String(text) => serde_json::from_str(&text),
                value => serde_json::from_value(value),
            }
            .map_err(|e| KipError::internal_error(format!("invalid commit log: {e}")))?;
            self.apply_commit(&plan, true).await?;
            self.flush(crate::tx::now_ms()).await?;
            table.remove(id).await.map_err(db_error)?;
            table.flush(crate::tx::now_ms()).await.map_err(db_error)?;
        }
        Ok(())
    }

    /// Applies a plan's after-images. `replay` is a recovery pass, where any
    /// effect may already be durable and each one is written only if absent.
    async fn apply_commit(&self, plan: &CommitPlan, replay: bool) -> Result<(), KipError> {
        self.remove_versions(&plan.purge_versions).await?;
        self.scrub_payload_versions(&plan.scrub_versions).await?;
        for (element, op) in &plan.writes {
            macro_rules! put {
                ($row:expr) => {{
                    self.put($row.as_ref()).await?;
                    self.record_version(
                        &plan.cx,
                        element.id(),
                        element.version(),
                        op,
                        $row.as_ref(),
                        replay,
                    )
                    .await?;
                }};
            }
            match element {
                Element::Concept(row) => put!(row),
                Element::Proposition(row) => put!(row),
                Element::Assertion(row) => put!(row),
                Element::Evidence(row) => put!(row),
                Element::Activity(row) => put!(row),
            }
        }
        // §66.8, §60.7: an erased element's exposure entries go with it.
        for (element, op) in &plan.writes {
            if op == "purge" {
                self.remove_exposures(&element.id().to_string()).await?;
            }
        }
        let committed = plan.journal.status == "committed";
        if let Some(row) = &plan.space {
            // Raw write: the plan already owns the audit and notification.
            let mut row = row.clone();
            let current = self.get_space(&row.space_id).await?;
            row.seq = row.seq.max(current.seq);
            if committed {
                row.seq = row.seq.max(plan.cx.seq);
            }
            let epoch = current.policies["_kip_authorization_version"]
                .as_u64()
                .unwrap_or(0)
                .max(
                    row.policies["_kip_authorization_version"]
                        .as_u64()
                        .unwrap_or(0),
                );
            if epoch > 0 {
                if !row.policies.is_object() {
                    row.policies = serde_json::json!({});
                }
                row.policies["_kip_authorization_version"] = serde_json::json!(epoch);
            }
            self.spaces()
                .update(
                    row._id,
                    super::full_row_fields(self.spaces().schema(), &row)?,
                )
                .await
                .map_err(db_error)?;
        }
        if committed {
            // The commit takes its sequence with its rows (§32.8): a plan from
            // before this took it at `begin` already, and moving forward is
            // idempotent.
            let space = self.get_space(&plan.cx.space).await?;
            self.advance_seq(&space, plan.cx.seq).await?;
        }
        for row in &plan.controls {
            self.put_control(row).await?;
        }
        for row in &plan.control_replacements {
            self.control_records()
                .update(
                    row._id,
                    super::full_row_fields(self.control_records().schema(), row)?,
                )
                .await
                .map_err(db_error)?;
        }
        for (index, row) in plan.audits.iter().enumerate() {
            self.governance
                .replay_mutation(row.clone(), &format!("{}:{index}", plan.cx.tx_id), replay)
                .await?;
        }
        for id in &plan.approvals {
            self.governance.consume_approval(*id).await?;
        }
        self.journal(&plan.cx, plan.journal.clone()).await?;
        Ok(())
    }
}
