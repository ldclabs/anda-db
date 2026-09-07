//! Protected state and crash recovery. The redo log is flushed before any
//! after-image is applied. Recovery is idempotent and runs under the Nexus lock.
use super::{Element, Store, eq_fields, rows::*, space::JournalEntry, write::WriteContext};
use crate::error::db_error;
use anda_db_schema::Fv;
use anda_kip::KipError;
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
    pub async fn commit_plan(&self, plan: CommitPlan) -> Result<TransactionRow, KipError> {
        // Reserved element ids must survive before a redo intent can name them.
        self.flush(crate::tx::now_ms()).await?;
        let id = self
            .commit_log()
            .add_from(&CommitLogRow {
                _id: 0,
                tx_id: plan.cx.tx_id.clone(),
                plan: serde_json::to_value(&plan)
                    .map_err(|e| KipError::internal_error(e.to_string()))?,
            })
            .await
            .map_err(db_error)?;
        self.commit_log()
            .flush(crate::tx::now_ms())
            .await
            .map_err(db_error)?;
        self.apply_commit(&plan).await?;
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

    pub async fn recover_commits(&self) -> Result<(), KipError> {
        let table = self.commit_log();
        let ids = table.ids();
        for id in ids {
            let row: CommitLogRow = table.get_as(id).await.map_err(db_error)?;
            let plan: CommitPlan = serde_json::from_value(row.plan)
                .map_err(|e| KipError::internal_error(format!("invalid commit log: {e}")))?;
            self.apply_commit(&plan).await?;
            self.flush(crate::tx::now_ms()).await?;
            table.remove(id).await.map_err(db_error)?;
            table.flush(crate::tx::now_ms()).await.map_err(db_error)?;
        }
        Ok(())
    }

    async fn apply_commit(&self, plan: &CommitPlan) -> Result<(), KipError> {
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
        if let Some(row) = &plan.space {
            // Raw write: the plan already owns the audit and notification.
            let mut row = row.clone();
            let current = self.get_space(&row.space_id).await?;
            row.seq = row.seq.max(current.seq);
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
                .replay_mutation(row.clone(), &format!("{}:{index}", plan.cx.tx_id))
                .await?;
        }
        for id in &plan.approvals {
            self.governance.consume_approval(*id).await?;
        }
        self.journal(&plan.cx, plan.journal.clone()).await?;
        Ok(())
    }
}
