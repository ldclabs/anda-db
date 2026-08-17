//! # Reading the Space at a past coordinate
//!
//! `AS OF SEQ 41` asks what this Brain held then, which is a different question
//! from `FOR TIME` — what was *true* then (§36.1). Answering it needs state the
//! current rows do not have, because a row is updated in place: version 3
//! overwrites version 2, and version 2 is gone.
//!
//! So every commit appends the complete row it wrote to a version log, and a
//! historical read is "the greatest version of this element whose sequence is
//! at or before the coordinate". An element with no such version did not exist
//! yet, which is why an `AS OF` read of a Concept created later finds nothing
//! rather than finding it in a state it never had.
//!
//! ## Why this is a scan
//!
//! The indexes on the current rows describe the present. A historical pattern
//! cannot use them — `{state: "active"}` today says nothing about what was
//! active at sequence 41 — so a historical read enumerates the version log for
//! its Space and reconstructs the coordinate. It is charged against the same
//! query budget as everything else, so a historical read of an enormous Space
//! refuses rather than stalls.

use anda_db::query::{Filter, RangeQuery};
use anda_db_schema::Fv;
use anda_kip::{ElementKind, Json, KipError, KipErrorCode};
use std::collections::BTreeMap;

use super::rows::*;
use super::{Element, Store, eq_field, eq_fields};
use crate::error::db_error;
use crate::id::ElementId;
use crate::store::write::WriteContext;

impl Store {
    /// Appends one element version, in the same commit as the row itself.
    pub async fn record_version<R: super::write::Row + serde::Serialize>(
        &self,
        cx: &WriteContext,
        id: ElementId,
        version: u64,
        op: &str,
        row: &R,
    ) -> Result<(), KipError> {
        let encoded = serde_json::to_value(row).map_err(|err| {
            KipError::internal_error(format!("an element row failed to encode: {err}"))
        })?;
        let entry = ElementVersionRow {
            _id: 0,
            space: cx.space.clone(),
            element: id.to_string(),
            kind: id.kind.to_string(),
            version,
            seq: cx.seq,
            tx_id: cx.tx_id.clone(),
            op: op.to_string(),
            row: encoded,
        };
        self.element_versions()
            .add_from(&entry)
            .await
            .map_err(db_error)?;
        Ok(())
    }

    /// Destroys every recorded version of one element.
    ///
    /// The half of a purge that is easy to forget and fatal to skip: every
    /// commit appends the whole row it wrote, so an element scrubbed only in
    /// its current row stays fully readable through `AS OF`. Returns how many
    /// versions were destroyed, so a purge receipt can say what it cost.
    ///
    /// Rows are removed rather than scrubbed, unlike the element itself: a
    /// version entry has no identity anything refers to, so there is nothing
    /// for a stub to keep resolvable.
    pub async fn purge_versions(&self, space_id: &str, id: ElementId) -> Result<usize, KipError> {
        let ids = self.version_ids(space_id, id).await?;
        self.remove_versions(&ids).await?;
        Ok(ids.len())
    }

    /// Destroys exactly the version rows a staged purge counted.
    ///
    /// Takes the ids rather than re-deriving them, so the number a purge
    /// receipt reports and the rows it erases cannot come apart.
    pub async fn remove_versions(&self, ids: &[u64]) -> Result<(), KipError> {
        let collection = self.element_versions();
        for row_id in ids {
            collection.remove(*row_id).await.map_err(db_error)?;
        }
        Ok(())
    }

    /// Every version row of one element.
    pub(crate) async fn version_ids(
        &self,
        space_id: &str,
        id: ElementId,
    ) -> Result<Vec<u64>, KipError> {
        self.element_versions()
            .query_all_ids(eq_fields(&[
                ("space", Fv::Text(space_id.to_string())),
                ("element", Fv::Text(id.to_string())),
            ]))
            .await
            .map_err(db_error)
    }

    /// One element as it stood at a coordinate, or `None` when it did not
    /// exist yet.
    pub async fn element_at(
        &self,
        space_id: &str,
        id: ElementId,
        seq: u64,
    ) -> Result<Option<Element>, KipError> {
        let ids = self
            .element_versions()
            .query_all_ids(Filter::And(vec![
                Box::new(eq_field("space", Fv::Text(space_id.to_string()))),
                Box::new(eq_field("element", Fv::Text(id.to_string()))),
                Box::new(Filter::Field((
                    "seq".to_string(),
                    RangeQuery::Le(Fv::U64(seq)),
                ))),
            ]))
            .await
            .map_err(db_error)?;

        let mut best: Option<ElementVersionRow> = None;
        for row_id in ids {
            let row: ElementVersionRow = self
                .element_versions()
                .get_as(row_id)
                .await
                .map_err(db_error)?;
            if best
                .as_ref()
                .is_none_or(|current| (row.seq, row.version) > (current.seq, current.version))
            {
                best = Some(row);
            }
        }
        best.map(decode).transpose()
    }

    /// Every element that existed in a Space at a coordinate, by kind.
    ///
    /// The whole log for the Space is read and reduced to one version per
    /// element, because "which elements existed then" cannot be answered from
    /// an index over what exists now.
    pub async fn elements_at(
        &self,
        space_id: &str,
        kind: ElementKind,
        seq: u64,
    ) -> Result<Vec<Element>, KipError> {
        let ids = self
            .element_versions()
            .query_all_ids(Filter::And(vec![
                Box::new(eq_field("space", Fv::Text(space_id.to_string()))),
                Box::new(eq_field("kind", Fv::Text(kind.to_string()))),
                Box::new(Filter::Field((
                    "seq".to_string(),
                    RangeQuery::Le(Fv::U64(seq)),
                ))),
            ]))
            .await
            .map_err(db_error)?;

        let mut latest: BTreeMap<String, ElementVersionRow> = BTreeMap::new();
        for row_id in ids {
            let row: ElementVersionRow = self
                .element_versions()
                .get_as(row_id)
                .await
                .map_err(db_error)?;
            match latest.get(&row.element) {
                Some(current) if (current.seq, current.version) >= (row.seq, row.version) => {}
                _ => {
                    latest.insert(row.element.clone(), row);
                }
            }
        }
        latest.into_values().map(decode).collect()
    }

    /// Resolves `AS OF TX :tx` to the Space sequence that transaction produced.
    pub async fn seq_of_transaction(&self, space_id: &str, tx_id: &str) -> Result<u64, KipError> {
        let row = self.find_transaction(tx_id).await?.ok_or_else(|| {
            KipError::new(
                KipErrorCode::TransactionUnknown,
                format!("this Nexus has no transaction {tx_id:?} to read as of"),
            )
        })?;
        if row.space != space_id {
            return Err(KipError::new(
                KipErrorCode::TransactionUnknown,
                format!("{tx_id:?} committed in another Space, so it names no coordinate here"),
            ));
        }
        Ok(row.seq)
    }

    /// Resolves `AS OF TIME :t` to the last coordinate committed at or before
    /// it.
    ///
    /// Wall-clock time is not the Space's ordering, so this is a lookup in the
    /// journal rather than arithmetic: the answer is the sequence of the last
    /// transaction that had committed by then, and a time before the first
    /// commit is coordinate 0 — an empty Space, not an error.
    pub async fn seq_at_time(&self, space_id: &str, at: &str) -> Result<u64, KipError> {
        let ids = self
            .transactions()
            .query_all_ids(eq_field("space", Fv::Text(space_id.to_string())))
            .await
            .map_err(db_error)?;
        let mut seq = 0u64;
        for id in ids {
            let row: TransactionRow = self.transactions().get_as(id).await.map_err(db_error)?;
            // Timestamps are one normalized UTC form, so lexicographic order
            // is chronological order.
            if row.committed_at.as_str() <= at && row.seq > seq {
                seq = row.seq;
            }
        }
        Ok(seq)
    }

    /// The Schema Environment version that was in force at a coordinate.
    ///
    /// An activation is a transaction like any other, so the environment a
    /// historical read resolves symbols through is the last one activated at
    /// or before the coordinate — never today's (§144).
    pub async fn schema_version_at(&self, space_id: &str, seq: u64) -> Result<u64, KipError> {
        let ids = self
            .schema_envs()
            .query_all_ids(eq_field("space", Fv::Text(space_id.to_string())))
            .await
            .map_err(db_error)?;
        let mut version = 0u64;
        for id in ids {
            let row: SchemaEnvRow = self.schema_envs().get_as(id).await.map_err(db_error)?;
            // The activation's own transaction is what puts it in force, so
            // its coordinate is the sequence that transaction produced.
            let activated_at = self
                .find_transaction(&row.tx_id)
                .await?
                .map(|tx| tx.seq)
                .unwrap_or(0);
            if activated_at <= seq && row.version > version {
                version = row.version;
            }
        }
        Ok(version)
    }

    /// The Space's current sequence, which is what a snapshot with no `AS OF`
    /// binds to.
    pub async fn current_seq(&self, space_id: &str) -> Result<u64, KipError> {
        Ok(self.get_space(space_id).await?.seq)
    }
}

fn decode(row: ElementVersionRow) -> Result<Element, KipError> {
    let kind = match row.kind.as_str() {
        "concept" => ElementKind::Concept,
        "proposition" => ElementKind::Proposition,
        "assertion" => ElementKind::Assertion,
        "evidence" => ElementKind::Evidence,
        "activity" => ElementKind::Activity,
        other => {
            return Err(KipError::internal_error(format!(
                "a version row carries the unknown kind {other:?}"
            )));
        }
    };
    let value = row.row;
    let unreadable = |err: serde_json::Error| {
        KipError::internal_error(format!(
            "the stored version {} of {} is unreadable: {err}",
            row.version, row.element
        ))
    };
    Ok(match kind {
        ElementKind::Concept => {
            Element::Concept(Box::new(serde_json::from_value(value).map_err(unreadable)?))
        }
        ElementKind::Proposition => {
            Element::Proposition(Box::new(serde_json::from_value(value).map_err(unreadable)?))
        }
        ElementKind::Assertion => {
            Element::Assertion(Box::new(serde_json::from_value(value).map_err(unreadable)?))
        }
        ElementKind::Evidence => {
            Element::Evidence(Box::new(serde_json::from_value(value).map_err(unreadable)?))
        }
        ElementKind::Activity => {
            Element::Activity(Box::new(serde_json::from_value(value).map_err(unreadable)?))
        }
    })
}

/// A coordinate a read is bound to.
///
/// `AS OF` names one; a request may also carry a `read.snapshot_token`. Both
/// resolve to a Space sequence, and everything downstream reads that one
/// number — a coordinate that meant different things in two places would be
/// worse than none.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Coordinate {
    /// The Space sequence the read is pinned to.
    pub seq: u64,
}

impl Coordinate {
    /// The opaque token a client uses to bind a later read to this coordinate.
    ///
    /// Opaque by contract, not by encryption: a client that parsed it would be
    /// depending on a shape this engine may change. It carries the Space so a
    /// token cannot be replayed against a different one, where the same
    /// sequence means something else entirely.
    pub fn to_token(self, space_id: &str) -> String {
        hex::encode(format!("kip:snapshot:{space_id}:{}", self.seq))
    }

    /// Reads a token back, refusing one issued for another Space.
    pub fn from_token(token: &str, space_id: &str) -> Result<Self, KipError> {
        let invalid = || {
            KipError::new(
                KipErrorCode::CursorInvalidated,
                format!("{token:?} is not a snapshot token this engine issued for this Space"),
            )
        };
        let decoded = hex::decode(token).map_err(|_| invalid())?;
        let text = String::from_utf8(decoded).map_err(|_| invalid())?;
        let rest = text.strip_prefix("kip:snapshot:").ok_or_else(invalid)?;
        let (space, seq) = rest.rsplit_once(':').ok_or_else(invalid)?;
        if space != space_id {
            return Err(KipError::new(
                KipErrorCode::CursorInvalidated,
                format!(
                    "this snapshot token was issued for Space {space:?}; a sequence means \
                     something different in {space_id:?}"
                ),
            ));
        }
        Ok(Coordinate {
            seq: seq.parse().map_err(|_| invalid())?,
        })
    }
}

/// The JSON a snapshot answer carries.
pub fn snapshot_json(space_id: &str, coordinate: Coordinate, schema_version: u64) -> Json {
    serde_json::json!({
        "space_id": space_id,
        "snapshot_seq": coordinate.seq,
        "schema_environment_version": schema_version,
        "snapshot_token": coordinate.to_token(space_id),
    })
}
