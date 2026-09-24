//! # The exposure log (Spec §66.8)
//!
//! A read never reinforces memory (§2.13), yet a Brain needs to know what it
//! retrieved and used: use is the strongest signal that a memory should stay
//! accessible. The exposure log keeps that signal without turning recall into
//! a write path:
//!
//! ```text
//! not cognitive state   no element, no space_seq, no Change Envelope entry
//! never evidence        never cited, never corroboration, never confidence
//! governed              read only under read_audit; an element the reader may
//!                       not discover is omitted, never counted
//! bounded               erased with its element (§60.7)
//! append-only           entries are never rewritten; the host records them
//! ```
//!
//! The host or Memory Interface Adapter records `retrieved` for items it
//! returned and `used` for a DecisionRecord's `used_refs`; Maintenance reads
//! the log in bounded batches and writes any reinforcement explicitly
//! (§59.1). Nothing here changes confidence, strength or utility.

use crate::{
    error::db_error,
    governance::{AuthContext, EffectiveAuthority, Permission, ResourceContext},
    id::ElementId,
    nexus::Session,
    store::{Element, Store, eq_field, rows::ExposureRow, rows::state},
};
use anda_db::query::{Filter, RangeQuery};
use anda_db_schema::Fv;
use anda_kip::{
    Json, KipError,
    cognitive::{Exposure, ExposureRecord},
};
use serde::{Deserialize, Serialize};
use serde_json::json;

/// The most entries one call records or returns.
pub const MAX_EXPOSURES: usize = 1000;
const READ_BATCH: usize = 256;

/// One exposure a host records. The Space, the time and the Principal are the
/// engine's, never the caller's.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExposureInput {
    pub element_id: String,
    pub exposure: Exposure,
    /// The snapshot sequence of the read that exposed the element.
    pub snapshot_seq: u64,
    /// The decision that used it; required for `used`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_ref: Option<String>,
    /// The recall that returned it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recall_ref: Option<String>,
}

/// A bounded page of the log, oldest first.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExposureQuery {
    /// Only this element's entries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub element_id: Option<String>,
    /// The `next_cursor` of an earlier page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// `1..=1000`; default 100.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

impl Store {
    /// Removes every exposure entry of one element; part of its erasure.
    pub(crate) async fn remove_exposures(&self, element: &str) -> Result<(), KipError> {
        let table = self.exposures();
        let ids = table
            .query_all_ids(eq_field("element", Fv::Text(element.into())))
            .await
            .map_err(db_error)?;
        for id in ids {
            table.remove(id).await.map_err(db_error)?;
        }
        Ok(())
    }

    /// How many exposure entries one element still has.
    pub(crate) async fn exposure_count(&self, element: &str) -> Result<usize, KipError> {
        Ok(self
            .exposures()
            .query_all_ids(eq_field("element", Fv::Text(element.into())))
            .await
            .map_err(db_error)?
            .len())
    }
}

impl Session {
    /// Appends exposure entries (§66.8).
    ///
    /// The caller must be able to read each element it reports, and a `used`
    /// entry names the decision that used it — an Activity the caller can
    /// read. The entries take no Space sequence and change no element.
    pub async fn record_exposures(
        &self,
        space: &str,
        entries: Vec<ExposureInput>,
    ) -> Result<Json, KipError> {
        if entries.is_empty() || entries.len() > MAX_EXPOSURES {
            return Err(KipError::constraint_violation(format!(
                "record 1..={MAX_EXPOSURES} exposure entries at a time"
            )));
        }
        self.with_authority(space, async |authority| {
            let store = &self.nexus.store;
            let current = store.get_space(space).await?.seq;
            let recorded_at = crate::time::now();
            let mut rows = Vec::with_capacity(entries.len());
            for entry in &entries {
                let element =
                    readable(store, space, &entry.element_id, &authority, &self.auth).await?;
                if entry.snapshot_seq > current {
                    return Err(KipError::constraint_violation(format!(
                        "snapshot_seq {} is ahead of the Space at {current}",
                        entry.snapshot_seq
                    )));
                }
                let decision = match (&entry.decision_ref, entry.exposure) {
                    (Some(reference), _) => {
                        let decision =
                            readable(store, space, reference, &authority, &self.auth).await?;
                        if !matches!(decision, Element::Activity(_)) {
                            return Err(KipError::constraint_violation(
                                "decision_ref names the Activity that recorded the decision",
                            ));
                        }
                        reference.clone()
                    }
                    (None, Exposure::Used) => {
                        return Err(KipError::constraint_violation(
                            "a used exposure names the decision that used the element",
                        ));
                    }
                    (None, Exposure::Retrieved) => String::new(),
                };
                let recall = entry.recall_ref.clone().unwrap_or_default();
                if recall.chars().count() > 1024 {
                    return Err(KipError::constraint_violation(
                        "recall_ref is at most 1024 characters",
                    ));
                }
                rows.push(ExposureRow {
                    _id: 0,
                    space: space.to_string(),
                    element: element.id().to_string(),
                    exposure: match entry.exposure {
                        Exposure::Retrieved => "retrieved".into(),
                        Exposure::Used => "used".into(),
                    },
                    snapshot_seq: entry.snapshot_seq,
                    recorded_at: recorded_at.clone(),
                    principal_id: self.auth.principal_id.clone(),
                    decision_ref: decision,
                    recall_ref: recall,
                });
            }
            let table = store.exposures();
            for row in &rows {
                table.add_from(row).await.map_err(db_error)?;
            }
            table.flush(crate::tx::now_ms()).await.map_err(db_error)?;
            Ok(json!({"recorded": rows.len()}))
        })
        .await
    }

    /// Reads the exposure log, oldest first (§66.8).
    ///
    /// Requires `read_audit`. An entry whose element the reader may not
    /// discover is omitted, and the page says nothing about how many were.
    pub async fn read_exposures(
        &self,
        space: &str,
        query: ExposureQuery,
    ) -> Result<Json, KipError> {
        let limit = query.limit.unwrap_or(100);
        if limit == 0 || limit > MAX_EXPOSURES {
            return Err(KipError::constraint_violation(format!(
                "exposure page limit is 1..={MAX_EXPOSURES}"
            )));
        }
        let after = match &query.cursor {
            None => 0,
            Some(cursor) => cursor
                .strip_prefix("exposure:")
                .and_then(|n| n.parse::<u64>().ok())
                .ok_or_else(|| {
                    KipError::new(
                        anda_kip::KipErrorCode::CursorInvalid,
                        "invalid exposure cursor",
                    )
                })?,
        };
        let _guard = self.nexus.read_guard().await?;
        let authority = self.effective_authority(space).await?;
        authority
            .authorize(
                Permission::ReadAudit,
                &ResourceContext::default(),
                &self.auth,
            )
            .into_result()?;
        let store = &self.nexus.store;
        let table = store.exposures();
        let mut scanned = after;
        let mut records = Vec::new();
        let mut last = None;
        let mut more = false;
        'pages: loop {
            // Scan the id range directly: combining multiple equality indexes
            // would materialize their full intersection before applying a limit.
            // Each batch starts after the last scanned id, including hidden rows.
            let ids = table
                .query_ids(
                    Filter::Field(("_id".into(), RangeQuery::Gt(Fv::U64(scanned)))),
                    Some(READ_BATCH),
                )
                .await
                .map_err(db_error)?;
            let exhausted = ids.len() < READ_BATCH;
            for id in ids {
                scanned = id;
                let row: ExposureRow = table.get_as(id).await.map_err(db_error)?;
                if row.space != space
                    || query
                        .element_id
                        .as_ref()
                        .is_some_and(|element| *element != row.element)
                {
                    continue;
                }
                let discoverable = match row.element.parse::<ElementId>() {
                    Ok(element) => store
                        .get_element(element)
                        .await
                        .ok()
                        .is_some_and(|element| {
                            element.state() != state::PURGED
                                && authority.may_read(&element, &self.auth).is_some()
                        }),
                    Err(_) => false,
                };
                if !discoverable {
                    continue;
                }
                // Look ahead to a visible entry, so hidden trailing rows never
                // produce a cursor or an extra empty page.
                if records.len() == limit {
                    more = true;
                    break 'pages;
                }
                last = Some(id);
                records.push(ExposureRecord {
                    space_id: row.space,
                    element_id: row.element,
                    exposure: if row.exposure == "used" {
                        Exposure::Used
                    } else {
                        Exposure::Retrieved
                    },
                    snapshot_seq: row.snapshot_seq,
                    recorded_at: row.recorded_at,
                    principal_id: row.principal_id,
                    decision_ref: Some(row.decision_ref).filter(|r| !r.is_empty()),
                    recall_ref: Some(row.recall_ref).filter(|r| !r.is_empty()),
                });
            }
            if exhausted {
                break;
            }
        }
        Ok(json!({
            "records": records,
            "next_cursor": if more { last.map(|id| format!("exposure:{id}")) } else { None },
        }))
    }
}

/// An element the caller may read, existence-neutrally.
async fn readable(
    store: &Store,
    space: &str,
    reference: &str,
    authority: &EffectiveAuthority,
    auth: &AuthContext,
) -> Result<Element, KipError> {
    let unavailable = || KipError::not_found_or_not_visible(format!("{reference} is unavailable"));
    let id: ElementId = reference.parse().map_err(|_| unavailable())?;
    let element = store.get_element(id).await.map_err(|_| unavailable())?;
    if element.space() != space
        || matches!(element.state(), state::PURGED | state::PENDING)
        || !authority
            .may_read(&element, auth)
            .is_some_and(|v| v.content)
    {
        return Err(unavailable());
    }
    Ok(element)
}
