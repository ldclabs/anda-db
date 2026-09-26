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
use anda_kip::{ElementKind, Json, KipError};
use std::collections::BTreeMap;

use super::rows::*;
use super::{Element, Store, eq_field, eq_fields};
use crate::error::db_error;
use crate::id::ElementId;
use crate::store::write::WriteContext;

/// Fixed-width big-endian decimal components provide tuple ordering. Space
/// and element are hex encoded, so user-provided separators cannot alias keys.
pub(crate) fn version_lookup_key(space: &str, element: &str, seq: u64, version: u64) -> String {
    format!(
        "{}/{}/{seq:020}/{version:020}",
        hex::encode(space),
        hex::encode(element)
    )
}

impl Store {
    /// Appends one element version, in the same commit as the row itself.
    ///
    /// `replay` is a recovery pass over a retained redo intent, which may run
    /// after the version was already flushed; only then is the log checked
    /// for it first.
    pub async fn record_version<R: super::write::Row + serde::Serialize>(
        &self,
        cx: &WriteContext,
        id: ElementId,
        version: u64,
        op: &str,
        row: &R,
        replay: bool,
    ) -> Result<(), KipError> {
        if replay {
            for row_id in self
                .element_versions()
                .query_all_ids(eq_fields(&[
                    ("space", Fv::Text(cx.space.clone())),
                    ("element", Fv::Text(id.to_string())),
                    ("tx_id", Fv::Text(cx.tx_id.clone())),
                ]))
                .await
                .map_err(db_error)?
            {
                let old: ElementVersionRow = self
                    .element_versions()
                    .get_as(row_id)
                    .await
                    .map_err(db_error)?;
                if old.tx_id == cx.tx_id && old.version == version {
                    return Ok(());
                }
            }
        }
        let entry = version_row(cx, id, version, op, row)?;
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

    /// Strips the Evidence payload out of recorded versions, keeping the rows.
    ///
    /// The half of a payload purge that is easy to forget and fatal to skip: a
    /// payload cleared only in the current row stays fully readable through
    /// `AS OF`, which would make §60.6 a promise the engine does not keep.
    ///
    /// Rewritten rather than removed, unlike an element purge: the Evidence
    /// record survives a payload purge, so its lifecycle history is not the
    /// thing being erased and destroying it would take more than the caller
    /// asked for.
    ///
    /// **A version this cannot rewrite refuses the whole statement** rather
    /// than being skipped. `EvidenceRow` declares no `serde` defaults, so a
    /// version written before a column was added does not decode — and a
    /// skipped version is bytes that survive while the receipt says they were
    /// destroyed. That is the one outcome a data-minimization instrument must
    /// never produce, so the failure is loud and the transaction does not
    /// commit.
    pub async fn scrub_payload_versions(&self, ids: &[u64]) -> Result<(), KipError> {
        let collection = self.element_versions();
        for row_id in ids {
            let mut version: ElementVersionRow =
                collection.get_as(*row_id).await.map_err(db_error)?;
            // Decoded and re-encoded through the row type so the columns a
            // payload purge clears are named in exactly one place — the same
            // place the current row is cleared from.
            let mut evidence: EvidenceRow =
                serde_json::from_value(version.row.clone()).map_err(|err| {
                    KipError::internal_error(format!(
                        "version {row_id} of {} does not decode as Evidence, so its payload \
                         cannot be erased: {err}",
                        version.element
                    ))
                })?;
            erase_payload(&mut evidence);
            version.row = serde_json::to_value(&evidence).map_err(|err| {
                KipError::internal_error(format!("an element version failed to encode: {err}"))
            })?;
            let fields = super::full_row_fields(collection.schema(), &version)?;
            collection.update(*row_id, fields).await.map_err(db_error)?;
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
        let table = self.element_versions();
        let index = table.get_btree_index(&["lookup_key"]).map_err(db_error)?;
        let mut ids = Vec::new();
        index
            .try_range_query_ids(
                RangeQuery::Between(
                    Fv::Text(version_lookup_key(space_id, &id.to_string(), 0, 0)),
                    Fv::Text(version_lookup_key(space_id, &id.to_string(), seq, u64::MAX)),
                ),
                true,
                |posting| {
                    ids.extend(posting.iter().min().copied());
                    false
                },
            )
            .map_err(db_error)?;
        match ids.first() {
            Some(id) => decode(table.get_as(*id).await.map_err(db_error)?).map(Some),
            None => Ok(None),
        }
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
        Ok(self
            .elements_at_bounded(space_id, kind, seq, usize::MAX)
            .await?
            .0)
    }

    /// Bound version-log work before decoding full historical rows.
    pub(crate) async fn elements_at_bounded(
        &self,
        space_id: &str,
        kind: ElementKind,
        seq: u64,
        budget: usize,
    ) -> Result<(Vec<Element>, usize), KipError> {
        use futures::StreamExt;
        let table = self.element_versions();
        // Both paths read the chosen version rows with bounded concurrency.
        let rows = |ids: Vec<u64>| {
            futures::stream::iter(ids)
                .map(|row_id| {
                    let table = table.clone();
                    async move {
                        table
                            .get_as::<ElementVersionRow>(row_id)
                            .await
                            .map_err(db_error)
                    }
                })
                .buffered(8)
        };
        // An early coordinate may contain few versions but many elements
        // created later. Prefer that small sequence range over all identities.
        let sequences = table.get_btree_index(&["seq"]).map_err(db_error)?;
        let past = RangeQuery::Le(Fv::U64(seq));
        if sequences
            .estimate_cardinality(past.clone(), budget.saturating_add(1))
            .map_err(db_error)?
            <= budget
        {
            let mut past_ids = Vec::new();
            sequences
                .try_range_query_ids(past, false, |ids| {
                    past_ids.extend_from_slice(ids);
                    true
                })
                .map_err(db_error)?;
            let ids = table
                .filter_candidate_ids(
                    eq_fields(&[
                        ("space", Fv::Text(space_id.into())),
                        ("kind", Fv::Text(kind.to_string())),
                    ]),
                    &past_ids,
                )
                .await
                .map_err(db_error)?;
            let scanned = ids.len();
            let mut latest = BTreeMap::<String, ElementVersionRow>::new();
            let mut stream = rows(ids);
            while let Some(row) = stream.next().await {
                let row = row?;
                if latest
                    .get(&row.element)
                    .is_none_or(|old| (row.seq, row.version) > (old.seq, old.version))
                {
                    latest.insert(row.element.clone(), row);
                }
            }
            return Ok((
                latest.into_values().map(decode).collect::<Result<_, _>>()?,
                scanned,
            ));
        }
        let index = table.get_btree_index(&["lookup_key"]).map_err(db_error)?;
        // Every id of a kind starts with its tag, so the hex prefix of
        // `"C-"` covers exactly that kind's version chains.
        let prefix = format!(
            "{}/{}",
            hex::encode(space_id),
            hex::encode(format!("{}-", ElementId::tag(kind)))
        );
        let high = format!("{prefix}\u{10ffff}");
        let mut cursor = prefix;
        let mut selected = Vec::new();
        let mut scanned = 0usize;
        loop {
            // Find the next element prefix, then seek to its predecessor
            // version. Jump past the entire version chain before continuing.
            let keys = index.range_query_with(
                RangeQuery::Between(Fv::Text(cursor.clone()), Fv::Text(high.clone())),
                |key, _| (false, vec![key]),
            );
            let Some(Fv::Text(key)) = keys.first() else {
                break;
            };
            let Some((group, _)) = key.rsplit_once('/') else {
                return Err(KipError::internal_error("invalid version locator"));
            };
            let Some((group, _)) = group.rsplit_once('/') else {
                return Err(KipError::internal_error("invalid version locator"));
            };
            index
                .try_range_query_ids(
                    RangeQuery::Between(
                        Fv::Text(format!("{group}/{:020}/{:020}", 0, 0)),
                        Fv::Text(format!("{group}/{seq:020}/{:020}", u64::MAX)),
                    ),
                    true,
                    |posting| {
                        selected.extend(posting.iter().min().copied());
                        false
                    },
                )
                .map_err(db_error)?;
            scanned = selected.len();
            if scanned > budget {
                return Err(KipError::resource_exhausted(
                    "historical element scan exceeds query budget",
                ));
            }
            cursor = format!("{group}/\u{10ffff}");
        }
        let mut stream = rows(selected);
        let mut elements = Vec::new();
        while let Some(row) = stream.next().await {
            elements.push(decode(row?)?);
        }
        Ok((elements, scanned))
    }

    /// Ordered by logical sequence, independent of journal insertion order.
    /// Caller holds the Nexus guard; only the selected page is materialized.
    pub(crate) async fn journal_page(
        &self,
        space: &str,
        after: u64,
        through: u64,
        limit: usize,
    ) -> Result<Vec<TransactionRow>, KipError> {
        if after >= through || limit == 0 {
            return Ok(vec![]);
        }
        let table = self.transactions();
        let mut ids = Vec::new();
        {
            let spaces = table.get_btree_index(&["space"]).map_err(db_error)?;
            let sequences = table.get_btree_index(&["seq"]).map_err(db_error)?;
            if let Some(result) = spaces.query_with(&Fv::Text(space.into()), |members| {
                Some(sequences.try_range_query_ids(
                    RangeQuery::Between(Fv::U64(after + 1), Fv::U64(through)),
                    false,
                    |posting| {
                        for id in posting {
                            if members.binary_search(id).is_ok() {
                                ids.push(*id);
                                if ids.len() == limit {
                                    return false;
                                }
                            }
                        }
                        true
                    },
                ))
            }) {
                result.map_err(db_error)?;
            }
        }
        let mut rows = Vec::with_capacity(ids.len());
        for id in ids {
            rows.push(table.get_as(id).await.map_err(db_error)?);
        }
        Ok(rows)
    }

    /// The transaction that produced one Space sequence, when the journal
    /// holds it.
    ///
    /// Sequence 0 is "nothing has happened here yet" and names no transaction;
    /// a sequence allocated by a run that never journalled — an aborted
    /// statement burns its number — names none either.
    pub async fn transaction_at_seq(
        &self,
        space_id: &str,
        seq: u64,
    ) -> Result<Option<TransactionRow>, KipError> {
        if seq == 0 {
            return Ok(None);
        }
        let ids = self
            .transactions()
            .query_all_ids(eq_fields(&[
                ("space", Fv::Text(space_id.to_string())),
                ("seq", Fv::U64(seq)),
            ]))
            .await
            .map_err(db_error)?;
        match ids.first() {
            None => Ok(None),
            Some(id) => Ok(Some(
                self.transactions().get_as(*id).await.map_err(db_error)?,
            )),
        }
    }

    /// Resolves `DESCRIBE SNAPSHOT AT TIME :t` to the last coordinate
    /// committed at or before the instant (§68).
    ///
    /// Wall-clock time is not the Space's ordering, so this is a lookup in the
    /// journal rather than arithmetic: the answer is the sequence of the last
    /// transaction that had committed by then, and a time before the first
    /// commit is coordinate 0 — an empty Space, not an error. This engine keeps
    /// every version, so no instant falls below a retention floor.
    pub async fn seq_at_time(&self, space_id: &str, at: &str) -> Result<u64, KipError> {
        let transactions = self.transactions();
        // Timestamps are one normalized UTC form, so lexicographic order is
        // chronological order and the index can range over them.
        let ids = transactions
            .query_all_ids(Filter::And(vec![
                Box::new(eq_field("space", Fv::Text(space_id.to_string()))),
                Box::new(Filter::Field((
                    "committed_at".to_string(),
                    RangeQuery::Le(Fv::Text(at.to_string())),
                ))),
            ]))
            .await
            .map_err(db_error)?;
        // The journal is appended in commit order under the Nexus write lock,
        // so among the entries committed by `at` the newest that took a
        // sequence holds the greatest one; a no-op holds none (0).
        for id in ids.into_iter().rev() {
            let row: TransactionRow = transactions.get_as(id).await.map_err(db_error)?;
            if row.seq > 0 {
                return Ok(row.seq);
            }
        }
        Ok(0)
    }

    /// The Schema Environment version that was in force at a coordinate.
    ///
    /// An activation is a transaction like any other, so the environment a
    /// historical read resolves symbols through is the last one activated at
    /// or before the coordinate — never today's (§20.9).
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

/// The version-log row one committed element version is recorded as.
pub(crate) fn version_row<R: serde::Serialize>(
    cx: &WriteContext,
    id: ElementId,
    version: u64,
    op: &str,
    row: &R,
) -> Result<ElementVersionRow, KipError> {
    let encoded = serde_json::to_value(row).map_err(|err| {
        KipError::internal_error(format!("an element row failed to encode: {err}"))
    })?;
    Ok(ElementVersionRow {
        _id: 0,
        space: cx.space.clone(),
        element: id.to_string(),
        kind: id.kind.to_string(),
        version,
        seq: cx.seq,
        tx_id: cx.tx_id.clone(),
        op: op.to_string(),
        lookup_key: None,
        row: encoded,
    })
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
    Element::from_json(kind, value).map_err(unreadable)
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
    ///
    /// A token this engine did not issue for this Space is `CursorInvalid`
    /// with `family: "snapshot"` and `reason: "malformed"` (§87.7): whether
    /// the bytes were forged or belong to another Space, they name no
    /// coordinate here.
    pub fn from_token(token: &str, space_id: &str) -> Result<Self, KipError> {
        let invalid = || {
            KipError::cursor_invalid(
                "snapshot",
                "malformed",
                format!("{token:?} is not a snapshot token this engine issued for this Space"),
            )
        };
        let decoded = hex::decode(token).map_err(|_| invalid())?;
        let text = String::from_utf8(decoded).map_err(|_| invalid())?;
        let rest = text.strip_prefix("kip:snapshot:").ok_or_else(invalid)?;
        let (space, seq) = rest.rsplit_once(':').ok_or_else(invalid)?;
        if space != space_id {
            return Err(KipError::cursor_invalid(
                "snapshot",
                "malformed",
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

/// One page of a paged answer: which traversal, pinned to which coordinate,
/// and how far in.
///
/// §44.8 makes a KQL cursor preserve **one canonical cognitive snapshot for
/// that traversal**, and §88.4 makes every cursor opaque or safely
/// server-mapped. A bare offset is neither: page two of a query re-runs
/// against whatever the Space holds by then, so a write between pages
/// duplicates or skips rows — silently, since both pages look well-formed —
/// and a caller can type any number it likes into a cursor slot.
///
/// The `family` tag is §102.28: a cursor issued by `HISTORY` must not continue
/// a `FIND`, even though both count from zero. Without it the two are the same
/// integer and the engine cannot tell which traversal it is being asked to
/// resume.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageCursor {
    pub family: CursorFamily,
    pub snapshot_seq: u64,
    pub offset: usize,
    /// The traversal this cursor continues: [`traversal_of`] the query or
    /// command that issued it. A cursor handed to a different query names a
    /// page of nothing (§44.8), and the token says so rather than answering
    /// with the wrong query's page.
    pub traversal: String,
}

/// The identity of one traversal, for the cursor it issues (§44.8, §88.4).
///
/// The lowered command with its `cursor` and `limit` slots blanked, plus the
/// parameters those slots did not consume: the same query paged with a
/// different page size continues the same traversal, and the token that
/// continues it is not part of the identity it continues.
pub fn traversal_of<T: serde::Serialize>(
    command: &T,
    request: Option<&anda_kip::Map<String, anda_kip::Json>>,
    operation: Option<&anda_kip::Map<String, anda_kip::Json>>,
) -> String {
    use sha3::{Digest, Sha3_256};
    let mut command = serde_json::to_value(command).unwrap_or(anda_kip::Json::Null);
    let mut consumed = Vec::new();
    blank_paging(&mut command, &mut consumed);
    let strip = |params: Option<&anda_kip::Map<String, anda_kip::Json>>| {
        params.map(|params| {
            let mut params = params.clone();
            for name in &consumed {
                params.remove(name);
            }
            anda_kip::Json::Object(params)
        })
    };
    let identity = serde_json::json!({
        "command": command,
        "request": strip(request),
        "operation": strip(operation),
    });
    let canonical = anda_kip::canonical_json(&identity);
    hex::encode(Sha3_256::digest(canonical.as_bytes()))[..16].to_string()
}

fn blank_paging(value: &mut anda_kip::Json, consumed: &mut Vec<String>) {
    match value {
        anda_kip::Json::Object(map) => {
            for slot in ["cursor", "limit"] {
                if let Some(taken) = map.remove(slot)
                    && let Some(name) = taken.get("Param").and_then(anda_kip::Json::as_str)
                {
                    consumed.push(name.to_string());
                }
            }
            for child in map.values_mut() {
                blank_paging(child, consumed);
            }
        }
        anda_kip::Json::Array(items) => {
            for item in items {
                blank_paging(item, consumed);
            }
        }
        _ => {}
    }
}

/// The operation families that issue page cursors (§102.28).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CursorFamily {
    /// `FIND ... LIMIT ... CURSOR`.
    Query,
    /// `SEARCH ... LIMIT ... CURSOR`.
    Search,
    /// `LIST ... LIMIT ... CURSOR`.
    List,
    /// `HISTORY ELEMENT | SPACE`.
    History,
}

impl CursorFamily {
    /// The family name §87.7 reports in `details.family`, which is also the
    /// tag inside the token.
    pub fn tag(self) -> &'static str {
        match self {
            CursorFamily::Query => "kql",
            CursorFamily::Search => "search",
            CursorFamily::List => "list",
            CursorFamily::History => "history",
        }
    }
}

impl PageCursor {
    /// Server-mapped continuations preserve ordinary paging without granting
    /// arbitrary historical reads. `seek` is the row id the page before this
    /// cursor ended at, when the next page can start right after it instead
    /// of walking its offset again. Eviction or reconnect requires a new
    /// page 1.
    pub(crate) fn issue(
        &self,
        store: &Store,
        space: &str,
        principal: &str,
        seek: Option<u64>,
    ) -> String {
        let token = self.to_token(space);
        let mut issued = store.issued_cursors.lock();
        if !issued.iter().any(|(p, t, _)| p == principal && t == &token) {
            if issued.len() == 1024 {
                issued.pop_front();
            }
            issued.push_back((principal.into(), token.clone(), seek));
        }
        token
    }

    /// The seek an issued cursor carries, or `CursorExpired` when this
    /// engine does not remember issuing the token to this principal.
    pub(crate) fn require_issued(
        &self,
        store: &Store,
        token: &str,
        principal: &str,
    ) -> Result<Option<u64>, KipError> {
        store
            .issued_cursors
            .lock()
            .iter()
            .find(|(p, t, _)| p == principal && t == token)
            .map(|(_, _, seek)| *seek)
            .ok_or_else(|| {
                KipError::cursor_expired(
                    self.family.tag(),
                    "continuation unavailable; start a new traversal",
                )
            })
    }

    /// The opaque token a client passes back to continue.
    ///
    /// Opaque by contract rather than by encryption, like a snapshot token: a
    /// client that decoded it would be depending on a shape this engine may
    /// change, and every field inside it is re-checked on the way back in.
    pub fn to_token(&self, space_id: &str) -> String {
        hex::encode(format!(
            "kip:cursor:{}:{space_id}:{}:{}:{}",
            self.family.tag(),
            self.traversal,
            self.snapshot_seq,
            self.offset
        ))
    }

    /// Reads a token back, refusing one this engine did not issue for this
    /// Space and this operation family.
    ///
    /// Every refusal is `CursorInvalid` with `reason: "malformed"` and the
    /// family this slot expected (§87.7): a forged token, one from another
    /// Space and one from another operation family all fail to name a page
    /// of this traversal, and telling them apart would only tell a forger
    /// which part to fix.
    pub fn from_token(
        token: &str,
        space_id: &str,
        family: CursorFamily,
        traversal: &str,
    ) -> Result<Self, KipError> {
        let invalid = || {
            KipError::cursor_invalid(
                family.tag(),
                "malformed",
                format!(
                    "{token:?} is not a {} cursor this engine issued for this Space; a cursor is \
                     opaque and belongs to the traversal that produced it",
                    family.tag()
                ),
            )
        };
        let decoded = hex::decode(token).map_err(|_| invalid())?;
        let text = String::from_utf8(decoded).map_err(|_| invalid())?;
        let rest = text.strip_prefix("kip:cursor:").ok_or_else(invalid)?;
        let (tag, rest) = rest.split_once(':').ok_or_else(invalid)?;
        if tag != family.tag() {
            return Err(invalid());
        }
        let (rest, offset) = rest.rsplit_once(':').ok_or_else(invalid)?;
        let (rest, snapshot_seq) = rest.rsplit_once(':').ok_or_else(invalid)?;
        let (space, issued_for) = rest.rsplit_once(':').ok_or_else(invalid)?;
        if space != space_id {
            return Err(invalid());
        }
        let offset: usize = offset.parse().map_err(|_| invalid())?;
        let snapshot_seq: u64 = snapshot_seq.parse().map_err(|_| invalid())?;
        // §44.8: a cursor continues the traversal that produced it. One from
        // another query would answer with that query's page, silently.
        if issued_for != traversal {
            return Err(KipError::new(
                anda_kip::KipErrorCode::CursorMismatch,
                format!(
                    "this {} cursor was issued by a different query; a cursor continues the \
                     traversal that produced it, so restart this one from its first page",
                    family.tag()
                ),
            ));
        }
        Ok(PageCursor {
            family,
            snapshot_seq,
            offset,
            traversal: issued_for.to_string(),
        })
    }
}

/// The snapshot coordinate `DESCRIBE SNAPSHOT` answers with (§68).
///
/// The sequence, the transaction that committed it and when, the schema
/// environment in force, and the token a later read binds to (§78). Sequence
/// 0 — nothing committed yet — names no transaction and says so with nulls
/// rather than inventing one.
pub fn snapshot_json(
    space_id: &str,
    coordinate: Coordinate,
    committed: Option<&TransactionRow>,
    schema_version: u64,
) -> Json {
    serde_json::json!({
        "space_id": space_id,
        "space_seq": coordinate.seq,
        "tx_id": committed.map(|row| row.tx_id.clone()),
        "committed_at": committed.map(|row| row.committed_at.clone()),
        "schema_environment_version": schema_version,
        "snapshot_token": coordinate.to_token(space_id),
    })
}

#[cfg(test)]
mod scale_tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn historical_locators_skip_long_chains_and_future_identities() {
        let db = anda_db::database::AndaDB::connect(
            Arc::new(object_store::memory::InMemory::new()),
            anda_db::database::DBConfig {
                name: "version_seek_test".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let store = Store::open(Arc::new(db)).await.unwrap();
        let table = store.element_versions();
        for seq in 1..=100u64 {
            let row = ConceptRow {
                _id: 1,
                space: "test".into(),
                state: "active".into(),
                version: seq,
                name: format!("v{seq}"),
                ..Default::default()
            };
            table
                .add_from(&ElementVersionRow {
                    space: "test".into(),
                    element: "C-1".into(),
                    kind: "concept".into(),
                    seq,
                    version: seq,
                    row: serde_json::to_value(row).unwrap(),
                    ..Default::default()
                })
                .await
                .unwrap();
        }
        for id in 2..=40u64 {
            let row = ConceptRow {
                _id: id,
                space: "test".into(),
                state: "active".into(),
                version: 1,
                ..Default::default()
            };
            table
                .add_from(&ElementVersionRow {
                    space: "test".into(),
                    element: format!("C-{id}"),
                    kind: "concept".into(),
                    seq: 200,
                    version: 1,
                    row: serde_json::to_value(row).unwrap(),
                    ..Default::default()
                })
                .await
                .unwrap();
        }
        let before = table.stats().get_count;
        let row = store
            .element_at("test", "C-1".parse().unwrap(), 77)
            .await
            .unwrap()
            .unwrap();
        let Element::Concept(row) = row else {
            panic!("concept");
        };
        assert_eq!(row.name, "v77");
        assert_eq!(table.stats().get_count - before, 1);
        let before = table.stats().get_count;
        let (rows, scanned) = store
            .elements_at_bounded("test", ElementKind::Concept, 77, 10)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(scanned, 1);
        assert_eq!(table.stats().get_count - before, 1);
        assert_eq!(
            store
                .elements_at_bounded("test", ElementKind::Concept, 1, 10)
                .await
                .unwrap()
                .0
                .len(),
            1
        );
        assert!(
            store
                .elements_at_bounded("test", ElementKind::Concept, 0, 10)
                .await
                .unwrap()
                .0
                .is_empty()
        );
    }
}
