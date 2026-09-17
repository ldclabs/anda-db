//! # History, changes and snapshots
//!
//! All three read the transaction journal, and all three report the same
//! coordinate: the Space sequence. One commit takes one sequence, so
//! "everything since sequence N" and "everything after transaction T" are the
//! same question asked two ways.
//!
//! `HISTORY` is chronology — what happened to this element, in order.
//! `CHANGES` is a stream — what happened after a coordinate the caller already
//! holds. The difference matters for a follower: a stream that restarted from
//! the beginning would replay work the caller already did.

use anda_db::query::{Filter, RangeQuery};
use anda_db_schema::Fv;
use anda_kip::{
    AsOf, ChangeEntry, ChangesCommand, HistoryCommand, Json, KipError, KipErrorCode, Scalar,
};

use super::Answer;
use super::describe::{scalar_json, scalar_str, scalar_usize};
use crate::kql::Context;
use crate::store::history::CursorFamily;
use crate::store::rows::TransactionRow;

/// `DESCRIBE SNAPSHOT [AS OF SEQ :s | AT TIME :t]` — a snapshot coordinate
/// (§68).
///
/// Without an operand it describes the current head; `AS OF SEQ` a past
/// coordinate; `AT TIME` resolves an instant to the last sequence committed
/// at or before it, which is how wall-clock time enters `AS OF SEQ` (§48.1).
/// A sequence the Space has not reached is refused rather than rounded down
/// to the present. The coordinate is a description: the sequence, the
/// transaction that committed it and when, the schema environment in force —
/// and the token a later read may bind to (§78).
pub async fn snapshot(
    cx: &mut Context<'_>,
    as_of: Option<&AsOf>,
    at_time: Option<&Scalar>,
) -> Result<Answer, KipError> {
    let space = cx.store.get_space(&cx.space).await?;
    let seq = match (as_of, at_time) {
        (Some(as_of), _) => cx.resolve_as_of(as_of).await?,
        (None, Some(scalar)) => {
            let at = scalar_str(cx, scalar, "DESCRIBE SNAPSHOT AT TIME")?;
            let at = crate::time::normalize(&at, "AT TIME")?;
            // This engine keeps every version, so no instant is below a
            // retention floor: a time before the first commit is sequence 0,
            // an empty Space rather than an error.
            cx.store.seq_at_time(&cx.space, &at).await?
        }
        (None, None) => space.seq,
    };
    if seq > space.seq {
        return Err(KipError::new(
            KipErrorCode::HistoricalSnapshotUnavailable,
            format!(
                "this Space is at sequence {}, so {seq} is not a coordinate it has reached",
                space.seq
            ),
        ));
    }
    let coordinate = crate::store::history::Coordinate { seq };
    let committed = cx.store.transaction_at_seq(&cx.space, seq).await?;
    let schema_version = cx.store.schema_version_at(&cx.space, seq).await?;
    // The token is a promise the engine keeps: a later read carrying it in
    // `read.snapshot_token` answers at this coordinate.
    Ok(Answer::whole(crate::store::history::snapshot_json(
        &cx.space,
        coordinate,
        committed.as_ref(),
        schema_version,
    )))
}

/// `HISTORY ELEMENT` and `HISTORY SPACE`.
pub async fn history(cx: &mut Context<'_>, command: &HistoryCommand) -> Result<Answer, KipError> {
    let (element, from_seq, to_seq, limit, cursor) = match command {
        HistoryCommand::Element {
            value,
            from_seq,
            to_seq,
            limit,
            cursor,
        } => (
            Some(scalar_str(cx, value, "HISTORY ELEMENT")?),
            from_seq.as_ref(),
            to_seq.as_ref(),
            limit.as_ref(),
            cursor.as_ref(),
        ),
        HistoryCommand::Space {
            from_seq,
            to_seq,
            limit,
            cursor,
        } => (
            None,
            from_seq.as_ref(),
            to_seq.as_ref(),
            limit.as_ref(),
            cursor.as_ref(),
        ),
    };

    // Through the read path's choke point, so an element this caller may not
    // read answers exactly as one that was never written does (§30.4).
    // Answering `[]` for both would be equally non-disclosing but less useful:
    // an empty page already means "nothing in this range", so a mistyped id
    // would come back as silence instead of as a mistake.
    if let Some(named) = &element {
        let id = named.parse::<crate::id::ElementId>()?;
        if cx.load(id).await?.is_none() {
            return Err(KipError::not_found_or_not_visible(format!(
                "no element {id}"
            )));
        }
    }

    let from = bound(cx, from_seq, 0)?;
    let to = bound(cx, to_seq, u64::MAX)?;
    let limit = match limit {
        Some(scalar) => scalar_usize(cx, scalar, "LIMIT")?,
        None => usize::MAX,
    };
    let offset = match cursor {
        Some(scalar) => super::read_cursor(cx, scalar, CursorFamily::History)?.offset,
        None => 0,
    };

    let mut filters = vec![Box::new(crate::store::eq_field(
        "space",
        Fv::Text(cx.space.clone()),
    ))];
    if let Some(id) = &element {
        // The journal records which elements each transaction touched, so an
        // element's chronology is an index lookup rather than a scan.
        filters.push(Box::new(crate::store::eq_field(
            "changed_ids",
            Fv::Text(id.clone()),
        )));
    }
    let mut rows = journal(cx, Filter::And(filters)).await?;
    rows.retain(|row| row.seq >= from && row.seq <= to);
    rows.sort_by_key(|row| row.seq);
    // Before the total is computed, so a page count cannot report entries the
    // page itself will not contain (§104).
    visible_changes(cx, &mut rows).await?;

    let total = rows.len();
    let page: Vec<Json> = rows
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|row| entry(&row, element.as_deref()))
        .collect();
    let consumed = offset + page.len();

    Ok(Answer {
        result: Json::Array(page),
        next_cursor: super::next_cursor(cx, CursorFamily::History, consumed, total),
        warnings: Vec::new(),
    })
}

/// `CHANGES SINCE` and `CHANGES AFTER SEQ` — the follower's stream.
pub async fn changes(cx: &mut Context<'_>, command: &ChangesCommand) -> Result<Answer, KipError> {
    let (after, limit) = match command {
        ChangesCommand::AfterSeq { seq, limit } => (
            scalar_usize(cx, seq, "CHANGES AFTER SEQ")? as u64,
            limit.as_ref(),
        ),
        ChangesCommand::Since { cursor, limit } => {
            // The cursor this engine issues *is* the sequence, so a caller can
            // reason about it — but it is still parsed rather than trusted.
            (change_cursor(cx, cursor)?, limit.as_ref())
        }
    };
    let limit = match limit {
        Some(scalar) => scalar_usize(cx, scalar, "LIMIT")?,
        None => 100,
    };

    let mut rows = journal(
        cx,
        Filter::And(vec![
            Box::new(crate::store::eq_field("space", Fv::Text(cx.space.clone()))),
            Box::new(Filter::Field((
                "seq".to_string(),
                RangeQuery::Gt(Fv::U64(after)),
            ))),
        ]),
    )
    .await?;
    rows.sort_by_key(|row| row.seq);
    let floor = cx
        .store
        .control_at(&cx.space, "internal/governance", u64::MAX)
        .await?
        .and_then(|r| r.value["coverage_floor"].as_u64())
        .unwrap_or(0);
    let complete = rows.len() <= limit && after >= floor;
    rows.truncate(limit);

    // The coordinate this page *consumed*, read before the visibility filter
    // and not after it. They differ for a restricted caller whose authority
    // hides a whole page of transactions: a cursor taken from the visible rows
    // would leave it exactly where it started, and the follower would re-read
    // the same hidden window forever instead of walking past it.
    let consumed = rows.last().map(|row| row.seq);
    visible_changes(cx, &mut rows).await?;

    let coverage = serde_json::json!({"through_seq":if complete {cx.pinned_seq} else {consumed.unwrap_or(after)},"complete":complete,"authorization_view":cx.projection_basis(&cx.policy,&cx.at,None).authorization_view});
    let page: Vec<Json> = rows
        .iter()
        .map(|row| {
            let mut v = entry(row, None);
            v["coverage"] = coverage.clone();
            v
        })
        .collect();

    Ok(Answer {
        result: Json::Array(page),
        // Issued whenever the page consumed anything, not only when the stream
        // was truncated: a follower that has caught up still needs to know
        // where it got to, and deriving that from the envelopes is work only it
        // can get wrong.
        next_cursor: consumed.map(|seq| seq.to_string()),
        warnings: Vec::new(),
    })
}

/// Reads a `CHANGES SINCE` cursor: the sequence the previous page consumed.
///
/// A token this engine did not issue — anything but a non-negative sequence,
/// spelled as a number or as the numeric string `next_cursor` carried — is
/// `CursorInvalid` with `family: "changes"` and `reason: "malformed"` (§87.7).
fn change_cursor(cx: &Context<'_>, scalar: &Scalar) -> Result<u64, KipError> {
    let value = scalar_json(cx, scalar)?;
    let parsed = match &value {
        Json::Number(number) => number.as_u64(),
        Json::String(text) => text.parse::<u64>().ok(),
        _ => None,
    };
    parsed.ok_or_else(|| {
        KipError::cursor_invalid(
            "changes",
            "malformed",
            format!("{value} is not a change cursor this engine issued"),
        )
    })
}

/// `DESCRIBE TRANSACTION`.
pub async fn transaction(cx: &mut Context<'_>, tx_id: &str) -> Result<Json, KipError> {
    let row = cx.store.find_transaction(tx_id).await?.ok_or_else(|| {
        KipError::new(
            KipErrorCode::TransactionUnknown,
            format!("this Nexus has no transaction {tx_id:?}"),
        )
    })?;
    Ok(described(&row))
}

/// `DESCRIBE TRANSACTION BY IDEMPOTENCY KEY` — the lost-response lookup (§80.4).
pub async fn transaction_by_key(cx: &mut Context<'_>, key: &str) -> Result<Json, KipError> {
    let row = crate::kml::find_transaction_for_key(cx.store, &cx.space, cx.auth, key)
        .await?
        .ok_or_else(|| {
            KipError::new(
                KipErrorCode::TransactionUnknown,
                format!(
                    "no transaction in this Space committed under the idempotency key {key:?}; \
                     the original request never committed, so it is safe to send again"
                ),
            )
        })?;
    Ok(described(&row))
}

/// One transaction, as `DESCRIBE TRANSACTION` answers it.
///
/// The Change Envelope shape (§36.1) plus the two facts a *description* is
/// asked for and a *stream entry* is not: whether it committed, and the
/// coordinate it was decided against. §80.4's whole use for this command is
/// "did my write land", and a caller reading the envelope's namespaced
/// extension to answer that would be reading around the answer rather than at
/// it. The envelope keeps the schema's shape; this adds to it.
fn described(row: &TransactionRow) -> Json {
    let mut described = entry(row, None);
    if let Some(object) = described.as_object_mut() {
        object.insert("status".to_string(), Json::from(row.status.clone()));
        object.insert("snapshot_seq".to_string(), Json::from(row.snapshot_seq));
    }
    described
}

async fn journal(cx: &Context<'_>, filter: Filter) -> Result<Vec<TransactionRow>, KipError> {
    let collection = cx.store.transactions();
    let ids = collection
        .query_all_ids(filter)
        .await
        .map_err(crate::error::db_error)?;
    let mut rows = Vec::with_capacity(ids.len());
    for id in ids {
        rows.push(
            collection
                .get_as(id)
                .await
                .map_err(crate::error::db_error)?,
        );
    }
    Ok(rows)
}

/// One journal entry, narrowed to one element when the caller asked about one.
/// Drops the change records for elements this caller may not read.
///
/// A transaction's change list names element ids, so an unfiltered history is
/// an existence channel for a Principal whose read authority is narrower than
/// the Space (§103). Only restricted callers pay for the check: for one whose
/// authority reaches the whole Space there is nothing to filter, and the whole
/// journal is already theirs to read.
///
/// A change to an element that has since been erased disappears from a
/// restricted caller's history, because there is nothing left to authorize
/// against. That is the conservative direction, and it is why the check is
/// skipped entirely for the unrestricted case rather than being applied
/// uniformly and losing history for everyone.
async fn visible_changes(
    cx: &mut Context<'_>,
    rows: &mut Vec<TransactionRow>,
) -> Result<(), KipError> {
    if cx.authority.reads_whole_space(cx.auth) {
        return Ok(());
    }
    for row in rows.iter_mut() {
        let mut kept = Vec::with_capacity(row.changes.len());
        for change in &row.changes {
            let Some(id) = change.get("id").and_then(Json::as_str) else {
                // A change record with no element — a no-op entry — discloses
                // nothing on its own.
                kept.push(change.clone());
                continue;
            };
            let parsed = id.parse::<crate::id::ElementId>()?;
            if let Some(element) = cx.load_unattached(parsed).await?
                && let Some(visibility) = cx
                    .authority
                    .may_read(&element, cx.auth)
                    .filter(|v| v.content)
            {
                let mut visible = change.clone();
                if !visibility.constraints.fields.is_empty() {
                    let fields = &visibility.constraints.fields;
                    if let Some(paths) = visible["touched"].as_array_mut() {
                        paths.retain(|p| {
                            p.as_str().is_some_and(|p| {
                                fields.iter().any(|f| {
                                    f == p
                                        .strip_prefix("fields.")
                                        .unwrap_or(p)
                                        .split('.')
                                        .next()
                                        .unwrap_or("")
                                })
                            })
                        });
                    }
                    if let Some(map) = visible.as_object_mut() {
                        map.remove("refs");
                        map.remove("planes");
                        map.remove("state");
                    }
                }
                kept.push(visible);
            }
        }
        row.changes = kept;
    }
    // A transaction whose every change is hidden is one this caller has no
    // business knowing happened.
    rows.retain(|row| {
        !row.changes.is_empty()
            || row.result["control_changes"]
                .as_array()
                .is_some_and(|c| !c.is_empty())
            || row.result.get("schema_environment_version").is_some()
    });
    Ok(())
}

/// Host page shape also carries coverage when no visible commits were returned.
pub(crate) async fn change_page(
    cx: &mut Context<'_>,
    after: u64,
    limit: usize,
) -> Result<Json, KipError> {
    change_page_through(cx, after, cx.pinned_seq, limit).await
}

/// A fixed inclusive coverage target; later commits cannot move a deadline.
pub(crate) async fn change_page_through(
    cx: &mut Context<'_>,
    after: u64,
    target: u64,
    limit: usize,
) -> Result<Json, KipError> {
    if limit == 0 || limit > 10000 || after > target || target > cx.pinned_seq {
        return Err(KipError::constraint_violation("invalid change page bounds"));
    }
    let mut rows = journal(
        cx,
        Filter::And(vec![
            Box::new(crate::store::eq_field("space", Fv::Text(cx.space.clone()))),
            Box::new(Filter::Field((
                "seq".into(),
                RangeQuery::Gt(Fv::U64(after)),
            ))),
            Box::new(Filter::Field((
                "seq".into(),
                RangeQuery::Le(Fv::U64(target)),
            ))),
        ]),
    )
    .await?;
    rows.sort_by_key(|r| r.seq);
    let floor = cx
        .store
        .control_at(&cx.space, "internal/governance", u64::MAX)
        .await?
        .and_then(|r| r.value["coverage_floor"].as_u64())
        .unwrap_or(0);
    let complete = rows.len() <= limit && after >= floor;
    rows.truncate(limit);
    let through = if complete {
        target
    } else {
        rows.last().map_or(after, |r| r.seq)
    };
    visible_changes(cx, &mut rows).await?;
    let coverage = serde_json::json!({"through_seq":through,"complete":complete,"authorization_view":cx.projection_basis(&cx.policy,&cx.at,None).authorization_view});
    let changes: Vec<_> = rows
        .iter()
        .map(|r| {
            let mut value = entry(r, None);
            value["coverage"] = coverage.clone();
            value
        })
        .collect();
    Ok(
        serde_json::json!({"changes":changes,"coverage":coverage,"through_time":cx.at,"next_cursor":through.to_string(),"resync_required":after<floor}),
    )
}

/// One journal row as the Change Envelope §36.1 fixes.
///
/// Built through [`anda_kip::ChangeEnvelope`] rather than by hand so `HISTORY
/// ELEMENT`, `HISTORY SPACE` and `CHANGES` cannot answer the same question in
/// three shapes — and so the other engine, which builds the same type, cannot
/// answer it in a fourth.
///
/// `element` narrows the `changes` list to the one the caller asked about. The
/// envelope still describes the whole transition, because that is what a
/// transition is (§36.2); what is filtered is which of its changes are
/// relevant to this chronology.
fn entry(row: &TransactionRow, element: Option<&str>) -> Json {
    // The journal stores each change in the entry shape it was committed
    // with; one that does not decode is dropped rather than rendered in a
    // shape no consumer was promised.
    let changes: Vec<ChangeEntry> = row
        .changes
        .iter()
        .filter(|change| match element {
            Some(id) => change.get("id").and_then(Json::as_str) == Some(id),
            None => true,
        })
        .filter_map(|change| serde_json::from_value(change.clone()).ok())
        .collect();
    let mut extensions = anda_kip::Map::new();
    extensions.insert(
        anda_kip::CHANGE_TRANSITION_EXTENSION.to_string(),
        Json::Object(anda_kip::ChangeEnvelope::transition_detail(
            row.snapshot_seq,
            &row.status,
        )),
    );
    let mut controls: Vec<anda_kip::ControlChange> = serde_json::from_value(
        row.result
            .get("control_changes")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([])),
    )
    .unwrap_or_default();
    if row.transaction_class == "governance"
        && row.result.get("schema_environment_version").is_some()
    {
        controls.push(anda_kip::ControlChange {
            kind: "schema".into(),
            version: row.schema_environment_version.to_string(),
        });
    }
    if row.changes.iter().any(|c| c["op"] == "merge") {
        controls.push(anda_kip::ControlChange {
            kind: "identity".into(),
            version: row.seq.to_string(),
        });
    }
    let envelope = anda_kip::ChangeEnvelope {
        control_changes: controls,
        coverage: None,
        kip: Some("2.0".to_string()),
        space_id: row.space.clone(),
        space_seq: row.seq,
        tx_id: row.tx_id.clone(),
        committed_at: Some(row.committed_at.clone()),
        transaction_class: Some(row.transaction_class.clone()),
        schema_environment_version: Some(row.schema_environment_version),
        changes,
        extensions: Some(extensions),
    };
    serde_json::to_value(&envelope).unwrap_or(Json::Null)
}

fn bound(cx: &Context<'_>, scalar: Option<&Scalar>, default: u64) -> Result<u64, KipError> {
    Ok(match scalar {
        Some(scalar) => scalar_usize(cx, scalar, "SEQ")? as u64,
        None => default,
    })
}
