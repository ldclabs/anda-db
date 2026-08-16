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
use anda_kip::{AsOf, ChangesCommand, HistoryCommand, Json, KipError, KipErrorCode, Scalar};

use super::Answer;
use super::describe::{scalar_str, scalar_usize};
use crate::kql::Context;
use crate::store::rows::TransactionRow;

/// `SNAPSHOT [AS OF ...]` — the coordinate a later read can bind to.
pub async fn snapshot(cx: &mut Context<'_>, as_of: Option<&AsOf>) -> Result<Answer, KipError> {
    if as_of.is_some() {
        return Err(KipError::new(
            KipErrorCode::HistoricalSnapshotUnavailable,
            "this engine retains no historical snapshots; SNAPSHOT reports the current \
             coordinate only",
        ));
    }
    let space = cx.store.get_space(&cx.space).await?;
    Ok(Answer::whole(serde_json::json!({
        "space_id": space.space_id,
        "snapshot_seq": space.seq,
        "schema_environment_version": space.schema_environment_version,
        // No token: a token promises a later read can be bound to this
        // coordinate, and this engine cannot honour that.
        "snapshot_token": Json::Null,
        "note": "the current committed coordinate; it cannot be re-read later",
    })))
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

    let from = bound(cx, from_seq, 0)?;
    let to = bound(cx, to_seq, u64::MAX)?;
    let limit = match limit {
        Some(scalar) => scalar_usize(cx, scalar, "LIMIT")?,
        None => usize::MAX,
    };
    let offset = match cursor {
        Some(scalar) => scalar_usize(cx, scalar, "CURSOR")?,
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
        next_cursor: (consumed < total).then(|| consumed.to_string()),
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
            (
                scalar_usize(cx, cursor, "CHANGES SINCE")? as u64,
                limit.as_ref(),
            )
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

    let last = rows.iter().take(limit).map(|row| row.seq).next_back();
    let more = rows.len() > limit;
    let page: Vec<Json> = rows
        .into_iter()
        .take(limit)
        .map(|row| entry(&row, None))
        .collect();

    Ok(Answer {
        result: Json::Array(page),
        // The cursor advances to the last sequence delivered, so resuming
        // never redelivers and never skips.
        next_cursor: more.then(|| last.unwrap_or(after).to_string()),
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
    Ok(entry(&row, None))
}

/// `DESCRIBE TRANSACTION BY IDEMPOTENCY KEY` — the lost-response lookup (§80.4).
pub async fn transaction_by_key(cx: &mut Context<'_>, key: &str) -> Result<Json, KipError> {
    let row = cx
        .store
        .find_transaction_by_idempotency_key(&cx.space, key)
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
    Ok(entry(&row, None))
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
fn entry(row: &TransactionRow, element: Option<&str>) -> Json {
    let changes: Vec<Json> = match element {
        Some(id) => row
            .changes
            .iter()
            .filter(|change| change.get("id").and_then(Json::as_str) == Some(id))
            .cloned()
            .collect(),
        None => row.changes.clone(),
    };
    serde_json::json!({
        "tx_id": row.tx_id,
        "space_id": row.space,
        "space_seq": row.seq,
        "snapshot_seq": row.snapshot_seq,
        "committed_at": row.committed_at,
        "status": row.status,
        "transaction_class": row.transaction_class,
        "schema_environment_version": row.schema_environment_version,
        "changes": changes,
    })
}

fn bound(cx: &Context<'_>, scalar: Option<&Scalar>, default: u64) -> Result<u64, KipError> {
    Ok(match scalar {
        Some(scalar) => scalar_usize(cx, scalar, "SEQ")? as u64,
        None => default,
    })
}
