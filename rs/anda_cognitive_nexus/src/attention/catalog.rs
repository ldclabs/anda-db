use super::*;
use anda_db::query::{Filter, RangeQuery};
use anda_db_schema::Fv;

/// One bounded discovery scan. A complete page exhausts its authorized snapshot,
/// not processing obligations. Claims must re-read current versions and fences.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WakePage {
    pub items: Vec<WakeRecord>,
    pub snapshot_seq: u64,
    pub scanned: usize,
    pub next_cursor: Option<String>,
    pub complete: bool,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    format: String,
    space: String,
    principal: String,
    instance: Option<String>,
    basis: String,
    snapshot_seq: u64,
    after_id: u64,
}

impl Session {
    /// Read-only, index-backed pagination over native wake history. Empty
    /// intermediate pages are valid; callers must follow next_cursor.
    pub async fn list_wakes(
        &self,
        space: &str,
        cursor: Option<&str>,
        scan_limit: usize,
    ) -> Result<WakePage, KipError> {
        if !(1..=200).contains(&scan_limit) {
            return Err(invalid("wake scan limit must be 1..=200"));
        }
        let _guard = self.nexus.read_guard().await?;
        let authority = self.effective_authority(space).await?;
        authority
            .authorize(
                Permission::Maintain,
                &ResourceContext::default(),
                &self.auth,
            )
            .into_result()?;
        let cx =
            Context::open(&self.nexus.store, space, None, None, &authority, &self.auth).await?;
        let current_basis = digest(&basis(&cx))?;
        let instance = self
            .nexus
            .store
            .control_at(space, CONFIG, u64::MAX)
            .await?
            .and_then(|r| {
                r.value["scope"]["space_instance"]
                    .as_str()
                    .map(str::to_owned)
            });
        let mut position = if let Some(cursor) = cursor {
            if cursor.len() > 8192 {
                return Err(invalid("wake cursor exceeds bound"));
            }
            let bytes = hex::decode(cursor).map_err(|_| invalid("invalid wake cursor"))?;
            serde_json::from_slice::<Cursor>(&bytes).map_err(|_| invalid("invalid wake cursor"))?
        } else {
            Cursor {
                format: "nexus:wake-cursor-v1".into(),
                space: space.into(),
                principal: self.auth.principal_id.clone(),
                instance: instance.clone(),
                basis: current_basis.clone(),
                snapshot_seq: cx.pinned_seq,
                after_id: 0,
            }
        };
        if position.format != "nexus:wake-cursor-v1"
            || position.space != space
            || position.principal != self.auth.principal_id
            || position.instance != instance
            || position.basis != current_basis
            || position.snapshot_seq > cx.pinned_seq
            || position.after_id > anda_kip::MAX_SAFE_INTEGER
        {
            return Err(conflict("wake_cursor_basis_changed"));
        }
        let table = self.nexus.store.control_records();
        let ids = table
            .query_ids(
                Filter::And(vec![
                    Box::new(crate::store::eq_field("space", Fv::Text(space.into()))),
                    Box::new(crate::store::eq_field("kind", Fv::Text("wake".into()))),
                    Box::new(Filter::Field((
                        "seq".into(),
                        RangeQuery::Le(Fv::U64(position.snapshot_seq)),
                    ))),
                    Box::new(Filter::Field((
                        "_id".into(),
                        RangeQuery::Gt(Fv::U64(position.after_id)),
                    ))),
                ]),
                Some(scan_limit + 1),
            )
            .await
            .map_err(crate::error::db_error)?;
        let complete = ids.len() <= scan_limit;
        let scanned = ids.len().min(scan_limit);
        let mut items = Vec::new();
        for id in ids.into_iter().take(scan_limit) {
            position.after_id = id;
            let row: ControlRecordRow = table.get_as(id).await.map_err(crate::error::db_error)?;
            // Native controls append versions in commit order. Select one head
            // with a bounded index lookup, not an unbounded history read per row.
            let latest = table
                .query_last_ids(
                    Filter::And(vec![
                        Box::new(crate::store::eq_field("space", Fv::Text(space.into()))),
                        Box::new(crate::store::eq_field("key", Fv::Text(row.key.clone()))),
                        Box::new(Filter::Field((
                            "seq".into(),
                            RangeQuery::Le(Fv::U64(position.snapshot_seq)),
                        ))),
                    ]),
                    Some(1),
                )
                .await
                .map_err(crate::error::db_error)?;
            if latest.first() != Some(&id) {
                continue;
            }
            let wake: WakeRecord =
                serde_json::from_value(row.value).map_err(|_| invalid("corrupt wake record"))?;
            if wake.wake_ref != row.key || wake.version != row.version {
                return Err(invalid("wake identity mismatch"));
            }
            let mut visible = true;
            for reference in [&wake.fire.watch_ref, &wake.fire_activity_ref] {
                let source = self.nexus.store.get_element(reference.parse()?).await?;
                visible &= source.space() == space
                    && authority
                        .may_read(&source, &self.auth)
                        .is_some_and(|v| v.content && v.constraints.fields.is_empty());
            }
            if visible {
                items.push(wake);
            }
        }
        Ok(WakePage {
            items,
            snapshot_seq: position.snapshot_seq,
            scanned,
            next_cursor: if complete {
                None
            } else {
                Some(hex::encode(
                    serde_json::to_vec(&position).map_err(|e| invalid(&e.to_string()))?,
                ))
            },
            complete,
        })
    }
}
