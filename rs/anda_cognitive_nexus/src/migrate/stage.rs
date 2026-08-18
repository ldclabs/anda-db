//! Phase 1 and 2: copy the 1.x rows somewhere durable, then clear the names.

use anda_db::{
    collection::CollectionConfig,
    database::AndaDB,
    error::DBError,
    query::{Filter, Query, RangeQuery},
    schema::{AndaDBSchema, Ft, Fv, Json, Schema},
};
use anda_kip::{KipError, KipErrorCode};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::store::{CONCEPTS, PROPOSITIONS};

/// Where the 1.x rows live between phase 1 and phase 3, and after it.
pub const LEGACY_STAGING: &str = "kip_legacy_v1";

/// What a staged row was in 1.x.
pub mod kind {
    /// A 1.x Concept row.
    pub const CONCEPT: &str = "concept";
    /// A 1.x Proposition row.
    pub const PROPOSITION: &str = "proposition";
    /// The sentinel that records phase 3 finished.
    pub const MARKER: &str = "marker";
}

/// A staged 1.x row, kept verbatim.
///
/// The 1.x document is one opaque `Json` rather than a mirrored struct on
/// purpose. A struct would be a second, drifting definition of a format that is
/// already frozen in history, and it would fail to load exactly the rows worth
/// looking at — the ones some old deployment wrote in a shape nobody remembers.
#[derive(Clone, Debug, Deserialize, Serialize, AndaDBSchema)]
pub struct LegacyRow {
    /// Staging id, unrelated to the 1.x id.
    pub _id: u64,
    /// One of [`kind`].
    #[field_type = "Text"]
    pub kind: String,
    /// The 1.x `_id`, so the mapping to a migrated element stays derivable.
    #[field_type = "U64"]
    pub legacy_id: u64,
    /// The 1.x document as it was stored.
    #[field_type = "Json"]
    pub doc: Json,
}

/// The kinds a caller can ask the staging area for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LegacyKind {
    /// 1.x Concepts.
    Concept,
    /// 1.x Propositions.
    Proposition,
}

impl LegacyKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            LegacyKind::Concept => kind::CONCEPT,
            LegacyKind::Proposition => kind::PROPOSITION,
        }
    }
}

fn db_error(err: DBError) -> KipError {
    KipError::new(KipErrorCode::InternalError, format!("migration: {err}"))
}

async fn init_staging(c: &mut anda_db::collection::Collection) -> Result<(), DBError> {
    c.create_btree_index_nx(&["kind"]).await?;
    c.create_btree_index_nx(&["legacy_id"]).await?;
    Ok(())
}

/// Whether a persisted `concepts` collection is the 1.x one.
///
/// Decided from the schema the collection actually carries, not from a version
/// number: 1.x and 2.0 both derive to schema version 0, which is why nothing
/// upstream catches this. A 1.x Concept has `type` and no `space`; a 2.0 one
/// has `space` and no `type`. Requiring both halves means a half-created
/// collection is not mistaken for either.
fn is_v1_concepts(schema: &Schema) -> bool {
    schema.get_field("type").is_some()
        && schema.get_field("metadata").is_some()
        && schema.get_field("space").is_none()
}

/// Extracts a 1.x layout into staging and drops the colliding collections.
pub(crate) async fn prepare(db: &Arc<AndaDB>) -> Result<(), KipError> {
    let collections = db.metadata().collections;
    if !collections.contains(CONCEPTS) {
        // A fresh database, or one already migrated: nothing occupies the name.
        return Ok(());
    }

    // Opened without a schema, so the persisted 1.x one stays in force and no
    // index is built against fields it does not have. Passing the 2.0 schema
    // here is what fails with a message about a missing `key` field.
    let concepts = db
        .open_collection(CONCEPTS.to_string(), async |_| Ok(()))
        .await
        .map_err(db_error)?;
    if !is_v1_concepts(&concepts.schema()) {
        return Ok(());
    }

    log::warn!(
        action = "migrate::prepare",
        collection = CONCEPTS;
        "KIP 1.x layout detected; extracting into {LEGACY_STAGING} before it is replaced",
    );

    let staging = db
        .open_or_create_collection(
            LegacyRow::schema().map_err(|err| {
                KipError::new(KipErrorCode::InternalError, format!("migration: {err}"))
            })?,
            CollectionConfig {
                name: LEGACY_STAGING.to_string(),
                description: "KIP 1.x rows, kept verbatim across the 2.0 migration".to_string(),
            },
            init_staging,
        )
        .await
        .map_err(db_error)?;

    // A previous attempt may have been interrupted part-way through the copy.
    // The source is still authoritative at this point — nothing has been
    // dropped yet — so the honest repair is to redo the copy from scratch
    // rather than guess which rows made it.
    if !staging.is_empty() {
        log::warn!(
            action = "migrate::prepare";
            "{LEGACY_STAGING} already holds rows and the 1.x source is still present; \
             an earlier extract was interrupted, redoing it",
        );
        for id in staging.ids() {
            staging.remove(id).await.map_err(db_error)?;
        }
    }

    copy_out(&concepts, &staging, kind::CONCEPT).await?;
    if collections.contains(PROPOSITIONS) {
        let propositions = db
            .open_collection(PROPOSITIONS.to_string(), async |_| Ok(()))
            .await
            .map_err(db_error)?;
        copy_out(&propositions, &staging, kind::PROPOSITION).await?;
    }
    staging.flush(unix_ms()).await.map_err(db_error)?;

    let staged = staging.len();
    log::warn!(
        action = "migrate::prepare",
        staged = staged;
        "extracted {staged} KIP 1.x row(s); dropping the 1.x collections",
    );

    // Only now, with a durable copy on the other side of a flush.
    db.delete_collection(CONCEPTS).await.map_err(db_error)?;
    if collections.contains(PROPOSITIONS) {
        db.delete_collection(PROPOSITIONS).await.map_err(db_error)?;
    }
    Ok(())
}

/// Reads a live 1.x layout without staging or changing anything.
///
/// This is what a dry run walks. It deliberately does not go through the
/// staging area: staging is a write, and the whole point of a dry run is that
/// an operator can point it at a production database and learn what would
/// happen without that database becoming different for having been asked.
pub(crate) async fn read_live_v1(
    db: &Arc<AndaDB>,
) -> Result<Option<(Vec<LegacyRow>, Vec<LegacyRow>)>, KipError> {
    let collections = db.metadata().collections;
    if !collections.contains(CONCEPTS) {
        return Ok(None);
    }
    let concepts = db
        .open_collection(CONCEPTS.to_string(), async |_| Ok(()))
        .await
        .map_err(db_error)?;
    if !is_v1_concepts(&concepts.schema()) {
        return Ok(None);
    }

    let mut concept_rows = Vec::new();
    read_into(&concepts, kind::CONCEPT, &mut concept_rows).await?;

    let mut proposition_rows = Vec::new();
    if collections.contains(PROPOSITIONS) {
        let propositions = db
            .open_collection(PROPOSITIONS.to_string(), async |_| Ok(()))
            .await
            .map_err(db_error)?;
        read_into(&propositions, kind::PROPOSITION, &mut proposition_rows).await?;
    }
    Ok(Some((concept_rows, proposition_rows)))
}

async fn read_into(
    source: &Arc<anda_db::collection::Collection>,
    kind: &str,
    out: &mut Vec<LegacyRow>,
) -> Result<(), KipError> {
    for id in source.ids() {
        let doc: Json = source.get_as(id).await.map_err(db_error)?;
        let legacy_id = doc.get("_id").and_then(|v| v.as_u64()).unwrap_or(id);
        out.push(LegacyRow {
            _id: 0,
            kind: kind.to_string(),
            legacy_id,
            doc,
        });
    }
    out.sort_by_key(|row| row.legacy_id);
    Ok(())
}

async fn copy_out(
    source: &Arc<anda_db::collection::Collection>,
    staging: &Arc<anda_db::collection::Collection>,
    kind: &str,
) -> Result<(), KipError> {
    for id in source.ids() {
        let doc: Json = source.get_as(id).await.map_err(db_error)?;
        let legacy_id = doc.get("_id").and_then(|v| v.as_u64()).unwrap_or(id);
        staging
            .add_from(&LegacyRow {
                _id: 0,
                kind: kind.to_string(),
                legacy_id,
                doc,
            })
            .await
            .map_err(db_error)?;
    }
    Ok(())
}

/// Opens the staging collection, or `None` when there is nothing staged.
pub(crate) async fn open(
    db: &Arc<AndaDB>,
) -> Result<Option<Arc<anda_db::collection::Collection>>, KipError> {
    if !db.metadata().collections.contains(LEGACY_STAGING) {
        return Ok(None);
    }
    let staging = db
        .open_collection(LEGACY_STAGING.to_string(), init_staging)
        .await
        .map_err(db_error)?;
    Ok(Some(staging))
}

/// Every staged row of one kind, oldest first.
pub(crate) async fn rows(
    staging: &Arc<anda_db::collection::Collection>,
    kind: LegacyKind,
) -> Result<Vec<LegacyRow>, KipError> {
    let mut rows: Vec<LegacyRow> = staging
        .search_as(Query {
            filter: Some(Filter::Field((
                "kind".to_string(),
                RangeQuery::Eq(Fv::Text(kind.as_str().to_string())),
            ))),
            limit: Some(usize::MAX),
            ..Default::default()
        })
        .await
        .map_err(db_error)?;
    rows.sort_by_key(|row| row.legacy_id);
    Ok(rows)
}

/// Whether phase 3 already finished.
pub(crate) async fn is_complete(
    staging: &Arc<anda_db::collection::Collection>,
) -> Result<bool, KipError> {
    let markers: Vec<LegacyRow> = staging
        .search_as(Query {
            filter: Some(Filter::Field((
                "kind".to_string(),
                RangeQuery::Eq(Fv::Text(kind::MARKER.to_string())),
            ))),
            limit: Some(1),
            ..Default::default()
        })
        .await
        .map_err(db_error)?;
    Ok(!markers.is_empty())
}

/// Records that phase 3 finished, so a later restart skips it.
pub(crate) async fn mark_complete(
    staging: &Arc<anda_db::collection::Collection>,
    summary: Json,
) -> Result<(), KipError> {
    staging
        .add_from(&LegacyRow {
            _id: 0,
            kind: kind::MARKER.to_string(),
            legacy_id: 0,
            doc: summary,
        })
        .await
        .map_err(db_error)?;
    staging.flush(unix_ms()).await.map_err(db_error)?;
    Ok(())
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Keeps `Ft` referenced for the derive's field-type attributes.
#[allow(dead_code)]
fn _ft_marker(_: Ft) {}
