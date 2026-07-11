use anda_cognitive_nexus::{CognitiveNexus, ConceptPK};
use anda_db::{
    collection::{Collection, CollectionConfig},
    database::AndaDB,
    error::DBError,
    index::BTree,
    query::{Filter, Query, RangeQuery},
    unix_ms,
};
use anda_db_schema::{AndaDBSchema, BoxError, Fv, Json};
use anda_kip::{
    CommandType, META_SELF_NAME, PERSON_SELF_KIP, PERSON_SYSTEM_KIP, PERSON_TYPE, Request,
    Response, parse_kml,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

#[derive(Debug, Deserialize, Serialize, AndaDBSchema)]
pub struct KIPLog {
    pub _id: u64,
    #[field_type = "Text"]
    pub command: CommandType,
    #[field_type = "Map<String, Json>"]
    pub request: Request,
    pub response: Json,
    pub period: u64,
    pub timestamp: u64,
}

#[derive(Debug, Serialize)]
pub struct KIPLogRef<'a> {
    pub _id: u64,
    pub command: CommandType,
    pub request: &'a Request,
    pub response: Json,
    pub period: u64,
    pub timestamp: u64,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ListLogParams {
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Error from [`Nexus::list_logs`], distinguishing client input errors from
/// internal failures so the handler can map them to 400 vs 500.
#[derive(Debug)]
pub enum ListLogsError {
    /// The supplied cursor could not be decoded.
    InvalidCursor(String),
    /// Engine or storage failure.
    Internal(BoxError),
}

#[derive(Debug, Clone)]
pub struct Nexus {
    nexus: Arc<CognitiveNexus>,
    logs: Arc<Collection>,
}

impl Nexus {
    /// Connects to the cognitive nexus, initializing the `$self` genesis KML
    /// with `self_principal_id` on first start.
    pub async fn connect(db: Arc<AndaDB>, self_principal_id: String) -> Result<Self, BoxError> {
        let id = self_principal_id;
        let nexus = CognitiveNexus::connect(db.clone(), async |nexus| {
            if !nexus
                .has_concept(&ConceptPK::Object {
                    r#type: PERSON_TYPE.to_string(),
                    name: META_SELF_NAME.to_string(),
                })
                .await
            {
                let kml = &[
                    &PERSON_SELF_KIP.replace("$self_reserved_principal_id", &id),
                    PERSON_SYSTEM_KIP,
                ]
                .join("\n");

                let result = nexus.execute_kml(parse_kml(kml)?, false).await?;
                log::info!(result:serde = result; "Init $self and $system");
            }

            Ok(())
        })
        .await?;

        let schema = KIPLog::schema()?;
        let logs = db
            .open_or_create_collection(
                schema,
                CollectionConfig {
                    name: "kip_logs".to_string(),
                    description: "KIP logs collection".to_string(),
                },
                async |collection| {
                    // create BTree indexes if not exists
                    collection.create_btree_index_nx(&["period"]).await?;
                    Ok::<(), DBError>(())
                },
            )
            .await?;

        Ok(Self {
            nexus: Arc::new(nexus),
            logs,
        })
    }

    pub async fn execute_kip(&self, request: Request) -> Response {
        let timestamp = unix_ms();

        let (command, res) = request.execute(self.nexus.as_ref()).await;
        let log = KIPLogRef {
            _id: 0, // This will be set by the database
            command,
            request: &request,
            response: match &res {
                Response::Ok { .. } => json!({"result": "..."}),
                Response::Err { error, .. } => json!({"error": error}),
            },
            period: timestamp / 3600 / 1000,
            timestamp,
        };

        // Log persistence is best-effort but must not be silent; durability
        // is handled by the periodic `AndaDB::auto_flush` task instead of a
        // per-request flush.
        if let Err(err) = self.logs.add_from(&log).await {
            log::error!(
                action = "Nexus::execute_kip";
                "failed to record KIP log: {err:?}",
            );
        }
        res
    }

    /// Lists persisted KIP logs in ascending `_id` order with cursor
    /// pagination.
    ///
    /// `limit` semantics: `None` defaults to 10 documents, values above 100
    /// are capped at 100, and `limit == 0` means "no data requested" — it
    /// returns an empty page with no cursor instead of being bumped to one
    /// document.
    pub async fn list_logs(
        &self,
        request: ListLogParams,
    ) -> Result<(Vec<KIPLog>, Option<String>), ListLogsError> {
        let limit = match request.limit {
            Some(0) => return Ok((Vec::new(), None)),
            Some(limit) => limit.min(100),
            None => 10,
        };
        let cursor = BTree::from_cursor::<u64>(&request.cursor)
            .map_err(|err| ListLogsError::InvalidCursor(err.to_string()))?
            .unwrap_or_default();
        let filter = Some(Filter::Field((
            "_id".to_string(),
            RangeQuery::Gt(Fv::U64(cursor)),
        )));

        let rt: Vec<KIPLog> = self
            .logs
            .search_as(Query {
                filter,
                limit: Some(limit),
                search: None,
            })
            .await
            .map_err(|err| ListLogsError::Internal(err.into()))?;
        let cursor = if rt.len() >= limit {
            rt.last().and_then(|log| BTree::to_cursor(&log._id))
        } else {
            None
        };
        Ok((rt, cursor))
    }

    /// Deletes logs whose `period` is strictly less than `before_period`
    /// (hours since the Unix epoch), in batches. Returns the number of
    /// deleted documents.
    ///
    /// Defensive termination: if a whole batch of matching ids deletes
    /// nothing (a pathological index/document mismatch would make
    /// `query_ids` keep returning the same undeletable ids), the loop exits
    /// instead of spinning without backoff; the next scheduled prune run
    /// retries.
    pub async fn prune_logs(&self, before_period: u64) -> Result<usize, BoxError> {
        let mut total = 0usize;
        loop {
            let ids = self
                .logs
                .query_ids(
                    Filter::Field(("period".to_string(), RangeQuery::Lt(Fv::U64(before_period)))),
                    Some(1000),
                )
                .await?;
            if ids.is_empty() {
                return Ok(total);
            }
            let mut deleted_this_batch = 0usize;
            for id in ids {
                if self.logs.remove(id).await?.is_some() {
                    deleted_this_batch += 1;
                }
            }
            if deleted_this_batch == 0 {
                log::warn!(
                    action = "Nexus::prune_logs";
                    "prune batch made no progress (index returned ids with no removable documents); stopping this run",
                );
                return Ok(total);
            }
            total += deleted_this_batch;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anda_db::{database::DBConfig, storage::StorageConfig};
    use object_store::memory::InMemory;

    async fn test_nexus() -> Nexus {
        let store = Arc::new(InMemory::new());
        let db = AndaDB::connect(
            store,
            DBConfig {
                name: "test_db".to_string(),
                description: "test".to_string(),
                storage: StorageConfig::default(),
                lock: None,
            },
        )
        .await
        .unwrap();
        Nexus::connect(Arc::new(db), "uuc56-gyb".to_string())
            .await
            .unwrap()
    }

    async fn add_log(nexus: &Nexus, period: u64) {
        let log = KIPLog {
            _id: 0,
            command: CommandType::Meta,
            request: Request::default(),
            response: serde_json::json!({"result": "..."}),
            period,
            timestamp: period * 3600 * 1000,
        };
        nexus.logs.add_from(&log).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn list_logs_is_safe_on_empty_collection_and_limit_zero() {
        let nexus = test_nexus().await;
        // `limit: Some(0)` returns an empty page; the others an empty
        // collection.
        for limit in [Some(0), Some(1), Some(1000), None] {
            let (logs, cursor) = nexus
                .list_logs(ListLogParams {
                    cursor: None,
                    limit,
                })
                .await
                .unwrap();
            assert!(logs.is_empty());
            assert!(cursor.is_none());
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn list_logs_paginates_and_rejects_invalid_cursor() {
        let nexus = test_nexus().await;
        for period in 1..=3u64 {
            add_log(&nexus, period).await;
        }

        // limit=0 means "no data": an empty page and no cursor, even when
        // documents exist.
        let (logs, cursor) = nexus
            .list_logs(ListLogParams {
                cursor: None,
                limit: Some(0),
            })
            .await
            .unwrap();
        assert!(logs.is_empty());
        assert!(cursor.is_none());

        // limit=1 pages one document at a time.
        let (logs, cursor) = nexus
            .list_logs(ListLogParams {
                cursor: None,
                limit: Some(1),
            })
            .await
            .unwrap();
        assert_eq!(logs.len(), 1);
        let first_id = logs[0]._id;
        assert!(cursor.is_some());

        // The cursor continues after the previous page.
        let (logs, _) = nexus
            .list_logs(ListLogParams {
                cursor,
                limit: Some(10),
            })
            .await
            .unwrap();
        assert_eq!(logs.len(), 2);
        assert!(logs.iter().all(|log| log._id > first_id));

        // Invalid cursors are client errors, not internal ones.
        let err = nexus
            .list_logs(ListLogParams {
                cursor: Some("!!! not base64 !!!".to_string()),
                limit: None,
            })
            .await;
        assert!(matches!(err, Err(ListLogsError::InvalidCursor(_))));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prune_logs_removes_expired_periods_only() {
        let nexus = test_nexus().await;
        for period in [1u64, 2, 10, 11] {
            add_log(&nexus, period).await;
        }

        let pruned = nexus.prune_logs(10).await.unwrap();
        assert_eq!(pruned, 2);

        let (logs, _) = nexus
            .list_logs(ListLogParams {
                cursor: None,
                limit: Some(100),
            })
            .await
            .unwrap();
        assert_eq!(logs.len(), 2);
        assert!(logs.iter().all(|log| log.period >= 10));

        // Idempotent when nothing is expired.
        assert_eq!(nexus.prune_logs(10).await.unwrap(), 0);
    }
}
