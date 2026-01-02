use anda_cognitive_nexus::{CognitiveNexus, ConceptPK};
use anda_db::{
    collection::{Collection, CollectionConfig},
    database::AndaDB,
    error::DBError,
    index::BTree,
    query::{Filter, Query, RangeQuery},
    unix_ms,
};
use anda_db_schema::{
    AndaDBSchema, BoxError, FieldEntry, FieldType, Fv, Json, Schema, SchemaError,
};
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

#[derive(Debug, Clone)]
pub struct Nexus {
    nexus: Arc<CognitiveNexus>,
    logs: Arc<Collection>,
}

impl Nexus {
    pub async fn connect(db: Arc<AndaDB>) -> Result<Self, BoxError> {
        let id = "uuc56-gyb".to_string(); // Principal::from_slice(&[1])
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

        let _ = self.logs.add_from(&log).await;
        let _ = self.logs.flush(timestamp).await;
        res
    }

    pub async fn list_logs(
        &self,
        request: ListLogParams,
    ) -> Result<(Vec<KIPLog>, Option<String>), BoxError> {
        let limit = request.limit.unwrap_or(10).min(100);
        let cursor = (BTree::from_cursor::<u64>(&request.cursor)?).unwrap_or_default();
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
            .await?;
        let cursor = if rt.len() >= limit {
            BTree::to_cursor(&rt.last().unwrap()._id)
        } else {
            None
        };
        Ok((rt, cursor))
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_conversation_status() {}
}
