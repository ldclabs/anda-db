use anda_cognitive_nexus::{
    CognitiveNexus,
    nexus::DEFAULT_SPACE,
    profiles::COGNITIVE_MEMORY,
    schema::{PackageState, SchemaLock, SchemaPackage},
};
use anda_db::{
    collection::{Collection, CollectionConfig},
    database::AndaDB,
    error::DBError,
    index::BTree,
    query::{Filter, Query, RangeQuery},
    unix_ms,
};
use anda_db_schema::{AndaDBSchema, BoxError, Fv, Json};
use anda_kip::{CommandType, Operation, Request, Response, execute_request};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{borrow::Cow, sync::Arc};

/// Smallest accepted value for the stored-request size cap. The truncated
/// stand-in itself needs room for its marker.
const MIN_LOGGED_REQUEST_BYTES: usize = 256;

/// Where an installed Schema Package came from, recorded on its package row.
const BUNDLED_SOURCE: &str = "bundled";

#[derive(Debug, Deserialize, Serialize, AndaDBSchema)]
pub struct KIPLog {
    pub _id: u64,
    /// The KIP languages this request actually ran, as parsed — `KQL`, `KML`,
    /// `META`, or a comma-joined set for a mixed batch.
    pub languages: String,
    #[field_type = "Map<String, Json>"]
    pub request: Request,
    pub response: Json,
    pub period: u64,
    pub timestamp: u64,
}

#[derive(Debug, Serialize)]
pub struct KIPLogRef<'a> {
    pub _id: u64,
    pub languages: &'a str,
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

/// A Schema Package artifact to install and activate at startup.
#[derive(Clone, Debug)]
pub struct SchemaPackageSource {
    /// Where the artifact came from — a file path, or `bundled`. Recorded on
    /// the installed package row so `LIST PACKAGES` can say where it entered.
    pub source: String,
    /// The artifact JSON itself.
    pub artifact: String,
}

#[derive(Debug, Clone)]
pub struct Nexus {
    nexus: Arc<CognitiveNexus>,
    logs: Arc<Collection>,
    /// Upper bound on the serialized KIP request stored in one audit
    /// document; see [`truncate_request`].
    max_logged_request_bytes: usize,
}

/// What a request will actually run, classified from the parsed commands.
///
/// Classifying costs a parse that execution then repeats, and the cheap
/// alternative — the caller's `operation.language` — is exactly the field
/// §73.1 says must never be trusted: it is advisory. An audit record that
/// called a mutation a read because the client labelled it one would be worse
/// than no audit record, and the HTTP layer decides how to describe a lost
/// response from the same answer, where the cost of believing a label is a
/// duplicated write.
#[derive(Clone, Debug)]
pub struct RequestLanguages(Vec<CommandType>);

impl RequestLanguages {
    /// Classifies every operation, in first-seen order.
    pub fn of(request: &Request) -> Self {
        let mut seen: Vec<CommandType> = Vec::new();
        for operation in &request.operations {
            let language = operation
                .parse()
                .map_or(CommandType::Unknown, |command| CommandType::from(&command));
            if !seen.contains(&language) {
                seen.push(language);
            }
        }
        if seen.is_empty() {
            seen.push(CommandType::Unknown);
        }
        Self(seen)
    }

    /// Whether any operation is a state-changing one.
    pub fn has_mutation(&self) -> bool {
        self.0.contains(&CommandType::Kml)
    }
}

impl std::fmt::Display for RequestLanguages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, language) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str(",")?;
            }
            write!(f, "{language}")?;
        }
        Ok(())
    }
}

/// Returns the request to persist in the audit log.
///
/// Every `/kip` request appends a durable document containing the client's
/// request envelope, so a client sending bodies near the configured limit would
/// add megabytes of permanent storage (and B-Tree/BM25 memory) per call. Once
/// the serialized request exceeds `max_bytes` it is replaced by a bounded
/// stand-in that still deserializes as a [`Request`], so `list_logs` keeps
/// working; the audit record then states what was dropped instead of storing
/// it. Operators who need full request bodies raise the limit.
///
/// The 2.0 envelope hides bulk in three places, not one: `operations[]`,
/// per-operation `parameters`, and `ingest.evidence[].payload` — which is
/// designed to carry an observed payload verbatim (§71.1) and is therefore the
/// largest of them in practice. The stand-in keeps only correlation and
/// execution metadata plus a prefix of the first command.
fn truncate_request(request: &Request, max_bytes: usize) -> Cow<'_, Request> {
    let max_bytes = max_bytes.max(MIN_LOGGED_REQUEST_BYTES);
    // A request that cannot be serialized at all is stored truncated as well:
    // it certainly cannot be stored verbatim.
    let size = serde_json::to_vec(request).map_or(usize::MAX, |bytes| bytes.len());
    if size <= max_bytes {
        return Cow::Borrowed(request);
    }

    // Keep a prefix of the first command for forensics. Half the budget leaves
    // room for the marker regardless of how the rest serializes.
    let keep = max_bytes / 2;
    let first = request
        .operations
        .first()
        .and_then(|operation| operation.command.as_deref())
        .unwrap_or_default();
    let end = first
        .char_indices()
        .map(|(i, _)| i)
        .take_while(|i| *i <= keep)
        .last()
        .unwrap_or(0);
    let marker = format!(
        "{}… [truncated: the {size}-byte request carried {} operation(s) and exceeded the \
         {max_bytes}-byte audit log limit]",
        &first[..end],
        request.operations.len(),
    );

    let logged = Request {
        kip: request.kip.clone(),
        request_id: request.request_id.clone(),
        space: request.space.clone(),
        execution: request.execution.clone(),
        operations: vec![Operation::new(marker.clone())],
        options: request.options.clone(),
        ..Default::default()
    };
    // Correlation metadata is client-supplied too, so keeping it is not by
    // itself a bound. If the stand-in is still over budget, drop everything
    // that is not the marker rather than let the cap be advisory.
    let logged = match serde_json::to_vec(&logged) {
        Ok(bytes) if bytes.len() <= max_bytes => logged,
        _ => Request {
            operations: vec![Operation::new(marker)],
            ..Default::default()
        },
    };
    Cow::Owned(logged)
}

impl Nexus {
    /// Connects to the cognitive nexus and puts a Schema Environment in force
    /// in the default Space.
    ///
    /// A Space that has activated nothing resolves Core only, and Core declares
    /// no Concept types, so such a server would refuse every `CREATE CONCEPT`
    /// it was ever sent. The baseline cognitive-memory profile is therefore
    /// always installed and activated, with `packages` layered on top; the
    /// resulting lock is activated only when it differs from the one already in
    /// force, so a restart does not mint an environment version for no change.
    ///
    /// There is no `$self` genesis here. KIP 1.x seeded a `$self` Person node
    /// carrying the server's principal id; in 2.0 a Person is explicitly *not*
    /// a Principal (`Person != PrincipalRecord != ActorBinding`), Principals
    /// are Governance state, and this engine has no Governance plane. Writing
    /// an identity into cognitive content to stand in for one would encode
    /// exactly the confusion the profile forbids.
    ///
    /// `max_logged_request_bytes` bounds the request stored in each audit log
    /// document (see [`truncate_request`]).
    pub async fn connect(
        db: Arc<AndaDB>,
        packages: &[SchemaPackageSource],
        max_logged_request_bytes: usize,
    ) -> Result<Self, BoxError> {
        let nexus = CognitiveNexus::connect(db.clone()).await?;

        let bundled = SchemaPackageSource {
            source: BUNDLED_SOURCE.to_string(),
            artifact: COGNITIVE_MEMORY.to_string(),
        };
        let mut lock = SchemaLock::default();
        for entry in std::iter::once(&bundled).chain(packages) {
            let package = SchemaPackage::parse(&entry.artifact).map_err(|err| {
                BoxError::from(format!("schema package from {}: {err}", entry.source))
            })?;
            let package_ref = nexus.install_package(&package, &entry.source).await?;
            lock.packages.insert(
                package_ref.package_id.clone(),
                package_ref.version.to_string(),
            );
            lock.states
                .insert(package_ref.package_id.clone(), PackageState::Active);
        }
        let environment = nexus.ensure_schema(DEFAULT_SPACE, lock).await?;
        log::info!(
            space = DEFAULT_SPACE,
            version = environment.version,
            packages:serde = environment.lock.packages;
            "Schema Environment in force",
        );

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
            max_logged_request_bytes,
        })
    }

    /// Runs a whole KIP request envelope and appends an audit record.
    ///
    /// Execution goes through [`execute_request`], which runs `independent` and
    /// `sequence` batches and refuses `atomic` rather than emulating it: one
    /// transaction across several operations is an engine property, and this
    /// engine does not have it yet.
    ///
    /// `languages` is passed in rather than derived here because the caller
    /// needs the same classification before this future is spawned — see
    /// [`RequestLanguages`] — and classifying twice would mean parsing every
    /// command three times.
    pub async fn execute_kip(&self, request: Request, languages: &RequestLanguages) -> Response {
        let timestamp = unix_ms();

        let languages = languages.to_string();
        let response = execute_request(self.nexus.as_ref(), &request).await;
        let logged_request = truncate_request(&request, self.max_logged_request_bytes);
        // Errors live at the operation level for an ordinary failure and at the
        // request level only for an envelope failure or an unknown outcome, so
        // an audit record that read one of them would be blank for half of the
        // failures it exists to record.
        let errors: Vec<&anda_kip::ErrorObject> = response
            .error
            .iter()
            .chain(response.results.iter().filter_map(|r| r.error.as_ref()))
            .collect();
        let log = KIPLogRef {
            _id: 0, // This will be set by the database
            languages: &languages,
            request: &logged_request,
            response: json!({
                "status": response.status,
                "tx_id": response.receipt.as_ref().and_then(|r| r.tx_id.as_deref()),
                "errors": errors,
            }),
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
        response
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
    use anda_kip::{
        Execution, ExecutionMode, IngestContext, IngestEvidence, RequestOptions, TopLevelStatus,
    };
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
        Nexus::connect(Arc::new(db), &[], 8 * 1024).await.unwrap()
    }

    /// Runs one command the way the handler does: classify once, then
    /// execute with that classification.
    async fn run(nexus: &Nexus, command: &str) -> Response {
        let request = Request::single(command);
        let languages = RequestLanguages::of(&request);
        nexus.execute_kip(request, &languages).await
    }

    async fn add_log(nexus: &Nexus, period: u64) {
        let log = KIPLog {
            _id: 0,
            languages: CommandType::Meta.to_string(),
            request: Request::default(),
            response: serde_json::json!({"status": "succeeded"}),
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

    /// The bundled profile is in force on a fresh database, and a restart
    /// against the same database must not mint a second environment version
    /// for an unchanged lock.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_schema_environment_is_activated_once_not_once_per_start() {
        let store = Arc::new(InMemory::new());
        let config = || DBConfig {
            name: "schema_bootstrap".to_string(),
            description: "test".to_string(),
            storage: StorageConfig::default(),
            lock: None,
        };
        let db = Arc::new(AndaDB::connect(store.clone(), config()).await.unwrap());
        let nexus = Nexus::connect(db.clone(), &[], 8 * 1024).await.unwrap();

        // A Concept type only the profile declares resolves, which is the
        // whole point of activating it.
        let response = run(
            &nexus,
            r#"CREATE CONCEPT ?p { TYPE "Person" NAME "Alice" }"#,
        )
        .await;
        assert_eq!(
            response.status,
            TopLevelStatus::Succeeded,
            "{:#?}",
            response.results
        );
        let version = response
            .receipt
            .as_ref()
            .and_then(|receipt| receipt.schema_environment_version);
        assert_eq!(version, Some(1));
        db.close().await.unwrap();

        let db = Arc::new(AndaDB::connect(store, config()).await.unwrap());
        let nexus = Nexus::connect(db, &[], 8 * 1024).await.unwrap();
        let response = run(&nexus, r#"CREATE CONCEPT ?p { TYPE "Person" NAME "Bob" }"#).await;
        assert_eq!(
            response
                .receipt
                .as_ref()
                .and_then(|receipt| receipt.schema_environment_version),
            Some(1),
            "a restart must not re-activate an unchanged Schema Lock",
        );
    }

    /// A request that fits the cap is stored verbatim.
    #[test]
    fn small_requests_are_logged_unchanged() {
        let request = Request {
            options: Some(RequestOptions {
                dry_run: Some(true),
                ..Default::default()
            }),
            ..Request::single("DESCRIBE PRIMER")
        };
        let logged = truncate_request(&request, 8 * 1024);
        assert!(matches!(logged, Cow::Borrowed(_)));
        assert_eq!(
            logged.operations[0].command.as_deref(),
            Some("DESCRIBE PRIMER")
        );
    }

    /// A near-body-limit request must not add megabytes of permanent storage
    /// per call. The stand-in stays bounded, keeps a forensic prefix, and
    /// still deserializes as a `Request` so `list_logs` keeps working.
    #[test]
    fn oversized_requests_are_logged_truncated_but_still_parseable() {
        let request = Request {
            request_id: Some("req-1".to_string()),
            // An ingest payload is designed to carry an observation verbatim,
            // so it is the envelope's largest member in practice.
            ingest: Some(IngestContext {
                evidence: vec![IngestEvidence {
                    key: "msg".to_string(),
                    evidence_class: "user_statement".to_string(),
                    payload: Some(Json::String("y".repeat(1024 * 1024))),
                    ..Default::default()
                }],
                extensions: None,
            }),
            operations: vec![
                Operation::new(format!("UPSERT {{ {} }}", "x".repeat(2 * 1024 * 1024)))
                    .with_parameters(
                        serde_json::from_value(json!({"blob": "z".repeat(4096)})).unwrap(),
                    ),
                Operation::new("DESCRIBE PRIMER"),
            ],
            execution: Some(Execution::new(ExecutionMode::Sequence)),
            ..Default::default()
        };

        let logged = truncate_request(&request, 1024);
        assert!(matches!(logged, Cow::Owned(_)));
        let encoded = serde_json::to_vec(&*logged).unwrap();
        assert!(encoded.len() <= 1024, "stored {} bytes", encoded.len());
        let command = logged.operations[0].command.as_deref().unwrap();
        assert!(command.starts_with("UPSERT { xxx"));
        assert!(command.contains("truncated"));
        assert!(command.contains("2 operation(s)"));
        assert_eq!(logged.operations.len(), 1);
        assert!(logged.ingest.is_none());
        assert!(logged.operations[0].parameters.is_none());
        assert_eq!(logged.request_id.as_deref(), Some("req-1"));

        // The persisted map must round-trip back into a `Request`, which is
        // `deny_unknown_fields`.
        let value = serde_json::to_value(&*logged).unwrap();
        let parsed: Request = serde_json::from_value(value).unwrap();
        assert_eq!(&parsed, &*logged);
    }

    /// Correlation metadata is client-supplied too: a caller cannot buy extra
    /// storage per request by moving the bulk into `request_id`.
    #[test]
    fn the_cap_bounds_client_supplied_metadata_as_well() {
        let request = Request {
            request_id: Some("r".repeat(64 * 1024)),
            ..Request::single("DESCRIBE PRIMER")
        };
        let logged = truncate_request(&request, MIN_LOGGED_REQUEST_BYTES);
        let encoded = serde_json::to_vec(&*logged).unwrap();
        assert!(
            encoded.len() <= MIN_LOGGED_REQUEST_BYTES,
            "stored {} bytes",
            encoded.len()
        );
        assert!(logged.request_id.is_none());
    }

    /// The audit record classifies by what the commands *are*. A declared
    /// `language` is advisory (§73.1), and a batch may mix families.
    #[test]
    fn languages_are_classified_from_the_parsed_commands() {
        assert_eq!(
            RequestLanguages::of(&Request::single("DESCRIBE PRIMER")).to_string(),
            "META"
        );
        assert_eq!(
            RequestLanguages::of(&Request::single("this is not a KIP command")).to_string(),
            "UNKNOWN"
        );

        let mixed = Request {
            operations: vec![
                Operation::new(r#"TOMBSTONE :x"#),
                Operation::new(r#"FIND(?x) WHERE { ?x {type: "Person"} }"#),
                Operation::new(r#"TOMBSTONE :y"#),
            ],
            execution: Some(Execution::new(ExecutionMode::Sequence)),
            ..Default::default()
        };
        let mixed = RequestLanguages::of(&mixed);
        assert_eq!(mixed.to_string(), "KML,KQL");
        assert!(mixed.has_mutation());
        assert!(!RequestLanguages::of(&Request::single("DESCRIBE PRIMER")).has_mutation());
    }

    /// The cap is enforced end to end: an oversized request is readable
    /// through `list_logs` in its truncated form, and the record says which
    /// language ran.
    #[tokio::test(flavor = "multi_thread")]
    async fn execute_kip_logs_a_bounded_request() {
        let nexus = test_nexus().await;
        let command = format!(
            "FIND(?x) WHERE {{ ?x {{name: \"{}\"}} }}",
            "z".repeat(100_000)
        );
        let _ = run(&nexus, &command).await;

        let (logs, _) = nexus
            .list_logs(ListLogParams {
                cursor: None,
                limit: Some(10),
            })
            .await
            .unwrap();
        let log = logs.last().expect("the request must be logged");
        let logged = log.request.operations[0].command.as_deref().unwrap();
        assert!(logged.len() < command.len());
        assert!(logged.contains("truncated"));
        assert_eq!(log.languages, "KQL");
    }
}
