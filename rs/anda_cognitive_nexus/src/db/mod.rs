//! # Cognitive Nexus Module
//!
//! This module provides the core database implementation for the cognitive nexus system.
//! It implements the Knowledge Interchange Protocol (KIP) executor interface and manages
//! concepts and propositions in a knowledge graph database.
//!
use anda_db::{
    collection::{Collection, CollectionConfig},
    database::AndaDB,
    error::DBError,
    index::{BTree, extract_json_text, virtual_field_name, virtual_field_value},
    query::{Filter, RangeQuery},
    unix_ms,
};
use anda_db_schema::Fv;
use anda_db_tfs::jieba_tokenizer;
use anda_db_utils::UniqueVec;
use anda_kip::*;
use async_trait::async_trait;
use futures::try_join;
use rustc_hash::{FxHashMap, FxHashSet};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::Arc,
};
use tokio::sync::RwLock;

use crate::{entity::*, helper::*, types::*};

mod kml;
mod kql;
mod matching;
mod meta;

#[cfg(test)]
mod tests;

/// Core database structure for the cognitive nexus system.
///
/// `CognitiveNexus` manages a knowledge graph composed of concepts and
/// propositions, providing high-level operations for querying and
/// manipulating the knowledge base. It implements the
/// [`anda_kip::Executor`] trait so any frontend that produces a KIP
/// [`Command`] (KQL query, KML mutation, or META introspection) can run
/// against it without further glue code.
///
/// # Architecture
///
/// - **Storage** — built on top of [`AndaDB`]. Two collections are used:
///   `concepts` (one row per concept node) and `propositions` (one row
///   per `(subject, object)` pair, holding all predicates that connect
///   them).
/// - **Indexes** — see [`CognitiveNexus::connect`] for the exact set of
///   B-Tree and BM25 indexes that are created on first run.
/// - **Caching** — every query/META call instantiates a fresh
///   [`QueryCache`] inside its [`QueryContext`] to avoid loading the same
///   row twice during a single execution. The cache is *not* shared
///   across calls; KML write paths invalidate cached rows on update.
/// - **KIP support** — full KIP v1.0 Release Candidate (KQL / KML including
///   `UPDATE`, `MERGE` and `EXPECT VERSION` / META including `EXPORT`).
///
/// # Concurrency
///
/// The struct uses a [`tokio::sync::RwLock`] (`kml_lock`) to guarantee KML
/// execution consistency:
///
/// - **Read lock** — acquired for KQL queries and META commands; allows
///   any number of concurrent readers.
/// - **Write lock** — acquired for KML mutations; ensures exclusive
///   access during data modifications.
///
/// This prevents race conditions during complex KML transactions that may
/// involve multiple concept and proposition updates across collections.
#[derive(Clone, Debug)]
pub struct CognitiveNexus {
    /// Underlying Anda DB instance shared with any other collections the
    /// host application may register.
    pub db: Arc<AndaDB>,
    /// `concepts` collection — one row per [`Concept`].
    pub concepts: Arc<Collection>,
    /// `propositions` collection — one row per [`Proposition`].
    pub propositions: Arc<Collection>,
    /// Read-write lock for KML execution consistency. KQL/META acquire
    /// the read lock; KML acquires the write lock.
    kml_lock: Arc<RwLock<()>>,
}

/// Implementation of the Knowledge Interchange Protocol (KIP) executor.
///
/// This trait implementation allows the cognitive nexus to process KIP commands,
/// including queries (KQL), markup language statements (KML), and meta commands.
#[async_trait]
impl Executor for CognitiveNexus {
    /// Executes a KIP command and returns the appropriate response.
    ///
    /// # Arguments
    ///
    /// * `command` - The KIP command to execute (KQL, KML, or Meta)
    /// * `dry_run` - Whether to perform a dry run (only applicable to KML commands)
    ///
    /// # Returns
    ///
    /// A `Response` containing the execution result, which may include:
    /// - Query results for KQL commands
    /// - Modification results for KML commands
    /// - Metadata for Meta commands
    ///
    /// # Concurrency
    ///
    /// - KQL and Meta commands acquire a read lock (allows concurrent execution)
    /// - KML commands acquire a write lock (ensures exclusive access during mutations)
    ///
    async fn execute(&self, command: Command, dry_run: bool) -> Response {
        match command {
            Command::Kql(command) => self.execute_kql(command).await.into(),
            Command::Kml(command) => match self.execute_kml(command, dry_run).await {
                Ok(result) => Response::Ok {
                    result,
                    next_cursor: None,
                },
                Err(error) => Response::err(error),
            },
            Command::Meta(command) => self.execute_meta(command).await.into(),
        }
    }
}

/// The system capsules bundled with this crate, applied by
/// [`CognitiveNexus::connect`] in dependency order (Genesis first). Each
/// entry is `(name, source, anchor)`: `name` keys the persisted content hash
/// (`capsule_hash:<name>`), and `anchor` is the `$ConceptType` definition the
/// capsule owns — used as a self-healing existence check besides the hash.
///
/// `persons/self.kip` / `persons/system.kip` are deliberately **not**
/// bundled: `$self` attributes evolve with the agent and must never be reset
/// to the template by a re-applied capsule. Applications apply those
/// capsules themselves.
const BUNDLED_CAPSULES: &[(&str, &str, &str)] = &[
    ("genesis", GENESIS_KIP, META_CONCEPT_TYPE),
    ("person", PERSON_KIP, PERSON_TYPE),
    ("preference", PREFERENCE_KIP, PREFERENCE_TYPE),
    ("event", EVENT_KIP, EVENT_TYPE),
    ("sleep_task", SLEEP_TASK_KIP, SLEEP_TASK_TYPE),
    ("insight", INSIGHT_KIP, INSIGHT_TYPE),
    ("commitment", COMMITMENT_KIP, COMMITMENT_TYPE),
];

/// Content hash of a bundled capsule source (hex-encoded SHA3-256). A
/// changed `.kip` file yields a new hash, which is what triggers re-applying
/// the capsule on existing database instances — no manual version bump.
fn capsule_hash(source: &str) -> String {
    use sha3::Digest;
    hex::encode(sha3::Sha3_256::digest(source.as_bytes()))
}

impl CognitiveNexus {
    /// Establishes a connection to the cognitive nexus database.
    ///
    /// This method initializes the database collections, creates necessary indexes,
    /// and sets up the initial schema. It also ensures that essential meta-concepts
    /// are present in the database.
    ///
    /// # Arguments
    ///
    /// * `db` - Reference to the underlying AndaDB database
    /// * `f` - Initialization function called after setup but before returning
    ///
    /// # Returns
    ///
    /// * `Ok(CognitiveNexus)` - Successfully initialized cognitive nexus
    /// * `Err(KipError)` - If initialization fails
    ///
    /// # Database Setup
    ///
    /// The method performs the following initialization steps:
    /// 1. Creates or opens the "concepts" collection with appropriate schema and indexes
    /// 2. Creates or opens the "propositions" collection with appropriate schema and indexes
    /// 3. Sets up text tokenization for full-text search capabilities
    /// 4. Synchronizes the bundled system capsules: each capsule in
    ///    `BUNDLED_CAPSULES` is (re-)applied when its content hash differs
    ///    from the one recorded in the database or when its anchor
    ///    `$ConceptType` definition is missing. Capsules are idempotent
    ///    `UPSERT` scripts, so crate upgrades that revise a capsule propagate
    ///    to existing instances automatically without touching user data.
    /// 5. Calls the provided initialization function
    ///
    /// # Indexes Created
    ///
    /// **Concepts Collection:**
    /// - BTree indexes: ["type", "name"], ["type"], ["name"]
    /// - BM25 index: ["name", "attributes", "metadata"]
    ///
    /// **Propositions Collection:**
    /// - BTree indexes: ["subject", "object"], ["subject"], ["object"], ["predicates"]
    /// - BM25 index: ["predicates", "properties"]
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let db = Arc::new(AndaDB::new("knowledge_base").await?);
    /// let nexus = CognitiveNexus::connect(db, |nexus| async {
    ///     // Custom initialization logic here
    ///     println!("Connected to database: {}", nexus.name());
    ///     Ok(())
    /// }).await?;
    /// ```
    pub async fn connect<F>(db: Arc<AndaDB>, f: F) -> Result<Self, KipError>
    where
        F: AsyncFnOnce(&CognitiveNexus) -> Result<(), KipError>,
    {
        let schema = Concept::schema().map_err(KipError::invalid_syntax)?;
        let concepts = db
            .open_or_create_collection(
                schema,
                CollectionConfig {
                    name: "concepts".to_string(),
                    description: "Concept nodes".to_string(),
                },
                async |collection| {
                    // set tokenizer
                    collection.set_tokenizer(jieba_tokenizer());
                    // create BTree indexes if not exists
                    collection.create_btree_index_nx(&["type", "name"]).await?;
                    collection.create_btree_index_nx(&["type"]).await?;
                    collection.create_btree_index_nx(&["name"]).await?;
                    collection
                        .create_bm25_index_nx(&["name", "attributes", "metadata"])
                        .await?;

                    Ok::<(), DBError>(())
                },
            )
            .await
            .map_err(db_to_kip_error)?;

        let schema = Proposition::schema().map_err(KipError::invalid_syntax)?;
        let propositions = db
            .open_or_create_collection(
                schema,
                CollectionConfig {
                    name: "propositions".to_string(),
                    description: "Proposition links".to_string(),
                },
                async |collection| {
                    // set tokenizer
                    collection.set_tokenizer(jieba_tokenizer());
                    // create BTree indexes if not exists
                    collection
                        .create_btree_index_nx(&["subject", "object"])
                        .await?;
                    collection.create_btree_index_nx(&["subject"]).await?;
                    collection.create_btree_index_nx(&["object"]).await?;
                    collection.create_btree_index_nx(&["predicates"]).await?;
                    collection
                        .create_bm25_index_nx(&["predicates", "properties"])
                        .await?;

                    Ok::<(), DBError>(())
                },
            )
            .await
            .map_err(db_to_kip_error)?;
        let this = Self {
            db,
            concepts,
            propositions,
            kml_lock: Arc::new(RwLock::new(())),
        };

        let ver = this.capsule_version();
        this.sync_bundled_capsules().await?;

        f(&this).await?;

        if ver <= 1 {
            this.save_capsule_version(2).await?;
        }
        Ok(this)
    }

    /// Applies every bundled capsule whose recorded content hash
    /// (`capsule_hash:<name>` extension on the `concepts` collection)
    /// differs from the source shipped with this crate, or whose anchor
    /// `$ConceptType` definition node is missing (self-healing).
    ///
    /// Bundled capsules are idempotent `UPSERT` scripts, so re-applying one
    /// after a crate upgrade shallow-merges the revised definitions into an
    /// existing instance without touching user data (schema nodes get a
    /// regular `_version` bump). A failed apply leaves the stored hash
    /// untouched, so the next [`connect`](Self::connect) retries it.
    async fn sync_bundled_capsules(&self) -> Result<(), KipError> {
        for (name, source, anchor) in BUNDLED_CAPSULES {
            let key = format!("capsule_hash:{name}");
            let current = capsule_hash(source);
            let stored: Option<String> = self.concepts.get_extension_as(&key);
            let hash_current = stored.as_deref() == Some(current.as_str());
            let anchor_missing = !self
                .has_concept(&ConceptPK::Object {
                    r#type: META_CONCEPT_TYPE.to_string(),
                    name: anchor.to_string(),
                })
                .await;
            if hash_current && !anchor_missing {
                continue;
            }

            self.execute_kml(parse_kml(source)?, false)
                .await
                .map_err(|err| {
                    KipError::new(
                        err.code,
                        format!("Bundled capsule {name:?} bootstrap failed: {}", err.message),
                    )
                })?;

            if !hash_current {
                self.concepts
                    .save_extension(key, Fv::Text(current))
                    .await
                    .map_err(db_to_kip_error)?;
            }
        }

        Ok(())
    }

    /// Closes the database connection and releases resources.
    ///
    /// This method should be called when the cognitive nexus is no longer needed
    /// to ensure proper cleanup of database resources.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Database closed successfully
    /// * `Err(KipError)` - If closing the database fails
    ///
    pub async fn close(&self) -> Result<(), KipError> {
        self.db.close().await.map_err(db_to_kip_error)
    }

    /// Returns the name of the underlying database.
    pub fn name(&self) -> &str {
        self.db.name()
    }

    /// Returns the persisted **capsule schema version** stored alongside
    /// the `concepts` collection.
    ///
    /// Routine capsule refreshes are driven by content hashes (see
    /// `BUNDLED_CAPSULES`), so this monotonically-increasing integer now
    /// serves as the cursor for **breaking migrations** — schema changes
    /// that idempotent `UPSERT` capsules cannot express (renames, removals,
    /// restructures). A return value of `0` means no version has been
    /// recorded yet (a fresh database).
    pub fn capsule_version(&self) -> u64 {
        self.concepts
            .get_extension("capsule_version")
            .and_then(|v| u64::try_from(v).ok())
            .unwrap_or(0)
    }

    /// Persists the capsule schema version. Called automatically by
    /// [`CognitiveNexus::connect`] after the bundled Genesis capsules
    /// have been applied; downstream applications can call it to record
    /// their own migration steps.
    pub async fn save_capsule_version(&self, version: u64) -> Result<(), KipError> {
        self.concepts
            .save_extension("capsule_version".to_string(), version.into())
            .await
            .map_err(db_to_kip_error)
    }

    /// Checks whether a concept exists in the database.
    ///
    /// This method performs a fast existence check without loading the full concept data.
    /// It supports both ID-based and object-based concept identification.
    ///
    /// # Arguments
    ///
    /// * `pk` - The primary key of the concept to check
    ///
    /// # Returns
    ///
    /// * `true` - If the concept exists
    /// * `false` - If the concept does not exist or cannot be found
    ///
    /// # Performance
    ///
    /// - For ID-based lookups: O(1) existence check
    /// - For object-based lookups: O(log n) index lookup followed by O(1) existence check
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Check by ID
    /// let exists = nexus.has_concept(&ConceptPK::ID(12345)).await;
    ///
    /// // Check by type and name
    /// let exists = nexus.has_concept(&ConceptPK::Object {
    ///     r#type: "Person".to_string(),
    ///     name: "Alice".to_string(),
    /// }).await;
    /// ```
    pub async fn has_concept(&self, pk: &ConceptPK) -> bool {
        let id = match pk {
            ConceptPK::ID(id) => *id,
            ConceptPK::Object { r#type, name } => match self.query_concept_id(r#type, name).await {
                Ok(id) => id,
                Err(_) => return false,
            },
        };

        self.concepts.contains(id)
    }

    /// Retrieves a concept from the database.
    ///
    /// This method loads the complete concept data including all attributes and metadata.
    /// It supports both ID-based and object-based concept identification.
    ///
    /// # Arguments
    ///
    /// * `pk` - The primary key of the concept to retrieve
    ///
    /// # Returns
    ///
    /// * `Ok(Concept)` - The loaded concept with all its data
    /// * `Err(KipError)` - If the concept is not found or loading fails
    ///
    pub async fn get_concept(&self, pk: &ConceptPK) -> Result<Concept, KipError> {
        let id = match pk {
            ConceptPK::ID(id) => *id,
            ConceptPK::Object { r#type, name } => self.query_concept_id(r#type, name).await?,
        };

        self.concepts.get_as(id).await.map_err(db_to_kip_error)
    }

    /// Retrieves an existing concept or initialises a new one if it does
    /// not yet exist.
    ///
    /// This is a convenience helper used by callers that want
    /// idempotent insertion semantics outside of the regular KML
    /// `UPSERT` path. The caller is responsible for guaranteeing that:
    ///
    /// - `r#type` already exists as a `$ConceptType` instance, and
    /// - `name` is non-empty.
    ///
    /// No type-existence check is performed here — for the protocol
    /// path use [`execute_kml`](Self::execute_kml) with an `UPSERT`
    /// statement.
    pub async fn get_or_init_concept(
        &self,
        r#type: String,
        name: String,
        attributes: Map<String, Json>,
        metadata: Map<String, Json>,
    ) -> Result<Concept, KipError> {
        match self.query_concept_id(&r#type, &name).await {
            Ok(id) => self.concepts.get_as(id).await.map_err(db_to_kip_error),
            Err(_) => {
                let mut concept = Concept {
                    _id: 0, // Will be set by the database
                    r#type,
                    name,
                    attributes,
                    metadata,
                };
                let id = self
                    .concepts
                    .add_from(&concept)
                    .await
                    .map_err(db_to_kip_error)?;

                concept._id = id;
                Ok(concept)
            }
        }
    }

    /// Executes a KQL `FIND` query and returns its result tuple.
    ///
    /// The result is `(value, next_cursor)`:
    ///
    /// - When the `FIND` clause has a single expression, `value` is its
    ///   raw payload (object / array / scalar). When it has more than one
    ///   expression, `value` is a JSON array of column arrays — one per
    ///   `FIND` expression — preserving column alignment across rows.
    /// - `next_cursor` is `Some` when `LIMIT` truncated the result and
    ///   the caller should resume by passing the cursor back via
    ///   `CURSOR "…"`.
    ///
    /// This method acquires the KML read lock so multiple queries may run
    /// concurrently against a stable snapshot.
    pub async fn execute_kql(&self, command: KqlQuery) -> Result<(Json, Option<String>), KipError> {
        let _guard = self.kml_lock.read().await;
        self.execute_kql_inner(command).await
    }

    async fn execute_kql_inner(
        &self,
        command: KqlQuery,
    ) -> Result<(Json, Option<String>), KipError> {
        let mut ctx = QueryContext::default();

        // 执行WHERE子句
        for clause in command.where_clauses {
            self.execute_where_clause(&mut ctx, clause).await?;
        }

        // 执行FIND子句
        let mut result = self
            .execute_find_clause(
                &mut ctx,
                command.find_clause,
                command.order_by,
                command.cursor,
                command.limit,
            )
            .await?;

        if result.0.len() == 1 {
            Ok((result.0.pop().unwrap(), result.1))
        } else {
            Ok((Json::Array(result.0), result.1))
        }
    }

    /// Executes a KML statement (`UPSERT`, `UPDATE`, `MERGE`, or `DELETE …`).
    ///
    /// When `dry_run` is `true`:
    ///
    /// - `UPSERT` validates that all referenced concept / proposition
    ///   types exist, that all variable handles can be resolved, and that
    ///   every `EXPECT VERSION` guard matches (`KIP_3005` otherwise), but
    ///   does **not** create or update any row.
    /// - `UPDATE` / `MERGE` run their full validation (including the
    ///   `KIP_3004` protected-scope checks) and pattern matching without
    ///   writing.
    /// - `DELETE CONCEPT` and protected `DELETE ATTRIBUTES` targets still
    ///   perform the `KIP_3004` protected-scope pre-flight check so agents
    ///   can probe for safety without side effects.
    /// - Other delete variants short-circuit and return zeroed counters.
    ///
    /// On success the returned JSON is shaped per KIP §4 — for upserts an
    /// [`UpsertResult`], for updates `{"updated": N, "matched": N}`, for
    /// merges `{"merged": true, "links_repointed": N, …}`, for deletes a
    /// `{"deleted_*": N, "updated_*": N}` map. KML acquires the write lock
    /// so it executes exclusively.
    pub async fn execute_kml(
        &self,
        command: KmlStatement,
        dry_run: bool,
    ) -> Result<Json, KipError> {
        let _guard = self.kml_lock.write().await;
        self.execute_kml_inner(command, dry_run).await
    }

    async fn execute_kml_inner(
        &self,
        command: KmlStatement,
        dry_run: bool,
    ) -> Result<Json, KipError> {
        match command {
            KmlStatement::Upsert(upsert_blocks) => {
                self.execute_upsert(upsert_blocks, dry_run).await
            }
            KmlStatement::Update(update_statement) => {
                self.execute_update(update_statement, dry_run).await
            }
            KmlStatement::Merge(merge_statement) => {
                self.execute_merge(merge_statement, dry_run).await
            }
            KmlStatement::Delete(delete_statement) => {
                self.execute_delete(delete_statement, dry_run).await
            }
        }
    }

    /// Executes a META command (`DESCRIBE …`, `SEARCH …`, or `EXPORT …`).
    ///
    /// META commands are read-only; they acquire the KML read lock and
    /// return `(value, next_cursor)` with the same conventions as
    /// [`execute_kql`](Self::execute_kql). `DESCRIBE PRIMER`, `DESCRIBE
    /// DOMAINS`, `SEARCH` and `EXPORT` return non-paginated payloads
    /// (`next_cursor == None`).
    pub async fn execute_meta(
        &self,
        command: MetaCommand,
    ) -> Result<(Json, Option<String>), KipError> {
        let _guard = self.kml_lock.read().await;
        self.execute_meta_inner(command).await
    }

    async fn execute_meta_inner(
        &self,
        command: MetaCommand,
    ) -> Result<(Json, Option<String>), KipError> {
        match command {
            MetaCommand::Describe(DescribeTarget::Primer) => {
                self.execute_describe_primer().await.map(|rt| (rt, None))
            }
            MetaCommand::Describe(DescribeTarget::Domains) => {
                self.execute_describe_domains().await.map(|rt| (rt, None))
            }
            MetaCommand::Describe(DescribeTarget::ConceptTypes { limit, cursor }) => {
                self.execute_describe_concept_types(limit, cursor).await
            }
            MetaCommand::Describe(DescribeTarget::ConceptType(name)) => self
                .execute_describe_concept_type(name)
                .await
                .map(|rt| (rt, None)),
            MetaCommand::Describe(DescribeTarget::PropositionTypes { limit, cursor }) => {
                self.execute_describe_proposition_types(limit, cursor).await
            }
            MetaCommand::Describe(DescribeTarget::PropositionType(name)) => self
                .execute_describe_proposition_type(name)
                .await
                .map(|rt| (rt, None)),
            MetaCommand::Search(command) => self.execute_search(command).await.map(|rt| (rt, None)),
            MetaCommand::Export(command) => self.execute_export(command).await.map(|rt| (rt, None)),
        }
    }

    async fn query_concept_id(&self, ty: &str, name: &str) -> Result<u64, KipError> {
        let virtual_name = virtual_field_name(&["type", "name"]);
        let virtual_val = virtual_field_value(&[
            Some(&Fv::Text(ty.to_string())),
            Some(&Fv::Text(name.to_string())),
        ])
        .unwrap();

        let mut ids = self
            .concepts
            .query_ids(
                Filter::Field((virtual_name, RangeQuery::Eq(virtual_val))),
                None,
            )
            .await
            .map_err(db_to_kip_error)?;
        ids.pop().ok_or(KipError::not_found(format!(
            "Concept {} not found",
            ConceptPK::Object {
                r#type: ty.to_string(),
                name: name.to_string()
            }
        )))
    }

    async fn query_concept_ids(&self, matcher: &ConceptMatcher) -> Result<Vec<u64>, KipError> {
        match matcher {
            ConceptMatcher::ID(id) => {
                let entity_id = EntityID::from_str(id).map_err(KipError::invalid_syntax)?;
                if let EntityID::Concept(concept_id) = entity_id {
                    Ok(vec![concept_id])
                } else {
                    Err(KipError::invalid_syntax(format!(
                        "Invalid concept node ID: {}",
                        id
                    )))
                }
            }
            ConceptMatcher::Type(type_name) => {
                let ids = self
                    .concepts
                    .query_ids(
                        Filter::Field((
                            "type".to_string(),
                            RangeQuery::Eq(Fv::Text(type_name.clone())),
                        )),
                        None,
                    )
                    .await
                    .map_err(db_to_kip_error)?;
                Ok(ids)
            }
            ConceptMatcher::Name(name) => {
                let ids = self
                    .concepts
                    .query_ids(
                        Filter::Field(("name".to_string(), RangeQuery::Eq(Fv::Text(name.clone())))),
                        None,
                    )
                    .await
                    .map_err(db_to_kip_error)?;
                Ok(ids)
            }
            ConceptMatcher::Object { r#type, name } => {
                let id = self.query_concept_id(r#type, name).await?;
                Ok(vec![id])
            }
        }
    }

    async fn try_get_concept_with<F, R>(
        &self,
        cache: &QueryCache,
        id: u64,
        f: F,
    ) -> Result<R, KipError>
    where
        F: FnOnce(&Concept) -> Result<R, KipError>,
    {
        if let Some(concept) = cache.concepts.read().get(&id) {
            return f(concept);
        }
        let concept: Concept = self.concepts.get_as(id).await.map_err(db_to_kip_error)?;
        let rt = f(&concept)?;
        cache.concepts.write().insert(id, concept);
        Ok(rt)
    }

    async fn try_get_proposition_with<F, R>(
        &self,
        cache: &QueryCache,
        id: u64,
        f: F,
    ) -> Result<R, KipError>
    where
        F: FnOnce(&Proposition) -> Result<R, KipError>,
    {
        if let Some(proposition) = cache.propositions.read().get(&id) {
            return f(proposition);
        }
        let proposition: Proposition = self
            .propositions
            .get_as(id)
            .await
            .map_err(db_to_kip_error)?;
        let rt = f(&proposition)?;
        cache.propositions.write().insert(id, proposition);
        Ok(rt)
    }
}
