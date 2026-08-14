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
///
/// # Poison recovery
///
/// A cancelled mutating future — or a failed flush, which poisons on *any*
/// error — puts an `anda_db` collection handle into the poisoned state, where
/// it rejects every further mutation. Recovery lives in
/// [`AndaDB::open_collection`] (drain the handle, drop it, reload with
/// mutation-intent replay and the repair scan), so a handle captured once at
/// [`connect`](CognitiveNexus::connect) would never reach it. The two handles
/// therefore live in swappable slots and are re-resolved by
/// [`reopen_collections`](CognitiveNexus::reopen_collections) instead of
/// bricking the nexus until the process restarts: every mutating entry point
/// checks [`Collection::is_poisoned`] before dispatching and reopens first, so
/// a poison event costs no failed command, with a post-failure backstop for a
/// handle poisoned mid-statement.
#[derive(Clone, Debug)]
pub struct CognitiveNexus {
    /// Underlying Anda DB instance shared with any other collections the
    /// host application may register.
    pub db: Arc<AndaDB>,
    /// `concepts` collection — one row per [`Concept`]. Read through
    /// [`concepts`](CognitiveNexus::concepts); the slot is swapped on poison
    /// recovery, and clones of this struct share it.
    concepts: Arc<parking_lot::RwLock<Arc<Collection>>>,
    /// `propositions` collection — one row per [`Proposition`]. Read through
    /// [`propositions`](CognitiveNexus::propositions).
    propositions: Arc<parking_lot::RwLock<Arc<Collection>>>,
    /// Read-write lock for KML execution consistency. KQL/META acquire
    /// the read lock; KML acquires the write lock.
    kml_lock: Arc<RwLock<()>>,
}

/// Tokenizer and index setup for the `concepts` collection.
///
/// Named (rather than inlined at the `connect` call site) because every
/// *re*-open must run exactly the same setup: `create_*_nx` are no-ops once
/// the index exists, but a freshly loaded handle starts with the default
/// tokenizer and must have the jieba chain re-installed.
async fn init_concepts_collection(collection: &mut Collection) -> Result<(), DBError> {
    // set tokenizer
    collection.set_tokenizer(jieba_tokenizer());
    // create BTree indexes if not exists
    collection.create_btree_index_nx(&["type", "name"]).await?;
    collection.create_btree_index_nx(&["type"]).await?;
    collection.create_btree_index_nx(&["name"]).await?;
    collection
        .create_bm25_index_nx(&["name", "attributes", "metadata"])
        .await?;
    Ok(())
}

/// Tokenizer and index setup for the `propositions` collection. See
/// [`init_concepts_collection`].
async fn init_propositions_collection(collection: &mut Collection) -> Result<(), DBError> {
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
    Ok(())
}

/// Maps a failed collection reopen, telling "retry later" apart from "give
/// up".
///
/// [`DBError::collection_state`] carries the rejecting handle's lifecycle
/// state as a typed source, so a collection that is being deleted (or is
/// already gone) is recognized structurally: it reports a state whose
/// [`CollectionState::is_recoverable`](anda_db::error::CollectionState::is_recoverable)
/// is `false`, and no amount of reopening brings its objects back. The error
/// code is left untouched — only the hint is added.
fn reopen_error(err: DBError) -> KipError {
    let unrecoverable = err
        .collection_state()
        .is_some_and(|state| !state.is_recoverable());
    let err = db_to_kip_error(err);
    if unrecoverable {
        return KipError::new(
            err.code,
            format!(
                "{}; the collection is being deleted or is gone — reopening cannot recover it",
                err.message
            ),
        );
    }
    err
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
/// entry is `(name, source, anchor_type, anchor_name)`: `name` keys the
/// persisted content hash (`capsule_hash:<name>`), and the anchor pair names
/// the meta-definition the capsule owns (a `$ConceptType` for type capsules,
/// a `$PropositionType` for predicate capsules) — used as a self-healing
/// existence check besides the hash.
///
/// The predicate capsules trail the concept types they reference: KIP
/// v1.0-RC11 pulled `involves` / `mentions` / `consolidated_to` /
/// `derived_from` out of `Event.kip` into standalone capsules (widened to
/// span `Event` and `Experience`) and added the Experience-specific
/// `has_step` / `caused_by` / `derived_insight` / `compiled_to`.
///
/// `persons/self.kip` / `persons/system.kip` are deliberately **not**
/// bundled: `$self` attributes evolve with the agent and must never be reset
/// to the template by a re-applied capsule. Applications apply those
/// capsules themselves.
const BUNDLED_CAPSULES: &[(&str, &str, &str, &str)] = &[
    ("genesis", GENESIS_KIP, META_CONCEPT_TYPE, META_CONCEPT_TYPE),
    ("person", PERSON_KIP, META_CONCEPT_TYPE, PERSON_TYPE),
    (
        "preference",
        PREFERENCE_KIP,
        META_CONCEPT_TYPE,
        PREFERENCE_TYPE,
    ),
    ("event", EVENT_KIP, META_CONCEPT_TYPE, EVENT_TYPE),
    (
        "sleep_task",
        SLEEP_TASK_KIP,
        META_CONCEPT_TYPE,
        SLEEP_TASK_TYPE,
    ),
    ("insight", INSIGHT_KIP, META_CONCEPT_TYPE, INSIGHT_TYPE),
    (
        "commitment",
        COMMITMENT_KIP,
        META_CONCEPT_TYPE,
        COMMITMENT_TYPE,
    ),
    (
        "experience",
        EXPERIENCE_KIP,
        META_CONCEPT_TYPE,
        EXPERIENCE_TYPE,
    ),
    (
        "experience_step",
        EXPERIENCE_STEP_KIP,
        META_CONCEPT_TYPE,
        EXPERIENCE_STEP_TYPE,
    ),
    ("skill", SKILL_KIP, META_CONCEPT_TYPE, SKILL_TYPE),
    (
        "involves",
        INVOLVES_PROP_KIP,
        META_PROPOSITION_TYPE,
        INVOLVES_TYPE,
    ),
    (
        "mentions",
        MENTIONS_PROP_KIP,
        META_PROPOSITION_TYPE,
        MENTIONS_TYPE,
    ),
    (
        "consolidated_to",
        CONSOLIDATED_TO_PROP_KIP,
        META_PROPOSITION_TYPE,
        CONSOLIDATED_TO_TYPE,
    ),
    (
        "derived_from",
        DERIVED_FROM_PROP_KIP,
        META_PROPOSITION_TYPE,
        DERIVED_FROM_TYPE,
    ),
    (
        "has_step",
        HAS_STEP_PROP_KIP,
        META_PROPOSITION_TYPE,
        HAS_STEP_TYPE,
    ),
    (
        "caused_by",
        CAUSED_BY_PROP_KIP,
        META_PROPOSITION_TYPE,
        CAUSED_BY_TYPE,
    ),
    (
        "derived_insight",
        DERIVED_INSIGHT_PROP_KIP,
        META_PROPOSITION_TYPE,
        DERIVED_INSIGHT_TYPE,
    ),
    (
        "compiled_to",
        COMPILED_TO_PROP_KIP,
        META_PROPOSITION_TYPE,
        COMPILED_TO_TYPE,
    ),
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
                async |collection| init_concepts_collection(collection).await,
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
                async |collection| init_propositions_collection(collection).await,
            )
            .await
            .map_err(db_to_kip_error)?;
        let this = Self {
            db,
            concepts: Arc::new(parking_lot::RwLock::new(concepts)),
            propositions: Arc::new(parking_lot::RwLock::new(propositions)),
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
    /// `$ConceptType` / `$PropositionType` definition node is missing
    /// (self-healing).
    ///
    /// Bundled capsules are idempotent `UPSERT` scripts, so re-applying one
    /// after a crate upgrade shallow-merges the revised definitions into an
    /// existing instance without touching user data (schema nodes get a
    /// regular `_version` bump). A failed apply leaves the stored hash
    /// untouched, so the next [`connect`](Self::connect) retries it.
    async fn sync_bundled_capsules(&self) -> Result<(), KipError> {
        for (name, source, anchor_type, anchor_name) in BUNDLED_CAPSULES {
            let key = format!("capsule_hash:{name}");
            let current = capsule_hash(source);
            let stored: Option<String> = self.concepts().get_extension_as(&key);
            let hash_current = stored.as_deref() == Some(current.as_str());
            let anchor_missing = !self
                .has_concept(&ConceptPK::Object {
                    r#type: anchor_type.to_string(),
                    name: anchor_name.to_string(),
                })
                .await;
            if hash_current && !anchor_missing {
                continue;
            }

            self.execute_kml_privileged(parse_kml(source)?)
                .await
                .map_err(|err| {
                    KipError::new(
                        err.code,
                        format!("Bundled capsule {name:?} bootstrap failed: {}", err.message),
                    )
                })?;

            if !hash_current {
                self.concepts()
                    .save_extension(key, Fv::Text(current))
                    .await
                    .map_err(db_to_kip_error)?;
            }
        }

        Ok(())
    }

    /// The current `concepts` collection handle (one row per [`Concept`]).
    ///
    /// Resolved through a swappable slot, so a handle replaced by
    /// [`reopen_collections`](Self::reopen_collections) is picked up by every
    /// later call. Hold the returned `Arc` for one operation, not across
    /// awaits that may recover it.
    pub fn concepts(&self) -> Arc<Collection> {
        self.concepts.read().clone()
    }

    /// The current `propositions` collection handle (one row per
    /// [`Proposition`]). See [`concepts`](Self::concepts).
    pub fn propositions(&self) -> Arc<Collection> {
        self.propositions.read().clone()
    }

    /// Re-resolves both collection handles through the database, replacing a
    /// handle that a cancelled mutation (or a failed flush) has poisoned.
    ///
    /// The recovery itself happens inside [`AndaDB::open_collection`]: it
    /// drains the poisoned handle, drops it *without* flushing its in-memory
    /// state, and reloads the collection with the mutation-intent replay and
    /// the repair scan. Re-opening a healthy collection returns the very same
    /// handle, so this is idempotent and cheap; the index setup is re-run
    /// (`create_*_nx` are no-ops) so the fresh handle regains the jieba
    /// tokenizer.
    ///
    /// Called automatically before and after every mutating statement (see
    /// [`ensure_live_collections`](Self::ensure_live_collections) and
    /// [`recover_if_poisoned`](Self::recover_if_poisoned)); exposed for hosts
    /// that drive their own recovery.
    ///
    /// Serialized against KML execution: swapping a handle mid-statement
    /// would let the statement's later steps resolve the fresh handle and
    /// silently complete "successfully" across two collection generations.
    /// The internal callers above already run under `kml_lock`; this public
    /// entry acquires it itself.
    pub async fn reopen_collections(&self) -> Result<(), KipError> {
        let _guard = self.kml_lock.write().await;
        self.reopen_collections_inner().await
    }

    /// [`reopen_collections`](Self::reopen_collections) without the
    /// `kml_lock` acquisition, for callers that already hold it.
    async fn reopen_collections_inner(&self) -> Result<(), KipError> {
        let concepts = self
            .db
            .open_collection("concepts".to_string(), async |collection| {
                init_concepts_collection(collection).await
            })
            .await
            .map_err(reopen_error)?;
        let propositions = self
            .db
            .open_collection("propositions".to_string(), async |collection| {
                init_propositions_collection(collection).await
            })
            .await
            .map_err(reopen_error)?;
        // Hold both slot guards before assigning so the pair swaps in one
        // step; a reader cannot observe new-concepts/old-propositions
        // between two separate assignments.
        let mut concepts_slot = self.concepts.write();
        let mut propositions_slot = self.propositions.write();
        *concepts_slot = concepts;
        *propositions_slot = propositions;
        Ok(())
    }

    /// Whether either handle has been poisoned by a cancelled mutation.
    ///
    /// Two relaxed atomic loads via [`Collection::is_poisoned`] — cheap enough
    /// to run before every mutating statement.
    fn has_poisoned_handle(&self) -> bool {
        self.concepts().is_poisoned() || self.propositions().is_poisoned()
    }

    /// Replaces an already-poisoned handle *before* a mutating statement is
    /// dispatched, so the statement runs on a live handle instead of costing
    /// the caller one guaranteed failure per poison event.
    ///
    /// Only [`CollectionState::Poisoned`](anda_db::error::CollectionState) is
    /// recovered here. `Closing` / `Closed` also count as recoverable in
    /// `anda_db`'s sense — their storage is intact — but reopening one would
    /// resurrect a collection the host deliberately closed; and from
    /// `Deleting` on, nothing is recoverable at all. Both are left to fail
    /// with `anda_db`'s own authoritative error.
    async fn ensure_live_collections(&self) -> Result<(), KipError> {
        if self.has_poisoned_handle() {
            self.reopen_collections_inner().await?;
        }
        Ok(())
    }

    /// Backstop for a handle poisoned *during* the statement that just failed
    /// (a cancelled concurrent mutation, or a flush that poisons on any
    /// error), so the next statement runs against a live handle instead of
    /// every statement failing until the process restarts.
    ///
    /// The trigger is the handle's own lifecycle state, not the error:
    /// `Collection::update` / `remove` poison on an unknown outcome and return
    /// the *underlying storage* error, so [`DBError::is_poisoned`] is false for
    /// exactly the case this backstop exists for. (A handle poisoned before
    /// dispatch never reaches here — [`ensure_live_collections`] already
    /// replaced it.)
    ///
    /// The failed statement is deliberately **not** retried: the poison may
    /// have landed between two of its writes, so it can be partially applied,
    /// and blindly re-running a non-idempotent `UPDATE` would apply it twice.
    async fn recover_if_poisoned<T>(&self, result: Result<T, KipError>) -> Result<T, KipError> {
        let Err(err) = result else {
            return result;
        };
        if !self.has_poisoned_handle() {
            return Err(err);
        }
        match self.reopen_collections_inner().await {
            // A failed reopen leaves the poisoned handle in place; the next
            // statement's pre-flight retries the recovery.
            Err(_) => Err(err),
            Ok(()) => Err(KipError::new(
                err.code,
                format!(
                    "{}; a collection handle was poisoned during this statement and has been \
                     reopened — the statement may have applied partially, verify before \
                     re-running it",
                    err.message
                ),
            )),
        }
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
        self.concepts()
            .get_extension("capsule_version")
            .and_then(|v| u64::try_from(v).ok())
            .unwrap_or(0)
    }

    /// Persists the capsule schema version. Called automatically by
    /// [`CognitiveNexus::connect`] after the bundled Genesis capsules
    /// have been applied; downstream applications can call it to record
    /// their own migration steps.
    pub async fn save_capsule_version(&self, version: u64) -> Result<(), KipError> {
        // A mutating step: hold `kml_lock` like every KML statement so the
        // ensure/recover reopen paths stay serialized against execution.
        let _guard = self.kml_lock.write().await;
        self.ensure_live_collections().await?;
        let result = self
            .concepts()
            .save_extension("capsule_version".to_string(), version.into())
            .await
            .map_err(db_to_kip_error);
        self.recover_if_poisoned(result).await
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

        self.concepts().contains(id)
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

        self.concepts().get_as(id).await.map_err(db_to_kip_error)
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
    ///
    /// The query→insert pair runs under the exclusive KML write lock so
    /// concurrent callers (or a concurrent KML statement) cannot race it
    /// into duplicate `{type, name}` concepts. A freshly created concept
    /// gets the engine-maintained `_version` / `_updated_at` system
    /// metadata, exactly like the KML `UPSERT` path (KIP §2.11).
    ///
    /// Like [`execute_kml`](Self::execute_kml), a poisoned handle is reopened
    /// before the insert is attempted (and after it, should the poison land
    /// mid-call).
    pub async fn get_or_init_concept(
        &self,
        r#type: String,
        name: String,
        attributes: Map<String, Json>,
        metadata: Map<String, Json>,
    ) -> Result<Concept, KipError> {
        let _guard = self.kml_lock.write().await;
        self.ensure_live_collections().await?;
        let result = match self.query_concept_id(&r#type, &name).await {
            Ok(id) => self.concepts().get_as(id).await.map_err(db_to_kip_error),
            Err(_) => {
                let mut metadata = metadata;
                init_system_metadata(&mut metadata, unix_ms());
                let mut concept = Concept {
                    _id: 0, // Will be set by the database
                    r#type,
                    name,
                    attributes,
                    metadata,
                };
                match self.concepts().add_from(&concept).await {
                    Ok(id) => {
                        concept._id = id;
                        Ok(concept)
                    }
                    Err(err) => Err(db_to_kip_error(err)),
                }
            }
        };
        self.recover_if_poisoned(result).await
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
    ///   types exist, that all variable handles can be resolved, that no
    ///   block writes attributes or metadata to a protected schema node
    ///   (`KIP_3004`), and that every `EXPECT VERSION` guard matches
    ///   (`KIP_3005` otherwise), but does **not** create or update any row.
    /// - `UPDATE` / `MERGE` run their full validation (including the
    ///   `KIP_3004` protected-scope checks) and pattern matching without
    ///   writing.
    /// - `DELETE CONCEPT`, `DELETE PROPOSITIONS` and protected
    ///   `DELETE ATTRIBUTES` targets still perform the `KIP_3004`
    ///   protected-scope pre-flight check so agents can probe for safety
    ///   without side effects.
    /// - Other delete variants short-circuit and return zeroed counters.
    ///
    /// On success the returned JSON is shaped per KIP §4 — for upserts an
    /// [`UpsertResult`], for updates `{"updated": N, "matched": N}`, for
    /// merges `{"merged": true, "links_repointed": N, …}`, for deletes a
    /// `{"deleted_*": N, "updated_*": N}` map. KML acquires the write lock
    /// so it executes exclusively.
    ///
    /// # Atomicity
    ///
    /// Statement-level failure modes (unknown types, missing targets,
    /// `EXPECT VERSION` conflicts, protected-scope violations, …) are caught
    /// by a validation preflight **before** any row is written, so they
    /// leave the graph untouched. Mid-execution storage failures, however,
    /// are not rolled back — the engine has no write-ahead transaction log —
    /// so a crashed multi-block `UPSERT` may leave a prefix of its blocks
    /// applied. Bundled capsules and well-formed `UPSERT`s are idempotent,
    /// so the standard recovery is to re-run the statement.
    pub async fn execute_kml(
        &self,
        command: KmlStatement,
        dry_run: bool,
    ) -> Result<Json, KipError> {
        let _guard = self.kml_lock.write().await;
        self.ensure_live_collections().await?;
        let result = self.execute_kml_inner(command, dry_run, false).await;
        self.recover_if_poisoned(result).await
    }

    /// Bootstrap-privileged [`execute_kml`](Self::execute_kml): the
    /// `KIP_3004` protected-scope checks that reject writes to the
    /// foundational schema nodes are bypassed, because the bundled capsules
    /// are precisely what *defines* those nodes (Genesis creates
    /// `$ConceptType`, `$PropositionType`, `Domain`, `belongs_to_domain` and
    /// the `CoreSchema` domain, and re-applies them on every crate upgrade).
    ///
    /// Deliberately private and only reachable from
    /// [`sync_bundled_capsules`](Self::sync_bundled_capsules): every
    /// caller-supplied statement goes through `execute_kml`, which never
    /// raises the flag.
    async fn execute_kml_privileged(&self, command: KmlStatement) -> Result<Json, KipError> {
        let _guard = self.kml_lock.write().await;
        self.ensure_live_collections().await?;
        let result = self.execute_kml_inner(command, false, true).await;
        self.recover_if_poisoned(result).await
    }

    /// `privileged` bypasses the protected-scope checks; see
    /// [`execute_kml_privileged`](Self::execute_kml_privileged).
    async fn execute_kml_inner(
        &self,
        command: KmlStatement,
        dry_run: bool,
        privileged: bool,
    ) -> Result<Json, KipError> {
        match command {
            KmlStatement::Upsert(upsert_blocks) => {
                self.execute_upsert(upsert_blocks, dry_run, privileged)
                    .await
            }
            KmlStatement::Update(update_statement) => {
                self.execute_update(update_statement, dry_run).await
            }
            KmlStatement::Merge(merge_statement) => {
                self.execute_merge(merge_statement, dry_run).await
            }
            KmlStatement::Delete(delete_statement) => {
                self.execute_delete(delete_statement, dry_run, privileged)
                    .await
            }
        }
    }

    /// Executes a META command (`DESCRIBE …`, `SEARCH …`, or `EXPORT …`).
    ///
    /// META commands are read-only; they acquire the KML read lock and
    /// return `(value, next_cursor)` with the same conventions as
    /// [`execute_kql`](Self::execute_kql). `DESCRIBE PRIMER`, `DESCRIBE
    /// DOMAINS` and `SEARCH` return non-paginated payloads
    /// (`next_cursor == None`); `DESCRIBE CONCEPT|PROPOSITION TYPES` and
    /// `EXPORT` paginate via `LIMIT` / `CURSOR`.
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
            MetaCommand::Export(command) => self.execute_export(command).await,
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
            .concepts()
            .query_all_ids(Filter::Field((virtual_name, RangeQuery::Eq(virtual_val))))
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

    /// Resolves a KQL concept matcher to concept ids.
    ///
    /// Grounding semantics (intentional asymmetry): fully-identified
    /// matchers — `{id: "…"}` (spec RC8) and `{type, name}` — refer to one
    /// specific node, so a missing target fails the query with `KIP_3002`
    /// to tell the agent its grounding is stale. Pattern matchers
    /// (`{type: …}` / `{name: …}`) describe a *set* and simply bind empty.
    async fn query_concept_ids(&self, matcher: &ConceptMatcher) -> Result<Vec<u64>, KipError> {
        match matcher {
            ConceptMatcher::ID(id) => {
                let entity_id = EntityID::from_str(id).map_err(KipError::invalid_syntax)?;
                if let EntityID::Concept(concept_id) = entity_id {
                    // Match-only `{id:}` target: a dangling id is a grounding
                    // error (`KIP_3002`), not an empty match — otherwise it
                    // would silently drain every joined pattern (spec RC8).
                    if !self.concepts().contains(concept_id) {
                        return Err(KipError::not_found(format!(
                            "Concept {} not found",
                            ConceptPK::ID(concept_id)
                        )));
                    }
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
                    .concepts()
                    .query_all_ids(Filter::Field((
                        "type".to_string(),
                        RangeQuery::Eq(Fv::Text(type_name.clone())),
                    )))
                    .await
                    .map_err(db_to_kip_error)?;
                Ok(ids)
            }
            ConceptMatcher::Name(name) => {
                let ids = self
                    .concepts()
                    .query_all_ids(Filter::Field((
                        "name".to_string(),
                        RangeQuery::Eq(Fv::Text(name.clone())),
                    )))
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
        let concept: Concept = self.concepts().get_as(id).await.map_err(db_to_kip_error)?;
        let rt = f(&concept)?;
        cache.concepts.write().insert(id, concept);
        Ok(rt)
    }

    /// Verifies that a match-only `(id: "…")` target refers to an existing
    /// proposition link (the row exists and carries the predicate). A
    /// dangling link id is a grounding error (`KIP_3002`, spec RC8), not an
    /// empty match — it would otherwise silently drain joined patterns.
    async fn ensure_proposition_link_exists(
        &self,
        cache: &QueryCache,
        entity_id: &EntityID,
    ) -> Result<(), KipError> {
        if let EntityID::Proposition(id, predicate) = entity_id {
            if self.propositions().contains(*id)
                && self
                    .try_get_proposition_with(cache, *id, |prop| {
                        Ok(prop.predicates.contains(predicate))
                    })
                    .await?
            {
                return Ok(());
            }
            return Err(KipError::not_found(format!(
                "Proposition {} not found",
                PropositionPK::ID(*id, predicate.clone())
            )));
        }
        Ok(())
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
            .propositions()
            .get_as(id)
            .await
            .map_err(db_to_kip_error)?;
        let rt = f(&proposition)?;
        cache.propositions.write().insert(id, proposition);
        Ok(rt)
    }
}
