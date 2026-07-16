//! Shared server state: the database registry and its lifecycle.
//!
//! The server always keeps one *primary* database open. Additional databases
//! created or opened at runtime are recorded in the primary database's
//! extension metadata (key [`DB_REGISTRY_KEY`]) so they are reopened
//! automatically after a restart.
//!
//! Every open database runs its own background auto-flush task. Closing a
//! database (or shutting the server down) cancels the task, which flushes and
//! closes the database before exiting.

use anda_db::{
    database::{AndaDB, DBConfig, DBMetadata},
    schema::validate_field_name,
    storage::StorageConfig,
};
use object_store::ObjectStore;
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    net::SocketAddr,
    sync::{
        Arc, Mutex as StdMutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, RwLock, Semaphore},
    task::{AbortHandle, JoinHandle},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::error::ApiError;

/// Extension key in the primary database that stores the names of all
/// non-primary databases to reopen on startup.
pub const DB_REGISTRY_KEY: &str = "server:databases";

/// Extra bounded window used only to observe task termination after issuing
/// aborts at the graceful shutdown deadline.
const FORCED_ABORT_JOIN_TIMEOUT: Duration = Duration::from_secs(1);

/// Server bootstrap options.
#[derive(Debug, Clone)]
pub struct ServerOptions {
    /// Server display name (returned by `info`).
    pub name: String,
    /// Server version (returned by `info`).
    pub version: String,
    /// Name of the primary database; it is created on first start and also
    /// stores the database registry.
    pub primary_db: String,
    /// Description used when creating the primary database.
    pub description: String,
    /// Storage configuration applied to every database this server opens.
    pub storage: StorageConfig,
    /// Optional bearer token required for all RPC endpoints.
    pub api_key: Option<String>,
    /// Interval of the per-database background flush task.
    pub flush_interval: Duration,
    /// Per-request processing deadline for the RPC endpoints.
    pub request_timeout: Duration,
    /// Maximum number of non-cancel-safe mutating RPCs that may run at once.
    pub max_concurrent_mutations: usize,
    /// Maximum time, measured from admission close, for admitted mutating
    /// RPCs to finish before crash-style abort (no database flush/close).
    pub shutdown_timeout: Duration,
    /// Maximum accepted request body size in bytes.
    pub max_body_size: usize,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            name: "anda_db_server".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            primary_db: "anda_db".to_string(),
            description: "Anda DB".to_string(),
            storage: StorageConfig::default(),
            api_key: None,
            flush_interval: Duration::from_secs(30),
            request_timeout: Duration::from_secs(300),
            max_concurrent_mutations: 32,
            shutdown_timeout: Duration::from_secs(30),
            max_body_size: 2 * 1024 * 1024,
        }
    }
}

/// How [`AppState::register_db`] should treat an existing database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    /// Create a new database; fail if it already exists.
    Create,
    /// Open an existing database; fail if it does not exist.
    Open,
    /// Open an existing database or create it if missing.
    Connect,
}

/// Server information returned by the `info` method.
#[derive(Debug, Serialize)]
pub struct ServerInfo {
    /// Server display name.
    pub name: String,
    /// Server version.
    pub version: String,
    /// Name of the primary database.
    pub primary_db: String,
    /// Names of all currently open databases.
    pub databases: Vec<String>,
}

struct DbEntry {
    db: AndaDB,
    cancel: CancellationToken,
    flush_task: JoinHandle<()>,
}

impl Drop for DbEntry {
    fn drop(&mut self) {
        // Backstop: an entry dropped without an explicit cancel (e.g. a
        // future code path that removes it from the registry and is then
        // itself cancelled) must not leave the auto-flush task running
        // forever with an open database — that task would keep writing to
        // storage while a reopened instance of the same database also
        // writes, i.e. two writers on one store. Cancelling here makes the
        // flush task flush, close the database, and exit on its own.
        self.cancel.cancel();
    }
}

/// Removes a task's abort handle when its tracked future exits, including
/// cancellation and panic paths.
struct TrackedTaskRegistration {
    id: u64,
    aborts: Arc<StdMutex<BTreeMap<u64, AbortHandle>>>,
}

impl Drop for TrackedTaskRegistration {
    fn drop(&mut self) {
        self.aborts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.id);
    }
}

struct Inner {
    options: ServerOptions,
    object_store: Arc<dyn ObjectStore>,
    cancel: CancellationToken,
    /// Set once [`AppState::shutdown`] begins; the RPC entry points reject
    /// new requests with 503 so a request accepted after the graceful-drain
    /// deadline cannot race the database close.
    shutting_down: AtomicBool,
    /// Serializes the synchronous admission decision with [`AppState::begin_shutdown`].
    /// A mutation is registered with `mutation_tasks` while holding this
    /// lock, so closing the tracker cannot race a late task spawn.
    rpc_admission: StdMutex<()>,
    /// Monotonic start of the shutdown budget. Recording it in
    /// `begin_shutdown` prevents the HTTP drain and mutation drain from each
    /// consuming a fresh full timeout.
    shutdown_started: OnceLock<Instant>,
    /// Cancels admitted read-only RPCs when shutdown begins. Read operations
    /// run in their handler task and are safe to drop.
    read_cancel: CancellationToken,
    /// Bounds non-cancel-safe mutations. A permit lives inside the spawned
    /// task, not the HTTP handler, so a timeout/disconnect cannot release the
    /// slot while the mutation is still running.
    mutation_slots: Arc<Semaphore>,
    /// Tracks every admitted non-cancel-safe mutation until its future exits.
    mutation_tasks: TaskTracker,
    /// Abort handles retained for the hard-deadline path. Normal shutdown
    /// never aborts mutations; once the deadline is exceeded, aborting them
    /// is treated as a process-crash boundary and databases are not flushed.
    mutation_aborts: Arc<StdMutex<BTreeMap<u64, AbortHandle>>>,
    next_mutation_id: AtomicU64,
    /// Tracks every database auto-flush owner independently of `DbEntry`.
    /// A `db.close` mutation temporarily moves its entry out of `databases`;
    /// global tracking keeps the task reachable on hard shutdown even if the
    /// mutation is then aborted and drops its JoinHandle.
    db_tasks: TaskTracker,
    db_task_aborts: Arc<StdMutex<BTreeMap<u64, AbortHandle>>>,
    next_db_task_id: AtomicU64,
    databases: RwLock<BTreeMap<String, DbEntry>>,
    /// Names of non-primary databases that should be reopened on the next
    /// start. Kept separate from `databases` so a database that failed to
    /// reopen stays registered (and is retried on the next start or via
    /// `db.open`) instead of being silently dropped by `persist_registry`.
    registry: RwLock<BTreeSet<String>>,
    /// Serializes database lifecycle operations (`register_db`/`close_db`).
    /// Storage I/O such as `AndaDB::open` runs while holding only this mutex,
    /// never the `databases` lock, so data RPCs are not blocked by a slow
    /// open/create/close.
    lifecycle: Mutex<()>,
}

/// Shared state for all RPC handlers. Cheap to clone.
#[derive(Clone)]
pub struct AppState {
    inner: Arc<Inner>,
}

impl AppState {
    /// Opens the primary database, reopens every registered database, and
    /// starts their background flush tasks.
    pub async fn connect(
        object_store: Arc<dyn ObjectStore>,
        options: ServerOptions,
    ) -> Result<Self, ApiError> {
        if matches!(options.api_key.as_deref(), Some(key) if key.trim().is_empty()) {
            return Err(ApiError::bad_request("API key must not be empty"));
        }

        validate_field_name(&options.primary_db)
            .map_err(|e| ApiError::invalid_input(format!("invalid primary database name: {e}")))?;

        let primary = AndaDB::connect(
            object_store.clone(),
            DBConfig {
                name: options.primary_db.clone(),
                description: options.description.clone(),
                storage: options.storage.clone(),
                lock: None,
            },
        )
        .await?;

        // Distinguish "no registry yet" from "registry corrupted": starting
        // with an unreadable registry and then persisting would overwrite it
        // with an empty set, silently dropping every registered database.
        let registered: BTreeSet<String> = match primary.get_extension(DB_REGISTRY_KEY) {
            Some(value) => value.deserialized().map_err(|err| {
                ApiError::internal(format!(
                    "failed to parse database registry extension {DB_REGISTRY_KEY:?}: {err}; \
                     refusing to start to avoid overwriting the registry"
                ))
            })?,
            None => BTreeSet::new(),
        };

        let max_concurrent_mutations = options.max_concurrent_mutations.max(1);
        let state = Self {
            inner: Arc::new(Inner {
                options,
                object_store,
                cancel: CancellationToken::new(),
                shutting_down: AtomicBool::new(false),
                rpc_admission: StdMutex::new(()),
                shutdown_started: OnceLock::new(),
                read_cancel: CancellationToken::new(),
                mutation_slots: Arc::new(Semaphore::new(max_concurrent_mutations)),
                mutation_tasks: TaskTracker::new(),
                mutation_aborts: Arc::new(StdMutex::new(BTreeMap::new())),
                next_mutation_id: AtomicU64::new(0),
                db_tasks: TaskTracker::new(),
                db_task_aborts: Arc::new(StdMutex::new(BTreeMap::new())),
                next_db_task_id: AtomicU64::new(0),
                databases: RwLock::new(BTreeMap::new()),
                registry: RwLock::new(BTreeSet::new()),
                lifecycle: Mutex::new(()),
            }),
        };

        {
            let mut dbs = state.inner.databases.write().await;
            let mut registry = state.inner.registry.write().await;
            let entry = state.new_entry(primary);
            dbs.insert(state.inner.options.primary_db.clone(), entry);

            for name in registered {
                if name == state.inner.options.primary_db {
                    continue;
                }
                // Keep the name registered even when the open fails, so a
                // transient storage error does not permanently remove the
                // database from the registry; it is retried on the next
                // start (or reopened at runtime via `db.open`).
                registry.insert(name.clone());
                let config = DBConfig {
                    name: name.clone(),
                    description: name.clone(),
                    storage: state.inner.options.storage.clone(),
                    lock: None,
                };
                match AndaDB::open(state.inner.object_store.clone(), config).await {
                    Ok(db) => {
                        let entry = state.new_entry(db);
                        dbs.insert(name, entry);
                    }
                    Err(err) => {
                        log::error!(
                            action = "AppState::connect",
                            database = name;
                            "failed to reopen registered database: {err:?}",
                        );
                    }
                }
            }
        }

        Ok(state)
    }

    /// Returns the configured API key, if any.
    pub fn api_key(&self) -> Option<&str> {
        self.inner.options.api_key.as_deref()
    }

    /// Returns the per-request processing deadline for the RPC endpoints.
    pub fn request_timeout(&self) -> Duration {
        self.inner.options.request_timeout
    }

    /// Returns the maximum accepted request body size in bytes.
    pub fn max_body_size(&self) -> usize {
        self.inner.options.max_body_size
    }

    /// Returns `true` once [`AppState::shutdown`] has begun. The RPC entry
    /// points check this and reject new requests with 503.
    pub fn is_shutting_down(&self) -> bool {
        self.inner.shutting_down.load(Ordering::Acquire)
    }

    /// Admits a cancel-safe read RPC and returns the token that cancels it
    /// when shutdown starts. The admission check is serialized with
    /// [`AppState::begin_shutdown`], so a read cannot slip in after shutdown
    /// closes admission.
    pub(crate) fn admit_read(&self) -> Result<CancellationToken, ApiError> {
        let _guard = self
            .inner
            .rpc_admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.is_shutting_down() {
            return Err(ApiError::unavailable());
        }
        Ok(self.inner.read_cancel.clone())
    }

    /// Acquires a bounded mutation slot, atomically admits the operation, and
    /// spawns it into the shutdown tracker. Dropping the returned handle only
    /// detaches the response waiter; the tracker and semaphore permit remain
    /// attached to the mutation until it exits.
    pub(crate) async fn spawn_mutation<F, T>(&self, mutation: F) -> Result<JoinHandle<T>, ApiError>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let permit = self
            .inner
            .mutation_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ApiError::unavailable())?;

        let _guard = self
            .inner
            .rpc_admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.is_shutting_down() {
            return Err(ApiError::unavailable());
        }

        // Prevent a very fast task from completing before its abort handle is
        // inserted: the tracked future does not poll `mutation` until the
        // synchronous registration below sends the start signal.
        let id = self.inner.next_mutation_id.fetch_add(1, Ordering::Relaxed);
        let aborts = self.inner.mutation_aborts.clone();
        let (start_tx, start_rx) = tokio::sync::oneshot::channel();
        let task = self.inner.mutation_tasks.spawn(async move {
            let _permit = permit;
            let _registration = TrackedTaskRegistration { id, aborts };
            start_rx
                .await
                .expect("mutation task start sender dropped before registration");
            mutation.await
        });
        self.inner
            .mutation_aborts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, task.abort_handle());
        start_tx
            .send(())
            .expect("mutation task exited before start signal");
        Ok(task)
    }

    /// Closes RPC admission immediately, cancels active read-only RPCs, and
    /// closes the mutation tracker. Already-admitted mutations continue under
    /// tracking and are drained by [`AppState::shutdown`].
    pub fn begin_shutdown(&self) {
        let _guard = self
            .inner
            .rpc_admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.inner.shutting_down.swap(true, Ordering::AcqRel) {
            return;
        }
        let _ = self.inner.shutdown_started.set(Instant::now());
        self.inner.mutation_slots.close();
        self.inner.mutation_tasks.close();
        self.inner.db_tasks.close();
        self.inner.read_cancel.cancel();
    }

    /// Returns server information including all open database names.
    pub async fn info(&self) -> ServerInfo {
        ServerInfo {
            name: self.inner.options.name.clone(),
            version: self.inner.options.version.clone(),
            primary_db: self.inner.options.primary_db.clone(),
            databases: self.db_names().await,
        }
    }

    /// Returns the names of all currently open databases.
    pub async fn db_names(&self) -> Vec<String> {
        self.inner.databases.read().await.keys().cloned().collect()
    }

    /// Returns an open database by name.
    pub async fn get_db(&self, name: &str) -> Result<AndaDB, ApiError> {
        self.inner
            .databases
            .read()
            .await
            .get(name)
            .map(|entry| entry.db.clone())
            .ok_or_else(|| ApiError::not_found(format!("database {name:?} not found")))
    }

    /// Creates, opens, or connects a database and registers it for reopening
    /// on the next server start.
    pub async fn register_db(
        &self,
        mode: OpenMode,
        name: &str,
        description: Option<String>,
    ) -> Result<DBMetadata, ApiError> {
        validate_field_name(name)
            .map_err(|e| ApiError::invalid_input(format!("invalid database name: {e}")))?;

        // Serialize lifecycle operations; this also prevents two concurrent
        // requests from opening the same database twice.
        let _guard = self.inner.lifecycle.lock().await;

        {
            let dbs = self.inner.databases.read().await;
            if let Some(entry) = dbs.get(name) {
                return match mode {
                    OpenMode::Create => Err(ApiError::already_exists(format!(
                        "database {name:?} already exists"
                    ))),
                    OpenMode::Open | OpenMode::Connect => Ok(entry.db.metadata()),
                };
            }
        }

        let config = DBConfig {
            name: name.to_string(),
            description: description.unwrap_or_else(|| name.to_string()),
            storage: self.inner.options.storage.clone(),
            lock: None,
        };
        // Real object-store I/O; runs without holding the `databases` lock so
        // a slow open/create does not stall unrelated RPCs.
        let db = match mode {
            OpenMode::Create => {
                match AndaDB::create(self.inner.object_store.clone(), config).await {
                    Ok(db) => db,
                    // At this call boundary `AlreadyExists` refers to the
                    // requested database metadata object. Return only the
                    // client-provided logical name; never the engine path.
                    Err(anda_db::error::DBError::AlreadyExists { .. }) => {
                        return Err(ApiError::already_exists(format!(
                            "database {name:?} already exists"
                        )));
                    }
                    Err(err) => return Err(err.into()),
                }
            }
            OpenMode::Open => {
                match AndaDB::open(self.inner.object_store.clone(), config).await {
                    Ok(db) => db,
                    // `AndaDB::open` performs this lookup before loading any
                    // collection, so this context proves a missing database
                    // rather than blindly trusting a nested NotFound variant.
                    Err(anda_db::error::DBError::NotFound { .. }) => {
                        return Err(ApiError::not_found(format!("database {name:?} not found")));
                    }
                    Err(err) => return Err(err.into()),
                }
            }
            OpenMode::Connect => AndaDB::connect(self.inner.object_store.clone(), config).await?,
        };

        let metadata = db.metadata();
        let mut pending_db = Some(db);
        let mut dbs = self.inner.databases.write().await;
        let rejected_by_shutdown = {
            let _admission = self
                .inner
                .rpc_admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.is_shutting_down() {
                true
            } else {
                let entry = self.new_entry(pending_db.take().expect("pending database missing"));
                dbs.insert(name.to_string(), entry);
                false
            }
        };
        drop(dbs);
        if rejected_by_shutdown {
            if let Some(db) = pending_db
                && let Err(err) = db.close().await
            {
                log::error!(
                    action = "AppState::register_db",
                    database = name;
                    "failed to close database whose registration lost to shutdown: {err:?}",
                );
            }
            return Err(ApiError::unavailable());
        }
        {
            self.inner.registry.write().await.insert(name.to_string());
        }
        if let Err(err) = self.persist_registry().await {
            // Success must mean "reopened automatically after a restart".
            // Unwind the registration completely so a client retry repeats
            // the whole open + persist flow.
            {
                self.inner.registry.write().await.remove(name);
            }
            let entry = { self.inner.databases.write().await.remove(name) };
            if let Some(entry) = &entry {
                entry.cancel.cancel();
            }
            if let Some(mut entry) = entry
                && let Err(close_err) = (&mut entry.flush_task).await
            {
                log::error!(
                    action = "AppState::register_db",
                    database = name;
                    "failed to close database after registry persistence failure: {close_err:?}",
                );
            }
            return Err(err);
        }
        Ok(metadata)
    }

    /// Flushes and closes a database, removing it from the registry so it is
    /// not reopened on the next start. The primary database cannot be closed.
    pub async fn close_db(&self, name: &str) -> Result<(), ApiError> {
        if name == self.inner.options.primary_db {
            return Err(ApiError::invalid_input(
                "the primary database cannot be closed",
            ));
        }

        let _guard = self.inner.lifecycle.lock().await;

        let entry = { self.inner.databases.write().await.remove(name) };
        // Cancel the flush task immediately — before any `.await` below.
        // Once the entry has left `databases` it is invisible to
        // `persist_registry`/`get_db`, so if this future were cancelled at a
        // later await point without the token cancelled, the auto-flush task
        // would keep running forever with an open database, and a client
        // retrying `db.open` would create a second writer on the same
        // storage. Cancelling the token makes `AndaDB::auto_flush` flush and
        // close the database on its own even if we never reach the
        // `flush_task.await` below (the `DbEntry` drop is a further
        // backstop).
        if let Some(entry) = &entry {
            entry.cancel.cancel();
        }
        // A registered database that failed to reopen has no open entry;
        // closing it just removes it from the registry.
        let registered = { self.inner.registry.write().await.remove(name) };
        if entry.is_none() && !registered {
            return Err(ApiError::not_found(format!("database {name:?} not found")));
        }
        let persisted = self.persist_registry().await;
        if persisted.is_err() && registered {
            // The database still closes below, but the durable registry
            // would reopen it after a restart. Keep it registered in memory
            // so a retry of `db.close` can attempt the durable removal again.
            self.inner.registry.write().await.insert(name.to_string());
        }

        if let Some(mut entry) = entry {
            // Wait for `AndaDB::auto_flush` to close the database (flushing
            // all collections) before reporting success.
            if let Err(err) = (&mut entry.flush_task).await {
                log::error!(
                    action = "AppState::close_db",
                    database = name;
                    "flush task failed: {err:?}",
                );
            }
        }
        // Success means the close is durable: the database will not be
        // reopened on the next start.
        persisted
    }

    /// Flushes and closes every open database. Called on server shutdown.
    ///
    /// Closes admission and drains every tracked mutation before databases
    /// are closed. The drain is bounded by [`ServerOptions::shutdown_timeout`].
    /// If that deadline expires, mutation tasks and auto-flush tasks are
    /// explicitly aborted and joined, and databases are dropped without a
    /// final flush/close. This is intentionally crash-equivalent: flushing a
    /// future that was cancelled at an arbitrary await could publish partial
    /// in-memory state. Durable recovery handles the next open.
    pub async fn shutdown(&self) {
        self.begin_shutdown();
        let remaining = self
            .inner
            .shutdown_started
            .get()
            .map(|started| {
                self.inner
                    .options
                    .shutdown_timeout
                    .saturating_sub(started.elapsed())
            })
            .unwrap_or(self.inner.options.shutdown_timeout);
        if tokio::time::timeout(remaining, self.inner.mutation_tasks.wait())
            .await
            .is_err()
        {
            log::error!(
                action = "AppState::shutdown",
                in_flight_mutations = self.inner.mutation_tasks.len();
                "mutation drain deadline exceeded; forcing crash-style task abort without database flush",
            );
            self.abort_after_shutdown_deadline().await;
            return;
        }

        self.inner.cancel.cancel();
        let entries: Vec<DbEntry> = {
            let mut dbs = self.inner.databases.write().await;
            std::mem::take(&mut *dbs).into_values().collect()
        };
        for mut entry in entries {
            let name = entry.db.name().to_string();
            if let Err(err) = (&mut entry.flush_task).await {
                log::error!(
                    action = "AppState::shutdown",
                    database = name;
                    "flush task failed: {err:?}",
                );
            }
        }
        // Covers any future lifecycle path that moves an entry out of the map
        // before shutdown takes its snapshot. Current `close_db` mutations
        // await their owner before the mutation tracker drains, so this is
        // normally already empty.
        self.inner.db_tasks.wait().await;
    }

    /// Hard-deadline shutdown path. Abort and join mutations first; only once
    /// none can touch a database do we abort its auto-flush owner. We never
    /// call `AndaDB::close` here because an aborted mutation may have left
    /// recoverable, but not safely flushable, intermediate in-memory state.
    async fn abort_after_shutdown_deadline(&self) {
        let aborts: Vec<AbortHandle> = self
            .inner
            .mutation_aborts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        for abort in aborts {
            abort.abort();
        }

        if tokio::time::timeout(FORCED_ABORT_JOIN_TIMEOUT, self.inner.mutation_tasks.wait())
            .await
            .is_err()
        {
            // A future that never yields cannot observe Tokio cancellation.
            // Do not touch its databases; runtime teardown is the only safe
            // remaining process-level boundary.
            log::error!(
                action = "AppState::abort_after_shutdown_deadline",
                in_flight_mutations = self.inner.mutation_tasks.len();
                "aborted mutations did not terminate; leaving database tasks untouched",
            );
            return;
        }

        // Auto-flush tasks have their own global registry because a
        // `db.close` mutation moves its `DbEntry` out of `databases` while it
        // awaits that task. Aborting the mutation drops/detaches the local
        // JoinHandle, but this registry still reaches the task.
        let db_aborts: Vec<AbortHandle> = self
            .inner
            .db_task_aborts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        for abort in db_aborts {
            abort.abort();
        }
        if tokio::time::timeout(FORCED_ABORT_JOIN_TIMEOUT, self.inner.db_tasks.wait())
            .await
            .is_err()
        {
            log::error!(
                action = "AppState::abort_after_shutdown_deadline",
                in_flight_database_tasks = self.inner.db_tasks.len();
                "aborted database tasks did not terminate; leaving registry owners untouched",
            );
            return;
        }

        let mut entries: Vec<DbEntry> = {
            let mut dbs = self.inner.databases.write().await;
            std::mem::take(&mut *dbs).into_values().collect()
        };
        // Consume the still-owned JoinHandles. Entries moved into an aborted
        // `db.close` future are already gone, but their tasks were covered by
        // the global tracker above.
        for entry in &mut entries {
            match (&mut entry.flush_task).await {
                Ok(()) => {}
                Err(err) if err.is_cancelled() => {}
                Err(err) => {
                    log::error!(
                        action = "AppState::abort_after_shutdown_deadline",
                        database = entry.db.name();
                        "auto-flush task failed during forced abort: {err:?}",
                    );
                }
            }
        }
        // Entries drop here and cancel their child tokens as a backstop. All
        // database tasks have already exited, so this cannot start a close.
        self.inner.cancel.cancel();
    }

    /// Spawns the background flush task for an open database.
    fn new_entry(&self, db: AndaDB) -> DbEntry {
        let cancel = self.inner.cancel.child_token();
        let id = self.inner.next_db_task_id.fetch_add(1, Ordering::Relaxed);
        let aborts = self.inner.db_task_aborts.clone();
        let (start_tx, start_rx) = tokio::sync::oneshot::channel();
        let flush_task = self.inner.db_tasks.spawn({
            let db = db.clone();
            let cancel = cancel.clone();
            let interval = self.inner.options.flush_interval;
            async move {
                let _registration = TrackedTaskRegistration { id, aborts };
                start_rx
                    .await
                    .expect("database task start sender dropped before registration");
                db.auto_flush(cancel, interval).await;
            }
        });
        self.inner
            .db_task_aborts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, flush_task.abort_handle());
        start_tx
            .send(())
            .expect("database task exited before start signal");
        DbEntry {
            db,
            cancel,
            flush_task,
        }
    }

    /// Persists the registered non-primary database names into the primary
    /// database's extensions.
    ///
    /// Callers whose RPC success implies a durable lifecycle transition
    /// (`db.create`/`db.open` reopening after restart, `db.close` staying
    /// closed) must propagate this error instead of reporting success.
    async fn persist_registry(&self) -> Result<(), ApiError> {
        let names: BTreeSet<String> = { self.inner.registry.read().await.clone() };
        let primary = {
            let dbs = self.inner.databases.read().await;
            dbs.get(&self.inner.options.primary_db)
                .map(|entry| entry.db.clone())
        };
        if let Some(db) = primary
            && let Err(err) = db
                .save_extension_from(DB_REGISTRY_KEY.to_string(), &names)
                .await
        {
            log::error!(
                action = "AppState::persist_registry",
                database = self.inner.options.primary_db;
                "failed to persist database registry: {err:?}",
            );
            return Err(err.into());
        }
        Ok(())
    }
}

/// Validates the API-key/listen-address combination at startup.
///
/// - An explicitly configured empty (or whitespace-only) API key is always
///   rejected: it would look enabled while accepting any request.
/// - Listening on a non-loopback address without an API key is rejected
///   unless `insecure_no_api_key` explicitly opts in — otherwise the whole
///   RPC API (including database creation and deletion of documents) would
///   be open to anyone who can reach the listener.
pub fn check_startup_api_key(
    api_key: Option<&str>,
    addr: &SocketAddr,
    insecure_no_api_key: bool,
) -> Result<(), String> {
    match api_key {
        Some(key) if key.trim().is_empty() => Err("API_KEY must not be empty".to_string()),
        Some(_) => Ok(()),
        None if addr.ip().is_loopback() || insecure_no_api_key => Ok(()),
        None => Err(format!(
            "refusing to listen on non-loopback address {addr} without API_KEY: \
             the RPC API would be open to anyone; set API_KEY or pass \
             --insecure-no-api-key to override"
        )),
    }
}
