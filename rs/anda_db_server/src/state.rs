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
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Mutex, RwLock},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::error::ApiError;

/// Extension key in the primary database that stores the names of all
/// non-primary databases to reopen on startup.
pub const DB_REGISTRY_KEY: &str = "server:databases";

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

struct Inner {
    options: ServerOptions,
    object_store: Arc<dyn ObjectStore>,
    cancel: CancellationToken,
    /// Set once [`AppState::shutdown`] begins; the RPC entry points reject
    /// new requests with 503 so a request accepted after the graceful-drain
    /// deadline cannot race the database close.
    shutting_down: AtomicBool,
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
            .map_err(|e| ApiError::bad_request(format!("invalid primary database name: {e}")))?;

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

        let state = Self {
            inner: Arc::new(Inner {
                options,
                object_store,
                cancel: CancellationToken::new(),
                shutting_down: AtomicBool::new(false),
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
            .map_err(|e| ApiError::bad_request(format!("invalid database name: {e}")))?;

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
            OpenMode::Create => AndaDB::create(self.inner.object_store.clone(), config).await?,
            OpenMode::Open => AndaDB::open(self.inner.object_store.clone(), config).await?,
            OpenMode::Connect => AndaDB::connect(self.inner.object_store.clone(), config).await?,
        };

        let metadata = db.metadata();
        let entry = self.new_entry(db);
        {
            let mut dbs = self.inner.databases.write().await;
            dbs.insert(name.to_string(), entry);
        }
        {
            self.inner.registry.write().await.insert(name.to_string());
        }
        self.persist_registry().await;
        Ok(metadata)
    }

    /// Flushes and closes a database, removing it from the registry so it is
    /// not reopened on the next start. The primary database cannot be closed.
    pub async fn close_db(&self, name: &str) -> Result<(), ApiError> {
        if name == self.inner.options.primary_db {
            return Err(ApiError::bad_request(
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
        self.persist_registry().await;

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
        Ok(())
    }

    /// Flushes and closes every open database. Called on server shutdown.
    ///
    /// Marks the state as shutting down first, so the RPC entry points
    /// reject new requests (503) while the databases are being closed —
    /// connection tasks spawned before the graceful-drain deadline may
    /// still be alive when this runs.
    pub async fn shutdown(&self) {
        self.inner.shutting_down.store(true, Ordering::Release);
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
    }

    /// Spawns the background flush task for an open database.
    fn new_entry(&self, db: AndaDB) -> DbEntry {
        let cancel = self.inner.cancel.child_token();
        let flush_task = tokio::spawn({
            let db = db.clone();
            let cancel = cancel.clone();
            let interval = self.inner.options.flush_interval;
            async move { db.auto_flush(cancel, interval).await }
        });
        DbEntry {
            db,
            cancel,
            flush_task,
        }
    }

    /// Persists the registered non-primary database names into the primary
    /// database's extensions. Best-effort: a failure is logged and the
    /// affected database stays usable, but it will not be reopened
    /// automatically until the registry is written again.
    async fn persist_registry(&self) {
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
        }
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
