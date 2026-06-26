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
    sync::Arc,
    time::Duration,
};
use tokio::{sync::RwLock, task::JoinHandle};
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

struct Inner {
    options: ServerOptions,
    object_store: Arc<dyn ObjectStore>,
    cancel: CancellationToken,
    databases: RwLock<BTreeMap<String, DbEntry>>,
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

        let registered: BTreeSet<String> = primary
            .get_extension_as(DB_REGISTRY_KEY)
            .unwrap_or_default();

        let state = Self {
            inner: Arc::new(Inner {
                options,
                object_store,
                cancel: CancellationToken::new(),
                databases: RwLock::new(BTreeMap::new()),
            }),
        };

        {
            let mut dbs = state.inner.databases.write().await;
            let entry = state.new_entry(primary);
            dbs.insert(state.inner.options.primary_db.clone(), entry);

            for name in registered {
                if name == state.inner.options.primary_db {
                    continue;
                }
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

        let mut dbs = self.inner.databases.write().await;
        if let Some(entry) = dbs.get(name) {
            return match mode {
                OpenMode::Create => Err(ApiError::already_exists(format!(
                    "database {name:?} already exists"
                ))),
                OpenMode::Open | OpenMode::Connect => Ok(entry.db.metadata()),
            };
        }

        let config = DBConfig {
            name: name.to_string(),
            description: description.unwrap_or_else(|| name.to_string()),
            storage: self.inner.options.storage.clone(),
            lock: None,
        };
        let db = match mode {
            OpenMode::Create => AndaDB::create(self.inner.object_store.clone(), config).await?,
            OpenMode::Open => AndaDB::open(self.inner.object_store.clone(), config).await?,
            OpenMode::Connect => AndaDB::connect(self.inner.object_store.clone(), config).await?,
        };

        let metadata = db.metadata();
        let entry = self.new_entry(db);
        dbs.insert(name.to_string(), entry);
        self.persist_registry(&dbs).await;
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

        let entry = {
            let mut dbs = self.inner.databases.write().await;
            let entry = dbs
                .remove(name)
                .ok_or_else(|| ApiError::not_found(format!("database {name:?} not found")))?;
            self.persist_registry(&dbs).await;
            entry
        };

        // Cancelling the flush task makes `AndaDB::auto_flush` close the
        // database (flushing all collections) before the task exits.
        entry.cancel.cancel();
        if let Err(err) = entry.flush_task.await {
            log::error!(
                action = "AppState::close_db",
                database = name;
                "flush task failed: {err:?}",
            );
        }
        Ok(())
    }

    /// Flushes and closes every open database. Called on server shutdown.
    pub async fn shutdown(&self) {
        self.inner.cancel.cancel();
        let entries: Vec<DbEntry> = {
            let mut dbs = self.inner.databases.write().await;
            std::mem::take(&mut *dbs).into_values().collect()
        };
        for entry in entries {
            let name = entry.db.name().to_string();
            if let Err(err) = entry.flush_task.await {
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

    /// Persists the set of non-primary database names into the primary
    /// database's extensions. Best-effort: a failure is logged and the
    /// affected database stays usable, but it will not be reopened
    /// automatically until the registry is written again.
    async fn persist_registry(&self, dbs: &BTreeMap<String, DbEntry>) {
        let primary = &self.inner.options.primary_db;
        let names: BTreeSet<&String> = dbs.keys().filter(|name| *name != primary).collect();
        if let Some(entry) = dbs.get(primary)
            && let Err(err) = entry
                .db
                .save_extension_from(DB_REGISTRY_KEY.to_string(), &names)
                .await
        {
            log::error!(
                action = "AppState::persist_registry",
                database = primary;
                "failed to persist database registry: {err:?}",
            );
        }
    }
}
