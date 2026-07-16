use futures::{stream, stream::StreamExt};
use object_store::ObjectStore;
use parking_lot::RwLock;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::{
    fmt::Debug,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

use crate::{
    collection::{Collection, CollectionConfig},
    error::DBError,
    schema::*,
    storage::{Storage, StorageConfig, StorageStats},
    unix_ms,
};

/// Main database structure that manages collections and storage.
///
/// AndaDB provides a high-level interface for creating, opening, and managing
/// collections of documents. It handles persistence through an object store
/// and maintains metadata about the database and its collections.
#[derive(Clone)]
pub struct AndaDB {
    inner: Arc<InnerDB>,
}

struct InnerDB {
    /// Database name
    name: String,
    /// Underlying object storage implementation
    object_store: Arc<dyn ObjectStore>,
    /// Storage layer for database operations
    storage: Storage,
    /// Database metadata protected by a read-write lock
    metadata: RwLock<DBMetadata>,
    /// Serializes the complete database-metadata persistence transaction.
    ///
    /// Collection lifecycle locks are intentionally per name, so operations
    /// on different collections may update `metadata` concurrently.  The
    /// persistence lock must therefore cover both taking the full metadata
    /// snapshot and writing it; otherwise an older snapshot can overwrite a
    /// newer one after both lifecycle operations have returned successfully.
    metadata_flush_lock: Arc<tokio::sync::Mutex<()>>,
    /// Map of collection names to collection instances
    collections: RwLock<BTreeMap<String, Arc<Collection>>>,
    /// Flag indicating whether the database is in read-only mode
    read_only: Arc<AtomicBool>,
    /// Set of collection names being dropped
    dropping_collections: RwLock<BTreeSet<String>>,
    /// Per-collection-name lifecycle locks (see
    /// [`AndaDB::lock_collection_name`]). Creating, opening (from storage),
    /// closing, and deleting a collection of the same name are serialized
    /// through the same entry, so e.g. an open cannot load a second writable
    /// instance while a close is still flushing, and concurrent creators of
    /// the same name observe the winner's registration instead of a storage
    /// conflict. Entries are removed again once no task holds or awaits them.
    collection_locks: parking_lot::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

/// RAII guard for a per-collection-name lifecycle lock.
///
/// Dropping the guard releases the lock and prunes the corresponding
/// [`InnerDB::collection_locks`] entry when no other task holds or awaits it,
/// so the map does not grow with every collection name ever touched.
struct CollectionNameLock {
    inner: Arc<InnerDB>,
    name: String,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for CollectionNameLock {
    fn drop(&mut self) {
        // Release the mutex first so this holder's reference to the
        // `Arc<Mutex>` (kept alive by the owned guard) is gone before the
        // strong-count check below.
        self.guard = None;
        let mut locks = self.inner.collection_locks.lock();
        if let Some(lock) = locks.get(&self.name)
            && Arc::strong_count(lock) == 1
        {
            // Only the map itself references the entry: no holder, no
            // waiter. (Waiters clone the Arc under the map mutex before
            // awaiting, so this check cannot race with a new waiter.)
            locks.remove(&self.name);
        }
    }
}

/// Database configuration parameters.
///
/// Contains settings that define the database's behavior and properties.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DBConfig {
    /// Database name
    pub name: String,

    /// Database description
    pub description: String,

    /// Storage configuration settings
    pub storage: StorageConfig,

    /// Optional opaque bytes as lock for the database
    pub lock: Option<ByteBufB64>,
}

impl Default for DBConfig {
    fn default() -> Self {
        Self {
            name: "anda_db".to_string(),
            description: "Anda DB".to_string(),
            storage: StorageConfig::default(),
            lock: None,
        }
    }
}

/// Database metadata.
///
/// Contains the database configuration and a set of collection names
/// that belong to this database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DBMetadata {
    /// Database configuration
    pub config: DBConfig,

    /// Set of collection names in this database
    pub collections: BTreeSet<String>,

    /// User-defined lightweight extension data persisted with database metadata.
    #[serde(default)]
    pub extensions: BTreeMap<String, FieldValue>,
}

impl Debug for AndaDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AndaDB({})", self.inner.name)
    }
}

impl AndaDB {
    /// Path where database metadata is stored
    const METADATA_PATH: &'static str = "db_meta.cbor";

    /// Returns aggregated storage I/O statistics across the database and all
    /// currently open collections.
    pub fn stats(&self) -> StorageStats {
        let mut stats = self.inner.storage.stats();
        for collection in self.inner.collections.read().values() {
            stats.merge(&collection.storage_stats());
        }
        stats
    }

    /// Creates a new database with the given configuration.
    ///
    /// This method initializes a new database with the specified configuration
    /// and object store. It validates the database name, connects to storage,
    /// and creates the initial metadata.
    ///
    /// # Arguments
    /// * `object_store` - The object store implementation to use for persistence
    /// * `config` - The database configuration
    ///
    /// # Returns
    /// A Result containing either the new AndaDB instance or an error
    pub async fn create(
        object_store: Arc<dyn ObjectStore>,
        config: DBConfig,
    ) -> Result<Self, DBError> {
        validate_field_name(config.name.as_str())?;

        let storage = Storage::connect(
            config.name.clone(),
            object_store.clone(),
            config.storage.clone(),
        )
        .await?;

        let metadata = DBMetadata {
            config,
            collections: BTreeSet::new(),
            extensions: BTreeMap::new(),
        };

        match storage.create(Self::METADATA_PATH, &metadata).await {
            Ok(_) => {
                // DB created successfully, and store storage metadata
                storage.store_metadata(0, unix_ms()).await?;
            }
            Err(err) => return Err(err),
        }

        Ok(Self {
            inner: Arc::new(InnerDB {
                name: metadata.config.name.clone(),
                object_store,
                storage,
                metadata: RwLock::new(metadata),
                metadata_flush_lock: Arc::new(tokio::sync::Mutex::new(())),
                collections: RwLock::new(BTreeMap::new()),
                read_only: Arc::new(AtomicBool::new(false)),
                dropping_collections: RwLock::new(BTreeSet::new()),
                collection_locks: parking_lot::Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Connects to an existing database or creates a new one if it doesn't exist.
    ///
    /// This method attempts to connect to an existing database with the given
    /// configuration. If the database doesn't exist, it creates a new one.
    ///
    /// # Arguments
    /// * `object_store` - The object store implementation to use for persistence
    /// * `config` - The database configuration
    ///
    /// # Returns
    /// A Result containing either the AndaDB instance or an error
    pub async fn connect(
        object_store: Arc<dyn ObjectStore>,
        config: DBConfig,
    ) -> Result<Self, DBError> {
        match Self::open(object_store.clone(), config.clone()).await {
            Ok(db) => Ok(db),
            Err(DBError::NotFound { .. }) => Self::create(object_store, config).await,
            Err(err) => Err(err),
        }
    }

    /// Connects to an existing database with the given configuration.
    /// This method fails if the database doesn't exist.
    pub async fn open(
        object_store: Arc<dyn ObjectStore>,
        config: DBConfig,
    ) -> Result<Self, DBError> {
        validate_field_name(config.name.as_str())?;

        let storage = Storage::connect(
            config.name.clone(),
            object_store.clone(),
            config.storage.clone(),
        )
        .await?;

        match storage.fetch::<DBMetadata>(Self::METADATA_PATH).await {
            Ok((metadata, _)) => {
                let set_lock = match (&metadata.config.lock, config.lock) {
                    (None, Some(lock)) => Some(lock),
                    (Some(existing_lock), lock) => {
                        if lock.as_ref() != Some(existing_lock) {
                            return Err(DBError::Storage {
                                name: config.name.clone(),
                                source: "Database lock mismatch".into(),
                            });
                        }
                        None
                    }
                    _ => None,
                };

                let this = Self {
                    inner: Arc::new(InnerDB {
                        name: metadata.config.name.clone(),
                        object_store,
                        storage,
                        metadata: RwLock::new(metadata),
                        metadata_flush_lock: Arc::new(tokio::sync::Mutex::new(())),
                        collections: RwLock::new(BTreeMap::new()),
                        read_only: Arc::new(AtomicBool::new(false)),
                        dropping_collections: RwLock::new(BTreeSet::new()),
                        collection_locks: parking_lot::Mutex::new(HashMap::new()),
                    }),
                };

                if let Some(lock) = set_lock {
                    this.set_lock(lock).await?;
                }

                Ok(this)
            }
            Err(err) => Err(err),
        }
    }

    /// Returns the name of the database.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Returns a clone of the database metadata.
    pub fn metadata(&self) -> DBMetadata {
        self.inner.metadata.read().clone()
    }

    /// Returns whether the database currently rejects mutations.
    ///
    /// This is a point-in-time observation intended for API boundaries that
    /// want to classify a read-only request before entering the storage
    /// engine. Callers must still treat a later engine error conservatively,
    /// because the mode can change concurrently after this check.
    pub fn is_read_only(&self) -> bool {
        self.inner.read_only.load(Ordering::Acquire)
    }

    pub(crate) fn read_only_flag(&self) -> Arc<AtomicBool> {
        self.inner.read_only.clone()
    }

    /// Sets the database to read-only mode.
    ///
    /// When in read-only mode, operations that modify the database will fail.
    /// This setting is propagated to all collections in the database.
    ///
    /// # Arguments
    /// * `read_only` - Whether to enable read-only mode
    pub fn set_read_only(&self, read_only: bool) {
        self.inner.read_only.store(read_only, Ordering::Release);
        log::warn!(
            action = "AndaDB::set_read_only",
            database = self.inner.name;
            "Database is set to read-only: {read_only}"
        );

        for collection in self.inner.collections.read().values() {
            collection.set_read_only(read_only);
        }
    }

    /// Closes the database, ensuring all data is flushed to storage.
    ///
    /// This method sets the database to read-only mode, closes all collections,
    /// and flushes any pending changes to storage.
    ///
    /// # Returns
    /// A Result indicating success or an error
    pub async fn close(&self) -> Result<(), DBError> {
        self.set_read_only(true);
        let collections = self
            .inner
            .collections
            .read()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let results: Vec<Result<(), DBError>> = stream::iter(collections)
            .map(|collection| async move { collection.close().await })
            .buffer_unordered(8) // 限制最多 8 个并发
            .collect()
            .await;
        // Log per-collection failures but continue closing the database to flush
        // metadata for the remaining successful collections, then surface the
        // first error so callers can react.
        let mut first_err: Option<DBError> = None;
        for r in results {
            if let Err(err) = r {
                log::error!(
                    action = "AndaDB::close",
                    database = self.inner.name;
                    "Collection close failed: {err:?}",
                );
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
        }

        let start = Instant::now();
        match self.flush_metadata(unix_ms()).await {
            Ok(_) => {
                let elapsed = start.elapsed();
                log::warn!(
                    action = "AndaDB::close",
                    database = self.inner.name,
                    elapsed = elapsed.as_millis();
                    "Database closed successfully in {elapsed:?}",
                );
            }
            Err(err) => {
                let elapsed = start.elapsed();
                log::error!(
                    action = "AndaDB::close",
                    database = self.inner.name,
                    elapsed = elapsed.as_millis();
                    "Failed to close database: {err:?}",
                );
                return Err(err);
            }
        }
        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(())
    }

    /// Flushes the database, ensuring all data is written to storage.
    pub async fn flush(&self) -> Result<(), DBError> {
        let collections = self
            .inner
            .collections
            .read()
            .values()
            .cloned()
            .collect::<Vec<_>>();

        let results: Vec<Result<bool, DBError>> = stream::iter(collections)
            .map(|collection| async move { collection.flush(unix_ms()).await })
            .buffer_unordered(8) // 限制最多 8 个并发
            .collect()
            .await;

        let mut first_err: Option<DBError> = None;
        for r in results {
            if let Err(err) = r {
                log::error!(
                    action = "AndaDB::flush",
                    database = self.inner.name;
                    "Collection flush failed: {err:?}",
                );
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
        }

        self.flush_metadata(unix_ms()).await?;
        if let Some(err) = first_err {
            return Err(err);
        }
        Ok(())
    }

    /// Automatically flushes the database at regular intervals.
    ///
    /// This method runs in a loop, waiting for the specified interval
    /// before flushing the database. When the cancellation token is triggered,
    /// the loop will exit and the database will be closed.
    ///
    /// # Arguments
    /// * `cancel_token` - A cancellation token to stop the loop
    /// * `interval` - The time interval between flushes
    ///
    pub async fn auto_flush(&self, cancel_token: CancellationToken, interval: Duration) {
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    let _ = self.close().await;
                    return;
                }
                _ = tokio::time::sleep(interval) => {}
            };

            let start = Instant::now();
            match self.flush().await {
                Ok(_) => {
                    let elapsed = start.elapsed();
                    log::warn!(
                        action = "AndaDB::auto_flush",
                        database = self.inner.name,
                        elapsed = elapsed.as_millis();
                        "Database flushed successfully in {elapsed:?}",
                    );
                }
                Err(err) => {
                    let elapsed = start.elapsed();
                    log::error!(
                        action = "AndaDB::auto_flush",
                        database = self.inner.name,
                        elapsed = elapsed.as_millis();
                        "Failed to flush database: {err:?}",
                    );
                }
            }
        }
    }

    /// Acquires the lifecycle lock for a collection name.
    ///
    /// Collection creation, the slow (load-from-storage) path of opening,
    /// closing, and deletion of the same name all run under this lock, so
    /// their storage-level effects cannot interleave. The lock is a plain
    /// (non-reentrant) tokio mutex: a creation/open callback `f` that
    /// creates, opens, closes, or deletes **the same collection name** it
    /// runs for would wait on itself forever and is not supported. Nested
    /// operations on *different* collection names are fine.
    async fn lock_collection_name(&self, name: &str) -> CollectionNameLock {
        let lock = {
            let mut locks = self.inner.collection_locks.lock();
            locks.entry(name.to_string()).or_default().clone()
        };
        let guard = lock.lock_owned().await;
        CollectionNameLock {
            inner: self.inner.clone(),
            name: name.to_string(),
            guard: Some(guard),
        }
    }

    /// Creates a new collection in the database.
    ///
    /// This method creates a new collection with the given schema and configuration.
    /// It also executes the provided function on the collection before finalizing creation.
    ///
    /// # Concurrency
    ///
    /// Creation runs under a per-name lifecycle lock (see
    /// [`AndaDB::lock_collection_name`]); `f` may create or open **other**
    /// collections, but must not create, open, close, or delete the same
    /// collection name it runs for — the lock is not reentrant and such a
    /// call would hang forever.
    ///
    /// # Arguments
    /// * `schema` - The schema defining the structure of documents in the collection
    /// * `config` - The collection configuration
    /// * `f` - A function to execute on the collection during creation
    ///
    /// # Returns
    /// A Result containing either the new Collection or an error
    pub async fn create_collection<F>(
        &self,
        schema: Schema,
        config: CollectionConfig,
        f: F,
    ) -> Result<Arc<Collection>, DBError>
    where
        F: AsyncFnOnce(&mut Collection) -> Result<(), DBError>,
    {
        if self.inner.read_only.load(Ordering::Relaxed) {
            return Err(DBError::Generic {
                name: self.inner.name.clone(),
                source: "database is read-only".into(),
            });
        }

        {
            if self.inner.collections.read().contains_key(&config.name) {
                return Err(DBError::AlreadyExists {
                    name: config.name,
                    path: self.inner.name.clone(),
                    source: "collection already exists".into(),
                    _id: 0,
                });
            }
        }

        {
            if self
                .inner
                .dropping_collections
                .read()
                .contains(&config.name)
            {
                return Err(DBError::AlreadyExists {
                    name: config.name,
                    path: self.inner.name.clone(),
                    source: "collection is being dropped".to_string().into(),
                    _id: 0,
                });
            }
        }

        // Serialize with other lifecycle operations on the same name, so a
        // concurrent creator is observed through its registration rather
        // than a storage conflict, and a concurrent delete/close cannot
        // interleave with the files written here.
        let _name_guard = self.lock_collection_name(&config.name).await;
        // Re-check the states that may have changed while waiting for the
        // lock: a concurrent delete may have started (and not finished), or
        // a concurrent creator may have registered the name.
        if self
            .inner
            .dropping_collections
            .read()
            .contains(&config.name)
        {
            return Err(DBError::AlreadyExists {
                name: config.name,
                path: self.inner.name.clone(),
                source: "collection is being dropped".to_string().into(),
                _id: 0,
            });
        }
        if self.inner.collections.read().contains_key(&config.name) {
            return Err(DBError::AlreadyExists {
                name: config.name,
                path: self.inner.name.clone(),
                source: "collection already exists".into(),
                _id: 0,
            });
        }
        // self.metadata.collections will check it exists again in Collection::create
        let collection = Collection::create(self.clone(), schema, config).await?;
        self.register_created_collection(collection, f).await
    }

    /// Runs the creation callback on a freshly created collection, registers
    /// it in the database, and persists collection and database metadata.
    async fn register_created_collection<F>(
        &self,
        mut collection: Collection,
        f: F,
    ) -> Result<Arc<Collection>, DBError>
    where
        F: AsyncFnOnce(&mut Collection) -> Result<(), DBError>,
    {
        let start = Instant::now();
        if let Err(err) = f(&mut collection).await {
            // The collection is not registered in the database metadata yet;
            // delete the files written so far so the name can be created again.
            let _ = collection.drop_data().await;
            return Err(err);
        }
        let collection = Arc::new(collection);
        {
            let mut collections = self.inner.collections.write();
            collections.insert(collection.name().to_string(), collection.clone());
            self.inner
                .metadata
                .write()
                .collections
                .insert(collection.name().to_string());
        }

        let now = unix_ms();
        collection.flush(now).await?;
        self.flush_metadata(now).await?;
        let elapsed = start.elapsed();
        log::warn!(
            action = "AndaDB::create_collection",
            database = self.inner.name,
            collection = collection.name(),
            elapsed = elapsed.as_millis();
            "Create a collection successfully in {elapsed:?}",
        );
        Ok(collection)
    }

    /// Opens an existing collection or creates a new one if it doesn't exist.
    ///
    /// This method attempts to open an existing collection with the given name.
    /// If the collection doesn't exist, it creates a new one with the provided
    /// schema and configuration.
    ///
    /// When opening an existing collection, the method compares the provided
    /// schema's version with the stored schema's version. If the provided schema
    /// has a higher version, the collection's schema will be upgraded automatically
    /// before executing the callback `f`.
    ///
    /// # Concurrency
    ///
    /// Creation and the load-from-storage open path run under a per-name
    /// lifecycle lock (see [`AndaDB::lock_collection_name`]); `f` may create
    /// or open **other** collections, but must not create, open, close, or
    /// delete the same collection name it runs for — the lock is not
    /// reentrant and such a call would hang forever.
    ///
    /// # Arguments
    /// * `schema` - The schema to use for creating or upgrading the collection
    /// * `config` - The collection configuration
    /// * `f` - A function to execute on the collection during opening/creation
    ///
    /// # Returns
    /// A Result containing either the Collection or an error
    pub async fn open_or_create_collection<F>(
        &self,
        schema: Schema,
        config: CollectionConfig,
        f: F,
    ) -> Result<Arc<Collection>, DBError>
    where
        F: AsyncFnOnce(&mut Collection) -> Result<(), DBError>,
    {
        if self.inner.read_only.load(Ordering::Relaxed) {
            return Err(DBError::Generic {
                name: self.inner.name.clone(),
                source: "database is read-only".into(),
            });
        }

        // A delete tombstone always wins over a cached handle: a cancelled
        // delete deliberately keeps both until a retry finishes the prefix
        // removal. Returning the handle first would resurrect an object that
        // has already entered its irreversible deleting state.
        {
            if self
                .inner
                .dropping_collections
                .read()
                .contains(&config.name)
            {
                return Err(DBError::AlreadyExists {
                    name: config.name,
                    path: self.inner.name.clone(),
                    source: "collection is being dropped".to_string().into(),
                    _id: 0,
                });
            }
        }

        {
            if let Some(collection) = self.inner.collections.read().get(&config.name)
                && collection.is_active_handle()
            {
                return Ok(collection.clone());
            }
        }

        if !self
            .inner
            .metadata
            .read()
            .collections
            .contains(&config.name)
        {
            // Serialize with other lifecycle operations on this name: when a
            // concurrent `open_or_create_collection` of the same name wins
            // the race, we observe its registration after acquiring the lock
            // and fall through to the open path instead of failing with
            // `AlreadyExists`.
            let name_guard = self.lock_collection_name(&config.name).await;
            // A delete of this name may have started while we were waiting
            // for the lock; creating now would resurrect it mid-drop.
            if self
                .inner
                .dropping_collections
                .read()
                .contains(&config.name)
            {
                return Err(DBError::AlreadyExists {
                    name: config.name,
                    path: self.inner.name.clone(),
                    source: "collection is being dropped".to_string().into(),
                    _id: 0,
                });
            }
            let exists_now = self.inner.collections.read().contains_key(&config.name)
                || self
                    .inner
                    .metadata
                    .read()
                    .collections
                    .contains(&config.name);
            if !exists_now {
                match Collection::create(self.clone(), schema.clone(), config.clone()).await {
                    Ok(collection) => {
                        return self.register_created_collection(collection, f).await;
                    }
                    Err(err @ DBError::AlreadyExists { .. }) => {
                        // Lost to a writer outside this process (or leftover
                        // files from a crashed create): fall back to opening.
                        // Release the name lock first — the open path below
                        // re-acquires it for the load from storage.
                        drop(name_guard);
                        return match self
                            .open_collection_with_schema(config.name, Some(schema), f)
                            .await
                        {
                            // Not a registered collection (e.g. leftover files
                            // from a crashed create): surface the original
                            // AlreadyExists so the caller can clean up with
                            // `delete_collection`.
                            Err(DBError::NotFound { .. }) => Err(err),
                            other => other,
                        };
                    }
                    Err(err) => return Err(err),
                }
            }
        }

        self.open_collection_with_schema(config.name, Some(schema), f)
            .await
    }

    /// Opens an existing collection.
    ///
    /// This method attempts to open an existing collection with the given name.
    /// It fails if the collection doesn't exist.
    ///
    /// # Concurrency
    ///
    /// Loading from storage runs under a per-name lifecycle lock (see
    /// [`AndaDB::lock_collection_name`]); `f` may create or open **other**
    /// collections, but must not create, open, close, or delete the same
    /// collection name it runs for — the lock is not reentrant and such a
    /// call would hang forever.
    ///
    /// # Arguments
    /// * `name` - The name of the collection to open
    /// * `f` - A function to execute on the collection during opening
    ///
    /// # Returns
    /// A Result containing either the Collection or an error
    pub async fn open_collection<F>(&self, name: String, f: F) -> Result<Arc<Collection>, DBError>
    where
        F: AsyncFnOnce(&mut Collection) -> Result<(), DBError>,
    {
        self.open_collection_with_schema(name, None, f).await
    }

    /// Opens an existing collection, upgrading its schema if the provided schema
    /// has a higher version than the stored one.
    async fn open_collection_with_schema<F>(
        &self,
        name: String,
        schema: Option<Schema>,
        f: F,
    ) -> Result<Arc<Collection>, DBError>
    where
        F: AsyncFnOnce(&mut Collection) -> Result<(), DBError>,
    {
        {
            if self.inner.dropping_collections.read().contains(&name) {
                return Err(DBError::AlreadyExists {
                    name: name.clone(),
                    path: self.inner.name.clone(),
                    source: "collection is being dropped".to_string().into(),
                    _id: 0,
                });
            }
        }

        {
            if let Some(collection) = self.inner.collections.read().get(&name)
                && collection.is_active_handle()
            {
                return Ok(collection.clone());
            }
        }

        {
            if !self.inner.metadata.read().collections.contains(&name) {
                return Err(DBError::NotFound {
                    name,
                    path: self.inner.name.clone(),
                    source: "collection not found".into(),
                    _id: 0,
                });
            }
        }

        // Load from storage under the per-name lifecycle lock: a concurrent
        // `close_collection` may still be flushing this collection's state
        // (its index files use overwrite semantics), and loading before that
        // finishes would create a second writable instance whose writes the
        // close would clobber. The lock also serializes with create/delete.
        let _name_guard = self.lock_collection_name(&name).await;
        // Re-check the fast paths after acquiring the lock: a concurrent
        // open may have registered the collection while we waited, or a
        // delete may have started/completed.
        {
            if self.inner.dropping_collections.read().contains(&name) {
                return Err(DBError::AlreadyExists {
                    name: name.clone(),
                    path: self.inner.name.clone(),
                    source: "collection is being dropped".to_string().into(),
                    _id: 0,
                });
            }
        }

        // A cancelled close deliberately leaves its retiring handle in the
        // registry. Finish its drain/flush under the same per-name lock before
        // loading a fresh generation, then remove only that exact Arc.
        let retiring = { self.inner.collections.read().get(&name).cloned() };
        if let Some(collection) = retiring {
            if collection.is_active_handle() {
                return Ok(collection);
            }
            if collection.is_poisoned() {
                // A poisoned handle is treated like a crashed process: its
                // in-memory state must not be flushed. Wait for in-flight
                // operations to drain, drop the handle, and let the fresh
                // load below run the reopen recovery path (mutation-intent
                // replay plus the repair scan).
                let _drain = collection.drain_operations().await;
            } else {
                collection.close().await?;
            }
            let mut collections = self.inner.collections.write();
            if collections
                .get(&name)
                .is_some_and(|current| Arc::ptr_eq(current, &collection))
            {
                collections.remove(&name);
            }
        }
        {
            if !self.inner.metadata.read().collections.contains(&name) {
                return Err(DBError::NotFound {
                    name,
                    path: self.inner.name.clone(),
                    source: "collection not found".into(),
                    _id: 0,
                });
            }
        }

        let collection = Collection::open(self.clone(), name, schema, f).await?;
        let collection = Arc::new(collection);
        {
            // A concurrent open of the same collection may have won the race
            // while we were loading. Keep the registered instance as the single
            // source of truth: two live instances would maintain divergent
            // in-memory state (doc id bitmap, indexes) over the same storage.
            let mut collections = self.inner.collections.write();
            if let Some(existing) = collections.get(collection.name()) {
                return Ok(existing.clone());
            }
            // Re-validate against a concurrent `delete_collection` that
            // completed while `Collection::open` was loading: registering now
            // would resurrect a "zombie" handle whose storage prefix has been
            // (or is being) deleted, and whose future flushes would write
            // objects back under the deleted prefix.
            if self
                .inner
                .dropping_collections
                .read()
                .contains(collection.name())
                || !self
                    .inner
                    .metadata
                    .read()
                    .collections
                    .contains(collection.name())
            {
                return Err(DBError::NotFound {
                    name: collection.name().to_string(),
                    path: self.inner.name.clone(),
                    source: "collection was deleted while being opened".into(),
                    _id: 0,
                });
            }
            collections.insert(collection.name().to_string(), collection.clone());
        }
        let now = unix_ms();
        // A read-only open may replay recovery state in memory for correct
        // reads, but must not persist it or let the callback mutate storage.
        if !self.inner.read_only.load(Ordering::Acquire) {
            collection.flush(now).await?;
        }
        Ok(collection)
    }

    /// Closes an open collection and removes it from the database's in-memory
    /// registry, so a subsequent open reloads it from storage.
    ///
    /// Use this instead of calling [`Collection::close`] directly: a closed
    /// collection that stays registered would keep being returned by
    /// [`AndaDB::open_collection`] / [`AndaDB::open_or_create_collection`] as
    /// a permanently read-only handle.
    ///
    /// The whole operation holds the per-name lifecycle lock (see
    /// [`AndaDB::lock_collection_name`]), so a concurrent open of the same
    /// name waits for the close to finish instead of loading a second
    /// writable instance from storage while the closing flush is still
    /// writing index files. The collection also closes mutation admission and
    /// drains operations that already entered before flushing. Any external
    /// `Arc<Collection>` retained by the caller is permanently retired and
    /// cannot be made writable again with `set_read_only(false)`.
    ///
    /// The handle remains registered until its close succeeds. This makes the
    /// transition cancellation-safe: aborting the close future cannot expose
    /// an empty registry slot where a second instance could open and consume
    /// the first instance's mutation journal. A retry (or an open) finishes
    /// retiring the same handle before loading a fresh generation.
    ///
    /// Returns `Ok(())` when the collection is not currently open.
    pub async fn close_collection(&self, name: &str) -> Result<(), DBError> {
        let _name_guard = self.lock_collection_name(name).await;
        let collection = { self.inner.collections.read().get(name).cloned() };
        if let Some(collection) = collection {
            collection.close().await?;
            let mut collections = self.inner.collections.write();
            if collections
                .get(name)
                .is_some_and(|current| Arc::ptr_eq(current, &collection))
            {
                collections.remove(name);
            }
        }
        Ok(())
    }

    /// Deletes a collection's metadata, cached instance, and storage prefix.
    ///
    /// The deletion first persists database metadata so reopening the database
    /// does not try to load the removed collection. Object deletion is then
    /// performed under the collection prefix.
    ///
    /// # Concurrency
    ///
    /// Deletion holds the per-name lifecycle lock (see
    /// [`AndaDB::lock_collection_name`]) for its whole duration, so it cannot
    /// interleave with a create, storage-loading open, or close of the same
    /// name. An open handle is moved to its irreversible deleting state before
    /// the prefix is listed: new mutations are rejected and operations that
    /// already passed admission are drained, so an old `Arc<Collection>`
    /// cannot recreate residual objects after deletion returns.
    pub async fn delete_collection(&self, name: &str) -> Result<(), DBError> {
        if self.inner.read_only.load(Ordering::Relaxed) {
            return Err(DBError::Generic {
                name: self.inner.name.clone(),
                source: "database is read-only".into(),
            });
        }

        // The name is used to build the storage prefix below.
        validate_field_name(name)?;

        let _name_guard = self.lock_collection_name(name).await;

        // Publish the tombstone before touching durable metadata. Open/create
        // fast paths consult it before the registry, and a cancelled future
        // leaves it in place for a later retry to take over.
        self.inner
            .dropping_collections
            .write()
            .insert(name.to_string());
        let collection = { self.inner.collections.read().get(name).cloned() };
        if let Some(collection) = &collection {
            collection.begin_delete()?;
        }

        // Always persist the current no-name snapshot, including on a retry
        // where an earlier cancelled call already removed it from memory but
        // may not have completed the object-store PUT.
        self.inner.metadata.write().collections.remove(name);
        self.flush_metadata(unix_ms()).await?;

        // Keep a registered handle reachable until its drain and prefix drop
        // succeed. If this await is cancelled, both handle and tombstone stay
        // available and no fresh writer can open over the same prefix.
        let drop_result = match &collection {
            Some(collection) => collection.drop_data().await,
            None => {
                let base_path = object_store::path::Path::from(self.name()).join(name);
                let storage_config = { self.inner.metadata.read().config.storage.clone() };
                match Storage::connect(base_path.to_string(), self.object_store(), storage_config)
                    .await
                {
                    Ok(storage) => storage.drop_data().await,
                    Err(err) => Err(err),
                }
            }
        };

        if let Err(err) = drop_result {
            log::error!(
                action = "AndaDB::delete_collection",
                database = self.inner.name,
                collection = name;
                "Failed to drop collection data: {err:?}",
            );
            return Err(err);
        }

        if let Some(collection) = collection {
            let mut collections = self.inner.collections.write();
            if collections
                .get(name)
                .is_some_and(|current| Arc::ptr_eq(current, &collection))
            {
                collections.remove(name);
            }
        }
        self.inner.dropping_collections.write().remove(name);
        Ok(())
    }

    async fn set_lock(&self, lock: ByteBufB64) -> Result<(), DBError> {
        {
            self.inner.metadata.write().config.lock = Some(lock);
        }
        self.flush_metadata(unix_ms()).await
    }

    /// Flushes database metadata to storage.
    ///
    /// This method writes the current database metadata to storage and
    /// updates the storage metadata with the current timestamp.
    ///
    /// # Arguments
    /// * `now_ms` - The current timestamp in milliseconds
    ///
    /// # Returns
    /// A Result indicating success or an error
    pub async fn flush_metadata(&self, now_ms: u64) -> Result<(), DBError> {
        // Keep the lock across snapshot creation and both durable writes.  In
        // particular, do not clone `metadata` before awaiting this guard: an
        // older waiter must observe changes made while it was queued instead
        // of writing its stale clone after the newer operation.
        let _flush_guard = self.inner.metadata_flush_lock.clone().lock_owned().await;
        let metadata = self.metadata();

        self.inner
            .storage
            .put(Self::METADATA_PATH, &metadata, None)
            .await?;
        self.inner.storage.store_metadata(0, now_ms).await?;
        Ok(())
    }

    /// Gets the value of a user-defined extension key.
    pub fn get_extension(&self, key: &str) -> Option<FieldValue> {
        self.inner.metadata.read().extensions.get(key).cloned()
    }

    /// Gets the value of a user-defined extension key and deserializes it to the specified type.
    pub fn get_extension_as<T>(&self, key: &str) -> Option<T>
    where
        T: DeserializeOwned,
    {
        self.get_extension(key).and_then(|v| v.deserialized().ok())
    }

    /// Sets a user-defined extension key-value pair.
    /// The change is persisted on the next `flush()` or `flush_metadata()`.
    /// The extensions should not be large, as they are stored in the same object as database metadata which size is expected to be small (<= 1MB) and loaded frequently.
    /// Values that fail [`FieldValue::validate_complexity`] are dropped with a warning.
    pub fn set_extension(&self, key: String, value: FieldValue) {
        if let Err(err) = value.validate_complexity() {
            log::warn!(
                action = "AndaDB::set_extension",
                database = self.inner.name,
                key = key;
                "Dropping extension value that exceeds complexity limits: {err:?}",
            );
            return;
        }
        self.inner.metadata.write().extensions.insert(key, value);
    }

    /// Sets a user-defined extension key-value pair by serializing the value from a generic type.
    /// The change is persisted on the next `flush()` or `flush_metadata()`.
    pub fn set_extension_from<T>(&self, key: String, value: T)
    where
        T: Serialize,
    {
        if let Ok(value) = FieldValue::serialized(&value, None) {
            self.set_extension(key, value);
        }
    }

    /// Updates a user-defined extension using a functional approach.
    ///
    /// This method retrieves the current value for the given key (if any) and computes
    /// a new value using the provided function. If the function returns `None`,
    /// no change is made to the extensions.
    ///
    /// # Arguments
    /// * `key` - The name of the extension key to update.
    /// * `f` - An update function that takes `Option<&FieldValue>` and returns `Option<FieldValue>`.
    ///
    /// # Returns
    /// Returns the previous value `Option<FieldValue>` if a change was made.
    ///
    /// # Notes
    /// The change is persisted to storage on the next `flush()` call.
    pub fn set_extension_with<F>(&self, key: String, f: F) -> Option<FieldValue>
    where
        F: FnOnce(Option<&FieldValue>) -> Option<FieldValue>,
    {
        let mut meta = self.inner.metadata.write();
        let old_value = meta.extensions.get(&key);
        let new_value = f(old_value);
        if let Some(value) = new_value {
            if let Err(err) = value.validate_complexity() {
                log::warn!(
                    action = "AndaDB::set_extension_with",
                    database = self.inner.name,
                    key = key;
                    "Dropping extension value that exceeds complexity limits: {err:?}",
                );
                return None;
            }
            meta.extensions.insert(key, value)
        } else {
            None
        }
    }

    /// Updates a user-defined extension by deserializing the current value, applying a function, and serializing the new value.
    pub fn set_extension_from_with<F, T>(&self, key: String, f: F) -> Option<T>
    where
        F: FnOnce(Option<T>) -> Option<T>,
        T: Serialize + DeserializeOwned,
    {
        let mut meta = self.inner.metadata.write();
        let old_value = meta.extensions.get(&key);
        let new_value = f(old_value.and_then(|v| v.clone().deserialized().ok()));
        if let Some(value) = new_value
            && let Ok(value) = FieldValue::serialized(&value, None)
        {
            if let Err(err) = value.validate_complexity() {
                log::warn!(
                    action = "AndaDB::set_extension_from_with",
                    database = self.inner.name,
                    key = key;
                    "Dropping extension value that exceeds complexity limits: {err:?}",
                );
                return None;
            }
            let old = meta.extensions.insert(key, value);
            return old.and_then(|v| v.deserialized().ok());
        }
        None
    }

    /// Sets a user-defined extension key-value pair and immediately persists the change.
    /// The extensions should not be large, as they are stored in the same object as database metadata which size is expected to be small (<= 1MB) and loaded frequently.
    pub async fn save_extension(&self, key: String, value: FieldValue) -> Result<(), DBError> {
        if self.inner.read_only.load(Ordering::Relaxed) {
            return Err(DBError::Generic {
                name: self.inner.name.clone(),
                source: "database is read-only".into(),
            });
        }
        value.validate_complexity()?;

        {
            self.inner.metadata.write().extensions.insert(key, value);
        }
        self.flush_metadata(unix_ms()).await
    }

    /// Sets a user-defined extension key-value pair by serializing the value from a generic type and immediately persists the change.
    pub async fn save_extension_from<T>(&self, key: String, value: &T) -> Result<(), DBError>
    where
        T: Serialize,
    {
        let field_value = FieldValue::serialized(value, None)?;
        self.save_extension(key, field_value).await
    }

    /// Removes a user-defined extension key and immediately persists the change.
    /// Returns the previous value if the key existed.
    pub async fn remove_extension(&self, key: &str) -> Result<Option<FieldValue>, DBError> {
        if self.inner.read_only.load(Ordering::Relaxed) {
            return Err(DBError::Generic {
                name: self.inner.name.clone(),
                source: "database is read-only".into(),
            });
        }

        let old = { self.inner.metadata.write().extensions.remove(key) };
        if old.is_some() {
            self.flush_metadata(unix_ms()).await?;
        }
        Ok(old)
    }

    /// Provides access to the entire extensions map for advanced use cases.
    pub fn extensions_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&BTreeMap<String, FieldValue>) -> R,
    {
        f(&self.inner.metadata.read().extensions)
    }

    /// Returns a clone of the object store.
    ///
    /// This method is used internally by collections to access the object store.
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.inner.object_store.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ByteBufB64, Fe, FieldValue, Ft, Schema};
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
        memory::InMemory, path::Path,
    };
    use std::{
        fmt,
        sync::{
            Mutex as StdMutex,
            atomic::{AtomicBool as TestAtomicBool, Ordering as TestOrdering},
        },
        task::Poll,
    };

    /// An object store that holds the first armed database-metadata PUT before
    /// it reaches the backing store. A second PUT is allowed through, so an
    /// implementation without a metadata serialization gate deterministically
    /// produces the harmful order "new snapshot, then old snapshot".
    #[derive(Debug)]
    struct ReverseMetadataPutStore {
        inner: Arc<InMemory>,
        metadata_path: Path,
        armed: TestAtomicBool,
        release_first: tokio::sync::watch::Receiver<bool>,
        snapshots: StdMutex<Vec<BTreeSet<String>>>,
    }

    impl ReverseMetadataPutStore {
        fn new(metadata_path: Path, release_first: tokio::sync::watch::Receiver<bool>) -> Self {
            Self {
                inner: Arc::new(InMemory::new()),
                metadata_path,
                armed: TestAtomicBool::new(false),
                release_first,
                snapshots: StdMutex::new(Vec::new()),
            }
        }

        fn arm(&self) {
            assert!(self.snapshots.lock().unwrap().is_empty());
            self.armed.store(true, TestOrdering::Release);
        }

        fn snapshots(&self) -> Vec<BTreeSet<String>> {
            self.snapshots.lock().unwrap().clone()
        }
    }

    impl fmt::Display for ReverseMetadataPutStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("ReverseMetadataPutStore")
        }
    }

    #[async_trait]
    impl ObjectStore for ReverseMetadataPutStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            if self.armed.load(TestOrdering::Acquire) && location == &self.metadata_path {
                let bytes: bytes::Bytes = payload.clone().into();
                let metadata: DBMetadata =
                    cbor2::from_reader(&bytes[..]).map_err(|err| object_store::Error::Generic {
                        store: "reverse_metadata_put",
                        source: err.into(),
                    })?;
                let put_index = {
                    let mut snapshots = self.snapshots.lock().unwrap();
                    let put_index = snapshots.len();
                    snapshots.push(metadata.collections);
                    put_index
                };

                if put_index == 0 {
                    let mut release = self.release_first.clone();
                    while !*release.borrow() {
                        release
                            .changed()
                            .await
                            .map_err(|_| object_store::Error::Generic {
                                store: "reverse_metadata_put",
                                source: "release sender dropped".into(),
                            })?;
                    }
                }
            }

            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<Path>>,
        ) -> BoxStream<'static, ObjectStoreResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn test_database_creation() {
        let object_store = Arc::new(InMemory::new());
        let config = DBConfig::default();

        let db = AndaDB::create(object_store, config).await.unwrap();
        assert_eq!(db.name(), "anda_db");
        assert!(db.metadata().collections.is_empty());
    }

    #[tokio::test]
    async fn test_database_connection() {
        let object_store = Arc::new(InMemory::new());
        let config = DBConfig {
            name: "test_db".to_string(),
            description: "Test Database".to_string(),
            storage: StorageConfig::default(),
            lock: None,
        };

        // First create the database
        {
            let _db = AndaDB::create(object_store.clone(), config.clone())
                .await
                .unwrap();
        }

        // Then connect to it
        let db = AndaDB::connect(object_store, config).await.unwrap();
        assert_eq!(db.name(), "test_db");
    }

    #[tokio::test]
    async fn test_database_open() {
        let object_store = Arc::new(InMemory::new());
        let config = DBConfig {
            name: "test_open_db".to_string(),
            description: "Test Open Database".to_string(),
            storage: StorageConfig::default(),
            lock: None,
        };

        // open 不存在的数据库应返回 NotFound
        let err = AndaDB::open(object_store.clone(), config.clone())
            .await
            .unwrap_err();
        match err {
            DBError::NotFound { .. } => {}
            _ => panic!("Expected NotFound when opening a non-existent database"),
        }

        // 创建后应可以正常 open
        let _db = AndaDB::create(object_store.clone(), config.clone())
            .await
            .unwrap();
        let db = AndaDB::open(object_store, config).await.unwrap();
        assert_eq!(db.name(), "test_open_db");
    }

    #[tokio::test]
    async fn test_database_open_lock_mismatch() {
        let object_store = Arc::new(InMemory::new());

        let create_config = DBConfig {
            name: "test_open_lock_db".to_string(),
            description: "Test Open Lock Database".to_string(),
            storage: StorageConfig::default(),
            lock: Some(ByteBufB64(vec![1, 2, 3])),
        };

        // 先创建带锁的数据库
        let _db = AndaDB::create(object_store.clone(), create_config)
            .await
            .unwrap();

        // 使用不匹配的 lock 打开应失败
        let open_config = DBConfig {
            name: "test_open_lock_db".to_string(),
            description: "Test Open Lock Database".to_string(),
            storage: StorageConfig::default(),
            lock: Some(ByteBufB64(vec![9, 9, 9])),
        };
        let err = AndaDB::open(object_store, open_config).await.unwrap_err();

        match err {
            DBError::Storage { .. } => {}
            _ => panic!("Expected Storage error for lock mismatch"),
        }
    }

    #[tokio::test]
    async fn test_database_open_with_matching_lock() {
        let object_store = Arc::new(InMemory::new());
        let lock = ByteBufB64(vec![7, 8, 9]);

        let create_config = DBConfig {
            name: "test_open_match_lock_db".to_string(),
            description: "Test Open Match Lock Database".to_string(),
            storage: StorageConfig::default(),
            lock: Some(lock.clone()),
        };

        // 先创建带锁数据库
        let _db = AndaDB::create(object_store.clone(), create_config)
            .await
            .unwrap();

        // 使用相同 lock 打开应成功
        let open_config = DBConfig {
            name: "test_open_match_lock_db".to_string(),
            description: "Test Open Match Lock Database".to_string(),
            storage: StorageConfig::default(),
            lock: Some(lock),
        };
        let db = AndaDB::open(object_store, open_config).await.unwrap();
        assert_eq!(db.name(), "test_open_match_lock_db");
    }

    #[tokio::test]
    async fn test_create_collection() {
        let object_store = Arc::new(InMemory::new());
        let config = DBConfig::default();
        let db = AndaDB::create(object_store, config).await.unwrap();

        let mut schema = Schema::builder();
        schema
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        let schema = schema.build().unwrap();

        let collection_config = CollectionConfig {
            name: "test_collection".to_string(),
            description: "Test Collection".to_string(),
        };

        let collection = db
            .create_collection(schema.clone(), collection_config.clone(), async |_| Ok(()))
            .await
            .unwrap();

        assert_eq!(collection.name(), "test_collection");
        assert!(db.metadata().collections.contains("test_collection"));
    }

    #[tokio::test]
    async fn test_open_collection() {
        let object_store = Arc::new(InMemory::new());
        let config = DBConfig::default();
        let db = AndaDB::create(object_store, config).await.unwrap();

        let mut schema = Schema::builder();
        schema
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        let schema = schema.build().unwrap();

        let collection_config = CollectionConfig {
            name: "test_collection".to_string(),
            description: "Test Collection".to_string(),
        };

        // Create collection first
        db.create_collection(schema.clone(), collection_config.clone(), async |_| Ok(()))
            .await
            .unwrap();

        // Then open it
        let collection = db
            .open_collection("test_collection".to_string(), async |_| Ok(()))
            .await
            .unwrap();

        assert_eq!(collection.name(), "test_collection");
    }

    #[tokio::test]
    async fn test_open_or_create_collection() {
        let object_store = Arc::new(InMemory::new());
        let config = DBConfig::default();
        let db = AndaDB::create(object_store, config).await.unwrap();

        let mut schema = Schema::builder();
        schema
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        let schema = schema.build().unwrap();

        let collection_config = CollectionConfig {
            name: "test_collection".to_string(),
            description: "Test Collection".to_string(),
        };

        // First call should create the collection
        let collection1 = db
            .open_or_create_collection(schema.clone(), collection_config.clone(), async |_| Ok(()))
            .await
            .unwrap();

        assert_eq!(collection1.name(), "test_collection");

        // Second call should open the existing collection
        let collection2 = db
            .open_or_create_collection(schema.clone(), collection_config.clone(), async |_| Ok(()))
            .await
            .unwrap();

        assert_eq!(collection2.name(), "test_collection");
    }

    #[tokio::test]
    async fn test_collection_create_guards_metadata_and_dropping_state() {
        let object_store = Arc::new(InMemory::new());
        let config = DBConfig::default();
        let db = AndaDB::create(object_store, config).await.unwrap();

        let mut schema = Schema::builder();
        schema
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        let schema = schema.build().unwrap();

        let ghost_config = CollectionConfig {
            name: "ghost".to_string(),
            description: "Ghost Collection".to_string(),
        };
        db.inner
            .metadata
            .write()
            .collections
            .insert(ghost_config.name.clone());
        let err = Collection::create(db.clone(), schema.clone(), ghost_config)
            .await
            .unwrap_err();
        assert!(matches!(err, DBError::AlreadyExists { .. }));
        db.inner.metadata.write().collections.remove("ghost");

        let dropping_config = CollectionConfig {
            name: "drop_me".to_string(),
            description: "Dropping Collection".to_string(),
        };
        db.inner
            .dropping_collections
            .write()
            .insert(dropping_config.name.clone());

        let err = db
            .create_collection(schema.clone(), dropping_config.clone(), async |_| Ok(()))
            .await
            .unwrap_err();
        assert!(matches!(err, DBError::AlreadyExists { .. }));

        let err = db
            .open_or_create_collection(schema.clone(), dropping_config.clone(), async |_| Ok(()))
            .await
            .unwrap_err();
        assert!(matches!(err, DBError::AlreadyExists { .. }));

        let err = db
            .open_collection(dropping_config.name, async |_| Ok(()))
            .await
            .unwrap_err();
        assert!(matches!(err, DBError::AlreadyExists { .. }));

        // The tombstone must also win when an old handle is still cached.
        // A cancelled delete intentionally retains both; checking the
        // registry first would hand the deleting handle back to the caller.
        let live_config = CollectionConfig {
            name: "live_drop".to_string(),
            description: "Live Dropping Collection".to_string(),
        };
        db.create_collection(schema.clone(), live_config.clone(), async |_| Ok(()))
            .await
            .unwrap();
        db.inner
            .dropping_collections
            .write()
            .insert(live_config.name.clone());
        let err = db
            .open_or_create_collection(schema, live_config.clone(), async |_| Ok(()))
            .await
            .unwrap_err();
        assert!(matches!(err, DBError::AlreadyExists { .. }));
        db.inner
            .dropping_collections
            .write()
            .remove(&live_config.name);
    }

    #[tokio::test]
    async fn test_read_only_mode() {
        let object_store = Arc::new(InMemory::new());
        let config = DBConfig::default();
        let db = AndaDB::create(object_store, config).await.unwrap();

        let mut schema = Schema::builder();
        schema
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        let schema = schema.build().unwrap();

        // Create collection while DB is writable
        let collection_config = CollectionConfig {
            name: "test_collection".to_string(),
            description: "Test Collection".to_string(),
        };
        let collection = db
            .create_collection(schema.clone(), collection_config.clone(), async |_| Ok(()))
            .await
            .unwrap();
        db.close_collection(collection.name()).await.unwrap();

        // Set database to read-only after unregistering the collection. A
        // storage-loaded handle must inherit this state; otherwise it becomes
        // a fresh writer that escaped the propagation loop above.
        db.set_read_only(true);
        let reopened = db
            .open_collection("test_collection".to_string(), async |collection| {
                assert!(collection.create_btree_index(&["name"]).await.is_err());
                Ok(())
            })
            .await
            .unwrap();
        reopened.set_read_only(false);
        let mut document = reopened.new_document();
        document
            .set_field("name", FieldValue::Text("blocked".to_string()))
            .unwrap();
        assert!(reopened.add(document).await.is_err());
        assert!(reopened.flush(unix_ms()).await.is_err());

        let err = db
            .save_extension("blocked".to_string(), FieldValue::Text("value".to_string()))
            .await
            .unwrap_err();
        assert!(matches!(err, DBError::Generic { .. }));

        let err = db.remove_extension("blocked").await.unwrap_err();
        assert!(matches!(err, DBError::Generic { .. }));

        // Attempt to create another collection should fail
        let collection_config2 = CollectionConfig {
            name: "test_collection2".to_string(),
            description: "Test Collection 2".to_string(),
        };
        let result = db
            .create_collection(schema, collection_config2, async |_| Ok(()))
            .await;

        assert!(result.is_err());
        match result {
            Err(DBError::Generic { .. }) => (),
            _ => panic!("Expected Generic error due to read-only mode"),
        }
    }

    #[tokio::test]
    async fn test_database_close() {
        let object_store = Arc::new(InMemory::new());
        let config = DBConfig::default();
        let db = AndaDB::create(object_store, config).await.unwrap();

        let mut schema = Schema::builder();
        schema
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        let schema = schema.build().unwrap();

        let collection_config = CollectionConfig {
            name: "test_collection".to_string(),
            description: "Test Collection".to_string(),
        };

        db.create_collection(schema, collection_config, async |_| Ok(()))
            .await
            .unwrap();

        // Close the database
        db.close().await.unwrap();

        // Database should be in read-only mode after closing
        assert!(db.inner.read_only.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_delete_collection() {
        let object_store = Arc::new(InMemory::new());
        let config = DBConfig::default();
        let db = AndaDB::create(object_store, config).await.unwrap();

        // 构建 schema
        let mut schema_builder = Schema::builder();
        schema_builder
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        let schema = schema_builder.build().unwrap();

        let collection_config = CollectionConfig {
            name: "test_collection".to_string(),
            description: "Test Collection".to_string(),
        };

        // 创建集合
        db.create_collection(schema.clone(), collection_config.clone(), async |_| Ok(()))
            .await
            .unwrap();
        assert!(db.metadata().collections.contains("test_collection"));

        // 删除集合
        db.delete_collection("test_collection").await.unwrap();
        assert!(!db.metadata().collections.contains("test_collection"));

        // 再次打开应返回 NotFound
        let res = db
            .open_collection("test_collection".to_string(), async |_| Ok(()))
            .await;
        match res {
            Err(DBError::NotFound { .. }) => {}
            _ => panic!("expected NotFound after delete_collection"),
        }

        // 可以重新创建同名集合
        db.create_collection(schema, collection_config, async |_| Ok(()))
            .await
            .unwrap();
        assert!(db.metadata().collections.contains("test_collection"));
    }

    #[tokio::test]
    async fn test_create_collection_cleans_up_after_callback_failure() {
        let object_store = Arc::new(InMemory::new());
        let db = AndaDB::create(object_store.clone(), DBConfig::default())
            .await
            .unwrap();

        let mut schema = Schema::builder();
        schema
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        let schema = schema.build().unwrap();

        let config = CollectionConfig {
            name: "broken".to_string(),
            description: "Broken Collection".to_string(),
        };

        let err = db
            .create_collection(schema.clone(), config.clone(), async |_| {
                Err(DBError::Generic {
                    name: "broken".to_string(),
                    source: "callback failed".into(),
                })
            })
            .await
            .unwrap_err();
        assert!(matches!(err, DBError::Generic { .. }));
        assert!(!db.metadata().collections.contains("broken"));

        // The half-created files were cleaned up, so the same name can be
        // created again (previously wedged with AlreadyExists).
        let collection = db
            .create_collection(schema, config, async |_| Ok(()))
            .await
            .unwrap();
        assert_eq!(collection.name(), "broken");
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_collection_cleans_unregistered_leftovers() {
        use bytes::Bytes;
        use object_store::{PutOptions, PutPayload, path::Path};

        let object_store = Arc::new(InMemory::new());
        let db = AndaDB::create(object_store.clone(), DBConfig::default())
            .await
            .unwrap();

        // Simulate files left behind by a crashed collection creation that was
        // never registered in the database metadata.
        object_store
            .put_opts(
                &Path::from("anda_db/ghostcol/meta.cbor"),
                PutPayload::from(Bytes::from_static(b"junk")),
                PutOptions::default(),
            )
            .await
            .unwrap();

        assert!(!db.metadata().collections.contains("ghostcol"));
        db.delete_collection("ghostcol").await.unwrap();

        let mut listed = object_store.list(Some(&Path::from("anda_db/ghostcol")));
        assert!(listed.next().await.is_none());

        // Deleting a collection that never existed is a no-op.
        db.delete_collection("ghostcol").await.unwrap();

        // Invalid names are rejected instead of being used as storage paths.
        assert!(db.delete_collection("../escape").await.is_err());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_db_extension_get_set_remove() {
        let object_store = Arc::new(InMemory::new());
        let config = DBConfig::default();
        let db = AndaDB::create(object_store, config).await.unwrap();

        // 初始状态：无扩展数据
        assert!(db.get_extension("key1").is_none());
        assert!(db.metadata().extensions.is_empty());

        // set_extension：设置后可以 get 到
        db.set_extension("key1".into(), FieldValue::Text("hello".into()));
        assert_eq!(
            db.get_extension("key1"),
            Some(FieldValue::Text("hello".into()))
        );

        // 支持不同类型
        db.set_extension("count".into(), FieldValue::U64(42));
        db.set_extension("flag".into(), FieldValue::Bool(true));
        assert_eq!(db.get_extension("count"), Some(FieldValue::U64(42)));
        assert_eq!(db.get_extension("flag"), Some(FieldValue::Bool(true)));

        // 覆盖已有 key
        db.set_extension("key1".into(), FieldValue::I64(-1));
        assert_eq!(db.get_extension("key1"), Some(FieldValue::I64(-1)));

        // metadata() 中也能看到 extensions
        let meta = db.metadata();
        assert_eq!(meta.extensions.len(), 3);
        assert_eq!(meta.extensions.get("key1"), Some(&FieldValue::I64(-1)));

        // remove_extension：移除存在的 key
        let old = db.remove_extension("count").await.unwrap();
        assert_eq!(old, Some(FieldValue::U64(42)));
        assert!(db.get_extension("count").is_none());

        // remove_extension：移除不存在的 key 返回 None
        let old = db.remove_extension("nonexistent").await.unwrap();
        assert!(old.is_none());

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_db_extension_save_and_persist() {
        let object_store = Arc::new(InMemory::new());
        let config = DBConfig::default();

        // 创建数据库并 save_extension
        {
            let db = AndaDB::create(object_store.clone(), config.clone())
                .await
                .unwrap();
            db.save_extension("persist_key".into(), FieldValue::Text("persisted".into()))
                .await
                .unwrap();
            assert_eq!(
                db.get_extension("persist_key"),
                Some(FieldValue::Text("persisted".into()))
            );
        }

        // 重新 connect，验证扩展数据仍然存在
        let db = AndaDB::connect(object_store, config).await.unwrap();
        assert_eq!(
            db.get_extension("persist_key"),
            Some(FieldValue::Text("persisted".into()))
        );

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_db_extension_flush_persist() {
        let object_store = Arc::new(InMemory::new());
        let config = DBConfig::default();

        // 创建数据库，set_extension + flush
        {
            let db = AndaDB::create(object_store.clone(), config.clone())
                .await
                .unwrap();
            db.set_extension("lazy_key".into(), FieldValue::Bytes(vec![1, 2, 3]));
            db.flush().await.unwrap();
        }

        // 重新 connect，验证扩展数据仍然存在
        let db = AndaDB::connect(object_store, config).await.unwrap();
        assert_eq!(
            db.get_extension("lazy_key"),
            Some(FieldValue::Bytes(vec![1, 2, 3]))
        );

        db.close().await.unwrap();
    }

    fn test_schema() -> Schema {
        let mut schema = Schema::builder();
        schema
            .add_field(Fe::new("name".to_string(), Ft::Text).unwrap())
            .unwrap();
        schema.build().unwrap()
    }

    /// Regression (P0-05): different collection names use independent
    /// lifecycle locks, but their full-database metadata snapshots must still
    /// be persisted in one global order. The backing store below holds x's old
    /// `{x}` PUT while y is registered in memory. Without the flush gate, y's
    /// `{x,y}` PUT overtakes it and the released `{x}` PUT wins last.
    #[tokio::test]
    async fn test_different_collection_registrations_serialize_db_metadata_puts() {
        const DB_NAME: &str = "metadata_race_db";

        let (release_tx, release_rx) = tokio::sync::watch::channel(false);
        let store = Arc::new(ReverseMetadataPutStore::new(
            Path::from(format!("{DB_NAME}/{}", AndaDB::METADATA_PATH)),
            release_rx,
        ));
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let config = DBConfig {
            name: DB_NAME.to_string(),
            description: "metadata race regression".to_string(),
            storage: StorageConfig {
                // Let the test store decode and record each CBOR snapshot.
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        };
        let db = AndaDB::create(object_store.clone(), config.clone())
            .await
            .unwrap();

        // Prepare and fully flush both collection objects before registration.
        // This leaves register_created_collection's collection flush on its
        // no-I/O fast path, so one manual future poll deterministically reaches
        // the database metadata gate.
        let x = Collection::create(
            db.clone(),
            test_schema(),
            CollectionConfig {
                name: "x".to_string(),
                description: String::new(),
            },
        )
        .await
        .unwrap();
        x.flush(unix_ms()).await.unwrap();
        let y = Collection::create(
            db.clone(),
            test_schema(),
            CollectionConfig {
                name: "y".to_string(),
                description: String::new(),
            },
        )
        .await
        .unwrap();
        y.flush(unix_ms()).await.unwrap();

        store.arm();

        // Register x and poll through to its blocked db_meta PUT. The payload
        // has already been serialized here, so it is permanently the old
        // `{x}` snapshot even though y will be registered before release.
        let mut x_registration = Box::pin(db.register_created_collection(x, async |_| Ok(())));
        assert!(matches!(
            futures::poll!(x_registration.as_mut()),
            Poll::Pending
        ));
        let x_only = BTreeSet::from(["x".to_string()]);
        assert_eq!(store.snapshots(), vec![x_only.clone()]);

        // Registration of another name reaches flush_metadata in the same
        // poll, but must wait on x's gate instead of issuing an overtaking PUT.
        let mut y_registration = Box::pin(db.register_created_collection(y, async |_| Ok(())));
        assert!(matches!(
            futures::poll!(y_registration.as_mut()),
            Poll::Pending
        ));
        let both = BTreeSet::from(["x".to_string(), "y".to_string()]);
        assert_eq!(db.metadata().collections, both);
        assert_eq!(
            store.snapshots(),
            vec![x_only],
            "y must wait for the metadata lock before taking or PUTting its snapshot",
        );

        release_tx.send(true).unwrap();
        let x = x_registration.await.unwrap();
        let y = y_registration.await.unwrap();
        assert_eq!(
            store.snapshots(),
            vec![BTreeSet::from(["x".into()]), both.clone()]
        );

        // Simulate a crash: do not call close/flush on the original instance.
        // A fresh instance must recover both registered collection names from
        // the durable db_meta object, not merely from the old in-memory map.
        drop(x);
        drop(y);
        drop(db);
        let reopened = AndaDB::open(object_store, config).await.unwrap();
        assert_eq!(reopened.metadata().collections, both);
        reopened
            .open_collection("x".to_string(), async |_| Ok(()))
            .await
            .unwrap();
        reopened
            .open_collection("y".to_string(), async |_| Ok(()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_close_collection_unregisters_handle() {
        let object_store = Arc::new(InMemory::new());
        let db = AndaDB::create(object_store, DBConfig::default())
            .await
            .unwrap();

        let config = CollectionConfig {
            name: "c1".to_string(),
            description: "".to_string(),
        };
        let collection = db
            .create_collection(test_schema(), config, async |_| Ok(()))
            .await
            .unwrap();

        // Close through the database so the handle is unregistered; a later
        // open must reload a fresh, writable instance instead of returning
        // the closed (permanently read-only) one.
        db.close_collection("c1").await.unwrap();
        assert!(collection.metadata().stats.read_only);

        let reopened = db
            .open_collection("c1".to_string(), async |_| Ok(()))
            .await
            .unwrap();
        assert!(!Arc::ptr_eq(&collection, &reopened));
        assert!(!reopened.metadata().stats.read_only);

        // Closing a collection that is not open is a no-op.
        db.close_collection("nonexistent").await.unwrap();
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_open_or_create_collection() {
        let object_store = Arc::new(InMemory::new());
        let db = AndaDB::create(object_store, DBConfig::default())
            .await
            .unwrap();
        let schema = test_schema();

        // All concurrent callers must succeed: the losers of the create race
        // fall through to opening the winner's collection instead of
        // surfacing AlreadyExists.
        let mut handles = Vec::new();
        for _ in 0..8 {
            let db = db.clone();
            let schema = schema.clone();
            handles.push(tokio::spawn(async move {
                db.open_or_create_collection(
                    schema,
                    CollectionConfig {
                        name: "racing".to_string(),
                        description: "".to_string(),
                    },
                    async |_| Ok(()),
                )
                .await
            }));
        }
        for handle in handles {
            let collection = handle
                .await
                .unwrap()
                .expect("open_or_create_collection must not fail on a create race");
            assert_eq!(collection.name(), "racing");
        }
        db.close().await.unwrap();
    }

    /// Regression (#2): the creation lock is per collection name, so a
    /// creation callback that creates *another* collection must not deadlock
    /// (a single global lock previously hung forever here).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_nested_collection_creation_in_callback_does_not_deadlock() {
        let object_store = Arc::new(InMemory::new());
        let db = AndaDB::create(object_store, DBConfig::default())
            .await
            .unwrap();
        let schema = test_schema();

        let outer = {
            let db2 = db.clone();
            let inner_schema = schema.clone();
            tokio::time::timeout(
                Duration::from_secs(30),
                db.create_collection(
                    schema.clone(),
                    CollectionConfig {
                        name: "outer".to_string(),
                        description: "".to_string(),
                    },
                    async move |_| {
                        let inner = db2
                            .open_or_create_collection(
                                inner_schema,
                                CollectionConfig {
                                    name: "inner".to_string(),
                                    description: "".to_string(),
                                },
                                async |_| Ok(()),
                            )
                            .await?;
                        assert_eq!(inner.name(), "inner");
                        Ok(())
                    },
                ),
            )
            .await
            .expect("nested creation of a different collection must not deadlock")
            .unwrap()
        };
        assert_eq!(outer.name(), "outer");
        assert!(db.metadata().collections.contains("outer"));
        assert!(db.metadata().collections.contains("inner"));

        // The nested collection is fully usable afterwards.
        let inner = db
            .open_collection("inner".to_string(), async |_| Ok(()))
            .await
            .unwrap();
        assert_eq!(inner.name(), "inner");
        db.close().await.unwrap();
    }

    /// Regression (#3): `close_collection` holds the per-name lifecycle lock
    /// across unregistering and closing, and a concurrent open of the same
    /// name waits for it — so no second writable instance can be loaded
    /// while the closing flush is still writing (overwrite-mode) index
    /// files. The final state must be a single live, writable instance with
    /// the data intact.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_close_and_open_collection() {
        let object_store = Arc::new(InMemory::new());
        let db = AndaDB::create(object_store, DBConfig::default())
            .await
            .unwrap();
        let collection = db
            .create_collection(
                test_schema(),
                CollectionConfig {
                    name: "c".to_string(),
                    description: "".to_string(),
                },
                async |_| Ok(()),
            )
            .await
            .unwrap();
        let mut doc = collection.new_document();
        doc.set_id(0); // placeholder; `add` assigns the real id
        doc.set_field("name", FieldValue::Text("kept".to_string()))
            .unwrap();
        collection.add(doc).await.unwrap();
        drop(collection);

        let close_task = {
            let db = db.clone();
            tokio::spawn(async move { db.close_collection("c").await })
        };
        let open_task = {
            let db = db.clone();
            tokio::spawn(async move { db.open_collection("c".to_string(), async |_| Ok(())).await })
        };

        tokio::time::timeout(Duration::from_secs(30), close_task)
            .await
            .expect("concurrent close must not deadlock")
            .unwrap()
            .unwrap();
        let opened = tokio::time::timeout(Duration::from_secs(30), open_task)
            .await
            .expect("concurrent open must not deadlock")
            .unwrap()
            .unwrap();
        assert_eq!(opened.name(), "c");

        // Regardless of interleaving, a fresh open afterwards yields a
        // single writable instance with the document intact.
        let reopened = db
            .open_collection("c".to_string(), async |_| Ok(()))
            .await
            .unwrap();
        assert!(!reopened.metadata().stats.read_only);
        assert_eq!(reopened.len(), 1);
        db.close().await.unwrap();
    }

    /// Regression (#19): `open_or_create_collection` must re-check
    /// `dropping_collections` after acquiring the per-name lock, so a delete
    /// that started while it was waiting cannot be raced by a create.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_open_or_create_rechecks_dropping_after_lock() {
        let object_store = Arc::new(InMemory::new());
        let db = AndaDB::create(object_store, DBConfig::default())
            .await
            .unwrap();

        // Hold the lifecycle lock for the name, then start an
        // open_or_create_collection: it passes the pre-lock checks and
        // blocks on the lock.
        let guard = db.lock_collection_name("late_drop").await;
        let create_task = {
            let db = db.clone();
            let schema = test_schema();
            tokio::spawn(async move {
                db.open_or_create_collection(
                    schema,
                    CollectionConfig {
                        name: "late_drop".to_string(),
                        description: "".to_string(),
                    },
                    async |_| Ok(()),
                )
                .await
            })
        };
        // Give the task time to reach the lock wait, then mark the name as
        // being dropped and release the lock.
        tokio::time::sleep(Duration::from_millis(200)).await;
        db.inner
            .dropping_collections
            .write()
            .insert("late_drop".to_string());
        drop(guard);

        let result = tokio::time::timeout(Duration::from_secs(30), create_task)
            .await
            .expect("open_or_create must not deadlock")
            .unwrap();
        assert!(
            matches!(result, Err(DBError::AlreadyExists { .. })),
            "creating a name that started dropping while waiting for the \
             lock must fail with AlreadyExists, got {result:?}",
        );
        db.inner.dropping_collections.write().remove("late_drop");
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_open_collection_racing_delete_is_serialized() {
        let object_store = Arc::new(InMemory::new());
        let db = AndaDB::create(object_store, DBConfig::default())
            .await
            .unwrap();
        let config = CollectionConfig {
            name: "racy".to_string(),
            description: "".to_string(),
        };
        db.create_collection(test_schema(), config, async |_| Ok(()))
            .await
            .unwrap();
        // Drop the cached handle so the open below performs a real load.
        db.close_collection("racy").await.unwrap();

        // Stall the open inside its callback (holding the per-name lifecycle
        // lock) while a concurrent delete_collection is issued: the delete
        // must wait for the open to finish instead of interleaving with it,
        // so no zombie handle over a deleted storage prefix can exist.
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel::<()>();
        let (resume_tx, resume_rx) = tokio::sync::oneshot::channel::<()>();
        let open_task = {
            let db = db.clone();
            tokio::spawn(async move {
                db.open_collection("racy".to_string(), async move |_| {
                    let _ = entered_tx.send(());
                    let _ = resume_rx.await;
                    Ok(())
                })
                .await
            })
        };
        entered_rx.await.unwrap();
        let delete_task = {
            let db = db.clone();
            tokio::spawn(async move { db.delete_collection("racy").await })
        };
        // The delete is blocked on the lifecycle lock; unblock the open.
        resume_tx.send(()).unwrap();

        // The open completes first (it holds the lock) and returns a live
        // handle; the delete then removes the collection.
        let opened = tokio::time::timeout(Duration::from_secs(30), open_task)
            .await
            .expect("open racing a delete must not deadlock")
            .unwrap()
            .expect("open must succeed before the delete runs");
        assert_eq!(opened.name(), "racy");
        tokio::time::timeout(Duration::from_secs(30), delete_task)
            .await
            .expect("delete racing an open must not deadlock")
            .unwrap()
            .unwrap();

        // The taken handle was closed by the delete and the name is gone.
        assert!(opened.metadata().stats.read_only);
        let err = db
            .open_collection("racy".to_string(), async |_| Ok(()))
            .await
            .unwrap_err();
        assert!(matches!(err, DBError::NotFound { .. }));

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_db_set_extension_with() {
        let object_store = Arc::new(InMemory::new());
        let config = DBConfig::default();
        let db = AndaDB::create(object_store, config).await.unwrap();

        let key = "test_key".to_string();

        // 1. Initial state: None
        let old = db.set_extension_with(key.clone(), |val| {
            assert!(val.is_none());
            Some(FieldValue::U64(100))
        });
        assert!(old.is_none());
        assert_eq!(db.get_extension(&key), Some(FieldValue::U64(100)));

        // 2. Update existing value: 100 -> 200
        let old = db.set_extension_with(key.clone(), |val| {
            if let Some(FieldValue::U64(v)) = val {
                return Some(FieldValue::U64(v + 100));
            }
            None
        });
        assert_eq!(old, Some(FieldValue::U64(100)));
        assert_eq!(db.get_extension(&key), Some(FieldValue::U64(200)));

        // 3. Return None: No change
        let old = db.set_extension_with(key.clone(), |_| None);
        assert!(old.is_none());
        assert_eq!(db.get_extension(&key), Some(FieldValue::U64(200)));

        db.close().await.unwrap();
    }
}
