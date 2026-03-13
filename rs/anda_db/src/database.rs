use futures::{stream, stream::StreamExt};
use object_store::ObjectStore;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
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
    /// Map of collection names to collection instances
    collections: RwLock<BTreeMap<String, Arc<Collection>>>,
    /// Flag indicating whether the database is in read-only mode
    read_only: AtomicBool,
    /// Set of collection names being dropped
    dropping_collections: RwLock<BTreeSet<String>>,
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

    /// Returns storage statistics for the database.
    pub fn stats(&self) -> StorageStats {
        self.inner.storage.stats()
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
                collections: RwLock::new(BTreeMap::new()),
                read_only: AtomicBool::new(false),
                dropping_collections: RwLock::new(BTreeSet::new()),
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
                        collections: RwLock::new(BTreeMap::new()),
                        read_only: AtomicBool::new(false),
                        dropping_collections: RwLock::new(BTreeSet::new()),
                    }),
                };

                if let Some(lock) = set_lock {
                    this.set_lock(lock).await?;
                }

                Ok(this)
            }
            Err(DBError::NotFound { .. }) => {
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
                        collections: RwLock::new(BTreeMap::new()),
                        read_only: AtomicBool::new(false),
                        dropping_collections: RwLock::new(BTreeSet::new()),
                    }),
                })
            }
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
                        collections: RwLock::new(BTreeMap::new()),
                        read_only: AtomicBool::new(false),
                        dropping_collections: RwLock::new(BTreeSet::new()),
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
        let _ = stream::iter(collections.into_iter())
            .map(|collection| async move { collection.close().await })
            .buffer_unordered(8) // 限制最多 8 个并发
            .collect::<Vec<_>>()
            .await;

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

        let _ = stream::iter(collections.into_iter())
            .map(|collection| async move { collection.flush(unix_ms()).await })
            .buffer_unordered(8) // 限制最多 8 个并发
            .collect::<Vec<_>>()
            .await;

        self.flush_metadata(unix_ms()).await
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

    /// Creates a new collection in the database.
    ///
    /// This method creates a new collection with the given schema and configuration.
    /// It also executes the provided function on the collection before finalizing creation.
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

        let start = Instant::now();
        // self.metadata.collections will check it exists again in Collection::create
        let mut collection = Collection::create(self.clone(), schema, config).await?;
        f(&mut collection).await?;
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

        {
            if let Some(collection) = self.inner.collections.read().get(&config.name) {
                return Ok(collection.clone());
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

        {
            if !self
                .inner
                .metadata
                .read()
                .collections
                .contains(&config.name)
            {
                return self.create_collection(schema, config, f).await;
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
            if let Some(collection) = self.inner.collections.read().get(&name) {
                return Ok(collection.clone());
            }
        }

        {
            if self.inner.dropping_collections.read().contains(&name) {
                return Err(DBError::AlreadyExists {
                    name,
                    path: self.inner.name.clone(),
                    source: "collection is being dropped".to_string().into(),
                    _id: 0,
                });
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
            let mut collections = self.inner.collections.write();
            collections.insert(collection.name().to_string(), collection.clone());
        }
        let now = unix_ms();
        collection.flush(now).await?;
        Ok(collection)
    }

    pub async fn delete_collection(&self, name: &str) -> Result<(), DBError> {
        if self.inner.read_only.load(Ordering::Relaxed) {
            return Err(DBError::Generic {
                name: self.inner.name.clone(),
                source: "database is read-only".into(),
            });
        }

        // 更新元数据并持久化
        {
            if !self.inner.metadata.write().collections.remove(name) {
                return Ok(());
            }

            self.inner
                .dropping_collections
                .write()
                .insert(name.to_string());
        }

        self.flush_metadata(unix_ms()).await?;
        if let Some(col) = { self.inner.collections.write().remove(name) } {
            let _ = col.drop_data().await;
        }

        self.inner.dropping_collections.write().remove(name);
        Ok(())
    }

    async fn set_lock(&self, lock: ByteBufB64) -> Result<(), DBError> {
        {
            self.inner.metadata.write().config.lock = Some(lock);
        }

        let metadata = self.metadata();
        self.inner
            .storage
            .put(Self::METADATA_PATH, &metadata, None)
            .await?;
        Ok(())
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

    /// Sets a user-defined extension key-value pair.
    /// The change is persisted on the next `flush()`.
    /// The extensions should not be large, as they are stored in the same object as database metadata which size is expected to be small (<= 1MB) and loaded frequently.
    pub fn set_extension(&self, key: String, value: FieldValue) {
        self.inner.metadata.write().extensions.insert(key, value);
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
            meta.extensions.insert(key, value)
        } else {
            None
        }
    }

    /// Sets a user-defined extension key-value pair and immediately persists the change.
    /// The extensions should not be large, as they are stored in the same object as database metadata which size is expected to be small (<= 1MB) and loaded frequently.
    pub async fn save_extension(&self, key: String, value: FieldValue) -> Result<(), DBError> {
        {
            self.inner.metadata.write().extensions.insert(key, value);
        }
        self.flush_metadata(unix_ms()).await
    }

    /// Removes a user-defined extension key and immediately persists the change.
    /// Returns the previous value if the key existed.
    pub async fn remove_extension(&self, key: &str) -> Result<Option<FieldValue>, DBError> {
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
    use object_store::memory::InMemory;

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
        let _collection = db
            .create_collection(schema.clone(), collection_config.clone(), async |_| Ok(()))
            .await
            .unwrap();

        // Set database to read-only
        db.set_read_only(true);

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
