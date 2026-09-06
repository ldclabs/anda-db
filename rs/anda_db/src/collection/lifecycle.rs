//! Collection lifecycle implementation.
use super::*;

impl Collection {
    /// Creates a new collection with the given schema and configuration.
    ///
    /// # Arguments
    /// * `db` - Reference to the database this collection belongs to
    /// * `schema` - Schema defining the structure of documents in this collection
    /// * `config` - Configuration parameters for the collection
    ///
    /// # Returns
    /// A new Collection instance or an error if creation fails
    pub(crate) async fn create(
        db: AndaDB,
        schema: Schema,
        config: CollectionConfig,
    ) -> Result<Self, DBError> {
        validate_field_name(config.name.as_str())?;

        let base_path = Path::from(db.name()).join(config.name.as_str());
        let db_metadata = db.metadata();
        if db_metadata.collections.contains(&config.name) {
            return Err(DBError::AlreadyExists {
                name: config.name,
                path: base_path.to_string(),
                source: "".into(),
                _id: 0,
            });
        }

        let storage = Storage::connect(
            base_path.to_string(),
            db.object_store(),
            db_metadata.config.storage.clone(),
        )
        .await?;
        let stats = CollectionStats {
            version: 1,
            ..Default::default()
        };
        let metadata = CollectionMetadata {
            config: config.clone(),
            schema: schema.clone(),
            btree_indexes: BTreeMap::new(),
            bm25_indexes: BTreeMap::new(),
            hnsw_indexes: BTreeMap::new(),
            stats,
            extensions: BTreeMap::new(),
        };

        let metadata_version = storage.create(Self::METADATA_PATH, &metadata).await?;
        let ids_data = Treemap::new().serialize::<Portable>();
        let ids_version = match storage.create(Self::IDS_PATH, &ids_data).await {
            Ok(ver) => ver,
            Err(err) => {
                // Remove the metadata object written above, otherwise the
                // half-created collection blocks re-creation under this name.
                let _ = storage.delete(Self::METADATA_PATH).await;
                return Err(err);
            }
        };

        // Created successfully; publish the storage metadata. A failure here
        // must not leave the two objects behind either: nothing registers the
        // collection, and its name would then be blocked by `AlreadyExists`
        // on every later create.
        if let Err(err) = storage.store_metadata(0, unix_ms()).await {
            let _ = storage.delete(Self::IDS_PATH).await;
            let _ = storage.delete(Self::METADATA_PATH).await;
            return Err(err);
        }

        Ok(Self {
            name: config.name.clone(),
            schema: Arc::new(schema),
            storage,
            btree_indexes: Vec::new(),
            bm25_indexes: Vec::new(),
            hnsw_indexes: Vec::new(),
            max_document_id: AtomicU64::new(0),
            search_count: AtomicU64::new(0),
            get_count: AtomicU64::new(0),
            tokenizer: default_tokenizer(),
            doc_ids: RwLock::new(BTreeSet::new()),
            doc_ids_bitmap: RwLock::new(Treemap::new()),
            committed_indexes: RwLock::new(IndexRegistry::from_metadata(&metadata)),
            metadata: RwLock::new(metadata),
            read_only: AtomicBool::new(false),
            database_read_only: db.read_only_flag(),
            lifecycle: AtomicU8::new(LIFECYCLE_ACTIVE),
            operation_gate: Arc::new(tokio::sync::RwLock::new(())),
            unique_commit_gate: Arc::new(tokio::sync::RwLock::new(())),
            unique_key_locks: std::sync::OnceLock::new(),
            recovery_pending: AtomicBool::new(false),
            recovery_gate: tokio::sync::Mutex::new(()),
            io_concurrency: AtomicUsize::new(8),
            recovery_issues: RwLock::new(BTreeMap::new()),
            last_saved_version: AtomicU64::new(0),
            metadata_version: RwLock::new(metadata_version),
            ids_version: RwLock::new(ids_version),
            index_hooks: Arc::new(DefaultIndexHooks),
            doc_locks: Self::new_doc_locks(),
            pending_mutations: parking_lot::Mutex::new(BTreeSet::new()),
            stale_mutation_intents: parking_lot::Mutex::new(BTreeSet::new()),
            extension_write_gate: tokio::sync::Mutex::new(()),
            next_mutation_sequence: AtomicU64::new(unix_ms()),
            durable_alloc_watermark: AtomicU64::new(0),
            watermark_gate: tokio::sync::Mutex::new(()),
        })
    }

    /// Opens an existing collection.
    ///
    /// # Arguments
    /// * `db` - Reference to the database this collection belongs to
    /// * `name` - Name of the collection to open
    /// * `f` - Function to execute on the collection before it's fully loaded
    ///
    /// # Returns
    /// The opened Collection instance or an error if opening fails
    pub(crate) async fn open<F>(
        db: AndaDB,
        name: String,
        schema: Option<Schema>,
        f: F,
    ) -> Result<Self, DBError>
    where
        F: AsyncFnOnce(&mut Collection) -> Result<(), DBError>,
    {
        validate_field_name(name.as_str())?;
        let base_path = Path::from(db.name()).join(name.as_str());
        let db_metadata = db.metadata();
        let storage = Storage::connect(
            base_path.to_string(),
            db.object_store(),
            db_metadata.config.storage.clone(),
        )
        .await?;

        let (metadata, metadata_version) = storage
            .fetch::<CollectionMetadata>(Self::METADATA_PATH)
            .await?;

        let (ids, ids_version) = storage.fetch::<Vec<u8>>(Self::IDS_PATH).await?;
        // The stored bitmap is kept as the collection's own: it already
        // describes exactly the set materialized below.
        let stored_ids =
            Treemap::try_deserialize::<Portable>(&ids).ok_or_else(|| DBError::Generic {
                name: name.clone(),
                source: "Failed to deserialize ids".into(),
            })?;
        let doc_ids: BTreeSet<DocumentId> = stored_ids.iter().collect();

        // The durable allocation watermark bounds the id window the repair
        // scan below must probe. Collections created before the watermark
        // existed load as 0; the metadata max keeps their bound intact.
        let alloc_watermark = match storage.fetch::<u64>(Self::ALLOCATION_WATERMARK_PATH).await {
            Ok((watermark, _)) => watermark,
            Err(DBError::NotFound { .. }) => 0,
            Err(err) => return Err(err),
        };
        let metadata_max_document_id = metadata.stats.max_document_id;

        let mut collection = Self {
            name,
            schema: Arc::new(metadata.schema.clone()),
            storage,
            btree_indexes: Vec::new(),
            bm25_indexes: Vec::new(),
            hnsw_indexes: Vec::new(),
            max_document_id: AtomicU64::new(metadata.stats.max_document_id),
            search_count: AtomicU64::new(metadata.stats.search_count),
            get_count: AtomicU64::new(metadata.stats.get_count),
            last_saved_version: AtomicU64::new(metadata.stats.version),
            tokenizer: default_tokenizer(),
            doc_ids: RwLock::new(doc_ids),
            doc_ids_bitmap: RwLock::new(stored_ids),
            committed_indexes: RwLock::new(IndexRegistry::from_metadata(&metadata)),
            metadata: RwLock::new(metadata),
            read_only: AtomicBool::new(false),
            database_read_only: db.read_only_flag(),
            lifecycle: AtomicU8::new(LIFECYCLE_ACTIVE),
            operation_gate: Arc::new(tokio::sync::RwLock::new(())),
            unique_commit_gate: Arc::new(tokio::sync::RwLock::new(())),
            unique_key_locks: std::sync::OnceLock::new(),
            recovery_pending: AtomicBool::new(true),
            recovery_gate: tokio::sync::Mutex::new(()),
            io_concurrency: AtomicUsize::new(8),
            recovery_issues: RwLock::new(BTreeMap::new()),
            metadata_version: RwLock::new(metadata_version),
            ids_version: RwLock::new(ids_version),
            index_hooks: Arc::new(DefaultIndexHooks),
            doc_locks: Self::new_doc_locks(),
            pending_mutations: parking_lot::Mutex::new(BTreeSet::new()),
            stale_mutation_intents: parking_lot::Mutex::new(BTreeSet::new()),
            extension_write_gate: tokio::sync::Mutex::new(()),
            next_mutation_sequence: AtomicU64::new(unix_ms()),
            durable_alloc_watermark: AtomicU64::new(alloc_watermark.max(metadata_max_document_id)),
            watermark_gate: tokio::sync::Mutex::new(()),
        };
        collection.load_indexes().await?;

        if let Some(schema) = schema
            && collection.try_upgrade_schema(schema).await?
        {
            // The callback may write documents with newly assigned field
            // indexes. Persist that assignment first, so cancellation or a
            // callback error cannot leave a new-schema document behind
            // metadata that still describes the old schema.
            collection.store_metadata_unclaimed().await?;
        }

        // The callback installs custom index hooks and may add indexes. Run it
        // before replay/repair so recovery derives values with the same
        // application semantics as normal CRUD. Replaying once with default
        // hooks would leave B-tree/BM25 phantom entries that a later replay
        // with custom hooks cannot identify and remove.
        f(&mut collection).await?;

        collection.ensure_recovered().await?;

        Ok(collection)
    }

    /// Sets the collection to read-only mode.
    ///
    /// # Arguments
    /// * `read_only` - Whether to enable read-only mode
    pub fn set_read_only(&self, read_only: bool) {
        if !read_only
            && (self.lifecycle.load(Ordering::Acquire) != LIFECYCLE_ACTIVE
                || self.database_read_only.load(Ordering::Acquire))
        {
            log::warn!(
                action = "Collection::set_read_only",
                collection = self.name;
                "Ignoring attempt to re-enable a closed collection handle",
            );
            return;
        }
        self.read_only.store(read_only, Ordering::Release);
        log::info!(
            action = "Collection::set_read_only",
            collection = self.name;
            "Collection is set to read-only: {read_only}",
        );
    }

    /// Closes the collection, ensuring all data is flushed to storage.
    ///
    /// # Returns
    /// Ok(()) if successful, or an error if closing fails
    pub async fn close(&self) -> Result<(), DBError> {
        self.ensure_recovered().await?;
        loop {
            match self.lifecycle.load(Ordering::Acquire) {
                LIFECYCLE_ACTIVE => {
                    if self
                        .lifecycle
                        .compare_exchange(
                            LIFECYCLE_ACTIVE,
                            LIFECYCLE_CLOSING,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
                LIFECYCLE_CLOSING => break,
                LIFECYCLE_CLOSED | LIFECYCLE_DELETED => return Ok(()),
                LIFECYCLE_DELETING => return Err(self.lifecycle_error()),
                _ => return Err(self.lifecycle_error()),
            }
        }
        // Publish the user-visible read-only state as soon as admission
        // closes. Existing operations are drained by the exclusive gate.
        self.read_only.store(true, Ordering::Release);
        let _operation_guard = self.operation_gate.clone().write_owned().await;
        match self.lifecycle.load(Ordering::Acquire) {
            LIFECYCLE_CLOSED | LIFECYCLE_DELETED => return Ok(()),
            LIFECYCLE_DELETING => return Err(self.lifecycle_error()),
            LIFECYCLE_CLOSING => {}
            _ => return Err(self.lifecycle_error()),
        }

        let start = Instant::now();
        let now_ms = unix_ms();
        let rt = self
            .guarded("Collection::close", self.flush_inner(now_ms))
            .await;
        let elapsed = start.elapsed();
        match rt {
            Ok(_) => {
                self.lifecycle.store(LIFECYCLE_CLOSED, Ordering::Release);
                log::info!(
                    action = "Collection::close",
                    collection = self.name,
                    elapsed = elapsed.as_millis();
                    "Collection closed successfully in {elapsed:?}",
                );
                Ok(())
            }
            Err(err) => {
                // The failed flush may have completed some of its dependent
                // writes; the in-memory watermarks are no longer trustworthy.
                // Poison so a reopen loads a fresh generation from storage
                // instead of retrying with diverged state.
                self.poison("Collection::close");
                log::error!(
                    action = "Collection::close",
                    collection = self.name,
                    elapsed = elapsed.as_millis();
                    "Failed to close collection: {err:?}",
                );
                Err(err)
            }
        }
    }

    /// Flushes all pending changes to storage.
    ///
    /// # Arguments
    /// * `now_ms` - Current timestamp in milliseconds
    ///
    /// # Returns
    /// `true` if changes were flushed, `false` if no changes needed to be
    /// flushed or the handle (or its database) is read-only. Read-only means
    /// "serve reads, persist nothing" — a read-only open may even replay
    /// recovery state in memory that must not reach storage — so a periodic
    /// flush is a no-op there rather than an error that would fail
    /// [`AndaDB::flush`] on every interval. `close` still flushes.
    pub async fn flush(&self, now_ms: u64) -> Result<bool, DBError> {
        self.ensure_recovered().await?;
        // The write guard both serializes complete flushes and freezes all
        // document/index mutations for the checkpoint transaction.
        let _operation_guard = self.operation_gate.clone().write_owned().await;
        if !self.is_active_handle() {
            return Err(self.lifecycle_error());
        }
        if self.is_read_only() {
            return Ok(false);
        }
        let rt = self
            .guarded("Collection::flush", self.flush_inner(now_ms))
            .await;
        if rt.is_err() {
            // A checkpoint is multiple dependent writes; after any failure the
            // in-memory watermarks no longer describe what is durable. Treat
            // it like a crash: reject further use and recover on reopen.
            self.poison("Collection::flush");
        }
        rt
    }

    /// A checkpoint is a sequence of dependent writes (collection metadata,
    /// indexes, ids bitmap, storage checkpoint, WAL retirement). Any error
    /// after the first write leaves memory and storage diverged in a way this
    /// handle no longer tracks — the caller ([`Collection::flush`]) poisons
    /// the handle, and reopening converges from storage. `flush` holds the
    /// exclusive `operation_gate`, so no mutation runs concurrently.
    pub(super) async fn flush_inner(&self, now_ms: u64) -> Result<bool, DBError> {
        // On a live handle every retained intent belongs to a mutation that
        // either completed (indexes and document agree) or failed
        // deterministically before its storage write (memory was rolled back
        // to the stored state). Unknown-outcome failures and cancellations
        // poison the handle before reaching this point, so no reconciliation
        // is needed here: the checkpoint below captures a consistent state
        // and simply retires the intents afterwards. Reconciliation happens
        // only on reopen (`replay_mutation_intents`).
        let has_pending_mutations = {
            !self.pending_mutations.lock().is_empty()
                || !self.stale_mutation_intents.lock().is_empty()
        };
        let has_pending_indexes = self.has_pending_index_flush();
        // Fast path: no collection metadata update and no index has pending
        // data. `store_metadata` re-checks the version itself; the check is
        // repeated here so the index flush below can run *before* the
        // metadata write without turning a no-op flush into a PUT.
        let has_pending_metadata = {
            let version = self.metadata.read().stats.version;
            self.last_saved_version.load(Ordering::Acquire) < version
        };
        if !has_pending_metadata && !has_pending_indexes && !has_pending_mutations {
            return Ok(false);
        }

        // Indexes are persisted **before** the collection metadata that
        // registers them. Metadata is the durable pointer to the index set:
        // publishing it first makes a newly created index reachable while its
        // own objects are still empty, and nothing ever re-backfills it — the
        // next open bootstraps the empty durable index, `create_*_index_nx`
        // sees it registered and swallows `AlreadyExists`, and the repair scan
        // only covers ids above the storage checkpoint. The reverse order can
        // only leave unreferenced index objects behind, which index creation
        // overwrites.
        let indexes_saved = if has_pending_indexes {
            self.store_indexes(now_ms).await?
        } else {
            false
        };

        let stored_check_point = self.store_metadata(now_ms).await?;

        if let Some(check_point) = stored_check_point {
            // The metadata snapshot was taken under the exclusive operation
            // gate, so every id at or below `check_point` is already visible
            // in the ids bitmap: there are no in-flight adds during a flush.
            self.store_ids().await?;
            // check_point is the last persisted document ID
            self.storage.store_metadata(check_point, now_ms).await?;
        }

        // The intent log is the commit record for document/index atomicity and
        // is removed last. A crash before this point replays the mutation; a
        // crash after it observes durable indexes, ids and checkpoint.
        if has_pending_mutations {
            self.clear_mutation_intents().await?;
        }

        Ok(stored_check_point.is_some() || indexes_saved || has_pending_mutations)
    }

    /// Irreversibly closes mutation admission before database metadata is
    /// unregistered. The database holds the per-name lifecycle lock when
    /// calling this; a cancelled delete leaves the tombstoned handle in the
    /// registry so a retry can finish draining and deleting it.
    pub(crate) fn begin_delete(&self) -> Result<(), DBError> {
        loop {
            let state = self.lifecycle.load(Ordering::Acquire);
            match state {
                LIFECYCLE_DELETED | LIFECYCLE_DELETING => break,
                // A poisoned handle may be deleted: deletion does not depend
                // on trustworthy in-memory state, it removes storage.
                LIFECYCLE_ACTIVE | LIFECYCLE_CLOSING | LIFECYCLE_CLOSED | LIFECYCLE_POISONED => {
                    if self
                        .lifecycle
                        .compare_exchange(
                            state,
                            LIFECYCLE_DELETING,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        break;
                    }
                }
                _ => return Err(self.lifecycle_error()),
            }
        }
        self.read_only.store(true, Ordering::Release);
        Ok(())
    }

    /// Drops the collection, deleting all associated data from storage.
    pub(crate) async fn drop_data(&self) -> Result<(), DBError> {
        self.begin_delete()?;
        let _operation_guard = self.operation_gate.clone().write_owned().await;
        if self.lifecycle.load(Ordering::Acquire) == LIFECYCLE_DELETED {
            return Ok(());
        }

        let start = Instant::now();
        let total = self.len();

        // 并发删除集合存储下的全部对象（文档、元数据、ids 和索引）
        self.storage.drop_data().await?;
        self.lifecycle.store(LIFECYCLE_DELETED, Ordering::Release);
        let elapsed = start.elapsed();
        log::info!(
            action = "Collection::drop_data",
            collection = self.name,
            deleted = total,
            elapsed = elapsed.as_millis();
            "Collection dropped. deleted={total}, elapsed={elapsed:?}"
        );

        Ok(())
    }
}
