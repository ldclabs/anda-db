//! Collection persistence implementation.
use super::*;

impl Collection {
    /// Stores collection metadata to storage if it has changed.
    ///
    /// A single conditional PUT against the last observed object version is
    /// the remaining defense against a second writer, which the deployment
    /// contract forbids. A `Precondition` conflict is not reconciled in
    /// place: it propagates, the caller poisons the handle and recovery
    /// happens on reopen (which re-reads the durable object version).
    ///
    /// # Arguments
    /// * `now_ms` - Current timestamp in milliseconds
    ///
    /// # Returns
    /// `Some(max_document_id)` if metadata was stored, `None` if no changes needed to be stored
    pub(super) async fn store_metadata(&self, now_ms: u64) -> Result<Option<DocumentId>, DBError> {
        // Fast path: if version is already saved, avoid cloning metadata.
        let current_version = { self.metadata.read().stats.version };
        if self.last_saved_version.load(Ordering::Acquire) >= current_version {
            return Ok(None);
        }

        // Re-acquire metadata with lock to get a consistent snapshot for
        // saving. Complete flushes are serialized by `operation_gate`, so the
        // snapshot cannot change while the PUT is in flight.
        let mut metadata = self.metadata();
        if self.last_saved_version.load(Ordering::Acquire) >= metadata.stats.version {
            return Ok(None);
        }
        metadata.stats.last_saved = now_ms.max(metadata.stats.last_saved);
        // `read_only` describes this handle, not the collection: `close`
        // flips it to `true` before its final flush, so persisting the live
        // value made every clean shutdown record `read_only: true` for a
        // field both constructors then ignore on load. Persist the neutral
        // value instead of a snapshot nothing honors.
        metadata.stats.read_only = false;
        self.persist_metadata_snapshot(&metadata).await?;
        self.last_saved_version
            .fetch_max(metadata.stats.version, Ordering::Release);
        self.update_metadata(|m| {
            m.stats.last_saved = metadata.stats.last_saved.max(m.stats.last_saved);
        });
        Ok(Some(metadata.stats.max_document_id))
    }

    /// Persists the current collection metadata object once, **without**
    /// claiming the flush version watermark: `last_saved_version` is
    /// deliberately not advanced, so the next periodic flush still observes
    /// `version > last_saved_version` and runs the full path
    /// (`store_metadata` + `store_ids`) — a metadata-only write must never
    /// make a later flush skip persisting the ids bitmap.
    ///
    /// `Ok(())` means the snapshot containing this call's change was durably
    /// written. Extension writers are serialized against flush and each other
    /// by `operation_gate` leases plus the caller-held admission checks, so a
    /// `Precondition` here means a second writer and is not retried.
    pub(super) async fn store_metadata_unclaimed(&self) -> Result<(), DBError> {
        let _gate = self.extension_write_gate.lock().await;
        let mut metadata = self.metadata();
        self.committed_indexes
            .read()
            .retain_committed(&mut metadata);
        // See `store_metadata`: the read-only flag is live handle state and is
        // never persisted.
        metadata.stats.read_only = false;
        self.persist_metadata_snapshot(&metadata).await
    }

    pub(super) async fn persist_metadata_snapshot(
        &self,
        metadata: &CollectionMetadata,
    ) -> Result<(), DBError> {
        let mut payload = Vec::new();
        cbor2::to_writer(metadata, &mut payload).map_err(|source| DBError::Serialization {
            name: self.name.clone(),
            source: source.into(),
        })?;
        let expected = self.metadata_version.read().clone();
        let result = self
            .storage
            .put_bytes(
                Self::METADATA_PATH,
                payload.into(),
                crate::storage::PutMode::Update(expected.into()),
            )
            .await;
        match result {
            Ok(version) => {
                *self.metadata_version.write() = version;
                *self.committed_indexes.write() = IndexRegistry::from_metadata(metadata);
                Ok(())
            }
            Err(err) => {
                if !matches!(
                    err,
                    DBError::PayloadTooLarge { .. } | DBError::Serialization { .. }
                ) {
                    self.poison("Collection::persist_metadata_snapshot");
                }
                Err(err)
            }
        }
    }

    /// Stores document IDs bitmap to storage.
    ///
    /// # Returns
    /// Ok(()) if successful, or an error if storing fails
    pub(super) async fn store_ids(&self) -> Result<(), DBError> {
        let data = {
            // Lock order matches the mutation helpers: `doc_ids`, then the
            // bitmap.
            let doc_ids = self.doc_ids.read();
            let mut bitmap = self.doc_ids_bitmap.write();

            // A cheap probe, not a proof: cardinality is O(containers) while
            // a full comparison would be O(documents). It catches an id-set
            // mutation that bypassed the helpers — the failure mode that
            // would otherwise persist a wrong id set and drop live documents
            // on the next open — and repairs it instead of shipping it.
            if bitmap.cardinality() != doc_ids.len() as u64 {
                log::error!(
                    action = "Collection::store_ids",
                    collection = self.name,
                    bitmap = bitmap.cardinality(),
                    live = doc_ids.len();
                    "Document id bitmap diverged from the id set; rebuilding it",
                );
                *bitmap = doc_ids.iter().copied().collect();
            }

            // `run_optimize` on a copy: it rewrites containers into run form,
            // which is what makes the stored object small but not what makes
            // the next incremental `add` cheap.
            let mut ids = bitmap.clone();
            ids.run_optimize();
            ids.serialize::<Portable>()
        };
        let ver = { self.ids_version.read().clone() };
        let ver = match self.storage.put(Self::IDS_PATH, &data, Some(ver)).await {
            Ok(ver) => ver,
            Err(err) => {
                return Err(err);
            }
        };

        *self.ids_version.write() = ver;
        Ok(())
    }

    /// Stores all indexes to storage.
    ///
    /// # Arguments
    /// * `now_ms` - Current timestamp in milliseconds
    ///
    /// # Returns
    /// Ok(()) if successful, or an error if storing fails
    pub(super) async fn store_indexes(&self, now_ms: u64) -> Result<bool, DBError> {
        let (btree_saved, bm25_saved, hnsw_saved) = try_join_await!(
            try_join_all(self.btree_indexes.iter().map(|index| index.flush(now_ms))),
            try_join_all(self.bm25_indexes.iter().map(|index| index.flush(now_ms))),
            try_join_all(self.hnsw_indexes.iter().map(|index| index.flush(now_ms))),
        )?;

        Ok(btree_saved.into_iter().any(|saved| saved)
            || bm25_saved.into_iter().any(|saved| saved)
            || hnsw_saved.into_iter().any(|saved| saved))
    }

    pub(super) fn has_pending_index_flush(&self) -> bool {
        self.btree_indexes.iter().any(BTree::has_pending_flush)
            || self.bm25_indexes.iter().any(BM25::has_pending_flush)
            || self.hnsw_indexes.iter().any(Hnsw::has_pending_flush)
    }
}
