//! Collection crud implementation.
use super::*;

impl Collection {
    /// Adds a new document to the collection.
    ///
    /// This method:
    /// 1. Validates the document against the collection schema
    /// 2. Assigns a unique ID to the document
    /// 3. Updates all relevant indexes
    /// 4. Persists the document to storage
    ///
    /// # Arguments
    /// * `doc` - The document to add to the collection
    ///
    /// # Returns
    /// The ID of the newly added document, or an error if addition fails
    ///
    /// # Errors
    /// Returns an error if:
    /// - The collection is in read-only mode
    /// - The document fails schema validation
    /// - Any index update fails
    /// - Storage operations fail
    pub async fn add(&self, doc: Document) -> Result<DocumentId, DBError> {
        let _operation_lease = self.mutation_lease().await?;
        // Past this point a dropped future is treated as a crash: in-memory
        // index/bitmap state may already diverge from storage, so the guard
        // poisons the handle and recovery happens on reopen.
        self.guarded("Collection::add", self.add_impl(doc)).await
    }

    /// Guarantees `id` is at or below the durable allocation watermark before
    /// any document object may be written for it. Persisted in strides, so
    /// this is one small PUT per [`Self::ALLOCATION_WATERMARK_STRIDE`] adds.
    /// A failed watermark PUT fails the add before anything else was written:
    /// the id is skipped and the handle stays healthy.
    pub(super) async fn ensure_allocation_watermark(&self, id: DocumentId) -> Result<(), DBError> {
        if id <= self.durable_alloc_watermark.load(Ordering::Acquire) {
            return Ok(());
        }
        let _gate = self.watermark_gate.lock().await;
        if id <= self.durable_alloc_watermark.load(Ordering::Acquire) {
            return Ok(());
        }
        let target = self
            .max_document_id
            .load(Ordering::Acquire)
            .max(id)
            .saturating_add(Self::ALLOCATION_WATERMARK_STRIDE);
        self.storage
            .put(Self::ALLOCATION_WATERMARK_PATH, &target, None)
            .await?;
        self.durable_alloc_watermark
            .fetch_max(target, Ordering::AcqRel);
        Ok(())
    }

    pub(super) async fn add_impl(&self, mut doc: Document) -> Result<DocumentId, DBError> {
        if !Arc::ptr_eq(&self.schema, doc.schema())
            && !self.schema.has_same_field_mapping(doc.schema())
        {
            return Err(DBError::Schema {
                name: self.name.clone(),
                source: "document schema does not match the collection's field mapping".into(),
            });
        }
        self.schema.validate(doc.fields())?;
        // Flush holds the exclusive `operation_gate` while this add holds a
        // shared lease, so a checkpoint can never observe this id before the
        // bitmap registration below completes. A failed add simply skips the
        // id forever; a cancelled add poisons the handle and the reopen
        // repair scan (bounded by the allocation watermark) recovers or
        // retires the id.
        let id = self.max_document_id.fetch_add(1, Ordering::Acquire) + 1;
        doc.set_id(id);

        // Adds write no per-mutation intent. The durable allocation watermark
        // guarantees the reopen repair scan enumerates every id that may have
        // a document object, so a committed-but-unacknowledged add is found
        // there instead of through a WAL record.
        self.ensure_allocation_watermark(id).await?;
        let _keys = self.unique_key_lease(&[&doc]).await?;

        let now_ms = unix_ms();
        let mut undo = Vec::new();

        let rt: Result<(), DBError> = (|| {
            for index in &self.btree_indexes {
                if let Some(fv) = self.index_hooks.btree_index_value(index, &doc) {
                    if fv.as_ref() == &FieldValue::Null {
                        continue;
                    }

                    // Recorded whatever the outcome: an array value may have
                    // been partially inserted before a later element failed,
                    // and the rollback must sweep those elements too.
                    let rt = index.insert(id, &fv, now_ms);
                    undo.push(IndexUndo::BTreeAdded(index, fv));
                    rt?;
                }
            }

            for index in &self.bm25_indexes {
                if let Some(text) = self.index_hooks.bm25_index_value(index, &doc) {
                    index.insert(id, &text, now_ms)?;
                    undo.push(IndexUndo::BM25Added(index, text));
                }
            }

            for index in &self.hnsw_indexes {
                if let Some(vector) = self.index_hooks.hnsw_index_value(index, &doc) {
                    index.insert(id, vector.into_owned(), now_ms)?;
                    undo.push(IndexUndo::HnswAdded(index));
                }
            }

            Ok(())
        })();

        if let Err(err) = rt {
            self.rollback_indexes(id, undo, now_ms);
            return Err(err);
        }

        let path = Self::doc_path(id);
        if let Err(err) = self.storage.create(&path, &doc).await {
            self.rollback_indexes(id, undo, now_ms);
            // `AlreadyExists` is the one *known* outcome of `PutMode::Create`:
            // this add wrote nothing, the object at `path` is someone else's
            // committed document (another writer, or a document this handle
            // has not observed). Deleting it would destroy their data, so the
            // compensating delete is deliberately skipped.
            if !matches!(err, DBError::AlreadyExists { .. }) {
                // Any other failure leaves the PUT outcome unknown: it may
                // have committed. Delete the object so the id cannot survive
                // as an orphan below a future checkpoint. If even the delete
                // outcome is unknown, treat it like a crash — the reopen
                // repair scan (whose checkpoint has not advanced past this
                // id) decides whether the document exists.
                match self.storage.delete(&path).await {
                    Ok(()) | Err(DBError::NotFound { .. }) => {}
                    Err(delete_err) => {
                        log::error!(
                            action = "Collection::add",
                            collection = self.name,
                            doc_id = id;
                            "Failed to clean up document after failed add: {delete_err:?}",
                        );
                        self.poison("Collection::add");
                    }
                }
            }
            return Err(err);
        }

        self.register_doc_id(id);

        self.update_metadata(|meta| {
            meta.stats.last_inserted = now_ms;
            meta.stats.version += 1;
            meta.stats.insert_count += 1;
        });

        Ok(id)
    }

    /// Adds a new document to the collection from a serializable value.
    ///
    /// This method:
    /// 1. Converts the value into a Document using the collection's schema
    /// 2. Validates the document against the schema
    /// 3. Assigns a unique ID to the document
    /// 4. Updates all relevant indexes
    /// 5. Persists the document to storage
    /// # Arguments
    /// * `val` - The value to convert into a document
    ///
    /// # Returns
    /// The ID of the newly added document, or an error if addition fails
    pub async fn add_from<T>(&self, val: &T) -> Result<DocumentId, DBError>
    where
        T: Serialize,
    {
        let doc = Document::try_from(self.schema(), val)?;
        self.add(doc).await
    }

    /// Updates an existing document with new field values.
    ///
    /// Concurrent `update` / `remove` calls for the same document id are
    /// serialized internally (striped per-id locks), so index state and the
    /// stored document cannot diverge under in-process concurrency. The
    /// version precondition on the storage write additionally guards against
    /// writers outside this process.
    ///
    /// # Durability
    ///
    /// Before changing either the document or any derived index, `update`
    /// durably records the document's previous indexed values. A successful
    /// call means the document object itself is durable; the next `flush`
    /// commits the corresponding index/ids generation and removes the intent.
    /// If the process stops first, collection open replays the intent and
    /// makes the stored document authoritative for every index.
    ///
    /// # Arguments
    /// * `id` - The ID of the document to update
    /// * `fields` - The new field values to apply
    ///
    /// # Returns
    /// Ok(Document) if successful, or an error if update fails
    ///
    /// # Errors
    /// Returns an error if:
    /// - The collection is in read-only mode
    /// - The document doesn't exist
    /// - The updated document fails schema validation
    /// - The updated document version not matching the stored version because of concurrent update
    /// - Any index update fails
    /// - Storage operations fail
    pub async fn update(
        &self,
        id: DocumentId,
        fields: BTreeMap<String, Fv>,
    ) -> Result<Document, DBError> {
        let _operation_lease = self.mutation_lease().await?;
        self.guarded("Collection::update", self.update_impl(id, fields))
            .await
    }

    pub(super) async fn update_impl(
        &self,
        id: DocumentId,
        fields: BTreeMap<String, Fv>,
    ) -> Result<Document, DBError> {
        if !self.doc_ids.read().contains(&id) {
            return Err(DBError::NotFound {
                name: "document".to_string(),
                path: self.name.clone(),
                source: format!("Document with ID {id} not found").into(),
                _id: id,
            });
        }

        if fields.is_empty() {
            return Err(DBError::Generic {
                name: self.name.clone(),
                source: "No fields to update".into(),
            });
        }
        if fields.contains_key(Schema::ID_KEY) {
            // The id is the document's storage path and its key in every
            // index; rewriting it inside the object would leave `get(id)`
            // returning a document that claims a different id.
            return Err(DBError::Schema {
                name: self.name.clone(),
                source: format!("field {:?} cannot be updated", Schema::ID_KEY).into(),
            });
        }

        // Serialize mutations of the same document (see `doc_locks`): the
        // read-modify-write below must not interleave with another update or
        // remove of this id, or rolled-back index entries could diverge from
        // the stored document.
        let _doc_guard = self.doc_lock(id).lock().await;
        self.ensure_mutable()?;

        let (doc, ver) = self
            .storage
            .get::<DocumentOwned>(&Self::doc_path(id))
            .await?;
        let mut doc = Document::try_from_doc(self.schema(), doc)?;
        let old_doc = doc.clone();

        // apply the new values
        let mut fields_keys = FxHashSet::default();
        for (field_name, fv) in fields {
            doc.set_field(&field_name, fv)?;
            fields_keys.insert(field_name);
        }

        // validate the updated document
        self.schema.validate(doc.fields())?;

        let _keys = self.unique_key_lease(&[&old_doc, &doc]).await?;

        // Persist the old indexable values before changing either side of the
        // document/index pair. The intent is cleared only by a successful
        // full flush after both sides are durable.
        self.record_mutation_intent(id, Some(&old_doc), Some(&doc))
            .await?;

        let now_ms = unix_ms();

        let mut undo = Vec::new();

        // update the indexes
        let rt: Result<(), DBError> = (|| {
            for index in &self.btree_indexes {
                let fields = index.virtual_field();
                if fields_keys.iter().any(|v| fields.contains(v)) {
                    let old_value = self
                        .index_hooks
                        .btree_index_value(index, &old_doc)
                        .unwrap_or(Cow::Owned(FieldValue::Null));
                    let new_value = self
                        .index_hooks
                        .btree_index_value(index, &doc)
                        .unwrap_or(Cow::Owned(FieldValue::Null));

                    let result = index.update(id, &old_value, &new_value, now_ms);
                    undo.push(IndexUndo::BTreeChanged(index, old_value, new_value));
                    result?;
                }
            }

            for index in &self.bm25_indexes {
                let fields = index.virtual_field();
                if fields_keys.iter().any(|v| fields.contains(v)) {
                    if let Some(text) = self.index_hooks.bm25_index_value(index, &old_doc) {
                        index.remove(id, &text, now_ms);
                        undo.push(IndexUndo::BM25Removed(index, text));
                    }

                    if let Some(text) = self.index_hooks.bm25_index_value(index, &doc) {
                        index.insert(id, &text, now_ms)?;
                        undo.push(IndexUndo::BM25Added(index, text));
                    }
                }
            }

            for index in &self.hnsw_indexes {
                let field_name = index.field_name();
                if fields_keys.contains(field_name) {
                    if let Some(vector) = self.index_hooks.hnsw_index_value(index, &old_doc) {
                        index.remove(id, now_ms);
                        undo.push(IndexUndo::HnswRemoved(index, vector));
                    }

                    if let Some(vector) = self.index_hooks.hnsw_index_value(index, &doc) {
                        undo.push(IndexUndo::HnswAdded(index));
                        index.insert(id, vector.into_owned(), now_ms)?;
                    }
                }
            }

            Ok(())
        })();

        if let Err(err) = rt {
            if !self.rollback_indexes(id, undo, now_ms) {
                // Memory and storage are diverged in a way this handle no
                // longer tracks; recover on reopen, exactly like the unknown
                // storage outcome below. `remove_impl` poisons unconditionally
                // on its equivalent path for the same reason.
                self.poison("Collection::update");
            }
            return Err(err);
        }

        // persist the updated document with update version
        let path = Self::doc_path(id);
        if let Err(err) = self.storage.put(&path, &doc, Some(ver)).await {
            self.rollback_indexes(id, undo, now_ms);
            // The PUT outcome is unknown: the new document may be durable
            // while memory was just rolled back. The retained intent plus a
            // reopen reconcile the divergence; this handle must not continue.
            self.poison("Collection::update");
            return Err(err);
        }

        self.update_metadata(|meta| {
            meta.stats.last_updated = now_ms;
            meta.stats.version += 1;
            meta.stats.update_count += 1;
        });

        Ok(doc)
    }

    /// Removes a document from the collection by its ID.
    ///
    /// This method (deliberately in this order, for crash safety):
    /// 1. Removes the document from all relevant indexes
    /// 2. Deletes the document object from storage
    /// 3. Removes the document ID from the bitmap
    ///
    /// Deleting the object before the bitmap update means a crash in between
    /// leaves a dead id that the reopen intent replay retires, instead of an
    /// orphaned object beyond the repair scan window.
    /// A durable mutation intent containing the old indexed values is written
    /// before phase 1. It is retired only after a full flush, so reopening
    /// after a crash can finish removing stale B-Tree, BM25 and HNSW entries.
    ///
    /// # Cost
    ///
    /// Removing a live document is proportional to its own indexed values.
    /// Removing a **dead id** (registered, but its object is already gone) or
    /// a document that no longer decodes has no values to remove by key, so
    /// it falls back to [`Self::purge_dead_ids_from_indexes`], which sweeps
    /// every B-tree key and the whole BM25 inverted index once — `O(index
    /// size)` for that one call. Retiring many such ids one by one multiplies
    /// that sweep; use [`Self::reconcile_storage`], which performs it once
    /// for the whole set.
    ///
    /// # Arguments
    /// * `id` - The ID of the document to remove
    ///
    /// # Returns
    /// Ok(Some(Document)) if successful, or Ok(None) if the document was not found, or an error if removal fails
    ///
    /// # Errors
    /// Returns an error if:
    /// - The collection is in read-only mode
    /// - Any index update fails
    /// - Storage operations fail
    pub async fn remove(&self, id: DocumentId) -> Result<Option<Document>, DBError> {
        let _operation_lease = self.mutation_lease().await?;
        self.guarded("Collection::remove", self.remove_impl(id))
            .await
    }

    pub(super) async fn remove_impl(&self, id: DocumentId) -> Result<Option<Document>, DBError> {
        // Membership check is non-authoritative; the bitmap mutation below
        // serializes concurrent removes and is the source of truth.
        if !self.doc_ids.read().contains(&id) {
            return Ok(None);
        }

        // Serialize mutations of the same document (see `doc_locks`).
        let _doc_guard = self.doc_lock(id).lock().await;
        self.ensure_mutable()?;

        let now_ms = unix_ms();
        let path = Self::doc_path(id);

        // Best-effort fetch to drive index cleanup. A dead id (still in the
        // id set, but its object is already gone) is removed from the
        // in-memory state like a normal removal, with its index postings
        // swept by id. A stored document that no longer satisfies the schema
        // (e.g. legacy data a later validation tightening rejects) must stay
        // removable — it is the only in-band way out of that state — so it is
        // deleted with the same id sweep instead of value-keyed index cleanup.
        let mut undecodable = false;
        let mut dead = false;
        let doc = match self.storage.get::<DocumentOwned>(&path).await {
            Ok((doc, _)) => match Document::try_from_doc(self.schema(), doc) {
                Ok(doc) => Some(doc),
                Err(err) => {
                    log::warn!(
                        action = "Collection::remove",
                        collection = self.name,
                        doc_id = id;
                        "Removing document that does not match the schema; \
                         sweeping its index postings by id: {err:?}",
                    );
                    undecodable = true;
                    None
                }
            },
            Err(DBError::NotFound { .. }) => {
                dead = true;
                None
            }
            Err(err) => {
                log::warn!(
                    action = "Collection::remove",
                    collection = self.name,
                    doc_id = id;
                    "Failed to fetch document for removal, aborting: {err:?}",
                );
                return Err(err);
            }
        };

        let _exclusive = if doc.is_none() && self.has_unique_indexes() {
            let gate = self.unique_commit_gate.clone().write_owned().await;
            self.ensure_mutable()?;
            Some(gate)
        } else {
            None
        };
        let _keys = if let Some(doc) = &doc {
            self.unique_key_lease(&[doc]).await?
        } else {
            None
        };

        if let Some(doc) = &doc {
            self.record_mutation_intent(id, Some(doc), None).await?;
        }
        if undecodable || dead {
            self.record_mutation_intent(id, None, None).await?;
            // Neither case has indexed values to remove by key: an
            // undecodable document cannot be turned into them, and a dead id
            // has no document at all. Sweep the postings by id instead —
            // dropping only the id-set entry would leave phantom matches that
            // `query_ids` keeps returning and a unique key that keeps
            // rejecting new documents. No pre-image intent can be recorded
            // for either (the image would not decode on replay, or does not
            // exist) and a purge has no value-keyed rollback; a crash or
            // delete failure below leaves the object (if any) present but
            // unindexed, and re-running `remove` completes the deletion.
            self.purge_dead_ids_from_indexes(&BTreeSet::from([id]), now_ms);
        }

        let mut undo = Vec::new();

        // Phase 1: remove index entries while we still hold the original
        // contents. Record actual removals so a storage delete failure can
        // restore the in-memory indexes before returning.
        if let Some(doc) = &doc {
            for index in &self.btree_indexes {
                if let Some(fv) = self.index_hooks.btree_index_value(index, doc)
                    && fv.as_ref() != &FieldValue::Null
                    && index.remove(id, &fv, now_ms)
                {
                    undo.push(IndexUndo::BTreeChanged(index, fv, Cow::Owned(Fv::Null)));
                }
            }

            for index in &self.bm25_indexes {
                if let Some(text) = self.index_hooks.bm25_index_value(index, doc)
                    && index.remove(id, &text, now_ms)
                {
                    undo.push(IndexUndo::BM25Removed(index, text));
                }
            }

            for index in &self.hnsw_indexes {
                if let Some(vector) = self.index_hooks.hnsw_index_value(index, doc)
                    && index.remove(id, now_ms)
                {
                    undo.push(IndexUndo::HnswRemoved(index, vector));
                }
            }
        }

        // Phase 2: delete the document object before the bitmap so that a
        // failure here keeps the document visible (and recoverable) rather
        // than producing an orphan beyond the auto-repair scan window.
        if (doc.is_some() || undecodable)
            && let Err(err) = self.storage.delete(&path).await
        {
            self.rollback_indexes(id, undo, now_ms);
            log::error!(
                action = "Collection::remove",
                collection = self.name,
                doc_id = id;
                "Failed to delete document from storage: {err:?}",
            );
            // The DELETE outcome is unknown: the object may be gone while the
            // bitmap and indexes were just restored. The retained intent plus
            // a reopen complete the removal; this handle must not continue.
            self.poison("Collection::remove");
            return Err(err);
        }

        // Phase 3: finalise by updating the in-memory id set, which is the
        // source of truth for concurrent removes of the same id.
        let removed = self.unregister_doc_id(id);

        if removed {
            self.update_metadata(|meta| {
                meta.stats.last_deleted = now_ms;
                meta.stats.version += 1;
                meta.stats.delete_count += 1;
            });
        }

        Ok(doc)
    }

    pub(super) fn rollback_indexes(
        &self,
        id: DocumentId,
        undo: Vec<IndexUndo<'_>>,
        now_ms: u64,
    ) -> bool {
        let mut restored = true;
        for entry in undo.into_iter().rev() {
            let result = match entry {
                IndexUndo::BTreeAdded(index, value) => {
                    index.remove(id, &value, now_ms);
                    Ok(())
                }
                IndexUndo::BTreeChanged(index, old, new) => {
                    // Purge both images before restoring: the failing update
                    // may have inserted only part of an array, or none of it.
                    index.remove(id, &new, now_ms);
                    index.remove(id, &old, now_ms);
                    index.insert(id, &old, now_ms).map(|_| ())
                }
                IndexUndo::BM25Added(index, text) => {
                    index.remove(id, &text, now_ms);
                    Ok(())
                }
                IndexUndo::BM25Removed(index, text) => index.insert(id, &text, now_ms),
                IndexUndo::HnswAdded(index) => {
                    index.remove(id, now_ms);
                    Ok(())
                }
                IndexUndo::HnswRemoved(index, vector) => {
                    index.insert(id, vector.into_owned(), now_ms)
                }
            };
            if let Err(err) = result {
                restored = false;
                log::error!(
                    "Collection {:?}: failed to restore document {id} index: {err}",
                    self.name
                );
            }
        }
        if !restored {
            self.poison("Collection::rollback_indexes");
        }
        restored
    }
}
