//! Collection recovery implementation.
use super::*;

impl Collection {
    /// Fetches the stored documents of `ids` in order, `io_concurrency` at a
    /// time, bypassing the read cache: recovery and backfill scans must not
    /// evict the hot working set.
    pub(super) fn fetch_documents<I>(
        &self,
        ids: I,
    ) -> impl futures::Stream<Item = (DocumentId, Result<DocumentOwned, DBError>)> + '_
    where
        I: IntoIterator<Item = DocumentId>,
        I::IntoIter: 'static,
    {
        futures::stream::iter(ids)
            .map(move |id| async move {
                let result = self.storage.fetch(&Self::doc_path(id)).await;
                (id, result.map(|(doc, _)| doc))
            })
            .buffered(self.io_concurrency())
    }

    pub(super) fn remove_document_from_indexes(&self, id: DocumentId, doc: &Document, now_ms: u64) {
        for index in &self.btree_indexes {
            if let Some(value) = self.index_hooks.btree_index_value(index, doc)
                && value.as_ref() != &FieldValue::Null
            {
                index.remove(id, &value, now_ms);
            }
        }
        for index in &self.bm25_indexes {
            if let Some(text) = self.index_hooks.bm25_index_value(index, doc) {
                // BM25::remove is intentionally idempotent for replay: even
                // after doc_tokens was removed by an earlier historical
                // value, it still purges postings derived from `text`.
                index.remove(id, &text, now_ms);
            }
        }
        for index in &self.hnsw_indexes {
            index.remove(id, now_ms);
        }
    }

    /// Makes every index describe `doc` exactly as it is stored now.
    ///
    /// The value-keyed postings derived from `doc` are removed first and then
    /// re-inserted, so the call is idempotent for a document that already
    /// reached some or all index objects (a crash between an index flush and
    /// the storage checkpoint, or a replayed intent whose final state was
    /// partially flushed) and never reports its own surviving posting as a
    /// duplicate. Index failures stop recovery and are reported rather than
    /// hiding the conflict. A genuine conflict must be surfaced to the caller
    /// instead of silently certifying an incompletely indexed document.
    ///
    /// Each index's value is derived from `doc` **once** and drives both the
    /// removal and the insert. Going through
    /// [`Self::remove_document_from_indexes`] first would re-run every hook
    /// and re-tokenize every BM25 text a second time, which doubles the cost
    /// of a recovery scan over a large collection.
    pub(super) fn reindex_document(
        &self,
        id: DocumentId,
        doc: &Document,
        now_ms: u64,
    ) -> Result<(), DBError> {
        for index in &self.btree_indexes {
            if let Some(value) = self.index_hooks.btree_index_value(index, doc)
                && value.as_ref() != &FieldValue::Null
            {
                index.remove(id, &value, now_ms);
                index.insert(id, &value, now_ms)?;
            }
        }
        for index in &self.bm25_indexes {
            if let Some(text) = self.index_hooks.bm25_index_value(index, doc) {
                index.remove(id, &text, now_ms);
                index.insert(id, &text, now_ms)?;
            }
        }
        for index in &self.hnsw_indexes {
            index.remove(id, now_ms);
            if let Some(vector) = self.index_hooks.hnsw_index_value(index, doc) {
                index.insert(id, vector.into_owned(), now_ms)?;
            }
        }
        Ok(())
    }

    /// Writes an add/update/remove intent before either the in-memory indexes
    /// or the durable document object are changed.
    pub(super) async fn record_mutation_intent(
        &self,
        id: DocumentId,
        previous: Option<&Document>,
        proposed: Option<&Document>,
    ) -> Result<(), DBError> {
        loop {
            let sequence = self.next_mutation_sequence.fetch_add(1, Ordering::AcqRel);
            let intent = MutationIntentRef {
                sequence,
                document_id: id,
                previous,
                proposed,
                purge_by_id: previous.is_none() && proposed.is_none(),
            };
            let path = Self::mutation_intent_path(sequence);
            match self.storage.create_intent(&path, &intent).await {
                Ok(_) => {
                    self.pending_mutations.lock().insert(sequence);
                    return Ok(());
                }
                // A retained intent from an earlier process may use the same
                // wall-clock-derived sequence. Advance until a free path is
                // found instead of overwriting recovery evidence.
                Err(DBError::AlreadyExists { .. }) => continue,
                Err(err) => return Err(err),
            }
        }
    }

    /// Loads the subset of durable mutation intents that is safe to use for
    /// both schema-history recovery and mutation replay.
    ///
    /// A single unusable intent must never make the collection unopenable:
    /// nothing clears it, so every reopen would fail identically, with no
    /// operator escape hatch. Undecodable records, reserved document ids and
    /// path/sequence mismatches are logged and returned as stale paths for the
    /// next flush to retire.
    pub(super) async fn load_mutation_intents(
        &self,
    ) -> Result<(BTreeMap<u64, MutationIntent>, BTreeSet<String>), DBError> {
        let mut stream = self
            .storage
            .list_meta(Some(Self::MUTATION_INTENT_PREFIX), None)
            .map(|meta| async move {
                let meta = meta?;
                let relative = meta
                    .location
                    .prefix_match(self.storage.base_path())
                    .ok_or_else(|| DBError::Storage {
                        name: self.name.clone(),
                        source: "mutation intent outside collection prefix".into(),
                    })?;
                let path = relative.collect::<Path>().to_string();
                let result = self.storage.fetch::<MutationIntent>(&path).await;
                Ok::<_, DBError>((path, result))
            })
            .buffered(self.io_concurrency());
        let mut intents = BTreeMap::<u64, MutationIntent>::new();
        let mut stale_paths = BTreeSet::new();
        while let Some(item) = stream.next().await {
            let (path, result) = item?;
            let intent = match result {
                Ok((intent, _)) => intent,
                // An intent object that no longer decodes carries no usable
                // recovery information. Storage-level failures still
                // propagate: those are transient and retrying the open is the
                // right answer.
                Err(err @ DBError::Serialization { .. }) => {
                    log::warn!(
                        action = "Collection::load_mutation_intents",
                        collection = self.name;
                        "Skipping undecodable mutation intent: {err:?}",
                    );
                    stale_paths.insert(path);
                    continue;
                }
                Err(err) => return Err(err),
            };
            if intent.document_id == 0 {
                log::warn!(
                    action = "Collection::load_mutation_intents",
                    collection = self.name,
                    sequence = intent.sequence;
                    "Skipping mutation intent with the reserved document id 0",
                );
                stale_paths.insert(path);
                continue;
            }
            let expected_path = Self::mutation_intent_path(intent.sequence);
            if path != expected_path {
                log::warn!(
                    action = "Collection::load_mutation_intents",
                    collection = self.name,
                    sequence = intent.sequence,
                    path;
                    "Skipping mutation intent whose path does not match its sequence",
                );
                stale_paths.insert(path);
                continue;
            }
            intents.insert(intent.sequence, intent);
        }
        Ok((intents, stale_paths))
    }

    /// Replays intents left by a crash or failed flush. Historical values are
    /// removed first; the document currently present in storage is then the
    /// sole source of truth for both the bitmap and every derived index.
    /// Unusable records found by [`Self::load_mutation_intents`] are retired
    /// by the next flush.
    pub(super) async fn replay_mutation_intents(&self) -> Result<usize, DBError> {
        let (intents, stale_paths) = self.load_mutation_intents().await?;
        *self.stale_mutation_intents.lock() = stale_paths;
        if intents.is_empty() {
            return Ok(0);
        }

        if let Some(last) = intents.last_key_value().map(|(sequence, _)| *sequence) {
            self.next_mutation_sequence
                .fetch_max(last.saturating_add(1), Ordering::AcqRel);
        }
        *self.pending_mutations.lock() = intents.keys().copied().collect();

        self.reconcile_mutation_intents(&intents).await?;
        Ok(intents.len())
    }

    pub(super) async fn reconcile_mutation_intents(
        &self,
        intents: &BTreeMap<u64, MutationIntent>,
    ) -> Result<(), DBError> {
        let now_ms = unix_ms();
        let mut affected_ids = BTreeSet::new();
        let mut unindexable_image_ids = BTreeSet::new();
        for intent in intents.values() {
            affected_ids.insert(intent.document_id);
            if intent.purge_by_id {
                unindexable_image_ids.insert(intent.document_id);
            }
            for candidate in [&intent.previous, &intent.proposed].into_iter().flatten() {
                // A recorded pre/post image that no longer satisfies the
                // current schema (e.g. after an upgrade) cannot be turned back
                // into indexed values. Skip it rather than failing the open
                // forever: the stored document below stays authoritative, and
                // postings only the undecodable image could name are removed
                // by the id sweep below before the document is re-indexed.
                match Document::try_from_doc(self.schema(), candidate.clone()) {
                    Ok(document) => {
                        self.remove_document_from_indexes(intent.document_id, &document, now_ms)
                    }
                    Err(err) => {
                        unindexable_image_ids.insert(intent.document_id);
                        log::warn!(
                            action = "Collection::reconcile_mutation_intents",
                            collection = self.name,
                            doc_id = intent.document_id,
                            sequence = intent.sequence;
                            "Skipping mutation intent image that does not match the schema: {err:?}",
                        );
                    }
                }
            }
        }

        // Index removal is value-keyed, so a posting under a key derived from
        // an undecodable image would survive every later remove/re-add and
        // become a permanent phantom (a unique B-tree key then rejects new
        // documents with that value forever). Sweep those ids out of every
        // index first; the re-index below restores the postings the current
        // document actually owns.
        if !unindexable_image_ids.is_empty() {
            self.purge_dead_ids_from_indexes(&unindexable_image_ids, now_ms);
        }

        // Prefetch reads only. Apply recovered documents in id order, after
        // all historical postings have been removed, just as the serial path.
        let mut current_documents = self.fetch_documents(affected_ids);
        while let Some((id, current)) = current_documents.next().await {
            match current {
                Ok(current) => {
                    // Same tolerance as `repair_document`: a stored document
                    // that does not match the schema is skipped (leaving the
                    // bitmap untouched) instead of bricking every open.
                    let current = match Document::try_from_doc(self.schema(), current) {
                        Ok(current) => current,
                        Err(err) => {
                            self.max_document_id.fetch_max(id, Ordering::AcqRel);
                            log::warn!(
                                action = "Collection::reconcile_mutation_intents",
                                collection = self.name,
                                doc_id = id;
                                "Skipping document that does not match the schema: {err:?}",
                            );
                            continue;
                        }
                    };
                    // The final state may already have reached some index
                    // objects during a partial flush; `reindex_document`
                    // removes it before the insert so unique indexes cannot
                    // reject their own surviving posting.
                    self.reindex_document(id, &current, now_ms)?;
                    self.max_document_id.fetch_max(id, Ordering::AcqRel);
                    self.register_doc_id(id);
                }
                Err(DBError::NotFound { .. }) => {
                    // Complete a crashed remove. HNSW can be purged by id even
                    // when no historical vector could be decoded.
                    for index in &self.hnsw_indexes {
                        index.remove(id, now_ms);
                    }
                    self.unregister_doc_id(id);
                }
                Err(err) => return Err(err),
            }
        }

        self.update_metadata(|meta| meta.stats.version += 1);
        Ok(())
    }

    pub(super) async fn clear_mutation_intents(&self) -> Result<(), DBError> {
        let sequences: Vec<_> = self.pending_mutations.lock().iter().copied().collect();
        let stale: Vec<_> = self.stale_mutation_intents.lock().iter().cloned().collect();
        let paths = sequences
            .into_iter()
            .map(|seq| (Self::mutation_intent_path(seq), Some(seq)))
            .chain(stale.into_iter().map(|path| (path, None)));
        let mut deletes = futures::stream::iter(paths)
            .map(|(path, seq)| async move {
                match self.storage.delete(&path).await {
                    Ok(()) | Err(DBError::NotFound { .. }) => {
                        if let Some(seq) = seq {
                            self.pending_mutations.lock().remove(&seq);
                        } else {
                            self.stale_mutation_intents.lock().remove(&path);
                        }
                        Ok(())
                    }
                    Err(err) => Err(err),
                }
            })
            .buffer_unordered(self.io_concurrency());
        // Drain every admitted delete before returning a failure.
        let mut error = None;
        while let Some(result) = deletes.next().await {
            if let Err(err) = result {
                error.get_or_insert(err);
            }
        }
        error.map_or(Ok(()), Err)
    }

    /// Reconciles the in-memory state with the document objects actually
    /// present in storage, in both directions:
    ///
    /// - documents on disk that are missing from the id bitmap are recovered
    ///   (added to the bitmap and re-indexed), and
    /// - bitmap ids without a backing object are dropped.
    ///
    /// This is an explicit **maintenance API**: unlike the bounded
    /// crash-recovery scan that runs on open ([`Self::auto_repair_indexes`],
    /// which probes only the checkpoint-to-allocation-watermark window), it
    /// lists the entire `data/` prefix — O(number of documents) in listing
    /// cost. An exclusive operation lease drains writers before the scan.
    /// Use it from an admin task when document counts look inconsistent.
    ///
    /// Returns `(recovered, dropped)`: documents recovered into the bitmap
    /// and dead ids removed from it. Changes are persisted by the next
    /// `flush()`.
    pub async fn reconcile_storage(&self) -> Result<(usize, usize), DBError> {
        self.ensure_recovered().await?;
        let _operation_lease = self.operation_gate.clone().write_owned().await;
        self.ensure_mutable()?;
        self.guarded(
            "Collection::reconcile_storage",
            self.reconcile_storage_impl(),
        )
        .await
    }

    pub(super) async fn reconcile_storage_impl(&self) -> Result<(usize, usize), DBError> {
        let now_ms = unix_ms();
        // Enumerate the ids of every document object under `data/`.
        let mut stored_ids: BTreeSet<DocumentId> = BTreeSet::new();
        {
            let mut stream = self.storage.list_meta(Some("data/"), None);
            while let Some(meta) = stream.next().await {
                let meta = meta?;
                if let Some(id) = meta
                    .location
                    .filename()
                    .and_then(|name| name.strip_suffix(".cbor"))
                    .and_then(|raw| raw.parse::<DocumentId>().ok())
                {
                    stored_ids.insert(id);
                }
            }
        }

        // Direction 1: recover documents that exist on disk but are missing
        // from the bitmap.
        let missing_in_bitmap: Vec<DocumentId> = {
            let doc_ids = self.doc_ids.read();
            stored_ids
                .iter()
                .copied()
                .filter(|id| !doc_ids.contains(id))
                .collect()
        };
        let mut recovered = 0usize;
        let mut documents = self.fetch_documents(missing_in_bitmap);
        while let Some((id, result)) = documents.next().await {
            match result {
                Ok(doc) => {
                    if self.repair_document(id, doc, now_ms)? {
                        recovered += 1;
                    }
                }
                // Deleted between listing and fetch; nothing to recover.
                Err(DBError::NotFound { .. }) => {}
                Err(err) => return Err(err),
            }
        }

        // No mutation can race this exclusive scan.
        let dead_ids: BTreeSet<DocumentId> = {
            let doc_ids = self.doc_ids.read();
            doc_ids
                .iter()
                .copied()
                .filter(|id| !stored_ids.contains(id))
                .collect()
        };
        let dropped = dead_ids.len();
        self.heal_missing_docs(&dead_ids, now_ms);

        if recovered > 0 || dropped > 0 {
            log::warn!(
                action = "Collection::reconcile_storage",
                collection = self.name,
                recovered = recovered,
                dropped = dropped;
                "Reconciled collection with storage: recovered={recovered}, dropped={dropped}",
            );
        }

        Ok((recovered, dropped))
    }

    /// Crash-recovery scan run on open, after mutation-intent replay.
    ///
    /// Every id that may have a document object lies in
    /// `checkpoint+1 ..= max(max_document_id, allocation watermark)`: an add
    /// publishes the durable watermark before its document object can exist
    /// (see [`Self::ensure_allocation_watermark`]). The scan probes that
    /// exact window — holes (failed or cancelled adds, removed documents)
    /// read as one cheap NotFound each — so no consecutive-miss heuristics
    /// are needed and no committed document can be skipped. The window is
    /// bounded by the mutations since the last successful flush plus one
    /// watermark stride.
    pub(super) async fn auto_repair_indexes(&self) -> Result<usize, DBError> {
        let check_point = self.storage.stats().check_point;
        let scan_max = self
            .max_document_id
            .load(Ordering::Acquire)
            .max(self.durable_alloc_watermark.load(Ordering::Acquire));

        let now_ms = unix_ms();
        let mut fixed = 0;
        let mut documents = self.fetch_documents(
            check_point
                .checked_add(1)
                .into_iter()
                .flat_map(move |start| start..=scan_max),
        );
        while let Some((id, result)) = documents.next().await {
            match result {
                Err(DBError::NotFound { .. }) => {}
                Err(err @ DBError::Serialization { .. }) => {
                    self.max_document_id.fetch_max(id, Ordering::AcqRel);
                    self.recovery_issues.write().insert(id, err.to_string());
                    log::warn!(
                        "Collection {:?}: skipped corrupt document {id}: {err}",
                        self.name
                    );
                }
                // A transient failure cannot be certified as recovered by a
                // later checkpoint. Abort, leaving all durable recovery data.
                Err(err) => return Err(err),
                Ok(doc) => {
                    if self.repair_document(id, doc, now_ms)? {
                        fixed += 1;
                    }
                }
            }
        }

        if fixed > 0 {
            // Make the recovery observable to the version watermark so the
            // flush that follows in the open path persists the repaired
            // bitmap instead of taking the no-change fast path.
            self.update_metadata(|meta| meta.stats.version += 1);
        }

        Ok(fixed)
    }

    /// Registers a document found in storage into the in-memory id set and
    /// re-indexes it. Index failures propagate so recovery cannot certify a
    /// document whose unique keys or other index entries were not restored.
    ///
    /// Returns `true` when the id was missing from the id set (i.e. an
    /// orphan was recovered).
    pub(super) fn repair_document(
        &self,
        id: DocumentId,
        doc: DocumentOwned,
        now_ms: u64,
    ) -> Result<bool, DBError> {
        // Keep the id allocator above every observed object even when the
        // document is skipped below, so future adds cannot collide with it.
        self.max_document_id.fetch_max(id, Ordering::AcqRel);

        // A document that no longer matches the schema must not brick the
        // whole collection open (index insert failures below are likewise
        // only logged). Skip it without registering; it stays recoverable
        // via `reconcile_storage` after the schema or the object is fixed.
        let doc = match Document::try_from_doc(self.schema(), doc) {
            Ok(doc) => doc,
            Err(err) => {
                self.recovery_issues.write().insert(id, err.to_string());
                log::warn!(
                    action = "Collection::repair_document",
                    collection = self.name,
                    doc_id = id;
                    "Skipping document that does not match the schema: {err:?}",
                );
                return Ok(false);
            }
        };

        self.reindex_document(id, &doc, now_ms)?;
        let is_new = self.register_doc_id(id);

        if is_new {
            self.update_metadata(|meta| {
                meta.stats.version += 1;
            });
        }

        Ok(is_new)
    }

    pub(super) async fn try_upgrade_schema(
        &mut self,
        mut new_schema: Schema,
    ) -> Result<bool, DBError> {
        if !new_schema.needs_upgrade(&self.schema) {
            return Ok(false);
        }

        // Once a retired value is pruned while materializing a document, it
        // cannot drive value-keyed index removal. Reject before persisting the
        // new schema; callers can reopen under the old schema to drop indexes.
        for (index, fields) in self
            .btree_indexes
            .iter()
            .map(|i| (i.name(), i.virtual_field()))
            .chain(
                self.bm25_indexes
                    .iter()
                    .map(|i| (i.name(), i.virtual_field())),
            )
        {
            for field in fields {
                if new_schema.get_field(field).is_none() {
                    return Err(self.retired_index_field_error(index, field));
                }
            }
        }
        for index in &self.hnsw_indexes {
            if new_schema.get_field(index.field_name()).is_none() {
                return Err(self.retired_index_field_error(index.name(), index.field_name()));
            }
        }

        let mut old_schema = self.schema.clone();
        if !old_schema.has_upgrade_history() {
            // Recover before changing the schema, and before replay/pruning
            // can erase retired indexes or nested keys. Include unregistered
            // documents as well as both images of every durable intent.
            // A failed scan leaves the collection metadata untouched.
            let mut recovery = old_schema.history_recovery();
            let mut documents = self.storage.list::<DocumentOwned>(Some("data/"), None);
            while let Some(document) = documents.next().await {
                recovery.observe(&document?.0)?;
            }
            let (intents, _) = self.load_mutation_intents().await?;
            for intent in intents.values() {
                for image in [intent.previous.as_ref(), intent.proposed.as_ref()]
                    .into_iter()
                    .flatten()
                {
                    recovery.observe(image)?;
                }
            }
            old_schema = Arc::new(recovery.finish());
        }
        new_schema.upgrade_with(&old_schema)?;
        self.schema = Arc::new(new_schema.clone());
        self.update_metadata(|m| {
            m.schema = new_schema;
            m.stats.version += 1;
        });

        log::warn!(
            action = "Collection::upgrade_schema",
            collection = self.name,
            version = self.schema.version();
            "Schema upgraded to version {}",
            self.schema.version()
        );
        Ok(true)
    }

    fn retired_index_field_error(&self, index: &str, field: &str) -> DBError {
        DBError::Schema {
            name: self.name.clone(),
            source: format!("cannot retire field {field:?} while index {index:?} references it; remove the index under the current schema before upgrading").into(),
        }
    }
}
