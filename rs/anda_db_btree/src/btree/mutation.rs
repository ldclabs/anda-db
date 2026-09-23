use super::*;

impl<PK: BTreeKey, FV: BTreeKey> BTreeIndex<PK, FV> {
    // Shared posting primitives: callers keep their own batching/packing
    // strategy, but uniqueness, versions and remove-last semantics agree.
    pub(super) fn append_posting(
        &self,
        posting: &mut Posting<PK>,
        doc_id: &PK,
        field_value: &FV,
        doc_size: usize,
    ) -> Result<usize, BTreeError> {
        if !self.config.allow_duplicates && !posting.docs.contains(doc_id) {
            return Err(BTreeError::AlreadyExists {
                name: self.name.clone(),
                id: json_value(doc_id),
                value: json_value(field_value),
            });
        }
        if posting.docs.push(doc_id.clone()) {
            Ok(doc_size)
        } else {
            Ok(0)
        }
    }

    pub(super) fn remove_posting_id(&self, doc_id: &PK, field_value: FV) -> Option<Removal<FV>> {
        // Keep the final ID removal and entry removal under the same shard
        // lock. Otherwise queries can see an empty posting, and a concurrent
        // unique insert mistakes that empty entry for a conflicting owner.
        let mut removed = None;
        let entry_removed = self
            .postings
            .remove_if_mut(&field_value, |_, posting| {
                // Size the whole entry only when this removal will empty it.
                let full_size = match posting.docs.as_slice() {
                    [only] if only == doc_id => posting_entry_size(&field_value, &*posting),
                    _ => 0,
                };
                if posting.docs.remove(doc_id).is_none() {
                    return false;
                }
                let empty = posting.docs.is_empty();
                let size_decrease = if empty {
                    full_size
                } else {
                    cbor_serialized_size(doc_id) + 2
                };
                removed = Some((posting.bucket_id, size_decrease));
                empty
            })
            .is_some();
        removed.map(|(bucket_id, size_decrease)| Removal {
            field_value,
            bucket_id,
            size_decrease,
            entry_removed,
        })
    }

    // Called with the bucket lock, never with a posting write guard. A posting
    // may have been recreated/migrated since the caller recorded its old owner.
    pub(super) fn detach_bucket_key(
        &self,
        id: u32,
        bucket: &mut BucketState<FV>,
        key: &FV,
    ) -> bool {
        if self.postings.get(key).is_some_and(|p| p.bucket_id == id) {
            return false;
        }
        bucket.fields.remove(key)
    }

    /// Inserts a document_id-field_value pair to the index
    ///
    /// # Arguments
    ///
    /// * `doc_id` - Document identifier
    /// * `field_value` - Key to index
    /// * `now_ms` - Current timestamp in milliseconds
    ///
    /// # Returns
    ///
    /// * `Ok(bool)` if the document_id-field_value pair was successfully added
    /// * `Err(BTreeError)` if failed
    pub fn insert(&self, doc_id: PK, field_value: FV, now_ms: u64) -> Result<bool, BTreeError> {
        self.ensure_ready()?;
        // Shared with other mutations, exclusive against `compact_buckets`.
        let _mutation_guard = self.mutation_gate.read();

        // Validate `doc_id` serialization up-front, before any state is
        // mutated, so a failing `Serialize` impl surfaces as an error instead
        // of a panic (and never leaves a half-applied insert behind).
        let doc_id_size =
            try_cbor_serialized_size(&doc_id).map_err(|err| self.serialization_error(err))? + 2;

        // Inserting between load_metadata() and load_buckets() is NOT supported:
        // the load overwrites postings by design (see `load_buckets`).
        let bucket = self.current_bucket();

        // Calculate the size increase for this insertion
        let mut is_new = false;
        let mut size_increase;
        let mut appended_existing_posting = false;
        let mut target_bucket = bucket;
        match self.postings.entry(field_value.clone()) {
            dashmap::Entry::Occupied(mut entry) => {
                let posting = entry.get_mut();
                target_bucket = posting.bucket_id;

                size_increase = self.append_posting(posting, &doc_id, &field_value, doc_id_size)?;
                appended_existing_posting = size_increase > 0;
            }
            dashmap::Entry::Vacant(entry) => {
                // Create a new posting for this field value
                let posting = Posting::new(bucket, doc_id.clone());
                // Reject an unserializable field value before inserting it:
                // nothing has been mutated yet, so returning here is clean.
                size_increase = try_posting_entry_size(&field_value, &posting)
                    .map_err(|err| self.serialization_error(err))?;
                entry.insert(posting);
                is_new = true;
            }
        };

        if is_new {
            // Add the field value to the B-tree for range queries.
            //
            // Re-check the posting inside the btree lock: a concurrent `remove`
            // may have already deleted the just-created posting, and its btree
            // cleanup (which also takes the btree lock, see
            // `remove_btree_key_if_posting_absent`) found nothing to remove.
            // Inserting unconditionally would leave a phantom key in the btree
            // with no backing posting.
            let mut btree = self.btree.write();
            if self.postings.contains_key(&field_value) {
                btree.insert(field_value.clone());
            }
        }

        // If the index was modified, update bucket state
        let mut new_bucket = 0;
        if size_increase > 0 {
            // Update bucket state
            let mut b = self.buckets.entry(target_bucket).or_default();

            // Check if the bucket has enough space. An existing posting the
            // bucket already lists is governed by `member_posting_stays`,
            // which `insert_array` applies to the same decision.
            let fits = b.fields.is_empty()
                || if appended_existing_posting && b.fields.contains(&field_value) {
                    self.member_posting_stays(&b, size_increase)
                } else {
                    b.size.saturating_add(size_increase) < self.config.bucket_overload_size
                };
            if fits {
                b.size = b.size.saturating_add(size_increase);
                // Mark as dirty, needs to be persisted
                self.mark_bucket_dirty(&mut b);
                // Add field value to bucket if not already present
                b.fields.insert(field_value.clone());
            } else {
                // If the current bucket is full, create a new one.
                //
                // Known benign drift: this migration writes `posting.bucket_id =
                // new_bucket` before the destination bucket entry is created
                // below. A concurrent `remove` that empties the posting in
                // that window skips its bucket accounting (the destination
                // entry does not exist yet), so the in-memory bucket size may
                // over-count and `bucket.fields` may keep the field value until the
                // next compaction. This only degrades packing decisions —
                // flush filters postings through the live `postings` map, so
                // the persisted state stays correct — and all size arithmetic
                // saturates.
                let mut source_size_decrease = 0;
                new_bucket = self.max_bucket_id.fetch_add(1, Ordering::Relaxed) + 1;
                {
                    if let Some(mut posting) = self.postings.get_mut(&field_value) {
                        // Update the posting's bucket ID
                        // The source bucket tracked this posting WITHOUT the
                        // just-appended doc_id (the migration path never added
                        // `size_increase` to it), so reclaim the exact
                        // pre-insert size. Compute this only on migration: CBOR
                        // sequence length and integer width can grow at
                        // boundaries such as 23 -> 24, so subtracting only the
                        // appended doc_id from the post-insert size is not exact.
                        source_size_decrease = if appended_existing_posting {
                            previous_posting_size_after_append(
                                &field_value,
                                target_bucket,
                                &posting.docs,
                            )
                        } else {
                            0
                        };

                        posting.bucket_id = new_bucket;
                        let migrated_posting_size = posting_entry_size(&field_value, &*posting);
                        size_increase = migrated_posting_size;
                    } else {
                        size_increase = 0;
                        new_bucket = 0;
                    }
                }
                // Remove the current field value from the current bucket
                // The freed space can still accommodate small growth in other field values
                if self.detach_bucket_key(target_bucket, &mut b, &field_value) {
                    b.size = b.size.saturating_sub(source_size_decrease);
                    // Source bucket must be marked dirty, otherwise stale on-disk
                    // entries may survive and be resurrected after restart.
                    self.mark_bucket_dirty(&mut b);
                }
            }
        }

        if new_bucket > 0 {
            // Create a new bucket and migrate this data to it
            self.register_in_new_bucket(new_bucket, size_increase, field_value);
        }

        if size_increase > 0 {
            self.update_metadata(|m| {
                m.stats.version += 1;
                m.stats.last_inserted = now_ms;
                m.stats.insert_count += 1;
            });
        }

        Ok(size_increase > 0)
    }

    /// Removes a document_id-field_value pair from the index
    ///
    /// # Arguments
    ///
    /// * `doc_id` - Document identifier
    /// * `field_value` - field to remove
    /// * `now_ms` - Current timestamp in milliseconds
    ///
    /// # Returns
    ///
    /// * `bool` - `true` if the document_id-field_value pair was successfully removed, `false` otherwise
    pub fn remove(&self, doc_id: PK, field_value: FV, now_ms: u64) -> bool {
        if self.load_state != LoadState::Ready {
            return false;
        }
        // Shared with other mutations, exclusive against `compact_buckets`.
        let _mutation_guard = self.mutation_gate.read();

        let Some(removal) = self.remove_posting_id(&doc_id, field_value) else {
            return false;
        };
        if removal.entry_removed {
            self.remove_btree_key_if_posting_absent(&removal.field_value);
        }
        if let Some(mut bucket) = self.buckets.get_mut(&removal.bucket_id) {
            bucket.size = bucket.size.saturating_sub(removal.size_decrease);
            self.mark_bucket_dirty(&mut bucket);
            if removal.entry_removed {
                self.detach_bucket_key(removal.bucket_id, &mut bucket, &removal.field_value);
            }
        }
        self.update_metadata(|m| {
            m.stats.version += 1;
            m.stats.last_deleted = now_ms;
            m.stats.delete_count += 1;
        });
        true
    }

    /// Batch-inserts `(doc_id, field_value)` pairs sharing the same `doc_id`.
    ///
    /// This is materially more efficient than calling [`Self::insert`] in a
    /// loop: bucket size tracking and B-tree key insertion are amortised, and
    /// the posting lock is acquired once per field value.
    ///
    /// The operation proceeds in three phases:
    ///
    /// 1. **Posting update** — for each field value, either append `doc_id` to
    ///    the existing posting or create a fresh one. Per-bucket size deltas
    ///    are accumulated.
    /// 2. **Bucket accounting** — for every affected bucket, apply the
    ///    aggregate delta. Newly created postings remain in the bucket when it
    ///    still has room, otherwise they are scheduled for migration. An
    ///    existing posting that grew past the limit is scheduled too when it
    ///    shares the bucket with other postings, so a hot posting ends up
    ///    isolated instead of dragging its neighbours into every rewrite; one
    ///    that already fills the bucket by itself grows in place (see
    ///    [`Self::insert`]).
    /// 3. **Migration** — scheduled postings are moved to freshly allocated
    ///    buckets; both source and destination buckets are marked dirty so a
    ///    crash cannot resurrect stale data.
    ///
    /// # Arguments
    ///
    /// * `doc_id` - Document identifier
    /// * `field_values` - Field values to index for this document. Duplicates
    ///   are coalesced.
    /// * `now_ms` - Current timestamp in milliseconds
    ///
    /// # Returns
    ///
    /// Number of new `(doc_id, field_value)` associations actually created.
    /// Idempotent calls return `0`.
    ///
    /// # Errors
    ///
    /// Returns [`BTreeError::AlreadyExists`] when `allow_duplicates` is `false`
    /// and one of the field values already maps to a different `doc_id`.
    /// In the sequential case this is rejected by a pre-check before any
    /// mutation. If the conflict only appears mid-loop (a concurrent writer
    /// added a conflicting `doc_id` after the pre-check), associations created
    /// for field values processed before the conflicting one remain applied,
    /// with consistent internal bookkeeping; values after it are not processed.
    pub fn insert_array(
        &self,
        doc_id: PK,
        field_values: Vec<FV>,
        now_ms: u64,
    ) -> Result<usize, BTreeError> {
        self.ensure_ready()?;
        if field_values.is_empty() {
            return Ok(0);
        }

        // Shared with other mutations, exclusive against `compact_buckets`.
        let _mutation_guard = self.mutation_gate.read();

        // Validate `doc_id` serialization up-front, before any state is
        // mutated (see `insert`).
        let doc_id_size =
            try_cbor_serialized_size(&doc_id).map_err(|err| self.serialization_error(err))? + 2;

        // Track which values were successfully inserted
        let mut inserted_count = 0;
        // Track which buckets were modified and need updates
        let mut bucket_updates: FxHashMap<u32, (usize, FxHashSet<FV>)> = FxHashMap::default();
        // New values that need to be added to the B-tree
        let mut new_btree_values = Vec::new();

        // Phase 1: collect existing postings and prepare modifications
        // Skip duplicate field values if not allowed
        if !self.config.allow_duplicates {
            for field_value in &field_values {
                if let Some(posting) = self.postings.get(field_value)
                    && !posting.docs.contains(&doc_id)
                {
                    return Err(BTreeError::AlreadyExists {
                        name: self.name.clone(),
                        id: json_value(&doc_id),
                        value: json_value(field_value),
                    });
                }
            }
        }

        let bucket_id = self.current_bucket();

        // An error detected mid-loop (uniqueness violation, or a field value
        // whose serialization fails) must NOT return early: postings already
        // modified in this call still need their btree keys and bucket
        // accounting (phases below), otherwise they would be invisible to
        // range queries and silently dropped by the next flush. Record the
        // error, stop processing further values, finish the bookkeeping for
        // what was applied, then surface the error.
        let mut deferred_error: Option<BTreeError> = None;

        for field_value in field_values {
            let size_increase;
            let mut target_bucket_id = bucket_id;
            match self.postings.entry(field_value.clone()) {
                dashmap::Entry::Occupied(mut entry) => {
                    let posting = entry.get_mut();
                    // Track the posting's actual bucket, not the current max_bucket_id
                    target_bucket_id = posting.bucket_id;

                    match self.append_posting(posting, &doc_id, &field_value, doc_id_size) {
                        Ok(size) => size_increase = size,
                        Err(err) => {
                            deferred_error = Some(err);
                            break;
                        }
                    }
                }
                dashmap::Entry::Vacant(entry) => {
                    // Create a new posting for this field value
                    let posting = Posting::new(bucket_id, doc_id.clone());
                    // Reject an unserializable field value before inserting
                    // it; nothing has been mutated for this value yet, so
                    // stop the loop and surface the error after finishing the
                    // bookkeeping for the values already applied.
                    match try_posting_entry_size(&field_value, &posting) {
                        Ok(size) => size_increase = size,
                        Err(err) => {
                            deferred_error = Some(self.serialization_error(err));
                            break;
                        }
                    }
                    // Insert the new posting
                    entry.insert(posting);
                    // Remember to add this to the B-tree for range queries
                    new_btree_values.push(field_value.clone());
                }
            };

            if size_increase > 0 {
                // Update the bucket size tracking for the posting's actual bucket
                let bucket_entry = bucket_updates
                    .entry(target_bucket_id)
                    .or_insert_with(|| (0, FxHashSet::default()));
                bucket_entry.0 += size_increase;
                bucket_entry.1.insert(field_value);
                inserted_count += 1;
            }
        }

        // Add all new values to the B-tree in a single operation.
        // Same phantom-key guard as in `insert`: skip keys whose posting was
        // concurrently removed between posting creation and this point.
        if !new_btree_values.is_empty() {
            let mut btree = self.btree.write();
            for field_value in new_btree_values {
                if self.postings.contains_key(&field_value) {
                    btree.insert(field_value);
                }
            }
        }

        // Phase 2: handle bucket overflow and updates
        // Process each field value individually to avoid migrating existing values unnecessarily.
        // field_values_to_migrate: (old_bucket_id, field_value, size)
        let mut field_values_to_migrate: Vec<(u32, FV, usize)> = Vec::new();
        for (bucket_id, (size_delta, field_values)) in bucket_updates {
            let mut bucket_entry = self.buckets.entry(bucket_id).or_default();

            self.mark_bucket_dirty(&mut bucket_entry);
            // Apply the aggregate delta computed in Phase 1. This covers both:
            //   * full posting size for newly-created postings, and
            //   * the per-doc_id growth for postings already living in this bucket.
            // Per-fv branches below only deal with placement of new postings.
            bucket_entry.size = bucket_entry.size.saturating_add(size_delta);

            for fv in field_values {
                let is_member = bucket_entry.fields.contains(&fv);
                if is_member && self.member_posting_stays(&bucket_entry, 0) {
                    // Existing posting whose growth was already folded into
                    // `size_delta` above, and which `member_posting_stays`
                    // keeps here. Skipping it also keeps this path free of the
                    // O(n) CBOR pass over the whole posting.
                    continue;
                }

                // A newly-created posting, or an existing one that outgrew a
                // shared bucket; decide from the live posting where it goes.
                //
                // Known benign drift: `fv_size` is recomputed here from the
                // current posting state, which under concurrent writers may
                // differ from the delta accumulated in Phase 1 (so the
                // rollback below can be slightly off). This only degrades
                // packing decisions; the persisted state stays correct and
                // all size arithmetic saturates.
                let fv_size = if let Some(posting) = self.postings.get(&fv) {
                    posting_entry_size(&fv, &*posting)
                } else {
                    // Posting was concurrently removed; nothing more to do.
                    continue;
                };

                if !is_member
                    && (bucket_entry.fields.is_empty()
                        || bucket_entry.size < self.config.bucket_overload_size)
                {
                    // Bucket has room (size already includes this fv via size_delta).
                    bucket_entry.fields.insert(fv);
                } else {
                    // Bucket is over the soft limit; migrate this fv to a fresh
                    // bucket. A new posting was never listed by this bucket, so
                    // roll back the size tentatively added for it here; a
                    // listed posting is detached — and its size reclaimed — in
                    // Phase 3.
                    if !is_member {
                        bucket_entry.size = bucket_entry.size.saturating_sub(fv_size);
                    }
                    field_values_to_migrate.push((bucket_id, fv, fv_size));
                }
            }
        }

        // Phase 3: Create new buckets if needed
        if !field_values_to_migrate.is_empty() {
            let mut next_bucket_id = self.max_bucket_id.fetch_add(1, Ordering::Relaxed) + 1;

            {
                self.buckets.entry(next_bucket_id).or_default();
                // release the lock on the entry
            }

            for (old_bucket_id, field_value, size) in field_values_to_migrate {
                if let Some(mut posting) = self.postings.get_mut(&field_value) {
                    posting.bucket_id = next_bucket_id;
                }

                if let Some(mut ob) = self.buckets.get_mut(&old_bucket_id)
                    && self.detach_bucket_key(old_bucket_id, &mut ob, &field_value)
                {
                    ob.size = ob.size.saturating_sub(size);
                    // Source bucket must be marked dirty, see insert() migration path.
                    self.mark_bucket_dirty(&mut ob);
                }

                let mut new_bucket = false;
                {
                    // entry().or_insert_with() instead of get_mut(): the bucket
                    // normally exists, but if it ever went missing the posting
                    // would silently stop being tracked by any bucket and be
                    // lost on the next reload.
                    let mut nb = self.buckets.entry(next_bucket_id).or_default();
                    if nb.fields.is_empty()
                        || nb.size.saturating_add(size) < self.config.bucket_overload_size
                    {
                        // Bucket has enough space, update directly
                        nb.size = nb.size.saturating_add(size);
                        self.mark_bucket_dirty(&mut nb);
                        nb.fields.insert(field_value.clone());
                    } else {
                        // Bucket doesn't have enough space, need to migrate to the next bucket
                        new_bucket = true;
                    }
                }

                if new_bucket {
                    next_bucket_id = self.max_bucket_id.fetch_add(1, Ordering::Relaxed) + 1;
                    // update the posting's bucket_id again
                    if let Some(mut posting) = self.postings.get_mut(&field_value) {
                        posting.bucket_id = next_bucket_id;
                    }
                    self.register_in_new_bucket(next_bucket_id, size, field_value);
                }
            }
        }

        // Update metadata if any items were inserted
        if inserted_count > 0 {
            self.update_metadata(|m| {
                m.stats.version += 1;
                m.stats.last_inserted = now_ms;
                m.stats.insert_count += inserted_count as u64;
            });
        }

        if let Some(err) = deferred_error {
            return Err(err);
        }

        Ok(inserted_count)
    }

    /// Batch removes multiple document_id-field_value pairs from the index
    ///
    /// This method is more efficient than calling remove() multiple times
    /// as it can optimize bucket updates and reduce lock contention.
    ///
    /// # Arguments
    ///
    /// * `doc_id` - Document identifier
    /// * `field_values` - Array of field values to remove for this document
    /// * `now_ms` - Current timestamp in milliseconds
    ///
    /// # Returns
    ///
    /// * `usize` - Number of items successfully removed
    pub fn remove_array(&self, doc_id: PK, field_values: Vec<FV>, now_ms: u64) -> usize {
        if self.load_state != LoadState::Ready {
            return 0;
        }
        if field_values.is_empty() {
            return 0;
        }

        // Shared with other mutations, exclusive against `compact_buckets`.
        let _mutation_guard = self.mutation_gate.read();

        let mut removed_count = 0;
        let mut entries_removed = FxHashSet::default();
        let mut bucket_updates: FxHashMap<u32, (usize, FxHashSet<FV>)> = FxHashMap::default();
        for value in field_values {
            let Some(removal) = self.remove_posting_id(&doc_id, value) else {
                continue;
            };
            removed_count += 1;
            if removal.entry_removed {
                entries_removed.insert(removal.field_value.clone());
            }
            let bucket = bucket_updates.entry(removal.bucket_id).or_default();
            bucket.0 = bucket.0.saturating_add(removal.size_decrease);
            bucket.1.insert(removal.field_value);
        }

        self.remove_btree_keys_if_postings_absent(&entries_removed);

        // Update all modified buckets
        for (bucket_id, (size_decrease, field_values)) in bucket_updates {
            if let Some(mut bucket) = self.buckets.get_mut(&bucket_id) {
                bucket.size = bucket.size.saturating_sub(size_decrease);
                self.mark_bucket_dirty(&mut bucket); // Mark as dirty

                // Remove field values that are completely removed
                for fv in &field_values {
                    if entries_removed.contains(fv) {
                        self.detach_bucket_key(bucket_id, &mut bucket, fv);
                    }
                }
            }
        }

        // Update metadata if any items were removed
        if removed_count > 0 {
            self.update_metadata(|m| {
                m.stats.version += 1;
                m.stats.last_deleted = now_ms;
                m.stats.delete_count += removed_count as u64;
            });
        }

        removed_count
    }

    /// Batch updates the index for a document
    ///
    /// # Arguments
    ///
    /// * `doc_id` - doc ID
    /// * `old_field_values` - old field values (without duplicates)
    /// * `new_field_values` - new field values (without duplicates)
    /// * `now_ms` - current timestamp (milliseconds)
    ///
    /// # Returns
    /// * `Result<(usize, usize), BTreeError>` - (removed count, inserted count)
    pub fn batch_update(
        &self,
        doc_id: PK,
        old_field_values: Vec<FV>,
        new_field_values: Vec<FV>,
        now_ms: u64,
    ) -> Result<(usize, usize), BTreeError> {
        self.ensure_ready()?;
        // 去重
        let old_set: FxHashSet<_> = old_field_values.into_iter().collect();
        let new_set: FxHashSet<_> = new_field_values.into_iter().collect();

        // 需要插入的值 = 新集合 - 旧集合
        let to_insert: Vec<_> = new_set.difference(&old_set).cloned().collect();
        // 需要删除的值 = 旧集合 - 新集合
        let to_remove: Vec<_> = old_set.difference(&new_set).cloned().collect();

        let inserted = if !to_insert.is_empty() {
            self.insert_array(doc_id.clone(), to_insert, now_ms)?
        } else {
            0
        };

        let removed = if !to_remove.is_empty() {
            self.remove_array(doc_id, to_remove, now_ms)
        } else {
            0
        };

        Ok((removed, inserted))
    }
}
