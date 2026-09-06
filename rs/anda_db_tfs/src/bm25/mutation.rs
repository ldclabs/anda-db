use super::*;

impl<T: Tokenizer> BM25Index<T> {
    /// Inserts a document into the index.
    ///
    /// The text is tokenized with a clone of the index's tokenizer; token
    /// frequencies and the document length (total token count) are then used
    /// to update the posting lists and the `total_tokens` counter that the
    /// average document length is derived from.
    ///
    /// Bucket bookkeeping happens in two steps. A token that already has a
    /// posting stays in the bucket that owns it: the bucket is only charged
    /// for the new entry. Brand-new tokens are placed in the tail bucket
    /// (`max_bucket_id`); those that do not fit under
    /// [`BM25Config::bucket_overload_size`] open a fresh tail bucket.
    ///
    /// # Arguments
    ///
    /// * `id` — unique, caller-assigned document identifier.
    /// * `text` — document text to index.
    /// * `now_ms` — wall-clock time in milliseconds, stored in
    ///   `stats.last_inserted`.
    ///
    /// # Errors
    ///
    /// * [`BM25Error::TokenizeFailed`] if the tokenizer produces no tokens.
    /// * [`BM25Error::AlreadyExists`] if `id` is already present.
    ///
    /// # Concurrency
    ///
    /// Safe to call concurrently with other `insert`/`remove`/`search` calls.
    pub fn insert(&self, id: u64, text: &str, now_ms: u64) -> Result<(), BM25Error> {
        self.require_loaded()?;
        // Tokenization does not inspect mutable index state.
        let token_freqs = collect_tokens(&mut self.tokenizer.clone(), text, None);
        if token_freqs.is_empty() {
            return Err(BM25Error::TokenizeFailed {
                name: self.name.clone(),
                id,
                text: truncate_error_text(text),
            });
        }
        let _mutation_guard = self.mutation_gate.read();
        let _doc_guard = self.doc_locks[Self::doc_stripe(id)].lock();
        let tokens: usize = token_freqs.values().sum();
        match self.doc_tokens.entry(id) {
            dashmap::Entry::Occupied(_) => {
                return Err(BM25Error::AlreadyExists {
                    name: self.name.clone(),
                    id,
                });
            }
            dashmap::Entry::Vacant(entry) => {
                entry.insert(tokens);
            }
        }
        self.max_document_id.fetch_max(id, Ordering::Relaxed);
        self.total_tokens
            .fetch_add(tokens as u64, Ordering::Relaxed);

        let doc_entry = doc_entry_size(id, tokens);
        let mut charges: FxHashMap<u32, usize> = FxHashMap::default();
        for (token, freq) in token_freqs {
            if let Some(owner) = self.append_posting(token, id, freq, doc_entry) {
                *charges.entry(owner).or_default() += cbor_serialized_size(&(id, freq)) + 2;
            }
        }
        for (owner, size) in charges {
            let mut bucket = self.buckets.entry(owner).or_default();
            bucket.size += size;
            if bucket.doc_ids.insert(id) {
                bucket.size += doc_entry;
            }
            bucket.mark_dirty();
        }
        self.update_metadata(|m| {
            m.stats.version += 1;
            m.stats.last_inserted = now_ms;
            m.stats.insert_count += 1;
        });
        Ok(())
    }

    /// Appends to an existing list (returning its owner for batched accounting),
    /// or publishes a new list and its final bucket registration together.
    /// No posting is ever exposed with a temporary owner. Each new token visits
    /// only the current tail and, if full, the next tail: no pending-list rescan.
    fn append_posting(&self, token: String, id: u64, freq: usize, doc_entry: usize) -> Option<u32> {
        if let Some(mut posting) = self.postings.get_mut(&token) {
            posting.1.push((id, freq));
            return Some(posting.0);
        }
        loop {
            let owner = self.max_bucket_id.load(Ordering::Acquire);
            // The same bucket -> posting order as unlist_if_unowned.
            let mut bucket = self.buckets.entry(owner).or_default();
            match self.postings.entry(token.clone()) {
                dashmap::Entry::Occupied(mut entry) => {
                    let posting = entry.get_mut();
                    posting.1.push((id, freq));
                    return Some(posting.0);
                }
                dashmap::Entry::Vacant(entry) => {
                    let size = cbor_serialized_size(&(entry.key(), (owner, &[(id, freq)]))) + 2;
                    let extra_doc = if bucket.doc_ids.contains(&id) {
                        0
                    } else {
                        doc_entry
                    };
                    if bucket.tokens.is_empty()
                        || bucket.size.saturating_add(size).saturating_add(extra_doc)
                            < self.config.bucket_overload_size
                    {
                        bucket.size += size + extra_doc;
                        bucket.tokens.insert(token);
                        bucket.doc_ids.insert(id);
                        bucket.mark_dirty();
                        entry.insert((owner, vec![(id, freq)]));
                        return None;
                    }
                    drop(entry);
                }
            }
            drop(bucket);
            // A competing writer may already have opened a tail. Reuse it.
            self.max_bucket_id
                .compare_exchange(owner, owner + 1, Ordering::AcqRel, Ordering::Acquire)
                .ok();
        }
    }

    /// Only the current owner may keep a token registered. The predicate and
    /// unlisting share the bucket lock, so a concurrent insertion cannot finish
    /// re-registering the token between the two operations.
    pub(super) fn unlist_if_unowned(&self, owner: u32, token: &str) {
        if let Some(mut bucket) = self.buckets.get_mut(&owner) {
            let unowned = self
                .postings
                .get(token)
                .is_none_or(|posting| posting.0 != owner);
            if unowned && bucket.tokens.remove(token) {
                bucket.mark_dirty();
            }
        }
    }

    /// Removes a document from the index.
    ///
    /// The caller must provide the *original text* that was used on
    /// [`insert`](Self::insert); it is re-tokenized to identify which posting
    /// lists should drop this document. If the text does not match, postings
    /// may retain stale entries — searches still skip them because scoring
    /// filters by `doc_tokens` membership, and the stale entries are pruned
    /// the next time the index is loaded via
    /// [`load_buckets`](Self::load_buckets). For idempotent recovery, cleanup
    /// by `text` still runs when `id` is already absent from `doc_tokens`; in
    /// that case the method returns `false` and deletion statistics are not
    /// incremented again.
    ///
    /// # Arguments
    ///
    /// * `id` — identifier of the document to remove.
    /// * `text` — original text of the document.
    /// * `now_ms` — wall-clock time, stored in `stats.last_deleted`.
    ///
    /// # Returns
    ///
    /// * `true` if a document with the given id was found and removed.
    /// * `false` if absent or not fully loaded (no mutation in the latter case).
    pub fn remove(&self, id: u64, text: &str, now_ms: u64) -> bool {
        if !self.is_fully_loaded() {
            return false;
        }
        let _mutation_guard = self.mutation_gate.read();
        let _doc_guard = self.doc_locks[Self::doc_stripe(id)].lock();

        // Even when `doc_tokens` was already removed, continue through the
        // supplied text and bucket bookkeeping. Crash-replay may encounter a
        // prefix of an earlier remove, and the retry must still purge stale
        // postings without double-counting the logical deletion.
        let removed_tokens = self.doc_tokens.remove(&id).map(|(_k, v)| v);
        let was_present = removed_tokens.is_some();

        if let Some(removed_tokens) = removed_tokens {
            // Mirror of `insert`: the token counter follows the `doc_tokens`
            // entry it belongs to, and the average document length is derived
            // from the pair at read time.
            self.total_tokens
                .fetch_sub(removed_tokens as u64, Ordering::Relaxed);
        }
        // Refund of the document's token-count entry from every bucket that
        // listed it. On a replay the count is unknown; the few bytes of
        // difference are within the estimate's tolerance.
        let doc_entry = doc_entry_size(id, removed_tokens.unwrap_or(0));

        // Tokenize the document
        let token_freqs = {
            let mut tokenizer = self.tokenizer.clone();
            collect_tokens(&mut tokenizer, text, None)
        };

        // buckets_to_update: FxHashMap<bucketid, FxHashMap<token, size_decrease>>
        let mut buckets_to_update: FxHashMap<u32, FxHashMap<String, usize>> = FxHashMap::default();
        // Remove from inverted index
        let mut maybe_empty_tokens: Vec<String> = Vec::new();
        for (token, _) in token_freqs {
            if let Some(mut posting) = self.postings.get_mut(&token) {
                // Remove every entry for this document in one pass. Duplicates
                // can exist when a previous remove() was given non-original
                // text and the document was re-inserted afterwards.
                let mut removed_vals: Vec<(u64, usize)> = Vec::new();
                posting.1.retain(|entry| {
                    if entry.0 == id {
                        removed_vals.push(*entry);
                        false
                    } else {
                        true
                    }
                });
                if removed_vals.is_empty() {
                    continue;
                }

                let size_decrease = if posting.1.is_empty() {
                    maybe_empty_tokens.push(token.clone());
                    cbor_serialized_size(&(&token, (posting.0, &removed_vals))) + 2
                } else {
                    removed_vals
                        .iter()
                        .map(|val| cbor_serialized_size(val) + 2)
                        .sum()
                };
                let b = buckets_to_update.entry(posting.0).or_default();
                b.insert(token, size_decrease);
            }
        }

        // Drop empty postings atomically: a concurrent insert may have appended
        // a new entry after the guard above was released, in which case the
        // posting must survive. `remove_if` re-checks under the shard lock.
        let mut removed_postings: FxHashSet<String> =
            FxHashSet::with_capacity_and_hasher(maybe_empty_tokens.len(), FxBuildHasher);
        for token in maybe_empty_tokens {
            if self
                .postings
                .remove_if(&token, |_, posting| posting.1.is_empty())
                .is_some()
            {
                removed_postings.insert(token);
            }
        }

        for (bucket_id, val) in buckets_to_update {
            if let Some(mut bucket) = self.buckets.get_mut(&bucket_id) {
                bucket.mark_dirty();
                bucket.size = bucket.size.saturating_sub(val.values().sum());
                if bucket.doc_ids.remove(&id) {
                    bucket.size = bucket.size.saturating_sub(doc_entry);
                }
            }
            for token in val.keys().filter(|token| removed_postings.contains(*token)) {
                self.unlist_if_unowned(bucket_id, token);
            }
        }

        // Other buckets may still reference this document in their serialized
        // doc_tokens (e.g. stale postings left by a remove() with non-original
        // text); mark them dirty so the next flush drops the reference.
        // Read-scan first to avoid write-locking every shard on each remove.
        let stale_buckets: Vec<u32> = self
            .buckets
            .iter()
            .filter(|bucket| bucket.doc_ids.contains(&id))
            .map(|bucket| *bucket.key())
            .collect();
        for bucket_id in stale_buckets {
            if let Some(mut bucket) = self.buckets.get_mut(&bucket_id)
                && bucket.doc_ids.remove(&id)
            {
                bucket.size = bucket.size.saturating_sub(doc_entry);
                bucket.mark_dirty();
            }
        }

        if was_present {
            self.update_metadata(|m| {
                m.stats.version += 1;
                m.stats.last_deleted = now_ms;
                m.stats.delete_count += 1;
            });
        }

        was_present
    }

    /// Erases a set of document ids from the index **without their text**.
    ///
    /// [`remove`](Self::remove) needs the document's original text to know
    /// which posting lists mention the document. A repair path that lost the
    /// document body — `anda_db`'s `Collection::reconcile_storage`, which
    /// drops ids whose stored object vanished in a crash — has no text to
    /// give, so this method sweeps the inverted index instead and drops every
    /// posting entry whose document id is in `ids`.
    ///
    /// # Cost, and why there is no cheaper route
    ///
    /// One pass over every posting list: `O(distinct tokens + posting
    /// entries)`. The per-bucket `doc_ids` sets look like a document → bucket
    /// index that could narrow the sweep, but they are a best-effort
    /// dirty-tracking hint, not a reverse index: a [`remove`](Self::remove)
    /// given non-original text clears a document from `doc_ids` while leaving
    /// its posting entries behind (that is exactly the state
    /// [`load_buckets`](Self::load_buckets) self-heals), so `doc_ids` can
    /// *under*-report. A repair path must not trust the bookkeeping it exists
    /// to repair, hence the full sweep. That is acceptable here because this
    /// is a maintenance operation whose caller already enumerates the
    /// collection's entire document prefix — and because it takes a *set*, so
    /// N dead ids cost one pass rather than N.
    ///
    /// # Consistency
    ///
    /// Every counter is left exactly consistent with the surviving postings:
    ///
    /// * each purged id's `doc_tokens` entry is dropped and its token count
    ///   subtracted from `total_tokens`, so the average document length
    ///   derived from the two stays correct (a wrong average silently skews
    ///   every subsequent BM25 score);
    /// * bucket sizes are decremented by the same estimates
    ///   [`insert`](Self::insert) accumulated, and a token whose posting list
    ///   became empty is unlisted from its bucket;
    /// * every bucket whose serialized content mentioned a purged id is marked
    ///   dirty, so the purge survives a flush + reload instead of being
    ///   resurrected from a stale bucket object's `doc_tokens`.
    ///
    /// # Arguments
    ///
    /// * `ids` — document ids to erase.
    /// * `now_ms` — wall-clock time, stored in `stats.last_deleted`.
    ///
    /// # Returns
    ///
    /// The number of ids that were actually present in the index, or zero
    /// without mutation if the index is not fully loaded.
    ///
    /// # Concurrency
    ///
    /// Takes the mutation gate *shared*, exactly like `insert`/`remove`: safe
    /// alongside them and alongside searches, exclusive against
    /// [`compact_buckets`](Self::compact_buckets). Like every other mutation
    /// it must not run concurrently with a flush (see the [`BM25Index`]
    /// concurrency contract).
    pub fn purge_ids(&self, ids: &BTreeSet<u64>, now_ms: u64) -> usize {
        if ids.is_empty() || !self.is_fully_loaded() {
            return 0;
        }
        let _mutation_guard = self.mutation_gate.read();
        let stripes: BTreeSet<usize> = ids.iter().map(|id| Self::doc_stripe(*id)).collect();
        let _doc_guards: Vec<_> = stripes
            .iter()
            .map(|stripe| self.doc_locks[*stripe].lock())
            .collect();
        let dead: FxHashSet<u64> = ids.iter().copied().collect();

        // Phase 1: drop the document lengths. As in `insert`/`remove`, the
        // token counter follows the `doc_tokens` entries it accounts for.
        let mut removed_docs = 0usize;
        let mut removed_tokens = 0u64;
        let mut token_counts: FxHashMap<u64, usize> = FxHashMap::default();
        for id in ids {
            if let Some((_, tokens)) = self.doc_tokens.remove(id) {
                removed_docs += 1;
                removed_tokens += tokens as u64;
                token_counts.insert(*id, tokens);
            }
        }
        if removed_tokens > 0 {
            self.total_tokens
                .fetch_sub(removed_tokens, Ordering::Relaxed);
        }

        // Phase 2: sweep every posting list once, collecting bucket updates
        // instead of applying them, so no `postings` shard guard is held while
        // the `buckets` map is touched.
        let mut bucket_size_decrease: FxHashMap<u32, usize> = FxHashMap::default();
        let mut emptied_tokens: Vec<(u32, String)> = Vec::new();
        for mut posting in self.postings.iter_mut() {
            let bucket_id = posting.0;
            let mut removed_entries: Vec<(u64, usize)> = Vec::new();
            posting.1.retain(|entry| {
                if dead.contains(&entry.0) {
                    removed_entries.push(*entry);
                    false
                } else {
                    true
                }
            });
            if removed_entries.is_empty() {
                continue;
            }

            // Mirror of `remove`: the whole `(token, (bucket, entries))` tuple
            // when the posting disappears — that is what `insert` charged for
            // a brand-new token — and the per-entry cost otherwise.
            let size_decrease = if posting.1.is_empty() {
                emptied_tokens.push((bucket_id, posting.key().clone()));
                cbor_serialized_size(&(posting.key(), (bucket_id, &removed_entries))) + 2
            } else {
                removed_entries
                    .iter()
                    .map(|entry| cbor_serialized_size(entry) + 2)
                    .sum()
            };
            *bucket_size_decrease.entry(bucket_id).or_default() += size_decrease;
        }

        // Phase 3: drop the emptied posting lists atomically. A concurrent
        // insert may have appended an entry after the sweep released the shard
        // guard, in which case the posting must survive; `remove_if` re-checks
        // under the shard lock.
        let mut removed_postings: FxHashSet<String> =
            FxHashSet::with_capacity_and_hasher(emptied_tokens.len(), FxBuildHasher);
        for (_, token) in emptied_tokens.iter() {
            if self
                .postings
                .remove_if(token, |_, posting| posting.1.is_empty())
                .is_some()
            {
                removed_postings.insert(token.clone());
            }
        }

        // Phase 4: resize and dirty every bucket that owned an affected token.
        let mut purged_postings = !bucket_size_decrease.is_empty();
        for (bucket_id, size_decrease) in bucket_size_decrease {
            if let Some(mut bucket) = self.buckets.get_mut(&bucket_id) {
                bucket.mark_dirty();
                bucket.size = bucket.size.saturating_sub(size_decrease);
            }
        }

        // Phase 5: unlist the tokens whose posting is genuinely gone.
        // `removed_postings` is a snapshot: a concurrent insert may have
        // re-created the posting, possibly in another bucket. Only drop the
        // token when no bucket claims it or a different one does, otherwise no
        // bucket would list it and `serialize_bucket` would lose the term.
        for (bucket_id, token) in emptied_tokens {
            if !removed_postings.contains(&token) {
                continue;
            }
            self.unlist_if_unowned(bucket_id, &token);
        }

        // Phase 6: drop the purged ids from every bucket's doc-id set. A
        // bucket can still list one without owning a posting for it, and its
        // serialized `doc_tokens` would resurrect the id on reload. Read-scan
        // first so a purge that touches nothing does not write-lock every
        // shard; probe by `ids` (the dead set is small) rather than by
        // `doc_ids` (which can hold the whole collection).
        let stale_buckets: Vec<u32> = self
            .buckets
            .iter()
            .filter(|bucket| {
                if dead.len() <= bucket.doc_ids.len() {
                    dead.iter().any(|id| bucket.doc_ids.contains(id))
                } else {
                    bucket.doc_ids.iter().any(|id| dead.contains(id))
                }
            })
            .map(|bucket| *bucket.key())
            .collect();
        purged_postings |= !stale_buckets.is_empty();
        for bucket_id in stale_buckets {
            if let Some(mut bucket) = self.buckets.get_mut(&bucket_id) {
                let removed: Vec<_> = if dead.len() <= bucket.doc_ids.len() {
                    dead.iter()
                        .filter(|id| bucket.doc_ids.contains(id))
                        .copied()
                        .collect()
                } else {
                    bucket
                        .doc_ids
                        .iter()
                        .filter(|id| dead.contains(id))
                        .copied()
                        .collect()
                };
                let changed = !removed.is_empty();
                for id in removed {
                    bucket.doc_ids.remove(&id);
                    let count = token_counts.get(&id).copied().unwrap_or(0);
                    bucket.size = bucket.size.saturating_sub(doc_entry_size(id, count));
                }
                if changed {
                    bucket.mark_dirty();
                }
            }
        }

        if removed_docs > 0 || purged_postings {
            self.update_metadata(|m| {
                m.stats.version += 1;
                m.stats.last_deleted = now_ms;
                m.stats.delete_count += removed_docs as u64;
            });
        }

        removed_docs
    }
}
