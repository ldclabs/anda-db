use super::*;

impl<T: Tokenizer> BM25Index<T> {
    /// Loads a complete index (metadata and all buckets) in one call.
    ///
    /// This is a convenience wrapper around [`load_metadata`](Self::load_metadata)
    /// followed by [`load_buckets`](Self::load_buckets).
    ///
    /// # Arguments
    ///
    /// * `tokenizer` — tokenizer to attach to the loaded index. It does not
    ///   need to be identical to the one originally used, but queries will
    ///   only be meaningful if the tokenization is compatible.
    /// * `metadata` — reader positioned at the start of the CBOR metadata blob.
    /// * `f` — async function invoked once per referenced [`BucketObject`];
    ///   return `Ok(Some(bytes))` for present buckets or `Ok(None)` to skip.
    ///
    /// # Returns
    ///
    /// The fully-loaded index, or a [`BM25Error`] if metadata could not be
    /// parsed or a bucket failed to load.
    pub async fn load_all<R: Read, F>(tokenizer: T, metadata: R, f: F) -> Result<Self, BM25Error>
    where
        F: AsyncFnMut(BucketObject) -> Result<Option<Vec<u8>>, BoxError>,
    {
        let mut index = Self::load_metadata(tokenizer, metadata)?;
        index.load_buckets(f).await?;
        Ok(index)
    }

    /// Loads all manifest-referenced objects, failing if any is missing.
    /// Legacy layouts still permit holes in their bucket-id range.
    pub async fn load_all_strict<R: Read, F>(
        tokenizer: T,
        metadata: R,
        f: F,
    ) -> Result<Self, BM25Error>
    where
        F: AsyncFnMut(BucketObject) -> Result<Option<Vec<u8>>, BoxError>,
    {
        let mut index = Self::load_metadata(tokenizer, metadata)?;
        index.load_buckets_strict(f).await?;
        Ok(index)
    }

    /// Loads only the index metadata, returning an empty shell.
    ///
    /// The returned index contains the correct configuration, statistics and
    /// id watermarks, but no postings or `doc_tokens`. Call
    /// [`load_buckets`](Self::load_buckets) afterwards to populate the inverted
    /// index (possibly on demand, or only for a subset of buckets).
    ///
    /// Manifest entries get clean placeholders. The shell preserves persisted
    /// statistics in `metadata()` / `stats()`, while `len()` is zero. Mutations
    /// are refused until every referenced bucket has been loaded; compaction
    /// is a no-op. A clean shell's metadata can still be persisted safely.
    pub fn load_metadata<R: Read>(tokenizer: T, r: R) -> Result<Self, BM25Error> {
        let index: BM25IndexOwned =
            cbor2::from_reader(r).map_err(|err| BM25Error::Serialization {
                name: "unknown".to_string(),
                source: err.into(),
            })?;
        let max_bucket_id = AtomicU32::new(index.metadata.stats.max_bucket_id);
        let max_document_id = AtomicU64::new(index.metadata.stats.max_document_id);
        let search_count = AtomicU64::new(index.metadata.stats.search_count);
        let last_saved_version = AtomicU64::new(index.metadata.stats.version);

        // A flush rebuilds the manifest from the in-memory bucket map, so
        // every referenced bucket needs a placeholder that keeps its
        // committed generation until `load_buckets` replaces it.
        let buckets: DashMap<u32, Bucket> = index
            .metadata
            .buckets
            .keys()
            .map(|bucket_id| (*bucket_id, Bucket::default()))
            .chain([(0, Bucket::default())])
            .collect();

        let unloaded_buckets = index.metadata.buckets.keys().copied().collect();
        Ok(BM25Index {
            name: index.metadata.name.clone(),
            tokenizer,
            config: index.metadata.config.clone(),
            doc_tokens: DashMap::new(),
            postings: DashMap::new(),
            buckets,
            metadata: RwLock::new(index.metadata),
            max_bucket_id,
            max_document_id,
            search_count,
            last_saved_version,
            // No document is loaded yet; `load_buckets` seeds this from the
            // documents it actually loads. The persisted
            // `stats.avg_doc_tokens` is not carried over — it would disagree
            // with an empty `doc_tokens` until then.
            total_tokens: AtomicU64::new(0),
            mutation_gate: RwLock::new(()),
            doc_locks: std::array::from_fn(|_| Mutex::new(())),
            load_state: LoadState::MetadataOnly,
            unloaded_buckets,
        })
    }

    /// Populates the inverted index from previously persisted buckets.
    ///
    /// Intended to be called right after [`load_metadata`](Self::load_metadata).
    /// When the loaded metadata carries a bucket manifest, `f` is invoked once
    /// per referenced [`BucketObject`]; without a manifest (data persisted by
    /// a pre-manifest release) every bucket id in `0..=max_bucket_id` is
    /// probed at generation `0` (the legacy object). Returning `Ok(None)`
    /// leaves that bucket unloaded. Partial indexes are read-only: insert and
    /// flush return errors, remove/purge do nothing, and compaction is a no-op.
    /// A later call may load the remaining buckets; `is_fully_loaded()` reports
    /// when mutation is allowed. Use `load_buckets_strict` when missing objects
    /// must fail the load instead of producing a partial index.
    ///
    /// After this call, `total_tokens` — and therefore the average document
    /// length derived from it — reflects exactly the documents that were
    /// loaded.
    ///
    /// Posting entries that reference a document with no token count in any
    /// loaded bucket are pruned and the affected buckets are marked dirty, so
    /// the next [`flush`](Self::flush) persists the cleanup. The same applies
    /// to a token present in more than one legacy bucket object (a leftover
    /// of the pre-manifest flush protocol): the copy in the highest-numbered
    /// bucket wins and the stale copy is dropped.
    pub async fn load_buckets<F>(&mut self, f: F) -> Result<(), BM25Error>
    where
        F: AsyncFnMut(BucketObject) -> Result<Option<Vec<u8>>, BoxError>,
    {
        self.load_buckets_impl(f, false).await
    }

    /// Like `load_buckets`, but a missing manifest object is an error.
    pub async fn load_buckets_strict<F>(&mut self, f: F) -> Result<(), BM25Error>
    where
        F: AsyncFnMut(BucketObject) -> Result<Option<Vec<u8>>, BoxError>,
    {
        self.load_buckets_impl(f, true).await
    }

    async fn load_buckets_impl<F>(&mut self, mut f: F, strict: bool) -> Result<(), BM25Error>
    where
        F: AsyncFnMut(BucketObject) -> Result<Option<Vec<u8>>, BoxError>,
    {
        // A failed/cancelled load cannot leave a writable half-populated index.
        self.load_state = LoadState::Partial;
        let mut doc_token_lengths: FxHashMap<u64, usize> = self
            .doc_tokens
            .iter()
            .map(|entry| (*entry.key(), *entry.value()))
            .collect();

        let manifest = { self.metadata.read().buckets.clone() };
        let legacy = manifest.is_empty();
        let objects: Vec<BucketObject> = if legacy {
            (0..=self.max_bucket_id.load(Ordering::Relaxed))
                .map(|bucket_id| BucketObject {
                    bucket_id,
                    generation: 0,
                })
                .collect()
        } else {
            manifest
                .iter()
                .map(|(bucket_id, generation)| BucketObject {
                    bucket_id: *bucket_id,
                    generation: *generation,
                })
                .collect()
        };

        let mut loaded_bucket_ids: Vec<u32> = Vec::new();
        for object in objects {
            let i = object.bucket_id;
            let data = f(object).await.map_err(|err| BM25Error::Generic {
                name: self.name.clone(),
                source: err,
            })?;
            if data.is_none() && !legacy {
                if strict {
                    return Err(BM25Error::Generic {
                        name: self.name.clone(),
                        source: format!(
                            "missing referenced bucket {} generation {}",
                            object.bucket_id, object.generation
                        )
                        .into(),
                    });
                }
                // The manifest references this bucket but the caller skipped
                // (or lost) its object. Keep an empty placeholder so the next
                // flush carries the manifest entry forward instead of
                // silently dropping the durable object.
                self.buckets.entry(i).or_default();
                continue;
            }
            if let Some(data) = data {
                loaded_bucket_ids.push(i);
                let bucket: BucketOwned =
                    cbor2::from_reader(&data[..]).map_err(|err| BM25Error::Serialization {
                        name: self.name.clone(),
                        source: err.into(),
                    })?;

                let mut b = Bucket {
                    size: data.len(),
                    ..Default::default()
                };
                if !bucket.doc_tokens.is_empty() {
                    b.doc_ids = bucket.doc_tokens.keys().copied().collect();
                    for (doc_id, token_count) in bucket.doc_tokens {
                        doc_token_lengths.insert(doc_id, token_count);
                        // Keep lengths alongside published postings even if a
                        // later read fails or is cancelled. An incremental
                        // retry may skip this already loaded bucket.
                        let previous = self.doc_tokens.insert(doc_id, token_count).unwrap_or(0);
                        let total = self.total_tokens.get_mut();
                        *total = *total + token_count as u64 - previous as u64;
                    }
                }

                if !bucket.postings.is_empty() {
                    for (token, mut posting) in bucket.postings {
                        // The bucket file path is the source of truth for ownership.
                        // If a stale lower-numbered bucket is still present after a
                        // partial flush, later buckets win and the old bucket is
                        // marked dirty so the stale token is removed on the next flush.
                        posting.0 = i;
                        if let Some(previous) = self.postings.insert(token.clone(), posting) {
                            let previous_bucket_id = previous.0;
                            if previous_bucket_id != i
                                && let Some(mut previous_bucket) =
                                    self.buckets.get_mut(&previous_bucket_id)
                                && previous_bucket.tokens.remove(&token)
                            {
                                let previous_size = cbor_serialized_size(&(&token, &previous)) + 2;
                                previous_bucket.size =
                                    previous_bucket.size.saturating_sub(previous_size);
                                previous_bucket.mark_dirty();
                            }
                        }

                        b.tokens.insert(token);
                    }
                }

                self.buckets.insert(i, b);
                self.unloaded_buckets.remove(&i);
            }
        }

        let mut doc_ids_by_bucket: FxHashMap<u32, FxHashSet<u64>> = FxHashMap::default();
        let mut loaded_doc_tokens: FxHashMap<u64, usize> = FxHashMap::default();
        let mut empty_tokens: Vec<(u32, String)> = Vec::new();
        let mut bucket_size_decrease: FxHashMap<u32, usize> = FxHashMap::default();

        for mut entry in self.postings.iter_mut() {
            let (token, posting) = entry.pair_mut();
            let bucket_id = posting.0;
            let doc_ids = doc_ids_by_bucket.entry(bucket_id).or_default();
            // Prune entries whose document has no token length anywhere.
            // Buckets are self-contained (a bucket's doc_tokens cover every
            // document referenced by its postings), so after loading, an entry
            // without a token length can only be a stale leftover from a
            // remove() that was given non-original text. Dropping it here makes
            // the index self-healing on reload. Documents from buckets that
            // were intentionally skipped (partial load) are not affected.
            let size = retain_posting(token, posting, |id| {
                if let Some(token_count) = doc_token_lengths.get(&id) {
                    loaded_doc_tokens.insert(id, *token_count);
                    doc_ids.insert(id);
                    true
                } else {
                    false
                }
            });
            if size > 0 {
                *bucket_size_decrease.entry(bucket_id).or_default() += size;
                if posting.1.is_empty() {
                    empty_tokens.push((bucket_id, token.clone()));
                }
            }
        }

        for (bucket_id, token) in empty_tokens {
            self.postings.remove(&token);
            if let Some(mut bucket) = self.buckets.get_mut(&bucket_id) {
                bucket.tokens.remove(&token);
            }
        }

        for (bucket_id, size_decrease) in bucket_size_decrease {
            if let Some(mut bucket) = self.buckets.get_mut(&bucket_id) {
                bucket.size = bucket.size.saturating_sub(size_decrease);
                bucket.mark_dirty();
            }
        }

        self.doc_tokens.clear();
        self.doc_tokens.extend(loaded_doc_tokens);

        let bucket_ids: Vec<u32> = self.buckets.iter().map(|b| *b.key()).collect();
        for bucket_id in bucket_ids {
            if let Some(mut bucket) = self.buckets.get_mut(&bucket_id) {
                let doc_ids = doc_ids_by_bucket.remove(&bucket_id).unwrap_or_default();
                if bucket.doc_ids != doc_ids {
                    bucket.doc_ids = doc_ids;
                    bucket.mark_dirty();
                }
            }
        }

        let total_tokens: usize = self.doc_tokens.iter().map(|r| *r.value()).sum();
        self.total_tokens
            .store(total_tokens as u64, Ordering::Relaxed);

        // From here the bucket map mirrors the committed layout, so rebuilding
        // the manifest from it is safe.
        self.load_state = if self.unloaded_buckets.is_empty() {
            LoadState::Complete
        } else {
            LoadState::Partial
        };

        if legacy && !loaded_bucket_ids.is_empty() {
            // Record in memory where each loaded bucket's durable object
            // lives (generation 0 = legacy object). The next flush commits a
            // real manifest whose clean buckets keep referencing these legacy
            // objects until they are rewritten.
            self.update_metadata(|m| {
                m.buckets = loaded_bucket_ids.iter().map(|id| (*id, 0)).collect();
            });
        }

        Ok(())
    }

    /// Persists metadata and every currently-dirty bucket.
    ///
    /// This is a convenience wrapper around [`flush_with`](Self::flush_with)
    /// that writes and flushes the metadata writer. A generic `Write` cannot
    /// guarantee atomic replacement or fsync: use `flush_with` with a durable
    /// commit callback for files/object stores. Never truncate a committed
    /// metadata file before calling this method; a clean flush invokes no writes.
    ///
    /// # Arguments
    ///
    /// * `metadata` — writer that receives the CBOR-encoded metadata blob.
    /// * `now_ms` — wall-clock time stored in `stats.last_saved`.
    /// * `f` — async function used to persist each dirty bucket.
    ///
    /// # Returns
    ///
    /// See [`flush_with`](Self::flush_with).
    pub async fn flush<W: Write, F, Fut>(
        &self,
        metadata: W,
        now_ms: u64,
        f: F,
    ) -> Result<FlushOutcome, BM25Error>
    where
        F: FnMut(BucketObject, Vec<u8>) -> Fut,
        Fut: Future<Output = Result<(), BoxError>>,
    {
        self.flush_with(
            now_ms,
            move |data: Vec<u8>| {
                let mut metadata = metadata;
                async move {
                    metadata.write_all(&data)?;
                    metadata.flush()?;
                    Ok(())
                }
            },
            f,
        )
        .await
    }

    /// Persists every dirty bucket to a new immutable object, then commits
    /// the metadata whose manifest references them.
    ///
    /// # Manifest commit protocol
    ///
    /// Every dirty bucket is serialized and written to a **fresh** object
    /// keyed by `(bucket_id, generation)` — the generation is this flush's
    /// metadata version, so a bucket object is never mutated in place once a
    /// committed manifest references it. The metadata (carrying the manifest
    /// `bucket_id -> generation`) is written last; that single write is the
    /// atomic commit point:
    ///
    /// * A crash or error **before** the metadata commit leaves the new
    ///   bucket objects as unreferenced garbage. A loader still sees the
    ///   previous manifest — a complete, consistent snapshot. Nothing is
    ///   lost; a retry can reuse the uncommitted generation.
    /// * **After** the commit, the objects replaced by this flush are
    ///   garbage. They are returned as [`FlushOutcome::obsolete`] for the
    ///   caller to delete best-effort; a failed deletion only leaks space.
    ///
    /// [`compact_buckets`](Self::compact_buckets) needs no special write
    /// ordering under this protocol: the repacked layout becomes visible
    /// atomically with the manifest commit, and every pre-compaction object
    /// is reported as obsolete.
    ///
    /// # Concurrency
    ///
    /// The caller must not run a flush concurrently with mutations,
    /// compaction, or another flush (see the [`BM25Index`] concurrency
    /// contract). Dirty versions and metadata are captured first, then each
    /// bucket is serialized and uploaded in turn. Only one payload is buffered
    /// at a time; all map guards are released before each callback is awaited.
    ///
    /// # Arguments
    ///
    /// * `now_ms` — wall-clock time stored in `stats.last_saved`.
    /// * `metadata_f` — async function that durably persists the CBOR
    ///   metadata blob; it must return `Ok(())` only after the write is
    ///   durable, because it is the commit point.
    /// * `f` — async function invoked once per dirty bucket with the target
    ///   [`BucketObject`] and the CBOR payload. It must create/overwrite the
    ///   object addressed by `(bucket_id, generation)`.
    ///
    /// # Returns
    ///
    /// * `Ok(outcome)` with [`FlushOutcome::saved`] `== false` when the index
    ///   was already fully persisted (no callback was invoked).
    /// * `Ok(outcome)` with `saved == true` after a successful commit;
    ///   [`FlushOutcome::obsolete`] lists the replaced bucket objects.
    /// * `Err` on serialization failure or a failed callback. If the metadata
    ///   callback reports an uncertain outcome after writing, reopen from the
    ///   durable metadata rather than assuming the commit did not happen.
    pub async fn flush_with<M, MFut, F, FFut>(
        &self,
        now_ms: u64,
        metadata_f: M,
        mut f: F,
    ) -> Result<FlushOutcome, BM25Error>
    where
        M: FnOnce(Vec<u8>) -> MFut,
        MFut: Future<Output = Result<(), BoxError>>,
        F: FnMut(BucketObject, Vec<u8>) -> FFut,
        FFut: Future<Output = Result<(), BoxError>>,
    {
        if self.load_state == LoadState::Partial {
            self.require_loaded()?;
        }
        let has_dirty = self.has_dirty_buckets();
        if !has_dirty && !self.has_pending_metadata_flush() {
            return Ok(FlushOutcome::default());
        }
        if has_dirty {
            self.require_loaded()?;
        }

        // A bucket object only becomes reachable through the manifest, so
        // dirty buckets always require a metadata commit. Loading can mark
        // buckets dirty (stale-entry pruning) without bumping the stats
        // version; force a fresh version in that case.
        if has_dirty && !self.has_pending_metadata_flush() {
            self.update_metadata(|m| m.stats.version += 1);
        }

        // Freeze only the small version list. The caller excludes mutations
        // across the entire flush; each payload is serialized just before its
        // upload, bounding extra payload memory to the largest dirty bucket.
        let mut dirty = self.collect_dirty_buckets();
        dirty.sort_unstable_by_key(|(id, _)| *id);

        let mut meta = self.metadata();
        meta.stats.last_saved = now_ms.max(meta.stats.last_saved);
        // This flush's generation: unique per committed manifest because the
        // stats version increases monotonically and is claimed exactly once.
        let generation = meta.stats.version;

        // Build the new manifest: dirty buckets move to this generation,
        // clean buckets keep their committed object. In-memory buckets that
        // were never persisted (e.g. the empty initial bucket) stay out.
        let committed = meta.buckets.clone();
        let dirty_ids: FxHashSet<u32> = dirty.iter().map(|(id, _)| *id).collect();
        let mut manifest = BTreeMap::new();
        for entry in self.buckets.iter() {
            let id = *entry.key();
            if dirty_ids.contains(&id) {
                manifest.insert(id, generation);
            } else if let Some(committed_generation) = committed.get(&id) {
                manifest.insert(id, *committed_generation);
            }
        }
        meta.buckets = manifest.clone();

        let mut meta_buf = Vec::with_capacity(256);
        cbor2::to_writer(&BM25IndexRef { metadata: &meta }, &mut meta_buf).map_err(|err| {
            BM25Error::Serialization {
                name: self.name.clone(),
                source: err.into(),
            }
        })?;

        // Objects the previous manifest referenced that the new one replaces
        // or drops (bucket rewrites, compaction leftovers, legacy objects).
        let obsolete: Vec<BucketObject> = committed
            .iter()
            .filter(|(id, generation)| manifest.get(id) != Some(generation))
            .map(|(id, generation)| BucketObject {
                bucket_id: *id,
                generation: *generation,
            })
            .collect();

        // NOTE: the callbacks are plain `FnMut`/`FnOnce` closures returning a
        // named future type and take owned `Vec<u8>` blobs: `AsyncFn*` bounds
        // here make the resulting future's `Send`-ness non-generalizable over
        // lifetimes (rustc: "implementation of `Send` is not general
        // enough"), which would break every downstream `tokio::spawn` of a
        // flush.

        // Phase 1: write every dirty bucket to its new immutable object.
        // Unreachable until the commit below, so any failure here leaves the
        // previous durable snapshot fully intact.
        for &(bucket_id, _) in &dirty {
            let buf = self
                .serialize_bucket(bucket_id)?
                .ok_or_else(|| BM25Error::Generic {
                    name: self.name.clone(),
                    source: "bucket changed during flush; caller must exclude concurrent mutations"
                        .into(),
                })?;
            f(
                BucketObject {
                    bucket_id,
                    generation,
                },
                buf,
            )
            .await
            .map_err(|source| BM25Error::Generic {
                name: self.name.clone(),
                source,
            })?;
        }

        // Phase 2: the manifest commit — the single atomic point.
        metadata_f(meta_buf)
            .await
            .map_err(|err| BM25Error::Generic {
                name: self.name.clone(),
                source: err,
            })?;

        // Publish the committed state in memory.
        self.last_saved_version
            .fetch_max(generation, Ordering::Release);
        self.update_metadata(|m| {
            m.stats.last_saved = meta.stats.last_saved.max(m.stats.last_saved);
            m.buckets = manifest;
        });
        for (bucket_id, version) in dirty {
            self.mark_bucket_saved(bucket_id, version);
        }

        Ok(FlushOutcome {
            saved: true,
            obsolete,
        })
    }

    /// Returns whether there are dirty buckets pending persistence.
    pub fn has_dirty_buckets(&self) -> bool {
        self.buckets.iter().any(|b| b.is_dirty())
    }

    /// Returns whether metadata has a newer logical version than the last
    /// serialized metadata snapshot.
    pub fn has_pending_metadata_flush(&self) -> bool {
        let current_version = { self.metadata.read().stats.version };
        self.last_saved_version.load(Ordering::Acquire) < current_version
    }

    /// Collects the ids and dirty-version snapshots of dirty buckets,
    /// releasing all DashMap iter locks before the caller starts making async
    /// persistence calls.
    fn collect_dirty_buckets(&self) -> Vec<(u32, u64)> {
        self.buckets
            .iter()
            .filter(|b| b.is_dirty())
            .map(|b| (*b.key(), b.dirty_version))
            .collect()
    }

    /// Serializes one bucket, dropping every DashMap guard before returning
    /// so no lock is held across the caller's async persistence call.
    /// Returns `Ok(None)` when the bucket no longer exists or is no longer
    /// dirty and should be skipped.
    ///
    /// The serialized content is also the truth for two pieces of bucket
    /// bookkeeping that are only estimated between flushes: `size` becomes
    /// the payload's exact length (what a reload sets it to as well) and
    /// `doc_ids` becomes exactly the set of documents whose token count the
    /// payload carries.
    fn serialize_bucket(&self, bucket_id: u32) -> Result<Option<Vec<u8>>, BM25Error> {
        let mut bucket = match self.buckets.get_mut(&bucket_id) {
            Some(b) if b.is_dirty() => b,
            _ => return Ok(None),
        };

        let mut referenced_doc_ids = FxHashSet::default();
        let postings: FxHashMap<_, _> = bucket
            .tokens
            .iter()
            .filter_map(|k| {
                let posting = self.postings.get(k)?;
                if posting.0 != bucket_id {
                    return None;
                }
                for (doc_id, _) in posting.1.iter() {
                    referenced_doc_ids.insert(*doc_id);
                }
                Some((k, posting))
            })
            .collect();

        let doc_tokens: FxHashMap<_, _> = referenced_doc_ids
            .iter()
            .filter_map(|id| self.doc_tokens.get(id).map(|v| (*id, *v)))
            .collect();

        let mut buf = Vec::with_capacity(4096);
        cbor2::to_writer(
            &BucketRef {
                postings: &postings,
                doc_tokens: &doc_tokens,
            },
            &mut buf,
        )
        .map_err(|err| BM25Error::Serialization {
            name: self.name.clone(),
            source: err.into(),
        })?;
        drop(postings);

        bucket.doc_ids = doc_tokens.into_keys().collect();
        bucket.size = buf.len();
        Ok(Some(buf))
    }

    /// Records that `bucket_id` was persisted at `snapshot_version`.
    ///
    /// Marks a captured version clean only after the manifest commit. Failed
    /// or cancelled writes leave every captured version retryable; callers
    /// must still exclude concurrent mutations for the whole flush.
    fn mark_bucket_saved(&self, bucket_id: u32, snapshot_version: u64) {
        if let Some(mut b) = self.buckets.get_mut(&bucket_id) {
            b.saved_version = b.saved_version.max(snapshot_version);
        }
    }
}
