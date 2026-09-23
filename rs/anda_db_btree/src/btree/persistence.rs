use super::*;

impl<PK, FV> BTreeIndex<PK, FV>
where
    PK: Ord + Eq + Hash + Debug + Clone + Serialize + DeserializeOwned,
    FV: Ord + Eq + Hash + Debug + Clone + Serialize + DeserializeOwned,
{
    /// Loads an index from metadata reader and a closure for loading buckets.
    ///
    /// # Arguments
    ///
    /// * `metadata` - Metadata reader
    /// * `f` - Closure for loading buckets
    ///
    /// # Returns
    ///
    /// * `Result<Self, BTreeError>` - Loaded index or error.
    pub async fn load_all<R: Read, F>(metadata: R, f: F) -> Result<Self, BTreeError>
    where
        F: AsyncFnMut(BucketObject) -> Result<Option<Vec<u8>>, BoxError>,
    {
        let mut index = Self::load_metadata(metadata)?;
        index.load_buckets(f).await?;
        Ok(index)
    }

    /// Loads an index from a reader
    /// This only loads metadata, you need to call [`Self::load_buckets`] to load the actual posting data
    ///
    /// # Arguments
    ///
    /// * `r` - Any type implementing the [`Read`] trait
    ///
    /// # Returns
    ///
    /// * `Result<Self, Error>` - Loaded index or error
    pub fn load_metadata<R: Read>(r: R) -> Result<Self, BTreeError> {
        // Deserialize the index metadata
        let decoded: IndexForLoad =
            cbor2::from_reader(r).map_err(|err| BTreeError::Serialization {
                name: "unknown".to_string(),
                source: err.into(),
            })?;
        let legacy_format = decoded.metadata.buckets.is_none();
        let mut metadata = BTreeMetadata {
            name: decoded.metadata.name,
            config: decoded.metadata.config,
            stats: decoded.metadata.stats,
            buckets: decoded.metadata.buckets.unwrap_or_default(),
        };
        // Same floor as `new()`: persisted metadata may carry a degenerate
        // (e.g. zero) bucket_overload_size from an older or corrupted file.
        metadata.config.clamp();

        // Extract configuration values
        let max_bucket_id = AtomicU32::new(metadata.stats.max_bucket_id);
        let query_count = AtomicU64::new(metadata.stats.query_count);
        let last_saved_version = AtomicU64::new(metadata.stats.version);

        // `num_elements` comes from untrusted storage; cap the pre-allocation
        // so a corrupted value cannot trigger a huge allocation (or a capacity
        // overflow panic). The map grows on demand past this hint anyway.
        const MAX_PREALLOCATED_CAPACITY: u64 = 1 << 16;
        let capacity = metadata.stats.num_elements.min(MAX_PREALLOCATED_CAPACITY) as usize;

        // Register every bucket the manifest references up front, as an
        // empty placeholder that `load_buckets` fills in. A flush rebuilds
        // the manifest from this map, so a committed object whose bucket was
        // skipped while loading is carried forward instead of being retired.
        let buckets: DashMap<u32, BucketState<FV>> = metadata
            .buckets
            .keys()
            .map(|bucket_id| (*bucket_id, BucketState::default()))
            .collect();
        buckets.entry(0).or_default();

        Ok(BTreeIndex {
            name: metadata.name.clone(),
            config: metadata.config.clone(),
            postings: DashMap::with_capacity(capacity),
            buckets,
            btree: RwLock::new(BTreeSet::new()),
            metadata: RwLock::new(metadata),
            query_count,
            max_bucket_id,
            last_saved_version,
            mutation_gate: RwLock::new(()),
            dirty_hint: AtomicBool::new(false),
            load_state: LoadState::MetadataOnly,
            legacy_format,
        })
    }

    /// Loads every referenced bucket. A missing manifest object is an error;
    /// missing legacy bucket ids are allowed because that format was sparse.
    ///
    /// Loading is a bootstrap operation: mutations are refused until it succeeds.
    /// An error or cancellation leaves the index read-only. Retrying starts from
    /// the original metadata and discards the previous incomplete attempt.
    pub async fn load_buckets<F>(&mut self, f: F) -> Result<(), BTreeError>
    where
        F: AsyncFnMut(BucketObject) -> Result<Option<Vec<u8>>, BoxError>,
    {
        self.load_buckets_inner(f, false).await.map(|_| ())
    }

    /// Explicitly allows missing manifest objects for diagnostic, read-only
    /// queries. Returns the missing objects; inspect `load_state()` before use.
    /// A partial index cannot be mutated, compacted or committed. Restore the
    /// objects and call `load_buckets` to obtain a complete index.
    pub async fn load_buckets_partial<F>(&mut self, f: F) -> Result<Vec<BucketObject>, BTreeError>
    where
        F: AsyncFnMut(BucketObject) -> Result<Option<Vec<u8>>, BoxError>,
    {
        self.load_buckets_inner(f, true).await
    }

    pub(super) async fn load_buckets_inner<F>(
        &mut self,
        mut f: F,
        allow_partial: bool,
    ) -> Result<Vec<BucketObject>, BTreeError>
    where
        F: AsyncFnMut(BucketObject) -> Result<Option<Vec<u8>>, BoxError>,
    {
        if self.load_state == LoadState::Ready {
            return Err(BTreeError::Generic {
                name: self.name.clone(),
                source: "buckets are already loaded; reopen to discard live state".into(),
            });
        }
        self.load_state = LoadState::Partial;
        self.postings.clear();
        self.btree.get_mut().clear();
        self.buckets.clear();
        self.dirty_hint.store(false, Ordering::Release);
        let manifest = self.metadata.get_mut().buckets.clone();
        for id in manifest.keys().copied().chain(std::iter::once(0)) {
            self.buckets.insert(id, BucketState::default());
        }
        let legacy = self.legacy_format;
        let mut missing = Vec::new();
        let objects: Vec<BucketObject> = if legacy {
            let max_bucket_id = self.max_bucket_id.load(Ordering::Relaxed);
            if max_bucket_id > MAX_LEGACY_BUCKET_ID {
                return Err(BTreeError::Generic {
                    name: self.name.clone(),
                    source: format!(
                        "legacy metadata max_bucket_id {max_bucket_id} exceeds the supported \
                         scan range {MAX_LEGACY_BUCKET_ID}; the metadata is corrupted"
                    )
                    .into(),
                });
            }
            (0..=max_bucket_id)
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
            let data = f(object).await.map_err(|err| BTreeError::Generic {
                name: self.name.clone(),
                source: err,
            })?;
            let Some(data) = data else {
                if !legacy {
                    if !allow_partial {
                        return Err(BTreeError::Generic {
                            name: self.name.clone(),
                            source: format!(
                                "bucket object ({}, {}) referenced by the manifest is missing",
                                i, object.generation
                            )
                            .into(),
                        });
                    }
                    log::warn!(
                        "BTreeIndex '{}': bucket object ({}, {}) referenced by the manifest is missing; index is read-only",
                        self.name,
                        i,
                        object.generation
                    );
                    missing.push(object);
                }
                continue;
            };

            loaded_bucket_ids.push(i);
            let bucket: BucketOwned<PK, FV> =
                cbor2::from_reader(&data[..]).map_err(|err| BTreeError::Serialization {
                    name: self.name.clone(),
                    source: err.into(),
                })?;
            let mut bks =
                FxHashSet::with_capacity_and_hasher(bucket.postings.len(), Default::default());
            let mut loaded_keys = Vec::with_capacity(bucket.postings.len());
            // Set when this bucket file contains stale entries (an empty
            // posting persisted by a pre-manifest release); the bucket is
            // loaded as dirty so the next flush rewrites the file without
            // them.
            let mut needs_repair = false;

            // Higher bucket ids are the newer state when a migrated posting
            // appears in more than one bucket. Reconcile the old in-memory
            // bucket ownership and mark it dirty so the stale lower bucket
            // is repaired on the next flush.
            for (field_value, stored) in bucket.postings {
                let mut posting = Posting::from(stored);
                // Only pre-manifest flushes could persist an empty posting
                // (they sampled a bucket between "posting emptied by
                // remove()" and "posting entry removed"); the manifest flush
                // filters empty postings out. Registering one would create a
                // "ghost" key visible to `keys()`, range queries and `len()`
                // with no backing documents. Treat it as a tombstone instead:
                // skip it, drop any stale copy already loaded from an older
                // bucket, and mark the affected buckets dirty to self-heal on
                // the next flush.
                if posting.docs.is_empty() {
                    needs_repair = true;
                    if let Some((_, previous)) = self.postings.remove(&field_value) {
                        self.detach_superseded_posting(&field_value, &previous, i);
                        self.btree.write().remove(&field_value);
                    }
                    continue;
                }

                posting.bucket_id = i;
                if let Some(previous) = self.postings.insert(field_value.clone(), posting) {
                    self.detach_superseded_posting(&field_value, &previous, i);
                }

                bks.insert(field_value.clone());
                loaded_keys.push(field_value);
            }

            self.btree.write().extend(loaded_keys);
            if needs_repair {
                self.dirty_hint.store(true, Ordering::Release);
            }
            // `data.len()` (the on-disk payload length) seeds the bucket
            // size here, while runtime mutations apply estimated deltas
            // (`posting_entry_size` + fudge). The two baselines can drift
            // slightly; the size is only used for packing decisions and
            // is always combined with saturating arithmetic.
            self.buckets.insert(
                i,
                BucketState::new(data.len(), needs_repair, bks, u64::from(needs_repair)),
            );
        }
        if missing.is_empty() {
            self.load_state = LoadState::Ready;
        }

        if legacy {
            // Record in memory where each loaded bucket's durable object
            // lives (generation 0 = legacy object). The next flush commits a
            // real manifest whose clean buckets keep referencing these legacy
            // objects until they are rewritten.
            self.update_metadata(|m| {
                m.buckets = loaded_bucket_ids.iter().map(|id| (*id, 0)).collect();
                m.stats.version += 1;
            });
        }

        self.legacy_format = false;
        Ok(missing)
    }

    /// Persists metadata and every currently-dirty bucket.
    ///
    /// This is a convenience wrapper around
    /// [`flush_owned_with`](Self::flush_owned_with) that writes the metadata
    /// blob to `metadata`; see `flush_owned_with` for the manifest commit
    /// protocol.
    ///
    /// # Arguments
    ///
    /// * `metadata` - writer that receives the CBOR-encoded metadata blob.
    /// * `now_ms`   - current unix-ms timestamp, recorded into `stats.last_saved`.
    /// * `f`        - async function used to persist each dirty bucket.
    ///
    /// # Durability
    ///
    /// `W` must be a "written means durable" target: this method treats the
    /// `write_all` into `metadata` (followed by [`Write::flush`], so a
    /// buffered writer does not hold the commit back) as the manifest commit
    /// point. Once it
    /// returns, the index advances `last_saved_version`, publishes the new
    /// manifest, clears every dirty mark, and reports the objects the previous
    /// manifest referenced in [`FlushOutcome::obsolete`] — which the caller is
    /// expected to delete. If `W` merely stages bytes for a later fallible
    /// upload and that upload fails, the durable metadata still points at
    /// generations the caller was just told to delete: unrecoverable bucket
    /// loss. Use [`flush_owned_with`](Self::flush_owned_with) and perform the
    /// upload inside its metadata callback, so a failure leaves the generation
    /// uncommitted and fully retryable.
    ///
    /// # Returns
    ///
    /// See [`flush_owned_with`](Self::flush_owned_with).
    pub async fn flush<W: Write, F, Fut>(
        &self,
        metadata: W,
        now_ms: u64,
        f: F,
    ) -> Result<FlushOutcome, BTreeError>
    where
        F: FnMut(BucketObject, Vec<u8>) -> Fut,
        Fut: Future<Output = Result<(), BoxError>>,
    {
        self.flush_owned_with(
            now_ms,
            move |data: Vec<u8>| {
                let mut metadata = metadata;
                async move {
                    metadata.write_all(&data).map_err(BoxError::from)?;
                    metadata.flush().map_err(BoxError::from)?;
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
    ///   previous manifest — a complete snapshot. A retry without another
    ///   mutation can reuse the same generation and bucket content.
    /// * **After** the commit, the objects replaced by this flush are
    ///   garbage. They are returned as [`FlushOutcome::obsolete`] for the
    ///   caller to delete best-effort; a failed deletion only leaks space.
    ///
    /// [`compact_buckets`](Self::compact_buckets) needs no special write
    /// ordering under this protocol: the repacked layout becomes visible
    /// atomically with the manifest commit, and every pre-compaction object
    /// is reported as obsolete.
    ///
    /// # Durability
    ///
    /// `metadata_writer` returning `Ok(())` *is* the commit: this method then
    /// advances `last_saved_version`, publishes the manifest in memory, clears
    /// every dirty mark and reports the replaced objects as
    /// [`FlushOutcome::obsolete`] for the caller to delete. It must therefore
    /// return `Ok(())` only once the metadata blob is durably stored — never
    /// after merely staging it for a later fallible upload. A failure after a
    /// premature `Ok(())` leaves the durable metadata referencing generations
    /// the caller was just told to delete, and those buckets cannot be
    /// recovered. Perform the upload *inside* the callback and propagate its
    /// error instead. If storage may have committed despite returning an
    /// error, reopen/recover at the caller rather than assuming no commit.
    ///
    /// # Concurrency
    ///
    /// The caller must not run a flush concurrently with mutations,
    /// compaction, or another flush (see the crate-level concurrency
    /// contract). Each bucket is encoded just before its write, bounding the
    /// payload buffer to one bucket. No internal guard crosses an await.
    ///
    /// # Arguments
    ///
    /// * `now_ms` - current unix-ms timestamp, recorded into `stats.last_saved`.
    /// * `metadata_writer` - async callback receiving the CBOR-encoded
    ///   metadata blob; it must return `Ok(())` only once the write is
    ///   durable, because it is the commit point.
    /// * `bucket_writer` - async callback invoked once per dirty bucket with
    ///   the target [`BucketObject`] and the CBOR payload. It must
    ///   create/overwrite the object addressed by `(bucket_id, generation)`.
    ///
    /// # Returns
    ///
    /// * `Ok(outcome)` with [`FlushOutcome::saved`] `== false` when the index
    ///   was already fully persisted (no callback was invoked).
    /// * `Ok(outcome)` with `saved == true` after a successful commit;
    ///   [`FlushOutcome::obsolete`] lists the replaced bucket objects.
    /// * `Err` on serialization or callback failure. In-memory dirty state
    ///   remains retryable; an uncertain metadata outcome requires recovery.
    ///
    /// # Note on callback bounds
    ///
    /// The callbacks are plain `FnMut`/`FnOnce` closures returning a named
    /// future type and take owned `Vec<u8>` blobs: `AsyncFn*` bounds here
    /// make the resulting future's `Send`-ness non-generalizable over
    /// lifetimes (rustc: "implementation of `Send` is not general enough"),
    /// which would break every downstream `tokio::spawn` of a flush.
    pub async fn flush_owned_with<M, MFut, F, FFut>(
        &self,
        now_ms: u64,
        metadata_writer: M,
        mut bucket_writer: F,
    ) -> Result<FlushOutcome, BTreeError>
    where
        M: FnOnce(Vec<u8>) -> MFut,
        MFut: Future<Output = Result<(), BoxError>>,
        F: FnMut(BucketObject, Vec<u8>) -> FFut,
        FFut: Future<Output = Result<(), BoxError>>,
    {
        self.ensure_ready()?;

        // Freeze identities and prepare the manifest. The caller keeps the
        // index quiescent until all streaming writes and the commit finish.
        let has_dirty = self.has_dirty_buckets();
        if !has_dirty && !self.has_pending_metadata_flush() {
            return Ok(FlushOutcome::default());
        }

        // A bucket object only becomes reachable through the manifest, so
        // dirty buckets always require a metadata commit. Loading can mark
        // buckets dirty (stale-entry repair) without bumping the stats
        // version; force a fresh version in that case.
        if has_dirty && !self.has_pending_metadata_flush() {
            self.update_metadata(|m| m.stats.version += 1);
        }

        let dirty = self.dirty_bucket_ids();

        let mut meta = self.metadata();
        meta.stats.last_saved = now_ms.max(meta.stats.last_saved);
        // This flush's generation: unique per committed manifest because the
        // stats version increases monotonically and is claimed exactly once.
        let generation = meta.stats.version;

        // Build the new manifest: dirty buckets move to this generation,
        // clean buckets keep their committed object. In-memory buckets that
        // were never persisted (e.g. the empty initial bucket) stay out.
        let committed = std::mem::take(&mut meta.buckets);
        let dirty_ids: FxHashSet<u32> = dirty.iter().copied().collect();
        for entry in self.buckets.iter() {
            let id = *entry.key();
            if dirty_ids.contains(&id) {
                meta.buckets.insert(id, generation);
            } else if let Some(committed_generation) = committed.get(&id) {
                meta.buckets.insert(id, *committed_generation);
            }
        }

        let mut meta_buf = Vec::with_capacity(256);
        cbor2::to_writer(&BTreeIndexRef { metadata: &meta }, &mut meta_buf).map_err(|err| {
            BTreeError::Serialization {
                name: self.name.clone(),
                source: err.into(),
            }
        })?;

        // Objects the previous manifest referenced that the new one replaces
        // or drops (bucket rewrites, compaction leftovers, legacy objects).
        let obsolete: Vec<BucketObject> = committed
            .iter()
            .filter(|(id, generation)| meta.buckets.get(id) != Some(generation))
            .map(|(id, generation)| BucketObject {
                bucket_id: *id,
                generation: *generation,
            })
            .collect();

        // Phase 1: write every dirty bucket to its new immutable object.
        // Unreachable until the commit below, so any failure here leaves the
        // previous durable snapshot fully intact.
        let mut saved_marks = Vec::with_capacity(dirty.len());
        for bucket_id in dirty {
            let snapshot =
                self.serialize_bucket_snapshot(bucket_id)?
                    .ok_or_else(|| BTreeError::Generic {
                        name: self.name.clone(),
                        source: "bucket changed during flush; exclude concurrent mutations".into(),
                    })?;
            saved_marks.push((snapshot.bucket_id, snapshot.dirty_version));
            bucket_writer(
                BucketObject {
                    bucket_id: snapshot.bucket_id,
                    generation,
                },
                snapshot.data,
            )
            .await
            .map_err(|source| BTreeError::Generic {
                name: self.name.clone(),
                source,
            })?;
        }

        // Phase 2: the manifest commit — the single atomic point.
        metadata_writer(meta_buf)
            .await
            .map_err(|source| BTreeError::Generic {
                name: self.name.clone(),
                source,
            })?;

        // Publish the committed state in memory.
        self.last_saved_version
            .fetch_max(generation, Ordering::Release);
        self.update_metadata(|m| {
            m.stats.last_saved = meta.stats.last_saved.max(m.stats.last_saved);
            m.buckets = meta.buckets;
        });
        for (bucket_id, dirty_version) in saved_marks {
            self.mark_bucket_snapshot_saved(bucket_id, dirty_version);
        }
        // Lower the poller fast-path hint only once nothing is dirty. The
        // flush contract guarantees no mutation runs concurrently, so this
        // scan cannot race a fresh dirty mark.
        if !self.buckets.iter().any(|bucket| bucket.dirty) {
            self.dirty_hint.store(false, Ordering::Release);
        }

        Ok(FlushOutcome {
            saved: true,
            obsolete,
        })
    }

    /// Returns whether there are dirty buckets pending persistence.
    ///
    /// Cheap to poll: the full bucket scan only runs after some mutation
    /// marked a bucket dirty since the last flush.
    pub fn has_dirty_buckets(&self) -> bool {
        self.dirty_hint.load(Ordering::Acquire) && self.buckets.iter().any(|bucket| bucket.dirty)
    }

    /// Returns whether metadata has a newer logical version than the last
    /// serialized metadata snapshot.
    pub fn has_pending_metadata_flush(&self) -> bool {
        let current_version = { self.metadata.read().stats.version };
        self.last_saved_version.load(Ordering::Acquire) < current_version
    }
}
