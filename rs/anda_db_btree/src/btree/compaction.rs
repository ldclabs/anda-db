use super::*;

impl<PK, FV> BTreeIndex<PK, FV>
where
    PK: Ord + Eq + Hash + Debug + Clone + Serialize + DeserializeOwned,
    FV: Ord + Eq + Hash + Debug + Clone + Serialize + DeserializeOwned,
{
    /// Compacts fragmented buckets by re-binning all field values into fewer, properly-sized
    /// buckets using a best-fit-decreasing bin-packing strategy.
    ///
    /// This is intended as a one-time repair after the bucket-splitting bug that created
    /// many tiny buckets. After compaction all buckets are marked dirty and will be
    /// persisted on the next [`flush`](Self::flush) call.
    ///
    /// # Concurrency
    ///
    /// This method rebuilds the bucket map non-atomically, so it takes the
    /// index's mutation gate **exclusively**: concurrent `insert*` / `remove*`
    /// calls block for its duration instead of losing postings created between
    /// the `postings` snapshot and the rebuild (such a posting would belong to
    /// no bucket, and buckets are what gets serialized). Excluding *flushes*
    /// remains the caller's responsibility (see the crate-level concurrency
    /// contract). It also requires a fully loaded index.
    ///
    /// # Persistence
    ///
    /// The repacked layout becomes durable atomically with the next flush's
    /// manifest commit; every pre-compaction bucket object is reported in
    /// that flush's [`FlushOutcome::obsolete`] for best-effort deletion. No
    /// special write ordering is required. See `anda_db`'s `BTree::compact`
    /// for a production wrapper.
    ///
    /// # Returns
    ///
    /// `(old_bucket_count, new_bucket_count)`
    pub fn compact_buckets(&self) -> (usize, usize) {
        let result = self.compact_buckets_with_outcome();
        (result.old_bucket_count, result.new_bucket_count)
    }

    /// Compacts the index and explicitly reports whether the layout changed.
    /// Read-only indexes and already canonical layouts are left untouched.
    pub fn compact_buckets_with_outcome(&self) -> CompactionOutcome {
        // Exclusive: no mutation may observe — or add to — the half-rebuilt
        // bucket map. Every mutator takes the shared side of this gate before
        // touching any other lock, so the ordering is uniform and deadlock-free.
        let _mutation_guard = self.mutation_gate.write();

        let old_count = self.buckets.len();
        if self.load_state != LoadState::Ready {
            // The postings are still on disk; there is nothing to repack.
            return CompactionOutcome {
                old_bucket_count: old_count,
                new_bucket_count: old_count,
                changed: false,
            };
        }

        let limit = self.config.bucket_overload_size;
        if old_count <= 1 {
            // Nothing to merge. Rebuild only when the sole bucket is full
            // while holding several postings (a posting appended into its own
            // bucket grows in place by design), so that it can be split.
            // "Full" is `>= limit`, matching the placement predicate
            // everywhere else: a bin accepts a posting only while
            // `size + posting < limit`.
            let needs_split = self
                .buckets
                .iter()
                .any(|bucket| bucket.size >= limit && bucket.fields.len() > 1);
            if !needs_split {
                return CompactionOutcome {
                    old_bucket_count: old_count,
                    new_bucket_count: old_count,
                    changed: false,
                };
            }
        }

        // Step 1: Estimate each field value's serialized contribution.
        let mut fv_sizes: Vec<(FV, usize)> = self
            .postings
            .iter()
            .map(|entry| {
                let size = posting_entry_size(entry.key(), entry.value());
                (entry.key().clone(), size)
            })
            .collect();

        if fv_sizes.is_empty() {
            self.buckets.clear();
            self.buckets
                .insert(0, BucketState::new(0, true, UniqueVec::default(), 1));
            self.dirty_hint.store(true, Ordering::Release);
            self.max_bucket_id.store(0, Ordering::Relaxed);
            self.update_metadata(|m| {
                m.stats.version += 1;
            });
            return CompactionOutcome {
                old_bucket_count: old_count,
                new_bucket_count: 1,
                changed: true,
            };
        }

        // Step 2: Sort by size descending for better packing.
        fv_sizes.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

        // Step 3: Best-fit-decreasing bin packing. Bins are indexed by their
        // remaining capacity, so each placement costs O(log bins) instead of
        // the linear scan of first-fit, which made compaction
        // O(keys × buckets).
        // Each bin: (accumulated_size, field_values)
        let mut bins: Vec<(usize, Vec<FV>)> = Vec::new();
        // remaining capacity -> bins (indices into `bins`) with that capacity;
        // a bin is listed only while it can still take another posting.
        let mut open_bins: BTreeMap<usize, Vec<usize>> = BTreeMap::new();

        for (fv, size) in fv_sizes {
            // A bin fits when `bin_size + size < limit`, i.e. its remaining
            // capacity exceeds `size`; take the tightest such bin.
            let candidate = open_bins
                .range(size.saturating_add(1)..)
                .next()
                .map(|(remaining, _)| *remaining);
            let index = candidate.and_then(|remaining| {
                let indices = open_bins.get_mut(&remaining)?;
                let index = indices.pop();
                if indices.is_empty() {
                    open_bins.remove(&remaining);
                }
                index
            });
            let index = match index {
                Some(index) => {
                    let bin = &mut bins[index];
                    bin.0 = bin.0.saturating_add(size);
                    bin.1.push(fv);
                    index
                }
                None => {
                    bins.push((size, vec![fv]));
                    bins.len() - 1
                }
            };
            let bin_size = bins[index].0;
            if bin_size < limit {
                open_bins.entry(limit - bin_size).or_default().push(index);
            }
        }

        // Do not manufacture dirty state for an already canonical layout.
        // Ordering within a bucket is irrelevant; it is serialized as a map.
        let unchanged = old_count == bins.len()
            && bins.iter().enumerate().all(|(i, (_, fields))| {
                self.buckets.get(&(i as u32)).is_some_and(|bucket| {
                    bucket.fields.len() == fields.len()
                        && fields.iter().all(|fv| {
                            bucket.fields.contains(fv)
                                && self
                                    .postings
                                    .get(fv)
                                    .is_some_and(|p| p.bucket_id == i as u32)
                        })
                })
            });
        if unchanged {
            return CompactionOutcome {
                old_bucket_count: old_count,
                new_bucket_count: old_count,
                changed: false,
            };
        }

        // Step 4: Rebuild buckets.
        self.buckets.clear();
        let new_count = bins.len();
        let max_id = new_count.saturating_sub(1) as u32;

        for (i, (size, field_values)) in bins.into_iter().enumerate() {
            let bucket_id = i as u32;

            // Update posting references.
            for fv in &field_values {
                if let Some(mut posting) = self.postings.get_mut(fv) {
                    posting.bucket_id = bucket_id;
                }
            }

            self.buckets.insert(
                bucket_id,
                BucketState::new(size, true, field_values.into(), 1),
            );
        }
        self.dirty_hint.store(true, Ordering::Release);

        self.max_bucket_id.store(max_id, Ordering::Relaxed);
        self.update_metadata(|m| {
            m.stats.version += 1;
        });

        CompactionOutcome {
            old_bucket_count: old_count,
            new_bucket_count: new_count,
            changed: true,
        }
    }
}
