use super::*;

impl<T: Tokenizer> BM25Index<T> {
    /// Repacks all tokens using best-fit-decreasing bin packing.
    ///
    /// Over the lifetime of an index — especially before bug fixes that tuned
    /// the bucket splitting logic — repeated inserts and removes can leave
    /// behind many under-filled buckets. `compact_buckets` estimates each
    /// posting's serialized CBOR size and performs a Best-Fit-Decreasing bin
    /// packing with [`BM25Config::bucket_overload_size`] as the bin capacity.
    ///
    /// After compaction:
    ///
    /// * bucket ids are reassigned to a contiguous `0..new_count` range;
    /// * every resulting bucket is marked dirty so the next
    ///   [`flush`](Self::flush) will rewrite the full on-disk layout.
    ///
    /// This is a heuristic, not a guarantee of the optimal bucket count. Its
    /// cost is `O(P + U log U)` for P posting entries and U distinct tokens,
    /// and it requires a fully loaded index. It rebuilds the bucket map
    /// non-atomically, so it takes the index's mutation gate **exclusively**:
    /// concurrent `insert`/`remove` calls block for its duration instead of
    /// creating a posting that lands in no bucket at all (only bucket contents
    /// are serialized, so such a token would be silently dropped by the next
    /// flush). Excluding *flushes* remains the caller's responsibility (see
    /// the [`BM25Index`] concurrency contract). The repacked layout becomes
    /// durable atomically with the next flush's manifest commit; the
    /// pre-compaction bucket objects are reported in that flush's
    /// [`FlushOutcome::obsolete`].
    ///
    /// # Returns
    ///
    /// `(old_bucket_count, new_bucket_count)`.
    pub fn compact_buckets(&self) -> (usize, usize) {
        // Exclusive: no mutation may observe — or add to — the half-rebuilt
        // bucket map. Every mutator takes the shared side of this gate before
        // touching any other lock, so the ordering is uniform and deadlock-free.
        let _mutation_guard = self.mutation_gate.write();

        let old_count = self.buckets.len();
        // The postings are still on disk; the bucket map is placeholders, so
        // there is nothing to repack and rebuilding it would drop every
        // committed bucket from the manifest.
        if !self.is_fully_loaded() || old_count <= 1 {
            return (old_count, old_count);
        }

        // Step 1: Estimate each token's serialized contribution.
        let mut token_sizes: Vec<(String, usize)> = self
            .postings
            .iter()
            .map(|entry| {
                let size = cbor_serialized_size(&(entry.key(), entry.value())) + 2;
                (entry.key().clone(), size)
            })
            .collect();

        if token_sizes.is_empty() {
            self.buckets.clear();
            self.buckets.insert(
                0,
                Bucket {
                    dirty_version: 1,
                    ..Default::default()
                },
            );
            self.max_bucket_id.store(0, Ordering::Relaxed);
            self.update_metadata(|m| {
                m.stats.version += 1;
            });
            return (old_count, 1);
        }

        // Step 2: Sort by size descending for better packing.
        token_sizes.sort_unstable_by_key(|b| std::cmp::Reverse(b.1));

        // Step 3: Best-fit-decreasing bin packing in O(n log n).
        // `by_remaining` maps remaining-capacity -> bin indices. We pick the bin with the
        // smallest remaining capacity that still fits the token (best fit), which keeps
        // bucket count low without scanning all bins per token.
        let limit = self.config.bucket_overload_size;
        // Each bin: (accumulated_size, tokens)
        let mut bins: Vec<(usize, Vec<String>)> = Vec::new();
        // remaining_capacity -> bin indices with that capacity
        let mut by_remaining: std::collections::BTreeMap<usize, Vec<usize>> =
            std::collections::BTreeMap::new();

        for (token, size) in token_sizes {
            // Find smallest remaining capacity >= size + 1 (preserve `<` limit semantics).
            let needed = size.saturating_add(1);
            let chosen = by_remaining
                .range_mut(needed..)
                .next()
                .and_then(|(_, idxs)| idxs.pop().map(|i| (i, idxs.is_empty())));

            match chosen {
                Some((idx, bucket_now_empty)) => {
                    let old_remaining = limit.saturating_sub(bins[idx].0);
                    if bucket_now_empty {
                        by_remaining.remove(&old_remaining);
                    }
                    bins[idx].0 += size;
                    bins[idx].1.push(token);
                    let new_remaining = limit.saturating_sub(bins[idx].0);
                    by_remaining.entry(new_remaining).or_default().push(idx);
                }
                None => {
                    let idx = bins.len();
                    bins.push((size, vec![token]));
                    let new_remaining = limit.saturating_sub(size);
                    by_remaining.entry(new_remaining).or_default().push(idx);
                }
            }
        }

        // Step 4: Rebuild buckets.
        self.buckets.clear();
        let new_count = bins.len();
        let max_id = new_count.saturating_sub(1) as u32;

        for (i, (size, tokens)) in bins.into_iter().enumerate() {
            let bucket_id = i as u32;

            // Update posting references and collect doc_ids.
            let mut doc_ids = FxHashSet::default();
            for token in &tokens {
                if let Some(mut posting) = self.postings.get_mut(token) {
                    posting.0 = bucket_id;
                    for (doc_id, _) in posting.1.iter() {
                        doc_ids.insert(*doc_id);
                    }
                }
            }

            // Token sizes drove the packing; the per-document token counts
            // the bucket object also carries are added here so `size` keeps
            // estimating the whole object, as `insert` and a reload do.
            let mut size = size;
            for doc_id in &doc_ids {
                if let Some(count) = self.doc_tokens.get(doc_id) {
                    size += doc_entry_size(*doc_id, *count);
                }
            }

            self.buckets.insert(
                bucket_id,
                Bucket {
                    dirty_version: 1,
                    saved_version: 0,
                    size,
                    tokens: tokens.into_iter().collect(),
                    doc_ids,
                },
            );
        }

        self.max_bucket_id.store(max_id, Ordering::Relaxed);
        self.update_metadata(|m| {
            m.stats.version += 1;
        });

        (old_count, new_count)
    }
}
