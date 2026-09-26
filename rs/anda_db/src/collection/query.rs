//! Collection query implementation.
use super::*;

impl Collection {
    /// Searches for documents matching the given query and returns them.
    ///
    /// # Arguments
    /// * `query` - The search query parameters
    ///
    /// # Returns
    /// A vector of matching documents, or an error if the search fails
    pub async fn search(&self, query: Query) -> Result<Vec<Document>, DBError> {
        self.search_with_options(query, SearchOptions::default())
            .await
    }

    /// Searches using explicit resource and recall controls.
    pub async fn search_with_options(
        &self,
        query: Query,
        options: SearchOptions,
    ) -> Result<Vec<Document>, DBError> {
        let ids = self.search_ids_with_options(query, options).await?;
        let schema = self.schema();
        let mut docs = Vec::with_capacity(ids.len());
        let mut stream = futures::stream::iter(ids)
            .map(|id| {
                let storage = self.storage.clone();
                async move { (id, storage.get::<DocumentOwned>(&Self::doc_path(id)).await) }
            })
            .buffered(self.io_concurrency());
        while let Some((id, result)) = stream.next().await {
            match result {
                Ok((doc, _)) => match Document::try_from_doc(schema.clone(), doc) {
                    Ok(doc) => docs.push(doc),
                    Err(err) => {
                        // One stored document that no longer satisfies the
                        // schema (legacy data a later validation tightening
                        // rejects) must not fail every search that matches
                        // it. Skip it; `get`/`update` on the id still report
                        // the error, and `remove` can delete it.
                        log::warn!(
                            action = "Collection::search",
                            collection = self.name,
                            doc_id = id;
                            "Skipping document that does not match the schema: {err:?}",
                        );
                    }
                },
                Err(DBError::NotFound { .. }) => {
                    // Under the poison-on-unknown-outcome contract a live
                    // handle should never observe a dead id: crash recovery
                    // happens on reopen. Log the anomaly; `reconcile_storage`
                    // is the explicit repair path.
                    log::warn!(
                        action = "Collection::search",
                        collection = self.name,
                        doc_id = id;
                        "Skipping dead document id without a backing object",
                    );
                }
                Err(err) => return Err(err),
            }
        }
        Ok(docs)
    }

    /// Drops document ids whose objects are missing from storage, from the
    /// in-memory id structures **and from every index that can still be
    /// purged**, so the next flush persists the repair.
    ///
    /// Only [`Self::reconcile_storage`] calls this: under the
    /// poison-on-unknown-outcome contract, dead ids (bitmap entry without a
    /// backing object) can only be produced by a crash, and reopen recovery
    /// resolves them before the handle serves reads. No-op in read-only mode
    /// or when the ids are not in the bitmap.
    ///
    /// Dropping the id from the bitmap alone is not a repair: the derived
    /// index postings survive, so a unique B-tree key keeps rejecting new
    /// documents (`AlreadyExists`) forever and `query_ids` / `search_ids`
    /// keep returning the dead id. The document body is gone, so its indexed
    /// values cannot be recomputed; each index is purged as far as its API
    /// allows — see [`Self::purge_dead_ids_from_indexes`].
    pub(super) fn heal_missing_docs(&self, ids: &BTreeSet<DocumentId>, now_ms: u64) {
        if ids.is_empty() || self.is_read_only() {
            return;
        }

        self.purge_dead_ids_from_indexes(ids, now_ms);

        // Update both id representations under one lock for the batch.
        let removed: Vec<DocumentId> = {
            let mut doc_ids = self.doc_ids.write();
            let mut removed = Vec::new();
            for id in ids {
                if doc_ids.remove(id) {
                    removed.push(*id);
                }
            }
            removed
        };

        if !removed.is_empty() {
            self.update_metadata(|meta| {
                meta.stats.version += 1;
            });
            log::warn!(
                action = "Collection::heal_missing_docs",
                collection = self.name,
                dropped = removed.len();
                "Removed {} dead document ids without a backing object: {removed:?}",
                removed.len(),
            );
        }
    }

    /// Removes index postings that reference the given document ids without
    /// knowing the indexed values.
    ///
    /// Used for dead ids (the document bodies are gone) and for live ids
    /// whose historical indexed values cannot be derived (a mutation-intent
    /// image that no longer decodes) — in both cases value-keyed removal is
    /// impossible, so each index is purged as far as it can be purged by id:
    ///
    /// - **HNSW** stores one vector per id and is purged directly.
    /// - **B-tree** needs the key, so the key space is swept **once** for the
    ///   whole dead set and every posting list that references a dead id is
    ///   repaired. That is O(distinct keys) per index, which is proportional
    ///   to the `data/` listing [`Self::reconcile_storage`] already performs.
    /// - **BM25** postings are keyed by the document's tokens, so the inverted
    ///   index is swept **once** for the whole dead set as well
    ///   ([`anda_db_tfs::BM25Index::purge_ids`]), which also drops the dead
    ///   documents' lengths from the BM25 counters the scores are derived
    ///   from.
    ///
    /// Both sweeps are `O(index size)` per index, which is proportional to the
    /// `data/` listing [`Self::reconcile_storage`] already performs.
    pub(super) fn purge_dead_ids_from_indexes(&self, dead_ids: &BTreeSet<DocumentId>, now_ms: u64) {
        for index in &self.btree_indexes {
            index.purge_ids(dead_ids, now_ms);
        }

        for index in &self.hnsw_indexes {
            for id in dead_ids {
                index.remove(*id, now_ms);
            }
        }

        for index in &self.bm25_indexes {
            index.purge_ids(dead_ids, now_ms);
        }
    }

    /// Searches for documents matching the given query and deserializes them into the specified type.
    ///
    /// # Type Parameters
    /// * `T` - The type to deserialize documents into
    ///
    /// # Arguments
    /// * `query` - The search query parameters
    ///
    /// # Returns
    /// A vector of deserialized objects of type T, or an error if the search or deserialization fails
    pub async fn search_as<T>(&self, query: Query) -> Result<Vec<T>, DBError>
    where
        T: DeserializeOwned,
    {
        let docs = self.search(query).await?;
        let mut rt = Vec::with_capacity(docs.len());
        for doc in docs {
            rt.push(doc.try_into()?);
        }
        Ok(rt)
    }

    /// Searches for documents matching the given query and returns only their IDs.
    ///
    /// This is more efficient than retrieving full documents when only IDs are needed.
    ///
    /// # Limit semantics
    ///
    /// `Query::limit` defaults to `10` and is clamped to
    /// [`Collection::MAX_SEARCH_LIMIT`]. An explicit `limit` of `0` returns
    /// an empty result (consistent with the underlying indexes' `top_k = 0`
    /// behavior). Each search index is asked for up to `limit * 10`
    /// candidates before reranking and filtering, capped at 4096 to bound
    /// the per-query search breadth.
    ///
    /// # Arguments
    /// * `query` - The search query parameters
    ///
    /// # Returns
    /// A vector of matching document IDs, or an error if the search fails
    pub async fn search_ids(&self, query: Query) -> Result<Vec<DocumentId>, DBError> {
        self.search_ids_with_options(query, SearchOptions::default())
            .await
    }

    /// Hybrid search with bounded oversampling and optional selective subset
    /// scoring. Prefiltered fusion ranks within the matching subset; disable
    /// prefiltering for the historical global-rank-then-filter behavior.
    pub async fn search_ids_with_options(
        &self,
        query: Query,
        options: SearchOptions,
    ) -> Result<Vec<DocumentId>, DBError> {
        self.ensure_recovered().await?;
        query
            .validate_complexity()
            .map_err(|source| DBError::Generic {
                name: self.name.clone(),
                source: source.into(),
            })?;
        self.search_count.fetch_add(1, Ordering::Relaxed);
        let limit = query.limit.unwrap_or(10).min(Self::MAX_SEARCH_LIMIT);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let Some(params) = query.search else {
            return match query.filter {
                Some(filter) => self.filter_by_field(filter, &[], limit, ScanOrder::Ascending),
                None => Ok(Vec::new()),
            };
        };
        if params.text.is_some() && self.bm25_indexes.is_empty() {
            return Err(DBError::Index {
                name: self.name.clone(),
                source: "text search requires a BM25 index, but none exists".into(),
            });
        }
        let matching_hnsw: Vec<_> = self
            .hnsw_indexes
            .iter()
            .filter(|index| {
                params
                    .vector
                    .as_ref()
                    .is_some_and(|v| v.len() == index.dimension())
            })
            .collect();
        if let Some(vector) = &params.vector
            && matching_hnsw.is_empty()
        {
            if params.text.is_none() {
                return Err(DBError::Index {
                    name: self.name.clone(),
                    source: format!(
                        "no HNSW index matches the query vector dimension {}",
                        vector.len()
                    )
                    .into(),
                });
            }
            // A hybrid query keeps its BM25 hits, but the dropped vector half
            // must stay visible: it usually means an embedding-model change.
            log::warn!(
                action = "Collection::search_ids",
                collection = self.name,
                dimension = vector.len();
                "No HNSW index matches the query vector dimension; returning text-only results",
            );
        }
        let prefilter_limit = options.prefilter_limit.min(4096);
        let selected = if let Some(filter) = &query.filter
            && prefilter_limit > 0
            && self.filter_cardinality_hint(filter) <= prefilter_limit
        {
            let ids = self.filter_by_field(
                filter.clone(),
                &[],
                prefilter_limit + 1,
                ScanOrder::Ascending,
            )?;
            (ids.len() <= prefilter_limit).then_some(ids)
        } else {
            None
        };
        let max_candidates = options.max_candidates.clamp(limit, 4096);
        let mut breadth = limit
            .saturating_mul(options.oversample.max(1))
            .min(max_candidates);
        let reranker = params.reranker.unwrap_or_default();
        // BM25 already scores every matching posting for any top-k. Retain
        // one bounded ranking and reuse prefixes when adaptive filtering asks
        // for a wider window, instead of tokenizing/scoring again each round.
        let text_rankings = if selected.is_none() && query.filter.is_some() && options.adaptive {
            if let Some(text) = &params.text {
                let mut rankings = Vec::new();
                for index in &self.bm25_indexes {
                    let hits = if params.logical_search {
                        index.try_search_advanced(
                            text,
                            max_candidates,
                            params.bm25_params.clone(),
                        )?
                    } else {
                        index.search(text, max_candidates, params.bm25_params.clone())
                    };
                    rankings.push(hits.into_iter().map(|hit| hit.0).collect::<Vec<_>>());
                }
                Some(rankings)
            } else {
                None
            }
        } else {
            None
        };
        loop {
            let mut rankings = Vec::new();
            if let Some(text) = &params.text {
                for (number, index) in self.bm25_indexes.iter().enumerate() {
                    if let Some(cached) = &text_rankings {
                        rankings.push(
                            cached[number]
                                .iter()
                                .take(breadth)
                                .copied()
                                .collect::<Vec<_>>(),
                        );
                        continue;
                    }
                    let hits = if let Some(ids) = &selected {
                        index.search_in_ids(
                            text,
                            breadth,
                            params.bm25_params.clone(),
                            ids,
                            params.logical_search,
                        )?
                    } else if params.logical_search {
                        index.try_search_advanced(text, breadth, params.bm25_params.clone())?
                    } else {
                        index.search(text, breadth, params.bm25_params.clone())
                    };
                    rankings.push(hits.into_iter().map(|hit| hit.0).collect::<Vec<_>>());
                }
            }
            if let Some(vector) = &params.vector {
                for index in &matching_hnsw {
                    let hits = if let Some(ids) = &selected {
                        index.search_in_ids(vector, breadth, ids)?
                    } else {
                        index.try_search(vector, breadth)?
                    };
                    rankings.push(hits.into_iter().map(|hit| hit.0).collect::<Vec<_>>());
                }
            }
            let exhausted = rankings.iter().all(|list| list.len() < breadth);
            // Each index list and RRF's score map already contain unique ids.
            let candidates = if rankings.len() == 1 {
                rankings.pop().unwrap()
            } else {
                reranker
                    .rerank(&rankings)
                    .into_iter()
                    .map(|(id, _)| id)
                    .collect()
            };
            if candidates.is_empty() {
                return Ok(Vec::new());
            }
            let mut matched = if selected.is_some() {
                candidates
            } else if let Some(filter) = &query.filter {
                self.filter_by_field(filter.clone(), &candidates, 0, ScanOrder::Ascending)?
            } else {
                candidates
            };
            if matched.len() >= limit
                || selected.is_some()
                || query.filter.is_none()
                || !options.adaptive
                || exhausted
                || breadth == max_candidates
            {
                matched.truncate(limit);
                return Ok(matched);
            }
            breadth = breadth.saturating_mul(2).min(max_candidates);
        }
    }

    /// Cheap hints only: wrong estimates can change the plan, never results.
    pub(super) fn filter_cardinality_hint(&self, filter: &Filter) -> usize {
        match filter {
            Filter::Field((name, RangeQuery::Eq(value))) => {
                if name == Schema::ID_KEY {
                    1
                } else {
                    self.btree_indexes
                        .iter()
                        .find(|i| i.name() == name)
                        .map_or(usize::MAX, |index| {
                            index.query_with(value, |ids| Some(ids.len())).unwrap_or(0)
                        })
                }
            }
            Filter::Field((name, RangeQuery::Include(ids))) if name == Schema::ID_KEY => ids.len(),
            Filter::And(filters) => filters
                .iter()
                .map(|f| self.filter_cardinality_hint(f))
                .min()
                .unwrap_or(0),
            Filter::Or(filters) => filters.iter().fold(0usize, |sum, f| {
                sum.saturating_add(self.filter_cardinality_hint(f))
            }),
            Filter::Field((name, query)) if name != Schema::ID_KEY => self
                .btree_indexes
                .iter()
                .find(|index| index.name() == name)
                .and_then(|index| index.estimate_cardinality(query.clone(), 4097).ok())
                .filter(|count| *count < 4097)
                .unwrap_or(usize::MAX),
            _ => usize::MAX,
        }
    }

    /// Queries the **smallest** matching document IDs.
    ///
    /// # Page semantics
    ///
    /// The result is the first `limit` matching ids in ascending id order, for
    /// every filter shape. Which end you get is a property of the method you
    /// call, never of the filter you pass: `_id Lt cursor` and
    /// `And([user Eq u, _id Lt cursor])` page identically here, and
    /// [`Collection::query_last_ids`] returns the other end for both. `_id`
    /// scans walk in the direction the method asks for and stop after one
    /// page; a B-tree field filter is evaluated in full (its key order is not
    /// id order) and trimmed to the requested end afterwards.
    ///
    /// # Limit semantics
    ///
    /// `limit` is clamped to [`Collection::MAX_SEARCH_LIMIT`], and `None`
    /// means "as many as that bound allows" rather than "every match": this
    /// is a public entry point (reachable over HTTP as `doc.query_ids`) and
    /// an unbounded filter materializes one `u64` per matching document.
    /// An explicit `Some(0)` returns an empty result, deliberately keeping
    /// the same zero-is-nothing convention as [`Collection::search_ids`]
    /// instead of overloading it as "unlimited" — the internal scan already
    /// uses `0` for "unbounded", and letting a caller reach that meaning is
    /// exactly the trap this bound removes. [`Collection::query_all_ids`] is
    /// the unbounded entry point.
    ///
    /// # Arguments
    /// * `filter` - The filter condition to apply
    /// * `limit` - Maximum number of results to return, clamped as above.
    ///
    /// # Returns
    /// Matching document IDs in ascending order, or an error if filtering fails.
    pub async fn query_ids(
        &self,
        filter: Filter,
        limit: Option<usize>,
    ) -> Result<Vec<DocumentId>, DBError> {
        self.query_ids_from(filter, limit, ScanOrder::Ascending)
            .await
    }

    /// Queries the **largest** matching document IDs — the newest page, for
    /// collections whose ids grow with time.
    ///
    /// Identical to [`Collection::query_ids`] except for which end of the
    /// match set it keeps: the result is the last `limit` matching ids, still
    /// returned in ascending id order. This is the entry point for
    /// newest-first cursor pagination (`And([owner Eq u, _id Lt cursor])`),
    /// which otherwise has to load every match and sort it in the caller.
    ///
    /// # Arguments
    /// * `filter` - The filter condition to apply
    /// * `limit` - Maximum number of results to return, clamped as in
    ///   [`Collection::query_ids`].
    ///
    /// # Returns
    /// Matching document IDs in ascending order, or an error if filtering fails.
    pub async fn query_last_ids(
        &self,
        filter: Filter,
        limit: Option<usize>,
    ) -> Result<Vec<DocumentId>, DBError> {
        self.query_ids_from(filter, limit, ScanOrder::Descending)
            .await
    }

    pub(super) async fn query_ids_from(
        &self,
        filter: Filter,
        limit: Option<usize>,
        order: ScanOrder,
    ) -> Result<Vec<DocumentId>, DBError> {
        self.ensure_recovered().await?;
        filter
            .validate_complexity()
            .map_err(|source| DBError::Generic {
                name: self.name.clone(),
                source: source.into(),
            })?;

        self.search_count.fetch_add(1, Ordering::Relaxed);
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        let limit = limit
            .unwrap_or(Self::MAX_SEARCH_LIMIT)
            .min(Self::MAX_SEARCH_LIMIT);
        self.filter_by_field(filter, &[], limit, order)
    }

    /// Queries **every** document ID matching a filter, with no result bound.
    ///
    /// [`Collection::query_ids`] clamps its result to
    /// [`Collection::MAX_SEARCH_LIMIT`] because it is reachable over HTTP.
    /// In-process callers whose correctness depends on completeness — cascade
    /// deletion, link re-pointing, set-difference (`NOT`) evaluation — must
    /// use this method instead: a silently truncated result there corrupts
    /// data rather than shortening a page. The caller owns bounding the
    /// result (memory is one `u64` per match) and must not expose this
    /// entry point to untrusted request paths.
    ///
    /// # Arguments
    /// * `filter` - The filter condition to apply
    ///
    /// # Returns
    /// All matching document IDs in ascending order, or an error if filtering
    /// fails.
    pub async fn query_all_ids(&self, filter: Filter) -> Result<Vec<DocumentId>, DBError> {
        self.ensure_recovered().await?;
        filter
            .validate_complexity()
            .map_err(|source| DBError::Generic {
                name: self.name.clone(),
                source: source.into(),
            })?;

        self.search_count.fetch_add(1, Ordering::Relaxed);
        // The internal scan uses `0` for "unbounded".
        self.filter_by_field(filter, &[], 0, ScanOrder::Ascending)
    }

    /// Owned-handle variant that offloads broad CPU scans to the bounded
    /// query pool. Selective point lookups avoid the scheduling overhead.
    pub async fn query_candidate_ids_on_worker(
        self: Arc<Self>,
        filter: Filter,
        limit: usize,
    ) -> Result<Vec<DocumentId>, DBError> {
        self.ensure_recovered().await?;
        filter
            .validate_complexity()
            .map_err(|source| DBError::Generic {
                name: self.name.clone(),
                source: source.into(),
            })?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        if self.filter_cardinality_hint(&filter) <= 4096 {
            return self.query_candidate_ids(filter, limit).await;
        }
        self.search_count.fetch_add(1, Ordering::Relaxed);
        crate::query::run_query_task(move || {
            self.filter_by_field(filter, &[], limit, ScanOrder::Ascending)
        })
        .await?
    }

    /// Complete owned-handle scan on the bounded query worker pool.
    pub async fn query_all_ids_on_worker(
        self: Arc<Self>,
        filter: Filter,
    ) -> Result<Vec<DocumentId>, DBError> {
        self.ensure_recovered().await?;
        filter
            .validate_complexity()
            .map_err(|source| DBError::Generic {
                name: self.name.clone(),
                source: source.into(),
            })?;
        self.search_count.fetch_add(1, Ordering::Relaxed);
        crate::query::run_query_task(move || {
            self.filter_by_field(filter, &[], 0, ScanOrder::Ascending)
        })
        .await?
    }

    /// In-process bounded candidate query. Unlike query_ids, the explicit
    /// caller budget is not clamped to the HTTP page cap. Reaching the limit
    /// is not proof of completeness: callers request budget + 1 to detect
    /// exhaustion before loading documents. Zero returns no ids.
    pub async fn query_candidate_ids(
        &self,
        filter: Filter,
        limit: usize,
    ) -> Result<Vec<DocumentId>, DBError> {
        self.ensure_recovered().await?;
        filter
            .validate_complexity()
            .map_err(|source| DBError::Generic {
                name: self.name.clone(),
                source: source.into(),
            })?;
        self.search_count.fetch_add(1, Ordering::Relaxed);
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.filter_by_field(filter, &[], limit, ScanOrder::Ascending)
    }

    /// Applies a filter to an explicit candidate set, preserving its order.
    /// An empty set is empty, unlike the internal unbounded-scan sentinel.
    pub async fn filter_candidate_ids(
        &self,
        filter: Filter,
        candidates: &[DocumentId],
    ) -> Result<Vec<DocumentId>, DBError> {
        self.ensure_recovered().await?;
        filter
            .validate_complexity()
            .map_err(|source| DBError::Generic {
                name: self.name.clone(),
                source: source.into(),
            })?;
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        self.filter_by_field(filter, candidates, 0, ScanOrder::Ascending)
    }

    /// Bounded ascending ID query plus per-query execution work counters.
    pub async fn query_ids_with_stats(
        &self,
        filter: Filter,
        limit: Option<usize>,
    ) -> Result<(Vec<DocumentId>, QueryStats), DBError> {
        self.ensure_recovered().await?;
        filter
            .validate_complexity()
            .map_err(|source| DBError::Generic {
                name: self.name.clone(),
                source: source.into(),
            })?;
        let mut stats = QueryStats::default();
        let limit = limit
            .unwrap_or(Self::MAX_SEARCH_LIMIT)
            .min(Self::MAX_SEARCH_LIMIT);
        if limit == 0 {
            return Ok((Vec::new(), stats));
        }
        self.search_count.fetch_add(1, Ordering::Relaxed);
        let ids =
            self.filter_by_field_tracked(filter, &[], limit, ScanOrder::Ascending, &mut stats)?;
        stats.returned_ids = ids.len();
        Ok((ids, stats))
    }

    /// Gets a document by its ID.
    ///
    /// # Arguments
    /// * `id` - The ID of the document to retrieve
    ///
    /// # Returns
    /// The document if found, or an error if retrieval fails
    pub async fn get(&self, id: DocumentId) -> Result<Document, DBError> {
        self.ensure_recovered().await?;
        if self.doc_ids.read().contains(&id) {
            self.get_count.fetch_add(1, Ordering::Relaxed);

            let path = Self::doc_path(id);
            match self.storage.get::<DocumentOwned>(&path).await {
                Ok((doc, _)) => {
                    let doc = Document::try_from_doc(self.schema(), doc)?;
                    return Ok(doc);
                }
                Err(DBError::NotFound { .. }) => {
                    // See the search path: a dead id on a live handle is an
                    // anomaly, repaired explicitly via `reconcile_storage`.
                    log::warn!(
                        action = "Collection::get",
                        collection = self.name,
                        doc_id = id;
                        "Document id has no backing object",
                    );
                }
                Err(err) => return Err(err),
            }
        }

        Err(DBError::NotFound {
            name: "document".to_string(),
            path: self.name.clone(),
            source: format!("Document {id} not found").into(),
            _id: id,
        })
    }

    /// Gets a document by its ID and deserializes it into the specified type.
    ///
    /// # Type Parameters
    /// * `T` - The type to deserialize the document into
    ///
    /// # Arguments
    /// * `id` - The ID of the document to retrieve
    ///
    /// # Returns
    /// The deserialized object of type T if found, or an error if retrieval or deserialization fails
    pub async fn get_as<T>(&self, id: DocumentId) -> Result<T, DBError>
    where
        T: DeserializeOwned,
    {
        let doc = self.get(id).await?;
        let obj = doc.try_into()?;
        Ok(obj)
    }

    /// Filters documents by a field condition.
    ///
    /// # Arguments
    /// * `filter` - The filter condition to apply
    /// * `candidates` - Optional list of document IDs to filter (if empty, all documents are considered)
    /// * `limit` - The number of results to stop retrieving. The returned vector may be shorter or larger than this limit.
    ///
    /// # Returns
    /// A vector of document IDs matching the filter paired with the direction
    /// the scan walked (see [`ScanOrder`]), or an error if filtering fails
    pub(super) fn filter_by_field(
        &self,
        filter: Filter,
        candidates: &[DocumentId],
        limit: usize,
        order: ScanOrder,
    ) -> Result<Vec<DocumentId>, DBError> {
        self.filter_by_field_tracked(filter, candidates, limit, order, &mut QueryStats::default())
    }

    fn filter_by_field_tracked(
        &self,
        filter: Filter,
        candidates: &[DocumentId],
        limit: usize,
        order: ScanOrder,
        stats: &mut QueryStats,
    ) -> Result<Vec<DocumentId>, DBError> {
        if candidates.is_empty() {
            let mut result =
                self.filter_by_field_with_tracked(filter, None, limit, order, stats)?;
            result.sort_unstable();
            order.truncate(&mut result, limit);
            Ok(result)
        } else {
            let cand_set: FxHashSet<DocumentId> = candidates.iter().copied().collect();
            let matched: FxHashSet<DocumentId> = self
                .filter_by_field_with_tracked(filter, Some(&cand_set), 0, order, stats)?
                .into_iter()
                .collect();

            let mut result = Vec::with_capacity(matched.len().min(candidates.len()));
            for id in candidates {
                if matched.contains(id) {
                    result.push(*id);
                }
            }
            // The result follows the caller's candidate order (relevance
            // descending for a hybrid search) and the inner scan ran unbounded,
            // so an over-long result is always trimmed from the tail by the
            // caller, regardless of `order`.
            Ok(result)
        }
    }

    /// Inner implementation of `filter_by_field` using a `FxHashSet` for O(1) candidate lookups.
    ///
    /// `order` is the end the caller wants. Walks over the id set itself
    /// (`_id` filters, complements) run that way and stop after `limit`
    /// hits; everything else — B-tree field scans (key order is not id
    /// order) and composite filters (an operand bounded on its own would
    /// drop matches the whole should keep) — is evaluated in full and the
    /// caller trims the match set to `limit`.
    fn filter_by_field_with_tracked(
        &self,
        filter: Filter,
        candidates: Option<&FxHashSet<DocumentId>>,
        limit: usize,
        order: ScanOrder,
        stats: &mut QueryStats,
    ) -> Result<Vec<DocumentId>, DBError> {
        stats.peak_intermediate_ids = stats
            .peak_intermediate_ids
            .max(candidates.map_or(0, FxHashSet::len));
        if candidates.is_some_and(|ids| ids.is_empty()) {
            return Ok(Vec::new());
        }
        if !matches!(filter, Filter::Field(_)) && only_id_fields(&filter) {
            let range =
                RangeQuery::try_convert_from(id_filter_range(filter)).map_err(|source| {
                    DBError::Generic {
                        name: self.name.clone(),
                        source,
                    }
                })?;
            return Ok(self.filter_by_id(range, candidates, limit, order));
        }
        match filter {
            Filter::Field((index_name, filter)) => {
                if index_name == Schema::ID_KEY {
                    let filter: RangeQuery<u64> =
                        RangeQuery::try_convert_from(filter).map_err(|err| DBError::Generic {
                            name: self.name.clone(),
                            source: err,
                        })?;
                    Ok(self.filter_by_id(filter, candidates, limit, order))
                } else if let Some(index) =
                    self.btree_indexes.iter().find(|i| i.name() == index_name)
                {
                    if let Some(candidates) = candidates {
                        let (mut ids, work) = index.intersect_ids(filter, candidates)?;
                        stats.membership_probes += work;
                        ids.sort_unstable();
                        order.truncate(&mut ids, limit);
                        return Ok(ids);
                    }
                    if let RangeQuery::Eq(value) = &filter {
                        let ids = index.ordered_ids(value)?.unwrap_or_default();
                        stats.index_keys += 1;
                        stats.ordered_snapshot_ids += ids.len();
                        let take = if limit == 0 {
                            ids.len()
                        } else {
                            limit.min(ids.len())
                        };
                        let start = if order.is_descending() {
                            ids.len() - take
                        } else {
                            0
                        };
                        stats.posting_ids += take;
                        return Ok(ids[start..start + take].to_vec());
                    }
                    // Ranges may map a document under multiple unordered keys;
                    // keep a bounded, deduplicated id-order page.
                    let keep = |id: &&DocumentId| candidates.is_none_or(|s| s.contains(*id));
                    if limit == 0 {
                        let mut rt = Vec::new();
                        index.try_range_query_ids(filter, false, |ids| {
                            stats.index_keys += 1;
                            stats.posting_ids += ids.len();
                            rt.extend(ids.iter().filter(keep));
                            true
                        })?;
                        rt.sort_unstable();
                        rt.dedup();
                        return Ok(rt);
                    }
                    let mut rt: BTreeSet<DocumentId> = BTreeSet::new();
                    index.try_range_query_ids(filter, false, |ids| {
                        stats.index_keys += 1;
                        stats.posting_ids += ids.len();
                        for id in ids.iter().filter(keep) {
                            order.retain_id(&mut rt, *id, limit);
                        }
                        true
                    })?;
                    Ok(rt.into_iter().collect())
                } else {
                    Err(DBError::Index {
                        name: self.name.clone(),
                        source: format!("BTree index {index_name:?} not found").into(),
                    })
                }
            }
            Filter::Or(queries) => {
                // The first/last K of a union is contained in the same end of
                // each branch. Retain a bounded, deduplicated union as we go.
                let mut result = BTreeSet::new();
                for query in queries {
                    for id in
                        self.filter_by_field_with_tracked(*query, candidates, limit, order, stats)?
                    {
                        order.retain_id(&mut result, id, limit);
                    }
                }
                Ok(result.into_iter().collect())
            }
            Filter::And(mut queries) => {
                // Complete dense results use a bulk intersection: taking a
                // posting lock per returned id is wasteful without a limit.
                if limit > 0
                    && let Some(result) =
                        self.indexed_id_page(&queries, candidates, limit, order, stats)
                {
                    return result;
                }
                queries.sort_by_cached_key(|filter| self.filter_cardinality_hint(filter));
                let mut iter = queries.into_iter();
                let Some(query) = iter.next() else {
                    return Ok(Vec::new());
                };
                let mut rt: FxHashSet<DocumentId> = self
                    .filter_by_field_with_tracked(*query, candidates, 0, order, stats)?
                    .into_iter()
                    .collect();

                for query in iter {
                    if rt.is_empty() {
                        return Ok(Vec::new());
                    }
                    rt = self
                        .filter_by_field_with_tracked(*query, Some(&rt), 0, order, stats)?
                        .into_iter()
                        .collect();
                    if rt.is_empty() {
                        return Ok(Vec::new());
                    }
                }

                // Every operand ran unbounded, so this is the complete
                // intersection; trim it to the requested end.
                let mut result: Vec<_> = rt.into_iter().collect();
                result.sort_unstable();
                order.truncate(&mut result, limit);
                Ok(result)
            }
            Filter::Not(query) => {
                let exclude: FxHashSet<u64> = self
                    .filter_by_field_with_tracked(*query, candidates, 0, order, stats)?
                    .into_iter()
                    .collect();
                Ok(self.walk_complement(&exclude, candidates, limit, order))
            }
        }
    }

    /// Intersects equality postings in id order, including nested AND and ID
    /// cursors. A shared ordered snapshot avoids holding index locks while
    /// testing the other postings (which may belong to the same index).
    fn indexed_id_page(
        &self,
        queries: &[Box<Filter>],
        candidates: Option<&FxHashSet<DocumentId>>,
        limit: usize,
        order: ScanOrder,
        stats: &mut QueryStats,
    ) -> Option<Result<Vec<DocumentId>, DBError>> {
        if candidates.is_some() {
            return None;
        }
        fn flatten<'a>(filter: &'a Filter, out: &mut Vec<&'a Filter>) {
            match filter {
                Filter::And(children) if !children.is_empty() => {
                    for child in children {
                        flatten(child, out);
                    }
                }
                _ => out.push(filter),
            }
        }
        let mut flat = Vec::new();
        for query in queries {
            flatten(query, &mut flat);
        }
        let mut equalities = Vec::new();
        let mut id_queries = Vec::new();
        for query in flat {
            if only_id_fields(query) {
                id_queries.push(Box::new(id_filter_range(query.clone())));
            } else if let Filter::Field((name, RangeQuery::Eq(value))) = query {
                equalities.push((name, value));
            } else {
                return None;
            }
        }
        if equalities.is_empty() {
            return None;
        }
        Some((|| {
            let mut indexes = Vec::new();
            for (name, value) in equalities {
                let index = self
                    .btree_indexes
                    .iter()
                    .find(|index| index.name() == name)
                    .ok_or_else(|| DBError::Index {
                        name: self.name.clone(),
                        source: format!("BTree index {name:?} not found").into(),
                    })?;
                let len = index.query_with(value, |ids| Some(ids.len())).unwrap_or(0);
                indexes.push((len, index, value));
            }
            indexes.sort_by_key(|(len, ..)| *len);
            let (driver_len, driver, value) = indexes[0];
            stats.index_keys += indexes.len();
            let has_id_filter = !id_queries.is_empty();
            let mut id_query = if id_queries.is_empty() {
                RangeQuery::Ge(0)
            } else {
                RangeQuery::<u64>::try_convert_from(if id_queries.len() == 1 {
                    *id_queries.pop().expect("one ID filter")
                } else {
                    RangeQuery::And(id_queries)
                })
                .map_err(|source| DBError::Generic {
                    name: self.name.clone(),
                    source,
                })?
            };
            normalize_id_query(&mut id_query);
            let Some((lo, hi)) = id_envelope(&id_query) else {
                return Ok(Vec::new());
            };
            let id_count = match &id_query {
                RangeQuery::Eq(_) => 1,
                RangeQuery::Include(ids) => ids.len(),
                _ => self.doc_ids.read().range_len(lo..=hi),
            };
            let cached = driver.cached_ordered_ids(value)?;
            if cached.is_none() && has_id_filter && id_count <= 4096 && id_count < driver_len {
                let ids = self.filter_by_id(id_query, None, 0, order);
                let mut selected = Vec::new();
                let walk: Box<dyn Iterator<Item = &u64>> = if order.is_descending() {
                    Box::new(ids.iter().rev())
                } else {
                    Box::new(ids.iter())
                };
                for &id in walk {
                    stats.bitmap_ids += 1;
                    let mut matched = true;
                    for (_, index, value) in &indexes {
                        stats.membership_probes += 1;
                        if !index.contains_id(value, id)? {
                            matched = false;
                            break;
                        }
                    }
                    if matched {
                        selected.push(id);
                    }
                    if limit > 0 && selected.len() == limit {
                        break;
                    }
                }
                if order.is_descending() {
                    selected.reverse();
                }
                return Ok(selected);
            }
            let ids = match cached {
                Some(ids) => ids,
                None => driver.ordered_ids(value)?.unwrap_or_default(),
            };
            stats.ordered_snapshot_ids += ids.len();
            let first = ids.partition_point(|id| *id < lo);
            let last = ids.partition_point(|id| *id <= hi);
            let live = self.doc_ids.read();
            let mut result = Vec::new();
            let walk: Box<dyn Iterator<Item = &u64>> = if order.is_descending() {
                Box::new(ids[first..last].iter().rev())
            } else {
                Box::new(ids[first..last].iter())
            };
            for &id in walk {
                stats.posting_ids += 1;
                if !live.contains(&id) || !matches_id_query(&id_query, id) {
                    continue;
                }
                let mut matches = true;
                for (_, index, value) in &indexes[1..] {
                    stats.membership_probes += 1;
                    if !index.contains_id(value, id)? {
                        matches = false;
                        break;
                    }
                }
                if matches {
                    result.push(id);
                }
                if limit > 0 && result.len() == limit {
                    break;
                }
            }
            if order.is_descending() {
                result.reverse();
            }
            Ok(result)
        })())
    }

    pub(super) fn filter_by_id(
        &self,
        mut query: RangeQuery<DocumentId>,
        candidates: Option<&FxHashSet<DocumentId>>,
        limit: usize,
        order: ScanOrder,
    ) -> Vec<DocumentId> {
        normalize_id_query(&mut query);
        let live = self.doc_ids.read();
        let composite = matches!(
            query,
            RangeQuery::And(_) | RangeQuery::Or(_) | RangeQuery::Not(_)
        );
        // Test each candidate when that is cheaper than walking the id set,
        // and always for composites, whose envelope may span every id.
        if let Some(candidates) = candidates
            && (composite || candidates.len() < live.len())
        {
            let mut result: Vec<_> = candidates
                .iter()
                .copied()
                .filter(|id| live.contains(id) && matches_id_query(&query, *id))
                .collect();
            result.sort_unstable();
            order.truncate(&mut result, limit);
            return result;
        }
        // Walks run in the caller's `order` and stop after `limit` hits;
        // the result is ascending either way.
        if let RangeQuery::Include(ids) = &query {
            // Sorted and deduplicated above, like the B-tree's Include: a
            // repeated id must not appear twice or fill the limit early.
            return Self::collect_ids(
                ids.iter().copied().filter(|id| live.contains(id)),
                candidates,
                None,
                limit,
                order,
            );
        }
        // An inverted `Between` has no envelope and matches nothing, as in
        // anda_db_btree, instead of panicking in `BTreeSet::range`.
        let Some((lo, hi)) = id_envelope(&query) else {
            return Vec::new();
        };
        Self::collect_ids(
            live.range(lo..=hi)
                .filter(|id| matches_id_query(&query, *id)),
            candidates,
            None,
            limit,
            order,
        )
    }

    /// Walks the whole id set from the end `order` asks for and returns the
    /// ids that are in `candidates` (when given) and not in `exclude`,
    /// stopping after `limit` hits (`0` = unbounded). Ascending on return.
    pub(super) fn walk_complement(
        &self,
        exclude: &FxHashSet<DocumentId>,
        candidates: Option<&FxHashSet<DocumentId>>,
        limit: usize,
        order: ScanOrder,
    ) -> Vec<DocumentId> {
        let doc_ids = self.doc_ids.read();
        if let Some(candidates) = candidates {
            let mut result: Vec<_> = candidates
                .iter()
                .copied()
                .filter(|id| doc_ids.contains(id) && !exclude.contains(id))
                .collect();
            result.sort_unstable();
            order.truncate(&mut result, limit);
            return result;
        }
        Self::collect_ids(
            doc_ids.range(0..=u64::MAX),
            candidates,
            Some(exclude),
            limit,
            order,
        )
    }

    /// Collects from `ids` — ascending, as the id set stores them — the ids
    /// that pass the `candidates` / `exclude` filters, walking from the end
    /// `order` asks for and stopping after `limit` hits (`0` = unbounded).
    /// The result is always ascending: the direction decides *which* ids a
    /// bounded walk keeps, never how they are ordered.
    pub(super) fn collect_ids(
        ids: impl DoubleEndedIterator<Item = DocumentId>,
        candidates: Option<&FxHashSet<DocumentId>>,
        exclude: Option<&FxHashSet<DocumentId>>,
        limit: usize,
        order: ScanOrder,
    ) -> Vec<DocumentId> {
        let keep = |id: &DocumentId| {
            candidates.is_none_or(|s| s.contains(id)) && exclude.is_none_or(|s| !s.contains(id))
        };
        let take = if limit > 0 { limit } else { usize::MAX };
        if order.is_descending() {
            let mut result: Vec<DocumentId> = ids.rev().filter(keep).take(take).collect();
            result.reverse();
            result
        } else {
            ids.filter(keep).take(take).collect()
        }
    }
}

fn normalize_id_query(query: &mut RangeQuery<u64>) {
    match query {
        RangeQuery::Include(ids) => {
            ids.sort_unstable();
            ids.dedup();
        }
        RangeQuery::And(queries) | RangeQuery::Or(queries) => {
            for q in queries {
                normalize_id_query(q);
            }
        }
        RangeQuery::Not(q) => normalize_id_query(q),
        _ => {}
    }
}

fn matches_id_query(query: &RangeQuery<u64>, id: u64) -> bool {
    match query {
        RangeQuery::Eq(key) => id == *key,
        RangeQuery::Gt(key) => id > *key,
        RangeQuery::Ge(key) => id >= *key,
        RangeQuery::Lt(key) => id < *key,
        RangeQuery::Le(key) => id <= *key,
        RangeQuery::Between(lo, hi) => *lo <= id && id <= *hi,
        RangeQuery::Include(ids) => ids.binary_search(&id).is_ok(),
        RangeQuery::And(queries) => {
            !queries.is_empty() && queries.iter().all(|q| matches_id_query(q, id))
        }
        RangeQuery::Or(queries) => queries.iter().any(|q| matches_id_query(q, id)),
        RangeQuery::Not(q) => !matches_id_query(q, id),
    }
}

fn only_id_fields(filter: &Filter) -> bool {
    match filter {
        Filter::Field((name, _)) => name == Schema::ID_KEY,
        Filter::And(filters) | Filter::Or(filters) => filters.iter().all(|f| only_id_fields(f)),
        Filter::Not(filter) => only_id_fields(filter),
    }
}

fn id_filter_range(filter: Filter) -> RangeQuery<Fv> {
    match filter {
        Filter::Field((_, query)) => query,
        Filter::And(filters) => RangeQuery::And(
            filters
                .into_iter()
                .map(|f| Box::new(id_filter_range(*f)))
                .collect(),
        ),
        Filter::Or(filters) => RangeQuery::Or(
            filters
                .into_iter()
                .map(|f| Box::new(id_filter_range(*f)))
                .collect(),
        ),
        Filter::Not(filter) => RangeQuery::Not(Box::new(id_filter_range(*filter))),
    }
}

/// A conservative interval containing every possible match. The exact
/// predicate is still tested, so disjoint unions and complements stay correct.
fn id_envelope(query: &RangeQuery<u64>) -> Option<(u64, u64)> {
    match query {
        RangeQuery::Eq(key) => Some((*key, *key)),
        RangeQuery::Gt(key) => key.checked_add(1).map(|lo| (lo, u64::MAX)),
        RangeQuery::Ge(key) => Some((*key, u64::MAX)),
        RangeQuery::Lt(key) => key.checked_sub(1).map(|hi| (0, hi)),
        RangeQuery::Le(key) => Some((0, *key)),
        RangeQuery::Between(lo, hi) => (lo <= hi).then_some((*lo, *hi)),
        RangeQuery::Include(ids) => Some((*ids.first()?, *ids.last()?)),
        RangeQuery::And(queries) => {
            if queries.is_empty() {
                return None;
            }
            queries.iter().try_fold((0, u64::MAX), |(lo, hi), query| {
                let (a, b) = id_envelope(query)?;
                (lo.max(a) <= hi.min(b)).then_some((lo.max(a), hi.min(b)))
            })
        }
        RangeQuery::Or(queries) => queries
            .iter()
            .filter_map(|q| id_envelope(q))
            .reduce(|(lo, hi), (a, b)| (lo.min(a), hi.max(b))),
        RangeQuery::Not(query) => match query.as_ref() {
            RangeQuery::Not(inner) => id_envelope(inner),
            _ => Some((0, u64::MAX)),
        },
    }
}
