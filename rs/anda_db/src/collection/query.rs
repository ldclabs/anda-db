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
            // `Not(Include([]))` excludes nothing, i.e. it walks every key.
            // The scan holds the index read lock while `f` runs, so the
            // matching keys are only collected here and removed afterwards.
            let stale: Vec<(Fv, Vec<DocumentId>)> = index.range_query_with(
                RangeQuery::Not(Box::new(RangeQuery::Include(Vec::new()))),
                |key, ids| {
                    let hits: Vec<DocumentId> = ids
                        .iter()
                        .copied()
                        .filter(|id| dead_ids.contains(id))
                        .collect();
                    if hits.is_empty() {
                        (true, Vec::new())
                    } else {
                        (true, vec![(key, hits)])
                    }
                },
            );

            for (key, ids) in stale {
                for id in ids {
                    index.remove(id, &key, now_ms);
                }
            }
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
            && params.text.is_none()
        {
            return Err(DBError::Index {
                name: self.name.clone(),
                source: format!(
                    "no HNSW index matches the query vector dimension {}",
                    vector.len()
                )
                .into(),
            });
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
        loop {
            let mut rankings = Vec::new();
            if let Some(text) = &params.text {
                for index in &self.bm25_indexes {
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

        let mut rt = self.filter_by_field(filter, &[], limit, order)?;
        order.truncate(&mut rt, limit);
        Ok(rt)
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
        let rt = self.filter_by_field(filter, &[], 0, ScanOrder::Ascending)?;
        Ok(rt)
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
        if candidates.is_empty() {
            let mut result = self.filter_by_field_with(filter, None, limit, order)?;
            result.sort_unstable();
            order.truncate(&mut result, limit);
            Ok(result)
        } else {
            let cand_set: FxHashSet<DocumentId> = candidates.iter().copied().collect();
            let matched: FxHashSet<DocumentId> = self
                .filter_by_field_with(filter, Some(&cand_set), 0, order)?
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
    pub(super) fn filter_by_field_with(
        &self,
        filter: Filter,
        candidates: Option<&FxHashSet<DocumentId>>,
        limit: usize,
        order: ScanOrder,
    ) -> Result<Vec<DocumentId>, DBError> {
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
                    // The whole match set must be *visited*: the scan walks
                    // the *key* space, so stopping it after `limit` ids would
                    // keep the ids under the smallest (or largest) keys
                    // rather than the smallest or largest ids that
                    // `query_ids` / `query_last_ids` promise — and a posting
                    // list is in insertion order, so not even a single key
                    // can be trimmed early.
                    //
                    // It must not be *materialized*, though: `query_ids` is
                    // reachable over HTTP and clamps its result precisely so
                    // one request cannot allocate a `u64` per matching
                    // document. The requested end is therefore kept in a
                    // bounded set — memory stays O(limit) however many
                    // documents match, and the set also de-duplicates the ids
                    // a non-unique index (array field, or plain duplicates)
                    // maps under several keys, so `search` never returns the
                    // same document twice. `limit == 0` (composite operands,
                    // `query_all_ids`) still collects everything.
                    let mut rt: BTreeSet<DocumentId> = BTreeSet::new();
                    index.try_range_query_ids(filter, false, |ids| {
                        for id in ids
                            .iter()
                            .filter(|id| candidates.is_none_or(|s| s.contains(id)))
                        {
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
                    for id in self.filter_by_field_with(*query, candidates, limit, order)? {
                        order.retain_id(&mut result, id, limit);
                    }
                }
                Ok(result.into_iter().collect())
            }
            Filter::And(mut queries) => {
                if limit > 0
                    && let Some(result) = self.indexed_id_page(&queries, candidates, limit, order)
                {
                    return result;
                }
                queries.sort_by_cached_key(|filter| self.filter_cardinality_hint(filter));
                let mut iter = queries.into_iter();
                let Some(query) = iter.next() else {
                    return Ok(Vec::new());
                };
                let mut rt: FxHashSet<DocumentId> = self
                    .filter_by_field_with(*query, candidates, 0, order)?
                    .into_iter()
                    .collect();

                for query in iter {
                    rt = self
                        .filter_by_field_with(*query, Some(&rt), 0, order)?
                        .into_iter()
                        .collect();
                    if rt.is_empty() {
                        return Ok(Vec::new());
                    }
                }

                // 每个操作数都以 limit = 0 求值，得到的是完整交集，
                // 由调用方按 `order` 截断长度
                let mut result: Vec<_> = rt.into_iter().collect();
                result.sort_unstable();
                order.truncate(&mut result, limit);
                Ok(result)
            }
            Filter::Not(query) => {
                let exclude: FxHashSet<u64> = self
                    .filter_by_field_with(*query, candidates, 0, order)?
                    .into_iter()
                    .collect();
                Ok(self.walk_complement(&exclude, candidates, limit, order))
            }
        }
    }

    /// Common owner/status equality plus id pagination. A posting is not in
    /// id order after deletes, so visit it once and retain only the first/last
    /// page. Other AND shapes continue through the general intersection path.
    fn indexed_id_page(
        &self,
        queries: &[Box<Filter>],
        candidates: Option<&FxHashSet<DocumentId>>,
        limit: usize,
        order: ScanOrder,
    ) -> Option<Result<Vec<DocumentId>, DBError>> {
        let mut indexed = None;
        let mut id_queries = Vec::new();
        for query in queries {
            if only_id_fields(query) {
                id_queries.push(Box::new(id_filter_range((**query).clone())));
            } else if let Filter::Field((name, RangeQuery::Eq(value))) = query.as_ref()
                && indexed.is_none()
            {
                indexed = Some((name, value));
            } else {
                return None;
            }
        }
        let (name, value) = indexed?;
        if id_queries.is_empty() {
            return None;
        }
        Some((|| {
            let mut id_query = RangeQuery::<u64>::try_convert_from(RangeQuery::And(id_queries))
                .map_err(|source| DBError::Generic {
                    name: self.name.clone(),
                    source,
                })?;
            normalize_id_query(&mut id_query);
            let index = self
                .btree_indexes
                .iter()
                .find(|i| i.name() == name)
                .ok_or_else(|| DBError::Index {
                    name: self.name.clone(),
                    source: format!("BTree index {name:?} not found").into(),
                })?;
            let live = self.doc_ids.read();
            let mut result = BTreeSet::new();
            index.try_range_query_ids(RangeQuery::Eq(value.clone()), false, |ids| {
                for &id in ids {
                    if candidates.is_none_or(|c| c.contains(&id))
                        && live.contains(&id)
                        && matches_id_query(&id_query, id)
                    {
                        order.retain_id(&mut result, id, limit);
                    }
                }
                true
            })?;
            Ok(result.into_iter().collect())
        })())
    }

    pub(super) fn filter_by_id(
        &self,
        mut query: RangeQuery<DocumentId>,
        candidates: Option<&FxHashSet<DocumentId>>,
        limit: usize,
        order: ScanOrder,
    ) -> Vec<DocumentId> {
        if matches!(
            query,
            RangeQuery::And(_) | RangeQuery::Or(_) | RangeQuery::Not(_)
        ) {
            normalize_id_query(&mut query);
            let live = self.doc_ids.read();
            if let Some(candidates) = candidates {
                let mut result: Vec<_> = candidates
                    .iter()
                    .copied()
                    .filter(|id| live.contains(id) && matches_id_query(&query, *id))
                    .collect();
                result.sort_unstable();
                order.truncate(&mut result, limit);
                return result;
            }
            let Some((lo, hi)) = id_envelope(&query) else {
                return Vec::new();
            };
            return Self::collect_ids(
                live.range(lo..=hi)
                    .copied()
                    .filter(|id| matches_id_query(&query, *id)),
                None,
                None,
                limit,
                order,
            );
        }
        // 遍历方向由调用方的 `order` 决定，两端都能提前终止；
        // 结果始终按 id 升序返回。
        if let Some(candidates) = candidates
            && candidates.len() < self.doc_ids.read().len()
            && !matches!(
                query,
                RangeQuery::And(_) | RangeQuery::Or(_) | RangeQuery::Not(_)
            )
        {
            let live = self.doc_ids.read();
            let mut result: Vec<_> = candidates
                .iter()
                .copied()
                .filter(|id| {
                    live.contains(id)
                        && match &query {
                            RangeQuery::Eq(key) => id == key,
                            RangeQuery::Gt(key) => id > key,
                            RangeQuery::Ge(key) => id >= key,
                            RangeQuery::Lt(key) => id < key,
                            RangeQuery::Le(key) => id <= key,
                            RangeQuery::Between(lo, hi) => lo <= id && id <= hi,
                            RangeQuery::Include(ids) => ids.contains(id),
                            _ => unreachable!(),
                        }
                })
                .collect();
            result.sort_unstable();
            order.truncate(&mut result, limit);
            return result;
        }
        match query {
            RangeQuery::Eq(id) => {
                if self.doc_ids.read().contains(&id) && candidates.is_none_or(|s| s.contains(&id)) {
                    vec![id]
                } else {
                    Vec::new()
                }
            }
            RangeQuery::Gt(start_key) => {
                let doc_ids = self.doc_ids.read();
                let range = doc_ids.range((
                    std::ops::Bound::Excluded(start_key),
                    std::ops::Bound::Unbounded,
                ));
                Self::collect_ids(range.copied(), candidates, None, limit, order)
            }
            RangeQuery::Ge(start_key) => {
                let doc_ids = self.doc_ids.read();
                Self::collect_ids(
                    doc_ids.range(start_key..).copied(),
                    candidates,
                    None,
                    limit,
                    order,
                )
            }
            RangeQuery::Lt(end_key) => {
                let doc_ids = self.doc_ids.read();
                Self::collect_ids(
                    doc_ids.range(..end_key).copied(),
                    candidates,
                    None,
                    limit,
                    order,
                )
            }
            RangeQuery::Le(end_key) => {
                let doc_ids = self.doc_ids.read();
                Self::collect_ids(
                    doc_ids.range(..=end_key).copied(),
                    candidates,
                    None,
                    limit,
                    order,
                )
            }
            RangeQuery::Between(start_key, end_key) => {
                if start_key > end_key {
                    // 与 anda_db_btree 的语义一致：区间反转匹配空集，
                    // 而不是让 BTreeSet::range 直接 panic
                    return Vec::new();
                }

                let doc_ids = self.doc_ids.read();
                Self::collect_ids(
                    doc_ids.range(start_key..=end_key).copied(),
                    candidates,
                    None,
                    limit,
                    order,
                )
            }
            RangeQuery::Include(mut ids) => {
                // 与 anda_db_btree 的 Include 一致：重复的 key 只产出一次
                // （那边用 BTreeSet 去重）。否则调用方传入的重复 id 会让同一个
                // 文档重复出现，并且提前占满 limit。
                ids.sort_unstable();
                ids.dedup();
                let doc_ids = self.doc_ids.read();
                Self::collect_ids(
                    ids.into_iter().filter(|id| doc_ids.contains(id)),
                    candidates,
                    None,
                    limit,
                    order,
                )
            }
            RangeQuery::And(_) | RangeQuery::Or(_) | RangeQuery::Not(_) => {
                unreachable!("composite id queries are evaluated above")
            }
        }
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
            doc_ids.iter().copied(),
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
