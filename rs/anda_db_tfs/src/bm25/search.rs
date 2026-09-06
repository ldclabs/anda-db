use super::*;

impl<T: Tokenizer> BM25Index<T> {
    /// Searches the index and returns the highest-scoring documents.
    ///
    /// The query is tokenized with the index's tokenizer. Multiple tokens are
    /// treated as a disjunction (OR). Use [`search_advanced`](Self::search_advanced)
    /// for boolean expressions with `AND` / `OR` / `NOT` and parentheses.
    ///
    /// # Arguments
    ///
    /// * `query` — raw query text.
    /// * `top_k` — maximum number of results to return; `0` yields an empty vector.
    /// * `params` — override the default [`BM25Params`] for this call only.
    ///
    /// # Returns
    ///
    /// A vector of `(document_id, score)` pairs sorted by descending score.
    pub fn search(&self, query: &str, top_k: usize, params: Option<BM25Params>) -> Vec<(u64, f32)> {
        if top_k == 0 {
            return Vec::new();
        }

        let params = params.as_ref().unwrap_or(&self.config.bm25);
        let mut context = QueryContext::new(self, params);
        let tokens = query_tokens(&mut self.tokenizer.clone(), query.trim());
        let scored_docs = self.score_tokens(&tokens, &mut context, None);
        // Count only queries that actually reached scoring (`top_k == 0`
        // short-circuits above), matching the HNSW index's semantics.
        self.search_count.fetch_add(1, Ordering::Relaxed);

        Self::top_k_results(scored_docs, top_k)
    }

    /// Searches the index with a boolean query expression.
    ///
    /// Unlike [`search`](Self::search), the query string is first parsed by
    /// [`QueryType::parse`] and may contain `AND`, `OR`, `NOT` operators and
    /// parentheses. Operator precedence is `OR < AND < NOT`; multiple bare
    /// terms default to `OR`.
    ///
    /// Any error of [`try_search_advanced`](Self::try_search_advanced) — the
    /// parser's size budget or the `NOT` complement guard — yields an empty
    /// result here; use `try_search_advanced` to observe it.
    ///
    /// # Arguments
    ///
    /// * `query` — e.g. `"(hello AND world) OR (rust AND NOT java)"`.
    /// * `top_k` — maximum number of results to return.
    /// * `params` — optional BM25 parameters override.
    ///
    /// # Returns
    ///
    /// A vector of `(document_id, score)` pairs sorted by descending score.
    pub fn search_advanced(
        &self,
        query: &str,
        top_k: usize,
        params: Option<BM25Params>,
    ) -> Vec<(u64, f32)> {
        self.try_search_advanced(query, top_k, params)
            .unwrap_or_default()
    }

    /// Searches the index with a boolean query expression and resource guards.
    ///
    /// # Errors
    ///
    /// * the query exceeds the parser's size or complexity budget (see
    ///   [`QueryType::try_parse`]);
    /// * a `NOT` operand needs to materialize a complement over more than
    ///   10 000 documents, including a candidate-relative complement. Simple
    ///   negative postings and even NOT chains avoid that materialization and
    ///   remain accepted for larger candidate sets.
    pub fn try_search_advanced(
        &self,
        query: &str,
        top_k: usize,
        params: Option<BM25Params>,
    ) -> Result<Vec<(u64, f32)>, BM25Error> {
        if top_k == 0 {
            return Ok(Vec::new());
        }

        let query_expr = QueryType::try_parse(query).map_err(|source| BM25Error::Generic {
            name: self.name.clone(),
            source: source.into(),
        })?;

        let params = params.as_ref().unwrap_or(&self.config.bm25);
        // Complement guards live at the point where an unrestricted universe
        // is materialized; candidate-scoped filters need no full-index scan.
        let mut context = QueryContext::new(self, params);
        let plan = QueryPlan::prepare(&query_expr, &mut self.tokenizer.clone());
        let scored_docs = self.execute_query(&plan, &mut context, None)?;
        // Count only queries that actually reached scoring: `top_k == 0`,
        // parse failures and rejected NOT complements all return above,
        // matching the HNSW index's search_count semantics.
        self.search_count.fetch_add(1, Ordering::Relaxed);

        Ok(Self::top_k_results(scored_docs, top_k))
    }

    /// Extracts the top-k results from scored documents using partial sorting.
    /// Uses `select_nth_unstable_by` for O(n + k·log(k)) instead of O(n·log(n)).
    fn top_k_results(scored_docs: FxHashMap<u64, f32>, top_k: usize) -> Vec<(u64, f32)> {
        if top_k == 0 || scored_docs.is_empty() {
            return Vec::new();
        }

        let mut results: Vec<(u64, f32)> = scored_docs.into_iter().collect();
        if results.len() > top_k {
            results.select_nth_unstable_by(top_k - 1, Self::compare_scored_docs);
            results.truncate(top_k);
        }
        results.sort_unstable_by(Self::compare_scored_docs);
        results
    }

    /// Total order over scored documents: descending score, `NaN` last, ties
    /// broken by ascending document id.
    ///
    /// `partial_cmp(..).unwrap_or(Equal)` is **not** a total order once a
    /// single `NaN` is present (it degrades to id-order against the `NaN` while
    /// the other pairs stay score-ordered, which produces comparison cycles and
    /// can make `sort_unstable_by` / `select_nth_unstable_by` panic). Scoring
    /// sanitizes its parameters so a `NaN` should be impossible, but the sort
    /// must not depend on that: `total_cmp` orders every `f32` bit pattern, and
    /// the explicit `NaN` bucket keeps unscorable documents at the end.
    pub(super) fn compare_scored_docs(a: &(u64, f32), b: &(u64, f32)) -> std::cmp::Ordering {
        match (a.1.is_nan(), b.1.is_nan()) {
            (true, true) => a.0.cmp(&b.0),
            (true, false) => std::cmp::Ordering::Greater,
            (false, true) => std::cmp::Ordering::Less,
            (false, false) => b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)),
        }
    }

    fn execute_query(
        &self,
        query: &QueryPlan,
        context: &mut QueryContext,
        candidates: Option<&Scores>,
    ) -> Result<Scores, BM25Error> {
        match query {
            QueryPlan::Terms(tokens) => Ok(self.score_tokens(tokens, context, candidates)),
            QueryPlan::Or(queries) => {
                let mut result = Scores::default();
                for query in queries {
                    for (id, score) in self.execute_query(query, context, candidates)? {
                        *result.entry(id).or_default() += score;
                    }
                }
                Ok(result)
            }
            QueryPlan::And(queries) => self.score_and(queries, context, candidates),
            QueryPlan::Not(query) => self.score_boolean_not(query, candidates),
        }
    }

    /// DF remains global even when the scoring work is restricted to candidates.
    /// Last duplicate wins, preserving recovery behavior for historical stale entries.
    fn score_tokens(
        &self,
        tokens: &[String],
        context: &mut QueryContext,
        candidates: Option<&Scores>,
    ) -> Scores {
        if context.doc_count == 0 || tokens.is_empty() {
            return Scores::default();
        }
        let mut scores = Scores::with_capacity_and_hasher(
            candidates.map_or(1000, Scores::len).min(context.doc_count),
            FxBuildHasher,
        );
        let mut valid: FxHashMap<u64, (f32, f32)> = FxHashMap::default();
        let mut live_ids = FxHashSet::default();
        // Factor the document-length normalization once. In particular, do
        // not divide every matching document's length by the same average.
        let norm_base = context.k1 * (1.0 - context.b);
        let norm_length = context.k1 * context.b / context.avg_length;
        let tf_gain = context.k1 + 1.0;
        // Length caching pays for overlapping lists. Sparse/disjoint OR lists
        // otherwise add a second map with essentially no cache hits.
        let cache_lengths = candidates.is_none()
            && tokens.len() > 1
            && tokens.iter().fold(0usize, |count, token| {
                count.saturating_add(
                    self.postings
                        .get(token)
                        .map_or(0, |posting| posting.1.len()),
                )
            }) >= context.doc_count.saturating_mul(2);
        for token in tokens {
            let cached_idf = context.idfs.get(token).copied();
            let df;
            {
                let Some(posting) = self.postings.get(token) else {
                    continue;
                };
                valid.clear();
                if let Some(candidates) = candidates {
                    valid.reserve(candidates.len().min(posting.1.len()));
                    live_ids.clear();
                    if cached_idf.is_none() {
                        live_ids.reserve(posting.1.len());
                    }
                    for &(id, tf) in &posting.1 {
                        let selected = candidates.contains_key(&id);
                        if !selected && cached_idf.is_some() {
                            continue;
                        }
                        if let Some(length) = self.doc_tokens.get(&id) {
                            if cached_idf.is_none() {
                                live_ids.insert(id);
                            }
                            if selected {
                                valid.insert(id, (tf as f32, *length as f32));
                            }
                        }
                    }
                    df = live_ids.len();
                } else {
                    // Keep the common unfiltered path branch-free per posting,
                    // and allocate once instead of growing/rehashing the buffer.
                    valid.reserve(posting.1.len());
                    if cache_lengths {
                        if context.doc_lengths.is_empty() {
                            context.doc_lengths.reserve(posting.1.len());
                        }
                        for (id, tf) in &posting.1 {
                            let length = match context.doc_lengths.entry(*id) {
                                std::collections::hash_map::Entry::Occupied(entry) => {
                                    Some(*entry.get())
                                }
                                std::collections::hash_map::Entry::Vacant(entry) => {
                                    self.doc_tokens.get(id).map(|length| *entry.insert(*length))
                                }
                            };
                            if let Some(length) = length {
                                valid.insert(*id, (*tf as f32, length as f32));
                            }
                        }
                    } else {
                        for (id, tf) in &posting.1 {
                            if let Some(length) = self.doc_tokens.get(id) {
                                valid.insert(*id, (*tf as f32, *length as f32));
                            }
                        }
                    }
                    df = valid.len();
                }
            } // Release the posting shard before floating-point scoring.
            let idf = cached_idf.unwrap_or_else(|| {
                // ln_1p preserves tiny IDFs for common terms in large corpora.
                // Concurrent inserts may increase DF beyond the query's count snapshot.
                let n = context.doc_count.max(df) as f64;
                let idf = ((n - df as f64 + 0.5) / (df as f64 + 0.5)).ln_1p() as f32;
                context.idfs.insert(token.clone(), idf);
                idf
            });
            let weight = idf * tf_gain;
            for (id, (tf, length)) in valid.drain() {
                *scores.entry(id).or_default() +=
                    tf * weight / (tf + norm_base + norm_length * length);
            }
        }
        scores
    }

    fn score_and(
        &self,
        queries: &[QueryPlan],
        context: &mut QueryContext,
        candidates: Option<&Scores>,
    ) -> Result<Scores, BM25Error> {
        if queries.is_empty() {
            return Ok(Scores::default());
        }
        let mut positives: Vec<_> = queries
            .iter()
            .filter(|q| !matches!(q, QueryPlan::Not(_)))
            .collect();
        positives.sort_by_cached_key(|query| self.estimate_matches(query, context.doc_count));
        // An AND made only from syntactic NOT nodes may still contain a
        // logically positive even-NOT filter. Seed from such a posting-derived
        // set instead of materializing the whole document universe.
        let seed_filter = if positives.is_empty() {
            queries
                .iter()
                .find(|query| Self::matches_without_complement(query))
        } else {
            None
        };
        let mut result = if let Some(first) = positives.first() {
            self.execute_query(first, context, candidates)?
        } else if let Some(seed) = seed_filter {
            self.execute_query(seed, context, candidates)?
        } else {
            self.candidate_ids(candidates)?
                .into_iter()
                .map(|id| (id, 0.0))
                .collect()
        };
        for query in positives.into_iter().skip(1) {
            if result.is_empty() {
                return Ok(result);
            }
            let matched = self.execute_query(query, context, Some(&result))?;
            result.retain(|id, score| {
                if let Some(other) = matched.get(id) {
                    *score += other;
                    true
                } else {
                    false
                }
            });
        }
        for query in queries {
            if result.is_empty() {
                break;
            }
            if seed_filter.is_some_and(|seed| std::ptr::eq(query, seed)) {
                continue;
            }
            if let QueryPlan::Not(operand) = query {
                self.apply_boolean_filter(&mut result, operand, false)?;
            }
        }
        Ok(result)
    }

    /// Keeps or removes documents matching `query` without copying the entire
    /// candidate set for the common AND-NOT cases.
    ///
    /// Negation polarity is pushed through leading NOT nodes. Positive ANDs
    /// and negative ORs can then filter in place, while a negative term removes
    /// only ids present in its posting lists. The remaining mixed-polarity
    /// shapes require a materialized candidate universe and are guarded by the
    /// same complement budget as a top-level NOT.
    fn apply_boolean_filter(
        &self,
        result: &mut Scores,
        query: &QueryPlan,
        keep_matches: bool,
    ) -> Result<(), BM25Error> {
        if result.is_empty() {
            return Ok(());
        }

        match query {
            QueryPlan::Not(inner) => {
                return self.apply_boolean_filter(result, inner, !keep_matches);
            }
            QueryPlan::And(queries) if keep_matches => {
                for query in queries {
                    self.apply_boolean_filter(result, query, true)?;
                    if result.is_empty() {
                        break;
                    }
                }
                return Ok(());
            }
            QueryPlan::Or(queries) if !keep_matches => {
                for query in queries {
                    self.apply_boolean_filter(result, query, false)?;
                    if result.is_empty() {
                        break;
                    }
                }
                return Ok(());
            }
            QueryPlan::Terms(tokens) if !keep_matches => {
                for token in tokens {
                    if let Some(posting) = self.postings.get(token) {
                        for (id, _) in &posting.1 {
                            result.remove(id);
                        }
                    }
                }
                return Ok(());
            }
            _ => {}
        }

        // An expression without NOT can be matched from its posting lists;
        // this avoids materializing the complement even for `NOT (a AND b)`.
        if let Some(matched) = self.match_positive_query(query, result) {
            result.retain(|id, _| matched.contains(id) == keep_matches);
            return Ok(());
        }

        self.ensure_complement_budget(result.len())?;
        let scope: FxHashSet<u64> = result.keys().copied().collect();
        let matched = self.match_query(query, &scope);
        result.retain(|id, _| matched.contains(id) == keep_matches);
        Ok(())
    }

    /// Returns matches for a query containing no NOT nodes. `None` signals
    /// that complement semantics are required and the caller must enforce the
    /// complement budget before constructing a candidate universe.
    fn match_positive_query<C: CandidateLookup>(
        &self,
        query: &QueryPlan,
        candidates: &C,
    ) -> Option<FxHashSet<u64>> {
        match query {
            QueryPlan::Terms(tokens) => {
                let mut result = FxHashSet::default();
                for token in tokens {
                    if let Some(posting) = self.postings.get(token) {
                        for (id, _) in &posting.1 {
                            if candidates.contains_doc(id) && self.doc_tokens.contains_key(id) {
                                result.insert(*id);
                            }
                        }
                    }
                }
                Some(result)
            }
            QueryPlan::Or(queries) => {
                let mut result = FxHashSet::default();
                for query in queries {
                    result.extend(self.match_positive_query(query, candidates)?);
                }
                Some(result)
            }
            QueryPlan::And(queries) => {
                if queries.is_empty() {
                    return Some(FxHashSet::default());
                }
                let mut ordered: Vec<_> = queries.iter().collect();
                ordered.sort_by_cached_key(|query| {
                    self.estimate_matches(query, candidates.candidate_len())
                });
                let mut result = self.match_positive_query(ordered[0], candidates)?;
                for query in ordered.into_iter().skip(1) {
                    if result.is_empty() {
                        break;
                    }
                    result = self.match_positive_query(query, &result)?;
                }
                Some(result)
            }
            // Eliminate double negation without materializing a universe. Four,
            // six, ... leading NOTs collapse through the same recursive case;
            // an odd number still signals that complement semantics are needed.
            QueryPlan::Not(inner) => match inner.as_ref() {
                QueryPlan::Not(inner) => self.match_positive_query(inner, candidates),
                _ => None,
            },
        }
    }

    fn matches_without_complement(query: &QueryPlan) -> bool {
        match query {
            QueryPlan::Terms(_) => true,
            QueryPlan::And(queries) | QueryPlan::Or(queries) => {
                queries.iter().all(Self::matches_without_complement)
            }
            QueryPlan::Not(inner) => match inner.as_ref() {
                QueryPlan::Not(inner) => Self::matches_without_complement(inner),
                _ => false,
            },
        }
    }

    /// Executes a NOT node with zero scores. Leading NOT pairs are cancelled
    /// before deciding whether a candidate universe is needed; an even chain
    /// over a positive expression can be answered directly from postings.
    fn score_boolean_not(
        &self,
        query: &QueryPlan,
        candidates: Option<&Scores>,
    ) -> Result<Scores, BM25Error> {
        let mut base = query;
        let mut negated = true; // The outer QueryPlan::Not matched by the caller.
        while let QueryPlan::Not(inner) = base {
            negated = !negated;
            base = inner;
        }

        if !negated {
            let direct = match candidates {
                Some(candidates) => self.match_positive_query(base, candidates),
                None => self.match_positive_query(base, &self.doc_tokens),
            };
            if let Some(matched) = direct {
                return Ok(matched.into_iter().map(|id| (id, 0.0)).collect());
            }
        }

        let scope = self.candidate_ids(candidates)?;
        let matched = self.match_query(base, &scope);
        let ids: FxHashSet<u64> = if negated {
            scope.difference(&matched).copied().collect()
        } else {
            matched
        };
        Ok(ids.into_iter().map(|id| (id, 0.0)).collect())
    }

    /// Boolean filters never need TF, IDF or a score map. This general set
    /// evaluator is called only after its candidate scope passed the complement
    /// budget; common AND-NOT forms use `apply_boolean_filter` instead.
    fn match_query(&self, query: &QueryPlan, scope: &FxHashSet<u64>) -> FxHashSet<u64> {
        if scope.is_empty() {
            return FxHashSet::default();
        }
        match query {
            QueryPlan::Terms(tokens) => {
                let mut result = FxHashSet::default();
                for token in tokens {
                    if let Some(posting) = self.postings.get(token) {
                        for (id, _) in &posting.1 {
                            if scope.contains(id) && self.doc_tokens.contains_key(id) {
                                result.insert(*id);
                            }
                        }
                    }
                }
                result
            }
            QueryPlan::Or(queries) => {
                let mut result = FxHashSet::default();
                for query in queries {
                    result.extend(self.match_query(query, scope));
                }
                result
            }
            QueryPlan::And(queries) => {
                if queries.is_empty() {
                    return FxHashSet::default();
                }
                let mut ordered: Vec<_> = queries.iter().collect();
                ordered.sort_by_cached_key(|query| self.estimate_matches(query, scope.len()));
                let mut result = scope.clone();
                for query in ordered {
                    result = self.match_query(query, &result);
                    if result.is_empty() {
                        break;
                    }
                }
                result
            }
            QueryPlan::Not(query) => {
                let excluded = self.match_query(query, scope);
                scope.difference(&excluded).copied().collect()
            }
        }
    }

    fn candidate_ids(&self, candidates: Option<&Scores>) -> Result<FxHashSet<u64>, BM25Error> {
        let count = candidates.map_or_else(|| self.doc_tokens.len(), Scores::len);
        self.ensure_complement_budget(count)?;

        Ok(match candidates {
            Some(candidates) => candidates.keys().copied().collect(),
            None => self.doc_tokens.iter().map(|entry| *entry.key()).collect(),
        })
    }

    fn ensure_complement_budget(&self, count: usize) -> Result<(), BM25Error> {
        if count > MAX_NOT_COMPLEMENT_DOCS {
            return Err(BM25Error::Generic {
                name: self.name.clone(),
                source: format!("logical NOT complement over {count} documents exceeds maximum {MAX_NOT_COMPLEMENT_DOCS}").into(),
            });
        }
        Ok(())
    }

    fn estimate_matches(&self, query: &QueryPlan, ceiling: usize) -> usize {
        match query {
            QueryPlan::Terms(tokens) => tokens.iter().fold(0usize, |n, token| {
                n.saturating_add(
                    self.postings
                        .get(token)
                        .map_or(0, |posting| posting.1.len()),
                )
                .min(ceiling)
            }),
            QueryPlan::And(queries) => queries
                .iter()
                .map(|query| self.estimate_matches(query, ceiling))
                .min()
                .unwrap_or(0),
            QueryPlan::Or(queries) => queries.iter().fold(0usize, |n, query| {
                n.saturating_add(self.estimate_matches(query, ceiling))
                    .min(ceiling)
            }),
            QueryPlan::Not(_) => ceiling,
        }
    }
}

type Scores = FxHashMap<u64, f32>;

trait CandidateLookup {
    fn contains_doc(&self, id: &u64) -> bool;
    fn candidate_len(&self) -> usize;
}

impl CandidateLookup for Scores {
    fn contains_doc(&self, id: &u64) -> bool {
        self.contains_key(id)
    }

    fn candidate_len(&self) -> usize {
        self.len()
    }
}

impl CandidateLookup for FxHashSet<u64> {
    fn contains_doc(&self, id: &u64) -> bool {
        self.contains(id)
    }

    fn candidate_len(&self) -> usize {
        self.len()
    }
}

impl CandidateLookup for DashMap<u64, usize> {
    fn contains_doc(&self, id: &u64) -> bool {
        self.contains_key(id)
    }

    fn candidate_len(&self) -> usize {
        self.len()
    }
}

struct QueryContext {
    doc_count: usize,
    avg_length: f32,
    k1: f32,
    b: f32,
    idfs: FxHashMap<String, f32>,
    /// Reuse first-observed lengths across multi-token scoring; searches are
    /// best-effort concurrent views, not transactional snapshots.
    doc_lengths: FxHashMap<u64, usize>,
}

impl QueryContext {
    fn new<T: Tokenizer>(index: &BM25Index<T>, params: &BM25Params) -> Self {
        let doc_count = index.doc_tokens.len();
        let avg_length = if doc_count == 0 {
            1.0
        } else {
            (index.total_tokens.load(Ordering::Relaxed) as f32 / doc_count as f32).max(1.0)
        };
        let (k1, b) = params.sanitized();
        Self {
            doc_count,
            avg_length,
            k1,
            b,
            idfs: FxHashMap::default(),
            doc_lengths: FxHashMap::default(),
        }
    }
}

enum QueryPlan {
    Terms(Vec<String>),
    Or(Vec<QueryPlan>),
    And(Vec<QueryPlan>),
    Not(Box<QueryPlan>),
}

fn query_tokens<T: Tokenizer>(tokenizer: &mut T, text: &str) -> Vec<String> {
    let mut tokens: Vec<_> = collect_tokens(tokenizer, text, None).into_keys().collect();
    tokens.sort_unstable();
    tokens
}

impl QueryPlan {
    fn prepare<T: Tokenizer>(query: &QueryType, tokenizer: &mut T) -> Self {
        match query {
            QueryType::Term(text) => Self::Terms(query_tokens(tokenizer, text)),
            QueryType::Not(query) => Self::Not(Box::new(Self::prepare(query, tokenizer))),
            QueryType::And(queries) => Self::And(
                queries
                    .iter()
                    .map(|q| Self::prepare(q, tokenizer))
                    .collect(),
            ),
            QueryType::Or(queries) => {
                let queries: Vec<_> = queries
                    .iter()
                    .map(|q| Self::prepare(q, tokenizer))
                    .collect();
                if queries.iter().all(|q| matches!(q, Self::Terms(_))) {
                    // Merge token sets, never the raw operand text: tokenizers
                    // may be case-sensitive or depend on whitespace/context.
                    let mut tokens = Vec::new();
                    for query in queries {
                        if let Self::Terms(mut terms) = query {
                            tokens.append(&mut terms);
                        }
                    }
                    tokens.sort_unstable();
                    tokens.dedup();
                    Self::Terms(tokens)
                } else {
                    Self::Or(queries)
                }
            }
        }
    }
}
