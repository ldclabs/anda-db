use super::*;

impl<T: Tokenizer> BM25Index<T> {
    /// Scores only the supplied document ids, retaining global document
    /// frequencies and average length. Useful for selective external filters.
    pub fn try_search_in_ids(
        &self,
        query: &str,
        top_k: usize,
        params: Option<BM25Params>,
        ids: &[u64],
        logical: bool,
    ) -> Result<Vec<(u64, f32)>, BM25Error> {
        if top_k == 0 {
            return Ok(Vec::new());
        }
        let scope: Scores = ids
            .iter()
            .copied()
            .filter(|id| self.doc_tokens.contains_key(id))
            .map(|id| (id, 0.0))
            .collect();
        let params = params.as_ref().unwrap_or(&self.config.bm25);
        let mut context = QueryContext::new(self, params);
        let scores = if logical {
            let expr = QueryType::try_parse(query).map_err(|source| BM25Error::Generic {
                name: self.name.clone(),
                source: source.into(),
            })?;
            let plan = QueryPlan::prepare(&expr, &mut self.tokenizer.clone());
            self.execute_query(&plan, &mut context, Some(&scope))?
        } else {
            let tokens = query_tokens(&mut self.tokenizer.clone(), query.trim());
            self.score_tokens(&tokens, &mut context, Some(&scope))
        };
        self.search_count.fetch_add(1, Ordering::Relaxed);
        Ok(Self::top_k_results(scores, top_k))
    }

    /// Scores the supplied document ids as if they were the whole corpus.
    ///
    /// Unlike [`Self::try_search_in_ids`], the document count, the average
    /// document length and every document frequency are computed over `ids`
    /// alone, so a document outside the scope cannot move a score inside it.
    /// This is what a caller needs when the scope is an authorization
    /// boundary: the result equals searching a fresh index built from exactly
    /// those documents. Ids the index does not hold are ignored.
    pub fn search_scoped(
        &self,
        query: &str,
        top_k: usize,
        params: Option<BM25Params>,
        ids: &[u64],
    ) -> Vec<(u64, f32)> {
        if top_k == 0 {
            return Vec::new();
        }
        let mut scope = Scores::default();
        let mut total_tokens = 0usize;
        for id in ids {
            if let Some(length) = self.doc_tokens.get(id)
                && scope.insert(*id, 0.0).is_none()
            {
                total_tokens += *length;
            }
        }
        if scope.is_empty() {
            return Vec::new();
        }
        let params = params.as_ref().unwrap_or(&self.config.bm25);
        let mut context = QueryContext::new(self, params);
        context.doc_count = scope.len();
        context.avg_length = (total_tokens as f32 / scope.len() as f32).max(1.0);
        context.scoped = true;
        let tokens = query_tokens(&mut self.tokenizer.clone(), query.trim());
        let scores = self.score_tokens(&tokens, &mut context, Some(&scope));
        self.search_count.fetch_add(1, Ordering::Relaxed);
        Self::top_k_results(scores, top_k)
    }

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
    /// * a `NOT` needs the complement of the whole index (`NOT a`,
    ///   `a OR NOT b`, `NOT a AND NOT b`) and the index holds more than
    ///   10 000 documents. Inside an `AND` with a positive operand
    ///   (`a AND NOT b`, `a AND (b OR NOT c)`) the complement is taken
    ///   relative to the documents already matched and has no such limit.
    ///   `NOT NOT x` is evaluated as `x`.
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
        // The complement guard lives where the whole index is materialized
        // (`universe`); candidate-scoped complements need no full-index scan.
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
        // Parsing already succeeded; even selectivity estimates would take
        // unnecessary posting locks when the external filter matched nothing.
        if candidates.is_some_and(Scores::is_empty) {
            return Ok(Scores::default());
        }
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
            QueryPlan::Not(inner) => {
                let mut result = self.universe(candidates)?;
                for id in self.match_ids(inner, &result) {
                    result.remove(&id);
                }
                Ok(result)
            }
        }
    }

    /// Scores each posting entry in one pass. DF is the posting length, so it
    /// stays global when the scoring work is restricted to candidates.
    fn score_tokens(
        &self,
        tokens: &[String],
        context: &mut QueryContext,
        candidates: Option<&Scores>,
    ) -> Scores {
        if context.doc_count == 0 || tokens.is_empty() || candidates.is_some_and(Scores::is_empty) {
            return Scores::default();
        }
        let mut scores = Scores::default();
        // Factor the document-length normalization once. In particular, do
        // not divide every matching document's length by the same average.
        let norm_base = context.k1 * (1.0 - context.b);
        let norm_length = context.k1 * context.b / context.avg_length;
        let tf_gain = context.k1 + 1.0;
        for token in tokens {
            let Some(posting) = self.postings.get(token) else {
                continue;
            };
            // A scoped search counts a term's documents inside the scope only;
            // otherwise the frequency is the whole index's.
            let df = match candidates {
                Some(candidates) if context.scoped => posting
                    .1
                    .iter()
                    .filter(|(id, _)| candidates.contains_key(id))
                    .count(),
                _ => posting.1.len(),
            };
            if df == 0 {
                continue;
            }
            let weight = context.idf(token, df) * tf_gain;
            // Allocate from actual matches, not a fixed minimum: missing and
            // rare terms should not reserve room for a thousand documents.
            if scores.is_empty() {
                scores
                    .reserve(candidates.map_or(posting.1.len(), |c| c.len().min(posting.1.len())));
            }
            for &(id, tf) in &posting.1 {
                if candidates.is_some_and(|candidates| !candidates.contains_key(&id)) {
                    continue;
                }
                if let Some(length) = self.doc_tokens.get(&id) {
                    let tf = tf as f32;
                    *scores.entry(id).or_default() +=
                        tf * weight / (tf + norm_base + norm_length * *length as f32);
                }
            }
        }
        scores
    }

    /// Intersects the positive operands, cheapest first, then drops the
    /// matches of each NOT operand from the intersection.
    fn score_and(
        &self,
        queries: &[QueryPlan],
        context: &mut QueryContext,
        candidates: Option<&Scores>,
    ) -> Result<Scores, BM25Error> {
        if queries.is_empty() {
            return Ok(Scores::default());
        }
        let (negatives, mut positives): (Vec<_>, Vec<_>) = queries
            .iter()
            .partition(|query| matches!(query, QueryPlan::Not(_)));
        positives.sort_by_cached_key(|query| self.estimate_matches(query, context.doc_count));
        let mut result = match positives.first() {
            Some(first) => self.execute_query(first, context, candidates)?,
            None => self.universe(candidates)?,
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
        for query in negatives {
            if result.is_empty() {
                break;
            }
            if let QueryPlan::Not(inner) = query {
                for id in self.match_ids(inner, &result) {
                    result.remove(&id);
                }
            }
        }
        Ok(result)
    }

    /// Ids matching `query` inside `scope`, without TF, IDF or scores. A NOT
    /// takes its complement relative to `scope`, which is already in memory,
    /// so only [`Self::universe`] needs a budget.
    fn match_ids<C: CandidateLookup>(&self, query: &QueryPlan, scope: &C) -> FxHashSet<u64> {
        match query {
            QueryPlan::Terms(tokens) => {
                let mut result = FxHashSet::default();
                for token in tokens {
                    if let Some(posting) = self.postings.get(token) {
                        for (id, _) in &posting.1 {
                            if scope.contains_doc(id) && self.doc_tokens.contains_key(id) {
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
                    result.extend(self.match_ids(query, scope));
                }
                result
            }
            QueryPlan::And(queries) => {
                let mut ordered: Vec<_> = queries.iter().collect();
                ordered.sort_by_cached_key(|query| {
                    self.estimate_matches(query, scope.candidate_len())
                });
                let Some((first, rest)) = ordered.split_first() else {
                    return FxHashSet::default();
                };
                let mut result = self.match_ids(first, scope);
                for query in rest {
                    if result.is_empty() {
                        break;
                    }
                    result = self.match_ids(query, &result);
                }
                result
            }
            QueryPlan::Not(inner) => {
                let mut result = scope.ids();
                for id in self.match_ids(inner, &result) {
                    result.remove(&id);
                }
                result
            }
        }
    }

    /// The zero-scored documents a complement starts from: the candidates,
    /// or every document when there are none. Only the latter materializes
    /// something new, so only it is subject to the complement budget.
    fn universe(&self, candidates: Option<&Scores>) -> Result<Scores, BM25Error> {
        if let Some(candidates) = candidates {
            return Ok(candidates.keys().map(|id| (*id, 0.0)).collect());
        }
        let count = self.doc_tokens.len();
        if count > MAX_NOT_COMPLEMENT_DOCS {
            return Err(BM25Error::Generic {
                name: self.name.clone(),
                source: format!("logical NOT complement over {count} documents exceeds maximum {MAX_NOT_COMPLEMENT_DOCS}").into(),
            });
        }
        Ok(self
            .doc_tokens
            .iter()
            .map(|entry| (*entry.key(), 0.0))
            .collect())
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
    fn ids(&self) -> FxHashSet<u64>;
}

impl CandidateLookup for Scores {
    fn contains_doc(&self, id: &u64) -> bool {
        self.contains_key(id)
    }

    fn candidate_len(&self) -> usize {
        self.len()
    }

    fn ids(&self) -> FxHashSet<u64> {
        self.keys().copied().collect()
    }
}

impl CandidateLookup for FxHashSet<u64> {
    fn contains_doc(&self, id: &u64) -> bool {
        self.contains(id)
    }

    fn candidate_len(&self) -> usize {
        self.len()
    }

    fn ids(&self) -> FxHashSet<u64> {
        self.clone()
    }
}

struct QueryContext {
    doc_count: usize,
    avg_length: f32,
    k1: f32,
    b: f32,
    idfs: FxHashMap<String, f32>,
    /// Whether document frequencies are counted inside the candidate scope.
    scoped: bool,
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
            scoped: false,
        }
    }

    /// The first DF seen for a token fixes its IDF for the whole query, so
    /// every branch of one boolean query scores the token alike.
    fn idf(&mut self, token: &str, df: usize) -> f32 {
        if let Some(idf) = self.idfs.get(token) {
            return *idf;
        }
        // ln_1p preserves tiny IDFs for common terms in large corpora.
        // Concurrent inserts may increase DF beyond the query's count snapshot.
        let n = self.doc_count.max(df) as f64;
        let idf = ((n - df as f64 + 0.5) / (df as f64 + 0.5)).ln_1p() as f32;
        self.idfs.insert(token.to_string(), idf);
        idf
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
            // `NOT NOT x` is `x`: a plan's NOT always needs a complement.
            QueryType::Not(query) => match Self::prepare(query, tokenizer) {
                Self::Not(inner) => *inner,
                inner => Self::Not(Box::new(inner)),
            },
            QueryType::And(queries) => Self::And(
                queries
                    .iter()
                    .map(|q| Self::prepare(q, tokenizer))
                    .collect(),
            ),
            QueryType::Or(queries) => {
                let mut tokens = Vec::new();
                let mut branches = Vec::new();
                for query in queries {
                    Self::prepare(query, tokenizer).collect_or(&mut tokens, &mut branches);
                }
                // Normalize the entire associative OR, including when it has
                // AND/NOT branches. Parentheses must not change term weights.
                // Tokenize operands independently to preserve custom tokenizers.
                tokens.sort_unstable();
                tokens.dedup();
                if branches.is_empty() {
                    return Self::Terms(tokens);
                }
                if !tokens.is_empty() {
                    branches.insert(0, Self::Terms(tokens));
                }
                Self::Or(branches)
            }
        }
    }

    fn collect_or(self, tokens: &mut Vec<String>, branches: &mut Vec<Self>) {
        match self {
            Self::Terms(mut terms) => tokens.append(&mut terms),
            Self::Or(queries) => {
                for query in queries {
                    query.collect_or(tokens, branches);
                }
            }
            query => branches.push(query),
        }
    }
}
