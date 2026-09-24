use super::*;
use crate::distance::PreparedQuery;
use std::cell::RefCell;

/// Per-query search breadth. Values must be in 1..=MAX_EF_SEARCH.
#[derive(Debug, Clone, Copy, Default)]
pub struct SearchOptions {
    pub ef_search: Option<usize>,
}

/// Reusable search allocations, exclusively borrowed by one query at a time.
/// The default APIs use one workspace per thread, with a reentrant fallback.
#[derive(Default)]
pub struct SearchWorkspace {
    visited: FxHashSet<u64>,
    candidates: BinaryHeap<(Reverse<OrderedFloat<f32>>, u64)>,
    results: BinaryHeap<(OrderedFloat<f32>, u64)>,
}

thread_local! {
    static WORKSPACE: RefCell<SearchWorkspace> = RefCell::new(SearchWorkspace::default());
}

fn with_workspace<R>(f: impl FnOnce(&mut SearchWorkspace) -> R) -> R {
    WORKSPACE.with(|cell| {
        if let Ok(mut workspace) = cell.try_borrow_mut() {
            let result = f(&mut workspace);
            workspace.trim();
            result
        } else {
            f(&mut SearchWorkspace::default())
        }
    })
}

impl SearchWorkspace {
    pub(super) fn reset(&mut self) {
        self.visited.clear();
        self.candidates.clear();
        self.results.clear();
    }
    pub(super) fn trim(&mut self) {
        // A single unusually expensive traversal must not permanently retain
        // an unbounded allocation on every worker thread.
        if self.visited.capacity() > 131_072 || self.candidates.capacity() > 131_072 {
            *self = Self::default();
        }
    }
}

impl HnswIndex {
    /// Finds at most top_k neighbors, sorted by ascending distance.
    /// top_k=0 is always a no-op; top_k>4096 is an error, not silent truncation.
    /// Search overlaps with mutations and retries missing entry points up to
    /// SEARCH_MAX_ATTEMPTS times; it is not a transactional graph snapshot.
    pub fn search(&self, query: &[bf16], top_k: usize) -> Result<Vec<(u64, f32)>, HnswError> {
        if top_k == 0 {
            return Ok(Vec::new());
        }
        let ef = self.validate_query(
            query.len(),
            query.iter().all(|v| v.is_finite()),
            top_k,
            SearchOptions::default(),
        )?;
        let query: Vec<f32> = query.iter().map(|v| v.to_f32()).collect();
        with_workspace(|workspace| self.search_validated(&query, top_k, ef, workspace))
    }

    /// Searches without quantizing the f32 query. See Self::search for limits.
    pub fn search_f32(&self, query: &[f32], top_k: usize) -> Result<Vec<(u64, f32)>, HnswError> {
        self.search_f32_with_options(query, top_k, SearchOptions::default())
    }

    /// Overrides the search breadth without changing persisted configuration.
    pub fn search_f32_with_options(
        &self,
        query: &[f32],
        top_k: usize,
        options: SearchOptions,
    ) -> Result<Vec<(u64, f32)>, HnswError> {
        with_workspace(|workspace| self.search_f32_with_workspace(query, top_k, options, workspace))
    }

    /// Searches using caller-owned scratch allocations.
    pub fn search_f32_with_workspace(
        &self,
        query: &[f32],
        top_k: usize,
        options: SearchOptions,
        workspace: &mut SearchWorkspace,
    ) -> Result<Vec<(u64, f32)>, HnswError> {
        if top_k == 0 {
            return Ok(Vec::new());
        }
        let ef = self.validate_query(
            query.len(),
            query.iter().all(|v| v.is_finite()),
            top_k,
            options,
        )?;
        self.search_validated(query, top_k, ef, workspace)
    }

    /// Exactly scores a caller-bounded candidate set, such as ids selected by
    /// a metadata prefilter, without walking the graph. Missing ids are
    /// skipped. Returns at most top_k hits sorted by distance, then id, using
    /// the same distances as graph search. Not counted in `search_count`.
    pub fn search_f32_in_ids(
        &self,
        query: &[f32],
        top_k: usize,
        ids: &[u64],
    ) -> Result<Vec<(u64, f32)>, HnswError> {
        if top_k == 0 {
            return Ok(Vec::new());
        }
        self.validate_query(
            query.len(),
            query.iter().all(|v| v.is_finite()),
            top_k,
            SearchOptions::default(),
        )?;
        let query = PreparedQuery::new(self.config.distance_metric, query);
        let nodes = self.nodes.pin();
        let mut results = Vec::with_capacity(ids.len());
        for &id in ids {
            if let Some(node) = nodes.get(&id) {
                results.push((id, self.search_distance(&query, node)?));
            }
        }
        results.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        results.truncate(top_k);
        Ok(results)
    }

    fn search_validated(
        &self,
        query: &[f32],
        top_k: usize,
        ef: usize,
        workspace: &mut SearchWorkspace,
    ) -> Result<Vec<(u64, f32)>, HnswError> {
        let query = PreparedQuery::new(self.config.distance_metric, query);
        for attempt in 0..Self::SEARCH_MAX_ATTEMPTS {
            if self.is_empty() {
                return Ok(Vec::new());
            }
            match self.search_attempt(&query, top_k, ef, workspace) {
                Ok(results) => {
                    self.search_count.fetch_add(1, Ordering::Relaxed);
                    return Ok(results);
                }
                Err(HnswError::NotFound { .. }) if attempt + 1 < Self::SEARCH_MAX_ATTEMPTS => {}
                Err(err) => return Err(err),
            }
        }
        unreachable!("the final attempt always returns")
    }

    fn validate_query(
        &self,
        dimension: usize,
        finite: bool,
        top_k: usize,
        options: SearchOptions,
    ) -> Result<usize, HnswError> {
        if dimension != self.dimension() {
            return Err(HnswError::DimensionMismatch {
                name: self.name.clone(),
                expected: self.dimension(),
                got: dimension,
            });
        }
        let ef = options.ef_search.unwrap_or(self.config.ef_search);
        if !finite
            || top_k > HnswConfig::MAX_EF_SEARCH
            || !(1..=HnswConfig::MAX_EF_SEARCH).contains(&ef)
        {
            return Err(HnswError::Generic { name: self.name.clone(),
                source: "Query values must be finite; top_k and ef_search must not exceed 4096, and ef_search must be positive".into() });
        }
        Ok(ef.max(top_k))
    }

    fn search_attempt(
        &self,
        query: &PreparedQuery<'_>,
        top_k: usize,
        ef: usize,
        workspace: &mut SearchWorkspace,
    ) -> Result<Vec<(u64, f32)>, HnswError> {
        let (mut id, level) = *self.entry_point.read();
        for layer in (1..=level).rev() {
            id = self.greedy_search(query, id, layer)?.0;
        }
        let mut results = Vec::new();
        self.search_layer_into(query, id, 0, ef, workspace, &mut results)?;
        results.truncate(top_k);
        Ok(results)
    }

    /// Allocation-free ef=1 descent. No visited set or heap is needed because
    /// each move strictly decreases the distance.
    pub(super) fn greedy_search(
        &self,
        query: &PreparedQuery<'_>,
        entry: u64,
        layer: u8,
    ) -> Result<Neighbor, HnswError> {
        let nodes = self.nodes.pin();
        let node = nodes.get(&entry).ok_or_else(|| self.missing_node(entry))?;
        let mut best = (entry, self.search_distance(query, node)?);
        loop {
            let previous = best.0;
            let Some(node) = nodes.get(&previous) else {
                return Err(self.missing_node(previous));
            };
            if let Some(neighbors) = node.neighbors.get(layer as usize) {
                for &(id, _) in neighbors {
                    if let Some(node) = nodes.get(&id)
                        && node.layer >= layer
                    {
                        let distance = self.search_distance(query, node)?;
                        if distance < best.1 {
                            best = (id, distance);
                        }
                    }
                }
            }
            if best.0 == previous {
                return Ok(best);
            }
        }
    }

    /// Beam search within one layer; output is sorted by ascending distance.
    /// The per-layer visited set already computes each distance at most once.
    pub(super) fn search_layer_into(
        &self,
        query: &PreparedQuery<'_>,
        entry: u64,
        layer: u8,
        ef: usize,
        workspace: &mut SearchWorkspace,
        output: &mut Vec<Neighbor>,
    ) -> Result<(), HnswError> {
        output.clear();
        if ef <= 1 {
            output.push(self.greedy_search(query, entry, layer)?);
            return Ok(());
        }
        workspace.reset();
        let nodes = self.nodes.pin();
        let node = nodes.get(&entry).ok_or_else(|| self.missing_node(entry))?;
        let distance = self.search_distance(query, node)?;
        workspace.visited.insert(entry);
        workspace
            .candidates
            .push((Reverse(OrderedFloat(distance)), entry));
        workspace.results.push((OrderedFloat(distance), entry));

        while let Some((Reverse(OrderedFloat(distance)), id)) = workspace.candidates.pop() {
            if workspace.results.len() >= ef
                && workspace.results.peek().is_some_and(|r| distance > r.0.0)
            {
                break;
            }
            if let Some(node) = nodes.get(&id)
                && let Some(neighbors) = node.neighbors.get(layer as usize)
            {
                for &(id, _) in neighbors {
                    if workspace.visited.insert(id)
                        && let Some(node) = nodes.get(&id)
                        && node.layer >= layer
                    {
                        let distance = self.search_distance(query, node)?;
                        if workspace.results.len() < ef
                            || workspace.results.peek().is_some_and(|r| distance < r.0.0)
                        {
                            workspace
                                .candidates
                                .push((Reverse(OrderedFloat(distance)), id));
                            workspace.results.push((OrderedFloat(distance), id));
                            if workspace.results.len() > ef {
                                workspace.results.pop();
                            }
                        }
                    }
                }
            }
        }
        output.reserve(workspace.results.len());
        while let Some((distance, id)) = workspace.results.pop() {
            output.push((id, distance.0));
        }
        output.reverse();
        Ok(())
    }

    fn search_distance(
        &self,
        query: &PreparedQuery<'_>,
        node: &GraphNode,
    ) -> Result<f32, HnswError> {
        query
            .compute(&node.vector, node.norm)
            .map_err(|err| HnswError::Generic {
                name: self.name.clone(),
                source: err.into(),
            })
    }

    fn missing_node(&self, id: u64) -> HnswError {
        HnswError::NotFound {
            name: self.name.clone(),
            id,
        }
    }
}
