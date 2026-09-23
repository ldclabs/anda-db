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
    distances: FxHashMap<u64, f32>,
    visited: FxHashSet<u64>,
    candidates: BinaryHeap<(Reverse<OrderedFloat<f32>>, u64, u8)>,
    results: BinaryHeap<(OrderedFloat<f32>, u64, u8)>,
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
        self.distances.clear();
        self.visited.clear();
        self.candidates.clear();
        self.results.clear();
    }
    pub(super) fn trim(&mut self) {
        // A single unusually expensive traversal must not permanently retain
        // an unbounded allocation on every worker thread.
        if self.distances.capacity() > 131_072
            || self.visited.capacity() > 131_072
            || self.candidates.capacity() > 131_072
        {
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

    fn search_validated(
        &self,
        query: &[f32],
        top_k: usize,
        ef: usize,
        workspace: &mut SearchWorkspace,
    ) -> Result<Vec<(u64, f32)>, HnswError> {
        let query = PreparedQuery::new(self.config.distance_metric, query);
        for attempt in 0..Self::SEARCH_MAX_ATTEMPTS {
            workspace.reset();
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
            let candidate = self.greedy_search(query, id, layer, workspace)?;
            id = candidate.0;
        }
        let mut results = self.search_layer(query, id, 0, ef, workspace)?;
        results.truncate(top_k);
        Ok(results
            .into_iter()
            .map(|(id, distance, _)| (id, distance))
            .collect())
    }

    /// Allocation-free ef=1 descent. No visited set or heap is needed because
    /// each move strictly decreases the distance.
    pub(super) fn greedy_search(
        &self,
        query: &PreparedQuery<'_>,
        entry: u64,
        layer: u8,
        workspace: &mut SearchWorkspace,
    ) -> Result<(u64, f32, u8), HnswError> {
        let nodes = self.nodes.pin();
        let node = nodes.get(&entry).ok_or_else(|| self.missing_node(entry))?;
        let mut best = (
            entry,
            self.search_distance(query, node, workspace)?,
            node.layer,
        );
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
                        let distance = self.search_distance(query, node, workspace)?;
                        if distance < best.1 {
                            best = (id, distance, node.layer);
                        }
                    }
                }
            }
            if best.0 == previous {
                return Ok(best);
            }
        }
    }

    pub(super) fn search_layer(
        &self,
        query: &PreparedQuery<'_>,
        entry: u64,
        layer: u8,
        ef: usize,
        workspace: &mut SearchWorkspace,
    ) -> Result<Vec<(u64, f32, u8)>, HnswError> {
        let mut output = Vec::new();
        self.search_layer_into(query, entry, layer, ef, workspace, &mut output)?;
        Ok(output)
    }

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
            output.push(self.greedy_search(query, entry, layer, workspace)?);
            return Ok(());
        }
        workspace.visited.clear();
        workspace.candidates.clear();
        workspace.results.clear();
        let nodes = self.nodes.pin();
        let node = nodes.get(&entry).ok_or_else(|| self.missing_node(entry))?;
        let distance = self.search_distance(query, node, workspace)?;
        workspace.visited.insert(entry);
        workspace
            .candidates
            .push((Reverse(OrderedFloat(distance)), entry, node.layer));
        workspace
            .results
            .push((OrderedFloat(distance), entry, node.layer));

        while let Some((Reverse(OrderedFloat(distance)), id, _)) = workspace.candidates.pop() {
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
                        let distance = self.search_distance(query, node, workspace)?;
                        if workspace.results.len() < ef
                            || workspace.results.peek().is_some_and(|r| distance < r.0.0)
                        {
                            workspace.candidates.push((
                                Reverse(OrderedFloat(distance)),
                                id,
                                node.layer,
                            ));
                            workspace
                                .results
                                .push((OrderedFloat(distance), id, node.layer));
                            if workspace.results.len() > ef {
                                workspace.results.pop();
                            }
                        }
                    }
                }
            }
        }
        output.reserve(workspace.results.len());
        while let Some((distance, id, layer)) = workspace.results.pop() {
            output.push((id, distance.0, layer));
        }
        output.reverse();
        Ok(())
    }

    fn search_distance(
        &self,
        query: &PreparedQuery<'_>,
        node: &GraphNode,
        workspace: &mut SearchWorkspace,
    ) -> Result<f32, HnswError> {
        match workspace.distances.entry(node.id) {
            Entry::Occupied(entry) => Ok(*entry.get()),
            Entry::Vacant(entry) => {
                let distance =
                    query
                        .compute(&node.vector, node.norm)
                        .map_err(|err| HnswError::Generic {
                            name: self.name.clone(),
                            source: err.into(),
                        })?;
                entry.insert(distance);
                Ok(distance)
            }
        }
    }

    fn missing_node(&self, id: u64) -> HnswError {
        HnswError::NotFound {
            name: self.name.clone(),
            id,
        }
    }
}
