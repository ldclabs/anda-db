use super::*;
use futures::stream::{self, FuturesUnordered, StreamExt, TryStreamExt};

/// Complete flush result. Stopping never acknowledges a durable generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushOutcome {
    NoChanges,
    Committed,
    Stopped,
}

/// Upload limits for the owned-buffer callback API.
#[derive(Debug, Clone, Copy)]
pub struct FlushOptions {
    /// Maximum number of node callbacks running at once.
    pub node_concurrency: usize,
    /// Maximum aggregate bytes owned by concurrent node callbacks, and the
    /// maximum size of each later IDs or metadata callback payload.
    pub max_in_flight_bytes: usize,
}
impl Default for FlushOptions {
    fn default() -> Self {
        Self {
            node_concurrency: 1,
            max_in_flight_bytes: 16 * 1024 * 1024,
        }
    }
}

struct FlushSnapshot {
    metadata: HnswMetadata,
    entry_point: (u64, u8),
    dirty_ids: Vec<u64>,
    nodes: Vec<Arc<GraphNode>>,
    ids: Treemap,
    removed: BTreeMap<u64, Tombstone>,
}

impl HnswIndex {
    /// Loads the metadata, staged IDs and validated nodes. The two readers may
    /// have different concrete types. Failed loads never publish a partial map.
    pub async fn load_all<M: Read, I: Read, F>(metadata: M, ids: I, f: F) -> Result<Self, HnswError>
    where
        F: AsyncFn(u64) -> Result<Option<Vec<u8>>, BoxError>,
    {
        let mut index = Self::load_metadata(metadata)?;
        index.load_ids(ids)?;
        index.load_nodes(f).await?;
        Ok(index)
    }

    /// Starts bootstrap from metadata. Complete load_ids and load_nodes before use.
    /// Invalid configuration is rejected rather than silently changing dimensions.
    pub fn load_metadata<R: Read>(r: R) -> Result<Self, HnswError> {
        let saved: HnswIndexOwned =
            cbor2::from_reader(r).map_err(|source| HnswError::Serialization {
                name: "unknown".into(),
                source: source.into(),
            })?;
        saved.metadata.config.validate(&saved.metadata.name)?;
        let mut index =
            Self::new_with_config(saved.metadata.name.clone(), saved.metadata.config.clone());
        *index.entry_point.get_mut() = saved.entry_point;
        let version = saved.metadata.stats.version;
        index
            .search_count
            .store(saved.metadata.stats.search_count, Ordering::Relaxed);
        index.last_saved_version.store(version, Ordering::Relaxed);
        index.full_saved_version.store(version, Ordering::Relaxed);
        *index.metadata.get_mut() = saved.metadata;
        *index.removed_nodes.get_mut() = saved
            .removed_nodes
            .into_iter()
            .map(|id| (id, Tombstone::new(true)))
            .collect();
        Ok(index)
    }

    /// Stages an IDs image without changing the live graph. The image remains
    /// exactly one CBOR byte string so generic CBOR validators and legacy
    /// readers agree on its framing.
    pub fn load_ids<R: Read>(&mut self, r: R) -> Result<(), HnswError> {
        let bytes: Vec<u8> = cbor2::from_reader(r).map_err(|e| self.serialization_error(e))?;
        let ids = Treemap::try_deserialize::<Portable>(&bytes)
            .ok_or_else(|| self.operation_error("Invalid IDs bitmap"))?;
        self.pending_ids = Some(ids);
        Ok(())
    }

    /// Transactionally builds a fresh node table. Missing blobs are dropped and
    /// malformed topology is repaired; invalid vectors or excessive degree fail.
    /// Mixed-generation images are rebuilt or repaired from the vectors
    /// referenced by IDs.
    /// This is recovery of partial progress, not rollback to an atomic snapshot.
    pub async fn load_nodes<F>(&mut self, f: F) -> Result<(), HnswError>
    where
        F: AsyncFn(u64) -> Result<Option<Vec<u8>>, BoxError>,
    {
        let ids = self
            .pending_ids
            .as_ref()
            .unwrap_or(self.ids.get_mut())
            .clone();
        let id_list: Vec<u64> = ids.iter().collect();
        let mut loaded = BTreeMap::new();
        let mut missing = 0;
        let config = &self.config;
        let name = &self.name;
        let f = &f;
        let mut stream = stream::iter(id_list)
            .map(|id| async move {
                let data = f(id).await.map_err(|source| HnswError::Generic {
                    name: name.clone(),
                    source,
                })?;
                let Some(data) = data else {
                    return Ok::<_, HnswError>((id, None));
                };
                let saved: PersistedNode =
                    cbor2::from_reader(data.as_slice()).map_err(|source| {
                        HnswError::Serialization {
                            name: name.clone(),
                            source: source.into(),
                        }
                    })?;
                Self::validate_loaded_node(name, config, id, &saved)?;
                let generation = saved.generation;
                Ok((id, Some((saved.into_graph(), generation))))
            })
            .buffer_unordered(Self::LOAD_NODES_CONCURRENCY);
        while let Some((id, node)) = stream.try_next().await? {
            if let Some(node) = node {
                loaded.insert(id, (Arc::new(node.0), node.1));
            } else {
                missing += 1;
            }
        }
        drop(stream);

        let mut metadata = self.metadata.read().clone();
        let saved_version = metadata.stats.version;
        let max_generation = loaded
            .values()
            .map(|(_, generation)| *generation)
            .max()
            .unwrap_or(0)
            .max(saved_version);
        let mixed = loaded
            .values()
            .any(|(_, generation)| *generation > saved_version);
        let rebuild_safe = loaded.values().all(|(node, _)| {
            self.config
                .distance_metric
                .validate_stored(&node.vector, &self.name)
                .is_ok()
        });
        let layers: FxHashMap<_, _> = loaded.iter().map(|(&id, (n, _))| (id, n.layer)).collect();
        let vectors: FxHashMap<_, _> = loaded
            .iter()
            .map(|(&id, (node, _))| (id, node.vector.clone()))
            .collect();
        let mut repaired = BTreeSet::new();
        for (&id, (node, generation)) in &mut loaded {
            let node = Arc::make_mut(node);
            let mut changed = false;
            for (layer, edges) in node.neighbors.iter_mut().enumerate() {
                let mut seen = FxHashSet::default();
                edges.retain(|&(target, _)| {
                    let keep = target != id
                        && layers.get(&target).is_some_and(|&l| l as usize >= layer)
                        && seen.insert(target);
                    changed |= !keep;
                    keep
                });
            }
            // Old snapshots may contain stale distances after ID reuse. A
            // mixed image containing a legacy-range vector cannot safely use
            // normal insertion to rebuild, so repair all of its edges here.
            if *generation == 0 || (mixed && !rebuild_safe) {
                for edges in &mut node.neighbors {
                    edges.retain_mut(|(target, distance)| {
                        let Ok(fresh) = self
                            .config
                            .distance_metric
                            .stored(&node.vector, &vectors[target])
                            .map(bf16::from_f32)
                        else {
                            changed = true;
                            return false;
                        };
                        changed |= *distance != fresh;
                        *distance = fresh;
                        true
                    });
                }
            }
            for edges in &mut node.neighbors {
                edges.sort_unstable_by_key(|edge| edge.0);
            }
            if changed {
                node.version = node.version.saturating_add(1);
                repaired.insert(id);
            }
        }

        let mut replacement = Self::new_with_config(self.name.clone(), self.config.clone());
        let mut live = Treemap::new();
        for (&id, (node, _)) in &loaded {
            live.add(id);
            replacement.nodes.pin().insert(id, node.clone());
        }
        *replacement.ids.get_mut() = live;
        *replacement.entry_point.get_mut() = *self.entry_point.read();
        let old_entry = *replacement.entry_point.read();
        let actual_max = loaded
            .values()
            .map(|(node, _)| node.layer)
            .max()
            .unwrap_or(0);
        let entry_valid = loaded
            .get(&old_entry.0)
            .is_some_and(|(node, _)| node.layer == old_entry.1 && node.layer == actual_max);
        if !entry_valid {
            replacement.repair_entry_point();
        }
        let entry_changed = old_entry != *replacement.entry_point.read();
        // A coherent image can legitimately be disconnected after cheap
        // deletion or approximate pruning. Preserve both modern and legacy
        // graphs on round-trip; only a node generation newer than metadata is
        // proof of an interrupted flush. Rebuild when every vector satisfies
        // current insertion bounds; otherwise the edge pass above provides a
        // compatible in-place repair for historical large finite vectors.
        let mut rebuilt = mixed && rebuild_safe;

        if rebuilt && !loaded.is_empty() {
            replacement = Self::try_new_seeded(self.name.clone(), Some(self.config.clone()), 0)?;
            for (&id, (node, _)) in &loaded {
                replacement.insert(id, node.vector.to_vec(), metadata.stats.last_inserted)?;
            }
            // Approximate pruning can isolate duplicate-vector nodes. A single
            // reserved ring edge per node provides reachability after recovery.
            if !replacement.base_reachable() {
                replacement.add_recovery_ring()?;
            }
            let nodes = replacement.nodes.pin();
            for (&id, (old, _)) in &loaded {
                if let Some(node) = nodes.get(&id) {
                    let mut node = (**node).clone();
                    node.version = old.version.saturating_add(1);
                    nodes.insert(id, Arc::new(node));
                }
            }
            repaired.extend(loaded.keys().copied());
        } else if loaded.is_empty() {
            rebuilt = mixed;
        }
        replacement.rebuild_incoming();
        let max_layer = replacement.entry_point.read().1;
        let changed = rebuilt
            || mixed
            || missing > 0
            || !repaired.is_empty()
            || entry_changed
            || metadata.stats.max_layer != max_layer
            || metadata.stats.num_elements != loaded.len() as u64;
        if changed {
            metadata.stats.version = max_generation
                .checked_add(1)
                .ok_or_else(|| self.operation_error("Index generation is exhausted"))?;
        }
        metadata.stats.delete_count = metadata.stats.delete_count.saturating_add(missing as u64);
        metadata.stats.max_layer = max_layer;
        *replacement.metadata.get_mut() = metadata;
        replacement
            .search_count
            .store(self.search_count.load(Ordering::Relaxed), Ordering::Relaxed);
        replacement
            .last_saved_version
            .store(saved_version, Ordering::Relaxed);
        replacement
            .full_saved_version
            .store(saved_version, Ordering::Relaxed);
        *replacement.dirty_nodes.get_mut() = repaired;
        *replacement.removed_nodes.get_mut() = self
            .removed_nodes
            .read()
            .iter()
            .filter(|(id, _)| !loaded.contains_key(id))
            .map(|(&id, t)| (id, t.clone()))
            .collect();
        if replacement.removed_nodes.read().len() != self.removed_nodes.read().len() && !changed {
            replacement.update_metadata(|m| m.stats.version = m.stats.version.saturating_add(1));
        }
        replacement.recovery = RecoveryReport {
            missing_nodes: missing,
            repaired_nodes: replacement.dirty_nodes.read().len() + usize::from(entry_changed),
            rebuilt,
        };
        if changed {
            log::warn!(index = self.name.as_str(), missing_nodes = missing, rebuilt;
                "Repaired HNSW bootstrap state; repaired nodes and metadata will be persisted");
        }
        if let Some(rng) = self.layer_rng.get_mut().take() {
            *replacement.layer_rng.get_mut() = Some(rng);
        }
        *self = replacement;
        Ok(())
    }

    fn validate_loaded_node(
        name: &str,
        config: &HnswConfig,
        id: u64,
        node: &PersistedNode,
    ) -> Result<(), HnswError> {
        let invalid = |message: &str| HnswError::Generic {
            name: name.into(),
            source: format!("Loaded node {id}: {message}").into(),
        };
        if node.id != id {
            return Err(invalid("id does not match the requested blob"));
        }
        if node.vector.len() != config.dimension {
            return Err(HnswError::DimensionMismatch {
                name: name.into(),
                expected: config.dimension,
                got: node.vector.len(),
            });
        }
        // Historical snapshots accepted every finite bf16 vector. Keep that
        // read contract even after a repaired legacy node is saved with a new
        // generation marker. New inserts still enforce the stricter pairwise
        // edge-range invariant before they can mutate the graph.
        if node.vector.iter().any(|value| !value.is_finite()) {
            return Err(invalid("vector contains NaN or infinity"));
        }
        if node.layer >= config.max_layers || node.neighbors.len() != node.layer as usize + 1 {
            return Err(invalid("invalid layer count"));
        }
        for (layer, neighbors) in node.neighbors.iter().enumerate() {
            if neighbors.len() > config.layer_limit(layer as u8) {
                return Err(invalid("neighbor count exceeds the configured layer limit"));
            }
            if neighbors.iter().any(|&(_, d)| {
                !d.is_finite()
                    || (config.distance_metric != DistanceMetric::InnerProduct && d.to_f32() < 0.0)
                    || (config.distance_metric == DistanceMetric::Cosine && d.to_f32() > 2.0)
            }) {
                return Err(invalid("invalid edge distance"));
            }
        }
        Ok(())
    }

    fn base_reachable(&self) -> bool {
        if self.is_empty() {
            return true;
        }
        let nodes = self.nodes.pin();
        let mut seen = FxHashSet::default();
        let mut pending = vec![self.entry_point.read().0];
        while let Some(id) = pending.pop() {
            if seen.insert(id)
                && let Some(node) = nodes.get(&id)
            {
                pending.extend(node.neighbors[0].iter().map(|&(id, _)| id));
            }
        }
        seen.len() == nodes.len()
    }

    fn add_recovery_ring(&self) -> Result<(), HnswError> {
        let ids = self.node_ids();
        if ids.len() < 2 {
            return Ok(());
        }
        let nodes = self.nodes.pin();
        for (position, &id) in ids.iter().enumerate() {
            let next = ids[(position + 1) % ids.len()];
            let mut node = (**nodes.get(&id).expect("live id")).clone();
            let target = nodes.get(&next).expect("live id");
            let distance = self
                .config
                .distance_metric
                .stored(&node.vector, &target.vector)?;
            let neighbors = &mut node.neighbors[0];
            neighbors.retain(|&(id, _)| id != next);
            neighbors.truncate(self.config.layer_capacity(0) - 1);
            neighbors.push((next, bf16::from_f32(distance)));
            neighbors.sort_unstable_by_key(|edge| edge.0);
            nodes.insert(id, Arc::new(node));
        }
        Ok(())
    }

    fn operation_error(&self, source: impl Into<BoxError>) -> HnswError {
        HnswError::Generic {
            name: self.name.clone(),
            source: source.into(),
        }
    }
    fn serialization_error(&self, source: impl Into<BoxError>) -> HnswError {
        HnswError::Serialization {
            name: self.name.clone(),
            source: source.into(),
        }
    }
}

impl HnswIndex {
    /// Captures only cheap immutable handles under the structural gate. Encoding
    /// and callback I/O happen after releasing the gate.
    fn capture_flush_snapshot(&self, now_ms: u64) -> Option<FlushSnapshot> {
        let _gate = self.structural_lock.lock();
        let dirty_ids: Vec<_> = self.dirty_nodes.read().iter().copied().collect();
        let mut metadata = self.metadata();
        if self.full_saved_version.load(Ordering::Acquire) >= metadata.stats.version
            && dirty_ids.is_empty()
        {
            return None;
        }
        metadata.stats.last_saved = metadata.stats.last_saved.max(now_ms);
        let nodes = self.nodes.pin();
        Some(FlushSnapshot {
            metadata,
            entry_point: *self.entry_point.read(),
            nodes: dirty_ids
                .iter()
                .filter_map(|id| nodes.get(id).cloned())
                .collect(),
            dirty_ids,
            ids: self.ids.read().clone(),
            removed: self.removed_nodes.read().clone(),
        })
    }

    fn encode_metadata(
        &self,
        metadata: &HnswMetadata,
        entry_point: (u64, u8),
        removed_nodes: Vec<u64>,
    ) -> Result<Vec<u8>, HnswError> {
        let mut bytes = Vec::new();
        cbor2::to_writer(
            &HnswIndexRef {
                metadata,
                entry_point,
                removed_nodes,
            },
            &mut bytes,
        )
        .map_err(|e| self.serialization_error(e))?;
        Ok(bytes)
    }

    fn encode_ids(&self, mut ids: Treemap) -> Result<Vec<u8>, HnswError> {
        ids.run_optimize();
        let mut bytes = Vec::new();
        cbor2::to_writer(
            &cbor2::Value::Bytes(ids.serialize::<Portable>()),
            &mut bytes,
        )
        .map_err(|e| self.serialization_error(e))?;
        Ok(bytes)
    }

    fn commit_flush_snapshot(&self, snapshot: &FlushSnapshot) {
        let _gate = self.structural_lock.lock();
        let nodes = self.nodes.pin();
        let mut dirty = self.dirty_nodes.write();
        for node in &snapshot.nodes {
            if nodes
                .get(&node.id)
                .is_some_and(|current| Arc::ptr_eq(current, node))
            {
                dirty.remove(&node.id);
            }
        }
        for id in &snapshot.dirty_ids {
            if !nodes.contains_key(id) {
                dirty.remove(id);
            }
        }
        drop(dirty);
        let mut removed = self.removed_nodes.write();
        for (id, saved) in &snapshot.removed {
            if let Some(current) = removed.get_mut(id)
                && Arc::ptr_eq(&current.token, &saved.token)
            {
                current.committed = true;
            }
        }
        drop(removed);
        self.full_saved_version
            .fetch_max(snapshot.metadata.stats.version, Ordering::Release);
        self.commit_metadata(
            snapshot.metadata.stats.version,
            snapshot.metadata.stats.last_saved,
        );
    }

    fn commit_metadata(&self, version: u64, now_ms: u64) {
        self.last_saved_version
            .fetch_max(version, Ordering::Release);
        self.update_metadata(|m| m.stats.last_saved = m.stats.last_saved.max(now_ms));
    }

    fn legacy_outcome(&self, outcome: FlushOutcome) -> Result<bool, HnswError> {
        match outcome {
            FlushOutcome::Committed => Ok(true),
            FlushOutcome::NoChanges => Ok(false),
            FlushOutcome::Stopped => {
                Err(self.operation_error("Flush stopped before commit; changes remain pending"))
            }
        }
    }

    /// Persists nodes, then the IDs image, then metadata. Callbacks confirm
    /// durability; use atomic writes plus node/metadata CAS (or immutable node
    /// generation paths) in production.
    ///
    /// The caller MUST serialize all persistence/purge calls. Structural
    /// mutations may overlap; later mutations remain pending. A stopped node
    /// callback returns an error here; use flush_with_options for explicit status.
    ///
    /// Fixed-key objects are recoverable partial progress, not atomic snapshots:
    /// generation markers let bootstrap recover a mixed image from live vectors.
    pub async fn flush_with<N, NF, I, IF, M, MF>(
        &self,
        now_ms: u64,
        node_f: N,
        ids_f: I,
        metadata_f: M,
    ) -> Result<bool, HnswError>
    where
        N: FnMut(u64, Vec<u8>) -> NF,
        NF: Future<Output = Result<bool, BoxError>>,
        I: FnOnce(Vec<u8>) -> IF,
        IF: Future<Output = Result<(), BoxError>>,
        M: FnOnce(Vec<u8>) -> MF,
        MF: Future<Output = Result<(), BoxError>>,
    {
        let outcome = self
            .flush_with_options(now_ms, FlushOptions::default(), node_f, ids_f, metadata_f)
            .await?;
        self.legacy_outcome(outcome)
    }

    /// Explicit flush status and bounded upload concurrency/serialized-payload
    /// budget. Errors, cancellation and Stopped leave the entire snapshot
    /// retryable. Before an error or Stopped result is returned, every node
    /// callback already created by this method is awaited to completion.
    pub async fn flush_with_options<N, NF, I, IF, M, MF>(
        &self,
        now_ms: u64,
        options: FlushOptions,
        mut node_f: N,
        ids_f: I,
        metadata_f: M,
    ) -> Result<FlushOutcome, HnswError>
    where
        N: FnMut(u64, Vec<u8>) -> NF,
        NF: Future<Output = Result<bool, BoxError>>,
        I: FnOnce(Vec<u8>) -> IF,
        IF: Future<Output = Result<(), BoxError>>,
        M: FnOnce(Vec<u8>) -> MF,
        MF: Future<Output = Result<(), BoxError>>,
    {
        if !(1..=64).contains(&options.node_concurrency) || options.max_in_flight_bytes == 0 {
            return Err(self
                .operation_error("Flush concurrency must be in 1..=64 and byte budget positive"));
        }
        let Some(snapshot) = self.capture_flush_snapshot(now_ms) else {
            return Ok(FlushOutcome::NoChanges);
        };
        let generation = snapshot.metadata.stats.version;
        let node_sizes = snapshot
            .nodes
            .iter()
            .map(|node| node.encoded_size(generation))
            .collect::<Result<Vec<_>, _>>()?;
        if node_sizes
            .iter()
            .any(|&size| size > options.max_in_flight_bytes)
        {
            return Err(self.operation_error("A node exceeds the configured flush byte budget"));
        }
        let mut pending = FuturesUnordered::new();
        let mut offset = 0;
        let mut in_flight_bytes = 0;
        let mut first_error = None;
        let mut stopped = false;
        while offset < snapshot.nodes.len() || !pending.is_empty() {
            if (first_error.is_some() || stopped) && pending.is_empty() {
                break;
            }
            while first_error.is_none()
                && !stopped
                && offset < snapshot.nodes.len()
                && pending.len() < options.node_concurrency
            {
                let node = &snapshot.nodes[offset];
                let size = node_sizes[offset];
                if size > options.max_in_flight_bytes - in_flight_bytes {
                    break;
                }
                let bytes = match node.encode_sized(generation, size) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        first_error = Some(error);
                        break;
                    }
                };
                debug_assert_eq!(bytes.len(), size);
                let future = node_f(node.id, bytes);
                pending.push(async move { (size, future.await) });
                in_flight_bytes += size;
                offset += 1;
            }
            if let Some((size, result)) = pending.next().await {
                in_flight_bytes -= size;
                match result {
                    Ok(true) => {}
                    Ok(false) => stopped = true,
                    Err(error) if first_error.is_none() => {
                        first_error = Some(self.operation_error(error));
                    }
                    Err(_) => {}
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        if stopped {
            return Ok(FlushOutcome::Stopped);
        }

        let ids = self.encode_ids(snapshot.ids.clone())?;
        if ids.len() > options.max_in_flight_bytes {
            return Err(
                self.operation_error("The IDs image exceeds the configured flush byte budget")
            );
        }
        ids_f(ids).await.map_err(|e| self.operation_error(e))?;

        let metadata = self.encode_metadata(
            &snapshot.metadata,
            snapshot.entry_point,
            snapshot.removed.keys().copied().collect(),
        )?;
        if metadata.len() > options.max_in_flight_bytes {
            return Err(
                self.operation_error("The metadata image exceeds the configured flush byte budget")
            );
        }
        metadata_f(metadata)
            .await
            .map_err(|e| self.operation_error(e))?;
        self.commit_flush_snapshot(&snapshot);
        Ok(FlushOutcome::Committed)
    }

    /// Writer compatibility adapter. Checks write_all AND Write::flush before
    /// acknowledging metadata/IDs. For fsync/object-store confirmation use
    /// flush_with. Do not truncate target files before a possible no-op call.
    pub async fn flush<M: Write, I: Write, F>(
        &self,
        metadata: M,
        ids: I,
        now_ms: u64,
        f: F,
    ) -> Result<bool, HnswError>
    where
        F: AsyncFnMut(u64, &[u8]) -> Result<bool, BoxError>,
    {
        let result = self.flush_outcome(metadata, ids, now_ms, f).await?;
        self.legacy_outcome(result)
    }

    /// Writer adapter with explicit NoChanges/Committed/Stopped status.
    pub async fn flush_outcome<M: Write, I: Write, F>(
        &self,
        mut metadata: M,
        mut ids: I,
        now_ms: u64,
        mut f: F,
    ) -> Result<FlushOutcome, HnswError>
    where
        F: AsyncFnMut(u64, &[u8]) -> Result<bool, BoxError>,
    {
        let Some(snapshot) = self.capture_flush_snapshot(now_ms) else {
            return Ok(FlushOutcome::NoChanges);
        };
        let generation = snapshot.metadata.stats.version;
        // AsyncFnMut can borrow its captures across await, so this compatibility
        // adapter deliberately uploads one borrowed buffer at a time.
        for node in &snapshot.nodes {
            let bytes = node.encode(generation)?;
            if !f(node.id, &bytes)
                .await
                .map_err(|e| self.operation_error(e))?
            {
                return Ok(FlushOutcome::Stopped);
            }
        }
        let id_bytes = self.encode_ids(snapshot.ids.clone())?;
        let metadata_bytes = self.encode_metadata(
            &snapshot.metadata,
            snapshot.entry_point,
            snapshot.removed.keys().copied().collect(),
        )?;
        ids.write_all(&id_bytes)
            .and_then(|_| ids.flush())
            .map_err(|e| self.serialization_error(e))?;
        metadata
            .write_all(&metadata_bytes)
            .and_then(|_| metadata.flush())
            .map_err(|e| self.serialization_error(e))?;
        self.commit_flush_snapshot(&snapshot);
        Ok(FlushOutcome::Committed)
    }

    pub fn has_dirty_nodes(&self) -> bool {
        !self.dirty_nodes.read().is_empty()
    }
    pub fn has_removed_nodes(&self) -> bool {
        !self.removed_nodes.read().is_empty()
    }
    pub fn removed_node_ids(&self) -> Vec<u64> {
        self.removed_nodes.read().keys().copied().collect()
    }
    /// Includes changes to IDs not acknowledged by a complete flush.
    pub fn has_pending_flush(&self) -> bool {
        self.has_dirty_nodes()
            || self.has_removed_nodes()
            || self.full_saved_version.load(Ordering::Acquire) < self.metadata.read().stats.version
    }
    pub fn has_pending_metadata_flush(&self) -> bool {
        self.last_saved_version.load(Ordering::Acquire) < self.metadata.read().stats.version
    }

    /// Deletes only tombstones acknowledged by a complete flush (or loaded from
    /// committed metadata). Pending later removals are never handed to f.
    /// Serialize with all other persistence calls; insert/remove may overlap.
    /// Ok(false), errors and cancellation preserve the current tombstone.
    pub async fn purge_removed_nodes<F>(&self, mut f: F) -> Result<(), HnswError>
    where
        F: AsyncFnMut(u64) -> Result<bool, BoxError>,
    {
        let removed: Vec<_> = self
            .removed_nodes
            .read()
            .iter()
            .filter(|(_, t)| t.committed)
            .map(|(&id, t)| (id, t.token.clone()))
            .collect();
        for (id, token) in removed {
            {
                let _gate = self.structural_lock.lock();
                let eligible = self
                    .removed_nodes
                    .read()
                    .get(&id)
                    .is_some_and(|t| t.committed && Arc::ptr_eq(&token, &t.token));
                if !eligible || self.nodes.pin().contains_key(&id) {
                    continue;
                }
            }
            if !f(id).await.map_err(|e| self.operation_error(e))? {
                return Ok(());
            }
            let _gate = self.structural_lock.lock();
            let mut removed = self.removed_nodes.write();
            if removed
                .get(&id)
                .is_some_and(|t| Arc::ptr_eq(&token, &t.token))
            {
                removed.remove(&id);
                drop(removed);
                self.update_metadata(|m| m.stats.version = m.stats.version.saturating_add(1));
            }
        }
        Ok(())
    }

    fn metadata_image(&self, now_ms: u64) -> Result<(u64, u64, Vec<u8>), HnswError> {
        let (mut metadata, entry, removed) = {
            let _gate = self.structural_lock.lock();
            (
                self.metadata(),
                *self.entry_point.read(),
                self.removed_node_ids(),
            )
        };
        metadata.stats.last_saved = metadata.stats.last_saved.max(now_ms);
        let bytes = self.encode_metadata(&metadata, entry, removed)?;
        Ok((metadata.stats.version, metadata.stats.last_saved, bytes))
    }

    /// Serializes current metadata without advancing any persistence watermark.
    pub fn metadata_bytes(&self) -> Result<Vec<u8>, HnswError> {
        self.metadata_image(0).map(|(_, _, b)| b)
    }

    /// Serializes metadata into a writer and checks its flush. This does not
    /// acknowledge IDs/nodes or authorize purge. Prefer metadata_bytes for an
    /// in-memory serialization, and store_metadata_with for external persistence.
    pub fn store_metadata<W: Write>(&self, mut w: W, now_ms: u64) -> Result<bool, HnswError> {
        if !self.has_pending_metadata_flush() {
            return Ok(false);
        }
        let (version, saved, bytes) = self.metadata_image(now_ms)?;
        w.write_all(&bytes)
            .and_then(|_| w.flush())
            .map_err(|e| self.serialization_error(e))?;
        self.commit_metadata(version, saved);
        Ok(true)
    }

    /// Confirms only metadata after callback success; never authorizes purge.
    pub async fn store_metadata_with<F>(&self, now_ms: u64, f: F) -> Result<bool, HnswError>
    where
        F: AsyncFnOnce(&[u8]) -> Result<(), BoxError>,
    {
        if !self.has_pending_metadata_flush() {
            return Ok(false);
        }
        let (version, saved, bytes) = self.metadata_image(now_ms)?;
        f(&bytes).await.map_err(|e| self.operation_error(e))?;
        self.commit_metadata(version, saved);
        Ok(true)
    }

    /// Writes the single-item IDs CBOR image and checks the writer flush.
    pub fn store_ids<W: Write>(&self, mut w: W) -> Result<(), HnswError> {
        let ids = {
            let _gate = self.structural_lock.lock();
            self.ids.read().clone()
        };
        let bytes = self.encode_ids(ids)?;
        w.write_all(&bytes)
            .and_then(|_| w.flush())
            .map_err(|e| self.serialization_error(e))
    }

    /// Granular node persistence. Immutable identity protects acknowledgements
    /// against concurrent rewrites and remove/reinsert of the same ID.
    /// This is not a complete flush and cannot authorize tombstone purge.
    pub async fn store_dirty_nodes<F>(&self, mut f: F) -> Result<(), HnswError>
    where
        F: AsyncFnMut(u64, &[u8]) -> Result<bool, BoxError>,
    {
        let ids: Vec<_> = self.dirty_nodes.read().iter().copied().collect();
        for id in ids {
            let snapshot = {
                let _gate = self.structural_lock.lock();
                if !self.dirty_nodes.read().contains(&id) {
                    continue;
                }
                let node = self.nodes.pin().get(&id).cloned();
                if node.is_none() {
                    self.dirty_nodes.write().remove(&id);
                }
                node.map(|node| (self.metadata.read().stats.version, node))
            };
            let Some((generation, node)) = snapshot else {
                continue;
            };
            let bytes = node.encode(generation)?;
            if !f(id, &bytes).await.map_err(|e| self.operation_error(e))? {
                return Ok(());
            }
            let _gate = self.structural_lock.lock();
            if self
                .nodes
                .pin()
                .get(&id)
                .is_some_and(|current| Arc::ptr_eq(current, &node))
            {
                self.dirty_nodes.write().remove(&id);
            }
        }
        Ok(())
    }
}
