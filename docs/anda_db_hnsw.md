# anda_db_hnsw — HNSW Vector Index

`anda_db_hnsw` is the approximate nearest-neighbor (ANN) index used as the
vector-search backbone of AndaDB's long-term memory subsystem. It implements
the Hierarchical Navigable Small World graph described by Malkov & Yashunin
(2018) with several engineering extensions tailored to an embedded,
persistable, AI-agent workload:

- **bf16 storage** — vectors are kept in `half::bf16` to roughly halve the
  in-memory footprint relative to `f32`, with negligible impact on recall.
- **Lock-free reads** — the node table is a `papaya::HashMap`, so query paths
  never block on writers.
- **Incremental persistence** — the index is serialized as three independently
  flushable artifacts (metadata / id bitmap / dirty node blobs), so a writer
  can fsync only what changed since the last flush.
- **Concurrent bulk load** — restoring an index streams node blobs through a
  bounded `buffer_unordered` (concurrency `LOAD_NODES_CONCURRENCY = 32`).

This document is a deep reference for the crate's design and algorithms.
For a hands-on introduction, see the [crate README](../rs/anda_db_hnsw/README.md).

## 1. Design Goals

| Goal                                  | How it is achieved                                               |
| ------------------------------------- | ---------------------------------------------------------------- |
| Low query latency at $N \gtrsim 10^6$ | Layered graph with $O(\log N)$ expected search                   |
| Embedded (no service)                 | Everything is a library; persistence is pluggable                |
| Memory frugal                         | `bf16` vectors, Roaring bitmap of live ids, `SmallVec` adjacency |
| Concurrent reads + writes             | `papaya` lock-free map, fine-grained `RwLock`s                   |
| Crash-safe incremental saves          | Version-watermarked, idempotent flush protocol                   |
| Small dependency tree                 | Pure Rust; no BLAS, no async runtime required                    |

## 2. HNSW Algorithm Primer

HNSW builds a multi-layer proximity graph. Layer 0 contains every live
element; upper layers contain a random sparse subset. Every search is a
greedy descent from the top layer to layer 0, followed by a layer-0 beam
search.

### 2.1 Layer Assignment

New nodes pick their highest layer by sampling from a (truncated)
geometric-ish distribution whose expected occupancy at layer $\ell$ decays
exponentially:

$$P(\ell) \;=\; (1-p)\, p^{\ell},\quad p \;=\; e^{-1/\operatorname{scale}}$$

In this crate the sample is drawn as $\ell = \lfloor -\ln(u)\cdot \operatorname{scale}\rfloor$
with $u \sim \mathrm{Uniform}(0,1]$, then clamped so that no node skips more
than one existing layer at a time. The `scale` factor is
`scale_factor.unwrap_or(1.0) / ln(max_connections)`.

See `LayerGen::generate` in [distance.rs](../rs/anda_db_hnsw/src/distance.rs).

### 2.2 Greedy Descent

Given entry point $e$ at the top layer $L$, for each layer $\ell$ from $L$ down
to 1, the algorithm repeatedly moves to the neighbor of the current node
that is closer to the query, until no neighbor improves the distance. This
yields an entry point at layer $\ell-1$ in $O(\log N)$ expected hops.

### 2.3 Beam Search (`search_layer`)

At layer 0 (or any layer during construction) the search is widened into a
beam of size $\mathrm{ef}$. It maintains:

- a *candidates* min-heap of nodes to visit, and
- a *results* max-heap (bounded to `ef`) of the best found so far.

A candidate is expanded only if it could still improve the top of the results
heap; the search terminates when the next candidate is farther than the
current farthest result.

### 2.4 Neighbor Selection

When a node is inserted or pruned, an over-sized candidate list must be
reduced to $M$ connections. Two strategies are available (configured via
`SelectNeighborsStrategy`):

- **`Simple`** — just take the $M$ closest. Fastest; recall degrades on
  strongly clustered data because many picked neighbors are mutually close.
- **`Heuristic`** — seeds with the single closest candidate, then at each
  step picks the remaining candidate that maximises

  $$\text{score}(c) \;=\; \underbrace{\overline{d(c,\mathrm{selected})}}_{\text{diversity}} \;-\; \lambda \cdot d(c,q),\qquad \lambda = 0.5,$$

  so that the selected set spreads across the local manifold. This mirrors
  Algorithm 4 of the original HNSW paper, tuned with a diversity term
  normalized over the current selected-set size.

The heuristic strategy is the default.

## 3. In-Memory Data Structures

```
HnswIndex
├── config:        HnswConfig                (frozen at construction)
├── layer_gen:     LayerGen
├── nodes:         papaya::HashMap<u64, HnswNode>   ← lock-free hot path
├── entry_point:   RwLock<(u64, u8)>         ← (id, layer)
├── metadata:      RwLock<HnswMetadata>      ← snapshot-able
├── ids:           RwLock<croaring::Treemap> ← live ids
├── dirty_nodes:   RwLock<BTreeSet<u64>>     ← pending flush
├── search_count:  AtomicU64
└── last_saved_version: AtomicU64            ← flush watermark
```

Each `HnswNode` stores:

```rust
pub struct HnswNode {
    pub id:       u64,
    pub layer:    u8,                               // highest layer
    pub vector:   Vec<bf16>,                        // stored vector
    pub neighbors: Vec<SmallVec<[(u64, bf16); 64]>>, // per-layer adjacency
    pub version:  u64,                              // write counter
}
```

`neighbors[l]` is the adjacency list at layer $l$. Edge weights are kept in
`bf16` because they are only used as cheap tie-breakers during pruning;
re-computation in `f32` happens whenever we need true precision.

Several invariants hold:

- `neighbors.len() == layer + 1`.
- Layer-0 lists are bounded by `2 * max_connections`; higher layers by
  `max_connections`. A 20 % slack (`max_conns + max_conns/5`) is permitted
  before a prune is triggered, to amortise the cost of `select_neighbors`.
- An edge may exist in one direction only: HNSW allows a lower-layer node
  to appear in a higher-layer node's neighbor list but forbids the reverse
  (the lower-layer node does not exist at that higher layer).

## 4. Insert Pipeline

`HnswIndex::insert` is organised into four explicit phases:

1. **Descend & collect.** Starting from the current entry point, greedily
   descend to the candidate layer. For each layer $\ell$ from the node's
   layer down to 0, run `search_layer` with width `ef_construction`, then
   pick neighbors via `select_neighbors`. Forward edges go into the new
   node's own adjacency list; required reverse edges are buffered in a
   `FxHashMap<u64, SmallVec<[(u8,(u64,bf16));8]>>`.

2. **Publish the new node.** Insert the `HnswNode` into the `papaya` map.
   From this point, concurrent searches can reach it. If the new node
   sits on a higher layer than the current entry point (or the entry point
   was concurrently deleted), promote it.

3. **Apply reverse edges + in-place pruning.** For each affected neighbor:
   - clone once (`papaya` has no in-place update API),
   - append all pending reverse edges,
   - if any layer's list exceeds `1.2 * max_conns`, re-run
     `select_neighbors` over the *current* adjacency list and replace it
     with the selected subset,
   - bump `version`, mark the id dirty, and write back via `nodes.insert`.

   This merged pass guarantees exactly one clone and one write per affected
   neighbor per insertion.

4. **Commit dirty set.** The local `BTreeSet<u64>` of touched ids is
   merged into the index-wide `dirty_nodes` under a single lock acquisition.

### Errors

- `DimensionMismatch` — vector length ≠ `config.dimension`.
- `Generic` — vector contains `NaN` / `±∞` (validated via `normalize`).
- `AlreadyExists` — an entry with `id` is already present.

## 5. Delete Semantics

`HnswIndex::remove`:

1. Reads the target node, then walks its own neighbor list layer-by-layer to
   locate the set of back-referencing nodes — this avoids an $O(N)$ scan.
2. For each back-referrer, clones, drops the edge to the deleted id at the
   relevant layer, bumps `version`, marks dirty, and writes back.
3. Removes the target from `nodes` and from the `ids` bitmap.
4. If the target was the entry point, `try_update_entry_point` picks a
   replacement by iterating `ids`, preferring the highest layer present.

Stale back-references (e.g. from nodes that were not listed as neighbors of
the removed node because they were pruned away earlier) are intentionally
tolerated: `search_layer` silently skips ids that are absent from the `nodes`
map. This trades a tiny amount of read-time work for a much cheaper delete.

## 6. Search Pipeline

`HnswIndex::search(query, top_k)`:

1. Validate dimension; reject `NaN`/`Inf`.
2. Greedy descent from the entry point down to layer 1.
3. Layer-0 beam search with width $\max(\mathrm{ef\_search},\,\mathrm{top\_k})$.
4. Truncate to `top_k` and sort by ascending distance.

`ef_search` is the primary recall/latency knob at query time; values in the
range `2 × top_k … 10 × top_k` are typical.

## 7. Persistence Model

Persistence is split into three artifacts:

### 7.1 Metadata (`HnswMetadata` — CBOR)

Carries `name`, the full `HnswConfig`, the live `HnswStats`, and the current
`entry_point`. Written by `store_metadata`. A monotonically increasing
`version` makes the call idempotent: if the on-disk version already matches
the in-memory one, the call is a no-op.

### 7.2 Ids (`croaring::Treemap` — Portable format)

Written by `store_ids` whenever the metadata is actually flushed. The bitmap
is used at load time to issue concurrent reads for every live node.

### 7.3 Nodes (per-id CBOR blobs)

Written by `store_dirty_nodes`, which consumes (but does not yet remove)
entries from the `dirty_nodes` set. The caller supplies an async closure
`F: AsyncFn(u64, &[u8]) -> Result<bool, BoxError>`:

- `Ok(true)` — persisted; continue.
- `Ok(false)` — stop cleanly; unprocessed ids are returned to `dirty_nodes`.
- `Err(e)`  — stop with error; the failing id is also requeued, so no data
  is silently dropped on I/O failure.

Crucially, the `papaya` pin guard is *only* held while serialising a node;
it is dropped before the `.await` to the user callback. Papaya guards
contain raw pointers and are therefore `!Send`, so they must not span
`.await` points — see the `test_async_send_lifetime` test in the crate.

### 7.4 `flush` — The Orchestrator

`flush(metadata_writer, ids_writer, now_ms, node_cb)` runs the three steps
in order and only writes ids + nodes if the metadata version actually
advanced *or* dirty nodes are still outstanding. This lets callers flush
after every write without amplifying writes when nothing has changed.

### 7.5 `load_all` — Bulk Reconstruction

```rust
pub async fn load_all<R1, R2, F>(
    metadata_reader: R1,
    ids_reader: R2,
    f: F,
) -> Result<Self, BoxError>
where
    F: AsyncFn(u64) -> Result<Option<Vec<u8>>, BoxError>,
```

After parsing metadata and the id bitmap, `load_nodes` uses

```rust
futures::stream::iter(ids)
    .map(|id| async move { (id, f(id).await) })
    .buffer_unordered(LOAD_NODES_CONCURRENCY)   // = 32
    .try_next()
    .await?
```

to issue up to 32 object-store reads in flight concurrently. The resulting
throughput is typically bound by the underlying store, not by per-node
deserialisation.

## 8. Concurrency & `Send` Safety

Hot paths are designed so that no lock is held across an `.await`:

- `papaya::LocalGuard` is `!Send` — it is always re-acquired via `nodes.pin()`
  per loop iteration and dropped before any suspension point.
- `parking_lot::RwLock` guards are held only around tiny critical sections
  (metadata snapshot, entry-point read/update, dirty-set append).
- All counters that are hit on every operation are `AtomicU64` with
  `Ordering::Relaxed` — they are *statistics*, not fences.

Writers do not serialise with each other at the granularity of the whole
index. Two concurrent inserts may both observe the same entry point and
both promote themselves to the top layer; the second one wins under the
`entry_point` write-lock. Neighbor-edge updates are similarly
last-writer-wins on each affected node's clone-then-`insert`, with the
`version` counter as the authoritative watermark.

## 9. Tuning Guide

| Parameter                   | Default     | Effect when increased                                    |
| --------------------------- | ----------- | -------------------------------------------------------- |
| `max_connections` (M)       | 32          | Higher recall; RAM grows linearly; insert cost grows too |
| `ef_construction`           | 200         | Better graph quality; slower inserts                     |
| `ef_search`                 | 50          | Higher recall; slower queries (must be ≥ `top_k`)        |
| `max_layers`                | 16          | Only matters for $N \gg 10^7$; rarely tuned              |
| `scale_factor`              | `1.0`       | `>1.0` makes upper layers denser; `<1.0` sparser         |
| `distance_metric`           | `Euclidean` | Pick the one matching your embedding model               |
| `select_neighbors_strategy` | `Heuristic` | `Simple` is faster to build, lower recall                |

Rough sizing for $N$ = 1 M, $D$ = 768, $M$ = 32:

- Vectors: $N \cdot D \cdot 2\,\mathrm{B} \approx$ 1.5 GiB
- Adjacency (layer 0, avg. 2 M edges/node): $\approx$ 640 MiB
- Total with metadata: ≈ 2.2 GiB

## 10. Error Model

```rust
pub enum HnswError {
    Generic { name, source },
    Serialization { name, source },
    DimensionMismatch { name, expected, got },
    NotFound { name, id },
    AlreadyExists { name, id },
}
```

All variants carry the index `name` so that multi-index applications can
attribute failures at the log level without extra wrapping.

## 11. API Quick Reference

| Method                                                 | Purpose                                                   |
| ------------------------------------------------------ | --------------------------------------------------------- |
| `HnswIndex::new`                                       | Create an empty in-memory index                           |
| `HnswIndex::load_all`                                  | Reconstruct from metadata + ids + async node reader       |
| `HnswIndex::load_metadata` / `load_ids` / `load_nodes` | Lower-level loaders                                       |
| `insert` / `insert_f32`                                | Add a vector (bf16 or f32 convenience)                    |
| `remove`                                               | Delete by id, fixing reverse edges and entry point        |
| `search` / `search_f32`                                | k-NN query, sorted ascending by distance                  |
| `get_node_with`                                        | Visit a node under its pin guard (for custom projections) |
| `node_ids`                                             | Snapshot the live-id set                                  |
| `has_dirty_nodes` / `dirty_nodes_len`                  | Inspect the flush backlog                                 |
| `store_metadata` / `store_ids` / `store_dirty_nodes`   | Granular persistence                                      |
| `flush`                                                | End-to-end persist in one call                            |
| `stats` / `metadata`                                   | Snapshot counters and configuration                       |

## 12. Minimal Example

```rust
use anda_db_hnsw::{HnswConfig, HnswIndex, DistanceMetric};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let index = HnswIndex::new(
    "memory".to_string(),
    Some(HnswConfig {
        dimension: 384,
        distance_metric: DistanceMetric::Cosine,
        ..Default::default()
    }),
);

// Insert
index.insert_f32(1, vec![0.1_f32; 384], 0)?;
index.insert_f32(2, vec![0.2_f32; 384], 0)?;

// Search
let hits = index.search_f32(&vec![0.15_f32; 384], 5)?;
for (id, dist) in hits { println!("{id}: {dist:.4}"); }

// Persist: metadata + ids into in-memory buffers, nodes via callback
let mut meta = Vec::new();
let mut ids  = Vec::new();
let mut blobs = std::collections::HashMap::new();
index
    .flush(&mut meta, &mut ids, 0, async |id, data| {
        blobs.insert(id, data.to_vec());
        Ok(true)
    })
    .await?;
# Ok(()) }
```

For a complete demo that also performs a load round-trip, see
[`rs/anda_db_hnsw/examples/hnsw_demo.rs`](../rs/anda_db_hnsw/examples/hnsw_demo.rs).

## References

- Yu. A. Malkov, D. A. Yashunin.
  *Efficient and robust approximate nearest neighbor search using hierarchical
  navigable small world graphs.* IEEE TPAMI, 2018.
- `papaya` crate — lock-free hash map, <https://docs.rs/papaya>.
- `croaring` crate — compressed bitmap, <https://docs.rs/croaring>.
- `half::bf16` — truncated 16-bit float, <https://docs.rs/half>.
