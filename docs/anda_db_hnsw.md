# anda_db_hnsw — HNSW vector index

`anda_db_hnsw` is the embeddable vector index used by AndaDB. It stores vectors
as bf16, computes distances in floating point, and supports incremental
persistence through caller-supplied callbacks.

## Usage

```rust
use anda_db_hnsw::{HnswConfig, HnswIndex, SearchOptions};

let index = HnswIndex::try_new(
    "memory".into(),
    Some(HnswConfig { dimension: 384, ..Default::default() }),
)?;
index.insert_f32(1, vec![0.1; 384], 0)?;
index.insert_f32(2, vec![0.2; 384], 0)?;

let hits = index.search_f32_with_options(
    &[0.15; 384],
    10,
    SearchOptions { ef_search: Some(100) },
)?;
# Ok::<(), anda_db_hnsw::HnswError>(())
```

Prefer `try_new`: it rejects invalid configuration. The compatibility
constructor `new` normalizes invalid values into the supported range.
`try_new_seeded(name, config, seed)` provides reproducible layer assignment
for tests and benchmarks with the same dependency versions and insertion order.
The seed is runtime state, not part of the persisted configuration.

## Configuration and queries

| Parameter | Default | Accepted range / meaning |
| --- | --- | --- |
| dimension | 512 | 1–16,384 |
| max_layers | 16 | 1–64; layer indices start at zero |
| max_connections | 32 | 2–128; layer zero targets 2×M |
| ef_construction | 200 | 1–4,096 |
| ef_search | 50 | 1–4,096; actual beam is max(ef, top_k) |
| scale_factor | None / 1.0 | Finite and positive |
| distance_metric | Euclidean | Euclidean, Cosine, InnerProduct, Manhattan |
| select_neighbors_strategy | Heuristic | Simple or Heuristic |
| reconnect_on_delete | false | Whether deletion attempts local reconnection |

Per-layer out-degree is bounded by `capacity + capacity/5`, where capacity is
2×M at layer zero and M above it. The allowance amortizes pruning.

Both query APIs treat `top_k == 0` as an unconditional no-op, without input
validation or a search-counter increment. Positive requests validate dimension
and finite input values. Requests above 4,096 return an error instead of
silently returning fewer results because of a hidden beam cap. An approximate
search can still return fewer than k results on a small or disconnected graph.

`search_f32` keeps the query in f32, avoiding query quantization.
`search` promotes a bf16 query once. Results are sorted by ascending distance.

`search_f32_with_options` changes ef for one query. The default methods reuse
thread-local scratch storage with a safe fallback for reentrant calls.
`search_f32_with_workspace` accepts an exclusively borrowed `SearchWorkspace`
for applications that want to own those allocations.

## Distance semantics and numeric bounds

| Metric | Distance |
| --- | --- |
| Euclidean | sqrt(sum((a − b)²)) |
| Cosine | 1 − cosine similarity, clamped to [0, 2] |
| InnerProduct | −dot(a, b) |
| Manhattan | sum(abs(a − b)) |

Smaller distances are better, including negative inner-product distances.
Cosine returns 1 when either norm is below f32::EPSILON.

Public `DistanceMetric::compute`, `compute_f32` and `compute_mixed` reject
dimension mismatches and non-finite inputs. Kernels keep the ordinary f32 fast
path, with wider arithmetic for overflow and Euclidean square underflow.
A result outside the finite f32 range is an error.

The index also checks a conservative **stored-vector** magnitude bound before
mutation so that every pair of accepted nodes has a finite bf16 edge distance.
Let B = bf16::MAX expressed as f64:

| Metric | Stored-vector bound |
| --- | --- |
| Euclidean | L2 norm ≤ B / 4 |
| Manhattan | L1 norm ≤ B / 4 |
| InnerProduct | L2 norm ≤ sqrt(B / 4) |
| Cosine | Any finite bf16 elements |

These ranges are far larger than typical embeddings. A finite f32 input that
overflows during bf16 conversion is rejected too. Query distance overflow
returns an error rather than inserting NaN/Inf into ranking heaps.
Rejected vector input leaves the graph and public operation counters unchanged.

Cosine query norms are prepared once; immutable node norms are cached.
The API still returns Euclidean distance, not squared L2.

## Graph storage and mutation

The public `HnswNode` type and its existing serde fields remain available:

```rust
# use anda_db_hnsw::half::bf16;
pub struct HnswNode {
    pub id: u64,
    pub layer: u8,
    pub vector: Vec<bf16>,
    pub neighbors: Vec<smallvec::SmallVec<[(u64, bf16); 64]>>,
    pub version: u64,
}
```

The hot runtime representation is separate: an immutable node is held by Arc,
its bf16 vector is shared by Arc, and each adjacency layer uses a Vec.
Updating edges copies adjacency without copying the vector. Adjacency lists are
ordered by target ID and contain no duplicates. Exact reverse references are
maintained per target and layer, including asymmetric edges created by pruning.

`get_node_with` materializes the public owned representation for compatibility;
it is intended for inspection/export, not the search hot path. Individual
`HnswNode::version` counters describe mutations of one node instance, not a
globally unique incarnation of a reusable ID. Use the index's persistence APIs
for generation-safe acknowledgement.

Deletion removes **all** incoming references before the ID can be reused.
A non-entry deletion retains the maximum layer because the surviving entry
already proves that layer exists. Only entry deletion scans for a replacement.

With `reconnect_on_delete=false`, deletions only remove edges; deletion-heavy
workloads can lose recall and connectivity. With true, affected incoming
neighbors also consider the deleted node's peers as reconnection candidates.
This improves resilience, but it is not a proof of connectivity for every
dataset/deletion order and costs additional distance work under the writer gate.

## Concurrency

Insert/remove are synchronous and serialized by a structural mutex. Search
does not take that mutex; it uses the concurrent node table and briefly reads
the entry-point RwLock. The whole query is therefore not described as lock-free
or as a linearizable snapshot. Missing entry points during concurrent deletion
are retried at most `SEARCH_MAX_ATTEMPTS` times.

**All persistence and purge calls must be serialized by the caller.**
Structural mutations may overlap with a flush; later changes stay pending.
AndaDB's Collection already holds its exclusive operation gate across flush,
which also excludes collection mutations.

No synchronous lock or papaya local pin guard spans a callback await.
Snapshots capture immutable node handles under the gate; encoding and I/O run
after it is released. A successful flush clears a node's dirty mark only if its
current Arc identity still matches the snapshot. Remove/reinsert cannot cause an
ABA acknowledgement even when the public node counter restarts.

## Persistence APIs

Prefer `flush_with_options` for owned buffers and explicit status:

```rust
use anda_db_hnsw::{BoxError, FlushOptions, FlushOutcome, HnswIndex};

# async fn example(index: &HnswIndex) -> Result<(), BoxError> {
let outcome = index.flush_with_options(
    1234,
    FlushOptions {
        node_concurrency: 8,
        max_in_flight_bytes: 16 * 1024 * 1024,
    },
    |id, bytes| async move {
        // Atomically persist this node object and confirm durability.
        # let _ = (id, bytes);
        Ok(true)
    },
    |bytes| async move {
        // Atomically persist the IDs object.
        # let _ = bytes;
        Ok(())
    },
    |bytes| async move {
        // Persist metadata last; use the backend's conditional-write token.
        # let _ = bytes;
        Ok(())
    },
).await?;

match outcome {
    FlushOutcome::Committed => { /* a complete pass was acknowledged */ }
    FlushOutcome::NoChanges => { /* no callbacks ran */ }
    FlushOutcome::Stopped => { /* no commit; changes remain pending */ }
}
# Ok(()) }
```

The placeholders above illustrate callback contracts; the complete
[filesystem demo](../rs/anda_db_hnsw/examples/hnsw_demo.rs) uses
`anda_object_store::MetaStore` over `object_store::LocalFileSystem` for
generation-backed CAS and staged cross-platform replacement, and requests
fsync durability where the platform supports it.

Upload concurrency is 1–64. The aggregate node callback buffers in flight and
each later IDs/metadata callback buffer stay within the configured byte budget.
A single payload larger than the budget is an error. Exact node sizes use
cbor2::serialized_size. IDs are written only after all node callbacks succeed;
metadata is written last. A node callback returning false stops without
committing. On an error or stop, every node callback already started by the
flush is awaited before the method returns. Cancellation can still leave an
individual backend write durable, so dirty evidence is retained for recovery.

Compatibility `flush_with` returns true only for a committed pass and false
for no changes; an early stop is now an error. `flush_outcome` gives the same
explicit status for borrowed-buffer node callbacks and synchronous writers.
`flush` is its bool compatibility adapter.

Writer adapters check both write_all and Write::flush before acknowledging the
pass. Write::flush is **not** a file fsync or remote durability guarantee.
For those guarantees use callbacks. Never truncate existing metadata/IDs files
before calling an API that may return NoChanges.

Granular interfaces remain available:

| API | Contract |
| --- | --- |
| metadata_bytes | Pure serialization; advances no watermark |
| store_metadata | Checks the writer flush, acknowledges metadata only |
| store_metadata_with | Acknowledges metadata after callback success |
| store_ids | Writes the IDs image and checks the writer flush |
| store_dirty_nodes | Acknowledges each unchanged node after callback success |

Granular metadata writes do not acknowledge a complete generation or authorize
purging. Writing to an in-memory Vec does not confirm an external backend write;
use metadata_bytes to serialize or store_metadata_with to confirm the backend.

## Tombstones and purge

The normal sequence is:

1. Complete a flush containing the deletion.
2. Call `purge_removed_nodes` to delete eligible node blobs.
3. Flush again, or on the next periodic pass, to save the reduced tombstone set.

A tombstone becomes eligible only when its exact removal instance was included
in a **complete** acknowledged snapshot, or loaded from committed metadata.
A removal occurring during flush belongs to a later pass. Reinserted IDs and
newer removal instances are checked around the callback and cannot accidentally
consume an older acknowledgement.

Purge callbacks return true to acknowledge, false to stop, or an error. Treat
“blob not found” as success: a crash after physical deletion but before the next
metadata flush can replay the deletion. Cancellation preserves unacknowledged
tombstones. `has_pending_flush` includes nodes, metadata/IDs and tombstones.

## Wire compatibility and recovery

The existing metadata fields and public node fields remain readable.
New node objects add an optional `g` field containing the pass generation.
The IDs object remains exactly one CBOR byte string containing the Portable
Roaring treemap. Older AndaDB readers and strict single-item CBOR validators
therefore read the same framing.

Each backend must atomically replace individual objects. Fixed-key node and
metadata replacements must also use backend conditional writes (CAS), or node
objects must use immutable generation-specific paths. This prevents an old
request whose result arrives late from replacing a newer committed object. The
sequence nodes → IDs → metadata is **recoverable partial progress**, not a
multi-object transaction or a promise to reopen the previous snapshot.
The AndaDB adapter retains one backend `ObjectVersion` token per live node (and
per unpurged tombstone); purging a blob removes its token.

Bootstrap proceeds transactionally in memory:

1. Validate metadata/config and stage IDs without changing a live graph.
2. Fetch/validate node objects with bounded concurrency, up to 32 in flight.
3. Remove missing references, duplicate edges, self-loops and invalid layer
   edges; repair the entry point and maximum layer. Reject non-finite numeric
   data, invalid node IDs/layer shapes and excessive degree. Persisted finite
   vectors that predate current insertion bounds remain loadable after later
   repair passes and saves.
4. Recompute legacy cached distances once during migration. Drop a legacy edge
   whose distance cannot be represented by the persisted `bf16` edge format.
5. If a node pass marker is newer than committed metadata, rebuild from vectors
   referenced by the loaded IDs. When a historical vector exceeds current
   insertion bounds, normalize topology and cached distances in place instead.
   If pruning still isolates nodes, reserve one bounded ring edge per node in a
   rebuilt graph. Coherent modern and legacy images retain their original graph,
   even if approximate pruning or cheap deletion left it disconnected.
6. Publish the replacement only after success; mark repairs dirty.

The loaded IDs object defines which vectors recovery retains. A failed pass can
therefore recover earlier or later progress depending on whether IDs were
replaced. Operation counters originate from the saved metadata; they are not
an audit log of writes that occurred after it. AndaDB's Collection additionally
replays document mutation intents to recover authoritative document state.

`recovery_report` exposes missing-node count, repair count and whether rebuilding
was required. A loader returning None explicitly permits dropping a missing blob;
return an error instead if missing data must abort your application's open.
Failure or cancellation during node loading leaves the previous runtime graph
intact and the staged IDs available for retry.

Invalid persisted configuration is rejected, rather than silently changing a
dimension or graph limit. Persisted repair results are saved by the next flush.

## Testing, sizing and benchmarks

```sh
cargo test -p anda_db_hnsw --all-targets
cargo clippy -p anda_db_hnsw --all-targets -- -D warnings
cargo run -p anda_db_hnsw --example hnsw_demo
```

The regression suite covers numeric overflow, ID reuse, buffered-writer failure,
stop/cancellation contracts, tombstone generations, mixed-image recovery,
transactional load retries, query bounds and bounded parallel uploads.
Recall tests use deterministic data and graph seeds.

Memory estimates must include alignment, capacities, Arc/hash-table overhead,
reverse references and temporary snapshots. On the measured 64-bit build,
(u64,bf16) occupies 16 bytes, not 10. A bf16 vector's elements take 2×dimension
bytes, but this does not describe the whole index footprint.

See the [benchmark guide](../rs/anda_db_hnsw/benches/README.md) for the configurable
data matrix, allocation/latency/recall measurements and release-profile controls.
