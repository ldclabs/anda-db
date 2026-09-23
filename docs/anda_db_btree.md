# anda_db_btree

[中文版](anda_db_btree.zh.md)

This reference describes the current 0.13 workspace implementation.
The index maps an ordered field value (FV) to unique document IDs (PK).
Exact lookups use a sharded hash map; ranges and string prefixes use a
separate ordered key set. Storage is supplied by the caller.

## Configuration and mutation

`BTreeConfig` has two fields:

| Field | Default | Meaning |
|---|---|---|
| `bucket_overload_size` | 512 KiB | Soft packing target, clamped to at least 64 bytes |
| `allow_duplicates` | true | Allow multiple different PKs for one FV |

The same (PK, FV) pair is always idempotent. With `allow_duplicates=false`,
a different PK for an occupied FV returns `BTreeError::AlreadyExists`.
`len()` counts distinct FVs, not documents or associations.

- `insert(pk, fv, now_ms) -> Result<bool, BTreeError>` adds one association.
- `remove(pk, fv, now_ms) -> bool` removes one association.
- `insert_array(pk, values, now_ms) -> Result<usize, BTreeError>` and
  `remove_array(...) -> usize` batch associations and coalesce duplicates.
- `batch_update(pk, old, new, now_ms) -> Result<(removed, inserted), BTreeError>`
  applies the difference. Supply the document's actual old values.

Batches amortize ordered-key locks, bucket accounting and stats. They are not
transactions. A concurrent uniqueness conflict after the precheck, or a
serialization failure mid-insertion, may leave an applied prefix. Its
bookkeeping is completed before returning the error. Batch update inserts
before removing; an insertion error leaves old associations in place.

IDs are unique. Appends preserve order, but deletion swaps the last ID into
the removed position; callers cannot rely on insertion order after a deletion.
Small postings use a compact vector. Larger ones add an ID-to-position map
for average constant-time membership and amortized deletion. After substantial
deletion, both allocations shrink geometrically, retaining room for regrowth;
the position map is dropped at four remaining IDs. Only IDs are serialized.

## Queries and pagination

`RangeQuery<FV>` supports `Eq`, `Gt`, `Ge`, `Lt`, `Le`, inclusive
`Between`, `Include`, `And`, `Or` and `Not`. These predicates operate on
field values, not on document-ID sets. An And of two different Eq values
matches no single FV, even if a document contains both values.

An inverted Between range is empty. And([]) and Or([]) are empty; Not
complements relative to indexed keys. Include and Or deduplicate keys.

| Method | Visit order | Return order |
|---|---|---|
| `range_query_with` / `try_range_query_with` | Smallest match first | Ascending keys |
| `range_query_rev_with` / `try_range_query_rev_with` | Largest match first | Ascending keys |
| `prefix_query_with` | Ascending | Ascending |
| `keys(cursor, limit)` | Ascending, cursor exclusive | Ascending |

Range callbacks receive `(&FV, &Vec<PK>)` and return `(continue, Vec<R>)`.
Results from the current callback are retained when continue=false. Reverse
scans reverse groups while preserving each callback's internal result order.
Prefix callbacks receive `(&str, &Vec<PK>)` and return
`(continue, Option<R>)`.

Boolean predicates compile to sorted disjoint intervals using only the query
input. And intersects intervals, Or unions them, and Not complements them.
No full-index candidate-key set is materialized, and nested Include does not
perform a linear membership scan for every candidate. A page stops scanning
when its callback stops; compilation still processes the input expression.

String prefixes scan from a borrowed str lower bound and stop at the first
key failing starts_with. Empty prefixes scan all keys. Keys containing
char::MAX, including suffixes after it, are supported.

### Resource limits

`RangeQuery::MAX_DEPTH = 64`, `MAX_NODES = 4096` and
`MAX_INCLUDE_KEYS = 65_536` bound total Include entries across a query.
Custom Serde decoding enforces these while constructing the tree, preserving
the externally tagged JSON/CBOR enum representation.

Programmatic queries are checked by `validate()` before conversion/evaluation.
Rejected owned queries are released iteratively, including on empty indexes;
`discard()` exposes safe iterative disposal to callers handling their own
programmatically constructed input.

Fallible query methods return BTreeError on invalid input. Non-fallible
methods log a warning and return an empty vector. Use fallible methods when
invalid input must be distinguished from no matches. `try_convert_from`
checks complexity and key conversion.

## Concurrency

Share the index through Arc. Mutations hold the mutation gate shared, update
posting/bucket shards and briefly lock metadata for stats. New or removed
ordered keys require a B-tree write lock. Compaction holds the mutation gate
exclusively; queries can still run.

Callbacks run under internal locks. Range/prefix callbacks may hold both an
ordered-key read lock and a posting guard. They must not re-enter the same
index; slow callbacks also delay writers.

Removing the last ID and removing its posting are atomic under one posting
shard lock. Concurrent queries do not receive a transient empty ID list, and
unique inserts do not mistake an emptied posting for a conflicting owner.
Ordered-key and bucket cleanup still recheck ownership after releasing that lock.

The caller must exclude mutations, compaction and other flushes for the
entire flush, including async callbacks. AndaDB's Collection operation gate
and wrapper flush gate supply this coordination. Dirty versions do not make
an uncoordinated flush safe. Concurrent queries do not provide cross-operation
snapshot isolation. One live writer per durable index is required.

## Loading and recovery

`load_metadata(reader)` returns `LoadState::MetadataOnly`.
`load_buckets(loader)` completes initialization; `load_all(reader, loader)`
combines both. The loader receives `BucketObject { bucket_id, generation }`
and returns `Result<Option<Vec<u8>>, BoxError>`.

An absent buckets field selects legacy loading. A present empty map denotes
a modern empty index and never scans old files. Legacy loading probes IDs
0..=max_bucket_id at generation 0; sparse missing IDs are allowed and the
watermark is capped at 1 << 20. Higher legacy IDs replace stale postings;
empty postings are tombstones. Successful legacy loading schedules metadata
persistence, so the next flush upgrades even without application mutations.

Missing manifest objects are errors, including generation 0 objects explicitly
referenced by an upgraded manifest. `load_buckets_partial` explicitly permits
missing objects for diagnostics and returns their identities. An incomplete,
failed or cancelled attempt remains `LoadState::Partial`.

The ordered key set is built once, from the loaded postings, after the bucket
objects have been read. A sorted bulk build replaces per-bucket insertion in
hash order. It also runs when loading stops on an error, so every loaded
posting stays reachable by range queries.

Only Ready accepts mutations:

| Operation on a read-only handle | Result |
|---|---|
| insert / insert_array / batch_update / flush | Error |
| remove / remove_array | false / zero |
| compaction | No-op |

Partial queries describe available postings, not complete results. Restore
the objects and retry load_buckets to discard the incomplete attempt and
rebuild a complete index. Loading an already Ready index is rejected; reopen
explicitly to discard live state.

## Persistence

Metadata preserves the `{"metadata": ...}` wrapper. Each bucket contains a
p map from FV to `(bucket_id, counter, [PK...])`. The loader takes the bucket
ID from the object it read. The counter is a per-posting update count kept by
earlier releases; it is written as 0 and ignored on load, so older releases
still decode new buckets. Named runtime types do not alter the tuple/array wire
representation; CBOR maps may have definite or indefinite lengths.

Generation 0 addresses legacy unsuffixed objects. The metadata buckets map
is the authoritative mapping from bucket IDs to object generations.

`flush_owned_with(now_ms, metadata_writer, bucket_writer)`:

1. Captures dirty bucket identities and prepares the manifest. Buckets without
   postings are left out of the manifest and never written; their previous
   objects are reported as obsolete. This includes empty objects committed by
   earlier releases, which drop out at the next flush.
2. Encodes and writes one bucket at a time without cloning postings or their
   auxiliary membership maps.
3. Invokes metadata_writer after all required bucket writes succeed.
4. On confirmed success, publishes the manifest, clears matching dirty marks
   and returns `FlushOutcome { saved, obsolete }`.

The index holds one encoded bucket buffer plus O(bucket count) metadata and
identity bookkeeping. An isolated oversized posting still needs a correspondingly
large buffer. Caller-side upload buffering adds its own memory.

A clean flush returns saved=false and calls neither writer. Before the
manifest commit, written buckets are unreachable and the previous snapshot
remains readable. Delete obsolete objects best-effort after a confirmed
commit. Coordinate garbage collection with the writer: unreferenced objects
can belong to an in-progress commit.

Generations derive from metadata versions. A failure retried without another
mutation can reuse the same generation/content; each retry does not necessarily
advance it. The writer must create/overwrite the addressed object.

Metadata success means a durable atomic replacement completed. A storage
error can have an unknown outcome after the request was sent; Err is not proof
that no commit occurred. Production adapters should reopen/recover when the
outcome is uncertain.

### Filesystem use

Use flush_owned_with. Open/write metadata only inside its callback: write a
same-directory temporary file, sync, atomically rename, and sync the directory
where supported. Make buckets durable before committing their manifest.

The runnable [example](../rs/anda_db_btree/examples/btree_demo.rs) uses
[atomic_file.rs](../rs/anda_db_btree/examples/support/atomic_file.rs). Its tests
cover no-op flush, bucket failure and failure before metadata replacement.

`flush(writer, ...)` remains useful for already-durable atomic sinks and
in-memory buffers. Write::flush cannot make an arbitrary Write atomic or
durable. Do not construct File::create on the live metadata file before calling
it: even a no-op flush would leave it truncated.

## Compaction

Bucket size is advisory. A growing posting in a shared full bucket migrates
away; a posting already alone grows in place. The soft limit does not split
one posting.

`compact_buckets_with_outcome()` returns
`CompactionOutcome { old_bucket_count, new_bucket_count, changed }`.
Deterministic best-fit-decreasing packing repairs ownership and marks rebuilt
buckets dirty. A canonical layout is a true no-op; a rebuild can retain the
same bucket count. Packing uses a fixed five-byte bucket-ID contribution, so
renumbering across CBOR integer-width boundaries cannot change the next sort
order. This is an in-memory estimate; persisted bucket encoding is unchanged.

`compact_buckets() -> (usize, usize)` is a compatibility wrapper whose counts
cannot identify every change. Use changed or pending-flush state when deciding
whether to save. AndaDB's compact_index commits changed layouts even when
counts match.

## Complexity and memory

N = indexed FVs, D = IDs in one posting, B = buckets, K = visited matches.

| Operation | Main cost, excluding callbacks |
|---|---|
| Point lookup | Average O(1) |
| Posting add/remove by ID | Amortized average O(1); occasional capacity shrinking and bounded scans for tiny vectors |
| New/last-removed FV | Additional O(log N) ordered-key change; average O(1) bucket membership insertion/removal |
| Primitive range/prefix page | O(log N + K) |
| Boolean range | Input-dependent interval algebra, then range scans; no full-index candidate materialization |
| Include | Input sort/deduplication plus lookups/interval traversal |
| Compaction | Encoded-size pass, FV sorting, O(log B) bin placement |
| Flush | Dirty data serialization and O(B) bookkeeping, one bucket buffer |
| Load | Read/decode referenced objects, rebuild posting and ordered-key structures |

FVs exist in the posting map, ordered set and bucket membership hash set:
long keys have three owned copies. Bucket membership has no insertion-order
contract; queries use the global ordered key set. Larger postings have a boxed
auxiliary PK map, so small postings only reserve one optional pointer for it.
Hash-table capacity remains a material memory cost; bucket iteration during
flush can scan unused capacity after extensive deletions.

Range and prefix queries go directly to their lookup/iterator instead of
checking every posting shard for emptiness. Standalone Include sorts and
deduplicates its owned vector in place. Flush moves the prepared manifest into
committed metadata without cloning it again.

Run `ANDA_BTREE_RUN_BENCH=1 cargo bench -p anda_db_btree --bench workloads`.
The explicit opt-in keeps harness-free benchmarks out of release-mode
`cargo test --all-targets` runs. The harness reports
median/p95/p99, throughput, allocation counts/bytes, peak extra live heap and
serialized bytes. It covers high/low cardinality, large posting deletion,
first/last pages, multi-threaded reads/writes and 4 KiB keys. Flush fixtures
isolate one posting per bucket and assert exactly 10% or 100% dirty buckets,
with optional injected per-bucket I/O delay. Set
`ANDA_BTREE_BENCH_NO_IO_DELAY=1` for alternating CPU-only comparisons.
Additional cases compare direct point lookup with Eq/Ge query paths, Include
sizes, retained heap after deleting 99,995 of 100,000 IDs, and repeated
compaction plus flush at 32 and 300 buckets.

Both versions must use the same instrumented allocator and build configuration.
See [maintenance results](anda_db_btree-maintenance.md) for measured comparisons,
the completed checklist and decisions about further optimizations.
