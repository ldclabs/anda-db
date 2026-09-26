# Million-row query measurements — 2026-09-26

Baseline: `db8608e`. Both executables use the same `core_workloads` query-scale
harness and Cargo.lock, built into separate target directories with
`release-speed`, `opt-level=3`, LTO disabled and 16 codegen units. See
[environment.json](environment.json) and [source hashes](source_hashes.json).

Each process seeds a real InMemory Collection before timing, then runs two
warmups and 15 samples per bounded query (five for complete ID collection).
Two pairs run in alternating before/after and after/before order at each size.
No compilation or test suite runs during these timed pairs. CPU cores were not
exclusively reserved. Timings include allocation-tracking overhead; these are
index-query microbenchmarks, not HTTP, cloud-storage or production QPS claims.

The table reports the median of the two process p50 values, in milliseconds.

| 1,000,001 documents | Before ms | After ms |
| --- | ---: | ---: |
| Rare key intersected with two dense keys, 10 results | 2.316855 | 0.001209 |
| Two dense keys, first 50 IDs | 32.798230 | 0.002313 |
| Two dense keys and a deep ID cursor, 50 IDs | 19.952125 | 0.002417 |
| Complete ID set | 33.671771 | 28.923333 |
| Empty intersection | 0.000458 | 0.000542 |

The empty case was already sub-microsecond and shows no improvement; its
absolute difference is about 0.08 µs. The explicit empty-set exit avoids
depending on compiler elimination of useless membership loops.

First dense-page preparation is **not free**: its first-call median is
34.974 ms before and 1.010 ms after. The after path retains an immutable sorted
posting for subsequent pages and invalidates it on mutation. Log fields named
`*_cold_posting` mean the first call of each workload in that process, not a
freshly opened index for each case; the deep-page workload reuses the ordering
prepared by the first-page workload.

Maximum additional tracked heap across the two million-row processes:

| Workload | Before bytes | After bytes |
| --- | ---: | ---: |
| Dense first page | 46,137,672 | 916 |
| Dense deep page | 46,137,771 | 1,047 |
| Complete ID set | 46,137,592 | 46,137,592 |

These are incremental warm-query allocations, not total RSS. In particular,
the retained million-ID ordering is roughly 8 MB and is outside the warm-query
allocation delta. General complete intersections still materialize their
working sets. Removing the duplicate document-ID tree reduces resident ID
bookkeeping, but no process-RSS reduction is claimed by these measurements.

An initial implementation reused the per-ID paging plan for unlimited results
and regressed complete collection to roughly 95 ms. It was corrected to use
bulk intersection for that case. The initial after logs and
[initial source hashes](initial-source_hashes.json) are retained rather than
discarded. The tables above use only the final `before-*` / `after-*` logs.

## Nexus current-row probe

[nexus-million.jsonl](nexus-million.jsonl) records queries through the real
parser and executor over 1,000,001 current Concept rows. Filler rows are copied
from a schema-valid KML Event template; their full historical logs are not
constructed. This probe checks results and logical Concept GETs, not latency.

| Query | Result | Concept GETs |
| --- | --- | ---: |
| Rare Person type | The one Person | 1 |
| Name FILTER | The one Person | 1 |
| Previously bound element | The one Person | 1 |
| COUNT of all Concepts | 1,000,001 | 0 |
| Reverse STRUCTURAL target | The one Experience | 2 |
| First page, limit 2 | C-1, C-2 | 4 |

GET counts include lookahead and reference authorization reads. They are
Collection calls, not remote object-store requests. The first-page result
therefore does not imply only two total reads.

## Reproduction

From the repository root, with fixture construction outside the timed phases:

```sh
CARGO_PROFILE_RELEASE_SPEED_LTO=false CARGO_PROFILE_RELEASE_SPEED_CODEGEN_UNITS=16 \
  ANDA_BENCH_QUERY_SCALE=1 ANDA_BENCH_DOCS=1000001 ANDA_BENCH_ITERATIONS=15 \
  cargo bench -p anda_db --bench core_workloads --profile release-speed

ANDA_NEXUS_BENCH=1 ANDA_BENCH_DOCS=1000001 \
  cargo bench -p anda_cognitive_nexus --bench query_scale --profile release-speed
```

A later paired [rerun](../anda_query_scale_2026-09-26-rerun/README.md)
compares `83d6427` with `4afaef1` using this harness and finds no measurable
change.

For a paired comparison, copy this commit's `core_workloads.rs` into a baseline
checkout, copy the same Cargo.lock, build both with the same flags in separate
targets, and save the executable paths from Cargo JSON output. Run them only
after builds/tests finish. Results for 100,000 and 1,000,001 rows, both trials,
are included alongside [summary.json](summary.json).
