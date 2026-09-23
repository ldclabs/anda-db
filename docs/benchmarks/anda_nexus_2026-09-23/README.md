# Cognitive Nexus review benchmarks — 2026-09-23

Baseline: `b77dd10`. The same [driver](driver.rs) compares the repaired runtime
with the baseline library in standalone Cargo release builds (`opt-level=3`,
no workspace size profile). See [environment.json](environment.json).

Each process creates an in-memory database, activates the memory profile, and
seeds 1,000 or 10,000 Concepts, Activities and Evidence rows. One percent of
Concepts are Person; the others are Preference. Version and transaction rows
are seeded directly outside timing to isolate append and paging cost. The
measured Skill mutation goes through ordinary KML validation and durable redo.
These synthetic histories are performance inputs, not a complete application
history or a measure of production throughput.

Each workload has one warm-up and five measured iterations. Three process
pairs run sequentially in alternating order; no build or test from this task
runs concurrently. The table is the median of the three per-process means.
Times include normal authorization, rendering and storage work in each selected
API. They do not include fixture construction. Raw observations are retained
in `before-1.jsonl` through `after-3.jsonl`.

| Records/history size | Workload | Before ms | After ms | Before / after |
| ---: | --- | ---: | ---: | ---: |
| 1,000 | Append one element version | 10.481 | 0.049 | 213.72× |
| 1,000 | Read 100 change envelopes | 4.075 | 0.755 | 5.40× |
| 1,000 | Typed keyword search | 26.248 | 14.471 | 1.81× |
| 1,000 | Create Skill + SkillRevision | 14.035 | 2.691 | 5.21× |
| 10,000 | Append one element version | 109.761 | 0.053 | 2065.11× |
| 10,000 | Read 100 change envelopes | 41.868 | 0.735 | 56.95× |
| 10,000 | Typed keyword search | 281.490 | 139.484 | 2.02× |
| 10,000 | Create Skill + SkillRevision | 139.443 | 12.902 | 10.81× |

Changes behind the results:

- Version deduplication intersects the existing transaction/element indexes,
  instead of decoding all previous versions of the element.
- CHANGES reads a bounded page in logical sequence order, borrowing index
  postings rather than materializing every pending journal row.
- Search narrows candidates with available type/predicate indexes while
  retaining authorization and field masking before scoring. Lineage ranges may
  still include other symbols in the same package; this is not a fully indexed
  search or a claim that authorized BM25 construction is constant-time.
- Ordinary Skill creation avoids historical Activity/Evidence scans needed
  only by attempt, outcome and evaluation validation. Outcome/evaluation writes
  still have their complete accounting checks; those workloads are not measured
  here and are not claimed to have become constant-time.

The repair also bounds intermediate join work, uses an Activity output index
for dependency producers and caches historical reconstruction within a query.
Those improvements have regression coverage but are not timed by this driver.

To reproduce, copy `driver.rs` into two temporary standalone Cargo projects,
using edition 2024 and path dependencies on each checkout's `anda_cognitive_nexus`,
`anda_db` and `anda_kip`, plus `object_store = "0.14"`, `serde_json = "1"`, and
`tokio = { version = "1", features = ["full"] }`. Use the same Cargo.lock,
build each with `cargo build --release`, then run the binaries sequentially in
the order recorded in the environment file. Do not mix workspace `opt-level=z`
with the standalone profile when comparing the results.
