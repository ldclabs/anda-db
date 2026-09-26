# Million-row query rerun — 2026-09-26

This rerun compares `83d6427` (before) with `4afaef1` (after), the follow-up
that re-keyed SEARCH caches by Space sequence and simplified paging/cursor
plumbing. It uses the same harnesses, Cargo.lock, compiler and build flags as
the [first measurement](../anda_query_scale_2026-09-26/README.md). See
[environment.json](environment.json) for hashes and settings.

Each version was exported with `git archive`, built into its own target
directory with `release-speed`, `opt-level=3`, LTO disabled and 16 codegen
units, and run only after both builds finished and the 1-minute load average
fell below 6. At each size, trial 1 ran before then after and trial 2 ran
after then before. CPU cores were not exclusively reserved.

**Result: no measurable change.** Every workload stays within ±4% of the
baseline, inside the spread between the two trials of one version. Tracked
heap allocations are byte-for-byte identical. Both Nexus probes return the
same answers with the same Concept GET counts.

## Core `core_workloads` query scale

Median of the two process p50 values, in milliseconds.

| 1,000,001 documents | Before ms | After ms | After / before |
| --- | ---: | ---: | ---: |
| Rare key intersected with two dense keys, 10 results | 0.001230 | 0.001250 | 1.02 |
| Empty intersection | 0.000541 | 0.000521 | 0.96 |
| Two dense keys, first 50 IDs | 0.002292 | 0.002292 | 1.00 |
| Two dense keys and a deep ID cursor, 50 IDs | 0.002437 | 0.002417 | 0.99 |
| Complete ID set | 30.863562 | 30.274187 | 0.98 |

| 100,000 documents | Before ms | After ms | After / before |
| --- | ---: | ---: | ---: |
| Rare key intersected with two dense keys, 10 results | 0.001271 | 0.001251 | 0.98 |
| Empty intersection | 0.000541 | 0.000562 | 1.04 |
| Two dense keys, first 50 IDs | 0.002249 | 0.002312 | 1.03 |
| Two dense keys and a deep ID cursor, 50 IDs | 0.002396 | 0.002437 | 1.02 |
| Complete ID set | 2.256541 | 2.353104 | 1.04 |

The 100,000-row complete-set difference is within trial noise: the after
trials measured 2.27 and 2.43 ms, the before trials 2.25 and 2.26 ms. At
1,000,001 rows the before trials spread from 29.5 to 32.2 ms.

The first dense page still prepares a sorted posting: at 1,000,001 rows the
first-call median is 1.188 ms before and 1.065 ms after. Maximum additional
tracked heap is unchanged in every workload, for example 948 bytes for the
first page and 46,137,592 bytes for the complete million-row ID set.

These numbers agree with the first measurement's optimized column, for
example 0.0023 ms for the first page and about 29–31 ms for the complete set.

## Nexus `query_scale` probe

Both versions seed 1,000,001 current Concept rows and pass every assertion.

| Query | Result | Concept GETs before | Concept GETs after |
| --- | --- | ---: | ---: |
| Rare Person type | The one Person | 1 | 1 |
| Name FILTER | The one Person | 1 | 1 |
| Previously bound element | The one Person | 1 | 1 |
| COUNT of all Concepts | 1,000,001 | 0 | 0 |
| Reverse STRUCTURAL target | The one Experience | 2 | 2 |
| First page, limit 2 | C-1, C-2 | 4 | 4 |

Maximum resident set size was 1,029,881,856 bytes before and 1,033,404,416
bytes after. Process time, 14.24 s and 13.98 s, is dominated by seeding and is
not a query latency.

## Not covered

Neither harness exercises the SEARCH corpus or prepared-scope caches that
`4afaef1` re-keyed. That change is covered by the regression test
`indexed_search_scopes_survive_writes_in_other_spaces` in
`rs/anda_cognitive_nexus/tests/meta.rs`, not by these measurements.

## Files

- `core-{before,after}-{100000,1000001}-{1,2}.jsonl`: raw Core output per
  version, size and trial.
- `nexus-{before,after}.jsonl` and `nexus-{before,after}.time.txt`: Nexus
  probe output and `/usr/bin/time -l` resource usage.
- [summary.json](summary.json): per-trial values and the medians above.

## Reproduction

Build each commit into a separate target directory, then run the executables
serially with the environment the first measurement documents:

```sh
CARGO_PROFILE_RELEASE_SPEED_LTO=false CARGO_PROFILE_RELEASE_SPEED_CODEGEN_UNITS=16 \
  CARGO_TARGET_DIR=<target> cargo bench --locked -p anda_db --bench core_workloads \
  -p anda_cognitive_nexus --bench query_scale --profile release-speed --no-run \
  --message-format=json

ANDA_BENCH_QUERY_SCALE=1 ANDA_BENCH_DOCS=1000001 ANDA_BENCH_ITERATIONS=15 \
  <core_workloads executable> --bench

ANDA_NEXUS_BENCH=1 ANDA_BENCH_DOCS=1000001 <query_scale executable> --bench
```
