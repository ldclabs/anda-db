# Anda Libraries: Local Before/After Benchmark Measurements (2026-09-06)

[中文版](README.zh.md)

Measurement date: 2026-09-06. Platform: arm64 macOS, rustc 1.97.1. The legacy implementation is sourced from the pre-task snapshot; the new implementation reflects commit `e21d9d2` in `source_hashes.json` (post-review multipart correctness fixes are not part of these performance numbers). Built into **distinct, clean target directories**, reading binary paths directly from Cargo JSON output. Both runs share the identical benchmark harness and Cargo.lock dependency tree.

Configuration: `CARGO_PROFILE_BENCH_OPT_LEVEL=3`, `CARGO_PROFILE_BENCH_LTO=false`. Each case undergoes 2 warmups and 15 measured iterations. Reported latencies include allocation-tracking overhead; p95 uses nearest-rank across 15 samples. These local microbenchmarks do not isolate CPU cores or simulate production cloud network topologies and should not be interpreted as end-to-end production speedups.

`allocations` represents allocation/reallocation count; `allocated_bytes` represents cumulative requested bytes (including realloc size deltas), not net surviving heap; `largest_allocation` is the single largest allocation request. Peak process RSS was captured via Python `resource.getrusage(RUSAGE_CHILDREN)` in macOS bytes. Each binary was executed in an isolated child process.

MetaStore on InMemory shares underlying `Bytes` directly, making the elimination of full-payload hashing appear disproportionately fast; this factor cannot be extrapolated to network or disk writes. Initial stream decryption now coalesces small cipher chunks up to 64 KiB (legacy emitted one chunk at a time); output sizes differ slightly, with maximum allocation serving as the primary metric for memory improvement.

Workload: `UniqueVec` evaluated 100,000 values across fully unique, 50% duplicate, and 99.9% duplicate distributions. Metadata stress read used 4 MiB objects with 1 KiB chunks (4,096 tags), substantially smaller than the 256 KiB default.

Key findings: Authenticated hot `head` and single-byte range reads eliminate redundant GMAC tag re-authentication; 16 MiB encrypted `put` and unaligned multipart operations eliminate full SHA-3 passes; interleaved range reads reduce redundant downloads and decryptions. GC benefits under network latency should be evaluated by request count: for 10 orphaned generations of a single key, metadata GETs dropped from 11 to 2 (verified via regression tests).

Tradeoffs: Strict input validation introduces cold metadata read overhead; collision guards add latency to small file writes; listing operations perform stricter structural checks. Streaming `UniqueVec` construction reduces cumulative allocations on duplicate inputs, but fully unique strings and select `From<Vec>` paths show regressions, increasing overall utils benchmark process RSS. All raw numbers are retained without selective filtering. Builder sampling caps pre-allocation reservations at 1 MiB element equivalents to avoid uncontrolled allocations based on raw input length.

## UniqueVec

| Case | Before median µs | After median µs | Before/after | Before/after allocated bytes |
| --- | ---: | ---: | ---: | ---: |
| u64/unique/from_iter | 460.833 | 461.292 | 1.00× | 1,979,656 / 1,992,964 |
| u64/unique/from_vec | 419.375 | 528.208 | 0.79× | 1,979,656 / 1,988,900 |
| string/unique | 10446.834 | 13068.792 | 0.80× | 12,476,808 / 19,135,900 |
| serde/unique | 12709.542 | 12962.542 | 0.98× | 16,368,168 / 19,644,988 |
| u64/half/from_iter | 320.917 | 326.334 | 0.98× | 1,979,656 / 1,992,964 |
| u64/half/from_vec | 320.375 | 411.791 | 0.78× | 1,979,656 / 1,988,900 |
| string/half | 10020.500 | 6990.208 | 1.43× | 12,476,808 / 9,940,276 |
| serde/half | 11027.417 | 8991.209 | 1.23× | 16,368,168 / 11,522,452 |
| u64/duplicate/from_iter | 162.375 | 158.167 | 1.03× | 1,979,656 / 4,332 |
| u64/duplicate/from_vec | 161.250 | 294.833 | 0.55× | 1,979,656 / 803,116 |
| string/duplicate | 6432.792 | 3718.625 | 1.73× | 12,476,808 / 3,415,796 |
| serde/duplicate | 8335.083 | 5819.125 | 1.43× | 16,368,168 / 3,415,796 |

Peak process RSS across utils benchmark: 38,617,088 → 45,350,912 bytes. This covers the entire benchmark suite and does not reflect single-object memory footprint.

## Object Stores

| Case | Before median µs | After median µs | Before/after | Before/after allocated bytes |
| --- | ---: | ---: | ---: | ---: |
| meta/put/4096 | 11.542 | 7.708 | 1.50× | 9,639 / 11,317 |
| encrypted/put/4096 | 12.416 | 8.917 | 1.39× | 15,164 / 16,357 |
| meta/put/1048576 | 1243.875 | 5.875 | 211.72× | 9,645 / 11,321 |
| encrypted/put/1048576 | 1573.458 | 324.167 | 4.85× | 1,060,358 / 1,061,255 |
| meta/put/16777216 | 20007.916 | 5.583 | 3583.72× | 9,966 / 11,321 |
| encrypted/put/16777216 | 24883.500 | 5249.125 | 4.74× | 16,801,377 / 16,798,177 |
| encrypted/head/hot | 25.292 | 1.125 | 22.48× | 322,929 / 3,626 |
| encrypted/range/hot | 25.708 | 1.667 | 15.42× | 324,045 / 4,674 |
| encrypted/head/cold | 89.625 | 98.583 | 0.91× | 389,959 / 239,169 |
| encrypted/ranges/interleaved | 69.333 | 8.167 | 8.49× | 410,306 / 34,791 |
| encrypted/read/full | 1733.916 | 1701.959 | 1.02× | 8,924,890 / 8,392,555 |
| encrypted/stream/first_chunk | 97.750 | 26.500 | 3.69× | 4,517,325 / 69,162 |
| encrypted/multipart/unaligned | 26214.500 | 5805.167 | 4.52× | 34,239,490 / 34,241,176 |
| local/put/fsync=false | 796.041 | 827.291 | 0.96× | 16,041 / 19,498 |
| local/put/fsync=true | 23170.916 | 20941.750 | 1.11× | 16,362 / 19,819 |
| gc/100_keys_1000_orphans | 1711.166 | 1676.125 | 1.02× | 1,289,654 / 1,318,375 |
| list/warm/100_keys | 46.208 | 68.459 | 0.67× | 182,097 / 178,736 |

Peak process RSS across store benchmark: 137,691,136 → 132,481,024 bytes. This covers the entire benchmark suite and does not reflect single-object memory footprint.

## Reproduction

```bash
CARGO_PROFILE_BENCH_LTO=false CARGO_PROFILE_BENCH_OPT_LEVEL=3 cargo bench -p anda_db_utils --bench unique_vec
CARGO_PROFILE_BENCH_LTO=false CARGO_PROFILE_BENCH_OPT_LEVEL=3 cargo bench -p anda_object_store --bench storage
```

Comparative runs must target separate `--target-dir` folders and execute the specific binary path emitted by Cargo JSON. Full p95 distributions, allocation counts, and largest allocations are captured in CSV files; process RSS is recorded in accompanying resources files.
