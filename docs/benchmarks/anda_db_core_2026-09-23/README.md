# Collection review measurements — 2026-09-23

The baseline is `a9d102c` with the same expanded benchmark source as this change.
Both executables use the workspace release profile (`opt-level=z`), Rust 1.98.1,
and the tracking allocator in `core_workloads.rs`. See [environment.json](environment.json).
Three alternating before/after trials ran without concurrent compilation or tests.
All numbers below are medians across the three trials; latency rows use each
trial's p50, and peak heap rows use its maximum additional tracked heap.

| Workload / measurement | Before | After |
| --- | ---: | ---: |
| Dense indexed equality + ID page, 100,000 documents, 20 results | 9.939 ms | 3.658 ms |
| Same page, additional peak heap | 2,762,184 B | 1,176 B |
| Unchanged text/vector update + flush | 49.834 µs | 5.375 µs |
| 30 unchanged updates + flushes, PUT count | 272 | 0 |
| Changed body update + flush, 100,000-document collection | 17.958 µs | 13.416 µs |
| 30 changed updates + flushes, PUT count | 122 | 92 |
| Recover 128 different updated documents, 5 ms injected operation delay | 1,147.834 ms | 383.431 ms |

Dense pages now scan one equality posting with a bounded result set. This still
visits the posting; it is not constant-time pagination, and general AND filters
retain their existing memory behavior. The ID bitmap supplies membership tests.
Unchanged updates skip persistence and index rebuilding. Changed updates retain
their durable intents but skip rewriting the unchanged collection ID bitmap.
The PUT totals vary slightly with wall-clock rate limiting of storage metadata;
the removed bitmap write is one PUT per changed-update checkpoint.

Recovery loads the same 264 objects in both variants. The changed implementation
prefetches current document bodies (default concurrency 8) and applies repairs
in order. PUTs change from 3 to 2; both runs delete 128 intents and list once.
The injected delay also applies to writes/deletes; this is a repeatable latency
model, not a measurement of S3 or a local filesystem.

There is no blanket tail-latency claim: the changed-update p95 median was
20.333 µs before and 21.250 µs after, and its measured peak heap remained 4,951 B.
The global tracking allocator includes other runtime allocations. Production
capacity planning should use representative documents and the real backend.

Raw trials: [before 1](before-1.jsonl), [after 1](after-1.jsonl),
[before 2](before-2.jsonl), [after 2](after-2.jsonl),
[before 3](before-3.jsonl), [after 3](after-3.jsonl).

Run from the repository root:

```bash
ANDA_BENCH_REVIEW=1 cargo bench -p anda_db --bench core_workloads
# Optional scale overrides; keep these identical when comparing executables.
ANDA_BENCH_REVIEW=1 ANDA_BENCH_DOCS=100000 ANDA_BENCH_ITERATIONS=30 \
  ANDA_BENCH_RECOVERY_DOCS=1000 cargo bench -p anda_db --bench core_workloads
```

For a historical comparison, compile the same benchmark source against each
library revision and save each executable outside `target` before rebuilding.
Seeding is outside the measured phases; query/mutation timings include result
checks, while recovery verification follows the timer. The default review mode
uses 128 distinct recovery documents; the existing `ANDA_BENCH_IO` mode continues
to cover repeated updates to one document.

## 中文说明

本次采用相同基准代码，对修改前后各执行三轮交替测量，运行时没有并发编译或测试。
上表为三轮中位数：10 万条同 owner 数据的分页耗时降低约 63%，额外峰值堆内存从
约 2.76 MB 降至 1.18 KB；30 次同值更新不再产生 PUT。普通更新的检查点每次减少
一次 ID 位图写入；128 个不同文档的恢复在每次操作模拟 5 ms 延迟时缩短约 67%。

分页仍需扫描 posting，复杂交集仍使用原路径。普通更新的 p95 没有改善，峰值堆内存
也未变化。存储元数据的时间限流会使 PUT 总数略有波动。恢复基准使用的是可复现的
延迟模型，并非实际云存储吞吐测试；原始结果与运行环境均见上方链接。
