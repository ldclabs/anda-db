# AndaDB 核心修复性能验证（2026-09-06）

本次为同机、同工作负载的方向性验证；不把单个微基准的倍数当作整个数据库的加速倍数。默认构建保持 `opt-level=z, lto=true`，另测 `release-speed` 的 `opt-level=3`。

环境：Rust 1.97.1，arm64，macOS 26.6.2。主内存数据集 10,000 条、查询 100 次；本地文件数据集 1,000 条、查询 20 次。文件后端使用 LocalFileSystem + MetaStore，开启 fsync。模拟网络延迟通过 InMemory 包装器施加固定等待。

**默认构建：修复前后**

| 工作负载 | 修复前 | 修复后 | 说明 |
| --- | ---: | ---: | --- |
| OR 分页 p50 | 0.608375 ms | 0.001167 ms | 521.32× |
| AND 分页 p50 | 0.248833 ms | 0.012125 ms | 20.52× |
| 单路 BM25 p50 | 0.895375 ms | 0.935333 ms | 本次未加速（4.5% 增加） |
| 选择性 BM25 p50 | 0.902833 ms | 0.401792 ms | 2.25× |
| 1.1 MB 串行 codec p50 | 0.304000 ms | 0.328333 ms | 本次未加速（8.0% 增加） |
| 文件后端，100 次更新的额外 Rust 堆 | 19.513 MiB | 0.243 MiB | 减少 98.8% |
| InMemory，100 次更新的额外 Rust 堆 | 38.772 MiB | 19.599 MiB | 后端本身仍在内存保留日志对象 |
| 64,000 次热读 + 1,000 次冷写的额外 GET | 273 | 20 | 减少 92.7%；条带由 256 增至 4096 |
| 两个 codec 写入任务下，1 ms 定时器最大实际间隔 | 46.595 ms | 2.913 ms | 反映该负载下的调度阻塞；不是生产请求 p99 |
| 请求 10 个满足 owner 条件的结果 | 1 | 10 | 该数据集有足够匹配，子集精算补足结果 |

内存数据由 Rust 全局分配器跟踪，包含 Rust 分配，非进程 RSS；不涵盖 C 库内部 malloc。日志磁盘格式仍保留前后映像，内存句柄改为只记录待清理序号。InMemory 后端的日志对象也计入堆，因此这里的降幅比文件后端小。

**恢复 I/O：100 条待提交意图**

| 注入的单次存储延迟 | 修复前 | 修复后 | 比值 |
| --- | ---: | ---: | ---: |
| 1 ms | 0.782 s | 0.112 s | 7.01× |
| 10 ms | 3.633 s | 0.579 s | 6.28× |
| 50 ms | 14.617 s | 2.379 s | 6.14× |

每次运行的请求数均为 **172 GET、3 PUT、100 DELETE、1 LIST**。收益来自有界并发，而非少读数据或提前丢弃日志。实际 timer 等待包含平台调度开销，不能把注入的 1 ms 当作实测请求耗时。

**可选速度构建**

| 10,000 条内存数据 | 修复后默认 z | release-speed（3） |
| --- | ---: | ---: |
| bounded_or p50 | 0.001167 ms | 0.000667 ms |
| bounded_and p50 | 0.012125 ms | 0.005667 ms |
| single_bm25 p50 | 0.935333 ms | 0.473208 ms |
| selective_bm25 p50 | 0.401792 ms | 0.166000 ms |
| reopen p50 | 7.303166 ms | 3.006959 ms |
| large_codec p50 | 0.328333 ms | 0.253875 ms |

此基准可执行文件大小约 2.90 MiB → 4.79 MiB（增加 65.3%）。这是基准二进制的大小，不是库的通用体积。默认发布配置保持不变，吞吐敏感部署可自行选择 `release-speed`。

**没有隐藏的退化与取舍**

- 单路 BM25 在默认 z 构建下未显示显著收益；移除多余 RRF/去重主要减少中间结构，主成本仍在底层评分。速度构建在这组数据上更有效。
- 文件后端清理 100 条日志，本次约 164 ms → 178 ms；有界并发在该后端的同步/元数据开销下未缩短时间。没有把模拟网络的加速倍数套到本地文件。
- 大 codec 的单次延迟有线程切换成本；有界阻塞池改善了并发负载下定时器的可调度性。本轮只 offload 独立拥有的 codec 缓冲，不把持有借用的 Collection/index 修改丢到后台。
- 保留完整日志前后映像用于重放和旧 schema 历史恢复。差量/仅索引映像需要额外格式与恢复协议；本轮用独立日志大小预算和序号集合解决合法大文档及内存放大问题。
- WAL 清理仍在生命周期排他租约内，以保证 close/delete/recreate 会等待旧清理结束；通过有界并发降低阻塞。没有引入可能跨越集合重建的后台删除任务。
- 广泛的非主键交集仍可能需要 O(匹配数) 内存。已优化 OR、主键布尔扫描、选择率排序和候选集内 NOT，未声称任意表达式都只使用 O(limit) 内存。
- 预过滤让 RRF 在匹配子集内排名。需要原全局排名再过滤的语义，可设置 `prefilter_limit: 0, adaptive: false`。

定时器测量使用 harness v3：计时从放行 CPU 工作前开始，覆盖整个工作期间，防止完全饥饿的定时器产生零样本。原先仅测初始 yield 突发的结果保存在 `scheduler_initial.jsonl`，不用于上述结论。

**复跑**

```bash
cargo bench -p anda_db --features full --bench core_workloads
ANDA_BENCH_IO=1 cargo bench -p anda_db --features full --bench core_workloads
ANDA_BENCH_CACHE=1 cargo bench -p anda_db --features full --bench core_workloads
ANDA_BENCH_SCHEDULER=1 cargo bench -p anda_db --features full --bench core_workloads
ANDA_BENCH_BACKEND=fs ANDA_BENCH_DOCS=1000 ANDA_BENCH_ITERATIONS=20 cargo bench -p anda_db --features full --bench core_workloads
cargo bench -p anda_db --features full --bench core_workloads --profile release-speed
```

基线使用本轮开始时的 `anda_db` 源码副本，并与修复版使用同一套基准程序和工作区底层依赖。原始逐项数据见 `results.jsonl`、各阶段 `.log` 和 `environment.json`；源码摘要见 `source_fingerprints.json`。没有声称进行过真实云端或物理断电验证。
