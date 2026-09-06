# Anda libraries: local before/after measurements

测量日期：2026-09-06。平台 arm64 macOS，rustc 1.97.1。旧实现来自本任务开始前保存的两个子库源码；新实现对应 `source_hashes.json` 记录的 `e21d9d2` 快照。该提交复审后的 multipart 正确性修复不在这组性能数字中。使用**不同的全新构建目录**，从 Cargo JSON 读取各自生成的可执行文件。两边共用同一份基准源码，依赖版本与工作区 Cargo.lock 一致。

配置：CARGO_PROFILE_BENCH_OPT_LEVEL=3、CARGO_PROFILE_BENCH_LTO=false；每项先预热 2 次，再测 15 次。延迟包含同一计数分配器的开销；p95 使用 15 个样本的 nearest-rank。这里只是本地微基准，未隔离 CPU、未模拟真实云网络，不代表生产端到端加速。

allocations 是分配/重分配次数；allocated_bytes 是累计申请字节（含 realloc 的新请求大小），不是净存活内存；largest_allocation 是最大单次分配。进程峰值 RSS 用 Python resource.getrusage(RUSAGE_CHILDREN) 读取，单位为 macOS 返回的字节。每个二进制由独立测量进程运行。

普通 MetaStore 在 InMemory 上可直接共享 Bytes，所以取消全量 payload 哈希会出现很大的相对加速；不能把这一倍数外推到网络或磁盘写入。首段解密现在最多聚合 64 KiB 的小加密块，旧实现每次输出一个小块；首段延迟对应的输出大小不同，主要用其最大分配量评估内存改善。

工作负载：UniqueVec 使用 100,000 个值，分别为全唯一、50% 重复和 99.9% 重复；元数据压力读取使用 4 MiB 对象、1 KiB 分块（4,096 个 tags），明显小于默认 256 KiB 分块。

关键结果：加密热 head 和单字节范围读取不再反复认证全部 tags；16 MiB 加密 put 与不对齐 multipart 省掉全量 SHA3；交错范围读取减少重复下载/解密。GC 在有网络延迟时的收益应看请求计数：单 key 的 10 个垃圾 generation，元数据 GET 从 11 次减为 2 次；该请求数量已经由回归测试验证。

取舍：新输入校验增加冷元数据读取成本；碰撞保护对小文件写入有成本；列表保持更强的结构检查。UniqueVec 流式构建显著降低重复输入的累计分配，但全唯一 String、部分 From<Vec> 场景会变慢，且整个 utils 基准进程的峰值 RSS 上升。保留了这些原始结果，没有把所有路径描述为加速。构建器的抽样预留每个容器最多按 1 MiB 元素量计算，优先避免受原始输入长度控制的大量预分配。

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

整个 utils 基准进程峰值 RSS：38,617,088 → 45,350,912 字节。该峰值覆盖整个基准矩阵，不能等同于单个对象或单次操作的内存。

## Object stores

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

整个 store 基准进程峰值 RSS：137,691,136 → 132,481,024 字节。该峰值覆盖整个基准矩阵，不能等同于单个对象或单次操作的内存。

## Reproduce

```bash
CARGO_PROFILE_BENCH_LTO=false CARGO_PROFILE_BENCH_OPT_LEVEL=3 cargo bench -p anda_db_utils --bench unique_vec
CARGO_PROFILE_BENCH_LTO=false CARGO_PROFILE_BENCH_OPT_LEVEL=3 cargo bench -p anda_object_store --bench storage
```

做前后对照时，必须为两个源码版本指定不同的 target-dir，并选择 Cargo 实际报告的可执行文件；不要复用旧文件名。CSV 包含全部 p95、分配计数及最大分配，resources 文件保存进程资源数据。
