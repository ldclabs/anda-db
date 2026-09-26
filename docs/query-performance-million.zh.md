# 百万级查询优化：实现与验证

本次基线为 `db8608e`。优化版本与本文一起提交。目标是减少小结果查询的索引访问、文档读取和中间分配，并给广查询明确的资源边界。

| 清单方向 | 已实现的路径 | 适用条件与边界 |
| --- | --- | --- |
| 小候选求交 | BTree membership/intersection API；空候选立即停止 | 范围查询仍可能访问多个不同键，不能把键空间访问算作常数 |
| lineage 过滤 | 先在 distinct index keys 上解析精确 symbol；保留 draft promotion | 历史读取不误用当前索引 |
| 绑定/FILTER 下推 | 元素 ID、引用字段已有绑定；相邻合取 FILTER 的安全文本等值条件（NFC 查询键） | FILTER 仍在授权视图上执行；不跨越可能报类型错误的表达式；受限权限保留通用路径 |
| LIMIT/分页 | 单一覆盖元素模式可提前取页；Store 内记录 ID seek；通用排序使用所需前缀 | 无历史/世界时间、无聚合排序且权限不受限且无授权时间条件才使用覆盖分页；快照变动后继续历史语义 |
| NOT/OPTIONAL | 简单终端模式在一条完整匹配后停止；按相关绑定复用内层结果 | 复合 NOT 保留完整求值，不能在只满足部分条件时提前结束 |
| 底层分页 | 等值 posting 的不可变有序快照；多等值条件和 ID 范围直接求交 | 首次排序及更新后的重建仍为 O(posting size)；热页可 seek |
| SEARCH top-k | 按协议的分数/字符串 ID 顺序选择 top-k；复用 prepared scope | 保持同分边界与 Scope 内 DF、平均长度语义 |
| 执行观测 | `QueryStats`：posting/bitmap 访问、membership 探测、快照基数、中间 ID、返回数 | 另有 Context::work 的候选/解预算计数；这些不冒充完整 CPU、I/O 或分配追踪 |
| STRUCTURAL | 可选派生键索引，包含 Core/Profile 字段存在性与反向 target | 仍读取授权视图验证；有序边位置由原始字段决定 |
| 历史读取 | 有序版本定位；重建时选择小历史范围或跳过完整旧版本链；并发读取选中版本 | 广快照仍受预算限制；没有新增持久化 checkpoint 或通用 MVCC |
| 受限权限搜索 | 缓存授权、脱敏后的搜索文本索引与约束；返回视图重新加载 | 数据/Space/schema/完整有效授权变动即换 key；冷构建仍受候选预算限制 |
| BELIEF | 查询内缓存同一 frame 各 Proposition 的投影 | cache key 包含完整 Frame、policy、时间、快照和 Schema；归档目标单独处理，事务输出改变时清空 |
| Hybrid/ANN | 更多范围/Include 选择率提示；自适应扩展复用一次 BM25 排名 | 向量精确子集和候选上限不变；未改变 HNSW 算法或 embedding 模型 |
| 加载/聚合 | 8 路有界预取、分块让出执行器；分组保存行索引；COUNT 避免整文档渲染；覆盖 COUNT | DISTINCT、权限、未绑定值语义保留；一般大 JOIN 仍有预算 |
| 并发调度 | 广索引扫描和排名进入有界 blocking pool | 保留 Nexus 一致性读写锁；未引入跨 Space 锁拆分 |
| 内存与冷打开 | 去掉文档 ID 的重复 BTreeSet，使用既有 bitmap rank/select；按需准备 posting 顺序 | BTree/BM25/HNSW 仍为内存索引；没有引入磁盘 ANN 或惰性 bucket 格式 |

## 运行与迁移

- Concept/Proposition/Assertion schema 升至 1，Evidence/Activity 升至 2，版本日志升至 1；新增列均可选。旧字段索引编号由 Schema 升级保留。
- `query_keys` 和 `lookup_key` 是声明给索引钩子的派生列，旧正文可以没有这些值。首次打开会为既有行建立新索引，后续更新由依赖声明维护。不要取消打开、恢复或写入过程。
- BTree posting、文档 ID bitmap 的持久化编码未改变。有序快照不持久化；缓存失效不影响已开始读取的不可变快照。
- prepared scopes 与授权文本缓存分别最多 4 项，各使用 64 MiB 的保守计费上限；过大的项不缓存。Store 重开时清空。缓存 key 用 Space 序号（该 Space 的任何行变化都会提交一个序号）加 schema 版本与完整有效授权，其它 Space 的写入不会使其失效。`PreparedScope` 只保存成员与长度统计及其准备时的索引版本，不保存 ID 副本，也不自动刷新；同一 Space 序号下的行不会变化，所以按序号取到的 scope 总是当前的。它们不缓存随读取时间变化的返回视图。包含生效/到期时间条件的 Grant 或 Policy 不缓存授权语料，避免时钟变化后继续使用旧可见域。
- 查询 worker 数为 `min(available_parallelism, 4)`。取消等待不会提前释放仍在运行的 worker 的许可；无 Tokio runtime 时直接执行只读任务。
- 100,000 候选与中间解预算继续适用于通用执行。覆盖 COUNT 不加载每个元素，索引分页只加载所需窗口；不能据此假设所有百万行 JOIN、历史查询或受限权限搜索都已无界可用。

## 验证方法

新增测试覆盖 100,001 个 Concept 的稀有类型、FILTER、已有绑定、首屏、续页及 COUNT；小候选交集的实际探测数；删除后的双向索引分页；跨 schema 升级的派生索引回填、更新和重开；历史长版本链及未来元素；BM25 prepared scope 的失效和字符串 ID 同分边界；授权缓存的字段遮盖与结果上限。

可复现的 Core 基准已加入 `core_workloads`：

```sh
CARGO_PROFILE_RELEASE_SPEED_LTO=false CARGO_PROFILE_RELEASE_SPEED_CODEGEN_UNITS=16 \
  ANDA_BENCH_QUERY_SCALE=1 ANDA_BENCH_DOCS=1000001 ANDA_BENCH_ITERATIONS=15 \
  cargo bench -p anda_db --bench core_workloads --profile release-speed
```

基准在计时前建立真实 Collection 和索引，分别记录冷 posting 首次读取与热查询；覆盖稀有/空交集、稠密首屏、深页及完整 ID 集。比较两个版本时必须使用相同 harness、Cargo.lock、优化级别和分配统计器，并在构建/测试结束后串行运行。

## 实测结果

[完整测量、环境及原始数据](benchmarks/anda_query_scale_2026-09-26/README.md)采用相同 harness/Cargo.lock、独立构建目录、同一优化级别，分别测试 100K 与 1,000,001 行，并交替执行两轮。

| 1,000,001 行，热查询 P50 | 基线 | 优化后 |
| --- | ---: | ---: |
| 稀有键与稠密键交集，10 个结果 | 2.317 ms | 0.001209 ms |
| 稠密首屏，50 个 ID | 32.798 ms | 0.002313 ms |
| 稠密深页，50 个 ID | 19.952 ms | 0.002417 ms |
| 完整 ID 集 | 33.672 ms | 28.923 ms |

首次稠密页仍需准备排序：本例约 1.010 ms。热页新增堆内存由约 46.1 MB 降至约 1 KB，但这不包含已保留的约 8 MB posting 排序；完整 ID 集仍需要完整工作集。空交集未测得收益。初版无界求交曾回退，已改为批量路径，初版记录也保留在数据目录。

真实 Nexus 查询在 1,000,001 个当前行上成功：稀有类型、FILTER、已有绑定各读取 1 个 Concept，COUNT 返回 1,000,001 且不读取 Concept 正文，反向结构查询读取 2 个 Concept。该合成夹具的填充行没有逐行建立版本日志，因此不作为百万历史版本或云端性能的结论。

Nexus 探针也可独立复跑：

```sh
ANDA_NEXUS_BENCH=1 ANDA_BENCH_DOCS=1000001 \
  cargo bench -p anda_cognitive_nexus --bench query_scale --profile release-speed
```

首次旧库索引回填、有时间条件的受限权限冷搜索、全历史扫描、全局读写锁、真实向量维度下的 RSS 和云端并发仍须按部署负载评估；本次没有声称所有百万行查询都能在固定耗时内返回。


## 最终检查

- `cargo test --workspace --all-features -j 2`：1,895 项通过，1 项既有 fixture 生成器保持 ignored；包含恢复、格式兼容、属性与召回测试。
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`、`cargo fmt --all -- --check`、`git diff --check`：通过。
- TypeScript codegen、类型检查和 34 个文件内的 577 项测试：通过；更新了 Rust/TypeScript parser 对照 corpus，未改 parser 或 WASM oracle。
- 1,000,001 当前行的 Nexus 结果/GET 探针，以及 100K/1M 的两轮配对 Core 基准：通过。
- 未运行 nightly fuzz、Python binding、独立 WASM/tokenizer workspace 的测试或真实云存储/独占主机并发压测。

测试还覆盖 Unicode 规范等价 FILTER、不得隐藏行相关类型错误、时间条件授权的缓存限制，以及归档目标与活动 slot 的投影缓存隔离。详细检查摘要见 [validation.json](benchmarks/anda_query_scale_2026-09-26/validation.json)。
