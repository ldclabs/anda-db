# B-tree 维护清单与验证结果

2026-09-06；基于 `f54fd747e3f33d5d32cf8c1686874bfb59da55fc` 的审查清单实现。
未使用 subagents。没有增加生产依赖；保留公开桶/metadata 的 CBOR 数据形状，移除了只为旧测试序列化器启用的 DashMap serde 特性。

## 完成清单

| 状态 | 项目 | 实现与证据 |
|---|---|---|
| [x] | B1 空清单与旧格式混淆 | 内部加载 DTO 区分字段缺失与显式空 map，拒绝显式 null；新空索引不探测遗留桶，清洁旧索引首次 flush 升级 |
| [x] | B2 不完整索引可写 | 完整加载缺桶报错；MetadataOnly/Partial/Ready 状态检查；显式部分加载、失败/取消后的重试；Collection 缺桶重开测试 |
| [x] | B3 超深查询析构溢出 | 迭代校验与释放；Serde 输入深度/节点/Include 总量限制；十万层子进程测试、空索引、转换与反向查询覆盖 |
| [x] | B4 元数据文件提前截断 | 示例使用提交回调内临时文件同步和原子替换；无变更、桶失败、重命名前失败、新旧快照重开测试 |
| [x] | B5 同桶数压缩漏保存 | CompactionOutcome.changed；稳定排序、真正 no-op；封装保存同桶数重建，随后重开校验 |
| [x] | PERF1 大 posting 删除 | 小列表直接保存，较大列表增加 ID→位置索引，平均 O(1) 按值删除；数组分配复用、单次哈希追加、缩容回退；计数与重载测试 |
| [x] | PERF2 复杂分页全量候选 | 借用输入边界编译不相交区间，布尔组合统一为区间代数；无 FV 克隆，无嵌套 Include 逐候选线性匹配；保留顺序和 callback 组 |
| [x] | PERF3 保存复制与峰值内存 | 借用序列化，不克隆 posting/辅助表；逐桶编码上传；O(桶数) 清单与记录，单桶数据缓冲 |
| [x] | PERF4 性能基线与候选评估 | 同一带内存计数的发布基准比较旧/新版本；正序完整测量和反序 CPU 复核，记录分位数、吞吐、内存和实际写出字节 |
| [x] | M1 状态与代码结构 | BucketState、Posting、Removal、LoadState；单/批量共用追加、删除、所有权清理原语；query/mutation/persistence/compaction/posting/state 分工，测试独立文件 |
| [x] | M2 模型与故障测试 | 连续数组操作、批量替换、压缩、增量检查点/重开、桶/清单失败；有序分组结果比较；共享 FV/重复输入/最终删除并发测试 |
| [x] | M3 文档与示例 | 技术文档重写，修正加载/唯一性/复杂度/锁/重试代号说明；开启并增加可运行 doctest |

修复实现主要位于 [B-tree 子模块](../rs/anda_db_btree/src/btree/)，定向回归见 [regressions.rs](../rs/anda_db_btree/tests/regressions.rs)、[模型测试](../rs/anda_db_btree/tests/proptest_model.rs)、[并发测试](../rs/anda_db_btree/tests/concurrency.rs) 和 [Collection 恢复测试](../rs/anda_db/tests/btree_recovery.rs)。

## 性能结果

环境：本机 arm64，rustc 1.97.1；使用仓库 bench/release 配置（opt-level=z、LTO）。旧版源代码在修改前复制，并加入同一基准程序。比较时不运行本任务的编译/测试；每个进程先预热 CPU。先旧后新完整运行，再新后旧进行无注入延迟的 CPU 复核。

常规查询/删除场景 100 个样本；构建、多线程、保存场景 20 个样本。计数分配器本身有开销；纳秒级时延有计时量化，倍率仅描述这些固定负载，不代表生产系统整体吞吐。p95/p99、吞吐、分配和原始序列化量均保存在 JSON 中。

查询含 10,000 个键，回调只保留一个结果；大 posting 含 100,000 个 ID。保存场景是 200 个独占 posting 的桶，键约 4 KiB，基准断言每轮恰好 10% 或 100% 的桶写出。内存列是操作期间额外存活堆的峰值，不含预先构建的查询/保存输入，不是进程 RSS。

| 场景 | 修改前中位数 μs | 修改后中位数 μs | 前/后时延 | 额外峰值堆 bytes：前 → 后 |
|---|---:|---:|---:|---:|
| 普通范围首项（1 万键） | 0.500 | 0.458 | 1.09× | 40 → 8 |
| And 范围首项（1 万键） | 235.583 | 0.583 | 404.09× | 233,216 → 80 |
| Or 范围首项（1 万键） | 630.875 | 0.666 | 947.26× | 327,224 → 200 |
| Not 筛出末项（1 万键） | 285.333 | 0.583 | 489.42× | 278,568 → 128 |
| And 反向首项（1 万键） | 338.208 | 0.834 | 405.53× | 233,256 → 312 |
| 10 万 ID：删除不存在的 ID | 31.125 | 0.042 | 741.07× | 0 → 0 |
| 10 万 ID：删除尾部 ID | 31.250 | 0.125 | 250.00× | 0 → 0 |
| 高基数：构建 2,000 个关联 | 747.541 | 644.250 | 1.16× | 505,128 → 410,480 |
| 低基数：构建 2,000 个关联 | 300.125 | 289.542 | 1.04× | 75,360 → 110,176 |
| 10% 脏桶保存，无注入延迟 | 127.208 | 114.916 | 1.11× | 179,917 → 19,617 |
| 100% 脏桶保存，无注入延迟 | 1,311.583 | 1,245.458 | 1.05× | 1,669,133 → 29,123 |
| 100% 脏桶保存，每桶注入 1 ms | 311,320.083 | 311,497.083 | 1.00× | 1,670,285 → 28,403 |

确定性计数回归同时验证：复杂首项分页不克隆索引 FV；10,000 ID 的删除不再进行 10,000 次比较；flush 不再为 10,000 ID 的 posting 克隆 20,000 次 PK。普通正向查询还复用第一组 callback 的结果数组，消除一次结果缓冲分配。

高基数构建场景的分配次数由约 4,661 次降至 2,659 次，峰值堆下降约 19%。低基数场景的峰值堆由 75,360 增至 110,176 bytes（约 46%）：位置索引多存储一个数组位置，换取大列表按值删除的平均常数复杂度。这是明确的内存取舍；小 posting 不分配位置表。

100% 脏桶、无注入延迟的额外峰值堆由约 1.67 MB 降至约 29 KB，减少约 98%。带注入延迟时，总耗时仍由串行对象 I/O 主导，未宣称该场景的吞吐提升。反序 CPU 复核给出相同方向：复杂首项分页约 400–946×，大列表尾部删除约 249×，高基数构建约 1.15×；点查询基本持平。

原始结果：[完整旧版](../rs/anda_db_btree/benches/results/verified-before.json)、[完整新版](../rs/anda_db_btree/benches/results/verified-after.json)、[CPU 旧版复核](../rs/anda_db_btree/benches/results/cpu-before.json)、[CPU 新版复核](../rs/anda_db_btree/benches/results/cpu-after.json)。

## 后续候选的决定

- 本次保留串行 I/O 回调契约。模拟延迟验证了 I/O 场景的瓶颈，但实际对象存储服务尚未做生产测量；并行加载涉及当前 lending AsyncFnMut、回调顺序及内存预算，应通过新增可选接口评估，不能直接并发调用既有回调。
- 本次保留统计锁和查询计数方式。基准中点查持平、多线程写入改善，没有足够证据值得为计数增加一套分片合并状态。
- 已采用小 posting 紧凑表示。大 posting 分段持久化会改变存储格式和回收协议；不混入这次保持旧格式可读的维护。单个孤立超大 posting 仍然需要完整编码缓冲。

这些决定完成了 PERF4 的评估范围；并不承诺消除所有可能的性能瓶颈。

## 验证命令

```sh
cargo test --workspace --all-features
cargo test -p anda_db_btree -p anda_db --all-features
cargo test -p anda_db_btree --all-targets --all-features
cargo clippy -p anda_db_btree -p anda_db --all-targets --all-features -- -D warnings
cargo fmt -p anda_db_btree -p anda_db -- --check
cargo bench -p anda_db_btree --bench workloads
ANDA_BTREE_BENCH_NO_IO_DELAY=1 cargo bench -p anda_db_btree --bench workloads
```

Rust 全工作区验证通过 1,512 个测试，fixture 生成测试按项目设置忽略。随后增加的两个加载边界回归以及最终查询分配优化，由受影响两 crate 的完整测试再次验证。受影响模块的最终测试、Clippy 和格式检查通过。本次未涉及 TS 引擎或生成文件。

全仓 `cargo fmt --all -- --check` 另发现未改动的 KIP 文件存在既有格式差异：`rs/anda_kip/src/parser.rs`、`rs/anda_kip/src/parser/common.rs`、`rs/anda_kip/src/parser/json.rs`。这些差异不属于本次 B-tree 修改，没有把无关格式变更混入此次改动；上述受影响两 crate 的格式检查单独通过。
