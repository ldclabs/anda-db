# anda_db_hnsw — HNSW 向量索引

[English](anda_db_hnsw.md)

`anda_db_hnsw` 是 AndaDB 使用的可嵌入式向量索引组件。它将向量存储为 bf16 格式，采用浮点计算距离，并通过调用方提供的回调函数支持增量持久化。

## 使用示例

```rust
use anda_db_hnsw::{HnswConfig, HnswIndex, SearchOptions};

let index = HnswIndex::try_new(
    "memory".into(),
    Some(HnswConfig { dimension: 384, ..Default::default() }),
)?;
index.insert_f32(1, vec![0.1; 384], 0)?;
index.insert_f32(2, vec![0.2; 384], 0)?;

let hits = index.search_f32_with_options(
    &[0.15; 384],
    10,
    SearchOptions { ef_search: Some(100) },
)?;
# Ok::<(), anda_db_hnsw::HnswError>(())
```

建议优先使用 `try_new`：它会在配置无效时返回错误。兼容构造函数 `new` 则会将非法值自动规范化到支持的区间。
`try_new_seeded(name, config, seed)` 为测试和基准测试提供确定性的图层分配（在相同依赖版本和相同插入顺序下可完全复现）。该 seed 属于运行时状态，不作为持久化配置保存。

## 配置与查询

| 参数 | 默认值 | 合法范围 / 含义 |
| --- | --- | --- |
| dimension | 512 | 1–16,384 |
| max_layers | 16 | 1–64；图层索引从 0 开始 |
| max_connections | 32 | 2–128；第 0 层目标容量为 2×M |
| ef_construction | 200 | 1–4,096 |
| ef_search | 50 | 1–4,096；实际搜索束宽度为 max(ef, top_k) |
| scale_factor | None / 1.0 | 有限正数 |
| distance_metric | Euclidean | Euclidean, Cosine, InnerProduct, Manhattan |
| select_neighbors_strategy | Heuristic | Simple 或 Heuristic |
| reconnect_on_delete | false | 删除节点时是否尝试局部重连 |

每层出度上限由 `capacity + capacity/5` 约束，其中第 0 层的 capacity 为 2×M，更高层为 M。预留的余量用于摊薄边剪枝开销。

两类查询 API 均将 `top_k == 0` 视为无条件的空操作，不进行输入校验，也不递增搜索计数器。正数请求会验证向量维度及数值有限性。请求 `top_k` 超过 4,096 时将明确返回错误，避免因内部隐藏上限而静默返回少于期望的结果。在小图或存在孤立连通分量的图上，近似搜索返回的结果数可能仍少于 k 个。

`search_f32` 保持查询向量为 f32，避免查询量化损失。
`search` 对 bf16 查询向量进行一次性提升。结果按距离升序排列。

`search_f32_with_options` 支持为单次查询动态配置 ef。默认方法复用线程局部工作区，在重入调用时安全回退。
`search_f32_with_workspace` 接收独占借用的 `SearchWorkspace`，适用于希望自主管理内存分配的应用。

## 距离语义与数值边界

| 度量类型 | 距离计算公式 |
| --- | --- |
| Euclidean | sqrt(sum((a − b)²)) |
| Cosine | 1 − 余弦相似度，截断至 [0, 2] |
| InnerProduct | −dot(a, b) |
| Manhattan | sum(abs(a − b)) |

距离值越小代表越相似，包括负的内积距离。
当任一向量的范数小于 `f32::EPSILON` 时，Cosine 返回 1。

公共接口 `DistanceMetric::compute`、`compute_f32` 与 `compute_mixed` 会严格检查维度匹配与输入数值有限性。计算内核保留常规 f32 快速路径，并在发生溢出或欧氏平方下溢时采用更宽精度的算术运算。计算结果若超出有限 f32 范围则报错。

索引在写入前还会对**存储向量**的幅值进行保守边界检查，确保被接纳的任意两节点间的边距离均在 bf16 表示范围内。设 B 为以 f64 表示的 `bf16::MAX`：

| 度量类型 | 存储向量幅值边界 |
| --- | --- |
| Euclidean | L2 范数 ≤ B / 4 |
| Manhattan | L1 范数 ≤ B / 4 |
| InnerProduct | L2 范数 ≤ sqrt(B / 4) |
| Cosine | 任意有限 bf16 元素 |

该数值范围远大于绝大多数真实的 Embedding 向量。在转为 bf16 过程中发生溢出的有限 f32 输入也会被拒绝。查询距离发生溢出将直接报错，而非将 NaN/Inf 插入排序堆。被拒绝的向量输入不会改变图结构或公开操作计数器。

Cosine 查询范数只预先计算一次；不可变节点的范数会被缓存。API 始终返回欧氏距离本身，而非平方 L2。

## 图存储与数据变更

公开的 `HnswNode` 类型及其既有 serde 字段保持兼容：

```rust
# use anda_db_hnsw::half::bf16;
pub struct HnswNode {
    pub id: u64,
    pub layer: u8,
    pub vector: Vec<bf16>,
    pub neighbors: Vec<smallvec::SmallVec<[(u64, bf16); 64]>>,
    pub version: u64,
}
```

运行时的热路径数据表示与之分离：不可变节点由 `Arc` 持有，其 bf16 向量由 `Arc` 共享，每层邻接关系使用 `Vec`。更新边时仅复制邻接列表，不复制向量数据。邻接列表按目标 ID 排序且不包含重复项。系统为每个目标和图层维护精确的反向引用，包含边剪枝引入的非对称边。

`get_node_with` 会物化公开的拥有所有权的表示形式以保证兼容性，主要用于外部检查或导出，而非搜索热路径。单个 `HnswNode::version` 计数器描述的是单节点实例的变更代数，而非可复用 ID 的全局唯一代数。如需安全的代际确认，应使用索引的持久化 API。

删除操作会在 ID 可被复用前移除**所有**入边引用。非入口节点的删除直接保留最高图层，因为尚存的入口节点已证明该图层的存在；仅当删除入口节点时才会扫描寻找替代节点。

当 `reconnect_on_delete=false` 时，删除仅单纯移除边；高频删除可能导致召回率下降和图连通性受损。当设置为 `true` 时，受影响的入边邻居会尝试将已删除节点的对端节点作为局部重连候选。这提升了图的鲁棒性，但不能在所有数据集和删除序列下数学证明连通性，且在写锁下会产生额外的距离计算开销。

## 并发模型

插入与删除操作为同步执行，通过结构互斥锁进行串行化。搜索操作不获取该互斥锁；它读取并发节点表并短暂读取入口点的 `RwLock`。因此整个查询过程不应被描述为无锁 (lock-free) 或可线性化的快照。在并发删除期间若遇到入口节点暂时缺失，最多重试 `SEARCH_MAX_ATTEMPTS` 次。

**所有持久化与 purge 调用必须由调用方进行串行化调度。**
结构变更可以与 flush 过程交叠进行；后续变更保持待处理状态。AndaDB 的 Collection 在 flush 期间已持有其独占操作门，这同样排除了集合级别的并发变更。

没有任何同步锁或 papaya 本地 pin guard 会跨越回调中的 await。快照在操作门保护下捕获不可变节点句柄；编码与 I/O 操作在释放门之后异步执行。成功的 flush 仅在节点当前的 `Arc` 身份仍与快照一致时才清除其脏标记。即便公开的节点计数器重置，移除并重新插入也绝不会引发 ABA 确认混淆。

## 持久化 API

推荐使用 `flush_with_options` 以获得拥有所有权的缓冲区及显式状态：

```rust
use anda_db_hnsw::{BoxError, FlushOptions, FlushOutcome, HnswIndex};

# async fn example(index: &HnswIndex) -> Result<(), BoxError> {
let outcome = index.flush_with_options(
    1234,
    FlushOptions {
        node_concurrency: 8,
        max_in_flight_bytes: 16 * 1024 * 1024,
    },
    |id, bytes| async move {
        // 原子持久化该节点对象并确认落盘
        # let _ = (id, bytes);
        Ok(true)
    },
    |bytes| async move {
        // 原子持久化 IDs 对象
        # let _ = bytes;
        Ok(())
    },
    |bytes| async move {
        // 最后持久化元数据；使用后端的条件写入 (CAS) token
        # let _ = bytes;
        Ok(())
    },
).await?;

match outcome {
    FlushOutcome::Committed => { /* 完整批次已获确认 */ }
    FlushOutcome::NoChanges => { /* 未运行任何回调 */ }
    FlushOutcome::Stopped => { /* 未提交；变更保持待处理 */ }
}
# Ok(()) }
```

上传并发度支持 1–64。在途的节点回调缓冲区总和及后续 IDs/元数据回调缓冲区严格受配置的字节预算约束。单个超出预算的载荷将直接返回错误。使用 `cbor2::serialized_size` 计算精确的节点大小。IDs 对象在所有节点回调成功后才写入；元数据最后写入。节点回调返回 `false` 会停止后续写入且不提交。在发生错误或终止时，flush 启动的所有节点回调在方法返回前均会被等待完成。取消操作可能导致底层已发生部分物理写入，因此脏数据证据会被保留以便后续恢复。

兼容接口 `flush_with` 仅在批次成功提交时返回 `true`，无变更时返回 `false`；中途停止将视为错误返回。`flush_outcome` 为借用缓冲区的节点回调和同步 writer 提供相同的显式状态；`flush` 是其返回 bool 的兼容适配器。

Writer 适配器在确认成功前会同时检查 `write_all` 与 `Write::flush`。`Write::flush` **并不**代表文件系统的 fsync 或远程落盘保证。如需强保证，应使用异步回调。在调用可能返回 `NoChanges` 的 API 之前，绝不要提前截断现有的元数据或 IDs 文件。

细粒度接口依然可用：

| API | 契约说明 |
| --- | --- |
| metadata_bytes | 纯序列化操作；不推进版本水位 |
| store_metadata | 检查 writer flush，仅确认元数据 |
| store_metadata_with | 回调成功后确认元数据 |
| store_ids | 写入 IDs 镜像并检查 writer flush |
| store_dirty_nodes | 回调成功后逐个确认未变更的节点 |

细粒度的元数据写入不代表确认了完整的代数，也不授权执行 purge 清理。仅向内存 `Vec` 写入不能证实外部存储已完成写入；应使用 `metadata_bytes` 进行序列化，或通过 `store_metadata_with` 确认后端落盘。

## 墓碑标记与物理清除 (Purge)

标准执行顺序：

1. 完成包含删除操作的 flush。
2. 调用 `purge_removed_nodes` 删除符合条件的节点 blob。
3. 再次 flush，或在下次周期性 flush 时持久化缩减后的墓碑集合。

仅当墓碑对应的确切删除实例包含在**已确认的完整**快照中，或已从已提交的元数据中加载时，该墓碑才具备物理清理资格。在 flush 期间发生的删除属于后续批次。重新插入的 ID 和更新的删除实例会在回调前后进行检查，绝不会意外消费过往的确认标记。

Purge 回调返回 `true` 表示确认，返回 `false` 表示中止，或返回错误。将“blob not found”视为成功：在物理删除后但下次元数据 flush 前发生崩溃，可能会重新回放该删除。取消操作会保留未确认的墓碑。`has_pending_flush` 综合覆盖节点、元数据/IDs 以及墓碑的状态。

## 传输协议兼容性与故障恢复

现有的元数据字段与公开节点字段完全保持向后兼容可读。
新节点对象增加了可选的 `g` 字段，记录写入时的 generation 代数。IDs 对象保持为单一 CBOR 字节串，包含 Portable Roaring 位图。旧版 AndaDB reader 和严格单项的 CBOR 校验器均可正常读取该格式。

存储后端必须具备对单个对象的原子替换能力。针对固定 key 的节点与元数据替换，必须使用后端的条件写入 (CAS)，或者节点对象必须采用包含 generation 的不可变路径。这可防止因网络延迟迟到的旧请求覆盖已提交的新对象。按照“节点 → IDs → 元数据”的写入顺序实现的是**可恢复的部分进度**，而非跨对象的原子事务，也不保证能回滚至上一快照。AndaDB 适配器为每个存活节点（及未 purge 的墓碑）维护一个后端的 `ObjectVersion` token；清除 blob 时将同步移除其 token。

启动阶段在内存中以事务化流程推进：

1. 校验元数据/配置并暂存 IDs，暂不修改内存中的活跃图。
2. 以有界并发（最多 32 个在途任务）拉取并校验节点对象。
3. 清除悬空引用、重复边、自环和非法层级的边；修复入口点与最高层级。拒绝非有限数值、非法节点 ID/层级结构及超出上限的度数。早于当前插入幅值限制的已持久化有限向量，在后续修复和保存后依然可以正常加载。
4. 在版本迁移时重新计算一次遗留的缓存距离。丢弃其距离无法用持久化 `bf16` 边格式表示的遗留边。
5. 若节点的代际标记比已提交的元数据更新，则从已加载 IDs 引用的向量中重建该节点。当历史向量超出当前插入幅值时，改为就地规范化拓扑结构与缓存距离。若剪枝导致节点孤立，在重建图中为每个节点保留一条有界的环形边。合法的现代与旧版镜像保留其原始图结构，即便近似剪枝或快速删除导致了图的断连。
6. 仅在全部成功后才替换发布内存状态；将修复过的节点标记为 dirty。

已加载的 IDs 对象决定了恢复过程保留哪些向量。因此，失败的批次可能恢复出较早或较晚的进度，取决于 IDs 对象是否已被替换。操作计数器恢复自已保存的元数据；它们并不是元数据保存之后发生写入的审计日志。AndaDB 的 Collection 还会额外回放文档变更意图，以恢复权威的业务文档状态。

`recovery_report` 暴露缺失节点数、修复次数以及是否触发了重建。加载器返回 `None` 显式允许丢弃缺失的 blob；若缺失数据必须阻止应用启动，则应返回错误。在节点加载期间发生失败或被取消，之前的运行时图结构保持不变，暂存的 IDs 可供重试。

遇到非法的持久化配置时会明确拒绝，而非静默修改维度或图容量限制。持久化的修复结果会在下次 flush 时自动落盘。

## 测试、容量估算与基准

```sh
cargo test -p anda_db_hnsw --all-targets
cargo clippy -p anda_db_hnsw --all-targets -- -D warnings
cargo run -p anda_db_hnsw --example hnsw_demo
```

回归测试套件覆盖数值溢出、ID 复用、缓冲 writer 写入失败、停止/取消契约、墓碑代际、混合镜像恢复、事务化加载重试、查询边界以及有界并行上传。召回率测试采用确定性数据与随机种子。

内存容量估算必须纳入内存对齐、容量预留、Arc/哈希表开销、反向引用以及临时快照。在 64 位构建中，`(u64, bf16)` 占用 16 字节而非 10 字节。bf16 向量本身的元素占用 `2 × dimension` 字节，但这仅代表纯向量数据，不能等同于整个索引的内存驻留体积。

有关可配置的数据矩阵、分配/延迟/召回率测量数据及 release 配置调优，请参考 [基准测试说明](../rs/anda_db_hnsw/benches/README.md)。
