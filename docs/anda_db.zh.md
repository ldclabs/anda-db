# AndaDB 技术文档

[English](anda_db.md)

版本：0.13.0

## 概述

AndaDB 是 AndaDB 工作区中的核心嵌入式数据库 crate。专为 AI Agent 记忆与知识负载设计，使应用程序能够在进程内存储结构化文档、支持多种检索模式建立索引，并将持久化逻辑保留在本地，无需委托给外部数据库服务。

在 crate 层面，AndaDB 提供：

- 管理集合与共享持久化的数据库对象
- 支持文档 CRUD 的 Schema 感知集合（Collection）
- 基于 B-Tree 索引的精确匹配与范围检索
- 基于 BM25 索引的全文检索
- 基于 HNSW 索引的向量相似度检索
- 基于互惠排名融合（RRF）重排的混合检索
- 基于 `object_store` 的持久化层，支持可选的压缩与缓存

本 crate 保持紧凑的公共接口，主要功能分布在以下模块：

- `database`：数据库生命周期与集合管理
- `collection`：文档操作、索引管理、检索与元数据
- `query`：混合检索、过滤条件与重排类型
- `index`：索引类型与自定义索引钩子（Index Hooks）
- `schema`：从 `anda_db_schema` 重新导出的类型系统
- `storage`：持久化、压缩、对象版本与 I/O 统计
- `error`：统一的 `DBError` 错误类型

## 设计目标

AndaDB 针对特定系统的需求进行了优化，而非通用的关系型数据库负载。

其核心目标包括：

- 为 AI Agent 提供持久化长期记忆
- 统一结构化、词法（全文）与语义（向量）检索
- 写入时执行显式 Schema 校验
- 嵌入式部署，无需强制依赖外部数据库服务端
- 基于 `object_store` 的存储抽象
- 对刷盘、检查点与只读模式具备可预测的运维控制

在工程实践中，该库表现为带有针对 Agent 记忆定制检索原语的文档数据库。

## 系统架构

`anda_db` 构建在工作区多个底层 crate 之上：

```text
Application
  -> anda_db::database::AndaDB
     -> anda_db::collection::Collection
        -> anda_db::index::{BTree, BM25, Hnsw}
        -> anda_db::storage::Storage
           -> object_store::ObjectStore

Supporting crates:
  - anda_db_schema: Schema 与字段值系统
  - anda_db_btree: 精确匹配/范围索引引擎
  - anda_db_tfs: BM25 全文索引引擎
  - anda_db_hnsw: 向量索引引擎
  - anda_db_utils: 基础工具（如 UniqueVec）
```

核心执行模型如下：

1. 根据 Schema 校验传入文档。
2. 提取并衍生文档的索引值。
3. 更新内存中的索引状态。
4. 将文档持久化至对象存储。
5. 刷盘元数据与索引文件，使状态达到持久化安全。

该库还包含面向故障恢复的逻辑，用于重新打开集合、加载已持久化的索引并修复部分刷盘不完整的中间状态。

## 核心概念

### Database（数据库）

`AndaDB` 是顶层句柄。一个数据库实例拥有：

- 数据库名称
- 共享的 `object_store` 后端
- 以数据库名称为根的存储命名空间
- 数据库元数据
- 已打开的集合集合

关键方法：

- `AndaDB::create`：创建新数据库；若元数据已存在则报错
- `AndaDB::connect`：打开已有数据库或创建新数据库
- `AndaDB::open`：打开已有数据库；若不存在则报错
- `create_collection`：创建新集合
- `open_collection`：打开已有集合
- `open_or_create_collection`：若存在则打开，否则创建
- `delete_collection`：删除集合及其持久化数据
- `flush`：刷盘所有已打开的集合与数据库元数据
- `close`：将数据库切换为只读模式并刷盘挂起的状态

数据库还公开了 `extensions` 映射表，用于存储与数据库元数据一同持久化的轻量级用户自定义元数据。

### Collection（集合）

集合是应用程序操作的基本单元。同一集合内的所有文档共享同一个 Schema。

一个集合拥有：

- 集合名称
- 当前活跃 Schema
- 集合存储命名空间
- 零个或多个 B-Tree、BM25 和 HNSW 索引
- 集合统计信息与元数据
- 内存文档 ID 追踪结构
- 用于文本索引的分词器配置
- 可选的自定义 `IndexHooks`

关键方法：

- `add` 与 `add_from`
- `get` 与 `get_as`
- `update`（拒绝修改 `_id`：ID 是文档的存储路径，也是每个索引中的 Key）
- `remove`（已失效 ID——仍记录在位图但其底层对象已丢失——与其他 ID 一样正常移除，通过 ID 清理其索引倒排项）
- `search` 与 `search_as`
- `search_ids` 与 `query_ids`（返回匹配的最小 ID，受 `MAX_SEARCH_LIMIT` 限制）
- `query_last_ids`（返回匹配的最大 ID——即由新到旧的游标分页）
- `query_all_ids`（无上限；适用于正确性依赖于完整结果集的进程内调用方）

有界查询保留匹配集哪一端的结果由所调用的方法决定，而非过滤条件的语法形态：仅 `_id < cursor` 与 `AND(user == u, _id < cursor)` 的分页行为完全一致。仅有 `_id` 过滤条件及其补集会按请求方向扫描 ID 集合并在满足一页后终止；B-Tree 字段过滤器遍历的是 Key 空间，其顺序与 ID 顺序不同，因此会完整评估后再裁剪至请求的一端。
- `create_btree_index`、`create_bm25_index`、`create_hnsw_index`
- `compact_btree_index`、`compact_bm25_index`
- `flush` 与 `close`

集合同样公开了自身的 `extensions` 映射表，用于存储少量特定于应用程序的元数据。

`new_document()` 会初始化内部 ID 占位值，调用方填写业务字段后即可交给 `add`。更新会比较规范化后的值；同值更新直接返回文档，不写日志或文档、不重建索引，也不增加变更统计。实际发生变化的文档会在写日志、修改索引前完成编码和大小检查；此阶段的 `PayloadTooLarge` 错误不会使集合句柄失效。

对于“一个索引等值条件与 ID 条件求交”的分页，执行器只扫描一次匹配 posting，并仅保留请求的一页。因此，即使某个 owner/status 对应大量文档，额外结果内存也受页大小约束。扫描仍会访问候选 ID，其他复杂交集保留原有执行方式。

### Schema

Schema 系统源自 `anda_db_schema`，并通过 `anda_db::schema` 重新导出。

定义 Schema 的常见方式有两种：

- 使用 `AndaDBSchema` derive 宏从 Rust 结构体派生
- 使用 Schema 构建器与字段项以编程方式构造

典型的字段类别包括：

- 标量值：整数、浮点数、布尔值、文本
- 字节数组
- 向量
- 数组与映射表
- 类似 JSON 的动态值
- 可选值（Optional）

集合在接受文档前会严格根据其 Schema 进行校验。

Schema 升级删除顶层字段前，必须先移除所有引用它的索引，包括复合 B-Tree 和多字段 BM25 索引。违规升级会在持久化新 Schema 前报错。应先按旧 Schema 打开集合并移除索引，关闭后再升级。

### Document Identity（文档标识）

文档 ID 为无符号 64 位整数（`u64`）。使用 `add` 与 `add_from` 时，集合在内部管理文档 ID 的分配。

每个集合维护：

- `max_document_id`
- 用于范围遍历的有序 `BTreeSet` ID 集合
- 用于高效成员判定与持久化的位图（`croaring::Treemap`）

该组合兼顾了快速包含检查、有序扫描与可靠的持久化恢复。

## 索引模型

AndaDB 支持三类互补的索引。在已有集合上创建新索引时，会先根据集合当前文档 ID 进行回填（backfill），回填成功后才将该索引注册到集合元数据中。若回填失败（例如唯一 B-Tree 索引冲突），该索引不会对外可见，并会尽最大努力清理临时索引元数据。

### B-Tree 索引

B-Tree 索引用于：

- 精确匹配
- 范围查询
- 唯一性约束
- 复合虚拟字段索引

它们是 `Filter::Field` 查询模型及直接 ID 范围过滤的底层支柱。

典型场景：

- 按会话/线程 ID 过滤
- 按 `created_at` 时间范围过滤
- 对外部业务键实施唯一性约束
- 查询由多个字段组合而成的合成复合键

对于多字段 B-Tree 索引，集合将各索引字段值组合为确定性的二进制表示，并将其作为虚拟字段存储。

### BM25 索引

BM25 索引支持针对一个或多个字段的全文检索。

关键特性：

- 集合可通过 `set_tokenizer` 自定义分词器
- 单个 BM25 索引可覆盖多个字段
- 查询支持标准模式与逻辑检索运算符模式
- 结果可与向量检索结果通过 RRF 进行融合

默认 BM25 路径作用于集合本地。只需创建一次索引，随后使用 `Query.search.text` 即可检索文档。

### HNSW 索引

HNSW 索引支持针对向量字段的近似最近邻（ANN）检索。

关键特性：

- 被索引字段可以是 `Vector` 或 `Option<Vector>`；可选值缺失或为 null 时跳过索引，后续更新可以补充或清空向量
- 索引构建由 `HnswConfig` 参数化控制
- 查询向量以 `Vec<f32>` 形式传入
- 检索返回带有排序的文档 ID，可与 BM25 结果融合
- 持久化的向量在读回时可表现为与 Schema 兼容的 `Array(U64 bits)` 格式；默认索引钩子在插入 HNSW 前会归一化该格式

该索引家族构成了嵌入（Embedding）或表征向量的语义检索路径。

### 混合检索与 RRF

当 `Query` 中同时包含文本与向量检索时，AndaDB 会并行执行二者，并使用 `RRFReranker` 合并各自的有序 ID 列表。

重排器工作流程：

- 为各结果列表分配互惠排名得分：$1 / (k + \text{rank})$
- 对同一文档在各列表中的得分进行求和
- 按合并后的综合得分降序排列

这是一种务实的混合检索策略，既保持了查询语义的简洁性，又兼顾了多路检索信号。

## 查询模型

查询接口设计高度精炼。

### `Query`

`Query` 包含：

- `search: Option<Search>`
- `filter: Option<Filter>`
- `limit: Option<usize>`

实际执行顺序为：

1. 由文本和/或向量检索生成带排序的候选文档
2. 应用过滤条件
3. 执行最终数量限制（Limit）

### `Search`

`Search` 包含：

- `text`：可选的 BM25 文本查询字符串
- `vector`：可选的 HNSW 查询向量
- `bm25_params`：可选的 BM25 评分调优参数
- `reranker`：可选的自定义 RRF 配置
- `logical_search`：是否启用 BM25 逻辑搜索运算符

### `Filter`

`Filter` 支持递归逻辑组合：

- `Field((index_name, range_query))`
- `Or(Vec<Box<Filter>>)`
- `And(Vec<Box<Filter>>)`
- `Not(Box<Filter>)`

可清晰表达复杂的复合约束条件，例如：

- 限制在特定会话 ID 内的向量检索
- 限制在特定时间窗口内的全文检索
- 排除已知记录的 ID 范围扫描

### 限制与候选集扩展

在内部，混合检索在过滤前获取的候选集数量可能超过最终 `limit`，以确保排序与过滤后仍能产生充足的有效结果。对外公开的 `limit` 始终是最终输出的契约保证。

`search_with_options` 与 `search_ids_with_options` 接收 `query::SearchOptions`，无需更改现有的 Query/Search 传输格式。默认参数为 10 倍过采样、4096 候选集上限、针对不超过 4096 个 ID 的高选择性子集执行精确评分，以及在后置过滤导致命中数不足时启用自适应扩展。子集 BM25 保留全局词项统计；子集向量使用精确距离。融合重排仅在选定的子集内进行。若要保留全局排序优先行为，可设置 `prefilter_limit: 0`；若需禁用自适应扩展，可设置 `adaptive: false`。子集向量的精确计算不会增加图遍历步数统计。

OR 分页仅保留各分支并集在所请求方向的一端。AND 优先评估代价低且选择性高的操作数，候选相对 NOT（candidate-relative NOT）保持在该范围内。复合主键范围在遍历有序 ID 时逐项测试谓词，而非物化完整的交集或补集。通用的宽泛非主键交集仍可能需要 $O(\text{matches})$ 的工作内存；最终结果 limit 并非所有执行计划的通用内存上限。

## 存储层

存储模块基于 `object_store` 实现持久化逻辑。

### 存储后端可移植性

AndaDB 最关键的设计抉择之一，在于避免将持久化绑定到单一本地磁盘实现上，而是构建在 `object_store::ObjectStore` trait 之上，为对象存储服务与本地环境提供统一的异步 API。

这意味着同一套 AndaDB 应用程序只需极小改动即可对接到不同的存储后端。根据宿主应用启用的 `object_store` feature flags，存储层可支持：

- 用于测试与临时运行的内存存储（In-memory）
- 用于嵌入式部署的本地文件系统存储
- Amazon S3
- Google Cloud Storage
- Azure Blob Storage
- 兼容 HTTP/WebDAV 的对象存储

这种可移植性对 AI 记忆系统尤为重要，它使得相同的集合、索引与刷盘逻辑能够在本地开发、自托管环境与云端对象存储间自由迁移，无需重构数据库层。

对于本地文件系统，`object_store` 0.14 未实现 `PutMode::Update`。通过将 `LocalFileSystem` 包装在 `anda_object_store::MetaStoreBuilder` 中，可提供 AndaDB 所需的条件写入语义。快速入门指南与内置示例均采用此适配器。

此外，`object_store` 建模的是对象存储语义而非 POSIX 文件系统语义。这为 AndaDB 提供了更坚实可靠的元数据与索引持久化基础，因为底层抽象天然支持条件读写、分块上传、批量删除以及可直接映射到现代云存储系统的缓冲适配器。

换言之，AndaDB 不仅将 `object_store` 用作便捷的包装层，更将其作为整个持久化层的可移植性与持久性边界。具体后端是部署层面的选择，其上的数据库核心逻辑保持完全一致。

### 存储命名空间

数据库与各个集合使用独立的存储前缀。

在单个集合内，持久化的对象包括：

- 集合元数据
- 文档 ID 位图
- 文档主体内容
- B-Tree 元数据与分桶数据
- BM25 元数据与分桶数据
- HNSW 元数据、ID 映射与节点文件
- 用于检查点与 I/O 统计的存储元数据

### 小型对象与流式写入

存储层区分两种写入模式：

- 通过 `put` 或 `put_bytes` 写入的小型对象
- 通过 `stream_writer` 写入的流式对象

`StorageConfig.max_small_object_size` 保护小型对象写入路径不受超大载荷的影响。

内部索引桶、索引元数据、HNSW 对象和集合 ID 位图使用独立的 `max(256 MiB, max_small_object_size)` 编码/解压后大小预算，读写两端保持一致。高频词或重复 B-Tree 键可以超过单文档的准入上限；CBOR 格式和条件写协议保持不变。posting 仍整体存储，其重写成本会随数据量增长，内部容量预算也仍有上限。

### 压缩

存储层支持使用 zstd 压缩数据载荷。

关键细节：

- 压缩为可选项，由 `StorageConfig.compress_level` 控制
- 若压缩未能减少数据体积，则自动跳过压缩
- 解压缩受到最大体积策略保护，防范解压炸弹（Decompression Bomb）风险

### 缓存

小型对象可使用 `moka` 在内存中建立缓存。

缓存主要面向：

- 频繁读取的元数据对象
- 小型文档读取
- Agent 循环中的高频重复访问模式

缓存大小可按条目数（`cache_max_capacity`）或按字节数（`cache_max_bytes`）显式配置。对于新部署，推荐使用显式字节预算，例如 `StorageConfig::default().with_cache_max_bytes(64 * 1024 * 1024)`。已持久化的条目数配置保持历史兼容语义。缓存一致性采用 4096 个固定代际分片条带（每个 Storage 仅占 32 KiB）：不相关路径的哈希碰撞只会导致缓存未命中，绝不会提供脏数据。

### 版本化更新

存储层通过对象存储的元数据（如 ETag 与版本 ID）构建并追踪 `ObjectVersion`。

版本信息用于执行条件更新，从而实现：

- 防止意外覆盖更新的状态
- 准确检测前置条件不满足（Precondition Failures）
- 安全协调元数据与索引的刷盘流程

流式 I/O 拥有独立的显式明文预算：默认为 256 MiB，可使用 `stream_reader_with_limit` 与 `stream_writer_with_limit` 自定义预算。读取采用独立于压缩比的明文上限保护，并拒绝零字节分块。缓冲文档 PUT 操作维持小型对象上限约束；内部 update 意图记录允许承载两份文档加信封开销。大型拥有的压缩/解压缓冲区（不小于 256 KiB）在有界的阻塞线程池中运行；任何集合的写变更均不会脱离其租约独立执行。

## 刷盘、持久化与故障恢复

AndaDB 中的持久化机制是增量式的，而非传统关系型数据库意义上的事务。

### `flush` 的执行内容

在集合层面，`flush` 执行持久化：

- 已更新的集合元数据
- 发生变动的文档 ID 位图
- 脏索引状态
- 存储检查点元数据

在数据库层面，`flush`：

- 刷盘所有已打开的集合
- 持久化数据库元数据

### `close` 语义

`close` 会将数据库或集合切换至只读模式，随后刷盘挂起的变更。这是拥有可变状态的进程在正常终止时的标准生命周期操作。

### 自动刷盘（Auto Flush）

数据库提供 `auto_flush(cancel_token, interval)` 用于后台周期性刷盘。这适用于希望限制持久化延迟、又无需在每次单次写入后都立即刷盘的 Agent 运行时。

### 恢复策略

重新打开集合时，库执行以下流程：

- 加载集合元数据
- 加载已持久化的文档 ID 位图
- 加载已持久化的索引
- 执行打开回调以安装确定性钩子和原始分词器；其首次查询、写入或索引创建操作会在执行前先完成恢复
- 重放持久化写入意图（更新/删除的预写记录 write-ahead intents）
- 在持久化检查点与分配水位线所限定的精确 ID 窗口内执行修复扫描（repair scan），恢复已写入对象存储但未完全反映在索引持久化中的文档

这套恢复机制是本 crate 能够稳定运行于长生命周期 Agent 记忆进程中的核心保证之一。

### 一致性契约：单写入者与取消即中毒

AndaDB 的持久化设计依赖三条显式规则：

1. **单数据库单写入者（Single Writer）。** 给定数据库前缀在同一时刻仅允许一个活跃进程执行修改操作。`DBConfig::lock` 是应用级密码，而非操作系统级租约屏障；最终防线是对集合与索引元数据对象施加条件 PUT——`Precondition` 冲突意味着存在第二写入者，系统绝不会在原地进行调和。
2. **取消即崩溃（Cancellation is a Crash）。** 产生变更的 Future（`add`、`update`、`remove`、`flush`、`close`、扩展字段写入、Compaction）必须轮询至执行完毕。若 Future 在执行中途被 Drop——或存储写入失败且结果未知——该集合句柄将**中毒（Poisoned）**：后续修改操作均会返回指明该状态的错误。在已中毒或已退役句柄上的读取操作仅属于该代际的尽力观察，可能落后于存储状态。在依赖恢复视图前必须重新打开集合。
3. **仅在 Reopen 时执行恢复。** 重新打开集合（例如通过 `AndaDB::open_collection`，它会自动丢弃已中毒句柄并加载全新代际）会重放预写意图并执行修复扫描，将内存状态收敛至以对象存储为唯一真实数据源。

在实践中，中毒现象极为罕见：内置驱动（`auto_flush`、HTTP 服务端）绝不会主动取消修改类 Future。避免将 AndaDB 修改操作包裹在 `tokio::select!` 或 `timeout` 中，除非已准备好在发生超时后重新打开集合。

### 分配水位（Allocation Watermark）

`add` 操作不写入单次变更的持久化记录。相反，集合维护一个持久化的**分配水位**（`alloc_watermark.cbor`，按每 64 次分配为一个步长跨度发布）：每个文档 ID 在写入其底层对象前必须先被分配水位覆盖，因此重新打开时的修复扫描能够精确枚举所有可能需要恢复的 ID——无需概率扫描启发式算法，且每 64 次新增仅增加一次微小的额外 PUT。Update 与 Remove 仍写入持久化意图记录，因为简单的对象扫描无法廉价检测出内容被修改或被删除。

唯一键写入会同时保留其旧的与新的原生索引键，直到文档 PUT/DELETE 及所有可能的回滚全部完成。锁按稳定的条带顺序获取；不相关的键仍可并发提交。恢复过程中若发生唯一性冲突将直接报错，而非静默忽略索引条目。失效 ID 移除携带仅含 ID 的清除意图。活跃句柄仅保留挂起的意图序列号，不保留完整文档副本。

恢复读取与意图退役采用受控的 I/O 并发度（默认为 8，每个句柄可通过 `set_io_concurrency(1..=64)` 配置）。偶发的读取失败将中止恢复流程，且不推进检查点。`recovery_issues()` 用于报告该句柄跳过的损坏或不符合 Schema 的文档对象。维护接口 `reconcile_storage` 在排他租约下执行全量扫描。

意图重放会并发预取不同文档的当前内容，再按 ID 顺序应用索引修复。有序 ID 集合与 roaring 位图由同一把锁保护，并共同维护成员变更脏标记；仅更新文档或扩展元数据的检查点不重写 `ids.cbor`。脏标记只在位图 PUT 成功后清除。

仅修改元数据的扩展写入可发布某个索引引用的移除，但绝不会发布新暂存的索引。完整检查点会先持久化索引数据，再将其注册到元数据中。未知的元数据 PUT 结果会使句柄中毒，从而在 reopen 时刷新其 CAS 令牌。

打开已迁移路径的数据库时，会采用所请求的前缀作为数据库与集合路径；持久化存储的历史旧数据库名称不会导致操作路径重定向。

## 只读模式与安全控制

数据库与集合均可显式切换至只读模式。

适用场景：

- 受控停机下线
- 运维维护窗口
- 在进程内通过稳定快照对外提供查询服务

只读模式会阻止文档和索引状态被持久化：当集合或其所属数据库处于只读状态时，`Collection::flush` 直接返回 `Ok(false)` 且不执行写入，而不是在 `AndaDB::flush` 及 auto-flush 循环的每个间隔中报错。`close` 仍会刷盘挂起状态，且 `AndaDB::flush` 仍会持久化发生变更的数据库元数据，因此对只读数据库执行的 `set_extension` 会在下一次 flush 时正常写入。

数据库级配置还支持可选的不透明 `lock` 凭据值。这使得应用程序能够确保只有携带预期锁凭据的进程才能打开数据库进行写操作。

## 元数据与扩展字段

数据库和集合均支持用户自定义的轻量级扩展字段（`extensions`）。

适用存储：

- 应用程序版本标记
- 数据摄取游标
- 同步检查点
- 小型运行时提示
- 策略标记位

不适用于大载荷数据，因为它们存储在会被高频读取的元数据对象中。

## 错误模型

本 crate 暴露了统一的 `DBError` 枚举。

主要错误类别包括：

- 通用错误
- Schema 校验错误
- 存储与 I/O 错误
- 索引相关错误
- 未找到（Not Found）与已存在（Already Exists）
- 前置条件失败（Precondition Failures）
- 序列化与反序列化失败
- 数据载荷过大（Payload Too Large）错误

错误模型保留了充分的结构化信息，便于调用方区分：

- 逻辑层面的应用问题（如集合不存在）
- 并发与条件写入冲突
- 底层对象存储的持久化失败
- 来自 Schema 或索引的校验失败

## 完整工作流示例

以下是使用 `anda_db` 构建应用程序的端到端典型流程：

```rust
use anda_db::{
	database::{AndaDB, DBConfig},
	collection::CollectionConfig,
	index::HnswConfig,
	query::{Filter, Query, RangeQuery, Search},
	schema::{AndaDBSchema, Fv, vector_from_f32},
	storage::StorageConfig,
};
use anda_object_store::MetaStoreBuilder;
use object_store::local::LocalFileSystem;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, AndaDBSchema)]
struct Memory {
	_id: u64,
	created_at: u64,
	topic: String,
	body: String,
	embedding: anda_db::schema::Vector,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	std::fs::create_dir_all("./data")?;
	let store = Arc::new(
	    MetaStoreBuilder::new(
	        LocalFileSystem::new_with_prefix("./data")?.with_fsync(true),
	        10000,
	    ).build(),
	);
	let db = AndaDB::connect(
		store,
		DBConfig {
			name: "agent_memory".into(),
			description: "Long-term agent memory".into(),
			storage: StorageConfig::default(),
			lock: None,
		},
	)
	.await?;

	let schema = Memory::schema()?;
	let memories = db
		.open_or_create_collection(
			schema,
			CollectionConfig {
				name: "memories".into(),
				description: "Semantic and lexical memory store".into(),
			},
			async |c| {
				c.create_btree_index_nx(&["created_at"]).await?;
				c.create_bm25_index_nx(&["topic", "body"]).await?;
				c.create_hnsw_index_nx(
					"embedding",
					HnswConfig {
						dimension: 4,
						..Default::default()
					},
				)
				.await?;
				Ok(())
			},
		)
		.await?;

	let memory = Memory {
		_id: 0,
		created_at: 1_713_000_000,
		topic: "rust".into(),
		body: "Rust makes long-running agent services safer.".into(),
		embedding: vector_from_f32(vec![0.1, 0.2, 0.3, 0.4]),
	};

	let id = memories.add_from(&memory).await?;

	let results: Vec<Memory> = memories
		.search_as(Query {
			search: Some(Search {
				text: Some("rust agent safety".into()),
				vector: Some(vec![0.1, 0.2, 0.3, 0.4]),
				..Default::default()
			}),
			filter: Some(Filter::Field((
				"created_at".into(),
				RangeQuery::Ge(Fv::U64(1_700_000_000)),
			))),
			limit: Some(10),
		})
		.await?;

	assert!(!results.is_empty());
	let _ = id;
	db.close().await?;
	Ok(())
}
```

## 运维指南

### 谨慎规划索引

对于高频用于过滤的字段，建立 B-Tree 索引。仅对需要参与全文检索的文本字段建立 BM25 索引。仅对维度稳定的向量字段建立 HNSW 索引。

### 控制扩展字段体积

数据库与集合的 extensions 应始终保持轻量，因为它们驻留在会被高频读取的元数据对象中。

### 制定明确的刷盘策略

对于高写入负载的 Agent 系统，推荐在以下策略中做出选择：

- 启用周期的 `auto_flush`
- 在批量摄取后执行显式 `flush`
- 在进程退出停机时显式调用 `close`

绝不要仅依赖操作系统的进程退出进行持久化。

### 优先采用 Schema Derive 模型

当应用程序已有表示记忆或知识对象的 Rust 结构体时，通过 derive `AndaDBSchema` 能够减少 Schema 漂移，并确保序列化行为与存储层严格对齐。

### 根据载荷形态调优存储

若文档与索引对象普遍较小，缓存与小对象写入机制是非常高效的默认配置。若载荷较大，应在 `StorageConfig` 中评估压缩级别与分块大小配置。

生产环境的 Collection 代码在 `src/collection/` 下被拆分为生命周期、持久化、恢复、索引操作、CRUD、查询执行及扩展字段等独立模块。对外公开的 `collection::Collection` 路径保持不变。强类型的逆序 undo 记录集中管理回滚；各索引包装器共享条件元数据提交与废弃对象退役辅助工具，同时 HNSW 保留其特有的图协议。

## 模块参考

### `database`

定义：

- `AndaDB`
- `DBConfig`
- `DBMetadata`

职责：

- 打开、创建、连接与关闭数据库
- 管理共享存储后端与打开的集合
- 协调集合的创建与删除
- 暴露数据库元数据与扩展字段

### `collection`

定义：

- `Collection`
- `CollectionConfig`
- `CollectionMetadata`
- `CollectionStats`

职责：

- 校验、插入、更新、删除与读取文档
- 创建与管理各类索引
- 执行过滤查询与混合检索
- 暴露集合统计、元数据与扩展字段

### `query`

定义：

- `Query`
- `Search`
- `Filter`
- `RRFReranker`
- 重新导出的 `RangeQuery`

职责：

- 独立于存储底层表达检索意图
- 支持混合检索与递归过滤条件组合

### `index`

定义底层工作区引擎的索引门面（Façade），包含用于自定义索引值提取的 `IndexHooks`。

### `storage`

定义：

- `Storage`
- `StorageConfig`
- `StorageMetadata`
- `StorageStats`
- `ObjectVersion`

职责：

- 编码与解码持久化对象
- 管理对象版本与条件写入
- 暴露缓存读取与写入辅助方法
- 追踪存储层指标与检查点

### `schema`

重新导出 `anda_db_schema` 的类型系统，包括：

- 字段类型与字段值
- Schema 构建器与校验
- 派生 Schema 支持
- 文档转换辅助工具

## 与工作区其他 Crate 的关系

`anda_db` crate 是嵌入式核心。工作区内的其他 crate 构建于其上：

- `anda_db_server`：暴露 HTTP RPC 接口
- `anda_db_shard_proxy`：提供分片与多租户路由能力
- `anda_cognitive_nexus`：构建高层知识工作流
- `anda_kip`：定义相邻组件使用的通信协议层

若仅需在 Rust 进程内使用嵌入式记忆数据库，直接引用 `anda_db` 即可。

## 总结

AndaDB 本质上是一个面向 AI 记忆系统优化的、具备 Schema 感知能力、支持多种索引的嵌入式文档存储引擎。其核心优势不仅在于持久化存储数据，更在于它将词法检索、结构化过滤和语义检索深度整合在写入路径中，同时保持作为普通 Rust 库的轻量化嵌入式部署形态。

对于 Agent 开发者而言，这种组合构成了其核心价值：单一的集合模型、统一的持久化层，以及能够无缝融合为单一记忆访问范式的多路检索能力。
