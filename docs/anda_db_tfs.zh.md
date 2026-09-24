# anda_db_tfs - AndaDB 全文检索引擎

[English](anda_db_tfs.md)

`anda_db_tfs` 是 [AndaDB](../README.md) 的嵌入式全文检索组件。它实现了经典的 **Okapi BM25** 排序算法，专为 AI 智能体的长期文本记忆场景设计。纯 Rust 编写、线程安全且依赖极少。它既可独立使用，也可由上层数据库直接作为 `anda_db::BM25` 索引复用。

---

## 1. 设计目标

| 目标 | 实现机制 |
| --- | --- |
| 嵌入式、零外部服务 | 纯 Rust 库；无需部署 Elasticsearch 或独立的 Tantivy 进程 |
| 中英文混合友好 | 可插拔的 `Tokenizer` 管道；内置 Porter 词干提取与 Jieba 中文分词 |
| 高并发读写 | `DashMap` 配合原子计数器；支持多线程并发执行 `insert` / `remove` / `search` |
| 增量持久化 | 倒排索引按**分桶 (buckets)** 切分；仅持久化变更过的脏桶 |
| 内存占用小 | 采用 `Vec`、`FxHashMap` 与紧凑 CBOR 编码 |
| 丰富的布尔查询 | 支持带括号的 `AND / OR / NOT` 语法，满足智能体灵活检索需求 |

---

## 2. 算法原理：Okapi BM25 概览

对于包含多个词项的查询 $q$，文档 $d$ 的 BM25 得分为：

$$
\text{score}(d, q) \;=\; \sum_{t \in q} \text{idf}(t) \cdot \frac{tf_{t,d}\,(k_1 + 1)}{tf_{t,d} + k_1\!\left(1 - b + b \cdot \dfrac{|d|}{\text{avgdl}}\right)}
$$

- $tf_{t,d}$：词项 $t$ 在文档 $d$ 中的出现频次 (term frequency)。
- $|d|$：文档 $d$ 的分词总数；$\text{avgdl}$ 为整个语料库的平均文档长度。
- $\text{idf}(t) = \ln\!\left(1 + \dfrac{N - df_t + 0.5}{df_t + 0.5}\right)$：经典 Okapi IDF 平滑公式。
- 超参数 $k_1$ 与 $b$ 通过 `BM25Params` 配置，默认值分别为 `k1=1.2` 与 `b=0.75`。

在评分前，库会对用户传入的参数进行防御性区间限制：$k_1$ 截断至 $[0, 1000]$，$b$ 截断至 $[0, 1]$；非有限浮点数自动回退至默认值，避免产生 `NaN` 或 `inf`。

---

## 3. 内存核心数据结构

```text
BM25Index
├── doc_tokens       DashMap<doc_id, token_count>           // 文档长度表
├── postings         DashMap<token, (bucket_id, Vec<(doc_id, tf)>)>  // 倒排索引
├── buckets          DashMap<bucket_id, Bucket>             // 分桶分片元数据
├── metadata         RwLock<BM25Metadata>                   // 索引名称 / 配置 / 统计信息
└── atomic counters  max_bucket_id, max_document_id, total_tokens, ...
```

- **倒排项 `(bucket_id, Vec<(doc_id, tf)>)`**：`bucket_id` 标识当前持有该词项的分桶；列表中每个已索引文档恰好一条记录，因此列表长度就是该词项的文档频率。`remove` 维持这一点：传入文本未能覆盖该文档的全部条目时，会按 ID 清扫其余条目。缺失文档的残留条目只可能出现在早期版本写出的数据中，加载时会被剔除。
- **Bucket (分桶)**：可独立序列化的单元，包含一组词项及其覆盖的 `doc_ids`。分桶通过双版本计数器追踪脏状态：

  | 字段 | 含义 |
  | --- | --- |
  | `dirty_version` | 每次发生变更时自增 |
  | `saved_version` | 最近一次成功持久化的版本号 |
  | `size` | 当前 CBOR 编码下的预估字节大小 |
  | `tokens` | 该分桶拥有的词项列表 |
  | `doc_ids` | 该分桶见过的文档 ID 集合 |

`is_dirty()` 当且仅当 `dirty_version > saved_version` 时成立。只有 manifest 提交成功后，flush 才会把捕获的脏版本记为已保存；失败后分桶仍可重试。调用方必须在整个 flush 期间排除变更、压缩和其他 flush。版本记账并不意味着可以安全地在 flush 期间并发修改。

---

## 4. 分桶切分策略

`BM25Config.bucket_overload_size` 是单个分桶对象序列化大小的**软限制目标**，默认值为 `512 KiB`。在执行 `insert` 时：

1. 已有倒排表的词项保持留在原有分桶中；该分桶仅追加新条目（若该文档首次落入该桶，还需记录文档长度条目）。
2. 全新词项若当前末尾分桶为空或预估大小在加入后仍 `< limit`（包含新文档长度记录），则放入末尾分桶。
3. 否则推进末尾分桶并重试。词项的倒排表与最终的分桶注册在桶锁保护下原子发布；已有倒排表绝不会出现临时的中间所有者。每个词项仅放置一次，无需反复移动待处理词项列表。

在两次 flush 之间，`size` 为累加各项变更得到的估算值。每次 flush 与重新加载均会将其替换为序列化桶对象的精确字节长度（包含该对象携带的逐文档分词计数），从而防止估算值长期漂移。Flush 过程同时使分桶的 `doc_ids` 提示变为完全精确。

注意：**已注册在分桶中的词项**绝不会因后续的 `insert` 而被移动。这避免了每次插入都创建新分桶的退化场景（见回归测试 `test_no_excessive_small_buckets`），但也意味着大小限制仅在首次放置词项时生效：后续只要有文档包含该词项，该分桶就会持续增长。高频词（如未在分词器中过滤的停用词）会导致所在分桶随语料库膨胀，且涉及该桶的每次 flush 都必须重写整个分桶。在分词器链中配置停用词过滤是最根本的解决手段；`compact_buckets` 会重排整个词项，但绝不会将单个词项切分至多个分桶。

### 4.1 空间碎片整理：`compact_buckets()`

针对旧版本遗留的碎片化索引，或因大量 `remove` 操作而产生稀疏空洞的分桶，可调用 `compact_buckets()` 执行单遍全量重排：

- 扫描所有 `postings` 并预估每个词项的 CBOR 序列化体积。
- 采用 **Best-Fit-Decreasing** 装箱算法，配合 `BTreeMap<remaining_capacity, bin_index>` 实现 `O(n log n)` 复杂度的重排。
- 重建 `buckets`：分桶 ID 从 `0` 重新连续编号，所有分桶标为 dirty，下一次 `flush` 会全量重写磁盘布局。

返回的 `(old_count, new_count)` 二元组可用于监控。并发调用 `insert` / `remove` 是线程安全的：`compact_buckets` 独占获取索引内部的变更门，任何写操作都无法观察到或向重建中的分桶写入数据。但它绝不能与 `flush` 并发交叠执行（详见 [§5 并发模型](#5-并发模型)）。

装箱时对桶 owner 使用固定的保守 CBOR 宽度，旧桶编号不会影响新布局。
没有数据变更时，重复压缩（包括重新加载后）不会改变版本或脏状态。
维护还会收缩容量超过长度四倍且超过 64 条记录的倒排列表，保留两倍存活长度的空间。
单桶索引同样执行这项内存维护；仅回收容量不会触发磁盘写入。

---

## 5. 并发模型

- 插入、移除、清除 (purge) 与搜索支持多线程并发调用。针对同一文档 ID 的写操作通过内部条带锁进行串行化，条带跨越文档成员资格、倒排表与分桶记账。批量 purge 按照去重后的条带编号升序依次获取条带锁。
- 加锁顺序为：变更门 (mutation gate) → 文档条带 (stripes) → 内部 maps。倒排表创建与分桶注册共享同一个桶锁。移除操作在注销词项前，会在持有该桶锁的前提下重新校验倒排表的所有权归属。
- 空间压缩 (Compaction) 独占持有变更门。调用方在执行异步 flush 的整个生命周期内，必须将 flush 与变更、压缩及其他 flush 严格互斥；AndaDB 的 Collection 独占操作租约提供了这一保证。每个持久化索引保持单一写入者是部署契约。
- 搜索操作为尽力而为的并发读取，非事务性快照。单次查询仅对语料库统计数据采样一次；计数器在写操作完成后收敛。系统不维护易失效的全局平均长度缓存。
- 纯元数据外壳对象通过 `metadata()` 与 `stats()` 暴露已持久化的统计数据，而 `len()` 返回当前已加载的文档数。部分加载或加载失败的索引拒绝插入和 flush；remove/purge 与压缩在此状态下为空操作。`is_fully_loaded()` 指示索引是否可写。
- Flush 捕获脏版本号与元数据，随后逐个序列化并上传分桶。额外的内存消耗受限于最大的单个脏桶。没有内部 map 锁会跨越 await，且在 manifest 提交成功前绝不将分桶标记为 clean。
- Top-k 选取采用 `select_nth_unstable_by` 随后对选出的结果排序：复杂度为 `O(matches + k log k)`。

---

## 6. 持久化存储布局

物理上，一个索引由**一个元数据 blob** 与**多个分桶 blob** 组成。上层应用可将其自由存储于本地文件、对象存储、KV 存储或任意其他后端。

```text
<root>/
  metadata                # CBOR 编码的 BM25Metadata（携带分桶清单 manifest）
  buckets/
    b_0_7                 # CBOR 编码的 BucketOwned，按 (bucket_id, generation) 寻址
    b_1_9
    b_2                   # 旧版（无 manifest 时代）位于 generation 0 的只读对象
    ...
```

分桶对象**一旦被引用即不可变**：每次 flush 将被替换的分桶写入全新的 `(bucket_id, generation)` 对象，元数据中的 manifest 是加载器唯一的权威数据源。

### 6.1 `BM25Metadata`

```rust
pub struct BM25Metadata {
    pub name: String,
    pub config: BM25Config,
    pub stats: BM25Stats,          // 版本号 / 计数 / 时间戳 / 水位
    /// 分桶清单：bucket_id -> 当前持久化对象的 generation 代数
    /// (0 = 旧版无后缀对象)。加载旧版数据时该字段为空。
    pub buckets: BTreeMap<u32, u64>,
}
```

`flush` 使用 `last_saved_version` 作为幂等防线：当 `stats.version` 未递增且无任何分桶处于 dirty 状态时，立即返回 `saved == false`。

### 6.2 分桶 CBOR 格式

```rust
struct BucketOwned {
    #[serde(rename = "p")] postings:  FxHashMap<String, PostingValue>,
    #[serde(rename = "d")] doc_tokens: FxHashMap<u64,    usize>,
}
```

简短的字段名 (`"p"`, `"d"`) 旨在缩减 CBOR 序列化体积。注意 `PostingValue` 内部内嵌了 `bucket_id`，因此加载无需任何额外的记账开销。

### 6.3 增量 Flush 流程

```rust
let outcome = index.flush(metadata_writer, now_ms, |object, bytes| {
    // 将 bytes 写入对应的存储对象 (object.bucket_id, object.generation)
    std::future::ready(Ok(()))
}).await?;
for object in &outcome.obsolete {
    // 尽力而为：删除新清单中不再引用的废弃对象
}
```

- `flush` 捕获脏版本及下一次清单，然后逐一序列化并上传脏分桶。仅属于该分桶的倒排表会被写出；文档长度直接从这些倒排表中推导得出。除尾桶（`max_bucket_id`）外，没有词项的分桶不再写成空对象，而是移出清单，其旧对象（包括早期版本写出的空对象）作为废弃对象返回。新词项只会进入尾桶，这些分桶否则会一直为空并在每次打开时被读取。保留尾桶是为了让清单永不为空。
- 每个脏分桶写入以 `(bucket_id, generation)` 命名的**新对象**；generation 即为本次 flush 的元数据版本，因此已提交的对象绝不会就地修改。
- 映射所有活跃分桶 ID 及其当前代数的元数据在**最后**写入。该单次写入即为原子提交点：在此之前的崩溃或错误会保留上一快照完好无损（新写入的对象作为未引用的垃圾数据存在）；写入成功之后，被替换的旧对象成为垃圾数据，并在 `FlushOutcome::obsolete` 中返回以供尽力而为清理。
- `compact_buckets` 无需特殊顺序：重排后的布局随着下一次 manifest 提交而原子对外可见，所有压缩前的旧对象都会被标记为废弃。

### 6.4 启动与部分加载

```rust
let idx = BM25Index::load_all(tokenizer, metadata_reader, async |object| {
    Ok(read_bucket(object).await?) // Ok(None) 表示暂不加载该分桶
}).await?;
```

- `load_metadata` 仅恢复元数据，适用于仅需读取统计信息的轻量级场景。
- 存在 manifest 时，`load_buckets` 严格读取清单所引用的 `(bucket_id, generation)` 对象。无清单的旧版元数据将回退至在 generation `0` 上扫描 `0..=max_bucket_id`（无后缀的旧对象），首次 flush 会自动将存储升级为 manifest 格式。
- `load_all` / `load_buckets` 允许通过返回 `Ok(None)` 进行显式的只读部分加载。增量加载会保留缺失的分桶列表；全部加载补齐后索引方可转为可写。失败或取消的加载维持只读状态，已加载的文档长度被保留以便重试。
- 生产环境启动推荐使用 `load_all_strict`；同时提供 `load_buckets_strict`。清单引用的对象若缺失将直接导致加载失败。旧版布局在扫描 bucket ID 时依然容忍空洞。
- 若同一词项出现在多个已加载的分桶中（仅可能发生在旧版多阶段 flush 写入的历史数据中），编号较大的分桶获胜。加载器会从较旧的分桶中剔除该词项，依据胜出的倒排表重建分桶的文档 ID 集合，并将修复过的分桶标记为 dirty，以便在下一次 flush 时清理磁盘上的陈旧归属。

---

## 7. 查询语法与高级检索

`search_advanced(query, top_k, params)` 支持布尔表达式，`QueryType::parse` 将输入字符串转换为语法树 AST：

```text
expr     := or_expr
or_expr  := and_expr ( " OR " and_expr )*
and_expr := not_expr ( " AND " not_expr )*
not_expr := "NOT " not_expr | term
term     := chunk ( whitespace chunk )*
chunk    := "(" or_expr ")" | word
```

优先级顺序为：`OR < AND < NOT`。操作符大小写敏感且两端必须带有空格（`NOT` 紧跟空格）：`a and b` 被视为三个普通单词，而 `NOT NOT a` 构成两层否定（按 `a` 求值）。核心特性：

- **多词项查询默认为 OR**：在 `search` 与 `search_advanced` 中，`"quick fox"` 与 `"quick OR fox"` 返回完全相同的结果。括号组与普通词并列时同样是 OR 的一个操作数：`rust (async AND tokio)` 即 `rust OR (async AND tokio)`。
- **得分合并机制**：`AND` 累加各子查询的 BM25 分数；`OR` 同样累加；`NOT` 生成得分为零的占位集合仅用于过滤，在 `AND` 上下文中它负责从结果集中**剔除**匹配项。
- **解析健壮性**：未配对的括号不会触发 panic，而是作为普通字符处理，确保直接透传用户输入时的安全性。空的分组等价于无匹配项的空 `OR`，因此 `a AND ()` 返回空结果。
- **分词归属与大小写**：解析器严格保留操作数的大小写。每个布尔操作数通过单次查询专属的分词器副本独立分词。词项的 OR 操作合并并去重分词后的 token，而非原始文本。自定义上下文敏感分词器因此能够区分纯文本检索与布尔词项表达式。
- **候选集执行优化**：AND 优先求值高选择性操作数，并将后续计算限制在幸存候选集内。每条倒排表单遍计分；DF 取倒排表长度，因此限定候选集的评分仍使用全局 DF/IDF。NOT 过滤器只进行集合成员判定，不浪费算力计算被丢弃项的 BM25 分数。IDF 在单次查询内缓存，并使用 `ln_1p` 确保数值稳定性。
- **`NOT` 补集防线**：对整个索引取补集（`NOT a`、`a OR NOT b`、`NOT a AND NOT b`）受 10,000 篇文档上限约束。与正向操作数同处一个 `AND` 中，或在 `try_search_in_ids` 下，NOT 相对已匹配的文档取补集，这些文档本来就在内存中，因此 `a AND NOT (b AND NOT c)` 不受限制。前导的成对 NOT 在规划阶段抵消：`hello AND NOT (NOT world)` 即 `hello AND world`，并参与计分。`try_search_advanced` 会暴露此类资源错误；`search_advanced` 遇到错误则返回空结果。严格解析会拒绝括号/NOT 嵌套预算耗尽的输入，而非静默曲解查询意图。
- **多字节安全**：分隔符 `" AND "` 与 `" OR "` 均为 ASCII 字符，在 UTF-8 字节流扫描下绝对安全。中日韩 (CJK) 混合文本无需特殊处理。

示例：

```rust
let hits = index.search_advanced(
    "(brown AND fox) AND NOT (rare OR sleeps)",
    10,
    None,
);
```

OR 归一化会展平嵌套的 OR，即使混有 AND/NOT 分支，也统一合并直接的词项操作数。
同一个 OR 内重复的归一化 token 仅计分一次，冗余的 OR 括号不会改变评分；复杂分支仍贡献各自的分数。
空候选集在读取倒排表前直接返回；非空候选检索继续使用全局 DF/IDF。

---

## 8. 分词器体系

所有分词器均实现 `tantivy_tokenizer_api::Tokenizer`，并通过 `TokenizerChain` 进行组合。核心 API：

| 函数 | 作用 |
| --- | --- |
| `TokenizerChain::builder(base).filter(f1).filter(f2).build()` | 构建分词流水线管道 |
| `default_tokenizer()` | `SimpleTokenizer -> RemoveLongFilter(32) -> LowerCaser -> Stemmer`（需开启 `tantivy` 特性） |
| `jieba_tokenizer()` | 在上述流水线前置 `JiebaMergeFilter`（需开启 `tantivy-jieba` 特性） |
| `collect_tokens(tok, text, inclusive)` | 独立分词辅助函数，用于上层预处理或非 BM25 匹配统计 |
| `flat_full_text_search(tok, query, text)` | 无需构建索引的朴素单文本匹配，适合短文本过滤 |
| `detect_script(text)` | 根据字符频次检测主导语言文字 (`Latin / Cyrillic / Arabic / Cjk / Other`) |

### 8.1 `JiebaMergeFilter`

在多语言混合文本中，`SimpleTokenizer` 会将连续的中文字符聚合成单个 token。通用的公开 `JiebaMergeFilter` 对 CJK 跨度重新切词，并在全局顺序上对齐 `(offset_from, offset_to, position, text)`。内置的 `jieba_tokenizer()` 采用相同的切词与过滤逻辑，但每次仅对一个 SimpleTokenizer 跨度进行缓冲和排序。回归测试将完整的分词流（包含 offset 与 position）与通用链进行比对。两者均能保证：

- 中文文本切词正确（`"北京市东城区长安街"` -> `北京`, `东城区`, `长安街`）；
- 英文、俄文、阿拉伯文等其他语种保留主流水线的词干提取与小写转换结果；
- 生成的 `TokenStream` 严格单调递增，确保下游 BM25 消费完全正确。

`RemoveLongFilter::limit(32)` 保留 UTF-8 字节长度**小于 32 字节**的词，而非 32 个 Unicode 字符。`collect_tokens` 仅在词项首次出现在词频表中时复制该词。

> **注意**：`collect_tokens` 会过滤掉字节长度 `token.text.len() <= 1` 的单字节 token，从而去除标点符号与孤立的单个英文字母。单个汉字不受影响，因为汉字的 UTF-8 编码至少占 3 个字节。

---

## 9. 错误处理

```rust
pub enum BM25Error {
    Generic       { name: String, source: BoxError },
    Serialization { name: String, source: BoxError },
    NotFound      { name: String, id: u64 },
    AlreadyExists { name: String, id: u64 },
    TokenizeFailed{ name: String, id: u64, text: String },
}
```

`Generic` 包含 I/O 闭包返回的错误以及 `try_search_advanced` 的查询保护错误；`Serialization` 包装 `cbor2` 序列化异常；`AlreadyExists` 与 `TokenizeFailed` 发生在 `insert` 期间（`TokenizeFailed` 最多截取文档前 256 字节文本）；本 crate 自身不会主动抛出 `NotFound`，保留给上层 API 进行幂等性检查。

---

## 10. 配置与性能调优

```rust
let cfg = BM25Config {
    bm25: BM25Params { k1: 1.5, b: 0.6 },
    bucket_overload_size: 1024 * 1024,     // 1 MiB
};
let index = BM25Index::new("mem".into(), jieba_tokenizer(), Some(cfg));
```

调优建议：

- **$k_1 \in [1.2, 2.0]$**：文档较短且关键词密度高时，略微调大可放大高频词的影响。
- **$b \in [0.5, 0.9]$**：文档长度差异显著时选择 `0.75` 到 `0.9`；文档长度较为均等时建议下调至 `0.5` 附近。
- **`bucket_overload_size`**：
  - 较小（如 `64KiB`）：增量 flush 的 I/O 放大更低，适合高频 checkpoint。
  - 较大（如 `2MiB`）：总分桶数更少，全量加载速度更快，适合读多写少的智能体记忆库。
- **定期碎片整理**：在后台任务中定期调用 `compact_buckets()` 保持分桶数稳定。它对并发的 `insert` / `remove` 是安全的（内部自动加门控）；调度时应确保其不与 `flush` 发生交叠。

---

## 11. 测试与性能基准

测试覆盖范围：

- `cargo test -p anda_db_tfs --all-features`
- `cargo test -p anda_db_tfs --no-default-features`
- `cargo test -p anda_db_tfs`
- `cargo test -p anda_db_tfs --features full --example tfs_demo`
- 覆盖 insert / remove / search 正确性、分桶序列化与部分加载、压缩后结果不变性、回归测试 `test_no_excessive_small_buckets`、UTF-8 查询解析等。

基准命令：`cargo bench -p anda_db_tfs --features full --bench tfs_tokenizer` 与 `cargo bench -p anda_db_tfs --features full --bench tfs_index`。后者涵盖评分、数据变更、load/flush、压缩及中英文混合文本。可复现的性能对比与存储架构权衡参见 [测量数据与审查清单](anda_db_tfs_review.zh.md)。

---

## 12. 快速上手

```rust
use anda_db_tfs::{BM25Index, default_tokenizer};

let idx = BM25Index::new("notes".into(), default_tokenizer(), None);
idx.insert(1, "The quick brown fox jumps over the lazy dog", 0).unwrap();
idx.insert(2, "A fast brown fox runs past the lazy dog",     0).unwrap();
idx.insert(3, "The lazy dog sleeps all day",                 0).unwrap();

for (id, score) in idx.search("fox", 10, None) {
    println!("doc {id}: {score:.3}");
}

for (id, score) in idx.search_advanced("(brown AND fox) AND NOT sleeps", 10, None) {
    println!("doc {id}: {score:.3}");
}
```

持久化到本地文件系统可直接复用经过完整测试的 [`write_atomic` 辅助函数](../rs/anda_db_tfs/examples/support/atomic_file.rs)。它在目标文件旁创建临时文件，写入并落盘同步，随后执行原子重命名，在 Unix 平台上还会对目录执行 fsync。若崩溃进程残留了第一个临时文件名，该辅助函数会自动递增序号直至创建全新文件；绝不会误删可能属于其他写进程的现有临时文件。完整的可运行适配器参见 [`tfs_demo`](../rs/anda_db_tfs/examples/tfs_demo.rs)。

```rust
use std::{fs, path::{Path, PathBuf}};
use anda_db_tfs::{BoxError, BucketObject};
// 在应用中引入上述链接中的 write_atomic 函数

fn bucket_path(object: BucketObject) -> PathBuf {
    if object.generation == 0 {
        format!("./idx/b_{}.cbor", object.bucket_id).into()
    } else {
        format!("./idx/b_{}_{}.cbor", object.bucket_id, object.generation).into()
    }
}

fs::create_dir_all("./idx")?;
let outcome = idx.flush_with(
    now_ms,
    |bytes| std::future::ready(
        write_atomic(Path::new("./idx/metadata.cbor"), &bytes)
            .map_err(BoxError::from)
    ),
    |object, bytes| std::future::ready(
        write_atomic(&bucket_path(object), &bytes).map_err(BoxError::from)
    ),
).await?;
for object in outcome.obsolete {
    let _ = fs::remove_file(bucket_path(object));
}

let idx = BM25Index::load_all_strict(
    default_tokenizer(), fs::File::open("./idx/metadata.cbor")?,
    async |object| match fs::read(bucket_path(object)) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    },
).await?;
```

切勿在 flush 之前直接通过 `File::create` 打开已提交的元数据文件：无脏数据的 clean flush 不会触发回调，这会导致原文件被错误截断为空。`flush<W: Write>` 负责写入并刷新其 writer，但自身无法提供原子替换或文件系统物理落盘。若元数据回调返回了不确定结果，必须从持久化状态中重新打开。

---

## 13. 使用注意事项

1. **移除操作应传入原始文本**：`remove(id, text, now_ms)` 对文本重新分词以定位该文档的倒排条目。若这些条目的词频之和与该文档的词项数不符（文本错误或分词器已更换），`remove` 会按 ID 清扫所有倒排表：结果仍然正确，但代价是 $O(\text{索引大小})$。若文档确实无法找回（例如文档内容已丢失的灾难恢复场景），应改用 `purge_ids(&BTreeSet<u64>, now_ms)`：它针对传入的 ID 集合单遍扫描所有倒排表，从 `doc_tokens` 与 `total_tokens` 中扣除对应 ID，并将受影响的分桶标记为 dirty。这是维护级别的 $O(\text{索引大小})$ 操作，应一次性传入所有废弃 ID，切忌在循环中逐个调用。
2. **`top_k = 0`**：保留用于 API 兼容。直接返回空集合且不触发内部排序。
3. **Flush 协调**：Crate 内部不负责 flush 调度的串行化。调用方必须保证 flush 绝不与变更、压缩或其他 flush 交叠（`anda_db` 的 `Collection` 已在更高层保证了该契约）；持久化索引保持单一写进程是架构部署契约。
4. **部分加载下的检索语义**：若 `load_buckets` 跳过了某个分桶，属于该分桶的词项将不可用。已加载的分桶同时携带有用于倒排表评分的文档长度，因此即便利载分桶被跳过，`len()` 仍可能统计到被已加载词项涉及的每篇文档。检索结果严格属于已加载倒排表的自然子集。
5. **纯嵌入式架构**：本库专为在 AndaDB 进程内直接嵌入而设计，不提供 HTTP 或 gRPC 服务。如需远程访问，请使用 `anda_db_server` 或 `anda_db_shard_proxy`。

---

## 14. 参考资料

- Robertson & Zaragoza. *The Probabilistic Relevance Framework: BM25 and Beyond*, 2009.
- [`tantivy_tokenizer_api`](https://docs.rs/tantivy-tokenizer-api) - 分词器 trait 定义。
- 更多回归测试与设计讨论，请参阅 `rs/anda_db_tfs/src/bm25/tests.rs`、`src/bm25/regression_tests.rs` 与 `tests/regressions.rs`，以及关于分桶策略的 [anda_db_btree.zh.md](anda_db_btree.zh.md)。
