# rs/anda_db 审查清单与修复结果

[English](anda_db_review.md)

## 后续审查修复完成（2026-09-23）

本次处理了 5 项正确性/API 问题、4 项性能优化和 2 项内部简化。持久化对象格式和
公开方法签名保持不变；集合 Schema 升级现在会强制检查“退休索引字段前先删除索引”的既有约束。

| 项目 | 实现与验证 |
| --- | --- |
| 内部对象容量 | 索引对象和集合 ID 位图采用对称的 `max(256 MiB, max_small_object_size)` 读写预算。默认配置下 10 万条共用词文本的 BM25 可落盘、重开；另以小文档上限覆盖 B-Tree/BM25 的压缩和非压缩路径。 |
| 删除索引字段 | 在保存新 Schema 前报错，覆盖单字段/复合 B-Tree、多字段 BM25 和 HNSW；先按旧 Schema 移除索引后可正常升级。 |
| 超限更新 | 在日志和索引变更前完成文档编码与大小检查，条件 PUT 复用编码结果；拒绝后句柄仍可继续写入。 |
| 可选向量 | 支持 `Option<Vector>` 建索引、回填、后补和清空，并验证重开行为。 |
| 原始文档构造 | `new_document()` 初始化 `_id` 占位值；业务必填字段的校验保持有效。 |
| 同值更新 | 比较规范化字段及 BM25/HNSW 派生值；跳过未变化索引，整文档无变化时不执行持久化。 |
| 高频键分页 | 索引等值加 ID 条件只扫描一次 posting，以有界空间保留首/尾页；覆盖删除导致 posting 重排的情况。 |
| ID 持久化 | 成员脏标记独立于集合元数据；纯 update/extension 检查点不再重写 `ids.cbor`。 |
| 恢复 I/O | 在配置并发度内预取不同文档的当前内容，按 ID 顺序应用修复；门控测试验证并发读取及其上限。 |
| ID 状态封装 | 有序集合、bitmap 和脏标记由同一把锁保护，通过统一方法同步变更。 |
| 初始化简化 | 创建和重开共享运行态初始化函数，各自保留存储流程。 |

验证依据：[集合回归用例](../rs/anda_db/tests/core_review.rs)、
[存储预算回归](../rs/anda_db/src/storage.rs)、[三轮配对基准](benchmarks/anda_db_core_2026-09-23/README.md)。
现有崩溃恢复、取消和格式兼容测试继续覆盖原合同，旧夹具未重新生成。

验证：全工作区 all-features 编译和测试通过（**1,765 passed，0 failed，1 项既有
fixture 生成测试忽略**）；全工作区 all-targets/all-features Clippy 在 `-D warnings`
下通过。本次新增 12 个回归测试，并检查格式、agent 文档一致性和本地文档链接。
`db_demo` 在临时目录的真实文件系统与 MetaStore 上连续运行两次，验证首次创建、
重新打开及 Jieba 检索。

三轮中位数：高频键分页由 9.939 ms 降至 3.658 ms，额外峰值堆内存从 2,762,184 B
降至 1,176 B；30 次同值更新/刷盘的 PUT 数从约 272 次降为 0；模拟延迟下的不同文档
恢复从 1,147.834 ms 降至 383.431 ms。普通更新每个检查点减少一次位图 PUT，但其
p95 未改善。完整 posting 仍有容量上限及整体重写成本，复杂过滤器仍保留原有内存行为。

下方 9 月 6 日报告保留其原始测试数量和基准结果，作为历史验证及设计背景。

**完成情况（2026-09-06）**

清单的 **15 项修复、8 项性能工作、4 项简化工作均已处理**。未使用 subagents。
原有用户改动保留；工作区中并行完成的 HNSW 节点 CAS 修改也保持完整。

| 验证 | 结果 |
| --- | --- |
| 工作区 `cargo test --workspace --all-features` | **1,621 passed，0 failed，1 ignored**（手动夹具生成） |
| 正式审查回归 | **25 个测试通过**，含更新/删除提交前后中断、独立唯一键并发、布尔分页参考集合与 schema 映射 |
| Core、Schema、TFS 严格 Clippy（all-targets / all-features / -D warnings） | 通过 |
| 独立 Rust 消费者，object_store 0.14，真实文件后端 | 创建、写入、关闭、重开、Jieba 检索通过 |
| v0_8 / v0_11 存储夹具 | 保持可读，未重新生成夹具 |
| AGENTS.md / CLAUDE.md | 已同步并通过一致性检查 |

实现采用按原生唯一键取锁、稳定排序加锁与提交后释放；没有保留整集合串行写入的初版方案。
元数据快照区分已提交与待发布索引；恢复读取遇到临时错误直接失败；维护删除有按 ID 的日志。
业务回调中的首次操作先完成恢复，分词器安装同步到已加载 BM25。日志在内存中只保留序号。

新增运行时接口：`SearchOptions`、`search_with_options`、`search_ids_with_options`、
`set_io_concurrency(1..=64)`、`recovery_issues()`、流式读写的 `*_with_limit`。
`StorageConfig::with_cache_max_bytes` 提供显式字节预算；旧缓存配置语义保留。
默认 release 仍为 opt-level=z，另提供可选 `release-speed`（opt-level=3）。

`collection.rs` 的生产职责已拆至 `src/collection/`，公开路径保持一致；内嵌大测试模块独立成文件。
CRUD 使用类型化撤销记录，B-Tree/BM25 共用条件元数据提交和过期对象回收代码。

独立接入验证额外修正了 S04 的本地文件示例：原生 LocalFileSystem 0.14 不支持条件更新，
现已统一使用 MetaStore 包装；README、技术文档、CLAUDE/AGENTS 和本地 skill 均已对齐。

性能数据、原始结果和复跑命令见 [基准报告](benchmarks/anda_db_core_2026-09-06/README.zh.md)。
有界 OR / 主键布尔计算、选择率排序、候选集下推和有界恢复 I/O 已落地。
没有声称所有路径都加速：单路 BM25 的中位延迟未显著改善，本地文件日志清理仍受后端同步成本影响。
大块 codec 有线程切换成本，详见基准中的串行与并发对照。

使用注意：默认选择性预过滤会在候选子集内做 RRF；需要历史的“全局排名后过滤”行为时，
设置 `SearchOptions { prefilter_limit: 0, adaptive: false, ..Default::default() }`。
普通的宽范围非主键交集仍可能使用 O(匹配数) 工作内存；结果 limit 并非所有计划的内存硬上限。
这些是明确保留的取舍，不是尚未执行的工作项。

**以下为修复前审查记录，保留问题背景与验收依据。**

审查日期：2026-09-06。基于提交 `8bc6d8bf743f6f622b8be4ea1715ab7ed01a7235` 加当前工作区内容；`anda_db` 版本为 0.11.1。审查全程未使用 subagents，未修改生产实现，也未覆盖原有未提交修改。

修复前结论：存在需要修复的问题。本次用运行中的代码确认了 **15 项缺陷或 API 边界错误**，其中 6 项建议按 P1 处理、9 项按 P2 处理。另确认了 1 项 BM25 压实行为，归入优化项，不将其冒充未满足现有文档的正确性缺陷。最优先的是唯一约束、索引发布顺序、恢复扫描错误处理和路径隔离。

这里的 P1 表示应在下一次依赖相应功能的生产发布前修复；P2 表示有确定触发条件，应安排修复。优先级不是漏洞严重性评级。

**修复前的范围与验证证据**

逐一审查了 `src` 下全部 11 个 Rust 文件的生产实现，共 9,910 行，含注释和文档；结合内嵌单元测试、5 个集成测试文件、格式兼容夹具、示例、Cargo 配置和技术文档检查契约。为确认调用语义，追踪了本地 schema、B-Tree、BM25、HNSW 和对象存储依赖中相关实现。没有把此审查扩展为整个工作区的完整审计。

| 检查 | 结果 |
| --- | --- |
| `cargo test -p anda_db --all-features -- --test-threads=4` | 186 passed；1 ignored（手动生成格式夹具） |
| `cargo clippy -p anda_db --all-features --all-targets -- -D warnings` | 通过 |
| 本次专项复现 | 16/16 确认当前行为：15 项问题，1 项压实优化依据 |
| 云对象存储、真实断电、性能吞吐基准 | 本次未执行；不声称有相应验证或提升百分比 |

修复前的 [异常探针](anda_db_review_repros.rs) 作为历史证据保留。当前运行入口改为永久的 [回归套件](../rs/anda_db/tests/review_regressions.rs)：

```bash
bash docs/run_anda_db_review_repros.sh --nocapture
```

**现在通过表示修复后的行为成立。** 下文保留原问题的触发过程与实施建议；勾选表示该工作项已处理。

**优先修复清单：P1**

- [x] **B01：让唯一键的占用持续到文档提交完成。**

  位置：[crud.rs](../rs/anda_db/src/collection/crud.rs)、[btree.rs](../rs/anda_db/src/index/btree.rs)。当前更新先改变索引，再异步写文档；按文档 ID 分条带的锁不能保护其他文档对旧唯一键的抢占。

  已复现：A 的唯一键是 `x`，更新为 `y` 的文档 PUT 尚未执行时，B 成功插入 `x`；中断 A 的更新并重开后，A、B 的持久化文档都为 `x`，但唯一索引只返回其中一个。失效句柄和重放日志不能修复这个跨文档约束冲突。

  执行：先实现覆盖旧键和新键的唯一键预留，持有到文档写入结果确定；按稳定顺序加锁，避免多唯一索引死锁。等待锁的操作醒来后重新检查句柄状态。可先用集合级串行提交作为较简单的正确性修复，再用基准决定是否细化。恢复遇到真实唯一冲突应显式报告，不能仅打印警告后把该文档计作完整恢复。

  验收：对 update/remove 与另一文档 add/update 的交错，在存储写前、写后丢响应、中断等位置注入故障；成功返回且恢复后的文档必须满足唯一约束，或恢复显式报告可操作的冲突。探针：`repro_unique_key_released_before_document_commit`。

- [x] **B02：所有元数据写入路径都必须遵守“索引先持久化、引用后发布”。**

  位置：[persistence.rs](../rs/anda_db/src/collection/persistence.rs)、[extensions.rs](../rs/anda_db/src/collection/extensions.rs)、[index_ops.rs](../rs/anda_db/src/collection/index_ops.rs)。`flush_inner` 的顺序已经正确，但 `store_metadata_unclaimed` 写的是包含未提交索引注册信息的完整元数据。

  已复现：已有文档且 checkpoint 已推进；打开回调创建 B-Tree 索引、调用 `save_extension`、随后返回错误。再次打开时索引已登记，`_nx` 不再回填，而磁盘索引还是空的；文档能 get，却无法通过索引找到。删除另一个索引也会走同一元数据发布入口。

  执行：区分已提交的索引注册集合和待发布变更；扩展写入只携带已提交索引描述，或在发布新增索引引用前完成必要的索引持久化。不能仅靠“不推进 last_saved_version”保证安全。

  验收：B-Tree、BM25、HNSW 各覆盖“回填→扩展写/删除另一索引→回调错误或中断→重开”，所有既有文档仍可检索。探针：`repro_extension_publishes_unflushed_new_index`。

- [x] **B03：恢复扫描遇到临时读取失败时，不得跨过失败 ID 提交 checkpoint。**

  位置：[recovery.rs](../rs/anda_db/src/collection/recovery.rs)。`auto_repair_indexes` 将 NotFound 以外的错误也记录后跳过；后续正常文档触发 flush，checkpoint 会越过读取失败的文档。

  已复现：两个已成功 add、尚未 checkpoint 的文档，恢复时仅让 `data/1.cbor` 的 GET 临时失败一次。打开成功且 checkpoint 变成 2；再次打开仍只能看到文档 2，而文档 1 的对象完好存在。必须手动 `reconcile_storage` 才找回。

  执行：区分确定不存在、确定损坏和临时 I/O 失败。临时失败应使本次恢复失败，或持久化待重试 ID，并阻止相关恢复边界被确认；损坏对象若允许跳过，也要保留可查询的诊断和后续修复入口。

  验收：对恢复窗口中的任意 ID 注入一次 GET/读取流失败，清除故障后重试即可自动找回所有正常对象，无需人工全量扫描。探针：`repro_transient_recovery_read_failure_is_checkpointed_past`。

- [x] **B04：重开时将用户设置的分词器应用到已加载的 BM25 索引。**

  位置：[lifecycle.rs](../rs/anda_db/src/collection/lifecycle.rs)、[index_ops.rs](../rs/anda_db/src/collection/index_ops.rs)、[collection.rs](../rs/anda_db/src/collection.rs)。索引先使用默认分词器 bootstrap，随后回调中的 `set_tokenizer` 只替换 Collection 字段，不更新 BM25 内部的分词器。

  已复现：按官方示例的方式设置 Jieba、创建索引、插入“南京市长江大桥”。重开前可搜索，重开并再次设置 Jieba 后，同样的全文查询为空；`collection.tokenize` 却仍显示正确分词。

  执行：让打开配置在 bootstrap 前提供分词器，或让 setter 安全地更新所有已加载索引的运行时分词器。真实的分词策略变更应触发重建或明确拒绝，避免同一索引混用两种词项语义。

  验收：中文、多词查询及重开后的新增、更新、删除结果一致；不仅检查 `tokenize()`。探针：`repro_custom_tokenizer_not_restored_to_loaded_bm25`。

- [x] **B05：迁移/恢复数据库前缀后，所有集合路径必须跟随打开路径。**

  位置：[database.rs](../rs/anda_db/src/database.rs)、[lifecycle.rs](../rs/anda_db/src/collection/lifecycle.rs)。Storage 已采用调用方的新路径，但 AndaDB 的 `name` 仍取自旧 `db_meta.cbor`；集合使用 `db.name()` 拼接路径。

  已复现：把 `reviewdb/` 完整复制到 `restored/` 后，以 `restored` 打开并新增文档，实际写入 `reviewdb/docs/data/2.cbor`。若旧前缀不存在，集合打不开；若旧前缀仍存在，会读写原库。删除路径也依赖同一旧名称。

  执行：由调用方路径确定存储命名空间，分离展示名称与存储前缀；协调 DB 元数据中的名称，不让旧元数据重定向操作。若不支持改名前缀，至少应在打开时明确拒绝，不能静默访问原库。

  验收：原前缀存在/不存在两种恢复场景，读、增、更新、删除集合都只访问目标前缀；用记录存储调用的包装器断言原前缀无写入。探针：`repro_relocated_database_uses_original_collection_prefix`。

- [x] **B06：`create_btree_index_nx` 只忽略“目标索引确实已存在”。**

  位置：[index_ops.rs](../rs/anda_db/src/collection/index_ops.rs)。它无条件吞掉整个创建过程返回的 `DBError::AlreadyExists`，但唯一索引回填冲突使用同一种错误。

  已复现：先存两个相同 key 的文档，再执行 `_nx` 创建唯一索引；调用返回成功，实际没有注册索引，第三个重复 key 仍可插入。组合唯一索引也受这一错误分类方式影响。

  执行：创建前检查目标索引，或仅在错误后确认目标索引已完整加载且配置匹配时返回成功；传播回填产生的重复键错误。错误中保留冲突键和文档 ID。

  验收：既有索引的重复初始化成功；既有文档违反唯一约束时初始化失败且不留下半成品。探针：`repro_create_btree_index_nx_swallows_duplicate_backfill_failure`。

**进一步修复清单：P2**

- [x] **B07：区分文档大小上限与意图日志大小上限。**

  位置：[recovery.rs](../rs/anda_db/src/collection/recovery.rs)、[storage.rs](../rs/anda_db/src/storage.rs)。日志同时保存旧、新两份完整文档，却走同一个 `max_small_object_size` 检查。默认上限实际为 2,048,000 字节（配置注释称为 2 MiB）。1,100,000 字符的 body 可以插入，但仅修改短 key 就会因日志超过上限而失败。

  执行：为内部意图定义足够且受控的独立限额，或保存差量/索引前后映像；同时考虑 CBOR 包装开销与兼容格式。验收：接近文档上限的合法文档仍可更新、删除并恢复。探针：`repro_accepted_large_document_cannot_be_updated`。

- [x] **B08：开放业务回调前，先保证 ID 分配器不会碰到未恢复对象。**

  位置：[lifecycle.rs](../rs/anda_db/src/collection/lifecycle.rs)。回调发生在 replay/repair 之前，分配器从旧 metadata max 起步，而不是从尚待扫描的持久化水位以上开始。

  已复现：文档 1 add 成功后进程结束，重开回调里添加另一文档，错误地再次分配 ID 1 并报 AlreadyExists，导致打开失败。执行：至少将回调可用的分配器推进到已持久化预留水位；更完整的 API 可拆分配置阶段和恢复后的业务阶段，同时保证 hooks 先于重放生效。验收：带未 checkpoint 新增的重开回调仍可安全 add，且原有文档完整恢复。探针：`repro_open_callback_add_runs_before_allocator_recovery`。

- [x] **B09：维护性删除也要留下可恢复的按 ID 清理记录。**

  位置：[crud.rs](../rs/anda_db/src/collection/crud.rs)。只有能正常构造 Document 时才记录删除日志；dead ID 或 schema 无法解码的文档走直接扫索引分支。

  已复现：已 checkpoint 文档的对象丢失，`remove` 成功清理内存后进程结束；重开时旧 bitmap 与唯一键占用恢复，替代文档仍被拒绝。执行：添加不依赖前映像的 purge-by-id 意图，恢复时扫除相应索引和 bitmap，checkpoint 后才删除该记录。验收：dead ID 和 schema-invalid 对象的删除在任意中断点可重试且不会恢复幽灵唯一键。探针：`repro_dead_id_removal_has_no_replay_record`。

- [x] **B10：扩展元数据的未知提交结果也必须令句柄失效。**

  位置：[persistence.rs](../rs/anda_db/src/collection/persistence.rs)、[extensions.rs](../rs/anda_db/src/collection/extensions.rs)。`guarded` 在 future 返回 Err 时也会解除保护；这里只有取消保护，没有对应的未知存储结果处理。

  已复现：meta PUT 实际成功但丢失响应，`save_extension` 返回错误后 state 仍为 Active；`open_collection` 返回同一个旧句柄，后续扩展写入因旧 CAS token 持续失败。执行：在元数据持久化边界分类错误，未知结果和 CAS 冲突触发 poison；序列化/本地大小预检失败保留可恢复的健康状态。同步覆盖 remove_extension、删除索引与 compaction 的错误出口。验收：ErrorAfter 后直接重开得到新句柄，后续写入成功。探针：`repro_unknown_extension_commit_does_not_poison`。

- [x] **B11：HNSW bootstrap 的清理应尊重只读模式。**

  位置：[hnsw.rs](../rs/anda_db/src/index/hnsw.rs)。bootstrap 无条件运行 `purge_orphan_node_blobs`，而上层只在打开完成后判断是否允许 flush。

  已复现：在未引用的节点对象存在时，只读 open 仍发出 DELETE。执行：将清理移到可写维护阶段，或显式传入只读打开策略。验收：只读打开的整个存储调用日志中没有 PUT/DELETE/COPY；可写重开仍能清理真实崩溃遗留对象。探针：`repro_read_only_open_deletes_orphan_hnsw_blobs`。

- [x] **B12：流式写入成功的数据必须能按约定读回。**

  位置：[storage.rs](../rs/anda_db/src/storage.rs)。读取以 `max(压缩后大小×16, 小对象上限×16)` 作为解压限额，流式写入却没有相应约束。

  已复现：小对象上限 1 KiB 时，64 KiB 重复文本可流式写入并 shutdown 成功，但只读出 16 KiB 就报超限。默认配置下，高压缩率且大于 32,768,000 字节的数据也会碰到该条件。执行：定义独立的流式明文大小预算，并在写端验证；或使用受验证的原始长度元数据。不能只由压缩比推断正常数据是否过大。验收：分别测试重复文本、零字节、不可压缩数据、限额前后边界，成功写入的对象应能完整 round-trip。探针：`repro_stream_writer_reader_rejects_compressible_roundtrip`。

- [x] **B13：压缩头识别不能依赖第一次缓冲至少有 4 字节。**

  位置：[storage.rs](../rs/anda_db/src/storage.rs)。`object_chunk_size=1..3` 合法进入构造，第一次 `fill_buf` 不足以识别 zstd magic，流式读取会直接返回压缩内容。

  执行：在不丢失前缀的前提下读取完整 magic，或在初始化验证并约束块大小；对 0 也明确拒绝。验收：块大小 1、2、3、4、默认值下，压缩与未压缩对象的 buffered/stream 结果一致，或小块配置明确报错。探针：`repro_stream_reader_small_chunk_does_not_sniff_zstd`。

- [x] **B14：遍历 JSON 数组时逐个检查元素。**

  位置：[index/mod.rs](../rs/anda_db/src/index/mod.rs)。只在首元素是字符串或对象时进入 JSON 数组，因此 `[0, "searchable", ["nested"]]` 的两个文本都被遗漏；首元素为嵌套数组时也不会递归。

  执行：移除首元素筛选，对所有元素使用现有迭代栈与复杂度预算，非文本元素自然跳过。验收：混合数组、嵌套数组和空数组语义一致；重建索引使历史遗漏文本可搜索。探针：`repro_json_text_extraction_depends_on_first_array_element`。

- [x] **B15：`add(Document)` 应验证 Document 的字段编号映射与集合一致。**

  位置：[crud.rs](../rs/anda_db/src/collection/crud.rs)。集合验证按自己的字段编号检查值，索引 hooks 却按传入 Document 自带的 schema 查字段名；两者不一定是同一套映射。

  已复现：两个 schema 都有文本字段 a/b，但注册顺序相反；add 接受外部 Document 后，查询 a=`A` 返回该 ID，get 却显示 a=`B`。执行：拒绝不兼容的字段编号映射，或按字段名显式转换后再统一验证和索引；不能只比较 Arc 指针。验收：独立构造但等价的 schema 可接受；编号不兼容的 schema 明确拒绝或按名转换，立即读取与重开前后的索引一致。探针：`repro_foreign_document_schema_yields_wrong_index_values`。

## 性能与简化清单

| 完成 | 编号 | 可执行改动 | 位置与依据 | 验收方式 |
| --- | --- | --- | --- | --- |
| [x] | P01 | 对布尔过滤实施有界集合计算和候选集下推 | [query.rs](../rs/anda_db/src/collection/query.rs)：Or/And 子树以 limit=0 展开；限额 1,000 只限制最终输出，复杂过滤仍可分配 O(N) 中间集合。Not 即使候选集很小也求全量排除集。 | 用 reference set 验证所有等价表达式与双向分页；记录高匹配率、嵌套过滤下的峰值分配、p95。Or 的每个分支可保留同方向前 K 个再合并：被分支排除的 ID 已有至少 K 个更靠前的并集成员；And 不能直接套这个截断规则。 |
| [x] | P02 | 对恢复读取、意图读取与清理使用有界并发；把垃圾回收与提交边界分开建模 | [recovery.rs](../rs/anda_db/src/collection/recovery.rs)：每个对象串行 await，日志删除又处于独占 operation_gate 内。 | 用 1/10/50 ms 存储延迟、64/1,000 条待恢复记录测重开与写入阻塞时间；并发保持可配置上限。串行部分约为 N×单次请求延迟，不应按 CPU 优化处理。必须先修 B03，任何并行化都不能吞掉恢复失败。 |
| [x] | P03 | 缩减在内存中保留的意图内容；评估索引映像或差量日志 | [recovery.rs](../rs/anda_db/src/collection/recovery.rs)：每次更新持有两份完整文档，重开还 clone 整张 intent map。正常 flush 主要需要路径/sequence 来清理。 | 测同一大文档重复更新 1,000 次、checkpoint 前后的 RSS、编码字节数和恢复时间；保持多次更新与部分 checkpoint 的重放正确性。内存中可只保留序号，磁盘格式优化单独实施。 |
| [x] | P04 | 让 BM25 compaction 按 changed/dirty 状态决定持久化，避免相同桶数反复重排 | [bm25.rs](../rs/anda_db/src/index/bm25.rs)：底层即使桶数不变也重建并标脏，wrapper 因 new_count >= old_count 直接返回。现有文档确实只承诺桶数减少时持久化，因此列为优化。 | 探针 `repro_bm25_compaction_same_bucket_count_does_not_flush` 已确认相同桶数压实后仍 dirty；参考 B-Tree 的 CompactionOutcome。第一次必要压实提交，紧接第二次无变化调用应为 no-op。 |
| [x] | P05 | 给单路检索提供直接返回路径，删除 RRF 后重复去重 | [query.rs](../rs/anda_db/src/collection/query.rs)、[query.rs](../rs/anda_db/src/query.rs)：即使只有一个排名列表也建 hash map、排序，再转 UniqueVec；RRF 的 map 已保证输出 ID 唯一。 | 对单 BM25、单 HNSW、混合与多同类索引比较结果顺序、边界值和分配数；多路 top-K 可进一步评估有界堆，避免无必要全排序。 |
| [x] | P06 | 测量同步计算对异步执行器的占用，按阈值做有界 CPU 调度 | [crud.rs](../rs/anda_db/src/collection/crud.rs)、[query.rs](../rs/anda_db/src/collection/query.rs)、[storage.rs](../rs/anda_db/src/storage.rs)：验证、tokenization、HNSW、CBOR 与 zstd 直接在 async 调用里同步执行。 | 同时运行短查询和大文档写入，测短请求 p99/执行器调度延迟。大任务可批量 offload，保留小任务快速路径；spawn_blocking 的任务不会随外层取消自动停止，必须保留操作租约和结束追踪。另对工作区 release 的 opt-level=z 与速度配置做实测比较。 |
| [x] | P07 | 建立按字节的缓存预算，测量固定 256 个失效条带的碰撞成本 | [storage.rs](../rs/anda_db/src/storage.rs)：默认每集合 10,000 个对象、每对象最多约 2 MB；大量写入会间接淘汰同条带的其他热对象。 | 用大文档、多集合及读写混合负载测实际 RSS、命中率和后端 GET 数。优先明确 cache_max_bytes；条带数、路径 generation 方案以测量选择，不能把既有 entry-count 配置静默改成字节语义。 |
| [x] | P08 | 为高选择性过滤提供可调的检索候选策略 | [query.rs](../rs/anda_db/src/collection/query.rs)：目前先取每索引 min(limit×10,4096) 候选再过滤，命中可能不足。该上限已有文档，是召回/成本取舍，不计作此次 bug。 | 测租户/时间过滤选择率 0.1%、1%、10% 的 recall@K 与延迟；评估先过滤、小集合精算或自适应扩候选，保留资源上限和近似检索语义。 |

- [x] **S01：把 Collection 按职责拆分，但保持锁与提交不变量集中。**
  `collection.rs` 共 9,850 行，其中生产实现和文档 4,418 行。可拆为 lifecycle、recovery、crud、query、index_ops、extensions 与 tests；先只移动代码，保持公开路径及 API。锁顺序、poison 条件、发布顺序保留在统一说明中，避免每个模块复制一份不同规则。验收：不改行为，已有测试与格式兼容夹具全部通过。

- [x] **S02：用类型化撤销记录替换三套 CRUD 中多张 HashMap。**
  [crud.rs](../rs/anda_db/src/collection/crud.rs)。这些容器用于追踪操作历史，不需要按 index 哈希查找。可使用 `Vec<UndoEntry>`，明确记录已经完成的阶段并逆序回滚，统一回滚失败的 poison 策略。必须在 B01 的跨文档隔离修复后实施；重构撤销记录本身不能解决唯一键竞争。

- [x] **S03：把元数据快照、提交与回收的状态表达清楚。**
  将 “unclaimed/full” 隐含约束转成命名明确的已提交快照、待发布索引变更、checkpoint 结果；统一 B-Tree/BM25 的 bucket manifest 提交及最佳努力回收公共部分。避免过早引入统一大 trait，把 HNSW 的节点/ids/metadata 恢复流程强行装进桶索引协议。验收依据是 B02/B10 和现有故障注入，不仅是编译通过。

- [x] **S04：消除文档与实际契约的矛盾，修复上手依赖版本。**
  [error.rs](../rs/anda_db/src/error.rs) 及多处注释声称非 Active 句柄拒绝所有操作，但 get/search/query 实际允许读取；[技术文档](anda_db.zh.md#L419) 又明确说明 poisoned 句柄仍可读。由于已有这一明确约定，本报告未把“缺少读取生命周期检查”计入 15 项 bug。应统一说明可读、可写、可恢复状态，以及读取旧句柄可能落后于存储的范围。
  [README.md](../rs/anda_db/README.md) 仍建议 `object_store=0.13`，而 crate 已依赖 0.14；照此接入会产生不同版本的 ObjectStore trait 不兼容，应更新并增加独立消费者编译验证。另外，`reconcile_storage` 仍描述“连续 missing 后停止”的旧恢复启发式，现已是水位有界扫描；清理这些会误导维护者的注释。

## 建议实施顺序与验收门槛

| 批次 | 工作项 | 完成标准 |
| --- | --- | --- |
| 1：约束与发布安全 | B01、B02、B03、B05、B06 | 唯一键交错测试、完整前缀调用日志、元数据发布故障矩阵通过；不会静默丢索引或遗漏正常对象 |
| 2：重开与恢复行为 | B04、B08、B09、B10、B11 | 重开前后分词、意图处理及只读行为一致；故障消失后可正常重试 |
| 3：输入及大小边界 | B07、B12、B13、B14、B15 | 最大合法文档 CRUD、流式 round-trip、异构 schema、混合 JSON 回归通过 |
| 4：低风险性能改进 | P01、P02、P03、P04、P05 | 先保留正确性基线，再报告相同数据与环境下的延迟、RSS、对象请求数变化 |
| 5：较大设计调整 | P06、P07、P08、S01—S04 | 小步骤实施；不要把模块拆分、磁盘格式变更与并发协议修改塞入同一次提交 |

每批至少运行 `cargo test -p anda_db --all-features` 和严格 Clippy。涉及磁盘格式/日志的修改必须保留 v0_8、v0_11 夹具可读性，增加旧数据升级后再次读写与重开的验证；不能只重新生成夹具使测试变绿。涉及并发协议的修改，应将串行崩溃测试补成确定性交错测试，并与参考文档集合和唯一键映射比较。

性能验收建议记录操作吞吐、p50/p95/p99、峰值 RSS、对象 GET/PUT/DELETE 次数、字节数、独占操作门等待时间和检索 recall@K。至少分开 InMemory、带固定延迟的存储与本地文件后端；生产速度结论应来自优化构建，不能从本次 debug 模式回归测试的用时推导。
