# 测试标准

[English](testing.md)

AndaDB 承诺数据耐久性与一致性，其测试套件必须覆盖常规软件测试常忽略的两个维度：**机器可能在任意时刻断电崩溃**，以及**任意操作序列的行为必须严格符合规范**。测试套件分层组织；除特别说明外，各层均在常规 `cargo test` / CI 中运行。

## 测试分层

### 1. 功能正确性（单元测试 + 集成测试）

覆盖每个公共 API 与错误分支的常规测试，以及每个修复 bug 的回归测试。代码位于各 crate 的 `src/` 与 `tests/` 目录。最全面的统一入口为 `rs/anda_db/tests/coverage_public_api.rs`。

### 2. 崩溃一致性与故障注入

对象存储的崩溃模型：单个 `put` 操作具备原子性，但连续的 put/delete 操作序列可能在任意中间状态被中断。

- [`anda_object_store::fault::FaultStore`](../rs/anda_object_store/src/fault.rs)：包装任意 `ObjectStore` 并注入故障：第 N 次变更后模拟断电、针对特定路径的写入失败、撕裂写 (torn writes)。同时记录所有到达后端的写操作，用于在测试中校验写入顺序。
- `rs/anda_db/tests/crash_recovery.rs`：回放确定性负载，模拟在**每一次**可能的变更后发生断电，重启并校验耐久性契约：
  - 数据库始终能够成功重新打开（绝不损坏至无法启动）；
  - 经 `flush` 成功确认的文档完整无损且已正确建立索引；
  - 在最后一次 ack 之后写入的文档，处于其变更历史允许的状态之一，绝不会出现损坏的非法中间状态；
  - 恢复后数据库能够正常接收新写入。
  该套件同时防护刷新不变量（例如 `flush_metadata` 绝不能推进 `last_saved_version`）、瞬态读故障下的干净错误返回，以及对象数据损坏时的 no-panic 行为。

修改 flush/recovery 路径时，该测试套件必须保持全部通过；若发现新的不变量，应补充至此处。

### 3. 基于模型的属性测试 (Property Tests)

对真实组件与极简且必然正确的参考模型并发执行随机操作序列，校验二者所有外部可观测行为严格一致。

- `rs/anda_db_btree/tests/proptest_model.rs` — B-Tree 与 `std::BTreeMap` 对比：校验变更结果、点查、任意嵌套范围查询、唯一键语义，以及极小桶尺寸下的分桶/分裂持久化往返加载。
- `rs/anda_db_tfs/tests/proptest_model.rs` — BM25 与朴素倒排索引对比：校验词项查询的精确召回集合、布尔查询集合代数、评分合理性以及持久化往返。

若发生失败，测试框架会自动收敛至最小可复现序列；应将该序列作为回归测试用例提交。

### 4. Fuzzing（解析器与不可信输入）

KIP 解析器通过 `anda_db_server` 暴露给不可信外部输入；其核心不变量为“始终以 `Result` 正常终止，绝不 panic”。

- `rs/anda_kip/tests/proptest_parser.rs` — 常规运行的模糊测试子集：任意 Unicode 输入、变异的合法语句与知识胶囊 (knowledge capsule)，以及 `quote_str`/`unquote_str` 往返测试。
- `rs/anda_kip/fuzz/` — 基于 `cargo fuzz` 的开放式覆盖率引导模糊测试目标（仅支持 nightly 工具链，详见其 README）。将发现的每个崩溃用例转化为确定性回归测试。

### 5. 量化质量指标（近似索引）

HNSW 属于近似算法：“返回了结果”并不等于“返回了高质量结果”。
`rs/anda_db_hnsw/tests/recall.rs` 在确定性向量集上通过暴力精确计算求得真实基准 (ground truth)，并在以下场景断言 `recall@10` 最低基线：全新索引、发生删除之后、持久化重新加载之后。若调整索引超参数或图算法，该最低基线即为质量契约。

### 6. 磁盘格式兼容性

`rs/anda_db/tests/fixtures/v<MAJOR>_<MINOR>/` 保存已发布版本写入的完整数据库目录，并提交至仓库。
`rs/anda_db/tests/format_compat.rs` 打开每个固件版本，验证文档、三种索引类型及扩展元数据均完好无损。破坏固件意味着破坏已有用户数据：必须提供迁移路径或回滚更改。在完成经过深思熟虑的向后兼容格式变更后，重新生成当前版本的固件：

```bash
cargo test -p anda_db --test format_compat -- --ignored generate
git add rs/anda_db/tests/fixtures
```

### 7. 代码覆盖率（质量参考，非唯一准入限制）

```bash
make coverage        # 终端摘要
make coverage-html   # HTML 完整报告
```

CI 会上传 lcov 产物。覆盖率用于定位尚未覆盖的逻辑分支；覆盖率本身不是终极质量目标——故障注入与随机属性测试往往比单纯凑覆盖率的用例能发现更多深层隐患。

## Schema 性能验证

```bash
cargo bench -p anda_db_schema --bench values
```

`coerce` 与 `materialize` 用例不计入输入构造和 CBOR 解码耗时。
`create`、`typed_update`、`decode_materialize` 分别测量文档创建、类型化更新以及完整的解码到文档路径。
用例覆盖字节缓冲区、整数数组、向量和 JSON。`json_encode` 测量复用输出缓冲区的 JSON 编码。评估时同时比较分配成本和耗时。

`typed_read_stream` 与 `typed_read_value_tree` 比较当前公共读取接口和消费所有权的 CBOR 值树方案。
当前 cbor2 的两种解码器都支持把字节串读为序列。值树可以缩短解码时间，但会将整数数组和向量展开为独立的 CBOR 值；公共接口保留字节流路径以避免这种中间值树。
替代方案仅用于基准比较，不增加运行时策略。

## 新特性开发 Checklist

- 新增公共 API → 功能测试（第 1 层）。
- 涉及 flush/recovery/存储布局变更 → 崩溃一致性测试（第 2 层）；若磁盘格式变更，重新生成固件（第 6 层）。
- 新增语义明确的数据结构 → 参考模型属性测试（第 3 层）。
- 解析不可信外部输入 → Fuzzing 模糊测试（第 4 层）。
- 近似/启发式算法变更 → 设定明确量化基线指标（第 5 层）。
