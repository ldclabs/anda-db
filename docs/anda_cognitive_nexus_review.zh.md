# Cognitive Nexus 审查修复记录（2026-09-23）

基线为 `b77dd10`。本次修改沿用单写实例、Nexus 执行锁与持久化 redo
设计，修复审查中复现的 10 项问题，并减少重复扫描。未改变 KIP grammar
或持久化行格式。

## 已完成的修复

| 问题 | 实现与验证 |
| --- | --- |
| Session 越过 Space 操作控制记录 | Grant、Delegation、Approval、ActorBinding、Policy 校验目标归属；全局 Principal/Group 只允许直接 system session；可信宿主入口保留 |
| DESCRIBE TRANSACTION 跨 Space 泄漏 | 检查 Space，过滤可见 change；不存在、跨 Space 和隐藏事务使用相同错误消息，不再保证缺失结果意味着可重新创建 |
| 脱敏后重新泄漏 tuple 内容 | 扩充视图后再次应用字段规则；tuple 匹配、端点绑定和遍历要求可读 tuple；隐藏 merge 目标不再通过 canonicalization 暴露 |
| snapshot_token 绕过历史权限 | 显式 AS OF 与 envelope token 都要求 read_history；分页续读按 Principal 保留服务端记录，防止伪造 cursor 坐标 |
| 带前置条件的重试失败、检查与执行之间有竞态 | 锁内先授权和回放已提交请求，新执行再检查 Space/schema 前置条件 |
| Capsule 绕过类型校验 | 新记录共用 Core 类型、属性、Facet carrier、结构及 tuple 端点校验；ValidationOnly 导入保持可用，本地创建仍拒绝 |
| 同事务重复 CLIENT KEY 创建两条记录 | 先查 staged，再查 Store；相同创建重用 ID，冲突创建拒绝；Rust/TypeScript 共享 fixture 覆盖 |
| dry-run 放过真实提交会拒绝的身份冲突 | 共享最终校验与治理传播；预览不消费审批；KML PREVIEW 的 shell 分配与其他写入互斥 |
| 提交前拒绝遗留 pending 行 | 统一 redo 移交前清理；redo 可能持久化后保留恢复所需 ID；Capsule 构建失败也清理 |
| HISTORY 翻页混入新事务 | 固定首个快照上界；LIST/SEARCH 在不能继续原坐标时显式过期 |

19 项新增 Rust 回归位于
[regressions.rs](../rs/anda_cognitive_nexus/tests/regressions.rs)，覆盖原始触发、
同型控制接口、合法续页、伪造 cursor、并发前置条件、旧数据索引回填及
ValidationOnly 导入。另有 [6 个共享执行用例](../fixtures/kip-conformance-2.0/transaction-final-validation.json)
和 TypeScript dry-run 回归。

## 性能与代码简化

- JOIN 使用共享的中间工作预算，等值匹配使用 hash 桶，保留 OPTIONAL 的空绑定语义；相同表头的 UNION 直接追加。
- 追加历史版本按既有 Space/element/transaction 索引去重，不再解码整个版本历史。
- CHANGES/Watch 按逻辑 seq 扫描有界页，正确处理物理 ID 与 seq 顺序不同的日志。
- Activity 输出索引缩小依赖 producer 候选；旧导入行的派生输入/输出键在首次建索引前补齐，之后写路径统一维护。
- 历史候选在单次 Context 内复用，并对实际扫描的版本收费；完整依赖验证结果也在相同 Context、policy/time 基础内复用。
- Skill/SkillRevision 普通创建只检查必要的当前事务记录，历史样本扫描保留给相关 Attempt/Outcome/Evaluation 校验。
- SEARCH 下推可用的类型/谓词范围，仍然只对授权且脱敏后的语料评分。
- SleepTask lease 在宿主边界类型化解析，消除多处缺字段默认为空字符串的读取；更新过时模块说明。

[成对基准及全部原始结果](benchmarks/anda_nexus_2026-09-23/README.md)
记录了 1 千与 1 万条数据下的本地延迟。它们不是生产吞吐量承诺；
Outcome/Evaluation 的完整历史核对仍会随相关数据规模增长。

## 对调用者的影响

分页 cursor 绑定当前 Store 与 Principal，最多保留 1024 项。重连、淘汰或
伪造来源返回 `CursorExpired`，应重新获取第一页。有效 KQL 续页仍可继续
首个快照，不额外要求 read_history；直接选择历史坐标则需要该权限。

全局身份管理应使用可信宿主的 GovernanceStore 或直接 system session，
不能再凭单个 Space 的授权修改全局 Principal/Group。受限 tuple 字段不能
通过匹配结果或 canonical 字段间接读取。

Schema 校验不会重新绑定存量元素未修改的类型。新的输出索引只回填派生键，
不修改认知版本、来源或原有持久化格式；旧格式 fixture 未重新生成。

## 验证

- `cargo test --workspace --all-features`：1796 passed，1 个既有 fixture 生成测试 ignored。
- 最后统一事务错误消息后再次执行 `cargo test -p anda_cognitive_nexus --all-features`：577 passed。
- `cargo check --workspace --all-features`：通过。
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`：通过。
- `make test-ts`：类型检查与 533 项测试通过，包括共享 fixture 与既有 WASM parser oracle。
- `pnpm run codegen` 从 `ts/kip-do` 执行，生成结果随修改提交。未修改 parser，因此无需重建 WASM。
- 格式、AGENTS/CLAUDE 一致性、补丁空白检查通过；工作区测试包含 crash recovery、旧格式兼容及 Watch 故障恢复。

原始失败场景现已作为永久回归通过；既有查询、导入重试、租约与故障恢复测试
同时通过。基准只覆盖注明的工作负载，没有据此宣称所有查询或写入都获得相同比例的提升。
