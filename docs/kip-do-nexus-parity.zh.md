# Nexus → kip-do 对等审计 (Parity Audit)

[English](kip-do-nexus-parity.md)

核对范围：`de2433b^..d9cdeb3`，包含 `de2433b` 本身。核对基于 Rust 实现、测试、提交差异和 TypeScript 实现，未修改 Rust 引擎、KIP 语法或协议版本。

## 结论及处理

`de2433b` 的新增宿主能力没有随时间格式和 parser corpus 更新同步到 kip-do。此前的 Watch 实现只更新 WatchState，不能提供 Rust 已承诺的持久交接。此更新补齐以下行为，SQLite `transactionSync` 承担 Rust redo plan 的原子提交职责。

| 范围 | 原先 kip-do 状态 | 本次处理 |
| --- | --- | --- |
| Watch handoff | 仅状态更新，无 Activity/wake | 同一提交保存 fire identity、Activity、wake、检查点和重放结果 |
| arm/rearm | 无固定检查点；无替换条件的受保护 rearm | 固定 generation、条件、期限、配置和观察依据；兼容可证明的旧 arm 历史 |
| silence 覆盖 | 扫描目标随当前 head 移动 | 固定 inclusive due_seq，保留水位，正确区分截止前、边界和截止后更新 |
| 文本与混合条件 | 未固定 evaluator，混合文本可能被跳过 | 要求固定 evaluator；结构化匹配后应用文本判断；异常不消耗覆盖 |
| 异步评估 | 缺失 | prepare/read/commit；完整候选和历史 before/after；unknown 延后；材料可撤销 |
| wake 状态机 | 缺失 | 版本/fence 检查、五分钟真实时钟租约、阻塞/恢复/取消/完成、幂等回执 |
| 完成与 continuation | 缺失 | KML 输出、最多 16 个子 wake、终态回执一起提交；失败整体回滚 |
| wake 分页与恢复 verifier | 缺失 | 有界 snapshot 分页；恢复需注册代码，await 后重新检查版本、权限和依据 |
| wake dispatch | 只有 SleepTask dispatch | 复用 Attempt/Decision/依赖检查，校验 Watch 输入；绑定与 observer 先决条件 |
| 查询与终态对账 | 缺失 | 直接认证 observer、CAS、观察时间与配置校验；not_started 可重试，finished 不代表成功 |
| 保护读取（`ceefc4f`） | 无对应 attention 记录 | 专门读取及通用 readControl 均保护 wake/dispatch/评估材料；推进要求 read_history |
| 上下文信任 | 只有全局权重 | 具体度优先、同级冲突拒绝、历史投影、setTrust 保留规则 |
| 校准提案 | 缺失 | 校验作用域、方法、Evidence、来源与不确定性；配置/来源/审计/回执原子提交 |
| 原生事务分类 | 假定存在 cognitive 写入 | control-only 提交为 service/governance；不把内部 wake 记录当成控制依据改变 |

## 已同步与存储引擎专属变更

- `5d0454c` 的标准毫秒 UTC 时间、类型错误分类、Profile/schema 校验以及 Governance 时间条件已经同步到 kip-do，本次保留并运行既有测试。
- `e2a2c05`、`d9cdeb3` 主要更新 parser oracle corpus；已有对应生成文件。
- `033a784`、`747c890` 涉及 AndaDB B-Tree 索引布局、稀疏值与查询计划优化。kip-do 使用 SQLite，不复制 B-Tree 实现；本次为 wake 发现增加本地复合索引。
- `72bc0b2`、`07fa988` 修复 AndaDB 已存宽字段与 Rust KIP 1.x 迁移，包括大批量 staged rows、SleepTask 历史状态、Commitment completed→fulfilled 和旧迁移修复。kip-do 没有这些 AndaDB 数据文件或 Rust 迁移入口，不存在可直接移植的对应路径。本次没有引入新的 KIP 1.x 导入器。

## 验证

- [Watch handoff 回归](../ts/kip-do/test/watch-handoff.test.ts)：SQLite 故障回滚、reconnect、幂等重放、输出/续接原子性、租约接管与最终检查、权限与异步取消竞态、评估完整性/材料撤销/旧 generation、旧 arm 历史。
- [期限和派发边界](../ts/kip-do/test/attention-boundaries.test.ts)：截止时间边界、有界分页中的新流量、固定评估时间、非幂等重复派发、绑定/observer 缺失、直接认证查询与 CAS、取消后的终态对账。
- [上下文信任回归](../ts/kip-do/test/contextual-trust.test.ts)：当前与历史 BELIEF、规则优先级和歧义、校准来源、审计/配置故障回滚、无 create 权限的校准和证据修正。
- 校准及两条 wake 对账路径覆盖一次性审批消耗后的重放：重放不消耗待用审批、不推进提交序号；新操作仍需审批，当前权限被拒绝后不能重放。
- `make test-ts` 覆盖全部既有 conformance、parser oracle、类型检查及上述回归；`pnpm run build` 验证发布入口与声明文件；`pnpm run codegen` 检查生成文件一致性。

宿主仍负责调度、外部执行器、异步评估和 verifier 注册。持久回执不保证外部副作用 exactly-once。API 参数及集成顺序见 [Brain host contracts](anda-brain-nexus-contracts.zh.md) 和 [kip-do README](../ts/kip-do/README.md#durable-watch-handoff)。
