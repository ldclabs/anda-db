# Anda Brain 对接 Nexus：KIP CognitiveMemory 2.1

本次实现对应 KIP `dcde1de`。协议版本仍是 KIP 2.0，标准包是
`kip://profiles/cognitive-memory@2.1.0`。Rust 与 SQLite/Durable Object 引擎均已提供
下列接口。Brain 的检索策略、调度循环、工具适配器与五意图 Memory Interface
由 Anda Brain 接入；数据库负责权限、引用、版本、事务和记录有效性。

所有协议时间输入必须是 `YYYY-MM-DDTHH:mm:ss.SSSZ`，整秒也必须写成
`.000Z`。非规范字符串（包括时区偏移和非法日期）返回 `ConstraintViolation`，
数字等非字符串返回 `TypeMismatch`；只有字段契约允许时才能省略或传 `null`。
宿主接口、Profile 字段和查询时间参数遵循同一规则。引擎生成时间截断到毫秒，
不自动转换客户端输入；提交顺序仍以 `space_seq` 为准，不能依赖时间戳唯一。

## 宿主初始化

1. 打开 `CognitiveNexus` 并激活标准包。
2. 在启动时注册需要的确定性评估器，再通过具有 `manage_policy` 权限的 Session
   发布规则、参数和 EvaluationPolicy。规则注册属于受信任宿主代码，不能从 KIP
   语句或上传文件执行代码。
3. 为 Brain 分配实际需要的权限。派生写入需要 `derive`；独立观察者通常需要
   `create`、`read`、`derive`、`record_outcome`。观察者与决策者的控制关系由
   受保护的 EvaluationPolicy 声明，不能靠不同 Actor 名字推断独立性。
4. 所有调用使用宿主认证得到的 `AuthContext`。不要从模型生成的请求体构造身份。

Rust 的 `anda_kip::cognitive` 和 TypeScript 包根导出 `ArtifactPin`、
`EvaluationPolicy`、`ObserverControl`、`EvaluationInput`、`EvaluationRule` 与
`DispatchRequest`。Rust 评估器是实现 `EvaluationRule` 的 `Arc`；纯函数闭包也实现此 trait。

| 操作 | Rust Session | TypeScript Session |
| --- | --- | --- |
| 设置投影策略 | `set_projection_policy(space, name, expected_version, settings)` | `setProjectionPolicy(name, expected, settings, space?)` |
| 设置受保护的信任权重 | `set_trust(space, expected, weights, default_weight)` | `setTrust(expected, weights, defaultWeight?, space?)` |
| 保存不可变 JSON 材料 | `put_artifact(space, content, source_refs)` | `putArtifact(content, sourceRefs, space?)` |
| 读取材料 | `read_artifact(space, pin)` | `readArtifact(pin, space?)` |
| 发布评估策略 | `set_evaluation_policy(space, expected, policy)` | `setEvaluationPolicy(expected, policy, space?)` |
| 撤回身份合并 | `withdraw_identity(space, decision_id, expected_identity_version, reason_evidence)` | `withdrawIdentity(decisionId, expectedIdentityVersion, reasonEvidence, space?)` |
| 带完整性水位的变更页 | `change_page(space, after, limit)` | `changePage(after, limit?, space?)` |
| 领取、续租或接管任务 | `lease_task(space, task_ref, expected_version, expires_at)` | `leaseTask(ref, expected, expiresAt, space?)` |
| 重新布置 Watch | `arm_watch(space, watch_ref, expected_version)` | `armWatch(ref, expected, space?)` |
| 推进 Watch | `advance_watch(space, ref, expected, generation, limit)` | `advanceWatch(ref, expected, generation, limit?, space?, evaluator?)` |
| 写入派发意图 | `enqueue_dispatch(space, request)` | `enqueueDispatch(request, space?)` |
| 派发前检查 | `begin_dispatch(space, attempt_id, expected_intent_version, fencing_token)` | `beginDispatch(attemptId, expected, fencingToken, space?)` |
| 验证清除报告 | `validate_erasure_plan(space, plan)` | `validateErasurePlan(plan, space?)` |
| 用观察结果完成对账 | `reconcile_dispatch(space, attempt_id, expected, outcome_ref)` | `reconcileDispatch(attemptId, expected, outcomeRef, space?)` |

自定义评估器注册在 Nexus 上：Rust `register_evaluation_rule(artifact, evaluator)`，
TypeScript `registerEvaluationRule(artifact, evaluator)`。按规范化规则内容的 SHA-256
摘要绑定；同一实例中不能重绑已有摘要。重启时恢复注册，历史材料本身不会自动加载代码。

## Skill 与学习记录

`Skill` 保存稳定身份和 `current_revision`；`SkillRevision` 保存不可变行为与
`revision_of`。两端均支持在一个 `MUTATE` 内创建双向引用。行为摘要覆盖
`attributes` 中除 `behavior_digest` 之外的全部字段，采用 `kip-jcs-safe-v1` 的
规范化 JSON 与 SHA-256。摘要格式为 `sha256:<64 位小写十六进制>`。Rust 可调用包根的 `content_digest`，
TypeScript 可调用 `contentDigest`。

```text
MUTATE {
  CREATE CONCEPT ?skill {
    TYPE "Skill"
    SET ATTRIBUTES {skill_class: "workflow", summary: "先验证再执行", status: "proposed"}
    SET STRUCTURAL {("current_revision", ?revision)}
  }
  CREATE CONCEPT ?revision {
    TYPE "SkillRevision"
    SET ATTRIBUTES {
      task_family: "deploy/preflight",
      procedure: "run the preflight check",
      behavior_digest: :digest
    }
    SET STRUCTURAL {("revision_of", ?skill)}
  }
}
```

换用新 revision 必须以 CAS 提交，状态重置为 `proposed`，清除当前 TrialState 和
GradingState。普通注释不会重置状态。创建 revision 本身不代表依赖已验证；自动使用
前还需要生产 Activity 的 DependencyBasis 或一次明确的依赖重验证。

学习流程按以下顺序提交：

1. `trial_open` + TrialRecord：冻结 revision/bundle、规则、参数、基线样本、分层权重、
   独立抽样单位、配额、观察窗口、缺失策略、评估策略版本和重放材料。
2. `action_gate` + DecisionRecord：区分 retrieved、used、applied，所有 applied revision
   必须出现在 inputs。附带 DependencyBasis 时，两个记录必须使用同一读取依据。
3. `action_attempt` + AttemptRecord：先持久化 Space 内唯一的 attempt_id，再执行动作。
   trial 必须早于决策；不得根据观察结果倒填试验归属。
4. `outcome_observation` + OutcomeRecord：用 observation_key 去重，关联 attempt 与
   decision；同一 attempt/metric/window 的多次观察不能成为多个独立样本。
5. `lifecycle_verdict` + EvaluationRecord，与 Skill 状态及缓存一起提交。所有修改的
   属性、结构和 Facet 平面都必须有版本守卫，或使用一个整体版本守卫。

OutcomeRecord 一旦附着就冻结，包括当时缺省的可选成员；修正使用新的观察与纠正关系。
导入的 Skill 状态回到 `proposed`，当前试验/等级缓存被清除，导入来源不能成为本地
可信观察者或直接进入派发。

Nexus 复核引用、试验和 revision 归属、独立 attempt、观察者控制、原始与当前策略，
并调用已注册评估器重算 comparison。不能凭 Activity 名字、手写 comparison 或
可变成功次数提升等级。只有 `trialed → adopted` 可以晋升，且必须达到独立样本配额、
非负效果门槛和规则的比较要求。紧急撤回允许零样本；同状态的不足样本监测是否保持
既有采用状态由受保护策略控制。

内置规则描述是 `{"engine":"kip:binary-stratified-v1"}`，参数是 `{"alpha":0.05}`。
它支持 stratified/randomized、预声明分层权重、`count_as_failure` 缺失处理和
Hoeffding 不确定性。paired、off-policy 或其他算法应注册自己的确定性评估器，校验其
配对、相关性和倾向概率契约。评估器收到数据库核对后的材料，不能绕过数据库的独立样本
配额、效果下限、观察者权限和事务守卫。

## 重放材料

材料目前是规范化 JSON。不要把 artifact handle 当 URL 或文件路径；引擎不会自动抓取。
读材料会重新检查全部 source_refs 的权限。规则/参数等无材料来源的发布需要
`manage_policy`，材料衍生需要 `derive`。相同内容摘要不能换一组更宽松的来源绑定。

Trial 的重放内容必须包含 `rule`、`parameters`、`basis`、`baseline_attempts` 和
`baseline_outcomes`。Evaluation 的重放内容必须包含 `rule`、`parameters`、
`trial_record`、`attempts` 和 `outcomes`，使用引用字符串作为 map 的键：

```json
{
  "attempts": {
    "X-12": {"record": "此处是完整 AttemptRecord 对象", "principal_id": "实际写入身份"}
  },
  "outcomes": {
    "E-8": {
      "record": "此处是完整 OutcomeRecord 对象",
      "status": "active",
      "corrected_by": [],
      "principal_id": "实际观察者身份",
      "observed_at": "2026-09-07T00:00:00.000Z"
    }
  }
}
```

上例只说明 map 的布局；`record` 实际应为对象。Trial 的两个 baseline map 使用同样
布局。材料的 source_refs 必须覆盖全部 revision、样本，以及 Evaluation 的 trial。
精确示例见两端的 `nexus_contracts` / `nexus-contracts` 集成测试。

## 依赖与身份

生产 Activity 的 DependencyBasis 记录真实读到的版本；引擎捕获 inputs 和最终
outputs 的版本。读取时递归检查 all_of、any_of 和 context 分组，不能把缺失依据
当作 current。`dependency_validation` 必须完成并指出精确输出；它不能替换旧 Assertion
的原始前提。刷新可变内容要写新版本及生产 Activity。

每次 MERGE 生成 `identity:<tx_id>:<source_id>` 决策，可通过受保护的 `read_control`
读取。撤回需要当前 identity version 与原因 Evidence，返回待复核写入集合；旧 tuple、
Assertion 和历史解析仍保留。无法恢复意图的引用保持 needs_review。

## 持久任务与变更流

先创建 `pending` SleepTask，再领取租约。租约保留 owner、递增的 fencing_token、
expires_at 和 attempt_count。完成任务时，在同一个 `MUTATE` 中写入终态与输出，使用
当前版本守卫；过期/被替换的租约不能提交终态。

先创建 `disarmed` Watch，再 arm。每次 arm 都增加 generation。推进时提交所读版本
和 generation，持久化 consumed_seq、matched 与状态。结构化 selectors 使用 AND，
ops/touched 数组内部使用 OR；文本条件须提供 Brain evaluator。Rust 使用
`advance_watch_with`，TypeScript 使用 `advanceWatch` 的 evaluator 参数。

`change_page` 在空页上也返回 coverage。仅在 `complete` 且 authorization_view 一致、
覆盖截止时间后，silence Watch 才能触发。权限、控制依据变化或无法证明的控制历史缺口
要求重新同步与布置 Watch。Rust 的控制检查点可识别中断的控制写入；旧库迁移之前的
控制历史不会被补造成完整历史。

派发顺序是：提交 AttemptRecord → enqueue_dispatch → begin_dispatch → 工具调用。
工具适配器的幂等/查询能力标志必须来自宿主配置，不能取自模型生成内容。每次实际派发
使用返回的 attempt_id 作为外部幂等键，并携带当前租约 token。返回动作只有
`dispatch`、`lookup`、`outcome_unknown`、`done`；不支持幂等或查询时，丢失结果后禁止
自动重派。外部工具自己的权限、实际环境与工具版本检查仍由执行适配器完成。

## 保留与清除

ErasurePlan 是 Memory Interface 的报告对象，不是标准包里的 Facet。Brain 在返回报告前
调用 `validate_erasure_plan` / `validateErasurePlan`，验证每个声称 erased 的目标和
completed 的依赖闭包。验证不会执行删除，也不会推进 Space 序列。

来源被 PURGE/PURGE PAYLOAD 时，拥有该来源的重放内容及其存储版本一起清除，保留
非内容的摘要墓碑防止同一材料被重新放回。删除生产 Activity 时保留非内容依赖边，防止
尚存摘要因丢失溯源而被误判为已清除。semantic_forgetting 的 completed 会检查已知依赖
闭包；有未清除、持有或未验证覆盖的目标时不能报告完成。备份目标必须有后端验证，
引擎不把模型填写的状态当成删除凭证，也不能召回不受它控制的外部导出。

Rust 通过提交重放日志恢复跨集合的认知、保护状态和元素治理更新；SQLite 通过原生事务
保证原子性。返回成功前完成持久化，重启先恢复提交再接收读取。宿主仍遵守 AndaDB 的
单写进程约束，并在需要本地持久化时选择合适的 object_store/fsync 配置。
