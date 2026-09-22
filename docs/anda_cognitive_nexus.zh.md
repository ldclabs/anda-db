# `anda_cognitive_nexus` — 技术参考

[English](anda_cognitive_nexus.md)

有关 0.13.1 受保护的 Watch/wake 宿主 API、原子性保证、实时租约边界以及其余宿主职责，请参阅[持久化 Watch 移交文档](../rs/anda_cognitive_nexus/README.md#durable-watch-handoff-0131)。

追踪 KIP v2 提交 `dcde1de`，包含 2.1.0 记忆词汇表。另请参阅 [KIP 参考文档](anda_kip.zh.md)与 [Anda Brain 宿主契约指南](anda-brain-nexus-contracts.zh.md)。

> 参考实现 **KIP 2.0** Cognitive Nexus —— 构建在 Anda DB 之上的嵌入式 AI Agent 记忆大脑。

|                       |                                                                                                                                                      |
| :-------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------- |
| Crate                 | [`anda_cognitive_nexus`](../rs/anda_cognitive_nexus/)                                                                                                |
| 版本                  | `0.13.x`                                                                                                                                             |
| 实现协议              | KIP **2.0** [`Executor`](../rs/anda_kip/src/executor.rs)（[SPECIFICATION.md](../rs/anda_kip/SPECIFICATION.md)）                                       |
| 存储后端              | [Anda DB](../rs/anda_db/) —— 内置 B-Tree + BM25 + HNSW 索引的嵌入式文档存储                                                                           |
| 其他关联实现          | [`anda_cognitive_nexus_server`](../rs/anda_cognitive_nexus_server/) (HTTP/JSON-RPC)、[`anda_cognitive_nexus_py`](../py/anda_cognitive_nexus_py/) (Py) |

> 历史上曾存在于本 crate 中的 KIP 1.x 引擎已被**直接删除，而非迁移重构**。KIP 2.0 是一套完全不同的数据模型，保留一个重命名后的 1.x 引擎其危害远大于彻底移除。本文档绝不涉及 `DELETE PROPOSITIONS`、`_version` 元数据或 Domains 等历史概念；若你在寻找这些内容，说明你查阅的主版本有误。

---

对于已有部署，在用本构建版本打开备份副本前，请先查阅[已发布的 v1 迁移指南](kip-v1-migration.zh.md)。

## 目录

1. [核心基石公理](#1-核心基石公理)
2. [Crate 结构布局](#2-crate-结构布局)
3. [存储架构](#3-存储架构)
4. [Schema Packages](#4-schema-packages)
5. [事务模型](#5-事务模型)
6. [KQL 执行机制](#6-kql-执行机制)
7. [KML 执行机制](#7-kml-执行机制)
8. [认识论投影（Epistemic Projection）](#8-认识论投影epistemic-projection)
9. [META 执行机制](#9-meta-执行机制)
10. [治理系统（Governance）](#10-治理系统governance)
11. [认知胶囊（Capsules）](#11-认知胶囊capsules)
12. [历史时序（History）](#12-历史时序history)
13. [宿主 API（Host APIs）](#13-宿主-apihost-apis)
14. [本引擎不承担的职责与边界](#14-本引擎不承担的职责与边界)
15. [测试规范](#15-测试规范)

---

## 1. 核心基石公理

```text
命题（Proposition）的存在  ≠  命题为真
```

**Proposition（命题）**是真值中立的元组。**Assertion（断言）**是某一行动者（Actor）对该命题作出的承诺，携带立场（stance）、模式（mode）、置信度（confidence）、关联证据（Evidence）及其现实有效时间（valid time）。*当前被采信的信念*是在指定命名策略下从断言中**投影（projected）**出来的，绝不持久化存储。

正因如此，[`PropositionRow`](../rs/anda_cognitive_nexus/src/store/rows.rs) 没有 `confidence` 列，而 `AssertionRow` 有；对主张的更正不是就地重写历史记录，而是记录带有取代（supersession）关联关系的*全新* Assertion。

代码中刻意严格区分的概念边界，任何自作聪明的“简化”都会导致系统概念崩塌：

```text
缺失 (missing)              ≠ 为假 (false)
置信度 (confidence)         ≠ 信任度 (trust) ≠ 记忆强度 (memory strength)
保留过期 retention.expires_at ≠ 现实有效截止 valid_time.until
Space (认知空间)            ≠ Domain (域)
Principal (主体)            ≠ 语义行动者 (semantic actor)
检索得分 (search score)     ≠ 置信度 (confidence)
批处理 (batch)              ≠ 事务 (transaction)
VALIDATE ≠ PREVIEW          ≠ 收据 (Receipt)
FOR TIME                    ≠ AS OF（当时客观事实如何 ≠ 当时大脑认知如何）
认知信任 (epistemic trust)  ≠ 影响权限 (influence authority) ≠ 工具权限 (tool permission)
```

---

## 2. Crate 结构布局

```text
src/
├── id.rs          ElementId：`C-1` `P-1` `A-1` `E-1` `X-1` —— 实体类别编码在 ID 前缀中
├── term.rs        引用、核心字面量、tuple_key
├── time.rs        统一规范的 UTC 格式；字典序 == 时间先后顺序
├── view.rs        原始核心视图（Spec §53.1）—— KQL 点路径的底层读取对象
├── profiles.rs    内置的认知记忆 Profile，同步自规范
├── store/         管理 10 个 anda_db 集合：数据行、写入路径、Spaces、
│   ├── history.rs   流水账日志，以及供 AS OF 读取的元素版本日志
│   └── planes.rs    基于数据行差异比对推导出的分平面版本计数器
├── schema/        符号标识、Package 工件、每个 Space 的独立环境
├── governance/    受保护的控制平面 —— 参见第 10 节
├── tx.rs          事务管理：暂存态、句柄、单事务单版本
├── kml/           变更子句、数值求值、目标选择器
├── kql/           解空间匹配、模式匹配、图遍历、过滤器、字段投影
├── projection/    认识论投影及其判定策略
├── meta/          DESCRIBE / LIST / SEARCH / VALIDATE / PREVIEW / HISTORY / …
├── capsule/       导出、完整性验证以及导入时的语义合并
└── nexus.rs       CognitiveNexus 与 Session：Executor trait 的具体实现
```

---

## 3. 存储架构

10 个集合用于认知状态，另有 8 个集合用于治理控制平面（第 10 节）。每个核心实体类别独立分配集合，因为它们拥有截然不同的字段结构与高频访问路径：例如投影计算需要首先拉取针对某一命题的所有 Assertion，而对齐阶段的 `SEARCH` 仅检索 Concept 的名称。

| 集合名称                          | 存储内容                                      |
| :-------------------------------- | :-------------------------------------------- |
| `concepts` `propositions` `assertions` `evidence` `activities` | 核心认知元素                                 |
| `spaces`                          | MemorySpace 注册表及其序列号                  |
| `transactions`                    | 事务提交日志（Commit journal）                |
| `schema_packages` `schema_envs`   | 已安装的 Package 工件及各 Space 的激活状态    |
| `element_versions`                | 记录元素历史版本的行 —— 供 `AS OF` 读取使用   |

四项核心设计决策：

**所有索引均为单字段索引。** `anda_db` 的复合 B-Tree 索引基于带 `with_unique()` 的虚拟字段构建，因此复合索引*必然*同时施加唯一性约束。若对 `(space, state)` 建立复合索引，意味着一个 Space 只能容纳至多一个活跃元素。系统中唯有 `tuple_key`、`space_id` 与 `tx_id` 具备严格唯一性；其余列均按单列建立索引，查询时通过 `Filter::And` 进行交集过滤。

**字段缺失采用空字符串表示，而非 `Option`。** `Option<T>` 列对应 `FieldType::Option`，B-Tree 索引无法将其作为统一的有序域进行范围扫描。此类列的合法业务值均不为空，因此 `""` 是明确且支持排序的“未设置”表示。

**所有引用字段在 JSON 旁额外维护一个 Key 列。** JSON 存储完整记录；Key 列存储 `Endpoint::key` 确定性字符串，从而将引用相等性判断转换为索引精准点查，避免全表扫描与反序列化比对。

**时间戳使用规范的 UTC 毫秒格式** —— `YYYY-MM-DDTHH:mm:ss.SSSZ` —— 保证字典序严格等价于时间先后顺序，时间范围查询可直接转化为文本的 B-Tree 范围查询。

---

## 4. Schema Packages

在 KIP 1.x 中，具权威性的 Schema 属于图状态的一部分，一次普通的写入即可修改某个类型的含义。而在 2.0 中，Schema 是不可变的版本化工件，通过每个 Space 独立的 **Schema Environment** 进行解析，持久化记录中的每个 `schema_ref` 均指向精确的版本号——彻底避免因外部发布新版本而导致存量实体的语义漂移。

```text
install_package   工件在本地存在
activate_schema   指定工件版本在当前 Space 中正式生效
```

安装不等于激活。携带不同内容到达的相同 `package_id@version` 会触发 `DigestMismatch` 错误，而非就地覆盖更新。环境版本只追加不覆写，因此记录了当时所处环境版本的历史事务将永久保持其原始语义。

声明为 `functional` 的谓词**不会**直接拒绝竞争性写入。两个对立的对象构成*分歧（disagreement）*，若拒绝存储其中之一将导致系统无法记录客观分歧；此类冲突会在投影计算时展开判定（第 8 节）。

---

## 5. 事务模型

单条 KML 语句即为一个独立事务，无论源码是否书写 `MUTATE { … }` 包装块。

```text
阶段 1   声明所有临时句柄（Handles）
阶段 2   解析并执行所有变更子句，此时所有句柄已绑定
提交阶段  统一执行单次写入、单次版本自增、单次写入日志
```

划分执行阶段使前向引用变得完全合法，而前向引用是保证原子溯源链形成的必要条件：例如一个由某 Activity 生成的 Evidence 记录，同时被该 Activity 列为产出物（output），构成合法的循环引用。

**`anda_db` 无法预先保留元素 ID** —— 其 `add_impl` 在底层直接调用 `fetch_add` —— 因此句柄所代表的元素先以 `state: "pending"` 的空壳状态插入，待事务提交时填入正式数据。任何读操作都会过滤 pending 实体，未使用的空壳在后续可安全清理。持久化重做计划（redo plan）在执行清理和恢复读请求之前，优先恢复已提交的后镜像（after-images）。选择块无法看到自身所在事务的未提交写入：子句书写顺序不承载修改语义，若允许清理过程看到本事务写入，会导致相同的逻辑仅因书写位置不同而产生差异。

**版本号在提交阶段统一分配**，而非每次写入即递增：在一个事务中，无论单个元素被多少个子句修改，其版本号仅递增一次。若某子句计算出的新状态与当前状态完全一致，则不产生实际修改，返回 `no_effect` 收据，绝不伪造未发生的流转。

**原子可见性由 `CognitiveNexus` 的 `RwLock` 保证**，而非来自 `anda_db` 底层。这属于进程内保证——鉴于 `anda_db` 本身对每个数据库只允许单一活跃写入者，该保证强度与底层存储完全匹配。

---

## 6. KQL 执行机制

查询过程将 `WHERE` 块求解为一个结果解集，随后执行字段投影。

功能支持：元素、元组与拓扑模式匹配、限定跳数的路径遍历、`FILTER`（全部 11 个内置函数）、`NOT` / `OPTIONAL` / `UNION`、点路径投影、聚合函数、`ORDER BY`、游标分页，以及两个独立的时间维度 —— `FOR TIME`（现实有效时间）与 `AS OF SEQ`（大脑认知历史序列号）。

`AS OF SEQ` 是查询支持的*唯一*历史时间轴。查询坐标必须是单调递增的序列号；若调用方持有墙上时钟时间，需先调用 `DESCRIBE SNAPSHOT AT TIME "…"` 解析对应的序列号坐标，再进行读取。若允许同一时间轴存在两种语法表达，会导致调用方的查询结果取决于引擎优先采信哪一种时间源。

调用方容易误解的两条关键规则：

- KQL 查询结果是**扁平数组**：投影单个变量时 ⇒ 每行返回单个标量；投影多个变量时 ⇒ 每行返回**数组**，绝不返回键值对对象；
- 除非模式显式指定，否则默认仅匹配 `active` 状态的实体。这是实体进入归档（archived）状态的本意。

`NOT` 与 `OPTIONAL` 针对每个输入解都会完整求值其内部子块。独立的 `UNION` 分支从空绑定开始；嵌套时，外层操作符仅将结果与其输入绑定兼容的部分进行连接。`NOT` 内部的局部变量严格限制在块内部。缺失的 optional 或分支变量可由后续模式进行绑定。完整解集在投影与分组之前执行去重；当未投影的内部绑定不同时，投影后的重复值仍会保留。

过滤器采用三值逻辑：针对 null 的比较与字符串测试返回 unknown，否定 unknown 不会使其为 true。即使分支结果为零行，也会静态校验函数参数个数、常量正则表达式、绑定参数、可见绑定位置以及 Schema 符号有效性。空解集或全 null 输入的聚合计算对 `COUNT` 返回 `0`，对 `SUM`、`AVG`、`MIN` 和 `MAX` 返回 null；传入不兼容的非 null 输入会触发 `TypeMismatch`。聚合排序键必须同时出现在 `FIND` 中。

`REGEX` 采用 Rust `regex` crate 的语法：支持 Unicode 的正则表达式，不支持反向引用与环视断言。编译后表达式受到 crate 默认的 10 MiB 编译体积上限及 250 层嵌套深度限制；每个进程最多缓存 256 个不同模式。非法或超出尺寸限制的表达式报错 `InvalidSyntax`。

原生图路径支持精确跳数、有界跳数及无界跳数。零跳路径允许两端为同一可见 Element 而无需实际边支撑。当跳数需要时路径可重复访问顶点；端点解会自动去重。多跳与零跳路径不得绑定 Proposition 变量。每个候选集的加载和路径扩展前沿都计入共享的 100,000 候选上限（`MAX_CANDIDATES`）。超出预算显式报错 `ResourceExhausted`；`LIMIT` 在求值完成后应用，不会中途截断图遍历。未绑定的零跳查询会枚举可见实体并消耗该预算。

**`Context::load` 是读取路径上的权限控制总闸门。** 每个模式匹配、过滤器、字段投影、聚合计算、检索命中和胶囊根节点均必须经过它来加载实体。因此调用方无权读取的元素在整个查询生命周期中均处于解空间之外 —— 参见第 10 节。

### 双重拓扑平面（Structural Planes）

`STRUCTURAL (?src, "field", ?dst)` 能够同时读取两类拓扑。**Profile** 字段通过已解析的具型符号定位；**Core** 核心字段（Spec §8.2）—— Assertion 的 `evidence`/`context`、Evidence 的 `source`/`generated_by`、Activity 的 `inputs`/`outputs`/`associated_actors` —— 则通过其普通字段名定位。因此，查询*引用了该 Evidence 的所有 Assertion*写作：
`STRUCTURAL (?a, "evidence", :e)`。

两类拓扑独立检索、绝不合并，`?edge.field` 对 Profile 字段返回完整符号标识，对 Core 字段返回原始纯文本名称 —— 即使 Profile 声明了一个名为 `evidence` 的字段，也只会*新增*拓扑边，而不会篡改 Assertion 的核心证据引用；若调用方只想匹配 Profile 字段，需通过完整符号进行寻址。Core 字段不返回 `index`：其顺序由底层存储顺序决定而非声明位置，写入路径采用追加语义，不响应 `AT` 指令。

`?edge STRUCTURAL (…)` 绑定边对象本身。Spec §43.7 将其定义为“虚拟拓扑查询状态，不一定是持久化的认知元素”，在实现中正是一个包含 `source`、`field`、`target` 以及 `index`（对于 Schema 声明为 `ordered` 的字段，表示该引用当前从 0 开始的位置索引，§17.4）的对象。无序字段不存在位置概念，其 `index` 直接读取为 null，而非返回虚假数值。

**分页游标必须携带坐标。** `CURSOR` 是引擎签发的不透明令牌，绝非调用方可随意拼写的分页 Offset（§88.4）。游标锁定了遍历开始时的快照坐标，确保第二页与第一页基于完全相同的规范快照进行应答（§44.8）；游标中编码了生成它的操作族，因此 `HISTORY` 游标无法用于继续读取 `FIND`（§102.28）。所有 KQL 应答均会在结果上下文中将该坐标作为 `snapshot_seq` 返回（§50）。

---

## 7. KML 执行机制

支持 `CREATE CONCEPT` / `UPSERT CONCEPT` / `ENSURE PROPOSITION` / `CREATE EVIDENCE|ASSERTION|ACTIVITY` / `ASSERT`（语法糖脱糖）/ `UPDATE` / `TRANSITION` / `SET RETENTION` / `PURGE` / `PURGE PAYLOAD` / `MERGE CONCEPT`，每个语句均支持可选的 `WHERE` 选择块与 `LIMIT`，并支持句柄绑定、`EXPECT VERSION` 校验、操作收据与 Dry-run 预检。

### 统一的 `TRANSITION` 语法

所有生命周期流转汇聚为单一通用语句（Spec §52.5）：

```text
TRANSITION <target> TO "<state>" [BY <ref>]
    [SET FIELDS {…}] [SET STRUCTURAL {…}] [WHERE {…}] [LIMIT n]
    [EXPECT VERSION …]
```

`retracted`、`superseded`、`corrected`、`running`、`completed`、`failed`、`cancelled`、`archived`、`tombstoned` —— 9 种状态收敛于同一语法结构下，早期草案中针对不同操作分散的 6 类语句被彻底归一。`BY` 严格且仅在状态变更为替代语义（`superseded`、`corrected`）时必填，其余状态下拒绝传入；`SET FIELDS` 与 `SET STRUCTURAL` 仅允许在 Activity 终态流转中使用，因为定稿 Activity 是唯一伴随内容最终落盘的状态跃迁。

语法中移除了 `EXPECT STATE`。让调用方重复声明其预期的当前状态属于冗余逻辑，因为引擎本就必须对此进行校验：`TRANSITION` 会读取当前状态，若当前状态不允许执行该流转，直接报错 `InvalidLifecycleTransition`，并在 `details.from` 与 `details.to` 中详细指明。无论调用方是否预先声明状态，得到的校验保证完全一致。

### 版本平面（Version Planes）

`EXPECT VERSION` 始终作为末尾子句，支持限定具体的版本平面：

```text
EXPECT VERSION 7                          针对整个实体
EXPECT VERSION 7 OF ATTRIBUTES            针对核心字段与自定义属性
EXPECT VERSION 3 OF STRUCTURAL            针对拓扑引用关系
EXPECT VERSION 2 OF RETENTION             针对生命周期保留记录
EXPECT VERSION 5 OF FACET "MnemonicState" 针对指定符号的单个 Facet
```

每个平面在 `_system.plane_versions` 下维护独立的自增计数器。提交时，计数器依据加载行与写回行的实际 diff 结果进行增量计算，而非简单取决于执行了哪条子句。若子句写回了相同的值，则不触碰任何平面；若 `TRANSITION` 最终确定了 Activity 的输出拓扑，则必然触碰 structural 平面，无论语句名称如何声明。这种机制使得对同一实体的 `MnemonicState` 衰减任务与业务状态判定能够并发运行而互不冲突，因为它们争用的是不同的平面计数器。版本不匹配抛出 `VersionConflict`，并在 `details.plane` 中指明冲突平面。

幂等键采用**重放机制（Replay）**（§26, §33）：网络超时不代表事务中止，因此当携带已提交幂等键的请求再次到达时，当前 Space 会直接原样返回该历史事务的收据（Receipt）——相同的 `tx_id`、`space_seq`、`committed_at` 与句柄映射 —— 绝不执行二次写入。返回信息携带属于重放调用的 Warning，以便明确告知重试方。单操作自带的幂等键优先级高于外层请求的幂等键，避免批处理中后续操作错误重放前序操作。Dry-run 既不重放历史也不产生重放记录：预检不会产生持久化提交（§69.3），若用真实提交回答预检请求，会导致写操作将自身报告为自身的预览。调用方亦可通过 `DESCRIBE TRANSACTION BY IDEMPOTENCY KEY` 进行故障恢复查询。

`CLIENT KEY` 是粒度更细的重试安全机制（§52.1）：若 `CREATE` 指定的 Key 已在早期操作中成功创建，则直接将该句柄解析为该存量实体且不产生任何写入，实现*单子句*级别的重试幂等。请求级 `ingest` 项中的相同 Key 对其生成的 Evidence 具备相同效果。

执行计划采用三趟扫描（`clauses::plan_pass`）：首先处理 `CREATE CONCEPT`，其次处理 `UPSERT`/`ENSURE`，最后处理其余所有操作。`ENSURE` 必须先看到同事务新建的 Concept 才能校验谓词的主语类型，而由 `ASSERT` 脱糖产生的 `CREATE ASSERTION` 需要引用 `ENSURE` 绑定的句柄。

**`LIMIT` 严格按实体 ID 升序裁剪。** Spec §52.7 允许运行时明确排序规范，定义明确的排序规则是有界批量扫描具备确定性与可重现性的前提。

**请求信封字段必须严格履行或显式报错，绝不可静默忽略。** `preconditions.space_seq` 与 `preconditions.schema_environment_version` 在命令执行前优先校验（§35.4）；`requires` 会对照 `DESCRIBE CAPABILITIES` 声明的能力清单逐项核对，遇到未知特性直接拒绝执行，绝不假定其可用（§67）；`ingest` 在当前命令自身的事务内部生成 Evidence，并将每项的 `key` 绑定为请求参数，确保观察凭据源于传输信封而非模型生成的文本（§71.1, §88.12）；`options.deadline_ms` 直接被拒绝，因为 §80.2 规定客户端超时不代表事务中止，本引擎无法取消正在落盘的提交过程。

`UPDATE` 仅能修改可变的、非受控保护的状态。其实际允许修改的范围由*引擎加载出的实体类型*决定，而非取决于命令语法：对 Assertion 调用报错 `EpistemicRevisionRequired`，对 Evidence 报错 `EvidenceCorrectionRequired`，对已终结的 Activity 报错 `ActivityTerminal`。

`MERGE CONCEPT` 具备非破坏性：被合并的源实体保留其全部状态，追加 `merged_into` 标记并置 `state: "merged"`，后续所有写入操作自动规范化重定向至存活的目标实体 —— 包括所有引用插槽，而不仅是三元组端点（§11.3）。此后以被合并行动者身份写入的 Assertion 都会自动归属至存活实体，从而消除合并前的数据碎片。

---

## 8. 认识论投影（Epistemic Projection）

`BELIEF` 与 `BELIEF SLOT` 执行的是动态投影计算，而非简单的数据读取。核心遵循三条判定法则：

- **无言为 `insufficient`（证据不足），绝非 `rejected`（否定）** —— 开放世界模型绝不会对无人涉足的问题给出“否定”回答；
- **重复不等于印证** —— 断言按印证分组（corroboration group）进行归属划分，每个印证组仅贡献一次权重；
- **共享证据合并印证组** —— 两人复述同一份原始观察凭据，在逻辑上仅构成单一观察源；若第三条断言同时引用了这两份复述，原本看似独立的两个印证组会立即塌缩合并为一组。这精准防范了虚假的伪造印证。

单值谓词（functional predicate）会触发冲突集扩展：即使没有任何人声称“非健康”，只要有人主张“降级”，而 Schema 规定该谓词只能存在单一事实，则冲突直接成立。

投影判定策略带有明确的命名与版本号，并随结果一同返回。阈值的任何修改都会导致返回的策略 ID 变更 —— 否则系统的审计链条将失真。

受保护的行动者信任权重参与投影计算，并按 Space 序列号进行版本锁定。系统不自动进行凭据质量评分；返回的警告会显式声明此能力边界。可通过 `DESCRIBE TRUST` 查看受保护的信任配置。

---

## 9. META 执行机制

严格遵守五层防护纪律，模块结构与此严格对应。混淆其中任意两层，调用方都会误将引擎未作出的承诺当作既定事实：

```text
DESCRIBE / SEARCH   发现 (Find)      —— 当前存储了什么
VERIFY              完整性 (Integrity) —— 工件是否与其声明一致
VALIDATE            合法性 (Legality)  —— 若提交是否能被接受
PREVIEW             影响 (Effect)    —— 若执行会产生何种变更
Receipt             事实 (Fact)      —— 实际持久化提交了什么
```

`DESCRIBE CAPABILITIES` 以结构化数据形式明确报告本引擎支持的能力**以及明确不支持的能力**，并附带不支持的具体原因。如果一个 Agent 只能通过触发运行时错误来发现能力缺失，说明系统浪费了一轮上下文对话；如果 Agent 无法得知缺失，往往会将未实现的功能误解为不存在的事实。

`PREVIEW KML` 直接走真实的 Dry-run 链路，而非独立的模拟逻辑，确保预检结果与真实提交绝无偏差。

关键字 `SEARCH` 基于调用方有权访问且经过字段脱敏（field-redacted）的视图动态构建临时 BM25 语料库。被权限屏蔽的文本绝对无法影响搜索匹配或评分。引擎将非负 BM25 得分 $s$ 归一化映射为 $s / (1 + s)$，并引入确定性的实体标识作为平局决胜规则。检索在共享候选预算范围内预先扫描语料后再行排序；在大型 Space 中，即便指定的 `LIMIT` 很小，也可能返回 `ResourceExhausted`。严禁利用持久化的全局排序直接回答权限受限的视图。当索引坐标发生变动时，搜索游标继续调用会报错 `CursorExpired`，因为系统不支持历史状态下的全文检索。

---

## 10. 治理系统（Governance）

```text
认知内容可以描述权限。
但只有治理控制平面能够授予权限。
```

一个 Space 中完全可以包含一条声称*“Alice 是系统管理员”*的 Proposition、一条高置信度支持该命题的 Assertion，以及两者的充分证据 —— 但 Alice 实际上依然没有任何管理权限。若不进行此种彻底切分，任何具备记忆写入能力的链路都会成为权限提权的漏洞，而 Agent 记忆系统的构建本质上正是提供写入链路。

### 控制平面设计

在同一数据库中、在相同 flush 保护下，额外维护 8 个独立于认知集合的治理集合，**且任何 KML 子句均无法触达**：`gov_principals`、`gov_principal_groups`、`gov_actor_bindings`、`gov_grants`、`gov_delegations`、`gov_policies`、`gov_approvals`、`gov_audit`。

授权（Grant）与委托（Delegation）采用独立的数据行类型，因为其鉴权求值逻辑完全不同。Grant 具备独立效力；Delegation 的有效性完全依附于其委托者*在当前这一刻*所拥有的权限，一旦撤销父级权限，子级 Delegation 立即失效，即便子记录的状态依然标记为 `active`。

策略版本（Policy versions）只追加不覆盖，以便在策略演进后，审计追踪仍能确切回答*“当时是基于哪一版本的策略放行的该操作”*。出于同样的原因，撤销权限表现为状态流转，绝不执行物理删除。

### 鉴权主体识别

调用方通过 `CognitiveNexus::session(auth)` 接入引擎。`AuthContext` 必须由**宿主（host）**基于经身份验证的传输层状态构建，严禁从用户请求体中反序列化 —— 请求信封自带的 `context` 块被明确定义为非权威数据，因为受到 Prompt 注入的 Agent 可以在请求体中构造任意内容。二者产生交集的唯一位置是调用目的（purpose），且属于不对称采纳：请求声明的 purpose 只能在宿主未指定时作为 `declared` 置信级补充，绝不可替换宿主绑定的参数。

权限在每次请求到达时重新解析计算，因此在会话建立后发生的权限撤销，对后续请求能够立即生效。

直接在 `CognitiveNexus` 实例上运行的嵌入式宿主以系统主体（System Principal）身份执行，拥有默认 Space。这是通过相同鉴权链路进行的真实授权，绝非绕过治理的后门。

### 判定决策流程

```text
协议核心不变式判定
    ↓
匹配命中的显式拒绝 (Explicit Deny)
    ↓
匹配命中的放行 (Allow)：Owner、Grant、Delegation 或 Policy 规则
    ↓
默认拒绝 (Default Deny)
```

多条授权可能同时放行某一操作；各项授权独立充分，系统选取匹配的**限制最少（least restrictive）**的一项作为决策依据，并继承其约束条件。合规义务（Obligations）则相反，采取叠加累积机制。

鉴权区分两个不同的评估作用域：命令门禁询问*“该主体是否被允许在此处执行该类操作”*；实体校验则深入询问*“该主体是否被允许针对该具体实体执行该操作”*。限制在特定分类分级下的 Grant 依然允许查询运行，约束条件在实体遍历阶段逐行生效。

### 逐实体防护的具体收益

- 调用方无权读取的实体在整个查询生命周期中均处于解空间之外 —— 不参与匹配、不计入总数、不参与排序、不出现在分页中；
- 字段掩码（Field mask）作用于缓存视图，避免通过结果行是否存在反向推断被遮蔽的字段内容；
- 读取 `_system.origin` 需要显式具备 `read_raw_origin` 权限，未授权时采用*保留该字段但隐藏内容（withheld）*的方式返回，而非完全移除 —— 直接丢弃会让调用方误以为原始记录并未记录溯源来源；
- 若主体的权限范围小于整个 Space，针对全 Space 的计数操作将被拒绝并返回明确原因；事务的变更列表也出于同样的原因进行过滤；
- 变更操作的每个修改目标都会经过独立授权，若批量扫描触及无权修改的目标，整个操作直接报错，绝不会静默减少修改范围。

### 归属声明与撤销

新建 Assertion 需要何种认识论权限由调用者的 ActorBinding 决定，而非由命令文本决定：作为该 Actor 本人绑定时需要 `assert` 权限；作为该 Actor 的合法代表绑定时需要 `assert_as_actor` 权限；无绑定关系时需要 `record_attributed_assertion` 权限。记录他人声明的主张属于正常的记忆转述，绝非身份冒用，必须保持为常规操作。

尽管统一采用 `TRANSITION` 语句表达所有生命周期跃迁，但权限检查依然严格绑定具体流转的目标状态。将主张流转至 `retracted` 或 `superseded` 状态，代表*原声明来源方*主动撤回或替代其主张，只有两种身份具备此资格：你亲自撰写了该主张，或绑定关系表明你全权代表该 Actor。其余主体只能将其流转至 `archived` 或 `tombstoned` 状态。因此，`retract_own` 与 `supersede_own` 守卫前一对状态，`archive` 与 `tombstone` 守卫后一对状态；管理审核人员若试图直接代表原作者撤回声明将被权限系统显式拒绝。

### 分级分类、权限上限与隔离区

实体的 `governance` 块在语法解析层被 `anda_kip` 严格禁止赋值，文本输入与预解析 AST 路径均受此约束。治理状态只能通过 `Session::classify` / `elevate_authority` / `quarantine` 进行变更，且*扩大公开范围*或*提升权限能力*的方向属于高危特权：提高密级需要 `update` 权限，降低密级则需要特许的 `declassify` 权限；降低权限上限无需审批，提高权限上限则可能触发审批流。

分类密级在事务提交时沿着衍生关系向上合并传承 —— 包括 Assertion 引用的 Evidence、Evidence 依赖的数据源、Activity 的输入物，以及 Activity 由输入派生出的输出物。然而权限绝不会沿相同关系自动扩散放大：提升权限上限时，系统会严格依据提交时记录的溯源关系进行合规校验。

隔离区（Quarantine）是一种特殊状态：常规召回检索会自动将其排除，但审查员仍可直接查阅，并附带明确的隔离原因。它既不是归档也不是撤回，而是声明当前大脑暂时不允许对该部分记忆进行常规调用。

### 数据擦除（Erasure）

`PURGE` 受到四重安全机制防护：需要具备显式的 `purge` 权限；法律留存（legal hold）指令可直接一票否决；`REFERENCE POLICY` 默认强制为 `deny_if_referenced`（被引用时禁止删除）；实际擦除后会保留一个携带内容摘要的标识存根（identity stub）。

擦除实体内容的同时会彻底销毁其所有已记录的历史版本 —— 若删除内容却保留版本日志，被删除实体仍能通过 `AS OF` 完全读取。销毁流程严格先删除历史版本、再删除存根：若在两步之间发生系统崩溃，可通过再次执行 purge 进行恢复；若顺序颠倒，系统将残留可读的历史存根且失去关联索引。

### 审计机制（Audit）

控制平面的所有变更均会完整镜像记录到审计日志中，同时附带完整的变更后新记录，鉴权决策结果也一并就地记录。`read_audit` 与 `read_governance_history` 是独立于业务 `read` 的高等级权限：前者记录了人们*做了什么*，后者记录了控制平面本身*经历了何种演变*。

`EffectiveAuthority::resolve_at` 用于回答*“主体在历史时间点 T 拥有何种访问权限”*，这绝不能用当下的权限状态推演历史。高影响操作生成的收据中会固化记录当时批准该操作的主体身份与策略版本。

---

## 11. 认知胶囊（Capsules）

`EXPORT CAPSULE` 利用 KQL 求解器选定根实体，遍历其引用的闭包集合，并导出精确的 Schema 引用与 Package 摘要。`VERIFY CAPSULE` 会重新计算内容摘要，并将工件是否 `signed`（已签名）与是否 `valid`（格式合法）**分开独立报告**，因为未签名的合法工件绝不能被含混标记为已验证。

导入是**宿主专属操作**，而非普通的 KIP 命令：KML 没有导入子句，META 也是纯只读的，防止通过 Prompt 注入直接决定让当前 Space 吸收其他大脑的认知数据。导入模式支持：`preview`、`merge`、`isolate`（直接导入至隔离区）。

实体唯一性对齐按以下优先级解析：历史导入记录 → `canonical_id` → Proposition 元组结构。幂等映射直接以 `client_key`（格式为 `kip:import:{digest}:{source id}`）的形式保存在实体本身上 —— 若使用独立的旁路映射表，旁路表可能在崩溃中残留而实体丢失，导致后续重新导入时错误对齐到不存在的实体。

源端 Space 本地的 `key` **绝不导入**：不同 Space 中的 `person:alice` 完全可能是两个完全不同的人。

---

## 12. 历史时序（History）

每次事务提交都会将所写入的完整数据行追加至 `element_versions` 集合中，且与实体行的写入处于同一提交事务中。若历史写入滞后于数据行，一旦系统在期间崩溃就会出现历史空洞，而带有空洞的历史记录会导致 `AS OF` 查询给出错误应答而非报错。历史记录存储完整行，而非 diff 差量。

历史读取**无法利用索引** —— 索引仅能反映当前最新状态 —— 因此引擎需要从版本日志中重构候选集并重新校验各项约束，开销同样受限于查询候选预算。历史时序读取必须保证三项前提：符号必须基于*该历史坐标下*的 Schema Environment 解析；投影计算只能看到当时已存在的 Assertion；单次读取必须锁定在唯一的确定坐标上。

---

## 13. 宿主 API（Host APIs）

这些接口被刻意设计在命令表面之外，因为它们绝对不属于由大模型 Prompt 决定的操作：

```rust
nexus.install_package(&package, "source")      // 安装不等于激活
nexus.activate_schema(space_id, lock)
nexus.ensure_schema(space_id, lock)            // 若 lock 未发生变更则为无操作（no-op）
nexus.install_and_activate(&artifacts, space)  // 常规引导启动流程
nexus.governance()                             // 底层未受守卫的控制平面直通接口
nexus.session(auth)                            // 建立经身份验证的会话

// 位于 Session 之上的方法，因为每项操作均属于背后有明确 Principal 的治理决策
session.designate_self(space_id, Some(concept)) // 指定该 Space 的语义主体 $self（Spec §5.6）
session.sweep_expired(space_id, action, limit)  // 保留期到期清理（Spec §19.1）
session.expire_lapsed_assertions(space_id, n)   // 断言失效流转至 `expired`（Spec §14.3）
session.classify / elevate_authority / quarantine / release_quarantine

// 针对控制平面的操作，以 Session 当前 Principal 的身份进行授权鉴权（Spec §29）
session.create_grant / revoke_grant                      // 对应权限 manage_grants
session.create_delegation                                // 对应权限 delegate | manage_delegation
session.revoke_delegation                                // 对应权限 manage_delegation
session.put_group / set_principal_status                 // 对应权限 manage_membership
session.create_binding / revoke_binding                  // 对应权限 manage_actor_binding
session.publish_policy                                   // 对应权限 manage_policy
session.approve                                          // 对应权限 approve_high_risk
session.install_package / activate_schema                // 对应权限 manage_schema
session.import_capsule(space, &capsule, isolate)         // 对应权限 import
```

控制平面方法**绝不**向认知命令开放：没有任何 KML 子句或 META 命令能够调用到它们，从而在结构上彻底杜绝 Prompt 注入风险。这些方法之所以存在，是因为由宿主*代表特定 Principal* 发起的调用必须通过该 Principal 的授权校验 —— 例如一个声明了 `manage_grants` 的 Grant 必须真正赋予对应能力，否则该授权就成了虚设。`nexus.governance()` 保持为底层的无守卫通道，因为 Space 在冷启动时必须能够初始化写入其首个 Grant。

`delegate` 与 `manage_delegation` 被严格切分：转授*自身*拥有的部分权限属于前者，而在两个其他 Principal 之间代为管理 Delegation 属于后者。若混为一谈，任何拥有自转授权限的人都将能够随意分发他人的权限。

`designate_self` 属于宿主 API 而非 KML 子句，因为 Spec §5.6 将自身身份的指定归类为受保护的 Space 配置：若允许认知内容指定大脑自身的身份，相当于允许外部数据反客为主定义大脑是谁。

数据清理属于显式调用的清理过程，绝非后台静默定时器。Nexus 是嵌入在调用方进程内的静态库；若后台线程自行按时间间隔删除记忆，这些删除动作发生时不仅没有正在执行的用户请求，也没有任何能够对此负责的主体 Principal。宿主决定*何时*遗忘；引擎决定*哪些可以*被遗忘 —— 法律留存指令可直接阻止包含其授权者在内的清理操作，且清理触及的每个元素都会逐一经过鉴权。

`ensure_schema` 的存在是为了避免重复生成环境版本：每次激活都会生成一个全新的环境版本号，若宿主在每次重启时不加判断地无条件重新激活相同的锁定文件，会导致环境版本无限前移，造成客户端缓存的前置条件失效，并在 `HISTORY` 中充斥无实质变更的 Schema 记录。

---

## 14. 本引擎不承担的职责与边界

在 `DESCRIBE CAPABILITIES` 中以结构化数据及原因明确报告，遇到时直接显式拒绝执行，绝不给出误导性结果：

| 能力缺失项                   | 拒绝原因说明                                                                |
| :--------------------------- | :-------------------------------------------------------------------------- |
| 语义（向量）/ 混合 `SEARCH`  | 未内置 Embedding 模型；显式报错拒绝，绝不静默降级为纯关键字检索            |
| `SEARCH … AS OF SEQ`         | 索引仅反映当下状态；当下的文本匹配不等于历史时间点的匹配                    |
| 跨多操作的原子批处理         | 跨操作的单一事务属于引擎级核心属性，非原子模式无法模拟此语义                |
| 认知胶囊签名                 | 当前未实现密码学签名，`VERIFY` 会将其与 `valid` 分开独立报告                |
| `restore` 胶囊导入模式       | 该模式的核心是将源端 `$self` 映射到目标端 `$self`，而导入绝不可按相似度自动对齐主体 |
| 自动化凭据质量评分           | 未内置自动凭据质量评估模型                                                  |
| 受控跨 Space 视图共享        | 不提供跨 Space 的隐式共享视图                                               |
| Space 级别的统一数据留存策略 | 数据保留期针对每个实体单独配置并在收到请求时显式清理，不支持按类别默认设置  |
| `SEARCH ASSERTION` / `ACTIVITY` | 这两类实体不包含用于全文索引的自由文本；显式报错拒绝，而非返回空结果导致误读 |
| 胶囊摘要算法 Profile 扩展    | 引擎严格锁定使用 kip-jcs-safe-v1 与 SHA-256 计算摘要；拒绝其他算法工件      |
| `options.deadline_ms` 超时配置 | Spec §80.2 规定客户端超时不代表事务中止，且提交无法中途取消；接受该字段会开出无法兑现的取消承诺 |

---

## 15. 测试规范

```bash
cargo test -p anda_cognitive_nexus --all-features
```

包含 11 个集成测试套件以及跨引擎规范一致性测试套件。**编写端到端测试时必须通过真实解析器输入**，绝不要使用手工构造的 AST：本项目中所有典型的隐蔽缺陷都是通过真实解析器发现的，若仅对单函数执行单元测试完全无法暴露这类问题 —— 例如某子句被解析器正常放行、但引擎直接忽略未执行，而收据却虚假报告执行成功。

`tests/conformance.rs` 运行位于 [`fixtures/kip-conformance-2.0/`](../fixtures/kip-conformance-2.0/) 的纯数据测试用例，以便其他语言的引擎能够运行完全相同的用例。测试时 ID 会归一化为 `C:<1>`，时间戳、`tx_id` 与具体得分被忽略。

`tests/governance.rs` 覆盖了 Spec §236–§247 规定的威胁模型测试套件 —— 包括认知内容提权、策略注入、委托权限放大、行动者身份冒用、撤销诚实性校验、检索侧信道防护、派生密级传承、派生权限校验、信任度自我提升、权限撤销、高危审批流以及审计日志防篡改。所有测试严格按攻击场景构建：攻击者完全控制输入，检验引擎是否履行其安全契约。
