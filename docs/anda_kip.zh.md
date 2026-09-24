# `anda_kip` — 技术参考

[English](anda_kip.md)

追踪 KIP 提交 `597db44`，包含草案记忆包 `kip://profiles/cognitive-memory@2.0.0`（内容摘要 `sha256:734aa0fd…`）以及严格的 UTC 毫秒时间戳契约（§6.5）。有关已实现的契约和能力边界，请参阅 [Cognitive Nexus 文档](anda_cognitive_nexus.zh.md)。

`anda_kip` 是 AndaDB 工作区的协议层：负责将 KIP 2.0 命令文本解析为闭合的、可执行的 AST，建模运行时信封（envelope），并定义执行引擎需要实现的接口边界。本 crate 本身不持有状态，也不做任何存储决策。

- **Crate**: `rs/anda_kip`
- **协议**: KIP 2.0（`SPECIFICATION.md` 随 crate 提供）
- **参考实现交叉校验**: `@ldclabs/kip-lang`

## 目录

1. [为什么 KIP 2.0 是一个全新的协议](#1-为什么-kip-20-是一个全新的协议)
2. [Crate 结构布局](#2-crate-结构布局)
3. [语言概览](#3-语言概览)
4. [可执行 AST](#4-可执行-ast)
5. [解析器](#5-解析器)
6. [解析器拒绝的语法及其原因](#6-解析器拒绝的语法及其原因)
7. [错误体系](#7-错误体系)
8. [运行时信封](#8-运行时信封)
9. [执行器框架](#9-执行器框架)
10. [核心数据模型](#10-核心数据模型)
11. [认知胶囊（Cognitive Capsules）](#11-认知胶囊cognitive-capsules)
12. [Function-Calling 集成](#12-function-calling-集成)
13. [实现者检查清单](#13-实现者检查清单)
14. [`kip-cli`](#14-kip-cli)
15. [从 `anda_kip` 0.11（KIP 1.x）迁移](#15-从-anda_kip-011kip-1x迁移)

---

## 1. 为什么 KIP 2.0 是一个全新的协议

KIP 1.x 存储自描述的 Concept/Proposition 图，其中 Proposition 携带了 `metadata.confidence`、`author` 字符串以及 `access_level`。这种单一平面将四个完全不同的问题混为一谈：

- 这*意味着*什么？
- 谁在*声称*它，其确定性有多大？
- 实际*观察*到了什么？
- 谁被*允许*查看它？

KIP 2.0 将它们彻底解耦，本 crate 中的一切设计均遵循一条基本公理：

```text
命题（Proposition）的存在  ≠  命题为真
```

| 元素 | 定义 | 可变性 |
| --- | --- | --- |
| **Concept** | 可引用的具型实体（Entity） | 可变状态 |
| **Proposition** | 真值中立的 `(subject, predicate, object)` 元组 | 元组不可变 |
| **Assertion** | 某一行动者（Actor）对某命题的主张与承诺 | 载荷不可变；通过取代（supersession）修订 |
| **Evidence** | 实际观察到的原始凭据 | 载荷不可变；通过血统链（lineage）更正 |
| **Activity** | 过程的溯源记录（Provenance） | 一旦进入终态即不可变 |

信念（Belief）是在指定命名策略下从断言（Assertions）中**投影（projected）**出来的，从不持久化存储。正因如此，`BELIEF` 仅存在于 KQL 中——投影绝不能作为修改目标或导出选择器。

API 刻意区分的概念界限：

```text
缺失 (missing)              ≠ 为假 (false)（开放世界假定：证据不足，而非已被拒绝）
检索得分 (search score)     ≠ 置信度 (confidence)
置信度                     ≠ 信任度 ≠ 记忆强度 ≠ 显著性 (salience)
名称 (name)                 ≠ 唯一标识 (identity)
主体 Principal（调用者）    ≠ 语义行动者 semantic actor（声称归属方）
认知内容                   ≠ 权限 (authority)
批处理 (batch)             ≠ 事务 (transaction)
超时 (timeout)             ≠ 中止 (abort)
进度 (progress)            ≠ 提交 (commit)
```

---

## 2. Crate 结构布局

```text
rs/anda_kip/
├── src/
│   ├── lib.rs          重新导出、内置提示词与工具 Schema
│   ├── ast.rs          可执行 AST（与 kip-lang 的 exec-ast.ts 镜像）
│   ├── parser.rs       入口点、输入预算、转义辅助函数
│   ├── parser/
│   │   ├── common.rs   词法层 + 三种语法共用的规则
│   │   ├── kql.rs      FIND 查询
│   │   ├── kml.rs      数据变更、ASSERT 脱糖、变更新约束保护
│   │   ├── meta.rs     DESCRIBE / LIST / SEARCH / VERIFY / … / EXPORT CAPSULE
│   │   └── json.rs     面向大模型的 JSON 方言解析器
│   ├── semantics.rs    Core Package 注册表、协议固定范围校验
│   ├── error.rs        Core Error Registry、错误类别、重试类别
│   ├── request.rs      请求/响应信封模型
│   ├── types.rs        核心数据模型 + 规范的读取结构
│   ├── capsule.rs      认知胶囊（Cognitive Capsules）、规范序列化
│   ├── conformance.rs  实现声明的 Profile 名称
│   ├── executor.rs     执行引擎接入边界（Seam）
│   └── bin/kip_cli.rs  语法检查 CLI 工具
├── schemas/            规范的请求/响应网络通信 Wire Schemas
├── grammar/            规范的 KQL / KML / META EBNF 语法定义
├── SPECIFICATION.md    规范的 KIP 2.0 规格说明书
├── KIPSyntax.md        面向 LLM 的语法速查手册
├── SelfInstructions.md Agent 应当如何使用自身记忆的指令规范
└── SystemInstructions.md 运行时环境对调用方承诺的指令规范
```

内置的 `schemas/` 与 `grammar/` 并非摆设：`tests/wire_schema.rs` 会对 Rust 信封类型与 Schema 进行双向交叉验证。任何偏离通信协议契约的类型更改都会直接导致构建失败，从而杜绝跨端不一致。

---

## 3. 语言概览

### 3.1 KQL — 读取查询

```prolog
FIND(?drug.name, COUNT(DISTINCT ?trial))
WHERE {
    ?drug CONCEPT {type: "Drug"}
    (?drug, "studied_in", ?trial)
    OPTIONAL { ?edge STRUCTURAL (?drug, "has_step", ?step) }
    FILTER(?drug.attributes.risk_level < 3)
}
AS OF SEQ 4200            // 指定认知历史时序
FOR TIME :world_time      // 指定现实有效时间（独立维度）
WITH EPISTEMIC { explain: "summary" }
ORDER BY COUNT(?trial) DESC
LIMIT 10
CURSOR :page
```

模式家族：`CONCEPT`、`PROPOSITION`、`ASSERTION`、`EVIDENCE`、`ACTIVITY`、`STRUCTURAL`、`BELIEF`、`BELIEF SLOT`，外加 `FILTER`、`NOT`、`OPTIONAL`、`UNION`。

原始读取与信念查询属于两个不同的问题：

```prolog
// 查出曾被作出的全部主张，真值中立
?p (:alice, "timezone", ?tz)

// 查出当前被采信的信念，以及其争议程度
?b BELIEF (:alice, "timezone", ?tz)
?slot BELIEF SLOT (:alice, "timezone")
```

`BELIEF` 从不遍历原始路径：分支交替与跳数限定词属于图遍历语法，而信念不会沿着遍历路径自然传导。

### 3.2 KML — 数据变更

```prolog
MUTATE {
    CREATE CONCEPT ?alice { TYPE "Person" CLIENT KEY "person:alice" NAME "Alice" }
    CREATE EVIDENCE ?msg  { SET FIELDS { evidence_class: "user_statement" } }
    ASSERT ?claim (?alice, "prefers", :dark_mode) {
        by: ?alice, mode: "stated", confidence: 0.9, evidence: ?msg
    }
    UPSERT CONCEPT ?drug {
        MATCH {key: "drug:aspirin"}
        SET ATTRIBUTES { risk_level: 2 }
    } EXPECT VERSION 3
    TRANSITION :old_claim TO "superseded" BY ?claim
}
```

语句家族：`CREATE CONCEPT` / `EVIDENCE` / `ASSERTION` / `ACTIVITY`、`UPSERT CONCEPT`、`ENSURE PROPOSITION`、`ASSERT`、`UPDATE`、`TRANSITION`、`SET RETENTION`、`PURGE`、`PURGE PAYLOAD`、`MERGE CONCEPT`。

`TRANSITION <target> TO "<state>" [BY <ref>] [SET FIELDS] [SET STRUCTURAL] [WHERE] [LIMIT] {EXPECT VERSION}` 是唯一的生命周期状态变更语句（Spec §52.5）：被引用的状态指明流转动作——Assertion 的 `retracted` / `superseded BY`；Evidence 的 `corrected BY`；Activity 的 `running` / `completed` / `failed` / `cancelled`（在同一语句中定稿字段与拓扑）；任何元素的 `archived` / `tombstoned`。引擎会根据目标类别和当前状态进行严格合法性校验（`InvalidLifecycleTransition`）。不存在 `EXPECT STATE` 语法。

每个变更语句均以相同的结构结尾（§52.8）：`[WHERE] [LIMIT] {EXPECT VERSION}`，该守卫紧随 `UPSERT` 的闭合括号或 `ENSURE PROPOSITION` 的元组之后。`EXPECT VERSION :v [OF ATTRIBUTES | STRUCTURAL | RETENTION | FACET "X"]` 守卫单个**版本平面（version plane）**（§35.1），可多次出现（每个平面至多一个守卫），因此针对同一元素的 Facet 清理与属性写入不会相互破坏版本守卫；解析器会拒绝重复守卫同一平面的语法。

单条语句独立编写时在语义上依然属于单子句事务；`explicit_transaction` 仅记录源代码所采用的书写形式。

### 3.3 META — 认知对齐与自省

```prolog
DESCRIBE PRIMER MODE "compact"
DESCRIBE TYPE "Person"
LIST SCHEMA PACKAGES STATUS "active" LIMIT 20
LIST DEPENDENTS "C-1" DEPTH 2 LIMIT 50
SEARCH CONCEPT "dark mode" MODE "hybrid" THRESHOLD 0.7 LIMIT 5
HISTORY ELEMENT "C-1" FROM SEQ 1 TO SEQ 99
VERIFY CAPSULE :artifact
VALIDATE KML :command
PREVIEW IMPORT CAPSULE :capsule INTO "space-1"
EXPORT CAPSULE :out WHERE { ?c CONCEPT {type: "Experience"} } AS OF SEQ 7
```

META 在语义上是纯只读的。`VERIFY`、`VALIDATE`、`PREVIEW` 以及真正的提交（commit）是四种不同的操作，绝不可混淆。

---

## 4. 可执行 AST

`anda_kip::ast` 是**可执行（executable）**树，而非普通语法解析树。所有开放式语法位置均已闭合：

- 谓词只能是 `PredTerm::Atom` 或 `PredTerm::Path`，绝不会是嵌套的交替/量词树；
- 过滤条件只能是比较表达式、逻辑节点、否定表达式或对已注册 `FilterFunction` 的调用；
- 变量由变量名加由 `PathStep` 组成的路径表达；
- `ASSERT` 语句已被完全消除——解析器已将其脱糖（desugared）。

匹配这些枚举的消费者具备完备性（total pattern matching）。

### 4.1 通信兼容性（Wire Compatibility）

序列化采用 serde 默认的外部标签化（externally-tagged）表示，与 `@ldclabs/kip-lang` 中的 `exec-ast.ts` 字段逐一对应：

```rust
use anda_kip::{KipValue, Scalar, SymbolRef};

assert_eq!(serde_json::to_string(&KipValue::Null)?, r#""Null""#);
assert_eq!(serde_json::to_string(&Scalar::Param("limit".into()))?, r#"{"Param":"limit"}"#);
assert_eq!(serde_json::to_string(&SymbolRef::Name("has_step".into()))?, r#"{"Name":"has_step"}"#);
# Ok::<(), serde_json::Error>(())
```

这确保了 Rust 引擎与 TypeScript 工具链能够针对相同的规范测试用例（conformance fixtures）进行差分测试。

### 4.2 值插槽（Value Slots）

| 类型 | 语法位置 | 说明 |
| --- | --- | --- |
| `KipValue` | 字面量 | `Null` 为单元变体 |
| `Scalar` | `parameter \| literal` | 未绑定的 `:name` 保留为 `Param` |
| `BoundValue` | `data_value` | 无需绑定时折叠为 `Value` |
| `MutationValue` | 赋值表达式右侧 | 为 `ADD`/`MUL`/`CLAMP`/`COALESCE` 引入 `Expr` |
| `MatchValue` | `pattern_value` | 位于 `ObjectMatcher` 内部 |
| `Term` | 元组端点 | 本身亦可为 Proposition |
| `SymbolRef` | `schema_symbol` | 引用标识或命名参数 |
| `ElementRef` | `target_ref` | `Handle` / `Param` / `Id` |

一处关键的不对称性：在**值（Value）**位置上，单独的 `?x` 是 `Handle`——代表本执行计划所创建的某个元素。而 `?x.field` 则是对该元素自有字段的*读取*并保留访问路径。在**匹配（Match）**位置上，`?x` 则是变量绑定。

---

## 5. 解析器

### 5.1 入口点

```rust
use anda_kip::{parse_kip, parse_kql, parse_kml, parse_meta, parse_json};

let command = parse_kip(r#"DESCRIBE PRIMER"#)?;          // 任意语言子集
let query   = parse_kql(r#"FIND(?x) WHERE { ?x {a: 1} }"#)?;
let mutation = parse_kml(r#"TRANSITION :old TO "archived""#)?;
let meta    = parse_meta(r#"DESCRIBE SNAPSHOT AT TIME "2026-01-01T00:00:00.000Z""#)?;
let value   = parse_json(r#"{ a: 1, /* not JSON5 */ }"#).is_err();
# Ok::<(), anda_kip::KipError>(())
```

每个入口点都是全量消费型的：一段输入对应一条命令。单个字符串中包含两条命令会被报错，而不是静默执行第一条。

### 5.2 词法规则

- **关键字对 ASCII 大小写不敏感**（`find` == `FIND`），规范形式采用全大写。
- **关键字属于上下文关键字，非全局保留字**。`by`、`mode`、`key`、`name`、`type` 与 `status` 在规范示例中均可用作字段名，`?a.lifecycle.status` 是合法的点路径。
- **`true` / `false` / `null` 区分大小写**——它们属于 JSON 字面量而非协议关键字。`(id: "P-1")` 中的 `id` 亦同。
- 行注释以 `//` 开头，视作 Token 分隔符。
- Token 之间的空白字符无语法含义。

### 5.3 输入预算

```rust
use anda_kip::{MAX_KIP_INPUT_LEN, MAX_KIP_NESTING_DEPTH, MAX_KIP_BATCH_COMMANDS};
```

输入长度和括号嵌套深度在正式解析前通过轻量预扫描进行检查，防止恶意构造的载荷耗尽调用栈。预扫描与解析器保持完全相同的逻辑跳过字符串字面量与行注释——否则注释内的 `"` 会错误触发字符串模式导致后续括号漏记。

### 5.4 错误报告

解析失败会返回行号、列号以及预期的 Token 描述：

```text
InvalidSyntax: at line 3, column 12: expected a literal or a :parameter, found "}"
```

---

## 6. 解析器拒绝的语法及其原因

这些规则属于 Schema 无关的不变式，因此在语法解析层直接强制拒绝，无需留待引擎在运行时报错。每一项规则的存在，都是为了防止认知记录被静默破坏。

| 拒绝的语法 | 原因 |
| --- | --- |
| `UPSERT CONCEPT ?c { MATCH {name: "Alice"} }` | `name` 是可变且可重复的；“名为 X 的 Concept”随时间演化会静默指向不同节点。必须基于 `id` 或 `key` 匹配。 |
| 对 Assertion 执行 `UPDATE ?a SET FIELDS {confidence: …}` | 认知载荷是不可变的。必须使用 `SUPERSEDING` 记录新的 Assertion。 |
| 对 Evidence 执行 `UPDATE ?e SET FIELDS {payload: …}` | 证据不可篡改。必须使用 `TRANSITION :old TO "corrected" BY :new`。 |
| 对 Proposition 执行 `UPDATE ?p SET FIELDS {subject: …}` | 元组不同即意味着不同的命题。 |
| 对 Assertion / Evidence / Activity 执行 `SET STRUCTURAL` | 记录的拓扑属于不可变载荷；挂起的 Activity 通过 `TRANSITION … TO "completed" SET STRUCTURAL` 定稿。 |
| `TRANSITION :a TO "superseded"` 缺少 `BY`；或 `TRANSITION :x TO "archived" BY :y` | `BY` 仅且必须用于 `superseded` / `corrected` 来指定替代元素（§52.5）。 |
| `TRANSITION :a TO "retracted" SET FIELDS {…}` | 仅流转至 Activity 终态时才允许最终写入字段或拓扑。 |
| `TRANSITION :a TO "retracted" EXPECT STATE "active"` | 协议不存在 `EXPECT STATE`：状态流转自身即对当前状态进行校验（§35.3）。 |
| `UPDATE :x EXPECT VERSION :v SET …`、`EXPECT VERSION 1 OF ATTRIBUTES EXPECT VERSION 2 OF ATTRIBUTES` | 守卫必须作为末尾子句，且每个版本平面至多一个（§52.8, §35.1）。 |
| `FIND … AS OF TX "tx-1"`、`AS OF TIME :t` | `AS OF SEQ` 是唯一的历史时间轴（§48.1）；需通过 `DESCRIBE TRANSACTION` / `DESCRIBE SNAPSHOT AT TIME` 将 ID 或时间点解析为序列号。 |
| 任何对 `_system`、`governance`、`space_id`、`space_seq` 的赋值 | 属于引擎管理的状态；外部认知逻辑不得越权自我提权。 |
| `ENSURE PROPOSITION (id: "P-1")` | `(id: …)` 仅用于匹配——绝不能仅凭 ID 创建结构。 |
| `ASSERT` 缺少 `by` 或缺少 `mode` | 臆测行动者会导致伪造归属；臆测模式会将传闻误作为直接观察。 |
| 在 KML 或 `EXPORT CAPSULE` 中使用 `BELIEF` | 投影（Projection）是虚拟的且纯只读。 |
| `BELIEF (?s, "a"\|"b", ?o)` | 信念不会沿着原始图路径传播。 |
| 在对 `?c` 的 `UPDATE` 中使用 `ADD(?other.n, 1)` | 更新表达式仅允许读取被更新元素本身；否则其结果将依赖未在语句中声明的隐式 Join。 |
| 两个子句声明同一个 `?handle` | 对其后续的前向引用将产生歧义。 |
| `?handle` 既未在执行计划中绑定，也未在子句的 `WHERE` 中绑定 | 抛出 `ReferenceError`，而非产生悬空写入。 |
| `PURGE … CONFIRM "purge"` | 确认语法的拼写是严格锁定的（全大写 `"PURGE"`）。 |
| 任何块内存在重复的 Key | 重复的 Key 通常是生成逻辑的疏漏；若按“最后写入者胜”静默覆盖将掩盖错误。 |

### 6.1 核心包（Core Package）注册表

`kip://core@2.0.0` 是规范本身定义的虚拟 Schema Package（§20.13）：在每个 Schema 环境中隐式处于活跃状态，永不失效且不可被遮蔽。因此其注册表在引擎介入前即可完成确定性判定。`semantics` 负责强制执行以下约束：

| 拒绝的值 | 注册表规范 |
| --- | --- |
| `stance: "maybe"` | `support \| reject \| uncertain` (§13.4) |
| `mode: "guessed"` | `observed \| stated \| inferred \| predicted \| hypothetical \| imported` (§13.5) |
| `("evidence", :e) {role: "bogus"}` | `support \| challenge \| context` (§56.2) |
| `TRANSITION … TO "succeeded"` | `retracted \| superseded \| corrected \| running \| completed \| failed \| cancelled \| archived \| tombstoned` (§52.5) |
| `SEARCH … MODE "fuzzy"` | `keyword \| semantic \| hybrid` (§66.3) |
| `DESCRIBE PRIMER MODE "verbose"` | `compact \| full` (§64) |
| `WITH EPISTEMIC { explanation: "verbose" }` | `none \| summary \| ledger` (§49.1) |
| `confidence: 5`、`THRESHOLD 5` | 取值范围 `[0,1]` (§13.6, §66) |

本层严格遵守的两条边界：

- **仅校验字面量。** `:parameter` 在执行时由信封绑定，因此 `mode: :mode` 永远放行——提前猜测会错误拒绝合法的动态命令。
- **仅校验协议锁定的词汇表。** 认知记忆 Profile 虽然也将 `memory_strength`、`salience` 和 `utility` 约束在 `[0,1]`，但它们属于扩展 *Package*：运行其他 Profile 的 Space 完全可能有其他定义。这些校验留给引擎层处理，因为只有引擎知晓当前活跃的 Schema 环境。同理，哪种 `TRANSITION` 状态匹配哪种实体类别、当前状态允许执行何种流转，在此处不作检查——§52.5 规定这是引擎层抛出 `InvalidLifecycleTransition` 的职责；语法层仅锁定状态的词汇空间。

`analyze` 还会返回针对潜在问题的警告信息，以便工具进行分析提示（如未受限的 `PURGE`、缺少 `LIMIT` 的 `FIND`、声明 `mode: "observed"` 却未提供 Evidence）：

```rust
use anda_kip::{Severity, analyze, parse_kip};

let command = parse_kip(r#"PURGE ?x WHERE { ?x {type: "Draft"} } CONFIRM "PURGE""#).unwrap();
let findings = analyze(&command);
assert!(findings.iter().any(|d| d.severity == Severity::Warning));
```

### 6.2 `ASSERT` 语法糖脱糖

`ASSERT` 是规范定义的语法糖（§55.1），解析器会将其严格展开为规范定义的三分子句，绝不凭空臆造多余逻辑：

```prolog
ASSERT ?a (:alice, "prefers", :dark_mode) {
    by: :alice, mode: "stated", confidence: 0.9, evidence: [:e1, :e2]
} SUPERSEDING :old
```

展开为三条子句：

1. `EnsureProposition { handle: "a#proposition", … }`
2. `CreateAssertion { handle: "a", set_fields: [proposition, asserted_by, mode, stance, confidence], set_structural: [("evidence", e1){role}, ("evidence", e2){role}] }`
3. `Transition { target: :old, to: "superseded", by: Handle("a") }`

关键细节：

- `stance` 默认为 `"support"` 并会被**物化（materialized）**，而非留给引擎隐式推导；
- `evidence` 属于保留的*拓扑（structural）*字段，因此数组形式会展开为每个凭据对象一条带角色修饰的边；
- 未指定句柄的 `ASSERT` 会根据子句位置分配一个形如 `#assert{N}` 的合成句柄，确保单次 `MUTATE` 内互不冲突（`#` 无法出现在合法的 KIP 标识符中，因而也不会与用户句柄冲突）。

---

## 7. 错误体系

KIP 2.0 废弃了 1.x 的数字错误码（`KIP_xxxx`），全面采用由稳定命名组成的错误注册表。

```rust
use anda_kip::{ErrorCategory, KipError, KipErrorCode, RetryClass};

let err = KipError::version_conflict("element changed since you read it");
assert_eq!(err.name(), "VersionConflict");
assert_eq!(err.category(), ErrorCategory::Transaction);
assert_eq!(err.retry_class(), RetryClass::RequiresRefresh);
assert!(err.effective_hint().contains("EXPECT VERSION"));
```

网络传输格式（§86.1）：

```json
{
  "code": "SchemaSymbolAmbiguous",
  "category": "schema",
  "message": "…",
  "hint": "…",
  "retry": {"class": "requires_different_input"},
  "details": {}
}
```

重试类别（Retry Classes）是面向调用方恢复逻辑的显式契约：

| 类别 | 含义 |
| --- | --- |
| `safe_same_request` | 未发生任何持久化变更；可按原样重新发送 |
| `requires_refresh` | 重新读取当前最新状态后重试 |
| `requires_different_input` | 必须修改请求参数 |
| `requires_authority` | 调用方缺少权限，而非信息不足 |
| `requires_new_snapshot` | 需要获取新的快照坐标 |
| `requires_reacquire_artifact` | 重新暂存数据字节 |
| `outcome_lookup_required` | 写入结果未决——**严禁直接重放原请求** |
| `non_retryable` | 不可重试 |

`KipErrorCode::ALL` 枚举了全部 79 个注册错误码；`KipErrorCode::from_name` 支持逆向解析。在需要避免由于区分“不存在”与“无权访问”而泄露实体存在性的场景下，统一使用 `NotFoundOrNotVisible`。

---

## 8. 运行时信封

### 8.1 请求（Request）

```rust
use anda_kip::{Execution, ExecutionMode, Operation, Request};

let request = Request {
    request_id: Some("req-1".into()),
    execution: Some(Execution {
        idempotency_key: Some("logical-write-key".into()),
        ..Execution::new(ExecutionMode::Atomic)
    }),
    operations: vec![Operation::new(r#"TRANSITION :old TO "archived""#).with_op_id("op-1")],
    ..Default::default()
};
request.validate()?;
# Ok::<(), anda_kip::KipError>(())
```

`Request::validate` 负责校验无需引擎介入即可判定的信封不变式：协议版本有效性、`operations[]` 非空且有界、多操作时显式声明 `execution.mode`、`atomic` 严禁与 `on_error: continue` 混用、`op_id` 全局唯一、每个 operation 仅允许包含 `command` 或 `ast` 之一、参数与摄取绑定名称格式正确。

### 8.2 执行模式（Execution Modes）

| 模式 | 保证语义 |
| --- | --- |
| `independent` | 各操作具备独立的快照与事务；故障完全隔离 |
| `sequence` | 有序执行；每个状态变更独立提交，且**已提交的操作绝不回滚** |
| `atomic` | 单一事务，单一快照，支持写后读（read-your-writes），全成功或全失败（All-or-None） |

批处理不是事务，除非执行模式明确指定为 `atomic`。中途发生故障的 `sequence` 执行会被报告为 `partial`，绝不能标为 `failed`——否则调用方误以为全盘失败而重放写入，将导致重复写入。

### 8.3 数据摄取（Ingestion）

实际观察到的凭据数据应通过传输信封进入 Evidence，而非嵌入在模型生成的命令文本中：

```json
{
  "kip": "2.0",
  "ingest": {"evidence": [{
    "key": "msg",
    "evidence_class": "user_statement",
    "payload": "I prefer dark mode.",
    "observed_at": "2026-08-14T01:00:00.000Z"
  }]},
  "operations": [{
    "language": "KML",
    "command": "ASSERT (:alice, \"prefers\", :dark_mode) { by: :alice, mode: \"stated\", evidence: :msg }"
  }]
}
```

每个 `key` 会作为参数绑定到新生成的 Evidence。生成过程具备事务性：若事务中止，绝不会持久化产生无主 Evidence。

### 8.4 响应（Response）

```rust
use anda_kip::{OperationResult, Response, TopLevelStatus};

let response = Response::from_results(vec![OperationResult::ok(serde_json::json!({"n": 1}))]);
assert_eq!(response.status, TopLevelStatus::Succeeded);
```

顶层状态为 `succeeded` / `failed` / `partial` / `outcome_unknown`；单个操作状态为 `succeeded` / `failed` / `skipped` / `rolled_back` / `no_effect`。`Receipt` 包含 `tx_id`、`space_seq`、摘要及加密证明；`ResultContext.search` 返回 `index_seq` 与 `current_space_seq` 的对比，防止将存在落后的索引伪装为快照一致。

---

## 9. 执行器框架

```rust
use anda_kip::{Command, Executor, Operation, Request, Response};
use async_trait::async_trait;

struct MyNexus;

#[async_trait]
impl Executor for MyNexus {
    async fn execute(
        &self,
        command: Command,
        request: &Request,
        operation: &Operation,
    ) -> Response {
        let dry_run = request.is_dry_run();
        let _operation_parameters = &operation.parameters;
        match command {
            Command::Kql(_query) => todo!("run the read"),
            Command::Kml(_statement) => todo!("run the transaction"),
            Command::Meta(_command) => todo!("answer the introspection"),
        }
    }
}
```

内置辅助方法：

- `execute_kip(executor, text, dry_run)` — 解析、分类并执行；
- `execute_readonly(executor, text, dry_run)` — 同上，但基于*命令本身的语义*拒绝一切产生状态变更的命令，而非依赖声明的标签；
- `execute_request(executor, &request)` — 执行 `independent` 与 `sequence` 模式。明确拒绝 `atomic` 模式而非进行简陋模拟：单事务、单快照与全原子提交属于引擎核心能力，简单的 `Executor` 循环调用无法保证这些语义。执行器接收完整的请求与操作上下文，必须正确遵循每个适用的信封字段，否则显式报错。

---

### SDK 校验与 wire 值

文本与外部 AST 共用查询主语、路径、META 操作数、精确匹配和重复对象键校验。
mutation handle 必须由同一计划输出或当前子句的 WHERE 绑定；NOT 内的新变量不向外导出。
Assertion 的枚举、区间规则不用于自定义属性和 Facet 成员。

原始传输输入使用 `Request::from_json`，保留重复键和数字精度检查。
`Request::parse_operations` 与 `execute_request` 在一次准备过程中复用解析结果，
包含 ingest 的请求也不再重复解析。显式 `payload: null`、`result: null` 保留为存在的值。
extension 值必须是对象，其中 `critical` 若存在则必须是布尔值。

批处理 helper 将 independent/sequence 中每次事务的回执放在 `results[i].receipt`；
顶层回执留给 atomic 执行，该 helper 不实现 atomic。恢复未知结果时应读取对应操作的回执。

Core 类型保留 Concept 的结构关系、合并引用及 Evidence 的 payload 清除标记。
生命周期引用数组使用 `Vec<Json>`，兼容 portable `{id}` 对象和已有 native view 的 ID 字符串；
portable artifact 的合法性仍由 vendored schema 判断。

---

## 10. 核心数据模型

`anda_kip::types` 建模了实体信封及各类核心实体：`Concept`、`Proposition`、`Assertion`、`Evidence`、`Activity`，每个实体均携带包含 `governance`、`retention`、`facets` 与 `_system` 的 `ElementEnvelope`。

**字段命名以规范为准，而非本 crate 自行定义。** §13.2 与 §15.3 严格锁定了各插槽命名，任何引擎若擅自重命名，都将导致跨引擎读取方对同一记录产生理解偏差：

```text
Assertion   proposition   asserted_by   stance   mode   confidence
            asserted_at   valid_time    evidence   context_refs   lifecycle
Evidence    evidence_class  payload  content_digest  media_type
            observed_at   source   generated_by   lifecycle
```

所有引用字段均持有对象（如 `{"id": "P-1"}`）而非单纯的 ID 字符串；凭据引用形式为 `{"id": "E-1", "role": "support"}`。§8 规定同一位置可合法存放本地 ID、经校验的 `canonical_id` 以及（作为扩展）跨 Space 引用，纯字符串无法表达这种结构。

系统中**不存在通用的、作者可自由写入的 metadata 字典**。各类数据必须严格放置在其专属槽位：

```text
语义载荷 (semantic payload)        → 具型字段 / attributes
认知状态 (epistemic state)         → Assertion
原始观测凭据 (observations)        → Evidence
溯源链 (provenance)                → Activity / _system.origin
治理状态 (governance)              → Governance state
存储生命周期 (storage lifecycle)   → retention
记忆/Profile状态 (mnemonic state)  → Facets
引擎内部真实数据 (engine truth)     → _system
```

枚举体系：`Stance`（`support`/`reject`/`uncertain`）、`AssertionMode`（`observed`/`stated`/`inferred`/`predicted`/`hypothetical`/`imported`）、`AssertionStatus`、`BeliefStatus`（`accepted`/`rejected`/`contested`/`uncertain`/`insufficient`），以及推荐的 `EVIDENCE_CLASSES` 与 `ACTIVITY_CLASSES` 列表。

`BeliefStatus::is_decided()` 仅在取值为 `accepted` 或 `rejected` 时返回 `true`。`insufficient` 代表开放世界中的未知状态，绝不可解读为“否定”。

---

## 11. 认知胶囊（Cognitive Capsules）

```text
胶囊原始字节  ≠  目标 Space 的变更权限
```

`anda_kip::capsule` 建模了胶囊工件帧结构——清单（manifest）、来源、Schema 依赖、数据记录、外部引用、二进制块（blobs）、处理策略、完整性校验——以及导入相关的词汇体系。记录载荷保持为 JSON 格式，因为记录包含哪些具体字段由活跃的 Schema Packages 决定，并在导入时由目标环境进行校验。

`Capsule::validate_frame` 是低开销的结构门禁：校验格式、内容摘要，以及增量胶囊中的 `base_seq`/`target_seq`。它不是 `VALIDATE CAPSULE`，后者需要具体引擎和目标 Space 参与。

`CapsuleRecords` 以一个有序 `Vec<Json>`（`.0`）保存记录；使用
`records.by_kind(ElementKind::Concept)` 按种类借用，不能为分组而改变参与摘要的数组顺序。
增量 `payload.changes` 使用 `Option<Vec<Json>>`，区分缺失与显式空数组。
`CapsuleHandling.extra` 保存完整 handling 对象，`CapsuleProof` 是 JSON 成员映射，
保留套件自定义字段及显式 null。`canonical_payload()` 覆盖 `format`、
`format_version` 和 `payload`，排除 `integrity`。

旧 SDK 调用点应将 `records.concepts` 等分组字段读取改成 `by_kind`，有序修改改成 `.0`，
用 `CapsuleRecords(vec![...])` 构造；handling 和 proof 成员放入对应映射。
直接构造 `CapsuleIntegrity` 时需要提供新增的可选 `covers` 字段。


`ImportMode` 包括 `preview` / `isolate` / `merge` / `restore`，且 `ImportMode::may_map_self()` 仅在 `restore` 模式下为 true：源端 `$self` 严禁静默映射为目标端 `$self`。`IdentityResolution::ORDER` 规定了保守的冲突解决序列，最终落脚于“新建实体”。`ExternalRefKind` 明确区分 `redacted`（源端刻意隐匿）与 `unavailable`（源端本身缺失）。

---

## 12. Function-Calling 集成

```rust
use anda_kip::{KIP_FUNCTION_DEFINITION, KIP_READONLY_FUNCTION_DEFINITION};

let write_tool = KIP_FUNCTION_DEFINITION.clone();      // execute_kip
let read_tool  = KIP_READONLY_FUNCTION_DEFINITION.clone(); // execute_kip_readonly
```

同时内置以下供系统提示词使用的参考文档：

- `KIP_SYNTAX` — 面向大模型的语法速查手册；
- `SELF_INSTRUCTIONS` — Agent 应当如何使用自身记忆的指令；
- `SYSTEM_INSTRUCTIONS` — 运行时系统对调用方承诺的规范。

---

## 13. 实现者检查清单

执行引擎必须维护的协议不变式；`anda_kip` 不会替你自动强制这些规则，因为每一项都需要具体的引擎状态支撑。

**唯一标识（Identity）**
- 由引擎分配、不透明且绝不重复使用的本地 `id`
- `key` 针对 `(space_id, schema_ref, key)` 三元组保持不可变且唯一
- `client_key` 发生冲突时返回 `ClientKeyConflict`
- `MERGE CONCEPT` 保持非破坏性；历史原始引用必须始终能够正常解析

**认知原则（Epistemics）**
- Proposition 元组不可变；每个语义元组对应唯一的规范 Proposition
- Assertion 载荷不可变；修订必须通过新建 Assertion + 建立取代关系实现
- `TRANSITION … TO "retracted"` 仅允许由断言提出者或其授权代表执行
- 严禁仅因缺乏支撑证据而直接得出 `rejected`（否定）结论
- 同一消息的 N 份副本仅构成单一证据基础，而非 N 份独立证据

**治理与安全（Governance）**
- 拒绝覆盖（Deny overrides）；协议不变式优先于一切自定义策略
- `_system`、`governance`、`space_id`、`space_seq` 绝不允许作者直接写入
- 在实体存在性本身需要保密的场景下，统一使用 `NotFoundOrNotVisible`
- 警惕聚合统计泄漏：针对不可见元素执行 `COUNT` 属于信息泄漏

**事务性（Transactions）**
- `atomic` 意味着单一 `tx_id`、单一快照以及单一发生状态变更的 `space_seq`
- 在提交时刻（而非仅仅计划阶段）严格履行每个 `EXPECT VERSION` 守卫约束（无论是全局版本还是某个版本平面 `OF`）；若不匹配，在 `VersionConflict.details.plane` 中指明冲突平面
- 幂等键（Idempotency keys）的作用域限定在具体的 Space 与授权主体内；相同的幂等键搭配不同的请求载荷 → 抛出 `IdempotencyConflict`
- 当执行结果无法明确确定时，返回 `outcome_unknown`

**读取规范（Reads）**
- `AS OF` 与 `FOR TIME` 是相互独立的时间维度
- 历史时序读取必须使用历史 Schema 解析，若不可用则返回 `HistoricalSchemaUnavailable`
- 游标必须防伪造；游标失效时应直接报错，绝不可静默重新对齐锚点
- 显式报告 SEARCH 索引的落后状态，不得将其误导为严格快照一致

**有界变更（Bounded Mutation）**
- `UPDATE`、`TRANSITION`、`SET RETENTION`、`PURGE` 以及 `PURGE PAYLOAD` 均接受 `LIMIT`；对未限制数量的变更应直接拒绝，绝不擅自猜测执行
- 默认禁止破坏性级联删除

---

## 14. `kip-cli`

```bash
cargo run -p anda_kip --bin kip-cli -- path/to/file.kip path/to/dir
```

遍历指定的文件与目录，解析所有 `.kip` 文件。解析成功打印分类后的语言类型，失败则打印错误信息以及恢复提示。任何文件解析失败均以非零状态码退出。

---

## 15. 从 `anda_kip` 0.11（KIP 1.x）迁移

这是一次破坏性的重构。1.x API 被直接移除而非废弃，因为其背后的底层语义已经彻底改变。

| 0.11 (KIP 1.x) | 0.13+ (KIP 2.0) |
| --- | --- |
| `UPSERT { CONCEPT ?c {…} }` | `CREATE CONCEPT` / `UPSERT CONCEPT` / `ASSERT` |
| `DELETE` | `TRANSITION … TO "archived"` / `TO "tombstoned"` / `PURGE` / `TO "retracted"` — 显式分类变更意图 |
| Proposition 上的 `metadata.confidence` | `Assertion.confidence`；Proposition 本身无置信度 |
| `metadata.author` | `Assertion.asserted_by`（语义行动者）与 `_system.origin`（引擎内部真相）— 完全解耦 |
| `access_level` | 治理分类（Governance classification）与访问策略 |
| 数字错误码 `KIP_3002` | 具名错误码 `KipErrorCode::NotFoundOrNotVisible`，携带类别与重试分类 |
| `Response::Ok/Err` 枚举 | `Response` 结构体，包含 `status` + `results[]` |
| `ConceptNode` / `PropositionLink` | `Concept` / `Proposition` / `Assertion` / `Evidence` / `Activity` |
| Genesis `.kip` 胶囊（`GENESIS_KIP` 等） | 移除 — Schema 是不可变的 Package 状态，而非图中的普通节点 |
| `capsule.rs` = 引导源文件 | `capsule.rs` = 可移植的认知胶囊（Cognitive Capsules） |

数据迁移属于语义分解，而非简单的字段重命名：一条陈旧的事实 Proposition 会拆分为一个 Proposition **外加**一条迁移生成的正面 Assertion；而存在歧义的历史值（如已衰减的 `confidence`、未结构化的 `author` 字符串）必须被显式标记为历史遗留数据保存，严禁擅自重新解释。规范的迁移指南请参阅 `SPECIFICATION.md` §103 以及 KIP 仓库的 `v2/migration/KIP-2.0-Migration-from-1.x.md`。
