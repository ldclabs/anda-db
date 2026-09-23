# `anda_db_schema` — 技术参考

[English](anda_db_schema.md)

> [Anda DB](https://github.com/ldclabs/anda-db) 所有子 crate 共用的类型系统、Schema 定义与文档模型。

|                 |                                                                                          |
| :-------------- | :--------------------------------------------------------------------------------------- |
| Crate           | [`anda_db_schema`](../rs/anda_db_schema/)                                                |
| 版本            | `0.13.x`                                                                                 |
| 配套 Crate      | [`anda_db_derive`](../rs/anda_db_derive/)（重新导出为 `AndaDBSchema` / `FieldTyped`）     |

---

## 目录

1. [概述](#1-概述)
2. [类型系统](#2-类型系统)
3. [字段值（Field Values）](#3-字段值field-values)
4. [字段条目（Field Entries）](#4-字段条目field-entries)
5. [Schema 与版本迁移](#5-schema-与版本迁移)
6. [文档模型（Documents）](#6-文档模型documents)
7. [Resource 预定义类型](#7-resource-预定义类型)
8. [Derive 宏](#8-derive-宏)
9. [序列化](#9-序列化)
10. [错误体系](#10-错误体系)
11. [API 参考](#11-api-参考)
12. [实战范例（Cookbook）](#12-实战范例cookbook)

---

## 1. 概述

### 1.1 核心职责

`anda_db_schema` 为 Anda DB 提供了最基础的类型词汇：

- 描述字段的结构形态（`FieldType`）；
- 承载运行时的实际数据值（`FieldValue`）；
- 封装字段元数据（`FieldEntry`）；
- 组合为带版本控制的 `Schema`；
- 将持久化存储记录表示为 `Document` / `DocumentOwned`。

这些原语围绕两个并行的工程目标进行设计：

- **紧凑、确定性的磁盘存储格式** —— 字段值归一化为 CBOR 格式（基于 [`cbor2`](https://docs.rs/cbor2)），且 `FieldEntry`/`Schema` 将其元数据键序列化为单个英文字母以最大化压缩存储体积。
- **自描述的动态类型系统** —— 闭合的 `FieldType` 枚举允许数据库接受任意用户定义的结构体，同时在写入时对每个字段执行严格的类型校验。

### 1.2 概念层次

```text
Schema ─────────────────────────── 文档布局定义（带版本）
 ├── _id : FieldEntry（必填，U64，idx = 0，唯一）
 ├── …  : FieldEntry
 │        ├── name        — Schema 内唯一字段名
 │        ├── description — 面向人类 / LLM 的描述文本
 │        ├── type        — FieldType 字段类型
 │        ├── unique      — 集合级唯一性约束标记
 │        └── idx         — 稳定的磁盘存储数字键
 │
 ├── FieldType  (闭合枚举)
 │   ├── 基本标量类型      Bool I64 U64 F64 F32 Bytes Text Json Vector
 │   └── 复合类型          Array(Vec<Ft>)  Map(BTreeMap<FieldKey, Ft>)  Option(Box<Ft>)
 │
 └── FieldValue (闭合枚举)
     ├── 对应各基本类型的变体
     ├── Vector(Vec<bf16>)
     ├── Array(Vec<FieldValue>)
     ├── Map(BTreeMap<FieldKey, FieldValue>)
     └── Null   ← Option(_) 字段的缺失值表示
```

### 1.3 源码布局

```text
rs/anda_db_schema/src/
├── lib.rs          # crate 级文档、重新导出、validate_field_name
├── error.rs        # SchemaError, BoxError
├── field.rs        # 稳定的公共门面与类型别名
├── field/
│   ├── field_type.rs # 类型声明、类型化解析准备与兼容性检查
│   ├── key.rs        # FieldKey 与通配符映射
│   ├── value.rs      # FieldValue 与 CBOR/JSON 转换
│   ├── entry.rs      # FieldEntry
│   ├── budget.rs     # 结构复杂度预算校验
│   └── tests.rs      # 字段级回归测试
├── schema/history.rs # 嵌套键墓碑（tombstones）与遗留历史恢复
├── type_construction.rs # 可失败的 derive 构造守卫
├── schema.rs       # Schema, SchemaBuilder
├── document.rs     # Document, DocumentOwned
├── resource.rs     # Resource（预定义 Schema）
└── value_serde.rs  # FieldKey/FieldValue 的 Serialize / Deserialize 实现
```

---

## 2. 类型系统

### 2.1 `FieldType`

```rust
pub enum FieldType {
    // 基本标量类型
    Bool, I64, U64, F64, F32, Bytes, Text, Json, Vector,
    // 复合类型
    Array(Vec<FieldType>),
    Map(BTreeMap<FieldKey, FieldType>),
    Option(Box<FieldType>),
}
```

常用类型别名：

| 别名     | 具体类型      |
| :------- | :------------ |
| `Ft`     | `FieldType`   |
| `Vector` | `Vec<bf16>`   |

### 2.2 基本类型及其 Rust 对应类型

| `FieldType` | `AndaDBSchema` 支持的 Rust 源类型                                                     |
| :---------- | :------------------------------------------------------------------------------------- |
| `Bool`      | `bool`                                                                                 |
| `I64`       | `i8`, `i16`, `i32`, `i64`, `isize`                                                     |
| `U64`       | `u8`, `u16`, `u32`, `u64`, `usize`                                                     |
| `F32`       | `f32`                                                                                  |
| `F64`       | `f64`                                                                                  |
| `Bytes`     | `Vec<u8>`, `[u8; N]`, `serde_bytes::*`, `ic_auth_types::ByteBufB64`, `ByteArrayB64<N>` |
| `Text`      | `String`, `&str`                                                                       |
| `Json`      | `serde_json::Value`                                                                    |
| `Vector`    | `Vec<bf16>`, `[bf16; N]`                                                               |

### 2.3 复合类型

#### `Array`

`FieldType::Array` 内部包含一个 `Vec<FieldType>`，其长度决定了数组的校验形态：

| `types.len()` | 语义说明                                                                   |
| :------------ | :------------------------------------------------------------------------- |
| `0`           | 异构数组 —— 元素按原样放行接收（主要用于数据回填或临时动态数据）。         |
| `1`           | 同构数组。每个元素必须严格满足单一内部类型。                               |
| `N > 1`       | 元组（Tuple）形态 —— `values.len()` 必须严格等于 `N`，元素按位置逐一匹配。 |

#### `Map`

`FieldType::Map` 以 `FieldKey`（字符串、有符号 `i64` 或字节数组）作为键，支持三种形态：

- **开放映射（Open Map）** —— 空声明接收任意 Map 项。无法仅通过修改元数据的方式在开放映射与固定键映射之间直接升级。空的 `FieldTyped` 结构体同样采用此开放形态。
- **通配符映射（Wildcard Map）** —— 仅包含单个键为通配符的条目（字符串使用 `"*"`，整数键使用 `i64::MIN`，字节数组使用 `b"*"`；参见 `FieldKey::is_wildcard`）。运行时允许该变体下的任意键，且所有值必须匹配通配符声明的值类型。通配符键与其他普通键混用属于非法声明（`FieldType::validate_declaration`）。
- **Schema 绑定映射（Schema-bound Map）** —— 类型声明中存在的键是值中唯一允许出现的键。必填键是指值类型*不为* `Option` 的键。

```rust
// 通配符字符串映射（≅ HashMap<String, U64>）
Ft::Map([(TEXT_WILDCARD_KEY.clone(), Ft::U64)].into_iter().collect());

// 通配符整数映射（≅ BTreeMap<i64, Text>）
Ft::Map([(I64_WILDCARD_KEY.clone(), Ft::Text)].into_iter().collect());

// Schema 绑定映射（仅允许 "title" 及可选的 "subtitle"）
Ft::Map([
    ("title".into(),    Ft::Text),
    ("subtitle".into(), Ft::Option(Box::new(Ft::Text))),
].into_iter().collect());
```

#### `Option`

`FieldType::Option(Box<Ft>)` 是声明可空（nullable）字段的唯一方式。类型*不为* `Option` 的字段在 `Schema::validate` 与 `FieldEntry::validate` 中均被视为必填字段。必填的 `Json` 字段允许承载 JSON null 载荷，但其键本身必须存在。

对此类字段，`set_field(name, Fv::Null)` 会归一化为 `Fv::Json(Json::Null)`，与 `try_from`、`set_field_as` 和存储读回一致。`Option<Json>` 则保留可选值的 `Fv::Null` 表示。

### 2.4 `FieldKey`

```rust
pub enum FieldKey {
    Text(String),
    I64(i64),
    Bytes(Vec<u8>),
}
```

公开了三个针对通配符约定的预构建常量：

```rust
pub static TEXT_WILDCARD_KEY:  LazyLock<FieldKey>; // "*"
pub static I64_WILDCARD_KEY:   LazyLock<FieldKey>; // i64::MIN
pub static BYTES_WILDCARD_KEY: LazyLock<FieldKey>; // b"*"
```

`FieldKey::is_wildcard()` 用于识别上述哨兵值；`as_wildcard_map(&BTreeMap<FieldKey, FieldType>)` 在 Map 类型为同构通配符映射时返回其唯一的条目。

支持从 `String`、`&str`、不超过 `i64` 的有符号整数类型、`Vec<u8>`、`[u8; N]`、`&[u8]` 以及 `cbor2::Value`（文本、整数、字节串或元素在 `0..=255` 范围内的整数数组 —— 即 serde 赋予 `Vec<u8>` / `[u8; N]` Map 键的形态，自动强转为 `Bytes` 键）进行转换。

### 2.5 字段命名规则

`validate_field_name` 强制实施严格的 ASCII 字符限制，确保字段名在所有存储后端间保持稳定：

- 非空，最大长度为 **64 字节**；
- 仅允许包含小写字母 `a`–`z`、数字 `0`–`9` 以及下划线 `_`。

`_id` 是合法的字段名；它是本 crate 中**唯一**的保留字段名（自动分配 `idx = 0` 且具备 `unique` 属性）。

### 2.6 类型级方法

| 方法 | 用途说明 |
| :--- | :------- |
| `FieldType::allows_null` | 仅当类型为 `Option(_)` 时返回 `true`。 |
| `FieldType::validate_declaration` | 检查 `self` 是否为合法的类型声明：杜绝 `Option<Option<T>>`，杜绝通配符键与其他普通键混用，校验嵌套深度预算。由 `FieldEntry::new`、`SchemaBuilder::add_field` 与 `Schema` 反序列化自动调用。 |
| `FieldType::extract` | CBOR → `FieldValue`，要求传入的 CBOR 数据严格匹配 `self`。 |
| `FieldType::validate` | 检查现有的 `FieldValue` 是否符合 `self`，放行第 3.3 节列出的兼容读回形态。 |
| `FieldType::is_compatible_upgrade_of` | 判定已存储字段是否能合法重新声明为 `self`（第 5.4 节）。 |

`extract` 是由类型驱动的（用于解析结构化输入），而 `FieldValue::try_from` 是由数据形状驱动的（用于读取无类型 CBOR）。

---

## 3. 字段值（Field Values）

### 3.1 `FieldValue`

```rust
pub enum FieldValue {
    Bool(bool),  I64(i64),  U64(u64),  F64(f64),  F32(f32),
    Bytes(Vec<u8>),  Text(String),  Json(serde_json::Value),
    Vector(Vec<bf16>),
    Array(Vec<FieldValue>),
    Map(BTreeMap<FieldKey, FieldValue>),
    Null,
}
```

常用类型别名：`Fv = FieldValue`。

`FieldValue: PartialEq` 具备明确的确定性语义，因为 `FieldValue::f64_from` 与 `f32_from` 在从 CBOR 解析提取时会显式拒绝 `NaN`。

### 3.2 构建字段值

#### 从拥有的 Rust 原生值构建

为所有基本类型以及常用集合类型实现了 `From`：

| `From<T>`                                            | 结果变体                  |
| :--------------------------------------------------- | :------------------------ |
| `bool` / `i64` / `u64` / `f64` / `f32`               | 一一对应                  |
| `Vec<u8>`                                            | `Bytes`                   |
| `String`                                             | `Text`                    |
| `serde_json::Value`                                  | `Json`                    |
| `Vec<bf16>`                                          | `Vector`                  |
| `Vec<T>`（其中 `T: Into<FieldValue>`）               | `Array`                   |
| `BTreeSet<T>`, `HashSet<T>`                          | `Array`                   |
| `BTreeMap<K, V>`, `HashMap<K, V>`, `serde_json::Map` | `Map`                     |
| `FieldKey`                                           | `Text`, `I64` 或 `Bytes`  |

#### 从任意 `Serialize` 值构建

```rust
let fv = Fv::serialized(&my_struct, Some(&Ft::Array(vec![Ft::Vector])))?;
```

`serialized` 先将其编码为 CBOR，随后在提供类型提示时调用 `FieldType::extract`，无提示时回退至 `FieldValue::try_from`。当无法仅从 CBOR 推断子值类型时必须提供类型提示 —— 最典型的为 `Vector`（其 CBOR 底层形态与 `Array<U64>` 无法区分）。

### 3.3 读取字段值

为所有基本类型（按值及按引用）以及多种集合形式实现了 `TryFrom`：

| 目标类型                               | 源变体                                  |
| :------------------------------------- | :-------------------------------------- |
| `bool` / `i64` / `u64` / `f64` / `f32` | 匹配的基本标量变体                      |
| `Vec<u8>` / `[u8; N]`                  | `Bytes`                                 |
| `String` / `&str`                      | `Text`                                  |
| `serde_json::Value`                    | `Json`                                  |
| `Vec<bf16>` / `[bf16; N]`              | `Vector`                                |
| `Vec<T>`                               | `Array`（当 `T: TryFrom<FieldValue>`）  |
| `BTreeMap<FieldKey, T>`                | `Map`                                   |

在通用反序列化无法精确还原声明变体的场景下，系统接受兼容的读回形态：`i64` 接受非负 `U64`；`f32` 接受已存 `f32` 读回时表现出的 `F64`；`Vec<bf16>` 接受 bf16 位模式构成的整数数组；`f64` / `f32` 接受 `I64` / `U64` —— 由于 JSON 只有单一数字类型，`1.0` 在传输中常以 `1` 呈现。`f64` 接受任意整数（通过 `as f64` 转换：在 $2^{53}$ 内精确，超出则舍入）；`f32` 仅接受能够被 `f32` 精确表示的整数，因此诸如 `16777217` 的值无论以整数还是浮点形式传入都会被直接拒绝，防止发生单方隐式舍入。`FieldType::validate` 执行相同的规则，读取文档（`Document::try_from_doc`）时则将上述形态归一化为规范变体。

对于任意 `DeserializeOwned` 类型，使用：

```rust
let user: MyUser = fv.deserialized()?;
```

`deserialized` 通过 CBOR 进行往返转换，能够处理 serde 支持反序列化的所有类型。

### 3.4 便捷访问器

`FieldValue::get_field_as<'a, T>(&'a self, key: &FieldKey) -> Option<&'a T>`
在读取嵌套 Map 时简化了 `Fv::Map(_) → BTreeMap::get → TryFrom` 这一长调用链。

### 3.5 向量辅助函数

```rust
pub fn vector_from_f32(v: Vec<f32>) -> Vector;
pub fn vector_from_f64(v: Vec<f64>) -> Vector;
pub fn vector_to_f32(v: &[bf16]) -> Vec<f32>;
```

逐元素执行有损转换（`bf16::from_f32` / `bf16::from_f64`）。

---

## 4. 字段条目（Field Entries）

### 4.1 定义

```rust
pub struct FieldEntry {
    name: String,        // 序列化键为 "n"
    description: String, // 序列化键为 "d"
    r#type: FieldType,   // 序列化键为 "t"
    unique: bool,        // 序列化键为 "u"
    idx: usize,          // 序列化键为 "i"
}
```

长字段名键（`name`、`description`、`type`、`unique`、`index`）作为 `serde(alias = …)` 提供向前兼容。

### 4.2 构建器

```rust
let entry = FieldEntry::new("title".into(), Ft::Text)?
    .with_description("文章标题".into())
    .with_unique();          // 可选
// .with_idx(N)              ← 极少手动调用；SchemaBuilder 会自动分配索引。
```

`new` 会立即执行 `validate_field_name` 校验。

### 4.3 访问器

| 方法 | 返回值 |
| :--- | :----- |
| `name()` | `&str` |
| `r#type()` | `&FieldType` |
| `required()` | 当且仅当类型不为 `Option(_)` 时返回 `true` |
| `unique()` | `bool` |
| `idx()` | `usize` |

### 4.4 修改器

| 方法 | 用途说明 |
| :--- | :------- |
| `with_idx(idx)` | Builder 链式调用风格；消耗 self。 |
| `set_idx(idx)` | 就地原地修改；由 `Schema::upgrade_with` 使用以避免内存克隆。 |

### 4.5 校验与强制转换

`FieldEntry::extract(cbor, validate)` 串联了 `FieldType::extract` 与可选的 `validate` 步骤，`FieldEntry::validate` 强制检查：

1. `Null` 仅对 `Option(_)` 类型合法；
2. 数据值必须严格满足 `FieldType::validate`。

`FieldEntry::coerce(value)` 是非 CBOR 来源 `FieldValue`（例如 JSON API 载荷）的入口方法：它对值执行与 `Document::try_from` 相同的强制转换规则 —— `Bytes` 字段接受 `0..=255` 数组，浮点字段接受整数，`I64` 字段接受非负 `U64`，`Vector` 字段接受 bf16 位模式数组 —— 随后强制执行结构复杂度预算校验。`Document::set_field` 同样经过此链路，保证创建文档与更新文档接受完全一致的输入形态。规范值保留其底层缓冲区；具型容器直接遍历，读取物化操作在单字段单次嵌套深度检查前整合了修剪、归一化与类型检查。已存储的数据值可能早于节点数和容器大小的准入限制，因此读取、恢复与索引重建会保留这些历史宽泛值。新插入与替换字段仍强制执行完整的默认预算；部分更新保持未变更的历史字段完好无损。

---

## 5. Schema 与版本迁移

### 5.1 定义

```rust
pub struct Schema {
    idx:     BTreeSet<usize>,
    fields:  BTreeMap<String, FieldEntry>,
    version: u64,
    // 私有字段：分配水位、历史遗留标记与嵌套键历史记录。
}
```

由 `SchemaBuilder` 与反序列化共同维护的不变式：

- `_id` 必须存在，类型为 `U64`，具备 `unique` 属性，且 `idx == 0`；
- 所有字段名必须通过 `validate_field_name`；
- 所有 `idx` 值必须唯一且 `≤ u16::MAX`（单个 Schema 包含 `_id` 在内最多可容纳 `u16::MAX + 1 = 65 536` 个字段）。

### 5.2 `SchemaBuilder`

```rust
let mut builder = Schema::builder();
builder.with_version(1);
builder.add_field(FieldEntry::new("title".into(), Ft::Text)?)?;
builder.add_field(FieldEntry::new("views".into(), Ft::U64)?)?;
builder.with_resource("thumbnail", false)?;
let schema = builder.build()?;
```

`add_field` 按插入顺序自动分配 `idx`（`1`, `2`, …）。`_id` 在 `SchemaBuilder::new` 时自动以 `idx = 0` 注入。

### 5.3 检查 API

```rust
schema.version()                  // u64
schema.len() / is_empty()
schema.get_field(name)            // Option<&FieldEntry>
schema.get_field_or_err(name)?    // Result<&FieldEntry, SchemaError>
schema.iter()                     // impl Iterator<Item = &FieldEntry>
schema.validate(&values)?
```

`validate` 既检查 `values` 中的每个键是否有匹配的字段定义，又检查所有必填字段是否均已提供。

### 5.4 版本控制与渐进式迁移

Schema 包含版本号以支持**渐进式（gradual）**在线迁移。新 Schema 通常在代码中通过 `#[derive(AndaDBSchema)]` 伴随连续递增索引生成；旧 Schema 则从存储中加载，保留此前已分配的索引。

```rust
new_schema.upgrade_with(&old_schema)?;
```

`upgrade_with` 遵循以下规则：

1. **必须满足 `new.version > old.version`**。
2. **存量已有字段**保留旧的 `idx` 与 `unique` 标记。其 `FieldType` 仅允许在能够保证已有持久化数据依然完全可读的方向上演进（`FieldType::is_compatible_upgrade_of`）：类型可变更为可选（顶层或复合类型内部的 `T` → `Option<T>`），嵌套结构体（具显式键的 `Map`）可新增可选键或删除现有键。其余一切变更均被拒绝。
3. **新增字段**必须声明为可选（Optional），并从旧 Schema 的**分配水位线（allocation watermark）**（`Schema::allocated_idx_end`）处获取全新索引，确保已删除字段的历史索引**绝不重复使用**。

这保证了在旧 Schema 下持久化的任何记录在升级后均能正常读取。读取时，已删除字段索引下的历史数据会被自动丢弃；等于或大于分配水位线的索引被判定为异常或损坏数据并报错拒绝。

嵌套字段的删除在 Schema 元数据中记录为墓碑（tombstones），包括数组、可选值及通配符 Map 值内部的嵌套删除。后续新版本若未进行全量数据迁移，严禁复用被删除的嵌套路径。`is_compatible_upgrade_of` 负责结构层面的检查；`Schema::upgrade_with` 进一步校验该历史墓碑记录。

在缺少 `next_idx` 或嵌套历史记录的情况下持久化的旧 Schema 依然可读。然而，未知的分配水位无法安全授权新的顶层索引，残缺的嵌套历史也无法授权新的嵌套键。核心集合在升级此类 Schema 之前，会预先扫描**所有底层原始存储文档**（包括未注册对象及有效、可重放的变更意图中的新旧镜像）。扫描过程应用与变更重放相同的解码、保留 ID 及路径/序号校验，防止损坏的恢复记录阻塞或扭曲 Schema 分配。缺失的分配水位由此扫描推导得出；已有的水位线保持权威，独立缺失的嵌套键历史亦由此补齐。扫描失败时元数据保持原状不变。

升级后的 Schema 在打开回调函数允许写入新文档之前持久化落盘。这确保了若回调函数执行失败或被取消，新分配的字段索引依然具备可恢复性。恢复后的水位线与历史记录在发生文档写入前即已持久化，后续的再次升级无需重复执行扫描。

自定义存储集成在排除并发写入者的前提下使用恢复累加器：

```rust
let mut recovery = old_schema.history_recovery();
for raw_document in all_raw_documents_and_recovery_images {
    recovery.observe(&raw_document)?;
}
let recovered = recovery.finish(); // 确认全量扫描完成
new_schema.upgrade_with(&recovered)?;
```

严禁将已经过修剪的值或仅由索引过滤出的部分子集传入恢复流程。`has_upgrade_history()` 用于汇报分配历史与嵌套键历史是否均已完备；`has_allocation_watermark()` 则单独描述索引历史状态。

### 5.5 `IndexedFieldValues`

```rust
pub type IndexedFieldValues = BTreeMap<usize, FieldValue>;
```

文档数据载荷的规范存储容器 —— 以字段索引 `idx` 为 Key，而非字段名称。

---

## 6. 文档模型（Documents）

### 6.1 两种形态

```rust
pub struct Document      { fields: IndexedFieldValues, schema: Arc<Schema> }
pub struct DocumentOwned { pub fields: IndexedFieldValues } // 可序列化
pub type   DocumentId = u64;
```

`Document` 是运行时操作接口（可逐字段对照 Schema 进行实时校验）。`DocumentOwned` 是磁盘存储与网络传输形态；其序列化结构为 `{ "f": IndexedFieldValues }` —— 采用单一短键以保持记录极度紧凑。

### 6.2 构造

```rust
// 创建空文档：
let mut doc = Document::new(schema.clone());

// 从现有数据载荷构造（按 Schema 执行校验）：
let doc = Document::try_from_doc(schema.clone(), owned_doc)?;

// 从任意 Serialize 结构体构造（执行校验）：
let doc = Document::try_from(schema.clone(), &my_struct)?;
```

### 6.3 读取

```rust
doc.id();                                     // DocumentId
doc.get_field("title");                       // Option<&Fv>
doc.get_field_or_err("title")?;               // Result<&Fv, SchemaError>
let title: String = doc.get_field_as("title")?;
let user:  TestUser = doc.try_into()?;        // 消耗 Document 并反序列化
```

`try_into` 会从文档重新构建以字段名为 Key 的 CBOR Map —— 自动省略缺失字段以触发 `#[serde(default)]` —— 随后委托 serde 完成剩余反序列化。

### 6.4 修改

```rust
doc.set_id(42);
doc.set_field("title", Fv::Text("Hi".into()))?;       // 如 try_from 执行强转，随后存储
doc.set_field_as("views", &123u64)?;                  // 先序列化再存储
doc.remove_field("title");                            // Option<Fv>
doc.set_doc(owned_doc)?;                              // 批量整体替换
```

### 6.5 转换

```rust
let owned: DocumentOwned = doc.into(); // 丢弃 Schema 引用
```

### 6.6 序列化形态

```json
{ "f": { "0": 42, "1": "Hi", "2": 123 } }
```

顶层键为字段 `idx` 的十进制字符串表示（这是 JSON 规范唯一允许的键格式；CBOR 则直接使用原生原生整数键）。

---

## 7. Resource 预定义类型

`Resource` 是用于描述外部资产的预定义结构体 —— 既可作为独立集合使用，也可作为嵌入式子文档使用。

```rust
#[derive(AndaDBSchema, FieldTyped, Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct Resource {
    pub _id:         u64,                          // 主键
    pub tags:        Vec<String>,                  // 类型标签，例如 ["text", "md"]
    pub name:        String,                       // 人类可读名称
    pub description: Option<String>,
    pub uri:         Option<String>,
    pub mime_type:   Option<String>,
    pub blob:        Option<ByteBufB64>,           // 内联二进制载荷
    pub size:        Option<u64>,
    #[unique] pub hash: Option<ByteArrayB64<32>>,  // SHA3-256 摘要
    pub metadata:    Option<Map<String, Json>>,
}
```

在其他任意自定义 Schema 中嵌入：

```rust
#[derive(AndaDBSchema)]
struct Article {
    _id: u64,
    title: String,
    thumbnail: Option<Resource>, // 展开为 FieldType::Option(Resource::field_type())
}
```

使用 `Schema::with_resource(name, required)` 构建器辅助方法可以在无需 derive 宏的情况下实现相同的 Schema 声明。

---

## 8. Derive 宏

两个宏均直接从 `anda_db_schema` 重新导出：

```rust
use anda_db_schema::{AndaDBSchema, FieldTyped};
```

### 8.1 `AndaDBSchema`

生成 `MyStruct::schema() -> Result<Schema, SchemaError>`。结构体上必须显式声明 `_id: u64`。构建器会自动注入其元数据，而结构体本身在序列化时必须输出 `"_id"` 字段。不允许跳过该字段、不允许为其分配整数 CBOR 键，也不允许覆盖其 `field_type`。

### 8.2 `FieldTyped`

生成 `MyStruct::try_field_type() -> Result<FieldType, SchemaError>` 以及现有的 `field_type() -> FieldType` 便捷包装器。返回结果是一个 `FieldType::Map`，其内部映射为 `field_name` → `FieldType`。这是嵌套用户结构体参与 Schema 构建的方式：`AndaDBSchema` 会调用派生嵌套类型的可失败构造器，从而向上冒泡递归类型错误。不可失败的包装器在遇到非法声明时触发 panic；自定义的历史 `field_type()` 方法保持兼容。

### 8.3 属性修饰符

| 属性 | 效果说明 |
| :--- | :------- |
| `#[field_type = "TypeDSL"]` | 显式覆盖推断出的类型（见下文）。 |
| `#[unique]` | 将字段标记为具备唯一性（需要配合 `AndaDBSchema`）。 |
| `#[serde(rename = "new_name")]` | 使用重命名后的序列化名称作为 Schema 字段名。 |
| `#[serde(rename_all = "...")]` | 容器级大小写转换规则，与 serde 语义完全一致。 |
| `#[serde(skip)]` / `skip_serializing` | 从 Schema 中彻底排除该字段。 |
| `///` 文档注释 | 提取作为字段的 `description` 描述文本。 |

`#[field_type = "..."]` 支持小型的类型 DSL（对空白字符不敏感）：基本类型（`Bytes`、`Text`、`U64`、`I64`、`F64`、`F32`、`Bool`、`Json`、`Vector`），以及 `Array<T>`、`Option<T>` 与 `Map<String|Text|Bytes, T>`：

```rust
#[field_type = "Bytes"]
some_id: [u8; 16],

#[field_type = "Array<F32>"]
samples: Vec<f32>,

#[field_type = "Option<Map<Text, Json>>"]
extra: Option<HashMap<String, Value>>,
```

完整语法规则与诊断请参阅 [anda_db_derive.zh.md](./anda_db_derive.zh.md)。

### 8.4 类型推断映射表

| Rust 源码类型                                                 | 推断出的 `FieldType`                   |
| :------------------------------------------------------------ | :------------------------------------- |
| `bool`                                                        | `Bool`                                 |
| `i8` … `i64`, `isize`                                         | `I64`                                  |
| `u8` … `u64`, `usize`                                         | `U64`                                  |
| `f32` / `f64`                                                 | `F32` / `F64`                          |
| `String`, `&str`                                              | `Text`                                 |
| `Vec<u8>`, `[u8; N]`, `Bytes`, `ByteArrayB64`, `ByteBufB64`   | `Bytes`                                |
| `Vec<bf16>`, `[bf16; N]`                                      | `Vector`                               |
| `serde_json::Value`                                           | `Json`                                 |
| `Vec<T>`, `HashSet<T>`, `BTreeSet<T>`                         | `Array(vec![T])`                       |
| `HashMap<K, V>`, `BTreeMap<K, V>`, `Map<K, V>` (`K = String`) | `Map({"*": V})`                        |
| 具有字节串键的 `HashMap<K, V>` 等                             | `Map({b"*": V})`                       |
| `Option<T>`                                                   | `Option(T)`                            |
| `Box<T>`, `Arc<T>`, `Rc<T>`, `Cow<'_, T>`                     | 从 `T` 推断（serde 透明展开）          |
| 其他任意路径 `Foo`                                            | `<Foo>::field_type()`（必须实现派生）   |

---

## 9. 序列化

### 9.1 两种格式，统一模型

`FieldValue` 与 `FieldKey` 拥有手工编写的 `Serialize` / `Deserialize` 实现，在执行时根据 `is_human_readable()` 自动分支：

|                             | 人类可读格式 (JSON 等)                 | 二进制格式 (CBOR, MessagePack 等) |
| :-------------------------- | :------------------------------------- | :-------------------------------- |
| `FieldKey::I64`             | `i64:<十进制数值>` 字符串              | 原生整数                          |
| `Bytes` / `FieldKey::Bytes` | `b64:<URL 安全 Base64>` 字符串         | 原生二进制字节串                  |
| `Vector`                    | `u16`（bf16 位表示）组成的数组         | 同左                              |
| `Json`                      | 保留文本/键前缀转义后的 JSON 文本      | 原始 JSON 数据结构                |
| `Null`                      | `null` / 单元结构                      | `null`                            |

仅显式前缀会被特殊解码：`b64:` 表示字节串，`i64:` 表示整数 Map 键，`txt:` 用于对普通文本中碰巧出现的保留前缀进行转义。普通字符串（如 `"test"`）保持为常规文本。嵌入的 JSON 字符串与对象键遵循完全相同的前缀转义规则。格式错误的前缀值将直接报错。

重复的文档索引、字段值 Map 键与类型声明 Map 键在覆盖早期条目之前会被直接拒绝。针对非有限浮点标量（如 NaN、无穷大）的 JSON 序列化会返回错误；CBOR 则保留无穷大。F32 读回检查使用真实的 JSON 格式化器/解析器，而非 Rust 的 Display。

### 9.2 CBOR 编码示例

```text
Fv::Null                  → f6
Fv::Bool(true)            → f5
Fv::U64(42)               → 18 2a
Fv::I64(-42)              → 38 29
Fv::Text("hello")         → 65 68 65 6c 6c 6f
Fv::Bytes([1,2,3,4])      → 44 01 02 03 04
Fv::Array([U64(1), Text("hello")])
                          → 82 01 65 68 65 6c 6c 6f
```

### 9.3 结合类型提示的完整往返

仅凭 CBOR 无法区分 `Vector` 与 `Array<U64>`（两者在底层均为短整数序列），因此在将任意用户数据序列化为 `FieldValue` 时，可传入 `FieldType` 提示：

```rust
let vv = vec![[bf16::from_f32(1.0), bf16::from_f32(1.1)]];

let fv = Fv::serialized(&vv, None)?;
// → Array([Array([U64(16256), U64(16269)])])

let fv = Fv::serialized(&vv, Some(&Ft::Array(vec![Ft::Vector])))?;
// → Array([Vector([1.0, 1.1])])
```

得益于 `half` crate 的 serde 实现，两种形态均可正常反序列化回 `Vec<[bf16; 2]>`，但在无类型的存储往返后，需要借助声明的 Schema 才能准确恢复规范的 `Vector` 变体。

---

## 10. 错误体系

```rust
pub enum SchemaError {
    Schema(String),       // 违反 Schema 级不变式
    FieldType(String),    // 非法的 FieldType 声明（validate_declaration 失败）
    FieldValue(String),   // 数据值不满足其声明的 FieldType
    FieldName(String),    // 非法的字段名
    Validation(String),   // 文档未通过 Schema::validate 校验
    Serialization(String) // CBOR / serde 序列化或反序列化错误
}

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
```

`BoxError` 是所有 `TryFrom<FieldValue>` 实现统一返回的错误包装类型。

---

## 11. API 参考

### 11.1 类型别名（从 crate 根重新导出）

| 别名                 | 具体类型                                   |
| :------------------- | :----------------------------------------- |
| `Ft`                 | `FieldType`                                |
| `Fv`                 | `FieldValue`                               |
| `Fe`                 | `FieldEntry`                               |
| `Cbor`               | `cbor2::Value`                             |
| `Json`               | `serde_json::Value`                        |
| `Map<K, V>`          | `serde_json::Map<K, V>`                    |
| `Vector`             | `Vec<bf16>`                                |
| `DocumentId`         | `u64`                                      |
| `IndexedFieldValues` | `BTreeMap<usize, FieldValue>`              |
| `BoxError`           | `Box<dyn std::error::Error + Send + Sync>` |

### 11.2 公共类型

| 类型 | 说明 |
| :--- | :--- |
| `FieldType` | 闭合的类型枚举。 |
| `FieldKey` | Map 键（`Text` / `I64` / `Bytes`）。 |
| `FieldValue` | 运行时数据值枚举。 |
| `FieldEntry` | 字段元数据，随各 Schema 持久化。 |
| `Schema` | 带有版本控制的 `FieldEntry` 集合。 |
| `SchemaBuilder` | `Schema` 的构造辅助工具。 |
| `Document` | 绑定 Schema 的文档。 |
| `DocumentOwned` | 独立的可序列化文档。 |
| `Resource` | 面向外部资产的预定义 Schema。 |
| `SchemaError` | 本 crate 的统一错误枚举。 |

### 11.3 独立函数

```rust
pub fn validate_field_name(s: &str) -> Result<(), SchemaError>;
pub fn as_wildcard_map(m: &BTreeMap<FieldKey, FieldType>) -> Option<(&FieldKey, &FieldType)>;
pub fn vector_from_f32(v: Vec<f32>) -> Vector;
pub fn vector_from_f64(v: Vec<f64>) -> Vector;
pub fn vector_to_f32(v: &[bf16]) -> Vec<f32>;
```

### 11.4 常量与静态变量

| 条目 | 取值说明 |
| :--- | :------- |
| `Schema::ID_KEY` | `"_id"` |
| `MAX_CONVERSION_DEPTH` | `128` —— CBOR ⇄ `FieldValue` 转换的最大嵌套深度限制 |
| `TEXT_WILDCARD_KEY` | `FieldKey::Text("*")` |
| `I64_WILDCARD_KEY` | `FieldKey::I64(i64::MIN)` |
| `BYTES_WILDCARD_KEY` | `FieldKey::Bytes(b"*")` |

---

## 12. 实战范例（Cookbook）

### 12.1 手工构建最小 Schema

```rust
use anda_db_schema::{Fe, Ft, Schema};
use std::sync::Arc;

let mut builder = Schema::builder();
builder.add_field(Fe::new("title".into(), Ft::Text)?
    .with_description("文档标题".into()))?;
builder.add_field(Fe::new("content".into(), Ft::Text)?)?;
builder.add_field(Fe::new("views".into(), Ft::U64)?)?;
let schema = builder.build()?;
let schema = Arc::new(schema);
```

### 12.2 通过 Derive 宏生成相同 Schema

```rust
use anda_db_schema::{AndaDBSchema, Schema};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize, AndaDBSchema)]
struct Article {
    /// 文档主键
    _id: u64,
    /// 文档标题
    title: String,
    /// 文档正文
    content: String,
    /// 浏览量
    views: u64,
}

let schema = Arc::new(Article::schema()?);
```

### 12.3 构建与读取文档

```rust
use anda_db_schema::{Document, DocumentOwned, Fv};

let mut doc = Document::new(schema.clone());
doc.set_id(1);
doc.set_field("title",   Fv::Text("Hello".into()))?;
doc.set_field("content", Fv::Text("World".into()))?;
doc.set_field("views",   Fv::U64(42))?;

let title = doc.get_field_as::<String>("title")?;
let owned: DocumentOwned = doc.into();
```

### 12.4 从结构体构建并执行全量校验

```rust
let article = Article {
    _id: 1,
    title: "Hello".into(),
    content: "World".into(),
    views: 42,
};
let doc = Document::try_from(schema.clone(), &article)?;
let back: Article = doc.try_into()?;
```

### 12.5 Schema 版本升级迁移

```rust
let mut builder = Schema::builder();
builder.with_version(1);
builder.add_field(Fe::new("name".into(), Ft::Text)?)?;
let old = builder.build()?;

let mut builder = Schema::builder();
builder.with_version(2);
builder.add_field(Fe::new("name".into(), Ft::Text)?)?;
builder.add_field(Fe::new("email".into(), Ft::Option(Box::new(Ft::Text)))?)?;
let mut new = builder.build()?;
new.upgrade_with(&old)?;
// name 维持 idx=1；email 分配得到 idx=2。
```

### 12.6 嵌入 `Resource` 类型

```rust
use anda_db_schema::{AndaDBSchema, Resource};

#[derive(AndaDBSchema)]
struct Article {
    _id: u64,
    title: String,
    thumbnail: Option<Resource>, // 递归调用 Resource::field_type()
}
```

---

*本文档与 `rs/anda_db_schema/` 共同维护。公共 API 发生变更时请同步更新。*
