# `anda_db_derive` 技术文档

[English](anda_db_derive.md)

**Crate 版本**：0.13  
**更新日期**：2026-09-06  

---

## 目录

1. [概述](#1-概述)
2. [Derive 宏](#2-derive-宏)
3. [属性配置](#3-属性配置)
4. [类型推断规则](#4-类型推断规则)
5. [`field_type` DSL](#5-field_type-dsl)
6. [使用示例](#6-使用示例)
7. [内部实现](#7-内部实现)
8. [诊断与错误处理](#8-诊断与错误处理)

---

## 1. 概述

### 1.1 核心作用

`anda_db_derive` 是一个过程宏 crate，用于从常规 Rust 结构体自动生成 AndaDB Schema 粘合代码。它导出两个 derive 宏，并通过 `anda_db_schema` 重新导出：

| 宏名称 | 生成的方法 |
| --- | --- |
| `FieldTyped` | `field_type() -> FieldType` 与 `try_field_type() -> Result<FieldType, SchemaError>` |
| `AndaDBSchema` | `pub fn schema() -> Result<Schema, SchemaError>` |

这两个宏仅适用于具有具名字段的结构体 (structs with named fields)。元组结构体 (tuple structs)、单元结构体 (unit structs)、枚举 (enums) 与联合体 (unions) 会被直接拒绝并触发 `compile_error!`。

### 1.2 Crate 代码布局

```text
rs/anda_db_derive/
├── src/
│   ├── lib.rs           # 公共宏入口
│   ├── schema.rs        # AndaDBSchema 实现
│   ├── field_typed.rs   # FieldTyped 实现
│   └── common.rs        # 共享解析与类型推断辅助逻辑
├── Cargo.toml
└── README.md
```

### 1.3 作用域与依赖解析

生成的代码通过直接的 `anda_db_schema` 依赖（支持重命名依赖）解析 Schema 类型，或在使用总库时通过 `anda_db::schema` 解析。用户在使用派生宏时无需显式导入 `Schema`、`FieldType`、`FieldEntry` 或 `FieldKey`。

---

## 2. Derive 宏

### 2.1 `FieldTyped`

```rust
#[proc_macro_derive(FieldTyped, attributes(field_type, cbor, serde))]
```

针对结构体中的每个具名字段，该宏生成一个 `(key, FieldType)` 二元组并将它们打包进 `FieldType::Map`。生成的 `field_type()` 是 `AndaDBSchema`（以及 `determine_field_type`）在遇到嵌套自定义结构体时调用的核心接口。

```rust
use anda_db_schema::{FieldType, FieldTyped};

#[derive(FieldTyped)]
struct User {
    id: u64,
    name: String,
    age: u32,
}

assert!(User::try_field_type().is_ok());
```

### 2.2 `AndaDBSchema`

```rust
#[proc_macro_derive(AndaDBSchema, attributes(field_type, unique, cbor, serde))]
```

通过 `Schema::builder()` 构造完整的 `Schema`。显式声明 `_id: u64` 字段是**强制要求**——Builder 会注入其系统元数据，而结构体本身必须具备该序列化字段。该宏在编译期验证该字段必须为 `u64`，且 serde 必须将其序列化为 `"_id"`（注意避免受 `rename_all` 规则干扰），并在代码生成阶段跳过该系统主键字段。

```rust
use anda_db_derive::AndaDBSchema;
use anda_db_schema::{FieldEntry, FieldType, Schema, SchemaError};

#[derive(AndaDBSchema)]
struct Article {
    /// 文章唯一标识
    _id: u64,
    /// 文章标题
    title: String,
    /// 文章正文
    content: String,
    /// 阅读浏览量
    views: u64,
}

// 展开代码简写示意：
impl Article {
    pub fn schema() -> Result<Schema, SchemaError> {
        let mut builder = Schema::builder();
        builder.add_field(
            FieldEntry::new("title".to_string(), FieldType::Text)?
                .with_description("文章标题".to_string()),
        )?;
        builder.add_field(
            FieldEntry::new("content".to_string(), FieldType::Text)?
                .with_description("文章正文".to_string()),
        )?;
        builder.add_field(
            FieldEntry::new("views".to_string(), FieldType::U64)?
                .with_description("阅读浏览量".to_string()),
        )?;
        builder.build()
    }
}
```

---

## 3. 属性配置

| 属性宏 | 适用目标 | 效果说明 |
| --- | --- | --- |
| `#[field_type = "..."]` | `FieldTyped`, `AndaDBSchema` | 使用 [DSL](#5-field_type-dsl) 显式覆盖推断出的字段类型。 |
| `#[cbor(key = N)]` | 仅 `FieldTyped` | 为嵌套字段指定整数 CBOR map key，匹配 `cbor2::Cbor` 整数键结构体。 |
| `#[unique]` | 仅 `AndaDBSchema` | 为生成的条目追加 `FieldEntry::with_unique()`。 |
| `#[serde(rename = "name")]` | 两者皆可 | 使用指定的序列化名称作为 Schema 字段名（使用 serialize 方向的名称）。 |
| `#[serde(rename_all = "...")]` | 两者皆可（容器级） | 对所有未显式命名的字段应用 serde 大小写规则，镜像 serde 的优先级。 |
| `#[serde(skip)]` / `#[serde(skip_serializing)]` | 两者皆可 | 字段绝不出现在序列化输出中，并从 Schema / 类型映射中排除。 |
| `#[serde(flatten)]` | 两者皆可 | **编译报错**：打平的键无法通过逐字段 Schema 条目描述。 |
| `#[serde(transparent)]` | 两者皆可（容器级） | **编译报错**：透明结构体序列化为其内部字段，而非 map。 |
| `/// 文档注释` | 仅 `AndaDBSchema` | 字段上的所有 `///` 行以空格连接并作为 `FieldEntry::with_description()` 输出。 |

补充说明：

- `FieldTyped` 会静默忽略 `#[unique]`，因为该宏不生成 `Schema`。
- 多行 `///` 文档注释会被连接；空文档行在连接前被丢弃。
- 仅识别首个 `serde(rename = "...")`；其他无法解析的 serde 语法会被跳过而不报错。`#[serde(with = "...")]` / `serialize_with` 可能会改变实际序列化结构——此时应配合显式的 `#[field_type = "..."]` 覆盖。
- 嵌套结构体字段上，`FieldTyped` 优先使用 `#[cbor(key = N)]` 而非 serde 名称。这使得类似 CWT 声明的 CBOR 原生数值类型能够对整数标签建模，同时不丢失 Schema 校验。
- `AndaDBSchema` 在编译期根据 AndaDB 命名规范（`[a-z0-9_]`，最长 64 字节）校验生成的每个字段名，并拒绝重名及与保留字段 `_id` 的冲突。嵌套 `FieldTyped` map 的键格式不受此限制（如 `camelCase` 可正常使用）；而在 `AndaDBSchema` 上，仅当转换后的名称依然合法时（如 `snake_case` / `lowercase` 或显式重命名）才允许使用。

---

## 4. 类型推断规则

当**未指定** `#[field_type]` 时，宏会遍历字段的 Rust 类型并生成对应的 `FieldType` token 流。

### 4.1 基础类型

| Rust 类型 | `FieldType` |
| --- | --- |
| `bool` | `Bool` |
| `i8`, `i16`, `i32`, `i64`, `isize` | `I64` |
| `u8`, `u16`, `u32`, `u64`, `usize` | `U64` |
| `f32` | `F32` |
| `f64` | `F64` |

### 4.2 字符串与二进制

| Rust 类型 | `FieldType` |
| --- | --- |
| `String`, `&str` | `Text` |
| `Vec<u8>`, `[u8; N]` | `Bytes` |
| `Bytes`, `ByteArray`, `ByteBuf` | `Bytes` |
| `BytesB64`, `ByteArrayB64`, `ByteBufB64` | `Bytes` |
| `serde_bytes::Bytes`, `serde_bytes::ByteArray`, `serde_bytes::ByteBuf` | `Bytes` |

### 4.3 向量与 JSON

| Rust 类型 | `FieldType` |
| --- | --- |
| `Vec<bf16>`, `[bf16; N]` | `Vector` |
| `Json`, `serde_json::Value` | `Json` |

> 单独的 `bf16`（或 `half::bf16`）**不是**合法的字段类型。应将其包裹在 `Vec` 中以获得 `Vector`。若需标量覆盖，必须同时使用 `#[field_type = "F32"]` 与 `#[serde(serialize_with = "bf16::serialize_as_f32")]`，因为默认的 bf16 序列化器输出的是整数位模式而非浮点数。

### 4.4 集合容器

| Rust 类型 | `FieldType` |
| --- | --- |
| `Vec<T>`, `VecDeque<T>`, `LinkedList<T>`, `BinaryHeap<T>`, `HashSet<T>`, `BTreeSet<T>` | `Array(T)` |
| `[T; N]`（`T` 为支持的非 byte/非 bf16 类型） | `Array(T)` |
| `(A, B, …)`（2 个或更多元素） | `Array([A, B, …])` — 元组形式的定长表示 |
| `(T,)`（单个元素） | **编译报错** — 单一内部类型已代表任意长度的同构数组 |
| `HashMap<K, V>`, `BTreeMap<K, V>`, `serde_json::Map<…>` | `Map({"*" => V})` |

针对 Map 类型，键 `K` 必须为以下之一：

- 字符串类类型 (`String`, `&str`) → 通配文本键 `"*"`
- 有符号整数类型 (`i8`, `i16`, `i32`, `i64`, `isize`) → 通配整数键 `i64::MIN`
- 二进制类类型 (`Vec<u8>`, `[u8; N]`, `Bytes`, `ByteArray`, `ByteBuf`, `*B64`) → 通配二进制键 `b"*"`

任何其他键类型都会触发编译错误。`Vec<u8>` 与 `[u8; N]` 键在 CBOR 中被作为整数数组处理（serde 对其没有 byte-string 特化）；Schema 端会将该形式强制转换与收敛为 `Bytes` 键。

### 4.5 可选值、智能指针与用户自定义类型

| Rust 类型 | `FieldType` |
| --- | --- |
| `Option<T>` | `Option(T)` |
| `Option<Option<T>>` | **编译报错** — serde 对 `Some(None)` 与 `None` 的序列化形式完全相同 |
| `u128` / `i128` | **编译报错** — AndaDB 整数类型基于 64 位 |
| `Box<T>` / `Arc<T>` / `Rc<T>` / `Cow<'_, T>` | 解构为内部的 `T`（serde 透明序列化它们） |
| 任何其他路径 `Foo`（包含 `Foo<G>`） | 对派生类型进行可失败的类型构建；兼容支持遗留的 `field_type()` 方法 |

特定完全限定路径即使前导段并非类型名也能被显式识别：

- `serde_bytes::Bytes` / `ByteArray` / `ByteBuf` → `Bytes`
- `serde_json::Value` → `Json`
- `half::bf16` → 编译报错（附带修改指引）

圆括号类型 (`(String)`) 以及 `macro_rules!` 替换生成的不可见 group 均可透明解包，因此宏生成的结构体与手写结构体具备完全一致的类型推断行为。

---

## 5. `field_type` DSL

传入 `#[field_type = "..."]` 的字符串由 `parse_field_type_str` 解析，遵循以下语法规范（在递归解析前对空白进行一次规范化；嵌套深度受限）：

```text
type        := primitive | array | option | map
primitive   := "Bytes" | "Text" | "U64" | "I64"
             | "F64"   | "F32"  | "Bool" | "Json" | "Vector"
array       := "Array<" type ">"
option      := "Option<" type ">"          -- type 自身不能是 option
map         := "Map<" map_key "," type ">"
map_key     := "String" | "Text" | "I64" | "i8" | "i16" | "i32" | "i64" | "isize" | "Bytes"
```

### 5.1 String 与 Text 等价性

在 Map 键中，`String` 与 `Text` 是**同义词**：`FieldType` 内部虽只有 `Text` 变体，但 `Map<String, T>` 对于熟悉 `HashMap<String, _>` 的用户更为自然。以下两种声明：

```rust
#[field_type = "Map<String, Json>"]
#[field_type = "Map<Text, Json>"]
```

均展开为相同的通配形式 `Map({"*" => Json})`。

有符号整数 Map 键使用 `I64` 并展开为整数通配键 `i64::MIN`：

```rust
#[field_type = "Map<I64, Text>"]
```

DSL 中的所有*值*类型均支持将 Rust 标量类型写法作为 `FieldType` 名称的同义词——如用 `String` / `str` 代替 `Text`，用 `u8` … `u64` / `usize` 代替 `U64`，用 `i8` … `i64` / `isize` 代替 `I64`，以及 `f32`、`f64` 与 `bool`——使得覆盖声明能够直接镜像字段的原生类型，例如 `#[field_type = "Option<Array<u64>>"]`。而 Map *键*严格受限于上述 `map_key` 规则（`String`、`Text`、`Bytes`、`I64` 及 `i8` … `isize`），因为这是 `FieldKey` 仅有的键变体：`Map<str, T>` 与 `Map<u64, T>` 会导致编译报错。同时严禁 `Option<Option<T>>`。

### 5.2 DSL 映射示例

| DSL 字符串 | 生成的 `FieldType` |
| --- | --- |
| `"Bytes"` | `Bytes` |
| `"Array<U64>"` | `Array(U64)` |
| `"Option<Text>"` | `Option(Text)` |
| `"Map<String, Json>"` | `Map({"*" => Json})` |
| `"Map<Text, Array<U64>>"` | `Map({"*" => Array(U64)})` |
| `"Map<I64, Text>"` | `Map({i64::MIN => Text})` |
| `"Map<Bytes, F64>"` | `Map({b"*" => F64})` |
| `"Option<Map<Bytes, F64>>"` | `Option(Map({b"*" => F64}))` |

### 5.3 编译期诊断提示

无法识别的输入会在对应宏源码位置抛出 `compile_error!`：

```text
Unsupported field type: '...'. Supported types: Bytes, Text, U64, I64,
F64, F32, Bool, Json, Vector, Array<T>, Option<T>, Map<String, T>,
Map<Text, T>, Map<I64, T>, Map<Bytes, T> (Rust spellings such as u64, i64,
f64, bool and String are accepted as synonyms)
```

```text
Unsupported Map key type: '...'. Expected 'String', 'Text', 'I64' or 'Bytes'.
```

```text
Invalid Map field type: '...'. Expected 'Map<KeyType, ValueType>'.
```

---

## 6. 使用示例

### 6.1 包含多种特性的完整 Schema

```rust
use anda_db_derive::AndaDBSchema;
use anda_db_schema::{Document, FieldEntry, FieldType, Fv, Schema, SchemaError, bf16};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(AndaDBSchema, Serialize, Deserialize, Debug)]
struct Article {
    /// AndaDB 管理的主键
    _id: u64,
    /// 文章标题（唯一索引）
    #[unique]
    title: String,
    /// 文章正文
    content: String,
    /// 累计浏览量
    views: u64,
    /// 发布状态标记
    published: bool,
    /// 可选标签列表
    tags: Option<Vec<String>>,
    /// 自由格式作者元数据
    author_meta: Option<serde_json::Value>,
    /// 向量嵌入
    embedding: Vec<bf16>,
}

fn main() -> Result<(), SchemaError> {
    let schema = Arc::new(Article::schema()?);

    let mut doc = Document::new(schema.clone());
    doc.set_id(1);
    doc.set_field("title",     Fv::Text("Hello World".into()))?;
    doc.set_field("content",   Fv::Text("This is the body".into()))?;
    doc.set_field("views",     Fv::U64(0))?;
    doc.set_field("published", Fv::Bool(true))?;
    doc.set_field("embedding", Fv::Vector(vec![bf16::from_f32(0.1); 512]))?;
    Ok(())
}
```

### 6.2 嵌套 `FieldTyped` 结构体

```rust
use anda_db_derive::{AndaDBSchema, FieldTyped};
use anda_db_schema::FieldType;

#[derive(FieldTyped, Debug)]
struct GeoLocation {
    latitude: f64,
    longitude: f64,
}

#[derive(AndaDBSchema)]
struct Place {
    _id: u64,
    name: String,
    /// 宏展开时调用 `GeoLocation::field_type()`
    location: GeoLocation,
}
```

### 6.3 覆盖推断类型

```rust
use anda_db_derive::AndaDBSchema;
use ic_auth_types::Xid;

#[derive(AndaDBSchema)]
struct Transaction {
    _id: u64,
    /// 将 `Xid` newtype 视作定长二进制字段
    #[field_type = "Bytes"]
    tx_id: Xid,
    /// 可选二进制 ID 列表 — 一次嵌套三层 DSL
    #[field_type = "Option<Array<Bytes>>"]
    inputs: Option<Vec<Xid>>,
    amount: u64,
}
```

---

通过已知容器直接递归（例如 `Option<Box<Self>>`）在编译期被拒绝，除非进行了显式类型覆盖。间接递归和类型别名递归在运行时由 `try_field_type()` 捕获并返回 `SchemaError`；`schema()` 为派生的嵌套类型传播该错误。不可失败的 `field_type()` 包装函数在遇到非法声明时会触发 panic。

结构体固定字段名禁止使用 `"*"` 与 `i64::MIN`。容器级 serde `tag` 与 `into`、容器级 `field_type`、重复的 `field_type`/`unique` 均会被拒绝。所有顶层字段（包含 `_id`）禁止使用整数 CBOR 键。借用与透明包装的 Map 键会根据其序列化键类型完成推断。

## 7. 内部实现

### 7.1 `common.rs`

| 符号 | 职责说明 |
| --- | --- |
| `named_fields` | 两个派生宏共享的“具名字段结构体”基础校验。 |
| `parse_container_serde_attrs` | 提取 `rename_all`（包含带方向形式）与 `transparent`。 |
| `parse_field_serde_attrs` | 提取 `rename`（包含带方向形式）、`skip*` 与 `flatten`。 |
| `RenameRule` | 兼容 serde 的 `rename_all` 大小写转换器。 |
| `effective_field_name` | 解析最终序列化名称（显式 rename 优于 rename_all）。 |
| `validate_schema_field_name` | 编译期镜像 `anda_db_schema::validate_field_name` 校验规则。 |
| `resolve_field_type` | 处理 `#[field_type]` 覆盖或回退至自动类型推断。 |
| `find_field_type_attr` | 提取并解析 `#[field_type = "..."]`。 |
| `parse_field_type_str` | 将 `field_type` DSL 编译为 `FieldType` token 流。 |
| `determine_field_type` | 直接从 `syn::Type` 推断 `FieldType`。 |
| `is_u8_type` | `u8` 类型断言判断。 |
| `is_string_type` | `String` / `str` 类型断言判断。 |
| `is_bytes_type` | 支持的 byte 容器类型断言判断。 |
| `is_bf16_type` | `bf16` 类型断言判断。 |
| `is_u64_type` | 用于校验 `_id: u64` 约束的类型断言。 |

所有可失败辅助函数均返回 `syn::Result`，并将错误 span 准确定位在违规的字段、类型或属性上，确保编译器诊断直接指向用户代码而非 `#[derive(...)]` 所在行。

### 7.2 `schema.rs`

`anda_db_schema_derive` 执行的流水线：

1. 将输入解析为 `DeriveInput`。
2. 拒绝非具名字段结构体，拒绝 `#[serde(transparent)]`。
3. 针对每个字段：
   - 若字段为 `_id`，验证其类型为 `u64` 且序列化名称保持为 `"_id"`，随后跳过该字段的代码生成；
   - 跳过标记了 `#[serde(skip)]` / `#[serde(skip_serializing)]` 的字段，拒绝 `#[serde(flatten)]`；
   - 解析最终序列化名称（显式 rename > `rename_all` > Rust 标识符），并根据 AndaDB 命名规则、保留的 `_id` 列及先前已见名称进行校验；
   - 通过 `#[field_type]` 或 `determine_field_type` 确定字段类型；
   - 提取以空格连接的 `///` 文档注释与 `#[unique]` 标记，随后组合成 `FieldEntry::new(...)?[.with_description(...)][.with_unique()]`。
4. 生成 `impl <Struct> { pub fn schema() -> Result<Schema, SchemaError> { … } }`。逐字段的错误以带有 span 的 `compile_error!` 就地输出，使得单次编译即可报告所有违规字段。

### 7.3 `field_typed.rs`

与 `schema.rs` 具有相同的前置解析与校验逻辑（但不包含 AndaDB 字段命名限制——嵌套 map 键允许自由格式）。针对每个参与序列化的字段，该宏生成 `(key, <field_type>)` 二元组并聚合为单个 `FieldType::Map`。生成的构造函数使用线程局部的类型构建守卫来验证最终声明。Key 通常是 serde 序列化字段名；当字段带有 `#[cbor(key = N)]` 时，生成的键为 `FieldKey::from(N)`，使得 Schema 能够精准对应 CBOR 整数键 map。

---

## 8. 诊断与错误处理

### 8.1 编译期错误

所有错误信息均精确定位在对应的字段、类型或属性位置。错误信息中的类型以 Rust 源代码呈现（例如 `fn() -> u64`），而非 AST 语法树转储。

| 错误信息摘要 | 触发原因 |
| --- | --- |
| `FieldTyped only supports structs` | 应用于 enum 或 union。 |
| `FieldTyped only supports structs with named fields` | 应用于 tuple 或 unit struct。 |
| `AndaDBSchema only supports structs` | 应用于 enum 或 union。 |
| `AndaDBSchema only supports structs with named fields` | 应用于 tuple 或 unit struct。 |
| `The '_id' field must be of type u64` | `_id` 声明的类型非 `u64`。 |
| `serde renames '_id' to "...", but the primary key must serialize as "_id"; …` | `rename_all` 或 `rename` 改变了 `_id` 的序列化名称。 |
| `field "..." serializes as "_id", which collides with the auto-generated primary key` | 其他字段被重命名为 `_id`。 |
| `schema field name "..." is not a valid AndaDB field name (...)` | 序列化名称违反 `[a-z0-9_]{1,64}` 约束（仅 `AndaDBSchema`）。 |
| `duplicate schema field name "..." (after serde renaming)` | 两个字段经 serde 重命名后产生冲突。 |
| `#[serde(flatten)] is not supported: …` | 展平字段无法映射为单一 Schema 条目。 |
| `… does not support #[serde(transparent)]: …` | 透明结构体无法序列化为 Map。 |
| `unknown #[serde(rename_all = "...")] rule; …` | 无法识别的命名大小写转换规则。 |
| `Unsupported field type: '...'. Supported types: …` | `#[field_type]` 中的 DSL 语法无法识别。 |
| `Unsupported Map key type: '...'. Expected 'String', 'Text', 'I64' or 'Bytes'.` | `Map<K, V>` 中使用了非法的键类型。 |
| `Invalid Map field type: '...'. Expected 'Map<KeyType, ValueType>'.` | DSL `Map<…>` 缺少逗号分隔的键值对。 |
| `Unsupported type: '...'. Consider: …` | 自动类型推断失败（例如 `()`、trait object、裸函数等）。 |
| `Option<Option<T>> cannot be described: …` | Rust 类型或 DSL 中存在嵌套 `Option`。 |
| `'u128' is not representable: AndaDB integers are 64-bit …` | 未加覆盖说明的 `u128` / `i128` 字段。 |
| `Standalone 'bf16' is not supported as a field type. Use 'Vec<bf16>' …` | 未包裹在 `Vec` 或缺少覆盖说明的裸 `bf16`。 |

### 8.2 运行时错误

`schema()` 最终调用 `Schema::builder().build()`，可能返回以下错误：

| 错误变体 | 描述说明 |
| --- | --- |
| `SchemaError::FieldName` | 字段名称非法。 |
| `SchemaError::FieldType` | 字段类型非法。 |
| `SchemaError::Validation` | Schema 级别整体验证未通过。 |
