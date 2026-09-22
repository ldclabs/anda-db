# AndaDB 技术文档中心

[English](README.md)

本目录是 AndaDB 工作区的技术文档中心。各文档聚焦于系统技术栈的不同层级，涵盖嵌入式数据库引擎、Schema 派生、索引底层机制、基于对象存储的持久化、[KIP 协议](https://github.com/ldclabs/kip) 以及 Cognitive Nexus 知识图谱。

新用户建议由此通览，然后按需查阅对应层级的详细文档。

## 文档内容范围

本目录文档旨在阐明以下核心问题：

- 核心嵌入式数据库的功能与使用方式
- Schema、Document 与 derive 宏的运作机制
- 精确匹配、词法检索与向量索引的内部机制与行为
- 基于 [`anda_object_store`](./anda_object_store.zh.md) 构建的持久化方案
- [KIP](https://github.com/ldclabs/kip) 的语法解析与执行模型
- Cognitive Nexus 如何将 AndaDB 构筑为 AI 记忆图谱

本目录不重复各 crate 的说明。服务部署、环境变量配置及二进制命令的使用说明，请参考 `rs/` 目录下各 crate 的 README。

## 推荐阅读路径

### 嵌入式数据库使用者

若需将 AndaDB 直接嵌入到 Rust 应用中：

1. [anda_db.zh.md](./anda_db.zh.md)
2. [anda_db_schema.zh.md](./anda_db_schema.zh.md)
3. [anda_db_derive.zh.md](./anda_db_derive.zh.md)
4. [anda_db_btree.zh.md](./anda_db_btree.zh.md)
5. [anda_db_tfs.zh.md](./anda_db_tfs.zh.md)
6. [anda_db_hnsw.zh.md](./anda_db_hnsw.zh.md)
7. [anda_object_store.zh.md](./anda_object_store.zh.md)

### 知识图谱与智能体记忆使用者

若需构建高阶 AI 记忆系统或基于 KIP 协议的应用：

1. [anda_kip.zh.md](./anda_kip.zh.md)
2. [anda_cognitive_nexus.zh.md](./anda_cognitive_nexus.zh.md)
3. [anda_db.zh.md](./anda_db.zh.md)
4. [anda_object_store.zh.md](./anda_object_store.zh.md)

### 存储与底层部署实现者

若关注持久化、数据耐久性、落盘加密或跨存储后端的移植能力：

1. [anda_object_store.zh.md](./anda_object_store.zh.md)
2. [anda_db.zh.md](./anda_db.zh.md)
3. [anda_db_btree.zh.md](./anda_db_btree.zh.md)
4. [anda_db_tfs.zh.md](./anda_db_tfs.zh.md)
5. [anda_db_hnsw.zh.md](./anda_db_hnsw.zh.md)

## 文档导航图

| 文档 | 层级 | 内容概述 | 适用场景 |
| --- | --- | --- | --- |
| [anda_db.zh.md](./anda_db.zh.md) | 核心数据库 | 数据库生命周期、集合、索引模型、查询模型、存储集成、耐久性与故障恢复 | 了解主嵌入式数据库 API 与运行行为 |
| [anda_db_schema.zh.md](./anda_db_schema.zh.md) | 类型系统 | 字段类型、字段值、Schema、文档、资源模型、序列化 | 设计 Schema、校验文档结构或检查磁盘类型规则 |
| [anda_db_derive.zh.md](./anda_db_derive.zh.md) | 代码生成 | `AndaDBSchema`、`FieldTyped`、属性宏、类型推断、`field_type` DSL | 通过 Rust struct 生成 Schema 并理解宏展开机制 |
| [anda_db_btree.zh.md](./anda_db_btree.zh.md) | 精确/范围检索 | 倒排 B-Tree 设计、范围查询、分桶持久化、正确性说明 | 优化过滤、精确查找、唯一约束或分桶整理 (compaction) |
| [anda_db_tfs.zh.md](./anda_db_tfs.zh.md) | 全文检索 | BM25 算法、分词管道、分桶分片、持久化布局 | 优化词法搜索、文本重排或分词行为 |
| [anda_db_hnsw.zh.md](./anda_db_hnsw.zh.md) | 向量检索 | HNSW 近似最近邻索引、bf16 向量、写入管道、持久化产物 | 调整向量检索与索引超参数 |
| [anda_object_store.zh.md](./anda_object_store.zh.md) | 存储底座 | `MetaStore`、`EncryptedStore`、可移植条件写入、AES-256-GCM 分块加密 | 构建可移植或落盘加密的对象存储持久化方案 |
| [anda_kip.zh.md](./anda_kip.zh.md) | 协议层 | KIP 解析器、AST、请求/响应模型、Executor 接口、错误码 | 集成面向大模型的协议处理或实现自定义后端 |
| [anda_cognitive_nexus.zh.md](./anda_cognitive_nexus.zh.md) | 知识图谱运行时 | 参考 KIP 执行器、概念/命题模型、图执行、引导启动流程 | 基于 AndaDB 构建或调试 AI 记忆系统 |
| [testing.zh.md](./testing.zh.md) | 质量保障 | 崩溃一致性测试框架、故障注入、属性测试、Fuzzing、召回率基线、格式兼容固件 | 为新特性添加测试或理解持久性测试契约 |

## 技术栈分层架构

```text
Application / Agent Runtime
  -> anda_kip                    协议、AST、请求/响应、Executor trait
  -> anda_cognitive_nexus        参考 KIP 执行器与知识图谱
  -> anda_db                     嵌入式存储与检索核心
     -> anda_db_schema           Schema 与文档模型
     -> anda_db_derive           Schema 生成的 derive 宏
     -> anda_db_btree            精确与范围索引
     -> anda_db_tfs              BM25 全文索引
     -> anda_db_hnsw             HNSW 向量索引
     -> anda_object_store        可移植元数据与加密封装
     -> object_store             本地与云端存储后端抽象
```

此架构设计具有明确职责边界：

- `anda_db` 是嵌入式存储与检索核心
- `anda_kip` 是面向模型的知识交互协议层
- `anda_cognitive_nexus` 是基于二者构建的参考图记忆系统

## 服务层文档

部分服务组件的核心说明维护在其各自 crate 的 README 中，而非本目录：

- `rs/anda_db_server/README.md`：核心数据库 HTTP 服务
- `rs/anda_cognitive_nexus_server/README.md`：KIP HTTP/JSON-RPC 服务
- `rs/anda_db_shard_proxy/README.md`：分片路由与多租户代理

## 快速入口

根据具体任务快速定位对应文档：

- 添加集合与混合检索：[anda_db.zh.md](./anda_db.zh.md)
- 设计字段布局与迁移规则：[anda_db_schema.zh.md](./anda_db_schema.zh.md)
- 在 Rust 结构体上使用 derive 宏：[anda_db_derive.zh.md](./anda_db_derive.zh.md)
- 调试范围过滤：[anda_db_btree.zh.md](./anda_db_btree.zh.md)
- 调优 BM25 或分词配置：[anda_db_tfs.zh.md](./anda_db_tfs.zh.md)
- 优化向量检索召回率与内存占用：[anda_db_hnsw.zh.md](./anda_db_hnsw.zh.md)
- 了解可移植条件写入与落盘加密：[anda_object_store.zh.md](./anda_object_store.zh.md)
- 解析与执行 KIP 语句：[anda_kip.zh.md](./anda_kip.zh.md)
- 了解 AI 记忆图谱运行时：[anda_cognitive_nexus.zh.md](./anda_cognitive_nexus.zh.md)

## 与项目根目录 README 的关系

项目根目录 [README.md](../README.md) 提供产品级全景概览：

- AndaDB 是什么
- 各工作区 crate 的职责
- 快速入门指引
- 主要示例位置

本 `docs/README.zh.md` 为技术细节深入研究的索引目录。
