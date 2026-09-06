**anda_db_utils / anda_object_store：19 项清单处理结果**

更新日期：2026-09-06。根据本任务的完整审查落实 R1–R9、P1–P7、M1–M2、D1，并根据对 `e21d9d2` 的复审补齐 multipart 发布、abort、overwrite-only 后端、generation 占位对象及残余缓冲五个边界。全程未使用 subagents；保留工作区其他独立改动。最初复现中的异常已经转为正式回归，下面记录最终实现与验收结果。

**验证结果**

| 检查 | 结果 |
| --- | --- |
| 两个子库的单元/回归/文档测试 | 47 项 utils 单元、79 项 store 单元、32 项新增集成回归、4 项文档测试通过 |
| 两子库所有目标、所有特性的严格 Clippy | 通过，-D warnings |
| cargo check --workspace --all-features --locked | 通过 |
| cargo test --workspace --all-features --locked | 全部通过、0 失败；1 项 fixture 生成测试按原设置忽略 |
| 两个子库严格 Rustdoc | 通过，RUSTDOCFLAGS=-D warnings |
| 格式与补丁空白检查 | 通过 |

忽略项是需要手工更新并提交的 on-disk format fixture 生成测试。没有执行真实云存储集成、设备断电或整个 TypeScript 套件；本次未改动 TypeScript 引擎。

**本地性能结果与取舍**

[完整基准、原始 CSV、进程资源记录及源码摘要](/Users/zensh/git/github.com/ldclabs/anda-db/docs/benchmarks/anda_libraries_20260906/README.md:1)。旧版、新版使用独立干净构建目录和同一工作负载；所有结果都保留，包含变慢场景。

在 InMemory 的本地微基准中，16 MiB 加密 put 的中位耗时约从 24.9 ms 降到 5.25 ms；4 MiB 对象、1 KiB 加密块的 metadata 压力场景下，热 head 约从 25.3 µs 降到 1.13 µs。交错范围读取约快 8.5 倍。它们不代表生产网络/磁盘的端到端加速。

冷元数据读取、部分 From<Vec>/全唯一 String 构建和列表增加了检查或分配成本。保留有界内存和统一校验的取舍，并给出完整测量数据。不能把“已处理性能清单”理解成每个输入都会更快。

**逐项完成情况**

- [x] **R1：统一元数据校验和删除边界。**

  [sidecar 校验](/Users/zensh/git/github.com/ldclabs/anda-db/rs/anda_object_store/src/sidecar.rs:260)。读取、覆盖清理、删除及 GC 统一检查 generation 结构和元数据认证。无效文档按未知引用保守处理；显式修复不跟随伪造路径。严格模式误删和父 key 指针误删子 key 的复现均已转为回归。

- [x] **R2：修复提交结果不确定时的缓存。**

  [提交失效守卫](/Users/zensh/git/github.com/ldclabs/anda-db/rs/anda_object_store/src/sidecar.rs:97)。元数据 put/delete 一旦开始，错误、panic 或 future 取消都会由同步 Drop 守卫使共享缓存失效；缓存发布完成后才解除守卫。采用罕见异常时清空共享缓存的保守策略，避免异步失效遗漏取消和持续保存旧 token。覆盖两种包装器、put/delete、错误/取消及条件读取、列表。

- [x] **R3：补齐 multipart 生命周期。**

  [共享上传状态](/Users/zensh/git/github.com/ldclabs/anda-db/rs/anda_object_store/src/upload.rs:17)。跟踪所有 part future，失败或丢弃后禁止成功提交；尾段/数据完成阶段取消会使上传作废。数据已完成时仅重试元数据，不再重复调用内部 complete；首次发布记录原提交身份，旧 payload 即使未清理，重试也不能覆盖其后的新提交。abort 在后端清理成功后才进入终态，失败可重试；已物化 payload 在同 key 锁内确认未提交后才删除，响应丢失不会导致已提交对象被删。完成/abort 释放 in-flight 注册和不再需要的元数据，重复 complete 不重新发布。InMemory/LocalFileSystem 均有回归。

- [x] **R4：旧格式复制时固定分块参数。**

  [加密复制](/Users/zensh/git/github.com/ldclabs/anda-db/rs/anda_object_store/src/encryption.rs:966)。copy/rename 为旧文档补存实际 chunk_size，保留原 chunk-AAD 模式后重新认证。缺少 c 的旧对象迁移后，改用另一默认分块大小并启用严格认证仍可读。

- [x] **R5：分离加密块与传输段。**

  [密文传输段组装](/Users/zensh/git/github.com/ldclabs/anda-db/rs/anda_object_store/src/upload.rs:154)。默认输出 8 MiB 固定传输段，最后一段可短；新增 with_multipart_part_size，最小 5 MiB。用带最小段/非末段等长检查的后端验证非对齐输入及并行 part 等待，避免 R2 不兼容。完整物理段发出后复制不足一段的残余数据，避免小尾段继续持有大块 backing allocation。保留底层服务的对象/分段上限。

- [x] **R6：修复故障注入覆盖。**

  [故障工具](/Users/zensh/git/github.com/ldclabs/anda-db/rs/anda_object_store/src/fault.rs:21)。增加 MultipartStart/Part/Complete/Abort 事件、ErrorAfter、PauseBefore/PauseAfter 门控。预算覆盖各修改阶段，TornWrite 在非普通 put 上明确失败。事件日志区分后端成功、响应失败与取消；旧 mutation_log 明确表示已放行请求。各 multipart 阶段的 Error/Crash、响应丢失和取消都已测试。

- [x] **R7：修复 UniqueVec 析构异常。**

  [统一删除入口](/Users/zensh/git/github.com/ldclabs/anda-db/rs/anda_db_utils/src/lib.rs:370)。使用 set.take，先完成 vector 修改，再析构取出的副本。remove/remove_if/swap_remove_if 复用同一入口；保留 retain 守卫。Hash、Eq、Clone、Drop 的恢复性 panic 回归检查成员一致性及去重。

- [x] **R8：移除多余 Clone 约束。**

  [MetaStore Clone](/Users/zensh/git/github.com/ldclabs/anda-db/rs/anda_object_store/src/lib.rs:132)、[EncryptedStore Clone](/Users/zensh/git/github.com/ldclabs/anda-db/rs/anda_object_store/src/encryption.rs:135)。手写 Clone，仅共享 Arc 和配置；非 Clone 后端可正常使用。编译断言覆盖 FaultStore<InMemory>，共享实例的现有 CAS 并发测试继续通过。

- [x] **R9：加固 generation 标识及冲突处理。**

  [generation 与 ETag](/Users/zensh/git/github.com/ldclabs/anda-db/rs/anda_object_store/src/generation.rs:17)。采用 128 位随机盐加进程内 64 位序列，时间回拨或随机源重复仍不复用进程内 ID。严格识别新格式与合法旧 8 位盐格式。普通 payload 优先用 Create 并重试碰撞；copy 同样优先使用条件创建。后端明确不支持这两种条件操作时，在单写者约束下以强 ID 配合 Overwrite 兼容。multipart 只做存在性检查并直接创建上传，不再落盘零字节占位，因此正常 abort、初始化失败和版本化后端都不会残留额外对象版本。固定时间/重复随机值、注入已有路径及 overwrite-only 后端回归均已覆盖。

- [x] **P1：缓存已验证元数据。**

  [认证上下文](/Users/zensh/git/github.com/ldclabs/anda-db/rs/anda_object_store/src/encryption.rs:282)。私有证书绑定验证上下文及路径，不能从序列化数据获得；Clone/重签名清除证书。热缓存不再扫描全部 tags 做 GMAC。测试验证一次认证后反复 head/get/list 不重复认证，并拒绝不同密钥、路径和严格策略继承旧证书。

- [x] **P2：增加缓存和输入预算。**

  [资源配置](/Users/zensh/git/github.com/ldclabs/anda-db/rs/anda_object_store/src/limits.rs:10)。默认估算缓存预算 64 MiB，并保留原条目数上界；新增 builder 字节预算与元数据限制。编码体积、对象大小、chunk 数量、tag 数关系均受检查，读取过程检查实际接收字节。自定义缓存保持自定义驱逐策略，但不能绕过认证/布局/大小验证。预算是估算值，不是进程 RSS 硬上限。

- [x] **P3：合并和并发范围读取。**

  [范围规划与解密](/Users/zensh/git/github.com/ldclabs/anda-db/rs/anda_object_store/src/encryption/ranges.rs:59)。将重复/交叠范围去重，合并相邻块且不填补空洞；每次请求最多 8 MiB 或一个更大的加密块，最多 8 个并发请求。按输入顺序还原结果，小片段不保留巨大缓冲。随机范围 oracle、大范围跨传输/加密边界和请求计数回归通过。

- [x] **P4：改进 UniqueVec 构建与反序列化。**

  [构建器](/Users/zensh/git/github.com/ldclabs/anda-db/rs/anda_db_utils/src/lib.rs:171)。FromIterator 与 Serde 直接去重构建；From<Vec> 复用原缓冲并收缩高稀疏度结果。构建期间使用有界抽样预留和首次重复后的查询策略，保留公开 push 的 panic 回滚。百万个相同整数最终只有常数级容量；真实借用 &str 反序列化通过。全唯一 String 和部分 From<Vec> 存在速度/分配取舍，已在基准中披露。

- [x] **P5：移除全量 payload ETag 哈希。**

  [提交 token](/Users/zensh/git/github.com/ldclabs/anda-db/rs/anda_object_store/src/generation.rs:49)。新 ETag 只从域分隔符和 generation 派生，两个上传器不再维护 payload hasher。旧 token 仍按不透明字符串读取/比较；复制和重命名生成独立 token，ABA 测试继续通过。基准支持保留该改动。

- [x] **P6：减少解密分配和保留内存。**

  [流式解密](/Users/zensh/git/github.com/ldclabs/anda-db/rs/anda_object_store/src/encryption.rs:1250)。分块 AAD 改为栈上 52 字节数组；单块解密复用 helper。流式解密只复制有界批次，小块最多聚合 64 KiB，大块以一块为下界；避免每个小块一次分配及复制整个上游对象。验证首段不保留整个对象分配，并保持每块验证后才输出明文。

- [x] **P7：优化 GC 的 I/O 和内存。**

  [可配置 GC](/Users/zensh/git/github.com/ldclabs/anda-db/rs/anda_object_store/src/sidecar.rs:1046)。并发标记、按 key 分组重查，减少同一 key 历史 generation 的重复元数据读取。提供逻辑 prefix、并发数、标记和候选预算；超预算时在任何删除前失败。删除前在同 key 锁内重查并保留 in-flight 保护。10 个同 key 垃圾 generation 的元数据 GET 从 11 次减为 2 次。

- [x] **M1：简化重复代码与文件组织。**

  [独立测试模块](/Users/zensh/git/github.com/ldclabs/anda-db/rs/anda_object_store/src/tests.rs:1)、[工具测试](/Users/zensh/git/github.com/ldclabs/anda-db/rs/anda_db_utils/src/tests.rs:1)。拆开五个源文件中的测试，抽取 generation、limits、upload 和 ranges helper。intersect_with 复用 retain，删除入口收敛；列表使用有序 buffered，移除索引回排。保持公开类型及序列化接口，新增配置方法。

- [x] **M2：更新文档和边界语义。**

  [技术文档](/Users/zensh/git/github.com/ldclabs/anda-db/docs/anda_object_store.md:1)。README 版本更新为 0.11；修正 ETag/padding、随机盐概率、旧实现说明、共享协调范围、资源限额和上传状态。明确拒绝版本寻址；元数据子请求传递支持的 Extensions，保留 get/put/complete/分隔列表相应响应 Extensions。严格 Rustdoc 检查及上下文回归通过。

- [x] **D1：启用本地写入 fsync 并明确限制。**

  [DB 服务](/Users/zensh/git/github.com/ldclabs/anda-db/rs/anda_db_server/src/main.rs:121)、[Nexus 服务](/Users/zensh/git/github.com/ldclabs/anda-db/rs/anda_cognitive_nexus_server/src/main.rs:481)。两个服务和存储示例开启 with_fsync(true)，同步更新本地技能示例。基准保留 fsync 开关的成本数据。上游 object_store 0.14.1 的独立 delete 不执行 fsync，已明确写入文档；本次没有宣称或测试所有操作的真实断电耐久性。

**兼容性与部署注意事项**

旧 CBOR 字段、旧 ETag、合法旧 generation 及未认证 legacy 加密对象的兼容模式继续保留。新写入不支持历史版本读取，跨独立实例多写者仍需外部协调。默认新增的元数据/GC/缓存预算可以显式调整；严格认证迁移策略保持可选。

成功 complete、abort 和失败的 payload materialization 会释放上传注册。已经失败的 part 应执行 abort 或丢弃上传器以释放剩余后端资源；GC 不负责枚举或终止云服务内部的未完成 multipart 会话。

本地 fsync 已用于写入，但上游独立删除和平台目录同步的限制仍然存在。需要完整主机断电保证的部署应选择提供相应删除耐久性的后端并单独验证。
