# anda_object_store

[English](anda_object_store.md)

`MetaStore` 为 `ObjectStore` 增加了逻辑元数据与条件更新能力。
`EncryptedStore` 增加了分块 AES-256-GCM 加密能力，并采用相同的提交协议。二选其一即可；嵌套使用通常只会增加冗余的元数据 I/O。

## 存储布局与提交协议

```text
meta/<key>              CBOR 元数据：逻辑对象的提交点 (commit point)
gen/<key>/<generation>  有效载荷 (payload)，一旦发布即不可变
data/<key>              旧版无 generation 标记元数据所对应的遗留 payload
```

常规的 put 操作先写入新的 generation payload，发布对应的元数据，随后以尽力而为的方式移除被替换的旧 payload。Generation 写入优先采用 `PutMode::Create`，若发生 ID 碰撞最多重试 8 次。若后端明确报告不支持条件创建，generation payload 会转为覆盖模式 (overwrite)；强随机 ID 与单写入进程契约使得与未知路径发生碰撞的概率微乎其微。

分块上传 (Multipart) 会先分配并注册全新 ID，验证尚无 payload 占用该 ID，随后直接启动后端的上传流程。它不会在磁盘持久化零字节的占位对象，因此中止操作 (abort) 或初始化失败绝不会泄露占位文件或产生多余的对象版本。复制操作同样优先采用 `CopyMode::Create`，重试碰撞，仅在后端不支持条件复制创建时才退回覆盖模式。在所有情况下，元数据始终是逻辑层面的唯一提交点。

新的 generation ID 包含 16 位十六进制的毫秒时间戳、32 位十六进制的随机盐 (128 位)，以及 16 位的进程级单调自增序号。即使发生系统时钟回拨或随机源重复，单调序号也能杜绝进程内部的 ID 复用；随机性则用于隔离不同的独立进程。读取器与 GC 同样兼容旧版的 `<16 hex>-<8 hex>` 格式。两种格式均严格要求全十六进制字符且仅占单个路径分段。

在元数据发布前发生的失败，会保留之前已提交的对象处于可读状态。在发布后发生的失败，可能意味着即使调用方收到了错误，新写入实际上已在后端生效。当元数据 put 或 delete 的结果处于未知状态，或其 future 被取消时，同步的 `Drop` 保护守卫会使共享元数据缓存完全失效。下一次查找将从存储后端重新加载已提交的权威状态。该策略在罕见的未知异常时主动使整个缓存变冷；常规成功操作则仅更新其自身的 key。

读取器先解析提交点，然后读取对应的不可变 payload。若该 payload 已被替换并回收，`get`、范围读取与复制操作在遇到 `NotFound` 时会刷新元数据并重试一次。这并非通用网络重试策略。由于历史 payload 会被回收，系统不支持历史版本寻址。

## 并发控制与条件操作

同一 wrapper 包装器的克隆实例共享其元数据缓存、按 key 的计算锁以及在途的 generation 注册表。后端自身无需实现 `Clone`。单独构建的不同包装器实例即使位于同一进程内，也不会自动共享该协调机制。

每个逻辑存储空间应保证单一协调写进程。在共享的包装器实例内部，针对同一 key 的数据变更被串行化。在跨独立实例的场景下，针对不存在提交点的 `PutMode::Create` 由后端的元数据条件写入负责仲裁。跨实例的 `Overwrite`、`Update` 与 GC 必须依赖外部的单写入者协调机制。

Generation payload 的兼容回退逻辑并不会在软件层面模拟后端的条件写入。因此，若存储后端自身不支持条件元数据 put，则无法提供跨实例的逻辑 `Create`；在此类后端上必须依赖外部协调机制。

- `Create` 拒绝已存在的合法文档。显式的 create/overwrite 可能会重建可解码但不合法、或完全无法解码的异常文档；清理时绝不会跟随不可信指针。
- `Update` 要求提供当前逻辑 ETag，并在按 key 锁定的临界区内向后端重新比对。元数据无效或不存在均无法满足该条件。不支持 `UpdateVersion::version` 并会返回错误。
- `if_match`、`if_none_match` 及时间条件均针对逻辑对象进行求值。ETag 条件优先于对应的时间条件（遵循 `GetOptions::check_preconditions` 规范）。
- `GetOptions::version` 返回 `NotSupported`；操作结果暴露 `version: None`。
- 重命名 (Rename) 采用“先复制后删除”策略，并非跨 key 的多对象原子事务。针对自身的重命名操作会验证其存在性与模式，而不会删除对象。

### 逻辑 ETag

ETag 是一个不透明的**提交身份标识 (commit identity)**，而非内容哈希校验和。新写入、复制与重命名操作生成填充的 URL-safe Base64 字符串，计算公式为：
`SHA3-256("anda_object_store.commit.v2:" || generation)`。
生成该 token 无需对全部 payload 进行哈希计算。连续提交中即便利载内容完全相同，也会产生不同的 ETag，防止陈旧 token 在 A → B → A 覆写后依然有效。条件操作继续将旧元数据中基于 payload 内容生成的旧版 ETag 作为不透明字符串接收与比对。不要试图从内容重新计算 ETag。

`MetaStore` 不负责校验 payload 字节的真伪。`EncryptedStore` 则通过 AES-GCM 块认证标签验证密文，并对其 sidecar 元数据单独执行认证。

## 元数据格式与向后兼容性

元数据采用紧凑的 CBOR 与 `cbor2` 进行序列化。通用字段包括：

| 字段 | 含义 |
| --- | --- |
| `s` | 逻辑字节大小 |
| `e` | 可选的不透明逻辑 ETag |
| `g` | 可选 generation；缺失表示旧版 `data/<key>` 布局 |
| `m` | 可选的提交时间戳（Unix 毫秒） |
| `o`, `v` | 旧版后端 ETag/版本字段 |

`MetaStore` 在新写入中不再输出缺失的 `o`/`v`。加密元数据在序列化时保留它们，因为旧版认证 AAD 将其纳入了签名范围。加密元数据增加以下字段：

| 字段 | 含义 |
| --- | --- |
| `n` | 12 字节基础 Nonce |
| `t` | 按顺序排列的 16 字节分块 Tag 数组 |
| `c` | 用于加密该对象的块大小 (chunk size) |
| `av` | 块 AAD 版本：0 为旧版空 AAD，1 为绑定上下文的 AAD |
| `an`, `at` | Sidecar 认证 Nonce 与 Tag |

Sidecar 的认证字段涵盖逻辑路径、大小、ETag、Nonce、Tags、分块配置、generation 与提交时间。新引入的可选字段仅在存在时才追加至 AAD 中，从而保证对旧版封印文档的验证兼容性。内存中的验证凭证 (validation certificate) 绝不被序列化；克隆元数据会清除该凭证，重新封印也会清除先前的凭证。

缺少 `g` 的 0.10 之前旧文档保持完全可读。覆盖写入操作会将其迁移至新的 generation，并尽力清理旧版 payload。复制与重命名操作保留源对象的 chunk-AAD 模式，显式记录其实际使用的 chunk size（即使旧元数据缺少 `c` 字段），并针对目标路径重新封印。因此，重新配置存储空间的默认 chunk size 不会影响已迁移对象的正常读取。注意：0.10 之前的旧二进制程序无法识别新的 generational 目录布局。

`m` 为 get/head/list 提供统一步调的逻辑时间戳。缺少 `m` 的早期代际元数据回退至 generation 自身的时间戳；两者皆缺失的更老元数据则回退至底层后端对象的物理时间戳（旧版 listing 与 payload 时间戳可能存在微小差异）。

## 加密机制与范围读取

加密算法采用 AES-256-GCM。密文长度与明文完全一致，因为认证 Tag 统一集中存放在 sidecar 元数据中。默认加密块大小为 256 KiB；若配置为 0 则自动规范化为 1 字节。应根据预期的范围读取模式选择合适的 chunk size；若使用极小的分块，需同步调大元数据大小限制。

每个对象分配一个随机的 96 位基础 Nonce。单个块的 Nonce 保留前 4 字节，并将块序号累加至末尾 64 位的计数器中。块序号在单个对象内部严格唯一；跨对象的随机唯一性属于概率保证。单独 32 位盐在 65,536 个样本下的生日碰撞概率约为 39.3%；盐匹配**并不**代表完整的 96 位 Nonce 或计数器范围发生碰撞。密钥轮转规划必须基于块范围与 sidecar GMAC 调用的总上限进行评估，而非假设盐绝不重复。

新的分块 AAD 通过域隔离机制绑定了 chunk size 与块序号。固定的 52 字节 AAD 直接在栈上构建。只有在对应的分块 Tag 校验通过后，才会向外吐出明文。损坏或截断的分块将导致读取直接失败。

`get_opts(range)` 会将请求范围对齐扩展至完整的加密块边界，并在校验通过后对明文进行裁剪。解密流从上游缓冲区中按有界批次（最多 64 KiB 或单个加密块，取较大者）复制数据；持有较小的输出切片不会锁死整块巨大的明文堆分配。上游存储后端可能仍保留其自身的缓冲区。HEAD 请求与空对象读取不会触发解密。

`get_ranges` 会统一规划所有请求范围，对重叠分块去重，并在不填补空洞的前提下合并相邻范围。单个子请求上限为 8 MiB，或在配置分块更大时限制为单个加密块。最多支持 8 路并发请求。返回的片段严格按照初始输入顺序重组，小片段数据会被复制出来以防长期占用大缓冲区。超大范围请求占用的内存必然与返回的字节数成正比。

## 分块上传 (Multipart) 生命周期

加密上传将底层的加密分块与传输网络分段 (transport parts) 相互解耦。默认物理分段大小为 8 MiB，可通过 `with_multipart_part_size` 自定义；低于 5 MiB 的值会自动向上规范化。所有非末尾的物理分段必须具备相同的大小，即便调用方提供了不同大小或未对齐的输入。这严格匹配了 [Cloudflare R2 Multipart 上传](https://developers.cloudflare.com/r2/objects/upload-objects/#part-size-limits) 所要求的等长分段规范。在发出完整的物理分段后，不足一段的残余尾部会被复制到独立的缓冲区中，避免继续持有庞大的密文内存块。底层后端的对象/分段最大限制依然生效。

Wrapper 会同步锁定分段序号；调用方可以并发 await 返回的 part future。所有返回的 part future 必须全部成功后方可执行 complete 提交。任何分段的失败、丢弃，或 payload 物化阶段的取消/失败，都会使该上传进入终态：必须执行 abort 并重新发起上传。被取消的尾段绝不会在后续成功的 complete 中被隐式漏掉。

一旦 payload 物化成功，元数据发布即使失败也可以重试，而无需重复调用后端的 complete。首次尝试会记录先前的提交标识。重试仅在提交点未发生变化或已指向当前上传时才会成功，同时还会核对已完成的 payload 是否依然存在且大小匹配。这防止了陈旧的重试覆盖已确认的新提交（即便清理逻辑将旧 payload 遗留在存储中）。在报告成功后重复调用 complete 将直接返回原始结果。

Abort 操作在底层后端清理完成后才将句柄标记为 aborted，因此明确的 abort 或 delete 失败支持安全重试。若 payload 物化已完成，abort 会在按 key 的写锁保护下重新读取元数据，仅当该 generation 尚未提交时才执行物理删除。因此，不确定的元数据响应绝不会导致 abort 误删已提交的对象。成功完成、成功中止及物化失败均会释放 GC 注册；完成后的句柄无需被显式 drop 即可被 GC 正确感知。对于存在失败分段的上传应执行 abort 以释放后端资源；直接 drop 仅能释放本地 GC 注册，云厂商内部未完成的分块会话仍依赖云服务商自身的生命周期规则清理，GC 不会主动枚举或终止此类云端会话。

`with_conditional_put()` 属于已废弃的空操作。Wrapper 始终在逻辑层校验先验条件；后端的条件元数据写入提供了上述跨实例仲裁能力。代码中应移除对该方法的调用。

## 元数据缓存与资源配额

两个 Builder 均保留了旧有的条目数量参数，并默认配置 64 MiB 的**预估键/值字节预算**。加权准入机制可限制大尺寸 Tag 表的内存占用。Moka 的缓存驱逐是异步执行的，且存在额外的记账与分配器开销；这并不是进程 RSS 的绝对硬上限。

- 元数据 TTL 默认为 1 小时。
- 加密元数据额外增加 20 分钟空闲超时 (idle timeout)。
- `with_meta_cache_bytes(bytes)` 重建内置的加权缓存。
- `with_meta_cache_ttl(ttl)` 调整 TTL，同时保留原始的条目与字节预算。
- `EncryptedStoreBuilder::with_meta_cache(custom)` 支持整体替换为自定义缓存，应用其自身的容量与驱逐策略。后续若调用 TTL 或字节预算方法将覆盖该自定义缓存，因此 Builder 的调用顺序具有明确含义。

解码后的加密元数据仅在通过认证、编码大小检查与结构校验后才被颁发凭证。私有凭证严格绑定确切的验证上下文（加密套件、策略、配置的旧版分块大小与配额限制）以及逻辑路径。对有效缓存值的重复读取可完全跳过全量 Tag 的扫描与重新认证。未受信任的裸外部缓存、其他 key/path 或严格模式读取器无法继承之前上下文中的凭证。

冷数据加载与提交操作受相同的按 key 互斥锁保护。Listing 操作可以复用有效的缓存元数据，但绝不会从未经协调的 listing 快照中逆向填充缓存。

`with_metadata_limits(MetadataLimits { ... })` 可配置以下默认限制：

| 配额项 | 默认值 |
| --- | --- |
| 编码后元数据字节数 | 64 MiB |
| 逻辑对象大小 | 1 TiB |
| 单对象最大加密分块数 | 4,194,304 |

配额组合生效。编码后元数据的大小可能先于分块数达到上限。系统会在流式传输过程中持续检查实际载荷大小，包括在声明长度不准确的情况下。认证的大小、分块大小与 Tag 数量之间的数学关系必须自洽。对于超大合法对象应显式放宽配额限制；过小的分块可能导致元数据体积超过 payload 本身。超出配额将直接返回错误，而非发布不可读取的损坏对象。

## 垃圾回收与元数据损坏处理

`collect_garbage()` 使用 `GarbageCollectionOptions::default()`；可配置版本支持指定逻辑前缀、并发度及与内存相关的基数预算。默认配置为：8 路并发请求、100 万条标记元数据条目上限、10 万个候选 payload。一旦超出预算，在删除任何 payload 之前立即安全终止。此时应缩小前缀范围或显式提高预算。

GC 流程严格分为标记阶段、收集候选阶段、执行清理阶段。元数据加载并发执行；候选集按 key 分组。在删除候选文件之前，每个 key 当前的元数据会在按 key 变更锁保护下重新读取并认证。这避免了为每个历史 generation 重复读取元数据。

无法识别的 generation ID、在 GC 开始时或之后生成的 generation，以及已注册的在途 payload 均会被安全跳过。对于可解码但未通过认证、或结构非法的异常文档，系统采取保守策略（与完全无法解码的元数据相同）：保留其所有的 payload。拉取失败将直接中止 GC。

覆盖写入或删除后的旧 payload 清理遵循完全相同的校验策略。显式修复操作可以替换或删除损坏的提交点，同时绝不盲目跟随不可信的 payload 引用。包含路径分隔符的伪造 generation 无法将清理操作重定向至其他 key。

严格模式下的加密 listing 会直接拒绝遗留或无法解码的元数据。兼容模式下的 listing 会跳过无法解码的文档并输出警告，但依然会坚决拒绝元数据认证失败的对象。兼容读取允许访问合法的未认证旧版对象；在系统完成迁移后应开启严格模式以关闭该回退路径。

## 请求上下文与错误处理

调用方的 Extensions 会透传至元数据 get/put 子请求，以及支持 Extensions 的 payload API（包括 copy 与 multipart 发布）。Get 结果保留 payload 响应的 Extensions。Put/complete 结果暴露元数据提交响应的 Extensions。带分隔符的 listing 保留后端 listing 响应的 Extensions。

ObjectStore 的 delete/list/get_ranges 接口不携带请求 Extensions；不要依赖它们实现每次命中元数据缓存时都必须执行的鉴权。鉴权应配置在调用方与 store 的边界处。

错误类型采用 `object_store::Error`：在被 wrapper 重映射的地方返回逻辑 NotFound、AlreadyExists、Precondition；底层后端错误与校验错误保留其详细诊断信息。后端失败并不证明未发生物理变更。非法的版本寻址在 get 时返回 `NotSupported`，在 update 时返回 `Precondition`。

## 本地文件系统组合与耐久性

```rust,no_run
use anda_object_store::{EncryptedStoreBuilder, MetaStoreBuilder};
use object_store::local::LocalFileSystem;

# fn configure() -> object_store::Result<()> {
let plain = MetaStoreBuilder::new(
    LocalFileSystem::new_with_prefix("./db")?.with_fsync(true),
    10_000,
).build();
let encrypted = EncryptedStoreBuilder::with_secret(
    LocalFileSystem::new_with_prefix("./encrypted-db")?.with_fsync(true),
    10_000,
    [7; 32], // 生产应用中请传入受管理的密钥
).with_meta_cache_bytes(64 * 1024 * 1024).build();
# let _ = (plain, encrypted);
# Ok(())
# }
```

服务入口与存储示例在本地写入时均开启了 fsync。在 `object_store 0.14.1` 中，这会在受支持的平台上对 put/copy/rename/multipart 完成操作同步写入文件及受影响的目录。这是以写延迟换取耐久性的取舍。**注意：它不会对独立的 delete 操作执行 fsync**，且目录 fsync 取决于操作系统平台。因此，无论是该选项还是 `FaultStore`，都不能证明所有操作均具备端到端的整机断电耐久性。需要独立删除耐久性的部署必须选择能提供该保证的存储后端。真实的设备/控制器断电测试与进程/future 级别的故障注入测试属于不同维度的验证。

## 故障注入与测试验证

`FaultStore` 是用于混沌测试的包装器，具备共享的 `FaultHandle` 控制器。故障预算统计所尝试的变更阶段，包括 `MultipartStart`、`MultipartPart`、`MultipartComplete` 与 `MultipartAbort`。为保证兼容性，Put 规则同时匹配 `MultipartStart`。定向规则可匹配 copy/rename 的源路径或目标路径。

故障类型包括：操作前 Error/Crash、仅针对普通 Put 的 TornWrite、ErrorAfter（后端成功但响应丢失），以及一次性门控 PauseBefore/PauseAfter。针对 multipart part 的 PauseBefore 发生在后端分配分段编号之后；激进的后端此时可能已缓冲了该分段。Complete 依然是独立的拦截发布事件。在某一请求触发模拟崩溃后，之前已准入的其他在途请求可能仍会执行完毕。

`mutation_log()` 记录所有已准入的请求（包含后端失败）。使用 `event_log()` 可细分区分 Attempted、BackendSucceeded、BackendFailed、ResponseFailed 与 Cancelled。BackendSucceeded 仅代表后端接口返回了 Ok，并不代表底层硬件已完成物理落盘。仅在请求静止空闲时才可重置控制器。

```bash
cargo test -p anda_db_utils -p anda_object_store --all-features --locked
cargo clippy -p anda_db_utils -p anda_object_store --all-targets --all-features --locked -- -D warnings
CARGO_PROFILE_BENCH_LTO=false CARGO_PROFILE_BENCH_OPT_LEVEL=3 cargo bench -p anda_object_store --bench storage
```

测试套件涵盖 InMemory/LocalFileSystem trait 一致性、旧版格式兼容、CAS/ABA 与并发检查、认证清理、未知结果与取消处理、分块上传重试、缓存信任边界、大小配额、范围查询 oracle 及请求/响应 Extensions。基准测试套件记录延迟、分配次数、分配字节数与最大单次分配。真实的云端集成与物理断电测试需结合具体生产部署环境单独验证。
