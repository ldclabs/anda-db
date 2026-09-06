# Schema / derive 审查清单完成记录

审查基线：`c2a7077`。完成日期：2026-09-06。

公开类型的导入路径和文档的 CBOR 布局保持兼容。Schema 增加升级历史元数据；旧格式仍能读取。旧库首次升级时，核心数据库会扫描原始文档和待恢复的写入记录，恢复历史后再分配字段编号。

- [x] **1. F32 JSON 往返。** 使用实际 JSON 格式化与解析行为识别读回值，保留精确 CBOR 扩展路径；固定种子的 20 万个位模式抽样验证有限 F32 的位模式往返，并验证默认解析与 `float_roundtrip` 两种配置。
- [x] **2. 旧版字段编号复用。** 历史不明时拒绝直接分配；`SchemaHistoryRecovery` 仅在水位缺失时从完整原始数据推导水位，已有可信水位不会被外来字段抬高。集合升级扫描未登记文档以及通过 replay 校验的 mutation intent 图像，并在开放回调写入前持久化升级后的 schema。
- [x] **3. 多版本嵌套字段复用。** 持久化删除路径，拒绝后续同名复用；覆盖数组、Option、通配 Map，以及 JSON/CBOR 重载后的升级。
- [x] **4. 开放 Map 升级。** 开放 Map 与固定字段 Map 之间的变化需要显式迁移。既防止收窄后旧值不合法，也防止开放后暴露早先被删除的键。
- [x] **5. FieldEntry 校验入口。** `SchemaBuilder::add_field` 重新检查反序列化条目的名称和类型，失败不改变 builder；同时检查 schema 水位上界。
- [x] **6. Json 校验。** 拒绝 Bytes、CBOR Tag、非文本键及其嵌套形式；必填 Json 区分字段缺失与显式 JSON null。
- [x] **7. 递归类型。** 已知容器中的直接递归在编译期拒绝；新增 `try_field_type()` 和构建保护，间接递归、类型别名通过错误返回结束。保留手写 `field_type()` 的兼容路径。
- [x] **8. `_id` 属性。** 在特殊分支返回前检查整数 CBOR 键及 flatten，补充 compile-fail 样例。
- [x] **9. 保留键冲突。** 固定结构体字段禁止 `"*"` 和 `i64::MIN`，避免被识别为通配符。
- [x] **10. 借用 Map 键。** 键推断处理引用、切片及透明包装；增加 `&str` / `&[u8]` 的实际文档写入和持久化读回测试。
- [x] **11. 非有限浮点 JSON 输出。** 人类可读格式拒绝 Infinity，避免静默变成 null；CBOR 保留无穷值。
- [x] **12. 重复键。** 文档字段编号、FieldType Map 和升级历史 Map 使用共享的重复键检查；JSON 转换也拒绝重复对象键。
- [x] **13. Json 转换深度。** 内存 CBOR → JSON 转换使用与周围容器相同的深度限制；DSL 中的 Option 不额外消耗容器深度。
- [x] **14. 规范值快速路径。** Scalar、Text、Bytes、Vector 直接保留数据；规范 Vector 的 `coerce` 不再构建 CBOR 中间数组，非规范数组仍回退到既有 CBOR 强制转换规则。
- [x] **15. JSON 所有权与读取遍历。** 移动字符串、键和 JSON 子树；文档读取将裁剪、规范化和类型检查合并，随后每字段检查一次复杂度，失败不替换原文档。
- [x] **16. 分配。** 标量复杂度检查不再分配堆栈；序列容量提示采用上限；CBOR 向量转换按已知长度分配。向量读回保留在当前 release 配置下实测更快的迭代器实现。
- [x] **17. 模块拆分。** `field.rs` 成为公开 facade；声明、键、值、元数据、预算和测试分开。FieldType 使用派生 Debug，机械重复的 From 转换使用小型辅助宏。
- [x] **18. 宏解析与诊断。** DSL 只规范化一次空白；拒绝重复或错误形式的辅助属性、容器 field_type，以及改变容器形状的 serde tag/into。
- [x] **19. 测试与文档。** 加入跨路径、跨版本、失败原子性测试；启用两个子库的 doctest，README 示例参与测试，更新依赖版本、迁移说明及过时用法。

对应实现和回归：

- [运行时回归](../rs/anda_db_schema/tests/regressions.rs)、[递归类型](../rs/anda_db_schema/tests/recursive_types.rs)、[借用键往返](../rs/anda_db_schema/tests/borrowed_keys.rs)
- [升级历史与恢复](../rs/anda_db_schema/src/schema/history.rs)、[集合升级调用方](../rs/anda_db/src/collection.rs)
- [宏编译样例](../rs/anda_db_derive/tests/ui/)、[性能基准](../rs/anda_db_schema/benches/values.rs)

## 性能验证

同一台机器、同一编译器，使用工作区的 bench/release 配置，对基线和修改后的代码串行运行 Criterion。20 个样本，预热 0.4 秒，测量 1 秒。以下为中心估计；基准只测 `coerce` 或解码后的文档组装/校验，输入构造、CBOR 解码和存储 I/O 不计入，不能直接外推为整个数据库的加速倍数。

| 场景 | 基线 | 修改后 |
| --- | ---: | ---: |
| 标量 coerce | 42.37 ns | 9.74 ns |
| 标量文档组装 | 167.06 ns | 67.21 ns |
| 1536 维规范 Vector coerce | 6.30 µs | 9.55 ns |
| 1536 维向量文档组装 | 6.83 µs | 6.71 µs |
| 100 项 JSON coerce | 5.87 µs | 0.80 µs |
| 100 项 JSON 文档组装 | 4.41 µs | 1.28 µs |
| 100 项文本数组 coerce | 3.72 µs | 1.16 µs |
| 100 项文本数组文档组装 | 1.99 µs | 1.22 µs |

独立分配探针（输入构造之后）：1536 维规范 Vector 的额外分配从 12 次、累计申请 57,360 字节降为 0；100 项 JSON 从 114 次、19,142 字节降为 2 次、2,424 字节。这里统计的是申请量，不是峰值内存。

```sh
cargo bench -p anda_db_schema --bench values -- --warm-up-time 0.4 --measurement-time 1 --sample-size 20 --noplot
```

## 验证命令及兼容性

```sh
cargo check --workspace --all-features
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test -p anda_db_schema --test regressions --features serde_json/float_roundtrip finite_f32_json_roundtrip_preserves_bits
cargo fmt -p anda_db_schema -p anda_db_derive -p anda_db -- --check
```

工作区测试包含旧格式 fixture 读取、集合历史扫描、15 个 trybuild 编译样例和文档用例。fixture 生成测试仍按设计忽略，旧 fixture 没有覆盖或重写。

Rust 1.89 的两个子库检查通过。Rust 1.88 检查被当前依赖 `cbor2 1.1.4` 的 `rust-version = 1.89` 拒绝，这是原有依赖与工作区 MSRV 声明不一致；本次没有升级工作区的 MSRV 声明。

自定义存储接入方必须在排除写入的情况下，向历史恢复器提供全部未裁剪的原始文档和待恢复图像；部分扫描不能证明编号安全。核心集合自动遵守这一流程。`field_type()` 作为原有不可失败接口，在非法声明上会 panic；需要处理错误的调用方使用 `try_field_type()`，派生的 `schema()` 会传播嵌套类型构建错误。
