# Migrating published KIP 1.x data

Nexus detects the persisted v1 collection shapes before opening its v2 store.
The migration is automatic, but operators should stop the old writer, take a
consistent object-store backup, and rehearse on a copy. It replaces the old
`concepts` and `propositions` collections in place. An old binary is not a
rollback mechanism; restore the pre-migration backup to roll back.

Extraction is copied and flushed into `kip_legacy_v1`, then checkpointed.
Every remaining old collection is checked against its staged copy before
deletion. A restart handles either remaining old collection separately and
does not delete an already-created v2 collection. Source rows remain staged
after migration, for audit and recovery.

Loading waits for the host's Schema activation. The complete generated Package
artifact and exact symbol mappings are saved together before installation;
resume does not resolve the same names against a changed environment or invent
different bytes for an already-installed Package. The compatibility package is
`kip://legacy/nexus@1.1.0`.

The published v1 Properties layout uses `a` / `m`. Migration also accepts the
verbose `attributes` / `metadata` representation used by recovery exports.
`LegacyRecord` preserves source data: a Concept's original row, or an
Assertion's per-predicate source properties and legacy identity. It is
provenance, not independent Evidence or permission.

- Person, Event, Preference, Insight, Commitment and SleepTask adopt compatible
  native types. Summary, time and task fields are normalized; a previously
  running, completed or failed v1 task becomes blocked without acquiring a v2
  lease. Its original execution status and result remain in LegacyRecord; the
  native task requires explicit review rather than resuming automatically.
- Commitment `completed` maps to `fulfilled` (unlike SleepTask execution states).
  Already migrated records with untouched attributes receive a one-time guarded
  correction on open; later user edits and original LegacyRecord content remain
  unchanged, and archived records stay archived.
- Optional values incompatible with native field types remain in LegacyRecord.
  Unsupported old learning/runtime artifacts receive distinct Legacy types,
  retaining their content without receiving validated standing. Generated names
  avoid collisions with existing vocabulary.
- Tuples whose endpoints satisfy the host predicate keep it. Incompatible old
  tuples use a distinct legacy predicate; compatible tuples are not moved away
  from the vocabulary normal host queries use.
- Imported claims preserve recorded confidence and resolvable attribution.
  Unknown attribution uses the migration actor and retains the original text.
  Nothing creates authenticated ActorBindings, source trust or an observation
  that the old system did not record.
- Validity windows map to `valid_time`, and expiry maps to retention. Pinning
  and mnemonic values are preserved separately from epistemic confidence.
- A recorded retraction is reconstructed when attribution is known. Native
  supersession requires a reconstructible, acyclic revision by the same actor
  about the same Proposition. Cross-Proposition changes and ambiguous excluded
  claims are archived with their annotations; they do not reappear as current
  positive beliefs. Native transitions are applied in revision order and are
  safe to repeat before the completion marker.

Malformed identifiers and dangling references can still refuse migration.
Inspect the named source rows and repair/rehearse on a backup copy. A failed
open is not evidence that the source memory was empty. Generic `migrate::plan`
is an inventory; the target host's actual Schema and application startup must
also be exercised before production cutover.

Anda Brain's regression fixture is a complete object-store snapshot generated
using the published Nexus/KIP 0.11.0 and AndaDB 0.11.1 versions in Brain
v0.11.0's lockfile. Application-specific conversation queues, caches and
bookkeeping remain the host's responsibility. Older wide conversation fields
remain readable and can receive status-only updates without truncation; new
field writes still enforce the current structural admission limits.

## 中文说明

迁移会自动检测旧集合，但上线前应停止旧写入进程、备份对象存储，并在副本上演练。
迁移就地替换两个图谱集合，是单向操作；回滚需要原始备份。

提取结果先写入并刷新 `kip_legacy_v1`，再保存完成检查点；删除旧集合前逐行核对
暂存副本。中断后会独立处理尚存的旧集合，不会删除已经建立的 v2 集合。Schema
激活后，完整 Package artifact 和精确符号映射在安装前一起保存，重启不重新猜测映射。

公开版 v1 的关系属性使用 `a` / `m` 缩写，恢复导出中的长字段名也能读取。源数据保留
在 LegacyRecord：Concept 的原始行，或 Assertion 对应的旧谓词属性与身份。它们是
来源记录，不是独立证据、身份认证或治理授权。

常见 Brain 类型转换为兼容的原生类型；旧 Insight 描述、SleepTask 原因/动作及时间
字段会转换。旧任务的 running、completed、failed 状态在原生任务上转为 blocked，
原始执行状态和结果完整保留在 LegacyRecord；任务需要显式复核，不获得虚构租约或自动恢复执行。
Commitment 的 `completed` 映射为 `fulfilled`，与 SleepTask 的执行状态处理不同。
已迁移记录在打开时进行一次有版本保护的纠正：只修改尚未被后续属性编辑覆盖的旧映射，
保留 LegacyRecord 原文和归档状态。
不能安全采用的学习/运行类型保留为 Legacy 类型；端点不符合新约束的旧关系使用独立 Legacy 谓词。有效期、
保留期、pinned 和记忆强度各自映射，绝不把 confidence 当作记忆强度。

可确认的撤回以及同 actor、同 Proposition 的无环替代链会重建；跨命题更正或不明确的
失效记录带着原注释归档，不重新进入当前正面信念。错误标识符和悬空引用仍可能阻止
迁移，应检查错误指出的源行并在备份副本上修复。通用 `migrate::plan` 只是清点，还应
验证目标宿主的实际 Schema 和完整启动路径。旧会话中较大的历史字段可以完整读取、
重建索引和更新状态；新插入或替换的字段仍受当前结构大小限制。
