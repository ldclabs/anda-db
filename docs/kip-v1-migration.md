# Migrating published KIP 1.x data

[中文版](kip-v1-migration.zh.md)

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
