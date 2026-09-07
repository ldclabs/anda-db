/**
 * The persistent home of one Cognitive Nexus.
 *
 * Storage only: this layer knows about rows, ids, Spaces, the version log and
 * the journal. It knows nothing about Schema symbols, epistemic status or
 * authorization — everything above it is written against these operations, and
 * a rule that lives here would be a rule no higher layer could see.
 *
 * Every method is synchronous, because the Durable Object SQLite API is, and
 * because that is what lets a whole transaction run inside `transactionSync`
 * and either commit or roll back as a unit.
 */

import { scopedIdempotencyKey } from '../idempotency.js'
import { errors } from '../errors.js'
import {
  compareElementId,
  ELEMENT_KINDS,
  formatElementId,
  parseElementId,
  tagOf,
  type ElementId,
  type ElementKind,
} from '../id.js'
import type { Json, JsonMap } from '../json.js'
import { idSet } from '../sql.js'
import { nowTime } from '../time.js'
import { decodeRow, rowToJson, type SqlRow } from './codec.js'
import { applySchema } from './ddl.js'
import { GovernanceStore } from './governance.js'
import {
  elementAt,
  elementsAt,
  schemaVersionAt,
  seqAtTime,
  seqOfTransaction,
} from './history.js'
import { elementReferences } from './references.js'
import { indexElement } from './search.js'
import {
  erasePayload,
  planesFromJson,
  State,
  TABLES,
  type ChangeOp,
  type ConceptRow,
  type Element,
  type ElementRow,
  type ElementVersionRow,
  type EvidenceRow,
  type PropositionRow,
  type SchemaEnvRow,
  type SchemaPackageRow,
  type SpaceRow,
  type TransactionRow,
} from './rows.js'
import { RowStore } from './table.js'

/**
 * What a change to an element is called in the version log.
 *
 * Finer than the wire `op` (§36.1): the log keeps the verb — `retract`,
 * `archive`, `classify` — because an auditor reading `HISTORY` through the
 * store wants to know *which* lifecycle move it was, while a Watch consuming
 * the change stream only needs to know that one happened. {@link wireOp}
 * folds these onto the normative seven.
 */
export type ChangeVerb =
  | 'create'
  | 'update'
  | 'archive'
  | 'tombstone'
  | 'merge'
  | 'quarantine'
  | 'release'
  | 'purge'
  | 'purge_payload'
  | 'retract'
  | 'supersede'
  | 'correct'
  | 'transition'
  | 'set_retention'
  | 'classify'
  | 'declassify'
  | 'elevate'
  | 'downgrade'
  | 'retention_expiry'
  | 'expire'

/**
 * The normative `op` one verb reports on the wire (§36.1).
 *
 * Every lifecycle verb — the TRANSITION states, quarantine and its release,
 * a retention expiry, an Assertion's own window lapsing — is `lifecycle`,
 * with the entry's `state {from, to}` saying which move it was. A Governance
 * relabel is an `update`: the element's content moved, under `governance.*`.
 */
export function wireOp(verb: ChangeVerb): ChangeOp {
  switch (verb) {
    case 'create':
      return 'create'
    case 'update':
    case 'classify':
    case 'declassify':
    case 'elevate':
    case 'downgrade':
      return 'update'
    case 'set_retention':
      return 'retention'
    case 'merge':
      return 'merge'
    case 'purge':
      return 'purge'
    case 'purge_payload':
      return 'payload_purge'
    default:
      return 'lifecycle'
  }
}

export class Store extends RowStore {
  /**
   * The Governance Control Plane's records.
   *
   * Reachable from here because they share one database and one transaction,
   * and kept in their own object because they are a different plane: no KML
   * clause resolves to anything on it, and nothing on it is an element. A
   * `store.governance.createGrant(...)` reads as the host API it is, where a
   * `store.createGrant(...)` would read as one more table.
   */
  readonly governance: GovernanceStore

  constructor(sql: SqlStorage) {
    super(sql)
    applySchema(sql)
    this.governance = new GovernanceStore(sql)
  }

  // --- Spaces ------------------------------------------------------------

  /** The Space registry row, or `null` when the Space does not exist. */
  space(spaceId: string): SpaceRow | null {
    return this.one<SpaceRow>(
      'spaces',
      'SELECT * FROM spaces WHERE space_id = ?',
      spaceId,
    )
  }

  /** Every Space, in creation order. */
  spaces(): SpaceRow[] {
    return this.all<SpaceRow>('spaces', 'SELECT * FROM spaces ORDER BY id')
  }

  /** Registers a Space. The caller supplies every Governance column. */
  createSpace(row: Omit<SpaceRow, 'id'>): SpaceRow {
    const stored = { ...row, id: this.insertRow('spaces', row) }
    this.governance.recordMutation({
      operation: 'create_space',
      at: stored.created_at,
      space_id: stored.space_id,
      resource: stored.space_id,
      record: stored as unknown as Json,
    })
    return stored
  }

  /**
   * Advances a Space's sequence coordinate and returns the new value.
   *
   * Every commit takes exactly one of these, whatever it touched: the sequence
   * is the Space's clock, not a per-element counter, and `CHANGES SINCE` reads
   * it as one ordered stream.
   */
  nextSeq(spaceId: string): number {
    if (this.currentSeq(spaceId) >= Number.MAX_SAFE_INTEGER) throw errors.resourceExhausted('Space sequence exceeds the portable numeric range')
    const row = this.sql
      .exec<{ seq: number }>(
        'UPDATE spaces SET seq = seq + 1 WHERE space_id = ? RETURNING seq',
        spaceId,
      )
      .toArray()[0]
    if (!row) {
      throw errors.notFoundOrNotVisible(`no MemorySpace ${spaceId}`)
    }
    return row.seq
  }

  /**
   * Every active element in a Space whose retention has lapsed (§19.1).
   *
   * Ordered by kind and id so a bounded sweep is repeatable: the same `limit`
   * over the same state acts on the same elements, which is what lets a host
   * run one in slices without wondering what it skipped.
   *
   * The empty string stores "no expiry" and sorts below every timestamp, so
   * the comparison excludes it rather than sweeping every element that never
   * declared one.
   */
  expiredElements(spaceId: string, now: string): ElementId[] {
    const out: ElementId[] = []
    for (const kind of ELEMENT_KINDS) {
      const rows = this.sql
        .exec<{ id: number }>(
          `SELECT id FROM ${TABLES[kind]}
             WHERE space = ? AND state = ? AND expires_at <> '' AND expires_at <= ?
             ORDER BY id`,
          spaceId,
          State.ACTIVE,
          now,
        )
        .toArray()
      for (const row of rows) out.push({ kind, seq: row.id })
    }
    return out
  }

  /**
   * Every active Assertion in a Space whose validity window has closed (§14.3).
   *
   * Ordered by id, so a bounded pass is repeatable.
   */
  lapsedAssertions(spaceId: string, now: string): ElementId[] {
    return this.sql
      .exec<{ id: number }>(
        `SELECT id FROM assertions
           WHERE space = ? AND state = ? AND status = 'active'
             AND valid_until <> '' AND valid_until <= ?
           ORDER BY id`,
        spaceId,
        State.ACTIVE,
        now,
      )
      .toArray()
      .map((row) => ({ kind: 'Assertion' as const, seq: row.id }))
  }

  /** The Space's current sequence coordinate, without advancing it. */
  currentSeq(spaceId: string): number {
    return this.space(spaceId)?.seq ?? 0
  }

  /** Overwrites a Space registry row. */
  putSpace(row: SpaceRow): void {
    this.updateRow('spaces', row)
    this.governance.recordMutation({
      operation: 'put_space',
      at: nowTime(),
      space_id: row.space_id,
      resource: row.space_id,
      record: row as unknown as Json,
    })
  }

  // --- elements ----------------------------------------------------------

  /**
   * Inserts the shell an in-flight transaction gets its id from.
   *
   * SQLite assigns the row id, and a transaction that has to resolve a forward
   * reference needs the id before it has the content — so the row goes in
   * `pending` and is filled in at commit. Nothing reads a pending element,
   * which is what makes {@link sweepPending} recovery by construction rather
   * than a heuristic: anything still pending after a crash belongs to no
   * committed transaction.
   */
  reserve(kind: ElementKind, space: string): ElementId {
    const at = nowTime()
    const table = TABLES[kind]
    this.sql.exec(
      `INSERT INTO ${table} (space, state, version, seq, created_at,
                             updated_at, created_tx, updated_tx)
         VALUES (?, ?, 0, 0, ?, ?, '', '')`,
      space,
      State.PENDING,
      at,
      at,
    )
    return { kind, seq: this.lastRowId() }
  }

  /**
   * Removes one reserved shell.
   *
   * Only a shell: the guard is what keeps this from being a delete path for
   * real elements, which this engine does not have — a purge leaves an identity
   * stub precisely so that references keep resolving.
   */
  removeShell(id: ElementId): void {
    this.sql.exec(
      `DELETE FROM ${TABLES[id.kind]} WHERE id = ? AND state = ?`,
      id.seq,
      State.PENDING,
    )
  }

  /** Deletes every element still wearing `pending`, in every Space. */
  sweepPending(): number {
    let removed = 0
    for (const table of Object.values(TABLES)) {
      const cursor = this.sql.exec(
        `DELETE FROM ${table} WHERE state = ?`,
        State.PENDING,
      )
      removed += cursor.rowsWritten
    }
    return removed
  }

  /** Loads one element, or `null` when no such row exists. */
  load(id: ElementId): Element | null {
    const table = TABLES[id.kind]
    const decoded = this.byId<ElementRow>(table, id.seq)
    if (decoded === null || decoded.state === State.PENDING) return null
    // A row written before the planes existed carries `{}`; every reader
    // wants the full counter set, so it is filled in here rather than in each.
    decoded.plane_versions = planesFromJson(decoded.plane_versions)
    return { kind: id.kind, row: decoded } as Element
  }

  /**
   * Loads several elements of one kind in a single query.
   *
   * Through `json_each` rather than `IN (?, ?, …)`: Durable Object SQLite binds
   * at most 100 parameters, and an id set is exactly the thing that outgrows
   * that without warning.
   */
  loadMany(kind: ElementKind, seqs: readonly number[]): Element[] {
    if (seqs.length === 0) return []
    const table = TABLES[kind]
    return this.all<ElementRow>(
      table,
      `SELECT t.* FROM ${table} t JOIN json_each(?) j ON t.id = j.value
         WHERE t.state <> ?`,
      idSet(seqs),
      State.PENDING,
    ).map((row) => {
      row.plane_versions = planesFromJson(row.plane_versions)
      return { kind, row } as Element
    })
  }

  /**
   * The Concept holding a Space-local logical key, if one does.
   *
   * `lineage` narrows the lookup rather than filtering its result, because
   * §7.3 scopes key uniqueness to `(space_id, lineage of schema_ref, key)`: a
   * Person and a Preference both keyed `"alice"` are two identities, not a
   * collision, while a Person written under `Person@1.0.0` and one upserted
   * after the package moved to `1.1.0` are one (§20.14) — which is what keeps
   * a package upgrade from minting a second `"alice"`.
   *
   * Without a declared type the key alone must still land on one Concept.
   * Returning the first of several would be the arbitrary winner §51 forbids
   * for names, arriving through `key` instead.
   */
  conceptByKey(
    space: string,
    lineage: string | null,
    key: string,
  ): ConceptRow | null {
    // The empty string stores "no logical key", so it must never match —
    // otherwise every keyless Concept in the Space would answer an upsert
    // meant for one of them.
    if (key === '') return null
    // The one lookup that does not go through `all()`: the duplicate guard has
    // to see the count *before* anything is decoded, or a Space holding two
    // Concepts on one key answers a corrupt JSON column with a parse error
    // instead of the message telling the caller to name the type.
    const rows =
      lineage === null
        ? this.sql
            .exec<SqlRow>(
              'SELECT * FROM concepts WHERE space = ? AND "key" = ?',
              space,
              key,
            )
            .toArray()
        : this.sql
            .exec<SqlRow>(
              'SELECT * FROM concepts WHERE space = ? AND lineage = ? AND "key" = ?',
              space,
              lineage,
              key,
            )
            .toArray()
    if (rows.length > 1) {
      throw errors.identityConflict(
        `the key ${JSON.stringify(key)} is carried by ${rows.length} Concepts ` +
          'in this Space, so it does not name one on its own; add the type — ' +
          'MATCH {type: …, key: …} — rather than letting the engine pick among them',
      )
    }
    const row = rows[0]
    return row === undefined ? null : decodeRow<ConceptRow>('concepts', row)
  }

  /** The canonical Proposition for a tuple identity, if it exists. */
  propositionByTuple(tupleKey: string): PropositionRow | null {
    return this.one<PropositionRow>(
      'propositions',
      'SELECT * FROM propositions WHERE tuple_key = ?',
      tupleKey,
    )
  }

  /**
   * The element a `CLIENT KEY` already created, if the caller is retrying.
   *
   * Scoped to the Space and the kind, because a client key is a caller's name
   * for one intended creation, not a global identity (§70).
   */
  byClientKey(
    kind: ElementKind,
    space: string,
    clientKey: string,
  ): Element | null {
    // The empty string stores "no client key", so it must never match —
    // otherwise every keyless element in the Space would answer for one
    // another. A Proposition has no client key either: its identity is its
    // tuple (§12.3), which is what `ENSURE` resolves through instead.
    if (clientKey === '' || kind === 'Proposition') return null
    const table = TABLES[kind]
    const row = this.one<ElementRow>(
      table,
      // Lowest id wins, deterministically: a database written before this
      // lookup existed may hold more than one, and a retry that resolved to
      // a different one each time would be worse than not resolving at all.
      `SELECT * FROM ${table} WHERE space = ? AND client_key = ? ORDER BY id LIMIT 1`,
      space,
      clientKey,
    )
    return row === null ? null : ({ kind, row } as Element)
  }

  /**
   * Writes an element's current row, appends its version, and re-indexes its
   * outgoing references.
   *
   * The three happen together on purpose. A row written without its version
   * entry is invisible to `AS OF`; a version entry without the row is a
   * history of something that is not there; and a reference index that lags
   * the row lets a purge conclude nothing points at an element that something
   * does.
   */
  put(element: Element, verb: ChangeVerb, txId: string): void {
    const table = TABLES[element.kind]
    const { row } = element
    this.updateRow(table, row)

    const id = formatElementId({ kind: element.kind, seq: row.id })
    this.appendVersion({
      space: row.space,
      element: id,
      kind: tagOf(element.kind),
      version: row.version,
      seq: row.seq,
      tx_id: txId,
      op: verb,
      row: rowToJson(row) as JsonMap,
    })
    this.reindexReferences(element)
    // In the same transaction as the row, which is the whole reason `SEARCH`
    // may report `index_seq` equal to `current_space_seq` (§66.5): a write that
    // rolled back rolled its index entry back with it.
    indexElement(this.sql, element)
  }

  /** Replaces the reverse-index entries for one element. */
  reindexReferences(element: Element): void {
    const id = formatElementId({ kind: element.kind, seq: element.row.id })
    this.sql.exec(
      'DELETE FROM element_refs WHERE space = ? AND from_id = ?',
      element.row.space,
      id,
    )
    for (const reference of elementReferences(element)) {
      this.sql.exec(
        `INSERT INTO element_refs (space, from_id, field, ord, to_id)
           VALUES (?, ?, ?, ?, ?)`,
        element.row.space,
        id,
        reference.field,
        reference.ord,
        formatElementId(reference.to),
      )
    }
  }

  /**
   * Every element that points at this one.
   *
   * Complete rather than best-effort: an incomplete answer would let a
   * destructive operation leave a dangling reference, which is the failure the
   * reverse index exists to prevent.
   */
  referrers(space: string, id: ElementId): { from: ElementId; field: string }[] {
    return this.sql
      .exec<{ from_id: string; field: string }>(
        `SELECT DISTINCT from_id, field FROM element_refs
           WHERE space = ? AND to_id = ? ORDER BY from_id, field`,
        space,
        formatElementId(id),
      )
      .toArray()
      .map((row) => ({ from: parseElementId(row.from_id), field: row.field }))
  }

  // --- the version log ---------------------------------------------------

  /** Appends one historical version. */
  appendVersion(row: Omit<ElementVersionRow, 'id'>): void {
    this.writeRow('element_versions', row)
  }

  /**
   * The row an element had at a Space sequence coordinate.
   *
   * The greatest version whose `seq` is at most the coordinate — `null` when
   * the element did not exist yet, which is a different answer from an element
   * that existed and was empty.
   */
  versionAt(space: string, id: ElementId, seq: number): ElementVersionRow | null {
    return this.one<ElementVersionRow>(
      'element_versions',
      `SELECT * FROM element_versions
         WHERE space = ? AND element = ? AND seq <= ?
         ORDER BY seq DESC, version DESC LIMIT 1`,
      space,
      formatElementId(id),
      seq,
    )
  }

  /** One element's version log, oldest first. */
  versionsOf(
    space: string,
    id: ElementId,
    fromSeq: number,
    toSeq: number,
    limit: number,
  ): ElementVersionRow[] {
    return this.all<ElementVersionRow>(
      'element_versions',
      `SELECT * FROM element_versions
         WHERE space = ? AND element = ? AND seq >= ? AND seq <= ?
         ORDER BY seq, version LIMIT ?`,
      space,
      formatElementId(id),
      fromSeq,
      toSeq,
      limit,
    )
  }

  /** The Space's whole version log over a coordinate range, oldest first. */
  versionsInSpace(
    space: string,
    fromSeq: number,
    toSeq: number,
    limit: number,
  ): ElementVersionRow[] {
    return this.all<ElementVersionRow>(
      'element_versions',
      `SELECT * FROM element_versions
         WHERE space = ? AND seq >= ? AND seq <= ?
         ORDER BY seq, id LIMIT ?`,
      space,
      fromSeq,
      toSeq,
      limit,
    )
  }

  /** One element as it stood at a coordinate, or `null` when it did not exist. */
  elementAt(space: string, id: ElementId, seq: number): Element | null {
    return elementAt(this.sql, space, id, seq)
  }

  /** Every element of one kind that existed in a Space at a coordinate. */
  elementsAt(space: string, kind: ElementKind, seq: number): Element[] {
    return elementsAt(this.sql, space, kind, seq)
  }

  /** The Space sequence one transaction produced (`DESCRIBE TRANSACTION`, §68). */
  seqOfTransaction(space: string, txId: string): number {
    return seqOfTransaction(this.sql, space, txId)
  }

  /**
   * The last coordinate committed at or before an instant
   * (`DESCRIBE SNAPSHOT AT TIME`, §68).
   */
  seqAtTime(space: string, at: string): number {
    return seqAtTime(this.sql, space, at)
  }

  /** The committed transaction that produced one Space coordinate, if any. */
  transactionAtSeq(space: string, seq: number): TransactionRow | null {
    return this.one<TransactionRow>(
      'transactions',
      `SELECT * FROM transactions
         WHERE space = ? AND seq = ? AND status = 'committed'
         ORDER BY id DESC LIMIT 1`,
      space,
      seq,
    )
  }

  /** The Schema Environment version that was in force at a coordinate (§20.9). */
  schemaVersionAt(space: string, seq: number): number {
    return schemaVersionAt(this.sql, space, seq)
  }

  /**
   * Destroys an element's version log.
   *
   * Purge scrubs the current row *after* this, never before: an element
   * scrubbed only in its current row stays fully readable through `AS OF`, and
   * the other order leaves a readable stub with nothing saying to look (§19.3).
   */
  purgeVersions(space: string, id: ElementId): number {
    return this.sql.exec(
      'DELETE FROM element_versions WHERE space = ? AND element = ?',
      space,
      formatElementId(id),
    ).rowsWritten
  }

  /**
   * Strips the Evidence payload out of an element's recorded versions.
   *
   * The half of a payload purge that is easy to forget and fatal to skip: a
   * payload cleared only in the current row stays fully readable through
   * `AS OF`, which would make §60.6 a promise the engine does not keep.
   *
   * Rewritten rather than deleted, unlike {@link purgeVersions}: the Evidence
   * record survives a payload purge, so its lifecycle history is not what is
   * being erased and destroying it would take more than the caller asked for.
   */
  scrubPayloadVersions(space: string, id: ElementId): number {
    const named = formatElementId(id)
    const rows = this.sql
      .exec<{ id: number; row: string }>(
        'SELECT id, row FROM element_versions WHERE space = ? AND element = ?',
        space,
        named,
      )
      .toArray()
    for (const version of rows) {
      const stored = JSON.parse(version.row) as Record<string, unknown>
      erasePayload(stored as unknown as EvidenceRow)
      this.sql.exec(
        'UPDATE element_versions SET row = ? WHERE id = ?',
        JSON.stringify(stored),
        version.id,
      )
    }
    return rows.length
  }

  /**
   * Every Activity in a Space that names one element among its `inputs`.
   *
   * The reverse of the provenance edge `Activity.inputs`, which is what
   * `LIST DEPENDENTS` walks (§63.5). An index seek on `element_refs` rather
   * than a scan, and sorted, because two engines answering the same question
   * must agree on which Activity first reached a shared dependent.
   */
  activitiesWithInput(space: string, id: ElementId): ElementId[] {
    return this.sql
      .exec<{ from_id: string }>(
        `SELECT DISTINCT from_id FROM element_refs
           WHERE space = ? AND to_id = ? AND field = 'inputs'
           ORDER BY from_id`,
        space,
        formatElementId(id),
      )
      .toArray()
      .map((row) => parseElementId(row.from_id))
      .filter((from) => from.kind === 'Activity')
      .sort(compareElementId)
  }

  // --- the transaction journal -------------------------------------------

  putTransaction(row: Omit<TransactionRow, 'id'>): void {
    this.writeRow('transactions', row)
  }

  /**
   * Journals a Governance write as a committed transaction.
   *
   * The fields a Governance write never carries — the idempotency key and the
   * three request digests — are filled in here, so a caller states only what
   * is actually its own. Mirrors `JournalEntry` in the Rust engine, where the
   * same constants live in one struct default.
   */
  putGovernanceTransaction(entry: {
    tx_id: string
    space: string
    seq: number
    snapshot_seq: number
    committed_at: string
    schema_environment_version: number
    result: Json
    changes: TransactionRow['changes']
  }): void {
    this.putTransaction({
      ...entry,
      status: 'committed',
      transaction_class: 'governance',
      idempotency_key: '',
      request_digest: '',
      semantic_plan_digest: '',
      result_digest: '',
    })
  }

  transaction(txId: string): TransactionRow | null {
    return this.one<TransactionRow>(
      'transactions',
      'SELECT * FROM transactions WHERE tx_id = ?',
      txId,
    )
  }

  /**
   * The transaction a caller's idempotency key already committed.
   *
   * This is what makes a lost response recoverable without writing again: the
   * caller replays the key, not the mutation (§80.4).
   */
  /** The retained transaction a client's key names for one Principal (§34.2). */
  transactionForKey(space: string, principalId: string, key: string): TransactionRow | null {
    return this.transactionByKey(space, scopedIdempotencyKey(principalId, key))
  }

  transactionByKey(space: string, key: string): TransactionRow | null {
    if (key === '') return null
    return this.one<TransactionRow>(
      'transactions',
      'SELECT * FROM transactions WHERE space = ? AND idempotency_key = ?',
      space,
      key,
    )
  }

  /** The Space's committed transactions over a coordinate range, oldest first. */
  transactionsInSpace(
    space: string,
    fromSeq: number,
    toSeq: number,
    limit: number,
  ): TransactionRow[] {
    // Committed only: a `no_effect` outcome is retained so an idempotent
    // resend can replay it (§34.3), but it took no sequence and is not a
    // state-changing commit, so it is not a Change Envelope (§36.1).
    return this.all<TransactionRow>(
      'transactions',
      `SELECT * FROM transactions
         WHERE space = ? AND seq >= ? AND seq <= ? AND status = 'committed'
         ORDER BY seq LIMIT ?`,
      space,
      fromSeq,
      toSeq,
      limit,
    )
  }

  // --- Schema Packages and Environments ----------------------------------

  installPackage(row: Omit<SchemaPackageRow, 'id'>): void {
    this.writeRow('schema_packages', row)
  }

  packageByRef(packageRef: string): SchemaPackageRow | null {
    return this.one<SchemaPackageRow>(
      'schema_packages',
      'SELECT * FROM schema_packages WHERE package_ref = ?',
      packageRef,
    )
  }

  packages(): SchemaPackageRow[] {
    return this.all<SchemaPackageRow>(
      'schema_packages',
      'SELECT * FROM schema_packages ORDER BY package_id, version',
    )
  }

  /** Appends a Schema Environment version. Existing versions are never edited. */
  appendSchemaEnv(row: Omit<SchemaEnvRow, 'id'>): void {
    this.writeRow('schema_envs', row)
  }

  /**
   * A Space's Schema Environment at a version, or its latest when none is
   * given.
   */
  schemaEnv(space: string, version?: number): SchemaEnvRow | null {
    return version === undefined
      ? this.one<SchemaEnvRow>(
          'schema_envs',
          `SELECT * FROM schema_envs WHERE space = ?
             ORDER BY version DESC LIMIT 1`,
          space,
        )
      : this.one<SchemaEnvRow>(
          'schema_envs',
          'SELECT * FROM schema_envs WHERE space = ? AND version = ?',
          space,
          version,
        )
  }
}

