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

import { errors } from '../errors.js'
import {
  formatElementId,
  parseElementId,
  tagOf,
  type ElementId,
  type ElementKind,
} from '../id.js'
import type { JsonMap } from '../json.js'
import { idSet } from '../sql.js'
import { nowTime } from '../time.js'
import {
  decodeRow,
  insertStatement,
  rowToJson,
  updateStatement,
  type SqlRow,
} from './codec.js'
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
import {
  State,
  TABLES,
  type ActivityRow,
  type AssertionRow,
  type ChangeEntry,
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

/** What a change to an element is called in the version log and the journal. */
export type ChangeOp =
  | 'create'
  | 'update'
  | 'archive'
  | 'tombstone'
  | 'merge'
  | 'quarantine'
  | 'release'
  | 'purge'
  | 'retract'
  | 'supersede'
  | 'correct'
  | 'transition'
  | 'retention'

export class Store {
  readonly sql: SqlStorage

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
    this.sql = sql
    applySchema(sql)
    this.governance = new GovernanceStore(sql)
  }

  // --- Spaces ------------------------------------------------------------

  /** The Space registry row, or `null` when the Space does not exist. */
  space(spaceId: string): SpaceRow | null {
    const row = this.sql
      .exec<SqlRow>('SELECT * FROM spaces WHERE space_id = ?', spaceId)
      .toArray()[0]
    return row ? decodeRow<SpaceRow>('spaces', row) : null
  }

  /** Every Space, in creation order. */
  spaces(): SpaceRow[] {
    return this.sql
      .exec<SqlRow>('SELECT * FROM spaces ORDER BY id')
      .toArray()
      .map((row) => decodeRow<SpaceRow>('spaces', row))
  }

  /** Registers a Space. The caller supplies every Governance column. */
  createSpace(row: Omit<SpaceRow, 'id'>): SpaceRow {
    const { sql, values } = insertStatement('spaces', row)
    this.sql.exec(sql, ...values)
    return { ...row, id: this.lastRowId() }
  }

  /**
   * Advances a Space's sequence coordinate and returns the new value.
   *
   * Every commit takes exactly one of these, whatever it touched: the sequence
   * is the Space's clock, not a per-element counter, and `CHANGES SINCE` reads
   * it as one ordered stream.
   */
  nextSeq(spaceId: string): number {
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

  /** The Space's current sequence coordinate, without advancing it. */
  currentSeq(spaceId: string): number {
    return this.space(spaceId)?.seq ?? 0
  }

  /** Overwrites a Space registry row. */
  putSpace(row: SpaceRow): void {
    const { sql, values } = updateStatement('spaces', row, row.id)
    this.sql.exec(sql, ...values)
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
    const row = this.sql
      .exec<SqlRow>(`SELECT * FROM ${table} WHERE id = ?`, id.seq)
      .toArray()[0]
    if (!row) return null
    const decoded = decodeRow<ElementRow>(table, row)
    if (decoded.state === State.PENDING) return null
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
    return this.sql
      .exec<SqlRow>(
        `SELECT t.* FROM ${table} t JOIN json_each(?) j ON t.id = j.value
           WHERE t.state <> ?`,
        idSet(seqs),
        State.PENDING,
      )
      .toArray()
      .map((row) => ({ kind, row: decodeRow<ElementRow>(table, row) }) as Element)
  }

  /** The Concept holding a Space-local logical key, if one does. */
  conceptByKey(space: string, key: string): ConceptRow | null {
    const row = this.sql
      .exec<SqlRow>(
        'SELECT * FROM concepts WHERE space = ? AND "key" = ?',
        space,
        key,
      )
      .toArray()[0]
    return row ? decodeRow<ConceptRow>('concepts', row) : null
  }

  /** The canonical Proposition for a tuple identity, if it exists. */
  propositionByTuple(tupleKey: string): PropositionRow | null {
    const row = this.sql
      .exec<SqlRow>('SELECT * FROM propositions WHERE tuple_key = ?', tupleKey)
      .toArray()[0]
    return row ? decodeRow<PropositionRow>('propositions', row) : null
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
    if (clientKey === '') return null
    const table = TABLES[kind]
    const row = this.sql
      .exec<SqlRow>(
        `SELECT * FROM ${table} WHERE space = ? AND client_key = ?`,
        space,
        clientKey,
      )
      .toArray()[0]
    if (!row) return null
    return { kind, row: decodeRow<ElementRow>(table, row) } as Element
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
  put(element: Element, op: ChangeOp, txId: string): ChangeEntry {
    const table = TABLES[element.kind]
    const { row } = element
    const { sql, values } = updateStatement(table, row, row.id)
    this.sql.exec(sql, ...values)

    const id = formatElementId({ kind: element.kind, seq: row.id })
    this.appendVersion({
      space: row.space,
      element: id,
      kind: tagOf(element.kind),
      version: row.version,
      seq: row.seq,
      tx_id: txId,
      op,
      row: rowToJson(row) as JsonMap,
    })
    this.reindexReferences(element)

    return { id, kind: element.kind, op, version: row.version }
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
    const { sql, values } = insertStatement('element_versions', row)
    this.sql.exec(sql, ...values)
  }

  /**
   * The row an element had at a Space sequence coordinate.
   *
   * The greatest version whose `seq` is at most the coordinate — `null` when
   * the element did not exist yet, which is a different answer from an element
   * that existed and was empty.
   */
  versionAt(space: string, id: ElementId, seq: number): ElementVersionRow | null {
    const row = this.sql
      .exec<SqlRow>(
        `SELECT * FROM element_versions
           WHERE space = ? AND element = ? AND seq <= ?
           ORDER BY seq DESC, version DESC LIMIT 1`,
        space,
        formatElementId(id),
        seq,
      )
      .toArray()[0]
    return row ? decodeRow<ElementVersionRow>('element_versions', row) : null
  }

  /** One element's version log, oldest first. */
  versionsOf(
    space: string,
    id: ElementId,
    fromSeq: number,
    toSeq: number,
    limit: number,
  ): ElementVersionRow[] {
    return this.sql
      .exec<SqlRow>(
        `SELECT * FROM element_versions
           WHERE space = ? AND element = ? AND seq >= ? AND seq <= ?
           ORDER BY seq, version LIMIT ?`,
        space,
        formatElementId(id),
        fromSeq,
        toSeq,
        limit,
      )
      .toArray()
      .map((row) => decodeRow<ElementVersionRow>('element_versions', row))
  }

  /** The Space's whole version log over a coordinate range, oldest first. */
  versionsInSpace(
    space: string,
    fromSeq: number,
    toSeq: number,
    limit: number,
  ): ElementVersionRow[] {
    return this.sql
      .exec<SqlRow>(
        `SELECT * FROM element_versions
           WHERE space = ? AND seq >= ? AND seq <= ?
           ORDER BY seq, id LIMIT ?`,
        space,
        fromSeq,
        toSeq,
        limit,
      )
      .toArray()
      .map((row) => decodeRow<ElementVersionRow>('element_versions', row))
  }

  /** One element as it stood at a coordinate, or `null` when it did not exist. */
  elementAt(space: string, id: ElementId, seq: number): Element | null {
    return elementAt(this.sql, space, id, seq)
  }

  /** Every element of one kind that existed in a Space at a coordinate. */
  elementsAt(space: string, kind: ElementKind, seq: number): Element[] {
    return elementsAt(this.sql, space, kind, seq)
  }

  /** Resolves `AS OF TX :tx` to the Space sequence that transaction produced. */
  seqOfTransaction(space: string, txId: string): number {
    return seqOfTransaction(this.sql, space, txId)
  }

  /** Resolves `AS OF TIME :t` to the last coordinate committed at or before it. */
  seqAtTime(space: string, at: string): number {
    return seqAtTime(this.sql, space, at)
  }

  /** The Schema Environment version that was in force at a coordinate (§144). */
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

  // --- the transaction journal -------------------------------------------

  putTransaction(row: Omit<TransactionRow, 'id'>): void {
    const { sql, values } = insertStatement('transactions', row)
    this.sql.exec(sql, ...values)
  }

  transaction(txId: string): TransactionRow | null {
    const row = this.sql
      .exec<SqlRow>('SELECT * FROM transactions WHERE tx_id = ?', txId)
      .toArray()[0]
    return row ? decodeRow<TransactionRow>('transactions', row) : null
  }

  /**
   * The transaction a caller's idempotency key already committed.
   *
   * This is what makes a lost response recoverable without writing again: the
   * caller replays the key, not the mutation (§80.4).
   */
  transactionByKey(space: string, key: string): TransactionRow | null {
    if (key === '') return null
    const row = this.sql
      .exec<SqlRow>(
        'SELECT * FROM transactions WHERE space = ? AND idempotency_key = ?',
        space,
        key,
      )
      .toArray()[0]
    return row ? decodeRow<TransactionRow>('transactions', row) : null
  }

  /** The Space's committed transactions over a coordinate range, oldest first. */
  transactionsInSpace(
    space: string,
    fromSeq: number,
    toSeq: number,
    limit: number,
  ): TransactionRow[] {
    return this.sql
      .exec<SqlRow>(
        `SELECT * FROM transactions
           WHERE space = ? AND seq >= ? AND seq <= ?
           ORDER BY seq LIMIT ?`,
        space,
        fromSeq,
        toSeq,
        limit,
      )
      .toArray()
      .map((row) => decodeRow<TransactionRow>('transactions', row))
  }

  // --- Schema Packages and Environments ----------------------------------

  installPackage(row: Omit<SchemaPackageRow, 'id'>): void {
    const { sql, values } = insertStatement('schema_packages', row)
    this.sql.exec(sql, ...values)
  }

  packageByRef(packageRef: string): SchemaPackageRow | null {
    const row = this.sql
      .exec<SqlRow>(
        'SELECT * FROM schema_packages WHERE package_ref = ?',
        packageRef,
      )
      .toArray()[0]
    return row ? decodeRow<SchemaPackageRow>('schema_packages', row) : null
  }

  packages(): SchemaPackageRow[] {
    return this.sql
      .exec<SqlRow>('SELECT * FROM schema_packages ORDER BY package_id, version')
      .toArray()
      .map((row) => decodeRow<SchemaPackageRow>('schema_packages', row))
  }

  /** Appends a Schema Environment version. Existing versions are never edited. */
  appendSchemaEnv(row: Omit<SchemaEnvRow, 'id'>): void {
    const { sql, values } = insertStatement('schema_envs', row)
    this.sql.exec(sql, ...values)
  }

  /**
   * A Space's Schema Environment at a version, or its latest when none is
   * given.
   */
  schemaEnv(space: string, version?: number): SchemaEnvRow | null {
    const row =
      version === undefined
        ? this.sql
            .exec<SqlRow>(
              `SELECT * FROM schema_envs WHERE space = ?
                 ORDER BY version DESC LIMIT 1`,
              space,
            )
            .toArray()[0]
        : this.sql
            .exec<SqlRow>(
              'SELECT * FROM schema_envs WHERE space = ? AND version = ?',
              space,
              version,
            )
            .toArray()[0]
    return row ? decodeRow<SchemaEnvRow>('schema_envs', row) : null
  }

  // --- helpers -----------------------------------------------------------

  /**
   * The row id SQLite just assigned.
   *
   * `SqlStorage` exposes it on the cursor as `lastRowId`, but only for the
   * statement that wrote it, so it is read immediately rather than carried.
   */
  private lastRowId(): number {
    const row = this.sql
      .exec<{ id: number }>('SELECT last_insert_rowid() AS id')
      .toArray()[0]
    if (!row) throw errors.internalError('no row id after an insert')
    return row.id
  }
}

/** Narrowing helpers, so a caller can assert the kind it asked for. */
export const asConcept = (element: Element): ConceptRow =>
  expect(element, 'Concept') as ConceptRow
export const asProposition = (element: Element): PropositionRow =>
  expect(element, 'Proposition') as PropositionRow
export const asAssertion = (element: Element): AssertionRow =>
  expect(element, 'Assertion') as AssertionRow
export const asEvidence = (element: Element): EvidenceRow =>
  expect(element, 'Evidence') as EvidenceRow
export const asActivity = (element: Element): ActivityRow =>
  expect(element, 'Activity') as ActivityRow

function expect(element: Element, kind: ElementKind): ElementRow {
  if (element.kind !== kind) {
    throw errors.structuralReferenceInvalid(
      `${formatElementId({ kind: element.kind, seq: element.row.id })} is a ` +
        `${element.kind} where a ${kind} was required`,
    )
  }
  return element.row
}
