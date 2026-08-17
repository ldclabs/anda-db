/**
 * # Reading the Space at a past coordinate
 *
 * `AS OF SEQ 41` asks what this Brain *held* then, which is a different question
 * from `FOR TIME` — what was *true* then (§36.1). Answering it needs state the
 * current rows do not have, because a row is updated in place: version 3
 * overwrites version 2, and version 2 is gone.
 *
 * So every commit appends the complete row it wrote to `element_versions`, and a
 * historical read is "the greatest version of this element whose sequence is at
 * or before the coordinate". An element with no such version did not exist yet,
 * which is why an `AS OF` read of a Concept created later finds nothing rather
 * than finding it in a state it never had.
 *
 * ## Why this is a scan
 *
 * The indexes on the current rows describe the present. A historical pattern
 * cannot use them — `{state: "active"}` today says nothing about what was active
 * at sequence 41 — so a historical read enumerates the version log for its Space
 * and reconstructs the coordinate. It is charged against the same query budget
 * as everything else, so a historical read of an enormous Space refuses rather
 * than stalls.
 *
 * ## Why the coordinate is one number carried everywhere
 *
 * `AS OF` names one and a request envelope may carry a `snapshot_token`. Both
 * resolve to a Space sequence, and everything downstream reads that one number:
 * a coordinate that meant different things in two places would be worse than
 * none, because the answer's own `snapshot_seq` could not say which it meant.
 *
 * @see rs/anda_cognitive_nexus/src/store/history.rs
 */

import { errors } from '../errors.js'
import { formatElementId, kindOfTag, tagOf, type ElementId, type ElementKind } from '../id.js'
import type { Json } from '../json.js'
import { decodeRow, type SqlRow } from './codec.js'
import type {
  Element,
  ElementRow,
  ElementVersionRow,
  TransactionRow,
} from './rows.js'

/** A coordinate a read is bound to. */
export interface Coordinate {
  /** The Space sequence the read is pinned to. */
  seq: number
}

/**
 * The opaque token a client uses to bind a later read to this coordinate.
 *
 * Opaque by contract, not by encryption: a client that parsed it would be
 * depending on a shape this engine may change. It carries the Space so a token
 * cannot be replayed against a different one, where the same sequence means
 * something else entirely.
 */
export function snapshotToken(spaceId: string, coordinate: Coordinate): string {
  return hexEncode(`kip:snapshot:${spaceId}:${coordinate.seq}`)
}

/** Reads a token back, refusing one issued for another Space. */
export function coordinateFromToken(token: string, spaceId: string): Coordinate {
  const invalid = () =>
    errors.cursorInvalidated(
      `${JSON.stringify(token)} is not a snapshot token this engine issued for ` +
        `this Space`,
    )
  let text: string
  try {
    text = hexDecode(token)
  } catch {
    throw invalid()
  }
  if (!text.startsWith('kip:snapshot:')) throw invalid()
  const rest = text.slice('kip:snapshot:'.length)
  const at = rest.lastIndexOf(':')
  if (at < 0) throw invalid()
  const space = rest.slice(0, at)
  const seq = Number(rest.slice(at + 1))
  if (!Number.isInteger(seq) || seq < 0) throw invalid()
  if (space !== spaceId) {
    throw errors.cursorInvalidated(
      `this snapshot token was issued for Space ${JSON.stringify(space)}; a ` +
        `sequence means something different in ${JSON.stringify(spaceId)}`,
    )
  }
  return { seq }
}

function hexEncode(text: string): string {
  return [...new TextEncoder().encode(text)]
    .map((byte) => byte.toString(16).padStart(2, '0'))
    .join('')
}

function hexDecode(hex: string): string {
  if (hex.length % 2 !== 0 || !/^[0-9a-f]*$/i.test(hex)) throw new Error('not hex')
  const bytes = new Uint8Array(hex.length / 2)
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16)
  }
  return new TextDecoder().decode(bytes)
}

/**
 * Turns one stored version row back into the element it recorded.
 *
 * The stored value is the whole row as it was written, so this is a cast rather
 * than a reconstruction — which is the point of storing whole rows instead of
 * diffs: a diff chain with one missing link answers a historical question
 * wrongly instead of refusing (§175).
 */
export function elementOfVersion(row: ElementVersionRow): Element {
  const kind = kindOfTag(row.kind)
  if (kind === null) {
    throw errors.internalError(
      `a version row carries the unknown kind ${JSON.stringify(row.kind)}`,
    )
  }
  return { kind, row: row.row as unknown as ElementRow } as Element
}

/** One element as it stood at a coordinate, or `null` when it did not exist. */
export function elementAt(
  sql: SqlStorage,
  space: string,
  id: ElementId,
  seq: number,
): Element | null {
  const row = sql
    .exec<SqlRow>(
      `SELECT * FROM element_versions
         WHERE space = ? AND element = ? AND seq <= ?
         ORDER BY seq DESC, version DESC, id DESC LIMIT 1`,
      space,
      formatElementId(id),
      seq,
    )
    .toArray()[0]
  return row === undefined
    ? null
    : elementOfVersion(decodeRow<ElementVersionRow>('element_versions', row))
}

/**
 * Every element of one kind that existed in a Space at a coordinate.
 *
 * The whole log for the Space and kind is read and reduced to one version per
 * element, because "which elements existed then" cannot be answered from an
 * index over what exists now. Ordered so the last row seen per element is the
 * one in force, with the log's own row id as the final tiebreak — two writes at
 * one coordinate would otherwise resolve to whichever the scan reached last.
 */
export function elementsAt(
  sql: SqlStorage,
  space: string,
  kind: ElementKind,
  seq: number,
): Element[] {
  const latest = new Map<string, ElementVersionRow>()
  for (const raw of sql
    .exec<SqlRow>(
      `SELECT * FROM element_versions
         WHERE space = ? AND kind = ? AND seq <= ?
         ORDER BY element, seq, version, id`,
      space,
      tagOf(kind),
      seq,
    )
    .toArray()) {
    const row = decodeRow<ElementVersionRow>('element_versions', raw)
    latest.set(row.element, row)
  }
  return [...latest.values()].map(elementOfVersion)
}

/** Resolves `AS OF TX :tx` to the Space sequence that transaction produced. */
export function seqOfTransaction(
  sql: SqlStorage,
  space: string,
  txId: string,
): number {
  const row = sql
    .exec<SqlRow>('SELECT * FROM transactions WHERE tx_id = ?', txId)
    .toArray()[0]
  if (row === undefined) {
    throw errors.transactionUnknown(
      `this Nexus has no transaction ${JSON.stringify(txId)} to read as of`,
    )
  }
  const decoded = decodeRow<TransactionRow>('transactions', row)
  if (decoded.space !== space) {
    throw errors.transactionUnknown(
      `${JSON.stringify(txId)} committed in another Space, so it names no ` +
        `coordinate here`,
    )
  }
  return decoded.seq
}

/**
 * Resolves `AS OF TIME :t` to the last coordinate committed at or before it.
 *
 * Wall-clock time is not the Space's ordering, so this is a lookup in the
 * journal rather than arithmetic: the answer is the sequence of the last
 * transaction that had committed by then, and a time before the first commit is
 * coordinate 0 — an empty Space, not an error.
 */
export function seqAtTime(sql: SqlStorage, space: string, at: string): number {
  const row = sql
    .exec<{ seq: number }>(
      `SELECT MAX(seq) AS seq FROM transactions
         WHERE space = ? AND committed_at <= ?`,
      space,
      at,
    )
    .toArray()[0]
  return row?.seq ?? 0
}

/**
 * The Schema Environment version that was in force at a coordinate (§144).
 *
 * The environment a historical read resolves symbols through is the last one
 * activated at or before the coordinate — never today's. Reconstructing the past
 * under today's schema would answer a question nobody asked, and would do it
 * silently: a symbol that resolves differently now returns different elements
 * rather than an error.
 */
export function schemaVersionAt(sql: SqlStorage, space: string, seq: number): number {
  const row = sql
    .exec<{ version: number }>(
      `SELECT MAX(version) AS version FROM schema_envs
         WHERE space = ? AND seq <= ?`,
      space,
      seq,
    )
    .toArray()[0]
  return row?.version ?? 0
}

/** The JSON a snapshot answer carries. */
export function snapshotJson(
  spaceId: string,
  coordinate: Coordinate,
  schemaVersion: number,
): Json {
  return {
    space_id: spaceId,
    snapshot_seq: coordinate.seq,
    schema_environment_version: schemaVersion,
    snapshot_token: snapshotToken(spaceId, coordinate),
  }
}
