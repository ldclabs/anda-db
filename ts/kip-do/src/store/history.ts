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

import { sha3_256Text } from '../digest.js'
import { detailed, errors, type CursorFamily as WireCursorFamily } from '../errors.js'
import { canonicalJson, type Json, type JsonMap } from '../json.js'
import { formatElementId, kindOfTag, tagOf, type ElementId, type ElementKind } from '../id.js'
import { decodeRow, type SqlRow } from './codec.js'
import {
  planesFromJson,
  type Element,
  type ElementRow,
  type ElementVersionRow,
  type TransactionRow,
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

/**
 * Reads a token back, refusing one issued for another Space.
 *
 * Both refusals are `CursorInvalid` with `reason: malformed` (§87.7): a token
 * for another Space is, from this Space's point of view, not a token at all,
 * and saying more would confirm the other Space exists.
 */
export function coordinateFromToken(token: string, spaceId: string): Coordinate {
  const invalid = () =>
    detailed.cursorInvalid(
      'snapshot',
      'malformed',
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
    throw detailed.cursorInvalid(
      'snapshot',
      'malformed',
      `this snapshot token was issued for Space ${JSON.stringify(space)}; a ` +
        `sequence means something different in ${JSON.stringify(spaceId)}`,
    )
  }
  return { seq }
}

/**
 * One page of a paged answer: which traversal, pinned to which coordinate, and
 * how far in.
 *
 * §44.8 makes a KQL cursor preserve **one canonical cognitive snapshot for that
 * traversal**, and §88.4 makes every cursor opaque or safely server-mapped. A
 * bare offset is neither: page two of a query re-runs against whatever the
 * Space holds by then, so a write between pages duplicates or skips rows —
 * silently, since both pages look well-formed — and a caller can type any
 * number it likes into a cursor slot.
 *
 * The family tag is §102.28: a cursor issued by `HISTORY` must not continue a
 * `FIND`, even though both count from zero. Without it the two are the same
 * integer and the engine cannot tell which traversal it is resuming.
 */
export interface PageCursor {
  family: CursorFamily
  snapshotSeq: number
  offset: number
  /**
   * The traversal this cursor continues: {@link traversalOf} the query or
   * command that issued it. A cursor handed to a different query names a page
   * of nothing (§44.8), and the token says so rather than answering with the
   * wrong query's page.
   */
  traversal: string
}

/**
 * The identity of one traversal, for the cursor it issues (§44.8, §88.4).
 *
 * The lowered command with its `cursor` and `limit` slots blanked, plus the
 * parameters those slots did not consume: the same query paged with a
 * different page size continues the same traversal, and the token that
 * continues it is not part of the identity it continues.
 */
export function traversalOf(command: unknown, request?: JsonMap, operation?: JsonMap): string {
  const consumed = new Set<string>()
  const blanked = blankPaging(JSON.parse(JSON.stringify(command ?? null)) as Json, consumed)
  const strip = (params: JsonMap | undefined): Json => {
    if (params === undefined) return null
    const out: JsonMap = {}
    for (const [name, value] of Object.entries(params)) {
      if (!consumed.has(name)) out[name] = value
    }
    return out
  }
  const identity = { command: blanked, request: strip(request), operation: strip(operation) }
  return sha3_256Text(canonicalJson(identity)).slice(0, 16)
}

function blankPaging(value: Json, consumed: Set<string>): Json {
  if (Array.isArray(value)) return value.map((item) => blankPaging(item, consumed))
  if (value === null || typeof value !== 'object') return value
  const out: JsonMap = {}
  for (const [key, child] of Object.entries(value)) {
    if (key === 'cursor' || key === 'limit') {
      const param =
        child !== null && typeof child === 'object' && !Array.isArray(child)
          ? (child as JsonMap).Param
          : undefined
      if (typeof param === 'string') consumed.add(param)
      continue
    }
    out[key] = blankPaging(child, consumed)
  }
  return out
}

/**
 * The operation families that issue page cursors (§87.7, §102.28).
 *
 * The same names `details.family` carries on a refusal, so a client reads one
 * vocabulary whichever side of the cursor it is looking at. `changes` is
 * absent: a change cursor is a Space sequence rather than a page token, and
 * is read by `meta/index.ts` on its own.
 */
export type CursorFamily = Extract<WireCursorFamily, 'kql' | 'search' | 'list' | 'history' | 'export'>

/** The opaque token a client passes back to continue. */
export function pageToken(spaceId: string, cursor: PageCursor): string {
  return hexEncode(
    `kip:cursor:${cursor.family}:${spaceId}:${cursor.traversal}:${cursor.snapshotSeq}:${cursor.offset}`,
  )
}

/**
 * Reads a page token back, refusing one this engine did not issue for this
 * Space and this operation family.
 */
export function pageCursorFromToken(
  token: string,
  spaceId: string,
  family: CursorFamily,
  traversal: string,
): PageCursor {
  const invalid = () =>
    detailed.cursorInvalid(
      family,
      'malformed',
      `${JSON.stringify(token)} is not a ${family} cursor this engine issued ` +
        `for this Space; a cursor is opaque and belongs to the traversal that ` +
        `produced it`,
    )
  let text: string
  try {
    text = hexDecode(token)
  } catch {
    throw invalid()
  }
  if (!text.startsWith('kip:cursor:')) throw invalid()
  const parts = text.slice('kip:cursor:'.length)
  const firstColon = parts.indexOf(':')
  if (firstColon < 0) throw invalid()
  if (parts.slice(0, firstColon) !== family) throw invalid()
  const rest = parts.slice(firstColon + 1)
  const lastColon = rest.lastIndexOf(':')
  if (lastColon < 0) throw invalid()
  const offset = Number(rest.slice(lastColon + 1))
  const head = rest.slice(0, lastColon)
  const seqColon = head.lastIndexOf(':')
  if (seqColon < 0) throw invalid()
  const snapshotSeq = Number(head.slice(seqColon + 1))
  const scoped = head.slice(0, seqColon)
  const traversalColon = scoped.lastIndexOf(':')
  if (traversalColon < 0) throw invalid()
  const issuedFor = scoped.slice(traversalColon + 1)
  const space = scoped.slice(0, traversalColon)
  if (space !== spaceId) throw invalid()
  if (!Number.isInteger(offset) || offset < 0) throw invalid()
  if (!Number.isInteger(snapshotSeq) || snapshotSeq < 0) throw invalid()
  // §44.8: a cursor continues the traversal that produced it. One from another
  // query would answer with that query's page, silently.
  if (issuedFor !== traversal) {
    throw errors.cursorMismatch(
      `this ${family} cursor was issued by a different query; a cursor continues ` +
        `the traversal that produced it, so restart this one from its first page`,
    )
  }
  return { family, snapshotSeq, offset, traversal: issuedFor }
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
 * wrongly instead of refusing (§2.12).
 */
export function elementOfVersion(row: ElementVersionRow): Element {
  const kind = kindOfTag(row.kind)
  if (kind === null) {
    throw errors.internalError(
      `a version row carries the unknown kind ${JSON.stringify(row.kind)}`,
    )
  }
  const stored = row.row as unknown as ElementRow
  stored.plane_versions = planesFromJson(stored.plane_versions)
  return { kind, row: stored } as Element
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
 * The Schema Environment version that was in force at a coordinate (§20.9).
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

