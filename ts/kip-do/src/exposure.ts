/**
 * The exposure log (Spec §66.8).
 *
 * A read never reinforces memory (§2.13), yet a Brain needs to know what it
 * retrieved and used. The log keeps that signal without turning recall into a
 * write path:
 *
 * ```text
 * not cognitive state   no element, no space_seq, no Change Envelope entry
 * never evidence        never cited, never corroboration, never confidence
 * governed              read only under read_audit; an element the reader may
 *                       not discover is omitted, never counted
 * bounded               erased with its element (§60.7)
 * append-only           entries are never rewritten; the host records them
 * ```
 *
 * Mirrors `rs/anda_cognitive_nexus/src/exposure.rs`.
 */

import type { AuthContext, EffectiveAuthority } from './governance/index.js'
import { requirePermitted, spaceResource } from './governance/index.js'
import { errors } from './errors.js'
import type { JsonMap } from './json.js'
import { formatElementId, tryParseElementId } from './id.js'
import { State, type Element } from './store/index.js'
import type { Store } from './store/store.js'
import { nowTime } from './time.js'

/** The most entries one call records or returns. */
export const MAX_EXPOSURES = 1000

export type Exposure = 'retrieved' | 'used'

/** One exposure a host records; the Space, time and Principal are the engine's. */
export interface ExposureInput {
  element_id: string
  exposure: Exposure
  /** The snapshot sequence of the read that exposed the element. */
  snapshot_seq: number
  /** The decision that used it; required for `used`. */
  decision_ref?: string
  /** The recall that returned it. */
  recall_ref?: string
}

/** One entry of the log (`kip-cognitive-records.schema.json#/$defs/ExposureRecord`). */
export interface ExposureRecord {
  space_id: string
  element_id: string
  exposure: Exposure
  snapshot_seq: number
  recorded_at: string
  principal_id: string
  decision_ref?: string
  recall_ref?: string
}

/** A bounded page of the log, oldest first. */
export interface ExposureQuery {
  element_id?: string
  /** The `next_cursor` of an earlier page. */
  cursor?: string
  /** `1..=1000`; default 100. */
  limit?: number
}

interface ExposureRow {
  id: number
  space: string
  element: string
  exposure: string
  snapshot_seq: number
  recorded_at: string
  principal_id: string
  decision_ref: string
  recall_ref: string
}

export interface ExposureHost {
  store: Store
  authority: EffectiveAuthority
  auth: AuthContext
}

function readable(host: ExposureHost, space: string, ref: string): Element {
  const unavailable = () => errors.notFoundOrNotVisible(`${ref} is unavailable`)
  const id = tryParseElementId(ref)
  const element = id === null ? null : host.store.load(id)
  if (!element || element.row.space !== space ||
    element.row.state === State.PURGED || element.row.state === State.PENDING) {
    throw unavailable()
  }
  if (!host.authority.mayRead(element, host.auth)?.content) throw unavailable()
  return element
}

/** Appends entries; they take no Space sequence and change no element. */
export function recordExposures(host: ExposureHost, space: string, entries: ExposureInput[]): JsonMap {
  if (!Array.isArray(entries) || entries.length === 0 || entries.length > MAX_EXPOSURES) {
    throw errors.constraintViolation(`record 1..=${MAX_EXPOSURES} exposure entries at a time`)
  }
  const current = host.store.space(space)?.seq ?? 0
  const recordedAt = nowTime()
  const rows: Omit<ExposureRow, 'id'>[] = []
  for (const entry of entries) {
    if (entry.exposure !== 'retrieved' && entry.exposure !== 'used') {
      throw errors.constraintViolation('exposure is retrieved or used')
    }
    const element = readable(host, space, entry.element_id)
    if (!Number.isSafeInteger(entry.snapshot_seq) || entry.snapshot_seq < 0 || entry.snapshot_seq > current) {
      throw errors.constraintViolation(`snapshot_seq ${entry.snapshot_seq} is ahead of the Space at ${current}`)
    }
    let decision = ''
    if (entry.decision_ref !== undefined) {
      if (readable(host, space, entry.decision_ref).kind !== 'Activity') {
        throw errors.constraintViolation('decision_ref names the Activity that recorded the decision')
      }
      decision = entry.decision_ref
    } else if (entry.exposure === 'used') {
      throw errors.constraintViolation('a used exposure names the decision that used the element')
    }
    const recall = entry.recall_ref ?? ''
    if ([...recall].length > 1024) throw errors.constraintViolation('recall_ref is at most 1024 characters')
    rows.push({
      space,
      element: formatElementId({ kind: element.kind, seq: element.row.id }),
      exposure: entry.exposure,
      snapshot_seq: entry.snapshot_seq,
      recorded_at: recordedAt,
      principal_id: host.auth.principal_id,
      decision_ref: decision,
      recall_ref: recall,
    })
  }
  for (const row of rows) {
    host.store.sql.exec(
      'INSERT INTO kip_exposures (space, element, exposure, snapshot_seq, recorded_at, principal_id, decision_ref, recall_ref) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
      row.space, row.element, row.exposure, row.snapshot_seq, row.recorded_at, row.principal_id, row.decision_ref, row.recall_ref,
    )
  }
  return { recorded: rows.length }
}

/** Reads the log under `read_audit`, omitting elements the reader may not discover. */
export function readExposures(host: ExposureHost, space: string, query: ExposureQuery = {}): JsonMap {
  const limit = query.limit ?? 100
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > MAX_EXPOSURES) {
    throw errors.constraintViolation(`exposure page limit is 1..=${MAX_EXPOSURES}`)
  }
  let after = 0
  if (query.cursor !== undefined) {
    const match = /^exposure:(\d+)$/.exec(query.cursor)
    if (!match) throw errors.cursorInvalid('invalid exposure cursor')
    after = Number(match[1])
  }
  requirePermitted(host.authority.authorize('read_audit', spaceResource(), host.auth))
  const rows = (query.element_id === undefined
    ? host.store.sql.exec('SELECT * FROM kip_exposures WHERE space = ? AND id > ? ORDER BY id', space, after)
    : host.store.sql.exec('SELECT * FROM kip_exposures WHERE space = ? AND element = ? AND id > ? ORDER BY id', space, query.element_id, after)
  ).toArray() as unknown as ExposureRow[]
  const records: ExposureRecord[] = []
  let last: number | null = null
  let more = false
  for (const row of rows) {
    if (records.length === limit) { more = true; break }
    last = row.id
    const id = tryParseElementId(row.element)
    const element = id === null ? null : host.store.load(id)
    if (!element || element.row.state === State.PURGED || !host.authority.mayRead(element, host.auth)) continue
    records.push({
      space_id: row.space,
      element_id: row.element,
      exposure: row.exposure === 'used' ? 'used' : 'retrieved',
      snapshot_seq: row.snapshot_seq,
      recorded_at: row.recorded_at,
      principal_id: row.principal_id,
      ...(row.decision_ref ? { decision_ref: row.decision_ref } : {}),
      ...(row.recall_ref ? { recall_ref: row.recall_ref } : {}),
    })
  }
  return {
    records: records as unknown as JsonMap[],
    next_cursor: more && last !== null ? `exposure:${last}` : null,
  }
}

/** Removes an erased element's entries (§60.7). */
export function removeExposures(store: Store, space: string, element: string): void {
  store.sql.exec('DELETE FROM kip_exposures WHERE space = ? AND element = ?', space, element)
}

/** How many entries an element still has, for erasure verification. */
export function exposureCount(store: Store, space: string, element: string): number {
  return Number(
    (store.sql.exec('SELECT COUNT(*) AS n FROM kip_exposures WHERE space = ? AND element = ?', space, element).one() as { n: number }).n,
  )
}
