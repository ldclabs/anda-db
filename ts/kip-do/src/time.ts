/**
 * # Time coordinates
 *
 * KIP keeps four independent time axes apart (Spec §36): world validity,
 * observation time, assertion time and engine transaction time. Confusing any
 * two of them is a semantic bug, not a formatting one, so the engine never
 * defaults one from another — it only agrees on how a timestamp is written.
 *
 * Every timestamp is normalized on write to
 *
 * ```text
 * YYYY-MM-DDTHH:MM:SS.sssZ
 * ```
 *
 * which has one useful property: it is fixed-width UTC, so lexicographic order
 * *is* chronological order. A range query over the stored text answers "valid
 * at time T" directly, with no parsing per row and no second numeric column
 * that could drift out of step with the string beside it — which also means
 * SQLite compares it correctly with plain `<` and `>`.
 */

import { errors } from './errors.js'

/** A normalized instant: fixed-width UTC RFC 3339 with milliseconds. */
export type Timestamp = string

/** The lower bound of the timestamp ordering, for open-started ranges. */
export const TIME_MIN = ''

/**
 * An upper bound above every normalized timestamp, for open-ended ranges.
 *
 * `~` sorts above every character a normalized timestamp can contain, so an
 * absent `valid_until` compares as "still applies" without a special case in
 * every range query.
 */
export const TIME_MAX = '~'

/**
 * RFC 3339, strictly.
 *
 * `Date.parse` accepts a great deal that RFC 3339 does not — `2026-8-1`,
 * `2026/08/01`, bare `12:00`, and in some runtimes free-form English — and
 * quietly produces an instant for it. A timestamp field that accepts
 * `"tomorrow"` and stores whatever the host thought that meant is worse than
 * one that refuses, so the shape is checked before the value is.
 */
const RFC3339 =
  /^(\d{4})-(\d{2})-(\d{2})[Tt ](\d{2}):(\d{2}):(\d{2})(\.\d+)?([Zz]|[+-]\d{2}:\d{2})$/

/**
 * Normalizes an RFC 3339 timestamp to the canonical stored form.
 *
 * The input's offset is honored and then converted to UTC: an offset is a way
 * of writing an instant, not a separate instant, and keeping it would break
 * the lexicographic ordering the storage layer relies on.
 */
export function normalizeTime(value: string, field: string): Timestamp {
  if (!RFC3339.test(value)) {
    throw errors.typeMismatch(
      `\`${field}\` must be an RFC 3339 timestamp, got ${JSON.stringify(value)}`,
    )
  }
  const at = Date.parse(value)
  if (Number.isNaN(at)) {
    throw errors.typeMismatch(
      `\`${field}\` is not a real instant: ${JSON.stringify(value)}`,
    )
  }
  return formatTime(at)
}

/** Writes an instant in the canonical stored form. */
export function formatTime(at: number | Date): Timestamp {
  return new Date(at).toISOString()
}

/** The current instant, in the canonical stored form. */
export function nowTime(): Timestamp {
  return new Date().toISOString()
}

/** Reads a stored timestamp back as epoch milliseconds. */
export function parseTime(value: string): number {
  const at = Date.parse(value)
  if (Number.isNaN(at)) {
    throw errors.typeMismatch(
      `${JSON.stringify(value)} is not a timestamp`,
    )
  }
  return at
}
