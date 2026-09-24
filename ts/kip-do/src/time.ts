/** KIP §6.5: canonical UTC milliseconds, with no input normalization. */
import { errors } from './errors.js'
import { isTimestamp } from './timestamp-format.js'

export type Timestamp = string
/** Indexed range sentinels, not protocol timestamps. */
export const TIME_MIN = ''
export const TIME_MAX = '~'

export { isTimestamp } from './timestamp-format.js'

/** Historical API name retained; valid input is returned byte-for-byte. */
export function normalizeTime(value: unknown, field: string): Timestamp {
  if (typeof value !== 'string') throw errors.typeMismatch(
    `\`${field}\` must be a timestamp string`,
  )
  if (!isTimestamp(value)) throw errors.constraintViolation(
    `\`${field}\` must be a valid UTC timestamp in YYYY-MM-DDTHH:mm:ss.SSSZ form, got ${JSON.stringify(value)}`,
  )
  return value
}

/** Truncate sub-millisecond clock resolution, including instants before epoch. */
export function formatTime(at: number | Date): Timestamp {
  return new Date(typeof at === 'number' ? Math.floor(at) : at).toISOString()
}

export function nowTime(): Timestamp {
  return formatTime(new Date())
}

export function parseTime(value: string): number {
  return Date.parse(normalizeTime(value, 'timestamp'))
}

/**
 * One `valid_time` endpoint (§25.2, §25.5): an exact instant, or a time bound
 * for an instant known only within `[earliest, latest]`.
 *
 * Stored in the row's text column: an instant as itself, a bound as its
 * canonical JSON object. The two cannot collide — a Timestamp never starts
 * with `{` — and the encoding is the reference engine's, byte for byte.
 */
export type TimePoint =
  | { exact: Timestamp }
  | { earliest: Timestamp | null; latest: Timestamp | null }

/** Reads a written endpoint; `null` for an absent or null one. */
export function readTimePoint(value: unknown, field: string): TimePoint | null {
  if (value === undefined || value === null) return null
  if (typeof value === 'string') return { exact: normalizeTime(value, field) }
  if (typeof value !== 'object' || Array.isArray(value)) {
    throw errors.typeMismatch(`\`${field}\` must be a timestamp or a time bound`)
  }
  const bound = value as Record<string, unknown>
  const keys = Object.keys(bound)
  if (keys.length === 0 || keys.some((k) => k !== 'earliest' && k !== 'latest')) {
    throw errors.constraintViolation(
      `\`${field}\` as a time bound takes \`earliest\` and/or \`latest\` and nothing else (§25.5)`,
    )
  }
  const side = (name: string): Timestamp | null => {
    const at = bound[name]
    if (at === undefined || at === null) return null
    if (typeof at !== 'string') throw errors.typeMismatch(`\`${field}.${name}\` must be a timestamp`)
    return normalizeTime(at, `${field}.${name}`)
  }
  const earliest = side('earliest'), latest = side('latest')
  if (earliest === null && latest === null) {
    throw errors.constraintViolation(`\`${field}\` as a time bound needs \`earliest\` or \`latest\` (§25.5)`)
  }
  if (earliest !== null && latest !== null && earliest > latest) {
    throw errors.constraintViolation(`\`${field}\` requires earliest <= latest (§25.5)`)
  }
  return { earliest, latest }
}

/** The storage encoding; see {@link TimePoint}. */
export function storeTimePoint(point: TimePoint | null): string {
  if (point === null) return ''
  if ('exact' in point) return point.exact
  return JSON.stringify(timePointJson(point))
}

/** Reads a stored endpoint; `null` for an empty column. */
export function loadTimePoint(text: string): TimePoint | null {
  if (text === '') return null
  if (!text.startsWith('{')) return { exact: text }
  const value = JSON.parse(text) as { earliest?: string; latest?: string }
  return { earliest: value.earliest ?? null, latest: value.latest ?? null }
}

/** The wire form: a Timestamp string or a `{earliest, latest}` object. */
export function timePointJson(point: TimePoint): string | { earliest?: string; latest?: string } {
  if ('exact' in point) return point.exact
  return {
    ...(point.earliest === null ? {} : { earliest: point.earliest }),
    ...(point.latest === null ? {} : { latest: point.latest }),
  }
}

/** The closed range of possible instants, with the range sentinels for ±∞. */
export function timePointRange(point: TimePoint): { lo: string; hi: string } {
  if ('exact' in point) return { lo: point.exact, hi: point.exact }
  return { lo: point.earliest ?? TIME_MIN, hi: point.latest ?? TIME_MAX }
}
