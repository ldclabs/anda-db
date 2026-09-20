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
