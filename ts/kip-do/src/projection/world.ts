/**
 * # World time (Spec §25.2–§25.5)
 *
 * What an Assertion's `valid_time` means at a projection instant, after
 * temporal succession. Pure computation over the eligible Assertions of one
 * slot, recomputed at every basis and never written back (§14.3): an
 * Assertion ended by its successor stays `active` and keeps answering for its
 * own time.
 *
 * Every endpoint is a closed range of possible instants `[lo, hi]`: an exact
 * instant is `[x, x]`, a time bound `[earliest, latest]`, and a missing side is
 * infinite, spelled with the range sentinels `''` (-∞) and `'~'` (+∞), which
 * sort around every canonical Timestamp.
 *
 * @see rs/anda_cognitive_nexus/src/projection/world.rs — the same algorithm
 */

import type { AssertionRow } from '../store/index.js'
import { TIME_MAX, TIME_MIN, loadTimePoint, timePointRange } from '../time.js'

/** A closed range of possible instants. */
export interface Span {
  lo: string
  hi: string
}

/** Where an effective interval lies relative to one instant (§25.5). */
export type Placement = 'inside' | 'outside' | 'indeterminate'

/** One eligible Assertion, as world time sees it. */
export interface Timed {
  /** The Proposition it is about: the candidate value. */
  proposition: string
  /** The `functional_by` partition of that value; empty otherwise. */
  partition: string
  /** The actor's equality key; empty when none was recorded. */
  actor: string
  /** The canonical context set, joined. */
  context: string
  stance: string
  /** Stated or observed, or a written `from` (§25.4 "who takes part"). */
  takesPart: boolean
  fromExact: boolean
  untilWritten: boolean
  /** The latest instant by which it claims to have begun (§25.4). */
  startKey: string
  /** The effective start and end, narrowed by succession. */
  start: Span
  end: Span
}

export function timed(row: AssertionRow, proposition: string, partition: string): Timed {
  const from = loadTimePoint(row.valid_from)
  const until = loadTimePoint(row.valid_until)
  // A missing `from` is the bound {latest: asserted_at} (§25.2).
  const start: Span = from === null
    ? { lo: TIME_MIN, hi: row.asserted_at === '' ? TIME_MAX : row.asserted_at }
    : timePointRange(from)
  const end: Span = until === null ? { lo: TIME_MAX, hi: TIME_MAX } : timePointRange(until)
  const startKey = from === null
    ? row.asserted_at
    : 'exact' in from
      ? from.exact
      : from.latest ?? row.asserted_at
  const context = [...new Set(row.context_refs.map((ref) => JSON.stringify(ref)))].sort()
  return {
    proposition,
    partition,
    actor: row.asserted_by_key,
    context: context.join('\u001f'),
    stance: row.stance,
    takesPart: row.mode === 'stated' || row.mode === 'observed' || from !== null,
    fromExact: from !== null && 'exact' in from,
    untilWritten: until !== null,
    startKey,
    start,
    end,
  }
}

/** Where the effective interval lies at `at` (§25.5). */
export function place(row: Timed, at: string): Placement {
  if (row.start.lo > at || row.end.hi <= at) return 'outside'
  if (row.start.hi <= at && row.end.lo > at) return 'inside'
  return 'indeterminate'
}

/**
 * The finite instants after `at` at which this interval's placement can
 * change, for `next_invalid_at` (§21.12).
 */
export function boundariesAfter(row: Timed, at: string): string[] {
  return [row.start.lo, row.start.hi, row.end.lo, row.end.hi].filter(
    (t) => t > at && t !== TIME_MAX,
  )
}

const max = (a: string, b: string): string => (a > b ? a : b)
const min = (a: string, b: string): string => (a < b ? a : b)

/**
 * Narrows every interval by temporal succession (§25.4).
 *
 * `slot` enables slot lines — the functional or `functional_by` case — where
 * distinct values of one slot by one actor succeed one another; proposition
 * lines, where one actor's opposite stances on one Proposition do, always
 * apply. Only the rows passed in take part, so a retracted, superseded, hidden
 * or out-of-context Assertion never changes a visible one.
 */
export function succeed(rows: Timed[], slot: boolean): void {
  const lines = new Map<string, number[]>()
  const add = (key: string, index: number): void => {
    const line = lines.get(key)
    if (line === undefined) lines.set(key, [index])
    else line.push(index)
  }
  rows.forEach((row, index) => {
    if (row.actor === '' || !row.takesPart) return
    add(`prop\u001f${row.actor}\u001f${row.context}\u001f${row.proposition}`, index)
    if (slot && row.stance === 'support') {
      add(`slot\u001f${row.actor}\u001f${row.context}\u001f${row.partition}`, index)
    }
  })
  const starts: Span[] = rows.map((r) => ({ ...r.start }))
  const ends: Span[] = rows.map((r) => ({ ...r.end }))
  for (const [key, line] of lines) {
    const isSlot = key.startsWith('slot')
    const disagree = (x: Timed, y: Timed): boolean =>
      isSlot ? x.proposition !== y.proposition : x.stance !== y.stance
    // A predecessor narrows a start that was not written exactly.
    const lineStart = new Map<number, Span>()
    for (const r of line) {
      const me = rows[r]!
      let span = me.start
      if (!me.fromExact) {
        let q: Timed | undefined
        for (const i of line) {
          const other = rows[i]!
          if (disagree(other, me) && other.startKey < me.startKey &&
              (q === undefined || other.startKey > q.startKey)) q = other
        }
        if (q !== undefined) span = { lo: max(me.start.lo, q.startKey), hi: me.startKey }
      }
      starts[r] = { lo: max(starts[r]!.lo, span.lo), hi: max(starts[r]!.hi, span.hi) }
      lineStart.set(r, span)
    }
    // A successor ends an open interval at its own effective start; tied
    // nearest successors combine bound by bound, so arrival order never
    // decides which one ended it.
    for (const r of line) {
      const me = rows[r]!
      if (me.untilWritten) continue
      const successors = line.filter((n) => disagree(rows[n]!, me) && rows[n]!.startKey > me.startKey)
      if (successors.length === 0) continue
      const nearest = successors.map((n) => rows[n]!.startKey).reduce(min)
      for (const n of successors.filter((n) => rows[n]!.startKey === nearest)) {
        const e = lineStart.get(n)!
        ends[r] = { lo: min(ends[r]!.lo, e.lo), hi: min(ends[r]!.hi, e.hi) }
      }
    }
  }
  rows.forEach((row, index) => {
    row.start = starts[index]!
    row.end = ends[index]!
  })
}
