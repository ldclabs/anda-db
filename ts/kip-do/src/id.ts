/**
 * # Element identity
 *
 * Every durable Cognitive Element carries an immutable, Nexus-local `id` that
 * is opaque to clients and never reused (Spec §7.1). It is minted from two
 * things the engine already has: the element's Core kind and the row id SQLite
 * allocated for it.
 *
 * Encoding the kind in the id is not decoration. A KIP reference such as
 * `{"id": "A-42"}` arrives with no other type information, and every read of
 * it has to know which table to look in before it can look anywhere. The
 * alternative — a global id → table map — would be a second index to keep
 * consistent with the rows it describes, for no gain.
 *
 * ```text
 * C-<n>   Concept
 * P-<n>   Proposition
 * A-<n>   Assertion
 * E-<n>   Evidence
 * X-<n>   Activity
 * ```
 *
 * Row ids come from `INTEGER PRIMARY KEY AUTOINCREMENT`, and the
 * `AUTOINCREMENT` is load-bearing rather than stylistic: SQLite's default
 * rowid assignment reuses the largest deleted id, which would hand a
 * brand-new element every reference the deleted one had. Nothing in this
 * engine ever deletes an element row — purge leaves an identity stub — but
 * "nothing currently does" is not what a never-reused id should rest on.
 */

import { errors } from './errors.js'

/** The Core kinds an element can be (Spec §6.1). */
export type ElementKind =
  | 'Concept'
  | 'Proposition'
  | 'Assertion'
  | 'Evidence'
  | 'Activity'

/** The Nexus-local identity of one Cognitive Element. */
export interface ElementId {
  readonly kind: ElementKind
  /** The row id inside that kind's table. */
  readonly seq: number
}

const TAGS: Readonly<Record<ElementKind, string>> = {
  Concept: 'C',
  Proposition: 'P',
  Assertion: 'A',
  Evidence: 'E',
  // `A` is taken by Assertion, and Activity is the rarer term in a reference,
  // so it takes the arbitrary letter.
  Activity: 'X',
}

const KINDS: Readonly<Record<string, ElementKind>> = {
  C: 'Concept',
  P: 'Proposition',
  A: 'Assertion',
  E: 'Evidence',
  X: 'Activity',
}

/** Every Core kind, in the order the wire tags sort. */
export const ELEMENT_KINDS: readonly ElementKind[] = [
  'Concept',
  'Proposition',
  'Assertion',
  'Evidence',
  'Activity',
]

/** The single-character kind tag used by the wire form. */
export const tagOf = (kind: ElementKind): string => TAGS[kind]

/** The kind a tag denotes, or `null` if it denotes none. */
export const kindOfTag = (tag: string): ElementKind | null =>
  KINDS[tag] ?? null

export const elementId = (kind: ElementKind, seq: number): ElementId => ({
  kind,
  seq,
})

/** Formats an id in its wire form, e.g. `C-42`. */
export function formatElementId(id: ElementId): string {
  return `${TAGS[id.kind]}-${id.seq}`
}

/**
 * Parses an id from its wire form.
 *
 * The accepted spelling is exact: no leading `+`, no leading zero, no
 * surrounding space. Anything looser would let one element answer to two
 * spellings of its own id, which is how two references to the same row come to
 * compare unequal.
 */
export function parseElementId(text: string): ElementId {
  const hyphen = text.indexOf('-')
  const tag = hyphen === 1 ? text.charAt(0) : ''
  const kind = tag === '' ? null : kindOfTag(tag)
  const digits = hyphen === 1 ? text.slice(2) : ''
  if (
    kind === null ||
    digits.length === 0 ||
    !/^\d+$/.test(digits) ||
    (digits.length > 1 && digits.startsWith('0'))
  ) {
    throw errors.invalidIdentifier(
      `${JSON.stringify(text)} is not a Nexus element id; the form is a kind ` +
        `tag, a hyphen and a decimal sequence, e.g. "C-42"`,
    )
  }
  const seq = Number(digits)
  // Row ids are 64-bit in SQLite and JS numbers are f64, so a value past 2^53
  // is still *well-formed* — it just cannot identify a row this engine ever
  // created. It has to report "not found" rather than a syntax error, which is
  // what the reference engine does.
  return elementId(kind, Number.isSafeInteger(seq) ? seq : UNREACHABLE_SEQ)
}

/** A syntactically valid sequence too large to name a row this engine created. */
export const UNREACHABLE_SEQ = -1

/** Non-throwing variant, for probing values that may not be element ids. */
export function tryParseElementId(text: string): ElementId | null {
  try {
    return parseElementId(text)
  } catch {
    return null
  }
}

/**
 * Parses an id, requiring it to name the expected kind.
 *
 * A reference that resolves to the wrong kind is a structural reference error,
 * not a lookup miss: an Assertion's `proposition` pointing at an Evidence
 * record is malformed input, and reporting it as "not found" would send a
 * caller looking for a row that was never the right row (Spec §17.2).
 */
export function parseElementIdOfKind(
  text: string,
  expected: ElementKind,
): ElementId {
  const id = parseElementId(text)
  if (id.kind !== expected) {
    throw errors.structuralReferenceInvalid(
      `${text} names a ${id.kind} where a ${expected} was required`,
    )
  }
  return id
}

/**
 * Total order over element ids: by kind tag, then by sequence.
 *
 * The kinds themselves are unordered — no Core kind outranks another — so the
 * order comes from the wire tag, which is stable. Bounded sweeps and cursors
 * depend on there being *an* order, documented, not on which one it is.
 */
export function compareElementId(a: ElementId, b: ElementId): number {
  const ta = TAGS[a.kind]
  const tb = TAGS[b.kind]
  if (ta !== tb) return ta < tb ? -1 : 1
  return a.seq === b.seq ? 0 : a.seq < b.seq ? -1 : 1
}

export function elementIdEquals(a: ElementId, b: ElementId): boolean {
  return a.kind === b.kind && a.seq === b.seq
}
