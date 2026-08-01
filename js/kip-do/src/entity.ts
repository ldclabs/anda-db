/**
 * Entity identity and row shapes.
 *
 * Mirrors `rs/anda_cognitive_nexus/src/entity.rs`. The `EntityID` string form
 * is a persisted wire format — it is stored verbatim in
 * `propositions.subject` / `propositions.object` and handed to clients — so
 * the encoding here must stay byte-identical to the Rust `Display`/`FromStr`
 * pair, not merely round-trip within this package.
 */

import { invalidSyntax } from './errors.js'

/** `C:{id}` — a concept node. */
export interface ConceptID {
  readonly kind: 'concept'
  readonly id: number
}

/**
 * `P:{id}:{predicate}` — one *link* of a proposition row.
 *
 * The addressed element is the `(row, predicate)` pair, not the row: a single
 * `(subject, object)` row carries every predicate connecting that pair, and
 * each predicate has its own attributes, metadata and `_version`.
 */
export interface PropositionID {
  readonly kind: 'proposition'
  readonly id: number
  readonly predicate: string
}

export type EntityID = ConceptID | PropositionID

export const conceptID = (id: number): ConceptID => ({ kind: 'concept', id })

export const propositionID = (
  id: number,
  predicate: string,
): PropositionID => ({ kind: 'proposition', id, predicate })

/** Formats an `EntityID` in its persisted string form. */
export function formatEntityID(e: EntityID): string {
  return e.kind === 'concept' ? `C:${e.id}` : `P:${e.id}:${e.predicate}`
}

/**
 * Parses the persisted string form.
 *
 * Note the split rule: only the *first* `:` after the `P:` prefix separates
 * the id from the predicate, so predicates may legally contain `:` and
 * `P:9:a:b` round-trips as predicate `"a:b"`. Splitting on every colon would
 * silently truncate such predicates — see the Rust comment at
 * `entity.rs:377-379`.
 */
export function parseEntityID(s: string): EntityID {
  if (s.startsWith('C:')) {
    const id = parseU64(s.slice(2))
    if (id === null) throw invalidSyntax(`invalid concept id: ${s}`)
    return conceptID(id)
  }
  if (s.startsWith('P:')) {
    const rest = s.slice(2)
    const sep = rest.indexOf(':')
    if (sep <= 0) throw invalidSyntax(`invalid proposition id: ${s}`)
    const id = parseU64(rest.slice(0, sep))
    if (id === null) throw invalidSyntax(`invalid proposition id: ${s}`)
    const predicate = rest.slice(sep + 1)
    if (predicate.length === 0) {
      throw invalidSyntax(`proposition id is missing its predicate: ${s}`)
    }
    return propositionID(id, predicate)
  }
  throw invalidSyntax(
    `invalid entity id ${JSON.stringify(s)}; expected "C:<id>" or "P:<id>:<predicate>"`,
  )
}

/** Non-throwing variant, for probing values that may not be entity ids. */
export function tryParseEntityID(s: string): EntityID | null {
  try {
    return parseEntityID(s)
  } catch {
    return null
  }
}

/** Sentinel for a syntactically valid id too large to name a real document. */
export const UNREACHABLE_ID = -1

function parseU64(s: string): number | null {
  if (!/^\d+$/.test(s)) return null
  const n = Number(s)
  // Ids are u64 in Rust but SQLite INTEGER is i64 and JS numbers are f64.
  // A value past 2^53 is still *well-formed* — it just cannot identify a row
  // this engine ever created, so it must report NotFound rather than a syntax
  // error, which is what the reference engine does.
  if (!Number.isSafeInteger(n)) return UNREACHABLE_ID
  return n
}

/**
 * Total order over entity ids, matching the derived `Ord` on the Rust enum:
 * every concept sorts before every proposition, then by id, then by predicate.
 * `EXPORT` and the entity-anchored KQL cursors depend on this exact order for
 * stable pagination (`meta.rs:420-423`).
 */
export function compareEntityID(a: EntityID, b: EntityID): number {
  if (a.kind !== b.kind) return a.kind === 'concept' ? -1 : 1
  if (a.id !== b.id) return a.id < b.id ? -1 : 1
  if (a.kind === 'concept' || b.kind === 'concept') return 0
  return a.predicate < b.predicate ? -1 : a.predicate > b.predicate ? 1 : 0
}

export function entityIDEquals(a: EntityID, b: EntityID): boolean {
  return compareEntityID(a, b) === 0
}

/** JSON object with string keys, as stored in `attributes` / `metadata`. */
export type JsonMap = Record<string, unknown>

/** A concept row as persisted in the `concepts` table. */
export interface Concept {
  id: number
  type: string
  name: string
  attributes: JsonMap
  metadata: JsonMap
}

/** Per-predicate payload of a proposition link. */
export interface LinkProperties {
  attributes: JsonMap
  metadata: JsonMap
}

/**
 * A proposition row plus all of its links.
 *
 * One row per `(subject, object)` pair; `links` is keyed by predicate. This
 * is the same logical model as the Rust `Proposition { predicates, properties }`
 * pair, with the two collapsed into one map because SQL stores the predicate
 * set as child rows rather than as an indexed array field.
 */
export interface Proposition {
  id: number
  subject: EntityID
  object: EntityID
  links: Map<string, LinkProperties>
}

/**
 * The KIP `ConceptNode` JSON shape returned to clients.
 *
 * `_type` is the serde discriminator on the Rust `EntityRef` enum
 * (`#[serde(tag = "_type")]`). It is part of the wire shape, not decoration:
 * a client that receives a mixed list of nodes and links uses it to tell them
 * apart.
 */
export function conceptNode(c: Concept): JsonMap {
  const node: JsonMap = {
    _type: 'ConceptNode',
    id: formatEntityID(conceptID(c.id)),
    type: c.type,
    name: c.name,
  }
  // Rust marks both maps `skip_serializing_if = "Map::is_empty"`, so an empty
  // map is absent from the wire, not present-and-empty. Emitting `{}` here
  // would make every node compare unequal across the two engines.
  if (Object.keys(c.attributes).length > 0) node.attributes = c.attributes
  if (Object.keys(c.metadata).length > 0) node.metadata = c.metadata
  return node
}

/**
 * The KIP `PropositionLink` JSON shape for one predicate of a row.
 *
 * Returns `null` when the predicate is absent, mirroring
 * `Proposition::to_proposition_link`.
 */
export function propositionLink(p: Proposition, predicate: string): JsonMap | null {
  const props = p.links.get(predicate)
  if (!props) return null
  const link: JsonMap = {
    _type: 'PropositionLink',
    id: formatEntityID(propositionID(p.id, predicate)),
    subject: formatEntityID(p.subject),
    predicate,
    object: formatEntityID(p.object),
  }
  if (Object.keys(props.attributes).length > 0) link.attributes = props.attributes
  if (Object.keys(props.metadata).length > 0) link.metadata = props.metadata
  return link
}
