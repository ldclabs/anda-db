/**
 * # The raw Core view
 *
 * Storage rows rendered as the wire shape (Spec §53.1). This is what a KQL dot
 * path reads — `?c.name`, `?a.confidence`, `?x.facets["MnemonicState"]` — so
 * the field names here are part of the query language's surface, not an
 * internal detail.
 *
 * Two deliberate departures from the Core types, both additive:
 *
 * - `structural` — Profile structural fields have no slot in the Core types,
 *   because Core keeps its own topology in typed fields. The engine stores
 *   them, so the view exposes them under one key rather than scattering them
 *   into `attributes`, where they would be mistaken for representation-local
 *   state.
 * - `merged_into` on a Concept — the forwarding pointer left by a
 *   non-destructive merge (§11.1). A reader that cannot see it cannot tell a
 *   merged-away Concept from a live one.
 *
 * An absent member is *absent*, not `null`: the Core types omit an empty map,
 * an empty list and an unset option, and a reader that saw `{}` where the Rust
 * engine sends nothing would compare unequal across the two engines.
 */

import { formatElementId, tagOf, type ElementId } from './id.js'
import { isJsonMap, type Json, type JsonMap } from './json.js'
import type { PathStep } from './kip/ast.js'
import {
  formatSymbolRef,
  isQualified,
  type SchemaEnvironment,
} from './schema/index.js'
import type {
  ActivityRow,
  AssertionRow,
  ConceptRow,
  Element,
  Envelope,
  EvidenceRow,
  PropositionRow,
} from './store/index.js'

/** Drops the members the Core wire shape omits when they are empty. */
function present(map: Record<string, Json | undefined>): JsonMap {
  const out: JsonMap = {}
  for (const [key, value] of Object.entries(map)) {
    if (value === undefined || value === null || value === '') continue
    if (Array.isArray(value) && value.length === 0) continue
    if (isJsonMap(value) && Object.keys(value).length === 0) continue
    out[key] = value
  }
  return out
}

/** The `id`, `kind` and `_system` block every element carries. */
function envelope(id: ElementId, row: Envelope): JsonMap {
  return present({
    id: formatElementId(id),
    // Lowercase, as the wire tag: `?c.kind` answers "concept", not "Concept".
    kind: id.kind.toLowerCase(),
    space_id: row.space,
    governance: row.governance,
    retention: row.retention,
    facets: row.facets,
    _system: present({
      version: row.version,
      created_at: row.created_at,
      updated_at: row.updated_at,
      created_tx: row.created_tx,
      updated_tx: row.updated_tx,
      state: row.state,
      space_seq: row.seq,
      origin: row.origin,
    }),
    structural: row.structural,
  })
}

function concept(id: ElementId, row: ConceptRow): JsonMap {
  return {
    ...envelope(id, row),
    ...present({
      schema_ref: row.schema_ref,
      key: row.key,
      name: row.name,
      canonical_id: row.canonical_id,
      aliases: row.aliases,
      attributes: row.attributes,
      merged_into: row.merged_into,
    }),
  }
}

function proposition(id: ElementId, row: PropositionRow): JsonMap {
  return {
    ...envelope(id, row),
    ...present({
      subject: row.subject,
      predicate_ref: row.predicate_ref,
      object: row.object,
      attributes: row.attributes,
    }),
  }
}

function assertion(id: ElementId, row: AssertionRow): JsonMap {
  return {
    ...envelope(id, row),
    ...present({
      proposition_id: row.proposition_id,
      asserted_by: row.asserted_by,
      stance: row.stance,
      mode: row.mode,
      // `-1` is the sentinel for "the actor stated none", and it must not
      // reach a reader as a confidence of minus one.
      confidence: row.confidence < 0 ? undefined : row.confidence,
      asserted_at: row.asserted_at,
      valid_time: present({ from: row.valid_from, until: row.valid_until }),
      evidence_refs: row.evidence_refs as unknown as Json,
      context_refs: row.context_refs,
      lifecycle: present({
        status: row.status,
        supersedes: row.supersedes,
        superseded_by: row.superseded_by,
        retracted_at: row.retracted_at,
      }),
    }),
  }
}

function evidence(id: ElementId, row: EvidenceRow): JsonMap {
  return {
    ...envelope(id, row),
    ...present({
      evidence_class: row.evidence_class,
      payload: present({
        mode: row.payload_mode,
        inline: row.payload_inline,
        content_ref: row.content_ref,
      }),
      content_digest: row.content_digest,
      media_type: row.media_type,
      observed_at: row.observed_at,
      source_refs: row.source_refs,
      generated_by: row.generated_by,
      lifecycle: present({
        status: row.status,
        corrects: row.corrects,
        corrected_by: row.corrected_by,
      }),
    }),
  }
}

function activity(id: ElementId, row: ActivityRow): JsonMap {
  return {
    ...envelope(id, row),
    ...present({
      activity_class: row.activity_class,
      started_at: row.started_at,
      ended_at: row.ended_at,
      inputs: row.inputs,
      outputs: row.outputs,
      associated_actors: row.associated_actors,
      parameters_digest: row.parameters_digest,
      status: row.status,
    }),
  }
}

/** Renders an element in the raw Core view. */
export function render(element: Element): JsonMap {
  const id: ElementId = { kind: element.kind, seq: element.row.id }
  switch (element.kind) {
    case 'Concept':
      return concept(id, element.row)
    case 'Proposition':
      return proposition(id, element.row)
    case 'Assertion':
      return assertion(id, element.row)
    case 'Evidence':
      return evidence(id, element.row)
    case 'Activity':
      return activity(id, element.row)
  }
}

/**
 * Reads one dot path out of a rendered view, resolving a Facet's local name.
 *
 * A Facet is stored under its exact symbol —
 * `kip://profiles/cognitive-memory@2.0.0/MnemonicState` — because a persisted
 * reference must name one version forever (§21). A command writes the local
 * name the environment resolves: `?m.facets["MnemonicState"].salience`. That
 * resolution belongs here, on the read, rather than in a second copy of the
 * Facet map under a name that would go stale the moment the Space activates a
 * different package version.
 *
 * A name the environment cannot resolve is left alone, so it reads as `null`
 * like any other missing member instead of failing a whole query.
 */
export function readPath(
  env: SchemaEnvironment,
  view: Json,
  path: readonly PathStep[],
): Json {
  let current: Json = view
  let inSymbolMap = false

  for (const step of path) {
    if (!isJsonMap(current)) return null
    const raw = 'Field' in step ? step.Field : step.Key
    const key = inSymbolMap ? resolveMember(env, current, raw) : raw
    current = (current[key] ?? null) as Json
    // The two maps whose keys are exact schema symbols. Everything else is
    // read verbatim: an attribute called `MnemonicState` is not a Facet.
    inSymbolMap = raw === 'facets' || raw === 'structural'
  }
  return current
}

/** The exact symbol a local name selects inside a symbol-keyed map. */
function resolveMember(
  env: SchemaEnvironment,
  map: JsonMap,
  name: string,
): string {
  if (isQualified(name) || Object.hasOwn(map, name)) return name
  for (const kind of ['Facet', 'StructuralField'] as const) {
    try {
      const exact = formatSymbolRef(env.resolveSymbol(kind, name, 'read'))
      if (Object.hasOwn(map, exact)) return exact
    } catch {
      // Not this kind, or not resolvable here. Falling through leaves the name
      // alone, which reads as a missing member rather than failing the query.
    }
  }
  return name
}

/** The element id tag, for the `IS_KIND` filter function. */
export const kindTag = (id: ElementId): string => tagOf(id.kind)
