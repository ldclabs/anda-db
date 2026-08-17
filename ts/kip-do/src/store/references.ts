/**
 * Every local element one element points at.
 *
 * The complete set, including the reference paths no dedicated column covers —
 * Profile structural fields, an Assertion's context, an Evidence record's
 * corrections. A purge planner and a Capsule closure both need *all* of them: a
 * walker that missed one would let a destructive operation leave a dangling
 * reference behind, or let an export ship a graph with a broken edge.
 *
 * This function is also what maintains `element_refs`, the reverse index — so
 * "which Assertions cite this Evidence" is an index seek here rather than the
 * full Space scan the Rust engine has to fall back on. That is the whole reason
 * to have one enumeration instead of a list per call site: the forward walk and
 * the reverse index cannot disagree about what a reference is.
 */

import { formatElementId, tryParseElementId, type ElementId } from '../id.js'
import { isJsonMap, type Json, type JsonMap } from '../json.js'
import { endpointFromJson, endpointLocal } from '../term.js'
import type { Element } from './rows.js'

/** One outgoing reference: which field carried it, and in what position. */
export interface ElementReference {
  /**
   * The field name, as the reverse index records it.
   *
   * Structural fields are prefixed `structural:` so a Profile field can never
   * be mistaken for a Core one — `inputs` the Activity column and `inputs` a
   * profile-defined structural field are different edges.
   */
  field: string
  /** Position within the field, so an ordered field keeps its order. */
  ord: number
  to: ElementId
}

/** The reference that an endpoint JSON value resolves to, if it is local. */
function local(value: Json): ElementId | null {
  try {
    return endpointLocal(endpointFromJson(value))
  } catch {
    // A malformed endpoint is not a reference. It is reported where the record
    // is read or validated, not here — this walker is used by destructive
    // planning, and throwing would make an already-damaged element impossible
    // to clean up.
    return null
  }
}

/** The reference a bare id string names, if it names one. */
function byId(value: unknown): ElementId | null {
  return typeof value === 'string' && value.length > 0
    ? tryParseElementId(value)
    : null
}

function pushEach(
  out: ElementReference[],
  field: string,
  values: readonly Json[],
  resolve: (value: Json) => ElementId | null,
): void {
  values.forEach((value, index) => {
    const to = resolve(value)
    if (to !== null) out.push({ field, ord: index, to })
  })
}

/** The Profile structural fields, whatever the element kind. */
function structural(out: ElementReference[], map: JsonMap): void {
  for (const [symbol, refs] of Object.entries(map)) {
    if (Array.isArray(refs)) {
      pushEach(out, `structural:${symbol}`, refs, local)
    }
  }
}

/** Every outgoing reference of one element. */
export function elementReferences(element: Element): ElementReference[] {
  const out: ElementReference[] = []
  const { row } = element

  switch (element.kind) {
    case 'Concept': {
      const merged = byId(element.row.merged_into)
      if (merged !== null) out.push({ field: 'merged_into', ord: 0, to: merged })
      break
    }
    case 'Proposition': {
      pushEach(out, 'subject', [element.row.subject], local)
      pushEach(out, 'object', [element.row.object], local)
      break
    }
    case 'Assertion': {
      const proposition = byId(element.row.proposition_id)
      if (proposition !== null) {
        out.push({ field: 'proposition', ord: 0, to: proposition })
      }
      pushEach(out, 'asserted_by', [element.row.asserted_by], local)
      pushEach(
        out,
        'evidence',
        element.row.evidence_refs as unknown as Json[],
        (value) => (isJsonMap(value) ? byId(value.evidence_id) : null),
      )
      pushEach(out, 'context', element.row.context_refs, local)
      pushEach(out, 'supersedes', element.row.supersedes, byId)
      pushEach(out, 'superseded_by', element.row.superseded_by, byId)
      break
    }
    case 'Evidence': {
      const generated = byId(element.row.generated_by)
      if (generated !== null) {
        out.push({ field: 'generated_by', ord: 0, to: generated })
      }
      pushEach(out, 'source', element.row.source_refs, local)
      pushEach(out, 'corrects', element.row.corrects, byId)
      pushEach(out, 'corrected_by', element.row.corrected_by, byId)
      break
    }
    case 'Activity': {
      pushEach(out, 'inputs', element.row.inputs, local)
      pushEach(out, 'outputs', element.row.outputs, local)
      pushEach(out, 'associated_actors', element.row.associated_actors, local)
      break
    }
  }

  structural(out, row.structural)
  return out
}

/** The distinct elements one element points at. */
export function referencedIds(element: Element): string[] {
  const seen = new Set<string>()
  for (const reference of elementReferences(element)) {
    seen.add(formatElementId(reference.to))
  }
  return [...seen]
}
