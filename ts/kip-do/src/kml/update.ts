/**
 * `UPDATE` — the mutable surface of an element, and nothing else.
 *
 * What may be rewritten is decided by what the element *is*, not by who is
 * writing (§57), and the refusals name the right way round rather than saying
 * "no":
 *
 * ```text
 * Assertion    EpistemicRevisionRequired    a new Assertion + SUPERSEDE
 * Evidence     EvidenceCorrectionRequired   CORRECT ... BY
 * Activity     InvalidLifecycleTransition   TRANSITION ACTIVITY
 * Proposition  ImmutableField               the tuple is its identity
 * ```
 *
 * The reachable surface is judged from the element the engine **loaded**, not
 * from how the command spelled its target: `UPDATE "A-7"` looks like any other
 * update, and only the engine knows that A-7 is an Assertion.
 */

import { errors } from '../errors.js'
import { formatElementId, type ElementId } from '../id.js'
import { isJsonMap, jsonEquals, type Json, type JsonMap } from '../json.js'
import type { FacetAssignment, StructuralEdge, UpdateAction } from '../kip/ast.js'
import { facetDef, formatSymbolRef, validateFacet } from '../schema/index.js'
import type { Element } from '../store/index.js'
import { endpointFromJson, endpointKey } from '../term.js'
import type { Transaction } from '../tx.js'
import { readPath } from '../view.js'
import { render } from '../view.js'
import {
  assignments,
  mutationValue,
  referenceValue,
  symbolName,
  type Bindings,
} from './value.js'

/** Refuses an element whose payload is not rewritable, naming the way round. */
export function requireUpdatable(id: ElementId, element: Element): void {
  const named = formatElementId(id)
  switch (element.kind) {
    case 'Assertion':
      // An Assertion's epistemic payload is historically immutable: a changed
      // commitment is a new Assertion plus supersession, never a rewrite.
      throw errors.epistemicRevisionRequired(
        `${named} is an Assertion; record a new Assertion and SUPERSEDE this ` +
          `one rather than rewriting what somebody committed to`,
      )
    case 'Evidence':
      throw errors.evidenceCorrectionRequired(
        `${named} is an Evidence record; wrong Evidence is corrected with ` +
          `CORRECT ... BY, never edited — the original observation happened`,
      )
    case 'Activity':
      throw errors.invalidLifecycleTransition(
        `${named} is an Activity; its fields and topology are finalized ` +
          `through TRANSITION ACTIVITY`,
      )
    default:
      return
  }
}

/**
 * Applies one `UPDATE` action to a staged element.
 *
 * Returns whether anything actually changed: a clause computing the state an
 * element is already in must not burn a version or emit a change record for a
 * transition that did not happen (§44).
 */
export function applyAction(
  tx: Transaction,
  b: Bindings,
  id: ElementId,
  element: Element,
  action: UpdateAction,
): void {
  const row = element.row
  // The target's own current values, which is all an update expression may
  // read (§52.4).
  const view = render(element)
  const read = (path: string[]): Json =>
    readPath(
      tx.env,
      view,
      path.map((step) => ({ Field: step })),
    )

  if ('SetFields' in action) {
    if (element.kind !== 'Concept') {
      throw errors.immutableField(
        `a ${element.kind} has no rewritable top-level fields`,
      )
    }
    for (const [name, value] of Object.entries(
      assignments(b, action.SetFields, read),
    )) {
      switch (name) {
        case 'name':
        case 'canonical_id':
          if (typeof value !== 'string') {
            throw errors.typeMismatch(`\`${name}\` must be a string`)
          }
          element.row[name] = value
          break
        case 'aliases':
          element.row.aliases = (Array.isArray(value) ? value : [value]).filter(
            (item): item is string => typeof item === 'string',
          )
          break
        default:
          // `key` is the immutable Space-local identity, and `_system`,
          // `governance` and `retention` are engine and control-plane state.
          throw errors.immutableField(
            `\`${name}\` is not a rewritable field of a Concept`,
          )
      }
    }
    return
  }

  if ('SetAttributes' in action) {
    if (element.kind !== 'Concept' && element.kind !== 'Proposition') {
      throw errors.immutableField(
        `a ${element.kind} carries no attributes`,
      )
    }
    Object.assign(
      element.row.attributes,
      assignments(b, action.SetAttributes, read),
    )
    return
  }

  if ('UnsetAttributes' in action) {
    if (element.kind !== 'Concept' && element.kind !== 'Proposition') {
      throw errors.immutableField(`a ${element.kind} carries no attributes`)
    }
    for (const name of action.UnsetAttributes) {
      delete element.row.attributes[name]
    }
    return
  }

  if ('SetFacet' in action) {
    mergeFacet(tx, b, row.facets, action.SetFacet, read)
    return
  }

  if ('UnsetFacet' in action) {
    const symbolText = resolveFacet(tx, b, action.UnsetFacet.facet)
    const facet = row.facets[symbolText]
    if (isJsonMap(facet)) {
      for (const field of action.UnsetFacet.fields) delete facet[field]
    }
    return
  }

  if ('SetStructural' in action) {
    if (element.kind !== 'Concept') {
      // Assertion and Evidence citations are immutable, and an Activity's
      // topology is finalized by TRANSITION.
      throw errors.immutableField(
        `a ${element.kind}'s topology is not rewritable through UPDATE`,
      )
    }
    for (const edge of action.SetStructural) {
      const field = resolveStructural(tx, b, edge)
      const value = referenceValue(mutationValue(b, edge.value, read), field)
      const current = row.structural[field]
      row.structural[field] = [
        ...(Array.isArray(current) ? current : []),
        value,
      ]
    }
    return
  }

  // UnsetStructural.
  if (element.kind !== 'Concept') {
    throw errors.immutableField(
      `a ${element.kind}'s topology is not rewritable through UPDATE`,
    )
  }
  for (const removal of action.UnsetStructural) {
    const field = resolveStructural(tx, b, removal)
    const target = referenceValue(mutationValue(b, removal.value, read), field)
    const current = row.structural[field]
    if (Array.isArray(current)) {
      // Ordered fields re-densify: removing the second of three leaves two,
      // not a hole where the caller's index used to point.
      row.structural[field] = current.filter(
        (value) => !sameReference(value, target),
      )
    }
  }
  void id
}

/** Merges a Facet's members rather than replacing the Facet (§59). */
function mergeFacet(
  tx: Transaction,
  b: Bindings,
  facets: JsonMap,
  assignment: FacetAssignment,
  read: (path: string[]) => Json,
): void {
  const symbol = tx.env.resolveSymbol(
    'Facet',
    symbolName(b, assignment.facet),
    'write',
  )
  const text = formatSymbolRef(symbol)
  const merged = {
    ...(isJsonMap(facets[text]) ? (facets[text] as JsonMap) : {}),
    ...assignments(b, assignment.values, read),
  }
  const definition = tx.env.definitionPackage(symbol)
  const def = definition === undefined ? undefined : facetDef(definition, symbol.name)
  // Validated against the *merged* result, not the assignment: a member that
  // is only legal beside another one is legal exactly when both are there.
  if (def !== undefined) validateFacet(text, def, merged).throwIfInvalid()
  facets[text] = merged
}

function resolveFacet(
  tx: Transaction,
  b: Bindings,
  facet: FacetAssignment['facet'],
): string {
  return formatSymbolRef(
    tx.env.resolveSymbol('Facet', symbolName(b, facet), 'write'),
  )
}

function resolveStructural(
  tx: Transaction,
  b: Bindings,
  edge: { field: StructuralEdge['field'] },
): string {
  return formatSymbolRef(
    tx.env.resolveSymbol('StructuralField', symbolName(b, edge.field), 'write'),
  )
}

function sameReference(left: Json, right: Json): boolean {
  if (jsonEquals(left, right)) return true
  try {
    return (
      endpointKey(endpointFromJson(left)) === endpointKey(endpointFromJson(right))
    )
  } catch {
    return false
  }
}
