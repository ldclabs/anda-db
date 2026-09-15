import { normalizeRecordRefs } from '../schema/contracts.js'
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
 * Activity     InvalidLifecycleTransition   TRANSITION … TO an Activity state
 * Proposition  ImmutableField               the tuple is its identity
 * ```
 *
 * The reachable surface is judged from the element the engine **loaded**, not
 * from how the command spelled its target: `UPDATE "A-7"` looks like any other
 * update, and only the engine knows that A-7 is an Assertion.
 */

import {
  GOVERNANCE_FIELDS,
  authorizeCanonicalIdentity,
  canonicalizeReference,
  edgeIndex,
  orderedField,
  placeReference,
  positionTaken,
  singleField,
  unorderedIndex,
} from './clauses.js'
import { errors } from '../errors.js'
import { formatElementId } from '../id.js'
import { isJsonMap, jsonEquals, type Json, type JsonMap } from '../json.js'
import type {
  FacetAssignment,
  StructuralEdge,
  UpdateAction,
} from '../kip/ast.js'
import {
  facetDef,
  formatSymbolRef,
  parseSymbolRef,
  validateAttributeMutability,
  validateAttributes,
  validateFacet,
  validateFacetCarrier,
  validateFacetMutability,
  type EndpointFacts,
} from '../schema/index.js'
import type { Element } from '../store/index.js'
import { endpointFromJson, endpointKey } from '../term.js'
import type { Transaction } from '../tx.js'
import { readPath } from '../view.js'
import {
  assignments,
  mutationValue,
  options,
  referenceValue,
  symbolName,
  type Bindings,
} from './value.js'

/**
 * The refusal for an element whose state this action may not reach.
 *
 * Per action rather than per element, because a Facet is representation-local
 * state and none of it is truth (§18.1): decaying an Evidence's salience says
 * nothing about what was observed, and `OutcomeRecord` — the graded index the
 * consequence channel writes on Evidence — has to live somewhere an UPDATE can
 * still reach to establish an optional member. What is immutable is the
 * record's own payload, and each refusal names the ritual that *is* legal so
 * an agent reading it knows what to do instead.
 */
function immutableTarget(element: Element, what: string) {
  const named = formatElementId({ kind: element.kind, seq: element.row.id })
  const kind = element.kind
  switch (kind) {
    case 'Assertion':
      // An Assertion's epistemic payload is historically immutable: a changed
      // commitment is a new Assertion plus supersession, never a rewrite.
      return errors.epistemicRevisionRequired(
        `${what} would rewrite ${named}'s epistemic payload; record a new ` +
          `Assertion and SUPERSEDE this one rather than rewriting what ` +
          `somebody committed to`,
      )
    case 'Evidence':
      return errors.evidenceCorrectionRequired(
        `${what} would rewrite what ${named} observed; wrong Evidence is ` +
          `corrected with CORRECT ... BY, never edited — the original ` +
          `observation happened`,
      )
    case 'Activity':
      return errors.invalidLifecycleTransition(
        `${what} does not reach ${named}: an Activity finalizes its fields ` +
          `and topology through TRANSITION … TO "completed" | "failed" | ` +
          `"cancelled" (§52.5)`,
      )
    default:
      return errors.immutableField(
        `${what} does not reach ${named}: ` + noAttributeBag(kind),
      )
  }
}

/**
 * Claims every path one assignment map writes, refusing a plan that specifies
 * two different final values for one of them (§53.4).
 */
function claim(
  tx: Transaction,
  element: Element,
  plane: string,
  values: JsonMap,
): void {
  const id = { kind: element.kind, seq: (element.row as { id: number }).id }
  for (const [name, value] of Object.entries(values)) {
    tx.claimAssignment(id, `${plane}.${name}`, value)
  }
}

/**
 * Applies one `UPDATE` action to a staged element.
 *
 * Returns whether anything actually changed: a clause computing the state an
 * element is already in must not burn a version or emit a change record for a
 * transition that did not happen (§44).
 */
/**
 * Why an element kind has no author-writable attribute bag.
 *
 * §6.4 removed the universal metadata bag, and §12.2 gives a Proposition its
 * tuple and the common envelope and nothing else. A bag there would be the one
 * place §12.6's forbidden fields — confidence, asserted_by, observed_at —
 * could be written onto a truth-neutral tuple and read back as if they
 * belonged to it.
 */
function noAttributeBag(kind: string): string {
  return (
    `a ${kind} carries no author-writable attribute bag (§6.4). ` +
    `Representation-local state goes in a Facet; anything with its own ` +
    `source, confidence or validity is an Assertion`
  )
}

export function applyAction(
  tx: Transaction,
  b: Bindings,
  element: Element,
  action: UpdateAction,
  view: Json,
): void {
  const row = element.row
  // `view` is the element as the *statement* found it, rendered once by the
  // caller: every action of one UPDATE reads the same starting state (§52.4).
  // Re-rendering per action would let two clauses on the same Facet member
  // compound, so the second would silently operate on what the first just
  // wrote for reasons the author cannot see in the text.
  const read = (path: string[]): Json =>
    readPath(
      tx.env,
      view,
      path.map((step) => ({ Field: step })),
    )

  if ('SetFields' in action) {
    if (element.kind !== 'Concept') {
      throw immutableTarget(element, 'SET FIELDS')
    }
    const fields = assignments(b, action.SetFields, read)
    claim(tx, element, 'fields', fields)
    for (const [name, value] of Object.entries(fields)) {
      switch (name) {
        case 'canonical_id':
          // Setting one and clearing one are the same decision (§5.4).
          authorizeCanonicalIdentity(tx)
        // falls through
        case 'name':
          if (typeof value !== 'string') {
            throw errors.typeMismatch(`\`${name}\` must be a string`)
          }
          element.row[name] = value
          break
        case 'aliases':
          if (!Array.isArray(value) || value.some((item) => typeof item !== 'string')) {
            throw errors.typeMismatch('`aliases` must be an array of strings')
          }
          element.row.aliases = value as string[]
          break
        default:
          // §31.3: a Governance member spelled as a field is refused under its
          // own code, so an agent learns it is the control plane's to assign.
          if (GOVERNANCE_FIELDS.includes(name)) {
            throw errors.protectedGovernanceField(
              `\`${name}\` is Governance state (§31.3): it is assigned and ` +
                `enforced by the control plane, never written by cognitive ` +
                `content and never inferred from it`,
            )
          }
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
    if (element.kind !== 'Concept') {
      throw immutableTarget(element, 'SET ATTRIBUTES')
    }
    const values = assignments(b, action.SetAttributes, read)
    claim(tx, element, 'attributes', values)
    Object.assign(element.row.attributes, values)
    return
  }

  if ('UnsetAttributes' in action) {
    if (element.kind !== 'Concept') {
      throw immutableTarget(element, 'UNSET ATTRIBUTES')
    }
    for (const name of action.UnsetAttributes) {
      tx.claimRemoval({kind: element.kind, seq: element.row.id}, `attributes.${name}`)
      delete element.row.attributes[name]
    }
    return
  }

  if ('SetFacet' in action) {
    // Resolved and evaluated once. An assignment may read the element it is
    // writing — `MUL(?m.facets[…].memory_strength, 0.5)` — so evaluating the
    // map a second time is not merely wasted work, it is a second reading of
    // state the first one is about to move.
    const facet = resolveFacetText(tx, b, action.SetFacet.facet)
    const values = assignments(b, action.SetFacet.values, read)
    normalizeRecordRefs(facet, values)
    if (action.SetFacet.values.length > 0 && Object.keys(values).length === 0) return
    claim(tx, element, `facets.${facet}`, values)
    mergeFacet(
      tx,
      b,
      row.facets,
      action.SetFacet,
      values,
      carrierOf(element),
      facetMembers(view, facet),
    )
    return
  }

  if ('UnsetFacet' in action) {
    const symbol = tx.env.resolveSymbol(
      'Facet',
      symbolName(b, action.UnsetFacet.facet),
      'write',
    )
    const symbolText = formatSymbolRef(symbol)
    for (const field of action.UnsetFacet.fields) {
      tx.claimRemoval({kind: element.kind, seq: element.row.id}, `facets.${symbolText}.${field}`)
    }
    const facet = row.facets[symbolText]
    if (isJsonMap(facet)) {
      // Erasing an immutable member is rewriting it to absent (§39), judged
      // against the state the statement started from.
      const before = facetMembers(view, symbolText)
      const after = { ...before }
      for (const field of action.UnsetFacet.fields) delete after[field]
      const definition = tx.env.definitionPackage(symbol)
      const def =
        definition === undefined ? undefined : facetDef(definition, symbol.name)
      if (def !== undefined) {
        validateFacetMutability(symbolText, def, before, after).throwIfInvalid()
      }
      for (const field of action.UnsetFacet.fields) delete facet[field]
      // An emptied Facet is removed rather than left as `{}`: a Facet present
      // with no members would read as "carried, and every member unknown".
      if (Object.keys(facet).length === 0) delete row.facets[symbolText]
    }
    return
  }

  if ('SetStructural' in action) {
    if (element.kind !== 'Concept') {
      // Assertion and Evidence citations are immutable, and an Activity's
      // topology is finalized by TRANSITION.
      throw immutableTarget(element, 'SET STRUCTURAL')
    }
    for (const edge of action.SetStructural) {
      const field = resolveStructural(tx, b, edge)
      // §11.3: adding a reference now is a new write, so it points at the
      // identity that survived a merge rather than the one it retired.
      const value = canonicalizeReference(
        tx,
        referenceValue(mutationValue(b, edge.value, read), field),
      )
      const current = row.structural[field]
      const items = Array.isArray(current) ? [...current] : []
      const index = edgeIndex(b, edge)
      tx.claimAssignment(
        {kind: element.kind, seq: element.row.id},
        `structural_relation.${field}.${endpointKey(endpointFromJson(value))}`,
        options(b, edge.options),
      )
      const ordered = orderedField(tx, field)
      // §17.4 forbids conflicting explicit positions *in one mutation plan*,
      // and a plan is free to spread them across clauses — so the claim is
      // tracked on the transaction rather than per clause.
      if (index !== null && !tx.claimPosition(element.row.id, field, index)
          && (items[index] === undefined || !sameReference(items[index]!, value))) {
        throw positionTaken(field, index)
      }
      // §17.5: on a single-cardinality field, `SET STRUCTURAL` *replaces*.
      // Appending and then failing the cardinality check would refuse the one
      // write the Specification says this form is for.
      if (singleField(tx, field)) {
        if (index !== null) {
          if (!ordered) throw unorderedIndex(field)
          if (index > 0) {
            throw errors.constraintViolation(
              `position ${index} is outside \`${field}\`, which holds at ` +
                `most one reference; positions are dense, and the only one ` +
                `it has is 0 (§17.4)`,
            )
          }
        }
        row.structural[field] = [value]
        continue
      }
      placeReference(items, value, index, ordered, field)
      row.structural[field] = items
    }
    return
  }

  // UnsetStructural.
  if (element.kind !== 'Concept') {
    throw immutableTarget(element, 'UNSET STRUCTURAL')
  }
  for (const removal of action.UnsetStructural) {
    const field = resolveStructural(tx, b, removal)
    const target = referenceValue(mutationValue(b, removal.value, read), field)
    const canonical = canonicalizeReference(tx, target)
    tx.claimRemoval(
      {kind: element.kind, seq: element.row.id},
      `structural_relation.${field}.${endpointKey(endpointFromJson(canonical))}`,
    )
    const current = row.structural[field]
    if (Array.isArray(current)) {
      // Ordered fields re-densify: removing the second of three leaves two,
      // not a hole where the caller's index used to point.
      row.structural[field] = current.filter(
        (value) => !sameReference(value, target),
      )
    }
  }
}

/**
 * What a Facet is being attached to, for the schema to judge (§58).
 *
 * The carrier's own type is part of it when it has one: a Facet declaring
 * `concept_types` is state about those Concepts, and a carrier whose type was
 * never supplied would make that half of the declaration unenforceable.
 */
export function carrierOf(element: Element): EndpointFacts {
  if (element.kind !== 'Concept') {
    return { kind: 'element', elementKind: element.kind }
  }
  const schemaRef = element.row.schema_ref
  return {
    kind: 'element',
    elementKind: 'Concept',
    schemaRef: schemaRef === '' ? undefined : schemaRef,
  }
}

/** The exact reference one Facet symbol slot resolves to. */
export function resolveFacetText(
  tx: Transaction,
  b: Bindings,
  facet: FacetAssignment['facet'],
): string {
  return formatSymbolRef(
    tx.env.resolveSymbol('Facet', symbolName(b, facet), 'write'),
  )
}

/** The members a rendered element carries under one Facet symbol. */
export function facetMembers(view: Json, facet: string): JsonMap {
  const facets = isJsonMap(view) ? view.facets : undefined
  const found = isJsonMap(facets) ? facets[facet] : undefined
  return isJsonMap(found) ? { ...found } : {}
}

/**
 * Whether any of these actions writes the attribute bag.
 *
 * The gate on {@link checkAttributes}: an `UPDATE` that only decays a Facet has
 * not been asked anything about the attributes, and refusing it for drift that
 * predates the statement would make an unrelated clause the place a stale
 * element finally fails.
 */
export function touchesAttributes(actions: readonly UpdateAction[]): boolean {
  return actions.some(
    (action) => 'SetAttributes' in action || 'UnsetAttributes' in action,
  )
}

/** Whether any of these actions writes the structural map. */
export function touchesStructural(actions: readonly UpdateAction[]): boolean {
  return actions.some(
    (action) => 'SetStructural' in action || 'UnsetStructural' in action,
  )
}

/**
 * Validates the attribute bag one statement's clauses left behind (§34–§39).
 *
 * Run once per element after every action, not per action: `UNSET ATTRIBUTES
 * {status} SET ATTRIBUTES {status: "adopted"}` passes through a state with no
 * `status` at all, and a required attribute is a statement about what the
 * element *is* when the statement ends, not about the order its clauses were
 * written in.
 *
 * `before` is the attribute map as the statement found it, which is what makes
 * §39 answerable: immutability constrains a transition, so establishing a value
 * and rewriting one have to be told apart.
 */
export function checkAttributes(
  tx: Transaction,
  element: Element,
  before: JsonMap,
): void {
  // No other kind has an author-writable attribute bag (§6.4), and the actions
  // that would have written one were already refused.
  if (element.kind !== 'Concept') return
  const schemaRef = element.row.schema_ref
  let symbol
  try {
    symbol = parseSymbolRef(schemaRef)
  } catch {
    return
  }
  // A type this environment cannot resolve declares nothing, so it declares no
  // contract to hold the write to. Deactivating a package stops validating its
  // elements; it does not start refusing them.
  const conceptType =
    tx.env.definitionPackage(symbol)?.definitions?.concept_types?.[symbol.name]
  if (conceptType === undefined) return
  const after = element.row.attributes
  validateAttributes(schemaRef, conceptType.attributes, after)
    .extend(
      validateAttributeMutability(
        schemaRef,
        conceptType.attributes,
        before,
        after,
      ),
    )
    .throwIfInvalid()
}

/** Merges a Facet's members rather than replacing the Facet (§59). */
function mergeFacet(
  tx: Transaction,
  b: Bindings,
  facets: JsonMap,
  assignment: FacetAssignment,
  values: JsonMap,
  carrier: EndpointFacts,
  before: JsonMap,
): void {
  const symbol = tx.env.resolveSymbol(
    'Facet',
    symbolName(b, assignment.facet),
    'write',
  )
  const text = formatSymbolRef(symbol)
  const definition = tx.env.definitionPackage(symbol)
  const def =
    definition === undefined ? undefined : facetDef(definition, symbol.name)
  if (def !== undefined) {
    // Validated against the *merged* result, not the assignment: a member that
    // is only legal beside another one is legal exactly when both are there.
    // Merged onto the statement's starting state rather than onto the row, so
    // what a clause is refused for does not depend on which clause ran first.
    const merged = { ...before, ...values }
    validateFacetCarrier(text, def, carrier)
      .extend(validateFacet(text, def, merged))
      .extend(validateFacetMutability(text, def, before, merged))
      .throwIfInvalid()
  }
  facets[text] = {
    ...(isJsonMap(facets[text]) ? (facets[text] as JsonMap) : {}),
    ...values,
  }
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
      endpointKey(endpointFromJson(left)) ===
      endpointKey(endpointFromJson(right))
    )
  } catch {
    return false
  }
}
