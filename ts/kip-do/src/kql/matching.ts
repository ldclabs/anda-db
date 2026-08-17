/**
 * Pattern matching.
 *
 * Each `WHERE` clause takes the solutions produced so far and returns the ones
 * it can extend. Clauses sharing a variable therefore join, `OPTIONAL` pads
 * rather than drops, `NOT` keeps what a pattern could *not* extend, and `UNION`
 * widens rather than filtering.
 *
 * Two rules are easy to get subtly wrong and are stated here once.
 *
 * **A pattern matches `active` elements unless it says otherwise.** That is
 * what archiving *means*: out of ordinary recall, still readable when asked for
 * by name. A reviewer writes `{state: "quarantined"}` and gets it.
 *
 * **`NOT` asks about the record, never about the world.** A Person with no
 * `prefers` Proposition is a Person nothing is recorded about; the absence is
 * not a claim that they prefer nothing (§51). The engine answers the first
 * question, and the spelling of the clause is what keeps them apart.
 */

import { errors } from '../errors.js'
import {
  formatElementId,
  parseElementId,
  tryParseElementId,
  type ElementId,
  type ElementKind,
} from '../id.js'
import { isJsonMap, type Json, type JsonMap } from '../json.js'
import type {
  BeliefTarget,
  Scalar,
  MatchValue,
  ObjectMatcher,
  PredAtom,
  PropositionMatcher,
  Term,
  WhereClause,
} from '../kip/ast.js'
import { kipValue as kipLiteral } from '../kml/value.js'
import { formatSymbolRef } from '../schema/index.js'
import { State, type PropositionRow, type SqlRow } from '../store/index.js'
import { decodeRow } from '../store/codec.js'
import type { Element, ElementRow } from '../store/index.js'
import { endpointFromJson, endpointKey } from '../term.js'
import { readPath } from '../view.js'
import {
  beliefToJson,
  project,
  slotPropositions,
  slotToJson,
  type Policy,
} from '../projection/index.js'
import { Context, LIMITS } from './context.js'
import { evaluateFilter } from './filter.js'
import {
  distinct,
  elementBinding,
  extend,
  literalBinding,
  symbolBinding,
  type Binding,
  type Solution,
} from './solution.js'

/** Where a parameter is looked up while a read runs. */
export interface ReadBindings {
  request: JsonMap
  operation: JsonMap
  /** The Epistemic Policy a BELIEF in this query projects under. */
  policy: Policy
}

export function parameterValue(b: ReadBindings, name: string): Json {
  if (Object.hasOwn(b.operation, name)) return b.operation[name] as Json
  if (Object.hasOwn(b.request, name)) return b.request[name] as Json
  throw errors.invalidRequestEnvelope(
    `the command reads :${name}, which the request does not bind`,
  )
}

/**
 * The matcher fields that name a *reference*, by element kind.
 *
 * These bind a variable as an element rather than as a Literal. Binding
 * `?a ASSERTION {proposition: ?p}` as the string `"P-1"` would never join with
 * `?p PROPOSITION (…)`, which binds an element — the two would compare unequal
 * while naming the same row.
 */
const REFERENCE_FIELDS: Readonly<Record<ElementKind, readonly string[]>> = {
  Concept: ['merged_into'],
  Proposition: ['subject', 'object'],
  Assertion: ['proposition', 'asserted_by'],
  Evidence: ['generated_by'],
  Activity: [],
}

/** Where a matcher field is read from in the rendered view. */
const FIELD_PATHS: Readonly<Record<string, string[]>> = {
  proposition: ['proposition_id'],
  status: ['lifecycle', 'status'],
  state: ['_system', 'state'],
  version: ['_system', 'version'],
  type: ['schema_ref'],
  predicate: ['predicate_ref'],
}

/** The SQL column a literal matcher field narrows on, by kind. */
const COLUMNS: Readonly<Record<ElementKind, Readonly<Record<string, string>>>> =
  {
    Concept: {
      type: 'schema_ref',
      schema_ref: 'schema_ref',
      name: 'name',
      key: 'key',
      canonical_id: 'canonical_id',
      state: 'state',
    },
    Proposition: { predicate: 'predicate_ref', state: 'state' },
    Assertion: {
      stance: 'stance',
      mode: 'mode',
      status: 'status',
      proposition: 'proposition_id',
      state: 'state',
    },
    Evidence: {
      evidence_class: 'evidence_class',
      status: 'status',
      content_digest: 'content_digest',
      generated_by: 'generated_by',
      state: 'state',
    },
    Activity: {
      activity_class: 'activity_class',
      status: 'status',
      state: 'state',
    },
  }

/** The matcher fields whose value is a schema symbol to resolve. */
const SYMBOL_FIELDS = new Set(['type', 'schema_ref', 'predicate'])

/** Runs a whole `WHERE` block against one starting solution set. */
export function solveAll(
  cx: Context,
  clauses: readonly WhereClause[],
  incoming: readonly Solution[],
  b: ReadBindings,
): Solution[] {
  let solutions = [...incoming]
  for (const clause of clauses) {
    if ('Union' in clause) {
      // A UNION is an alternative to everything the block has said so far, not
      // a further narrowing of it. So its arm is evaluated against what entered
      // the block — evaluating it against the accumulated solutions would make
      // `{ A UNION { B } }` mean `A AND B`, which is the opposite of widening
      // and returns nothing whenever the two arms disagree.
      solutions = distinct([
        ...solutions,
        ...solveAll(cx, clause.Union, incoming, b),
      ])
    } else {
      solutions = solveClause(cx, clause, solutions, b)
    }
    if (solutions.length > LIMITS.solutions) {
      cx.spend('scans', solutions.length)
    }
  }
  return solutions
}

function solveClause(
  cx: Context,
  clause: WhereClause,
  incoming: readonly Solution[],
  b: ReadBindings,
): Solution[] {
  if ('Filter' in clause) {
    return incoming.filter((solution) =>
      evaluateFilter(cx, clause.Filter.expression, solution, b),
    )
  }
  if ('Not' in clause) {
    // The record, never the world: a solution survives when the inner block
    // finds nothing to extend it with.
    return incoming.filter(
      (solution) => solveAll(cx, clause.Not, [solution], b).length === 0,
    )
  }
  if ('Optional' in clause) {
    const out: Solution[] = []
    for (const solution of incoming) {
      const extended = solveAll(cx, clause.Optional, [solution], b)
      // Padding rather than dropping is the whole point: the row survives with
      // the optional variables simply unbound, and a projection of one reads
      // null.
      out.push(...(extended.length === 0 ? [solution] : extended))
    }
    return out
  }
  if ('Union' in clause) {
    // Handled by `solveAll`, which is the only place that still has the
    // solutions the block started from.
    throw errors.internalError('a UNION reached the per-clause path')
  }
  if ('Concept' in clause) {
    return element(cx, 'Concept', clause.Concept.variable, clause.Concept.matcher, incoming, b)
  }
  if ('Assertion' in clause) {
    return element(cx, 'Assertion', clause.Assertion.variable, clause.Assertion.matcher, incoming, b)
  }
  if ('Evidence' in clause) {
    return element(cx, 'Evidence', clause.Evidence.variable, clause.Evidence.matcher, incoming, b)
  }
  if ('Activity' in clause) {
    return element(cx, 'Activity', clause.Activity.variable, clause.Activity.matcher, incoming, b)
  }
  if ('Proposition' in clause) {
    return propositions(cx, clause.Proposition.variable, clause.Proposition.matcher, incoming, b)
  }
  if ('Structural' in clause) {
    return structural(cx, clause.Structural, incoming, b)
  }
  if ('Belief' in clause) {
    return belief(cx, clause.Belief, incoming, b)
  }
  if ('BeliefSlot' in clause) {
    return beliefSlot(cx, clause.BeliefSlot, incoming, b)
  }

  const name = Object.keys(clause)[0] ?? 'this pattern'
  throw errors.unsupportedCapability(
    `the ${name} pattern is not implemented by this engine yet; see ` +
      `DESCRIBE CAPABILITIES`,
  )
}

// --- element patterns -------------------------------------------------------

function element(
  cx: Context,
  kind: ElementKind,
  variable: string,
  matcher: ObjectMatcher,
  incoming: readonly Solution[],
  b: ReadBindings,
): Solution[] {
  const out: Solution[] = []
  for (const solution of incoming) {
    // A variable the incoming solutions already bound narrows the scan to one
    // element rather than re-scanning the table for each row.
    const bound = solution.get(variable)
    const candidates =
      bound?.kind === 'element'
        ? bound.id.kind === kind
          ? [bound.id]
          : []
        : scan(cx, kind, matcher, solution, b)

    for (const id of candidates) {
      const extended = checkMatcher(cx, id, matcher, solution, b)
      if (extended === null) continue
      const bindings = extend(extended, variable, elementBinding(id))
      if (bindings !== null) out.push(bindings)
    }
  }
  return out
}

/** The candidate ids for one element pattern, narrowed in SQL where possible. */
function scan(
  cx: Context,
  kind: ElementKind,
  matcher: ObjectMatcher,
  solution: Solution,
  b: ReadBindings,
): ElementId[] {
  const table = cx.table(kind)
  const columns = COLUMNS[kind]
  const wheres = ['space = ?']
  const values: SqlStorageValue[] = [cx.space]

  const direct = literalOf(matcher.id, solution, b)
  if (typeof direct === 'string') {
    const id = tryParseElementId(direct)
    return id !== null && id.kind === kind ? [id] : []
  }

  // At a past coordinate the indexes say nothing: they describe the present.
  // The version log is reconstructed instead, and every narrowing this function
  // would have pushed into SQL is re-applied by `checkMatcher` against the
  // reconstructed row — including the default `state = active`, which is why
  // dropping the SQL predicates here does not widen the answer.
  if (cx.historical) {
    return cx.reconstruct(kind).map((element) => ({
      kind: element.kind,
      seq: element.row.id,
    }))
  }

  for (const [field, value] of Object.entries(matcher)) {
    const column = columns[field]
    if (column === undefined) continue
    const literal = literalOf(value, solution, b)
    if (typeof literal !== 'string') continue
    wheres.push(`"${column}" = ?`)
    values.push(SYMBOL_FIELDS.has(field) ? resolveSymbol(cx, field, literal) : literal)
  }

  // A pattern matches active elements unless it says otherwise — that is what
  // archiving means.
  if (!Object.hasOwn(matcher, 'state')) {
    wheres.push('state = ?')
    values.push(State.ACTIVE)
  }
  wheres.push('state <> ?')
  values.push(State.PENDING)

  const rows = cx.store.sql
    .exec<SqlRow>(
      `SELECT * FROM ${table} WHERE ${wheres.join(' AND ')} ORDER BY id`,
      ...values,
    )
    .toArray()
  cx.spend('scans', rows.length)

  return rows.map((row) =>
    cx.remember({ kind, row: decodeRow<ElementRow>(table, row) } as Element),
  )
}

/**
 * Checks every matcher field against the element, binding the variables.
 *
 * The SQL narrowing above is an optimization; this is the rule. A field the
 * scan could not push down — a variable, a nested value, a Facet path — is
 * checked here, so a pattern never matches more than it said.
 */
function checkMatcher(
  cx: Context,
  id: ElementId,
  matcher: ObjectMatcher,
  solution: Solution,
  b: ReadBindings,
): Solution | null {
  const view = cx.view(id)
  if (view === null) return null
  if (!Object.hasOwn(matcher, 'state')) {
    // Read off the row, not off the view. A field-masked Grant redacts
    // `_system` out of the view entirely, and reading the state from there
    // would silently skip this check for exactly those callers — letting an
    // archived, tombstoned or purged element match a pattern that named no
    // state, on the two paths where the SQL narrowing is not there to catch it:
    // a literal `{id: …}` target and every historical read.
    const element = cx.load(id)
    if (element === null || element.row.state !== State.ACTIVE) return null
  }

  let current: Solution = solution
  for (const [field, expected] of Object.entries(matcher)) {
    const path = FIELD_PATHS[field] ?? [field]
    const actual = readField(view, path)
    const asReference = REFERENCE_FIELDS[id.kind].includes(field)

    if ('Variable' in expected) {
      const binding = bindingOf(actual, asReference)
      if (binding === null) return null
      const next = extend(current, expected.Variable, binding)
      if (next === null) return null
      current = next
      continue
    }

    const wanted = literalOf(expected, current, b)
    if (wanted === undefined) {
      throw errors.unsupportedCapability(
        `a ${field} matcher of this shape is not implemented by this engine ` +
          `yet; see DESCRIBE CAPABILITIES`,
      )
    }
    const resolved =
      SYMBOL_FIELDS.has(field) && typeof wanted === 'string'
        ? resolveSymbol(cx, field, wanted)
        : wanted
    if (!sameValue(actual, resolved, asReference)) return null
  }
  return current
}

/** Reads a matcher field, following the path the view actually stores it at. */
function readField(view: JsonMap, path: readonly string[]): Json {
  let current: Json = view
  for (const step of path) {
    if (!isJsonMap(current)) return null
    current = (current[step] ?? null) as Json
  }
  return current
}

/** The binding a view value produces, as an element or as a Literal. */
function bindingOf(value: Json, asReference: boolean): Binding | null {
  if (value === null) return null
  if (asReference) {
    const id =
      typeof value === 'string'
        ? tryParseElementId(value)
        : isJsonMap(value) && typeof value.id === 'string'
          ? tryParseElementId(value.id)
          : null
    return id === null ? null : elementBinding(id)
  }
  return literalBinding(value)
}

/** Whether a stored value satisfies a matcher's expected value. */
function sameValue(actual: Json, expected: Json, asReference: boolean): boolean {
  if (asReference) {
    const left = bindingOf(actual, true)
    const right =
      typeof expected === 'string' || isJsonMap(expected)
        ? bindingOf(expected as Json, true)
        : null
    return left !== null && right !== null
      ? formatElementId((left as { id: ElementId }).id) ===
          formatElementId((right as { id: ElementId }).id)
      : false
  }
  if (isJsonMap(actual) && isJsonMap(expected)) {
    // A reference-shaped expectation compares by identity, not by member.
    return JSON.stringify(actual) === JSON.stringify(expected)
  }
  return actual === expected
}

/** The literal a matcher value carries, or `undefined` when it carries none. */
function literalOf(
  value: MatchValue | undefined,
  solution: Solution,
  b: ReadBindings,
): Json | undefined {
  if (value === undefined) return undefined
  if ('Literal' in value) return kipLiteral(value.Literal)
  if ('Param' in value) return parameterValue(b, value.Param)
  if ('Variable' in value) {
    const bound = solution.get(value.Variable)
    if (bound === undefined) return undefined
    return bound.kind === 'element'
      ? formatElementId(bound.id)
      : (bound.value as Json)
  }
  return undefined
}

/** Resolves a schema symbol a matcher wrote as a local name. */
function resolveSymbol(cx: Context, field: string, name: string): string {
  const kind = field === 'predicate' ? 'PredicateType' : 'ConceptType'
  return formatSymbolRef(cx.env.resolveSymbol(kind, name, 'read'))
}

// --- Proposition patterns ---------------------------------------------------

function propositions(
  cx: Context,
  variable: string | null,
  matcher: PropositionMatcher,
  incoming: readonly Solution[],
  b: ReadBindings,
): Solution[] {
  if ('Id' in matcher) {
    const value = 'Param' in matcher.Id
      ? parameterValue(b, matcher.Id.Param)
      : kipLiteral(matcher.Id.Literal)
    if (typeof value !== 'string') {
      throw errors.typeMismatch('a Proposition id must be a string')
    }
    const id = parseElementId(value)
    const out: Solution[] = []
    for (const solution of incoming) {
      if (cx.view(id) === null) continue
      const next = variable === null ? solution : extend(solution, variable, elementBinding(id))
      if (next !== null) out.push(next)
    }
    return out
  }

  const { subject, predicate, object } = matcher.Tuple
  if (!('Atom' in predicate)) {
    throw errors.unsupportedCapability(
      'a predicate path or hop quantifier is not implemented by this engine ' +
        'yet; see DESCRIBE CAPABILITIES',
    )
  }

  const out: Solution[] = []
  for (const solution of incoming) {
    for (const row of tupleCandidates(cx, subject, predicate.Atom, object, solution, b)) {
      const id: ElementId = { kind: 'Proposition', seq: row.seq }
      // The choke point, for the same reason every other pattern consults it: a
      // Proposition this caller may not read is not matched, not counted and
      // not bound.
      if (cx.view(id) === null) continue
      let current: Solution | null = solution
      current = bindTerm(current, subject, row.subject, b)
      if (current === null) continue
      current = bindTerm(current, object, row.object, b)
      if (current === null) continue
      if ('Variable' in predicate.Atom) {
        current = extend(current, predicate.Atom.Variable, symbolBinding(row.predicate_ref))
        if (current === null) continue
      }
      if (variable !== null) {
        current = extend(current, variable, elementBinding(id))
        if (current === null) continue
      }
      out.push(current)
    }
  }
  return out
}

interface TupleRow {
  seq: number
  subject: Json
  object: Json
  predicate_ref: string
}

/** The Proposition rows a tuple pattern could match, narrowed by its pinned ends. */
function tupleCandidates(
  cx: Context,
  subject: Term,
  predicate: PredAtom,
  object: Term,
  solution: Solution,
  b: ReadBindings,
): TupleRow[] {
  // The narrowing is computed once and then either pushed into SQL or applied in
  // JavaScript, so the present and historical paths cannot drift apart about
  // what a tuple pattern matches. Splitting them into two independent filters is
  // how a historical read would silently answer more than a present one.
  const subjectKey = pinnedKey(subject, solution, b)
  const objectKey = pinnedKey(object, solution, b)
  let predicateRef: string | null = null
  if (!('Variable' in predicate)) {
    const name =
      'Literal' in predicate ? predicate.Literal : parameterValue(b, predicate.Param)
    if (typeof name !== 'string') {
      throw errors.typeMismatch('a predicate must be a symbol string')
    }
    predicateRef = resolveSymbol(cx, 'predicate', name)
  }

  if (cx.historical) {
    return cx
      .reconstruct('Proposition')
      .map((element) => element.row as PropositionRow)
      .filter(
        (row) =>
          row.state === State.ACTIVE &&
          (subjectKey === null || row.subject_key === subjectKey) &&
          (objectKey === null || row.object_key === objectKey) &&
          (predicateRef === null || row.predicate_ref === predicateRef),
      )
      .map((row) => ({
        seq: row.id,
        subject: row.subject as Json,
        object: row.object as Json,
        predicate_ref: row.predicate_ref,
      }))
  }

  const wheres = ['space = ?', 'state = ?']
  const values: SqlStorageValue[] = [cx.space, State.ACTIVE]
  if (subjectKey !== null) {
    wheres.push('subject_key = ?')
    values.push(subjectKey)
  }
  if (objectKey !== null) {
    wheres.push('object_key = ?')
    values.push(objectKey)
  }
  if (predicateRef !== null) {
    wheres.push('predicate_ref = ?')
    values.push(predicateRef)
  }

  // The whole row rather than the four columns the tuple needs, so the element
  // can be remembered through `Context`'s visibility check. Reading less here
  // would mean loading the row a second time to ask whether the caller may see
  // it — and skipping the question would let a tuple pattern match a
  // Proposition that is outside this caller's query universe (§104).
  const rows = cx.store.sql
    .exec<SqlRow>(
      `SELECT * FROM propositions WHERE ${wheres.join(' AND ')} ORDER BY id`,
      ...values,
    )
    .toArray()
  cx.spend('scans', rows.length)

  return rows.map((row) => {
    const decoded = decodeRow<PropositionRow>('propositions', row)
    cx.remember({ kind: 'Proposition', row: decoded })
    return {
      seq: decoded.id,
      subject: decoded.subject as Json,
      object: decoded.object as Json,
      predicate_ref: decoded.predicate_ref,
    }
  })
}

/**
 * The endpoint key a term is already pinned to, if it is.
 *
 * A variable an earlier pattern bound counts as pinned — that is what turns a
 * join into an index seek instead of a scan of every tuple in the Space.
 */
function pinnedKey(
  term: Term,
  solution: Solution,
  b: ReadBindings,
): string | null {
  const endpoint = termEndpoint(term, solution, b)
  return endpoint === null ? null : endpointKey(endpointFromJson(endpoint))
}

/** The endpoint JSON a term denotes, when it denotes one already. */
function termEndpoint(
  term: Term,
  solution: Solution,
  b: ReadBindings,
): Json | null {
  if ('Variable' in term) {
    const bound = solution.get(term.Variable)
    if (bound === undefined) return null
    return bound.kind === 'element'
      ? { id: formatElementId(bound.id) }
      : (bound.value as Json)
  }
  if ('Literal' in term) return kipLiteral(term.Literal)
  if ('Param' in term) {
    const value = parameterValue(b, term.Param)
    return typeof value === 'string' && tryParseElementId(value) !== null
      ? { id: value }
      : value
  }
  if ('Match' in term) {
    const id = literalOf(term.Match.id, solution, b)
    return typeof id === 'string' ? { id } : null
  }
  return null
}

/** Binds a tuple endpoint's variable, or checks it against what it holds. */
function bindTerm(
  solution: Solution,
  term: Term,
  value: Json,
  b: ReadBindings,
): Solution | null {
  const local = isJsonMap(value) && typeof value.id === 'string'
    ? tryParseElementId(value.id)
    : null

  if ('Variable' in term) {
    const binding = local !== null ? elementBinding(local) : literalBinding(value)
    return extend(solution, term.Variable, binding)
  }
  const expected = termEndpoint(term, solution, b)
  if (expected === null) return solution
  return endpointKey(endpointFromJson(expected)) ===
    endpointKey(endpointFromJson(value))
    ? solution
    : null
}

// --- structural patterns ----------------------------------------------------

/**
 * `STRUCTURAL (?src, "field", ?dst)` — Profile record topology.
 *
 * Profile fields only, which is a known gap rather than a design: an
 * Assertion's `evidence` and an Activity's `inputs` are Core structural fields
 * living in typed columns, and this pattern cannot reach them, so "which
 * Assertions cite this Evidence" has no spelling yet. The reverse index in
 * `element_refs` already holds the answer; the pattern is what does not ask it.
 */
function structural(
  cx: Context,
  clause: {
    variable: string | null
    subject: Term
    field: { Name: string } | { Param: string }
    object: Term
  },
  incoming: readonly Solution[],
  b: ReadBindings,
): Solution[] {
  const name = 'Name' in clause.field ? clause.field.Name : String(parameterValue(b, clause.field.Param))
  const field = formatSymbolRef(cx.env.resolveSymbol('StructuralField', name, 'read'))

  const out: Solution[] = []
  if (cx.historical) {
    for (const solution of incoming) {
      const fixedSource = termEndpoint(clause.subject, solution, b)
      const fixedId =
        isJsonMap(fixedSource) && typeof fixedSource.id === 'string'
          ? tryParseElementId(fixedSource.id)
          : null
      const sources =
        fixedSource === null
          ? cx.reconstruct('Concept').map((element) => ({
              kind: element.kind,
              seq: element.row.id,
            } as ElementId))
          : fixedId?.kind === 'Concept'
            ? [fixedId]
            : []

      for (const src of sources) {
        const view = cx.view(src)
        const structural = view === null ? null : view.structural
        const references = isJsonMap(structural) ? structural[field] : null
        if (!Array.isArray(references)) continue
        for (const reference of references) {
          if (!isJsonMap(reference) || typeof reference.id !== 'string') continue
          const dst = tryParseElementId(reference.id)
          if (dst === null || cx.view(dst) === null) continue
          let current: Solution | null = solution
          current = bindTerm(current, clause.subject, { id: formatElementId(src) }, b)
          if (current === null) continue
          current = bindTerm(current, clause.object, reference as Json, b)
          if (current === null) continue
          if (clause.variable !== null) {
            current = extend(current, clause.variable, symbolBinding(field))
            if (current === null) continue
          }
          out.push(current)
        }
      }
    }
    return out
  }

  for (const solution of incoming) {
    const wheres = ['space = ?', 'field = ?']
    const values: SqlStorageValue[] = [cx.space, `structural:${field}`]
    const from = termEndpoint(clause.subject, solution, b)
    if (isJsonMap(from) && typeof from.id === 'string') {
      wheres.push('from_id = ?')
      values.push(from.id)
    }
    const to = termEndpoint(clause.object, solution, b)
    if (isJsonMap(to) && typeof to.id === 'string') {
      wheres.push('to_id = ?')
      values.push(to.id)
    }

    const rows = cx.store.sql
      .exec<{ from_id: string; to_id: string }>(
        `SELECT from_id, to_id FROM element_refs
           WHERE ${wheres.join(' AND ')} ORDER BY from_id, ord`,
        ...values,
      )
      .toArray()
    cx.spend('scans', rows.length)

    for (const row of rows) {
      const src = parseElementId(row.from_id)
      const dst = parseElementId(row.to_id)
      if (cx.view(src) === null || cx.view(dst) === null) continue
      let current: Solution | null = solution
      current = bindTerm(current, clause.subject, { id: row.from_id }, b)
      if (current === null) continue
      current = bindTerm(current, clause.object, { id: row.to_id }, b)
      if (current === null) continue
      if (clause.variable !== null) {
        current = extend(current, clause.variable, symbolBinding(field))
        if (current === null) continue
      }
      out.push(current)
    }
  }
  return out
}

// --- projection patterns ----------------------------------------------------

/**
 * `?b BELIEF (…)` — what this Brain currently holds about a Proposition.
 *
 * The target must already be bound. Projecting over an unbound variable would
 * mean projecting over every Proposition in the Space, which is not a slower
 * version of the question — it is a different one, and answering it would hand
 * back beliefs about tuples the caller never mentioned.
 */
function belief(
  cx: Context,
  clause: { variable: string; target: BeliefTarget },
  incoming: readonly Solution[],
  b: ReadBindings,
): Solution[] {
  const out: Solution[] = []
  for (const solution of incoming) {
    const target = beliefTarget(cx, clause.target, solution, b)
    const projected = project(cx, target, b.policy)
    const next = extend(
      solution,
      clause.variable,
      literalBinding(beliefToJson(projected) as Json),
    )
    if (next !== null) out.push(next)
  }
  return out
}

/** The Proposition a BELIEF clause names. */
function beliefTarget(
  cx: Context,
  target: BeliefTarget,
  solution: Solution,
  b: ReadBindings,
): ElementId {
  if ('Proposition' in target) {
    const bound = solution.get(target.Proposition)
    if (bound === undefined || bound.kind !== 'element') {
      throw errors.projectionTargetUnbound(
        `?${target.Proposition} is not bound to a Proposition where the ` +
          `BELIEF clause reads it; bind it with a pattern first`,
      )
    }
    return bound.id
  }
  if ('Id' in target) {
    const value =
      'Param' in target.Id
        ? parameterValue(b, target.Id.Param)
        : kipLiteral(target.Id.Literal)
    if (typeof value !== 'string') {
      throw errors.typeMismatch('a Proposition id must be a string')
    }
    return parseElementId(value)
  }
  // An inline tuple: resolved the way a pattern would, and refused when it
  // names no Proposition on record rather than projecting about nothing.
  const tuple = propositions(cx, '__belief', { Tuple: target.Tuple }, [solution], b)
  const first = tuple[0]?.get('__belief')
  if (first === undefined || first.kind !== 'element') {
    throw errors.notFoundOrNotVisible(
      'the BELIEF tuple names no Proposition on record here',
    )
  }
  return first.id
}

/**
 * `?slot BELIEF SLOT (?subject, "predicate")` — the conflict set of one slot.
 *
 * Reports every candidate rather than a winner: a functional slot holding two
 * accepted values is a real state, and naming one of them would be taking a
 * side the record does not take.
 */
function beliefSlot(
  cx: Context,
  clause: { variable: string; subject: Term; predicate: PredAtom },
  incoming: readonly Solution[],
  b: ReadBindings,
): Solution[] {
  const out: Solution[] = []
  for (const solution of incoming) {
    const subject = termEndpoint(clause.subject, solution, b)
    if (subject === null) {
      // Unbounded rather than unbound: the clause is well-formed, and the set
      // it would range over is every subject in the Space.
      throw errors.projectionTargetUnbounded(
        'BELIEF SLOT needs a bound subject; bind it with a pattern first',
      )
    }
    if ('Variable' in clause.predicate) {
      throw errors.projectionTargetUnbounded(
        'BELIEF SLOT needs an exact predicate; a projection never walks a ' +
          'variable predicate',
      )
    }
    const name =
      'Literal' in clause.predicate
        ? clause.predicate.Literal
        : parameterValue(b, clause.predicate.Param)
    if (typeof name !== 'string') {
      throw errors.typeMismatch('a predicate must be a symbol string')
    }
    const predicateRef = resolveSymbol(cx, 'predicate', name)
    const key = endpointKey(endpointFromJson(subject))
    const beliefs = slotPropositions(cx, key, predicateRef).map((id) =>
      project(cx, id, b.policy),
    )
    const next = extend(
      solution,
      clause.variable,
      literalBinding(slotToJson(subject, predicateRef, beliefs) as Json),
    )
    if (next !== null) out.push(next)
  }
  return out
}

/** Reads a dot path off a bound variable, for filters and projections. */
export function readVariable(
  cx: Context,
  solution: Solution,
  variable: string,
  path: readonly { Field: string }[] | readonly { Key: string }[] | readonly (
    | { Field: string }
    | { Key: string }
  )[],
): Json {
  const bound = solution.get(variable)
  if (bound === undefined) return null
  if (path.length === 0) {
    return bound.kind === 'element'
      ? formatElementId(bound.id)
      : (bound.value as Json)
  }
  if (bound.kind === 'element') {
    const view = cx.view(bound.id)
    return view === null ? null : readPath(cx.env, view, path)
  }
  // A non-element binding can still have members: a BELIEF binds a projection
  // object, and `?b.support.score` reads into it exactly as a dot path reads
  // into an element's view. Refusing here would make the projection's own
  // output unreadable by the language that produced it.
  return readPath(cx.env, bound.value as Json, path)
}

export { kipLiteral }

/** Evaluates a `parameter | literal` slot. */
export function scalarValue(scalar: Scalar, b: ReadBindings): Json {
  return 'Param' in scalar ? parameterValue(b, scalar.Param) : kipLiteral(scalar.Literal)
}
