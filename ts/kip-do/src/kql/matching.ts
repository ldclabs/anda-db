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
  PropositionTriple,
  Term,
  WhereClause,
} from '../kip/ast.js'
import { kipValue as kipLiteral } from '../kml/value.js'
import {
  formatSymbolRef,
  lineageOfSymbol,
  lineageText,
  structuralFieldDef,
} from '../schema/index.js'
import { State, type PropositionRow } from '../store/index.js'
import type { Element, ElementRow } from '../store/index.js'
import { endpointFromJson, endpointKey } from '../term.js'
import { readPath } from '../view.js'
import {
  beliefToJson,
  project,
  slotPropositions,
  slotToJson,
  projectionBasis,
  checkProjectionHistory,
  ungroundedBelief,
  type Policy,
  type Slot,
} from '../projection/index.js'
import { nowTime } from '../time.js'
import { Context, LIMITS } from './context.js'
import { evaluateFilter } from './filter.js'
import {
  distinct,
  elementBinding,
  extend,
  literalBinding,
  symbolBinding,
  type Binding,
  type MutableSolution,
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

/**
 * Where a matcher field is read from in the rendered view.
 *
 * What is left here is sugar — a short spelling the Specification's own
 * examples use — rather than translation. `proposition` used to be redirected
 * to `proposition_id` because the view spelled it that way; the view now
 * spells it as §13.2 does, so the entry is gone and `FIND(?a.proposition)`
 * reads the same slot the pattern matched on.
 */
const FIELD_PATHS: Readonly<Record<string, string[]>> = {
  status: ['lifecycle', 'status'],
  state: ['_system', 'state'],
  version: ['_system', 'version'],
  type: ['schema_ref'],
  predicate: ['predicate_ref'],
}

/**
 * The SQL column a literal matcher field narrows on, by kind.
 *
 * `type` and `predicate` seek on the *lineage* columns (§20.14, §43.1): a
 * pattern's symbol matches every readable version of it, and each matched
 * element reports its own exact `schema_ref` / `predicate_ref`.
 */
const COLUMNS: Readonly<Record<ElementKind, Readonly<Record<string, string>>>> =
  {
    Concept: {
      type: 'lineage',
      schema_ref: 'schema_ref',
      name: 'name',
      key: 'key',
      canonical_id: 'canonical_id',
      state: 'state',
    },
    Proposition: { predicate: 'predicate_lineage', state: 'state' },
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

/**
 * The internal handle a `BELIEF (triple)` binds its resolved Proposition to.
 *
 * Named rather than spelled inline because it is a variable name that must not
 * collide with a caller's: it is stripped from the row before the belief is
 * bound, and a query that happened to use the same spelling would lose its own
 * column.
 */
const BELIEF_TARGET = '__belief_target'

/** The matcher fields whose value is a schema symbol to resolve. */
const SYMBOL_FIELDS = new Set(['type', 'schema_ref', 'predicate'])

/**
 * The matcher fields that compare by symbol lineage rather than exact
 * reference (§20.14): `type` and `predicate` are resolution sugar for a
 * lineage, while `schema_ref` names an exact version and is matched exactly.
 */
const LINEAGE_FIELDS = new Set(['type', 'predicate'])

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

  const rows = cx.store.all<ElementRow>(
    table,
    `SELECT * FROM ${table} WHERE ${wheres.join(' AND ')} ORDER BY id`,
    ...values,
  )
  cx.spend('scans', rows.length)

  return rows.map((row) => cx.remember({ kind, row } as Element))
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
    if (LINEAGE_FIELDS.has(field) && typeof wanted === 'string') {
      // The view carries the exact reference; the pattern names a lineage.
      if (typeof actual !== 'string' || lineageText(actual) !== resolveSymbol(cx, field, wanted)) {
        return null
      }
      continue
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

/**
 * Resolves a schema symbol a matcher wrote as a local name.
 *
 * `type` and `predicate` resolve to the lineage the pattern matches over
 * (§20.14); `schema_ref` to the exact reference it names.
 */
function resolveSymbol(cx: Context, field: string, name: string): string {
  const kind = field === 'predicate' ? 'PredicateType' : 'ConceptType'
  const symbol = cx.env.resolveSymbol(kind, name, 'read')
  return LINEAGE_FIELDS.has(field) ? lineageOfSymbol(symbol) : formatSymbolRef(symbol)
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
      current = bindTupleTerm(cx, current, subject, row.subject, b)
      if (current === null) continue
      current = bindTupleTerm(cx, current, object, row.object, b)
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

/**
 * The Proposition rows a tuple pattern could match, narrowed by its pinned
 * ends.
 *
 * Matching is **canonical** (§12.3, §43.2): an endpoint naming a Concept
 * matches a stored endpoint whose `merged_into` chain resolves to the same
 * identity, so after `MERGE CONCEPT :alicia INTO :alice` both spellings find
 * the tuple recorded on `alicia`. The predicate matches by lineage (§20.14),
 * so a tuple written under an earlier package version is still found.
 */
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
  const subjectKeys = pinnedKeys(cx, subject, solution, b)
  const objectKeys = pinnedKeys(cx, object, solution, b)
  let predicateLineage: string | null = null
  if (!('Variable' in predicate)) {
    const name =
      'Literal' in predicate ? predicate.Literal : parameterValue(b, predicate.Param)
    if (typeof name !== 'string') {
      throw errors.typeMismatch('a predicate must be a symbol string')
    }
    predicateLineage = resolveSymbol(cx, 'predicate', name)
  }

  if (cx.historical) {
    return cx
      .reconstruct('Proposition')
      .map((element) => element.row as PropositionRow)
      .filter(
        (row) =>
          row.state === State.ACTIVE &&
          (subjectKeys === null || subjectKeys.includes(row.subject_key)) &&
          (objectKeys === null || objectKeys.includes(row.object_key)) &&
          (predicateLineage === null ||
            lineageOfRow(row) === predicateLineage),
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
  if (subjectKeys !== null) {
    wheres.push(`subject_key IN (SELECT value FROM json_each(?))`)
    values.push(JSON.stringify(subjectKeys))
  }
  if (objectKeys !== null) {
    wheres.push(`object_key IN (SELECT value FROM json_each(?))`)
    values.push(JSON.stringify(objectKeys))
  }
  if (predicateLineage !== null) {
    wheres.push('predicate_lineage = ?')
    values.push(predicateLineage)
  }

  // The whole row rather than the four columns the tuple needs, so the element
  // can be remembered through `Context`'s visibility check. Reading less here
  // would mean loading the row a second time to ask whether the caller may see
  // it — and skipping the question would let a tuple pattern match a
  // Proposition that is outside this caller's query universe (§104).
  const rows = cx.store.all<PropositionRow>(
    'propositions',
    `SELECT * FROM propositions WHERE ${wheres.join(' AND ')} ORDER BY id`,
    ...values,
  )
  cx.spend('scans', rows.length)

  return rows.map((decoded) => {
    cx.remember({ kind: 'Proposition', row: decoded })
    return {
      seq: decoded.id,
      subject: decoded.subject as Json,
      object: decoded.object as Json,
      predicate_ref: decoded.predicate_ref,
    }
  })
}

/** A stored Proposition's predicate lineage, filled from the exact ref for an older row. */
function lineageOfRow(row: PropositionRow): string {
  return row.predicate_lineage === '' ? lineageText(row.predicate_ref) : row.predicate_lineage
}

/**
 * The endpoint keys a term is already pinned to, if it is — every spelling of
 * one canonical identity (§43.2), or `null` for an open slot.
 */
function pinnedKeys(
  cx: Context,
  term: Term,
  solution: Solution,
  b: ReadBindings,
): string[] | null {
  const endpoint = termEndpoint(term, solution, b)
  if (endpoint === null) return null
  return canonicalKeys(cx, endpointFromJson(endpoint))
}

/**
 * Every endpoint key that matches one endpoint canonically (§12.3).
 *
 * A local Concept reference matches the whole cluster of Concepts whose
 * `merged_into` chains end at the same identity; any other endpoint — a
 * Literal, a canonical identity, a foreign reference, a record — matches
 * itself alone.
 */
export function canonicalKeys(cx: Context, endpoint: ReturnType<typeof endpointFromJson>): string[] {
  if (endpoint.kind !== 'local' || endpoint.id.kind !== 'Concept') {
    return [endpointKey(endpoint)]
  }
  return cx
    .canonicalCluster(endpoint.id)
    .map((id) => endpointKey({ kind: 'local', id }))
}

/**
 * The endpoint JSON a term denotes, when it denotes one already.
 *
 * `null` means exactly one thing: **not pinned yet** — an unbound variable the
 * pattern will bind from whatever it matches. It is not a place to put "this
 * engine cannot resolve that", because every caller reads `null` as an open
 * slot: `pinnedKey` omits the SQL predicate, `bindTerm` checks nothing, and
 * `tupleIsGrounded` answers "not grounded". A term the engine cannot resolve
 * therefore throws rather than returning `null`, or the tuple pattern would
 * quietly match every Proposition under its predicate (§43.2).
 */
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
  if ('Match' in term) return matcherEndpoint(term.Match, b)
  // §43.2 blesses `(id: …)` as a `term`, which is how a statement about a
  // statement names an existing Proposition. Not built here yet — and refused
  // rather than ignored, for the reason above.
  throw errors.unsupportedCapability(
    'a nested Proposition in a tuple endpoint (§43.2) is not implemented by ' +
      'this engine yet; bind the Proposition with its own pattern and pass ' +
      'the variable',
  )
}

/**
 * The endpoint an inline `{...}` matcher names (§8.1, §8.2).
 *
 * Only two spellings of an object pattern *name* something: `{id: …}` is a
 * Local Element Reference and `{canonical_id: …}` is a Canonical Identity
 * Reference. Every other matcher describes a search, and resolving one would
 * pick a winner among the Concepts a description is allowed to match — the
 * arbitrary choice §7.2 forbids for names. The same two fields, in the same
 * order, that `termValue` accepts on the mutation path.
 *
 * The refusal is `IdentitySelectorRequired` and not `UnsupportedCapability`:
 * no engine should ever resolve a description to one endpoint, so this is not
 * a gap that a later version closes.
 */
function matcherEndpoint(matcher: ObjectMatcher, b: ReadBindings): Json {
  for (const field of ['id', 'canonical_id'] as const) {
    const member = matcher[field]
    if (member === undefined) continue
    // An identity resolves the endpoint; it does not also filter it. A matcher
    // carrying more than the identity asked for something this position cannot
    // do, and answering it by dropping the rest would let
    // `{id: "C-1", name: "Zed"}` match C-1 whatever C-1 is called — the silent
    // wrong answer an unconstrained endpoint gives, one member in.
    const extra = Object.keys(matcher).filter((key) => key !== field)
    if (extra.length > 0) {
      throw errors.identitySelectorRequired(
        `a tuple endpoint names \`${field}\`, so it is resolved by identity ` +
          `and not matched by description; ${extra.join(', ')} would be ` +
          `silently ignored. Drop ${extra.length === 1 ? 'it' : 'them'} or ` +
          `bind the element with its own pattern`,
      )
    }
    // Read off the member itself rather than through `literalOf`, which
    // collapses an unbound variable to `undefined` — indistinguishable here
    // from "no such field", and this position has no open-slot reading: an
    // identity is written down or it is not one.
    const value =
      'Literal' in member
        ? kipLiteral(member.Literal)
        : 'Param' in member
          ? parameterValue(b, member.Param)
          : undefined
    if (value === undefined) {
      throw errors.identitySelectorRequired(
        `\`${field}\` in a tuple endpoint must be a literal identity or a ` +
          `parameter, not a pattern`,
      )
    }
    if (typeof value !== 'string') {
      throw errors.identitySelectorRequired(
        `\`${field}\` in a tuple endpoint must be a string, got ` +
          JSON.stringify(value),
      )
    }
    return { [field]: value }
  }
  throw errors.identitySelectorRequired(
    'a tuple endpoint written as an object must name a stable identity: ' +
      '{id: "…"} or {canonical_id: "…"}; matching one by description would ' +
      'pick a winner among the Concepts a description is allowed to share',
  )
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

/**
 * Binds a tuple endpoint's variable, or checks it canonically against what
 * it holds (§43.2): a pinned Concept matches a stored endpoint anywhere in
 * its merge cluster, while a Literal or a record matches exactly.
 */
function bindTupleTerm(
  cx: Context,
  solution: Solution,
  term: Term,
  value: Json,
  b: ReadBindings,
): Solution | null {
  if ('Variable' in term) return bindTerm(solution, term, value, b)
  const expected = termEndpoint(term, solution, b)
  if (expected === null) return solution
  const stored = endpointKey(endpointFromJson(value))
  return canonicalKeys(cx, endpointFromJson(expected)).includes(stored) ? solution : null
}

// --- structural patterns ----------------------------------------------------

/**
 * Which element kind owns each Core structural field (§8.2), and where the
 * rendered view keeps it.
 *
 * These are the fields the protocol defines rather than a Profile: they live in
 * typed columns, KML routes them apart from the generic `structural` map, and
 * `STRUCTURAL` reaches them by the same plain names the write path uses. A
 * Profile field is addressed by its resolved symbol and can therefore never
 * collide with one of these, which is what stops a Profile named `evidence`
 * from quietly redefining what an Assertion cites.
 */
const CORE_STRUCTURAL_FIELDS: Readonly<
  Record<string, { kind: ElementKind; view: string }>
> = {
  evidence: { kind: 'Assertion', view: 'evidence' },
  context: { kind: 'Assertion', view: 'context_refs' },
  source: { kind: 'Evidence', view: 'source' },
  generated_by: { kind: 'Evidence', view: 'generated_by' },
  inputs: { kind: 'Activity', view: 'inputs' },
  outputs: { kind: 'Activity', view: 'outputs' },
  associated_actors: { kind: 'Activity', view: 'associated_actors' },
}

/**
 * `STRUCTURAL (?src, "field", ?dst)` — record topology.
 *
 * Both planes, addressed the same way: a Profile structural field resolves
 * through the Schema Environment to a symbol, and a Core one (§8.2) is named
 * plainly. So "which Assertions cite this Evidence" is
 * `STRUCTURAL (?a, "evidence", :e)`, and it reads the same reverse index a
 * purge planner walks rather than a second enumeration that could disagree
 * with it.
 *
 * The two are never merged. A name is looked up on each plane independently,
 * and `?edge.field` carries the full symbol for a Profile field and the plain
 * name for a Core one, so a result says which plane it came from. A Profile
 * that declares a field named `evidence` therefore adds edges rather than
 * changing what an Assertion cites — and a caller that wants only the Profile
 * one addresses it by its full symbol.
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

  // Both planes are consulted, and a name resolves on each independently. In
  // the ordinary case exactly one answers, so this reads as one lookup. When
  // both do — a Profile that declared a field named `source` — the pattern
  // reports the edges of both rather than picking a winner, and `?edge.field`
  // says which plane each came from: the Core one plainly, the Profile one by
  // its full symbol. Silently preferring either would be the failure this
  // routing exists to prevent, in one direction or the other.
  const core = Object.hasOwn(CORE_STRUCTURAL_FIELDS, name)
    ? CORE_STRUCTURAL_FIELDS[name]!
    : null
  let symbol: ReturnType<Context['env']['resolveSymbol']> | null = null
  try {
    symbol = cx.env.resolveSymbol('StructuralField', name, 'read')
  } catch (err) {
    // A name that answers on neither plane is the caller's mistake, and the
    // schema layer's message is the one that says what to do about it.
    if (core === null) throw err
  }

  /** One structural plane, and how to read it. */
  interface Plane {
    /** What `?edge.field` reports: a plain Core name or a full symbol. */
    field: string
    /** How the reverse index spells it. */
    indexed: string
    /** Whether §17.4 gives this field a declared position. */
    ordered: boolean
    /**
     * The kind that can carry it.
     *
     * Two different uses, and they are not the same question. For an unbound
     * source it is the kind to scan — Profile fields are scanned over Concepts
     * because that is where they are declared in practice, and scanning one
     * kind is what keeps an unbound source from walking the whole Space. For a
     * *bound* source it filters only when `exclusive`: a Core field lives in a
     * column its owning kind alone has, but every element carries the generic
     * `structural` map, so an Assertion with a Profile field is a real answer.
     */
    holder: ElementKind
    /** Whether `holder` is the only kind that can carry this field at all. */
    exclusive: boolean
    /** Where a rendered view keeps it. */
    read: (view: JsonMap) => Json | undefined
  }

  const planes: Plane[] = []
  if (core !== null) {
    planes.push({
      field: name,
      // Core fields are recorded under their plain names; Profile ones are
      // prefixed, so a Profile `inputs` and an Activity's stay different edges.
      indexed: name,
      // A Core field declares no order — the write path appends rather than
      // honoring `AT` — so it reports none, and a caller is not invited to
      // treat storage order as a position.
      ordered: false,
      holder: core.kind,
      exclusive: true,
      read: (view) => view[core.view],
    })
  }
  if (symbol !== null) {
    const field = formatSymbolRef(symbol)
    const pkg = cx.env.definitionPackage(symbol)
    planes.push({
      field,
      indexed: `structural:${field}`,
      // §17.4: an ordered field exposes each reference's current position as
      // `?edge.index`; an unordered one exposes no index at all, so the member
      // reads null there rather than reporting a position the field lacks.
      ordered:
        (pkg === undefined ? undefined : structuralFieldDef(pkg, symbol.name))
          ?.ordered === true,
      // Every element carries the generic `structural` map, but only a Concept
      // is ever a Profile field's source in practice, and scanning one kind is
      // what keeps an unbound source from being a whole-Space walk.
      holder: 'Concept',
      exclusive: false,
      read: (view) =>
        isJsonMap(view.structural) ? view.structural[field] : undefined,
    })
  }

  // §43.7: the bound edge is *virtual* structural query state, explicitly "not
  // necessarily a durable Cognitive Element" — so it binds as the value it is,
  // describing the reference rather than standing in for a record Core does
  // not keep.
  const edge = (
    plane: Plane,
    source: string,
    target: Json,
    index: number | null,
  ): Json =>
    ({
      source: { id: source },
      field: plane.field,
      target,
      index: plane.ordered ? index : null,
    }) as Json

  const out: Solution[] = []
  if (cx.historical) {
    for (const plane of planes) {
      for (const solution of incoming) {
        const fixedSource = termEndpoint(clause.subject, solution, b)
        const fixedId =
          isJsonMap(fixedSource) && typeof fixedSource.id === 'string'
            ? tryParseElementId(fixedSource.id)
            : null
        const sources =
          fixedSource === null
            ? cx.reconstruct(plane.holder).map((element) => ({
                kind: element.kind,
                seq: element.row.id,
              } as ElementId))
            : fixedId === null || (plane.exclusive && fixedId.kind !== plane.holder)
              ? []
              : [fixedId]

        for (const src of sources) {
          const view = cx.view(src)
          const carried = view === null ? undefined : plane.read(view)
          // `generated_by` is single-cardinality and renders as one reference
          // rather than a list; a field with at most one edge is still a field.
          const references = Array.isArray(carried)
            ? carried
            : isJsonMap(carried)
              ? [carried as Json]
              : null
          if (references === null) continue
          for (const [position, reference] of references.entries()) {
            if (!isJsonMap(reference) || typeof reference.id !== 'string') continue
            const dst = tryParseElementId(reference.id)
            if (dst === null || cx.view(dst) === null) continue
            let current: Solution | null = solution
            current = bindTerm(current, clause.subject, { id: formatElementId(src) }, b)
            if (current === null) continue
            current = bindTerm(current, clause.object, reference as Json, b)
            if (current === null) continue
            if (clause.variable !== null) {
              current = extend(
                current,
                clause.variable,
                literalBinding(
                  edge(plane, formatElementId(src), reference as Json, position),
                ),
              )
              if (current === null) continue
            }
            out.push(current)
          }
        }
      }
    }
    return out
  }

  for (const plane of planes) {
    for (const solution of incoming) {
      const wheres = ['space = ?', 'field = ?']
      const values: SqlStorageValue[] = [cx.space, plane.indexed]
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
        .exec<{ from_id: string; to_id: string; ord: number }>(
          `SELECT from_id, to_id, ord FROM element_refs
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
          current = extend(
            current,
            clause.variable,
            literalBinding(edge(plane, row.from_id, { id: row.to_id } as Json, row.ord)),
          )
          if (current === null) continue
        }
        out.push(current)
      }
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
    // A tuple is matched the way a Proposition pattern is, and its bindings
    // travel with the belief: `?b BELIEF (?s, "prefers", ?o)` asks about every
    // object ?s prefers, and answering with one belief and an unbound ?o would
    // report a projection the caller cannot tell the subject of. One row per
    // Proposition it matched, each carrying the endpoints it matched them at.
    if ('Tuple' in clause.target) {
      const matched = propositions(
        cx,
        BELIEF_TARGET,
        { Tuple: clause.target.Tuple },
        [solution],
        b,
      )
      if (matched.length === 0) {
        // Only a *fully grounded* tuple earns the §46.4 answer. A tuple with
        // an unbound end asked about a family of slots, and "no Proposition"
        // there is an empty match, not one belief about nothing.
        if (!tupleIsGrounded(clause.target.Tuple, solution, b)) {
          throw errors.notFoundOrNotVisible(
            'the BELIEF tuple names no Proposition on record here',
          )
        }
        const next = extend(
          solution,
          clause.variable,
          literalBinding(
            beliefToJson(ungroundedBelief(cx, b.policy, nowTime())) as Json,
          ),
        )
        if (next !== null) out.push(next)
        continue
      }
      for (const row of matched) {
        const bound = row.get(BELIEF_TARGET)
        if (bound === undefined || bound.kind !== 'element') continue
        const carried: MutableSolution = new Map(row)
        carried.delete(BELIEF_TARGET)
        const next = extend(
          carried,
          clause.variable,
          literalBinding(beliefToJson(project(cx, bound.id, b.policy)) as Json),
        )
        if (next !== null) out.push(next)
      }
      continue
    }

    const target = beliefTarget(clause.target, solution, b)
    const next = extend(
      solution,
      clause.variable,
      literalBinding(beliefToJson(project(cx, target, b.policy)) as Json),
    )
    if (next !== null) out.push(next)
  }
  return out
}

/**
 * The Proposition a BELIEF clause names by identity.
 *
 * Only the two single-target forms — a bound Proposition variable and the
 * `(id: …)` form. A tuple is resolved by {@link belief} itself, which needs the
 * bindings the match made and not only the Proposition it landed on.
 */
function beliefTarget(
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
  // A tuple never reaches here: `belief` resolves one itself, because it needs
  // the bindings the match made and not only the Proposition it landed on.
  throw errors.internalError('a BELIEF tuple reached the single-target path')
}

/**
 * Whether every position of a tuple names something exactly (§46.3).
 *
 * This is what separates "the Proposition does not exist" from "the pattern did
 * not match": only a fully grounded tuple asks about one Proposition, so only a
 * fully grounded tuple can be answered with one belief about the Proposition
 * that is missing.
 */
function tupleIsGrounded(
  tuple: PropositionTriple,
  solution: Solution,
  b: ReadBindings,
): boolean {
  if (termEndpoint(tuple.subject, solution, b) === null) return false
  if (termEndpoint(tuple.object, solution, b) === null) return false
  // A traversal path is never grounded in this sense: §46.1 refuses a raw path
  // under BELIEF precisely because projection must not propagate belief along
  // one.
  if (!('Atom' in tuple.predicate)) return false
  return !('Variable' in tuple.predicate.Atom)
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
    const predicateLineage = resolveSymbol(cx, 'predicate', name)
    const keys = canonicalKeys(cx, endpointFromJson(subject))
    const validAt = cx.validAt
    checkProjectionHistory(cx, b.policy)
    const slot: Slot = {
      basis: projectionBasis(cx, b.policy, validAt),
      // §12.3: the slot sees every Assertion in it, whichever version its
      // Proposition was created under and whichever merged spelling of the
      // subject it was recorded on.
      candidates: slotPropositions(cx, keys, predicateLineage).map((id) =>
        project(cx, id, b.policy, validAt),
      ),
      policy: b.policy,
      validAt,
      asOf: cx.asOf ?? null,
      warnings: [
        'protected actor trust weights are applied; evidence quality is not automatically graded',
        'no evidence-quality evaluation is applied: a cited Evidence record is ' +
          'counted for its independence, never for how good it is',
      ],
    }
    const next = extend(
      solution,
      clause.variable,
      literalBinding(slotToJson(subject, predicateLineage, slot) as Json),
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

/**
 * A paging count: `LIMIT`, and the `FROM SEQ` / `TO SEQ` bounds of a
 * chronology.
 *
 * Its own reader rather than {@link readNumber}, because these are *counts*:
 * a `LIMIT "x"` coerced with `Number` becomes `NaN` and then silently pages
 * nothing — a mistyped command that answers instead of refusing. §102.28 puts
 * a scalar of the wrong type on `TypeMismatch`.
 *
 * Read-side only, and named apart from KML's `scalarNumber` / `scalarText` on
 * purpose: those take `(bindings, value, what)` because they resolve handles as
 * well as parameters, and one name over two argument orders is a swap the
 * compiler is the only thing catching.
 */
export function readCount(
  scalar: Scalar,
  b: ReadBindings,
  what: string,
): number {
  const value = scalarValue(scalar, b)
  if (typeof value !== 'number' || !Number.isInteger(value) || value < 0) {
    throw errors.typeMismatch(
      `${what} must be a non-negative integer, got ${JSON.stringify(value)}`,
    )
  }
  return value
}

/** A `parameter | literal` slot that must hold a number of any shape. */
export function readNumber(
  scalar: Scalar,
  b: ReadBindings,
  what: string,
): number {
  const value = scalarValue(scalar, b)
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw errors.typeMismatch(
      `${what} takes a number, got ${JSON.stringify(value)}`,
    )
  }
  return value
}

/** A `parameter | literal` slot that must hold a string. */
export function readText(
  scalar: Scalar,
  b: ReadBindings,
  what: string,
): string {
  const value = scalarValue(scalar, b)
  if (typeof value !== 'string') {
    throw errors.typeMismatch(
      `${what} takes a string, got ${JSON.stringify(value)}`,
    )
  }
  return value
}
