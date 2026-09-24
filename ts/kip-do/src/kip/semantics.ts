/**
 * Static checks the Core Package decides on its own (Spec §20.13).
 *
 * `kip://core@2.0.0` is a virtual Schema Package the Specification defines
 * itself: implicitly active in every Schema Environment, never deactivated,
 * never shadowed. Its registries therefore hold no matter which packages a
 * Space has installed, which makes them decidable before an engine is
 * involved.
 *
 * This is the TypeScript half of `rs/anda_kip/src/semantics.rs`, and the two
 * are kept identical on purpose: the same command must be refused by both
 * engines for the same reason, or a memory written against one deployment
 * fails against the other. `test/parser-oracle.test.ts` is what enforces that,
 * with `anda_kip` compiled to WASM as the reference.
 *
 * `@ldclabs/kip-lang` ships its own `analyzeSemantics`, which is deliberately
 * not used here. It is an editor-facing linter and makes three calls this
 * engine cannot:
 *
 * - it may judge which `TRANSITION … TO` state fits which target kind, but
 *   §52.5 makes that the engine's check (`InvalidLifecycleTransition`); only
 *   the state *vocabulary* is fixed by the language, and only that is checked
 *   here;
 * - SEARCH threshold validation runs at the engine boundary, after parameter
 *   binding; §66.4 requires both literal and bound thresholds in `[0,1]`;
 * - it enforces the Cognitive Memory Profile's `[0,1]` signals, which belong
 *   to a *package* rather than to the protocol. A Space running a different
 *   Profile may mean something else by `salience`, and only the active Schema
 *   Environment knows.
 *
 * Two boundaries, matching the Rust side exactly:
 *
 * - **only written literals are checked** — a `:parameter` is bound from the
 *   envelope at execution time, so nothing here can know its value;
 * - **only protocol-fixed vocabulary is checked** — `confidence` is `[0,1]`
 *   because §13.6 says so.
 */

import { KipError } from '../errors.js'
import type { Json, JsonMap } from '../json.js'
import { kipValue } from '../kml/value.js'
import { checkDraftDefinition } from '../schema/draft.js'
import type {
  Assignments,
  BoundValue,
  Command,
  KmlStatement,
  KqlQuery,
  MetaCommand,
  MutationClause,
  MutationValue,
  Scalar,
  StructuralEdge,
  WhereClause,
} from './ast.js'

/** `stance` — what an Assertion does with its Proposition (§13.4). */
export const STANCES = ['support', 'reject', 'uncertain'] as const

/** `mode` — how an Assertion was arrived at (§13.5). */
export const ASSERTION_MODES = [
  'observed',
  'stated',
  'inferred',
  'predicted',
  'hypothetical',
  'imported',
] as const

/** The Assertion lifecycle states (§14). */
export const ASSERTION_LIFECYCLE = [
  'active',
  'retracted',
  'superseded',
  'expired',
] as const

/** The Evidence lifecycle states (§57.2). */
export const EVIDENCE_LIFECYCLE = ['active', 'corrected'] as const

/** What an Evidence citation does for a claim (§56.2). */
export const EVIDENCE_ROLES = ['support', 'challenge', 'context'] as const

/** The Activity statuses (§16). */
export const ACTIVITY_STATUS = [
  'pending',
  'running',
  'completed',
  'failed',
  'cancelled',
] as const

/** The Activity terminal states (§16.6). */
export const ACTIVITY_TERMINAL = ['completed', 'failed', 'cancelled'] as const

/**
 * The states `TRANSITION ... TO` may name, in Spec §52.5 order.
 *
 * The same vocabulary `anda_kip::transition_state::ALL` fixes: the language
 * decides which words are states, and the engine decides which state fits
 * which kind and which current state a move is legal from.
 */
export const TRANSITION_STATES = [
  'retracted',
  'superseded',
  'corrected',
  'running',
  'completed',
  'failed',
  'cancelled',
  'archived',
  'tombstoned',
] as const

/** The moves that name the replacing element with `BY` (§52.5). */
export const TRANSITION_WITH_BY = ['superseded', 'corrected'] as const

/**
 * The Activity status moves: the only ones that may finalize fields or
 * topology in the same statement (§52.5).
 */
export const TRANSITION_ACTIVITY = [
  'running',
  'completed',
  'failed',
  'cancelled',
] as const

/** The belief statuses an Epistemic Projection can return (§21.3). */
export const BELIEF_STATUSES = [
  'accepted',
  'rejected',
  'contested',
  'uncertain',
  'insufficient',
] as const

/** The baseline SEARCH modes (§66.3). */
export const SEARCH_MODES = ['keyword', 'semantic', 'hybrid'] as const

/** The `DESCRIBE PRIMER` modes (§64). */
export const PRIMER_MODES = ['compact', 'full'] as const

/** The explanation levels `WITH EPISTEMIC` accepts (§49.1). */
export const EXPLANATION_LEVELS = ['none', 'summary', 'ledger'] as const

/** How much a diagnostic matters. */
export type Severity = 'error' | 'warning'

/** One finding about a parsed command. */
export interface Diagnostic {
  severity: Severity
  /** The registry code it would be reported under. */
  code: 'ConstraintViolation' | 'ResultLimitExceeded'
  message: string
}

/**
 * Reports everything the Core Package can decide about a parsed command.
 *
 * Warnings are included, so this is the entry point for a tool that shows
 * findings rather than rejecting; {@link checkSemantics} is the one that
 * rejects.
 */
export function analyzeSemantics(command: Command): Diagnostic[] {
  const out: Diagnostic[] = []
  if ('Kql' in command) analyzeKql(command.Kql, out)
  else if ('Kml' in command) analyzeKml(command.Kml, out)
  else if ('Meta' in command) analyzeMeta(command.Meta, out)
  return out
}

/**
 * Throws on the first `error` finding, ignoring warnings.
 *
 * This is what the parser runs, which is why a command whose `stance` is
 * misspelled is rejected here rather than half-way through a transaction.
 */
export function checkSemantics(command: Command): void {
  const fatal = analyzeSemantics(command).find((d) => d.severity === 'error')
  if (fatal) throw new KipError(fatal.code, fatal.message)
}

// ---------------------------------------------------------------------------
// Value inspection
// ---------------------------------------------------------------------------

/**
 * The written string in a value position, or `undefined` when there is nothing
 * to check: a `:parameter` is bound at execution time, and a non-string value
 * is a type error the Schema layer reports.
 */
function literalStr(value: MutationValue | undefined): string | undefined {
  if (!value || !('Value' in value)) return undefined
  const inner = value.Value
  return typeof inner === 'object' && inner !== null && 'String' in inner
    ? inner.String
    : undefined
}

function literalNum(value: MutationValue | undefined): number | undefined {
  if (!value || !('Value' in value)) return undefined
  const inner = value.Value
  return typeof inner === 'object' && inner !== null && 'Number' in inner
    ? inner.Number
    : undefined
}

function scalarStr(scalar: Scalar | null | undefined): string | undefined {
  if (!scalar || !('Literal' in scalar)) return undefined
  const inner = scalar.Literal
  return typeof inner === 'object' && inner !== null && 'String' in inner
    ? inner.String
    : undefined
}

/** A `BoundValue`'s written string, for structural-edge options. */
function boundStr(value: unknown): string | undefined {
  if (!value || typeof value !== 'object' || !('Value' in value)) {
    return undefined
  }
  const inner = (value as { Value: unknown }).Value
  return typeof inner === 'object' && inner !== null && 'String' in inner
    ? (inner as { String: string }).String
    : undefined
}

function checkEnum(
  written: string | undefined,
  allowed: readonly string[],
  label: string,
  out: Diagnostic[],
): void {
  if (written === undefined || allowed.includes(written)) return
  out.push({
    severity: 'error',
    code: 'ConstraintViolation',
    message: `${label} must be one of ${allowed.join(' | ')}, found ${JSON.stringify(written)}`,
  })
}

function checkUnitInterval(
  value: number | undefined,
  label: string,
  out: Diagnostic[],
): void {
  if (value === undefined || (value >= 0 && value <= 1)) return
  out.push({
    severity: 'error',
    code: 'ConstraintViolation',
    message: `${label} must be within [0, 1], found ${value}`,
  })
}

// ---------------------------------------------------------------------------
// KML
// ---------------------------------------------------------------------------

function analyzeKml(statement: KmlStatement, out: Diagnostic[]): void {
  for (const clause of statement.clauses) analyzeClause(clause, out)
}

/**
 * A bound object as JSON, when every value in it is written; `undefined` when
 * a `:parameter` anywhere leaves it for the engine.
 */
function literalObject(object: Record<string, BoundValue>): JsonMap | undefined {
  const value = (bound: BoundValue): Json | undefined => {
    if ('Value' in bound) return kipValue(bound.Value)
    if ('Array' in bound) {
      const items = bound.Array.map(value)
      return items.includes(undefined) ? undefined : (items as Json[])
    }
    if ('Object' in bound) return fields(bound.Object)
    return undefined
  }
  const fields = (entries: [string, BoundValue][]): JsonMap | undefined => {
    const out: JsonMap = {}
    for (const [key, item] of entries) {
      const json = value(item)
      if (json === undefined) return undefined
      out[key] = json
    }
    return out
  }
  return fields(Object.entries(object))
}

function analyzeClause(clause: MutationClause, out: Diagnostic[]): void {
  // `ASSERT` has already been desugared into `CreateAssertion` by `lower`, so
  // checking the created Assertion covers the sugar form too.
  if ('CreateAssertion' in clause) {
    const record = clause.CreateAssertion
    if (record.set_fields) {
      analyzeAssignments(record.set_fields, out)
      analyzeAssertionShape(record.set_fields, record.set_structural, out)
    }
    analyzeStructural(record.set_structural, out)
    return
  }
  // A fully written DEFINE body is judged here (§20.16); a body with a
  // parameter is judged by the engine once it is bound, and whether the name
  // already resolves depends on the Schema Environment.
  if ('Define' in clause) {
    const body = literalObject(clause.Define.definition)
    if (body !== undefined) {
      try {
        checkDraftDefinition(clause.Define.kind, body)
      } catch (err) {
        out.push({ severity: 'error', code: 'ConstraintViolation', message: (err as Error).message })
      }
    }
    return
  }
  if ('Update' in clause) {
    const update = clause.Update
    for (const action of update.actions) {
      if ('SetFields' in action) analyzeAssignments(action.SetFields, out)

    }
    warnUnbounded('UPDATE', !!update.where_clauses, !!update.limit, out)
    return
  }
  // §52.5 fixes the state vocabulary the statement may name; which states
  // fit which target kind, and which current state a move is legal from, is
  // the engine's check (`InvalidLifecycleTransition`). Whether `BY` and
  // `SET` fit the state is the grammar's, for a literal state — `lower`
  // refuses `TO "retracted" BY :b` — and the engine's for a `:parameter`.
  if ('Transition' in clause) {
    const transition = clause.Transition
    checkEnum(
      scalarStr(transition.to),
      TRANSITION_STATES,
      'TRANSITION ... TO',
      out,
    )
    warnUnbounded(
      'TRANSITION',
      !!transition.where_clauses,
      !!transition.limit,
      out,
    )
    return
  }
  if ('SetRetention' in clause) {
    const retention = clause.SetRetention
    warnUnbounded(
      'SET RETENTION',
      !!retention.where_clauses,
      !!retention.limit,
      out,
    )
    return
  }
  if ('Purge' in clause) {
    warnUnbounded(
      'PURGE',
      !!clause.Purge.where_clauses,
      !!clause.Purge.limit,
      out,
    )
    return
  }
  if ('PurgePayload' in clause) {
    // §60.5 names PURGE PAYLOAD alongside PURGE: byte destruction over an
    // unbounded WHERE is exactly the sweep that must not run by accident.
    warnUnbounded(
      'PURGE PAYLOAD',
      !!clause.PurgePayload.where_clauses,
      !!clause.PurgePayload.limit,
      out,
    )
  }
}

/**
 * Core fields only; attribute and Facet members belong to their Schema.
 */
function analyzeAssignments(
  assignments: Assignments,
  out: Diagnostic[],
): void {
  for (const [field, value] of assignments) {
    if (field === 'stance') checkEnum(literalStr(value), STANCES, 'stance', out)
    else if (field === 'mode') {
      checkEnum(literalStr(value), ASSERTION_MODES, 'mode', out)
    } else if (field === 'confidence') {
      checkUnitInterval(literalNum(value), 'confidence', out)
    }
  }
}

/**
 * `role` on an `("evidence", …)` citation comes from the Core registry
 * (§56.2). Options on any other structural field are package-defined, and only
 * the Schema Environment can judge those.
 */
function analyzeStructural(
  edges: StructuralEdge[] | null | undefined,
  out: Diagnostic[],
): void {
  for (const edge of edges ?? []) {
    if (!('Name' in edge.field) || edge.field.Name !== 'evidence') continue
    const role = edge.options?.role
    if (role !== undefined) {
      checkEnum(boundStr(role), EVIDENCE_ROLES, 'an Evidence citation role', out)
    }
  }
}

/**
 * An observation that cites nothing is a valid Assertion, but it is the shape
 * a forgotten citation takes: `mode: "observed"` claims the actor saw it, and
 * what they saw is exactly what Evidence records.
 */
function analyzeAssertionShape(
  fields: Assignments,
  structural: StructuralEdge[] | null | undefined,
  out: Diagnostic[],
): void {
  const citesEvidence = (structural ?? []).some(
    (edge) => 'Name' in edge.field && edge.field.Name === 'evidence',
  )
  // A claim taken from captured material was made when the source says, not
  // when this write runs. Without `at` or a written `valid.from` it takes the
  // transaction time as its start key (§13.2, §25.4), so an old claim recorded
  // late would end a current value it predates. A parameter may carry either,
  // so it is given the benefit of the doubt.
  const isNull = (value: MutationValue): boolean => 'Value' in value && value.Value === 'Null'
  const written = (name: string): boolean =>
    fields.some(([field, value]) => field === name && !isNull(value))
  const writesFrom = fields.some(([field, value]) => {
    if (field !== 'valid_time') return false
    if ('Value' in value) {
      const inner = value.Value
      return typeof inner === 'object' && 'Object' in inner &&
        inner.Object.from !== undefined && inner.Object.from !== 'Null'
    }
    if ('Object' in value) {
      return value.Object.some(([key, item]) => key === 'from' && !('Value' in item && item.Value === 'Null'))
    }
    return true
  })
  if (citesEvidence && !written('asserted_at') && !writesFrom) {
    out.push({
      severity: 'warning',
      code: 'ConstraintViolation',
      message:
        "ASSERT cites evidence but gives no at: asserted_at defaults to the transaction time, which is the claim's start key; a claim recorded later than it was made should carry at: <the source's observed time> (§13.2, §25.4)",
    })
  }
  const observed = fields.some(
    ([name, value]) => name === 'mode' && literalStr(value) === 'observed',
  )
  if (observed && !citesEvidence) {
    out.push({
      severity: 'warning',
      code: 'ConstraintViolation',
      message:
        'mode: "observed" without evidence: an observation normally cites the artifact it was observed from',
    })
  }
}

/**
 * Spec §52.7 names the statements whose `WHERE` can select an unbounded set. A
 * statement that names its target directly is already bounded to one element,
 * so only the pattern-selecting form is worth warning about.
 */
function warnUnbounded(
  statement: string,
  hasWhere: boolean,
  hasLimit: boolean,
  out: Diagnostic[],
): void {
  if (hasWhere && !hasLimit) {
    out.push({
      severity: 'warning',
      code: 'ResultLimitExceeded',
      message: `${statement} selects by pattern without a LIMIT: the match set is unbounded, and an over-broad one cannot be undone`,
    })
  }
}

// ---------------------------------------------------------------------------
// KQL
// ---------------------------------------------------------------------------

function analyzeKql(query: KqlQuery, out: Diagnostic[]): void {
  const explanation = query.epistemic?.explanation
  if (explanation !== undefined) {
    checkEnum(
      boundStr(explanation),
      EXPLANATION_LEVELS,
      'WITH EPISTEMIC explanation',
      out,
    )
  }
  analyzePatterns(query.where_clauses, out)
  if (!query.limit) {
    out.push({
      severity: 'warning',
      code: 'ResultLimitExceeded',
      message:
        'FIND without a LIMIT: an unbounded recall returns whatever the Space happens to hold',
    })
  }
}

/**
 * A Search Pattern is the META `SEARCH` bound into a query (§43.8), so its
 * `MODE` answers to the same registry wherever in the block it sits.
 */
function analyzePatterns(clauses: readonly WhereClause[], out: Diagnostic[]): void {
  for (const clause of clauses) {
    if ('Search' in clause) checkSearchMode(clause.Search.mode, out)
    else if ('Not' in clause) analyzePatterns(clause.Not, out)
    else if ('Optional' in clause) analyzePatterns(clause.Optional, out)
    else if ('Union' in clause) analyzePatterns(clause.Union, out)
  }
}

function checkSearchMode(mode: Scalar | null | undefined, out: Diagnostic[]): void {
  checkEnum(scalarStr(mode), SEARCH_MODES, 'SEARCH MODE', out)
}

// ---------------------------------------------------------------------------
// META
// ---------------------------------------------------------------------------

function analyzeMeta(meta: MetaCommand, out: Diagnostic[]): void {
  // `THRESHOLD` is compared against a retrieval score whose semantics the
  // engine declares (§66.5) rather than inherits, so it carries no
  // protocol-fixed range to check it against.
  if ('Search' in meta) {
    checkSearchMode(meta.Search.mode, out)
    return
  }
  if ('Describe' in meta) {
    const target = meta.Describe
    if (typeof target === 'object' && 'Primer' in target) {
      checkEnum(
        scalarStr(target.Primer.mode),
        PRIMER_MODES,
        'DESCRIBE PRIMER MODE',
        out,
      )
    }
  }
}
