/** Validation that does not depend on a branch producing runtime solutions. */
import { errors } from '../errors.js'
import type { DotPathVar, KqlQuery, ObjectMatcher, PredAtom, PropositionMatcher, Term, WhereClause } from '../kip/ast.js'
import type { Context } from './context.js'
import { validateFilter } from './filter.js'
import { kipLiteral, parameterValue, type ReadBindings } from './matching.js'

export function validatePath(cx: Context, value: DotPathVar, scope: ReadonlySet<string>): void {
  if (!scope.has(value.var)) throw errors.invalidSyntax(`?${value.var} has no visible pattern binding site`)
  // Only the element's root symbol maps select Schema names. Attribute
  // objects may use arbitrary keys named "facets" or "structural" themselves.
  if (value.path.length < 2) return
  const root = value.path[0]!
  const field = 'Field' in root ? root.Field : root.Key
  const step = value.path[1]!
  const name = 'Field' in step ? step.Field : step.Key
  if (field === 'facets') cx.env.resolveSymbol('Facet', name, 'read')
  if (field === 'structural' && !CORE_STRUCTURAL.has(name)) cx.env.resolveSymbol('StructuralField', name, 'read')
}

const CORE_STRUCTURAL = new Set(['evidence', 'context', 'source', 'generated_by', 'inputs', 'outputs', 'associated_actors'])

export function validateQuery(cx: Context, query: KqlQuery, b: ReadBindings): void {
  const scope = validateWhere(cx, query.where_clauses, b)
  for (const expression of query.find_clause.expressions) validatePath(cx, 'Variable' in expression ? expression.Variable : expression.Aggregation.var, scope)
  const grouped = query.find_clause.expressions.some((e) => 'Aggregation' in e)
  for (const item of query.order_by ?? []) {
    validatePath(cx, item.variable, scope)
    const projected = query.find_clause.expressions.some((e) => {
      const path = 'Variable' in e ? e.Variable : e.Aggregation.var
      return JSON.stringify(path) === JSON.stringify(item.variable) && (item.aggregation === null ? 'Variable' in e : 'Aggregation' in e && e.Aggregation.func === item.aggregation && e.Aggregation.distinct === (item.distinct ?? false))
    })
    if ((grouped || item.aggregation !== null) && !projected) throw errors.invalidSyntax('ORDER BY must name projected columns when grouping or sorting by an aggregate')
  }
}

/** NOT-local names never leave their block; UNION arms have independent sites. */
export function validateWhere(cx: Context, clauses: readonly WhereClause[], b: ReadBindings, inherited: ReadonlySet<string> = new Set(b.ambient?.keys())): Set<string> {
  const scope = new Set(inherited)
  const pred = (atom: PredAtom): void => {
    if ('Variable' in atom) { scope.add(atom.Variable); return }
    const name = 'Literal' in atom ? atom.Literal : parameterValue(b, atom.Param)
    if (typeof name !== 'string') throw errors.typeMismatch('a predicate must be a symbol string')
    cx.env.resolveSymbol('PredicateType', name, 'read')
  }
  const match = (matcher: ObjectMatcher, root = true, symbolMap?: 'Facet' | 'StructuralField'): void => {
    const value = (expected: import('../kip/ast.js').MatchValue, nestedMap?: 'Facet' | 'StructuralField'): void => {
      if ('Variable' in expected) scope.add(expected.Variable)
      else if ('Param' in expected) parameterValue(b, expected.Param)
      else if ('Match' in expected) match(expected.Match, false, nestedMap)
      else if ('Array' in expected) expected.Array.forEach((item) => value(item))
      else if ('Proposition' in expected) proposition(expected.Proposition)
    }
    for (const [field, expected] of Object.entries(matcher)) {
      if (symbolMap !== undefined && !(symbolMap === 'StructuralField' && CORE_STRUCTURAL.has(field))) cx.env.resolveSymbol(symbolMap, field, 'read')
      value(expected, root && field === 'facets' ? 'Facet' : root && field === 'structural' ? 'StructuralField' : undefined)
      const literal = 'Param' in expected ? parameterValue(b, expected.Param) : 'Literal' in expected ? kipLiteral(expected.Literal) : undefined
      if (root && (field === 'type' || field === 'schema_ref' || field === 'predicate')) {
        if (literal !== undefined && typeof literal !== 'string') throw errors.typeMismatch(`${field} must be a symbol string`)
        if (typeof literal === 'string') cx.env.resolveSymbol(field === 'predicate' ? 'PredicateType' : 'ConceptType', literal, 'read')
      }
      if (root && (field === 'id' || field === 'canonical_id') && literal !== undefined && typeof literal !== 'string') throw errors.typeMismatch(`${field} must be a string`)
    }
  }
  const proposition = (matcher: PropositionMatcher): void => {
    if ('Id' in matcher) {
      if ('Param' in matcher.Id) parameterValue(b, matcher.Id.Param)
      return
    }
    const { subject, predicate, object } = matcher.Tuple
    if ('Path' in predicate) {
      if (predicate.Path.some((part) => 'Variable' in part.predicate)) throw errors.invalidSyntax('predicate variables cannot have quantifiers or alternatives')
      for (const part of predicate.Path) {
        if (part.hops && (!Number.isSafeInteger(part.hops.min) || part.hops.min < 0 || (part.hops.max !== null && (!Number.isSafeInteger(part.hops.max) || part.hops.max < part.hops.min)))) throw errors.invalidSyntax('invalid path hop range')
        pred(part.predicate)
      }
      throw errors.unsupportedCapability('a predicate path or hop quantifier is not implemented by this engine yet; see DESCRIBE CAPABILITIES')
    }
    pred(predicate.Atom)
    term(subject)
    term(object)
  }
  const term = (value: Term): void => {
    if ('Variable' in value) scope.add(value.Variable)
    else if ('Param' in value) parameterValue(b, value.Param)
    else if ('Match' in value) match(value.Match)
    else if ('Proposition' in value) proposition(value.Proposition)
  }
  for (const clause of clauses) {
    if ('Not' in clause) validateWhere(cx, clause.Not, b, scope)
    else if ('Optional' in clause || 'Union' in clause) {
      const next = 'Optional' in clause ? validateWhere(cx, clause.Optional, b, scope) : validateWhere(cx, clause.Union, b)
      for (const name of next) scope.add(name)
    } else if ('Filter' in clause) {
      validateFilter(cx, clause.Filter.expression, b, (value) => validatePath(cx, value, scope))
    } else if ('Proposition' in clause) {
      if (clause.Proposition.variable !== null) scope.add(clause.Proposition.variable)
      proposition(clause.Proposition.matcher)
    } else if ('Structural' in clause) {
      const pattern = clause.Structural
      if (pattern.variable !== null) scope.add(pattern.variable)
      term(pattern.subject)
      term(pattern.object)
      const name = 'Name' in pattern.field ? pattern.field.Name : parameterValue(b, pattern.field.Param)
      if (typeof name !== 'string') throw errors.typeMismatch('a structural field must be a symbol string')
      if (!CORE_STRUCTURAL.has(name)) cx.env.resolveSymbol('StructuralField', name, 'read')
    } else if ('Belief' in clause) {
      const { variable, target } = clause.Belief
      if ('Proposition' in target && !scope.has(target.Proposition)) throw errors.projectionTargetUnbound(`?${target.Proposition} is not bound to a Proposition before BELIEF`)
      if ('Tuple' in target) proposition(target)
      if ('Id' in target && 'Param' in target.Id) parameterValue(b, target.Id.Param)
      scope.add(variable)
    } else if ('BeliefSlot' in clause) {
      const pattern = clause.BeliefSlot
      if ('Variable' in pattern.subject && !scope.has(pattern.subject.Variable)) throw errors.projectionTargetUnbounded('BELIEF SLOT needs a bound subject')
      if ('Variable' in pattern.predicate) throw errors.projectionTargetUnbounded('BELIEF SLOT needs an exact predicate')
      term(pattern.subject)
      pred(pattern.predicate)
      scope.add(pattern.variable)
    } else if ('Search' in clause) {
      // §43.8: a Search Pattern binds its hits; its term and modifiers are
      // read when it runs, where a wrong type is reported.
      scope.add(clause.Search.variable)
    } else {
      const pattern = 'Concept' in clause ? clause.Concept : 'Assertion' in clause ? clause.Assertion : 'Evidence' in clause ? clause.Evidence : clause.Activity
      scope.add(pattern.variable)
      match(pattern.matcher)
    }
  }
  return scope
}
