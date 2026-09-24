/** Block-local admission missing from the toolkit's current lowering. */
import { errors } from '../errors.js'
import type {
  Assignments, BoundValue, Command, ElementRef, MatchValue, MutationValue,
  ObjectMatcher, PropositionMatcher, StructuralEdge, Term, WhereClause,
} from './ast.js'

function term(value: Term, names: Set<string>): void {
  if ('Variable' in value) names.add(value.Variable)
  else if ('Match' in value) matcher(value.Match, names)
  else if ('Proposition' in value) proposition(value.Proposition, names)
}
function proposition(value: PropositionMatcher, names: Set<string>): void {
  if (!('Tuple' in value)) return
  term(value.Tuple.subject, names)
  term(value.Tuple.object, names)
  const predicate = value.Tuple.predicate
  if ('Atom' in predicate && 'Variable' in predicate.Atom) names.add(predicate.Atom.Variable)
  else if ('Path' in predicate) for (const atom of predicate.Path) {
    if ('Variable' in atom.predicate) names.add(atom.predicate.Variable)
  }
}
function matcher(value: ObjectMatcher, names: Set<string>): void {
  for (const member of Object.values(value)) matchValue(member, names)
}
function matchValue(value: MatchValue, names: Set<string>): void {
  if ('Variable' in value) names.add(value.Variable)
  else if ('Array' in value) for (const item of value.Array) matchValue(item, names)
  else if ('Match' in value) matcher(value.Match, names)
  else if ('Proposition' in value) proposition(value.Proposition, names)
}
function where(clauses: WhereClause[], names: Set<string>): void {
  for (const clause of clauses) {
    if ('Not' in clause || 'Filter' in clause) continue
    if ('Optional' in clause) { where(clause.Optional, names); continue }
    if ('Union' in clause) { where(clause.Union, names); continue }
    if ('Concept' in clause || 'Assertion' in clause || 'Evidence' in clause || 'Activity' in clause) {
      const body = 'Concept' in clause ? clause.Concept : 'Assertion' in clause ? clause.Assertion : 'Evidence' in clause ? clause.Evidence : clause.Activity
      names.add(body.variable); matcher(body.matcher, names)
    } else if ('Proposition' in clause) {
      if (clause.Proposition.variable !== null) names.add(clause.Proposition.variable)
      proposition(clause.Proposition.matcher, names)
    } else if ('Structural' in clause) {
      if (clause.Structural.variable !== null) names.add(clause.Structural.variable)
      term(clause.Structural.subject, names); term(clause.Structural.object, names)
    }
    // BELIEF and a Search Pattern never reach a mutation WHERE (§43.8).
  }
}
function bound(value: BoundValue | MutationValue, names: Set<string>): void {
  if ('Handle' in value) names.add(value.Handle)
  else if ('Array' in value) for (const item of value.Array) bound(item, names)
  else if ('Object' in value) for (const [, item] of value.Object) bound(item, names)
}
function assignments(values: Assignments | null, names: Set<string>): void {
  for (const [, value] of values ?? []) bound(value, names)
}
function edges(values: StructuralEdge[] | null, names: Set<string>): void {
  for (const edge of values ?? []) {
    bound(edge.value, names)
    for (const value of Object.values(edge.options ?? {})) bound(value, names)
  }
}
function element(value: ElementRef, names: Set<string>): void {
  if ('Handle' in value) names.add(value.Handle)
}

export function checkMutationHandles(command: Command): void {
  if (!('Kml' in command)) return
  const clauses = command.Kml.clauses
  const outputs = new Set<string>()
  for (const clause of clauses) {
    const body = Object.values(clause)[0]
    if ('handle' in body && typeof body.handle === 'string') outputs.add(body.handle)
  }
  for (const clause of clauses) {
    const body = Object.values(clause)[0]
    const local = new Set<string>()
    if ('where_clauses' in body && body.where_clauses) where(body.where_clauses, local)
    const references = new Set<string>()
    if ('CreateConcept' in clause || 'UpsertConcept' in clause || 'CreateAssertion' in clause || 'CreateEvidence' in clause || 'CreateActivity' in clause) {
      const c = 'CreateConcept' in clause ? clause.CreateConcept : 'UpsertConcept' in clause ? clause.UpsertConcept : 'CreateAssertion' in clause ? clause.CreateAssertion : 'CreateEvidence' in clause ? clause.CreateEvidence : clause.CreateActivity
      assignments(c.set_fields, references)
      if ('set_attributes' in c) assignments(c.set_attributes, references)
      for (const facet of c.set_facets) assignments(facet.values, references)
      edges(c.set_structural, references)
      if ('unset_structural' in c) for (const removal of c.unset_structural ?? []) bound(removal.value, references)
    } else if ('EnsureProposition' in clause) {
      term(clause.EnsureProposition.subject, references)
      term(clause.EnsureProposition.object, references)
    } else if ('Update' in clause) {
      element(clause.Update.target, references)
      for (const action of clause.Update.actions) {
        if ('SetFields' in action) assignments(action.SetFields, references)
        else if ('SetAttributes' in action) assignments(action.SetAttributes, references)
        else if ('SetFacet' in action) assignments(action.SetFacet.values, references)
        else if ('SetStructural' in action) edges(action.SetStructural, references)
        else if ('UnsetStructural' in action) for (const removal of action.UnsetStructural) bound(removal.value, references)
      }
    } else if ('Transition' in clause) {
      const c = clause.Transition
      element(c.target, references)
      if (c.by) element(c.by, references)
      assignments(c.set_fields, references); edges(c.set_structural, references)
    } else if ('SetRetention' in clause) {
      element(clause.SetRetention.target, references)
      assignments(clause.SetRetention.values, references)
    } else if ('Purge' in clause) element(clause.Purge.target, references)
    else if ('PurgePayload' in clause) element(clause.PurgePayload.target, references)
    else if ('MergeConcept' in clause) { element(clause.MergeConcept.source, references); element(clause.MergeConcept.into, references) }
    for (const name of references) {
      if (!outputs.has(name) && !local.has(name)) throw errors.referenceError(`?${name} is not bound by this mutation's outputs or WHERE`)
    }
  }
}
