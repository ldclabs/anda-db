import { validateValue } from './contracts.js'
/**
 * # Package validation
 *
 * Core validation and package validation are different layers (Spec §91–§93),
 * and this module is only the second one: attribute shape, Literal datatype,
 * Facet shape, structural cardinality. Element shape, same-Space closure and
 * `_system` protection are Core's, enforced in the store and the mutation
 * layer, and a package cannot weaken them (§18.2).
 *
 * ## What must *not* be rejected here
 *
 * A `functional` predicate says one subject has at most one true object. It is
 * an **epistemic** statement, so two competing objects are a contested belief —
 * something the Nexus has to be able to store in order to report it (§95,
 * §25.1). Turning it into a write rejection would mean the system could never
 * record disagreement, which is most of what a memory system is for.
 *
 * Likewise `open_world: true` means an absent claim is unknown, not false.
 * Nothing here infers falsity from absence.
 *
 * ## Severity
 *
 * Only deterministic declared constraints produce errors (§98). Model hints are
 * advisory and never become hard validators (§20.5), so nothing in
 * `model_hints` is read by this module at all.
 */

import { errors } from '../errors.js'
import { canonicalJson, jsonEquals, type Json, type JsonMap } from '../json.js'
import type {
  AttributeSpec,
  EndpointSpec,
  FacetDef,
  FieldSpec,
  PredicateDef,
  StructuralFieldDef,
} from './package.js'
import { isUnconstrained, literalTypesOf } from './package.js'
import type { ElementKind } from '../id.js'
import type { Endpoint } from '../term.js'
import { normalizeTime } from '../time.js'

/** How much a violation matters (Spec §98). */
export type Severity = 'error' | 'warning' | 'info'

/** One validation finding (Spec §97). */
export interface Violation {
  /** A stable code. */
  code: string
  /** The symbol whose contract was broken. */
  schema_ref: string
  /** Where in the element the problem is, e.g. `attributes.display_name`. */
  path: string
  message: string
  severity: Severity
}

/** The result of validating one element against its declared schema. */
export class Validation {
  /** Findings at `error` severity. */
  readonly violations: Violation[] = []
  /** Findings below that. */
  readonly warnings: Violation[] = []

  get valid(): boolean {
    return this.violations.length === 0
  }

  /** Records a finding, routing it by severity. */
  push(violation: Violation): this {
    if (violation.severity === 'error') this.violations.push(violation)
    else this.warnings.push(violation)
    return this
  }

  /** Merges another result into this one. */
  extend(other: Validation): this {
    this.violations.push(...other.violations)
    this.warnings.push(...other.warnings)
    return this
  }

  toJSON(): { violations: Violation[]; warnings: Violation[] } {
    return { violations: this.violations, warnings: this.warnings }
  }

  /** Turns a failed validation into the error a caller sees. */
  throwIfInvalid(): this {
    if (this.valid) return this
    const summary = this.violations
      .map((v) => `${v.path}: ${v.message}`)
      .join('; ')
    throw errors.constraintViolation(summary, this.toJSON() as unknown as Json)
  }
}

function error(
  code: string,
  schemaRef: string,
  path: string,
  message: string,
): Violation {
  return { code, schema_ref: schemaRef, path, message, severity: 'error' }
}

/**
 * Whether a value satisfies one declared validation type (§35).
 *
 * The `type` slot is either a name or a list of accepted names, and a list is a
 * union: `["timestamp", "null"]` is how the shipped profile spells a nullable
 * field.
 */
function matchesType(declared: Json | undefined, value: Json): boolean {
  if (declared === undefined || declared === null) return true
  if (typeof declared === 'string') return matchesTypeName(declared, value)
  if (Array.isArray(declared)) {
    return declared.some((name) => matchesType(name, value))
  }
  return false
}

function matchesTypeName(name: string, value: Json): boolean {
  switch (name) {
    case 'any':
      return true
    case 'string':
      return typeof value === 'string'
    case 'number':
      return typeof value === 'number'
    case 'integer':
      return typeof value === 'number' && Number.isInteger(value)
    case 'boolean':
      return typeof value === 'boolean'
    case 'array':
      return Array.isArray(value)
    case 'object':
      return typeof value === 'object' && value !== null && !Array.isArray(value)
    case 'null':
      return value === null
    // A timestamp is carried as a string; its shape is checked where it is
    // normalized, so that one parser decides what a timestamp is.
    case 'timestamp':
      return typeof value === 'string'
    // An unrecognized type name is a package this engine does not fully
    // understand. Accepting the value is the conservative reading: it cannot be
    // validated, and inventing a failure would reject data on the strength of a
    // name the engine simply has not implemented.
    default:
      return true
  }
}

function typeName(declared: Json | undefined): string {
  if (declared === undefined) return 'any'
  if (typeof declared === 'string') return declared
  if (Array.isArray(declared)) return declared.map(typeName).join(' or ')
  return canonicalJson(declared)
}

function jsonKind(value: Json): string {
  if (value === null) return 'null'
  if (Array.isArray(value)) return 'array'
  return typeof value
}

/** Validates one declared field's value. */
function validateField(
  schemaRef: string,
  path: string,
  spec: FieldSpec,
  value: Json,
  into: Validation,
): void {
  if (spec.value_schema !== undefined) {
    try { validateValue(spec.value_schema, value) } catch (err) {
      into.push(error('SCHEMA_VALUE_NOT_ALLOWED', schemaRef, path, String(err)))
    }
  }
  if (!matchesType(spec.type, value)) {
    into.push(
      error(
        'SCHEMA_TYPE_MISMATCH',
        schemaRef,
        path,
        `expected ${typeName(spec.type)}, got ${jsonKind(value)}`,
      ),
    )
    // A value of the wrong type cannot meaningfully be range-checked.
    return
  }
  if (typeof value === 'number') {
    if (spec.minimum !== undefined && value < spec.minimum) {
      into.push(
        error(
          'SCHEMA_RANGE_VIOLATION',
          schemaRef,
          path,
          `${value} is below the declared minimum ${spec.minimum}`,
        ),
      )
    }
    if (spec.maximum !== undefined && value > spec.maximum) {
      into.push(
        error(
          'SCHEMA_RANGE_VIOLATION',
          schemaRef,
          path,
          `${value} is above the declared maximum ${spec.maximum}`,
        ),
      )
    }
  }
  if (spec.enum !== undefined && !spec.enum.some((v) => jsonEquals(v, value))) {
    into.push(
      error(
        'SCHEMA_VALUE_NOT_ALLOWED',
        schemaRef,
        path,
        `${canonicalJson(value)} is not one of the declared values`,
      ),
    )
  }
}

/** Validates a field map against a declared field set. */
function validateFields(
  schemaRef: string,
  prefix: string,
  open: boolean,
  declared: Record<string, FieldSpec>,
  values: JsonMap,
  into: Validation,
): void {
  for (const [name, spec] of Object.entries(declared)) {
    const path = `${prefix}.${name}`
    if (Object.hasOwn(values, name)) {
      validateField(schemaRef, path, spec, values[name] as Json, into)
    } else if (spec.required === true) {
      into.push(
        error(
          'SCHEMA_REQUIRED_MISSING',
          schemaRef,
          path,
          'the schema declares this field required',
        ),
      )
    }
  }
  if (!open) {
    for (const name of Object.keys(values)) {
      if (!Object.hasOwn(declared, name)) {
        into.push(
          error(
            'SCHEMA_UNKNOWN_FIELD',
            schemaRef,
            `${prefix}.${name}`,
            'the schema is closed and declares no such field',
          ),
        )
      }
    }
  }
}

/** Validates a Concept's attributes against its type (§34–§37). */
export function validateAttributes(
  schemaRef: string,
  spec: AttributeSpec | undefined,
  attributes: JsonMap,
): Validation {
  const result = new Validation()
  validateFields(
    schemaRef,
    'attributes',
    spec?.open === true,
    spec?.fields ?? {},
    attributes,
    result,
  )
  if (spec?.value_schema !== undefined) {
    try { validateValue(spec.value_schema, attributes) } catch (err) {
      result.push(error('SCHEMA_VALUE_NOT_ALLOWED', schemaRef, 'attributes', String(err)))
    }
  }
  return result
}

/**
 * Reports members that changed despite being declared immutable (§39).
 *
 * Needs both states because immutability is a statement about a transition, not
 * about a value: the same map is fine on creation and illegal as an edit.
 */
function validateMutability(
  schemaRef: string,
  prefix: string,
  what: string,
  declared: Record<string, FieldSpec>,
  before: JsonMap,
  after: JsonMap,
  into: Validation,
): void {
  for (const [name, field] of Object.entries(declared)) {
    if (field.mutable !== false) continue
    // Setting an immutable member that was never set is establishing it, not
    // changing it; only a change to an existing value is refused.
    if (
      Object.hasOwn(before, name) &&
      !jsonEquals(before[name] as Json, (after[name] ?? null) as Json)
    ) {
      into.push(
        error(
          'SCHEMA_IMMUTABLE_FIELD',
          schemaRef,
          `${prefix}.${name}`,
          `the schema declares this ${what} immutable; record a new element ` +
            'instead of rewriting it',
        ),
      )
    }
  }
}

/** Reports attributes that changed despite being declared immutable (§39). */
export function validateAttributeMutability(
  schemaRef: string,
  spec: AttributeSpec | undefined,
  before: JsonMap,
  after: JsonMap,
): Validation {
  const result = new Validation()
  validateMutability(
    schemaRef,
    'attributes',
    'attribute',
    spec?.fields ?? {},
    before,
    after,
    result,
  )
  return result
}

/**
 * Reports Facet members that changed despite being declared immutable (§39).
 *
 * A Facet is representation-local state and most of it is meant to move — that
 * is what metabolism does to `MnemonicState`. A member the Profile pins down is
 * the exception, and it is the whole point of the ones that are pinned:
 * `OutcomeRecord` is the graded index over what the world did, and an actor
 * that can rewrite its own grade has not been graded.
 */
export function validateFacetMutability(
  schemaRef: string,
  def: FacetDef,
  before: JsonMap,
  after: JsonMap,
): Validation {
  const result = new Validation()
  validateMutability(
    schemaRef,
    'facets',
    'Facet member',
    def.fields ?? {},
    before,
    after,
    result,
  )
  return result
}

/**
 * What the caller knows about one end of a Proposition or structural edge.
 *
 * Supplied by the caller rather than looked up here, because deciding *what a
 * reference points at* is a storage question and this module has no storage.
 * That keeps schema validation a pure function of `(environment, facts)`,
 * which is what makes it testable without a database and deterministic across
 * engines (§99).
 */
export type EndpointFacts =
  /** A reference to a Cognitive Element. */
  | { kind: 'element'; elementKind: ElementKind; schemaRef?: string }
  /** A Literal value, by its datatype symbol. */
  | { kind: 'literal'; datatype: string }
  /**
   * A reference this engine cannot resolve locally — a canonical identity or a
   * foreign Space reference.
   *
   * Unresolvable is not the same as wrong: the endpoint's type is simply
   * unknown here, and inventing a violation from an unknown would reject
   * legitimate data.
   */
  | { kind: 'unresolved' }

/** Checks one endpoint against its declared constraints (§42–§44). */
export function checkEndpoint(
  schemaRef: string,
  path: string,
  spec: EndpointSpec | undefined,
  facts: EndpointFacts,
  into: Validation,
): void {
  if (isUnconstrained(spec)) return
  const refuse = (message: string): void => {
    into.push(error('SCHEMA_ENDPOINT_NOT_ALLOWED', schemaRef, path, message))
  }
  const kinds = spec?.kinds ?? []
  const conceptTypes = spec?.concept_types ?? []
  const datatypes = literalTypesOf(spec)

  switch (facts.kind) {
    case 'unresolved':
      return
    case 'literal':
      if (datatypes.length === 0) {
        refuse(
          'the schema declares this endpoint an element reference, not a Literal',
        )
      } else if (!datatypes.some((name) => sameDatatype(name, facts.datatype))) {
        refuse(
          `a Literal of datatype ${facts.datatype} is not among the declared ` +
            `datatypes: ${datatypes.join(', ')}`,
        )
      }
      return
    case 'element': {
      // The mirror of the Literal branch's first refusal. A spec that names
      // only datatypes has said what may occupy this end, and it is not a
      // reference — letting one through because the spec never spelled out a
      // `kinds` list would make the two directions of the same declaration
      // mean different things.
      if (kinds.length === 0 && conceptTypes.length === 0) {
        refuse(
          `the schema declares this endpoint a Literal of ${datatypes.join(', ')}, ` +
            'not an element reference',
        )
        return
      }
      if (
        kinds.length > 0 &&
        !kinds.some(
          (allowed) => allowed.toLowerCase() === facts.elementKind.toLowerCase(),
        )
      ) {
        refuse(
          `a ${facts.elementKind} is not among the declared kinds: ` +
            kinds.join(', '),
        )
        return
      }
      if (conceptTypes.length === 0) return
      if (facts.elementKind !== 'Concept') {
        refuse(
          'the schema declares this endpoint a Concept of a specific type, ' +
            `and a ${facts.elementKind} cannot have one`,
        )
        return
      }
      // A Concept whose type this engine has not been told is not a Concept of
      // the wrong type. Reporting one would turn a missing lookup into a
      // schema violation.
      if (facts.schemaRef !== undefined && !conceptTypes.includes(facts.schemaRef)) {
        refuse(
          `${facts.schemaRef} is not among the declared Concept types: ` +
            conceptTypes.join(', '),
        )
      }
      return
    }
  }
}

/**
 * Whether a declared datatype name and a Literal's datatype symbol agree.
 *
 * §9.2 spells the four names bare — `string`, `number`, `boolean`, `null` —
 * and the engine's datatype symbols carry the `kip:` scheme; a package may
 * write either.
 */
function sameDatatype(declared: string, actual: string): boolean {
  const strip = (name: string) => (name.startsWith('kip:') ? name.slice(4) : name)
  return strip(declared) === strip(actual)
}

/**
 * Validates a Proposition's object Literal against the Predicate's `nullable`
 * and `format` declarations (§20.15).
 *
 * Both are write-time checks and neither is part of identity: a `null` object
 * is a semantic Literal only where the Predicate permits it (§9.5), and a
 * `format` says what shape a string must have — a timestamp, a URI — without
 * changing which Literal it is. A package-defined format name this engine
 * does not know is accepted: it cannot be checked, and inventing a failure
 * would reject data on the strength of a word.
 */
export function validatePredicateObjectLiteral(
  schemaRef: string,
  def: PredicateDef,
  object: Endpoint,
): Validation {
  const result = new Validation()
  if (object.kind !== 'literal') return result
  const spec = def.object
  const { value } = object.literal
  if (value === null) {
    if (spec?.nullable !== true) {
      result.push(
        error(
          'SCHEMA_NULL_NOT_PERMITTED',
          schemaRef,
          'object',
          'the Predicate does not permit a null object (§9.5); represent ' +
            'unknown state by absence or uncertainty rather than an invented null',
        ),
      )
    }
    return result
  }
  const format = spec?.format
  if (typeof format !== 'string' || typeof value !== 'string') return result
  if (format === 'timestamp') {
    try {
      normalizeTime(value, 'object')
    } catch {
      result.push(
        error(
          'SCHEMA_FORMAT_VIOLATION',
          schemaRef,
          'object',
          `the Predicate declares its object a timestamp, and ${JSON.stringify(value)} is not one`,
        ),
      )
    }
  } else if (format === 'uri') {
    if (!/^[A-Za-z][A-Za-z0-9+.-]*:\S+$/.test(value)) {
      result.push(
        error(
          'SCHEMA_FORMAT_VIOLATION',
          schemaRef,
          'object',
          `the Predicate declares its object a URI, and ${JSON.stringify(value)} has no scheme`,
        ),
      )
    }
  }
  return result
}

/**
 * Checks that this kind of element may carry this Facet (§58).
 *
 * A Facet declares what it is state *about*: `SkillUtility` is procedural
 * usefulness and belongs on a Skill, `OutcomeRecord` is the graded index over
 * an instrument's output and belongs on Evidence. Carrying one somewhere else
 * would make the name mean something the Profile never said.
 *
 * The carrier's own type is part of the facts when it has one, so a Facet
 * declaring `concept_types` refuses a record — which cannot be a Concept of
 * any type — and refuses a Concept of another type. A carrier whose type was
 * not supplied is not a Concept of the wrong type and is not refused for it.
 */
export function validateFacetCarrier(
  schemaRef: string,
  def: FacetDef,
  carrier: EndpointFacts,
): Validation {
  const result = new Validation()
  checkEndpoint(schemaRef, 'facets', def.applicable_to, carrier, result)
  return result
}

/** Validates a tuple's endpoints against its predicate (§41–§44). */
export function validatePredicateEndpoints(
  schemaRef: string,
  def: PredicateDef,
  subject: EndpointFacts,
  object: EndpointFacts,
): Validation {
  const result = new Validation()
  checkEndpoint(schemaRef, 'subject', def.subject, subject, result)
  checkEndpoint(schemaRef, 'object', def.object, object, result)
  return result
}

/** Validates a structural field's endpoints against its declaration (§62–§66). */
export function validateStructuralEndpoints(
  schemaRef: string,
  def: StructuralFieldDef,
  source: EndpointFacts,
  targets: readonly EndpointFacts[],
): Validation {
  const result = new Validation()
  checkEndpoint(schemaRef, 'source', def.source, source, result)
  for (const target of targets) {
    checkEndpoint(schemaRef, 'target', def.target, target, result)
  }
  return result
}

/** Validates one Facet's members against its definition (§58–§60). */
export function validateFacet(
  schemaRef: string,
  def: FacetDef,
  values: JsonMap,
): Validation {
  const result = new Validation()
  validateFields(
    schemaRef,
    'facets',
    def.closed !== true,
    def.fields ?? {},
    values,
    result,
  )
  if (def.value_schema !== undefined) {
    try { validateValue(def.value_schema as Json, values) } catch (err) {
      result.push(error('SCHEMA_VALUE_NOT_ALLOWED', schemaRef, 'facets', String(err)))
    }
  }
  return result
}

/**
 * Validates one structural field's references (§62–§66).
 *
 * `targets` are the referenced elements' equality keys, in the order they were
 * written.
 */
export function validateStructural(
  schemaRef: string,
  def: StructuralFieldDef,
  targets: readonly string[],
): Validation {
  const result = new Validation()
  const count = targets.length
  const min = def.cardinality?.min ?? 0
  const max = def.cardinality?.max
  if (count < min) {
    result.push(
      error(
        'SCHEMA_CARDINALITY_VIOLATION',
        schemaRef,
        'structural',
        `the schema requires at least ${min} reference(s), got ${count}`,
      ),
    )
  }
  if (max !== undefined && max !== null && count > max) {
    result.push(
      error(
        'SCHEMA_CARDINALITY_VIOLATION',
        schemaRef,
        'structural',
        `the schema permits at most ${max} reference(s), got ${count}`,
      ),
    )
  }
  if (def.unique === true) {
    const seen = new Set<string>()
    for (const target of targets) {
      if (seen.has(target)) {
        result.push(
          error(
            'SCHEMA_DUPLICATE_REFERENCE',
            schemaRef,
            'structural',
            `${target} appears more than once in a field declared unique`,
          ),
        )
      }
      seen.add(target)
    }
  }
  return result
}
