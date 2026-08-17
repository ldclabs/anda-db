/**
 * # Package validation
 *
 * Core validation and package validation are different layers (Spec §91–§93),
 * and this module is only the second one: attribute shape, Literal datatype,
 * Facet shape, structural cardinality. Element shape, same-Space closure and
 * `_system` protection are Core's, enforced in the store and the mutation
 * layer, and a package cannot weaken them (§92, §240.32).
 *
 * ## What must *not* be rejected here
 *
 * A `functional` predicate says one subject has at most one true object. It is
 * an **epistemic** statement, so two competing objects are a contested belief —
 * something the Nexus has to be able to store in order to report it (§95,
 * §240.28). Turning it into a write rejection would mean the system could never
 * record disagreement, which is most of what a memory system is for.
 *
 * Likewise `open_world: true` means an absent claim is unknown, not false.
 * Nothing here infers falsity from absence.
 *
 * ## Severity
 *
 * Only deterministic declared constraints produce errors (§98). Model hints are
 * advisory and never become hard validators (§240.34), so nothing in
 * `model_hints` is read by this module at all.
 */

import { errors } from '../errors.js'
import { canonicalJson, jsonEquals, type Json, type JsonMap } from '../json.js'
import type { AttributeSpec, FacetDef, FieldSpec, StructuralFieldDef } from './package.js'

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
  return result
}

/**
 * Reports attributes that changed despite being declared immutable (§39).
 *
 * Needs both states because immutability is a statement about a transition, not
 * about a value: the same attribute map is fine on creation and illegal as an
 * edit.
 */
export function validateAttributeMutability(
  schemaRef: string,
  spec: AttributeSpec | undefined,
  before: JsonMap,
  after: JsonMap,
): Validation {
  const result = new Validation()
  for (const [name, field] of Object.entries(spec?.fields ?? {})) {
    if (field.mutable !== false) continue
    // Setting an immutable attribute that was never set is establishing it,
    // not changing it; only a change to an existing value is refused.
    if (
      Object.hasOwn(before, name) &&
      !jsonEquals(before[name] as Json, (after[name] ?? null) as Json)
    ) {
      result.push(
        error(
          'SCHEMA_IMMUTABLE_FIELD',
          schemaRef,
          `attributes.${name}`,
          'the schema declares this attribute immutable; record a new element ' +
            'or a new Assertion instead of rewriting it',
        ),
      )
    }
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
