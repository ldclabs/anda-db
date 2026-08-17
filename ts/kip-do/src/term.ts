/**
 * # References and Literals
 *
 * A Proposition endpoint is either a reference to a Cognitive Element or a
 * Literal (Spec §6, §9). Both have to reduce to a deterministic key, because
 * Proposition identity is defined by the tuple and a Space keeps one canonical
 * Proposition per semantic tuple (§12.5, §93.6) — an equality question the
 * storage layer has to answer, not a matter of taste.
 *
 * The two rules that make it deterministic:
 *
 * - a number is equal by *normalized finite value*, so `1`, `1.0` and `1e0`
 *   are one Literal and not three Propositions (§9.4);
 * - a language tag is part of Literal identity, so `"苹果"@zh-Hans` and a bare
 *   `"苹果"` are different Literals (§9.5).
 */

import { digestParts } from './digest.js'
import { errors } from './errors.js'
import { formatElementId, parseElementId, type ElementId } from './id.js'
import { isJsonMap, type Json, type JsonMap } from './json.js'

/** The Core datatype of a string Literal. */
export const DT_STRING = 'kip:string'
/** The Core datatype of a numeric Literal. */
export const DT_NUMBER = 'kip:number'
/** The Core datatype of a boolean Literal. */
export const DT_BOOLEAN = 'kip:boolean'
/** The Core datatype of the `null` Literal. */
export const DT_NULL = 'kip:null'

/**
 * A Core Literal (Spec §9.2).
 *
 * The payload is restricted to JSON scalar semantics; an array or object is
 * not a Core Literal, and a structured value that needs semantic identity
 * belongs in a Concept.
 */
export interface Literal {
  value: null | boolean | number | string
  /**
   * The datatype symbol: one of the `DT_*` constants, or a Schema-defined
   * refinement such as `kip:datetime`.
   */
  datatype: string
  /** The language tag, when language is semantically relevant. */
  language?: string
}

/**
 * Builds a Literal from a bare JSON scalar, inferring the Core datatype.
 *
 * This is the "primitive shorthand" of §9.3: the model-facing syntax writes
 * `"+08:00"` or `3`, and the canonical internal model still distinguishes the
 * datatype.
 */
export function literalFromScalar(value: Json): Literal {
  switch (typeof value) {
    case 'string':
      return { value, datatype: DT_STRING }
    case 'number':
      if (!Number.isFinite(value)) {
        throw errors.typeMismatch(
          'NaN and Infinity are not valid Core JSON numbers',
        )
      }
      return { value, datatype: DT_NUMBER }
    case 'boolean':
      return { value, datatype: DT_BOOLEAN }
    case 'object':
      if (value === null) return { value: null, datatype: DT_NULL }
      break
  }
  throw errors.typeMismatch(
    'arrays and objects are not Core Literals; a structured value with its ' +
      'own semantic identity belongs in a Concept',
  )
}

/** Reads the explicit `{value, datatype, language}` form. */
export function literalFromObject(map: JsonMap): Literal {
  const literal = literalFromScalar(map.value ?? null)
  const datatype = map.datatype
  if (datatype !== undefined && datatype !== null) {
    if (typeof datatype !== 'string') {
      throw errors.typeMismatch('a Literal datatype must be a symbol string')
    }
    literal.datatype = datatype
  }
  const language = map.language
  if (language !== undefined && language !== null) {
    if (typeof language !== 'string') {
      throw errors.typeMismatch(
        'a Literal language must be a language tag string',
      )
    }
    literal.language = language
  }
  return literal
}

/**
 * The persisted form: always the explicit object, never the shorthand.
 *
 * Storing the shorthand would throw away the datatype the moment a Schema
 * refined it — `kip:datetime` would read back as `kip:string`.
 */
export function literalToJson(literal: Literal): JsonMap {
  const map: JsonMap = { value: literal.value, datatype: literal.datatype }
  if (literal.language !== undefined) map.language = literal.language
  return map
}

/**
 * Canonicalizes a finite number to its normalized value form.
 *
 * `1`, `1.0` and `1e0` all reduce to `1`, so they cannot become three distinct
 * Propositions (§9.4).
 */
function canonicalNumber(n: number): string {
  // `Number.prototype.toString` already emits the shortest round-tripping
  // form, and writes an integral double without a fractional part — so `1.0`
  // and `1` arrive here as the same value and leave as the same string.
  return Object.is(n, -0) ? '0' : String(n)
}

/** One endpoint of a Proposition tuple. */
export type Endpoint =
  /** A same-Space reference to a Cognitive Element (§6.1). */
  | { kind: 'local'; id: ElementId }
  /** A canonical external identity, used when no local Concept exists (§6.2). */
  | { kind: 'canonical'; canonicalId: string }
  /** An explicit cross-Space reference (§6.3). */
  | { kind: 'foreign'; spaceId: string; elementId: string }
  /** A Literal value (§9). */
  | { kind: 'literal'; literal: Literal }

/**
 * The field separator inside a composite key.
 *
 * A unit separator cannot occur in an element id and is vanishingly unlikely
 * in a datatype symbol, so no two different endpoints can collide by writing
 * each other's separator.
 */
const SEP = '\u001f'

/**
 * Reads an endpoint from its persisted JSON form.
 *
 * A bare scalar is the Literal shorthand; an object is a reference or the
 * explicit Literal form, told apart by which key it carries.
 */
export function endpointFromJson(value: Json): Endpoint {
  if (!isJsonMap(value)) {
    return { kind: 'literal', literal: literalFromScalar(value) }
  }

  if (value.id !== undefined) {
    if (typeof value.id !== 'string') {
      throw errors.structuralReferenceInvalid(
        "an element reference's `id` must be a string",
      )
    }
    return { kind: 'local', id: parseElementId(value.id) }
  }
  if (value.canonical_id !== undefined) {
    if (typeof value.canonical_id !== 'string') {
      throw errors.structuralReferenceInvalid(
        "a canonical identity reference's `canonical_id` must be a string",
      )
    }
    return { kind: 'canonical', canonicalId: value.canonical_id }
  }
  if (value.space_id !== undefined && value.element_id !== undefined) {
    if (
      typeof value.space_id !== 'string' ||
      typeof value.element_id !== 'string'
    ) {
      throw errors.structuralReferenceInvalid(
        'a foreign Space reference needs string `space_id` and `element_id`',
      )
    }
    return {
      kind: 'foreign',
      spaceId: value.space_id,
      elementId: value.element_id,
    }
  }
  if (Object.hasOwn(value, 'value')) {
    return { kind: 'literal', literal: literalFromObject(value) }
  }
  throw errors.structuralReferenceInvalid(
    'an endpoint object must carry `id`, `canonical_id`, `space_id`+`element_id`, ' +
      'or a Literal `value`',
  )
}

/** The persisted JSON form. */
export function endpointToJson(endpoint: Endpoint): JsonMap {
  switch (endpoint.kind) {
    case 'local':
      return { id: formatElementId(endpoint.id) }
    case 'canonical':
      return { canonical_id: endpoint.canonicalId }
    case 'foreign':
      return { space_id: endpoint.spaceId, element_id: endpoint.elementId }
    case 'literal':
      return literalToJson(endpoint.literal)
  }
}

/**
 * The deterministic equality key.
 *
 * Two endpoints are the same endpoint exactly when their keys are equal, which
 * is what makes an index over this column answer the identity question the
 * tuple asks.
 */
export function endpointKey(endpoint: Endpoint): string {
  switch (endpoint.kind) {
    case 'local':
      return `id${SEP}${formatElementId(endpoint.id)}`
    case 'canonical':
      return `cid${SEP}${endpoint.canonicalId}`
    case 'foreign':
      return `fs${SEP}${endpoint.spaceId}${SEP}${endpoint.elementId}`
    case 'literal': {
      const { value, datatype, language } = endpoint.literal
      const head = `lit${SEP}${datatype}${SEP}${language ?? ''}${SEP}`
      switch (typeof value) {
        case 'string':
          return `${head}s${value}`
        case 'number':
          return `${head}n${canonicalNumber(value)}`
        case 'boolean':
          return `${head}b${value}`
        default:
          // The datatype segment already separates `null` from the empty
          // string, so the payload segment can be empty.
          return `${head}z`
      }
    }
  }
}

/**
 * The element this endpoint resolves to inside this Space, if any.
 *
 * Same-Space closure is checked against this: a Literal has nothing to close
 * over, and a canonical or foreign reference is deliberately outside the rule
 * (§7).
 */
export function endpointLocal(endpoint: Endpoint): ElementId | null {
  return endpoint.kind === 'local' ? endpoint.id : null
}

/**
 * The structural identity of a Proposition tuple within its Space (§12.5).
 *
 * Digested rather than concatenated because the raw key of a Literal endpoint
 * is unbounded — a Proposition object can be a paragraph — while an index key
 * should not be. The digest is over the same separated encoding the individual
 * key columns use, so two tuples collide exactly when their endpoints and
 * predicate are equal.
 */
export function tupleKey(
  space: string,
  subject: Endpoint,
  predicateRef: string,
  object: Endpoint,
): string {
  return digestParts([
    space,
    endpointKey(subject),
    predicateRef,
    endpointKey(object),
  ])
}

/**
 * Reads a structural reference that must resolve to a local element.
 *
 * Core structural references are same-Space by definition; a canonical or
 * foreign identity in one of these slots is a malformed record rather than an
 * unresolved lookup (§8.2, §93.3).
 */
export function localRef(value: Json, field: string): ElementId {
  const local = endpointLocal(endpointFromJson(value))
  if (local === null) {
    throw errors.structuralReferenceInvalid(
      `\`${field}\` must reference a local element by id`,
    )
  }
  return local
}
