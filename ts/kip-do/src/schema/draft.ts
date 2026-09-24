/**
 * # Draft vocabulary definitions (Spec §20.16)
 *
 * What a `DEFINE` body may declare, decided on the body alone: a string
 * `description` for both kinds, the members of that kind's package definition
 * and nothing else, and no authority over the data — a draft Predicate claims
 * no closed world and no exclusive-value completeness, and a draft Concept Type
 * has only open, optional attributes of the §9.2 base types.
 *
 * The same rules run twice: statically on a fully literal body (the parser's
 * semantic check) and in the engine on the body after its parameters are bound.
 *
 * @see rs/anda_kip/src/draft.rs — the same rules, message for message
 */

import { errors } from '../errors.js'
import { isJsonMap, type Json } from '../json.js'

/** The fixed package every draft symbol belongs to. */
export const DRAFT_PACKAGE_ID = 'kip://local/draft'
/** Its only version: a draft symbol never changes. */
export const DRAFT_PACKAGE_VERSION = '0.0.0'
/** The exact package reference draft symbols are written under. */
export const DRAFT_PACKAGE_REF = `${DRAFT_PACKAGE_ID}@${DRAFT_PACKAGE_VERSION}`

/** The members a draft Predicate may declare, with §20.15's meanings. */
export const DRAFT_PREDICATE_MEMBERS: readonly string[] = [
  'description', 'subject', 'object', 'functional', 'functional_by', 'open_world',
  'complete', 'boolean_completeness', 'temporal_conflict',
]
/** The members a draft Concept Type may declare. */
export const DRAFT_TYPE_MEMBERS: readonly string[] = ['description', 'attributes']
/** The §9.2 base types a draft attribute may name. */
export const DRAFT_ATTRIBUTE_TYPES: readonly string[] = ['string', 'number', 'boolean', 'null']

export type DraftKind = 'Predicate' | 'ConceptType'

/** The exact reference a draft symbol of this name is written under. */
export function draftSymbolRef(name: string): string {
  return `${DRAFT_PACKAGE_REF}/${name}`
}

/** Checks a `DEFINE` body, parameters already bound, against §20.16. */
export function checkDraftDefinition(kind: DraftKind, definition: Json): void {
  const what = kind === 'Predicate' ? 'a draft Predicate' : 'a draft Concept Type'
  const reject = (message: string): never => {
    throw errors.constraintViolation(`${message} (§20.16)`)
  }
  if (!isJsonMap(definition)) return reject(`${what} is defined by an object`)
  const description = definition.description
  if (description === undefined || description === '') {
    reject(`${what} declares a description: it is the only meaning a later reader of the symbol gets`)
  }
  if (typeof description !== 'string') reject("a draft symbol's description is a string")
  const allowed = kind === 'Predicate' ? DRAFT_PREDICATE_MEMBERS : DRAFT_TYPE_MEMBERS
  const foreign = Object.keys(definition).sort().find((member) => !allowed.includes(member))
  if (foreign !== undefined) reject(`${what} declares no \`${foreign}\`; it declares ${allowed.join(', ')}`)

  if (kind === 'Predicate') {
    for (const member of ['functional', 'open_world', 'complete', 'boolean_completeness']) {
      if (definition[member] !== undefined && typeof definition[member] !== 'boolean') reject(`\`${member}\` is a boolean`)
    }
    if (definition.temporal_conflict !== undefined &&
        definition.temporal_conflict !== 'overlapping_valid_time' && definition.temporal_conflict !== 'none') {
      reject('temporal_conflict is "overlapping_valid_time" or "none" (§20.15)')
    }
    for (const side of ['subject', 'object']) {
      if (definition[side] !== undefined && !isJsonMap(definition[side])) reject(`\`${side}\` is an endpoint object`)
    }
    if (definition.open_world === false) {
      reject('a draft Predicate cannot declare open_world: false; a closed-world reading is authority only an installed package claims')
    }
    if (definition.complete === true) {
      reject('a draft Predicate cannot declare complete: true; exclusive-value completeness is authority only an installed package claims')
    }
    if (definition.functional_by !== undefined) {
      if (definition.functional_by !== 'object_type') {
        reject(`functional_by is "object_type", got ${JSON.stringify(definition.functional_by)}`)
      }
      if (definition.functional === true) reject('functional_by cannot be combined with functional: true')
      // The partition is the object's Concept Type, so the object is declared
      // as Concepts; an omitted one is unconstrained.
      const object = definition.object
      const conceptsOnly = isJsonMap(object) && !('literal_types' in object) &&
        (!Array.isArray(object.kinds) || object.kinds.every((k) => k === 'Concept'))
      if (!conceptsOnly) {
        reject("functional_by partitions by the object's Concept Type, so the draft Predicate declares its object as Concepts")
      }
    }
    return
  }

  const attributes = definition.attributes
  if (attributes === undefined) return
  if (!isJsonMap(attributes)) return reject('draft Concept Type attributes are an object {open, fields}')
  const extra = Object.keys(attributes).sort().find((k) => k !== 'open' && k !== 'fields')
  if (extra !== undefined) reject(`draft Concept Type attributes declare only open and fields, not \`${extra}\``)
  if (attributes.open !== undefined && attributes.open !== true) reject('draft Concept Type attributes are open')
  const fields = attributes.fields
  if (fields === undefined) return
  if (!isJsonMap(fields)) return reject('draft Concept Type attribute fields are an object')
  for (const name of Object.keys(fields).sort()) {
    const field = fields[name]
    if (!isJsonMap(field)) return reject(`draft Concept Type attribute \`${name}\` is an object {type, description?}`)
    if ('required' in field) {
      reject(`draft Concept Type attribute \`${name}\` declares no required member; draft attributes are always optional`)
    }
    const member = Object.keys(field).sort().find((k) => k !== 'type' && k !== 'description')
    if (member !== undefined) {
      reject(`draft Concept Type attribute \`${name}\` declares only type and description, not \`${member}\``)
    }
    if (field.description !== undefined && typeof field.description !== 'string') {
      reject(`draft Concept Type attribute \`${name}\` has a string description`)
    }
    const type = field.type
    if (type === undefined) reject(`draft Concept Type attribute \`${name}\` declares its type`)
    const types = Array.isArray(type) && type.length > 0 ? type : [type]
    for (const ty of types) {
      if (typeof ty !== 'string' || !DRAFT_ATTRIBUTE_TYPES.includes(ty)) {
        reject(`draft Concept Type attribute \`${name}\` has type ${JSON.stringify(ty)}; draft attributes use the base types ${DRAFT_ATTRIBUTE_TYPES.join(', ')} (§9.2)`)
      }
    }
  }
}
