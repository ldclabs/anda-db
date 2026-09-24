import { isJsonMap, type JsonMap } from '../json.js'
import { COGNITIVE_MEMORY_ID, COGNITIVE_MEMORY_VERSION } from './profiles.generated.js'

/**
 * The bundled Cognitive Memory package reference, `kip://profiles/cognitive-memory@2.0.0`,
 * derived from the generated artifact so a package bump is one regeneration.
 */
export const COGNITIVE_MEMORY_REF = `${COGNITIVE_MEMORY_ID}@${COGNITIVE_MEMORY_VERSION}`

/** The prefix every symbol of that package carries, with its trailing slash. */
export const PROFILE_PREFIX = `${COGNITIVE_MEMORY_REF}/`

/** One of that package's Facets on an element, when it carries it as an object. */
export function profileFacet(element: { row: { facets: JsonMap } }, name: string): JsonMap | undefined {
  const value = element.row.facets[PROFILE_PREFIX + name]
  return isJsonMap(value) ? value : undefined
}
