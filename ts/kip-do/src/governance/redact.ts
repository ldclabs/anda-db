/**
 * # What a permitted read is allowed to contain
 *
 * Two narrowings, applied to the rendered view before anything reads a field
 * from it:
 *
 * ```text
 * field mask       a Grant may allow `read` over some members only (§109)
 * raw origin       `_system.origin` needs its own permission (§110)
 * ```
 *
 * ## Why the mask is applied at load, not at projection
 *
 * A redacted field has to be invisible to `FILTER` and `ORDER BY` as well as to
 * the projection list. Otherwise
 *
 * ```text
 * FIND(?c) WHERE { ?c CONCEPT {type: "Person"} FILTER(?c.attributes.salary > 200000) }
 * ```
 *
 * answers the question the mask was meant to refuse — the rows come back without
 * the field, and their *membership* is the disclosure. So the view cached for one
 * query is redacted once, on the way in, and every later stage reads the same
 * narrowed object.
 *
 * ## Why identity survives every mask
 *
 * An element that reached this point is one the caller may read. Stripping its
 * `id` or `kind` would produce a row nothing can join on or cite, which is not a
 * safer answer — it is an unusable one. A mask narrows *content*; membership was
 * already decided by the visibility check.
 *
 * @see rs/anda_cognitive_nexus/src/governance/redact.rs
 */

import type { JsonMap } from '../json.js'
import type { AuthorityConstraints } from './rows.js'

/**
 * The members a mask never removes.
 *
 * `id` and `kind` are how a caller refers to what it just read, and `space_id`
 * is what tells it which Brain answered. A Grant that listed only `name` still
 * means "name, of a thing you can name back".
 */
const ALWAYS_VISIBLE = ['id', 'kind', 'space_id']

/**
 * Narrows a rendered element view to what this decision permits.
 *
 * `mayReadOrigin` comes from the `read_raw_origin` permission rather than from
 * the field mask, because engine origin is a different disclosure from content:
 * it names the Principal that wrote the element and the channel it arrived on,
 * which is operational information about the deployment rather than about the
 * memory (§110).
 *
 * Mutates in place. The view it is handed was rendered for this query and is
 * cached by it, so a copy would be a second object to keep in step.
 */
export function redactView(
  view: JsonMap,
  constraints: AuthorityConstraints,
  mayReadOrigin: boolean,
): void {
  if (!mayReadOrigin) redactOrigin(view._system)
  if (constraints.fields.length === 0) return
  for (const key of Object.keys(view)) {
    if (ALWAYS_VISIBLE.includes(key)) continue
    if (constraints.fields.includes(key)) continue
    delete view[key]
  }
}

/**
 * Replaces engine origin with the fact that there was one.
 *
 * Removing `origin` entirely would say "this element has no recorded origin",
 * which is a claim — and a false one, since every element here has one. What is
 * withheld is *whose*: the reader learns the write was attributed, not to whom
 * (§110).
 */
function redactOrigin(system: unknown): void {
  if (system === null || typeof system !== 'object' || Array.isArray(system)) return
  const block = system as JsonMap
  if (!Object.hasOwn(block, 'origin')) return
  block.origin = { redacted: 'read_raw_origin' }
}
