/**
 * # Mnemonic strength (Spec §59.1, Profile §6.1)
 *
 * Decay is computed, not written. `MnemonicState.memory_strength` is the last
 * explicitly written base, `last_metabolized_at` its anchor and
 * `strength_policy` a pinned policy artifact; the read-only member
 * `effective_strength` is derived from the three when a read is evaluated and
 * never written back.
 *
 * This engine knows the bundled policies, the standard
 * `kip:strength-half-life-30d`. A missing base, anchor or pin, a policy it does
 * not know, or a pin whose digest is not that artifact's leaves the value
 * `null` — unknown — and never falls back to another policy or a default.
 *
 * @see rs/anda_cognitive_nexus/src/projection/strength.rs — the same rules
 */

import { isJsonMap, type Json, type JsonMap } from '../json.js'
import { COGNITIVE_MEMORY_ID, STRENGTH_POLICIES } from '../schema/profiles.generated.js'
import { lineageText } from '../schema/symbol.js'
import { parseTime } from '../time.js'

interface HalfLife {
  policyId: string
  contentDigest: string
  halfLifeMs: number
}

const HALF_LIVES: readonly HalfLife[] = STRENGTH_POLICIES.flatMap((artifact) => {
  const method = artifact.method as JsonMap | undefined
  const integrity = artifact.integrity as JsonMap | undefined
  if (method?.kind !== 'half_life' || typeof method.half_life_ms !== 'number') return []
  if (typeof artifact.policy_id !== 'string' || typeof integrity?.content_digest !== 'string') return []
  return [{ policyId: artifact.policy_id, contentDigest: integrity.content_digest, halfLifeMs: method.half_life_ms }]
})

/** Whether a Facet key names the Profile's `MnemonicState`, in any version. */
function isMnemonicState(key: string): boolean {
  return key.startsWith(COGNITIVE_MEMORY_ID) && lineageText(key) === `${COGNITIVE_MEMORY_ID}/MnemonicState`
}

/** `effective_strength` for one `MnemonicState` at the instant `now`. */
export function effectiveStrength(state: JsonMap, now: string): number | null {
  const base = state.memory_strength
  const anchor = state.last_metabolized_at
  const pin = state.strength_policy
  if (typeof base !== 'number' || typeof anchor !== 'string' || !isJsonMap(pin)) return null
  const policy = HALF_LIVES.find(
    (p) => p.policyId === pin.artifact_ref && p.contentDigest === pin.content_digest,
  )
  if (policy === undefined) return null
  let from: number
  let at: number
  try {
    from = parseTime(anchor)
    at = parseTime(now)
  } catch {
    return null
  }
  // Before its anchor the value is the base.
  const elapsed = Math.max(0, at - from)
  return base * 2 ** (-elapsed / policy.halfLifeMs)
}

/**
 * Adds the computed `effective_strength` to every `MnemonicState` Facet of a
 * rendered view, evaluated at `now` — the read's own instant, never its
 * `FOR TIME` (Profile §6.1).
 */
export function computeStrength(view: JsonMap, now: string): void {
  if (!isJsonMap(view.facets)) return
  for (const [key, state] of Object.entries(view.facets)) {
    if (!isMnemonicState(key) || !isJsonMap(state)) continue
    state.effective_strength = effectiveStrength(state, now) as Json
  }
}

/**
 * Removes what {@link computeStrength} added: a computed member never leaves
 * the read that evaluated it, so an export carries the state it is computed
 * from (§18.2).
 */
export function stripStrength(view: JsonMap): void {
  if (!isJsonMap(view.facets)) return
  for (const [key, state] of Object.entries(view.facets)) {
    if (isMnemonicState(key) && isJsonMap(state)) delete state.effective_strength
  }
}
