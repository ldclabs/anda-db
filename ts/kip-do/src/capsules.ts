/**
 * Bootstrap capsule application.
 *
 * The capsule sources themselves are generated from `rs/anda_kip/capsules/`
 * into `capsules.generated.ts` — see `scripts/codegen-capsules.mjs`.
 */

export { BUNDLED_CAPSULES, type Capsule } from './capsules.generated.js'

/**
 * Content hash of a capsule source.
 *
 * A changed `.kip` file yields a new hash, which is what re-applies the
 * capsule on an existing database — there is no manual version to bump and so
 * no way to forget to bump it. FNV-1a rather than a cryptographic digest
 * because this only needs change detection, and Workers expose no synchronous
 * hashing primitive.
 */
export function capsuleHash(source: string): string {
  let hash = 0x811c9dc5
  for (let i = 0; i < source.length; i++) {
    hash ^= source.charCodeAt(i)
    hash = Math.imul(hash, 0x01000193) >>> 0
  }
  return hash.toString(16).padStart(8, '0')
}
