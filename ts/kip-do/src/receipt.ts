/**
 * The digest a Receipt is sealed with (§33.2), in one place: the issuer seals
 * with it and `VERIFY RECEIPT` recomputes it, so the two cannot disagree about
 * what the digest covers — the Receipt without `receipt_digest`, `proofs` and
 * `extensions`, in canonical JSON.
 */
import { DIGEST_PROFILE } from './capsule/index.js'
import { sha3_256Text } from './digest.js'
import { canonicalJson, type JsonMap } from './json.js'

export function receiptDigest(receipt: JsonMap): string {
  const { receipt_digest: _digest, proofs: _proofs, extensions: _extensions, ...bare } = receipt
  return `${DIGEST_PROFILE}:${sha3_256Text(canonicalJson(bare))}`
}
