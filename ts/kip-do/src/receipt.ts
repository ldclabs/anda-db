import { sha3_256Text } from './digest.js'
import { canonicalJson, type JsonMap } from './json.js'

/**
 * The digest a Receipt is sealed with (§33.2), in one place: the issuer seals
 * with it and `VERIFY RECEIPT` recomputes it, so the two cannot disagree about
 * what the digest covers — the Receipt without `receipt_digest`, `proofs` and
 * `extensions`, in canonical JSON.
 */
export const RECEIPT_DIGEST_ALGORITHM = 'sha3-256'

export function receiptDigest(receipt: JsonMap): string {
  const { receipt_digest: _digest, proofs: _proofs, extensions: _extensions, ...bare } = receipt
  return `${RECEIPT_DIGEST_ALGORITHM}:${sha3_256Text(canonicalJson(bare))}`
}
