/**
 * # Physical erasure
 *
 * `PURGE` is the one operation that destroys rather than records. Everything
 * else in this engine appends: an archive moves a lifecycle state, a retraction
 * leaves the Assertion standing, a correction supersedes. Erasure exists
 * because a legal obligation can require it, and for no other reason (§170).
 *
 * It lives in the Governance plane rather than beside the other KML clauses
 * because of what it needs and what it owes. It needs an element-scoped
 * approval — §167 lists purging critical Evidence among the operations a policy
 * may demand independent sign-off for — and it owes the Governance audit a
 * receipt per erased element. A clause executor is not a module that reaches
 * for either, which is exactly how the reference engine and this one drifted
 * apart the first time this was written here.
 *
 * @see rs/anda_cognitive_nexus/src/governance/purge.rs
 */

import { canonicalJson } from '../json.js'
import { sha256Text } from '../digest.js'
import { errors } from '../errors.js'
import { formatElementId, parseElementId, type ElementId } from '../id.js'
import { State, TABLES, specOf, type Element } from '../store/index.js'
import type { Transaction } from '../tx.js'
import { requireApproved } from './approval.js'
import { resourceOfElement } from './decision.js'

/**
 * `PURGE` — physical erasure, leaving an identity stub (§19.3, §170–§177).
 *
 * Two orderings matter and neither is arbitrary.
 *
 * **The version log goes first.** An element scrubbed only in its current row
 * stays fully readable through `AS OF`, so erasing that way is erasure in name
 * only; and doing it the other way round leaves, after a crash, a scrubbed stub
 * with an intact history and nothing saying to look.
 *
 * **The row survives.** Deleting it would leave every reference to it dangling,
 * and a dangling reference does not say "this was erased" — it says nothing.
 * What is left is identity, provenance and a digest of what used to be there.
 */
export function stage(
  tx: Transaction,
  id: ElementId,
  policy: ReferencePolicy,
): void {
  const named = formatElementId(id)
  const element = tx.load(id)

  // §167 lists purging critical Evidence among the operations a policy may
  // require independent approval for, and this is where such an approval is
  // resolved — bound to this element, not merely to the permission.
  const approved = requireApproved(
    tx.store,
    tx.cx.space,
    resourceOfElement(element),
    tx.authority.authorize('purge', resourceOfElement(element), tx.auth),
    tx.auth,
  )

  // §163: a legal hold is exactly the thing purge must not walk past, and it is
  // checked before anything destructive is decided. Lifting the hold is a
  // separate Governance decision under its own permission.
  if (hasLegalHold(element)) {
    throw errors.legalHoldConflict(
      `${named} is under a legal hold; lifting the hold is a separate ` +
        `Governance decision under its own permission`,
    )
  }

  const referrers = tx.store.referrers(tx.cx.space, id)
  if (policy === 'deny_if_referenced' && referrers.length > 0) {
    // Names how many, not which: the referring elements may be ones this caller
    // cannot read, and a purge refusal must not become a way to enumerate them
    // (§103).
    throw errors.purgeDenied(
      `${referrers.length} element(s) still reference ${named}. Erasing a ` +
        `referenced element leaves a history that points at nothing; choose ` +
        `REFERENCE POLICY "tombstone_reference" to keep the identity stub, or ` +
        `"authorized_cascade" to erase the dependents too`,
    )
  }
  if (policy === 'authorized_cascade') {
    // Each dependent is authorized and hold-checked on its own before any of
    // them is touched. A cascade that erased half and then refused would leave
    // a graph nothing can describe.
    const dependents = [...new Set(referrers.map((r) => formatElementId(r.from)))]
      .map(parseElementId)
      .filter((dependent) => formatElementId(dependent) !== named)
    for (const dependent of dependents) {
      tx.authorizeElement(dependent, 'purge')
      if (hasLegalHold(tx.load(dependent))) {
        throw errors.legalHoldConflict(
          `${formatElementId(dependent)} depends on ${named} and is under a ` +
            `legal hold, so this cascade cannot complete`,
        )
      }
    }
    for (const dependent of dependents) scrub(tx, dependent, policy)
  }

  scrub(tx, id, policy)
  // Spent last, and only here: the whole statement runs inside the Durable
  // Object's transaction, so a later clause that refuses rolls this back with
  // everything else. An approval buys a completed erasure, not an attempt.
  approved.spend(tx.store)
}

/**
 * Whether an element is held against erasure (§82, §163).
 *
 * A cognitive writer cannot set this: `legal_hold` in a `retention` block needs
 * the `legal_hold` permission of its own, precisely so that content cannot make
 * itself undeletable.
 */
function hasLegalHold(element: Element): boolean {
  return element.row.retention.legal_hold === true
}

/**
 * The columns a purged stub keeps.
 *
 * Identity, provenance and lifecycle — what an auditor needs to know that
 * something was here and who wrote it — and nothing that was observed about the
 * world. `origin` names the Principal that wrote the element, which is audit
 * information about the deployment rather than the content being erased, and
 * losing it would make the stub unattributable. `governance` is overwritten with
 * the purge marker immediately after.
 */
const PURGE_KEEPS: ReadonlySet<string> = new Set([
  'space',
  'state',
  'version',
  'seq',
  'created_at',
  'updated_at',
  'created_tx',
  'updated_tx',
  'origin',
  'governance',
])

/**
 * The empty value of the same shape, so a cleared column still type-checks
 * against its row and still encodes as the JSON its table declares.
 */
function emptyLike(value: unknown): unknown {
  if (Array.isArray(value)) return []
  if (typeof value === 'number') return 0
  if (typeof value === 'boolean') return false
  if (value !== null && typeof value === 'object') return {}
  if (typeof value === 'string') return ''
  return null
}

/**
 * Replaces one element's content with its identity stub.
 *
 * The version log goes first. A crash between the two leaves an element whose
 * current row still has its content and whose past does not, which is
 * recoverable by purging again; the other order leaves a stub whose full
 * contents are still readable through the history, which is not recoverable at
 * all because nothing says to look (§19.3).
 */
function scrub(tx: Transaction, id: ElementId, policy: ReferencePolicy): void {
  const element = tx.load(id)
  const digest = sha256Text(canonicalJson(element.row))
  tx.store.purgeVersions(tx.cx.space, id)

  const row = element.row as unknown as Record<string, unknown>
  // Driven by the table's own column list rather than by an allow-list of
  // fields to clear: a column added later would otherwise quietly survive
  // erasure, and the columns that matter most — a Proposition's tuple, an
  // Assertion's stance and confidence — are exactly the ones an allow-list
  // forgets. Only {@link PURGE_KEEPS} survives.
  for (const column of specOf(TABLES[element.kind]).columns) {
    if (PURGE_KEEPS.has(column)) continue
    row[column] = emptyLike(row[column])
  }
  element.row.state = State.PURGED
  // The stub says what it is and what it held, and nothing about the content
  // itself. `purged: true` is what a reader checks instead of guessing from an
  // empty name.
  element.row.governance = { purged: true, content_digest: digest }
  tx.markChanged(id, 'purge')

  // The receipt §164 permits: enough to audit the erasure, and nothing of what
  // was erased.
  tx.store.governance.recordMutation({
    operation: 'purge',
    at: tx.cx.at,
    space_id: tx.cx.space,
    resource: formatElementId(id),
    principal_id: tx.auth.principal_id,
    record: {
      element: formatElementId(id),
      content_digest: digest,
      reference_policy: policy,
      tx_id: tx.cx.tx_id,
    },
  })
}

/**
 * How a purge treats elements that still point at the target (§173).
 *
 * The default refuses, because in a cognitive history an Assertion, an Activity
 * or an Experience may point at the target, and erasing the whole dependency
 * chain falsifies history (§175). KIP 1.x made destructive cascade ordinary —
 * `DELETE ... DETACH` — and 2.0 deliberately does not.
 */
export type ReferencePolicy =
  | 'deny_if_referenced'
  | 'tombstone_reference'
  | 'authorized_cascade'

/** Reads the `REFERENCE POLICY` clause; absent means the conservative one. */
export function referencePolicy(name: string | null): ReferencePolicy {
  if (name === null || name === 'deny_if_referenced') return 'deny_if_referenced'
  if (name === 'tombstone_reference' || name === 'authorized_cascade') return name
  // Defaulting would silently run a destructive operation under a policy the
  // caller did not ask for.
  throw errors.constraintViolation(
    `${JSON.stringify(name)} is not a reference policy; this engine ` +
      `implements deny_if_referenced, tombstone_reference and authorized_cascade`,
  )
}
