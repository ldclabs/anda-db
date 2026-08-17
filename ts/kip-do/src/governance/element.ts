/**
 * # Changing an element's own Governance members
 *
 * An element's `governance` block — its classification, its influence-authority
 * ceiling, its quarantine reason — is not an author-writable field. The parser
 * refuses it in every assignment, on the text path and the pre-parsed AST path
 * alike, so no KML statement can reach it (§50, §51). This module is the other
 * half: the authorized ways it *does* change.
 *
 * ```text
 * classify              raise a label            update
 *                       lower one                declassify
 * elevate_authority     raise a ceiling          elevate_authority + approvals
 *                       lower one                elevate_authority
 * quarantine / release  hold out of ordinary use quarantine
 * ```
 *
 * ## Why raising is ordinary and lowering is not
 *
 * Classification and authority move in opposite directions, and each has one
 * privileged direction: the one that *reveals* or *empowers*. An agent that
 * notices it has written something sensitive should be able to say so without a
 * Governance ticket, and an agent that has decided a Skill is dangerous should
 * be able to demote it immediately (§132). Making the cautious direction
 * privileged would make caution rare.
 *
 * ## Why non-amplification is checked at elevation, not at derivation
 *
 * Everything is created `descriptive`, the bottom of the ladder, so a derived
 * artifact cannot exceed its inputs by default — the rule holds without doing
 * anything. It becomes load-bearing only when somebody asks to *raise* one, and
 * that is where the lineage recorded at commit is read: a summary of a
 * descriptive Skill cannot become behavioral, however locally it was written
 * (§127, §128, §243).
 *
 * ## Why these commit as transactions
 *
 * Each writes a new element version and takes a Space sequence, exactly as a
 * cognitive write does. That is what keeps §177 answerable — *what
 * classification did this element have then* — and what puts the change in the
 * authorized change stream (§186). Each is recorded in the Governance audit as
 * well, because the two logs answer different questions: the version log says
 * what the element looked like, the audit says who decided that and why.
 *
 * @see rs/anda_cognitive_nexus/src/governance/element.rs
 */

import { errors } from '../errors.js'
import { formatElementId, tryParseElementId, type ElementId } from '../id.js'
import type { Json, JsonMap } from '../json.js'
import {
  State,
  classificationOf,
  type Element,
  type Store,
} from '../store/index.js'
import { nowTime } from '../time.js'
import { consumeResolvedApprovals, resolveApproval } from './approval.js'
import type { AuthContext } from './auth.js'
import {
  requirePermitted,
  resourceOfElement,
  authorityCeiling,
  type EffectiveAuthority,
} from './decision.js'
import { authority, classification } from './lattice.js'

/** The `governance` member holding an element's influence-authority ceiling. */
export const AUTHORITY_KEY = 'max_influence_authority'
/** The `governance` member recording what a derived element was derived from. */
export const LINEAGE_KEY = 'authority_lineage'
/** The `governance` member recording why an element is held out of use. */
export const QUARANTINE_KEY = 'quarantine_reason'

/** The influence-authority ceiling an element carries (§124). */
export function ceilingOf(element: Element): string {
  const stated = element.row.governance[AUTHORITY_KEY]
  return typeof stated === 'string' && stated !== '' ? stated : authority.DEFAULT
}

/** The elements a derived artifact inherits its ceiling from (§128). */
export function lineageOf(element: Element): string[] {
  const values = element.row.governance[LINEAGE_KEY]
  if (!Array.isArray(values)) return []
  return values.filter((value): value is string => typeof value === 'string')
}

/** What one Governance write to an element needs from its caller. */
export interface ElementGovernanceContext {
  store: Store
  space: string
  authority: EffectiveAuthority
  auth: AuthContext
}

/**
 * Sets one element's classification label (§93, §100).
 *
 * Returns the label that was there before, so a caller can report the transition
 * rather than only the destination.
 */
export function classify(
  cx: ElementGovernanceContext,
  id: ElementId,
  label: string,
): string {
  const element = readable(cx, id)
  const resource = resourceOfElement(element)

  const current = classificationOf(element)
  const effective = current === '' ? cx.authority.defaultClassification() : current
  const lowering = classification.rank(label) < classification.rank(effective)
  const permission = lowering ? 'declassify' : 'update'
  const decision = requirePermitted(
    resolveApproval(
      cx.store,
      cx.space,
      resource,
      cx.authority.authorize(permission, resource, cx.auth),
      cx.auth,
    ),
  )

  const op = lowering ? 'declassify' : 'classify'
  const version = commit(cx, element, op, null, (block) =>
    setMember(block, 'classification', label),
  )
  audit(cx, id, op, { from: current, to: label, version })
  consumeResolvedApprovals(cx.store, decision)
  return current
}

/**
 * Raises or lowers how strongly one element may influence action (§129, §132).
 *
 * Raising is checked against the element's authority lineage: a derived artifact
 * cannot be elevated past the lowest ceiling it was derived from, so no chain of
 * summarizing turns a descriptive note into an executable one.
 *
 * Returns the ceiling the element carried before.
 */
export function elevateAuthority(
  cx: ElementGovernanceContext,
  id: ElementId,
  cls: string,
): string {
  if (cls !== authority.DESCRIPTIVE && cls !== '' && authority.rank(cls) === 0) {
    throw errors.constraintViolation(
      `${JSON.stringify(cls)} is not an influence-authority class this engine ` +
        `implements`,
    )
  }
  const element = readable(cx, id)
  const resource = resourceOfElement(element)
  // §129: elevation is exactly the operation a policy asks for independent
  // approval on, and §246 requires that one approval of two is not partial
  // activation. That is decided here rather than by the caller.
  const decision = requirePermitted(
    resolveApproval(
      cx.store,
      cx.space,
      resource,
      cx.authority.authorize('elevate_authority', resource, cx.auth),
      cx.auth,
    ),
  )

  const current = ceilingOf(element)
  const raising = authority.rank(cls) > authority.rank(current)
  if (raising) {
    const grantedCeiling = authorityCeiling(decision.constraints)
    if (authority.rank(cls) > authority.rank(grantedCeiling)) {
      throw errors.notAuthorized(
        `${JSON.stringify(cls)} exceeds this Principal's influence-authority ` +
          `ceiling ${JSON.stringify(grantedCeiling)}`,
      )
    }
    const bound = inheritedCeiling(cx, element)
    if (authority.rank(cls) > authority.rank(bound)) {
      throw errors.notAuthorized(
        `${formatElementId(id)} was derived from material capped at ` +
          `${JSON.stringify(bound)}, so it cannot be raised to ` +
          `${JSON.stringify(cls)}. Transformation does not raise authority — ` +
          `elevate what it was derived from, or record an independent artifact`,
      )
    }
  }

  const op = raising ? 'elevate' : 'downgrade'
  const version = commit(cx, element, op, null, (block) =>
    setMember(block, AUTHORITY_KEY, cls),
  )
  // §130: an elevation record names the artifact, both ceilings, who decided
  // and when. The transaction and the audit entry supply the rest between them.
  audit(cx, id, raising ? 'elevate_authority' : 'downgrade_authority', {
    from: current,
    to: cls,
    version,
  })
  consumeResolvedApprovals(cx.store, decision)
  return current
}

/**
 * Holds an element out of ordinary use, pending review (§133).
 *
 * Not a retraction and not an archive: it says *local Governance does not
 * currently allow ordinary use of this*, which is a statement about this Brain
 * and not about the source (§134). Ordinary recall excludes it by construction,
 * because a pattern that names no state matches `active`; a reviewer that writes
 * `{state: "quarantined"}` can still see it.
 */
export function quarantine(
  cx: ElementGovernanceContext,
  id: ElementId,
  reason: string,
): void {
  const element = readable(cx, id)
  const resource = resourceOfElement(element)
  const decision = requirePermitted(
    resolveApproval(
      cx.store,
      cx.space,
      resource,
      cx.authority.authorize('quarantine', resource, cx.auth),
      cx.auth,
    ),
  )
  const version = commit(cx, element, 'quarantine', State.QUARANTINED, (block) =>
    setMember(block, QUARANTINE_KEY, reason),
  )
  audit(cx, id, 'quarantine', { reason, version })
  consumeResolvedApprovals(cx.store, decision)
}

/** Returns a quarantined element to ordinary use. */
export function release(cx: ElementGovernanceContext, id: ElementId): void {
  const element = readable(cx, id)
  if (element.row.state !== State.QUARANTINED) {
    throw errors.invalidLifecycleTransition(
      `${formatElementId(id)} is ${JSON.stringify(element.row.state)}, not ` +
        `quarantined; releasing it would silently revive an element that was ` +
        `archived or tombstoned for a different reason`,
    )
  }
  const resource = resourceOfElement(element)
  const decision = requirePermitted(
    resolveApproval(
      cx.store,
      cx.space,
      resource,
      cx.authority.authorize('quarantine', resource, cx.auth),
      cx.auth,
    ),
  )
  // `release`, matching the reference engine: `HISTORY ELEMENT` returns this
  // verb, so a name only one engine uses is a wire divergence.
  const version = commit(cx, element, 'release', State.ACTIVE, (block) =>
    setMember(block, QUARANTINE_KEY, null),
  )
  audit(cx, id, 'release_quarantine', { version })
  consumeResolvedApprovals(cx.store, decision)
}

// ---------------------------------------------------------------------------
// Shared plumbing
// ---------------------------------------------------------------------------

/**
 * Loads an element the caller is entitled to see.
 *
 * Reading it is the floor for every operation here: a caller who may not see an
 * element must not be able to learn what it is classified as by trying to change
 * it.
 */
function readable(cx: ElementGovernanceContext, id: ElementId): Element {
  const element = cx.store.load(id)
  if (element === null || element.row.space !== cx.space) {
    throw errors.notFoundOrNotVisible(`no element ${formatElementId(id)}`)
  }
  requirePermitted(
    cx.authority.authorize('read', resourceOfElement(element), cx.auth),
  )
  return element
}

/**
 * The lowest ceiling among the material this element was derived from.
 *
 * An element with no recorded lineage is not derived from anything this engine
 * knows about, so nothing bounds it beyond policy. An input that has since been
 * erased bounds it at the bottom: authority that cannot be verified is authority
 * that is not held.
 */
function inheritedCeiling(cx: ElementGovernanceContext, element: Element): string {
  const lineage = lineageOf(element)
  if (lineage.length === 0) return authority.EXECUTABLE
  let bound: string = authority.EXECUTABLE
  for (const reference of lineage) {
    const id = tryParseElementId(reference)
    if (id === null) continue
    const input = cx.store.load(id)
    if (input === null) return authority.DESCRIPTIVE
    bound = authority.meet(bound, ceilingOf(input))
  }
  return bound
}

/**
 * Writes a Governance patch onto an element as its own transaction.
 *
 * The version log entry is appended in the same write as the row, for the same
 * reason every cognitive write does it: a history written afterwards can be
 * missing exactly the change a crash interrupted, and a history with a hole
 * answers `AS OF` wrongly instead of refusing.
 */
function commit(
  cx: ElementGovernanceContext,
  element: Element,
  op: string,
  state: string | null,
  patch: (block: JsonMap) => JsonMap,
): number {
  const seq = cx.store.nextSeq(cx.space)
  const at = nowTime()
  const txId = `tx-${cx.space}-${seq}-${at}`
  const row = element.row
  row.governance = patch(row.governance)
  if (state !== null) row.state = state
  row.version += 1
  row.seq = seq
  row.updated_at = at
  row.updated_tx = txId
  // The Principal that made the decision, not the one that wrote the content:
  // this *is* a new version of the element, and attributing it to the original
  // author would misreport who reclassified it.
  row.origin = { principal_id: cx.auth.principal_id, channel: 'governance' }
  const change = cx.store.put(element, op as never, txId)
  cx.store.putGovernanceTransaction({
    tx_id: txId,
    space: cx.space,
    seq,
    snapshot_seq: seq - 1,
    committed_at: at,
    schema_environment_version: cx.authority.space.schema_environment_version,
    result: { element: formatElementId({ kind: element.kind, seq: row.id }), op },
    changes: [change],
  })
  return row.version
}

/**
 * Merges one member into an element's Governance block.
 *
 * A merge rather than a replacement, and `null` removes rather than stores:
 * classification, authority ceiling and quarantine reason are separate decisions
 * under separate permissions, and changing one must not silently drop another.
 */
function setMember(block: JsonMap, key: string, value: string | null): JsonMap {
  const out: JsonMap = { ...block }
  if (value === null || value === '') delete out[key]
  else out[key] = value
  return out
}

function audit(
  cx: ElementGovernanceContext,
  id: ElementId,
  operation: string,
  record: Record<string, Json>,
): void {
  cx.store.governance.recordMutation({
    operation,
    space_id: cx.space,
    resource: formatElementId(id),
    principal_id: cx.auth.principal_id,
    record: { ...record, element: formatElementId(id) } as Json,
  })
}
