/**
 * # The Epistemic Projection
 *
 * Belief is **projected from the Assertions on record**, never stored. That is
 * the whole reason KIP 2.0 separates a Proposition from an Assertion about it:
 * the tuple is truth-neutral, and what this Brain currently holds is a function
 * of the claims, the policy and the moment you ask.
 *
 * Three rules do most of the work, and each of them is a way of not lying:
 *
 * **Silence is insufficient, never rejection.** Nobody having said anything is
 * the open-world state (§21.5). Reporting it as `rejected` would turn an absence
 * of records into a claim about the world.
 *
 * **Repetition is not corroboration.** One actor asserting the same thing five
 * times is one source. Claims are grouped by actor *and* by the Evidence they
 * cite, each group contributes its strongest claim once, and independent groups
 * accumulate with diminishing returns.
 *
 * **Shared Evidence merges groups.** Two people relaying the same observation
 * are one observation, and a third claim citing both collapses two apparently
 * independent groups into one — which is exactly the shape manufactured
 * corroboration takes.
 *
 * ## What is deliberately not here
 *
 * Trust evaluation and evidence-quality assessment. Every eligible group counts
 * equally, and **every projection says so in its warnings**. Do not remove that
 * warning without implementing the stages: a score that silently assumes all
 * sources are equally trustworthy, without saying so, is worse than one that
 * refuses — the caller cannot tell the difference from a calibrated answer.
 */

import { formatElementId, type ElementId } from '../id.js'
import type { Json, JsonMap } from '../json.js'
import { predicateDef } from '../schema/index.js'
import { parseSymbolRef } from '../schema/index.js'
import {
  State,
  type AssertionRow,
  type PropositionRow,
  type SqlRow,
} from '../store/index.js'
import { decodeRow } from '../store/codec.js'
import type { Context } from '../kql/context.js'
import { nowTime } from '../time.js'
import { admits, modeExclusion, type Policy } from './policy.js'

/** One Assertion, reduced to what the projection scores. */
interface Candidate {
  id: string
  /** The actor's equality key, or a per-Assertion sentinel when it has none. */
  actor: string
  evidence: string[]
  stance: string
  confidence: number
  /** Whether this claim supports a *rival* value of a functional slot. */
  opposesTarget: boolean
}

/** The Assertions on each side, and the ones left out. */
interface Ledger {
  supporting: string[]
  opposing: string[]
  uncertain: string[]
  excluded: { assertion_id: string; reason: string }[]
  supportGroups: number
  oppositionGroups: number
  warnings: string[]
}

/** A projected belief, as a query binds and projects it. */
export interface Belief {
  proposition: ElementId
  status: string
  support: number
  opposition: number
  ledger: Ledger
  policy: Policy
  validAt: string
}

/**
 * The warnings every projection carries while stages 9 and 10 are missing.
 *
 * They are not decoration. A caller that reads `accepted` without them would
 * believe the engine weighed who said it and how good the evidence was.
 */
const MISSING_STAGE_WARNINGS = [
  'no trust model is applied: every eligible corroboration group counts equally, ' +
    'whoever asserted it',
  'no evidence-quality evaluation is applied: a cited Evidence record is counted ' +
    'for its independence, never for how good it is',
]

/** Projects the belief about one Proposition. */
export function project(
  cx: Context,
  proposition: ElementId,
  policy: Policy,
  validAt: string = nowTime(),
): Belief {
  const ledger: Ledger = {
    supporting: [],
    opposing: [],
    uncertain: [],
    excluded: [],
    supportGroups: 0,
    oppositionGroups: 0,
    warnings: [...MISSING_STAGE_WARNINGS],
  }

  const candidates: Candidate[] = []
  for (const row of assertionsAbout(cx, proposition)) {
    const candidate = admit(row, policy, ledger, false)
    if (candidate !== null) candidates.push(candidate)
  }

  // Conflict-set expansion (§58): support for a rival value of a functional
  // slot is opposition to this one. The schema says the slot holds one value,
  // so somebody claiming another value *is* disagreeing — even though no
  // Assertion anywhere says "not this".
  if (policy.expand_conflicts) {
    for (const rival of functionalRivals(cx, proposition)) {
      for (const row of assertionsAbout(cx, rival)) {
        const candidate = admit(row, policy, ledger, true)
        if (candidate !== null) candidates.push(candidate)
      }
    }
  }

  const [support, supportGroups] = aggregate(candidates, false)
  const [opposition, oppositionGroups] = aggregate(candidates, true)
  ledger.supportGroups = supportGroups
  ledger.oppositionGroups = oppositionGroups

  return {
    proposition,
    status: classify(support, opposition, ledger, policy),
    support,
    opposition,
    ledger,
    policy,
    validAt,
  }
}

/** Whether one Assertion is eligible, recording why when it is not. */
function admit(
  row: AssertionRow,
  policy: Policy,
  ledger: Ledger,
  opposesTarget: boolean,
): Candidate | null {
  const id = formatElementId({ kind: 'Assertion', seq: row.id })

  // A retracted claim was withdrawn and a superseded one was revised: both are
  // history, and history is not what this Brain currently holds (§59).
  if (row.status !== 'active') {
    ledger.excluded.push({ assertion_id: id, reason: `lifecycle_${row.status}` })
    return null
  }
  if (row.state !== State.ACTIVE) {
    ledger.excluded.push({ assertion_id: id, reason: `record_${row.state}` })
    return null
  }
  if (!admits(policy, row.mode)) {
    ledger.excluded.push({ assertion_id: id, reason: modeExclusion(row.mode) })
    return null
  }
  // An `uncertain` stance engages the question without taking a side: it keeps
  // the belief out of `insufficient` without pushing it either way.
  if (row.stance === 'uncertain') {
    ledger.uncertain.push(id)
    return null
  }

  const side = opposesTarget ? ledger.opposing : row.stance === 'reject' ? ledger.opposing : ledger.supporting
  side.push(id)

  return {
    id,
    // An Assertion with no recorded actor cannot be grouped with anything, so
    // it is its own group rather than joining a nameless one with every other
    // unattributed claim.
    actor: row.asserted_by_key === '' ? `anonymous:${id}` : row.asserted_by_key,
    evidence: row.evidence_refs.map((ref) => ref.evidence_id),
    stance: row.stance,
    confidence: row.confidence < 0 ? policy.unstated_confidence : row.confidence,
    opposesTarget,
  }
}

/**
 * Groups one side by independence and scores it.
 *
 * Union-find over actor and Evidence keys: a claim joins every group it shares
 * a key with, and joining two of them merges them, because a claim bridging two
 * apparently independent groups proves they were not.
 */
function aggregate(
  candidates: readonly Candidate[],
  opposing: boolean,
): [number, number] {
  const side = candidates.filter((candidate) =>
    opposing
      ? candidate.opposesTarget || candidate.stance === 'reject'
      : !candidate.opposesTarget && candidate.stance === 'support',
  )
  if (side.length === 0) return [0, 0]

  const groups: { keys: Set<string>; confidence: number }[] = []
  for (const candidate of side) {
    const keys = new Set<string>([`actor:${candidate.actor}`])
    for (const id of candidate.evidence) keys.add(`evidence:${id}`)

    const overlapping = groups.filter((group) =>
      [...keys].some((key) => group.keys.has(key)),
    )
    if (overlapping.length === 0) {
      groups.push({ keys, confidence: candidate.confidence })
      continue
    }
    const merged = overlapping[0] as { keys: Set<string>; confidence: number }
    for (const key of keys) merged.keys.add(key)
    merged.confidence = Math.max(merged.confidence, candidate.confidence)
    for (const other of overlapping.slice(1)) {
      for (const key of other.keys) merged.keys.add(key)
      merged.confidence = Math.max(merged.confidence, other.confidence)
      groups.splice(groups.indexOf(other), 1)
    }
  }

  // Independent groups accumulate with diminishing returns: two moderate
  // independent sources say more than either alone. Nothing here is a
  // calibrated probability, which is why the score is declared as normalized
  // strength rather than reported as one.
  const score =
    1 -
    groups.reduce(
      (acc, group) => acc * (1 - Math.min(Math.max(group.confidence, 0), 1)),
      1,
    )
  return [score, groups.length]
}

/** Belief-state classification (§68–§73). */
function classify(
  support: number,
  opposition: number,
  ledger: Ledger,
  policy: Policy,
): string {
  const engaged =
    ledger.supporting.length > 0 ||
    ledger.opposing.length > 0 ||
    ledger.uncertain.length > 0
  // The open-world state. Nobody has spoken, which is not a denial.
  if (!engaged) return 'insufficient'
  if (support >= policy.accept && opposition < policy.material) return 'accepted'
  // Rejection needs positive opposition, and is never inferred from an absence
  // of support (§21.5).
  if (opposition >= policy.accept && support < policy.material) return 'rejected'
  if (support >= policy.material && opposition >= policy.material) {
    return 'contested'
  }
  return 'uncertain'
}

/** The projection output a query binds and projects (§75). */
export function beliefToJson(belief: Belief): JsonMap {
  return {
    proposition_id: formatElementId(belief.proposition),
    status: belief.status,
    support: {
      score: belief.support,
      // Said out loud, because a number between 0 and 1 looks like a
      // probability and this one is not calibrated as one.
      score_semantics: 'normalized_support_not_probability',
      assertion_ids: belief.ledger.supporting,
      independent_groups: belief.ledger.supportGroups,
    },
    opposition: {
      score: belief.opposition,
      score_semantics: 'normalized_support_not_probability',
      assertion_ids: belief.ledger.opposing,
      independent_groups: belief.ledger.oppositionGroups,
    },
    uncertainty: {
      level: uncertaintyLevel(belief),
      // Uncertainty is not `1 - confidence`: it has causes, and naming them is
      // what makes it actionable.
      reasons: uncertaintyReasons(belief),
    },
    temporal: { valid_at: belief.validAt },
    policy: { id: belief.policy.id, version: belief.policy.version },
    explanation: {
      excluded: belief.ledger.excluded as unknown as Json,
      uncertain_assertions: belief.ledger.uncertain,
      warnings: belief.ledger.warnings,
    },
  }
}

function uncertaintyLevel(belief: Belief): string {
  switch (belief.status) {
    case 'insufficient':
      return 'total'
    case 'contested':
    case 'uncertain':
      return 'high'
    default:
      return 'low'
  }
}

function uncertaintyReasons(belief: Belief): string[] {
  const reasons: string[] = []
  const { supportGroups, oppositionGroups } = belief.ledger
  if (supportGroups === 0 && oppositionGroups === 0) {
    reasons.push('no eligible assertions')
  }
  if (supportGroups > 0 && oppositionGroups > 0) {
    reasons.push(
      `${supportGroups} independent source(s) support and ` +
        `${oppositionGroups} oppose`,
    )
  }
  if (supportGroups === 1 && oppositionGroups === 0) {
    reasons.push('a single independent source')
  }
  return reasons
}

/**
 * Renders a slot projection: the conflict set, not a winner (§35).
 *
 * The field names follow the agent-facing syntax card, which promises
 * `accepted_values` and `candidate_projections`. `accepted_values` is a *list*
 * because a functional slot with two accepted values is a real state the Brain
 * can be in, and reporting one of them would be picking a side the record does
 * not.
 */
export function slotToJson(
  subject: Json,
  predicateRef: string,
  beliefs: readonly Belief[],
): JsonMap {
  const accepted = beliefs.filter((belief) => belief.status === 'accepted')
  const engaged = beliefs.filter((belief) => belief.status !== 'insufficient')
  const leading = [...engaged].sort((a, b) => b.support - a.support)[0]
  return {
    subject,
    predicate_ref: predicateRef,
    accepted_values: accepted.map((belief) =>
      formatElementId(belief.proposition),
    ),
    candidate_projections: beliefs.map(beliefToJson) as unknown as Json,
    leading: leading === undefined ? null : formatElementId(leading.proposition),
    contested: beliefs.some((belief) => belief.status === 'contested'),
  }
}

// --- reads ------------------------------------------------------------------

/**
 * Every Assertion about one Proposition that this caller may read.
 *
 * Through `Context`'s choke point, which is what gives the projection its
 * governance-visibility stage for free: an Assertion outside the caller's query
 * universe must not contribute to a belief, because the belief's status and
 * score would then be derived from content the caller is not entitled to — a
 * number that answers the question the visibility rule refused.
 *
 * Silence and exclusion look the same to the projection, which is correct here:
 * a caller who cannot see the dissent gets `accepted` rather than `contested`,
 * exactly as it would if the dissent had never been written. Reporting
 * "contested, but you may not see why" would be the disclosure.
 */
function assertionsAbout(cx: Context, proposition: ElementId): AssertionRow[] {
  const target = formatElementId(proposition)
  // At a past coordinate the projection sees only the Assertions that existed
  // then: a belief computed from today's commitments and reported under a past
  // coordinate would be an answer to neither question.
  if (cx.historical) {
    return cx
      .reconstruct('Assertion')
      .map((element) => element.row as AssertionRow)
      .filter((row) => row.proposition_id === target)
  }
  const rows = cx.store.sql
    .exec<SqlRow>(
      `SELECT * FROM assertions WHERE space = ? AND proposition_id = ?
         ORDER BY id`,
      cx.space,
      target,
    )
    .toArray()
  cx.spend('scans', rows.length)
  const visible: AssertionRow[] = []
  for (const row of rows) {
    const decoded = decodeRow<AssertionRow>('assertions', row)
    const id = cx.remember({ kind: 'Assertion', row: decoded })
    if (cx.view(id) === null) continue
    visible.push(decoded)
  }
  return visible
}

/** The Propositions competing with this one for a functional slot. */
export function functionalRivals(
  cx: Context,
  target: ElementId,
): ElementId[] {
  const element = cx.load(target)
  if (element === null || element.kind !== 'Proposition') return []
  const row = element.row

  let functional = false
  try {
    const symbol = parseSymbolRef(row.predicate_ref)
    const definition = cx.env.definitionPackage(symbol)
    functional =
      definition !== undefined &&
      predicateDef(definition, symbol.name)?.functional === true
  } catch {
    // A predicate this environment cannot resolve declares nothing, so it
    // declares no exclusivity either.
    return []
  }
  if (!functional) return []

  return slotPropositions(cx, row.subject_key, row.predicate_ref).filter(
    (id) => id.seq !== target.seq,
  )
}

/** Every active Proposition in one `(subject, predicate)` slot. */
export function slotPropositions(
  cx: Context,
  subjectKey: string,
  predicateRef: string,
): ElementId[] {
  if (cx.historical) {
    return cx
      .reconstruct('Proposition')
      .filter((element) => {
        const row = element.row as PropositionRow
        return (
          row.state === State.ACTIVE &&
          row.subject_key === subjectKey &&
          row.predicate_ref === predicateRef
        )
      })
      .map((element) => ({ kind: 'Proposition', seq: element.row.id }) as ElementId)
  }

  // The whole row, so each rival is remembered through the visibility check
  // rather than named by id alone. A rival this caller may not read must not
  // widen a functional predicate's conflict set: its Assertions would then be
  // read on the caller's behalf and reported as contest.
  const rows = cx.store.sql
    .exec<SqlRow>(
      `SELECT * FROM propositions
         WHERE space = ? AND state = ? AND subject_key = ? AND predicate_ref = ?
         ORDER BY id`,
      cx.space,
      State.ACTIVE,
      subjectKey,
      predicateRef,
    )
    .toArray()
  cx.spend('scans', rows.length)
  const visible: ElementId[] = []
  for (const row of rows) {
    const id = cx.remember({
      kind: 'Proposition',
      row: decodeRow<PropositionRow>('propositions', row),
    })
    if (cx.view(id) !== null) visible.push(id)
  }
  return visible
}

export {
  BASELINE_ID,
  BASELINE_VERSION,
  admits,
  baseline,
  forecast,
  modeExclusion,
  policyFromSettings,
  policyIdentity,
  type Policy,
} from './policy.js'
