import { errors } from '../errors.js'
import { dependencyValidity } from './dependency.js'
import { sha256Text } from '../digest.js'
import { canonicalJson } from '../json.js'
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

import { formatElementId, tryParseElementId, type ElementId } from '../id.js'
import { isJsonMap, jsonEquals, type Json, type JsonMap } from '../json.js'
import {
  lineageText,
  parseSymbolRef,
  predicateDef,
  predicateRules,
  type PredicateRules,
} from '../schema/index.js'
import { endpointFromJson, endpointKey } from '../term.js'
import {
  State,
  type AssertionRow,
  type PropositionRow,
} from '../store/index.js'
import type { Context } from '../kql/context.js'
import { admits, modeExclusion, type Policy } from './policy.js'

/** One Assertion, reduced to what the projection scores. */
interface Candidate {
  id: string
  /** The actor's equality key, or a per-Assertion sentinel when it has none. */
  actor: string
  evidence: string[]
  stance: string
  confidence: number
  /** The actor as a reader can follow it, for the ledger. */
  actorRef: Json
  /** Whether this claim supports a *rival* value of a functional slot. */
  opposesTarget: boolean
}

/**
 * One corroboration group: the independent root §23.3 counts once.
 *
 * Reported rather than merely counted, because §27.2 asks a side for its
 * `root_groups` and §27.4 lists corroboration groups among what an Epistemic
 * Ledger contains. A count says two sources agreed; the groups say *which*
 * two, which is what lets a reader check they were really independent.
 */
interface Group {
  /** The semantic actors behind this root, as references. */
  actors: Json[]
  /** The Evidence records it rests on. */
  evidence: string[]
  assertion_ids: string[]
  contribution: number
}

/** The Assertions on each side, and the ones left out. */
interface Ledger {
  supporting: string[]
  opposing: string[]
  uncertain: string[]
  excluded: { assertion_id: string; reason: string }[]
  supportGroups: Group[]
  oppositionGroups: Group[]
  warnings: string[]
}

/** A projected belief, as a query binds and projects it. */
export interface Belief {
  basis: JsonMap
  candidateStatus: string
  conflictRefs: string[]
  conflictReasons: string[]
  /**
   * The Proposition projected, when one durably exists.
   *
   * `null` is a real answer rather than a missing field: a fully grounded
   * `BELIEF` over a tuple no Proposition has been created for answers
   * `insufficient` with no id (§46.4), and a read must not create the
   * Proposition just to have something to point at.
   */
  proposition: ElementId | null
  status: string
  support: number
  opposition: number
  ledger: Ledger
  policy: Policy
  validAt: string
  /** The cognitive coordinate it read, when the read was bound to one. */
  asOf: number | null
}

/**
 * The warnings every projection carries while stages 9 and 10 are missing.
 *
 * They are not decoration. A caller that reads `accepted` without them would
 * believe the engine weighed who said it and how good the evidence was.
 */
const MISSING_STAGE_WARNINGS = [
  'protected actor trust weights are applied; evidence quality is not automatically graded',
  'no evidence-quality evaluation is applied: a cited Evidence record is counted ' +
    'for its independence, never for how good it is',
]

/** Projects the belief about one Proposition. */
export function project(
  cx: Context,
  proposition: ElementId,
  policy: Policy,
  validAt: string = cx.validAt,
): Belief {
  checkProjectionHistory(cx, policy)
  const ledger: Ledger = {
    supporting: [],
    opposing: [],
    uncertain: [],
    excluded: [],
    supportGroups: [],
    oppositionGroups: [],
    warnings: [...MISSING_STAGE_WARNINGS],
  }

  const candidates: Candidate[] = []
  for (const row of assertionsAbout(cx, proposition)) {
    const candidate = admit(cx, row, policy, ledger, false, validAt)
    if (candidate !== null) candidates.push(candidate)
  }

  const localLedger = { ...ledger }
  const [localSupport, localGroups] = aggregate(candidates, false)
  const [localOpposition, localOpposingGroups] = aggregate(candidates, true)
  localLedger.supportGroups = localGroups
  localLedger.oppositionGroups = localOpposingGroups
  const candidateStatus = classify(localSupport, localOpposition, localLedger, policy)
  const conflictRefs: string[] = []
  const boundaries: string[] = []
  for (const row of assertionsAbout(cx, proposition)) boundaries.push(row.valid_from, row.valid_until)
  // Conflict-set expansion (§25, §20.15): support for a rival value of a
  // functional slot is opposition to this one. The schema says the slot holds
  // one value, so somebody claiming another value *is* disagreeing — even
  // though no Assertion anywhere says "not this". `complete` says the
  // candidates are exclusive, which this expansion already treats them as;
  // `boolean_completeness` does the same for `true` against `false` on a
  // non-functional Predicate; and `temporal_conflict: "none"` turns the whole
  // rule off, because values that never conflict on time never conflict.
  const rules = predicateRulesOf(cx, proposition)
  if (policy.expand_conflicts && rules.temporal_conflict !== 'none') {
    for (const rival of exclusiveRivals(cx, proposition, rules)) {
      const rivalCandidates: Candidate[] = []
      for (const row of assertionsAbout(cx, rival)) {
        boundaries.push(row.valid_from, row.valid_until)
        if (row.stance !== 'support') continue
        const candidate = admit(cx, row, policy, ledger, true, validAt)
        if (candidate !== null) rivalCandidates.push(candidate)
      }
      const [rivalSupport] = aggregate(rivalCandidates.map((c) => ({ ...c, opposesTarget: false })), false)
      if (localSupport >= policy.material && rivalSupport >= policy.material) conflictRefs.push(formatElementId(rival))
      candidates.push(...rivalCandidates)
    }
  }

  const [support, supportGroups] = aggregate(candidates, false)
  const [opposition, oppositionGroups] = aggregate(candidates, true)
  ledger.supportGroups = supportGroups
  ledger.oppositionGroups = oppositionGroups

  if (!rules.open_world) {
    ledger.warnings.push(
      'the Predicate is declared closed-world (§24.2): an absence of ' +
        'Propositions may be read as closed-world, but this projection still ' +
        'reports insufficient rather than inferring rejection from silence',
    )
  }

  let unverified = false
  for (const row of assertionsAbout(cx, proposition)) {
    if ((row.mode !== 'inferred' && !cx.store.controlAt(cx.space, `identity_review/A-${row.id}`, cx.asOf ?? cx.store.currentSeq(cx.space))) || !ledger.supporting.includes(formatElementId({ kind: 'Assertion', seq: row.id }))) continue
    const checked = dependencyValidity(cx, { kind: 'Assertion', row }, policy, validAt)
    if (checked.action_eligible !== true) unverified = true
    const next = (checked.basis as JsonMap).next_invalid_at
    if (typeof next === 'string') boundaries.push(next)
  }
  let status = conflictRefs.length ? 'contested' : classify(support, opposition, ledger, policy)
  if (unverified && status === 'accepted') {
    status = 'uncertain'
    ledger.warnings.push('inferred support has no verified recursive dependency basis')
  }
  return {
    proposition,
    status,
    basis: projectionBasis(cx, policy, validAt, boundaries.filter((t) => t > validAt).sort()[0] ?? null),
    candidateStatus, conflictRefs,
    conflictReasons: conflictRefs.length ? [rules.functional ? 'functional_value' : 'exclusive_value'] : [],
    support,
    opposition,
    ledger,
    policy,
    validAt,
    asOf: cx.asOf ?? null,
  }
}

/**
 * The answer for a fully grounded `BELIEF` whose Proposition does not exist
 * (§46.4).
 *
 * Nobody has asserted a tuple nobody has created, so the honest answer is
 * `insufficient` with a null id — not an empty result set. Returning no row
 * would make the Agent infer "unknown" from "the pattern did not match", which
 * is the inference §24 exists to prevent, and it is indistinguishable from a
 * query that was simply written wrong.
 *
 * A read must not create the Proposition to have something to point at.
 */
export function ungroundedBelief(
  cx: Context,
  policy: Policy,
  validAt: string,
 ): Belief {
  checkProjectionHistory(cx, policy)
  return {
    proposition: null,
    basis: projectionBasis(cx, policy, validAt),
    candidateStatus: 'insufficient', conflictRefs: [], conflictReasons: [],
    status: 'insufficient',
    support: 0,
    opposition: 0,
    ledger: {
      supporting: [],
      opposing: [],
      uncertain: [],
      excluded: [],
      supportGroups: [],
      oppositionGroups: [],
      warnings: [
        'no Proposition exists for this tuple in this Space, so nothing has ' +
          'been asserted about it; that is an open-world absence, not a denial',
      ],
    },
    policy,
    validAt,
    asOf: cx.asOf ?? null,
  }
}

/** Whether one Assertion is eligible, recording why when it is not. */
function admit(
  cx: Context,
  row: AssertionRow,
  policy: Policy,
  ledger: Ledger,
  opposesTarget: boolean,
  validAt: string,
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
  const exclude = (reason: string): null => { ledger.excluded.push({ assertion_id: id, reason }); return null }
  if ((row.valid_from && row.valid_from > validAt) || (row.valid_until && row.valid_until <= validAt)) return exclude('outside_valid_time')
  for (const reference of row.context_refs) {
    const canonical = cx.canonicalEndpoint(reference as Json) as JsonMap
    if (!policy.context_refs.includes(String(canonical.id))) return exclude('context_mismatch')
  }
  for (const reference of row.evidence_refs) {
    const eid = tryParseElementId(reference.id)
    const root = eid === null ? null : cx.load(eid)
    if (!root || root.kind !== 'Evidence') return exclude('evidence_unavailable')
    if (root.row.status === 'corrected') return exclude('corrected_evidence')
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
    // The equality key is internal — it carries separators no reader should
    // have to parse — so the reference travels alongside it for the ledger.
    actorRef: (row.asserted_by ?? null) as Json,
    evidence: row.evidence_refs.map((ref) => ref.id),
    stance: row.stance,
    confidence: (row.confidence < 0 ? policy.unstated_confidence : row.confidence) * (policy.trust_weights[(row.asserted_by as JsonMap)?.id as string] ?? policy.default_trust_weight),
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
): [number, Group[]] {
  const side = candidates.filter((candidate) =>
    opposing
      ? candidate.opposesTarget || candidate.stance === 'reject'
      : !candidate.opposesTarget && candidate.stance === 'support',
  )
  if (side.length === 0) return [0, []]

  interface Bucket {
    keys: Set<string>
    actors: Json[]
    evidence: Set<string>
    assertions: Set<string>
    confidence: number
  }
  const absorb = (into: Bucket, other: Bucket): void => {
    for (const key of other.keys) into.keys.add(key)
    for (const id of other.evidence) into.evidence.add(id)
    for (const id of other.assertions) into.assertions.add(id)
    for (const actor of other.actors) {
      if (!into.actors.some((held) => jsonEquals(held, actor))) into.actors.push(actor)
    }
    into.confidence = Math.max(into.confidence, other.confidence)
  }
  const groups: Bucket[] = []
  for (const candidate of side) {
    const keys = new Set<string>([`actor:${candidate.actor}`])
    for (const id of candidate.evidence) keys.add(`evidence:${id}`)
    const arriving: Bucket = {
      keys,
      actors: [candidate.actorRef],
      evidence: new Set(candidate.evidence),
      assertions: new Set([candidate.id]),
      confidence: candidate.confidence,
    }

    const overlapping = groups.filter((group) =>
      [...keys].some((key) => group.keys.has(key)),
    )
    if (overlapping.length === 0) {
      groups.push(arriving)
      continue
    }
    const merged = overlapping[0] as Bucket
    absorb(merged, arriving)
    for (const other of overlapping.slice(1)) {
      absorb(merged, other)
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
  return [
    score,
    groups.map((group) => ({
      actors: group.actors,
      evidence: [...group.evidence].sort(),
      assertion_ids: [...group.assertions].sort(),
      contribution: group.confidence,
    })),
  ]
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

/**
 * The side the policy would favor if forced to choose (§27.2, §21.6).
 *
 * `support` under `accepted`, `opposition` under `rejected`, and under
 * `contested` the side with more eligible independent roots — the
 * corroboration groups §23 counts once each — with an exact tie, `uncertain`
 * and `insufficient` reporting `none`. Disclosure for a consumer that must act
 * anyway; it never changes `status`.
 */
export function leadingSide(belief: Belief): 'support' | 'opposition' | 'none' {
  switch (belief.status) {
    case 'accepted':
      return 'support'
    case 'rejected':
      return 'opposition'
    case 'contested': {
      const support = belief.ledger.supportGroups.length
      const opposition = belief.ledger.oppositionGroups.length
      if (support === opposition) return 'none'
      return support > opposition ? 'support' : 'opposition'
    }
    default:
      return 'none'
  }
}

/** The projection output a query binds and projects (§27.2). */
export function beliefToJson(belief: Belief): JsonMap {
  return {
    proposition_id:
      belief.proposition === null ? null : formatElementId(belief.proposition),
    status: belief.status,
    basis: belief.basis,
    candidate_status: belief.candidateStatus,
    slot_status: belief.status,
    conflict_refs: belief.conflictRefs,
    conflict_reasons: belief.conflictReasons,
    leading: leadingSide(belief),
    // The Assertion ids and the corroboration groups *are* the ledger: they
    // name who said it and which observations stood behind them. A caller that
    // asked for no explanation is not handed them under another key (§49.2).
    support: side(belief, belief.support, belief.ledger.supporting, belief.ledger.supportGroups),
    opposition: side(
      belief,
      belief.opposition,
      belief.ledger.opposing,
      belief.ledger.oppositionGroups,
    ),
    uncertainty: {
      level: uncertaintyLevel(belief),
      // Uncertainty is not `1 - confidence`: it has causes, and naming them is
      // what makes it actionable.
      reasons: uncertaintyReasons(belief),
    },
    temporal: { valid_at: belief.validAt, as_of_seq: belief.asOf },
    policy: { id: belief.policy.id, version: belief.policy.version },
    // §49.1, §49.2: `none` returns no ledger at all rather than an empty one.
    // An empty object reads as "we looked and found nothing to explain", and
    // what happened is that the caller declined to be told.
    ...(belief.policy.explanation === 'none'
      ? {}
      : belief.policy.explanation === 'summary'
        ? {
            explanation: {
              excluded_count: belief.ledger.excluded.length,
              uncertain_count: belief.ledger.uncertain.length,
              warnings: belief.ledger.warnings,
            },
          }
        : {
            explanation: {
              excluded: belief.ledger.excluded as unknown as Json,
              uncertain_assertions: belief.ledger.uncertain,
              warnings: belief.ledger.warnings,
            },
          }),
  }
}

/** One side of the projection, with the roots its score came from (§27.2). */
function side(
  belief: Belief,
  score: number,
  assertionIds: string[],
  groups: Group[],
): JsonMap {
  const disclosed = belief.policy.explanation === 'ledger'
  return {
    score,
    // Said out loud, because a number between 0 and 1 looks like a probability
    // and this one is not calibrated as one.
    score_semantics: 'normalized_support_not_probability',
    assertion_ids: disclosed ? assertionIds : [],
    root_groups: (disclosed ? groups : []) as unknown as Json,
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
  const supportGroups = belief.ledger.supportGroups.length
  const oppositionGroups = belief.ledger.oppositionGroups.length
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
 * Renders a slot projection: the conflict set, not a winner (§47.3).
 *
 * `accepted_values` is a *list* because a functional slot with two accepted
 * values is a real state the Brain can be in, and reporting one of them would
 * be picking a side the record does not.
 *
 * `status` leads, because §47.4 asks a grounded empty slot to answer
 * `insufficient` with an empty `accepted_values` rather than force the Agent
 * to infer unknown from zero raw rows — and an Agent that has to derive the
 * slot's state by scanning `candidate_projections` is doing exactly that.
 * `subject`, `predicate_ref`, `leading` and `contested` are additive: they
 * name what the slot was about and which side is ahead *without* claiming it
 * settled anything.
 *
 * The slot reports its own `policy` and `temporal`, from the coordinates it
 * *ran* under rather than from whichever candidate happened to come first.
 * Reading them off a candidate leaves them null exactly when the slot is
 * empty — which is the case §47.4 is about, and the one where a caller most
 * needs to know the answer was computed rather than skipped.
 */
export function slotToJson(
  subject: Json,
  predicateRef: string,
  slot: Slot,
): JsonMap {
  const beliefs = slot.candidates
  const accepted = beliefs.filter((belief) => belief.status === 'accepted')
  const engaged = beliefs.filter((belief) => belief.status !== 'insufficient')
  const leading = [...engaged].sort((a, b) => b.support - a.support)[0]
  // Two accepted values in one slot is a contradiction the caller has to see,
  // even though each candidate was accepted on its own.
  const contested =
    beliefs.some((belief) => belief.status === 'contested')

  // §47.3's four statuses, decided over the slot rather than over any one
  // candidate.
  const status = contested
    ? 'contested'
    : accepted.length > 0
      ? 'accepted'
      : engaged.length > 0
        ? 'uncertain'
        : 'insufficient'

  return {
    status,
    basis: { ...slot.basis, next_invalid_at: beliefs.map((b) => b.basis.next_invalid_at).filter((t): t is string => typeof t === 'string').sort()[0] ?? null },
    subject,
    predicate_ref: predicateRef,
    accepted_values: accepted.flatMap((belief) =>
      belief.proposition === null ? [] : [formatElementId(belief.proposition)],
    ),
    candidate_projections: beliefs.map(beliefToJson) as unknown as Json,
    leading:
      leading === undefined || leading.proposition === null
        ? null
        : formatElementId(leading.proposition),
    contested,
    uncertainty: {
      level:
        status === 'insufficient'
          ? 'total'
          : status === 'contested' || status === 'uncertain'
            ? 'high'
            : 'low',
      reasons: leading === undefined ? [] : uncertaintyReasons(leading),
    },
    temporal: { valid_at: slot.validAt, as_of_seq: slot.asOf },
    policy: { id: slot.policy.id, version: slot.policy.version },
    // §47.3 lists an explanation on the slot too. A slot's own explanation is
    // about the *set*: how many candidates competed for it, and what this
    // engine could not weigh between them.
    explanation: {
      candidate_count: beliefs.length,
      accepted_count: accepted.length,
      warnings: slot.warnings,
    },
  }
}

/**
 * One subject-predicate slot, projected (§47.2).
 *
 * Carries the coordinates the projection ran under, so the slot can report
 * them whether or not any candidate exists.
 */
export interface Slot {
  basis: JsonMap
  candidates: Belief[]
  policy: Policy
  validAt: string
  asOf: number | null
  warnings: string[]
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
  const rows = cx.store.all<AssertionRow>(
    'assertions',
    `SELECT * FROM assertions WHERE space = ? AND proposition_id = ?
       ORDER BY id`,
    cx.space,
    target,
  )
  cx.spend('scans', rows.length)
  const visible: AssertionRow[] = []
  for (const decoded of rows) {
    const id = cx.remember({ kind: 'Assertion', row: decoded })
    if (cx.view(id) === null) continue
    visible.push(decoded)
  }
  return visible
}

/** The Predicate's declarations behind one Proposition, with §20.15's defaults. */
function predicateRulesOf(cx: Context, target: ElementId): PredicateRules {
  const element = cx.load(target)
  if (element === null || element.kind !== 'Proposition') return predicateRules(undefined)
  try {
    const symbol = parseSymbolRef(element.row.predicate_ref)
    const definition = cx.env.definitionPackage(symbol)
    return predicateRules(
      definition === undefined ? undefined : predicateDef(definition, symbol.name),
    )
  } catch {
    // A predicate this environment cannot resolve declares nothing, so it
    // declares no exclusivity either.
    return predicateRules(undefined)
  }
}

/**
 * The Propositions whose support counts against this one (§25, §20.15).
 *
 * On a `functional` slot every other object is a rival — the slot holds one
 * value, and `complete` only says out loud that accepting one rejects the
 * others. Under `boolean_completeness` the one rival is the other boolean
 * value: `(s, p, false)` is the negation of `(s, p, true)`, and nothing else
 * in the slot is.
 */
export function exclusiveRivals(
  cx: Context,
  target: ElementId,
  rules: PredicateRules,
): ElementId[] {
  const element = cx.load(target)
  if (element === null || element.kind !== 'Proposition') return []
  const row = element.row
  const others = () =>
    slotPropositions(cx, [row.subject_key], lineageText(row.predicate_ref)).filter(
      (id) => id.seq !== target.seq,
    )

  if (rules.functional) return others()
  if (rules.boolean_completeness) {
    const object = row.object as Json
    const value = isJsonMap(object) ? object.value : undefined
    if (typeof value !== 'boolean') return []
    const negation = endpointKey(endpointFromJson({ value: !value, datatype: 'kip:boolean' }))
    return others().filter((id) => {
      const rival = cx.load(id)
      return rival?.kind === 'Proposition' && rival.row.object_key === negation
    })
  }
  return []
}

/**
 * Every active Proposition in one `(subject, predicate)` slot (§47.2).
 *
 * The subject is a set of endpoint keys — every merged spelling of one
 * canonical identity (§43.2) — and the predicate a lineage (§20.14), so the
 * slot sees every Assertion in it whichever version or spelling its
 * Proposition was created under.
 */
export function slotPropositions(
  cx: Context,
  subjectKeys: readonly string[],
  predicateLineage: string,
): ElementId[] {
  if (subjectKeys.length === 0) return []
  if (cx.historical) {
    return cx
      .reconstruct('Proposition')
      .filter((element) => {
        const row = element.row as PropositionRow
        return (
          row.state === State.ACTIVE &&
          subjectKeys.includes(row.subject_key) &&
          (row.predicate_lineage === '' ? lineageText(row.predicate_ref) : row.predicate_lineage) ===
            predicateLineage
        )
      })
      .map((element) => ({ kind: 'Proposition', seq: element.row.id }) as ElementId)
  }

  // The whole row, so each rival is remembered through the visibility check
  // rather than named by id alone. A rival this caller may not read must not
  // widen a functional predicate's conflict set: its Assertions would then be
  // read on the caller's behalf and reported as contest.
  const rows = cx.store.all<PropositionRow>(
    'propositions',
    `SELECT * FROM propositions
       WHERE space = ? AND state = ?
         AND subject_key IN (SELECT value FROM json_each(?))
         AND predicate_lineage = ?
       ORDER BY id`,
    cx.space,
    State.ACTIVE,
    JSON.stringify(subjectKeys),
    predicateLineage,
  )
  cx.spend('scans', rows.length)
  const visible: ElementId[] = []
  for (const row of rows) {
    const id = cx.remember({ kind: 'Proposition', row })
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
  type Policy,
} from './policy.js'

/** Full computation basis. Opaque digests never expose grants or hidden counts. */
export function projectionBasis(cx: Context, policy: Policy, at: string, next: string | null = null): JsonMap {
  const contextRefs = policy.context_refs
  const identityVersions = (cx.authority.space.policies._kip_identity_changes ?? []) as number[]
  const { _kip_identity_changes: _identity, ...policies } = cx.authority.space.policies
  const authorization = { ...cx.authority, space: { ...cx.authority.space, seq: 0, schema_environment_version: 0, policies }, auth: cx.auth }

  return {
    space_id: cx.space, snapshot_seq: cx.asOf ?? cx.store.currentSeq(cx.space),
    schema_environment_version: cx.env.version, identity_version: Math.max(0, ...identityVersions.filter((v) => v <= (cx.asOf ?? cx.store.currentSeq(cx.space)))),
    policy: { id: policy.id, version: `sha256:${sha256Text(canonicalJson([policy.version, policy.accept, policy.material, policy.modes, policy.expand_conflicts, policy.unstated_confidence]))}` },
    trust_version: policy.trust_version,
    authorization_view: sha256Text(canonicalJson(authorization)),
    context_refs: contextRefs, purpose: policy.purpose || cx.auth.purpose || 'unspecified', risk: policy.risk || cx.auth.risk || 'unspecified',
    valid_at: at, next_invalid_at: next,
  }
}

export function checkProjectionHistory(_cx: Context, policy: Policy): void {
  if (!policy.trust_version || policy.trust_version === 'unavailable') throw errors.historicalSnapshotUnavailable('projection control history unavailable')
}
