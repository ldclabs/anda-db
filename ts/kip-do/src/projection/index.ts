import { parseElementId } from '../id.js'
import { trustWeight } from '../trust.js'
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
import { boundariesAfter, place, succeed, timed, type Placement, type Timed } from './world.js'
import {
  lineageText,
  parseSymbolRef,
  predicateDef,
  predicateRules,
  type PredicateRules,
} from '../schema/index.js'
import { endpointFromJson, endpointKey, referencedElement } from '../term.js'
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
  /** Eligible, but indeterminate at the instant (§25.5). */
  indeterminate: string[]
  excluded: { assertion_id: string; reason: string }[]
  supportGroups: Group[]
  oppositionGroups: Group[]
  warnings: string[]
  /** §27.2 uncertainty reasons: `temporal_indeterminate`, `outranked`. */
  reasons: string[]
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
  /** The precedence rule that decided this candidate, when one did (§21.13). */
  precedence: JsonMap | null
  /** `leading` recomputed over a slot's final conflict set (§21.11). */
  slotLeading: 'support' | 'opposition' | 'none' | null
  /** Inferred support without a verified dependency basis. */
  unverified: boolean
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

/** The Propositions projected together, and how their values relate. */
interface Frame {
  /**
   * Every candidate value, the target included: the whole slot when the
   * predicate constrains one (§20.15).
   */
  members: ElementId[]
  /** Each member's `functional_by` partition: its object's Concept Type lineage. */
  partitions: Map<string, string>
  /** Functional or `functional_by` without `complete`: a conflict set (§21.11). */
  conflict: boolean
  /** Slot lines take part in temporal succession (§25.4). */
  slotLines: boolean
  /** For each member, the rivals whose support opposes it (§25). */
  opposing: Map<string, ElementId[]>
  /** The slot subject's equality key, for first-person testimony (§21.13). */
  subjectKeys: string[]
  /** Which constraint a standing conflict names. */
  reason: string
  rules: PredicateRules
}

const partitionOf = (frame: Frame, member: ElementId): string =>
  frame.partitions.get(formatElementId(member)) ?? ''

/** The Propositions projected together with `target` (§20.15). */
function frameOf(cx: Context, target: ElementId): Frame {
  const rules = predicateRulesOf(cx, target)
  const frame: Frame = {
    members: [target], partitions: new Map(), conflict: false, slotLines: false,
    opposing: new Map(), subjectKeys: [], reason: 'functional_value', rules,
  }
  const element = cx.load(target)
  // §20.15, §25.2: values that never conflict on time form no slot.
  if (element === null || element.kind !== 'Proposition' || rules.temporal_conflict === 'none') return frame
  const slot = rules.functional || rules.functional_by
  if (!slot && !rules.boolean_completeness) return frame
  const row = element.row
  frame.members = slotPropositions(cx, [row.subject_key], cx.env.lineage('PredicateType', row.predicate_ref))
  if (!frame.members.some((id) => id.seq === target.seq)) frame.members.push(target)
  frame.subjectKeys = [row.subject_key]
  frame.slotLines = slot
  // `complete` makes a functional slot's values exclusive: accepting one
  // rejects the others. Without it, competing values are a conflict set and
  // never opposition (§21.11, §25).
  frame.conflict = slot && !rules.complete
  frame.reason = slot ? 'functional_value' : 'exclusive_value'
  const objects = new Map<string, Json>()
  for (const member of frame.members) {
    const value = cx.load(member)
    if (value === null || value.kind !== 'Proposition') continue
    if (rules.functional_by) {
      // The partition is the object's Concept Type lineage.
      let partition = ''
      const object = endpointFromJson(value.row.object as Json)
      if (object.kind === 'local') {
        const concept = cx.load(object.id)
        if (concept !== null && concept.kind === 'Concept') partition = cx.env.lineage('ConceptType', concept.row.schema_ref)
      }
      frame.partitions.set(formatElementId(member), partition)
    }
    objects.set(formatElementId(member), value.row.object as Json)
  }
  for (const [member, object] of objects) {
    // §12.7, §20.15: under `boolean_completeness`, object `false` is the
    // negation of object `true`, so each opposes the other.
    const flag = booleanObject(object)
    const rivals: ElementId[] = []
    for (const [other, value] of objects) {
      if (other === member) continue
      const otherId = parseElementId(other)
      const exclusive =
        (slot && rules.complete && partitionOf(frame, otherId) === partitionOf(frame, parseElementId(member))) ||
        (rules.boolean_completeness && flag !== null && booleanObject(value) === !flag)
      if (exclusive) rivals.push(otherId)
    }
    if (rivals.length > 0) frame.opposing.set(member, rivals)
  }
  return frame
}

function booleanObject(object: Json): boolean | null {
  const value = isJsonMap(object) ? object.value : object
  return typeof value === 'boolean' ? value : null
}

/** One eligible Assertion with its world-time placement. */
interface Row {
  timed: Timed
  candidate: Candidate
  source: AssertionRow
  contexts: string[]
  placement: Placement
}

/** One eligible, inside support of a candidate, as §21.13's rules read it. */
interface Support {
  contexts: string[]
  firstPerson: boolean
  bySubject: boolean
  observed: boolean
  startKey: string
}

/**
 * `kip:memory-default` precedence (§21.13): the first rule under which one
 * candidate prevails over every other candidate of the conflict set decides it.
 */
function precedence(set: number[], supportOf: (i: number) => Support[]): [string, number] | null {
  const supports = new Map(set.map((i) => [i, supportOf(i)]))
  const strictSuperset = (a: string[], b: string[]): boolean =>
    a.length > b.length && b.every((value) => a.includes(value))
  const newest = (i: number): string =>
    supports.get(i)!.map((s) => s.startKey).reduce((a, b) => (a > b ? a : b), '')
  const rules: [string, (a: number, b: number) => boolean][] = [
    ['context_specificity', (a, b) =>
      supports.get(a)!.some((x) => supports.get(b)!.every((y) => strictSuperset(x.contexts, y.contexts)))],
    ['first_person_testimony', (a, b) =>
      supports.get(a)!.some((x) => x.firstPerson) &&
      !supports.get(b)!.some((y) => y.bySubject || y.observed)],
    // Recency compares start keys — when values were claimed to hold, never
    // when they were recorded (§13.2).
    ['recency', (a, b) => newest(a) > newest(b)],
  ]
  for (const [rule, prevails] of rules) {
    const winners = set.filter((a) => set.every((b) => b === a || prevails(a, b)))
    if (winners.length === 1) return [rule, winners[0]!]
  }
  return null
}

/** Projects the belief about one Proposition, together with its slot. */
export function project(
  cx: Context,
  proposition: ElementId,
  policy: Policy,
  validAt: string = cx.validAt,
): Belief {
  checkProjectionHistory(cx, policy)
  const frame = frameOf(cx, proposition)
  const beliefs = projectFrame(cx, frame, policy, validAt)
  return beliefs.find((b) => b.proposition?.seq === proposition.seq) ?? beliefs[0]!
}

/**
 * Projects every candidate of one slot at one basis (§21.11): a single
 * `BELIEF` and a `BELIEF SLOT` at one basis agree.
 */
export function projectSlot(
  cx: Context,
  members: ElementId[],
  policy: Policy,
  validAt: string = cx.validAt,
): Belief[] {
  checkProjectionHistory(cx, policy)
  if (members.length === 0) return []
  const frame = frameOf(cx, members[0]!)
  // An unconstrained slot projects every value on its own.
  if (frame.members.length < members.length) frame.members = members
  return projectFrame(cx, frame, policy, validAt)
}

/**
 * Eligibility, then world time over the eligible set — succession narrows
 * intervals, and each Assertion is inside, outside or indeterminate at the
 * instant (§25.4, §25.5) — then candidate status from what is inside, then the
 * slot stage: a conflict set stands `contested` unless the policy's precedence
 * rules resolve it (§21.11, §21.13).
 */
function projectFrame(cx: Context, frame: Frame, policy: Policy, validAt: string): Belief[] {
  const rows: Row[] = []
  const excluded = new Map<string, { assertion_id: string; reason: string }[]>()
  for (const member of frame.members) {
    const key = formatElementId(member)
    for (const row of assertionsAbout(cx, member)) {
      const admitted = admit(cx, row, policy)
      if (typeof admitted === 'string') {
        const list = excluded.get(key) ?? []
        list.push({ assertion_id: formatElementId({ kind: 'Assertion', seq: row.id }), reason: admitted })
        excluded.set(key, list)
        continue
      }
      // Lines and precedence compare the actor and the context set as they
      // are now, merge-resolved: a context merged after the claim was written
      // is still the same scope (§25.4).
      const contexts = [...new Set(row.context_refs.map((ref) =>
        endpointKey(endpointFromJson(cx.canonicalEndpoint(ref as Json)))))].sort()
      const line = timed(row, key, partitionOf(frame, member))
      line.context = contexts.join('\u001f')
      const actor = referencedElement(row.asserted_by as Json)
      if (actor !== null && actor.kind === 'Concept') {
        line.actor = endpointKey({ kind: 'local', id: cx.canonicalOf(actor) })
      }
      rows.push({
        timed: line,
        candidate: admitted,
        source: row,
        contexts,
        placement: 'outside',
      })
    }
  }
  const timedRows = rows.map((r) => r.timed)
  succeed(timedRows, frame.slotLines)
  let nextInvalid: string | null = null
  for (const row of rows) {
    row.placement = place(row.timed, validAt)
    for (const t of boundariesAfter(row.timed, validAt)) if (nextInvalid === null || t < nextInvalid) nextInvalid = t
  }
  const weigh = (candidate: Candidate): Candidate =>
    policy.structural === true ? { ...candidate, confidence: 1 } : candidate

  const beliefs: Belief[] = []
  for (const member of frame.members) {
    const key = formatElementId(member)
    const ledger: Ledger = {
      supporting: [], opposing: [], uncertain: [], indeterminate: [],
      excluded: excluded.get(key) ?? [], supportGroups: [], oppositionGroups: [],
      warnings: [...MISSING_STAGE_WARNINGS], reasons: [],
    }
    let next = nextInvalid
    let unverified = false
    const candidates: Candidate[] = []
    for (const row of rows.filter((r) => r.timed.proposition === key)) {
      const id = row.candidate.id
      if (row.placement === 'outside') { ledger.excluded.push({ assertion_id: id, reason: 'outside_valid_time' }); continue }
      // Material, but it cannot decide a status (§25.5).
      if (row.placement === 'indeterminate') { ledger.indeterminate.push(id); continue }
      if (row.source.mode === 'inferred' || cx.store.controlAt(cx.space, `identity_review/A-${row.source.id}`, cx.asOf ?? cx.store.currentSeq(cx.space))) {
        const checked = dependencyValidity(cx, { kind: 'Assertion', row: row.source }, policy, validAt)
        if (checked.action_eligible !== true && row.candidate.stance === 'support') unverified = true
        const t = (checked.basis as JsonMap).next_invalid_at
        if (typeof t === 'string' && (next === null || t < next)) next = t
      }
      // An `uncertain` stance engages the question without taking a side: it
      // keeps the belief out of `insufficient` without pushing it either way.
      if (row.candidate.stance === 'uncertain') { ledger.uncertain.push(id); continue }
      ;(row.candidate.stance === 'reject' ? ledger.opposing : ledger.supporting).push(id)
      candidates.push(weigh(row.candidate))
    }
    const [localSupport, localGroups] = aggregate(candidates, false)
    const [localOpposition, localOpposingGroups] = aggregate(candidates, true)
    let candidateStatus = classify(localSupport, localOpposition, ledger, policy)
    // A candidate whose only material at the instant is indeterminate is
    // `uncertain`, and says why (§25.5); beside material that is inside it
    // decides nothing and is only listed.
    if (candidateStatus === 'insufficient' && ledger.indeterminate.length > 0) {
      candidateStatus = 'uncertain'
      ledger.reasons.push('temporal_indeterminate')
    }
    ledger.supportGroups = localGroups
    ledger.oppositionGroups = localOpposingGroups

    // Exclusive values (§25, `complete`, boolean negation): a rival's support
    // opposes this value.
    const conflictRefs: string[] = []
    if (policy.expand_conflicts) {
      for (const rival of frame.opposing.get(key) ?? []) {
        const rivalKey = formatElementId(rival)
        const rivalCandidates = rows
          .filter((r) => r.timed.proposition === rivalKey && r.placement === 'inside' && r.candidate.stance === 'support')
          .map((r) => weigh(r.candidate))
        const [rivalSupport] = aggregate(rivalCandidates, false)
        if (localSupport >= policy.material && rivalSupport >= policy.material) conflictRefs.push(rivalKey)
        for (const candidate of rivalCandidates) {
          ledger.opposing.push(candidate.id)
          candidates.push({ ...candidate, opposesTarget: true })
        }
      }
    }
    const [support, supportGroups] = aggregate(candidates, false)
    const [opposition, oppositionGroups] = aggregate(candidates, true)
    ledger.supportGroups = supportGroups
    ledger.oppositionGroups = oppositionGroups
    if (!frame.rules.open_world) {
      ledger.warnings.push(
        'the Predicate is declared closed-world (§24.2): an absence of ' +
          'Propositions may be read as closed-world, but this projection still ' +
          'reports insufficient rather than inferring rejection from silence',
      )
    }
    const engaged = ledger.supporting.length + ledger.opposing.length + ledger.uncertain.length > 0
    let status = conflictRefs.length ? 'contested' : engaged ? classify(support, opposition, ledger, policy) : candidateStatus
    if (unverified && status === 'accepted') {
      status = 'uncertain'
      ledger.warnings.push('inferred support has no verified recursive dependency basis')
    }
    beliefs.push({
      proposition: member,
      status,
      basis: projectionBasis(cx, policy, validAt, next),
      candidateStatus,
      conflictRefs,
      conflictReasons: conflictRefs.length ? ['exclusive_value'] : [],
      precedence: null,
      slotLeading: null,
      unverified,
      support,
      opposition,
      ledger,
      policy,
      validAt,
      asOf: cx.asOf ?? null,
    })
  }

  // The slot stage (§21.11): materially supported values of one functional
  // slot — per partition under `functional_by` — conflict.
  if (frame.conflict) {
    const partitions = [...new Set(frame.members.map((m) => partitionOf(frame, m)))].sort()
    for (const partition of partitions) {
      const supported = beliefs
        .map((b, i) => [b, i] as const)
        .filter(([b, i]) => partitionOf(frame, frame.members[i]!) === partition &&
          b.support >= policy.material && !b.unverified)
        .map(([, i]) => i)
      if (supported.length < 2) continue
      const supportRows = (i: number): Support[] => {
        const key = formatElementId(frame.members[i]!)
        return rows
          .filter((r) => r.timed.proposition === key && r.placement === 'inside' && r.candidate.stance === 'support')
          .map((r) => ({
            contexts: r.contexts,
            firstPerson: frame.subjectKeys.includes(r.timed.actor) && (r.source.mode === 'stated' || r.source.mode === 'observed'),
            bySubject: frame.subjectKeys.includes(r.timed.actor),
            observed: r.source.mode === 'observed',
            startKey: r.timed.startKey,
          }))
      }
      const winner = policy.precedence === true ? precedence(supported, supportRows) : null
      const ids = supported.map((i) => formatElementId(frame.members[i]!))
      // Leading compares eligible independent roots (§27.2), never the numeric
      // support a structural policy does not have.
      const roots = beliefs.map((b) => b.ledger.supportGroups.length)
      for (const i of supported) {
        const me = formatElementId(frame.members[i]!)
        const belief = beliefs[i]!
        if (winner !== null && winner[1] === i) {
          belief.precedence = { rule: winner[0], prevailed_over: ids.filter((id) => id !== me) }
        } else if (winner !== null) {
          belief.status = 'uncertain'
          belief.ledger.reasons.push('outranked')
          belief.precedence = { rule: winner[0], outranked_by: formatElementId(frame.members[winner[1]]!) }
        } else {
          // Leading over the final conflict set: a tie between the values is
          // `none` (§21.11).
          const best = Math.max(...supported.filter((j) => j !== i).map((j) => roots[j]!))
          const mine = roots[i]!
          belief.slotLeading = mine > best ? 'support' : mine < best ? 'opposition' : 'none'
          belief.status = 'contested'
          belief.conflictRefs = ids.filter((id) => id !== me)
          belief.conflictReasons = [frame.reason]
        }
      }
    }
  }
  return beliefs
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
      indeterminate: [],
      excluded: [],
      supportGroups: [],
      oppositionGroups: [],
      warnings: [
        'no Proposition exists for this tuple in this Space, so nothing has ' +
          'been asserted about it; that is an open-world absence, not a denial',
      ],
      reasons: [],
    },
    precedence: null,
    slotLeading: null,
    unverified: false,
    policy,
    validAt,
    asOf: cx.asOf ?? null,
  }
}

/**
 * Lifecycle, visibility, context, Evidence and mode eligibility: the
 * Candidate, or the reason it was left out. World time is decided over the
 * eligible set afterwards, because succession needs every eligible Assertion
 * of the slot (§25.4).
 */
function admit(cx: Context, row: AssertionRow, policy: Policy): Candidate | string {
  const id = formatElementId({ kind: 'Assertion', seq: row.id })
  // A retracted claim was withdrawn and a superseded one was revised: both are
  // history, and history is not what this Brain currently holds (§59).
  // `expired` is computed from world time and never stored (§14.3); a row an
  // earlier draft stored it on is read as active.
  if (row.status !== 'active' && row.status !== 'expired') return `lifecycle_${row.status}`
  if (row.state !== State.ACTIVE) return `record_${row.state}`
  for (const reference of row.context_refs) {
    const canonical = cx.canonicalEndpoint(reference as Json) as JsonMap
    if (!policy.context_refs.includes(String(canonical.id))) return 'context_mismatch'
  }
  for (const reference of row.evidence_refs) {
    const eid = tryParseElementId(reference.id)
    const root = eid === null ? null : cx.load(eid)
    if (!root || root.kind !== 'Evidence') return 'evidence_unavailable'
    if (root.row.status === 'corrected') return 'corrected_evidence'
  }
  if (!admits(policy, row.mode)) return modeExclusion(row.mode)

  const actor =
    typeof row.asserted_by === 'string'
      ? row.asserted_by
      : (((row.asserted_by as JsonMap)?.id as string) ?? '')
  let trust = policy.trust_weights[actor] ?? policy.default_trust_weight
  if (policy.contextual_trust_rules?.length) {
    const proposition = cx.load(parseElementId(row.proposition_id))
    if (!proposition || proposition.kind !== 'Proposition') return 'proposition_unavailable'
    trust = trustWeight(
      policy.contextual_trust_rules,
      trust,
      actor,
      proposition.row.predicate_ref,
      policy.context_refs,
    )
  }
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
    confidence:
      (row.confidence < 0 ? policy.unstated_confidence : row.confidence) *
      trust,
    opposesTarget: false,
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
  if (belief.slotLeading !== null) return belief.slotLeading
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
    ...(belief.precedence === null ? {} : { precedence: belief.precedence }),
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
              indeterminate_assertions: belief.ledger.indeterminate,
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
  // A structural policy weighs nothing and outputs no number (§21.10).
  const weighted = belief.policy.structural !== true
  return {
    score: weighted ? score : null,
    // Said out loud, because a number between 0 and 1 looks like a probability
    // and this one is not calibrated as one.
    score_semantics: weighted ? 'normalized_support_not_probability' : null,
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

/**
 * Why the answer is as uncertain as it is (§27.2): the machine codes a caller
 * can act on — `temporal_indeterminate` (§25.5) and `outranked` (§21.13). The
 * prose behind them is the Epistemic Ledger.
 */
function uncertaintyReasons(belief: Belief): string[] {
  return [...belief.ledger.reasons]
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
 * The slot's `basis` carries the policy, `valid_at` and snapshot it *ran* under
 * (§47.3), so an empty slot still says what it was computed against. Which
 * side leads is each candidate projection's own `leading`.
 */
export function slotToJson(
  subject: Json,
  predicateRef: string,
  slot: Slot,
): JsonMap {
  const beliefs = slot.candidates
  const accepted = beliefs.filter((belief) => belief.status === 'accepted')
  const engaged = beliefs.filter((belief) => belief.status !== 'insufficient')
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
    uncertainty: {
      level:
        status === 'insufficient'
          ? 'total'
          : status === 'contested' || status === 'uncertain'
            ? 'high'
            : 'low',
      reasons: [...new Set(beliefs.flatMap(uncertaintyReasons))].sort(),
    },
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
  /** The coordinates it ran under, reported whether or not any candidate exists. */
  basis: JsonMap
  candidates: Belief[]
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
          cx.env.lineage('PredicateType', row.predicate_lineage === '' ? lineageText(row.predicate_ref) : row.predicate_lineage) ===
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
         AND predicate_lineage IN (SELECT value FROM json_each(?))
       ORDER BY id`,
    cx.space,
    State.ACTIVE,
    JSON.stringify(subjectKeys),
    // A draft symbol promoted into the lineage is read as it (§20.16).
    JSON.stringify(cx.env.lineagesOf('PredicateType', predicateLineage)),
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
