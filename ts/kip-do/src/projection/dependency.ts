/** Cognitive Consistency §3: computed validity, never an author-written flag. */
import type { Context } from '../kql/context.js'
import { isInvalidated } from '../recording.js'
import { tryParseElementId, formatElementId } from '../id.js'
import { canonicalJson, isJsonMap, type JsonMap } from '../json.js'
import { State, type ActivityRow, type Element } from '../store/index.js'
import { validateValue, pinnedPlane } from '../schema/contracts.js'
import { projectionBasis } from './index.js'
import type { Policy } from './policy.js'

export function isDerived(element: Element): boolean {
  if (element.kind === 'Assertion') return element.row.mode === 'inferred'
  if (element.kind !== 'Concept') return false
  return ['SkillRevision', 'Insight', 'WorkingState'].some((name) => element.row.schema_ref.endsWith('/' + name)) ||
    Object.keys(element.row.structural).some((name) => name.endsWith('/derived_from'))
}
interface Validity { state: number; reasons: string[]; next: string | null }
const issue = (state: number, reason: string): Validity => ({ state, reasons: [reason], next: null })
const absorb = (into: Validity, other: Validity): void => {
  into.state = Math.max(into.state, other.state)
  into.reasons.push(...other.reasons)
  into.next = [into.next, other.next].filter((v): v is string => v !== null).sort()[0] ?? null
}

export function dependencyValidity(cx: Context, element: Element, policy: Policy, at: string): JsonMap {
  if (!policy.trust_version || policy.trust_version === 'unavailable') return {status:'unverifiable',action_eligible:false,reasons:['historical projection control state unavailable'],basis:projectionBasis(cx,policy,at)}
  function check(element: Element, path: string[]): Validity {
    const id = formatElementId({ kind: element.kind, seq: element.row.id })
    if (path.includes(id) || path.length >= 64) return issue(2, 'dependency cycle or traversal limit')
    const visibility = cx.authority.mayRead(element, cx.auth)
    if (!visibility?.content || visibility.constraints.fields.length) return issue(2, 'dependency source unavailable')
    if (element.row.state !== State.ACTIVE && element.row.state !== State.MERGED) return issue(1, 'dependency lifecycle changed')
    if (isInvalidated(element)) return issue(1, 'dependency extraction repaired')
    const result: Validity = { state: 0, reasons: [], next: null }
    if (element.kind === 'Evidence' && element.row.status === 'corrected') return issue(1, 'dependency evidence corrected')
    if (element.kind === 'Assertion') {
      const row = element.row
      if (row.status !== 'active' || (row.valid_from && row.valid_from > at) || (row.valid_until && row.valid_until <= at)) return issue(1, 'dependency no longer eligible')
      result.next = [row.valid_from, row.valid_until].filter((t) => t > at).sort()[0] ?? null
    }
    if (cx.store.controlAt(cx.space, `identity_review/${id}`, cx.asOf ?? cx.store.currentSeq(cx.space))) return issue(1, 'identity interpretation requires review')
    if (!isDerived(element)) return result
    const activities = cx.asOf !== null
      ? cx.reconstruct('Activity').map((e) => e.row as ActivityRow)
      : cx.store.all<ActivityRow>('activities', 'SELECT * FROM activities WHERE space = ? ORDER BY id', cx.space)
    cx.spend('scans', activities.length)
    let producer: { seq: number; contract: JsonMap } | null = null
    for (const activity of activities) {
      const readable = cx.load({ kind: 'Activity', seq: activity.id }, false)
      if (!readable || !cx.authority.mayRead(readable, cx.auth)?.content) continue
      if (activity.activity_class !== 'dependency_validation' && activity.created_tx !== element.row.created_tx && activity.updated_tx !== element.row.updated_tx) continue
      if (activity.status !== 'completed' || activity.state !== State.ACTIVE) continue
      const runtime = activity.origin._kip_runtime as JsonMap | undefined
      if (!runtime || (runtime.output_versions as JsonMap)?.[id] !== element.row.version) continue
      if (!activity.outputs.some((ref) => (typeof ref === 'string' ? ref : (ref as JsonMap).id) === id)) continue
      const contract = Object.entries(activity.facets).find(([name]) => name.endsWith('/DependencyBasis'))?.[1]
      if (isJsonMap(contract) && (!producer || activity.seq > producer.seq)) producer = { seq: activity.seq, contract }
    }
    if (!producer) return issue(2, 'exact producing DependencyBasis unavailable')
    const { contract } = producer
    try { validateValue({ $ref: 'urn:kip:2.0:schema:cognitive-records#/$defs/DependencyBasis' }, contract) }
    catch { return issue(2, 'dependency contract is invalid') }
    const basis = projectionBasis(cx, policy, at)
    const old = contract.policy_basis as JsonMap
    for (const coordinate of ['schema_environment_version', 'identity_version', 'policy', 'trust_version', 'purpose', 'risk']) {
      if (canonicalJson(old[coordinate]) !== canonicalJson(basis[coordinate])) absorb(result, issue(1, 'dependency computation policy changed'))
    }
    if (!(old.context_refs as string[]).every((r) => (basis.context_refs as string[]).includes(r))) absorb(result, issue(1, 'dependency context mismatch'))
    for (const group of contract.groups as JsonMap[]) {
      const members = (group.pins as JsonMap[]).map((pin): Validity => {
        const source = tryParseElementId(String(pin.id))
        const row = source && cx.load(source, false)
        if (!row || !cx.authority.mayRead(row, cx.auth)?.content) return issue(2, 'dependency source unavailable')
        const planes = isJsonMap(pin.planes) && Object.keys(pin.planes).length ? pin.planes : null
        const changed = planes ? Object.entries(planes).some(([name, version]) => pinnedPlane(row.row.plane_versions as unknown as JsonMap, name) !== version) : row.row.version !== pin.version
        return changed ? issue(1, 'dependency source version changed') : check(row, [...path, id])
      })
      if (group.role === 'context') {
        if (members.some((m) => m.state)) result.reasons.push('context dependency changed')
      } else if (group.role === 'any_of' && members.some((m) => m.state === 0)) {
        members.filter((m) => m.state === 0).forEach((m) => absorb(result, m))
      } else members.forEach((m) => absorb(result, m))
    }
    return result
  }
  const checked = check(element, [])
  return { status: ['current', 'needs_review', 'unverifiable'][checked.state]!, action_eligible: checked.state === 0, reasons: [...new Set(checked.reasons)].sort(), basis: projectionBasis(cx, policy, at, checked.next) }
}
