import type { Session } from '../nexus.js'
import type { Json, JsonMap } from '../json.js'
import type { ControlRecord } from '../control.js'
import type { Transaction } from '../tx.js'
import type {
  PreparedWatchPage,
  WatchCandidate,
  WatchCheckpoint,
  WatchEvaluation,
  Ticket,
  Payload,
} from './types.js'
import { authorizedArtifact } from '../control.js'
import { errors } from '../errors.js'
import { digest } from '../schema/contracts.js'
import { parseElementId } from '../id.js'
import { render } from '../view.js'
import {
  CONFIG,
  bytes,
  bounded,
  json,
  eq,
  invalid,
  conflict,
  fullRead,
  permit,
  runtimeRef,
  stageControl,
  transaction,
  commit,
  exact,
} from './common.js'
import { advanceWatch, matchChange, checkControls } from './watch.js'

export const pageDigest = (page: JsonMap): string =>
  digest({
    changes: page.changes!,
    coverage: page.coverage!,
    resync_required: page.resync_required ?? null,
  })
export function candidates(
  s: Session,
  space: string,
  condition: Json,
  page: JsonMap,
  silence: boolean,
  dueAt: string | null,
  semantic: boolean,
): WatchCandidate[] {
  const result: WatchCandidate[] = []
  for (const envelope of page.changes as JsonMap[]) {
    checkControls(envelope)
    if (silence && dueAt && String(envelope.committed_at) > dueAt) continue
    if (!semantic) continue
    for (const change of envelope.changes as JsonMap[]) {
      if (!matchChange(s, space, condition, envelope, change)) continue
      const seq = Number(envelope.space_seq),
        id = parseElementId(String(change.id)),
        after = s.nexus.store.elementAt(space, id, seq)
      if (!after || after.row.version !== change.new_version)
        conflict('history_gap')
      const before =
        change.old_version === undefined
          ? null
          : s.nexus.store.elementAt(space, id, seq - 1)
      if (
        change.old_version !== undefined &&
        (!before || before.row.version !== change.old_version)
      )
        conflict('history_gap')
      fullRead(s, space, String(change.id), false)
      for (const e of [after, before]) {
        if (!e) continue
        const v = s.effectiveAuthority(space).mayRead(e, s.auth)
        if (e.row.space !== space || !v?.content || v.constraints.fields.length)
          throw errors.notFoundOrNotVisible(
            'semantic history is not fully visible',
          )
      }
      if (result.length >= 512)
        throw errors.resourceExhausted(
          'semantic page exceeds 512 candidates; reduce the page budget',
        )
      const old = before ? render(before) : null,
        current = render(after)
      result.push({
        id: digest({ seq, change, before: old, after: current }),
        envelope_seq: seq,
        change,
        before: old,
        after: current,
      })
    }
  }
  return result
}
function material(
  tx: Transaction,
  content: Json,
  sources: string[],
): { artifact_ref: string; content_digest: string } {
  if (bytes(JSON.stringify(content)) > 524288)
    throw errors.resourceExhausted('semantic material exceeds 512 KiB')
  const hash = digest(content),
    pin = { artifact_ref: `kip:artifact:${hash}`, content_digest: hash }
  stageControl(tx, `artifact/${pin.artifact_ref}`, 0, 'artifact', {
    state: 'available',
    content,
    content_digest: hash,
    source_refs: [...new Set(sources)].sort(),
  })
  return pin
}
export function prepare(
  s: Session,
  space: string,
  ref: string,
  expected: number,
  generation: number,
  limit: number,
  preparationKey: string,
  condition: Json,
  page: JsonMap,
  cp: WatchCheckpoint,
  saved: ControlRecord | null,
  sourceAt: string,
  target: number,
  deadline: boolean,
  semantic: boolean,
  key: string,
  hash: string,
): JsonMap {
  permit(s, space, 'derive')
  const found = candidates(
    s,
    space,
    condition,
    page,
    cp.watch_class === 'silence',
    cp.due_at,
    semantic,
  )
  const ticketRef = runtimeRef('watch-page', {
    scope: cp.config.scope,
    principal: s.auth.principal_id,
    watch_ref: ref,
    preparation_key: preparationKey,
  })
  const coverage = page.coverage as JsonMap
  const prepared: PreparedWatchPage = {
    ticket_ref: ticketRef,
    watch_ref: ref,
    arm_generation: generation,
    expected_version: expected,
    source_snapshot_seq: s.nexus.store.currentSeq(space),
    through_seq: Number(coverage.through_seq),
    deadline_covered: deadline && coverage.complete === true,
    page_digest: pageDigest(page),
    evaluator: semantic ? cp.config.pins.evaluator : null,
    condition,
    candidates: found,
  }
  const payload: Payload = {
    prepared,
    source_at: sourceAt,
    target,
    due_seq: cp.due_seq,
    deadline,
    checkpoint_digest: saved ? digest(saved.value) : null,
    basis: cp.basis,
  }
  const sources = [ref, ...found.map((c) => String(c.change.id))]
  for (const r of sources) fullRead(s, space, r, false)
  const tx = transaction(s, space)
  if (!s.nexus.store.controlAt(space, CONFIG))
    stageControl(tx, CONFIG, 0, 'runtime', cp.config)
  const pin = material(tx, json(payload), sources)
  const ticket: Ticket = {
    format: 'nexus:watch-page-v1',
    principal: s.auth.principal_id,
    watch_ref: ref,
    expected,
    generation,
    limit,
    material: pin,
  }
  stageControl(tx, ticketRef, 0, 'runtime', ticket)
  // Only references enter the journal; erasure can revoke all content copies.
  return commit(s, tx, key, hash, { ticket_ref: ticketRef })
}
export function validateEvaluation(
  payload: Payload,
  e: WatchEvaluation,
): Map<number, boolean> {
  exact(e, ['evaluation_key', 'evaluator', 'judgments'])
  if (
    !bounded(e.evaluation_key) ||
    !eq(e.evaluator, payload.prepared.evaluator) ||
    !Array.isArray(e.judgments) ||
    e.judgments.length !== payload.prepared.candidates.length
  )
    invalid('evaluation does not cover the pinned page/evaluator')
  const expected = new Map(
      payload.prepared.candidates.map((c) => [c.id, c.envelope_seq]),
    ),
    seen = new Set<string>(),
    matches = new Map<number, boolean>()
  for (const j of e.judgments) {
    exact(j, ['candidate_id', 'result', 'rationale'])
    if (
      typeof j.rationale !== 'string' ||
      !j.rationale.trim() ||
      bytes(j.rationale) > 4096 ||
      !['match', 'no_match', 'unknown'].includes(j.result)
    )
      invalid(
        'each semantic judgment needs a bounded rationale and valid result',
      )
    const seq = expected.get(j.candidate_id)
    if (seq === undefined) invalid('unknown semantic candidate')
    if (seen.has(j.candidate_id)) invalid('duplicate semantic candidate')
    seen.add(j.candidate_id)
    matches.set(seq, (matches.get(seq) ?? false) || j.result === 'match')
  }
  return matches
}
export function recordEvaluation(
  tx: Transaction,
  ticketRef: string,
  evaluation: WatchEvaluation,
  payload: Payload,
  accepted: boolean,
): string {
  const ref = runtimeRef('watch-evaluation', {
    ticket_ref: ticketRef,
    principal: tx.auth.principal_id,
    evaluation_key: evaluation.evaluation_key,
  })
  const pin = material(tx, json({ ticket_ref: ticketRef, evaluation }), [
    payload.prepared.watch_ref,
    ...payload.prepared.candidates.map((c) => String(c.change.id)),
  ])
  stageControl(tx, ref, 0, 'runtime', {
    format: 'nexus:watch-evaluation-v1',
    ticket_ref: ticketRef,
    material: pin,
    accepted,
  })
  return ref
}
function loadTicket(
  s: Session,
  space: string,
  ref: string,
): { ticket: Ticket; payload: Payload } {
  if (!ref.startsWith('watch-page/v1/') || bytes(ref) > 256)
    invalid('invalid watch ticket')
  const row = s.nexus.store.controlAt(space, ref)
  if (!row) throw errors.notFoundOrNotVisible('watch ticket unavailable')
  const ticket = row.value as unknown as Ticket
  if (
    ticket.format !== 'nexus:watch-page-v1' ||
    ticket.principal !== s.auth.principal_id
  )
    throw errors.notAuthorized(
      'watch ticket belongs to another authorization view',
    )
  const { payload: value } = authorizedArtifact(
    s.nexus.store,
    space,
    ticket.material.artifact_ref,
    s.effectiveAuthority(space),
    s.auth,
  )
  if (digest(value) !== ticket.material.content_digest)
    invalid('prepared material digest mismatch')
  const payload = value as unknown as Payload
  if (payload.prepared.ticket_ref !== ref) conflict('basis_changed')
  return { ticket, payload }
}
export function readPreparedWatchPage(
  s: Session,
  space: string,
  ref: string,
): PreparedWatchPage {
  return loadTicket(s, space, ref).payload.prepared
}
export function prepareWatchPage(
  s: Session,
  space: string,
  ref: string,
  expected: number,
  generation: number,
  limit: number,
  key: string,
): JsonMap {
  if (!bounded(key) || !Number.isInteger(limit) || limit < 1 || limit > 200)
    invalid('invalid semantic page key/budget')
  const result = advanceWatch(
    s,
    space,
    ref,
    expected,
    generation,
    limit,
    undefined,
    { kind: 'prepare', key },
  )
  return {
    ...result,
    prepared: json(readPreparedWatchPage(s, space, String(result.ticket_ref))),
  }
}
export function commitWatchPage(
  s: Session,
  space: string,
  ref: string,
  evaluation: WatchEvaluation,
): JsonMap {
  const { ticket, payload } = loadTicket(s, space, ref)
  validateEvaluation(payload, evaluation)
  return advanceWatch(
    s,
    space,
    ticket.watch_ref,
    ticket.expected,
    ticket.generation,
    ticket.limit,
    undefined,
    { kind: 'evaluate', ticketRef: ref, ticket, payload, evaluation },
  )
}
