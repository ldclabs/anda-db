import type { Session } from '../nexus.js'
import type { Json, JsonMap } from '../json.js'
import type { Element } from '../store/rows.js'
import { errors } from '../errors.js'
import { digest } from '../schema/contracts.js'
import { normalizeTime, nowTime } from '../time.js'
import {
  bindWatchCondition,
  structuredCondition,
  changePage,
} from '../runtime.js'
import { parseElementId } from '../id.js'
import { parseKip } from '../kip/parser.js'
import { planKml } from '../kml/index.js'
import { render } from '../view.js'
import { authorizedArtifact } from '../control.js'
import {
  PROFILE,
  FORMAT,
  CONFIG,
  obj,
  json,
  eq,
  safe,
  invalid,
  conflict,
  next,
  basis,
  context,
  fullRead,
  permit,
  transaction,
  configuration,
  validateConfig,
  stageControl,
  checkpointKey,
  requestKey,
  runtimeRef,
  replay,
  commit,
  fireKey,
} from './common.js'
import type { Mode, WatchCheckpoint, WatchFire, WakeRecord } from './types.js'
import {
  candidates,
  prepare,
  pageDigest,
  validateEvaluation,
  recordEvaluation,
} from './evaluation.js'

function values(e: Element): {
  row: Extract<Element, { kind: 'Concept' }>['row']
  watch: JsonMap
} {
  if (
    e.kind !== 'Concept' ||
    e.row.schema_ref !== PROFILE + 'Watch' ||
    e.row.state !== 'active'
  )
    invalid('target must be an active CognitiveMemory 2.1 Watch')
  return {
    row: e.row,
    watch: structuredClone(obj(e.row.facets[PROFILE + 'WatchState'])),
  }
}
function due(e: Element): string | null {
  const v = values(e).row.attributes.due_at
  return v === undefined || v === null
    ? null
    : normalizeTime(v, 'Watch deadline')
}
function authorize(s: Session, space: string, ref: string): Element {
  const e = fullRead(s, space, ref)
  values(e)
  permit(s, space, 'update', e)
  return e
}
export function armWatch(
  s: Session,
  space: string,
  ref: string,
  expected: number,
  replacement: [Json, string | null] | null = null,
): JsonMap {
  return s.nexus.transact(() => {
    const e = authorize(s, space, ref),
      { row, watch: old } = values(e)
    const request = {
        operation: 'arm_watch',
        watch_ref: ref,
        expected,
        replacement,
      },
      key = requestKey(s, request),
      hash = digest(json(request))
    const previous = replay(s, space, key, hash)
    if (previous) return previous
    if (e.row.version !== expected) conflict('version_conflict')
    const cx = context(s, space),
      condition = bindWatchCondition(
        cx,
        replacement ? replacement[0] : row.attributes.condition!,
      ),
      deadline = replacement
        ? replacement[1] === null
          ? null
          : normalizeTime(replacement[1], 'Watch deadline')
        : due(e)
    if (row.attributes.watch_class === 'silence' && deadline === null)
      invalid('silence Watch requires a deadline')
    const pinned = basis(cx),
      generation = next(Number(old.arm_generation ?? 0)),
      tx = transaction(s, space),
      cfg = configuration(s, space)
    if (!s.nexus.store.controlAt(space, CONFIG))
      stageControl(tx, CONFIG, 0, 'runtime', cfg)
    const id = parseElementId(ref)
    tx.expectVersions(id, [{ version: expected, plane: null }])
    const watch = {
      arm_generation: generation,
      armed_seq: tx.snapshotSeq,
      condition_digest: digest(condition),
      authorization_view: pinned.authorization!,
      consumed_seq: tx.snapshotSeq,
      matched: false,
    }
    const checkpoint: WatchCheckpoint = {
      format: 'nexus:watch-checkpoint-v1',
      watch_ref: ref,
      arm_generation: generation,
      watch_class: String(row.attributes.watch_class),
      due_at: deadline,
      condition_digest: digest(condition),
      basis: pinned,
      config: cfg,
      due_seq: null,
      matched_seq: null,
    }
    const target = values(tx.load(id)).row
    Object.assign(target.attributes, {
      condition,
      due_at: deadline,
      status: 'armed',
    })
    target.facets[PROFILE + 'WatchState'] = watch
    tx.authorizedWatchUpdates.add(ref)
    tx.markChanged(id, 'update')
    stageControl(tx, checkpointKey(ref, generation), 0, 'runtime', checkpoint)
    return commit(s, tx, key, hash, { watch, status: 'armed' })
  })
}

export function matchChange(
  s: Session,
  space: string,
  condition: Json,
  envelope: JsonMap,
  change: JsonMap,
): boolean {
  const c = obj(condition)
  if (c.element !== undefined && c.element !== change.id) return false
  if (Array.isArray(c.ops) && !c.ops.includes(change.op!)) return false
  if (
    Array.isArray(c.touched) &&
    (!Array.isArray(change.touched) ||
      !change.touched.some((p) => (c.touched as Json[]).includes(p)))
  )
    return false
  if (c.type !== undefined || c.slot !== undefined) {
    const retained = s.nexus.store.elementAt(
      space,
      parseElementId(String(change.id)),
      Number(envelope.space_seq),
    )
    if (!retained) conflict('history_gap')
    let value = render(retained)
    if (c.type !== undefined && c.type !== value.schema_ref) return false
    if (c.slot !== undefined) {
      if (retained.kind === 'Assertion') {
        const p = s.nexus.store.elementAt(
          space,
          parseElementId(retained.row.proposition_id),
          Number(envelope.space_seq),
        )
        if (!p) conflict('history_gap')
        value = render(p)
      }
      if (
        (typeof value.subject === 'string'
          ? value.subject
          : obj(value.subject).id) !== obj(c.slot).subject ||
        value.predicate_ref !== obj(c.slot).predicate
      )
        return false
    }
  }
  return true
}

export function advanceWatch(
  s: Session,
  space: string,
  ref: string,
  expected: number,
  generation: number,
  limit: number,
  evaluate?: (condition: Json, change: Json) => boolean,
  mode: Mode = { kind: 'immediate' },
): JsonMap {
  return s.nexus.transact(() => {
    const store = s.nexus.store,
      e = authorize(s, space, ref)
    permit(s, space, 'read_history')
    const immediate = {
      operation: 'advance_watch',
      watch_ref: ref,
      expected,
      generation,
      limit,
    }
    const identity =
      mode.kind === 'prepare'
        ? { operation: 'prepare_watch_page', watch_ref: ref, key: mode.key }
        : mode.kind === 'evaluate'
          ? {
              operation: 'commit_watch_page',
              ticket_ref: mode.ticketRef,
              evaluation_key: mode.evaluation.evaluation_key,
            }
          : immediate
    const request =
      mode.kind === 'prepare'
        ? {
            watch_ref: ref,
            expected,
            generation,
            limit,
            preparation_key: mode.key,
          }
        : mode.kind === 'evaluate'
          ? mode.evaluation
          : immediate
    const key = requestKey(s, identity),
      hash = digest(json(request)),
      previous = replay(s, space, key, hash)
    if (previous) {
      if (typeof previous.fire_activity_ref === 'string')
        fullRead(s, space, previous.fire_activity_ref)
      if (typeof previous.evaluation_ref === 'string') {
        const saved = obj(
          store.controlAt(space, previous.evaluation_ref)?.value,
        )
        authorizedArtifact(
          store,
          space,
          String(obj(saved.material).artifact_ref),
          s.effectiveAuthority(space),
          s.auth,
        )
      }
      return previous
    }
    const { row, watch } = values(e)
    if (
      !safe(expected) ||
      !safe(generation) ||
      e.row.version !== expected ||
      watch.arm_generation !== generation ||
      row.attributes.status !== 'armed'
    )
      conflict('generation_conflict')
    if (!Number.isInteger(limit) || limit < 1 || limit > 10000)
      invalid('invalid change page limit')
    const condition = row.attributes.condition!,
      structured = structuredCondition(condition),
      semantic = !structured || obj(condition).text !== undefined
    const cx = context(s, space),
      pinned = basis(cx)
    if (watch.authorization_view !== pinned.authorization)
      conflict('basis_changed')
    const saved = store.controlAt(space, checkpointKey(ref, generation))
    let cp: WatchCheckpoint
    if (saved) cp = structuredClone(saved.value) as unknown as WatchCheckpoint
    else {
      const armed = Number(watch.armed_seq)
      if (!safe(armed)) invalid('missing armed sequence')
      const original = store.elementAt(space, parseElementId(ref), armed + 1)
      if (!original) conflict('history_gap')
      const old = values(original)
      if (
        old.watch.arm_generation !== generation ||
        !eq(old.row.attributes.condition, condition) ||
        old.row.attributes.watch_class !== row.attributes.watch_class ||
        due(original) !== due(e)
      )
        conflict('history_gap')
      cp = {
        format: 'nexus:watch-checkpoint-v1',
        watch_ref: ref,
        arm_generation: generation,
        watch_class: String(row.attributes.watch_class),
        due_at: due(e),
        condition_digest: digest(condition),
        basis: pinned,
        config: configuration(s, space),
        due_seq: null,
        matched_seq: null,
      }
    }
    if (
      cp.format !== 'nexus:watch-checkpoint-v1' ||
      cp.watch_ref !== ref ||
      cp.arm_generation !== generation
    )
      invalid('unsupported or mismatched Watch checkpoint')
    validateConfig(cp.config)
    if (
      !eq(cp.basis, pinned) ||
      cp.condition_digest !== digest(condition) ||
      cp.due_at !== due(e) ||
      cp.watch_class !== row.attributes.watch_class
    )
      conflict('basis_changed')
    const cfg = store.controlAt(space, CONFIG)
    if (cfg && !eq(cfg.value, cp.config)) conflict('basis_changed')
    if (semantic && !cp.config.pins.evaluator)
      throw errors.unsupportedCapability(
        'text and mixed Watch conditions need a pinned host evaluator',
      )
    const silence = cp.watch_class === 'silence',
      sourceAt = mode.kind === 'evaluate' ? mode.payload.source_at : nowTime(),
      deadline = cp.due_at !== null && cp.due_at <= sourceAt
    if (mode.kind === 'evaluate') {
      if (
        mode.ticket.principal !== s.auth.principal_id ||
        !eq(mode.payload.basis, pinned) ||
        mode.payload.checkpoint_digest !== (saved ? digest(saved.value) : null)
      )
        conflict('basis_changed')
      cp.due_seq = mode.payload.due_seq
    }
    if (silence && deadline && cp.due_seq === null)
      cp.due_seq = store.seqAtTime(space, cp.due_at!)
    const after = Number(watch.consumed_seq),
      target =
        mode.kind === 'evaluate'
          ? mode.payload.target
          : silence && deadline
            ? Math.max(cp.due_seq!, Number(watch.armed_seq))
            : store.currentSeq(space)
    const page = changePage(s, after, limit, space, Math.max(after, target)),
      coverage = obj(page.coverage)
    if (page.resync_required === true) conflict('history_gap')
    if (watch.authorization_view !== coverage.authorization_view)
      conflict('basis_changed')
    if (mode.kind === 'prepare')
      return prepare(
        s,
        space,
        ref,
        expected,
        generation,
        limit,
        mode.key,
        condition,
        page,
        cp,
        saved,
        sourceAt,
        target,
        deadline,
        semantic,
        key,
        hash,
      )
    let evaluated: Map<number, boolean> | null = null
    if (mode.kind === 'evaluate') {
      if (
        mode.payload.prepared.page_digest !== pageDigest(page) ||
        mode.payload.deadline !== deadline
      )
        conflict('prepared_page_changed')
      const actual = candidates(
        s,
        space,
        condition,
        page,
        silence,
        cp.due_at,
        semantic,
      )
      if (
        !eq(
          actual.map((c) => c.id),
          mode.payload.prepared.candidates.map((c) => c.id),
        )
      )
        conflict('prepared_candidates_changed')
      evaluated = validateEvaluation(mode.payload, mode.evaluation)
      if (mode.evaluation.judgments.some((j) => j.result === 'unknown')) {
        const tx = transaction(s, space),
          evaluationRef = recordEvaluation(
            tx,
            mode.ticketRef,
            mode.evaluation,
            mode.payload,
            false,
          )
        return commit(s, tx, key, hash, {
          status: 'deferred',
          reason: 'semantic_unknown',
          ticket_ref: mode.ticketRef,
          evaluation_ref: evaluationRef,
        })
      }
    }
    let matched = watch.matched === true
    for (const envelope of page.changes as JsonMap[]) {
      checkControls(envelope)
      if (silence && cp.due_at && String(envelope.committed_at) > cp.due_at)
        continue
      const changes = envelope.changes as JsonMap[]
      const hit = structured
        ? changes.some((c) => matchChange(s, space, condition, envelope, c))
        : changes.length > 0
      let semanticHit = false
      if (evaluated)
        semanticHit = evaluated.get(Number(envelope.space_seq)) ?? false
      else if (hit && semantic) {
        if (!evaluate)
          throw errors.unsupportedCapability(
            'text Watch requires a registered host evaluator',
          )
        const observedSeq = store.currentSeq(space)
        const result = evaluate(
          structuredClone(condition),
          structuredClone(envelope),
        )
        if (store.currentSeq(space) !== observedSeq)
          conflict('evaluator_changed_snapshot')
        if (typeof result !== 'boolean')
          invalid(
            'synchronous Watch evaluator must return a boolean; use prepareWatchPage for async evaluation',
          )
        semanticHit = result
      }
      if (hit && (!semantic || semanticHit)) {
        matched = true
        cp.matched_seq ??= Number(envelope.space_seq)
      }
    }
    watch.matched = matched
    watch.consumed_seq = coverage.through_seq!
    const covered = coverage.complete === true,
      status =
        (!silence && matched) || (silence && deadline && covered && !matched)
          ? 'fired'
          : deadline && covered
            ? 'expired'
            : 'armed'
    const tx = transaction(s, space),
      id = parseElementId(ref)
    tx.expectVersions(id, [{ version: expected, plane: null }])
    const updated = values(tx.load(id)).row
    updated.attributes.status = status
    updated.facets[PROFILE + 'WatchState'] = watch
    tx.authorizedWatchUpdates.add(ref)
    tx.markChanged(id, 'update')
    if (!cfg) stageControl(tx, CONFIG, 0, 'runtime', cp.config)
    stageControl(
      tx,
      checkpointKey(ref, generation),
      saved?.version ?? 0,
      'runtime',
      cp,
    )
    const result: JsonMap = { status, watch, coverage }
    if (mode.kind === 'evaluate') {
      result.ticket_ref = mode.ticketRef
      result.evaluation_ref = recordEvaluation(
        tx,
        mode.ticketRef,
        mode.evaluation,
        mode.payload,
        true,
      )
    }
    if (status === 'fired') {
      if (!silence && cp.matched_seq === null)
        invalid('firing lacks matching envelope')
      const fire: WatchFire = {
          watch_ref: ref,
          arm_generation: generation,
          trigger: silence
            ? { kind: 'silence', due_at: cp.due_at!, due_seq: cp.due_seq! }
            : { kind: 'delta', matched_seq: cp.matched_seq! },
        },
        fire_key = fireKey(fire)
      const command = parseKip(
        'CREATE ACTIVITY ?watch_fire {CLIENT KEY :key SET FIELDS {activity_class:"watch_fire",status:"completed"} SET STRUCTURAL {("inputs",:watch)}}',
      )
      if (!('Kml' in command)) invalid('invalid native fire plan')
      planKml(tx, command.Kml, { key: fire_key, watch: ref })
      const activity = tx.handles().watch_fire!
      if (!tx.staged.get(activity)?.isNew)
        conflict('fire_identity_already_exists')
      tx.authorizedWatchFires.add(activity)
      const wakeRef = runtimeRef('wake', {
        domain: 'anda-brain:wake-v1',
        scope: cp.config.scope,
        fire_key,
      })
      const wake: WakeRecord = {
        format: FORMAT,
        scope: cp.config.scope,
        wake_ref: wakeRef,
        fire,
        fire_activity_ref: activity,
        pins: cp.config.pins,
        version: 1,
        fence: 0,
        state: { stage: 'pending', not_before_ms: 0 },
      }
      stageControl(tx, wakeRef, 0, 'wake', wake)
      Object.assign(result, {
        fire_key,
        fire_activity_ref: activity,
        wake_ref: wakeRef,
      })
    }
    return commit(s, tx, key, hash, result)
  })
}
export function checkControls(envelope: JsonMap): void {
  if (
    (envelope.control_changes as JsonMap[]).some((c) =>
      ['schema', 'identity', 'policy', 'trust', 'authorization', 'recording'].includes(
        String(c.kind),
      ),
    )
  )
    conflict('basis_changed')
}
