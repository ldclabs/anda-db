import type { Session } from '../nexus.js'
import type { JsonMap } from '../json.js'
import type {
  RuntimePin,
  DispatchLookup,
  DispatchLookupObserver,
} from './types.js'
import { SYSTEM_PRINCIPAL } from '../governance/index.js'
import { errors } from '../errors.js'
import { digest } from '../schema/contracts.js'
import { nowTime, normalizeTime } from '../time.js'
import { publishControl } from '../control.js'
import { checkAttentionAttempt } from '../runtime.js'
import {
  PROFILE,
  obj,
  json,
  eq,
  bounded,
  validDigest,
  validatePin,
  exact,
  invalid,
  conflict,
  next,
  fullRead,
  runtimeRef,
  requestKey,
  stageControl,
  transaction,
  replay,
  commit,
} from './common.js'
import { loadWake, live, currentWake } from './work.js'

const observerKey = (pin: RuntimePin): string =>
  runtimeRef('attention-lookup-observer', pin)
function observer(s: Session, space: string, pin: RuntimePin): JsonMap | null {
  const row = s.nexus.store.controlAt(space, observerKey(pin))
  return row ? { version: row.version, observer: row.value } : null
}
export function setLookupObserver(
  s: Session,
  space: string,
  expected: number,
  value: DispatchLookupObserver,
): JsonMap {
  exact(value, ['binding', 'principal_id', 'configuration_digest'])
  validatePin(value.binding)
  if (
    !bounded(value.principal_id) ||
    !value.principal_id.startsWith('kip:principal:') ||
    value.principal_id === SYSTEM_PRINCIPAL ||
    !validDigest(value.configuration_digest)
  )
    invalid('invalid direct lookup observer')
  const row = publishControl(
    s.nexus.store,
    space,
    observerKey(value.binding),
    'policy',
    expected,
    json(value),
    { principal_id: s.auth.principal_id },
  )
  return { version: row.version, observer: row.value }
}
export function beginWakeDispatch(
  s: Session,
  space: string,
  ref: string,
  expected: number,
  fence: number,
  attemptRef: string,
  supportsIdempotency: boolean,
  supportsOutcomeLookup: boolean,
): JsonMap {
  return s.nexus.transact(() => {
    if (
      typeof supportsIdempotency !== 'boolean' ||
      typeof supportsOutcomeLookup !== 'boolean'
    )
      invalid('dispatch capabilities must be booleans')
    const store = s.nexus.store,
      wake = loadWake(s, space, ref)
    if (wake.version !== expected) conflict('version_conflict')
    if (wake.fence !== fence) conflict('lease_lost')
    live(s, wake)
    currentWake(s, space, wake)
    if (!wake.pins.binding)
      throw errors.unsupportedCapability(
        'wake dispatch requires a registered host binding',
      )
    const attempt = checkAttentionAttempt(s, space, attemptRef),
      decision = fullRead(s, space, String(attempt.decision_ref))
    if (
      decision.kind !== 'Activity' ||
      !decision.row.inputs.some((r) =>
        [wake.fire_activity_ref, wake.fire.watch_ref].includes(
          typeof r === 'string' ? r : String(obj(json(r)).id),
        ),
      )
    )
      invalid('dispatch decision must name its Watch/fire input')
    const request = {
      wake_ref: ref,
      attempt_ref: attemptRef,
      supports_idempotency: supportsIdempotency,
      supports_outcome_lookup: supportsOutcomeLookup,
      binding: wake.pins.binding,
    }
    const dispatchRef = runtimeRef('dispatch', {
        scope: wake.scope,
        attempt_id: attempt.attempt_id,
      }),
      old = store.controlAt(space, dispatchRef),
      saved = obj(old?.value),
      lookup = observer(s, space, wake.pins.binding)
    if (supportsOutcomeLookup && !lookup)
      throw errors.unsupportedCapability(
        'wake dispatch outcome lookup requires a registered observer',
      )
    if (old && !eq(saved.lookup_observer, lookup))
      conflict('lookup_observer_changed')
    if (old && !eq(saved.request, request)) invalid('idempotency_conflict')
    const action =
      saved.state === 'completed'
        ? 'done'
        : saved.state === 'ready'
          ? 'dispatch'
          : saved.state === 'outcome_unknown'
            ? 'outcome_unknown'
            : !old || supportsIdempotency
              ? 'dispatch'
              : supportsOutcomeLookup
                ? 'lookup'
                : 'outcome_unknown'
    const version = old?.version ?? 0,
      at = nowTime(),
      value = {
        request,
        state:
          action === 'done'
            ? 'completed'
            : action === 'outcome_unknown'
              ? 'outcome_unknown'
              : 'dispatching',
        attempt_id: attempt.attempt_id!,
        fencing_token: fence,
        outcome_ref: saved.outcome_ref ?? null,
        lookup_observer: lookup,
        first_dispatch_at: saved.first_dispatch_at ?? at,
        last_dispatch_at:
          action === 'dispatch' ? at : (saved.last_dispatch_at ?? null),
        lookup_receipt_ref: saved.lookup_receipt_ref ?? null,
      }
    const tx = transaction(s, space)
    tx.attentionLeases.push({ reference: ref, version: expected, fence })
    stageControl(tx, dispatchRef, version, 'dispatch', value)
    if (!old) {
      const indexKey = `attention/dispatches/${ref}`,
        index = store.controlAt(space, indexKey),
        refs = (index?.value ?? []) as string[]
      if (refs.length >= 128) invalid('wake dispatch budget exhausted')
      stageControl(tx, indexKey, index?.version ?? 0, 'runtime', [
        ...refs,
        dispatchRef,
      ])
    }
    const call = {
      operation: 'begin_wake_dispatch',
      dispatch_ref: dispatchRef,
      version,
      request,
      fence,
    }
    // Deliberately do not replay a prior send permission.
    return commit(s, tx, requestKey(s, call), digest(json(call)), {
      action,
      dispatch_ref: dispatchRef,
      idempotency_key: attempt.attempt_id!,
      version: next(version),
      intent: json(value),
    })
  })
}
export function reconcileWakeDispatch(
  s: Session,
  space: string,
  ref: string,
  expected: number,
  outcomeRef: string,
  authorizeWrite: () => void,
): JsonMap {
  const store = s.nexus.store,
    old = store.controlAt(space, ref)
  if (!old) throw errors.notFoundOrNotVisible('dispatch unavailable')
  if (old.kind !== 'dispatch' || !ref.startsWith('dispatch/v1/'))
    invalid('not a wake dispatch')
  const saved = obj(old.value),
    outcome = fullRead(s, space, outcomeRef),
    record = obj(outcome.row.facets[PROFILE + 'OutcomeRecord'])
  if (
    record.attempt_ref !== obj(saved.request).attempt_ref ||
    record.terminal !== true
  )
    invalid('observation does not close this attempt')
  fullRead(s, space, String(record.attempt_ref))
  if (saved.outcome_ref === outcomeRef) return saved
  authorizeWrite()
  if (old.version !== expected) conflict('version_conflict')
  if (typeof saved.outcome_ref === 'string')
    invalid('conflicting terminal observation')
  return obj(
    publishControl(
      store,
      space,
      ref,
      'dispatch',
      expected,
      {
        ...saved,
        state:
          record.outcome_status === 'unknown' ? 'outcome_unknown' : 'completed',
        outcome_ref: outcomeRef,
      },
      { principal_id: s.auth.principal_id },
    ).value,
  )
}
export function reconcileWakeLookup(
  s: Session,
  space: string,
  ref: string,
  expected: number,
  observation: DispatchLookup,
  authorizeWrite: () => void,
): JsonMap {
  exact(observation, [
    'observation_key',
    'observed_at',
    'configuration_digest',
    'status',
  ])
  if (
    !bounded(observation.observation_key) ||
    !['not_started', 'running', 'finished', 'unknown'].includes(
      observation.status,
    )
  )
    invalid('invalid dispatch lookup observation')
  observation = {
    ...observation,
    observed_at: normalizeTime(observation.observed_at, 'lookup observed_at'),
  }
  const store = s.nexus.store,
    old = store.controlAt(space, ref)
  if (!old) throw errors.notFoundOrNotVisible('dispatch unavailable')
  const saved = obj(old.value),
    request = obj(saved.request),
    retained = obj(saved.lookup_observer),
    config = retained.observer as unknown as DispatchLookupObserver | undefined
  if (
    old.kind !== 'dispatch' ||
    !ref.startsWith('dispatch/v1/') ||
    request.supports_outcome_lookup !== true
  )
    invalid('dispatch has no authoritative lookup channel')
  if (!config)
    throw errors.unsupportedCapability('dispatch did not pin a lookup observer')
  if (
    config.principal_id !== s.auth.principal_id ||
    !s.auth.auth_method ||
    s.auth.auth_strength === 'none' ||
    s.auth.delegation_chain.length
  )
    throw errors.notAuthorized(
      'directly authenticated lookup observer required',
    )
  if (
    config.configuration_digest !== observation.configuration_digest ||
    !eq(observer(s, space, config.binding), retained)
  )
    conflict('lookup_observer_changed')
  fullRead(s, space, String(request.attempt_ref))
  const identity = {
      operation: 'reconcile_wake_lookup',
      dispatch_ref: ref,
      observation_key: observation.observation_key,
    },
    key = requestKey(s, identity),
    hash = digest(json({ dispatch_ref: ref, observation })),
    previous = replay(s, space, key, hash)
  if (previous) return previous
  authorizeWrite()
  if (old.version !== expected) conflict('version_conflict')
  if (typeof saved.outcome_ref === 'string' || saved.state === 'completed')
    invalid('terminal Outcome cannot be reopened by lookup')
  if (
    typeof saved.last_dispatch_at !== 'string' ||
    observation.observed_at < saved.last_dispatch_at ||
    observation.observed_at > nowTime()
  )
    invalid('lookup time is outside the dispatch interval')
  const receiptRef = runtimeRef('dispatch-lookup', {
    scope: space,
    principal: s.auth.principal_id,
    dispatch_ref: ref,
    observation_key: observation.observation_key,
  })
  const value = {
      ...saved,
      state:
        observation.status === 'not_started'
          ? 'ready'
          : observation.status === 'unknown'
            ? 'outcome_unknown'
            : 'dispatching',
      lookup_receipt_ref: receiptRef,
    },
    tx = transaction(s, space)
  stageControl(tx, receiptRef, 0, 'runtime', {
    format: 'nexus:dispatch-lookup-v1',
    dispatch_ref: ref,
    observation,
    observer: config,
  })
  stageControl(tx, ref, expected, 'dispatch', value)
  return commit(s, tx, key, hash, {
    dispatch_ref: ref,
    version: next(expected),
    intent: value,
    lookup_receipt_ref: receiptRef,
  })
}
