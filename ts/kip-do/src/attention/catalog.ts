import type { Session } from '../nexus.js'
import type { ControlRecord } from '../control.js'
import type { WakePage, WakeRecord, AttentionConfig } from './types.js'
import { authorizedArtifact, publishControl } from '../control.js'
import { errors } from '../errors.js'
import { digest } from '../schema/contracts.js'
import { parseElementId } from '../id.js'
import {
  CONFIG,
  obj,
  json,
  eq,
  bytes,
  safe,
  invalid,
  conflict,
  permit,
  fullRead,
  context,
  basis,
  validateConfig,
} from './common.js'
import { loadWake } from './work.js'

export function setAttentionConfig(
  s: Session,
  space: string,
  expected: number,
  config: AttentionConfig,
): import('../json.js').JsonMap {
  validateConfig(config)
  const previous = s.nexus.store.controlAt(space, CONFIG)
  if (previous && !eq(obj(previous.value).scope, config.scope))
    invalid('attention instance cannot be replaced in place')
  const row = publishControl(
    s.nexus.store,
    space,
    CONFIG,
    'policy',
    expected,
    json(config),
    { principal_id: s.auth.principal_id },
  )
  return { version: row.version, config: row.value }
}
export function authorizeControlRead(
  s: Session,
  space: string,
  row: ControlRecord,
): void {
  if (row.kind === 'wake') loadWake(s, space, row.key)
  else if (row.kind === 'dispatch' && row.key.startsWith('dispatch/v1/')) {
    const request = obj(obj(row.value).request)
    loadWake(s, space, String(request.wake_ref))
    fullRead(s, space, String(request.attempt_ref))
  } else if (row.kind === 'runtime' && row.key !== CONFIG) {
    if (!row.key.startsWith('watch-evaluation/v1/'))
      throw errors.notAuthorized(
        'attention runtime records use their dedicated read or replay API',
      )
    authorizedArtifact(
      s.nexus.store,
      space,
      String(obj(obj(row.value).material).artifact_ref),
      s.effectiveAuthority(space),
      s.auth,
    )
  }
}
interface Cursor {
  format: string
  space: string
  principal: string
  instance: string | null
  basis: string
  snapshot_seq: number
  after_id: number
}
export function listWakes(
  s: Session,
  space: string,
  cursor: string | null,
  limit: number,
): WakePage {
  if (!Number.isInteger(limit) || limit < 1 || limit > 200)
    invalid('wake scan limit must be 1..=200')
  permit(s, space, 'maintain')
  const store = s.nexus.store,
    authority = s.effectiveAuthority(space),
    head = store.currentSeq(space),
    currentBasis = digest(basis(context(s, space)))
  const instance = (obj(obj(store.controlAt(space, CONFIG)?.value).scope)
    .space_instance ?? null) as string | null
  let position: Cursor = {
    format: 'nexus:wake-cursor-v1',
    space,
    principal: s.auth.principal_id,
    instance,
    basis: currentBasis,
    snapshot_seq: head,
    after_id: 0,
  }
  if (cursor !== null) {
    if (bytes(cursor) > 8192 || !/^(?:[0-9a-f]{2})+$/.test(cursor))
      invalid('invalid wake cursor')
    try {
      position = JSON.parse(
        new TextDecoder('utf-8', { fatal: true, ignoreBOM: false }).decode(
          Uint8Array.from(cursor.match(/../g)!, (c) => parseInt(c, 16)),
        ),
      ) as Cursor
    } catch {
      invalid('invalid wake cursor')
    }
  }
  if (
    !position ||
    position.format !== 'nexus:wake-cursor-v1' ||
    position.space !== space ||
    position.principal !== s.auth.principal_id ||
    position.instance !== instance ||
    position.basis !== currentBasis ||
    !safe(position.snapshot_seq) ||
    position.snapshot_seq > head ||
    !safe(position.after_id)
  )
    conflict('wake_cursor_basis_changed')
  const rows = store.all<ControlRecord>(
    'kip_control_records',
    "SELECT * FROM kip_control_records WHERE space = ? AND kind = 'wake' AND seq <= ? AND id > ? ORDER BY id LIMIT ?",
    space,
    position.snapshot_seq,
    position.after_id,
    limit + 1,
  )
  const complete = rows.length <= limit,
    scanned = Math.min(rows.length, limit),
    items: WakeRecord[] = []
  for (const row of rows.slice(0, limit)) {
    position.after_id = row.id
    if (store.controlAt(space, row.key, position.snapshot_seq)?.id !== row.id)
      continue
    const wake = row.value as unknown as WakeRecord
    if (wake.wake_ref !== row.key || wake.version !== row.version)
      invalid('wake identity mismatch')
    if (
      [wake.fire.watch_ref, wake.fire_activity_ref].every((ref) => {
        const e = store.load(parseElementId(ref)),
          v = e && authority.mayRead(e, s.auth)
        return (
          e?.row.space === space && v?.content && !v.constraints.fields.length
        )
      })
    )
      items.push(wake)
  }
  const nextCursor = complete
    ? null
    : [...new TextEncoder().encode(JSON.stringify(position))]
        .map((b) => b.toString(16).padStart(2, '0'))
        .join('')
  return {
    items,
    snapshot_seq: position.snapshot_seq,
    scanned,
    next_cursor: nextCursor,
    complete,
  }
}
