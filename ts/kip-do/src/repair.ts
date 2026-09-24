/**
 * Recording repair (Spec §57.8).
 *
 * Extraction or attribution can be wrong while the captured source is right:
 * the Brain recorded that Alice said something she never said. That is not
 * Alice's retraction and not a correction of sound Evidence, and writing it as
 * either would forge a history:
 *
 * ```text
 * the actor's claim was wrong     supersession by the same actor   (§14.2)
 * the world moved on              one new Assertion; succession    (§25.4)
 * the Brain recorded it wrongly   recording repair                 (here)
 * ```
 *
 * One transaction verifies the source's identity, digest and locator, the
 * recorder's origin, the expected versions and every replacement, then appends
 * a terminal `recording_repair` Activity and invalidates the wrong extraction.
 * The invalidation is protected Governance state on the element — a new
 * version, the payload untouched — so history reads the repair state at its
 * own snapshot, and `_system.recording_validity` names the repair. Current
 * projection excludes it; a derived artifact that depended on it needs review.
 *
 * The repair Activity names the source and the invalidated extraction as its
 * inputs; the replacements, the recorder's own ordinary Assertions, are named
 * only in the `RecordingRepair` Facet. Mirrors
 * `rs/anda_cognitive_nexus/src/repair.rs`.
 */

import type { AuthContext, EffectiveAuthority, Permission } from './governance/index.js'
import { requirePermitted, resourceOfElement } from './governance/index.js'
import { errors, KipError } from './errors.js'
import { canonicalJson, isJsonMap, type Json, type JsonMap } from './json.js'
import { formatElementId, tryParseElementId } from './id.js'
import { endpointFromJson, endpointKey, referencedElement } from './term.js'
import { parseTime } from './time.js'
import { parseKip } from './kip/parser.js'
import { planKml } from './kml/index.js'
import { digest } from './schema/contracts.js'
import { State, type Element } from './store/index.js'
import type { Store } from './store/store.js'
import type { EvidenceRow } from './store/rows.js'
import { REPAIR_CLASS, REPAIR_FACET, REPAIR_KEY, repairRef } from './recording.js'
import type { Transaction } from './tx.js'

/** The input of the protected repair (`kip-cognitive-records.schema.json#/$defs/RecordingRepair`). */
export interface RecordingRepair {
  source_ref: string
  source_digest: string
  /** A JSON Pointer or `bytes=<start>-<end>` range inside the source. */
  source_locator: string
  invalidated_refs: string[]
  replacement_refs: string[]
  reason: 'extraction_error' | 'attribution_error'
  /** `_system.version` each named element must still have. */
  expected_versions: Record<string, number>
}

export { REPAIR_CLASS, REPAIR_FACET, REPAIR_KEY, isInvalidated, recordingValidity, repairRef } from './recording.js'
export type { RecordingValidity } from './recording.js'

export interface RepairHost {
  store: Store
  authority: EffectiveAuthority
  auth: AuthContext
  /** A transaction in the repair's Space, begun by the caller. */
  begin(): Transaction
}

/**
 * Runs one repair inside the caller's platform transaction; a thrown error
 * rolls everything back. A retry of the same repair answers from the recorded
 * one without writing.
 */
export function repairRecording(host: RepairHost, space: string, repair: RecordingRepair): JsonMap {
  checkShape(repair)
  const key = `${REPAIR_CLASS}:${digest(repair as unknown as Json)}`
  const recorded = host.store.all<{ id: number }>(
    'activities',
    'SELECT id FROM activities WHERE space = ? AND client_key = ?',
    space,
    key,
  )[0]
  if (recorded !== undefined) {
    const activity = formatElementId({ kind: 'Activity', seq: recorded.id })
    visible(host, space, activity)
    authorizeRepair(host, space, repair)
    return {
      repair_ref: activity,
      invalidated_refs: repair.invalidated_refs,
      replacement_refs: repair.replacement_refs,
      replayed: true,
    }
  }
  check(host, space, repair)
  const tx = host.begin()
  const activity = stage(tx, repair, key)
  const outcome = tx.commit('')
  return {
    repair_ref: activity,
    invalidated_refs: repair.invalidated_refs,
    replacement_refs: repair.replacement_refs,
    receipt: {
      tx_id: outcome.tx_id,
      space_id: outcome.space_id,
      space_seq: outcome.space_seq,
      committed_at: outcome.committed_at,
      status: outcome.status,
    },
  }
}

function checkShape(repair: RecordingRepair): void {
  const fail = (message: string): never => { throw errors.constraintViolation(message) }
  if (!Array.isArray(repair.invalidated_refs) || repair.invalidated_refs.length === 0) {
    fail('a recording repair invalidates at least one extraction')
  }
  if (!repair.source_ref || !repair.source_locator) {
    fail('a recording repair names its source and a locator inside it')
  }
  if (repair.reason !== 'extraction_error' && repair.reason !== 'attribution_error') {
    fail('reason is extraction_error or attribution_error')
  }
  const replacements = repair.replacement_refs ?? []
  const invalidated = new Set(repair.invalidated_refs)
  const replaced = new Set(replacements)
  if (invalidated.size !== repair.invalidated_refs.length || replaced.size !== replacements.length) {
    fail('a recording repair names each element once')
  }
  if ([...invalidated].some((ref) => replaced.has(ref)) || invalidated.has(repair.source_ref)) {
    fail('an invalidated extraction is neither its own replacement nor the source')
  }
  const versions = repair.expected_versions ?? {}
  for (const ref of repair.invalidated_refs) {
    if (!Object.hasOwn(versions, ref)) fail(`expected_versions must guard the invalidated extraction ${ref}`)
  }
  for (const ref of Object.keys(versions)) {
    if (!invalidated.has(ref) && !replaced.has(ref)) {
      fail(`expected_versions names ${ref}, which this repair does not touch`)
    }
  }
}

/** An element the caller may read in full, existence-neutrally. */
function visible(host: RepairHost, space: string, ref: string): Element {
  const unavailable = () => errors.notFoundOrNotVisible(`${ref} is unavailable`)
  const id = tryParseElementId(ref)
  const element = id === null ? null : host.store.load(id)
  if (!element || element.row.space !== space ||
    element.row.state === State.PURGED || element.row.state === State.PENDING) {
    throw unavailable()
  }
  const view = host.authority.mayRead(element, host.auth)
  if (!view?.content || view.constraints.fields.length) throw unavailable()
  return element
}

function authorizeRepair(host: RepairHost, space: string, repair: RecordingRepair): void {
  for (const ref of repair.invalidated_refs) {
    const element = visible(host, space, ref)
    requirePermitted(host.authority.authorize('repair_recording', resourceOfElement(element), host.auth))
  }
}

const cites = (row: { evidence_refs: { id: string }[] }, source: string): boolean =>
  row.evidence_refs.some((reference) => reference.id === source)

const recorder = (row: { origin: JsonMap }): string | null =>
  row.origin.import !== undefined ? null : typeof row.origin.principal_id === 'string' ? row.origin.principal_id : null

function stale(ref: string, actual: number, expected: number): KipError {
  return errors.versionConflict(`${ref} is at version ${actual}, not the expected ${expected}`)
}

/** Compare actors after merges, keeping the original Assertion untouched. */
function actorKey(host: RepairHost, space: string, actor: Json): string {
  let id = referencedElement(actor)
  if (id === null) return endpointKey(endpointFromJson(actor))
  for (let hop = 0; hop < 64; hop++) {
    if (id.kind !== 'Concept') return endpointKey({ kind: 'local', id })
    const element = host.store.load(id)
    if (!element || element.row.space !== space) throw errors.notFoundOrNotVisible('actor is unavailable')
    if (element.kind !== 'Concept' || element.row.merged_into === '') return endpointKey({ kind: 'local', id })
    const next = tryParseElementId(element.row.merged_into)
    if (next === null) throw errors.internalError('invalid actor merge target')
    id = next
  }
  throw errors.internalError('actor merge chain exceeds 64 hops')
}

/** Everything §57.8 asks the engine to verify, against current state. */
function check(host: RepairHost, space: string, repair: RecordingRepair): void {
  const source = visible(host, space, repair.source_ref)
  if (source.kind !== 'Evidence') {
    throw errors.constraintViolation('a recording repair names captured Evidence as its source')
  }
  verifySource(source.row, repair)
  const sourceTime = timestampAtLocator(source.row.payload_inline as Json, repair.source_locator)

  const principal = host.auth.principal_id
  const actors = new Set<string>()
  const times = new Set<string>()
  for (const ref of repair.invalidated_refs) {
    const element = visible(host, space, ref)
    requirePermitted(host.authority.authorize('repair_recording', resourceOfElement(element), host.auth))
    if (element.kind !== 'Assertion') {
      throw errors.constraintViolation(`${ref} is not an Assertion; a recording repair invalidates an extracted claim`)
    }
    const row = element.row
    const earlier = repairRef(row.governance)
    if (earlier !== null) throw errors.constraintViolation(`${ref} was already invalidated by ${earlier}`)
    if (row.state === State.TOMBSTONED) throw errors.notFoundOrNotVisible(`${ref} is unavailable`)
    if (!cites(row, repair.source_ref)) {
      throw errors.constraintViolation(`${ref} does not cite ${repair.source_ref}; a repair covers outputs of that source`)
    }
    // §57.8: by default a repair reaches only the recorder's own source-backed
    // outputs. Recording attribution is not authority.
    if (recorder(row) !== principal) {
      throw errors.notAuthorized(
        `${ref} was not recorded by this Principal; a recording repair reaches only the ` +
          `recorder's own source-backed outputs (§57.8)`,
      )
    }
    const expected = repair.expected_versions[ref]!
    if (expected !== row.version) throw stale(ref, row.version, expected)
    if (repair.reason === 'extraction_error' && repair.replacement_refs.length > 0) {
      actors.add(actorKey(host, space, row.asserted_by as Json))
    }
    times.add(row.asserted_at)
  }

  const observedAt = source.row.observed_at
  for (const ref of repair.replacement_refs) {
    const element = visible(host, space, ref)
    if (element.kind !== 'Assertion') throw errors.constraintViolation(`replacement ${ref} is not an Assertion`)
    const row = element.row
    if (row.state !== State.ACTIVE || row.status !== 'active' || repairRef(row.governance) !== null) {
      throw errors.constraintViolation(`replacement ${ref} is not an active, standing Assertion`)
    }
    if (recorder(row) !== principal) {
      throw errors.notAuthorized(`replacement ${ref} was not recorded by this Principal`)
    }
    // The ordinary permission for the replacement, as of now.
    const permission: Permission =
      row.asserted_by_key === '' || host.authority.isBoundToActor(row.asserted_by_key)
        ? 'assert'
        : 'record_attributed_assertion'
    requirePermitted(host.authority.authorize(permission, resourceOfElement(element), host.auth))
    if (!cites(row, repair.source_ref)) {
      throw errors.constraintViolation(`replacement ${ref} does not cite ${repair.source_ref}`)
    }
    if (repair.reason === 'extraction_error' && !actors.has(actorKey(host, space, row.asserted_by as Json))) {
      throw errors.constraintViolation(
        `replacement ${ref} names another actor; an extraction error keeps the actor, ` +
          `and a wrong actor is an attribution_error`,
      )
    }
    // §57.8: a replacement describes the original claim, so its claim time is
    // recovered from the original source — never the repair's time.
    // A historical message can already have supplied its own claim time,
    // earlier than observed_at (§13.2). Capture time does not override it.
    const preserved = times.has(row.asserted_at) && (observedAt === '' || row.asserted_at <= observedAt)
    const claimed = preserved || row.asserted_at === observedAt || row.asserted_at === sourceTime
    if (!claimed) {
      throw errors.constraintViolation(
        `replacement ${ref} is asserted at ${row.asserted_at}, not at the original source's time; ` +
          `a repair recovers asserted_at from the source it repairs (§57.8)`,
      )
    }
    visible(host, space, row.proposition_id)
    for (const evidence of row.evidence_refs) visible(host, space, evidence.id)
    const expected = repair.expected_versions[ref]
    if (expected !== undefined && expected !== row.version) throw stale(ref, row.version, expected)
  }
}

/**
 * The digest a source is verified against: the one it was captured with, or —
 * for bytes held inline without one, as ingestion mints them (§71.1) — the
 * canonical digest of those bytes.
 */
export function sourceDigest(evidence: EvidenceRow): string | null {
  if (evidence.content_digest !== '') return evidence.content_digest
  if (evidence.payload_mode === 'inline' && evidence.payload_inline !== null && evidence.payload_inline !== undefined) {
    return digest(evidence.payload_inline)
  }
  return null
}

/** Same digest, and the locator resolves inside bytes the engine still holds. */
function verifySource(evidence: EvidenceRow, repair: RecordingRepair): void {
  const captured = sourceDigest(evidence)
  if (captured === null) {
    throw errors.constraintViolation('the source carries no digest a repair could be verified against')
  }
  if (captured !== repair.source_digest) {
    throw errors.digestMismatch('the source digest does not match the captured source')
  }
  if (evidence.payload_mode !== 'inline' || evidence.payload_inline === null || evidence.payload_inline === undefined) {
    throw errors.constraintViolation('the source bytes are not held inline, so the locator cannot be verified')
  }
  verifyLocator(evidence.payload_inline, repair.source_locator)
}

/**
 * A JSON Pointer into the payload, or `bytes=<start>-<end>` (inclusive) over
 * its text — the string itself, or the canonical JSON of anything else.
 */
export function verifyLocator(payload: Json, locator: string): void {
  const unresolved = () =>
    errors.constraintViolation(`source locator ${JSON.stringify(locator)} does not resolve inside the source`)
  if (locator.startsWith('/')) {
    let cursor: Json | undefined = payload
    for (const raw of locator.slice(1).split('/')) {
      const token = raw.replace(/~1/g, '/').replace(/~0/g, '~')
      if (Array.isArray(cursor)) {
        if (!/^(0|[1-9]\d*)$/.test(token)) throw unresolved()
        cursor = cursor[Number(token)]
      } else if (isJsonMap(cursor) && Object.hasOwn(cursor, token)) {
        cursor = cursor[token]
      } else {
        throw unresolved()
      }
      if (cursor === undefined) throw unresolved()
    }
    return
  }
  const range = /^bytes=(\d+)-(\d+)$/.exec(locator)
  if (range) {
    const [start, end] = [Number(range[1]), Number(range[2])]
    const text = typeof payload === 'string' ? payload : canonicalJson(payload)
    const length = new TextEncoder().encode(text).length
    if (start <= end && end < length) return
    throw unresolved()
  }
  throw errors.constraintViolation(
    `unsupported source locator ${JSON.stringify(locator)}: use a JSON Pointer or bytes=<start>-<end>`,
  )
}

/** Recover only a timestamp the locator explicitly selects; never guess date fields. */
function timestampAtLocator(payload: Json, locator: string): string | undefined {
  let selected: Json | undefined
  if (locator.startsWith('/')) {
    selected = payload
    for (const raw of locator.slice(1).split('/')) {
      const token = raw.replace(/~1/g, '/').replace(/~0/g, '~')
      selected = Array.isArray(selected) ? selected[Number(token)]
        : isJsonMap(selected) ? selected[token] : undefined
    }
  } else {
    const range = /^bytes=(\d+)-(\d+)$/.exec(locator)
    if (range) {
      const text = typeof payload === 'string' ? payload : canonicalJson(payload)
      selected = new TextDecoder().decode(new TextEncoder().encode(text).slice(Number(range[1]), Number(range[2]) + 1))
    }
  }
  if (typeof selected !== 'string') return undefined
  try { parseTime(selected); return selected } catch { return undefined }
}

/** Plans the repair Activity, the invalidations and the `recording` control coordinate (§36.1). */
function stage(tx: Transaction, repair: RecordingRepair, key: string): string {
  const inputs = [repair.source_ref, ...repair.invalidated_refs]
  const tuples = inputs.map((_, i) => `("inputs", :input${i})`).join(' ')
  const command = parseKip(
    `CREATE ACTIVITY ?repair {
      CLIENT KEY :key
      SET FIELDS {activity_class: "${REPAIR_CLASS}", status: "completed", started_at: :now, ended_at: :now}
      SET FACET "${REPAIR_FACET}" {
        source_ref: :source_ref, source_digest: :source_digest,
        source_locator: :source_locator, invalidated_refs: :invalidated_refs,
        replacement_refs: :replacement_refs, reason: :reason,
        expected_versions: :expected_versions
      }
      SET STRUCTURAL { ${tuples} }
    }`,
  )
  if (!('Kml' in command)) throw errors.internalError('repair command is not KML')
  const parameters: JsonMap = {
    key,
    now: tx.cx.at,
    source_ref: repair.source_ref,
    source_digest: repair.source_digest,
    source_locator: repair.source_locator,
    invalidated_refs: repair.invalidated_refs,
    replacement_refs: repair.replacement_refs,
    reason: repair.reason,
    expected_versions: repair.expected_versions,
  }
  inputs.forEach((ref, i) => { parameters[`input${i}`] = ref })
  planKml(tx, command.Kml, parameters)
  const activity = tx.handles().repair!
  tx.authorizedRecordingRepairs.add(activity)
  for (const ref of repair.invalidated_refs) {
    const id = tryParseElementId(ref)!
    const element = tx.load(id)
    element.row.governance = { ...element.row.governance, [REPAIR_KEY]: activity }
    tx.markChanged(id, 'update')
  }
  const controlKey = `recording/${activity}`
  tx.controlEffects.push({
    record_id: `${tx.cx.tx_id}:${controlKey}`,
    space: tx.cx.space,
    key: controlKey,
    seq: 0,
    version: 1,
    kind: 'recording',
    value: {
      repair_ref: activity,
      source_ref: repair.source_ref,
      invalidated_refs: repair.invalidated_refs,
      replacement_refs: repair.replacement_refs,
      reason: repair.reason,
    },
    origin: tx.cx.origin,
  })
  return activity
}
