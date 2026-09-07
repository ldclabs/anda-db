import { canonicalJson } from './json.js'
/**
 * The request envelope (§71), and every invariant of it this engine checks
 * before anything runs.
 *
 * Fail-fast and whole-request: each of these decides what the operations
 * *mean*, so running half a batch and then discovering the envelope was
 * malformed would leave durable writes behind a request that was never valid.
 * A pure function over the envelope and the Space it names, so the rules can
 * be read and tested without a Durable Object around them — the reference
 * engine keeps the same rules in `anda_kip::Request::validate`.
 */
import { KipError } from './errors.js'
import type { RequestContext } from './governance/index.js'
import type { JsonMap } from './json.js'
import { parseKip } from './kip/parser.js'
import { checkIngest, type IngestContext } from './kml/index.js'
import { capabilityState, KIP_VERSION } from './meta/capabilities.js'
import type { ReadOptions } from './nexus.js'

/** The Space a request runs in, as the check needs it. */
export interface EnvelopeSpace {
  id: string
  row(): { seq: number; schema_environment_version: number }
}

/** The execution modes §75 names. */
const EXECUTION_MODES = ['independent', 'sequence', 'atomic'] as const
/**
 * Exported because {@link KipDatabase.executeKipBatch} takes one.
 *
 * A public method whose argument type has no name is a method a host can only
 * call with an inline literal, which is the kind of gap a published surface is
 * supposed to close rather than create.
 */
export type ExecutionMode = (typeof EXECUTION_MODES)[number]

/** The `on_error` values §75.2 names. */
const ON_ERRORS = ['stop', 'continue'] as const
/** Exported for the same reason as {@link ExecutionMode}. */
export type OnError = (typeof ON_ERRORS)[number]

/** The request envelope this object reads (Spec §71). */
export interface KipRequestEnvelope {
  kip?: string
  request_id?: string
  space?: { id?: string }
  compatibility_profile?: string
  execution?: {
    mode?: string
    on_error?: OnError
    isolation?: string
    idempotency_key?: string
  }
  read?: ReadOptions
  ingest?: IngestContext
  preconditions?: { space_seq?: number; schema_environment_version?: number }
  operations?: {
    op_id?: string
    command?: string
    parameters?: JsonMap
    idempotency_key?: string
  }[]
  parameters?: JsonMap
  context?: RequestContext
  requires?: Record<string, boolean>
  options?: { deadline_ms?: number }
  extensions?: JsonMap
}

/**
 * The namespaced extensions a request marks `critical`, across every block
 * that may carry an `extensions` map: the envelope, `execution`, `read`,
 * `preconditions`, `context`, `options`, `ingest`, and each operation.
 */
function criticalExtensions(envelope: KipRequestEnvelope): string[] {
  const blocks: unknown[] = [
    envelope,
    envelope.execution,
    envelope.read,
    envelope.preconditions,
    envelope.context,
    envelope.options,
    envelope.ingest,
    ...(envelope.operations ?? []),
  ]
  const names = new Set<string>()
  for (const block of blocks) {
    if (block === null || typeof block !== 'object') continue
    const extensions = (block as { extensions?: unknown }).extensions
    if (extensions === null || typeof extensions !== 'object') continue
    for (const [name, value] of Object.entries(extensions as Record<string, unknown>)) {
      if (
        value !== null &&
        typeof value === 'object' &&
        (value as { critical?: unknown }).critical === true
      ) {
        names.add(name)
      }
    }
  }
  return [...names].sort()
}

/**
 * Every envelope invariant this engine can check before anything runs.
 *
 * Fail-fast and whole-request: each of these decides what the operations
 * *mean*, so running half a batch and then discovering the envelope was
 * malformed would leave durable writes behind a request that was never
 * valid.
 */
export function checkEnvelope(envelope: KipRequestEnvelope, space: EnvelopeSpace): void {
  try { canonicalJson(envelope) } catch (error) {
    throw new KipError('InvalidRequestEnvelope', String(error))
  }

  // §87.1. Silently executing a request that declared another protocol
  // version is the failure this code exists for: the caller believes it is
  // talking to the version it named.
  // Required, not merely checked when present: `kip` is in the normative
  // request schema's `required` list, and a request that names no version
  // gets run under whichever one the engine happens to be — which is the
  // same silent mismatch, minus the evidence.
  if (envelope.kip !== KIP_VERSION) {
    throw new KipError(
      'UnsupportedProtocolVersion',
      envelope.kip === undefined
        ? `this runtime speaks KIP ${KIP_VERSION}, and a request must say ` +
          `which version it speaks: set "kip": "${KIP_VERSION}"`
        : `this runtime speaks KIP ${KIP_VERSION}, the request declares ` +
          JSON.stringify(envelope.kip),
    )
  }

  if (
    envelope.compatibility_profile !== undefined &&
    (typeof envelope.compatibility_profile !== 'string' ||
      envelope.compatibility_profile === '')
  ) {
    throw new KipError(
      'InvalidRequestEnvelope',
      '`compatibility_profile` names a profile, so it is a non-empty string when given',
    )
  }
  const operations = envelope.operations ?? []
  if (operations.length === 0) {
    throw new KipError(
      'InvalidRequestEnvelope',
      'a request needs at least one operation',
    )
  }
  // §75: whether earlier commits survive a later failure is not a detail to
  // leave to an engine default, so a multi-operation request declares it.
  if (operations.length > 1 && envelope.execution === undefined) {
    throw new KipError(
      'InvalidRequestEnvelope',
      'a multi-operation request must declare execution.mode: independent, ' +
        'sequence or atomic — operations[] is a batch, not a transaction',
    )
  }
  const seen = new Set<string>()
  for (const operation of operations) {
    if (operation.op_id === undefined) continue
    if (seen.has(operation.op_id)) {
      throw new KipError(
        'InvalidRequestEnvelope',
        `op_id ${JSON.stringify(operation.op_id)} appears twice; it is ` +
          `request-local and is how a caller pairs an answer with its operation`,
      )
    }
    seen.add(operation.op_id)
  }

  const mode = envelope.execution?.mode
  if (mode !== undefined && !EXECUTION_MODES.includes(mode as ExecutionMode)) {
    throw new KipError(
      'InvalidRequestEnvelope',
      `execution.mode is one of ${EXECUTION_MODES.join(', ')}, not ` +
        JSON.stringify(mode),
    )
  }
  // §75.2 gives `on_error` two values. An unrecognized one has to be refused
  // rather than defaulted, because the default it would fall into is
  // `continue`: a sequence meant to stop would run its remaining operations
  // and commit writes the caller asked to have skipped.
  const onError = envelope.execution?.on_error
  if (onError !== undefined && !ON_ERRORS.includes(onError)) {
    throw new KipError(
      'InvalidRequestEnvelope',
      `execution.on_error is one of ${ON_ERRORS.join(', ')}, not ` +
        JSON.stringify(onError),
    )
  }
  // §75.4 and §32.1. Refused outright rather than run as a sequence: a batch
  // that committed operation by operation while the caller asked for
  // all-or-none is the one failure `atomic` exists to prevent. (The §75.3
  // rule that atomic cannot pair with `on_error: continue` needs no check
  // here, because no atomic request gets past this line.)
  if (mode === 'atomic') {
    throw new KipError(
      'UnsupportedIsolation',
      'this engine has no atomic batch: one transaction across several ' +
        'operations is not implemented, and running them as a sequence ' +
        'would look like one',
    )
  }

  // §32.2: a weaker guarantee must not silently satisfy a request for a
  // stronger one. A Durable Object serializes its own callers, so this
  // engine can honour `serializable` for one operation and nothing beyond it.
  const isolation = envelope.execution?.isolation
  if (isolation !== undefined && isolation !== 'serializable') {
    throw new KipError(
      'UnsupportedIsolation',
      `this engine runs each operation serializably inside one Durable ` +
        `Object and offers no other isolation; it will not accept ` +
        `${JSON.stringify(isolation)} by ignoring it`,
    )
  }

  // §5.5: the Space is never inferred. One object is one Space, so an
  // envelope naming a different one is refused rather than answered from
  // this one.
  const named = envelope.space?.id
  if (named !== undefined && named !== space.id) {
    throw new KipError(
      'NotFoundOrNotVisible',
      `this object holds ${space.id}; it does not resolve ` +
        `${JSON.stringify(named)}`,
    )
  }

  // §35.4.
  const preconditions = envelope.preconditions
  if (preconditions !== undefined) {
    const row = space.row()
    if (
      preconditions.space_seq !== undefined &&
      row.seq !== preconditions.space_seq
    ) {
      throw new KipError(
        'PreconditionFailed',
        `this request expects ${space.id} at sequence ` +
          `${preconditions.space_seq}, and it is at ${row.seq}`,
      )
    }
    if (
      preconditions.schema_environment_version !== undefined &&
      row.schema_environment_version !==
        preconditions.schema_environment_version
    ) {
      throw new KipError(
        'PreconditionFailed',
        `this request expects Schema Environment version ` +
          `${preconditions.schema_environment_version}, and ` +
          `${space.id} is on version ` +
          `${row.schema_environment_version}`,
      )
    }
  }

  // A `critical` extension is a precondition, not a hint (request schema,
  // `extensions`): this engine implements no request extensions, so one it
  // is told it must honor fails the request rather than being ignored.
  const critical = criticalExtensions(envelope)
  if (critical.length > 0) {
    throw new KipError(
      'UnsupportedCapability',
      `this request marks the extension(s) ${JSON.stringify(critical)} critical, ` +
        `and this engine implements no request extensions; a critical extension ` +
        `it cannot honor fails the request rather than being silently ignored`,
    )
  }

  // §67: a fail-fast capability check. Running a command that needed
  // ingestion and answering it from re-typed command text is a wrong answer
  // wearing a success status — and a requirement nobody recognized must not
  // pass, because the caller believes it ran.
  for (const [name, wanted] of Object.entries(envelope.requires ?? {})) {
    const have = capabilityState(name)
    if (have === undefined) {
      throw new KipError(
        'UnsupportedCapability',
        `this request requires the capability ${JSON.stringify(name)}, ` +
          `which this engine does not recognize; it will not report an ` +
          `unknown requirement as satisfied`,
      )
    }
    if (have !== wanted) {
      throw new KipError(
        'UnsupportedCapability',
        `this request requires the capability ${JSON.stringify(name)} to ` +
          `be ${JSON.stringify(wanted)}, and this engine reports ${have}; ` +
          `DESCRIBE CAPABILITIES lists what it does and does not implement`,
      )
    }
  }

  // §80.1/§80.2. A Durable Object runs a statement to completion; accepting
  // a deadline would promise a cancellation that never happens.
  if (envelope.options?.deadline_ms !== undefined) {
    throw new KipError(
      'UnsupportedCapability',
      'this engine does not enforce `options.deadline_ms`: a statement runs ' +
        'to completion inside the object, and §80.2 is explicit that a ' +
        'client timeout is not an abort',
    )
  }

  // §71.1. Checked here rather than at mint time, because the block decides
  // what every operation of the batch can cite: discovering it malformed
  // after the first statement committed would leave durable writes behind a
  // request that was never valid.
  if (envelope.ingest !== undefined) {
    checkIngest(envelope.ingest)
    // §71.1 mints each entry inside the request's transaction scope, and
    // makes ingestion transactional. A request whose operations are all
    // reads opens no such scope, so the Evidence would be minted nowhere
    // while the request still answered `succeeded` — and the caller would go
    // on believing the observation was recorded, which is the fidelity
    // failure §88.12 has ingestion exist to prevent.
    //
    // Only refused once every operation parsed. A command that does not
    // parse should still report its own syntax error rather than being
    // recast as an envelope fault.
    let allParsed = true
    let anyMutation = false
    for (const operation of operations) {
      try {
        if ('Kml' in parseKip(operation.command ?? '')) anyMutation = true
      } catch {
        allParsed = false
      }
    }
    if (allParsed && !anyMutation) {
      throw new KipError(
        'InvalidRequestEnvelope',
        'an `ingest` block mints Evidence inside the request\'s ' +
          'transaction, so the request must carry at least one KML ' +
          'operation; a read-only request would drop the observation ' +
          'while reporting success',
      )
    }
  }
}
