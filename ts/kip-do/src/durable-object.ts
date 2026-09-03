/**
 * The Durable Object a host deploys.
 *
 * One Cognitive Nexus per object. The class is thin on purpose: it owns the
 * request envelope and nothing else, because every decision worth making is
 * made below it.
 *
 * The one decision it does own is which HTTP status a failure gets, and it is
 * not cosmetic. A KIP error carries a retry class, and mapping the wrong status
 * onto it tells a client's recovery policy to do the wrong thing — most
 * expensively when a lost response is reported as a clean failure and the
 * client writes again.
 */

import { DurableObject } from 'cloudflare:workers'
import { DIGEST_PROFILE } from './capsule/index.js'
import { sha3_256Text } from './digest.js'
import { KipError, errors, type KipErrorJSON } from './errors.js'
import { canonicalJson, type Json, type JsonMap } from './json.js'
import { parseKip } from './kip/parser.js'
import { checkIngest, type IngestContext } from './kml/index.js'
import {
  mergeRequestContext,
  systemAuth,
  type AuthContext,
  type RequestContext,
} from './governance/index.js'
import { capabilityState, KIP_VERSION } from './meta/capabilities.js'
import { CognitiveNexus, type NexusOptions, type ReadOptions } from './nexus.js'
import {
  BUNDLED_PACKAGES,
  COGNITIVE_MEMORY,
  type SchemaPackage,
} from './schema/index.js'
import type { Outcome } from './tx.js'

/** The bindings a host gives the object. */
export interface KipDatabaseEnv {
  [key: string]: unknown
}

/**
 * The coordinates one read answered at (Spec §50).
 *
 * Reported rather than implied: an answer that cannot say which coordinate it
 * read is an answer a caller cannot reproduce, and it is the same coordinate a
 * page cursor pins.
 */
export interface KipResultContext {
  /** The Space coordinate the read was pinned to. */
  snapshot_seq: number
  /** The world-time basis, when `FOR TIME` named one. */
  valid_at?: string
}

/** One operation's outcome (Spec §83). */
export type KipOperationStatus =
  | 'succeeded'
  | 'failed'
  | 'skipped'
  | 'no_effect'

/**
 * One operation's answer (Spec §81, `$defs/OperationResult`).
 *
 * `status` is required and is not derivable from the other fields: a mutation
 * that changed nothing answers `no_effect` with no error and no result, and a
 * consumer reading "no error" as "committed" would count a write that never
 * happened.
 *
 * §75: in `independent` and `sequence` modes every state-changing operation
 * returns its own Receipt in `receipt`; the top-level `receipt` exists only in
 * `atomic` mode, which this engine does not have. The rest of the mutation
 * outcome — bound handles, per-element changes, the governance decision —
 * rides in `extensions`, because the wire Receipt is closed to the members
 * §33.2 names.
 */
export interface KipResult {
  /** The request-local id of the operation this answers (§73). */
  op_id?: string
  status: KipOperationStatus
  result?: Json
  context?: KipResultContext
  /**
   * The cursor for the next page, when one remains (§44.8).
   *
   * A KQL cursor is the opaque token this engine issues, so a caller has no
   * other way to obtain one — a paged `FIND` whose continuation token never
   * left the object could not be continued at all.
   */
  next_cursor?: string
  warnings?: string[]
  /** This operation's own transaction Receipt (§33.2, §75). */
  receipt?: KipReceipt
  extensions?: {
    'kip-do/outcome'?: Outcome
    /**
     * §63.5: a `LIST DEPENDENTS` walk was cut short by an element the caller
     * may not discover. The rows stay a bare array, so the flag rides here.
     */
    'kip-do/dependents'?: { truncated: true }
  }
  error?: KipErrorJSON
}

/**
 * The Principal a commit was attributed to, the ActorBinding it exercised
 * and the delegation chain it acted under (§33.2), so an auditor can tie the
 * Receipt to the Governance decision without reading the audit log.
 */
export interface KipReceiptOrigin {
  principal_id: string
  actor_binding_id: string | null
  delegation_digest: string | null
}

/** A transaction outcome in the shape §33.2 fixes for the wire. */
export interface KipReceipt {
  status: 'committed' | 'no_effect'
  tx_id: string
  space_id: string
  snapshot_seq: number
  /** Absent on `no_effect`: no sequence was taken. */
  space_seq?: number
  committed_at?: string
  transaction_class: string
  schema_environment_version: number
  /**
   * The canonical digest of the Receipt without `receipt_digest` and
   * `proofs` (§33.2); a signed Receipt would sign it.
   */
  receipt_digest: string
  origin: KipReceiptOrigin
}

/** The response envelope (Spec §81). */
export interface KipResponse {
  kip: string
  /** Echoed so a client can pair an answer with the attempt it made (§72.1). */
  request_id?: string
  /** §82. Derived from the operations, never from the transport status. */
  status: 'succeeded' | 'failed' | 'partial'
  /**
   * The mode and error policy that were used, and the request's idempotency
   * key echoed back so an `outcome_unknown` response can be recovered by key
   * (§80.4, §81).
   */
  execution?: { mode: string; on_error?: string; idempotency_key?: string }
  results: KipResult[]
  context?: { space_id: string }
  /**
   * The transaction's Receipt in `atomic` mode only (§75.3). This engine has
   * no atomic batch, so the member is never present; every operation's own
   * Receipt is on that operation.
   */
  receipt?: KipReceipt
  error?: KipErrorJSON
}

/**
 * A KIP 2.0 Cognitive Nexus in one Durable Object.
 *
 * Subclass it and bind the subclass; the base class does not register itself.
 *
 * ```ts
 * export class MyKipDatabase extends KipDatabase<Env> {}
 * ```
 */
export class KipDatabase<Env = KipDatabaseEnv> extends DurableObject<Env> {
  protected readonly nexus: CognitiveNexus

  constructor(ctx: DurableObjectState, env: Env, options: NexusOptions = {}) {
    super(ctx, env)
    this.nexus = CognitiveNexus.connect(ctx.storage, options)
    // A Space that has activated nothing resolves Core and nothing else, and
    // Core declares no Concept types at all — so an object that skipped this
    // would refuse every `CREATE CONCEPT` with a message about schema rather
    // than about what the caller did. Activating the bundled profile is the
    // default a host can override by subclassing.
    this.nexus.activatePackages(this.packages())
  }

  /** The Schema Packages this object activates on construction. */
  protected packages(): readonly SchemaPackage[] {
    return BUNDLED_PACKAGES.length > 0 ? BUNDLED_PACKAGES : [COGNITIVE_MEMORY]
  }

  /**
   * The identity one request runs as.
   *
   * The default is the engine itself, which owns the default Space — the
   * embedded case, where the object *is* the owner. A multi-tenant host
   * overrides this: it authenticates the caller from what it observed about the
   * connection and returns that Principal, and every command then gets exactly
   * what the caller's Grants say.
   *
   * `context` is the envelope's non-authoritative block, and it is passed for
   * one reason: a caller may *narrow* its session with a declared purpose and can
   * never widen it (§12). Identity, authentication strength and delegation are
   * the host's to decide — an override that read `principal_id` off the request
   * body would make the whole plane decorative, because a request body is
   * exactly what an Agent under prompt injection controls.
   */
  protected authenticate(context: RequestContext | undefined): AuthContext {
    return mergeRequestContext(systemAuth(), context)
  }

  /**
   * Runs one command, whichever language it is.
   *
   * The parsed semantics decide, never the caller's framing: a request that
   * calls its command a query and sends a mutation runs as the mutation it is,
   * or not at all.
   */
  executeKip(
    command: string,
    params: JsonMap = {},
    context?: RequestContext,
    read?: ReadOptions,
    ingest?: IngestContext,
    idempotencyKey?: string,
    readonly = false,
  ): KipResult {
    try {
      const auth = this.authenticate(context)
      const session = this.nexus.session(auth)
      const parsed = parseKip(command)
      if (readonly && 'Kml' in parsed) {
        // §76: the read-only path rejects state-changing *semantics*, not a
        // declared label — so no envelope field can talk a write past it.
        throw errors.readonlyViolation(
          'this endpoint executes KQL and META only; KML mutations must go ' +
            'through the state-capable runtime',
        )
      }
      if ('Kml' in parsed) {
        const outcome = session.mutate(parsed.Kml, params, {
          ingest,
          idempotencyKey,
        })
        return {
          // §32.8: a transaction whose durable state is unchanged reports
          // `no_effect`, and it is a different answer from `succeeded` — it
          // took no Space sequence and appended no Commit Record.
          status: outcome.status === 'no_effect' ? 'no_effect' : 'succeeded',
          ...(outcome.warnings.length === 0
            ? {}
            : { warnings: outcome.warnings }),
          // §75: the operation's own Receipt, in the shape §33.2 fixes.
          receipt: receiptOf(outcome, auth),
          extensions: { 'kip-do/outcome': outcome },
        }
      }
      if ('Kql' in parsed) {
        const answer = session.findPage(parsed.Kql, params, read ?? {})
        return {
          status: 'succeeded',
          result: answer.rows as Json,
          context: {
            snapshot_seq: answer.snapshotSeq,
            ...(answer.validAt === null ? {} : { valid_at: answer.validAt }),
          },
          ...(answer.nextCursor === null
            ? {}
            : { next_cursor: answer.nextCursor }),
        }
      }
      const answer = session.describePage(command, params)
      return {
        status: 'succeeded',
        result: answer.result,
        ...(answer.nextCursor === null
          ? {}
          : { next_cursor: answer.nextCursor }),
        ...(answer.truncated
          ? {
              warnings: [
                'the traversal was cut short by an element this caller may not ' +
                  'discover (§63.5); the closure reported is incomplete',
              ],
              extensions: { 'kip-do/dependents': { truncated: true as const } },
            }
          : {}),
      }
    } catch (err) {
      return { status: 'failed', error: KipError.from(err).toJSON() }
    }
  }

  /**
   * Runs a batch, operation by operation.
   *
   * Each operation is atomic on its own; the batch is not. `execution.mode:
   * "atomic"` would need one transaction across all of them, which this engine
   * does not have — so a request asking for it is refused rather than run as a
   * sequence that looks like one.
   *
   * The envelope's context is authenticated once and applies to every operation:
   * a batch is one request, and letting operation two run as a different
   * Principal from operation one would make the identity per-command state that
   * a caller could vary.
   */
  executeKipBatch(
    commands: readonly {
      op_id?: string
      command: string
      parameters?: JsonMap
      idempotencyKey?: string
    }[],
    context?: RequestContext,
    read?: ReadOptions,
    execution: { mode: ExecutionMode; onError: OnError } = {
      mode: 'independent',
      onError: 'stop',
    },
    ingest?: IngestContext,
    readonly = false,
  ): KipResult[] {
    const results: KipResult[] = []
    let stopped = false
    for (const operation of commands) {
      if (stopped) {
        // §75.2: under `sequence` with `on_error: stop`, what follows a failure
        // did not run. `skipped` says that; a missing entry would leave the
        // caller matching answers to operations by position and guessing.
        results.push({
          ...(operation.op_id === undefined ? {} : { op_id: operation.op_id }),
          status: 'skipped',
        })
        continue
      }
      const result = this.executeKip(
        operation.command,
        operation.parameters ?? {},
        context,
        read,
        ingest,
        operation.idempotencyKey,
        readonly,
      )
      if (operation.op_id !== undefined) result.op_id = operation.op_id
      // `independent` isolates failures by definition; only `sequence` with
      // `on_error: stop` short-circuits. Earlier commits stay durable either
      // way — this engine commits per operation.
      if (
        result.status === 'failed' &&
        execution.mode === 'sequence' &&
        execution.onError === 'stop'
      ) {
        stopped = true
      }
      results.push(result)
    }
    return results
  }

  override async fetch(request: Request): Promise<Response> {
    if (request.method !== 'POST') {
      return new Response('POST a KIP request', { status: 405 })
    }
    // §76: a dedicated read-only path, so a caller can hand a query to an
    // endpoint that *cannot* write rather than trusting that it will not.
    const readonly = new URL(request.url).pathname.endsWith('/readonly')
    let body: unknown
    try {
      body = await request.json()
    } catch {
      return this.envelope(
        { error: new KipError('InvalidRequestEnvelope', 'the body is not JSON').toJSON() },
        400,
      )
    }
    return this.handle(body, readonly)
  }

  private handle(body: unknown, readonly = false): Response {
    const envelope = (body ?? {}) as KipRequestEnvelope
    const requestId = envelope.request_id
    try {
      this.checkEnvelope(envelope)
    } catch (err) {
      const error = KipError.from(err)
      return this.envelope(
        { request_id: requestId, error: error.toJSON() },
        statusForError(error.toJSON()),
      )
    }

    const operations = envelope.operations ?? []
    const mode = (envelope.execution?.mode ?? 'independent') as ExecutionMode
    // §75.2: `stop` when absent. A sequence that defaulted to `continue`
    // would commit the writes the caller asked to have skipped.
    const onError = (envelope.execution?.on_error ?? 'stop') as OnError
    const idempotencyKey = envelope.execution?.idempotency_key
    const results = this.executeKipBatch(
      operations.map((operation, index) => ({
        ...(operation.op_id === undefined ? {} : { op_id: operation.op_id }),
        command: operation.command ?? '',
        // §74: a top-level `parameters` block is the request's own binding
        // environment; an operation's own block narrows it. Merged here rather
        // than in the engine, because binding is an envelope concern and the
        // engine only ever sees one map.
        parameters: { ...(envelope.parameters ?? {}), ...(operation.parameters ?? {}) },
        ...operationIdempotency(operation, envelope, index),
      })),
      envelope.context,
      envelope.read,
      { mode, onError },
      envelope.ingest,
      readonly,
    )
    return this.envelope(
      {
        request_id: requestId,
        results,
        execution: {
          mode,
          ...(mode === 'sequence' ? { on_error: onError } : {}),
          // §81: echoed, so an `outcome_unknown` response can be recovered
          // by the key the caller chose (§80.4).
          ...(idempotencyKey === undefined ? {} : { idempotency_key: idempotencyKey }),
        },
        context: { space_id: this.nexus.space },
      },
      statusFor(results),
    )
  }

  /**
   * Every envelope invariant this engine can check before anything runs.
   *
   * Fail-fast and whole-request: each of these decides what the operations
   * *mean*, so running half a batch and then discovering the envelope was
   * malformed would leave durable writes behind a request that was never
   * valid.
   */
  private checkEnvelope(envelope: KipRequestEnvelope): void {
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
    if (named !== undefined && named !== this.nexus.space) {
      throw new KipError(
        'NotFoundOrNotVisible',
        `this object holds ${this.nexus.space}; it does not resolve ` +
          `${JSON.stringify(named)}`,
      )
    }

    // §35.4.
    const preconditions = envelope.preconditions
    if (preconditions !== undefined) {
      const row = this.nexus.spaceRow()
      if (
        preconditions.space_seq !== undefined &&
        row.seq !== preconditions.space_seq
      ) {
        throw new KipError(
          'PreconditionFailed',
          `this request expects ${this.nexus.space} at sequence ` +
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
            `${this.nexus.space} is on version ` +
            `${row.schema_environment_version}`,
        )
      }
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

  private envelope(
    partial: {
      request_id?: string
      results?: KipResult[]
      execution?: { mode: string; on_error?: string; idempotency_key?: string }
      context?: { space_id: string }
      error?: KipErrorJSON
    },
    status: number,
  ): Response {
    const results = partial.results ?? []
    const response: KipResponse = {
      kip: KIP_VERSION,
      ...(partial.request_id === undefined
        ? {}
        : { request_id: partial.request_id }),
      // §82, derived from what the operations did. `partial` is the honest
      // answer for a batch that half committed: reporting it as `failed`
      // invites the client to re-send writes that landed.
      status:
        partial.error !== undefined
          ? 'failed'
          : topLevelStatus(results),
      ...(partial.execution === undefined
        ? {}
        : { execution: partial.execution }),
      results,
      ...(partial.context === undefined ? {} : { context: partial.context }),
      ...(partial.error === undefined ? {} : { error: partial.error }),
    }
    return Response.json(response, { status })
  }
}

/** The execution modes §75 names. */
const EXECUTION_MODES = ['independent', 'sequence', 'atomic'] as const
type ExecutionMode = (typeof EXECUTION_MODES)[number]

/** The `on_error` values §75.2 names. */
const ON_ERRORS = ['stop', 'continue'] as const
type OnError = (typeof ON_ERRORS)[number]

/** The request envelope this object reads (Spec §71). */
interface KipRequestEnvelope {
  kip?: string
  request_id?: string
  space?: { id?: string }
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
}

/**
 * The idempotency key one operation of a batch commits under (§34.2, §73).
 *
 * An operation's own key wins: §73 puts `idempotency_key` on the operation
 * precisely so a caller can name one logical mutation intent per write. The
 * envelope's key is one intent *for the request*, and this engine has no
 * atomic batch — each operation is its own transaction — so handing the same
 * key to three writes would make the second and third replay the first's
 * outcome and commit nothing. §34.2 requires the key to be scoped at least by
 * operation endpoint, so the envelope's key is narrowed by the operation's
 * position: an identical retry reproduces the same positions and still
 * deduplicates, while three different writes in one request stay three.
 *
 * The position rather than `op_id`, which is optional: a key that existed only
 * for the operations that named one would be retry-safe by accident.
 */
function operationIdempotency(
  operation: { idempotency_key?: string },
  envelope: KipRequestEnvelope,
  index: number,
): { idempotencyKey?: string } {
  if (operation.idempotency_key !== undefined) {
    return { idempotencyKey: operation.idempotency_key }
  }
  const requestKey = envelope.execution?.idempotency_key
  if (requestKey === undefined) return {}
  const operations = envelope.operations ?? []
  return {
    idempotencyKey: operations.length === 1 ? requestKey : `${requestKey}#${index}`,
  }
}

/**
 * §82, from the operations rather than from the transport.
 *
 * `partial` needs an operation that actually succeeded, not merely one that did
 * not fail: a `sequence` that fails on its first operation answers `['failed',
 * 'skipped', …]`, and calling that partial would tell the client some of its
 * writes landed when none did — the one reading that stops it retrying.
 */
function topLevelStatus(
  results: readonly KipResult[],
): 'succeeded' | 'failed' | 'partial' {
  const failed = results.some((r) => r.status === 'failed')
  if (!failed) return 'succeeded'
  const succeeded = results.some(
    (r) => r.status === 'succeeded' || r.status === 'no_effect',
  )
  return succeeded ? 'partial' : 'failed'
}

/**
 * One transaction outcome in the shape §33.2 fixes for the wire.
 *
 * A projection rather than the raw outcome: the wire Receipt is closed to the
 * members §33.2 names, and this engine's outcome also carries bound handles
 * and per-element changes — which stay in the operation's `extensions`.
 *
 * `space_seq` and `committed_at` are absent on `no_effect`: no sequence was
 * taken and nothing was committed, and a Receipt that carried a coordinate
 * would claim a commit that did not happen. `receipt_digest` is the engine's
 * canonical-JSON digest over the Receipt without itself and without
 * `proofs`, so a signed Receipt (§33.3) would sign exactly what a reader can
 * recompute. sha3-256, the algorithm §37.7's profile is registered under in
 * this project and the one a Capsule digest already carries: a Receipt and a
 * Capsule from the same Space must not be verifiable under two different
 * hashes, and the other reference engine seals under the same one. `origin` is read off the authorization context the statement
 * ran under: the Principal, the ActorBinding it spoke through if any, and a
 * digest of the delegation chain when the request named one.
 */
export function receiptOf(outcome: Outcome, auth: AuthContext): KipReceipt {
  const unsigned: Omit<KipReceipt, 'receipt_digest'> = {
    status: outcome.status,
    tx_id: outcome.tx_id,
    space_id: outcome.space_id,
    snapshot_seq: outcome.snapshot_seq,
    ...(outcome.space_seq === null ? {} : { space_seq: outcome.space_seq }),
    ...(outcome.committed_at === null ? {} : { committed_at: outcome.committed_at }),
    transaction_class: 'cognitive',
    ...(outcome.request_digest === ''
      ? {}
      : { request_digest: outcome.request_digest }),
    schema_environment_version: outcome.schema_environment_version,
    origin: {
      principal_id: auth.principal_id,
      actor_binding_id: outcome.actor_binding_id,
      delegation_digest:
        auth.delegation_chain.length === 0
          ? null
          : `${DIGEST_PROFILE}:${sha3_256Text(canonicalJson(auth.delegation_chain))}`,
    },
  }
  return {
    ...unsigned,
    receipt_digest: `${DIGEST_PROFILE}:${sha3_256Text(canonicalJson(unsigned))}`,
  }
}

/**
 * The status a batch's outcome gets.
 *
 * **Partial success is 207, not 500.** Earlier operations in a batch have
 * already committed and are durable; reporting the whole request as a failure
 * invites the client to re-send writes that landed.
 */
function statusFor(results: readonly KipResult[]): number {
  const failed = results.filter((result) => result.status === 'failed')
  if (failed.length === 0) return 200
  // 207 only where something actually committed — the same distinction
  // `topLevelStatus` draws, and for the same reason.
  return topLevelStatus(results) === 'partial'
    ? 207
    : statusForError(failed[0]?.error)
}

/**
 * The status one error gets, from its retry class rather than its name.
 *
 * The retry class is what a client's recovery policy switches on, so the status
 * has to agree with it — a `requires_authority` failure answered with 400 tells
 * the client to rewrite a request that was fine.
 */
function statusForError(error: KipErrorJSON | undefined): number {
  if (error === undefined) return 500
  switch (error.retry.class) {
    case 'requires_authority':
      return 403
    case 'requires_different_input':
      return 400
    case 'requires_refresh':
    case 'requires_new_snapshot':
      return 409
    case 'requires_reacquire_artifact':
      return 422
    // The write may well have landed. 500 reads as "nothing happened", and a
    // client acting on that writes again.
    case 'outcome_lookup_required':
      return 503
    case 'safe_same_request':
      return 503
    default:
      return error.category === 'governance'
        ? 403
        : error.category === 'system'
          ? 500
          : 400
  }
}
