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
import { KipError, type KipErrorJSON } from './errors.js'
import type { Json, JsonMap } from './json.js'
import { parseKip } from './kip/parser.js'
import {
  mergeRequestContext,
  systemAuth,
  type AuthContext,
  type RequestContext,
} from './governance/index.js'
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

/** One operation's answer. */
export interface KipResult {
  result?: Json
  receipt?: Outcome
  error?: KipErrorJSON
}

/** The response envelope (Spec §85). */
export interface KipResponse {
  kip: string
  results: KipResult[]
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
  ): KipResult {
    try {
      const session = this.nexus.session(this.authenticate(context))
      const parsed = parseKip(command)
      if ('Kml' in parsed) {
        return { receipt: session.mutate(parsed.Kml, params) }
      }
      if ('Kql' in parsed) {
        return { result: session.find(parsed.Kql, params, read ?? {}) as Json }
      }
      return { result: session.describe(command, params) }
    } catch (err) {
      return { error: KipError.from(err).toJSON() }
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
    commands: readonly { command: string; parameters?: JsonMap }[],
    context?: RequestContext,
    read?: ReadOptions,
  ): KipResult[] {
    return commands.map((operation) =>
      this.executeKip(operation.command, operation.parameters ?? {}, context, read),
    )
  }

  override async fetch(request: Request): Promise<Response> {
    if (request.method !== 'POST') {
      return new Response('POST a KIP request', { status: 405 })
    }
    let body: unknown
    try {
      body = await request.json()
    } catch {
      return this.envelope(
        { error: new KipError('InvalidRequestEnvelope', 'the body is not JSON').toJSON() },
        400,
      )
    }
    return this.handle(body)
  }

  private handle(body: unknown): Response {
    const envelope = (body ?? {}) as {
      kip?: string
      operations?: { command?: string; parameters?: JsonMap }[]
      execution?: { mode?: string }
      context?: RequestContext
      read?: ReadOptions
    }
    if (envelope.execution?.mode === 'atomic') {
      return this.envelope(
        {
          error: new KipError(
            'UnsupportedIsolation',
            'this engine has no atomic batch: one transaction across several ' +
              'operations is not implemented, and running them as a sequence ' +
              'would look like one',
          ).toJSON(),
        },
        400,
      )
    }
    const operations = envelope.operations ?? []
    if (operations.length === 0) {
      return this.envelope(
        {
          error: new KipError(
            'InvalidRequestEnvelope',
            'a request needs at least one operation',
          ).toJSON(),
        },
        400,
      )
    }

    const results = this.executeKipBatch(
      operations.map((operation) => ({
        command: operation.command ?? '',
        parameters: operation.parameters,
      })),
      envelope.context,
      envelope.read,
    )
    return this.envelope({ results }, statusFor(results))
  }

  private envelope(
    partial: { results?: KipResult[]; error?: KipErrorJSON },
    status: number,
  ): Response {
    const response: KipResponse = {
      kip: '2.0',
      results: partial.results ?? [],
      ...(partial.error === undefined ? {} : { error: partial.error }),
    }
    return Response.json(response, { status })
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
  const failed = results.filter((result) => result.error !== undefined)
  if (failed.length === 0) return 200
  if (failed.length < results.length) return 207
  return statusForError(failed[0]?.error)
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
