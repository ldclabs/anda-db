/**
 * The Durable Object wrapper: one KIP database per object.
 *
 * `KipDatabase` is meant to be subclassed so an application can supply its
 * tokenizer binding and add its own RPC methods. The base class owns schema
 * setup, the JSON-RPC surface (`execute_kip`), and the re-index alarm.
 */

import { DurableObject } from 'cloudflare:workers'
import {
  BOOTSTRAP_VERSION,
  CognitiveNexus,
  type KipResponse,
} from './nexus.js'
import { KipError } from './errors.js'
import { parseKipBatch } from './kip/parser.js'
import { configureSql } from './schema.js'
import { AlinkTokenizer, SimpleTokenizer, type Tokenizer } from './tokenizer.js'

export interface KipDatabaseEnv {
  /**
   * Binding for `cf-tokenizer`. A Container binding, a service binding, or
   * anything else exposing `fetch`. When absent the database falls back to
   * `SimpleTokenizer`, which is adequate for basic ASCII-oriented corpora but
   * has no dictionary-based Chinese segmentation — so multilingual production
   * deployments should bind the service.
   */
  TOKENIZER?: { fetch(input: RequestInfo, init?: RequestInit): Promise<Response> }
}

/** How often the re-index alarm runs while work remains. */
const REINDEX_INTERVAL_MS = 30_000

/** Durable marker written only after schema and capsule bootstrap succeeds. */
export const BOOTSTRAP_VERSION_KEY = '__kip_bootstrap_version'

export class KipDatabase<
  Env extends KipDatabaseEnv = KipDatabaseEnv,
> extends DurableObject<Env> {
  protected readonly nexus: CognitiveNexus
  #tokenizer?: Tokenizer

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)

    this.nexus = new CognitiveNexus(
      ctx.storage.sql,
      // `transactionSync` gives the engine real atomicity: a KML statement
      // either commits whole or rolls back. This is the capability the Rust
      // engine lacks, and it is why the port needs no preflight pass.
      (fn) => ctx.storage.transactionSync(fn),
      // Resolved on first use, not here. `createTokenizer` is meant to be
      // overridden, and a subclass's own field initializers do not run until
      // after this constructor returns — calling it now would hand the
      // override a half-built instance.
      { tokenizer: { tokenize: (texts) => this.tokenizer().tokenize(texts) } },
    )

    // Connection-local settings must be applied before the first storage
    // access: `PRAGMA foreign_keys` is a no-op once a transaction has begun.
    configureSql(ctx.storage.sql)

    const currentVersion = ctx.storage.kv.get<string>(
      BOOTSTRAP_VERSION_KEY,
    )
    if (currentVersion === BOOTSTRAP_VERSION) {
      return
    }

    // Schema and base capsules must be in place before any request is served.
    // A Durable Object may be constructed again after every eviction, so the
    // durable version marker avoids replaying all DDL and capsule checks on
    // each wake-up. It is written last: an interrupted bootstrap leaves the
    // old marker in place and is therefore retried safely next time.
    CognitiveNexus.bootstrap(ctx.storage.sql)
    this.nexus.applyBundledCapsules()
    ctx.storage.kv.put(BOOTSTRAP_VERSION_KEY, BOOTSTRAP_VERSION)
  }

  /**
   * Override to supply a different segmentation authority.
   *
   * Called lazily on the first tokenization, so an override may safely read
   * the subclass's own fields.
   */
  protected createTokenizer(env: Env): Tokenizer {
    return env.TOKENIZER
      ? new AlinkTokenizer(env.TOKENIZER)
      : new SimpleTokenizer()
  }

  /** Memoized `createTokenizer(env)`. */
  private tokenizer(): Tokenizer {
    return (this.#tokenizer ??= this.createTokenizer(this.env))
  }

  /**
   * Executes one KIP command.
   *
   * Exposed as an RPC method so callers can use the typed stub directly
   * rather than constructing HTTP requests.
   */
  async executeKip(command: string): Promise<KipResponse> {
    this.scheduleReindex()
    return this.nexus.execute(command)
  }

  /**
   * Executes several commands, stopping at the first failure.
   *
   * Statements are *not* wrapped in one transaction: each is individually
   * atomic, and a KIP request carrying multiple statements does not promise
   * all-or-nothing across them. The response reports how many applied so a
   * caller can resume rather than guess.
   */
  async executeKipBatch(commands: string[]): Promise<KipResponse[]> {
    this.scheduleReindex()
    const parsed = parseKipBatch(commands)
    const out: KipResponse[] = []
    for (const entry of parsed) {
      if ('error' in entry) {
        out.push({ error: entry.error.toJSON() })
        break
      }
      const response = await this.nexus.run(entry.ok)
      out.push(response)
      if ('error' in response) break
    }
    return out
  }

  /** JSON-RPC surface, matching `anda_cognitive_nexus_server`. */
  override async fetch(request: Request): Promise<Response> {
    if (request.method !== 'POST') {
      return json({ error: 'method not allowed' }, 405)
    }

    let body: { method?: string; params?: unknown; id?: unknown }
    try {
      body = (await request.json()) as typeof body
    } catch {
      return json({ error: 'invalid JSON body' }, 400)
    }

    if (body.method !== 'execute_kip') {
      return json({ error: `unknown method ${String(body.method)}` }, 400)
    }

    const params = (body.params ?? {}) as {
      command?: string
      commands?: string[]
    }

    try {
      if (Array.isArray(params.commands)) {
        const result = await this.executeKipBatch(params.commands)
        return json({ id: body.id ?? null, result })
      }
      if (typeof params.command === 'string') {
        const result = await this.executeKip(params.command)
        return json({ id: body.id ?? null, result })
      }
      return json({ error: 'params must carry `command` or `commands`' }, 400)
    } catch (err) {
      // The engine converts its own failures into KIP envelopes, so reaching
      // here means something outside it broke. Still answer in the KIP shape
      // rather than leaking a stack trace.
      return json({ id: body.id ?? null, error: KipError.from(err).toJSON() }, 500)
    }
  }

  /**
   * Rebuilds search index rows whose tokenizer version is stale.
   *
   * Runs on an alarm rather than inline: tokenization is a network round trip
   * and this object is single-threaded, so doing it on the write path would
   * put every writer behind the tokenizer's latency.
   */
  override async alarm(): Promise<void> {
    try {
      const version = await this.nexus.liveTokenizerVersion()
      const done = await this.nexus.reindexStale(version)
      // Reschedule while work remains; stop cleanly when the index is current
      // so an idle object can hibernate.
      if (done > 0) {
        await this.ctx.storage.setAlarm(Date.now() + REINDEX_INTERVAL_MS)
      }
    } catch {
      // A tokenizer outage must not end the retry loop: workerd retries a
      // throwing alarm only a bounded number of times, and on a quiet object
      // nothing else would ever re-arm it — stale rows would stay
      // unsearchable until the next write. Re-arm explicitly and let the
      // next run resume where this one stopped.
      await this.ctx.storage.setAlarm(Date.now() + REINDEX_INTERVAL_MS)
    }
  }

  private scheduleReindex(): void {
    // Fire-and-forget: an alarm that fails to schedule delays re-indexing but
    // must never fail the write that triggered it.
    void this.ctx.storage.getAlarm().then((existing) => {
      if (existing === null) {
        return this.ctx.storage.setAlarm(Date.now() + REINDEX_INTERVAL_MS)
      }
      return undefined
    }).catch(() => undefined)
  }
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  })
}
