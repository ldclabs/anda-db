/**
 * The KIP 2.0 error type.
 *
 * The registry itself — codes, categories, retry classes and agent-facing
 * hints — is generated from `rs/anda_kip/src/error.rs` into
 * `errors.generated.ts`; see `scripts/codegen-errors.mjs`. Only behaviour
 * lives here.
 *
 * Two members of the wire envelope (§86.1) are load-bearing and easy to treat
 * as decoration:
 *
 * - `hint` is the spec's recovery instruction, which is what makes a KIP error
 *   self-correcting for an Agent rather than something to log.
 * - `retry.class` is what a client's recovery policy switches on.
 *   `outcome_lookup_required` in particular must never be flattened into "it
 *   failed": the write may well have landed, and re-sending it duplicates.
 */

import {
  KIP_ERROR_CODES,
  KIP_ERROR_REGISTRY,
  type KipErrorCategory,
  type KipErrorCode,
  type KipRetryClass,
} from './errors.generated.js'
import type { Json } from './json.js'

export {
  KIP_ERROR_CODES,
  KIP_ERROR_REGISTRY,
  type KipErrorCategory,
  type KipErrorCode,
  type KipErrorSpec,
  type KipRetryClass,
} from './errors.generated.js'

/** The `retry` member of an error object. */
export interface RetryInfo {
  class: KipRetryClass
  after_ms?: number
}

/** The JSON envelope a KIP error takes on the wire (Spec §86.1). */
export interface KipErrorJSON {
  code: KipErrorCode
  category: KipErrorCategory
  message: string
  hint: string
  retry: RetryInfo
  details?: Json
}

export class KipError extends Error {
  readonly code: KipErrorCode
  readonly category: KipErrorCategory
  readonly hint: string
  readonly retry: RetryInfo
  readonly details?: Json

  constructor(code: KipErrorCode, message: string, details?: Json) {
    super(message)
    const spec = KIP_ERROR_REGISTRY[code]
    // `Error.name` is the code: KIP 2.0 replaced the numeric codes of 1.x with
    // stable names, so there is no second spelling to keep in step.
    this.name = code
    this.code = code
    this.category = spec.category
    this.hint = spec.hint
    this.retry = { class: spec.retry }
    if (details !== undefined) this.details = details
  }

  /** Suggests how long the caller should wait before retrying. */
  withRetryAfter(ms: number): this {
    this.retry.after_ms = ms
    return this
  }

  toJSON(): KipErrorJSON {
    const json: KipErrorJSON = {
      code: this.code,
      category: this.category,
      message: this.message,
      hint: this.hint,
      retry: this.retry,
    }
    if (this.details !== undefined) json.details = this.details
    return json
  }

  /**
   * Normalizes an arbitrary thrown value into a `KipError`.
   *
   * Everything leaving the engine must carry a registry code: a bare
   * `SQLITE_CONSTRAINT` string reaching the Agent is unactionable, because the
   * recovery path keys off `code` and `retry`. Recognizable SQLite and Durable
   * Object failures are mapped to their KIP equivalents; anything else becomes
   * `InternalError` with the original message preserved.
   */
  static from(err: unknown): KipError {
    if (err instanceof KipError) return err

    const message = err instanceof Error ? err.message : String(err)

    // The unique indexes on `propositions(space, tuple_key)` and
    // `concepts(space, schema_ref, key)` are the SQL expression of KIP's
    // identity rules, so violating one means "something already claims this
    // identity", not "engine bug".
    if (/UNIQUE constraint failed/i.test(message)) {
      return new KipError('IdentityConflict', message)
    }
    // Durable Object storage ceilings (2 MB row, 10 GB database) surface as
    // plain SQLITE_TOOBIG / "too large" strings. They are resource conditions
    // the caller can act on by writing less, not internal failures.
    if (/SQLITE_TOOBIG|string or blob too big|too large/i.test(message)) {
      return new KipError('ResourceExhausted', message)
    }
    return new KipError('InternalError', message)
  }
}

/**
 * One constructor per registry code, named in camelCase.
 *
 * Built by a loop rather than written out, so a code added to `anda_kip`
 * arrives with its constructor already present and correctly classified. The
 * mapped type is what makes `errors.schemaSymbolNotFound(...)` a compile-time
 * name rather than a string lookup that typos silently.
 */
export type ErrorFactories = {
  [C in KipErrorCode as Uncapitalize<C>]: (
    message: string,
    details?: Json,
  ) => KipError
}

export const errors: ErrorFactories = Object.fromEntries(
  KIP_ERROR_CODES.map((code) => [
    code.charAt(0).toLowerCase() + code.slice(1),
    (message: string, details?: Json) => new KipError(code, message, details),
  ]),
) as ErrorFactories
