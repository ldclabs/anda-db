/**
 * KIP error type.
 *
 * The taxonomy itself (codes, names, agent-facing hints) is generated from
 * `rs/anda_kip/src/error.rs` into `errors.generated.ts` — see
 * `scripts/codegen-errors.mjs`. Only behavior lives here.
 */

import {
  KIP_ERROR_HINTS,
  KIP_ERROR_NAMES,
  type KipErrorCode,
} from './errors.generated.js'

export {
  KIP_ERROR_CODES,
  KIP_ERROR_HINTS,
  KIP_ERROR_NAMES,
  type KipErrorCode,
} from './errors.generated.js'

/** The JSON envelope a KIP error takes on the wire. */
export interface KipErrorJSON {
  code: KipErrorCode
  name: string
  message: string
  hint: string
}

export class KipError extends Error {
  readonly code: KipErrorCode
  readonly hint: string

  constructor(code: KipErrorCode, message: string, hint?: string) {
    super(message)
    this.name = KIP_ERROR_NAMES[code]
    this.code = code
    this.hint = hint ?? KIP_ERROR_HINTS[code]
  }

  toJSON(): KipErrorJSON {
    return {
      code: this.code,
      name: this.name,
      message: this.message,
      hint: this.hint,
    }
  }

  /** Rebuilds a `KipError` from the envelope the WASM parser returns. */
  static fromJSON(json: KipErrorJSON): KipError {
    return new KipError(json.code, json.message, json.hint)
  }

  /**
   * Normalizes an arbitrary thrown value into a `KipError`.
   *
   * Everything leaving the engine must carry a KIP code: a bare
   * `SQLITE_CONSTRAINT` string reaching the agent is unactionable, and the
   * agent's recovery path keys off `code`. Recognizable SQLite and Durable
   * Object failures are mapped to their KIP equivalents; anything else
   * becomes InternalError with the original message preserved.
   */
  static from(err: unknown): KipError {
    if (err instanceof KipError) return err

    const message = err instanceof Error ? err.message : String(err)

    // The unique indexes on `concepts(type, name)` and
    // `propositions(subject, object)` are the SQL expression of KIP's identity
    // rules, so violating one means "this already exists", not "engine bug".
    if (/UNIQUE constraint failed/i.test(message)) {
      return new KipError('KIP_3003', message)
    }
    // Durable Object storage ceilings (2 MB row, 10 GB database) surface as
    // plain SQLITE_TOOBIG / "too large" strings. They are resource conditions
    // the caller can act on by writing less, not internal failures.
    if (/SQLITE_TOOBIG|string or blob too big|too large/i.test(message)) {
      return new KipError('KIP_4002', message)
    }
    return new KipError('KIP_4003', message)
  }
}

export const invalidSyntax = (m: string) => new KipError('KIP_1001', m)
export const invalidIdentifier = (m: string) => new KipError('KIP_1002', m)
export const typeMismatch = (m: string) => new KipError('KIP_2001', m)
export const constraintViolation = (m: string) => new KipError('KIP_2002', m)
export const invalidValueType = (m: string) => new KipError('KIP_2003', m)
export const referenceError = (m: string) => new KipError('KIP_3001', m)
export const notFound = (m: string) => new KipError('KIP_3002', m)
export const duplicateExists = (m: string) => new KipError('KIP_3003', m)
export const immutableTarget = (m: string) => new KipError('KIP_3004', m)
export const versionConflict = (m: string) => new KipError('KIP_3005', m)
export const executionTimeout = (m: string) => new KipError('KIP_4001', m)
export const resourceExhausted = (m: string) => new KipError('KIP_4002', m)
/**
 * Alias for `resourceExhausted`. The engine's traversal and join caps report
 * KIP_4002 — the Rust engine uses `KipError::resource_exhausted` for exactly
 * these — but "query too complex" is what the condition actually is at those
 * call sites, and naming it that way keeps the caps readable.
 */
export const queryTooComplex = resourceExhausted
export const internalError = (m: string) => new KipError('KIP_4003', m)
