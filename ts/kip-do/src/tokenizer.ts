/**
 * Tokenization for full-text search.
 *
 * Durable Object SQLite ships FTS5 with only the built-in tokenizers
 * (`ascii`, `unicode61`, `porter`, `trigram`) and a Worker cannot load a C
 * extension, so jieba is not available inside SQLite. Chinese text under
 * `unicode61` collapses a whole Han run into a single token and returns
 * nothing for realistic queries.
 *
 * The engine therefore treats an external service as the sole segmentation
 * authority — `cf-tokenizer`, which wraps jieba-rs behind
 * `POST /tokenize`. Both the write path (indexing) and the read path
 * (`SEARCH`) call the same service, which is what prevents the write/read
 * asymmetry that a "tokenize on write, approximate on read" design produces.
 *
 * The service stamps `X-Tokenizer-Version` on every response. That version is
 * persisted per row in `tok_ver`; when it no longer matches the live service,
 * the affected rows are stale and must be re-indexed, because token
 * vocabularies from different versions are not comparable.
 */

import { executionTimeout, internalError } from './errors.js'

/** Batch cap enforced by the service (`MAX_TEXTS_PER_BATCH`). */
export const MAX_TEXTS_PER_BATCH = 256

/** Result of tokenizing a batch. */
export interface TokenizeResult {
  /** One token list per input text, positionally aligned. */
  tokens: string[][]
  /** `X-Tokenizer-Version` of the service that produced them. */
  version: string
}

export interface Tokenizer {
  /**
   * Segments texts for indexing and querying.
   *
   * Implementations must be deterministic for a given version: the same text
   * must yield the same tokens on the write path and the read path.
   */
  tokenize(texts: string[]): Promise<TokenizeResult>
}

/**
 * A binding that can service a `fetch`.
 *
 * Typed structurally so the same client works against a Container binding, a
 * service binding, or a plain origin — the three ways `cf-tokenizer` is
 * reachable from a Worker.
 */
export interface FetcherLike {
  fetch(input: RequestInfo, init?: RequestInit): Promise<Response>
}

export interface AlinkTokenizerOptions {
  /**
   * Base URL of the service. When calling through a service or Container
   * binding the host is ignored by the runtime but a valid absolute URL is
   * still required; the default is fine in that case.
   */
  baseUrl?: string
  /** Per-request timeout. The service is pure CPU, so this should be short. */
  timeoutMs?: number
}

/**
 * Client for `cf-tokenizer`.
 *
 * The class keeps its historical name for API compatibility. Contract (see
 * `rs/cf-tokenizer/README.md`):
 *   `POST /tokenize  { texts: string[], mode: "search" } -> { tokens: string[][] }`
 *   every successful response carries `X-Tokenizer-Version`.
 */
export class AlinkTokenizer implements Tokenizer {
  readonly #fetcher: FetcherLike
  readonly #baseUrl: string
  readonly #timeoutMs: number

  constructor(fetcher: FetcherLike, options: AlinkTokenizerOptions = {}) {
    this.#fetcher = fetcher
    this.#baseUrl = (options.baseUrl ?? 'http://tokenizer').replace(/\/+$/, '')
    this.#timeoutMs = options.timeoutMs ?? 5_000
  }

  async tokenize(texts: string[]): Promise<TokenizeResult> {
    if (texts.length === 0) return { tokens: [], version: 'empty' }

    const tokens: string[][] = []
    let version: string | null = null

    // The service rejects oversized batches rather than truncating, so
    // chunking is the client's job.
    for (let i = 0; i < texts.length; i += MAX_TEXTS_PER_BATCH) {
      const chunk = texts.slice(i, i + MAX_TEXTS_PER_BATCH)
      const result = await this.#post(chunk)

      // A version change mid-write would stamp one `tok_ver` across rows
      // tokenized by two different vocabularies, which silently defeats the
      // staleness check that drives re-indexing.
      if (version !== null && result.version !== version) {
        throw internalError(
          `tokenizer version changed mid-batch (${version} -> ${result.version}); ` +
            `retry the write so every row is stamped with one version`,
        )
      }
      version = result.version
      tokens.push(...result.tokens)
    }

    return { tokens, version: version! }
  }

  async #post(texts: string[]): Promise<TokenizeResult> {
    // The deadline covers the *whole* exchange, body included: the signal is
    // cleared only after the payload is consumed. Clearing it when the
    // headers arrive would let a stalled body read hang forever — and every
    // KML write queues behind this call, so a hang wedges the object.
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), this.#timeoutMs)
    try {
      let response: Response
      try {
        response = await this.#fetcher.fetch(`${this.#baseUrl}/tokenize`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ texts, mode: 'search' }),
          signal: controller.signal,
        })
      } catch (err) {
        // Only the deadline is a timeout; DNS, refused connections and reset
        // streams are service failures and must not masquerade as one.
        if (controller.signal.aborted) {
          throw executionTimeout(
            `tokenizer request timed out after ${this.#timeoutMs}ms`,
          )
        }
        throw internalError(
          `tokenizer request failed: ${(err as Error).message}`,
        )
      }

      if (!response.ok) {
        const body = await response.text().catch(() => '')
        throw internalError(
          `tokenizer returned ${response.status}: ${body.slice(0, 200)}`,
        )
      }

      const version = response.headers.get('x-tokenizer-version')
      if (!version) {
        // Without a version there is no way to detect stale index rows later,
        // so an unversioned response is treated as a broken deployment rather
        // than silently accepted.
        throw internalError(
          'tokenizer response is missing the X-Tokenizer-Version header',
        )
      }

      let payload: { tokens?: string[][] }
      try {
        payload = (await response.json()) as typeof payload
      } catch (err) {
        if (controller.signal.aborted) {
          throw executionTimeout(
            `tokenizer response body timed out after ${this.#timeoutMs}ms`,
          )
        }
        throw internalError(
          `tokenizer response is not valid JSON: ${(err as Error).message}`,
        )
      }
      if (
        !Array.isArray(payload.tokens) ||
        payload.tokens.length !== texts.length
      ) {
        throw internalError(
          `tokenizer returned ${payload.tokens?.length ?? 0} token lists for ` +
            `${texts.length} texts`,
        )
      }
      return { tokens: payload.tokens, version }
    } finally {
      clearTimeout(timer)
    }
  }
}

/**
 * Whitespace/punctuation tokenizer used in tests and for ASCII-only
 * deployments that do not want the extra service hop.
 *
 * It is intentionally *not* a fallback for `AlinkTokenizer`: silently
 * degrading to this when the service is unreachable would write rows whose
 * tokens disagree with everything indexed before and after them. The engine
 * fails the write instead, and the caller decides.
 */
export class SimpleTokenizer implements Tokenizer {
  static readonly VERSION = 'simple-1'

  async tokenize(texts: string[]): Promise<TokenizeResult> {
    return {
      tokens: texts.map((text) => normalizeTokens(splitSimple(text))),
      version: SimpleTokenizer.VERSION,
    }
  }
}

function splitSimple(text: string): string[] {
  const lowered = text.normalize('NFKC').toLowerCase()
  const out: string[] = []
  // Latin/digit runs become words; each CJK codepoint becomes its own token,
  // which is the best a tokenizer without a dictionary can do. This is why it
  // is a test helper and not a production path.
  for (const match of lowered.matchAll(
    /[\p{Script=Han}]|[\p{Letter}\p{Number}]+/gu,
  )) {
    out.push(match[0])
  }
  return out
}

/** Order-preserving dedup plus the 256-token cap the service applies. */
function normalizeTokens(tokens: string[]): string[] {
  const seen = new Set<string>()
  const out: string[] = []
  for (const token of tokens) {
    if (seen.has(token)) continue
    seen.add(token)
    out.push(token)
    if (out.length >= 256) break
  }
  return out
}

/**
 * Collects the searchable text of a JSON value.
 *
 * Mirrors `extract_json_text` in `anda_db`: the BM25 corpus is built from
 * every string *and* every object key reachable inside `attributes` /
 * `metadata`, not just top-level values. Reproducing this shape matters — an
 * index built over a different corpus ranks differently even with identical
 * tokenization.
 *
 * The caps mirror the Rust ones: they bound the work a single pathological
 * document can cause on a request path that also holds the Durable Object's
 * single thread.
 */
export function extractJsonText(
  value: unknown,
  out: string[] = [],
  depth = 0,
): string[] {
  const MAX_DEPTH = 8
  const MAX_FRAGMENTS = 512

  if (depth > MAX_DEPTH || out.length >= MAX_FRAGMENTS) return out

  if (typeof value === 'string') {
    out.push(value)
  } else if (typeof value === 'number' || typeof value === 'boolean') {
    out.push(String(value))
  } else if (Array.isArray(value)) {
    for (const item of value) {
      if (out.length >= MAX_FRAGMENTS) break
      extractJsonText(item, out, depth + 1)
    }
  } else if (value && typeof value === 'object') {
    for (const [key, item] of Object.entries(value)) {
      if (out.length >= MAX_FRAGMENTS) break
      // Reserved `_`-prefixed metadata is engine bookkeeping (`_version`,
      // `_updated_at`); indexing it would let a query match on timestamps.
      if (key.startsWith('_')) continue
      out.push(key)
      extractJsonText(item, out, depth + 1)
    }
  }
  return out
}
