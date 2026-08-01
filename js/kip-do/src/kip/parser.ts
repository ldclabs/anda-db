/**
 * Bridge to the KIP grammar compiled to WebAssembly.
 *
 * The grammar is not reimplemented in TypeScript. `rs/anda_kip` is pure
 * computation with no I/O and compiles to `wasm32-unknown-unknown` unchanged,
 * so the Durable Object engine reuses the exact parser, AST and error
 * taxonomy that the Rust engine uses. That removes the single largest source
 * of divergence between the two implementations: a KIP command either parses
 * identically in both, or in neither.
 *
 * The module is imported statically. Workers instantiate WASM at isolate
 * startup rather than at first call, so there is no async init to await and
 * no cold-start cost on the request path.
 */

import {
  initSync,
  parse as wasmParse,
  parse_batch as wasmParseBatch,
  parser_version as wasmParserVersion,
} from '../../vendor/anda_kip_wasm/anda_kip_wasm.js'
// Wrangler and miniflare compile a `.wasm` import into a `WebAssembly.Module`
// at deploy time — no network fetch, no async instantiation on the request
// path. This is the only import form Workers support for WASM.
import wasmModule from '../../vendor/anda_kip_wasm/anda_kip_wasm_bg.wasm'
import { KipError, type KipErrorJSON, internalError } from '../errors.js'
import type { Command } from './ast.js'

// Instantiate at module scope, i.e. at isolate startup rather than on the
// first query. `initSync` is idempotent: it returns early if the module is
// already live, so re-entry from another import path is harmless.
initSync({ module: wasmModule as WebAssembly.Module })

/** Envelope returned by the WASM `parse` / `parse_batch` entry points. */
type ParseEnvelope = { ok: Command } | { error: KipErrorJSON }

function unwrap(raw: string): Command {
  let envelope: ParseEnvelope
  try {
    envelope = JSON.parse(raw) as ParseEnvelope
  } catch (err) {
    throw internalError(
      `the KIP parser returned malformed JSON: ${(err as Error).message}`,
    )
  }
  if ('error' in envelope) throw KipError.fromJSON(envelope.error)
  return envelope.ok
}

/**
 * Parses one KIP command (KQL, KML or META).
 *
 * @throws {KipError} with the grammar's own code, name, message and recovery
 * hint — identical to what the Rust engine would produce for the same input.
 */
export function parseKip(source: string): Command {
  return unwrap(wasmParse(source))
}

/**
 * Parses several commands in one crossing of the WASM boundary.
 *
 * A KIP request may carry a `commands` array, and each crossing costs a
 * string copy in both directions. Results are positionally aligned with the
 * input; a failure is returned in place rather than thrown, so one bad
 * statement does not discard the rest of the batch.
 */
export function parseKipBatch(
  sources: string[],
): ({ ok: Command } | { error: KipError })[] {
  if (sources.length === 0) return []

  const raw = wasmParseBatch(JSON.stringify(sources))
  let parsed: unknown
  try {
    parsed = JSON.parse(raw)
  } catch (err) {
    throw internalError(
      `the KIP parser returned malformed JSON: ${(err as Error).message}`,
    )
  }

  // A non-array result means the batch itself was rejected (malformed input
  // JSON), which the WASM side reports with the single-error envelope.
  if (!Array.isArray(parsed)) {
    const envelope = parsed as ParseEnvelope
    if ('error' in envelope) throw KipError.fromJSON(envelope.error)
    throw internalError('the KIP parser returned an unexpected batch shape')
  }

  return (parsed as ParseEnvelope[]).map((envelope) =>
    'error' in envelope
      ? { error: KipError.fromJSON(envelope.error) }
      : { ok: envelope.ok },
  )
}

/**
 * Version of the bundled grammar.
 *
 * Surfaced in `DESCRIBE PRIMER` so a grammar bump is visible to the agent
 * rather than silently changing what a stored command means.
 */
export function parserVersion(): string {
  return wasmParserVersion()
}
