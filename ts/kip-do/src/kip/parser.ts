/**
 * Bridge to the KIP grammar.
 *
 * The grammar is not reimplemented here. `@ldclabs/kip-lang` owns the lexer,
 * the parser and — the part that matters to an engine — `lower`, which turns
 * the syntax tree into the executable AST and rejects everything the syntax
 * admits but the language does not: an unknown filter function, `{type: "T"}`
 * where an identity is required, an UPDATE expression reading a foreign
 * variable. What reaches `src/exec/` is already normalized.
 *
 * That AST is byte-for-byte the wire form of `anda_kip`'s Rust AST, and
 * `test/parser-oracle.test.ts` asserts it: for a corpus drawn from the
 * conformance fixtures, the Rust parser's own tests and the bundled capsules,
 * `lower(parse(src))` must deep-equal what the Rust grammar compiled to WASM
 * produces, or both must reject. The two engines agreeing on what a command
 * means is enforced there rather than assumed here.
 */

import {
  KIP_SPEC_REVISION,
  KipSyntaxError,
  checkBatchBudget,
  checkBudget,
  lower,
  lowerAll,
  parse,
  PARSER_VERSION,
} from '@ldclabs/kip-lang'
import type { Program } from '@ldclabs/kip-lang'
import { KipError, errors, type KipErrorCode } from '../errors.js'
import type { Command } from './ast.js'

/**
 * The parse-time codes kip-lang reports, in this engine's registry.
 *
 * The toolkit still spells its three codes the 1.x way (`KIP_1001` and
 * friends) while KIP 2.0 replaced the numbers with stable names, so the
 * translation has to happen somewhere. It happens here, at the boundary,
 * rather than by teaching the rest of the engine two spellings — and an
 * unrecognized code becomes `InvalidSyntax` rather than `InternalError`,
 * because whatever it was, the command did not parse.
 */
const SYNTAX_CODES: Record<string, KipErrorCode> = {
  KIP_1001: 'InvalidSyntax',
  KIP_1002: 'InvalidIdentifier',
  KIP_4002: 'ResourceExhausted',
}

/**
 * Turns whatever kip-lang reports into a `KipError`.
 *
 * `parse` accumulates diagnostics so an editor can keep showing a tree after a
 * mistake; an engine wants the opposite — the first thing that makes the
 * command unexecutable, with a code it can put on the wire. The `hint` and
 * `retry` class come from this package's own generated registry, so the
 * agent-facing recovery contract is identical to what the Rust engine sends
 * for the same code.
 */
function toKipError(err: unknown): KipError {
  if (err instanceof KipSyntaxError) {
    return new KipError(SYNTAX_CODES[err.code] ?? 'InvalidSyntax', err.message)
  }
  // A diagnostic raised by `parseProgram` is already a `KipError`; re-wrapping
  // it would turn a syntax error into an InternalError and strip the code the
  // agent recovers from.
  if (err instanceof KipError) return err
  if (err instanceof Error) return errors.internalError(err.message)
  return errors.internalError(String(err))
}

/** Parses to a syntax tree, raising the first error diagnostic. */
function parseProgram(source: string): Program {
  checkBudget(source)
  const { ast, diagnostics } = parse(source)
  const fatal = diagnostics.find((d) => d.severity === 'error')
  if (fatal) {
    const where = ` (line ${fatal.range.start.line + 1}, column ${
      fatal.range.start.column + 1
    })`
    throw errors.invalidSyntax(fatal.message + where)
  }
  return ast
}

/**
 * Parses one KIP command (KQL, KML or META).
 *
 * @throws {KipError} carrying the grammar's own code, name, message and
 * recovery hint — the same envelope the Rust engine produces for the same
 * input.
 */
export function parseKip(source: string): Command {
  try {
    return lower(parseProgram(source))
  } catch (err) {
    throw toKipError(err)
  }
}

/**
 * Parses several commands.
 *
 * A KIP request may carry a `commands` array. Results are positionally aligned
 * with the input; a failure is returned in place rather than thrown, so one bad
 * statement does not discard the rest of the batch.
 */
export function parseKipBatch(
  sources: string[],
): ({ ok: Command } | { error: KipError })[] {
  if (sources.length === 0) return []
  try {
    checkBatchBudget(sources.length)
  } catch (err) {
    throw toKipError(err)
  }

  return sources.map((source) => {
    try {
      return { ok: parseKip(source) }
    } catch (err) {
      return { error: KipError.from(err) }
    }
  })
}

/**
 * Parses source text that is a sequence of commands, such as a schema capsule.
 *
 * Unlike {@link parseKip} this accepts more than one statement. Consecutive
 * `UPSERT` blocks still fold into a single command, which is what a capsule
 * relies on to apply as a unit.
 */
export function parseKipAll(source: string): Command[] {
  try {
    return lowerAll(parseProgram(source))
  } catch (err) {
    throw toKipError(err)
  }
}

/**
 * Version of the bundled grammar.
 *
 * Surfaced in `DESCRIBE PRIMER` so a grammar bump is visible to the agent
 * rather than silently changing what a stored command means.
 */
export function parserVersion(): string {
  return PARSER_VERSION
}

/**
 * KIP specification revision the bundled grammar targets.
 *
 * `parserVersion` moves whenever the implementation ships a fix; this moves
 * only when the language does, which is the thing an agent — or another
 * engine — actually needs to compare against.
 */
export function specRevision(): string {
  return KIP_SPEC_REVISION
}
