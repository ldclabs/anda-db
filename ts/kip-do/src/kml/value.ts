/**
 * Evaluating a KML right-hand side.
 *
 * The grammar admits a `parameter` at every depth of an array or object, so no
 * assignment, option block or epistemic setting is plain JSON when it reaches
 * the engine. This module closes those holes: it substitutes parameters and
 * handles, and turns the result into the JSON a column stores.
 *
 * The rule that matters most here is the one about references. A **bare string
 * is a Literal**, never a reference: `asserted_by: "C-1"` stores the *text*
 * `C-1`, and the reference form is `asserted_by: {id: "C-1"}`. A handle or a
 * parameter holding an element id resolves to the reference form, because that
 * is unambiguously what it names — and normalizing it here is what keeps an
 * edge written with `:param` from being stored as a string that no structural
 * pattern can ever traverse.
 */

import { errors } from '../errors.js'
import { formatElementId, tryParseElementId } from '../id.js'
import { isJsonMap, type Json, type JsonMap } from '../json.js'
import type {
  Assignments,
  BoundValue,
  KipValue,
  MutationValue,
  Scalar,
  SymbolSlot,
  UpdateExpr,
} from '../kip/ast.js'
import type { Transaction } from '../tx.js'

/** Where a `:parameter` or a `?handle` is looked up. */
export interface Bindings {
  /** Request-level parameters, shared by every operation. */
  request: JsonMap
  /** Operation-level parameters, which win over request-level ones. */
  operation: JsonMap
  tx: Transaction
}

export function bindings(
  tx: Transaction,
  request: JsonMap | undefined,
  operation: JsonMap | undefined,
): Bindings {
  return { tx, request: request ?? {}, operation: operation ?? {} }
}

/** Resolves a `:name` parameter, or fails naming it. */
export function parameter(b: Bindings, name: string): Json {
  if (Object.hasOwn(b.operation, name)) return b.operation[name] as Json
  if (Object.hasOwn(b.request, name)) return b.request[name] as Json
  throw errors.invalidRequestEnvelope(
    `the command reads :${name}, which the request does not bind`,
  )
}

/** Resolves a `?handle` to the element it names, in wire form. */
export function handleId(b: Bindings, name: string): string {
  const id = b.tx.handle(name)
  if (id === null) {
    throw errors.invalidSyntax(
      `?${name} is not bound in this mutation block`,
    )
  }
  return formatElementId(id)
}

/** A `KipValue` as plain JSON. */
export function kipValue(value: KipValue): Json {
  if (value === 'Null') return null
  if ('Bool' in value) return value.Bool
  if ('Number' in value) return value.Number
  if ('String' in value) return value.String
  if ('Array' in value) return value.Array.map(kipValue)
  return Object.fromEntries(
    Object.entries(value.Object).map(([k, v]) => [k, kipValue(v)]),
  )
}

/**
 * A `data_value` with its parameters and handles filled in.
 *
 * A `Variable` here is an update expression reading the target's own fields,
 * which only the update path can evaluate — so it is refused rather than
 * silently read as null.
 */
export function boundValue(b: Bindings, value: BoundValue): Json {
  if ('Value' in value) return kipValue(value.Value)
  if ('Param' in value) return parameter(b, value.Param)
  if ('Handle' in value) return { id: handleId(b, value.Handle) }
  if ('Variable' in value) {
    throw errors.invalidSyntax(
      `?${value.Variable.var} cannot be read here; a variable is only ` +
        `readable in an UPDATE expression over the element being updated`,
    )
  }
  if ('Array' in value) return value.Array.map((item) => boundValue(b, item))
  return Object.fromEntries(
    value.Object.map(([key, item]) => [key, boundValue(b, item)]),
  )
}

/** A `parameter | literal` slot. */
export function scalar(b: Bindings, value: Scalar): Json {
  return 'Param' in value ? parameter(b, value.Param) : kipValue(value.Literal)
}

/** A `parameter | literal` slot that has to be text. */
export function scalarText(b: Bindings, value: Scalar, what: string): string {
  const resolved = scalar(b, value)
  if (typeof resolved !== 'string') {
    throw errors.typeMismatch(
      `${what} must be a string, got ${JSON.stringify(resolved)}`,
    )
  }
  return resolved
}

/** A `parameter | literal` slot that has to be a number. */
export function scalarNumber(b: Bindings, value: Scalar, what: string): number {
  const resolved = scalar(b, value)
  if (typeof resolved !== 'number' || !Number.isFinite(resolved)) {
    throw errors.typeMismatch(
      `${what} must be a number, got ${JSON.stringify(resolved)}`,
    )
  }
  return resolved
}

/** Reads a schema-symbol slot — a quoted symbol or a parameter — to a name. */
export function symbolName(b: Bindings, symbol: SymbolSlot): string {
  if ('Name' in symbol) return symbol.Name
  const resolved = parameter(b, symbol.Param)
  if (typeof resolved !== 'string') {
    throw errors.typeMismatch(
      `a schema symbol must be a string, got ${JSON.stringify(resolved)}`,
    )
  }
  return resolved
}

/**
 * The functions an UPDATE expression may call (§52.4).
 *
 * Arithmetic over the target's *own* fields only. `lower` already rejected a
 * reference to any other variable, which is what lets each matched element be
 * updated from its own row without a join.
 */
function updateExpr(
  b: Bindings,
  expr: UpdateExpr,
  read: (path: string[]) => Json,
): Json {
  if ('Number' in expr) return expr.Number
  if ('Param' in expr) return parameter(b, expr.Param)
  if ('Variable' in expr) {
    return read(
      expr.Variable.path.map((step) =>
        'Field' in step ? step.Field : step.Key,
      ),
    )
  }
  const args = expr.Function.args.map((arg) => updateExpr(b, arg, read))
  const numeric = (index: number, fallback = 0): number => {
    const value = args[index]
    return typeof value === 'number' && Number.isFinite(value) ? value : fallback
  }
  switch (expr.Function.func) {
    case 'Add':
      return numeric(0) + numeric(1)
    case 'Mul':
      return numeric(0) * numeric(1)
    case 'Clamp':
      return Math.min(Math.max(numeric(0), numeric(1)), numeric(2))
    case 'Coalesce':
      // The first argument that is actually there. A missing field reads as
      // null, which is exactly what this exists to replace.
      return args.find((value) => value !== null && value !== undefined) ?? null
  }
}

/**
 * A KML right-hand side: a bound value, or arithmetic over the target's own
 * fields.
 *
 * `read` is how an expression reaches the element being updated. A clause with
 * no target to read from passes one that refuses, so `MUL(?c.x, 2)` in a
 * `CREATE` fails saying why instead of quietly multiplying null.
 */
export function mutationValue(
  b: Bindings,
  value: MutationValue,
  read?: (path: string[]) => Json,
): Json {
  if ('Expr' in value) {
    const reader =
      read ??
      (() => {
        throw errors.invalidSyntax(
          'an arithmetic update expression needs an element to read from; ' +
            'it is only meaningful in UPDATE',
        )
      })
    return updateExpr(b, value.Expr, reader)
  }
  return boundValue(b, value)
}

/** Evaluates an assignment list into the JSON map a clause writes. */
export function assignments(
  b: Bindings,
  list: Assignments,
  read?: (path: string[]) => Json,
): JsonMap {
  const out: JsonMap = {}
  for (const [name, value] of list) {
    out[name] = mutationValue(b, value, read)
  }
  return out
}

/** Evaluates an options block, which never reads the target. */
export function options(
  b: Bindings,
  block: Record<string, BoundValue> | null,
): JsonMap {
  if (block === null) return {}
  return Object.fromEntries(
    Object.entries(block).map(([key, value]) => [key, boundValue(b, value)]),
  )
}

/**
 * Normalizes a value written into a reference slot.
 *
 * A handle already arrives as `{id: …}`. A parameter carrying `"C-3"` does not,
 * and storing it verbatim produces an edge that exists in the column and that
 * no structural pattern can traverse — the defect is invisible because the
 * write reports success. An object that already looks like a reference is left
 * exactly as written, including a canonical or cross-Space one.
 */
export function referenceValue(value: Json, field: string): Json {
  if (isJsonMap(value)) return value
  if (typeof value === 'string' && tryParseElementId(value) !== null) {
    return { id: value }
  }
  throw errors.structuralReferenceInvalid(
    `\`${field}\` must reference an element; got ${JSON.stringify(value)}. ` +
      `A bare string is a Literal — write {id: "C-1"} for a reference.`,
  )
}

/** The element id a reference value names, or `''` when it names none. */
export function referenceId(value: Json): string {
  if (isJsonMap(value) && typeof value.id === 'string') return value.id
  return ''
}
