/**
 * `FILTER` evaluation.
 *
 * Filters run in TypeScript rather than being pushed into SQL. That is not a
 * shortcut: Durable Object SQLite caps a `LIKE`/`GLOB` pattern at 50 bytes, and
 * SQLite's type affinity would quietly make `'5' = 5` true where KIP says it is
 * not.
 *
 * The comparison rule is the one worth stating: **a comparison between unlike
 * types decides nothing**, so it is false, and so is its negation's counterpart
 * — `?c.name > 5` and `?c.name <= 5` are both false for a string name. An
 * engine that coerced would answer a question nobody asked.
 */

import { errors } from '../errors.js'
import { isJsonMap, jsonEquals, type Json } from '../json.js'
import type {
  ComparisonOperator,
  FilterExpression,
  FilterOperand,
} from '../kip/ast.js'
import { tryParseElementId } from '../id.js'
import type { Context } from './context.js'
import { kipLiteral, parameterValue, readVariable, type ReadBindings } from './matching.js'
import type { Solution } from './solution.js'

export function evaluateFilter(
  cx: Context,
  expression: FilterExpression,
  solution: Solution,
  b: ReadBindings,
): boolean {
  if ('Not' in expression) {
    return !evaluateFilter(cx, expression.Not, solution, b)
  }
  if ('Logical' in expression) {
    const { left, operator, right } = expression.Logical
    const first = evaluateFilter(cx, left, solution, b)
    // Short-circuiting is observable here only through cost, but a query that
    // pays for the second half of an already-decided `OR` pays it per row.
    if (operator === 'And') {
      return first && evaluateFilter(cx, right, solution, b)
    }
    return first || evaluateFilter(cx, right, solution, b)
  }
  if ('Comparison' in expression) {
    const { left, operator, right } = expression.Comparison
    return compare(
      operand(cx, left, solution, b),
      operator,
      operand(cx, right, solution, b),
    )
  }
  return callFunction(cx, expression.Function, solution, b)
}

/** One side of a comparison, or one argument of a function. */
function operand(
  cx: Context,
  value: FilterOperand,
  solution: Solution,
  b: ReadBindings,
): Json {
  if ('Variable' in value) {
    return readVariable(cx, solution, value.Variable.var, value.Variable.path)
  }
  if ('Literal' in value) return kipLiteral(value.Literal)
  if ('Param' in value) return parameterValue(b, value.Param)
  if ('List' in value) {
    return value.List.map((item) => operand(cx, item, solution, b))
  }
  const inner = operand(cx, value.Negate, solution, b)
  return typeof inner === 'number' ? -inner : null
}

function compare(left: Json, operator: ComparisonOperator, right: Json): boolean {
  if (operator === 'Equal') return jsonEquals(left, right)
  if (operator === 'NotEqual') return !jsonEquals(left, right)

  // Ordering is only defined within one type. Comparing across two — or
  // against a null nobody bound — decides nothing, which is false in both
  // directions rather than an arbitrary winner.
  if (typeof left !== typeof right) return false
  if (typeof left === 'number' && typeof right === 'number') {
    return order(left - right, operator)
  }
  if (typeof left === 'string' && typeof right === 'string') {
    return order(left < right ? -1 : left > right ? 1 : 0, operator)
  }
  return false
}

function order(sign: number, operator: ComparisonOperator): boolean {
  switch (operator) {
    case 'LessThan':
      return sign < 0
    case 'GreaterThan':
      return sign > 0
    case 'LessEqual':
      return sign <= 0
    case 'GreaterEqual':
      return sign >= 0
    default:
      return false
  }
}

function callFunction(
  cx: Context,
  call: { func: string; args: FilterOperand[] },
  solution: Solution,
  b: ReadBindings,
): boolean {
  const args = call.args.map((arg) => operand(cx, arg, solution, b))
  const [first, second] = args
  const text = (value: Json): string | null =>
    typeof value === 'string' ? value : null

  switch (call.func) {
    case 'IsNull':
      return first === null || first === undefined
    case 'IsNotNull':
      return first !== null && first !== undefined
    case 'Contains': {
      const haystack = text(first as Json)
      const needle = text(second as Json)
      return haystack !== null && needle !== null && haystack.includes(needle)
    }
    case 'StartsWith': {
      const haystack = text(first as Json)
      const needle = text(second as Json)
      return haystack !== null && needle !== null && haystack.startsWith(needle)
    }
    case 'EndsWith': {
      const haystack = text(first as Json)
      const needle = text(second as Json)
      return haystack !== null && needle !== null && haystack.endsWith(needle)
    }
    case 'Regex': {
      const haystack = text(first as Json)
      const pattern = text(second as Json)
      if (haystack === null || pattern === null) return false
      try {
        return new RegExp(pattern).test(haystack)
      } catch {
        throw errors.invalidSyntax(
          `${JSON.stringify(pattern)} is not a valid regular expression`,
        )
      }
    }
    case 'In': {
      const list = second
      return Array.isArray(list)
        ? list.some((item) => jsonEquals(item as Json, first as Json))
        : false
    }
    case 'IsLiteral':
      // An element reference is not a Literal, whatever its text looks like.
      return !isElement(first as Json)
    case 'IsElement':
      return isElement(first as Json)
    case 'IsKind': {
      const value = first as Json
      const kind = text(second as Json)
      if (!isElement(value) || kind === null) return false
      const id = tryParseElementId(
        typeof value === 'string' ? value : String((value as { id: string }).id),
      )
      return id !== null && id.kind.toLowerCase() === kind.toLowerCase()
    }
    case 'LiteralType':
      return literalType(first as Json) === text(second as Json)
    default:
      throw errors.unsupportedCapability(
        `the filter function ${call.func} is not implemented by this engine yet`,
      )
  }
}

/** Whether a value is an element reference rather than a Literal. */
function isElement(value: Json): boolean {
  if (typeof value === 'string') return tryParseElementId(value) !== null
  return isJsonMap(value) && typeof value.id === 'string'
}

function literalType(value: Json): string {
  if (value === null) return 'null'
  if (Array.isArray(value)) return 'array'
  if (typeof value === 'object') return 'object'
  return typeof value
}
