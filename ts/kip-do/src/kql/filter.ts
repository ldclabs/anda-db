/**
 * `FILTER` evaluation.
 *
 * Filters run in TypeScript rather than being pushed into SQL. That is not a
 * shortcut: Durable Object SQLite caps a `LIKE`/`GLOB` pattern at 50 bytes, and
 * SQLite's type affinity would quietly make `'5' = 5` true where KIP says it is
 * not.
 *
 * The comparison rule is the one worth stating: **a comparison between unlike
 * types decides nothing**, including under negation: neither `?c.name > 5`
 * nor `!(?c.name > 5)` accepts a row whose name is a string. An
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
import { referenceText } from '../store/references.js'
import type { Context } from './context.js'
import { kipLiteral, parameterValue, readVariable, type ReadBindings } from './matching.js'
import type { Solution } from './solution.js'

/** Three-valued truth: null means the operands could not decide a condition. */
type Truth = boolean | null

export function evaluateFilter(cx: Context, expression: FilterExpression, solution: Solution, b: ReadBindings): boolean {
  return truth(cx, expression, solution, b) === true
}

function truth(cx: Context, expression: FilterExpression, solution: Solution, b: ReadBindings): Truth {
  if ('Not' in expression) {
    const value = truth(cx, expression.Not, solution, b)
    return value === null ? null : !value
  }
  if ('Logical' in expression) {
    const { left, operator, right } = expression.Logical
    const first = truth(cx, left, solution, b)
    if (operator === 'And' && first === false) return false
    if (operator === 'Or' && first === true) return true
    const second = truth(cx, right, solution, b)
    if (operator === 'And') return second === false ? false : first === null || second === null ? null : true
    return second === true ? true : first === null || second === null ? null : false
  }
  if ('Comparison' in expression) {
    const { left, operator, right } = expression.Comparison
    return compare(operand(cx, left, solution, b), operator, operand(cx, right, solution, b))
  }
  return callFunction(cx, expression.Function, solution, b)
}

const ARITY: Readonly<Record<string, number>> = { IsNull: 1, IsNotNull: 1, IsLiteral: 1, IsElement: 1, Contains: 2, StartsWith: 2, EndsWith: 2, Regex: 2, In: 2, IsKind: 2, LiteralType: 2 }

/** Inspect every branch before evaluation, including constant REGEX arguments. */
export function validateFilter(cx: Context, expression: FilterExpression, b: ReadBindings, variable: (value: { var: string; path: import('../kip/ast.js').PathStep[] }) => void): void {
  const inspect = (value: FilterOperand): boolean => {
    if ('Variable' in value) { variable(value.Variable); return false }
    if ('Param' in value) { parameterValue(b, value.Param); return true }
    if ('List' in value) return value.List.map(inspect).every(Boolean)
    if ('Negate' in value) return inspect(value.Negate)
    return true
  }
  if ('Not' in expression) validateFilter(cx, expression.Not, b, variable)
  else if ('Logical' in expression) {
    validateFilter(cx, expression.Logical.left, b, variable)
    validateFilter(cx, expression.Logical.right, b, variable)
  } else if ('Comparison' in expression) {
    const { left, operator, right } = expression.Comparison
    const leftConstant = inspect(left)
    const rightConstant = inspect(right)
    if (leftConstant && rightConstant) compare(operand(cx, left, new Map(), b), operator, operand(cx, right, new Map(), b))
  } else {
    const call = expression.Function
    if (ARITY[call.func] === undefined || (call.args.length !== ARITY[call.func] && !(call.func === 'LiteralType' && call.args.length === 1))) throw errors.invalidSyntax(`invalid function or arity: ${call.func}`)
    const constant = call.args.map(inspect)
    const stringPositions = ['Contains', 'StartsWith', 'EndsWith', 'Regex'].includes(call.func) ? [0, 1] : ['IsKind', 'LiteralType'].includes(call.func) ? [1] : []
    for (const index of stringPositions) {
      if (!constant[index]) continue
      const value = operand(cx, call.args[index]!, new Map(), b)
      if (value !== null && typeof value !== 'string') throw errors.typeMismatch(`${call.func} requires a string input`)
    }
    if (call.func === 'In' && constant[1]) {
      const list = operand(cx, call.args[1]!, new Map(), b)
      if (list !== null && !Array.isArray(list)) throw errors.typeMismatch('IN requires a list as its second argument')
    }
    if (call.func === 'Regex' && constant[1]) {
      const pattern = operand(cx, call.args[1]!, new Map(), b)
      if (pattern !== null && typeof pattern !== 'string') throw errors.typeMismatch('REGEX requires a string pattern')
      if (typeof pattern === 'string') regex(pattern)
    }
  }
}

function regex(pattern: string): RegExp {
  try { return new RegExp(pattern) }
  catch { throw errors.invalidSyntax(`${JSON.stringify(pattern)} is not a valid regular expression`) }
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

function compare(left: Json, operator: ComparisonOperator, right: Json): Truth {
  if (left === null || right === null || typeof left !== typeof right) return null
  const equal = typeof left === 'string' && typeof right === 'string' ? left.normalize('NFC') === right.normalize('NFC') : jsonEquals(left, right)
  if (operator === 'Equal') return equal
  if (operator === 'NotEqual') return !equal

  // Ordering is only defined within one type. Comparing across two — or
  // against a null nobody bound — decides nothing, which is false in both
  // directions rather than an arbitrary winner.
  if (typeof left === 'number' && typeof right === 'number') {
    return order(left - right, operator)
  }
  if (typeof left === 'string' && typeof right === 'string') {
    return order(left < right ? -1 : left > right ? 1 : 0, operator)
  }
  if (typeof left === 'boolean' && typeof right === 'boolean') return order(Number(left) - Number(right), operator)
  throw errors.typeMismatch('ordering requires comparable scalar inputs')
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
): Truth {
  const args = call.args.map((arg) => operand(cx, arg, solution, b))
  const [first, second] = args
  const text = (value: Json): string => {
    if (typeof value !== 'string') throw errors.typeMismatch(`${call.func} requires a string input`)
    return value
  }

  const isNull = (value: Json | undefined): boolean => value === null || value === undefined
  if (call.func !== 'IsNull' && call.func !== 'IsNotNull' && args.some(isNull)) return null
  switch (call.func) {
    case 'IsNull':
      return isNull(first)
    case 'IsNotNull':
      return !isNull(first)
    case 'Contains': {
      const haystack = text(first as Json)
      const needle = text(second as Json)
      return haystack === null || needle === null ? null : haystack.includes(needle)
    }
    case 'StartsWith': {
      const haystack = text(first as Json)
      const needle = text(second as Json)
      return haystack === null || needle === null ? null : haystack.startsWith(needle)
    }
    case 'EndsWith': {
      const haystack = text(first as Json)
      const needle = text(second as Json)
      return haystack === null || needle === null ? null : haystack.endsWith(needle)
    }
    case 'Regex': {
      const haystack = text(first as Json)
      const pattern = text(second as Json)
      if (haystack === null || pattern === null) return null
      return regex(pattern).test(haystack)
    }
    case 'In': {
      const list = second
      return Array.isArray(list)
        ? list.some((item) => item !== null && compare(first as Json, 'Equal', item as Json) === true)
        : (() => { throw errors.typeMismatch('IN requires a list as its second argument') })()
    }
    case 'IsLiteral':
      // An element reference is not a Literal, whatever its text looks like.
      return !argumentIsElement(call.args[0]!, first as Json, solution)
    case 'IsElement':
      return argumentIsElement(call.args[0]!, first as Json, solution)
    case 'IsKind': {
      const value = first as Json
      const kind = text(second as Json)
      if (!argumentIsElement(call.args[0]!, value, solution) || kind === null) return false
      const id = tryParseElementId(referenceText(value))
      return id !== null && id.kind.toLowerCase() === kind.toLowerCase()
    }
    case 'LiteralType':
      return second === undefined ? !argumentIsElement(call.args[0]!, first as Json, solution) : literalType(first as Json) === text(second as Json)
    default:
      throw errors.unsupportedCapability(
        `the filter function ${call.func} is not implemented by this engine yet`,
      )
  }
}

function argumentIsElement(arg: FilterOperand, value: Json, solution: Solution): boolean {
  if ('Variable' in arg && arg.Variable.path.length === 0) return solution.get(arg.Variable.var)?.kind === 'element'
  return isJsonMap(value) && typeof value.id === 'string'
}

function literalType(value: Json): string {
  if (value === null) return 'null'
  if (Array.isArray(value)) return 'array'
  if (typeof value === 'object') return 'object'
  return typeof value
}
