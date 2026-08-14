/**
 * Solution tables — the relational algebra KQL's WHERE clauses operate on.
 *
 * Mirrors `types.rs:287-660`. A `SolutionTable` is a column header plus rows
 * of bindings. The engine keeps a *forest* of them rather than one wide
 * table, with the invariant that each variable lives in exactly one table:
 * variable groups that share nothing stay separate so their cross product is
 * never materialized until a clause actually forces it.
 *
 * Every operation is bounded by `MAX_SOLUTION_COMBINATIONS`. In the Rust
 * engine that cap protects a server process; here it also protects the
 * Durable Object's single thread and its 128 MB isolate, so it is enforced
 * more eagerly, not less.
 */

import { queryTooComplex } from '../errors.js'
import {
  type EntityID,
  compareEntityID,
  formatEntityID,
} from '../entity.js'

/** Engine cap on materialized solution rows (`types.rs:253`). */
export const MAX_SOLUTION_COMBINATIONS = 65_536

/**
 * A single cell.
 *
 * `predicate` exists because a predicate variable binds a *string*, not a
 * row id, and after a UNION one variable may legally hold both kinds.
 * `null` is the OPTIONAL padding value and acts as a join wildcard.
 */
export type BindingValue =
  | { kind: 'entity'; id: EntityID }
  | { kind: 'predicate'; name: string }
  | { kind: 'null' }

export const NULL_BINDING: BindingValue = { kind: 'null' }

export const entityBinding = (id: EntityID): BindingValue => ({
  kind: 'entity',
  id,
})

export const predicateBinding = (name: string): BindingValue => ({
  kind: 'predicate',
  name,
})

/**
 * Separator between the per-cell keys of a row key.
 *
 * Written as an escape rather than a literal control byte: a raw U+0001 in
 * the source makes git classify this file as binary, so its diffs stop
 * rendering, and any tool that normalizes control characters would silently
 * merge cell boundaries.
 */
const KEY_SEPARATOR = '\u0001'

/**
 * Stable key for hashing and dedup.
 *
 * Every non-null key carries an `e:` / `p:` prefix, so the bare `n` standing
 * for the null padding value cannot collide with either. `undefined` — a
 * variable absent from the row being keyed — is the same absence as an
 * explicit null and keys the same way.
 */
export function bindingKey(value: BindingValue | undefined): string {
  if (!value) return 'n'
  switch (value.kind) {
    case 'entity':
      return `e:${formatEntityID(value.id)}`
    case 'predicate':
      return `p:${value.name}`
    case 'null':
      return 'n'
  }
}

/** Joins per-cell keys into one row key. */
export function rowKeyOf(
  values: readonly (BindingValue | undefined)[],
): string {
  return values.map(bindingKey).join(KEY_SEPARATOR)
}

export function bindingEquals(a: BindingValue, b: BindingValue): boolean {
  return bindingKey(a) === bindingKey(b)
}

function rowKey(row: readonly BindingValue[], cols: readonly number[]): string {
  return rowKeyOf(cols.map((c) => row[c]!))
}

export class SolutionTable {
  constructor(
    readonly vars: string[],
    readonly rows: BindingValue[][],
  ) {}

  static empty(vars: string[]): SolutionTable {
    return new SolutionTable(vars, [])
  }

  static single(varName: string, values: BindingValue[]): SolutionTable {
    return new SolutionTable(
      [varName],
      values.map((v) => [v]),
    )
  }

  covers(varName: string): boolean {
    return this.vars.includes(varName)
  }

  column(varName: string): number | null {
    const idx = this.vars.indexOf(varName)
    return idx === -1 ? null : idx
  }

  get isEmpty(): boolean {
    return this.rows.length === 0
  }

  /**
   * Natural join on shared variables; cross product when none are shared.
   *
   * Rows whose join key contains a `null` binding are wildcards: OPTIONAL
   * padding must match anything, so they are probed against the full right
   * side rather than by hash. That is the same degenerate path the Rust
   * implementation takes (`types.rs:386-403`).
   */
  join(other: SolutionTable): SolutionTable {
    const shared = this.vars.filter((v) => other.vars.includes(v))
    const rightOnly = other.vars.filter((v) => !this.vars.includes(v))
    const outVars = [...this.vars, ...rightOnly]

    if (shared.length === 0) {
      return new SolutionTable(
        outVars,
        crossProduct(this.rows, other.rows, other.vars, rightOnly),
      )
    }

    const leftCols = shared.map((v) => this.column(v)!)
    const rightCols = shared.map((v) => other.column(v)!)
    const rightOnlyCols = rightOnly.map((v) => other.column(v)!)

    // Hash the right side, keeping wildcard rows aside for the nested-loop
    // fallback.
    const index = new Map<string, BindingValue[][]>()
    const wild: BindingValue[][] = []
    for (const row of other.rows) {
      if (rightCols.some((c) => row[c]!.kind === 'null')) {
        wild.push(row)
        continue
      }
      const key = rowKey(row, rightCols)
      const bucket = index.get(key)
      if (bucket) bucket.push(row)
      else index.set(key, [row])
    }

    const out: BindingValue[][] = []
    for (const left of this.rows) {
      const leftWild = leftCols.some((c) => left[c]!.kind === 'null')
      const candidates = leftWild
        ? other.rows
        : [...(index.get(rowKey(left, leftCols)) ?? []), ...wild]

      for (const right of candidates) {
        // Checked for every candidate, not just when the *left* row is a
        // wildcard: a right row reaches `wild` as soon as one shared column
        // is null, and its remaining shared columns still have to agree.
        if (!compatible(left, leftCols, right, rightCols)) continue
        out.push([...left, ...rightOnlyCols.map((c) => right[c]!)])
        if (out.length > MAX_SOLUTION_COMBINATIONS) {
          throw queryTooComplex(
            `joining solutions on ${shared.join(', ')} produces more than ` +
              `${MAX_SOLUTION_COMBINATIONS} rows; add a more selective ` +
              `pattern or lower the LIMIT`,
          )
        }
      }
    }
    return new SolutionTable(outVars, out)
  }

  /**
   * Left join for OPTIONAL: every left row survives, padded with `null` when
   * the right side has no match. Hash-partitioned on the shared columns like
   * `join`, with null-carrying rows probed as wildcards, so the cost is
   * proportional to the matches rather than |left| x |right|.
   */
  leftJoin(other: SolutionTable): SolutionTable {
    const shared = this.vars.filter((v) => other.vars.includes(v))
    const rightOnly = other.vars.filter((v) => !this.vars.includes(v))
    const outVars = [...this.vars, ...rightOnly]
    const rightOnlyCols = rightOnly.map((v) => other.column(v)!)
    const leftCols = shared.map((v) => this.column(v)!)
    const rightCols = shared.map((v) => other.column(v)!)

    const index = new Map<string, BindingValue[][]>()
    const wild: BindingValue[][] = []
    for (const row of other.rows) {
      if (rightCols.some((c) => row[c]!.kind === 'null')) {
        wild.push(row)
        continue
      }
      const key = rowKey(row, rightCols)
      const bucket = index.get(key)
      if (bucket) bucket.push(row)
      else index.set(key, [row])
    }

    const out: BindingValue[][] = []
    for (const left of this.rows) {
      const leftWild = leftCols.some((c) => left[c]!.kind === 'null')
      const candidates = leftWild
        ? other.rows
        : [...(index.get(rowKey(left, leftCols)) ?? []), ...wild]

      let matched = false
      for (const right of candidates) {
        if (!compatible(left, leftCols, right, rightCols)) continue
        matched = true
        out.push([...left, ...rightOnlyCols.map((c) => right[c]!)])
        if (out.length > MAX_SOLUTION_COMBINATIONS) {
          throw queryTooComplex(
            `OPTIONAL produces more than ${MAX_SOLUTION_COMBINATIONS} rows`,
          )
        }
      }
      if (!matched) {
        out.push([...left, ...rightOnly.map(() => NULL_BINDING)])
      }
    }
    return new SolutionTable(outVars, out)
  }

  /** Row-wise concatenation with padding and dedup, for UNION. */
  union(other: SolutionTable): SolutionTable {
    const outVars = [...this.vars]
    for (const v of other.vars) if (!outVars.includes(v)) outVars.push(v)

    const seen = new Set<string>()
    const out: BindingValue[][] = []
    const pad = (table: SolutionTable, row: BindingValue[]) =>
      outVars.map((v) => {
        const col = table.column(v)
        return col === null ? NULL_BINDING : row[col]!
      })

    for (const [table, rows] of [
      [this, this.rows] as const,
      [other, other.rows] as const,
    ]) {
      for (const row of rows) {
        const padded = pad(table, row)
        const key = rowKeyOf(padded)
        if (seen.has(key)) continue
        seen.add(key)
        out.push(padded)
        if (out.length > MAX_SOLUTION_COMBINATIONS) {
          throw queryTooComplex(
            `UNION produces more than ${MAX_SOLUTION_COMBINATIONS} rows`,
          )
        }
      }
    }
    return new SolutionTable(outVars, out)
  }

  /** Keeps rows satisfying a predicate, in place. */
  retain(predicate: (row: BindingValue[]) => boolean): void {
    const kept = this.rows.filter(predicate)
    this.rows.length = 0
    this.rows.push(...kept)
  }

  /**
   * Distinct non-null bindings of one variable, in row order — entity and
   * predicate bindings alike (`types.rs` `distinct_values`). This is the seed
   * for NOT / OPTIONAL child scopes, where a predicate-bound variable must
   * stay visible.
   */
  distinctValues(varName: string): BindingValue[] {
    const col = this.column(varName)
    if (col === null) return []
    const seen = new Set<string>()
    const out: BindingValue[] = []
    for (const row of this.rows) {
      const cell = row[col]!
      if (cell.kind === 'null') continue
      const key = bindingKey(cell)
      if (seen.has(key)) continue
      seen.add(key)
      out.push(cell)
    }
    return out
  }

  /** Distinct entity bindings of one variable, in ascending id order. */
  entityDomain(varName: string): EntityID[] {
    const col = this.column(varName)
    if (col === null) return []
    const seen = new Set<string>()
    const out: EntityID[] = []
    for (const row of this.rows) {
      const cell = row[col]!
      if (cell.kind !== 'entity') continue
      const key = formatEntityID(cell.id)
      if (seen.has(key)) continue
      seen.add(key)
      out.push(cell.id)
    }
    out.sort(compareEntityID)
    return out
  }
}

function compatible(
  left: readonly BindingValue[],
  leftCols: readonly number[],
  right: readonly BindingValue[],
  rightCols: readonly number[],
): boolean {
  for (let i = 0; i < leftCols.length; i++) {
    const a = left[leftCols[i]!]!
    const b = right[rightCols[i]!]!
    // A null on either side is OPTIONAL padding and matches anything.
    if (a.kind === 'null' || b.kind === 'null') continue
    if (!bindingEquals(a, b)) return false
  }
  return true
}

function crossProduct(
  left: readonly BindingValue[][],
  right: readonly BindingValue[][],
  _rightVars: readonly string[],
  rightOnly: readonly string[],
): BindingValue[][] {
  if (left.length * right.length > MAX_SOLUTION_COMBINATIONS) {
    throw queryTooComplex(
      `cross product of ${left.length} x ${right.length} solutions exceeds ` +
        `${MAX_SOLUTION_COMBINATIONS} rows; the WHERE clauses share no ` +
        `variable, so they cannot be joined — add a connecting pattern`,
    )
  }
  const out: BindingValue[][] = []
  const cols = rightOnly.map((_, i) => i)
  for (const l of left) {
    for (const r of right) {
      out.push([...l, ...cols.map((i) => r[i]!)])
    }
  }
  return out
}

/**
 * The forest of solution tables carried through a WHERE clause list.
 *
 * `mergeTable` folds a new table into the forest, joining through every
 * existing table that shares a variable with the accumulator. Two previously
 * disconnected groups are only cross-joined when the new table bridges them,
 * which is what keeps unrelated patterns from multiplying out.
 */
export class SolutionContext {
  tables: SolutionTable[] = []

  /**
   * When true, "dangling id" grounding failures (`KIP_3002`) degrade to an
   * empty match instead of failing the whole query. Set for NOT / OPTIONAL /
   * UNION child scopes (KIP §3.4.7), mirroring `lenient_grounding` in the
   * Rust engine: a sub-pattern that cannot match makes the NOT succeed, the
   * OPTIONAL pad with null, or the UNION branch contribute nothing.
   */
  lenient = false

  mergeTable(table: SolutionTable): void {
    let acc = table
    const keep: SolutionTable[] = []
    for (const existing of this.tables) {
      if (existing.vars.some((v) => acc.covers(v))) {
        acc = existing.join(acc)
      } else {
        keep.push(existing)
      }
    }
    keep.push(acc)
    this.tables = keep
  }

  find(varName: string): SolutionTable | null {
    return this.tables.find((t) => t.covers(varName)) ?? null
  }

  boundVars(): Set<string> {
    const out = new Set<string>()
    for (const t of this.tables) for (const v of t.vars) out.add(v)
    return out
  }

  /**
   * Materializes one table covering all the given variables, removing the
   * tables it consumed. This is where disconnected groups finally cross-join,
   * so it is only called when a clause genuinely needs the correlation
   * (FILTER, NOT, multi-variable FIND).
   */
  joinCovering(vars: readonly string[]): SolutionTable {
    const involved: SolutionTable[] = []
    const rest: SolutionTable[] = []
    for (const t of this.tables) {
      if (vars.some((v) => t.covers(v))) involved.push(t)
      else rest.push(t)
    }
    if (involved.length === 0) return SolutionTable.empty([])

    let acc = involved[0]!
    for (let i = 1; i < involved.length; i++) acc = acc.join(involved[i]!)
    this.tables = [...rest, acc]
    return acc
  }

  /** True when any table has been reduced to zero rows. */
  get isUnsatisfiable(): boolean {
    return this.tables.some((t) => t.isEmpty)
  }
}
