/**
 * KQL execution: WHERE clause evaluation and FIND projection.
 *
 * Clauses run sequentially in source order against a `SolutionContext`, the
 * same as the Rust engine — there is no planner and no clause reordering, so
 * query shape determines cost and the caps in `solution.ts` are the safety
 * net.
 *
 * FILTER functions (`CONTAINS`, `REGEX`, …) are evaluated in TypeScript over
 * already-materialized bindings, never pushed into SQL. That is not a
 * shortcut: Durable Object SQLite caps a `LIKE`/`GLOB` pattern at 50 bytes
 * and has no regex at all, so pushing them down would silently break on the
 * inputs users actually write.
 */

import type {
  ConceptMatcher,
  FilterExpression,
  FilterOperand,
  Json,
  KipValue,
  PredTerm,
  PropositionMatcher,
  TargetTerm,
  WhereClause,
} from '../kip/ast.js'
import {
  type EntityID,
  compareEntityID,
  conceptID,
  formatEntityID,
  parseEntityID,
  propositionID,
} from '../entity.js'
import {
  KipError,
  internalError,
  invalidSyntax,
  notFound,
  queryTooComplex,
  referenceError,
} from '../errors.js'
import type { Store } from '../store.js'
import {
  type BindingValue,
  NULL_BINDING,
  SolutionContext,
  SolutionTable,
  bindingKey,
  entityBinding,
  predicateBinding,
  rowKeyOf,
} from './solution.js'

/** Engine cap on multi-hop traversal depth (`matching.rs:14`). */
export const MAX_MULTI_HOP = 10

export class KqlExecutor {
  constructor(private readonly store: Store) {}

  // -------------------------------------------------------------------
  // WHERE
  // -------------------------------------------------------------------

  executeWhere(clauses: readonly WhereClause[], ctx: SolutionContext): void {
    // Every clause runs unconditionally, mirroring the Rust engine
    // (`db/mod.rs:715-717`). FIND projects per covering table, so a clause
    // whose variables are disjoint from an already-empty group still has to
    // contribute its bindings — skipping it would make the result depend on
    // clause order.
    for (const clause of clauses) this.executeClause(clause, ctx)
  }

  private executeClause(clause: WhereClause, ctx: SolutionContext): void {
    if ('Concept' in clause) {
      this.executeConceptClause(
        clause.Concept.variable,
        clause.Concept.matcher,
        ctx,
      )
    } else if ('Proposition' in clause) {
      this.executePropositionClause(
        clause.Proposition.variable,
        clause.Proposition.matcher,
        ctx,
      )
    } else if ('Filter' in clause) {
      this.executeFilter(clause.Filter.expression, ctx)
    } else if ('Not' in clause) {
      this.executeNot(clause.Not, ctx)
    } else if ('Optional' in clause) {
      this.executeOptional(clause.Optional, ctx)
    } else if ('Union' in clause) {
      this.executeUnion(clause.Union, ctx)
    } else {
      throw internalError(
        `unhandled WHERE clause: ${Object.keys(clause).join(', ')}`,
      )
    }
  }

  /**
   * Runs a grounding resolution, degrading `KIP_3002` to an empty match in a
   * lenient (NOT / OPTIONAL / UNION) scope. Mirrors `lenient_grounding` in
   * the Rust engine: only the precise dangling-id error degrades, and the
   * variable columns survive so the parent block can null-pad them.
   */
  private groundLenient<T>(ctx: SolutionContext, resolve: () => T[]): T[] {
    try {
      return resolve()
    } catch (err) {
      if (ctx.lenient && err instanceof KipError && err.code === 'KIP_3002') {
        return []
      }
      throw err
    }
  }

  private executeConceptClause(
    variable: string,
    matcher: ConceptMatcher,
    ctx: SolutionContext,
  ): void {
    const ids = this.groundLenient(ctx, () =>
      this.resolveConceptMatcher(matcher),
    )
    const existing = ctx.find(variable)
    if (existing) {
      // Semi-join: constrain the existing binding rather than re-introducing
      // the variable, so correlations established by earlier clauses survive.
      const allowed = new Set(ids.map((id) => formatEntityID(conceptID(id))))
      const col = existing.column(variable)!
      existing.retain((row) => {
        const cell = row[col]!
        if (cell.kind === 'null') return true
        if (cell.kind !== 'entity') return false
        return allowed.has(formatEntityID(cell.id))
      })
    } else {
      ctx.mergeTable(
        SolutionTable.single(
          variable,
          ids.map((id) => entityBinding(conceptID(id))),
        ),
      )
    }
  }

  /**
   * Resolves a literal `P:<n>:<predicate>` address, or fails.
   *
   * A wrong *shape* is a syntax error; a well-formed address that names
   * nothing is NotFound. Collapsing both into "no match" would let a stale id
   * silently produce an empty result set.
   */
  requireLink(raw: string): EntityID {
    const entity = parseEntityID(raw)
    if (entity.kind !== 'proposition') {
      throw invalidSyntax(
        `${raw} must be a Proposition ID of the form "P:<n>:<predicate>"`,
      )
    }
    const row = this.store.getProposition(entity.id)
    if (!row?.links.has(entity.predicate)) {
      throw notFound(`Proposition ${raw} not found`)
    }
    return entity
  }

  /** Concept ids matching a `{...}` pattern. */
  resolveConceptMatcher(matcher: ConceptMatcher): number[] {
    if ('ID' in matcher) {
      const entity = parseEntityID(matcher.ID)
      if (entity.kind !== 'concept') {
        throw invalidSyntax(
          `${matcher.ID} is not a concept id; a {id:} matcher needs "C:<n>"`,
        )
      }
      if (!this.store.conceptExists(entity.id)) {
        throw notFound(`Concept ${matcher.ID} not found`)
      }
      return [entity.id]
    }
    if ('Type' in matcher) return this.store.conceptIdsByType(matcher.Type)
    if ('Name' in matcher) return this.store.conceptIdsByName(matcher.Name)
    if ('Object' in matcher) {
      const id = this.store.findConceptByTypeName(
        matcher.Object.type,
        matcher.Object.name,
      )
      if (id === null) {
        throw notFound(
          `Concept {type: ${JSON.stringify(matcher.Object.type)}, ` +
            `name: ${JSON.stringify(matcher.Object.name)}} not found`,
        )
      }
      return [id]
    }
    throw internalError(
      `unhandled concept matcher: ${Object.keys(matcher).join(', ')}`,
    )
  }

  private executePropositionClause(
    variable: string | null,
    matcher: PropositionMatcher,
    ctx: SolutionContext,
  ): void {
    if ('ID' in matcher) {
      const entities = this.groundLenient(ctx, () => [
        this.requireLink(matcher.ID),
      ])
      if (variable) {
        ctx.mergeTable(
          SolutionTable.single(variable, entities.map(entityBinding)),
        )
      }
      return
    }

    const { subject, predicate, object } = matcher.Object

    if ('MultiHop' in predicate) {
      this.executeMultiHop(subject, predicate, object, ctx)
      return
    }

    // Resolve each endpoint to either a concrete id set or a variable name.
    const subjectSlot = this.resolveTargetSlot(subject, ctx)
    const objectSlot = this.resolveTargetSlot(object, ctx)
    const predicateNames = predicateCandidates(predicate)

    const rowIds = this.store.matchPropositionRows(
      subjectSlot.ids,
      objectSlot.ids,
      predicateNames,
    )
    if (rowIds.length > MAX_ROW_MATCHES) {
      throw queryTooComplex(
        `proposition pattern matches ${rowIds.length} rows, over the engine ` +
          `cap of ${MAX_ROW_MATCHES}; constrain an endpoint or the predicate`,
      )
    }
    const rows = this.store.getPropositions(rowIds)

    // Build one solution row per (row, predicate) link, which is the element
    // KIP addresses — not per proposition row.
    const vars: string[] = []
    const predVar = 'Variable' in predicate ? predicate.Variable : null
    if (subjectSlot.variable) vars.push(subjectSlot.variable)
    if (predVar && !vars.includes(predVar)) vars.push(predVar)
    if (objectSlot.variable && !vars.includes(objectSlot.variable)) {
      vars.push(objectSlot.variable)
    }
    if (variable && !vars.includes(variable)) vars.push(variable)

    const solutionRows: BindingValue[][] = []
    for (const id of rowIds) {
      const row = rows.get(id)
      if (!row) continue
      for (const [name] of row.links) {
        if (predicateNames && !predicateNames.includes(name)) continue
        // A variable naming several positions degenerates to an equality
        // filter on the row rather than independent bindings — for every
        // position, not just the subject/object pair (`matching.rs:744-786`).
        const cells = new Map<string, BindingValue>()
        const bind = (varName: string, value: BindingValue): boolean => {
          const existing = cells.get(varName)
          if (existing && bindingsDiffer(existing, value)) return false
          cells.set(varName, value)
          return true
        }
        if (subjectSlot.variable) {
          bind(subjectSlot.variable, entityBinding(row.subject))
        }
        if (predVar && !bind(predVar, predicateBinding(name))) continue
        if (
          objectSlot.variable &&
          !bind(objectSlot.variable, entityBinding(row.object))
        ) {
          continue
        }
        if (
          variable &&
          !bind(variable, entityBinding(propositionID(row.id, name)))
        ) {
          continue
        }
        solutionRows.push(vars.map((v) => cells.get(v) ?? { kind: 'null' }))
        if (solutionRows.length > MAX_ROW_MATCHES) {
          throw queryTooComplex(
            `proposition pattern produces more than ${MAX_ROW_MATCHES} links`,
          )
        }
      }
    }

    if (vars.length === 0) {
      // Fully ground pattern: it either holds or falsifies the conjunction.
      if (solutionRows.length === 0) ctx.mergeTable(SolutionTable.empty([]))
      return
    }
    ctx.mergeTable(new SolutionTable(vars, solutionRows))
  }

  /**
   * `(?a, "pred"{m,n}, ?b)` — reachability, evaluated as a recursive CTE.
   *
   * One endpoint must already be bound to a concrete set; with both free the
   * traversal would enumerate the transitive closure of the entire graph.
   */
  private executeMultiHop(
    subject: TargetTerm,
    predicate: Extract<PredTerm, { MultiHop: unknown }>,
    object: TargetTerm,
    ctx: SolutionContext,
  ): void {
    const { predicate: name, min, max } = predicate.MultiHop
    const maxHops = max ?? MAX_MULTI_HOP
    if (min > MAX_MULTI_HOP || maxHops > MAX_MULTI_HOP) {
      throw queryTooComplex(
        `multi-hop quantifier exceeds the engine cap of ${MAX_MULTI_HOP} hops; ` +
          `lower the bound or traverse in stages`,
      )
    }

    const subjectSlot = this.resolveTargetSlot(subject, ctx)
    const objectSlot = this.resolveTargetSlot(object, ctx)

    let start: readonly EntityID[]
    let direction: 'forward' | 'backward'
    let boundVar: string | null
    let freeVar: string | null
    /** Far endpoint, when it names a concrete set rather than a variable. */
    let targets: EntityID[] | null

    if (subjectSlot.ids) {
      start = subjectSlot.ids
      direction = 'forward'
      boundVar = subjectSlot.variable
      freeVar = objectSlot.variable
      targets = objectSlot.variable === null ? objectSlot.ids : null
    } else if (objectSlot.ids) {
      start = objectSlot.ids
      direction = 'backward'
      boundVar = objectSlot.variable
      freeVar = subjectSlot.variable
      targets = subjectSlot.variable === null ? subjectSlot.ids : null
    } else {
      throw invalidSyntax(
        'The subject or object cannot both be variables in multi-hop matching',
      )
    }

    // A grounded far endpoint constrains which reached nodes count, exactly as
    // `path_matches_targets` does in the Rust engine (`matching.rs:110-139`).
    // Without it `(?s, "isa"{1,3}, {name: "X"})` degenerates into "?s reaches
    // anything" and the named endpoint is silently ignored. A far endpoint
    // that is a *variable* is left alone: the correlation it carries is
    // enforced by the natural join on that variable below.
    const targetKeys =
      targets === null ? null : new Set(targets.map(formatEntityID))

    // Traverse per start node so the reached set stays attributable to the
    // endpoint it came from — a single combined traversal would lose which
    // start reached which node and produce a spurious cross product.
    const vars: string[] = []
    if (boundVar) vars.push(boundVar)
    if (freeVar && !vars.includes(freeVar)) vars.push(freeVar)

    const rows: BindingValue[][] = []
    let matched = 0
    for (const origin of start) {
      const reached = this.store.reachable(
        [origin],
        name,
        min,
        maxHops,
        direction,
      )
      for (const hit of reached) {
        if (targetKeys && !targetKeys.has(formatEntityID(hit.node))) continue
        const cells = new Map<string, BindingValue>()
        if (boundVar) cells.set(boundVar, entityBinding(origin))
        if (freeVar) {
          const existing = cells.get(freeVar)
          const value = entityBinding(hit.node)
          if (existing && bindingsDiffer(existing, value)) continue
          cells.set(freeVar, value)
        }
        matched++
        if (vars.length > 0) {
          rows.push(vars.map((v) => cells.get(v)!))
        }
        if (rows.length > MAX_SOLUTION_ROWS) {
          throw queryTooComplex(
            `multi-hop traversal on "${name}" enumerates more than ` +
              `${MAX_SOLUTION_ROWS} results; narrow the start set or lower ` +
              `the hop bound`,
          )
        }
      }
    }

    if (vars.length === 0) {
      // Both endpoints ground: the pattern either holds or falsifies the
      // conjunction, the same contract `executePropositionClause` honours.
      if (matched === 0) ctx.mergeTable(SolutionTable.empty([]))
      return
    }
    ctx.mergeTable(new SolutionTable(vars, rows))
  }

  /**
   * Reduces an endpoint to either a concrete id set (grounded) or a variable
   * name (free). A variable already bound in the context counts as grounded,
   * which is what makes clause order determine the access path.
   */
  private resolveTargetSlot(
    term: TargetTerm,
    ctx: SolutionContext,
  ): { ids: EntityID[] | null; variable: string | null } {
    if ('Variable' in term) {
      const table = ctx.find(term.Variable)
      if (table) return { ids: table.entityDomain(term.Variable), variable: term.Variable }
      return { ids: null, variable: term.Variable }
    }
    if ('Concept' in term) {
      return {
        ids: this.groundLenient(ctx, () =>
          this.resolveConceptMatcher(term.Concept).map(conceptID),
        ),
        variable: null,
      }
    }
    if ('Proposition' in term) {
      // Meta-statement endpoint: resolve the nested pattern to the links it
      // matches, each addressed as `P:{id}:{predicate}`.
      return {
        ids: this.groundLenient(ctx, () =>
          this.resolveNestedProposition(term.Proposition, ctx),
        ),
        variable: null,
      }
    }
    throw internalError(
      `unhandled target term: ${Object.keys(term).join(', ')}`,
    )
  }

  /**
   * Resolves a nested proposition pattern to the link addresses it matches.
   *
   * Public because KML needs the same resolution for meta-statement endpoints:
   * `((?a, "treats", ?b), "cited_by", ?src)` must name an existing link before
   * a higher-order proposition can point at it.
   */
  resolveNestedProposition(
    matcher: PropositionMatcher,
    ctx: SolutionContext,
  ): EntityID[] {
    if ('ID' in matcher) return [this.requireLink(matcher.ID)]
    const { subject, predicate, object } = matcher.Object
    if ('MultiHop' in predicate) {
      throw queryTooComplex(
        'a multi-hop quantifier cannot appear inside a nested proposition endpoint',
      )
    }
    const subjectSlot = this.resolveTargetSlot(subject, ctx)
    const objectSlot = this.resolveTargetSlot(object, ctx)
    const names = predicateCandidates(predicate)
    const rowIds = this.store.matchPropositionRows(
      subjectSlot.ids,
      objectSlot.ids,
      names,
    )
    // The same cap the top-level pattern applies: an endpoint pattern with a
    // free predicate and free endpoints scans the whole edge table, and this
    // engine materializes the result inside a 128 MB isolate.
    if (rowIds.length > MAX_ROW_MATCHES) {
      throw queryTooComplex(
        `nested proposition endpoint matches ${rowIds.length} rows, over the ` +
          `engine cap of ${MAX_ROW_MATCHES}; constrain an endpoint or the predicate`,
      )
    }
    const rows = this.store.getPropositions(rowIds)
    const out: EntityID[] = []
    for (const id of rowIds) {
      const row = rows.get(id)
      if (!row) continue
      for (const [name] of row.links) {
        if (names && !names.includes(name)) continue
        out.push(propositionID(row.id, name))
        if (out.length > MAX_ROW_MATCHES) {
          throw queryTooComplex(
            `nested proposition endpoint produces more than ` +
              `${MAX_ROW_MATCHES} links`,
          )
        }
      }
    }
    return out
  }

  // -------------------------------------------------------------------
  // FILTER / NOT / OPTIONAL / UNION
  // -------------------------------------------------------------------

  private executeFilter(expr: FilterExpression, ctx: SolutionContext): void {
    const vars = new Set<string>()
    collectFilterVars(expr, vars)

    if (vars.size === 0) {
      // Constant expression: true is a no-op, false discards every solution.
      // Rows are cleared in place so the columns stay bound — replacing the
      // forest would let a later clause silently rebuild the domains
      // (`kql.rs` `execute_filter_clause_inner`).
      if (!this.evalFilter(expr, new Map())) {
        for (const table of ctx.tables) table.rows.length = 0
      }
      return
    }

    for (const v of vars) {
      if (!ctx.boundVars().has(v)) {
        throw referenceError(
          `FILTER references ?${v}, which is not bound by any preceding clause`,
        )
      }
    }

    const table = ctx.joinCovering([...vars])
    // Memoize on the binding tuple: repeated bindings are common after a
    // join, and each evaluation may load an entity row.
    const memo = new Map<string, boolean>()
    table.retain((row) => {
      const env = new Map<string, BindingValue>()
      for (const v of vars) {
        const col = table.column(v)
        if (col !== null) env.set(v, row[col]!)
      }
      const key = rowKeyOf([...vars].map((v) => env.get(v)))
      let verdict = memo.get(key)
      if (verdict === undefined) {
        verdict = this.evalFilter(expr, env)
        memo.set(key, verdict)
      }
      return verdict
    })
  }

  private executeNot(block: WhereClause[], ctx: SolutionContext): void {
    // Grounding is lenient inside the block (KIP §3.4.7.1): a dangling id
    // makes the NOT pattern unmatchable, i.e. the clause succeeds and
    // excludes nothing — it must not abort the query.
    const child = new SolutionContext()
    child.lenient = true
    // Seed the child with the *domains* of shared variables, not their
    // correlations: NOT tests existence of the pattern, and carrying the
    // outer row structure in would make it test the joined shape instead.
    // `distinctValues` keeps predicate bindings visible, so a NOT block
    // correlated on a predicate variable can still exclude.
    const outerVars = ctx.boundVars()
    const blockVars = new Set<string>()
    collectClauseVars(block, blockVars)
    const shared = [...blockVars].filter((v) => outerVars.has(v))

    for (const v of shared) {
      const table = ctx.find(v)!
      child.mergeTable(SolutionTable.single(v, table.distinctValues(v)))
    }

    this.executeWhere(block, child)

    // NOT is a pure filter: with no outer-bound variable referenced it
    // cannot exclude anything (`kql.rs:396-398`).
    if (shared.length === 0) return

    // The block's clauses are conjunctive, so a single empty table means the
    // whole block pattern is unsatisfiable and therefore excludes nothing —
    // even when a *different*, disconnected table inside it did match
    // (`kql.rs:400-403`). Without this the surviving table's rows would read
    // as exclusions the block never actually produced.
    if (child.isUnsatisfiable) return

    /** The shared tuple of one row, or `null` when a position is unbound. */
    const tuple = (
      table: SolutionTable,
      row: readonly BindingValue[],
    ): string | null => {
      const cells: BindingValue[] = []
      for (const v of shared) {
        const col = table.column(v)
        // A padded or absent position carries no identity, so it neither
        // contributes an exclusion nor can be excluded (`kql.rs:417-421`).
        if (col === null || row[col]!.kind === 'null') return null
        cells.push(row[col]!)
      }
      return rowKeyOf(cells)
    }

    const excluded = new Set<string>()
    const joined = child.joinCovering(shared)
    for (const row of joined.rows) {
      const key = tuple(joined, row)
      if (key !== null) excluded.add(key)
    }
    if (excluded.size === 0) return

    const outer = ctx.joinCovering(shared)
    outer.retain((row) => {
      const key = tuple(outer, row)
      return key === null || !excluded.has(key)
    })
  }

  private executeOptional(block: WhereClause[], ctx: SolutionContext): void {
    // Grounding is lenient inside the block (KIP §3.4.7.2): a dangling id
    // makes the optional pattern unmatchable — the outer solutions are kept
    // and the block's variables project `null`.
    const child = new SolutionContext()
    child.lenient = true
    const outerVars = ctx.boundVars()
    const blockVars = new Set<string>()
    collectClauseVars(block, blockVars)
    const seed = [...blockVars].filter((v) => outerVars.has(v))

    for (const v of seed) {
      const table = ctx.find(v)!
      child.mergeTable(SolutionTable.single(v, table.distinctValues(v)))
    }
    this.executeWhere(block, child)

    // A block that bound nothing at all extends nothing (`kql.rs:600-604`).
    if (child.tables.length === 0) return
    const blockTable = child.joinCovering([...blockVars])
    if (blockTable.vars.length === 0) return

    // Shared variables are re-derived from the *materialized* block: a
    // seeded variable whose column survived is the join anchor.
    const shared = blockTable.vars.filter((v) => outerVars.has(v))
    if (shared.length === 0) {
      // No outer anchor: new bindings extend every outer solution. With no
      // block solution the new variables project `null` for every outer row
      // rather than annihilating them (`kql.rs:612-618`).
      if (blockTable.rows.length === 0) {
        blockTable.rows.push(blockTable.vars.map(() => NULL_BINDING))
      }
      ctx.mergeTable(blockTable)
      return
    }
    const outer = ctx.joinCovering(shared)
    const merged = outer.leftJoin(blockTable)
    ctx.tables = [...ctx.tables.filter((t) => t !== outer), merged]
  }

  private executeUnion(block: WhereClause[], ctx: SolutionContext): void {
    // A UNION branch runs in a fresh scope: outer bindings are deliberately
    // invisible so the branch describes an independent alternative. Grounding
    // is lenient (KIP §3.4.7.3): a dangling id makes the branch contribute
    // nothing instead of failing the query.
    const child = new SolutionContext()
    child.lenient = true
    this.executeWhere(block, child)
    const branchVars = new Set<string>()
    collectClauseVars(block, branchVars)
    const branch =
      child.tables.length === 0
        ? SolutionTable.empty([...branchVars])
        : child.joinCovering([...branchVars])

    if (ctx.tables.length === 0) {
      ctx.tables = [branch]
      return
    }
    const allVars = new Set<string>()
    for (const t of ctx.tables) for (const v of t.vars) allVars.add(v)
    const outer = ctx.joinCovering([...allVars])
    ctx.tables = [outer.union(branch)]
  }

  // -------------------------------------------------------------------
  // Filter evaluation
  // -------------------------------------------------------------------

  private evalFilter(
    expr: FilterExpression,
    env: Map<string, BindingValue>,
  ): boolean {
    if ('Comparison' in expr) {
      const left = this.resolveOperand(expr.Comparison.left, env)
      const right = this.resolveOperand(expr.Comparison.right, env)
      return compare(left, expr.Comparison.operator, right)
    }
    if ('Logical' in expr) {
      const left = this.evalFilter(expr.Logical.left, env)
      if (expr.Logical.operator === 'And') {
        return left && this.evalFilter(expr.Logical.right, env)
      }
      return left || this.evalFilter(expr.Logical.right, env)
    }
    if ('Not' in expr) return !this.evalFilter(expr.Not, env)
    if ('Function' in expr) {
      const args = expr.Function.args.map((a) => this.resolveOperand(a, env))
      const subject = args[0]
      const argument = args[1]

      // These three take a non-string subject, so they are handled before the
      // string-only guard below.
      switch (expr.Function.func) {
        case 'IsNull':
          return subject === null || subject === undefined
        case 'IsNotNull':
          return subject !== null && subject !== undefined
        case 'In':
          return (
            Array.isArray(argument) && argument.some((v) => deepEqual(subject, v))
          )
        default:
          break
      }

      if (typeof subject !== 'string' || typeof argument !== 'string') {
        return false
      }
      switch (expr.Function.func) {
        case 'Contains':
          return subject.includes(argument)
        case 'StartsWith':
          return subject.startsWith(argument)
        case 'EndsWith':
          return subject.endsWith(argument)
        case 'Regex':
          // Constructed per call rather than cached: a cache keyed on the
          // pattern is only a win when the same FILTER runs many times, and
          // the binding memo above already collapses that case.
          try {
            return new RegExp(argument).test(subject)
          } catch (err) {
            throw referenceError(
              `invalid REGEX pattern ${JSON.stringify(argument)}: ${(err as Error).message}`,
            )
          }
        default:
          throw internalError(
            `unhandled FILTER function: ${expr.Function.func}`,
          )
      }
    }
    throw internalError(
      `unhandled FILTER expression: ${Object.keys(expr).join(', ')}`,
    )
  }

  private resolveOperand(
    operand: FilterOperand,
    env: Map<string, BindingValue>,
  ): unknown {
    if ('Literal' in operand) return kipValueToJson(operand.Literal)
    if ('List' in operand) return operand.List.map(kipValueToJson)
    if ('Variable' in operand) {
      const binding = env.get(operand.Variable.var)
      if (!binding) return null
      return this.loadField(binding, operand.Variable.path)
    }
    throw internalError(
      `unhandled FILTER operand: ${Object.keys(operand).join(', ')}`,
    )
  }

  /** Reads a dot path off a bound entity, e.g. `?d.attributes.risk`. */
  loadField(binding: BindingValue, path: readonly string[]): unknown {
    if (binding.kind === 'null') return null
    if (binding.kind === 'predicate') {
      // A predicate variable has no fields; only its bare value is readable.
      return path.length === 0 ? binding.name : null
    }
    const entity = binding.id
    let root: Record<string, unknown>
    if (entity.kind === 'concept') {
      const concept = this.store.getConcept(entity.id)
      if (!concept) return null
      if (path.length === 0) return formatEntityID(entity)
      root = {
        id: formatEntityID(entity),
        type: concept.type,
        name: concept.name,
        attributes: concept.attributes,
        metadata: concept.metadata,
      }
    } else {
      const row = this.store.getProposition(entity.id)
      const link = row?.links.get(entity.predicate)
      if (!row || !link) return null
      if (path.length === 0) return formatEntityID(entity)
      root = {
        id: formatEntityID(entity),
        subject: formatEntityID(row.subject),
        predicate: entity.predicate,
        object: formatEntityID(row.object),
        attributes: link.attributes,
        metadata: link.metadata,
      }
    }
    return getPath(root, path)
  }
}

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

/** Cap on rows a single proposition pattern may materialize. */
const MAX_ROW_MATCHES = 65_536
const MAX_SOLUTION_ROWS = 65_536

function bindingsDiffer(a: BindingValue, b: BindingValue): boolean {
  return bindingKey(a) !== bindingKey(b)
}

function predicateCandidates(predicate: PredTerm): string[] | null {
  if ('Literal' in predicate) return [predicate.Literal]
  if ('Alternative' in predicate) return predicate.Alternative
  return null
}

export function collectFilterVars(
  expr: FilterExpression,
  out: Set<string>,
): void {
  if ('Comparison' in expr) {
    collectOperandVars(expr.Comparison.left, out)
    collectOperandVars(expr.Comparison.right, out)
  } else if ('Logical' in expr) {
    collectFilterVars(expr.Logical.left, out)
    collectFilterVars(expr.Logical.right, out)
  } else if ('Not' in expr) {
    collectFilterVars(expr.Not, out)
  } else if ('Function' in expr) {
    for (const arg of expr.Function.args) collectOperandVars(arg, out)
  }
}

function collectOperandVars(operand: FilterOperand, out: Set<string>): void {
  if ('Variable' in operand) out.add(operand.Variable.var)
}

export function collectClauseVars(
  clauses: readonly WhereClause[],
  out: Set<string>,
): void {
  for (const clause of clauses) {
    if ('Concept' in clause) {
      out.add(clause.Concept.variable)
    } else if ('Proposition' in clause) {
      if (clause.Proposition.variable) out.add(clause.Proposition.variable)
      collectMatcherVars(clause.Proposition.matcher, out)
    } else if ('Filter' in clause) {
      collectFilterVars(clause.Filter.expression, out)
    } else if ('Not' in clause) {
      collectClauseVars(clause.Not, out)
    } else if ('Optional' in clause) {
      collectClauseVars(clause.Optional, out)
    } else if ('Union' in clause) {
      collectClauseVars(clause.Union, out)
    }
  }
}

function collectMatcherVars(
  matcher: PropositionMatcher,
  out: Set<string>,
): void {
  if ('ID' in matcher) return
  collectTermVars(matcher.Object.subject, out)
  collectTermVars(matcher.Object.object, out)
  if ('Variable' in matcher.Object.predicate) {
    out.add(matcher.Object.predicate.Variable)
  }
}

function collectTermVars(term: TargetTerm, out: Set<string>): void {
  if ('Variable' in term) out.add(term.Variable)
  else if ('Proposition' in term) collectMatcherVars(term.Proposition, out)
}

export function kipValueToJson(value: KipValue): Json {
  if (value === 'Null') return null
  if (typeof value !== 'object') return value as Json
  if ('Bool' in value) return value.Bool
  if ('Number' in value) return value.Number
  if ('String' in value) return value.String
  if ('Array' in value) return value.Array.map(kipValueToJson)
  if ('Object' in value) {
    const out: Record<string, Json> = {}
    for (const [k, v] of Object.entries(value.Object)) out[k] = kipValueToJson(v)
    return out
  }
  throw internalError(`unhandled KIP value: ${Object.keys(value).join(', ')}`)
}

function getPath(root: unknown, path: readonly string[]): unknown {
  let cursor: unknown = root
  for (const segment of path) {
    if (cursor === null || typeof cursor !== 'object') return null
    cursor = (cursor as Record<string, unknown>)[segment]
    if (cursor === undefined) return null
  }
  return cursor ?? null
}

function compare(left: unknown, op: string, right: unknown): boolean {
  switch (op) {
    case 'Equal':
      return deepEqual(left, right)
    case 'NotEqual':
      return !deepEqual(left, right)
    default:
      break
  }
  // Ordered comparisons are only meaningful between two numbers or two
  // strings. Anything else (null from an absent attribute, an object) is
  // false rather than coerced — JS coercion would make `null < 3` true and
  // silently include rows with no value at all.
  if (typeof left === 'number' && typeof right === 'number') {
    return applyOrder(left, right, op)
  }
  if (typeof left === 'string' && typeof right === 'string') {
    return applyOrder(left, right, op)
  }
  return false
}

function applyOrder<T extends number | string>(
  left: T,
  right: T,
  op: string,
): boolean {
  switch (op) {
    case 'LessThan':
      return left < right
    case 'LessEqual':
      return left <= right
    case 'GreaterThan':
      return left > right
    case 'GreaterEqual':
      return left >= right
    default:
      throw internalError(`unhandled comparison operator: ${op}`)
  }
}

function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true
  if (a === null || b === null) return false
  if (typeof a !== typeof b) return false
  if (typeof a !== 'object') return false
  if (Array.isArray(a) !== Array.isArray(b)) return false
  if (Array.isArray(a) && Array.isArray(b)) {
    return a.length === b.length && a.every((v, i) => deepEqual(v, b[i]))
  }
  const ka = Object.keys(a as object)
  const kb = Object.keys(b as object)
  return (
    ka.length === kb.length &&
    ka.every((k) =>
      deepEqual(
        (a as Record<string, unknown>)[k],
        (b as Record<string, unknown>)[k],
      ),
    )
  )
}

export { compareEntityID }
