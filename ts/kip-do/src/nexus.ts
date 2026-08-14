/**
 * The KIP engine for one Durable Object.
 *
 * ## Why there is still a write lock
 *
 * A Durable Object is single-threaded and globally unique, which removes most
 * of the coordination the Rust engine needs. It does *not* remove all of it.
 * Tokenization is an external service call, so a KML statement is
 * read → `await tokenize` → write. Awaiting opens the input gate, and another
 * KML statement can interleave in that window and invalidate the state the
 * first one read. The lock below closes exactly that window; the write itself
 * is atomic through `transactionSync`.
 *
 * If tokenization ever moves in-process, the lock can go: everything else in
 * a KML statement is synchronous.
 *
 * KQL and META take no lock — they never await mid-statement, so a query runs
 * to completion on a single consistent snapshot for free.
 */

import type {
  Command,
  DescribeTarget,
  Json,
  KqlQuery,
  MetaCommand,
  SearchCommand,
  UpsertBlock,
  WhereClause,
} from './kip/ast.js'
import {
  parseKip,
  parseKipAll,
  parserVersion,
  specRevision,
} from './kip/parser.js'
import {
  type EntityID,
  type JsonMap,
  compareEntityID,
  conceptID,
  conceptNode,
  formatEntityID,
  propositionLink,
} from './entity.js'
import { KipError, internalError, notFound, referenceError } from './errors.js'
import { KmlExecutor, conceptTokenKey, type TokenMap } from './exec/kml.js'
import { KqlExecutor, collectClauseVars } from './exec/kql.js'
import {
  SolutionContext,
  type BindingValue,
  rowKeyOf,
} from './exec/solution.js'
import { BUNDLED_CAPSULES, capsuleHash } from './capsules.js'
import { SCHEMA_VERSION, applySchema, metaGet, metaSet } from './schema.js'
import { Store } from './store.js'
import { ftsQuote } from './sql.js'
import {
  type Tokenizer,
  type TokenizeResult,
  extractJsonText,
} from './tokenizer.js'

/** KIP response envelope. */
export type KipResponse =
  | { result: Json; next_cursor?: string | null }
  | { error: ReturnType<KipError['toJSON']> }

export interface NexusOptions {
  /** Segmentation authority for full-text search. */
  tokenizer: Tokenizer
  /** Injectable clock, for deterministic tests. */
  now?: () => number
}

/**
 * Version of all persistent state owned by the engine bootstrap.
 *
 * Capsule hashes make content changes self-versioning, so updating a bundled
 * `.kip` file cannot accidentally leave an existing Durable Object on the old
 * definitions. Capsule order is included because later capsules may depend on
 * earlier ones.
 */
export const BOOTSTRAP_VERSION = [
  `schema:${SCHEMA_VERSION}`,
  ...BUNDLED_CAPSULES.map(
    (capsule) => `capsule:${capsule.name}:${capsuleHash(capsule.source)}`,
  ),
].join('|')

/** Transaction runner — `ctx.storage.transactionSync` in production. */
export type TransactionRunner = <T>(fn: () => T) => T

export class CognitiveNexus {
  readonly store: Store
  private readonly kql: KqlExecutor
  private readonly kml: KmlExecutor
  private readonly tokenizer: Tokenizer
  private readonly transact: TransactionRunner
  /** Serializes KML statements across their `await` on the tokenizer. */
  private writeChain: Promise<unknown> = Promise.resolve()

  constructor(
    private readonly sql: SqlStorage,
    transact: TransactionRunner,
    options: NexusOptions,
  ) {
    this.transact = transact
    this.tokenizer = options.tokenizer
    this.store = new Store(sql)
    this.kql = new KqlExecutor(this.store)
    this.kml = new KmlExecutor(this.store, options.now ?? (() => Date.now()))
  }

  /** Creates tables and indexes. Idempotent; safe to retry after interruption. */
  static bootstrap(sql: SqlStorage): void {
    applySchema(sql)
  }

  /**
   * Applies the bundled bootstrap capsules.
   *
   * KIP is schema-first, so a database with no `$ConceptType` definitions
   * cannot accept any write at all — this is what makes a fresh object
   * usable. Each capsule is skipped when its content hash is unchanged *and*
   * its anchor type exists; the anchor check is what repairs a database whose
   * bootstrap was interrupted midway.
   *
   * Runs outside the tokenizer path: capsule concepts are indexed lazily by
   * `reindexStale`, so bootstrap never blocks on a network round trip.
   */
  applyBundledCapsules(): void {
    for (const capsule of BUNDLED_CAPSULES) {
      const key = `capsule_hash:${capsule.name}`
      const hash = capsuleHash(capsule.source)
      const anchorPresent =
        this.store.findConceptByTypeName('$ConceptType', capsule.anchor) !== null
      if (metaGet(this.sql, key) === hash && anchorPresent) continue

      // Capsules create the very nodes the protected-scope guard defends, so
      // they run privileged. The flag is cleared in `finally` so a capsule
      // that throws cannot leave the engine permissive.
      this.kml.privileged = true
      try {
        // A capsule is a sequence of UPSERT blocks, which the grammar reads as
        // one command; `parseKipAll` splits only where a genuinely new command
        // starts, so a `{` inside a string can no longer cut a block in half.
        for (const command of parseKipAll(capsule.source)) {
          if (!('Kml' in command)) {
            throw internalError(
              `capsule ${capsule.name} contains a non-KML statement`,
            )
          }
          this.transact(() =>
            this.kml.execute(command.Kml, new Map(), 'bootstrap'),
          )
        }
      } finally {
        this.kml.privileged = false
      }
      metaSet(this.sql, key, hash)
    }
  }

  /** Parses and executes one KIP command. Never throws. */
  async execute(source: string): Promise<KipResponse> {
    try {
      return await this.run(parseKip(source))
    } catch (err) {
      return { error: KipError.from(err).toJSON() }
    }
  }

  /**
   * Runs an already-parsed command.
   *
   * `searchTokens` carries the one piece of async state a command may need —
   * a tokenized SEARCH term — as an argument rather than as engine state. It
   * has to travel with the command: `prime` awaits the tokenizer, which opens
   * the Durable Object's input gate, so a field would be readable (and
   * overwritable) by a concurrently arriving request.
   */
  async executeCommand(
    command: Command,
    searchTokens: string[] | null = null,
  ): Promise<KipResponse> {
    try {
      if ('Kql' in command) {
        const { result, cursor } = this.executeKql(command.Kql)
        return { result, next_cursor: cursor }
      }
      if ('Kml' in command) {
        return { result: await this.executeKml(command) }
      }
      if ('Meta' in command) {
        const { result, cursor } = this.executeMeta(command.Meta, searchTokens)
        return { result, next_cursor: cursor }
      }
      throw internalError(
        `unhandled command: ${Object.keys(command).join(', ')}`,
      )
    } catch (err) {
      return { error: KipError.from(err).toJSON() }
    }
  }

  // -------------------------------------------------------------------
  // KQL
  // -------------------------------------------------------------------

  private executeKql(query: KqlQuery): { result: Json; cursor: string | null } {
    const ctx = new SolutionContext()
    this.kql.executeWhere(query.where_clauses, ctx)
    return this.project(query, ctx)
  }

  /**
   * FIND projection.
   *
   * Supports the single- and multi-variable plain forms and global
   * aggregation. Cursors are numeric offsets over a deterministic order; see
   * README "Known divergences" for how this differs from the Rust engine's
   * five cursor schemes.
   */
  private project(
    query: KqlQuery,
    ctx: SolutionContext,
  ): { result: Json; cursor: string | null } {
    const expressions = query.find_clause.expressions
    const vars = new Set<string>()
    for (const expr of expressions) {
      if ('Variable' in expr) vars.add(expr.Variable.var)
      else vars.add(expr.Aggregation.var.var)
    }

    // A FIND column naming a variable that no clause mentions cannot be
    // projected. The check is against the *clauses*, not the runtime tables:
    // an unsatisfiable WHERE empties every table, and that is "no matches",
    // not a malformed query.
    const declared = new Set<string>()
    collectClauseVars(query.where_clauses, declared)
    for (const v of vars) {
      if (!declared.has(v)) {
        throw referenceError(`Unbound variable: ${JSON.stringify(v)}`)
      }
    }

    // Global aggregation collapses the solution set to one row. Each
    // aggregate is evaluated over the table covering *its own* variable, not
    // over a join of all of them: `COUNT(?a)` with an unrelated `?b` in the
    // WHERE would otherwise be multiplied by however many `?b` there are.
    const aggregations = expressions.filter((e) => 'Aggregation' in e)
    if (aggregations.length > 0 && aggregations.length === expressions.length) {
      const row: Json[] = expressions.map((expr) => {
        if (!('Aggregation' in expr)) throw internalError('mixed aggregation')
        const own = ctx.find(expr.Aggregation.var.var)
        return this.aggregate(
          expr.Aggregation,
          own ?? ctx.joinCovering([expr.Aggregation.var.var]),
        )
      })
      return { result: row.length === 1 ? row[0]! : row, cursor: null }
    }

    const table = ctx.joinCovering([...vars])
    if (aggregations.length > 0) {
      throw referenceError(
        'mixing aggregate and plain columns in FIND requires a grouping key, ' +
          'which this engine does not yet implement',
      )
    }

    // Group FIND expressions by variable, preserving first-appearance order.
    // `FIND(?d.name, ?d.attributes.risk)` names one variable twice, which is a
    // single column whose entries are `[name, risk]` pairs — not two columns.
    // This mirrors `collect_find_items` in the Rust engine.
    const items: { var: string; paths: string[][] }[] = []
    for (const expr of expressions) {
      if (!('Variable' in expr)) continue
      const existing = items.find((i) => i.var === expr.Variable.var)
      if (existing) existing.paths.push(expr.Variable.path)
      else items.push({ var: expr.Variable.var, paths: [expr.Variable.path] })
    }

    // Materialize solution rows, deduplicated, then sort if ORDER BY asked.
    type Row = { cells: BindingValue[]; sort: unknown[] }
    const rows: Row[] = []
    const seen = new Set<string>()
    for (const row of table.rows) {
      const cells = items.map((item) => {
        const col = table.column(item.var)
        return col === null ? ({ kind: 'null' } as BindingValue) : row[col]!
      })
      const key = rowKeyOf(cells)
      if (seen.has(key)) continue
      seen.add(key)

      const sort = (query.order_by ?? []).map((order) => {
        const col = table.column(order.variable.var)
        return col === null
          ? null
          : this.kql.loadField(row[col]!, order.variable.path)
      })
      rows.push({ cells, sort })
    }

    const orderBy = query.order_by ?? []
    if (orderBy.length > 0) {
      rows.sort((a, b) => {
        for (let i = 0; i < orderBy.length; i++) {
          const av = a.sort[i]
          const bv = b.sort[i]
          // Absent keys sort last in both directions: a row with no value for
          // the sort field is unranked, not "smallest", and burying it at the
          // top of an ASC page would hide the rows the caller asked for.
          const aNull = av === null || av === undefined
          const bNull = bv === null || bv === undefined
          if (aNull || bNull) {
            if (aNull && bNull) continue
            return aNull ? 1 : -1
          }
          const direction = orderBy[i]!.direction === 'Desc' ? -1 : 1
          const cmp = direction * compareSortKeys(av, bv)
          if (cmp !== 0) return cmp
        }
        return 0
      })
    }

    const offset = parseOffsetCursor(query.cursor)
    const limit = query.limit ?? rows.length
    const page = rows.slice(offset, offset + limit)
    const nextOffset = offset + page.length
    const cursor = nextOffset < rows.length ? String(nextOffset) : null

    /** Renders one item's value for one row: a bare value, or a tuple when
     * the same variable was projected through several dot paths. */
    const cell = (item: { paths: string[][] }, binding: BindingValue): Json => {
      if (item.paths.length === 1) {
        return this.renderColumn(binding, item.paths[0]!)
      }
      return item.paths.map((path) => this.renderColumn(binding, path))
    }

    // A single projected variable yields a flat list of values. Several yield
    // one array *per column*, index-aligned across columns — the shape the
    // Rust engine produces (`project_multi_var` pushes one `Json::Array` per
    // FIND item). Returning rows instead would be a different wire shape for
    // every multi-column query.
    if (items.length === 1) {
      const item = items[0]!
      return {
        result: page.map((row) => cell(item, row.cells[0]!)),
        cursor,
      }
    }

    const columns: Json[] = items.map((item, index) =>
      page.map((row) => cell(item, row.cells[index]!)),
    )
    return { result: columns, cursor }
  }

  /** Renders one projected column: a full node when bare, a field otherwise. */
  private renderColumn(binding: BindingValue, path: readonly string[]): Json {
    if (binding.kind === 'null') return null
    if (binding.kind === 'predicate') {
      return path.length === 0 ? binding.name : null
    }
    if (path.length > 0) {
      return (this.kql.loadField(binding, path) ?? null) as Json
    }
    const entity = binding.id
    if (entity.kind === 'concept') {
      const concept = this.store.getConcept(entity.id)
      return concept ? (conceptNode(concept) as Json) : null
    }
    const row = this.store.getProposition(entity.id)
    if (!row) return null
    return (propositionLink(row, entity.predicate) ?? null) as Json
  }

  private aggregate(
    agg: { func: string; var: { var: string; path: string[] }; distinct: boolean },
    table: ReturnType<SolutionContext['joinCovering']>,
  ): Json {
    const col = table.column(agg.var.var)
    if (col === null) return agg.func === 'Count' ? 0 : null

    const values: unknown[] = []
    const seen = new Set<string>()
    for (const row of table.rows) {
      const value = this.kql.loadField(row[col]!, agg.var.path)
      if (value === null || value === undefined) continue
      if (agg.distinct) {
        const key = JSON.stringify(value)
        if (seen.has(key)) continue
        seen.add(key)
      }
      values.push(value)
    }

    switch (agg.func) {
      case 'Count':
        return values.length
      case 'Sum':
        return values.reduce<number>((s, v) => s + toNumber(v), 0)
      case 'Avg':
        return values.length === 0
          ? null
          : values.reduce<number>((s, v) => s + toNumber(v), 0) / values.length
      case 'Min':
        return values.length === 0
          ? null
          : (values.reduce((a, b) => (compareSortKeys(a, b) <= 0 ? a : b)) as Json)
      case 'Max':
        return values.length === 0
          ? null
          : (values.reduce((a, b) => (compareSortKeys(a, b) >= 0 ? a : b)) as Json)
      default:
        throw internalError(`unhandled aggregation: ${agg.func}`)
    }
  }

  // -------------------------------------------------------------------
  // KML
  // -------------------------------------------------------------------

  /**
   * Runs a KML statement: resolve tokens, then apply atomically.
   *
   * The statement is queued on `writeChain` so the read → tokenize → write
   * sequence is never interleaved with another mutation. The queue is a
   * promise chain rather than a semaphore because ordering matters: two
   * UPSERTs on the same concept must apply in arrival order.
   */
  private executeKml(command: Extract<Command, { Kml: unknown }>): Promise<Json> {
    const run = async (): Promise<Json> => {
      const tokenized = await this.resolveTokens(command.Kml)
      return this.transact(() =>
        this.kml.execute(command.Kml, tokenized.tokens, tokenized.version) as Json,
      )
    }
    // Chain regardless of the previous statement's outcome, so one failure
    // does not wedge every subsequent write.
    const queued = this.writeChain.then(run, run)
    this.writeChain = queued.catch(() => undefined)
    return queued
  }

  /**
   * Pre-computes the tokens a statement will need.
   *
   * Only UPSERT introduces new searchable text; the other statements either
   * remove rows or edit fields whose post-state is derivable from the AST.
   * For anything not covered here the FTS row is left as-is and picked up by
   * `reindexStale`, which is why `tok_ver` exists.
   */
  private async resolveTokens(
    statement: Extract<Command, { Kml: unknown }>['Kml'],
  ): Promise<{ tokens: TokenMap; version: string }> {
    const pending = new Map<
      string,
      {
        name: string
        attributes: Record<string, unknown>
        metadata: Record<string, unknown>
      }
    >()

    if ('Upsert' in statement) {
      for (const block of statement.Upsert as UpsertBlock[]) {
        for (const item of block.items) {
          if (!('Concept' in item)) continue
          const matcher = item.Concept.concept
          if (!('Object' in matcher)) continue
          const { type, name } = matcher.Object
          const key = conceptTokenKey(type, name)
          let projected = pending.get(key)
          if (!projected) {
            const existingId = this.store.findConceptByTypeName(type, name)
            const existing =
              existingId === null ? null : this.store.getConcept(existingId)
            projected = {
              name,
              attributes: { ...(existing?.attributes ?? {}) },
              metadata: { ...(existing?.metadata ?? {}) },
            }
            pending.set(key, projected)
          }
          projected.attributes = {
            ...projected.attributes,
            ...(item.Concept.set_attributes ?? {}),
          }
          projected.metadata = {
            ...projected.metadata,
            ...(block.metadata ?? {}),
            ...(item.Concept.metadata ?? {}),
          }
        }
      }
    }

    if (pending.size === 0) {
      return { tokens: new Map(), version: this.tokenizerVersion() }
    }

    const keys = [...pending.keys()]
    const texts = [...pending.values()].map((projected) =>
      [
        projected.name,
        ...extractJsonText(projected.attributes),
        ...extractJsonText(projected.metadata),
      ].join(' '),
    )
    const result: TokenizeResult = await this.tokenizer.tokenize(texts)
    const tokens: TokenMap = new Map()
    for (let i = 0; i < keys.length; i++) {
      tokens.set(keys[i]!, result.tokens[i] ?? [])
    }
    return { tokens, version: result.version }
  }

  // -------------------------------------------------------------------
  // META
  // -------------------------------------------------------------------

  private executeMeta(
    command: MetaCommand,
    searchTokens: string[] | null,
  ): {
    result: Json
    cursor: string | null
  } {
    if ('Describe' in command) {
      return this.describe(command.Describe)
    }
    if ('Search' in command) {
      return { result: this.search(command.Search, searchTokens), cursor: null }
    }
    if ('Export' in command) {
      throw internalError(
        'EXPORT is not implemented in this engine yet; see README "Coverage"',
      )
    }
    throw internalError(
      `unhandled META command: ${Object.keys(command).join(', ')}`,
    )
  }

  /**
   * Every concept of one type, in id order.
   *
   * Batched through `getConcepts` rather than looked up one id at a time: a
   * `DESCRIBE PRIMER` covers three whole categories, and the Durable Object
   * serves them on its single thread.
   */
  private conceptsOfType(type: string) {
    const ids = this.store.conceptIdsByType(type)
    const byId = this.store.getConcepts(ids)
    return ids
      .map((id) => byId.get(id))
      .filter((c): c is NonNullable<typeof c> => !!c)
  }

  private describe(target: DescribeTarget): {
    result: Json
    cursor: string | null
  } {
    if (target === 'Primer') {
      const conceptTypes = this.conceptsOfType('$ConceptType').map((c) => c.name)
      const propositionTypes = this.conceptsOfType('$PropositionType').map(
        (c) => c.name,
      )
      const domains = this.conceptsOfType('Domain').map((c) => ({
        name: c.name,
        attributes: c.attributes as Json,
      }))

      return {
        result: {
          engine: '@ldclabs/kip-do',
          parser_version: parserVersion(),
          spec_revision: specRevision(),
          concept_types: conceptTypes,
          proposition_types: propositionTypes,
          domains,
          // Out-of-band capability advert (KIP §5.2.1). This engine has no
          // embedding store, so SEARCH degrades semantic/hybrid to keyword —
          // the same posture the Rust engine advertises.
          search_modes: ['keyword'],
        },
        cursor: null,
      }
    }
    if (target === 'Domains') {
      return {
        result: this.conceptsOfType('Domain').map((c) => conceptNode(c)) as Json,
        cursor: null,
      }
    }
    if (typeof target === 'object') {
      if ('ConceptTypes' in target) {
        return this.typeNames('$ConceptType', target.ConceptTypes)
      }
      if ('PropositionTypes' in target) {
        return this.typeNames('$PropositionType', target.PropositionTypes)
      }
      if ('ConceptType' in target) {
        return {
          result: this.typeDefinition('$ConceptType', target.ConceptType),
          cursor: null,
        }
      }
      if ('PropositionType' in target) {
        return {
          result: this.typeDefinition('$PropositionType', target.PropositionType),
          cursor: null,
        }
      }
    }
    throw internalError(`unhandled DESCRIBE target: ${JSON.stringify(target)}`)
  }

  private typeNames(
    metaType: string,
    page: { limit: number | null; cursor: string | null },
  ): { result: Json; cursor: string | null } {
    const names = this.conceptsOfType(metaType)
      .map((c) => c.name)
      .sort()
    const offset = parseOffsetCursor(page.cursor)
    const limit = page.limit ?? names.length
    const result = names.slice(offset, offset + limit)
    const nextOffset = offset + result.length
    return {
      result,
      cursor:
        limit > 0 && nextOffset < names.length ? String(nextOffset) : null,
    }
  }

  /**
   * A type definition is returned as a `ConceptInfo`, not a full node: it
   * carries identity and the declared schema, but not engine metadata, which
   * describes the definition row rather than the type it defines.
   */
  private typeDefinition(metaType: string, name: string): Json {
    const id = this.store.findConceptByTypeName(metaType, name)
    if (id === null) throw notFound(`${metaType} ${JSON.stringify(name)} not found`)
    const concept = this.store.requireConcept(id)
    return {
      id: formatEntityID(conceptID(concept.id)),
      type: concept.type,
      name: concept.name,
      attributes: concept.attributes as Json,
    }
  }

  /**
   * `SEARCH` — keyword retrieval over the FTS index.
   *
   * The query goes through the *same* tokenizer as the write path. That is
   * the whole point of the external service: a query tokenized differently
   * from the index cannot match it, and the failure is silent (empty results,
   * no error). Because tokenization is async and this method is sync, the
   * caller must have resolved the tokens first; see `run`.
   */
  private search(command: SearchCommand, tokens: string[] | null): Json {
    if (!tokens) {
      throw internalError(
        'SEARCH tokens were not resolved before execution; call execute() rather than executeCommand()',
      )
    }
    const limit = Math.min(command.limit ?? 100, 100)
    // Widen the candidate pool when a type filter will drop most hits, so a
    // rare type is not starved by common ones ranking above it.
    const topK = limit * (command.in_type ? 100 : 10)
    const threshold = command.threshold ?? 0

    if (tokens.length === 0) return []
    const query = ftsQuote(tokens)

    if (command.target === 'Concept') {
      const hits = this.store.searchConcepts(query, topK)
      // One batched load: with the x100 widening above, per-hit point
      // lookups would be thousands of SELECTs on the object's single thread.
      const byId = this.store.getConcepts(hits.map((hit) => hit.id))
      const out: Json[] = []
      for (const hit of hits) {
        const concept = byId.get(hit.id)
        if (!concept) continue
        if (command.in_type && concept.type !== command.in_type) continue
        const score = normalizeScore(hit.score)
        if (score < threshold) continue
        const node = conceptNode(concept)
        ;(node.metadata as JsonMap) = {
          ...(node.metadata as JsonMap),
          _score: score,
        }
        out.push(node as Json)
        if (out.length >= limit) break
      }
      return out
    }

    const hits = this.store.searchPropositions(
      query,
      topK,
      command.in_type ?? undefined,
    )
    const byId = this.store.getPropositions([
      ...new Set(hits.map((hit) => hit.id)),
    ])
    const out: Json[] = []
    for (const hit of hits) {
      const row = byId.get(hit.id)
      if (!row) continue
      const link = propositionLink(row, hit.predicate)
      if (!link) continue
      const score = normalizeScore(hit.score)
      if (score < threshold) continue
      ;(link.metadata as JsonMap) = {
        ...(link.metadata as JsonMap),
        _score: score,
      }
      out.push(link as Json)
      if (out.length >= limit) return out
    }
    return out
  }

  /**
   * Resolves the async state a command needs before its synchronous
   * execution: today that is only a tokenized SEARCH term. The result is
   * returned rather than stored, so two commands in flight over the same
   * object cannot see each other's tokens.
   */
  private async prime(command: Command): Promise<string[] | null> {
    if ('Meta' in command && 'Search' in command.Meta) {
      const result = await this.tokenizer.tokenize([command.Meta.Search.term])
      return result.tokens[0] ?? []
    }
    return null
  }

  /** Tokenizer version the index was last built with. */
  tokenizerVersion(): string {
    return metaGet(this.sql, 'tokenizer_version') ?? 'unknown'
  }

  /** Version reported by the tokenizer service that would handle a request now. */
  async liveTokenizerVersion(): Promise<string> {
    const result = await this.tokenizer.tokenize([''])
    return result.version
  }

  /**
   * Re-tokenizes rows whose `tok_ver` does not match the live service.
   *
   * Two things drive rows here: a proposition write (whose text is only
   * knowable after the row exists, so it is never tokenized inline), and a
   * `TOKENIZER_VERSION` bump, after which every previously indexed row holds
   * tokens from an incomparable vocabulary.
   *
   * Call it from an alarm, not from the request path: it is a bounded batch
   * precisely so it can be run repeatedly until it returns 0 without holding
   * the object's single thread for long.
   */
  async reindexStale(currentVersion: string, batch = 128): Promise<number> {
    // Queued on the same chain as KML writes: the read → tokenize → write
    // sequence crosses an `await`, and a mutation landing in that window
    // would be clobbered by FTS tokens computed from its pre-write text —
    // stamped with the current version, so the row would never self-heal.
    const run = () => this.reindexStaleInner(currentVersion, batch)
    const queued = this.writeChain.then(run, run)
    this.writeChain = queued.catch(() => undefined)
    return queued
  }

  private async reindexStaleInner(
    currentVersion: string,
    batch: number,
  ): Promise<number> {
    let done = 0

    const conceptIds = this.store.staleConceptIds(currentVersion, batch)
    if (conceptIds.length > 0) {
      const concepts = this.store.getConcepts(conceptIds)
      const order: number[] = []
      const texts: string[] = []
      for (const id of conceptIds) {
        const concept = concepts.get(id)
        if (!concept) continue
        order.push(id)
        texts.push(
          [
            concept.name,
            ...extractJsonText(concept.attributes),
            ...extractJsonText(concept.metadata),
          ].join(' '),
        )
      }
      const result = await this.tokenizer.tokenize(texts)
      this.transact(() => {
        for (let i = 0; i < order.length; i++) {
          this.store.setConceptFts(
            order[i]!,
            result.tokens[i] ?? [],
            result.version,
          )
        }
      })
      metaSet(this.sql, 'tokenizer_version', result.version)
      done += order.length
    }

    const remaining = batch - done
    if (remaining <= 0) return done

    const propIds = this.store.stalePropositionIds(currentVersion, remaining)
    if (propIds.length > 0) {
      const rows = this.store.getPropositions(propIds)
      const links: { id: number; predicate: string }[] = []
      const texts: string[] = []
      for (const id of propIds) {
        const row = rows.get(id)
        if (!row) continue
        for (const [predicate, props] of row.links) {
          links.push({ id, predicate })
          texts.push(
            [
              predicate,
              ...extractJsonText(props.attributes),
              ...extractJsonText(props.metadata),
            ].join(' '),
          )
        }
      }
      const result = await this.tokenizer.tokenize(texts)
      this.transact(() => {
        const byRow = new Map<
          number,
          { predicate: string; tokens: readonly string[] }[]
        >()
        for (let i = 0; i < links.length; i++) {
          const link = links[i]!
          const entries = byRow.get(link.id)
          const entry = {
            predicate: link.predicate,
            tokens: result.tokens[i] ?? [],
          }
          if (entries) entries.push(entry)
          else byRow.set(link.id, [entry])
        }
        for (const [id, entries] of byRow) {
          this.store.setPropositionFts(id, entries, result.version)
        }
      })
      metaSet(this.sql, 'tokenizer_version', result.version)
      done += new Set(links.map((link) => link.id)).size
    }

    return done
  }

  /** Public wrapper that primes async state before synchronous execution. */
  async run(command: Command): Promise<KipResponse> {
    let searchTokens: string[] | null
    try {
      searchTokens = await this.prime(command)
    } catch (err) {
      return { error: KipError.from(err).toJSON() }
    }
    return this.executeCommand(command, searchTokens)
  }
}

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

/**
 * Numeric offset cursor.
 *
 * A non-decimal token is rejected rather than silently treated as offset 0:
 * quietly restarting pagination hands the client duplicate pages, which is
 * worse than an error it can act on.
 */
export function parseOffsetCursor(cursor: string | null | undefined): number {
  if (cursor == null) return 0
  if (!/^\d+$/.test(cursor)) {
    throw new KipError(
      'KIP_1001',
      `invalid cursor ${JSON.stringify(cursor)}; expected a decimal offset`,
    )
  }
  return Number(cursor)
}

/**
 * Maps a raw BM25 score into `[0, 1)`.
 *
 * Corpus-independent saturation, matching the shape the Rust engine uses
 * (`helper.rs:69-88`). The absolute values differ because FTS5's BM25 is
 * scaled differently from `anda_db`'s — a `THRESHOLD` calibrated against the
 * Rust engine does not carry over. See README "Known divergences".
 */
export function normalizeScore(raw: number): number {
  const score = Math.max(raw, 0)
  return score / (score + 2)
}

function toNumber(value: unknown): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : 0
}


function compareSortKeys(a: unknown, b: unknown): number {
  if (a === null || a === undefined) return b === null || b === undefined ? 0 : -1
  if (b === null || b === undefined) return 1
  if (typeof a === 'number' && typeof b === 'number') return a - b
  return String(a) < String(b) ? -1 : String(a) > String(b) ? 1 : 0
}

export { compareEntityID, conceptID, formatEntityID }
export type { EntityID, WhereClause }
