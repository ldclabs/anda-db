/**
 * Associative retrieval (§66), shared by the META `SEARCH` statement and the
 * KQL Search Pattern (§43.8), so the two cannot drift into different notions
 * of a hit.
 *
 * Every hit goes through the same read decision a `FIND` would, and carries the
 * **redacted** view: a field a Grant masked out of a query must not come back
 * through a search snippet (§88.5). A score is relevance, never confidence,
 * and a miss is never absence (§66.6).
 */

import { errors } from '../errors.js'
import { isPermitted, redactView, resourceOfElement } from '../governance/index.js'
import { elementId, formatElementId, type ElementId, type ElementKind } from '../id.js'
import type { JsonMap } from '../json.js'
import type { Scalar, SearchTarget } from '../kip/ast.js'
import { lineageOfSymbol, lineageText } from '../schema/index.js'
import { TABLES } from '../store/index.js'
import { searchCorpus, searchTokens, searchValues, termCounts } from '../store/search.js'
import { MAX_QUERY_TOKENS, segment } from '../tokenizer.js'
import { compareCodePoints } from '../json.js'
import type { Context } from './context.js'
import { readNumber, readText, type ReadBindings } from './matching.js'

/** One ranked, authorized hit. */
export interface Hit {
  score: number
  id: ElementId
  kind: ElementKind
  /** The redacted view the hit was ranked on. */
  view: JsonMap
}

/** The modifiers a search takes, as written. */
export interface SearchSpec {
  target: SearchTarget
  term: Scalar
  with_type: Scalar | null
  with_predicate: Scalar | null
  mode: Scalar | null
  threshold: Scalar | null
}

/**
 * Ranks the authorized, redacted corpus for one search, best first. `cap` is
 * the tightest `max_results` a Grant placed on the hits.
 *
 * `want` is how many hits the caller can use. A caller whose authority
 * reaches the whole Space unnarrowed is ranked from the full-text index and
 * reads only those hits; any other reads its whole authorized corpus, and the
 * bound changes nothing.
 */
export function rank(
  context: Context,
  spec: SearchSpec,
  b: ReadBindings,
  want = Number.POSITIVE_INFINITY,
): { hits: Hit[]; cap: number | null } {
  const term = readText(spec.term, b, 'SEARCH')
  if (spec.mode !== null) {
    const mode = readText(spec.mode, b, 'MODE')
    if (mode !== 'keyword') {
      throw errors.searchModeUnsupported(
        `this engine has no embedding model, so ${JSON.stringify(mode)} search is ` +
          `unavailable; "keyword" is the only mode`,
      )
    }
  }
  const threshold = spec.threshold === null ? 0 : readNumber(spec.threshold, b, 'THRESHOLD')
  if (!Number.isFinite(threshold) || threshold < 0 || threshold > 1) {
    throw errors.typeMismatch('THRESHOLD must be a number in [0, 1]')
  }
  if ((spec.with_type !== null && spec.target !== 'Concept') ||
      (spec.with_predicate !== null && spec.target !== 'Proposition')) {
    throw errors.invalidSyntax('SEARCH modifier is not meaningful for this kind')
  }
  // §20.14: a symbol in a search narrows to its lineage, so a hit written
  // under an earlier package version is still a hit.
  const withType =
    spec.with_type === null
      ? null
      : lineageOfSymbol(
          context.env.resolveSymbol('ConceptType', readText(spec.with_type, b, 'WITH TYPE'), 'read'),
        )
  const withPredicate =
    spec.with_predicate === null
      ? null
      : lineageOfSymbol(
          context.env.resolveSymbol(
            'PredicateType',
            readText(spec.with_predicate, b, 'WITH PREDICATE'),
            'read',
          ),
        )

  let kind: ElementKind
  switch (spec.target) {
    case 'Concept':
    case 'Proposition':
    case 'Evidence':
      kind = spec.target
      break
    default:
      // An Assertion's content is a stance and a number; an Activity's is a
      // class and two timestamps. Refusing says so; answering nothing would
      // read as "no such claim exists".
      throw errors.unsupportedCapability(
        'Assertions and Activities carry no free text, so this engine builds no ' +
          'full-text index over them; reach them through the Proposition or Evidence ' +
          'they are about',
      )
  }

  let cap: number | null = null
  const queryTokens = [...new Set(segment(term, MAX_QUERY_TOKENS))]
  if (!context.historical && context.authority.searchesWholeSpace(context.auth)) {
    const hits = rankIndexed(context, kind, queryTokens, withType ?? withPredicate, threshold, want)
    if (hits !== null) return { hits, cap: null }
  }
  // Authorize and redact BEFORE building corpus statistics. Global FTS5 BM25
  // scores leak hidden text, even when the final hit itself is readable.
  const documents: { id: ElementId; view: JsonMap; tokens: string[] }[] = []
  for (const row of context.store.sql.exec<{ id: number }>(
    `SELECT id FROM ${TABLES[kind]} WHERE space = ? AND state = 'active' ORDER BY id`, context.space,
  )) {
    context.spend('scans', 1)
    const id = elementId(kind, row.id)
    const element = context.load(id)
    if (element === null) continue
    const decision = context.authority.authorize('search', resourceOfElement(element), context.auth)
    if (!isPermitted(decision.decision)) continue
    if (decision.constraints.max_results !== null) {
      cap = cap === null ? decision.constraints.max_results : Math.min(cap, decision.constraints.max_results)
    }
    const readable = context.view(id)
    if (readable === null) continue
    const view = structuredClone(readable)
    redactView(view, decision.constraints, context.readOrigin)
    if (withType !== null && lineageText(String(view.schema_ref ?? '')) !== withType) continue
    if (withPredicate !== null && lineageText(String(view.predicate_ref ?? '')) !== withPredicate) continue
    const tokens = searchTokens(kind, view)
    if (tokens.length > 0) documents.push({ id, view, tokens })
  }
  const avgLength = documents.reduce((n, doc) => n + doc.tokens.length, 0) / (documents.length || 1)
  const frequencies = new Map(queryTokens.map(token => [token,
    documents.reduce((n, doc) => n + Number(doc.tokens.includes(token)), 0)]))
  const hits: Hit[] = []
  for (const doc of documents) {
    const counts = new Map<string, number>()
    for (const token of doc.tokens) counts.set(token, (counts.get(token) ?? 0) + 1)
    const corpus = { size: documents.length, avgLength, frequencies }
    const score = relevance(counts, doc.tokens.length, queryTokens, corpus)
    if (score === null || score < threshold) continue
    hits.push({ score, id: doc.id, kind, view: doc.view })
  }
  hits.sort(byRank)
  return { hits, cap }
}

/** The statistics one corpus scores its documents with. */
interface CorpusStats {
  size: number
  avgLength: number
  /** How many documents of the corpus hold each query token. */
  frequencies: Map<string, number>
}

/**
 * One document's normalized BM25 relevance (k1 1.2, b 0.75) against a corpus,
 * or `null` when it matches no query token.
 */
function relevance(
  counts: Map<string, number>,
  length: number,
  queryTokens: readonly string[],
  corpus: CorpusStats,
): number | null {
  let rawScore = 0
  for (const token of queryTokens) {
    const tf = counts.get(token) ?? 0
    if (tf === 0) continue
    const df = corpus.frequencies.get(token) ?? 0
    const idf = Math.log(1 + (corpus.size - df + 0.5) / (df + 0.5))
    rawScore += idf * tf * 2.2 / (tf + 1.2 * (0.25 + 0.75 * length / corpus.avgLength))
  }
  return rawScore > 0 ? rawScore / (1 + rawScore) : null
}

/** Best first; equal scores in id order, so a traversal pages stably. */
function byRank(a: { score: number; id: ElementId }, b: { score: number; id: ElementId }): number {
  return b.score - a.score || compareCodePoints(formatElementId(a.id), formatElementId(b.id))
}

/**
 * Ranks a caller's whole-Space corpus straight from the full-text index.
 *
 * For a caller whose authority reaches every element unnarrowed, the
 * authorized corpus *is* the Space's active corpus of the kind, so the
 * statistics a scan would compute come from the index's length table and
 * term matches — the same tokens, the same formula — and only the hits a
 * caller can use are read. `null` sends the caller back to the scan: a hit
 * that is not readable after all, or a term FTS5 cannot match.
 */
function rankIndexed(
  context: Context,
  kind: ElementKind,
  queryTokens: readonly string[],
  lineage: string | null,
  threshold: number,
  want: number,
): Hit[] | null {
  if (queryTokens.length === 0) return []
  const lengths = new Map<number, number>()
  let total = 0
  for (const doc of searchCorpus(context.store.sql, kind, context.space)) {
    if (lineage !== null && lineageText(doc.symbol) !== lineage) continue
    lengths.set(doc.id, doc.len)
    total += doc.len
  }
  if (lengths.size === 0) return []
  let matches: Map<number, Map<string, number>>
  try {
    matches = termCounts(context.store.sql, kind, queryTokens)
  } catch {
    return null
  }
  const frequencies = new Map<string, number>()
  for (const [seq, counts] of matches) {
    if (!lengths.has(seq)) continue
    for (const token of counts.keys()) frequencies.set(token, (frequencies.get(token) ?? 0) + 1)
  }
  const corpus = { size: lengths.size, avgLength: total / lengths.size, frequencies }
  const ranked: { score: number; id: ElementId }[] = []
  for (const [seq, counts] of matches) {
    const length = lengths.get(seq)
    if (length === undefined) continue
    const score = relevance(counts, length, queryTokens, corpus)
    if (score !== null && score >= threshold) ranked.push({ score, id: elementId(kind, seq) })
  }
  ranked.sort(byRank)
  const hits: Hit[] = []
  for (const { score, id } of ranked) {
    if (hits.length >= want) break
    context.spend('scans', 1)
    const element = context.load(id)
    if (element === null) return null
    const decision = context.authority.authorize('search', resourceOfElement(element), context.auth)
    const readable = context.view(id)
    if (!isPermitted(decision.decision) || readable === null) return null
    const view = structuredClone(readable)
    redactView(view, decision.constraints, context.readOrigin)
    hits.push({ score, id, kind, view })
  }
  return hits
}

/** The text a kind is grounded on, read from the redacted view. */
export function groundingText(kind: ElementKind, view: JsonMap): string {
  let text = ''
  const collect = (value: unknown): void => {
    if (typeof value === 'string') {
      text = text === '' ? value : `${text} ${value}`
    } else if (Array.isArray(value)) {
      value.forEach(collect)
    } else if (value !== null && typeof value === 'object') {
      Object.values(value as Record<string, unknown>).forEach(collect)
    }
  }
  for (const value of searchValues(kind, view)) collect(value)
  return text
}
