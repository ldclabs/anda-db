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
 */
export function rank(
  context: Context,
  spec: SearchSpec,
  b: ReadBindings,
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
    const tokens = segment(groundingText(kind, view))
    if (tokens.length > 0) documents.push({ id, view, tokens })
  }
  const avgLength = documents.reduce((n, doc) => n + doc.tokens.length, 0) / (documents.length || 1)
  const frequencies = new Map(queryTokens.map(token => [token,
    documents.reduce((n, doc) => n + Number(doc.tokens.includes(token)), 0)]))
  const hits: Hit[] = []
  for (const doc of documents) {
    let rawScore = 0
    const counts = new Map<string, number>()
    for (const token of doc.tokens) counts.set(token, (counts.get(token) ?? 0) + 1)
    for (const token of queryTokens) {
      const tf = counts.get(token) ?? 0
      if (tf === 0) continue
      const df = frequencies.get(token) ?? 0
      const idf = Math.log(1 + (documents.length - df + 0.5) / (df + 0.5))
      rawScore += idf * tf * 2.2 / (tf + 1.2 * (0.25 + 0.75 * doc.tokens.length / avgLength))
    }
    if (rawScore <= 0) continue
    const score = rawScore / (1 + rawScore)
    if (score < threshold) continue
    hits.push({ score, id: doc.id, kind, view: doc.view })
  }
  hits.sort((a, b2) => b2.score - a.score ||
    compareCodePoints(formatElementId(a.id), formatElementId(b2.id)))
  return { hits, cap }
}

/** The text a kind is grounded on, read from the redacted view. */
export function groundingText(kind: ElementKind, view: JsonMap): string {
  const fields =
    kind === 'Concept'
      ? ['name', 'aliases', 'attributes']
      : kind === 'Proposition'
        ? ['predicate_ref']
        : kind === 'Evidence'
          ? ['payload']
          : []
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
  for (const field of fields) collect(view[field])
  return text
}
