/**
 * The full-text index behind `SEARCH`.
 *
 * Three FTS5 tables, one per searchable kind, keyed by the element's row id.
 * Maintenance runs inside {@link Store.put} — the single funnel every write
 * passes through — so the index commits in the same `transactionSync` as the
 * row it describes. That is what lets the engine report `index_seq` equal to
 * `current_space_seq` without lying (§66.5, §79): there is no window in which
 * the index lags, because a transaction that rolled back rolled the index back
 * with it.
 *
 * The corpus mirrors the Rust engine's field-by-field (`rs/anda_cognitive_nexus`
 * `meta/inspect.rs`). Two engines ranking the same corpus differently is a
 * quality difference a caller can live with; two engines searching *different
 * text* is a correctness difference nobody can debug from the outside.
 *
 * Assertions and Activities are absent on purpose. An Assertion's content is a
 * stance, a mode and a number; an Activity's is a class and two timestamps.
 * Neither carries free text, and an index over them would answer "no such
 * claim" to every question.
 */

import { jsonEquals, type Json } from '../json.js'
import { ftsQuote } from '../sql.js'
import { extractJsonText, segmentToText } from '../tokenizer.js'
import type { ElementKind } from '../id.js'
import { decodeRow, type SqlRow } from './codec.js'
import type { Element, ElementRow } from './rows.js'

/** One kind's index: the FTS table, its columns, and how a row fills them. */
interface SearchableKind {
  fts: string
  table: string
  /** The row columns the index is built from, one FTS column each. */
  columns: readonly string[]
  /** The same fields as a rendered view names them. */
  viewFields: readonly string[]
  /** The segmented text of each column, from the fields in column order. */
  textOf(values: readonly unknown[]): string[]
}

const strings = (value: unknown): string[] =>
  Array.isArray(value) ? value.filter((item): item is string => typeof item === 'string') : []

const CONCEPT: SearchableKind = {
  fts: 'fts_concepts',
  table: 'concepts',
  columns: ['name', 'aliases', 'attributes'],
  viewFields: ['name', 'aliases', 'attributes'],
  textOf: ([name, aliases, attributes]) => [
    segmentToText(typeof name === 'string' ? name : ''),
    segmentToText(strings(aliases).join(' ')),
    segmentToText(extractJsonText(attributes ?? {}).join(' ')),
  ],
}

const PROPOSITION: SearchableKind = {
  fts: 'fts_propositions',
  table: 'propositions',
  // A Proposition's whole content is its tuple (§12.2), so the only text it
  // has of its own is the predicate it was written under. The endpoints carry
  // the words, and they are Concepts and Literals a search reaches on their
  // own terms.
  columns: ['predicate_ref'],
  viewFields: ['predicate_ref'],
  // The exact symbol, segmented like anything else: `unicode61` splits it at
  // the scheme and path separators, so `SEARCH PROPOSITION "prefers"` finds
  // tuples under `kip://profiles/cognitive-memory@2.0.0/prefers` without the
  // caller having to know the package it came from.
  textOf: ([predicate]) => [segmentToText(typeof predicate === 'string' ? predicate : '')],
}

const EVIDENCE: SearchableKind = {
  fts: 'fts_evidence',
  table: 'evidence',
  columns: ['payload_inline'],
  viewFields: ['payload'],
  textOf: ([payload]) => [segmentToText(extractJsonText(payload ?? null).join(' '))],
}

/**
 * The tokens one rendered view is ranked on: the text the index would hold
 * for the same fields, so a scan and an index rank one corpus alike.
 */
export function searchTokens(kind: ElementKind, view: Record<string, unknown>): string[] {
  const searchable = SEARCHABLE[kind]
  if (searchable === undefined) return []
  return searchable
    .textOf(searchable.viewFields.map((field) => view[field]))
    .flatMap((text) => (text === '' ? [] : text.split(' ')))
}

export const SEARCHABLE: Readonly<Partial<Record<ElementKind, SearchableKind>>> = {
  Concept: CONCEPT,
  Proposition: PROPOSITION,
  Evidence: EVIDENCE,
}

/** Every FTS table, for a rebuild. */
export const SEARCH_TABLES: readonly SearchableKind[] = [CONCEPT, PROPOSITION, EVIDENCE]

/**
 * Brings one element's index entry in step with its row.
 *
 * Always a delete followed by an insert computed from the row as it now
 * stands, which is what makes the archive, tombstone and purge paths correct
 * for free: a purged stub carries no text, so recomputing from it removes the
 * text from the index rather than leaving it findable.
 */
export function indexElement(
  sql: SqlStorage,
  element: Element,
  before: Element | null = null,
): void {
  const kind = SEARCHABLE[element.kind]
  if (kind === undefined) return
  // The indexed columns are the row columns of the same names, so a write
  // that changed none of them has nothing to re-index.
  if (before !== null) {
    const was = before.row as unknown as Record<string, Json>
    const now = element.row as unknown as Record<string, Json>
    if (kind.columns.every((column) => jsonEquals(was[column] ?? null, now[column] ?? null))) return
  }
  const rowid = element.row.id
  sql.exec(`DELETE FROM ${kind.fts} WHERE rowid = ?`, rowid)
  sql.exec(`DELETE FROM search_docs WHERE kind = ? AND id = ?`, element.kind, rowid)

  const row = element.row as unknown as Record<string, unknown>
  const text = kind.textOf(kind.columns.map((column) => row[column]))
  // An element with nothing to index stays out of the table entirely. An empty
  // row would still be a document FTS5 counts toward the average length that
  // BM25 divides by, so it would shift the scores of every real hit.
  if (text.every((value) => value === '')) return
  const placeholders = kind.columns.map(() => '?').join(', ')
  sql.exec(
    `INSERT INTO ${kind.fts} (rowid, ${kind.columns.join(', ')})
       VALUES (?, ${placeholders})`,
    rowid,
    ...text,
  )
  sql.exec(
    `INSERT INTO search_docs (kind, id, len) VALUES (?, ?, ?)`,
    element.kind,
    rowid,
    text.reduce((n, value) => n + tokenCount(value), 0),
  )
}

/** How many tokens a segmented column holds: they are joined by one space. */
function tokenCount(text: string): number {
  return text === '' ? 0 : text.split(' ').length
}

/** One scored hit, before the caller applies its own filters. */
/** One indexed document of a Space's corpus. */
export interface CorpusDocument {
  id: number
  /** Its token count across the indexed columns. */
  len: number
  /** Its `schema_ref` or `predicate_ref`; empty for Evidence. */
  symbol: string
}

/**
 * Every active, indexed document of one kind in a Space.
 *
 * Reads the length table and a few element columns, never a row's content:
 * this is the corpus whose statistics a whole-Space SEARCH scores with.
 */
export function searchCorpus(
  sql: SqlStorage,
  kind: ElementKind,
  space: string,
): CorpusDocument[] {
  const searchable = SEARCHABLE[kind]
  if (searchable === undefined) return []
  const symbol =
    kind === 'Concept' ? 'e.schema_ref' : kind === 'Proposition' ? 'e.predicate_ref' : `''`
  return sql
    .exec<{ id: number; len: number; symbol: string }>(
      `SELECT d.id AS id, d.len AS len, ${symbol} AS symbol
         FROM search_docs d
         JOIN ${searchable.table} e ON e.id = d.id
        WHERE d.kind = ? AND e.space = ? AND e.state = 'active'`,
      kind,
      space,
    )
    .toArray()
}

/**
 * How often each query token occurs in every indexed document of one kind
 * that holds at least one of them, by id.
 *
 * The FTS match only narrows to candidates; the counts come from the stored
 * segmented text, so they are the same tokens {@link searchCorpus} counted.
 */
export function termCounts(
  sql: SqlStorage,
  kind: ElementKind,
  tokens: readonly string[],
): Map<number, Map<string, number>> {
  const searchable = SEARCHABLE[kind]
  const out = new Map<number, Map<string, number>>()
  if (searchable === undefined || tokens.length === 0) return out
  const wanted = new Set(tokens)
  for (const row of sql.exec<Record<string, SqlStorageValue>>(
    `SELECT rowid AS id, ${searchable.columns.join(', ')}
       FROM ${searchable.fts}
      WHERE ${searchable.fts} MATCH ?`,
    ftsQuote(tokens),
  )) {
    const counts = new Map<string, number>()
    for (const column of searchable.columns) {
      const text = String(row[column] ?? '')
      if (text === '') continue
      for (const token of text.split(' ')) {
        if (wanted.has(token)) counts.set(token, (counts.get(token) ?? 0) + 1)
      }
    }
    if (counts.size > 0) out.set(Number(row.id), counts)
  }
  return out
}

/**
 * Rebuilds every index from the element tables.
 *
 * Runs when the tables are new, and again whenever the segmenter's own output
 * changes under it: tokens produced by two different ICU vocabularies are not
 * comparable, and a row indexed under the old one is unreachable rather than
 * merely ranked worse.
 *
 * Not incremental and not resumable, because it cannot be: a half-rebuilt index
 * is one that answers some questions from the old vocabulary and some from the
 * new. A Durable Object's storage is local and the rebuild is a scan of its own
 * tables, so the whole thing runs in one pass.
 */
export function rebuildSearch(sql: SqlStorage): void {
  for (const kind of SEARCH_TABLES) {
    sql.exec(`DELETE FROM ${kind.fts}`)
  }
  sql.exec(`DELETE FROM search_docs`)
  for (const [name, kind] of Object.entries(SEARCHABLE) as [
    ElementKind,
    SearchableKind,
  ][]) {
    for (const row of sql.exec<SqlRow>(`SELECT * FROM ${kind.table}`)) {
      const decoded = decodeRow<ElementRow>(kind.table, row)
      indexElement(sql, { kind: name, row: decoded } as Element)
    }
  }
}
