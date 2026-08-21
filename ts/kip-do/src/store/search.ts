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

import type { JsonMap } from '../json.js'
import { ftsQuote } from '../sql.js'
import { extractJsonText, segment, segmentToText } from '../tokenizer.js'
import type { ElementKind } from '../id.js'
import { decodeRow, type SqlRow } from './codec.js'
import type { Element, ElementRow } from './rows.js'

/** One kind's index: the FTS table, its columns, and how a row fills them. */
interface SearchableKind {
  fts: string
  table: string
  columns: readonly string[]
  /** The already-segmented text for each column, in column order. */
  textOf(element: Element): string[]
}

const CONCEPT: SearchableKind = {
  fts: 'fts_concepts',
  table: 'concepts',
  columns: ['name', 'aliases', 'attributes'],
  textOf: (element) => {
    const row = element.row as { name: string; aliases: string[]; attributes: JsonMap }
    return [
      segmentToText(row.name),
      segmentToText(row.aliases.join(' ')),
      segmentToText(extractJsonText(row.attributes).join(' ')),
    ]
  },
}

const PROPOSITION: SearchableKind = {
  fts: 'fts_propositions',
  table: 'propositions',
  columns: ['predicate_ref', 'attributes'],
  textOf: (element) => {
    const row = element.row as { predicate_ref: string; attributes: JsonMap }
    return [
      // The exact symbol, segmented like anything else: `unicode61` splits it
      // at the scheme and path separators, so `SEARCH PROPOSITION "prefers"`
      // finds tuples under `kip://profiles/cognitive-memory@2.0.0/prefers`
      // without the caller having to know the package it came from.
      segmentToText(row.predicate_ref),
      segmentToText(extractJsonText(row.attributes).join(' ')),
    ]
  },
}

const EVIDENCE: SearchableKind = {
  fts: 'fts_evidence',
  table: 'evidence',
  columns: ['payload_inline'],
  textOf: (element) => {
    const row = element.row as { payload_inline: unknown }
    return [segmentToText(extractJsonText(row.payload_inline).join(' '))]
  },
}

/** The kinds this engine indexes, keyed by element kind. */
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
export function indexElement(sql: SqlStorage, element: Element): void {
  const kind = SEARCHABLE[element.kind]
  if (kind === undefined) return
  const rowid = element.row.id
  sql.exec(`DELETE FROM ${kind.fts} WHERE rowid = ?`, rowid)

  const text = kind.textOf(element)
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
}

/** One scored hit, before the caller applies its own filters. */
export interface SearchHit {
  seq: number
  score: number
}

export interface SearchQuery {
  kind: ElementKind
  space: string
  /** The raw term. Segmented here, by the same function that indexed. */
  term: string
  /** How many rows to score. Callers over-fetch, because filters come after. */
  limit: number
}

/**
 * Scores one kind against a term, most relevant first.
 *
 * The score is `-bm25()`: FTS5 returns a value that is *more negative* the
 * better the match, and every layer above this one — `THRESHOLD`, the sort, the
 * Rust engine it has to agree with — reads a score as bigger-is-better. Leaving
 * the sign alone would make the default `THRESHOLD 0.0` reject every hit.
 *
 * Space and lifecycle are joined from the element table rather than copied into
 * the index, so there is one copy of the truth about which Space a row is in.
 */
export function searchIndex(sql: SqlStorage, query: SearchQuery): SearchHit[] {
  const kind = SEARCHABLE[query.kind]
  if (kind === undefined) return []
  const tokens = segment(query.term)
  // A term that segments to nothing — punctuation, an empty string — is not an
  // error and not a match. Handing FTS5 an empty MATCH would be a syntax error
  // about a query the caller never wrote.
  if (tokens.length === 0) return []

  return sql
    .exec<{ seq: number; score: number }>(
      `SELECT f.rowid AS seq, -bm25(${kind.fts}) AS score
         FROM ${kind.fts} f
         JOIN ${kind.table} e ON e.id = f.rowid
        WHERE ${kind.fts} MATCH ?
          AND e.space = ?
          AND e.state = 'active'
        ORDER BY score DESC
        LIMIT ?`,
      ftsQuote(tokens),
      query.space,
      query.limit,
    )
    .toArray()
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
