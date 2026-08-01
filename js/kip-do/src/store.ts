/**
 * Row-level data access over the Durable Object's SQLite storage.
 *
 * Everything here is **synchronous**. `ctx.storage.sql.exec()` returns a
 * cursor without yielding, so a whole read or a whole write batch runs without
 * opening the input gate — which is what lets `transactionSync` wrap a KML
 * statement in a real atomic unit. Nothing in this file may become `async`;
 * doing so would silently reintroduce the interleaving that the Rust engine
 * needs a lock to prevent.
 *
 * The one asynchronous dependency, tokenization, is deliberately kept *out*
 * of this layer: callers resolve tokens first and hand them in already
 * computed. See `nexus.ts` for how that ordering is enforced.
 */

import {
  type Concept,
  type EntityID,
  type JsonMap,
  type LinkProperties,
  type Proposition,
  conceptID,
  formatEntityID,
  parseEntityID,
  propositionID,
} from './entity.js'
import { notFound } from './errors.js'
import { decodeJson, encodeJson, idSet } from './sql.js'

interface ConceptRow {
  [key: string]: SqlStorageValue
  id: number
  type: string
  name: string
  attributes: string
  metadata: string
}

interface PropositionRow {
  [key: string]: SqlStorageValue
  id: number
  subject: string
  object: string
}

interface LinkRow {
  [key: string]: SqlStorageValue
  prop_id: number
  predicate: string
  attributes: string
  metadata: string
}

export class Store {
  constructor(private readonly sql: SqlStorage) {}

  // -------------------------------------------------------------------
  // Concepts
  // -------------------------------------------------------------------

  getConcept(id: number): Concept | null {
    const row = this.sql
      .exec<ConceptRow>(
        'SELECT id, type, name, attributes, metadata FROM concepts WHERE id = ?',
        id,
      )
      .toArray()[0]
    return row ? toConcept(row) : null
  }

  requireConcept(id: number): Concept {
    const concept = this.getConcept(id)
    if (!concept) throw notFound(`Concept C:${id} not found`)
    return concept
  }

  /**
   * Loads many concepts in one statement.
   *
   * The `json_each` join is not an optimization — it is the only way to look
   * up more than 100 ids at once under the bound-parameter limit.
   */
  getConcepts(ids: readonly number[]): Map<number, Concept> {
    const out = new Map<number, Concept>()
    if (ids.length === 0) return out
    const rows = this.sql
      .exec<ConceptRow>(
        `SELECT c.id, c.type, c.name, c.attributes, c.metadata
           FROM concepts c JOIN json_each(?) j ON c.id = j.value`,
        idSet(ids),
      )
      .toArray()
    for (const row of rows) out.set(row.id, toConcept(row))
    return out
  }

  conceptExists(id: number): boolean {
    return (
      this.sql
        .exec<{ n: number }>('SELECT 1 AS n FROM concepts WHERE id = ?', id)
        .toArray().length > 0
    )
  }

  findConceptByTypeName(type: string, name: string): number | null {
    const row = this.sql
      .exec<{ id: number }>(
        'SELECT id FROM concepts WHERE type = ? AND name = ?',
        type,
        name,
      )
      .toArray()[0]
    return row ? row.id : null
  }

  conceptIdsByType(type: string): number[] {
    return this.sql
      .exec<{ id: number }>(
        'SELECT id FROM concepts WHERE type = ? ORDER BY id',
        type,
      )
      .toArray()
      .map((r) => r.id)
  }

  conceptIdsByName(name: string): number[] {
    return this.sql
      .exec<{ id: number }>(
        'SELECT id FROM concepts WHERE name = ? ORDER BY id',
        name,
      )
      .toArray()
      .map((r) => r.id)
  }

  insertConcept(
    type: string,
    name: string,
    attributes: JsonMap,
    metadata: JsonMap,
  ): number {
    const cursor = this.sql.exec<{ id: number }>(
      `INSERT INTO concepts (type, name, attributes, metadata)
         VALUES (?, ?, ?, ?) RETURNING id`,
      type,
      name,
      encodeJson(attributes, `concept ${type}/${name} attributes`),
      encodeJson(metadata, `concept ${type}/${name} metadata`),
    )
    return cursor.one().id
  }

  updateConceptFields(
    id: number,
    attributes: JsonMap,
    metadata: JsonMap,
  ): void {
    this.sql.exec(
      'UPDATE concepts SET attributes = ?, metadata = ? WHERE id = ?',
      encodeJson(attributes, `concept C:${id} attributes`),
      encodeJson(metadata, `concept C:${id} metadata`),
      id,
    )
  }

  deleteConcepts(ids: readonly number[]): number {
    if (ids.length === 0) return 0
    const cursor = this.sql.exec(
      `DELETE FROM concepts WHERE id IN (SELECT value FROM json_each(?))`,
      idSet(ids),
    )
    for (const id of ids) this.clearConceptFts(id)
    return cursor.rowsWritten
  }

  /** Current `_version` of a concept; `0` when the row does not exist. */
  conceptVersion(id: number): number {
    const row = this.sql
      .exec<{ version: number }>(
        'SELECT version FROM concepts WHERE id = ?',
        id,
      )
      .toArray()[0]
    return row ? row.version : 0
  }

  // -------------------------------------------------------------------
  // Propositions
  // -------------------------------------------------------------------

  getProposition(id: number): Proposition | null {
    const row = this.sql
      .exec<PropositionRow>(
        'SELECT id, subject, object FROM propositions WHERE id = ?',
        id,
      )
      .toArray()[0]
    if (!row) return null
    const links = this.sql
      .exec<LinkRow>(
        `SELECT prop_id, predicate, attributes, metadata
           FROM proposition_links WHERE prop_id = ? ORDER BY predicate`,
        id,
      )
      .toArray()
    return toProposition(row, links)
  }

  requireProposition(id: number): Proposition {
    const p = this.getProposition(id)
    if (!p) throw notFound(`Proposition P:${id} not found`)
    return p
  }

  getPropositions(ids: readonly number[]): Map<number, Proposition> {
    const out = new Map<number, Proposition>()
    if (ids.length === 0) return out
    const set = idSet(ids)
    const rows = this.sql
      .exec<PropositionRow>(
        `SELECT p.id, p.subject, p.object
           FROM propositions p JOIN json_each(?) j ON p.id = j.value`,
        set,
      )
      .toArray()
    const linksById = new Map<number, LinkRow[]>()
    const linkRows = this.sql
      .exec<LinkRow>(
        `SELECT l.prop_id, l.predicate, l.attributes, l.metadata
           FROM proposition_links l JOIN json_each(?) j ON l.prop_id = j.value
          ORDER BY l.prop_id, l.predicate`,
        set,
      )
      .toArray()
    for (const link of linkRows) {
      const bucket = linksById.get(link.prop_id)
      if (bucket) bucket.push(link)
      else linksById.set(link.prop_id, [link])
    }
    for (const row of rows) {
      out.set(row.id, toProposition(row, linksById.get(row.id) ?? []))
    }
    return out
  }

  findPropositionRow(subject: EntityID, object: EntityID): number | null {
    const row = this.sql
      .exec<{ id: number }>(
        'SELECT id FROM propositions WHERE subject = ? AND object = ?',
        formatEntityID(subject),
        formatEntityID(object),
      )
      .toArray()[0]
    return row ? row.id : null
  }

  insertPropositionRow(subject: EntityID, object: EntityID): number {
    return this.sql
      .exec<{ id: number }>(
        'INSERT INTO propositions (subject, object) VALUES (?, ?) RETURNING id',
        formatEntityID(subject),
        formatEntityID(object),
      )
      .one().id
  }

  /**
   * Moves a row's endpoints without changing its id.
   *
   * Only MERGE's repointing calls this. Deleting and re-inserting would be
   * simpler but would mint a new id, invalidating every higher-order
   * proposition whose endpoint string names this row's links.
   */
  relocateProposition(id: number, subject: EntityID, object: EntityID): void {
    this.sql.exec(
      'UPDATE propositions SET subject = ?, object = ? WHERE id = ?',
      formatEntityID(subject),
      formatEntityID(object),
      id,
    )
  }

  upsertLink(
    propId: number,
    predicate: string,
    attributes: JsonMap,
    metadata: JsonMap,
  ): void {
    this.sql.exec(
      `INSERT INTO proposition_links (prop_id, predicate, attributes, metadata)
         VALUES (?, ?, ?, ?)
       ON CONFLICT(prop_id, predicate)
         DO UPDATE SET attributes = excluded.attributes,
                       metadata   = excluded.metadata`,
      propId,
      predicate,
      encodeJson(attributes, `link P:${propId}:${predicate} attributes`),
      encodeJson(metadata, `link P:${propId}:${predicate} metadata`),
    )
  }

  /**
   * Removes one link and, when it was the row's last predicate, the row
   * itself — a proposition with no predicates carries no meaning and would
   * otherwise occupy the `(subject, object)` unique slot forever.
   *
   * Returns true when the whole row was removed.
   */
  deleteLink(propId: number, predicate: string): boolean {
    this.sql.exec(
      'DELETE FROM proposition_links WHERE prop_id = ? AND predicate = ?',
      propId,
      predicate,
    )
    const remaining = this.sql
      .exec<{ n: number }>(
        'SELECT COUNT(*) AS n FROM proposition_links WHERE prop_id = ?',
        propId,
      )
      .one().n
    if (remaining === 0) {
      this.deletePropositionRows([propId])
      return true
    }
    return false
  }

  deletePropositionRows(ids: readonly number[]): number {
    if (ids.length === 0) return 0
    const cursor = this.sql.exec(
      'DELETE FROM propositions WHERE id IN (SELECT value FROM json_each(?))',
      idSet(ids),
    )
    for (const id of ids) this.clearPropositionFts(id)
    return cursor.rowsWritten
  }

  /** `_version` of one link; `0` when the row or the predicate is absent. */
  linkVersion(propId: number, predicate: string): number {
    const row = this.sql
      .exec<{ version: number }>(
        'SELECT version FROM proposition_links WHERE prop_id = ? AND predicate = ?',
        propId,
        predicate,
      )
      .toArray()[0]
    return row ? row.version : 0
  }

  // -------------------------------------------------------------------
  // Graph traversal
  // -------------------------------------------------------------------

  /**
   * Proposition rows with the given endpoints, resolved as id sets.
   *
   * `null` means "unconstrained" for that endpoint. Both being `null` is a
   * full scan of the edge table and callers must bound it.
   */
  matchPropositionRows(
    subjects: readonly EntityID[] | null,
    objects: readonly EntityID[] | null,
    predicates: readonly string[] | null,
  ): number[] {
    const clauses: string[] = []
    const params: string[] = []

    if (subjects) {
      clauses.push('p.subject IN (SELECT value FROM json_each(?))')
      params.push(idSet(subjects.map(formatEntityID)))
    }
    if (objects) {
      clauses.push('p.object IN (SELECT value FROM json_each(?))')
      params.push(idSet(objects.map(formatEntityID)))
    }
    if (predicates) {
      clauses.push(
        `EXISTS (SELECT 1 FROM proposition_links l
                  WHERE l.prop_id = p.id
                    AND l.predicate IN (SELECT value FROM json_each(?)))`,
      )
      params.push(idSet(predicates))
    }

    const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : ''
    return this.sql
      .exec<{ id: number }>(
        `SELECT p.id FROM propositions p ${where} ORDER BY p.id`,
        ...params,
      )
      .toArray()
      .map((r) => r.id)
  }

  /**
   * Multi-hop reachability for `"pred"{min,max}`.
   *
   * The Rust engine walks this with an explicit BFS queue, issuing one
   * storage round-trip per frontier node (`matching.rs:594-605`). A recursive
   * CTE pushes the entire traversal into SQLite as a single statement.
   *
   * `visited` is keyed on the node alone rather than `(node, depth)`: this
   * returns reachable *nodes* with their minimum hop count, not the set of
   * distinct paths. That is a deliberate difference from the Rust engine —
   * see the note in README.md under "Known divergences".
   *
   * `direction` picks which endpoint advances, so the same query serves
   * `(?start, "p"{1,3}, ?end)` and its reverse.
   */
  reachable(
    start: readonly EntityID[],
    predicate: string,
    minHops: number,
    maxHops: number,
    direction: 'forward' | 'backward',
  ): { node: EntityID; hops: number }[] {
    const [from, to] =
      direction === 'forward' ? ['subject', 'object'] : ['object', 'subject']

    const rows = this.sql
      .exec<{ node: string; hops: number }>(
        `WITH RECURSIVE reach(node, hops) AS (
           SELECT value, 0 FROM json_each(?1)
           UNION
           SELECT p.${to}, r.hops + 1
             FROM reach r
             JOIN propositions p ON p.${from} = r.node
             JOIN proposition_links l
               ON l.prop_id = p.id AND l.predicate = ?2
            WHERE r.hops < ?3
         )
         SELECT node, MIN(hops) AS hops
           FROM reach
          GROUP BY node
         HAVING MIN(hops) >= ?4
          ORDER BY node`,
        idSet(start.map(formatEntityID)),
        predicate,
        maxHops,
        minHops,
      )
      .toArray()

    return rows.map((r) => ({ node: parseEntityID(r.node), hops: r.hops }))
  }

  /**
   * Transitive closure of everything that must go when `roots` are deleted.
   *
   * A proposition endpoint may be `"P:{id}:{predicate}"` — a reference to one
   * *link* of another row — so this closure cannot be expressed as a foreign
   * key and cannot be driven by `ON DELETE CASCADE`. Every link of a
   * discovered row is a potential referent, which is why the recursive step
   * re-derives the `P:{id}:{predicate}` form for each predicate rather than
   * matching on the row id.
   */
  propositionClosure(roots: readonly EntityID[]): number[] {
    if (roots.length === 0) return []
    return this.sql
      .exec<{ id: number }>(
        `WITH RECURSIVE refs(ref) AS (
           SELECT value FROM json_each(?1)
           UNION
           SELECT 'P:' || p.id || ':' || l.predicate
             FROM propositions p
             JOIN proposition_links l ON l.prop_id = p.id
             JOIN refs r ON p.subject = r.ref OR p.object = r.ref
         )
         SELECT DISTINCT p.id
           FROM propositions p
           JOIN refs r ON p.subject = r.ref OR p.object = r.ref
          ORDER BY p.id`,
        idSet(roots.map(formatEntityID)),
      )
      .toArray()
      .map((r) => r.id)
  }

  // -------------------------------------------------------------------
  // Full-text index
  // -------------------------------------------------------------------

  /**
   * Replaces a document's tokens.
   *
   * FTS5 has no upsert, so a rewrite is delete-then-insert. Both statements
   * run inside the caller's transaction, so a reader never observes the gap.
   */
  setConceptFts(id: number, tokens: readonly string[], tokVer: string): void {
    this.sql.exec('DELETE FROM concepts_fts WHERE rowid = ?', id)
    this.sql.exec(
      'INSERT INTO concepts_fts (rowid, tokens) VALUES (?, ?)',
      id,
      tokens.join(' '),
    )
    this.sql.exec('UPDATE concepts SET tok_ver = ? WHERE id = ?', tokVer, id)
  }

  setPropositionFts(
    id: number,
    tokens: readonly string[],
    tokVer: string,
  ): void {
    this.sql.exec('DELETE FROM propositions_fts WHERE rowid = ?', id)
    this.sql.exec(
      'INSERT INTO propositions_fts (rowid, tokens) VALUES (?, ?)',
      id,
      tokens.join(' '),
    )
    this.sql.exec('UPDATE propositions SET tok_ver = ? WHERE id = ?', tokVer, id)
  }

  clearConceptFts(id: number): void {
    this.sql.exec('DELETE FROM concepts_fts WHERE rowid = ?', id)
  }

  clearPropositionFts(id: number): void {
    this.sql.exec('DELETE FROM propositions_fts WHERE rowid = ?', id)
  }

  /**
   * BM25-ranked concept ids for a tokenized query.
   *
   * FTS5's `bm25()` returns a *negative* score where more negative is a
   * better match. It is negated here so callers work with the "larger is
   * better" convention the KIP `_score` field uses.
   */
  searchConcepts(
    ftsQuery: string,
    topK: number,
  ): { id: number; score: number }[] {
    return this.sql
      .exec<{ id: number; score: number }>(
        `SELECT rowid AS id, -bm25(concepts_fts) AS score
           FROM concepts_fts
          WHERE concepts_fts MATCH ?
          ORDER BY score DESC
          LIMIT ?`,
        ftsQuery,
        topK,
      )
      .toArray()
  }

  searchPropositions(
    ftsQuery: string,
    topK: number,
  ): { id: number; score: number }[] {
    return this.sql
      .exec<{ id: number; score: number }>(
        `SELECT rowid AS id, -bm25(propositions_fts) AS score
           FROM propositions_fts
          WHERE propositions_fts MATCH ?
          ORDER BY score DESC
          LIMIT ?`,
        ftsQuery,
        topK,
      )
      .toArray()
  }

  /**
   * Marks a row's search index as needing rebuild.
   *
   * Used when a write changed searchable text but the tokens could not be
   * resolved inline. Nulling `tok_ver` is what makes the row visible to
   * `staleConceptIds` / `stalePropositionIds`.
   */
  markConceptStale(id: number): void {
    this.sql.exec('UPDATE concepts SET tok_ver = NULL WHERE id = ?', id)
  }

  markPropositionStale(id: number): void {
    this.sql.exec('UPDATE propositions SET tok_ver = NULL WHERE id = ?', id)
  }

  /** Concept ids whose tokens were produced by a different tokenizer version. */
  staleConceptIds(currentVersion: string, limit: number): number[] {
    return this.sql
      .exec<{ id: number }>(
        `SELECT id FROM concepts
          WHERE tok_ver IS NULL OR tok_ver <> ?
          ORDER BY id LIMIT ?`,
        currentVersion,
        limit,
      )
      .toArray()
      .map((r) => r.id)
  }

  stalePropositionIds(currentVersion: string, limit: number): number[] {
    return this.sql
      .exec<{ id: number }>(
        `SELECT id FROM propositions
          WHERE tok_ver IS NULL OR tok_ver <> ?
          ORDER BY id LIMIT ?`,
        currentVersion,
        limit,
      )
      .toArray()
      .map((r) => r.id)
  }
}

function toConcept(row: ConceptRow): Concept {
  return {
    id: row.id,
    type: row.type,
    name: row.name,
    attributes: decodeJson(row.attributes),
    metadata: decodeJson(row.metadata),
  }
}

function toProposition(row: PropositionRow, links: LinkRow[]): Proposition {
  const map = new Map<string, LinkProperties>()
  for (const link of links) {
    map.set(link.predicate, {
      attributes: decodeJson(link.attributes),
      metadata: decodeJson(link.metadata),
    })
  }
  return {
    id: row.id,
    subject: parseEntityID(row.subject),
    object: parseEntityID(row.object),
    links: map,
  }
}

export { conceptID, propositionID }
