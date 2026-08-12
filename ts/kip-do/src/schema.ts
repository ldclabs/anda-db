/**
 * SQLite schema for a KIP knowledge graph inside one Durable Object.
 *
 * Design notes that are not obvious from the DDL:
 *
 * **AUTOINCREMENT is required, not stylistic.** Entity ids are referenced by
 * *string* (`"C:123"`) from `propositions.subject` / `.object`, so SQLite's
 * default rowid reuse after a delete would silently hand a brand-new concept
 * every edge the deleted one had. `AUTOINCREMENT` forbids reuse.
 *
 * **Predicates are child rows, not an array column.** The Rust engine stores
 * one row per `(subject, object)` pair with a `predicates` set indexed by a
 * multi-valued B-Tree. SQL has no equivalent index, and the addressed element
 * in KIP is the *link* `(row, predicate)` — it carries its own attributes,
 * metadata and `_version`. Splitting the predicates into `proposition_links`
 * preserves the `P:{id}:{predicate}` address exactly while giving a real
 * index on predicate lookups.
 *
 * **The `ON DELETE CASCADE` here is narrow on purpose.** It only removes a
 * row's links when the row goes. The graph-level cascade — deleting a concept
 * must delete propositions referencing it, and then propositions referencing
 * *those links* — cannot be a foreign key: the reference is a
 * `(row, predicate)` pair encoded in a string that may point at either table.
 * That closure is computed with a recursive CTE in `exec/kml.ts`.
 *
 * **`_version` is mirrored into a generated column.** KIP keeps it inside the
 * `metadata` JSON map, which is where clients read it, but `EXPECT VERSION`
 * guards need it indexable and comparable without parsing JSON per row.
 */

/** Bumped whenever the DDL below changes in a way that needs a migration. */
export const SCHEMA_VERSION = 1

/**
 * Statements run synchronously during Durable Object construction, once for
 * each schema version. Every statement is idempotent so a re-run after a
 * partially applied migration is safe.
 */
export const SCHEMA_STATEMENTS: readonly string[] = [
  // --- concepts ----------------------------------------------------------
  `CREATE TABLE IF NOT EXISTS concepts (
     id         INTEGER PRIMARY KEY AUTOINCREMENT,
     type       TEXT NOT NULL,
     name       TEXT NOT NULL,
     attributes TEXT NOT NULL DEFAULT '{}',
     metadata   TEXT NOT NULL DEFAULT '{}',
     version    INTEGER GENERATED ALWAYS AS (
                  COALESCE(json_extract(metadata, '$._version'), 1)
                ) VIRTUAL,
     tok_ver    TEXT
   )`,
  // KIP identity for a concept is (type, name); this index is that rule.
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_concepts_type_name
     ON concepts(type, name)`,
  `CREATE INDEX IF NOT EXISTS idx_concepts_type ON concepts(type)`,
  `CREATE INDEX IF NOT EXISTS idx_concepts_name ON concepts(name)`,
  // Drives incremental re-tokenization after a TOKENIZER_VERSION bump.
  `CREATE INDEX IF NOT EXISTS idx_concepts_tok_ver ON concepts(tok_ver)`,

  // --- propositions ------------------------------------------------------
  `CREATE TABLE IF NOT EXISTS propositions (
     id      INTEGER PRIMARY KEY AUTOINCREMENT,
     subject TEXT NOT NULL,
     object  TEXT NOT NULL,
     tok_ver TEXT
   )`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_propositions_subject_object
     ON propositions(subject, object)`,
  // The (subject, object) unique index already serves subject-prefixed
  // lookups, so only the reverse direction needs its own index.
  `CREATE INDEX IF NOT EXISTS idx_propositions_object ON propositions(object)`,
  `CREATE INDEX IF NOT EXISTS idx_propositions_tok_ver ON propositions(tok_ver)`,

  // --- proposition links (one row per predicate) -------------------------
  `CREATE TABLE IF NOT EXISTS proposition_links (
     prop_id    INTEGER NOT NULL
                REFERENCES propositions(id) ON DELETE CASCADE,
     predicate  TEXT NOT NULL,
     attributes TEXT NOT NULL DEFAULT '{}',
     metadata   TEXT NOT NULL DEFAULT '{}',
     version    INTEGER GENERATED ALWAYS AS (
                  COALESCE(json_extract(metadata, '$._version'), 1)
                ) VIRTUAL,
     PRIMARY KEY (prop_id, predicate)
   ) WITHOUT ROWID`,
  // Predicate-first: the ordering that makes `(?s, "treats", ?o)` an index
  // seek instead of a scan of every link in the graph.
  `CREATE INDEX IF NOT EXISTS idx_links_predicate
     ON proposition_links(predicate, prop_id)`,

  // --- full-text search --------------------------------------------------
  //
  // `tokenize = 'ascii'` is deliberate and load-bearing. The external
  // tokenizer service is the *sole* segmentation authority: it applies
  // NFKC, lowercasing, script-aware segmentation (jieba for Han, UAX#29
  // otherwise), and stores the result as a space-joined token string. The
  // `ascii` tokenizer then does no linguistic work of its own — it splits on
  // ASCII punctuation/space and treats every byte >= 0x80 as a token
  // character, so a pre-segmented CJK token survives intact. Choosing
  // `unicode61` instead would apply another Unicode normalization and folding
  // policy, so the stored vocabulary would no longer be exactly the external
  // service's versioned output.
  //
  // These are ordinary (not contentless) FTS5 tables. A contentless table
  // cannot be deleted from without replaying the original text, and
  // `contentless_delete=1` needs a SQLite version we cannot assume. The cost
  // is one duplicated copy of the token string, which is bounded at 256
  // tokens per document by the tokenizer service.
  `CREATE VIRTUAL TABLE IF NOT EXISTS concepts_fts
     USING fts5(tokens, tokenize = 'ascii')`,
  `CREATE VIRTUAL TABLE IF NOT EXISTS propositions_fts
     USING fts5(tokens, tokenize = 'ascii')`,

  // --- engine key/value sidecar -----------------------------------------
  //
  // Replaces the `Collection` extension store the Rust engine uses for
  // bundled-capsule content hashes and the schema version.
  `CREATE TABLE IF NOT EXISTS kip_meta (
     k TEXT PRIMARY KEY,
     v TEXT NOT NULL
   ) WITHOUT ROWID`,
]

/**
 * Enables connection-local SQLite settings.
 *
 * Foreign keys are enabled per connection, not persisted with the schema, so
 * this still has to run whenever a Durable Object instance is constructed even
 * when the schema itself is already current.
 */
export function configureSql(sql: SqlStorage): void {
  sql.exec('PRAGMA foreign_keys = ON')
}

/** Applies the current schema. Safe to retry after an interrupted migration. */
export function applySchema(sql: SqlStorage): void {
  configureSql(sql)
  for (const statement of SCHEMA_STATEMENTS) {
    sql.exec(statement)
  }
  sql.exec(
    `INSERT INTO kip_meta (k, v) VALUES ('schema_version', ?)
       ON CONFLICT(k) DO UPDATE SET v = excluded.v`,
    String(SCHEMA_VERSION),
  )
}

/** Reads a sidecar value, or `null` when absent. */
export function metaGet(sql: SqlStorage, key: string): string | null {
  const row = sql
    .exec<{ v: string }>('SELECT v FROM kip_meta WHERE k = ?', key)
    .toArray()[0]
  return row ? row.v : null
}

/** Writes a sidecar value. */
export function metaSet(sql: SqlStorage, key: string, value: string): void {
  sql.exec(
    `INSERT INTO kip_meta (k, v) VALUES (?, ?)
       ON CONFLICT(k) DO UPDATE SET v = excluded.v`,
    key,
    value,
  )
}
