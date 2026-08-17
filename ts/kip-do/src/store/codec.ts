/**
 * The one place a table's column list is written down.
 *
 * Reading and writing a row are the two halves of the same mapping, and when
 * they are spelled separately a column added to one half stops being persisted
 * without anything failing — the row still loads, the value is just gone. So
 * both halves are generated from these declarations: `COLUMNS` gives the order
 * and `JSON_COLUMNS` says which of them hold encoded JSON rather than a scalar.
 *
 * Every write rewrites the whole row rather than computing a delta, for the
 * same reason: a delta is a second enumeration of the columns.
 */

import { encodeJson } from '../sql.js'
import { canonicalJson, type Json } from '../json.js'

/** Column sets, per table. `id` is the rowid and is never written explicitly. */
interface TableSpec {
  /** Every column except `id`, in a fixed order. */
  columns: readonly string[]
  /** Which of them are JSON text. */
  json: ReadonlySet<string>
}

const ENVELOPE_COLUMNS = [
  'space',
  'state',
  'version',
  'seq',
  'created_at',
  'updated_at',
  'created_tx',
  'updated_tx',
  'origin',
  'facets',
  'structural',
  'governance',
  'retention',
  'expires_at',
] as const

const ENVELOPE_JSON = ['origin', 'facets', 'structural', 'governance', 'retention']

function spec(
  extraColumns: readonly string[],
  extraJson: readonly string[],
): TableSpec {
  return {
    columns: [...ENVELOPE_COLUMNS, ...extraColumns],
    json: new Set([...ENVELOPE_JSON, ...extraJson]),
  }
}

export const TABLE_SPECS: Readonly<Record<string, TableSpec>> = {
  concepts: spec(
    [
      'client_key',
      'schema_ref',
      'key',
      'name',
      'canonical_id',
      'aliases',
      'attributes',
      'merged_into',
    ],
    ['aliases', 'attributes'],
  ),
  propositions: spec(
    [
      'subject',
      'subject_key',
      'predicate_ref',
      'object',
      'object_key',
      'tuple_key',
      'attributes',
    ],
    ['subject', 'object', 'attributes'],
  ),
  assertions: spec(
    [
      'client_key',
      'proposition_id',
      'asserted_by',
      'asserted_by_key',
      'stance',
      'mode',
      'confidence',
      'asserted_at',
      'valid_from',
      'valid_until',
      'evidence_refs',
      'context_refs',
      'status',
      'supersedes',
      'superseded_by',
      'retracted_at',
    ],
    [
      'asserted_by',
      'evidence_refs',
      'context_refs',
      'supersedes',
      'superseded_by',
    ],
  ),
  evidence: spec(
    [
      'client_key',
      'evidence_class',
      'payload_mode',
      'payload_inline',
      'content_ref',
      'content_digest',
      'media_type',
      'observed_at',
      'source_refs',
      'generated_by',
      'status',
      'corrects',
      'corrected_by',
    ],
    ['payload_inline', 'source_refs', 'corrects', 'corrected_by'],
  ),
  activities: spec(
    [
      'client_key',
      'activity_class',
      'started_at',
      'ended_at',
      'inputs',
      'outputs',
      'associated_actors',
      'parameters_digest',
      'status',
    ],
    ['inputs', 'outputs', 'associated_actors'],
  ),
  spaces: {
    columns: [
      'space_id',
      'uri',
      'name',
      'description',
      'owner_principal',
      'owners',
      'status',
      'default_policy_id',
      'trust_policy_id',
      'default_classification',
      'audit_mode',
      'created_at',
      'seq',
      'schema_environment_version',
      'policies',
    ],
    json: new Set(['owners', 'policies']),
  },
  schema_packages: {
    columns: [
      'package_ref',
      'package_id',
      'version',
      'content_digest',
      'declared_digest',
      'artifact',
      'installed_at',
      'source',
    ],
    json: new Set(['artifact']),
  },
  schema_envs: {
    columns: ['space', 'version', 'lock', 'created_at', 'tx_id'],
    json: new Set(['lock']),
  },
  element_versions: {
    columns: ['space', 'element', 'kind', 'version', 'seq', 'tx_id', 'op', 'row'],
    json: new Set(['row']),
  },
  transactions: {
    columns: [
      'tx_id',
      'space',
      'seq',
      'snapshot_seq',
      'committed_at',
      'status',
      'transaction_class',
      'idempotency_key',
      'request_digest',
      'semantic_plan_digest',
      'result_digest',
      'schema_environment_version',
      'result',
      'changes',
    ],
    json: new Set(['result', 'changes']),
  },
}

function specOf(table: string): TableSpec {
  const found = TABLE_SPECS[table]
  if (!found) throw new Error(`unknown table ${table}`)
  return found
}

/** A row as SQLite hands it back: scalars, with JSON columns still text. */
export type SqlRow = Record<string, SqlStorageValue>

/**
 * Turns a stored row into its decoded form.
 *
 * A JSON column that fails to parse is a corrupted row, not a missing value.
 * It is left to throw rather than defaulting to `{}`, because a default here
 * would present a damaged element as an ordinary empty one and let the next
 * write make the loss permanent.
 */
export function decodeRow<T>(table: string, row: SqlRow): T {
  const { json } = specOf(table)
  const out: Record<string, unknown> = {}
  for (const [column, value] of Object.entries(row)) {
    out[column] = json.has(column) ? JSON.parse(value as string) : value
  }
  return out as T
}

/**
 * Builds the `INSERT` statement and bound values for a whole row.
 *
 * `id` is omitted so SQLite allocates it; the caller reads it back from
 * `last_row_id`, which is how an element learns its own KIP id.
 */
export function insertStatement(
  table: string,
  row: object,
): { sql: string; values: SqlStorageValue[] } {
  const { columns } = specOf(table)
  return {
    sql: `INSERT INTO ${table} (${columns.map(quote).join(', ')})
            VALUES (${columns.map(() => '?').join(', ')})`,
    values: columns.map((column) =>
      bind(table, column, (row as Record<string, unknown>)[column]),
    ),
  }
}

/** Builds the `UPDATE` statement that rewrites every column of one row. */
export function updateStatement(
  table: string,
  row: object,
  id: number,
): { sql: string; values: SqlStorageValue[] } {
  const { columns } = specOf(table)
  const values = row as Record<string, unknown>
  return {
    sql: `UPDATE ${table} SET ${columns.map((c) => `${quote(c)} = ?`).join(', ')}
            WHERE id = ?`,
    values: [...columns.map((c) => bind(table, c, values[c])), id],
  }
}

/**
 * `key` is a SQLite keyword, and quoting every column is cheaper than
 * remembering which ones need it.
 */
const quote = (column: string): string => `"${column}"`

function bind(
  table: string,
  column: string,
  value: unknown,
): SqlStorageValue {
  if (specOf(table).json.has(column)) {
    return encodeJson(value ?? null, `${table}.${column}`)
  }
  if (value === undefined || value === null) {
    // Every scalar column is NOT NULL with an empty-string or zero default;
    // an undefined here is a row the caller under-filled, and writing NULL
    // would fail at the constraint with a message naming no field.
    throw new Error(`${table}.${column} was not set`)
  }
  return value as SqlStorageValue
}

/** Canonical JSON of a decoded row, for the version log and digests. */
export function rowToJson(row: unknown): Json {
  return JSON.parse(canonicalJson(row)) as Json
}
