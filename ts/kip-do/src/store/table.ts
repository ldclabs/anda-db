/**
 * Reading and writing whole rows, for every table.
 *
 * `codec.ts` says what a table's columns *are*; this says how a row gets in
 * and out. The pair is deliberate: a `SELECT` whose result is not run through
 * `decodeRow` hands back JSON columns as raw text, and nothing about that looks
 * wrong until a `.filter(…)` runs over a string that should have been an array.
 *
 * Written once and inherited, because both planes store rows the same way and
 * only one of them had said so. `Store` spelled the
 * `exec(…).toArray()[0]` / `decodeRow` pairing out at twenty call sites while
 * `GovernanceStore` kept it behind four private methods — so a table whose
 * decoding needed to change had one place to change on one plane and twenty on
 * the other.
 *
 * The statements themselves stay with the methods that need them. This is not
 * a query builder: SQL that is assembled from fragments is SQL nobody can read
 * against the indexes it is supposed to use, and every index in `ddl.ts`
 * exists for a statement written out in full somewhere.
 */

import { errors } from '../errors.js'
import {
  decodeRow,
  insertStatement,
  updateStatement,
  type SqlRow,
} from './codec.js'

/**
 * The row-level primitives shared by the cognitive and the Governance store.
 *
 * `sql` stays public: the read path assembles predicates the store has no
 * method for — a KQL pattern's `WHERE` is built from the query, not from a
 * fixed list — and reaching the handle is honest about that where a
 * `queryElementsMatching(…)` on the store would not be.
 */
export abstract class RowStore {
  readonly sql: SqlStorage

  constructor(sql: SqlStorage) {
    this.sql = sql
  }

  /** The first row of a statement, decoded, or `null` when it selected none. */
  one<T>(table: string, sql: string, ...values: SqlStorageValue[]): T | null {
    const row = this.sql.exec<SqlRow>(sql, ...values).toArray()[0]
    return row ? decodeRow<T>(table, row) : null
  }

  /** Every row of a statement, decoded, in the order the statement asked for. */
  all<T>(table: string, sql: string, ...values: SqlStorageValue[]): T[] {
    return this.sql
      .exec<SqlRow>(sql, ...values)
      .toArray()
      .map((row) => decodeRow<T>(table, row))
  }

  /** One row by its rowid, decoded. */
  protected byId<T>(table: string, id: number): T | null {
    return this.one<T>(table, `SELECT * FROM ${table} WHERE id = ?`, id)
  }

  /**
   * Writes a whole row. `id` is never bound, so SQLite allocates it.
   *
   * Separate from {@link insertRow} because most writes do not want the id
   * back, and reading it costs a second statement: the version log takes one
   * of these per element per commit, so charging every append for a
   * `last_insert_rowid()` nobody reads is a round-trip per element inside the
   * commit transaction.
   */
  protected writeRow(table: string, row: object): void {
    const { sql, values } = insertStatement(table, row)
    this.sql.exec(sql, ...values)
  }

  /**
   * Writes a whole row and returns the id SQLite assigned it.
   *
   * How an element learns its own KIP id. The id is read back immediately
   * because `last_insert_rowid()` describes the connection's last write and
   * nothing else, so carrying it across another statement would carry the
   * wrong one — which is also why this cannot be deferred to the caller.
   */
  protected insertRow(table: string, row: object): number {
    this.writeRow(table, row)
    return this.lastRowId()
  }

  /**
   * The row id SQLite just assigned.
   *
   * Its own method because one insert is not written through
   * {@link insertRow}: the shell an in-flight transaction reserves is a
   * deliberately partial row, filled in at commit, so it names its columns
   * itself and still needs the id back.
   */
  protected lastRowId(): number {
    const found = this.sql
      .exec<{ id: number }>('SELECT last_insert_rowid() AS id')
      .toArray()[0]
    if (!found) throw errors.internalError('no row id after an insert')
    return found.id
  }

  /** Rewrites every column of one row. */
  protected updateRow(table: string, row: { id: number }): void {
    const { sql, values } = updateStatement(table, row, row.id)
    this.sql.exec(sql, ...values)
  }
}
