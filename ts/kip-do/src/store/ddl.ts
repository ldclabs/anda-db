/**
 * The SQLite schema of one Cognitive Nexus.
 *
 * Five element tables, one per Core kind, plus the registries and logs that
 * make history, idempotency and Schema resolution answerable. It mirrors the
 * ten `anda_db` collections of `rs/anda_cognitive_nexus/src/store/` — the row
 * shapes are the same information, so the two engines can be read side by
 * side — but the indexing does not, and the differences are deliberate.
 *
 * **The envelope is repeated per table, not shared.** `space`, `state`,
 * `version` and the `_system` columns appear in every element table instead of
 * a nested block or a common parent. An index is built over a named column, so
 * a shared envelope would put every selective predicate the engine has — Space,
 * lifecycle state, change sequence — behind a JSON path.
 *
 * **Every reference gets a key column beside its JSON.** The JSON is the
 * record; the key is `endpointKey`, the deterministic string that makes
 * reference equality an index lookup rather than a scan-and-compare.
 *
 * **Absence is the empty string, not NULL.** No legal value of these columns —
 * an element id, a Schema symbol, a normalized timestamp — is ever empty, so
 * `''` is an unambiguous "unset" that still sorts and still compares with `=`.
 * NULL would need `IS NULL` in every predicate and would drop the row from
 * ordinary comparisons instead.
 *
 * **Composite indexes are used freely, and that is a genuine difference from
 * the Rust engine.** There, a multi-field B-Tree index is also a uniqueness
 * constraint, so every index has to be single-field and intersected. SQLite has
 * no such coupling: `(space, state)` is one seek here and two intersected scans
 * there.
 *
 * **`AUTOINCREMENT` is required, not stylistic.** Element ids are referenced by
 * *string* (`"C-42"`) from other rows, so SQLite's default rowid reuse after a
 * delete would hand a brand-new element every reference the deleted one had.
 * Nothing here deletes an element row — purge leaves an identity stub — but a
 * never-reused id should not rest on "nothing currently does".
 */

import { errors } from '../errors.js'

/**
 * Bumped whenever the DDL below changes in a way that needs a migration.
 *
 * 2 — the Governance Control Plane's eight tables. Purely additive: every
 * cognitive table is unchanged, so an existing database gains the control plane
 * on the next construction and keeps everything it had.
 */
export const SCHEMA_VERSION = 2

/**
 * The `_system` envelope every element table repeats.
 *
 * Written as a fragment rather than duplicated five times so a column cannot
 * be added to four tables and forgotten in the fifth.
 */
const ENVELOPE = `
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  space       TEXT NOT NULL,
  state       TEXT NOT NULL,
  version     INTEGER NOT NULL,
  seq         INTEGER NOT NULL,
  created_at  TEXT NOT NULL,
  updated_at  TEXT NOT NULL,
  created_tx  TEXT NOT NULL,
  updated_tx  TEXT NOT NULL,
  origin      TEXT NOT NULL DEFAULT '{}',
  facets      TEXT NOT NULL DEFAULT '{}',
  structural  TEXT NOT NULL DEFAULT '{}',
  governance  TEXT NOT NULL DEFAULT '{}',
  retention   TEXT NOT NULL DEFAULT '{}',
  expires_at  TEXT NOT NULL DEFAULT ''`

/** The indexes every element table carries, whatever kind it holds. */
function envelopeIndexes(table: string): string[] {
  return [
    // Space plus lifecycle state is the predicate almost every read carries.
    `CREATE INDEX IF NOT EXISTS idx_${table}_space_state
       ON ${table}(space, state)`,
    // The CHANGES cursor and every `AS OF SEQ` narrowing range over this.
    `CREATE INDEX IF NOT EXISTS idx_${table}_space_seq
       ON ${table}(space, seq)`,
    // The retention sweep. Partial, because the overwhelming majority of
    // elements never set an expiry and would otherwise sit in the index
    // sharing one key.
    `CREATE INDEX IF NOT EXISTS idx_${table}_space_expires
       ON ${table}(space, expires_at) WHERE expires_at <> ''`,
  ]
}

/**
 * Statements run during Durable Object construction, in order.
 *
 * Every statement is idempotent, so a re-run after a partially applied
 * migration is safe — which is the only recovery a Durable Object gets, since
 * construction is not a transaction.
 */
export const SCHEMA_STATEMENTS: readonly string[] = [
  // --- concepts ----------------------------------------------------------
  `CREATE TABLE IF NOT EXISTS concepts (${ENVELOPE},
     client_key   TEXT NOT NULL DEFAULT '',
     schema_ref   TEXT NOT NULL DEFAULT '',
     key          TEXT NOT NULL DEFAULT '',
     name         TEXT NOT NULL DEFAULT '',
     canonical_id TEXT NOT NULL DEFAULT '',
     aliases      TEXT NOT NULL DEFAULT '[]',
     attributes   TEXT NOT NULL DEFAULT '{}',
     merged_into  TEXT NOT NULL DEFAULT ''
   )`,
  ...envelopeIndexes('concepts'),
  // The Space-local logical key is the immutable identity (§5.3), so this
  // index *is* that rule and not an optimization. Partial: a Concept may have
  // no key at all, and several such Concepts must not collide on `''`.
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_concepts_key
     ON concepts(space, key) WHERE key <> ''`,
  // `name` is mutable grounding state and duplicates are allowed (§5.2), so
  // this one is deliberately not unique.
  `CREATE INDEX IF NOT EXISTS idx_concepts_name ON concepts(space, name)`,
  `CREATE INDEX IF NOT EXISTS idx_concepts_schema_ref
     ON concepts(space, schema_ref)`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_concepts_canonical
     ON concepts(space, canonical_id) WHERE canonical_id <> ''`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_concepts_client_key
     ON concepts(space, client_key) WHERE client_key <> ''`,

  // --- propositions ------------------------------------------------------
  //
  // No confidence column, and its absence is the point: confidence lives on
  // the Assertions about this tuple (§12.8).
  `CREATE TABLE IF NOT EXISTS propositions (${ENVELOPE},
     subject       TEXT NOT NULL DEFAULT '{}',
     subject_key   TEXT NOT NULL DEFAULT '',
     predicate_ref TEXT NOT NULL DEFAULT '',
     object        TEXT NOT NULL DEFAULT '{}',
     object_key    TEXT NOT NULL DEFAULT '',
     tuple_key     TEXT NOT NULL DEFAULT '',
     attributes    TEXT NOT NULL DEFAULT '{}'
   )`,
  ...envelopeIndexes('propositions'),
  // One canonical Proposition per semantic tuple per Space (§93.6). This is
  // the constraint `ENSURE PROPOSITION` resolves against instead of racing two
  // writers into a duplicate. The digest already covers the Space, so the
  // uniqueness is global rather than per-Space.
  //
  // Partial, because a reserved shell has no tuple yet: without the predicate
  // every concurrent reservation would collide on the empty key, and the
  // constraint that keeps two Propositions apart would be the thing stopping
  // one from being created.
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_propositions_tuple
     ON propositions(tuple_key) WHERE tuple_key <> ''`,
  // Predicate-first, so `(?s, "treats", ?o)` is a seek rather than a scan of
  // every tuple in the Space.
  `CREATE INDEX IF NOT EXISTS idx_propositions_predicate
     ON propositions(space, predicate_ref, subject_key)`,
  `CREATE INDEX IF NOT EXISTS idx_propositions_subject
     ON propositions(space, subject_key, predicate_ref)`,
  `CREATE INDEX IF NOT EXISTS idx_propositions_object
     ON propositions(space, object_key, predicate_ref)`,

  // --- assertions --------------------------------------------------------
  //
  // The epistemic payload is historically immutable: a changed commitment is a
  // new Assertion plus supersession, never a rewrite (§15.1). Only the
  // lifecycle columns move.
  `CREATE TABLE IF NOT EXISTS assertions (${ENVELOPE},
     client_key      TEXT NOT NULL DEFAULT '',
     proposition_id  TEXT NOT NULL DEFAULT '',
     asserted_by     TEXT NOT NULL DEFAULT '{}',
     asserted_by_key TEXT NOT NULL DEFAULT '',
     stance          TEXT NOT NULL DEFAULT '',
     mode            TEXT NOT NULL DEFAULT '',
     confidence      REAL NOT NULL DEFAULT -1,
     asserted_at     TEXT NOT NULL DEFAULT '',
     valid_from      TEXT NOT NULL DEFAULT '',
     valid_until     TEXT NOT NULL DEFAULT '',
     evidence_refs   TEXT NOT NULL DEFAULT '[]',
     context_refs    TEXT NOT NULL DEFAULT '[]',
     status          TEXT NOT NULL DEFAULT 'active',
     supersedes      TEXT NOT NULL DEFAULT '[]',
     superseded_by   TEXT NOT NULL DEFAULT '[]',
     retracted_at    TEXT NOT NULL DEFAULT ''
   )`,
  ...envelopeIndexes('assertions'),
  // The projection's driving read: every Assertion about one Proposition.
  `CREATE INDEX IF NOT EXISTS idx_assertions_proposition
     ON assertions(space, proposition_id, status)`,
  `CREATE INDEX IF NOT EXISTS idx_assertions_actor
     ON assertions(space, asserted_by_key)`,
  // Validity is a range query over two fixed-width UTC strings, which is why
  // `valid_until` uses '' for "still applies" and is read through TIME_MAX.
  `CREATE INDEX IF NOT EXISTS idx_assertions_valid
     ON assertions(space, valid_from, valid_until)`,
  // Asserting the same thing twice is a repetition, not a duplicate, so an
  // Assertion cannot be deduplicated structurally — the client key is the only
  // thing that makes creation retry-safe (§72).
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_assertions_client_key
     ON assertions(space, client_key) WHERE client_key <> ''`,

  // --- evidence ----------------------------------------------------------
  `CREATE TABLE IF NOT EXISTS evidence (${ENVELOPE},
     client_key     TEXT NOT NULL DEFAULT '',
     evidence_class TEXT NOT NULL DEFAULT '',
     payload_mode   TEXT NOT NULL DEFAULT '',
     payload_inline TEXT NOT NULL DEFAULT 'null',
     content_ref    TEXT NOT NULL DEFAULT '',
     content_digest TEXT NOT NULL DEFAULT '',
     media_type     TEXT NOT NULL DEFAULT '',
     observed_at    TEXT NOT NULL DEFAULT '',
     source_refs    TEXT NOT NULL DEFAULT '[]',
     generated_by   TEXT NOT NULL DEFAULT '',
     status         TEXT NOT NULL DEFAULT 'active',
     corrects       TEXT NOT NULL DEFAULT '[]',
     corrected_by   TEXT NOT NULL DEFAULT '[]'
   )`,
  ...envelopeIndexes('evidence'),
  // Indexed for lookup, never for identity: equal digests do not imply
  // identical Evidence, because two independent observations of the same text
  // are two observations (§73).
  `CREATE INDEX IF NOT EXISTS idx_evidence_digest
     ON evidence(space, content_digest) WHERE content_digest <> ''`,
  `CREATE INDEX IF NOT EXISTS idx_evidence_class
     ON evidence(space, evidence_class, status)`,
  `CREATE INDEX IF NOT EXISTS idx_evidence_observed
     ON evidence(space, observed_at)`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_evidence_client_key
     ON evidence(space, client_key) WHERE client_key <> ''`,

  // --- activities --------------------------------------------------------
  //
  // An Activity describes a process; it is not a Transaction (§22.1).
  `CREATE TABLE IF NOT EXISTS activities (${ENVELOPE},
     client_key        TEXT NOT NULL DEFAULT '',
     activity_class    TEXT NOT NULL DEFAULT '',
     started_at        TEXT NOT NULL DEFAULT '',
     ended_at          TEXT NOT NULL DEFAULT '',
     inputs            TEXT NOT NULL DEFAULT '[]',
     outputs           TEXT NOT NULL DEFAULT '[]',
     associated_actors TEXT NOT NULL DEFAULT '[]',
     parameters_digest TEXT NOT NULL DEFAULT '',
     status            TEXT NOT NULL DEFAULT ''
   )`,
  ...envelopeIndexes('activities'),
  `CREATE INDEX IF NOT EXISTS idx_activities_class
     ON activities(space, activity_class, status)`,
  `CREATE INDEX IF NOT EXISTS idx_activities_started
     ON activities(space, started_at)`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_activities_client_key
     ON activities(space, client_key) WHERE client_key <> ''`,

  // --- the reverse reference index ---------------------------------------
  //
  // Every element-to-element reference, from whichever field carries it: the
  // Core ones (a Proposition's endpoints, an Assertion's Proposition, actor,
  // Evidence and context, an Evidence record's sources and generating
  // Activity, an Activity's inputs, outputs and actors, a Concept's
  // `merged_into`) and the Profile structural fields alike.
  //
  // The Rust engine answers "what refers to this?" with a full Space scan,
  // because its structural fields and context references have no key column
  // and an incomplete answer would let a destructive operation leave a
  // dangling reference. Here that answer is an index seek instead. The cost is
  // that this table has to be rewritten whenever an element's references
  // change — which is why it is derived from one function, `elementReferences`,
  // rather than maintained per call site.
  `CREATE TABLE IF NOT EXISTS element_refs (
     space   TEXT NOT NULL,
     from_id TEXT NOT NULL,
     field   TEXT NOT NULL,
     ord     INTEGER NOT NULL,
     to_id   TEXT NOT NULL,
     PRIMARY KEY (space, from_id, field, ord)
   ) WITHOUT ROWID`,
  `CREATE INDEX IF NOT EXISTS idx_element_refs_to
     ON element_refs(space, to_id, field)`,

  // --- the element version log -------------------------------------------
  //
  // Element rows are updated in place, so the current row is all a reader
  // would have if this log did not exist — and `AS OF` would have nothing to
  // read. Each commit appends the complete row it wrote, so a past coordinate
  // can be reconstructed rather than guessed at from a change list.
  //
  // The whole row is stored, not a diff: a diff chain has to be replayed from
  // the beginning to answer one question, and a chain with one missing link
  // answers it wrongly instead of refusing.
  `CREATE TABLE IF NOT EXISTS element_versions (
     id      INTEGER PRIMARY KEY AUTOINCREMENT,
     space   TEXT NOT NULL,
     element TEXT NOT NULL,
     kind    TEXT NOT NULL,
     version INTEGER NOT NULL,
     seq     INTEGER NOT NULL,
     tx_id   TEXT NOT NULL,
     op      TEXT NOT NULL,
     row     TEXT NOT NULL
   )`,
  // `AS OF SEQ s` reads the greatest version of an element whose seq is at
  // most s, which this index answers with one backward seek.
  `CREATE INDEX IF NOT EXISTS idx_versions_element
     ON element_versions(space, element, seq)`,
  // `HISTORY SPACE` and the historical candidate reconstruction range over the
  // Space's whole log instead.
  `CREATE INDEX IF NOT EXISTS idx_versions_space_seq
     ON element_versions(space, seq, kind)`,
  `CREATE INDEX IF NOT EXISTS idx_versions_tx ON element_versions(tx_id)`,

  // --- the transaction journal -------------------------------------------
  //
  // What makes HISTORY, CHANGES and idempotent recovery answerable: a caller
  // that lost a response looks the transaction up by its key rather than
  // writing again (§80.4).
  `CREATE TABLE IF NOT EXISTS transactions (
     id                         INTEGER PRIMARY KEY AUTOINCREMENT,
     tx_id                      TEXT NOT NULL,
     space                      TEXT NOT NULL,
     seq                        INTEGER NOT NULL,
     snapshot_seq               INTEGER NOT NULL,
     committed_at               TEXT NOT NULL,
     status                     TEXT NOT NULL,
     transaction_class          TEXT NOT NULL DEFAULT 'cognitive',
     idempotency_key            TEXT NOT NULL DEFAULT '',
     request_digest             TEXT NOT NULL DEFAULT '',
     semantic_plan_digest       TEXT NOT NULL DEFAULT '',
     result_digest              TEXT NOT NULL DEFAULT '',
     schema_environment_version INTEGER NOT NULL DEFAULT 0,
     result                     TEXT NOT NULL DEFAULT 'null',
     changes                    TEXT NOT NULL DEFAULT '[]'
   )`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_tx
     ON transactions(tx_id)`,
  `CREATE INDEX IF NOT EXISTS idx_transactions_space_seq
     ON transactions(space, seq)`,
  // Scoped per Space rather than globally, so two Spaces may reuse a key
  // without colliding.
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_idempotency
     ON transactions(space, idempotency_key) WHERE idempotency_key <> ''`,

  // --- the MemorySpace registry ------------------------------------------
  //
  // A Space is not a Domain: semantic organization does not confer ownership
  // (§30).
  `CREATE TABLE IF NOT EXISTS spaces (
     id                         INTEGER PRIMARY KEY AUTOINCREMENT,
     space_id                   TEXT NOT NULL,
     uri                        TEXT NOT NULL DEFAULT '',
     name                       TEXT NOT NULL DEFAULT '',
     description                TEXT NOT NULL DEFAULT '',
     owner_principal            TEXT NOT NULL DEFAULT '',
     owners                     TEXT NOT NULL DEFAULT '[]',
     status                     TEXT NOT NULL DEFAULT 'active',
     default_policy_id          TEXT NOT NULL DEFAULT '',
     trust_policy_id            TEXT NOT NULL DEFAULT '',
     default_classification     TEXT NOT NULL DEFAULT '',
     audit_mode                 TEXT NOT NULL DEFAULT '',
     created_at                 TEXT NOT NULL,
     seq                        INTEGER NOT NULL DEFAULT 0,
     schema_environment_version INTEGER NOT NULL DEFAULT 0,
     policies                   TEXT NOT NULL DEFAULT '{}'
   )`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_spaces_space_id
     ON spaces(space_id)`,

  // --- installed Schema Package artifacts --------------------------------
  //
  // Immutable: `package_id + version` identifies one canonical content
  // forever, and the same reference arriving with different content is an
  // integrity error rather than an update (§240.4). Installation is also not
  // activation — an installed package takes no part in resolution until
  // Governance says so (§240.18).
  `CREATE TABLE IF NOT EXISTS schema_packages (
     id              INTEGER PRIMARY KEY AUTOINCREMENT,
     package_ref     TEXT NOT NULL,
     package_id      TEXT NOT NULL,
     version         TEXT NOT NULL,
     content_digest  TEXT NOT NULL,
     declared_digest TEXT NOT NULL DEFAULT '',
     artifact        TEXT NOT NULL,
     installed_at    TEXT NOT NULL,
     source          TEXT NOT NULL DEFAULT ''
   )`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_schema_packages_ref
     ON schema_packages(package_ref)`,
  `CREATE INDEX IF NOT EXISTS idx_schema_packages_id
     ON schema_packages(package_id, version)`,

  // --- Schema Environment versions ---------------------------------------
  //
  // Appended, never updated: a transaction records which environment version
  // it ran under (§144), and rewriting an environment in place would
  // retroactively change what those transactions meant.
  `CREATE TABLE IF NOT EXISTS schema_envs (
     id         INTEGER PRIMARY KEY AUTOINCREMENT,
     space      TEXT NOT NULL,
     version    INTEGER NOT NULL,
     lock       TEXT NOT NULL,
     created_at TEXT NOT NULL,
     tx_id      TEXT NOT NULL DEFAULT ''
   )`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_schema_envs_version
     ON schema_envs(space, version)`,

  // --- engine key/value sidecar ------------------------------------------
  `CREATE TABLE IF NOT EXISTS kip_meta (
     k TEXT PRIMARY KEY,
     v TEXT NOT NULL
   ) WITHOUT ROWID`,

  // === the Governance Control Plane ======================================
  //
  // Eight tables beside the cognitive ones, and the boundary between them is
  // the security property: **no KML clause reaches these**. They are written
  // through host APIs only, which is what keeps a prompt injection into
  // ordinary memory formation off the control plane (§264).
  //
  // Three shapes repeat across all of them and each is load-bearing:
  //
  // - **`status` is a column, not a delete.** Revoking a Grant must stop it
  //   authorizing future operations without rewriting the audit that says it
  //   authorized a past one (§36), so every lookup filters on status and every
  //   *historical* lookup ignores it and reads the timestamps instead.
  // - **`created_at` / `revoked_at` bound a record in time.** `AS OF` over the
  //   control plane is answered from these, which is why revocation stamps
  //   `revoked_at` rather than only flipping `status`.
  // - **`version` counts changes**, so a cached decision can be invalidated
  //   (§187).

  // --- Principals ---------------------------------------------------------
  //
  // A Principal is an authenticated execution identity, never a semantic
  // Person: the bridge between the two is an ActorBinding, under Governance
  // authority rather than by anyone writing a Proposition about it (§7, §14).
  `CREATE TABLE IF NOT EXISTS gov_principals (
     id              INTEGER PRIMARY KEY AUTOINCREMENT,
     principal_id    TEXT NOT NULL,
     principal_class TEXT NOT NULL DEFAULT '',
     status          TEXT NOT NULL DEFAULT 'active',
     display_name    TEXT NOT NULL DEFAULT '',
     auth_provider   TEXT NOT NULL DEFAULT '',
     auth_subject    TEXT NOT NULL DEFAULT '',
     created_at      TEXT NOT NULL,
     updated_at      TEXT NOT NULL DEFAULT '',
     revoked_at      TEXT NOT NULL DEFAULT '',
     version         INTEGER NOT NULL DEFAULT 1
   )`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_gov_principals_id
     ON gov_principals(principal_id)`,

  // --- Principal groups ---------------------------------------------------
  //
  // Membership is one current list on the row rather than a join table. "Which
  // groups is this Principal in" is therefore a scan over the groups with
  // `json_each`, not an index seek — acceptable because a deployment has few
  // groups and every authorization asks the question once, and *correct*
  // because the historical answer is replayed from the audit rather than from
  // this list anyway (§177). A join table would give the live question an index
  // and give the historical one a second thing to keep in step.
  `CREATE TABLE IF NOT EXISTS gov_principal_groups (
     id          INTEGER PRIMARY KEY AUTOINCREMENT,
     group_id    TEXT NOT NULL,
     name        TEXT NOT NULL DEFAULT '',
     description TEXT NOT NULL DEFAULT '',
     members     TEXT NOT NULL DEFAULT '[]',
     status      TEXT NOT NULL DEFAULT 'active',
     created_at  TEXT NOT NULL,
     updated_at  TEXT NOT NULL DEFAULT '',
     version     INTEGER NOT NULL DEFAULT 1
   )`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_gov_groups_id
     ON gov_principal_groups(group_id)`,

  // --- ActorBindings ------------------------------------------------------
  //
  // `actor_key` is an endpoint key and `actor_ref` is what the caller typed.
  // Both are kept: the key is what `assertions.asserted_by_key` is compared
  // against, and a binding stored in any other spelling would silently never
  // match — the worst failure for a record whose whole job is to be found.
  `CREATE TABLE IF NOT EXISTS gov_actor_bindings (
     id            INTEGER PRIMARY KEY AUTOINCREMENT,
     principal_id  TEXT NOT NULL,
     actor_ref     TEXT NOT NULL DEFAULT '',
     actor_key     TEXT NOT NULL DEFAULT '',
     binding_class TEXT NOT NULL DEFAULT '',
     assurance     TEXT NOT NULL DEFAULT '',
     scope         TEXT NOT NULL DEFAULT '*',
     status        TEXT NOT NULL DEFAULT 'active',
     created_at    TEXT NOT NULL,
     updated_at    TEXT NOT NULL DEFAULT '',
     revoked_at    TEXT NOT NULL DEFAULT '',
     version       INTEGER NOT NULL DEFAULT 1
   )`,
  `CREATE INDEX IF NOT EXISTS idx_gov_bindings_principal
     ON gov_actor_bindings(principal_id, status)`,
  `CREATE INDEX IF NOT EXISTS idx_gov_bindings_actor
     ON gov_actor_bindings(actor_key, status)`,

  // --- Grants -------------------------------------------------------------
  //
  // Exactly one Space per Grant, never a pattern: a Space is the authorization
  // boundary and URI structure confers nothing (§22).
  `CREATE TABLE IF NOT EXISTS gov_grants (
     id                 INTEGER PRIMARY KEY AUTOINCREMENT,
     space_id           TEXT NOT NULL,
     grantee_principal  TEXT NOT NULL DEFAULT '',
     grantee_group      TEXT NOT NULL DEFAULT '',
     actions            TEXT NOT NULL DEFAULT '[]',
     scope              TEXT NOT NULL DEFAULT '{}',
     conditions         TEXT NOT NULL DEFAULT '{}',
     constraints        TEXT NOT NULL DEFAULT '{}',
     delegation_allowed INTEGER NOT NULL DEFAULT 0,
     status             TEXT NOT NULL DEFAULT 'active',
     granted_by         TEXT NOT NULL DEFAULT '',
     created_at         TEXT NOT NULL,
     updated_at         TEXT NOT NULL DEFAULT '',
     revoked_at         TEXT NOT NULL DEFAULT '',
     version            INTEGER NOT NULL DEFAULT 1
   )`,
  // The two grantee columns are indexed separately rather than together: a
  // decision loads the Principal's own Grants and its groups' Grants and
  // evaluates them as one standing, so neither lookup ever carries the other's
  // key.
  `CREATE INDEX IF NOT EXISTS idx_gov_grants_principal
     ON gov_grants(space_id, grantee_principal, status)`,
  `CREATE INDEX IF NOT EXISTS idx_gov_grants_group
     ON gov_grants(space_id, grantee_group, status)`,

  // --- Delegations --------------------------------------------------------
  //
  // A separate table from Grants because the two are *evaluated* differently: a
  // Grant is checked against its own record, a Delegation against a record plus
  // a live question — does the delegator still hold this? — whose answer can
  // change without this row changing at all (§35).
  `CREATE TABLE IF NOT EXISTS gov_delegations (
     id                   INTEGER PRIMARY KEY AUTOINCREMENT,
     space_id             TEXT NOT NULL,
     delegator_principal  TEXT NOT NULL,
     delegate_principal   TEXT NOT NULL,
     actions              TEXT NOT NULL DEFAULT '[]',
     scope                TEXT NOT NULL DEFAULT '{}',
     conditions           TEXT NOT NULL DEFAULT '{}',
     constraints          TEXT NOT NULL DEFAULT '{}',
     parent_delegation    TEXT NOT NULL DEFAULT '',
     may_redelegate       INTEGER NOT NULL DEFAULT 0,
     status               TEXT NOT NULL DEFAULT 'active',
     created_at           TEXT NOT NULL,
     updated_at           TEXT NOT NULL DEFAULT '',
     revoked_at           TEXT NOT NULL DEFAULT '',
     version              INTEGER NOT NULL DEFAULT 1
   )`,
  `CREATE INDEX IF NOT EXISTS idx_gov_delegations_delegate
     ON gov_delegations(space_id, delegate_principal, status)`,
  `CREATE INDEX IF NOT EXISTS idx_gov_delegations_delegator
     ON gov_delegations(space_id, delegator_principal, status)`,

  // --- Governance Policy versions -----------------------------------------
  //
  // Appended, never updated, exactly like the Schema Environment log: an audit
  // has to answer *which policy version authorized this operation*, and
  // rewriting a policy in place retroactively changes that answer (§46).
  `CREATE TABLE IF NOT EXISTS gov_policies (
     id          INTEGER PRIMARY KEY AUTOINCREMENT,
     policy_ref  TEXT NOT NULL,
     policy_id   TEXT NOT NULL,
     version     INTEGER NOT NULL,
     space_id    TEXT NOT NULL DEFAULT '*',
     description TEXT NOT NULL DEFAULT '',
     statements  TEXT NOT NULL DEFAULT '[]',
     created_at  TEXT NOT NULL,
     created_by  TEXT NOT NULL DEFAULT ''
   )`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_gov_policies_ref
     ON gov_policies(policy_ref)`,
  `CREATE INDEX IF NOT EXISTS idx_gov_policies_id
     ON gov_policies(policy_id, version)`,

  // --- Approvals ----------------------------------------------------------
  //
  // `subject_digest` binds an approval to one concrete operation. Without it an
  // approval for "purge this one Evidence record" would authorize purging
  // anything (§246).
  `CREATE TABLE IF NOT EXISTS gov_approvals (
     id                  INTEGER PRIMARY KEY AUTOINCREMENT,
     space_id            TEXT NOT NULL,
     operation           TEXT NOT NULL DEFAULT '',
     resource            TEXT NOT NULL DEFAULT '',
     subject_digest      TEXT NOT NULL DEFAULT '',
     required            INTEGER NOT NULL DEFAULT 1,
     approvals           TEXT NOT NULL DEFAULT '[]',
     approver_ids        TEXT NOT NULL DEFAULT '[]',
     allow_self_approval INTEGER NOT NULL DEFAULT 0,
     status              TEXT NOT NULL DEFAULT 'pending',
     requested_by        TEXT NOT NULL DEFAULT '',
     created_at          TEXT NOT NULL,
     updated_at          TEXT NOT NULL DEFAULT '',
     expires_at          TEXT NOT NULL DEFAULT '',
     version             INTEGER NOT NULL DEFAULT 1
   )`,
  `CREATE INDEX IF NOT EXISTS idx_gov_approvals_subject
     ON gov_approvals(space_id, subject_digest, status)`,

  // --- the Governance audit log -------------------------------------------
  //
  // Append-only, and it stores whole records rather than diffs for the same
  // reason the element version log does: a diff chain with one missing link
  // answers a historical question wrongly instead of refusing (§175). It is
  // also what a Principal's *past* group membership is replayed from, since the
  // group row only says who is in it now.
  `CREATE TABLE IF NOT EXISTS gov_audit (
     id               INTEGER PRIMARY KEY AUTOINCREMENT,
     entry_class      TEXT NOT NULL DEFAULT '',
     at               TEXT NOT NULL,
     space_id         TEXT NOT NULL DEFAULT '*',
     principal_id     TEXT NOT NULL DEFAULT '',
     delegation_chain TEXT NOT NULL DEFAULT '[]',
     operation        TEXT NOT NULL DEFAULT '',
     resource         TEXT NOT NULL DEFAULT '',
     decision         TEXT NOT NULL DEFAULT '',
     reason           TEXT NOT NULL DEFAULT '',
     policy_id        TEXT NOT NULL DEFAULT '',
     policy_version   INTEGER NOT NULL DEFAULT 0,
     authorities_used TEXT NOT NULL DEFAULT '[]',
     approvals        TEXT NOT NULL DEFAULT '[]',
     obligations      TEXT NOT NULL DEFAULT '{}',
     record           TEXT NOT NULL DEFAULT 'null',
     request_id       TEXT NOT NULL DEFAULT '',
     tx_id            TEXT NOT NULL DEFAULT ''
   )`,
  `CREATE INDEX IF NOT EXISTS idx_gov_audit_space
     ON gov_audit(space_id, id)`,
  `CREATE INDEX IF NOT EXISTS idx_gov_audit_operation
     ON gov_audit(operation, at)`,
]

/**
 * Enables connection-local SQLite settings.
 *
 * These are per connection, not persisted with the schema, so this has to run
 * whenever a Durable Object instance is constructed even when the schema
 * itself is already current.
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

  const previous = metaGet(sql, 'schema_version')
  if (previous !== null && Number(previous) > SCHEMA_VERSION) {
    // A database written by a newer build has columns this one does not know
    // to maintain. Reading it would silently drop them on the next write.
    throw errors.internalError(
      `this database was written at schema version ${previous}, and this ` +
        `build understands ${SCHEMA_VERSION}`,
    )
  }
  metaSet(sql, 'schema_version', String(SCHEMA_VERSION))
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
