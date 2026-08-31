/**
 * The Cognitive Nexus: one KIP 2.0 engine over one Durable Object's SQLite.
 *
 * This is the seam a host holds. It owns the Space registry, the Schema
 * Environment, and the transaction boundary — and it is deliberately thin,
 * because everything interesting lives in the layers below it.
 *
 * The transaction boundary is the one thing it cannot delegate.
 * `ctx.storage.transactionSync` gives real all-or-nothing commit, so a KML
 * statement either lands whole or leaves nothing behind, shells included. The
 * Rust engine has no equivalent and recovers by sweeping `pending` rows on
 * open; this one gets the property from the platform.
 */

import { errors, KipError } from './errors.js'
import { formatElementId } from './id.js'

/**
 * What one retention sweep did, and what it left alone (§19.1).
 *
 * The counts are the point. A sweep that reported only what it touched would
 * read as complete, and "swept 4" when 9 expired is the shape of a compliance
 * failure nobody notices.
 */
export interface RetentionSweep {
  /** The elements it acted on. */
  swept: string[]
  /** How many were kept because a legal hold blocks removal (§19.1). */
  held: number
  /** How many the caller was not authorized to act on. */
  refused: number
  /** How many were left for the next sweep by `limit`. */
  remaining: number
}
import {
  EffectiveAuthority,
  archiveExpired,
  classify,
  Approved,
  elevateAuthority,
  isPermitted,
  parsePermission,
  principalClass,
  expireAssertion,
  quarantine,
  release,
  tombstoneExpired,
  requirePermitted,
  resolveApproval,
  spaceResource,
  systemAuth,
  type ActorBindingRow,
  type ApprovalRow,
  type AuthContext,
  type Authorization,
  type DelegationRow,
  type ElementGovernanceContext,
  type GovernancePolicyRow,
  type GrantRow,
  type Permission,
  type PrincipalGroupRow,
  type PrincipalRow,
} from './governance/index.js'
import { kmlPermissions, kqlPermissions, metaPermissions } from './governance/gate.js'
import { isAlwaysAudited } from './governance/index.js'
import type { Json, JsonMap } from './json.js'
import { parseKip } from './kip/parser.js'
import type { ElementId } from './id.js'
import type { Command, KmlStatement, KqlQuery } from './kip/ast.js'
import { executeKml, type IngestContext, type KmlContext } from './kml/index.js'
import {
  executeKqlPage,
  type KqlAnswer,
  type KqlContext,
} from './kql/index.js'
import { executeMeta, type MetaContext } from './meta/index.js'
import {
  BUNDLED_PACKAGES,
  CORE_PACKAGE,
  CORE_PACKAGE_REF,
  SchemaEnvironment,
  emptyLock,
  lockFromJson,
  packageRefOf,
  formatPackageRef,
  parsePackage,
  type SchemaLock,
  type SchemaPackage,
} from './schema/index.js'
import {
  Store,
  type TransactionRow,
  type ActorBindingDraft,
  type DelegationDraft,
  type GrantDraft,
  type GroupDraft,
  type PolicyDraft,
  type SpaceRow,
} from './store/index.js'
import { canonicalJson } from './json.js'
import { sha256Text } from './digest.js'
import { normalizeTime, nowTime } from './time.js'
import type { Outcome } from './tx.js'

/** The Space a Nexus uses when the caller names none. */
export const DEFAULT_SPACE = 'kip:space:default'

/** The Principal a Nexus attributes its own bootstrap writes to. */
export const SYSTEM_PRINCIPAL = 'kip:principal:system'

/** Options a host may set when connecting. */
export interface NexusOptions {
  /** The Space every command runs in unless told otherwise. */
  space?: string
  /**
   * Whether to install the bundled Schema Package artifacts.
   *
   * Installing is not activating (§20.12): a host still has to say which
   * packages its Space resolves through.
   */
  installBundled?: boolean
}

export class CognitiveNexus {
  readonly store: Store
  readonly space: string
  private readonly storage: DurableObjectStorage

  private constructor(
    storage: DurableObjectStorage,
    store: Store,
    space: string,
  ) {
    this.storage = storage
    this.store = store
    this.space = space
  }

  /** Opens (or creates) the Nexus held by one Durable Object. */
  static connect(
    storage: DurableObjectStorage,
    options: NexusOptions = {},
  ): CognitiveNexus {
    const store = new Store(storage.sql)
    const space = options.space ?? DEFAULT_SPACE
    const nexus = new CognitiveNexus(storage, store, space)

    storage.transactionSync(() => {
      // A shell surviving a crash is impossible while every statement runs in
      // a transaction; the sweep stays because the invariant is cheap to
      // enforce and a surviving `pending` row would be invisible rather than
      // obviously wrong.
      store.sweepPending()
      // Default deny would lock an embedded host out of its own database, so
      // the system Principal exists and owns the default Space. That is not a
      // bypass: the in-process host runs with owner authority *through* the
      // authorization path, so a Space whose policy denies something denies it
      // here too (§28.2).
      store.governance.ensurePrincipal({
        principal_id: SYSTEM_PRINCIPAL,
        principal_class: principalClass.SYSTEM,
        display_name: 'the engine itself',
        auth_provider: 'engine',
      })
      if (store.space(space) === null) nexus.registerSpace(space)
      if (options.installBundled !== false) {
        for (const artifact of [CORE_PACKAGE, ...BUNDLED_PACKAGES]) {
          nexus.installPackage(artifact, 'bundled')
        }
      }
    })
    return nexus
  }

  /** The Space registry row. */
  spaceRow(space = this.space): SpaceRow {
    const row = this.store.space(space)
    if (row === null) {
      throw errors.notFoundOrNotVisible(`no MemorySpace ${space}`)
    }
    return row
  }

  /**
   * Installs one Schema Package artifact.
   *
   * Immutable by reference: the same `package_id@version` arriving with
   * different content is an integrity error rather than an update (§20.4), so
   * a re-install of identical bytes is a no-op and a changed one is refused.
   */
  installPackage(artifact: SchemaPackage, source: string): void {
    const ref = formatPackageRef(packageRefOf(artifact))
    const digest = sha256Text(canonicalJson(artifact))
    const existing = this.store.packageByRef(ref)
    if (existing !== null) {
      if (existing.content_digest !== digest) {
        throw errors.digestMismatch(
          `${ref} is already installed with different content; a package ` +
            `version identifies one canonical content forever`,
        )
      }
      return
    }
    const declared = artifact.integrity?.content_digest ?? ''
    this.store.installPackage({
      package_ref: ref,
      package_id: packageRefOf(artifact).packageId,
      version: artifact.manifest?.version ?? '',
      content_digest: digest,
      // Recorded, not verified: the artifact's own digest is computed under a
      // canonicalization profile that is still a draft.
      declared_digest: declared,
      artifact: artifact as unknown as JsonMap,
      installed_at: nowTime(),
      source,
    })
  }

  /**
   * Activates a Schema Lock, minting a new environment version.
   *
   * An identical lock is not re-activated: every activation mints a version
   * that transactions record (§20.9), and a restart is not a schema change.
   */
  ensureSchema(lock: SchemaLock, space = this.space): SchemaEnvironment {
    return this.storage.transactionSync(() => {
      const current = this.store.schemaEnv(space)
      if (
        current !== null &&
        canonicalJson(current.lock) === canonicalJson(lock)
      ) {
        return this.environment(space)
      }
      // Resolving first is what stops a lock naming an uninstalled package
      // from becoming the Space's environment: it fails here, not at the first
      // symbol lookup somewhere unrelated.
      const version = (current?.version ?? 0) + 1
      const env = SchemaEnvironment.resolve(version, lock, this.artifacts())
      const snapshotSeq = this.store.currentSeq(space)
      const firstActivation = current === null
      const seq = firstActivation ? snapshotSeq + 1 : this.store.nextSeq(space)
      const activatedAt = nowTime()
      const txId = firstActivation ? '' : `tx-${space}-${seq}-${activatedAt}`
      this.store.appendSchemaEnv({
        space,
        version,
        lock: lock as unknown as JsonMap,
        created_at: activatedAt,
        tx_id: txId,
        seq,
      })
      const row = this.spaceRow(space)
      row.schema_environment_version = version
      this.store.putSpace(row)
      if (!firstActivation) {
        this.store.putTransaction({
          tx_id: txId,
          space,
          seq,
          snapshot_seq: snapshotSeq,
          committed_at: activatedAt,
          status: 'committed',
          transaction_class: 'governance',
          idempotency_key: '',
          request_digest: '',
          semantic_plan_digest: '',
          result_digest: '',
          schema_environment_version: version,
          result: { schema_environment_version: version },
          changes: [],
        })
      }
      return env
    })
  }

  /** Activates exactly the named packages, installing them first if given. */
  activatePackages(
    artifacts: readonly (SchemaPackage | string)[],
    space = this.space,
  ): SchemaEnvironment {
    const lock = emptyLock()
    lock.packages['kip://core'] = '2.0.0'
    lock.states['kip://core'] = 'active'
    for (const source of artifacts) {
      const artifact =
        typeof source === 'string' ? parsePackage(source) : source
      this.installPackage(artifact, 'host')
      const ref = packageRefOf(artifact)
      lock.packages[ref.packageId] = artifact.manifest?.version ?? ''
      lock.states[ref.packageId] = 'active'
    }
    return this.ensureSchema(lock, space)
  }

  /** The Space's current Schema Environment. */
  environment(space = this.space): SchemaEnvironment {
    const row = this.store.schemaEnv(space)
    if (row === null) return SchemaEnvironment.coreOnly()
    return SchemaEnvironment.resolve(
      row.version,
      lockFromJson(row.lock),
      this.artifacts(),
    )
  }

  /**
   * A Space's Schema Environment at one version (§20.9).
   *
   * What a historical read resolves symbols through. Version 0 is the Core-only
   * environment a Space has before it activates anything — an honest answer
   * rather than a missing one, because a read at a coordinate before the first
   * activation happened under exactly that.
   */
  environmentAt(space: string, version: number): SchemaEnvironment {
    if (version === 0) return SchemaEnvironment.coreOnly()
    const row = this.store.schemaEnv(space, version)
    if (row === null) {
      throw errors.historicalSchemaUnavailable(
        `${space} has no Schema Environment version ${version}; the coordinate ` +
          `cannot be resolved under the schema that was in force at it`,
      )
    }
    return SchemaEnvironment.resolve(
      row.version,
      lockFromJson(row.lock),
      this.artifacts(),
    )
  }

  private artifacts(): Map<string, SchemaPackage> {
    const out = new Map<string, SchemaPackage>([
      [CORE_PACKAGE_REF, CORE_PACKAGE],
    ])
    for (const row of this.store.packages()) {
      out.set(row.package_ref, row.artifact as unknown as SchemaPackage)
    }
    return out
  }

  /**
   * One authenticated caller's view of this Nexus.
   *
   * This is what a multi-tenant host executes through: it authenticates the
   * caller itself, builds an {@link AuthContext} from what it *observed*, and
   * every command run here is authorized against the control plane before it
   * touches anything.
   *
   * A session holds identity, not authority. Authority is resolved from the
   * control plane on each request, so a session that has been running since
   * January does not still hold what January's Grants said (§28.6).
   */
  session(auth: AuthContext): Session {
    return new Session(this, auth)
  }

  /**
   * A session running as the engine itself.
   *
   * The embedded case: one object, one owner, and the object *is* the owner. A
   * host serving more than one caller must not use this — authenticate and go
   * through {@link CognitiveNexus.session}, or every caller is the owner.
   */
  systemSession(): Session {
    return this.session(systemAuth())
  }

  /** Parses and runs one KML statement, returning its receipt. */
  execute(command: string, params: JsonMap = {}): Outcome {
    return this.systemSession().execute(command, params)
  }

  /**
   * Parses and runs one META command.
   *
   * META is read-only by construction, with one exception the language makes
   * explicit: `PREVIEW KML` runs the real dry-run path, which writes nothing.
   */
  describe(command: string, params: JsonMap = {}): Json {
    return this.systemSession().describe(command, params)
  }

  /** Parses and runs one META command, reporting its page cursor. */
  describePage(
    command: string,
    params: JsonMap = {},
  ): { result: Json; nextCursor: string | null } {
    return this.systemSession().describePage(command, params)
  }

  /**
   * Parses and runs one KQL query, returning the bare result array.
   *
   * Reads take no transaction: a Durable Object is single-threaded, so nothing
   * can change underneath a query that has already started.
   */
  query(command: string, params: JsonMap = {}, read: ReadOptions = {}): Json[] {
    return this.systemSession().query(command, params, read)
  }

  /** Runs one KQL command, reporting its coordinates and page cursor (§50). */
  queryPage(
    command: string,
    params: JsonMap = {},
    read: ReadOptions = {},
  ): KqlAnswer {
    return this.systemSession().queryPage(command, params, read)
  }

  /** Runs one parsed KQL query, reporting its coordinates and page cursor. */
  findPage(
    query: KqlQuery,
    params: JsonMap = {},
    options: Partial<KqlContext> & ReadOptions = {},
  ): KqlAnswer {
    return this.systemSession().findPage(query, params, options)
  }

  /** Runs one parsed KQL query and returns its rows. */
  find(
    query: KqlQuery,
    params: JsonMap = {},
    options: Partial<KqlContext> & ReadOptions = {},
  ): Json[] {
    return this.findPage(query, params, options).rows
  }

  /** Runs one KML statement, all-or-nothing. */
  mutate(
    statement: KmlStatement,
    params: JsonMap = {},
    options: MutationOptions = {},
  ): Outcome {
    return this.systemSession().mutate(statement, params, options)
  }

  /** Runs a command and returns the failure instead of throwing it. */
  tryExecute(
    command: string,
    params: JsonMap = {},
  ): { ok: Outcome } | { error: KipError } {
    return this.systemSession().tryExecute(command, params)
  }

  /**
   * Runs `body` inside the object's transaction.
   *
   * Exposed for {@link Session}, which owns the command paths but not the
   * storage handle. The boundary is the platform's: a clause that throws
   * unwinds everything the statement wrote, shells included.
   *
   * @internal
   */
  transact<T>(body: () => T): T {
    return this.storage.transactionSync(body)
  }

  private registerSpace(space: string): void {
    this.store.createSpace({
      space_id: space,
      uri: '',
      name: space,
      description: 'A KIP 2.0 MemorySpace.',
      owner_principal: SYSTEM_PRINCIPAL,
      owners: [SYSTEM_PRINCIPAL],
      status: 'active',
      default_policy_id: '',
      trust_policy_id: '',
      // Never `public` by default: §95 forbids reading an absent
      // classification as freely disclosable.
      default_classification: 'internal',
      audit_mode: 'standard',
      created_at: nowTime(),
      seq: 0,
      schema_environment_version: 0,
      self_concept: '',
      policies: {} as Json as JsonMap,
    })
  }
}

/**
 * One authenticated caller's view of a Nexus.
 *
 * Every command runs through {@link Session.gate} first, which asks whether this
 * Principal may do this *here at all*. That is Space scope and deliberately so:
 * at this point no element has been read, and reading one to decide whether it
 * may be read would be the disclosure the check exists to prevent. Per-element
 * authorization happens where the elements are.
 *
 * The session caches identity and nothing else. Authority is resolved from the
 * control plane on every command, which is what makes a revocation take effect
 * for a session that started before it (§28.6).
 */
/**
 * The `read` block of a request envelope (§85).
 *
 * A snapshot token binds a read to the coordinate a previous `SNAPSHOT`
 * reported, which is how a caller makes several requests answer at one
 * coordinate rather than at whatever each of them happens to find.
 */
export interface ReadOptions {
  snapshot_token?: string
}

/** The execution knobs a caller may vary without changing engine truth. */
export interface MutationOptions {
  space?: string
  operation?: JsonMap
  idempotencyKey?: string
  dryRun?: boolean
  /**
   * The request envelope's ingestion context (§71.1).
   *
   * Minted into this statement's transaction, so the command cites `:key`
   * rather than retyping the observation into its own text.
   */
  ingest?: IngestContext
}

export class Session {
  readonly nexus: CognitiveNexus
  readonly auth: AuthContext

  constructor(nexus: CognitiveNexus, auth: AuthContext) {
    this.nexus = nexus
    this.auth = auth
  }

  /** What this Principal may do in a Space, resolved fresh. */
  effectiveAuthority(space = this.nexus.space): EffectiveAuthority {
    return EffectiveAuthority.resolve(this.nexus.store, space, this.auth)
  }

  /** Parses and runs one KML statement, returning its receipt. */
  execute(command: string, params: JsonMap = {}): Outcome {
    const parsed: Command = parseKip(command)
    if ('Kml' in parsed) return this.mutate(parsed.Kml, params)
    throw errors.languageMismatch(
      'this command is not a KML statement; a query has no receipt — use ' +
        'query() for KQL and describe() for META',
    )
  }

  /** Parses and runs one KQL query, returning the bare result array. */
  query(command: string, params: JsonMap = {}, read: ReadOptions = {}): Json[] {
    return this.queryPage(command, params, read).rows
  }

  /** Runs one KQL command, reporting its coordinates and page cursor (§50). */
  queryPage(
    command: string,
    params: JsonMap = {},
    read: ReadOptions = {},
  ): KqlAnswer {
    const parsed: Command = parseKip(command)
    if (!('Kql' in parsed)) {
      throw errors.languageMismatch('this command is not a KQL query')
    }
    return this.findPage(parsed.Kql, params, read)
  }

  /** Parses and runs one META command. */
  describe(command: string, params: JsonMap = {}): Json {
    return this.describePage(command, params).result
  }

  /**
   * Parses and runs one META command, reporting its page cursor.
   *
   * `LIST` answers with a bare array and `SEARCH` with an object carrying its
   * own `next_cursor`, so the token a caller needs to continue a `LIST` has
   * nowhere to ride in the body — it comes back here instead. A cursor is
   * opaque (§88.4), so a caller that is never handed one cannot page at all.
   */
  describePage(
    command: string,
    params: JsonMap = {},
  ): { result: Json; nextCursor: string | null } {
    const parsed: Command = parseKip(command)
    if (!('Meta' in parsed)) {
      throw errors.languageMismatch('this command is not a META command')
    }
    const space = this.nexus.space
    const authority = this.effectiveAuthority(space)
    const decisions = this.gate(authority, metaPermissions(parsed.Meta))
    const page: { next_cursor?: string } = {}
    const cx: MetaContext = {
      store: this.nexus.store,
      space,
      env: this.nexus.environment(space),
      request: params,
      environmentAt: (version) => this.nexus.environmentAt(space, version),
      authority,
      auth: this.auth,
      page,
    }
    // `PREVIEW KML` mints shells to allocate ids and then discards them, so it
    // runs inside a transaction like any other mutation path — one that is
    // simply never committed.
    const result = this.nexus.transact(() => {
      const answer = executeMeta(parsed.Meta, cx)
      this.consume(decisions)
      return answer
    })
    return { result, nextCursor: page.next_cursor ?? null }
  }

  /** Runs one parsed KQL query, reporting its coordinates and page cursor. */
  findPage(
    query: KqlQuery,
    params: JsonMap = {},
    options: Partial<KqlContext> & ReadOptions = {},
  ): KqlAnswer {
    const space = options.space ?? this.nexus.space
    const authority = this.effectiveAuthority(space)
    // Both spellings, because both reach `executeKql`: the envelope's
    // `snapshot_token` and the context's own `snapshotToken`. Gating on one of
    // them would let the other buy a historical read for the price of an
    // ordinary one, which is the whole reason this argument exists.
    const snapshotToken = options.snapshot_token ?? options.snapshotToken
    const decisions = this.gate(
      authority,
      kqlPermissions(query, snapshotToken !== undefined),
    )
    const result = executeKqlPage(query, {
      store: this.nexus.store,
      space,
      env: this.nexus.environment(space),
      request: params,
      environmentAt: (version) => this.nexus.environmentAt(space, version),
      ...options,
      snapshotToken,
      // After the spread: a caller may vary the Space or the parameters, and
      // must not be able to vary who it is by passing an `options` object.
      authority,
      auth: this.auth,
    })
    this.consume(decisions)
    return result
  }

  /** Runs one parsed KQL query and returns its rows. */
  find(
    query: KqlQuery,
    params: JsonMap = {},
    options: Partial<KqlContext> & ReadOptions = {},
  ): Json[] {
    return this.findPage(query, params, options).rows
  }

  /** Runs one KML statement, all-or-nothing. */
  mutate(
    statement: KmlStatement,
    params: JsonMap = {},
    options: MutationOptions = {},
  ): Outcome {
    const space = options.space ?? this.nexus.space
    const authority = this.effectiveAuthority(space)
    const needed = kmlPermissions(statement)

    // §26, §33: a timeout is not an abort. A client that lost its response
    // resends the same key and gets the outcome its first attempt produced,
    // rather than writing a second time or being told its own write is a
    // conflict.
    //
    // Authorized before it answers — a replay is still a read of what this
    // Space did — but deliberately *without* resolving approvals: nothing is
    // being done a second time, and an approval the first attempt already
    // spent must not make a lost response unrecoverable.
    const replayed =
      options.idempotencyKey === undefined || options.dryRun === true
        ? null
        : this.nexus.store.transactionByKey(space, options.idempotencyKey)
    if (replayed !== null) {
      for (const permission of needed) {
        requirePermittedForReplay(
          authority.authorize(permission, spaceResource(), this.auth),
        )
      }
      return replay(replayed)
    }

    const decisions = this.gate(authority, needed)
    const provenance = accessProvenance(statement, authority, this.auth)
    const cx: KmlContext = {
      store: this.nexus.store,
      space,
      env: this.nexus.environment(space),
      // `_system.origin` records who the runtime *observed*, never what the
      // content claimed (§26). It is the session's Principal, so an element
      // written under a revoked identity stays attributable to it.
      origin: this.origin(),
      request: params,
      ingest: options.ingest,
      operation: options.operation,
      idempotencyKey: options.idempotencyKey,
      dryRun: options.dryRun,
      // After the spread, for the same reason as the read path: identity is not
      // one of the knobs an options object may turn.
      authority,
      auth: this.auth,
    }
    const outcome = this.nexus.transact(() => {
      const committed = executeKml(statement, cx)
      this.consume(decisions)
      return committed
    })
    return provenance === null ? outcome : { ...outcome, governance: provenance }
  }

  /**
   * What this Principal could do in a Space at a past instant (§48.5).
   *
   * A historical answer, and nothing more: that a Principal could read something
   * in January says nothing about whether it can today (§48.5). Reading it needs
   * `read_governance_history`, which is separate from `read_audit` — one is what
   * the control plane *was*, the other is what people *did*.
   */
  accessAsOf(at: string, space = this.nexus.space): Json {
    const now = this.effectiveAuthority(space)
    requirePermitted(
      now.authorize('read_governance_history', spaceResource(), this.auth),
    )
    const then = EffectiveAuthority.resolveAt(
      this.nexus.store,
      space,
      this.auth,
      normalizeTime(at, 'AS OF'),
    )
    return {
      at,
      space_id: space,
      principal_id: then.principal.principal_id,
      groups: then.groups,
      is_space_owner: then.isOwner,
      permissions: then.permissionNames(this.auth),
      policy:
        then.policy === null
          ? null
          : `${then.policy.policy_id}@${then.policy.version}`,
      // Said out loud rather than implied: reconstructing a whole historical
      // delegation chain would need the delegator's historical Grants
      // recursively, so this report does not claim a precision it lacks.
      caveats: [
        'Delegations are resolved against their delegator’s authority as it ' +
          'stands now, not as it stood then',
        'this is what the control plane said at that instant, and says nothing ' +
          'about today',
      ],
    } as Json
  }

  /** Runs a command and returns the failure instead of throwing it. */
  tryExecute(
    command: string,
    params: JsonMap = {},
  ): { ok: Outcome } | { error: KipError } {
    try {
      return { ok: this.execute(command, params) }
    } catch (err) {
      return { error: KipError.from(err) }
    }
  }

  /**
   * Reads the Governance audit for a Space (§29).
   *
   * Its own permission, because the audit says what everyone else did: a caller
   * who may read a Space's cognition has not thereby earned the right to read
   * who has been reading it.
   */
  readAudit(limit = 50, space = this.nexus.space) {
    const authority = this.effectiveAuthority(space)
    requirePermitted(authority.authorize('read_audit', spaceResource(), this.auth))
    return this.nexus.store.governance.readAudit(space, limit)
  }

  /**
   * Sets one element's classification (§93, §100).
   *
   * A Governance operation rather than a KML clause, because an element's
   * `governance` block is not author-writable: the parser refuses it in every
   * assignment. Raising a label needs `update` and lowering one needs
   * `declassify` — it is disclosure that requires authority, not caution.
   *
   * Returns the label the element carried before.
   */
  classify(element: ElementId, label: string, space = this.nexus.space): string {
    return this.nexus.transact(() =>
      classify(this.governanceContext(space), element, label),
    )
  }

  /**
   * Raises or lowers how strongly one element may influence action.
   *
   * Raising is bounded by the element's authority lineage, so no chain of
   * summarizing turns a descriptive note into an executable one (§31.5). Lowering
   * is deliberately as easy as the permission itself: an incident response that
   * had to wait for an approval would arrive late (§31.5).
   *
   * Returns the ceiling the element carried before.
   */
  elevateAuthority(element: ElementId, cls: string, space = this.nexus.space): string {
    return this.nexus.transact(() =>
      elevateAuthority(this.governanceContext(space), element, cls),
    )
  }

  /**
   * Holds an element out of ordinary use, pending review (§39.2).
   *
   * Not a retraction: it says this Brain does not currently allow ordinary use
   * of the element, which is a statement about this Brain and not about whoever
   * wrote it (§39.2).
   */
  quarantine(element: ElementId, reason: string, space = this.nexus.space): void {
    this.nexus.transact(() =>
      quarantine(this.governanceContext(space), element, reason),
    )
  }

  /** Returns a quarantined element to ordinary use. */
  releaseQuarantine(element: ElementId, space = this.nexus.space): void {
    this.nexus.transact(() => release(this.governanceContext(space), element))
  }

  /**
   * Designates the Concept this Space treats as its semantic `$self` (§5.6).
   *
   * A Governance operation and not a KML clause, because §5.6 makes the
   * designation protected Space configuration: "ordinary KML MUST NOT create
   * or change it". Cognitive content that could name the Brain's own identity
   * would be content deciding who the Brain is, which is the laundering §88.8
   * is about.
   *
   * Every Capsule rule about source and destination `$self` (§38.4, §38.5)
   * refers to this designation, and a Space that has designated none has no
   * `$self` for those rules to map onto.
   *
   * Pass `null` to clear it.
   */
  designateSelf(concept: ElementId | null, space = this.nexus.space): void {
    this.nexus.transact(() => {
      const authority = this.effectiveAuthority(space)
      this.consume(this.gate(authority, ['manage_policy']))
      const row = this.nexus.store.space(space)
      if (row === null) {
        throw errors.notFoundOrNotVisible(`no MemorySpace ${space}`)
      }
      if (concept === null) {
        this.nexus.store.putSpace({ ...row, self_concept: '' })
        return
      }
      // Refused rather than stored as a name nothing resolves: every `$self`
      // rule downstream dereferences it, and a dangling one would make the
      // Space's own identity a broken link.
      const element = this.nexus.store.load(concept)
      if (element === null || element.row.space !== space) {
        throw errors.structuralReferenceInvalid(
          `${formatElementId(concept)} is not a Concept in this Space; a self ` +
            `identity is Space-local (§5.3)`,
        )
      }
      if (element.kind !== 'Concept') {
        throw errors.structuralReferenceInvalid(
          `${formatElementId(concept)} is a ${element.kind}; a Space's self ` +
            `identity is a Concept (§5.6)`,
        )
      }
      this.nexus.store.putSpace({
        ...row,
        self_concept: formatElementId(concept),
      })
    })
  }

  /**
   * Acts on the elements whose retention has lapsed (§19.1, §19.2).
   *
   * `retention.expires_at` says when the *record* stops being kept. It is not
   * `valid_time.until`, which says when the claim stops applying, and it is not
   * archival, which says the element is out of ordinary recall while still
   * being kept.
   *
   * An explicit sweep rather than a background timer, and the capability answer
   * says so. A Durable Object could schedule an alarm; one that deleted memory
   * on its own schedule would act while no request was in flight and no
   * Principal was accountable for it. The host decides when forgetting happens;
   * the engine decides what may be forgotten.
   *
   * Four gates, in this order: `manage_retention` at Space scope, the action's
   * own permission per element, the legal hold (§19.1), and per-element
   * authorization. A held or unauthorized element is **skipped and counted**,
   * not silently dropped: "swept 4" when 9 expired is the shape of a compliance
   * failure nobody notices.
   *
   * Purge is deliberately not an action here. §19.3 makes physical erasure a
   * high-impact operation with its own reference policy and its own destruction
   * of the version log; running it over a set the caller never enumerated would
   * be the largest irreversible action this engine can take, reached by a
   * maintenance call.
   */
  sweepExpired(
    action: 'archive' | 'tombstone' = 'tombstone',
    limit = 100,
    space = this.nexus.space,
  ): RetentionSweep {
    return this.nexus.transact(() => {
      const authority = this.effectiveAuthority(space)
      this.consume(this.gate(authority, ['manage_retention']))
      const cx = this.governanceContext(space)
      const report: RetentionSweep = {
        swept: [],
        held: 0,
        refused: 0,
        remaining: 0,
      }
      for (const id of this.nexus.store.expiredElements(space, nowTime())) {
        if (report.swept.length >= limit) {
          report.remaining += 1
          continue
        }
        const element = this.nexus.store.load(id)
        if (element === null) continue
        // §19.1: a hold blocks removal for everyone, including a sweep the
        // holder authorized. Reported as held rather than as failed, because
        // nothing went wrong — the record is being kept on purpose.
        if (element.row.retention.legal_hold === true) {
          report.held += 1
          continue
        }
        try {
          const changed =
            action === 'archive'
              ? archiveExpired(cx, id)
              : tombstoneExpired(cx, id)
          if (changed) report.swept.push(formatElementId(id))
        } catch {
          report.refused += 1
        }
      }
      return report
    })
  }

  /**
   * Marks the Assertions whose validity windows have closed as `expired`.
   *
   * §14.3's lifecycle state, reached explicitly. The alternative — deriving it
   * on every read and never recording it — leaves `expired` as a state the
   * model names and nothing produces, and leaves a caller unable to ask which
   * claims have lapsed without recomputing the answer itself.
   *
   * Not retraction and not supersession (§14.1, §14.2): nobody withdrew these
   * and nothing replaced them; their own stated windows ran out.
   */
  expireLapsedAssertions(limit = 100, space = this.nexus.space): string[] {
    return this.nexus.transact(() => {
      const cx = this.governanceContext(space)
      const now = nowTime()
      const expired: string[] = []
      for (const id of this.nexus.store.lapsedAssertions(space, now)) {
        if (expired.length >= limit) break
        try {
          if (expireAssertion(cx, id, now)) expired.push(formatElementId(id))
        } catch {
          // An Assertion this caller may not maintain stays as it is; the
          // sweep is not a way around per-element authorization.
        }
      }
      return expired
    })
  }

  // --- the governed control plane -----------------------------------------
  //
  // §29 registers a name for each control-plane operation, and until these
  // existed no gate asked for any of them: a Grant listing `manage_grants`
  // conferred nothing, which is the failure mode the registry exists to
  // prevent — authority that looks conferred and is not, discovered during an
  // incident.
  //
  // These do not put the control plane in reach of cognition. No KML clause
  // and no META command resolves to any of them, which is what keeps a prompt
  // injection off the plane; they are host calls, and what changed is that a
  // host call made *as a Principal* is now authorized as that Principal.
  // `nexus.store.governance` remains the host's own unguarded path, for the
  // bootstrap that has to happen before any Grant exists.

  /**
   * Creates a Grant in this Space (§29, `manage_grants`).
   *
   * The actions are checked against the registry before the record is written:
   * a Grant naming a permission this engine does not implement confers nothing,
   * and the holder must learn that here rather than during an incident.
   */
  createGrant(draft: GrantDraft, space = this.nexus.space): GrantRow {
    return this.nexus.transact(() => {
      const approvals = this.gate(this.effectiveAuthority(space), ['manage_grants'])
      for (const action of draft.actions) parsePermission(action)
      const row = this.nexus.store.governance.createGrant(
        { ...draft, space_id: space },
        this.auth.principal_id,
      )
      this.consume(approvals)
      return row
    })
  }

  /** Revokes a Grant (§29, `manage_grants`). Revoked, never deleted. */
  revokeGrant(id: number, space = this.nexus.space): void {
    this.nexus.transact(() => {
      const approvals = this.gate(this.effectiveAuthority(space), ['manage_grants'])
      this.nexus.store.governance.revokeGrant(id, this.auth.principal_id)
      this.consume(approvals)
    })
  }

  /**
   * Creates a Delegation (§29).
   *
   * Which permission this asks for depends on whose authority is being passed
   * on, and the distinction is the whole reason both names exist: conferring
   * part of *one's own* authority is `delegate`, and administering a
   * Delegation between two other Principals is `manage_delegation`. Collapsing
   * them would let anyone who may delegate their own authority hand out
   * somebody else's.
   */
  createDelegation(draft: DelegationDraft, space = this.nexus.space): DelegationRow {
    return this.nexus.transact(() => {
      const own = draft.delegator_principal === this.auth.principal_id
      const approvals = this.gate(this.effectiveAuthority(space), [
        own ? 'delegate' : 'manage_delegation',
      ])
      for (const action of draft.actions) parsePermission(action)
      const row = this.nexus.store.governance.createDelegation(
        { ...draft, space_id: space },
        this.auth.principal_id,
      )
      this.consume(approvals)
      return row
    })
  }

  /**
   * Revokes a Delegation (§29, `manage_delegation`).
   *
   * Revoking one's own asks for the same thing as revoking another's: unlike
   * conferring, withdrawing authority is never the more dangerous direction,
   * and a caller who could not reach the record could not withdraw at all.
   */
  revokeDelegation(id: number, space = this.nexus.space): void {
    this.nexus.transact(() => {
      const approvals = this.gate(this.effectiveAuthority(space), ['manage_delegation'])
      this.nexus.store.governance.revokeDelegation(id, this.auth.principal_id)
      this.consume(approvals)
    })
  }

  /** Creates or replaces a Principal group (§29, `manage_membership`). */
  putGroup(draft: GroupDraft, space = this.nexus.space): PrincipalGroupRow {
    return this.nexus.transact(() => {
      const approvals = this.gate(this.effectiveAuthority(space), ['manage_membership'])
      const row = this.nexus.store.governance.putGroup(draft, this.auth.principal_id)
      this.consume(approvals)
      return row
    })
  }

  /** Suspends or restores a Principal (§29, `manage_membership`). */
  setPrincipalStatus(
    principalId: string,
    status: string,
    space = this.nexus.space,
  ): PrincipalRow {
    return this.nexus.transact(() => {
      const approvals = this.gate(this.effectiveAuthority(space), ['manage_membership'])
      const row = this.nexus.store.governance.setPrincipalStatus(
        principalId,
        status,
        this.auth.principal_id,
      )
      this.consume(approvals)
      return row
    })
  }

  /**
   * Binds a Principal to a semantic actor (§17, `manage_actor_binding`).
   *
   * The record that decides whether writing `asserted_by: ?alice` is attributed
   * recording or speaking as Alice, so writing one is more authority than
   * either — a writer who could bind itself could authorize its own
   * impersonation.
   */
  createBinding(draft: ActorBindingDraft, space = this.nexus.space): ActorBindingRow {
    return this.nexus.transact(() => {
      const approvals = this.gate(this.effectiveAuthority(space), ['manage_actor_binding'])
      const row = this.nexus.store.governance.createBinding(
        draft,
        this.auth.principal_id,
      )
      this.consume(approvals)
      return row
    })
  }

  /** Revokes an ActorBinding (§17, `manage_actor_binding`). */
  revokeBinding(id: number, space = this.nexus.space): void {
    this.nexus.transact(() => {
      const approvals = this.gate(this.effectiveAuthority(space), ['manage_actor_binding'])
      this.nexus.store.governance.revokeBinding(id, this.auth.principal_id)
      this.consume(approvals)
    })
  }

  /** Publishes a Governance Policy version (§29, `manage_policy`). */
  publishPolicy(draft: PolicyDraft, space = this.nexus.space): GovernancePolicyRow {
    return this.nexus.transact(() => {
      const approvals = this.gate(this.effectiveAuthority(space), ['manage_policy'])
      const row = this.nexus.store.governance.publishPolicy(
        { ...draft, space_id: draft.space_id ?? space },
        this.auth.principal_id,
      )
      this.consume(approvals)
      return row
    })
  }

  /**
   * Supplies one of the independent approvals a high-risk operation needs
   * (§40, `approve_high_risk`).
   *
   * Its own permission rather than the operation's: the point of an
   * independent approval is that the approver is not the one asking, so the
   * authority to approve cannot be the authority to act.
   */
  approve(id: number, note = '', space = this.nexus.space): ApprovalRow {
    return this.nexus.transact(() => {
      const approvals = this.gate(this.effectiveAuthority(space), ['approve_high_risk'])
      const row = this.nexus.store.governance.approve(id, this.auth.principal_id, note)
      this.consume(approvals)
      return row
    })
  }

  /**
   * Installs a Schema Package artifact (§20, `manage_schema`).
   *
   * Installing does not activate: what a symbol means in this Space is decided
   * by the Schema Lock, and this only makes an artifact available to be locked
   * onto.
   */
  installPackage(artifact: SchemaPackage, source: string, space = this.nexus.space): void {
    this.nexus.transact(() => {
      const approvals = this.gate(this.effectiveAuthority(space), ['manage_schema'])
      this.nexus.installPackage(artifact, source)
      this.consume(approvals)
    })
  }

  /**
   * Activates a Schema Lock over the installed artifacts (§20, `manage_schema`).
   *
   * The operation that changes what every stored symbol resolves to, which is
   * why it is gated rather than treated as configuration: a package swapped
   * underneath a Space rewrites the meaning of cognition already written.
   */
  activatePackages(
    artifacts: readonly (SchemaPackage | string)[],
    space = this.nexus.space,
  ): SchemaEnvironment {
    return this.nexus.transact(() => {
      const approvals = this.gate(this.effectiveAuthority(space), ['manage_schema'])
      const env = this.nexus.activatePackages(artifacts, space)
      this.consume(approvals)
      return env
    })
  }

  private governanceContext(space: string): ElementGovernanceContext {
    return {
      store: this.nexus.store,
      space,
      authority: this.effectiveAuthority(space),
      auth: this.auth,
    }
  }

  /** The `_system.origin` this session stamps on what it writes. */
  private origin(): JsonMap {
    return {
      principal_id: this.auth.principal_id,
      ...(this.auth.client === '' ? {} : { channel: this.auth.client }),
    }
  }

  /**
   * Requires every permission a command asks for, at Space scope.
   *
   * A policy may require independent approval for a whole command family —
   * declassification, elevation, export — and a satisfied approval is what turns
   * that into an allow. An unsatisfied one stays a refusal: `require_approval`
   * is not a soft yes (§40).
   */
  private gate(
    authority: EffectiveAuthority,
    needed: readonly Permission[],
  ): Approved[] {
    const space = authority.space.space_id
    const resource = spaceResource()
    const decisions: Approved[] = []
    for (const permission of needed) {
      const decision = resolveApproval(
        this.nexus.store,
        space,
        resource,
        authority.authorize(permission, resource, this.auth),
        this.auth,
      )
      if (!isPermitted(decision.decision)) {
        this.audit(authority, decision)
      }
      const approved = Approved.require(decision)
      if (approved.decision.obligations.audit) {
        this.audit(authority, approved.decision)
      }
      decisions.push(approved)
    }
    return decisions
  }

  /** Spends approvals only after the operation they authorized succeeded. */
  private consume(approvals: readonly Approved[]): void {
    for (const approved of approvals) approved.spend(this.nexus.store)
  }

  /**
   * Writes one decision to the Governance audit.
   *
   * Best effort by design at this layer: a denial that could not be logged is
   * still a denial, and failing the request a second time over the log would turn
   * an audit outage into an availability outage. An obligation that genuinely
   * must not proceed unlogged is the caller's to enforce (§86.1).
   */
  private audit(authority: EffectiveAuthority, decision: Authorization): void {
    try {
      this.nexus.store.governance.recordDecision({
        at: nowTime(),
        space_id: authority.space.space_id,
        principal_id: this.auth.principal_id,
        delegation_chain: [...this.auth.delegation_chain],
        operation: decision.permission,
        decision: decision.decision,
        reason: decision.reason,
        policy_id: decision.policy_id,
        policy_version: decision.policy_version,
        authorities_used: [...decision.authorities_used],
      })
    } catch {
      // See above: an audit failure does not become a second failure mode.
    }
  }
}

/**
 * Whether this caller may be handed the outcome of a write it already made.
 *
 * The permission is checked exactly as it would be for the write itself, so a
 * caller who could not have run the command cannot learn what it did.
 *
 * An outstanding *approval* obligation deliberately does not block it. An
 * approval authorizes the work, and on a replay the work already happened —
 * demanding a second one to learn the outcome of the first is what would make
 * a lost response unrecoverable, which is the failure §33 exists to prevent.
 */
function requirePermittedForReplay(decision: Authorization): void {
  if (decision.decision === 'require_approval') return
  requirePermitted(decision)
}

/**
 * The outcome a recorded transaction produced, handed back on a resend.
 *
 * Everything a receipt carries was written at commit, so this reconstructs the
 * original answer rather than approximating it. Two things are honestly
 * different from the first response and say so:
 *
 * - `warnings` carries a replay notice. A caller that reads it learns its first
 *   attempt landed, which is the fact it resent to find out.
 * - The original run's own warnings are not persisted and are therefore gone. A
 *   replay that invented them would be worse than one that says nothing.
 */
function replay(row: TransactionRow): Outcome {
  const result = (row.result ?? {}) as { handles?: Record<string, string> }
  return {
    status: row.status === 'committed' ? 'committed' : 'no_effect',
    tx_id: row.tx_id,
    space_id: row.space,
    space_seq: row.status === 'committed' ? row.seq : null,
    snapshot_seq: row.snapshot_seq,
    committed_at: row.status === 'committed' ? row.committed_at : null,
    schema_environment_version: row.schema_environment_version,
    handles: result.handles ?? {},
    changes: row.changes,
    warnings: [
      `this is the recorded outcome of transaction ${row.tx_id}, replayed ` +
        `under the idempotency key it committed with: nothing ran a second ` +
        `time, and any warnings the first attempt reported are not kept`,
    ],
  }
}

/**
 * The access-decision provenance a high-impact receipt carries (§33.1).
 *
 * Only for high-impact statements. Attaching it to every commit would bury the
 * cases that matter under the ones that do not, and the point of the record is
 * that somebody reads it: an erasure, an export or a Governance change has to be
 * explainable later in terms of the identity and policy that authorized it.
 *
 * It names the effective Principal, the delegation chain and the policy version,
 * and deliberately not the Grants of anyone else.
 */
function accessProvenance(
  statement: KmlStatement,
  authority: EffectiveAuthority,
  auth: AuthContext,
): JsonMap | null {
  const permissions = kmlPermissions(statement)
  if (!permissions.some(isAlwaysAudited)) return null
  return {
    principal_id: auth.principal_id,
    delegation_chain: [...auth.delegation_chain],
    authentication_strength: auth.auth_strength,
    purpose: { value: auth.purpose, assurance: auth.purpose_assurance },
    policy:
      authority.policy === null
        ? null
        : { id: authority.policy.policy_id, version: authority.policy.version },
    operations: permissions,
  } as unknown as JsonMap
}
