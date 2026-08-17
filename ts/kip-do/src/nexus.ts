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
import {
  EffectiveAuthority,
  classify,
  Approved,
  elevateAuthority,
  isPermitted,
  principalClass,
  quarantine,
  release,
  requirePermitted,
  resolveApproval,
  spaceResource,
  systemAuth,
  type AuthContext,
  type Authorization,
  type ElementGovernanceContext,
  type Permission,
} from './governance/index.js'
import { kmlPermissions, kqlPermissions, metaPermissions } from './governance/gate.js'
import { isAlwaysAudited } from './governance/index.js'
import type { Json, JsonMap } from './json.js'
import { parseKip } from './kip/parser.js'
import type { ElementId } from './id.js'
import type { Command, KmlStatement, KqlQuery } from './kip/ast.js'
import { executeKml, type KmlContext } from './kml/index.js'
import { executeKql, type KqlContext } from './kql/index.js'
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
import { Store, type SpaceRow } from './store/index.js'
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
   * Installing is not activating (§240.18): a host still has to say which
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
      // here too (§212).
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
   * different content is an integrity error rather than an update (§240.4), so
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
   * that transactions record (§144), and a restart is not a schema change.
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
   * A Space's Schema Environment at one version (§144).
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
   * January does not still hold what January's Grants said (§188, §245).
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

  /**
   * Parses and runs one KQL query, returning the bare result array.
   *
   * Reads take no transaction: a Durable Object is single-threaded, so nothing
   * can change underneath a query that has already started.
   */
  query(command: string, params: JsonMap = {}, read: ReadOptions = {}): Json[] {
    return this.systemSession().query(command, params, read)
  }

  /** Runs one parsed KQL query. */
  find(
    query: KqlQuery,
    params: JsonMap = {},
    options: Partial<KqlContext> & ReadOptions = {},
  ): Json[] {
    return this.systemSession().find(query, params, options)
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
 * for a session that started before it (§188, §245).
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
    const parsed: Command = parseKip(command)
    if (!('Kql' in parsed)) {
      throw errors.languageMismatch('this command is not a KQL query')
    }
    return this.find(parsed.Kql, params, read)
  }

  /** Parses and runs one META command. */
  describe(command: string, params: JsonMap = {}): Json {
    const parsed: Command = parseKip(command)
    if (!('Meta' in parsed)) {
      throw errors.languageMismatch('this command is not a META command')
    }
    const space = this.nexus.space
    const authority = this.effectiveAuthority(space)
    const decisions = this.gate(authority, metaPermissions(parsed.Meta))
    const cx: MetaContext = {
      store: this.nexus.store,
      space,
      env: this.nexus.environment(space),
      request: params,
      environmentAt: (version) => this.nexus.environmentAt(space, version),
      authority,
      auth: this.auth,
    }
    // `PREVIEW KML` mints shells to allocate ids and then discards them, so it
    // runs inside a transaction like any other mutation path — one that is
    // simply never committed.
    return this.nexus.transact(() => {
      const result = executeMeta(parsed.Meta, cx)
      this.consume(decisions)
      return result
    })
  }

  /** Runs one parsed KQL query. */
  find(
    query: KqlQuery,
    params: JsonMap = {},
    options: Partial<KqlContext> & ReadOptions = {},
  ): Json[] {
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
    const result = executeKql(query, {
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

  /** Runs one KML statement, all-or-nothing. */
  mutate(
    statement: KmlStatement,
    params: JsonMap = {},
    options: MutationOptions = {},
  ): Outcome {
    const space = options.space ?? this.nexus.space
    const authority = this.effectiveAuthority(space)
    const decisions = this.gate(authority, kmlPermissions(statement))
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
   * What this Principal could do in a Space at a past instant (§176, §177).
   *
   * A historical answer, and nothing more: that a Principal could read something
   * in January says nothing about whether it can today (§179). Reading it needs
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
   * Reads the Governance audit for a Space (§89, §172).
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
   * summarizing turns a descriptive note into an executable one (§127). Lowering
   * is deliberately as easy as the permission itself: an incident response that
   * had to wait for an approval would arrive late (§132).
   *
   * Returns the ceiling the element carried before.
   */
  elevateAuthority(element: ElementId, cls: string, space = this.nexus.space): string {
    return this.nexus.transact(() =>
      elevateAuthority(this.governanceContext(space), element, cls),
    )
  }

  /**
   * Holds an element out of ordinary use, pending review (§133).
   *
   * Not a retraction: it says this Brain does not currently allow ordinary use
   * of the element, which is a statement about this Brain and not about whoever
   * wrote it (§134).
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
   * must not proceed unlogged is the caller's to enforce (§184).
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
 * The access-decision provenance a high-impact receipt carries (§178).
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
