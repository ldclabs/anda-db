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
import type { Json, JsonMap } from './json.js'
import { parseKip } from './kip/parser.js'
import type { Command, KmlStatement } from './kip/ast.js'
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
import { nowTime } from './time.js'
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
      this.store.appendSchemaEnv({
        space,
        version,
        lock: lock as unknown as JsonMap,
        created_at: nowTime(),
        tx_id: '',
      })
      const row = this.spaceRow(space)
      row.schema_environment_version = version
      this.store.putSpace(row)
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

  private artifacts(): Map<string, SchemaPackage> {
    const out = new Map<string, SchemaPackage>([
      [CORE_PACKAGE_REF, CORE_PACKAGE],
    ])
    for (const row of this.store.packages()) {
      out.set(row.package_ref, row.artifact as unknown as SchemaPackage)
    }
    return out
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

  /**
   * Parses and runs one META command.
   *
   * META is read-only by construction, with one exception the language makes
   * explicit: `PREVIEW KML` runs the real dry-run path, which writes nothing.
   */
  describe(command: string, params: JsonMap = {}): Json {
    const parsed: Command = parseKip(command)
    if (!('Meta' in parsed)) {
      throw errors.languageMismatch('this command is not a META command')
    }
    const space = this.space
    const cx: MetaContext = {
      store: this.store,
      space,
      env: this.environment(space),
      request: params,
    }
    // `PREVIEW KML` mints shells to allocate ids and then discards them, so it
    // runs inside a transaction like any other mutation path — one that is
    // simply never committed.
    return this.storage.transactionSync(() => executeMeta(parsed.Meta, cx))
  }

  /**
   * Parses and runs one KQL query, returning the bare result array.
   *
   * Reads take no transaction: a Durable Object is single-threaded, so nothing
   * can change underneath a query that has already started.
   */
  query(command: string, params: JsonMap = {}): Json[] {
    const parsed: Command = parseKip(command)
    if (!('Kql' in parsed)) {
      throw errors.languageMismatch(
        'this command is not a KQL query',
      )
    }
    return this.find(parsed.Kql, params)
  }

  /** Runs one parsed KQL query. */
  find(
    query: Parameters<typeof executeKql>[0],
    params: JsonMap = {},
    options: Partial<KqlContext> = {},
  ): Json[] {
    const space = options.space ?? this.space
    const cx: KqlContext = {
      store: this.store,
      space,
      env: this.environment(space),
      request: params,
      ...options,
    }
    return executeKql(query, cx)
  }

  /** Runs one KML statement, all-or-nothing. */
  mutate(
    statement: KmlStatement,
    params: JsonMap = {},
    options: Partial<KmlContext> = {},
  ): Outcome {
    const env = this.environment(options.space ?? this.space)
    const cx: KmlContext = {
      store: this.store,
      space: options.space ?? this.space,
      env,
      origin: options.origin ?? { principal_id: SYSTEM_PRINCIPAL },
      request: params,
      ...options,
    }
    // The transaction boundary: a clause that throws unwinds everything this
    // statement wrote, including the shells its handles were minted from.
    return this.storage.transactionSync(() => executeKml(statement, cx))
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
