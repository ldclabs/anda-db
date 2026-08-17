/**
 * # The per-Space Schema Environment
 *
 * The resolution set that turns a model-facing local name into one exact
 * symbol, or fails saying why. Every element persists the exact symbol it
 * resolved to, so an environment change never retroactively re-types data that
 * is already stored — it only changes what the *next* write resolves.
 */

import { errors, KipError } from '../errors.js'
import type { JsonMap } from '../json.js'
import {
  defines,
  packageRefOf,
  symbolRefOf,
  type SchemaPackage,
} from './package.js'
import {
  formatPackageRef,
  formatSymbolRef,
  isQualified,
  KIND_NAMES,
  parseSymbolRef,
  type PackageRef,
  type SymbolKind,
  type SymbolRef,
} from './symbol.js'

/** The reserved package that describes Core itself (§158). */
export const CORE_PACKAGE_ID = 'kip://core'

/** The Core package this engine implements. */
export const CORE_PACKAGE_REF = 'kip://core@2.0.0'

/** The baseline Evidence classes (Spec §15.2). */
export const EVIDENCE_CLASSES: readonly string[] = [
  'observation',
  'user_statement',
  'agent_statement',
  'tool_result',
  'measurement',
  'message',
  'document',
  'web_resource',
  'external_assertion',
  'human_feedback',
  'derived_result',
]

/** The baseline Activity classes (Spec §16.2). */
export const ACTIVITY_CLASSES: readonly string[] = [
  'extraction',
  'tool_execution',
  'human_review',
  'inference',
  'summarization',
  'semantic_consolidation',
  'procedural_consolidation',
  'skill_compilation',
  'import',
  'schema_migration',
  'entity_merge',
  'experience_formation',
  'belief_revision',
]

/** The stances an Assertion may take (§13.4). */
export const STANCES: readonly string[] = ['support', 'reject', 'uncertain']

/** How an Assertion was arrived at (§13.5). */
export const ASSERTION_MODES: readonly string[] = [
  'observed',
  'stated',
  'inferred',
  'predicted',
  'hypothetical',
  'imported',
]

const registry = (values: readonly string[], description: string) => ({
  values: [...values],
  description,
})

/**
 * The built-in `kip://core@2.0.0` artifact.
 *
 * Core defines the open registries and nothing else: element kinds are fixed
 * and not redefinable (§240.22), and Concept types are schema-defined, so Core
 * deliberately declares none. A Space with only Core installed can therefore
 * hold Propositions and Assertions but cannot type a Concept until a profile is
 * activated — which is define-before-use working as intended.
 */
export const CORE_PACKAGE: SchemaPackage = {
  format: 'KIP-Schema-Package',
  format_version: '2.0',
  manifest: {
    package_id: CORE_PACKAGE_ID,
    version: '2.0.0',
    package_ref: CORE_PACKAGE_REF,
    name: 'KIP Core',
    description: 'The Core element kinds and open registries of KIP 2.0.',
  },
  definitions: {
    registry_extensions: {
      evidence_classes: registry(
        EVIDENCE_CLASSES,
        'Baseline Evidence classes (§15.2).',
      ),
      activity_classes: registry(
        ACTIVITY_CLASSES,
        'Baseline Activity classes (§16.2).',
      ),
      stances: registry(STANCES, 'The stances an Assertion may take (§13.4).'),
      assertion_modes: registry(
        ASSERTION_MODES,
        'How an Assertion was arrived at (§13.5).',
      ),
    },
  },
}

/** A package's activation state in one Space (Spec §81). */
export type PackageState =
  /** Available locally, not active for ordinary writes (§82). */
  | 'installed'
  /**
   * Usable to inspect imported data, never a default for local cognition
   * (§83). This is where an untrusted or foreign package lands.
   */
  | 'validation_only'
  /** Permitted by Space Governance, and eligible for new writes (§84). */
  | 'active'
  /** Readable; new writes should avoid it (§85). */
  | 'deprecated'
  /**
   * Barred from new operations after a security or semantic incident; existing
   * data stays inspectable (§86).
   */
  | 'blocked'
  /** Isolated pending review. It MUST NOT affect normal resolution (§87). */
  | 'quarantined'

/** Whether a new write may bind data to this package. */
export const allowsWrite = (state: PackageState): boolean =>
  state === 'active' || state === 'deprecated'

/**
 * Whether an existing reference into this package still resolves.
 *
 * Blocked packages included: data already bound to a blocked package does not
 * stop meaning what it meant, and refusing to resolve it would make the
 * incident unauditable (§86). What a blocked package cannot do is accept new
 * writes.
 */
export const allowsRead = (state: PackageState): boolean =>
  state !== 'quarantined'

/**
 * Whether this package participates in bare local-name resolution.
 *
 * Narrower than reads on purpose: a local name is the caller not saying which
 * package they meant, so only packages the Space has actually endorsed may
 * answer.
 */
export const answersLocalNames = (state: PackageState): boolean =>
  state === 'active' || state === 'deprecated'

/** What a resolution is for. */
export type Intent = 'read' | 'write'

/** The immutable Schema Lock of one environment version (§25). */
export interface SchemaLock {
  /** The resolution set: one exact version per package id. */
  packages: Record<string, string>
  /** Each package's activation state. */
  states: Record<string, PackageState>
  /**
   * The version new writes bind to, when it differs from the read version
   * (§80).
   */
  write_defaults: Record<string, string>
  /** Model-friendly aliases: alias → canonical symbol reference (§21). */
  aliases: Record<string, string>
}

export function emptyLock(): SchemaLock {
  return { packages: {}, states: {}, write_defaults: {}, aliases: {} }
}

/** Reads a stored lock, filling in the members an older one may not carry. */
export function lockFromJson(value: JsonMap): SchemaLock {
  const lock = emptyLock()
  return { ...lock, ...(value as unknown as SchemaLock) }
}

/** A resolved Schema Environment, ready to answer symbol questions. */
export class SchemaEnvironment {
  /** The environment version. Every activation mints a new one (§143). */
  readonly version: number
  readonly lock: SchemaLock
  /** The artifacts, keyed by canonical package reference. */
  private readonly artifacts: Map<string, SchemaPackage>

  private constructor(
    version: number,
    lock: SchemaLock,
    artifacts: Map<string, SchemaPackage>,
  ) {
    this.version = version
    this.lock = lock
    this.artifacts = artifacts
  }

  /** The environment a Space starts with: Core, active, and nothing else. */
  static coreOnly(): SchemaEnvironment {
    const lock = emptyLock()
    lock.packages[CORE_PACKAGE_ID] = '2.0.0'
    lock.states[CORE_PACKAGE_ID] = 'active'
    return new SchemaEnvironment(
      0,
      lock,
      new Map([[CORE_PACKAGE_REF, CORE_PACKAGE]]),
    )
  }

  /**
   * Builds an environment from a lock and the artifacts it names.
   *
   * A lock naming a package whose artifact is absent fails here rather than at
   * the first query: an environment that resolves some of its own lock is worse
   * than one that refuses to exist, because the failure would surface as a
   * missing symbol somewhere unrelated (§182).
   */
  static resolve(
    version: number,
    lock: SchemaLock,
    available: ReadonlyMap<string, SchemaPackage>,
  ): SchemaEnvironment {
    const artifacts = new Map<string, SchemaPackage>()
    for (const [packageId, packageVersion] of Object.entries(lock.packages)) {
      const packageRef = `${packageId}@${packageVersion}`
      const artifact =
        available.get(packageRef) ??
        (packageRef === CORE_PACKAGE_REF ? CORE_PACKAGE : undefined)
      if (artifact === undefined) {
        throw errors.schemaPackageUnavailable(
          `the Schema Lock names ${packageRef} but its artifact is not ` +
            `installed in this Nexus`,
        )
      }
      artifacts.set(packageRef, artifact)
    }
    return new SchemaEnvironment(version, lock, artifacts)
  }

  /** The exact reference a package id resolves to in this environment. */
  packageRef(packageId: string, intent: Intent): string | null {
    const version =
      intent === 'write'
        ? (this.lock.write_defaults[packageId] ?? this.lock.packages[packageId])
        : this.lock.packages[packageId]
    return version === undefined ? null : `${packageId}@${version}`
  }

  /** A package's activation state, defaulting to `installed`. */
  state(packageId: string): PackageState {
    return this.lock.states[packageId] ?? 'installed'
  }

  /** An installed artifact by exact reference. */
  artifact(packageRef: string): SchemaPackage | undefined {
    return this.artifacts.get(packageRef)
  }

  /** Every package reference this environment resolves. */
  packageRefs(): string[] {
    return [...this.artifacts.keys()]
  }

  /**
   * Resolves a model-facing name to one exact symbol.
   *
   * Accepts a canonical reference, a configured alias, or a bare local name, in
   * that order.
   */
  resolveSymbol(kind: SymbolKind, name: string, intent: Intent): SymbolRef {
    if (isQualified(name)) {
      return this.checkQualified(kind, parseSymbolRef(name), intent)
    }
    const alias = this.lock.aliases[name]
    if (alias !== undefined) {
      // An alias is a resolution aid, not an identity: it resolves to an exact
      // symbol, which is then checked like any other (§21, §22).
      let symbol: SymbolRef
      try {
        symbol = parseSymbolRef(alias)
      } catch (err) {
        throw errors.schemaSymbolNotFound(
          `the alias ${JSON.stringify(name)} points at ` +
            `${JSON.stringify(alias)}, which is not a symbol: ` +
            `${KipError.from(err).message}`,
        )
      }
      return this.checkQualified(kind, symbol, intent)
    }
    return this.resolveLocal(kind, name, intent)
  }

  /** Resolves and returns the canonical text, which is what gets persisted. */
  resolveSymbolText(kind: SymbolKind, name: string, intent: Intent): string {
    return formatSymbolRef(this.resolveSymbol(kind, name, intent))
  }

  /** Resolves a bare local name across the packages that may answer. */
  private resolveLocal(
    kind: SymbolKind,
    name: string,
    intent: Intent,
  ): SymbolRef {
    const candidates: SymbolRef[] = []
    for (const packageId of Object.keys(this.lock.packages)) {
      if (!answersLocalNames(this.state(packageId))) continue
      const packageRef = this.packageRef(packageId, intent)
      if (packageRef === null) continue
      const artifact = this.artifacts.get(packageRef)
      if (artifact === undefined) continue
      if (defines(artifact, kind, name)) {
        candidates.push(symbolRefOf(artifact, name))
      }
    }

    const first = candidates[0]
    if (candidates.length === 1 && first !== undefined) return first
    if (candidates.length === 0) {
      throw errors.schemaSymbolNotFound(
        `no active Schema Package defines the ${KIND_NAMES[kind]} ` +
          `${JSON.stringify(name)} in this Space; a data mutation never ` +
          `creates a schema definition, so it must be published and activated ` +
          `first`,
      )
    }
    // Spec §184: tell the Agent how to recover, by name.
    const listed = candidates.map(formatSymbolRef)
    throw errors.schemaSymbolAmbiguous(
      `the ${KIND_NAMES[kind]} ${JSON.stringify(name)} is defined by more ` +
        `than one active package; use an exact qualified reference or a ` +
        `configured alias. Candidates: ${listed.join(', ')}`,
      listed,
    )
  }

  /** Checks that an exact symbol exists here and may be used for this intent. */
  private checkQualified(
    kind: SymbolKind,
    symbol: SymbolRef,
    intent: Intent,
  ): SymbolRef {
    const packageRef = formatPackageRef(symbol.package)
    const state = this.state(symbol.package.packageId)

    const artifact = this.artifacts.get(packageRef)
    if (artifact === undefined) {
      throw errors.schemaPackageUnavailable(
        `${packageRef} is not part of this Space's Schema Environment, so ` +
          `${formatSymbolRef(symbol)} cannot be resolved`,
      )
    }
    if (!defines(artifact, kind, symbol.name)) {
      throw errors.schemaSymbolNotFound(
        `${packageRef} defines no ${KIND_NAMES[kind]} named ` +
          `${JSON.stringify(symbol.name)}`,
      )
    }
    if (intent === 'read' && !allowsRead(state)) {
      throw errors.protectedSchemaState(
        `${packageRef} is quarantined and takes no part in schema resolution`,
      )
    }
    if (intent === 'write' && !allowsWrite(state)) {
      throw errors.protectedSchemaState(
        `${packageRef} is ${state} in this Space and cannot bind new data; ` +
          `existing data that references it still resolves`,
      )
    }
    return symbol
  }

  /** The definition behind a resolved symbol, when the caller needs it. */
  definitionPackage(symbol: SymbolRef): SchemaPackage | undefined {
    return this.artifacts.get(formatPackageRef(symbol.package))
  }

  /**
   * The values one of Core's open registries accepts here.
   *
   * Open means a package may add to it, not that anything goes: an unregistered
   * class is still refused (§69).
   */
  registry(name: string): Set<string> {
    const values = new Set<string>()
    for (const artifact of this.artifacts.values()) {
      const entry = artifact.definitions?.registry_extensions?.[name]
      if (entry !== null && typeof entry === 'object' && !Array.isArray(entry)) {
        const list = (entry as JsonMap).values
        if (Array.isArray(list)) {
          for (const value of list) {
            if (typeof value === 'string') values.add(value)
          }
        }
      }
    }
    return values
  }
}

/** The package reference an artifact declares, as text. */
export const packageRefText = (artifact: SchemaPackage): string =>
  formatPackageRef(packageRefOf(artifact) as PackageRef)
