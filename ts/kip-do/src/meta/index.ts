/**
 * # Executing META
 *
 * META is how an Agent finds its footing before it reads or writes anything:
 * what this Nexus is, which Space it is in, what the schema says, what the
 * engine can actually do.
 *
 * ## The five-layer discipline
 *
 * The syntax card states it and this module follows it, because collapsing any
 * two of these is how a caller ends up believing something the engine never
 * said:
 *
 * ```text
 * DESCRIBE / SEARCH   find        — what is here
 * VERIFY              integrity   — is this artifact what it claims to be
 * VALIDATE            legality    — would this be accepted
 * PREVIEW             effect      — what would it do
 * Receipt             fact        — what actually committed
 * ```
 *
 * A `VALIDATE` that passed is not a promise that a write will commit, and a
 * `PREVIEW` that computed an effect is not a commit. Only a receipt says
 * something happened.
 */

import { KipError, errors, KIP_ERROR_CODES, KIP_ERROR_REGISTRY } from '../errors.js'
import {
  EffectiveAuthority,
  familyOf,
  describePermission,
  type AuthContext,
} from '../governance/index.js'
import {
  elementId,
  formatElementId,
  parseElementId,
  tryParseElementId,
  type ElementId,
  type ElementKind,
} from '../id.js'
import type { Json, JsonMap } from '../json.js'
import type {
  ChangesCommand,
  DescribeTarget,
  HistoryCommand,
  ListCommand,
  MetaCommand,
  Scalar,
  SearchCommand,
  ValidateCommand,
} from '../kip/ast.js'
import { parseKip, parserVersion, specRevision } from '../kip/parser.js'
import { executeKml } from '../kml/index.js'
import { Context } from '../kql/context.js'
import { bindCoordinate, type KqlContext } from '../kql/index.js'
import { scalarValue, type ReadBindings } from '../kql/matching.js'
import { baseline, forecast } from '../projection/policy.js'
import {
  conceptTypeDef,
  facetDef,
  formatSymbolRef,
  predicateDef,
  structuralFieldDef,
  symbols,
  type SchemaEnvironment,
  type SymbolKind,
} from '../schema/index.js'
import {
  searchIndex,
  snapshotJson,
  type ChangeEntry,
  type Store,
} from '../store/index.js'
import { capabilities, KIP_VERSION } from './capabilities.js'
import { exportCapsule, verifyCapsule } from '../capsule/index.js'

/** What one META execution needs from its caller. */
export interface MetaContext {
  store: Store
  space: string
  env: SchemaEnvironment
  request?: JsonMap
  operation?: JsonMap
  /**
   * What the caller may do here, resolved once for the whole command.
   *
   * Required rather than optional: `DESCRIBE ACCESS` and `DESCRIBE EXECUTION
   * CONTEXT` answer *about the caller*, and a context that could arrive without
   * one would have to invent a fallback — which is how "no control plane" and
   * "no authority" become the same answer.
   */
  authority: EffectiveAuthority
  /** Who the caller is. */
  auth: AuthContext
  /**
   * The Schema Environment of a past coordinate (§144).
   *
   * `SNAPSHOT AS OF` and `DESCRIBE SCHEMA ENVIRONMENT AS OF` both answer about
   * a coordinate, and both have to answer about the schema that was in force at
   * it rather than today's.
   */
  environmentAt: (version: number) => SchemaEnvironment
}

/** Runs one META command. */
export function executeMeta(command: MetaCommand, cx: MetaContext): Json {
  const b: ReadBindings = {
    request: cx.request ?? {},
    operation: cx.operation ?? {},
    policy: baseline(),
  }

  if ('Describe' in command) return describe(command.Describe, cx, b)
  if ('List' in command) return list(command.List, cx, b)
  if ('Validate' in command) return validate(command.Validate, b)
  if ('Preview' in command) {
    if (!('Kml' in command.Preview)) {
      throw errors.unsupportedCapability(
        'PREVIEW IMPORT CAPSULE needs the Capsule import path, which this ' +
          'engine has not built; see DESCRIBE CAPABILITIES',
      )
    }
    return previewKml(text(command.Preview.Kml, b, 'PREVIEW KML'), cx)
  }
  if ('History' in command) return history(command.History, cx, b)
  if ('Changes' in command) return changes(command.Changes, cx, b)
  if ('ExportCapsule' in command) {
    return exportCapsule(command.ExportCapsule, cx, b)
  }
  if ('Verify' in command) {
    if (command.Verify.target !== 'Capsule') {
      throw errors.unsupportedCapability(
        `VERIFY ${command.Verify.target} is not implemented by this engine; ` +
          `reporting an unchecked artifact as valid would cancel the point of ` +
          `asking`,
      )
    }
    return verifyCapsule(scalarValue(command.Verify.value, b))
  }
  if ('Snapshot' in command) {
    // A SNAPSHOT token promises that a coordinate can be read back, so issuing
    // one is only honest once the engine can honour it. It can now.
    const seq = bindCoordinate(command.Snapshot, readContext(cx), b) ??
      cx.store.currentSeq(cx.space)
    return snapshotJson(
      cx.space,
      { seq },
      cx.store.schemaVersionAt(cx.space, seq),
    )
  }
  return search(command.Search, cx, b)
}

// --- DESCRIBE ---------------------------------------------------------------

function describe(
  target: DescribeTarget,
  cx: MetaContext,
  b: ReadBindings,
): Json {
  if (target === 'Protocol') return protocol()
  if (target === 'Capabilities') return capabilities()
  if (target === 'ExecutionContext') {
    return {
      space_id: cx.space,
      schema_environment_version: cx.env.version,
      space_seq: cx.store.currentSeq(cx.space),
      principal: {
        principal_id: cx.authority.principal.principal_id,
        principal_class: cx.authority.principal.principal_class,
        status: cx.authority.principal.status,
        groups: cx.authority.groups,
        is_space_owner: cx.authority.isOwner,
        auth_strength: cx.auth.auth_strength,
        purpose: cx.auth.purpose,
        purpose_assurance: cx.auth.purpose_assurance,
      },
      // §266: an Agent that does not know when its Delegation expires plans
      // work it will not be allowed to finish.
      authority_expires_at: cx.authority.earliestExpiry(),
      governance: {
        enforced:
          'commands, reads and writes; see DESCRIBE CAPABILITIES for what is ' +
          'covered',
        default_classification: cx.authority.defaultClassification(),
        policy:
          cx.authority.policy === null
            ? null
            : `${cx.authority.policy.policy_id}@${cx.authority.policy.version}`,
      },
    } as Json
  }
  if (target === 'ProjectionCapability') {
    return {
      policies: [baseline().id, forecast().id],
      statuses: ['accepted', 'rejected', 'contested', 'uncertain', 'insufficient'],
      score_semantics: 'normalized_support_not_probability',
      explanation: true,
      missing_stages: [
        {
          stage: 'trust_evaluation',
          reason: 'no trust model; every eligible corroboration group counts equally',
        },
        {
          stage: 'evidence_quality',
          reason:
            'a cited Evidence record is counted for its independence, never ' +
            'for how good it is',
        },
      ],
    } as Json
  }

  if ('Primer' in target) return primer(cx)
  if ('Space' in target) {
    const name =
      target.Space.value === null ? cx.space : text(target.Space.value, b, 'SPACE')
    const row = cx.store.space(name)
    if (row === null) {
      throw errors.notFoundOrNotVisible(`no MemorySpace ${name}`)
    }
    return { ...row, id: undefined } as unknown as Json
  }
  if ('SchemaEnvironment' in target) {
    // §144: at a coordinate, the environment that was in force *then*. A
    // historical answer under today's schema would describe a resolution that
    // never happened.
    const env =
      target.SchemaEnvironment.as_of === null
        ? cx.env
        : cx.environmentAt(
            cx.store.schemaVersionAt(
              cx.space,
              bindCoordinate(target.SchemaEnvironment, readContext(cx), b) ?? 0,
            ),
          )
    return {
      version: env.version,
      lock: env.lock as unknown as Json,
      packages: env.packageRefs(),
    } as Json
  }
  if ('Package' in target) {
    const reference = text(target.Package, b, 'DESCRIBE PACKAGE')
    const row = cx.store.packageByRef(reference)
    if (row === null) {
      throw errors.schemaPackageUnavailable(`${reference} is not installed here`)
    }
    return {
      package_ref: row.package_ref,
      content_digest: row.content_digest,
      declared_digest: row.declared_digest,
      installed_at: row.installed_at,
      source: row.source,
      // Whether it takes part in resolution is a different question from
      // whether it is here (§240.18).
      active: cx.env.packageRefs().includes(row.package_ref),
      artifact: row.artifact as unknown as Json,
    } as Json
  }
  for (const [key, kind] of [
    ['Type', 'ConceptType'],
    ['Predicate', 'PredicateType'],
    ['Facet', 'Facet'],
    ['StructuralField', 'StructuralField'],
  ] as const) {
    if (key in target) {
      return symbol(cx, kind, text((target as never)[key], b, `DESCRIBE ${key}`))
    }
  }
  if ('Error' in target) {
    const code = text(target.Error, b, 'DESCRIBE ERROR')
    const spec = KIP_ERROR_REGISTRY[code as never] as
      | { category: string; retry: string; hint: string }
      | undefined
    if (spec === undefined) {
      throw errors.notFoundOrNotVisible(`no registered error code ${code}`)
    }
    return {
      code,
      category: spec.category,
      retry: spec.retry,
      hint: spec.hint,
    } as Json
  }
  if ('Transaction' in target) {
    const id = text(target.Transaction, b, 'DESCRIBE TRANSACTION')
    const row = cx.store.transaction(id)
    if (row === null) {
      throw errors.transactionUnknown(`no transaction ${id} in this Nexus`)
    }
    return { ...row, id: undefined } as unknown as Json
  }
  if ('TransactionByIdempotencyKey' in target) {
    const key = text(
      target.TransactionByIdempotencyKey,
      b,
      'DESCRIBE TRANSACTION BY IDEMPOTENCY KEY',
    )
    const row = cx.store.transactionByKey(cx.space, key)
    if (row === null) {
      // A key nobody committed under is not an error the caller can fix by
      // retrying differently: it means the write never landed.
      throw errors.transactionUnknown(
        `no transaction committed under idempotency key ${JSON.stringify(key)}`,
      )
    }
    return { ...row, id: undefined } as unknown as Json
  }
  if ('EpistemicPolicy' in target) {
    const named =
      target.EpistemicPolicy.value === null
        ? baseline().id
        : text(target.EpistemicPolicy.value, b, 'DESCRIBE EPISTEMIC POLICY')
    for (const policy of [baseline(), forecast()]) {
      if (policy.id === named) return policy as unknown as Json
    }
    throw errors.projectionPolicyUnavailable(
      `no Epistemic Policy named ${JSON.stringify(named)} is available here`,
    )
  }

  // Reporting an empty answer here would read as a judgement — "nothing is
  // trusted", "you may do nothing" — which is not what an absent subsystem
  // means.
  if ('Trust' in target) {
    throw errors.unsupportedCapability(
      'this engine evaluates no source trust; an empty trust report would ' +
        'read as a judgement that nothing is trusted',
    )
  }
  // §266: an Agent must be able to learn what it may do without first being
  // permitted to do it, so this asks for no permission of its own.
  //
  // Deliberately coarse. It answers "could this ever be allowed here" rather
  // than "is this allowed on that element", because the second question's answer
  // depends on an element whose existence the caller may not be entitled to
  // learn — a per-element access report is an existence oracle (§103).
  if ('Access' in target) {
    const held = cx.authority.permissionNames(cx.auth)
    const byFamily: Record<string, Json> = {}
    for (const permission of held) {
      const family = familyOf(permission)
      const entries = (byFamily[family] ?? []) as Json[]
      entries.push({ permission, description: describePermission(permission) })
      byFamily[family] = entries
    }
    return {
      space_id: cx.space,
      principal_id: cx.authority.principal.principal_id,
      is_space_owner: cx.authority.isOwner,
      groups: cx.authority.groups,
      permissions: held,
      families: byFamily,
      granularity:
        'per element, on reads and writes alike; this report is per Space, ' +
        'because a per-element access report is an existence oracle',
      expires_at: cx.authority.earliestExpiry(),
    } as Json
  }
  if ('Capsule' in target) {
    throw errors.unsupportedCapability(
      'this engine has no Capsule reader, so it cannot describe one',
    )
  }
  throw errors.unsupportedCapability(
    'DESCRIBE COMPATIBILITY needs a package compatibility model this engine ' +
      'has not built',
  )
}

/** The orientation an Agent needs before its first command. */
function primer(cx: MetaContext): Json {
  return {
    kip: KIP_VERSION,
    space_id: cx.space,
    schema_environment_version: cx.env.version,
    packages: cx.env.packageRefs(),
    types: symbolList(cx.env, 'ConceptType'),
    predicates: symbolList(cx.env, 'PredicateType'),
    facets: symbolList(cx.env, 'Facet'),
    structural_fields: symbolList(cx.env, 'StructuralField'),
    grammar: { parser: parserVersion(), spec_revision: specRevision() },
    note:
      'Concept types are schema-defined: a mutation never creates one. ' +
      'Activate a Schema Package first.',
  } as Json
}

function protocol(): Json {
  return {
    kip: KIP_VERSION,
    implementation: { name: '@ldclabs/kip-do', runtime: 'cloudflare-durable-object' },
    grammar: { parser: parserVersion(), spec_revision: specRevision() },
    languages: ['KQL', 'KML', 'META'],
  } as Json
}

/** A resolved symbol and the definition behind it. */
function symbol(cx: MetaContext, kind: SymbolKind, name: string): Json {
  const resolved = cx.env.resolveSymbol(kind, name, 'read')
  const artifact = cx.env.definitionPackage(resolved)
  const definition =
    artifact === undefined
      ? undefined
      : kind === 'ConceptType'
        ? conceptTypeDef(artifact, resolved.name)
        : kind === 'PredicateType'
          ? predicateDef(artifact, resolved.name)
          : kind === 'Facet'
            ? facetDef(artifact, resolved.name)
            : structuralFieldDef(artifact, resolved.name)
  return {
    // The canonical identity, never the local name the caller wrote (§106):
    // a local name means nothing outside the environment that resolved it.
    ref: formatSymbolRef(resolved),
    kind,
    definition: (definition ?? null) as Json,
  } as Json
}

function symbolList(env: SchemaEnvironment, kind: SymbolKind): string[] {
  const out: string[] = []
  for (const reference of env.packageRefs()) {
    const artifact = env.artifact(reference)
    if (artifact === undefined) continue
    for (const name of symbols(artifact, kind)) out.push(`${reference}/${name}`)
  }
  return out.sort()
}

// --- LIST -------------------------------------------------------------------

function list(command: ListCommand, cx: MetaContext, b: ReadBindings): Json {
  const page = <T>(items: T[]): Json => {
    const offset =
      command.cursor === null ? 0 : Number(scalarValue(command.cursor, b))
    const limit =
      command.limit === null ? null : Number(scalarValue(command.limit, b))
    const window = items.slice(offset)
    return (limit === null ? window : window.slice(0, limit)) as Json
  }

  switch (command.target) {
    case 'Spaces':
      return page(cx.store.spaces().map((row) => row.space_id))
    case 'SchemaPackages':
      return page(
        cx.store.packages().map((row) => ({
          package_ref: row.package_ref,
          // Installed is not active, and a list that conflated them would let
          // a caller write against a package the Space does not resolve.
          state: cx.env.packageRefs().includes(row.package_ref)
            ? 'active'
            : 'installed',
        })),
      )
    case 'Types':
      return page(symbolList(cx.env, 'ConceptType'))
    case 'Predicates':
      return page(symbolList(cx.env, 'PredicateType'))
    case 'Facets':
      return page(symbolList(cx.env, 'Facet'))
    case 'StructuralFields':
      return page(symbolList(cx.env, 'StructuralField'))
    case 'EpistemicPolicies':
      return page([baseline().id, forecast().id])
  }
}

// --- VALIDATE and PREVIEW ---------------------------------------------------

/**
 * Legality, not effect and not permission.
 *
 * A `VALIDATE` that passed says the command is well-formed and its symbols
 * resolve. It does not promise a write will commit: the state it would act on
 * can change, and this engine's Governance plane does not exist to consult.
 */
function validate(command: ValidateCommand, b: ReadBindings): Json {
  const source = scalarValue(command.value, b)
  switch (command.target) {
    case 'Kql':
    case 'Kml': {
      if (typeof source !== 'string') {
        throw errors.typeMismatch('VALIDATE takes the command text')
      }
      try {
        const parsed = parseKip(source)
        const language = 'Kql' in parsed ? 'Kql' : 'Kml' in parsed ? 'Kml' : 'Meta'
        if (language !== command.target) {
          return {
            valid: false,
            // The actual parsed semantics rule, not the keyword the caller
            // used to ask.
            violations: [
              {
                code: 'LanguageMismatch',
                message: `this is a ${language} command, not ${command.target}`,
              },
            ],
          } as Json
        }
        return { valid: true, violations: [] } as Json
      } catch (err) {
        const failure = KipError.from(err)
        return {
          valid: false,
          violations: [{ code: failure.code, message: failure.message }],
        } as Json
      }
    }
    case 'Capsule':
    case 'SchemaPackage':
      return verifyCapsule(source)
    case 'ImportPlan':
      throw errors.unsupportedCapability(
        'VALIDATE IMPORT PLAN needs the Capsule import path, which this ' +
          'engine has not built',
      )
  }
}

/**
 * What a mutation *would* do.
 *
 * The real dry-run path, not a separate simulation: a preview written twice
 * drifts from the commit it is previewing, and the drift shows up as a caller
 * acting on an effect that never happens.
 */
function previewKml(source: string, cx: MetaContext): Json {
  const parsed = parseKip(source)
  if (!('Kml' in parsed)) {
    throw errors.languageMismatch('PREVIEW KML takes a KML statement')
  }
  const outcome = executeKml(parsed.Kml, {
    store: cx.store,
    space: cx.space,
    env: cx.env,
    origin: {},
    request: cx.request,
    operation: cx.operation,
    dryRun: true,
    // A preview runs the real write path, so it is authorized like one. A
    // preview that could compute an effect the caller may not cause would be a
    // way to learn what a refused write would have done.
    authority: cx.authority,
    auth: cx.auth,
  })
  return {
    status: outcome.status,
    changes: outcome.changes as unknown as Json,
    handles: outcome.handles,
    warnings: outcome.warnings,
    // Said plainly, because "no_effect" on a preview is about the preview and
    // not about what a commit would do.
    note: 'a preview never commits; only a receipt says something happened',
  } as Json
}

// --- HISTORY and CHANGES ----------------------------------------------------

function history(
  command: HistoryCommand,
  cx: MetaContext,
  b: ReadBindings,
): Json {
  const range = (paging: {
    from_seq: Scalar | null
    to_seq: Scalar | null
    limit: Scalar | null
  }) => ({
    from: paging.from_seq === null ? 0 : Number(scalarValue(paging.from_seq, b)),
    to:
      paging.to_seq === null
        ? Number.MAX_SAFE_INTEGER
        : Number(scalarValue(paging.to_seq, b)),
    limit: paging.limit === null ? 100 : Number(scalarValue(paging.limit, b)),
  })

  if ('Element' in command) {
    const id: ElementId = parseElementId(
      text(command.Element.value, b, 'HISTORY ELEMENT'),
    )
    // Through the read path's choke point, so an element this caller may not
    // read answers exactly as one that was never written does. A history that
    // resolved where a read did not would make the version log an existence
    // oracle (§103).
    if (reader(cx).load(id) === null) {
      throw errors.notFoundOrNotVisible(`no element ${formatElementId(id)}`)
    }
    const { from, to, limit } = range(command.Element)
    return cx.store
      .versionsOf(cx.space, id, from, to, limit)
      .map((row) => ({
        element: row.element,
        version: row.version,
        space_seq: row.seq,
        tx_id: row.tx_id,
        op: row.op,
      })) as unknown as Json
  }
  const { from, to, limit } = range(command.Space)
  return visibleChanges(
    cx,
    cx.store.transactionsInSpace(cx.space, from, to, limit).map((row) => ({
      tx_id: row.tx_id,
      space_seq: row.seq,
      committed_at: row.committed_at,
      status: row.status,
      changes: row.changes,
    })),
  ) as unknown as Json
}

/** A read context, for the META paths that have to resolve an element. */
function reader(cx: MetaContext): Context {
  return new Context(cx.store, cx.env, cx.space, cx.authority, cx.auth)
}

/**
 * The KQL context a META command borrows to resolve a coordinate.
 *
 * `SNAPSHOT AS OF …` names a coordinate exactly as a query does, and it has to
 * resolve to the same number: two spellings of "which coordinate is this" would
 * eventually disagree about a transaction id or a future sequence.
 */
function readContext(cx: MetaContext): KqlContext {
  return {
    store: cx.store,
    space: cx.space,
    env: cx.env,
    request: cx.request,
    operation: cx.operation,
    authority: cx.authority,
    auth: cx.auth,
    environmentAt: cx.environmentAt,
  }
}

/**
 * Narrows a change list to the elements this caller may read (§103).
 *
 * A transaction's change list names element ids, so an unfiltered history is an
 * existence channel for a Principal whose read authority is narrower than the
 * Space. Only restricted callers pay for the check: for one whose authority
 * reaches the whole Space there is nothing to filter, and the whole journal is
 * already theirs.
 *
 * A change to an element that has since been erased disappears from a restricted
 * caller's history, because there is nothing left to authorize against. That is
 * the conservative direction, and it is why the check is skipped entirely for
 * the unrestricted case rather than applied uniformly and losing history for
 * everyone.
 */
function visibleChanges<T extends { changes: readonly ChangeEntry[] }>(
  cx: MetaContext,
  rows: readonly T[],
): T[] {
  if (cx.authority.readsWholeSpace(cx.auth)) return [...rows]
  const context = reader(cx)
  return rows
    .map((row) => ({
      ...row,
      changes: row.changes.filter((change) => {
        const id = tryParseElementId(change.id)
        return id !== null && context.load(id) !== null
      }),
    }))
    // A transaction whose every change is hidden is one this caller has no
    // business knowing happened.
    .filter((row) => row.changes.length > 0)
}

function changes(
  command: ChangesCommand,
  cx: MetaContext,
  b: ReadBindings,
): Json {
  const after =
    'Since' in command
      ? Number(scalarValue(command.Since.cursor, b))
      : Number(scalarValue(command.AfterSeq.seq, b))
  const limitScalar = 'Since' in command ? command.Since.limit : command.AfterSeq.limit
  const limit = limitScalar === null ? 100 : Number(scalarValue(limitScalar, b))
  if (!Number.isInteger(after) || after < 0) {
    throw errors.changeCursorInvalid(
      'a CHANGES cursor from this engine is a Space sequence coordinate',
    )
  }

  const journal = cx.store.transactionsInSpace(
    cx.space,
    after + 1,
    Number.MAX_SAFE_INTEGER,
    limit,
  )
  const rows = visibleChanges(cx, journal)
  return {
    changes: rows.flatMap((row) =>
      row.changes.map((change) => ({ ...change, space_seq: row.seq })),
    ),
    // The cursor advances to the last coordinate this page *consumed*, not to
    // the last one it could show. They differ for a restricted caller whose
    // authority hides a whole page of transactions: taking the cursor from the
    // visible rows would leave it exactly where it started, and the follower
    // would re-read the same hidden window forever instead of walking past it.
    // A caller that saw nothing because there was nothing holds its place.
    cursor: journal[journal.length - 1]?.seq ?? after,
  } as unknown as Json
}

// --- SEARCH -----------------------------------------------------------------

/**
 * Associative grounding (§66).
 *
 * The contract is the Rust engine's, field for field, because two engines that
 * *refuse* differently are two engines an Agent has to be written against
 * twice: the same unsupported mode, the same historical refusal, the same
 * defaults, the same hit shape. What they are allowed to differ on is ranking,
 * and they do — ICU's dictionary is not jieba's — which is why the answer says
 * what its scores mean rather than inviting them to be compared.
 *
 * Every hit goes through the same read decision a `FIND` would, and carries the
 * **redacted** view: a field a Grant masked out of a query must not come back
 * through a search snippet (§105).
 */
function search(command: SearchCommand, cx: MetaContext, b: ReadBindings): Json {
  const term = text(command.term, b, 'SEARCH')

  if (command.mode !== null) {
    const mode = text(command.mode, b, 'MODE')
    if (mode !== 'keyword') {
      throw errors.searchModeUnsupported(
        `this engine has no embedding model, so ${JSON.stringify(mode)} search is ` +
          `unavailable; "keyword" is the only mode`,
      )
    }
  }
  if (command.as_of_seq !== null) {
    // The index is maintained with the current state and keeps no history of
    // itself, so answering this from today's index would be searching the
    // present under a past coordinate (§66.1).
    throw errors.historicalSearchUnavailable(
      'this engine keeps no historical index, so AS OF SEQ search is unavailable',
    )
  }

  const threshold = command.threshold === null ? 0 : numberOf(command.threshold, b, 'THRESHOLD')
  const limit =
    command.limit === null ? 10 : Math.min(numberOf(command.limit, b, 'LIMIT'), 100)
  const offset = command.cursor === null ? 0 : numberOf(command.cursor, b, 'CURSOR')
  const withType =
    command.with_type === null
      ? null
      : formatSymbolRef(
          cx.env.resolveSymbol('ConceptType', text(command.with_type, b, 'WITH TYPE'), 'read'),
        )
  const withPredicate =
    command.with_predicate === null
      ? null
      : formatSymbolRef(
          cx.env.resolveSymbol(
            'PredicateType',
            text(command.with_predicate, b, 'WITH PREDICATE'),
            'read',
          ),
        )

  let kinds: ElementKind[]
  switch (command.target) {
    case 'Concept':
      kinds = ['Concept']
      break
    case 'Proposition':
      kinds = ['Proposition']
      break
    case 'Evidence':
      kinds = ['Evidence']
      break
    case 'Cognition':
      kinds = ['Concept', 'Proposition', 'Evidence']
      break
    default:
      // An Assertion's content is a stance and a number; an Activity's is a
      // class and two timestamps. Refusing says so; answering nothing would
      // read as "no such claim exists".
      throw errors.searchIndexUnavailable(
        'Assertions and Activities carry no free text, so this engine builds no ' +
          'full-text index over them; reach them through the Proposition or Evidence ' +
          'they are about',
      )
  }

  const context = reader(cx)
  const scored: { score: number; hit: JsonMap }[] = []
  for (const kind of kinds) {
    // Over-fetch, because every filter below runs after scoring: the window has
    // to be wide enough that a page survives them.
    const window = Math.max(limit + offset, 1) * 4
    for (const row of searchIndex(cx.store.sql, {
      kind,
      space: cx.space,
      term,
      limit: window,
    })) {
      if (row.score < threshold) continue
      const id = elementId(kind, row.seq)
      // Applies the read decision and returns the **redacted** view; `null` is
      // an element this caller may not read, which is indistinguishable from
      // one that does not exist and must stay that way (§95). A field a Grant
      // masked out of a query must not come back through a search hit (§105).
      const view = context.view(id)
      if (view === null) continue
      if (withType !== null && view.schema_ref !== withType) continue
      if (withPredicate !== null && view.predicate_ref !== withPredicate) continue
      scored.push({
        score: row.score,
        hit: {
          id: formatElementId(id),
          kind: kind.toLowerCase(),
          // Named `score`, never `confidence`: copying this into an Assertion
          // would invent an epistemic commitment out of a text match (§2.10).
          score: row.score,
          element: view,
        },
      })
    }
  }
  // Scores from three FTS tables are not strictly comparable — each has its own
  // corpus statistics — and the Rust engine merges three separate BM25 indexes
  // the same way. Ordering them together is a ranking heuristic, which is
  // exactly what `score_semantics` tells the caller it is.
  scored.sort((a, b2) => b2.score - a.score)

  const total = scored.length
  const page = scored.slice(offset, offset + limit)
  const consumed = offset + page.length
  const spaceSeq = cx.store.currentSeq(cx.space)

  return {
    hits: page.map((entry) => entry.hit),
    search_context: {
      mode: 'keyword',
      score_semantics: 'bm25_relevance_not_confidence',
      // The index is written inside the same transaction as the row it
      // describes, so these are equal by construction rather than by luck
      // (§66.5, §79). A caller deciding what a miss means needs to know which.
      index_seq: spaceSeq,
      current_space_seq: spaceSeq,
      consistency: 'index is maintained synchronously with commits',
    },
    caveat:
      'a SEARCH score is not a confidence and a miss is not an absence; ground ' +
      'with SEARCH, then read with FIND or BELIEF',
    // The Rust engine carries this on the operation result; this engine's
    // envelope has no such slot, so it rides in the body — the same place
    // `CHANGES` puts its cursor.
    ...(consumed < total ? { next_cursor: String(consumed) } : {}),
  } as unknown as Json
}

// --- small helpers ----------------------------------------------------------

function numberOf(scalar: Scalar, b: ReadBindings, what: string): number {
  const value = scalarValue(scalar, b)
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw errors.typeMismatch(
      `${what} takes a number, got ${JSON.stringify(value)}`,
    )
  }
  return value
}

function text(scalar: Scalar, b: ReadBindings, what: string): string {
  const value = scalarValue(scalar, b)
  if (typeof value !== 'string') {
    throw errors.typeMismatch(
      `${what} takes a string, got ${JSON.stringify(value)}`,
    )
  }
  return value
}

export { capabilities, KIP_VERSION } from './capabilities.js'
export { KIP_ERROR_CODES }
