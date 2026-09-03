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

import { KipError, detailed, errors, KIP_ERROR_CODES, KIP_ERROR_REGISTRY } from '../errors.js'
import {
  EffectiveAuthority,
  authority as authorityLadder,
  authorityCeiling,
  bindingId,
  familyOf,
  describePermission,
  isPermitted,
  spaceResource,
  type AuthContext,
} from '../governance/index.js'
import {
  compareElementId,
  elementId,
  formatElementId,
  parseElementId,
  tryParseElementId,
  type ElementId,
  type ElementKind,
} from '../id.js'
import type { Json, JsonMap } from '../json.js'
import type {
  AsOf,
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
import { baseline, forecast, type Policy } from '../projection/policy.js'
import { endpointFromJson, endpointLocal } from '../term.js'
import {
  conceptTypeDef,
  facetDef,
  formatSymbolRef,
  lineageOfSymbol,
  lineageText,
  predicateDef,
  structuralFieldDef,
  symbols,
  type SchemaEnvironment,
  type SymbolKind,
} from '../schema/index.js'
import {
  State,
  TABLES,
  pageCursorFromToken,
  pageToken,
  searchIndex,
  snapshotToken,
  type ChangeEntry,
  type CursorFamily,
  type PageCursor,
  type Store,
  type TransactionRow,
} from '../store/index.js'
import { normalizeTime } from '../time.js'
import {
  capabilities,
  KIP_VERSION,
  MAX_DEPENDENTS_DEPTH,
} from './capabilities.js'
import {
  describeCapsule,
  exportCapsule,
  verifyCapsule,
} from '../capsule/index.js'

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
   * Required rather than optional: `DESCRIBE ACCESS` and the Primer's
   * `execution_context` answer *about the caller*, and a context that could
   * arrive without one would have to invent a fallback — which is how "no
   * control plane" and "no authority" become the same answer.
   */
  authority: EffectiveAuthority
  /** Who the caller is. */
  auth: AuthContext
  /**
   * The Schema Environment of a past coordinate (§20.9).
   *
   * `DESCRIBE SNAPSHOT AS OF` and `DESCRIBE SCHEMA ENVIRONMENT AS OF` both
   * answer about a coordinate, and both have to answer about the schema that
   * was in force at it rather than today's.
   */
  environmentAt: (version: number) => SchemaEnvironment
  /**
   * Where a paging META command leaves its continuation token (§44.8).
   *
   * An out-parameter, because the bodies these commands answer with have no
   * slot for one: `LIST` answers with a bare array. Without it the engine
   * accepts an opaque `list` cursor it has no way to hand out, which makes
   * `LIST ... LIMIT n CURSOR ...` impossible to reach rather than merely
   * awkward. `SEARCH` carries its own inside its object body and does not use
   * this.
   */
  page?: { next_cursor?: string; truncated?: boolean }
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

  if ('Primer' in target) {
    return primer(
      cx,
      target.Primer.mode === null
        ? 'compact'
        : text(target.Primer.mode, b, 'DESCRIBE PRIMER MODE'),
    )
  }
  if ('Space' in target) {
    const name =
      target.Space.value === null ? cx.space : text(target.Space.value, b, 'SPACE')
    const row = cx.store.space(name)
    if (row === null) {
      throw errors.notFoundOrNotVisible(`no MemorySpace ${name}`)
    }
    // §68 folded the old EXECUTION CONTEXT answer into the Space and Primer
    // descriptions: a caller learns where it is and who it is in one read.
    return {
      ...row,
      id: undefined,
      execution_context: executionContext(cx),
    } as unknown as Json
  }
  if ('SchemaEnvironment' in target) {
    // §20.9: at a coordinate, the environment that was in force *then*. A
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
      // whether it is here (§20.12).
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
      if (policy.id === named) return policyJson(policy)
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
  // §67.2: an Agent must be able to learn what it may do without first being
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
      // §31.3: the influence-authority classes this caller may elevate an
      // element to — up to the ceiling its `elevate_authority` standing
      // carries, and none at all without that permission.
      elevatable_authority_classes: elevatableClasses(cx),
      granularity:
        'per element, on reads and writes alike; this report is per Space, ' +
        'because a per-element access report is an existence oracle',
      expires_at: cx.authority.earliestExpiry(),
    } as Json
  }
  if ('Snapshot' in target) return snapshot(target.Snapshot, cx, b)
  if ('Capsule' in target) {
    const source = scalarValue(target.Capsule, b)
    if (typeof source !== 'string') {
      throw errors.typeMismatch('DESCRIBE CAPSULE takes the artifact text')
    }
    return describeCapsule(source)
  }
  throw errors.unsupportedCapability(
    'DESCRIBE COMPATIBILITY needs a package compatibility model this engine ' +
      'has not built',
  )
}

/**
 * What the old `DESCRIBE EXECUTION CONTEXT` reported, now a member of the
 * Primer and of `DESCRIBE SPACE` (§64, §68): who is asking, in which Space,
 * under which policy, and until when.
 *
 * §64.2 is a MUST: the Primer distinguishes the authenticated Principal from
 * the semantic `$self`. They answer different questions — who is asking, and
 * who this Brain is — and an Agent that conflates them will sign the Brain's
 * memories with the caller's name.
 */
function executionContext(cx: MetaContext): Json {
  return {
    space_id: cx.space,
    space_seq: cx.store.currentSeq(cx.space),
    schema_environment_version: cx.env.version,
    principal: {
      id: cx.authority.principal.principal_id,
      principal_id: cx.authority.principal.principal_id,
      principal_class: cx.authority.principal.principal_class,
      status: cx.authority.principal.status,
      authenticated: cx.authority.principal.principal_class !== 'anonymous',
      authentication_strength: cx.auth.auth_strength,
      auth_strength: cx.auth.auth_strength,
      groups: cx.authority.groups,
      is_space_owner: cx.authority.isOwner,
      purpose: cx.auth.purpose,
      purpose_assurance: cx.auth.purpose_assurance,
    },
    // §28.3: the ActorBindings this Principal may speak through, so an Agent
    // knows which actors it can assert as before it tries.
    actor_bindings: cx.authority.bindings.map((binding) => ({
      id: bindingId(binding.id),
      actor: binding.actor_ref,
      binding_class: binding.binding_class,
      assurance: binding.assurance,
    })),
    delegation_chain: [...cx.auth.delegation_chain],
    // §67.2: an Agent that does not know when its Delegation expires plans
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
    note:
      'the Principal is the authenticated caller, never the semantic actor ' +
      'a claim is attributed to (§13.3)',
  } as Json
}

/**
 * The influence-authority classes this caller may elevate an element to
 * (§31.3), in ladder order up to the ceiling its standing carries.
 *
 * Empty without `elevate_authority`: `DESCRIBE ACCESS` says what could ever
 * be allowed, and a caller who cannot elevate at all has no class to reach.
 */
function elevatableClasses(cx: MetaContext): string[] {
  const decision = cx.authority.authorize('elevate_authority', spaceResource(), cx.auth)
  if (!isPermitted(decision.decision)) return []
  const ceiling = authorityLadder.rank(authorityCeiling(decision.constraints))
  return [
    authorityLadder.DESCRIPTIVE,
    authorityLadder.ADVISORY,
    authorityLadder.BEHAVIORAL,
    authorityLadder.EXECUTABLE,
  ].filter((cls) => authorityLadder.rank(cls) <= ceiling)
}

/**
 * `DESCRIBE SNAPSHOT [AS OF SEQ :s | AT TIME :t]` — a snapshot coordinate
 * (§68).
 *
 * Without an operand it describes the current head; `AS OF SEQ` a past
 * coordinate; `AT TIME` resolves an instant to the last sequence committed at
 * or before it, which is how wall-clock time enters `AS OF SEQ` (§48.1) — the
 * engine never guesses which of several sequences an instant means. A
 * sequence beyond the head is refused rather than rounded to the present,
 * and coordinate 0 — before the first commit — is an empty Space rather than
 * an error. This engine keeps every version, so no instant is before its
 * retention floor.
 *
 * The coordinate is a description: `space_seq`, the transaction that
 * committed it, its commit time and the schema environment version in force.
 * The `snapshot_token` beside it is what `read.snapshot_token` binds a later
 * read to, so a caller can make several requests answer at one coordinate.
 */
function snapshot(
  target: { as_of: AsOf | null; at_time: Scalar | null },
  cx: MetaContext,
  b: ReadBindings,
): Json {
  // The grammar takes one operand or the other, never both. A tree that
  // arrived off the text path (§73) can still carry two, and the sequence a
  // coordinate names is the more exact of the answers — so it wins, as it does
  // on the other reference engine, rather than the two disagreeing silently.
  let seq: number
  if (target.as_of === null && target.at_time !== null) {
    const at = normalizeTime(text(target.at_time, b, 'DESCRIBE SNAPSHOT AT TIME'), 'AT TIME')
    seq = cx.store.seqAtTime(cx.space, at)
  } else {
    seq =
      bindCoordinate({ as_of: target.as_of }, readContext(cx), b) ??
      cx.store.currentSeq(cx.space)
  }
  const committed = seq === 0 ? null : cx.store.transactionAtSeq(cx.space, seq)
  return {
    space_id: cx.space,
    space_seq: seq,
    tx_id: committed?.tx_id ?? null,
    committed_at: committed?.committed_at ?? null,
    schema_environment_version: cx.store.schemaVersionAt(cx.space, seq),
    snapshot_token: snapshotToken(cx.space, { seq }),
  } as Json
}

/**
 * The orientation an Agent reads first (§64).
 *
 * Ordered by what a caller has to know before it can do anything useful: where
 * it is, what the schema lets it say, what the engine can do, and the
 * invariants that will otherwise bite it.
 *
 * The key structure is the reference engine's, member for member. A Primer is
 * the one document every client parses, so two shapes for it is the divergence
 * that costs the most: `primer.schema.types` on one engine and `primer.types`
 * on the other reads as a Space with no types rather than as a wrong path.
 */
function primer(cx: MetaContext, mode: string): Json {
  if (mode !== 'compact' && mode !== 'full') {
    throw errors.invalidSyntax(
      `DESCRIBE PRIMER MODE takes "compact" or "full", got ${JSON.stringify(mode)}`,
    )
  }
  const space = cx.store.space(cx.space)
  const primer: JsonMap = {
    // §64.2 is a MUST: the Primer distinguishes the authenticated Principal
    // from the semantic `$self`. What the old EXECUTION CONTEXT reported —
    // the Principal, its bindings, the Space, the policy and the expiry —
    // lives here now (§68).
    execution_context: executionContext(cx),
    cognitive_identity: selfIdentity(space?.self_concept ?? ''),
    space: {
      id: cx.space,
      name: space?.name ?? '',
      description: space?.description ?? '',
      seq: space?.seq ?? 0,
    },
    contents: contents(cx),
    schema: {
      environment_version: cx.env.version,
      packages: cx.env.packageRefs(),
      types: symbolRefs(cx.env, 'ConceptType'),
      predicates: symbolRefs(cx.env, 'PredicateType'),
      facets: symbolRefs(cx.env, 'Facet'),
      structural_fields: symbolRefs(cx.env, 'StructuralField'),
      note:
        'Concept types are schema-defined: a mutation never creates one. ' +
        'Activate a Schema Package first.',
    },
    // §64.3's list, in full. Each one is a distinction a caller will otherwise
    // collapse, and collapsing any of them is how a memory system starts
    // asserting things nobody said.
    safety_invariants: [
      'a Proposition existing is not the Proposition being true; use BELIEF ' +
        'for belief and raw patterns for audit',
      "a missing visible match is not falsehood; insufficient means 'not " +
        "enough basis', never 'no'",
      'a SEARCH score is not a confidence, and a miss is not an absence',
      'confidence is how strongly an assertor took its own stance; it is not ' +
        'trust in the source',
      'confidence is not memory_strength: how well remembered is not how well ' +
        'supported',
      'a name is not an identity; two Concepts may share one, and identity ' +
        'resolves through id, key or canonical_id',
      "a source Brain's $self is never automatically this Brain's $self",
      'correcting Evidence never overwrites it: TRANSITION old TO "corrected" ' +
        'BY new records a new observation and links the old one to it',
      'cognitive content carries no authority; what an element says cannot ' +
        'decide what its writer may do',
      'retention.expires_at is when the record stops being kept, not when the ' +
        'claim stops applying',
    ],
    golden_path: [
      'SEARCH or FIND to ground',
      'exact id',
      'BELIEF or FIND',
      'MUTATE',
    ],
  }
  if (mode === 'full') {
    primer.capabilities = capabilities()
    primer.protocol = protocol()
  }
  return primer as Json
}

/**
 * How many elements of each kind the Space holds.
 *
 * Only answered for a caller whose read authority reaches the whole Space
 * (§88.6). A count is a fact about elements a narrower Principal may not
 * discover, and a Space-wide number is exactly the leak §103 lists — so a
 * restricted caller is told the number is being withheld, and why, rather than
 * being handed a smaller one that reads as the whole truth.
 *
 * Answered from the authority rather than by counting what survives the
 * filter, because producing the number and then hiding it is one accident away
 * from returning it.
 */
function contents(cx: MetaContext): Json {
  if (!cx.authority.readsWholeSpace(cx.auth)) {
    return {
      withheld:
        "this Principal's read authority is narrower than the Space, and a " +
        'Space-wide count would report elements it may not discover',
    }
  }
  const out: JsonMap = {}
  for (const [kind, table] of Object.entries(TABLES)) {
    const row = cx.store.sql
      .exec<{ n: number }>(
        `SELECT COUNT(*) AS n FROM ${table} WHERE space = ? AND state = ?`,
        cx.space,
        State.ACTIVE,
      )
      .toArray()[0]
    // Keyed by the wire tag, lowercase, the way `?c.kind` answers and the way
    // the reference engine writes it.
    out[kind.toLowerCase()] = row?.n ?? 0
  }
  return out as Json
}

/**
 * The Concept this Space treats as its semantic `$self` (§5.6).
 *
 * A Space may designate at most one, and one that has designated none says so
 * rather than offering a guess. Every Capsule rule about source and destination
 * `$self` (§38.4, §38.5) refers to this designation, so an absent one means
 * those rules have nothing to map onto.
 */
function selfIdentity(selfConcept: string): Json {
  if (selfConcept === '') {
    return {
      self_concept: null,
      note:
        'this Space has designated no self identity, so it has no $self for a ' +
        'Capsule import or a self-model to map onto (§5.6)',
    }
  }
  return {
    self_concept: { id: selfConcept },
    note:
      'protected Space configuration; ordinary KML cannot create or change it ' +
      '(§5.6)',
  }
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
    // The canonical identity, never the local name the caller wrote (§65):
    // a local name means nothing outside the environment that resolved it.
    ref: formatSymbolRef(resolved),
    kind,
    definition: (definition ?? null) as Json,
  } as Json
}

/**
 * Every symbol of one kind the environment resolves, as `LIST` reports them.
 *
 * A row rather than a bare reference, and the same row
 * `rs/anda_cognitive_nexus` produces — a `LIST` answer is read by clients that
 * talk to both engines, and two shapes for one command is the divergence that
 * costs the most to find: a reader written for objects gets an empty result
 * from an engine that hands back strings, and an empty result reads as an
 * empty Space rather than as a wrong shape.
 *
 * `local_name` is what a command may write and `ref` is what it resolves to;
 * `status` is why both are needed, because a deprecated package still resolves
 * a qualified reference while no longer answering a bare local name (§20.12).
 */
/**
 * One Epistemic Policy, in the shape `DESCRIBE` and `LIST` both report.
 *
 * The wire names, which are not this engine's internal ones: `accept` is a
 * threshold and says so, and `modes` is an eligibility gate rather than a
 * weighting — the two notes are carried because a reader that assumed
 * otherwise would read a projection's score as a probability. The same shape
 * `rs/anda_cognitive_nexus` writes, so a client parses one policy document
 * whichever engine answered.
 */
function policyJson(policy: Policy): Json {
  return {
    id: policy.id,
    version: policy.version,
    eligible_modes: policy.modes,
    accept_threshold: policy.accept,
    material_threshold: policy.material,
    unstated_confidence_weight: policy.unstated_confidence,
    conflict_set_expansion: policy.expand_conflicts,
    notes: [
      'mode gates eligibility and never weights a claim: a mode does not ' +
        'grant trust',
      'corroboration groups are counted once; repetition is not evidence',
    ],
  } as Json
}

function symbolList(env: SchemaEnvironment, kind: SymbolKind): Json[] {
  const out: Json[] = []
  for (const reference of env.packageRefs()) {
    const artifact = env.artifact(reference)
    if (artifact === undefined) continue
    const packageId = packageIdOf(reference)
    for (const name of symbols(artifact, kind)) {
      out.push({
        ref: `${reference}/${name}`,
        local_name: name,
        package_ref: reference,
        status: env.state(packageId),
      })
    }
  }
  return out.sort((a, b) =>
    String((a as { ref: string }).ref).localeCompare(
      String((b as { ref: string }).ref),
    ),
  )
}

/** The package id half of a `package_id@version` reference. */
function packageIdOf(packageRef: string): string {
  const at = packageRef.lastIndexOf('@')
  return at === -1 ? packageRef : packageRef.slice(0, at)
}

/** The same symbols as bare references, for the places that report names. */
function symbolRefs(env: SchemaEnvironment, kind: SymbolKind): string[] {
  return symbolList(env, kind).map((entry) => (entry as { ref: string }).ref)
}

// --- LIST -------------------------------------------------------------------

function list(command: ListCommand, cx: MetaContext, b: ReadBindings): Json {
  const page = <T>(items: T[]): Json => {
    const offset =
      command.cursor === null
        ? 0
        : readPageCursor(command.cursor, b, cx.space, 'list').offset
    const limit = command.limit === null ? null : count(command.limit, b, 'LIMIT')
    const window = items.slice(offset)
    const rows = limit === null ? window : window.slice(0, limit)
    const consumed = offset + rows.length
    // The cursor this page's continuation needs. Issued rather than left
    // implicit: a cursor is opaque (§88.4), so a caller that is never handed
    // one cannot page at all.
    if (cx.page !== undefined && consumed < items.length) {
      cx.page.next_cursor = pageToken(cx.space, {
        family: 'list',
        snapshotSeq: cx.store.currentSeq(cx.space),
        offset: consumed,
      })
    }
    return rows as Json
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
      return page([policyJson(baseline()), policyJson(forecast())])
    case 'Dependents': {
      // §63.5: the result carries `truncated: true` when traversal was cut
      // short by an element the caller may not discover — without saying
      // where, which would be the disclosure. The rows stay the bare array
      // every LIST target answers with (the shared fixtures pin that shape);
      // the flag rides beside the page cursor, and the request envelope
      // reports it on the operation result.
      const walked = dependents(command, cx, b)
      if (cx.page !== undefined && walked.truncated) cx.page.truncated = true
      return page(walked.rows)
    }
  }
}

/**
 * `LIST DEPENDENTS :id [DEPTH :n]` — bounded reverse provenance closure
 * (§63.5).
 *
 * The traversal is the one §63.5 spells out and nothing more:
 *
 * ```text
 * X ∈ Activity.inputs → that Activity → each element in Activity.outputs
 * ```
 *
 * Each output is a dependent of `X` at distance 1, and the walk repeats from
 * each dependent up to `DEPTH` (default 1).
 *
 * The Structural-Field extension §63.5 permits — traversing fields the Schema
 * Environment *documents* as derivation lineage — is deliberately not taken. A
 * Schema Package carries no machine-readable lineage marker, so honouring it
 * would mean this Core engine hard-coding the names of one Profile's fields
 * (`derived_from`, `compiled_from`, `consolidated_to`) and guessing each one's
 * direction. Guessing wrong yields an element's *sources* where it promised its
 * dependents, which is worse than not answering: §57.5 asks for a review list,
 * and a review list with the arrows reversed sends the reviewer to the wrong
 * artifacts.
 *
 * Reachability is topology, not judgment (§57.5): a listed dependent is not
 * thereby stale, wrong, or in need of change.
 *
 * @see rs/anda_cognitive_nexus/src/meta/describe.rs
 */
function dependents(
  command: ListCommand,
  cx: MetaContext,
  b: ReadBindings,
): { rows: Json[]; truncated: boolean } {
  if (command.element === null) {
    // The grammar requires the operand, so reaching here means an AST arrived
    // from somewhere that does not.
    throw errors.invalidSyntax(
      'LIST DEPENDENTS requires the element whose dependents are listed',
    )
  }
  const named = text(command.element, b, 'LIST DEPENDENTS')
  const depth =
    command.depth === null ? 1 : depthBound(scalarValue(command.depth, b))

  const root = parseElementId(named)
  const context = reader(cx)
  // §30.4: a root this caller may not discover is answered exactly as an absent
  // one is. Refusing here would turn the command into an existence oracle for
  // elements the caller cannot read.
  if (context.load(root) === null) return { rows: [], truncated: false }
  let truncated = false

  // Everything below is walked in sorted id order, and each level's frontier is
  // sorted before the next one runs. Two engines answering the same question
  // must agree on which Activity first reached a dependent that two of them
  // produced, or the `via` they report — and the paging that slices this list —
  // would depend on storage layout.
  const seen = new Set<string>([formatElementId(root)])
  let frontier = [root]
  const rows: Json[] = []

  for (let distance = 1; distance <= depth; distance += 1) {
    const next: ElementId[] = []
    for (const source of frontier) {
      for (const activity of cx.store.activitiesWithInput(cx.space, source)) {
        // An Activity the caller may not read is not a route: naming it in
        // `via` would disclose it, and walking through it would disclose that
        // it exists (§30.4). The cut is reported as `truncated`, without
        // saying where.
        const element = context.load(activity)
        if (element === null || element.kind !== 'Activity') {
          truncated = true
          continue
        }
        for (const output of element.row.outputs) {
          let id: ElementId | null = null
          try {
            id = endpointLocal(endpointFromJson(output))
          } catch {
            continue
          }
          if (id === null) continue
          const key = formatElementId(id)
          // First reach wins, so a dependent is reported at its shortest
          // distance and a DAG that converges does not report it twice.
          if (seen.has(key)) continue
          seen.add(key)
          if (context.load(id) === null) {
            truncated = true
            continue
          }
          next.push(id)
          rows.push({
            id: key,
            kind: id.kind.toLowerCase(),
            distance,
            via: { activity: formatElementId(activity) },
          })
        }
      }
    }
    if (next.length === 0) break
    frontier = next.sort(compareElementId)
  }
  return { rows, truncated }
}

/**
 * Reads the `DEPTH` bound, capped at {@link MAX_DEPENDENTS_DEPTH}.
 *
 * The two engines have to refuse the same bound with the same code, so the
 * shape is stated rather than inherited from whatever coercion was convenient:
 * anything that is not a non-negative integer is a `TypeMismatch`, and only
 * zero is a `ConstraintViolation`. A numeric string is accepted for the same
 * reason `LIMIT` accepts one — a caller binding a parameter from JSON may not
 * control its type.
 *
 * @see rs/anda_cognitive_nexus/src/meta/describe.rs — `depth_bound`
 */
function depthBound(value: Json): number {
  // `Number()` is not the parse the Rust engine runs: it reads `""` as zero,
  // `" 2 "` as two and `"0x10"` as sixteen, none of which `str::parse::<u64>`
  // accepts. A string is therefore matched as digits before it is converted.
  const asked =
    typeof value === 'number'
      ? value
      : typeof value === 'string' && /^\d+$/.test(value)
        ? Number(value)
        : Number.NaN
  if (!Number.isSafeInteger(asked) || asked < 0) {
    throw errors.typeMismatch(
      `DEPTH takes a non-negative integer, got ${JSON.stringify(value)}`,
    )
  }
  if (asked === 0) {
    throw errors.constraintViolation(
      'DEPTH 0 asks for the element itself, which is not one of its dependents',
    )
  }
  return Math.min(asked, MAX_DEPENDENTS_DEPTH)
}

// --- VALIDATE and PREVIEW ---------------------------------------------------

/**
 * Reads a `CURSOR` slot as the opaque token this engine issues.
 *
 * §88.4: a cursor is opaque or authenticated, never a number a caller can
 * invent. §102.28 adds that one family's cursor must not continue another's,
 * which is why the family is checked rather than merely encoded.
 */
function readPageCursor(
  cursor: Scalar,
  b: ReadBindings,
  space: string,
  family: CursorFamily,
): PageCursor {
  const value = scalarValue(cursor, b)
  if (typeof value !== 'string') {
    throw errors.cursorTypeMismatch(
      `a CURSOR is the opaque token this engine issued, got ` +
        `${JSON.stringify(value)}`,
    )
  }
  return pageCursorFromToken(value, space, family)
}

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
    cursor: Scalar | null
  }) => ({
    from: paging.from_seq === null ? 0 : count(paging.from_seq, b, 'FROM SEQ'),
    to:
      paging.to_seq === null
        ? Number.MAX_SAFE_INTEGER
        : count(paging.to_seq, b, 'TO SEQ'),
    limit: paging.limit === null ? 100 : count(paging.limit, b, 'LIMIT'),
    // A history cursor is a page token of its own family (§87.7): one issued
    // by a FIND or a LIST must not continue a chronology.
    offset:
      paging.cursor === null
        ? 0
        : readPageCursor(paging.cursor, b, cx.space, 'history').offset,
  })
  // The continuation a page hands back, when more of the chronology remains.
  const paged = <T>(rows: T[], offset: number, limit: number): T[] => {
    const window = rows.slice(offset, offset + limit)
    if (cx.page !== undefined && offset + window.length < rows.length) {
      cx.page.next_cursor = pageToken(cx.space, {
        family: 'history',
        snapshotSeq: cx.store.currentSeq(cx.space),
        offset: offset + window.length,
      })
    }
    return window
  }

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
    const { from, to, limit, offset } = range(command.Element)
    // The version log answers *which transitions touched this element* with an
    // index seek; the journal then supplies the transition itself. Going
    // through both is what makes an element's chronology the same grain as a
    // Space's — §68.1 calls HISTORY a transition chronology, and §36.2 makes a
    // transition one envelope, not one row per element per commit.
    // One past the page, and the extra row is the whole point: `paged` issues
    // a continuation only when it can see that something follows the window,
    // and a fetch stopping exactly at `offset + limit` never can — every page
    // would look like the last one.
    const touched = cx.store.versionsOf(cx.space, id, from, to, offset + limit + 1)
    const named = formatElementId(id)
    const envelopes: TransactionRow[] = []
    for (const version of touched) {
      const row = cx.store.transaction(version.tx_id)
      if (row !== null) envelopes.push(row)
    }
    return paged(
      visibleChanges(cx, envelopes).map((row) => changeEnvelope(row, named)),
      offset,
      limit,
    ) as unknown as Json
  }
  const { from, to, limit, offset } = range(command.Space)
  return paged(
    visibleChanges(
      cx,
      // Again one past the page, so the continuation is issued (see above).
      cx.store.transactionsInSpace(cx.space, from, to, offset + limit + 1),
    ).map((row) => changeEnvelope(row, null)),
    offset,
    limit,
  ) as unknown as Json
}

/**
 * One journal row as the Change Envelope §36.1 fixes.
 *
 * `HISTORY ELEMENT`, `HISTORY SPACE` and `CHANGES` all answer in this shape,
 * because they are the same unit — one committed transition — asked for over
 * different ranges. Emitting one grain in one and another in the next would
 * make "what happened to this element" and "what happened here" two
 * incomparable answers, and would cost a consumer the §36.3 deduplication key
 * `space_id + space_seq + tx_id`.
 *
 * `element` narrows the `changes` list to the one the caller asked about. The
 * envelope still describes the whole transition, because that is what a
 * transition is (§36.2); what is narrowed is which of its changes this
 * chronology is about.
 *
 * @see rs/anda_cognitive_nexus/src/meta/history.rs — `entry`
 * @see anda_kip::ChangeEnvelope
 */
function changeEnvelope(row: TransactionRow, element: string | null): Json {
  return {
    space_id: row.space,
    space_seq: row.seq,
    tx_id: row.tx_id,
    committed_at: row.committed_at,
    transaction_class: row.transaction_class,
    snapshot_seq: row.snapshot_seq,
    status: row.status,
    schema_environment_version: row.schema_environment_version,
    changes:
      element === null
        ? row.changes
        : row.changes.filter((change) => change.id === element),
  } as unknown as Json
}

/** A read context, for the META paths that have to resolve an element. */
function reader(cx: MetaContext): Context {
  return new Context(cx.store, cx.env, cx.space, cx.authority, cx.auth)
}

/**
 * The KQL context a META command borrows to resolve a coordinate.
 *
 * `DESCRIBE SNAPSHOT AS OF …` names a coordinate exactly as a query does, and
 * it has to resolve to the same number: two spellings of "which coordinate is
 * this" would eventually disagree about a future sequence.
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
  const limit = limitScalar === null ? 100 : count(limitScalar, b, 'LIMIT')
  if (!Number.isInteger(after) || after < 0) {
    // §87.7: a change cursor is a Space sequence the consumer durably recorded;
    // anything else is `CursorInvalid` with `family: changes`.
    throw detailed.cursorInvalid(
      'changes',
      'malformed',
      'a CHANGES cursor from this engine is a Space sequence coordinate',
    )
  }

  const journal = cx.store.transactionsInSpace(
    cx.space,
    after + 1,
    Number.MAX_SAFE_INTEGER,
    limit,
  )
  // One envelope per committed transition, never a flattened list of changes:
  // §36.2 makes the envelope the unit of atomicity, so a consumer handed the
  // changes loose cannot tell which of them happened together — and §36.3's
  // deduplication key needs the `tx_id` and `space_id` that flattening drops.
  const rows = visibleChanges(cx, journal)
  // The cursor advances to the last coordinate this page *consumed*, not to the
  // last one it could show. They differ for a restricted caller whose authority
  // hides a whole page of transactions: taking the cursor from the visible rows
  // would leave it exactly where it started, and the follower would re-read the
  // same hidden window forever instead of walking past it. A caller that saw
  // nothing because there was nothing holds its place.
  //
  // It rides the paging slot every other META command uses rather than a field
  // inside the body, so a caller reads one page the same way whatever it asked
  // for.
  const consumed = journal[journal.length - 1]?.seq
  if (cx.page !== undefined && consumed !== undefined) {
    cx.page.next_cursor = String(consumed)
  }
  return rows.map((row) => changeEnvelope(row, null)) as unknown as Json
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
 * through a search snippet (§88.5).
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
  const offset =
    command.cursor === null
      ? 0
      : readPageCursor(command.cursor, b, cx.space, 'search').offset
  // §20.14: a symbol in a search narrows to its lineage, so a hit written
  // under an earlier package version is still a hit.
  const withType =
    command.with_type === null
      ? null
      : lineageOfSymbol(
          cx.env.resolveSymbol('ConceptType', text(command.with_type, b, 'WITH TYPE'), 'read'),
        )
  const withPredicate =
    command.with_predicate === null
      ? null
      : lineageOfSymbol(
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
      // masked out of a query must not come back through a search hit (§88.5).
      const view = context.view(id)
      if (view === null) continue
      if (withType !== null && lineageText(String(view.schema_ref ?? '')) !== withType) continue
      if (
        withPredicate !== null &&
        lineageText(String(view.predicate_ref ?? '')) !== withPredicate
      ) {
        continue
      }
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
    ...(consumed < total
      ? {
          next_cursor: pageToken(cx.space, {
            family: 'search',
            snapshotSeq: spaceSeq,
            offset: consumed,
          }),
        }
      : {}),
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

/**
 * A paging count: `LIMIT`, and the `FROM SEQ` / `TO SEQ` bounds of a
 * chronology.
 *
 * Separate from {@link numberOf} because these are *counts*, and a `LIMIT "x"`
 * coerced with `Number` becomes `NaN` and then silently pages nothing — a
 * mistyped command that answers rather than refuses. §102.28 puts a scalar of
 * the wrong type on `TypeMismatch`, which is also what the KQL side of the
 * engine already does for the same clause.
 */
function count(scalar: Scalar, b: ReadBindings, what: string): number {
  const value = scalarValue(scalar, b)
  if (typeof value !== 'number' || !Number.isInteger(value) || value < 0) {
    throw errors.typeMismatch(
      `${what} must be a non-negative integer, got ${JSON.stringify(value)}`,
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
