/**
 * The KML mutation clauses.
 *
 * Two rules shape everything here.
 *
 * **Field mutability is a property of the element kind** (§57): what may be
 * written is decided by what the element *is*, not by who is writing. An
 * Assertion's epistemic payload is immutable however privileged the caller —
 * correcting a claim records a *new* Assertion with supersession.
 *
 * **Core structural fields and Profile structural fields are routed apart.**
 * `Assertion.evidence`, `Evidence.source`, `Activity.inputs`/`outputs` are
 * defined by the protocol itself (§8.2) and land in typed columns; everything
 * else is resolved through the Schema Environment into the generic `structural`
 * map. Routing them together would be the mistake: a Profile could then declare
 * a field named `evidence` and quietly change what an Assertion cites.
 */

import { errors } from '../errors.js'
import {
  formatElementId,
  parseElementId,
  parseElementIdOfKind,
  type ElementId,
  type ElementKind,
} from '../id.js'
import { isJsonMap, jsonEquals, type Json, type JsonMap } from '../json.js'
import type {
  ConceptCreate,
  ConceptUpsert,
  ElementRef,
  EnsureProposition,
  FacetAssignment,
  MutationClause,
  ObjectMatcher,
  RecordCreate,
  StructuralEdge,
  Term,
} from '../kip/ast.js'
import {
  facetDef,
  formatSymbolRef,
  structuralFieldDef,
  validateAttributes,
  validateFacet,
  validateStructural,
  type SymbolKind,
} from '../schema/index.js'
import {
  State,
  type AssertionRow,
  type ConceptRow,
  type Element,
  type EvidenceRow,
  type PropositionRow,
} from '../store/index.js'
import {
  endpointFromJson,
  endpointKey,
  endpointToJson,
  tupleKey,
} from '../term.js'
import { normalizeTime } from '../time.js'
import type { Transaction } from '../tx.js'
import {
  assignments,
  bindings,
  handleId,
  kipValue,
  mutationValue,
  options,
  parameter,
  referenceId,
  referenceValue,
  scalar,
  scalarText,
  symbolName,
  type Bindings,
} from './value.js'

/**
 * Every Core structural field, by the kind that owns it.
 *
 * A Concept and a Proposition own none: every structural field they carry is
 * Profile-defined.
 */
const CORE_STRUCTURAL: Readonly<Record<ElementKind, readonly string[]>> = {
  Concept: [],
  Proposition: [],
  Assertion: ['evidence', 'context'],
  Evidence: ['source', 'generated_by'],
  Activity: ['inputs', 'outputs', 'associated_actors'],
}

/** How many planning passes {@link planPass} distributes clauses over. */
export const PLAN_PASSES = 3

/**
 * Which planning pass a clause belongs to.
 *
 * Clause order carries no mutation semantics (§24), so the engine is free to
 * choose one — and it needs to, because two clause families cannot see
 * everything they need in source order:
 *
 * ```text
 * 0  CREATE CONCEPT       stages typed Concepts other clauses validate against
 * 1  UPSERT / ENSURE      resolve existing identity, binding their handles late
 * 2  everything else      sees a complete handle map and every staged type
 * ```
 *
 * `ENSURE` is in pass 1 rather than pass 0 because checking a predicate's
 * declared subject type means knowing what type the subject *is* — including
 * when this same transaction just created it. It is before pass 2 for the
 * opposite reason: the `ASSERT` desugaring emits an `ENSURE` whose handle the
 * generated `CREATE ASSERTION` reads.
 */
export function planPass(clause: MutationClause): number {
  if ('CreateConcept' in clause) return 0
  if ('UpsertConcept' in clause || 'EnsureProposition' in clause) return 1
  return 2
}

/**
 * Declares the handles a clause binds, before any clause is interpreted.
 *
 * `UPSERT` and `ENSURE` are absent on purpose: their target may already exist,
 * so minting a shell for them would allocate an id that is thrown away on every
 * resolve-to-existing.
 */
export function declareHandles(tx: Transaction, clause: MutationClause): void {
  if ('CreateConcept' in clause) tx.declare(clause.CreateConcept.handle, 'Concept')
  else if ('CreateEvidence' in clause) tx.declare(clause.CreateEvidence.handle, 'Evidence')
  else if ('CreateAssertion' in clause) tx.declare(clause.CreateAssertion.handle, 'Assertion')
  else if ('CreateActivity' in clause) tx.declare(clause.CreateActivity.handle, 'Activity')
}

/** Interprets one clause against a plan with every handle already bound. */
export function apply(
  tx: Transaction,
  clause: MutationClause,
  request: JsonMap | undefined,
  operation: JsonMap | undefined,
): void {
  const b = bindings(tx, request, operation)

  if ('CreateConcept' in clause) return createConcept(tx, b, clause.CreateConcept)
  if ('UpsertConcept' in clause) return upsertConcept(tx, b, clause.UpsertConcept)
  if ('EnsureProposition' in clause) {
    ensureProposition(tx, b, clause.EnsureProposition)
    return
  }
  if ('CreateEvidence' in clause) return createRecord(tx, b, clause.CreateEvidence, 'Evidence')
  if ('CreateAssertion' in clause) return createRecord(tx, b, clause.CreateAssertion, 'Assertion')
  if ('CreateActivity' in clause) return createRecord(tx, b, clause.CreateActivity, 'Activity')
  if ('RetractAssertion' in clause) {
    const { target, where_clauses, limit, expect_state } = clause.RetractAssertion
    const id = directTarget(b, target, where_clauses, limit, 'RETRACT')
    if (expect_state !== null) tx.expectAssertionStatus(id, scalarText(b, expect_state, 'EXPECT STATE'))
    return retract(tx, id)
  }
  if ('SupersedeAssertion' in clause) {
    const { target, by, expect_state } = clause.SupersedeAssertion
    const id = refTarget(b, target, 'SUPERSEDE')
    if (expect_state !== null) tx.expectAssertionStatus(id, scalarText(b, expect_state, 'EXPECT STATE'))
    return supersede(tx, id, refTarget(b, by, 'SUPERSEDE ... BY'))
  }
  if ('CorrectEvidence' in clause) {
    const { target, by, expect_state } = clause.CorrectEvidence
    const id = refTarget(b, target, 'CORRECT')
    if (expect_state !== null) tx.expectState(id, scalarText(b, expect_state, 'EXPECT STATE'))
    return correct(tx, id, refTarget(b, by, 'CORRECT ... BY'))
  }
  if ('TransitionActivity' in clause) {
    const { target, to, expect_state } = clause.TransitionActivity
    const id = refTarget(b, target, 'TRANSITION')
    if (expect_state !== null) tx.expectState(id, scalarText(b, expect_state, 'EXPECT STATE'))
    return transition(tx, id, scalarText(b, to, 'TRANSITION ... TO'))
  }
  if ('Archive' in clause) {
    const { target, where_clauses, limit, expect_state } = clause.Archive
    const id = directTarget(b, target, where_clauses, limit, 'ARCHIVE')
    if (expect_state !== null) tx.expectState(id, scalarText(b, expect_state, 'EXPECT STATE'))
    return changeState(tx, id, State.ARCHIVED, 'archive')
  }
  if ('Tombstone' in clause) {
    const { target, where_clauses, limit, expect_state } = clause.Tombstone
    const id = directTarget(b, target, where_clauses, limit, 'TOMBSTONE')
    if (expect_state !== null) tx.expectState(id, scalarText(b, expect_state, 'EXPECT STATE'))
    return changeState(tx, id, State.TOMBSTONED, 'tombstone')
  }

  // Everything below is a clause this stage has not built yet. Refusing by name
  // beats accepting and doing nothing: a mutation that reports success and
  // writes nothing is the defect this project keeps finding.
  const name = Object.keys(clause)[0] ?? 'this clause'
  throw errors.unsupportedCapability(
    `${name} is not implemented by this engine yet; see DESCRIBE CAPABILITIES`,
  )
}

// --- creation ---------------------------------------------------------------

function createConcept(tx: Transaction, b: Bindings, clause: ConceptCreate): void {
  const id = requireHandle(tx, clause.handle)
  if (clause.type === null) {
    throw errors.schemaSymbolNotFound(
      'CREATE CONCEPT needs a TYPE: a Concept\'s type is schema-defined, and ' +
        'this engine will not invent one',
    )
  }
  const typeName = symbolName(b, clause.type)
  const symbol = tx.env.resolveSymbol('ConceptType', typeName, 'write')

  const fields = new Fields(
    clause.set_fields === null ? {} : assignments(b, clause.set_fields),
  )
  const attributes =
    clause.set_attributes === null ? {} : assignments(b, clause.set_attributes)
  const facets = resolveFacets(tx, b, clause.set_facets)
  const structural = collectStructural(tx, b, clause.set_structural, [])

  const key = fields.text('key')
  const canonicalId = fields.text('canonical_id')
  const aliases = fields
    .array('aliases')
    .filter((value): value is string => typeof value === 'string')
  const retention = fields.json('retention')
  const extraName = fields.text('name')
  fields.rest('Concept')

  const definition = tx.env.definitionPackage(symbol)
  validateAttributes(
    formatSymbolRef(symbol),
    definition?.definitions?.concept_types?.[symbol.name]?.attributes,
    attributes,
  ).throwIfInvalid()

  const row: ConceptRow = {
    ...blank(id),
    client_key: clause.client_key === null ? '' : scalarText(b, clause.client_key, 'CLIENT KEY'),
    schema_ref: formatSymbolRef(symbol),
    key,
    name: clause.name === null ? extraName : scalarText(b, clause.name, 'NAME'),
    canonical_id: canonicalId,
    aliases,
    attributes,
    merged_into: '',
    facets,
    structural: structural.profile,
    retention,
    expires_at: expiresAt(retention),
  }
  tx.stageNew(id, { kind: 'Concept', row })
}

function createRecord(
  tx: Transaction,
  b: Bindings,
  clause: RecordCreate,
  kind: 'Evidence' | 'Assertion' | 'Activity',
): void {
  const id = requireHandle(tx, clause.handle)
  const clientKey =
    clause.client_key === null ? '' : scalarText(b, clause.client_key, 'CLIENT KEY')
  const fields = new Fields(
    clause.set_fields === null ? {} : assignments(b, clause.set_fields),
  )
  const facets = resolveFacets(tx, b, clause.set_facets)
  const structural = collectStructural(tx, b, clause.set_structural, CORE_STRUCTURAL[kind])
  const retention = fields.json('retention')
  const envelope = { ...blank(id), facets, structural: structural.profile, retention, expires_at: expiresAt(retention) }

  let element: Element
  switch (kind) {
    case 'Evidence': {
      const [payloadMode, payloadInline, contentRef] = splitPayload(fields.json('payload'))
      const sources = structural.values('source')
      const row: EvidenceRow = {
        ...envelope,
        client_key: clientKey,
        evidence_class: fields.required('evidence_class', 'CREATE EVIDENCE'),
        payload_mode: payloadMode,
        payload_inline: payloadInline,
        content_ref: contentRef,
        content_digest: fields.text('content_digest'),
        media_type: fields.text('media_type'),
        observed_at: fields.timestamp('observed_at'),
        source_refs: sources,
        generated_by: referenceId(structural.one('generated_by') ?? null),
        status: 'active',
        corrects: [],
        corrected_by: [],
      }
      element = { kind, row }
      break
    }
    case 'Assertion': {
      const proposition = fields.reference('proposition', 'CREATE ASSERTION')
      const assertedBy = fields.json('asserted_by')
      // Each citation keeps the role it was cited in: Core records that this
      // Assertion cites E *as supporting*, and never that E proves anything —
      // that judgement belongs to the Projection (§8.4).
      const evidence = structural.take('evidence').map(([value, opts]) => {
        const citation: JsonMap = { evidence_id: referenceId(value) }
        if (typeof opts.role === 'string') citation.role = opts.role
        return citation as unknown as { evidence_id: string; role?: string }
      })
      const validTime = fields.json('valid_time')
      const confidence = fields.confidence()
      const row: AssertionRow = {
        ...envelope,
        client_key: clientKey,
        proposition_id: formatElementId(
          parseElementIdOfKind(proposition, 'Proposition'),
        ),
        asserted_by: assertedBy,
        // An Assertion with no actor is a claim nobody made; the column stays
        // empty rather than being keyed as a malformed endpoint.
        asserted_by_key:
          Object.keys(assertedBy).length === 0
            ? ''
            : endpointKey(endpointFromJson(assertedBy)),
        stance: fields.required('stance', 'CREATE ASSERTION'),
        mode: fields.required('mode', 'CREATE ASSERTION'),
        confidence,
        asserted_at: fields.timestamp('asserted_at'),
        valid_from: validTimePart(validTime, 'from'),
        valid_until: validTimePart(validTime, 'until'),
        evidence_refs: evidence,
        context_refs: structural.values('context'),
        status: 'active',
        supersedes: [],
        superseded_by: [],
        retracted_at: '',
      }
      element = { kind, row }
      break
    }
    case 'Activity': {
      const status = fields.text('status')
      element = {
        kind,
        row: {
          ...envelope,
          client_key: clientKey,
          activity_class: fields.required('activity_class', 'CREATE ACTIVITY'),
          started_at: fields.timestamp('started_at'),
          ended_at: fields.timestamp('ended_at'),
          inputs: structural.values('inputs'),
          outputs: structural.values('outputs'),
          associated_actors: structural.values('associated_actors'),
          parameters_digest: fields.text('parameters_digest'),
          status: status === '' ? 'pending' : status,
        },
      }
      break
    }
  }
  fields.rest(kind)
  tx.stageNew(id, element)
}

/**
 * `UPSERT CONCEPT` — resolve a stable identity, or create it.
 *
 * The MATCH must name a stable identity (`id` or `key`), never a name: a name
 * is mutable grounding state that several Concepts may share, so upserting on
 * one would silently pick a winner (§5.2, §5.3).
 */
function upsertConcept(tx: Transaction, b: Bindings, clause: ConceptUpsert): void {
  const matcher = clause.match
  if (matcher === null) {
    throw errors.identitySelectorRequired(
      'UPSERT CONCEPT needs a MATCH on a stable identity: {id: …} or {key: …}',
    )
  }
  const existing = resolveIdentity(tx, b, matcher)
  if (existing === null) {
    throw errors.unsupportedCapability(
      'UPSERT CONCEPT that has to create its target is not implemented by ' +
        'this engine yet; see DESCRIBE CAPABILITIES',
    )
  }
  tx.bindExisting(clause.handle, existing)
  if (clause.expect_version !== null) {
    tx.expectVersion(existing, numberOf(b, clause.expect_version, 'EXPECT VERSION'))
  }

  const element = tx.load(existing)
  if (element.kind !== 'Concept') {
    throw errors.structuralReferenceInvalid(
      `${formatElementId(existing)} is a ${element.kind}, not a Concept`,
    )
  }
  const before = JSON.stringify(element.row)

  if (clause.set_attributes !== null) {
    Object.assign(element.row.attributes, assignments(b, clause.set_attributes))
  }
  if (clause.unset_attributes !== null) {
    for (const name of clause.unset_attributes) delete element.row.attributes[name]
  }
  if (clause.set_fields !== null) {
    applyConceptFields(element.row, new Fields(assignments(b, clause.set_fields)))
  }
  for (const [symbolText, values] of Object.entries(resolveFacets(tx, b, clause.set_facets))) {
    element.row.facets[symbolText] = {
      ...(element.row.facets[symbolText] as JsonMap | undefined),
      ...(values as JsonMap),
    }
  }
  for (const unset of clause.unset_facets) {
    const symbolText = formatSymbolRef(
      tx.env.resolveSymbol('Facet', symbolName(b, unset.facet), 'write'),
    )
    const facet = element.row.facets[symbolText]
    if (isJsonMap(facet)) {
      for (const field of unset.fields) delete facet[field]
    }
  }
  if (clause.set_structural !== null) {
    const edges = collectStructural(tx, b, clause.set_structural, [])
    for (const [field, values] of Object.entries(edges.profile)) {
      const current = element.row.structural[field]
      element.row.structural[field] = [
        ...(Array.isArray(current) ? current : []),
        ...(values as Json[]),
      ]
    }
  }
  if (clause.unset_structural !== null) {
    for (const removal of clause.unset_structural) {
      const field = formatSymbolRef(
        tx.env.resolveSymbol('StructuralField', symbolName(b, removal.field), 'write'),
      )
      const target = referenceValue(mutationValue(b, removal.value), field)
      const current = element.row.structural[field]
      if (Array.isArray(current)) {
        element.row.structural[field] = current.filter(
          (value) => !jsonEquals(value, target),
        )
      }
    }
  }

  // A clause that computes the state an element is already in changes nothing:
  // no version bump, no change record, and a receipt that says `no_effect`
  // rather than claiming a transition that did not happen (§44).
  if (JSON.stringify(element.row) !== before) tx.markChanged(existing, 'update')
}

/**
 * `ENSURE PROPOSITION` — resolve the tuple, or create it.
 *
 * One Space keeps one canonical Proposition per semantic tuple (§93.6), so this
 * is a lookup by `tuple_key` and not a create-then-deduplicate.
 */
function ensureProposition(
  tx: Transaction,
  b: Bindings,
  clause: EnsureProposition,
): ElementId {
  const subject = endpointFromJson(termValue(b, clause.subject, 'subject'))
  const object = endpointFromJson(termValue(b, clause.object, 'object'))
  const predicate = tx.env.resolveSymbol(
    'PredicateType',
    predicateName(b, clause.predicate),
    'write',
  )
  const predicateRef = formatSymbolRef(predicate)
  const key = tupleKey(tx.cx.space, subject, predicateRef, object)

  const found = tx.store.propositionByTuple(key)
  const id =
    found === null
      ? tx.mint('Proposition')
      : { kind: 'Proposition' as const, seq: found.id }

  if (found === null) {
    const row: PropositionRow = {
      ...blank(id),
      subject: endpointToJson(subject),
      subject_key: endpointKey(subject),
      predicate_ref: predicateRef,
      object: endpointToJson(object),
      object_key: endpointKey(object),
      tuple_key: key,
      attributes: {},
    }
    tx.stageNew(id, { kind: 'Proposition', row })
  } else {
    tx.load(id)
    if (clause.expect_version !== null) {
      tx.expectVersion(id, numberOf(b, clause.expect_version, 'EXPECT VERSION'))
    }
  }
  if (clause.handle !== null) tx.bindExisting(clause.handle, id)
  return id
}

// --- lifecycle --------------------------------------------------------------

/**
 * `RETRACT` — the source withdraws its claim (§68).
 *
 * The record stays exactly where it is: retraction is an epistemic status, not
 * a deletion, and the Assertion remains readable and citable. Its engine
 * `state` does not move.
 */
function retract(tx: Transaction, id: ElementId): void {
  const element = requireKind(tx, id, 'Assertion')
  if (element.row.status === 'retracted') return
  element.row.status = 'retracted'
  element.row.retracted_at = tx.cx.at
  tx.markChanged(id, 'retract')
}

/** `SUPERSEDE ... BY` — a later Assertion replaces an earlier one (§15.1). */
function supersede(tx: Transaction, id: ElementId, by: ElementId): void {
  if (formatElementId(id) === formatElementId(by)) {
    throw errors.supersessionMismatch(
      `${formatElementId(id)} cannot supersede itself`,
    )
  }
  const older = requireKind(tx, id, 'Assertion')
  const newer = requireKind(tx, by, 'Assertion')
  if (older.row.proposition_id !== newer.row.proposition_id) {
    // Supersession is a claim about the same Proposition; across two of them it
    // would silently retire a claim nobody revised.
    throw errors.supersessionMismatch(
      `${formatElementId(by)} is about ${newer.row.proposition_id} and ` +
        `${formatElementId(id)} about ${older.row.proposition_id}`,
    )
  }
  const olderId = formatElementId(id)
  const newerId = formatElementId(by)
  if (!older.row.superseded_by.includes(newerId)) {
    older.row.superseded_by.push(newerId)
    older.row.status = 'superseded'
    tx.markChanged(id, 'supersede')
  }
  if (!newer.row.supersedes.includes(olderId)) {
    newer.row.supersedes.push(olderId)
    tx.markChanged(by, 'supersede')
  }
}

/** `CORRECT ... BY` — a later observation corrects an earlier one (§20). */
function correct(tx: Transaction, id: ElementId, by: ElementId): void {
  if (formatElementId(id) === formatElementId(by)) {
    throw errors.evidenceCorrectionConflict(
      `${formatElementId(id)} cannot correct itself`,
    )
  }
  const older = requireKind(tx, id, 'Evidence')
  const newer = requireKind(tx, by, 'Evidence')
  const olderId = formatElementId(id)
  const newerId = formatElementId(by)
  if (!older.row.corrected_by.includes(newerId)) {
    older.row.corrected_by.push(newerId)
    older.row.status = 'corrected'
    tx.markChanged(id, 'correct')
  }
  if (!newer.row.corrects.includes(olderId)) {
    newer.row.corrects.push(olderId)
    tx.markChanged(by, 'correct')
  }
}

/** `TRANSITION ... TO` — an Activity's lifecycle (§55). */
const ACTIVITY_TERMINAL = new Set(['completed', 'failed', 'aborted'])

function transition(tx: Transaction, id: ElementId, to: string): void {
  const element = requireKind(tx, id, 'Activity')
  if (ACTIVITY_TERMINAL.has(element.row.status)) {
    // Terminal topology freezes with the Activity (§22.3): re-opening a
    // finished process would let its provenance be rewritten after the fact.
    throw errors.activityTerminal(
      `${formatElementId(id)} is ${element.row.status} and cannot transition ` +
        `to ${JSON.stringify(to)}`,
    )
  }
  if (element.row.status === to) return
  element.row.status = to
  if (ACTIVITY_TERMINAL.has(to) && element.row.ended_at === '') {
    element.row.ended_at = tx.cx.at
  }
  tx.markChanged(id, 'transition')
}

/** `ARCHIVE` / `TOMBSTONE` — engine state, never an epistemic claim (§80). */
function changeState(
  tx: Transaction,
  id: ElementId,
  state: string,
  op: 'archive' | 'tombstone',
): void {
  const element = tx.load(id)
  if (element.row.state === state) return
  element.row.state = state
  tx.markChanged(id, op)
}

// --- targets ----------------------------------------------------------------

/** Resolves an `ElementRef` — a handle, a parameter or a literal id. */
function refTarget(
  b: Bindings,
  target: ElementRef,
  what: string,
): ElementId {
  if ('Handle' in target) return parseElementId(handleId(b, target.Handle))
  if ('Id' in target) return parseElementId(target.Id)
  const value = parameter(b, target.Param)
  if (typeof value !== 'string') {
    throw errors.typeMismatch(
      `${what} needs an element id, got ${JSON.stringify(value)}`,
    )
  }
  return parseElementId(value)
}

/**
 * Resolves the target of a clause that may instead carry a selection block.
 *
 * A selection block runs a KQL pattern, which this stage does not have. It is
 * refused by name rather than ignored: a sweep that silently acts on nothing
 * and reports success is worse than one that says it cannot run.
 */
function directTarget(
  b: Bindings,
  target: ElementRef,
  where: unknown,
  limit: unknown,
  what: string,
): ElementId {
  if (where !== null || limit !== null) {
    throw errors.unsupportedCapability(
      `${what} with a WHERE selection block is not implemented by this ` +
        `engine yet; see DESCRIBE CAPABILITIES`,
    )
  }
  return refTarget(b, target, what)
}

function requireHandle(tx: Transaction, name: string): ElementId {
  const id = tx.handle(name)
  if (id === null) {
    throw errors.internalError(`?${name} was never declared`)
  }
  return id
}

function requireKind<K extends ElementKind>(
  tx: Transaction,
  id: ElementId,
  kind: K,
): Extract<Element, { kind: K }> {
  const element = tx.load(id)
  if (element.kind !== kind) {
    throw errors.structuralReferenceInvalid(
      `${formatElementId(id)} is a ${element.kind} where a ${kind} was required`,
    )
  }
  return element as Extract<Element, { kind: K }>
}

/**
 * Resolves an identity matcher to the element it names.
 *
 * Only `id` and `key` are stable identities. A `name` is mutable grounding
 * state that duplicates are allowed to share, so matching on it would let an
 * upsert pick a winner among several equally valid Concepts (§5.2).
 */
function resolveIdentity(
  tx: Transaction,
  b: Bindings,
  matcher: ObjectMatcher,
): ElementId | null {
  const read = (field: string): string | null => {
    const value = matcher[field]
    if (value === undefined) return null
    if ('Literal' in value) {
      const literal = kipValue(value.Literal)
      if (typeof literal !== 'string') return null
      return literal
    }
    if ('Param' in value) {
      const resolved = parameter(b, value.Param)
      return typeof resolved === 'string' ? resolved : null
    }
    throw errors.identitySelectorRequired(
      'a MATCH identity must be a literal or a parameter, never a variable',
    )
  }

  const id = read('id')
  if (id !== null) {
    const parsed = parseElementId(id)
    return tx.store.load(parsed) === null ? null : parsed
  }
  const key = read('key')
  if (key !== null) {
    const found = tx.store.conceptByKey(tx.cx.space, key)
    return found === null ? null : { kind: 'Concept', seq: found.id }
  }
  if (Object.hasOwn(matcher, 'name')) {
    throw errors.nameIdentityForbidden(
      'a Concept name is mutable grounding state and several Concepts may ' +
        'share one, so it cannot identify an upsert target; use {key: …} or ' +
        '{id: …}',
    )
  }
  throw errors.identitySelectorRequired(
    'MATCH must name a stable identity: {id: …} or {key: …}',
  )
}

// --- field routing ----------------------------------------------------------

/**
 * Splits a `SET FIELDS` map into the columns one element kind accepts.
 *
 * Anything left over is reported rather than dropped: silently discarding a
 * field would mean a caller's write appeared to succeed while the value went
 * nowhere.
 */
class Fields {
  private readonly map: JsonMap

  constructor(map: JsonMap) {
    this.map = map
    for (const name of PROTECTED_FIELDS) {
      if (Object.hasOwn(map, name)) {
        throw errors.protectedSystemField(
          `\`${name}\` is engine state and cognitive content may never write ` +
            `it; it records what the runtime observed, not what a command claims`,
        )
      }
    }
  }

  private take(name: string): Json | undefined {
    if (!Object.hasOwn(this.map, name)) return undefined
    const value = this.map[name]
    delete this.map[name]
    return value as Json
  }

  text(name: string): string {
    const value = this.take(name)
    if (value === undefined || value === null) return ''
    if (typeof value !== 'string') {
      throw errors.typeMismatch(
        `\`${name}\` must be a string, got ${JSON.stringify(value)}`,
      )
    }
    return value
  }

  required(name: string, what: string): string {
    const value = this.text(name)
    if (value === '') {
      throw errors.schemaFieldNotFound(`${what} needs \`${name}\``)
    }
    return value
  }

  timestamp(name: string): string {
    const value = this.take(name)
    if (value === undefined || value === null) return ''
    if (typeof value !== 'string') {
      throw errors.typeMismatch(
        `\`${name}\` must be an RFC 3339 timestamp string, got ` +
          `${JSON.stringify(value)}`,
      )
    }
    return normalizeTime(value, name)
  }

  json(name: string): JsonMap {
    const value = this.take(name)
    return isJsonMap(value) ? value : {}
  }

  array(name: string): Json[] {
    const value = this.take(name)
    if (value === undefined || value === null) return []
    return Array.isArray(value) ? value : [value]
  }

  reference(name: string, what: string): string {
    const value = this.take(name)
    if (value === undefined || value === null) {
      throw errors.schemaFieldNotFound(`${what} needs \`${name}\``)
    }
    const id = referenceId(referenceValue(value, name))
    if (id === '') {
      throw errors.structuralReferenceInvalid(
        `\`${name}\` must reference an element by id`,
      )
    }
    return id
  }

  /** Epistemic support in `[0, 1]`, or `-1` when the actor stated none. */
  confidence(): number {
    const value = this.take('confidence')
    if (value === undefined || value === null) return -1
    if (typeof value !== 'number' || value < 0 || value > 1) {
      throw errors.typeMismatch(
        '`confidence` is epistemic support in [0, 1]; it is not trust and not ' +
          'memory strength',
      )
    }
    return value
  }

  /** Reports any field the element kind does not accept. */
  rest(kind: string): void {
    const names = Object.keys(this.map)
    if (names.length === 0) return
    throw errors.schemaFieldNotFound(
      `a ${kind} has no field(s) named: ${names.join(', ')}`,
    )
  }
}

/**
 * The fields no cognitive content may write (§26, §43).
 *
 * `_system` and `governance` record what the runtime and the control plane
 * observed. Content that could set them would be laundering provenance and
 * granting itself authority — which is precisely the prompt-injection path a
 * Governance plane exists to close.
 */
const PROTECTED_FIELDS = ['_system', 'governance', 'space_id', 'space_seq']

/** The subset of Concept fields an `UPSERT` may rewrite. */
function applyConceptFields(row: ConceptRow, fields: Fields): void {
  const name = fields.text('name')
  if (name !== '') row.name = name
  const canonical = fields.text('canonical_id')
  if (canonical !== '') row.canonical_id = canonical
  const aliases = fields.array('aliases')
  if (aliases.length > 0) {
    row.aliases = aliases.filter((v): v is string => typeof v === 'string')
  }
  const retention = fields.json('retention')
  if (Object.keys(retention).length > 0) {
    row.retention = retention
    row.expires_at = expiresAt(retention)
  }
  if (Object.hasOwn(fields as never, 'key')) {
    // The logical key is the immutable Space-local identity (§5.3): rewriting
    // it would move the element to a different identity while keeping its
    // history, which is what a merge is for.
    throw errors.immutableField('a Concept `key` is immutable once set')
  }
  fields.rest('Concept')
}

// --- facets and structural fields -------------------------------------------

/** Resolves each Facet symbol to its exact reference and validates its members. */
function resolveFacets(
  tx: Transaction,
  b: Bindings,
  list: readonly FacetAssignment[],
): JsonMap {
  const out: JsonMap = {}
  for (const entry of list) {
    const symbol = tx.env.resolveSymbol('Facet', symbolName(b, entry.facet), 'write')
    const text = formatSymbolRef(symbol)
    const values = assignments(b, entry.values)
    const definition = tx.env.definitionPackage(symbol)
    const def = definition === undefined ? undefined : facetDef(definition, symbol.name)
    if (def !== undefined) validateFacet(text, def, values).throwIfInvalid()
    out[text] = { ...(out[text] as JsonMap | undefined), ...values }
  }
  return out
}

/** The structural edges of one clause, split by who owns the field. */
class Structural {
  readonly core = new Map<string, [Json, JsonMap][]>()
  readonly profile: JsonMap = {}

  take(field: string): [Json, JsonMap][] {
    const found = this.core.get(field) ?? []
    this.core.delete(field)
    return found
  }

  values(field: string): Json[] {
    return this.take(field).map(([value]) => value)
  }

  one(field: string): Json | null {
    return this.values(field)[0] ?? null
  }
}

function collectStructural(
  tx: Transaction,
  b: Bindings,
  edges: readonly StructuralEdge[] | null,
  coreFields: readonly string[],
): Structural {
  const out = new Structural()
  if (edges === null) return out

  for (const edge of edges) {
    const name = symbolName(b, edge.field)
    const value = referenceValue(mutationValue(b, edge.value), name)
    if (coreFields.includes(name)) {
      const list = out.core.get(name) ?? []
      list.push([value, options(b, edge.options)])
      out.core.set(name, list)
      continue
    }
    const symbol = tx.env.resolveSymbol('StructuralField', name, 'write')
    const text = formatSymbolRef(symbol)
    const current = out.profile[text]
    out.profile[text] = [...(Array.isArray(current) ? current : []), value]
  }

  for (const [text, values] of Object.entries(out.profile)) {
    const symbol = tx.env.resolveSymbol('StructuralField', text, 'write')
    const definition = tx.env.definitionPackage(symbol)
    const def =
      definition === undefined ? undefined : structuralFieldDef(definition, symbol.name)
    if (def !== undefined && Array.isArray(values)) {
      validateStructural(
        text,
        def,
        values.map((value) => endpointKey(endpointFromJson(value))),
      ).throwIfInvalid()
    }
  }

  const leftover = [...out.core.keys()].filter((f) => !coreFields.includes(f))
  if (leftover.length > 0) {
    throw errors.schemaFieldNotFound(
      `unknown structural field(s): ${leftover.join(', ')}`,
    )
  }
  return out
}

// --- small helpers ----------------------------------------------------------

/** The envelope a newly staged element starts from; commit fills the rest. */
function blank(id: ElementId) {
  return {
    id: id.seq,
    space: '',
    state: State.ACTIVE,
    version: 0,
    seq: 0,
    created_at: '',
    updated_at: '',
    created_tx: '',
    updated_tx: '',
    origin: {} as JsonMap,
    facets: {} as JsonMap,
    structural: {} as JsonMap,
    governance: {} as JsonMap,
    retention: {} as JsonMap,
    expires_at: '',
  }
}

/** A Proposition endpoint written in a KML clause. */
function termValue(b: Bindings, term: Term, what: string): Json {
  if ('Variable' in term) return { id: handleId(b, term.Variable) }
  if ('Param' in term) {
    const value = parameter(b, term.Param)
    return typeof value === 'string' ? { id: value } : value
  }
  if ('Literal' in term) return kipValue(term.Literal)
  if ('Match' in term) {
    // `{id: "C-1"}` is how one command references what an earlier one created:
    // handles are transaction-local, so an id is the only thing that crosses.
    // Only a stable identity is accepted — matching an endpoint by name would
    // pick a winner among Concepts that are allowed to share one.
    return matcherIdentity(b, term.Match, what)
  }
  throw errors.unsupportedCapability(
    `${what} written as a nested Proposition is not implemented by this ` +
      `engine yet; see DESCRIBE CAPABILITIES`,
  )
}

/** The reference an identity matcher names, for use as a tuple endpoint. */
function matcherIdentity(
  b: Bindings,
  matcher: ObjectMatcher,
  what: string,
): Json {
  for (const field of ['id', 'canonical_id']) {
    const value = matcher[field]
    if (value === undefined) continue
    if ('Literal' in value) {
      const literal = kipValue(value.Literal)
      if (typeof literal === 'string') return { [field]: literal }
    } else if ('Param' in value) {
      const resolved = parameter(b, value.Param)
      if (typeof resolved === 'string') return { [field]: resolved }
    }
    throw errors.identitySelectorRequired(
      `${what} must name ${field} with a literal or a parameter`,
    )
  }
  throw errors.identitySelectorRequired(
    `${what} written as an object must name a stable identity: ` +
      `{id: "…"} or {canonical_id: "…"}`,
  )
}

function predicateName(b: Bindings, atom: { Literal: string } | { Param: string } | { Variable: string }): string {
  if ('Literal' in atom) return atom.Literal
  if ('Param' in atom) {
    const value = parameter(b, atom.Param)
    if (typeof value !== 'string') {
      throw errors.typeMismatch('a predicate must be a symbol string')
    }
    return value
  }
  throw errors.invalidSyntax(
    'ENSURE PROPOSITION needs an exact predicate, not a variable',
  )
}

function numberOf(b: Bindings, value: { Literal: unknown } | { Param: string }, what: string): number {
  const resolved = scalar(b, value as never)
  if (typeof resolved !== 'number' || !Number.isInteger(resolved)) {
    throw errors.typeMismatch(
      `${what} must be an integer, got ${JSON.stringify(resolved)}`,
    )
  }
  return resolved
}

/** `retention.expires_at`, lifted out for the retention sweep (§34). */
function expiresAt(retention: JsonMap): string {
  const value = retention.expires_at
  if (value === undefined || value === null) return ''
  if (typeof value !== 'string') {
    throw errors.typeMismatch('`retention.expires_at` must be a timestamp')
  }
  return normalizeTime(value, 'retention.expires_at')
}

/** `valid_time: {from, until}` — world validity, never storage lifecycle (§34). */
function validTimePart(validTime: JsonMap, part: 'from' | 'until'): string {
  const value = validTime[part]
  if (value === undefined || value === null) return ''
  if (typeof value !== 'string') {
    throw errors.typeMismatch(`\`valid_time.${part}\` must be a timestamp`)
  }
  return normalizeTime(value, `valid_time.${part}`)
}

/** Evidence carries its payload inline or by content reference (§19). */
function splitPayload(payload: JsonMap): [string, Json, string] {
  if (Object.keys(payload).length === 0) return ['', null, '']
  const ref = payload.content_ref
  if (typeof ref === 'string' && ref !== '') return ['external', null, ref]
  const inline = Object.hasOwn(payload, 'inline') ? (payload.inline as Json) : payload
  return ['inline', inline, '']
}

/** Exposed for the tests that pin the routing table. */
export const CORE_STRUCTURAL_FIELDS = CORE_STRUCTURAL

/** Exposed so `DESCRIBE CAPABILITIES` can name what is not built yet. */
export const SYMBOL_KINDS: readonly SymbolKind[] = [
  'ConceptType',
  'PredicateType',
  'Facet',
  'StructuralField',
  'Enum',
]
