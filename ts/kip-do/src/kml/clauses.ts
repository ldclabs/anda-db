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
import type { Permission } from '../governance/index.js'
import { referencePolicy, stage as stagePurge } from '../governance/purge.js'
import {
  formatElementId,
  parseElementId,
  parseElementIdOfKind,
  type ElementId,
  type ElementKind,
  elementIdEquals,
} from '../id.js'
import { isJsonMap, jsonEquals, type Json, type JsonMap } from '../json.js'
import type {
  ConceptCreate,
  ConceptUpsert,
  ElementRef,
  Scalar,
  WhereClause,
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
import { resolveTargets } from './select.js'
import { applyAction, requireUpdatable } from './update.js'
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
  const select = (
    target: ElementRef,
    where: readonly WhereClause[] | null,
    limit: Scalar | null,
    what: string,
    permission: Permission,
  ) =>
    resolveTargets(tx, b, target, where, limit, request, operation, what, permission)

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
    for (const id of select(target, where_clauses, limit, 'RETRACT', 'retract_own').authorized(tx)) {
      if (expect_state !== null) {
        tx.expectAssertionStatus(id, scalarText(b, expect_state, 'EXPECT STATE'))
      }
      retract(tx, id)
    }
    return
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
    for (const id of select(target, where_clauses, limit, 'ARCHIVE', 'archive').authorized(tx)) {
      if (expect_state !== null) {
        tx.expectState(id, scalarText(b, expect_state, 'EXPECT STATE'))
      }
      changeState(tx, id, State.ARCHIVED, 'archive')
    }
    return
  }
  if ('Tombstone' in clause) {
    const { target, where_clauses, limit, expect_state } = clause.Tombstone
    for (const id of select(target, where_clauses, limit, 'TOMBSTONE', 'tombstone').authorized(tx)) {
      if (expect_state !== null) {
        tx.expectState(id, scalarText(b, expect_state, 'EXPECT STATE'))
      }
      changeState(tx, id, State.TOMBSTONED, 'tombstone')
    }
    return
  }
  if ('Update' in clause) {
    const { target, where_clauses, limit, expect_version, actions } = clause.Update
    for (const id of select(target, where_clauses, limit, 'UPDATE', 'update').authorized(tx)) {
      if (expect_version !== null) {
        tx.expectVersion(id, numberOf(b, expect_version, 'EXPECT VERSION'))
      }
      const element = tx.load(id)
      requireUpdatable(id, element)
      const before = JSON.stringify(element.row)
      for (const action of actions) applyAction(tx, b, element, action)
      if (JSON.stringify(element.row) !== before) tx.markChanged(id, 'update')
    }
    return
  }
  if ('Purge' in clause) {
    const { target, where_clauses, limit, reference_policy } = clause.Purge
    const policy = referencePolicy(
      reference_policy === null ? null : scalarText(b, reference_policy, 'REFERENCE POLICY'),
    )
    for (const id of select(target, where_clauses, limit, 'PURGE', 'purge').authorized(tx)) {
      stagePurge(tx, id, policy)
    }
    return
  }
  if ('MergeConcept' in clause) {
    const { source, into, where_clauses, expect_version } = clause.MergeConcept
    const sources = select(source, where_clauses, null, 'MERGE CONCEPT', 'merge_identity').authorized(tx)
    const targets = select(into, where_clauses, null, 'MERGE CONCEPT ... INTO', 'merge_identity').authorized(tx)
    return merge(tx, b, sources, targets, expect_version)
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
  authorizeRetention(tx, retention)
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
  const element: Element = { kind: 'Concept', row }
  tx.authorizeCreated(element, 'create')
  tx.stageNew(id, element)
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
  authorizeRetention(tx, retention)
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
  // §17, §18: which epistemic-mutation permission a new Assertion needs depends
  // on whom the claim is attributed to, and that is only knowable here. `assert`
  // is the floor for writing any commitment; recording somebody else's claim or
  // speaking as an actor each add their own on top of it.
  if (element.kind === 'Assertion') {
    tx.authorizeCreated(element, 'assert')
    const extra = attributionPermission(tx, element.row.asserted_by_key)
    if (extra !== 'assert') tx.authorizeCreated(element, extra)
  } else {
    tx.authorizeCreated(element, 'create')
  }
  tx.stageNew(id, element)
}

/**
 * Which epistemic-mutation permission a new Assertion needs, beyond `assert`.
 *
 * The three cases §17 keeps apart, decided by what Governance says about the
 * writer rather than by what the command claims:
 *
 * ```text
 * bound as this actor          assert                       one's own commitment
 * bound as representing it     assert_as_actor              exercising its authority
 * not bound to it at all       record_attributed_assertion  "X said P"
 * ```
 *
 * The third is not impersonation and must stay ordinary: a Formation Agent that
 * observed "Alice: I prefer dark mode" has to be able to store it as Alice's
 * stated claim without thereby being able to act as Alice.
 */
function attributionPermission(tx: Transaction, actorKey: string): Permission {
  if (actorKey === '') return 'assert'
  const bound = tx.authority.bindingClassOf(actorKey)
  if (bound === null) return 'record_attributed_assertion'
  return bound === 'self' || bound === 'service_identity' ? 'assert' : 'assert_as_actor'
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
  // Spec §51: name-only upsert is forbidden. A name is mutable grounding state
  // that may be duplicated, so resolving identity through it would merge two
  // different Concepts that happen to share a label. `key` is read only when
  // `id` is absent, so a member the selector never consults is never evaluated.
  const selectorId = matchText(b, matcher, 'id')
  const selectorKey = selectorId === null ? matchText(b, matcher, 'key') : null
  if (selectorId === null && selectorKey === null) {
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
  // MATCH is an `object_pattern` — the same production a KQL Concept pattern
  // uses — so `type` here is what it is there: schema-resolution sugar for an
  // exact `schema_ref` (§43.1). It carries identity weight in both halves of an
  // upsert. On a resolve it is part of the address, because key uniqueness is
  // scoped to `(space_id, schema_ref, key)` (§7.3). On a create it is the only
  // place the new Concept's type can come from, and `schema_ref` is fixed at
  // creation — so a Concept minted without one stays untyped forever, which
  // §10.1 does not admit as a state a Concept can be in.
  const declaredType = matchText(b, matcher, 'type')
  const schemaRef =
    declaredType === null
      ? null
      : formatSymbolRef(tx.env.resolveSymbol('ConceptType', declaredType, 'write'))
  const found =
    selectorId === null
      ? resolveByKey(tx, selectorKey as string, schemaRef)
      : resolveById(tx, selectorId, schemaRef)

  let existing: ElementId
  if (found !== null) {
    existing = found
    tx.bindExisting(clause.handle, existing)
    if (clause.expect_version !== null) {
      tx.expectVersion(existing, numberOf(b, clause.expect_version, 'EXPECT VERSION'))
    }
  } else {
    // Nothing matched, so this is the "insert" half — and three things can stop
    // it, in the order they stop being about what the caller asked for and
    // start being about what the engine may mint.
    if (clause.expect_version !== null) {
      const expected = numberOf(b, clause.expect_version, 'EXPECT VERSION')
      if (expected !== 0) {
        throw errors.versionConflict(
          `no Concept matches this selector, so it cannot be at version ${expected}`,
        )
      }
    }
    if (selectorId !== null) {
      // §53: an UPSERT by id resolves, it never mints — the id would not be the
      // one the caller named. Reported existence-neutrally, without saying
      // whether the element is absent or merely of another type, so that an id
      // probe cannot map the Space by reading the difference (§86.4).
      throw errors.notFoundOrNotVisible(
        `${selectorId} does not exist, and an UPSERT by id cannot mint an id ` +
          'the caller chose',
      )
    }
    existing = createFromMatch(tx, clause.handle, selectorKey as string, schemaRef)
  }

  const element = tx.load(existing)
  if (element.kind !== 'Concept') {
    throw errors.structuralReferenceInvalid(
      `${formatElementId(existing)} is a ${element.kind}, not a Concept`,
    )
  }
  // An upsert is a create or an update and the caller cannot know which in
  // advance, so each half is authorized as what it turned out to be. The
  // command gate already asked for both; this asks about *this* element.
  if (found === null) {
    tx.authorizeCreated(element, 'create')
  } else {
    tx.authorizeElement(existing, 'update')
  }
  const before = JSON.stringify(element.row)

  if (clause.set_attributes !== null) {
    Object.assign(element.row.attributes, assignments(b, clause.set_attributes))
  }
  if (clause.unset_attributes !== null) {
    for (const name of clause.unset_attributes) delete element.row.attributes[name]
  }
  if (clause.set_fields !== null) {
    applyConceptFields(tx, element.row, new Fields(assignments(b, clause.set_fields)))
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
 * The insert half of an UPSERT: a Concept pinned to the identity it matched.
 *
 * Only the identity comes from `MATCH`. Every other member of the pattern is a
 * *selector* — it says which Concept the clause is about, not what a new one
 * should hold — so grounding state arrives through `SET FIELDS` and nowhere
 * else. Seeding `name` from the selector would also make the same command mean
 * two things depending on whether it resolved or created.
 */
function createFromMatch(
  tx: Transaction,
  handle: string,
  key: string,
  schemaRef: string | null,
): ElementId {
  if (schemaRef === null) {
    throw errors.schemaSymbolNotFound(
      'UPSERT CONCEPT creates only through MATCH {type: …, key: …}: a ' +
        "Concept's type is schema-defined and fixed at creation, so a Concept " +
        'minted without one could never be given a type afterwards',
    )
  }
  const id = tx.mint('Concept')
  tx.bindExisting(handle, id)
  const row: ConceptRow = {
    ...blank(id),
    client_key: '',
    schema_ref: schemaRef,
    key,
    name: '',
    canonical_id: '',
    aliases: [],
    attributes: {},
    merged_into: '',
  }
  const element: Element = { kind: 'Concept', row }
  tx.authorizeCreated(element, 'create')
  tx.stageNew(id, element)
  return id
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
    const element: Element = { kind: 'Proposition', row }
    tx.authorizeCreated(element, 'create')
    tx.stageNew(id, element)
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
  requireStanding(tx, id, element.row, 'RETRACT')
  if (element.row.status === 'retracted') return
  element.row.status = 'retracted'
  element.row.retracted_at = tx.cx.at
  tx.markChanged(id, 'retract')
}

/**
 * Whether this caller may record that the *source* withdrew a claim (§68).
 *
 * `RETRACT` and `SUPERSEDE` state something about the original actor: that it
 * took its claim back. Only two kinds of caller can honestly say so — the one
 * that wrote the record, and one an ActorBinding says represents the actor.
 *
 * A moderator who holds neither is not stuck: `ARCHIVE` and `TOMBSTONE` remove
 * the Assertion from ordinary recall without claiming anybody recanted, which is
 * the true statement available to it. Letting it retract instead would have the
 * engine assert something about the source that never happened.
 */
function requireStanding(
  tx: Transaction,
  id: ElementId,
  row: AssertionRow,
  what: string,
): void {
  if (tx.mayRepresentAssertion(row)) return
  throw errors.retractionNotAuthorized(
    `${what} records that the source withdrew ${formatElementId(id)}, and this ` +
      `Principal neither wrote it nor is bound to the actor it is attributed ` +
      `to. ARCHIVE or TOMBSTONE excludes it from recall without claiming a ` +
      `retraction that did not happen`,
  )
}

/** `SUPERSEDE ... BY` — a later Assertion replaces an earlier one (§15.1). */
function supersede(tx: Transaction, id: ElementId, by: ElementId): void {
  if (elementIdEquals(id, by)) {
    throw errors.supersessionMismatch(
      `${formatElementId(id)} cannot supersede itself`,
    )
  }
  tx.authorizeElement(id, 'supersede_own')
  tx.authorizeElement(by, 'supersede_own')
  const older = requireKind(tx, id, 'Assertion')
  const newer = requireKind(tx, by, 'Assertion')
  requireStanding(tx, id, older.row, 'SUPERSEDE')
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
  if (elementIdEquals(id, by)) {
    throw errors.evidenceCorrectionConflict(
      `${formatElementId(id)} cannot correct itself`,
    )
  }
  tx.authorizeElement(id, 'maintain')
  tx.authorizeElement(by, 'maintain')
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
  tx.authorizeElement(id, 'update')
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

/**
 * `MERGE CONCEPT ... INTO` — consolidating two records of one thing.
 *
 * Non-destructive (§11.1): the source keeps every field it had, gains a
 * `merged_into` forwarding pointer and the `merged` state. A reader that
 * followed a reference to it can therefore tell a consolidated Concept from a
 * retired one, which `archived` would not have said.
 *
 * A selection naming more than one of either side is refused rather than
 * resolved: identity is not something to pick by description, and a merge that
 * guessed which of two Concepts named "Alice" was meant would consolidate the
 * wrong pair irreversibly.
 */
function merge(
  tx: Transaction,
  b: Bindings,
  sources: readonly ElementId[],
  targets: readonly ElementId[],
  expectVersion: Scalar | null,
): void {
  if (sources.length !== 1 || targets.length !== 1) {
    // Not a merge conflict — a selector problem. Identity is never chosen by
    // description, and a merge that guessed which of two Concepts named
    // "Alice" was meant would consolidate the wrong pair irreversibly.
    throw errors.identitySelectorRequired(
      `MERGE CONCEPT needs exactly one source and one target; this selection ` +
        `named ${sources.length} and ${targets.length}. Name a stable ` +
        `identity — {key: …} or {id: …} — rather than a description`,
    )
  }
  const source = sources[0] as ElementId
  const target = targets[0] as ElementId
  if (elementIdEquals(source, target)) {
    throw errors.identityMergeConflict(
      `${formatElementId(source)} cannot be merged into itself`,
    )
  }
  if (expectVersion !== null) {
    tx.expectVersion(source, numberOf(b, expectVersion, 'EXPECT VERSION'))
  }

  const from = requireKind(tx, source, 'Concept')
  requireKind(tx, target, 'Concept')
  if (from.row.merged_into !== '') {
    // Re-pointing an already-merged Concept would make the forwarding chain
    // say two different things about where the identity went.
    throw errors.identityMergeConflict(
      `${formatElementId(source)} was already merged into ${from.row.merged_into}`,
    )
  }
  from.row.merged_into = formatElementId(target)
  from.row.state = State.MERGED
  tx.markChanged(source, 'merge')
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

/** The Concept an `id` selector names, or `null` when it names none. */
function resolveById(
  tx: Transaction,
  id: string,
  schemaRef: string | null,
): ElementId | null {
  const parsed = parseElementId(id)
  // The kind is spelled in the id the caller wrote, so saying so reveals
  // nothing they did not already state.
  if (parsed.kind !== 'Concept') {
    throw errors.structuralReferenceInvalid(
      `${formatElementId(parsed)} names a ${parsed.kind}, and UPSERT CONCEPT resolves Concepts`,
    )
  }
  const element = tx.store.load(parsed)
  if (element === null || element.kind !== 'Concept') return null
  // A declared type is part of the pattern, so an element of another type is
  // simply not a match. Reported as no match rather than as a type mismatch,
  // which would let an id probe map the Space by reading the difference
  // (§86.4) — and an upsert by id may not create, so this still fails loudly.
  if (schemaRef !== null && element.row.schema_ref !== schemaRef) return null
  return parsed
}

/** The Concept a `key` selector names, or `null` when it names none. */
function resolveByKey(
  tx: Transaction,
  key: string,
  schemaRef: string | null,
): ElementId | null {
  const found = tx.store.conceptByKey(tx.cx.space, schemaRef, key)
  return found === null ? null : { kind: 'Concept', seq: found.id }
}

/**
 * Reads one `MATCH` member as a string.
 *
 * `MATCH` values share the pattern grammar, which admits variables and nested
 * matchers that mean nothing to an upsert: `?v` is bound by a `WHERE` an upsert
 * does not have. Rejecting them here is what keeps a member from being accepted
 * and then quietly skipped.
 *
 * A member of the wrong *type* is rejected for the same reason. Reading
 * `{type: 42}` as "no type declared" would turn a malformed command into a
 * different, valid one and answer it.
 */
function matchText(
  b: Bindings,
  matcher: ObjectMatcher,
  field: string,
): string | null {
  const value = matcher[field]
  if (value === undefined) return null
  let resolved: Json
  if ('Literal' in value) {
    resolved = kipValue(value.Literal)
  } else if ('Param' in value) {
    resolved = parameter(b, value.Param)
  } else {
    throw errors.identitySelectorRequired(
      `an UPSERT MATCH \`${field}\` must be a literal or a parameter`,
    )
  }
  if (typeof resolved !== 'string') {
    throw errors.typeMismatch(
      `an UPSERT MATCH \`${field}\` must be a string, got ${JSON.stringify(resolved)}`,
    )
  }
  return resolved
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
function applyConceptFields(tx: Transaction, row: ConceptRow, fields: Fields): void {
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
    authorizeRetention(tx, retention)
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

/**
 * Authorizes a `retention` block a creation carried.
 *
 * Setting how long an element is kept is a lifecycle decision under its own
 * permission (§80), not a side effect of writing content. The `UPDATE` path
 * refuses the field outright — retention is control-plane state, not a mutable
 * content field — so this is the one route by which a KML statement can set it,
 * and it is gated rather than free.
 */
function authorizeRetention(tx: Transaction, retention: JsonMap): void {
  if (Object.keys(retention).length === 0) return
  tx.require('manage_retention')
  // §163: a legal hold blocks erasure, so a cognitive writer that could set one
  // could make its own content undeletable. Placing or lifting a hold is its
  // own permission, above ordinary retention management.
  if (Object.hasOwn(retention, 'legal_hold')) tx.require('legal_hold')
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
