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
  ACTIVITY_TERMINAL,
  ASSERTION_MODES,
  EVIDENCE_ROLES,
  STANCES,
} from '../kip/semantics.js'
import type { Permission } from '../governance/index.js'
import {
  referencePolicy,
  stage as stagePurge,
  stagePayload as stagePayloadPurge,
} from '../governance/purge.js'
import {
  formatElementId,
  parseElementId,
  parseElementIdOfKind,
  tryParseElementId,
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
  predicateDef,
  structuralFieldDef,
  validateAttributes,
  validateFacet,
  validateFacetCarrier,
  validatePredicateEndpoints,
  validateStructural,
  validateStructuralEndpoints,
  type EndpointFacts,
  type StructuralFieldDef,
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
  type Endpoint,
} from '../term.js'
import { normalizeTime } from '../time.js'
import { render } from '../view.js'
import type { Transaction } from '../tx.js'
import { resolveTargets } from './select.js'
import {
  applyAction,
  carrierOf,
  checkAttributes,
  touchesAttributes,
  touchesStructural,
} from './update.js'
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
      const before = JSON.stringify(element.row)
      const attributesBefore =
        element.kind === 'Concept' ? { ...element.row.attributes } : {}
      // Rendered once: every action of one UPDATE reads the element as it was
      // when the statement began (§52.4).
      const view = render(element)
      for (const action of actions) applyAction(tx, b, element, action, view)
      if (touchesAttributes(actions)) {
        checkAttributes(tx, element, attributesBefore)
      }
      if (touchesStructural(actions)) checkStructural(tx, element)
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
  if ('PurgePayload' in clause) {
    // §60.6: the data-minimization instrument. A Space can discard observed raw
    // bytes after digesting them without destroying the evidence event, its
    // citations, or its provenance role. No `REFERENCE POLICY` and no referrer
    // check: the element survives, so nothing can be left pointing at nothing.
    const { target, where_clauses, limit } = clause.PurgePayload
    const selected = select(target, where_clauses, limit, 'PURGE PAYLOAD', 'purge')
    for (const id of selected.authorized(tx)) {
      const report = stagePayloadPurge(tx, id)
      if (!report.erased) {
        // §60.6: purging an already-purged payload is a `no_effect`, and saying
        // so beats a silent success that reads as "erased again".
        tx.warn(`the payload of ${formatElementId(id)} was already purged`)
        continue
      }
      tx.warn(
        `payload purge of ${formatElementId(id)} destroyed its bytes and ` +
          `scrubbed ${report.versionsScrubbed} recorded version(s); the ` +
          `record, its digest and its citations survive`,
      )
    }
    return
  }
  if ('SetRetention' in clause) {
    const { target, values, where_clauses, limit, expect_version } = clause.SetRetention
    const selected = select(
      target,
      where_clauses,
      limit,
      'SET RETENTION',
      'manage_retention',
    )
    // §19: retention is storage lifecycle. `expires_at` here is when the
    // *record* stops being kept, never when the claim stops applying — that is
    // `valid_time.until` on an Assertion, and nothing here touches it.
    const retention = assignments(b, values)
    checkRetention(retention)
    const expires = expiresAt(retention)
    for (const id of selected.authorized(tx)) {
      if (expect_version !== null) {
        tx.expectVersion(id, numberOf(b, expect_version, 'EXPECT VERSION'))
      }
      const element = tx.load(id)
      authorizeLegalHold(tx, element.row.retention, retention)
      // A block that says what is already recorded is a no-op rather than a
      // version bump: retention is policy, and restating a policy is not a
      // change to it.
      if (jsonEquals(element.row.retention, retention)) continue
      element.row.retention = retention
      element.row.expires_at = expires
      tx.markChanged(id, 'set_retention')
    }
    return
  }
  if ('MergeConcept' in clause) {
    const { source, into, where_clauses, expect_version } = clause.MergeConcept
    const sources = select(source, where_clauses, null, 'MERGE CONCEPT', 'merge_identity').authorized(tx)
    const targets = select(into, where_clauses, null, 'MERGE CONCEPT ... INTO', 'merge_identity').authorized(tx)
    return merge(tx, b, sources, targets, expect_version)
  }

  // Every clause the grammar produces is handled above, so this is a guard
  // against a grammar that grows rather than a list of things left to build.
  // It refuses by name: a mutation that reports success and writes nothing is
  // the defect this project keeps finding.
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
  // Kind only: these Facets are re-checked against the Concept's resolved type
  // by `validateAttributes`' neighbour below, once the type symbol is known.
  const facets = resolveFacets(tx, b, clause.set_facets, {
    kind: 'element',
    elementKind: 'Concept',
    schemaRef: formatSymbolRef(symbol),
  })
  const structural = collectStructural(tx, b, clause.set_structural, [])

  const key = fields.text('key')
  const canonicalId = fields.text('canonical_id')
  if (canonicalId !== '') authorizeCanonicalIdentity(tx)
  const aliases = fields
    .array('aliases')
    .filter((value): value is string => typeof value === 'string')
  const retention = fields.json('retention')
  authorizeRetention(tx, retention)
  const extraName = fields.text('name')
  fields.rest('Concept')

  const clientKey =
    clause.client_key === null ? '' : scalarText(b, clause.client_key, 'CLIENT KEY')
  if (resolveClientKey(tx, 'Concept', clientKey, clause.handle)) return

  const definition = tx.env.definitionPackage(symbol)
  validateAttributes(
    formatSymbolRef(symbol),
    definition?.definitions?.concept_types?.[symbol.name]?.attributes,
    attributes,
  ).throwIfInvalid()

  const row: ConceptRow = {
    ...blank(id),
    client_key: clientKey,
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
  checkStructural(tx, element)
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
  if (resolveClientKey(tx, kind, clientKey, clause.handle)) return
  const fields = new Fields(
    clause.set_fields === null ? {} : assignments(b, clause.set_fields),
  )
  // A record is not a Concept and has no type to name.
  const facets = resolveFacets(tx, b, clause.set_facets, {
    kind: 'element',
    elementKind: kind,
  })
  const structural = collectStructural(tx, b, clause.set_structural, CORE_STRUCTURAL[kind])
  const retention = fields.json('retention')
  authorizeRetention(tx, retention)
  const envelope = { ...blank(id), facets, structural: structural.profile, retention, expires_at: expiresAt(retention) }

  let element: Element
  switch (kind) {
    case 'Evidence': {
      const [payloadMode, payloadInline, contentRef] = splitPayload(
        fields.value('payload'),
      )
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
      // §11.3: a claim recorded now is attributed to the identity that
      // survived the merge, or the two would never meet again.
      const assertedBy = canonicalizeReference(tx, fields.json('asserted_by'))
      // Each citation keeps the role it was cited in: Core records that this
      // Assertion cites E *as supporting*, and never that E proves anything —
      // that judgement belongs to the Projection (§8.4).
      const evidence = structural.take('evidence').map(([value, opts]) => {
        // Stored in the wire shape §13.2 fixes — `{id, role}` — so the
        // view renders it without a rename, and one place fewer can drift
        // from the other.
        const citation: JsonMap = { id: referenceId(value) }
        // §20.13 fixes the citation roles. `challenge` and `support` are the
        // difference between dissent and corroboration, so a role no reader
        // can interpret is refused rather than stored.
        if (opts.role !== undefined && opts.role !== null) {
          if (typeof opts.role !== 'string') {
            throw errors.typeMismatch(
              'an Evidence citation `role` must be a string, got ' +
                JSON.stringify(opts.role),
            )
          }
          checkRegistry(opts.role, 'role', EVIDENCE_ROLES)
          citation.role = opts.role
        }
        return citation as unknown as { id: string; role?: string }
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
        stance: fields.registry('stance', STANCES, 'CREATE ASSERTION'),
        mode: fields.registry('mode', ASSERTION_MODES, 'CREATE ASSERTION'),
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
  checkStructural(tx, element)
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
  const attributesBefore = { ...element.row.attributes }

  if (clause.set_attributes !== null) {
    Object.assign(element.row.attributes, assignments(b, clause.set_attributes))
  }
  if (clause.unset_attributes !== null) {
    for (const name of clause.unset_attributes) delete element.row.attributes[name]
  }
  if (clause.set_fields !== null) {
    applyConceptFields(tx, element.row, new Fields(assignments(b, clause.set_fields)))
  }
  // An upsert's Facet clauses are the same clauses `UPDATE` runs, so they go
  // through the same applier: merged-result validation and §39 immutability
  // are not something a second spelling of the same write may skip.
  const upsertView = render(element)
  for (const assignment of clause.set_facets) {
    applyAction(tx, b, element, { SetFacet: assignment }, upsertView)
  }
  for (const unset of clause.unset_facets) {
    applyAction(tx, b, element, { UnsetFacet: unset }, upsertView)
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

  // An upsert is a create or an update (§51), and either half leaves a Concept
  // its type has to still accept. The insert half is checked even when the
  // clause writes no attributes: a create leaves a Concept its type accepts or
  // it does not happen (§36), and `CREATE CONCEPT` has always been held to
  // that.
  if (
    found === null ||
    clause.set_attributes !== null ||
    clause.unset_attributes !== null
  ) {
    checkAttributes(tx, element, attributesBefore)
  }
  if (clause.set_structural !== null || clause.unset_structural !== null) {
    checkStructural(tx, element)
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
  // §11.3: a new write canonicalizes a merged reference to the surviving
  // Concept. Without this a merge would be decorative — every later claim about
  // the merged-away Concept would accumulate on the identity the merge said was
  // the same one, and the two would never meet again.
  const subject = canonicalizeEndpoint(
    tx,
    endpointFromJson(termValue(b, clause.subject, 'subject')),
  )
  const object = canonicalizeEndpoint(
    tx,
    endpointFromJson(termValue(b, clause.object, 'object')),
  )
  const predicate = tx.env.resolveSymbol(
    'PredicateType',
    predicateName(b, clause.predicate),
    'write',
  )
  const predicateRef = formatSymbolRef(predicate)
  const definition = tx.env.definitionPackage(predicate)
  const def = definition === undefined ? undefined : predicateDef(definition, predicate.name)
  if (def !== undefined) {
    validatePredicateEndpoints(
      predicateRef,
      def,
      factsFor(tx, subject),
      factsFor(tx, object),
    ).throwIfInvalid()
  }
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

/**
 * `TRANSITION ... TO` — an Activity's lifecycle (§55).
 *
 * The terminal set is the Core Package's (§20.13), not this engine's.
 * Inventing one locally is how the two reference engines came to disagree
 * about whether `TO "cancelled"` froze anything: each had a plausible extra
 * word and neither had the registry.
 */
const TERMINAL = new Set<string>(ACTIVITY_TERMINAL)

/**
 * The stored stand-in for "this Assertion states no confidence".
 *
 * Out of band rather than a nullable column, because range queries run over
 * it; `[0, 1]` is enforced on the way in so nothing real can collide with it.
 */
export const NO_CONFIDENCE = -1

/**
 * Resolves a `CLIENT KEY` to the element an earlier attempt already created.
 *
 * §52.1: a `CREATE` creates a historically distinct element *unless* a
 * `client_key` proves a retry of the same logical creation. Returns whether
 * the clause was satisfied by an existing element, in which case the handle now
 * points at it and nothing is written — a retry writes nothing, which is what
 * makes it a retry rather than a second creation.
 *
 * The resolved element is authoritative: this does not compare the incoming
 * fields against it and quietly rewrite one to match the other. A key that
 * names two different logical creations is a client bug, and picking a winner
 * silently would turn it into a data-loss bug.
 */
function resolveClientKey(
  tx: Transaction,
  kind: ElementKind,
  clientKey: string,
  handle: string,
): boolean {
  if (clientKey === '') return false
  const existing = tx.store.byClientKey(kind, tx.cx.space, clientKey)
  if (existing === null) return false
  tx.rebind(handle, {
    kind: existing.kind,
    seq: (existing.row as { id: number }).id,
  })
  return true
}

/** Checks one value against a Core registry (§20.13). */
export function checkRegistry(
  value: string,
  name: string,
  registry: readonly string[],
): void {
  if (registry.includes(value)) return
  throw errors.constraintViolation(
    `\`${name}\` is fixed by the Core Package (§20.13): it takes ` +
      `${registry.join(' | ')}, not ${JSON.stringify(value)}`,
  )
}

function transition(tx: Transaction, id: ElementId, to: string): void {
  tx.authorizeElement(id, 'update')
  const element = requireKind(tx, id, 'Activity')
  if (TERMINAL.has(element.row.status)) {
    // Terminal topology freezes with the Activity (§22.3): re-opening a
    // finished process would let its provenance be rewritten after the fact.
    throw errors.activityTerminal(
      `${formatElementId(id)} is ${element.row.status} and cannot transition ` +
        `to ${JSON.stringify(to)}`,
    )
  }
  if (element.row.status === to) return
  element.row.status = to
  if (TERMINAL.has(to) && element.row.ended_at === '') {
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
  // §29: administratively excluding somebody else's claim is a different act
  // from tidying one's own, and only the first is moderation. `archive` is
  // still asked for — this is on top of it, not instead of it, so a moderator
  // needs both and a Grant listing only `moderate_assertion` confers nothing.
  if (element.kind === 'Assertion' && !tx.mayRepresentAssertion(element.row)) {
    tx.require('moderate_assertion')
  }
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

  // §11.1: canonical resolution follows `merged_into` to its fixpoint, so a
  // cycle would make that walk run forever. The check is on the target's
  // chain, before anything is written.
  if (
    canonicalChain(tx, target).some((step) => elementIdEquals(step, source))
  ) {
    throw errors.identityMergeConflict(
      `${formatElementId(target)} already resolves back to ` +
        `${formatElementId(source)}; merging would make canonical resolution cycle`,
    )
  }

  // Already pointing where this statement wants it: the merge happened, so
  // saying so again changes nothing. A client that lost the response to a
  // MERGE and re-sent it (§80.4) gets `no_effect` rather than a conflict —
  // which is the answer the reference engine gives, and the only one that
  // makes the retry §80.4 recommends safe.
  if (from.row.merged_into === formatElementId(target)) return

  if (from.row.merged_into !== '') {
    // Re-pointing an already-merged Concept somewhere *else* would make the
    // forwarding chain say two different things about where the identity went.
    throw errors.identityMergeConflict(
      `${formatElementId(source)} was already merged into ${from.row.merged_into}`,
    )
  }
  from.row.merged_into = formatElementId(target)
  from.row.state = State.MERGED
  tx.markChanged(source, 'merge')
}

// --- merged identity --------------------------------------------------------

/**
 * The `merged_into` chain above one Concept, ending at its canonical id.
 *
 * Bounded independently of the cycle check that maintains it: a chain longer
 * than this is corrupt state, and walking it forever would turn corruption
 * into a hang.
 */
function canonicalChain(tx: Transaction, from: ElementId): ElementId[] {
  const MAX_HOPS = 64
  const chain: ElementId[] = [from]
  let cursor = from
  for (let hop = 0; hop < MAX_HOPS; hop += 1) {
    const element = tx.peek(cursor)
    if (element === null || element.kind !== 'Concept') return chain
    if (element.row.merged_into === '') return chain
    const next = tryParseElementId(element.row.merged_into)
    if (next === null) return chain
    if (chain.some((step) => elementIdEquals(step, next))) return chain
    chain.push(next)
    cursor = next
  }
  throw errors.internalError(
    `the merged_into chain above ${formatElementId(from)} is longer than ` +
      `${MAX_HOPS} hops`,
  )
}

/**
 * Follows a merged Concept's forwarding pointer to the identity that survived.
 *
 * Only for endpoints of a *new* write (§11.3). A historical Proposition keeps
 * referring to what it referred to (§11.2): rewriting those would erase what
 * the memory used to say, which is the whole reason merge is non-destructive.
 */
function canonicalizeEndpoint(tx: Transaction, endpoint: Endpoint): Endpoint {
  if (endpoint.kind !== 'local' || endpoint.id.kind !== 'Concept') {
    return endpoint
  }
  const chain = canonicalChain(tx, endpoint.id)
  const canonical = chain[chain.length - 1] as ElementId
  return elementIdEquals(canonical, endpoint.id)
    ? endpoint
    : { kind: 'local', id: canonical }
}

/**
 * Rewrites one reference value onto the Concept a merge made canonical.
 *
 * §11.3: ordinary new writes canonicalize merged references. Doing it only for
 * `ENSURE PROPOSITION` endpoints makes a merge decorative for everything else:
 * new Assertions keep accumulating under `asserted_by: :A` after A was merged
 * into B, and the two identities the merge declared to be one never meet again.
 *
 * A reference this cannot resolve is left exactly as written. Canonicalizing is
 * a rewrite toward an identity the Space already declared; it is not a place to
 * invent one.
 */
export function canonicalizeReference(tx: Transaction, value: JsonMap): JsonMap {
  if (!isJsonMap(value) || typeof value.id !== 'string') return value
  const id = tryParseElementId(value.id)
  if (id === null || id.kind !== 'Concept') return value
  const chain = canonicalChain(tx, id)
  const canonical = chain[chain.length - 1] as ElementId
  if (elementIdEquals(canonical, id)) return value
  return { ...value, id: formatElementId(canonical) }
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

  /**
   * Reads a Core-registry field, refusing a word the registry does not name.
   *
   * The protocol layer checks these too (§20.13), but only where the command
   * spells a literal — a `:parameter` is bound here, at execution time, which
   * is the first moment its value exists. Leaving the engine's half out is how
   * `stance: :s` came to store `"maybe"`: the row keeps a word no reader can
   * interpret, `?a.stance` reads back null, and the projection counts the
   * Assertion as an actor who engaged.
   */
  registry(name: string, registry: readonly string[], what: string): string {
    const value = this.required(name, what)
    checkRegistry(value, name, registry)
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

  /**
   * Reads a field as whatever JSON it is, without re-typing it.
   *
   * {@link json} coerces anything that is not an object to `{}`, which is right
   * for a hook block and wrong for an Evidence payload: §15.3 makes the payload
   * the observation, and invariant 33 forbids re-typing it. A `payload: "she
   * said yes"` that arrived as a string has to stay a string.
   */
  value(name: string): Json {
    const value = this.take(name)
    return value === undefined ? null : value
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
    if (value === undefined || value === null) return NO_CONFIDENCE
    if (typeof value !== 'number') {
      throw errors.typeMismatch(
        '`confidence` must be a number in [0, 1]; it is not trust and not ' +
          'memory strength',
      )
    }
    if (value < 0 || value > 1) {
      throw errors.constraintViolation(
        `\`confidence\` is epistemic support in [0, 1] (§13.6), got ${value}`,
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
  if (canonical !== '') {
    authorizeCanonicalIdentity(tx)
    row.canonical_id = canonical
  }
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

/**
 * Resolves each Facet symbol to its exact reference and validates it.
 *
 * `carrier` is what the Facet is being attached to: a Facet declares what it
 * is state *about*, so `OutcomeRecord` — the graded index over an instrument's
 * output — is refused on anything but Evidence rather than stored under a name
 * that would then mean something the Profile never said (§58).
 *
 * The check reads the carrier's Core kind, not which Concept type it is: a
 * Facet declaring `concept_types` refuses a record, which cannot be a Concept
 * of any type, and accepts any Concept. The reference engine draws the line in
 * the same place, and `schema-endpoints` in the conformance suite pins it.
 */
function resolveFacets(
  tx: Transaction,
  b: Bindings,
  list: readonly FacetAssignment[],
  carrier: EndpointFacts,
): JsonMap {
  const out: JsonMap = {}
  for (const entry of list) {
    const symbol = tx.env.resolveSymbol('Facet', symbolName(b, entry.facet), 'write')
    const text = formatSymbolRef(symbol)
    const values = assignments(b, entry.values)
    const definition = tx.env.definitionPackage(symbol)
    const def = definition === undefined ? undefined : facetDef(definition, symbol.name)
    if (def !== undefined) {
      validateFacetCarrier(text, def, carrier)
        .extend(validateFacet(text, def, values))
        .throwIfInvalid()
    }
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

/** Reads a structural reference's `index` option as a zero-based position. */
export function edgeIndex(b: Bindings, edge: StructuralEdge): number | null {
  if (edge.options === null) return null
  const value = options(b, edge.options).index
  if (value === undefined || value === null) return null
  if (typeof value !== 'number' || !Number.isInteger(value) || value < 0) {
    throw errors.typeMismatch(
      `a structural reference \`index\` is a zero-based position, got ` +
        JSON.stringify(value),
    )
  }
  return value
}

/** Whether a structural field declares a stable order (§17.4). */
export function orderedField(tx: Transaction, field: string): boolean {
  return fieldDef(tx, field)?.ordered === true
}

/**
 * Whether a structural field holds at most one reference (§17.5).
 *
 * The one that decides whether `SET STRUCTURAL` appends or *replaces*:
 * appending to a single-cardinality field and then failing the cardinality
 * check would refuse the one write the Specification says this form is for.
 */
export function singleField(tx: Transaction, field: string): boolean {
  return fieldDef(tx, field)?.cardinality?.max === 1
}

function fieldDef(
  tx: Transaction,
  field: string,
): StructuralFieldDef | undefined {
  try {
    const symbol = tx.env.resolveSymbol('StructuralField', field, 'read')
    const pkg = tx.env.definitionPackage(symbol)
    return pkg === undefined ? undefined : structuralFieldDef(pkg, symbol.name)
  } catch {
    return undefined
  }
}

/** An `index` on a field that declares no order (§17.4). */
export function unorderedIndex(field: string): Error {
  return errors.constraintViolation(
    `\`${field}\` is not an ordered structural field, so a reference in it ` +
      `has no position; an \`index\` here would order nothing and no query ` +
      `could read it back (§17.4)`,
  )
}

export function positionTaken(field: string, index: number): Error {
  return errors.constraintViolation(
    `two references claim position ${index} of \`${field}\` in one mutation ` +
      `plan; an order cannot hold both, and picking one would be the engine ` +
      `choosing (§17.4)`,
  )
}

/**
 * Places one reference in a structural field, honoring declared order (§17.4).
 *
 * An **ordered** field carries one stable, dense, zero-based total order per
 * source element. Three rules the Specification states as MUSTs, and which this
 * engine used to accept and then drop on the floor:
 *
 * - a reference written without an index appends, in mutation order;
 * - an explicit `{index: n}` declares the intended position, and one outside
 *   the dense range `0..=len` fails validation — positions are dense, and
 *   appending is exactly `len`;
 * - two explicit positions that collide inside one mutation plan fail.
 *
 * An **unordered** field has no positions at all, so `{index: n}` on one is
 * refused rather than ignored: silently dropping it would let an author believe
 * they had ordered something no query can order.
 *
 * Returns whether the field's contents changed.
 */
export function placeReference(
  items: Json[],
  value: Json,
  index: number | null,
  ordered: boolean,
  field: string,
): boolean {
  const at = items.findIndex((item) => sameReference(item, value))
  if (index === null) {
    if (at >= 0) return false
    items.push(value)
    return true
  }
  if (!ordered) throw unorderedIndex(field)
  // A reference already present is moved rather than duplicated: re-stating one
  // with a position is how an author re-orders.
  if (at >= 0) items.splice(at, 1)
  if (index > items.length) {
    throw errors.constraintViolation(
      `position ${index} is outside \`${field}\`, which holds ${items.length} ` +
        `reference(s); positions are dense, and appending is position ` +
        `${items.length} (§17.4)`,
    )
  }
  items.splice(index, 0, value)
  return at !== index
}

/** Whether two structural entries point at the same element. */
export function sameReference(stored: Json, given: Json): boolean {
  const idOf = (value: Json): string | null => {
    if (typeof value === 'string') return value
    if (isJsonMap(value) && typeof value.id === 'string') return value.id
    return null
  }
  const a = idOf(stored)
  const b2 = idOf(given)
  if (a !== null && b2 !== null) return a === b2
  return jsonEquals(stored, given)
}

/**
 * Validates one element's Profile structural fields against their declarations
 * (§62–§66).
 *
 * Endpoint types, cardinality and uniqueness together, because they are one
 * declaration: `has_step` says an Experience holds ordered, distinct
 * ExperienceSteps, and an engine that counted them without asking what they
 * were would admit the wrong kind of element as long as it came alone.
 *
 * Judged on the element's whole structural map after the statement, not on the
 * clause: a minimum cardinality is a statement about what the element holds,
 * and `UNSET STRUCTURAL` can break it as easily as `SET` can.
 */
export function checkStructural(tx: Transaction, element: Element): void {
  const source = carrierOf(element)
  for (const [field, refs] of Object.entries(element.row.structural)) {
    if (!Array.isArray(refs)) continue
    const symbol = tx.env.resolveSymbol('StructuralField', field, 'write')
    const definition = tx.env.definitionPackage(symbol)
    const def =
      definition === undefined ? undefined : structuralFieldDef(definition, symbol.name)
    // A field this environment cannot resolve declares nothing to hold the
    // write to, the same stance a Proposition takes on an unresolvable
    // predicate.
    if (def === undefined) continue
    const endpoints = refs.map((value) => endpointFromJson(value))
    validateStructuralEndpoints(
      field,
      def,
      source,
      endpoints.map((endpoint) => factsFor(tx, endpoint)),
    )
      .extend(validateStructural(field, def, endpoints.map(endpointKey)))
      .throwIfInvalid()
  }
}

/**
 * What this engine knows about one endpoint, for the schema to judge (§42–§44).
 *
 * A staged element is the authority: within a transaction, a reference to
 * something an earlier clause just created must see it, or a Proposition whose
 * subject the same block minted would look untyped.
 *
 * A canonical identity or a foreign Space reference resolves to nothing here,
 * and that is reported as unknown rather than as wrong — inventing a violation
 * from an unresolved lookup would reject legitimate cross-Space data.
 */
function factsFor(tx: Transaction, endpoint: Endpoint): EndpointFacts {
  if (endpoint.kind === 'literal') {
    return { kind: 'literal', datatype: endpoint.literal.datatype }
  }
  if (endpoint.kind !== 'local') return { kind: 'unresolved' }
  const elementKind = endpoint.id.kind
  if (elementKind !== 'Concept') return { kind: 'element', elementKind }
  const staged = tx.stagedConceptType(endpoint.id)
  const schemaRef =
    staged ?? (tx.store.load(endpoint.id)?.row as ConceptRow | undefined)?.schema_ref
  return {
    kind: 'element',
    elementKind,
    schemaRef: schemaRef === undefined || schemaRef === '' ? undefined : schemaRef,
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
  const claimed = new Map<string, Set<number>>()

  for (const edge of edges) {
    const name = symbolName(b, edge.field)
    // §11.3, as for a tuple endpoint: a record created now points at the
    // identity that survived, not at the one a merge retired.
    const value = canonicalizeReference(
      tx,
      referenceValue(mutationValue(b, edge.value), name),
    )
    if (coreFields.includes(name)) {
      const list = out.core.get(name) ?? []
      list.push([value, options(b, edge.options)])
      out.core.set(name, list)
      continue
    }
    const symbol = tx.env.resolveSymbol('StructuralField', name, 'write')
    const text = formatSymbolRef(symbol)
    const current = out.profile[text]
    const items = Array.isArray(current) ? [...current] : []
    // A declared position is honored on a create exactly as it is on an update
    // (§17.4): a Concept written with its steps out of order and positions
    // attached would otherwise land in mutation order, and the author would
    // have no way to tell.
    const index = edgeIndex(b, edge)
    if (index !== null) {
      const seen = claimed.get(text) ?? new Set<number>()
      if (seen.has(index)) throw positionTaken(text, index)
      seen.add(index)
      claimed.set(text, seen)
    }
    placeReference(items, value, index, orderedField(tx, text), text)
    out.profile[text] = items
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
    // An identity resolves the endpoint; it does not also filter it. Dropping
    // the rest would let `{id: "C-1", name: "Zed"}` write against C-1 whatever
    // C-1 is called.
    const extra = Object.keys(matcher).filter((key) => key !== field)
    if (extra.length > 0) {
      throw errors.identitySelectorRequired(
        `${what} names \`${field}\`, so it is resolved by identity and not ` +
          `matched by description; ${extra.join(', ')} would be silently ` +
          `ignored. Drop ${extra.length === 1 ? 'it' : 'them'}`,
      )
    }
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
  checkRetention(retention)
  authorizeLegalHold(tx, {}, retention)
}

/**
 * Gates a change to an element's legal hold.
 *
 * §19.1 gives the retention hook its `legal_hold` member and §60.3 states what
 * it does: a held element may not be purged, by anyone, whatever the reference
 * policy says. §60.3 then draws the conclusion this gate implements — because a
 * hold blocks erasure for everyone, the authority to set or lift one SHOULD be
 * scoped apart from ordinary retention management.
 *
 * Both directions are gated. *Placing* a hold is that authority. *Lifting* one
 * is a writer evading deletion — and lifting does not require naming the
 * member, because `SET RETENTION` replaces the block rather than patching it: a
 * hold disappears when the next block simply omits it. Gating on the transition
 * rather than on the words in the block is what closes that.
 */
function authorizeLegalHold(
  tx: Transaction,
  current: JsonMap,
  next: JsonMap,
): void {
  const held = (block: JsonMap) => block.legal_hold === true
  if (held(next) || held(current)) tx.require('legal_hold')
}

/**
 * Authorizes writing a Concept's canonical identity (§5.4).
 *
 * A `canonical_id` is a high-assurance claim that this Concept *is* the thing
 * some other system names — the identity a Capsule import resolves on, and the
 * one a merge follows. Deciding that is more authority than editing a label,
 * so it is its own permission rather than a side effect of `create` or
 * `update`.
 *
 * Clearing one asks for the same thing as setting one: an identity binding
 * that could be dropped by anyone who may rename the Concept would be no
 * binding at all.
 */
export function authorizeCanonicalIdentity(tx: Transaction): void {
  tx.require('bind_canonical_identity')
}

/** The members §19.1 gives the retention hook. */
const RETENTION_MEMBERS = ['retention_class', 'expires_at', 'legal_hold']

/**
 * Checks a retention block against §19.1's shape.
 *
 * A member outside it is refused rather than stored. The wire type carries
 * exactly these three, so anything else is written, kept, and then read back as
 * null — the caller's write appears to succeed while the value is unreachable
 * from every query that could notice it went missing.
 */
export function checkRetention(retention: JsonMap): void {
  for (const name of Object.keys(retention)) {
    if (!RETENTION_MEMBERS.includes(name)) {
      throw errors.schemaFieldNotFound(
        `\`retention\` has no member named \`${name}\`; §19.1 gives it ` +
          `${RETENTION_MEMBERS.join(', ')}. Storage-lifecycle state that needs ` +
          `a shape of its own belongs in a Facet`,
      )
    }
  }
  const kind = retention.retention_class
  if (kind !== undefined && kind !== null && typeof kind !== 'string') {
    throw errors.typeMismatch('`retention.retention_class` is a string')
  }
  const hold = retention.legal_hold
  if (hold !== undefined && hold !== null && typeof hold !== 'boolean') {
    throw errors.typeMismatch('`retention.legal_hold` is a boolean')
  }
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
/**
 * Splits an Evidence `payload` into the three columns that store it.
 *
 * A payload is whatever the observation was (§15.3): a string, a number, an
 * object, an array. Only two shapes are interpreted rather than stored —
 * absence, and an object naming `content_ref`, which is the external-payload
 * form of §15.4. Everything else is kept as it arrived, because invariant 33
 * forbids re-typing a transport-supplied payload, and because an engine that
 * quietly turned a scalar payload into an empty object would report a
 * successful `CREATE EVIDENCE` for an observation it did not keep.
 *
 * @see rs/anda_cognitive_nexus/src/kml/clauses.rs — `split_payload`
 */
function splitPayload(payload: Json): [string, Json, string] {
  if (payload === null) return ['', null, '']
  if (isJsonMap(payload) && Object.hasOwn(payload, 'content_ref')) {
    const ref = payload.content_ref
    return ['external', null, typeof ref === 'string' ? ref : '']
  }
  return ['inline', payload, '']
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
