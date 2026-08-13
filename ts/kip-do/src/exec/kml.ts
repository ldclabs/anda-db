/**
 * KML execution: UPSERT, UPDATE, MERGE, DELETE.
 *
 * The structural difference from the Rust engine is that a statement here is
 * atomic. `anda_db` has no write-ahead log, so its executor runs a full
 * preflight pass (`kml.rs:86-126`) to catch declared failures before writing,
 * and still documents that "a crashed multi-block UPSERT may leave a prefix
 * of its blocks applied" (`mod.rs:761-770`). `ctx.storage.transactionSync()`
 * removes both the preflight and the failure window: the callback either
 * completes or the whole statement rolls back.
 *
 * That is why every method here is synchronous and takes an already-resolved
 * token map. Awaiting inside a transaction is not possible, and awaiting
 * inside a read-modify-write outside one would open the input gate and let
 * another mutation interleave.
 */

import type {
  ConceptBlock,
  ConceptMatcher,
  DeleteStatement,
  Json,
  KmlStatement,
  MergeStatement,
  PropositionBlock,
  TargetTerm,
  UpdateExpr,
  UpdateStatement,
  UpdateValue,
  UpsertBlock,
  WhereClause,
} from '../kip/ast.js'
import {
  type EntityID,
  type JsonMap,
  conceptID,
  formatEntityID,
  parseEntityID,
  propositionID,
} from '../entity.js'
import {
  KipError,
  constraintViolation,
  duplicateExists,
  immutableTarget,
  internalError,
  invalidSyntax,
  notFound,
  referenceError,
  typeMismatch,
  versionConflict,
} from '../errors.js'
import type { Store } from '../store.js'
import { SolutionContext } from './solution.js'
import { KqlExecutor } from './kql.js'

/**
 * Meta-types whose instances are schema definitions.
 *
 * Creating and updating them through UPSERT is how a schema is declared, so
 * that stays allowed. Deleting or merging one is not: every instance of the
 * described type depends on it, and a single command would invalidate them
 * all with no way back.
 */
const PROTECTED_TYPES = new Set(['$ConceptType', '$PropositionType'])

/** Meta-type under which concept types are declared. */
const META_CONCEPT_TYPE = '$ConceptType'
/** Meta-type under which predicates are declared. */
const META_PROPOSITION_TYPE = '$PropositionType'

/** Reserved metadata namespace; engine-owned, never client-writable. */
const RESERVED_PREFIX = '_'

/**
 * Concepts the engine refuses to mutate through any KML statement.
 *
 * These are the schema's own foundations — the self-defining meta-types, the
 * `Domain` type, the `belongs_to_domain` predicate that anchors every domain
 * membership, and the core domain itself — plus the system actor identities.
 * Rewriting any of them would invalidate the graph's ability to describe
 * itself, so KIP puts them out of reach rather than trusting callers.
 * Mirrors `is_protected_concept` in the Rust engine.
 */
function isProtectedSchemaConcept(type: string, name: string): boolean {
  if (
    type === META_CONCEPT_TYPE &&
    (name === META_CONCEPT_TYPE || name === META_PROPOSITION_TYPE || name === 'Domain')
  ) {
    return true
  }
  if (type === META_PROPOSITION_TYPE && name === 'belongs_to_domain') return true
  // The spec lists `CoreSchema` as the representative core domain.
  if (type === 'Domain' && name === 'CoreSchema') return true
  return false
}

/** `$self` / `$system` — the agent's own identity tuples. */
function isSystemActor(type: string, name: string): boolean {
  return type === 'Person' && (name === '$self' || name === '$system')
}

function isProtectedConcept(type: string, name: string): boolean {
  return isProtectedSchemaConcept(type, name) || isSystemActor(type, name)
}

export interface KmlResult {
  [key: string]: Json
}

/**
 * Key under which a concept's pre-resolved tokens are stored.
 *
 * `\u0000` separates the parts so a type ending in the separator cannot
 * collide with a name beginning with it.
 */
export function conceptTokenKey(type: string, name: string): string {
  return `${type}\u0000${name}`
}

/** Tokens resolved before the transaction opens, keyed by entity id string. */
export type TokenMap = Map<string, string[]>

export class KmlExecutor {
  private readonly kql: KqlExecutor
  /**
   * Statement-scoped memo of type definitions already verified or created.
   *
   * KIP is schema-first: a concept type and a predicate must be declared as
   * `$ConceptType` / `$PropositionType` concepts before anything can use
   * them. Without the memo a batch import would re-query the same definition
   * once per block; with it, once per statement.
   */
  private verifiedTypes = new Set<string>()
  /**
   * Suspends the protected-scope guard.
   *
   * The bundled capsules *create* the protected nodes — Genesis defines
   * `$ConceptType` by instantiating it — so bootstrap must be able to write
   * what ordinary statements may not. Enabled only by
   * `CognitiveNexus.applyBundledCapsules`, never from a client command.
   */
  privileged = false

  constructor(
    private readonly store: Store,
    private readonly nowMs: () => number = () => Date.now(),
  ) {
    this.kql = new KqlExecutor(store)
  }

  execute(
    statement: KmlStatement,
    tokens: TokenMap,
    tokVer: string,
  ): KmlResult {
    this.verifiedTypes = new Set()
    if ('Upsert' in statement) {
      return this.executeUpsert(statement.Upsert, tokens, tokVer)
    }
    if ('Delete' in statement) {
      return this.executeDelete(statement.Delete, tokens, tokVer)
    }
    if ('Update' in statement) {
      return this.executeUpdate(statement.Update, tokens, tokVer)
    }
    if ('Merge' in statement) {
      return this.executeMerge(statement.Merge, tokens, tokVer)
    }
    throw internalError(
      `unhandled KML statement: ${Object.keys(statement).join(', ')}`,
    )
  }

  // -------------------------------------------------------------------
  // UPSERT
  // -------------------------------------------------------------------

  private executeUpsert(
    blocks: readonly UpsertBlock[],
    tokens: TokenMap,
    tokVer: string,
  ): KmlResult {
    // The result shape is `UpsertResult` from the KIP types: the ids are part
    // of the contract, not a count — a caller uses them to reference what it
    // just wrote without a follow-up query.
    const upsertedConcepts: string[] = []
    const upsertedLinks: string[] = []
    // Handles are block-scoped in KIP: `?c` declared in one block is visible
    // to later items of the same block.
    const handles = new Map<string, EntityID>()

    for (const block of blocks) {
      const blockMetadata = block.metadata ?? {}
      for (const item of block.items) {
        if ('Concept' in item) {
          upsertedConcepts.push(
            this.upsertConcept(item.Concept, blockMetadata, handles, tokens, tokVer),
          )
        } else if ('Proposition' in item) {
          upsertedLinks.push(
            this.upsertProposition(
              item.Proposition,
              blockMetadata,
              handles,
              tokens,
              tokVer,
            ),
          )
        } else {
          throw internalError(
            `unhandled UPSERT item: ${Object.keys(item).join(', ')}`,
          )
        }
      }
    }

    return {
      blocks: blocks.length,
      upsert_concept_nodes: upsertedConcepts,
      upsert_proposition_links: upsertedLinks,
    }
  }

  /**
   * Fails unless `type` is declared as a `$ConceptType` concept.
   *
   * The meta-types themselves are exempt — otherwise declaring the first type
   * would require a type that could never be declared.
   */
  private requireConceptType(type: string): void {
    if (type === META_CONCEPT_TYPE || type === META_PROPOSITION_TYPE) return
    const key = `c:${type}`
    if (this.verifiedTypes.has(key)) return
    if (this.store.findConceptByTypeName(META_CONCEPT_TYPE, type) === null) {
      throw typeMismatch(`Concept type ${type} is not defined`)
    }
    this.verifiedTypes.add(key)
  }

  /** Fails unless `predicate` is declared as a `$PropositionType` concept. */
  private requirePropositionType(predicate: string): void {
    const key = `p:${predicate}`
    if (this.verifiedTypes.has(key)) return
    if (
      this.store.findConceptByTypeName(META_PROPOSITION_TYPE, predicate) === null
    ) {
      throw typeMismatch(`Proposition type ${predicate} is not defined`)
    }
    this.verifiedTypes.add(key)
  }

  private upsertConcept(
    block: ConceptBlock,
    blockMetadata: JsonMap,
    handles: Map<string, EntityID>,
    tokens: TokenMap,
    tokVer: string,
  ): string {
    // `{id:}` addresses an existing node: UPSERT may update it but never
    // creates one, because an id is the engine's to assign.
    if ('ID' in block.concept) {
      return this.upsertConceptById(block, blockMetadata, handles, tokens, tokVer)
    }
    const { type, name } = requireConceptObject(block.concept)
    // A bare re-declaration of a protected node changes nothing, and capsule
    // re-application relies on that being a no-op. Only a block that would
    // actually write to one is refused.
    if (
      Object.keys(block.set_attributes ?? {}).length > 0 ||
      Object.keys(block.metadata ?? {}).length > 0 ||
      Object.keys(blockMetadata).length > 0 ||
      (block.set_propositions ?? []).length > 0
    ) {
      this.guardProtected(type, name)
    }
    this.requireConceptType(type)

    let id = this.store.findConceptByTypeName(type, name)

    if (block.expect_version != null) {
      const current = id === null ? 0 : this.store.conceptVersion(id)
      if (current !== block.expect_version) {
        throw versionConflict(
          `Concept {type: ${JSON.stringify(type)}, name: ${JSON.stringify(name)}} ` +
            `EXPECT VERSION ${block.expect_version} does not match current ` +
            `_version ${current}; the UPSERT was aborted`,
        )
      }
    }

    const attributes = sanitize(block.set_attributes ?? {}, 'attributes')
    const metadata = sanitize(
      { ...blockMetadata, ...(block.metadata ?? {}) },
      'metadata',
    )

    if (id === null) {
      const meta = { ...metadata }
      initVersion(meta, this.nowMs())
      id = this.store.insertConcept(type, name, attributes, meta)
    } else {
      const existing = this.store.requireConcept(id)
      // Attributes are shallow-merged, never replaced: UPSERT is additive by
      // definition, and replacing would make a partial write destructive.
      const merged = { ...existing.attributes, ...attributes }
      const mergedMeta = { ...existing.metadata, ...metadata }
      bumpVersion(mergedMeta, this.nowMs())
      this.store.updateConceptFields(id, merged, mergedMeta)
    }

    if (block.handle) handles.set(block.handle, conceptID(id))
    this.indexConcept(id, tokens, tokVer)

    for (const set of block.set_propositions ?? []) {
      const object = this.resolveTarget(set.object, handles)
      this.writeLink(
        conceptID(id),
        set.predicate,
        object,
        {},
        sanitize({ ...blockMetadata, ...(set.metadata ?? {}) }, 'metadata'),
        tokens,
        tokVer,
      )
    }
    return formatEntityID(conceptID(id))
  }

  /** Refuses a mutation that targets system-protected schema. */
  private guardProtected(type: string, name: string): void {
    if (this.privileged) return
    if (isProtectedConcept(type, name)) {
      throw immutableTarget(
        `Concept {type: ${JSON.stringify(type)}, name: ${JSON.stringify(name)}} ` +
          `is system-protected and cannot be modified`,
      )
    }
  }

  /** `UPSERT { CONCEPT ?x { {id: "C:n"} ... } }` — update an existing node. */
  private upsertConceptById(
    block: ConceptBlock,
    blockMetadata: JsonMap,
    handles: Map<string, EntityID>,
    tokens: TokenMap,
    tokVer: string,
  ): string {
    const raw = (block.concept as { ID: string }).ID
    const entity = parseEntityID(raw)
    if (entity.kind !== 'concept') {
      throw invalidSyntax(`${raw} must be a Concept ID of the form "C:<n>"`)
    }
    const existing = this.store.getConcept(entity.id)
    if (!existing) throw notFound(`Concept ${raw} not found`)
    this.guardProtected(existing.type, existing.name)

    if (block.expect_version != null) {
      const current = this.store.conceptVersion(entity.id)
      if (current !== block.expect_version) {
        throw versionConflict(
          `Concept ${raw} EXPECT VERSION ${block.expect_version} does not ` +
            `match current _version ${current}; the UPSERT was aborted`,
        )
      }
    }

    const attributes = {
      ...existing.attributes,
      ...sanitize(block.set_attributes ?? {}, 'attributes'),
    }
    const metadata = {
      ...existing.metadata,
      ...sanitize({ ...blockMetadata, ...(block.metadata ?? {}) }, 'metadata'),
    }
    bumpVersion(metadata, this.nowMs())
    this.store.updateConceptFields(entity.id, attributes, metadata)
    if (block.handle) handles.set(block.handle, conceptID(entity.id))
    this.indexConcept(entity.id, tokens, tokVer)

    for (const set of block.set_propositions ?? []) {
      this.writeLink(
        conceptID(entity.id),
        set.predicate,
        this.resolveTarget(set.object, handles),
        {},
        sanitize({ ...blockMetadata, ...(set.metadata ?? {}) }, 'metadata'),
        tokens,
        tokVer,
      )
    }
    return formatEntityID(conceptID(entity.id))
  }

  private upsertProposition(
    block: PropositionBlock,
    blockMetadata: JsonMap,
    handles: Map<string, EntityID>,
    tokens: TokenMap,
    tokVer: string,
  ): string {
    if ('ID' in block.proposition) {
      const link = this.kql.requireLink(block.proposition.ID)
      if (link.kind !== 'proposition') {
        throw internalError(
          `expected proposition link, got ${formatEntityID(link)}`,
        )
      }
      const row = this.store.requireProposition(link.id)
      const existing = row.links.get(link.predicate)!
      if (block.expect_version != null) {
        const current = this.store.linkVersion(link.id, link.predicate)
        if (current !== block.expect_version) {
          throw versionConflict(
            `Proposition ${formatEntityID(link)} EXPECT VERSION ` +
              `${block.expect_version} does not match current _version ` +
              `${current}; the UPSERT was aborted`,
          )
        }
      }

      const attributes = {
        ...existing.attributes,
        ...sanitize(block.set_attributes ?? {}, 'attributes'),
      }
      const metadata = {
        ...existing.metadata,
        ...sanitize(
          { ...blockMetadata, ...(block.metadata ?? {}) },
          'metadata',
        ),
      }
      bumpVersion(metadata, this.nowMs())
      this.store.upsertLink(
        link.id,
        link.predicate,
        attributes,
        metadata,
      )
      this.indexProposition(link.id, tokens, tokVer)
      if (block.handle) handles.set(block.handle, link)
      return formatEntityID(link)
    }
    const { subject, predicate, object } = block.proposition.Object
    if (!('Literal' in predicate)) {
      throw referenceError(
        'UPSERT PROPOSITION requires a literal predicate',
      )
    }
    const subjectID = this.resolveTarget(subject, handles)
    const objectID = this.resolveTarget(object, handles)

    if (block.expect_version != null) {
      const rowId = this.store.findPropositionRow(subjectID, objectID)
      const current =
        rowId === null ? 0 : this.store.linkVersion(rowId, predicate.Literal)
      if (current !== block.expect_version) {
        throw versionConflict(
          `Proposition (${formatEntityID(subjectID)}, ${JSON.stringify(predicate.Literal)}, ` +
            `${formatEntityID(objectID)}) EXPECT VERSION ${block.expect_version} does not ` +
            `match current _version ${current}; the UPSERT was aborted`,
        )
      }
    }

    const id = this.writeLink(
      subjectID,
      predicate.Literal,
      objectID,
      sanitize(block.set_attributes ?? {}, 'attributes'),
      sanitize({ ...blockMetadata, ...(block.metadata ?? {}) }, 'metadata'),
      tokens,
      tokVer,
    )
    const link = propositionID(id, predicate.Literal)
    if (block.handle) handles.set(block.handle, link)
    return formatEntityID(link)
  }

  /**
   * Creates or updates one link, returning the proposition row id.
   *
   * Self-loops are rejected. The Rust engine attributes this to its storage
   * model; the real reason to keep it is that the deletion closure and the
   * EXPORT topological order both assume higher-order references are acyclic.
   */
  private writeLink(
    subject: EntityID,
    predicate: string,
    object: EntityID,
    attributes: JsonMap,
    metadata: JsonMap,
    tokens: TokenMap,
    tokVer: string,
  ): number {
    this.requirePropositionType(predicate)
    if (formatEntityID(subject) === formatEntityID(object)) {
      throw invalidSyntax(
        `self-loop propositions are not supported: Subject and object cannot ` +
          `be the same entity ${formatEntityID(subject)}`,
      )
    }
    this.requireExists(subject)
    this.requireExists(object)

    let rowId = this.store.findPropositionRow(subject, object)
    if (rowId === null) rowId = this.store.insertPropositionRow(subject, object)

    const existing = this.store.getProposition(rowId)?.links.get(predicate)
    const mergedAttributes = { ...(existing?.attributes ?? {}), ...attributes }
    const mergedMetadata = { ...(existing?.metadata ?? {}), ...metadata }
    if (existing) bumpVersion(mergedMetadata, this.nowMs())
    else initVersion(mergedMetadata, this.nowMs())

    this.store.upsertLink(rowId, predicate, mergedAttributes, mergedMetadata)
    this.indexProposition(rowId, tokens, tokVer)
    return rowId
  }

  private requireExists(entity: EntityID): void {
    if (entity.kind === 'concept') {
      if (!this.store.conceptExists(entity.id)) {
        throw notFound(`Concept ${formatEntityID(entity)} does not exist`)
      }
      return
    }
    const row = this.store.getProposition(entity.id)
    if (!row?.links.has(entity.predicate)) {
      throw notFound(`Proposition ${formatEntityID(entity)} does not exist`)
    }
  }

  private resolveTarget(
    term: TargetTerm,
    handles: Map<string, EntityID>,
  ): EntityID {
    if ('Variable' in term) {
      const bound = handles.get(term.Variable)
      if (!bound) {
        throw referenceError(
          `?${term.Variable} is not bound; declare the CONCEPT block before referencing it`,
        )
      }
      return bound
    }
    if ('Concept' in term) {
      const ids = this.kql.resolveConceptMatcher(term.Concept)
      if (ids.length === 0) {
        throw notFound(
          `no concept matches ${JSON.stringify(term.Concept)}`,
        )
      }
      return conceptID(ids[0]!)
    }
    if ('Proposition' in term) {
      if ('ID' in term.Proposition) {
        return parseEntityID(term.Proposition.ID)
      }
      // A meta-statement endpoint: the nested pattern must already name
      // exactly one existing link, since a higher-order proposition points at
      // a specific `P:{id}:{predicate}` and cannot create it implicitly.
      const matches = this.kql.resolveNestedProposition(
        term.Proposition,
        new SolutionContext(),
      )
      if (matches.length === 0) {
        throw notFound(
          'the nested proposition endpoint does not match any existing link',
        )
      }
      if (matches.length > 1) {
        throw referenceError(
          `the nested proposition endpoint matches ${matches.length} links; ` +
            `it must identify exactly one`,
        )
      }
      return matches[0]!
    }
    throw internalError(`unhandled target term: ${Object.keys(term).join(', ')}`)
  }

  // -------------------------------------------------------------------
  // DELETE
  // -------------------------------------------------------------------

  private executeDelete(
    statement: DeleteStatement,
    tokens: TokenMap,
    tokVer: string,
  ): KmlResult {
    if ('DeleteConcept' in statement) {
      const { target, where_clauses } = statement.DeleteConcept
      const targets = this.resolveTargets(target, where_clauses)
      const conceptIds: number[] = []
      for (const entity of targets) {
        if (entity.kind !== 'concept') continue
        const concept = this.store.getConcept(entity.id)
        if (!concept) continue
        if (PROTECTED_TYPES.has(concept.type)) {
          throw immutableTarget(
            `concept ${formatEntityID(entity)} is a protected system node`,
          )
        }
        this.guardProtected(concept.type, concept.name)
        conceptIds.push(entity.id)
      }
      // The closure must be computed before the concepts go: it is derived by
      // following string references, which stop resolving once the rows are
      // deleted.
      const propIds = this.store.propositionClosure(
        conceptIds.map((id) => conceptID(id)),
      )
      this.store.deletePropositionRows(propIds)
      const deleted = this.store.deleteConcepts(conceptIds)
      return { deleted_propositions: propIds.length, deleted_concepts: deleted }
    }

    if ('DeletePropositions' in statement) {
      const { target, where_clauses } = statement.DeletePropositions
      const targets = this.resolveTargets(target, where_clauses)
      let removed = 0
      const orphaned: EntityID[] = []
      for (const entity of targets) {
        if (entity.kind !== 'proposition') continue
        const row = this.store.getProposition(entity.id)
        for (const endpoint of [row?.subject, row?.object]) {
          if (endpoint?.kind !== 'concept') continue
          const concept = this.store.getConcept(endpoint.id)
          if (concept) this.guardProtected(concept.type, concept.name)
        }
        const rowGone = this.store.deleteLink(entity.id, entity.predicate)
        removed++
        // Higher-order propositions referencing this link are now dangling.
        orphaned.push(entity)
        if (!rowGone) this.indexProposition(entity.id, tokens, tokVer)
      }
      const cascade = this.store.propositionClosure(orphaned)
      this.store.deletePropositionRows(cascade)
      // Cascaded higher-order statements count as deleted propositions: they
      // are propositions, and reporting them separately would understate what
      // the statement removed.
      return { deleted_propositions: removed + cascade.length }
    }

    if ('DeleteAttributes' in statement) {
      const { attributes, target, where_clauses } = statement.DeleteAttributes
      return this.deleteKeys(
        target,
        where_clauses,
        attributes,
        'attributes',
        tokens,
        tokVer,
      )
    }

    if ('DeleteMetadata' in statement) {
      const { keys, target, where_clauses } = statement.DeleteMetadata
      for (const key of keys) {
        if (key.startsWith(RESERVED_PREFIX)) {
          throw constraintViolation(
            `metadata key ${JSON.stringify(key)} is engine-reserved and cannot be deleted`,
          )
        }
      }
      return this.deleteKeys(
        target,
        where_clauses,
        keys,
        'metadata',
        tokens,
        tokVer,
      )
    }

    throw internalError(
      `unhandled DELETE statement: ${Object.keys(statement).join(', ')}`,
    )
  }

  private deleteKeys(
    target: string,
    where: readonly WhereClause[],
    keys: readonly string[],
    field: 'attributes' | 'metadata',
    tokens: TokenMap,
    tokVer: string,
  ): KmlResult {
    const targets = this.resolveTargets(target, where)
    let updatedConcepts = 0
    let updatedPropositions = 0
    for (const entity of targets) {
      if (entity.kind === 'concept') {
        const concept = this.store.getConcept(entity.id)
        if (!concept) continue
        this.guardProtected(concept.type, concept.name)
        const map = { ...concept[field] }
        let changed = false
        for (const key of keys) {
          if (key in map) {
            delete map[key]
            changed = true
          }
        }
        if (!changed) continue
        const attributes = field === 'attributes' ? map : concept.attributes
        const metadata = field === 'metadata' ? map : { ...concept.metadata }
        bumpVersion(metadata, this.nowMs())
        this.store.updateConceptFields(entity.id, attributes, metadata)
        this.indexConcept(entity.id, tokens, tokVer)
        updatedConcepts++
      } else {
        const row = this.store.getProposition(entity.id)
        const link = row?.links.get(entity.predicate)
        if (!row || !link) continue
        const map = { ...link[field] }
        let changed = false
        for (const key of keys) {
          if (key in map) {
            delete map[key]
            changed = true
          }
        }
        if (!changed) continue
        const attributes = field === 'attributes' ? map : link.attributes
        const metadata = field === 'metadata' ? map : { ...link.metadata }
        bumpVersion(metadata, this.nowMs())
        this.store.upsertLink(row.id, entity.predicate, attributes, metadata)
        this.indexProposition(row.id, tokens, tokVer)
        updatedPropositions++
      }
    }
    return {
      updated_concepts: updatedConcepts,
      updated_propositions: updatedPropositions,
    }
  }

  // -------------------------------------------------------------------
  // UPDATE / MERGE
  // -------------------------------------------------------------------

  private executeUpdate(
    statement: UpdateStatement,
    tokens: TokenMap,
    tokVer: string,
  ): KmlResult {
    const all = this.resolveTargets(statement.target, statement.where_clauses)
    const matched = all.length
    // LIMIT truncates after counting matches, mirroring `kml.rs:1084-1091`.
    // Which elements survive is unspecified without ORDER BY; the entity
    // order here is at least deterministic, unlike the Rust engine's.
    const targets =
      statement.limit == null ? all : all.slice(0, statement.limit)

    let updated = 0
    for (const entity of targets) {
      if (entity.kind === 'concept') {
        const concept = this.store.getConcept(entity.id)
        if (!concept) continue
        this.guardProtected(concept.type, concept.name)
        const root = {
          attributes: concept.attributes,
          metadata: concept.metadata,
        }
        const attributes = applyUpdates(
          concept.attributes,
          statement.set_attributes,
          root,
        )
        const metadata = applyUpdates(
          concept.metadata,
          statement.set_metadata,
          root,
        )
        bumpVersion(metadata, this.nowMs())
        this.store.updateConceptFields(entity.id, attributes, metadata)
        this.indexConcept(entity.id, tokens, tokVer)
        updated++
      } else {
        const row = this.store.getProposition(entity.id)
        const link = row?.links.get(entity.predicate)
        if (!row || !link) continue
        const root = { attributes: link.attributes, metadata: link.metadata }
        const attributes = applyUpdates(
          link.attributes,
          statement.set_attributes,
          root,
        )
        const metadata = applyUpdates(
          link.metadata,
          statement.set_metadata,
          root,
        )
        bumpVersion(metadata, this.nowMs())
        this.store.upsertLink(row.id, entity.predicate, attributes, metadata)
        this.indexProposition(row.id, tokens, tokVer)
        updated++
      }
    }
    return { matched, updated }
  }

  private executeMerge(
    statement: MergeStatement,
    tokens: TokenMap,
    tokVer: string,
  ): KmlResult {
    // Both operands are bound by the same WHERE, so a missing source makes
    // the whole clause list unsatisfiable — the failure surfaces on whichever
    // resolution runs first. Replaying an applied MERGE lands here, and the
    // caller's real question is "did my merge happen?", which the target's
    // provenance trail answers.
    let source: EntityID
    let target: EntityID
    try {
      target = this.resolveSingle(statement.target, statement.where_clauses)
      source = this.resolveSingle(statement.source, statement.where_clauses)
    } catch (err) {
      throw this.diagnoseAlreadyMerged(err, statement)
    }
    if (source.kind !== 'concept' || target.kind !== 'concept') {
      throw referenceError('MERGE CONCEPT requires both operands to be concepts')
    }
    if (source.id === target.id) {
      // Source and target bind the same node: a no-op success (KIP §4.4).
      return {
        merged: true,
        links_repointed: 0,
        links_deduplicated: 0,
        attributes_filled: 0,
      }
    }

    const sourceConcept = this.store.requireConcept(source.id)
    const targetConcept = this.store.requireConcept(target.id)
    if (sourceConcept.type !== targetConcept.type) {
      throw constraintViolation(
        `cannot merge ${sourceConcept.type} into ${targetConcept.type}: types differ`,
      )
    }
    this.guardProtected(sourceConcept.type, sourceConcept.name)
    this.guardProtected(targetConcept.type, targetConcept.name)
    if (PROTECTED_TYPES.has(sourceConcept.type)) {
      throw immutableTarget(
        `concept type ${sourceConcept.type} is protected and cannot be merged`,
      )
    }

    const repointed = this.repointLinks(source, target, tokens, tokVer)

    // Target wins on conflict; the source's name is kept as an alias so the
    // merged identity stays findable by its old name.
    let attributesFilled = 0
    for (const key of Object.keys(sourceConcept.attributes)) {
      if (!(key in targetConcept.attributes)) attributesFilled++
    }
    const attributes: JsonMap = {
      ...sourceConcept.attributes,
      ...targetConcept.attributes,
    }
    const aliases = new Set<string>([
      ...toStringArray(targetConcept.attributes.aliases),
      ...toStringArray(sourceConcept.attributes.aliases),
      sourceConcept.name,
    ])
    attributes.aliases = [...aliases]

    // Provenance records the source's *identity* (`Type:Name`), not its id:
    // the id is gone after the merge and means nothing to a reader, while the
    // identity is what someone searching for the old name will look up. The
    // source's own trail rides along, so a chain of merges stays traceable.
    const metadata = { ...targetConcept.metadata }
    metadata._merged_from = [
      ...toStringArray(metadata._merged_from),
      ...toStringArray(sourceConcept.metadata._merged_from),
      `${sourceConcept.type}:${sourceConcept.name}`,
    ]
    bumpVersion(metadata, this.nowMs())

    this.store.updateConceptFields(target.id, attributes, metadata)
    this.store.deleteConcepts([source.id])
    this.indexConcept(target.id, tokens, tokVer)

    return {
      merged: true,
      links_repointed: repointed.repointed,
      links_deduplicated: repointed.deduplicated,
      attributes_filled: attributesFilled,
    }
  }

  /**
   * Repoints every proposition endpoint from `from` to `to`.
   *
   * When repointing would collide with an existing `(subject, object)` row,
   * the links are moved onto the surviving row and the source row is dropped
   * — which changes those links' ids, so higher-order propositions
   * referencing the old `P:{id}:{predicate}` must be repointed in turn. That
   * is why this is a worklist and not a single UPDATE.
   */
  private repointLinks(
    from: EntityID,
    to: EntityID,
    tokens: TokenMap,
    tokVer: string,
  ): { repointed: number; deduplicated: number } {
    let repointed = 0
    let deduplicated = 0
    const worklist: [EntityID, EntityID][] = [[from, to]]

    while (worklist.length > 0) {
      const [oldRef, newRef] = worklist.shift()!
      const oldStr = formatEntityID(oldRef)
      const affected = this.store.matchPropositionRows([oldRef], null, null)
      const reverse = this.store.matchPropositionRows(null, [oldRef], null)
      const rowIds = [...new Set([...affected, ...reverse])]

      for (const rowId of rowIds) {
        const row = this.store.getProposition(rowId)
        if (!row) continue
        const subject =
          formatEntityID(row.subject) === oldStr ? newRef : row.subject
        const object =
          formatEntityID(row.object) === oldStr ? newRef : row.object

        // Repointing collapsed the edge into a self-loop; it carries no
        // meaning, so drop it along with anything that referenced it.
        if (formatEntityID(subject) === formatEntityID(object)) {
          const orphans = [...row.links.keys()].map((p) =>
            propositionID(row.id, p),
          )
          this.store.deletePropositionRows([
            row.id,
            ...this.store.propositionClosure(orphans),
          ])
          continue
        }

        const collision = this.store.findPropositionRow(subject, object)
        if (collision === null || collision === row.id) {
          // No collision: move the endpoints in place. The row id survives,
          // so higher-order propositions referencing this row's links stay
          // valid and need no repointing.
          this.store.relocateProposition(row.id, subject, object)
          this.indexProposition(row.id, tokens, tokVer)
          repointed += row.links.size
          continue
        }

        // Fold this row's links into the surviving row, then retire it.
        for (const [predicate, props] of row.links) {
          const existing = this.store.getProposition(collision)?.links.get(predicate)
          if (existing) {
            // The surviving row already carries this predicate, so the two
            // links collapse into one; keys it lacks are filled from the
            // source rather than dropped.
            deduplicated++
          } else {
            repointed++
          }
          this.store.upsertLink(
            collision,
            predicate,
            { ...props.attributes, ...(existing?.attributes ?? {}) },
            { ...props.metadata, ...(existing?.metadata ?? {}) },
          )
          worklist.push([
            propositionID(row.id, predicate),
            propositionID(collision, predicate),
          ])
        }
        this.store.deletePropositionRows([row.id])
        this.indexProposition(collision, tokens, tokVer)
      }
    }
    return { repointed, deduplicated }
  }

  /**
   * Turns "the source is gone" into "the merge already happened", when the
   * target's provenance trail says so.
   */
  private diagnoseAlreadyMerged(err: unknown, statement: MergeStatement): unknown {
    if (!(err instanceof KipError) || err.code !== 'KIP_3002') return err

    // Work from the identities the WHERE *states* rather than from resolved
    // ids: the source is gone, so it has no id left to look up.
    const sourceId = this.identityFor(statement.source, statement.where_clauses)
    const targetId = this.identityFor(statement.target, statement.where_clauses)
    if (!sourceId || !targetId) return err

    const [type, ...rest] = targetId.split(':')
    const targetRow = this.store.findConceptByTypeName(type!, rest.join(':'))
    if (targetRow === null) return err

    const trail = toStringArray(
      this.store.getConcept(targetRow)?.metadata._merged_from,
    )
    if (!trail.includes(sourceId)) return err
    return notFound(
      `${sourceId} was already merged into ${targetId}; the statement has no effect`,
    )
  }

  /** The `Type:Name` a WHERE clause states for a variable, if it states one. */
  private identityFor(
    variable: string,
    where: readonly WhereClause[],
  ): string | null {
    for (const clause of where) {
      if (!('Concept' in clause)) continue
      if (clause.Concept.variable !== variable) continue
      const matcher = clause.Concept.matcher
      if ('Object' in matcher) {
        return `${matcher.Object.type}:${matcher.Object.name}`
      }
    }
    return null
  }

  // -------------------------------------------------------------------
  // Shared helpers
  // -------------------------------------------------------------------

  private resolveTargets(
    target: string,
    where: readonly WhereClause[],
  ): EntityID[] {
    const ctx = new SolutionContext()
    this.kql.executeWhere(where, ctx)
    const table = ctx.find(target)
    if (!table) {
      throw referenceError(
        `?${target} is not bound by the WHERE clause`,
      )
    }
    return table.entityDomain(target)
  }

  private resolveSingle(
    target: string,
    where: readonly WhereClause[],
  ): EntityID {
    const all = this.resolveTargets(target, where)
    if (all.length === 0) {
      // The bound node is gone — replaying an already-applied MERGE lands
      // here, and NotFound is what tells the caller it was already done.
      throw notFound(`?${target} matched no entity`)
    }
    if (all.length > 1) {
      throw duplicateExists(
        `?${target} must bind exactly one entity, but matched ${all.length}`,
      )
    }
    return all[0]!
  }

  /**
   * Writes the FTS row for a concept from the pre-resolved token map.
   *
   * Tokens are keyed by `(type, name)` rather than by id because they are
   * resolved *before* the transaction opens, when a newly created concept has
   * no id yet. `(type, name)` is the concept's identity in KIP and is known
   * from the statement itself.
   *
   * A miss is not an error: it means this statement did not change the
   * concept's searchable text. Clearing `tok_ver` marks the row for
   * `reindexStale` instead of writing tokens that might be stale.
   */
  private indexConcept(id: number, tokens: TokenMap, tokVer: string): void {
    const concept = this.store.getConcept(id)
    if (!concept) return
    const resolved = tokens.get(conceptTokenKey(concept.type, concept.name))
    if (resolved) this.store.setConceptFts(id, resolved, tokVer)
    else this.store.markConceptStale(id)
  }

  /**
   * Proposition text (predicates plus link properties) is only knowable after
   * the row exists, so it is never tokenized inline. The row is marked stale
   * and `CognitiveNexus.reindexStale` picks it up out of band.
   */
  private indexProposition(id: number, _tokens: TokenMap, _tokVer: string): void {
    this.store.markPropositionStale(id)
  }
}

// -----------------------------------------------------------------------
// Metadata bookkeeping
// -----------------------------------------------------------------------

function initVersion(metadata: JsonMap, nowMs: number): void {
  metadata._version = 1
  metadata._created_at = new Date(nowMs).toISOString()
  metadata._updated_at = metadata._created_at
}

function bumpVersion(metadata: JsonMap, nowMs: number): void {
  const current = typeof metadata._version === 'number' ? metadata._version : 1
  metadata._version = current + 1
  metadata._updated_at = new Date(nowMs).toISOString()
}

/** Strips engine-reserved keys from client-supplied maps. */
function sanitize(map: Record<string, unknown>, what: string): JsonMap {
  const out: JsonMap = {}
  for (const [key, value] of Object.entries(map)) {
    if (key.startsWith(RESERVED_PREFIX)) {
      throw constraintViolation(
        `${what} key ${JSON.stringify(key)} is engine-reserved; the ` +
          `${RESERVED_PREFIX} namespace is maintained by the engine`,
      )
    }
    out[key] = value
  }
  return out
}

function requireConceptObject(matcher: ConceptMatcher): {
  type: string
  name: string
} {
  if ('Object' in matcher) return matcher.Object
  throw referenceError(
    'UPSERT CONCEPT requires a {type, name} pattern so the concept can be created when absent',
  )
}

function applyUpdates(
  current: JsonMap,
  updates: readonly [string, UpdateValue][] | null,
  root: { attributes: JsonMap; metadata: JsonMap },
): JsonMap {
  if (!updates) return { ...current }
  const out = { ...current }
  for (const [key, expr] of updates) {
    if (key.startsWith(RESERVED_PREFIX)) {
      throw constraintViolation(
        `cannot assign to engine-reserved key ${JSON.stringify(key)}`,
      )
    }
    const value = evalUpdateValue(expr, root)
    // A null result skips the key for this element rather than writing null,
    // so a COALESCE with no fallback leaves the existing value alone.
    if (value === null) continue
    out[key] = value
  }
  return out
}

function evalUpdateValue(
  expr: UpdateValue,
  root: { attributes: JsonMap; metadata: JsonMap },
): Json {
  if ('Json' in expr) return expr.Json
  if ('Expr' in expr) return evalUpdateOperand(expr.Expr, root)
  throw internalError(
    `unhandled UPDATE value: ${Object.keys(expr).join(', ')}`,
  )
}

function evalUpdateOperand(
  operand: UpdateExpr,
  root: { attributes: JsonMap; metadata: JsonMap },
): Json {
  if ('Number' in operand) return operand.Number
  if ('Variable' in operand) {
    // The parser restricts these to the UPDATE target's own fields, which is
    // what lets each element be computed from its own row without a join.
    let cursor: unknown = root
    for (const segment of operand.Variable.path) {
      if (cursor === null || typeof cursor !== 'object') return null
      cursor = (cursor as Record<string, unknown>)[segment]
    }
    return (cursor ?? null) as Json
  }
  if ('Function' in operand) {
    const { func, args } = operand.Function
    switch (func) {
      case 'Add':
        return args.reduce<number>(
          (sum, a) => sum + numeric(evalUpdateOperand(a, root)),
          0,
        )
      case 'Mul':
        return args.reduce<number>(
          (product, a) => product * numeric(evalUpdateOperand(a, root)),
          1,
        )
      case 'Clamp': {
        const [value, lo, hi] = args
        if (!value || !lo || !hi) {
          throw internalError('CLAMP requires three arguments')
        }
        return Math.min(
          Math.max(
            numeric(evalUpdateOperand(value, root)),
            numeric(evalUpdateOperand(lo, root)),
          ),
          numeric(evalUpdateOperand(hi, root)),
        )
      }
      case 'Coalesce':
        for (const candidate of args) {
          const value = evalUpdateOperand(candidate, root)
          if (value !== null && value !== undefined) return value
        }
        return null
      default:
        throw internalError(`unhandled UPDATE function: ${String(func)}`)
    }
  }
  throw internalError(
    `unhandled UPDATE operand: ${Object.keys(operand).join(', ')}`,
  )
}

function numeric(value: Json): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : 0
}

function toStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return []
  return value.filter((v): v is string => typeof v === 'string')
}
