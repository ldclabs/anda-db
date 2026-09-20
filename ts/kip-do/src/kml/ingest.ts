import { authorizedArtifact } from '../control.js'
import { digest } from '../schema/contracts.js'
/**
 * The request envelope's ingestion context (§71.1).
 *
 * ## Why the runtime mints the Evidence, and not the model
 *
 * An observation reaches a Brain through the transport: a user's message, a
 * tool's output, a document that arrived. The naive way to record it is to have
 * the model write `CREATE EVIDENCE ... SET FIELDS {payload: "…"}` with the
 * observation typed back out inside the command — and that is exactly the
 * fidelity risk §88.12 names. A model retyping an observation truncates it,
 * normalizes its whitespace, fixes its spelling, or paraphrases it, and the
 * record then says the source said something it did not.
 *
 * So the envelope carries the payload the runtime observed, this module mints
 * the Evidence from it, and the command only ever cites `:key`. The bytes never
 * pass through model-generated text.
 *
 * ## Minted inside the statement's transaction
 *
 * Not before it. A statement that fails takes its ingested Evidence with it, or
 * a Space accumulates observations whose claims were never recorded — Evidence
 * for nothing, indistinguishable later from an observation somebody chose not
 * to act on.
 *
 * @see rs/anda_cognitive_nexus/src/kml/mod.rs — `mint_ingested_evidence`
 */

import { errors } from '../errors.js'
import { formatElementId, tryParseElementId, type ElementId } from '../id.js'
import { isJsonMap, canonicalJson, type Json, type JsonMap } from '../json.js'
import {
  facetDef,
  formatSymbolRef,
  lineageOfSymbol,
  validateFacet,
  validateFacetCarrier,
} from '../schema/index.js'
import { blankEnvelope, type EvidenceRow } from '../store/index.js'
import { normalizeTime } from '../time.js'
import type { Transaction } from '../tx.js'
import { recordsOutcome } from './clauses.js'

/**
 * An element reference as the envelope spells one (§71.1): by exact id, or by
 * Concept Type lineage plus logical key — the same two shapes a bound
 * parameter takes. Never a name (§7.2), and never a Principal (§88.1).
 */
export type ElementReference = { id: string } | { type: string; key: string }

/** One Evidence record to mint inside the request's transaction scope. */
export interface IngestEvidence {
  /** The request-local binding name; commands cite it as `:key`. */
  key: string
  /** What kind of observation this is (§15.2). */
  evidence_class: string
  /** The inline payload, preserved without model rewriting. */
  payload?: Json
  /** A runtime artifact handle carrying the payload bytes instead. */
  payload_artifact?: string
  media_type?: string
  /** When the observation happened — not when the record was written. */
  observed_at?: string
  /**
   * The semantic source actor, recorded as Evidence source.
   *
   * An element reference, never a name and never a Principal (§88.1): who
   * authenticated is engine origin, and who said it is cognition.
   */
  source_actor?: ElementReference
  /** A retry-safe logical identity for the minted Evidence. */
  client_key?: string
  /**
   * Facet name to value object, validated exactly as `SET FACET` on
   * `CREATE EVIDENCE` would be (§71.1). This is how instrumentation attaches
   * `OutcomeRecord` to an ingested `outcome` without re-typing anything.
   */
  facets?: Record<string, JsonMap>
  extensions?: JsonMap
}

/** The `ingest` block of a request envelope. */
export interface IngestContext {
  evidence?: IngestEvidence[]
  extensions?: JsonMap
}

/** The character budgets §71 gives the ingest members. */
const LIMITS = {
  SHORT_LABEL: 256,
  OPAQUE_TOKEN: 8192,
  SOURCE_ACTOR: 512,
  ELEMENT_REFERENCE_KEY: 1024,
  CLIENT_KEY: 1024,
} as const

/**
 * Checks an ingest block's shape, before anything is minted.
 *
 * Whole-block and up front, like every other envelope invariant: a block that
 * turns out to be malformed halfway through would leave the Evidence its first
 * entries minted behind a request that was never valid.
 */
export function checkIngest(ingest: IngestContext): IngestEvidence[] {
  const entries = ingest.evidence ?? []
  if (entries.length === 0) {
    throw errors.invalidRequestEnvelope(
      'an ingest context must carry at least one Evidence entry',
    )
  }
  const seen = new Set<string>()
  for (const entry of entries) {
    checkEntry(entry)
    if (seen.has(entry.key)) {
      throw errors.invalidRequestEnvelope(
        `ingest key ${JSON.stringify(entry.key)} is claimed by two Evidence entries`,
      )
    }
    seen.add(entry.key)
  }
  return entries
}

function checkEntry(entry: IngestEvidence): void {
  bindingName(entry.key, 'ingest key')
  if ((entry.evidence_class ?? '').trim() === '') {
    throw errors.invalidRequestEnvelope(
      'an ingest Evidence entry must declare an evidence_class',
    )
  }
  bounded(entry.evidence_class, 'ingest evidence_class', LIMITS.SHORT_LABEL)
  optional(entry.payload_artifact, 'ingest payload_artifact', LIMITS.OPAQUE_TOKEN)
  optional(entry.media_type, 'ingest media_type', LIMITS.SHORT_LABEL)
  if (entry.observed_at !== undefined) normalizeTime(entry.observed_at, 'ingest.observed_at')
  optional(entry.client_key, 'ingest client_key', LIMITS.CLIENT_KEY)
  if (entry.source_actor !== undefined) {
    checkElementReference(entry.source_actor, 'ingest source_actor')
  }
  if (entry.facets !== undefined) {
    if (!isJsonMap(entry.facets)) {
      throw errors.invalidRequestEnvelope(
        'ingest facets is a map from Facet name to value object',
      )
    }
    for (const [name, values] of Object.entries(entry.facets)) {
      if (name.trim() === '' || !isJsonMap(values)) {
        throw errors.invalidRequestEnvelope(
          `ingest facets[${JSON.stringify(name)}] must be a value object`,
        )
      }
    }
  }
  // Exactly one, because the two answer the same question differently: an
  // entry carrying both leaves the runtime choosing which observation the
  // record is of.
  const inline = entry.payload !== undefined && entry.payload !== null
  const handle = entry.payload_artifact !== undefined
  if (inline === handle) {
    throw errors.invalidRequestEnvelope(
      `ingest entry ${JSON.stringify(entry.key)} must declare exactly one of ` +
        `payload / payload_artifact`,
    )
  }
}

/**
 * Checks that a reference takes exactly one of its two shapes (§71.1).
 *
 * A bare string is refused at the envelope: it could only be a name, and a
 * source recorded by name is a citation nothing resolves (§7.2).
 */
function checkElementReference(value: unknown, what: string): void {
  const shape = () =>
    errors.invalidRequestEnvelope(
      `${what} is an element reference: {id} or {type, key}, never a name`,
    )
  if (!isJsonMap(value)) throw shape()
  const keys = Object.keys(value).sort()
  if (keys.length === 1 && keys[0] === 'id') {
    if (typeof value.id !== 'string') throw shape()
    bounded(value.id, what, LIMITS.SOURCE_ACTOR)
    return
  }
  if (keys.length === 2 && keys[0] === 'key' && keys[1] === 'type') {
    if (typeof value.type !== 'string' || typeof value.key !== 'string') throw shape()
    bounded(value.type, what, LIMITS.ELEMENT_REFERENCE_KEY)
    bounded(value.key, what, LIMITS.ELEMENT_REFERENCE_KEY)
    return
  }
  throw shape()
}

function bindingName(name: string, what: string): void {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name ?? '')) {
    throw errors.invalidIdentifier(
      `${what} ${JSON.stringify(name)} must match [A-Za-z_][A-Za-z0-9_]*`,
    )
  }
}

function optional(value: string | undefined, what: string, max: number): void {
  if (value === undefined) return
  bounded(value, what, max)
}

function bounded(value: string, what: string, max: number): void {
  if (value.trim() === '') {
    throw errors.invalidRequestEnvelope(`${what} must not be empty`)
  }
  // Characters, not bytes: the schema counts characters, and counting bytes
  // would reject a legal value that happens to be non-ASCII.
  if ([...value].length > max) {
    throw errors.invalidRequestEnvelope(
      `${what} is longer than the ${max} characters §71 allows`,
    )
  }
}

/**
 * Mints the Evidence an `ingest` block declares, and binds each as `:key`.
 *
 * Returns the bindings to merge into the request's parameters. A command then
 * cites `:msg` and gets a reference to a record whose payload is what the
 * runtime observed.
 */
export function mintIngestedEvidence(
  tx: Transaction,
  ingest: IngestContext | undefined,
  parameters: JsonMap | undefined,
): JsonMap | null {
  if (ingest === undefined) return null
  const entries = checkIngest(ingest)

  const bound: JsonMap = {}
  const ingestedByKey = new Map<string, ElementId>()
  for (const entry of entries) {
    // A request parameter of the same name would make it ambiguous which value
    // the command cited, and the two cannot be reconciled: one is a
    // caller-supplied value, the other is an element this request created.
    if (parameters !== undefined && Object.hasOwn(parameters, entry.key)) {
      throw errors.invalidRequestEnvelope(
        `the ingest key ${JSON.stringify(entry.key)} is also a request ` +
          `parameter; a command citing :${entry.key} could mean either`,
      )
    }
    let payload=entry.payload ?? null, artifactSources:string[]=[]
    if (entry.payload_artifact !== undefined) {
      if(entry.media_type && entry.media_type !== 'application/json')throw errors.unsupportedCapability('governed artifacts carry canonical application/json bytes')
      const artifact=authorizedArtifact(tx.store,tx.cx.space,entry.payload_artifact,tx.authority,tx.auth)
      payload=artifact.payload;artifactSources=artifact.sources
    }

    const clientKey = entry.client_key ?? ''
    const sourceRefs = [...(entry.source_actor === undefined ? [] : [{id:formatElementId(sourceActor(tx,entry.source_actor))}]),...artifactSources.map((id)=>({id}))]
    const observedAt = entry.observed_at === undefined ? tx.cx.at : normalizeTime(entry.observed_at, 'ingest.observed_at')
    const mediaType = entry.media_type ?? (entry.payload_artifact ? 'application/json' : '')
    const facets = ingestedFacets(tx, entry.facets)
    if (clientKey !== '') {
      const stored = tx.store.byClientKey('Evidence', tx.cx.space, clientKey)
      const existingId = ingestedByKey.get(clientKey) ?? (stored ? {kind: 'Evidence' as const, seq: (stored.row as EvidenceRow).id} : undefined)
      const existing = existingId ? tx.load(existingId) : null
      if (existing !== null) {
        if (existing.kind !== 'Evidence') throw errors.internalError('ingest key resolved to a non-Evidence element')
        const old = existing.row
        if (old.evidence_class !== entry.evidence_class || old.payload_mode !== 'inline'
          || canonicalJson(old.payload_inline) !== canonicalJson(payload)
          || canonicalJson(old.source_refs) !== canonicalJson(sourceRefs)
          || old.media_type !== mediaType
          || entry.observed_at !== undefined && old.observed_at !== observedAt
          || !Object.entries(facets).every(([name,value]) => old.facets[name] !== undefined && canonicalJson(old.facets[name]) === canonicalJson(value))) {
          throw errors.clientKeyConflict('ingest client_key already names a different observation; use a distinct message/ingestion identity')
        }
        bound[entry.key] = {
          id: formatElementId({
            kind: existing.kind,
            seq: (existing.row as { id: number }).id,
          }),
        }
        continue
      }
    }

    const id = tx.mint('Evidence')
    const row: EvidenceRow = {
      ...blankEnvelope(id.seq),
      facets,
      client_key: clientKey,
      evidence_class: entry.evidence_class,
      payload_mode: 'inline',
      payload_inline: payload,
      content_ref: '',
      content_digest: entry.payload_artifact ? digest(payload) : '',
      media_type: mediaType,
      observed_at: observedAt,
      source_refs: sourceRefs,
      generated_by: '',
      status: 'active',
      corrects: [],
      corrected_by: [],
    }
    const element = { kind: 'Evidence' as const, row }
    tx.authorizeCreated(element, 'create')
    // §71.1, §29.8: an ingested `outcome` needs `record_outcome` exactly as a
    // `CREATE EVIDENCE` of that class does — the envelope is not a way around
    // the consequence channel's gate.
    if (recordsOutcome(element)) tx.authorizeCreated(element, 'record_outcome')
    tx.stageNew(id, element)
    if (clientKey !== '') ingestedByKey.set(clientKey, id)
    bound[entry.key] = { id: formatElementId(id) }
  }
  return bound
}

/**
 * Resolves and validates an entry's `facets` map exactly as `SET FACET` on
 * `CREATE EVIDENCE` would (§71.1).
 *
 * The same resolution — a local name through the environment, an exact
 * reference checked against it — and the same validation: the Facet must be
 * applicable to Evidence, its members must fit the definition, and a closed
 * Facet refuses a member it does not declare. An entry that fails takes the
 * whole request's transaction with it.
 */
function ingestedFacets(
  tx: Transaction,
  facets: Record<string, JsonMap> | undefined,
): JsonMap {
  const out: JsonMap = {}
  if (facets === undefined) return out
  for (const [name, values] of Object.entries(facets)) {
    const symbol = tx.env.resolveSymbol('Facet', name, 'write')
    const text = formatSymbolRef(symbol)
    const definition = tx.env.definitionPackage(symbol)
    const def = definition === undefined ? undefined : facetDef(definition, symbol.name)
    if (def !== undefined) {
      validateFacetCarrier(text, def, { kind: 'element', elementKind: 'Evidence' })
        .extend(validateFacet(text, def, values))
        .throwIfInvalid()
    }
    out[text] = { ...(out[text] as JsonMap | undefined), ...values }
  }
  return out
}

/**
 * Resolves an ingest entry's `source_actor` to a Concept in this Space.
 *
 * By exact id, or by Concept Type lineage plus logical key — resolved through
 * the Schema Environment the way `UPSERT CONCEPT ... MATCH {type, key}` is
 * (§54.4, §20.14). Refused rather than stored as a bare name: §71.1 records
 * the actor as Evidence *source*, and a source slot holding a string nothing
 * resolves is a citation a reader cannot follow. The actor is a semantic actor
 * and never a Principal (§88.1), so this looks among Concepts and never in the
 * control plane.
 */
function sourceActor(tx: Transaction, actor: ElementReference): ElementId {
  if ('id' in actor) {
    const id = tryParseElementId(actor.id)
    if (id !== null && tx.peek(id) !== null) return id
    throw errors.notFoundOrNotVisible(
      `the ingest source actor ${JSON.stringify(actor.id)} names no element in ` +
        `this Space; an Evidence source must resolve to something a reader can ` +
        `follow`,
    )
  }
  const symbol = tx.env.resolveSymbol('ConceptType', actor.type, 'read')
  const found = tx.store.conceptByKey(tx.cx.space, lineageOfSymbol(symbol), actor.key)
  if (found !== null) return { kind: 'Concept', seq: found.id }
  throw errors.notFoundOrNotVisible(
    `no ${actor.type} keyed ${JSON.stringify(actor.key)} exists in this Space; ` +
      `an Evidence source must resolve to something a reader can follow`,
  )
}
