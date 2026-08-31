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
import { formatElementId, tryParseElementId } from '../id.js'
import type { Json, JsonMap } from '../json.js'
import { State, type EvidenceRow } from '../store/index.js'
import { normalizeTime } from '../time.js'
import type { Transaction } from '../tx.js'

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
   * Never a Principal (§88.1): who authenticated is engine origin, and who
   * said it is cognition.
   */
  source_actor?: string
  /** A retry-safe logical identity for the minted Evidence. */
  client_key?: string
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
  optional(entry.observed_at, 'ingest observed_at', LIMITS.SHORT_LABEL)
  optional(entry.source_actor, 'ingest source_actor', LIMITS.SOURCE_ACTOR)
  optional(entry.client_key, 'ingest client_key', LIMITS.CLIENT_KEY)
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
    // An artifact handle promises bytes this engine has nowhere to fetch from.
    // Minting an Evidence record with an empty payload under a handle that
    // resolves to nothing would be the fabrication the whole mechanism exists
    // to prevent (§85.2).
    if (entry.payload_artifact !== undefined) {
      throw errors.unsupportedCapability(
        'this engine has no artifact store, so `payload_artifact` names bytes ' +
          'it cannot read; send the observation as an inline `payload`',
      )
    }

    // A retry of the same logical ingestion resolves to the Evidence the first
    // attempt minted, exactly as `CLIENT KEY` does on a `CREATE` (§52.1) —
    // which is what makes re-sending a lost request safe.
    const clientKey = entry.client_key ?? ''
    if (clientKey !== '') {
      const existing = tx.store.byClientKey('Evidence', tx.cx.space, clientKey)
      if (existing !== null) {
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
      id: id.seq,
      space: '',
      state: State.ACTIVE,
      version: 0,
      seq: 0,
      created_at: '',
      updated_at: '',
      created_tx: '',
      updated_tx: '',
      origin: {},
      facets: {},
      structural: {},
      governance: {},
      retention: {},
      expires_at: '',
      client_key: clientKey,
      evidence_class: entry.evidence_class,
      payload_mode: 'inline',
      payload_inline: entry.payload ?? null,
      content_ref: '',
      content_digest: '',
      media_type: entry.media_type ?? '',
      observed_at:
        entry.observed_at === undefined
          ? tx.cx.at
          : normalizeTime(entry.observed_at, 'ingest.observed_at'),
      source_refs:
        entry.source_actor === undefined
          ? []
          : [sourceActor(tx, entry.source_actor)],
      generated_by: '',
      status: 'active',
      corrects: [],
      corrected_by: [],
    }
    const element = { kind: 'Evidence' as const, row }
    tx.authorizeCreated(element, 'create')
    tx.stageNew(id, element)
    bound[entry.key] = { id: formatElementId(id) }
  }
  return bound
}

/**
 * Resolves an ingest entry's `source_actor` to a reference in this Space.
 *
 * Refused rather than stored as a bare name. §71.1 records the actor as
 * Evidence *source*, and a source slot holding a string nothing resolves is a
 * citation a reader cannot follow — indistinguishable, later, from one that was
 * checked. The actor is a semantic actor and never a Principal (§88.1), so this
 * looks it up among Concepts and never in the control plane.
 */
function sourceActor(tx: Transaction, actor: string): Json {
  const asId = tryParseElementId(actor)
  if (asId !== null && tx.store.load(asId) !== null) {
    return { id: formatElementId(asId) }
  }
  const row = tx.store.sql
    .exec<{ id: number }>(
      // Lowest id wins, deterministically: the unique index makes a second one
      // impossible going forward, and a retry that resolved differently each
      // time would be worse than not resolving at all.
      `SELECT id FROM concepts WHERE space = ? AND canonical_id = ?
         ORDER BY id LIMIT 1`,
      tx.cx.space,
      actor,
    )
    .toArray()[0]
  if (row !== undefined) {
    return { id: formatElementId({ kind: 'Concept', seq: row.id }) }
  }
  throw errors.notFoundOrNotVisible(
    `the ingest source actor ${JSON.stringify(actor)} names no Concept in ` +
      `this Space; an Evidence source must resolve to something a reader can ` +
      `follow`,
  )
}
