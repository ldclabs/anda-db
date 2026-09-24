import { validateValue } from '../schema/contracts.js'
import { parseCanonicalJson } from '@ldclabs/kip-lang'
/**
 * # Capsules
 *
 * A Capsule is a bounded, self-contained excerpt of a Space — the unit of
 * cognitive portability. The two operations here are deliberately kept apart:
 *
 * ```text
 * EXPORT   write an excerpt, with the exact schema symbols it depends on
 * VERIFY   check the artifact is what it claims to be — never that it is true
 * ```
 *
 * Import is not here. Resolving a source's identities onto the destination's,
 * rewriting every reference and staying idempotent across a retry is a write
 * path, and a half-built one hands the destination a graph with broken edges
 * and no way to notice. It is refused by name; see `DESCRIBE CAPABILITIES`.
 */

import { sha256Text } from '../digest.js'
import { errors } from '../errors.js'
import { formatElementId, type ElementId } from '../id.js'
import {
  canonicalJson,
  compareCodePoints,
  isJsonMap,
  type Json,
  type JsonMap,
} from '../json.js'
import { stripStrength } from '../projection/strength.js'
import type { ExportCapsuleCommand } from '../kip/ast.js'
import { boundValue } from '../kml/value.js'
import { Context } from '../kql/context.js'
import { solveAll, type ReadBindings } from '../kql/matching.js'
import { referencedIds } from '../store/index.js'
import type { MetaContext } from '../meta/index.js'

/** How far the provenance walk follows references out from the roots. */
const DEFAULT_DEPTH = 3

/**
 * `EXPORT CAPSULE :out WHERE { … }`.
 *
 * The `WHERE` block selects roots exactly as a query would, and the closure
 * follows their references outward. An unbounded selection is refused by the
 * grammar, which is where it belongs: an unbounded EXPORT is not a smaller
 * thing to hand somebody, it is the Brain.
 */
export function exportCapsule(
  command: ExportCapsuleCommand,
  cx: MetaContext,
  b: ReadBindings,
): Json {
  if (command.as_of !== null) {
    throw errors.unsupportedCapability(
      'EXPORT CAPSULE AS OF needs a historical read path, which this engine ' +
        'has not built',
    )
  }
  const options = evaluateOptions(command.options, cx)
  // §40.3's vocabulary, spelled the way §40.3 spells it. An engine that
  // invented its own words for the same three shapes would make a Capsule's own
  // manifest unreadable to the destination that has to decide whether to trust
  // it.
  const closure = stringOption(options, 'closure', 'referential')
  if (!['closed', 'referential', 'selective'].includes(closure)) {
    throw errors.unsupportedCapability(
      `§40.3 declares a closure as "closed", "referential" or "selective"; ` +
        `this Capsule asks for ${JSON.stringify(closure)}`,
    )
  }
  // A proof profile promises a signature, and this engine holds no signing
  // keys. Emitting an unsigned Capsule under a profile that names one would put
  // the claim in the manifest and nothing behind it (§37.8).
  if (options.proof_profile !== undefined && options.proof_profile !== null) {
    throw errors.unsupportedCapability(
      `this engine signs nothing, so it cannot produce a Capsule under the ` +
        `proof profile ${JSON.stringify(options.proof_profile)}; an exported ` +
        `Capsule is unsigned and says so`,
    )
  }
  if (options.include_blobs === true) {
    throw errors.unsupportedCapability(
      'this engine stores no blobs, so it cannot include them in a Capsule',
    )
  }
  const depth =
    typeof options.provenance_depth === 'number'
      ? options.provenance_depth
      : DEFAULT_DEPTH
  const includeSchema = options.include_schema !== false

  // The same choke point a query uses, so an export cannot root on an element
  // the caller may not read (§78: export is a further permission over what a
  // read already reached, never a way around it).
  const context = new Context(cx.store, cx.env, cx.space, cx.authority, cx.auth)
  const roots = new Set<string>()
  for (const solution of solveAll(context, command.where_clauses, [new Map()], b)) {
    for (const binding of solution.values()) {
      if (binding.kind === 'element') roots.add(formatElementId(binding.id))
    }
  }

  const ids =
    closure === 'selective' ? [...roots] : expand(context, [...roots], depth)

  const records: Record<string, Json[]> = {
    concepts: [],
    propositions: [],
    assertions: [],
    evidence: [],
    activities: [],
  }
  const schemaRefs = new Set<string>()
  const omitted: JsonMap[] = []
  const included = new Set<string>()
  const sourceControl: JsonMap = {}
  const bucket: Record<string, string> = {
    Concept: 'concepts',
    Proposition: 'propositions',
    Assertion: 'assertions',
    Evidence: 'evidence',
    Activity: 'activities',
  }

  for (const text of ids.sort()) {
    const id = parse(text)
    const element = context.load(id)
    // The *redacted* view, not a fresh render: a field mask that applied to a
    // query and not to an export would make `EXPORT CAPSULE` the way around it,
    // and export is meant to be a further permission over what a read already
    // reached (§29.2).
    const view = context.view(id)
    if (element === null || view === null) continue
    collectSchemaRefs(view, schemaRefs)
    const { canonical_subject: _subject, canonical_object: _object, ...record } = view
    // A computed member never leaves the read that evaluated it (§18.2).
    if (isJsonMap(record.facets)) {
      record.facets = structuredClone(record.facets)
      stripStrength(record)
    }
    if (isJsonMap(record.governance)) {
      const known = new Set(['classification', 'authority_class', 'policy_ref'])
      const extra = Object.fromEntries(Object.entries(record.governance).filter(([k]) => !known.has(k)))
      record.governance = Object.fromEntries(Object.entries(record.governance).filter(([k]) => known.has(k)))
      if (Object.keys(extra).length) sourceControl[text] = extra
    }
    try { validateValue({ $ref: 'urn:kip:2.0:schema:element' }, record) }
    catch {
      if (closure === 'closed') throw errors.constraintViolation('closed Capsule requires complete, visible canonical element fields')
      const origin = isJsonMap(record._system) ? record._system.origin : null
      omitted.push({ id: text, kind: !isJsonMap(record._system) || (isJsonMap(origin) && origin.redacted === true) ? 'redacted' : 'unavailable', locator: { id: text } })
      continue
    }
    included.add(text)
    records[bucket[element.kind] as string]?.push(record as Json)
  }

  const externalRefs: JsonMap[] = omitted
  const seenExternal = new Set<string>(omitted.map((r) => String(r.id)))
  for (const id of ids) {
    const element = context.load(parse(id))
    if (element === null) continue
    for (const referenced of referencedIds(element)) {
      if (included.has(referenced) || seenExternal.has(referenced)) continue
      seenExternal.add(referenced)
      externalRefs.push({
        id: referenced,
        kind: 'source_element',
        locator: { id: referenced },
      })
    }
  }
  // Code-point order, not locale order and not JavaScript's own `<`: the array
  // is part of the payload the Capsule digest covers, and an order the Rust
  // engine — which sorts by UTF-8 bytes — would not produce is a digest
  // mismatch that reads as tampering.
  externalRefs.sort((a, b2) => compareCodePoints(String(a.id), String(b2.id)))
  // A `closed` Capsule promises self-containment, so it fails rather than
  // shipping the promise with a hole in it. §40.3 names the three shapes so a
  // destination can tell them apart; one that claimed `closed` and carried
  // ExternalRefs would make the word mean nothing.
  if (closure === 'closed' && externalRefs.length > 0) {
    throw errors.constraintViolation(
      `a "closed" Capsule carries everything it references, and this export ` +
        `would leave ${externalRefs.length} reference(s) outside it — the ` +
        `first is ${String(externalRefs[0]?.id)}. Raise ` +
        `\`provenance_depth\`, widen the roots, or ask for a "referential" ` +
        `closure, which declares what it does not carry`,
    )
  }

  const space = cx.store.space(cx.space)
  const payload: JsonMap = {
    manifest: { kind: 'snapshot', closure, roots: [...roots].sort() },
    source: { space_id: cx.space, snapshot_seq: space?.seq ?? 0 },
    // §20.4: the exact refs travel with the records. A Capsule exporting
    // local names would arrive meaning whatever the destination happens to
    // call them.
    schema_dependencies: includeSchema ? schemaDependencies(cx, schemaRefs) : [],
    records: Object.values(records).flat(),
    // §40.1: what the records reference but do not carry is *declared*, not
    // dropped. A Capsule missing an edge and saying nothing imports as a graph
    // the destination believes is whole.
    external_refs: externalRefs as unknown as Json,
    blobs: {},
    handling: Object.keys(sourceControl).length ? { 'anda/source_control': sourceControl } : {},
  }

  return {
    // The frame discriminator and version are the artifact's contract, not
    // this engine's label for its own output: `anda_kip`'s `validate_frame`
    // rejects any other `format` outright, so a Capsule written under a
    // different name is one the reference engine will not open — which defeats
    // the only thing a Capsule is for. Spec §37.6 and the Capsule design doc
    // both spell it `KIP-Cognitive-Capsule` / `2.0`.
    format: 'KIP-Cognitive-Capsule',
    format_version: '2.0-draft',
    payload,
    integrity: {
      content_digest: payloadDigest(payload),
      digest_profile: 'kip-jcs-safe-v1',
      // No proofs: this engine signs nothing, and an empty proof list is an
      // honest "unsigned" rather than a claim of provenance.
      signatures: [],
    },
  } as Json
}

/**
 * Integrity, and only integrity.
 *
 * A matching digest says the artifact is intact. It says nothing about whether
 * its claims are true, and nothing about who wrote it — which is why `signed`
 * is reported separately rather than folded into `valid`.
 */
export function verifyCapsule(capsule: Json): Json {
  const artifact = typeof capsule === 'string' ? parseJsonArtifact(capsule) : capsule
  if (!isJsonMap(artifact)) {
    throw errors.artifactParseError('a Capsule must be a JSON object')
  }
  const payload = artifact.payload
  const integrity = artifact.integrity
  if (!isJsonMap(payload) || !isJsonMap(integrity)) {
    throw errors.capsuleValidationFailed(
      'a Capsule needs a `payload` and an `integrity` block',
    )
  }

  if (artifact.format !== 'KIP-Cognitive-Capsule' || artifact.format_version !== '2.0-draft' || integrity.digest_profile !== 'kip-jcs-safe-v1') throw errors.unsupportedCapability('Capsule requires format_version 2.0-draft and kip-jcs-safe-v1; older drafts need explicit migration')
  validateValue({ $ref: 'urn:kip:2.0:schema:capsule' }, artifact)
  const declared = integrity.content_digest
  checkDigestProfile(declared)
  const recomputed = payloadDigest(payload)
  if (declared !== recomputed) {
    throw errors.digestMismatch(
      `this Capsule declares the digest ${String(declared)} and its payload ` +
        `digests to ${recomputed}; it was modified after it was written`,
    )
  }

  const proofs = Array.isArray(integrity.signatures) ? integrity.signatures : []
  return {
    valid: true,
    content_digest: recomputed,
    digest_profile: integrity.digest_profile ?? null,
    // An unsigned Capsule proves nothing about who wrote it. Saying so is the
    // difference between "intact" and "trustworthy".
    signed: proofs.length > 0,
    records: countRecords(payload),
    note: 'a matching digest means the artifact is intact, not that its claims are true',
  } as Json
}

// --- helpers ----------------------------------------------------------------

function parse(text: string): ElementId {
  // Every id in this set came from `formatElementId`, so a failure here is an
  // engine bug rather than input.
  const [tag, seq] = [text.charAt(0), Number(text.slice(2))]
  const kind = { C: 'Concept', P: 'Proposition', A: 'Assertion', E: 'Evidence', X: 'Activity' }[
    tag
  ]
  if (kind === undefined) throw errors.internalError(`unreadable element id ${text}`)
  return { kind: kind as ElementId['kind'], seq }
}

/** Walks the referential closure out from the roots. */
function expand(cx: Context, roots: string[], depth: number): string[] {
  const seen = new Set(roots)
  let frontier = [...roots]
  for (let step = 0; step < depth; step++) {
    const next: string[] = []
    for (const text of frontier) {
      const element = cx.load(parse(text))
      if (element === null) continue
      for (const referenced of referencedIds(element)) {
        if (!seen.has(referenced)) {
          seen.add(referenced)
          next.push(referenced)
        }
      }
    }
    if (next.length === 0) break
    frontier = next
  }
  return [...seen]
}

/** Every exact schema symbol the exported records mention. */
function collectSchemaRefs(view: Json, into: Set<string>): void {
  if (Array.isArray(view)) {
    for (const item of view) collectSchemaRefs(item, into)
    return
  }
  if (!isJsonMap(view)) return
  for (const [key, value] of Object.entries(view)) {
    if (
      (key === 'schema_ref' || key === 'predicate_ref') &&
      typeof value === 'string'
    ) {
      into.add(value)
    }
    if (key === 'facets' || key === 'structural') {
      if (isJsonMap(value)) for (const symbol of Object.keys(value)) into.add(symbol)
    }
    collectSchemaRefs(value as Json, into)
  }
}

/** The packages those symbols come from, with the digests this Nexus computed. */
function schemaDependencies(cx: MetaContext, refs: ReadonlySet<string>): Json {
  const packages = new Set<string>()
  for (const symbol of refs) {
    const at = symbol.lastIndexOf('@')
    const slash = at === -1 ? -1 : symbol.indexOf('/', at)
    if (slash !== -1) packages.add(symbol.slice(0, slash))
  }
  return [...packages].sort().map((reference) => {
    const row = cx.store.packageByRef(reference)
    if (row === null) {
      if (reference === 'kip://core@2.0.0') return { package_ref: reference }
      throw errors.schemaPackageUnavailable(reference)
    }
    const { integrity: _integrity, ...covered } = row.artifact
    return { package_ref: reference, content_digest: 'sha256:' + sha256Text(canonicalJson(covered)) }

  }) as Json
}

/**
 * The digest algorithm a Capsule content digest uses.
 *
 * SHA-256 over the native frame excluding integrity, matching
 * `rs/anda_cognitive_nexus::capsule::DIGEST_PROFILE`. A Capsule is the one
 * artifact that leaves this engine and is checked by another, so the algorithm
 * is part of the interoperability contract rather than an engine choice: two
 * implementations hashing the same canonical bytes differently produce
 * different digests for the same cognition, and every cross-engine `VERIFY
 * CAPSULE` then fails for a reason neither side can see.
 *
 * The engine-local digests — Proposition tuple identity, Schema Package
 * content, purge stubs, approval subjects — stay on SHA-256. None of them
 * crosses an engine boundary, and changing them would rewrite every stored
 * `tuple_key`.
 */
export const DIGEST_PROFILE = 'sha256'

function payloadDigest(payload: Json): string {
  return `${DIGEST_PROFILE}:${sha256Text(canonicalJson({ format: 'KIP-Cognitive-Capsule', format_version: '2.0-draft', payload }))}`
}

/**
 * Refuses a Capsule digested under an algorithm this engine cannot compute.
 *
 * Reported as an unsupported profile rather than as a digest mismatch, and the
 * difference matters: a mismatch says *this artifact was modified*, which is an
 * accusation. An artifact written by an engine that hashes its canonical bytes
 * differently is intact and unreadable here, and telling an operator it was
 * tampered with would send them hunting for an attacker that does not exist
 * (§86.4).
 */
function checkDigestProfile(declared: unknown): void {
  if (typeof declared !== 'string' || !declared.includes(':')) {
    throw errors.capsuleValidationFailed(
      `this Capsule's content digest ${JSON.stringify(declared)} names no ` +
        `algorithm; a digest whose profile is unstated cannot be checked`,
    )
  }
  const profile = declared.slice(0, declared.indexOf(':'))
  if (profile !== DIGEST_PROFILE) {
    throw errors.unsupportedCapability(
      `this Capsule is digested under ${JSON.stringify(profile)} and this ` +
        `engine computes ${JSON.stringify(DIGEST_PROFILE)} over RFC 8785 ` +
        `canonical JSON; it cannot check the artifact's integrity, which is ` +
        `not the same as finding it corrupt`,
    )
  }
}

function countRecords(payload: JsonMap): number {
  const records = payload.records
  return Array.isArray(records) ? records.length : 0
}

function parseJsonArtifact(source: string): Json {
  try {
    return parseCanonicalJson(source) as Json
  } catch (err) {
    throw errors.artifactParseError(
      `this is not a readable Capsule artifact: ${String(err)}`,
    )
  }
}

function evaluateOptions(
  options: ExportCapsuleCommand['options'],
  cx: MetaContext,
): JsonMap {
  if (options === null) return {}
  const b = {
    tx: null as never,
    request: cx.request ?? {},
    operation: cx.operation ?? {},
  }
  return Object.fromEntries(
    Object.entries(options).map(([key, value]) => [key, boundValue(b, value)]),
  )
}

function stringOption(options: JsonMap, name: string, fallback: string): string {
  const value = options[name]
  return typeof value === 'string' ? value : fallback
}

/**
 * Reports what a Capsule artifact contains, without importing it (§63.3).
 *
 * Inspection rather than verification: this is the manifest, the source
 * identity, the schema it was written against and how much of each kind it
 * carries. `VERIFY CAPSULE` is what checks the digest, and the two are kept
 * apart on purpose — describing an artifact must not read as vouching for it.
 *
 * It answers from the parsed artifact alone. Nothing here touches the Space, so
 * an operator can look at a Capsule before deciding whether this Brain should
 * see it at all.
 */
export function describeCapsule(source: string): Json {
  const artifact = parseJsonArtifact(source)
  if (!isJsonMap(artifact)) {
    throw errors.artifactParseError('a Capsule must be a JSON object')
  }
  const payload = isJsonMap(artifact.payload) ? artifact.payload : {}
  const integrity = isJsonMap(artifact.integrity) ? artifact.integrity : {}
  const records = isJsonMap(payload.records) ? payload.records : {}
  const proofs = Array.isArray(integrity.signatures) ? integrity.signatures : []
  const externalRefs = Array.isArray(payload.external_refs)
    ? payload.external_refs.length
    : 0
  return {
    format: artifact.format ?? null,
    manifest: payload.manifest ?? null,
    source: payload.source ?? null,
    schema: payload.schema ?? null,
    counts: Object.fromEntries(
      Object.entries(records).map(([kind, list]) => [
        kind,
        Array.isArray(list) ? list.length : 0,
      ]),
    ),
    external_refs: externalRefs,
    blobs: Array.isArray(payload.blobs) ? payload.blobs.length : 0,
    integrity: {
      content_digest: integrity.content_digest ?? null,
      // Stated separately from the digest, because they answer different
      // questions: the digest says the bytes are intact, a signature would say
      // who stood behind them, and neither says the claims are true (§37.8).
      signed: proofs.length > 0,
    },
    note:
      'this describes the artifact; VERIFY CAPSULE checks its digest, and ' +
      'neither makes its claims true',
  } as Json
}
