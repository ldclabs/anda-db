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
import { canonicalJson, isJsonMap, type Json, type JsonMap } from '../json.js'
import type { ExportCapsuleCommand } from '../kip/ast.js'
import { boundValue } from '../kml/value.js'
import { Context } from '../kql/context.js'
import { solveAll, type ReadBindings } from '../kql/matching.js'
import { referencedIds } from '../store/index.js'
import { nowTime } from '../time.js'
import { render } from '../view.js'
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
  const closure = stringOption(options, 'closure', 'referential')
  if (closure !== 'referential' && closure !== 'none') {
    throw errors.unsupportedCapability(
      `this engine writes a "referential" closure or "none"; it has no ` +
        `${JSON.stringify(closure)}`,
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

  const context = new Context(cx.store, cx.env, cx.space)
  const roots = new Set<string>()
  for (const solution of solveAll(context, command.where_clauses, [new Map()], b)) {
    for (const binding of solution.values()) {
      if (binding.kind === 'element') roots.add(formatElementId(binding.id))
    }
  }

  const ids =
    closure === 'none' ? [...roots] : expand(context, [...roots], depth)

  const records: Record<string, Json[]> = {
    concepts: [],
    propositions: [],
    assertions: [],
    evidence: [],
    activities: [],
  }
  const schemaRefs = new Set<string>()
  const bucket: Record<string, string> = {
    Concept: 'concepts',
    Proposition: 'propositions',
    Assertion: 'assertions',
    Evidence: 'evidence',
    Activity: 'activities',
  }

  for (const text of ids.sort()) {
    const element = context.load(parse(text))
    if (element === null) continue
    const view = render(element)
    collectSchemaRefs(view, schemaRefs)
    records[bucket[element.kind] as string]?.push(view as Json)
  }

  const space = cx.store.space(cx.space)
  const payload: JsonMap = {
    manifest: {
      kind: 'snapshot',
      created_at: nowTime(),
      // `roots_only` unless the closure actually ran: a Capsule claiming a
      // completeness it does not have imports as a graph the destination
      // believes is whole.
      completeness: closure === 'referential' ? 'referential_closure' : 'roots_only',
      closure: { mode: closure, provenance_depth: depth },
    },
    source: {
      space_ref: cx.space,
      snapshot_seq: space?.seq ?? 0,
      schema_environment_version: cx.env.version,
    },
    // §240.47: the exact refs travel with the records. A Capsule exporting
    // local names would arrive meaning whatever the destination happens to
    // call them.
    schema: includeSchema ? schemaDependencies(cx, schemaRefs) : [],
    records: records as unknown as Json,
    external_refs: [],
    blobs: [],
  }

  return {
    format: 'KIP-Capsule',
    format_version: '2.0-draft',
    payload,
    integrity: {
      content_digest: payloadDigest(payload),
      digest_profile:
        'engine-local canonical JSON (the KIP profile is still a draft)',
      // No proofs: this engine signs nothing, and an empty proof list is an
      // honest "unsigned" rather than a claim of provenance.
      proofs: [],
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

  const declared = integrity.content_digest
  const recomputed = payloadDigest(payload)
  if (declared !== recomputed) {
    throw errors.digestMismatch(
      `this Capsule declares the digest ${String(declared)} and its payload ` +
        `digests to ${recomputed}; it was modified after it was written, or ` +
        `written by an engine using a different canonicalization`,
    )
  }

  const proofs = Array.isArray(integrity.proofs) ? integrity.proofs : []
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
    return {
      package_ref: reference,
      // The digest this Nexus computed, not the one the artifact claims about
      // itself — a destination checking the wrong one learns nothing.
      content_digest: row?.content_digest ?? null,
      installed_here: row !== null,
    }
  }) as Json
}

function payloadDigest(payload: Json): string {
  return `sha256:${sha256Text(canonicalJson(payload))}`
}

function countRecords(payload: JsonMap): number {
  const records = payload.records
  if (!isJsonMap(records)) return 0
  return Object.values(records).reduce<number>(
    (total, list) => total + (Array.isArray(list) ? list.length : 0),
    0,
  )
}

function parseJsonArtifact(source: string): Json {
  try {
    return JSON.parse(source) as Json
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
