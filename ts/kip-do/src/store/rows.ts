/**
 * The persisted row shapes.
 *
 * These are storage, not protocol: the KIP wire shapes are what a client
 * receives, and these carry the same information plus the columns the engine
 * needs to *find* it. JSON columns arrive here already decoded — the store
 * owns the text form, and nothing above it should be parsing a column.
 *
 * The field names match `rs/anda_cognitive_nexus/src/store/rows.rs` so the two
 * engines can be read side by side; where they differ, the difference is
 * commented at the point it happens.
 */

import { formatElementId, type ElementKind } from '../id.js'
import { isJsonMap, type Json, type JsonMap } from '../json.js'
import { parseSymbolRef } from '../schema/symbol.js'

/** The engine-level state of an element (`_system.state`, Spec §6.3). */
export const State = {
  /** Ordinary, recallable state. */
  ACTIVE: 'active',
  /** Removed from ordinary recall, still readable and referable (§41.2). */
  ARCHIVED: 'archived',
  /** Logically deleted; identity and references survive (§41.3). */
  TOMBSTONED: 'tombstoned',
  /**
   * Consolidated into another Concept, and still addressable (§11.1).
   *
   * Distinct from `archived`: both leave ordinary recall, but only this one
   * says *where the identity went*, which is what lets a reader follow
   * `merged_into` instead of concluding the Concept was retired.
   */
  MERGED: 'merged',
  /**
   * Held out of ordinary use by Governance, pending review (§39.2).
   *
   * Distinct from `archived`, and the distinction is the point (§39.2).
   * Archiving says *this is no longer in ordinary recall*; quarantine says
   * *local Governance does not currently allow ordinary use of this*. Neither
   * says the original actor took anything back.
   */
  QUARANTINED: 'quarantined',
  /**
   * Physically erased, with only an identity stub left (§19.3).
   *
   * The row survives so that references keep resolving and provenance-root
   * identity survives byte destruction; its content does not.
   */
  PURGED: 'purged',
  /**
   * Minted by an in-flight transaction and not yet committed.
   *
   * Not a KIP state. It exists because a row id is assigned at insert time, so
   * a transaction that needs an id before it can resolve a forward reference
   * has to insert something first. Nothing reads a pending element, and
   * anything still wearing this state after a crash belongs to no committed
   * transaction — which is what makes the recovery sweep correct rather than
   * heuristic.
   */
  PENDING: 'pending',
} as const

/**
 * One counter per version plane (Spec §6.3, §35.1).
 *
 * `_system.version` advances on every committed change; each of these
 * advances only when its plane changes, so a guard `OF ATTRIBUTES` is not
 * spoiled by a concurrent Facet sweep on the same element. `facets` is keyed
 * by the Facet's local symbol name — the spelling §36.1's example uses and
 * the one `?x._system.plane_versions.facets["MnemonicState"]` reads — so two
 * versions of one Facet lineage (§20.14) share one counter.
 */
export interface PlaneVersions {
  attributes: number
  structural: number
  retention: number
  facets: Record<string, number>
}

/** The counters a plane has before anything wrote to it. */
export function emptyPlanes(): PlaneVersions {
  return { attributes: 0, structural: 0, retention: 0, facets: {} }
}

/**
 * Reads a stored `plane_versions` column, filling in what an older row may
 * not carry.
 */
export function planesFromJson(value: unknown): PlaneVersions {
  const out = emptyPlanes()
  if (!isJsonMap(value)) return out
  for (const plane of ['attributes', 'structural', 'retention'] as const) {
    const n = value[plane]
    if (typeof n === 'number' && Number.isInteger(n) && n >= 0) out[plane] = n
  }
  if (isJsonMap(value.facets)) {
    for (const [name, n] of Object.entries(value.facets)) {
      if (typeof n === 'number' && Number.isInteger(n) && n >= 0) out.facets[name] = n
    }
  }
  return out
}

/**
 * A version plane, as a guard or a change entry names it.
 *
 * `facets.<Symbol>` carries the Facet's local symbol name, matching the key
 * of {@link PlaneVersions.facets}.
 */
export type PlaneKey = 'attributes' | 'structural' | 'retention' | `facets.${string}`

/** The counter one plane key selects. */
export function planeCounter(planes: PlaneVersions, key: PlaneKey): number {
  if (key === 'attributes' || key === 'structural' || key === 'retention') {
    return planes[key]
  }
  return planes.facets[key.slice('facets.'.length)] ?? 0
}

/** Advances one plane's counter, in place. */
export function bumpPlane(planes: PlaneVersions, key: PlaneKey): void {
  if (key === 'attributes' || key === 'structural' || key === 'retention') {
    planes[key] += 1
    return
  }
  const facet = key.slice('facets.'.length)
  planes.facets[facet] = (planes.facets[facet] ?? 0) + 1
}

/** The `_system` envelope every element carries. */
export interface Envelope {
  /** The row id; the element's KIP id is `<tag>-{id}`. */
  id: number
  /** The home MemorySpace (§29). */
  space: string
  state: string
  /** `_system.version` — the target of a bare `EXPECT VERSION`. */
  version: number
  /** `_system.plane_versions` — the targets of `EXPECT VERSION ... OF` (§35.1). */
  plane_versions: PlaneVersions
  /** `_system.space_seq` of the last state change; the `CHANGES` cursor. */
  seq: number
  created_at: string
  updated_at: string
  created_tx: string
  updated_tx: string
  /** `_system.origin` — what the runtime observed, never a claim (§24.2). */
  origin: JsonMap
  /** Schema-validated Facets, keyed by facet symbol (§35). */
  facets: JsonMap
  /** Profile structural fields: symbol → ordered array of references (§8.2). */
  structural: JsonMap
  /** The Governance hook (§31). */
  governance: JsonMap
  /** The storage-lifecycle hook (§33). */
  retention: JsonMap
  /**
   * `retention.expires_at`, lifted out for the retention sweep.
   *
   * Storage lifecycle only — never `valid_until` (§34).
   */
  expires_at: string
}

/**
 * The envelope a newly staged element starts from; commit fills the rest.
 *
 * Here rather than beside either caller, because there are two of them —
 * ordinary KML creation and the request envelope's Evidence ingestion — and an
 * envelope field added to one and forgotten in the other does not fail: the
 * write path throws `${table}.${column} was not set` only for a *missing* key,
 * so a second, stale spelling of this object is exactly the kind of drift that
 * lands in production as one element kind quietly missing a plane counter.
 */
export function blankEnvelope(id: number): Envelope {
  return {
    id,
    space: '',
    state: State.ACTIVE,
    version: 0,
    plane_versions: emptyPlanes(),
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
  }
}

/** A Concept — a unit of meaning (Spec §10). */
export interface ConceptRow extends Envelope {
  /**
   * The `CLIENT KEY` this element was created under, for retry-safe creation
   * (§70). Scoped to the Space, not globally unique.
   */
  client_key: string
  /** The exact Schema symbol identity this Concept is typed by (§10.3). */
  schema_ref: string
  /**
   * The lineage of `schema_ref` — `kip://<package-path>/<Symbol>`, no version
   * (§20.14). What identity and matching compare, so a package upgrade never
   * splits one type's population into two; `schema_ref` stays exact.
   */
  lineage: string
  /** The immutable Space-local logical key (§5.3). */
  key: string
  /** Mutable grounding state; duplicates are allowed, so this is not identity (§5.2). */
  name: string
  /** A high-assurance cross-system identity (§5.4). */
  canonical_id: string
  /** Alternative names — grounding state, like `name` (§10.6). */
  aliases: string[]
  /** Representation-local state with no independent epistemic lifecycle (§10.4). */
  attributes: JsonMap
  /**
   * The surviving Concept this one was merged into, empty when none.
   *
   * Merge is non-destructive: the source stays addressable and its history
   * keeps resolving, so this is a forwarding pointer rather than a delete
   * (§11.1).
   */
  merged_into: string
}

/**
 * A Proposition — a truth-neutral tuple (Spec §12).
 *
 * There is no confidence column, and its absence is the point: confidence
 * lives on the Assertions about this tuple (§12.8).
 */
export interface PropositionRow extends Envelope {
  /** The subject endpoint, always an element reference. */
  subject: JsonMap
  /** The subject's deterministic equality key. */
  subject_key: string
  /** The exact predicate symbol identity. */
  predicate_ref: string
  /** The lineage of `predicate_ref` (§20.14), which tuple identity compares. */
  predicate_lineage: string
  /** The object endpoint: an element reference or a Literal. */
  object: JsonMap
  /** The object's deterministic equality key. */
  object_key: string
  /** The tuple's structural identity within its Space (§12.5). */
  tuple_key: string
}

/** One cited Evidence record and the role it plays (Spec §13.2). */
export interface EvidenceRef {
  id: string
  role?: string
}

/**
 * An Assertion — one actor's epistemic commitment (Spec §14).
 *
 * The epistemic payload is historically immutable: a changed commitment is a
 * new Assertion plus supersession, never a rewrite (§15.1).
 */
export interface AssertionRow extends Envelope {
  client_key: string
  /** The Proposition this Assertion is about — exactly one (§93.10). */
  proposition_id: string
  /** The semantic actor whose commitment this is (§14.4). */
  asserted_by: JsonMap
  asserted_by_key: string
  /** `support`, `reject` or `uncertain` (§14.5). */
  stance: string
  /** How the claim was arrived at (§14.6). */
  mode: string
  /**
   * Epistemic support in `[0, 1]`, or `-1` when the actor stated none.
   *
   * Not memory accessibility and not trust (§2.8, §40); a negative sentinel
   * keeps "no confidence given" orderable below every real value instead of
   * being confused with `0.0`, which is a real claim of no support.
   */
  confidence: number
  /** When the actor made the claim (§36.3). */
  asserted_at: string
  /** When the claim starts applying (§36.1). */
  valid_from: string
  /** When it stops applying; empty means open-ended. */
  valid_until: string
  /** The Evidence cited, with roles. */
  evidence_refs: EvidenceRef[]
  /** The context this claim was made in. */
  context_refs: Json[]
  /**
   * The epistemic lifecycle: `active`, `retracted`, `superseded`, `expired`.
   *
   * Distinct from `state`: an Assertion can be epistemically retracted while
   * its record stays perfectly active, and archiving the record does not
   * retract the claim (§80).
   */
  status: string
  supersedes: string[]
  superseded_by: string[]
  retracted_at: string
}

/** An Evidence record — an observation (Spec §17). */
export interface EvidenceRow extends Envelope {
  client_key: string
  /** What kind of observation this is (§18). */
  evidence_class: string
  /** `inline` or `external`. */
  payload_mode: string
  /** The observed content, when carried inline (§19.1). */
  payload_inline: Json
  /** A content-addressed reference, when carried externally (§19.2). */
  content_ref: string
  /**
   * A digest of the content.
   *
   * Indexed for lookup, never for identity: equal digests do not imply
   * identical Evidence, because two independent observations of the same text
   * are two observations (§73).
   */
  content_digest: string
  media_type: string
  /** When the observation happened — not when the record was written (§36.2). */
  observed_at: string
  /** Where the observation came from. */
  source_refs: Json[]
  /** The Activity that produced it. */
  generated_by: string
  /** The correction state: `active` or `corrected`. */
  status: string
  corrects: string[]
  corrected_by: string[]
}

/**
 * An Activity — a provenance record for a process (Spec §22).
 *
 * An Activity describes a process; it is not a Transaction (§22.1).
 */
export interface ActivityRow extends Envelope {
  client_key: string
  activity_class: string
  started_at: string
  /** When it ended; terminal topology freezes with it (§22.3). */
  ended_at: string
  inputs: Json[]
  outputs: Json[]
  /** The semantic actors involved — not authenticated Principals. */
  associated_actors: Json[]
  parameters_digest: string
  /** The lifecycle state (§55). */
  status: string
}

/** Any element row, discriminated by which table it came from. */
export type ElementRow =
  | ConceptRow
  | PropositionRow
  | AssertionRow
  | EvidenceRow
  | ActivityRow

/** One loaded Cognitive Element, tagged with the kind that identifies it. */
/**
 * The `payload.mode` a purged Evidence payload reports (§60.6).
 *
 * A distinct mode rather than an empty one: "the bytes were destroyed" and
 * "this Evidence never carried bytes" are different facts, and a reader that
 * cannot tell them apart will read a data-minimization decision as a malformed
 * record.
 */
export const PAYLOAD_PURGED = 'purged'

/**
 * Clears the payload columns of one Evidence row, in place.
 *
 * Lives beside the row rather than beside the purge that calls it, because it
 * runs twice per payload purge — once on the current row, once per recorded
 * version — and the columns a payload purge may touch have to be named in
 * exactly one place. The surrounding record — digest, media type, observed
 * time, source, `generated_by`, lifecycle, Facets, structural topology — is
 * what §60.6 promises survives, and it is untouched here.
 *
 * @see rs/anda_cognitive_nexus/src/store/rows.rs
 */
export function erasePayload(row: EvidenceRow): void {
  row.payload_mode = PAYLOAD_PURGED
  row.payload_inline = null
  row.content_ref = ''
}

export type Element =
  | { kind: 'Concept'; row: ConceptRow }
  | { kind: 'Proposition'; row: PropositionRow }
  | { kind: 'Assertion'; row: AssertionRow }
  | { kind: 'Evidence'; row: EvidenceRow }
  | { kind: 'Activity'; row: ActivityRow }

/**
 * The classification label this element carries, if it carries one.
 *
 * Empty means the element states none, which is **not** `public`: the Space's
 * default applies instead (§95). Resolving that default is the authorization
 * layer's job, because only it knows which Space the read is running in.
 */
export function classificationOf(element: Element): string {
  const label = element.row.governance.classification
  return typeof label === 'string' ? label : ''
}

/**
 * The exact Schema symbol this element is typed by, where it has one.
 *
 * A Proposition's predicate and an Evidence record's class play the same role
 * for authorization — they are what a Grant scoped to a schema reference is
 * scoped to — so they answer here rather than forcing every caller to match on
 * the kind first. An Assertion is typed by the Proposition it is about, not by a
 * symbol of its own, so it answers with nothing.
 */
export function schemaRefOf(element: Element): string {
  switch (element.kind) {
    case 'Concept':
      return element.row.schema_ref
    case 'Proposition':
      return element.row.predicate_ref
    case 'Evidence':
      return element.row.evidence_class
    case 'Activity':
      return element.row.activity_class
    case 'Assertion':
      return ''
  }
}

/** The SQL table each Core kind lives in. */
export const TABLES: Readonly<Record<ElementKind, string>> = {
  Concept: 'concepts',
  Proposition: 'propositions',
  Assertion: 'assertions',
  Evidence: 'evidence',
  Activity: 'activities',
}

/** A MemorySpace — the Governance container every element belongs to (§28). */
export interface SpaceRow {
  id: number
  /** The Space's stable id, as it appears in `space_id` on every element. */
  space_id: string
  uri: string
  name: string
  description: string
  /**
   * The Principal that owns the Space — an authenticated identity, not a
   * semantic `$self` Concept.
   */
  owner_principal: string
  /**
   * Every owning Principal (Governance §20, §23).
   *
   * Ownership is Governance state. It is not derived from a semantic ownership
   * Proposition, from the Space's name, or from who wrote the most into it.
   */
  owners: string[]
  /** `active`, `suspended` or `archived`. */
  status: string
  /** The Governance Policy this Space is evaluated under; empty for none. */
  default_policy_id: string
  /**
   * The epistemic trust policy bound to this Space; empty for none.
   *
   * Kept apart from `default_policy_id` because trust and access are different
   * questions: what this Brain believes and what a caller may see are decided
   * by different state under different authority (§22.5, §22.3).
   */
  trust_policy_id: string
  /**
   * The classification an element gets when nothing else assigns one.
   *
   * Never `public` by default: §95 forbids reading an absent classification as
   * freely disclosable.
   */
  default_classification: string
  audit_mode: string
  created_at: string
  /** The Space's current sequence coordinate; every commit advances it. */
  seq: number
  schema_environment_version: number
  /**
   * The Concept this Space treats as its semantic `$self` (§5.6).
   *
   * Protected Space configuration, not cognitive content: ordinary KML has no
   * path to it, and changing it is a Governance operation. Empty means the
   * Space has designated none — which §5.6 admits, and which is the honest
   * answer when nothing has been designated rather than a guess at which
   * Person Concept "looks like" the Brain.
   */
  self_concept: string
  /** Space-local Governance settings that have no column of their own. */
  policies: JsonMap
}

/** One installed Schema Package artifact (Spec §4, §28). */
export interface SchemaPackageRow {
  id: number
  /** The canonical exact reference, e.g. `kip://core@2.0.0`. */
  package_ref: string
  package_id: string
  version: string
  /**
   * The engine's own digest over the stored artifact.
   *
   * Distinct from `declared_digest`: this one is computed here and is what
   * detects a same-version replacement (§20.11). The artifact's own digest is
   * recorded but not treated as verified.
   */
  content_digest: string
  declared_digest: string
  artifact: JsonMap
  installed_at: string
  /** Where it came from. Transport is not verification (§20.11). */
  source: string
}

/** One immutable version of a Space's Schema Environment (Spec §20.8). */
export interface SchemaEnvRow {
  id: number
  space: string
  version: number
  /** The resolved Schema Lock (§25). */
  lock: JsonMap
  created_at: string
  tx_id: string
  /**
   * The first Space coordinate this environment could have applied to (§20.9).
   *
   * The bootstrap environment first applies to the first cognitive coordinate;
   * every later activation is a Governance transaction and owns this coordinate
   * itself. In both cases, everything at an earlier coordinate resolves through
   * the older environment.
   */
  seq: number
}

/** One historical version of one element (Spec §36, §78). */
export interface ElementVersionRow {
  id: number
  space: string
  /** The element this is a version of, e.g. `C-1`. */
  element: string
  /** The element's kind tag, so a scan can narrow without parsing ids. */
  kind: string
  version: number
  /**
   * The Space sequence this version became current at.
   *
   * `AS OF SEQ s` reads the greatest version whose `seq` is at most `s`.
   */
  seq: number
  tx_id: string
  /** What the change was called: `create`, `update`, `archive`, … */
  op: string
  /** The complete row, as stored. */
  row: JsonMap
}

/** What one commit did to an element, on the wire (§36.1). */
export type ChangeOp =
  | 'create'
  | 'update'
  | 'lifecycle'
  | 'retention'
  | 'merge'
  | 'purge'
  | 'payload_purge'

/**
 * One entry of a Change Envelope, in the normative shape of
 * `schemas/kip-change-envelope.schema.json` (§36.1).
 *
 * Names and versions, never values: `touched` lists the paths that changed
 * and `planes` the counters after the commit, which is what a Watch needs to
 * decide whether a slot, an element or a type moved without reading payload.
 * The same shape `anda_kip::ChangeEntry` serializes to, member for member.
 */
export interface ChangeEntry {
  op: ChangeOp
  /** The Core kind, lowercase, as `?x.kind` spells it. */
  kind: string
  id: string
  /** The exact Concept Type reference; present for Concept entries. */
  schema_ref?: string
  /** The version before this commit, when the element existed. */
  old_version?: number
  new_version: number
  /** The stored status before and after; present for `lifecycle` entries. */
  state?: { from: string; to: string }
  /** The references that place the entry. */
  refs?: {
    /** Assertion entries: the Proposition the Assertion is about. */
    proposition?: string
    /** Proposition entries: the subject element id. */
    subject?: string
    /** Proposition entries: the exact Predicate reference. */
    predicate_ref?: string
    /** Merge entries on the source Concept: the canonical target. */
    merged_into?: string
  }
  /** The paths changed — names only, never values. */
  touched?: string[]
  /**
   * The plane counters after this commit, on entries that touched a plane.
   *
   * The wire shape, not the in-memory one: `facets` is present only when a
   * Facet counter exists, exactly as {@link planesToJson} writes it and as the
   * Rust engine serializes `PlaneVersions`. An entry that always carried an
   * empty `facets` object would differ from the other reference engine byte
   * for byte on the commonest entry there is.
   */
  planes?: WirePlaneVersions
}

/** The plane counters as the wire carries them; see {@link planesToJson}. */
export interface WirePlaneVersions {
  attributes: number
  structural: number
  retention: number
  facets?: Record<string, number>
}

/**
 * Builds one Change Envelope entry for an element as it stands after a commit.
 *
 * The references are read off the element itself, so the entry cannot name a
 * Proposition the Assertion is not about. `old_version`, `state`, `touched`
 * and `planes` are what only the transaction knows and are passed in.
 */
export function changeEntryOf(
  element: Element,
  op: ChangeOp,
  extras: {
    old_version?: number
    state?: { from: string; to: string }
    touched?: readonly string[]
    planes?: WirePlaneVersions
  } = {},
): ChangeEntry {
  const { row } = element
  const entry: ChangeEntry = {
    op,
    // Lowercase, as every other wire tag: `?c.kind` answers "concept", and a
    // change record that said "Concept" would be the one place the stream
    // spelled a Core kind differently from the elements it describes.
    kind: element.kind.toLowerCase(),
    id: formatElementId({ kind: element.kind, seq: row.id }),
    new_version: row.version,
  }
  if (extras.old_version !== undefined) entry.old_version = extras.old_version
  if (element.kind === 'Concept' && element.row.schema_ref !== '') {
    entry.schema_ref = element.row.schema_ref
  }
  if (extras.state !== undefined) entry.state = extras.state
  const refs: NonNullable<ChangeEntry['refs']> = {}
  switch (element.kind) {
    case 'Assertion':
      if (element.row.proposition_id !== '') refs.proposition = element.row.proposition_id
      break
    case 'Proposition': {
      const subject = element.row.subject.id
      if (typeof subject === 'string') refs.subject = subject
      if (element.row.predicate_ref !== '') refs.predicate_ref = element.row.predicate_ref
      break
    }
    case 'Concept':
      if (op === 'merge' && element.row.merged_into !== '') {
        refs.merged_into = element.row.merged_into
      }
      break
    default:
      break
  }
  if (Object.keys(refs).length > 0) entry.refs = refs
  if (extras.touched !== undefined && extras.touched.length > 0) {
    entry.touched = [...extras.touched]
  }
  if (extras.planes !== undefined) entry.planes = extras.planes
  return entry
}

/**
 * The plane counters as the wire carries them: `facets` only when it holds
 * something, matching `anda_kip::PlaneVersions`' serialization.
 */
export function planesToJson(planes: PlaneVersions): WirePlaneVersions & JsonMap {
  const out: WirePlaneVersions & JsonMap = {
    attributes: planes.attributes,
    structural: planes.structural,
    retention: planes.retention,
  }
  if (Object.keys(planes.facets).length > 0) out.facets = { ...planes.facets }
  return out
}

/**
 * The local symbol name a Facet or Structural Field key carries.
 *
 * Stored keys are exact symbols (`kip://…@2.0.0/MnemonicState`); the plane
 * counters and the `touched` paths name the symbol the way §36.1's example
 * does, by its local name, so two versions of one lineage share a counter.
 */
export function symbolLocalName(key: string): string {
  try {
    return parseSymbolRef(key).name
  } catch {
    return key
  }
}

/** One committed transaction (Spec §82). */
export interface TransactionRow {
  id: number
  tx_id: string
  space: string
  /** The Space sequence this commit produced. */
  seq: number
  /** The snapshot the transaction started from. */
  snapshot_seq: number
  committed_at: string
  /** `committed`, `aborted` or `no_effect`. */
  status: string
  transaction_class: string
  /** The idempotency key, empty when the caller supplied none. */
  idempotency_key: string
  request_digest: string
  semantic_plan_digest: string
  result_digest: string
  schema_environment_version: number
  /** The response this transaction produced, replayed on idempotent retry. */
  result: Json
  changes: ChangeEntry[]
}
