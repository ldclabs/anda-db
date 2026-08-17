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

import type { ElementKind } from '../id.js'
import type { Json, JsonMap } from '../json.js'

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
   * Held out of ordinary use by Governance, pending review (§133).
   *
   * Distinct from `archived`, and the distinction is the point (§134).
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

export type ElementState = (typeof State)[keyof typeof State]

/** The `_system` envelope every element carries. */
export interface Envelope {
  /** The row id; the element's KIP id is `<tag>-{id}`. */
  id: number
  /** The home MemorySpace (§29). */
  space: string
  state: string
  /** `_system.version` — the target of `EXPECT VERSION`. */
  version: number
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

/** A Concept — a unit of meaning (Spec §10). */
export interface ConceptRow extends Envelope {
  /**
   * The `CLIENT KEY` this element was created under, for retry-safe creation
   * (§70). Scoped to the Space, not globally unique.
   */
  client_key: string
  /** The exact Schema symbol identity this Concept is typed by (§10.3). */
  schema_ref: string
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
  /** The object endpoint: an element reference or a Literal. */
  object: JsonMap
  /** The object's deterministic equality key. */
  object_key: string
  /** The tuple's structural identity within its Space (§12.5). */
  tuple_key: string
  /** Representation-local state about the tuple itself (§12.9). */
  attributes: JsonMap
}

/** One cited Evidence record and the role it plays. */
export interface EvidenceRef {
  evidence_id: string
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
export type Element =
  | { kind: 'Concept'; row: ConceptRow }
  | { kind: 'Proposition'; row: PropositionRow }
  | { kind: 'Assertion'; row: AssertionRow }
  | { kind: 'Evidence'; row: EvidenceRow }
  | { kind: 'Activity'; row: ActivityRow }

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
   * by different state under different authority (§111, §116).
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
   * detects a same-version replacement (§150). The artifact's own digest is
   * recorded but not treated as verified.
   */
  content_digest: string
  declared_digest: string
  artifact: JsonMap
  installed_at: string
  /** Where it came from. Transport is not verification (§240.42). */
  source: string
}

/** One immutable version of a Space's Schema Environment (Spec §23, §143). */
export interface SchemaEnvRow {
  id: number
  space: string
  version: number
  /** The resolved Schema Lock (§25). */
  lock: JsonMap
  created_at: string
  tx_id: string
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

/** One entry of a transaction's change list. */
export interface ChangeEntry {
  id: string
  kind: string
  op: string
  version: number
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
