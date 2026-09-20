//! # The persisted row shapes
//!
//! One row struct per Core element kind (Spec §6.1). They are storage, not
//! protocol: [`anda_kip::Concept`] and friends are the wire shape, and these
//! carry the same information plus the columns the engine needs to *find* it.
//!
//! Three decisions are worth stating once, because every row repeats them.
//!
//! **The envelope is repeated, not shared.** `space`, `state`, `version` and
//! the `_system` columns appear in each struct instead of a nested block. A
//! B-Tree index is built over a named column, so a nested envelope would put
//! Space and lifecycle-state predicates behind a JSON path.
//!
//! **Every reference gets a key column beside its JSON.** The JSON is the
//! record; the key is [`Endpoint::key`](crate::term::Endpoint::key), the
//! deterministic string used for reference equality. Only the columns used by
//! indexed lookups need B-Tree indexes; other keys are read with their rows.
//!
//! **Absence is the empty string, not `Option`.** Optional text columns use
//! `""` as their storage sentinel. The view omits absent values, and the
//! store's sparse index hook omits these sentinels from B-Tree postings.
//! Timestamp range scans therefore only visit actual normalized timestamps.

use anda_db_schema::{AndaDBSchema, Json, Map};
use serde::{Deserialize, Serialize};

/// The engine-level state of an element (`_system.state`, Spec §6.3).
pub mod state {
    /// Ordinary, recallable state.
    pub const ACTIVE: &str = "active";
    /// Removed from ordinary recall, still readable and referable (§41.2).
    pub const ARCHIVED: &str = "archived";
    /// Logically deleted; identity and references survive (§41.3).
    pub const TOMBSTONED: &str = "tombstoned";
    /// Consolidated into another Concept, and still addressable (§11.1).
    ///
    /// Distinct from `archived`: both leave ordinary recall, but only this one
    /// says *where the identity went*, which is what lets a reader follow
    /// `merged_into` instead of concluding the Concept was retired.
    pub const MERGED: &str = "merged";
    /// Held out of ordinary use by Governance, pending review (§39.2).
    ///
    /// Distinct from `archived`, and the distinction is the point (§39.2).
    /// Archiving says *this is no longer in ordinary recall*; quarantine says
    /// *local Governance does not currently allow ordinary use of this*. Both
    /// leave the element readable to an authorized reviewer, and neither says
    /// the original actor took anything back.
    pub const QUARANTINED: &str = "quarantined";
    /// Physically erased, with only an identity stub left (§19.3).
    ///
    /// The row survives so that references keep resolving and provenance-root
    /// identity survives byte destruction; its content does not.
    pub const PURGED: &str = "purged";
    /// Minted by an in-flight transaction and not yet committed.
    ///
    /// Not a KIP state: it exists because `anda_db` assigns element ids at
    /// insert time, so a transaction that needs an id before it can resolve a
    /// forward reference has to insert something first. Nothing reads a pending
    /// element, and anything still wearing this state after a crash belongs to
    /// no committed transaction, which is what makes the recovery sweep
    /// correct rather than heuristic.
    pub const PENDING: &str = "pending";
}

/// A Concept — a unit of meaning (Spec §10).
#[derive(Clone, Debug, Default, Deserialize, Serialize, AndaDBSchema)]
pub struct ConceptRow {
    /// The row id; the element's KIP id is `C-{_id}`.
    pub _id: u64,
    /// The home MemorySpace (§29).
    pub space: String,
    /// `_system.state`.
    pub state: String,
    /// `_system.version` — the target of `EXPECT VERSION`.
    pub version: u64,
    /// `_system.plane_versions` — one counter per version plane (§6.3), the
    /// target of `EXPECT VERSION ... OF`. Stored as the wire shape of
    /// [`anda_kip::PlaneVersions`]; `null` decodes as every counter at zero.
    pub plane_versions: Json,
    /// `_system.space_seq` of the last state change; the `CHANGES` cursor.
    pub seq: u64,
    /// `_system.created_at`.
    pub created_at: String,
    /// `_system.updated_at`.
    pub updated_at: String,
    /// `_system.created_tx`.
    pub created_tx: String,
    /// `_system.updated_tx`.
    pub updated_tx: String,
    /// `_system.origin` — what the runtime observed, never a claim (§24.2).
    pub origin: Json,
    /// The `CLIENT KEY` this element was created under, for retry-safe
    /// creation (§70). Scoped to the Space, not globally unique.
    pub client_key: String,
    /// The exact Schema symbol identity this Concept is typed by (§10.3).
    pub schema_ref: String,
    /// The immutable Space-local logical key (§5.3).
    pub key: String,
    /// Mutable grounding state; duplicates are allowed, so this is not
    /// identity (§5.2).
    pub name: String,
    /// A high-assurance cross-system identity (§5.4).
    pub canonical_id: String,
    /// Alternative names — grounding state, like `name` (§10.6).
    pub aliases: Vec<String>,
    /// Representation-local state with no independent epistemic lifecycle
    /// (§10.4).
    pub attributes: Map<String, Json>,
    /// Schema-validated Facets, keyed by facet symbol (§35).
    pub facets: Map<String, Json>,
    /// Profile structural fields: symbol → ordered array of references (§8.2).
    pub structural: Map<String, Json>,
    /// The Governance hook (§31).
    pub governance: Json,
    /// The storage-lifecycle hook (§33).
    pub retention: Json,
    /// `retention.expires_at`, lifted out for the retention sweep. Storage
    /// lifecycle only — never `valid_until` (§34).
    pub expires_at: String,
    /// The surviving Concept this one was merged into, empty when none.
    ///
    /// Merge is non-destructive: the source stays addressable and its history
    /// keeps resolving, so this is a forwarding pointer rather than a delete
    /// (§11.1).
    pub merged_into: String,
}

/// A Proposition — a truth-neutral tuple (Spec §12).
///
/// There is no confidence column, and its absence is the point: confidence
/// lives on the Assertions about this tuple (§12.8).
#[derive(Clone, Debug, Default, Deserialize, Serialize, AndaDBSchema)]
pub struct PropositionRow {
    /// The row id; the element's KIP id is `P-{_id}`.
    pub _id: u64,
    /// The home MemorySpace.
    pub space: String,
    /// `_system.state`.
    pub state: String,
    /// `_system.version`.
    pub version: u64,
    /// `_system.plane_versions` (§6.3); see [`ConceptRow::plane_versions`].
    pub plane_versions: Json,
    /// `_system.space_seq` of the last state change.
    pub seq: u64,
    /// `_system.created_at`.
    pub created_at: String,
    /// `_system.updated_at`.
    pub updated_at: String,
    /// `_system.created_tx`.
    pub created_tx: String,
    /// `_system.updated_tx`.
    pub updated_tx: String,
    /// `_system.origin`.
    pub origin: Json,
    /// The subject endpoint, always an element reference.
    pub subject: Json,
    /// The subject's deterministic equality key.
    pub subject_key: String,
    /// The exact predicate symbol identity.
    pub predicate_ref: String,
    /// The object endpoint: an element reference or a Literal.
    pub object: Json,
    /// The object's deterministic equality key.
    pub object_key: String,
    /// The tuple's structural identity within its Space (§12.5).
    ///
    /// Unique, which is how one Space keeps one canonical Proposition per
    /// semantic tuple (§93.6) — the constraint `ENSURE PROPOSITION` resolves
    /// against instead of racing two writers into a duplicate.
    #[unique]
    pub tuple_key: String,
    /// Schema-validated Facets.
    pub facets: Map<String, Json>,
    /// Profile structural fields.
    pub structural: Map<String, Json>,
    /// The Governance hook. Proposition *existence* is sensitive data, so this
    /// is not a formality (§32).
    pub governance: Json,
    /// The storage-lifecycle hook.
    pub retention: Json,
    /// `retention.expires_at`.
    pub expires_at: String,
}

/// An Assertion — one actor's epistemic commitment (Spec §14).
///
/// The epistemic payload is historically immutable: a changed commitment is a
/// new Assertion plus supersession, never a rewrite (§15.1). Only the
/// lifecycle columns move.
#[derive(Clone, Debug, Default, Deserialize, Serialize, AndaDBSchema)]
pub struct AssertionRow {
    /// The row id; the element's KIP id is `A-{_id}`.
    pub _id: u64,
    /// The home MemorySpace.
    pub space: String,
    /// `_system.state`.
    pub state: String,
    /// `_system.version`.
    pub version: u64,
    /// `_system.plane_versions` (§6.3); see [`ConceptRow::plane_versions`].
    pub plane_versions: Json,
    /// `_system.space_seq` of the last state change.
    pub seq: u64,
    /// `_system.created_at`.
    pub created_at: String,
    /// `_system.updated_at`.
    pub updated_at: String,
    /// `_system.created_tx`.
    pub created_tx: String,
    /// `_system.updated_tx`.
    pub updated_tx: String,
    /// `_system.origin`.
    pub origin: Json,
    /// The `CLIENT KEY` this Assertion was created under.
    ///
    /// Unlike a Proposition, an Assertion genuinely needs one: asserting the
    /// same thing twice is a *repetition*, not a duplicate, so the engine
    /// cannot deduplicate it structurally (§72).
    pub client_key: String,
    /// The Proposition this Assertion is about — exactly one (§93.10).
    pub proposition_id: String,
    /// The semantic actor whose commitment this is (§14.4).
    pub asserted_by: Json,
    /// The assertor's deterministic equality key.
    pub asserted_by_key: String,
    /// `support`, `reject` or `uncertain` (§14.5).
    pub stance: String,
    /// How the claim was arrived at (§14.6).
    pub mode: String,
    /// Epistemic support in `[0, 1]`, or `-1` when the actor stated none.
    ///
    /// Not memory accessibility and not trust (§2.8, §40); a negative sentinel
    /// keeps "no confidence given" orderable below every real value instead of
    /// being confused with `0.0`, which is a real claim of no support.
    pub confidence: f64,
    /// When the actor made the claim (§36.3).
    pub asserted_at: String,
    /// When the claim starts applying (§36.1).
    pub valid_from: String,
    /// When it stops applying; empty means open-ended.
    pub valid_until: String,
    /// The Evidence cited, with roles: `[{evidence_id, role}]`.
    pub evidence_refs: Vec<Json>,
    /// The cited Evidence ids alone, for projection and reference checks.
    pub evidence_ids: Vec<String>,
    /// The context this claim was made in.
    pub context_refs: Vec<Json>,
    /// The lifecycle state: `active`, `retracted`, `superseded`, `expired`.
    ///
    /// Distinct from `state`: an Assertion can be epistemically retracted
    /// while its record stays perfectly active, and archiving the record does
    /// not retract the claim (§80).
    pub status: String,
    /// Assertions this one replaces.
    pub supersedes: Vec<String>,
    /// Assertions that replaced this one.
    pub superseded_by: Vec<String>,
    /// When the assertor withdrew it.
    pub retracted_at: String,
    /// Schema-validated Facets.
    pub facets: Map<String, Json>,
    /// Profile structural fields.
    pub structural: Map<String, Json>,
    /// The Governance hook.
    pub governance: Json,
    /// The storage-lifecycle hook.
    pub retention: Json,
    /// `retention.expires_at`.
    pub expires_at: String,
}

/// An Evidence record — an observation (Spec §17).
#[derive(Clone, Debug, Default, Deserialize, Serialize, AndaDBSchema)]
pub struct EvidenceRow {
    /// The row id; the element's KIP id is `E-{_id}`.
    pub _id: u64,
    /// The home MemorySpace.
    pub space: String,
    /// `_system.state`.
    pub state: String,
    /// `_system.version`.
    pub version: u64,
    /// `_system.plane_versions` (§6.3); see [`ConceptRow::plane_versions`].
    pub plane_versions: Json,
    /// `_system.space_seq` of the last state change.
    pub seq: u64,
    /// `_system.created_at`.
    pub created_at: String,
    /// `_system.updated_at`.
    pub updated_at: String,
    /// `_system.created_tx`.
    pub created_tx: String,
    /// `_system.updated_tx`.
    pub updated_tx: String,
    /// `_system.origin`.
    pub origin: Json,
    /// The `CLIENT KEY` this Evidence was created under.
    pub client_key: String,
    /// What kind of observation this is (§18).
    pub evidence_class: String,
    /// `inline` or `external`.
    pub payload_mode: String,
    /// The observed content, when carried inline (§19.1).
    pub payload_inline: Json,
    /// A content-addressed reference, when carried externally (§19.2).
    pub content_ref: String,
    /// A digest of the content.
    ///
    /// Indexed for lookup, never for identity: equal digests do not imply
    /// identical Evidence, because two independent observations of the same
    /// text are two observations (§73).
    pub content_digest: String,
    /// The payload's media type.
    pub media_type: String,
    /// When the observation happened — not when the record was written (§36.2).
    pub observed_at: String,
    /// Where the observation came from.
    pub source_refs: Vec<Json>,
    /// The source references' equality keys.
    pub source_keys: Vec<String>,
    /// The Activity that produced it.
    pub generated_by: String,
    /// The correction state: `active` or `corrected`.
    pub status: String,
    /// Evidence this record corrects.
    pub corrects: Vec<String>,
    /// Evidence that corrected this record.
    pub corrected_by: Vec<String>,
    /// Schema-validated Facets.
    pub facets: Map<String, Json>,
    /// Profile structural fields.
    pub structural: Map<String, Json>,
    /// The Governance hook.
    pub governance: Json,
    /// The storage-lifecycle hook. Evidence deletion is audit-sensitive (§43).
    pub retention: Json,
    /// `retention.expires_at`.
    pub expires_at: String,
}

/// An Activity — a provenance record for a process (Spec §22).
///
/// An Activity describes a process; it is not a Transaction (§22.1).
#[derive(Clone, Debug, Default, Deserialize, Serialize, AndaDBSchema)]
pub struct ActivityRow {
    /// The row id; the element's KIP id is `X-{_id}`.
    pub _id: u64,
    /// The home MemorySpace.
    pub space: String,
    /// `_system.state`.
    pub state: String,
    /// `_system.version`.
    pub version: u64,
    /// `_system.plane_versions` (§6.3); see [`ConceptRow::plane_versions`].
    pub plane_versions: Json,
    /// `_system.space_seq` of the last state change.
    pub seq: u64,
    /// `_system.created_at`.
    pub created_at: String,
    /// `_system.updated_at`.
    pub updated_at: String,
    /// `_system.created_tx`.
    pub created_tx: String,
    /// `_system.updated_tx`.
    pub updated_tx: String,
    /// `_system.origin`.
    pub origin: Json,
    /// The `CLIENT KEY` this Activity was created under.
    pub client_key: String,
    /// What kind of process this was.
    pub activity_class: String,
    /// When it started.
    pub started_at: String,
    /// When it ended; terminal topology freezes with it (§22.3).
    pub ended_at: String,
    /// What it consumed.
    pub inputs: Vec<Json>,
    /// The inputs' equality keys.
    pub input_keys: Vec<String>,
    /// What it produced.
    pub outputs: Vec<Json>,
    /// The outputs' equality keys.
    pub output_keys: Vec<String>,
    /// The semantic actors involved — not authenticated Principals.
    pub associated_actors: Vec<Json>,
    /// A digest of the parameters it ran with.
    pub parameters_digest: String,
    /// The lifecycle state (§55).
    pub status: String,
    /// Schema-validated Facets.
    pub facets: Map<String, Json>,
    /// Profile structural fields.
    pub structural: Map<String, Json>,
    /// The Governance hook.
    pub governance: Json,
    /// The storage-lifecycle hook.
    pub retention: Json,
    /// `retention.expires_at`.
    pub expires_at: String,
}

/// A MemorySpace — the Governance container every element belongs to (§28).
///
/// A Space is not a Domain: semantic organization does not confer ownership,
/// and migrating a 1.x Domain into a Space is exactly the mistake §30 names.
#[derive(Clone, Debug, Default, Deserialize, Serialize, AndaDBSchema)]
pub struct SpaceRow {
    /// The row id.
    pub _id: u64,
    /// The Space's stable id, as it appears in `space_id` on every element.
    #[unique]
    pub space_id: String,
    /// A resolvable URI for the Space, when it has one.
    pub uri: String,
    /// A human-readable label.
    pub name: String,
    /// What this Space is for.
    pub description: String,
    /// The Principal that owns the Space — an authenticated identity, not a
    /// semantic `$self` Concept (§6 of the migration guide).
    ///
    /// The first owner. [`owners`](Self::owners) is the full set; this column
    /// stays because it is what every existing row carries and because a Space
    /// with no owner at all is a Space nobody can administer.
    pub owner_principal: String,
    /// Every owning Principal (Governance §20, §23).
    ///
    /// Ownership is Governance state. It is not derived from a semantic
    /// ownership Proposition, from the Space's name, or from who happened to
    /// write the most into it.
    pub owners: Vec<String>,
    /// `active`, `suspended` or `archived`.
    pub status: String,
    /// The Governance Policy this Space is evaluated under; empty for none.
    pub default_policy_id: String,
    /// The epistemic trust policy bound to this Space; empty for none.
    ///
    /// Kept apart from `default_policy_id` because trust and access are
    /// different questions: what this Brain believes and what a caller may see
    /// are decided by different state under different authority (§22.5, §22.3).
    pub trust_policy_id: String,
    /// The classification an element gets when nothing else assigns one.
    ///
    /// Never `public` by default: §95 forbids reading an absent classification
    /// as freely disclosable.
    pub default_classification: String,
    /// How much of what happens here is written to the Governance audit.
    pub audit_mode: String,
    /// When the Space was created.
    pub created_at: String,
    /// The Space's current sequence coordinate; every commit advances it.
    pub seq: u64,
    /// The active Schema Environment version.
    pub schema_environment_version: u64,
    /// The Concept this Space treats as its semantic `$self` (§5.6).
    ///
    /// Protected Space configuration, not cognitive content: ordinary KML has
    /// no path to it, and changing it is a Governance operation. Empty means
    /// the Space has designated none — which §5.6 admits, and which is the
    /// honest answer when nothing has been designated rather than a guess at
    /// which Person Concept "looks like" the Brain.
    pub self_concept: String,
    /// Space-local Governance settings that have no column of their own.
    pub policies: Json,
}

/// An immutable version of a protected control value. Erasure replaces content
/// with a tombstone; it never restores an earlier retained version.
#[derive(Clone, Debug, Serialize, Deserialize, AndaDBSchema)]
pub struct ControlRecordRow {
    pub _id: u64,
    #[unique]
    pub record_id: String,
    pub space: String,
    pub key: String,
    pub seq: u64,
    pub version: u64,
    pub kind: String,
    pub value: Json,
    pub origin: Json,
}

/// Durable redo intent, removed only after all of its effects are flushed.
#[derive(Clone, Debug, Serialize, Deserialize, AndaDBSchema)]
pub struct CommitLogRow {
    pub _id: u64,
    #[unique]
    pub tx_id: String,
    pub plan: Json,
}

/// One installed Schema Package artifact (Spec §4, §28).
///
/// Immutable: `package_id + version` identifies one canonical content forever,
/// and the same reference arriving with different content is an integrity
/// error rather than an update (§20.4). Installation is also not
/// activation — an installed package takes no part in resolution until
/// Governance says so (§20.12).
#[derive(Clone, Debug, Default, Deserialize, Serialize, AndaDBSchema)]
pub struct SchemaPackageRow {
    /// The row id.
    pub _id: u64,
    /// The canonical exact reference, e.g. `kip://core@2.0.0`.
    #[unique]
    pub package_ref: String,
    /// The stable namespace-qualified name.
    pub package_id: String,
    /// The exact version.
    pub version: String,
    /// The engine's own digest over the stored artifact.
    ///
    /// Distinct from `declared_digest`: this one is computed here and is what
    /// detects a same-version replacement (§20.11). The artifact's own digest is
    /// recorded but not treated as verified.
    pub content_digest: String,
    /// The digest the artifact claims for itself, verbatim.
    pub declared_digest: String,
    /// The artifact itself.
    pub artifact: Json,
    /// When this Nexus installed it.
    pub installed_at: String,
    /// Where it came from. Transport is not verification (§20.11).
    pub source: String,
}

/// One immutable version of a Space's Schema Environment (Spec §20.8).
///
/// Appended, never updated: a transaction records which environment version it
/// ran under (§20.9), and rewriting an environment in place would retroactively
/// change what those transactions meant.
#[derive(Clone, Debug, Default, Deserialize, Serialize, AndaDBSchema)]
pub struct SchemaEnvRow {
    /// The row id.
    pub _id: u64,
    /// The Space this environment governs.
    pub space: String,
    /// The environment version; monotonic per Space.
    pub version: u64,
    /// The resolved Schema Lock (§25).
    pub lock: Json,
    /// When this version was activated.
    pub created_at: String,
    /// The transaction that activated it.
    pub tx_id: String,
}

/// One historical version of one element (Spec §36, §78).
///
/// Rows are updated in place, so the current row is all a reader would have if
/// this log did not exist — and `AS OF` would have nothing to read. Each commit
/// appends the complete row it wrote, so a past coordinate can be reconstructed
/// rather than guessed at from a change list.
///
/// The whole row is stored, not a diff: a diff chain has to be replayed from
/// the beginning to answer one question, and a chain with one missing link
/// answers it wrongly instead of refusing.
#[derive(Clone, Debug, Default, Deserialize, Serialize, AndaDBSchema)]
pub struct ElementVersionRow {
    /// The row id.
    pub _id: u64,
    /// The Space the element lives in.
    pub space: String,
    /// The element this is a version of, e.g. `C-1`.
    pub element: String,
    /// The element's kind tag, so a scan can narrow without parsing ids.
    pub kind: String,
    /// The element's version at this coordinate.
    pub version: u64,
    /// The Space sequence this version became current at. `AS OF SEQ s` reads
    /// the greatest version whose `seq` is at most `s`.
    pub seq: u64,
    /// The transaction that wrote it.
    pub tx_id: String,
    /// What the change was called: `create`, `update`, `archive`, …
    pub op: String,
    /// The complete row, as stored.
    pub row: Json,
}

/// The `payload.mode` a purged Evidence payload reports (§60.6).
///
/// A distinct mode rather than an empty one: "the bytes were destroyed" and
/// "this Evidence never carried bytes" are different facts, and a reader that
/// cannot tell them apart will read a data-minimization decision as a
/// malformed record.
pub const PAYLOAD_PURGED: &str = "purged";

/// Clears the payload columns of one Evidence row, in place.
///
/// Lives beside the row rather than beside the purge that calls it, because it
/// runs twice per payload purge — once on the current row, once per recorded
/// version — and the columns a payload purge may touch have to be named in
/// exactly one place. The surrounding record — digest, media type, observed
/// time, source, `generated_by`, lifecycle, Facets, structural topology — is
/// what §60.6 promises survives, and it is untouched here.
pub fn erase_payload(row: &mut EvidenceRow) {
    row.payload_mode = PAYLOAD_PURGED.to_string();
    row.payload_inline = Json::Null;
    row.content_ref = String::new();
}

/// One committed transaction (Spec §82).
///
/// The journal is what makes `HISTORY`, `CHANGES` and idempotent recovery
/// answerable: a caller that lost a response looks the transaction up by its
/// key rather than writing again (§80.4).
#[derive(Clone, Debug, Default, Deserialize, Serialize, AndaDBSchema)]
pub struct TransactionRow {
    /// The row id.
    pub _id: u64,
    /// The engine-assigned transaction id.
    #[unique]
    pub tx_id: String,
    /// The Space that committed.
    pub space: String,
    /// The Space sequence this commit produced.
    pub seq: u64,
    /// The snapshot the transaction started from.
    pub snapshot_seq: u64,
    /// When it committed.
    pub committed_at: String,
    /// `committed`, `aborted` or `no_effect`.
    pub status: String,
    /// The transaction class, e.g. `cognitive`.
    pub transaction_class: String,
    /// The idempotency key, empty when the caller supplied none.
    ///
    /// Lookup intersects this sparse index with `space`, so two Spaces may
    /// reuse a key without colliding.
    pub idempotency_key: String,
    /// A digest of the request that produced it.
    pub request_digest: String,
    /// A digest of the semantic plan that was executed.
    pub semantic_plan_digest: String,
    /// A digest of the result.
    pub result_digest: String,
    /// The Schema Environment version the commit ran under.
    pub schema_environment_version: u64,
    /// The response this transaction produced, replayed on idempotent retry.
    pub result: Json,
    /// One entry per changed element: `{id, kind, op, version}`.
    pub changes: Vec<Json>,
    /// The changed elements' ids, for `HISTORY ELEMENT`.
    pub changed_ids: Vec<String>,
    /// The Receipt `origin` this commit was attributed to (§33.2): the
    /// Principal, the ActorBinding it exercised and the digest of the
    /// delegation chain it acted under.
    ///
    /// Journalled rather than recomputed, because a resend replays the
    /// Receipt the first attempt produced (§80.4). Rebuilding it from the
    /// caller who happened to ask would attribute the original commit to
    /// whoever resent it, and dropping it would change `receipt_digest` — so
    /// the replayed Receipt would no longer be the one that was signed
    /// (§33.3).
    pub origin: Json,
}
