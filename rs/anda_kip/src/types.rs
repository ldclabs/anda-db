//! # The KIP 2.0 Core data model (Spec §6–§19)
//!
//! Plus the read-out shapes the Specification fixes elsewhere and every engine
//! would otherwise invent for itself: the Epistemic Projection output (§27.2),
//! the Change Stream envelope (§36.1) and the capability report (§67).
//!
//! KIP 2.0 keeps meaning, belief, evidence, provenance, mnemonic state,
//! retention and governance in separate planes. That separation is the point of
//! the version, so these types deliberately refuse to offer the one thing KIP
//! 1.x had that made it easy to blur them: a universal author-writable
//! `metadata` bag (Spec §6.4).
//!
//! Where a value goes:
//!
//! ```text
//! semantic payload       → typed fields / attributes
//! epistemic state        → Assertion
//! Evidence               → Evidence
//! provenance             → Activity / origin
//! governance             → Governance state
//! storage lifecycle      → retention
//! mnemonic/profile state → Facets
//! engine truth           → _system
//! ```

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::ast::{Json, Map};

/// Engine-maintained `_system` members ordinary KML must never write (§6.3).
///
/// These are the members *inside* `_system`. The top-level field names a
/// mutation may not assign to at all — `_system` itself among them — are
/// [`crate::parser::PROTECTED_FIELDS`], which is what the parser checks.
pub const PROTECTED_SYSTEM_FIELDS: &[&str] = &[
    "version",
    "created_at",
    "updated_at",
    "created_tx",
    "updated_tx",
    "state",
    "origin",
    "space_seq",
];

wire_enum! {
    /// The Core Cognitive Element kinds (Spec §6.1).
    ///
    /// `MemorySpace` is a Governance container, not an ordinary element, and
    /// Profile objects such as Experience or Skill are typed Concepts plus
    /// Facets — not new Core kinds.
    #[derive(Default)]
    pub enum ElementKind {
        /// A unit of meaning.
        #[default]
        Concept = "concept",
        /// A truth-neutral `(subject, predicate, object)` tuple.
        Proposition = "proposition",
        /// One actor's epistemic commitment about a Proposition.
        Assertion = "assertion",
        /// An observation record.
        Evidence = "evidence",
        /// A provenance record for a process.
        Activity = "activity",
    }
}

/// The common envelope every durable Cognitive Element carries (Spec §6.2).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ElementEnvelope {
    /// The immutable Nexus-local id: opaque to clients, never reused (§7.1).
    pub id: String,
    /// Which Core kind this element is.
    ///
    /// Not optional: every durable element is exactly one of the five Core
    /// kinds (§6.1), and a reader that cannot tell which cannot tell an
    /// Assertion's `confidence` from a Concept attribute of the same name.
    pub kind: ElementKind,
    /// The element's one home Space (§5.2).
    ///
    /// Omitted rather than sent as an empty string when the view has no Space
    /// to report: a blank home Space is not a Space, and saying so plainly
    /// beats encoding "unknown" as a value that looks like an answer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub space_id: Option<String>,
    /// Governance state — part of the protected control plane.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub governance: Option<GovernanceState>,
    /// Storage lifecycle, never world validity (§19.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<Retention>,
    /// Schema-validated Facets, keyed by facet symbol (§18.1).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub facets: BTreeMap<String, Map<String, Json>>,
    /// Engine truth. Read freely; never write it from a mutation.
    #[serde(default, rename = "_system", skip_serializing_if = "Option::is_none")]
    pub system: Option<SystemState>,
}

/// The Governance members carried on an element (Spec §31.1).
///
/// Author-unwritable by construction: `governance` is in the parser's protected
/// field list, so these arrive only from an authorized Governance operation.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct GovernanceState {
    /// A policy-defined classification label, e.g. `private`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classification: Option<String>,
    /// The policy this element is evaluated under.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_ref: Option<String>,
    /// How far this element may influence behavior (§31.3): `descriptive`,
    /// `advisory`, `behavioral` or `executable`.
    ///
    /// A ceiling, not a truth score: a memory can be certainly true and still
    /// be `descriptive`. Governance-protected — ordinary KML cannot write it,
    /// and it is never inferred from cognitive content. Absent means
    /// `descriptive`, which §31.4 also makes the floor for anything imported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_class: Option<String>,
    /// What this element was derived from, for authority non-amplification.
    ///
    /// §31.5: transformation, summarization and compilation MUST NOT erase
    /// authority-relevant origin lineage. This is where a runtime records it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub authority_lineage: Vec<String>,
    /// Runtime-specific Governance members.
    ///
    /// Flattened rather than dropped: a runtime records things here that this
    /// crate has no name for — why an element is quarantined, what a purged
    /// stub digests to — and silently discarding them on the way through the
    /// wire shape would make an element's own Governance block lie by omission.
    #[serde(flatten, default, skip_serializing_if = "Map::is_empty")]
    pub extensions: Map<String, Json>,
}

/// The storage-lifecycle hook (Spec §19.1).
///
/// `expires_at` is when the *record* stops being retained. It is not
/// `Assertion.valid_time.until`, which is when the *claim* stops applying.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct Retention {
    /// The retention class this element falls under.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_class: Option<String>,
    /// When retention lapses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    /// Whether a legal hold blocks removal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legal_hold: Option<bool>,
}

/// The memory-authority classes §31.3 names, lowest first.
pub const AUTHORITY_CLASSES: &[&str] = &["descriptive", "advisory", "behavioral", "executable"];

/// One counter per version plane (Spec §6.3).
///
/// `version` advances on every committed change to the element; each plane
/// counter advances only when its plane changes, so a guard on one plane
/// (`EXPECT VERSION n OF ATTRIBUTES`, §35.1) is not spoiled by a concurrent
/// write to another.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct PlaneVersions {
    /// Fields and attributes.
    #[serde(default)]
    pub attributes: u64,
    /// Structural References.
    #[serde(default)]
    pub structural: u64,
    /// The retention record.
    #[serde(default)]
    pub retention: u64,
    /// One counter per Facet symbol.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub facets: BTreeMap<String, u64>,
}

/// Engine-maintained state (Spec §6.3).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct SystemState {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_references: Vec<Json>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_versions: Option<Map<String, Json>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_versions: Option<Map<String, Json>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependency_validity: Option<Json>,
    /// Monotonic mutation counter; the target of a bare `EXPECT VERSION`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    /// The per-plane counters `EXPECT VERSION ... OF` guards (§35.1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plane_versions: Option<PlaneVersions>,
    /// When the engine first wrote this element.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    /// When the engine last wrote it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    /// The transaction that created it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_tx: Option<String>,
    /// The transaction that last updated it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_tx: Option<String>,
    /// The engine-level state, e.g. `active`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    /// The Space sequence coordinate of the last state change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub space_seq: Option<u64>,
    /// Who wrote it, through what channel — engine origin, not a claim (§2.5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<Origin>,
}

/// Engine origin: what the runtime observed, not what the content claims.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct Origin {
    /// The authenticated Principal behind the write.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_id: Option<String>,
    /// The transport or channel the write arrived on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    /// The import this element arrived with, when it arrived by import.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub import_id: Option<String>,
}

/// A Concept — a unit of meaning (Spec §10.2).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct Concept {
    /// The common envelope.
    #[serde(flatten)]
    pub envelope: ElementEnvelope,
    /// The exact Schema symbol identity this Concept is typed by (§10.3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_ref: Option<String>,
    /// The immutable Space-local logical key (§7.3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Mutable grounding/display state; duplicates are allowed (§7.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// A high-assurance cross-system identity (§7.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_id: Option<String>,
    /// Alternative names.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
    /// Representation-local state with no independent epistemic lifecycle
    /// (§10.4). A value that can conflict or needs a source belongs in a
    /// Proposition plus an Assertion instead (§10.5).
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub attributes: Map<String, Json>,
}

/// A Proposition — a truth-neutral tuple (Spec §12.2).
///
/// Existence does not imply truth, and the tuple carries no confidence: that
/// lives on the Assertions about it (§2.1, §12.6).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct Proposition {
    /// The common envelope.
    #[serde(flatten)]
    pub envelope: ElementEnvelope,
    /// The subject endpoint, always an Element reference.
    pub subject: Json,
    /// The exact predicate symbol identity.
    pub predicate_ref: String,
    /// The object endpoint: an Element reference or a Literal.
    pub object: Json,
}

wire_enum! {
    /// The stance an Assertion takes (Spec §13.4).
    ///
    /// A `reject` stance about `(x, allergic_to, y)` is not the same claim as
    /// a `support` stance about `(x, allergic_to, false)` (§12.7).
    pub enum Stance {
        /// The assertor holds the Proposition.
        Support = "support",
        /// The assertor denies the Proposition.
        Reject = "reject",
        /// The assertor holds neither.
        Uncertain = "uncertain",
    }
}

wire_enum! {
    /// How an Assertion was arrived at (Spec §13.5, §26).
    ///
    /// A mode does not automatically grant trust.
    pub enum AssertionMode {
        /// Directly observed by the assertor.
        Observed = "observed",
        /// Said by the assertor.
        Stated = "stated",
        /// Derived by reasoning.
        Inferred = "inferred",
        /// Projected about the future.
        Predicted = "predicted",
        /// Entertained without commitment.
        Hypothetical = "hypothetical",
        /// Carried in from another system.
        Imported = "imported",
    }
}

wire_enum! {
    /// The lifecycle of an Assertion (Spec §14).
    pub enum AssertionStatus {
        /// Current.
        Active = "active",
        /// Withdrawn by the assertor or an authorized representative (§14.1).
        Retracted = "retracted",
        /// Replaced by a newer Assertion in a compatible lineage (§14.2): the
        /// claim was wrong for the time it covered, so projection drops it for
        /// every `FOR TIME`.
        Superseded = "superseded",
        /// Computed, never stored (§14.3): the Assertion's `valid_time.until`
        /// lies before the projection's `valid_at`. No statement produces it
        /// and no Change Envelope carries it.
        Expired = "expired",
    }
}

/// The world-time window a claim applies to.
///
/// Independent of `retention.expires_at`, which is storage lifecycle (§19.2).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ValidTime {
    /// When the claim starts applying.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    /// When it stops; `None` means open-ended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<String>,
}

/// One Evidence citation, with the role it plays (Spec §13.2).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct EvidenceRef {
    /// The cited Evidence element.
    pub id: String,
    /// What the citation does for the claim: `support`, `challenge` or
    /// `context` (§56.2).
    ///
    /// A citation with no role is not a supporting one — it is a citation
    /// whose role the record does not state, which projection has to treat
    /// differently from one that says `support`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
}

impl EvidenceRef {
    /// Cites an Evidence element in the given role.
    pub fn new(id: impl Into<String>, role: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            role: Some(role.into()),
        }
    }
}

/// An Assertion — one actor's epistemic commitment (Spec §13.2).
///
/// The epistemic payload is immutable after creation: a changed commitment is a
/// new Assertion plus supersession, never a rewrite (§13.7).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct Assertion {
    /// The common envelope.
    #[serde(flatten)]
    pub envelope: ElementEnvelope,
    /// The Proposition this Assertion is about — exactly one.
    ///
    /// A reference rather than a bare id string, like every other reference
    /// slot in the model: §8 admits a local id, a validated `canonical_id` and
    /// (as an extension) a foreign-Space reference, and a string can only ever
    /// spell the first.
    pub proposition: Json,
    /// The semantic actor whose commitment this is — not the writing Principal
    /// (§13.3).
    pub asserted_by: Json,
    /// The stance taken.
    pub stance: Option<Stance>,
    /// How it was arrived at.
    pub mode: Option<AssertionMode>,
    /// Epistemic support in `[0, 1]`. Not memory accessibility (§2.8).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    /// When the actor made the claim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asserted_at: Option<String>,
    /// The world-time window the claim applies to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_time: Option<ValidTime>,
    /// The Evidence cited, each with its role.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<EvidenceRef>,
    /// The context this claim was made in.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context_refs: Vec<Json>,
    /// Belief-revision state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<AssertionLifecycle>,
}

/// The revision state of an Assertion (Spec §14, §57).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct AssertionLifecycle {
    /// The current lifecycle state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<AssertionStatus>,
    /// Assertions this one replaces.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supersedes: Vec<String>,
    /// Assertions that replaced this one.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub superseded_by: Vec<String>,
    /// When the assertor withdrew it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retracted_at: Option<String>,
}

/// How an Evidence payload is carried.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct EvidencePayload {
    /// `inline` or `external`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// The payload itself, when carried inline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline: Option<Json>,
    /// A content-addressed reference, when carried externally.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_ref: Option<String>,
}

/// An Evidence record — an observation (Spec §15.3).
///
/// Payload and observation identity are immutable; a mistake is corrected with
/// `TRANSITION :old TO "corrected" BY :new`, never by rewriting (§15.5).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct Evidence {
    /// The common envelope.
    #[serde(flatten)]
    pub envelope: ElementEnvelope,
    /// What kind of observation this is (§15.2).
    pub evidence_class: String,
    /// The observed content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<EvidencePayload>,
    /// A digest of the content. Equal digests do not imply identical Evidence
    /// (§15.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_digest: Option<String>,
    /// The payload's media type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    /// When the observation happened — not when the record was written.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<String>,
    /// Where the observation came from — a Concept or another Evidence
    /// (§20.13).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source: Vec<Json>,
    /// The Activity that produced it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generated_by: Option<Json>,
    /// Correction state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<EvidenceLifecycle>,
}

/// The correction state of an Evidence record (Spec §57.2).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct EvidenceLifecycle {
    /// The current lifecycle state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// Evidence this record corrects.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub corrects: Vec<String>,
    /// Evidence that corrected this record.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub corrected_by: Vec<String>,
}

/// An Activity — a provenance record (Spec §16.3).
///
/// An Activity describes a process; it is not a Transaction (§16.4).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct Activity {
    /// The common envelope.
    #[serde(flatten)]
    pub envelope: ElementEnvelope,
    /// What kind of process this was (§16.2).
    pub activity_class: String,
    /// When it started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    /// When it ended; terminal outputs freeze with it (§16.6).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<String>,
    /// What it consumed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<Json>,
    /// What it produced.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<Json>,
    /// The semantic actors involved.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub associated_actors: Vec<Json>,
    /// A digest of the parameters it ran with.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters_digest: Option<String>,
    /// Its lifecycle state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

wire_enum! {
    /// The belief statuses an Epistemic Projection can return (Spec §21.3).
    ///
    /// KIP is open-world: [`BeliefStatus::Insufficient`] is the unknown state,
    /// and [`BeliefStatus::Rejected`] must never be produced merely because
    /// support is absent (§21.5, §24).
    ///
    /// Which is also why it is the `Default`: silence is the absence of a
    /// basis, never a verdict. A default of `Accepted` would let an unfilled
    /// field read as a belief nobody holds, and one of `Rejected` would turn
    /// "nobody said anything" into "the Brain denies it".
    #[derive(Default)]
    pub enum BeliefStatus {
        /// Eligible support is sufficient and unresolved opposition is below
        /// the policy boundary.
        Accepted = "accepted",
        /// Eligible opposition is sufficient.
        Rejected = "rejected",
        /// Material support and material opposition coexist, unresolved.
        Contested = "contested",
        /// Meaningful material exists but is too weak to decide.
        Uncertain = "uncertain",
        /// No sufficient eligible epistemic basis exists — the open-world
        /// unknown.
        #[default]
        Insufficient = "insufficient",
    }
}

impl BeliefStatus {
    /// Whether this status means the Projection settled on a truth value.
    ///
    /// `contested`, `uncertain` and `insufficient` are all real answers about
    /// the state of the evidence; they are not "no".
    pub fn is_decided(&self) -> bool {
        matches!(self, BeliefStatus::Accepted | BeliefStatus::Rejected)
    }
}

/// The baseline Evidence classes (Spec §15.2).
///
/// Schema and Profile extensions may add namespaced classes, which is why this
/// is a list of recommended values rather than a closed enum.
pub const EVIDENCE_CLASSES: &[&str] = &[
    "observation",
    "user_statement",
    "agent_statement",
    "tool_result",
    "measurement",
    "message",
    "document",
    "web_resource",
    "external_assertion",
    "human_feedback",
    "derived_result",
    "outcome",
];

/// The baseline Activity classes (Spec §16.2).
pub const ACTIVITY_CLASSES: &[&str] = &[
    "extraction",
    "tool_execution",
    "human_review",
    "inference",
    "summarization",
    "semantic_consolidation",
    "procedural_consolidation",
    "skill_compilation",
    "import",
    "schema_migration",
    "entity_merge",
    "experience_formation",
    "belief_revision",
];

// ---------------------------------------------------------------------------
// Read-out shapes the Specification fixes outside §6–§19
// ---------------------------------------------------------------------------

/// What an Epistemic Projection answers with (Spec §27.2).
///
/// A `BELIEF` result is not a Core element — it is computed, never stored
/// (§21.2) — but its shape is normative all the same, and an engine that
/// invents its own makes every consumer engine-specific. Four properties the
/// shape exists to preserve:
///
/// - support and opposition are reported **separately**, because a claim with
///   strong evidence on both sides is not the same as one with none, and a
///   single blended number cannot tell them apart;
/// - scores never come without their [`SCORE_SEMANTICS`] label: an unlabelled
///   0.7 invites the reader to assume a probability (§27.3);
/// - the policy that produced the answer is named, so a different threshold
///   yields a visibly different answer rather than a silently different one;
/// - `temporal` carries both axes, since *what was known* and *what was true
///   then* are independent (§48.3).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct Projection {
    /// Complete computation coordinates; required on runtime projection results.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub basis: Option<ProjectionBasis>,
    /// Candidate-local diagnosis, before slot constraints.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_status: Option<BeliefStatus>,
    /// The final status after slot constraints and dependency validation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot_status: Option<BeliefStatus>,
    #[serde(default)]
    pub conflict_refs: Vec<String>,
    #[serde(default)]
    pub conflict_reasons: Vec<String>,
    /// The Proposition this belief is about, when one durably exists.
    ///
    /// `None` is a real answer, not a missing field: a fully grounded `BELIEF`
    /// over a tuple no Proposition has been created for returns
    /// `insufficient` with no id (§46.4). A read must not create the
    /// Proposition to have something to point at.
    ///
    /// An id rather than a reference object, because this names the subject of
    /// the projection rather than occupying a reference slot on a record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposition_id: Option<String>,
    /// The projected belief status.
    pub status: BeliefStatus,
    /// The material supporting the Proposition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub support: Option<ProjectionSide>,
    /// The material opposing it. Never assume the two scores sum to 1 (§27.3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opposition: Option<ProjectionSide>,
    /// Why the answer is not firmer than it is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uncertainty: Option<ProjectionUncertainty>,
    /// The coordinates the projection ran at.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temporal: Option<ProjectionTemporal>,
    /// Which policy decided it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<crate::request::PolicyIdentity>,
    /// The Epistemic Ledger, when one was requested and authorized (§27.4).
    ///
    /// Open by construction: what a ledger contains is the projection
    /// implementation's decision, and §27.4 forbids requiring private
    /// chain-of-thought to produce one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explanation: Option<Json>,
}

/// One side — supporting or opposing — of a projection (Spec §27.2).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ProjectionSide {
    /// The strength of this side, in whatever `score_semantics` declares.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
    /// What the score means. Required alongside a score (§27.3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score_semantics: Option<String>,
    /// The Assertions on this side.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assertion_ids: Vec<String>,
    /// The corroboration groups they collapse into.
    ///
    /// Two actors repeating one observation are one root, not two: this is
    /// where independent support is distinguished from repetition (§23).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub root_groups: Vec<Json>,
}

/// Why a projection is not firmer than it is (Spec §27.2).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ProjectionUncertainty {
    /// A qualitative or numeric level, as the policy defines it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub level: Option<Json>,
    /// What limited the answer — staleness, low trust, exclusions, thin
    /// material. Naming these is what separates "we do not know" from "we
    /// looked and found nothing".
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reasons: Vec<String>,
}

/// The two independent time axes a projection ran under (Spec §27.2, §48.3).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ProjectionTemporal {
    /// The world moment the claims were evaluated for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_at: Option<String>,
    /// The cognitive history the projection read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub as_of_seq: Option<u64>,
}

/// KIP Cognitive Consistency §2. Opaque control identities disclose no grants.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ProjectionBasis {
    pub space_id: String,
    pub snapshot_seq: u64,
    pub schema_environment_version: u64,
    pub identity_version: u64,
    pub policy: ProjectionPolicyVersion,
    pub trust_version: String,
    pub authorization_view: String,
    pub context_refs: Vec<String>,
    pub purpose: String,
    pub risk: String,
    pub valid_at: String,
    pub next_invalid_at: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ProjectionPolicyVersion {
    pub id: String,
    pub version: String,
}

/// The score interpretations §27.3 names.
///
/// A recommendation rather than a closed enum: an implementation may declare
/// its own, and the requirement is that it declares *something*.
pub const SCORE_SEMANTICS: &[&str] = &[
    "ordinal_strength",
    "normalized_support",
    "calibrated_probability",
    "log_odds",
    "implementation_specific",
];

/// One committed transition, as the Change Stream delivers it (Spec §36.1).
///
/// The envelope is the unit of atomicity: a consumer must treat everything in
/// `changes` as one cognitive transition (§36.2). Delivery may be
/// at-least-once, so `space_id + space_seq + tx_id` is the deduplication key
/// (§36.3) — and a replayed envelope must not become new Evidence,
/// reinforcement or a duplicated Experience (§36.4).
///
/// **`HISTORY` answers in envelopes too.** §68.1 defines `HISTORY` as
/// *transition chronology*, and §36.2 defines a transition as one envelope, so
/// `HISTORY ELEMENT` and `HISTORY SPACE` are the same unit asked for over a
/// different range — an element's, or a Space's. Emitting one grain there and
/// another in `CHANGES` would make "what happened to this element" and "what
/// happened here" two incomparable answers to the same question.
///
/// The fields past §36.1's conceptual shape are optional and carried here
/// rather than added per engine: an envelope is the one artifact two engines
/// hand the same consumer, so a field one of them invents is a field the other
/// silently lacks.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ChangeEnvelope {
    /// Governed control-plane transitions, separate from cognitive elements.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub control_changes: Vec<ControlChange>,
    /// Coverage belongs to an authorization view; sequence gaps prove no silence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coverage: Option<ChangeCoverage>,
    /// The protocol version, when the runtime stamps it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kip: Option<String>,
    /// The Space that committed.
    pub space_id: String,
    /// The commit sequence this transition produced.
    pub space_seq: u64,
    /// The transaction that produced it.
    pub tx_id: String,
    /// When it committed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_at: Option<String>,
    /// The transaction class, e.g. `cognitive`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction_class: Option<String>,
    /// The Schema Environment version in force when it committed (§33.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_environment_version: Option<u64>,
    /// What changed, one entry per element (§36.1,
    /// `schemas/kip-change-envelope.schema.json`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changes: Vec<ChangeEntry>,
    /// Namespaced extensions (§36.1).
    ///
    /// The schema is `additionalProperties: false`, so anything past §36.1's
    /// shape lives here rather than beside it: a member one runtime invents at
    /// the top level is a member the other's consumer rejects outright.
    /// [`ChangeEnvelope::transition_detail`] is what both engines in this
    /// repository carry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Map<String, Json>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ControlChange {
    pub kind: String,
    pub version: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ChangeCoverage {
    pub through_seq: u64,
    pub authorization_view: String,
    pub complete: bool,
}

/// The extension key carrying what a transition was decided against (§36.1).
///
/// Two facts §36.1's shape has no slot for and a chronology reader wants:
/// `snapshot_seq`, the coordinate the transaction read from — `space_seq` says
/// what the commit produced, this says what it was decided against, which is
/// how a stale write is told from a serial one — and `status`, so that a
/// `no_effect` transition stays a real entry rather than becoming
/// indistinguishable from a request nobody made.
pub const CHANGE_TRANSITION_EXTENSION: &str = "anda/transition";

impl ChangeEnvelope {
    /// Builds the [`CHANGE_TRANSITION_EXTENSION`] value, so both engines spell
    /// it the same way.
    pub fn transition_detail(snapshot_seq: u64, status: &str) -> Map<String, Json> {
        let mut detail = Map::new();
        detail.insert("snapshot_seq".to_string(), Json::from(snapshot_seq));
        detail.insert("status".to_string(), Json::from(status));
        detail
    }

    /// The at-least-once deduplication key (Spec §36.3).
    pub fn dedup_key(&self) -> (&str, u64, &str) {
        (&self.space_id, self.space_seq, &self.tx_id)
    }
}

/// What one commit did to one element (Spec §36.1).
///
/// Names and versions, never values: `touched` carries the paths that changed
/// and `planes` the counters after the commit, which is exactly what a Watch
/// needs to decide whether a slot, an element or a type moved without reading
/// payload. Existence protection applies per entry (§30.4): an element the
/// consumer may not discover is omitted from the envelope it receives.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ChangeEntry {
    /// What the commit did to the element.
    pub op: ChangeOp,
    /// Which Core kind the element is.
    pub kind: ElementKind,
    /// The element id.
    pub id: String,
    /// The exact Concept Type reference; present for Concept entries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_ref: Option<String>,
    /// The version before this commit, when the element existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_version: Option<u64>,
    /// The version after this commit.
    pub new_version: u64,
    /// The stored status before and after; present for `lifecycle` entries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<ChangeState>,
    /// The references that place the entry: the Proposition for an Assertion,
    /// subject and predicate for a Proposition, the target for a merge.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refs: Option<ChangeRefs>,
    /// The paths changed — attribute, Facet, Structural Field or retention
    /// names — carrying names only, never values.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub touched: Vec<String>,
    /// The plane counters after this commit, for each plane the entry touched.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planes: Option<PlaneVersions>,
    /// Namespaced extensions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Map<String, Json>>,
}

/// The operations a Change Envelope entry records (Spec §36.1).
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ChangeOp {
    /// The element was created.
    Create,
    /// Mutable state changed.
    Update,
    /// The lifecycle status moved (`TRANSITION`).
    Lifecycle,
    /// The retention record changed.
    Retention,
    /// The Concept was merged into another.
    Merge,
    /// The element was physically erased.
    Purge,
    /// The Evidence payload was erased; the record survives.
    PayloadPurge,
}

/// A lifecycle move, as an entry records it.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ChangeState {
    /// The stored status before the commit.
    pub from: String,
    /// The stored status after it.
    pub to: String,
}

/// The references that place a Change Envelope entry (Spec §36.1).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ChangeRefs {
    /// Assertion entries: the Proposition the Assertion is about.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposition: Option<String>,
    /// Proposition entries: the subject element id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    /// Proposition entries: the exact Predicate reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate_ref: Option<String>,
    /// Merge entries on the source Concept: the canonical target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merged_into: Option<String>,
}

/// What `DESCRIBE CAPABILITIES` answers with (Spec §67).
///
/// The three-way split is the point. **Supported** is what the runtime
/// implements; **available** is what this Principal may actually request. A
/// runtime that reports only the first tells a caller to try things it will be
/// refused for; one that reports only the second makes an authorization gap
/// look like a missing feature. Neither is a Grant dump — §67.2 is explicit
/// that availability is not unlimited authorization, and §67.3 allows the
/// enumeration itself to be redacted.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct Capabilities {
    /// The conformance profiles the runtime claims (§89).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub profiles: Vec<crate::conformance::ConformanceProfile>,
    /// What this runtime or Space technically implements (§67.1).
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub supported: Map<String, Json>,
    /// What the current Principal may request, in at least some scope (§67.2).
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub available: Map<String, Json>,
    /// Quotas and ceilings that apply.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub limits: Map<String, Json>,
    /// Capabilities this crate has no name for.
    #[serde(flatten, default, skip_serializing_if = "Map::is_empty")]
    pub extensions: Map<String, Json>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every spelling `wire_enum!` generates is the one spelling.
    ///
    /// The macro is the crate's only declaration of a closed wire vocabulary,
    /// and nine public types now depend on it agreeing with itself:
    /// `semantics::STANCES` and friends *are* `NAMES`, so a `NAMES` out of
    /// step with `as_str` would have the parser validate written words against
    /// strings the engine never emits. Checked per type rather than per
    /// variant, so a variant added anywhere is covered without a new row here.
    #[test]
    fn a_wire_vocabulary_has_exactly_one_spelling_per_value() {
        macro_rules! check {
            ($ty:ty) => {{
                let label = stringify!($ty);
                let all = <$ty>::ALL;
                let names = <$ty>::NAMES;
                assert_eq!(all.len(), names.len(), "{label}: ALL and NAMES disagree");
                assert!(!all.is_empty(), "{label}: an empty vocabulary");

                for (value, name) in all.iter().zip(names) {
                    // `as_str`, Display, serde and the registry slice are one
                    // string,
                    assert_eq!(value.as_str(), *name, "{label}: as_str");
                    assert_eq!(&value.to_string(), name, "{label}: Display");
                    assert_eq!(
                        serde_json::to_value(value).expect("serializes"),
                        Json::String((*name).to_string()),
                        "{label}: Serialize"
                    );
                    // and every one of them round-trips back to the value.
                    assert_eq!(
                        &name.parse::<$ty>().expect("FromStr takes its own name"),
                        value,
                        "{label}: FromStr"
                    );
                    assert_eq!(
                        &serde_json::from_value::<$ty>(Json::String((*name).to_string()))
                            .expect("Deserialize takes its own name"),
                        value,
                        "{label}: Deserialize"
                    );
                    assert_eq!(<$ty>::from_wire(name), Some(*value), "{label}: from_wire");
                }

                // A vocabulary is closed: nothing outside the list is
                // admitted, and the externally-tagged map form a *derived*
                // enum would have accepted is not a KIP value.
                assert!(
                    "kip/not-a-value".parse::<$ty>().is_err(),
                    "{label}: FromStr"
                );
                assert!(
                    serde_json::from_str::<$ty>(r#"{"kip/not-a-value":null}"#).is_err(),
                    "{label}: a wire vocabulary is a string, never a tagged map"
                );

                // Two variants sharing a spelling would make the wire
                // ambiguous in the direction that has no error to report.
                let unique: BTreeMap<&str, ()> = names.iter().map(|n| (*n, ())).collect();
                assert_eq!(unique.len(), names.len(), "{label}: a repeated spelling");
            }};
        }

        check!(ElementKind);
        check!(Stance);
        check!(AssertionMode);
        check!(AssertionStatus);
        check!(BeliefStatus);
        check!(crate::error::ErrorCategory);
        check!(crate::error::RetryClass);
        check!(crate::request::SearchMode);
        check!(crate::conformance::ConformanceProfile);
    }

    #[test]
    fn vocabularies_use_their_wire_spellings() {
        assert_eq!(
            serde_json::to_string(&Stance::Support).unwrap(),
            r#""support""#
        );
        assert_eq!(
            serde_json::to_string(&AssertionMode::Hypothetical).unwrap(),
            r#""hypothetical""#
        );
        assert_eq!(
            serde_json::to_string(&BeliefStatus::Insufficient).unwrap(),
            r#""insufficient""#
        );
        assert_eq!(
            serde_json::to_string(&ElementKind::Proposition).unwrap(),
            r#""proposition""#
        );
    }

    #[test]
    fn an_unfilled_projection_is_unknown_and_not_a_verdict() {
        // §21.5 / §24: absence of support is never rejection, so the state a
        // projection falls back to has to be the open-world unknown.
        assert_eq!(Projection::default().status, BeliefStatus::Insufficient);
        assert_eq!(
            serde_json::to_value(Projection::default()).unwrap(),
            serde_json::json!({ "status": "insufficient", "conflict_refs": [], "conflict_reasons": [] })
        );

        // §46.4: a fully grounded BELIEF over a tuple no Proposition exists
        // for answers `insufficient` with no id — and the read must not
        // create one just to have something to point at.
        assert_eq!(Projection::default().proposition_id, None);
    }

    #[test]
    fn a_projection_reports_support_and_opposition_separately() {
        // §27.3: the two are not complements, and a caller must be able to
        // see strong-evidence-on-both-sides for what it is.
        let projection = Projection {
            proposition_id: Some("P-1".into()),
            status: BeliefStatus::Contested,
            support: Some(ProjectionSide {
                score: Some(0.8),
                score_semantics: Some("ordinal_strength".into()),
                assertion_ids: vec!["A-1".into()],
                root_groups: vec![serde_json::json!({ "roots": ["E-1"] })],
            }),
            opposition: Some(ProjectionSide {
                score: Some(0.7),
                score_semantics: Some("ordinal_strength".into()),
                assertion_ids: vec!["A-2".into()],
                root_groups: Vec::new(),
            }),
            uncertainty: Some(ProjectionUncertainty {
                level: Some(serde_json::json!("high")),
                reasons: vec!["conflicting independent roots".into()],
            }),
            temporal: Some(ProjectionTemporal {
                valid_at: Some("2026-01-01T00:00:00.000Z".into()),
                as_of_seq: Some(1500),
            }),
            policy: Some(crate::request::PolicyIdentity::new("kip:policy:baseline")),
            explanation: None,
            ..Default::default()
        };

        let json = serde_json::to_value(&projection).unwrap();
        assert_eq!(json["status"], "contested");
        // The scores do not sum to 1, and nothing in the shape suggests they
        // should.
        assert_eq!(json["support"]["score"], 0.8);
        assert_eq!(json["opposition"]["score"], 0.7);
        assert_eq!(json["support"]["score_semantics"], "ordinal_strength");
        assert_eq!(json["temporal"]["as_of_seq"], 1500);
        assert_eq!(
            serde_json::from_value::<Projection>(json).unwrap(),
            projection
        );
    }

    #[test]
    fn a_change_envelope_dedupes_on_the_key_the_spec_names() {
        // §36.3: delivery may be at-least-once, so the consumer needs exactly
        // this triple to recognize a replay.
        let mut extensions = Map::new();
        extensions.insert(
            CHANGE_TRANSITION_EXTENSION.to_string(),
            Json::Object(ChangeEnvelope::transition_detail(1500, "committed")),
        );
        let envelope = ChangeEnvelope {
            control_changes: vec![],
            coverage: None,
            kip: Some("2.0".into()),
            space_id: "space-1".into(),
            space_seq: 1501,
            tx_id: "tx-900".into(),
            committed_at: Some("2026-01-01T00:00:00.000Z".into()),
            transaction_class: Some("cognitive".into()),
            schema_environment_version: Some(1),
            extensions: Some(extensions),
            changes: vec![ChangeEntry {
                op: ChangeOp::Create,
                kind: ElementKind::Concept,
                id: "C-1".into(),
                schema_ref: Some("kip://profiles/cognitive-memory@2.0.0/Person".into()),
                old_version: None,
                new_version: 1,
                state: None,
                refs: None,
                touched: vec!["attributes.name".into()],
                planes: Some(PlaneVersions {
                    attributes: 1,
                    ..Default::default()
                }),
                extensions: None,
            }],
        };
        assert_eq!(envelope.dedup_key(), ("space-1", 1501, "tx-900"));
        // §36.1: entries carry names and versions, never values, in the shape
        // `schemas/kip-change-envelope.schema.json` fixes.
        let entry = serde_json::to_value(&envelope.changes[0]).unwrap();
        assert_eq!(entry["op"], "create");
        assert_eq!(entry["kind"], "concept");
        assert_eq!(entry["planes"]["attributes"], 1);
        assert!(entry.get("old_version").is_none());
        let lifecycle = ChangeEntry {
            op: ChangeOp::Lifecycle,
            kind: ElementKind::Assertion,
            id: "A-1".into(),
            schema_ref: None,
            old_version: Some(2),
            new_version: 3,
            state: Some(ChangeState {
                from: "active".into(),
                to: "superseded".into(),
            }),
            refs: Some(ChangeRefs {
                proposition: Some("P-1".into()),
                ..Default::default()
            }),
            touched: vec![],
            planes: None,
            extensions: None,
        };
        let json = serde_json::to_value(&lifecycle).unwrap();
        assert_eq!(json["op"], "lifecycle");
        assert_eq!(json["state"]["to"], "superseded");
        assert_eq!(json["refs"]["proposition"], "P-1");
        assert!(json["refs"].get("subject").is_none());

        let replayed = envelope.clone();
        assert_eq!(replayed.dedup_key(), envelope.dedup_key());

        // The fields past §36.1's conceptual shape are optional, so an engine
        // that journals none of them still produces a legal envelope — and the
        // wire form omits them rather than reporting a guess.
        let minimal = ChangeEnvelope {
            space_id: "space-1".into(),
            space_seq: 1501,
            tx_id: "tx-900".into(),
            ..Default::default()
        };
        let json = serde_json::to_value(&minimal).unwrap();
        assert_eq!(json.as_object().unwrap().len(), 3);
        assert_eq!(json["space_seq"], 1501);
    }

    #[test]
    fn capabilities_keep_supported_and_available_apart() {
        // §67.1 vs §67.2: implementing a feature and being allowed to ask for
        // it are different answers, and collapsing them makes an
        // authorization gap look like a missing feature.
        let capabilities = Capabilities {
            profiles: vec![
                crate::conformance::ConformanceProfile::Core,
                crate::conformance::ConformanceProfile::Kql,
            ],
            supported: serde_json::json!({ "belief_slot": true, "capsule_import": true })
                .as_object()
                .cloned()
                .unwrap(),
            available: serde_json::json!({ "belief_slot": true })
                .as_object()
                .cloned()
                .unwrap(),
            limits: serde_json::json!({ "max_limit": 1000 })
                .as_object()
                .cloned()
                .unwrap(),
            extensions: Map::new(),
        };

        let json = serde_json::to_value(&capabilities).unwrap();
        assert_eq!(json["profiles"], serde_json::json!(["KIP-Core", "KIP-KQL"]));
        assert_eq!(json["supported"]["capsule_import"], true);
        assert!(
            json["available"].get("capsule_import").is_none(),
            "supported but not available must stay visible as exactly that"
        );
        assert_eq!(
            serde_json::from_value::<Capabilities>(json).unwrap(),
            capabilities
        );
    }

    #[test]
    fn the_envelope_nests_engine_truth_under_its_reserved_name() {
        let concept = Concept {
            envelope: ElementEnvelope {
                id: "C-1".into(),
                kind: ElementKind::Concept,
                space_id: Some("space-1".into()),
                system: Some(SystemState {
                    version: Some(3),
                    ..Default::default()
                }),
                ..Default::default()
            },
            name: Some("Alice".into()),
            ..Default::default()
        };
        let json = serde_json::to_value(&concept).unwrap();
        assert_eq!(json["_system"]["version"], 3);
        assert_eq!(json["name"], "Alice");
        assert_eq!(json["kind"], "concept");

        let decoded: Concept = serde_json::from_value(json).unwrap();
        assert_eq!(decoded, concept);
    }

    #[test]
    fn absence_of_support_is_not_rejection() {
        // Spec §21.5 and §24: open-world semantics.
        assert!(!BeliefStatus::Insufficient.is_decided());
        assert!(!BeliefStatus::Contested.is_decided());
        assert!(BeliefStatus::Rejected.is_decided());
    }

    #[test]
    fn retention_and_valid_time_are_different_fields() {
        // Spec §19.2: one is storage lifecycle, the other world applicability.
        let retention = Retention {
            expires_at: Some("2027-01-01T00:00:00.000Z".into()),
            ..Default::default()
        };
        let valid = ValidTime {
            until: Some("2026-06-01T00:00:00.000Z".into()),
            ..Default::default()
        };
        assert_ne!(retention.expires_at, valid.until);
    }

    #[test]
    fn the_protected_system_field_list_matches_the_spec() {
        assert_eq!(PROTECTED_SYSTEM_FIELDS.len(), 8);
        assert!(PROTECTED_SYSTEM_FIELDS.contains(&"space_seq"));
        assert!(PROTECTED_SYSTEM_FIELDS.contains(&"origin"));
    }

    #[test]
    fn assertion_round_trips_with_its_epistemic_payload() {
        let assertion = Assertion {
            envelope: ElementEnvelope {
                id: "A-1".into(),
                kind: ElementKind::Assertion,
                ..Default::default()
            },
            proposition: serde_json::json!({"id": "P-1"}),
            asserted_by: serde_json::json!({"id": "C-alice"}),
            stance: Some(Stance::Support),
            mode: Some(AssertionMode::Stated),
            confidence: Some(0.9),
            evidence: vec![EvidenceRef::new("E-1", "support")],
            ..Default::default()
        };
        let encoded = serde_json::to_string(&assertion).unwrap();
        let decoded: Assertion = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, assertion);
    }

    #[test]
    fn the_core_elements_use_the_field_names_the_spec_gives_them() {
        // §13.2 and §15.3 fix these slots, and an engine that renames one
        // makes every cross-engine reader wrong about the same record. The
        // reference slots carry objects rather than bare ids, because §8
        // admits a canonical or foreign identity in the same position and a
        // string can only ever spell a local one.
        let assertion = serde_json::to_value(Assertion {
            proposition: serde_json::json!({"id": "P-1"}),
            asserted_by: serde_json::json!({"id": "C-alice"}),
            evidence: vec![EvidenceRef::new("E-1", "support")],
            ..Default::default()
        })
        .unwrap();
        assert_eq!(assertion["proposition"], serde_json::json!({"id": "P-1"}));
        assert_eq!(
            assertion["evidence"],
            serde_json::json!([{"id": "E-1", "role": "support"}])
        );
        for gone in ["proposition_id", "evidence_refs"] {
            assert!(
                assertion.get(gone).is_none(),
                "{gone} is not a KIP 2.0 slot"
            );
        }

        let evidence = serde_json::to_value(Evidence {
            evidence_class: "tool_result".into(),
            source: vec![serde_json::json!({"id": "C-tool"})],
            ..Default::default()
        })
        .unwrap();
        assert_eq!(evidence["source"], serde_json::json!([{"id": "C-tool"}]));
        assert!(evidence.get("source_refs").is_none());
    }
}
