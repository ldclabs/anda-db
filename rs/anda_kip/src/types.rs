//! # The KIP 2.0 Core data model (Spec §6–§19)
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
use std::{collections::BTreeMap, fmt};

use crate::ast::{Json, Map};

/// Engine-maintained `_system` members ordinary KML must never write (§6.3).
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

/// The Core Cognitive Element kinds (Spec §6.1).
///
/// `MemorySpace` is a Governance container, not an ordinary element, and Profile
/// objects such as Experience or Skill are typed Concepts plus Facets — not new
/// Core kinds.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum ElementKind {
    /// A unit of meaning.
    Concept,
    /// A truth-neutral `(subject, predicate, object)` tuple.
    Proposition,
    /// One actor's epistemic commitment about a Proposition.
    Assertion,
    /// An observation record.
    Evidence,
    /// A provenance record for a process.
    Activity,
}

impl fmt::Display for ElementKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            ElementKind::Concept => "concept",
            ElementKind::Proposition => "proposition",
            ElementKind::Assertion => "assertion",
            ElementKind::Evidence => "evidence",
            ElementKind::Activity => "activity",
        };
        f.write_str(name)
    }
}

/// The common envelope every durable Cognitive Element carries (Spec §6.2).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ElementEnvelope {
    /// The immutable Nexus-local id: opaque to clients, never reused (§7.1).
    pub id: String,
    /// Which Core kind this element is.
    pub kind: Option<ElementKind>,
    /// The element's one home Space (§5.2).
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
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct GovernanceState {
    /// A policy-defined classification label, e.g. `private`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classification: Option<String>,
    /// The policy this element is evaluated under.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_ref: Option<String>,
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

/// Engine-maintained state (Spec §6.3).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct SystemState {
    /// Monotonic mutation counter; the target of `EXPECT VERSION`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
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

/// The stance an Assertion takes (Spec §13.4).
///
/// A `reject` stance about `(x, allergic_to, y)` is not the same claim as a
/// `support` stance about `(x, allergic_to, false)` (§12.7).
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Stance {
    /// The assertor holds the Proposition.
    Support,
    /// The assertor denies the Proposition.
    Reject,
    /// The assertor holds neither.
    Uncertain,
}

/// How an Assertion was arrived at (Spec §13.5, §26).
///
/// A mode does not automatically grant trust.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum AssertionMode {
    /// Directly observed by the assertor.
    Observed,
    /// Said by the assertor.
    Stated,
    /// Derived by reasoning.
    Inferred,
    /// Projected about the future.
    Predicted,
    /// Entertained without commitment.
    Hypothetical,
    /// Carried in from another system.
    Imported,
}

/// The lifecycle of an Assertion (Spec §14).
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum AssertionStatus {
    /// Current.
    Active,
    /// Withdrawn by the assertor or an authorized representative (§14.1).
    Retracted,
    /// Replaced by a newer Assertion in a compatible lineage (§14.2).
    Superseded,
    /// No longer current under its lifecycle model (§14.3).
    Expired,
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

/// One Evidence citation, with the role it plays.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct EvidenceRef {
    /// The cited Evidence element.
    pub evidence_id: String,
    /// What the citation does for the claim, e.g. `support`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
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
    /// The Proposition this Assertion is about.
    pub proposition_id: String,
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
    /// The Evidence cited.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<EvidenceRef>,
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
/// `CORRECT EVIDENCE`, never by rewriting (§15.5).
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
    /// Where the observation came from.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_refs: Vec<Json>,
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

/// The belief statuses an Epistemic Projection can return (Spec §21.3).
///
/// KIP is open-world: [`BeliefStatus::Insufficient`] is the unknown state, and
/// [`BeliefStatus::Rejected`] must never be produced merely because support is
/// absent (§21.5, §24).
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum BeliefStatus {
    /// Eligible support is sufficient and unresolved opposition is below the
    /// policy boundary.
    Accepted,
    /// Eligible opposition is sufficient.
    Rejected,
    /// Material support and material opposition coexist, unresolved.
    Contested,
    /// Meaningful material exists but is too weak to decide.
    Uncertain,
    /// No sufficient eligible epistemic basis exists — the open-world unknown.
    Insufficient,
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

#[cfg(test)]
mod tests {
    use super::*;

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
    fn the_envelope_nests_engine_truth_under_its_reserved_name() {
        let concept = Concept {
            envelope: ElementEnvelope {
                id: "C-1".into(),
                kind: Some(ElementKind::Concept),
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
            expires_at: Some("2027-01-01T00:00:00Z".into()),
            ..Default::default()
        };
        let valid = ValidTime {
            until: Some("2026-06-01T00:00:00Z".into()),
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
                kind: Some(ElementKind::Assertion),
                ..Default::default()
            },
            proposition_id: "P-1".into(),
            asserted_by: serde_json::json!({"id": "C-alice"}),
            stance: Some(Stance::Support),
            mode: Some(AssertionMode::Stated),
            confidence: Some(0.9),
            evidence_refs: vec![EvidenceRef {
                evidence_id: "E-1".into(),
                role: Some("support".into()),
            }],
            ..Default::default()
        };
        let encoded = serde_json::to_string(&assertion).unwrap();
        let decoded: Assertion = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, assertion);
    }
}
