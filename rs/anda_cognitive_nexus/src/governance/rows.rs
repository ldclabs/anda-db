//! # The protected records of the Governance Control Plane
//!
//! These rows live in the same database as cognitive state and are emphatically
//! **not** cognitive state (Governance §2). A Concept saying "Alice is an
//! administrator" is a claim; a [`GrantRow`] naming Alice is authority. The
//! whole plane exists to keep those two things from being the same write.
//!
//! Consequences that shape every row here:
//!
//! - **No KML clause can reach them.** They are written through host APIs only,
//!   which is what stops a prompt injection into ordinary memory formation from
//!   having a path to privilege escalation (§264).
//! - **Revocation is a status change, never a delete.** A revoked Grant must
//!   stop authorizing future operations without rewriting the audit that says
//!   it authorized a past one (§36, §49).
//! - **Every mutation is mirrored into [`GovernanceAuditRow`] with the complete
//!   new record.** Whole records rather than diffs, for the same reason the
//!   element version log stores whole rows: a diff chain with one missing link
//!   answers a historical question wrongly instead of refusing (§175).
//!
//! Ids are minted from the `anda_db` row id rather than carried in a column,
//! because `anda_db` assigns the row id at insert and a second write to store
//! the derived string would be a window where the record exists without its own
//! name. Records a caller names — Principals, groups, policies — keep the
//! caller's id, because those names appear in Grants and in audit records and
//! must stay stable.

use anda_db_schema::{AndaDBSchema, Json};
use serde::{Deserialize, Serialize};

/// What kind of runtime identity a Principal is (§7.1).
pub mod principal_class {
    /// A human being, authenticated by the deployment.
    pub const HUMAN: &str = "human";
    /// An autonomous agent acting under its own or a delegated identity.
    pub const AGENT: &str = "agent";
    /// A machine identity for service-to-service calls (§218).
    pub const SERVICE: &str = "service";
    /// The engine's own identity for maintenance it performs itself (§212).
    pub const SYSTEM: &str = "system";
    /// An unauthenticated caller, where a Space's policy admits one (§217).
    pub const ANONYMOUS: &str = "anonymous";
}

/// The lifecycle every Governance record shares (§9).
///
/// `revoked` is terminal and deliberately not a deletion: an operation that ran
/// while the record was active stays attributable to it.
pub mod status {
    /// In force.
    pub const ACTIVE: &str = "active";
    /// Temporarily ineffective; may return to `active`.
    pub const SUSPENDED: &str = "suspended";
    /// Permanently ineffective for future operations.
    pub const REVOKED: &str = "revoked";
}

/// How a Principal is connected to a semantic actor (§14.2).
pub mod binding_class {
    /// The Principal *is* this actor.
    pub const SELF: &str = "self";
    /// A machine identity standing for a service.
    pub const SERVICE_IDENTITY: &str = "service_identity";
    /// The Principal may speak on the actor's behalf.
    pub const REPRESENTS: &str = "represents";
    /// An agent acting for an organization.
    pub const ORGANIZATION_AGENT: &str = "organization_agent";
    /// A maintenance identity, which represents no one.
    pub const MAINTENANCE_IDENTITY: &str = "maintenance_identity";
}

/// How well the binding between Principal and actor is established (§16).
///
/// This is what the Epistemic Model reads as attribution assurance. It is not
/// confidence and not trust: it says how sure the *engine* is about who spoke,
/// not how much anyone should believe what was said.
pub mod assurance {
    /// Established by the deployment's identity system.
    pub const VERIFIED: &str = "verified";
    /// Inferred from strong but indirect signals.
    pub const STRONGLY_INFERRED: &str = "strongly_inferred";
    /// Recorded, but nothing checked it.
    pub const UNVERIFIED: &str = "unverified";
}

/// The deployment-defined strength ladder (§11).
///
/// Ordered, because a policy says "at least"; the vocabulary is otherwise not
/// KIP's business — KIP consumes authenticated identity, it does not perform
/// authentication.
pub mod auth_strength {
    /// No authentication was performed.
    pub const NONE: &str = "none";
    /// An ordinary authenticated session.
    pub const STANDARD: &str = "standard";
    /// Multi-factor, hardware-backed, or otherwise elevated.
    pub const STRONG: &str = "strong";

    /// The rung a strength name sits on, for "at least" comparisons.
    ///
    /// An unrecognized name is the *lowest* rung rather than the highest: a
    /// deployment that invents a strength must not have it silently satisfy
    /// every `min_auth_strength` in the Space.
    pub fn rank(name: &str) -> u8 {
        match name {
            STRONG => 2,
            STANDARD => 1,
            _ => 0,
        }
    }
}

/// How much a Principal's declared purpose can be relied on (§12).
///
/// A purpose is context, never proof. `declared` is what a caller wrote in the
/// request envelope, and a high-risk Grant must not depend on it alone.
pub mod purpose_assurance {
    /// Self-declared in the request. Trusted for nothing on its own.
    pub const DECLARED: &str = "declared";
    /// Fixed for the session by the host at authentication time.
    pub const SESSION_BOUND: &str = "session_bound";
    /// Set by the runtime itself, not by any caller.
    pub const SYSTEM_BOUND: &str = "system_bound";
    /// Carried by a satisfied Approval.
    pub const APPROVED: &str = "approved";

    /// The rung a purpose assurance sits on, for "at least" comparisons.
    pub fn rank(name: &str) -> u8 {
        match name {
            APPROVED => 3,
            SYSTEM_BOUND => 2,
            SESSION_BOUND => 1,
            _ => 0,
        }
    }
}

/// An authenticated execution identity (§7, §8).
///
/// A Principal answers "who is making this protocol operation". It is not the
/// semantic Person or Agent Concept an Assertion is attributed to — that is
/// what [`ActorBindingRow`] bridges, under Governance authority rather than by
/// anyone writing a Proposition about it.
///
/// KIP standardizes no authentication protocol. `auth_provider` and
/// `auth_subject` record *which* deployment subsystem vouched for this identity
/// and under what opaque subject; neither is a credential and neither is
/// verified here.
#[derive(Clone, Debug, Default, Deserialize, Serialize, AndaDBSchema)]
pub struct PrincipalRow {
    /// The row id.
    pub _id: u64,
    /// The stable Principal id, as it appears in `_system.origin.principal_id`.
    #[unique]
    pub principal_id: String,
    /// One of [`principal_class`].
    pub principal_class: String,
    /// One of [`status`].
    pub status: String,
    /// A human-readable label. Carries no authority (§203).
    pub display_name: String,
    /// Which deployment subsystem authenticated this identity.
    pub auth_provider: String,
    /// That subsystem's opaque subject reference. Never a credential.
    pub auth_subject: String,
    /// When the record was created.
    pub created_at: String,
    /// When it last changed.
    pub updated_at: String,
    /// When it was revoked; empty while it is not.
    pub revoked_at: String,
    /// Bumped on every change, so a cached decision can be invalidated (§187).
    pub version: u64,
}

/// A named set of Principals (§25).
///
/// Membership controls authority, which is exactly why it cannot be derived
/// from ordinary cognitive Propositions: an Agent that could write "I am a
/// maintainer" would be granting itself maintenance.
#[derive(Clone, Debug, Default, Deserialize, Serialize, AndaDBSchema)]
pub struct PrincipalGroupRow {
    /// The row id.
    pub _id: u64,
    /// The stable group id, as Grants name it.
    #[unique]
    pub group_id: String,
    /// A human-readable label.
    pub name: String,
    /// What the group is for.
    pub description: String,
    /// The member Principal ids. Indexed elementwise, so "which groups is P
    /// in" is one lookup on the authorization hot path.
    pub members: Vec<String>,
    /// One of [`status`].
    pub status: String,
    /// When the record was created.
    pub created_at: String,
    /// When it last changed.
    pub updated_at: String,
    /// Bumped on every change.
    pub version: u64,
}

/// The trusted link between a Principal and a semantic actor (§14).
///
/// Without one, a Principal may still *record* what an actor said — recording a
/// claim is not impersonation (§17) — but it cannot exercise that actor's
/// authority. The two are separate permissions for a reason: a Formation Agent
/// that observed "Alice: I prefer dark mode" should be able to store it as
/// Alice's stated claim without thereby being able to retract Alice's other
/// Assertions.
#[derive(Clone, Debug, Default, Deserialize, Serialize, AndaDBSchema)]
pub struct ActorBindingRow {
    /// The row id, which also mints `kip:binding:{_id}`.
    pub _id: u64,
    /// The Principal side of the binding.
    pub principal_id: String,
    /// The semantic actor as the caller named it: an element id or a canonical
    /// identity. Kept verbatim so a report can show what was configured.
    pub actor_ref: String,
    /// The same actor as an endpoint key.
    ///
    /// Normalized on write, because this is what it is compared against:
    /// `Assertion.asserted_by_key` is an endpoint key, and a binding stored in
    /// any other spelling would silently never match.
    pub actor_key: String,
    /// One of [`binding_class`].
    pub binding_class: String,
    /// One of [`assurance`].
    pub assurance: String,
    /// The Space this binding applies in, or `*` for every Space.
    ///
    /// Scoped because representation is not global: an agent that may speak for
    /// an organization inside its project Space should not thereby speak for it
    /// in someone's personal Brain.
    pub scope: String,
    /// One of [`status`].
    pub status: String,
    /// When the record was created.
    pub created_at: String,
    /// When it last changed.
    pub updated_at: String,
    /// When it was revoked; empty while it is not.
    pub revoked_at: String,
    /// Bumped on every change.
    pub version: u64,
}

/// Authority conferred on a Principal or group over one Space (§28, §29).
///
/// A Grant is authority state the Nexus evaluates, not a credential: holding a
/// serialized copy proves nothing, because the grantee is identified by the
/// authenticated Principal and not by possession of the record (§30).
///
/// An empty scope list means "every value", which reads as permissive and is:
/// the restriction that matters is `actions`, and a Grant with no actions
/// confers nothing at all.
#[derive(Clone, Debug, Default, Deserialize, Serialize, AndaDBSchema)]
pub struct GrantRow {
    /// The row id, which also mints `kip:grant:{_id}`.
    pub _id: u64,
    /// The Space whose authority this confers. Exactly one, never a pattern:
    /// a Space is the authorization boundary, and URI structure confers
    /// nothing (§22).
    pub space_id: String,
    /// The grantee Principal; empty when the grantee is a group.
    pub grantee_principal: String,
    /// The grantee group; empty when the grantee is a Principal.
    pub grantee_group: String,
    /// The permission names conferred.
    pub actions: Vec<String>,
    /// The [`AuthorityScope`] this Grant is bounded to.
    pub scope: Json,
    /// The [`AuthorityConditions`] that must hold at decision time.
    pub conditions: Json,
    /// The [`AuthorityConstraints`] every allowed operation carries.
    pub constraints: Json,
    /// Whether the grantee may delegate any part of this (§34: false by
    /// default, because uncontrolled sub-agent spawning must not multiply
    /// privilege).
    pub delegation_allowed: bool,
    /// One of [`status`].
    pub status: String,
    /// The Principal that created the Grant, for separation-of-duties checks.
    pub granted_by: String,
    /// When the record was created.
    pub created_at: String,
    /// When it last changed.
    pub updated_at: String,
    /// When it was revoked; empty while it is not.
    pub revoked_at: String,
    /// Bumped on every change.
    pub version: u64,
}

/// One Principal conferring part of *its own* authority on another (§32).
///
/// Kept apart from [`GrantRow`] because the two are evaluated differently, and
/// collapsing them would lose the difference. A Grant stands on its own; a
/// Delegation is only ever as good as its delegator's authority *right now* —
/// if the parent is revoked, the child stops working even though its own record
/// still says `active` and its own expiry is still in the future (§35).
#[derive(Clone, Debug, Default, Deserialize, Serialize, AndaDBSchema)]
pub struct DelegationRow {
    /// The row id, which also mints `kip:delegation:{_id}`.
    pub _id: u64,
    /// The Space this delegation acts in.
    pub space_id: String,
    /// The Principal conferring the authority.
    pub delegator_principal: String,
    /// The Principal receiving it.
    pub delegate_principal: String,
    /// The permission names conferred. Attenuated against the delegator's
    /// effective authority at decision time, not merely at creation time.
    pub actions: Vec<String>,
    /// The [`AuthorityScope`] this delegation is bounded to.
    pub scope: Json,
    /// The [`AuthorityConditions`] that must hold at decision time.
    pub conditions: Json,
    /// The [`AuthorityConstraints`] every allowed operation carries.
    pub constraints: Json,
    /// The delegation this one descends from, when it is a re-delegation.
    pub parent_delegation: String,
    /// Whether the delegate may re-delegate. Non-transitive by default (§34).
    pub may_redelegate: bool,
    /// One of [`status`].
    pub status: String,
    /// When the record was created.
    pub created_at: String,
    /// When it last changed.
    pub updated_at: String,
    /// When it was revoked; empty while it is not.
    pub revoked_at: String,
    /// Bumped on every change.
    pub version: u64,
}

/// One immutable version of a Governance Policy (§44, §46).
///
/// Appended, never updated, for the same reason the Schema Environment log is:
/// an audit has to be able to answer *which policy version authorized this
/// operation*, and rewriting a policy in place retroactively changes the answer.
///
/// The version in force for a Space is the greatest version of the policy the
/// Space names; the version in force *at a past coordinate* is the greatest one
/// created at or before it.
#[derive(Clone, Debug, Default, Deserialize, Serialize, AndaDBSchema)]
pub struct GovernancePolicyRow {
    /// The row id.
    pub _id: u64,
    /// `{policy_id}@{version}` — the exact identity of one version.
    #[unique]
    pub policy_ref: String,
    /// The stable policy id a Space binds to.
    pub policy_id: String,
    /// The version; monotonic per policy id.
    pub version: u64,
    /// The Space this policy governs, or `*` for a Nexus-wide policy.
    pub space_id: String,
    /// What the policy is for.
    pub description: String,
    /// The ordered [`PolicyStatement`] list.
    pub statements: Vec<Json>,
    /// When this version was created.
    pub created_at: String,
    /// The Principal that created it.
    pub created_by: String,
}

/// A pending or satisfied multi-party approval (§167–§170).
///
/// Approval is control state, not a semantic statement: a Concept saying "Alice
/// approved this" satisfies nothing. What satisfies it is an approving
/// Principal recorded here by an authorized Governance operation.
///
/// `subject_digest` binds the approval to one concrete operation. Without it an
/// approval for "purge this one Evidence record" would authorize purging
/// anything, which is the failure mode §246 tests for.
#[derive(Clone, Debug, Default, Deserialize, Serialize, AndaDBSchema)]
pub struct ApprovalRow {
    /// The row id, which also mints `kip:approval:{_id}`.
    pub _id: u64,
    /// The Space the operation runs in.
    pub space_id: String,
    /// The permission being approved.
    pub operation: String,
    /// The resource it targets.
    pub resource: String,
    /// A digest binding this approval to one concrete request.
    pub subject_digest: String,
    /// How many independent approvals are required.
    pub required: u64,
    /// One entry per approval: `{principal_id, at, note}`.
    pub approvals: Vec<Json>,
    /// The approving Principal ids, lifted out for the separation-of-duties
    /// check and for indexed lookup.
    pub approver_ids: Vec<String>,
    /// Whether the requester may also approve. False by default: the same
    /// Principal proposing and approving is the separation-of-duties failure
    /// §170 names first.
    pub allow_self_approval: bool,
    /// `pending`, `granted`, `denied`, `expired` or `consumed`.
    pub status: String,
    /// The Principal that asked for the approval.
    pub requested_by: String,
    /// When it was requested.
    pub created_at: String,
    /// When it last changed.
    pub updated_at: String,
    /// When it stops being usable; empty for no expiry.
    pub expires_at: String,
    /// Bumped on every change.
    pub version: u64,
}

/// One append-preserved Governance audit entry (§172–§175).
///
/// Two things are recorded here and deliberately tagged apart:
///
/// ```text
/// mutation   the control plane changed, and this is the whole new record
/// decision   an authorization decision was made, and this is what decided it
/// ```
///
/// Corrections append; nothing here is ever rewritten. An audit that could be
/// edited would answer §247 — *which policy version authorized this?* — with
/// today's answer rather than the one that was true.
#[derive(Clone, Debug, Default, Deserialize, Serialize, AndaDBSchema)]
pub struct GovernanceAuditRow {
    /// The row id, which also mints `kip:audit:{_id}`.
    pub _id: u64,
    /// `mutation` or `decision`.
    pub entry_class: String,
    /// When it happened.
    pub at: String,
    /// The Space it concerned, or `*` for a Nexus-wide record.
    pub space_id: String,
    /// The acting Principal.
    pub principal_id: String,
    /// The delegation chain the operation ran under, delegator-first.
    pub delegation_chain: Vec<String>,
    /// The permission or Governance operation name.
    pub operation: String,
    /// What it acted on.
    pub resource: String,
    /// `allow`, `allow_with_constraints`, `deny`, `require_approval`, or the
    /// mutation verb for a `mutation` entry.
    pub decision: String,
    /// Why, in one line. Safe to show a caller (§267).
    pub reason: String,
    /// The policy id that decided it.
    pub policy_id: String,
    /// That policy's version.
    pub policy_version: u64,
    /// The Grant and Delegation ids that matched.
    pub authorities_used: Vec<String>,
    /// The Approval ids that were consumed.
    pub approvals: Vec<String>,
    /// The obligations the decision carried.
    pub obligations: Json,
    /// For a `mutation` entry: the complete new record, as stored.
    pub record: Json,
    /// The request this belonged to, when there was one.
    pub request_id: String,
    /// The cognitive transaction it authorized, when there was one.
    pub tx_id: String,
}

// ---------------------------------------------------------------------------
// The shared shapes stored in the Json columns
// ---------------------------------------------------------------------------

/// What a Grant or Delegation is bounded to (§90).
///
/// Every list is "empty means every value". That is permissive by design: the
/// action list is what confers authority, and a scope is a *narrowing* of it.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct AuthorityScope {
    /// Core element kinds in scope.
    pub kinds: Vec<String>,
    /// Schema symbol references in scope.
    pub schema_refs: Vec<String>,
    /// Classification labels in scope.
    pub classifications: Vec<String>,
    /// Specific element ids in scope, when the authority is that narrow.
    pub elements: Vec<String>,
}

impl AuthorityScope {
    /// Whether `other` stays inside this scope.
    ///
    /// An empty list here means "no restriction", so anything narrows it; a
    /// non-empty list requires the child to be a subset. An empty child list
    /// against a non-empty parent list is *not* a subset — it means "every
    /// value", which is exactly what attenuation must refuse (§31).
    pub fn contains(&self, other: &Self) -> bool {
        narrows(&self.kinds, &other.kinds)
            && narrows(&self.schema_refs, &other.schema_refs)
            && narrows(&self.classifications, &other.classifications)
            && narrows(&self.elements, &other.elements)
    }

    /// The intersection of two scopes, as the effective bound of a chain.
    pub fn intersect(&self, other: &Self) -> Self {
        Self {
            kinds: intersect(&self.kinds, &other.kinds),
            schema_refs: intersect(&self.schema_refs, &other.schema_refs),
            classifications: intersect(&self.classifications, &other.classifications),
            elements: intersect(&self.elements, &other.elements),
        }
    }
}

/// What must hold at decision time for an authority to apply (§29, §38).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct AuthorityConditions {
    /// The purposes this authority is limited to; empty means any purpose.
    pub purpose: Vec<String>,
    /// The least purpose assurance accepted, from [`purpose_assurance`].
    pub min_purpose_assurance: String,
    /// The least authentication strength accepted, from [`auth_strength`].
    pub min_auth_strength: String,
    /// Not in force before this instant; empty for no lower bound.
    pub valid_from: String,
    /// Not in force at or after this instant; empty for no upper bound.
    pub valid_until: String,
}

impl AuthorityConditions {
    /// Whether `other` is at least as restrictive as this.
    ///
    /// Time is the subtle one: a child that outlives its parent is the classic
    /// delegation amplification (§238), so an empty child `valid_until` against
    /// a bounded parent fails.
    pub fn contains(&self, other: &Self) -> bool {
        narrows(&self.purpose, &other.purpose)
            && purpose_assurance::rank(&other.min_purpose_assurance)
                >= purpose_assurance::rank(&self.min_purpose_assurance)
            && auth_strength::rank(&other.min_auth_strength)
                >= auth_strength::rank(&self.min_auth_strength)
            && at_least(&self.valid_from, &other.valid_from)
            && at_most(&self.valid_until, &other.valid_until)
    }
}

/// What every operation an authority allows must carry (§39).
///
/// These are not permissions; they ride along with an allow. A Grant that
/// allows `read` with `fields: ["summary"]` allows the read *and* narrows what
/// comes back.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct AuthorityConstraints {
    /// The element fields that may be returned; empty means every field.
    pub fields: Vec<String>,
    /// The most rows a read may return; absent means the engine's own limit.
    #[serde(deserialize_with = "lenient_max_results")]
    pub max_results: Option<u64>,
    /// The highest influence authority reachable, from the authority classes.
    pub max_influence_authority: String,
    /// The highest classification readable; empty means the Space default.
    pub max_classification: String,
    /// Whether the result may leave the Space at all (§78).
    pub export: bool,
}

/// Reads `max_results`, treating anything that is not a non-negative integer as
/// the tightest cap there is.
///
/// Fails closed rather than open. These blobs are parsed with
/// `unwrap_or_default`, so a `max_results` serde cannot read would otherwise
/// take the whole constraint set down with it and hand back an *unrestricted*
/// authority — a malformed bound widening exactly what it was written to
/// narrow. Matches `asConstraints` in the JavaScript engine.
fn lenient_max_results<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(match Option::<Json>::deserialize(deserializer)? {
        None | Some(Json::Null) => None,
        Some(Json::Number(number)) => Some(number.as_u64().unwrap_or(0)),
        Some(_) => Some(0),
    })
}

impl AuthorityConstraints {
    /// The stricter of two constraint sets — what a chain actually allows.
    pub fn tighten(&self, other: &Self) -> Self {
        Self {
            fields: intersect_fields(&self.fields, &other.fields),
            max_results: match (self.max_results, other.max_results) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None) => Some(a),
                (None, b) => b,
            },
            max_influence_authority: lower_named(
                &self.max_influence_authority,
                &other.max_influence_authority,
                crate::governance::authority::rank,
            ),
            max_classification: lower_named(
                &self.max_classification,
                &other.max_classification,
                crate::governance::classification::rank,
            ),
            export: self.export && other.export,
        }
    }

    /// Whether `other` stays inside every constraint imposed by this one.
    pub fn contains(&self, other: &Self) -> bool {
        let bounded_results = self.max_results.is_none()
            || other
                .max_results
                .is_some_and(|child| child <= self.max_results.unwrap_or(u64::MAX));
        let bounded_authority = self.max_influence_authority.is_empty()
            || (!other.max_influence_authority.is_empty()
                && crate::governance::authority::rank(&other.max_influence_authority)
                    <= crate::governance::authority::rank(&self.max_influence_authority));
        let bounded_classification = self.max_classification.is_empty()
            || (!other.max_classification.is_empty()
                && crate::governance::classification::rank(&other.max_classification)
                    <= crate::governance::classification::rank(&self.max_classification));
        narrows(&self.fields, &other.fields)
            && bounded_results
            && bounded_authority
            && bounded_classification
            && (self.export || !other.export)
    }
}

/// One ordered rule in a Policy (§45).
///
/// `deny` wins over `allow` regardless of order (§42); the order matters only
/// for reading the policy, never for resolving it. Making order significant
/// would mean a policy's meaning depends on where somebody appended a rule.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct PolicyStatement {
    /// `allow` or `deny`.
    pub effect: String,
    /// The Principal ids this statement matches; empty means every Principal.
    pub principals: Vec<String>,
    /// The groups it matches; empty means every group.
    pub groups: Vec<String>,
    /// The permission names it matches; empty means every permission.
    pub actions: Vec<String>,
    /// The resources it matches.
    pub resource: AuthorityScope,
    /// The conditions under which it applies.
    pub conditions: AuthorityConditions,
    /// The constraints an `allow` carries.
    pub constraints: AuthorityConstraints,
    /// The obligations an `allow` carries.
    pub obligations: PolicyObligations,
}

/// What an allow requires the runtime to do as well (§184).
///
/// An obligation that cannot be satisfied denies the operation. A policy that
/// requires an audit record, on a runtime whose audit is unavailable, must not
/// quietly proceed without one.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(default)]
pub struct PolicyObligations {
    /// Whether the decision must be written to the audit log.
    pub audit: bool,
    /// How many independent approvals the operation needs.
    pub approvals_required: u64,
    /// A named redaction profile to apply to the result.
    pub redaction_profile: String,
}

impl PolicyObligations {
    /// The union of two obligation sets — obligations only ever accumulate.
    pub fn merge(&self, other: &Self) -> Self {
        Self {
            audit: self.audit || other.audit,
            approvals_required: self.approvals_required.max(other.approvals_required),
            redaction_profile: if other.redaction_profile.is_empty() {
                self.redaction_profile.clone()
            } else {
                other.redaction_profile.clone()
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Set helpers
// ---------------------------------------------------------------------------

/// Whether `child` stays inside `parent`, where empty means "unrestricted".
fn narrows(parent: &[String], child: &[String]) -> bool {
    if parent.is_empty() {
        return true;
    }
    !child.is_empty() && child.iter().all(|value| parent.contains(value))
}

/// The intersection of two "empty means unrestricted" lists.
fn intersect(a: &[String], b: &[String]) -> Vec<String> {
    if a.is_empty() {
        return b.to_vec();
    }
    if b.is_empty() {
        return a.to_vec();
    }
    a.iter().filter(|v| b.contains(v)).cloned().collect()
}

/// The intersection of two field allowlists.
///
/// Identical to [`intersect`] today; kept separate because a field list is an
/// allowlist over a fixed vocabulary while a scope list is a filter over open
/// values, and the two will not stay the same shape.
fn intersect_fields(a: &[String], b: &[String]) -> Vec<String> {
    intersect(a, b)
}

/// Whether `child` starts no earlier than `parent`, with empty meaning "no
/// lower bound".
fn at_least(parent: &str, child: &str) -> bool {
    parent.is_empty() || (!child.is_empty() && child >= parent)
}

/// Whether `child` ends no later than `parent`, with empty meaning "no upper
/// bound".
fn at_most(parent: &str, child: &str) -> bool {
    parent.is_empty() || (!child.is_empty() && child <= parent)
}

/// The lower of two ranked names, treating empty as "no ceiling stated".
fn lower_named(a: &str, b: &str, rank: fn(&str) -> u8) -> String {
    match (a.is_empty(), b.is_empty()) {
        (true, _) => b.to_string(),
        (_, true) => a.to_string(),
        _ if rank(a) <= rank(b) => a.to_string(),
        _ => b.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope(kinds: &[&str]) -> AuthorityScope {
        AuthorityScope {
            kinds: kinds.iter().map(|k| k.to_string()).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn an_unrestricted_parent_contains_everything() {
        assert!(AuthorityScope::default().contains(&scope(&["concept"])));
        assert!(AuthorityScope::default().contains(&AuthorityScope::default()));
    }

    #[test]
    fn a_child_may_not_widen_a_restricted_parent() {
        let parent = scope(&["concept"]);
        assert!(parent.contains(&scope(&["concept"])));
        assert!(!parent.contains(&scope(&["concept", "evidence"])));
        // "Every kind" is the widening §31 exists to refuse, so an empty child
        // list against a bounded parent must not pass.
        assert!(!parent.contains(&AuthorityScope::default()));
    }

    #[test]
    fn a_child_delegation_may_not_outlive_its_parent() {
        let parent = AuthorityConditions {
            valid_until: "2026-09-01T00:00:00.000Z".into(),
            ..Default::default()
        };
        let inside = AuthorityConditions {
            valid_until: "2026-08-20T00:00:00.000Z".into(),
            ..Default::default()
        };
        let beyond = AuthorityConditions {
            valid_until: "2027-01-01T00:00:00.000Z".into(),
            ..Default::default()
        };
        assert!(parent.contains(&inside));
        assert!(!parent.contains(&beyond));
        // §238: "read + export, valid 1 year" under a one-day parent.
        assert!(!parent.contains(&AuthorityConditions::default()));
    }

    #[test]
    fn a_child_may_not_lower_the_authentication_bar() {
        let parent = AuthorityConditions {
            min_auth_strength: auth_strength::STRONG.into(),
            ..Default::default()
        };
        assert!(parent.contains(&AuthorityConditions {
            min_auth_strength: auth_strength::STRONG.into(),
            ..Default::default()
        }));
        assert!(!parent.contains(&AuthorityConditions {
            min_auth_strength: auth_strength::STANDARD.into(),
            ..Default::default()
        }));
    }

    #[test]
    fn an_unknown_authentication_strength_is_the_weakest_rung() {
        // A deployment that invents a name must not have it satisfy every bar.
        assert_eq!(auth_strength::rank("quantum-grade"), 0);
        assert!(
            auth_strength::rank("quantum-grade") < auth_strength::rank(auth_strength::STANDARD)
        );
    }

    #[test]
    fn constraints_only_ever_tighten() {
        let broad = AuthorityConstraints {
            max_results: Some(1000),
            export: true,
            ..Default::default()
        };
        let narrow = AuthorityConstraints {
            fields: vec!["summary".into()],
            max_results: Some(10),
            export: false,
            ..Default::default()
        };
        let effective = broad.tighten(&narrow);
        assert_eq!(effective.max_results, Some(10));
        assert_eq!(effective.fields, vec!["summary".to_string()]);
        assert!(!effective.export);
        assert!(broad.contains(&narrow));
        assert!(!narrow.contains(&AuthorityConstraints::default()));
    }

    #[test]
    fn obligations_only_ever_accumulate() {
        let a = PolicyObligations {
            audit: true,
            ..Default::default()
        };
        let b = PolicyObligations {
            approvals_required: 2,
            redaction_profile: "safe-summary".into(),
            ..Default::default()
        };
        let merged = a.merge(&b);
        assert!(merged.audit);
        assert_eq!(merged.approvals_required, 2);
        assert_eq!(merged.redaction_profile, "safe-summary");
    }
}
