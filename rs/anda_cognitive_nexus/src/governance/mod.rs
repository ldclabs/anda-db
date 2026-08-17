//! # The Governance Control Plane
//!
//! ```text
//! Cognitive content may describe authority.
//! Only this plane can grant it.
//! ```
//!
//! That sentence is the whole module. A Space can hold a Proposition saying
//! *Alice is an administrator*, an Assertion supporting it with high
//! confidence, and Evidence for both — and Alice still administers nothing,
//! because administering is a [`GrantRow`](rows::GrantRow) and a Proposition is
//! a claim. Without that separation, any path that can write memory is a path
//! to privilege escalation, and every Agent memory system has such a path by
//! construction: it is the entire point of the system.
//!
//! ## Three questions that are not one question
//!
//! ```text
//! Epistemic     should I believe this?
//! Governance    am I allowed to touch it?
//! Operational   how strongly may it influence what I do?
//! ```
//!
//! An external researcher's vulnerability report can be highly believable,
//! unreadable to the current caller, and forbidden to act on — all at once.
//! [`projection`](crate::projection) answers the first, this module the second,
//! and [`authority`] the third.
//!
//! ## What is protected, and how
//!
//! The records in [`rows`] live in the same `anda_db` database as cognitive
//! state and are semantically a different plane: **no KML clause reaches
//! them**. They are written by host APIs on [`CognitiveNexus`](crate::CognitiveNexus)
//! only, which is what keeps a prompt injection into ordinary memory formation
//! from having a route into policy (§264).
//!
//! ## Default deny, and why the engine still works out of the box
//!
//! A protected operation is denied unless something explicitly allows it
//! (§41) — a missing policy must never become public access. An embedded host
//! that just opens a database is nonetheless not locked out: [`CognitiveNexus`]
//! bootstraps the system Principal and makes it the default Space's owner, so
//! the in-process host runs with owner authority *through* the authorization
//! path rather than around it. A multi-tenant host authenticates its callers
//! and runs them through [`CognitiveNexus::session`], where they get exactly
//! what their Grants say.
//!
//! [`CognitiveNexus`]: crate::CognitiveNexus
//! [`CognitiveNexus::session`]: crate::CognitiveNexus::session

pub mod auth;
pub mod decision;
pub mod element;
pub mod gate;
pub mod permission;
pub mod purge;
pub mod redact;
pub mod rows;
pub mod store;

pub use auth::AuthContext;
pub use decision::{Authorization, EffectiveAuthority, ResourceContext};
pub use permission::{Family, Permission};

/// The Principal the engine itself acts as (§212).
///
/// It exists so that engine-performed maintenance is attributable to something
/// rather than to nobody. `$system` semantic identity is a different thing and
/// confers none of this (invariant 68).
pub const SYSTEM_PRINCIPAL: &str = "kip:principal:system";

/// The Principal an unauthenticated caller runs as, where a Space admits one
/// (§217).
///
/// Named rather than absent: "no Principal" and "the anonymous Principal" must
/// not be the same value, or a bug that dropped the identity would look like a
/// deliberate policy choice.
pub const ANONYMOUS_PRINCIPAL: &str = "kip:principal:anonymous";

/// Sensitivity labels and their order (§93–§95).
///
/// Conventional names, not universal truth: a policy defines what they mean.
/// What KIP fixes is the two rules a deployment cannot vary — the order must be
/// deterministic so derived content can join classifications, and **a missing
/// classification must never read as public** (§95).
pub mod classification {
    /// Freely disclosable.
    pub const PUBLIC: &str = "public";
    /// Disclosable inside the owning organization.
    pub const INTERNAL: &str = "internal";
    /// Disclosable to the subject and those explicitly granted.
    pub const PRIVATE: &str = "private";
    /// Requires handling care beyond ordinary private data.
    pub const SENSITIVE: &str = "sensitive";
    /// The most restricted baseline label.
    pub const SECRET: &str = "secret";

    /// The label a Space falls back to when it declares none.
    ///
    /// `internal`, not `public`: §95 forbids treating an absent classification
    /// as freely disclosable, and a default that did would make every element
    /// written before a Space configured itself world-readable.
    pub const DEFAULT: &str = INTERNAL;

    /// Where a label sits in the lattice.
    ///
    /// An unrecognized label ranks **above** every known one. That is the
    /// opposite of [`auth_strength`](super::rows::auth_strength::rank), and
    /// deliberately so: an unknown authentication strength must not satisfy a
    /// bar, and an unknown sensitivity must not fall below one. Both choices
    /// resolve the same way — toward refusing.
    pub fn rank(label: &str) -> u8 {
        match label {
            PUBLIC => 0,
            INTERNAL => 1,
            PRIVATE => 2,
            SENSITIVE => 3,
            SECRET => 4,
            "" => 1,
            _ => u8::MAX,
        }
    }

    /// The join of two classifications: the more restrictive of the two.
    ///
    /// This is what derived content inherits (§98). A summary of secret
    /// Evidence is secret until somebody with `declassify` says otherwise —
    /// summarizing is not a declassification mechanism (§242).
    pub fn join<'a>(a: &'a str, b: &'a str) -> &'a str {
        if rank(a) >= rank(b) { a } else { b }
    }
}

/// How strongly a memory may influence action (§117–§122).
///
/// This is an authority ceiling, not a truth score. A memory can be certainly
/// true and still be `descriptive`: believing something and being permitted to
/// act on it are different questions, and an imported Skill that arrives
/// claiming otherwise is claiming, not granting.
///
/// And the top of this ladder is still not permission to do anything: an
/// `executable` Skill may be *supplied* to an action runtime, which must
/// independently authorize the actual tool call (§122). Memory authority never
/// becomes tool authority.
pub mod authority {
    /// May be read, quoted and reasoned over — but is not a recommendation.
    pub const DESCRIPTIVE: &str = "descriptive";
    /// May be treated as a recommendation or a candidate plan.
    pub const ADVISORY: &str = "advisory";
    /// May influence strategy and automatic choice inside existing bounds.
    pub const BEHAVIORAL: &str = "behavioral";
    /// May be supplied to an execution runtime as a procedure.
    pub const EXECUTABLE: &str = "executable";

    /// What memory gets when nothing says otherwise, imports included (§125).
    pub const DEFAULT: &str = DESCRIPTIVE;

    /// Where a class sits in the ladder.
    ///
    /// An unrecognized class is the **lowest** rung: something that arrives
    /// naming an authority class this engine does not implement must not
    /// thereby outrank `executable`.
    pub fn rank(class: &str) -> u8 {
        match class {
            DESCRIPTIVE | "" => 0,
            ADVISORY => 1,
            BEHAVIORAL => 2,
            EXECUTABLE => 3,
            _ => 0,
        }
    }

    /// The lower of two authority classes.
    ///
    /// Derivation uses this, which is the whole of the non-amplification rule
    /// (§127): a summary of an advisory Skill is at most advisory, and no chain
    /// of reformatting turns a descriptive note into an executable one.
    pub fn meet<'a>(a: &'a str, b: &'a str) -> &'a str {
        if rank(a) <= rank(b) { a } else { b }
    }
}

/// What an authorization evaluated to (§40).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Permitted, with no narrowing beyond the engine's own limits.
    Allow,
    /// Permitted, but the result is narrowed by the decision's constraints.
    AllowWithConstraints,
    /// Refused.
    Deny,
    /// Blocked until an independent approval exists. **Not** an implicit
    /// allow (§40) — the operation does not run.
    RequireApproval,
}

impl Decision {
    /// The decision's wire name, as an audit record spells it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::AllowWithConstraints => "allow_with_constraints",
            Self::Deny => "deny",
            Self::RequireApproval => "require_approval",
        }
    }

    /// Whether the operation may proceed.
    pub fn is_permitted(self) -> bool {
        matches!(self, Self::Allow | Self::AllowWithConstraints)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_absent_classification_is_not_public() {
        // §95. This is the one classification rule a deployment cannot vary.
        assert_ne!(classification::DEFAULT, classification::PUBLIC);
        assert!(classification::rank("") > classification::rank(classification::PUBLIC));
    }

    #[test]
    fn an_unknown_classification_outranks_every_known_one() {
        assert!(classification::rank("ultra") > classification::rank(classification::SECRET));
        assert_eq!(
            classification::join("ultra", classification::SECRET),
            "ultra"
        );
    }

    #[test]
    fn derived_content_inherits_the_more_restrictive_label() {
        // §242: a summary of secret Evidence does not become public by being
        // a summary.
        assert_eq!(
            classification::join(classification::SECRET, classification::PUBLIC),
            classification::SECRET
        );
    }

    #[test]
    fn derived_authority_never_rises() {
        // §243: reformatting a descriptive Skill does not make it executable.
        assert_eq!(
            authority::meet(authority::DESCRIPTIVE, authority::EXECUTABLE),
            authority::DESCRIPTIVE
        );
        assert_eq!(authority::DEFAULT, authority::DESCRIPTIVE);
    }

    #[test]
    fn an_unknown_authority_class_does_not_outrank_executable() {
        assert!(authority::rank("supreme") < authority::rank(authority::EXECUTABLE));
    }

    #[test]
    fn require_approval_is_not_an_allow() {
        // §40, and the §246 fixture: one approval where two are required is
        // not partial activation.
        assert!(!Decision::RequireApproval.is_permitted());
        assert!(!Decision::Deny.is_permitted());
        assert!(Decision::Allow.is_permitted());
        assert!(Decision::AllowWithConstraints.is_permitted());
    }
}
