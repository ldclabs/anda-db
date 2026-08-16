//! # The permission registry
//!
//! One name per distinction the protocol requires an implementation to keep
//! (Spec §29, Governance §52–§89, §249). The names may be refined; the
//! distinctions may not, and every one of them exists because collapsing it
//! silently widens authority:
//!
//! ```text
//! read      ≠ export          reading is not taking a copy away (§78)
//! read      ≠ discover        knowing a thing exists is its own disclosure
//! update    ≠ declassify      a writer must not relabel sensitivity
//! assert    ≠ assert_as_actor recording a claim is not impersonation (§17)
//! maintain  ≠ manage_policy   a maintenance agent must not widen itself
//! tombstone ≠ purge           logical removal is not erasure
//! ```
//!
//! A Grant lists permissions by these names. An unrecognized name is rejected
//! at Grant creation rather than ignored at decision time: a typo that silently
//! confers nothing is a Grant that looks like authority and is not, and the
//! holder discovers it during an incident.

use anda_kip::KipError;

/// The permission families (Spec §29, Governance §52).
///
/// Families are for explanation — `DESCRIBE ACCESS` groups by them, and a
/// denial can name one without naming a policy. Authorization never resolves on
/// a family; it always resolves on one [`Permission`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Family {
    /// Learning that something exists, and reading it.
    Discovery,
    /// Creating and changing cognitive content.
    CognitiveMutation,
    /// Committing to, withdrawing or moderating epistemic claims.
    EpistemicMutation,
    /// Binding Principals to semantic actors, and merging identities.
    Identity,
    /// Consolidation, archival and other custodial work.
    Maintenance,
    /// Moving cognition across a Space boundary.
    Sharing,
    /// Retention, legal holds, erasure and declassification.
    Lifecycle,
    /// Changing the control plane itself.
    Governance,
    /// Raising or lowering how strongly memory may influence action.
    Authority,
    /// Reading what the control plane recorded.
    Audit,
}

impl Family {
    /// The family's wire name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Discovery => "discovery",
            Self::CognitiveMutation => "cognitive_mutation",
            Self::EpistemicMutation => "epistemic_mutation",
            Self::Identity => "identity",
            Self::Maintenance => "maintenance",
            Self::Sharing => "sharing",
            Self::Lifecycle => "lifecycle",
            Self::Governance => "governance",
            Self::Authority => "authority",
            Self::Audit => "audit",
        }
    }
}

macro_rules! permissions {
    ($( $variant:ident => $name:literal, $family:ident, $doc:literal );* $(;)?) => {
        /// One protected operation.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        pub enum Permission {
            $(
                #[doc = $doc]
                $variant,
            )*
        }

        impl Permission {
            /// Every permission this engine knows, in registry order.
            pub const ALL: &'static [Permission] = &[$( Permission::$variant ),*];

            /// The permission's wire name, as a Grant spells it.
            pub fn as_str(self) -> &'static str {
                match self {
                    $( Self::$variant => $name, )*
                }
            }

            /// Which family it belongs to.
            pub fn family(self) -> Family {
                match self {
                    $( Self::$variant => Family::$family, )*
                }
            }

            /// What the permission allows, in one line.
            pub fn description(self) -> &'static str {
                match self {
                    $( Self::$variant => $doc, )*
                }
            }

            /// Resolves a wire name.
            ///
            /// Unknown names fail rather than being dropped: a Grant naming a
            /// permission this engine does not implement confers nothing, and
            /// the holder must learn that when the Grant is written.
            pub fn parse(name: &str) -> Result<Self, KipError> {
                match name {
                    $( $name => Ok(Self::$variant), )*
                    other => Err(KipError::not_authorized(format!(
                        "{other:?} is not a permission this engine implements; \
                         DESCRIBE ACCESS lists the registry"
                    ))),
                }
            }
        }
    };
}

permissions! {
    // Discovery / Read (§53–§58)
    Discover => "discover", Discovery,
        "learn that an element or match exists";
    Read => "read", Discovery,
        "read the permitted content fields of a known element";
    Search => "search", Discovery,
        "retrieve associatively over the authorized search universe";
    Project => "project", Discovery,
        "run an Epistemic Projection under a permitted policy";
    ReadRawOrigin => "read_raw_origin", Discovery,
        "read engine origin: which Principal and channel wrote an element";
    ReadHistory => "read_history", Discovery,
        "read past element versions and change streams";

    // Cognitive mutation (§59–§62)
    Create => "create", CognitiveMutation,
        "create Concepts, Propositions, Evidence and Activities";
    Update => "update", CognitiveMutation,
        "change mutable, non-protected fields of an existing element";
    Derive => "derive", CognitiveMutation,
        "create derived output from content already read, under propagation rules";

    // Epistemic mutation (§63–§70)
    Assert => "assert", EpistemicMutation,
        "record one's own epistemic commitment";
    RecordAttributedAssertion => "record_attributed_assertion", EpistemicMutation,
        "record that another actor stated or believed something, with provenance";
    AssertAsActor => "assert_as_actor", EpistemicMutation,
        "exercise a bound actor's representation authority";
    RetractOwn => "retract_own", EpistemicMutation,
        "retract an Assertion one is authorized to represent";
    SupersedeOwn => "supersede_own", EpistemicMutation,
        "supersede an Assertion one is authorized to represent";
    ModerateAssertion => "moderate_assertion", EpistemicMutation,
        "administratively exclude a third party's Assertion without claiming they retracted it";

    // Identity (§71–§74)
    ManageActorBinding => "manage_actor_binding", Identity,
        "create, change or revoke the binding between a Principal and a semantic actor";
    BindCanonicalIdentity => "bind_canonical_identity", Identity,
        "attach a canonical identity to a Concept";
    MergeIdentity => "merge_identity", Identity,
        "consolidate two Concepts into one identity";

    // Maintenance (§75)
    Maintain => "maintain", Maintenance,
        "perform custodial consolidation and repair";
    Archive => "archive", Maintenance,
        "remove an element from ordinary recall, keeping it readable";
    Quarantine => "quarantine", Maintenance,
        "place an element in a state ordinary recall excludes, without claiming retraction";
    Tombstone => "tombstone", Maintenance,
        "logically delete an element, keeping its identity and references";

    // Sharing (§76–§79)
    Import => "import", Sharing,
        "accept another Brain's cognition into this Space";
    Export => "export", Sharing,
        "take cognition out of the Space";
    Share => "share", Sharing,
        "expose a controlled view of this Space to another";

    // Lifecycle (§80–§82, §88, §100)
    ManageRetention => "manage_retention", Lifecycle,
        "set or change how long an element is retained";
    LegalHold => "legal_hold", Lifecycle,
        "place or lift a hold that blocks erasure";
    Purge => "purge", Lifecycle,
        "physically erase an element and its retained history";
    Declassify => "declassify", Lifecycle,
        "lower an element's classification";

    // Governance (§83–§86)
    ManageMembership => "manage_membership", Governance,
        "change who belongs to a Principal group";
    ManageGrants => "manage_grants", Governance,
        "create or revoke Grants in this Space";
    ManageDelegation => "manage_delegation", Governance,
        "create or revoke Delegations in this Space";
    Delegate => "delegate", Governance,
        "confer part of one's own authority on another Principal";
    ManagePolicy => "manage_policy", Governance,
        "publish a new version of the Space's Governance Policy";
    ManageTrust => "manage_trust", Governance,
        "bind or version the trust policy the projection reads";
    ManageSchema => "manage_schema", Governance,
        "install a Schema Package or activate a Schema Lock";

    // Authority (§87, §129)
    ElevateAuthority => "elevate_authority", Authority,
        "raise how strongly a memory may influence action";
    ApproveHighRisk => "approve_high_risk", Authority,
        "supply one of the independent approvals a high-risk operation needs";

    // Audit (§89)
    ReadAudit => "read_audit", Audit,
        "read the Governance audit log";
    ReadGovernanceHistory => "read_governance_history", Audit,
        "read past Governance state: who had access, under which policy version";
}

impl std::fmt::Display for Permission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Permission {
    /// Whether this permission is high-impact enough that a Space's audit
    /// obligation applies to it even when no policy statement says so (§172).
    ///
    /// The list is the one §172 enumerates: changing the control plane, moving
    /// cognition across the Space boundary, erasing, and raising authority. A
    /// deployment may audit more; it may not audit less, because these are the
    /// operations whose absence from a log is itself the incident.
    pub fn is_always_audited(self) -> bool {
        matches!(
            self.family(),
            Family::Governance | Family::Authority | Family::Identity
        ) || matches!(
            self,
            Self::Import | Self::Export | Self::Share | Self::Purge | Self::LegalHold
        )
    }

    /// Whether cognitive content may ever be the basis for allowing this.
    ///
    /// Never, for anything. The method exists so the rule has a name and one
    /// place to be read from (§48, §24 of the invariants): cognitive content
    /// may *restrict* authority and must never expand it.
    pub fn grantable_by_cognitive_content(self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn every_permission_has_a_distinct_name() {
        let names: HashSet<&str> = Permission::ALL.iter().map(|p| p.as_str()).collect();
        assert_eq!(names.len(), Permission::ALL.len());
    }

    #[test]
    fn a_name_round_trips_through_the_registry() {
        for permission in Permission::ALL {
            assert_eq!(Permission::parse(permission.as_str()).unwrap(), *permission);
        }
    }

    #[test]
    fn an_unimplemented_permission_name_is_refused_not_ignored() {
        // A Grant that silently confers nothing is worse than one that fails:
        // the holder finds out during an incident instead of at write time.
        let err = Permission::parse("read_everything").unwrap_err();
        assert_eq!(err.name(), "NotAuthorized");
    }

    #[test]
    fn the_registry_keeps_the_distinctions_the_spec_requires() {
        // §271: these pairs are the core governance equations. If a refactor
        // ever merges one of them, this test is what says so.
        for (a, b) in [
            (Permission::Read, Permission::Export),
            (Permission::Read, Permission::Discover),
            (Permission::Update, Permission::Declassify),
            (Permission::Assert, Permission::AssertAsActor),
            (
                Permission::RecordAttributedAssertion,
                Permission::AssertAsActor,
            ),
            (Permission::Maintain, Permission::ManagePolicy),
            (Permission::Tombstone, Permission::Purge),
            (Permission::RetractOwn, Permission::ModerateAssertion),
        ] {
            assert_ne!(a, b);
            assert_ne!(a.as_str(), b.as_str());
        }
    }

    #[test]
    fn changing_the_control_plane_is_always_audited() {
        assert!(Permission::ManagePolicy.is_always_audited());
        assert!(Permission::ElevateAuthority.is_always_audited());
        assert!(Permission::Purge.is_always_audited());
        assert!(Permission::Export.is_always_audited());
        // An ordinary read is audited only where a policy asks for it (§173).
        assert!(!Permission::Read.is_always_audited());
    }
}
