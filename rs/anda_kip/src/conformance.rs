//! # Conformance profiles (Spec §89–§102)
//!
//! > An implementation MUST declare which KIP 2.0 conformance profiles it
//! > supports.
//!
//! The declaration is a runtime's, not a library's: whether a Space offers
//! serializable transactions or retains historical snapshots is a property of
//! the engine behind [`crate::Executor`], and this crate holds no state to
//! decide it with. What it can do is give that declaration a shape — the
//! profile names, spelled the way §89 spells them — so an engine answering
//! `DESCRIBE CAPABILITIES` and a client reading the answer are talking about
//! the same list.
//!
//! What this crate does supply is [`PROTOCOL_SURFACE`]: the profiles whose
//! *language surface* it implements. Parsing every KQL statement family is a
//! necessary part of `KIP-KQL` and not a sufficient one — the rest of that
//! profile is evaluation, which belongs to the engine. Reporting the two
//! separately is the difference between "this parses" and "this works".

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

/// A KIP 2.0 conformance profile (Spec §89).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ConformanceProfile {
    /// Core elements, envelope, identity, immutability, merge (§90).
    Core,
    /// Schema Packages and the Schema Environment (§91).
    Schema,
    /// Projection, policies, open-world semantics (§92).
    Epistemic,
    /// Principals, permissions, classification, authority (§93).
    Governance,
    /// Atomicity, idempotency, receipts, preconditions (§94).
    Transactions,
    /// Capsule export, verification, import pipeline (§95).
    Capsule,
    /// The read language (§96).
    Kql,
    /// The mutation language (§97).
    Kml,
    /// Introspection, grounding, verification, preview (§98).
    Meta,
    /// The request/response envelope and execution modes (§99).
    Runtime,
    /// `AS OF`, history, change cursors (§100).
    Historical,
    /// Proofs, signatures, checkpoints (§101).
    HighAssurance,
    /// KIP 1.x migration and compatibility, required only of an
    /// implementation that claims it (§103).
    Migration1x,
}

impl ConformanceProfile {
    /// Every profile §89 names, in the order it names them.
    pub const ALL: &'static [ConformanceProfile] = &[
        ConformanceProfile::Core,
        ConformanceProfile::Schema,
        ConformanceProfile::Epistemic,
        ConformanceProfile::Governance,
        ConformanceProfile::Transactions,
        ConformanceProfile::Capsule,
        ConformanceProfile::Kql,
        ConformanceProfile::Kml,
        ConformanceProfile::Meta,
        ConformanceProfile::Runtime,
        ConformanceProfile::Historical,
        ConformanceProfile::HighAssurance,
        ConformanceProfile::Migration1x,
    ];

    /// The wire name, e.g. `"KIP-High-Assurance"`.
    pub fn name(&self) -> &'static str {
        match self {
            ConformanceProfile::Core => "KIP-Core",
            ConformanceProfile::Schema => "KIP-Schema",
            ConformanceProfile::Epistemic => "KIP-Epistemic",
            ConformanceProfile::Governance => "KIP-Governance",
            ConformanceProfile::Transactions => "KIP-Transactions",
            ConformanceProfile::Capsule => "KIP-Capsule",
            ConformanceProfile::Kql => "KIP-KQL",
            ConformanceProfile::Kml => "KIP-KML",
            ConformanceProfile::Meta => "KIP-META",
            ConformanceProfile::Runtime => "KIP-Runtime",
            ConformanceProfile::Historical => "KIP-Historical",
            ConformanceProfile::HighAssurance => "KIP-High-Assurance",
            ConformanceProfile::Migration1x => "KIP-1-Migration",
        }
    }

    /// Looks a profile up by its wire name.
    pub fn from_name(name: &str) -> Option<Self> {
        ConformanceProfile::ALL
            .iter()
            .copied()
            .find(|profile| profile.name() == name)
    }
}

impl fmt::Display for ConformanceProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for ConformanceProfile {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ConformanceProfile::from_name(s)
            .ok_or_else(|| format!("unknown KIP 2.0 conformance profile {s:?}"))
    }
}

impl Serialize for ConformanceProfile {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.name())
    }
}

impl<'de> Deserialize<'de> for ConformanceProfile {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        ConformanceProfile::from_str(&name).map_err(serde::de::Error::custom)
    }
}

/// The profiles whose **language surface** this crate implements in full.
///
/// Every statement family of these three parses, lowers to the executable AST,
/// and is held to the schema-independent rules — which is what a protocol
/// library can carry of them. Evaluation, Schema resolution, Governance and
/// transactions are the rest of each profile, and belong to the engine; a
/// runtime therefore declares its own list rather than reusing this one.
pub const PROTOCOL_SURFACE: &[ConformanceProfile] = &[
    ConformanceProfile::Kql,
    ConformanceProfile::Kml,
    ConformanceProfile::Meta,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_profile_round_trips_by_its_wire_name() {
        for profile in ConformanceProfile::ALL {
            assert_eq!(
                ConformanceProfile::from_name(profile.name()),
                Some(*profile)
            );
            assert_eq!(
                serde_json::to_string(profile).unwrap(),
                format!("\"{}\"", profile.name())
            );
        }
        assert!(ConformanceProfile::from_name("KIP-Imaginary").is_none());
    }

    #[test]
    fn the_registry_matches_the_specifications_listing() {
        // §89 lists thirteen; a profile added upstream must be added here
        // rather than silently missing from every declaration.
        assert_eq!(ConformanceProfile::ALL.len(), 13);
        let names: Vec<&str> = ConformanceProfile::ALL.iter().map(|p| p.name()).collect();
        assert_eq!(
            names,
            vec![
                "KIP-Core",
                "KIP-Schema",
                "KIP-Epistemic",
                "KIP-Governance",
                "KIP-Transactions",
                "KIP-Capsule",
                "KIP-KQL",
                "KIP-KML",
                "KIP-META",
                "KIP-Runtime",
                "KIP-Historical",
                "KIP-High-Assurance",
                "KIP-1-Migration",
            ]
        );
    }
}
