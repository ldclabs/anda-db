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

use std::sync::LazyLock;

use serde::Deserialize;

wire_enum! {
    /// A KIP 2.0 conformance profile (Spec §89).
    ///
    /// Ordered so a declared set can be held in a `BTreeSet` and printed in
    /// one order; §89's listing order is the one the variants are declared in.
    #[derive(PartialOrd, Ord)]
    pub enum ConformanceProfile {
        /// Core elements, envelope, identity, immutability, merge (§90).
        Core = "KIP-Core",
        /// Schema Packages and the Schema Environment (§91).
        Schema = "KIP-Schema",
        /// Projection, policies, open-world semantics (§92).
        Epistemic = "KIP-Epistemic",
        /// Principals, permissions, classification, authority (§93).
        Governance = "KIP-Governance",
        /// Atomicity, idempotency, receipts, preconditions (§94).
        Transactions = "KIP-Transactions",
        /// The read language (§96).
        Kql = "KIP-KQL",
        /// The mutation language (§97).
        Kml = "KIP-KML",
        /// Introspection, grounding, verification, preview (§98).
        Meta = "KIP-META",
        /// The request/response envelope and execution modes (§99).
        Runtime = "KIP-Runtime",
    }
}

impl ConformanceProfile {
    /// The wire name, e.g. `"KIP-Governance"`.
    ///
    /// The spelling a profile is declared under is its name in §89, so this
    /// reads better at call sites than [`Self::as_str`] does; they are the
    /// same string.
    pub const fn name(&self) -> &'static str {
        self.as_str()
    }

    /// Looks a profile up by its wire name.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::from_wire(name)
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

/// The capability vocabulary every engine in this repository answers (§67.4).
///
/// Shipped as `capabilities.json` beside the Specification, and read by the
/// TypeScript engine through the same file, because the failure this prevents
/// is cross-engine: a name one engine answers and the other has never heard of
/// is refused as `UnsupportedCapability` even where the capability is built,
/// and the caller cannot tell that apart from a real gap.
static CAPABILITY_NAMES: LazyLock<CapabilityNames> =
    LazyLock::new(|| serde_json::from_str(include_str!("../capabilities.json")).unwrap());

#[derive(Deserialize)]
struct CapabilityNames {
    registry: Vec<String>,
    engine: Vec<String>,
}

/// The §67.4 registry names, in the Specification's order.
///
/// A runtime MUST NOT rename these and MUST answer for each; it MAY add
/// namespaced entries of its own.
pub fn capability_registry_names() -> &'static [String] {
    &CAPABILITY_NAMES.registry
}

/// The engine-local capability names every engine here answers, sorted.
///
/// Membership is a promise to answer, not a claim of support: an engine
/// partitions this list into what it implements and what it does not, and
/// `requires` gets `true` or `false` for every name rather than the
/// `unrecognized` §67.4 makes a failure.
pub fn capability_engine_names() -> &'static [String] {
    &CAPABILITY_NAMES.engine
}

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

    /// The §67.4 registry is the Specification's list, read from the
    /// Specification.
    ///
    /// Transcribing 24 names by hand produces a list that compiles and is
    /// quietly one rename behind; the spec ships with this crate, so the test
    /// can read the source instead of a copy of it.
    #[test]
    fn the_capability_registry_is_the_one_the_specification_prints() {
        let spec = include_str!("../SPECIFICATION.md");
        let section = spec
            .split("## 67.4 Capability registry")
            .nth(1)
            .expect("§67.4 is in the Specification");
        let listing = section
            .split("```text")
            .nth(1)
            .and_then(|rest| rest.split("```").next())
            .expect("§67.4 prints the registry in a text block");
        let printed: Vec<&str> = listing
            .lines()
            .filter_map(|line| line.split_whitespace().next())
            .collect();
        assert_eq!(printed, capability_registry_names());
    }

    /// The shared engine vocabulary is a set, and reads as one.
    #[test]
    fn the_engine_capability_names_are_sorted_and_unique() {
        let names = capability_engine_names();
        assert!(!names.is_empty());
        for pair in names.windows(2) {
            assert!(
                pair[0] < pair[1],
                "{} then {} is out of order",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn the_registry_matches_the_specifications_listing() {
        // §89 lists nine; a profile added upstream must be added here
        // rather than silently missing from every declaration.
        assert_eq!(ConformanceProfile::ALL.len(), 9);
        let names: Vec<&str> = ConformanceProfile::ALL.iter().map(|p| p.name()).collect();
        assert_eq!(
            names,
            vec![
                "KIP-Core",
                "KIP-Schema",
                "KIP-Epistemic",
                "KIP-Governance",
                "KIP-Transactions",
                "KIP-KQL",
                "KIP-KML",
                "KIP-META",
                "KIP-Runtime",
            ]
        );
    }
}
