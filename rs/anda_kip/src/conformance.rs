//! # Conformance levels (Spec §89–§102)
//!
//! > An implementation claims a **conformance level**, never a subset of one.
//!
//! §89 has two levels: `KIP-Core` and `KIP-CognitiveMemory`. The declaration
//! is a runtime's, not a library's — whether an engine implements temporal
//! succession or the Search Pattern is a property of the engine behind
//! [`crate::Executor`] — so this crate only gives the declaration a shape.
//!
//! The eight **areas** (`KIP-Schema` … `KIP-Runtime`) partition the KIP-Core
//! requirements for test selection and diagnosis; passing an area claims
//! nothing by itself. [`PROTOCOL_SURFACE`] names the areas whose *language
//! surface* this crate implements: parsing every KQL statement family is a
//! necessary part of `KIP-KQL` and not a sufficient one.

use std::sync::LazyLock;

use serde::Deserialize;

wire_enum! {
    /// A KIP 2.0 conformance level (Spec §89).
    ///
    /// Ordered so a declared set can be held in a `BTreeSet`; §89's listing
    /// order is the one the variants are declared in.
    #[derive(PartialOrd, Ord)]
    pub enum ConformanceProfile {
        /// Every requirement of §90–§99.
        Core = "KIP-Core",
        /// KIP-Core plus the standard Profile package, dependency validity,
        /// computed mnemonic strength, recording repair and the Profile
        /// invariants.
        CognitiveMemory = "KIP-CognitiveMemory",
    }
}

impl ConformanceProfile {
    /// The wire name, e.g. `"KIP-Core"`.
    pub const fn name(&self) -> &'static str {
        self.as_str()
    }

    /// Looks a level up by its wire name.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::from_wire(name)
    }
}

wire_enum! {
    /// An area of the KIP-Core requirements (Spec §89), for test selection
    /// and diagnosis only — never a claim.
    #[derive(PartialOrd, Ord)]
    pub enum ConformanceArea {
        /// Schema Packages and the Schema Environment (§91).
        Schema = "KIP-Schema",
        /// Projection, policies, world time, open-world semantics (§92).
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

/// The areas whose **language surface** this crate implements in full.
///
/// Every statement family of these three parses, lowers to the executable AST,
/// and is held to the schema-independent rules — which is what a protocol
/// library can carry of them. Evaluation belongs to the engine.
pub const PROTOCOL_SURFACE: &[ConformanceArea] = &[
    ConformanceArea::Kql,
    ConformanceArea::Kml,
    ConformanceArea::Meta,
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
    fn every_level_and_area_round_trips_by_its_wire_name() {
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
        assert!(ConformanceProfile::from_name("KIP-KQL").is_none());
        for area in ConformanceArea::ALL {
            assert_eq!(ConformanceArea::from_wire(area.as_str()), Some(*area));
        }
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
    fn the_levels_are_the_ones_the_specification_prints() {
        let spec = include_str!("../SPECIFICATION.md");
        let section = spec
            .split("# 89. Conformance Model")
            .nth(1)
            .and_then(|rest| rest.split("```text").nth(1))
            .and_then(|rest| rest.split("```").next())
            .expect("§89 prints the levels in a text block");
        let printed: Vec<&str> = section
            .lines()
            .filter(|line| line.starts_with("KIP-"))
            .filter_map(|line| line.split_whitespace().next())
            .collect();
        let names: Vec<&str> = ConformanceProfile::ALL.iter().map(|p| p.name()).collect();
        assert_eq!(printed, names);
        assert_eq!(ConformanceArea::ALL.len(), 8);
    }
}
