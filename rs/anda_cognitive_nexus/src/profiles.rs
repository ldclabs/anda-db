//! # Bundled Schema Package artifacts
//!
//! A Space that has activated nothing resolves Core and nothing else, and Core
//! declares no Concept types at all — it carries the element kinds and the open
//! registries (§158). So a Nexus with no activated profile can read and write
//! Assertions about types it does not have, which is to say: it cannot create a
//! Concept.
//!
//! This module ships the baseline profile so a host can hand a fresh Space a
//! working ontology without going looking for a file. It is the artifact
//! **verbatim** from the specification repository
//! (`profiles/cognitive-memory-2.0.0.schema.json`), not a Rust transcription of
//! it: a hand-maintained copy would drift toward whatever this engine happens
//! to support, and the point of a profile is that two engines mean the same
//! thing by `Preference`.
//!
//! Installing is not activating (§240.18). Bundling the bytes says nothing
//! about which Space may resolve symbols through them; that stays a decision
//! the host makes with
//! [`CognitiveNexus::ensure_schema`](crate::CognitiveNexus::ensure_schema).

/// The KIP Cognitive Memory Profile, version 2.0.0.
///
/// Re-copy it from the spec repository when the profile changes; nothing here
/// edits it.
pub const COGNITIVE_MEMORY: &str = include_str!("../profiles/cognitive-memory-2.0.0.json");

/// The package id [`COGNITIVE_MEMORY`] declares.
pub const COGNITIVE_MEMORY_ID: &str = "kip://profiles/cognitive-memory";

/// The version [`COGNITIVE_MEMORY`] declares.
pub const COGNITIVE_MEMORY_VERSION: &str = "2.0.0";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaPackage;

    /// The bundled bytes must parse, and must be the package the two constants
    /// claim — a host builds its Schema Lock from those strings, and a lock
    /// naming a package that is not installed refuses to activate at all.
    #[test]
    fn the_bundled_profile_is_the_package_its_constants_name() {
        let package = SchemaPackage::parse(COGNITIVE_MEMORY).expect("the bundled profile parses");
        let package_ref = package.package_ref().expect("it declares a package ref");
        assert_eq!(package_ref.package_id, COGNITIVE_MEMORY_ID);
        assert_eq!(package_ref.version.to_string(), COGNITIVE_MEMORY_VERSION);
    }
}
