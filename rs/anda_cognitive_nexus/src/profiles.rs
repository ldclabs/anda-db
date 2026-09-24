//! # Bundled Schema Package artifacts
//!
//! A Space that has activated nothing resolves Core and nothing else, and Core
//! declares no Concept types at all — it carries the element kinds and the open
//! registries (§20.13). So a Nexus with no activated profile can read and write
//! Assertions about types it does not have, which is to say: it cannot create a
//! Concept.
//!
//! This module ships the baseline profile so a host can hand a fresh Space a
//! working ontology without going looking for a file. It is the artifact
//! **verbatim** from the specification repository
//! (`profiles/cognitive-memory-2.0.0.schema.json`), not a Rust transcription of
//! it: a hand-maintained copy would drift toward whatever this engine happens
//! to support, and the point of a profile is that two engines mean the same
//! thing by `prefers`.
//!
//! The 2.0 draft rewrote `cognitive-memory@2.0.0` in place, so the reference
//! alone no longer tells two revisions apart: only the content digest does
//! (`integrity.content_digest`). This engine ships exactly one revision and
//! does not migrate Spaces activated under an earlier draft.
//!
//! Installing is not activating (§20.12). Bundling the bytes says nothing
//! about which Space may resolve symbols through them; that stays a decision
//! the host makes with
//! [`CognitiveNexus::ensure_schema`](crate::CognitiveNexus::ensure_schema).

/// The KIP Cognitive Memory Profile package, `cognitive-memory@2.0.0`.
///
/// Re-copy it from the spec repository when the profile changes; nothing here
/// edits it.
pub const COGNITIVE_MEMORY: &str = include_str!("../profiles/cognitive-memory-2.0.0.json");

/// The package id [`COGNITIVE_MEMORY`] declares.
pub const COGNITIVE_MEMORY_ID: &str = "kip://profiles/cognitive-memory";

/// The version [`COGNITIVE_MEMORY`] declares.
pub const COGNITIVE_MEMORY_VERSION: &str = "2.0.0";

/// The exact package reference [`COGNITIVE_MEMORY`] declares.
pub const COGNITIVE_MEMORY_REF: &str = cognitive_memory!();

/// The content digest of the bundled [`COGNITIVE_MEMORY`] revision.
pub const COGNITIVE_MEMORY_DIGEST: &str =
    "sha256:734aa0fd93b6258d433a6f71915d9d5118552e2ea0458a3050d368dcfcc1d1b3";

/// The minimal general domain package, `kip://domains/general@1.0.0`: places,
/// organizations, topics and everyday relations. Optional; a host installs it
/// beside [`COGNITIVE_MEMORY`] when it wants those symbols.
pub const GENERAL_DOMAIN: &str = include_str!("../profiles/general-domain-1.0.0.json");

/// The package id [`GENERAL_DOMAIN`] declares.
pub const GENERAL_DOMAIN_ID: &str = "kip://domains/general";

/// The version [`GENERAL_DOMAIN`] declares.
pub const GENERAL_DOMAIN_VERSION: &str = "1.0.0";

/// The machine-readable `kip:memory-default` policy artifact (Spec §21.13).
pub const MEMORY_DEFAULT_POLICY: &str = include_str!("../profiles/policy-memory-default.json");

/// The standard mnemonic strength policy, `kip:strength-half-life-30d`
/// (Spec §59.1, Profile §6.1): the one `strength_policy` pin this engine
/// computes `effective_strength` for.
pub const STRENGTH_HALF_LIFE_30D: &str =
    include_str!("../profiles/policy-strength-half-life-30d.json");

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
        assert_eq!(package_ref.to_string(), COGNITIVE_MEMORY_REF);
        let artifact: anda_kip::Json = serde_json::from_str(COGNITIVE_MEMORY).unwrap();
        assert_eq!(
            artifact["integrity"]["content_digest"],
            COGNITIVE_MEMORY_DIGEST
        );
        crate::schema::contracts::verify_artifact(&artifact).expect("the digest covers the bytes");

        let general = SchemaPackage::parse(GENERAL_DOMAIN).expect("the general domain parses");
        let general_ref = general.package_ref().unwrap();
        assert_eq!(general_ref.package_id, GENERAL_DOMAIN_ID);
        assert_eq!(general_ref.version.to_string(), GENERAL_DOMAIN_VERSION);

        let policy: anda_kip::Json = serde_json::from_str(MEMORY_DEFAULT_POLICY).unwrap();
        assert_eq!(policy["policy_id"], "kip:memory-default");

        let strength: anda_kip::Json = serde_json::from_str(STRENGTH_HALF_LIFE_30D).unwrap();
        assert_eq!(strength["policy_id"], "kip:strength-half-life-30d");
        crate::schema::contracts::verify_artifact(&strength).expect("its digest covers the bytes");
    }

    /// The syntax card a model reads must name everything this Profile declares.
    ///
    /// `anda_kip`'s `KIPSyntax.md` is a hand-maintained copy of the document in
    /// the specification repository, and this artifact is a hand-maintained
    /// copy of the Profile beside it. Nothing keeps two hand-copied files in
    /// step, and they have already drifted once: the card described
    /// `MnemonicState` as two members while the artifact declared three, and
    /// omitted `Skill.summary`, which the artifact makes *required*. A model
    /// working from the card then writes a `CREATE CONCEPT` the engine refuses
    /// for a missing field the card never mentioned, and the refusal reads as
    /// the model's mistake rather than as the card's.
    ///
    /// Names only, matched as whole words anywhere in the card. It is prose
    /// written for a reader and cannot be generated from the artifact, so this
    /// is a tripwire rather than a proof: a symbol whose name is also an
    /// ordinary English word can pass without being documented. What it does
    /// catch is the whole class that actually happened — a member added to the
    /// artifact and never written into the card.
    #[test]
    fn the_syntax_card_names_every_symbol_this_profile_declares() {
        let package: anda_kip::Json = serde_json::from_str(COGNITIVE_MEMORY).unwrap();
        let definitions = &package["definitions"];
        let instructions = format!(
            "{}\n{}",
            anda_kip::KIP_SYNTAX,
            anda_kip::COGNITIVE_MEMORY_PROFILE
        );
        let words: std::collections::BTreeSet<&str> = instructions
            .split(|c: char| !c.is_alphanumeric() && c != '_')
            .collect();

        let mut missing: Vec<String> = Vec::new();
        let mut require = |what: &str, name: &str| {
            if !words.contains(name) {
                missing.push(format!("{what} `{name}`"));
            }
        };

        for kind in ["concept_types", "predicates", "structural_fields", "facets"] {
            for name in definitions[kind].as_object().unwrap().keys() {
                require(kind, name);
            }
        }
        assert!(
            missing.is_empty(),
            "KIPSyntax.md never mentions: {}",
            missing.join(", ")
        );
    }
}
