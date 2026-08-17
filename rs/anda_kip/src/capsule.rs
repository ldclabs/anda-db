//! # Cognitive Capsule (Spec §37–§41)
//!
//! A Cognitive Capsule is a portable, immutable, inspectable artifact carrying
//! cognitive state or state changes between systems and Spaces.
//!
//! The invariant the whole design hangs on:
//!
//! ```text
//! Capsule bytes  ≠  destination mutation authority
//! ```
//!
//! A valid signature proves that a signer attested to a content digest and
//! scope. It proves nothing about truth, safety, utility, trust, authority, or
//! whether the cognition applies at the destination (§37.8). Which is why these
//! types model the artifact and never apply it: importing runs
//! `VERIFY → VALIDATE → PREVIEW → Governance analysis → Import Plan → atomic
//! Import Transaction` (§41.2), and every step of that belongs to the engine.
//!
//! Record payloads are carried as JSON rather than as closed structs: which
//! fields a record has is the active Schema Packages' decision, and the
//! destination validates them against its own environment (§39.5, §41.3). The
//! *frame* — manifest, source, schema dependencies, external refs, blobs,
//! handling, integrity — is normative, so that is typed.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::ast::{Json, Map};
use crate::error::{KipError, KipErrorCode};

/// The `format` discriminator of a native Capsule.
pub const CAPSULE_FORMAT: &str = "KIP-Cognitive-Capsule";

/// The Capsule format version this crate writes.
pub const CAPSULE_VERSION: &str = "2.0";

/// A portable Cognitive Capsule (Spec §37.6).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Capsule {
    /// Always [`CAPSULE_FORMAT`] for a native Capsule.
    pub format: String,
    /// The Capsule format version.
    pub version: String,
    /// Everything the Capsule carries.
    pub payload: CapsulePayload,
    /// The digest and proofs over [`Capsule::payload`].
    pub integrity: CapsuleIntegrity,
}

impl Capsule {
    /// Creates a Capsule frame in this crate's format and version.
    pub fn new(payload: CapsulePayload, integrity: CapsuleIntegrity) -> Self {
        Self {
            format: CAPSULE_FORMAT.to_string(),
            version: CAPSULE_VERSION.to_string(),
            payload,
            integrity,
        }
    }

    /// Checks the frame invariants this crate can decide without a destination.
    ///
    /// This is the cheap structural gate, not `VALIDATE CAPSULE`: Schema
    /// legality, identity resolution and Governance all need an engine and a
    /// destination Space.
    pub fn validate_frame(&self) -> Result<(), KipError> {
        if self.format != CAPSULE_FORMAT {
            return Err(KipError::capsule_validation_failed(format!(
                "expected format {CAPSULE_FORMAT:?}, found {:?}",
                self.format
            )));
        }
        if self.integrity.content_digest.trim().is_empty() {
            return Err(KipError::new(
                KipErrorCode::CapsuleValidationFailed,
                "a Capsule must carry a content digest: portable artifact identity is \
                 cryptographic, not positional",
            ));
        }
        if self.payload.manifest.kind == CapsuleKind::Delta {
            let source = &self.payload.source;
            if source.base_seq.is_none() || source.target_seq.is_none() {
                return Err(KipError::new(
                    KipErrorCode::CapsuleValidationFailed,
                    "a delta Capsule must declare base_seq and target_seq: delta application \
                     requires base/checkpoint compatibility",
                ));
            }
        }
        Ok(())
    }
}

/// What a Capsule carries (Spec §37.6).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct CapsulePayload {
    /// What kind of Capsule this is and how complete it claims to be.
    pub manifest: CapsuleManifest,
    /// Where it came from.
    pub source: CapsuleSource,
    /// The Schema Packages its records were written against.
    ///
    /// Embedded packages may be used validation-only and MUST NOT auto-activate
    /// at the destination (§41.3).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schema: Vec<SchemaDependency>,
    /// The cognitive records themselves.
    #[serde(default)]
    pub records: CapsuleRecords,
    /// Dependencies deliberately left out, named rather than dangling (§40.1).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub external_refs: Vec<ExternalRef>,
    /// Content-addressed blobs the records reference.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blobs: Vec<BlobRef>,
    /// What the source asks of anyone handling this Capsule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handling: Option<CapsuleHandling>,
    /// Namespaced extensions.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub extensions: Map<String, Json>,
}

/// The two baseline Capsule kinds (Spec §37.3).
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum CapsuleKind {
    /// Selected cognitive state at one source snapshot (§37.4).
    #[default]
    Snapshot,
    /// Ordered changes over one source lineage between two sequences (§37.5).
    Delta,
}

/// What the Capsule claims about itself (Spec §37.6).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct CapsuleManifest {
    /// Snapshot or delta.
    pub kind: CapsuleKind,
    /// When the Capsule was produced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    /// How complete the selection is, e.g. `selection_complete`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completeness: Option<String>,
    /// What the Capsule closes over (§40.3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closure: Option<Json>,
}

/// Where a Capsule came from (Spec §37.6, §37.5).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct CapsuleSource {
    /// The source Nexus.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nexus_id: Option<String>,
    /// The source Space.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub space_ref: Option<String>,
    /// The pinned source snapshot a snapshot Capsule was exported at.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_seq: Option<u64>,
    /// The lower bound of a delta Capsule's lineage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_seq: Option<u64>,
    /// The upper bound of a delta Capsule's lineage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_seq: Option<u64>,
    /// Which Schema Environment version the records were written under.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_environment_version: Option<u64>,
}

/// One Schema Package a Capsule depends on (Spec §20.11).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct SchemaDependency {
    /// The package path, e.g. `kip://profiles/cognitive-memory`.
    pub package: String,
    /// The exact version; packages persist by exact version (§20.4).
    pub version: String,
    /// The package artifact digest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
}

/// The cognitive records a Capsule carries, grouped by Core kind.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct CapsuleRecords {
    /// Concept records.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub concepts: Vec<Json>,
    /// Proposition records.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub propositions: Vec<Json>,
    /// Assertion records.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub assertions: Vec<Json>,
    /// Evidence records.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<Json>,
    /// Activity records.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub activities: Vec<Json>,
}

impl CapsuleRecords {
    /// The total number of records carried.
    pub fn len(&self) -> usize {
        self.concepts.len()
            + self.propositions.len()
            + self.assertions.len()
            + self.evidence.len()
            + self.activities.len()
    }

    /// Whether the Capsule carries no records at all.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// What kind of thing an omitted dependency was (Spec §40.1).
///
/// [`ExternalRefKind::Redacted`] and [`ExternalRefKind::Unavailable`] must stay
/// distinguishable where policy permits: one means the source withheld it, the
/// other means the source does not have it (§40.2).
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ExternalRefKind {
    /// An element in the source Space that was not included.
    SourceElement,
    /// A cross-system canonical identity.
    CanonicalIdentity,
    /// A semantic locator rather than an identity.
    SemanticLocator,
    /// An artifact outside any Nexus.
    ExternalArtifact,
    /// The source intentionally withheld it.
    Redacted,
    /// The source does not possess or provide it.
    Unavailable,
}

/// A dependency the Capsule names but does not carry (Spec §40.1).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ExternalRef {
    /// The capsule-local reference this stands in for.
    #[serde(rename = "ref")]
    pub reference: String,
    /// What kind of omission this is.
    pub kind: ExternalRefKind,
    /// Whatever identity the source can safely disclose.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<Json>,
    /// Why it was omitted, where policy permits saying.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// A content-addressed blob a Capsule references (Spec §41.5).
///
/// Import MUST NOT automatically fetch arbitrary URLs; network access is a
/// separate runtime authority.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct BlobRef {
    /// The capsule-local reference used by the records.
    #[serde(rename = "ref")]
    pub reference: String,
    /// The content digest that identifies the bytes.
    pub digest: String,
    /// The blob's media type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    /// The size in bytes, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    /// Where the bytes may be fetched from, subject to separate authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locator: Option<String>,
}

/// What the source asks of anyone handling this Capsule (Spec §37.6).
///
/// A request, not an enforcement mechanism: the destination applies its own
/// trust, classification, authority, Schema and Governance policy (§39.5).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct CapsuleHandling {
    /// How the source classified this content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_classification: Option<String>,
    /// Handling requirements the source asks for.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requirements: Vec<Json>,
}

/// The digest and proofs over a Capsule payload (Spec §37.6).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct CapsuleIntegrity {
    /// The canonical content digest, e.g. `sha256:...`.
    pub content_digest: String,
    /// Signatures and other proofs over that digest.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub proofs: Vec<CapsuleProof>,
}

/// One proof over a Capsule's content digest (Spec §37.8).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct CapsuleProof {
    /// The proof kind, e.g. `signature`.
    #[serde(rename = "type")]
    pub proof_type: String,
    /// The cryptographic suite used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suite: Option<String>,
    /// How to obtain the verification key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification_method: Option<String>,
    /// The proof value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

/// How a Capsule is brought into a destination Space (Spec §39).
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum ImportMode {
    /// Read-only simulation; no destination cognitive state is created (§39.1).
    Preview,
    /// Imports into a quarantined review state rather than ordinary Recall
    /// state (§39.2).
    Isolate,
    /// Merges another source's cognition under destination identity and
    /// Governance policy (§39.3).
    Merge,
    /// Restores the same Brain/owner lineage under stronger identity checks
    /// (§39.4).
    Restore,
}

impl ImportMode {
    /// Whether this mode can create durable destination state.
    pub fn is_durable(&self) -> bool {
        !matches!(self, ImportMode::Preview)
    }

    /// Whether this mode may map a source `$self` onto the destination `$self`.
    ///
    /// Only a verified restore may, and only when Governance has verified same
    /// owner, same Brain identity, backup lineage and explicit restore
    /// authority (§38.4, §38.5). Ordinary Agent-to-Agent sharing maps source
    /// self to the *source Agent's* semantic identity instead.
    pub fn may_map_self(&self) -> bool {
        matches!(self, ImportMode::Restore)
    }
}

/// The identity resolution order an import should follow (Spec §38.2).
///
/// Conservative on purpose: a source element id must never automatically become
/// the destination local primary id (§38.1), and equal names are not equal
/// identities (§38.3).
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum IdentityResolution {
    /// A prior verified import mapping.
    PriorImportMapping,
    /// A trusted `canonical_id`.
    TrustedCanonicalId,
    /// A mapping a human or policy explicitly approved.
    ApprovedMapping,
    /// A portable identity the Schema defines.
    SchemaPortableIdentity,
    /// Nothing matched; create a new Concept.
    CreateNew,
}

impl IdentityResolution {
    /// The resolution steps in the order §38.2 recommends trying them.
    pub const ORDER: &'static [IdentityResolution] = &[
        IdentityResolution::PriorImportMapping,
        IdentityResolution::TrustedCanonicalId,
        IdentityResolution::ApprovedMapping,
        IdentityResolution::SchemaPortableIdentity,
        IdentityResolution::CreateNew,
    ];
}

/// A capsule-local reference map, from `ref` to whatever the caller resolved it
/// to. Kept ordered so an import plan renders deterministically.
pub type CapsuleRefMap = BTreeMap<String, String>;

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> Capsule {
        Capsule::new(
            CapsulePayload {
                manifest: CapsuleManifest {
                    kind: CapsuleKind::Snapshot,
                    created_at: Some("2026-08-13T15:00:00Z".into()),
                    completeness: Some("selection_complete".into()),
                    closure: Some(serde_json::json!({"semantic": "closed"})),
                },
                source: CapsuleSource {
                    nexus_id: Some("nexus:source-A".into()),
                    space_ref: Some("space:project-kip".into()),
                    snapshot_seq: Some(8123),
                    ..Default::default()
                },
                schema: vec![SchemaDependency {
                    package: "kip://core".into(),
                    version: "2.0.0".into(),
                    digest: Some("sha256:abc".into()),
                }],
                records: CapsuleRecords {
                    concepts: vec![serde_json::json!({"ref": "c:1", "name": "Alice"})],
                    ..Default::default()
                },
                ..Default::default()
            },
            CapsuleIntegrity {
                content_digest: "sha256:abc".into(),
                proofs: vec![],
            },
        )
    }

    #[test]
    fn a_capsule_round_trips_through_its_wire_shape() {
        let capsule = snapshot();
        let json = serde_json::to_value(&capsule).unwrap();
        assert_eq!(json["format"], CAPSULE_FORMAT);
        assert_eq!(json["version"], "2.0");
        assert_eq!(json["payload"]["manifest"]["kind"], "snapshot");
        assert_eq!(json["payload"]["source"]["snapshot_seq"], 8123);

        let decoded: Capsule = serde_json::from_value(json).unwrap();
        assert_eq!(decoded, capsule);
        assert_eq!(decoded.payload.records.len(), 1);
    }

    #[test]
    fn frame_validation_requires_a_content_digest() {
        let mut capsule = snapshot();
        capsule.integrity.content_digest = String::new();
        let err = capsule.validate_frame().expect_err("no digest");
        assert_eq!(err.code, KipErrorCode::CapsuleValidationFailed);

        assert!(snapshot().validate_frame().is_ok());
    }

    #[test]
    fn a_delta_capsule_must_declare_its_lineage() {
        let mut capsule = snapshot();
        capsule.payload.manifest.kind = CapsuleKind::Delta;
        assert!(capsule.validate_frame().is_err());

        capsule.payload.source.base_seq = Some(8000);
        capsule.payload.source.target_seq = Some(8123);
        assert!(capsule.validate_frame().is_ok());
    }

    #[test]
    fn a_foreign_format_is_not_a_native_capsule() {
        let mut capsule = snapshot();
        capsule.format = "KIP-1.x-EXPORT".into();
        // Spec migration invariant 14: a legacy export is not a native Capsule.
        assert!(capsule.validate_frame().is_err());
    }

    #[test]
    fn only_a_verified_restore_may_map_self() {
        // Spec §38.4/§38.5: ordinary sharing must not carry a source `$self`
        // onto the destination's own identity.
        for mode in [ImportMode::Preview, ImportMode::Isolate, ImportMode::Merge] {
            assert!(!mode.may_map_self(), "{mode:?} must not map $self");
        }
        assert!(ImportMode::Restore.may_map_self());
    }

    #[test]
    fn preview_creates_no_durable_state() {
        assert!(!ImportMode::Preview.is_durable());
        assert!(ImportMode::Merge.is_durable());
    }

    #[test]
    fn redacted_and_unavailable_stay_distinguishable() {
        // Spec §40.2: collapsing these loses whether the source *had* the thing.
        let redacted = serde_json::to_string(&ExternalRefKind::Redacted).unwrap();
        let unavailable = serde_json::to_string(&ExternalRefKind::Unavailable).unwrap();
        assert_eq!(redacted, r#""redacted""#);
        assert_eq!(unavailable, r#""unavailable""#);
        assert_ne!(redacted, unavailable);
    }

    #[test]
    fn identity_resolution_tries_creation_last() {
        assert_eq!(
            IdentityResolution::ORDER.last(),
            Some(&IdentityResolution::CreateNew)
        );
        assert_eq!(
            IdentityResolution::ORDER.first(),
            Some(&IdentityResolution::PriorImportMapping)
        );
        assert_eq!(IdentityResolution::ORDER.len(), 5);
    }
}
