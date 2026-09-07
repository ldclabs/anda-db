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
use std::fmt::Write;

use crate::ast::{Json, Map};
use crate::error::{KipError, KipErrorCode};

/// The `format` discriminator of a native Capsule.
pub const CAPSULE_FORMAT: &str = "KIP-Cognitive-Capsule";

/// The Capsule format version this crate writes.
pub const CAPSULE_VERSION: &str = "2.0-draft";

/// A portable Cognitive Capsule (Spec §37.6).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Capsule {
    /// Always [`CAPSULE_FORMAT`] for a native Capsule.
    pub format: String,
    /// The Capsule format version.
    #[serde(rename = "format_version")]
    pub version: String,
    /// Everything the Capsule carries.
    pub payload: CapsulePayload,
    /// The digest and proofs over [`Capsule::payload`].
    pub integrity: CapsuleIntegrity,
}

impl Capsule {
    /// Creates a Capsule frame in this crate's format and version.
    pub fn new(payload: CapsulePayload, mut integrity: CapsuleIntegrity) -> Self {
        if integrity.digest_profile.is_empty() {
            integrity.digest_profile = "kip-jcs-safe-v1".into();
        }
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
        if self.version != CAPSULE_VERSION || self.integrity.digest_profile != "kip-jcs-safe-v1" {
            return Err(KipError::unsupported_capability(
                "Capsule requires format_version 2.0-draft and kip-jcs-safe-v1; older drafts need explicit migration",
            ));
        }
        crate::validate_json(
            &serde_json::to_value(self)
                .map_err(|e| KipError::capsule_validation_failed(e.to_string()))?,
        )?;
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
            let manifest = &self.payload.manifest;
            if manifest.base_seq.is_none() || manifest.target_seq.is_none() {
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
    #[serde(default, rename = "schema_dependencies")]
    pub schema: Vec<SchemaDependency>,
    /// The cognitive records themselves.
    #[serde(default)]
    pub records: CapsuleRecords,
    /// Dependencies deliberately left out, named rather than dangling (§40.1).
    #[serde(default)]
    pub external_refs: Vec<ExternalRef>,
    /// Content-addressed blobs the records reference.
    #[serde(default)]
    pub blobs: BTreeMap<String, String>,
    /// What the source asks of anyone handling this Capsule.
    #[serde(default)]
    pub handling: CapsuleHandling,
    /// Namespaced extensions.
    #[serde(skip)]
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
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct CapsuleManifest {
    #[serde(default)]
    pub roots: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_seq: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_seq: Option<u64>,
    /// Snapshot or delta.
    pub kind: CapsuleKind,
    /// When the Capsule was produced.
    #[serde(skip)]
    pub created_at: Option<String>,
    /// How complete the selection is, e.g. `selection_complete`.
    #[serde(skip)]
    pub completeness: Option<String>,
    /// What the Capsule closes over (§40.3).
    pub closure: String,
}

impl Default for CapsuleManifest {
    fn default() -> Self {
        Self {
            roots: Vec::new(),
            base_seq: None,
            target_seq: None,
            kind: CapsuleKind::Snapshot,
            created_at: None,
            completeness: None,
            closure: "selective".into(),
        }
    }
}

/// Where a Capsule came from (Spec §37.6, §37.5).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct CapsuleSource {
    /// The source Nexus.
    #[serde(skip)]
    pub nexus_id: Option<String>,
    /// The source Space.
    #[serde(default, rename = "space_id")]
    pub space_ref: Option<String>,
    /// The pinned source snapshot a snapshot Capsule was exported at.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_seq: Option<u64>,
    /// The lower bound of a delta Capsule's lineage.
    #[serde(skip)]
    pub base_seq: Option<u64>,
    /// The upper bound of a delta Capsule's lineage.
    #[serde(skip)]
    pub target_seq: Option<u64>,
    /// Which Schema Environment version the records were written under.
    #[serde(skip)]
    pub schema_environment_version: Option<u64>,
}

/// One Schema Package a Capsule depends on (Spec §20.11).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(try_from = "Json", into = "Json")]
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
#[serde(try_from = "Vec<Json>", into = "Vec<Json>")]
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
    #[serde(rename = "id")]
    pub reference: String,
    /// What kind of omission this is.
    pub kind: ExternalRefKind,
    /// Whatever identity the source can safely disclose.
    #[serde(default, rename = "locator", skip_serializing_if = "Option::is_none")]
    pub identity: Option<Json>,
    /// Why it was omitted, where policy permits saying.
    #[serde(skip)]
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
    #[serde(flatten)]
    pub extra: Map<String, Json>,
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
    #[serde(default)]
    pub digest_profile: String,
    /// The canonical content digest, e.g. `sha256:...`.
    pub content_digest: String,
    /// Signatures and other proofs over that digest.
    #[serde(default, rename = "signatures")]
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

// ---------------------------------------------------------------------------
// Canonical serialization (Spec §37.7)
// ---------------------------------------------------------------------------

/// Serializes a JSON value to the canonical bytes a Capsule digest is taken
/// over (Spec §37.7).
///
/// Portable artifact identity is cryptographic, so two implementations that
/// disagree about which bytes a Capsule *is* produce different digests for the
/// same cognition — and every `VERIFY CAPSULE` across that boundary fails for
/// a reason neither side can see. Pinning the encoding is what makes the
/// digest mean the same thing on both sides.
///
/// The form is RFC 8785 (JCS):
///
/// - object members sorted by their keys' UTF-16 code units;
/// - no insignificant whitespace;
/// - the shortest round-tripping number form;
/// - the minimal string escaping JSON allows.
///
/// Values must have passed `validate_json` at the ingestion boundary. Use
/// `try_canonical_json` for unchecked host values.
pub fn canonical_json(value: &Json) -> String {
    let mut out = String::new();
    write_canonical(value, &mut out);
    out
}

/// Validate an arbitrary host value before producing portable artifact bytes.
pub fn try_canonical_json(value: &Json) -> Result<String, KipError> {
    crate::validate_json(value)?;
    Ok(canonical_json(value))
}

fn write_canonical(value: &Json, out: &mut String) {
    match value {
        Json::Number(number) => {
            out.push_str(
                ryu_js::Buffer::new().format(number.as_f64().expect("JSON number is finite")),
            );
        }
        Json::Null | Json::Bool(_) | Json::String(_) => {
            // serde_json already emits these in the form JCS prescribes.
            write!(out, "{value}").expect("writing to a String cannot fail");
        }
        Json::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        Json::Object(members) => {
            // JCS orders by UTF-16 code units, which differs from Rust's
            // UTF-8 byte order for astral-plane keys: a surrogate pair starts
            // with 0xD800..=0xDBFF, below the U+E000..=U+FFFF range whose
            // UTF-8 sorts above it. Rare, but a digest that depends on which
            // one you picked is not deterministic.
            // Cached rather than plain `sort_by_key`: the key is an allocated
            // `Vec<u16>`, and recomputing it on every comparison would turn a
            // digest of a large Capsule into O(n log n) allocations.
            let mut keys: Vec<&String> = members.keys().collect();
            keys.sort_by_cached_key(|key| utf16_units(key));

            out.push('{');
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write!(out, "{}", Json::String(key.clone()))
                    .expect("writing to a String cannot fail");
                out.push(':');
                write_canonical(&members[key], out);
            }
            out.push('}');
        }
    }
}

fn utf16_units(text: &str) -> Vec<u16> {
    text.encode_utf16().collect()
}

impl Capsule {
    /// The canonical bytes this Capsule's `integrity.content_digest` covers.
    ///
    /// The digest is taken over the **payload**, not over the whole artifact:
    /// a signature must not cover itself, and adding a countersignature must
    /// not invalidate the digest the first signer attested to (§37.8).
    ///
    /// The hash function stays the caller's: `content_digest` is an
    /// `algorithm:value` pair precisely so the algorithm can be negotiated
    /// and rotated, and pinning one here would freeze it.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use anda_kip::{Capsule, CapsuleIntegrity, CapsulePayload};
    ///
    /// let capsule = Capsule::new(CapsulePayload::default(), CapsuleIntegrity::default());
    /// // Two Capsules carrying the same cognition digest identically,
    /// // whatever order their fields were built in.
    /// assert_eq!(
    ///     capsule.canonical_payload(),
    ///     Capsule::new(CapsulePayload::default(), CapsuleIntegrity::default())
    ///         .canonical_payload()
    /// );
    /// ```
    pub fn canonical_payload(&self) -> String {
        let value = serde_json::to_value(&self.payload)
            .expect("a Capsule payload is representable as JSON");
        canonical_json(
            &serde_json::json!({"format": self.format, "format_version": self.version, "payload": value}),
        )
    }
}

impl From<SchemaDependency> for Json {
    fn from(value: SchemaDependency) -> Self {
        let mut object = Map::new();
        object.insert(
            "package_ref".into(),
            Json::String(format!("{}@{}", value.package, value.version)),
        );
        if let Some(digest) = value.digest {
            object.insert("content_digest".into(), Json::String(digest));
        }
        Json::Object(object)
    }
}
impl TryFrom<Json> for SchemaDependency {
    type Error = String;
    fn try_from(value: Json) -> Result<Self, Self::Error> {
        let reference = value["package_ref"]
            .as_str()
            .ok_or("schema dependency needs package_ref")?;
        let (package, version) = reference
            .rsplit_once('@')
            .ok_or("schema dependency needs exact version")?;
        Ok(Self {
            package: package.into(),
            version: version.into(),
            digest: value["content_digest"].as_str().map(str::to_string),
        })
    }
}
impl From<CapsuleRecords> for Vec<Json> {
    fn from(value: CapsuleRecords) -> Self {
        value
            .concepts
            .into_iter()
            .chain(value.propositions)
            .chain(value.assertions)
            .chain(value.evidence)
            .chain(value.activities)
            .collect()
    }
}
impl TryFrom<Vec<Json>> for CapsuleRecords {
    type Error = String;
    fn try_from(values: Vec<Json>) -> Result<Self, Self::Error> {
        let mut out = Self::default();
        for value in values {
            match value["kind"].as_str() {
                Some("concept") => out.concepts.push(value),
                Some("proposition") => out.propositions.push(value),
                Some("assertion") => out.assertions.push(value),
                Some("evidence") => out.evidence.push(value),
                Some("activity") => out.activities.push(value),
                _ => return Err("Capsule record needs a Core kind".into()),
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> Capsule {
        Capsule::new(
            CapsulePayload {
                manifest: CapsuleManifest {
                    kind: CapsuleKind::Snapshot,
                    created_at: None,
                    completeness: None,
                    closure: "closed".into(),
                    ..Default::default()
                },
                source: CapsuleSource {
                    nexus_id: None,
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
                    concepts: vec![
                        serde_json::json!({"id": "c:1", "kind": "concept", "name": "Alice"}),
                    ],
                    ..Default::default()
                },
                ..Default::default()
            },
            CapsuleIntegrity {
                digest_profile: "kip-jcs-safe-v1".into(),
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
        assert_eq!(json["format_version"], "2.0-draft");
        assert_eq!(json["payload"]["manifest"]["kind"], "snapshot");
        assert_eq!(json["payload"]["source"]["snapshot_seq"], 8123);

        let decoded: Capsule = serde_json::from_value(json).unwrap();
        assert_eq!(decoded, capsule);
        assert_eq!(decoded.payload.records.len(), 1);
    }

    #[test]
    fn the_canonical_form_does_not_depend_on_how_the_json_was_built() {
        // The same cognition written in two member orders must digest to the
        // same bytes, or a Capsule's identity depends on its author's habits.
        let one = serde_json::from_str::<Json>(
            r#"{ "b": [1, {"z": true, "a": null}], "a": "x", "é": 1 }"#,
        )
        .unwrap();
        let two = serde_json::from_str::<Json>(
            r#"{ "é": 1, "a": "x", "b": [1, {"a": null, "z": true}] }"#,
        )
        .unwrap();
        assert_eq!(canonical_json(&one), canonical_json(&two));
        assert_eq!(
            canonical_json(&one),
            r#"{"a":"x","b":[1,{"a":null,"z":true}],"é":1}"#
        );
    }

    #[test]
    fn canonical_keys_sort_by_utf16_code_units() {
        // U+10000 encodes as the surrogate pair D800 DC00, which sorts below
        // U+FFFD — the opposite of their UTF-8 byte order. Sorting the Rust
        // way here would make the digest depend on the implementation.
        let value = serde_json::json!({ "\u{10000}": 1, "\u{fffd}": 2 });
        assert_eq!(canonical_json(&value), "{\"\u{10000}\":1,\"\u{fffd}\":2}");

        let mut rust_order: Vec<&str> = vec!["\u{10000}", "\u{fffd}"];
        rust_order.sort();
        assert_eq!(
            rust_order,
            vec!["\u{fffd}", "\u{10000}"],
            "the two orders really do differ, so the test is not vacuous"
        );
    }

    #[test]
    fn a_capsules_digest_covers_its_payload_and_not_its_proofs() {
        // §37.8: a signature cannot cover itself, and countersigning must not
        // invalidate what the first signer attested to.
        let mut capsule = snapshot();
        let before = capsule.canonical_payload();
        capsule.integrity.proofs.push(CapsuleProof {
            proof_type: "signature".into(),
            suite: None,
            verification_method: None,
            signature: Some("sig".into()),
        });
        assert_eq!(capsule.canonical_payload(), before);

        // But changing what it carries does change it.
        capsule.payload.records.concepts.push(serde_json::json!({}));
        assert_ne!(capsule.canonical_payload(), before);
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

        capsule.payload.manifest.base_seq = Some(8000);
        capsule.payload.manifest.target_seq = Some(8123);
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
