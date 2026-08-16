//! # The Schema Package artifact
//!
//! Authoritative Schema in KIP 2.0 is an immutable versioned artifact, not a
//! set of graph nodes (Spec §1, §240.1). This is the difference from 1.x that
//! costs the most to get wrong: in 1.x an ordinary `UPSERT` could reshape what
//! a type meant, so cognition and the rules governing cognition sat in the same
//! mutable store. Here a package is content-addressed and read-only, and
//! changing semantics means publishing a new version and activating it through
//! Governance.
//!
//! The structs below mirror the shipped artifact format — the same JSON as
//! `KIP/v2/profiles/cognitive-memory-2.0.0.schema.json`, which the tests parse
//! rather than a hand-written imitation of it.
//!
//! Everything is `#[serde(default)]` and unknown fields are kept: a package
//! written against a later minor format revision must stay readable, and an
//! engine that dropped the fields it did not recognize would silently change
//! the artifact's digest-covered content.

use anda_kip::{Json, KipError, Map};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::symbol::{PackageRef, SymbolKind, SymbolRef};

/// A published Schema Package.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct SchemaPackage {
    /// The artifact format tag, e.g. `KIP-Schema-Package`.
    #[serde(default)]
    pub format: String,
    /// The format revision this artifact was written against.
    #[serde(default)]
    pub format_version: String,
    /// Identity and provenance.
    #[serde(default)]
    pub manifest: Manifest,
    /// The packages this one needs resolved.
    #[serde(default)]
    pub dependencies: Vec<Dependency>,
    /// The symbols this package defines.
    #[serde(default)]
    pub definitions: Definitions,
    /// Declared cross-element constraints.
    #[serde(default)]
    pub constraints: Vec<Json>,
    /// Model-friendly aliases, per symbol section.
    #[serde(default)]
    pub aliases: Map<String, Json>,
    /// How this version relates to its predecessor.
    #[serde(default)]
    pub compatibility: Option<Json>,
    /// Declarative migration descriptors.
    #[serde(default)]
    pub migrations: Option<Json>,
    /// Advisory guidance for an Agent. Never a validator (§240.34).
    #[serde(default)]
    pub model_hints: Option<Json>,
    /// The canonicalization profile the digest was computed under.
    #[serde(default)]
    pub canonicalization: Option<Json>,
    /// The content digest and any signatures.
    #[serde(default)]
    pub integrity: Option<Integrity>,
    /// Anything a later format revision added.
    #[serde(flatten)]
    pub extra: Map<String, Json>,
}

/// Identity and provenance.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Manifest {
    /// The stable namespace-qualified name, e.g. `kip://core`.
    #[serde(default)]
    pub package_id: String,
    /// The exact version.
    #[serde(default)]
    pub version: String,
    /// The two above, joined: `kip://core@2.0.0`.
    #[serde(default)]
    pub package_ref: String,
    /// A human-readable label.
    #[serde(default)]
    pub name: String,
    /// What the package is for.
    #[serde(default)]
    pub description: String,
    /// Who published it. Namespace identity does not prove this (§240.41).
    #[serde(default)]
    pub publisher: Json,
    /// Anything else the manifest carries.
    #[serde(flatten)]
    pub extra: Map<String, Json>,
}

/// One resolved dependency.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Dependency {
    /// The depended-on package.
    #[serde(default)]
    pub package_id: String,
    /// The exact version required.
    #[serde(default)]
    pub version: String,
    /// The two joined.
    #[serde(default)]
    pub package_ref: String,
    /// Whether activation fails without it (§73).
    #[serde(default)]
    pub required: bool,
    /// Anything else.
    #[serde(flatten)]
    pub extra: Map<String, Json>,
}

/// The content digest and signatures.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Integrity {
    /// Which canonicalization the digest was taken under.
    #[serde(default)]
    pub digest_profile: String,
    /// The digest itself, e.g. `sha256:...`.
    #[serde(default)]
    pub content_digest: String,
    /// What the digest covers.
    #[serde(default)]
    pub covers: String,
    /// Signatures over the digest. A signature is not local approval (§90).
    #[serde(default)]
    pub signatures: Vec<Json>,
}

/// The symbols a package defines, one map per symbol kind.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Definitions {
    /// Concept types, keyed by local name.
    #[serde(default)]
    pub concept_types: BTreeMap<String, ConceptTypeDef>,
    /// Predicate types, keyed by local name.
    #[serde(default)]
    pub predicates: BTreeMap<String, PredicateDef>,
    /// Facet definitions, keyed by local name.
    #[serde(default)]
    pub facets: BTreeMap<String, FacetDef>,
    /// Structural field definitions, keyed by local name.
    #[serde(default)]
    pub structural_fields: BTreeMap<String, StructuralFieldDef>,
    /// Named value sets, keyed by local name.
    #[serde(default)]
    pub enums: BTreeMap<String, Json>,
    /// Additions to Core's open registries, e.g. `activity_classes` (§69).
    #[serde(default)]
    pub registry_extensions: BTreeMap<String, Json>,
}

/// A Concept type definition (§32).
///
/// A Concept type says what a Concept *is*, never whether anything about it is
/// true (§33).
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ConceptTypeDef {
    /// The canonical symbol reference.
    #[serde(default)]
    pub r#ref: String,
    /// Always `ConceptType`.
    #[serde(default)]
    pub kind: String,
    /// What the type means.
    #[serde(default)]
    pub description: String,
    /// The attribute contract.
    #[serde(default)]
    pub attributes: AttributeSpec,
    /// Advisory guidance.
    #[serde(default)]
    pub model_hints: Option<Json>,
    /// Anything else.
    #[serde(flatten)]
    pub extra: Map<String, Json>,
}

/// The attribute contract of a Concept type (§34–§40).
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct AttributeSpec {
    /// Whether attributes not named here are permitted (§37).
    #[serde(default)]
    pub open: bool,
    /// The declared attributes.
    #[serde(default)]
    pub fields: BTreeMap<String, FieldSpec>,
}

/// One declared field of an attribute set or a Facet.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct FieldSpec {
    /// The validation type: a name, or a list of accepted names.
    #[serde(default)]
    pub r#type: Json,
    /// Whether the field must be present (§36).
    #[serde(default)]
    pub required: bool,
    /// Whether the field may change after creation (§39).
    #[serde(default = "yes")]
    pub mutable: bool,
    /// The inclusive lower bound for a numeric field.
    #[serde(default)]
    pub minimum: Option<f64>,
    /// The inclusive upper bound for a numeric field.
    #[serde(default)]
    pub maximum: Option<f64>,
    /// The accepted values, when the field is a closed set.
    #[serde(default)]
    pub r#enum: Option<Vec<Json>>,
    /// The value used when the field is absent (§40).
    #[serde(default)]
    pub default: Option<Json>,
    /// Anything else.
    #[serde(flatten)]
    pub extra: Map<String, Json>,
}

fn yes() -> bool {
    true
}

/// A predicate type definition (§41).
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct PredicateDef {
    /// The canonical symbol reference.
    #[serde(default)]
    pub r#ref: String,
    /// Always `PredicateType`.
    #[serde(default)]
    pub kind: String,
    /// What the predicate means. Semantics must be explicit (§57).
    #[serde(default)]
    pub description: String,
    /// What may appear as the subject (§42).
    #[serde(default)]
    pub subject: EndpointSpec,
    /// What may appear as the object (§43, §44).
    #[serde(default)]
    pub object: EndpointSpec,
    /// Whether one subject has at most one true object (§45).
    ///
    /// This is an *epistemic* statement, not a storage constraint: a functional
    /// predicate with two competing objects is a contested belief, which the
    /// engine must be able to store in order to report it (§46, §95).
    #[serde(default)]
    pub functional: bool,
    /// Whether absence of a claim means unknown rather than false (§51).
    #[serde(default = "yes")]
    pub open_world: bool,
    /// Whether the recorded set is claimed to be exhaustive.
    #[serde(default)]
    pub complete: bool,
    /// Anything else, including the algebraic hints of §52–§55.
    #[serde(flatten)]
    pub extra: Map<String, Json>,
}

/// What may occupy one end of a Proposition or structural edge.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct EndpointSpec {
    /// Permitted Concept types, as canonical references.
    #[serde(default)]
    pub concept_types: Vec<String>,
    /// Permitted Core element kinds, e.g. `Concept`, `Assertion`.
    #[serde(default)]
    pub kinds: Vec<String>,
    /// Permitted Literal datatypes, when a Literal is allowed (§44).
    #[serde(default)]
    pub datatypes: Vec<String>,
    /// Anything else.
    #[serde(flatten)]
    pub extra: Map<String, Json>,
}

impl EndpointSpec {
    /// Whether this endpoint declares no restriction at all.
    pub fn is_unconstrained(&self) -> bool {
        self.concept_types.is_empty() && self.kinds.is_empty() && self.datatypes.is_empty()
    }
}

/// A Facet definition (§58).
///
/// A Facet is a validated namespaced extension, not an untyped metadata bag
/// (§240.31) — which is exactly what KIP 1.x `metadata` had become.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct FacetDef {
    /// The canonical symbol reference.
    #[serde(default)]
    pub r#ref: String,
    /// Always `FacetDefinition`.
    #[serde(default)]
    pub kind: String,
    /// What the Facet carries.
    #[serde(default)]
    pub description: String,
    /// Whether members not named here are rejected (§60).
    #[serde(default)]
    pub closed: bool,
    /// Which elements may carry this Facet.
    #[serde(default)]
    pub applicable_to: EndpointSpec,
    /// The declared members.
    #[serde(default)]
    pub fields: BTreeMap<String, FieldSpec>,
    /// Advisory guidance.
    #[serde(default)]
    pub model_hints: Option<Json>,
    /// Anything else.
    #[serde(flatten)]
    pub extra: Map<String, Json>,
}

/// A structural field definition (§62).
///
/// Structural fields are record topology, not semantic Propositions (§64): a
/// claim *about* a structural relation is a separate Proposition plus
/// Assertion.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct StructuralFieldDef {
    /// The canonical symbol reference.
    #[serde(default)]
    pub r#ref: String,
    /// Always `StructuralFieldDefinition`.
    #[serde(default)]
    pub kind: String,
    /// What the field connects.
    #[serde(default)]
    pub description: String,
    /// Which elements may carry the field.
    #[serde(default)]
    pub source: EndpointSpec,
    /// What the field may point at.
    #[serde(default)]
    pub target: EndpointSpec,
    /// How many references are permitted (§47).
    #[serde(default)]
    pub cardinality: Cardinality,
    /// Whether edge order is meaningful (§66).
    ///
    /// Order is not causality: `has_step` being ordered says step 3 follows
    /// step 2, never that it was caused by it (§47 of the Core model).
    #[serde(default)]
    pub ordered: bool,
    /// Whether the same target may appear twice.
    #[serde(default)]
    pub unique: bool,
    /// Anything else.
    #[serde(flatten)]
    pub extra: Map<String, Json>,
}

/// A structural field's permitted reference count.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Cardinality {
    /// The minimum, zero when unwritten.
    #[serde(default)]
    pub min: u32,
    /// The maximum; `None` means unbounded.
    #[serde(default)]
    pub max: Option<u32>,
}

impl SchemaPackage {
    /// Parses an artifact and checks the identity it declares is coherent.
    pub fn parse(source: &str) -> Result<Self, KipError> {
        let package: SchemaPackage = serde_json::from_str(source).map_err(|err| {
            KipError::new(
                anda_kip::KipErrorCode::ArtifactParseError,
                format!("this is not a readable Schema Package artifact: {err}"),
            )
        })?;
        package.package_ref()?;
        Ok(package)
    }

    /// The package's exact identity.
    ///
    /// `package_ref` is the authority when present, and `package_id@version`
    /// must agree with it: an artifact whose two spellings of its own identity
    /// disagree is one that could be installed under a name its symbols do not
    /// claim, so it is rejected rather than reconciled.
    pub fn package_ref(&self) -> Result<PackageRef, KipError> {
        let joined = format!("{}@{}", self.manifest.package_id, self.manifest.version);
        let declared = if self.manifest.package_ref.is_empty() {
            joined.clone()
        } else {
            self.manifest.package_ref.clone()
        };
        if !self.manifest.package_id.is_empty()
            && !self.manifest.version.is_empty()
            && declared != joined
        {
            return Err(KipError::new(
                anda_kip::KipErrorCode::CapsuleValidationFailed,
                format!(
                    "the artifact calls itself {declared:?} but its package_id and version join to \
                     {joined:?}"
                ),
            ));
        }
        declared.parse()
    }

    /// Whether this package defines a symbol of the given kind.
    pub fn defines(&self, kind: SymbolKind, name: &str) -> bool {
        match kind {
            SymbolKind::ConceptType => self.definitions.concept_types.contains_key(name),
            SymbolKind::PredicateType => self.definitions.predicates.contains_key(name),
            SymbolKind::Facet => self.definitions.facets.contains_key(name),
            SymbolKind::StructuralField => self.definitions.structural_fields.contains_key(name),
            SymbolKind::Enum => self.definitions.enums.contains_key(name),
        }
    }

    /// The local names this package defines for one symbol kind.
    pub fn symbols(&self, kind: SymbolKind) -> Vec<&str> {
        fn names<T>(map: &BTreeMap<String, T>) -> Vec<&str> {
            map.keys().map(String::as_str).collect()
        }
        match kind {
            SymbolKind::ConceptType => names(&self.definitions.concept_types),
            SymbolKind::PredicateType => names(&self.definitions.predicates),
            SymbolKind::Facet => names(&self.definitions.facets),
            SymbolKind::StructuralField => names(&self.definitions.structural_fields),
            SymbolKind::Enum => names(&self.definitions.enums),
        }
    }

    /// The canonical reference for one of this package's local names.
    pub fn symbol_ref(&self, name: &str) -> Result<SymbolRef, KipError> {
        Ok(SymbolRef {
            package: self.package_ref()?,
            name: name.to_string(),
        })
    }

    /// A Concept type definition by local name.
    pub fn concept_type(&self, name: &str) -> Option<&ConceptTypeDef> {
        self.definitions.concept_types.get(name)
    }

    /// A predicate definition by local name.
    pub fn predicate(&self, name: &str) -> Option<&PredicateDef> {
        self.definitions.predicates.get(name)
    }

    /// A Facet definition by local name.
    pub fn facet(&self, name: &str) -> Option<&FacetDef> {
        self.definitions.facets.get(name)
    }

    /// A structural field definition by local name.
    pub fn structural_field(&self, name: &str) -> Option<&StructuralFieldDef> {
        self.definitions.structural_fields.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The standard profile, as shipped by the specification repository.
    ///
    /// Parsing the real artifact rather than a hand-written imitation is the
    /// point: an imitation would drift toward whatever this module already
    /// supports, and stop testing anything.
    const COGNITIVE_MEMORY: &str = crate::profiles::COGNITIVE_MEMORY;

    fn profile() -> SchemaPackage {
        SchemaPackage::parse(COGNITIVE_MEMORY).unwrap()
    }

    #[test]
    fn the_standard_profile_parses_into_the_model() {
        let package = profile();
        assert_eq!(package.format, "KIP-Schema-Package");
        assert_eq!(
            package.package_ref().unwrap().to_string(),
            "kip://profiles/cognitive-memory@2.0.0"
        );
        // It depends on Core, exactly.
        let core = &package.dependencies[0];
        assert_eq!(core.package_ref, "kip://core@2.0.0");
        assert!(core.required);

        assert!(package.defines(SymbolKind::ConceptType, "Person"));
        assert!(package.defines(SymbolKind::PredicateType, "prefers"));
        assert!(package.defines(SymbolKind::Facet, "MnemonicState"));
        assert!(package.defines(SymbolKind::StructuralField, "has_step"));
        assert!(!package.defines(SymbolKind::ConceptType, "prefers"));
    }

    #[test]
    fn a_functional_predicate_is_an_epistemic_claim_not_a_storage_rule() {
        // Spec §46, §95: `functional` produces conflict diagnostics. If it
        // rejected writes, the Nexus could not store disagreement at all.
        let package = profile();
        let prefers = package.predicate("prefers").unwrap();
        assert!(!prefers.functional);
        assert!(prefers.open_world, "absence of a claim is not falsity");
        assert!(!prefers.complete);
        assert_eq!(
            prefers.subject.concept_types,
            vec!["kip://profiles/cognitive-memory@2.0.0/Person"]
        );
        assert_eq!(prefers.object.kinds, vec!["Concept"]);
    }

    #[test]
    fn an_ordered_structural_field_declares_order_not_causality() {
        let package = profile();
        let has_step = package.structural_field("has_step").unwrap();
        assert!(has_step.ordered);
        assert!(has_step.unique);
        assert_eq!(has_step.cardinality.min, 0);
        assert_eq!(has_step.cardinality.max, None, "unbounded");
    }

    #[test]
    fn a_closed_facet_names_every_member_it_allows() {
        let package = profile();
        let mnemonic = package.facet("MnemonicState").unwrap();
        assert!(mnemonic.closed);
        let strength = &mnemonic.fields["memory_strength"];
        assert_eq!(strength.minimum, Some(0.0));
        assert_eq!(strength.maximum, Some(1.0));
        assert!(strength.mutable);
        assert!(!strength.required);
        // A nullable member declares a list of accepted types.
        assert!(mnemonic.fields["last_metabolized_at"].r#type.is_array());
    }

    #[test]
    fn an_artifact_that_disagrees_with_its_own_identity_is_refused() {
        // A package installed under a name its symbols do not claim would make
        // every `kip://.../Symbol` in it resolve to the wrong place.
        let source = r#"{
            "manifest": {
                "package_id": "kip://acme/hr",
                "version": "1.0.0",
                "package_ref": "kip://acme/payroll@1.0.0"
            }
        }"#;
        let err = SchemaPackage::parse(source).unwrap_err();
        assert!(err.message.contains("kip://acme/payroll@1.0.0"));

        // Agreeing spellings are fine, and so is omitting the joined one.
        assert!(
            SchemaPackage::parse(
                r#"{"manifest": {"package_id": "kip://acme/hr", "version": "1.0.0"}}"#
            )
            .is_ok()
        );
    }

    #[test]
    fn an_unreadable_artifact_is_a_parse_error_not_a_silent_empty_package() {
        let err = SchemaPackage::parse("{not json").unwrap_err();
        assert_eq!(err.name(), "ArtifactParseError");
    }

    #[test]
    fn unknown_fields_survive_a_round_trip() {
        // A package written against a later format revision must stay
        // readable, and re-encoding it must not change its digest-covered
        // content by dropping what this engine did not recognize.
        let source = r#"{
            "format": "KIP-Schema-Package",
            "manifest": {"package_id": "kip://acme/hr", "version": "1.0.0", "vendor": "acme"},
            "future_section": {"a": 1}
        }"#;
        let package = SchemaPackage::parse(source).unwrap();
        assert_eq!(package.manifest.extra["vendor"], "acme");
        let encoded = serde_json::to_value(&package).unwrap();
        assert_eq!(encoded["future_section"]["a"], 1);
        assert_eq!(encoded["manifest"]["vendor"], "acme");
    }
}
