//! # The Schema Environment
//!
//! The exact set of package versions and aliases active in one MemorySpace
//! (Spec §23). It answers one question, and the whole engine depends on it
//! answering deterministically:
//!
//! ```text
//! this local name, in this Space, right now → which exact symbol?
//! ```
//!
//! Three rules shape the answer.
//!
//! **Ambiguity fails; it never guesses** (§20, §240.10). If two installed
//! packages both define `Person`, the runtime says so and lists the
//! candidates. Picking one would silently bind data to a meaning the caller
//! did not choose, and the binding is persisted.
//!
//! **Define-before-use** (§110). An unknown type in an ordinary write is an
//! error, not an invitation to create a schema. Schema authoring is a separate
//! governance process, which is the whole point of moving Schema out of the
//! graph.
//!
//! **Read resolution is wider than write resolution** (§80, §85). Data written
//! under a now-deprecated package must keep resolving — its meaning did not
//! change when the Space's default did — while new writes are steered to the
//! active version.

use anda_kip::{ACTIVITY_CLASSES, EVIDENCE_CLASSES, Json, KipError, KipErrorCode};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Arc, LazyLock},
};

use super::package::{Definitions, Manifest, SchemaPackage};
use super::symbol::{PackageRef, SymbolKind, SymbolRef, is_qualified};

/// The reserved package that describes Core itself (§158).
pub const CORE_PACKAGE_ID: &str = "kip://core";

/// The Core package this engine implements.
pub static CORE_PACKAGE_REF: LazyLock<PackageRef> =
    LazyLock::new(|| "kip://core@2.0.0".parse().expect("a valid Core reference"));

/// The built-in `kip://core@2.0.0` artifact.
///
/// Core defines the open registries and nothing else: element kinds are fixed
/// and not redefinable (§240.22), and Concept types are schema-defined, so
/// Core deliberately declares none (Core Data Model §49). A Space with only
/// Core installed can therefore hold Propositions and Assertions but cannot
/// type a Concept until a profile is activated — which is define-before-use
/// working as intended.
///
/// The registry values are read from `anda_kip`'s constants rather than
/// duplicated here, so the package cannot drift from the protocol crate.
pub static CORE_PACKAGE: LazyLock<Arc<SchemaPackage>> = LazyLock::new(|| {
    let registry = |values: &[&str], description: &str| {
        serde_json::json!({
            "values": values,
            "description": description,
        })
    };
    Arc::new(SchemaPackage {
        format: "KIP-Schema-Package".into(),
        format_version: "2.0".into(),
        manifest: Manifest {
            package_id: CORE_PACKAGE_ID.into(),
            version: "2.0.0".into(),
            package_ref: CORE_PACKAGE_REF.to_string(),
            name: "KIP Core".into(),
            description: "The Core element kinds and open registries of KIP 2.0.".into(),
            ..Default::default()
        },
        definitions: Definitions {
            registry_extensions: BTreeMap::from([
                (
                    "evidence_classes".to_string(),
                    registry(EVIDENCE_CLASSES, "Baseline Evidence classes (§15.2)."),
                ),
                (
                    "activity_classes".to_string(),
                    registry(ACTIVITY_CLASSES, "Baseline Activity classes (§16.2)."),
                ),
                (
                    "stances".to_string(),
                    registry(
                        &["support", "reject", "uncertain"],
                        "The stances an Assertion may take (§13.4).",
                    ),
                ),
                (
                    "assertion_modes".to_string(),
                    registry(
                        &[
                            "observed",
                            "stated",
                            "inferred",
                            "predicted",
                            "hypothetical",
                            "imported",
                        ],
                        "How an Assertion was arrived at (§13.5).",
                    ),
                ),
            ]),
            ..Default::default()
        },
        ..Default::default()
    })
});

/// A package's activation state in one Space (Spec §81).
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PackageState {
    /// Available locally, not active for ordinary writes (§82).
    #[default]
    Installed,
    /// Usable to inspect imported data, never a default for local cognition
    /// (§83). This is where an untrusted or foreign package lands.
    ValidationOnly,
    /// Permitted by Space Governance, and eligible for new writes (§84).
    Active,
    /// Readable; new writes should avoid it (§85).
    Deprecated,
    /// Barred from new operations after a security or semantic incident;
    /// existing data stays inspectable (§86).
    Blocked,
    /// Isolated pending review. It MUST NOT affect normal resolution (§87).
    Quarantined,
}

impl PackageState {
    /// Whether a new write may bind data to this package.
    pub fn allows_write(&self) -> bool {
        matches!(self, PackageState::Active | PackageState::Deprecated)
    }

    /// Whether an existing reference into this package still resolves.
    ///
    /// Blocked packages included: data already bound to a blocked package does
    /// not stop meaning what it meant, and refusing to resolve it would make
    /// the incident unauditable (§86). What a blocked package cannot do is
    /// accept new writes.
    pub fn allows_read(&self) -> bool {
        !matches!(self, PackageState::Quarantined)
    }

    /// Whether this package participates in bare local-name resolution.
    ///
    /// Narrower than reads on purpose: a local name is the caller not saying
    /// which package they meant, so only packages the Space has actually
    /// endorsed may answer.
    pub fn answers_local_names(&self) -> bool {
        matches!(self, PackageState::Active | PackageState::Deprecated)
    }
}

/// What a resolution is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Intent {
    /// Reading, or validating data that already exists.
    Read,
    /// Binding new data to a symbol.
    Write,
}

/// The immutable Schema Lock of one environment version (§25).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct SchemaLock {
    /// The resolution set: one exact version per package id.
    #[serde(default)]
    pub packages: BTreeMap<String, String>,
    /// Each package's activation state.
    #[serde(default)]
    pub states: BTreeMap<String, PackageState>,
    /// The version new writes bind to, when it differs from the read version
    /// (§80).
    #[serde(default)]
    pub write_defaults: BTreeMap<String, String>,
    /// Model-friendly aliases: alias → canonical symbol reference (§21).
    #[serde(default)]
    pub aliases: BTreeMap<String, String>,
}

/// A resolved Schema Environment, ready to answer symbol questions.
#[derive(Clone, Debug)]
pub struct SchemaEnvironment {
    /// The environment version. Every activation mints a new one (§143).
    pub version: u64,
    /// The lock this environment resolved.
    pub lock: SchemaLock,
    /// The artifacts, keyed by canonical package reference.
    artifacts: BTreeMap<String, Arc<SchemaPackage>>,
}

impl Default for SchemaEnvironment {
    fn default() -> Self {
        Self::core_only()
    }
}

impl SchemaEnvironment {
    /// The environment a Space starts with: Core, active, and nothing else.
    pub fn core_only() -> Self {
        let mut lock = SchemaLock::default();
        lock.packages
            .insert(CORE_PACKAGE_ID.to_string(), "2.0.0".to_string());
        lock.states
            .insert(CORE_PACKAGE_ID.to_string(), PackageState::Active);
        Self {
            version: 0,
            lock,
            artifacts: BTreeMap::from([(CORE_PACKAGE_REF.to_string(), CORE_PACKAGE.clone())]),
        }
    }

    /// Builds an environment from a lock and the artifacts it names.
    ///
    /// A lock naming a package whose artifact is absent fails here rather than
    /// at the first query: an environment that resolves some of its own lock is
    /// worse than one that refuses to exist, because the failure would surface
    /// as a missing symbol somewhere unrelated (§182).
    pub fn resolve(
        version: u64,
        lock: SchemaLock,
        available: &BTreeMap<String, Arc<SchemaPackage>>,
    ) -> Result<Self, KipError> {
        let mut artifacts = BTreeMap::new();
        for (package_id, package_version) in &lock.packages {
            let package_ref = format!("{package_id}@{package_version}");
            let artifact = if package_ref == CORE_PACKAGE_REF.to_string() {
                available
                    .get(&package_ref)
                    .cloned()
                    .unwrap_or_else(|| CORE_PACKAGE.clone())
            } else {
                available.get(&package_ref).cloned().ok_or_else(|| {
                    KipError::new(
                        KipErrorCode::SchemaPackageUnavailable,
                        format!(
                            "the Schema Lock names {package_ref} but its artifact is not installed \
                             in this Nexus"
                        ),
                    )
                })?
            };
            artifacts.insert(package_ref, artifact);
        }
        Ok(Self {
            version,
            lock,
            artifacts,
        })
    }

    /// The exact reference a package id resolves to in this environment.
    pub fn package_ref(&self, package_id: &str, intent: Intent) -> Option<String> {
        let version = match intent {
            Intent::Write => self
                .lock
                .write_defaults
                .get(package_id)
                .or_else(|| self.lock.packages.get(package_id)),
            Intent::Read => self.lock.packages.get(package_id),
        }?;
        Some(format!("{package_id}@{version}"))
    }

    /// A package's activation state, defaulting to `installed`.
    pub fn state(&self, package_id: &str) -> PackageState {
        self.lock
            .states
            .get(package_id)
            .copied()
            .unwrap_or_default()
    }

    /// An installed artifact by exact reference.
    pub fn artifact(&self, package_ref: &str) -> Option<&Arc<SchemaPackage>> {
        self.artifacts.get(package_ref)
    }

    /// Resolves a model-facing name to one exact symbol.
    ///
    /// Accepts a canonical reference, a configured alias, or a bare local name,
    /// in that order.
    pub fn resolve_symbol(
        &self,
        kind: SymbolKind,
        name: &str,
        intent: Intent,
    ) -> Result<SymbolRef, KipError> {
        if is_qualified(name) {
            return self.check_qualified(kind, &name.parse::<SymbolRef>()?, intent);
        }
        if let Some(target) = self.lock.aliases.get(name) {
            // An alias is a resolution aid, not an identity: it resolves to an
            // exact symbol, which is then checked like any other (§21, §22).
            let symbol: SymbolRef = target.parse().map_err(|err: KipError| {
                KipError::new(
                    KipErrorCode::SchemaSymbolNotFound,
                    format!(
                        "the alias {name:?} points at {target:?}, which is not a symbol: {err}"
                    ),
                )
            })?;
            return self.check_qualified(kind, &symbol, intent);
        }
        self.resolve_local(kind, name, intent)
    }

    /// Resolves a bare local name across the packages that may answer.
    fn resolve_local(
        &self,
        kind: SymbolKind,
        name: &str,
        intent: Intent,
    ) -> Result<SymbolRef, KipError> {
        let mut candidates: Vec<SymbolRef> = Vec::new();
        for package_id in self.lock.packages.keys() {
            if !self.state(package_id).answers_local_names() {
                continue;
            }
            let Some(package_ref) = self.package_ref(package_id, intent) else {
                continue;
            };
            let Some(artifact) = self.artifacts.get(&package_ref) else {
                continue;
            };
            if artifact.defines(kind, name) {
                candidates.push(artifact.symbol_ref(name)?);
            }
        }

        match candidates.len() {
            1 => Ok(candidates.remove(0)),
            0 => Err(KipError::new(
                KipErrorCode::SchemaSymbolNotFound,
                format!(
                    "no active Schema Package defines the {kind} {name:?} in this Space; a data \
                     mutation never creates a schema definition, so it must be published and \
                     activated first"
                ),
            )),
            _ => {
                // Spec §184: tell the Agent how to recover, by name.
                let listed = candidates
                    .iter()
                    .map(SymbolRef::to_string)
                    .collect::<Vec<_>>()
                    .join("\n  ");
                Err(KipError::new(
                    KipErrorCode::SchemaSymbolAmbiguous,
                    format!("the {kind} {name:?} is defined by more than one active package"),
                )
                .with_hint(format!(
                    "use an exact qualified reference or a configured alias; candidates:\n  \
                     {listed}"
                ))
                .with_details(Json::Array(
                    candidates
                        .iter()
                        .map(|symbol| Json::String(symbol.to_string()))
                        .collect(),
                )))
            }
        }
    }

    /// Checks that an exact symbol exists here and may be used for this intent.
    fn check_qualified(
        &self,
        kind: SymbolKind,
        symbol: &SymbolRef,
        intent: Intent,
    ) -> Result<SymbolRef, KipError> {
        let package_ref = symbol.package.to_string();
        let state = self.state(&symbol.package.package_id);

        let artifact = self.artifacts.get(&package_ref).ok_or_else(|| {
            KipError::new(
                KipErrorCode::SchemaPackageUnavailable,
                format!(
                    "{package_ref} is not part of this Space's Schema Environment, so \
                     {symbol} cannot be resolved"
                ),
            )
        })?;

        if !artifact.defines(kind, &symbol.name) {
            return Err(KipError::new(
                KipErrorCode::SchemaSymbolNotFound,
                format!("{package_ref} defines no {kind} named {:?}", symbol.name),
            ));
        }

        match intent {
            Intent::Read if !state.allows_read() => Err(KipError::new(
                KipErrorCode::ProtectedSchemaState,
                format!("{package_ref} is quarantined and takes no part in schema resolution"),
            )),
            Intent::Write if !state.allows_write() => Err(KipError::new(
                KipErrorCode::ProtectedSchemaState,
                format!(
                    "{package_ref} is {:?} in this Space and cannot bind new data; existing data \
                     that references it still resolves",
                    state
                ),
            )),
            _ => Ok(symbol.clone()),
        }
    }

    /// The definition behind a resolved symbol, when the caller needs it.
    pub fn definition_package(&self, symbol: &SymbolRef) -> Option<&Arc<SchemaPackage>> {
        self.artifacts.get(&symbol.package.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const COGNITIVE_MEMORY: &str = crate::profiles::COGNITIVE_MEMORY;

    /// A second package that also defines `Person`, to force ambiguity.
    const ACME_HR: &str = r#"{
        "format": "KIP-Schema-Package",
        "manifest": {"package_id": "kip://acme/hr", "version": "1.0.0"},
        "definitions": {
            "concept_types": {
                "Person": {"kind": "ConceptType", "description": "An employee."},
                "Employee": {"kind": "ConceptType", "description": "An employee."}
            }
        }
    }"#;

    fn available() -> BTreeMap<String, Arc<SchemaPackage>> {
        let mut map = BTreeMap::new();
        for source in [COGNITIVE_MEMORY, ACME_HR] {
            let package = SchemaPackage::parse(source).unwrap();
            map.insert(
                package.package_ref().unwrap().to_string(),
                Arc::new(package),
            );
        }
        map
    }

    fn lock(entries: &[(&str, &str, PackageState)]) -> SchemaLock {
        let mut lock = SchemaLock::default();
        for (id, version, state) in entries {
            lock.packages.insert(id.to_string(), version.to_string());
            lock.states.insert(id.to_string(), *state);
        }
        lock
    }

    fn profile_env() -> SchemaEnvironment {
        SchemaEnvironment::resolve(
            1,
            lock(&[(
                "kip://profiles/cognitive-memory",
                "2.0.0",
                PackageState::Active,
            )]),
            &available(),
        )
        .unwrap()
    }

    #[test]
    fn a_local_name_resolves_to_one_exact_symbol() {
        // Spec §19: local names are sugar, resolved deterministically before
        // execution. What gets persisted is always the exact reference.
        let env = profile_env();
        let symbol = env
            .resolve_symbol(SymbolKind::ConceptType, "Person", Intent::Write)
            .unwrap();
        assert_eq!(
            symbol.to_string(),
            "kip://profiles/cognitive-memory@2.0.0/Person"
        );
        // The same name resolves the same way through its canonical spelling.
        assert_eq!(
            env.resolve_symbol(SymbolKind::ConceptType, &symbol.to_string(), Intent::Write)
                .unwrap(),
            symbol
        );
    }

    #[test]
    fn an_ambiguous_local_name_fails_and_names_the_candidates() {
        // Spec §20 and §240.10: the runtime must not guess. Guessing would
        // bind persisted data to a meaning nobody chose.
        let env = SchemaEnvironment::resolve(
            2,
            lock(&[
                (
                    "kip://profiles/cognitive-memory",
                    "2.0.0",
                    PackageState::Active,
                ),
                ("kip://acme/hr", "1.0.0", PackageState::Active),
            ]),
            &available(),
        )
        .unwrap();

        let err = env
            .resolve_symbol(SymbolKind::ConceptType, "Person", Intent::Write)
            .unwrap_err();
        assert_eq!(err.name(), "SchemaSymbolAmbiguous");
        // §184: the recovery hint has to be actionable, which means naming
        // both candidates.
        assert!(err.effective_hint().contains("kip://acme/hr@1.0.0/Person"));
        assert!(
            err.effective_hint()
                .contains("kip://profiles/cognitive-memory@2.0.0/Person")
        );

        // An unambiguous name in the same environment still resolves, and the
        // qualified spelling always does.
        assert!(
            env.resolve_symbol(SymbolKind::ConceptType, "Employee", Intent::Write)
                .is_ok()
        );
        assert!(
            env.resolve_symbol(
                SymbolKind::ConceptType,
                "kip://acme/hr@1.0.0/Person",
                Intent::Write
            )
            .is_ok()
        );
    }

    #[test]
    fn an_unknown_type_is_an_error_not_a_new_definition() {
        // Spec §110: define-before-use. A data write that could create schema
        // would put schema authority back inside ordinary cognition, which is
        // the 1.x mistake this version exists to fix.
        let env = profile_env();
        let err = env
            .resolve_symbol(SymbolKind::ConceptType, "Spaceship", Intent::Write)
            .unwrap_err();
        assert_eq!(err.name(), "SchemaSymbolNotFound");
        assert!(err.message.contains("never creates a schema definition"));
    }

    #[test]
    fn a_deprecated_package_keeps_reading_and_a_blocked_one_stops_writing() {
        // Spec §85, §86: data does not stop meaning what it meant when the
        // Space changes its mind about a package.
        let deprecated = SchemaEnvironment::resolve(
            3,
            lock(&[(
                "kip://profiles/cognitive-memory",
                "2.0.0",
                PackageState::Deprecated,
            )]),
            &available(),
        )
        .unwrap();
        assert!(
            deprecated
                .resolve_symbol(SymbolKind::ConceptType, "Person", Intent::Write)
                .is_ok(),
            "deprecated is discouraged, not forbidden"
        );

        let blocked = SchemaEnvironment::resolve(
            4,
            lock(&[(
                "kip://profiles/cognitive-memory",
                "2.0.0",
                PackageState::Blocked,
            )]),
            &available(),
        )
        .unwrap();
        let qualified = "kip://profiles/cognitive-memory@2.0.0/Person";
        assert!(
            blocked
                .resolve_symbol(SymbolKind::ConceptType, qualified, Intent::Read)
                .is_ok(),
            "existing data stays inspectable"
        );
        let err = blocked
            .resolve_symbol(SymbolKind::ConceptType, qualified, Intent::Write)
            .unwrap_err();
        assert_eq!(err.name(), "ProtectedSchemaState");
    }

    #[test]
    fn a_quarantined_package_takes_no_part_in_resolution() {
        // Spec §87: it MUST NOT affect normal schema resolution — including
        // not making an otherwise-unambiguous local name ambiguous.
        let env = SchemaEnvironment::resolve(
            5,
            lock(&[
                (
                    "kip://profiles/cognitive-memory",
                    "2.0.0",
                    PackageState::Active,
                ),
                ("kip://acme/hr", "1.0.0", PackageState::Quarantined),
            ]),
            &available(),
        )
        .unwrap();

        let symbol = env
            .resolve_symbol(SymbolKind::ConceptType, "Person", Intent::Write)
            .unwrap();
        assert_eq!(
            symbol.to_string(),
            "kip://profiles/cognitive-memory@2.0.0/Person"
        );
        let err = env
            .resolve_symbol(
                SymbolKind::ConceptType,
                "kip://acme/hr@1.0.0/Person",
                Intent::Read,
            )
            .unwrap_err();
        assert_eq!(err.name(), "ProtectedSchemaState");
    }

    #[test]
    fn an_alias_resolves_to_an_exact_symbol_without_becoming_one() {
        // Spec §21, §22: aliases are resolution aids. Persisted elements store
        // the exact reference, so changing an alias later cannot rewrite them.
        let mut lock = lock(&[
            (
                "kip://profiles/cognitive-memory",
                "2.0.0",
                PackageState::Active,
            ),
            ("kip://acme/hr", "1.0.0", PackageState::Active),
        ]);
        lock.aliases.insert(
            "HR.Person".to_string(),
            "kip://acme/hr@1.0.0/Person".to_string(),
        );
        let env = SchemaEnvironment::resolve(6, lock, &available()).unwrap();

        let symbol = env
            .resolve_symbol(SymbolKind::ConceptType, "HR.Person", Intent::Write)
            .unwrap();
        assert_eq!(symbol.to_string(), "kip://acme/hr@1.0.0/Person");
        // The alias does not disambiguate the bare name it aliases.
        assert!(
            env.resolve_symbol(SymbolKind::ConceptType, "Person", Intent::Write)
                .is_err()
        );
    }

    #[test]
    fn a_symbol_of_the_wrong_kind_is_not_found() {
        // `prefers` is a predicate, so asking for a Concept type by that name
        // must miss rather than return a predicate definition.
        let env = profile_env();
        assert!(
            env.resolve_symbol(SymbolKind::PredicateType, "prefers", Intent::Write)
                .is_ok()
        );
        let err = env
            .resolve_symbol(SymbolKind::ConceptType, "prefers", Intent::Write)
            .unwrap_err();
        assert_eq!(err.name(), "SchemaSymbolNotFound");
    }

    #[test]
    fn a_lock_that_names_a_missing_artifact_refuses_to_resolve() {
        // Spec §182: a half-resolved environment would surface its failure as
        // a missing symbol somewhere unrelated.
        let err = SchemaEnvironment::resolve(
            7,
            lock(&[("kip://acme/nowhere", "9.9.9", PackageState::Active)]),
            &available(),
        )
        .unwrap_err();
        assert_eq!(err.name(), "SchemaPackageUnavailable");
    }

    #[test]
    fn core_is_present_without_being_installed_and_types_nothing() {
        // Spec §158: conformance to Core does not depend on package
        // activation. Core Data Model §49: Concept types are schema-defined,
        // so Core declares none — a Space with only Core cannot type a
        // Concept, and that is define-before-use working.
        let env = SchemaEnvironment::core_only();
        assert_eq!(env.state(CORE_PACKAGE_ID), PackageState::Active);
        assert!(
            env.artifact(&CORE_PACKAGE_REF.to_string())
                .unwrap()
                .definitions
                .concept_types
                .is_empty()
        );
        assert!(
            env.resolve_symbol(SymbolKind::ConceptType, "Person", Intent::Write)
                .is_err()
        );

        // The registries it does define come from `anda_kip`, so they cannot
        // drift from the protocol crate.
        let core = env.artifact(&CORE_PACKAGE_REF.to_string()).unwrap();
        let classes = &core.definitions.registry_extensions["evidence_classes"]["values"];
        assert_eq!(classes.as_array().unwrap().len(), EVIDENCE_CLASSES.len());
        assert!(classes.as_array().unwrap().contains(&Json::from("message")));
    }

    #[test]
    fn a_write_default_can_differ_from_the_read_version() {
        // Spec §80: a Space may read one version while binding new data to
        // another during a dual-version period (§136).
        let mut lock = lock(&[(
            "kip://profiles/cognitive-memory",
            "2.0.0",
            PackageState::Active,
        )]);
        lock.write_defaults.insert(
            "kip://profiles/cognitive-memory".to_string(),
            "2.1.0".to_string(),
        );
        let env = SchemaEnvironment::resolve(8, lock, &available()).unwrap();
        assert_eq!(
            env.package_ref("kip://profiles/cognitive-memory", Intent::Read),
            Some("kip://profiles/cognitive-memory@2.0.0".to_string())
        );
        assert_eq!(
            env.package_ref("kip://profiles/cognitive-memory", Intent::Write),
            Some("kip://profiles/cognitive-memory@2.1.0".to_string())
        );
    }
}
