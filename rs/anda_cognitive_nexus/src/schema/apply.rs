//! # Applying a Schema Environment to an element
//!
//! The seam between resolution and storage: the mutation path hands over what
//! it knows about an element, and gets back either the exact symbols to persist
//! or a [`Validation`] explaining what the declared contract refuses.
//!
//! The endpoint facts are supplied by the caller rather than looked up here,
//! because deciding *what a reference points at* is a storage question and this
//! module has no storage. That keeps schema validation a pure function of
//! `(environment, facts)`, which is what makes it testable without a database
//! and deterministic across engines (§99).

use anda_kip::{ElementKind, Json, KipError, Map};

use super::env::{Intent, SchemaEnvironment};
use super::package::{ConceptTypeDef, EndpointSpec, FacetDef, PredicateDef, StructuralFieldDef};
use super::symbol::{SymbolKind, SymbolRef};
use super::validate::{self, Severity, Validation, Violation};

/// What the caller knows about one end of a Proposition or structural edge.
#[derive(Clone, Debug, PartialEq)]
pub enum EndpointFacts {
    /// A reference to a Cognitive Element.
    Element {
        /// Which Core kind it is.
        kind: ElementKind,
        /// Its `schema_ref`, when it is a typed Concept.
        schema_ref: Option<String>,
    },
    /// A Literal value.
    Literal {
        /// The Literal's datatype symbol.
        datatype: String,
    },
    /// A reference this engine cannot resolve locally — a canonical identity
    /// or a foreign Space reference.
    ///
    /// Unresolvable is not the same as wrong: the endpoint's type is simply
    /// unknown here, and inventing a violation from an unknown would reject
    /// legitimate data (§46 of the invariants: missing schema causes an
    /// explicit unresolved state, not a guess).
    Unresolved,
}

/// The Core kind name a package artifact spells.
fn kind_name(kind: ElementKind) -> &'static str {
    match kind {
        ElementKind::Concept => "Concept",
        ElementKind::Proposition => "Proposition",
        ElementKind::Assertion => "Assertion",
        ElementKind::Evidence => "Evidence",
        ElementKind::Activity => "Activity",
    }
}

/// Checks one endpoint against its declared constraints (§42–§44).
fn check_endpoint(
    schema_ref: &str,
    path: &str,
    spec: &EndpointSpec,
    facts: &EndpointFacts,
    into: &mut Validation,
) {
    if spec.is_unconstrained() {
        return;
    }
    let refuse = |message: String| Violation {
        code: "SCHEMA_ENDPOINT_NOT_ALLOWED".to_string(),
        schema_ref: schema_ref.to_string(),
        path: path.to_string(),
        message,
        severity: Severity::Error,
    };

    match facts {
        EndpointFacts::Unresolved => {}
        EndpointFacts::Literal { datatype } => {
            if spec.datatypes.is_empty() {
                into.push(refuse(
                    "the schema declares this endpoint an element reference, not a Literal".into(),
                ));
            } else if !spec.datatypes.iter().any(|allowed| allowed == datatype) {
                into.push(refuse(format!(
                    "a Literal of datatype {datatype} is not among the declared datatypes: {}",
                    spec.datatypes.join(", ")
                )));
            }
        }
        EndpointFacts::Element {
            kind,
            schema_ref: element_type,
        } => {
            if !spec.kinds.is_empty()
                && !spec
                    .kinds
                    .iter()
                    .any(|allowed| allowed.eq_ignore_ascii_case(kind_name(*kind)))
            {
                into.push(refuse(format!(
                    "a {} is not among the declared kinds: {}",
                    kind_name(*kind),
                    spec.kinds.join(", ")
                )));
                return;
            }
            if spec.concept_types.is_empty() {
                return;
            }
            if *kind != ElementKind::Concept {
                into.push(refuse(format!(
                    "the schema declares this endpoint a Concept of a specific type, and a {} \
                     cannot have one",
                    kind_name(*kind)
                )));
                return;
            }
            // A Concept whose type this engine has not been told is not a
            // Concept of the wrong type. Reporting one would turn a missing
            // lookup into a schema violation.
            if let Some(actual) = element_type
                && !spec.concept_types.iter().any(|allowed| allowed == actual)
            {
                into.push(refuse(format!(
                    "{actual} is not among the declared Concept types: {}",
                    spec.concept_types.join(", ")
                )));
            }
        }
    }
}

impl SchemaEnvironment {
    /// The Concept type definition behind a resolved symbol.
    pub fn concept_type_def(&self, symbol: &SymbolRef) -> Result<&ConceptTypeDef, KipError> {
        self.definition_package(symbol)
            .and_then(|package| package.concept_type(&symbol.name))
            .ok_or_else(|| missing(symbol, SymbolKind::ConceptType))
    }

    /// The predicate definition behind a resolved symbol.
    pub fn predicate_def(&self, symbol: &SymbolRef) -> Result<&PredicateDef, KipError> {
        self.definition_package(symbol)
            .and_then(|package| package.predicate(&symbol.name))
            .ok_or_else(|| missing(symbol, SymbolKind::PredicateType))
    }

    /// The Facet definition behind a resolved symbol.
    pub fn facet_def(&self, symbol: &SymbolRef) -> Result<&FacetDef, KipError> {
        self.definition_package(symbol)
            .and_then(|package| package.facet(&symbol.name))
            .ok_or_else(|| missing(symbol, SymbolKind::Facet))
    }

    /// The structural field definition behind a resolved symbol.
    pub fn structural_field_def(
        &self,
        symbol: &SymbolRef,
    ) -> Result<&StructuralFieldDef, KipError> {
        self.definition_package(symbol)
            .and_then(|package| package.structural_field(&symbol.name))
            .ok_or_else(|| missing(symbol, SymbolKind::StructuralField))
    }

    /// Resolves a Concept's type and validates its attributes and Facets.
    ///
    /// Returns the exact `schema_ref` to persist alongside the findings: the
    /// caller writes the canonical reference, never the local name it passed in
    /// (§13, §240.6).
    pub fn prepare_concept(
        &self,
        type_name: &str,
        attributes: &Map<String, Json>,
        facets: &Map<String, Json>,
        intent: Intent,
    ) -> Result<(SymbolRef, Validation), KipError> {
        let symbol = self.resolve_symbol(SymbolKind::ConceptType, type_name, intent)?;
        let def = self.concept_type_def(&symbol)?;
        let mut result =
            validate::validate_attributes(&symbol.to_string(), &def.attributes, attributes);
        result.extend(self.validate_facets(facets, ElementKind::Concept, intent)?);
        Ok((symbol, result))
    }

    /// Resolves each Facet symbol and validates its members.
    ///
    /// A Facet is a validated namespaced extension, so an unresolvable Facet
    /// symbol is an error rather than a pass-through: letting it through would
    /// restore exactly the untyped metadata bag KIP 2.0 removed (§240.31).
    pub fn validate_facets(
        &self,
        facets: &Map<String, Json>,
        carrier: ElementKind,
        intent: Intent,
    ) -> Result<Validation, KipError> {
        let mut result = Validation::default();
        for (name, value) in facets {
            let symbol = self.resolve_symbol(SymbolKind::Facet, name, intent)?;
            let def = self.facet_def(&symbol)?;
            let schema_ref = symbol.to_string();

            check_endpoint(
                &schema_ref,
                &format!("facets.{name}"),
                &def.applicable_to,
                &EndpointFacts::Element {
                    kind: carrier,
                    schema_ref: None,
                },
                &mut result,
            );

            match value.as_object() {
                Some(members) => result.extend(validate::validate_facet(&schema_ref, def, members)),
                None => result.push(Violation {
                    code: "SCHEMA_TYPE_MISMATCH".to_string(),
                    schema_ref,
                    path: format!("facets.{name}"),
                    message: "a Facet's value is an object of its declared members".to_string(),
                    severity: Severity::Error,
                }),
            }
        }
        Ok(result)
    }

    /// Resolves a predicate and validates a tuple's endpoints.
    ///
    /// A `functional` predicate produces no violation here even when the
    /// subject already has another object: that is a contested belief for the
    /// Epistemic Projection to report, and refusing the write would mean the
    /// Nexus could not record disagreement at all (§95, §240.28).
    pub fn prepare_proposition(
        &self,
        predicate_name: &str,
        subject: &EndpointFacts,
        object: &EndpointFacts,
        intent: Intent,
    ) -> Result<(SymbolRef, Validation), KipError> {
        let symbol = self.resolve_symbol(SymbolKind::PredicateType, predicate_name, intent)?;
        let def = self.predicate_def(&symbol)?;
        let schema_ref = symbol.to_string();

        let mut result = Validation::default();
        check_endpoint(&schema_ref, "subject", &def.subject, subject, &mut result);
        check_endpoint(&schema_ref, "object", &def.object, object, &mut result);
        Ok((symbol, result))
    }

    /// Resolves a structural field and validates its references.
    pub fn prepare_structural(
        &self,
        field_name: &str,
        source: &EndpointFacts,
        targets: &[(String, EndpointFacts)],
        intent: Intent,
    ) -> Result<(SymbolRef, Validation), KipError> {
        let symbol = self.resolve_symbol(SymbolKind::StructuralField, field_name, intent)?;
        let def = self.structural_field_def(&symbol)?;
        let schema_ref = symbol.to_string();

        let mut result = Validation::default();
        check_endpoint(&schema_ref, "source", &def.source, source, &mut result);
        for (_, facts) in targets {
            check_endpoint(&schema_ref, "target", &def.target, facts, &mut result);
        }
        let keys: Vec<String> = targets.iter().map(|(key, _)| key.clone()).collect();
        result.extend(validate::validate_structural(&schema_ref, def, &keys));
        Ok((symbol, result))
    }
}

fn missing(symbol: &SymbolRef, kind: SymbolKind) -> KipError {
    KipError::new(
        anda_kip::KipErrorCode::SchemaSymbolNotFound,
        format!("{symbol} resolved, but its package defines no {kind} by that name"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::env::{PackageState, SchemaLock};
    use crate::schema::package::SchemaPackage;
    use serde_json::json;
    use std::{collections::BTreeMap, sync::Arc};

    const COGNITIVE_MEMORY: &str = crate::profiles::COGNITIVE_MEMORY;
    const PROFILE: &str = "kip://profiles/cognitive-memory";

    fn env() -> SchemaEnvironment {
        let package = SchemaPackage::parse(COGNITIVE_MEMORY).unwrap();
        let available = BTreeMap::from([(
            package.package_ref().unwrap().to_string(),
            Arc::new(package),
        )]);
        let mut lock = SchemaLock::default();
        lock.packages.insert(PROFILE.into(), "2.0.0".into());
        lock.states.insert(PROFILE.into(), PackageState::Active);
        SchemaEnvironment::resolve(1, lock, &available).unwrap()
    }

    fn map(value: Json) -> Map<String, Json> {
        value.as_object().cloned().unwrap()
    }

    fn person() -> EndpointFacts {
        EndpointFacts::Element {
            kind: ElementKind::Concept,
            schema_ref: Some(format!("{PROFILE}@2.0.0/Person")),
        }
    }

    #[test]
    fn a_local_type_name_becomes_the_exact_reference_that_gets_persisted() {
        // Spec §13, §240.6: what the caller wrote is sugar; what is stored is
        // the exact version, so the element's meaning cannot drift later.
        let (symbol, result) = env()
            .prepare_concept(
                "Person",
                &map(json!({"display_name": "Alice"})),
                &map(json!({})),
                Intent::Write,
            )
            .unwrap();
        assert_eq!(symbol.to_string(), format!("{PROFILE}@2.0.0/Person"));
        assert!(result.is_valid());
    }

    #[test]
    fn a_functional_predicate_never_blocks_a_write() {
        // Spec §95, §240.28: two competing objects are a contested belief.
        // Refusing the write would leave the Nexus unable to record
        // disagreement, which is most of what it exists to do.
        let env = env();
        let dark = EndpointFacts::Element {
            kind: ElementKind::Concept,
            schema_ref: Some(format!("{PROFILE}@2.0.0/Preference")),
        };
        for _ in 0..2 {
            let (_, result) = env
                .prepare_proposition("prefers", &person(), &dark, Intent::Write)
                .unwrap();
            assert!(result.is_valid());
        }
    }

    #[test]
    fn a_declared_endpoint_type_is_enforced() {
        let env = env();
        // `prefers` declares a Person subject and a Concept object.
        let evidence = EndpointFacts::Element {
            kind: ElementKind::Evidence,
            schema_ref: None,
        };
        let (_, result) = env
            .prepare_proposition("prefers", &person(), &evidence, Intent::Write)
            .unwrap();
        assert!(!result.is_valid());
        assert_eq!(result.violations[0].path, "object");
        assert_eq!(result.violations[0].code, "SCHEMA_ENDPOINT_NOT_ALLOWED");

        let wrong_subject = EndpointFacts::Element {
            kind: ElementKind::Concept,
            schema_ref: Some(format!("{PROFILE}@2.0.0/Event")),
        };
        let (_, result) = env
            .prepare_proposition(
                "prefers",
                &wrong_subject,
                &EndpointFacts::Element {
                    kind: ElementKind::Concept,
                    schema_ref: None,
                },
                Intent::Write,
            )
            .unwrap();
        assert_eq!(result.violations[0].path, "subject");

        // A Literal where an element belongs.
        let (_, result) = env
            .prepare_proposition(
                "prefers",
                &person(),
                &EndpointFacts::Literal {
                    datatype: "kip:string".into(),
                },
                Intent::Write,
            )
            .unwrap();
        assert!(result.violations[0].message.contains("not a Literal"));
    }

    #[test]
    fn an_unresolvable_endpoint_is_unknown_rather_than_wrong() {
        // A canonical or foreign reference is deliberately outside same-Space
        // closure, so its type is simply not knowable here. Inventing a
        // violation from an unknown would reject legitimate data.
        let (_, result) = env()
            .prepare_proposition(
                "prefers",
                &EndpointFacts::Unresolved,
                &EndpointFacts::Unresolved,
                Intent::Write,
            )
            .unwrap();
        assert!(result.is_valid());
    }

    #[test]
    fn an_unknown_facet_symbol_is_refused_rather_than_carried() {
        // Spec §240.31: letting an unresolvable Facet through would restore
        // the untyped metadata bag this version removed.
        let err = env()
            .validate_facets(
                &map(json!({"WhateverIWant": {"secret": 1}})),
                ElementKind::Concept,
                Intent::Write,
            )
            .unwrap_err();
        assert_eq!(err.name(), "SchemaSymbolNotFound");
    }

    #[test]
    fn a_facet_is_checked_against_its_declared_members_and_carrier() {
        let env = env();
        let ok = env
            .validate_facets(
                &map(json!({"MnemonicState": {"memory_strength": 0.4}})),
                ElementKind::Concept,
                Intent::Write,
            )
            .unwrap();
        assert!(ok.is_valid());

        // Closed Facet, undeclared member.
        let smuggled = env
            .validate_facets(
                &map(json!({"MnemonicState": {"confidence": 0.9}})),
                ElementKind::Concept,
                Intent::Write,
            )
            .unwrap();
        assert_eq!(smuggled.violations[0].code, "SCHEMA_UNKNOWN_FIELD");

        // Declared `applicable_to` Concept, carried by an Assertion.
        let wrong_carrier = env
            .validate_facets(
                &map(json!({"MnemonicState": {"memory_strength": 0.4}})),
                ElementKind::Assertion,
                Intent::Write,
            )
            .unwrap();
        assert_eq!(
            wrong_carrier.violations[0].code,
            "SCHEMA_ENDPOINT_NOT_ALLOWED"
        );
    }

    #[test]
    fn structural_targets_are_checked_for_type_and_for_duplication() {
        let env = env();
        let experience = EndpointFacts::Element {
            kind: ElementKind::Concept,
            schema_ref: Some(format!("{PROFILE}@2.0.0/Experience")),
        };
        let step = |id: &str| {
            (
                id.to_string(),
                EndpointFacts::Element {
                    kind: ElementKind::Concept,
                    schema_ref: Some(format!("{PROFILE}@2.0.0/ExperienceStep")),
                },
            )
        };

        let (symbol, ok) = env
            .prepare_structural(
                "has_step",
                &experience,
                &[step("C-1"), step("C-2")],
                Intent::Write,
            )
            .unwrap();
        assert_eq!(symbol.to_string(), format!("{PROFILE}@2.0.0/has_step"));
        assert!(ok.is_valid());

        let (_, duplicated) = env
            .prepare_structural(
                "has_step",
                &experience,
                &[step("C-1"), step("C-1")],
                Intent::Write,
            )
            .unwrap();
        assert_eq!(duplicated.violations[0].code, "SCHEMA_DUPLICATE_REFERENCE");

        let (_, wrong_target) = env
            .prepare_structural(
                "has_step",
                &experience,
                &[(
                    "C-9".to_string(),
                    EndpointFacts::Element {
                        kind: ElementKind::Concept,
                        schema_ref: Some(format!("{PROFILE}@2.0.0/Person")),
                    },
                )],
                Intent::Write,
            )
            .unwrap();
        assert_eq!(wrong_target.violations[0].path, "target");
    }
}
