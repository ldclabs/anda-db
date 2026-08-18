//! Phase 3a: a Schema Package for the vocabulary 1.x invented at runtime.
//!
//! In 1.x a type was a string on a row, so a deployment's vocabulary is
//! whatever its writers happened to use. In 2.0 every `schema_ref` names an
//! exact symbol in an immutable versioned Package (§20.4), which leaves a
//! migration two options: discard the legacy types, or publish a Package that
//! contains them.
//!
//! This publishes one, per the guide's §8:
//!
//! ```text
//! kip://legacy/<deployment>@1.0.0
//! ```
//!
//! The vocabulary is read off the data rather than declared, because the 1.x
//! `$ConceptType` graph nodes were advisory — a 1.x write could use a type
//! nobody had declared, and refusing those rows at migration time would lose
//! exactly the records that most need explaining.
//!
//! Two things it does not do. It does not put legacy types into
//! `kip://core` — Core is the protocol's, not a deployment's. And it does not
//! constrain: every generated type is open-attribute and every predicate is
//! non-functional and open-world, because a constraint invented here would be
//! one the 1.x data was never checked against, and the first thing it would do
//! is reject the deployment's own history.

use anda_kip::{Json, KipError, Map};
use serde_json::json;
use std::collections::BTreeSet;

use super::stage::LegacyRow;

/// The package id every migrated symbol resolves through.
pub const LEGACY_PACKAGE_ID: &str = "kip://legacy/nexus";
/// Its version. One migration, one version.
pub const LEGACY_PACKAGE_VERSION: &str = "1.0.0";

/// The exact reference migrated elements are written against.
pub fn legacy_package_ref() -> String {
    format!("{LEGACY_PACKAGE_ID}@{LEGACY_PACKAGE_VERSION}")
}

fn symbol(name: &str) -> String {
    format!("{}/{}", legacy_package_ref(), name)
}

/// The vocabulary a set of staged rows actually uses.
#[derive(Debug, Default)]
pub struct Vocabulary {
    /// Every distinct 1.x Concept `type`.
    pub concept_types: BTreeSet<String>,
    /// Every distinct 1.x predicate.
    pub predicates: BTreeSet<String>,
}

impl Vocabulary {
    /// Reads the vocabulary off the staged rows.
    pub fn scan(concepts: &[LegacyRow], propositions: &[LegacyRow]) -> Self {
        let mut vocabulary = Vocabulary::default();
        for row in concepts {
            if let Some(name) = row.doc.get("type").and_then(Json::as_str)
                && !name.is_empty()
            {
                vocabulary.concept_types.insert(name.to_string());
            }
        }
        for row in propositions {
            // 1.x stored a *set* of predicates per row, each with its own
            // properties: one row is many 2.0 tuples, so every member counts.
            if let Some(list) = row.doc.get("predicates").and_then(Json::as_array) {
                for predicate in list.iter().filter_map(Json::as_str) {
                    if !predicate.is_empty() {
                        vocabulary.predicates.insert(predicate.to_string());
                    }
                }
            }
        }
        vocabulary
    }

    /// Whether there is anything to publish.
    pub fn is_empty(&self) -> bool {
        self.concept_types.is_empty() && self.predicates.is_empty()
    }

    /// The exact symbol a legacy Concept type resolves to.
    pub fn concept_ref(&self, name: &str) -> Option<String> {
        self.concept_types.contains(name).then(|| symbol(name))
    }

    /// The exact symbol a legacy predicate resolves to.
    pub fn predicate_ref(&self, name: &str) -> Option<String> {
        self.predicates.contains(name).then(|| symbol(name))
    }

    /// Renders the Package artifact.
    pub fn artifact(&self) -> Result<Json, KipError> {
        let mut concept_types = Map::new();
        for name in &self.concept_types {
            concept_types.insert(
                name.clone(),
                json!({
                    "ref": symbol(name),
                    "kind": "ConceptType",
                    "description": format!(
                        "Migrated KIP 1.x Concept type {name:?}. Generated from the data, \
                         not from a declaration: 1.x types were strings on rows."
                    ),
                    // Open, and with no declared field: 1.x attributes were
                    // free-form, and a field list derived from today's rows
                    // would silently forbid tomorrow's.
                    "attributes": {"open": true, "fields": {}},
                }),
            );
        }

        let mut predicates = Map::new();
        for name in &self.predicates {
            predicates.insert(
                name.clone(),
                json!({
                    "ref": symbol(name),
                    "kind": "PredicateType",
                    "description": format!(
                        "Migrated KIP 1.x predicate {name:?}. Generated from the data."
                    ),
                    // Unconstrained ends: 1.x never checked them, so a domain
                    // or range asserted here would be a claim about the old
                    // system that the old system never enforced.
                    "subject": {"kinds": ["Concept", "Proposition"]},
                    "object": {"kinds": ["Concept", "Proposition", "Literal"]},
                    // Never functional. Marking a legacy predicate functional
                    // would turn two rows that coexisted for years into a
                    // conflict the projection has to resolve.
                    "functional": false,
                    "open_world": true,
                    "complete": false,
                }),
            );
        }

        Ok(json!({
            "format": "KIP-Schema-Package",
            "format_version": "2.0",
            "manifest": {
                "package_id": LEGACY_PACKAGE_ID,
                "version": LEGACY_PACKAGE_VERSION,
                "package_ref": legacy_package_ref(),
                "name": "KIP 1.x legacy vocabulary",
                "description":
                    "Concept types and predicates recovered from a KIP 1.x deployment during \
                     migration. Compatibility surface, not a designed ontology: migrate toward \
                     kip://profiles/cognitive-memory where the semantics actually match.",
                "publisher": "urn:kip:publisher:migration",
                "purpose": "compatibility",
            },
            "dependencies": [],
            "definitions": {
                "concept_types": concept_types,
                "predicates": predicates,
                "facets": {},
                "structural_fields": {},
                "enums": {},
                "registry_extensions": {},
            },
            "constraints": [],
            "aliases": {},
            "compatibility": {},
            "migrations": [],
            "model_hints": {},
        }))
    }
}
