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
use std::collections::{BTreeMap, BTreeSet};

use super::stage::LegacyRow;
use crate::schema::{Intent, SchemaEnvironment, SymbolKind};

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
///
/// Split in two after [`Vocabulary::resolve`]: names the Space can already
/// resolve are *adopted* — a migrated element is written against the host's own
/// symbol — and only what is left is declared here.
///
/// That split is the whole point. A 1.x Brain used `Person`, `Event` and
/// `Preference`, and so does the Cognitive Memory Profile a 2.0 host activates.
/// Minting a legacy `Person` beside the profile's would leave two symbols
/// spelled the same, and every command naming the bare local name would resolve
/// to neither — `SchemaSymbolAmbiguous`, on a Space whose data migrated
/// perfectly.
#[derive(Debug, Default)]
pub struct Vocabulary {
    /// Every distinct 1.x Concept `type` this package declares.
    pub concept_types: BTreeSet<String>,
    /// Every distinct 1.x predicate this package declares.
    pub predicates: BTreeSet<String>,
    /// Legacy name → the exact symbol an already-active package provides.
    adopted: BTreeMap<(SymbolKind, String), String>,
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

    /// Hands every name the Space can already resolve to the package that
    /// provides it, and keeps the rest for this one.
    ///
    /// Adoption is by local name, which is the same identity 1.x had: a 1.x
    /// `Person` and the profile's `Person` are the same word for the same
    /// thing, and the guide's §8 says exactly this — migrate toward the
    /// standard profile where the semantics match.
    ///
    /// Where they do not match, nothing is adopted: a name no active package
    /// declares stays here, unconstrained, because a 1.x deployment's
    /// `ShipmentLeg` means whatever that deployment meant by it.
    pub fn resolve(&mut self, env: &SchemaEnvironment) {
        for (kind, names) in [
            (SymbolKind::ConceptType, &mut self.concept_types),
            (SymbolKind::PredicateType, &mut self.predicates),
        ] {
            let mut keep = BTreeSet::new();
            for name in std::mem::take(names) {
                // `Intent::Write`: a migrated element is written against this
                // symbol, so a package that may be read but not written to is
                // not one to adopt.
                match env.resolve_symbol(kind, &name, Intent::Write) {
                    Ok(symbol) => {
                        self.adopted.insert((kind, name), symbol.to_string());
                    }
                    Err(_) => {
                        keep.insert(name);
                    }
                }
            }
            *names = keep;
        }
    }

    /// Whether there is anything to publish.
    pub fn is_empty(&self) -> bool {
        self.concept_types.is_empty() && self.predicates.is_empty()
    }

    /// The exact symbol a legacy Concept type resolves to.
    pub fn concept_ref(&self, name: &str) -> Option<String> {
        self.symbol_ref(SymbolKind::ConceptType, &self.concept_types, name)
    }

    /// The exact symbol a legacy predicate resolves to.
    pub fn predicate_ref(&self, name: &str) -> Option<String> {
        self.symbol_ref(SymbolKind::PredicateType, &self.predicates, name)
    }

    fn symbol_ref(
        &self,
        kind: SymbolKind,
        declared: &BTreeSet<String>,
        name: &str,
    ) -> Option<String> {
        if let Some(adopted) = self.adopted.get(&(kind, name.to_string())) {
            return Some(adopted.clone());
        }
        declared.contains(name).then(|| symbol(name))
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
