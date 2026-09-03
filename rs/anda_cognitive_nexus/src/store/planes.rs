//! # Version planes
//!
//! `_system.version` advances on every committed change to an element;
//! `_system.plane_versions` holds one counter per **version plane** — the
//! `attributes` plane (Core fields and attributes), the `structural` plane
//! (Structural References), the `retention` plane (the retention record) and
//! one counter per Facet symbol — and each counter advances only when its
//! plane changes (Spec §6.3, §35.1). That is what lets a `MnemonicState` decay
//! sweep guarded `OF FACET "MnemonicState"` and a status verdict guarded
//! `OF ATTRIBUTES` run against the same element without spoiling each other's
//! guard.
//!
//! The counters are derived here, at commit, from a diff of the row the
//! transaction loaded against the row it is about to write. Deriving them
//! from the diff rather than from the clause that ran is what keeps the rule
//! honest: a clause that *wrote the same value back* touched no plane, and a
//! `TRANSITION` that finalized an Activity's outputs touched the structural
//! plane whatever its name says.
//!
//! ## What each plane holds
//!
//! ```text
//! attributes   Concept: key, name, canonical_id, aliases, attributes
//!              Evidence: evidence_class, payload, content_digest, media_type, observed_at
//!              Activity: activity_class, started_at, ended_at, parameters_digest
//!              Assertion: the immutable payload (touched only by a purge stub)
//! structural   the Profile structural map, plus the Core structural fields of
//!              each record kind (§8.2): Assertion.evidence / .context,
//!              Evidence.source / .generated_by, Activity.inputs / .outputs /
//!              .associated_actors
//! retention    the retention hook and its lifted expires_at
//! facets.<X>   one Facet, by symbol
//! ```
//!
//! Lifecycle columns — `_system.state`, a record's `status`, the supersession
//! and correction links, `retracted_at`, `merged_into` — and the Governance
//! block belong to no plane: a lifecycle move or a Governance decision advances
//! `version` and nothing else, unless it also finalized content (§6.3). They
//! are still named in `touched` so a Watch can see what moved, beside the
//! `state {from, to}` a lifecycle entry reports the move in: the two answer
//! different questions, and a consumer watching a named slot reads `touched`
//! on every entry rather than only on the ones that carry no `state`.
//!
//! ## Counters at creation
//!
//! A new element starts every plane it carries content in at `1` and every
//! other plane at `0`, so `EXPECT VERSION 0 OF <plane>` reads as "this plane
//! has never been written" (§35.2) from the first version on. The attributes
//! plane counts as written when a Concept carries any of its Core fields or
//! attributes, and always for a record kind, which cannot exist without its
//! Core fields; a Proposition has no attributes plane content, its tuple being
//! identity rather than a field. The same rule gives a migrated 1.x element
//! its counters: `attributes: 1` when it has attributes, `structural: 1` when it
//! has references, `facets: {X: 1}` for each Facet present, `retention: 1` when
//! it carries a retention record.

use anda_kip::{Json, Map, PlaneVersions};
use std::collections::BTreeSet;

use super::Element;
use super::rows::*;

/// Reads a row's `plane_versions` column; `null` is every counter at zero.
pub fn decode(value: &Json) -> PlaneVersions {
    if value.is_null() {
        return PlaneVersions::default();
    }
    serde_json::from_value(value.clone()).unwrap_or_default()
}

/// The stored form of the counters.
pub fn encode(planes: &PlaneVersions) -> Json {
    serde_json::to_value(planes).unwrap_or(Json::Null)
}

/// The counters as the wire shape spells them.
///
/// Facet counters are stored keyed by the symbol's local name, which is
/// already what §36.1's example writes and what
/// `?x._system.plane_versions.facets["MnemonicState"]` reads, so this is a
/// copy rather than a translation. It stays a named step because the wire
/// spelling is a contract and the storage keying is not.
pub fn to_wire(planes: &PlaneVersions) -> PlaneVersions {
    planes.clone()
}

/// The local name of a stored symbol — the part after the version's `/`.
///
/// A Core structural field is stored under its plain name and comes back
/// unchanged.
pub fn local_name(symbol: &str) -> String {
    match symbol.parse::<crate::schema::SymbolRef>() {
        Ok(symbol) => symbol.name,
        Err(_) => symbol.rsplit('/').next().unwrap_or(symbol).to_string(),
    }
}

/// One version plane a guard may name, resolved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlaneKey {
    /// The whole element: `_system.version`.
    Element,
    /// Fields and attributes.
    Attributes,
    /// Structural References.
    Structural,
    /// The retention record.
    Retention,
    /// One Facet, by the local symbol name its counter is keyed under.
    ///
    /// The lineage, not the exact reference (§20.14): a Facet written under
    /// `p@1.0.0/MnemonicState` and one written under `p@2.0.0/MnemonicState`
    /// are one Facet with one counter, so a guard survives a package upgrade
    /// instead of reading a plane that has never been written.
    Facet {
        /// The Facet's local symbol name.
        local: String,
    },
}

impl PlaneKey {
    /// The plane name a `VersionConflict` reports in `details.plane` (§35.1).
    pub fn name(&self) -> String {
        match self {
            PlaneKey::Element => "version".to_string(),
            PlaneKey::Attributes => "attributes".to_string(),
            PlaneKey::Structural => "structural".to_string(),
            PlaneKey::Retention => "retention".to_string(),
            PlaneKey::Facet { local, .. } => format!("facets.{local}"),
        }
    }

    /// This plane's counter.
    pub fn counter(&self, version: u64, planes: &PlaneVersions) -> u64 {
        match self {
            PlaneKey::Element => version,
            PlaneKey::Attributes => planes.attributes,
            PlaneKey::Structural => planes.structural,
            PlaneKey::Retention => planes.retention,
            PlaneKey::Facet { local } => planes.facets.get(local).copied().unwrap_or(0),
        }
    }
}

/// What one commit changed on one element, by name.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Touched {
    /// The changed paths, names only, in the spelling §36.1 fixes.
    pub paths: Vec<String>,
    /// Whether the attributes plane moved.
    pub attributes: bool,
    /// Whether the structural plane moved.
    pub structural: bool,
    /// Whether the retention plane moved.
    pub retention: bool,
    /// The Facets that moved, by local symbol name (§20.14).
    pub facets: Vec<String>,
}

impl Touched {
    /// Whether any plane at all moved.
    pub fn any_plane(&self) -> bool {
        self.attributes || self.structural || self.retention || !self.facets.is_empty()
    }

    /// The counters after a commit that made these changes.
    pub fn advance(&self, mut planes: PlaneVersions) -> PlaneVersions {
        if self.attributes {
            planes.attributes += 1;
        }
        if self.structural {
            planes.structural += 1;
        }
        if self.retention {
            planes.retention += 1;
        }
        for facet in &self.facets {
            *planes.facets.entry(facet.clone()).or_insert(0) += 1;
        }
        planes
    }

    fn field(&mut self, name: &str) {
        self.attributes = true;
        self.paths.push(format!("fields.{name}"));
    }

    fn lifecycle(&mut self, name: &str) {
        self.paths.push(format!("fields.{name}"));
    }

    fn structural_field(&mut self, name: &str) {
        self.structural = true;
        self.paths.push(format!("structural.{}", local_name(name)));
    }
}

/// The counters a freshly created element starts with.
pub fn initial(element: &Element) -> PlaneVersions {
    let has_attributes = match element {
        Element::Concept(row) => {
            !row.key.is_empty()
                || !row.name.is_empty()
                || !row.canonical_id.is_empty()
                || !row.aliases.is_empty()
                || !row.attributes.is_empty()
        }
        Element::Proposition(_) => false,
        Element::Assertion(_) | Element::Evidence(_) | Element::Activity(_) => true,
    };
    let has_structural = !structural_of(element).is_empty();
    let has_retention = !element.retention().is_null();
    PlaneVersions {
        attributes: u64::from(has_attributes),
        structural: u64::from(has_structural),
        retention: u64::from(has_retention),
        facets: element
            .facets()
            .keys()
            .map(|symbol| (local_name(symbol), 1))
            .collect(),
    }
}

/// Every structural reference an element carries, keyed by field name.
///
/// Core fields under their plain names, Profile fields under their symbols.
fn structural_of(element: &Element) -> Map<String, Json> {
    let mut out = Map::new();
    {
        let mut core = |name: &str, refs: &[Json]| {
            if !refs.is_empty() {
                out.insert(name.to_string(), Json::Array(refs.to_vec()));
            }
        };
        // Only the typed Core edges differ between the kinds; the Profile
        // fields below are a shared column and are walked once.
        match element {
            Element::Concept(_) | Element::Proposition(_) => {}
            Element::Assertion(row) => {
                core("evidence", &row.evidence_refs);
                core("context", &row.context_refs);
            }
            Element::Evidence(row) => {
                core("source", &row.source_refs);
                if !row.generated_by.is_empty() {
                    core("generated_by", &[Json::String(row.generated_by.clone())]);
                }
            }
            Element::Activity(row) => {
                core("inputs", &row.inputs);
                core("outputs", &row.outputs);
                core("associated_actors", &row.associated_actors);
            }
        }
    }
    for (field, refs) in element.structural() {
        // An emptied field is the same as an absent one: `UNSET STRUCTURAL`
        // removes the key, and a `[]` left behind must not count as content.
        if refs.as_array().is_some_and(|items| items.is_empty()) {
            continue;
        }
        out.insert(field.clone(), refs.clone());
    }
    out
}

/// What changed between the row a transaction loaded and the row it writes.
///
/// The two are the same element, so they are the same kind; a kind mismatch
/// is reported as everything having changed rather than as nothing, because
/// the conservative reading of a corrupt diff is that every guard should
/// fail.
pub fn diff(before: &Element, after: &Element) -> Touched {
    let mut touched = Touched::default();

    match (before, after) {
        (Element::Concept(a), Element::Concept(b)) => {
            if a.key != b.key {
                touched.field("key");
            }
            if a.name != b.name {
                touched.field("name");
            }
            if a.canonical_id != b.canonical_id {
                touched.field("canonical_id");
            }
            if a.aliases != b.aliases {
                touched.field("aliases");
            }
            for name in keys_that_differ(&a.attributes, &b.attributes) {
                touched.attributes = true;
                touched.paths.push(format!("attributes.{name}"));
            }
            if a.merged_into != b.merged_into {
                touched.lifecycle("merged_into");
            }
        }
        (Element::Proposition(_), Element::Proposition(_)) => {}
        (Element::Assertion(a), Element::Assertion(b)) => {
            for (name, changed) in [
                ("proposition", a.proposition_id != b.proposition_id),
                ("asserted_by", a.asserted_by != b.asserted_by),
                ("stance", a.stance != b.stance),
                ("mode", a.mode != b.mode),
                ("confidence", a.confidence != b.confidence),
                ("asserted_at", a.asserted_at != b.asserted_at),
                (
                    "valid_time",
                    a.valid_from != b.valid_from || a.valid_until != b.valid_until,
                ),
            ] {
                if changed {
                    touched.field(name);
                }
            }
            for (name, changed) in [
                ("status", a.status != b.status),
                ("retracted_at", a.retracted_at != b.retracted_at),
                ("supersedes", a.supersedes != b.supersedes),
                ("superseded_by", a.superseded_by != b.superseded_by),
            ] {
                if changed {
                    touched.lifecycle(name);
                }
            }
        }
        (Element::Evidence(a), Element::Evidence(b)) => {
            for (name, changed) in [
                ("evidence_class", a.evidence_class != b.evidence_class),
                (
                    "payload",
                    a.payload_mode != b.payload_mode
                        || a.payload_inline != b.payload_inline
                        || a.content_ref != b.content_ref,
                ),
                ("content_digest", a.content_digest != b.content_digest),
                ("media_type", a.media_type != b.media_type),
                ("observed_at", a.observed_at != b.observed_at),
            ] {
                if changed {
                    touched.field(name);
                }
            }
            for (name, changed) in [
                ("status", a.status != b.status),
                ("corrects", a.corrects != b.corrects),
                ("corrected_by", a.corrected_by != b.corrected_by),
            ] {
                if changed {
                    touched.lifecycle(name);
                }
            }
        }
        (Element::Activity(a), Element::Activity(b)) => {
            for (name, changed) in [
                ("activity_class", a.activity_class != b.activity_class),
                ("started_at", a.started_at != b.started_at),
                ("ended_at", a.ended_at != b.ended_at),
                (
                    "parameters_digest",
                    a.parameters_digest != b.parameters_digest,
                ),
            ] {
                if changed {
                    touched.field(name);
                }
            }
            if a.status != b.status {
                touched.lifecycle("status");
            }
        }
        _ => {
            touched.attributes = true;
            touched.structural = true;
            touched.retention = true;
            touched.paths.push("kind".to_string());
        }
    }

    // The structural plane, both Core and Profile fields.
    let before_structural = structural_of(before);
    let after_structural = structural_of(after);
    for name in keys_that_differ(&before_structural, &after_structural) {
        touched.structural_field(&name);
    }

    // Facets, one counter each, keyed by the local name so two versions of one
    // lineage share it (§20.14).
    for symbol in keys_that_differ(before.facets(), after.facets()) {
        let local = local_name(&symbol);
        touched.paths.push(format!("facets.{local}"));
        touched.facets.push(local);
    }

    // Retention: the hook and the column lifted out of it.
    let (before_retention, before_expires) = (before.retention(), before.expires_at());
    let (after_retention, after_expires) = (after.retention(), after.expires_at());
    if before_retention != after_retention || before_expires != after_expires {
        touched.retention = true;
        touched.paths.push("retention".to_string());
    }

    // Governance and state: named, never a plane.
    if before.state() != after.state() {
        touched.paths.push("state".to_string());
    }
    let empty = Map::new();
    let before_governance = before.governance().as_object().unwrap_or(&empty);
    let after_governance = after.governance().as_object().unwrap_or(&empty);
    for member in keys_that_differ(before_governance, after_governance) {
        touched.paths.push(format!("governance.{member}"));
    }

    touched.paths.sort();
    touched.paths.dedup();
    touched.facets.sort();
    touched.facets.dedup();
    touched
}

/// The keys whose values differ between two maps, added or removed included.
fn keys_that_differ(a: &Map<String, Json>, b: &Map<String, Json>) -> Vec<String> {
    let keys: BTreeSet<&String> = a.keys().chain(b.keys()).collect();
    keys.into_iter()
        .filter(|key| a.get(*key) != b.get(*key))
        .cloned()
        .collect()
}

/// The lifecycle state a Change Envelope entry reports (§36.1, §52.5).
///
/// One word per element, combining the two columns a lifecycle lives in: the
/// engine state when the element has left ordinary recall, else the record's
/// own status — an Assertion's `retracted`, an Activity's `running` — else
/// `active`. `TRANSITION` validates its moves against this same word, so what
/// the envelope says moved is what the executor checked.
pub fn lifecycle_state(element: &Element) -> String {
    let system = element.state();
    if system != state::ACTIVE && !system.is_empty() {
        return system.to_string();
    }
    let status = match element {
        Element::Assertion(row) => row.status.as_str(),
        Element::Evidence(row) => row.status.as_str(),
        Element::Activity(row) => row.status.as_str(),
        Element::Concept(_) | Element::Proposition(_) => "",
    };
    if status.is_empty() {
        state::ACTIVE.to_string()
    } else {
        status.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anda_kip::Json;

    fn concept(attributes: Json, facets: Json, structural: Json) -> Element {
        Element::Concept(Box::new(ConceptRow {
            _id: 1,
            name: "Alice".into(),
            attributes: attributes.as_object().cloned().unwrap_or_default(),
            facets: facets.as_object().cloned().unwrap_or_default(),
            structural: structural.as_object().cloned().unwrap_or_default(),
            ..Default::default()
        }))
    }

    #[test]
    fn a_facet_write_moves_only_its_own_counter() {
        let before = concept(
            serde_json::json!({"a": 1}),
            serde_json::json!({"kip://p@1.0.0/MnemonicState": {"salience": 0.5}}),
            Json::Null,
        );
        let after = concept(
            serde_json::json!({"a": 1}),
            serde_json::json!({"kip://p@1.0.0/MnemonicState": {"salience": 0.7}}),
            Json::Null,
        );
        let touched = diff(&before, &after);
        assert!(!touched.attributes);
        assert!(!touched.structural);
        assert_eq!(touched.facets, vec!["MnemonicState".to_string()]);
        assert_eq!(touched.paths, vec!["facets.MnemonicState".to_string()]);

        let planes = touched.advance(initial(&before));
        assert_eq!(planes.attributes, 1);
        assert_eq!(planes.structural, 0);
        assert_eq!(planes.facets["MnemonicState"], 2);
    }

    #[test]
    fn writing_the_same_value_back_touches_nothing() {
        let element = concept(serde_json::json!({"a": 1}), Json::Null, Json::Null);
        assert_eq!(diff(&element, &element), Touched::default());
    }

    #[test]
    fn an_attribute_and_a_structural_change_are_two_planes() {
        let before = concept(serde_json::json!({}), Json::Null, Json::Null);
        let after = concept(
            serde_json::json!({"note": "x"}),
            Json::Null,
            serde_json::json!({"kip://p@1.0.0/has_step": [{"id": "C-2"}]}),
        );
        let touched = diff(&before, &after);
        assert!(touched.attributes && touched.structural);
        assert_eq!(
            touched.paths,
            vec![
                "attributes.note".to_string(),
                "structural.has_step".to_string()
            ]
        );
    }

    #[test]
    fn a_lifecycle_move_is_named_but_moves_no_plane() {
        let mut before = AssertionRow {
            _id: 1,
            status: "active".into(),
            ..Default::default()
        };
        let after = AssertionRow {
            status: "retracted".into(),
            retracted_at: "2026-01-01T00:00:00.000Z".into(),
            ..before.clone()
        };
        before.status = "active".into();
        let touched = diff(
            &Element::Assertion(Box::new(before.clone())),
            &Element::Assertion(Box::new(after.clone())),
        );
        assert!(!touched.any_plane());
        // The move advances `_system.version` and no plane counter, and the
        // columns that encode it are still named, so a Watch reading `touched`
        // sees what moved without having to special-case a lifecycle entry.
        assert_eq!(
            touched.paths,
            vec![
                "fields.retracted_at".to_string(),
                "fields.status".to_string()
            ]
        );
        assert_eq!(
            lifecycle_state(&Element::Assertion(Box::new(before))),
            "active"
        );
        assert_eq!(
            lifecycle_state(&Element::Assertion(Box::new(after))),
            "retracted"
        );
    }

    #[test]
    fn a_new_element_starts_at_one_where_it_has_content() {
        let planes = initial(&concept(
            Json::Null,
            serde_json::json!({"kip://p@1.0.0/MnemonicState": {"salience": 0.5}}),
            Json::Null,
        ));
        // The name is a Core field, so the attributes plane has been written.
        assert_eq!(planes.attributes, 1);
        assert_eq!(planes.structural, 0);
        assert_eq!(planes.retention, 0);
        assert_eq!(planes.facets["MnemonicState"], 1);
        assert_eq!(
            to_wire(&planes).facets["MnemonicState"],
            1,
            "the wire spelling keys a Facet counter by its local name"
        );
    }
}
