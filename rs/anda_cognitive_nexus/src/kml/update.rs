//! # `UPDATE` — mutable state only
//!
//! The parser already refuses the rewrites it can see: `UPDATE ?a SET FIELDS
//! {confidence: …}` never reaches an engine when the block types `?a` as an
//! Assertion (§76). But it can only see what the command spells out, and
//! `UPDATE :A-7 SET FIELDS {confidence: 0.1}` spells out nothing — the target
//! is an id, and only the engine knows what kind of element wears it. So the
//! same rules are enforced again here, against the element actually loaded.
//!
//! ## What is mutable, and why that list is short
//!
//! ```text
//! Concept      name · canonical_id · aliases · attributes · facets · structural
//! Proposition  attributes · facets            (the tuple itself is immutable, §12.5)
//! Assertion    facets                         (epistemic payload is history, §15.1)
//! Evidence     facets                         (an observation is corrected, never edited, §70)
//! Activity     facets                         (topology is finalized by TRANSITION, §93)
//! ```
//!
//! Facets are on every row because a Facet is representation-local state and
//! none of it is truth (§35): decaying an Assertion's `memory_strength` says
//! nothing about whether the Assertion is believed, which is exactly why
//! metabolism is allowed to touch it and confidence is not.
//!
//! Each refusal answers with the code that names the *ritual* the caller
//! should have used — `EpistemicRevisionRequired`, `EvidenceCorrectionRequired`,
//! `InvalidLifecycleTransition` — rather than a flat "not allowed", because the
//! agent reading it has to know what to do instead.

use anda_kip::{
    ElementKind, FacetAssignment, FacetUnset, Json, KipError, KipErrorCode, Map, StructuralEdge,
    StructuralRemoval, UpdateAction,
};

use super::clauses::{Applied, apply_facets, bindings, resolve_structural_field};
use super::value::{assignments_to_json, structural_value};
use crate::id::ElementId;
use crate::store::Element;
use crate::tx::Transaction;

/// Applies one `UPDATE` action to one already-loaded element.
///
/// `view` is the element's rendered form, read by update expressions: `MUL(?m
/// .facets["MnemonicState"].memory_strength, 0.9)` needs the current value, and
/// it may read nothing else (§52.4).
pub async fn apply_action(
    tx: &mut Transaction,
    id: ElementId,
    action: &UpdateAction,
    view: &Json,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<Applied, KipError> {
    match action {
        UpdateAction::SetFields(assignments) => {
            let b = bindings(tx, request, operation);
            let fields = assignments_to_json(&b, assignments, Some(view))?;
            set_fields(tx, id, fields).await
        }
        UpdateAction::SetAttributes(assignments) => {
            let b = bindings(tx, request, operation);
            let values = assignments_to_json(&b, assignments, Some(view))?;
            let attributes = attributes_mut(tx, id, "SET ATTRIBUTES").await?;
            let mut changed = Applied::default();
            for (key, value) in values {
                if attributes.get(&key) != Some(&value) {
                    attributes.insert(key, value);
                    changed.changed = true;
                }
            }
            Ok(changed)
        }
        UpdateAction::UnsetAttributes(names) => {
            let attributes = attributes_mut(tx, id, "UNSET ATTRIBUTES").await?;
            let mut changed = Applied::default();
            for name in names {
                if attributes.remove(name).is_some() {
                    changed.changed = true;
                }
            }
            Ok(changed)
        }
        UpdateAction::SetFacet(assignment) => {
            set_facet(tx, id, assignment, view, request, operation).await
        }
        UpdateAction::UnsetFacet(unset) => unset_facet(tx, id, unset, request, operation).await,
        UpdateAction::SetStructural(edges) => {
            set_structural(tx, id, edges, view, request, operation).await
        }
        UpdateAction::UnsetStructural(removals) => {
            unset_structural(tx, id, removals, view, request, operation).await
        }
    }
}

/// The Core fields `UPDATE` may write, by element kind.
///
/// Only a Concept has any: everything else's Core state is either historical
/// record or lifecycle owned by a dedicated clause.
async fn set_fields(
    tx: &mut Transaction,
    id: ElementId,
    fields: Map<String, Json>,
) -> Result<Applied, KipError> {
    let element = tx.load(id).await?;
    let Element::Concept(row) = element else {
        return Err(immutable_target(element.kind(), id, "SET FIELDS"));
    };

    let mut applied = Applied::default();
    for (field, value) in fields {
        match (field.as_str(), value) {
            ("name", Json::String(name)) => {
                if row.name != name {
                    row.name = name;
                    applied.changed = true;
                }
            }
            ("canonical_id", Json::String(canonical)) => {
                if row.canonical_id != canonical {
                    row.canonical_id = canonical;
                    applied.changed = true;
                }
            }
            ("aliases", Json::Array(items)) => {
                let aliases: Vec<String> = items
                    .iter()
                    .filter_map(|item| item.as_str().map(str::to_string))
                    .collect();
                if row.aliases != aliases {
                    row.aliases = aliases;
                    applied.changed = true;
                }
            }
            // `key` is the Space-local identity a Concept is resolved by
            // (§5.3); letting an update move it would silently re-point every
            // `UPSERT ... MATCH {key: …}` that ever named it.
            ("key", _) | ("client_key", _) | ("schema_ref", _) => {
                return Err(KipError::new(
                    KipErrorCode::ImmutableField,
                    format!("`{field}` is fixed at creation; it is what makes {id} that element"),
                ));
            }
            ("retention", _) => {
                return Err(KipError::new(
                    KipErrorCode::ImmutableField,
                    "retention is storage lifecycle, not content: use SET RETENTION",
                ));
            }
            (field, value) => {
                return Err(KipError::type_mismatch(format!(
                    "a Concept has no mutable Core field `{field}` accepting {value}; \
                     representation-local state belongs in SET ATTRIBUTES"
                )));
            }
        }
    }
    Ok(applied)
}

async fn set_facet(
    tx: &mut Transaction,
    id: ElementId,
    assignment: &FacetAssignment,
    view: &Json,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<Applied, KipError> {
    let kind = tx.load(id).await?.kind();
    let b = bindings(tx, request, operation);
    let facets = apply_facets(tx, &b, std::slice::from_ref(assignment), kind, Some(view)).await?;

    let target = facets_mut(tx, id).await?;
    let mut applied = Applied::default();
    for (facet, value) in facets {
        // A Facet assignment merges members rather than replacing the Facet:
        // `SET FACET "MnemonicState" {salience: 0.4}` must not silently drop a
        // `memory_strength` nobody mentioned.
        let entry = target
            .entry(facet)
            .or_insert_with(|| Json::Object(Map::new()));
        let Json::Object(members) = value else {
            continue;
        };
        let Json::Object(existing) = entry else {
            continue;
        };
        for (member, value) in members {
            if existing.get(&member) != Some(&value) {
                existing.insert(member, value);
                applied.changed = true;
            }
        }
    }
    Ok(applied)
}

async fn unset_facet(
    tx: &mut Transaction,
    id: ElementId,
    unset: &FacetUnset,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<Applied, KipError> {
    let b = bindings(tx, request, operation);
    let name = super::clauses::symbol_of(&b, &unset.facet)?;
    let symbol = tx.env.resolve_symbol(
        crate::schema::SymbolKind::Facet,
        &name,
        crate::schema::Intent::Write,
    )?;
    let key = symbol.to_string();

    let facets = facets_mut(tx, id).await?;
    let mut applied = Applied::default();
    let Some(Json::Object(members)) = facets.get_mut(&key) else {
        return Ok(applied);
    };
    for field in &unset.fields {
        if members.remove(field).is_some() {
            applied.changed = true;
        }
    }
    // An emptied Facet is removed rather than left as `{}`: a Facet present
    // with no members would read as "carried, and every member unknown".
    if members.is_empty() {
        facets.remove(&key);
        applied.changed = true;
    }
    Ok(applied)
}

async fn set_structural(
    tx: &mut Transaction,
    id: ElementId,
    edges: &[StructuralEdge],
    view: &Json,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<Applied, KipError> {
    let kind = tx.load(id).await?.kind();
    if kind != ElementKind::Concept {
        return Err(immutable_target(kind, id, "SET STRUCTURAL"));
    }
    let b = bindings(tx, request, operation);
    let mut resolved: Vec<(String, Json)> = Vec::with_capacity(edges.len());
    for edge in edges {
        let field = resolve_structural_field(tx, &b, &edge.field)?;
        resolved.push((field, structural_value(b.value(&edge.value, Some(view))?)));
    }

    let structural = structural_mut(tx, id).await?;
    let mut applied = Applied::default();
    for (field, value) in resolved {
        let entry = structural
            .entry(field)
            .or_insert_with(|| Json::Array(Vec::new()));
        if let Json::Array(items) = entry
            && !items.contains(&value)
        {
            items.push(value);
            applied.changed = true;
        }
    }
    Ok(applied)
}

async fn unset_structural(
    tx: &mut Transaction,
    id: ElementId,
    removals: &[StructuralRemoval],
    view: &Json,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<Applied, KipError> {
    let kind = tx.load(id).await?.kind();
    if kind != ElementKind::Concept {
        return Err(immutable_target(kind, id, "UNSET STRUCTURAL"));
    }
    let b = bindings(tx, request, operation);
    let mut resolved: Vec<(String, Json)> = Vec::with_capacity(removals.len());
    for removal in removals {
        let field = resolve_structural_field(tx, &b, &removal.field)?;
        resolved.push((
            field,
            structural_value(b.value(&removal.value, Some(view))?),
        ));
    }

    let structural = structural_mut(tx, id).await?;
    let mut applied = Applied::default();
    for (field, value) in resolved {
        let Some(Json::Array(items)) = structural.get_mut(&field) else {
            continue;
        };
        let before = items.len();
        // An ordered structural field re-densifies: removing the second of
        // three references leaves two, not a hole (§8.2).
        items.retain(|item| !same_reference(item, &value));
        if items.len() != before {
            applied.changed = true;
        }
        if items.is_empty() {
            structural.remove(&field);
        }
    }
    Ok(applied)
}

/// Whether two structural entries point at the same element.
///
/// A reference is persisted as `{"id": "C-1"}` but may be written as the bare
/// id, so equality is on the identity rather than on the JSON shape.
fn same_reference(stored: &Json, given: &Json) -> bool {
    fn id_of(value: &Json) -> Option<&str> {
        match value {
            Json::String(text) => Some(text.as_str()),
            Json::Object(map) => map.get("id").and_then(Json::as_str),
            _ => None,
        }
    }
    match (id_of(stored), id_of(given)) {
        (Some(a), Some(b)) => a == b,
        _ => stored == given,
    }
}

async fn attributes_mut<'a>(
    tx: &'a mut Transaction,
    id: ElementId,
    what: &str,
) -> Result<&'a mut Map<String, Json>, KipError> {
    match tx.load(id).await? {
        Element::Concept(row) => Ok(&mut row.attributes),
        Element::Proposition(row) => Ok(&mut row.attributes),
        other => Err(immutable_target(other.kind(), id, what)),
    }
}

async fn facets_mut(
    tx: &mut Transaction,
    id: ElementId,
) -> Result<&mut Map<String, Json>, KipError> {
    Ok(match tx.load(id).await? {
        Element::Concept(row) => &mut row.facets,
        Element::Proposition(row) => &mut row.facets,
        Element::Assertion(row) => &mut row.facets,
        Element::Evidence(row) => &mut row.facets,
        Element::Activity(row) => &mut row.facets,
    })
}

async fn structural_mut(
    tx: &mut Transaction,
    id: ElementId,
) -> Result<&mut Map<String, Json>, KipError> {
    match tx.load(id).await? {
        Element::Concept(row) => Ok(&mut row.structural),
        other => Err(immutable_target(other.kind(), id, "structural mutation")),
    }
}

/// The refusal for an element whose state this action may not reach.
///
/// The code names the ritual that *is* legal, so an agent reading it knows
/// what to do rather than that it may not do this.
fn immutable_target(kind: ElementKind, id: ElementId, what: &str) -> KipError {
    match kind {
        ElementKind::Assertion => KipError::new(
            KipErrorCode::EpistemicRevisionRequired,
            format!(
                "{what} would rewrite {id}'s epistemic payload; a changed commitment is a new \
                 Assertion with SUPERSEDING, so the record of what was believed survives"
            ),
        ),
        ElementKind::Evidence => KipError::new(
            KipErrorCode::EvidenceCorrectionRequired,
            format!(
                "{what} would rewrite what {id} observed; wrong Evidence is corrected with \
                 CORRECT EVIDENCE :old BY :new, never edited in place"
            ),
        ),
        ElementKind::Activity => KipError::new(
            KipErrorCode::InvalidLifecycleTransition,
            format!(
                "{what} does not reach an Activity: a pending one finalizes its fields and \
                 topology through TRANSITION ACTIVITY, and a terminal one is immutable"
            ),
        ),
        ElementKind::Proposition => KipError::new(
            KipErrorCode::ImmutableField,
            format!(
                "{what} does not reach a Proposition's tuple: a different tuple is a different \
                 Proposition (§12.5)"
            ),
        ),
        ElementKind::Concept => KipError::new(
            KipErrorCode::ImmutableField,
            format!("{what} does not reach {id}"),
        ),
    }
}
