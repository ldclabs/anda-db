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
//! Proposition  facets                         (the tuple is its whole content, §12.2)
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

use super::clauses::{Applied, bindings, resolve_facets, resolve_structural_field};
use super::value::{Bindings, assignments_to_json, structural_value};
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
    // The carrier's own type, when it has one: a Facet declaring
    // `concept_types` is state about those Concepts (§58).
    let carrier = crate::schema::EndpointFacts::Element {
        kind,
        schema_ref: view
            .get("schema_ref")
            .and_then(Json::as_str)
            .filter(|text| !text.is_empty())
            .map(str::to_string),
    };
    let b = bindings(tx, request, operation);
    let facets = resolve_facets(tx, &b, std::slice::from_ref(assignment), Some(view))?;
    let pinned = facet_contract(tx, &b, &assignment.facet)?;

    // A Facet assignment merges members rather than replacing the Facet:
    // `SET FACET "MnemonicState" {salience: 0.4}` must not silently drop a
    // `memory_strength` nobody mentioned. So the merged result is what the
    // schema is shown, and what §39 immutability is judged against — an
    // assignment read on its own would refuse every partial write to a Facet
    // with required members.
    let mut merged = Map::new();
    for (facet, value) in &facets {
        let Json::Object(members) = value else {
            continue;
        };
        let existing = current_members(view, facet);
        let mut after = existing.clone();
        after.extend(members.clone());
        pinned.check(&existing, &after)?;
        merged.insert(facet.clone(), Json::Object(after));
    }
    tx.env
        .validate_facets(&merged, &carrier, crate::schema::Intent::Write)?
        .into_result()?;

    let target = facets_mut(tx, id).await?;
    let mut applied = Applied::default();
    for (facet, value) in facets {
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

/// Whether any of these actions writes the attribute bag.
///
/// The gate on [`check_attributes`]: an `UPDATE` that only decays a Facet has
/// not been asked anything about the attributes, and refusing it for drift
/// that predates the statement would make an unrelated clause the place a
/// stale element finally fails.
pub fn touches_attributes(actions: &[UpdateAction]) -> bool {
    actions.iter().any(|action| {
        matches!(
            action,
            UpdateAction::SetAttributes(_) | UpdateAction::UnsetAttributes(_)
        )
    })
}

/// Whether any of these actions writes the structural map.
pub fn touches_structural(actions: &[UpdateAction]) -> bool {
    actions.iter().any(|action| {
        matches!(
            action,
            UpdateAction::SetStructural(_) | UpdateAction::UnsetStructural(_)
        )
    })
}

/// Validates the attribute bag one statement's actions left behind (§34–§39).
///
/// Run once per element after every action, not per action: `UNSET ATTRIBUTES
/// {status} SET ATTRIBUTES {status: "adopted"}` passes through a state with no
/// `status` at all, and a required attribute is a statement about what the
/// element *is* when the statement ends, not about the order its clauses were
/// written in.
///
/// `view` is the element as the statement found it, which is what makes §39
/// answerable: immutability constrains a transition, so establishing a value
/// and rewriting one have to be told apart.
pub async fn check_attributes(
    tx: &mut Transaction,
    id: ElementId,
    view: &Json,
) -> Result<(), KipError> {
    let (schema_ref, after) = match tx.load(id).await? {
        Element::Concept(row) => (row.schema_ref.clone(), row.attributes.clone()),
        // No other kind has an author-writable attribute bag (§6.4), and the
        // actions that would have written one were already refused.
        _ => return Ok(()),
    };
    // A type this environment cannot resolve declares nothing, so it declares
    // no contract to hold the write to — the same stance a Proposition takes
    // on an unresolvable predicate. Deactivating a package stops validating
    // its elements; it does not start refusing them.
    let Ok(symbol) = schema_ref.parse::<crate::schema::SymbolRef>() else {
        return Ok(());
    };
    let Ok(def) = tx.env.concept_type_def(&symbol) else {
        return Ok(());
    };
    let before = match view.get("attributes") {
        Some(Json::Object(attributes)) => attributes.clone(),
        _ => Map::new(),
    };
    let mut result = crate::schema::validate_attributes(&schema_ref, &def.attributes, &after);
    result.extend(crate::schema::validate_attribute_mutability(
        &schema_ref,
        &def.attributes,
        &before,
        &after,
    ));
    result.into_result()?;
    Ok(())
}

/// The members an element already carries under one Facet symbol.
///
/// Read from the rendered view rather than the row, because that is the state
/// the whole statement is judged against: every action of one `UPDATE` sees
/// the element as it was when the statement began (§52.4).
fn current_members(view: &Json, facet: &str) -> Map<String, Json> {
    match view.get("facets").and_then(|facets| facets.get(facet)) {
        Some(Json::Object(members)) => members.clone(),
        _ => Map::new(),
    }
}

/// The immutability contract of one Facet, as the Schema Environment reads it.
///
/// Resolved before the element is borrowed mutably, and empty when the Facet
/// resolves to no definition this Space can read — an engine that cannot see
/// the contract does not get to invent one.
struct Pinned {
    schema_ref: String,
    def: Option<crate::schema::FacetDef>,
}

impl Pinned {
    fn check(&self, before: &Map<String, Json>, after: &Map<String, Json>) -> Result<(), KipError> {
        let Some(def) = &self.def else {
            return Ok(());
        };
        crate::schema::validate_facet_mutability(&self.schema_ref, def, before, after)
            .into_result()?;
        Ok(())
    }
}

fn facet_contract(
    tx: &Transaction,
    b: &Bindings<'_>,
    facet: &anda_kip::SymbolRef,
) -> Result<Pinned, KipError> {
    let name = super::clauses::symbol_name(b, facet)?;
    let symbol = tx.env.resolve_symbol(
        crate::schema::SymbolKind::Facet,
        &name,
        crate::schema::Intent::Write,
    )?;
    Ok(Pinned {
        schema_ref: symbol.to_string(),
        def: tx.env.facet_def(&symbol).ok().cloned(),
    })
}

async fn unset_facet(
    tx: &mut Transaction,
    id: ElementId,
    unset: &FacetUnset,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<Applied, KipError> {
    let b = bindings(tx, request, operation);
    let name = super::clauses::symbol_name(&b, &unset.facet)?;
    let symbol = tx.env.resolve_symbol(
        crate::schema::SymbolKind::Facet,
        &name,
        crate::schema::Intent::Write,
    )?;
    let key = symbol.to_string();

    let pinned = facet_contract(tx, &b, &unset.facet)?;
    let facets = facets_mut(tx, id).await?;
    let mut applied = Applied::default();
    let Some(Json::Object(members)) = facets.get_mut(&key) else {
        return Ok(applied);
    };
    // Erasing an immutable member is rewriting it to absent.
    let mut after = members.clone();
    for field in &unset.fields {
        after.remove(field);
    }
    pinned.check(members, &after)?;
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
    let mut resolved = resolve_edges(tx, edges, Some(view), request, operation)?;
    // §11.3: a reference added now resolves through whatever merges the Space
    // has already declared, so an edge cannot re-point at an identity the
    // Space said was the same as another one.
    for edge in &mut resolved {
        edge.value = super::clauses::canonicalize_reference(tx, edge.value.clone()).await?;
    }

    let mut applied = Applied::default();
    for edge in resolved {
        if let Some(index) = edge.index
            && !tx.claim_position(id, &edge.field, index)
        {
            return Err(position_taken(&edge.field, index));
        }
        let structural = structural_mut(tx, id).await?;
        let entry = structural
            .entry(edge.field.clone())
            .or_insert_with(|| Json::Array(Vec::new()));
        let Json::Array(items) = entry else { continue };
        // §17.5: on a single-cardinality field, `SET STRUCTURAL` *replaces*.
        // Appending and then failing the cardinality check would refuse the
        // one write the Specification says this form is for.
        if edge.single {
            // The one position a single-cardinality field has is 0, and an
            // `index` on an unordered field is refused wherever it is written
            // — dropping it here would let an author believe they had ordered
            // a field no query can order (§17.4).
            if let Some(index) = edge.index {
                if !edge.ordered {
                    return Err(unordered_index(&edge.field));
                }
                if index > 0 {
                    return Err(KipError::constraint_violation(format!(
                        "position {index} is outside `{}`, which holds at most one reference; \
                         positions are dense, and the only one it has is 0 (§17.4)",
                        edge.field
                    )));
                }
            }
            let replaced = items
                .first()
                .is_none_or(|held| !same_reference(held, &edge.value));
            items.clear();
            items.push(edge.value);
            applied.changed |= replaced;
            continue;
        }
        if place_reference(items, edge.value, edge.index, edge.ordered, &edge.field)? {
            applied.changed = true;
        }
    }
    Ok(applied)
}

/// One resolved `SET STRUCTURAL` edge: where it points, and where it goes.
pub(crate) struct ResolvedEdge {
    pub field: String,
    pub value: Json,
    pub index: Option<usize>,
    pub ordered: bool,
    /// Whether the field holds at most one reference (§17.5).
    pub single: bool,
}

/// Resolves the field symbol, the target and the declared position of each edge.
pub(crate) fn resolve_edges(
    tx: &Transaction,
    edges: &[StructuralEdge],
    view: Option<&Json>,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<Vec<ResolvedEdge>, KipError> {
    let b = bindings(tx, request, operation);
    let mut resolved = Vec::with_capacity(edges.len());
    for edge in edges {
        let field = resolve_structural_field(tx, &b, &edge.field)?;
        let symbol: crate::schema::SymbolRef = field.parse()?;
        let def = tx.env.structural_field_def(&symbol)?;
        let ordered = def.ordered;
        let single = def.cardinality.max == Some(1);
        let index = match &edge.options {
            Some(options) => match options.get("index") {
                Some(value) => Some(super::clauses::read_index(&b.bound(value, view)?)?),
                None => None,
            },
            None => None,
        };
        resolved.push(ResolvedEdge {
            field,
            value: structural_value(b.value(&edge.value, view)?),
            index,
            ordered,
            single,
        });
    }
    Ok(resolved)
}

/// An `index` on a field that declares no order (§17.4).
fn unordered_index(field: &str) -> KipError {
    KipError::constraint_violation(format!(
        "`{field}` is not an ordered structural field, so a reference in it has no position; an \
         `index` here would order nothing and no query could read it back (§17.4)"
    ))
}

fn position_taken(field: &str, index: usize) -> KipError {
    KipError::constraint_violation(format!(
        "two references claim position {index} of `{field}` in one mutation plan; an order cannot \
         hold both, and picking one would be the engine choosing (§17.4)"
    ))
}

/// Places one reference in a structural field, honoring declared order (§17.4).
///
/// An **ordered** field carries one stable, dense, zero-based total order per
/// source element. Three rules the Specification states as MUSTs, and which
/// this engine used to accept and then drop on the floor:
///
/// - a reference written without an index appends, in mutation order;
/// - an explicit `{index: n}` declares the intended position, and one outside
///   the dense range `0..=len` fails validation — positions are dense, and
///   appending is exactly `len`;
/// - two explicit positions that collide inside one mutation plan fail.
///
/// An **unordered** field has no positions at all, so `{index: n}` on one is
/// refused rather than ignored: silently dropping it would let an author
/// believe they had ordered something no query can order.
///
/// Returns whether the field's contents changed.
pub(crate) fn place_reference(
    items: &mut Vec<Json>,
    value: Json,
    index: Option<usize>,
    ordered: bool,
    field: &str,
) -> Result<bool, KipError> {
    let Some(index) = index else {
        if items.iter().any(|item| same_reference(item, &value)) {
            return Ok(false);
        }
        items.push(value);
        return Ok(true);
    };

    if !ordered {
        return Err(unordered_index(field));
    }

    // A reference already present is moved rather than duplicated: re-stating
    // one with a position is how an author re-orders.
    let existing = items.iter().position(|item| same_reference(item, &value));
    if let Some(from) = existing {
        items.remove(from);
    }
    if index > items.len() {
        return Err(KipError::constraint_violation(format!(
            "position {index} is outside `{field}`, which holds {} reference(s); positions are \
             dense, and appending is position {} (§17.4)",
            items.len(),
            items.len()
        )));
    }
    items.insert(index, value);
    Ok(existing != Some(index))
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
                "{what} does not reach a Proposition: the tuple is its whole content (§12.2), a \
                 different tuple is a different Proposition (§12.5), and it carries no \
                 author-writable attribute bag (§6.4). Representation-local state about a tuple \
                 goes in a Facet; anything with its own source, confidence or validity is an \
                 Assertion"
            ),
        ),
        ElementKind::Concept => KipError::new(
            KipErrorCode::ImmutableField,
            format!("{what} does not reach {id}"),
        ),
    }
}
