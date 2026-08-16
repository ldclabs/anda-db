//! # The mutation clauses
//!
//! One handler per [`MutationClause`]. The shape they share: resolve schema
//! symbols and values, build the final row, stage it. Nothing here writes to
//! storage — [`Transaction::commit`] does that once, which is what makes the
//! version rule and the change stream come out right.
//!
//! ## The lines these handlers hold
//!
//! **A Proposition is truth-neutral** (§60). `ENSURE PROPOSITION` resolves or
//! creates a tuple and records no confidence, no source and no stance, because
//! those belong to Assertions about it.
//!
//! **An Assertion's epistemic payload is immutable** (§76). There is no clause
//! that edits stance, mode, confidence or evidence — correcting a claim is
//! `CREATE ASSERTION` plus `SUPERSEDE`, which is why the parser rejects
//! `UPDATE ?a SET FIELDS {confidence: ...}` before an engine ever sees it.
//!
//! **Retraction is not deletion** (§41.1). A retracted Assertion keeps
//! existing; only its lifecycle status moves, so the historical record of what
//! was once believed survives.

use anda_kip::{
    ConceptCreate, ConceptUpsert, CorrectEvidence, ElementKind, EnsureProposition, Json, KipError,
    Map, MatchValue, MutationClause, RecordCreate, RemovalStatement, RetractAssertion,
    SetRetention, SupersedeAssertion, SymbolRef as AstSymbolRef, TransitionActivity,
};
use std::collections::BTreeMap;

use super::value::{Bindings, assignments_to_json, reference};
use crate::id::ElementId;
use crate::schema::{EndpointFacts, Intent};
use crate::store::rows::*;
use crate::store::{Element, Store};
use crate::term::{Endpoint, tuple_key};
use crate::time;
use crate::tx::Transaction;

/// Declares the handles a clause binds, before any clause is interpreted.
///
/// Phase 1 of two-phase planning (§23): every handle must exist before any
/// clause runs, because a clause may reference a handle a later clause
/// declares.
pub async fn declare_handles(
    tx: &mut Transaction,
    clause: &MutationClause,
) -> Result<(), KipError> {
    let (handle, kind) = match clause {
        MutationClause::CreateConcept(c) => (Some(c.handle.as_str()), ElementKind::Concept),
        MutationClause::UpsertConcept(_) => return Ok(()),
        MutationClause::CreateEvidence(c) => (Some(c.handle.as_str()), ElementKind::Evidence),
        MutationClause::CreateAssertion(c) => (Some(c.handle.as_str()), ElementKind::Assertion),
        MutationClause::CreateActivity(c) => (Some(c.handle.as_str()), ElementKind::Activity),
        // `ENSURE` may resolve to an existing tuple, so its id cannot be
        // minted up front; it is bound in phase 2.
        MutationClause::EnsureProposition(_) => return Ok(()),
        _ => return Ok(()),
    };
    if let Some(handle) = handle {
        tx.declare(handle, kind).await?;
    }
    Ok(())
}

/// Which planning pass a clause belongs to.
///
/// Clause order carries no mutation semantics (§24), so the engine is free to
/// choose one — and it needs to, because two clause families cannot see
/// everything they need in source order:
///
/// ```text
/// 0  CREATE CONCEPT       stages typed Concepts other clauses validate against
/// 1  UPSERT / ENSURE      resolve existing identity, binding their handles late
/// 2  everything else      sees a complete handle map and every staged type
/// ```
///
/// `ENSURE` is in pass 1 rather than pass 0 because checking a predicate's
/// declared subject type means knowing what type the subject *is* — including
/// when this same transaction just created it. It is after pass 2 for the
/// opposite reason: the `ASSERT` desugaring emits an `ENSURE` whose handle the
/// generated `CREATE ASSERTION` reads.
pub fn plan_pass(clause: &MutationClause) -> u8 {
    match clause {
        MutationClause::CreateConcept(_) => 0,
        MutationClause::UpsertConcept(_) | MutationClause::EnsureProposition(_) => 1,
        _ => 2,
    }
}

/// How many planning passes [`plan_pass`] distributes clauses over.
pub const PLAN_PASSES: u8 = 3;

/// Interprets one clause against a plan with every handle already bound.
pub async fn apply(
    store: &Store,
    tx: &mut Transaction,
    clause: &MutationClause,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    match clause {
        MutationClause::CreateConcept(c) => create_concept(tx, c, request, operation).await,
        MutationClause::UpsertConcept(c) => upsert_concept(store, tx, c, request, operation).await,
        MutationClause::EnsureProposition(c) => {
            ensure_proposition(store, tx, c, request, operation).await
        }
        MutationClause::CreateEvidence(c) => {
            create_record(tx, c, ElementKind::Evidence, request, operation).await
        }
        MutationClause::CreateAssertion(c) => {
            create_record(tx, c, ElementKind::Assertion, request, operation).await
        }
        MutationClause::CreateActivity(c) => {
            create_record(tx, c, ElementKind::Activity, request, operation).await
        }
        MutationClause::RetractAssertion(c) => retract(tx, c, request, operation).await,
        MutationClause::SupersedeAssertion(c) => supersede(tx, c, request, operation).await,
        MutationClause::CorrectEvidence(c) => correct_evidence(tx, c, request, operation).await,
        MutationClause::TransitionActivity(c) => transition(tx, c, request, operation).await,
        MutationClause::SetRetention(c) => set_retention(tx, c, request, operation).await,
        MutationClause::Archive(c) => {
            remove(tx, c, state::ARCHIVED, "archive", request, operation).await
        }
        MutationClause::Tombstone(c) => {
            remove(tx, c, state::TOMBSTONED, "tombstone", request, operation).await
        }
        MutationClause::Update(_) | MutationClause::Purge(_) | MutationClause::MergeConcept(_) => {
            Err(KipError::unsupported_capability(
                "UPDATE, PURGE and MERGE CONCEPT are not implemented in this engine yet; they need \
             the KQL solver for their selection blocks and, for PURGE, the Governance plane",
            ))
        }
    }
}

fn bindings<'a>(
    tx: &'a Transaction,
    request: Option<&'a Map<String, Json>>,
    operation: Option<&'a Map<String, Json>>,
) -> Bindings<'a> {
    Bindings {
        request,
        operation,
        handles: tx.handles(),
    }
}

/// Reads a `SymbolRef` slot — a quoted symbol or a parameter — to a local name.
fn symbol_name(b: &Bindings<'_>, symbol: &AstSymbolRef) -> Result<String, KipError> {
    match symbol {
        AstSymbolRef::Name(name) => Ok(name.clone()),
        AstSymbolRef::Param(name) => match b.param(name)? {
            Json::String(text) => Ok(text),
            other => Err(KipError::type_mismatch(format!(
                "the parameter :{name} must carry a schema symbol string, got {other}"
            ))),
        },
    }
}

/// Splits a `SET FIELDS` map into the columns one element kind accepts.
///
/// Field mutability is enforced by element kind (§57): the set of writable
/// fields is a property of what the element *is*, not of who is writing.
struct Fields(Map<String, Json>);

impl Fields {
    fn take(&mut self, name: &str) -> Option<Json> {
        self.0.remove(name)
    }

    fn text(&mut self, name: &str) -> Result<String, KipError> {
        match self.take(name) {
            None | Some(Json::Null) => Ok(String::new()),
            Some(Json::String(text)) => Ok(text),
            Some(other) => Err(KipError::type_mismatch(format!(
                "`{name}` must be a string, got {other}"
            ))),
        }
    }

    fn timestamp(&mut self, name: &str) -> Result<String, KipError> {
        match self.take(name) {
            None | Some(Json::Null) => Ok(String::new()),
            Some(Json::String(text)) => time::normalize(&text, name),
            Some(other) => Err(KipError::type_mismatch(format!(
                "`{name}` must be an RFC 3339 timestamp string, got {other}"
            ))),
        }
    }

    fn json(&mut self, name: &str) -> Json {
        self.take(name).unwrap_or(Json::Null)
    }

    fn array(&mut self, name: &str) -> Result<Vec<Json>, KipError> {
        match self.take(name) {
            None | Some(Json::Null) => Ok(vec![]),
            Some(Json::Array(items)) => Ok(items),
            Some(other) => Ok(vec![other]),
        }
    }

    /// Reports any field the element kind does not accept.
    ///
    /// Silently dropping one would mean a caller's write appeared to succeed
    /// while the value went nowhere.
    fn rest(self, kind: &str) -> Result<Map<String, Json>, KipError> {
        if self.0.is_empty() {
            return Ok(Map::new());
        }
        let names: Vec<&str> = self.0.keys().map(String::as_str).collect();
        Err(KipError::schema_field_not_found(format!(
            "a {kind} has no field(s) named: {}",
            names.join(", ")
        )))
    }
}

/// Builds the Facets map, resolving each Facet symbol to its exact reference.
async fn facets_of(
    tx: &Transaction,
    b: &Bindings<'_>,
    assignments: &[anda_kip::FacetAssignment],
    carrier: ElementKind,
) -> Result<Map<String, Json>, KipError> {
    let mut facets = Map::new();
    for assignment in assignments {
        let name = symbol_name(b, &assignment.facet)?;
        let symbol =
            tx.env
                .resolve_symbol(crate::schema::SymbolKind::Facet, &name, Intent::Write)?;
        let members = assignments_to_json(b, &assignment.values, None)?;
        facets.insert(symbol.to_string(), Json::Object(members));
    }
    tx.env
        .validate_facets(&facets, carrier, Intent::Write)?
        .into_result()?;
    Ok(facets)
}

/// The structural edges of one clause, split by who owns the field.
///
/// Core structural fields — `Assertion.evidence`, `Evidence.source`,
/// `Activity.inputs`/`outputs` — are defined by the protocol itself (Spec §8.2)
/// and land in typed columns. Everything else is a Profile field, resolved
/// through the Schema Environment into the generic `structural` map.
///
/// Routing them together would be the mistake: a Profile could then declare a
/// field named `evidence` and quietly change what an Assertion cites.
/// One structural edge: the element it points at, plus its edge options.
type Edge = (Json, Map<String, Json>);

#[derive(Default)]
struct Structural {
    core: BTreeMap<String, Vec<Edge>>,
    profile: Map<String, Json>,
}

impl Structural {
    fn take(&mut self, field: &str) -> Vec<Edge> {
        self.core.remove(field).unwrap_or_default()
    }

    fn values(&mut self, field: &str) -> Vec<Json> {
        self.take(field)
            .into_iter()
            .map(|(value, _)| value)
            .collect()
    }

    fn one(&mut self, field: &str) -> Option<Json> {
        self.take(field).into_iter().next().map(|(value, _)| value)
    }
}

/// Splits and resolves `SET STRUCTURAL` edges.
fn collect_structural(
    tx: &Transaction,
    b: &Bindings<'_>,
    edges: Option<&Vec<anda_kip::StructuralEdge>>,
    core_fields: &[&str],
) -> Result<Structural, KipError> {
    let mut out = Structural::default();
    let Some(edges) = edges else {
        return Ok(out);
    };
    let mut grouped: BTreeMap<String, Vec<Json>> = BTreeMap::new();
    for edge in edges {
        let name = symbol_name(b, &edge.field)?;
        let value = b.value(&edge.value, None)?;
        if core_fields.contains(&name.as_str()) {
            let mut options = Map::new();
            if let Some(block) = &edge.options {
                for (key, item) in block {
                    options.insert(key.clone(), b.bound(item, None)?);
                }
            }
            out.core.entry(name).or_default().push((value, options));
            continue;
        }
        let symbol = tx.env.resolve_symbol(
            crate::schema::SymbolKind::StructuralField,
            &name,
            Intent::Write,
        )?;
        grouped.entry(symbol.to_string()).or_default().push(value);
    }
    for (field, refs) in grouped {
        out.profile.insert(field, Json::Array(refs));
    }
    Ok(out)
}

async fn create_concept(
    tx: &mut Transaction,
    clause: &ConceptCreate,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    let b = bindings(tx, request, operation);
    let id = b.handle(&clause.handle)?;
    let type_name = clause
        .r#type
        .as_ref()
        .map(|symbol| symbol_name(&b, symbol))
        .transpose()?
        .ok_or_else(|| {
            KipError::schema_symbol_not_found(
                "CREATE CONCEPT needs a TYPE: a Concept's type is schema-defined, and this engine \
                 will not invent one",
            )
        })?;

    let attributes = clause
        .set_attributes
        .as_ref()
        .map(|a| assignments_to_json(&b, a, None))
        .transpose()?
        .unwrap_or_default();
    let name = clause
        .name
        .as_ref()
        .map(|scalar| b.scalar_str(scalar, "NAME"))
        .transpose()?
        .unwrap_or_default();
    let client_key = clause
        .client_key
        .as_ref()
        .map(|scalar| b.scalar_str(scalar, "CLIENT KEY"))
        .transpose()?
        .unwrap_or_default();
    let mut fields = Fields(
        clause
            .set_fields
            .as_ref()
            .map(|f| assignments_to_json(&b, f, None))
            .transpose()?
            .unwrap_or_default(),
    );
    let key = fields.text("key")?;
    let canonical_id = fields.text("canonical_id")?;
    let aliases = fields
        .array("aliases")?
        .into_iter()
        .filter_map(|value| value.as_str().map(str::to_string))
        .collect();
    let retention = fields.json("retention");
    let governance = fields.json("governance");
    let extra_name = fields.text("name")?;
    fields.rest("Concept")?;

    let facets = facets_of(tx, &b, &clause.set_facets, ElementKind::Concept).await?;
    // A Concept has no Core structural fields; every one is Profile-defined.
    let structural = collect_structural(tx, &b, clause.set_structural.as_ref(), &[])?.profile;

    let (symbol, validation) =
        tx.env
            .prepare_concept(&type_name, &attributes, &facets, Intent::Write)?;
    validation.into_result()?;

    let row = ConceptRow {
        _id: id.seq,
        schema_ref: symbol.to_string(),
        key,
        name: if name.is_empty() { extra_name } else { name },
        canonical_id,
        aliases,
        attributes,
        facets,
        structural,
        client_key,
        expires_at: expires_at(&retention)?,
        retention,
        governance,
        ..Default::default()
    };
    tx.stage_new(id, Element::Concept(Box::new(row)), "create");
    Ok(())
}

async fn create_record(
    tx: &mut Transaction,
    clause: &RecordCreate,
    kind: ElementKind,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    let b = bindings(tx, request, operation);
    let id = b.handle(&clause.handle)?;
    let client_key = clause
        .client_key
        .as_ref()
        .map(|scalar| b.scalar_str(scalar, "CLIENT KEY"))
        .transpose()?
        .unwrap_or_default();
    let mut fields = Fields(
        clause
            .set_fields
            .as_ref()
            .map(|f| assignments_to_json(&b, f, None))
            .transpose()?
            .unwrap_or_default(),
    );
    let facets = facets_of(tx, &b, &clause.set_facets, kind).await?;
    let mut structural =
        collect_structural(tx, &b, clause.set_structural.as_ref(), core_fields(kind))?;
    let retention = fields.json("retention");
    let governance = fields.json("governance");

    let row = match kind {
        ElementKind::Evidence => {
            let payload = fields.json("payload");
            let (payload_mode, payload_inline, content_ref) = split_payload(payload)?;
            let source_refs = structural.values("source");
            let row = EvidenceRow {
                _id: id.seq,
                evidence_class: require_text(&mut fields, "evidence_class", "CREATE EVIDENCE")?,
                payload_mode,
                payload_inline,
                content_ref,
                content_digest: fields.text("content_digest")?,
                media_type: fields.text("media_type")?,
                observed_at: fields.timestamp("observed_at")?,
                source_keys: source_refs.iter().map(endpoint_key).collect(),
                source_refs,
                generated_by: structural
                    .one("generated_by")
                    .map(|value| reference_id(&value))
                    .unwrap_or_default(),
                status: "active".to_string(),
                client_key,
                facets,
                structural: structural.profile,
                expires_at: expires_at(&retention)?,
                retention,
                governance,
                ..Default::default()
            };
            Element::Evidence(Box::new(row))
        }
        ElementKind::Assertion => {
            let proposition = require_reference(&mut fields, "proposition", "CREATE ASSERTION")?;
            let asserted_by = fields.json("asserted_by");
            // Each citation keeps the role it was cited in: Core records that
            // this Assertion cites E *as supporting*, and never that E proves
            // anything — that judgement belongs to the Projection (§8.4).
            let evidence: Vec<Json> = structural
                .take("evidence")
                .into_iter()
                .map(|(value, options)| {
                    let mut citation = Map::new();
                    citation.insert("evidence_id".into(), Json::String(reference_id(&value)));
                    if let Some(role) = options.get("role") {
                        citation.insert("role".into(), role.clone());
                    }
                    Json::Object(citation)
                })
                .collect();
            let valid_time = fields.json("valid_time");
            let row = AssertionRow {
                _id: id.seq,
                proposition_id: proposition.to_string(),
                asserted_by_key: endpoint_key(&asserted_by),
                asserted_by,
                stance: require_text(&mut fields, "stance", "CREATE ASSERTION")?,
                mode: require_text(&mut fields, "mode", "CREATE ASSERTION")?,
                confidence: match fields.take("confidence") {
                    None | Some(Json::Null) => -1.0,
                    Some(Json::Number(n)) => n.as_f64().unwrap_or(-1.0),
                    Some(other) => {
                        return Err(KipError::type_mismatch(format!(
                            "`confidence` must be a number in [0, 1], got {other}"
                        )));
                    }
                },
                asserted_at: fields.timestamp("asserted_at")?,
                valid_from: valid_time_part(&valid_time, "from")?,
                valid_until: valid_time_part(&valid_time, "until")?,
                evidence_ids: evidence.iter().filter_map(evidence_id).collect(),
                evidence_refs: evidence,
                context_refs: structural.values("context"),
                status: "active".to_string(),
                client_key,
                facets,
                structural: structural.profile,
                expires_at: expires_at(&retention)?,
                retention,
                governance,
                ..Default::default()
            };
            if row.confidence > 1.0 {
                return Err(KipError::type_mismatch(
                    "`confidence` is epistemic support in [0, 1]",
                ));
            }
            Element::Assertion(Box::new(row))
        }
        ElementKind::Activity => {
            let inputs = structural.values("inputs");
            let outputs = structural.values("outputs");
            let row = ActivityRow {
                _id: id.seq,
                activity_class: require_text(&mut fields, "activity_class", "CREATE ACTIVITY")?,
                started_at: fields.timestamp("started_at")?,
                ended_at: fields.timestamp("ended_at")?,
                input_keys: inputs.iter().map(endpoint_key).collect(),
                inputs,
                output_keys: outputs.iter().map(endpoint_key).collect(),
                outputs,
                associated_actors: structural.values("associated_actors"),
                parameters_digest: fields.text("parameters_digest")?,
                status: {
                    let status = fields.text("status")?;
                    if status.is_empty() {
                        "pending".to_string()
                    } else {
                        status
                    }
                },
                client_key,
                facets,
                structural: structural.profile,
                expires_at: expires_at(&retention)?,
                retention,
                governance,
                ..Default::default()
            };
            Element::Activity(Box::new(row))
        }
        other => {
            return Err(KipError::internal_error(format!(
                "{other} has no record-create form"
            )));
        }
    };
    fields.rest(&kind.to_string())?;
    tx.stage_new(id, row, "create");
    Ok(())
}

async fn ensure_proposition(
    store: &Store,
    tx: &mut Transaction,
    clause: &EnsureProposition,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    let b = bindings(tx, request, operation);
    let subject = b.term(&clause.subject)?;
    let object = b.term(&clause.object)?;
    let predicate = match &clause.predicate {
        anda_kip::PredAtom::Literal(name) => name.clone(),
        anda_kip::PredAtom::Param(name) => match b.param(name)? {
            Json::String(text) => text,
            other => {
                return Err(KipError::type_mismatch(format!(
                    "the parameter :{name} must carry a predicate symbol, got {other}"
                )));
            }
        },
        anda_kip::PredAtom::Variable(_) => {
            return Err(KipError::invalid_syntax(
                "ENSURE PROPOSITION needs an exact predicate; a variable predicate is a read form",
            ));
        }
    };

    let expect_version = clause
        .expect_version
        .as_ref()
        .map(|scalar| b.scalar_u64(scalar, "EXPECT VERSION"))
        .transpose()?;
    // `b` borrows `tx`, and resolving endpoint facts needs it mutably.
    let _ = b;

    let subject_facts = facts_for(store, tx, &subject).await?;
    let object_facts = facts_for(store, tx, &object).await?;
    let (symbol, validation) =
        tx.env
            .prepare_proposition(&predicate, &subject_facts, &object_facts, Intent::Write)?;
    validation.into_result()?;

    let key = tuple_key(&tx.cx.space, &subject, &symbol.to_string(), &object);

    // Resolve-or-create: one Space keeps one canonical Proposition per
    // semantic tuple (§59), so an existing tuple is bound rather than
    // duplicated — and binding it changes nothing, because the tuple is
    // immutable (§61).
    if let Some(existing) = store.find_proposition(&key).await? {
        let id = ElementId::new(ElementKind::Proposition, existing._id);
        if let Some(expected) = expect_version {
            tx.expect_version(id, expected).await?;
        }
        if let Some(handle) = &clause.handle {
            tx.bind_existing(handle, id)?;
        }
        return Ok(());
    }

    if let Some(expected) = expect_version {
        // Spec §42: `EXPECT VERSION 0` is the create-only guard, and it is
        // satisfied precisely because nothing was found above.
        if expected != 0 {
            return Err(KipError::version_conflict(format!(
                "this tuple does not exist yet, so it cannot be at version {expected}"
            )));
        }
    }

    let id = tx.mint(ElementKind::Proposition).await?;
    if let Some(handle) = &clause.handle {
        tx.bind_existing(handle, id)?;
    }
    let row = PropositionRow {
        _id: id.seq,
        subject: subject.to_json(),
        subject_key: subject.key(),
        predicate_ref: symbol.to_string(),
        object: object.to_json(),
        object_key: object.key(),
        tuple_key: key,
        ..Default::default()
    };
    tx.stage_new(id, Element::Proposition(Box::new(row)), "create");
    Ok(())
}

async fn upsert_concept(
    store: &Store,
    tx: &mut Transaction,
    clause: &ConceptUpsert,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    let b = bindings(tx, request, operation);
    let matcher = clause.r#match.as_ref().ok_or_else(|| {
        KipError::identity_selector_required(
            "UPSERT CONCEPT needs a MATCH block carrying `id` or `key`",
        )
    })?;

    // Spec §51: name-only upsert is forbidden. A name is mutable grounding
    // state that may be duplicated, so resolving identity through it would
    // merge two different Concepts that happen to share a label.
    let selector = matcher
        .get("id")
        .map(|value| ("id", value))
        .or_else(|| matcher.get("key").map(|value| ("key", value)))
        .ok_or_else(|| {
            KipError::identity_selector_required(
                "UPSERT CONCEPT resolves identity through `id` or `key` only; `name` is mutable \
                 grounding state and two Concepts may share one",
            )
        })?;

    let selector_value = match selector.1 {
        MatchValue::Literal(value) => Json::from(value.clone()),
        MatchValue::Param(name) => b.param(name)?,
        _ => {
            return Err(KipError::identity_selector_required(
                "an UPSERT identity selector must be a literal or a parameter",
            ));
        }
    };
    let Json::String(selector_value) = selector_value else {
        return Err(KipError::type_mismatch(
            "an UPSERT identity selector must be a string",
        ));
    };

    let existing = match selector.0 {
        "id" => {
            let id: ElementId = selector_value.parse()?;
            store.contains(id).await.then_some(id)
        }
        _ => store
            .find_concept_by_key(&tx.cx.space, &selector_value)
            .await?
            .map(|row| ElementId::new(ElementKind::Concept, row._id)),
    };

    let id = match existing {
        Some(id) => {
            if let Some(expected) = &clause.expect_version {
                let expected = b.scalar_u64(expected, "EXPECT VERSION")?;
                tx.expect_version(id, expected).await?;
            }
            id
        }
        None => {
            if let Some(expected) = &clause.expect_version {
                let expected = b.scalar_u64(expected, "EXPECT VERSION")?;
                if expected != 0 {
                    return Err(KipError::version_conflict(format!(
                        "no Concept matches this selector, so it cannot be at version {expected}"
                    )));
                }
            }
            if selector.0 == "id" {
                return Err(KipError::not_found_or_not_visible(format!(
                    "{selector_value} does not exist, and an UPSERT by id cannot mint an id the \
                     caller chose"
                )));
            }
            let id = tx.mint(ElementKind::Concept).await?;
            let row = ConceptRow {
                _id: id.seq,
                key: selector_value.clone(),
                ..Default::default()
            };
            tx.stage_new(id, Element::Concept(Box::new(row)), "create");
            id
        }
    };
    tx.bind_existing(&clause.handle, id)?;

    apply_concept_assignments(tx, clause, id, request, operation).await
}

async fn apply_concept_assignments(
    tx: &mut Transaction,
    clause: &ConceptUpsert,
    id: ElementId,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    let b = bindings(tx, request, operation);
    let attributes = clause
        .set_attributes
        .as_ref()
        .map(|a| assignments_to_json(&b, a, None))
        .transpose()?;
    let mut fields = Fields(
        clause
            .set_fields
            .as_ref()
            .map(|f| assignments_to_json(&b, f, None))
            .transpose()?
            .unwrap_or_default(),
    );
    let name = fields.take("name");
    let canonical_id = fields.take("canonical_id");
    let unset = clause.unset_attributes.clone().unwrap_or_default();
    let facets = facets_of(tx, &b, &clause.set_facets, ElementKind::Concept).await?;
    fields.rest("Concept")?;

    let element = tx.load(id).await?;
    let Element::Concept(row) = element else {
        return Err(KipError::structural_reference_invalid(format!(
            "{id} is not a Concept"
        )));
    };

    let mut changed = false;
    if let Some(attributes) = attributes {
        for (key, value) in attributes {
            if row.attributes.get(&key) != Some(&value) {
                row.attributes.insert(key, value);
                changed = true;
            }
        }
    }
    for key in unset {
        if row.attributes.remove(&key).is_some() {
            changed = true;
        }
    }
    if let Some(Json::String(name)) = name
        && row.name != name
    {
        row.name = name;
        changed = true;
    }
    if let Some(Json::String(canonical)) = canonical_id
        && row.canonical_id != canonical
    {
        row.canonical_id = canonical;
        changed = true;
    }
    for (facet, value) in facets {
        if row.facets.get(&facet) != Some(&value) {
            row.facets.insert(facet, value);
            changed = true;
        }
    }

    // A no-effect final state changes nothing: no version bump, no change
    // record, no receipt claiming a transition that did not happen (§44).
    if changed {
        tx.mark_changed(id, "update");
    }
    Ok(())
}

async fn retract(
    tx: &mut Transaction,
    clause: &RetractAssertion,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    if clause.where_clauses.is_some() {
        return Err(unsupported_selection("RETRACT ASSERTION"));
    }
    let b = bindings(tx, request, operation);
    let id = b.element_ref(&clause.target)?;
    let expect_state = clause
        .expect_state
        .as_ref()
        .map(|s| b.scalar_str(s, "EXPECT STATE"))
        .transpose()?;
    let at = tx.cx.at.clone();

    if let Some(expected) = expect_state {
        tx.expect_assertion_status(id, &expected).await?;
    }
    let row = assertion_mut(tx, id).await?;
    // Spec §41.1: retraction is not deletion. The Assertion goes on existing,
    // so the record of what was once believed — and by whom — survives.
    if row.status == "retracted" {
        return Ok(());
    }
    row.status = "retracted".to_string();
    row.retracted_at = at;
    tx.mark_changed(id, "retract");
    Ok(())
}

async fn supersede(
    tx: &mut Transaction,
    clause: &SupersedeAssertion,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    let b = bindings(tx, request, operation);
    let old = b.element_ref(&clause.target)?;
    let new = b.element_ref(&clause.by)?;
    let expect_state = clause
        .expect_state
        .as_ref()
        .map(|s| b.scalar_str(s, "EXPECT STATE"))
        .transpose()?;
    if old == new {
        return Err(KipError::new(
            anda_kip::KipErrorCode::SupersessionMismatch,
            "an Assertion cannot supersede itself",
        ));
    }
    if let Some(expected) = expect_state {
        tx.expect_assertion_status(old, &expected).await?;
    }

    let old_row = assertion_mut(tx, old).await?;
    let proposition = old_row.proposition_id.clone();
    old_row.status = "superseded".to_string();
    if !old_row.superseded_by.contains(&new.to_string()) {
        old_row.superseded_by.push(new.to_string());
    }
    tx.mark_changed(old, "supersede");

    let new_row = assertion_mut(tx, new).await?;
    // Supersession is belief revision within one lineage, so the replacement
    // must be about the same Proposition. Two claims about different tuples
    // are a contradiction, and a contradiction is not a supersession (§31 of
    // the Epistemic Model).
    if new_row.proposition_id != proposition {
        return Err(KipError::new(
            anda_kip::KipErrorCode::SupersessionMismatch,
            format!(
                "{new} is about {}, not about {proposition}",
                new_row.proposition_id
            ),
        ));
    }
    if !new_row.supersedes.contains(&old.to_string()) {
        new_row.supersedes.push(old.to_string());
        tx.mark_changed(new, "supersede");
    }
    Ok(())
}

async fn correct_evidence(
    tx: &mut Transaction,
    clause: &CorrectEvidence,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    let b = bindings(tx, request, operation);
    let old = b.element_ref(&clause.target)?;
    let new = b.element_ref(&clause.by)?;
    if old == new {
        return Err(KipError::new(
            anda_kip::KipErrorCode::EvidenceCorrectionConflict,
            "an Evidence record cannot correct itself",
        ));
    }

    // Spec §70: wrong Evidence is corrected, never rewritten. The original
    // observation stays exactly as observed, because what a source said is a
    // historical fact even when it was wrong.
    let old_row = evidence_mut(tx, old).await?;
    old_row.status = "corrected".to_string();
    if !old_row.corrected_by.contains(&new.to_string()) {
        old_row.corrected_by.push(new.to_string());
    }
    tx.mark_changed(old, "correct");

    let new_row = evidence_mut(tx, new).await?;
    if !new_row.corrects.contains(&old.to_string()) {
        new_row.corrects.push(old.to_string());
        tx.mark_changed(new, "correct");
    }
    Ok(())
}

async fn transition(
    tx: &mut Transaction,
    clause: &TransitionActivity,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    let b = bindings(tx, request, operation);
    let id = b.element_ref(&clause.target)?;
    let to = b.scalar_str(&clause.to, "TRANSITION ACTIVITY ... TO")?;
    let expect_state = clause
        .expect_state
        .as_ref()
        .map(|s| b.scalar_str(s, "EXPECT STATE"))
        .transpose()?;
    let set_fields = clause
        .set_fields
        .as_ref()
        .map(|f| assignments_to_json(&b, f, None))
        .transpose()?
        .unwrap_or_default();
    let at = tx.cx.at.clone();

    let element = tx.load(id).await?;
    let Element::Activity(row) = element else {
        return Err(KipError::structural_reference_invalid(format!(
            "{id} is not an Activity"
        )));
    };
    if let Some(expected) = expect_state
        && row.status != expected
    {
        return Err(KipError::precondition_failed(format!(
            "{id} is {:?}, not the expected {expected:?}",
            row.status
        )));
    }
    // Spec §93: a terminal Activity's provenance topology is immutable. Once
    // it has ended, what it consumed and produced is a historical record.
    if is_terminal(&row.status) {
        return Err(KipError::activity_terminal(format!(
            "{id} is already {:?}; a completed Activity's provenance is immutable",
            row.status
        )));
    }

    let mut fields = Fields(set_fields);
    let outputs = fields.array("outputs")?;
    if !outputs.is_empty() {
        row.output_keys = outputs.iter().map(endpoint_key).collect();
        row.outputs = outputs;
    }
    let ended = fields.timestamp("ended_at")?;
    fields.rest("Activity")?;

    row.status = to.clone();
    if is_terminal(&to) {
        // Terminal outputs freeze with the end time, so a transition that
        // forgot to give one still records when the freeze happened.
        row.ended_at = if ended.is_empty() { at } else { ended };
    } else if !ended.is_empty() {
        row.ended_at = ended;
    }
    tx.mark_changed(id, "transition");
    Ok(())
}

async fn set_retention(
    tx: &mut Transaction,
    clause: &SetRetention,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    if clause.where_clauses.is_some() {
        return Err(unsupported_selection("SET RETENTION"));
    }
    let b = bindings(tx, request, operation);
    let id = b.element_ref(&clause.target)?;
    let values = assignments_to_json(&b, &clause.values, None)?;
    if let Some(expected) = &clause.expect_version {
        let expected = b.scalar_u64(expected, "EXPECT VERSION")?;
        tx.expect_version(id, expected).await?;
    }

    // Spec §19: retention is storage lifecycle. `expires_at` here is when the
    // *record* stops being retained, never when the claim stops applying —
    // that is `valid_time.until`, on an Assertion, and nothing here touches it.
    let retention = Json::Object(values);
    let expires = expires_at(&retention)?;
    let element = tx.load(id).await?;
    let (current, current_expires) = retention_mut(element);
    if *current == retention {
        return Ok(());
    }
    *current = retention;
    *current_expires = expires;
    tx.mark_changed(id, "set_retention");
    Ok(())
}

async fn remove(
    tx: &mut Transaction,
    clause: &RemovalStatement,
    to: &str,
    op: &'static str,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    if clause.where_clauses.is_some() {
        return Err(unsupported_selection("ARCHIVE / TOMBSTONE"));
    }
    let b = bindings(tx, request, operation);
    let id = b.element_ref(&clause.target)?;
    let expect_state = clause
        .expect_state
        .as_ref()
        .map(|s| b.scalar_str(s, "EXPECT STATE"))
        .transpose()?;
    if let Some(expected) = expect_state {
        tx.expect_state(id, &expected).await?;
    }

    // Neither archive nor tombstone erases anything: references keep resolving
    // (§93.33), which is what stops a removal from silently breaking every
    // Assertion that cited the element.
    let element = tx.load(id).await?;
    if element.state() == to {
        return Ok(());
    }
    set_state(element, to);
    tx.mark_changed(id, op);
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn unsupported_selection(what: &str) -> KipError {
    KipError::unsupported_capability(format!(
        "{what} with a WHERE block needs the KQL solver, which is not wired into the mutation \
         path yet; name the target directly instead"
    ))
}

fn is_terminal(status: &str) -> bool {
    matches!(status, "completed" | "failed" | "cancelled" | "aborted")
}

fn require_text(fields: &mut Fields, name: &str, clause: &str) -> Result<String, KipError> {
    let value = fields.text(name)?;
    if value.is_empty() {
        return Err(KipError::constraint_violation(format!(
            "{clause} requires `{name}`"
        )));
    }
    Ok(value)
}

fn require_reference(fields: &mut Fields, name: &str, clause: &str) -> Result<ElementId, KipError> {
    let value = fields.json(name);
    let text = match &value {
        Json::String(text) => text.clone(),
        Json::Object(map) => map
            .get("id")
            .and_then(Json::as_str)
            .map(str::to_string)
            .unwrap_or_default(),
        _ => String::new(),
    };
    if text.is_empty() {
        return Err(KipError::constraint_violation(format!(
            "{clause} requires `{name}` to reference an element"
        )));
    }
    text.parse()
}

fn split_payload(payload: Json) -> Result<(String, Json, String), KipError> {
    match payload {
        Json::Null => Ok((String::new(), Json::Null, String::new())),
        Json::Object(map) if map.contains_key("content_ref") => {
            let content_ref = map
                .get("content_ref")
                .and_then(Json::as_str)
                .unwrap_or_default()
                .to_string();
            Ok(("external".to_string(), Json::Null, content_ref))
        }
        other => Ok(("inline".to_string(), other, String::new())),
    }
}

fn evidence_id(value: &Json) -> Option<String> {
    match value {
        Json::String(text) => Some(text.clone()),
        Json::Object(map) => map
            .get("evidence_id")
            .or_else(|| map.get("id"))
            .and_then(Json::as_str)
            .map(str::to_string),
        _ => None,
    }
}

fn endpoint_key(value: &Json) -> String {
    Endpoint::from_json(value)
        .map(|endpoint| endpoint.key())
        .unwrap_or_default()
}

/// The Core structural fields one element kind owns (Spec §8.2).
///
/// A Profile may not redefine these: they are how the protocol assembles its
/// own records, and a Profile field named `evidence` would otherwise change
/// what an Assertion cites.
fn core_fields(kind: ElementKind) -> &'static [&'static str] {
    match kind {
        ElementKind::Assertion => &["evidence", "context"],
        ElementKind::Evidence => &["source", "generated_by"],
        ElementKind::Activity => &["inputs", "outputs", "associated_actors"],
        // A Concept's and a Proposition's topology is entirely Profile-defined.
        ElementKind::Concept | ElementKind::Proposition => &[],
    }
}

/// Reads an element id out of a reference value.
fn reference_id(value: &Json) -> String {
    match value {
        Json::String(text) => text.clone(),
        Json::Object(map) => map
            .get("id")
            .and_then(Json::as_str)
            .unwrap_or_default()
            .to_string(),
        _ => String::new(),
    }
}

fn valid_time_part(valid_time: &Json, part: &str) -> Result<String, KipError> {
    match valid_time.get(part) {
        None | Some(Json::Null) => Ok(String::new()),
        Some(Json::String(text)) => time::normalize(text, &format!("valid_time.{part}")),
        Some(other) => Err(KipError::type_mismatch(format!(
            "`valid_time.{part}` must be a timestamp, got {other}"
        ))),
    }
}

fn expires_at(retention: &Json) -> Result<String, KipError> {
    match retention.get("expires_at") {
        None | Some(Json::Null) => Ok(String::new()),
        Some(Json::String(text)) => time::normalize(text, "retention.expires_at"),
        Some(other) => Err(KipError::type_mismatch(format!(
            "`retention.expires_at` must be a timestamp, got {other}"
        ))),
    }
}

fn set_state(element: &mut Element, to: &str) {
    match element {
        Element::Concept(row) => row.state = to.to_string(),
        Element::Proposition(row) => row.state = to.to_string(),
        Element::Assertion(row) => row.state = to.to_string(),
        Element::Evidence(row) => row.state = to.to_string(),
        Element::Activity(row) => row.state = to.to_string(),
    }
}

fn retention_mut(element: &mut Element) -> (&mut Json, &mut String) {
    match element {
        Element::Concept(row) => (&mut row.retention, &mut row.expires_at),
        Element::Proposition(row) => (&mut row.retention, &mut row.expires_at),
        Element::Assertion(row) => (&mut row.retention, &mut row.expires_at),
        Element::Evidence(row) => (&mut row.retention, &mut row.expires_at),
        Element::Activity(row) => (&mut row.retention, &mut row.expires_at),
    }
}

async fn assertion_mut(tx: &mut Transaction, id: ElementId) -> Result<&mut AssertionRow, KipError> {
    match tx.load(id).await? {
        Element::Assertion(row) => Ok(row),
        _ => Err(KipError::structural_reference_invalid(format!(
            "{id} is not an Assertion"
        ))),
    }
}

async fn evidence_mut(tx: &mut Transaction, id: ElementId) -> Result<&mut EvidenceRow, KipError> {
    match tx.load(id).await? {
        Element::Evidence(row) => Ok(row),
        _ => Err(KipError::structural_reference_invalid(format!(
            "{id} is not an Evidence record"
        ))),
    }
}

/// What the Schema Environment needs to know about one endpoint.
async fn facts_for(
    store: &Store,
    tx: &mut Transaction,
    endpoint: &Endpoint,
) -> Result<EndpointFacts, KipError> {
    Ok(match endpoint {
        Endpoint::Literal(literal) => EndpointFacts::Literal {
            datatype: literal.datatype.clone(),
        },
        Endpoint::Local(id) => {
            // A staged element is the authority: within a transaction a
            // reference to something this transaction just created must see it.
            let schema_ref = match tx.staged_concept_type(*id) {
                Some(schema_ref) => Some(schema_ref),
                None if id.kind == ElementKind::Concept => {
                    store.find_concept(*id).await.ok().map(|row| row.schema_ref)
                }
                None => None,
            };
            EndpointFacts::Element {
                kind: id.kind,
                schema_ref,
            }
        }
        _ => EndpointFacts::Unresolved,
    })
}

/// Builds the persisted reference form for an element id.
pub fn element_reference(id: ElementId) -> Json {
    reference(id)
}
