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
    KipErrorCode, Map, MatchValue, MergeConcept, MutationClause, RecordCreate, RemovalStatement,
    RetractAssertion, SetRetention, SupersedeAssertion, SymbolRef as AstSymbolRef,
    TransitionActivity, UpdateAction, UpdateStatement,
};
use std::collections::BTreeMap;

use super::select::{self, Targets};
use super::update;
use super::value::{Bindings, assignments_to_json, reference, structural_value};
use crate::governance::Permission;
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
        MutationClause::Update(c) => update_elements(store, tx, c, request, operation).await,
        MutationClause::RetractAssertion(c) => retract(store, tx, c, request, operation).await,
        MutationClause::SupersedeAssertion(c) => supersede(tx, c, request, operation).await,
        MutationClause::CorrectEvidence(c) => correct_evidence(tx, c, request, operation).await,
        MutationClause::TransitionActivity(c) => transition(tx, c, request, operation).await,
        MutationClause::SetRetention(c) => set_retention(store, tx, c, request, operation).await,
        MutationClause::Archive(c) => {
            remove(
                store,
                tx,
                c,
                state::ARCHIVED,
                "archive",
                Permission::Archive,
                request,
                operation,
            )
            .await
        }
        MutationClause::Tombstone(c) => {
            remove(
                store,
                tx,
                c,
                state::TOMBSTONED,
                "tombstone",
                Permission::Tombstone,
                request,
                operation,
            )
            .await
        }
        MutationClause::MergeConcept(c) => merge_concept(store, tx, c, request, operation).await,
        MutationClause::Purge(c) => purge(store, tx, c, request, operation).await,
    }
}

/// The substitution scope one clause evaluates its right-hand sides in.
pub fn bindings<'a>(
    tx: &'a Transaction,
    request: Option<&'a Map<String, Json>>,
    operation: Option<&'a Map<String, Json>>,
) -> Bindings<'a> {
    Bindings {
        request,
        operation,
        handles: tx.handles(),
        env: Some(&tx.env),
    }
}

/// Whether an action changed anything.
///
/// A clause that computes the state an element is already in changes nothing:
/// no version bump, no change record, and a receipt that says `no_effect`
/// rather than claiming a transition that did not happen (§44).
#[derive(Debug, Default)]
pub struct Applied {
    /// Whether the element's stored state differs from what was there before.
    pub changed: bool,
}

/// Reads a `SymbolRef` slot — a quoted symbol or a parameter — to a local name.
pub fn symbol_of(b: &Bindings<'_>, symbol: &AstSymbolRef) -> Result<String, KipError> {
    symbol_name(b, symbol)
}

/// Resolves a Profile structural field name to its exact schema symbol.
///
/// A Concept carries no Core structural fields — every one it has is
/// Profile-defined (§8.2) — so this is the whole resolution for the `UPDATE`
/// path.
pub fn resolve_structural_field(
    tx: &Transaction,
    b: &Bindings<'_>,
    field: &AstSymbolRef,
) -> Result<String, KipError> {
    let name = symbol_name(b, field)?;
    if CORE_STRUCTURAL_FIELDS.contains(&name.as_str()) {
        return Err(KipError::structural_reference_invalid(format!(
            "`{name}` is a Core structural field of a record, not a Concept topology field; a \
             Concept's structural fields are Profile-defined"
        )));
    }
    Ok(tx
        .env
        .resolve_symbol(
            crate::schema::SymbolKind::StructuralField,
            &name,
            Intent::Write,
        )?
        .to_string())
}

/// Every Core structural field, across the record kinds that own one.
const CORE_STRUCTURAL_FIELDS: &[&str] = &[
    "evidence",
    "context",
    "source",
    "generated_by",
    "inputs",
    "outputs",
    "associated_actors",
];

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
    apply_facets(tx, b, assignments, carrier, None).await
}

/// Builds the Facets map, optionally letting each member read the element being
/// updated.
///
/// `view` is what makes `MUL(?m.facets["MnemonicState"].memory_strength, 0.9)`
/// mean anything: an update expression reads the target's own current value and
/// nothing else (§52.4).
pub async fn apply_facets(
    tx: &Transaction,
    b: &Bindings<'_>,
    assignments: &[anda_kip::FacetAssignment],
    carrier: ElementKind,
    view: Option<&Json>,
) -> Result<Map<String, Json>, KipError> {
    let mut facets = Map::new();
    for assignment in assignments {
        let name = symbol_name(b, &assignment.facet)?;
        let symbol =
            tx.env
                .resolve_symbol(crate::schema::SymbolKind::Facet, &name, Intent::Write)?;
        let members = assignments_to_json(b, &assignment.values, view)?;
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
        let value = structural_value(b.value(&edge.value, None)?);
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
    require_retention_authority(tx, &retention)?;
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
        ..Default::default()
    };
    let element = Element::Concept(Box::new(row));
    tx.authorize_created(&element, Permission::Create)?;
    tx.stage_new(id, element, "create");
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
    require_retention_authority(tx, &retention)?;

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
    // §17, §18: which epistemic-mutation permission this needs depends on whom
    // the claim is attributed to, and that is only knowable here. `assert` is
    // the floor for writing any commitment; recording somebody else's claim or
    // speaking as an actor each add their own on top of it.
    if let Element::Assertion(row) = &row {
        tx.authorize_created(&row_element(row), Permission::Assert)?;
        let extra = attribution_permission(tx, &row.asserted_by_key);
        if extra != Permission::Assert {
            tx.authorize_created(&row_element(row), extra)?;
        }
    } else {
        tx.authorize_created(&row, Permission::Create)?;
    }
    tx.stage_new(id, row, "create");
    Ok(())
}

/// Wraps one Assertion row back into an [`Element`] for authorization.
fn row_element(row: &AssertionRow) -> Element {
    Element::Assertion(Box::new(row.clone()))
}

/// Which epistemic-mutation permission a new Assertion needs, beyond `assert`.
///
/// The three cases §17 keeps apart, decided by what Governance says about the
/// writer rather than by what the command claims:
///
/// ```text
/// bound as this actor          assert                one's own commitment
/// bound as representing it     assert_as_actor       exercising its authority
/// not bound to it at all       record_attributed_assertion   "X said P"
/// ```
///
/// The third is not impersonation and must stay ordinary: a Formation Agent
/// that observed "Alice: I prefer dark mode" has to be able to store it as
/// Alice's stated claim without thereby being able to act as Alice.
fn attribution_permission(tx: &Transaction, actor_key: &str) -> Permission {
    if actor_key.is_empty() {
        return Permission::Assert;
    }
    match tx.authority.binding_class(actor_key) {
        None => Permission::RecordAttributedAssertion,
        Some(class)
            if class == crate::governance::rows::binding_class::SELF
                || class == crate::governance::rows::binding_class::SERVICE_IDENTITY =>
        {
            Permission::Assert
        }
        Some(_) => Permission::AssertAsActor,
    }
}

/// `PURGE` — physical erasure (§170–§177).
///
/// The one clause that runs outside the transaction it was planned in. Every
/// other mutation stages a row and commits once; a purge destroys the version
/// log as well, and doing that inside a transaction that might still abort
/// would mean a rolled-back statement had already erased history.
async fn purge(
    store: &Store,
    tx: &mut Transaction,
    clause: &anda_kip::PurgeStatement,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    let (targets, policy) = {
        let b = bindings(tx, request, operation);
        let policy = clause
            .reference_policy
            .as_ref()
            .map(|scalar| b.scalar_str(scalar, "REFERENCE POLICY"))
            .transpose()?;
        let targets = select::targets(
            store,
            tx,
            "PURGE",
            Permission::Purge,
            &clause.target,
            clause.where_clauses.as_ref(),
            clause.limit.as_ref(),
            &b,
        )
        .await?;
        (
            targets,
            crate::governance::purge::ReferencePolicy::parse(policy.as_deref())?,
        )
    };

    let ids = targets.authorized(tx).await?;
    if tx.dry_run {
        // A preview must compute the effect without performing it, and there is
        // no such thing as a reversible erasure to perform and undo.
        for id in &ids {
            tx.warn(format!(
                "PURGE would erase {id} and every recorded version of it"
            ));
        }
        return Ok(());
    }
    for id in ids {
        let report = crate::governance::purge::purge(
            store,
            &tx.cx.space,
            id,
            policy,
            &tx.authority,
            &tx.auth,
        )
        .await?;
        tx.warn(format!(
            "purged {id}: {} historical version(s) destroyed",
            report.versions_destroyed
        ));
    }
    Ok(())
}

/// Refuses a retention block written by a caller who may not set one.
///
/// `SET RETENTION` asks for `manage_retention`; `SET FIELDS {retention: …}`
/// writes the same state and must ask for the same thing, or the clause that
/// checks is the one nobody uses.
fn require_retention_authority(tx: &Transaction, retention: &Json) -> Result<(), KipError> {
    if retention.is_null() {
        return Ok(());
    }
    tx.require(Permission::ManageRetention)?;
    require_legal_hold_authority(tx, retention)
}

/// Refuses a legal hold written by a caller who may not place one.
///
/// §163 names this attack by its shape: a cognitive writer must not be able to
/// evade deletion by setting `legal_hold = true`. Placing a hold blocks erasure
/// for everyone, so it is its own permission rather than part of retention
/// management.
fn require_legal_hold_authority(tx: &Transaction, retention: &Json) -> Result<(), KipError> {
    let held = retention
        .get("legal_hold")
        .and_then(Json::as_bool)
        .unwrap_or(false);
    if !held {
        return Ok(());
    }
    tx.require(Permission::LegalHold)
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

    // §11.3: a new write canonicalizes a merged reference to the surviving
    // Concept. Without this a merge would be decorative — every later claim
    // about the merged-away Concept would accumulate on the identity the merge
    // said was the same one, and the two would never meet again.
    let subject = canonicalize(tx, subject).await?;
    let object = canonicalize(tx, object).await?;

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
    let element = Element::Proposition(Box::new(row));
    tx.authorize_created(&element, Permission::Create)?;
    tx.stage_new(id, element, "create");
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
            let element = Element::Concept(Box::new(row));
            tx.authorize_created(&element, Permission::Create)?;
            tx.stage_new(id, element, "create");
            id
        }
    };
    tx.bind_existing(&clause.handle, id)?;
    // An upsert that resolved to an existing Concept is changing it, and the
    // caller may hold `create` without holding `update` — which is exactly the
    // case an upsert makes hard to see from the command alone.
    if existing.is_some() {
        tx.authorize_element(id, Permission::Update).await?;
    }

    apply_concept_assignments(tx, clause, id, request, operation).await
}

/// Applies an `UPSERT CONCEPT`'s mutable state.
///
/// The clause carries its actions as separate optional members rather than as
/// an ordered list, so the engine picks the order — and then runs them through
/// the same appliers `UPDATE` uses. Two code paths for "write mutable Concept
/// state" is how `UNSET FACET` came to be accepted, parsed, and silently
/// dropped: a caller cannot tell a mutation that did nothing from one that was
/// never implemented.
async fn apply_concept_assignments(
    tx: &mut Transaction,
    clause: &ConceptUpsert,
    id: ElementId,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    let mut actions: Vec<UpdateAction> = Vec::new();
    if let Some(fields) = &clause.set_fields {
        actions.push(UpdateAction::SetFields(fields.clone()));
    }
    if let Some(attributes) = &clause.set_attributes {
        actions.push(UpdateAction::SetAttributes(attributes.clone()));
    }
    if let Some(unset) = &clause.unset_attributes {
        actions.push(UpdateAction::UnsetAttributes(unset.clone()));
    }
    for facet in &clause.set_facets {
        actions.push(UpdateAction::SetFacet(facet.clone()));
    }
    for facet in &clause.unset_facets {
        actions.push(UpdateAction::UnsetFacet(facet.clone()));
    }
    if let Some(edges) = &clause.set_structural {
        actions.push(UpdateAction::SetStructural(edges.clone()));
    }
    if let Some(removals) = &clause.unset_structural {
        actions.push(UpdateAction::UnsetStructural(removals.clone()));
    }

    // An UPSERT has no update expressions — the parser rejects `?var` reads
    // outside UPDATE — but the appliers still take the view, so the same
    // function serves both.
    let view = crate::view::render(tx.load(id).await?);
    let mut changed = false;
    for action in &actions {
        changed |= update::apply_action(tx, id, action, &view, request, operation)
            .await?
            .changed;
    }

    // A no-effect final state changes nothing: no version bump, no change
    // record, no receipt claiming a transition that did not happen (§44).
    if changed {
        tx.mark_changed(id, "update");
    }
    Ok(())
}

/// `UPDATE` — mutable state on already-existing elements.
///
/// UPDATE never creates (§52.4): a selection block that matches nothing leaves
/// the transaction with nothing to do, which is a `no_effect`, not an error and
/// certainly not an insert.
async fn update_elements(
    store: &Store,
    tx: &mut Transaction,
    clause: &UpdateStatement,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    let targets = {
        let b = bindings(tx, request, operation);
        select::targets(
            store,
            tx,
            "UPDATE",
            Permission::Update,
            &clause.target,
            clause.where_clauses.as_ref(),
            clause.limit.as_ref(),
            &b,
        )
        .await?
    };

    for id in targets.authorized(tx).await? {
        if let Some(expected) = &clause.expect_version {
            let expected = {
                let b = bindings(tx, request, operation);
                b.scalar_u64(expected, "EXPECT VERSION")?
            };
            tx.expect_version(id, expected).await?;
        }

        // Every action of one UPDATE reads the element as it was when the
        // statement began: two actions on the same Facet member must not
        // compound, or the second would silently operate on what the first
        // just wrote for reasons the author cannot see in the text.
        let view = crate::view::render(tx.load(id).await?);
        let mut changed = false;
        for action in &clause.actions {
            changed |= update::apply_action(tx, id, action, &view, request, operation)
                .await?
                .changed;
        }
        if changed {
            tx.mark_changed(id, "update");
        }
    }
    Ok(())
}

async fn retract(
    store: &Store,
    tx: &mut Transaction,
    clause: &RetractAssertion,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    let targets = {
        let b = bindings(tx, request, operation);
        select::targets(
            store,
            tx,
            "RETRACT ASSERTION",
            Permission::RetractOwn,
            &clause.target,
            clause.where_clauses.as_ref(),
            clause.limit.as_ref(),
            &b,
        )
        .await?
    };
    let expect_state = {
        let b = bindings(tx, request, operation);
        clause
            .expect_state
            .as_ref()
            .map(|s| b.scalar_str(s, "EXPECT STATE"))
            .transpose()?
    };
    let at = tx.cx.at.clone();

    for id in targets.authorized(tx).await? {
        if let Some(expected) = &expect_state {
            tx.expect_assertion_status(id, expected).await?;
        }
        require_representation(tx, id, "RETRACT ASSERTION").await?;
        let row = assertion_mut(tx, id).await?;
        // Spec §41.1: retraction is not deletion. The Assertion goes on
        // existing, so the record of what was once believed — and by whom —
        // survives.
        if row.status == "retracted" {
            continue;
        }
        row.status = "retracted".to_string();
        row.retracted_at = at.clone();
        tx.mark_changed(id, "retract");
    }
    Ok(())
}

/// Refuses to record a withdrawal the caller has no standing to make (§68).
///
/// Retraction and supersession both say something about the *source*: that it
/// took its claim back, or replaced it. A moderator who merely wants the claim
/// out of recall has `ARCHIVE` and `TOMBSTONE`, which say what they actually
/// mean. Letting administrative dislike write itself down as the source's own
/// withdrawal would make the epistemic record report an event that never
/// happened — and that record is the entire product of this engine.
async fn require_representation(
    tx: &mut Transaction,
    id: ElementId,
    what: &str,
) -> Result<(), KipError> {
    let row = assertion_mut(tx, id).await?.clone();
    if tx.may_represent_assertion(&row) {
        return Ok(());
    }
    Err(KipError::retraction_not_authorized(format!(
        "{what} records that the source withdrew this claim, and this Principal neither wrote \
         {id} nor holds an ActorBinding representing {}. ARCHIVE or TOMBSTONE removes it from \
         recall without claiming a withdrawal that did not happen",
        if row.asserted_by_key.is_empty() {
            "its author"
        } else {
            row.asserted_by_key.as_str()
        }
    )))
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
    tx.authorize_element(old, Permission::SupersedeOwn).await?;
    tx.authorize_element(new, Permission::SupersedeOwn).await?;
    require_representation(tx, old, "SUPERSEDE ASSERTION").await?;

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

    tx.authorize_element(old, Permission::Maintain).await?;
    tx.authorize_element(new, Permission::Maintain).await?;

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
    let Element::Activity(_) = element else {
        return Err(KipError::structural_reference_invalid(format!(
            "{id} is not an Activity"
        )));
    };
    tx.authorize_element(id, Permission::Update).await?;
    let Element::Activity(row) = tx.load(id).await? else {
        unreachable!("just checked");
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
    store: &Store,
    tx: &mut Transaction,
    clause: &SetRetention,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    let (targets, values, expected) = {
        let b = bindings(tx, request, operation);
        let targets = select::targets(
            store,
            tx,
            "SET RETENTION",
            Permission::ManageRetention,
            &clause.target,
            clause.where_clauses.as_ref(),
            clause.limit.as_ref(),
            &b,
        )
        .await?;
        let values = assignments_to_json(&b, &clause.values, None)?;
        let expected = clause
            .expect_version
            .as_ref()
            .map(|scalar| b.scalar_u64(scalar, "EXPECT VERSION"))
            .transpose()?;
        (targets, values, expected)
    };

    // Spec §19: retention is storage lifecycle. `expires_at` here is when the
    // *record* stops being retained, never when the claim stops applying —
    // that is `valid_time.until`, on an Assertion, and nothing here touches it.
    let retention = Json::Object(values);
    require_legal_hold_authority(tx, &retention)?;
    let expires = expires_at(&retention)?;
    for id in targets.authorized(tx).await? {
        if let Some(expected) = expected {
            tx.expect_version(id, expected).await?;
        }
        let element = tx.load(id).await?;
        let (current, current_expires) = retention_mut(element);
        if *current == retention {
            continue;
        }
        *current = retention.clone();
        *current_expires = expires.clone();
        tx.mark_changed(id, "set_retention");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn remove(
    store: &Store,
    tx: &mut Transaction,
    clause: &RemovalStatement,
    to: &str,
    op: &'static str,
    permission: Permission,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    let (targets, expect_state) = {
        let b = bindings(tx, request, operation);
        let targets = select::targets(
            store,
            tx,
            op,
            permission,
            &clause.target,
            clause.where_clauses.as_ref(),
            clause.limit.as_ref(),
            &b,
        )
        .await?;
        let expect_state = clause
            .expect_state
            .as_ref()
            .map(|s| b.scalar_str(s, "EXPECT STATE"))
            .transpose()?;
        (targets, expect_state)
    };

    for id in targets.authorized(tx).await? {
        if let Some(expected) = &expect_state {
            tx.expect_state(id, expected).await?;
        }

        // Neither archive nor tombstone erases anything: references keep
        // resolving (§93.33), which is what stops a removal from silently
        // breaking every Assertion that cited the element.
        let element = tx.load(id).await?;
        if element.state() == to {
            continue;
        }
        set_state(element, to);
        tx.mark_changed(id, op);
    }
    Ok(())
}

/// `MERGE CONCEPT ?source INTO ?target` — non-destructive identity
/// consolidation (§11.1).
///
/// Nothing is copied and nothing is deleted. The source keeps its id, its
/// attributes and its history; it gains a forwarding pointer and leaves
/// ordinary recall. That is the whole merge, and the restraint is the point:
/// copying the source's state onto the target would invent claims nobody made,
/// and rewriting the references would erase what the memory used to say.
async fn merge_concept(
    store: &Store,
    tx: &mut Transaction,
    clause: &MergeConcept,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    let (source, target, expected) = {
        let b = bindings(tx, request, operation);
        // MERGE takes no LIMIT: its operands are named, and the block only
        // guards them (§52.7). Each side must therefore resolve to exactly one
        // Concept — a pattern that binds several is selecting an identity by
        // description, which is what merge exists to stop people doing.
        let source = one_operand(
            store,
            tx,
            "MERGE CONCEPT source",
            Permission::MergeIdentity,
            &clause.source,
            clause.where_clauses.as_ref(),
            &b,
        )
        .await?;
        let target = one_operand(
            store,
            tx,
            "MERGE CONCEPT target",
            Permission::MergeIdentity,
            &clause.into,
            clause.where_clauses.as_ref(),
            &b,
        )
        .await?;
        let expected = clause
            .expect_version
            .as_ref()
            .map(|scalar| b.scalar_u64(scalar, "EXPECT VERSION"))
            .transpose()?;
        (source, target, expected)
    };
    let source = match source {
        Some(targets) => targets.authorized(tx).await?.into_iter().next(),
        None => None,
    };
    let target = match target {
        Some(targets) => targets.authorized(tx).await?.into_iter().next(),
        None => None,
    };
    let (Some(source), Some(target)) = (source, target) else {
        // The guard block matched nothing: no merge, no error.
        return Ok(());
    };

    if source == target {
        return Err(KipError::new(
            KipErrorCode::IdentityMergeConflict,
            "a Concept cannot be merged into itself",
        ));
    }
    if source.kind != ElementKind::Concept || target.kind != ElementKind::Concept {
        return Err(KipError::structural_reference_invalid(
            "MERGE CONCEPT consolidates Concepts; other element kinds have no merged identity",
        ));
    }
    if let Some(expected) = expected {
        tx.expect_version(source, expected).await?;
    }

    // §11.1: canonical resolution follows `merged_into` to its fixpoint, so a
    // cycle would make that walk run forever. The check is on the target's
    // chain, before anything is written.
    let chain = canonical_chain(tx, target).await?;
    if chain.contains(&source) {
        return Err(KipError::new(
            KipErrorCode::IdentityMergeConflict,
            format!(
                "{target} already resolves back to {source}; merging would make canonical \
                 resolution cycle"
            ),
        ));
    }

    let element = tx.load(source).await?;
    let Element::Concept(row) = element else {
        return Err(KipError::structural_reference_invalid(format!(
            "{source} is not a Concept"
        )));
    };
    if row.merged_into == target.to_string() {
        return Ok(());
    }
    if !row.merged_into.is_empty() {
        return Err(KipError::new(
            KipErrorCode::IdentityMergeConflict,
            format!(
                "{source} is already merged into {}; re-pointing it would rewrite an identity \
                 decision that other writes have since canonicalized through",
                row.merged_into
            ),
        ));
    }
    row.merged_into = target.to_string();
    // Merged, not archived: the two say different things. Archived means "out
    // of ordinary recall"; merged additionally means "this identity is now
    // that one", which is what a reader needs in order to follow the pointer.
    row.state = state::MERGED.to_string();
    tx.mark_changed(source, "merge");
    Ok(())
}

/// Resolves one operand of a statement that acts on exactly one element.
async fn one_operand(
    store: &Store,
    tx: &Transaction,
    what: &str,
    permission: Permission,
    target: &anda_kip::ElementRef,
    where_clauses: Option<&Vec<anda_kip::WhereClause>>,
    b: &Bindings<'_>,
) -> Result<Option<Targets>, KipError> {
    let targets: Targets =
        select::targets(store, tx, what, permission, target, where_clauses, None, b).await?;
    match targets.len() {
        0 => Ok(None),
        1 => Ok(Some(targets)),
        n => Err(KipError::new(
            KipErrorCode::IdentitySelectorRequired,
            format!("the {what} block binds {n} elements; it must name exactly one"),
        )),
    }
}

/// Follows a merged Concept's forwarding pointer to the identity that survived.
///
/// Only for endpoints of a *new* write (§11.3). A historical Proposition keeps
/// referring to what it referred to (§11.2): rewriting those would erase what
/// the memory used to say, which is the whole reason merge is non-destructive.
async fn canonicalize(tx: &mut Transaction, endpoint: Endpoint) -> Result<Endpoint, KipError> {
    let Endpoint::Local(id) = endpoint else {
        return Ok(endpoint);
    };
    if id.kind != ElementKind::Concept {
        return Ok(endpoint);
    }
    let chain = canonical_chain(tx, id).await?;
    Ok(Endpoint::Local(*chain.last().unwrap_or(&id)))
}

/// The `merged_into` chain above one Concept, ending at its canonical id.
///
/// Bounded independently of the cycle check that maintains it: a chain longer
/// than this is corrupt state, and walking it forever would turn corruption
/// into a hang.
async fn canonical_chain(
    tx: &mut Transaction,
    from: ElementId,
) -> Result<Vec<ElementId>, KipError> {
    const MAX_HOPS: usize = 64;
    let mut chain = vec![from];
    let mut cursor = from;
    for _ in 0..MAX_HOPS {
        let next = match tx.load(cursor).await {
            Ok(Element::Concept(row)) if !row.merged_into.is_empty() => {
                row.merged_into.parse::<ElementId>()?
            }
            _ => return Ok(chain),
        };
        if chain.contains(&next) {
            return Ok(chain);
        }
        chain.push(next);
        cursor = next;
    }
    Err(KipError::internal_error(format!(
        "the merged_into chain above {from} is longer than {MAX_HOPS} hops"
    )))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
