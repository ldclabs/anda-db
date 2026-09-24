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
//! **An Assertion's epistemic payload is immutable** (§58.1). There is no clause
//! that edits stance, mode, confidence or evidence — correcting a claim is
//! `CREATE ASSERTION` plus `TRANSITION ... TO "superseded"`, which is why the
//! parser rejects `UPDATE ?a SET FIELDS {confidence: ...}` before an engine
//! ever sees it.
//!
//! **Retraction is not deletion** (§57.3, §60). A retracted Assertion keeps
//! existing; only its lifecycle status moves, so the historical record of what
//! was once believed survives. Every lifecycle move is the one `TRANSITION`
//! statement (§52.5), dispatched on the state it names and the kind it lands
//! on.

use anda_kip::{
    ASSERTION_MODES, ChangeOp, ConceptCreate, ConceptUpsert, EVIDENCE_ROLES, ElementKind,
    EnsureProposition, ExpectVersion, Json, KipError, KipErrorCode, Map, MatchValue, MergeConcept,
    MutationClause, RecordCreate, STANCES, SetRetention, SymbolRef as AstSymbolRef, Transition,
    UpdateAction, UpdateStatement, VersionPlane, transition_state,
};
use std::collections::BTreeMap;

use super::select::{self, Targets};
use super::update;
use super::value::{Bindings, assignments_to_json, structural_value};
use crate::governance::Permission;
use crate::id::ElementId;
use crate::schema::{EndpointFacts, Intent, SymbolKind, same_lineage};
use crate::store::planes::{self, PlaneKey};
use crate::store::rows::*;
use crate::store::{Element, Store};
use crate::term::{Endpoint, tuple_key};
use crate::time;
use crate::tx::{Guard, Transaction};

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

/// Seed all declared Concept types before validating cyclic structural edges.
pub(crate) fn declare_concept_type(
    tx: &mut Transaction,
    clause: &MutationClause,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    if let MutationClause::CreateConcept(c) = clause
        && let Some(symbol) = &c.r#type
    {
        let b = bindings(tx, request, operation);
        let id = b.handle(&c.handle)?;
        let name = symbol_name(&b, symbol)?;
        let symbol =
            tx.env
                .resolve_symbol(crate::schema::SymbolKind::ConceptType, &name, Intent::Write)?;
        tx.declared_types.insert(id, symbol.to_string());
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
/// 2  CREATE records       finalize Evidence, Assertions and Activities
/// 3  remaining mutations  read frozen block-output views
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
        MutationClause::CreateEvidence(_)
        | MutationClause::CreateAssertion(_)
        | MutationClause::CreateActivity(_) => 2,
        _ => 3,
    }
}

/// How many planning passes [`plan_pass`] distributes clauses over.
pub const PLAN_PASSES: u8 = 4;

/// Interprets one clause against a plan with every handle already bound.
pub async fn apply(
    store: &Store,
    tx: &mut Transaction,
    clause: &MutationClause,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    match clause {
        MutationClause::CreateConcept(c) => create_concept(store, tx, c, request, operation).await,
        MutationClause::UpsertConcept(c) => upsert_concept(store, tx, c, request, operation).await,
        MutationClause::EnsureProposition(c) => {
            ensure_proposition(store, tx, c, request, operation).await
        }
        MutationClause::CreateEvidence(c) => {
            create_record(store, tx, c, ElementKind::Evidence, request, operation).await
        }
        MutationClause::CreateAssertion(c) => {
            create_record(store, tx, c, ElementKind::Assertion, request, operation).await
        }
        MutationClause::CreateActivity(c) => {
            create_record(store, tx, c, ElementKind::Activity, request, operation).await
        }
        // Routed before any plan runs (`kml::execute`); the parser never puts
        // it beside another clause.
        MutationClause::Define(_) => Err(KipError::invalid_syntax(
            "DEFINE is a standalone operation, never a clause of MUTATE (§20.16)",
        )),
        MutationClause::Update(c) => update_elements(store, tx, c, request, operation).await,
        MutationClause::Transition(c) => transition(store, tx, c, request, operation).await,
        MutationClause::SetRetention(c) => set_retention(store, tx, c, request, operation).await,
        MutationClause::MergeConcept(c) => merge_concept(store, tx, c, request, operation).await,
        MutationClause::Purge(c) => purge(store, tx, c, request, operation).await,
        MutationClause::PurgePayload(c) => purge_payload(store, tx, c, request, operation).await,
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
pub(super) fn symbol_name(b: &Bindings<'_>, symbol: &AstSymbolRef) -> Result<String, KipError> {
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
    /// Wraps a `SET FIELDS` map, refusing the envelope members no command
    /// writes (§6.3, §28.1).
    fn new(map: Map<String, Json>) -> Result<Self, KipError> {
        crate::kml::update::reject_protected_fields(&map)?;
        Ok(Fields(map))
    }

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
    carrier: &crate::schema::EndpointFacts,
    view: Option<&Json>,
) -> Result<Map<String, Json>, KipError> {
    let facets = resolve_facets(tx, b, assignments, view)?;
    tx.env
        .validate_facets(&facets, carrier, Intent::Write)?
        .into_result()?;
    Ok(facets)
}

/// Resolves each Facet symbol and its members, validating neither.
///
/// `UPDATE` needs this half on its own: a Facet assignment *merges* (§59), so
/// what the schema has to be shown is the merged result, not the clause. A
/// Facet with required members — `OutcomeRecord` is the first one the Cognitive
/// Memory Profile ships — would otherwise be unwritable one member at a time,
/// because every partial assignment would read as a Facet missing the members
/// the element already carries.
pub fn resolve_facets(
    tx: &Transaction,
    b: &Bindings<'_>,
    assignments: &[anda_kip::FacetAssignment],
    view: Option<&Json>,
) -> Result<Map<String, Json>, KipError> {
    let mut facets = Map::new();
    for assignment in assignments {
        let name = symbol_name(b, &assignment.facet)?;
        let symbol =
            tx.env
                .resolve_symbol(crate::schema::SymbolKind::Facet, &name, Intent::Write)?;
        let mut members = assignments_to_json(b, &assignment.values, view)?;
        crate::schema::contracts::normalize_record_refs(&symbol.to_string(), &mut members);
        facets.insert(symbol.to_string(), Json::Object(members));
    }
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
    /// Rewrites every reference onto the Concept a merge made canonical (§11.3).
    async fn canonicalize(&mut self, tx: &mut Transaction) -> Result<(), KipError> {
        for edges in self.core.values_mut() {
            for (value, _) in edges.iter_mut() {
                *value = canonicalize_reference(tx, std::mem::take(value)).await?;
            }
        }
        for value in self.profile.values_mut() {
            if let Json::Array(items) = value {
                *items = canonicalize_all(tx, std::mem::take(items)).await?;
            }
        }
        Ok(())
    }

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
    let mut claimed: BTreeMap<String, std::collections::BTreeSet<usize>> = BTreeMap::new();
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
        let field = symbol.to_string();
        let ordered = tx.env.structural_field_def(&symbol)?.ordered;
        // A declared position is honored on a create exactly as it is on an
        // update (§17.4): a Concept written with its steps out of order and
        // positions attached would otherwise land in mutation order, and the
        // author would have no way to tell.
        let index = match &edge.options {
            Some(block) => match block.get("index") {
                Some(value) => Some(read_index(&b.bound(value, None)?)?),
                None => None,
            },
            None => None,
        };
        if let Some(index) = index
            && !claimed.entry(field.clone()).or_default().insert(index)
        {
            return Err(KipError::constraint_violation(format!(
                "two references claim position {index} of `{field}` in one mutation plan; an \
                 order cannot hold both, and picking one would be the engine choosing (§17.4)"
            )));
        }
        let items = grouped.entry(field.clone()).or_default();
        update::place_reference(items, value, index, ordered, &field)?;
    }
    for (field, refs) in grouped {
        out.profile.insert(field, Json::Array(refs));
    }
    Ok(out)
}

/// Reads a structural reference's `index` option as a zero-based position.
pub(crate) fn read_index(value: &Json) -> Result<usize, KipError> {
    match value.as_u64() {
        Some(index) => Ok(index as usize),
        None => Err(KipError::type_mismatch(format!(
            "a structural reference `index` is a zero-based position, got {value}"
        ))),
    }
}

async fn create_concept(
    store: &Store,
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
    let existing = find_client_key(store, tx, ElementKind::Concept, &client_key).await?;
    let b = bindings(tx, request, operation);
    let mut fields = Fields::new(
        clause
            .set_fields
            .as_ref()
            .map(|f| assignments_to_json(&b, f, None))
            .transpose()?
            .unwrap_or_default(),
    )?;
    let key = fields.text("key")?;
    let canonical_id = fields.text("canonical_id")?;
    // §5.4, and the same gate `UPDATE ... SET FIELDS` runs: binding a
    // cross-system identity is its own authority, not a side effect of `create`.
    if !canonical_id.is_empty() {
        tx.require(Permission::BindCanonicalIdentity)?;
    }
    let aliases = fields
        .array("aliases")?
        .into_iter()
        .filter_map(|value| value.as_str().map(str::to_string))
        .collect();
    let retention = fields.json("retention");
    require_retention_authority(tx, &retention)?;
    let extra_name = fields.text("name")?;
    fields.rest("Concept")?;

    // Kind only here: the Concept's type symbol is resolved a few lines down by
    // `prepare_concept`, which re-checks these Facets against it. Naming the
    // type twice would mean resolving it twice and could disagree with itself.
    let facets = apply_facets(
        tx,
        &b,
        &clause.set_facets,
        &crate::schema::EndpointFacts::Element {
            kind: ElementKind::Concept,
            schema_ref: None,
        },
        None,
    )
    .await?;
    // A Concept has no Core structural fields; every one is Profile-defined.
    let mut structural = collect_structural(tx, &b, clause.set_structural.as_ref(), &[])?;
    // §11.3: a new write resolves references through whatever merges the Space
    // has already declared.
    structural.canonicalize(tx).await?;
    let structural = structural.profile;

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
    if client_key_retry(tx, existing, &clause.handle, &element).await? {
        return Ok(());
    }
    tx.authorize_created(&element, Permission::Create)?;
    tx.stage_new(id, element, ChangeOp::Create);
    check_structural(store, tx, id).await
}

async fn create_record(
    store: &Store,
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
    let existing = find_client_key(store, tx, kind, &client_key).await?;
    let b = bindings(tx, request, operation);
    let mut fields = Fields::new(
        clause
            .set_fields
            .as_ref()
            .map(|f| assignments_to_json(&b, f, None))
            .transpose()?
            .unwrap_or_default(),
    )?;
    // A record is not a Concept and has no type to name.
    let facets = apply_facets(
        tx,
        &b,
        &clause.set_facets,
        &crate::schema::EndpointFacts::Element {
            kind,
            schema_ref: None,
        },
        None,
    )
    .await?;
    let mut structural =
        collect_structural(tx, &b, clause.set_structural.as_ref(), core_fields(kind))?;
    let retention = fields.json("retention");
    require_retention_authority(tx, &retention)?;
    // §11.3: a new write resolves references through whatever merges the Space
    // has already declared. Doing this for tuple endpoints alone would leave a
    // merge decorative everywhere else — new Assertions would keep piling up
    // under a Concept the Space said was the same as another one.
    structural.canonicalize(tx).await?;

    let draft = Draft {
        id,
        client_key,
        facets,
        retention,
    };
    let row = match kind {
        ElementKind::Evidence => evidence_row(draft, &mut fields, &mut structural)?,
        ElementKind::Assertion => assertion_row(tx, draft, &mut fields, &mut structural).await?,
        ElementKind::Activity => activity_row(draft, &mut fields, &mut structural)?,
        other => {
            return Err(KipError::internal_error(format!(
                "{other} has no record-create form"
            )));
        }
    };
    fields.rest(&kind.to_string())?;
    // §52.1: a key that already names an element either proves this is a retry
    // — same creation, so nothing is written and the handle points at what the
    // first attempt made — or names different work, which is a conflict.
    if client_key_retry(tx, existing, &clause.handle, &row).await? {
        return Ok(());
    }
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
        // The binding this write exercised, for the Receipt's `origin`
        // (§33.2): the one covering the actor, when the caller holds one.
        if let Some(binding) = tx
            .authority
            .bindings
            .iter()
            .find(|binding| binding.actor_key == row.asserted_by_key)
        {
            tx.note_binding(format!("kip:binding:{}", binding._id));
        }
    } else {
        tx.authorize_created(&row, Permission::Create)?;
    }
    require_outcome_authority(tx, &row)?;
    tx.stage_new(id, row, ChangeOp::Create);
    check_structural(store, tx, id).await
}

/// What every record clause settles before its kind-specific row is built.
struct Draft {
    id: ElementId,
    client_key: String,
    facets: Map<String, Json>,
    retention: Json,
}

/// The Evidence row a `CREATE EVIDENCE` clause describes (§15.3).
fn evidence_row(
    draft: Draft,
    fields: &mut Fields,
    structural: &mut Structural,
) -> Result<Element, KipError> {
    let Draft {
        id,
        client_key,
        facets,
        retention,
    } = draft;
    let payload = fields.json("payload");
    let (payload_mode, payload_inline, content_ref) = split_payload(payload)?;
    let source_refs = structural.values("source");
    let evidence_class = require_text(fields, "evidence_class", "CREATE EVIDENCE")?;
    let row = EvidenceRow {
        _id: id.seq,
        evidence_class,
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
        structural: std::mem::take(&mut structural.profile),
        expires_at: expires_at(&retention)?,
        retention,
        ..Default::default()
    };
    Ok(Element::Evidence(Box::new(row)))
}

/// The Assertion row a `CREATE ASSERTION` clause describes (§13.2).
async fn assertion_row(
    tx: &mut Transaction,
    draft: Draft,
    fields: &mut Fields,
    structural: &mut Structural,
) -> Result<Element, KipError> {
    let Draft {
        id,
        client_key,
        facets,
        retention,
    } = draft;
    let proposition = require_reference(fields, "proposition", "CREATE ASSERTION")?;
    // The semantic actor is a reference like any other, and a merged one has
    // to resolve to the surviving identity or the actor's own claims split
    // across two Concepts the Space calls one (§11.3).
    let asserted_by = canonicalize_reference(tx, fields.json("asserted_by")).await?;
    // §13.3: `asserted_by` is REQUIRED. A claim whose actor cannot be resolved
    // is recorded as Evidence, not asserted — an Assertion is one actor's
    // commitment, and one with no actor commits nobody.
    let asserted_by_key = endpoint_key(&asserted_by);
    if asserted_by_key.is_empty() {
        return Err(KipError::constraint_violation(
            "CREATE ASSERTION requires `asserted_by`, the semantic actor whose commitment this \
             is (§13.3); a claim with no resolvable actor is recorded as Evidence, not asserted",
        ));
    }
    // Each citation keeps the role it was cited in: Core records that this
    // Assertion cites E *as supporting*, and never that E proves anything —
    // that judgement belongs to the Projection (§8.4).
    let evidence: Vec<Json> = structural
        .take("evidence")
        .into_iter()
        .map(|(value, options)| {
            // Stored in the wire shape §13.2 fixes — `{id, role}` — so the
            // view renders it without a rename, and one place fewer can drift
            // from the other.
            let mut citation = Map::new();
            citation.insert("id".into(), Json::String(reference_id(&value)));
            if let Some(role) = options.get("role") {
                // §20.13 fixes the Evidence roles, and a citation whose role
                // nobody can read is a citation whose meaning is lost:
                // `challenge` and `support` are the difference between
                // corroboration and dissent.
                let role = role.as_str().ok_or_else(|| {
                    KipError::type_mismatch(format!(
                        "an Evidence citation `role` must be a string, got {role}"
                    ))
                })?;
                check_registry(role, "role", EVIDENCE_ROLES)?;
                citation.insert("role".into(), Json::String(role.to_string()));
            }
            Ok(Json::Object(citation))
        })
        .collect::<Result<Vec<Json>, KipError>>()?;
    let given_context = fields.json("context_refs");
    let mut contexts = structural.values("context");
    if !given_context.is_null() {
        contexts.extend(
            given_context
                .as_array()
                .ok_or_else(|| {
                    KipError::type_mismatch("context_refs must be an array of Concept references")
                })?
                .iter()
                .cloned(),
        );
    }
    let mut context_refs = Vec::new();
    for value in contexts {
        let name = value
            .as_str()
            .or_else(|| value.get("id").and_then(Json::as_str))
            .ok_or_else(|| {
                KipError::type_mismatch("context_refs must contain Concept references")
            })?;
        let id = name.parse::<ElementId>()?;
        if id.kind != ElementKind::Concept {
            return Err(KipError::type_mismatch("context_refs must name Concepts"));
        }
        tx.load(id).await?;
        context_refs.push(canonicalize_reference(tx, serde_json::json!({"id": name})).await?);
    }
    context_refs.sort_by_key(Json::to_string);
    context_refs.dedup();
    let valid_time = fields.json("valid_time");
    if let Some(members) = valid_time.as_object()
        && members.keys().any(|k| k != "from" && k != "until")
    {
        return Err(KipError::constraint_violation(
            "valid_time has only `from` and `until` (§25.2)",
        ));
    } else if !valid_time.is_null() && !valid_time.is_object() {
        return Err(KipError::type_mismatch(
            "valid_time must be an object with `from` and `until`",
        ));
    }
    let from = time::Point::read(valid_time.get("from"), "valid_time.from")?;
    let until = time::Point::read(valid_time.get("until"), "valid_time.until")?;
    // §25.5: an interval is invalid when its earliest possible start is not
    // before its latest possible end; exact bounds need from < until.
    if let (Some(from), Some(until)) = (&from, &until)
        && let (Some(start), Some(end)) = (from.range().lo, until.range().hi)
        && start >= end
    {
        return Err(KipError::constraint_violation(
            "valid_time requires from < until",
        ));
    }
    // ASSERT lowers to CREATE ASSERTION; §55.1 defaults its omitted `at`
    // to the engine transaction time, never a simulated evaluation clock.
    let asserted_at = fields.timestamp("asserted_at")?;
    let row = AssertionRow {
        _id: id.seq,
        proposition_id: proposition.to_string(),
        asserted_by_key,
        asserted_by,
        stance: require_registry(fields, "stance", STANCES, "CREATE ASSERTION")?,
        mode: require_registry(fields, "mode", ASSERTION_MODES, "CREATE ASSERTION")?,
        confidence: read_confidence(fields)?,
        asserted_at: if asserted_at.is_empty() {
            tx.cx.at.clone()
        } else {
            asserted_at
        },
        valid_from: from.as_ref().map(time::Point::store).unwrap_or_default(),
        valid_until: until.as_ref().map(time::Point::store).unwrap_or_default(),
        evidence_ids: evidence.iter().filter_map(evidence_id).collect(),
        evidence_refs: evidence,
        context_refs,
        status: "active".to_string(),
        client_key,
        facets,
        structural: std::mem::take(&mut structural.profile),
        expires_at: expires_at(&retention)?,
        retention,
        ..Default::default()
    };
    Ok(Element::Assertion(Box::new(row)))
}

/// The Activity row a `CREATE ACTIVITY` clause describes (§16.3).
fn activity_row(
    draft: Draft,
    fields: &mut Fields,
    structural: &mut Structural,
) -> Result<Element, KipError> {
    let Draft {
        id,
        client_key,
        facets,
        retention,
    } = draft;
    let inputs = structural.values("inputs");
    let outputs = structural.values("outputs");
    let activity_class = require_text(fields, "activity_class", "CREATE ACTIVITY")?;
    let row = ActivityRow {
        _id: id.seq,
        activity_class,
        started_at: fields.timestamp("started_at")?,
        ended_at: fields.timestamp("ended_at")?,
        input_keys: inputs.iter().map(endpoint_key).collect(),
        inputs,
        output_keys: outputs.iter().map(endpoint_key).collect(),
        outputs,
        associated_actors: structural.values("associated_actors"),
        parameters_digest: fields.text("parameters_digest")?,
        status: {
            // §16, §20.13: the Activity status registry is Core's, so a word
            // outside it is refused here rather than stored — the parser only
            // sees a written literal, and a `:parameter` status is bound at
            // execution time.
            let status = fields.text("status")?;
            if status.is_empty() {
                "pending".to_string()
            } else {
                check_registry(&status, "status", anda_kip::ACTIVITY_STATUS)?;
                status
            }
        },
        client_key,
        facets,
        structural: std::mem::take(&mut structural.profile),
        expires_at: expires_at(&retention)?,
        retention,
        ..Default::default()
    };
    Ok(Element::Activity(Box::new(row)))
}

/// The Evidence class that is the consequence channel (§15.7).
pub(crate) const OUTCOME_EVIDENCE_CLASS: &str = "outcome";
/// The Activity class that links an outcome to the decision it grades (§15.7).
pub(crate) const OUTCOME_OBSERVATION_CLASS: &str = "outcome_observation";

/// Whether a new record is part of the consequence channel (§15.7, §29.8).
pub(crate) fn records_outcome(element: &Element) -> bool {
    match element {
        Element::Evidence(row) => row.evidence_class == OUTCOME_EVIDENCE_CLASS,
        Element::Activity(row) => row.activity_class == OUTCOME_OBSERVATION_CLASS,
        _ => false,
    }
}

/// Requires `record_outcome` for a write into the consequence channel (§29.8).
///
/// Writing `outcome`-class Evidence, and the observation Activity that links
/// it to a decision, is what a lifecycle verdict later grades cognition
/// against; Governance restricts it to instrumentation Principals. It is
/// asked for in addition to the permission the creation itself needs and
/// never instead of it, and it does not additionally require `derive`.
///
/// Asked against the element being created, not at Space scope: a policy that
/// grants instrumentation `record_outcome` only for a classification or a type
/// has to be able to say so, and the other reference engine authorizes the
/// same write the same way.
pub(crate) fn require_outcome_authority(
    tx: &mut Transaction,
    element: &Element,
) -> Result<(), KipError> {
    if records_outcome(element) {
        tx.authorize_created(element, Permission::RecordOutcome)?;
    }
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

/// `PURGE` — physical erasure (§60.3, §60.4).
///
/// The identity stub is staged like every other write. Destruction of the old
/// version log is deferred until commit, after every clause and every purge
/// target has passed validation; a statement that later refuses therefore
/// erases nothing.
async fn purge(
    store: &Store,
    tx: &mut Transaction,
    clause: &anda_kip::PurgeStatement,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    let (targets, policy, guards) = {
        let b = bindings(tx, request, operation);
        let policy = clause
            .reference_policy
            .as_ref()
            .map(|scalar| b.scalar_str(scalar, "REFERENCE POLICY"))
            .transpose()?;
        let targets = select::targets(
            store,
            tx,
            &select::Selection {
                what: "PURGE",
                permission: Permission::Purge,
                target: &clause.target,
                where_clauses: clause.where_clauses.as_ref(),
                limit: clause.limit.as_ref(),
            },
            &b,
        )
        .await?;
        let guards = resolve_guards(tx, &b, &clause.expect_versions)?;
        (
            targets,
            crate::governance::purge::ReferencePolicy::parse(policy.as_deref())?,
            guards,
        )
    };

    let ids = targets.authorized(tx).await?;
    for id in &ids {
        tx.expect_versions(*id, &guards).await?;
    }
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
        let report = crate::governance::purge::stage(store, tx, id, policy).await?;
        tx.warn(format!(
            "purge of {id} staged with {} identity stub(s) and {} historical version(s) scheduled for destruction",
            report.purged.len(),
            report.versions_destroyed,
        ));
    }
    Ok(())
}

/// `PURGE PAYLOAD` — Evidence bytes only (§60.6).
///
/// The data-minimization instrument: a Space can discard observed raw bytes
/// after digesting them without destroying the evidence event, its citations,
/// or its provenance role. That is why this asks for no `REFERENCE POLICY` and
/// never consults the target's referrers — the element survives, so nothing
/// can be left pointing at nothing.
async fn purge_payload(
    store: &Store,
    tx: &mut Transaction,
    clause: &anda_kip::PurgePayloadStatement,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    let (targets, guards) = {
        let b = bindings(tx, request, operation);
        let targets = select::targets(
            store,
            tx,
            &select::Selection {
                what: "PURGE PAYLOAD",
                permission: Permission::Purge,
                target: &clause.target,
                where_clauses: clause.where_clauses.as_ref(),
                limit: clause.limit.as_ref(),
            },
            &b,
        )
        .await?;
        (targets, resolve_guards(tx, &b, &clause.expect_versions)?)
    };

    let ids = targets.authorized(tx).await?;
    for id in &ids {
        tx.expect_versions(*id, &guards).await?;
    }
    if tx.dry_run {
        // A preview must compute the effect without performing it, and there
        // is no such thing as a reversible byte destruction to perform and
        // undo.
        for id in &ids {
            tx.warn(format!(
                "PURGE PAYLOAD would destroy the payload bytes of {id}, keeping the record, its \
                 digest and its citations"
            ));
        }
        return Ok(());
    }
    for id in ids {
        let report = crate::governance::purge::stage_payload(store, tx, id).await?;
        if !report.erased {
            // §60.6: purging an already-purged payload is a `no_effect`, and
            // saying so beats a silent success that reads as "erased again".
            tx.warn(format!("the payload of {id} was already purged"));
            continue;
        }
        tx.warn(format!(
            "payload purge of {id} destroyed its bytes and scrubbed {} recorded version(s); the \
             record, its digest and its citations survive",
            report.versions_scrubbed,
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
    // A creation has nothing to lift, so the "before" is the empty block. The
    // shape check runs after the permission rather than telling an unauthorized
    // caller which member it got wrong.
    check_retention(retention)?;
    require_legal_hold_authority(tx, &Json::Null, retention)
}

/// Refuses a change to an element's legal hold by a caller who may not make it.
///
/// §19.1 gives the retention hook its `legal_hold` member and §60.3 states what
/// it does: a held element may not be purged, by anyone, whatever the reference
/// policy says. §60.3 then draws the conclusion this gate implements — because a
/// hold blocks erasure for everyone, the authority to set or lift one SHOULD be
/// scoped apart from ordinary retention management.
///
/// Both directions are gated. *Placing* a hold is that authority. *Lifting* one
/// is a writer evading deletion — and lifting does not require naming the
/// member, because `SET RETENTION` replaces the block rather than patching it: a
/// hold disappears when the next block simply omits it. Gating on the transition
/// rather than on the words in the block is what closes that.
fn require_legal_hold_authority(
    tx: &Transaction,
    current: &Json,
    next: &Json,
) -> Result<(), KipError> {
    let held = |block: &Json| {
        block
            .get("legal_hold")
            .and_then(Json::as_bool)
            .unwrap_or(false)
    };
    if !held(next) && !held(current) {
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

    let guards = resolve_guards(tx, &b, &clause.expect_versions)?;
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

    // §12.3, §20.14: identity compares the predicate's lineage, so the same
    // tuple written under a later version of the package resolves to the
    // Proposition the Space already holds; the stored `predicate_ref` stays
    // the exact reference resolved now.
    let key = tuple_key(
        &tx.cx.space,
        &subject,
        &crate::schema::lineage_of(&symbol.to_string()),
        &object,
    );

    // Resolve-or-create: one Space keeps one canonical Proposition per
    // semantic tuple (§12.4), so an existing tuple is bound rather than
    // duplicated — and binding it changes nothing, because the tuple is
    // immutable (§12.5).
    let existing = match tx.staged_proposition(&key) {
        Some(id) => Some(id),
        None => store
            .find_proposition(&key)
            .await?
            .map(|row| ElementId::new(ElementKind::Proposition, row._id)),
    };
    if let Some(id) = existing {
        tx.expect_versions(id, &guards).await?;
        if let Some(handle) = &clause.handle {
            tx.bind_existing(handle, id)?;
        }
        return Ok(());
    }

    // §35.2: the bare `EXPECT VERSION 0` is the create-only guard, and it is
    // satisfied precisely because nothing was found above; a plane guard at
    // 0 says the plane has never been written, which is also true here.
    check_guards_absent(&guards, "this tuple does not exist yet")?;

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
    tx.stage_new(id, element, ChangeOp::Create);
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

    // Spec §54.2, §7.2: name-only upsert is forbidden. A name is mutable grounding
    // state that may be duplicated, so resolving identity through it would
    // merge two different Concepts that happen to share a label.
    let selector = match matcher
        .get("id")
        .map(|value| ("id", value))
        .or_else(|| matcher.get("key").map(|value| ("key", value)))
    {
        Some(selector) => selector,
        // Its own code, because the two refusals mean different things to a
        // caller: `name` was offered as identity and is not one, versus no
        // identity was offered at all. `ts/kip-do` answers the same two.
        None if matcher.contains_key("name") => {
            return Err(KipError::new(
                KipErrorCode::NameIdentityForbidden,
                "a Concept name is mutable grounding state and several Concepts may share one, \
                 so it cannot identify an upsert target; use {key: …} or {id: …}",
            ));
        }
        None => {
            return Err(KipError::identity_selector_required(
                "UPSERT CONCEPT resolves identity through `id` or `key` only",
            ));
        }
    };

    let selector_value = match_text(&b, selector.1, selector.0)?;

    // MATCH is an `object_pattern` — the same production a KQL Concept pattern
    // uses — so `type` here is what it is there: schema-resolution sugar for an
    // exact `schema_ref` (§43.1). It carries identity weight in both halves of
    // an upsert. On a resolve it is part of the address, because key uniqueness
    // is scoped to `(space_id, schema_ref, key)` (§7.3). On a create it is the
    // only place the new Concept's type can come from, and `schema_ref` is
    // fixed at creation — so a Concept minted without one stays untyped
    // forever, which §10.1 does not admit as a state a Concept can be in.
    let declared_type = matcher
        .get("type")
        .map(|value| match_text(&b, value, "type"))
        .transpose()?
        .map(|name| {
            tx.env
                .resolve_symbol(SymbolKind::ConceptType, &name, Intent::Write)
        })
        .transpose()?
        .map(|symbol| symbol.to_string());

    let existing = match selector.0 {
        "id" => {
            let id: ElementId = selector_value.parse()?;
            // The kind is spelled in the id the caller wrote, so saying so
            // reveals nothing they did not already state.
            if id.kind != ElementKind::Concept {
                return Err(KipError::structural_reference_invalid(format!(
                    "{id} names a {:?}, and UPSERT CONCEPT resolves Concepts",
                    id.kind
                )));
            }
            match store.find_concept(id).await {
                // A declared type is part of the pattern, so an element of
                // another type is simply not a match. Reported as no match
                // rather than as a type mismatch, which would let an id probe
                // map the Space by reading the difference (§86.4) — and an
                // upsert by id may not create, so this still fails loudly.
                Ok(row) => match &declared_type {
                    Some(declared) if !same_lineage(&row.schema_ref, declared) => None,
                    _ => Some(id),
                },
                // Only absence is "no match". A poisoned collection or a row
                // that will not decode is the engine failing, and reporting it
                // as absence would send the caller to fix a command that is
                // not what went wrong.
                Err(err) if err.code == KipErrorCode::NotFoundOrNotVisible => None,
                Err(err) => return Err(err),
            }
        }
        _ => store
            .find_concept_by_key(&tx.cx.space, declared_type.as_deref(), &selector_value)
            .await?
            .map(|row| ElementId::new(ElementKind::Concept, row._id)),
    };

    if let Some(id) = existing
        && matcher.keys().any(|key| key != "type" && key != selector.0)
    {
        let mut cx = crate::kql::Context::open(
            store,
            &tx.cx.space,
            request,
            operation,
            &tx.authority,
            &tx.auth,
        )
        .await?;
        let loaded = cx.load(id).await?;
        if loaded.is_none()
            || !cx.matches_element_view(ElementKind::Concept, &cx.view_of(id), matcher)?
        {
            return Err(KipError::not_found_or_not_visible(
                "no accessible Concept satisfies the complete UPSERT selector",
            ));
        }
    }

    let guards = resolve_guards(tx, &b, &clause.expect_versions)?;
    let id = match existing {
        Some(id) => {
            tx.expect_versions(id, &guards).await?;
            id
        }
        None => {
            check_guards_absent(&guards, "no Concept matches this selector")?;
            if selector.0 == "id" {
                return Err(KipError::not_found_or_not_visible(format!(
                    "{selector_value} does not exist, and an UPSERT by id cannot mint an id the \
                     caller chose"
                )));
            }
            let declared = declared_type.ok_or_else(|| {
                KipError::schema_symbol_not_found(
                    "UPSERT CONCEPT creates only through MATCH {type: ..., key: ...}: a Concept's \
                     type is schema-defined and fixed at creation, so a Concept minted without \
                     one could never be given a type afterwards",
                )
            })?;
            let id = tx.mint(ElementKind::Concept).await?;
            let row = ConceptRow {
                _id: id.seq,
                schema_ref: declared,
                key: selector_value.clone(),
                ..Default::default()
            };
            let element = Element::Concept(Box::new(row));
            tx.authorize_created(&element, Permission::Create)?;
            tx.stage_new(id, element, ChangeOp::Create);
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

    apply_concept_assignments(
        store,
        tx,
        clause,
        id,
        existing.is_none(),
        request,
        operation,
    )
    .await
}

/// Reads one `MATCH` member as a string.
///
/// `MATCH` values share the pattern grammar, which admits variables and nested
/// matchers that mean nothing to an upsert: `?v` is bound by a `WHERE` an
/// upsert does not have. Rejecting them here is what keeps a member from being
/// accepted and then quietly skipped.
fn match_text(b: &Bindings<'_>, value: &MatchValue, what: &str) -> Result<String, KipError> {
    let value = match value {
        MatchValue::Literal(value) => Json::from(value.clone()),
        MatchValue::Param(name) => b.param(name)?,
        _ => {
            return Err(KipError::identity_selector_required(format!(
                "an UPSERT MATCH `{what}` must be a literal or a parameter"
            )));
        }
    };
    match value {
        Json::String(text) => Ok(text),
        other => Err(KipError::type_mismatch(format!(
            "an UPSERT MATCH `{what}` must be a string, got {other}"
        ))),
    }
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
    store: &Store,
    tx: &mut Transaction,
    clause: &ConceptUpsert,
    id: ElementId,
    created: bool,
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
    for action in &actions {
        update::apply_action(tx, id, action, &view, request, operation).await?;
    }
    // The insert half is a create, and a create leaves a Concept its type
    // accepts or it does not happen (§36) — `CREATE CONCEPT` has always been
    // held to that, and an upsert that mints one is not a quieter way in.
    if created || update::touches_attributes(&actions) {
        update::check_attributes(tx, id, &view).await?;
    }
    if update::touches_structural(&actions) {
        check_structural(store, tx, id).await?;
    }

    // A no-effect final state changes nothing: no version bump, no change
    // record, no receipt claiming a transition that did not happen (§32.8).
    if crate::view::render(tx.load(id).await?) != view {
        tx.mark_changed(id, ChangeOp::Update);
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
    let (targets, guards) = {
        let b = bindings(tx, request, operation);
        let targets = select::targets(
            store,
            tx,
            &select::Selection {
                what: "UPDATE",
                permission: Permission::Update,
                target: &clause.target,
                where_clauses: clause.where_clauses.as_ref(),
                limit: clause.limit.as_ref(),
            },
            &b,
        )
        .await?;
        (targets, resolve_guards(tx, &b, &clause.expect_versions)?)
    };

    for id in targets.authorized(tx).await? {
        tx.expect_versions(id, &guards).await?;

        // Every action of one UPDATE reads the element as it was when the
        // statement began: two actions on the same Facet member must not
        // compound, or the second would silently operate on what the first
        // just wrote for reasons the author cannot see in the text.
        let view = crate::view::render(tx.load(id).await?);
        for action in &clause.actions {
            update::apply_action(tx, id, action, &view, request, operation).await?;
        }
        if update::touches_attributes(&clause.actions) {
            update::check_attributes(tx, id, &view).await?;
        }
        if update::touches_structural(&clause.actions) {
            check_structural(store, tx, id).await?;
        }
        if crate::view::render(tx.load(id).await?) != view {
            tx.mark_changed(id, ChangeOp::Update);
        }
    }
    Ok(())
}

/// `TRANSITION <target> TO "<state>" [BY <ref>] [SET FIELDS {...}]
/// [SET STRUCTURAL {...}] [WHERE {...}] [LIMIT :n] {EXPECT VERSION ...}` — the
/// one lifecycle statement (§52.5).
///
/// The quoted state names the move, and the engine validates it against the
/// target's kind and its current lifecycle state (§35.3): an Assertion moved
/// to `running` and a Concept moved to `retracted` are refused as
/// `InvalidLifecycleTransition`, a move to the state already held is a
/// `no_effect`, and there is no `EXPECT STATE` because the executor already
/// checks what one would restate.
///
/// A literal state was classified before it got here: the parser checked the
/// vocabulary, `BY` and the finalizing clauses (§52.5), and the gate asked for
/// the state's permission at Space scope. A `:parameter` state arrives
/// unclassified, so the same rules are applied at execution, and every target
/// is authorized individually with the permission the bound state names —
/// which is the check that decides in either case.
async fn transition(
    store: &Store,
    tx: &mut Transaction,
    clause: &Transition,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    let (targets, guards, state, by, set_fields) = {
        let b = bindings(tx, request, operation);
        let state = b.scalar_str(&clause.to, "TRANSITION ... TO")?;
        if clause.state().is_none() {
            // The three rules the parser applies to a literal state, applied
            // here to a bound one (§52.5).
            check_registry(&state, "TRANSITION ... TO", transition_state::ALL)?;
            let names_replacement = transition_state::WITH_BY.contains(&state.as_str());
            if clause.by.is_some() != names_replacement {
                return Err(KipError::invalid_syntax(if names_replacement {
                    format!(
                        "TRANSITION ... TO {state:?} names the replacing element with BY (§52.5)"
                    )
                } else {
                    format!(
                        "TRANSITION ... TO {state:?} takes no BY: only superseded and corrected \
                         name a replacement (§52.5)"
                    )
                }));
            }
            if clause.finalizes() && !transition_state::ACTIVITY.contains(&state.as_str()) {
                return Err(KipError::invalid_syntax(format!(
                    "SET FIELDS and SET STRUCTURAL finalize an Activity; TRANSITION ... TO \
                     {state:?} carries neither (§52.5)"
                )));
            }
        }
        let permission = element_permission(&state).ok_or_else(|| {
            KipError::constraint_violation(format!("{state:?} is not a lifecycle state (§52.5)"))
        })?;
        let targets = select::targets(
            store,
            tx,
            &select::Selection {
                what: "TRANSITION",
                permission,
                target: &clause.target,
                where_clauses: clause.where_clauses.as_ref(),
                limit: clause.limit.as_ref(),
            },
            &b,
        )
        .await?;
        let guards = resolve_guards(tx, &b, &clause.expect_versions)?;
        let by = match &clause.by {
            Some(reference) => Some(b.element_ref(reference)?),
            None => None,
        };
        let set_fields = clause
            .set_fields
            .as_ref()
            .map(|fields| assignments_to_json(&b, fields, None))
            .transpose()?;
        (targets, guards, state, by, set_fields)
    };
    // Correcting Evidence writes the new record as well as linking it, so it
    // costs `create` on top of the per-element `maintain` (§57.2).
    if state == transition_state::CORRECTED {
        tx.require(Permission::Create)?;
    }

    for id in targets.authorized(tx).await? {
        tx.expect_versions(id, &guards).await?;
        move_element(
            store,
            tx,
            &LifecycleMove {
                id,
                state: &state,
                by,
                set_fields: set_fields.as_ref(),
                set_structural: clause.set_structural.as_ref(),
                request,
                operation,
            },
        )
        .await?;
    }
    Ok(())
}

/// The permission each target of a `TRANSITION` is authorized with (§52.5).
fn element_permission(state: &str) -> Option<Permission> {
    Some(match state {
        transition_state::RETRACTED => Permission::RetractOwn,
        transition_state::SUPERSEDED => Permission::SupersedeOwn,
        transition_state::CORRECTED => Permission::Maintain,
        transition_state::RUNNING
        | transition_state::COMPLETED
        | transition_state::FAILED
        | transition_state::CANCELLED => Permission::Update,
        transition_state::ARCHIVED => Permission::Archive,
        transition_state::TOMBSTONED => Permission::Tombstone,
        _ => return None,
    })
}

/// One `TRANSITION` target and the clause members that apply to it (§52.5).
struct LifecycleMove<'a> {
    id: ElementId,
    state: &'a str,
    by: Option<ElementId>,
    set_fields: Option<&'a Map<String, Json>>,
    set_structural: Option<&'a Vec<anda_kip::StructuralEdge>>,
    request: Option<&'a Map<String, Json>>,
    operation: Option<&'a Map<String, Json>>,
}

/// The refusal every illegal move is: from where the element is, to where the
/// clause asked, for a kind that has no such move (§52.5).
fn refuse_move(id: ElementId, kind: ElementKind, from: &str, to: &str) -> KipError {
    KipError::invalid_lifecycle_transition_from(
        from,
        to,
        format!("{id} is {from:?}, and a {kind} cannot move from there to {to:?} (§52.5)"),
    )
}

async fn move_element(
    store: &Store,
    tx: &mut Transaction,
    mv: &LifecycleMove<'_>,
) -> Result<(), KipError> {
    use transition_state as ts;
    let (kind, mut current, mut engine_state) = {
        let element = tx.load(mv.id).await?;
        (
            element.kind(),
            planes::lifecycle_state(element),
            element.state().to_string(),
        )
    };
    // Formation has semantic state active; empty/pending is only the private
    // shell marker until the first commit. Normalize the validation view,
    // leaving the stored marker and every committed/Activity state untouched.
    if kind == ElementKind::Concept
        && tx.is_new_element(mv.id)
        && (engine_state.is_empty() || engine_state == state::PENDING)
    {
        current = state::ACTIVE.to_string();
        engine_state = state::ACTIVE.to_string();
    }
    let fits = match mv.state {
        ts::RETRACTED | ts::SUPERSEDED => kind == ElementKind::Assertion,
        ts::CORRECTED => kind == ElementKind::Evidence,
        ts::RUNNING | ts::COMPLETED | ts::FAILED | ts::CANCELLED => kind == ElementKind::Activity,
        ts::ARCHIVED | ts::TOMBSTONED => true,
        _ => false,
    };
    if !fits {
        return Err(refuse_move(mv.id, kind, &current, mv.state));
    }
    match mv.state {
        ts::ARCHIVED | ts::TOMBSTONED => {
            shelve(tx, mv.id, kind, mv.state, &current, &engine_state).await
        }
        ts::RETRACTED => retract(tx, mv.id, kind, &current).await,
        ts::SUPERSEDED => supersede(tx, mv.id, kind, &current, mv.by).await,
        ts::CORRECTED => correct(tx, mv.id, kind, &current, mv.by).await,
        ts::RUNNING | ts::COMPLETED | ts::FAILED | ts::CANCELLED => {
            move_activity(store, tx, mv, kind, &current, &engine_state).await
        }
        _ => Err(refuse_move(mv.id, kind, &current, mv.state)),
    }
}

/// `archived` and `tombstoned` (§60): out of ordinary recall, history kept.
async fn shelve(
    tx: &mut Transaction,
    id: ElementId,
    kind: ElementKind,
    state: &str,
    current: &str,
    engine_state: &str,
) -> Result<(), KipError> {
    use transition_state as ts;
    if engine_state == state {
        return Ok(());
    }
    let legal = engine_state == state::ACTIVE
        || (state == ts::TOMBSTONED && engine_state == state::ARCHIVED);
    if !legal {
        return Err(refuse_move(id, kind, current, state));
    }
    if let Element::Assertion(row) = tx.load(id).await? {
        let row = row.clone();
        if !tx.may_represent_assertion(&row) {
            tx.require(Permission::ModerateAssertion)?;
        }
    }
    *tx.load(id).await?.state_mut() = state.to_string();
    tx.mark_changed(id, ChangeOp::Lifecycle);
    Ok(())
}

/// `retracted` (§57.3): the assertor withdraws the claim.
async fn retract(
    tx: &mut Transaction,
    id: ElementId,
    kind: ElementKind,
    current: &str,
) -> Result<(), KipError> {
    use transition_state as ts;
    if current == ts::RETRACTED {
        return Ok(());
    }
    if current != state::ACTIVE {
        return Err(refuse_move(id, kind, current, ts::RETRACTED));
    }
    require_representation(tx, id, "TRANSITION ... TO \"retracted\"").await?;
    let at = tx.cx.at.clone();
    let row = row_mut::<AssertionRow>(tx, id).await?;
    row.status = ts::RETRACTED.to_string();
    row.retracted_at = at;
    tx.mark_changed(id, ChangeOp::Lifecycle);
    Ok(())
}

/// What `superseded` and `corrected` share: a revision names its replacement
/// with `BY`, and the replacement is linked both ways (§14.2, §57.2).
struct Revision {
    state: &'static str,
    /// The permission the replacement is authorized with.
    by_permission: Permission,
    by_message: &'static str,
    self_reference: KipErrorCode,
    self_message: &'static str,
}

/// The replacement a revision links, after the checks every revision makes:
/// `BY` is present, the same replacement already linked is a no-op (`None`),
/// the record is still active, it is not replacing itself, and the caller may
/// act on the replacement.
async fn revision_target<R: Revisable>(
    tx: &mut Transaction,
    id: ElementId,
    kind: ElementKind,
    current: &str,
    by: Option<ElementId>,
    revision: &Revision,
) -> Result<Option<ElementId>, KipError> {
    let new = by.ok_or_else(|| KipError::invalid_syntax(revision.by_message))?;
    if current == revision.state
        && row_mut::<R>(tx, id)
            .await?
            .revised_by()
            .contains(&new.to_string())
    {
        return Ok(None);
    }
    if current != state::ACTIVE {
        return Err(refuse_move(id, kind, current, revision.state));
    }
    if new == id {
        return Err(KipError::new(
            revision.self_reference,
            revision.self_message,
        ));
    }
    tx.authorize_element(new, revision.by_permission).await?;
    Ok(Some(new))
}

/// Links a revision both ways and records the move.
async fn link_revision<R: Revisable>(
    tx: &mut Transaction,
    id: ElementId,
    new: ElementId,
    state: &str,
) -> Result<(), KipError> {
    let new_row = row_mut::<R>(tx, new).await?;
    if !new_row.revises().contains(&id.to_string()) {
        new_row.revises().push(id.to_string());
        tx.mark_changed(new, ChangeOp::Update);
    }
    let old_row = row_mut::<R>(tx, id).await?;
    *old_row.status_mut() = state.to_string();
    if !old_row.revised_by().contains(&new.to_string()) {
        old_row.revised_by().push(new.to_string());
    }
    tx.mark_changed(id, ChangeOp::Lifecycle);
    Ok(())
}

/// `superseded` (§57.4): the claim was wrong, and a newer Assertion about the
/// same Proposition says what is right.
async fn supersede(
    tx: &mut Transaction,
    id: ElementId,
    kind: ElementKind,
    current: &str,
    by: Option<ElementId>,
) -> Result<(), KipError> {
    const REVISION: Revision = Revision {
        state: transition_state::SUPERSEDED,
        by_permission: Permission::SupersedeOwn,
        by_message: "TRANSITION ... TO \"superseded\" names the newer Assertion with BY",
        self_reference: KipErrorCode::SupersessionMismatch,
        self_message: "an Assertion cannot supersede itself",
    };
    let Some(new) = revision_target::<AssertionRow>(tx, id, kind, current, by, &REVISION).await?
    else {
        return Ok(());
    };
    require_representation(tx, id, "TRANSITION ... TO \"superseded\"").await?;
    let (proposition, old_actor, old_scope) = claim_scope(tx, id).await?;
    let (replacement, new_actor, new_scope) = claim_scope(tx, new).await?;
    // Supersession stays inside the actor and the scope that were wrong
    // (§14.2): it never moves a claim to another actor, nor widens or narrows
    // what that actor said. A claim wrong only in its scope is withdrawn and
    // the scoped claim asserted anew.
    if old_actor != new_actor {
        return Err(KipError::new(
            KipErrorCode::SupersessionMismatch,
            format!("{new} is another actor's claim; {id} can be superseded only by its own actor"),
        ));
    }
    if old_scope != new_scope {
        return Err(KipError::new(
            KipErrorCode::SupersessionMismatch,
            format!(
                "{new} holds in another context set than {id}; a claim wrong only in its scope \
                 is retracted and asserted anew (§14.2)"
            ),
        ));
    }
    // A value-only correction replaces the claim with another value of the
    // same slot (§14.2): the same canonical subject and predicate lineage.
    // Anything wider is a different claim, never a revision of this one.
    if replacement != proposition {
        let old_slot = slot_of(tx, proposition.parse()?).await?;
        let new_slot = slot_of(tx, replacement.parse()?).await?;
        if old_slot.is_none() || old_slot != new_slot {
            return Err(KipError::new(
                KipErrorCode::SupersessionMismatch,
                format!(
                    "{new} is about {replacement}, which is neither {proposition} nor another \
                     value of its slot"
                ),
            ));
        }
    }
    link_revision::<AssertionRow>(tx, id, new, REVISION.state).await
}

/// What a revision must keep (§14.2): the Proposition, and the canonical
/// actor and context set, merge-resolved so a claim recorded before a merge
/// compares equal to one recorded after it.
async fn claim_scope(
    tx: &mut Transaction,
    id: ElementId,
) -> Result<(String, String, Vec<String>), KipError> {
    let (proposition, actor, contexts) = {
        let row = row_mut::<AssertionRow>(tx, id).await?;
        (
            row.proposition_id.clone(),
            row.asserted_by.clone(),
            row.context_refs.clone(),
        )
    };
    let actor = canonical_key(tx, &actor).await?;
    let mut scope = Vec::with_capacity(contexts.len());
    for context in &contexts {
        scope.push(canonical_key(tx, context).await?);
    }
    scope.sort();
    scope.dedup();
    Ok((proposition, actor, scope))
}

/// A Proposition's slot: its canonical subject and its predicate lineage.
async fn slot_of(
    tx: &mut Transaction,
    proposition: ElementId,
) -> Result<Option<(String, String)>, KipError> {
    Ok(match tx.final_element(proposition).await? {
        Element::Proposition(row) => Some((
            canonical_key(tx, &row.subject).await?,
            crate::schema::lineage_of(&row.predicate_ref),
        )),
        _ => None,
    })
}

/// The endpoint key of a stored reference, after following merges.
///
/// For comparison only: unlike [`canonicalize_reference`] it records no
/// reference binding, because nothing is being written under it.
async fn canonical_key(tx: &mut Transaction, value: &Json) -> Result<String, KipError> {
    match element_reference(value) {
        Some(id) if id.kind == ElementKind::Concept => {
            let chain = canonical_chain(tx, id).await?;
            Ok(Endpoint::Local(*chain.last().unwrap_or(&id)).key())
        }
        Some(id) => Ok(Endpoint::Local(id).key()),
        None => Ok(endpoint_key(value)),
    }
}

/// The element a stored reference names, written as an id string or as
/// `{"id": ...}` — the two spellings a reference field accepts.
pub(crate) fn element_reference(value: &Json) -> Option<ElementId> {
    match value {
        Json::String(text) => text.parse().ok(),
        Json::Object(map) => map.get("id")?.as_str()?.parse().ok(),
        _ => None,
    }
}

/// `corrected` (§57.2): the record was wrong, and a new Evidence record
/// carries the correction.
async fn correct(
    tx: &mut Transaction,
    id: ElementId,
    kind: ElementKind,
    current: &str,
    by: Option<ElementId>,
) -> Result<(), KipError> {
    const REVISION: Revision = Revision {
        state: transition_state::CORRECTED,
        by_permission: Permission::Maintain,
        by_message: "TRANSITION ... TO \"corrected\" names the new Evidence with BY",
        self_reference: KipErrorCode::EvidenceCorrectionConflict,
        self_message: "an Evidence record cannot correct itself",
    };
    let Some(new) = revision_target::<EvidenceRow>(tx, id, kind, current, by, &REVISION).await?
    else {
        return Ok(());
    };
    link_revision::<EvidenceRow>(tx, id, new, REVISION.state).await
}

/// An Activity's status moves (§16): forward from `pending`, and a finished
/// Activity's provenance is immutable (§16.6).
async fn move_activity(
    store: &Store,
    tx: &mut Transaction,
    mv: &LifecycleMove<'_>,
    kind: ElementKind,
    current: &str,
    engine_state: &str,
) -> Result<(), KipError> {
    use transition_state as ts;
    if current == mv.state {
        return Ok(());
    }
    if is_terminal(current) {
        return Err(KipError::activity_terminal(format!(
            "{} is already {current:?}; a finished Activity's provenance is immutable",
            mv.id
        )));
    }
    if engine_state != state::ACTIVE {
        return Err(refuse_move(mv.id, kind, current, mv.state));
    }
    let legal = match mv.state {
        ts::RUNNING => current == "pending",
        _ => current == "pending" || current == ts::RUNNING,
    };
    if !legal {
        return Err(refuse_move(mv.id, kind, current, mv.state));
    }
    finalize_activity(store, tx, mv).await?;
    tx.mark_changed(mv.id, ChangeOp::Lifecycle);
    Ok(())
}

async fn finalize_activity(
    store: &Store,
    tx: &mut Transaction,
    mv: &LifecycleMove<'_>,
) -> Result<(), KipError> {
    let id = mv.id;
    let mut fields = Fields::new(mv.set_fields.cloned().unwrap_or_default())?;
    let started = fields.timestamp("started_at")?;
    let ended = fields.timestamp("ended_at")?;
    let parameters_digest = fields.text("parameters_digest")?;
    fields.rest("Activity")?;

    let structural = match mv.set_structural {
        Some(edges) => {
            let mut structural = {
                let b = bindings(tx, mv.request, mv.operation);
                collect_structural(tx, &b, Some(edges), core_fields(ElementKind::Activity))?
            };
            // §11.3: a reference added now resolves through whatever merges
            // the Space has already declared.
            structural.canonicalize(tx).await?;
            Some(structural)
        }
        None => None,
    };

    let at = tx.cx.at.clone();
    let row = row_mut::<ActivityRow>(tx, id).await?;
    if !started.is_empty() {
        row.started_at = started;
    }
    if !parameters_digest.is_empty() {
        row.parameters_digest = parameters_digest;
    }
    if let Some(mut structural) = structural {
        // Finalized topology is added to what the Activity already recorded:
        // an output named twice is one output, and a reference the Activity
        // already carries is not moved.
        for (field, refs) in [
            ("inputs", &mut row.inputs),
            ("outputs", &mut row.outputs),
            ("associated_actors", &mut row.associated_actors),
        ] {
            for (value, _) in structural.take(field) {
                let key = endpoint_key(&value);
                if !refs.iter().any(|held| endpoint_key(held) == key) {
                    refs.push(value);
                }
            }
        }
        row.input_keys = row.inputs.iter().map(endpoint_key).collect();
        row.output_keys = row.outputs.iter().map(endpoint_key).collect();
        for (field, refs) in structural.profile {
            let Json::Array(items) = refs else {
                continue;
            };
            let entry = row
                .structural
                .entry(field.clone())
                .or_insert_with(|| Json::Array(Vec::new()));
            if let Json::Array(held) = entry {
                for item in items {
                    update::place_reference(held, item, None, false, &field)?;
                }
            }
        }
    }

    row.status = mv.state.to_string();
    if !ended.is_empty() {
        row.ended_at = ended;
    } else if is_terminal(mv.state) && row.ended_at.is_empty() {
        // Terminal outputs freeze with the end time, so a transition that
        // forgot to give one still records when the freeze happened. Only when
        // the Activity has none: an `ended_at` the caller already recorded is
        // an observed instant, and replacing it with the commit time would
        // lose the observation to a clock the caller never asked about.
        row.ended_at = at;
    }
    if mv.set_structural.is_some() {
        check_structural(store, tx, id).await?;
    }
    Ok(())
}

/// Refuses to record a withdrawal the caller has no standing to make (§14.1,
/// §57.3).
///
/// Retraction and supersession both say something about the *source*: that it
/// took its claim back, or replaced it. A moderator who merely wants the claim
/// out of recall has `TRANSITION ... TO "archived"` and `"tombstoned"`, which
/// say what they actually mean. Letting administrative dislike write itself
/// down as the source's own withdrawal would make the epistemic record report
/// an event that never happened — and that record is the entire product of
/// this engine.
async fn require_representation(
    tx: &mut Transaction,
    id: ElementId,
    what: &str,
) -> Result<(), KipError> {
    let row = row_mut::<AssertionRow>(tx, id).await?.clone();
    if tx.may_represent_assertion(&row) {
        return Ok(());
    }
    Err(KipError::retraction_not_authorized(format!(
        "{what} records that the source withdrew this claim, and this Principal neither wrote \
         {id} nor holds an ActorBinding representing {}. TRANSITION ... TO \"archived\" or \
         \"tombstoned\" removes it from recall without claiming a withdrawal that did not happen",
        if row.asserted_by_key.is_empty() {
            "its author"
        } else {
            row.asserted_by_key.as_str()
        }
    )))
}

/// Resolves a statement's trailing `EXPECT VERSION` guards (§35.1, §52.8).
///
/// A guard without a plane compares the element's `_system.version`; one with
/// a plane compares that plane's own counter. A Facet plane is resolved to its
/// exact symbol here, and the refusal a mismatch produces names the local
/// name the guard was written with.
pub(super) fn resolve_guards(
    tx: &Transaction,
    b: &Bindings<'_>,
    guards: &[ExpectVersion],
) -> Result<Vec<Guard>, KipError> {
    let mut out = Vec::with_capacity(guards.len());
    for guard in guards {
        let version = b.scalar_u64(&guard.version, "EXPECT VERSION")?;
        let plane = match &guard.plane {
            None => PlaneKey::Element,
            Some(VersionPlane::Attributes) => PlaneKey::Attributes,
            Some(VersionPlane::Structural) => PlaneKey::Structural,
            Some(VersionPlane::Retention) => PlaneKey::Retention,
            Some(VersionPlane::Facet(symbol)) => {
                let name = symbol_name(b, symbol)?;
                let resolved = tx
                    .env
                    .resolve_symbol(SymbolKind::Facet, &name, Intent::Read)?;
                // The local name, which is the lineage's key (§20.14): the
                // counter a Facet accumulated under an earlier version of its
                // package is the same counter after an upgrade.
                PlaneKey::Facet {
                    local: resolved.name.clone(),
                }
            }
        };
        out.push(Guard { version, plane });
    }
    Ok(out)
}

/// Checks a statement's guards against an element that does not exist (§35.2).
///
/// Every counter of an absent element is 0, so the bare `EXPECT VERSION 0` —
/// the create-only guard — passes, and so does a plane guard at 0, which says
/// the plane has never been written. Anything else names a version the
/// element cannot be at.
fn check_guards_absent(guards: &[Guard], why: &str) -> Result<(), KipError> {
    for guard in guards {
        if guard.version == 0 {
            continue;
        }
        return Err(match &guard.plane {
            PlaneKey::Element => KipError::version_conflict(format!(
                "{why}, so it cannot be at version {}",
                guard.version
            )),
            plane => KipError::version_conflict_on_plane(
                &plane.name(),
                format!(
                    "{why}, so its {} plane cannot be at version {}",
                    plane.name(),
                    guard.version
                ),
            ),
        });
    }
    Ok(())
}

async fn set_retention(
    store: &Store,
    tx: &mut Transaction,
    clause: &SetRetention,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    let (targets, values, guards) = {
        let b = bindings(tx, request, operation);
        let targets = select::targets(
            store,
            tx,
            &select::Selection {
                what: "SET RETENTION",
                permission: Permission::ManageRetention,
                target: &clause.target,
                where_clauses: clause.where_clauses.as_ref(),
                limit: clause.limit.as_ref(),
            },
            &b,
        )
        .await?;
        let values = assignments_to_json(&b, &clause.values, None)?;
        let guards = resolve_guards(tx, &b, &clause.expect_versions)?;
        (targets, values, guards)
    };

    // Spec §19: retention is storage lifecycle. `expires_at` here is when the
    // *record* stops being retained, never when the claim stops applying —
    // that is `valid_time.until`, on an Assertion, and nothing here touches it.
    let retention = Json::Object(values);
    check_retention(&retention)?;
    let expires = expires_at(&retention)?;
    for id in targets.authorized(tx).await? {
        tx.expect_versions(id, &guards).await?;
        let current = tx.load(id).await?.retention().clone();
        // The hold gate needs what is recorded, not only what was written: the
        // block replaces rather than patches, so omitting `legal_hold` lifts one.
        require_legal_hold_authority(tx, &current, &retention)?;
        if current == retention {
            continue;
        }
        let (slot, slot_expires) = tx.load(id).await?.retention_mut();
        *slot = retention.clone();
        *slot_expires = expires.clone();
        tx.mark_changed(id, ChangeOp::Retention);
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
    let (source, target, guards) = {
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
        let guards = resolve_guards(tx, &b, &clause.expect_versions)?;
        (source, target, guards)
    };
    let source = source.authorized(tx).await?.into_iter().next().unwrap();
    let target = target.authorized(tx).await?.into_iter().next().unwrap();

    if source.kind != ElementKind::Concept || target.kind != ElementKind::Concept {
        return Err(KipError::structural_reference_invalid(
            "MERGE CONCEPT consolidates Concepts; other element kinds have no merged identity",
        ));
    }
    // The guards apply to the source, the Concept whose identity the statement
    // moves.
    tx.expect_versions(source, &guards).await?;

    // Identity compatibility is schema-lineage identity, not the display name
    // or the exact package version. Authorization above also enforces Space.
    let source_type = match tx.load(source).await? {
        Element::Concept(row) => row.schema_ref.clone(),
        _ => unreachable!(),
    };
    let target_type = match tx.load(target).await? {
        Element::Concept(row) => row.schema_ref.clone(),
        _ => unreachable!(),
    };
    if !same_lineage(&source_type, &target_type) {
        return Err(KipError::new(
            KipErrorCode::IdentityMergeConflict,
            "MERGE CONCEPT endpoints have incompatible Concept Type lineages",
        ));
    }
    if source == target {
        return Ok(());
    }
    let chain = canonical_chain(tx, target).await?;
    if chain.contains(&source) {
        return Err(KipError::new(
            KipErrorCode::IdentityMergeConflict,
            "MERGE CONCEPT would create an identity cycle",
        ));
    }
    let canonical_target = *chain.last().unwrap_or(&target);
    let source_chain = canonical_chain(tx, source).await?;
    if source_chain.len() > 1 && source_chain.last() == Some(&canonical_target) {
        return Ok(());
    }
    let Element::Concept(row) = tx.load(source).await? else {
        unreachable!()
    };
    if !row.merged_into.is_empty() {
        return Err(KipError::new(
            KipErrorCode::IdentityMergeConflict,
            format!("{source} is already merged into an incompatible canonical target"),
        ));
    }
    row.merged_into = target.to_string();
    // Merged, not archived: the two say different things. Archived means "out
    // of ordinary recall"; merged additionally means "this identity is now
    // that one", which is what a reader needs in order to follow the pointer.
    row.state = state::MERGED.to_string();
    tx.mark_changed(source, ChangeOp::Merge);
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
) -> Result<Targets, KipError> {
    let targets: Targets = select::targets(
        store,
        tx,
        &select::Selection {
            what,
            permission,
            target,
            where_clauses,
            limit: None,
        },
        b,
    )
    .await?;
    match targets.len() {
        0 => Err(KipError::not_found_or_not_visible(format!(
            "the {what} block does not select a visible Concept",
        ))),
        1 => Ok(targets),
        n => Err(KipError::new(
            KipErrorCode::IdentityMergeConflict,
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
    let resolved = *chain.last().unwrap_or(&id);
    tx.record_reference(&id.to_string(), &resolved.to_string());
    Ok(Endpoint::Local(resolved))
}

/// Rewrites one reference value onto the Concept a merge made canonical.
///
/// §11.3: ordinary new writes canonicalize merged references. Doing it only
/// for `ENSURE PROPOSITION` endpoints — which is where this started — makes a
/// merge decorative for everything else: new Assertions keep accumulating
/// under `asserted_by: :A` after A was merged into B, and the two identities
/// the merge declared to be one never meet again.
///
/// A reference this cannot resolve is left exactly as written. Canonicalizing
/// is a rewrite toward an identity the Space already declared; it is not a
/// place to invent one.
pub(crate) async fn canonicalize_reference(
    tx: &mut Transaction,
    value: Json,
) -> Result<Json, KipError> {
    let (text, was_object) = match &value {
        Json::String(text) => (text.clone(), false),
        Json::Object(map) => match map.get("id").and_then(Json::as_str) {
            Some(id) => (id.to_string(), true),
            None => return Ok(value),
        },
        _ => return Ok(value),
    };
    let Ok(id) = text.parse::<ElementId>() else {
        return Ok(value);
    };
    if id.kind != ElementKind::Concept {
        return Ok(value);
    }
    let chain = canonical_chain(tx, id).await?;
    let canonical = *chain.last().unwrap_or(&id);
    tx.record_reference(&text, &canonical.to_string());
    if canonical == id {
        return Ok(value);
    }
    Ok(if was_object {
        let mut map = match value {
            Json::Object(map) => map,
            _ => Map::new(),
        };
        map.insert("id".to_string(), Json::String(canonical.to_string()));
        Json::Object(map)
    } else {
        Json::String(canonical.to_string())
    })
}

/// Canonicalizes every reference in a list.
async fn canonicalize_all(tx: &mut Transaction, values: Vec<Json>) -> Result<Vec<Json>, KipError> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        out.push(canonicalize_reference(tx, value).await?);
    }
    Ok(out)
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

/// Whether an Activity has ended, and its provenance frozen (§16.6).
///
/// The set is the Core Package's, not this engine's (§20.13). Inventing a
/// terminal state locally is how the two reference engines came to disagree
/// about whether `TRANSITION ... TO "cancelled"` froze anything — each had a
/// plausible extra word and neither had the registry.
fn is_terminal(status: &str) -> bool {
    anda_kip::ACTIVITY_TERMINAL.contains(&status)
}

/// The element an earlier attempt already created under this `CLIENT KEY`.
///
/// Looked up before the clause is built so the build can be skipped when the
/// answer is "nothing yet", and settled by [`client_key_retry`] once there is
/// something to compare.
async fn find_client_key(
    store: &Store,
    tx: &Transaction,
    kind: ElementKind,
    client_key: &str,
) -> Result<Option<ElementId>, KipError> {
    if client_key.is_empty() {
        return Ok(None);
    }
    if let Some(id) = tx.staged_client_key(kind, client_key) {
        return Ok(Some(id));
    }
    store
        .find_by_client_key(&tx.cx.space, kind, client_key)
        .await
}

/// Whether this creation proves itself a retry of the one the key already made.
///
/// §52.1: a `CREATE` creates a historically distinct element *unless* a
/// `client_key` proves a retry of the same logical creation. Proving it is the
/// point — the key alone does not — so the built element is compared with the
/// one the key already names, over the members a creation fixes:
///
/// - **equal** — a retry. The handle points at the existing element, the
///   caller gets the id its first attempt produced, and nothing is written,
///   which is what makes it a retry rather than a second creation.
/// - **different** — `ClientKeyConflict`. Two logical creations under one key
///   is a client bug, and silently returning the first would leave the second
///   one's work undone while reporting success.
async fn client_key_retry(
    tx: &mut Transaction,
    existing: Option<ElementId>,
    handle: &str,
    element: &Element,
) -> Result<bool, KipError> {
    let Some(existing_id) = existing else {
        return Ok(false);
    };
    let staged_creation = tx.is_new_element(existing_id);
    let differing = {
        let stored = tx.load(existing_id).await?;
        // Whether the stored element still is what its creation made it. Once
        // something has legitimately edited it, its mutable state is no longer
        // evidence about the creation, and comparing it would turn an ordinary
        // rename into a permanent failure for the bootstrap that re-runs the
        // same `CLIENT KEY`.
        let pristine = staged_creation || stored.version() == 1;
        creation_differs(element, stored, pristine)
    };
    if let Some(member) = differing {
        return Err(KipError::new(
            KipErrorCode::ClientKeyConflict,
            format!(
                "that CLIENT KEY already names {existing_id}, whose `{member}` is not the one \
                 this creation declares; a key proves a retry of the same logical creation \
                 (§52.1), so use a fresh key, or address the existing element by id"
            ),
        ));
    }
    tx.rebind(handle, existing_id);
    Ok(true)
}

/// The first creation-fixed member two elements disagree about, if any.
///
/// Only members a creation fixes and nothing later rewrites, plus — while the
/// element is still `pristine`, meaning nothing has edited it since — the
/// mutable ones the creation declared. Comparing state an ordinary `UPDATE`
/// may have moved would report a legitimate edit as a conflicting retry, and
/// the bootstrap that re-runs the same `CLIENT KEY` would fail forever. An
/// Activity's topology is the clearest case — a terminal `TRANSITION`
/// finalizes it (§52.5) — so only its class is compared.
fn creation_differs(new: &Element, old: &Element, pristine: bool) -> Option<&'static str> {
    let differs = |name: &'static str, a: bool| (!a).then_some(name);
    match (new, old) {
        (Element::Concept(new), Element::Concept(old)) => differs(
            "type",
            crate::schema::symbol::same_lineage(&new.schema_ref, &old.schema_ref),
        )
        .or_else(|| differs("key", new.key == old.key))
        // A Concept's name is mutable grounding state (§7.2), so it is
        // evidence about the creation only while nothing has edited the
        // element since.
        .or_else(|| differs("name", !pristine || new.name == old.name)),
        (Element::Evidence(new), Element::Evidence(old)) => {
            differs("evidence_class", new.evidence_class == old.evidence_class)
                .or_else(|| differs("payload", new.payload_inline == old.payload_inline))
                // The refs rather than the derived keys, and the key rather
                // than the raw reference just below: each member is compared
                // in the one shape *both* engines store, so the two cannot
                // call the same reuse a retry and a conflict.
                .or_else(|| differs("source", new.source_refs == old.source_refs))
        }
        (Element::Assertion(new), Element::Assertion(old)) => {
            differs("proposition", new.proposition_id == old.proposition_id)
                .or_else(|| differs("asserted_by", new.asserted_by_key == old.asserted_by_key))
                .or_else(|| differs("stance", new.stance == old.stance))
                .or_else(|| differs("mode", new.mode == old.mode))
        }
        (Element::Activity(new), Element::Activity(old)) => {
            differs("activity_class", new.activity_class == old.activity_class)
        }
        // A different kind cannot happen — the lookup is scoped by kind — and
        // if it ever did, it is the conflict this exists to report.
        _ => Some("kind"),
    }
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

/// Reads a Core-registry field, refusing a word the registry does not name.
///
/// The protocol layer checks these too (§20.13), but only where the command
/// spells a literal — a `:parameter` is bound *here*, at execution time, which
/// is the first moment its value exists. Leaving the engine's half out is how
/// `stance: :s` came to store `"maybe"`: the row keeps a word no reader can
/// interpret, `?a.stance` reads back `null`, and the projection counts the
/// Assertion as an actor who engaged — turning `insufficient` into `uncertain`
/// on the strength of a typo.
fn require_registry(
    fields: &mut Fields,
    name: &str,
    registry: &[&str],
    clause: &str,
) -> Result<String, KipError> {
    let value = require_text(fields, name, clause)?;
    check_registry(&value, name, registry)?;
    Ok(value)
}

/// Checks one value against a Core registry.
pub(crate) fn check_registry(value: &str, name: &str, registry: &[&str]) -> Result<(), KipError> {
    if registry.contains(&value) {
        return Ok(());
    }
    Err(KipError::constraint_violation(format!(
        "`{name}` is fixed by the Core Package (§20.13): it takes {}, not {value:?}",
        registry.join(" | ")
    )))
}

/// Reads `confidence`, which is epistemic support in `[0, 1]` (§13.6).
///
/// A missing confidence is stored as `-1.0`, the sentinel the view reads as
/// "the actor stated none". A caller-supplied negative would land on the same
/// sentinel and silently become silence, so the lower bound is checked as
/// carefully as the upper one.
fn read_confidence(fields: &mut Fields) -> Result<f64, KipError> {
    match fields.take("confidence") {
        None | Some(Json::Null) => Ok(NO_CONFIDENCE),
        Some(Json::Number(number)) => {
            let value = number.as_f64().ok_or_else(|| {
                KipError::type_mismatch("`confidence` must be a number in [0, 1]")
            })?;
            if !(0.0..=1.0).contains(&value) {
                return Err(KipError::constraint_violation(format!(
                    "`confidence` is epistemic support in [0, 1] (§13.6), got {value}"
                )));
            }
            Ok(value)
        }
        Some(other) => Err(KipError::type_mismatch(format!(
            "`confidence` must be a number in [0, 1], got {other}"
        ))),
    }
}

/// The stored stand-in for "this Assertion states no confidence".
///
/// Out of band rather than `Option`, because the column is a plain `f64` that
/// range queries run over; `[0, 1]` is enforced on the way in so nothing real
/// can collide with it.
pub(crate) const NO_CONFIDENCE: f64 = -1.0;

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
        Json::Object(map) => map.get("id").and_then(Json::as_str).map(str::to_string),
        _ => None,
    }
}

pub(crate) fn endpoint_key(value: &Json) -> String {
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

fn expires_at(retention: &Json) -> Result<String, KipError> {
    match retention.get("expires_at") {
        None | Some(Json::Null) => Ok(String::new()),
        Some(Json::String(text)) => time::normalize(text, "retention.expires_at"),
        Some(other) => Err(KipError::type_mismatch(format!(
            "`retention.expires_at` must be a timestamp, got {other}"
        ))),
    }
}

/// The members §19.1 gives the retention hook.
const RETENTION_MEMBERS: &[&str] = &["retention_class", "expires_at", "legal_hold"];

/// Checks a retention block against §19.1's shape.
///
/// A member outside it is refused rather than stored. The wire type carries
/// exactly these three, so anything else is written, kept, and then read back
/// as null — the caller's write appears to succeed while the value is
/// unreachable from every query that could notice it went missing.
pub(crate) fn check_retention(retention: &Json) -> Result<(), KipError> {
    if retention.is_null() {
        return Ok(());
    }
    let Json::Object(members) = retention else {
        return Err(KipError::type_mismatch(format!(
            "`retention` is an object with the members {}, got {retention}",
            RETENTION_MEMBERS.join(", ")
        )));
    };
    for name in members.keys() {
        if !RETENTION_MEMBERS.contains(&name.as_str()) {
            return Err(KipError::schema_field_not_found(format!(
                "`retention` has no member named `{name}`; §19.1 gives it {}. Storage-lifecycle \
                 state that needs a shape of its own belongs in a Facet",
                RETENTION_MEMBERS.join(", ")
            )));
        }
    }
    match members.get("retention_class") {
        None | Some(Json::Null) | Some(Json::String(_)) => {}
        Some(other) => {
            return Err(KipError::type_mismatch(format!(
                "`retention.retention_class` is a string, got {other}"
            )));
        }
    }
    match members.get("legal_hold") {
        None | Some(Json::Null) | Some(Json::Bool(_)) => {}
        Some(other) => {
            return Err(KipError::type_mismatch(format!(
                "`retention.legal_hold` is a boolean, got {other}"
            )));
        }
    }
    Ok(())
}

/// A row type behind one [`Element`] variant, so a clause can ask for "the
/// Assertion at this id" once instead of matching the variant at every site.
trait ElementRow: Sized + 'static {
    /// What the row is called when the id turns out to hold something else.
    const WHAT: &'static str;
    fn of(element: &mut Element) -> Option<&mut Self>;
}

impl ElementRow for AssertionRow {
    const WHAT: &'static str = "an Assertion";
    fn of(element: &mut Element) -> Option<&mut Self> {
        match element {
            Element::Assertion(row) => Some(row),
            _ => None,
        }
    }
}

impl ElementRow for EvidenceRow {
    const WHAT: &'static str = "an Evidence record";
    fn of(element: &mut Element) -> Option<&mut Self> {
        match element {
            Element::Evidence(row) => Some(row),
            _ => None,
        }
    }
}

impl ElementRow for ActivityRow {
    const WHAT: &'static str = "an Activity";
    fn of(element: &mut Element) -> Option<&mut Self> {
        match element {
            Element::Activity(row) => Some(row),
            _ => None,
        }
    }
}

/// The row of kind `R` at `id`, or the reference error a mismatched kind is.
async fn row_mut<R: ElementRow>(tx: &mut Transaction, id: ElementId) -> Result<&mut R, KipError> {
    let element = tx.load(id).await?;
    R::of(element)
        .ok_or_else(|| KipError::structural_reference_invalid(format!("{id} is not {}", R::WHAT)))
}

/// A record a newer one can replace (§14.2, §57.2): the lineage it links.
trait Revisable: ElementRow {
    /// The newer records that replaced this one.
    fn revised_by(&mut self) -> &mut Vec<String>;
    /// The older records this one replaces.
    fn revises(&mut self) -> &mut Vec<String>;
    fn status_mut(&mut self) -> &mut String;
}

impl Revisable for AssertionRow {
    fn revised_by(&mut self) -> &mut Vec<String> {
        &mut self.superseded_by
    }
    fn revises(&mut self) -> &mut Vec<String> {
        &mut self.supersedes
    }
    fn status_mut(&mut self) -> &mut String {
        &mut self.status
    }
}

impl Revisable for EvidenceRow {
    fn revised_by(&mut self) -> &mut Vec<String> {
        &mut self.corrected_by
    }
    fn revises(&mut self) -> &mut Vec<String> {
        &mut self.corrects
    }
    fn status_mut(&mut self) -> &mut String {
        &mut self.status
    }
}

/// What the Schema Environment needs to know about one endpoint.
/// Validates one element's Profile structural fields against their
/// declarations (§62–§66).
///
/// Endpoint types, cardinality and uniqueness together, because they are one
/// declaration: `has_step` says an Experience holds ordered, distinct
/// ExperienceSteps, and an engine that counted them without asking what they
/// were would admit the wrong kind of element as long as it came alone.
///
/// Judged on the element's whole structural map after the statement, not on
/// the clause: a minimum cardinality is a statement about what the element
/// holds, and `UNSET STRUCTURAL` can break it as easily as `SET` can.
pub(crate) async fn check_structural(
    store: &Store,
    tx: &mut Transaction,
    id: ElementId,
) -> Result<(), KipError> {
    let (source, structural) = {
        let element = tx.load(id).await?;
        // Only a Concept is typed by a symbol of its own; the other four are
        // typed by what they are about, which declares no Profile fields.
        let schema_ref = match element {
            Element::Concept(row) => Some(row.schema_ref.clone()).filter(|s| !s.is_empty()),
            _ => None,
        };
        (
            crate::schema::EndpointFacts::Element {
                kind: element.kind(),
                schema_ref,
            },
            element.structural().clone(),
        )
    };

    for (field, refs) in &structural {
        let Json::Array(items) = refs else {
            continue;
        };
        let mut targets = Vec::with_capacity(items.len());
        for item in items {
            let endpoint = Endpoint::from_json(item)?;
            let facts = facts_for(store, tx, &endpoint).await?;
            targets.push((endpoint.key(), facts));
        }
        let (_, validation) = tx
            .env
            .prepare_structural(field, &source, &targets, Intent::Write)?;
        validation.into_result()?;
    }
    Ok(())
}

async fn facts_for(
    store: &Store,
    tx: &mut Transaction,
    endpoint: &Endpoint,
) -> Result<EndpointFacts, KipError> {
    Ok(match endpoint {
        Endpoint::Literal(literal) => EndpointFacts::Literal {
            datatype: literal.datatype.clone(),
            value: literal.value.clone(),
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
