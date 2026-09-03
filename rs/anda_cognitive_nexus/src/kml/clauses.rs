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
use crate::schema::{EndpointFacts, Intent, SymbolKind};
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
        let members = assignments_to_json(b, &assignment.values, view)?;
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
    if resolve_client_key(store, tx, ElementKind::Concept, &client_key, &clause.handle).await? {
        return Ok(());
    }
    let b = bindings(tx, request, operation);
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
    refuse_protected_fields(&fields)?;
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
    tx.authorize_created(&element, Permission::Create)?;
    tx.stage_new(id, element, ChangeOp::Create);
    check_structural(store, tx, id).await
}

/// Refuses a Governance member spelled as a Core field (§28.1, §31.3).
///
/// `authority_class` and `classification` are assigned by Governance and read
/// under `governance`; a `SET FIELDS` naming one is refused as protected rather
/// than as unknown, so the caller learns which plane the member lives on.
fn refuse_protected_fields(fields: &Fields) -> Result<(), KipError> {
    for protected in ["authority_class", "classification", "governance"] {
        if fields.0.contains_key(protected) {
            return Err(update::protected_governance(protected));
        }
    }
    Ok(())
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
    if resolve_client_key(store, tx, kind, &client_key, &clause.handle).await? {
        return Ok(());
    }
    let b = bindings(tx, request, operation);
    let mut fields = Fields(
        clause
            .set_fields
            .as_ref()
            .map(|f| assignments_to_json(&b, f, None))
            .transpose()?
            .unwrap_or_default(),
    );
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

    refuse_protected_fields(&fields)?;
    let row = match kind {
        ElementKind::Evidence => {
            let payload = fields.json("payload");
            let (payload_mode, payload_inline, content_ref) = split_payload(payload)?;
            let source_refs = structural.values("source");
            let evidence_class = require_text(&mut fields, "evidence_class", "CREATE EVIDENCE")?;
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
                structural: structural.profile,
                expires_at: expires_at(&retention)?,
                retention,
                ..Default::default()
            };
            Element::Evidence(Box::new(row))
        }
        ElementKind::Assertion => {
            let proposition = require_reference(&mut fields, "proposition", "CREATE ASSERTION")?;
            // The semantic actor is a reference like any other, and a merged
            // one has to resolve to the surviving identity or the actor's own
            // claims split across two Concepts the Space calls one (§11.3).
            let asserted_by = canonicalize_reference(tx, fields.json("asserted_by")).await?;
            // §13.3: `asserted_by` is REQUIRED. A claim whose actor cannot be
            // resolved is recorded as Evidence, not asserted — an Assertion is
            // one actor's commitment, and one with no actor commits nobody.
            let asserted_by_key = endpoint_key(&asserted_by);
            if asserted_by_key.is_empty() {
                return Err(KipError::constraint_violation(
                    "CREATE ASSERTION requires `asserted_by`, the semantic actor whose commitment \
                     this is (§13.3); a claim with no resolvable actor is recorded as Evidence, \
                     not asserted",
                ));
            }
            // Each citation keeps the role it was cited in: Core records that
            // this Assertion cites E *as supporting*, and never that E proves
            // anything — that judgement belongs to the Projection (§8.4).
            let evidence: Vec<Json> = structural
                .take("evidence")
                .into_iter()
                .map(|(value, options)| {
                    // Stored in the wire shape §13.2 fixes — `{id, role}` —
                    // so the view renders it without a rename, and one place
                    // fewer can drift from the other.
                    let mut citation = Map::new();
                    citation.insert("id".into(), Json::String(reference_id(&value)));
                    if let Some(role) = options.get("role") {
                        // §20.13 fixes the Evidence roles, and a citation
                        // whose role nobody can read is a citation whose
                        // meaning is lost: `challenge` and `support` are the
                        // difference between corroboration and dissent.
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
            let valid_time = fields.json("valid_time");
            let row = AssertionRow {
                _id: id.seq,
                proposition_id: proposition.to_string(),
                asserted_by_key,
                asserted_by,
                stance: require_registry(&mut fields, "stance", STANCES, "CREATE ASSERTION")?,
                mode: require_registry(&mut fields, "mode", ASSERTION_MODES, "CREATE ASSERTION")?,
                confidence: read_confidence(&mut fields)?,
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
            Element::Assertion(Box::new(row))
        }
        ElementKind::Activity => {
            let inputs = structural.values("inputs");
            let outputs = structural.values("outputs");
            let activity_class = require_text(&mut fields, "activity_class", "CREATE ACTIVITY")?;
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
            "PURGE",
            Permission::Purge,
            &clause.target,
            clause.where_clauses.as_ref(),
            clause.limit.as_ref(),
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
            "PURGE PAYLOAD",
            Permission::Purge,
            &clause.target,
            clause.where_clauses.as_ref(),
            clause.limit.as_ref(),
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
    if let Some(existing) = store.find_proposition(&key).await? {
        let id = ElementId::new(ElementKind::Proposition, existing._id);
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
                    Some(declared) if &row.schema_ref != declared => None,
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
    let mut changed = false;
    for action in &actions {
        changed |= update::apply_action(tx, id, action, &view, request, operation)
            .await?
            .changed;
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
    if changed {
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
            "UPDATE",
            Permission::Update,
            &clause.target,
            clause.where_clauses.as_ref(),
            clause.limit.as_ref(),
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
        let mut changed = false;
        for action in &clause.actions {
            changed |= update::apply_action(tx, id, action, &view, request, operation)
                .await?
                .changed;
        }
        if update::touches_attributes(&clause.actions) {
            update::check_attributes(tx, id, &view).await?;
        }
        if update::touches_structural(&clause.actions) {
            check_structural(store, tx, id).await?;
        }
        if changed {
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
            "TRANSITION",
            permission,
            &clause.target,
            clause.where_clauses.as_ref(),
            clause.limit.as_ref(),
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
            id,
            &state,
            by,
            set_fields.as_ref(),
            clause.set_structural.as_ref(),
            request,
            operation,
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

/// Moves one element to one lifecycle state (§52.5, §57.2–§57.4, §60).
///
/// The legality table, by state and current lifecycle word:
///
/// ```text
/// retracted     Assertion   from active
/// superseded    Assertion   from active, BY an Assertion about the same Proposition
/// corrected     Evidence    from active, BY new Evidence
/// running       Activity    from pending
/// completed |   Activity    from pending or running; from a terminal state the
/// failed |                  refusal is ActivityTerminal, because the provenance
/// cancelled                 topology is frozen (§16.6)
/// archived      any         from active (engine state)
/// tombstoned    any         from active or archived (engine state)
/// ```
///
/// A move to the state already held returns without staging anything, so the
/// transaction reports `no_effect` for it (§32.8).
#[allow(clippy::too_many_arguments)]
async fn move_element(
    store: &Store,
    tx: &mut Transaction,
    id: ElementId,
    state: &str,
    by: Option<ElementId>,
    set_fields: Option<&Map<String, Json>>,
    set_structural: Option<&Vec<anda_kip::StructuralEdge>>,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    use transition_state as ts;

    let (kind, current, engine_state) = {
        let element = tx.load(id).await?;
        (
            element.kind(),
            planes::lifecycle_state(element),
            element.state().to_string(),
        )
    };
    let refuse = |from: &str| {
        KipError::invalid_lifecycle_transition_from(
            from,
            state,
            format!("{id} is {from:?}, and a {kind} cannot move from there to {state:?} (§52.5)"),
        )
    };
    let fits = match state {
        ts::RETRACTED | ts::SUPERSEDED => kind == ElementKind::Assertion,
        ts::CORRECTED => kind == ElementKind::Evidence,
        ts::RUNNING | ts::COMPLETED | ts::FAILED | ts::CANCELLED => kind == ElementKind::Activity,
        ts::ARCHIVED | ts::TOMBSTONED => true,
        _ => false,
    };
    if !fits {
        return Err(refuse(&current));
    }

    match state {
        ts::ARCHIVED | ts::TOMBSTONED => {
            if engine_state == state {
                return Ok(());
            }
            let legal = engine_state == state::ACTIVE
                || (state == ts::TOMBSTONED && engine_state == state::ARCHIVED);
            if !legal {
                return Err(refuse(&current));
            }
            // §14.1, §29: administratively excluding somebody else's claim is
            // a different act from tidying one's own, and only the first is
            // moderation. Asked for on top of `archive`/`tombstone`, never
            // instead of it, so a Grant listing only `moderate_assertion`
            // confers nothing.
            if let Element::Assertion(row) = tx.load(id).await? {
                let row = row.clone();
                if !tx.may_represent_assertion(&row) {
                    tx.require(Permission::ModerateAssertion)?;
                }
            }
            // Neither archive nor tombstone erases anything: references keep
            // resolving (§60.1, §60.2), which is what stops a removal from
            // silently breaking every Assertion that cited the element.
            set_state(tx.load(id).await?, state);
            tx.mark_changed(id, ChangeOp::Lifecycle);
        }
        ts::RETRACTED => {
            if current == ts::RETRACTED {
                return Ok(());
            }
            if current != state::ACTIVE {
                return Err(refuse(&current));
            }
            require_representation(tx, id, "TRANSITION ... TO \"retracted\"").await?;
            // §57.3: retraction preserves the historical payload. The
            // Assertion goes on existing, so the record of what was once
            // believed — and by whom — survives.
            let at = tx.cx.at.clone();
            let row = assertion_mut(tx, id).await?;
            row.status = ts::RETRACTED.to_string();
            row.retracted_at = at;
            tx.mark_changed(id, ChangeOp::Lifecycle);
        }
        ts::SUPERSEDED => {
            let new = by.ok_or_else(|| {
                KipError::invalid_syntax(
                    "TRANSITION ... TO \"superseded\" names the newer Assertion with BY",
                )
            })?;
            // §52.5 makes the current lifecycle state the first thing the
            // engine validates: from `retracted` no replacement is legal, so
            // the answer is `InvalidLifecycleTransition` whatever BY names.
            // Checking the operand first would report a mismatch between two
            // Assertions when the move was never available in the first place.
            //
            // `no_effect` only when this very supersession is already
            // recorded. Superseded by *another* Assertion is a second revision
            // and `superseded` is not a state one is legal from (§57.4);
            // answering `no_effect` there would tell the caller its lineage was
            // recorded when nothing was written.
            if current == ts::SUPERSEDED {
                let already = assertion_mut(tx, id)
                    .await?
                    .superseded_by
                    .contains(&new.to_string());
                if already {
                    return Ok(());
                }
            }
            if current != state::ACTIVE {
                return Err(refuse(&current));
            }
            if new == id {
                return Err(KipError::new(
                    KipErrorCode::SupersessionMismatch,
                    "an Assertion cannot supersede itself",
                ));
            }
            tx.authorize_element(new, Permission::SupersedeOwn).await?;
            require_representation(tx, id, "TRANSITION ... TO \"superseded\"").await?;

            // Supersession is belief revision within one lineage, so the
            // replacement must be about the same Proposition. Two claims
            // about different tuples are a contradiction, and a contradiction
            // is not a supersession (§57.4).
            let proposition = assertion_mut(tx, id).await?.proposition_id.clone();
            let new_row = assertion_mut(tx, new).await?;
            if new_row.proposition_id != proposition {
                return Err(KipError::new(
                    KipErrorCode::SupersessionMismatch,
                    format!(
                        "{new} is about {}, not about {proposition}",
                        new_row.proposition_id
                    ),
                ));
            }
            if !new_row.supersedes.contains(&id.to_string()) {
                new_row.supersedes.push(id.to_string());
                tx.mark_changed(new, ChangeOp::Update);
            }
            let old_row = assertion_mut(tx, id).await?;
            old_row.status = ts::SUPERSEDED.to_string();
            if !old_row.superseded_by.contains(&new.to_string()) {
                old_row.superseded_by.push(new.to_string());
            }
            tx.mark_changed(id, ChangeOp::Lifecycle);
        }
        ts::CORRECTED => {
            let new = by.ok_or_else(|| {
                KipError::invalid_syntax(
                    "TRANSITION ... TO \"corrected\" names the new Evidence with BY",
                )
            })?;
            // The move is judged before the operand, as supersession judges
            // it, and by the same rule: already corrected by this very record
            // is `no_effect`, corrected by another is a second correction and
            // `corrected` is not a state one is legal from (§57.2).
            if current == ts::CORRECTED {
                let already = evidence_mut(tx, id)
                    .await?
                    .corrected_by
                    .contains(&new.to_string());
                if already {
                    return Ok(());
                }
            }
            if current != state::ACTIVE {
                return Err(refuse(&current));
            }
            if new == id {
                return Err(KipError::new(
                    KipErrorCode::EvidenceCorrectionConflict,
                    "an Evidence record cannot correct itself",
                ));
            }
            tx.authorize_element(new, Permission::Maintain).await?;
            // §57.2: wrong Evidence is corrected, never rewritten. The
            // original observation stays exactly as observed, because what a
            // source said is a historical fact even when it was wrong.
            let new_row = evidence_mut(tx, new).await?;
            if !new_row.corrects.contains(&id.to_string()) {
                new_row.corrects.push(id.to_string());
                tx.mark_changed(new, ChangeOp::Update);
            }
            let old_row = evidence_mut(tx, id).await?;
            old_row.status = ts::CORRECTED.to_string();
            if !old_row.corrected_by.contains(&new.to_string()) {
                old_row.corrected_by.push(new.to_string());
            }
            tx.mark_changed(id, ChangeOp::Lifecycle);
        }
        ts::RUNNING | ts::COMPLETED | ts::FAILED | ts::CANCELLED => {
            if current == state {
                return Ok(());
            }
            // §16.6: a terminal Activity's provenance topology is immutable.
            // Once it has ended, what it consumed and produced is a
            // historical record.
            if is_terminal(&current) {
                return Err(KipError::activity_terminal(format!(
                    "{id} is already {current:?}; a finished Activity's provenance is immutable"
                )));
            }
            if engine_state != state::ACTIVE {
                return Err(refuse(&current));
            }
            let legal = match state {
                ts::RUNNING => current == "pending",
                _ => current == "pending" || current == ts::RUNNING,
            };
            if !legal {
                return Err(refuse(&current));
            }
            finalize_activity(
                store,
                tx,
                id,
                state,
                set_fields,
                set_structural,
                request,
                operation,
            )
            .await?;
            tx.mark_changed(id, ChangeOp::Lifecycle);
        }
        _ => return Err(refuse(&current)),
    }
    Ok(())
}

/// Moves an Activity's status, finalizing the fields and topology the same
/// statement carries (§52.5, §16.6).
#[allow(clippy::too_many_arguments)]
async fn finalize_activity(
    store: &Store,
    tx: &mut Transaction,
    id: ElementId,
    state: &str,
    set_fields: Option<&Map<String, Json>>,
    set_structural: Option<&Vec<anda_kip::StructuralEdge>>,
    request: Option<&Map<String, Json>>,
    operation: Option<&Map<String, Json>>,
) -> Result<(), KipError> {
    let mut fields = Fields(set_fields.cloned().unwrap_or_default());
    let started = fields.timestamp("started_at")?;
    let ended = fields.timestamp("ended_at")?;
    let parameters_digest = fields.text("parameters_digest")?;
    refuse_protected_fields(&fields)?;
    fields.rest("Activity")?;

    let structural = match set_structural {
        Some(edges) => {
            let mut structural = {
                let b = bindings(tx, request, operation);
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
    let row = activity_mut(tx, id).await?;
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

    row.status = state.to_string();
    if !ended.is_empty() {
        row.ended_at = ended;
    } else if is_terminal(state) && row.ended_at.is_empty() {
        // Terminal outputs freeze with the end time, so a transition that
        // forgot to give one still records when the freeze happened. Only when
        // the Activity has none: an `ended_at` the caller already recorded is
        // an observed instant, and replacing it with the commit time would
        // lose the observation to a clock the caller never asked about.
        row.ended_at = at;
    }
    if set_structural.is_some() {
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
    let row = assertion_mut(tx, id).await?.clone();
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
            "SET RETENTION",
            Permission::ManageRetention,
            &clause.target,
            clause.where_clauses.as_ref(),
            clause.limit.as_ref(),
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
        let current = retention_mut(tx.load(id).await?).0.clone();
        // The hold gate needs what is recorded, not only what was written: the
        // block replaces rather than patches, so omitting `legal_hold` lifts one.
        require_legal_hold_authority(tx, &current, &retention)?;
        if current == retention {
            continue;
        }
        let (slot, slot_expires) = retention_mut(tx.load(id).await?);
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
    // The guards apply to the source, the Concept whose identity the statement
    // moves.
    tx.expect_versions(source, &guards).await?;

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

/// Resolves a `CLIENT KEY` to the element an earlier attempt already created.
///
/// §52.1: a `CREATE` creates a historically distinct element *unless* a
/// `client_key` proves a retry of the same logical creation. Returns whether
/// the clause was satisfied by an existing element, in which case the handle
/// now points at it and nothing is written — a retry writes nothing, which is
/// what makes it a retry rather than a second creation.
///
/// The resolved element is authoritative: this does not compare the incoming
/// fields against it and quietly rewrite one to match the other. A key that
/// names two different logical creations is a client bug, and picking a winner
/// silently would turn it into a data-loss bug.
async fn resolve_client_key(
    store: &Store,
    tx: &mut Transaction,
    kind: ElementKind,
    client_key: &str,
    handle: &str,
) -> Result<bool, KipError> {
    if client_key.is_empty() {
        return Ok(false);
    }
    let Some(existing) = store
        .find_by_client_key(&tx.cx.space, kind, client_key)
        .await?
    else {
        return Ok(false);
    };
    tx.rebind(handle, existing);
    Ok(true)
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

async fn activity_mut(tx: &mut Transaction, id: ElementId) -> Result<&mut ActivityRow, KipError> {
    match tx.load(id).await? {
        Element::Activity(row) => Ok(row),
        _ => Err(KipError::structural_reference_invalid(format!(
            "{id} is not an Activity"
        ))),
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
        let kind = element.kind();
        let (schema_ref, structural) = match element {
            Element::Concept(row) => (Some(row.schema_ref.clone()), row.structural.clone()),
            Element::Proposition(row) => (None, row.structural.clone()),
            Element::Assertion(row) => (None, row.structural.clone()),
            Element::Evidence(row) => (None, row.structural.clone()),
            Element::Activity(row) => (None, row.structural.clone()),
        };
        (
            crate::schema::EndpointFacts::Element {
                kind,
                schema_ref: schema_ref.filter(|text| !text.is_empty()),
            },
            structural,
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
        // A field this environment cannot resolve declares nothing to hold the
        // write to, the same stance a Proposition takes on an unresolvable
        // predicate.
        let Ok((_, validation)) =
            tx.env
                .prepare_structural(field, &source, &targets, Intent::Write)
        else {
            continue;
        };
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
