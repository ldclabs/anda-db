//! # `DESCRIBE` and `LIST`
//!
//! Introspection returns **canonical identity** (Spec §106): a `DESCRIBE TYPE
//! "Person"` answers with `kip://profiles/cognitive-memory@2.0.0/Person`, not
//! with the local name the caller happened to write. A caller that stored the
//! local name would have stored something whose meaning changes when the
//! Space's schema does.

use anda_kip::{DescribeTarget, Json, KipError, KipErrorCode, ListCommand, ListTarget, Scalar};

use super::{Answer, capabilities, protocol};
use crate::governance::{Permission, ResourceContext};
use crate::kql::Context;
use crate::projection::Policy;
use crate::schema::{Intent, SymbolKind};

/// Runs one `DESCRIBE`.
pub async fn run(cx: &mut Context<'_>, target: &DescribeTarget) -> Result<Answer, KipError> {
    Ok(match target {
        DescribeTarget::Protocol => Answer::whole(protocol()),
        DescribeTarget::Capabilities => Answer::whole(capabilities()),
        DescribeTarget::ProjectionCapability => Answer::whole(serde_json::json!({
            "policies": [Policy::baseline().id, Policy::forecast().id],
            "statuses": ["accepted", "rejected", "contested", "uncertain", "insufficient"],
            "score_semantics": "normalized_support_not_probability",
            "explanation": true,
            "implemented_stages": [
                // Not a stage the projection performs so much as one it
                // inherits: every Assertion it reads comes through the same
                // authorization gate every other read does, so a claim the
                // caller may not see contributes nothing to the belief.
                "governance_visibility",
                "semantic_grounding", "conflict_set_expansion", "lifecycle_eligibility",
                "temporal_eligibility", "mode_eligibility", "corroboration_grouping",
                "aggregation", "classification", "explanation"
            ],
            "missing_stages": ["trust_evaluation", "evidence_quality"],
        })),
        DescribeTarget::ExecutionContext => Answer::whole(execution_context(cx).await?),
        DescribeTarget::Primer { mode } => Answer::whole(primer(cx, mode.as_ref()).await?),
        DescribeTarget::Space { value } => {
            let id = match value {
                Some(scalar) => scalar_str(cx, scalar, "DESCRIBE SPACE")?,
                None => cx.space.clone(),
            };
            Answer::whole(space(cx, &id).await?)
        }
        DescribeTarget::SchemaEnvironment { as_of } => {
            // The environment a past coordinate resolved through, not today's:
            // reconstructing history under current schema would answer a
            // question nobody asked (§144).
            if let Some(as_of) = as_of {
                let seq = cx.resolve_as_of(as_of).await?;
                let version = cx.store.schema_version_at(&cx.space, seq).await?;
                let env = cx.store.schema_environment_at(&cx.space, version).await?;
                let mut answer = schema_environment_of(&env);
                if let Some(object) = answer.as_object_mut() {
                    object.insert("snapshot_seq".to_string(), serde_json::json!(seq));
                }
                return Ok(Answer::whole(answer));
            }
            Answer::whole(schema_environment(cx))
        }
        DescribeTarget::Package(scalar) => {
            let reference = scalar_str(cx, scalar, "DESCRIBE PACKAGE")?;
            Answer::whole(package(cx, &reference)?)
        }
        DescribeTarget::Type(scalar) => Answer::whole(symbol(cx, SymbolKind::ConceptType, scalar)?),
        DescribeTarget::Predicate(scalar) => {
            Answer::whole(symbol(cx, SymbolKind::PredicateType, scalar)?)
        }
        DescribeTarget::Facet(scalar) => Answer::whole(symbol(cx, SymbolKind::Facet, scalar)?),
        DescribeTarget::StructuralField(scalar) => {
            Answer::whole(symbol(cx, SymbolKind::StructuralField, scalar)?)
        }
        DescribeTarget::Compatibility { from, to } => {
            let from = scalar_str(cx, from, "DESCRIBE COMPATIBILITY FROM")?;
            let to = scalar_str(cx, to, "DESCRIBE COMPATIBILITY TO")?;
            Answer::whole(compatibility(cx, &from, &to)?)
        }
        DescribeTarget::Error(scalar) => {
            let code = scalar_str(cx, scalar, "DESCRIBE ERROR")?;
            Answer::whole(error(&code)?)
        }
        DescribeTarget::EpistemicPolicy { value } => {
            let name = match value {
                Some(scalar) => scalar_str(cx, scalar, "DESCRIBE EPISTEMIC POLICY")?,
                None => cx.policy.id.clone(),
            };
            Answer::whole(policy(&name)?)
        }
        DescribeTarget::Transaction(scalar) => {
            let tx_id = scalar_str(cx, scalar, "DESCRIBE TRANSACTION")?;
            Answer::whole(super::history::transaction(cx, &tx_id).await?)
        }
        DescribeTarget::TransactionByIdempotencyKey(scalar) => {
            let key = scalar_str(cx, scalar, "DESCRIBE TRANSACTION BY IDEMPOTENCY KEY")?;
            Answer::whole(super::history::transaction_by_key(cx, &key).await?)
        }
        DescribeTarget::Snapshot { as_of } => {
            return super::history::snapshot(cx, as_of.as_ref()).await;
        }
        DescribeTarget::Capsule(_) => {
            return Err(KipError::unsupported_capability(
                "this engine has no Capsule reader, so it cannot describe one",
            ));
        }
        // Reporting an empty trust answer would read as "nothing is trusted",
        // which is a judgement. This engine evaluates no source trust, so it
        // says that instead of implying it.
        DescribeTarget::Trust { .. } => {
            return Err(KipError::unsupported_capability(
                "this engine evaluates no source trust; an empty trust report would read as a \
                 judgement that nothing is trusted",
            ));
        }
        DescribeTarget::Access { with } => Answer::whole(access(cx, with.as_ref())?),
    })
}

/// `DESCRIBE ACCESS` — what this caller may do here (§229, §230, §266).
///
/// Answers about the caller's own authority and nothing else. It names the
/// permissions held and the Grants that carry them, and it does **not** list
/// policy statements, other Principals, or which elements exist — an access
/// report that explained the whole policy would be a disclosure channel for
/// the state the policy protects (§267).
///
/// The permission list is Space-scoped and deliberately coarse: it answers
/// "could this ever be allowed here", not "is it allowed on element X". The
/// second question cannot be answered without naming X, and naming X to a
/// caller who may not discover it is the leak §103 is about.
fn access(cx: &mut Context<'_>, with: Option<&anda_kip::BoundObject>) -> Result<Json, KipError> {
    let authority = cx.authority;
    let auth = cx.auth;
    let mut report = serde_json::json!({
        "space_id": cx.space,
        "principal": {
            "id": authority.principal.principal_id,
            "class": authority.principal.principal_class,
            "status": authority.principal.status,
            "authenticated": auth.is_authenticated(),
            "authentication_strength": auth.auth_strength,
        },
        "groups": authority.groups,
        "is_owner": authority.is_owner,
        "purpose": {
            "value": auth.purpose,
            "assurance": auth.purpose_assurance,
        },
        "delegation_chain": auth.delegation_chain,
        "permissions": authority.permission_names(auth),
        "default_classification": authority.default_classification(),
        "policy": match &authority.policy {
            Some(policy) => serde_json::json!({
                "id": policy.policy_id,
                "version": policy.version,
            }),
            // "No policy is bound" is a fact about configuration, not a denial.
            None => serde_json::json!(null),
        },
        "note": "permissions are Space-scoped; an element may still be out of \
                 scope for the Grant that carries one",
    });

    if let Some(block) = with {
        let settings = crate::projection::settings_of(block, |name| cx.param_ref(name))?;
        let operation = settings
            .get("operation")
            .and_then(Json::as_str)
            .ok_or_else(|| {
                KipError::invalid_request_envelope("DESCRIBE ACCESS WITH must name an `operation`")
            })?;
        let permission = Permission::parse(operation)?;
        let resource = ResourceContext {
            kind: string_of(&settings, "kind"),
            schema_ref: string_of(&settings, "schema_ref"),
            classification: string_of(&settings, "classification"),
            element_id: string_of(&settings, "element"),
        };
        let decision = authority.authorize(permission, &resource, auth);
        if let Some(object) = report.as_object_mut() {
            object.insert(
                "decision".to_string(),
                serde_json::json!({
                    "operation": permission.as_str(),
                    "family": permission.family().as_str(),
                    "decision": decision.decision.as_str(),
                    "reason": decision.reason,
                    "constraints": decision.constraints,
                    "obligations": decision.obligations,
                }),
            );
        }
    }
    Ok(report)
}

fn string_of(settings: &anda_kip::Map<String, Json>, key: &str) -> String {
    settings
        .get(key)
        .and_then(Json::as_str)
        .unwrap_or_default()
        .to_string()
}

/// Runs one `LIST`.
pub async fn list(cx: &mut Context<'_>, command: &ListCommand) -> Result<Answer, KipError> {
    let limit = match &command.limit {
        Some(scalar) => scalar_usize(cx, scalar, "LIMIT")?,
        None => usize::MAX,
    };
    let cursor = match &command.cursor {
        Some(scalar) => scalar_usize(cx, scalar, "CURSOR")?,
        None => 0,
    };

    let mut items: Vec<Json> = match command.target {
        ListTarget::Spaces => spaces(cx).await?,
        ListTarget::SchemaPackages => {
            let status = match &command.status {
                Some(scalar) => Some(scalar_str(cx, scalar, "STATUS")?),
                None => None,
            };
            packages(cx, status.as_deref())
        }
        ListTarget::Types => symbols(cx, SymbolKind::ConceptType),
        ListTarget::Predicates => symbols(cx, SymbolKind::PredicateType),
        ListTarget::Facets => symbols(cx, SymbolKind::Facet),
        ListTarget::StructuralFields => symbols(cx, SymbolKind::StructuralField),
        ListTarget::EpistemicPolicies => vec![
            policy(&Policy::baseline().id)?,
            policy(&Policy::forecast().id)?,
        ],
    };

    let total = items.len();
    items = items.into_iter().skip(cursor).take(limit).collect();
    let consumed = cursor + items.len();
    Ok(Answer {
        result: Json::Array(items),
        next_cursor: (consumed < total).then(|| consumed.to_string()),
    })
}

async fn execution_context(cx: &mut Context<'_>) -> Result<Json, KipError> {
    let space = cx.store.get_space(&cx.space).await?;
    Ok(serde_json::json!({
        "space_id": cx.space,
        "space_seq": space.seq,
        "schema_environment_version": cx.env.version,
        "epistemic_policy": {"id": cx.policy.id, "version": cx.policy.version},
        // Who this request is running as. An Agent that cannot see its own
        // identity cannot reason about why something was refused (§266).
        "principal": {
            "id": cx.auth.principal_id,
            "authenticated": cx.auth.is_authenticated(),
            "authentication_strength": cx.auth.auth_strength,
        },
        "governance": {
            "enforced": true,
            "hint": "DESCRIBE ACCESS reports what this Principal may do here",
        },
        // Stated rather than implied: a caller reading this should know that a
        // plain read sees committed state, and that a past coordinate has to
        // be asked for.
        "read_basis": "current committed state; a past coordinate is read with AS OF",
    }))
}

/// The orientation document an Agent reads first.
///
/// Ordered by what a caller has to know before it can do anything useful:
/// where it is, what the schema lets it say, what the engine can do, and the
/// invariants that will otherwise bite it.
async fn primer(cx: &mut Context<'_>, mode: Option<&Scalar>) -> Result<Json, KipError> {
    let mode = match mode {
        Some(scalar) => scalar_str(cx, scalar, "MODE")?,
        None => "compact".to_string(),
    };
    let space = cx.store.get_space(&cx.space).await?;
    let counts = counts(cx).await?;

    let mut primer = serde_json::json!({
        "space": {
            "id": space.space_id,
            "name": space.name,
            "description": space.description,
            "seq": space.seq,
        },
        "contents": counts,
        "schema": {
            "environment_version": cx.env.version,
            "packages": cx.env.lock.packages,
            "types": symbol_names(cx, SymbolKind::ConceptType),
            "predicates": symbol_names(cx, SymbolKind::PredicateType),
        },
        // These are the distinctions a caller will otherwise get wrong, and
        // getting them wrong is how a memory system starts asserting things
        // nobody said.
        "safety_invariants": [
            "a Proposition existing is not the Proposition being true; use BELIEF for belief \
             and raw patterns for audit",
            "insufficient means 'not enough basis', never 'no'",
            "correcting a claim is a new Assertion plus SUPERSEDE, never an edit",
            "a SEARCH score is not a confidence and a miss is not an absence",
            "retention.expires_at is when the record stops being kept, not when the claim \
             stops applying",
        ],
        "golden_path": ["SEARCH or FIND to ground", "exact id", "BELIEF or FIND", "MUTATE"],
    });

    if mode == "full" {
        primer["capabilities"] = capabilities();
        primer["protocol"] = protocol();
    } else if mode != "compact" {
        return Err(KipError::invalid_syntax(format!(
            "DESCRIBE PRIMER MODE takes \"compact\" or \"full\", got {mode:?}"
        )));
    }
    Ok(primer)
}

/// How many elements of each kind the Space holds.
///
/// Only answered for a caller whose read authority reaches the whole Space
/// (§106). A count is a fact about elements a narrower Principal may not
/// discover, and a Space-wide number is exactly the leak §103 lists — so a
/// restricted caller is told that the number is being withheld, and why,
/// rather than being handed a smaller one that reads as the whole truth.
///
/// Answered from the authority rather than by counting what survives the
/// filter, because producing the number and then hiding it is one accident
/// away from returning it.
async fn counts(cx: &mut Context<'_>) -> Result<Json, KipError> {
    if !cx.authority.reads_whole_space(cx.auth) {
        return Ok(serde_json::json!({
            "withheld": "this Principal's read authority is narrower than the Space, and a \
                         Space-wide count would report elements it may not discover",
        }));
    }
    let mut counts = serde_json::Map::new();
    for kind in [
        anda_kip::ElementKind::Concept,
        anda_kip::ElementKind::Proposition,
        anda_kip::ElementKind::Assertion,
        anda_kip::ElementKind::Evidence,
        anda_kip::ElementKind::Activity,
    ] {
        let ids = cx
            .store
            .elements(kind)
            .query_all_ids(anda_db::query::Filter::And(vec![
                Box::new(crate::store::eq_field(
                    "space",
                    anda_db_schema::Fv::Text(cx.space.clone()),
                )),
                Box::new(crate::store::eq_field(
                    "state",
                    anda_db_schema::Fv::Text("active".to_string()),
                )),
            ]))
            .await
            .map_err(crate::error::db_error)?;
        counts.insert(kind.to_string(), Json::from(ids.len()));
    }
    Ok(Json::Object(counts))
}

async fn space(cx: &mut Context<'_>, id: &str) -> Result<Json, KipError> {
    let row = cx.store.get_space(id).await?;
    Ok(serde_json::json!({
        "id": row.space_id,
        "uri": row.uri,
        "name": row.name,
        "description": row.description,
        "owner_principal": row.owner_principal,
        "created_at": row.created_at,
        "seq": row.seq,
        "schema_environment_version": row.schema_environment_version,
    }))
}

async fn spaces(cx: &mut Context<'_>) -> Result<Vec<Json>, KipError> {
    let collection = cx.store.spaces();
    let ids = collection
        .query_all_ids(anda_db::query::Filter::Field((
            "space_id".to_string(),
            anda_db::query::RangeQuery::Gt(anda_db_schema::Fv::Text(String::new())),
        )))
        .await
        .map_err(crate::error::db_error)?;
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        let row: crate::store::rows::SpaceRow = collection
            .get_as(id)
            .await
            .map_err(crate::error::db_error)?;
        out.push(serde_json::json!({
            "id": row.space_id,
            "name": row.name,
            "seq": row.seq,
            "schema_environment_version": row.schema_environment_version,
        }));
    }
    Ok(out)
}

fn schema_environment(cx: &Context<'_>) -> Json {
    schema_environment_of(&cx.env)
}

fn schema_environment_of(env: &crate::schema::SchemaEnvironment) -> Json {
    serde_json::json!({
        "version": env.version,
        "packages": env.lock.packages,
        "states": env.lock.states,
        "write_defaults": env.lock.write_defaults,
        "aliases": env.lock.aliases,
    })
}

fn packages(cx: &Context<'_>, status: Option<&str>) -> Vec<Json> {
    cx.env
        .lock
        .packages
        .iter()
        .filter_map(|(id, version)| {
            let state = cx.env.state(id);
            let state_name = serde_json::to_value(state)
                .ok()
                .and_then(|value| value.as_str().map(str::to_string))
                .unwrap_or_default();
            if let Some(status) = status
                && status != state_name
            {
                return None;
            }
            let package_ref = format!("{id}@{version}");
            let artifact = cx.env.artifact(&package_ref);
            Some(serde_json::json!({
                "package_ref": package_ref,
                "package_id": id,
                "version": version,
                "status": state_name,
                "name": artifact.map(|a| a.manifest.name.clone()).unwrap_or_default(),
                "description": artifact
                    .map(|a| a.manifest.description.clone())
                    .unwrap_or_default(),
            }))
        })
        .collect()
}

fn package(cx: &Context<'_>, reference: &str) -> Result<Json, KipError> {
    let artifact = cx.env.artifact(reference).ok_or_else(|| {
        KipError::new(
            KipErrorCode::SchemaPackageUnavailable,
            format!("{reference} is not part of this Space's Schema Environment"),
        )
    })?;
    Ok(serde_json::json!({
        "package_ref": reference,
        "manifest": artifact.manifest,
        "dependencies": artifact.dependencies,
        "status": cx.env.state(&artifact.manifest.package_id),
        "symbols": {
            "concept_types": artifact.symbols(SymbolKind::ConceptType),
            "predicates": artifact.symbols(SymbolKind::PredicateType),
            "facets": artifact.symbols(SymbolKind::Facet),
            "structural_fields": artifact.symbols(SymbolKind::StructuralField),
        },
    }))
}

/// Describes one schema symbol, always by its canonical identity (§106).
fn symbol(cx: &mut Context<'_>, kind: SymbolKind, scalar: &Scalar) -> Result<Json, KipError> {
    let name = scalar_str(cx, scalar, "DESCRIBE")?;
    let symbol = cx.env.resolve_symbol(kind, &name, Intent::Read)?;
    let package = cx.env.definition_package(&symbol).ok_or_else(|| {
        KipError::new(
            KipErrorCode::SchemaPackageUnavailable,
            format!("{symbol} resolved, but its package is no longer available"),
        )
    })?;
    let definition = match kind {
        SymbolKind::ConceptType => serde_json::to_value(package.concept_type(&symbol.name)),
        SymbolKind::PredicateType => serde_json::to_value(package.predicate(&symbol.name)),
        SymbolKind::Facet => serde_json::to_value(package.facet(&symbol.name)),
        SymbolKind::StructuralField => serde_json::to_value(package.structural_field(&symbol.name)),
        SymbolKind::Enum => serde_json::to_value(package.definitions.enums.get(&symbol.name)),
    }
    .unwrap_or(Json::Null);

    Ok(serde_json::json!({
        // The canonical reference, not the local name the caller wrote: a
        // caller that stored the local name would have stored something whose
        // meaning changes when the Space's schema does.
        "ref": symbol.to_string(),
        "kind": kind.to_string(),
        "local_name": symbol.name,
        "package_ref": symbol.package.to_string(),
        "definition": definition,
    }))
}

fn symbols(cx: &Context<'_>, kind: SymbolKind) -> Vec<Json> {
    let mut out = Vec::new();
    for (package_id, version) in &cx.env.lock.packages {
        let package_ref = format!("{package_id}@{version}");
        let Some(artifact) = cx.env.artifact(&package_ref) else {
            continue;
        };
        for name in artifact.symbols(kind) {
            out.push(serde_json::json!({
                "ref": format!("{package_ref}/{name}"),
                "local_name": name,
                "package_ref": package_ref,
                "status": cx.env.state(package_id),
            }));
        }
    }
    out
}

fn symbol_names(cx: &Context<'_>, kind: SymbolKind) -> Vec<String> {
    symbols(cx, kind)
        .into_iter()
        .filter_map(|entry| entry["ref"].as_str().map(str::to_string))
        .collect()
}

fn compatibility(cx: &Context<'_>, from: &str, to: &str) -> Result<Json, KipError> {
    let missing = |reference: &str| {
        KipError::new(
            KipErrorCode::SchemaPackageUnavailable,
            format!("{reference} is not part of this Space's Schema Environment"),
        )
    };
    let source = cx.env.artifact(from).ok_or_else(|| missing(from))?;
    let target = cx.env.artifact(to).ok_or_else(|| missing(to))?;
    Ok(serde_json::json!({
        "from": from,
        "to": to,
        // Reported as the packages declare it. This engine performs no
        // compatibility analysis of its own, and inventing a classification
        // would be a claim about semantics it never examined.
        "declared": target.compatibility.clone().unwrap_or(Json::Null),
        "same_package": source.manifest.package_id == target.manifest.package_id,
        "analysis": "not performed; this engine reports the declaration only",
    }))
}

fn error(code: &str) -> Result<Json, KipError> {
    let parsed = anda_kip::KipErrorCode::from_name(code).ok_or_else(|| {
        KipError::not_found_or_not_visible(format!(
            "{code:?} is not a code in the KIP 2.0 error registry"
        ))
    })?;
    Ok(serde_json::json!({
        "code": parsed.name(),
        "category": parsed.category().as_str(),
        "retry_class": parsed.retry_class().as_str(),
        "hint": parsed.hint(),
    }))
}

fn policy(name: &str) -> Result<Json, KipError> {
    let policy = match name {
        "baseline" | crate::projection::policy::BASELINE_ID => Policy::baseline(),
        "forecast" | "kip:policy:forecast" => Policy::forecast(),
        other => {
            return Err(KipError::new(
                KipErrorCode::ProjectionPolicyUnavailable,
                format!("this Nexus has no epistemic policy {other:?}"),
            ));
        }
    };
    Ok(serde_json::json!({
        "id": policy.id,
        "version": policy.version,
        "eligible_modes": policy.modes,
        "accept_threshold": policy.accept,
        "material_threshold": policy.material,
        "unstated_confidence_weight": policy.unstated_confidence,
        "conflict_set_expansion": policy.expand_conflicts,
        "notes": [
            "mode gates eligibility and never weights a claim: a mode does not grant trust",
            "corroboration groups are counted once; repetition is not evidence",
        ],
    }))
}

pub(super) fn scalar_str(
    cx: &Context<'_>,
    scalar: &Scalar,
    what: &str,
) -> Result<String, KipError> {
    match scalar_json(cx, scalar)? {
        Json::String(text) => Ok(text),
        other => Err(KipError::type_mismatch(format!(
            "{what} takes a string, got {other}"
        ))),
    }
}

pub(super) fn scalar_usize(
    cx: &Context<'_>,
    scalar: &Scalar,
    what: &str,
) -> Result<usize, KipError> {
    match scalar_json(cx, scalar)? {
        Json::Number(n) => n.as_u64().map(|n| n as usize).ok_or_else(|| {
            KipError::type_mismatch(format!("{what} takes a non-negative integer, got {n}"))
        }),
        Json::String(text) => text.parse().map_err(|_| {
            KipError::new(
                KipErrorCode::CursorInvalidated,
                format!("{what} is not a cursor this engine issued: {text:?}"),
            )
        }),
        other => Err(KipError::type_mismatch(format!(
            "{what} takes a non-negative integer, got {other}"
        ))),
    }
}

pub(super) fn scalar_json(cx: &Context<'_>, scalar: &Scalar) -> Result<Json, KipError> {
    Ok(match scalar {
        Scalar::Literal(literal) => Json::from(literal.clone()),
        Scalar::Param(name) => cx.param_ref(name)?,
    })
}
