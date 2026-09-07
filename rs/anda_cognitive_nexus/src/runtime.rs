//! Durable host primitives. Scheduling and external I/O remain with the host;
//! Nexus owns compare-and-set state, fences, coverage and dispatch identities.
use crate::{
    governance::{Permission, ResourceContext},
    kql::Context,
    nexus::Session,
    schema::contracts::digest,
    store::{Element, planes::PlaneKey, rows::state, space::JournalEntry},
    tx::{Guard, Transaction},
};
use anda_kip::{Json, KipError, Map};
use serde_json::json;
const PROFILE: &str = "kip://profiles/cognitive-memory@2.1.0/";

fn structured_condition(condition: &Json) -> bool {
    ["element", "slot", "type"]
        .iter()
        .any(|k| condition.get(*k).is_some())
}

async fn bind_watch_condition(cx: &mut Context<'_>, mut condition: Json) -> Result<Json, KipError> {
    if !structured_condition(&condition) {
        return Ok(condition);
    }
    for key in condition.as_object().into_iter().flat_map(|v| v.keys()) {
        if !["element", "slot", "type", "ops", "touched", "text"].contains(&key.as_str()) {
            return Err(KipError::constraint_violation(
                "unknown structured Watch selector",
            ));
        }
    }
    if let Some(name) = condition.get("type") {
        let name = name.as_str().ok_or_else(|| {
            KipError::type_mismatch("Watch type must be a type name or reference")
        })?;
        condition["type"] = json!(
            cx.env
                .resolve_symbol(
                    crate::schema::SymbolKind::ConceptType,
                    name,
                    crate::schema::Intent::Read
                )?
                .to_string()
        );
    }
    for key in ["ops", "touched"] {
        if let Some(value) = condition.get(key) {
            let items = value
                .as_array()
                .ok_or_else(|| KipError::type_mismatch("Watch ops/touched must be arrays"))?;
            for item in items {
                if item.as_str().is_none_or(|s| s.is_empty()) {
                    return Err(KipError::type_mismatch(
                        "Watch selectors must contain nonempty strings",
                    ));
                }
                if key == "ops"
                    && serde_json::from_value::<anda_kip::ChangeOp>(item.clone()).is_err()
                {
                    return Err(KipError::constraint_violation(
                        "unknown Watch change operation",
                    ));
                }
            }
        }
    }
    let mut refs = vec![];
    if let Some(element) = condition.get("element") {
        refs.push(
            element
                .as_str()
                .ok_or_else(|| KipError::type_mismatch("Watch element must be a reference"))?
                .to_string(),
        );
    }
    if let Some(slot) = condition.get("slot") {
        let subject = slot["subject"]
            .as_str()
            .ok_or_else(|| KipError::type_mismatch("Watch slot needs subject"))?;
        let id = subject.parse::<crate::ElementId>()?;
        if id.kind != anda_kip::ElementKind::Concept {
            return Err(KipError::constraint_violation(
                "Watch slot subject must be a Concept",
            ));
        }
        let canonical = cx.canonical_of(id).await?.to_string();
        let predicate = slot["predicate"]
            .as_str()
            .ok_or_else(|| KipError::type_mismatch("Watch slot needs predicate"))?;
        let predicate = cx
            .env
            .resolve_symbol(
                crate::schema::SymbolKind::PredicateType,
                predicate,
                crate::schema::Intent::Read,
            )?
            .to_string();
        refs.push(canonical.clone());
        condition["slot"] = json!({"subject":canonical,"predicate":predicate});
    }
    for reference in refs {
        let row = cx
            .load_unattached(reference.parse()?)
            .await?
            .ok_or_else(|| {
                KipError::not_found_or_not_visible(
                    "Watch selector is outside the observation scope",
                )
            })?;
        let visibility = cx
            .authority
            .may_read(&row, cx.auth)
            .filter(|v| v.content)
            .ok_or_else(|| {
                KipError::not_found_or_not_visible(
                    "Watch selector is outside the observation scope",
                )
            })?;
        if !visibility.constraints.fields.is_empty()
            && condition["touched"].as_array().is_some_and(|paths| {
                paths.iter().any(|p| {
                    !visibility.constraints.fields.iter().any(|f| {
                        Some(f.as_str())
                            == p.as_str().map(|p| {
                                p.strip_prefix("fields.")
                                    .unwrap_or(p)
                                    .split('.')
                                    .next()
                                    .unwrap_or("")
                            })
                    })
                })
            })
        {
            return Err(KipError::not_authorized(
                "Watch field selector is outside the observation scope",
            ));
        }
    }
    Ok(condition)
}
fn facet<'a>(element: &'a Element, name: &str) -> Result<&'a Json, KipError> {
    element
        .facets()
        .get(&format!("{PROFILE}{name}"))
        .ok_or_else(|| KipError::constraint_violation(format!("missing {name}")))
}

impl Session {
    async fn check_dispatch(
        &self,
        space: &str,
        request: &anda_kip::cognitive::DispatchRequest,
    ) -> Result<Json, KipError> {
        let store = &self.nexus.store;
        let authority = self.effective_authority(space).await?;
        let task = store.get_element(request.task_ref.parse()?).await?;
        let task_view = crate::view::render(&task);
        let lease = facet(&task, "LeaseState")?;
        if task.space() != space
            || task_view["attributes"]["status"] != "running"
            || lease["owner"] != self.auth.principal_id
            || lease["fencing_token"] != request.fencing_token
            || lease["expires_at"].as_str().unwrap_or("") <= crate::time::now().as_str()
        {
            return Err(KipError::version_conflict(
                "dispatch requires the current unexpired lease fence",
            ));
        }
        authority
            .authorize(
                Permission::Update,
                &ResourceContext::of_element(&task),
                &self.auth,
            )
            .into_result()?;
        let activity = store.get_element(request.attempt_ref.parse()?).await?;
        authority
            .authorize(
                Permission::Read,
                &ResourceContext::of_element(&activity),
                &self.auth,
            )
            .into_result()?;
        if activity.envelope().origin.get("import").is_some()
            || activity.space() != space
            || activity.state() != state::ACTIVE
        {
            return Err(KipError::not_found_or_not_visible("attempt unavailable"));
        }
        let attempt = facet(&activity, "AttemptRecord")?;
        store
            .artifact_value(space, &attempt["selection_policy"])
            .await?;
        if let Some(trial_ref) = attempt["trial_ref"].as_str() {
            let trial = store.get_element(trial_ref.parse()?).await?;
            let trial = facet(&trial, "TrialRecord")?;
            let key = format!(
                "evaluation_policy/{}",
                trial["evaluation_policy"]["id"].as_str().unwrap_or("")
            );
            let policy = store
                .control_at(space, &key, u64::MAX)
                .await?
                .ok_or_else(|| KipError::not_authorized("current evaluation policy unavailable"))?;
            let rules = policy.value["allowed_rules"].as_array();
            let params = policy.value["allowed_parameters"].as_array();
            if !rules.is_some_and(|r| r.contains(&trial["rule"]["content_digest"]))
                || !params.is_some_and(|r| r.contains(&trial["parameters"]["content_digest"]))
                || policy.value["observer_control_digest"]
                    != trial["comparability"]["observer_control_digest"]
                || trial["quota"].as_u64() < policy.value["minimum_independent_attempts"].as_u64()
            {
                return Err(KipError::not_authorized(
                    "current policy no longer authorizes trial dispatch",
                ));
            }
        }
        if attempt["preconditions_satisfied"] != "yes" {
            return Err(KipError::constraint_violation(
                "attempt preconditions are not satisfied",
            ));
        }
        let decision = store
            .get_element(attempt["decision_ref"].as_str().unwrap_or("").parse()?)
            .await?;
        authority
            .authorize(
                Permission::Read,
                &ResourceContext::of_element(&decision),
                &self.auth,
            )
            .into_result()?;
        let record = facet(&decision, "DecisionRecord")?;
        let mut settings = Map::new();
        settings.insert(
            "context_refs".into(),
            record["basis"]["context_refs"].clone(),
        );
        settings.insert("purpose".into(), record["basis"]["purpose"].clone());
        settings.insert("risk".into(), record["basis"]["risk"].clone());
        let policy = store
            .projection_policy_at(space, u64::MAX, &settings)
            .await?;
        let mut cx = Context::open(store, space, None, None, &authority, &self.auth).await?;
        let basis = serde_json::to_value(cx.projection_basis(&policy, &cx.at, None)).unwrap();
        for key in [
            "schema_environment_version",
            "identity_version",
            "policy",
            "trust_version",
            "authorization_view",
            "context_refs",
            "purpose",
            "risk",
        ] {
            if basis[key] != record["basis"][key] {
                return Err(KipError::version_conflict(
                    "action decision basis changed; re-plan before dispatch",
                ));
            }
        }
        for reference in attempt["applied_revisions"]
            .as_array()
            .into_iter()
            .flatten()
        {
            let id = reference.as_str().unwrap_or("").parse()?;
            let revision = store.get_element(id).await?;
            let decision = authority
                .authorize(
                    Permission::Read,
                    &ResourceContext::of_element(&revision),
                    &self.auth,
                )
                .into_result()?;
            if revision.envelope().governance["authority_class"] != "executable"
                || (!decision.constraints.max_influence_authority.is_empty()
                    && crate::governance::authority::rank(
                        &decision.constraints.max_influence_authority,
                    ) < crate::governance::authority::rank("executable"))
            {
                return Err(KipError::not_authorized(
                    "exact revision lacks executable authority in this scope",
                ));
            }
            let family = revision
                .structural()
                .get(&format!("{PROFILE}revision_of"))
                .and_then(Json::as_array)
                .and_then(|r| r.first())
                .and_then(|r| r.as_str().or_else(|| r["id"].as_str()))
                .ok_or_else(|| KipError::constraint_violation("revision family unavailable"))?;
            let family = store.get_element(family.parse()?).await?;
            if !family
                .structural()
                .get(&format!("{PROFILE}current_revision"))
                .and_then(Json::as_array)
                .is_some_and(|r| {
                    r.iter()
                        .any(|r| r.as_str().or_else(|| r["id"].as_str()) == reference.as_str())
                })
            {
                return Err(KipError::version_conflict(
                    "selected revision is no longer current",
                ));
            }
            let at = cx.at.clone();
            let validity = cx.dependency_validity(&revision, &policy, &at).await?;
            if validity["action_eligible"] != true {
                return Err(KipError::version_conflict(
                    "revision dependency validity changed",
                ));
            }
        }
        let contract = facet(&decision, "DependencyBasis")?;
        for group in contract["groups"].as_array().into_iter().flatten() {
            if group["role"] == "context" {
                continue;
            }
            let mut members = vec![];
            for pin in group["pins"].as_array().into_iter().flatten() {
                let source = store
                    .get_element(pin["id"].as_str().unwrap_or("").parse()?)
                    .await?;
                let version_ok =
                    if let Some(planes) = pin["planes"].as_object().filter(|p| !p.is_empty()) {
                        planes.iter().all(|(key, expected)| {
                            crate::schema::contracts::pinned_plane(
                                &serde_json::to_value(source.plane_versions()).unwrap(),
                                key,
                            ) == expected.as_u64()
                        })
                    } else {
                        pin["version"].as_u64() == Some(source.version())
                    };
                let at = cx.at.clone();
                members.push(
                    version_ok
                        && cx.dependency_validity(&source, &policy, &at).await?["action_eligible"]
                            == true,
                );
            }
            if members.is_empty()
                || (if group["role"] == "any_of" {
                    !members.iter().any(|v| *v)
                } else {
                    !members.iter().all(|v| *v)
                })
            {
                return Err(KipError::version_conflict("action prerequisite changed"));
            }
        }
        Ok(attempt.clone())
    }

    pub async fn enqueue_dispatch(
        &self,
        space: &str,
        request: anda_kip::cognitive::DispatchRequest,
    ) -> Result<Json, KipError> {
        self.governed(space, Permission::Maintain, async || {
            let attempt = self.check_dispatch(space, &request).await?;
            let key = format!("dispatch/{}", attempt["attempt_id"].as_str().unwrap_or(""));
            let request = serde_json::to_value(request).unwrap();
            if let Some(old) = self.nexus.store.control_at(space, &key, u64::MAX).await? {
                if old.value["request"] != request {
                    return Err(KipError::constraint_violation(
                        "attempt already has a different dispatch intent",
                    ));
                }
                return Ok(json!({"version":old.version,"intent":old.value}));
            }
            let value =
                json!({"state":"ready","attempt_id":attempt["attempt_id"],"request":request});
            let row = self
                .nexus
                .store
                .publish_control(
                    space,
                    &key,
                    "dispatch",
                    0,
                    value,
                    json!({"principal_id":self.auth.principal_id}),
                )
                .await?;
            Ok(json!({"version":row.version,"intent":row.value}))
        })
        .await
    }

    /// Call immediately before external I/O. The returned key is the original
    /// attempt_id. A lost response does not authorize a fresh external attempt.
    pub async fn begin_dispatch(
        &self,
        space: &str,
        attempt_id: &str,
        expected: u64,
        fencing_token: u64,
    ) -> Result<Json, KipError> {
        self.governed(space, Permission::Maintain, async || {
            let key = format!("dispatch/{attempt_id}");
            let row = self.nexus.store.control_at(space, &key, u64::MAX).await?
                .ok_or_else(|| KipError::not_found_or_not_visible("dispatch intent unavailable"))?;
            if row.version != expected {
                return Err(KipError::version_conflict("dispatch intent version changed"));
            }
            let mut request: anda_kip::cognitive::DispatchRequest =
                serde_json::from_value(row.value["request"].clone())
                    .map_err(|e| KipError::internal_error(e.to_string()))?;
            let authority = self.effective_authority(space).await?;
            let attempt = self.nexus.store.get_element(request.attempt_ref.parse()?).await?;
            authority.authorize(Permission::Read, &ResourceContext::of_element(&attempt), &self.auth).into_result()?;
            if row.value["state"] == "completed" {
                return Ok(json!({"action":"done", "idempotency_key":attempt_id, "intent":row.value, "version":row.version}));
            }
            if row.value["state"] == "outcome_unknown" {
                return Ok(json!({"action":"outcome_unknown", "idempotency_key":attempt_id, "version":row.version}));
            }
            if fencing_token < request.fencing_token {
                return Err(KipError::version_conflict("stale dispatch fence"));
            }
            request.fencing_token = fencing_token;
            self.check_dispatch(space, &request).await?;
            let action = if row.value["state"] == "ready" || request.supports_idempotency {
                "dispatch"
            } else if request.supports_outcome_lookup {
                "lookup"
            } else {
                "outcome_unknown"
            };
            let mut value = row.value;
            value["request"] = serde_json::to_value(&request).unwrap();
            value["state"] = json!(if action == "outcome_unknown" { "outcome_unknown" } else { "dispatching" });
            let saved = self.nexus.store.publish_control(space, &key, "dispatch", expected,
                value, json!({"principal_id":self.auth.principal_id})).await?;
            Ok(json!({"action":action, "idempotency_key":attempt_id, "intent":saved.value, "version":saved.version}))
        }).await
    }

    /// Reconcile with a recorded instrument observation; no external retry is
    /// performed here, including when the original worker lease has expired.
    pub async fn reconcile_dispatch(
        &self,
        space: &str,
        attempt_id: &str,
        expected: u64,
        outcome_ref: &str,
    ) -> Result<Json, KipError> {
        self.governed(space, Permission::RecordOutcome, async || {
            let store = &self.nexus.store;
            let key = format!("dispatch/{attempt_id}");
            let row = store
                .control_at(space, &key, u64::MAX)
                .await?
                .ok_or_else(|| KipError::not_found_or_not_visible("dispatch unavailable"))?;
            let outcome = store.get_element(outcome_ref.parse()?).await?;
            let authority = self.effective_authority(space).await?;
            authority
                .authorize(
                    Permission::Read,
                    &ResourceContext::of_element(&outcome),
                    &self.auth,
                )
                .into_result()?;
            let record = facet(&outcome, "OutcomeRecord")?;
            if outcome.space() != space
                || record["attempt_ref"] != row.value["request"]["attempt_ref"]
                || record["terminal"] != true
            {
                return Err(KipError::constraint_violation(
                    "reconciliation must name this attempt's terminal observation",
                ));
            }
            if row.value["state"] == "completed" && row.value["outcome_ref"] == outcome_ref {
                return Ok(row.value);
            }
            let mut value = row.value;
            value["state"] = json!(if record["outcome_status"] == "unknown" {
                "outcome_unknown"
            } else {
                "completed"
            });
            value["outcome_ref"] = json!(outcome_ref);
            Ok(store
                .publish_control(
                    space,
                    &key,
                    "dispatch",
                    expected,
                    value,
                    json!({"principal_id":self.auth.principal_id}),
                )
                .await?
                .value)
        })
        .await
    }
    pub async fn change_page(
        &self,
        space: &str,
        after: u64,
        limit: usize,
    ) -> Result<Json, KipError> {
        let _guard = self.nexus.read_guard().await?;
        let authority = self.effective_authority(space).await?;
        authority
            .authorize(
                Permission::ReadHistory,
                &ResourceContext::default(),
                &self.auth,
            )
            .into_result()?;
        let mut cx =
            Context::open(&self.nexus.store, space, None, None, &authority, &self.auth).await?;
        crate::meta::history::change_page(&mut cx, after, limit).await
    }

    /// Initial claim, renewal, or takeover after expiry. Expected version is
    /// the read-side CAS; the returned fencing token must accompany dispatch.
    pub async fn lease_task(
        &self,
        space: &str,
        task_ref: &str,
        expected: u64,
        expires_at: &str,
    ) -> Result<Json, KipError> {
        let expires_at = crate::time::normalize(expires_at, "lease expires_at")?;
        self.with_authority(space,async |authority| {
            let id=task_ref.parse()?;
            let mut tx=Transaction::begin(&self.nexus.store,space,json!({"principal_id":self.auth.principal_id}),false,authority,(*self.auth).clone()).await?;
            tx.authorize_element(id,Permission::Update).await?;
            tx.expect_versions(id,&[Guard{version:expected,plane:PlaneKey::Element}]).await?;
            let now=tx.cx.at.clone();
            let Element::Concept(row)=tx.load(id).await? else {return Err(KipError::constraint_violation("task must be a SleepTask"));};
            if row.schema_ref!=format!("{PROFILE}SleepTask") {return Err(KipError::constraint_violation("task must be a SleepTask"));}
            let previous=row.facets.get(&format!("{PROFILE}LeaseState")).cloned();
            let takeover=row.attributes.get("status")!=Some(&json!("running")) || previous.as_ref().is_some_and(|p|p["expires_at"].as_str().unwrap_or("")<=now.as_str());
            let fence=previous.as_ref().and_then(|p|p["fencing_token"].as_u64()).unwrap_or(0)+u64::from(takeover||previous.is_none());
            let attempts=previous.as_ref().and_then(|p|p["attempt_count"].as_u64()).unwrap_or(0)+u64::from(takeover||previous.is_none());
            let lease=json!({"owner":self.auth.principal_id,"fencing_token":fence,"expires_at":expires_at,"attempt_count":attempts});
            row.attributes.insert("status".into(),json!("running"));row.facets.insert(format!("{PROFILE}LeaseState"),lease.clone());
            tx.mark_changed(id,anda_kip::ChangeOp::Update);let outcome=tx.commit(JournalEntry::default()).await?;
            Ok(json!({"lease":lease,"receipt":outcome.receipt}))
        }).await
    }

    /// Arming always creates a new generation, including identical conditions.
    pub async fn arm_watch(
        &self,
        space: &str,
        watch_ref: &str,
        expected: u64,
    ) -> Result<Json, KipError> {
        self.with_authority(space,async |authority| {
            let id=watch_ref.parse()?;
            let mut cx=Context::open(&self.nexus.store,space,None,None,&authority,&self.auth).await?;
            let existing=self.nexus.store.get_element(id).await?;
            let condition=bind_watch_condition(&mut cx,crate::view::render(&existing)["attributes"]["condition"].clone()).await?;
            let authorization=cx.projection_basis(&cx.policy,&cx.at,None).authorization_view;
            let mut tx=Transaction::begin(&self.nexus.store,space,json!({"principal_id":self.auth.principal_id}),false,authority,(*self.auth).clone()).await?;
            tx.authorize_element(id,Permission::Update).await?;
            tx.expect_versions(id,&[Guard{version:expected,plane:PlaneKey::Element}]).await?;
            let seq=tx.cx.seq-1;
            let Element::Concept(row)=tx.load(id).await? else {return Err(KipError::constraint_violation("watch must be a Watch Concept"));};
            if row.schema_ref!=format!("{PROFILE}Watch") {return Err(KipError::constraint_violation("watch must be a Watch Concept"));}
            row.attributes.insert("condition".into(),condition);
            let generation=row.facets.get(&format!("{PROFILE}WatchState")).and_then(|v|v["arm_generation"].as_u64()).unwrap_or(0)+1;
            let watch=json!({"arm_generation":generation,"armed_seq":seq,"condition_digest":digest(row.attributes.get("condition").unwrap_or(&Json::Null))?,"authorization_view":authorization,"consumed_seq":seq,"matched":false});
            row.attributes.insert("status".into(),json!("armed"));row.facets.insert(format!("{PROFILE}WatchState"),watch.clone());
            tx.authorized_watch_updates.insert(id);tx.mark_changed(id,anda_kip::ChangeOp::Update);
            let outcome=tx.commit(JournalEntry::default()).await?;Ok(json!({"watch":watch,"receipt":outcome.receipt}))
        }).await
    }

    pub async fn advance_watch(
        &self,
        space: &str,
        watch_ref: &str,
        expected: u64,
        generation: u64,
        limit: usize,
    ) -> Result<Json, KipError> {
        {
            let _guard = self.nexus.read_guard().await?;
            let row = self.nexus.store.get_element(watch_ref.parse()?).await?;
            if !structured_condition(&crate::view::render(&row)["attributes"]["condition"]) {
                return Err(KipError::unsupported_capability(
                    "text-only Watch requires an explicit Brain evaluator",
                ));
            }
        }
        self.advance_watch_with(space, watch_ref, expected, generation, limit, |_, _| {
            Err(KipError::unsupported_capability(
                "text-only Watch conditions require an explicit Brain evaluator",
            ))
        })
        .await
    }

    /// The optional evaluator only receives authorized change records. It is a
    /// host binding, never an author-set `matched` flag in KML.
    pub async fn advance_watch_with<F>(
        &self,
        space: &str,
        watch_ref: &str,
        expected: u64,
        generation: u64,
        limit: usize,
        evaluate: F,
    ) -> Result<Json, KipError>
    where
        F: Fn(&Json, &Json) -> Result<bool, KipError>,
    {
        self.with_authority(space,async |authority| {
            let store=&self.nexus.store;let id=watch_ref.parse()?;
            let element=store.get_element(id).await?;let mut watch=facet(&element,"WatchState")?.clone();
            let view=crate::view::render(&element);let condition=&view["attributes"]["condition"];
            if watch["arm_generation"]!=generation || view["attributes"]["status"]!="armed" {return Err(KipError::version_conflict("Watch generation is no longer armed"));}
            authority.authorize(Permission::ReadHistory,&ResourceContext::default(),&self.auth).into_result()?;
            let mut cx=Context::open(store,space,None,None,&authority,&self.auth).await?;
            let page=crate::meta::history::change_page(&mut cx,watch["consumed_seq"].as_u64().unwrap_or(0),limit).await?;
            if page["resync_required"]==true {return Err(KipError::version_conflict("control history has a gap; re-arm Watch with a new coverage basis"));}
            if watch["authorization_view"]!=page["coverage"]["authorization_view"] {return Err(KipError::version_conflict("authorization changed; re-arm the Watch with a new coverage basis"));}
            let silence=view["attributes"]["watch_class"]=="silence";
            let due=view["attributes"]["due_at"].as_str().map(|t|crate::time::normalize(t,"Watch deadline")).transpose()?;
            let mut matched=watch["matched"]==true;
            for envelope in page["changes"].as_array().into_iter().flatten() {
                if silence && due.as_ref().is_some_and(|d|envelope["committed_at"].as_str().is_some_and(|t|t>d.as_str())) {continue;}
                if envelope["control_changes"].as_array().is_some_and(|changes|changes.iter().any(|c|matches!(c["kind"].as_str(),Some("schema"|"identity"|"policy"|"trust")))) {return Err(KipError::version_conflict("Watch control basis changed; re-arm after resynchronization"));}
                if !structured_condition(condition) {matched|=evaluate(condition,envelope)?;continue;}
                for change in envelope["changes"].as_array().into_iter().flatten() {
                    let matched_change=if condition.is_string() || (condition.get("element").is_none() && condition.get("slot").is_none() && condition.get("type").is_none()) { evaluate(condition,change)? }
                    else {
                        let mut yes=true;
                        if let Some(element)=condition.get("element") {yes &= element==&change["id"];}
                        if let Some(ops)=condition["ops"].as_array() {yes &= ops.contains(&change["op"]);}
                        if let Some(touched)=condition["touched"].as_array() {yes &= change["touched"].as_array().is_some_and(|p|p.iter().any(|p|touched.contains(p)));}
                        if condition.get("type").is_some() || condition.get("slot").is_some() {
                            let target=change["id"].as_str().unwrap_or("").parse()?;
                            let retained=store.element_at(space,target,envelope["space_seq"].as_u64().unwrap_or(0)).await?.ok_or_else(||KipError::version_conflict("Watch history was erased; resynchronize coverage"))?;
                            let v=crate::view::render(&retained);
                            if let Some(t)=condition.get("type") {yes &= &v["schema_ref"]==t;}
                            if let Some(slot)=condition.get("slot") {
                                let p=if let Element::Assertion(a)=retained {store.element_at(space,a.proposition_id.parse()?,envelope["space_seq"].as_u64().unwrap_or(0)).await?.map(|r|crate::view::render(&r)).ok_or_else(||KipError::version_conflict("Watch slot history unavailable"))?} else {v};
                                yes &= p["subject"].as_str().or_else(||p["subject"]["id"].as_str())==slot["subject"].as_str() && p["predicate_ref"]==slot["predicate"];
                            }
                        }
                        yes
                    };
                    matched|=matched_change;
                }
            }
            watch["matched"]=json!(matched);watch["consumed_seq"]=page["coverage"]["through_seq"].clone();
            let deadline=due.as_ref().is_some_and(|d|d.as_str()<=cx.at.as_str()) && page["coverage"]["complete"]==true;
            let status=if !silence && matched || silence && deadline && !matched {"fired"} else if deadline {"expired"} else {"armed"};
            let mut tx=Transaction::begin(store,space,json!({"principal_id":self.auth.principal_id}),false,authority,(*self.auth).clone()).await?;
            tx.authorize_element(id,Permission::Update).await?;tx.expect_versions(id,&[Guard{version:expected,plane:PlaneKey::Element}]).await?;
            let Element::Concept(row)=tx.load(id).await? else {return Err(KipError::constraint_violation("watch is not Concept"));};
            row.attributes.insert("status".into(),json!(status));row.facets.insert(format!("{PROFILE}WatchState"),watch.clone());
            tx.authorized_watch_updates.insert(id);tx.mark_changed(id,anda_kip::ChangeOp::Update);let outcome=tx.commit(JournalEntry::default()).await?;
            Ok(json!({"watch":watch,"status":status,"coverage":page["coverage"],"receipt":outcome.receipt}))
        }).await
    }
}
