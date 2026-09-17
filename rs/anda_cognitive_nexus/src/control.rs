//! Protected Nexus contracts used by Brain hosts. These operations do not run
//! a scheduler, select memories, or execute external effects.
use crate::{
    governance::{Permission, ResourceContext},
    nexus::Session,
    projection::Policy,
    schema::contracts::digest,
    store::{Store, control::CommitPlan, rows::*, space::JournalEntry},
};
use anda_kip::{Json, KipError, KipErrorCode, Map};
use serde_json::json;

pub fn initial_projection() -> Json {
    json!({"baseline": Policy::baseline(), "forecast": Policy::forecast()})
}

impl Store {
    pub(crate) async fn authorized_artifact(
        &self,
        space: &str,
        reference: &str,
        authority: &crate::governance::EffectiveAuthority,
        auth: &crate::governance::AuthContext,
    ) -> Result<(Json, Vec<String>), KipError> {
        let row = self
            .control_at(space, &format!("artifact/{reference}"), u64::MAX)
            .await?
            .ok_or_else(|| KipError::not_found_or_not_visible("artifact unavailable"))?;
        let sources: Vec<String> = serde_json::from_value(row.value["source_refs"].clone())
            .map_err(|_| {
                KipError::not_found_or_not_visible("artifact material binding unavailable")
            })?;
        if sources.is_empty() {
            authority
                .authorize(
                    Permission::ReadGovernanceHistory,
                    &ResourceContext::default(),
                    auth,
                )
                .into_result()?;
        }
        for reference in &sources {
            let row = self.get_element(reference.parse()?).await?;
            if !authority
                .may_read(&row, auth)
                .is_some_and(|v| v.content && v.constraints.fields.is_empty())
            {
                return Err(KipError::not_found_or_not_visible("artifact unavailable"));
            }
        }
        Ok((
            self.artifact_value(
                space,
                &json!({"artifact_ref":reference,"content_digest":row.value["content_digest"]}),
            )
            .await?,
            sources,
        ))
    }
    pub(crate) async fn require_artifact_material(
        &self,
        space: &str,
        pin: &Json,
        refs: &[String],
    ) -> Result<(), KipError> {
        let key = format!("artifact/{}", pin["artifact_ref"].as_str().unwrap_or(""));
        let row = self
            .control_at(space, &key, u64::MAX)
            .await?
            .ok_or_else(|| KipError::not_found_or_not_visible("artifact unavailable"))?;
        let sources = row.value["source_refs"].as_array().ok_or_else(|| {
            KipError::constraint_violation("artifact material binding unavailable")
        })?;
        if refs.iter().any(|r| !sources.contains(&json!(r))) {
            return Err(KipError::constraint_violation(
                "replay artifact must inherit governance and erasure from every material input",
            ));
        }
        Ok(())
    }
    pub(crate) async fn artifact_value(&self, space: &str, pin: &Json) -> Result<Json, KipError> {
        let reference = pin["artifact_ref"]
            .as_str()
            .ok_or_else(|| KipError::constraint_violation("artifact reference required"))?;
        let row = self
            .control_at(space, &format!("artifact/{reference}"), u64::MAX)
            .await?
            .ok_or_else(|| KipError::not_found_or_not_visible("artifact unavailable"))?;
        if row.value["state"] != "available" {
            return Err(KipError::not_found_or_not_visible(
                "artifact erased or unavailable",
            ));
        }
        let value = &row.value["content"];
        if pin["content_digest"].as_str() != Some(digest(value)?.as_str()) {
            return Err(KipError::new(
                KipErrorCode::DigestMismatch,
                "artifact bytes do not match pinned digest",
            ));
        }
        Ok(value.clone())
    }

    pub(crate) async fn ensure_control_history(&self, space: &SpaceRow) -> Result<(), KipError> {
        for (key, kind, value) in [
            ("projection", "policy", initial_projection()),
            (
                "trust",
                "trust",
                json!({"weights":{}, "default_weight":1.0}),
            ),
        ] {
            if self
                .control_at(&space.space_id, key, u64::MAX)
                .await?
                .is_none()
            {
                self.put_control(&ControlRecordRow {
                    _id: 0,
                    record_id: format!("{}#{key}:genesis", space.space_id),
                    space: space.space_id.clone(),
                    key: key.into(),
                    seq: space.seq,
                    version: 1,
                    kind: kind.into(),
                    value,
                    origin: json!({"principal_id":"kip:principal:system"}),
                })
                .await?;
            }
        }
        self.governance.initialize_control_delivery(space).await?;
        Ok(())
    }

    pub(crate) async fn publish_control(
        &self,
        space: &str,
        key: &str,
        kind: &str,
        expected: u64,
        value: Json,
        origin: Json,
    ) -> Result<ControlRecordRow, KipError> {
        anda_kip::validate_json(&value)?;
        let previous = self.control_at(space, key, u64::MAX).await?;
        if previous.as_ref().map_or(0, |r| r.version) != expected {
            return Err(KipError::version_conflict(
                "protected control version changed",
            ));
        }
        if expected >= anda_kip::MAX_SAFE_INTEGER {
            return Err(KipError::constraint_violation("control version exhausted"));
        }
        let cx = self.begin_transaction(space, origin.clone()).await?;
        let row = ControlRecordRow {
            _id: 0,
            record_id: format!("{}:{key}", cx.tx_id),
            space: space.into(),
            key: key.into(),
            seq: cx.seq,
            version: expected + 1,
            kind: kind.into(),
            value,
            origin: origin.clone(),
        };
        let controls = if matches!(
            kind,
            "policy" | "trust" | "identity" | "authorization" | "schema"
        ) {
            json!([{"kind":kind,"version":cx.seq.to_string()}])
        } else {
            json!([])
        };
        let result = json!({"control_changes":controls,"key":key,"version":row.version});
        self.commit_plan(CommitPlan {
            cx,
            journal: JournalEntry {
                status: "committed".into(),
                transaction_class: if controls.as_array().is_some_and(|c| !c.is_empty()) {
                    "governance".into()
                } else {
                    "service".into()
                },
                schema_environment_version: self.get_space(space).await?.schema_environment_version,
                result,
                origin,
                ..Default::default()
            },
            writes: vec![],
            controls: vec![row.clone()],
            control_replacements: vec![],
            space: None,
            purge_versions: vec![],
            scrub_versions: vec![],
            audits: vec![],
            approvals: vec![],
        })
        .await?;
        Ok(row)
    }

    pub async fn projection_policy_at(
        &self,
        space: &str,
        seq: u64,
        settings: &Map<String, Json>,
    ) -> Result<Policy, KipError> {
        let explicit = settings.get("policy").is_some_and(Json::is_string);
        let coordinate = if explicit { u64::MAX } else { seq };
        let control = self
            .control_at(space, "projection", coordinate)
            .await?
            .ok_or_else(|| {
                KipError::new(
                    KipErrorCode::HistoricalSnapshotUnavailable,
                    "projection policy history unavailable",
                )
            })?;
        let requested = Policy::from_settings(settings)?;
        let name = if requested.id.starts_with("kip:policy:forecast") {
            "forecast"
        } else {
            "baseline"
        };
        let mut policy: Policy = serde_json::from_value(control.value[name].clone())
            .map_err(|e| KipError::internal_error(e.to_string()))?;
        // Apply only settings explicitly requested by the reader.
        if settings.contains_key("accept") {
            policy.accept = requested.accept;
        }
        if settings.contains_key("material") {
            policy.material = requested.material;
        }
        if ["modes", "include_predicted", "include_hypothetical"]
            .iter()
            .any(|k| settings.contains_key(*k))
        {
            policy.modes = requested.modes;
        }
        if settings.contains_key("explanation") {
            policy.explanation = requested.explanation;
        }
        policy.id = requested.id;
        policy.explicit_selection = explicit;
        policy.context_refs = requested.context_refs;
        policy.purpose = requested.purpose;
        policy.risk = requested.risk;
        if policy.material > policy.accept {
            return Err(KipError::constraint_violation("material exceeds accept"));
        }
        let trust = self
            .control_at(space, "trust", coordinate)
            .await?
            .ok_or_else(|| {
                KipError::new(
                    KipErrorCode::HistoricalSnapshotUnavailable,
                    "trust history unavailable",
                )
            })?;
        policy.trust_weights = serde_json::from_value(trust.value["weights"].clone())
            .map_err(|e| KipError::internal_error(e.to_string()))?;
        policy.default_trust_weight = trust.value["default_weight"].as_f64().unwrap_or(1.0);
        policy.contextual_trust_rules = serde_json::from_value(
            trust
                .value
                .get("rules")
                .cloned()
                .unwrap_or_else(|| json!([])),
        )
        .map_err(|e| KipError::internal_error(e.to_string()))?;
        policy.trust_version = digest(&trust.value)?;
        Ok(policy)
    }
}

impl Session {
    /// Validate a Brain's proposed erasure report against actual owned storage.
    /// This is read-only; it never turns a proposed state into a deletion.
    pub async fn validate_erasure_plan(&self, space: &str, plan: &Json) -> Result<(), KipError> {
        self.with_authority(space, async |authority| {
            authority
                .authorize(Permission::Purge, &ResourceContext::default(), &self.auth)
                .into_result()?;
            let tx = crate::tx::Transaction::inspection(
                &self.nexus.store,
                space,
                authority,
                (*self.auth).clone(),
            )
            .await?;
            tx.validate_erasure_plan(plan).await
        })
        .await
    }

    /// Store immutable replay/rule/source content. Read and erasure authorization
    /// follows every material source; reusing a digest cannot weaken that binding.
    pub async fn put_artifact(
        &self,
        space: &str,
        content: Json,
        source_refs: Vec<String>,
    ) -> Result<anda_kip::cognitive::ArtifactPin, KipError> {
        let content_digest = digest(&content)?;
        let artifact_ref = format!("kip:artifact:{}", content_digest);
        let pin = anda_kip::cognitive::ArtifactPin {
            artifact_ref: artifact_ref.clone(),
            content_digest: content_digest.clone(),
        };
        let permission = if source_refs.is_empty() {
            Permission::ManagePolicy
        } else {
            Permission::Derive
        };
        self.governed(space,permission,async || {
            let authority=self.effective_authority(space).await?;
            let mut sources=source_refs; sources.sort(); sources.dedup();
            for reference in &sources {
                let row=self.nexus.store.get_element(reference.parse()?).await?;
                if row.space()!=space || row.state()!=state::ACTIVE { return Err(KipError::not_found_or_not_visible("artifact material input unavailable")); }
                let visibility=authority.may_read(&row,&self.auth).filter(|v|v.content && v.constraints.fields.is_empty()).ok_or_else(||KipError::not_found_or_not_visible("artifact material input unavailable"))?;
                let _=visibility;
            }
            let key=format!("artifact/{artifact_ref}");
            let value=json!({"state":"available","content":content,"content_digest":content_digest,"source_refs":sources});
            if let Some(old)=self.nexus.store.control_at(space,&key,u64::MAX).await? {
                if old.value!=value { return Err(KipError::constraint_violation("artifact identity already has a different material binding or erasure tombstone")); }
            } else {
                self.nexus.store.publish_control(space,&key,"artifact",0,value,json!({"principal_id":self.auth.principal_id})).await?;
            }
            Ok(pin)
        }).await
    }

    pub async fn read_artifact(
        &self,
        space: &str,
        pin: &anda_kip::cognitive::ArtifactPin,
    ) -> Result<Json, KipError> {
        let _guard = self.nexus.read_guard().await?;
        let authority = self.effective_authority(space).await?;
        let row = self
            .nexus
            .store
            .control_at(space, &format!("artifact/{}", pin.artifact_ref), u64::MAX)
            .await?
            .ok_or_else(|| KipError::not_found_or_not_visible("artifact unavailable"))?;
        let sources = row.value["source_refs"]
            .as_array()
            .ok_or_else(|| KipError::not_found_or_not_visible("artifact binding unavailable"))?;
        if sources.is_empty() {
            authority
                .authorize(
                    Permission::ReadGovernanceHistory,
                    &ResourceContext::default(),
                    &self.auth,
                )
                .into_result()?;
        }
        for source in sources {
            let element = self
                .nexus
                .store
                .get_element(source.as_str().unwrap_or("").parse()?)
                .await?;
            if !authority
                .may_read(&element, &self.auth)
                .is_some_and(|v| v.content && v.constraints.fields.is_empty())
            {
                return Err(KipError::not_found_or_not_visible("artifact unavailable"));
            }
        }
        self.nexus
            .store
            .artifact_value(space, &serde_json::to_value(pin).unwrap())
            .await
    }

    pub async fn set_evaluation_policy(
        &self,
        space: &str,
        expected_version: u64,
        policy: anda_kip::cognitive::EvaluationPolicy,
    ) -> Result<ControlRecordRow, KipError> {
        if policy.id.is_empty()
            || policy.version.is_empty()
            || policy.minimum_independent_attempts < 2
            || policy.observers.iter().any(|o| {
                o.control_domain.is_empty()
                    || o.configuration_digest.is_empty()
                    || o.principal_id.is_empty()
            })
        {
            return Err(KipError::constraint_violation(
                "incomplete evaluation policy",
            ));
        }
        if policy.observer_control_digest
            != digest(&serde_json::to_value(&policy.observers).unwrap())?
        {
            return Err(KipError::new(
                KipErrorCode::DigestMismatch,
                "observer control digest mismatch",
            ));
        }
        self.governed(space, Permission::ManagePolicy, async || {
            self.nexus
                .store
                .publish_control(
                    space,
                    &format!("evaluation_policy/{}", policy.id),
                    "policy",
                    expected_version,
                    serde_json::to_value(policy).unwrap(),
                    json!({"principal_id":self.auth.principal_id}),
                )
                .await
        })
        .await
    }
    /// Withdraw a resolution, preserving old tuples and returning the affected
    /// write set. Exact supplied references are evidence, never reconstructed intent.
    pub async fn withdraw_identity(
        &self,
        space: &str,
        decision_id: &str,
        expected_identity_version: u64,
        reason_evidence: Vec<String>,
    ) -> Result<Json, KipError> {
        self.governed(space,Permission::MergeIdentity,async || {
            let store=&self.nexus.store;
            let authority=self.effective_authority(space).await?;
            let mut decision=store.control_at(space,decision_id,u64::MAX).await?.ok_or_else(|| KipError::not_found_or_not_visible("identity decision unavailable"))?;
            let current=authority.space.policies["_kip_identity_changes"].as_array().into_iter().flatten().filter_map(Json::as_u64).max().unwrap_or(0);
            if current != expected_identity_version { return Err(KipError::version_conflict("identity version changed")); }
            if decision.kind != "identity" || decision.value["status"] != "active" { return Err(KipError::constraint_violation("decision is not an active identity resolution")); }
            if reason_evidence.is_empty() { return Err(KipError::constraint_violation("identity withdrawal requires reason Evidence")); }
            for reference in &reason_evidence {
                let row=store.get_element(reference.parse()?).await?;
                if !matches!(row,crate::store::Element::Evidence(_)) || row.space()!=space { return Err(KipError::not_found_or_not_visible("reason Evidence unavailable")); }
                authority.authorize(Permission::Read,&ResourceContext::of_element(&row),&self.auth).into_result()?;
            }
            let source=decision.value["source"].as_str().unwrap_or("").parse::<crate::ElementId>()?;
            let target=decision.value["target"].as_str().unwrap_or("");
            let old=store.get_element(source).await?;
            let crate::store::Element::Concept(old_row)=&old else { return Err(KipError::constraint_violation("identity source is not a Concept")); };
            if old_row.merged_into != target { return Err(KipError::version_conflict("resolution no longer current")); }
            // Removing an edge cannot introduce a cycle. Reopening a source can
            // expose identity collisions, which must be checked before staging.
            for id in store.concepts().query_all_ids(crate::store::eq_field("space",anda_db_schema::Fv::Text(space.into()))).await.map_err(crate::error::db_error)? {
                let row:ConceptRow=store.concepts().get_as(id).await.map_err(crate::error::db_error)?;
                if id == source.seq || row.state != state::ACTIVE { continue; }
                if (!old_row.canonical_id.is_empty() && row.canonical_id==old_row.canonical_id)
                    || (!old_row.key.is_empty() && row.key==old_row.key && crate::schema::lineage_of(&row.schema_ref)==crate::schema::lineage_of(&old_row.schema_ref)) {
                    return Err(KipError::new(KipErrorCode::IdentityConflict,"withdrawal conflicts with an active key or canonical identity"));
                }
            }
            let mut tx=crate::tx::Transaction::begin(store,space,json!({"principal_id":self.auth.principal_id}),false,authority.clone(),(*self.auth).clone()).await?;
            let crate::store::Element::Concept(row)=tx.load(source).await? else { unreachable!() };
            row.merged_into.clear(); row.state=state::ACTIVE.into();
            tx.mark_changed(source,anda_kip::ChangeOp::Update); tx.identity_changed=true;
            let mut affected=std::collections::BTreeMap::<String,Json>::new();
            for id in store.element_versions().query_all_ids(crate::store::eq_field("space",anda_db_schema::Fv::Text(space.into()))).await.map_err(crate::error::db_error)? {
                let version:ElementVersionRow=store.element_versions().get_as(id).await.map_err(crate::error::db_error)?;
                if version.seq < decision.seq { continue; }
                for reference in version.row["origin"]["_kip_runtime"]["input_references"].as_array().into_iter().flatten() {
                    if reference["resolved"]==target || reference["supplied"]==source.to_string() {
                        affected.insert(version.element.clone(),json!({"ref":version.element,"status":"needs_review","ambiguous":reference["supplied"]==reference["resolved"],"supplied":reference["supplied"],"resolved":reference["resolved"]}));
                    }
                }
            }
            for (id,value) in &affected {
                let key=format!("identity_review/{id}");
                let version=store.control_at(space,&key,u64::MAX).await?.map_or(1,|r|r.version+1);
                tx.control_effects.push(ControlRecordRow{_id:0,record_id:format!("{}:{key}",tx.cx.tx_id),space:space.into(),key,seq:tx.cx.seq,version,kind:"identity".into(),value:value.clone(),origin:tx.cx.origin.clone()});
            }
            decision._id=0; decision.record_id=format!("{}:{decision_id}",tx.cx.tx_id); decision.seq=tx.cx.seq; decision.version+=1;
            decision.value["status"]=json!("withdrawn"); decision.value["reason_evidence"]=json!(reason_evidence); decision.value["withdrawn_at_version"]=json!(tx.cx.seq);
            tx.control_effects.push(decision);
            let outcome=tx.commit(JournalEntry::default()).await?;
            let mut visible=vec![]; let mut complete=true;
            for (id,item) in affected {
                let row=store.get_element(id.parse()?).await?;
                if authority.may_read(&row,&self.auth).is_some_and(|v|v.content) { visible.push(item); } else { complete=false; }
            }
            Ok(json!({"decision_id":decision_id,"identity_version":outcome.receipt.space_seq,"review_set":visible,"complete":complete,"receipt":outcome.receipt}))
        }).await
    }
    /// Replace one named policy with a CAS; prior versions remain replayable.
    pub async fn set_projection_policy(
        &self,
        space: &str,
        name: &str,
        expected_version: u64,
        settings: Map<String, Json>,
    ) -> Result<ControlRecordRow, KipError> {
        if !matches!(name, "baseline" | "forecast") {
            return Err(KipError::constraint_violation(
                "policy name must be baseline or forecast",
            ));
        }
        let mut settings = settings;
        settings.insert("policy".into(), Json::String(name.into()));
        let mut policy = Policy::from_settings(&settings)?;
        policy.explicit_selection = false;
        self.governed(space, Permission::ManagePolicy, async || {
            let store = &self.nexus.store;
            let mut config = store
                .control_at(space, "projection", u64::MAX)
                .await?
                .ok_or_else(|| KipError::internal_error("missing projection genesis"))?
                .value;
            config[name] = serde_json::to_value(policy).unwrap();
            store
                .publish_control(
                    space,
                    "projection",
                    "policy",
                    expected_version,
                    config,
                    json!({"principal_id":self.auth.principal_id}),
                )
                .await
        })
        .await
    }

    /// Trust weights are protected runtime configuration, never cognition.
    pub async fn set_trust(
        &self,
        space: &str,
        expected_version: u64,
        weights: std::collections::BTreeMap<String, f64>,
        default_weight: f64,
    ) -> Result<ControlRecordRow, KipError> {
        if std::iter::once(&default_weight)
            .chain(weights.values())
            .any(|v| !v.is_finite() || !(0.0..=1.0).contains(v))
        {
            return Err(KipError::constraint_violation(
                "trust weights must be in [0,1]",
            ));
        }
        self.governed(space, Permission::ManageTrust, async || {
            let mut value = json!({"weights":weights,"default_weight":default_weight});
            if let Some(old) = self
                .nexus
                .store
                .control_at(space, "trust", u64::MAX)
                .await?
                && let Some(rules) = old.value.get("rules")
            {
                value["rules"] = rules.clone();
            }
            self.nexus
                .store
                .publish_control(
                    space,
                    "trust",
                    "trust",
                    expected_version,
                    value,
                    json!({"principal_id":self.auth.principal_id}),
                )
                .await
        })
        .await
    }

    pub async fn read_control(
        &self,
        space: &str,
        key: &str,
        seq: Option<u64>,
    ) -> Result<Option<ControlRecordRow>, KipError> {
        if key.starts_with("internal/") {
            return Err(KipError::not_authorized(
                "internal control checkpoints are not caller-visible",
            ));
        }
        if key.starts_with("artifact/") {
            return Err(KipError::not_authorized(
                "artifact content is read through read_artifact with material-source authorization",
            ));
        }
        let _guard = self.nexus.read_guard().await?;
        let authority = self.effective_authority(space).await?;
        authority
            .authorize(
                Permission::ReadGovernanceHistory,
                &ResourceContext::default(),
                &self.auth,
            )
            .into_result()?;
        let row = self
            .nexus
            .store
            .control_at(space, key, seq.unwrap_or(u64::MAX))
            .await?;
        if let Some(row) = &row {
            crate::attention::authorize_control_read(self, &authority, space, row).await?;
        }
        Ok(row)
    }
}
