use super::*;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DispatchLookupObserver {
    pub binding: RuntimePin,
    pub principal_id: String,
    pub configuration_digest: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DispatchLookupStatus {
    NotStarted,
    Running,
    Finished,
    Unknown,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DispatchLookup {
    pub observation_key: String,
    pub observed_at: String,
    pub configuration_digest: String,
    pub status: DispatchLookupStatus,
}

pub(super) fn observer_key(binding: &RuntimePin) -> Result<String, KipError> {
    runtime_ref("attention-lookup-observer", &json!(binding))
}

pub(super) async fn observer(
    store: &Store,
    space: &str,
    binding: &RuntimePin,
) -> Result<Json, KipError> {
    Ok(store
        .control_at(space, &observer_key(binding)?, u64::MAX)
        .await?
        .map(|r| json!({"version":r.version,"observer":r.value}))
        .unwrap_or(Json::Null))
}

impl Session {
    /// Register the authenticated target-system lookup authority before dispatch.
    /// This does not make its Finished status an independent success Outcome.
    pub async fn set_dispatch_lookup_observer(
        &self,
        space: &str,
        expected: u64,
        observer: DispatchLookupObserver,
    ) -> Result<Json, KipError> {
        if !bounded(&observer.binding.id)
            || !valid_digest(&observer.binding.digest)
            || !bounded(&observer.principal_id)
            || !observer.principal_id.starts_with("kip:principal:")
            || observer.principal_id == crate::governance::SYSTEM_PRINCIPAL
            || !valid_digest(&observer.configuration_digest)
        {
            return Err(invalid("invalid direct lookup observer"));
        }
        self.governed(space, Permission::ManagePolicy, async || {
            let key = observer_key(&observer.binding)?;
            let row = self
                .nexus
                .store
                .publish_control(
                    space,
                    &key,
                    "policy",
                    expected,
                    json!(observer),
                    json!({"principal_id":self.auth.principal_id}),
                )
                .await?;
            Ok(json!({"version":row.version,"observer":row.value}))
        })
        .await
    }

    pub async fn reconcile_wake_lookup(
        &self,
        space: &str,
        dispatch_ref: &str,
        expected: u64,
        mut observation: DispatchLookup,
    ) -> Result<Json, KipError> {
        if !bounded(&observation.observation_key) {
            return Err(invalid("bounded observation key required"));
        }
        observation.observed_at =
            crate::time::normalize(&observation.observed_at, "lookup observed_at")?;
        self.governed(space, Permission::RecordOutcome, async || {
            let store = &self.nexus.store;
            let authority = self.effective_authority(space).await?;
            let old = store
                .control_at(space, dispatch_ref, u64::MAX)
                .await?
                .ok_or_else(|| KipError::not_found_or_not_visible("dispatch unavailable"))?;
            if old.kind != "dispatch"
                || !dispatch_ref.starts_with("dispatch/v1/")
                || old.value["request"]["supports_outcome_lookup"] != true
            {
                return Err(invalid("dispatch has no authoritative lookup channel"));
            }
            let retained = &old.value["lookup_observer"];
            let config: DispatchLookupObserver =
                serde_json::from_value(retained["observer"].clone()).map_err(|_| {
                    KipError::unsupported_capability("dispatch did not pin a lookup observer")
                })?;
            if config.principal_id != self.auth.principal_id
                || self.auth.auth_method.is_empty()
                || self.auth.auth_strength == "none"
                || !self.auth.delegation_chain.is_empty()
            {
                return Err(KipError::not_authorized(
                    "directly authenticated lookup observer required",
                ));
            }
            if config.configuration_digest != observation.configuration_digest
                || observer(store, space, &config.binding).await? != *retained
            {
                return Err(conflict("lookup_observer_changed"));
            }
            let attempt_ref = old.value["request"]["attempt_ref"]
                .as_str()
                .ok_or_else(|| invalid("dispatch lacks attempt"))?;
            let attempt = store.get_element(attempt_ref.parse()?).await?;
            if attempt.space() != space {
                return Err(KipError::not_found_or_not_visible("attempt unavailable"));
            }
            authority
                .authorize(
                    Permission::Read,
                    &ResourceContext::of_element(&attempt),
                    &self.auth,
                )
                .into_result()?;
            let identity = json!({
                "operation": "reconcile_wake_lookup",
                "dispatch_ref": dispatch_ref,
                "observation_key": observation.observation_key,
            });
            let key = request_key(&self.auth.principal_id, &identity)?;
            let request_digest =
                digest(&json!({"dispatch_ref": dispatch_ref, "observation": observation}))?;
            if let Some(result) = replay(store, space, &key, &request_digest).await? {
                return Ok(result);
            }
            if old.version != expected {
                return Err(conflict("version_conflict"));
            }
            if old.value["outcome_ref"].is_string() || old.value["state"] == "completed" {
                return Err(invalid("terminal Outcome cannot be reopened by lookup"));
            }
            let last = old.value["last_dispatch_at"]
                .as_str()
                .ok_or_else(|| invalid("dispatch has no retained start time"))?;
            if observation.observed_at.as_str() < last
                || observation.observed_at > crate::time::now()
            {
                return Err(invalid("lookup time is outside the dispatch interval"));
            }
            let receipt_ref = runtime_ref(
                "dispatch-lookup",
                &json!({
                    "scope": space,
                    "principal": self.auth.principal_id,
                    "dispatch_ref": dispatch_ref,
                    "observation_key": observation.observation_key,
                }),
            )?;
            let mut value = old.value;
            value["state"] = json!(match observation.status {
                DispatchLookupStatus::NotStarted => "ready",
                DispatchLookupStatus::Unknown => "outcome_unknown",
                DispatchLookupStatus::Running | DispatchLookupStatus::Finished => "dispatching",
            });
            value["lookup_receipt_ref"] = json!(receipt_ref);
            let mut tx = Transaction::begin(
                store,
                space,
                json!({"principal_id":self.auth.principal_id}),
                false,
                authority,
                (*self.auth).clone(),
            )
            .await?;
            stage_control(
                store,
                &mut tx,
                &receipt_ref,
                0,
                "runtime",
                json!({
                    "format": "nexus:dispatch-lookup-v1",
                    "dispatch_ref": dispatch_ref,
                    "observation": observation,
                    "observer": config,
                }),
            )
            .await?;
            stage_control(
                store,
                &mut tx,
                dispatch_ref,
                expected,
                "dispatch",
                value.clone(),
            )
            .await?;
            commit(
                store,
                tx,
                key,
                request_digest,
                json!({
                    "dispatch_ref": dispatch_ref,
                    "version": next(expected)?,
                    "intent": value,
                    "lookup_receipt_ref": receipt_ref,
                }),
            )
            .await
        })
        .await
    }
}
