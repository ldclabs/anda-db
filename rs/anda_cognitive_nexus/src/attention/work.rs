use super::*;

pub(super) async fn load(
    session: &Session,
    authority: &EffectiveAuthority,
    space: &str,
    reference: &str,
) -> Result<WakeRecord, KipError> {
    authority
        .authorize(
            Permission::Maintain,
            &ResourceContext::default(),
            &session.auth,
        )
        .into_result()?;
    let row = session
        .nexus
        .store
        .control_at(space, reference, u64::MAX)
        .await?
        .ok_or_else(|| KipError::not_found_or_not_visible("wake unavailable"))?;
    if row.kind != "wake" {
        return Err(invalid("not a wake record"));
    }
    let wake: WakeRecord =
        serde_json::from_value(row.value).map_err(|_| invalid("corrupt wake record"))?;
    if wake.wake_ref != reference
        || wake.version != row.version
        || !matches!(wake.format.as_str(), FORMAT | CONTINUATION)
        || (wake.format == FORMAT && (wake.parent_ref.is_some() || wake.continuation_key.is_some()))
        || (wake.format == CONTINUATION
            && (wake.parent_ref.is_none() || wake.continuation_key.is_none()))
    {
        return Err(invalid("invalid wake identity/version"));
    }
    for reference in [&wake.fire.watch_ref, &wake.fire_activity_ref] {
        let element = session.nexus.store.get_element(reference.parse()?).await?;
        if element.space() != space || element.state() != crate::store::rows::state::ACTIVE {
            return Err(KipError::not_found_or_not_visible(
                "wake source unavailable",
            ));
        }
        authority
            .authorize(
                Permission::Read,
                &ResourceContext::of_element(&element),
                &session.auth,
            )
            .into_result()?;
        if !authority
            .may_read(&element, &session.auth)
            .is_some_and(|v| v.content && v.constraints.fields.is_empty())
        {
            return Err(KipError::not_found_or_not_visible(
                "wake source is not fully visible",
            ));
        }
    }
    Ok(wake)
}

async fn current(
    session: &Session,
    authority: &EffectiveAuthority,
    space: &str,
    wake: &WakeRecord,
) -> Result<(), KipError> {
    let store = &session.nexus.store;
    let cfg = store
        .control_at(space, CONFIG, u64::MAX)
        .await?
        .ok_or_else(|| conflict("binding_unavailable"))?;
    if cfg.value
        != json!(AttentionConfig {
            scope: wake.scope.clone(),
            pins: wake.pins.clone()
        })
    {
        return Err(conflict("basis_changed"));
    }
    let watch = store.get_element(wake.fire.watch_ref.parse()?).await?;
    let view = crate::view::render(&watch);
    if view["attributes"]["status"] != "fired"
        || view["facets"][format!("{PROFILE}WatchState")]["arm_generation"]
            != wake.fire.arm_generation
    {
        return Err(conflict("generation_conflict"));
    }
    let checkpoint = store
        .control_at(
            space,
            &checkpoint_key(&wake.fire.watch_ref, wake.fire.arm_generation),
            u64::MAX,
        )
        .await?
        .ok_or_else(|| conflict("history_gap"))?;
    let checkpoint: WatchCheckpoint = serde_json::from_value(checkpoint.value)
        .map_err(|_| invalid("corrupt Watch checkpoint"))?;
    if checkpoint.format != "nexus:watch-checkpoint-v1"
        || checkpoint.watch_ref != wake.fire.watch_ref
        || checkpoint.arm_generation != wake.fire.arm_generation
    {
        return Err(invalid("unsupported or mismatched Watch checkpoint"));
    }
    let cx = Context::open(store, space, None, None, authority, &session.auth).await?;
    if checkpoint.basis != basis(&cx) {
        return Err(conflict("basis_changed"));
    }
    Ok(())
}

fn live(session: &Session, wake: &WakeRecord, now: u64) -> Result<(), KipError> {
    if !matches!(&wake.state,WakeState::Running{lease} if lease.owner==session.auth.principal_id && lease.expires_at_ms>now)
    {
        return Err(conflict("lease_lost"));
    }
    Ok(())
}

fn expiry(value: &str, now: u64) -> Result<u64, KipError> {
    let expiry = crate::time::parse(value)?.timestamp_millis();
    if expiry < 0 || expiry as u64 <= now || expiry as u64 > now.saturating_add(MAX_LEASE_MS) {
        return Err(invalid(
            "wake lease must expire within five real-time minutes",
        ));
    }
    Ok(expiry as u64)
}

enum Action<'a> {
    Claim(&'a str),
    Renew(&'a str),
    Block(&'a WakeRetry),
    Resume,
    ResumeVerified(&'a str, &'a RuntimePin),
    Cancel(&'a str),
    Finish(&'a str, &'a Map<String, Json>, &'a [WakeContinuation]),
}

impl Session {
    /// Durably record dispatch intent before allowing external I/O. A repeated
    /// non-idempotent dispatch returns lookup/unknown, never a fresh send permit.
    /// The executor must independently enforce actual target-system authority.
    #[allow(clippy::too_many_arguments)]
    pub async fn begin_wake_dispatch(
        &self,
        space: &str,
        reference: &str,
        expected: u64,
        fence: u64,
        attempt_ref: &str,
        supports_idempotency: bool,
        supports_outcome_lookup: bool,
    ) -> Result<Json, KipError> {
        self.with_authority(space, async |authority| {
            let store = &self.nexus.store;
            let wake = load(self, &authority, space, reference).await?;
            if wake.version != expected {
                return Err(conflict("version_conflict"));
            }
            if wake.fence != fence {
                return Err(conflict("lease_lost"));
            }
            live(self, &wake, crate::tx::now_ms())?;
            current(self, &authority, space, &wake).await?;
            if wake.pins.binding.is_none() {
                return Err(KipError::unsupported_capability(
                    "wake dispatch requires a registered host binding",
                ));
            }
            // Shared with the existing SleepTask dispatch path: native act,
            // exact revisions, executable authority, policy and dependencies.
            let attempt = self.check_attention_attempt(space, attempt_ref).await?;
            let decision = store
                .get_element(attempt["decision_ref"].as_str().unwrap_or("").parse()?)
                .await?;
            let Element::Activity(decision) = decision else {
                return Err(invalid("attempt decision is not Activity"));
            };
            if !decision.inputs.iter().any(|r| {
                r.as_str()
                    .or_else(|| r["id"].as_str())
                    .is_some_and(|r| r == wake.fire_activity_ref || r == wake.fire.watch_ref)
            }) {
                return Err(invalid("dispatch decision must name its Watch/fire input"));
            }
            let request = json!({"wake_ref":reference,"attempt_ref":attempt_ref,"supports_idempotency":supports_idempotency,"supports_outcome_lookup":supports_outcome_lookup,"binding":wake.pins.binding});
            let dispatch_ref = runtime_ref(
                "dispatch",
                &json!({"scope":wake.scope,"attempt_id":attempt["attempt_id"]}),
            )?;
            let old = store.control_at(space, &dispatch_ref, u64::MAX).await?;
            let lookup_observer =
                super::lookup::observer(store, space, wake.pins.binding.as_ref().unwrap()).await?;
            if supports_outcome_lookup && lookup_observer.is_null() {
                return Err(KipError::unsupported_capability(
                    "wake dispatch outcome lookup requires a registered observer",
                ));
            }
            if let Some(old) = &old
                && old.value["lookup_observer"] != lookup_observer
            {
                return Err(conflict("lookup_observer_changed"));
            }
            if let Some(old) = &old
                && old.value["request"] != request
            {
                return Err(invalid("idempotency_conflict"));
            }
            let action = match old.as_ref().and_then(|r| r.value["state"].as_str()) {
                Some("completed") => "done",
                Some("ready") => "dispatch",
                Some("outcome_unknown") => "outcome_unknown",
                Some(_) if supports_idempotency => "dispatch",
                Some(_) if supports_outcome_lookup => "lookup",
                Some(_) => "outcome_unknown",
                None => "dispatch",
            };
            let version = old.as_ref().map_or(0, |r| r.version);
            let dispatch_at = crate::time::now();
            let value = json!({
                "request": request,
                "state": if action == "done" { "completed" } else if action == "outcome_unknown" { "outcome_unknown" } else { "dispatching" },
                "attempt_id": attempt["attempt_id"],
                "fencing_token": fence,
                "outcome_ref": old.as_ref().map(|r| r.value["outcome_ref"].clone()),
                "lookup_observer": lookup_observer,
                "first_dispatch_at": old.as_ref().and_then(|r| r.value["first_dispatch_at"].as_str()).unwrap_or(&dispatch_at),
                "last_dispatch_at": if action == "dispatch" { json!(dispatch_at) } else { old.as_ref().map(|r| r.value["last_dispatch_at"].clone()).unwrap_or(Json::Null) },
                "lookup_receipt_ref": old.as_ref().map(|r| r.value["lookup_receipt_ref"].clone()),
            });
            let mut tx = Transaction::begin(
                store,
                space,
                json!({"principal_id":self.auth.principal_id}),
                false,
                authority,
                (*self.auth).clone(),
            )
            .await?;
            tx.attention_leases
                .push((reference.into(), expected, fence));
            stage_control(
                store,
                &mut tx,
                &dispatch_ref,
                version,
                "dispatch",
                value.clone(),
            )
            .await?;
            if old.is_none() {
                let index_key = format!("attention/dispatches/{reference}");
                let index = store.control_at(space, &index_key, u64::MAX).await?;
                let mut refs = index
                    .as_ref()
                    .and_then(|r| r.value.as_array())
                    .cloned()
                    .unwrap_or_default();
                if refs.len() >= 128 {
                    return Err(invalid("wake dispatch budget exhausted"));
                }
                refs.push(json!(dispatch_ref));
                stage_control(
                    store,
                    &mut tx,
                    &index_key,
                    index.map_or(0, |r| r.version),
                    "runtime",
                    json!(refs),
                )
                .await?;
            }
            let call = json!({"operation":"begin_wake_dispatch","dispatch_ref":dispatch_ref,"version":version,"request":request,"fence":fence});
            commit(store,tx,request_key(&self.auth.principal_id,&call)?,digest(&call)?,json!({"action":action,"dispatch_ref":dispatch_ref,"idempotency_key":attempt["attempt_id"],"version":next(version)?,"intent":value})).await
        })
        .await
    }

    /// Independent terminal observation resolves the same dispatch identity,
    /// including after cancellation. Unknown evidence never becomes success.
    pub async fn reconcile_wake_dispatch(
        &self,
        space: &str,
        dispatch_ref: &str,
        expected: u64,
        outcome_ref: &str,
    ) -> Result<Json, KipError> {
        self.governed(space, Permission::RecordOutcome, async || {
            let store = &self.nexus.store;
            let authority = self.effective_authority(space).await?;
            let old = store
                .control_at(space, dispatch_ref, u64::MAX)
                .await?
                .ok_or_else(|| KipError::not_found_or_not_visible("dispatch unavailable"))?;
            if old.kind != "dispatch" || !dispatch_ref.starts_with("dispatch/v1/") {
                return Err(invalid("not a wake dispatch"));
            }
            let outcome = store.get_element(outcome_ref.parse()?).await?;
            authority
                .authorize(
                    Permission::Read,
                    &ResourceContext::of_element(&outcome),
                    &self.auth,
                )
                .into_result()?;
            let view = crate::view::render(&outcome);
            let record = &view["facets"][format!("{PROFILE}OutcomeRecord")];
            if outcome.space() != space
                || record["attempt_ref"] != old.value["request"]["attempt_ref"]
                || record["terminal"] != true
            {
                return Err(invalid("observation does not close this attempt"));
            }
            let attempt = store
                .get_element(record["attempt_ref"].as_str().unwrap_or("").parse()?)
                .await?;
            authority
                .authorize(
                    Permission::Read,
                    &ResourceContext::of_element(&attempt),
                    &self.auth,
                )
                .into_result()?;
            if old.value["outcome_ref"] == outcome_ref {
                return Ok(old.value);
            }
            if old.version != expected {
                return Err(conflict("version_conflict"));
            }
            if old.value["outcome_ref"].is_string() {
                return Err(invalid("conflicting terminal observation"));
            }
            let mut value = old.value;
            value["state"] = json!(if record["outcome_status"] == "unknown" {
                "outcome_unknown"
            } else {
                "completed"
            });
            value["outcome_ref"] = json!(outcome_ref);
            Ok(store
                .publish_control(
                    space,
                    dispatch_ref,
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

    /// Read one protected work item; reading neither claims nor renews it.
    pub async fn read_wake(&self, space: &str, reference: &str) -> Result<WakeRecord, KipError> {
        let _guard = self.nexus.read_guard().await?;
        let authority = self.effective_authority(space).await?;
        load(self, &authority, space, reference).await
    }

    pub async fn claim_wake(
        &self,
        space: &str,
        reference: &str,
        expected: u64,
        fence: u64,
        expires_at: &str,
    ) -> Result<Json, KipError> {
        self.update_wake(space, reference, expected, fence, Action::Claim(expires_at))
            .await
    }

    pub async fn renew_wake(
        &self,
        space: &str,
        reference: &str,
        expected: u64,
        fence: u64,
        expires_at: &str,
    ) -> Result<Json, KipError> {
        self.update_wake(space, reference, expected, fence, Action::Renew(expires_at))
            .await
    }

    pub async fn block_wake(
        &self,
        space: &str,
        reference: &str,
        expected: u64,
        fence: u64,
        retry: WakeRetry,
    ) -> Result<Json, KipError> {
        self.update_wake(space, reference, expected, fence, Action::Block(&retry))
            .await
    }

    /// Timed retries only. Arbitrary prose/on_change proof needs a registered
    /// condition verifier and is explicitly unavailable in this release.
    pub async fn resume_wake(
        &self,
        space: &str,
        reference: &str,
        expected: u64,
        fence: u64,
    ) -> Result<Json, KipError> {
        let verification = self
            .verified_resume(space, reference, expected, fence)
            .await?;
        match verification {
            Some((condition, pin)) => {
                self.update_wake(
                    space,
                    reference,
                    expected,
                    fence,
                    Action::ResumeVerified(&condition, &pin),
                )
                .await
            }
            None => {
                self.update_wake(space, reference, expected, fence, Action::Resume)
                    .await
            }
        }
    }

    pub async fn cancel_wake(
        &self,
        space: &str,
        reference: &str,
        expected: u64,
        fence: u64,
        reason: &str,
    ) -> Result<Json, KipError> {
        self.update_wake(space, reference, expected, fence, Action::Cancel(reason))
            .await
    }

    /// One bounded KML output block, continuation wakes and the parent's
    /// terminal receipt commit together. No callbacks or external I/O run here.
    #[allow(clippy::too_many_arguments)]
    pub async fn finish_wake(
        &self,
        space: &str,
        reference: &str,
        expected: u64,
        fence: u64,
        command: &str,
        parameters: Map<String, Json>,
        continuations: Vec<WakeContinuation>,
    ) -> Result<Json, KipError> {
        self.update_wake(
            space,
            reference,
            expected,
            fence,
            Action::Finish(command, &parameters, &continuations),
        )
        .await
    }

    async fn update_wake(
        &self,
        space: &str,
        reference: &str,
        expected: u64,
        fence: u64,
        action: Action<'_>,
    ) -> Result<Json, KipError> {
        self.with_authority(space, async |authority| {
            let store = &self.nexus.store;
            let mut wake = load(self, &authority, space, reference).await?;
            let (operation, body) = match &action {
                Action::Claim(t) => (
                    "claim_wake",
                    json!({"expires_at":crate::time::normalize(t,"lease expiry")?}),
                ),
                Action::Renew(t) => (
                    "renew_wake",
                    json!({"expires_at":crate::time::normalize(t,"lease expiry")?}),
                ),
                Action::Block(r) => ("block_wake", json!(r)),
                Action::Resume | Action::ResumeVerified(..) => ("resume_wake", Json::Null),
                Action::Cancel(reason) => ("cancel_wake", json!({"reason":reason})),
                Action::Finish(command, parameters, continuations) => (
                    "finish_wake",
                    json!({"command":command,"parameters":parameters,"continuations":continuations}),
                ),
            };
            let operation_key = runtime_ref(
                "operation",
                &json!({"domain":"anda-brain:operation-v1","scope":wake.scope,"wake_ref":reference,"operation":operation,"step":expected}),
            )?;
            let key = format!(
                "attention\u{1f}{}\u{1f}{operation_key}",
                self.auth.principal_id
            );
            let request_digest = digest(&json!({"request":{"fence":fence,"body":body},"pins":wake.pins}))?;
            if let Some(result) = replay(store, space, &key, &request_digest).await? {
                for output in result["outputs"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(Json::as_str)
                {
                    if let Ok(id) = output.parse::<crate::ElementId>() {
                        let row = store.get_element(id).await?;
                        authority
                            .authorize(
                                Permission::Read,
                                &ResourceContext::of_element(&row),
                                &self.auth,
                            )
                            .into_result()?;
                    }
                }
                return Ok(result);
            }
            if wake.version != expected {
                return Err(conflict("version_conflict"));
            }
            if wake.fence != fence {
                return Err(conflict("lease_lost"));
            }
            if matches!(
                wake.state,
                WakeState::Completed { .. } | WakeState::Cancelled { .. }
            ) {
                return Err(conflict("version_conflict"));
            }
            if !matches!(action, Action::Cancel(_) | Action::Block(_)) {
                current(self, &authority, space, &wake).await?;
            }
            let now = crate::tx::now_ms();
            let receipt_ref = operation_key.replacen("operation/", "receipt/", 1);
            let mut outputs = Vec::<String>::new();
            let verification = match &action {
                Action::ResumeVerified(condition, pin) => {
                    Some(json!({"condition_digest":condition,"verifier":pin}))
                }
                _ => None,
            };
            match &action {
                Action::Claim(t) => {
                    match &wake.state {
                        WakeState::Pending { not_before_ms } if *not_before_ms <= now => {}
                        WakeState::Running { lease } if lease.expires_at_ms <= now => {}
                        _ => return Err(conflict("not_ready")),
                    }
                    wake.fence = next(wake.fence)?;
                    wake.state = WakeState::Running {
                        lease: WakeLease {
                            owner: self.auth.principal_id.clone(),
                            expires_at_ms: expiry(t, now)?,
                        },
                    };
                }
                Action::Renew(t) => {
                    live(self, &wake, now)?;
                    let expires = expiry(t, now)?;
                    let WakeState::Running { lease } = &mut wake.state else {
                        unreachable!()
                    };
                    if expires < lease.expires_at_ms {
                        return Err(invalid("lease renewal cannot shorten expiry"));
                    }
                    lease.expires_at_ms = expires;
                }
                Action::Block(retry) => {
                    live(self, &wake, now)?;
                    if !matches!(
                        retry.reason.as_str(),
                        "basis_changed"
                            | "history_gap"
                            | "budget_exhausted"
                            | "binding_unavailable"
                            | "semantic_unknown"
                            | "outcome_unknown"
                    ) {
                        return Err(invalid("invalid blocked reason"));
                    }
                    match &retry.resume {
                        WakeResume::At { not_before_ms }
                            if *not_before_ms > now
                                && *not_before_ms <= anda_kip::MAX_SAFE_INTEGER
                                && retry.reason != "outcome_unknown" => {}
                        WakeResume::OnChange { condition_digest } if valid_digest(condition_digest) => {}
                        _ => {
                            return Err(invalid(
                                "blocked work needs an explicit bounded resume condition",
                            ));
                        }
                    }
                    wake.state = WakeState::Blocked {
                        retry: (*retry).clone(),
                    };
                }
                Action::Resume | Action::ResumeVerified(..) => {
                    match &wake.state {
                        WakeState::Blocked {
                            retry:
                                WakeRetry {
                                    resume: WakeResume::At { not_before_ms },
                                    ..
                                },
                        } if *not_before_ms <= now => {}
                        WakeState::Blocked {
                            retry:
                                WakeRetry {
                                    resume: WakeResume::OnChange { condition_digest },
                                    ..
                                },
                        } if matches!(&action,Action::ResumeVerified(verified,_) if condition_digest==*verified) =>
                            {}
                        WakeState::Blocked {
                            retry:
                                WakeRetry {
                                    resume: WakeResume::OnChange { .. },
                                    ..
                                },
                        } => {
                            return Err(KipError::unsupported_capability(
                                "on_change wake recovery requires a registered condition verifier",
                            ));
                        }
                        _ => return Err(conflict("not_ready")),
                    }
                    wake.state = WakeState::Pending { not_before_ms: now };
                }
                Action::Cancel(reason) => {
                    if !bounded(reason) {
                        return Err(invalid("bounded cancellation reason required"));
                    }
                    wake.fence = next(wake.fence)?;
                    wake.state = WakeState::Cancelled {
                        receipt_ref: receipt_ref.clone(),
                    };
                }
                Action::Finish(command, _, continuations) => {
                    live(self, &wake, now)?;
                    if command.len() > 65_536 || continuations.len() > 16 {
                        return Err(invalid("wake completion exceeds output budget"));
                    }
                    if let Some(index) = store
                        .control_at(
                            space,
                            &format!("attention/dispatches/{reference}"),
                            u64::MAX,
                        )
                        .await?
                    {
                        for key in index
                            .value
                            .as_array()
                            .into_iter()
                            .flatten()
                            .filter_map(Json::as_str)
                        {
                            if store
                                .control_at(space, key, u64::MAX)
                                .await?
                                .is_none_or(|r| r.value["state"] != "completed")
                            {
                                return Err(conflict("outcome_unknown"));
                            }
                        }
                    }
                    wake.state = WakeState::Completed {
                        receipt_ref: receipt_ref.clone(),
                    };
                }
            }
            wake.version = next(wake.version)?;
            let mut tx = Transaction::begin(
                store,
                space,
                json!({"principal_id":self.auth.principal_id}),
                false,
                authority.clone(),
                (*self.auth).clone(),
            )
            .await?;
            if matches!(
                action,
                Action::Renew(_) | Action::Block(_) | Action::Finish(..)
            ) {
                tx.attention_leases
                    .push((reference.into(), expected, fence));
            }
            if let Action::Finish(command, parameters, continuations) = action {
                if !command.trim().is_empty() {
                    let anda_kip::Command::Kml(statement) = anda_kip::parse_kip(command)? else {
                        return Err(invalid("wake outputs must be one KML block"));
                    };
                    if statement.clauses.len() > 128 {
                        return Err(invalid("wake completion exceeds clause budget"));
                    }
                    // Apply the same base permissions as the normal write lane.
                    // Requests needing interactive approvals are refused here;
                    // a saved wake does not supply approval authority.
                    for permission in crate::governance::gate::kml_permissions(&statement) {
                        authority
                            .authorize(permission, &ResourceContext::default(), &self.auth)
                            .into_result()?;
                    }
                    if let Err(error) = crate::kml::plan(
                        store,
                        &mut tx,
                        &statement,
                        Some(parameters),
                        &anda_kip::Operation::new(command),
                    )
                    .await
                    {
                        tx.abort().await;
                        return Err(error);
                    }
                    outputs.extend(tx.handles().values().map(ToString::to_string));
                }
                let mut keys = std::collections::BTreeSet::new();
                for child in continuations {
                    if child.key.is_empty()
                        || child.key.len() > 128
                        || !keys.insert(&child.key)
                        || child.not_before_ms > anda_kip::MAX_SAFE_INTEGER
                    {
                        tx.abort().await;
                        return Err(invalid("invalid/duplicate continuation key"));
                    }
                    let child_ref = runtime_ref(
                        "wake",
                        &json!({"domain":"anda-brain:wake-continuation-v1","scope":wake.scope,"parent_ref":reference,"operation_key":operation_key,"key":child.key}),
                    )?;
                    let mut child_wake = wake.clone();
                    child_wake.format = CONTINUATION.into();
                    child_wake.wake_ref = child_ref.clone();
                    child_wake.parent_ref = Some(reference.into());
                    child_wake.continuation_key = Some(child.key.clone());
                    child_wake.version = 1;
                    child_wake.fence = 0;
                    child_wake.state = WakeState::Pending {
                        not_before_ms: child.not_before_ms,
                    };
                    stage_control(store, &mut tx, &child_ref, 0, "wake", json!(child_wake)).await?;
                    outputs.push(child_ref);
                }
            }
            if outputs.len() > 128 {
                tx.abort().await;
                return Err(invalid("wake completion exceeds output count"));
            }
            // Outputs cannot commit if the lease expired during bounded planning.
            if operation == "finish_wake" {
                let before = load(self, &authority, space, reference).await?;
                live(self, &before, crate::tx::now_ms())?;
                current(self, &authority, space, &before).await?;
            }
            stage_control(store, &mut tx, reference, expected, "wake", json!(wake)).await?;
            let receipt = json!({"format":FORMAT,"identity":{"scope":wake.scope,"operation_key":operation_key,"request_digest":request_digest},"pins":wake.pins,"state":{"status":"committed","commit_seq":tx.cx.seq,"outputs":outputs}});
            stage_control(store, &mut tx, &receipt_ref, 0, "runtime", receipt).await?;
            commit(store,tx,key,request_digest,json!({"wake":wake,"receipt_ref":receipt_ref,"outputs":outputs,"resume_verification":verification})).await
        })
        .await
    }
}
