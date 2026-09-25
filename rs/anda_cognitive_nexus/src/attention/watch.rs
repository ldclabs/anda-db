use super::evaluation::{self, Mode};
use super::*;
use crate::{runtime::bind_watch_condition, store::planes::PlaneKey, tx::Guard};

fn watch_values(element: &Element) -> Result<(&crate::store::rows::ConceptRow, Json), KipError> {
    let Element::Concept(row) = element else {
        return Err(invalid("target must be a Watch"));
    };
    if row.schema_ref != format!("{PROFILE}Watch") || row.state != crate::store::rows::state::ACTIVE
    {
        return Err(invalid(
            "target must be an active CognitiveMemory 2.1 Watch",
        ));
    }
    Ok((
        row,
        row.facets
            .get(&format!("{PROFILE}WatchState"))
            .cloned()
            .unwrap_or(Json::Null),
    ))
}

fn due(row: &crate::store::rows::ConceptRow) -> Result<Option<String>, KipError> {
    row.attributes
        .get("due_at")
        .filter(|v| !v.is_null())
        .map(|v| {
            crate::time::normalize(
                v.as_str()
                    .ok_or_else(|| invalid("invalid Watch deadline"))?,
                "Watch deadline",
            )
        })
        .transpose()
}

async fn authorize(
    session: &Session,
    authority: &EffectiveAuthority,
    space: &str,
    reference: &str,
) -> Result<Element, KipError> {
    let element = session.nexus.store.get_element(reference.parse()?).await?;
    if element.space() != space {
        return Err(KipError::not_found_or_not_visible("Watch unavailable"));
    }
    watch_values(&element)?;
    if !authority
        .may_read(&element, &session.auth)
        .is_some_and(|v| v.content && v.constraints.fields.is_empty())
    {
        return Err(KipError::not_found_or_not_visible(
            "Watch condition is not fully visible",
        ));
    }
    for permission in [Permission::Read, Permission::Update] {
        authority
            .authorize(
                permission,
                &ResourceContext::of_element(&element),
                &session.auth,
            )
            .into_result()?;
    }
    Ok(element)
}

/// The protected record of one Watch firing.
const WATCH_FIRE: &str = concat!(
    r#"CREATE ACTIVITY ?watch_fire {CLIENT KEY :key "#,
    r#"SET FIELDS {activity_class:"watch_fire",status:"completed"} "#,
    r#"SET STRUCTURAL {("inputs",:watch)}}"#,
);

/// Which Watch one pass advances, from which generation and version, and how
/// far it may read.
pub(super) struct WatchStep<'a> {
    pub watch_ref: &'a str,
    pub expected: u64,
    pub generation: u64,
    pub limit: usize,
}

impl Session {
    /// Arm a new generation. The native checkpoint pins deadline, condition,
    /// configuration and authorization; ordinary KML cannot attest those facts.
    pub async fn arm_watch(
        &self,
        space: &str,
        watch_ref: &str,
        expected: u64,
    ) -> Result<Json, KipError> {
        self.arm_attention_watch(space, watch_ref, expected, None)
            .await
    }

    /// Change selectors/deadline only while atomically starting a new generation.
    pub async fn rearm_watch(
        &self,
        space: &str,
        watch_ref: &str,
        expected: u64,
        condition: Json,
        due_at: Option<String>,
    ) -> Result<Json, KipError> {
        self.arm_attention_watch(space, watch_ref, expected, Some((condition, due_at)))
            .await
    }

    async fn arm_attention_watch(
        &self,
        space: &str,
        watch_ref: &str,
        expected: u64,
        replacement: Option<(Json, Option<String>)>,
    ) -> Result<Json, KipError> {
        self.with_authority(space, async |authority| {
            let element = authorize(self, &authority, space, watch_ref).await?;
            let (old, old_state) = watch_values(&element)?;
            let request = json!({
                "operation": "arm_watch",
                "watch_ref": watch_ref,
                "expected": expected,
                "replacement": replacement,
            });
            let key = request_key(&self.auth.principal_id, &request)?;
            let request_digest = digest(&request)?;
            if let Some(result) = replay(&self.nexus.store, space, &key, &request_digest).await? {
                return Ok(result);
            }
            if element.version() != expected {
                return Err(conflict("version_conflict"));
            }
            let mut cx =
                Context::open(&self.nexus.store, space, None, None, &authority, &self.auth).await?;
            let condition = bind_watch_condition(
                &mut cx,
                replacement
                    .as_ref()
                    .map(|v| v.0.clone())
                    .unwrap_or_else(|| {
                        old.attributes
                            .get("condition")
                            .cloned()
                            .unwrap_or(Json::Null)
                    }),
            )
            .await?;
            let deadline = match &replacement {
                Some((_, value)) => value
                    .as_ref()
                    .map(|v| crate::time::normalize(v, "Watch deadline"))
                    .transpose()?,
                None => due(old)?,
            };
            if old.attributes["watch_class"] == "silence" && deadline.is_none() {
                return Err(invalid("silence Watch requires a deadline"));
            }
            let pinned_basis = basis(&cx);
            let generation = next(old_state["arm_generation"].as_u64().unwrap_or(0))?;
            let mut tx = Transaction::begin(
                &self.nexus.store,
                space,
                json!({"principal_id":self.auth.principal_id}),
                false,
                authority,
                (*self.auth).clone(),
            )
            .await?;
            let cfg = configuration(&self.nexus.store, &mut tx).await?;
            let id = element.id();
            tx.expect_versions(
                id,
                &[Guard {
                    version: expected,
                    plane: PlaneKey::Element,
                }],
            )
            .await?;
            let watch = json!({
                "arm_generation": generation,
                "armed_seq": tx.cx.seq-1,
                "condition_digest": digest(&condition)?,
                "authorization_view": pinned_basis["authorization"],
                "consumed_seq": tx.cx.seq-1,
                "matched": false,
            });
            let checkpoint = WatchCheckpoint {
                format: "nexus:watch-checkpoint-v1".into(),
                watch_ref: watch_ref.into(),
                arm_generation: generation,
                watch_class: old.attributes["watch_class"].as_str().unwrap_or("").into(),
                due_at: deadline.clone(),
                condition_digest: digest(&condition)?,
                basis: pinned_basis,
                config: cfg,
                due_seq: None,
                matched_seq: None,
            };
            let Element::Concept(row) = tx.load(id).await? else {
                unreachable!()
            };
            row.attributes.insert("condition".into(), condition);
            row.attributes.insert("due_at".into(), json!(deadline));
            row.attributes.insert("status".into(), json!("armed"));
            row.facets
                .insert(format!("{PROFILE}WatchState"), watch.clone());
            tx.authorized_watch_updates.insert(id);
            tx.mark_changed(id, anda_kip::ChangeOp::Update);
            stage_control(
                &self.nexus.store,
                &mut tx,
                &checkpoint_key(watch_ref, generation),
                0,
                "runtime",
                json!(checkpoint),
            )
            .await?;
            commit(
                &self.nexus.store,
                tx,
                key,
                request_digest,
                json!({"watch":watch,"status":"armed"}),
            )
            .await
        })
        .await
    }

    pub async fn advance_watch(
        &self,
        space: &str,
        watch_ref: &str,
        expected: u64,
        generation: u64,
        limit: usize,
    ) -> Result<Json, KipError> {
        self.advance_watch_with(space, watch_ref, expected, generation, limit, |_, _| {
            Err(KipError::unsupported_capability(
                "text Watch requires a registered host evaluator",
            ))
        })
        .await
    }

    /// Synchronous host evaluator. Text/mixed conditions additionally require a
    /// pinned evaluator in AttentionConfig. This is not an asynchronous LLM API.
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
        self.advance_watch_mode(
            space,
            WatchStep {
                watch_ref,
                expected,
                generation,
                limit,
            },
            evaluate,
            Mode::Immediate,
        )
        .await
    }

    pub(super) async fn advance_watch_mode<F>(
        &self,
        space: &str,
        step: WatchStep<'_>,
        evaluate: F,
        mode: Mode,
    ) -> Result<Json, KipError>
    where
        F: Fn(&Json, &Json) -> Result<bool, KipError>,
    {
        let WatchStep {
            watch_ref,
            expected,
            generation,
            limit,
        } = step;
        self.with_authority(space, async |authority| {
            let store = &self.nexus.store;
            let element = authorize(self, &authority, space, watch_ref).await?;
            authority
                .authorize(
                    Permission::ReadHistory,
                    &ResourceContext::default(),
                    &self.auth,
                )
                .into_result()?;
            let (identity, request) = mode.requests(watch_ref, expected, generation, limit);
            let key = request_key(&self.auth.principal_id, &identity)?;
            let request_digest = digest(&request)?;
            if let Some(result) = replay(store, space, &key, &request_digest).await? {
                if let Some(reference) = result["fire_activity_ref"].as_str() {
                    let activity = store.get_element(reference.parse()?).await?;
                    authority
                        .authorize(
                            Permission::Read,
                            &ResourceContext::of_element(&activity),
                            &self.auth,
                        )
                        .into_result()?;
                }
                return Ok(result);
            }
            let (row, mut watch) = watch_values(&element)?;
            if element.version() != expected
                || watch["arm_generation"] != generation
                || row.attributes["status"] != "armed"
            {
                return Err(conflict("generation_conflict"));
            }
            if limit == 0 || limit > 10_000 {
                return Err(invalid("invalid change page limit"));
            }
            let condition = &row.attributes["condition"];
            let structured = crate::runtime::structured_condition(condition);
            let semantic = !structured || condition.get("text").is_some();
            let mut cx = Context::open(store, space, None, None, &authority, &self.auth).await?;
            let pinned_basis = basis(&cx);
            if watch["authorization_view"] != pinned_basis["authorization"] {
                return Err(conflict("basis_changed"));
            }
            let saved = store
                .control_at(space, &checkpoint_key(watch_ref, generation), u64::MAX)
                .await?;
            // Old armed Watches can resume only if their original arm values
            // are retained and unchanged; no new observation interval is invented.
            let mut checkpoint: WatchCheckpoint = if let Some(saved) = &saved {
                serde_json::from_value(saved.value.clone())
                    .map_err(|_| invalid("corrupt Watch checkpoint"))?
            } else {
                let armed_seq = watch["armed_seq"]
                    .as_u64()
                    .ok_or_else(|| invalid("missing armed sequence"))?;
                let original = store
                    .element_at(space, element.id(), armed_seq + 1)
                    .await?
                    .ok_or_else(|| conflict("history_gap"))?;
                let (original, original_state) = watch_values(&original)?;
                if original_state["arm_generation"] != generation
                    || original.attributes["condition"] != *condition
                    || original.attributes["watch_class"] != row.attributes["watch_class"]
                    || due(original)? != due(row)?
                {
                    return Err(conflict("history_gap"));
                }
                let cfg = store
                    .control_at(space, CONFIG, u64::MAX)
                    .await?
                    .map(|r| serde_json::from_value(r.value))
                    .transpose()
                    .map_err(|_| invalid("corrupt attention configuration"))?;
                // The transaction below installs a default config if absent.
                let cfg = cfg.unwrap_or(AttentionConfig {
                    scope: RuntimeScope {
                        space_id: space.into(),
                        space_instance: hex::encode(rand::random::<[u8; 32]>()),
                    },
                    pins: RuntimePins {
                        policy: RuntimePin {
                            id: "nexus:structured-watch-v1".into(),
                            digest: digest(&json!({"engine":"nexus:structured-watch-v1"}))?,
                        },
                        evaluator: None,
                        binding: None,
                    },
                });
                WatchCheckpoint {
                    format: "nexus:watch-checkpoint-v1".into(),
                    watch_ref: watch_ref.into(),
                    arm_generation: generation,
                    watch_class: row.attributes["watch_class"].as_str().unwrap_or("").into(),
                    due_at: due(row)?,
                    condition_digest: digest(condition)?,
                    basis: pinned_basis.clone(),
                    config: cfg,
                    due_seq: None,
                    matched_seq: None,
                }
            };
            if checkpoint.format != "nexus:watch-checkpoint-v1"
                || checkpoint.watch_ref != watch_ref
                || checkpoint.arm_generation != generation
            {
                return Err(invalid("unsupported or mismatched Watch checkpoint"));
            }
            validate_config(&checkpoint.config)?;
            if checkpoint.basis != pinned_basis
                || checkpoint.condition_digest != digest(condition)?
                || checkpoint.due_at != due(row)?
                || checkpoint.watch_class != row.attributes["watch_class"].as_str().unwrap_or("")
            {
                return Err(conflict("basis_changed"));
            }
            if let Some(cfg) = store.control_at(space, CONFIG, u64::MAX).await?
                && cfg.value != json!(checkpoint.config)
            {
                return Err(conflict("basis_changed"));
            }
            if semantic && checkpoint.config.pins.evaluator.is_none() {
                return Err(KipError::unsupported_capability(
                    "text and mixed Watch conditions need a pinned host evaluator",
                ));
            }
            let silence = checkpoint.watch_class == "silence";
            let source_at = match &mode {
                Mode::Evaluate { payload, .. } => payload.source_at.clone(),
                _ => cx.at.clone(),
            };
            let deadline = checkpoint.due_at.as_ref().is_some_and(|d| d <= &source_at);
            if let Mode::Evaluate {
                payload, ticket, ..
            } = &mode
            {
                if ticket.principal != self.auth.principal_id
                    || payload.basis != pinned_basis
                    || payload.checkpoint_digest
                        != saved.as_ref().map(|r| digest(&r.value)).transpose()?
                {
                    return Err(conflict("basis_changed"));
                }
                checkpoint.due_seq = payload.due_seq;
            }
            if silence && deadline && checkpoint.due_seq.is_none() {
                checkpoint.due_seq = Some(
                    store
                        .seq_at_time(space, checkpoint.due_at.as_deref().unwrap())
                        .await?,
                );
            }
            let after = watch["consumed_seq"]
                .as_u64()
                .ok_or_else(|| invalid("missing Watch coverage"))?;
            let target = match &mode {
                Mode::Evaluate { payload, .. } => payload.target,
                _ => {
                    if silence && deadline {
                        checkpoint
                            .due_seq
                            .unwrap()
                            .max(watch["armed_seq"].as_u64().unwrap_or(0))
                    } else {
                        cx.pinned_seq
                    }
                }
            };
            // A pre-deadline pass may already have proved beyond the later
            // resolved due_seq. Never move the stored watermark backwards.
            let page =
                crate::meta::history::change_page_through(&mut cx, after, after.max(target), limit)
                    .await?;
            if page["resync_required"] == true {
                return Err(conflict("history_gap"));
            }
            if watch["authorization_view"] != page["coverage"]["authorization_view"] {
                return Err(conflict("basis_changed"));
            }
            if let Mode::Prepare(preparation_key) = &mode {
                let preparation = evaluation::Preparation {
                    authority,
                    watch_ref,
                    expected,
                    generation,
                    limit,
                    preparation_key,
                    condition: condition.clone(),
                    page: &page,
                    checkpoint: &checkpoint,
                    saved: saved.as_ref(),
                    source_at,
                    target,
                    deadline,
                    semantic,
                    key,
                    request_digest,
                };
                return evaluation::prepare(self, preparation).await;
            }
            let evaluated = if let Mode::Evaluate {
                payload,
                evaluation,
                ticket_ref,
                ..
            } = &mode
            {
                if payload.prepared.page_digest != evaluation::page_digest(&page)?
                    || payload.deadline != deadline
                {
                    return Err(conflict("prepared_page_changed"));
                }
                let actual = evaluation::candidates(
                    self,
                    &authority,
                    condition,
                    &page,
                    silence,
                    checkpoint.due_at.as_deref(),
                    semantic,
                )
                .await?;
                if actual.iter().map(|c| &c.id).collect::<Vec<_>>()
                    != payload
                        .prepared
                        .candidates
                        .iter()
                        .map(|c| &c.id)
                        .collect::<Vec<_>>()
                {
                    return Err(conflict("prepared_candidates_changed"));
                }
                let matches = evaluation::validate(payload, evaluation)?;
                if evaluation
                    .judgments
                    .iter()
                    .any(|j| j.result == WatchMatch::Unknown)
                {
                    let mut tx = Transaction::begin(
                        store,
                        space,
                        json!({"principal_id":self.auth.principal_id}),
                        false,
                        authority,
                        (*self.auth).clone(),
                    )
                    .await?;
                    let evaluation_ref =
                        evaluation::record(store, &mut tx, ticket_ref, evaluation, payload, false)
                            .await?;
                    return commit(
                        store,
                        tx,
                        key,
                        request_digest,
                        json!({
                            "status": "deferred",
                            "reason": "semantic_unknown",
                            "ticket_ref": ticket_ref,
                            "evaluation_ref": evaluation_ref,
                        }),
                    )
                    .await;
                }
                Some(matches)
            } else {
                None
            };
            let mut matched = watch["matched"] == true;
            for envelope in page["changes"].as_array().into_iter().flatten() {
                if envelope["control_changes"].as_array().is_some_and(|cs| {
                    cs.iter().any(|c| {
                        matches!(
                            c["kind"].as_str(),
                            Some(
                                "schema"
                                    | "identity"
                                    | "policy"
                                    | "trust"
                                    | "authorization"
                                    | "recording"
                            )
                        )
                    })
                }) {
                    return Err(conflict("basis_changed"));
                }
                if silence
                    && checkpoint.due_at.as_ref().is_some_and(|d| {
                        envelope["committed_at"]
                            .as_str()
                            .is_some_and(|at| at > d.as_str())
                    })
                {
                    continue;
                }
                let mut hit = !structured
                    && envelope["changes"]
                        .as_array()
                        .is_some_and(|v| !v.is_empty());
                if structured {
                    for change in envelope["changes"].as_array().into_iter().flatten() {
                        if match_change(store, space, condition, envelope, change).await? {
                            hit = true;
                            break;
                        }
                    }
                }
                let semantic_hit = if let Some(evaluated) = &evaluated {
                    evaluated
                        .get(&envelope["space_seq"].as_u64().unwrap_or(0))
                        .copied()
                        .unwrap_or(false)
                } else if hit && semantic {
                    evaluate(condition, envelope)?
                } else {
                    false
                };
                if hit && (!semantic || semantic_hit) {
                    matched = true;
                    if checkpoint.matched_seq.is_none() {
                        checkpoint.matched_seq = Some(
                            envelope["space_seq"]
                                .as_u64()
                                .ok_or_else(|| invalid("change has no sequence"))?,
                        );
                    }
                }
            }
            watch["matched"] = json!(matched);
            watch["consumed_seq"] = page["coverage"]["through_seq"].clone();
            let covered = page["coverage"]["complete"] == true;
            let status = if (!silence && matched) || (silence && deadline && covered && !matched) {
                "fired"
            } else if deadline && covered {
                "expired"
            } else {
                "armed"
            };
            let mut tx = Transaction::begin(
                store,
                space,
                json!({"principal_id":self.auth.principal_id}),
                false,
                authority,
                (*self.auth).clone(),
            )
            .await?;
            tx.expect_versions(
                element.id(),
                &[Guard {
                    version: expected,
                    plane: PlaneKey::Element,
                }],
            )
            .await?;
            let Element::Concept(next) = tx.load(element.id()).await? else {
                unreachable!()
            };
            next.attributes.insert("status".into(), json!(status));
            next.facets
                .insert(format!("{PROFILE}WatchState"), watch.clone());
            tx.authorized_watch_updates.insert(element.id());
            tx.mark_changed(element.id(), anda_kip::ChangeOp::Update);
            if store.control_at(space, CONFIG, u64::MAX).await?.is_none() {
                stage_control(
                    store,
                    &mut tx,
                    CONFIG,
                    0,
                    "runtime",
                    json!(checkpoint.config),
                )
                .await?;
            }
            stage_control(
                store,
                &mut tx,
                &checkpoint_key(watch_ref, generation),
                saved.map_or(0, |s| s.version),
                "runtime",
                json!(checkpoint),
            )
            .await?;
            let mut result = json!({"status":status,"watch":watch,"coverage":page["coverage"]});
            if let Mode::Evaluate {
                ticket_ref,
                evaluation,
                payload,
                ..
            } = &mode
            {
                let evaluation_ref =
                    evaluation::record(store, &mut tx, ticket_ref, evaluation, payload, true)
                        .await?;
                result["ticket_ref"] = json!(ticket_ref);
                result["evaluation_ref"] = json!(evaluation_ref);
            }
            if status == "fired" {
                let fire = WatchFire {
                    watch_ref: watch_ref.into(),
                    arm_generation: generation,
                    trigger: if silence {
                        WatchTrigger::Silence {
                            due_at: checkpoint.due_at.clone().unwrap(),
                            due_seq: checkpoint.due_seq.unwrap(),
                        }
                    } else {
                        WatchTrigger::Delta {
                            matched_seq: checkpoint
                                .matched_seq
                                .ok_or_else(|| invalid("firing lacks matching envelope"))?,
                        }
                    },
                };
                let fire_key = fire.key()?;
                let anda_kip::Command::Kml(statement) = anda_kip::parse_kip(WATCH_FIRE)? else {
                    unreachable!()
                };
                let parameters = Map::from_iter([
                    ("key".into(), json!(fire_key)),
                    ("watch".into(), json!(watch_ref)),
                ]);
                if let Err(e) = crate::kml::plan(
                    store,
                    &mut tx,
                    &statement,
                    Some(&parameters),
                    &anda_kip::Operation::new(WATCH_FIRE),
                )
                .await
                {
                    tx.abort().await;
                    return Err(e);
                }
                let activity_id = tx.handles()["watch_fire"];
                if !tx.is_new_element(activity_id) {
                    tx.abort().await;
                    return Err(conflict("fire_identity_already_exists"));
                }
                tx.authorized_watch_fires.insert(activity_id);
                let activity = activity_id.to_string();
                let wake_ref = runtime_ref(
                    "wake",
                    &json!({
                        "domain": "anda-brain:wake-v1",
                        "scope": checkpoint.config.scope,
                        "fire_key": fire_key,
                    }),
                )?;
                let wake = WakeRecord {
                    format: FORMAT.into(),
                    scope: checkpoint.config.scope.clone(),
                    wake_ref: wake_ref.clone(),
                    fire,
                    fire_activity_ref: activity.clone(),
                    pins: checkpoint.config.pins.clone(),
                    version: 1,
                    fence: 0,
                    state: WakeState::Pending { not_before_ms: 0 },
                    parent_ref: None,
                    continuation_key: None,
                };
                stage_control(store, &mut tx, &wake_ref, 0, "wake", json!(wake)).await?;
                result["fire_key"] = json!(fire_key);
                result["fire_activity_ref"] = json!(activity);
                result["wake_ref"] = json!(wake_ref);
            }
            commit(store, tx, key, request_digest, result).await
        })
        .await
    }
}

pub(super) async fn match_change(
    store: &Store,
    space: &str,
    condition: &Json,
    envelope: &Json,
    change: &Json,
) -> Result<bool, KipError> {
    if condition
        .get("element")
        .is_some_and(|id| id != &change["id"])
    {
        return Ok(false);
    }
    if condition["ops"]
        .as_array()
        .is_some_and(|ops| !ops.contains(&change["op"]))
    {
        return Ok(false);
    }
    if let Some(paths) = condition["touched"].as_array()
        && !change["touched"]
            .as_array()
            .is_some_and(|p| p.iter().any(|p| paths.contains(p)))
    {
        return Ok(false);
    }
    if condition.get("type").is_some() || condition.get("slot").is_some() {
        let target = change["id"].as_str().unwrap_or("").parse()?;
        let seq = envelope["space_seq"]
            .as_u64()
            .ok_or_else(|| invalid("missing change sequence"))?;
        let retained = store
            .element_at(space, target, seq)
            .await?
            .ok_or_else(|| conflict("history_gap"))?;
        let view = crate::view::render(&retained);
        if condition
            .get("type")
            .is_some_and(|t| t != &view["schema_ref"])
        {
            return Ok(false);
        }
        if let Some(slot) = condition.get("slot") {
            let p = if let Element::Assertion(a) = retained {
                store
                    .element_at(space, a.proposition_id.parse()?, seq)
                    .await?
                    .map(|r| crate::view::render(&r))
                    .ok_or_else(|| conflict("history_gap"))?
            } else {
                view
            };
            if p["subject"]
                .as_str()
                .or_else(|| p["subject"]["id"].as_str())
                != slot["subject"].as_str()
                || p["predicate_ref"] != slot["predicate"]
            {
                return Ok(false);
            }
        }
    }
    Ok(true)
}
