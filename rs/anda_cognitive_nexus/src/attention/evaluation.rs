use super::watch::WatchStep;
use super::*;
use anda_kip::cognitive::ArtifactPin;
use std::collections::{BTreeMap, BTreeSet};

const TICKET_FORMAT: &str = "nexus:watch-page-v1";
const MAX_CANDIDATES: usize = 512;
const MAX_PAGE_BYTES: usize = 524_288;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WatchCandidate {
    pub id: String,
    pub envelope_seq: u64,
    pub change: Json,
    /// Raw, authorized Core views at this exact transition, not beliefs. A
    /// missing before view means creation, never unavailable historical data.
    pub before: Option<Json>,
    pub after: Json,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedWatchPage {
    pub ticket_ref: String,
    pub watch_ref: String,
    pub arm_generation: u64,
    pub expected_version: u64,
    pub source_snapshot_seq: u64,
    pub through_seq: u64,
    pub deadline_covered: bool,
    pub page_digest: String,
    pub evaluator: Option<RuntimePin>,
    pub condition: Json,
    pub candidates: Vec<WatchCandidate>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WatchMatch {
    Match,
    NoMatch,
    Unknown,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WatchJudgment {
    pub candidate_id: String,
    pub result: WatchMatch,
    /// Bounded host/evaluator reasoning, retained as governed material.
    pub rationale: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WatchEvaluation {
    pub evaluation_key: String,
    pub evaluator: Option<RuntimePin>,
    pub judgments: Vec<WatchJudgment>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Ticket {
    pub format: String,
    pub principal: String,
    pub watch_ref: String,
    pub expected: u64,
    pub generation: u64,
    pub limit: usize,
    pub material: ArtifactPin,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Payload {
    pub prepared: PreparedWatchPage,
    pub source_at: String,
    pub target: u64,
    pub due_seq: Option<u64>,
    pub deadline: bool,
    pub checkpoint_digest: Option<String>,
    pub basis: Json,
}

pub(super) enum Mode {
    Immediate,
    Prepare(String),
    Evaluate {
        ticket_ref: String,
        ticket: Box<Ticket>,
        payload: Box<Payload>,
        evaluation: WatchEvaluation,
    },
}

impl Mode {
    pub(super) fn requests(
        &self,
        watch: &str,
        expected: u64,
        generation: u64,
        limit: usize,
    ) -> (Json, Json) {
        match self {
            Self::Immediate => {
                let r = json!({
                    "operation": "advance_watch",
                    "watch_ref": watch,
                    "expected": expected,
                    "generation": generation,
                    "limit": limit,
                });
                (r.clone(), r)
            }
            Self::Prepare(key) => (
                json!({"operation":"prepare_watch_page","watch_ref":watch,"key":key}),
                json!({
                    "watch_ref": watch,
                    "expected": expected,
                    "generation": generation,
                    "limit": limit,
                    "preparation_key": key,
                }),
            ),
            Self::Evaluate {
                ticket_ref,
                evaluation,
                ..
            } => (
                json!({
                    "operation": "commit_watch_page",
                    "ticket_ref": ticket_ref,
                    "evaluation_key": evaluation.evaluation_key,
                }),
                json!(evaluation),
            ),
        }
    }
}

pub(super) fn page_digest(page: &Json) -> Result<String, KipError> {
    digest(&json!({
        "changes": page["changes"],
        "coverage": page["coverage"],
        "resync_required": page["resync_required"],
    }))
}

pub(super) async fn candidates(
    session: &Session,
    authority: &EffectiveAuthority,
    condition: &Json,
    page: &Json,
    silence: bool,
    due_at: Option<&str>,
    semantic: bool,
) -> Result<Vec<WatchCandidate>, KipError> {
    let store = &session.nexus.store;
    let space = &authority.space.space_id;
    let mut candidates = Vec::new();
    for envelope in page["changes"].as_array().into_iter().flatten() {
        if envelope["control_changes"].as_array().is_some_and(|cs| {
            cs.iter().any(|c| {
                matches!(
                    c["kind"].as_str(),
                    Some(
                        "schema" | "identity" | "policy" | "trust" | "authorization" | "recording"
                    )
                )
            })
        }) {
            return Err(conflict("basis_changed"));
        }
        if silence
            && due_at.is_some_and(|d| envelope["committed_at"].as_str().is_some_and(|t| t > d))
        {
            continue;
        }
        if !semantic {
            continue;
        }
        for change in envelope["changes"].as_array().into_iter().flatten() {
            if !super::watch::match_change(store, space, condition, envelope, change).await? {
                continue;
            }
            let seq = envelope["space_seq"]
                .as_u64()
                .ok_or_else(|| invalid("missing change sequence"))?;
            let source = change["id"]
                .as_str()
                .ok_or_else(|| invalid("missing candidate identity"))?
                .parse()?;
            let after = store
                .element_at(space, source, seq)
                .await?
                .ok_or_else(|| conflict("history_gap"))?;
            if Some(after.version()) != change["new_version"].as_u64() {
                return Err(conflict("history_gap"));
            }
            let before = if let Some(version) = change["old_version"].as_u64() {
                let old = store
                    .element_at(space, source, seq.saturating_sub(1))
                    .await?
                    .ok_or_else(|| conflict("history_gap"))?;
                if old.version() != version {
                    return Err(conflict("history_gap"));
                }
                Some(old)
            } else {
                None
            };
            let current = store.get_element(source).await?;
            for row in std::iter::once(&current)
                .chain(std::iter::once(&after))
                .chain(before.iter())
            {
                if row.space() != space
                    || !authority
                        .may_read(row, &session.auth)
                        .is_some_and(|v| v.content && v.constraints.fields.is_empty())
                {
                    return Err(KipError::not_found_or_not_visible(
                        "semantic history is not fully visible",
                    ));
                }
            }
            if candidates.len() >= MAX_CANDIDATES {
                return Err(KipError::resource_exhausted(
                    "semantic page exceeds 512 candidates; reduce the page budget",
                ));
            }
            let before = before.as_ref().map(crate::view::render);
            let after = crate::view::render(&after);
            candidates.push(WatchCandidate {
                id: digest(&json!({"seq":seq,"change":change,"before":before,"after":after}))?,
                envelope_seq: seq,
                change: change.clone(),
                before,
                after,
            });
        }
    }
    Ok(candidates)
}

/// Everything one Watch evaluation preparation reads, as the Watch pass
/// resolved it.
pub(super) struct Preparation<'a> {
    pub authority: EffectiveAuthority,
    pub watch_ref: &'a str,
    pub expected: u64,
    pub generation: u64,
    pub limit: usize,
    pub preparation_key: &'a str,
    pub condition: Json,
    pub page: &'a Json,
    pub checkpoint: &'a WatchCheckpoint,
    pub saved: Option<&'a ControlRecordRow>,
    pub source_at: String,
    pub target: u64,
    pub deadline: bool,
    pub semantic: bool,
    pub key: String,
    pub request_digest: String,
}

pub(super) async fn prepare(
    session: &Session,
    preparation: Preparation<'_>,
) -> Result<Json, KipError> {
    let Preparation {
        authority,
        watch_ref,
        expected,
        generation,
        limit,
        preparation_key,
        condition,
        page,
        checkpoint,
        saved,
        source_at,
        target,
        deadline,
        semantic,
        key,
        request_digest,
    } = preparation;
    let space = &authority.space.space_id;
    let store = &session.nexus.store;
    authority
        .authorize(
            Permission::Derive,
            &ResourceContext::default(),
            &session.auth,
        )
        .into_result()?;
    let candidates = candidates(
        session,
        &authority,
        &condition,
        page,
        checkpoint.watch_class == "silence",
        checkpoint.due_at.as_deref(),
        semantic,
    )
    .await?;
    let ticket_ref = runtime_ref(
        "watch-page",
        &json!({
            "scope": checkpoint.config.scope,
            "principal": session.auth.principal_id,
            "watch_ref": watch_ref,
            "preparation_key": preparation_key,
        }),
    )?;
    let prepared = PreparedWatchPage {
        ticket_ref: ticket_ref.clone(),
        watch_ref: watch_ref.into(),
        arm_generation: generation,
        expected_version: expected,
        source_snapshot_seq: store.get_space(space).await?.seq,
        through_seq: page["coverage"]["through_seq"].as_u64().unwrap_or(0),
        deadline_covered: deadline && page["coverage"]["complete"] == true,
        page_digest: page_digest(page)?,
        evaluator: if semantic {
            checkpoint.config.pins.evaluator.clone()
        } else {
            None
        },
        condition,
        candidates,
    };
    let payload = Payload {
        prepared,
        source_at,
        target,
        due_seq: checkpoint.due_seq,
        deadline,
        checkpoint_digest: saved.map(|r| digest(&r.value)).transpose()?,
        basis: checkpoint.basis.clone(),
    };
    let content = json!(payload);
    if serde_json::to_vec(&content)
        .map_err(|e| invalid(&e.to_string()))?
        .len()
        > MAX_PAGE_BYTES
    {
        return Err(KipError::resource_exhausted(
            "prepared semantic page exceeds 512 KiB",
        ));
    }
    let mut sources = BTreeSet::from([watch_ref.to_string()]);
    for candidate in &payload.prepared.candidates {
        sources.insert(candidate.change["id"].as_str().unwrap().to_string());
    }
    for reference in &sources {
        let row = store.get_element(reference.parse()?).await?;
        if row.space() != *space
            || !authority
                .may_read(&row, &session.auth)
                .is_some_and(|v| v.content && v.constraints.fields.is_empty())
        {
            return Err(KipError::not_found_or_not_visible(
                "semantic material is not fully visible",
            ));
        }
    }
    let material = ArtifactPin {
        content_digest: digest(&content)?,
        artifact_ref: format!("kip:artifact:{}", digest(&content)?),
    };
    let ticket = Ticket {
        format: TICKET_FORMAT.into(),
        principal: session.auth.principal_id.clone(),
        watch_ref: watch_ref.into(),
        expected,
        generation,
        limit,
        material: material.clone(),
    };
    let mut tx = Transaction::begin(
        store,
        space,
        json!({"principal_id":session.auth.principal_id}),
        false,
        authority.clone(),
        (*session.auth).clone(),
    )
    .await?;
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
        &format!("artifact/{}", material.artifact_ref),
        0,
        "artifact",
        json!({
            "state": "available",
            "content": content,
            "content_digest": material.content_digest,
            "source_refs": sources,
        }),
    )
    .await?;
    stage_control(store, &mut tx, &ticket_ref, 0, "runtime", json!(ticket)).await?;
    // The transaction journal retains only refs. Material copies remain governed
    // artifacts so erasure can revoke them rather than leaving a plaintext replay.
    commit(
        store,
        tx,
        key,
        request_digest,
        json!({"ticket_ref":ticket_ref}),
    )
    .await
}

pub(super) fn validate(
    payload: &Payload,
    evaluation: &WatchEvaluation,
) -> Result<BTreeMap<u64, bool>, KipError> {
    if !bounded(&evaluation.evaluation_key)
        || evaluation.evaluator != payload.prepared.evaluator
        || evaluation.judgments.len() != payload.prepared.candidates.len()
    {
        return Err(invalid(
            "evaluation does not cover the pinned page/evaluator",
        ));
    }
    let expected: BTreeMap<_, _> = payload
        .prepared
        .candidates
        .iter()
        .map(|c| (c.id.as_str(), c.envelope_seq))
        .collect();
    let mut seen = BTreeSet::new();
    let mut matches = BTreeMap::new();
    for judgment in &evaluation.judgments {
        if judgment.rationale.trim().is_empty() || judgment.rationale.len() > 4096 {
            return Err(invalid("each semantic judgment needs a bounded rationale"));
        }
        let seq = expected
            .get(judgment.candidate_id.as_str())
            .ok_or_else(|| invalid("unknown semantic candidate"))?;
        if !seen.insert(&judgment.candidate_id) {
            return Err(invalid("duplicate semantic candidate"));
        }
        let hit = matches.entry(*seq).or_insert(false);
        *hit |= judgment.result == WatchMatch::Match;
    }
    Ok(matches)
}

pub(super) async fn record(
    store: &Store,
    tx: &mut Transaction,
    ticket_ref: &str,
    evaluation: &WatchEvaluation,
    payload: &Payload,
    accepted: bool,
) -> Result<String, KipError> {
    let reference = runtime_ref(
        "watch-evaluation",
        &json!({
            "ticket_ref": ticket_ref,
            "principal": tx.auth.principal_id,
            "evaluation_key": evaluation.evaluation_key,
        }),
    )?;
    let content = json!({"ticket_ref":ticket_ref,"evaluation":evaluation});
    if serde_json::to_vec(&content)
        .map_err(|e| invalid(&e.to_string()))?
        .len()
        > MAX_PAGE_BYTES
    {
        return Err(KipError::resource_exhausted(
            "semantic evaluation exceeds 512 KiB",
        ));
    }
    let mut sources = BTreeSet::from([payload.prepared.watch_ref.clone()]);
    for candidate in &payload.prepared.candidates {
        sources.insert(candidate.change["id"].as_str().unwrap().to_string());
    }
    let material = ArtifactPin {
        content_digest: digest(&content)?,
        artifact_ref: format!("kip:artifact:{}", digest(&content)?),
    };
    stage_control(
        store,
        tx,
        &format!("artifact/{}", material.artifact_ref),
        0,
        "artifact",
        json!({
            "state": "available",
            "content": content,
            "content_digest": material.content_digest,
            "source_refs": sources,
        }),
    )
    .await?;
    stage_control(
        store,
        tx,
        &reference,
        0,
        "runtime",
        json!({
            "format": "nexus:watch-evaluation-v1",
            "ticket_ref": ticket_ref,
            "material": material,
            "accepted": accepted,
        }),
    )
    .await?;
    Ok(reference)
}

impl Session {
    pub async fn prepare_watch_page(
        &self,
        space: &str,
        watch_ref: &str,
        expected: u64,
        generation: u64,
        limit: usize,
        preparation_key: &str,
    ) -> Result<Json, KipError> {
        if !bounded(preparation_key) || !(1..=200).contains(&limit) {
            return Err(invalid("invalid semantic page key/budget"));
        }
        let mut result = self
            .advance_watch_mode(
                space,
                WatchStep {
                    watch_ref,
                    expected,
                    generation,
                    limit,
                },
                |_, _| Ok(false),
                Mode::Prepare(preparation_key.into()),
            )
            .await?;
        let prepared = self
            .read_prepared_watch_page(space, result["ticket_ref"].as_str().unwrap())
            .await?;
        result["prepared"] = json!(prepared);
        Ok(result)
    }

    async fn load_watch_ticket(
        &self,
        space: &str,
        reference: &str,
    ) -> Result<(Ticket, Payload), KipError> {
        if !reference.starts_with("watch-page/v1/") || reference.len() > 256 {
            return Err(invalid("invalid watch ticket"));
        }
        let _guard = self.nexus.read_guard().await?;
        let authority = self.effective_authority(space).await?;
        let row = self
            .nexus
            .store
            .control_at(space, reference, u64::MAX)
            .await?
            .ok_or_else(|| KipError::not_found_or_not_visible("watch ticket unavailable"))?;
        let ticket: Ticket =
            serde_json::from_value(row.value).map_err(|_| invalid("invalid watch ticket"))?;
        if ticket.format != TICKET_FORMAT || ticket.principal != self.auth.principal_id {
            return Err(KipError::not_authorized(
                "watch ticket belongs to another authorization view",
            ));
        }
        let (value, _) = self
            .nexus
            .store
            .authorized_artifact(space, &ticket.material.artifact_ref, &authority, &self.auth)
            .await?;
        if digest(&value)? != ticket.material.content_digest {
            return Err(invalid("prepared material digest mismatch"));
        }
        let payload: Payload =
            serde_json::from_value(value).map_err(|_| invalid("invalid prepared material"))?;
        if payload.prepared.ticket_ref != reference {
            return Err(conflict("basis_changed"));
        }
        Ok((ticket, payload))
    }

    pub async fn read_prepared_watch_page(
        &self,
        space: &str,
        reference: &str,
    ) -> Result<PreparedWatchPage, KipError> {
        Ok(self.load_watch_ticket(space, reference).await?.1.prepared)
    }

    pub async fn commit_watch_page(
        &self,
        space: &str,
        reference: &str,
        evaluation: WatchEvaluation,
    ) -> Result<Json, KipError> {
        let (ticket, payload) = self.load_watch_ticket(space, reference).await?;
        validate(&payload, &evaluation)?;
        self.advance_watch_mode(
            space,
            WatchStep {
                watch_ref: &ticket.watch_ref.clone(),
                expected: ticket.expected,
                generation: ticket.generation,
                limit: ticket.limit,
            },
            |_, _| Ok(false),
            Mode::Evaluate {
                ticket_ref: reference.into(),
                ticket: Box::new(ticket),
                payload: Box::new(payload),
                evaluation,
            },
        )
        .await
    }
}
