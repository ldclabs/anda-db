//! Protected attention records. Creating a wake grants no external authority.
//! Scheduling, asynchronous semantic evaluation and external I/O belong to the host.
use crate::{
    governance::{EffectiveAuthority, Permission, ResourceContext},
    kql::Context,
    nexus::Session,
    schema::contracts::digest,
    store::{Element, Store, rows::ControlRecordRow, space::JournalEntry},
    tx::Transaction,
};
use anda_kip::{Json, KipError, Map};
use serde::{Deserialize, Serialize};
use serde_json::json;

mod catalog;
mod evaluation;
mod lookup;
pub(crate) mod resume;
mod watch;
mod work;

pub use catalog::WakePage;
pub use evaluation::{
    PreparedWatchPage, WatchCandidate, WatchEvaluation, WatchJudgment, WatchMatch,
};
pub use lookup::{DispatchLookup, DispatchLookupObserver, DispatchLookupStatus};
pub use resume::{WakeResumeInput, WakeResumeVerifier};

const PROFILE: &str = cognitive_memory!("");
const FORMAT: &str = "anda-brain:attention-v1";
const CONTINUATION: &str = "anda-brain:attention-continuation-v1";
const CONFIG: &str = "attention/config";
const MAX_LEASE_MS: u64 = 300_000;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimeScope {
    pub space_id: String,
    pub space_instance: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimePin {
    pub id: String,
    pub digest: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RuntimePins {
    pub policy: RuntimePin,
    pub evaluator: Option<RuntimePin>,
    pub binding: Option<RuntimePin>,
}

/// Trusted host registration, not a binding implementation or permission grant.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AttentionConfig {
    pub scope: RuntimeScope,
    pub pins: RuntimePins,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WatchTrigger {
    Delta { matched_seq: u64 },
    Silence { due_at: String, due_seq: u64 },
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WatchFire {
    pub watch_ref: String,
    pub arm_generation: u64,
    pub trigger: WatchTrigger,
}

impl WatchFire {
    pub fn key(&self) -> Result<String, KipError> {
        let id = crate::ElementId::parse_kind(&self.watch_ref, anda_kip::ElementKind::Concept)?;
        if id.to_string() != self.watch_ref
            || id.seq == 0
            || id.seq > anda_kip::MAX_SAFE_INTEGER
            || self.arm_generation == 0
            || self.arm_generation > anda_kip::MAX_SAFE_INTEGER
        {
            return Err(invalid("invalid Watch fire identity"));
        }
        let suffix = match &self.trigger {
            WatchTrigger::Delta { matched_seq }
                if *matched_seq > 0 && *matched_seq <= anda_kip::MAX_SAFE_INTEGER =>
            {
                matched_seq.to_string()
            }
            WatchTrigger::Silence { due_at, due_seq } if *due_seq <= anda_kip::MAX_SAFE_INTEGER => {
                if crate::time::normalize(due_at, "Watch deadline")? != *due_at {
                    return Err(invalid("deadline must use normalized UTC"));
                }
                format!("silence:{due_at}")
            }
            _ => return Err(invalid("invalid matching sequence")),
        };
        Ok(format!(
            "watch_fire:{}:{}:{suffix}",
            self.watch_ref, self.arm_generation
        ))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WakeLease {
    pub owner: String,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WakeRetry {
    pub reason: String,
    pub resume: WakeResume,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WakeResume {
    At { not_before_ms: u64 },
    OnChange { condition_digest: String },
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "stage", rename_all = "snake_case", deny_unknown_fields)]
pub enum WakeState {
    Pending { not_before_ms: u64 },
    Running { lease: WakeLease },
    Blocked { retry: WakeRetry },
    Completed { receipt_ref: String },
    Cancelled { receipt_ref: String },
}

/// Watch-origin wakes retain the frozen R0 shape. Continuations carry a separate
/// format and a parent ref; they never pretend a second Watch firing occurred.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WakeRecord {
    pub format: String,
    pub scope: RuntimeScope,
    pub wake_ref: String,
    pub fire: WatchFire,
    pub fire_activity_ref: String,
    pub pins: RuntimePins,
    pub version: u64,
    pub fence: u64,
    pub state: WakeState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_key: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WakeContinuation {
    /// Unique within this completion, bounded to 128 UTF-8 bytes.
    pub key: String,
    pub not_before_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WatchCheckpoint {
    format: String,
    watch_ref: String,
    arm_generation: u64,
    watch_class: String,
    due_at: Option<String>,
    condition_digest: String,
    basis: Json,
    config: AttentionConfig,
    due_seq: Option<u64>,
    matched_seq: Option<u64>,
}

fn invalid(message: &str) -> KipError {
    KipError::constraint_violation(message)
}
fn conflict(reason: &str) -> KipError {
    let mut error = KipError::version_conflict(reason);
    error.details = Some(json!({"attention_reason":reason}));
    error
}
fn bounded(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && value.trim() == value
}
fn valid_digest(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|v| {
        v.len() == 64
            && v.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}
fn validate_config(config: &AttentionConfig) -> Result<(), KipError> {
    if !bounded(&config.scope.space_id) || !bounded(&config.scope.space_instance) {
        return Err(invalid("invalid runtime scope"));
    }
    for pin in std::iter::once(&config.pins.policy)
        .chain(config.pins.evaluator.iter())
        .chain(config.pins.binding.iter())
    {
        if !bounded(&pin.id) || !valid_digest(&pin.digest) {
            return Err(invalid("invalid runtime configuration pin"));
        }
    }
    Ok(())
}
fn next(value: u64) -> Result<u64, KipError> {
    value
        .checked_add(1)
        .filter(|v| *v <= anda_kip::MAX_SAFE_INTEGER)
        .ok_or_else(|| KipError::resource_exhausted("attention counter exhausted"))
}
fn runtime_ref(prefix: &str, input: &Json) -> Result<String, KipError> {
    Ok(format!("{prefix}/v1/{}", &digest(input)?[7..]))
}
fn checkpoint_key(watch: &str, generation: u64) -> String {
    format!("attention/watch/{watch}/{generation}")
}

fn basis(cx: &Context<'_>) -> Json {
    let b = cx.projection_basis(&cx.policy, &cx.at, None);
    json!({
        "schema": b.schema_environment_version,
        "identity": b.identity_version,
        "policy": b.policy,
        "trust": b.trust_version,
        "authorization": b.authorization_view,
    })
}

async fn configuration(store: &Store, tx: &mut Transaction) -> Result<AttentionConfig, KipError> {
    if let Some(row) = store.control_at(&tx.cx.space, CONFIG, u64::MAX).await? {
        let cfg: AttentionConfig = serde_json::from_value(row.value)
            .map_err(|_| invalid("corrupt attention configuration"))?;
        validate_config(&cfg)?;
        return Ok(cfg);
    }
    let cfg = AttentionConfig {
        scope: RuntimeScope {
            space_id: tx.cx.space.clone(),
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
    };
    validate_config(&cfg)?;
    stage_control(store, tx, CONFIG, 0, "runtime", json!(cfg)).await?;
    Ok(cfg)
}

pub(crate) async fn stage_control(
    store: &Store,
    tx: &mut Transaction,
    key: &str,
    expected: u64,
    kind: &str,
    value: Json,
) -> Result<(), KipError> {
    anda_kip::validate_json(&value)?;
    if store
        .control_at(&tx.cx.space, key, u64::MAX)
        .await?
        .map_or(0, |r| r.version)
        != expected
        || tx.control_effects.iter().any(|r| r.key == key)
    {
        return Err(conflict("version_conflict"));
    }
    tx.control_effects.push(ControlRecordRow {
        _id: 0,
        record_id: format!("{}:{key}", tx.cx.tx_id),
        space: tx.cx.space.clone(),
        key: key.into(),
        seq: tx.cx.seq,
        version: next(expected)?,
        kind: kind.into(),
        value,
        origin: tx.cx.origin.clone(),
    });
    Ok(())
}

pub(crate) fn request_key(principal: &str, request: &Json) -> Result<String, KipError> {
    Ok(format!(
        "attention\u{1f}{principal}\u{1f}{}",
        digest(request)?
    ))
}

async fn reply(store: &Store, tx_id: &str) -> Result<Json, KipError> {
    let row = store.find_transaction(tx_id).await?.ok_or_else(|| {
        KipError::outcome_unknown("attention commit receipt unavailable; re-read before retry")
    })?;
    let mut result = row.result["runtime"].clone();
    if !result.is_object() {
        return Err(invalid("attention receipt lacks its retained result"));
    }
    let replay = crate::kml::replay(&row);
    result["receipt"] =
        serde_json::to_value(&replay.results[0].receipt).map_err(|e| invalid(&e.to_string()))?;
    Ok(result)
}

pub(crate) async fn replay(
    store: &Store,
    space: &str,
    key: &str,
    request_digest: &str,
) -> Result<Option<Json>, KipError> {
    let Some(row) = store
        .find_transaction_by_idempotency_key(space, key)
        .await?
    else {
        return Ok(None);
    };
    if row.request_digest != request_digest {
        return Err(invalid("idempotency_conflict"));
    }
    Ok(Some(reply(store, &row.tx_id).await?))
}

pub(crate) async fn commit(
    store: &Store,
    mut tx: Transaction,
    key: String,
    request_digest: String,
    result: Json,
) -> Result<Json, KipError> {
    tx.runtime_result = Some(result);
    let tx_id = tx.cx.tx_id.clone();
    tx.commit(JournalEntry {
        idempotency_key: key,
        request_digest,
        ..Default::default()
    })
    .await?;
    reply(store, &tx_id).await
}

/// Last real-time fence check immediately before the redo plan is accepted.
pub(crate) async fn validate_commit_leases(
    store: &Store,
    space: &str,
    principal: &str,
    guards: &[(String, u64, u64)],
) -> Result<(), KipError> {
    let now = crate::tx::now_ms();
    for (reference, version, fence) in guards {
        let row = store
            .control_at(space, reference, u64::MAX)
            .await?
            .ok_or_else(|| conflict("lease_lost"))?;
        let wake: WakeRecord =
            serde_json::from_value(row.value).map_err(|_| invalid("corrupt wake record"))?;
        if row.version != *version
            || wake.fence != *fence
            || !matches!(&wake.state, WakeState::Running { lease }
                if lease.owner == principal && lease.expires_at_ms > now)
        {
            return Err(conflict("lease_lost"));
        }
    }
    Ok(())
}

/// The generic Governance control reader must not bypass the permissions on
/// work records or the cognitive elements they describe.
pub(crate) async fn authorize_control_read(
    session: &Session,
    authority: &EffectiveAuthority,
    space: &str,
    row: &ControlRecordRow,
) -> Result<(), KipError> {
    if row.kind == "wake" {
        work::load(session, authority, space, &row.key).await?;
    } else if row.kind == "dispatch" && row.key.starts_with("dispatch/v1/") {
        let request = &row.value["request"];
        let wake_ref = request["wake_ref"]
            .as_str()
            .ok_or_else(|| invalid("dispatch lacks wake reference"))?;
        work::load(session, authority, space, wake_ref).await?;
        let attempt_ref = request["attempt_ref"]
            .as_str()
            .ok_or_else(|| invalid("dispatch lacks attempt reference"))?;
        let attempt = session
            .nexus
            .store
            .get_element(attempt_ref.parse()?)
            .await?;
        if attempt.space() != space || attempt.state() != crate::store::rows::state::ACTIVE {
            return Err(KipError::not_found_or_not_visible(
                "dispatch attempt unavailable",
            ));
        }
        authority
            .authorize(
                Permission::Read,
                &ResourceContext::of_element(&attempt),
                &session.auth,
            )
            .into_result()?;
        if !authority
            .may_read(&attempt, &session.auth)
            .is_some_and(|v| v.content && v.constraints.fields.is_empty())
        {
            return Err(KipError::not_found_or_not_visible(
                "dispatch attempt is not fully visible",
            ));
        }
    } else if row.kind == "runtime" && row.key != CONFIG {
        if !row.key.starts_with("watch-evaluation/v1/") {
            return Err(KipError::not_authorized(
                "attention runtime records use their dedicated read or replay API",
            ));
        }
        let material = row.value["material"]["artifact_ref"]
            .as_str()
            .ok_or_else(|| invalid("evaluation material reference missing"))?;
        session
            .nexus
            .store
            .authorized_artifact(space, material, authority, &session.auth)
            .await?;
    }
    Ok(())
}

impl Session {
    /// Pins trusted host configuration. Scope is immutable after first arming;
    /// config changes invalidate old observation bases, never silently re-arm.
    pub async fn set_attention_config(
        &self,
        space: &str,
        expected: u64,
        config: AttentionConfig,
    ) -> Result<Json, KipError> {
        validate_config(&config)?;
        self.governed(space, Permission::ManagePolicy, async || {
            if let Some(old) = self.nexus.store.control_at(space, CONFIG, u64::MAX).await? {
                let previous: AttentionConfig = serde_json::from_value(old.value)
                    .map_err(|_| invalid("corrupt attention configuration"))?;
                if previous.scope != config.scope {
                    return Err(invalid("attention instance cannot be replaced in place"));
                }
            }
            let saved = self
                .nexus
                .store
                .publish_control(
                    space,
                    CONFIG,
                    "policy",
                    expected,
                    json!(config),
                    json!({"principal_id":self.auth.principal_id}),
                )
                .await?;
            Ok(json!({"version":saved.version,"config":saved.value}))
        })
        .await
    }
}
