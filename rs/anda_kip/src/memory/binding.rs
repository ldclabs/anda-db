//! # The Memory Interface wire shapes
//!
//! The optional Agent-to-Brain binding of `Memory-Interface.md`: five intents,
//! one request shape, processing receipts and a read barrier. These types
//! mirror `schemas/kip-memory.schema.json`; a Brain Adapter parses a
//! [`Request`], answers a [`Response`], and publishes a [`Descriptor`] in its
//! connection setup.
//!
//! A static shape proves nothing about source fidelity, authority, processing
//! progress, coverage, token budgets or erasure — the Adapter enforces those.
//! Nor does anything here give a raw Nexus a binding: a runtime advertises
//! `memory_interface` only when a connected Brain serves it (§2).
//!
//! [`MemorySession`] is the host-side receipt bookkeeping the specification
//! assigns to the session (§5.1): every outstanding receipt stays a barrier
//! until a trusted recall accounts for it, and the attention cursor is kept by
//! the host (§4). It grants nothing and never claims that processing finished.

use crate::{Json, KipError, MAX_SAFE_INTEGER, SpaceSelector};
use serde::{Deserialize, Serialize};

/// The binding's wire version, `kip_memory` on every request and response.
pub const KIP_MEMORY_VERSION: &str = "2.0";

/// The most receipts one recall may name in `after`, and one session may hold.
pub const MAX_AFTER: usize = 128;

/// The canonical task and context scope of a request (§3).
///
/// Exact references resolved by the host, never labels or message text.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context_refs: Vec<String>,
}

impl Scope {
    /// The same scope with `context_refs` sorted and deduplicated, the form
    /// the canonical context set of a ProjectionBasis compares (§25.3).
    pub fn canonical(&self) -> Self {
        let mut context_refs = self.context_refs.clone();
        context_refs.sort();
        context_refs.dedup();
        Self {
            task_ref: self.task_ref.clone(),
            context_refs,
        }
    }
}

/// Output and deadline budgets; the tokenizer defaults to the advertised one.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Budget {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokenizer: Option<String>,
}

/// A Memory Interface level (`profiles/memory-bundles.json`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Bundle {
    MemoryBasic,
    MemoryExperience,
    MemoryLearning,
}

impl Bundle {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MemoryBasic => "memory_basic",
            Self::MemoryExperience => "memory_experience",
            Self::MemoryLearning => "memory_learning",
        }
    }

    /// The level this one depends on, if any.
    pub fn requires(self) -> Option<Self> {
        match self {
            Self::MemoryBasic => None,
            Self::MemoryExperience => Some(Self::MemoryBasic),
            Self::MemoryLearning => Some(Self::MemoryExperience),
        }
    }
}

/// One of the five intents.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Observe,
    Recall,
    Revise,
    Feedback,
    Forget,
}

impl Operation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Observe => "observe",
            Self::Recall => "recall",
            Self::Revise => "revise",
            Self::Feedback => "feedback",
            Self::Forget => "forget",
        }
    }

    /// Whether the intent mutates memory, and so needs an idempotency key and
    /// answers with a receipt (§3, §5).
    pub fn is_mutation(self) -> bool {
        self != Self::Recall
    }
}

/// `observe`: capture or resolve a host-issued source.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObserveInput {
    pub source_ref: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecallMode {
    #[default]
    Answer,
    Action,
    Resume,
    Attention,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecallDetail {
    #[default]
    Brief,
    Evidence,
}

/// World time and retained cognitive history, kept apart (§4).
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecallTime {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub as_of_seq: Option<u64>,
}

/// `recall`: a briefing, an expansion of an earlier result, or attention.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecallInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<RecallMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal: Option<String>,
    /// Transient caller situation; never an implicit write.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
    /// Processing barriers: receipts whose effects this recall must include.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub after: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<RecallDetail>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<RecallTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attention_cursor: Option<String>,
}

impl RecallInput {
    /// The mode, `answer` when omitted.
    pub fn mode(&self) -> RecallMode {
        self.mode.unwrap_or_default()
    }

    /// The input rules the schema cannot say with types alone.
    pub fn validate(&self) -> Result<(), KipError> {
        if self.query.is_none()
            && self.target_ref.is_none()
            && self.mode != Some(RecallMode::Attention)
        {
            return Err(KipError::invalid_request_envelope(
                "recall needs a query, a target_ref or mode \"attention\"",
            ));
        }
        for (name, text) in [
            ("query", &self.query),
            ("goal", &self.goal),
            ("context", &self.context),
        ] {
            if let Some(text) = text
                && (text.is_empty() || text.chars().count() > 4096)
            {
                return Err(KipError::invalid_request_envelope(format!(
                    "recall {name} must be 1..=4096 characters"
                )));
            }
        }
        check_refs("after", &self.after)?;
        for reference in self.target_ref.iter().chain(&self.attention_cursor) {
            check_ref(reference)?;
        }
        if let Some(time) = &self.time {
            if time.valid_at.is_none() && time.as_of_seq.is_none() {
                return Err(KipError::invalid_request_envelope(
                    "recall time names valid_at, as_of_seq or both",
                ));
            }
            if let Some(valid_at) = &time.valid_at {
                crate::timestamp::parse(valid_at, "time.valid_at")?;
            }
            if time.as_of_seq.is_some_and(|seq| seq > MAX_SAFE_INTEGER) {
                return Err(KipError::invalid_request_envelope(
                    "as_of_seq must be a non-negative safe integer",
                ));
            }
        }
        Ok(())
    }
}

/// Which of three histories a revision describes (§4, Spec §14.2).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    /// The actor's earlier claim was wrong: supersession by the same actor.
    Correction,
    /// The world moved on: one new Assertion; temporal succession ends the old.
    WorldChange,
    /// The Brain recorded what the actor never said: recording repair (§57.8).
    Misrecorded,
    /// The Adapter decides and discloses; it never supersedes on a guess.
    #[default]
    Unspecified,
}

/// `revise`: a captured correction, change or misrecording report.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReviseInput {
    pub source_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_kind: Option<ChangeKind>,
}

/// `feedback`: preserved with its actual origin; never automatically a grade.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackInput {
    pub source_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_ref: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ForgetMode {
    PayloadOnly,
    Semantic,
}

/// `forget`: a bounded, governed ErasurePlan over an exact target.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ForgetInput {
    pub target_ref: String,
    pub mode: ForgetMode,
}

/// A request's input, read by its operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Intent {
    Observe(ObserveInput),
    Recall(RecallInput),
    Revise(ReviseInput),
    Feedback(FeedbackInput),
    Forget(ForgetInput),
}

/// One intent, one request (§1, §3).
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub kip_memory: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    pub operation: Operation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub space: Option<SpaceSelector>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<Scope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget: Option<Budget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// The levels the request needs; omitted means `memory_basic` (§3).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requires: Vec<Bundle>,
    pub input: Json,
}

impl Request {
    /// The typed input for this request's operation, after the envelope
    /// rules the schema states: the version, an idempotency key on every
    /// mutation and none on a recall, and each input's own rules.
    pub fn intent(&self) -> Result<Intent, KipError> {
        if self.kip_memory != KIP_MEMORY_VERSION {
            return Err(KipError::invalid_request_envelope(format!(
                "kip_memory must be {KIP_MEMORY_VERSION:?}, got {:?}",
                self.kip_memory
            )));
        }
        match (&self.idempotency_key, self.operation.is_mutation()) {
            (None, true) => {
                return Err(KipError::invalid_request_envelope(format!(
                    "{} is a mutation and requires an idempotency_key",
                    self.operation.as_str()
                )));
            }
            (Some(_), false) => {
                return Err(KipError::invalid_request_envelope(
                    "recall is read-only and takes no idempotency_key",
                ));
            }
            (Some(key), true) => check_ref(key)?,
            (None, false) => {}
        }
        if let Some(scope) = &self.scope {
            if let Some(task) = &scope.task_ref {
                check_ref(task)?;
            }
            check_refs("context_refs", &scope.context_refs)?;
        }
        let input = self.input.clone();
        let parse = |error: serde_json::Error| {
            KipError::invalid_request_envelope(format!(
                "invalid {} input: {error}",
                self.operation.as_str()
            ))
        };
        let intent = match self.operation {
            Operation::Observe => {
                let input: ObserveInput = serde_json::from_value(input).map_err(parse)?;
                check_ref(&input.source_ref)?;
                Intent::Observe(input)
            }
            Operation::Recall => {
                let input: RecallInput = serde_json::from_value(input).map_err(parse)?;
                input.validate()?;
                Intent::Recall(input)
            }
            Operation::Revise => {
                let input: ReviseInput = serde_json::from_value(input).map_err(parse)?;
                check_ref(&input.source_ref)?;
                if let Some(target) = &input.target_ref {
                    check_ref(target)?;
                }
                Intent::Revise(input)
            }
            Operation::Feedback => {
                let input: FeedbackInput = serde_json::from_value(input).map_err(parse)?;
                for reference in std::iter::once(&input.source_ref)
                    .chain(&input.decision_ref)
                    .chain(&input.attempt_ref)
                {
                    check_ref(reference)?;
                }
                Intent::Feedback(input)
            }
            Operation::Forget => {
                let input: ForgetInput = serde_json::from_value(input).map_err(parse)?;
                check_ref(&input.target_ref)?;
                Intent::Forget(input)
            }
        };
        Ok(intent)
    }

    /// The levels this request needs, `memory_basic` when it names none.
    pub fn required_bundles(&self) -> Vec<Bundle> {
        if self.requires.is_empty() {
            vec![Bundle::MemoryBasic]
        } else {
            self.requires.clone()
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Succeeded,
    Pending,
    Partial,
    Failed,
}

/// The immutable intake acknowledgement (§5). Distinct from a KIP
/// Transaction Receipt: one intent may produce several later transactions.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub receipt_ref: String,
    pub operation: Operation,
    pub space_id: String,
    pub accepted_seq: u64,
}

/// Processing phases; monotone except that recorded or processed may fail.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Recorded,
    Processed,
    Available,
    Failed,
}

/// Terminal dispositions; `erased` only for a completed forget.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Disposition {
    Formed,
    EvidenceOnly,
    Skipped,
    Erased,
}

/// Current progress of one receipt: a read view, never a rewritten outcome.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Progress {
    pub receipt_ref: String,
    pub phase: Phase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disposition: Option<Disposition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_seq: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub available_seq: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<crate::ErrorObject>,
}

impl Progress {
    /// The phase invariants of §5: `processed` pins its disposition and
    /// `resolved_seq`, `available` adds `available_seq` and keeps
    /// `resolved_seq <= available_seq`, `failed` carries its error, and
    /// `recorded` claims no effects.
    pub fn validate(&self, accepted_seq: Option<u64>) -> Result<(), KipError> {
        let invalid = |message: &str| {
            Err(KipError::constraint_violation(format!(
                "progress of {}: {message}",
                self.receipt_ref
            )))
        };
        match self.phase {
            Phase::Recorded => {
                if self.disposition.is_some()
                    || self.resolved_seq.is_some()
                    || self.available_seq.is_some()
                {
                    return invalid("recorded work claims no disposition or sequence");
                }
            }
            Phase::Processed | Phase::Available => {
                let (Some(disposition), Some(resolved)) = (self.disposition, self.resolved_seq)
                else {
                    return invalid("processed work names its disposition and resolved_seq");
                };
                if disposition == Disposition::Erased && self.phase != Phase::Available {
                    return invalid("erased is reported only once erasure is available");
                }
                if accepted_seq.is_some_and(|accepted| resolved < accepted) {
                    return invalid("resolved_seq precedes accepted_seq");
                }
                if self.phase == Phase::Available {
                    let Some(available) = self.available_seq else {
                        return invalid("available work names its available_seq");
                    };
                    if available < resolved {
                        return invalid("available_seq precedes resolved_seq");
                    }
                } else if self.available_seq.is_some() {
                    return invalid("processed work is not yet available");
                }
            }
            Phase::Failed => {
                if self.error.is_none() {
                    return invalid("failed work carries its error");
                }
            }
        }
        Ok(())
    }

    /// Whether this progress satisfies a barrier for a recall whose basis is
    /// at `snapshot_seq` (§5): available, and no newer than the basis.
    pub fn satisfies(&self, snapshot_seq: u64) -> bool {
        self.phase == Phase::Available && self.available_seq.is_some_and(|seq| seq <= snapshot_seq)
    }
}

/// What observe, revise and feedback formed.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FormationResult {
    pub summary: String,
    pub memory_refs: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ForgetStatus {
    Pending,
    Partial,
    Blocked,
    Completed,
}

/// An erasure operation; `completed` only after every in-scope surface is
/// verified (§4, Spec §60.7).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ForgetResult {
    pub status: ForgetStatus,
    pub plan_ref: String,
    pub summary: String,
    pub coverage_ref: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemRole {
    Fact,
    Constraint,
    Experience,
    Procedure,
    Warning,
    /// Raw source material, never silently accepted knowledge (§6).
    Source,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EpistemicStatus {
    Accepted,
    Rejected,
    Contested,
    Uncertain,
    /// Not enough basis — never "no".
    Insufficient,
    NotApplicable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Standing {
    Unproven,
    Validated,
    Revoked,
    Unverifiable,
}

/// One typed item of a briefing.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryItem {
    #[serde(rename = "ref")]
    pub reference: String,
    pub text: String,
    pub role: ItemRole,
    pub epistemic_status: EpistemicStatus,
    pub evidence_refs: Vec<String>,
    /// Memory sufficiency, never authorization to act.
    pub action_eligible: bool,
    /// Required for a procedure (§6).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub standing: Option<Standing>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelState {
    Complete,
    Incomplete,
    /// Only after an authoritative scoped absence or irrelevance determination.
    NotApplicable,
}

/// The seven recall channels (§6, Profile §20.2).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Channels {
    pub constraints: ChannelState,
    pub commitments: ChannelState,
    pub dependencies: ChannelState,
    pub failures: ChannelState,
    pub experiences: ChannelState,
    pub skills: ChannelState,
    pub evidence: ChannelState,
}

impl Channels {
    /// Every channel in one state; a starting point, not a determination.
    pub fn all(state: ChannelState) -> Self {
        Self {
            constraints: state,
            commitments: state,
            dependencies: state,
            failures: state,
            experiences: state,
            skills: state,
            evidence: state,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&'static str, ChannelState)> {
        [
            ("constraints", self.constraints),
            ("commitments", self.commitments),
            ("dependencies", self.dependencies),
            ("failures", self.failures),
            ("experiences", self.experiences),
            ("skills", self.skills),
            ("evidence", self.evidence),
        ]
        .into_iter()
    }

    /// Whether no channel is incomplete.
    pub fn complete(&self) -> bool {
        self.iter()
            .all(|(_, state)| state != ChannelState::Incomplete)
    }
}

/// Declared coverage of a briefing.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Coverage {
    pub complete: bool,
    pub scope: Scope,
    pub channels: Channels,
    pub pending_receipts: Vec<String>,
    pub unverified_preconditions: Vec<String>,
    pub action_eligible: bool,
}

impl Coverage {
    /// Derives `complete` and `action_eligible` from the parts, so neither can
    /// be claimed past a truncated channel, an unsatisfied barrier or an
    /// unverified precondition (§6).
    pub fn new(
        scope: Scope,
        channels: Channels,
        pending_receipts: Vec<String>,
        unverified_preconditions: Vec<String>,
    ) -> Self {
        let complete = channels.complete() && pending_receipts.is_empty();
        let action_eligible = complete && unverified_preconditions.is_empty();
        Self {
            complete,
            scope,
            channels,
            pending_receipts,
            unverified_preconditions,
            action_eligible,
        }
    }
}

/// `detail: "evidence"`: the retained basis and coverage in normative shapes.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Details {
    /// A ProjectionBasis (`kip-projection.schema.json`).
    pub basis: Json,
    /// A RecallCoverage (`kip-cognitive-records.schema.json`).
    pub coverage: Json,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub elements: Vec<Json>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttentionKind {
    WatchFired,
    CommitmentDue,
}

/// A fired Watch or a due Commitment (§4). It grants nothing.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AttentionItem {
    #[serde(rename = "ref")]
    pub reference: String,
    pub kind: AttentionKind,
    pub summary: String,
    /// The `space_seq` of the commit that raised it.
    pub raised_seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub due_at: Option<String>,
    pub target_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<f64>,
}

/// The recall result.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Briefing {
    pub summary: String,
    pub items: Vec<MemoryItem>,
    pub uncertainties: Vec<String>,
    pub basis_ref: String,
    pub coverage: Coverage,
    /// The progress of every receipt the request named in `after`.
    pub after: Vec<Progress>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Details>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attention: Option<Vec<AttentionItem>>,
    /// The last attention item delivered, or the cursor given for an empty page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attention_cursor: Option<String>,
}

/// One response per request.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Response {
    pub kip_memory: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    pub operation: Operation,
    pub status: Status,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt: Option<Receipt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<Progress>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Json>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<crate::ErrorObject>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

impl Response {
    /// A failed response carrying the error; mutations may add a receipt when
    /// intake was durable before the failure.
    pub fn failed(request: &Request, error: &KipError) -> Self {
        Self {
            kip_memory: KIP_MEMORY_VERSION.into(),
            request_id: request.request_id.clone(),
            operation: request.operation,
            status: Status::Failed,
            receipt: None,
            progress: None,
            result: None,
            error: Some(crate::ErrorObject::from(error.clone())),
            warnings: Vec::new(),
        }
    }
}

/// What a deployment advertising `memory_interface` publishes (§2).
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Descriptor {
    pub kip_memory: String,
    pub bundles: Vec<Bundle>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_scope: Option<Scope>,
    pub default_budget: Budget,
    pub tokenizer: String,
    pub minimum_response_tokens: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_space: Option<SpaceSelector>,
}

impl Descriptor {
    /// The rules a host must satisfy before advertising: the version, at least
    /// `memory_basic`, every level's dependency advertised transitively, and a
    /// default budget with both an output and a deadline bound.
    pub fn validate(&self) -> Result<(), KipError> {
        if self.kip_memory != KIP_MEMORY_VERSION {
            return Err(KipError::constraint_violation(format!(
                "descriptor kip_memory must be {KIP_MEMORY_VERSION:?}"
            )));
        }
        if self.bundles.is_empty() || !self.bundles.contains(&Bundle::MemoryBasic) {
            return Err(KipError::constraint_violation(
                "a Memory Interface descriptor advertises at least memory_basic",
            ));
        }
        let mut seen = std::collections::BTreeSet::new();
        for bundle in &self.bundles {
            if !seen.insert(*bundle) {
                return Err(KipError::constraint_violation(format!(
                    "{} is advertised twice",
                    bundle.as_str()
                )));
            }
            if let Some(dependency) = bundle.requires()
                && !self.bundles.contains(&dependency)
            {
                return Err(KipError::unsupported_capability(format!(
                    "{} must advertise its dependency {}",
                    bundle.as_str(),
                    dependency.as_str()
                )));
            }
        }
        let budget = &self.default_budget;
        if budget
            .max_output_tokens
            .is_none_or(|v| v == 0 || v > MAX_SAFE_INTEGER)
            || budget
                .deadline_ms
                .is_none_or(|v| v == 0 || v > MAX_SAFE_INTEGER)
        {
            return Err(KipError::constraint_violation(
                "default_budget names max_output_tokens and deadline_ms",
            ));
        }
        if self.tokenizer.is_empty() || self.minimum_response_tokens == 0 {
            return Err(KipError::constraint_violation(
                "a descriptor names its tokenizer and a positive minimum_response_tokens",
            ));
        }
        Ok(())
    }

    /// Whether a level is advertised.
    pub fn advertises(&self, bundle: Bundle) -> bool {
        self.bundles.contains(&bundle)
    }

    /// Checks a request's `requires` against the advertised levels (§3):
    /// every named level must be available; omitting the list means
    /// `memory_basic`, never a guessed advanced level.
    pub fn check_requires(&self, request: &Request) -> Result<(), KipError> {
        for bundle in request.required_bundles() {
            if !self.advertises(bundle) {
                return Err(KipError::unsupported_capability(format!(
                    "this binding does not advertise {}",
                    bundle.as_str()
                )));
            }
        }
        Ok(())
    }
}

/// The persisted state of a [`MemorySession`]; checkpoint it with the
/// caller's session.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MemorySessionSnapshot {
    pub space_id: String,
    #[serde(default)]
    pub scope: Scope,
    #[serde(default)]
    pub outstanding: Vec<String>,
    /// The cursor of the last attention the host consumed (§4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attention_cursor: Option<String>,
}

/// Host-side receipt bookkeeping (§5.1), the Rust counterpart of
/// `@ldclabs/kip-lang`'s `MemorySession`.
///
/// Handles and scope must come from the authenticated binding, not model
/// text. Every acknowledged receipt stays outstanding until a trusted recall
/// accounts for it; a maximum sequence, an index watermark or an intake
/// acknowledgement never clears one. Overflow is refused rather than dropping
/// an older pending receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemorySession {
    state: MemorySessionSnapshot,
}

impl MemorySession {
    pub fn new(snapshot: MemorySessionSnapshot) -> Result<Self, KipError> {
        if snapshot.space_id.is_empty()
            || snapshot.outstanding.len() > MAX_AFTER
            || snapshot.outstanding.iter().any(|r| check_ref(r).is_err())
            || snapshot
                .attention_cursor
                .as_ref()
                .is_some_and(|c| check_ref(c).is_err())
        {
            return Err(KipError::constraint_violation(
                "invalid memory session snapshot",
            ));
        }
        let mut outstanding = Vec::with_capacity(snapshot.outstanding.len());
        for receipt in snapshot.outstanding {
            if !outstanding.contains(&receipt) {
                outstanding.push(receipt);
            }
        }
        Ok(Self {
            state: MemorySessionSnapshot {
                space_id: snapshot.space_id,
                scope: snapshot.scope.canonical(),
                outstanding,
                attention_cursor: snapshot.attention_cursor,
            },
        })
    }

    /// Keeps a receipt acknowledged by this session's Space as a barrier.
    pub fn record_receipt(&mut self, space_id: &str, receipt_ref: &str) -> Result<(), KipError> {
        if space_id != self.state.space_id {
            return Err(KipError::constraint_violation("receipt scope mismatch"));
        }
        check_ref(receipt_ref)?;
        if self.state.outstanding.iter().any(|r| r == receipt_ref) {
            return Ok(());
        }
        if self.state.outstanding.len() >= MAX_AFTER {
            return Err(KipError::result_limit_exceeded(
                "processing barrier limit exceeded; retain the receipt externally",
            ));
        }
        self.state.outstanding.push(receipt_ref.to_string());
        Ok(())
    }

    /// Builds a scoped recall. Every outstanding receipt joins `after`;
    /// `attention` and `resume` carry the kept attention cursor unless the
    /// caller names one, so a restart neither replays consumed attention nor
    /// skips raised items.
    pub fn recall(&self, mut input: RecallInput) -> Result<Request, KipError> {
        input.validate()?;
        let mut after = self.state.outstanding.clone();
        for receipt in input.after.drain(..) {
            if !after.contains(&receipt) {
                after.push(receipt);
            }
        }
        if after.len() > MAX_AFTER {
            return Err(KipError::result_limit_exceeded(
                "processing barrier limit exceeded",
            ));
        }
        input.after = after;
        if input.attention_cursor.is_none()
            && matches!(input.mode, Some(RecallMode::Attention | RecallMode::Resume))
        {
            input.attention_cursor = self.state.attention_cursor.clone();
        }
        Ok(Request {
            kip_memory: KIP_MEMORY_VERSION.into(),
            request_id: None,
            operation: Operation::Recall,
            space: Some(SpaceSelector {
                id: Some(self.state.space_id.clone()),
                uri: None,
            }),
            scope: Some(self.state.scope.clone()),
            budget: None,
            idempotency_key: None,
            requires: Vec::new(),
            input: serde_json::to_value(input)
                .map_err(|e| KipError::internal_error(e.to_string()))?,
        })
    }

    /// Clears receipts a trusted successful recall actually accounted for.
    pub fn acknowledge_recall<S: AsRef<str>>(&mut self, accounted: &[S]) {
        self.state
            .outstanding
            .retain(|r| !accounted.iter().any(|a| a.as_ref() == r));
    }

    /// Keeps the cursor a trusted successful `attention` or `resume` recall
    /// returned, once the host has taken its items. Consuming attention
    /// changes nothing in memory; it only moves this cursor.
    pub fn acknowledge_attention(&mut self, attention_cursor: &str) -> Result<(), KipError> {
        check_ref(attention_cursor)?;
        self.state.attention_cursor = Some(attention_cursor.to_string());
        Ok(())
    }

    pub fn outstanding(&self) -> &[String] {
        &self.state.outstanding
    }

    pub fn snapshot(&self) -> MemorySessionSnapshot {
        self.state.clone()
    }
}

/// A reference: 1..=1024 characters.
fn check_ref(reference: &str) -> Result<(), KipError> {
    if reference.is_empty() || reference.chars().count() > 1024 {
        return Err(KipError::invalid_request_envelope(
            "a reference is 1..=1024 characters",
        ));
    }
    Ok(())
}

fn check_refs(name: &str, references: &[String]) -> Result<(), KipError> {
    if references.len() > MAX_AFTER {
        return Err(KipError::invalid_request_envelope(format!(
            "{name} names at most {MAX_AFTER} references"
        )));
    }
    for (index, reference) in references.iter().enumerate() {
        check_ref(reference)?;
        if references[..index].contains(reference) {
            return Err(KipError::invalid_request_envelope(format!(
                "{name} names {reference:?} twice"
            )));
        }
    }
    Ok(())
}

#[cfg(feature = "schema-validation")]
pub use validate::{validate_descriptor, validate_request, validate_response};

/// Validation against the vendored `kip-memory.schema.json` and its closure.
#[cfg(feature = "schema-validation")]
mod validate {
    use crate::{Json, KipError, Map};
    use jsonschema::{Retrieve, Uri, Validator};
    use std::sync::LazyLock;

    const DOCUMENTS: &[&str] = &[
        crate::memory::MEMORY_SCHEMA,
        crate::memory::COMMON_SCHEMA,
        crate::memory::REQUEST_SCHEMA,
        crate::memory::RESPONSE_SCHEMA,
        crate::memory::PROJECTION_SCHEMA,
        crate::memory::COGNITIVE_RECORDS_SCHEMA,
        crate::memory::ELEMENT_SCHEMA,
    ];

    struct Pinned(Map<String, Json>);
    impl Retrieve for Pinned {
        fn retrieve(
            &self,
            uri: &Uri<String>,
        ) -> Result<Json, Box<dyn std::error::Error + Send + Sync>> {
            self.0
                .get(uri.as_str())
                .cloned()
                .ok_or_else(|| format!("unpinned schema resource {uri}").into())
        }
    }

    fn compile(definition: &str) -> Validator {
        let resources = DOCUMENTS
            .iter()
            .map(|text| {
                let value: Json = serde_json::from_str(text).expect("vendored schema");
                (value["$id"].as_str().expect("schema id").to_string(), value)
            })
            .collect();
        jsonschema::options()
            .with_draft(jsonschema::Draft::Draft202012)
            .should_validate_formats(true)
            .with_format("date-time", |value| {
                crate::timestamp::parse(value, "timestamp").is_ok()
            })
            .with_format("timestamp", |value| {
                crate::timestamp::parse(value, "timestamp").is_ok()
            })
            .with_retriever(Pinned(resources))
            .build(&serde_json::json!({
                "$ref": format!("urn:kip:2.0:schema:memory#/$defs/{definition}")
            }))
            .expect("vendored memory schema compiles")
    }

    static REQUEST: LazyLock<Validator> = LazyLock::new(|| compile("Request"));
    static RESPONSE: LazyLock<Validator> = LazyLock::new(|| compile("Response"));
    static DESCRIPTOR: LazyLock<Validator> = LazyLock::new(|| compile("Descriptor"));

    fn check(validator: &Validator, value: &Json, what: &str) -> Result<(), String> {
        validator
            .validate(value)
            .map_err(|error| format!("{what} does not match kip-memory.schema.json: {error}"))
    }

    /// A request against `$defs/Request`; a mismatch is `InvalidRequestEnvelope`.
    pub fn validate_request(value: &Json) -> Result<(), KipError> {
        check(&REQUEST, value, "memory request").map_err(KipError::invalid_request_envelope)
    }

    /// A response against `$defs/Response`; a mismatch is an Adapter bug.
    pub fn validate_response(value: &Json) -> Result<(), KipError> {
        check(&RESPONSE, value, "memory response").map_err(KipError::internal_error)
    }

    /// A descriptor against `$defs/Descriptor`.
    pub fn validate_descriptor(value: &Json) -> Result<(), KipError> {
        check(&DESCRIPTOR, value, "memory descriptor").map_err(KipError::constraint_violation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn request(value: Json) -> Request {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn mutations_need_a_key_and_recall_takes_none() {
        let observe = request(json!({
            "kip_memory": "2.0", "operation": "observe",
            "input": {"source_ref": "source-77"}
        }));
        assert_eq!(
            observe.intent().unwrap_err().code,
            crate::KipErrorCode::InvalidRequestEnvelope
        );
        let keyed = request(json!({
            "kip_memory": "2.0", "operation": "observe", "idempotency_key": "observe:source-77",
            "scope": {"task_ref": "task-9"}, "input": {"source_ref": "source-77"}
        }));
        assert_eq!(
            keyed.intent().unwrap(),
            Intent::Observe(ObserveInput {
                source_ref: "source-77".into()
            })
        );
        let recall = request(json!({
            "kip_memory": "2.0", "operation": "recall", "idempotency_key": "k",
            "input": {"query": "q"}
        }));
        assert!(recall.intent().is_err());
        let empty = request(json!({"kip_memory": "2.0", "operation": "recall", "input": {}}));
        assert!(empty.intent().is_err());
        let attention = request(json!({
            "kip_memory": "2.0", "operation": "recall",
            "input": {"mode": "attention", "attention_cursor": "attention:12:C-1"}
        }));
        assert!(
            matches!(attention.intent().unwrap(), Intent::Recall(r) if r.mode() == RecallMode::Attention)
        );
        let unknown = request(json!({
            "kip_memory": "2.0", "operation": "forget", "idempotency_key": "k",
            "input": {"target_ref": "A-1", "mode": "semantic", "extra": 1}
        }));
        assert!(unknown.intent().is_err());
        let bad_time = request(json!({
            "kip_memory": "2.0", "operation": "recall",
            "input": {"query": "q", "time": {"valid_at": "2026-01-01T00:00:00Z"}}
        }));
        assert!(bad_time.intent().is_err());
        assert_eq!(keyed.required_bundles(), vec![Bundle::MemoryBasic]);
    }

    #[test]
    fn descriptors_advertise_levels_transitively() {
        let mut descriptor = Descriptor {
            kip_memory: "2.0".into(),
            bundles: vec![Bundle::MemoryBasic],
            default_scope: None,
            default_budget: Budget {
                max_output_tokens: Some(4096),
                deadline_ms: Some(30_000),
                tokenizer: None,
            },
            tokenizer: "o200k_base".into(),
            minimum_response_tokens: 256,
            default_space: None,
        };
        descriptor.validate().unwrap();
        let learning = request(json!({
            "kip_memory": "2.0", "operation": "recall", "requires": ["memory_learning"],
            "input": {"query": "q"}
        }));
        assert_eq!(
            descriptor.check_requires(&learning).unwrap_err().code,
            crate::KipErrorCode::UnsupportedCapability
        );
        descriptor.bundles = vec![Bundle::MemoryBasic, Bundle::MemoryLearning];
        assert!(descriptor.validate().is_err());
        descriptor.bundles = vec![Bundle::MemoryExperience];
        assert!(descriptor.validate().is_err());
        descriptor.bundles = vec![
            Bundle::MemoryBasic,
            Bundle::MemoryExperience,
            Bundle::MemoryLearning,
        ];
        descriptor.validate().unwrap();
        descriptor.check_requires(&learning).unwrap();
        descriptor.default_budget.deadline_ms = None;
        assert!(descriptor.validate().is_err());
    }

    #[test]
    fn progress_phases_keep_their_sequence_order() {
        let mut progress = Progress {
            receipt_ref: "r-1".into(),
            phase: Phase::Recorded,
            disposition: None,
            resolved_seq: None,
            available_seq: None,
            reason: None,
            error: None,
        };
        progress.validate(Some(5)).unwrap();
        assert!(!progress.satisfies(u64::MAX));
        progress.phase = Phase::Processed;
        assert!(progress.validate(Some(5)).is_err());
        progress.disposition = Some(Disposition::Formed);
        progress.resolved_seq = Some(7);
        progress.validate(Some(5)).unwrap();
        assert!(progress.validate(Some(8)).is_err());
        progress.phase = Phase::Available;
        progress.available_seq = Some(6);
        assert!(progress.validate(Some(5)).is_err());
        progress.available_seq = Some(7);
        progress.validate(Some(5)).unwrap();
        assert!(progress.satisfies(7));
        assert!(!progress.satisfies(6));
        progress.phase = Phase::Failed;
        assert!(progress.validate(None).is_err());
    }

    #[test]
    fn coverage_cannot_claim_eligibility_past_a_gap() {
        let complete = Coverage::new(
            Scope::default(),
            Channels::all(ChannelState::Complete),
            vec![],
            vec![],
        );
        assert!(complete.complete && complete.action_eligible);
        let mut channels = Channels::all(ChannelState::Complete);
        channels.constraints = ChannelState::Incomplete;
        let truncated = Coverage::new(Scope::default(), channels, vec![], vec![]);
        assert!(!truncated.complete && !truncated.action_eligible);
        let pending = Coverage::new(
            Scope::default(),
            Channels::all(ChannelState::NotApplicable),
            vec!["r-1".into()],
            vec![],
        );
        assert!(!pending.action_eligible);
        let unverified = Coverage::new(
            Scope::default(),
            Channels::all(ChannelState::Complete),
            vec![],
            vec!["deploy window".into()],
        );
        assert!(unverified.complete && !unverified.action_eligible);
    }

    #[test]
    fn a_session_keeps_every_outstanding_receipt_and_the_attention_cursor() {
        let mut session = MemorySession::new(MemorySessionSnapshot {
            space_id: "space".into(),
            scope: Scope {
                task_ref: Some("task-9".into()),
                context_refs: vec!["C-2".into(), "C-1".into(), "C-2".into()],
            },
            ..Default::default()
        })
        .unwrap();
        assert!(session.record_receipt("other", "r-A").is_err());
        session.record_receipt("space", "r-A").unwrap();
        session.record_receipt("space", "r-B").unwrap();
        session.record_receipt("space", "r-A").unwrap();
        // B finishing first clears B only; A stays a barrier.
        session.acknowledge_recall(&["r-B"]);
        let recall = session
            .recall(RecallInput {
                query: Some("what changed?".into()),
                after: vec!["r-C".into()],
                ..Default::default()
            })
            .unwrap();
        assert_eq!(recall.input["after"], json!(["r-A", "r-C"]));
        assert_eq!(
            recall.scope.as_ref().unwrap().context_refs,
            vec!["C-1", "C-2"]
        );
        assert_eq!(recall.space.as_ref().unwrap().id.as_deref(), Some("space"));
        assert!(recall.intent().is_ok());

        session.acknowledge_attention("attention:12:C-1").unwrap();
        let attention = session
            .recall(RecallInput {
                mode: Some(RecallMode::Attention),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(attention.input["attention_cursor"], "attention:12:C-1");
        let answer = session
            .recall(RecallInput {
                query: Some("q".into()),
                ..Default::default()
            })
            .unwrap();
        assert!(answer.input.get("attention_cursor").is_none());

        // A restart restores the same barriers and cursor.
        let restored = MemorySession::new(session.snapshot()).unwrap();
        assert_eq!(restored, session);

        for n in 0..(MAX_AFTER - 1) {
            session.record_receipt("space", &format!("r-{n}")).unwrap();
        }
        assert_eq!(
            session.record_receipt("space", "r-over").unwrap_err().code,
            crate::KipErrorCode::ResultLimitExceeded
        );
        assert_eq!(session.outstanding().len(), MAX_AFTER);
        assert!(session.outstanding().contains(&"r-A".to_string()));
    }

    #[cfg(feature = "schema-validation")]
    #[test]
    fn wire_shapes_validate_against_the_vendored_schema() {
        let session = MemorySession::new(MemorySessionSnapshot {
            space_id: "space".into(),
            ..Default::default()
        })
        .unwrap();
        let recall = session
            .recall(RecallInput {
                query: Some("q".into()),
                after: vec!["r-1".into()],
                ..Default::default()
            })
            .unwrap();
        validate_request(&serde_json::to_value(&recall).unwrap()).unwrap();
        assert!(
            validate_request(
                &json!({"kip_memory": "2.0", "operation": "observe", "input": {"source_ref": "s"}})
            )
            .is_err()
        );

        let briefing = Briefing {
            summary: "Nothing new.".into(),
            items: vec![MemoryItem {
                reference: "A-1".into(),
                text: "Alice lives in Shanghai.".into(),
                role: ItemRole::Fact,
                epistemic_status: EpistemicStatus::Accepted,
                evidence_refs: vec!["E-1".into()],
                action_eligible: true,
                standing: None,
            }],
            uncertainties: vec![],
            basis_ref: "basis-1".into(),
            coverage: Coverage::new(
                Scope::default(),
                Channels::all(ChannelState::Complete),
                vec![],
                vec![],
            ),
            after: vec![Progress {
                receipt_ref: "r-1".into(),
                phase: Phase::Available,
                disposition: Some(Disposition::Formed),
                resolved_seq: Some(3),
                available_seq: Some(3),
                reason: None,
                error: None,
            }],
            continuation_ref: None,
            details: None,
            attention: None,
            attention_cursor: None,
        };
        let response = Response {
            kip_memory: "2.0".into(),
            request_id: None,
            operation: Operation::Recall,
            status: Status::Succeeded,
            receipt: None,
            progress: None,
            result: Some(serde_json::to_value(&briefing).unwrap()),
            error: None,
            warnings: vec![],
        };
        validate_response(&serde_json::to_value(&response).unwrap()).unwrap();

        let failed = Response::failed(
            &recall,
            &KipError::unsupported_capability("no memory_learning"),
        );
        validate_response(&serde_json::to_value(&failed).unwrap()).unwrap();

        let descriptor = json!({
            "kip_memory": "2.0", "bundles": ["memory_basic"],
            "default_budget": {"max_output_tokens": 4096, "deadline_ms": 30000},
            "tokenizer": "o200k_base", "minimum_response_tokens": 256
        });
        validate_descriptor(&descriptor).unwrap();
        let typed: Descriptor = serde_json::from_value(descriptor).unwrap();
        typed.validate().unwrap();
        assert!(
            validate_descriptor(&json!({"kip_memory": "2.0", "bundles": ["memory_learning"]}))
                .is_err()
        );
    }
}
