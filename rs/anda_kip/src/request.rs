//! # The KIP 2.0 runtime envelope (Spec §70–§85)
//!
//! JSON is the baseline logical request/response format, and the envelope is
//! transport-neutral: MCP, HTTP, IPC, WebSocket or canister calls must all show
//! equivalent KIP semantics (§70.1).
//!
//! Three identities that are routinely confused and must not be (§72):
//!
//! ```text
//! request_id       one transport/execution attempt
//! idempotency_key  one logical mutation intent
//! tx_id            an engine-assigned transaction fact
//! ```
//!
//! And two things this module refuses to let a caller blur:
//!
//! - a declared `language` label cannot downgrade a write into read-only
//!   semantics — the parsed command is authoritative (§73.1);
//! - `operations[]` is a batch, not a transaction, unless `execution.mode` says
//!   `atomic` (§75.4).

use serde::{Deserialize, Serialize};

use crate::ast::{Command, CommandType, Json, Map};
use crate::error::{ErrorObject, KipError, KipErrorCode};
use crate::parser::{MAX_KIP_BATCH_COMMANDS, parse_kip, validate_command};

/// The protocol profile this crate speaks.
pub const KIP_VERSION: &str = "2.0";

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

/// The KIP 2.0 request envelope (Spec §71).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Request {
    /// The requested protocol profile; always `"2.0"` here.
    pub kip: String,
    /// One transport/execution attempt. Not an idempotency key, not a `tx_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// Which MemorySpace the request runs against.
    ///
    /// A Space is never inferred from conversation context (§5.5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub space: Option<SpaceSelector>,
    /// An explicit compatibility profile, e.g. `kip-1-compat`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compatibility_profile: Option<String>,
    /// How the operations relate to one another.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<Execution>,
    /// The read coordinate to bind this request to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read: Option<ReadBinding>,
    /// Source material the runtime mints into Evidence (§71.1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingest: Option<IngestContext>,
    /// Space/schema preconditions for the whole request (§35.4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preconditions: Option<Preconditions>,
    /// The operations to run; at least one.
    pub operations: Vec<Operation>,
    /// Request-level parameter bindings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Map<String, Json>>,
    /// Non-authoritative context: purpose, risk, locale, client.
    ///
    /// None of it grants identity, access, representation or authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<RequestContext>,
    /// Fail-fast capability preconditions (§67).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requires: Option<Map<String, Json>>,
    /// Deadline and dry-run options.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<RequestOptions>,
    /// Namespaced extensions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Map<String, Json>>,
}

impl Default for Request {
    fn default() -> Self {
        Self {
            kip: KIP_VERSION.to_string(),
            request_id: None,
            space: None,
            compatibility_profile: None,
            execution: None,
            read: None,
            ingest: None,
            preconditions: None,
            operations: Vec::new(),
            parameters: None,
            context: None,
            requires: None,
            options: None,
            extensions: None,
        }
    }
}

impl Request {
    /// Builds a single-operation request from one command string.
    pub fn single(command: impl Into<String>) -> Self {
        Self {
            operations: vec![Operation::new(command)],
            ..Default::default()
        }
    }

    /// Whether this request asks for validation only (§69.3, §75).
    pub fn is_dry_run(&self) -> bool {
        self.options
            .as_ref()
            .and_then(|o| o.dry_run)
            .unwrap_or(false)
    }

    /// The declared execution mode; a lone operation needs none.
    pub fn execution_mode(&self) -> ExecutionMode {
        self.execution
            .as_ref()
            .map(|e| e.mode)
            .unwrap_or(ExecutionMode::Independent)
    }

    /// Checks every envelope invariant that does not need an engine.
    ///
    /// This is the structural gate: Governance, Schema resolution, snapshot
    /// alignment and commit-time revalidation all remain runtime invariants.
    pub fn validate(&self) -> Result<(), KipError> {
        if self.kip != KIP_VERSION {
            return Err(KipError::unsupported_protocol_version(format!(
                "this runtime speaks KIP {KIP_VERSION}, the request declares {:?}",
                self.kip
            )));
        }
        if self.operations.is_empty() {
            return Err(KipError::invalid_request_envelope(
                "a request must carry at least one operation",
            ));
        }
        if self.operations.len() > MAX_KIP_BATCH_COMMANDS {
            return Err(KipError::resource_exhausted(format!(
                "batch of {} operations exceeds maximum {MAX_KIP_BATCH_COMMANDS}",
                self.operations.len()
            )));
        }

        validate_optional_non_empty(&self.request_id, "request_id")?;
        validate_optional_non_empty(&self.compatibility_profile, "compatibility_profile")?;

        if let Some(space) = &self.space {
            if space.id.is_none() && space.uri.is_none() {
                return Err(KipError::invalid_request_envelope(
                    "space must identify a MemorySpace by `id`, `uri`, or both",
                ));
            }
            validate_optional_non_empty(&space.id, "space.id")?;
            validate_optional_non_empty(&space.uri, "space.uri")?;
        }

        if let Some(execution) = &self.execution {
            validate_optional_non_empty(&execution.isolation, "execution.isolation")?;
            validate_optional_non_empty(&execution.idempotency_key, "execution.idempotency_key")?;
        }
        if let Some(read) = &self.read {
            validate_optional_non_empty(&read.snapshot_token, "read.snapshot_token")?;
        }
        if let Some(context) = &self.context {
            validate_optional_non_empty(&context.purpose, "context.purpose")?;
            validate_optional_non_empty(&context.risk, "context.risk")?;
            validate_optional_non_empty(&context.locale, "context.locale")?;
            validate_optional_non_empty(&context.client, "context.client")?;
        }
        if self
            .options
            .as_ref()
            .and_then(|options| options.deadline_ms)
            == Some(0)
        {
            return Err(KipError::invalid_request_envelope(
                "options.deadline_ms must be greater than zero",
            ));
        }

        // A multi-operation request must say how its operations relate: whether
        // earlier commits survive a later failure is not a detail to leave to
        // an engine default (§75, §75.4).
        if self.operations.len() > 1 && self.execution.is_none() {
            return Err(KipError::invalid_request_envelope(
                "a multi-operation request must declare execution.mode: independent, sequence \
                 or atomic — operations[] is a batch, not a transaction",
            ));
        }

        if let Some(execution) = &self.execution
            && execution.mode == ExecutionMode::Atomic
            && execution.on_error == Some(OnError::Continue)
        {
            return Err(KipError::invalid_request_envelope(
                "an atomic transaction cannot continue past an error: it commits all or none",
            ));
        }

        let mut seen_ops: Vec<&str> = Vec::new();
        for operation in &self.operations {
            operation.validate()?;
            if let Some(op_id) = &operation.op_id {
                if seen_ops.contains(&op_id.as_str()) {
                    return Err(KipError::invalid_request_envelope(format!(
                        "op_id {op_id:?} is used by two operations in one request"
                    )));
                }
                seen_ops.push(op_id);
            }
        }

        if let Some(parameters) = &self.parameters {
            for name in parameters.keys() {
                validate_binding_name(name, "parameter")?;
            }
        }
        if let Some(ingest) = &self.ingest {
            ingest.validate()?;
        }

        Ok(())
    }

    /// Parses and classifies every operation, enforcing the language contract.
    ///
    /// A caller-supplied `language` that disagrees with the parsed command is
    /// rejected rather than trusted: that label is exactly the lever an
    /// injection would pull to get a write past a read-only path (§73.1, §88.3).
    pub fn parse_operations(&self) -> Result<Vec<Command>, KipError> {
        self.validate()?;
        self.operations
            .iter()
            .map(|operation| operation.parse())
            .collect()
    }
}

/// Which MemorySpace a request runs against (Spec §5).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SpaceSelector {
    /// The Space id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// The Space URI. When both are given they must resolve to the same Space.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
}

/// How a request's operations relate to one another (Spec §75).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Execution {
    /// The execution mode.
    pub mode: ExecutionMode,
    /// What to do when an operation fails; meaningful mainly for `sequence`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_error: Option<OnError>,
    /// The requested isolation guarantee. An unsupported stronger guarantee
    /// must fail explicitly rather than silently downgrade.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub isolation: Option<String>,
    /// One logical mutation intent (§34.1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Namespaced extensions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Map<String, Json>>,
}

impl Execution {
    /// An execution block declaring just a mode.
    pub fn new(mode: ExecutionMode) -> Self {
        Self {
            mode,
            on_error: None,
            isolation: None,
            idempotency_key: None,
            extensions: None,
        }
    }
}

/// The three execution modes (Spec §75).
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum ExecutionMode {
    /// Semantically independent; separate snapshots, separate transactions,
    /// failure isolated per operation (§75.1).
    #[default]
    Independent,
    /// Ordered; each state-changing operation commits separately and earlier
    /// commits are **not** rolled back (§75.2).
    Sequence,
    /// One transaction, one snapshot, read-your-writes, all-or-none (§75.3).
    Atomic,
}

impl ExecutionMode {
    /// Whether the whole request commits or aborts as one unit.
    pub fn is_transactional(&self) -> bool {
        matches!(self, ExecutionMode::Atomic)
    }
}

/// What a `sequence` run does after a failure (Spec §75.2).
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum OnError {
    /// Stop at the first failure.
    Stop,
    /// Keep going. Illegal under `atomic`.
    Continue,
}

/// Binds a request to a readable cognitive state coordinate (Spec §78).
///
/// A snapshot token is not an authority token; current Governance always applies.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReadBinding {
    /// An opaque runtime token. Clients must not parse or modify it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_token: Option<String>,
    /// Namespaced extensions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Map<String, Json>>,
}

/// Space and schema preconditions for the whole request (Spec §35.4).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Preconditions {
    /// The Space commit sequence the request expects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub space_seq: Option<u64>,
    /// The exact Schema Environment version the request expects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_environment_version: Option<u64>,
    /// Namespaced extensions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Map<String, Json>>,
}

/// One operation in a request (Spec §73).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Operation {
    /// A request-local correlation id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op_id: Option<String>,
    /// The declared language. Advisory only: the parsed command is
    /// authoritative for security classification (§73.1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<CommandType>,
    /// The KIP command text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// A pre-parsed command, where the runtime advertises the capability.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ast: Option<Command>,
    /// Operation-local parameter bindings.
    ///
    /// Parameters are structurally bound to complete value positions, never
    /// string-interpolated: they are data, not code (§74, §88.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Map<String, Json>>,
    /// One logical mutation intent, scoped to this operation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Reserved operation-local options.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<OperationOptions>,
    /// Namespaced extensions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Map<String, Json>>,
}

/// Reserved operation-local options container.
///
/// KIP 2.0 defines no standard members here yet. Keeping the container typed
/// prevents a misspelled future option from being silently accepted.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OperationOptions {
    /// Namespaced extensions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Map<String, Json>>,
}

impl Operation {
    /// Builds an operation from a command string.
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            command: Some(command.into()),
            ..Default::default()
        }
    }

    /// Names this operation for correlation in the response.
    pub fn with_op_id(mut self, op_id: impl Into<String>) -> Self {
        self.op_id = Some(op_id.into());
        self
    }

    /// Binds parameters for this operation.
    pub fn with_parameters(mut self, parameters: Map<String, Json>) -> Self {
        self.parameters = Some(parameters);
        self
    }

    /// Checks the operation's own shape.
    pub fn validate(&self) -> Result<(), KipError> {
        match (&self.command, &self.ast) {
            (Some(command), None) if !command.trim().is_empty() => {}
            (Some(_), None) => {
                return Err(KipError::invalid_request_envelope(
                    "an operation's command must not be empty",
                ));
            }
            (None, Some(_)) => {}
            (Some(_), Some(_)) => {
                return Err(KipError::invalid_request_envelope(
                    "an operation carries either `command` text or a pre-parsed `ast`, never both",
                ));
            }
            (None, None) => {
                return Err(KipError::invalid_request_envelope(
                    "an operation must carry either `command` text or a pre-parsed `ast`",
                ));
            }
        }

        validate_optional_non_empty(&self.op_id, "op_id")?;
        validate_optional_non_empty(&self.idempotency_key, "operation.idempotency_key")?;

        if let Some(parameters) = &self.parameters {
            for name in parameters.keys() {
                validate_binding_name(name, "parameter")?;
            }
        }
        Ok(())
    }

    /// Parses this operation into a command, enforcing the language contract.
    pub fn parse(&self) -> Result<Command, KipError> {
        self.validate()?;

        let command = match (&self.command, &self.ast) {
            (Some(text), _) => parse_kip(text)?,
            (None, Some(ast)) => {
                // A supplied AST skipped the parser, and with it every rule the
                // grammar enforces while reading. Re-check them here, or the
                // `ast` form becomes a way to hand an engine exactly the
                // commands the text form exists to reject (§73).
                validate_command(ast)?;
                ast.clone()
            }
            (None, None) => unreachable!("validate rejected the empty operation"),
        };

        if let Some(declared) = self.language {
            let actual = CommandType::from(&command);
            if declared != actual {
                return Err(KipError::language_mismatch(format!(
                    "the operation declares {declared} but the command is {actual}"
                )));
            }
        }
        Ok(command)
    }
}

/// Non-authoritative request context (Spec §71).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RequestContext {
    /// Why the caller is asking, e.g. `answer_user`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub purpose: Option<String>,
    /// The caller's own risk assessment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub risk: Option<String>,
    /// The caller's locale.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locale: Option<String>,
    /// The calling client.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client: Option<String>,
    /// Namespaced extensions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Map<String, Json>>,
}

/// Request-level options (Spec §80.1).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RequestOptions {
    /// Validation/preview mode. A dry run must not establish a durable commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dry_run: Option<bool>,
    /// The client's execution window.
    ///
    /// Expiry is not proof that a transaction aborted (§80.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u64>,
    /// Namespaced extensions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Map<String, Json>>,
}

/// Source material the runtime mints into Evidence (Spec §71.1).
///
/// The point is Evidence fidelity: observed payloads should reach Evidence from
/// the transport envelope, not by an Agent re-typing them inside KML text
/// (§88.12).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct IngestContext {
    /// The Evidence entries to mint, each bound as a request parameter.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence: Vec<IngestEvidence>,
    /// Namespaced extensions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Map<String, Json>>,
}

impl IngestContext {
    /// Checks the ingest entries' shape.
    pub fn validate(&self) -> Result<(), KipError> {
        if self.evidence.is_empty() {
            return Err(KipError::invalid_request_envelope(
                "an ingest context must carry at least one Evidence entry",
            ));
        }
        let mut seen: Vec<&str> = Vec::new();
        for entry in &self.evidence {
            entry.validate()?;
            if seen.contains(&entry.key.as_str()) {
                return Err(KipError::invalid_request_envelope(format!(
                    "ingest key {:?} is claimed by two Evidence entries",
                    entry.key
                )));
            }
            seen.push(&entry.key);
        }
        Ok(())
    }
}

/// One Evidence entry to mint inside the request's transaction scope.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct IngestEvidence {
    /// The request-local binding name; commands cite it as `:key`.
    pub key: String,
    /// What kind of observation this is (§15.2).
    pub evidence_class: String,
    /// The inline payload, preserved without model rewriting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Json>,
    /// A runtime artifact handle carrying the payload bytes instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_artifact: Option<String>,
    /// The payload's media type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    /// When the observation happened.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<String>,
    /// The semantic source actor — recorded as Evidence source, never as
    /// Principal identity (§88.1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_actor: Option<String>,
    /// A retry-safe logical identity for the minted Evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_key: Option<String>,
    /// Namespaced extensions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Map<String, Json>>,
}

impl IngestEvidence {
    /// Checks that the entry names a binding and carries exactly one payload.
    pub fn validate(&self) -> Result<(), KipError> {
        validate_binding_name(&self.key, "ingest key")?;
        if self.evidence_class.trim().is_empty() {
            return Err(KipError::invalid_request_envelope(
                "an ingest Evidence entry must declare an evidence_class",
            ));
        }
        validate_optional_non_empty(&self.payload_artifact, "ingest payload_artifact")?;
        validate_optional_non_empty(&self.media_type, "ingest media_type")?;
        validate_optional_non_empty(&self.observed_at, "ingest observed_at")?;
        validate_optional_non_empty(&self.source_actor, "ingest source_actor")?;
        validate_optional_non_empty(&self.client_key, "ingest client_key")?;
        match (&self.payload, &self.payload_artifact) {
            (Some(_), None) | (None, Some(_)) => Ok(()),
            _ => Err(KipError::invalid_request_envelope(format!(
                "ingest entry {:?} must declare exactly one of payload / payload_artifact",
                self.key
            ))),
        }
    }
}

fn validate_optional_non_empty(value: &Option<String>, what: &str) -> Result<(), KipError> {
    if value.as_ref().is_some_and(|value| value.trim().is_empty()) {
        Err(KipError::invalid_request_envelope(format!(
            "{what} must not be empty"
        )))
    } else {
        Ok(())
    }
}

/// Parameter and ingest binding names share the identifier shape the grammar
/// uses, so `:name` always resolves to something spellable in a command.
fn validate_binding_name(name: &str, what: &str) -> Result<(), KipError> {
    let mut chars = name.chars();
    let valid = match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {
            chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(KipError::invalid_identifier(format!(
            "{what} {name:?} must match [A-Za-z_][A-Za-z0-9_]*"
        )))
    }
}

// ---------------------------------------------------------------------------
// Response
// ---------------------------------------------------------------------------

/// The KIP 2.0 response envelope (Spec §81).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Response {
    /// The protocol profile that produced this response.
    pub kip: String,
    /// Echoes the request's `request_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// The request-level outcome.
    pub status: TopLevelStatus,
    /// The execution mode that was actually used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<ResponseExecution>,
    /// One entry per operation.
    pub results: Vec<OperationResult>,
    /// Request-level context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<ResponseContext>,
    /// The snapshot coordinate the reads ran at.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<SnapshotContext>,
    /// The commit receipt, for a state-changing request (§33.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt: Option<Receipt>,
    /// Non-fatal caveats. A required failure is an Error, never a Warning.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<Warning>,
    /// A request-level pagination cursor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// A request-level error, when the request failed before its operations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorObject>,
    /// Namespaced extensions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Map<String, Json>>,
}

impl Default for Response {
    fn default() -> Self {
        Self {
            kip: KIP_VERSION.to_string(),
            request_id: None,
            status: TopLevelStatus::Succeeded,
            execution: None,
            results: Vec::new(),
            context: None,
            snapshot: None,
            receipt: None,
            warnings: Vec::new(),
            next_cursor: None,
            error: None,
            extensions: None,
        }
    }
}

impl Response {
    /// A successful single-operation response carrying one result value.
    pub fn ok(result: Json) -> Self {
        Self {
            results: vec![OperationResult::ok(result)],
            ..Default::default()
        }
    }

    /// A failed single-operation response.
    pub fn failed(error: impl Into<ErrorObject>) -> Self {
        let error = error.into();
        Self {
            status: TopLevelStatus::Failed,
            results: vec![OperationResult::failed(error.clone())],
            error: Some(error),
            ..Default::default()
        }
    }

    /// Builds a response from per-operation results, deriving the top-level
    /// status from them.
    ///
    /// `partial` is a real outcome, not a rounding of `failed`: under
    /// `sequence`, earlier commits are durable even when a later operation
    /// failed (§75.2), and a caller that treats the whole request as failed
    /// will re-issue writes that already landed.
    pub fn from_results(results: Vec<OperationResult>) -> Self {
        let status = TopLevelStatus::derive(&results);
        Self {
            status,
            results,
            ..Default::default()
        }
    }

    /// Marks the outcome as unknown, the state §80.3 requires when a write may
    /// have committed but the response path cannot establish whether it did.
    pub fn outcome_unknown(error: impl Into<ErrorObject>) -> Self {
        Self {
            status: TopLevelStatus::OutcomeUnknown,
            error: Some(error.into()),
            ..Default::default()
        }
    }

    /// Correlates this response with its request.
    pub fn with_request_id(mut self, request_id: Option<String>) -> Self {
        self.request_id = request_id;
        self
    }

    /// The first result's value, for the single-operation case.
    pub fn first_result(&self) -> Option<&Json> {
        self.results.first().and_then(|r| r.result.as_ref())
    }
}

impl From<KipError> for Response {
    fn from(err: KipError) -> Self {
        Response::failed(err)
    }
}

/// The request-level outcome (Spec §82).
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TopLevelStatus {
    /// Every operation succeeded.
    #[default]
    Succeeded,
    /// Nothing succeeded.
    Failed,
    /// Some operations succeeded and some did not.
    Partial,
    /// A write may or may not have committed (§80.3).
    OutcomeUnknown,
}

impl TopLevelStatus {
    /// Derives the request-level status from its operation results.
    pub fn derive(results: &[OperationResult]) -> Self {
        if results.is_empty() {
            return TopLevelStatus::Succeeded;
        }
        let succeeded = results
            .iter()
            .filter(|r| {
                matches!(
                    r.status,
                    OperationStatus::Succeeded | OperationStatus::NoEffect
                )
            })
            .count();
        if succeeded == results.len() {
            TopLevelStatus::Succeeded
        } else if succeeded == 0 {
            TopLevelStatus::Failed
        } else {
            TopLevelStatus::Partial
        }
    }
}

/// The per-operation outcome (Spec §83).
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    /// The operation ran and produced its effect.
    #[default]
    Succeeded,
    /// The operation ran and failed.
    Failed,
    /// The operation never ran.
    Skipped,
    /// The operation executed tentatively inside a transaction that then
    /// aborted; no durable state resulted (§83.1).
    RolledBack,
    /// The operation ran and changed nothing (§32.8).
    NoEffect,
}

/// The execution block echoed back on the response.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ResponseExecution {
    /// The mode that was used.
    pub mode: ExecutionMode,
    /// The error policy that was used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_error: Option<OnError>,
    /// The isolation that was actually provided.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub isolation: Option<String>,
    /// Namespaced extensions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Map<String, Json>>,
}

/// One operation's outcome (Spec §81).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct OperationResult {
    /// Echoes the operation's `op_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op_id: Option<String>,
    /// What happened.
    pub status: OperationStatus,
    /// The result value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Json>,
    /// The coordinates and policies this result was produced under.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<ResultContext>,
    /// Non-fatal caveats about this result.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<Warning>,
    /// Why it failed. Required when `status` is `failed`; absent on success.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorObject>,
    /// The cursor to continue from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// Namespaced extensions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Map<String, Json>>,
}

impl OperationResult {
    /// A successful result.
    pub fn ok(result: Json) -> Self {
        Self {
            status: OperationStatus::Succeeded,
            result: Some(result),
            ..Default::default()
        }
    }

    /// A failed result.
    pub fn failed(error: impl Into<ErrorObject>) -> Self {
        Self {
            status: OperationStatus::Failed,
            error: Some(error.into()),
            ..Default::default()
        }
    }

    /// A result that ran but changed nothing.
    pub fn no_effect() -> Self {
        Self {
            status: OperationStatus::NoEffect,
            ..Default::default()
        }
    }

    /// A result that was tentatively executed and then rolled back.
    pub fn rolled_back() -> Self {
        Self {
            status: OperationStatus::RolledBack,
            ..Default::default()
        }
    }

    /// Correlates this result with its operation.
    pub fn with_op_id(mut self, op_id: Option<String>) -> Self {
        self.op_id = op_id;
        self
    }
}

/// Request-level response context.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ResponseContext {
    /// The Space the request ran against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub space_id: Option<String>,
    /// The Schema Environment version in force.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_environment_version: Option<u64>,
    /// The compatibility profile that was applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compatibility_profile_used: Option<String>,
    /// Namespaced extensions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Map<String, Json>>,
}

/// The coordinates and policies one result was produced under (Spec §50).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ResultContext {
    /// The Space this result came from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub space_id: Option<String>,
    /// The snapshot coordinate the read ran at.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_seq: Option<u64>,
    /// The Schema Environment version used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_environment_version: Option<u64>,
    /// Which Projection Policy produced any belief in this result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epistemic_policy: Option<PolicyIdentity>,
    /// How a SEARCH result was produced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<SearchContext>,
    /// The cursor this page was produced from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Namespaced extensions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Map<String, Json>>,
}

/// The string-or-integer wire form of a policy version.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(untagged)]
pub enum PolicyVersion {
    /// A semantic or otherwise textual policy version.
    Text(String),
    /// A monotonically increasing numeric policy version.
    Integer(u64),
}

/// Identifies the policy a projection ran under.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct PolicyIdentity {
    /// The policy id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// The policy version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<PolicyVersion>,
}

/// How a SEARCH result was produced (Spec §66, §79).
///
/// A lagging index must not be presented as transaction-snapshot-consistent
/// when it is not, which is what `index_seq` versus `current_space_seq` makes
/// visible.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct SearchContext {
    /// The sequence the index reflects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_seq: Option<u64>,
    /// The Space's current sequence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_space_seq: Option<u64>,
    /// The consistency actually provided.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consistency: Option<String>,
    /// The search mode used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<SearchMode>,
    /// What the score means. A relevance score is not confidence (§2.10).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score_semantics: Option<String>,
    /// Namespaced extensions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Map<String, Json>>,
}

/// The baseline SEARCH modes (Spec §66.3).
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum SearchMode {
    /// Lexical matching.
    Keyword,
    /// Embedding similarity.
    Semantic,
    /// Both.
    Hybrid,
}

/// The snapshot coordinate a response was produced at (Spec §78).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct SnapshotContext {
    /// The Space the snapshot belongs to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub space_id: Option<String>,
    /// The snapshot sequence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_seq: Option<u64>,
    /// The Schema Environment version at that coordinate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_environment_version: Option<u64>,
    /// An opaque token to bind later reads to the same coordinate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_token: Option<String>,
    /// Namespaced extensions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Map<String, Json>>,
}

/// The receipt for a state-changing request (Spec §33).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Receipt {
    /// What the transaction did.
    pub status: ReceiptStatus,
    /// The engine-assigned transaction id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tx_id: Option<String>,
    /// The Space that committed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub space_id: Option<String>,
    /// The snapshot the transaction started from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_seq: Option<u64>,
    /// The Space sequence the commit produced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub space_seq: Option<u64>,
    /// When it committed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_at: Option<String>,
    /// The transaction class, e.g. `cognitive`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction_class: Option<String>,
    /// A digest of the request that produced it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_digest: Option<String>,
    /// A digest of the semantic plan that was executed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub semantic_plan_digest: Option<String>,
    /// A digest of the result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_digest: Option<String>,
    /// The Schema Environment version the commit ran under.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_environment_version: Option<u64>,
    /// A summary of what changed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_summary: Option<Json>,
    /// Signatures over the receipt (§33.3).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub proofs: Vec<Json>,
    /// Namespaced extensions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Map<String, Json>>,
}

/// What a transaction did (Spec §33.2).
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptStatus {
    /// The transaction committed durably.
    Committed,
    /// The transaction aborted; no durable state resulted.
    Aborted,
    /// The transaction ran and changed nothing.
    NoEffect,
    /// The transaction is still in flight.
    Pending,
    /// The outcome could not be established (§80.3).
    Unknown,
}

/// A non-fatal caveat (Spec §81).
///
/// A required failure must be an Error, not a Warning.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(untagged)]
pub enum Warning {
    /// A bare message.
    Message(String),
    /// A coded warning.
    Coded {
        /// A stable warning code.
        code: String,
        /// The human-readable message.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message: Option<String>,
        /// Structured detail.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        details: Option<Json>,
        /// Namespaced extensions.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        extensions: Option<Map<String, Json>>,
    },
}

impl From<&str> for Warning {
    fn from(message: &str) -> Self {
        Warning::Message(message.to_string())
    }
}

impl From<String> for Warning {
    fn from(message: String) -> Self {
        Warning::Message(message)
    }
}

impl From<KipErrorCode> for Warning {
    fn from(code: KipErrorCode) -> Self {
        Warning::Coded {
            code: code.name().to_string(),
            message: None,
            details: None,
            extensions: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::KipErrorCode;

    #[test]
    fn a_lone_operation_needs_no_execution_mode() {
        let request = Request::single(r#"FIND(?x) WHERE { ?x {type: "T"} }"#);
        assert!(request.validate().is_ok());
    }

    #[test]
    fn unknown_wire_fields_are_rejected_instead_of_silently_ignored() {
        // `option` is a dangerous typo here: silently dropping it would turn a
        // requested dry run into a durable write.
        let bad_request = serde_json::json!({
            "kip": "2.0",
            "operations": [{"command": "ARCHIVE :x"}],
            "option": {"dry_run": true}
        });
        assert!(serde_json::from_value::<Request>(bad_request).is_err());

        let bad_operation = serde_json::json!({
            "kip": "2.0",
            "operations": [{
                "command": "DESCRIBE PROTOCOL",
                "paramters": {"x": 1}
            }]
        });
        assert!(serde_json::from_value::<Request>(bad_operation).is_err());

        let bad_operation_option = serde_json::json!({
            "kip": "2.0",
            "operations": [{
                "command": "ARCHIVE :x",
                "options": {"dry_run": true}
            }]
        });
        assert!(serde_json::from_value::<Request>(bad_operation_option).is_err());
    }

    #[test]
    fn optional_envelope_fields_must_not_be_empty_when_present() {
        let mut request = Request {
            space: Some(SpaceSelector::default()),
            ..Request::single("DESCRIBE PROTOCOL")
        };
        assert!(request.validate().is_err());

        request.space = Some(SpaceSelector {
            id: Some(" ".into()),
            uri: None,
        });
        assert!(request.validate().is_err());

        request.space = Some(SpaceSelector {
            id: Some("space-1".into()),
            uri: None,
        });
        request.options = Some(RequestOptions {
            deadline_ms: Some(0),
            ..Default::default()
        });
        assert!(request.validate().is_err());

        request.options = None;
        request.context = Some(RequestContext {
            purpose: Some(" ".into()),
            ..Default::default()
        });
        assert!(request.validate().is_err());
    }

    #[test]
    fn policy_versions_accept_the_wire_schemas_text_and_integer_forms() {
        let numeric: PolicyIdentity = serde_json::from_value(serde_json::json!({
            "id": "projection-policy",
            "version": 7
        }))
        .unwrap();
        assert_eq!(numeric.version, Some(PolicyVersion::Integer(7)));

        let textual: PolicyIdentity = serde_json::from_value(serde_json::json!({
            "version": "7.1"
        }))
        .unwrap();
        assert_eq!(textual.version, Some(PolicyVersion::Text("7.1".into())));
    }

    #[test]
    fn a_batch_must_say_how_its_operations_relate() {
        // Spec §75.4: operations[] is not a transaction unless it says so.
        let mut request = Request::single("DESCRIBE PROTOCOL");
        request.operations.push(Operation::new("DESCRIBE PRIMER"));
        let err = request.validate().expect_err("no execution mode");
        assert_eq!(err.code, KipErrorCode::InvalidRequestEnvelope);

        request.execution = Some(Execution::new(ExecutionMode::Independent));
        assert!(request.validate().is_ok());
    }

    #[test]
    fn an_atomic_transaction_cannot_continue_past_an_error() {
        let mut request = Request::single("ARCHIVE :a");
        request.operations.push(Operation::new("ARCHIVE :b"));
        request.execution = Some(Execution {
            on_error: Some(OnError::Continue),
            ..Execution::new(ExecutionMode::Atomic)
        });
        assert!(request.validate().is_err());

        request.execution = Some(Execution {
            on_error: Some(OnError::Continue),
            ..Execution::new(ExecutionMode::Sequence)
        });
        assert!(request.validate().is_ok());
    }

    #[test]
    fn a_declared_language_cannot_relabel_a_write_as_a_read() {
        // Spec §73.1 / §88.3: the parsed command is authoritative.
        let operation = Operation {
            language: Some(CommandType::Kql),
            ..Operation::new(r#"TOMBSTONE :x"#)
        };
        let err = operation.parse().expect_err("mislabelled write");
        assert_eq!(err.code, KipErrorCode::LanguageMismatch);

        let honest = Operation {
            language: Some(CommandType::Kml),
            ..Operation::new(r#"TOMBSTONE :x"#)
        };
        assert!(honest.parse().unwrap().is_mutation());
    }

    #[test]
    fn an_operation_carries_text_or_an_ast_but_never_both() {
        let ast = parse_kip("DESCRIBE PROTOCOL").unwrap();
        let both = Operation {
            ast: Some(ast.clone()),
            ..Operation::new("DESCRIBE PROTOCOL")
        };
        assert!(both.validate().is_err());

        let neither = Operation::default();
        assert!(neither.validate().is_err());

        let ast_only = Operation {
            ast: Some(ast),
            ..Default::default()
        };
        assert_eq!(
            ast_only.parse().unwrap(),
            parse_kip("DESCRIBE PROTOCOL").unwrap()
        );
    }

    #[test]
    fn a_pre_parsed_ast_gets_the_same_guards_as_command_text() {
        // Spec §73: `ast` is an alternative encoding of the same operation, not
        // a way around the rules the text form is rejected by.
        let text = r#"UPDATE ?a SET FIELDS { confidence: 0.1 } WHERE { ?a ASSERTION {id: "A-1"} }"#;
        assert!(parse_kip(text).is_err(), "the text form must be rejected");

        let rewrite_immutable_payload = serde_json::json!({"Kml": {
            "explicit_transaction": false,
            "clauses": [{"Update": {
                "target": {"Handle": "a"},
                "expect_version": Json::Null,
                "actions": [{"SetFields": [["confidence", {"Value": {"Number": 0.1}}]]}],
                "where_clauses": [{"Assertion": {
                    "variable": "a",
                    "matcher": {"id": {"Literal": {"String": "A-1"}}}
                }}],
                "limit": Json::Null
            }}]
        }});
        let write_engine_truth = serde_json::json!({"Kml": {
            "explicit_transaction": false,
            "clauses": [{"CreateConcept": {
                "handle": "c", "type": Json::Null, "client_key": Json::Null, "name": Json::Null,
                "set_fields": [["_system", {"Value": {"Number": 1}}]],
                "set_attributes": Json::Null, "set_facets": [], "set_structural": Json::Null
            }}]
        }});
        let unconfirmed_purge = serde_json::json!({"Kml": {
            "explicit_transaction": false,
            "clauses": [{"Purge": {
                "target": {"Param": "x"}, "where_clauses": Json::Null, "limit": Json::Null,
                "reference_policy": Json::Null, "confirm": ""
            }}]
        }});
        let belief_as_an_export_selector = serde_json::json!({"Meta": {"ExportCapsule": {
            "target": {"Param": "out"},
            "where_clauses": [{"Belief": {"variable": "b", "target": {"Proposition": "p"}}}],
            "options": Json::Null, "as_of": Json::Null
        }}});

        for ast in [
            rewrite_immutable_payload,
            write_engine_truth,
            unconfirmed_purge,
            belief_as_an_export_selector,
        ] {
            let operation = Operation {
                ast: Some(serde_json::from_value(ast.clone()).expect("decodes")),
                ..Default::default()
            };
            let err = operation
                .parse()
                .expect_err(&format!("must be rejected: {ast}"));
            assert_eq!(err.code, KipErrorCode::InvalidSyntax);
        }

        // A tree that the parser would have produced still round-trips.
        let honest = Operation {
            ast: Some(parse_kip(r#"ARCHIVE :old"#).unwrap()),
            ..Default::default()
        };
        assert!(honest.parse().unwrap().is_mutation());
    }

    #[test]
    fn op_ids_must_be_unique_within_a_request() {
        let mut request = Request::single("DESCRIBE PROTOCOL");
        request.operations[0].op_id = Some("op-1".into());
        request
            .operations
            .push(Operation::new("DESCRIBE PRIMER").with_op_id("op-1"));
        request.execution = Some(Execution::new(ExecutionMode::Independent));
        assert!(request.validate().is_err());
    }

    #[test]
    fn parameter_names_must_be_spellable_in_a_command() {
        let mut request = Request::single("DESCRIBE PROTOCOL");
        let mut parameters = Map::new();
        parameters.insert("2bad".into(), Json::from(1));
        request.parameters = Some(parameters);
        let err = request.validate().expect_err("bad parameter name");
        assert_eq!(err.code, KipErrorCode::InvalidIdentifier);
    }

    #[test]
    fn ingest_entries_carry_exactly_one_payload() {
        let base = IngestEvidence {
            key: "msg".into(),
            evidence_class: "user_statement".into(),
            ..Default::default()
        };
        assert!(base.validate().is_err());

        let inline = IngestEvidence {
            payload: Some(Json::from("I prefer dark mode.")),
            ..base.clone()
        };
        assert!(inline.validate().is_ok());

        let both = IngestEvidence {
            payload: Some(Json::from("x")),
            payload_artifact: Some("artifact-1".into()),
            ..base.clone()
        };
        assert!(both.validate().is_err());

        let duplicate = IngestContext {
            evidence: vec![inline.clone(), inline],
            extensions: None,
        };
        assert!(duplicate.validate().is_err());
    }

    #[test]
    fn a_partial_batch_is_not_a_failed_batch() {
        // Spec §75.2: under `sequence`, earlier commits are durable. Reporting
        // the whole request as failed invites a caller to re-issue them.
        let results = vec![
            OperationResult::ok(Json::from(1)),
            OperationResult::failed(KipError::not_found_or_not_visible("gone")),
        ];
        assert_eq!(TopLevelStatus::derive(&results), TopLevelStatus::Partial);

        let all_ok = vec![
            OperationResult::ok(Json::Null),
            OperationResult::no_effect(),
        ];
        assert_eq!(TopLevelStatus::derive(&all_ok), TopLevelStatus::Succeeded);

        let all_bad = vec![OperationResult::failed(KipError::internal_error("boom"))];
        assert_eq!(TopLevelStatus::derive(&all_bad), TopLevelStatus::Failed);
    }

    #[test]
    fn rolled_back_is_not_success() {
        // Spec §83.1: it executed tentatively, but nothing durable resulted.
        let results = vec![OperationResult::rolled_back()];
        assert_eq!(TopLevelStatus::derive(&results), TopLevelStatus::Failed);
    }

    #[test]
    fn the_envelope_round_trips_through_its_wire_shape() {
        let request = Request {
            request_id: Some("req-1".into()),
            space: Some(SpaceSelector {
                id: Some("space-1".into()),
                uri: None,
            }),
            execution: Some(Execution {
                idempotency_key: Some("logical-write-key".into()),
                isolation: Some("serializable".into()),
                ..Execution::new(ExecutionMode::Atomic)
            }),
            operations: vec![Operation::new(r#"ARCHIVE :x"#).with_op_id("op-1")],
            options: Some(RequestOptions {
                deadline_ms: Some(10_000),
                ..Default::default()
            }),
            ..Default::default()
        };
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["kip"], "2.0");
        assert_eq!(json["execution"]["mode"], "atomic");
        assert_eq!(json["operations"][0]["op_id"], "op-1");
        let decoded: Request = serde_json::from_value(json).unwrap();
        assert_eq!(decoded, request);

        let response = Response {
            receipt: Some(Receipt {
                status: ReceiptStatus::Committed,
                tx_id: Some("tx-9".into()),
                space_seq: Some(4201),
                snapshot_seq: Some(4200),
                space_id: Some("space-1".into()),
                committed_at: Some("2026-08-16T00:00:00Z".into()),
                transaction_class: None,
                request_digest: None,
                semantic_plan_digest: None,
                result_digest: None,
                schema_environment_version: None,
                change_summary: None,
                proofs: vec![],
                extensions: None,
            }),
            warnings: vec![Warning::Message("search index lagged".into())],
            ..Response::ok(Json::from(true))
        };
        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["status"], "succeeded");
        assert_eq!(json["receipt"]["status"], "committed");
        assert_eq!(json["warnings"][0], "search index lagged");
        let decoded: Response = serde_json::from_value(json).unwrap();
        assert_eq!(decoded, response);
    }

    #[test]
    fn an_error_response_carries_the_registry_shape() {
        let response = Response::from(KipError::version_conflict("element changed"));
        assert_eq!(response.status, TopLevelStatus::Failed);
        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["error"]["code"], "VersionConflict");
        assert_eq!(json["error"]["retry"]["class"], "requires_refresh");
        assert_eq!(json["results"][0]["status"], "failed");
    }

    #[test]
    fn a_lost_response_is_not_a_failed_write() {
        // Spec §80.3: the client must look the transaction up, not re-mutate.
        let response = Response::outcome_unknown(KipError::outcome_unknown("connection dropped"));
        assert_eq!(response.status, TopLevelStatus::OutcomeUnknown);
        assert_eq!(
            response.error.unwrap().retry.unwrap().class,
            crate::error::RetryClass::OutcomeLookupRequired
        );
    }

    #[test]
    fn an_over_sized_batch_is_rejected_before_execution() {
        let mut request = Request::single("DESCRIBE PROTOCOL");
        request.execution = Some(Execution::new(ExecutionMode::Independent));
        for _ in 0..MAX_KIP_BATCH_COMMANDS {
            request.operations.push(Operation::new("DESCRIBE PROTOCOL"));
        }
        let err = request.validate().expect_err("too many operations");
        assert_eq!(err.code, KipErrorCode::ResourceExhausted);
    }

    #[test]
    fn an_unknown_protocol_version_fails_fast() {
        let request = Request {
            kip: "1.0".into(),
            ..Request::single("DESCRIBE PROTOCOL")
        };
        let err = request.validate().expect_err("wrong version");
        assert_eq!(err.code, KipErrorCode::UnsupportedProtocolVersion);
    }
}
