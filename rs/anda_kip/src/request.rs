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
use std::collections::BTreeMap;

use crate::ast::{Command, CommandType, Json, Map};
use crate::error::{ErrorObject, KipError, KipErrorCode};
use crate::parser::{MAX_KIP_BATCH_COMMANDS, parse_kip, validate_command};

/// The protocol profile this crate speaks.
pub const KIP_VERSION: &str = "2.0";

/// The length ceilings `kip-request.schema.json` puts on envelope strings.
///
/// These are the wire contract, not defensive guesses: a runtime that accepts
/// a longer value accepts something a conforming peer is entitled to reject,
/// and the disagreement surfaces as a mysterious rejection downstream rather
/// than as a clear one here. Lengths are counted in characters, as the schema
/// counts them.
pub mod limits {
    /// `RequestId` and `Operation.op_id`.
    pub const REQUEST_ID: usize = 256;
    /// `IdempotencyKey`, at the request and the operation level.
    pub const IDEMPOTENCY_KEY: usize = 1024;
    /// `SpaceSelector.id`.
    pub const SPACE_ID: usize = 512;
    /// `SpaceSelector.uri`.
    pub const SPACE_URI: usize = 2048;
    /// `compatibility_profile`.
    pub const COMPATIBILITY_PROFILE: usize = 128;
    /// `OpaqueToken` — snapshot tokens and artifact handles.
    pub const OPAQUE_TOKEN: usize = 8192;
    /// `execution.isolation`.
    pub const ISOLATION: usize = 64;
    /// `context.purpose`, `context.client`, `evidence_class`, `media_type`.
    pub const SHORT_LABEL: usize = 256;
    /// `context.risk`.
    pub const RISK: usize = 128;
    /// `context.locale`.
    pub const LOCALE: usize = 64;
    /// `ElementReference.id` — `ingest.evidence[].source_actor` by id.
    pub const SOURCE_ACTOR: usize = 512;
    /// `ElementReference.type` and `.key`.
    pub const ELEMENT_REFERENCE_KEY: usize = 1024;
    /// A Facet name in `ingest.evidence[].facets`.
    pub const FACET_NAME: usize = 512;
    /// `ingest.evidence[].client_key`.
    pub const CLIENT_KEY: usize = 1024;
}

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

    /// Decode a wire request without losing numeric source digits or duplicate keys.
    pub fn from_json(source: &str) -> Result<Self, KipError> {
        Self::from_value(crate::parse_canonical_json(source)?)
    }

    /// Decode an envelope while preserving protocol timestamp error classes.
    pub fn from_value(value: Json) -> Result<Self, KipError> {
        if let Some(entries) = value.pointer("/ingest/evidence").and_then(Json::as_array) {
            for entry in entries {
                if let Some(at) = entry.get("observed_at") {
                    crate::timestamp::validate_value(at, "ingest.observed_at")?;
                }
            }
        }
        let request: Self = serde_json::from_value(value)
            .map_err(|e| KipError::invalid_request_envelope(e.to_string()))?;
        request.validate()?;
        Ok(request)
    }

    /// Checks every envelope invariant that does not need an engine.
    ///
    /// This is the structural gate: Governance, Schema resolution, snapshot
    /// alignment and commit-time revalidation all remain runtime invariants.
    pub fn validate(&self) -> Result<(), KipError> {
        crate::validate_json(
            &serde_json::to_value(self)
                .map_err(|e| KipError::invalid_request_envelope(e.to_string()))?,
        )?;
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

        validate_optional_non_empty(&self.request_id, "request_id", limits::REQUEST_ID)?;
        validate_optional_non_empty(
            &self.compatibility_profile,
            "compatibility_profile",
            limits::COMPATIBILITY_PROFILE,
        )?;
        validate_extensions(&self.extensions, "extensions")?;

        // Each optional block holds itself to its own rules; what is left here
        // is what only the whole envelope can decide.
        validate_block(self.space.as_ref())?;
        validate_block(self.execution.as_ref())?;
        validate_block(self.read.as_ref())?;
        validate_block(self.preconditions.as_ref())?;
        validate_block(self.context.as_ref())?;
        validate_block(self.options.as_ref())?;

        // A multi-operation request must say how its operations relate: whether
        // earlier commits survive a later failure is not a detail to leave to
        // an engine default (§75, §75.4).
        if self.operations.len() > 1 && self.execution.is_none() {
            return Err(KipError::invalid_request_envelope(
                "a multi-operation request must declare execution.mode: independent, sequence \
                 or atomic — operations[] is a batch, not a transaction",
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
        if let Some(requires) = &self.requires {
            for name in requires.keys() {
                validate_capability_name(name)?;
            }
        }
        if let Some(ingest) = &self.ingest {
            ingest.validate()?;
            // §71.1 mints each entry "inside the request's transaction scope"
            // and makes ingestion transactional. A request whose operations are
            // all reads opens no such scope, so the Evidence would be minted
            // nowhere while the request still answered `succeeded` — and the
            // caller would go on believing the observation was recorded, which
            // is precisely the fidelity failure §88.12 has ingestion exist to
            // prevent.
            //
            // Only refused once every operation parsed. A command that does not
            // parse should still report its own syntax error rather than being
            // recast as an envelope fault.
            //
            // The scan stops at the first mutation and keeps no command: the
            // question is whether *some* operation opens a transaction, and
            // the executor parses them all again anyway.
            let mut every_operation_parsed = true;
            let mut opens_a_transaction = false;
            for operation in &self.operations {
                match operation.parse() {
                    Ok(command) if command.is_mutation() => {
                        opens_a_transaction = true;
                        break;
                    }
                    Ok(_) => {}
                    Err(_) => every_operation_parsed = false,
                }
            }
            if every_operation_parsed && !opens_a_transaction {
                return Err(KipError::invalid_request_envelope(
                    "an `ingest` block mints Evidence inside the request's transaction, so the \
                     request must carry at least one KML operation; a read-only request would \
                     drop the observation while reporting success",
                ));
            }
        }

        Ok(())
    }

    /// The namespaced extensions this request marks `critical`.
    ///
    /// §71 makes a critical extension a precondition rather than a hint: a
    /// runtime that does not implement one MUST fail instead of proceeding
    /// without it, because the caller has said the request means something
    /// different without it. This crate cannot know what a given runtime
    /// supports, so it surfaces the list and leaves the decision where the
    /// knowledge is.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use anda_kip::Request;
    ///
    /// let mut request = Request::single("DESCRIBE PROTOCOL");
    /// request.extensions = serde_json::json!({
    ///     "acme/tracing": { "critical": false },
    ///     "acme/redaction": { "critical": true }
    /// })
    /// .as_object()
    /// .cloned();
    ///
    /// assert_eq!(request.critical_extensions(), vec!["acme/redaction"]);
    /// ```
    pub fn critical_extensions(&self) -> Vec<&str> {
        let blocks = [
            self.extensions.as_ref(),
            self.execution.as_ref().and_then(|e| e.extensions.as_ref()),
            self.read.as_ref().and_then(|r| r.extensions.as_ref()),
            self.preconditions
                .as_ref()
                .and_then(|p| p.extensions.as_ref()),
            self.context.as_ref().and_then(|c| c.extensions.as_ref()),
            self.options.as_ref().and_then(|o| o.extensions.as_ref()),
            self.ingest.as_ref().and_then(|i| i.extensions.as_ref()),
        ];
        let operation_blocks = self.operations.iter().flat_map(|operation| {
            [
                operation.extensions.as_ref(),
                operation
                    .options
                    .as_ref()
                    .and_then(|o| o.extensions.as_ref()),
            ]
        });
        let ingest_blocks = self
            .ingest
            .iter()
            .flat_map(|ingest| ingest.evidence.iter().map(|e| e.extensions.as_ref()));

        let mut critical = Vec::new();
        for block in blocks
            .into_iter()
            .chain(operation_blocks)
            .chain(ingest_blocks)
            .flatten()
        {
            for (name, value) in block {
                if value.get("critical") == Some(&Json::Bool(true)) {
                    critical.push(name.as_str());
                }
            }
        }
        critical.sort_unstable();
        critical.dedup();
        critical
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

impl EnvelopeBlock for SpaceSelector {
    fn validate(&self) -> Result<(), KipError> {
        if self.id.is_none() && self.uri.is_none() {
            return Err(KipError::invalid_request_envelope(
                "space must identify a MemorySpace by `id`, `uri`, or both",
            ));
        }
        validate_optional_non_empty(&self.id, "space.id", limits::SPACE_ID)?;
        validate_optional_non_empty(&self.uri, "space.uri", limits::SPACE_URI)
    }
}

/// How a request's operations relate to one another (Spec §75).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Execution {
    /// The execution mode.
    pub mode: ExecutionMode,
    /// What to do when an operation fails; meaningful for `sequence`, and
    /// `stop` when absent (§75.2). Atomic execution aborts unconditionally.
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

    /// The error policy in force: what was declared, else `stop` (§75.2).
    pub fn effective_on_error(&self) -> OnError {
        self.on_error.unwrap_or_default()
    }
}

impl EnvelopeBlock for Execution {
    fn validate(&self) -> Result<(), KipError> {
        if self.mode == ExecutionMode::Atomic && self.on_error == Some(OnError::Continue) {
            return Err(KipError::invalid_request_envelope(
                "an atomic transaction cannot continue past an error: it commits all or none",
            ));
        }
        validate_optional_non_empty(&self.isolation, "execution.isolation", limits::ISOLATION)?;
        validate_optional_non_empty(
            &self.idempotency_key,
            "execution.idempotency_key",
            limits::IDEMPOTENCY_KEY,
        )?;
        validate_extensions(&self.extensions, "execution.extensions")
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
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum OnError {
    /// Stop at the first failure; the operations not started are reported
    /// `skipped`. The default.
    #[default]
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

impl EnvelopeBlock for ReadBinding {
    fn validate(&self) -> Result<(), KipError> {
        validate_optional_non_empty(
            &self.snapshot_token,
            "read.snapshot_token",
            limits::OPAQUE_TOKEN,
        )?;
        validate_extensions(&self.extensions, "read.extensions")
    }
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

impl EnvelopeBlock for Preconditions {
    fn validate(&self) -> Result<(), KipError> {
        validate_extensions(&self.extensions, "preconditions.extensions")
    }
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

        validate_optional_non_empty(&self.op_id, "op_id", limits::REQUEST_ID)?;
        validate_optional_non_empty(
            &self.idempotency_key,
            "operation.idempotency_key",
            limits::IDEMPOTENCY_KEY,
        )?;
        validate_extensions(&self.extensions, "operation.extensions")?;
        if let Some(options) = &self.options {
            validate_extensions(&options.extensions, "operation.options.extensions")?;
        }

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

impl EnvelopeBlock for RequestContext {
    fn validate(&self) -> Result<(), KipError> {
        validate_optional_non_empty(&self.purpose, "context.purpose", limits::SHORT_LABEL)?;
        validate_optional_non_empty(&self.risk, "context.risk", limits::RISK)?;
        validate_optional_non_empty(&self.locale, "context.locale", limits::LOCALE)?;
        validate_optional_non_empty(&self.client, "context.client", limits::SHORT_LABEL)?;
        validate_extensions(&self.extensions, "context.extensions")
    }
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

impl EnvelopeBlock for RequestOptions {
    fn validate(&self) -> Result<(), KipError> {
        validate_extensions(&self.extensions, "options.extensions")?;
        if self.deadline_ms == Some(0) {
            return Err(KipError::invalid_request_envelope(
                "options.deadline_ms must be greater than zero",
            ));
        }
        Ok(())
    }
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
        validate_extensions(&self.extensions, "ingest.extensions")?;
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

/// A same-Space element reference on the wire: by exact id, or by Concept
/// Type lineage plus key (Spec §7.2, §20.14). Names are never references.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ElementReference {
    /// The exact element id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// The Concept Type — local name or exact reference — resolved through
    /// the symbol lineage.
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    /// The Space-local logical key within that type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

impl ElementReference {
    /// A reference by exact id.
    pub fn by_id(id: impl Into<String>) -> Self {
        Self {
            id: Some(id.into()),
            r#type: None,
            key: None,
        }
    }

    /// A reference by Concept Type lineage plus key.
    pub fn by_key(r#type: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            id: None,
            r#type: Some(r#type.into()),
            key: Some(key.into()),
        }
    }

    /// Checks that the reference takes exactly one of its two shapes.
    pub fn validate(&self, what: &str) -> Result<(), KipError> {
        match (&self.id, &self.r#type, &self.key) {
            (Some(id), None, None) => validate_bounded(id, what, limits::SOURCE_ACTOR),
            (None, Some(r#type), Some(key)) => {
                validate_bounded(r#type, what, limits::ELEMENT_REFERENCE_KEY)?;
                validate_bounded(key, what, limits::ELEMENT_REFERENCE_KEY)
            }
            _ => Err(KipError::invalid_request_envelope(format!(
                "{what} is an element reference: {{id}} or {{type, key}}, never a name"
            ))),
        }
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
    /// The semantic source actor, as an element reference — recorded as the
    /// Evidence's `source`, never as Principal identity and never resolved by
    /// name (§71.1, §88.1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_actor: Option<ElementReference>,
    /// A retry-safe logical identity for the minted Evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_key: Option<String>,
    /// Facet name (local or exact ref) to value object, validated exactly as
    /// `SET FACET` on `CREATE EVIDENCE` would be (§71.1). This is how
    /// instrumentation attaches `OutcomeRecord` to an ingested `outcome`
    /// without re-typing anything.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub facets: BTreeMap<String, Map<String, Json>>,
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
        validate_bounded(
            &self.evidence_class,
            "ingest evidence_class",
            limits::SHORT_LABEL,
        )?;
        validate_optional_non_empty(
            &self.payload_artifact,
            "ingest payload_artifact",
            limits::OPAQUE_TOKEN,
        )?;
        validate_optional_non_empty(&self.media_type, "ingest media_type", limits::SHORT_LABEL)?;
        if let Some(at) = &self.observed_at {
            crate::timestamp::parse(at, "ingest.observed_at")?;
        }
        if let Some(source_actor) = &self.source_actor {
            source_actor.validate("ingest source_actor")?;
        }
        validate_optional_non_empty(&self.client_key, "ingest client_key", limits::CLIENT_KEY)?;
        for facet in self.facets.keys() {
            validate_bounded(facet, "ingest facets name", limits::FACET_NAME)?;
        }
        validate_extensions(&self.extensions, "ingest evidence extensions")?;
        match (&self.payload, &self.payload_artifact) {
            (Some(_), None) | (None, Some(_)) => Ok(()),
            _ => Err(KipError::invalid_request_envelope(format!(
                "ingest entry {:?} must declare exactly one of payload / payload_artifact",
                self.key
            ))),
        }
    }
}

/// One optional block of the request envelope, answering for its own fields.
///
/// [`Request::validate`] used to reach into all six of them, which put a
/// block's rules three hundred lines from the fields they constrain — and made
/// "does this block get checked at all?" a question about a long function
/// rather than about the block. A block now carries its rules, and the request
/// decides only what no single block can: how the operations relate, whether
/// their ids collide, whether an `ingest` block has a transaction to be minted
/// into.
trait EnvelopeBlock {
    /// The block's own rules.
    fn validate(&self) -> Result<(), KipError>;
}

/// Validates an envelope block when the request carries one.
fn validate_block<T: EnvelopeBlock>(block: Option<&T>) -> Result<(), KipError> {
    match block {
        Some(block) => block.validate(),
        None => Ok(()),
    }
}

fn validate_optional_non_empty(
    value: &Option<String>,
    what: &str,
    max: usize,
) -> Result<(), KipError> {
    let Some(value) = value else { return Ok(()) };
    validate_bounded(value, what, max)
}

fn validate_bounded(value: &str, what: &str, max: usize) -> Result<(), KipError> {
    if value.trim().is_empty() {
        return Err(KipError::invalid_request_envelope(format!(
            "{what} must not be empty"
        )));
    }
    // The schema counts characters, so counting bytes here would reject a
    // legal value that happens to be non-ASCII.
    let length = value.chars().count();
    if length > max {
        return Err(KipError::invalid_request_envelope(format!(
            "{what} is {length} characters, and the wire schema allows at most {max}"
        )));
    }
    Ok(())
}

/// Extension keys are namespaced: `vendor/feature` (Spec §71).
///
/// An unnamespaced key is a vendor field sitting in the shared namespace,
/// where it will one day collide with a standard one — which is why the schema
/// admits no such key rather than tolerating it.
fn validate_extensions(extensions: &Option<Map<String, Json>>, what: &str) -> Result<(), KipError> {
    let Some(extensions) = extensions else {
        return Ok(());
    };
    for name in extensions.keys() {
        if !is_namespaced_extension(name) {
            return Err(KipError::invalid_identifier(format!(
                "{what} key {name:?} must be namespaced as <vendor>/<feature>"
            )));
        }
    }
    Ok(())
}

fn is_namespaced_extension(name: &str) -> bool {
    let Some((namespace, rest)) = name.split_once('/') else {
        return false;
    };
    let head_ok = |segment: &str| {
        segment
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphanumeric())
    };
    head_ok(namespace)
        && namespace
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        && head_ok(rest)
        && rest
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '/'))
}

/// Capability names are dotted identifiers, e.g. `belief_slot`, `kip.streaming`.
fn validate_capability_name(name: &str) -> Result<(), KipError> {
    let valid = name.chars().next().is_some_and(|c| c.is_ascii_alphabetic())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'));
    if valid {
        Ok(())
    } else {
        Err(KipError::invalid_identifier(format!(
            "capability requirement {name:?} must match [A-Za-z][A-Za-z0-9_.-]*"
        )))
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
    /// Echo of the request's idempotency key when one was given, so an
    /// `outcome_unknown` response can be recovered by key (§80.4, §81).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
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
    /// The operation's own transaction Receipt in `sequence` and
    /// `independent` modes (§75); absent in `atomic` mode, where the
    /// top-level receipt is the transaction's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt: Option<Receipt>,
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
    /// The world valid-time basis the projection ran under, when `FOR TIME`
    /// was applied.
    ///
    /// An independent axis from `snapshot_seq` (Spec §48.3): that one says
    /// *which cognitive history* was read, this one says *what moment in the
    /// world* the claims were evaluated for. Reporting the first without the
    /// second leaves a caller unable to tell a stale answer from a
    /// deliberately historical one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_at: Option<String>,
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
///
/// The id is not optional. An answer that reports *some* policy produced it
/// without saying which is indistinguishable from one that reports nothing,
/// and a projection whose policy cannot be named cannot be audited or
/// reproduced (Spec §27.2). The wire schema says the same thing with
/// `minProperties: 1`; saying it in the type is what makes the empty shape
/// unconstructible rather than merely invalid.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct PolicyIdentity {
    /// The policy id.
    pub id: String,
    /// The policy version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<PolicyVersion>,
}

impl PolicyIdentity {
    /// Names a policy without a version.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            version: None,
        }
    }

    /// Names a policy and the version of it that ran.
    pub fn versioned(id: impl Into<String>, version: PolicyVersion) -> Self {
        Self {
            id: id.into(),
            version: Some(version),
        }
    }
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

wire_enum! {
    /// The baseline SEARCH modes (Spec §66.3).
    pub enum SearchMode {
        /// Lexical matching.
        Keyword = "keyword",
        /// Embedding similarity.
        Semantic = "semantic",
        /// Both.
        Hybrid = "hybrid",
    }
}

/// The snapshot coordinate a response was produced at (Spec §78).
///
/// The sequence is not optional: a snapshot block whose coordinate is absent
/// states that the read was pinned somewhere without saying where, which no
/// caller can bind a later read to. Omit the whole block instead.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct SnapshotContext {
    /// The Space the snapshot belongs to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub space_id: Option<String>,
    /// The snapshot sequence.
    pub snapshot_seq: u64,
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

impl SnapshotContext {
    /// The snapshot coordinate a read was pinned to.
    pub fn at(snapshot_seq: u64) -> Self {
        Self {
            space_id: None,
            snapshot_seq,
            schema_environment_version: None,
            snapshot_token: None,
            extensions: None,
        }
    }
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
    /// The canonical digest of the Receipt without `receipt_digest` and
    /// `proofs` (§33.2); a signed Receipt signs it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_digest: Option<String>,
    /// The Principal the commit was attributed to, the ActorBinding it
    /// exercised and the delegation chain it acted under (§33.2), so an
    /// auditor can tie the Receipt to the Governance decision without
    /// reading the audit log.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<ReceiptOrigin>,
    /// Namespaced extensions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<Map<String, Json>>,
}

/// Who a commit was attributed to (Spec §33.2).
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ReceiptOrigin {
    /// The authenticated Principal.
    pub principal_id: String,
    /// The ActorBinding exercised (§28.3), when one was.
    #[serde(default)]
    pub actor_binding_id: Option<String>,
    /// The digest of the delegation chain acted under (§28.5), when any.
    #[serde(default)]
    pub delegation_digest: Option<String>,
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
            "operations": [{"command": "TRANSITION :x TO \"archived\""}],
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
                "command": "TRANSITION :x TO \"archived\"",
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
            "id": "projection-policy",
            "version": "7.1"
        }))
        .unwrap();
        assert_eq!(textual.version, Some(PolicyVersion::Text("7.1".into())));

        // A policy that cannot be named cannot be audited, so the id is not
        // optional and a version-only identity does not decode.
        assert!(
            serde_json::from_value::<PolicyIdentity>(serde_json::json!({ "version": "7.1" }))
                .is_err()
        );
        assert_eq!(
            serde_json::to_value(PolicyIdentity::new("projection-policy")).unwrap(),
            serde_json::json!({ "id": "projection-policy" })
        );
    }

    #[test]
    fn envelope_strings_are_held_to_the_wire_schemas_length_ceilings() {
        let mut request = Request::single("DESCRIBE PROTOCOL");
        request.request_id = Some("r".repeat(limits::REQUEST_ID));
        request.validate().expect("exactly at the ceiling is legal");

        request.request_id = Some("r".repeat(limits::REQUEST_ID + 1));
        let err = request.validate().expect_err("one over the ceiling");
        assert_eq!(err.code, KipErrorCode::InvalidRequestEnvelope);
        assert!(err.message.contains("at most"), "{}", err.message);

        // The schema counts characters, so a multi-byte value is not
        // rejected for the length of its encoding.
        request.request_id = Some("é".repeat(limits::REQUEST_ID));
        request.validate().expect("characters, not bytes");
    }

    #[test]
    fn extension_keys_must_be_namespaced() {
        let mut request = Request::single("DESCRIBE PROTOCOL");
        request.extensions = serde_json::json!({ "acme/tracing": { "critical": false } })
            .as_object()
            .cloned();
        request.validate().expect("a namespaced key is fine");

        request.extensions = serde_json::json!({ "tracing": { "critical": true } })
            .as_object()
            .cloned();
        let err = request.validate().expect_err("unnamespaced");
        assert_eq!(err.code, KipErrorCode::InvalidIdentifier);

        // A vendor field cannot squat the standard namespace by nesting either.
        request.extensions = serde_json::json!({ "/tracing": {} }).as_object().cloned();
        assert!(request.validate().is_err());
    }

    #[test]
    fn critical_extensions_are_reported_from_every_block_that_carries_them() {
        // §71: a runtime that cannot honour a critical extension must fail
        // rather than quietly proceed, so it has to be able to find them all.
        let mut request = Request::single("DESCRIBE PROTOCOL");
        request.extensions = serde_json::json!({
            "acme/redaction": { "critical": true },
            "acme/tracing": { "critical": false }
        })
        .as_object()
        .cloned();
        request.operations[0].extensions =
            serde_json::json!({ "acme/hints": { "critical": true } })
                .as_object()
                .cloned();
        request.execution = Some(Execution {
            mode: ExecutionMode::Independent,
            on_error: None,
            isolation: None,
            idempotency_key: None,
            extensions: serde_json::json!({ "acme/pinning": { "critical": true } })
                .as_object()
                .cloned(),
        });

        request.validate().expect("a legal envelope");
        assert_eq!(
            request.critical_extensions(),
            vec!["acme/hints", "acme/pinning", "acme/redaction"]
        );

        assert!(
            Request::single("DESCRIBE PROTOCOL")
                .critical_extensions()
                .is_empty()
        );
    }

    #[test]
    fn capability_requirements_are_named_like_capabilities() {
        let mut request = Request::single("DESCRIBE PROTOCOL");
        request.requires = serde_json::json!({ "belief_slot": true, "kip.streaming": true })
            .as_object()
            .cloned();
        request.validate().expect("dotted identifiers are fine");

        request.requires = serde_json::json!({ "!! nonsense": true })
            .as_object()
            .cloned();
        let err = request.validate().expect_err("not an identifier");
        assert_eq!(err.code, KipErrorCode::InvalidIdentifier);
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
        let mut request = Request::single(r#"TRANSITION :a TO "archived""#);
        request
            .operations
            .push(Operation::new(r#"TRANSITION :b TO "archived""#));
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
            ..Operation::new(r#"TRANSITION :x TO "tombstoned""#)
        };
        let err = operation.parse().expect_err("mislabelled write");
        assert_eq!(err.code, KipErrorCode::LanguageMismatch);

        let honest = Operation {
            language: Some(CommandType::Kml),
            ..Operation::new(r#"TRANSITION :x TO "tombstoned""#)
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
                "actions": [{"SetFields": [["confidence", {"Value": {"Number": 0.1}}]]}],
                "where_clauses": [{"Assertion": {
                    "variable": "a",
                    "matcher": {"id": {"Literal": {"String": "A-1"}}}
                }}],
                "limit": Json::Null,
                "expect_versions": []
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
                "expect_versions": [], "reference_policy": Json::Null, "confirm": ""
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
            ast: Some(parse_kip(r#"TRANSITION :old TO "archived""#).unwrap()),
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
    fn an_ingest_block_needs_a_transaction_to_be_minted_into() {
        // §71.1 mints each entry inside the request's transaction scope, and
        // makes ingestion transactional. A read-only request opens no such
        // scope: minting nothing while answering `succeeded` would leave the
        // caller believing the observation was recorded, which is the fidelity
        // failure ingestion exists to prevent.
        let ingest = IngestContext {
            evidence: vec![IngestEvidence {
                key: "msg".into(),
                evidence_class: "user_statement".into(),
                payload: Some(Json::from("I prefer dark mode.")),
                ..Default::default()
            }],
            extensions: None,
        };

        let mut read = Request::single(r#"FIND(?x) WHERE { ?x {type: "T"} }"#);
        read.ingest = Some(ingest.clone());
        let err = read
            .validate()
            .expect_err("a read has nothing to mint into");
        assert_eq!(err.code, KipErrorCode::InvalidRequestEnvelope);

        let mut write = Request::single(
            r#"ASSERT (:alice, "prefers", :dark) { by: :alice, mode: "stated", evidence: :msg }"#,
        );
        write.ingest = Some(ingest.clone());
        write.validate().expect("a KML operation carries the mint");

        // One KML operation among reads is enough: the transaction it opens is
        // the scope, and which operation opened it is not the caller's problem.
        let mut mixed = Request::single("DESCRIBE PRIMER");
        mixed
            .operations
            .push(Operation::new(r#"CREATE CONCEPT ?c { TYPE "T" NAME "n" }"#));
        mixed.execution = Some(Execution {
            mode: ExecutionMode::Sequence,
            on_error: None,
            isolation: None,
            idempotency_key: None,
            extensions: None,
        });
        mixed.ingest = Some(ingest);
        mixed.validate().expect("one mutation is a transaction");
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
            operations: vec![Operation::new(r#"TRANSITION :x TO "archived""#).with_op_id("op-1")],
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
                committed_at: Some("2026-08-16T00:00:00.000Z".into()),
                transaction_class: None,
                request_digest: None,
                semantic_plan_digest: None,
                result_digest: None,
                schema_environment_version: None,
                change_summary: None,
                proofs: vec![],
                receipt_digest: None,
                origin: None,
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
