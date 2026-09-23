//! # Execution framework
//!
//! [`Executor`] is the seam between the protocol and an engine: this crate
//! parses, classifies and validates; the engine decides everything that needs
//! state — Schema resolution, Governance, transactions, projection.
//!
//! Two contracts this module keeps on the protocol side, because getting them
//! wrong is a security bug rather than a behaviour difference:
//!
//! - a read-only path rejects state-changing semantics by what the command
//!   *is*, never by what it was labelled (§73.1, §88.3);
//! - a batch is not a transaction: [`execute_request`] runs `independent` and
//!   `sequence` and refuses to pretend it can offer `atomic` (§75.4).

use async_trait::async_trait;
use std::sync::Arc;

use crate::ast::{Command, CommandType};
use crate::error::KipError;
use crate::parser::parse_kip;
use crate::request::{
    ExecutionMode, OnError, Operation, OperationResult, OperationStatus, Request, Response,
    ResponseExecution, TopLevelStatus,
};

/// How a KIP command is executed against a Cognitive Nexus.
///
/// # Examples
///
/// ```rust,no_run
/// use anda_kip::{Command, Executor, Operation, Request, Response};
/// use async_trait::async_trait;
///
/// struct MyNexus;
///
/// #[async_trait]
/// impl Executor for MyNexus {
///     async fn execute(
///         &self,
///         command: Command,
///         request: &Request,
///         operation: &Operation,
///     ) -> Response {
///         let dry_run = request.is_dry_run();
///         let _operation_parameters = &operation.parameters;
///         match command {
///             Command::Kql(_query) => todo!("run the read"),
///             Command::Kml(_statement) => todo!("run the transaction"),
///             Command::Meta(_command) => todo!("answer the introspection"),
///         }
///     }
/// }
/// ```
#[async_trait]
pub trait Executor: Send + Sync {
    /// Executes one parsed command with its complete envelope context.
    ///
    /// An implementation MUST honor every applicable request- and
    /// operation-level field — including Space selection, parameter bindings,
    /// ingest, preconditions, idempotency, capability requirements, snapshot
    /// binding, deadline and dry-run — or fail explicitly. Ignoring one can
    /// change the meaning or safety of the command. In particular, a dry run
    /// MUST NOT establish a durable cognitive commit (Spec §69.3).
    ///
    /// Implementations report failures with the registry codes in
    /// [`crate::KipErrorCode`], which is what lets an Agent's retry policy
    /// switch on the outcome instead of reading prose.
    async fn execute(&self, command: Command, request: &Request, operation: &Operation)
    -> Response;
}

#[async_trait]
impl Executor for Box<dyn Executor> {
    async fn execute(
        &self,
        command: Command,
        request: &Request,
        operation: &Operation,
    ) -> Response {
        (**self).execute(command, request, operation).await
    }
}

#[async_trait]
impl Executor for Arc<dyn Executor> {
    async fn execute(
        &self,
        command: Command,
        request: &Request,
        operation: &Operation,
    ) -> Response {
        (**self).execute(command, request, operation).await
    }
}

#[async_trait]
impl Executor for &dyn Executor {
    async fn execute(
        &self,
        command: Command,
        request: &Request,
        operation: &Operation,
    ) -> Response {
        (**self).execute(command, request, operation).await
    }
}

/// Parses and executes one KIP command string.
///
/// Returns the classified language alongside the response so a caller can log
/// or meter reads and writes separately without re-parsing.
pub async fn execute_kip(
    executor: &impl Executor,
    command: &str,
    dry_run: bool,
) -> (CommandType, Response) {
    execute_one(executor, command, dry_run, |_| Ok(())).await
}

/// Parses and executes one KIP command on a read-only path.
///
/// Accepts KQL and META — including `VERIFY`, `VALIDATE`, `PREVIEW`, `HISTORY`,
/// `CHANGES` and `EXPORT CAPSULE` — and rejects state-changing
/// semantics (Spec §76).
///
/// The rejection is on parsed semantics, not on a declared label, so no
/// envelope field can talk a write past this boundary.
pub async fn execute_readonly(
    executor: &impl Executor,
    command: &str,
    dry_run: bool,
) -> (CommandType, Response) {
    execute_one(executor, command, dry_run, admits_readonly).await
}

/// The read-only endpoint's admission rule (§76, §88.3).
///
/// Decided on the parsed command, so a `language` label cannot downgrade a
/// write into read-only semantics (§73.1).
fn admits_readonly(command: &Command) -> Result<(), KipError> {
    if command.is_mutation() {
        return Err(KipError::readonly_violation(
            "this endpoint executes KQL and META only; KML mutations must go through the \
             state-capable runtime",
        ));
    }
    Ok(())
}

/// Parses one command into its own single-operation request and runs it,
/// refusing anything the endpoint does not admit.
///
/// The two entry points differ only in `admits`, and the difference is decided
/// on the parsed command: a declared language never reaches this, so no
/// envelope field can talk a write past a read-only endpoint (§73.1, §76).
async fn execute_one(
    executor: &impl Executor,
    text: &str,
    dry_run: bool,
    admits: impl Fn(&Command) -> Result<(), KipError>,
) -> (CommandType, Response) {
    let command = match parse_kip(text) {
        Ok(command) => command,
        Err(err) => return (CommandType::Unknown, err.into()),
    };
    let language = CommandType::from(&command);
    if let Err(err) = admits(&command) {
        return (language, err.into());
    }

    let request = single_command_request(text, dry_run);
    let response = executor
        .execute(command, &request, &request.operations[0])
        .await;
    (language, response)
}

fn single_command_request(command: &str, dry_run: bool) -> Request {
    let mut request = Request::single(command);
    request.options = Some(crate::request::RequestOptions {
        dry_run: Some(dry_run),
        ..Default::default()
    });
    request
}

/// Runs a whole request envelope.
///
/// Handles [`ExecutionMode::Independent`] and [`ExecutionMode::Sequence`].
/// [`ExecutionMode::Atomic`] is deliberately **not** emulated: one transaction,
/// one snapshot, read-your-writes and all-or-none commit are engine
/// properties, and a loop over an [`Executor`] cannot provide them. An engine
/// that does support atomic execution consumes the [`Request`] itself rather
/// than going through this helper.
///
/// Under `sequence`, an operation after a failure is reported as
/// [`OperationStatus::Skipped`] when `on_error` is `stop` — the earlier commits
/// stay durable, which is why the request-level status becomes
/// [`TopLevelStatus::Partial`] rather than `failed` (§75.2).
pub async fn execute_request(executor: &impl Executor, request: &Request) -> Response {
    run_request(executor, request, |_| Ok(())).await
}

/// A validated envelope with its operations parsed once, ready for execution.
///
/// Hosts may inspect the parsed commands for admission, timeout reporting and
/// logging. The envelope and commands cannot be changed independently. Syntax
/// errors remain per-operation results, preserving batch execution semantics.
#[derive(Debug)]
pub struct PreparedRequest {
    request: Request,
    parsed: Vec<Result<Command, KipError>>,
}

impl PreparedRequest {
    /// Validates an envelope and prepares all operations, including ingest.
    pub fn new(request: Request) -> Result<Self, KipError> {
        let parsed = request.prepare_operations()?;
        Ok(Self { request, parsed })
    }

    /// Decodes and prepares an envelope without parsing ingest commands twice.
    /// For raw JSON, call [`crate::parse_canonical_json`] first to preserve
    /// duplicate-key and numeric-source checks.
    pub fn from_value(value: crate::Json) -> Result<Self, KipError> {
        Self::new(Request::decode_value(value)?)
    }

    /// The validated envelope, available for correlation and bounded logging.
    pub fn request(&self) -> &Request {
        &self.request
    }

    /// Each prepared operation, in request order, including syntax failures.
    pub fn operations(&self) -> &[Result<Command, KipError>] {
        &self.parsed
    }

    /// Executes the prepared commands with the ordinary batch semantics.
    pub async fn execute(self, executor: &impl Executor) -> Response {
        run_prepared_request(executor, &self.request, self.parsed, |_| Ok(())).await
    }
}

/// Runs a whole request envelope on a read-only path (§76).
///
/// The envelope counterpart of [`execute_readonly`]: accepts KQL and META —
/// `VERIFY`, `VALIDATE`, `PREVIEW`, `HISTORY`, `CHANGES` and `EXPORT CAPSULE`
/// included — and refuses state-changing semantics.
///
/// The refusal is decided on every operation's *parsed* command before any of
/// them runs, and it fails the request rather than one operation. A caller that
/// sent a write to the read endpoint has a bug in what it thinks it is doing,
/// and executing the reads around the write would hide it — while an engine
/// that ran them would have to be trusted not to have committed anything.
pub async fn execute_request_readonly(executor: &impl Executor, request: &Request) -> Response {
    run_request(executor, request, admits_readonly).await
}

/// Runs an envelope, refusing anything `admits` does not allow.
///
/// The two entry points differ only in `admits`, exactly as the two
/// single-command ones do.
async fn run_request(
    executor: &impl Executor,
    request: &Request,
    admits: impl Fn(&Command) -> Result<(), KipError>,
) -> Response {
    let parsed = match request.prepare_operations() {
        Ok(parsed) => parsed,
        Err(err) => return Response::from(err).with_request_id(request.request_id.clone()),
    };

    run_prepared_request(executor, request, parsed, admits).await
}

async fn run_prepared_request(
    executor: &impl Executor,
    request: &Request,
    parsed: Vec<Result<Command, KipError>>,
    admits: impl Fn(&Command) -> Result<(), KipError>,
) -> Response {
    let mode = request.execution_mode();
    if mode == ExecutionMode::Atomic {
        return Response::from(KipError::unsupported_capability(
            "atomic execution needs one transaction, one snapshot and all-or-none commit; this \
             helper runs operations one at a time and will not fake them",
        ))
        .with_request_id(request.request_id.clone());
    }

    // Parsed once, up front: a read-only endpoint has to refuse a write before
    // a sibling operation runs, and parsing again inside the loop would let the
    // classification the gate used drift from the one the executor sees (§73.1).
    // An operation that does not parse cannot be a mutation; it fails as its
    // own result below, which is where a caller can correlate it by `op_id`.
    for command in parsed.iter().filter_map(|parsed| parsed.as_ref().ok()) {
        if let Err(err) = admits(command) {
            return Response::from(err).with_request_id(request.request_id.clone());
        }
    }

    let on_error = request
        .execution
        .as_ref()
        .and_then(|e| e.on_error)
        .unwrap_or(OnError::Stop);

    let mut results = Vec::with_capacity(request.operations.len());
    let mut stopped = false;
    let mut snapshot = None;
    let mut outcome_unknown_error = None;

    for (operation, parsed) in request.operations.iter().zip(parsed) {
        if stopped {
            results.push(
                OperationResult {
                    status: OperationStatus::Skipped,
                    ..Default::default()
                }
                .with_op_id(operation.op_id.clone()),
            );
            continue;
        }

        let result = match parsed {
            Ok(command) => {
                let response = executor.execute(command, request, operation).await;
                if response.status == TopLevelStatus::OutcomeUnknown
                    && outcome_unknown_error.is_none()
                {
                    outcome_unknown_error = Some(response.error.clone().unwrap_or_else(|| {
                        KipError::outcome_unknown(
                            "the executor could not establish whether the operation committed",
                        )
                        .into()
                    }));
                }
                if response.snapshot.is_some() {
                    snapshot = response.snapshot.clone();
                }
                operation_result_from(response)
            }
            Err(err) => OperationResult::failed(err),
        }
        .with_op_id(operation.op_id.clone());

        // `independent` isolates failures by definition; only `sequence` with
        // `on_error: stop` short-circuits.
        if result.status == OperationStatus::Failed
            && mode == ExecutionMode::Sequence
            && on_error == OnError::Stop
        {
            stopped = true;
        }
        results.push(result);
    }

    Response {
        status: if outcome_unknown_error.is_some() {
            TopLevelStatus::OutcomeUnknown
        } else {
            TopLevelStatus::derive(&results)
        },
        execution: Some(ResponseExecution {
            mode,
            on_error: Some(on_error),
            isolation: request.execution.as_ref().and_then(|e| e.isolation.clone()),
            // §81: echoed so a client holding `outcome_unknown` can recover by
            // key without re-deriving it.
            idempotency_key: request
                .execution
                .as_ref()
                .and_then(|e| e.idempotency_key.clone()),
            extensions: None,
        }),
        results,
        snapshot,
        error: outcome_unknown_error,
        ..Default::default()
    }
    .with_request_id(request.request_id.clone())
}

/// Folds a single-command [`Response`] into one operation's result.
///
/// A single-command response states its caveats and its cursor at the request
/// level; here that *is* the operation level, so they are carried across rather
/// than dropped on the way in.
fn operation_result_from(response: Response) -> OperationResult {
    let Response {
        results,
        warnings,
        next_cursor,
        error,
        receipt,
        ..
    } = response;

    if let Some(mut result) = results.into_iter().next() {
        result.warnings.extend(warnings);
        result.next_cursor = result.next_cursor.or(next_cursor);
        result.receipt = result.receipt.or(receipt);
        return result;
    }
    match error {
        Some(error) => OperationResult {
            status: OperationStatus::Failed,
            error: Some(error),
            warnings,
            next_cursor,
            receipt,
            ..Default::default()
        },
        None => OperationResult {
            warnings,
            next_cursor,
            receipt,
            ..OperationResult::no_effect()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Json;
    use crate::error::KipErrorCode;
    use crate::request::{Execution, Operation, Receipt, ReceiptStatus};

    fn committed_receipt(tx_id: &str) -> Receipt {
        Receipt {
            status: ReceiptStatus::Committed,
            tx_id: Some(tx_id.into()),
            space_seq: Some(42),
            space_id: Some("space-1".into()),
            snapshot_seq: Some(41),
            committed_at: Some("2026-08-16T00:00:00Z".into()),
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
        }
    }

    struct EchoNexus;

    #[async_trait]
    impl Executor for EchoNexus {
        async fn execute(
            &self,
            command: Command,
            _request: &Request,
            _operation: &Operation,
        ) -> Response {
            Response::ok(Json::String(CommandType::from(&command).to_string()))
        }
    }

    struct FailingNexus;

    #[async_trait]
    impl Executor for FailingNexus {
        async fn execute(
            &self,
            _command: Command,
            _request: &Request,
            _operation: &Operation,
        ) -> Response {
            Response::from(KipError::not_found_or_not_visible("nothing here"))
        }
    }

    #[tokio::test]
    async fn execute_kip_classifies_what_it_ran() {
        let (language, response) =
            execute_kip(&EchoNexus, r#"FIND(?x) WHERE { ?x {type: "T"} }"#, false).await;
        assert_eq!(language, CommandType::Kql);
        assert_eq!(response.first_result(), Some(&Json::String("KQL".into())));

        let (language, _) = execute_kip(&EchoNexus, "not a command", false).await;
        assert_eq!(language, CommandType::Unknown);
    }

    #[tokio::test]
    async fn the_readonly_path_rejects_writes_by_semantics() {
        let (language, response) =
            execute_readonly(&EchoNexus, r#"TRANSITION :x TO "tombstoned""#, false).await;
        assert_eq!(language, CommandType::Kml);
        assert_eq!(
            response.error.unwrap().parsed_code(),
            Some(KipErrorCode::ReadonlyViolation)
        );

        // META and KQL pass through, EXPORT CAPSULE included.
        for command in [
            "DESCRIBE PRIMER",
            r#"EXPORT CAPSULE :out WHERE { ?c {type: "T"} }"#,
            r#"FIND(?x) WHERE { ?x {type: "T"} }"#,
            "DESCRIBE SNAPSHOT",
        ] {
            let (_, response) = execute_readonly(&EchoNexus, command, false).await;
            assert_eq!(response.status, TopLevelStatus::Succeeded, "for {command}");
        }
    }

    #[tokio::test]
    async fn a_sequence_stops_but_keeps_what_already_ran() {
        let request = Request {
            execution: Some(Execution {
                on_error: Some(OnError::Stop),
                ..Execution::new(ExecutionMode::Sequence)
            }),
            operations: vec![
                Operation::new("DESCRIBE PROTOCOL").with_op_id("op-1"),
                Operation::new("DESCRIBE PRIMER").with_op_id("op-2"),
            ],
            ..Default::default()
        };

        let response = execute_request(&FailingNexus, &request).await;
        assert_eq!(response.results[0].status, OperationStatus::Failed);
        assert_eq!(response.results[1].status, OperationStatus::Skipped);
        assert_eq!(response.results[1].op_id.as_deref(), Some("op-2"));
        assert_eq!(response.status, TopLevelStatus::Failed);
    }

    #[tokio::test]
    async fn independent_operations_isolate_their_failures() {
        let request = Request {
            execution: Some(Execution::new(ExecutionMode::Independent)),
            operations: vec![
                Operation::new("DESCRIBE PROTOCOL"),
                Operation::new("nonsense"),
            ],
            ..Default::default()
        };

        let response = execute_request(&EchoNexus, &request).await;
        assert_eq!(response.results[0].status, OperationStatus::Succeeded);
        assert_eq!(response.results[1].status, OperationStatus::Failed);
        // Spec §75.2: a partial batch must not be reported as a total failure.
        assert_eq!(response.status, TopLevelStatus::Partial);
    }

    #[tokio::test]
    async fn a_commit_receipt_survives_the_batch_runner() {
        use crate::request::Warning;

        struct Committing;

        #[async_trait]
        impl Executor for Committing {
            async fn execute(
                &self,
                _command: Command,
                _request: &Request,
                _operation: &Operation,
            ) -> Response {
                Response {
                    receipt: Some(committed_receipt("tx-9")),
                    warnings: vec![Warning::Message("index lagged".into())],
                    ..Response::ok(Json::Bool(true))
                }
            }
        }

        // Spec §80.3: recovering a lost outcome means looking the transaction
        // up, which needs the `tx_id` the executor reported.
        let response = execute_request(
            &Committing,
            &Request::single(r#"TRANSITION :x TO "archived""#),
        )
        .await;
        assert_eq!(
            response.results[0]
                .receipt
                .as_ref()
                .and_then(|r| r.tx_id.as_deref()),
            Some("tx-9")
        );
        assert!(response.receipt.is_none());
        // A single-command response states its caveats at the request level;
        // here that is the operation level.
        assert_eq!(response.results[0].warnings.len(), 1);
    }

    #[tokio::test]
    async fn the_readonly_envelope_path_rejects_writes_by_semantics() {
        // The label says KQL; the command is a mutation. §73.1: what it parses
        // as is what decides, so the declared language cannot talk it through.
        let request = Request {
            execution: Some(Execution::new(ExecutionMode::Sequence)),
            operations: vec![
                Operation::new("DESCRIBE PRIMER").with_op_id("op-1"),
                Operation::new(r#"TRANSITION :x TO "tombstoned""#).with_op_id("op-2"),
            ],
            ..Default::default()
        };

        let response = execute_request_readonly(&EchoNexus, &request).await;
        assert_eq!(response.status, TopLevelStatus::Failed);
        assert_eq!(
            response.error.as_ref().unwrap().parsed_code(),
            Some(KipErrorCode::ReadonlyViolation)
        );
        // An envelope failure carries one result mirroring it, not one per
        // operation: the read beside the write was not executed either, so a
        // caller cannot mistake a half-served request for a served one.
        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].op_id, None);
    }

    #[tokio::test]
    async fn the_readonly_envelope_path_serves_reads_and_meta() {
        let request = Request {
            execution: Some(Execution::new(ExecutionMode::Independent)),
            operations: vec![
                Operation::new("DESCRIBE PRIMER"),
                Operation::new(r#"EXPORT CAPSULE :out WHERE { ?c {type: "T"} }"#),
                Operation::new(r#"FIND(?x) WHERE { ?x {type: "T"} }"#),
            ],
            ..Default::default()
        };

        let response = execute_request_readonly(&EchoNexus, &request).await;
        assert_eq!(response.status, TopLevelStatus::Succeeded, "{response:#?}");
        assert_eq!(response.results.len(), 3);
    }

    /// An unparseable operation is not a write, and refusing the whole request
    /// for it would report a syntax error as a readonly violation.
    #[tokio::test]
    async fn an_unparseable_operation_fails_on_its_own_result_on_the_readonly_path() {
        let response =
            execute_request_readonly(&EchoNexus, &Request::single("not a command")).await;
        assert_eq!(response.results.len(), 1);
        assert_eq!(
            response.results[0]
                .error
                .as_ref()
                .and_then(|error| error.parsed_code()),
            Some(KipErrorCode::InvalidSyntax)
        );
    }

    #[tokio::test]
    async fn atomic_execution_is_refused_rather_than_faked() {
        let request = Request {
            execution: Some(Execution::new(ExecutionMode::Atomic)),
            operations: vec![
                Operation::new(r#"TRANSITION :a TO "archived""#),
                Operation::new(r#"TRANSITION :b TO "archived""#),
            ],
            ..Default::default()
        };
        let response = execute_request(&EchoNexus, &request).await;
        assert_eq!(
            response.error.unwrap().parsed_code(),
            Some(KipErrorCode::UnsupportedCapability)
        );
    }

    #[tokio::test]
    async fn an_invalid_envelope_never_reaches_the_executor() {
        let request = Request {
            kip: "1.0".into(),
            ..Request::single("DESCRIBE PROTOCOL")
        };
        let response = execute_request(&EchoNexus, &request).await;
        assert_eq!(
            response.error.unwrap().parsed_code(),
            Some(KipErrorCode::UnsupportedProtocolVersion)
        );
    }

    #[tokio::test]
    async fn the_executor_receives_the_complete_request_and_operation_context() {
        struct ContextAware;

        #[async_trait]
        impl Executor for ContextAware {
            async fn execute(
                &self,
                _command: Command,
                request: &Request,
                operation: &Operation,
            ) -> Response {
                Response::ok(serde_json::json!({
                    "space": request.space.as_ref().and_then(|space| space.id.clone()),
                    "request_parameter": request
                        .parameters
                        .as_ref()
                        .and_then(|parameters| parameters.get("request_value"))
                        .cloned(),
                    "operation_parameter": operation
                        .parameters
                        .as_ref()
                        .and_then(|parameters| parameters.get("operation_value"))
                        .cloned(),
                    "dry_run": request.is_dry_run(),
                }))
            }
        }

        let request = serde_json::from_value::<Request>(serde_json::json!({
            "kip": "2.0",
            "space": {"id": "space-7"},
            "operations": [{
                "command": "DESCRIBE PROTOCOL",
                "parameters": {"operation_value": 2}
            }],
            "parameters": {"request_value": 1},
            "options": {"dry_run": true}
        }))
        .unwrap();

        let response = execute_request(&ContextAware, &request).await;
        assert_eq!(
            response.results[0].result,
            Some(serde_json::json!({
                "space": "space-7",
                "request_parameter": 1,
                "operation_parameter": 2,
                "dry_run": true,
            }))
        );
    }

    #[tokio::test]
    async fn an_unknown_write_outcome_is_never_flattened_to_failed() {
        struct UnknownOutcome;

        #[async_trait]
        impl Executor for UnknownOutcome {
            async fn execute(
                &self,
                _command: Command,
                _request: &Request,
                _operation: &Operation,
            ) -> Response {
                Response::outcome_unknown(KipError::outcome_unknown("connection dropped"))
            }
        }

        let response = execute_request(
            &UnknownOutcome,
            &Request::single(r#"TRANSITION :x TO "archived""#),
        )
        .await;
        assert_eq!(response.status, TopLevelStatus::OutcomeUnknown);
        assert_eq!(
            response
                .error
                .as_ref()
                .and_then(|error| error.parsed_code()),
            Some(KipErrorCode::OutcomeUnknown)
        );
    }

    #[tokio::test]
    async fn an_unknown_outcome_never_leaks_an_earlier_transactions_receipt() {
        struct CommitThenUnknown;

        #[async_trait]
        impl Executor for CommitThenUnknown {
            async fn execute(
                &self,
                _command: Command,
                _request: &Request,
                operation: &Operation,
            ) -> Response {
                if operation.op_id.as_deref() == Some("known") {
                    Response {
                        receipt: Some(committed_receipt("tx-known")),
                        ..Response::ok(Json::Bool(true))
                    }
                } else {
                    Response::outcome_unknown(KipError::outcome_unknown("connection dropped"))
                }
            }
        }

        let request = Request {
            execution: Some(Execution::new(ExecutionMode::Independent)),
            operations: vec![
                Operation::new(r#"TRANSITION :a TO "archived""#).with_op_id("known"),
                Operation::new(r#"TRANSITION :b TO "archived""#).with_op_id("unknown"),
            ],
            ..Default::default()
        };
        let response = execute_request(&CommitThenUnknown, &request).await;
        assert_eq!(response.status, TopLevelStatus::OutcomeUnknown);
        assert_eq!(response.receipt, None);
    }

    #[tokio::test]
    async fn prepared_execution_matches_ordinary_batch_semantics() {
        for mode in [
            ExecutionMode::Independent,
            ExecutionMode::Sequence,
            ExecutionMode::Atomic,
        ] {
            for on_error in [OnError::Stop, OnError::Continue] {
                if mode == ExecutionMode::Atomic && on_error == OnError::Continue {
                    continue;
                }
                let request = Request {
                    request_id: Some("prepared".into()),
                    execution: Some(Execution {
                        on_error: Some(on_error),
                        idempotency_key: Some("same-key".into()),
                        ..Execution::new(mode)
                    }),
                    operations: vec![
                        Operation::new("DESCRIBE PRIMER").with_op_id("read"),
                        Operation::new("not a command").with_op_id("bad"),
                        Operation::new(r#"TRANSITION :a TO "archived""#).with_op_id("write"),
                    ],
                    ..Default::default()
                };
                let expected = execute_request(&EchoNexus, &request).await;
                let prepared =
                    PreparedRequest::from_value(serde_json::to_value(request).unwrap()).unwrap();
                assert_eq!(prepared.operations().len(), 3);
                assert!(prepared.operations()[1].is_err());
                assert!(prepared.operations()[2].as_ref().unwrap().is_mutation());
                let actual = prepared.execute(&EchoNexus).await;
                assert_eq!(
                    serde_json::to_value(actual).unwrap(),
                    serde_json::to_value(expected).unwrap()
                );
            }
        }
    }

    #[test]
    fn prepared_decode_preserves_ingest_and_timestamp_validation() {
        let mut value = serde_json::json!({
            "kip":"2.0", "operations":[{"command":"DESCRIBE PRIMER"}],
            "ingest":{"evidence":[{"key":"e", "evidence_class":"user_statement", "payload":null}]}
        });
        assert_eq!(
            PreparedRequest::from_value(value.clone()).unwrap_err().code,
            Request::from_value(value.clone()).unwrap_err().code
        );
        value["operations"][0]["command"] = serde_json::json!(r#"TRANSITION :a TO "archived""#);
        assert!(PreparedRequest::from_value(value.clone()).is_ok());
        for at in [
            serde_json::json!(42),
            serde_json::json!("2026-09-23T00:00:00Z"),
        ] {
            value["ingest"]["evidence"][0]["observed_at"] = at;
            assert_eq!(
                PreparedRequest::from_value(value.clone()).unwrap_err().code,
                Request::from_value(value.clone()).unwrap_err().code
            );
        }
    }
}
