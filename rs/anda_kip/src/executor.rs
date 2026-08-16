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
    let request = single_command_request(command, dry_run);
    match parse_kip(command) {
        Ok(command) => {
            let language = CommandType::from(&command);
            let response = executor
                .execute(command, &request, &request.operations[0])
                .await;
            (language, response)
        }
        Err(err) => (CommandType::Unknown, err.into()),
    }
}

/// Parses and executes one KIP command on a read-only path.
///
/// Accepts KQL and META — including `VERIFY`, `VALIDATE`, `PREVIEW`, `HISTORY`,
/// `CHANGES`, `SNAPSHOT` and `EXPORT CAPSULE` — and rejects state-changing
/// semantics (Spec §76).
///
/// The rejection is on parsed semantics, not on a declared label, so no
/// envelope field can talk a write past this boundary.
pub async fn execute_readonly(
    executor: &impl Executor,
    command: &str,
    dry_run: bool,
) -> (CommandType, Response) {
    let request = single_command_request(command, dry_run);
    match parse_kip(command) {
        Ok(command) if command.is_mutation() => (
            CommandType::Kml,
            KipError::readonly_violation(
                "this endpoint executes KQL and META only; KML mutations must go through the \
                 state-capable runtime",
            )
            .into(),
        ),
        Ok(command) => {
            let language = CommandType::from(&command);
            let response = executor
                .execute(command, &request, &request.operations[0])
                .await;
            (language, response)
        }
        Err(err) => (CommandType::Unknown, err.into()),
    }
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
    if let Err(err) = request.validate() {
        return Response::from(err).with_request_id(request.request_id.clone());
    }

    let mode = request.execution_mode();
    if mode == ExecutionMode::Atomic {
        return Response::from(KipError::unsupported_capability(
            "atomic execution needs one transaction, one snapshot and all-or-none commit; this \
             helper runs operations one at a time and will not fake them",
        ))
        .with_request_id(request.request_id.clone());
    }

    let on_error = request
        .execution
        .as_ref()
        .and_then(|e| e.on_error)
        .unwrap_or(OnError::Stop);

    let mut results = Vec::with_capacity(request.operations.len());
    let mut stopped = false;
    // This helper runs each operation as its own transaction, so a batch can
    // produce several receipts while the envelope has one slot for them. The
    // latest commit is the one a caller normally needs to inspect. If an
    // operation reports `outcome_unknown`, however, only that operation's
    // receipt is safe recovery data: returning an earlier known receipt would
    // point lookup at the wrong transaction (§80.3).
    let mut receipt = None;
    let mut snapshot = None;
    let mut outcome_unknown_error = None;

    for operation in &request.operations {
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

        let result = match operation.parse() {
            Ok(command) => {
                let response = executor.execute(command, request, operation).await;
                if response.status == TopLevelStatus::OutcomeUnknown {
                    if outcome_unknown_error.is_none() {
                        receipt = response.receipt.clone();
                        outcome_unknown_error = Some(response.error.clone().unwrap_or_else(|| {
                            KipError::outcome_unknown(
                                "the executor could not establish whether the operation committed",
                            )
                            .into()
                        }));
                    }
                } else if outcome_unknown_error.is_none() && response.receipt.is_some() {
                    receipt = response.receipt.clone();
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
            extensions: None,
        }),
        results,
        receipt,
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
        ..
    } = response;

    if let Some(mut result) = results.into_iter().next() {
        result.warnings.extend(warnings);
        result.next_cursor = result.next_cursor.or(next_cursor);
        return result;
    }
    match error {
        Some(error) => OperationResult {
            status: OperationStatus::Failed,
            error: Some(error),
            warnings,
            next_cursor,
            ..Default::default()
        },
        None => OperationResult {
            warnings,
            next_cursor,
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
        let (language, response) = execute_readonly(&EchoNexus, r#"TOMBSTONE :x"#, false).await;
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
            "SNAPSHOT",
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
        let response = execute_request(&Committing, &Request::single(r#"ARCHIVE :x"#)).await;
        assert_eq!(
            response.receipt.as_ref().and_then(|r| r.tx_id.as_deref()),
            Some("tx-9")
        );
        // A single-command response states its caveats at the request level;
        // here that is the operation level.
        assert_eq!(response.results[0].warnings.len(), 1);
    }

    #[tokio::test]
    async fn atomic_execution_is_refused_rather_than_faked() {
        let request = Request {
            execution: Some(Execution::new(ExecutionMode::Atomic)),
            operations: vec![Operation::new("ARCHIVE :a"), Operation::new("ARCHIVE :b")],
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

        let response = execute_request(&UnknownOutcome, &Request::single("ARCHIVE :x")).await;
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
                Operation::new("ARCHIVE :a").with_op_id("known"),
                Operation::new("ARCHIVE :b").with_op_id("unknown"),
            ],
            ..Default::default()
        };
        let response = execute_request(&CommitThenUnknown, &request).await;
        assert_eq!(response.status, TopLevelStatus::OutcomeUnknown);
        assert_eq!(response.receipt, None);
    }
}
