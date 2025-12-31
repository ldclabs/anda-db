//! # Execution framework and traits
//!
//! This module provides the execution framework for Knowledge Interaction Protocol (KIP) commands.
//! It defines the core `Executor` trait that must be implemented by any KIP command processor,
//! and provides a convenient high-level function for executing KIP commands from string input.
//!
//! The executor is responsible for taking parsed KIP commands (KQL queries, KML statements,
//! or META commands) and executing them against a knowledge graph or cognitive nexus,
//! returning structured responses.
use async_trait::async_trait;
use std::sync::Arc;

use crate::{Command, CommandType, Response, parse_kip};

/// The core trait that defines how KIP commands are executed.
///
/// This trait must be implemented by any system that wants to process KIP commands.
/// It provides a single asynchronous method for executing parsed commands and returning
/// structured responses.
///
/// # Design Philosophy
///
/// The `Executor` trait is designed to be:
/// - **Asynchronous**: All operations return futures to support non-blocking I/O
/// - **Generic**: Can be implemented by different backend systems (databases, APIs, etc.)
/// - **Error-safe**: Uses `Result` types for proper error handling
/// - **Send-safe**: Futures are `Send` to support multi-threaded execution
///
/// # Implementation Examples
///
/// ```rust,no_run
/// use anda_kip::{Executor, Command, Json, KipError, Response};
/// use async_trait::async_trait;
///
/// struct MyKnowledgeGraph {
///     // Your knowledge graph implementation
/// }
///
/// #[async_trait]
/// impl Executor for MyKnowledgeGraph {
///     async fn execute(&self, command: Command, dry_run: bool) -> Response {
///         match command {
///             Command::Kql(query) => {
///                 // Execute KQL query against knowledge graph
///                 todo!("Implement KQL execution")
///             },
///             Command::Kml(statement) => {
///                 // Execute KML statement to modify knowledge graph
///                 todo!("Implement KML execution")
///             },
///             Command::Meta(meta_cmd) => {
///                 // Execute META command for introspection
///                 todo!("Implement META execution")
///             }
///         }
///     }
/// }
/// ```
#[async_trait]
pub trait Executor: Send + Sync {
    /// Executes a parsed KIP command and returns the result.
    ///
    /// This method takes ownership of a `Command` (which can be a KQL query,
    /// KML statement, or META command) and executes it against the underlying
    /// knowledge system.
    ///
    /// # Arguments
    ///
    /// * `command` - The parsed KIP command to execute
    /// * `dry_run` - If true, the command is executed in dry-run mode (no state changes)
    ///
    /// # Returns
    ///
    /// - `Response`: The response from the execution.
    ///
    /// # Error Handling
    ///
    /// Implementations should return appropriate `KipError` variants for different
    /// failure scenarios following KIP Standard Error Codes:
    /// - **1xxx (Syntax Errors)**: `KipError::invalid_syntax()`, `KipError::invalid_identifier()`
    /// - **2xxx (Schema Errors)**: `KipError::type_mismatch()`, `KipError::constraint_violation()`
    /// - **3xxx (Logic/Data Errors)**: `KipError::not_found()`, `KipError::reference_error()`
    /// - **4xxx (System Errors)**: `KipError::internal_error()`, `KipError::execution_timeout()`
    ///
    /// # Performance Considerations
    ///
    /// - This method is async to support non-blocking I/O operations
    /// - The returned future is `Send` to enable multi-threaded execution
    /// - Implementations should consider query optimization and caching
    async fn execute(&self, command: Command, dry_run: bool) -> Response;
}

/// High-level convenience function for executing KIP commands from string input.
///
/// This function provides a complete pipeline from raw KIP command string to execution result.
/// It handles parsing the input string into a structured command and then delegates execution
/// to the provided executor implementation.
///
/// # Workflow
///
/// 1. **Parse**: Convert the input string into a structured `Command` AST
/// 2. **Execute**: Pass the parsed command to the executor for processing
/// 3. **Return**: Provide the structured response or detailed error information
///
/// # Arguments
///
/// * `executor` - An implementation of the `Executor` trait that will process the command
/// * `command` - The raw KIP command string to parse and execute
/// * `dry_run` - If true, the command is executed in dry-run mode (no state changes)
///
/// # Returns
///
/// A [`Response`] containing:
/// - `Result(Response)`: Successful execution with structured response data
/// - `Error(KipError)`: Either parsing or execution error with detailed information
///
/// # Error Types
///
/// This function can return errors following KIP Standard Error Codes:
/// - **Parse Errors**: `KIP_1001` (InvalidSyntax) when the input string is malformed
/// - **Execution Errors**: Various KIP error codes returned by the executor
///
/// # Examples
///
/// ```rust,no_run
/// use anda_kip::{execute_kip, Executor, Response};
///
/// async fn example(my_executor: impl Executor) {
///     // Execute a KQL query
///     let kql_result = execute_kip(
///         &my_executor,
///         "FIND(?drug) WHERE { ?drug {type: \"Drug\"} }",
///         true // dry_run
///     ).await;
///     println!("{kql_result:#?}");
///
///     // Execute a KML statement
///     let kml_result = execute_kip(
///         &my_executor,
///         "UPSERT { CONCEPT ?drug { {type: \"Drug\", name: \"Aspirin\" } } }",
///         true // dry_run
///     ).await;
///     println!("{kml_result:#?}");
///
///     // Execute a META command
///     let meta_result = execute_kip(
///         &my_executor,
///         "DESCRIBE PRIMER",
///         false // dry_run
///     ).await;
///
///     println!("{meta_result:#?}");
/// }
/// ```
///
/// # Performance Notes
///
/// - Parsing is performed synchronously before execution
/// - Consider caching parsed commands for repeated execution
/// - The executor implementation determines overall performance characteristics
pub async fn execute_kip(
    executor: &impl Executor,
    command: &str,
    dry_run: bool,
) -> (CommandType, Response) {
    // Parse the raw command string into a structured Command AST
    match parse_kip(command) {
        Ok(cmd) => {
            // Delegate execution to the provided executor
            let ty = CommandType::from(&cmd);
            (ty, executor.execute(cmd, dry_run).await)
        }
        Err(err) => (CommandType::Unknown, err.into()),
    }
}

#[async_trait]
impl Executor for Box<dyn Executor> {
    async fn execute(&self, command: Command, dry_run: bool) -> Response {
        (**self).execute(command, dry_run).await
    }
}

#[async_trait]
impl Executor for Arc<dyn Executor> {
    async fn execute(&self, command: Command, dry_run: bool) -> Response {
        (**self).execute(command, dry_run).await
    }
}

#[async_trait]
impl Executor for &dyn Executor {
    async fn execute(&self, command: Command, dry_run: bool) -> Response {
        (**self).execute(command, dry_run).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Command, Json};

    #[derive(Debug, Clone)]
    struct MyKnowledgeGraph {}

    #[async_trait]
    impl Executor for MyKnowledgeGraph {
        async fn execute(&self, command: Command, _dry_run: bool) -> Response {
            match command {
                Command::Kql(_) => Response::ok(Json::String("Mock KQL Response".to_string())),
                Command::Kml(_) => Response::ok(Json::String("Mock KML Response".to_string())),
                Command::Meta(_) => Response::ok(Json::String("Mock META Response".to_string())),
            }
        }
    }

    #[tokio::test]
    async fn test_execute_kip() {
        let executor = MyKnowledgeGraph {};

        // Test KQL command execution
        let kql_command = "FIND(?drug) WHERE { ?drug {type: \"Drug\"} }";
        let (ty, res) = execute_kip(&executor, kql_command, false).await;
        assert_eq!(ty, CommandType::Kql);
        assert!(matches!(res, Response::Ok { .. }));
    }
}
