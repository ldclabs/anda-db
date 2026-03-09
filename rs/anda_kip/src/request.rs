//! # Request/Response structures for JSON-based communication
//!
//! This module implements the standardized request-response model for all interactions
//! with the Cognitive Nexus as defined in KIP specification section 6.
//!
//! LLM Agents send structured requests (typically encapsulated in Function Calling)
//! containing KIP commands to the Cognitive Nexus, which returns structured JSON responses.
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::borrow::Cow;

use crate::{
    CommandType, Json, Map,
    error::KipError,
    executor::{Executor, execute_kip},
};

/// Represents a single command item in the batch `commands` array.
///
/// Each element in the `commands` array can be either:
/// - A simple `String` (uses shared `parameters` from the parent Request)
/// - An `Object` with `{command, parameters}` (independent parameters override shared ones)
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum CommandItem {
    /// A simple command string that uses shared parameters from the parent Request
    Simple(String),

    /// A command with its own independent parameters that override shared parameters
    WithParams {
        /// The KIP command string
        command: String,

        /// Optional independent parameters for this specific command.
        /// These parameters override any shared parameters with the same key.
        #[serde(default)]
        parameters: Map<String, Json>,
    },
}

/// Defines the arguments for the `execute_kip` function, which is the standard interface
/// for an LLM to interact with the Cognitive Nexus.
///
/// # Single Command Example
/// ```json
/// {
///   "command": "FIND(?drug.name) WHERE { ?symptom {name: :symptom_name} (?drug, \"treats\", ?symptom) } LIMIT :limit",
///   "parameters": {
///     "symptom_name": "Headache",
///     "limit": 10
///   },
///   "dry_run": false
/// }
/// ```
///
/// # Batch Execution Example
/// ```json
/// {
///   "commands": [
///     "DESCRIBE PRIMER",
///     "FIND(?t.name) WHERE { ?t {type: \"$ConceptType\"} } LIMIT 50",
///     {
///       "command": "UPSERT { CONCEPT ?e { {type:\"Event\", name: :name} } }",
///       "parameters": { "name": "MyEvent" }
///     }
///   ],
///   "parameters": { "limit": 10 },
///   "dry_run": false
/// }
/// ```
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Request {
    /// The complete KIP command string (KQL, KML, or META).
    /// It can include placeholders like `:param_name` for parameter substitution.
    /// **Mutually exclusive with `commands`**.
    #[serde(default)]
    pub command: String,

    /// An array of KIP commands for batch execution.
    /// **Mutually exclusive with `command`**.
    /// Each element can be a `String` (uses shared `parameters`) or an `Object` with
    /// `{command, parameters}` (independent parameters override shared).
    /// Commands execute sequentially; **execution stops on first error**.
    #[serde(default)]
    pub commands: Vec<CommandItem>,

    /// An optional map of key-value pairs for parameter substitution.
    /// Keys in this map correspond to placeholders in the `command` string.
    /// For batch execution, these are shared parameters used by all commands
    /// unless overridden by individual command parameters.
    #[serde(default)]
    pub parameters: Map<String, Json>,

    /// If true, the command is validated for syntax and logic but not executed.
    /// No changes will be persisted to the knowledge graph.
    #[serde(default)]
    pub dry_run: bool,
}

impl Request {
    /// Checks if this request uses batch commands mode
    pub fn is_batch(&self) -> bool {
        !self.commands.is_empty()
    }

    /// Returns an iterator over all commands in this request.
    ///
    /// For single command mode, yields one item with the command and shared parameters.
    /// For batch mode, yields each command item with merged parameters
    /// (command-specific parameters override shared parameters).
    pub fn iter_commands(
        &self,
    ) -> impl Iterator<Item = (Cow<'_, str>, Cow<'_, Map<String, Json>>)> {
        let shared_params = &self.parameters;
        let single_command = &self.command;
        let commands = &self.commands;

        let is_batch = !commands.is_empty();

        // For single command mode
        let single_iter = if !is_batch {
            Some(std::iter::once((
                Cow::Borrowed(single_command.as_str()),
                Cow::Borrowed(shared_params),
            )))
        } else {
            None
        };

        // For batch mode
        let batch_iter = if is_batch {
            Some(commands.iter().map(move |item| match item {
                CommandItem::Simple(cmd) => {
                    (Cow::Borrowed(cmd.as_str()), Cow::Borrowed(shared_params))
                }
                CommandItem::WithParams {
                    command,
                    parameters,
                } => {
                    if parameters.is_empty() {
                        (
                            Cow::Borrowed(command.as_str()),
                            Cow::Borrowed(shared_params),
                        )
                    } else if shared_params.is_empty() {
                        (Cow::Borrowed(command.as_str()), Cow::Borrowed(parameters))
                    } else {
                        // Merge parameters: command-specific overrides shared
                        let mut merged = shared_params.clone();
                        for (k, v) in parameters {
                            merged.insert(k.clone(), v.clone());
                        }
                        (Cow::Borrowed(command.as_str()), Cow::Owned(merged))
                    }
                }
            }))
        } else {
            None
        };

        single_iter
            .into_iter()
            .flatten()
            .chain(batch_iter.into_iter().flatten())
    }

    /// Substitutes parameters into a command string
    ///
    /// Replaces all `:key_name` placeholders in the command with corresponding values
    /// from the parameters map.
    ///
    /// # Warning
    /// Placeholders must occupy a full JSON value position (e.g., `name: :param`).
    /// Do NOT embed placeholders inside quoted strings (e.g., `"Hello :name"`),
    /// because string values will be JSON-serialized with quotes, causing syntax errors.
    fn substitute_params(command: &str, parameters: &Map<String, Json>) -> String {
        if parameters.is_empty() {
            return command.to_string();
        }

        let mut result = command.to_string();
        for (key, value) in parameters {
            let placeholder = format!(":{}", key);
            let replacement = match value {
                Json::Number(n) => n.to_string(),
                Json::Bool(b) => b.to_string(),
                Json::Null => "null".to_string(),
                _ => serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()),
            };
            result = result.replace(&placeholder, &replacement);
        }
        result
    }

    /// Core implementation that checks for placeholders inside quoted strings.
    ///
    /// Returns a list of (parameter_name, position) for each placeholder found inside a quoted string.
    fn find_placeholders_in_strings(
        command: &str,
        parameters: &Map<String, Json>,
    ) -> Result<(), String> {
        let mut warnings = Vec::new();

        for key in parameters.keys() {
            let placeholder = format!(":{}", key);
            let mut search_start = 0;

            while let Some(pos) = command[search_start..].find(&placeholder) {
                let abs_pos = search_start + pos;

                // Check if this placeholder is inside a quoted string
                // by counting unescaped quotes before this position
                let before = &command[..abs_pos];
                let mut in_string = false;
                let mut chars = before.chars().peekable();

                while let Some(ch) = chars.next() {
                    if ch == '\\' {
                        // Skip escaped character
                        chars.next();
                    } else if ch == '"' {
                        in_string = !in_string;
                    }
                }

                if in_string {
                    warnings.push((key.clone(), abs_pos));
                }

                search_start = abs_pos + placeholder.len();
            }
        }

        if warnings.is_empty() {
            return Ok(());
        }

        let param_names: Vec<_> = warnings.iter().map(|(n, _)| n.as_str()).collect();
        Err(format!(
            "Possible cause: placeholder(s) {:?} appear to be inside quoted strings. \
                             Placeholders must occupy a full JSON value position, not be embedded \
                             inside strings.",
            param_names
        ))
    }

    /// Converts the request to a complete command string with parameter substitution
    ///
    /// Replaces all `:key_name` placeholders in the command with corresponding values
    /// from the parameters map. If no parameters are provided, returns the original command.
    ///
    /// # Note
    /// This method only works for single command mode. For batch mode, use `iter_commands()`
    /// or `execute()` instead.
    ///
    /// # Returns
    /// - `Cow::Borrowed` if no parameters need substitution
    /// - `Cow::Owned` if parameter substitution was performed
    pub fn to_command(&self) -> Cow<'_, str> {
        if self.parameters.is_empty() {
            Cow::Borrowed(&self.command)
        } else {
            Cow::Owned(Self::substitute_params(&self.command, &self.parameters))
        }
    }

    /// Executes the KIP command(s) using the provided executor
    ///
    /// For single command mode, executes the single command and returns its result.
    /// For batch mode, executes commands sequentially and stops on first error,
    /// returning an array of results for successfully executed commands.
    ///
    /// # Note
    /// If a parse error occurs, this method will check for common misuse patterns
    /// (like placeholders inside quoted strings) and include helpful hints in the error.
    pub async fn execute(&self, nexus: &impl Executor) -> (CommandType, Response) {
        if self.is_batch() {
            self.execute_batch(nexus).await
        } else {
            let command = self.to_command();
            let (cmd_type, response) = execute_kip(nexus, &command, self.dry_run).await;

            // If there's an error and we have parameters, check for placeholder misuse
            if let Response::Err { ref error, .. } = response
                && error.code.starts_with("KIP_1")
                && !self.parameters.is_empty()
            {
                let warnings = Self::find_placeholders_in_strings(&self.command, &self.parameters);
                if let Err(extra_hint) = warnings {
                    let mut new_error = error.clone();
                    new_error.hint = Some(match &error.hint {
                        Some(existing) => format!("{} {}", existing, extra_hint),
                        None => extra_hint,
                    });
                    return (
                        cmd_type,
                        Response::Err {
                            error: new_error,
                            result: None,
                        },
                    );
                }
            }

            (cmd_type, response)
        }
    }

    /// Executes batch commands sequentially
    ///
    /// Commands are executed in order. Execution stops on first error.
    /// Returns an array of results for all successfully executed commands.
    async fn execute_batch(&self, nexus: &impl Executor) -> (CommandType, Response) {
        let mut results = Vec::with_capacity(self.commands.len());
        let mut command_type = CommandType::Unknown;

        for (cmd, params) in self.iter_commands() {
            let substituted = Self::substitute_params(&cmd, &params);
            let (cmd_type, response) = execute_kip(nexus, &substituted, self.dry_run).await;
            if command_type != CommandType::Kml && cmd_type != CommandType::Unknown {
                command_type = cmd_type;
            }

            match response {
                Response::Ok { .. } => {
                    results.push(response);
                }
                Response::Err { mut error, .. } => {
                    // Check for placeholder misuse if it's a syntax error
                    if error.code.starts_with("KIP_1") && !params.is_empty() {
                        let warnings = Self::find_placeholders_in_strings(&cmd, &params);
                        if let Err(extra_hint) = warnings {
                            error.hint = Some(match &error.hint {
                                Some(existing) => format!("{} {}", existing, extra_hint),
                                None => extra_hint,
                            });
                        }
                    }
                    results.push(Response::err(error));

                    // Stop on first error, return error response
                    return (command_type, Response::ok(json!(results)));
                }
            }
        }

        (command_type, Response::ok(json!(results)))
    }
}

/// Response structure from the Cognitive Nexus
///
/// All responses from the Cognitive Nexus are JSON objects with this structure.
/// Either `result` or `error` must be present, but never both.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum Response {
    /// Successful response containing the request results
    ///
    /// Must be present when the request succeeds.
    Ok {
        /// The internal structure is defined by the specific KIP request command.
        result: Json,

        // An opaque token representing the pagination position after the last returned result.
        // If present, there may be more results available.
        #[serde(skip_serializing_if = "Option::is_none")]
        next_cursor: Option<String>,

        /// If true, the client should ignore this response.
        #[serde(skip_serializing_if = "Option::is_none")]
        ignore: Option<bool>,
    },

    /// Error response containing structured error details
    ///
    /// Must be present when the request fails.
    /// Contains detailed information about what went wrong.
    Err {
        error: ErrorObject,

        /// Partial result data, if any, when an error occurs.
        #[serde(skip_serializing_if = "Option::is_none")]
        result: Option<Json>,
    },
}

impl Response {
    pub fn ok(result: Json) -> Self {
        Self::Ok {
            result,
            next_cursor: None,
            ignore: None,
        }
    }

    pub fn err(error: impl Into<ErrorObject>) -> Self {
        Self::Err {
            error: error.into(),
            result: None,
        }
    }

    pub fn into_result(self) -> Result<Json, ErrorObject> {
        match self {
            Self::Ok { result, .. } => Ok(result),
            Self::Err { error, .. } => Err(error),
        }
    }
}

/// Structured error details for failed requests
///
/// Provides comprehensive error information following KIP Standard Error Codes.
/// Error codes are divided into 4 categories:
/// - **1xxx (Syntax Errors)**: Syntax errors where the code has incorrect format.
/// - **2xxx (Schema Errors)**: Schema errors violating type definitions or data constraints.
/// - **3xxx (Logic/Data Errors)**: Logic or data errors, such as referencing non-existent variables.
/// - **4xxx (System Errors)**: System-level errors, such as timeouts or insufficient permissions.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ErrorObject {
    /// KIP Standard Error Code (e.g., "KIP_1001", "KIP_2001")
    pub code: String,

    /// Human-readable error message
    ///
    /// Provides detailed information about the error for debugging and user feedback.
    pub message: String,

    /// Recovery hint for the AI Agent
    ///
    /// Provides suggestions on how to fix the error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,

    /// Optional additional error data
    ///
    /// May contain structured data relevant to the specific error,
    /// such as validation details or context information.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Json>,
}

// #[cfg(feature = "nightly")]
// impl<E> From<E> for ErrorObject
// where
//     E: std::error::Error,
// {
//     default fn from(error: E) -> Self {
//         ErrorObject {
//             name: std::any::type_name::<E>()
//                 .split("::")
//                 .last()
//                 .unwrap_or("Error")
//                 .to_string(),
//             message: error.to_string(),
//             data: None,
//         }
//     }
// }

impl From<String> for ErrorObject {
    fn from(message: String) -> Self {
        ErrorObject {
            code: "KIP_4000".to_string(),
            message,
            hint: None,
            data: None,
        }
    }
}

/// Conversion from serde_json::Error to ErrorObject
///
/// Handles JSON serialization/deserialization errors
impl From<serde_json::Error> for ErrorObject {
    fn from(error: serde_json::Error) -> Self {
        ErrorObject {
            code: "KIP_1001".to_string(),
            message: error.to_string(),
            hint: Some("Check JSON data format is valid.".to_string()),
            data: None,
        }
    }
}

/// Conversion from KipError to ErrorObject
///
/// Maps internal KIP errors to the standardized error response format
impl From<KipError> for ErrorObject {
    fn from(error: KipError) -> Self {
        let hint = error.hint().to_string();
        ErrorObject {
            code: error.code_str().to_string(),
            message: error.message,
            hint: Some(hint),
            data: None,
        }
    }
}

/// Display implementation for ErrorObject
impl std::fmt::Display for ErrorObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(hint) = &self.hint {
            write!(f, "[{}] {}, Hint: {}", self.code, self.message, hint)
        } else {
            write!(f, "[{}] {}", self.code, self.message)
        }
    }
}

/// Conversion from KipError to Response
///
/// Automatically wraps KIP errors in the appropriate response format
impl From<KipError> for Response {
    fn from(error: KipError) -> Self {
        Response::Err {
            error: error.into(),
            result: None,
        }
    }
}

impl<E> From<Result<(Json, Option<String>), E>> for Response
where
    E: Into<ErrorObject>,
{
    fn from(result: Result<(Json, Option<String>), E>) -> Self {
        match result {
            Ok((result, next_cursor)) => Response::Ok {
                result,
                next_cursor,
                ignore: None,
            },
            Err(err) => Response::Err {
                error: err.into(),
                result: None,
            },
        }
    }
}

impl<E> From<Result<Json, E>> for Response
where
    E: Into<ErrorObject>,
{
    fn from(result: Result<Json, E>) -> Self {
        match result {
            Ok(result) => Response::Ok {
                result,
                next_cursor: None,
                ignore: None,
            },
            Err(err) => Response::Err {
                error: err.into(),
                result: None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Command, parse_kml, parse_kql};
    use async_trait::async_trait;
    use serde_json::json;

    #[test]
    fn test_to_command_empty_parameters() {
        let request = Request {
            command: "FIND(?drug) WHERE { ?drug {type: \"Drug\"} }".to_string(),
            ..Default::default()
        };

        let result = request.to_command();
        assert_eq!(result, "FIND(?drug) WHERE { ?drug {type: \"Drug\"} }");
        assert!(matches!(result, Cow::Borrowed(_)));
        assert!(parse_kql(&result).is_ok());
    }

    #[test]
    fn test_to_command_string_parameter() {
        let mut parameters = Map::new();
        parameters.insert(
            "symptom_name".to_string(),
            Json::String("Headache".to_string()),
        );

        let request = Request {
            command: "FIND(?symptom) WHERE { ?symptom {name: :symptom_name} }".to_string(),
            parameters,
            ..Default::default()
        };

        let result = request.to_command();
        assert_eq!(
            result,
            "FIND(?symptom) WHERE { ?symptom {name: \"Headache\"} }"
        );
        assert!(matches!(result, Cow::Owned(_)));
        assert!(parse_kql(&result).is_ok());
    }

    #[test]
    fn test_to_command_number_parameter() {
        let mut parameters = Map::new();
        parameters.insert(
            "limit".to_string(),
            Json::Number(serde_json::Number::from(10)),
        );
        parameters.insert(
            "risk_level".to_string(),
            Json::Number(serde_json::Number::from_f64(3.5).unwrap()),
        );

        let request = Request {
            command: "FIND(?drug) WHERE { ?drug {type: \"Drug\"} FILTER(?drug.attributes.risk_level < :risk_level) } LIMIT :limit".to_string(),
            parameters,
            ..Default::default()
        };

        let result = request.to_command();
        assert_eq!(
            result,
            "FIND(?drug) WHERE { ?drug {type: \"Drug\"} FILTER(?drug.attributes.risk_level < 3.5) } LIMIT 10"
        );
        assert!(parse_kql(&result).is_ok());
    }

    #[test]
    fn test_to_command_object_parameter() {
        let mut parameters = Map::new();
        parameters.insert(
            "metadata".to_string(),
            json!({"confidence": 0.95, "source": "clinical_trial"}),
        );

        let request = Request {
            command:
                "UPSERT { CONCEPT ?drug { {type: \"Drug\", name: \"TestDrug\"} } WITH METADATA :metadata }"
                    .to_string(),
            parameters,
            ..Default::default()
        };

        let result = request.to_command();
        assert_eq!(
            result,
            "UPSERT { CONCEPT ?drug { {type: \"Drug\", name: \"TestDrug\"} } WITH METADATA {\"confidence\":0.95,\"source\":\"clinical_trial\"} }"
        );

        assert!(parse_kml(&result).is_ok());
    }

    #[test]
    fn test_to_command_multiple_parameters() {
        let mut parameters = Map::new();
        parameters.insert(
            "symptom_name".to_string(),
            Json::String("Headache".to_string()),
        );
        parameters.insert(
            "limit".to_string(),
            Json::Number(serde_json::Number::from(5)),
        );
        parameters.insert("include_experimental".to_string(), Json::Bool(false));

        let request = Request {
            command: r#"
                FIND(?drug.name)
                WHERE {
                    ?symptom{name: :symptom_name}
                    (?drug, "treats", ?symptom)
                    FILTER(?drug.attributes.experimental == :include_experimental)
                }
                LIMIT :limit
            "#
            .to_string(),
            parameters,
            ..Default::default()
        };

        let result = request.to_command();
        let expected = r#"
                FIND(?drug.name)
                WHERE {
                    ?symptom{name: "Headache"}
                    (?drug, "treats", ?symptom)
                    FILTER(?drug.attributes.experimental == false)
                }
                LIMIT 5
            "#;
        assert_eq!(result, expected);
        assert!(parse_kql(&result).is_ok());
    }

    #[test]
    fn test_to_command_same_parameter_multiple_times() {
        let mut parameters = Map::new();
        parameters.insert(
            "drug_type".to_string(),
            Json::String("Analgesic".to_string()),
        );

        let request = Request {
            command:
                "FIND(?drug1, ?drug2) WHERE { ?drug1 {type: :drug_type} ?drug2 {type: :drug_type} }"
                    .to_string(),
            parameters,
            ..Default::default()
        };

        let result = request.to_command();
        assert_eq!(
            result,
            "FIND(?drug1, ?drug2) WHERE { ?drug1 {type: \"Analgesic\"} ?drug2 {type: \"Analgesic\"} }"
        );
        assert!(parse_kql(&result).is_ok());
    }

    #[test]
    fn test_to_command_parameter_not_found() {
        let mut parameters = Map::new();
        parameters.insert(
            "existing_param".to_string(),
            Json::String("value".to_string()),
        );

        let request = Request {
            command: "FIND(?item) WHERE { ?item{name: :missing_param} }".to_string(),
            parameters,
            ..Default::default()
        };

        let result = request.to_command();
        // 不存在的参数应该保持原样
        assert_eq!(result, "FIND(?item) WHERE { ?item{name: :missing_param} }");
        assert!(parse_kql(&result).is_err());
    }

    #[test]
    fn test_to_command_special_characters_in_string() {
        let mut parameters = Map::new();
        parameters.insert(
            "special_name".to_string(),
            Json::String("Drug with \"quotes\" and :symbols".to_string()),
        );

        let request = Request {
            command: "FIND(?drug) WHERE { ?drug{name: :special_name} }".to_string(),
            parameters,
            ..Default::default()
        };

        let result = request.to_command();
        assert_eq!(
            result,
            "FIND(?drug) WHERE { ?drug{name: \"Drug with \\\"quotes\\\" and :symbols\"} }"
        );
        assert!(parse_kql(&result).is_ok());
    }

    #[test]
    fn test_to_command_complex_kip_example() {
        // 测试一个符合 KIP 规范的完整示例
        let mut parameters = Map::new();
        parameters.insert(
            "symptom_name".to_string(),
            Json::String("Brain Fog".to_string()),
        );
        parameters.insert(
            "confidence_threshold".to_string(),
            Json::Number(serde_json::Number::from_f64(0.8).unwrap()),
        );
        parameters.insert(
            "max_results".to_string(),
            Json::Number(serde_json::Number::from(20)),
        );

        let request = Request {
            command: r#"
                FIND(?drug.name, ?drug.metadata.confidence)
                WHERE {
                    (?drug, "treats", {name: :symptom_name})
                    FILTER(?drug.metadata.confidence > :confidence_threshold)
                }
                ORDER BY ?drug.metadata.confidence DESC
                LIMIT :max_results
            "#
            .to_string(),
            parameters,
            ..Default::default()
        };

        let result = request.to_command();
        let expected = r#"
                FIND(?drug.name, ?drug.metadata.confidence)
                WHERE {
                    (?drug, "treats", {name: "Brain Fog"})
                    FILTER(?drug.metadata.confidence > 0.8)
                }
                ORDER BY ?drug.metadata.confidence DESC
                LIMIT 20
            "#;
        assert_eq!(result, expected);
        assert!(parse_kql(expected).is_ok());
    }

    #[test]
    fn test_response() {
        let res = Response::ok(json!("Success"));
        assert_eq!(
            serde_json::to_string(&res).unwrap(),
            r#"{"result":"Success"}"#
        );
        assert_eq!(
            res,
            serde_json::from_str(r#"{"result":"Success"}"#).unwrap()
        );

        let res = Response::Ok {
            result: json!("Success"),
            next_cursor: Some("abcdef".to_string()),
            ignore: Some(true),
        };
        assert_eq!(
            serde_json::to_string(&res).unwrap(),
            r#"{"result":"Success","next_cursor":"abcdef","ignore":true}"#
        );

        let res = Response::err(ErrorObject {
            code: "KIP_4003".to_string(),
            message: "An error occurred".to_string(),
            hint: Some("Contact system administrator.".to_string()),
            data: Some(json!("Additional info")),
        });
        assert_eq!(
            serde_json::to_string(&res).unwrap(),
            r#"{"error":{"code":"KIP_4003","message":"An error occurred","hint":"Contact system administrator.","data":"Additional info"}}"#
        );
    }

    #[test]
    fn test_command_item_deserialization() {
        // Test simple string command
        let simple: CommandItem = serde_json::from_str(r#""DESCRIBE PRIMER""#).unwrap();
        assert!(matches!(simple, CommandItem::Simple(s) if s == "DESCRIBE PRIMER"));

        // Test command with parameters
        let with_params: CommandItem = serde_json::from_str(
            r#"{"command": "FIND(?x) WHERE { ?x {name: :name} }", "parameters": {"name": "test"}}"#,
        )
        .unwrap();
        match with_params {
            CommandItem::WithParams {
                command,
                parameters,
            } => {
                assert_eq!(command, "FIND(?x) WHERE { ?x {name: :name} }");
                assert_eq!(
                    parameters.get("name"),
                    Some(&Json::String("test".to_string()))
                );
            }
            _ => panic!("Expected WithParams variant"),
        }

        // Test command with empty parameters
        let with_empty_params: CommandItem =
            serde_json::from_str(r#"{"command": "DESCRIBE PRIMER", "parameters": {}}"#).unwrap();
        match with_empty_params {
            CommandItem::WithParams {
                command,
                parameters,
            } => {
                assert_eq!(command, "DESCRIBE PRIMER");
                assert!(parameters.is_empty());
            }
            _ => panic!("Expected WithParams variant"),
        }
    }

    #[test]
    fn test_batch_request_deserialization() {
        let json_str = r#"{
            "commands": [
                "DESCRIBE PRIMER",
                "FIND(?t.name) WHERE { ?t {type: \"$ConceptType\"} } LIMIT 50",
                {
                    "command": "UPSERT { CONCEPT ?e { {type:\"Event\", name: :name} } }",
                    "parameters": { "name": "MyEvent" }
                }
            ],
            "parameters": { "limit": 10 }
        }"#;

        let request: Request = serde_json::from_str(json_str).unwrap();
        assert!(request.is_batch());
        assert_eq!(request.commands.len(), 3);
        assert_eq!(
            request.parameters.get("limit"),
            Some(&Json::Number(10.into()))
        );

        // Verify command items
        assert!(matches!(&request.commands[0], CommandItem::Simple(s) if s == "DESCRIBE PRIMER"));
        assert!(matches!(&request.commands[1], CommandItem::Simple(s) if s.contains("FIND")));
        match &request.commands[2] {
            CommandItem::WithParams {
                command,
                parameters,
            } => {
                assert!(command.contains("UPSERT"));
                assert_eq!(
                    parameters.get("name"),
                    Some(&Json::String("MyEvent".to_string()))
                );
            }
            _ => panic!("Expected WithParams variant"),
        }
    }

    #[test]
    fn test_iter_commands_single_mode() {
        let mut parameters = Map::new();
        parameters.insert("name".to_string(), Json::String("test".to_string()));

        let request = Request {
            command: "FIND(?x) WHERE { ?x {name: :name} }".to_string(),
            commands: vec![],
            parameters: parameters.clone(),
            dry_run: false,
        };

        assert!(!request.is_batch());
        let items: Vec<_> = request.iter_commands().collect();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].0.as_ref(), "FIND(?x) WHERE { ?x {name: :name} }");
        assert_eq!(
            items[0].1.get("name"),
            Some(&Json::String("test".to_string()))
        );
    }

    #[test]
    fn test_iter_commands_batch_mode() {
        let mut shared_params = Map::new();
        shared_params.insert("limit".to_string(), Json::Number(10.into()));
        shared_params.insert(
            "shared".to_string(),
            Json::String("shared_value".to_string()),
        );

        let mut cmd_params = Map::new();
        cmd_params.insert("name".to_string(), Json::String("MyEvent".to_string()));
        cmd_params.insert("shared".to_string(), Json::String("overridden".to_string()));

        let request = Request {
            command: String::new(),
            commands: vec![
                CommandItem::Simple("DESCRIBE PRIMER".to_string()),
                CommandItem::WithParams {
                    command: "FIND(?x) WHERE { ?x {type: :type} }".to_string(),
                    parameters: Map::new(),
                },
                CommandItem::WithParams {
                    command: "UPSERT { CONCEPT ?e { {name: :name} } }".to_string(),
                    parameters: cmd_params,
                },
            ],
            parameters: shared_params,
            dry_run: false,
        };

        assert!(request.is_batch());
        let items: Vec<_> = request.iter_commands().collect();
        assert_eq!(items.len(), 3);

        // First command uses shared params
        assert_eq!(items[0].0.as_ref(), "DESCRIBE PRIMER");
        assert_eq!(items[0].1.get("limit"), Some(&Json::Number(10.into())));

        // Second command (with empty params) uses shared params
        assert_eq!(items[1].0.as_ref(), "FIND(?x) WHERE { ?x {type: :type} }");
        assert_eq!(items[1].1.get("limit"), Some(&Json::Number(10.into())));

        // Third command has merged params (command-specific overrides shared)
        assert_eq!(
            items[2].0.as_ref(),
            "UPSERT { CONCEPT ?e { {name: :name} } }"
        );
        assert_eq!(
            items[2].1.get("name"),
            Some(&Json::String("MyEvent".to_string()))
        );
        assert_eq!(items[2].1.get("limit"), Some(&Json::Number(10.into())));
        // "shared" should be overridden
        assert_eq!(
            items[2].1.get("shared"),
            Some(&Json::String("overridden".to_string()))
        );
    }

    #[test]
    fn test_batch_request_serialization() {
        let mut cmd_params = Map::new();
        cmd_params.insert("name".to_string(), Json::String("MyEvent".to_string()));

        let mut shared_params = Map::new();
        shared_params.insert("limit".to_string(), Json::Number(10.into()));

        let request = Request {
            command: String::new(),
            commands: vec![
                CommandItem::Simple("DESCRIBE PRIMER".to_string()),
                CommandItem::WithParams {
                    command: "UPSERT { CONCEPT ?e { {name: :name} } }".to_string(),
                    parameters: cmd_params,
                },
            ],
            parameters: shared_params,
            dry_run: true,
        };

        let json_str = serde_json::to_string(&request).unwrap();
        let parsed: Request = serde_json::from_str(&json_str).unwrap();

        assert!(parsed.is_batch());
        assert_eq!(parsed.commands.len(), 2);
        assert!(parsed.dry_run);
    }

    #[test]
    fn test_single_command_mode_default() {
        let request = Request {
            command: "DESCRIBE PRIMER".to_string(),
            ..Default::default()
        };

        assert!(!request.is_batch());
        assert!(request.commands.is_empty());
    }

    #[test]
    fn test_validate_placeholder_usage_valid() {
        // Valid usage: placeholder at value position
        let mut parameters = Map::new();
        parameters.insert("name".to_string(), Json::String("John".to_string()));
        parameters.insert("age".to_string(), Json::Number(25.into()));

        let request = Request {
            command: r#"SET ATTRIBUTES { name: :name, age: :age }"#.to_string(),
            parameters,
            ..Default::default()
        };

        let warnings = Request::find_placeholders_in_strings(&request.command, &request.parameters);
        assert!(warnings.is_ok(), "Should have no warnings for valid usage");
    }

    #[test]
    fn test_validate_placeholder_usage_invalid() {
        // Invalid usage: placeholder inside quoted string
        let mut parameters = Map::new();
        parameters.insert("user_id".to_string(), Json::String("user123".to_string()));

        let request = Request {
            command: r#"SET ATTRIBUTES { summary: "Hello :user_id, welcome!" }"#.to_string(),
            parameters,
            ..Default::default()
        };

        let warnings = Request::find_placeholders_in_strings(&request.command, &request.parameters);
        assert!(
            warnings.unwrap_err().contains("user_id"),
            "Should warn about 'user_id' placeholder"
        );
    }

    #[test]
    fn test_validate_placeholder_usage_mixed() {
        // Mix of valid and invalid usages
        let mut parameters = Map::new();
        parameters.insert("name".to_string(), Json::String("John".to_string()));
        parameters.insert("user_id".to_string(), Json::String("user123".to_string()));

        let request = Request {
            command: r#"SET ATTRIBUTES { name: :name, summary: "User :user_id joined" }"#
                .to_string(),
            parameters,
            ..Default::default()
        };

        let warnings = Request::find_placeholders_in_strings(&request.command, &request.parameters);
        assert!(
            warnings.unwrap_err().contains("user_id"),
            "Should warn about 'user_id' placeholder"
        );
    }

    #[test]
    fn test_validate_placeholder_usage_escaped_quotes() {
        // Placeholder after escaped quote should be correctly detected
        let mut parameters = Map::new();
        parameters.insert("name".to_string(), Json::String("John".to_string()));

        let request = Request {
            command: r#"SET ATTRIBUTES { desc: "Say \"hello\" to :name" }"#.to_string(),
            parameters,
            ..Default::default()
        };

        let warnings = Request::find_placeholders_in_strings(&request.command, &request.parameters);
        // :name is still inside the string (after escaped quotes)
        assert!(
            warnings.unwrap_err().contains("name"),
            "Should warn about 'name' placeholder"
        );
    }

    // --- Mock Executor for async execute tests ---

    #[derive(Debug)]
    struct MockExecutor;

    #[async_trait]
    impl Executor for MockExecutor {
        async fn execute(&self, command: Command, _dry_run: bool) -> Response {
            match command {
                Command::Kql(query) => Response::Ok {
                    result: json!({
                        "type": "kql",
                        "find_count": query.find_clause.expressions.len()
                    }),
                    next_cursor: None,
                    ignore: None,
                },
                Command::Kml(_) => Response::ok(json!({"type": "kml", "upserted": 1})),
                Command::Meta(_) => Response::ok(json!({"type": "meta"})),
            }
        }
    }

    #[derive(Debug)]
    struct FailingExecutor;

    #[async_trait]
    impl Executor for FailingExecutor {
        async fn execute(&self, _command: Command, _dry_run: bool) -> Response {
            Response::err(ErrorObject {
                code: "KIP_3001".to_string(),
                message: "Not found".to_string(),
                hint: Some("Check that the referenced concept exists.".to_string()),
                data: None,
            })
        }
    }

    // --- Single command execute tests ---

    #[tokio::test]
    async fn test_execute_single_kql_command() {
        let executor = MockExecutor;
        let request = Request {
            command: r#"FIND(?drug.name) WHERE { ?drug {type: "Drug"} } LIMIT 10"#.to_string(),
            ..Default::default()
        };

        let (cmd_type, response) = request.execute(&executor).await;
        assert_eq!(cmd_type, CommandType::Kql);
        match response {
            Response::Ok {
                result,
                next_cursor,
                ignore,
            } => {
                assert_eq!(result["type"], "kql");
                assert_eq!(result["find_count"], 1);
                assert!(next_cursor.is_none());
                assert!(ignore.is_none());
            }
            _ => panic!("Expected Ok response"),
        }
    }

    #[tokio::test]
    async fn test_execute_single_kml_command() {
        let executor = MockExecutor;
        let request = Request {
            command: r#"UPSERT { CONCEPT ?d { {type: "Drug", name: "Aspirin"} } }"#.to_string(),
            ..Default::default()
        };

        let (cmd_type, response) = request.execute(&executor).await;
        assert_eq!(cmd_type, CommandType::Kml);
        match response {
            Response::Ok { result, .. } => {
                assert_eq!(result["type"], "kml");
                assert_eq!(result["upserted"], 1);
            }
            _ => panic!("Expected Ok response"),
        }
    }

    #[tokio::test]
    async fn test_execute_single_meta_command() {
        let executor = MockExecutor;
        let request = Request {
            command: "DESCRIBE PRIMER".to_string(),
            ..Default::default()
        };

        let (cmd_type, response) = request.execute(&executor).await;
        assert_eq!(cmd_type, CommandType::Meta);
        match response {
            Response::Ok { result, .. } => {
                assert_eq!(result["type"], "meta");
            }
            _ => panic!("Expected Ok response"),
        }
    }

    #[tokio::test]
    async fn test_execute_single_command_with_params() {
        let executor = MockExecutor;
        let mut parameters = Map::new();
        parameters.insert("name".to_string(), Json::String("Aspirin".to_string()));
        parameters.insert("limit".to_string(), Json::Number(5.into()));

        let request = Request {
            command: r#"FIND(?drug) WHERE { ?drug {name: :name} } LIMIT :limit"#.to_string(),
            parameters,
            ..Default::default()
        };

        let (cmd_type, response) = request.execute(&executor).await;
        assert_eq!(cmd_type, CommandType::Kql);
        assert!(matches!(response, Response::Ok { .. }));
    }

    #[tokio::test]
    async fn test_execute_single_command_syntax_error() {
        let executor = MockExecutor;
        let request = Request {
            command: "INVALID COMMAND SYNTAX".to_string(),
            ..Default::default()
        };

        let (cmd_type, response) = request.execute(&executor).await;
        assert_eq!(cmd_type, CommandType::Unknown);
        match response {
            Response::Err { error, .. } => {
                assert!(error.code.starts_with("KIP_1"));
            }
            _ => panic!("Expected Err response for syntax error"),
        }
    }

    #[tokio::test]
    async fn test_execute_single_command_syntax_error_with_placeholder_hint() {
        let executor = MockExecutor;
        let mut parameters = Map::new();
        parameters.insert("name".to_string(), Json::String("test".to_string()));

        let request = Request {
            command: r#"FIND(?x) WHERE { ?x {name: "Hello :name"} }"#.to_string(),
            parameters,
            ..Default::default()
        };

        let (cmd_type, response) = request.execute(&executor).await;
        assert_eq!(cmd_type, CommandType::Unknown);
        match response {
            Response::Err { error, .. } => {
                assert!(error.code.starts_with("KIP_1"));
                assert!(error.hint.as_ref().unwrap().contains("name"));
            }
            _ => panic!("Expected Err response"),
        }
    }

    #[tokio::test]
    async fn test_execute_single_command_executor_error() {
        let executor = FailingExecutor;
        let request = Request {
            command: r#"FIND(?drug) WHERE { ?drug {type: "Drug"} }"#.to_string(),
            ..Default::default()
        };

        let (cmd_type, response) = request.execute(&executor).await;
        assert_eq!(cmd_type, CommandType::Kql);
        match response {
            Response::Err { error, .. } => {
                assert_eq!(error.code, "KIP_3001");
                assert_eq!(error.message, "Not found");
            }
            _ => panic!("Expected Err response from failing executor"),
        }
    }

    // --- Batch command execute tests ---

    #[tokio::test]
    async fn test_execute_batch_all_success() {
        let executor = MockExecutor;
        let request = Request {
            commands: vec![
                CommandItem::Simple("DESCRIBE PRIMER".to_string()),
                CommandItem::Simple(
                    r#"FIND(?t.name) WHERE { ?t {type: "$ConceptType"} } LIMIT 50"#.to_string(),
                ),
            ],
            ..Default::default()
        };

        let (cmd_type, response) = request.execute(&executor).await;
        assert_eq!(cmd_type, CommandType::Kql);
        match response {
            Response::Ok { result, .. } => {
                let arr = result.as_array().unwrap();
                assert_eq!(arr.len(), 2);
                // First result is META
                assert_eq!(arr[0]["result"]["type"], "meta");
                // Second result is KQL
                assert_eq!(arr[1]["result"]["type"], "kql");
            }
            _ => panic!("Expected Ok response for batch"),
        }
    }

    #[tokio::test]
    async fn test_execute_batch_with_params() {
        let executor = MockExecutor;
        let mut shared_params = Map::new();
        shared_params.insert("limit".to_string(), Json::Number(10.into()));

        let mut cmd_params = Map::new();
        cmd_params.insert("name".to_string(), Json::String("TestDrug".to_string()));

        let request = Request {
            commands: vec![
                CommandItem::Simple("DESCRIBE PRIMER".to_string()),
                CommandItem::WithParams {
                    command: r#"UPSERT { CONCEPT ?d { {type: "Drug", name: :name} } }"#.to_string(),
                    parameters: cmd_params,
                },
            ],
            parameters: shared_params,
            ..Default::default()
        };

        let (cmd_type, response) = request.execute(&executor).await;
        assert_eq!(cmd_type, CommandType::Kml);
        match response {
            Response::Ok { result, .. } => {
                let arr = result.as_array().unwrap();
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0]["result"]["type"], "meta");
                assert_eq!(arr[1]["result"]["type"], "kml");
            }
            _ => panic!("Expected Ok response for batch with params"),
        }
    }

    #[tokio::test]
    async fn test_execute_batch_stops_on_syntax_error() {
        let executor = MockExecutor;
        let request = Request {
            commands: vec![
                CommandItem::Simple("DESCRIBE PRIMER".to_string()),
                CommandItem::Simple("INVALID SYNTAX HERE".to_string()),
                CommandItem::Simple(r#"FIND(?t) WHERE { ?t {type: "$ConceptType"} }"#.to_string()),
            ],
            ..Default::default()
        };

        let (_cmd_type, response) = request.execute(&executor).await;
        match response {
            Response::Ok { result, .. } => {
                let arr = result.as_array().unwrap();
                // Should have 2 results: first success, second error, third not executed
                assert_eq!(arr.len(), 2);
                // First result is success
                assert!(arr[0]["result"].is_object());
                // Second result is error
                assert!(arr[1]["error"].is_object());
                assert!(
                    arr[1]["error"]["code"]
                        .as_str()
                        .unwrap()
                        .starts_with("KIP_1")
                );
            }
            _ => panic!("Expected Ok response wrapping batch results"),
        }
    }

    #[tokio::test]
    async fn test_execute_batch_stops_on_executor_error() {
        let executor = FailingExecutor;
        let request = Request {
            commands: vec![
                CommandItem::Simple(r#"FIND(?t) WHERE { ?t {type: "$ConceptType"} }"#.to_string()),
                CommandItem::Simple("DESCRIBE PRIMER".to_string()),
            ],
            ..Default::default()
        };

        let (_cmd_type, response) = request.execute(&executor).await;
        match response {
            Response::Ok { result, .. } => {
                let arr = result.as_array().unwrap();
                // First command fails, stops immediately
                assert_eq!(arr.len(), 1);
                assert!(arr[0]["error"].is_object());
                assert_eq!(arr[0]["error"]["code"], "KIP_3001");
            }
            _ => panic!("Expected Ok response wrapping batch results"),
        }
    }

    #[tokio::test]
    async fn test_execute_batch_mixed_command_types() {
        let executor = MockExecutor;
        let request = Request {
            commands: vec![
                CommandItem::Simple("DESCRIBE PRIMER".to_string()),
                CommandItem::Simple(r#"FIND(?d) WHERE { ?d {type: "Drug"} } LIMIT 5"#.to_string()),
                CommandItem::Simple(
                    r#"UPSERT { CONCEPT ?d { {type: "Drug", name: "NewDrug"} } }"#.to_string(),
                ),
            ],
            ..Default::default()
        };

        let (cmd_type, response) = request.execute(&executor).await;
        // KML takes precedence once encountered
        assert_eq!(cmd_type, CommandType::Kml);
        match response {
            Response::Ok { result, .. } => {
                let arr = result.as_array().unwrap();
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0]["result"]["type"], "meta");
                assert_eq!(arr[1]["result"]["type"], "kql");
                assert_eq!(arr[2]["result"]["type"], "kml");
            }
            _ => panic!("Expected Ok response"),
        }
    }

    #[tokio::test]
    async fn test_execute_batch_empty_commands() {
        let executor = MockExecutor;
        let request = Request {
            commands: vec![],
            ..Default::default()
        };

        // Empty commands means is_batch() is false, falls to single command mode
        assert!(!request.is_batch());
        let (cmd_type, response) = request.execute(&executor).await;
        // Empty command string is a syntax error
        assert_eq!(cmd_type, CommandType::Unknown);
        assert!(matches!(response, Response::Err { .. }));
    }

    #[tokio::test]
    async fn test_execute_batch_syntax_error_with_placeholder_hint() {
        let executor = MockExecutor;
        let mut params = Map::new();
        params.insert("name".to_string(), Json::String("test".to_string()));

        let request = Request {
            commands: vec![
                CommandItem::Simple("DESCRIBE PRIMER".to_string()),
                CommandItem::WithParams {
                    command: r#"FIND(?x) WHERE { ?x {name: "Hello :name"} }"#.to_string(),
                    parameters: params,
                },
            ],
            ..Default::default()
        };

        let (_cmd_type, response) = request.execute(&executor).await;
        match response {
            Response::Ok { result, .. } => {
                let arr = result.as_array().unwrap();
                assert_eq!(arr.len(), 2);
                // First succeeds
                assert!(arr[0]["result"].is_object());
                // Second has error with placeholder hint
                let error = &arr[1]["error"];
                assert!(error["code"].as_str().unwrap().starts_with("KIP_1"));
                assert!(error["hint"].as_str().unwrap().contains("name"));
            }
            _ => panic!("Expected Ok response wrapping batch results"),
        }
    }
}
