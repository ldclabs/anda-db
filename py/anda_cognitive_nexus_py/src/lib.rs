#![allow(non_local_definitions)]

use anda_cognitive_nexus::{nexus::DEFAULT_SPACE, profiles::COGNITIVE_MEMORY, CognitiveNexus};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::executor::Executor;
use anda_kip::{
    execute_request, parse_kip, CommandType, Json, KipError, Map, Number, Request, RequestOptions,
    Response,
};
use anda_object_store::MetaStoreBuilder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyList, PyLong, PyString, PyTuple};
use serde::{Deserialize, Serialize};
use serde_pyobject::to_pyobject;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// This is a simple example of exposing a Rust function to Python using PyO3.
///
/// # Python Example
/// sum_as_string(2, 3)  # returns '5'
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

/// Python-facing wrapper for the Rust CognitiveNexus.
///
/// Exposed as a Python class. Use `PyAndaDB.create(db_config)` to construct from Python.
#[pyclass]
pub struct PyAndaDB {
    nexus: Arc<CognitiveNexus>,
    closed: AtomicBool,
}

/// Python-facing wrapper for the Rust CommandType enum.
///
/// Exposed as a Python class, not a true Python enum.Enum. Use PyCommandType.Kml, etc.
#[pyclass]
#[derive(Clone)]
pub enum PyCommandType {
    Kml,
    Kql,
    Meta,
    Unknown,
}

impl From<CommandType> for PyCommandType {
    fn from(cmd: CommandType) -> Self {
        match cmd {
            CommandType::Kml => PyCommandType::Kml,
            CommandType::Kql => PyCommandType::Kql,
            CommandType::Meta => PyCommandType::Meta,
            _ => PyCommandType::Unknown,
        }
    }
}

impl From<&str> for PyCommandType {
    fn from(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "kml" => PyCommandType::Kml,
            "kql" => PyCommandType::Kql,
            "meta" => PyCommandType::Meta,
            _ => PyCommandType::Unknown,
        }
    }
}

#[pymethods]
impl PyCommandType {
    /// Parse a command type name (case-insensitive). Unrecognized names map to Unknown.
    // `from_str` is the Python-facing method name; it is not the std trait.
    #[allow(clippy::should_implement_trait)]
    #[staticmethod]
    pub fn from_str(s: &str) -> Self {
        PyCommandType::from(s)
    }
}

#[pymethods]
impl PyAndaDB {
    #[staticmethod]
    #[pyo3(text_signature = "(db_config: AndaDbConfig) -> Awaitable[PyAndaDB]")]
    /// Create a new AndaDB instance from a Python AndaDbConfig object.
    ///
    /// Args:
    ///     db_config (AndaDbConfig): Database configuration as a Python class (see AndaDbConfig).
    ///
    /// Returns:
    ///     Awaitable[PyAndaDB]: An awaitable AndaDB instance.
    ///
    /// Raises:
    ///     RuntimeError: If config deserialization or DB creation fails.
    pub fn create<'py>(py: Python<'py>, db_config: AndaDbConfig) -> PyResult<&'py PyAny> {
        log::debug!("AndaDB.create called: db_config={:?}", db_config);
        let fut = async move {
            match create_kip_db(db_config).await {
                Ok(nexus) => Ok(PyAndaDB {
                    nexus,
                    closed: AtomicBool::new(false),
                }),
                Err(e) => Err(PyRuntimeError::new_err(format!("DB creation error: {}", e))),
            }
        };
        pyo3_asyncio::tokio::future_into_py(py, fut)
    }

    #[pyo3(signature = (command, dry_run = false, parameters = None))]
    #[pyo3(
        text_signature = "(command: str, dry_run: bool = False, parameters: dict = None) -> Awaitable[Dict[str, Any]]"
    )]
    /// Execute a KIP command asynchronously.
    ///
    /// Args:
    ///     command (str): KIP command string (KML/KQL/META).
    ///     dry_run (bool, optional): If True, performs a dry run. Defaults to False.
    ///     parameters (dict, optional): Command parameters. Defaults to None.
    ///
    /// Returns:
    ///     Awaitable[Dict[str, Any]]: Awaitable Python dictionary with:
    ///         - "type" (PyCommandType): The type of the executed command (Python class, not enum.Enum).
    ///         - "response" (dict): The command response as a native Python dictionary
    ///           (converted from the underlying `serde_json::Value` using serde-pyobject).
    ///
    /// Raises:
    ///     ValueError: If `parameters` is not a JSON-compatible dict.
    ///     RuntimeError: If KIP execution fails.
    pub fn execute_kip<'py>(
        &self,
        py: Python<'py>,
        command: String,
        dry_run: bool,
        parameters: Option<&PyDict>,
    ) -> PyResult<&'py PyAny> {
        log::debug!(
            "AndaDB.execute_kip called: dry_run={}, command={}",
            dry_run,
            command
        );

        // Convert Python dict -> Map<String, Json> directly, without a JSON string
        // round-trip. Conversion failures surface as Python exceptions; they must
        // never panic, as release builds abort on panic.
        let params_map: Map<String, Json> = match parameters {
            Some(dict) => pydict_to_json_map(dict, 0)?,
            None => Map::new(),
        };

        let nexus = self.nexus.clone();

        // Async future that returns a PyObject (a Python dict)
        let fut = async move {
            let (cmd_type, response) =
                execute_kip(nexus.as_ref(), command, Some(params_map), dry_run).await;
            // Convert both the cmd_type and the response into Python objects while holding the GIL
            let py_obj: PyObject = Python::with_gil(|py| -> PyResult<PyObject> {
                // 1) Wrap cmd_type into the Python-visible class
                let py_cmd_wrapper = Py::new(py, PyCommandType::from(cmd_type))?;

                // 2) Convert response (serde-serializable) into a Python object using serde-pyobject
                let py_response = to_pyobject(py, &response).map_err(|e| {
                    PyRuntimeError::new_err(format!("Response conversion error: {}", e))
                })?;

                // 3) Build the resulting Python dict {"type": <PyCommandType>, "response": <py_response>}
                let out_dict = PyDict::new(py);
                out_dict.set_item("type", py_cmd_wrapper.as_ref(py))?;
                out_dict.set_item("response", py_response)?;

                Ok(out_dict.into())
            })?;

            Ok(py_obj)
        };

        // Convert the Rust Future -> Python awaitable
        pyo3_asyncio::tokio::future_into_py(py, fut)
    }

    #[pyo3(text_signature = "() -> Awaitable[None]")]
    /// Close the database, flushing all pending data to storage.
    ///
    /// Call this before the process exits when using a file-backed store,
    /// otherwise buffered data may be lost. Calling it more than once is a no-op.
    /// After closing, `execute_kip` KML commands will fail with a read-only error.
    ///
    /// Returns:
    ///     Awaitable[None]
    ///
    /// Raises:
    ///     RuntimeError: If closing the database fails.
    pub fn close<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let already_closed = self.closed.swap(true, Ordering::SeqCst);
        let nexus = self.nexus.clone();
        let fut = async move {
            if already_closed {
                return Ok(());
            }
            nexus
                .close()
                .await
                .map_err(|e| PyRuntimeError::new_err(format!("DB close error: {}", e)))
        };
        pyo3_asyncio::tokio::future_into_py(py, fut)
    }
}

/// Exposes the Rust AndaDbConfig struct as a Python class.
///
/// All fields are accessible and mutable from Python. Construct directly in Python and pass to PyAndaDB.create.
///
/// Example:
///     config = AndaDbConfig(
///         store_location_type=StoreLocationType.InMem,
///         store_location="",
///         db_name="test_db",
///         db_desc="Test database",
///         meta_cache_capacity=10000
///     )
#[pyclass]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AndaDbConfig {
    #[pyo3(get, set)]
    pub store_location_type: StoreLocationType,
    #[pyo3(get, set)]
    pub store_location: String,
    #[pyo3(get, set)]
    pub db_name: String,
    #[pyo3(get, set)]
    pub db_desc: Option<String>,
    #[pyo3(get, set)]
    pub meta_cache_capacity: Option<u64>,
}

#[pymethods]
impl AndaDbConfig {
    #[new]
    pub fn new(
        store_location_type: StoreLocationType,
        store_location: String,
        db_name: String,
        db_desc: Option<String>,
        meta_cache_capacity: Option<u64>,
    ) -> PyResult<Self> {
        Ok(AndaDbConfig {
            store_location_type,
            store_location,
            db_name,
            db_desc,
            meta_cache_capacity,
        })
    }
}

impl AndaDbConfig {
    /// Verifies the configuration for AndaDbConfig.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `store_location_type` is `LocalFile` and `store_location` is empty.
    /// - `store_location` does not exist on the filesystem.
    pub fn verify_config(&self) -> Result<(), String> {
        if let StoreLocationType::LocalFile = self.store_location_type {
            if self.store_location.trim().is_empty() {
                return Err(
                    "store_location is required when store_location_type is LocalFile".to_string(),
                );
            }
            use std::path::Path;
            if !Path::new(&self.store_location).exists() {
                return Err(format!(
                    "store_location path does not exist: {}",
                    self.store_location
                ));
            }
        }
        Ok(())
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn anda_cognitive_nexus_py(_py: Python, m: &PyModule) -> PyResult<()> {
    // The host process (or another Rust-backed extension) may have already
    // installed a global logger; that must not make `import` fail or panic.
    let _ = structured_logger::Builder::new().try_init();
    m.add_class::<PyAndaDB>()?;
    m.add_class::<PyCommandType>()?;
    m.add_class::<StoreLocationType>()?;
    m.add_class::<AndaDbConfig>()?;
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}

/// Exposes the Rust StoreLocationType enum as a Python class.
///
/// Use StoreLocationType.InMem and StoreLocationType.LocalFile in Python configs.
#[pyclass]
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum StoreLocationType {
    InMem,
    LocalFile,
}

#[pymethods]
impl StoreLocationType {
    /// str(self) -> "in_mem" or "local_file"
    fn __str__(&self) -> &'static str {
        match self {
            StoreLocationType::InMem => "in_mem",
            StoreLocationType::LocalFile => "local_file",
        }
    }
}

impl TryFrom<&str> for StoreLocationType {
    type Error = PyErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let s: String = value.to_string();

        if StoreLocationType::InMem.__str__() == s {
            Ok(StoreLocationType::InMem)
        } else if StoreLocationType::LocalFile.__str__() == s {
            Ok(StoreLocationType::LocalFile)
        } else {
            Err(PyValueError::new_err(format!(
                "Invalid StoreLocationType: {}",
                s
            )))
        }
    }
}

/// Maximum nesting depth accepted for KIP parameter values, guarding against
/// stack exhaustion from deeply nested Python structures.
const MAX_PARAMS_DEPTH: usize = 128;

/// Converts a Python dict into KIP command parameters.
///
/// Keys must be strings; values must be JSON-compatible (str, bool, int,
/// float, None, list, tuple, dict). Anything else raises `ValueError`.
fn pydict_to_json_map(dict: &PyDict, depth: usize) -> PyResult<Map<String, Json>> {
    let mut map = Map::new();
    for (key, value) in dict.iter() {
        let key = key.downcast::<PyString>().map_err(|_| {
            PyValueError::new_err(format!(
                "parameter keys must be strings, got: {}",
                key.get_type().name().unwrap_or("<unknown>")
            ))
        })?;
        map.insert(key.to_str()?.to_owned(), py_to_json(value, depth + 1)?);
    }
    Ok(map)
}

/// Converts a single Python value into JSON, raising `ValueError` for values
/// that have no JSON equivalent. Must never panic: release builds abort on
/// panic, which would take down the host Python interpreter.
fn py_to_json(value: &PyAny, depth: usize) -> PyResult<Json> {
    if depth > MAX_PARAMS_DEPTH {
        return Err(PyValueError::new_err(format!(
            "parameters nested deeper than {} levels",
            MAX_PARAMS_DEPTH
        )));
    }
    if value.is_none() {
        return Ok(Json::Null);
    }
    // PyBool must be checked before PyLong: bool is a subclass of int in Python.
    if let Ok(v) = value.downcast::<PyBool>() {
        return Ok(Json::Bool(v.is_true()));
    }
    if value.downcast::<PyLong>().is_ok() {
        if let Ok(v) = value.extract::<i64>() {
            return Ok(Json::from(v));
        }
        if let Ok(v) = value.extract::<u64>() {
            return Ok(Json::from(v));
        }
        return Err(PyValueError::new_err(
            "integer parameter out of JSON number range",
        ));
    }
    if let Ok(v) = value.downcast::<PyFloat>() {
        return Number::from_f64(v.value())
            .map(Json::Number)
            .ok_or_else(|| {
                PyValueError::new_err("non-finite float parameter is not JSON-compatible")
            });
    }
    if let Ok(v) = value.downcast::<PyString>() {
        return Ok(Json::String(v.to_str()?.to_owned()));
    }
    if let Ok(list) = value.downcast::<PyList>() {
        let mut arr = Vec::with_capacity(list.len());
        for item in list.iter() {
            arr.push(py_to_json(item, depth + 1)?);
        }
        return Ok(Json::Array(arr));
    }
    if let Ok(tuple) = value.downcast::<PyTuple>() {
        let mut arr = Vec::with_capacity(tuple.len());
        for item in tuple.iter() {
            arr.push(py_to_json(item, depth + 1)?);
        }
        return Ok(Json::Array(arr));
    }
    if let Ok(nested) = value.downcast::<PyDict>() {
        return Ok(Json::Object(pydict_to_json_map(nested, depth)?));
    }
    Err(PyValueError::new_err(format!(
        "unsupported parameter type: {}",
        value.get_type().name().unwrap_or("<unknown>")
    )))
}

/// Create a CognitiveNexus instance from AndaDbConfig.
/// Returns an Arc-wrapped Nexus for use in KIP execution.
/// * `db_config` - Database configuration as an `AndaDbConfig` struct.
///     - `store_location_type`: `"InMem"` for in-memory DB, `"LocalFile"` for file-backed DB.
///     - `store_location`: Required if `store_location_type` is `"LocalFile"`.
///     - `DB_name`: Name of the database.
///     - `DB_desc`: Optional description of the database.
///     - `meta_cache_capacity`: Optional cache capacity for metadata (default: 10000).
///
///
/// # Errors
/// Returns an error if the config is invalid or DB/Nexus creation fails.
pub async fn create_kip_db(db_config: AndaDbConfig) -> Result<Arc<CognitiveNexus>, BoxError> {
    db_config
        .verify_config()
        .map_err(KipError::internal_error)?;

    let db_name = db_config.db_name.as_str();
    let db_desc = db_config.db_desc.as_deref().unwrap_or_default();
    let meta_cache_capacity = db_config.meta_cache_capacity.unwrap_or(10000);

    let object_store: Arc<dyn object_store::ObjectStore> = match db_config.store_location_type {
        StoreLocationType::InMem => Arc::new(InMemory::new()),
        StoreLocationType::LocalFile => {
            let local_file = MetaStoreBuilder::new(
                LocalFileSystem::new_with_prefix(&db_config.store_location)
                    .map_err(|err| KipError::internal_error(err.to_string()))?,
                meta_cache_capacity,
            )
            .build();
            Arc::new(local_file)
        }
    };

    let db_config = DBConfig {
        name: db_name.to_string(),
        description: db_desc.to_string(),
        ..Default::default()
    };

    let db = Arc::new(AndaDB::connect(object_store, db_config).await?);
    let nexus = CognitiveNexus::connect(db).await?;
    // A Space that has activated no Schema Package resolves the Core package
    // only, and Core declares no Concept types — so without this the binding
    // would open cleanly and then refuse every `CREATE CONCEPT` sent to it.
    // Activation is skipped when the same lock is already in force, so
    // re-opening a file-backed database does not mint an environment version.
    nexus
        .install_and_activate(&[("bundled", COGNITIVE_MEMORY)], DEFAULT_SPACE)
        .await?;
    Ok(Arc::new(nexus))
}

/// Executes one KIP command using an existing Executor instance.
///
/// The command is wrapped in a single-operation KIP 2.0 request envelope
/// (§71). Parameters are bound structurally, as request-level bindings a
/// command cites as `:name` — they are data, never text spliced into the
/// command (§74, §88.2).
///
/// # Arguments
///
/// * `nexus` - Reference to an Executor instance (`&(impl Executor + Sync)`).
/// * `command` - The KIP command string to execute (KML/KQL/META).
/// * `parameters` - An optional map of command parameters (`Option<Map<String, Json>>`). If `None`, treated as empty.
/// * `dry_run` - If true, validates without establishing a durable commit.
///
/// # Returns
///
/// The command family and the response. A failed execution is a `Response`
/// carrying an error object, not an `Err`: a KIP failure is an answer with a
/// registered code, hint and retry class, and flattening it into a string
/// would throw away everything a caller needs to recover.
///
/// # Example
///
/// Refer to the `examples` directory.
pub async fn execute_kip(
    nexus: &impl Executor,
    command: String,
    parameters: Option<Map<String, Json>>,
    dry_run: bool,
) -> (CommandType, Response) {
    // Classified from the parsed command, never from a declared label (§73.1).
    let language = parse_kip(&command).map_or(CommandType::Unknown, |c| CommandType::from(&c));

    let request = Request {
        parameters: parameters.filter(|p| !p.is_empty()),
        options: Some(RequestOptions {
            dry_run: Some(dry_run),
            ..Default::default()
        }),
        ..Request::single(command)
    };

    (language, execute_request(nexus, &request).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anda_kip::{Json, Map, TopLevelStatus};
    use std::future::Future;

    // Helper to run async code in tests
    fn block_on<F: Future<Output = T>, T>(fut: F) -> T {
        tokio::runtime::Runtime::new().unwrap().block_on(fut)
    }

    /// Records what somebody prefers, the way KIP 2.0 records anything: the
    /// Concepts exist, a Proposition states the tuple without claiming it, and
    /// an Assertion is what commits to it.
    ///
    /// The types come from the bundled cognitive-memory profile. There is no
    /// `$ConceptType` node to create first — in 2.0 authoritative Schema is an
    /// immutable package artifact, not graph state, so a KML statement cannot
    /// invent a type on its way to using it.
    static RECORD_A_PREFERENCE: &str = r#"
        MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark mode" }
            CREATE EVIDENCE ?said {
                SET FIELDS {
                    evidence_class: "user_statement",
                    payload: "I prefer dark mode."
                }
            }
            ASSERT ?a (?alice, "prefers", ?dark) {
                by: ?alice, mode: "stated", confidence: 0.9, evidence: ?said
            }
        }
        "#;

    /// A second, independent claim, so the read below has more than one row to
    /// order — and a second confidence to order them by.
    static RECORD_ANOTHER_PREFERENCE: &str = r#"
        MUTATE {
            CREATE CONCEPT ?bob { TYPE "Person" NAME "Bob" }
            CREATE CONCEPT ?light { TYPE "Preference" NAME "Light mode" }
            CREATE EVIDENCE ?said {
                SET FIELDS {
                    evidence_class: "user_statement",
                    payload: "Light mode, please."
                }
            }
            ASSERT ?a (?bob, "prefers", ?light) {
                by: ?bob, mode: "stated", confidence: 0.6, evidence: ?said
            }
        }
        "#;

    fn run(nexus: &CognitiveNexus, command: &str) -> Response {
        let (_, response) = block_on(execute_kip(nexus, command.to_string(), None, false));
        response
    }

    #[test]
    fn test_execute_kip_in_mem() {
        let db_config_in_mem = AndaDbConfig {
            store_location_type: StoreLocationType::InMem,
            store_location: "".to_owned(),
            db_name: "test_preferences_db".to_string(),
            db_desc: Some("Ephemeral DB for the KIP binding test".to_string()),
            meta_cache_capacity: Some(10000),
        };
        let nexus = block_on(create_kip_db(db_config_in_mem)).expect("Failed to create Nexus");

        for kml in [RECORD_A_PREFERENCE, RECORD_ANOTHER_PREFERENCE] {
            let response = run(nexus.as_ref(), kml);
            assert_eq!(
                response.status,
                TopLevelStatus::Succeeded,
                "KML failed: {:#?}",
                response.results
            );
        }

        // Read the Assertions back. This asks who claimed what with how much
        // confidence — not what is true: belief is projected from Assertions
        // under a policy, and a raw read must not be mistaken for one.
        let query = r#"
        FIND(?person.name, ?a.confidence)
        WHERE {
            ?person CONCEPT {type: "Person"}
            ?p PROPOSITION (?person, "prefers", ?pref)
            ?a ASSERTION {proposition: ?p}
        }
        ORDER BY ?a.confidence DESC
        "#;
        let (language, query_response) =
            block_on(execute_kip(nexus.as_ref(), query.to_string(), None, false));
        assert!(matches!(language, CommandType::Kql));
        assert_eq!(
            query_response.status,
            TopLevelStatus::Succeeded,
            "KQL failed: {:#?}",
            query_response.results
        );

        let result = query_response
            .first_result()
            .expect("the read returns a result");
        let rows = result
            .as_array()
            .unwrap_or_else(|| panic!("unexpected result shape: {result:#}"));
        // A row of a multi-variable projection is an array, in FIND order.
        // ORDER BY confidence DESC puts Alice's 0.9 before Bob's 0.6.
        assert_eq!(
            rows,
            &vec![
                Json::from(vec![Json::from("Alice"), Json::from(0.9)]),
                Json::from(vec![Json::from("Bob"), Json::from(0.6)]),
            ],
            "unexpected rows: {result:#}"
        );
    }

    /// A dry run validates without committing (§69.3). The Concept it would
    /// have created must not be readable afterwards.
    #[test]
    fn a_dry_run_leaves_nothing_behind() {
        let nexus = block_on(create_kip_db(AndaDbConfig {
            store_location_type: StoreLocationType::InMem,
            store_location: "".to_owned(),
            db_name: "test_dry_run_db".to_string(),
            db_desc: None,
            meta_cache_capacity: Some(10000),
        }))
        .expect("Failed to create Nexus");

        let command = r#"CREATE CONCEPT ?c { TYPE "Person" NAME "Ghost" }"#.to_string();
        let (_, response) = block_on(execute_kip(nexus.as_ref(), command, None, true));
        assert_eq!(
            response.status,
            TopLevelStatus::Succeeded,
            "dry run failed: {:#?}",
            response.results
        );

        let read = run(
            nexus.as_ref(),
            r#"FIND(?c.name) WHERE { ?c CONCEPT {type: "Person", name: "Ghost"} }"#,
        );
        let rows = read
            .first_result()
            .and_then(|result| result.as_array().cloned())
            .unwrap_or_default();
        assert!(rows.is_empty(), "a dry run must not commit: {rows:#?}");
    }

    /// Parameters are bound as data, not spliced into the command text.
    #[test]
    fn parameters_are_bound_structurally() {
        let nexus = block_on(create_kip_db(AndaDbConfig {
            store_location_type: StoreLocationType::InMem,
            store_location: "".to_owned(),
            db_name: "test_params_db".to_string(),
            db_desc: None,
            meta_cache_capacity: Some(10000),
        }))
        .expect("Failed to create Nexus");

        let response = run(
            nexus.as_ref(),
            r#"CREATE CONCEPT ?c { TYPE "Person" NAME "Carol" }"#,
        );
        assert_eq!(response.status, TopLevelStatus::Succeeded);

        let mut parameters: Map<String, Json> = Map::new();
        parameters.insert("who".to_string(), Json::from("Carol"));
        let (_, response) = block_on(execute_kip(
            nexus.as_ref(),
            r#"FIND(?c.name) WHERE { ?c CONCEPT {type: "Person", name: :who} }"#.to_string(),
            Some(parameters),
            false,
        ));
        assert_eq!(
            response.status,
            TopLevelStatus::Succeeded,
            "parameterized read failed: {:#?}",
            response.results
        );
        let rows = response
            .first_result()
            .and_then(|result| result.as_array().cloned())
            .unwrap_or_default();
        assert_eq!(rows, vec![Json::from("Carol")], "{rows:#?}");
    }
}
