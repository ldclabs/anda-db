#![allow(non_local_definitions)]

use anda_cognitive_nexus::{nexus::DEFAULT_SPACE, profiles::COGNITIVE_MEMORY, CognitiveNexus};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::executor::Executor;
use anda_kip::{
    execute_request, execute_request_readonly, parse_kip, Capsule, CommandType, Json, KipError,
    Map, Number, Request, RequestOptions, Response, KIP_VERSION,
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

/// The KIP protocol version this binding speaks.
///
/// It is the value a request envelope's `kip` member must carry: a request
/// declaring anything else is refused with `UnsupportedProtocolVersion` rather
/// than interpreted under this version's rules.
///
/// What the *engine* behind the binding supports is a separate question with a
/// separate answer — ask it with `DESCRIBE CAPABILITIES`, which reports the
/// conformance profiles and capability names as structured data (§67, §89).
///
/// # Python Example
/// kip_version()  # returns '2.0'
#[pyfunction]
fn kip_version() -> &'static str {
    KIP_VERSION
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
    /// Execute one KIP command asynchronously.
    ///
    /// The command is wrapped in a single-operation KIP 2.0 request envelope
    /// (§71). Use `execute_request` for everything an envelope can express that
    /// one command cannot — a named MemorySpace, several operations, an
    /// idempotency key, preconditions, ingested Evidence, a deadline — and
    /// `execute_kip_readonly` when the command must not be allowed to write.
    ///
    /// Args:
    ///     command (str): KIP command string (KML/KQL/META).
    ///     dry_run (bool, optional): If True, validates without establishing a
    ///         durable commit (§69.3). Defaults to False.
    ///     parameters (dict, optional): Request-level parameter bindings the
    ///         command cites as `:name`. They occupy complete value positions
    ///         and are never spliced into the command text (§74, §88.2).
    ///         Defaults to None.
    ///
    /// Returns:
    ///     Awaitable[Dict[str, Any]]: Awaitable Python dictionary with:
    ///         - "type" (PyCommandType): The family the command was classified
    ///           as by parsing it — the only classification that means anything
    ///           for a security decision (§73.1). A command that does not parse
    ///           is `Unknown`.
    ///         - "response" (dict): The KIP 2.0 response envelope (§81).
    ///
    /// Note:
    ///     A failed command is a response carrying an error, not an exception:
    ///     a KIP failure has a registered code, a hint and a retry class, and
    ///     flattening it into a Python exception message would throw away
    ///     everything a caller needs to recover.
    ///
    /// Raises:
    ///     ValueError: If `parameters` is not a JSON-compatible dict.
    ///     RuntimeError: If the response cannot be converted to Python objects.
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
            // Convert both the cmd_type and the response into Python objects
            // while holding the GIL.
            Python::with_gil(|py| command_response_dict(py, cmd_type, &response))
        };

        // Convert the Rust Future -> Python awaitable
        pyo3_asyncio::tokio::future_into_py(py, fut)
    }

    #[pyo3(signature = (command, parameters = None))]
    #[pyo3(text_signature = "(command: str, parameters: dict = None) -> Awaitable[Dict[str, Any]]")]
    /// Execute a KIP command on the read-only path (§76).
    ///
    /// Accepts KQL and META — including `VERIFY`, `VALIDATE`, `PREVIEW`,
    /// `HISTORY`, `CHANGES` and `EXPORT CAPSULE` — and refuses anything that
    /// changes state with a `ReadonlyViolation` error. The refusal is decided
    /// on what the command parses as, not on any label attached to it, so this
    /// boundary holds even for a command an untrusted prompt composed
    /// (§73.1, §88.3).
    ///
    /// Args:
    ///     command (str): KIP command string (KQL/META).
    ///     parameters (dict, optional): Request-level parameter bindings the
    ///         command cites as `:name`. Data, never command text (§74).
    ///
    /// Returns:
    ///     Awaitable[Dict[str, Any]]: The same `{"type", "response"}` shape as
    ///         `execute_kip`.
    ///
    /// Raises:
    ///     ValueError: If `parameters` is not a JSON-compatible dict.
    ///     RuntimeError: If the response cannot be converted to Python objects.
    pub fn execute_kip_readonly<'py>(
        &self,
        py: Python<'py>,
        command: String,
        parameters: Option<&PyDict>,
    ) -> PyResult<&'py PyAny> {
        log::debug!("AndaDB.execute_kip_readonly called: command={}", command);

        let params_map: Map<String, Json> = match parameters {
            Some(dict) => pydict_to_json_map(dict, 0)?,
            None => Map::new(),
        };
        let nexus = self.nexus.clone();

        let fut = async move {
            let (cmd_type, response) =
                execute_kip_readonly(nexus.as_ref(), command, Some(params_map)).await;
            Python::with_gil(|py| command_response_dict(py, cmd_type, &response))
        };
        pyo3_asyncio::tokio::future_into_py(py, fut)
    }

    #[pyo3(text_signature = "(request: dict) -> Awaitable[Dict[str, Any]]")]
    /// Execute a complete KIP 2.0 request envelope (§71).
    ///
    /// This is the full protocol surface, and the shape the HTTP server speaks:
    /// everything `execute_kip` cannot express lives here — the MemorySpace to
    /// run against (§5.5, never inferred), several `operations` under an
    /// `execution.mode` of `independent` or `sequence` (§75), an
    /// `idempotency_key` that makes a write retry-safe (§34), `preconditions`
    /// (§35.4), `requires` capability preconditions (§67), an `ingest` block
    /// that mints Evidence from the transport envelope instead of from
    /// model-written command text (§71.1, §88.12), and `options.deadline_ms`.
    ///
    /// Args:
    ///     request (dict): The envelope. `kip` must be `"2.0"` and
    ///         `operations` must hold at least one operation; unknown members
    ///         are rejected rather than ignored.
    ///
    /// Returns:
    ///     Awaitable[Dict[str, Any]]: The response envelope (§81) — `status`,
    ///         one `results[]` entry per operation in request order, and
    ///         `receipt` / `snapshot` / `warnings` where they apply. Unlike
    ///         `execute_kip` this is the bare envelope, with no `"type"`
    ///         alongside it: a request whose operations are a read and a write
    ///         has no single language, and `results[]` correlates by `op_id`.
    ///
    /// Raises:
    ///     ValueError: If `request` contains values with no JSON equivalent.
    ///     RuntimeError: If the response cannot be converted to Python objects.
    ///
    /// Note:
    ///     `execution.mode: "atomic"` is refused with `UnsupportedCapability`
    ///     rather than approximated: one transaction, one snapshot and
    ///     all-or-none commit are properties this runner cannot provide by
    ///     running operations one at a time, and faking them would tell a
    ///     caller their writes were atomic when they were not (§75.4).
    pub fn execute_request<'py>(&self, py: Python<'py>, request: &PyDict) -> PyResult<&'py PyAny> {
        log::debug!("AndaDB.execute_request called: request={}", request);
        let envelope = Json::Object(pydict_to_json_map(request, 0)?);
        let nexus = self.nexus.clone();

        let fut = async move {
            let response = run_envelope(nexus.as_ref(), envelope, Endpoint::StateCapable).await;
            Python::with_gil(|py| response_object(py, &response))
        };
        pyo3_asyncio::tokio::future_into_py(py, fut)
    }

    #[pyo3(text_signature = "(request: dict) -> Awaitable[Dict[str, Any]]")]
    /// Execute a complete KIP 2.0 request envelope on the read-only path (§76).
    ///
    /// The envelope counterpart of `execute_kip_readonly`. Every operation is
    /// classified before any of them runs, and one state-changing operation
    /// fails the whole request with `ReadonlyViolation` — the reads beside it
    /// are not served either, so a caller cannot mistake a half-served request
    /// for a served one.
    ///
    /// Args:
    ///     request (dict): The envelope, as for `execute_request`.
    ///
    /// Returns:
    ///     Awaitable[Dict[str, Any]]: The response envelope (§81).
    ///
    /// Raises:
    ///     ValueError: If `request` contains values with no JSON equivalent.
    ///     RuntimeError: If the response cannot be converted to Python objects.
    pub fn execute_request_readonly<'py>(
        &self,
        py: Python<'py>,
        request: &PyDict,
    ) -> PyResult<&'py PyAny> {
        log::debug!(
            "AndaDB.execute_request_readonly called: request={}",
            request
        );
        let envelope = Json::Object(pydict_to_json_map(request, 0)?);
        let nexus = self.nexus.clone();

        let fut = async move {
            let response = run_envelope(nexus.as_ref(), envelope, Endpoint::Readonly).await;
            Python::with_gil(|py| response_object(py, &response))
        };
        pyo3_asyncio::tokio::future_into_py(py, fut)
    }

    #[pyo3(signature = (capsule, space_id = None, isolate = false))]
    #[pyo3(
        text_signature = "(capsule: dict, space_id: str = None, isolate: bool = False) -> Awaitable[Dict[str, Any]]"
    )]
    /// Import a Cognitive Capsule into a MemorySpace (§39, §41).
    ///
    /// A host operation, deliberately not a KIP command: KML has no import
    /// clause and META is read-only, so the only thing an Agent can do through
    /// the protocol is `PREVIEW IMPORT CAPSULE`. Deciding that this Space
    /// accepts another Brain's cognition is the host's call, and keeping it off
    /// the command surface keeps a prompt from making it.
    ///
    /// Capsule bytes are not destination mutation authority (§37.2). Everything
    /// imported is re-validated against this Space's Schema Environment and
    /// re-authorized under its Governance; source trust, source authority and
    /// source lifecycle standing do not transfer (§39.5, §41.4).
    ///
    /// Args:
    ///     capsule (dict): The Capsule artifact, as `EXPORT CAPSULE` produced it.
    ///     space_id (str, optional): Destination Space. Omitted, it is the
    ///         Space this database was opened with — a host's own configured
    ///         default, which is not the conversational inference §5.5
    ///         forbids.
    ///     isolate (bool, optional): Import into quarantine for review rather
    ///         than into ordinary Recall state (§39.2). Quarantine holds
    ///         cognition out of use without claiming its author took it back.
    ///
    /// Returns:
    ///     Awaitable[Dict[str, Any]]: `{"imported", "counts", "identity_map",
    ///         "warnings"}` — the source-to-destination identity map is what
    ///         makes a re-import of the same artifact idempotent.
    ///
    /// Raises:
    ///     ValueError: If `capsule` contains values with no JSON equivalent.
    ///     RuntimeError: If the artifact is not a readable Capsule, or the
    ///         import is refused.
    pub fn import_capsule<'py>(
        &self,
        py: Python<'py>,
        capsule: &PyDict,
        space_id: Option<String>,
        isolate: bool,
    ) -> PyResult<&'py PyAny> {
        let artifact = Json::Object(pydict_to_json_map(capsule, 0)?);
        let nexus = self.nexus.clone();

        let fut = async move {
            let space_id = space_id.unwrap_or_else(|| DEFAULT_SPACE.to_string());
            let report = import_capsule_json(nexus.as_ref(), artifact, &space_id, isolate)
                .await
                .map_err(|err| PyRuntimeError::new_err(format!("capsule import failed: {err}")))?;
            Python::with_gil(|py| json_object(py, &report))
        };
        pyo3_asyncio::tokio::future_into_py(py, fut)
    }

    #[pyo3(signature = (artifacts, space_id = None))]
    #[pyo3(
        text_signature = "(artifacts: List[str], space_id: str = None) -> Awaitable[Dict[str, Any]]"
    )]
    /// Install Schema Package artifacts and put exactly those in force (§20).
    ///
    /// A protected Schema operation, not a KIP command: §20.10 keeps package
    /// installation and activation out of ordinary KML entirely, so a write
    /// cannot change what a type means on its way to using it.
    ///
    /// The resulting Schema Lock names **exactly** the packages given, so a
    /// host that still wants the baseline ontology includes
    /// `COGNITIVE_MEMORY_PROFILE` in the list; leaving it out deactivates it
    /// (§20.9). Activation is skipped when the same lock is already in force,
    /// because every real activation mints a new Schema Environment version and
    /// invalidates clients pinning the old one (§20.8, §35.4).
    ///
    /// Args:
    ///     artifacts (List[str]): Schema Package artifacts, each the JSON text
    ///         of one package (§20.11).
    ///     space_id (str, optional): The Space to activate in; the default
    ///         Space when omitted.
    ///
    /// Returns:
    ///     Awaitable[Dict[str, Any]]: `{"space_id", "schema_environment_version",
    ///         "lock"}` — the version is what a client pins in `preconditions`,
    ///         and the lock is what it pinned to.
    ///
    /// Raises:
    ///     RuntimeError: If an artifact does not parse, or activation fails.
    pub fn install_schema_packages<'py>(
        &self,
        py: Python<'py>,
        artifacts: Vec<String>,
        space_id: Option<String>,
    ) -> PyResult<&'py PyAny> {
        let nexus = self.nexus.clone();

        let fut = async move {
            let space_id = space_id.unwrap_or_else(|| DEFAULT_SPACE.to_string());
            let sourced: Vec<(&str, &str)> =
                artifacts.iter().map(|a| ("host", a.as_str())).collect();
            let report = activate_schema_packages(nexus.as_ref(), &sourced, &space_id)
                .await
                .map_err(|err| {
                    PyRuntimeError::new_err(format!("schema activation failed: {err}"))
                })?;
            Python::with_gil(|py| json_object(py, &report))
        };
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
    /// The Schema Package artifacts to put in force in the default MemorySpace,
    /// each the JSON text of one package (§20.11).
    ///
    /// `None` activates the bundled Cognitive Memory Profile, which is what a
    /// host wants unless it has an ontology of its own. A list names *exactly*
    /// the packages in force, so a host supplying its own and still wanting the
    /// baseline includes `COGNITIVE_MEMORY_PROFILE` in it (§20.9).
    ///
    /// An empty list activates nothing beyond the Core Package, which declares
    /// no Concept types at all — a Space that can hold Assertions about types
    /// it does not have, and cannot create a Concept. It is a legitimate choice
    /// for a host that installs its schema later; it is not a useful default.
    // Defaulted so a config serialized before this field existed still reads
    // back: `None` is the value that field would have carried.
    #[serde(default)]
    #[pyo3(get, set)]
    pub schema_packages: Option<Vec<String>>,
}

#[pymethods]
impl AndaDbConfig {
    #[new]
    #[pyo3(signature = (
        store_location_type,
        store_location,
        db_name,
        db_desc = None,
        meta_cache_capacity = None,
        schema_packages = None,
    ))]
    pub fn new(
        store_location_type: StoreLocationType,
        store_location: String,
        db_name: String,
        db_desc: Option<String>,
        meta_cache_capacity: Option<u64>,
        schema_packages: Option<Vec<String>>,
    ) -> PyResult<Self> {
        Ok(AndaDbConfig {
            store_location_type,
            store_location,
            db_name,
            db_desc,
            meta_cache_capacity,
            schema_packages,
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
    m.add_function(wrap_pyfunction!(kip_version, m)?)?;
    m.add("KIP_VERSION", KIP_VERSION)?;
    // The bundled baseline ontology, verbatim as the specification publishes
    // it. A host that activates its own Schema Packages passes this alongside
    // them when it still wants `Person`, `Event` and the rest —
    // a Schema Lock names exactly the packages in force, so leaving it out
    // deactivates it (§20.9).
    m.add("COGNITIVE_MEMORY_PROFILE", COGNITIVE_MEMORY)?;
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

/// Converts a JSON value into a Python object.
fn json_object(py: Python<'_>, value: &Json) -> PyResult<PyObject> {
    to_pyobject(py, value)
        .map(|obj| -> PyObject { obj.into() })
        .map_err(|e| PyRuntimeError::new_err(format!("JSON conversion error: {}", e)))
}

/// Converts a KIP response envelope into a Python object.
///
/// The whole envelope crosses, not just the result: `status`, the per-operation
/// `results[]`, the `receipt` a write committed under, the `snapshot` a read ran
/// at and any `warnings` are what a caller needs to tell a served answer from a
/// partial one (§81).
fn response_object(py: Python<'_>, response: &Response) -> PyResult<PyObject> {
    to_pyobject(py, response)
        .map(|obj| obj.into())
        .map_err(|e| PyRuntimeError::new_err(format!("Response conversion error: {}", e)))
}

/// Builds the `{"type", "response"}` dict the single-command entry points
/// return.
///
/// `type` is the family the command was *classified* as by parsing it, which is
/// the only classification that means anything for a security decision (§73.1).
fn command_response_dict(
    py: Python<'_>,
    cmd_type: CommandType,
    response: &Response,
) -> PyResult<PyObject> {
    let py_cmd_wrapper = Py::new(py, PyCommandType::from(cmd_type))?;
    let out_dict = PyDict::new(py);
    out_dict.set_item("type", py_cmd_wrapper.as_ref(py))?;
    out_dict.set_item("response", response_object(py, response)?)?;
    Ok(out_dict.into())
}

/// Maximum nesting depth accepted for any JSON built from Python — parameters,
/// a whole request envelope, a Capsule artifact — guarding against stack
/// exhaustion from deeply nested Python structures.
const MAX_JSON_DEPTH: usize = 128;

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
    if depth > MAX_JSON_DEPTH {
        return Err(PyValueError::new_err(format!(
            "value nested deeper than {} levels",
            MAX_JSON_DEPTH
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
///     - `schema_packages`: Optional Schema Package artifacts to activate in the
///       default Space; `None` activates the bundled Cognitive Memory Profile.
///
///
/// # Errors
/// Returns an error if the config is invalid or DB/Nexus creation fails.
pub async fn create_kip_db(mut db_config: AndaDbConfig) -> Result<Arc<CognitiveNexus>, BoxError> {
    db_config
        .verify_config()
        .map_err(KipError::internal_error)?;

    // Taken, not cloned: the bundled profile alone is 58 KB, a host may pass
    // several, and the config they came from is shadowed and dropped below.
    let schema_packages = db_config.schema_packages.take();
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
    //
    // The bootstrap runs here rather than after connect for that reason: a host
    // that activated the bundled profile and then replaced it with its own on
    // every start would walk the Schema Environment version forward each time,
    // invalidating clients' `preconditions.schema_environment_version` (§20.8).
    let artifacts: Vec<(&str, &str)> = match &schema_packages {
        Some(packages) => packages.iter().map(|p| ("host", p.as_str())).collect(),
        None => vec![("bundled", COGNITIVE_MEMORY)],
    };
    activate_schema_packages(&nexus, &artifacts, DEFAULT_SPACE).await?;
    Ok(Arc::new(nexus))
}

/// Imports a Capsule artifact into a Space (§39, §41).
///
/// A host operation, not a KIP command: KML has no import clause and META is
/// read-only, so the only thing an Agent can do through the protocol is
/// `PREVIEW IMPORT CAPSULE`. Whether this Space accepts another Brain's
/// cognition is the host's decision, and keeping it off the command surface
/// keeps a prompt from making it.
async fn import_capsule_json(
    nexus: &CognitiveNexus,
    artifact: Json,
    space_id: &str,
    isolate: bool,
) -> Result<Json, KipError> {
    let capsule: Capsule = serde_json::from_value(artifact).map_err(|err| {
        KipError::invalid_request_envelope(format!("not a readable Cognitive Capsule: {err}"))
    })?;
    let report = if isolate {
        nexus.import_capsule_isolated(&capsule, space_id).await?
    } else {
        nexus.import_capsule(&capsule, space_id).await?
    };
    Ok(report.to_json(false))
}

/// Installs each `(source, artifact)` and puts exactly those packages in force
/// in a Space.
///
/// A protected Schema operation, not a KIP command: §20.10 keeps package
/// installation and activation out of KML entirely, so an Agent cannot change
/// what a type means on its way to using it. The host decides.
///
/// `source` is recorded on the installed package row and is what `LIST
/// PACKAGES` reports as where an artifact entered, so the profile this binding
/// ships and one the embedding host supplied must not arrive under the same
/// label.
async fn activate_schema_packages(
    nexus: &CognitiveNexus,
    artifacts: &[(&str, &str)],
    space_id: &str,
) -> Result<Json, KipError> {
    let environment = nexus.install_and_activate(artifacts, space_id).await?;
    // The version is what a client pins in `preconditions`, and the lock is
    // what it pinned to — reporting one without the other leaves a caller
    // holding a number it cannot interpret (§20.8, §35.4).
    Ok(serde_json::json!({
        "space_id": space_id,
        "schema_environment_version": environment.version,
        "lock": environment.lock,
    }))
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
    let request = single_operation_request(command, parameters, dry_run);
    (language, execute_request(nexus, &request).await)
}

/// Executes one KIP command on the read-only path (§76).
///
/// Accepts KQL and META — `VERIFY`, `VALIDATE`, `PREVIEW`, `HISTORY`,
/// `CHANGES` and `EXPORT CAPSULE` included — and refuses state-changing
/// semantics with `ReadonlyViolation`.
///
/// The refusal is decided on what the command *parses as*, never on a label a
/// caller attached, so nothing in the envelope can talk a write past this
/// boundary (§73.1, §88.3).
pub async fn execute_kip_readonly(
    nexus: &impl Executor,
    command: String,
    parameters: Option<Map<String, Json>>,
) -> (CommandType, Response) {
    let language = parse_kip(&command).map_or(CommandType::Unknown, |c| CommandType::from(&c));
    // A read commits nothing, so there is no durable effect for a dry run to
    // withhold; `VALIDATE` and `PREVIEW` are the read-side forms of the same
    // question and go through as ordinary META.
    let request = single_operation_request(command, parameters, false);
    (language, execute_request_readonly(nexus, &request).await)
}

/// Wraps one command in a single-operation request envelope (§71).
///
/// Parameters are bound at the request level, as bindings the command cites as
/// `:name` — they are data, never text spliced into the command (§74, §88.2).
fn single_operation_request(
    command: String,
    parameters: Option<Map<String, Json>>,
    dry_run: bool,
) -> Request {
    Request {
        parameters: parameters.filter(|p| !p.is_empty()),
        options: Some(RequestOptions {
            dry_run: Some(dry_run),
            ..Default::default()
        }),
        ..Request::single(command)
    }
}

/// Runs a KIP 2.0 request envelope built by a Python caller (§71).
///
/// A malformed envelope comes back as a `Response` carrying
/// `InvalidRequestEnvelope`, not as an error: the caller asked a protocol
/// question and gets a protocol answer, with the same shape and the same
/// registered code an engine that rejected it would return (§86).
///
/// `endpoint` selects the path: [`Endpoint::Readonly`] is §76, where a
/// state-changing operation fails the request instead of running.
async fn run_envelope(nexus: &impl Executor, envelope: Json, endpoint: Endpoint) -> Response {
    let request = match serde_json::from_value::<Request>(envelope) {
        Ok(request) => request,
        Err(err) => {
            return Response::from(KipError::invalid_request_envelope(format!(
                "the request is not a KIP {KIP_VERSION} envelope: {err}"
            )));
        }
    };
    match endpoint {
        Endpoint::Readonly => execute_request_readonly(nexus, &request).await,
        Endpoint::StateCapable => execute_request(nexus, &request).await,
    }
}

/// Which runtime an envelope is being executed against (§76, §77).
///
/// Named rather than a `bool`, because this is the boundary that decides
/// whether KML is admitted at all: a call site reading `false` says nothing
/// about what it selected, and a refactor that flips it silently turns the
/// read-only endpoint into a writing one (§88.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Endpoint {
    /// §77: KQL, KML and META, subject to Governance.
    StateCapable,
    /// §76: KQL and META only.
    Readonly,
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
            CREATE CONCEPT ?dark { TYPE "Person" NAME "Dark mode" }
            CREATE EVIDENCE ?said {
                SET FIELDS {
                    evidence_class: "user_statement",
                    payload: "I prefer dark mode.",
                    observed_at: "2026-09-07T00:00:00Z",
                    content_digest: "sha256:202ae77786db17a262130d6b033af5fe53f18716053d94549c28e1b7b991e642"
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
            CREATE CONCEPT ?light { TYPE "Person" NAME "Light mode" }
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
            schema_packages: None,
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
            schema_packages: None,
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
            schema_packages: None,
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

    fn in_mem(db_name: &str) -> Arc<CognitiveNexus> {
        block_on(create_kip_db(AndaDbConfig {
            store_location_type: StoreLocationType::InMem,
            store_location: String::new(),
            db_name: db_name.to_string(),
            db_desc: None,
            meta_cache_capacity: Some(10000),
            schema_packages: None,
        }))
        .expect("Failed to create Nexus")
    }

    /// §76: the read-only path admits reads and refuses writes, and it decides
    /// which is which by parsing the command rather than by trusting a label.
    #[test]
    fn the_readonly_path_refuses_a_write_and_serves_a_read() {
        let nexus = in_mem("test_readonly_db");

        let (language, response) = block_on(execute_kip_readonly(
            nexus.as_ref(),
            r#"CREATE CONCEPT ?c { TYPE "Person" NAME "Mallory" }"#.to_string(),
            None,
        ));
        // Classified as the write it is, then refused for being one.
        assert!(matches!(language, CommandType::Kml));
        assert_eq!(response.status, TopLevelStatus::Failed);
        assert_eq!(
            response
                .error
                .as_ref()
                .and_then(|error| error.parsed_code()),
            Some(anda_kip::KipErrorCode::ReadonlyViolation)
        );

        // And it did not happen: the state-capable path finds nothing.
        let read = run(
            nexus.as_ref(),
            r#"FIND(?c.name) WHERE { ?c CONCEPT {type: "Person", name: "Mallory"} }"#,
        );
        let rows = read
            .first_result()
            .and_then(|result| result.as_array().cloned())
            .unwrap_or_default();
        assert!(
            rows.is_empty(),
            "a refused write must not commit: {rows:#?}"
        );

        // META and KQL go through, with their parameters bound as data.
        let (language, response) = block_on(execute_kip_readonly(
            nexus.as_ref(),
            "DESCRIBE PRIMER".into(),
            None,
        ));
        assert!(matches!(language, CommandType::Meta));
        assert_eq!(response.status, TopLevelStatus::Succeeded, "{response:#?}");
    }

    /// The whole §71 envelope reaches the engine: a named Space (§5.5), a
    /// `sequence` of operations correlated by `op_id` (§75.2), and Evidence
    /// minted from the envelope rather than re-typed inside KML text (§71.1).
    #[test]
    fn the_envelope_carries_space_ingest_and_a_sequence() {
        let nexus = in_mem("test_envelope_db");

        let envelope = serde_json::json!({
            "kip": KIP_VERSION,
            "request_id": "req-1",
            "space": {"id": DEFAULT_SPACE},
            "execution": {"mode": "sequence", "on_error": "stop"},
            "ingest": {
                "evidence": [{
                    "key": "msg",
                    "evidence_class": "user_statement",
                    "payload": "I prefer dark mode.",
                    "media_type": "text/plain",
                    "observed_at": "2026-08-14T01:00:00Z"
                }]
            },
            "operations": [
                {
                    "op_id": "write",
                    "command": r#"
                        MUTATE {
                            CREATE CONCEPT ?alice { TYPE "Person" NAME :who }
                            CREATE CONCEPT ?dark { TYPE "Person" NAME "Dark mode" }
                            ASSERT ?a (?alice, "prefers", ?dark) {
                                by: ?alice, mode: "stated", confidence: 0.9, evidence: :msg
                            }
                        }
                    "#
                },
                {
                    "op_id": "read",
                    "command": r#"FIND(?c.name) WHERE { ?c CONCEPT {type: "Person", name: :who} }"#
                }
            ],
            "parameters": {"who": "Alice"}
        });

        let response = block_on(run_envelope(
            nexus.as_ref(),
            envelope,
            Endpoint::StateCapable,
        ));
        assert_eq!(
            response.status,
            TopLevelStatus::Succeeded,
            "envelope failed: {:#?}",
            response
        );
        assert_eq!(response.request_id.as_deref(), Some("req-1"));
        assert_eq!(response.results.len(), 2);
        assert_eq!(response.results[0].op_id.as_deref(), Some("write"));
        assert_eq!(response.results[1].op_id.as_deref(), Some("read"));
        // The later operation observed the earlier one's committed effects.
        assert_eq!(
            response.results[1].result,
            Some(Json::from(vec![Json::from("Alice")]))
        );
        // The write ran in its own transaction and reported its own Receipt;
        // the top-level slot is reserved for `atomic` (§75.2).
        assert!(response.results[0].receipt.is_some());

        // The Evidence the envelope minted is the one the Assertion cites — it
        // was never re-typed into the command text (§88.12).
        let evidence = run(
            nexus.as_ref(),
            r#"FIND(?e.payload) WHERE { ?e EVIDENCE {evidence_class: "user_statement"} }"#,
        );
        assert_eq!(
            evidence.first_result().and_then(|r| r.as_array().cloned()),
            Some(vec![serde_json::json!({
                "mode": "inline",
                "inline": "I prefer dark mode."
            })]),
            "{evidence:#?}"
        );
    }

    /// A read-only endpoint refuses the whole envelope when any operation is a
    /// write, and does not serve the reads beside it.
    #[test]
    fn a_readonly_envelope_refuses_a_batch_containing_a_write() {
        let nexus = in_mem("test_readonly_envelope_db");

        let envelope = serde_json::json!({
            "kip": KIP_VERSION,
            "execution": {"mode": "independent"},
            "operations": [
                {"op_id": "read", "command": "DESCRIBE PRIMER"},
                // Labelled as a read; it is a write, and §73.1 says the parse
                // decides.
                {
                    "op_id": "write",
                    "language": "KML",
                    "command": r#"CREATE CONCEPT ?c { TYPE "Person" NAME "Mallory" }"#
                }
            ]
        });

        let response = block_on(run_envelope(nexus.as_ref(), envelope, Endpoint::Readonly));
        assert_eq!(
            response
                .error
                .as_ref()
                .and_then(|error| error.parsed_code()),
            Some(anda_kip::KipErrorCode::ReadonlyViolation),
            "{response:#?}"
        );
        // No operation result of its own: the read was not served either.
        assert!(response.results.iter().all(|r| r.op_id.is_none()));
    }

    /// A malformed envelope is answered, not raised: same shape, registered
    /// code, so a caller's recovery path does not need a second branch.
    #[test]
    fn a_malformed_envelope_is_answered_with_a_registered_code() {
        let nexus = in_mem("test_bad_envelope_db");

        for envelope in [
            // Not this protocol version.
            serde_json::json!({"kip": "1.0", "operations": [{"command": "DESCRIBE PRIMER"}]}),
            // A member no KIP 2.0 envelope has: accepted silently, it would
            // read as a setting that took effect.
            serde_json::json!({
                "kip": KIP_VERSION,
                "operations": [{"command": "DESCRIBE PRIMER"}],
                "readonly": true
            }),
            // Several operations and no declared execution mode: whether an
            // earlier commit survives a later failure is not an engine default
            // (§75.4).
            serde_json::json!({
                "kip": KIP_VERSION,
                "operations": [
                    {"command": "DESCRIBE PRIMER"},
                    {"command": "DESCRIBE PROTOCOL"}
                ]
            }),
        ] {
            let response = block_on(run_envelope(
                nexus.as_ref(),
                envelope.clone(),
                Endpoint::StateCapable,
            ));
            assert_eq!(
                response.status,
                TopLevelStatus::Failed,
                "accepted {envelope}"
            );
            assert!(response.error.is_some(), "no error for {envelope}");
        }
    }

    /// A Capsule carries cognition between two Nexuses (§37, §41).
    ///
    /// Export is a META command an Agent can issue; import is not, and this is
    /// the host API that does it.
    #[test]
    fn a_capsule_carries_cognition_into_another_nexus() {
        let source = in_mem("test_capsule_source_db");
        let response = run(source.as_ref(), RECORD_A_PREFERENCE);
        assert_eq!(response.status, TopLevelStatus::Succeeded, "{response:#?}");

        let exported = run(
            source.as_ref(),
            r#"EXPORT CAPSULE ?a WHERE { ?a ASSERTION {} }"#,
        );
        assert_eq!(exported.status, TopLevelStatus::Succeeded, "{exported:#?}");
        let capsule = exported
            .first_result()
            .cloned()
            .expect("the export returns the artifact");

        let destination = in_mem("test_capsule_destination_db");
        let report = block_on(import_capsule_json(
            destination.as_ref(),
            capsule.clone(),
            DEFAULT_SPACE,
            false,
        ))
        .expect("the import succeeds");
        assert_eq!(report["imported"], Json::Bool(true), "{report:#}");
        assert!(
            report["identity_map"]
                .as_object()
                .is_some_and(|m| !m.is_empty()),
            "{report:#}"
        );

        // The claim arrived, and arrived as a claim: the Assertion is what
        // carries the confidence, and nothing about it became truth on the way.
        let read = run(
            destination.as_ref(),
            r#"FIND(?person.name, ?a.confidence)
               WHERE {
                   ?person CONCEPT {type: "Person"}
                   ?p PROPOSITION (?person, "prefers", ?pref)
                   ?a ASSERTION {proposition: ?p}
               }"#,
        );
        assert_eq!(
            read.first_result().and_then(|r| r.as_array().cloned()),
            Some(vec![Json::from(vec![Json::from("Alice"), Json::from(0.9)])]),
            "{read:#?}"
        );

        // Re-importing the same artifact resolves every record back to the
        // element the first import created, so it is not a second copy.
        block_on(import_capsule_json(
            destination.as_ref(),
            capsule,
            DEFAULT_SPACE,
            false,
        ))
        .expect("a re-import succeeds");
        let again = run(
            destination.as_ref(),
            r#"FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} }"#,
        );
        assert_eq!(
            again.first_result().and_then(|r| r.as_array().cloned()),
            Some(vec![Json::from("Alice")]),
            "a re-import duplicated the Concept: {again:#?}"
        );
    }

    /// §20.10: a host activates Schema Packages, and the Schema Lock names
    /// exactly what it asked for — so dropping the baseline deactivates it.
    #[test]
    fn activating_a_lock_puts_exactly_those_packages_in_force() {
        let nexus = in_mem("test_schema_db");

        // The bootstrap already activated the baseline, so re-activating the
        // same lock changes nothing and must not mint a version (§20.8).
        let first = block_on(activate_schema_packages(
            &nexus,
            &[("bundled", COGNITIVE_MEMORY)],
            DEFAULT_SPACE,
        ))
        .unwrap();
        let second = block_on(activate_schema_packages(
            &nexus,
            &[("bundled", COGNITIVE_MEMORY)],
            DEFAULT_SPACE,
        ))
        .unwrap();
        assert_eq!(
            first["schema_environment_version"], second["schema_environment_version"],
            "an unchanged lock re-activated: {second:#}"
        );
        assert!(
            second["lock"]["packages"]
                .get(anda_cognitive_nexus::profiles::COGNITIVE_MEMORY_ID)
                .is_some(),
            "{second:#}"
        );

        // Activating nothing leaves Core alone in force, and Core declares no
        // Concept types: the Space can still hold Assertions about types it
        // does not have, and can no longer create a Concept.
        block_on(activate_schema_packages(&nexus, &[], DEFAULT_SPACE)).unwrap();
        let refused = run(
            nexus.as_ref(),
            r#"CREATE CONCEPT ?c { TYPE "Person" NAME "Nobody" }"#,
        );
        assert_eq!(refused.status, TopLevelStatus::Failed, "{refused:#?}");
    }
}
