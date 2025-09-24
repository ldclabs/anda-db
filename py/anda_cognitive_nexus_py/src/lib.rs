use anda_cognitive_nexus::CognitiveNexus;
use anda_db::database::{AndaDB, DBConfig};
use object_store::local::LocalFileSystem;
use anda_kip::{CommandType, KipError, Request, Response};
use object_store::memory::InMemory;
use pyo3::prelude::*;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::types::PyDict;
use serde::{Deserialize, Serialize};
use serde_pyobject::to_pyobject;
use serde_json::{Map, Value};
use std::sync::Arc;
use anda_object_store::{MetaStoreBuilder};
use anda_kip::Json;
use anda_kip::executor::Executor;
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
        match s.trim() {
            "Kml" => PyCommandType::Kml,
            "Kql" => PyCommandType::Kql,
            "Meta" => PyCommandType::Meta,
            _ => PyCommandType::Unknown,
        }
    }
}

#[pymethods]
impl PyCommandType {
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
        log::info!("AndaDB.create called with db_config.");
        let fut = async move {
            match create_kip_db(db_config).await {
                Ok(nexus) => Ok(PyAndaDB { nexus }),
                Err(e) => Err(PyRuntimeError::new_err(format!("DB creation error: {}", e)))
            }
        };
        Ok(pyo3_asyncio::tokio::future_into_py(py, fut)?)
    }

    #[pyo3(signature = (command, dry_run = false, parameters = None))]
    #[pyo3(text_signature = "(command: str, dry_run: bool = False, parameters: dict = None) -> Awaitable[Dict[str, Any]]")]
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
    ///     RuntimeError: If KIP execution fails.
    pub fn execute_kip<'py>(
        &self,
        py: Python<'py>,
        command: String,
        dry_run: bool,
        parameters: Option<&PyDict>,
    ) -> PyResult<&'py PyAny> {
        log::info!(
            "AndaDB.execute_kip called: command={}, dry_run={}",
            command,
            dry_run
        );

        // Convert Python dict -> serde_json::Map<String, Value>
        let params_map = parameters
            .map(|dict| {
                let json_mod = py.import("json").unwrap();
                let json_str: String = json_mod
                    .call_method1("dumps", (dict,))
                    .unwrap()
                    .extract()
                    .unwrap();
                serde_json::from_str::<Map<String, Value>>(&json_str).unwrap()
            })
            .unwrap_or_default();

        log::debug!("params_map: {}", serde_json::to_string(&params_map).unwrap());

        let nexus = self.nexus.clone();

        // Async future that returns a PyObject (a Python dict)
        let fut = async move {
            match execute_kip(
                nexus.as_ref(),
                command,
                Some(
                    params_map
                        .into_iter()
                        .map(|(k, v)| (k, Json::from(v)))
                        .collect(),
                ),
                dry_run,
            )
            .await
            {
                Ok((cmd_type, response)) => {
                    // Convert both the cmd_type and the response into Python objects while holding the GIL
                    let py_obj: PyObject = Python::with_gil(|py| -> PyResult<PyObject> {
                        // 1) Wrap cmd_type into the Python-visible class
                        let py_cmd_wrapper = Py::new(py, PyCommandType::from(cmd_type))?;

                        // 2) Convert response (serde-serializable) into a Python object using serde-pyobject
                        let py_response = to_pyobject(py, &response)
                            .map_err(|e| PyRuntimeError::new_err(format!("Response conversion error: {}", e)))?;

                        // 3) Build the resulting Python dict {"type": <PyCommandType>, "response": <py_response>}
                        let out_dict = PyDict::new(py);
                        out_dict.set_item("type", py_cmd_wrapper.as_ref(py))?;
                        out_dict.set_item("response", py_response)?;

                        Ok(out_dict.into())
                    })?;

                    Ok(py_obj)
                }
                Err(e) => Err(PyRuntimeError::new_err(format!("KIP execution error: {}", e))),
            }
        };

        // Convert the Rust Future -> Python awaitable
        Ok(pyo3_asyncio::tokio::future_into_py(py, fut)?)
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
        Ok(
            AndaDbConfig {
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
                return Err("store_location is required when store_location_type is LocalFile".to_string());
            }
            use std::path::Path;
            if !Path::new(&self.store_location).exists() {
                return Err(format!("store_location path does not exist: {}", self.store_location));
            }
        }
        Ok(())
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn anda_cognitive_nexus_py(_py: Python, m: &PyModule) -> PyResult<()> {
    structured_logger::init();
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

        if StoreLocationType::InMem.__str__() == s
            { Ok(StoreLocationType::InMem) }
        else if  StoreLocationType::LocalFile.__str__() == s
            { Ok(StoreLocationType::LocalFile) }
        else { 
            Err(PyValueError::new_err(format!("Invalid StoreLocationType: {}", s)))
        }
    }
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
pub async fn create_kip_db(
    db_config: AndaDbConfig,
) -> Result<Arc<CognitiveNexus>, BoxError> {
    db_config.verify_config()
        .map_err(|e| KipError::Execution(e))?;

    let db_name = db_config.db_name.as_str();
    let db_desc = db_config.db_desc.as_deref().unwrap_or_default();
    let meta_cache_capacity = db_config.meta_cache_capacity.unwrap_or(10000);

    let object_store: Arc<dyn object_store::ObjectStore> = match db_config.store_location_type {
        StoreLocationType::InMem => Arc::new(InMemory::new()),
        StoreLocationType::LocalFile => {
            let local_file = MetaStoreBuilder::new(
                LocalFileSystem::new_with_prefix(&db_config.store_location)
                    .map_err(|err| KipError::Execution(err.to_string()))?,
                meta_cache_capacity,
            ).build();
            Arc::new(local_file)
        }
    };

    let db_config = DBConfig {
        name: db_name.to_string(),
        description: db_desc.to_string(),
        ..Default::default()
    };

    let db = Arc::new(AndaDB::connect(object_store, db_config).await?);
    let nexus = Arc::new(CognitiveNexus::connect(db, async |_| Ok(())).await?);
    Ok(nexus)
}

/// Executes a KIP command using an existing Executor instance.
///
/// # Arguments
///
/// * `nexus` - Reference to an Executor instance (`&(impl Executor + Sync)`).
/// * `command` - The KIP command string to execute (KML/KQL/META).
/// * `parameters` - An optional map of command parameters (`Option<Map<String, Json>>`). If `None`, treated as empty.
/// * `dry_run` - If true, performs a dry run without committing changes.
///
/// # Returns
///
/// Returns a tuple of the command type and the response on success, or a boxed error on failure.
///
/// # Errors
///
/// Returns an error if the KIP command execution fails.
///
/// # Example
/// 
/// Refer to tools/anda_cognitive_nexus_py/examples directory
pub async fn execute_kip(
    nexus: &(impl Executor + Sync),
    command: String,
    parameters: Option<Map<String, Json>>,
    dry_run: bool,
) -> Result<(CommandType, Response), BoxError> {
    let params_map = parameters.unwrap_or_default();

    let request = Request {
        command,
        parameters: params_map,
        dry_run,
    };

    Ok(request.execute(nexus).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anda_kip::{Json, Map};
    use std::future::Future;

    // Helper to run async code in tests
    fn block_on<F: Future<Output = T>, T>(fut: F) -> T {
        tokio::runtime::Runtime::new().unwrap().block_on(fut)
    }

    // Create basic concept types and medical knowledge capsule
    static MEDICAL_KNOWLEDGE_KML: &str = r#"
        UPSERT {
            // Define concept types
            CONCEPT ?drug_type {
                {type: "$ConceptType", name: "Drug"}
                SET ATTRIBUTES {
                    description: "Pharmaceutical drug concept type"
                }
            }

            CONCEPT ?symptom_type {
                {type: "$ConceptType", name: "Symptom"}
                SET ATTRIBUTES {
                    description: "Medical symptom concept type"
                }
            }

            // Define relation types
            CONCEPT ?treats_relation {
                {type: "$PropositionType", name: "treats"}
                SET ATTRIBUTES {
                    description: "Drug treats symptom relationship"
                }
            }

            CONCEPT ?has_side_effect_relation {
                {type: "$PropositionType", name: "has_side_effect"}
                SET ATTRIBUTES {
                    description: "Drug has side effect relationship"
                }
            }

            // Create symptom concepts
            CONCEPT ?headache {
                {type: "Symptom", name: "Headache"}
                SET ATTRIBUTES {
                    severity_scale: "1-10",
                    description: "Pain in the head or neck area"
                }
            }

            CONCEPT ?fever {
                {type: "Symptom", name: "Fever"}
                SET ATTRIBUTES {
                    normal_temp: "98.6°F (37°C)",
                    description: "Elevated body temperature"
                }
            }

            CONCEPT ?stomach_irritation {
                {type: "Symptom", name: "Stomach Irritation"}
                SET ATTRIBUTES {
                    severity: "mild to moderate",
                    description: "Gastrointestinal discomfort"
                }
            }

            // Create specific drug concepts
            CONCEPT ?aspirin {
                {type: "Drug", name: "Aspirin"}
                SET ATTRIBUTES {
                    molecular_formula: "C9H8O4",
                    risk_level: 1,
                    description: "Common pain reliever and anti-inflammatory drug"
                }
                SET PROPOSITIONS {
                    ("treats", ?headache)
                    ("treats", ?fever)
                    ("has_side_effect", ?stomach_irritation)
                }
            }
        }
        WITH METADATA {
            source: "KIP Demo Medical Knowledge",
            author: "Demo System",
            confidence: 0.95,
            created_at: "2025-07-01T00:00:00Z"
        }
        "#;

    // Create a new hypothetical drug
    static NEW_DRUG_KML: &str = r#"
        UPSERT {
            CONCEPT ?brain_fog {
                {type: "Symptom", name: $symptom_name}
                SET ATTRIBUTES {
                    description: "Mental fatigue and lack of clarity",
                    cognitive_impact: "high"
                }
            }

            CONCEPT ?neural_bloom {
                {type: "Symptom", name: "Neural Bloom"}
                SET ATTRIBUTES {
                    description: "A rare side effect characterized by temporary burst of creative thoughts",
                    frequency: "rare",
                    severity: "mild"
                }
            }

            CONCEPT ?cognizine {
                {type: "Drug", name: "Cognizine"}
                SET ATTRIBUTES {
                    molecular_formula: "C12H15N5O3",
                    risk_level: 2,
                    description: "A novel nootropic drug designed to enhance cognitive functions",
                    status: "experimental"
                }
                SET PROPOSITIONS {
                    ("treats", {type: "Symptom", name: "Brain Fog"})
                    ("has_side_effect", ?neural_bloom)
                }
            }
        }
        WITH METADATA {
            source: "Experimental Drug Research",
            confidence: 0.75,
            status: "under_review"
        }
        "#;

    #[test]
    fn test_execute_kip_in_mem() {
        // 1. Execute the first KML command from the demo to set up schema and initial data
        println!("\n1. Executing Medical Knowledge KML...");

        // Add db_config for in-memory DB (as AndaDbConfig struct expects)
        let db_config_in_mem = AndaDbConfig {
            store_location_type: StoreLocationType::InMem,
            store_location: "".to_owned(),
            db_name: "test_medical_db".to_string(),
            db_desc: Some("Ephemeral DB for medical KIP test".to_string()),
            meta_cache_capacity: Some(10000),
        };

        // Create Nexus instance for in-memory DB
        let nexus_in_mem = block_on(create_kip_db(db_config_in_mem)).expect("Failed to create in_mem Nexus");

        // Use empty Map for parameters
        let empty_params: Map<String, Json> = Map::new();

        let (_, response1) = block_on(execute_kip(
            nexus_in_mem.as_ref(),
            MEDICAL_KNOWLEDGE_KML.to_string(),
            Some(empty_params.clone()),
            false,
        ))
        .expect("Execution of medical_knowledge_kml failed");
        assert!(matches!(response1, Response::Ok { .. }), "Expected first KML execution to be Ok, but got {:?}", response1);
        println!("Medical Knowledge KML executed successfully (in_mem DB).");

        // 2. Execute the second KML command from the demo to add more data
        println!("\n2. Executing New Drug KML...");

        let mut ql_parameters = Map::new();
        ql_parameters.insert(
            "symptom_name".to_string(),
            Json::String("Brain Fog".to_string()),
        );

        let (_, response2) = block_on(execute_kip(
            nexus_in_mem.as_ref(),
            NEW_DRUG_KML.to_string(),
            Some(ql_parameters.clone()),
            false,
        ))
        .expect("Execution of new_drug_kml failed");
        assert!(matches!(response2, Response::Ok { .. }), "Expected third KML execution to be Ok, but got {:?}", response2);
        println!("New Drug KML executed successfully (in_mem DB).");
        // 3. Execute a KQL query from the demo to verify the data
        println!("\n3. Executing KQL Query to find all drugs...");
        let query = r#"
        FIND(?drug.name, ?drug.attributes.molecular_formula, ?drug.attributes.risk_level)
        WHERE {
            ?drug {type: "Drug"}
        }
        ORDER BY ?drug.attributes.risk_level ASC
        "#;

        let (_, query_response) = block_on(execute_kip(
            nexus_in_mem.as_ref(),
            query.to_string(),
            None,
            false,
        ))
        .expect("Execution of KQL query failed");

        println!("Query Response: {:#?}", query_response);

        // 4. Assert that the query was successful and returned the correct data
        assert!(matches!(query_response, Response::Ok { .. }), "Expected KQL query to be Ok, but got {:?}", query_response);

        if let Response::Ok { result, .. } = query_response {
            let result_array = result.as_array().expect("Result should be an array");
            assert_eq!(result_array.len(), 2, "Expected to find 2 drugs, but found {}", result_array.len());
            println!("Successfully found 2 drugs as expected.");
        } else {
            panic!("Query failed, expected Ok response");
        }

        println!("\n--- Full Stateful KIP Execution Test Passed ---");        
    }
}
