//! # Anda DB Server
//!
//! A HTTP server for Anda DB using a lightweight RPC protocol over HTTP,
//! with both JSON and CBOR data interchange formats.
//!
//! ## API
//!
//! RPC endpoints:
//! - `POST /` for server-level methods (e.g. `create_database`)
//! - `POST /{db_name}` for database-scoped methods
//!
//! ### Root Methods (`POST /`)
//!
//! | Method | Params | Description |
//! |--------|--------|-------------|
//! | `get_information` | — | Server information |
//! | `create_database` | `{name, description?}` | Create a database |
//! | `list_databases` | — | List all database names |
//!
//! ### Database Methods (`POST /{db_name}`)
//!
//! | Method | Params | Description |
//! |--------|--------|-------------|
//! | `get_information` | — | Server information |
//! | `get_db_metadata` | — | Database metadata |
//! | `flush_db` | — | Flush all data to storage |
//! | `create_collection` | `{config, schema, btree_indexes?, bm25_indexes?, hnsw_indexes?}` | Create a new collection |
//! | `get_collection_metadata` | `{collection}` | Get collection metadata |
//! | `delete_collection` | `{collection}` | Delete a collection |
//! | `add_document` | `{collection, document}` | Add a document |
//! | `get_document` | `{collection, id}` | Get a document |
//! | `update_document` | `{collection, id, fields}` | Update a document |
//! | `remove_document` | `{collection, id}` | Remove a document |
//! | `search_documents` | `{collection, query}` | Search documents |
//! | `search_document_ids` | `{collection, query}` | Search document IDs |
//! | `query_document_ids` | `{collection, filter, limit?}` | Query IDs by filter |
//!
//! ### Request Format
//!
//! ```json
//! {"method": "get_db_metadata", "params": {"collection": "articles"}}
//! ```
//!
//! ### Response Format
//!
//! Success:
//! ```json
//! {"result": {...}}
//! ```
//!
//! Error:
//! ```json
//! {"error": {"code": -32001, "message": "database not found: demo"}}
//! ```
//!
//! ### Data Format
//!
//! Both JSON and CBOR are supported. Use HTTP headers to control the format:
//! - `Content-Type: application/json` or `application/cbor` for request bodies
//! - `Accept: application/json` or `application/cbor` for response bodies
//! - Under CBOR protocol, `params` and `result` are encoded as CBOR values
//!
//! ## Usage
//!
//! ```bash
//! # Start with local filesystem storage
//! cargo run -p anda_db_server -- local --db ./debug/db
//!
//! # Start with in-memory storage
//! cargo run -p anda_db_server
//!
//! # With API key authentication
//! cargo run -p anda_db_server -- --api-key my-secret local --db ./debug/db
//! ```

use anda_db::{
    database::{AndaDB, DBConfig},
    storage::StorageConfig,
};
use anda_db_server::{build_router, handler::AppState};
use anda_object_store::MetaStoreBuilder;
use axum::BoxError;
use clap::{Parser, Subcommand};
use object_store::{ObjectStore, aws::AmazonS3Builder, local::LocalFileSystem, memory::InMemory};
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc, time::Duration};
use structured_logger::{Builder, async_json::new_writer, get_env_level};
use tokio::signal;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

const APP_NAME: &str = env!("CARGO_PKG_NAME");
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Address and port to listen on
    #[clap(long, env = "ADDR", default_value = "127.0.0.1:8080")]
    addr: String,

    /// API key for authentication (optional)
    #[clap(long, env = "API_KEY")]
    api_key: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Use local filesystem storage
    Local {
        /// Path to the database directory
        #[clap(long, env = "LOCAL_DB_PATH", default_value = "./db")]
        db: String,
    },
}

/// Main entry point for the Anda DB Server.
///
/// # Example Usage
/// ```bash
/// cargo run -p anda_db_server -- local --db ./debug/db
/// ```
#[tokio::main]
async fn main() -> Result<(), BoxError> {
    dotenv::dotenv().ok();
    let cli = Cli::parse();

    // Initialize structured logging with JSON format
    Builder::with_level(&get_env_level().to_string())
        .with_target_writer("*", new_writer(tokio::io::stdout()))
        .init();

    let object_store = match cli.command {
        Some(Commands::Local { db }) => build_object_store(db, None)?,
        None => build_object_store("memory".to_string(), None)?,
    };

    let db_config = DBConfig {
        name: "anda_db".to_string(),
        description: "Anda DB".to_string(),
        storage: StorageConfig {
            cache_max_capacity: 100000,
            compress_level: 3,
            object_chunk_size: 256 * 1024,
            bucket_overload_size: 1024 * 1024,
            max_small_object_size: 1024 * 1024 * 10,
        },
        lock: None,
    };

    let db = AndaDB::connect(object_store.clone(), db_config.clone()).await?;
    let db = Arc::new(db);

    // Start background auto-flush task
    let cancel_token = CancellationToken::new();
    let db_clone = db.clone();
    let cancel_clone = cancel_token.clone();
    let db_task = async {
        db_clone
            .auto_flush(cancel_clone, Duration::from_secs(30))
            .await;
        Ok::<(), std::io::Error>(())
    };

    let mut databases = BTreeMap::new();
    databases.insert(db_config.name.clone(), db.clone());

    let state = AppState {
        databases: Arc::new(RwLock::new(databases)),
        object_store,
        storage: db_config.storage.clone(),
        name: APP_NAME.to_string(),
        version: APP_VERSION.to_string(),
        api_key: cli.api_key,
    };

    let app = build_router(state);

    let addr: SocketAddr = cli.addr.parse()?;
    let listener = create_reuse_port_listener(addr).await?;
    log::warn!("{}@{} listening on {:?}", APP_NAME, APP_VERSION, addr);
    let server_task =
        axum::serve(listener, app).with_graceful_shutdown(shutdown_signal(cancel_token));

    tokio::try_join!(server_task, db_task)?;
    Ok(())
}

/// Handles graceful shutdown on SIGINT/SIGTERM.
pub async fn shutdown_signal(cancel_token: CancellationToken) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    log::warn!("received termination signal, starting graceful shutdown");
    cancel_token.cancel();
}

/// Creates a TCP listener with SO_REUSEPORT enabled.
pub async fn create_reuse_port_listener(
    addr: SocketAddr,
) -> Result<tokio::net::TcpListener, BoxError> {
    let socket = match &addr {
        SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
        SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
    };

    socket.set_reuseport(true)?;
    socket.bind(addr)?;
    let listener = socket.listen(1024)?;
    Ok(listener)
}

/// Builds an ObjectStore based on the storage type.
fn build_object_store(
    ty: String,
    cfg: Option<BTreeMap<String, String>>,
) -> Result<Arc<dyn ObjectStore>, BoxError> {
    match ty.as_str() {
        "" | "memory" | "in_memory" => Ok(Arc::new(InMemory::new())),
        "s3" => {
            let mut builder: AmazonS3Builder = Default::default();
            for (k, v) in cfg.unwrap_or_default().iter() {
                if let Ok(config_key) = k.to_ascii_lowercase().parse() {
                    builder = builder.with_config(config_key, v);
                }
            }
            let os = builder.build()?;
            Ok(Arc::new(os))
        }
        _ => {
            let os = LocalFileSystem::new_with_prefix(ty)?;
            let os = MetaStoreBuilder::new(os, 100000).build();
            Ok(Arc::new(os))
        }
    }
}
