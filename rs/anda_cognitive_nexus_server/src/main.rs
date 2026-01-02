use anda_db::{
    database::{AndaDB, DBConfig},
    storage::StorageConfig,
};
use anda_object_store::MetaStoreBuilder;
use axum::{BoxError, Router, routing};
use clap::{Parser, Subcommand};
use object_store::{ObjectStore, aws::AmazonS3Builder, local::LocalFileSystem, memory::InMemory};
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc};
use structured_logger::{Builder, async_json::new_writer, get_env_level};
use tokio::signal;
use tokio_util::sync::CancellationToken;

mod handler;
mod nexus;

use handler::*;

const APP_NAME: &str = env!("CARGO_PKG_NAME");
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Port to listen on
    #[clap(long, env = "ADDR", default_value = "127.0.0.1:8080")]
    addr: String,

    #[clap(long, env = "API_KEY")]
    api_key: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
pub enum Commands {
    Local {
        #[clap(long, env = "LOCAL_DB_PATH", default_value = "./db")]
        db: String,
    },
}

/// Main entry point for the server.
///
/// # Example Usage
/// ```bash
/// cargo run -p anda_cognitive_nexus_server -- local --db ./debug/db
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
        Some(Commands::Local { db }) => build_object_store(db, None).unwrap(),
        None => build_object_store("memory".to_string(), None).unwrap(),
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

    let db = AndaDB::connect(object_store.clone(), db_config).await?;
    let nexus = nexus::Nexus::connect(Arc::new(db)).await?;

    let state = AppState {
        nexus,
        name: APP_NAME.to_string(),
        version: APP_VERSION.to_string(),
        api_key: cli.api_key,
    };
    let app = Router::new()
        .route("/", routing::get(get_information))
        .route("/kip", routing::post(post_kip))
        .with_state(state);
    let cancel_token = CancellationToken::new();
    let addr: SocketAddr = cli.addr.parse()?;
    let listener = create_reuse_port_listener(addr).await?;
    log::warn!("{}@{} listening on {:?}", APP_NAME, APP_VERSION, addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(cancel_token))
        .await?;

    Ok(())
}

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
