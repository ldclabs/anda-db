//! Anda DB Server binary: CLI parsing, storage bootstrap, and graceful
//! shutdown. See the crate docs ([`anda_db_server`]) for the API reference.
//!
//! ```bash
//! # In-memory storage (data is lost on exit)
//! cargo run -p anda_db_server
//!
//! # Local filesystem storage
//! cargo run -p anda_db_server -- local --path ./debug/db
//!
//! # S3-compatible storage, configured via AWS_* environment variables
//! cargo run -p anda_db_server -- s3
//!
//! # With API key authentication
//! cargo run -p anda_db_server -- --api-key my-secret local --path ./debug/db
//! ```

use anda_db_server::{AppState, ServerOptions, build_router};
use anda_object_store::MetaStoreBuilder;
use axum::BoxError;
use clap::{Parser, Subcommand};
use object_store::{ObjectStore, aws::AmazonS3Builder, local::LocalFileSystem, memory::InMemory};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use structured_logger::{Builder, async_json::new_writer, get_env_level};
use tokio::signal;
use tokio_util::sync::CancellationToken;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const APP_NAME: &str = env!("CARGO_PKG_NAME");
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Address and port to listen on
    #[clap(long, env = "ADDR", default_value = "127.0.0.1:8080")]
    addr: String,

    /// API key required as `Authorization: Bearer <key>` (optional)
    #[clap(long, env = "API_KEY")]
    api_key: Option<String>,

    /// Name of the primary database (created on first start; also stores
    /// the registry of databases to reopen)
    #[clap(long, env = "PRIMARY_DB", default_value = "anda_db")]
    primary_db: String,

    /// Background flush interval in seconds for every open database
    #[clap(long, env = "FLUSH_INTERVAL_SECS", default_value = "30")]
    flush_interval_secs: u64,

    /// Per-request processing timeout in seconds for the RPC endpoints
    #[clap(long, env = "REQUEST_TIMEOUT_SECS", default_value = "300")]
    request_timeout_secs: u64,

    /// Maximum accepted request body size in bytes
    #[clap(long, env = "MAX_BODY_SIZE", default_value = "2097152")]
    max_body_size: usize,

    /// Grace period in seconds to drain in-flight requests on shutdown
    #[clap(long, env = "SHUTDOWN_TIMEOUT_SECS", default_value = "30")]
    shutdown_timeout_secs: u64,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// In-memory storage; all data is lost when the server exits (default)
    Memory,
    /// Local filesystem storage
    Local {
        /// Path to the database directory
        #[clap(long, env = "LOCAL_DB_PATH", default_value = "./db")]
        path: String,
    },
    /// S3-compatible storage configured via AWS_* environment variables
    /// (AWS_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT, ...)
    S3,
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    dotenv::dotenv().ok();
    let cli = Cli::parse();

    Builder::with_level(&get_env_level().to_string())
        .with_target_writer("*", new_writer(tokio::io::stdout()))
        .init();

    let object_store: Arc<dyn ObjectStore> = match cli.command {
        None | Some(Commands::Memory) => Arc::new(InMemory::new()),
        Some(Commands::Local { path }) => {
            let store = LocalFileSystem::new_with_prefix(path)?;
            // The local filesystem backend needs the metadata wrapper for
            // conditional-put support used by the storage layer.
            Arc::new(MetaStoreBuilder::new(store, 100_000).build())
        }
        Some(Commands::S3) => Arc::new(AmazonS3Builder::from_env().build()?),
    };

    let state = AppState::connect(
        object_store,
        ServerOptions {
            name: APP_NAME.to_string(),
            version: APP_VERSION.to_string(),
            primary_db: cli.primary_db,
            api_key: cli.api_key,
            flush_interval: Duration::from_secs(cli.flush_interval_secs.max(1)),
            request_timeout: Duration::from_secs(cli.request_timeout_secs.max(1)),
            max_body_size: cli.max_body_size.max(1024),
            ..Default::default()
        },
    )
    .await
    .map_err(|err| err.message)?;

    let app = build_router(state.clone());
    let addr: SocketAddr = cli.addr.parse()?;
    let listener = create_reuse_port_listener(addr).await?;
    log::warn!("{APP_NAME}@{APP_VERSION} listening on {addr:?}");

    // A termination signal cancels the token; graceful shutdown then drains
    // in-flight connections, bounded by `shutdown_timeout_secs` so a hung
    // client cannot keep the process alive forever.
    let shutdown = CancellationToken::new();
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            shutdown_signal().await;
            shutdown.cancel();
        }
    });

    let drain = Duration::from_secs(cli.shutdown_timeout_secs.max(1));
    let serve = axum::serve(listener, app).with_graceful_shutdown({
        let shutdown = shutdown.clone();
        async move { shutdown.cancelled().await }
    });
    let result = tokio::select! {
        result = serve => result,
        _ = async {
            shutdown.cancelled().await;
            tokio::time::sleep(drain).await;
        } => {
            log::warn!("graceful shutdown drain deadline exceeded, closing now");
            Ok(())
        }
    };

    // Flush and close every open database before exiting, even when the
    // server loop returned an error.
    state.shutdown().await;
    result?;
    Ok(())
}

/// Resolves when SIGINT (Ctrl+C) or SIGTERM is received.
async fn shutdown_signal() {
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
}

/// Creates a TCP listener with SO_REUSEPORT enabled so multiple server
/// processes can share the port for zero-downtime restarts.
async fn create_reuse_port_listener(addr: SocketAddr) -> Result<tokio::net::TcpListener, BoxError> {
    let socket = match &addr {
        SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
        SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
    };

    socket.set_reuseport(true)?;
    socket.bind(addr)?;
    let listener = socket.listen(1024)?;
    Ok(listener)
}
