//! HTTP server for the Anda Cognitive Nexus.
//!
//! The binary opens an AndaDB-backed cognitive nexus, exposes a health/info
//! endpoint, and accepts KIP commands over HTTP at `/kip`.

use anda_db::{
    database::{AndaDB, DBConfig},
    storage::StorageConfig,
    unix_ms,
};
use anda_object_store::MetaStoreBuilder;
use axum::{BoxError, Router, extract::DefaultBodyLimit, middleware, routing};
use clap::{Parser, Subcommand};
use mimalloc::MiMalloc;
use object_store::{ObjectStore, local::LocalFileSystem, memory::InMemory};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use structured_logger::{Builder, async_json::new_writer, get_env_level};
use tokio::signal;
use tokio_util::sync::CancellationToken;

mod handler;
mod nexus;

use handler::*;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const APP_NAME: &str = env!("CARGO_PKG_NAME");
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Port to listen on
    #[clap(long, env = "ADDR", default_value = "127.0.0.1:8080")]
    addr: String,

    /// API key protecting the `/kip` endpoint. Required when listening on
    /// a non-loopback address unless --insecure-no-api-key is passed.
    #[clap(long, env = "API_KEY")]
    api_key: Option<String>,

    /// Allow running without API_KEY on a non-loopback address, leaving the
    /// KIP endpoint (arbitrary graph mutations) open to anyone who can
    /// reach the server (dangerous)
    #[clap(long, env = "INSECURE_NO_API_KEY")]
    insecure_no_api_key: bool,

    /// Reserved principal id injected into the `$self` genesis KML on
    /// first start
    #[clap(long, env = "SELF_PRINCIPAL_ID", default_value = "uuc56-gyb")]
    self_principal_id: String,

    /// Background flush interval in seconds for the database
    #[clap(long, env = "FLUSH_INTERVAL_SECS", default_value = "30")]
    flush_interval_secs: u64,

    /// Per-request processing timeout in seconds for `/kip`
    #[clap(long, env = "REQUEST_TIMEOUT_SECS", default_value = "300")]
    request_timeout_secs: u64,

    /// Maximum accepted request body size in bytes
    #[clap(long, env = "MAX_BODY_SIZE", default_value = "2097152")]
    max_body_size: usize,

    /// Retention window for the `kip_logs` collection in days;
    /// 0 disables pruning (default)
    #[clap(long, env = "LOG_RETENTION_DAYS", default_value = "0")]
    log_retention_days: u64,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
/// Storage backend selection for the server.
pub enum Commands {
    /// Use a local filesystem-backed database.
    Local {
        /// Local database directory.
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
    let addr: SocketAddr = cli.addr.parse()?;
    validate_api_key_policy(cli.api_key.as_deref(), &addr, cli.insecure_no_api_key)?;
    // Initialize structured logging with JSON format
    Builder::with_level(&get_env_level().to_string())
        .with_target_writer("*", new_writer(tokio::io::stdout()))
        .init();

    let object_store = match cli.command {
        Some(Commands::Local { db }) => build_object_store(db)?,
        None => build_object_store("memory".to_string())?,
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
            ..Default::default()
        },
        lock: None,
    };

    let db = Arc::new(AndaDB::connect(object_store.clone(), db_config).await?);
    let nexus = nexus::Nexus::connect(db.clone(), cli.self_principal_id).await?;

    let state = AppState {
        nexus: nexus.clone(),
        name: APP_NAME.to_string(),
        version: APP_VERSION.to_string(),
        api_key: cli.api_key,
        request_timeout: Duration::from_secs(cli.request_timeout_secs.max(1)),
    };
    let app = Router::new()
        .route("/", routing::get(get_information))
        .route("/kip", routing::post(post_kip))
        .layer(DefaultBodyLimit::max(cli.max_body_size.max(1024)))
        .layer(middleware::from_fn(normalize_rejections))
        .with_state(state);
    let cancel_token = CancellationToken::new();

    // Periodic flush of database/collection metadata; when the token is
    // cancelled the task flushes and closes the database before exiting.
    let flush_task = tokio::spawn({
        let db = db.clone();
        let cancel = cancel_token.child_token();
        let interval = Duration::from_secs(cli.flush_interval_secs.max(1));
        async move { db.auto_flush(cancel, interval).await }
    });

    // Optional retention cleanup for the `kip_logs` collection, driven by
    // the indexed `period` field (hours since the Unix epoch).
    if cli.log_retention_days > 0 {
        let nexus = nexus.clone();
        let cancel = cancel_token.child_token();
        let retention_hours = cli.log_retention_days * 24;
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(3600)) => {}
                }
                let before_period = (unix_ms() / 3_600_000).saturating_sub(retention_hours);
                match nexus.prune_logs(before_period).await {
                    Ok(0) => {}
                    Ok(n) => log::info!("pruned {n} expired KIP logs"),
                    Err(err) => log::error!("failed to prune KIP logs: {err:?}"),
                }
            }
        });
    }

    let listener = create_reuse_port_listener(addr).await?;
    log::warn!("{}@{} listening on {:?}", APP_NAME, APP_VERSION, addr);

    let result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(cancel_token.clone()))
        .await;

    // Flush and close the database before exiting, even when the server
    // loop returned an error.
    cancel_token.cancel();
    if let Err(err) = flush_task.await {
        log::error!("flush task failed: {err:?}");
    }
    result?;
    Ok(())
}

/// Refuses insecure listener configurations: `/kip` executes arbitrary KML
/// graph mutations, so a non-loopback listener without an API key must be
/// an explicit opt-in (`--insecure-no-api-key` / `INSECURE_NO_API_KEY`).
/// An empty API key is always rejected.
fn validate_api_key_policy(
    api_key: Option<&str>,
    addr: &SocketAddr,
    insecure_no_api_key: bool,
) -> Result<(), BoxError> {
    if matches!(api_key, Some(key) if key.trim().is_empty()) {
        return Err("API_KEY must not be empty".into());
    }
    if api_key.is_none() && !addr.ip().is_loopback() && !insecure_no_api_key {
        return Err(format!(
            "refusing to listen on non-loopback address {addr} without API_KEY: \
             the KIP endpoint would be open to anyone; set API_KEY or pass \
             --insecure-no-api-key to override"
        )
        .into());
    }
    Ok(())
}

/// Waits for a process termination signal and triggers graceful shutdown.
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

/// Creates a TCP listener with `SO_REUSEPORT` enabled.
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

fn build_object_store(ty: String) -> Result<Arc<dyn ObjectStore>, BoxError> {
    match ty.as_str() {
        "" | "memory" | "in_memory" => Ok(Arc::new(InMemory::new())),
        path => {
            let os = LocalFileSystem::new_with_prefix(path)?;
            let os = MetaStoreBuilder::new(os, 100000).build();
            Ok(Arc::new(os))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn api_key_policy_rejects_empty_key_everywhere() {
        for listen in ["127.0.0.1:8080", "0.0.0.0:8080"] {
            assert!(validate_api_key_policy(Some(""), &addr(listen), false).is_err());
            assert!(validate_api_key_policy(Some("  "), &addr(listen), true).is_err());
        }
    }

    #[test]
    fn api_key_policy_refuses_non_loopback_without_key() {
        for listen in ["0.0.0.0:8080", "192.168.1.10:8080", "[::]:8080"] {
            let err = validate_api_key_policy(None, &addr(listen), false)
                .expect_err("non-loopback without API_KEY must be refused");
            assert!(err.to_string().contains("--insecure-no-api-key"));
            // The explicit escape hatch allows it.
            assert!(validate_api_key_policy(None, &addr(listen), true).is_ok());
            // A real API key allows it.
            assert!(validate_api_key_policy(Some("secret"), &addr(listen), false).is_ok());
        }
    }

    #[test]
    fn api_key_policy_allows_loopback_without_key() {
        for listen in ["127.0.0.1:8080", "[::1]:8080"] {
            assert!(validate_api_key_policy(None, &addr(listen), false).is_ok());
        }
    }
}
