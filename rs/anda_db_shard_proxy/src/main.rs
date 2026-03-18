//! # Anda DB Shard Proxy
//!
//! A reverse proxy that routes incoming HTTP requests to the correct database
//! shard instance based on a shared routing table stored in PostgreSQL.
//!
//! ## Architecture
//!
//! ```text
//!  ┌──────────┐       ┌──────────────────┐       ┌────────────────┐
//!  │  Client  │──────▶│  Shard Proxy (N) │──────▶│  DB Shard 0    │
//!  └──────────┘       │                  │       ├────────────────┤
//!                     │  ┌────────────┐  │       │  DB Shard 1    │
//!                     │  │ In-memory  │  │       ├────────────────┤
//!                     │  │ DashMap    │  │       │  DB Shard 2    │
//!                     │  └─────┬──────┘  │       └────────────────┘
//!                     │        │         │
//!                     │  ┌─────▼──────┐  │
//!                     │  │ PostgreSQL │  │  (LISTEN/NOTIFY for sync)
//!                     │  └────────────┘  │
//!                     └──────────────────┘
//! ```
//!
//! ## Request Routing
//!
//! The database name is extracted from:
//! 1. The first path segment: `/{db_name}/...`
//! 2. Or the `X-Database` header
//!
//! ## Management API (auth required)
//!
//! | Method   | Path             | Description              |
//! |----------|------------------|--------------------------|
//! | `GET`    | `/_admin/shards` | List all shard entries   |
//! | `PUT`    | `/_admin/shards` | Add or update a shard    |
//! | `DELETE` | `/_admin/shards` | Delete a shard entry     |
//!
//! ### PUT body
//! ```json
//! {"db_name": "mydb", "shard_id": 1, "backend_addr": "http://10.0.0.1:8080"}
//! ```
//!
//! ### DELETE body
//! ```json
//! {"db_name": "mydb"}
//! ```
//!
//! ## Usage
//!
//! ```bash
//! export DATABASE_URL="postgres://user:pass@localhost/shard_proxy"
//! export API_KEY="my-secret"
//! cargo run -p anda_db_shard_proxy -- --addr 0.0.0.0:8080
//! ```

use axum::{BoxError, body::Body};
use clap::Parser;
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use sqlx::postgres::PgPoolOptions;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use structured_logger::{Builder, async_json::new_writer, get_env_level};
use tokio::signal;
use tokio_util::sync::CancellationToken;

use anda_db_shard_proxy::handler::build_router;
use anda_db_shard_proxy::proxy::AppState;
use anda_db_shard_proxy::router;
use anda_db_shard_proxy::store::{ResolvedRoute, ShardStore};

const APP_NAME: &str = env!("CARGO_PKG_NAME");
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser)]
#[command(author, version, about = "Anda DB Shard Routing Proxy")]
struct Cli {
    /// Address and port to listen on
    #[clap(long, env = "ADDR", default_value = "127.0.0.1:8080")]
    addr: String,

    /// PostgreSQL connection URL
    /// The password should be URL-encoded if it contains special characters.
    #[clap(long, env = "DATABASE_URL")]
    database_url: String,

    /// Optional path prefix to strip when extracting the database name from the URL.
    /// For example, with `--path-prefix /db/`, a request to `/db/mydb/query` would extract `mydb` as the database name.
    #[clap(long, env = "PATH_PREFIX", default_value = "/")]
    path_prefix: String,

    /// API key for management endpoints (optional but recommended)
    #[clap(long, env = "API_KEY")]
    api_key: Option<String>,

    /// Maximum PostgreSQL connections in the pool
    #[clap(long, env = "PG_MAX_CONNECTIONS", default_value = "5")]
    pg_max_connections: u32,

    /// Maximum timeout for proxy requests in seconds
    #[clap(long, env = "PROXY_REQUEST_TIMEOUT", default_value = "300")]
    proxy_request_timeout: u32,

    /// Default backend address to use if no shard mapping is found
    #[clap(long, env = "DEFAULT_BACKEND_ADDR")]
    default_backend_addr: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    dotenv::dotenv().ok();
    let cli = Cli::parse();

    Builder::with_level(&get_env_level().to_string())
        .with_target_writer("*", new_writer(tokio::io::stdout()))
        .init();

    // Create global cancellation token for graceful shutdown
    let global_cancel_token = CancellationToken::new();

    // Connect to PostgreSQL
    let pool = PgPoolOptions::new()
        .max_connections(cli.pg_max_connections)
        .connect(&cli.database_url)
        .await?;
    log::warn!("connected to PostgreSQL");

    // Initialize the shard store (creates table if needed + loads cache)
    let store = ShardStore::new(pool).await?;

    // Spawn the LISTEN/NOTIFY background listener for cross-instance sync
    store
        .clone()
        .spawn_listener(global_cancel_token.child_token());

    // Build HTTP client for proxying
    let http_client: Client<_, Body> = Client::builder(TokioExecutor::new())
        .http2_only(false)
        .build_http();

    let state = AppState {
        store,
        client: Arc::new(http_client),
        api_key: Arc::new(cli.api_key),
        db_name_extractor: Arc::new(router::PrefixExtractor {
            prefix: cli.path_prefix.clone(),
        }),
        proxy_request_timeout: Duration::from_secs(cli.proxy_request_timeout as u64),
        default_backend: cli.default_backend_addr.map(|addr| ResolvedRoute {
            db_name: None,
            shard_id: 0,
            backend_addr: addr,
            read_only: true,
        }),
    };

    let app = build_router(state);
    let addr: SocketAddr = cli.addr.parse()?;
    let listener = create_reuse_port_listener(addr).await?;
    let shutdown_token = global_cancel_token.clone();

    log::warn!(
        "{}@{} starting shard proxy on {}",
        APP_NAME,
        APP_VERSION,
        cli.addr
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown_token))
        .await?;

    log::warn!("shut down gracefully");
    Ok(())
}

async fn shutdown_signal(cancel_token: CancellationToken) {
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
