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
//! The database name is extracted from the first path segment after the
//! configured `--path-prefix`: `{prefix}{db_name}/...`. Client-supplied
//! shard headers are ignored; only server-side routing metadata selects a
//! shard. Names must match the backend rules (`[a-z0-9_]{1,64}`).
//!
//! The `read_only` flag on a shard backend is advisory routing metadata: the
//! RPC protocol is POST-based, so the proxy cannot tell reads from writes by
//! HTTP method and does not enforce it; enforcement is up to the backend
//! (e.g. `db.set_read_only`).
//!
//! ## Management API (auth required)
//!
//! | Method   | Path                        | Body                                                | Description                    |
//! |----------|-----------------------------|-----------------------------------------------------|--------------------------------|
//! | `GET`    | `/_admin/db_shards/{db}`    | –                                                   | Get one db→shard binding       |
//! | `PUT`    | `/_admin/db_shards`         | `{"db_name": "mydb", "shard_id": 1}`                | Assign a database to a shard   |
//! | `DELETE` | `/_admin/db_shards`         | `{"db_name": "mydb"}`                               | Remove a db→shard binding      |
//! | `GET`    | `/_admin/shard_backends`    | –                                                   | List all shard backends        |
//! | `PUT`    | `/_admin/shard_backends`    | `{"shard_id": 1, "backend_addr": "http://10.0.0.1:8080", "read_only": false}` | Add or update a shard backend |
//! | `DELETE` | `/_admin/shard_backends`    | `{"shard_id": 1}`                                   | Delete a shard backend         |
//!
//! ## Usage
//!
//! ```bash
//! export DATABASE_URL="postgres://user:pass@localhost/shard_proxy"
//! export API_KEY="my-secret"
//! cargo run -p anda_db_shard_proxy -- --addr 0.0.0.0:8080
//! ```
//!
//! Listening on a non-loopback address requires `API_KEY` (or the explicit
//! `--insecure-no-api-key` override); otherwise the management API would be
//! open to anyone who can reach the proxy.

use axum::{BoxError, body::Body};
use clap::Parser;
use hyper_util::{client::legacy::Client, rt::TokioExecutor};
use mimalloc::MiMalloc;
use sqlx::postgres::PgPoolOptions;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use structured_logger::{Builder, async_json::new_writer, get_env_level};
use tokio::signal;
use tokio_util::sync::CancellationToken;

use anda_db_shard_proxy::handler::build_router;
use anda_db_shard_proxy::proxy::{AppState, validate_backend_addr};
use anda_db_shard_proxy::router;
use anda_db_shard_proxy::store::{ResolvedRoute, ShardStore};

const APP_NAME: &str = env!("CARGO_PKG_NAME");
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

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

    /// API key for management endpoints. Required when listening on a
    /// non-loopback address unless --insecure-no-api-key is passed.
    #[clap(long, env = "API_KEY")]
    api_key: Option<String>,

    /// Allow running without API_KEY on a non-loopback address, leaving the
    /// management API open to anyone who can reach the proxy (dangerous)
    #[clap(long, env = "INSECURE_NO_API_KEY")]
    insecure_no_api_key: bool,

    /// Maximum PostgreSQL connections in the pool
    #[clap(long, env = "PG_MAX_CONNECTIONS", default_value = "5")]
    pg_max_connections: u32,

    /// Timeout in seconds for a proxied request up to the response headers
    /// (connect + send + wait for the backend to start responding); the
    /// response body stream itself is not bounded by this timeout
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
    if matches!(cli.api_key.as_deref(), Some(key) if key.trim().is_empty()) {
        return Err("API_KEY must not be empty".into());
    }

    let addr: SocketAddr = cli.addr.parse()?;
    // Without an API key the management API (which can redirect every
    // tenant's traffic to an arbitrary backend) would be open to anyone who
    // can reach the listener; only allow that on loopback or with an
    // explicit override.
    if cli.api_key.is_none() && !addr.ip().is_loopback() && !cli.insecure_no_api_key {
        return Err(format!(
            "refusing to listen on non-loopback address {addr} without API_KEY: \
             the management API would be open to anyone; set API_KEY or pass \
             --insecure-no-api-key to override"
        )
        .into());
    }

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

    // Reject a misconfigured default backend at startup instead of failing
    // every request with 500/BAD_GATEWAY later.
    let default_backend = match cli.default_backend_addr {
        Some(addr) => {
            validate_backend_addr(&addr).map_err(|e| format!("invalid default backend: {e}"))?;
            Some(ResolvedRoute {
                db_name: None,
                shard_id: 0,
                backend_addr: addr,
                read_only: true,
            })
        }
        None => None,
    };

    let state = AppState {
        store,
        client: Arc::new(http_client),
        api_key: Arc::new(cli.api_key),
        db_name_extractor: Arc::new(router::PrefixExtractor::new(cli.path_prefix.clone())),
        proxy_request_timeout: Duration::from_secs(cli.proxy_request_timeout.max(1) as u64),
        default_backend,
    };

    let app = build_router(state);
    let listener = create_reuse_port_listener(addr).await?;
    let shutdown_token = global_cancel_token.clone();

    log::warn!(
        "{}@{} starting shard proxy on {}",
        APP_NAME,
        APP_VERSION,
        cli.addr
    );
    // `into_make_service_with_connect_info` exposes the client address so the
    // proxy can append it to `X-Forwarded-For`.
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
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
