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
use axum::BoxError;
use clap::{Parser, Subcommand};
use mimalloc::MiMalloc;
use object_store::{ObjectStore, local::LocalFileSystem, memory::InMemory};
use std::{io, net::SocketAddr, sync::Arc, time::Duration};
use structured_logger::{Builder, async_json::new_writer, get_env_level};
use tokio::{
    signal,
    sync::{Mutex, Semaphore},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

mod handler;
mod nexus;

use handler::*;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const APP_NAME: &str = env!("CARGO_PKG_NAME");
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
/// Bounded cleanup window after the graceful deadline has already forced an
/// abort. This is not an extension of the graceful drain contract.
const FORCED_ABORT_JOIN_TIMEOUT: Duration = Duration::from_secs(1);

/// The hard execution deadline is this multiple of the per-request response
/// deadline. It is derived instead of separately configurable so the two can
/// never be set inconsistently: the response deadline must always fire first,
/// and the hard deadline exists only to reclaim a bounded mutation permit
/// from an execution that is never going to finish.
const EXECUTION_TIMEOUT_FACTOR: u32 = 4;

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

    /// Additional Schema Package artifacts (JSON files) to install and
    /// activate in the default MemorySpace, on top of the bundled
    /// cognitive-memory profile. Repeat the flag, or separate paths with
    /// commas in the environment variable.
    #[clap(long, env = "SCHEMA_PACKAGE", value_delimiter = ',')]
    schema_package: Vec<String>,

    /// Background flush interval in seconds for the database
    #[clap(long, env = "FLUSH_INTERVAL_SECS", default_value = "30")]
    flush_interval_secs: u64,

    /// Per-request processing timeout in seconds for `/kip`
    #[clap(long, env = "REQUEST_TIMEOUT_SECS", default_value = "300")]
    request_timeout_secs: u64,

    /// Maximum accepted request body size in bytes
    #[clap(long, env = "MAX_BODY_SIZE", default_value = "2097152")]
    max_body_size: usize,

    /// Maximum size in bytes of the KIP request stored in one `kip_logs`
    /// audit document. A larger request is stored truncated. Raise it to
    /// `--max-body-size` to keep full request bodies.
    #[clap(long, env = "MAX_LOGGED_REQUEST_BYTES", default_value = "8192")]
    max_logged_request_bytes: usize,

    /// Maximum number of concurrently executing KIP mutations
    #[clap(long, env = "MAX_CONCURRENT_MUTATIONS", default_value = "64")]
    max_concurrent_mutations: usize,

    /// Total graceful-shutdown drain deadline in seconds
    #[clap(long, env = "SHUTDOWN_DRAIN_TIMEOUT_SECS", default_value = "300")]
    shutdown_drain_timeout_secs: u64,

    /// Retention window for the `kip_logs` collection in days. Every `/kip`
    /// request appends a durable audit document, so unbounded retention
    /// (`0`) grows storage and index memory forever and must be chosen
    /// explicitly.
    #[clap(long, env = "LOG_RETENTION_DAYS", default_value = "30")]
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
    // Reject an out-of-range retention window before anything is opened: an
    // unchecked `days * 24` wraps and would prune almost the whole audit log.
    let retention_hours = retention_hours(cli.log_retention_days)?;
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

    let packages = read_schema_packages(&cli.schema_package)?;
    let db = Arc::new(AndaDB::connect(object_store.clone(), db_config).await?);
    let nexus = nexus::Nexus::connect(db.clone(), &packages, cli.max_logged_request_bytes).await?;

    let admission = CancellationToken::new();
    let mutation_tasks = TaskTracker::new();
    let mutation_aborts = Arc::new(Mutex::new(Vec::new()));
    let request_timeout = Duration::from_secs(cli.request_timeout_secs.max(1));
    let state = AppState {
        nexus: nexus.clone(),
        name: APP_NAME.to_string(),
        version: APP_VERSION.to_string(),
        request_timeout,
        execution_timeout: request_timeout.saturating_mul(EXECUTION_TIMEOUT_FACTOR),
        admission: admission.clone(),
        mutation_tasks: mutation_tasks.clone(),
        mutation_permits: Arc::new(Semaphore::new(cli.max_concurrent_mutations.max(1))),
        mutation_aborts: mutation_aborts.clone(),
    };
    let app = build_router(state, cli.api_key, cli.max_body_size);
    let auto_flush_cancel = CancellationToken::new();
    let retention_cancel = CancellationToken::new();

    // Periodic flush of database/collection metadata; when the token is
    // cancelled the task flushes and closes the database before exiting.
    let mut flush_task = tokio::spawn({
        let db = db.clone();
        let cancel = auto_flush_cancel.child_token();
        let interval = Duration::from_secs(cli.flush_interval_secs.max(1));
        async move { db.auto_flush(cancel, interval).await }
    });

    // Optional retention cleanup for the `kip_logs` collection, driven by
    // the indexed `period` field (hours since the Unix epoch).
    let retention_task = if let Some(retention_hours) = retention_hours {
        let nexus = nexus.clone();
        let cancel = retention_cancel.child_token();
        Some(tokio::spawn(async move {
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
        }))
    } else {
        None
    };

    let listener = create_reuse_port_listener(addr).await?;
    log::warn!("{}@{} listening on {:?}", APP_NAME, APP_VERSION, addr);

    let stop_accepting = CancellationToken::new();
    let mut server_task = tokio::spawn({
        let stop_accepting = stop_accepting.clone();
        async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(stop_accepting.cancelled_owned())
                .await
        }
    });
    let drain_timeout = Duration::from_secs(cli.shutdown_drain_timeout_secs.max(1));
    let (server_event, fatal_shutdown) = tokio::select! {
        joined = &mut server_task => (Some(joined), false),
        _ = shutdown_signal() => (None, false),
        _ = admission.cancelled() => {
            log::error!(
                "a KIP mutation exceeded its hard execution deadline; starting process shutdown"
            );
            (None, true)
        }
    };
    let shutdown_deadline = tokio::time::Instant::now() + drain_timeout;
    let (server_forced_abort, mut result) = match server_event {
        Some(joined) => {
            admission.cancel();
            retention_cancel.cancel();
            match joined {
                Ok(result) => (false, result),
                Err(err) => (
                    true,
                    Err(io::Error::other(format!("HTTP server task failed: {err}"))),
                ),
            }
        }
        None => {
            admission.cancel();
            retention_cancel.cancel();
            stop_accepting.cancel();
            match tokio::time::timeout_at(shutdown_deadline, &mut server_task).await {
                Ok(joined) => match joined {
                    Ok(result) => (false, result),
                    Err(err) => (
                        true,
                        Err(io::Error::other(format!("HTTP server task failed: {err}"))),
                    ),
                },
                Err(_) => {
                    log::error!(
                        "HTTP handler drain deadline exceeded; aborting remaining handlers"
                    );
                    server_task.abort();
                    if tokio::time::timeout(FORCED_ABORT_JOIN_TIMEOUT, &mut server_task)
                        .await
                        .is_err()
                    {
                        log::error!(
                            "aborted HTTP server task did not terminate before cleanup deadline"
                        );
                    }
                    (true, Ok(()))
                }
            }
        }
    };
    if fatal_shutdown && result.is_ok() {
        // Exit non-zero so supervisors configured with an on-failure restart
        // policy replace the crash-recoverable process.
        result = Err(io::Error::other(
            "KIP mutation exceeded the hard execution deadline",
        ));
    }

    // No new handler can spawn a mutation after the HTTP server has drained.
    // Stop retention, then drain every detached non-cancel-safe mutation
    // before allowing auto_flush to close the database.
    let mut forced_crash = server_forced_abort;
    if let Some(mut task) = retention_task {
        match tokio::time::timeout_at(shutdown_deadline, &mut task).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                log::error!("retention task failed during shutdown: {err:?}");
                forced_crash = true;
            }
            Err(_) => {
                log::error!("retention drain deadline exceeded; aborting retention task");
                forced_crash = true;
                task.abort();
                if tokio::time::timeout(FORCED_ABORT_JOIN_TIMEOUT, &mut task)
                    .await
                    .is_err()
                {
                    log::error!("aborted retention task did not terminate before cleanup deadline");
                }
            }
        }
    }
    // Serialize TaskTracker::close with the handler's final admission check,
    // spawn, and abort-handle registration. TaskTracker::close alone does not
    // reject later spawns.
    {
        let _registration = mutation_aborts.lock().await;
        mutation_tasks.close();
    }
    if tokio::time::timeout_at(shutdown_deadline, mutation_tasks.wait())
        .await
        .is_err()
    {
        log::error!("KIP mutation drain deadline exceeded; aborting remaining mutations");
        forced_crash = true;
        let handles = mutation_aborts.lock().await;
        for handle in handles.iter().filter(|handle| !handle.is_finished()) {
            handle.abort();
        }
        drop(handles);
        if tokio::time::timeout(FORCED_ABORT_JOIN_TIMEOUT, mutation_tasks.wait())
            .await
            .is_err()
        {
            log::error!("aborted KIP mutations did not terminate before cleanup deadline");
        }
    }

    if forced_crash {
        // Never publish an arbitrary cancellation point through a graceful
        // close. Abort auto-flush and leave the database for crash recovery.
        flush_task.abort();
        if tokio::time::timeout(FORCED_ABORT_JOIN_TIMEOUT, &mut flush_task)
            .await
            .is_err()
        {
            log::error!("aborted auto-flush task did not terminate before cleanup deadline");
        }
    } else {
        // Every task that can mutate the database drained normally. Give the
        // final flush/close only the time remaining in the same total
        // shutdown budget; if it overruns, stop at a crash-recoverable point.
        auto_flush_cancel.cancel();
        match tokio::time::timeout_at(shutdown_deadline, &mut flush_task).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => log::error!("flush task failed: {err:?}"),
            Err(_) => {
                log::error!("database close exceeded the total shutdown deadline; aborting");
                flush_task.abort();
                if tokio::time::timeout(FORCED_ABORT_JOIN_TIMEOUT, &mut flush_task)
                    .await
                    .is_err()
                {
                    log::error!("aborted database close did not terminate before cleanup deadline");
                }
            }
        }
    }
    result?;
    Ok(())
}

/// Reads the operator-supplied Schema Package artifacts.
///
/// Read before the database is opened: a missing or unreadable package must
/// stop the process at startup, not after a Space is already serving. The
/// artifacts are not parsed here — `Nexus::connect` does that, so one code path
/// owns what "a valid package" means.
fn read_schema_packages(paths: &[String]) -> Result<Vec<nexus::SchemaPackageSource>, BoxError> {
    paths
        .iter()
        .map(|path| {
            let artifact = std::fs::read_to_string(path)
                .map_err(|err| format!("failed to read schema package {path:?}: {err}"))?;
            Ok(nexus::SchemaPackageSource {
                source: path.clone(),
                artifact,
            })
        })
        .collect()
}

/// Converts the configured retention window from days to hours.
///
/// `None` means "keep every audit log forever" — an explicit operator
/// choice, since every `/kip` request appends a durable document. An
/// out-of-range value is refused at startup instead of wrapping: an
/// unchecked `days * 24` turns `768614336404564651` into `8`, which would
/// silently prune essentially the whole audit log (and panic in a debug
/// build).
fn retention_hours(days: u64) -> Result<Option<u64>, BoxError> {
    if days == 0 {
        return Ok(None);
    }
    match days.checked_mul(24) {
        Some(hours) => Ok(Some(hours)),
        None => Err(format!(
            "LOG_RETENTION_DAYS={days} is out of range: it must not exceed {}",
            u64::MAX / 24
        )
        .into()),
    }
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
pub async fn shutdown_signal() {
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

    /// An unchecked `days * 24` wraps modulo 2^64, so an operator typo could
    /// turn a huge retention window into a few hours and delete essentially
    /// the whole audit log (or panic at startup in a debug build).
    #[test]
    fn retention_window_rejects_out_of_range_days() {
        assert_eq!(retention_hours(0).unwrap(), None);
        assert_eq!(retention_hours(1).unwrap(), Some(24));
        assert_eq!(retention_hours(30).unwrap(), Some(720));
        assert_eq!(
            retention_hours(u64::MAX / 24).unwrap(),
            Some(u64::MAX / 24 * 24)
        );

        // 768614336404564651 * 24 wraps to 8.
        for days in [768_614_336_404_564_651u64, u64::MAX / 24 + 1, u64::MAX] {
            let err = retention_hours(days)
                .err()
                .unwrap_or_else(|| panic!("{days} must be refused"))
                .to_string();
            assert!(err.contains("out of range"), "unexpected error: {err}");
        }
    }
}
