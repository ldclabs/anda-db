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
use std::{fs::File, io, net::SocketAddr, sync::Arc, time::Duration};
use structured_logger::{Builder, async_json::new_writer, get_env_level};
use tokio::{signal, task::JoinHandle};
use tokio_util::sync::CancellationToken;

mod handler;
mod nexus;
mod runtime;

use runtime::{ExecutionManager, abort_task, finish_task};

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

    /// Maximum admitted KIP requests, including reads and request-body reception.
    /// The legacy option name is retained for deployment compatibility.
    #[clap(long, env = "MAX_CONCURRENT_MUTATIONS", default_value = "64")]
    max_concurrent_mutations: usize,

    /// Total graceful-shutdown drain deadline in seconds
    #[clap(long, env = "SHUTDOWN_DRAIN_TIMEOUT_SECS", default_value = "300")]
    shutdown_drain_timeout_secs: u64,

    /// Retention window for `execute_kip` audit documents in days. Unbounded retention
    /// (`0`) grows storage and index memory forever and must be chosen
    /// explicitly.
    #[clap(long, env = "LOG_RETENTION_DAYS", default_value = "30")]
    log_retention_days: u64,

    /// Object cache budget for a new database, in bytes (0 disables caching).
    /// Existing databases retain their persisted storage configuration.
    #[clap(long, env = "CACHE_MAX_BYTES", default_value = "67108864")]
    cache_max_bytes: u64,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
/// Storage backend selection for the server.
pub enum Commands {
    /// Use ephemeral in-memory storage (the default); all data is lost on exit.
    Memory,
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

    // Fail before opening storage when another process already serves this port.
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let packages = read_schema_packages(&cli.schema_package)?;
    // Keep the local writer lock alive until every database task has stopped.
    let backend = build_object_store(cli.command)?;

    let db_config = DBConfig {
        name: "anda_db".to_string(),
        description: "Anda DB".to_string(),
        storage: StorageConfig {
            cache_max_bytes: Some(cli.cache_max_bytes),
            compress_level: 3,
            object_chunk_size: 256 * 1024,
            bucket_overload_size: 1024 * 1024,
            max_small_object_size: 1024 * 1024 * 10,
            ..Default::default()
        },
        lock: None,
    };

    let db = Arc::new(AndaDB::connect(backend.store.clone(), db_config).await?);
    let nexus = nexus::Nexus::connect(db.clone(), &packages, cli.max_logged_request_bytes).await?;

    let executions = ExecutionManager::new(cli.max_concurrent_mutations);
    let state = AppState {
        nexus: nexus.clone(),
        name: APP_NAME.to_string(),
        version: APP_VERSION.to_string(),
        request_timeout: Duration::from_secs(cli.request_timeout_secs.max(1)),
        executions: executions.clone(),
    };
    let app = build_router(state, cli.api_key, cli.max_body_size);
    let background_cancel = CancellationToken::new();
    let flush_task = tokio::spawn(periodic_flush(
        db.clone(),
        background_cancel.clone(),
        Duration::from_secs(cli.flush_interval_secs.max(1)),
    ));
    let retention_task = retention_hours.map(|hours| {
        let cancel = background_cancel.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(3600)) => {}
                }
                let before = (unix_ms() / 3_600_000).saturating_sub(hours);
                match nexus.prune_logs(before, &cancel).await {
                    Ok(0) => {}
                    Ok(n) => log::info!("pruned {n} expired KIP logs"),
                    Err(err) => log::error!("failed to prune KIP logs: {err:?}"),
                }
            }
        })
    });

    log::warn!(
        "{APP_NAME}@{APP_VERSION} listening on {:?}",
        listener.local_addr()?
    );
    let stop_accepting = CancellationToken::new();
    let mut server_task = tokio::spawn({
        let stop_accepting = stop_accepting.clone();
        async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(stop_accepting.cancelled_owned())
                .await
        }
    });
    let (server_event, fatal_shutdown) = tokio::select! {
        joined = &mut server_task => (Some(joined), false),
        _ = shutdown_signal() => (None, false),
        _ = executions.admission.cancelled() => (None, true),
    };
    executions.admission.cancel();
    stop_accepting.cancel();
    background_cancel.cancel();
    let deadline =
        tokio::time::Instant::now() + Duration::from_secs(cli.shutdown_drain_timeout_secs.max(1));
    let server_result = match server_event {
        Some(joined) => joined
            .map_err(|err| io::Error::other(format!("HTTP server task failed: {err}")))
            .and_then(|result| result),
        None => finish_task(&mut server_task, deadline, "HTTP server")
            .await
            .and_then(|result| result),
    };
    shutdown_database(
        db,
        executions,
        flush_task,
        retention_task,
        deadline,
        server_result,
    )
    .await?;
    if fatal_shutdown {
        return Err(io::Error::other("KIP execution exceeded its hard deadline").into());
    }
    drop(backend);
    Ok(())
}

/// Stop between flushes; final close is owned by shutdown so its error reaches
/// the process exit status. Never cancel an in-progress flush during normal drain.
async fn periodic_flush(db: Arc<AndaDB>, cancel: CancellationToken, interval: Duration) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(interval) => {}
        }
        if let Err(err) = db.flush().await {
            log::error!("database flush failed: {err}");
        }
    }
}

async fn shutdown_database(
    db: Arc<AndaDB>,
    executions: ExecutionManager,
    mut flush_task: JoinHandle<()>,
    retention_task: Option<JoinHandle<()>>,
    deadline: tokio::time::Instant,
    server_result: io::Result<()>,
) -> io::Result<()> {
    let mut failure = server_result.err();
    if let Some(mut task) = retention_task
        && let Err(err) = finish_task(&mut task, deadline, "retention").await
    {
        log::error!("{err}");
        failure.get_or_insert(err);
    }
    if let Err(err) = executions.drain(deadline).await {
        log::error!("{err}");
        failure.get_or_insert(err);
    }
    if failure.is_some() {
        // A task may have been cancelled mid-write. Leave persisted state for
        // reopen/recovery rather than publishing an arbitrary cancellation point.
        abort_task(&mut flush_task).await;
    } else if let Err(err) = finish_task(&mut flush_task, deadline, "periodic flush").await {
        failure = Some(err);
    }
    if let Some(err) = failure {
        return Err(err);
    }
    let mut close = tokio::spawn(async move { db.close().await });
    finish_task(&mut close, deadline, "database close")
        .await?
        .map_err(io::Error::other)
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
/// choice, since each executed KIP envelope appends a durable document. An
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

/// The lock file remains present after exit. Unlinking it would let another
/// process lock a new inode while a live writer still holds the old one.
struct StorageBackend {
    store: Arc<dyn ObjectStore>,
    _writer_lock: Option<File>,
}

fn build_object_store(command: Option<Commands>) -> Result<StorageBackend, BoxError> {
    match command {
        None | Some(Commands::Memory) => Ok(StorageBackend {
            store: Arc::new(InMemory::new()),
            _writer_lock: None,
        }),
        Some(Commands::Local { db }) => {
            std::fs::create_dir_all(&db)?;
            let path = std::fs::canonicalize(&db)?;
            let lock = File::options()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path.join(".anda-nexus-writer.lock"))?;
            lock.try_lock().map_err(|err| {
                io::Error::other(format!(
                    "cannot acquire the database writer lock for {}: {err}",
                    path.display(),
                ))
            })?;
            let local = LocalFileSystem::new_with_prefix(path)?.with_fsync(true);
            Ok(StorageBackend {
                store: Arc::new(MetaStoreBuilder::new(local, 100_000).build()),
                _writer_lock: Some(lock),
            })
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

    #[tokio::test]
    async fn local_directories_are_created_locked_and_persistent() {
        use object_store::{ObjectStoreExt, path::Path};
        let root = tempfile::tempdir().unwrap();
        for name in ["memory", "in_memory", "nested/new"] {
            let path = root.path().join(name);
            let command = || {
                Some(Commands::Local {
                    db: path.to_str().unwrap().into(),
                })
            };
            let backend = build_object_store(command()).unwrap();
            assert!(path.is_dir());
            assert!(
                build_object_store(command()).is_err(),
                "duplicate local writer must fail"
            );
            let key = Path::from("round-trip");
            backend.store.put(&key, "persisted".into()).await.unwrap();
            drop(backend);
            let reopened = build_object_store(command()).unwrap();
            let bytes = reopened
                .store
                .get(&key)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert_eq!(&bytes[..], b"persisted");
        }
        assert_eq!(
            build_object_store(None).unwrap().store.to_string(),
            "InMemory"
        );
        assert_eq!(
            build_object_store(Some(Commands::Memory))
                .unwrap()
                .store
                .to_string(),
            "InMemory"
        );
    }

    #[tokio::test]
    async fn shutdown_propagates_final_close_failure() {
        use anda_object_store::{FaultOp, FaultRule, FaultStore};
        let (store, fault) = FaultStore::wrap(InMemory::new());
        let db = Arc::new(
            AndaDB::connect(
                Arc::new(store),
                DBConfig {
                    name: "close_failure".into(),
                    description: String::new(),
                    storage: StorageConfig::default(),
                    lock: None,
                },
            )
            .await
            .unwrap(),
        );
        let _nexus = nexus::Nexus::connect(db.clone(), &[], 8192).await.unwrap();
        fault.push_rule(FaultRule::fail_once(FaultOp::Put, ""));
        let result = shutdown_database(
            db,
            ExecutionManager::new(1),
            tokio::spawn(async {}),
            None,
            tokio::time::Instant::now() + Duration::from_secs(5),
            Ok(()),
        )
        .await;
        assert!(
            result.is_err(),
            "final persistence failure must reach the process result"
        );
    }

    #[tokio::test]
    async fn forced_shutdown_skips_close_and_reports_failure() {
        let db = Arc::new(
            AndaDB::connect(
                Arc::new(InMemory::new()),
                DBConfig {
                    name: "forced_shutdown".into(),
                    description: String::new(),
                    storage: StorageConfig::default(),
                    lock: None,
                },
            )
            .await
            .unwrap(),
        );
        let result = shutdown_database(
            db.clone(),
            ExecutionManager::new(1),
            tokio::spawn(async {}),
            Some(tokio::spawn(std::future::pending())),
            tokio::time::Instant::now() + Duration::from_millis(20),
            Ok(()),
        )
        .await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
        // Close sets read-only. The forced path must leave recovery to reopen.
        assert!(!db.is_read_only());
    }
}
