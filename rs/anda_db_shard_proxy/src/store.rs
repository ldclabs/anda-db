//! PostgreSQL-backed routing metadata storage for the shard proxy.
//!
//! The store keeps two layers of routing information:
//! - database-to-shard assignments, which are effectively stable identifiers
//! - shard-to-backend assignments, which can change during upgrades or moves
//!
//! A local [`DashMap`] cache is used for fast request-time lookups, while
//! PostgreSQL remains the source of truth and distributes incremental updates
//! through `LISTEN/NOTIFY`.

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::postgres::PgListener;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Mapping from database name to its assigned shard ID.
/// Once established, this binding is permanent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbShard {
    /// Logical database name used by clients.
    pub db_name: String,
    /// Target shard identifier that owns the database.
    pub shard_id: u32,
}

/// Mapping from shard ID to its current backend address.
/// This binding can change, e.g. during instance upgrades or migrations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardBackend {
    /// Stable shard identifier.
    pub shard_id: u32,
    /// Base URL of the shard backend that should receive proxied traffic.
    pub backend_addr: String,
    /// If true, the backend is in read-only mode (e.g. during migration).
    #[serde(default)]
    pub read_only: bool,
}

/// Fully resolved routing information returned to the proxy layer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedRoute {
    /// Resolved database name when the lookup started from a logical database.
    pub db_name: Option<String>,
    /// Shard identifier selected for the request.
    pub shard_id: u32,
    /// Backend base URL that will receive the forwarded request.
    pub backend_addr: String,
    /// Whether the selected backend currently advertises read-only status.
    pub read_only: bool,
}

// Incremental events sent via PostgreSQL NOTIFY payloads.
#[derive(Serialize, Deserialize)]
#[serde(tag = "op")]
enum DbShardEvent {
    #[serde(rename = "assign")]
    Assign { db_name: String, shard_id: u32 },
    #[serde(rename = "unassign")]
    Unassign { db_name: String },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "op")]
enum BackendEvent {
    #[serde(rename = "upsert")]
    Upsert {
        shard_id: u32,
        backend_addr: String,
        read_only: bool,
    },
    #[serde(rename = "delete")]
    Delete { shard_id: u32 },
}

/// Persistent shard routing store backed by PostgreSQL with in-memory DashMap caches.
///
/// Two-table design:
/// - `db_shards`: db_name → shard_id (large, immutable once set)
/// - `shard_backends`: shard_id → backend_addr (small, mutable for upgrades)
///
/// Uses PostgreSQL `LISTEN/NOTIFY` so that multiple proxy instances stay in sync.
#[derive(Clone)]
pub struct ShardStore {
    pool: PgPool,
    /// db_name → shard_id
    db_cache: Arc<DashMap<String, u32>>,
    /// shard_id → ShardBackend
    backend_cache: Arc<DashMap<u32, ShardBackend>>,
}

impl ShardStore {
    /// Create the store, ensure tables exist, and load the initial data into caches.
    pub async fn new(pool: PgPool) -> Result<Self, sqlx::Error> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS db_shards (
                db_name     TEXT    PRIMARY KEY,
                shard_id    INT     NOT NULL,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            "#,
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS shard_backends (
                shard_id      INT     PRIMARY KEY,
                backend_addr  TEXT    NOT NULL,
                read_only     BOOLEAN NOT NULL DEFAULT false,
                updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            "#,
        )
        .execute(&pool)
        .await?;

        let store = Self {
            pool,
            db_cache: Arc::new(DashMap::new()),
            backend_cache: Arc::new(DashMap::new()),
        };
        store.reload_all().await?;
        Ok(store)
    }

    // ── Cache reload ────────────────────────────────────────────────────────

    async fn reload_all(&self) -> Result<(), sqlx::Error> {
        self.reload_backend_cache().await?;
        Ok(())
    }

    /// Reload the shard-backend cache from PostgreSQL.
    ///
    /// This is primarily used during startup and listener reconnects so the
    /// proxy can recover from missed notifications while it was offline.
    async fn reload_backend_cache(&self) -> Result<(), sqlx::Error> {
        let rows: Vec<(i32, String, bool)> =
            sqlx::query_as("SELECT shard_id, backend_addr, read_only FROM shard_backends")
                .fetch_all(&self.pool)
                .await?;
        self.backend_cache.clear();
        for (shard_id, backend_addr, read_only) in rows {
            self.backend_cache.insert(
                shard_id as u32,
                ShardBackend {
                    shard_id: shard_id as u32,
                    backend_addr,
                    read_only,
                },
            );
        }
        Ok(())
    }

    // ── Lookups ─────────────────────────────────────────────────────────────

    /// Resolve a database name to its full route (shard + backend).
    ///
    /// On cache miss, queries the database and caches the result.
    pub async fn resolve(&self, db_name: &str) -> Option<ResolvedRoute> {
        let shard_id = match self.db_cache.get(db_name) {
            Some(entry) => *entry,
            None => {
                let row: Option<(i32,)> =
                    sqlx::query_as("SELECT shard_id FROM db_shards WHERE db_name = $1")
                        .bind(db_name)
                        .fetch_optional(&self.pool)
                        .await
                        .ok()?;
                let (sid,) = row?;
                let shard_id = sid as u32;
                self.db_cache.insert(db_name.to_string(), shard_id);
                shard_id
            }
        };
        let backend = self.backend_cache.get(&shard_id)?;
        Some(ResolvedRoute {
            db_name: Some(db_name.to_string()),
            shard_id,
            backend_addr: backend.backend_addr.clone(),
            read_only: backend.read_only,
        })
    }

    /// Resolve routing information directly from a shard identifier.
    ///
    /// This path is used when the client already knows the target shard and
    /// sends `Shard-ID` or `X-Shard` instead of a database name.
    pub async fn resolve_by_shard(&self, shard_id: u32) -> Option<ResolvedRoute> {
        let backend = self.backend_cache.get(&shard_id)?;
        Some(ResolvedRoute {
            db_name: None,
            shard_id,
            backend_addr: backend.backend_addr.clone(),
            read_only: backend.read_only,
        })
    }

    /// Fetch one database-to-shard assignment.
    ///
    /// The lookup uses the cache first and falls back to PostgreSQL on a miss.
    /// If PostgreSQL returns a row, the cache is populated before returning.
    pub async fn get_db_shard(&self, db_name: &str) -> Option<DbShard> {
        match self.db_cache.get(db_name) {
            Some(entry) => Some(DbShard {
                db_name: db_name.to_string(),
                shard_id: *entry,
            }),
            None => {
                let row: Option<(i32,)> =
                    sqlx::query_as("SELECT shard_id FROM db_shards WHERE db_name = $1")
                        .bind(db_name)
                        .fetch_optional(&self.pool)
                        .await
                        .ok()?;
                let (sid,) = row?;
                let shard_id = sid as u32;
                self.db_cache.insert(db_name.to_string(), shard_id);
                Some(DbShard {
                    db_name: db_name.to_string(),
                    shard_id,
                })
            }
        }
    }

    /// Return a snapshot of all cached shard backend entries.
    ///
    /// This method is used by the administrative API and intentionally reads
    /// from the in-memory cache so it stays cheap at request time.
    pub fn list_shard_backends(&self) -> Vec<ShardBackend> {
        self.backend_cache
            .iter()
            .map(|e| e.value().clone())
            .collect()
    }

    // ── db_shards mutations (permanent bindings) ────────────────────────────

    /// Assign a database to a shard.
    ///
    /// The insert is idempotent: if the database already exists, the existing
    /// binding is kept and no reassignment is performed.
    pub async fn assign_db(&self, db_name: &str, shard_id: u32) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO db_shards (db_name, shard_id) VALUES ($1, $2) \
             ON CONFLICT (db_name) DO NOTHING",
        )
        .bind(db_name)
        .bind(shard_id as i32)
        .execute(&self.pool)
        .await?;

        self.db_cache.insert(db_name.to_string(), shard_id);
        self.notify(
            "db_shards_changed",
            &DbShardEvent::Assign {
                db_name: db_name.to_string(),
                shard_id,
            },
        )
        .await?;
        Ok(())
    }

    /// Remove a database-to-shard binding.
    ///
    /// Returns `true` when a row was deleted and `false` when the binding was
    /// already absent.
    pub async fn unassign_db(&self, db_name: &str) -> Result<bool, sqlx::Error> {
        let result = sqlx::query("DELETE FROM db_shards WHERE db_name = $1")
            .bind(db_name)
            .execute(&self.pool)
            .await?;

        self.db_cache.remove(db_name);
        self.notify(
            "db_shards_changed",
            &DbShardEvent::Unassign {
                db_name: db_name.to_string(),
            },
        )
        .await?;
        Ok(result.rows_affected() > 0)
    }

    // ── shard_backends mutations (dynamic bindings) ─────────────────────────

    /// Add or update a shard backend entry.
    ///
    /// This operation is intentionally mutable so operators can redirect a
    /// shard to a new backend during maintenance, failover, or migration.
    pub async fn upsert_backend(&self, backend: &ShardBackend) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO shard_backends (shard_id, backend_addr, read_only, updated_at)
            VALUES ($1, $2, $3, now())
            ON CONFLICT (shard_id) DO UPDATE
                SET backend_addr = EXCLUDED.backend_addr,
                    read_only = EXCLUDED.read_only,
                    updated_at = now()
            "#,
        )
        .bind(backend.shard_id as i32)
        .bind(&backend.backend_addr)
        .bind(backend.read_only)
        .execute(&self.pool)
        .await?;

        self.backend_cache.insert(backend.shard_id, backend.clone());
        self.notify(
            "shard_backends_changed",
            &BackendEvent::Upsert {
                shard_id: backend.shard_id,
                backend_addr: backend.backend_addr.clone(),
                read_only: backend.read_only,
            },
        )
        .await?;
        Ok(())
    }

    /// Remove a shard backend entry.
    ///
    /// Returns `true` when the entry existed and was removed.
    pub async fn delete_backend(&self, shard_id: u32) -> Result<bool, sqlx::Error> {
        let result = sqlx::query("DELETE FROM shard_backends WHERE shard_id = $1")
            .bind(shard_id as i32)
            .execute(&self.pool)
            .await?;

        self.backend_cache.remove(&shard_id);
        self.notify("shard_backends_changed", &BackendEvent::Delete { shard_id })
            .await?;
        Ok(result.rows_affected() > 0)
    }

    // ── NOTIFY / LISTEN ─────────────────────────────────────────────────────

    /// Send a PostgreSQL `NOTIFY` event so other proxy instances can refresh
    /// their in-memory caches without polling.
    async fn notify<T: Serialize>(&self, channel: &str, event: &T) -> Result<(), sqlx::Error> {
        let payload = serde_json::to_string(event).unwrap_or_default();
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(channel)
            .bind(&payload)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Apply a database-assignment event received from PostgreSQL.
    fn apply_db_event(&self, payload: &str) {
        match serde_json::from_str::<DbShardEvent>(payload) {
            Ok(DbShardEvent::Assign { db_name, shard_id }) => {
                self.db_cache.insert(db_name, shard_id);
            }
            Ok(DbShardEvent::Unassign { db_name }) => {
                self.db_cache.remove(&db_name);
            }
            Err(e) => {
                log::warn!("failed to parse db_shards_changed payload: {}", e);
            }
        }
    }

    /// Apply a shard-backend event received from PostgreSQL.
    fn apply_backend_event(&self, payload: &str) {
        match serde_json::from_str::<BackendEvent>(payload) {
            Ok(BackendEvent::Upsert {
                shard_id,
                backend_addr,
                read_only,
            }) => {
                self.backend_cache.insert(
                    shard_id,
                    ShardBackend {
                        shard_id,
                        backend_addr,
                        read_only,
                    },
                );
            }
            Ok(BackendEvent::Delete { shard_id }) => {
                self.backend_cache.remove(&shard_id);
            }
            Err(e) => {
                log::warn!("failed to parse shard_backends_changed payload: {}", e);
            }
        }
    }

    /// Spawn a background task that listens for PostgreSQL NOTIFY events
    /// and applies incremental cache updates when the routing table changes.
    pub fn spawn_listener(self, cancel: CancellationToken) {
        tokio::spawn(async move {
            loop {
                if let Err(e) = self.listen_loop(&cancel).await {
                    log::error!("pg listener error, reconnecting: {}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
                if cancel.is_cancelled() {
                    return;
                }
            }
        });
    }

    /// Listen for PostgreSQL notifications until cancelled or the connection
    /// fails, in which case the caller can reconnect.
    async fn listen_loop(&self, cancel: &CancellationToken) -> Result<(), sqlx::Error> {
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener
            .listen_all(["db_shards_changed", "shard_backends_changed"])
            .await?;

        // Reload backends on (re)connect to catch any events missed during downtime.
        if let Err(e) = self.reload_backend_cache().await {
            log::error!("failed to reload backend cache on connect: {}", e);
        }

        loop {
            tokio::select! {
                notification = listener.recv() => {
                    match notification {
                        Ok(n) => {
                            let channel = n.channel();
                            let payload = n.payload();
                            log::info!("received notify on {}", channel);
                            match channel {
                                "db_shards_changed" => self.apply_db_event(payload),
                                "shard_backends_changed" => self.apply_backend_event(payload),
                                _ => {}
                            }
                        }
                        Err(e) => return Err(e),
                    }
                }
                _ = cancel.cancelled() => return Ok(()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use std::collections::HashMap;

    fn test_store() -> ShardStore {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://user:pass@localhost/test")
            .expect("connect_lazy should parse URL");
        ShardStore {
            pool,
            db_cache: Arc::new(DashMap::new()),
            backend_cache: Arc::new(DashMap::new()),
        }
    }

    #[tokio::test]
    async fn apply_db_event_assign_and_unassign_updates_cache() {
        let store = test_store();

        store.apply_db_event(r#"{"op":"assign","db_name":"db_a","shard_id":3}"#);
        assert_eq!(store.db_cache.get("db_a").map(|v| *v), Some(3));

        store.apply_db_event(r#"{"op":"unassign","db_name":"db_a"}"#);
        assert!(store.db_cache.get("db_a").is_none());
    }

    #[tokio::test]
    async fn apply_db_event_invalid_payload_does_not_change_cache() {
        let store = test_store();
        store.db_cache.insert("db_keep".to_string(), 9);

        store.apply_db_event("not-json");

        assert_eq!(store.db_cache.get("db_keep").map(|v| *v), Some(9));
        assert_eq!(store.db_cache.len(), 1);
    }

    #[tokio::test]
    async fn apply_backend_event_upsert_delete_and_resolve_by_shard() {
        let store = test_store();

        store.apply_backend_event(
            r#"{"op":"upsert","shard_id":7,"backend_addr":"http://127.0.0.1:7000","read_only":true}"#,
        );

        let resolved = store
            .resolve_by_shard(7)
            .await
            .expect("route should be resolved after upsert");
        assert_eq!(resolved.db_name, None);
        assert_eq!(resolved.shard_id, 7);
        assert_eq!(resolved.backend_addr, "http://127.0.0.1:7000");
        assert!(resolved.read_only);

        store.apply_backend_event(r#"{"op":"delete","shard_id":7}"#);
        assert!(store.resolve_by_shard(7).await.is_none());
    }

    #[tokio::test]
    async fn list_shard_backends_returns_cached_items() {
        let store = test_store();

        store.backend_cache.insert(
            1,
            ShardBackend {
                shard_id: 1,
                backend_addr: "http://127.0.0.1:8001".to_string(),
                read_only: false,
            },
        );
        store.backend_cache.insert(
            2,
            ShardBackend {
                shard_id: 2,
                backend_addr: "http://127.0.0.1:8002".to_string(),
                read_only: true,
            },
        );

        let backends = store.list_shard_backends();
        assert_eq!(backends.len(), 2);

        let by_id: HashMap<u32, ShardBackend> = backends
            .into_iter()
            .map(|backend| (backend.shard_id, backend))
            .collect();

        assert_eq!(
            by_id.get(&1).map(|backend| backend.backend_addr.as_str()),
            Some("http://127.0.0.1:8001")
        );
        assert_eq!(by_id.get(&1).map(|backend| backend.read_only), Some(false));
        assert_eq!(
            by_id.get(&2).map(|backend| backend.backend_addr.as_str()),
            Some("http://127.0.0.1:8002")
        );
        assert_eq!(by_id.get(&2).map(|backend| backend.read_only), Some(true));
    }
}
