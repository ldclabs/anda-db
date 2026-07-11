//! PostgreSQL-backed routing metadata storage for the shard proxy.
//!
//! The store keeps two layers of routing information:
//! - database-to-shard assignments, which are effectively stable identifiers
//! - shard-to-backend assignments, which can change during upgrades or moves
//!
//! Local in-memory caches (a bounded [`moka`] cache for db→shard lookups, a
//! [`DashMap`] for the small shard→backend table) are used for fast
//! request-time lookups, while PostgreSQL remains the source of truth and
//! distributes incremental updates through `LISTEN/NOTIFY`.

use dashmap::DashMap;
use moka::{Expiry, future::Cache};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::postgres::PgListener;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

/// How long a positive db→shard cache entry is trusted before it is
/// re-validated against PostgreSQL. A last line of defense against missed
/// NOTIFY events; incremental events keep the cache fresh well before this.
const DB_CACHE_POSITIVE_TTL: Duration = Duration::from_secs(60);

/// How long a negative ("no such database") cache entry suppresses
/// PostgreSQL lookups. Short, so a fresh assignment becomes visible quickly
/// even if its NOTIFY event was missed.
const DB_CACHE_NEGATIVE_TTL: Duration = Duration::from_secs(5);

/// Maximum number of db→shard cache entries (positive or negative). The
/// cache must be bounded: every unauthenticated request with an unknown
/// database name inserts a negative entry, so an unbounded map would let an
/// attacker grow proxy memory without limit by probing random names.
const DB_CACHE_CAPACITY: u64 = 100_000;

/// Mapping from database name to its assigned shard ID.
/// This binding can be updated by administrators when needed.
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
    /// Advisory flag: the backend is in read-only mode (e.g. during
    /// migration). The proxy forwards it in routing metadata but does **not**
    /// enforce it — the RPC protocol is POST-based, so the HTTP method cannot
    /// distinguish reads from writes; enforcement is the backend's job.
    #[serde(default)]
    pub read_only: bool,
}

/// A cached db→shard lookup result. Both positive and negative entries
/// expire (see [`DB_CACHE_POSITIVE_TTL`] / [`DB_CACHE_NEGATIVE_TTL`]) so
/// missed NOTIFY events have bounded impact.
#[derive(Debug, Clone, Copy)]
enum DbCacheEntry {
    /// The database is assigned to this shard.
    Found { shard_id: u32, cached_at: Instant },
    /// PostgreSQL had no row for this database name.
    NotFound { cached_at: Instant },
}

impl DbCacheEntry {
    fn found(shard_id: u32) -> Self {
        Self::Found {
            shard_id,
            cached_at: Instant::now(),
        }
    }

    fn not_found() -> Self {
        Self::NotFound {
            cached_at: Instant::now(),
        }
    }

    /// Returns the cached result if the entry is still fresh.
    fn get(&self) -> Option<Option<u32>> {
        match *self {
            Self::Found {
                shard_id,
                cached_at,
            } if cached_at.elapsed() < DB_CACHE_POSITIVE_TTL => Some(Some(shard_id)),
            Self::NotFound { cached_at } if cached_at.elapsed() < DB_CACHE_NEGATIVE_TTL => {
                Some(None)
            }
            _ => None,
        }
    }

    /// Physical lifetime of this entry in the bounded cache; matches the
    /// logical TTL that [`DbCacheEntry::get`] enforces on hits.
    fn ttl(&self) -> Duration {
        match self {
            Self::Found { .. } => DB_CACHE_POSITIVE_TTL,
            Self::NotFound { .. } => DB_CACHE_NEGATIVE_TTL,
        }
    }
}

/// Per-variant expiration for the bounded db→shard cache: positive entries
/// live [`DB_CACHE_POSITIVE_TTL`], negative entries only
/// [`DB_CACHE_NEGATIVE_TTL`], so a flood of unknown database names is
/// physically evicted quickly instead of merely expiring logically while
/// still occupying memory.
struct DbCacheExpiry;

impl Expiry<String, DbCacheEntry> for DbCacheExpiry {
    fn expire_after_create(
        &self,
        _key: &String,
        value: &DbCacheEntry,
        _created_at: Instant,
    ) -> Option<Duration> {
        Some(value.ttl())
    }

    fn expire_after_update(
        &self,
        _key: &String,
        value: &DbCacheEntry,
        _updated_at: Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        Some(value.ttl())
    }
}

/// Builds the bounded db→shard cache: capacity-limited and expired per entry
/// variant, so memory stays bounded even under random-name probing.
fn new_db_cache() -> Cache<String, DbCacheEntry> {
    Cache::builder()
        .max_capacity(DB_CACHE_CAPACITY)
        .expire_after(DbCacheExpiry)
        .build()
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

/// Persistent shard routing store backed by PostgreSQL with in-memory caches.
///
/// Two-table design:
/// - `db_shards`: db_name → shard_id (large, mostly stable; can be updated)
/// - `shard_backends`: shard_id → backend_addr (small, mutable for upgrades)
///
/// Uses PostgreSQL `LISTEN/NOTIFY` so that multiple proxy instances stay in sync.
#[derive(Clone)]
pub struct ShardStore {
    pool: PgPool,
    /// db_name → cached lookup result (positive or negative). Bounded
    /// ([`DB_CACHE_CAPACITY`]) and physically expired per entry variant, so
    /// unauthenticated requests probing random names cannot grow memory
    /// without limit.
    db_cache: Cache<String, DbCacheEntry>,
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
            db_cache: new_db_cache(),
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

    /// Look up the shard for a database name, using the cache first.
    ///
    /// On a fresh cache miss the result — including "not assigned" — is
    /// cached, so unknown database names cannot stampede PostgreSQL.
    /// PostgreSQL errors are propagated instead of being folded into
    /// "not found".
    async fn lookup_db_shard(&self, db_name: &str) -> Result<Option<u32>, sqlx::Error> {
        if let Some(entry) = self.db_cache.get(db_name).await
            && let Some(cached) = entry.get()
        {
            return Ok(cached);
        }

        let row: Option<(i32,)> =
            sqlx::query_as("SELECT shard_id FROM db_shards WHERE db_name = $1")
                .bind(db_name)
                .fetch_optional(&self.pool)
                .await?;
        match row {
            Some((sid,)) => {
                let shard_id = sid as u32;
                self.db_cache
                    .insert(db_name.to_string(), DbCacheEntry::found(shard_id))
                    .await;
                Ok(Some(shard_id))
            }
            None => {
                self.db_cache
                    .insert(db_name.to_string(), DbCacheEntry::not_found())
                    .await;
                Ok(None)
            }
        }
    }

    /// Resolve a database name to its full route (shard + backend).
    ///
    /// Returns `Ok(None)` when the database has no assignment (or the shard
    /// has no backend) and `Err` when PostgreSQL could not be queried, so the
    /// proxy can answer 404 and 503 respectively.
    pub async fn resolve(&self, db_name: &str) -> Result<Option<ResolvedRoute>, sqlx::Error> {
        let Some(shard_id) = self.lookup_db_shard(db_name).await? else {
            return Ok(None);
        };
        let Some(backend) = self.backend_cache.get(&shard_id) else {
            return Ok(None);
        };
        Ok(Some(ResolvedRoute {
            db_name: Some(db_name.to_string()),
            shard_id,
            backend_addr: backend.backend_addr.clone(),
            read_only: backend.read_only,
        }))
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
    /// The lookup uses the cache first and falls back to PostgreSQL on a
    /// miss. Query errors are propagated so callers can distinguish "not
    /// assigned" (`Ok(None)`) from "PostgreSQL unavailable" (`Err`).
    pub async fn get_db_shard(&self, db_name: &str) -> Result<Option<DbShard>, sqlx::Error> {
        Ok(self
            .lookup_db_shard(db_name)
            .await?
            .map(|shard_id| DbShard {
                db_name: db_name.to_string(),
                shard_id,
            }))
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

    // ── db_shards mutations ───────────────────────────────────────────────────

    /// Assign a database to a shard.
    ///
    /// If the database already exists, its shard binding is updated. The
    /// write and its `NOTIFY` run in one transaction, so either both take
    /// effect or neither does, and other instances receive the event exactly
    /// when the row becomes visible.
    pub async fn assign_db(&self, db_name: &str, shard_id: u32) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO db_shards (db_name, shard_id) VALUES ($1, $2) \
             ON CONFLICT (db_name) DO UPDATE SET shard_id = EXCLUDED.shard_id",
        )
        .bind(db_name)
        .bind(shard_id as i32)
        .execute(&mut *tx)
        .await?;
        Self::notify_tx(
            &mut tx,
            "db_shards_changed",
            &DbShardEvent::Assign {
                db_name: db_name.to_string(),
                shard_id,
            },
        )
        .await?;
        tx.commit().await?;

        self.db_cache
            .insert(db_name.to_string(), DbCacheEntry::found(shard_id))
            .await;
        Ok(())
    }

    /// Remove a database-to-shard binding.
    ///
    /// Returns `true` when a row was deleted and `false` when the binding was
    /// already absent.
    pub async fn unassign_db(&self, db_name: &str) -> Result<bool, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query("DELETE FROM db_shards WHERE db_name = $1")
            .bind(db_name)
            .execute(&mut *tx)
            .await?;
        Self::notify_tx(
            &mut tx,
            "db_shards_changed",
            &DbShardEvent::Unassign {
                db_name: db_name.to_string(),
            },
        )
        .await?;
        tx.commit().await?;

        self.db_cache.invalidate(db_name).await;
        Ok(result.rows_affected() > 0)
    }

    // ── shard_backends mutations (dynamic bindings) ─────────────────────────

    /// Add or update a shard backend entry.
    ///
    /// This operation is intentionally mutable so operators can redirect a
    /// shard to a new backend during maintenance, failover, or migration.
    pub async fn upsert_backend(&self, backend: &ShardBackend) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
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
        .execute(&mut *tx)
        .await?;
        Self::notify_tx(
            &mut tx,
            "shard_backends_changed",
            &BackendEvent::Upsert {
                shard_id: backend.shard_id,
                backend_addr: backend.backend_addr.clone(),
                read_only: backend.read_only,
            },
        )
        .await?;
        tx.commit().await?;

        self.backend_cache.insert(backend.shard_id, backend.clone());
        Ok(())
    }

    /// Remove a shard backend entry.
    ///
    /// Returns `true` when the entry existed and was removed.
    pub async fn delete_backend(&self, shard_id: u32) -> Result<bool, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query("DELETE FROM shard_backends WHERE shard_id = $1")
            .bind(shard_id as i32)
            .execute(&mut *tx)
            .await?;
        Self::notify_tx(
            &mut tx,
            "shard_backends_changed",
            &BackendEvent::Delete { shard_id },
        )
        .await?;
        tx.commit().await?;

        self.backend_cache.remove(&shard_id);
        Ok(result.rows_affected() > 0)
    }

    // ── NOTIFY / LISTEN ─────────────────────────────────────────────────────

    /// Queue a PostgreSQL `NOTIFY` inside the given transaction; it is
    /// delivered to other proxy instances when the transaction commits, so
    /// cache updates cannot be observed before the data change itself.
    async fn notify_tx<T: Serialize>(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        channel: &str,
        event: &T,
    ) -> Result<(), sqlx::Error> {
        // The event enums serialize infallibly; a failure here would be a bug
        // that must not be masked by an empty payload.
        let payload = serde_json::to_string(event).expect("event serialization must not fail");
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(channel)
            .bind(&payload)
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    /// Apply a database-assignment event received from PostgreSQL.
    async fn apply_db_event(&self, payload: &str) {
        match serde_json::from_str::<DbShardEvent>(payload) {
            Ok(DbShardEvent::Assign { db_name, shard_id }) => {
                self.db_cache
                    .insert(db_name, DbCacheEntry::found(shard_id))
                    .await;
            }
            Ok(DbShardEvent::Unassign { db_name }) => {
                self.db_cache.invalidate(&db_name).await;
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
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
                        _ = cancel.cancelled() => return,
                    }
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

        // Recover from any events missed while the listener was offline:
        // reload the (small) backend table and drop every db→shard entry so
        // the lazy path re-resolves against PostgreSQL. Without the clear, a
        // missed unassign/assign would keep routing a tenant to a shard that
        // no longer owns it.
        if let Err(e) = self.reload_backend_cache().await {
            log::error!("failed to reload backend cache on connect: {}", e);
        }
        self.db_cache.invalidate_all();

        loop {
            tokio::select! {
                // `try_recv`, not `recv`: `recv()` transparently reconnects
                // after a dropped connection and never reports it, silently
                // discarding every NOTIFY delivered during the gap — so this
                // cache resync would only ever run for errors that tear the
                // whole loop down, not for common network blips.
                notification = listener.try_recv() => {
                    match notification {
                        Ok(Some(n)) => {
                            let channel = n.channel();
                            let payload = n.payload();
                            log::info!("received notify on {}", channel);
                            match channel {
                                "db_shards_changed" => self.apply_db_event(payload).await,
                                "shard_backends_changed" => self.apply_backend_event(payload),
                                _ => {}
                            }
                        }
                        // The listener lost its connection (and, with sqlx's
                        // default eager reconnect, already re-established
                        // it). Events during the gap are gone: drop every
                        // db→shard entry and reload the backend table. A
                        // reload failure propagates to the outer rebuild
                        // path, which reconnects and resyncs again.
                        Ok(None) => {
                            log::warn!("pg listener reconnected, resyncing routing caches");
                            self.db_cache.invalidate_all();
                            self.reload_backend_cache().await?;
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
            db_cache: new_db_cache(),
            backend_cache: Arc::new(DashMap::new()),
        }
    }

    async fn cached_shard(store: &ShardStore, db_name: &str) -> Option<Option<u32>> {
        store
            .db_cache
            .get(db_name)
            .await
            .and_then(|entry| entry.get())
    }

    #[tokio::test]
    async fn apply_db_event_assign_and_unassign_updates_cache() {
        let store = test_store();

        store
            .apply_db_event(r#"{"op":"assign","db_name":"db_a","shard_id":3}"#)
            .await;
        assert_eq!(cached_shard(&store, "db_a").await, Some(Some(3)));

        store
            .apply_db_event(r#"{"op":"unassign","db_name":"db_a"}"#)
            .await;
        assert!(store.db_cache.get("db_a").await.is_none());
    }

    #[tokio::test]
    async fn apply_db_event_invalid_payload_does_not_change_cache() {
        let store = test_store();
        store
            .db_cache
            .insert("db_keep".to_string(), DbCacheEntry::found(9))
            .await;

        store.apply_db_event("not-json").await;

        assert_eq!(cached_shard(&store, "db_keep").await, Some(Some(9)));
        store.db_cache.run_pending_tasks().await;
        assert_eq!(store.db_cache.entry_count(), 1);
    }

    #[test]
    fn db_cache_entries_expire() {
        let fresh_hit = DbCacheEntry::found(7);
        assert_eq!(fresh_hit.get(), Some(Some(7)));
        let fresh_miss = DbCacheEntry::not_found();
        assert_eq!(fresh_miss.get(), Some(None));

        let old = Instant::now() - DB_CACHE_POSITIVE_TTL;
        let stale_hit = DbCacheEntry::Found {
            shard_id: 7,
            cached_at: old,
        };
        assert_eq!(stale_hit.get(), None);
        let stale_miss = DbCacheEntry::NotFound {
            cached_at: Instant::now() - DB_CACHE_NEGATIVE_TTL,
        };
        assert_eq!(stale_miss.get(), None);
    }

    #[test]
    fn db_cache_expiry_uses_variant_ttl() {
        let expiry = DbCacheExpiry;
        let key = "db".to_string();
        let now = Instant::now();

        assert_eq!(
            expiry.expire_after_create(&key, &DbCacheEntry::found(1), now),
            Some(DB_CACHE_POSITIVE_TTL)
        );
        assert_eq!(
            expiry.expire_after_create(&key, &DbCacheEntry::not_found(), now),
            Some(DB_CACHE_NEGATIVE_TTL)
        );
        // A re-insert (e.g. an assign event replacing a negative entry) must
        // reset the expiration according to the *new* variant.
        assert_eq!(
            expiry.expire_after_update(
                &key,
                &DbCacheEntry::found(1),
                now,
                Some(DB_CACHE_NEGATIVE_TTL)
            ),
            Some(DB_CACHE_POSITIVE_TTL)
        );
        assert_eq!(
            expiry.expire_after_update(
                &key,
                &DbCacheEntry::not_found(),
                now,
                Some(DB_CACHE_POSITIVE_TTL)
            ),
            Some(DB_CACHE_NEGATIVE_TTL)
        );
    }

    #[tokio::test]
    async fn db_cache_is_bounded_under_random_name_flood() {
        // Same shape as `new_db_cache`, but with a small capacity so the
        // test stays fast; the production cache only differs in the bound.
        let cache: Cache<String, DbCacheEntry> = Cache::builder()
            .max_capacity(64)
            .expire_after(DbCacheExpiry)
            .build();

        for i in 0..10_000u32 {
            cache
                .insert(format!("missing_{i}"), DbCacheEntry::not_found())
                .await;
        }
        cache.run_pending_tasks().await;
        assert!(
            cache.entry_count() <= 64,
            "cache must stay bounded, got {}",
            cache.entry_count()
        );
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
