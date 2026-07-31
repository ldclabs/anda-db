//! Root-scope methods: database lifecycle and API-key provisioning on the
//! server registry.
//!
//! Everything in this module is admin-only: the root scope rejects
//! per-database keys outright (see [`crate::auth`]), so these handlers do not
//! repeat the check.

use anda_db::database::DBMetadata;
use serde::{Deserialize, Serialize};

use crate::{
    error::ApiError,
    state::{AppState, OpenMode},
};

/// Parameters identifying a database, with an optional description used
/// when the database is created.
#[derive(Debug, Deserialize)]
pub struct DatabaseParams {
    /// Database name (lowercase ASCII letters, digits, and underscores).
    pub name: String,
    /// Optional description; defaults to the database name.
    #[serde(default)]
    pub description: Option<String>,
}

/// Parameters of `db.create`.
///
/// Kept separate from [`DatabaseParams`] so that `api_key` cannot be sent to
/// a method that would silently ignore it: rotating the key of an existing
/// database goes through `db.set_api_key`.
#[derive(Debug, Deserialize)]
pub struct CreateDatabaseParams {
    /// Database name (lowercase ASCII letters, digits, and underscores).
    pub name: String,
    /// Optional description; defaults to the database name.
    #[serde(default)]
    pub description: Option<String>,
    /// Optional API key granting access to this database only. When omitted
    /// the database is governed by the admin key alone, which is how every
    /// database behaved before per-database keys existed.
    #[serde(default)]
    pub api_key: Option<String>,
}

/// Parameters of `db.set_api_key` / `db.remove_api_key`.
#[derive(Debug, Deserialize)]
pub struct ApiKeyParams {
    /// Name of an open or registered database.
    pub name: String,
    /// The key to bind. When omitted, the server generates one with a CSPRNG
    /// and returns it in [`ApiKeyResult::api_key`].
    #[serde(default)]
    pub api_key: Option<String>,
}

/// Result of `db.set_api_key`.
#[derive(Debug, Serialize)]
pub struct ApiKeyResult {
    /// The database the key was bound to.
    pub name: String,
    /// The generated key, returned exactly once and never recoverable
    /// afterwards (only its hash is stored). `null` when the caller supplied
    /// the key — a supplied secret is never echoed back.
    pub api_key: Option<String>,
}

/// `db.create`
pub async fn create(
    state: &AppState,
    params: CreateDatabaseParams,
) -> Result<DBMetadata, ApiError> {
    state
        .register_db(
            OpenMode::Create,
            &params.name,
            params.description,
            params.api_key,
        )
        .await
}

/// `db.open` / `db.connect`
pub async fn register(
    state: &AppState,
    mode: OpenMode,
    params: DatabaseParams,
) -> Result<DBMetadata, ApiError> {
    state
        .register_db(mode, &params.name, params.description, None)
        .await
}

/// `db.close`
pub async fn close(state: &AppState, params: DatabaseParams) -> Result<(), ApiError> {
    state.close_db(&params.name).await
}

/// `db.set_api_key` — binds or rotates a database's API key.
pub async fn set_api_key(state: &AppState, params: ApiKeyParams) -> Result<ApiKeyResult, ApiError> {
    let generated = state.set_db_api_key(&params.name, params.api_key).await?;
    Ok(ApiKeyResult {
        name: params.name,
        api_key: generated,
    })
}

/// `db.remove_api_key` — drops a binding, returning the database to the
/// admin-key fallback. Returns whether a key was bound.
pub async fn remove_api_key(state: &AppState, params: ApiKeyParams) -> Result<bool, ApiError> {
    state.remove_db_api_key(&params.name).await
}
