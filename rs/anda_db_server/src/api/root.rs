//! Root-scope methods: database lifecycle on the server registry.

use anda_db::database::DBMetadata;
use serde::Deserialize;

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

/// `db.create` / `db.open` / `db.connect`
pub async fn register(
    state: &AppState,
    mode: OpenMode,
    params: DatabaseParams,
) -> Result<DBMetadata, ApiError> {
    state
        .register_db(mode, &params.name, params.description)
        .await
}

/// `db.close`
pub async fn close(state: &AppState, params: DatabaseParams) -> Result<(), ApiError> {
    state.close_db(&params.name).await
}
