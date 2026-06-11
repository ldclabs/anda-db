//! Database-scope methods: flush, read-only mode, and extension metadata.

use anda_db::{database::AndaDB, schema::Fv};
use serde::Deserialize;

use crate::error::ApiError;

/// Parameters for toggling read-only mode.
#[derive(Debug, Deserialize)]
pub struct SetReadOnlyParams {
    /// `true` to reject writes, `false` to allow them again.
    pub read_only: bool,
}

/// Parameters identifying an extension entry.
#[derive(Debug, Deserialize)]
pub struct ExtensionKeyParams {
    /// Extension key.
    pub key: String,
}

/// Parameters for storing an extension entry.
#[derive(Debug, Deserialize)]
pub struct SaveExtensionParams {
    /// Extension key.
    pub key: String,
    /// Extension value.
    pub value: Fv,
}

/// `db.flush`
pub async fn flush(db: &AndaDB) -> Result<(), ApiError> {
    db.flush().await?;
    Ok(())
}

/// `db.set_read_only`
pub fn set_read_only(db: &AndaDB, params: SetReadOnlyParams) {
    db.set_read_only(params.read_only);
}

/// `db.get_extension`
pub fn get_extension(db: &AndaDB, params: ExtensionKeyParams) -> Option<Fv> {
    db.get_extension(&params.key)
}

/// `db.save_extension` — sets the value and persists database metadata.
pub async fn save_extension(db: &AndaDB, params: SaveExtensionParams) -> Result<(), ApiError> {
    db.save_extension(params.key, params.value).await?;
    Ok(())
}

/// `db.remove_extension` — returns the previous value, if any.
pub async fn remove_extension(
    db: &AndaDB,
    params: ExtensionKeyParams,
) -> Result<Option<Fv>, ApiError> {
    Ok(db.remove_extension(&params.key).await?)
}
