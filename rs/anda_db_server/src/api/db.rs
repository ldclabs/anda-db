//! Database-scope methods: flush, read-only mode, and extension metadata.

use anda_db::{database::AndaDB, schema::Fv};
use serde::Deserialize;

use crate::error::ApiError;

pub(super) fn ensure_writable(db: &AndaDB) -> Result<(), ApiError> {
    if db.is_read_only() {
        return Err(ApiError::conflict(format!(
            "database {:?} is read-only",
            db.name()
        )));
    }
    Ok(())
}

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
    ensure_writable(db)?;
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
    ensure_writable(db)?;
    params
        .value
        .validate_complexity()
        .map_err(|err| ApiError::invalid_input(format!("invalid extension value: {err}")))?;
    db.save_extension(params.key, params.value).await?;
    Ok(())
}

/// `db.remove_extension` — returns the previous value, if any.
pub async fn remove_extension(
    db: &AndaDB,
    params: ExtensionKeyParams,
) -> Result<Option<Fv>, ApiError> {
    ensure_writable(db)?;
    Ok(db.remove_extension(&params.key).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anda_db::{database::DBConfig, storage::StorageConfig};
    use axum::http::StatusCode;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    async fn test_db(name: &str) -> AndaDB {
        AndaDB::connect(
            Arc::new(InMemory::new()),
            DBConfig {
                name: name.to_string(),
                description: String::new(),
                storage: StorageConfig::default(),
                lock: None,
            },
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn extension_boundaries_classify_read_only_and_complexity() {
        let db = test_db("extension_boundaries").await;
        db.set_read_only(true);

        let error = save_extension(
            &db,
            SaveExtensionParams {
                key: "key".to_string(),
                value: Fv::Null,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(error.status, StatusCode::CONFLICT);
        assert_eq!(error.code, "conflict");
        assert_eq!(
            error.message,
            "database \"extension_boundaries\" is read-only"
        );
        let error = flush(&db).await.unwrap_err();
        assert_eq!(error.status, StatusCode::CONFLICT);
        assert_eq!(error.code, "conflict");

        db.set_read_only(false);
        let mut too_deep = Fv::Null;
        for _ in 0..=65 {
            too_deep = Fv::Array(vec![too_deep]);
        }
        let error = save_extension(
            &db,
            SaveExtensionParams {
                key: "key".to_string(),
                value: too_deep,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert_eq!(error.code, "invalid_input");
        assert!(error.message.contains("maximum depth"));

        db.close().await.unwrap();
    }
}
