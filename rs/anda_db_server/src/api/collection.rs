//! Collection-scope methods: lifecycle, indexes, and extension metadata.
//!
//! Index definitions are part of collection creation: the engine only
//! allows index changes while it holds exclusive access to the collection
//! (at creation or on the first open after a restart). `collection.ensure`
//! therefore guarantees the listed indexes only when it actually creates or
//! first opens the collection; a requested HNSW configuration that differs
//! from the persisted one is answered as a `409` at that point (see
//! [`ensure`]).

use anda_db::{
    collection::{Collection, CollectionConfig, CollectionMetadata, CollectionStats},
    database::AndaDB,
    error::DBError,
    index::HnswConfig,
    schema::{FieldType, Fv, Schema, as_wildcard_map, validate_field_name},
};
use serde::Deserialize;
use std::sync::{Arc, Mutex};

use super::db::{
    ExtensionKeyParams, SaveExtensionParams, SetReadOnlyParams,
    ensure_writable as ensure_db_writable,
};
use crate::error::ApiError;

/// Parameters identifying a collection.
#[derive(Debug, Deserialize)]
pub struct CollectionParams {
    /// Collection name.
    pub collection: String,
}

/// Parameters for `collection.create` and `collection.ensure`.
#[derive(Debug, Deserialize)]
pub struct CreateCollectionParams {
    /// Collection name and description.
    pub config: CollectionConfig,
    /// Document schema; must contain a `_id: U64` field.
    pub schema: Schema,
    /// B-Tree index definitions, each a list of field names
    /// (multiple names define a multi-field index).
    #[serde(default)]
    pub btree_indexes: Vec<Vec<String>>,
    /// Fields of the BM25 full-text index (at most one per collection).
    #[serde(default)]
    pub bm25_indexes: Vec<String>,
    /// HNSW vector index definitions.
    #[serde(default)]
    pub hnsw_indexes: Vec<HnswIndexParams>,
}

/// An HNSW vector index on one field.
#[derive(Debug, Deserialize)]
pub struct HnswIndexParams {
    /// Vector field to index.
    pub field: String,
    /// HNSW configuration (dimension, distance metric, etc.).
    pub config: HnswConfig,
}

/// Parameters bundling a collection name with an extension key.
#[derive(Debug, Deserialize)]
pub struct CollectionExtensionParams {
    /// Collection name.
    pub collection: String,
    /// Extension parameters.
    #[serde(flatten)]
    pub params: ExtensionKeyParams,
}

/// Parameters bundling a collection name with an extension entry.
#[derive(Debug, Deserialize)]
pub struct CollectionSaveExtensionParams {
    /// Collection name.
    pub collection: String,
    /// Extension parameters.
    #[serde(flatten)]
    pub params: SaveExtensionParams,
}

/// Parameters bundling a collection name with a read-only flag.
#[derive(Debug, Deserialize)]
pub struct CollectionSetReadOnlyParams {
    /// Collection name.
    pub collection: String,
    /// Read-only parameters.
    #[serde(flatten)]
    pub params: SetReadOnlyParams,
}

/// Opens a collection, loading it from storage on first access.
///
/// The engine call runs on its own task, never inline. Read RPCs are
/// dispatched on the cancellable path (their future is dropped on client
/// disconnect, request timeout, or shutdown), but `AndaDB::open_collection`
/// finishes a cold open with `Collection::flush`, which arms a cancel guard
/// that **poisons the handle** if its future is dropped mid-write. Without
/// this hop, the first `doc.get` on a cold collection that hit the request
/// timeout poisoned a perfectly healthy collection for every concurrent and
/// subsequent operation. Dropping the caller now only detaches the join
/// handle; the open itself runs to completion.
pub async fn open(db: &AndaDB, name: &str) -> Result<Arc<Collection>, ApiError> {
    // Prove the client-facing 404 from logical metadata before entering the
    // engine. A later NotFound can mean missing/corrupt persisted collection
    // state and must be handled by the conservative DBError fallback.
    if !db.metadata().collections.contains(name) {
        return Err(ApiError::not_found(format!(
            "collection {name:?} not found"
        )));
    }
    let opening = tokio::spawn({
        let db = db.clone();
        let name = name.to_string();
        async move {
            let result = db.open_collection(name.clone(), async |_| Ok(())).await;
            if let Err(err) = &result {
                // The engine open paths log nothing on failure, and a caller
                // that was cancelled has already dropped the JoinHandle —
                // without this line a failed cold open would be observed by
                // nobody at all.
                log::warn!(
                    action = "collection::open",
                    collection = name;
                    "collection open failed: {err:?}",
                );
            }
            result
        }
    });
    match opening.await {
        Ok(result) => Ok(result?),
        Err(err) => {
            log::error!(
                action = "collection::open",
                collection = name;
                "collection open task failed: {err:?}",
            );
            Err(ApiError::internal("internal server error"))
        }
    }
}

fn btree_type_is_supported(field_type: &FieldType) -> bool {
    // Mirror `BTree::new`: unwrap at most one Option layer, then at most one
    // homogeneous Array/Map layer. Deeper container shapes are unsupported by
    // the engine and must be rejected as request input here.
    let field_type = match field_type {
        FieldType::Option(inner) => inner.as_ref(),
        other => other,
    };
    let key_type = match field_type {
        FieldType::Array(inner) if inner.len() == 1 => inner[0].clone(),
        // `as_wildcard_map` is the shared rule the engine uses. A one-entry
        // map that a nested `FieldTyped` struct declares is *not* a wildcard,
        // so it falls through and is rejected here — otherwise the index would
        // be accepted and then fail inside `BTree::new`.
        FieldType::Map(inner) if as_wildcard_map(inner).is_some() => {
            let key = as_wildcard_map(inner).expect("wildcard checked above").0;
            key.field_type()
        }
        other => other.clone(),
    };
    matches!(
        key_type,
        FieldType::I64 | FieldType::U64 | FieldType::Bytes | FieldType::Text
    )
}

fn validate_definition(params: &CreateCollectionParams) -> Result<(), ApiError> {
    validate_field_name(&params.config.name)
        .map_err(|err| ApiError::invalid_input(format!("invalid collection name: {err}")))?;

    for fields in &params.btree_indexes {
        if fields.is_empty() {
            return Err(ApiError::invalid_input(
                "B-Tree index requires at least one field",
            ));
        }
        for name in fields {
            if params.schema.get_field(name).is_none() {
                return Err(ApiError::invalid_input(format!(
                    "B-Tree index field {name:?} is not declared in the schema"
                )));
            }
        }
        // The engine rejects a single-field `_id` index: the primary key is
        // answered from the always-present id bitmap, so such an index could
        // never serve a query. Say so here instead of returning the engine's
        // generic failure.
        if fields.len() == 1 && fields[0] == Schema::ID_KEY {
            return Err(ApiError::invalid_input(format!(
                "B-Tree index on {:?} is not supported: the primary key is always queryable",
                Schema::ID_KEY
            )));
        }
        if fields.len() == 1 {
            let field = params
                .schema
                .get_field(&fields[0])
                .expect("field presence checked above");
            if !btree_type_is_supported(field.r#type()) {
                return Err(ApiError::invalid_input(format!(
                    "field {:?} has type {:?}, which cannot be used by a B-Tree index",
                    fields[0],
                    field.r#type()
                )));
            }
        }
    }

    for name in &params.bm25_indexes {
        if params.schema.get_field(name).is_none() {
            return Err(ApiError::invalid_input(format!(
                "BM25 index field {name:?} is not declared in the schema"
            )));
        }
    }

    for index in &params.hnsw_indexes {
        let field = params.schema.get_field(&index.field).ok_or_else(|| {
            ApiError::invalid_input(format!(
                "HNSW index field {:?} is not declared in the schema",
                index.field
            ))
        })?;
        if field.r#type() != &FieldType::Vector {
            return Err(ApiError::invalid_input(format!(
                "HNSW index field {:?} must have type Vector",
                index.field
            )));
        }
        index
            .config
            .validate(&index.field)
            .map_err(|err| ApiError::invalid_input(err.to_string()))?;
    }

    Ok(())
}

/// Rejects a write the collection cannot accept, with a status the client can
/// act on.
///
/// `stats().read_only` alone is not enough: a handle that a cancelled
/// operation poisoned, or that is closing or already deleted, is not
/// read-only, so its writes surfaced as an opaque 500.
/// [`ApiError::from_collection_state`] splits the recoverable states (retry)
/// from the deleted ones (gone); the same classification is applied to errors
/// the engine raises later, in `ApiError::from(DBError)`.
pub(super) fn ensure_writable(collection: &Collection) -> Result<(), ApiError> {
    if let Some(err) = ApiError::from_collection_state(collection.state()) {
        return Err(err);
    }
    if collection.stats().read_only {
        return Err(ApiError::conflict(format!(
            "collection {:?} is read-only",
            collection.name()
        )));
    }
    Ok(())
}

/// `collection.create` — fails if the collection already exists.
pub async fn create(
    db: &AndaDB,
    params: CreateCollectionParams,
) -> Result<CollectionMetadata, ApiError> {
    validate_definition(&params)?;
    ensure_db_writable(db)?;
    if db.metadata().collections.contains(&params.config.name) {
        return Err(ApiError::already_exists(format!(
            "collection {:?} already exists",
            params.config.name
        )));
    }
    let CreateCollectionParams {
        config,
        schema,
        btree_indexes,
        bm25_indexes,
        hnsw_indexes,
    } = params;
    let collection_name = config.name.clone();
    let collection = match db
        .create_collection(schema, config, async |collection| {
            ensure_indexes(collection, &btree_indexes, &bm25_indexes, &hnsw_indexes).await
        })
        .await
    {
        Ok(collection) => collection,
        Err(err @ DBError::AlreadyExists { .. })
            if db.metadata().collections.contains(&collection_name) =>
        {
            // The pre-check above was clear, and the name is now registered:
            // another request won the per-name creation race. This proves a
            // logical conflict without trusting the engine variant or exposing
            // its physical path/source.
            log::warn!(
                action = "collection::create",
                collection = collection_name;
                "concurrent collection creation conflict: {err:?}",
            );
            return Err(ApiError::already_exists(format!(
                "collection {collection_name:?} already exists"
            )));
        }
        Err(err) => return Err(err.into()),
    };
    Ok(collection.metadata())
}

/// `collection.ensure` — opens the collection or creates it if missing.
///
/// The engine refuses to silently keep an existing HNSW index whose persisted
/// configuration differs from the request (`create_hnsw_index_nx`), but its
/// `DBError::Index` would be sanitized into an opaque 500 that only fires on
/// the first load after a restart. The conflict is proven here instead,
/// inside the exclusive-access callback where the persisted configuration is
/// loaded, and answered as an actionable `409 conflict`: the caller owns this
/// configuration, so echoing it back leaks nothing.
pub async fn ensure(
    db: &AndaDB,
    params: CreateCollectionParams,
) -> Result<CollectionMetadata, ApiError> {
    validate_definition(&params)?;
    ensure_db_writable(db)?;
    let CreateCollectionParams {
        config,
        schema,
        btree_indexes,
        bm25_indexes,
        hnsw_indexes,
    } = params;
    let collection_name = config.name.clone();
    let hnsw_conflict: Mutex<Option<String>> = Mutex::new(None);
    let result = db
        .open_or_create_collection(schema, config, async |collection| {
            if let Some(conflict) = hnsw_config_conflict(collection, &hnsw_indexes) {
                *hnsw_conflict
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(conflict);
                // Abort the open: proceeding would either fail later inside
                // `create_hnsw_index_nx` with an engine error this function
                // could not safely attribute, or silently keep the old
                // configuration.
                return Err(DBError::Index {
                    name: collection.name().to_string(),
                    source: "HNSW index configuration conflict".into(),
                });
            }
            ensure_indexes(collection, &btree_indexes, &bm25_indexes, &hnsw_indexes).await
        })
        .await;
    match result {
        Ok(collection) => Ok(collection.metadata()),
        Err(err) => {
            // The callback proved the conflict against the persisted
            // configuration before returning `err`, so the client-safe
            // message wins over the sanitizing `DBError` fallback.
            if let Some(conflict) = hnsw_conflict
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
            {
                log::warn!(
                    action = "collection::ensure",
                    collection = collection_name;
                    "HNSW index configuration conflict: {conflict}",
                );
                return Err(ApiError::conflict(conflict));
            }
            Err(err.into())
        }
    }
}

/// Returns an actionable client-safe message when `collection` already
/// carries an HNSW index whose persisted configuration differs from a
/// requested one, or `None` when every requested index is absent or
/// identical.
///
/// Must run inside the create/open callback: that is the only point where
/// the server holds the collection exclusively with its persisted indexes
/// loaded, so the comparison cannot race another request.
fn hnsw_config_conflict(
    collection: &Collection,
    hnsw_indexes: &[HnswIndexParams],
) -> Option<String> {
    for index in hnsw_indexes {
        if let Ok(view) = collection.get_hnsw_index(&index.field) {
            let persisted = view.metadata().config;
            if persisted != index.config {
                return Some(format!(
                    "HNSW index on field {:?} already exists with a different configuration; \
                     remove and recreate the index to change it. \
                     persisted={persisted:?}, requested={:?}",
                    index.field, index.config
                ));
            }
        }
    }
    None
}

async fn ensure_indexes(
    collection: &mut Collection,
    btree_indexes: &[Vec<String>],
    bm25_indexes: &[String],
    hnsw_indexes: &[HnswIndexParams],
) -> Result<(), DBError> {
    for fields in btree_indexes {
        let fields: Vec<&str> = fields.iter().map(String::as_str).collect();
        collection.create_btree_index_nx(&fields).await?;
    }
    if !bm25_indexes.is_empty() {
        let fields: Vec<&str> = bm25_indexes.iter().map(String::as_str).collect();
        collection.create_bm25_index_nx(&fields).await?;
    }
    for index in hnsw_indexes {
        collection
            .create_hnsw_index_nx(&index.field, index.config.clone())
            .await?;
    }
    Ok(())
}

/// `collection.metadata`
pub async fn metadata(
    db: &AndaDB,
    params: CollectionParams,
) -> Result<CollectionMetadata, ApiError> {
    Ok(open(db, &params.collection).await?.metadata())
}

/// `collection.stats`
pub async fn stats(db: &AndaDB, params: CollectionParams) -> Result<CollectionStats, ApiError> {
    Ok(open(db, &params.collection).await?.stats())
}

/// `collection.delete` — removes the collection and all of its data.
pub async fn delete(db: &AndaDB, params: CollectionParams) -> Result<(), ApiError> {
    ensure_db_writable(db)?;
    if !db.metadata().collections.contains(&params.collection) {
        return Err(ApiError::not_found(format!(
            "collection {:?} not found",
            params.collection
        )));
    }
    db.delete_collection(&params.collection).await?;
    Ok(())
}

/// `collection.flush` — returns `true` if pending changes were written.
pub async fn flush(db: &AndaDB, params: CollectionParams) -> Result<bool, ApiError> {
    let collection = open(db, &params.collection).await?;
    ensure_writable(&collection)?;
    Ok(collection.flush(anda_db::unix_ms()).await?)
}

/// `collection.set_read_only`
pub async fn set_read_only(
    db: &AndaDB,
    params: CollectionSetReadOnlyParams,
) -> Result<(), ApiError> {
    if !params.params.read_only && db.is_read_only() {
        return Err(ApiError::conflict(format!(
            "database {:?} is read-only",
            db.name()
        )));
    }
    let collection = open(db, &params.collection).await?;
    collection.set_read_only(params.params.read_only);
    Ok(())
}

/// `collection.get_extension`
pub async fn get_extension(
    db: &AndaDB,
    params: CollectionExtensionParams,
) -> Result<Option<Fv>, ApiError> {
    let collection = open(db, &params.collection).await?;
    Ok(collection.get_extension(&params.params.key))
}

/// `collection.save_extension` — sets the value and persists collection metadata.
pub async fn save_extension(
    db: &AndaDB,
    params: CollectionSaveExtensionParams,
) -> Result<(), ApiError> {
    let collection = open(db, &params.collection).await?;
    ensure_writable(&collection)?;
    params
        .params
        .value
        .validate_complexity()
        .map_err(|err| ApiError::invalid_input(format!("invalid extension value: {err}")))?;
    collection
        .save_extension(params.params.key, params.params.value)
        .await?;
    Ok(())
}

/// `collection.remove_extension` — returns the previous value, if any.
pub async fn remove_extension(
    db: &AndaDB,
    params: CollectionExtensionParams,
) -> Result<Option<Fv>, ApiError> {
    let collection = open(db, &params.collection).await?;
    ensure_writable(&collection)?;
    Ok(collection.remove_extension(&params.params.key).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anda_db::{database::DBConfig, storage::StorageConfig};
    use axum::http::StatusCode;
    use object_store::memory::InMemory;

    fn params(name: &str) -> CreateCollectionParams {
        CreateCollectionParams {
            config: CollectionConfig {
                name: name.to_string(),
                description: String::new(),
            },
            schema: Schema::builder().build().unwrap(),
            btree_indexes: Vec::new(),
            bm25_indexes: Vec::new(),
            hnsw_indexes: Vec::new(),
        }
    }

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
    async fn database_read_only_is_a_conflict_for_collection_mutations() {
        let db = test_db("read_only_collections").await;
        db.set_read_only(true);

        let error = create(&db, params("items")).await.unwrap_err();
        assert_eq!(error.status, StatusCode::CONFLICT);
        assert_eq!(error.code, "conflict");
        assert_eq!(
            error.message,
            "database \"read_only_collections\" is read-only"
        );

        db.set_read_only(false);
        create(&db, params("items")).await.unwrap();
        db.set_read_only(true);

        for error in [
            ensure(&db, params("items")).await.unwrap_err(),
            delete(
                &db,
                CollectionParams {
                    collection: "items".to_string(),
                },
            )
            .await
            .unwrap_err(),
            flush(
                &db,
                CollectionParams {
                    collection: "items".to_string(),
                },
            )
            .await
            .unwrap_err(),
        ] {
            assert_eq!(error.status, StatusCode::CONFLICT);
            assert_eq!(error.code, "conflict");
        }

        db.set_read_only(false);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_collection_create_is_a_sanitized_conflict() {
        let db = test_db("concurrent_collection_create").await;
        let (left, right) =
            tokio::join!(create(&db, params("items")), create(&db, params("items")));

        let error = match (left, right) {
            (Ok(_), Err(error)) | (Err(error), Ok(_)) => error,
            (left, right) => panic!(
                "expected one success and one conflict, got left={:?}, right={:?}",
                left.map(|_| ()),
                right.map(|_| ())
            ),
        };
        assert_eq!(error.status, StatusCode::CONFLICT);
        assert_eq!(error.code, "already_exists");
        assert_eq!(error.message, "collection \"items\" already exists");
        assert!(!error.message.contains("meta.cbor"));

        db.close().await.unwrap();
    }
}
