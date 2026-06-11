//! Collection-scope methods: lifecycle, indexes, and extension metadata.
//!
//! Index definitions are part of collection creation: the engine only
//! allows index changes while it holds exclusive access to the collection
//! (at creation or on the first open after a restart). `collection.ensure`
//! therefore guarantees the listed indexes only when it actually creates or
//! first opens the collection.

use anda_db::{
    collection::{Collection, CollectionConfig, CollectionMetadata, CollectionStats},
    database::AndaDB,
    error::DBError,
    index::HnswConfig,
    schema::{Fv, Schema},
};
use serde::Deserialize;
use std::sync::Arc;

use super::db::{ExtensionKeyParams, SaveExtensionParams, SetReadOnlyParams};
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
pub async fn open(db: &AndaDB, name: &str) -> Result<Arc<Collection>, ApiError> {
    Ok(db
        .open_collection(name.to_string(), async |_| Ok(()))
        .await?)
}

/// `collection.create` — fails if the collection already exists.
pub async fn create(
    db: &AndaDB,
    params: CreateCollectionParams,
) -> Result<CollectionMetadata, ApiError> {
    let CreateCollectionParams {
        config,
        schema,
        btree_indexes,
        bm25_indexes,
        hnsw_indexes,
    } = params;
    let collection = db
        .create_collection(schema, config, async |collection| {
            ensure_indexes(collection, &btree_indexes, &bm25_indexes, &hnsw_indexes).await
        })
        .await?;
    Ok(collection.metadata())
}

/// `collection.ensure` — opens the collection or creates it if missing.
pub async fn ensure(
    db: &AndaDB,
    params: CreateCollectionParams,
) -> Result<CollectionMetadata, ApiError> {
    let CreateCollectionParams {
        config,
        schema,
        btree_indexes,
        bm25_indexes,
        hnsw_indexes,
    } = params;
    let collection = db
        .open_or_create_collection(schema, config, async |collection| {
            ensure_indexes(collection, &btree_indexes, &bm25_indexes, &hnsw_indexes).await
        })
        .await?;
    Ok(collection.metadata())
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
    db.delete_collection(&params.collection).await?;
    Ok(())
}

/// `collection.flush` — returns `true` if pending changes were written.
pub async fn flush(db: &AndaDB, params: CollectionParams) -> Result<bool, ApiError> {
    let collection = open(db, &params.collection).await?;
    Ok(collection.flush(anda_db::unix_ms()).await?)
}

/// `collection.set_read_only`
pub async fn set_read_only(
    db: &AndaDB,
    params: CollectionSetReadOnlyParams,
) -> Result<(), ApiError> {
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
    Ok(collection.remove_extension(&params.params.key).await?)
}
