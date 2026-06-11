//! Document-scope methods: CRUD, hybrid search, and filtered ID queries.
//!
//! Documents travel on the wire as field maps (`{field_name: value}`).
//! CBOR encoding preserves binary values such as `bf16` vectors losslessly;
//! the engine assigns `_id` on insert and any client-provided value for it
//! is ignored.

use anda_db::{
    database::AndaDB,
    error::DBError,
    query::{Filter, Query},
    schema::{DocumentId, FieldType, Fv, Schema, bf16},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::collection::open;
use crate::error::ApiError;

/// Parameters for inserting one document.
#[derive(Debug, Deserialize)]
pub struct AddParams {
    /// Target collection name.
    pub collection: String,
    /// Document field map.
    pub doc: BTreeMap<String, Fv>,
}

/// Parameters for inserting multiple documents.
#[derive(Debug, Deserialize)]
pub struct AddManyParams {
    /// Target collection name.
    pub collection: String,
    /// Document field maps, inserted in order.
    pub docs: Vec<BTreeMap<String, Fv>>,
}

/// Parameters identifying one document.
#[derive(Debug, Deserialize)]
pub struct DocumentIdParams {
    /// Target collection name.
    pub collection: String,
    /// Document primary key.
    pub _id: DocumentId,
}

/// Parameters identifying multiple documents.
#[derive(Debug, Deserialize)]
pub struct DocumentIdsParams {
    /// Target collection name.
    pub collection: String,
    /// Document primary keys.
    pub _ids: Vec<DocumentId>,
}

/// Parameters for partially updating one document.
#[derive(Debug, Deserialize)]
pub struct UpdateParams {
    /// Target collection name.
    pub collection: String,
    /// Document primary key.
    pub _id: DocumentId,
    /// Field updates to apply.
    pub fields: BTreeMap<String, Fv>,
}

/// Parameters for hybrid search.
#[derive(Debug, Deserialize)]
pub struct SearchParams {
    /// Target collection name.
    pub collection: String,
    /// Full-text/vector search, filter, and limit settings.
    pub query: Query,
}

/// Parameters for filtered document ID queries.
#[derive(Debug, Deserialize)]
pub struct QueryIdsParams {
    /// Target collection name.
    pub collection: String,
    /// Filter expression evaluated against B-Tree indexes.
    pub filter: Filter,
    /// Optional maximum number of IDs to return.
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Result of a document insert.
#[derive(Debug, Serialize)]
pub struct AddResult {
    /// Engine-assigned document ID.
    pub _id: DocumentId,
}

/// Converts numeric arrays into engine `Vector` values for `Vector`-typed
/// fields. Floats are interpreted as numeric values; integers are bf16 bit
/// patterns, matching the engine's wire convention so that documents read
/// from the server can be written back unchanged.
fn coerce_vector_fields(schema: &Schema, doc: &mut BTreeMap<String, Fv>) -> Result<(), ApiError> {
    fn is_vector(field_type: &FieldType) -> bool {
        match field_type {
            FieldType::Vector => true,
            FieldType::Option(inner) => is_vector(inner),
            _ => false,
        }
    }

    fn to_bf16(value: &Fv) -> Option<bf16> {
        match value {
            Fv::F64(v) => Some(bf16::from_f64(*v)),
            Fv::F32(v) => Some(bf16::from_f32(*v)),
            Fv::U64(v) => u16::try_from(*v).ok().map(bf16::from_bits),
            Fv::I64(v) => u16::try_from(*v).ok().map(bf16::from_bits),
            _ => None,
        }
    }

    for (name, value) in doc.iter_mut() {
        if let Fv::Array(elements) = value
            && let Some(field) = schema.get_field(name)
            && is_vector(field.r#type())
        {
            let vector = elements
                .iter()
                .map(to_bf16)
                .collect::<Option<Vec<bf16>>>()
                .ok_or_else(|| {
                    ApiError::bad_request(format!(
                        "field {name:?}: a Vector field expects an array of numbers"
                    ))
                })?;
            *value = Fv::Vector(vector);
        }
    }
    Ok(())
}

/// `doc.add`
pub async fn add(db: &AndaDB, params: AddParams) -> Result<AddResult, ApiError> {
    let collection = open(db, &params.collection).await?;
    let mut doc = params.doc;
    // Schema validation requires `_id`; the engine assigns the real ID.
    doc.entry("_id".to_string()).or_insert_with(|| 0u64.into());
    coerce_vector_fields(&collection.schema(), &mut doc)?;
    let id = collection.add_from(&doc).await?;
    Ok(AddResult { _id: id })
}

/// `doc.add_many` — inserts documents in order. Not atomic: on failure the
/// already-inserted documents remain and the error reports the failing index.
pub async fn add_many(db: &AndaDB, params: AddManyParams) -> Result<Vec<AddResult>, ApiError> {
    let collection = open(db, &params.collection).await?;
    let mut results = Vec::with_capacity(params.docs.len());
    for (i, mut doc) in params.docs.into_iter().enumerate() {
        doc.entry("_id".to_string()).or_insert_with(|| 0u64.into());
        coerce_vector_fields(&collection.schema(), &mut doc)?;
        match collection.add_from(&doc).await {
            Ok(id) => results.push(AddResult { _id: id }),
            Err(err) => {
                let mut api_err = ApiError::from(err);
                api_err.message = format!(
                    "doc.add_many failed at index {i} ({} documents inserted): {}",
                    results.len(),
                    api_err.message
                );
                return Err(api_err);
            }
        }
    }
    Ok(results)
}

/// `doc.get`
pub async fn get(db: &AndaDB, params: DocumentIdParams) -> Result<Fv, ApiError> {
    let collection = open(db, &params.collection).await?;
    Ok(collection.get_as(params._id).await?)
}

/// `doc.get_many` — returns one entry per requested ID, `null` for missing
/// documents.
pub async fn get_many(db: &AndaDB, params: DocumentIdsParams) -> Result<Vec<Option<Fv>>, ApiError> {
    let collection = open(db, &params.collection).await?;
    let mut docs = Vec::with_capacity(params._ids.len());
    for id in params._ids {
        match collection.get_as::<Fv>(id).await {
            Ok(doc) => docs.push(Some(doc)),
            Err(DBError::NotFound { .. }) => docs.push(None),
            Err(err) => return Err(err.into()),
        }
    }
    Ok(docs)
}

/// `doc.update` — returns the updated document.
pub async fn update(db: &AndaDB, params: UpdateParams) -> Result<Fv, ApiError> {
    let collection = open(db, &params.collection).await?;
    let mut fields = params.fields;
    coerce_vector_fields(&collection.schema(), &mut fields)?;
    let doc = collection.update(params._id, fields).await?;
    Ok(doc.try_into()?)
}

/// `doc.remove` — returns the removed document, or `null` if it did not exist.
pub async fn remove(db: &AndaDB, params: DocumentIdParams) -> Result<Option<Fv>, ApiError> {
    let collection = open(db, &params.collection).await?;
    match collection.remove(params._id).await? {
        Some(doc) => Ok(Some(doc.try_into()?)),
        None => Ok(None),
    }
}

/// `doc.exists`
pub async fn exists(db: &AndaDB, params: DocumentIdParams) -> Result<bool, ApiError> {
    let collection = open(db, &params.collection).await?;
    Ok(collection.contains(params._id))
}

/// `doc.count`
pub async fn count(db: &AndaDB, params: super::CollectionParams) -> Result<u64, ApiError> {
    let collection = open(db, &params.collection).await?;
    Ok(collection.len() as u64)
}

/// `doc.search` — returns matching documents.
pub async fn search(db: &AndaDB, params: SearchParams) -> Result<Vec<Fv>, ApiError> {
    let collection = open(db, &params.collection).await?;
    Ok(collection.search_as(params.query).await?)
}

/// `doc.search_ids` — returns matching document IDs only.
pub async fn search_ids(db: &AndaDB, params: SearchParams) -> Result<Vec<DocumentId>, ApiError> {
    let collection = open(db, &params.collection).await?;
    Ok(collection.search_ids(params.query).await?)
}

/// `doc.query_ids` — returns document IDs matching a B-Tree filter.
pub async fn query_ids(db: &AndaDB, params: QueryIdsParams) -> Result<Vec<DocumentId>, ApiError> {
    let collection = open(db, &params.collection).await?;
    Ok(collection.query_ids(params.filter, params.limit).await?)
}
