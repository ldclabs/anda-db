//! Document-scope methods: CRUD, hybrid search, and filtered ID queries.
//!
//! Documents travel on the wire as field maps (`{field_name: value}`).
//! CBOR encoding preserves binary values such as `bf16` vectors losslessly;
//! the engine assigns `_id` on insert and any client-provided value for it
//! is ignored.

use anda_db::{
    collection::{Collection, CollectionMetadata},
    database::AndaDB,
    error::DBError,
    query::{Filter, Query, RangeQuery},
    schema::{Document, DocumentId, FieldType, Fv, Schema, as_wildcard_map, bf16},
};
use anda_db_tfs::QueryType;
use futures::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

use super::collection::{ensure_writable, open};
use crate::{error::ApiError, state::AppState};

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
                    ApiError::invalid_input(format!(
                        "field {name:?}: a Vector field expects an array of numbers"
                    ))
                })?;
            *value = Fv::Vector(vector);
        }
    }
    Ok(())
}

fn prepare_document(
    collection: &Collection,
    mut doc: BTreeMap<String, Fv>,
) -> Result<Document, ApiError> {
    // Schema validation requires `_id`; the engine assigns the real ID.
    doc.entry("_id".to_string()).or_insert_with(|| 0u64.into());
    let schema = collection.schema();
    coerce_vector_fields(&schema, &mut doc)?;
    let validated =
        Document::try_from(schema, &doc).map_err(|err| ApiError::invalid_input(err.to_string()))?;
    validate_stored_vectors(
        collection,
        validated.schema().iter().filter_map(|field| {
            validated
                .get_field(field.name())
                .map(|value| (field.name(), value))
        }),
    )?;
    Ok(validated)
}

/// Classify only the typed index conflict; a storage AlreadyExists stays an
/// internal error. The engine owns uniqueness checks under its key locks.
fn document_write_error(err: DBError) -> ApiError {
    if let Some(index) = err.unique_index_conflict() {
        return ApiError::conflict(format!(
            "unique index {index:?} conflicts with an existing document"
        ));
    }
    err.into()
}

async fn add_validated_document(
    collection: &Collection,
    doc: Document,
) -> Result<DocumentId, ApiError> {
    collection.add(doc).await.map_err(document_write_error)
}

/// Validate supplied vectors before mutation, including values decoded from
/// native CBOR and bf16 bit patterns. Only indexed vectors have a dimension.
fn validate_stored_vectors<'a>(
    collection: &Collection,
    fields: impl IntoIterator<Item = (&'a str, &'a Fv)>,
) -> Result<(), ApiError> {
    for (name, value) in fields {
        if let Fv::Vector(vector) = value {
            if vector.iter().any(|v| !v.is_finite()) {
                return Err(ApiError::invalid_input(format!(
                    "field {name:?}: vector values must all be finite numbers"
                )));
            }
            if let Ok(index) = collection.get_hnsw_index(name)
                && vector.len() != index.dimension()
            {
                return Err(ApiError::invalid_input(format!(
                    "field {name:?}: expected vector dimension {}, got {}",
                    index.dimension(),
                    vector.len()
                )));
            }
        }
    }
    Ok(())
}

/// Validates the requested field updates and returns them in the canonical
/// shape the engine stores.
///
/// `Document::set_field` coerces too, so this is not what makes the write
/// well-formed; it is what turns a bad wire value into one `invalid_input`
/// naming the field before entering the engine mutation.
fn coerce_update_fields(
    schema: &Schema,
    fields: BTreeMap<String, Fv>,
) -> Result<BTreeMap<String, Fv>, ApiError> {
    if fields.is_empty() {
        return Err(ApiError::invalid_input(
            "doc.update requires at least one field",
        ));
    }
    if fields.contains_key(Schema::ID_KEY) {
        return Err(ApiError::invalid_input("document _id cannot be updated"));
    }
    // `FieldEntry::coerce`, not `validate`: `doc.add` sends every value
    // through `Document::try_from`'s CBOR coercion, so a `Bytes` field
    // accepts `[1, 2, 3]` there. Validating the raw wire value here would let
    // a client create a document it cannot then update.
    let mut coerced = BTreeMap::new();
    for (name, value) in fields {
        let field = schema.get_field(&name).ok_or_else(|| {
            ApiError::invalid_input(format!("field {name:?} is not declared in the schema"))
        })?;
        let value = field
            .coerce(value)
            .map_err(|err| ApiError::invalid_input(err.to_string()))?;
        coerced.insert(name, value);
    }
    Ok(coerced)
}

fn range_matches_field_type(field_type: &FieldType, query: &RangeQuery<Fv>) -> Option<bool> {
    match field_type {
        FieldType::I64 => Some(RangeQuery::<i64>::try_convert_from(query.clone()).is_ok()),
        FieldType::U64 => Some(RangeQuery::<u64>::try_convert_from(query.clone()).is_ok()),
        FieldType::Text => Some(RangeQuery::<String>::try_convert_from(query.clone()).is_ok()),
        FieldType::Bytes => Some(RangeQuery::<Vec<u8>>::try_convert_from(query.clone()).is_ok()),
        FieldType::Option(inner) => range_matches_field_type(inner, query),
        FieldType::Array(inner) if inner.len() == 1 => range_matches_field_type(&inner[0], query),
        // Only a *wildcard* map indexes its keys. A one-entry map declared by
        // a nested `FieldTyped` struct is not one, so `as_wildcard_map` is the
        // shared rule rather than a one-entry approximation of it.
        FieldType::Map(inner) => {
            range_matches_field_type(&as_wildcard_map(inner)?.0.field_type(), query)
        }
        // A persisted B-Tree with any other key type is an internal metadata
        // inconsistency, not a client query error.
        _ => None,
    }
}

fn validate_filter(metadata: &CollectionMetadata, filter: &Filter) -> Result<(), ApiError> {
    match filter {
        Filter::Field((name, query)) => {
            let field_type = if name == Schema::ID_KEY {
                &FieldType::U64
            } else {
                metadata
                    .btree_indexes
                    .get(name)
                    .ok_or_else(|| {
                        ApiError::invalid_query(format!(
                            "query requires B-Tree index {name:?}, but it does not exist"
                        ))
                    })?
                    .r#type()
            };
            match range_matches_field_type(field_type, query) {
                Some(true) => Ok(()),
                Some(false) => Err(ApiError::invalid_query(format!(
                    "filter value type does not match B-Tree index {name:?}"
                ))),
                None => {
                    log::error!(
                        action = "validate_filter",
                        collection = metadata.config.name,
                        index = name;
                        "persisted B-Tree metadata has an unsupported field type",
                    );
                    Err(ApiError::internal("internal server error"))
                }
            }
        }
        Filter::Or(filters) | Filter::And(filters) => {
            for filter in filters {
                validate_filter(metadata, filter)?;
            }
            Ok(())
        }
        Filter::Not(filter) => validate_filter(metadata, filter),
    }
}

fn validate_search_query(collection: &Collection, query: &Query) -> Result<(), ApiError> {
    query
        .validate_complexity()
        .map_err(ApiError::invalid_query)?;
    let metadata = collection.metadata();

    if let Some(search) = &query.search {
        if let Some(text) = &search.text {
            if metadata.bm25_indexes.is_empty() {
                return Err(ApiError::invalid_query(
                    "text search requires a BM25 index, but none exists",
                ));
            }
            if search.logical_search {
                QueryType::try_parse(text).map_err(ApiError::invalid_query)?;
            }
        }

        if let Some(vector) = &search.vector {
            validate_query_vector(vector)?;
            let mut compatible = false;
            for field in metadata.hnsw_indexes.keys() {
                // Metadata says the index exists. Failure to find its live
                // wrapper is therefore internal and goes through the
                // conservative engine-error mapping.
                let index = collection.get_hnsw_index(field)?;
                compatible |= index.dimension() == vector.len();
            }
            if !compatible && search.text.is_none() {
                return Err(ApiError::invalid_query(format!(
                    "no HNSW index matches query vector dimension {}",
                    vector.len()
                )));
            }
        }
    }

    if let Some(filter) = &query.filter {
        validate_filter(&metadata, filter)?;
    }
    Ok(())
}

fn validate_query_vector(vector: &[f32]) -> Result<(), ApiError> {
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(ApiError::invalid_query(
            "query vector values must all be finite numbers",
        ));
    }
    Ok(())
}

/// `doc.add`
pub async fn add(state: &AppState, db: &AndaDB, params: AddParams) -> Result<AddResult, ApiError> {
    let collection = open(state, db, &params.collection).await?;
    ensure_writable(&collection)?;
    let doc = prepare_document(&collection, params.doc)?;
    let id = add_validated_document(&collection, doc).await?;
    Ok(AddResult { _id: id })
}

/// Maximum number of documents accepted by a single `doc.add_many` call.
const MAX_ADD_MANY_DOCS: usize = 10_000;

fn add_many_error(index: usize, inserted: &[AddResult], mut error: ApiError) -> ApiError {
    let inserted: Vec<DocumentId> = inserted.iter().map(|result| result._id).collect();
    error.message = format!(
        "doc.add_many failed at index {index}: {}; inserted document ids: {}",
        error.message,
        serde_json::to_string(&inserted).unwrap_or_else(|_| "[]".to_string())
    );
    error
}

/// `doc.add_many` — inserts documents in order. Not atomic: on failure the
/// already-inserted documents remain and the error reports the failing index
/// together with the IDs of the documents that were inserted, so clients can
/// compensate.
pub async fn add_many(
    state: &AppState,
    db: &AndaDB,
    params: AddManyParams,
) -> Result<Vec<AddResult>, ApiError> {
    if params.docs.len() > MAX_ADD_MANY_DOCS {
        return Err(ApiError::invalid_input(format!(
            "doc.add_many accepts at most {MAX_ADD_MANY_DOCS} documents, got {}",
            params.docs.len()
        )));
    }

    let collection = open(state, db, &params.collection).await?;
    ensure_writable(&collection)?;
    let mut prepared = Vec::with_capacity(params.docs.len());
    for (i, doc) in params.docs.into_iter().enumerate() {
        prepared
            .push(prepare_document(&collection, doc).map_err(|err| add_many_error(i, &[], err))?);
    }

    let mut results = Vec::with_capacity(prepared.len());
    for (i, doc) in prepared.into_iter().enumerate() {
        match add_validated_document(&collection, doc).await {
            Ok(id) => results.push(AddResult { _id: id }),
            Err(error) => return Err(add_many_error(i, &results, error)),
        }
    }
    Ok(results)
}

/// `doc.get`
pub async fn get(state: &AppState, db: &AndaDB, params: DocumentIdParams) -> Result<Fv, ApiError> {
    let collection = open(state, db, &params.collection).await?;
    if !collection.contains(params._id) {
        return Err(ApiError::not_found(format!(
            "document {} not found",
            params._id
        )));
    }
    Ok(collection.get_as(params._id).await?)
}

/// Maximum number of IDs accepted by a single `doc.get_many` call.
///
/// Each ID costs one object-store fetch, and the request allocates a result
/// vector sized from the client-supplied list, so an uncapped batch turns a
/// 2 MiB body (roughly 230k IDs) into one request. The bound matches the
/// number of documents a single `doc.search` can return.
const MAX_GET_MANY_IDS: usize = 1_000;

/// `doc.get_many` — returns one entry per requested ID, `null` for missing
/// documents. Duplicate IDs are answered once per occurrence.
pub async fn get_many(
    state: &AppState,
    db: &AndaDB,
    params: DocumentIdsParams,
) -> Result<Vec<Option<Fv>>, ApiError> {
    if params._ids.len() > MAX_GET_MANY_IDS {
        return Err(ApiError::invalid_input(format!(
            "doc.get_many accepts at most {MAX_GET_MANY_IDS} ids, got {}",
            params._ids.len()
        )));
    }

    let collection = open(state, db, &params.collection).await?;
    let requested = params._ids.len();
    // Same shape as `Collection::search`'s id→document expansion: a batch
    // request would otherwise be N serial object-store round trips.
    let mut stream = futures::stream::iter(params._ids)
        .map(|id| {
            let collection = collection.clone();
            async move {
                if !collection.contains(id) {
                    return Ok(None);
                }
                collection.get_as::<Fv>(id).await.map(Some)
            }
        })
        .buffered(8);

    let mut docs = Vec::with_capacity(requested);
    while let Some(doc) = stream.next().await {
        docs.push(doc?);
    }
    Ok(docs)
}

/// `doc.update` — returns the updated document.
pub async fn update(state: &AppState, db: &AndaDB, params: UpdateParams) -> Result<Fv, ApiError> {
    let collection = open(state, db, &params.collection).await?;
    ensure_writable(&collection)?;
    if !collection.contains(params._id) {
        return Err(ApiError::not_found(format!(
            "document {} not found",
            params._id
        )));
    }
    let mut fields = params.fields;
    coerce_vector_fields(&collection.schema(), &mut fields)?;
    let fields = coerce_update_fields(&collection.schema(), fields)?;
    validate_stored_vectors(
        &collection,
        fields.iter().map(|(name, value)| (name.as_str(), value)),
    )?;
    let doc = match collection.update(params._id, fields).await {
        Ok(doc) => doc,
        // In `Collection::update`, a Precondition can only come from the
        // versioned PUT of this already-loaded document. This operation
        // context proves a logical concurrent-update conflict; the physical
        // path and source still stay in server logs.
        Err(err @ DBError::Precondition { .. }) => {
            log::warn!(
                action = "document::update",
                collection = params.collection,
                document_id = params._id;
                "concurrent document update conflict: {err:?}",
            );
            return Err(ApiError::conflict(
                "document changed concurrently; reload it and retry",
            ));
        }
        Err(err) => return Err(document_write_error(err)),
    };
    Ok(doc.try_into()?)
}

/// `doc.remove` — returns the removed document, or `null` if it did not exist.
pub async fn remove(
    state: &AppState,
    db: &AndaDB,
    params: DocumentIdParams,
) -> Result<Option<Fv>, ApiError> {
    let collection = open(state, db, &params.collection).await?;
    ensure_writable(&collection)?;
    match collection.remove(params._id).await? {
        Some(doc) => Ok(Some(doc.try_into()?)),
        None => Ok(None),
    }
}

/// `doc.exists`
pub async fn exists(
    state: &AppState,
    db: &AndaDB,
    params: DocumentIdParams,
) -> Result<bool, ApiError> {
    let collection = open(state, db, &params.collection).await?;
    Ok(collection.contains(params._id))
}

/// `doc.count`
pub async fn count(
    state: &AppState,
    db: &AndaDB,
    params: super::CollectionParams,
) -> Result<u64, ApiError> {
    let collection = open(state, db, &params.collection).await?;
    Ok(collection.len() as u64)
}

/// `doc.search` — returns matching documents.
pub async fn search(
    state: &AppState,
    db: &AndaDB,
    params: SearchParams,
) -> Result<Vec<Fv>, ApiError> {
    let collection = open(state, db, &params.collection).await?;
    validate_search_query(&collection, &params.query)?;
    Ok(collection.search_as(params.query).await?)
}

/// `doc.search_ids` — returns matching document IDs only.
pub async fn search_ids(
    state: &AppState,
    db: &AndaDB,
    params: SearchParams,
) -> Result<Vec<DocumentId>, ApiError> {
    let collection = open(state, db, &params.collection).await?;
    validate_search_query(&collection, &params.query)?;
    Ok(collection.search_ids(params.query).await?)
}

/// Maximum number of document IDs a single `doc.query_ids` call may return.
///
/// An omitted `limit` used to mean "every matching ID", so a one-line request
/// with a broad filter returned one `DocumentId` per document in the
/// collection in a single response body. It now means this bound, matching
/// the engine's own `Collection::MAX_SEARCH_LIMIT`; an explicit `0` keeps its
/// "no data requested" meaning.
const MAX_QUERY_IDS: usize = 1_000;

/// `doc.query_ids` — returns the **smallest** document IDs matching a B-Tree
/// filter.
pub async fn query_ids(
    state: &AppState,
    db: &AndaDB,
    params: QueryIdsParams,
) -> Result<Vec<DocumentId>, ApiError> {
    let (collection, limit) = prepare_query_ids(state, db, &params, "doc.query_ids").await?;
    Ok(collection.query_ids(params.filter, Some(limit)).await?)
}

/// `doc.query_last_ids` — returns the **largest** document IDs matching a
/// B-Tree filter, for newest-first cursor pagination.
///
/// Identical to `doc.query_ids` except for which end of the match set it
/// keeps; without it a client paginating newest-first has to fetch every
/// matching ID and sort them itself.
pub async fn query_last_ids(
    state: &AppState,
    db: &AndaDB,
    params: QueryIdsParams,
) -> Result<Vec<DocumentId>, ApiError> {
    let (collection, limit) = prepare_query_ids(state, db, &params, "doc.query_last_ids").await?;
    Ok(collection
        .query_last_ids(params.filter, Some(limit))
        .await?)
}

async fn prepare_query_ids(
    state: &AppState,
    db: &AndaDB,
    params: &QueryIdsParams,
    method: &str,
) -> Result<(Arc<Collection>, usize), ApiError> {
    let limit = match params.limit {
        Some(limit) if limit > MAX_QUERY_IDS => {
            return Err(ApiError::invalid_input(format!(
                "{method} accepts a limit of at most {MAX_QUERY_IDS}, got {limit}"
            )));
        }
        Some(limit) => limit,
        None => MAX_QUERY_IDS,
    };

    let collection = open(state, db, &params.collection).await?;
    params
        .filter
        .validate_complexity()
        .map_err(ApiError::invalid_query)?;
    validate_filter(&collection.metadata(), &params.filter)?;
    Ok((collection, limit))
}

#[cfg(test)]
mod tests {
    use super::*;
    use anda_db::{
        collection::CollectionConfig,
        schema::{FieldEntry, FieldType},
    };
    use axum::http::StatusCode;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    #[test]
    fn storage_collisions_are_not_classified_as_unique_index_conflicts() {
        let error = document_write_error(DBError::AlreadyExists {
            name: "document".into(),
            path: "/private/storage".into(),
            source: "private backend error".into(),
            _id: 1,
        });
        assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(error.message, "internal server error");
    }

    #[test]
    fn non_finite_query_vectors_are_explicit_invalid_query_errors() {
        for value in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            let error = validate_query_vector(&[0.0, value]).unwrap_err();
            assert_eq!(error.status, StatusCode::BAD_REQUEST);
            assert_eq!(error.code, "invalid_query");
            assert_eq!(
                error.message,
                "query vector values must all be finite numbers"
            );
        }
    }

    #[tokio::test]
    async fn unique_index_conflicts_are_explicit_and_sanitized() {
        let state = AppState::connect(
            Arc::new(InMemory::new()),
            crate::ServerOptions {
                primary_db: "unique_conflicts".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let db = state.get_db("unique_conflicts").await.unwrap();

        let mut schema = Schema::builder();
        schema
            .add_field(
                FieldEntry::new("slug".to_string(), FieldType::Text)
                    .unwrap()
                    .with_unique(),
            )
            .unwrap();
        db.create_collection(
            schema.build().unwrap(),
            CollectionConfig {
                name: "articles".to_string(),
                description: String::new(),
            },
            async |collection| collection.create_btree_index_nx(&["slug"]).await,
        )
        .await
        .unwrap();

        let add_slug = |slug: &str| AddParams {
            collection: "articles".to_string(),
            doc: BTreeMap::from([("slug".to_string(), Fv::Text(slug.to_string()))]),
        };
        let first = add(&state, &db, add_slug("first")).await.unwrap();
        let second = add(&state, &db, add_slug("second")).await.unwrap();

        let duplicate = add(&state, &db, add_slug("first")).await.unwrap_err();
        assert_eq!(duplicate.status, StatusCode::CONFLICT);
        assert_eq!(duplicate.code, "conflict");
        assert_eq!(
            duplicate.message,
            "unique index \"slug\" conflicts with an existing document"
        );

        let duplicate = update(
            &state,
            &db,
            UpdateParams {
                collection: "articles".to_string(),
                _id: second._id,
                fields: BTreeMap::from([("slug".to_string(), Fv::Text("first".to_string()))]),
            },
        )
        .await
        .unwrap_err();
        assert_eq!(duplicate.status, StatusCode::CONFLICT);
        assert_eq!(duplicate.code, "conflict");
        assert!(!duplicate.message.contains("data/"));
        assert_ne!(first._id, second._id);

        state.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn add_many_prevalidates_all_vector_coercions_before_inserting() {
        let state = AppState::connect(
            Arc::new(InMemory::new()),
            crate::ServerOptions {
                primary_db: "add_many_prevalidation".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let db = state.get_db("add_many_prevalidation").await.unwrap();

        let mut schema = Schema::builder();
        schema
            .add_field(FieldEntry::new("text".to_string(), FieldType::Text).unwrap())
            .unwrap();
        schema
            .add_field(FieldEntry::new("embedding".to_string(), FieldType::Vector).unwrap())
            .unwrap();
        let collection = db
            .create_collection(
                schema.build().unwrap(),
                CollectionConfig {
                    name: "memories".to_string(),
                    description: String::new(),
                },
                async |_| Ok(()),
            )
            .await
            .unwrap();

        let valid = BTreeMap::from([
            ("text".to_string(), Fv::Text("valid".to_string())),
            (
                "embedding".to_string(),
                Fv::Array(vec![Fv::F64(1.0), Fv::F64(0.0)]),
            ),
        ]);
        let invalid = BTreeMap::from([
            ("text".to_string(), Fv::Text("invalid".to_string())),
            (
                "embedding".to_string(),
                Fv::Array(vec![Fv::F64(0.0), Fv::Text("not-a-number".to_string())]),
            ),
        ]);

        let error = add_many(
            &state,
            &db,
            AddManyParams {
                collection: "memories".to_string(),
                docs: vec![valid, invalid],
            },
        )
        .await
        .unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(error.message.contains("failed at index 1"));
        assert!(error.message.contains("inserted document ids: []"));
        assert_eq!(
            collection.len(),
            0,
            "prevalidation must prevent partial writes"
        );

        state.shutdown().await.unwrap();
    }
}
