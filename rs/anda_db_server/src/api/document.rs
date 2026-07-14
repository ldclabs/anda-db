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
    index::{from_virtual_field_name, virtual_field_value},
    query::{Filter, Query, RangeQuery},
    schema::{Document, DocumentId, FieldType, Fv, Schema, bf16},
};
use anda_db_tfs::QueryType;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::collection::{ensure_writable, open};
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
    Ok(validated)
}

fn default_btree_values(fields: &[String], doc: &Document) -> Vec<Fv> {
    match fields {
        [] => Vec::new(),
        [name] => match doc.get_field(name) {
            Some(Fv::Array(values)) => values.clone(),
            Some(Fv::Map(values)) => values.keys().cloned().map(Fv::from).collect(),
            Some(value) => vec![value.clone()],
            None => Vec::new(),
        },
        _ => {
            let values: Vec<Option<&Fv>> = fields.iter().map(|name| doc.get_field(name)).collect();
            virtual_field_value(&values).into_iter().collect()
        }
    }
}

/// Returns the first unique B-Tree whose current postings prove a logical
/// conflict for `doc`. The server always installs the default index hooks, so
/// deriving values here mirrors the collection's single-field array/map
/// expansion and composite-key encoding.
fn find_unique_conflict(
    collection: &Collection,
    doc: &Document,
    excluding_id: Option<DocumentId>,
) -> Result<Option<String>, ApiError> {
    for name in collection.metadata().btree_indexes.keys() {
        let fields = from_virtual_field_name(name);
        let field_refs: Vec<&str> = fields.iter().map(String::as_str).collect();
        let index = collection.get_btree_index(&field_refs)?;
        if index.allow_duplicates() {
            continue;
        }

        for value in default_btree_values(index.virtual_field(), doc) {
            if value == Fv::Null {
                continue;
            }
            let conflicts = index
                .query_with(&value, |ids| {
                    Some(ids.iter().any(|id| Some(*id) != excluding_id))
                })
                .unwrap_or(false);
            if conflicts {
                return Ok(Some(index.name().to_string()));
            }
        }
    }
    Ok(None)
}

fn unique_conflict_error(index: &str) -> ApiError {
    ApiError::conflict(format!(
        "unique index {index:?} conflicts with an existing document"
    ))
}

async fn add_validated_document(
    collection: &Collection,
    doc: Document,
) -> Result<DocumentId, ApiError> {
    if let Some(index) = find_unique_conflict(collection, &doc, None)? {
        return Err(unique_conflict_error(&index));
    }

    let conflict_doc = doc.clone();
    match collection.add(doc).await {
        Ok(id) => Ok(id),
        Err(err @ DBError::AlreadyExists { .. }) => {
            // `Collection::add` can also receive AlreadyExists from storage.
            // Re-read the logical unique indexes before classifying this as a
            // client conflict; otherwise retain the generic engine fallback.
            log::warn!(
                action = "document::add",
                collection = collection.name();
                "document add returned AlreadyExists: {err:?}",
            );
            if let Some(index) = find_unique_conflict(collection, &conflict_doc, None)? {
                Err(unique_conflict_error(&index))
            } else {
                Err(err.into())
            }
        }
        Err(err) => Err(err.into()),
    }
}

fn validate_update_fields(schema: &Schema, fields: &BTreeMap<String, Fv>) -> Result<(), ApiError> {
    if fields.is_empty() {
        return Err(ApiError::invalid_input(
            "doc.update requires at least one field",
        ));
    }
    if fields.contains_key(Schema::ID_KEY) {
        return Err(ApiError::invalid_input("document _id cannot be updated"));
    }
    for (name, value) in fields {
        let field = schema.get_field(name).ok_or_else(|| {
            ApiError::invalid_input(format!("field {name:?} is not declared in the schema"))
        })?;
        field
            .validate(value)
            .map_err(|err| ApiError::invalid_input(err.to_string()))?;
    }
    Ok(())
}

fn proposed_update(
    mut current: Document,
    fields: &BTreeMap<String, Fv>,
) -> Result<Document, ApiError> {
    for (name, value) in fields {
        current
            .set_field(name, value.clone())
            .map_err(|err| ApiError::invalid_input(err.to_string()))?;
    }
    Ok(current)
}

fn range_matches_field_type(field_type: &FieldType, query: &RangeQuery<Fv>) -> Option<bool> {
    match field_type {
        FieldType::I64 => Some(RangeQuery::<i64>::try_convert_from(query.clone()).is_ok()),
        FieldType::U64 => Some(RangeQuery::<u64>::try_convert_from(query.clone()).is_ok()),
        FieldType::Text => Some(RangeQuery::<String>::try_convert_from(query.clone()).is_ok()),
        FieldType::Bytes => Some(RangeQuery::<Vec<u8>>::try_convert_from(query.clone()).is_ok()),
        FieldType::Option(inner) => range_matches_field_type(inner, query),
        FieldType::Array(inner) if inner.len() == 1 => range_matches_field_type(&inner[0], query),
        FieldType::Map(inner) if inner.len() == 1 => {
            let key_type = inner.keys().next()?.field_type();
            range_matches_field_type(&key_type, query)
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
pub async fn add(db: &AndaDB, params: AddParams) -> Result<AddResult, ApiError> {
    let collection = open(db, &params.collection).await?;
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
pub async fn add_many(db: &AndaDB, params: AddManyParams) -> Result<Vec<AddResult>, ApiError> {
    if params.docs.len() > MAX_ADD_MANY_DOCS {
        return Err(ApiError::invalid_input(format!(
            "doc.add_many accepts at most {MAX_ADD_MANY_DOCS} documents, got {}",
            params.docs.len()
        )));
    }

    let collection = open(db, &params.collection).await?;
    ensure_writable(&collection)?;
    let schema = collection.schema();
    let mut docs = params.docs;
    let mut prepared = Vec::with_capacity(docs.len());

    // Coerce every document before the first durable insert. Otherwise a
    // malformed vector in a later document bypasses the partial-success
    // envelope and leaves the caller without the IDs needed for
    // compensation.
    for (i, doc) in docs.iter_mut().enumerate() {
        doc.entry("_id".to_string()).or_insert_with(|| 0u64.into());
        if let Err(error) = coerce_vector_fields(&schema, doc) {
            return Err(add_many_error(i, &[], error));
        }
        match Document::try_from(schema.clone(), doc)
            .map_err(|err| ApiError::invalid_input(err.to_string()))
        {
            Ok(doc) => prepared.push(doc),
            Err(error) => return Err(add_many_error(i, &[], error)),
        }
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
pub async fn get(db: &AndaDB, params: DocumentIdParams) -> Result<Fv, ApiError> {
    let collection = open(db, &params.collection).await?;
    if !collection.contains(params._id) {
        return Err(ApiError::not_found(format!(
            "document {} not found",
            params._id
        )));
    }
    Ok(collection.get_as(params._id).await?)
}

/// `doc.get_many` — returns one entry per requested ID, `null` for missing
/// documents.
pub async fn get_many(db: &AndaDB, params: DocumentIdsParams) -> Result<Vec<Option<Fv>>, ApiError> {
    let collection = open(db, &params.collection).await?;
    let mut docs = Vec::with_capacity(params._ids.len());
    for id in params._ids {
        if !collection.contains(id) {
            docs.push(None);
            continue;
        }
        match collection.get_as::<Fv>(id).await {
            Ok(doc) => docs.push(Some(doc)),
            Err(err) => return Err(err.into()),
        }
    }
    Ok(docs)
}

/// `doc.update` — returns the updated document.
pub async fn update(db: &AndaDB, params: UpdateParams) -> Result<Fv, ApiError> {
    let collection = open(db, &params.collection).await?;
    ensure_writable(&collection)?;
    if !collection.contains(params._id) {
        return Err(ApiError::not_found(format!(
            "document {} not found",
            params._id
        )));
    }
    let mut fields = params.fields;
    coerce_vector_fields(&collection.schema(), &mut fields)?;
    validate_update_fields(&collection.schema(), &fields)?;
    let prospective = proposed_update(collection.get(params._id).await?, &fields)?;
    if let Some(index) = find_unique_conflict(&collection, &prospective, Some(params._id))? {
        return Err(unique_conflict_error(&index));
    }
    let conflict_fields = fields.clone();
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
        Err(err @ DBError::AlreadyExists { .. }) => {
            log::warn!(
                action = "document::update",
                collection = params.collection,
                document_id = params._id;
                "document update returned AlreadyExists: {err:?}",
            );
            // Rebuild the proposed value from the latest durable document so a
            // concurrent update to an untouched composite-index field cannot
            // make the preflight stale. Only a confirmed posting collision is
            // exposed as 409; storage AlreadyExists remains generic 500.
            let latest = proposed_update(collection.get(params._id).await?, &conflict_fields)?;
            if let Some(index) = find_unique_conflict(&collection, &latest, Some(params._id))? {
                return Err(unique_conflict_error(&index));
            }
            return Err(err.into());
        }
        Err(err) => return Err(err.into()),
    };
    Ok(doc.try_into()?)
}

/// `doc.remove` — returns the removed document, or `null` if it did not exist.
pub async fn remove(db: &AndaDB, params: DocumentIdParams) -> Result<Option<Fv>, ApiError> {
    let collection = open(db, &params.collection).await?;
    ensure_writable(&collection)?;
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
    validate_search_query(&collection, &params.query)?;
    Ok(collection.search_as(params.query).await?)
}

/// `doc.search_ids` — returns matching document IDs only.
pub async fn search_ids(db: &AndaDB, params: SearchParams) -> Result<Vec<DocumentId>, ApiError> {
    let collection = open(db, &params.collection).await?;
    validate_search_query(&collection, &params.query)?;
    Ok(collection.search_ids(params.query).await?)
}

/// `doc.query_ids` — returns document IDs matching a B-Tree filter.
pub async fn query_ids(db: &AndaDB, params: QueryIdsParams) -> Result<Vec<DocumentId>, ApiError> {
    let collection = open(db, &params.collection).await?;
    params
        .filter
        .validate_complexity()
        .map_err(ApiError::invalid_query)?;
    validate_filter(&collection.metadata(), &params.filter)?;
    Ok(collection.query_ids(params.filter, params.limit).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anda_db::{
        collection::CollectionConfig,
        database::DBConfig,
        schema::{FieldEntry, FieldType},
        storage::StorageConfig,
    };
    use axum::http::StatusCode;
    use object_store::memory::InMemory;
    use std::sync::Arc;

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
        let db = AndaDB::connect(
            Arc::new(InMemory::new()),
            DBConfig {
                name: "unique_conflicts".to_string(),
                description: String::new(),
                storage: StorageConfig::default(),
                lock: None,
            },
        )
        .await
        .unwrap();

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
        let first = add(&db, add_slug("first")).await.unwrap();
        let second = add(&db, add_slug("second")).await.unwrap();

        let duplicate = add(&db, add_slug("first")).await.unwrap_err();
        assert_eq!(duplicate.status, StatusCode::CONFLICT);
        assert_eq!(duplicate.code, "conflict");
        assert_eq!(
            duplicate.message,
            "unique index \"slug\" conflicts with an existing document"
        );

        let duplicate = update(
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

        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn add_many_prevalidates_all_vector_coercions_before_inserting() {
        let db = AndaDB::connect(
            Arc::new(InMemory::new()),
            DBConfig {
                name: "add_many_prevalidation".to_string(),
                description: String::new(),
                storage: StorageConfig::default(),
                lock: None,
            },
        )
        .await
        .unwrap();

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

        db.close().await.unwrap();
    }
}
