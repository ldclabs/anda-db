use anda_db::{
    collection::{Collection, CollectionConfig},
    database::{AndaDB, DBConfig},
    error::DBError,
    index::{BM25, BTree, Hnsw, HnswConfig, default_tokenizer},
    query::{Filter, Query, RRFReranker, RangeQuery, Search},
    schema::{
        AndaDBSchema, ByteBufB64, Document, FieldEntry, FieldKey, FieldType, Fv, Schema,
        SchemaError, Vector, bf16,
    },
    storage::{PutMode, Storage, StorageConfig, StorageStats},
};
use async_trait::async_trait;
use bytes::Bytes;
use croaring::{Portable, Treemap};
use futures::{StreamExt, io::AsyncWriteExt as FuturesAsyncWriteExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
    memory::InMemory, path::Path,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt,
    io::IoSlice,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, AndaDBSchema)]
struct PublicDoc {
    _id: u64,
    name: String,
    age: u64,
    body: String,
    tags: Vec<String>,
    attrs: BTreeMap<String, u64>,
    embedding: Vector,
}

async fn test_db(name: &str) -> Result<AndaDB, DBError> {
    AndaDB::create(
        Arc::new(InMemory::new()),
        DBConfig {
            name: name.to_string(),
            description: "coverage public api".to_string(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None,
        },
    )
    .await
}

async fn raw_storage(name: &str, config: StorageConfig) -> Result<Storage, DBError> {
    Storage::connect(name.to_string(), Arc::new(InMemory::new()), config).await
}

fn collection_config(name: &str) -> CollectionConfig {
    CollectionConfig {
        name: name.to_string(),
        description: "coverage collection".to_string(),
    }
}

fn doc(id: u64, name: &str, age: u64) -> PublicDoc {
    PublicDoc {
        _id: id,
        name: name.to_string(),
        age,
        body: format!("{name} body text"),
        tags: vec![format!("tag-{age}")],
        attrs: BTreeMap::from([(format!("k{age}"), age)]),
        embedding: vec![0.1, 0.2, 0.3, 0.4]
            .into_iter()
            .map(bf16::from_f32)
            .collect(),
    }
}

fn id_filter(query: RangeQuery<Fv>) -> Filter {
    Filter::Field(("_id".to_string(), query))
}

fn query_filter(index: &str, query: RangeQuery<Fv>) -> Query {
    Query {
        filter: Some(Filter::Field((index.to_string(), query))),
        ..Default::default()
    }
}

fn vector(values: &[f32]) -> Vector {
    values.iter().copied().map(bf16::from_f32).collect()
}

fn named_doc(name: &str, age: u64, body: &str, embedding: &[f32]) -> PublicDoc {
    let mut item = doc(0, name, age);
    item.body = body.to_string();
    item.embedding = vector(embedding);
    item
}

fn db_config(name: &str, lock: Option<ByteBufB64>) -> DBConfig {
    DBConfig {
        name: name.to_string(),
        description: "coverage public api".to_string(),
        storage: StorageConfig {
            compress_level: 0,
            ..Default::default()
        },
        lock,
    }
}

fn schema_v1() -> Result<Schema, SchemaError> {
    let mut builder = Schema::builder();
    builder.with_version(1);
    builder.add_field(FieldEntry::new("name".to_string(), FieldType::Text)?)?;
    builder.add_field(FieldEntry::new("age".to_string(), FieldType::U64)?)?;
    builder.build()
}

fn schema_v2() -> Result<Schema, SchemaError> {
    let mut builder = Schema::builder();
    builder.with_version(2);
    builder.add_field(FieldEntry::new("name".to_string(), FieldType::Text)?)?;
    builder.add_field(FieldEntry::new("age".to_string(), FieldType::U64)?)?;
    builder.add_field(FieldEntry::new(
        "email".to_string(),
        FieldType::Option(Box::new(FieldType::Text)),
    )?)?;
    builder.build()
}

#[derive(Debug)]
struct FailPutStore {
    inner: Arc<InMemory>,
    fail_suffix: String,
    fail_next_put: Arc<AtomicBool>,
}

impl FailPutStore {
    fn new(fail_suffix: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(InMemory::new()),
            fail_suffix: fail_suffix.into(),
            fail_next_put: Arc::new(AtomicBool::new(false)),
        }
    }

    fn fail_next_put(&self) {
        self.fail_next_put.store(true, Ordering::Release);
    }
}

impl fmt::Display for FailPutStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FailPutStore")
    }
}

#[async_trait]
impl ObjectStore for FailPutStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        if location.to_string().ends_with(&self.fail_suffix)
            && self.fail_next_put.swap(false, Ordering::AcqRel)
        {
            return Err(object_store::Error::Generic {
                store: "fail_put",
                source: "injected put failure".into(),
            });
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

async fn create_indexed_collection(db: &AndaDB, name: &str) -> Result<Arc<Collection>, DBError> {
    db.create_collection(
        PublicDoc::schema()?,
        collection_config(name),
        async |collection| {
            assert!(matches!(
                collection.create_btree_index(&[]).await,
                Err(DBError::Schema { .. })
            ));
            assert!(matches!(
                collection.create_btree_index(&["missing"]).await,
                Err(DBError::Schema { .. } | DBError::NotFound { .. })
            ));

            collection.create_btree_index(&["age"]).await?;
            assert!(matches!(
                collection.create_btree_index(&["age"]).await,
                Err(DBError::AlreadyExists { .. })
            ));
            collection.create_btree_index_nx(&["age"]).await?;

            collection.create_btree_index(&["name", "age"]).await?;
            assert!(collection.get_btree_index(&["name", "age"]).is_ok());

            assert!(matches!(
                collection.create_bm25_index(&[]).await,
                Err(DBError::Schema { .. })
            ));
            collection.create_bm25_index(&["body"]).await?;
            assert!(matches!(
                collection.create_bm25_index(&["body"]).await,
                Err(DBError::AlreadyExists { .. })
            ));
            collection.create_bm25_index_nx(&["body"]).await?;

            assert!(matches!(
                collection
                    .create_hnsw_index("bad-name", HnswConfig::default())
                    .await,
                Err(DBError::Schema { .. })
            ));
            assert!(matches!(
                collection
                    .create_hnsw_index("missing", HnswConfig::default())
                    .await,
                Err(DBError::NotFound { .. })
            ));
            assert!(matches!(
                collection
                    .create_hnsw_index("age", HnswConfig::default())
                    .await,
                Err(DBError::Schema { .. })
            ));
            collection
                .create_hnsw_index(
                    "embedding",
                    HnswConfig {
                        dimension: 4,
                        ..Default::default()
                    },
                )
                .await?;
            collection
                .create_hnsw_index_nx(
                    "embedding",
                    HnswConfig {
                        dimension: 4,
                        ..Default::default()
                    },
                )
                .await?;

            assert!(matches!(
                collection.get_btree_index(&["unknown"]),
                Err(DBError::Index { .. })
            ));
            assert!(matches!(
                collection.get_bm25_index(&["unknown"]),
                Err(DBError::Index { .. })
            ));
            assert!(matches!(
                collection.get_hnsw_index("unknown"),
                Err(DBError::Index { .. })
            ));
            assert!(matches!(
                collection.compact_btree_index(&["unknown"]).await,
                Err(DBError::Index { .. })
            ));
            assert!(matches!(
                collection.compact_bm25_index(&["unknown"]).await,
                Err(DBError::Index { .. })
            ));

            assert!(!collection.remove_btree_index(&["not_present"]).await?);
            assert!(!collection.remove_bm25_index(&["not_present"]).await?);
            assert!(!collection.remove_hnsw_index("not_present").await?);
            assert!(matches!(
                collection.remove_btree_index(&[]).await,
                Err(DBError::Schema { .. })
            ));
            assert!(matches!(
                collection.remove_bm25_index(&[]).await,
                Err(DBError::Schema { .. })
            ));
            assert!(matches!(
                collection.remove_hnsw_index("").await,
                Err(DBError::Schema { .. })
            ));

            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn collection_public_error_paths_and_id_filters() -> Result<(), DBError> {
    let db = test_db("coverage_collection_db").await?;
    let collection = create_indexed_collection(&db, "docs").await?;

    assert_eq!(format!("{collection:?}"), "Collection(docs)");
    assert!(collection.is_empty());
    assert_eq!(collection.len(), 0);
    assert_eq!(collection.max_document_id(), 0);
    assert_eq!(collection.latest_document_id(), None);
    assert_eq!(collection.ids(), Vec::<u64>::new());
    assert!(!collection.contains(1));
    assert_eq!(collection.storage_stats().check_point, 0);
    assert!(!collection.tokenize("alpha beta").is_empty());

    collection.set_read_only(true);
    assert!(matches!(
        collection.add_from(&doc(0, "blocked", 1)).await,
        Err(DBError::Generic { .. })
    ));
    assert!(matches!(
        collection
            .update(1, BTreeMap::from([("age".to_string(), Fv::U64(2))]))
            .await,
        Err(DBError::Generic { .. })
    ));
    assert!(matches!(
        collection.remove(1).await,
        Err(DBError::Generic { .. })
    ));
    collection.set_read_only(false);

    for age in 1..=5 {
        collection
            .add_from(&doc(0, &format!("doc{age}"), age))
            .await?;
    }

    assert_eq!(collection.len(), 5);
    assert_eq!(collection.max_document_id(), 5);
    assert_eq!(collection.latest_document_id(), Some(5));
    assert_eq!(collection.ids(), vec![1, 2, 3, 4, 5]);
    assert!(collection.contains(3));

    assert!(matches!(
        collection
            .update(99, BTreeMap::from([("age".to_string(), Fv::U64(9))]))
            .await,
        Err(DBError::NotFound { .. })
    ));
    assert!(matches!(
        collection.update(1, BTreeMap::new()).await,
        Err(DBError::Generic { .. })
    ));

    let updated = collection
        .update(1, BTreeMap::from([("age".to_string(), Fv::U64(42))]))
        .await?;
    assert_eq!(updated.get_field_as::<u64>("age")?, 42);
    assert_eq!(collection.get_as::<PublicDoc>(1).await?.age, 42);
    assert!(matches!(
        collection.get(99).await,
        Err(DBError::NotFound { .. })
    ));
    assert!(collection.remove(99).await?.is_none());

    assert_eq!(
        collection
            .query_ids(id_filter(RangeQuery::Eq(Fv::U64(2))), None)
            .await?,
        vec![2]
    );
    assert_eq!(
        collection
            .query_ids(id_filter(RangeQuery::Gt(Fv::U64(3))), Some(2))
            .await?,
        vec![4, 5]
    );
    assert_eq!(
        collection
            .query_ids(id_filter(RangeQuery::Ge(Fv::U64(4))), None)
            .await?,
        vec![4, 5]
    );
    assert_eq!(
        collection
            .query_ids(id_filter(RangeQuery::Lt(Fv::U64(4))), Some(2))
            .await?,
        vec![2, 3]
    );
    assert_eq!(
        collection
            .query_ids(id_filter(RangeQuery::Le(Fv::U64(3))), Some(2))
            .await?,
        vec![2, 3]
    );
    assert_eq!(
        collection
            .query_ids(id_filter(RangeQuery::Between(Fv::U64(2), Fv::U64(4))), None,)
            .await?,
        vec![2, 3, 4]
    );
    assert_eq!(
        collection
            .query_ids(
                id_filter(RangeQuery::Include(vec![Fv::U64(5), Fv::U64(1)])),
                None,
            )
            .await?,
        vec![1, 5]
    );

    let or_filter = Filter::Or(vec![
        Box::new(id_filter(RangeQuery::Eq(Fv::U64(1)))),
        Box::new(id_filter(RangeQuery::Eq(Fv::U64(5)))),
    ]);
    assert_eq!(collection.query_ids(or_filter, None).await?, vec![1, 5]);

    let and_filter = Filter::And(vec![
        Box::new(id_filter(RangeQuery::Ge(Fv::U64(2)))),
        Box::new(id_filter(RangeQuery::Le(Fv::U64(4)))),
    ]);
    assert_eq!(collection.query_ids(and_filter, None).await?, vec![2, 3, 4]);

    let not_filter = Filter::Not(Box::new(id_filter(RangeQuery::Eq(Fv::U64(3)))));
    assert_eq!(
        collection.query_ids(not_filter, Some(3)).await?,
        vec![1, 2, 4]
    );

    assert_eq!(
        collection
            .query_ids(
                id_filter(RangeQuery::And(vec![
                    Box::new(RangeQuery::Ge(Fv::U64(2))),
                    Box::new(RangeQuery::Le(Fv::U64(4))),
                ])),
                None,
            )
            .await?,
        vec![2, 3, 4]
    );
    assert_eq!(
        collection
            .query_ids(
                id_filter(RangeQuery::Or(vec![
                    Box::new(RangeQuery::Eq(Fv::U64(1))),
                    Box::new(RangeQuery::Eq(Fv::U64(4))),
                ])),
                None,
            )
            .await?,
        vec![1, 4]
    );
    assert_eq!(
        collection
            .query_ids(
                id_filter(RangeQuery::Not(Box::new(RangeQuery::Eq(Fv::U64(3))))),
                Some(3),
            )
            .await?,
        vec![1, 2, 4]
    );
    assert_eq!(
        collection
            .query_ids(
                id_filter(RangeQuery::Or(vec![
                    Box::new(RangeQuery::Lt(Fv::U64(4))),
                    Box::new(RangeQuery::Le(Fv::U64(5))),
                ])),
                Some(2),
            )
            .await?,
        vec![2, 3]
    );

    assert!(matches!(
        collection
            .query_ids(
                Filter::Field(("missing_index".to_string(), RangeQuery::Eq(Fv::U64(1)))),
                None,
            )
            .await,
        Err(DBError::Index { .. })
    ));

    let search_none = collection
        .search_ids(Query {
            search: None,
            filter: None,
            limit: Some(2),
        })
        .await?;
    assert!(search_none.is_empty());
    assert_eq!(
        collection
            .search_ids(Query {
                search: Some(Search {
                    text: Some("body".to_string()),
                    ..Default::default()
                }),
                filter: Some(id_filter(RangeQuery::Lt(Fv::U64(5)))),
                limit: Some(2),
            })
            .await?,
        vec![3, 4]
    );

    Ok(())
}

#[tokio::test]
async fn collection_search_update_remove_and_extensions() -> Result<(), DBError> {
    let db = test_db("coverage_collection_search_db").await?;
    let collection = db
        .create_collection(
            PublicDoc::schema()?,
            collection_config("docs"),
            async |collection| {
                collection.create_btree_index(&["_id"]).await?;
                collection.create_btree_index(&["age"]).await?;
                collection.create_bm25_index(&["body", "tags"]).await?;
                collection
                    .create_hnsw_index(
                        "embedding",
                        HnswConfig {
                            dimension: 4,
                            ..Default::default()
                        },
                    )
                    .await?;
                Ok(())
            },
        )
        .await?;

    let alpha = named_doc(
        "alpha",
        10,
        "rust agent database memory",
        &[0.1, 0.2, 0.3, 0.4],
    );
    let beta = named_doc(
        "beta",
        20,
        "rust vector database search",
        &[0.4, 0.3, 0.2, 0.1],
    );
    let gamma = named_doc("gamma", 30, "logical planner memory", &[0.2, 0.2, 0.2, 0.2]);
    let alpha_id = collection.add_from(&alpha).await?;
    let beta_id = collection.add_from(&beta).await?;
    let gamma_id = collection.add_from(&gamma).await?;
    assert_eq!((alpha_id, beta_id, gamma_id), (1, 2, 3));

    let logical_ids = collection
        .search_ids(Query {
            search: Some(Search {
                text: Some("rust AND database".to_string()),
                logical_search: true,
                ..Default::default()
            }),
            limit: Some(5),
            ..Default::default()
        })
        .await?;
    assert!(logical_ids.contains(&alpha_id));
    assert!(logical_ids.contains(&beta_id));

    let filtered_candidates = collection
        .search_ids(Query {
            search: Some(Search {
                text: Some("database memory".to_string()),
                vector: Some(vec![0.1, 0.2, 0.3, 0.4]),
                reranker: Some(RRFReranker::new(10.0)),
                ..Default::default()
            }),
            filter: Some(Filter::Field((
                "age".to_string(),
                RangeQuery::Ge(Fv::U64(20)),
            ))),
            limit: Some(2),
        })
        .await?;
    assert!(filtered_candidates.iter().all(|id| *id != alpha_id));
    assert!(filtered_candidates.contains(&beta_id) || filtered_candidates.contains(&gamma_id));

    let updated_vector = vector(&[0.9, 0.8, 0.7, 0.6]);
    collection
        .update(
            beta_id,
            BTreeMap::from([
                (
                    "body".to_string(),
                    Fv::Text("updated logical database".to_string()),
                ),
                ("embedding".to_string(), Fv::Vector(updated_vector.clone())),
            ]),
        )
        .await?;

    let updated_text = collection
        .search_ids(Query {
            search: Some(Search {
                text: Some("updated AND database".to_string()),
                logical_search: true,
                ..Default::default()
            }),
            limit: Some(5),
            ..Default::default()
        })
        .await?;
    assert_eq!(updated_text, vec![beta_id]);
    assert!(
        collection
            .search_ids(Query {
                search: Some(Search {
                    vector: Some(updated_vector.iter().map(|v| v.to_f32()).collect()),
                    ..Default::default()
                }),
                limit: Some(1),
                ..Default::default()
            })
            .await?
            .contains(&beta_id)
    );

    let missing_after_remove = collection
        .remove(gamma_id)
        .await?
        .expect("existing document should be returned");
    assert_eq!(
        missing_after_remove.get_field_as::<String>("name")?,
        "gamma"
    );
    assert!(
        !collection
            .search_ids(query_filter(
                "age",
                RangeQuery::Include(vec![Fv::U64(30), Fv::U64(10)])
            ))
            .await?
            .contains(&gamma_id)
    );

    collection.set_extension_from("typed".to_string(), vec![1_u64, 2, 3]);
    assert_eq!(
        collection.get_extension_as::<Vec<u64>>("typed"),
        Some(vec![1, 2, 3])
    );
    assert_eq!(
        collection.set_extension_from_with::<_, u64>("counter".to_string(), |old| {
            Some(old.unwrap_or(0) + 1)
        }),
        None
    );
    assert_eq!(
        collection.set_extension_from_with::<_, u64>("counter".to_string(), |old| {
            Some(old.unwrap_or(0) + 1)
        }),
        Some(1)
    );
    assert!(collection.extensions_with(|extensions| extensions.contains_key("counter")));
    collection
        .save_extension_from(
            "saved".to_string(),
            &BTreeMap::from([("ok".to_string(), true)]),
        )
        .await?;

    db.close().await?;
    Ok(())
}

#[tokio::test]
async fn collection_auto_repair_recovers_dirty_documents_and_indexes() -> Result<(), DBError> {
    let store = Arc::new(InMemory::new());
    let config = db_config("coverage_repair_db", None);
    let db = AndaDB::create(store.clone(), config.clone()).await?;
    let schema = PublicDoc::schema()?;
    db.create_collection(
        schema.clone(),
        collection_config("docs"),
        async |collection| {
            collection.create_btree_index(&["_id"]).await?;
            collection.create_btree_index(&["age"]).await?;
            collection.create_bm25_index(&["body"]).await?;
            collection
                .create_hnsw_index(
                    "embedding",
                    HnswConfig {
                        dimension: 4,
                        ..Default::default()
                    },
                )
                .await?;
            Ok(())
        },
    )
    .await?;
    db.close().await?;

    let raw_collection_storage = Storage::connect(
        "coverage_repair_db/docs".to_string(),
        store.clone(),
        config.storage,
    )
    .await?;
    let dirty = named_doc(
        "dirty",
        77,
        "recoverable dirty document",
        &[0.7, 0.1, 0.1, 0.1],
    );
    let mut dirty_doc = Document::try_from(Arc::new(schema), &dirty)?;
    dirty_doc.set_id(1);
    raw_collection_storage
        .put("data/1.cbor", &dirty_doc, None)
        .await?;

    let reopened = AndaDB::connect(store, db_config("coverage_repair_db", None)).await?;
    let collection = reopened
        .open_collection("docs".to_string(), async |_| Ok(()))
        .await?;
    assert!(collection.contains(1));
    assert_eq!(collection.latest_document_id(), Some(1));
    assert_eq!(
        collection
            .query_ids(
                Filter::Field(("age".to_string(), RangeQuery::Eq(Fv::U64(77)))),
                None,
            )
            .await?,
        vec![1]
    );
    assert_eq!(
        collection
            .search_ids(Query {
                search: Some(Search {
                    text: Some("recoverable".to_string()),
                    ..Default::default()
                }),
                limit: Some(5),
                ..Default::default()
            })
            .await?,
        vec![1]
    );
    assert!(
        collection
            .search_ids(Query {
                search: Some(Search {
                    vector: Some(vec![0.7, 0.1, 0.1, 0.1]),
                    ..Default::default()
                }),
                limit: Some(1),
                ..Default::default()
            })
            .await?
            .contains(&1)
    );

    reopened.close().await?;
    Ok(())
}

#[tokio::test]
async fn collection_handles_corrupt_and_stale_ids_metadata() -> Result<(), DBError> {
    let store = Arc::new(InMemory::new());
    let config = db_config("coverage_stale_ids_db", None);
    let db = AndaDB::create(store.clone(), config.clone()).await?;
    db.create_collection(
        PublicDoc::schema()?,
        collection_config("docs"),
        async |collection| {
            collection.set_tokenizer(default_tokenizer());
            let _ = collection.new_document();
            Ok(())
        },
    )
    .await?;
    db.close().await?;

    let raw_collection_storage = Storage::connect(
        "coverage_stale_ids_db/docs".to_string(),
        store.clone(),
        config.storage,
    )
    .await?;
    raw_collection_storage
        .put("ids.cbor", &vec![0xff_u8], None)
        .await?;
    let reopened = AndaDB::connect(store.clone(), db_config("coverage_stale_ids_db", None)).await?;
    assert!(matches!(
        reopened
            .open_collection("docs".to_string(), async |_| Ok(()))
            .await,
        Err(DBError::Generic { .. })
    ));

    let mut ids = Treemap::new();
    ids.add(1);
    ids.run_optimize();
    raw_collection_storage
        .put("ids.cbor", &ids.serialize::<Portable>(), None)
        .await?;

    let reopened = AndaDB::connect(store, db_config("coverage_stale_ids_db", None)).await?;
    let collection = reopened
        .open_collection("docs".to_string(), async |collection| {
            collection.create_btree_index(&["age"]).await?;
            Ok(())
        })
        .await?;
    assert!(collection.contains(1));
    assert!(matches!(
        collection.get(1).await,
        Err(DBError::NotFound { .. })
    ));
    assert!(
        collection
            .search(Query {
                filter: Some(id_filter(RangeQuery::Eq(Fv::U64(1)))),
                limit: Some(1),
                ..Default::default()
            })
            .await?
            .is_empty()
    );
    assert!(collection.remove(1).await?.is_none());
    assert!(!collection.contains(1));

    Ok(())
}

#[tokio::test]
async fn collection_rolls_back_indexes_when_document_put_fails() -> Result<(), DBError> {
    let add_store = Arc::new(FailPutStore::new("data/1.cbor"));
    let add_db =
        AndaDB::create(add_store.clone(), db_config("coverage_add_rollback", None)).await?;
    let add_collection = add_db
        .create_collection(
            PublicDoc::schema()?,
            collection_config("docs"),
            async |collection| {
                collection.create_btree_index(&["age"]).await?;
                collection.create_bm25_index(&["body"]).await?;
                collection
                    .create_hnsw_index(
                        "embedding",
                        HnswConfig {
                            dimension: 4,
                            ..Default::default()
                        },
                    )
                    .await?;
                Ok(())
            },
        )
        .await?;
    add_store.fail_next_put();
    assert!(matches!(
        add_collection
            .add_from(&named_doc(
                "bad",
                99,
                "rollback insert",
                &[0.9, 0.1, 0.1, 0.1]
            ))
            .await,
        Err(DBError::Storage { .. })
    ));
    assert!(add_collection.is_empty());
    assert!(
        add_collection
            .query_ids(
                Filter::Field(("age".to_string(), RangeQuery::Eq(Fv::U64(99)))),
                None,
            )
            .await?
            .is_empty()
    );
    assert!(
        add_collection
            .search_ids(Query {
                search: Some(Search {
                    text: Some("rollback".to_string()),
                    ..Default::default()
                }),
                limit: Some(5),
                ..Default::default()
            })
            .await?
            .is_empty()
    );

    let update_store = Arc::new(FailPutStore::new("data/1.cbor"));
    let update_db = AndaDB::create(
        update_store.clone(),
        db_config("coverage_update_rollback", None),
    )
    .await?;
    let update_collection = update_db
        .create_collection(
            PublicDoc::schema()?,
            collection_config("docs"),
            async |collection| {
                collection.create_btree_index(&["age"]).await?;
                collection.create_bm25_index(&["body"]).await?;
                collection
                    .create_hnsw_index(
                        "embedding",
                        HnswConfig {
                            dimension: 4,
                            ..Default::default()
                        },
                    )
                    .await?;
                Ok(())
            },
        )
        .await?;
    let id = update_collection
        .add_from(&named_doc(
            "old",
            10,
            "old searchable body",
            &[0.1, 0.2, 0.3, 0.4],
        ))
        .await?;
    update_store.fail_next_put();
    assert!(matches!(
        update_collection
            .update(
                id,
                BTreeMap::from([
                    ("age".to_string(), Fv::U64(20)),
                    (
                        "body".to_string(),
                        Fv::Text("new searchable body".to_string()),
                    ),
                    (
                        "embedding".to_string(),
                        Fv::Vector(vector(&[0.4, 0.3, 0.2, 0.1]))
                    ),
                ]),
            )
            .await,
        Err(DBError::Storage { .. })
    ));
    assert_eq!(update_collection.get_as::<PublicDoc>(id).await?.age, 10);
    assert_eq!(
        update_collection
            .query_ids(
                Filter::Field(("age".to_string(), RangeQuery::Eq(Fv::U64(10)))),
                None,
            )
            .await?,
        vec![id]
    );
    assert!(
        update_collection
            .query_ids(
                Filter::Field(("age".to_string(), RangeQuery::Eq(Fv::U64(20)))),
                None,
            )
            .await?
            .is_empty()
    );
    assert_eq!(
        update_collection
            .search_ids(Query {
                search: Some(Search {
                    text: Some("old".to_string()),
                    ..Default::default()
                }),
                limit: Some(5),
                ..Default::default()
            })
            .await?,
        vec![id]
    );

    Ok(())
}

#[tokio::test]
async fn database_public_error_paths() -> Result<(), DBError> {
    let db = test_db("coverage_database_db").await?;
    assert_eq!(format!("{db:?}"), "AndaDB(coverage_database_db)");
    assert_eq!(db.stats().check_point, 0);
    let _ = db.object_store();

    db.set_read_only(true);
    assert!(matches!(
        db.create_collection(
            PublicDoc::schema()?,
            collection_config("blocked"),
            async |_| Ok(())
        )
        .await,
        Err(DBError::Generic { .. })
    ));
    assert!(matches!(
        db.open_or_create_collection(
            PublicDoc::schema()?,
            collection_config("blocked"),
            async |_| Ok(())
        )
        .await,
        Err(DBError::Generic { .. })
    ));
    assert!(matches!(
        db.delete_collection("blocked").await,
        Err(DBError::Generic { .. })
    ));
    db.set_read_only(false);

    let collection = db
        .create_collection(
            PublicDoc::schema()?,
            collection_config("docs"),
            async |_| Ok(()),
        )
        .await?;
    let same = db
        .open_or_create_collection(
            PublicDoc::schema()?,
            collection_config("docs"),
            async |_| panic!("cached collection should be returned before callback"),
        )
        .await?;
    assert!(Arc::ptr_eq(&collection, &same));

    assert!(matches!(
        db.create_collection(
            PublicDoc::schema()?,
            collection_config("docs"),
            async |_| Ok(())
        )
        .await,
        Err(DBError::AlreadyExists { .. })
    ));
    assert!(matches!(
        db.open_collection("missing".to_string(), async |_| Ok(()))
            .await,
        Err(DBError::NotFound { .. })
    ));

    db.delete_collection("missing").await?;
    db.delete_collection("docs").await?;
    db.delete_collection("docs").await?;

    Ok(())
}

#[tokio::test]
async fn database_metadata_and_close_edge_paths() -> Result<(), DBError> {
    let lock_store = Arc::new(InMemory::new());
    let lock = ByteBufB64(vec![4, 5, 6]);
    AndaDB::create(
        lock_store.clone(),
        db_config("coverage_connect_lock_mismatch", Some(lock)),
    )
    .await?;
    assert!(matches!(
        AndaDB::connect(
            lock_store,
            db_config(
                "coverage_connect_lock_mismatch",
                Some(ByteBufB64(vec![6, 5, 4]))
            ),
        )
        .await,
        Err(DBError::Storage { .. })
    ));

    let corrupt_store = Arc::new(InMemory::new());
    let corrupt_config = db_config("coverage_corrupt_db_meta", None);
    AndaDB::create(corrupt_store.clone(), corrupt_config.clone()).await?;
    let raw_db_storage = Storage::connect(
        "coverage_corrupt_db_meta".to_string(),
        corrupt_store.clone(),
        corrupt_config.storage,
    )
    .await?;
    raw_db_storage
        .put_bytes(
            "db_meta.cbor",
            Bytes::from_static(b"not valid metadata"),
            PutMode::Overwrite,
        )
        .await?;
    assert!(matches!(
        AndaDB::connect(corrupt_store, db_config("coverage_corrupt_db_meta", None)).await,
        Err(DBError::Serialization { .. })
    ));

    let stats_db = test_db("coverage_stats_merge_db").await?;
    let stats_collection = stats_db
        .create_collection(
            PublicDoc::schema()?,
            collection_config("docs"),
            async |_| Ok(()),
        )
        .await?;
    stats_collection.add_from(&doc(0, "stats", 1)).await?;
    assert!(stats_db.stats().total_put_count > 0);

    let close_store = Arc::new(FailPutStore::new("docs/meta.cbor"));
    let close_db = AndaDB::create(
        close_store.clone(),
        db_config("coverage_close_first_err", None),
    )
    .await?;
    let close_collection = close_db
        .create_collection(
            PublicDoc::schema()?,
            collection_config("docs"),
            async |_| Ok(()),
        )
        .await?;
    close_collection.add_from(&doc(0, "close", 1)).await?;
    close_store.fail_next_put();
    assert!(matches!(
        close_db.close().await,
        Err(DBError::Storage { .. })
    ));

    let delete_store = Arc::new(InMemory::new());
    let delete_config = db_config("coverage_delete_missing_files", None);
    let delete_db = AndaDB::create(delete_store.clone(), delete_config.clone()).await?;
    delete_db
        .create_collection(
            PublicDoc::schema()?,
            collection_config("docs"),
            async |_| Ok(()),
        )
        .await?;
    delete_db.close().await?;
    let raw_collection_storage = Storage::connect(
        "coverage_delete_missing_files/docs".to_string(),
        delete_store.clone(),
        delete_config.storage,
    )
    .await?;
    raw_collection_storage.drop_data().await?;
    let reopened = AndaDB::connect(
        delete_store,
        db_config("coverage_delete_missing_files", None),
    )
    .await?;
    reopened.delete_collection("docs").await?;

    Ok(())
}

#[tokio::test]
async fn database_delete_unloaded_collection_drops_stored_data() -> Result<(), DBError> {
    let store = Arc::new(InMemory::new());
    let config = db_config("coverage_unloaded_delete_db", None);
    let db = AndaDB::create(store.clone(), config.clone()).await?;
    let collection = db
        .create_collection(
            PublicDoc::schema()?,
            collection_config("docs"),
            async |collection| {
                collection.create_btree_index(&["age"]).await?;
                collection.create_bm25_index(&["body"]).await?;
                Ok(())
            },
        )
        .await?;
    collection.add_from(&doc(0, "drop-me", 12)).await?;
    db.close().await?;

    let reopened = AndaDB::connect(store, config).await?;
    reopened.delete_collection("docs").await?;
    assert!(matches!(
        reopened
            .open_collection("docs".to_string(), async |_| Ok(()))
            .await,
        Err(DBError::NotFound { .. })
    ));

    Ok(())
}

#[tokio::test]
async fn database_lock_schema_autoflush_and_extension_paths() -> Result<(), DBError> {
    let store = Arc::new(InMemory::new());
    let lock = ByteBufB64(vec![1, 2, 3, 4]);
    let db = AndaDB::create(store.clone(), db_config("coverage_lock_db", None)).await?;
    db.set_extension_from("typed".to_string(), vec![7_u64, 8]);
    assert_eq!(db.get_extension_as::<Vec<u64>>("typed"), Some(vec![7, 8]));
    assert_eq!(
        db.set_extension_from_with::<_, u64>("counter".to_string(), |old| {
            Some(old.unwrap_or(0) + 1)
        }),
        None
    );
    assert_eq!(
        db.set_extension_from_with::<_, u64>("counter".to_string(), |old| {
            Some(old.unwrap_or(0) + 1)
        }),
        Some(1)
    );
    assert_eq!(
        db.set_extension_from_with::<_, u64>("counter".to_string(), |_| None),
        None
    );
    db.save_extension_from(
        "saved".to_string(),
        &BTreeMap::from([("ok".to_string(), true)]),
    )
    .await?;
    assert!(db.extensions_with(|extensions| extensions.contains_key("saved")));
    db.flush().await?;
    drop(db);

    let locked = AndaDB::connect(
        store.clone(),
        db_config("coverage_lock_db", Some(lock.clone())),
    )
    .await?;
    assert_eq!(
        locked
            .metadata()
            .config
            .lock
            .as_ref()
            .map(|v| v.0.as_slice()),
        Some(lock.0.as_slice())
    );
    drop(locked);
    assert!(matches!(
        AndaDB::open(store.clone(), db_config("coverage_lock_db", None)).await,
        Err(DBError::Storage { .. })
    ));

    let open_store = Arc::new(InMemory::new());
    let open_lock = ByteBufB64(vec![9, 8, 7]);
    let open_db =
        AndaDB::create(open_store.clone(), db_config("coverage_open_lock_db", None)).await?;
    drop(open_db);
    let opened = AndaDB::open(
        open_store.clone(),
        db_config("coverage_open_lock_db", Some(open_lock)),
    )
    .await?;
    assert!(opened.metadata().config.lock.is_some());

    let schema_store = Arc::new(InMemory::new());
    let schema_db =
        AndaDB::create(schema_store.clone(), db_config("coverage_schema_db", None)).await?;
    schema_db
        .create_collection(schema_v1()?, collection_config("docs"), async |_| Ok(()))
        .await?;
    schema_db.close().await?;
    let schema_db = AndaDB::connect(schema_store, db_config("coverage_schema_db", None)).await?;
    let upgraded = schema_db
        .open_or_create_collection(schema_v2()?, collection_config("docs"), async |_| Ok(()))
        .await?;
    assert_eq!(upgraded.schema().version(), 2);
    assert!(upgraded.schema().get_field("email").is_some());

    let cancel = CancellationToken::new();
    let auto_db = schema_db.clone();
    let auto_cancel = cancel.clone();
    let task = tokio::spawn(async move {
        auto_db
            .auto_flush(auto_cancel, Duration::from_millis(1))
            .await;
    });
    tokio::time::sleep(Duration::from_millis(5)).await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("auto_flush task should stop")
        .expect("auto_flush task should not panic");

    Ok(())
}

#[tokio::test]
async fn database_and_collection_failed_put_paths_are_reported() -> Result<(), DBError> {
    let close_store = Arc::new(FailPutStore::new("db_meta.cbor"));
    let close_db = AndaDB::create(
        close_store.clone(),
        db_config("coverage_fail_db_close", None),
    )
    .await?;
    close_store.fail_next_put();
    assert!(matches!(
        close_db.close().await,
        Err(DBError::Storage { .. })
    ));

    let collection_close_store = Arc::new(FailPutStore::new("docs/meta.cbor"));
    let collection_close_db = AndaDB::create(
        collection_close_store.clone(),
        db_config("coverage_fail_collection_close", None),
    )
    .await?;
    let collection = collection_close_db
        .create_collection(
            PublicDoc::schema()?,
            collection_config("docs"),
            async |_| Ok(()),
        )
        .await?;
    collection.add_from(&doc(0, "close-fail", 1)).await?;
    collection_close_store.fail_next_put();
    assert!(matches!(
        collection.close().await,
        Err(DBError::Storage { .. })
    ));

    let flush_store = Arc::new(FailPutStore::new("docs/meta.cbor"));
    let flush_db = AndaDB::create(
        flush_store.clone(),
        db_config("coverage_fail_db_flush", None),
    )
    .await?;
    let flush_collection = flush_db
        .create_collection(
            PublicDoc::schema()?,
            collection_config("docs"),
            async |_| Ok(()),
        )
        .await?;
    flush_collection.add_from(&doc(0, "flush-fail", 2)).await?;
    flush_store.fail_next_put();
    assert!(matches!(
        flush_db.flush().await,
        Err(DBError::Storage { .. })
    ));

    let auto_store = Arc::new(FailPutStore::new("db_meta.cbor"));
    let auto_db = AndaDB::create(
        auto_store.clone(),
        db_config("coverage_fail_auto_flush", None),
    )
    .await?;
    auto_store.fail_next_put();
    let cancel = CancellationToken::new();
    let auto_task_db = auto_db.clone();
    let auto_task_cancel = cancel.clone();
    let task = tokio::spawn(async move {
        auto_task_db
            .auto_flush(auto_task_cancel, Duration::from_millis(1))
            .await;
    });
    tokio::time::sleep(Duration::from_millis(5)).await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("auto_flush task should stop")
        .expect("auto_flush task should not panic");

    Ok(())
}

#[tokio::test]
async fn index_wrappers_public_api() -> Result<(), DBError> {
    let storage = raw_storage(
        "coverage_index_storage",
        StorageConfig {
            compress_level: 0,
            ..Default::default()
        },
    )
    .await?;
    let now = anda_db::unix_ms();

    let bm25 = BM25::new(
        vec!["title".into(), "body".into()],
        default_tokenizer(),
        storage.clone(),
        now,
    )
    .await?;
    assert_eq!(format!("{bm25:?}"), "BM25Index(title-body)");
    assert_eq!(bm25.name(), "title-body");
    assert_eq!(
        bm25.virtual_field(),
        &["title".to_string(), "body".to_string()]
    );
    assert_eq!(&bm25, &bm25);
    let mut bm25_hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(&&bm25, &mut bm25_hasher);
    assert!(bm25.insert(1, "alpha beta", now).is_ok());
    assert!(bm25.insert(2, "", now).is_ok());
    assert!(!bm25.search("alpha", 10, None).is_empty());
    assert!(!bm25.search_advanced("alpha OR beta", 10, None).is_empty());
    assert!(bm25.remove(1, "alpha beta", now + 1));
    assert!(bm25.has_pending_flush());
    assert!(bm25.flush(now + 2).await?);
    assert!(!bm25.has_pending_flush());
    assert!(!bm25.flush(now + 3).await?);
    let bm25_reloaded =
        BM25::bootstrap("title-body".into(), default_tokenizer(), storage.clone()).await?;
    assert_eq!(bm25_reloaded.metadata().name, "title-body");
    assert!(bm25_reloaded.stats().num_elements <= bm25.stats().num_elements);
    bm25_reloaded.compact_index().await?;

    let field = FieldEntry::new("embedding".into(), FieldType::Vector)?;
    let hnsw = Hnsw::new(
        &field,
        HnswConfig {
            dimension: 4,
            ..Default::default()
        },
        storage.clone(),
        now,
    )
    .await?;
    assert_eq!(format!("{hnsw:?}"), "HnswIndex(embedding)");
    assert_eq!(hnsw.name(), "embedding");
    assert_eq!(hnsw.field_name(), "embedding");
    assert_eq!(&hnsw, &hnsw);
    let mut hnsw_hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(&&hnsw, &mut hnsw_hasher);
    hnsw.insert(1, doc(0, "vector", 1).embedding, now)?;
    assert!(!hnsw.search(&[0.1, 0.2, 0.3, 0.4], 1).is_empty());
    assert!(hnsw.try_search(&[0.1, 0.2], 1).is_err());
    assert!(hnsw.remove(1, now + 1));
    assert!(hnsw.has_pending_flush());
    assert!(hnsw.flush(now + 2).await?);
    assert!(!hnsw.has_pending_flush());
    assert!(!hnsw.flush(now + 3).await?);
    let hnsw_reloaded = Hnsw::bootstrap("embedding".into(), storage.clone()).await?;
    assert_eq!(hnsw_reloaded.metadata().name, "embedding");
    assert_eq!(hnsw_reloaded.stats().num_elements, 0);

    let btree = BTree::new(
        FieldEntry::new("age".into(), FieldType::U64)?,
        storage.clone(),
        now,
    )
    .await?;
    assert!(btree.insert(1, &Fv::U64(10), now)?);
    assert_eq!(btree.keys(None, Some(10)), vec![Fv::U64(10)]);
    let cursor = BTree::to_cursor(&10_u64);
    assert_eq!(btree.keys(cursor, Some(10)), Vec::<Fv>::new());
    assert!(btree.keys(Some("bad-cursor".into()), Some(10)).is_empty());
    btree.compact_index().await?;

    Ok(())
}

#[tokio::test]
async fn storage_public_edge_paths() -> Result<(), DBError> {
    let mut left = StorageStats {
        total_cache_get_count: u64::MAX,
        total_fetch_count: 1,
        total_fetch_bytes: 2,
        total_put_count: 3,
        total_put_bytes: 4,
        total_delete_count: 5,
        ..Default::default()
    };
    let right = StorageStats {
        total_cache_get_count: 1,
        total_fetch_count: 10,
        total_fetch_bytes: 20,
        total_put_count: 30,
        total_put_bytes: 40,
        total_delete_count: 50,
        ..Default::default()
    };
    left.merge(&right);
    assert_eq!(left.total_cache_get_count, u64::MAX);
    assert_eq!(left.total_fetch_count, 11);
    assert_eq!(left.total_fetch_bytes, 22);
    assert_eq!(left.total_put_count, 33);
    assert_eq!(left.total_put_bytes, 44);
    assert_eq!(left.total_delete_count, 55);

    let storage = raw_storage(
        "coverage_storage_edges",
        StorageConfig {
            cache_max_capacity: 1,
            compress_level: 0,
            max_small_object_size: 8,
            object_chunk_size: 4,
            ..Default::default()
        },
    )
    .await?;

    let no_cache = raw_storage(
        "coverage_storage_no_cache",
        StorageConfig {
            cache_max_capacity: 0,
            compress_level: 0,
            ..Default::default()
        },
    )
    .await?;
    assert_eq!(no_cache.metadata().config.cache_max_capacity, 0);
    no_cache.store_metadata(3, anda_db::unix_ms()).await?;
    assert_eq!(no_cache.stats().check_point, 3);
    no_cache
        .put_bytes(
            "bad_cbor",
            Bytes::from_static(b"not valid cbor"),
            PutMode::Overwrite,
        )
        .await?;
    assert!(matches!(
        no_cache.fetch::<u64>("bad_cbor").await,
        Err(DBError::Serialization { .. })
    ));
    assert!(matches!(
        no_cache.get::<u64>("bad_cbor").await,
        Err(DBError::Serialization { .. })
    ));

    let mut empty_writer = no_cache.to_writer("empty_flush", PutMode::Overwrite);
    FuturesAsyncWriteExt::flush(&mut empty_writer)
        .await
        .unwrap();
    assert!(empty_writer.take_version().is_none());

    let tiny = raw_storage(
        "coverage_storage_tiny",
        StorageConfig {
            compress_level: 0,
            max_small_object_size: 1,
            ..Default::default()
        },
    )
    .await?;
    let mut too_large = tiny.to_writer("too_large", PutMode::Overwrite);
    FuturesAsyncWriteExt::write_all(&mut too_large, b"12")
        .await
        .unwrap();
    assert!(FuturesAsyncWriteExt::flush(&mut too_large).await.is_err());

    let mut shutdown_writer = no_cache.to_writer("shutdown", PutMode::Overwrite);
    AsyncWriteExt::write_all(&mut shutdown_writer, b"ok")
        .await
        .unwrap();
    AsyncWriteExt::shutdown(&mut shutdown_writer).await.unwrap();
    assert!(shutdown_writer.take_version().is_some());

    assert!(matches!(
        storage
            .put_bytes(
                "too_large",
                Bytes::from_static(b"0123456789"),
                PutMode::Overwrite
            )
            .await,
        Err(DBError::PayloadTooLarge { .. })
    ));

    let version = storage
        .put_bytes("raw/a", Bytes::from_static(b"abc"), PutMode::Overwrite)
        .await?;
    let update_version = storage
        .put_bytes(
            "raw/a",
            Bytes::from_static(b"def"),
            PutMode::Update(version.into()),
        )
        .await?;
    assert!(update_version.e_tag.is_some() || update_version.version.is_none());
    let (raw, _) = storage.fetch_bytes("raw/a").await?;
    assert_eq!(raw, Bytes::from_static(b"def"));

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct Small {
        v: u64,
    }
    let doc = Small { v: 7 };
    storage.create("docs/1", &doc).await?;
    assert!(matches!(
        storage.create("docs/1", &doc).await,
        Err(DBError::AlreadyExists { .. })
    ));
    assert_eq!(storage.get::<Small>("docs/1").await?.0, doc);
    assert_eq!(storage.get::<Small>("docs/1").await?.0, doc);
    assert!(storage.stats().total_cache_get_count > 0);

    let mut writer = storage.to_writer("raw/vectored", PutMode::Overwrite);
    let slices = [IoSlice::new(b"ab"), IoSlice::new(b"cd")];
    let written = FuturesAsyncWriteExt::write_vectored(&mut writer, &slices)
        .await
        .unwrap();
    assert_eq!(written, 4);
    FuturesAsyncWriteExt::flush(&mut writer).await.unwrap();
    assert!(writer.take_version().is_some());
    let (vectored, _) = storage.fetch_bytes("raw/vectored").await?;
    assert_eq!(vectored, Bytes::from_static(b"abcd"));

    let streaming = raw_storage(
        "coverage_storage_streaming",
        StorageConfig {
            compress_level: 0,
            object_chunk_size: 4,
            ..Default::default()
        },
    )
    .await?;
    let mut stream_writer = streaming.stream_writer("stream/plain");
    stream_writer.write_all(b"plain stream").await.unwrap();
    stream_writer.shutdown().await.unwrap();
    let mut reader = streaming.stream_reader("stream/plain").await?;
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();
    assert_eq!(out, b"plain stream");

    storage.put("docs/2", &Small { v: 8 }, None).await?;
    storage.put("docs/3", &Small { v: 9 }, None).await?;
    let mut listed = storage.list::<Small>(Some("docs"), Some("docs/1"));
    let mut values = Vec::new();
    while let Some(item) = listed.next().await {
        values.push(item?.0.v);
    }
    assert_eq!(values, vec![8, 9]);

    let mut metas = storage.list_meta(Some("docs"), Some("docs/1"));
    let mut meta_count = 0;
    while let Some(item) = metas.next().await {
        item?;
        meta_count += 1;
    }
    assert_eq!(meta_count, 2);

    storage.delete("docs/1").await?;
    storage.delete("docs/1").await?;
    assert!(matches!(
        storage.fetch_bytes("docs/1").await,
        Err(DBError::NotFound { .. })
    ));

    Ok(())
}
