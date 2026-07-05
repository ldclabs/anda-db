use anda_db::{
    collection::{Collection, CollectionConfig},
    database::{AndaDB, DBConfig},
    error::DBError,
    index::HnswConfig,
    query::{Filter, Query, RangeQuery, Search},
    schema::{AndaDBSchema, Fv, Json, Resource, Vector, vector_from_f32},
    storage::StorageConfig,
};
use anda_db_tfs::jieba_tokenizer;
use anda_object_store::MetaStoreBuilder;
use ic_auth_types::Xid;
use object_store::local::LocalFileSystem;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
use structured_logger::unix_ms;

#[derive(Debug, Clone, Serialize, Deserialize, AndaDBSchema)]
pub struct Knowledge {
    pub _id: u64,
    // thread ID, thread is a conversation that multi agents can join.
    #[field_type = "Bytes"]
    pub thread: Xid,
    // seconds since epoch
    pub created_at: u64,
    // knowledge authors
    pub authors: Vec<String>,
    // knowledge description
    pub description: String,
    // knowledge embedding for vector search
    pub embedding: Vector,
    // knowledge metadata
    pub metadata: BTreeMap<String, Json>,
    // Data source
    pub source: Option<Resource>,
    // confidence score
    pub score: Option<i64>,
    // verification hash
    pub hash: Option<[u8; 32]>,
}

// cargo run --example db_demo --features=full
#[tokio::main]
async fn main() -> Result<(), DBError> {
    // init structured logger
    structured_logger::init();

    // let object_store = InMemory::new();
    let object_store = MetaStoreBuilder::new(
        LocalFileSystem::new_with_prefix("./debug/metastore")?,
        10000,
    )
    .build();

    let db_config = DBConfig {
        name: "anda_db_demo".to_string(),
        description: "Anda DB demo".to_string(),
        storage: StorageConfig {
            compress_level: 0, // no compression
            ..Default::default()
        },
        lock: None, // no lock for demo
    };

    // connect to the database (create if it doesn't exist)
    let db = AndaDB::connect(Arc::new(object_store), db_config).await?;
    log::info!(
        action = "connect",
        database = db.name();
        "connected to database"
    );

    // knowledge schema
    let schema = Knowledge::schema()?;

    println!("-----> Schema: {:#?}", schema);

    let collection_config = CollectionConfig {
        name: "knowledges".to_string(),
        description: "My knowledges".to_string(),
    };

    let collection = db
        .open_or_create_collection(schema, collection_config, async |collection| {
            // set tokenizer
            collection.set_tokenizer(jieba_tokenizer());

            // create BTree indexes if not exists
            collection.create_btree_index_nx(&["thread"]).await?;
            collection.create_btree_index_nx(&["created_at"]).await?;
            collection.create_btree_index_nx(&["authors"]).await?;
            collection.create_btree_index_nx(&["score"]).await?;

            // create BM25 & HNSW indexes if not exists
            collection
                .create_bm25_index_nx(&["authors", "description", "metadata", "source"])
                .await?;
            collection
                .create_hnsw_index_nx(
                    "embedding",
                    HnswConfig {
                        dimension: 10,
                        ..Default::default()
                    },
                )
                .await?;
            Ok::<(), DBError>(())
        })
        .await?;
    log::info!(
        action = "open_or_create_collection",
        collection = collection.name();
        "opened or created collection"
    );

    add_knowledges_and_query(&collection).await?;

    db.close().await?;

    Ok(())
}

async fn add_knowledges_and_query(collection: &Arc<Collection>) -> Result<(), DBError> {
    let mut thread = Xid::new();

    let knowledges = vec![
        Knowledge {
            _id: 0,
            thread: thread.clone(),
            created_at: unix_ms() / 1000,
            authors: vec!["Anda".to_string(), "Bill".to_string()],
            metadata: BTreeMap::new(),
            description: "Rust 是一门系统级编程语言，专注于安全性、并发性和性能。Rust 的所有权系统是其最独特的特性之一，它在编译时确保内存安全。".to_string(),
            embedding: vector_from_f32(vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]),
            source: None,
            score: None,
            hash: None,
        },
        Knowledge {
            _id: 0,
            thread: thread.clone(),
            created_at: unix_ms() / 1000,
            authors: vec!["Charlie".to_string()],
            metadata: BTreeMap::new(),
            description: "向量数据库是一种特殊类型的数据库，专门用于存储和检索向量嵌入,与传统数据库相比，向量数据库能够高效地进行相似性搜索。".to_string(),
            embedding: vector_from_f32(vec![0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2]),
            source: None,
            score: None,
            hash: None,
        },
    ];

    let metadata = collection.metadata();
    println!("-----> Collection metadata: {:?}", metadata);

    println!("-----> Add knowledges");
    if metadata.stats.num_documents == 0 {
        for knowledge in knowledges {
            let id = collection.add_from(&knowledge).await?;
            println!("Knowledge id: {id}");
        }
        collection.flush(unix_ms()).await?;
    }

    println!("-----> Search: id = 1");
    let result: Vec<Knowledge> = collection
        .search_as(Query {
            filter: Some(Filter::Field((
                "_id".to_string(),
                RangeQuery::Eq(Fv::U64(1)),
            ))),
            ..Default::default()
        })
        .await?;
    assert_eq!(result.len(), 1);
    // set thread id to the first knowledge for next search
    thread = result[0].thread.clone();
    for doc in &result {
        println!("Find knowledge: {:?}\n", doc);
    }

    println!("-----> Search: thread = xxx");
    let result: Vec<Knowledge> = collection
        .search_as(Query {
            filter: Some(Filter::Field((
                "thread".to_string(),
                RangeQuery::Eq(Fv::Bytes(thread.as_slice().into())),
            ))),
            ..Default::default()
        })
        .await?;
    assert_eq!(result.len(), 2);
    for doc in &result {
        println!("Find knowledge: {:?}\n", doc);
    }

    println!("-----> Search: text = Rust");
    let result: Vec<Knowledge> = collection
        .search_as(Query {
            search: Some(Search {
                text: Some("rust".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await?;
    assert_eq!(result.len(), 1);
    for doc in &result {
        println!("Find knowledge: {:?}\n", doc);
    }

    println!("-----> Search: vector search");
    let result: Vec<Knowledge> = collection
        .search_as(Query {
            search: Some(Search {
                vector: Some(vec![0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2]),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await?;
    assert_eq!(result.len(), 2);
    for doc in &result {
        println!("Find knowledge: {:?}\n", doc);
    }

    println!("-----> Search: compound query");
    let result: Vec<Knowledge> = collection
        .search_as(Query {
            search: Some(Search {
                text: Some("数据库".to_string()),
                vector: Some(vec![0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2]),
                ..Default::default()
            }),
            filter: Some(Filter::Field((
                "_id".to_string(),
                RangeQuery::Gt(Fv::U64(1)),
            ))),
            ..Default::default()
        })
        .await?;
    assert_eq!(result.len(), 1);
    for doc in &result {
        println!("Find knowledge: {:?}\n", doc);
    }

    Ok(())
}
