use anda_db::{
    collection::CollectionConfig,
    database::{AndaDB, DBConfig},
    index::HnswConfig,
    query::{Query, Search},
    schema::{AndaDBSchema, Fv, Vector, vector_from_f32},
    storage::StorageConfig,
};
use object_store::memory::InMemory;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, Clone, Serialize, Deserialize, AndaDBSchema)]
struct Memory {
    _id: u64,
    embedding: Vector,
}

#[tokio::test]
async fn collection_vector_updates_and_large_finite_values_survive_reopen() {
    let store = Arc::new(InMemory::new());
    let config = DBConfig {
        name: "hnsw_updates".into(),
        description: "HNSW update regression".into(),
        storage: StorageConfig::default(),
        lock: None,
    };
    let db = AndaDB::connect(store.clone(), config.clone())
        .await
        .unwrap();
    let collection = db
        .open_or_create_collection(
            Memory::schema().unwrap(),
            CollectionConfig {
                name: "memories".into(),
                description: "vectors".into(),
            },
            async |c| {
                c.create_hnsw_index_nx(
                    "embedding",
                    HnswConfig {
                        dimension: 2,
                        max_layers: 1,
                        max_connections: 2,
                        ef_construction: 16,
                        ef_search: 16,
                        ..Default::default()
                    },
                )
                .await?;
                Ok(())
            },
        )
        .await
        .unwrap();
    let mut ids = Vec::new();
    for x in 1..=40 {
        ids.push(
            collection
                .add_from(&Memory {
                    _id: 0,
                    embedding: vector_from_f32(vec![x as f32, 0.0]),
                })
                .await
                .unwrap(),
        );
    }
    let target = ids[3];
    for x in [1.5, 1000.0, 1e20, 1.5] {
        collection
            .update(
                target,
                BTreeMap::from([(
                    "embedding".into(),
                    Fv::Vector(vector_from_f32(vec![x, 0.0])),
                )]),
            )
            .await
            .unwrap();
        collection.flush(10).await.unwrap();
        let memory: Memory = collection.get_as(target).await.unwrap();
        let results: Vec<Memory> = collection
            .search_as(Query {
                search: Some(Search {
                    vector: Some(memory.embedding.iter().map(|v| v.to_f32()).collect()),
                    ..Default::default()
                }),
                limit: Some(5),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(results[0]._id, target);
    }
    db.close().await.unwrap();
    drop(collection);
    drop(db);
    let reopened = AndaDB::connect(store, config).await.unwrap();
    let collection = reopened
        .open_collection("memories".into(), async |_| Ok(()))
        .await
        .unwrap();
    let results: Vec<Memory> = collection
        .search_as(Query {
            search: Some(Search {
                vector: Some(vec![1.5, 0.0]),
                ..Default::default()
            }),
            limit: Some(5),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(results[0]._id, target);
    reopened.close().await.unwrap();
}
