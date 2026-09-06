use anda_db::{
    collection::CollectionConfig,
    database::{AndaDB, DBConfig},
    schema::AndaDBSchema,
    storage::StorageConfig,
};
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, AndaDBSchema)]
struct Item {
    _id: u64,
    age: u64,
}

#[tokio::test]
async fn collection_refuses_to_open_when_a_committed_index_bucket_is_missing() {
    let store = Arc::new(InMemory::new());
    let config = DBConfig {
        name: "missing_btree".into(),
        description: String::new(),
        storage: StorageConfig::default(),
        lock: None,
    };
    let db = AndaDB::connect(store.clone(), config.clone())
        .await
        .unwrap();
    let collection = db
        .open_or_create_collection(
            Item::schema().unwrap(),
            CollectionConfig {
                name: "items".into(),
                description: String::new(),
            },
            async |c| {
                c.create_btree_index_nx(&["age"]).await?;
                Ok(())
            },
        )
        .await
        .unwrap();
    collection
        .add_from(&Item { _id: 0, age: 42 })
        .await
        .unwrap();
    db.close().await.unwrap();
    drop(collection);
    drop(db);

    let objects: Vec<_> = store.list(None).try_collect().await.unwrap();
    let object = objects
        .into_iter()
        .find(|o| o.location.as_ref().contains("/btree_indexes/age/b_"))
        .expect("persisted B-tree bucket");
    store.delete(&object.location).await.unwrap();
    let db = AndaDB::connect(store, config).await.unwrap();
    let error = db
        .open_collection("items".into(), async |_| Ok(()))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("missing"), "{error}");
}
