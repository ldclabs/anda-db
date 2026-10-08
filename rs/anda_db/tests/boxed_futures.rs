//! The heavy database and collection futures are boxed.
//!
//! In a debug build every `.await` keeps the awaited future in its caller's
//! stack frame, so futures inlined into one another cost the sum of their sizes
//! at every level of a call chain. Boxing the heavy ones keeps a caller's frame
//! to a pointer per await; the Cognitive Nexus `stack_budget` test checks the
//! resulting stack depth end to end.

use anda_db::{
    collection::CollectionConfig,
    database::{AndaDB, DBConfig},
    schema::AndaDBSchema,
    unix_ms,
};
use object_store::memory::InMemory;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, future::Future, sync::Arc};

#[derive(Debug, Clone, Serialize, Deserialize, AndaDBSchema)]
struct Doc {
    _id: u64,
    key: String,
}

/// A boxed future is a pointer; anything larger was left inline.
fn assert_boxed<F: Future>(name: &str, future: F) {
    assert_eq!(
        size_of_val(&future),
        size_of::<usize>(),
        "{name} returns its state machine inline instead of boxed"
    );
}

#[tokio::test]
async fn heavy_futures_are_boxed() {
    let store = Arc::new(InMemory::new());
    assert_boxed(
        "AndaDB::connect",
        AndaDB::connect(store.clone(), DBConfig::default()),
    );
    let db = AndaDB::connect(store, DBConfig::default()).await.unwrap();
    let config = || CollectionConfig {
        name: "docs".into(),
        ..Default::default()
    };
    assert_boxed(
        "AndaDB::open_or_create_collection",
        db.open_or_create_collection(Doc::schema().unwrap(), config(), async |_| Ok(())),
    );
    assert_boxed(
        "AndaDB::create_collection",
        db.create_collection(Doc::schema().unwrap(), config(), async |_| Ok(())),
    );
    assert_boxed(
        "AndaDB::open_collection",
        db.open_collection("docs".into(), async |_| Ok(())),
    );
    let docs = db
        .open_or_create_collection(Doc::schema().unwrap(), config(), async |_| Ok(()))
        .await
        .unwrap();
    let doc = Doc {
        _id: 0,
        key: "a".into(),
    };
    assert_boxed("Collection::add_from", docs.add_from(&doc));
    assert_boxed("Collection::update", docs.update(1, BTreeMap::new()));
    assert_boxed("Collection::remove", docs.remove(1));
    assert_boxed("Collection::flush", docs.flush(unix_ms()));
    assert_boxed("Collection::close", docs.close());
    assert_boxed(
        "Collection::save_extension_from",
        docs.save_extension_from("k".into(), &1u64),
    );
    assert_boxed("Collection::remove_extension", docs.remove_extension("k"));
    // Index setup runs in the open callback, which holds the collection mutably.
    db.open_or_create_collection(
        Doc::schema().unwrap(),
        CollectionConfig {
            name: "indexed".into(),
            ..Default::default()
        },
        async |c| {
            assert_boxed(
                "Collection::create_btree_index",
                c.create_btree_index(&["key"]),
            );
            assert_boxed(
                "Collection::create_bm25_index",
                c.create_bm25_index(&["key"]),
            );
            Ok(())
        },
    )
    .await
    .unwrap();
    assert_boxed("AndaDB::close_collection", db.close_collection("docs"));
    assert_boxed("AndaDB::close", db.close());
}
