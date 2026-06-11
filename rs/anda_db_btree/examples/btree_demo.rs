use std::io::{Read, Write};

use anda_db_btree::{BTreeConfig, BTreeIndex, RangeQuery};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new B-tree index
    let config = BTreeConfig {
        bucket_overload_size: 1024 * 512, // 512KB per bucket
        allow_duplicates: true,
    };
    let index = BTreeIndex::<u64, String>::new("my_index".to_string(), Some(config));

    // Insert some data
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let apple = "apple".to_string();
    let banana = "banana".to_string();
    let cherry = "cherry".to_string();
    let date = "date".to_string();
    let berry = "berry".to_string();

    index.insert(1, apple.clone(), now_ms).unwrap();
    index.insert(2, banana.clone(), now_ms).unwrap();
    index.insert(3, cherry.clone(), now_ms).unwrap();
    index.insert(4, date.clone(), now_ms).unwrap();
    index.insert(5, berry.clone(), now_ms).unwrap();

    // Search for exact matches
    let result = index.query_with(&apple, |ids| Some(ids.clone()));
    assert!(result.is_some());
    println!("Documents with 'apple': {:?}", result.unwrap());

    // Range queries
    let query = RangeQuery::Between(banana.clone(), date.clone());
    let results = index.range_query_with(query, |k, ids| {
        println!("Key: {}, IDs: {:?}", k, ids);
        (true, vec![k.clone()])
    });
    println!("Keys in range: {:?}", results);

    // Prefix query (for String keys)
    let results =
        index.prefix_query_with("app", |k, ids| (true, Some((k.to_string(), ids.clone()))));
    println!("Keys with prefix 'app': {:?}", results);

    // persist the index to files
    {
        std::fs::create_dir_all("debug/btree_demo")?;
        let metadata = std::fs::File::create("debug/btree_demo/metadata.cbor")?;
        index
            .flush(metadata, now_ms, async |id, data| {
                let mut bucket =
                    std::fs::File::create(format!("debug/btree_demo/bucket_{id}.cbor"))?;
                bucket.write_all(data)?;
                Ok(true)
            })
            .await?;
    }

    // Load the index from metadata
    let mut index2 = BTreeIndex::<u64, String>::load_metadata(std::fs::File::open(
        "debug/btree_demo/metadata.cbor",
    )?)?;

    assert_eq!(index2.name(), "my_index");
    assert_eq!(index2.len(), 0);

    // Load the index data
    index2
        .load_buckets(async |id: u32| {
            let mut file = std::fs::File::open(format!("debug/btree_demo/bucket_{id}.cbor"))?;
            let mut data = Vec::new();
            file.read_to_end(&mut data)?;
            Ok(Some(data))
        })
        .await?;

    assert_eq!(index2.len(), 5);

    let result = index.query_with(&apple, |ids| Some(ids.clone()));
    assert!(result.is_some());

    // Remove data
    let ok = index.remove(1, apple.clone(), now_ms);
    assert!(ok);
    let result = index.query_with(&apple, |ids| Some(ids.clone()));
    assert!(result.is_none());

    println!("OK");

    Ok(())
}
