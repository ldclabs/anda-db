use anda_db_btree::{BtreeConfig, BtreeError, BtreeIndex, RangeQuery};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new B-tree index
    let config = BtreeConfig {
        bucket_overload_size: 1024 * 512, // 512KB per bucket
        allow_duplicates: true,
    };
    let index = BtreeIndex::<String, u64>::new("my_index".to_string(), Some(config));

    // Insert some data
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    index.insert(1, "apple".to_string(), now_ms).unwrap();
    index.insert(2, "banana".to_string(), now_ms).unwrap();
    index.insert(3, "cherry".to_string(), now_ms).unwrap();

    // Batch insert
    let items = vec![(4, "date".to_string()), (5, "elderberry".to_string())];
    index.batch_insert(items, now_ms).unwrap();

    // Search for exact matches
    let result = index.search_with("apple".to_string(), |ids| Some(ids.clone()));
    assert!(result.is_some());
    println!("Documents with 'apple': {:?}", result.unwrap());

    // Range queries
    let query = RangeQuery::Between("banana".to_string(), "date".to_string());
    let results = index.search_range_with(query, |k, ids| {
        println!("Key: {}, IDs: {:?}", k, ids);
        (true, Some(k.clone()))
    });
    println!("Keys in range: {:?}", results);

    // Prefix search (for String keys)
    let results =
        index.search_prefix_with("app", |k, ids| (true, Some((k.to_string(), ids.clone()))));
    println!("Keys with prefix 'app': {:?}", results);

    // persist the index to files
    {
        let file = tokio::fs::File::create("debug/btree_demo_metadata.cbor")
            .await?
            .compat_write();
        // Store the index metadata
        index.store_metadata(file, 0).await?;

        // Store the index data
        index
            .store_dirty_buckets(async |id: u32, data: Vec<u8>| {
                let mut file =
                    tokio::fs::File::create(format!("debug/btree_demo_bucket_{id}.cbor"))
                        .await
                        .map_err(|err| BtreeError::Generic {
                            name: index.name().to_string(),
                            source: err.into(),
                        })?;
                file.write_all(&data)
                    .await
                    .map_err(|err| BtreeError::Generic {
                        name: index.name().to_string(),
                        source: err.into(),
                    })?;
                file.flush().await.map_err(|err| BtreeError::Generic {
                    name: index.name().to_string(),
                    source: err.into(),
                })?;
                Ok(true)
            })
            .await?;
    }

    // Load the index from metadata
    let mut index2 = BtreeIndex::<String, u64>::load_metadata(
        tokio::fs::File::open("debug/btree_demo_metadata.cbor")
            .await?
            .compat(),
    )
    .await?;

    assert_eq!(index2.name(), "my_index");
    assert_eq!(index2.len(), 0);

    // Load the index data
    index2
        .load_buckets(async |id: u32| {
            let mut file = tokio::fs::File::open(format!("debug/btree_demo_bucket_{id}.cbor"))
                .await
                .map_err(|err| BtreeError::Generic {
                    name: index.name().to_string(),
                    source: err.into(),
                })?;
            let mut data = Vec::new();
            file.read_to_end(&mut data)
                .await
                .map_err(|err| BtreeError::Generic {
                    name: index.name().to_string(),
                    source: err.into(),
                })?;
            Ok(data)
        })
        .await?;

    assert_eq!(index2.len(), 5);

    let result = index.search_with("apple".to_string(), |ids| Some(ids.clone()));
    assert!(result.is_some());

    // Remove data
    let ok = index.remove(1, "apple".to_string(), now_ms);
    assert!(ok);
    let result = index.search_with("apple".to_string(), |ids| Some(ids.clone()));
    assert!(result.is_none());

    println!("OK");

    Ok(())
}
