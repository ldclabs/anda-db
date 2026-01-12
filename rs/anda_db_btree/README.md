# Anda-DB B-tree Index Library

[![Crates.io](https://img.shields.io/crates/v/anda_db_btree)](https://crates.io/crates/anda_db_btree)
[![Documentation](https://docs.rs/anda_db_btree/badge.svg)](https://docs.rs/anda_db_btree)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Build Status](https://github.com/ldclabs/anda-db/actions/workflows/test.yml/badge.svg)](https://github.com/ldclabs/anda-db/actions)

A high-performance B-tree based index implementation for Anda-DB, optimized for concurrent access and efficient range queries.

## Features

- **Support for various data types**: Index fields of u64, i64, String, binary data and more
- **Efficient range queries**: Optimized for fast range-based lookups
- **Prefix query**: Specialized support for string prefix queries
- **Efficient serialization**: Fast CBOR-based serialization and deserialization
- **Incremental Persistent**: Support incremental index updates persistent (insertions and deletions)
- **Thread-safe concurrent access**: Safely use the index from multiple threads

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
anda_db_btree = "0.4.0"
```

### Basic Example

```rust
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
    let mut index2 = BTreeIndex::<String, u64>::load_metadata(std::fs::File::open(
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
```

## Configuration

The `BTreeConfig` struct allows customizing the index behavior:

```rust
let config = BTreeConfig {
    // Maximum size of a bucket before creating a new one (in bytes)
    bucket_overload_size: 1024 * 512, // 512KB

    // Whether to allow duplicate keys
    // If false, attempting to insert a duplicate key will result in an error
    allow_duplicates: true,
};
```

## Performance Considerations

- **Bucket Size**: Adjust `bucket_overload_size` based on your data characteristics.
- **Concurrency**: The index is designed for concurrent access, using lock-free data structures where possible.
- **Memory Usage**: The index keeps all data in memory for fast access. For very large datasets, consider using multiple smaller indices.

## Error Handling

The library provides a comprehensive error type `BTreeError` that covers various failure scenarios:

- `BTreeError::Generic`: General index-related errors
- `BTreeError::Serialization`: CBOR serialization/deserialization errors
- `BTreeError::NotFound`: When a requested value is not found in the index
- `BTreeError::AlreadyExists`: When trying to insert a duplicate key with `allow_duplicates` set to false

## License
Copyright © 2026 [LDC Labs](https://github.com/ldclabs).

`ldclabs/anda-db` is licensed under the MIT License. See [LICENSE](../../LICENSE) for the full license text.