# Anda Object Store

Anda Object Store is a Rust library that extends the functionality of the [object_store](https://docs.rs/object_store) crate with additional features like metadata management and encryption. It's designed to work seamlessly with Anda DB, providing enhanced storage capabilities for AI applications.

## Features

### MetaStore

`MetaStore` is a wrapper around an `ObjectStore` implementation that adds metadata capabilities:

- **Metadata Management**: Stores metadata for each object in a separate location
- **Conditional Updates**: Enables conditional updates for storage backends that don't natively support them (like `LocalFileSystem`)
- **Metadata Caching**: Reduces storage operations with configurable caching
- **Content Hashing**: Automatically generates E-Tags (SHA3-256 hashes) for stored content

```rust
use anda_object_store::MetaStoreBuilder;
use object_store::local::LocalFileSystem;

// Create a MetaStore with a local filesystem backend
let storage = MetaStoreBuilder::new(
    LocalFileSystem::new_with_prefix("my_store").unwrap(),
    10000, // metadata cache capacity
)
.build();
```

### EncryptedStore

`EncryptedStore` provides transparent AES-256-GCM encryption and decryption for stored objects:

- **Transparent Encryption**: Automatically encrypts data before storage and decrypts upon retrieval
- **Chunked Encryption**: Handles large objects by splitting them into manageable chunks
- **Metadata Caching**: Improves performance with configurable caching
- **Conditional Put Operations**: Optional optimistic concurrency control

```rust
use anda_object_store::EncryptedStoreBuilder;
use object_store::local::LocalFileSystem;

// Create a secret key (in production, use a secure random key)
let secret = [0u8; 32];

// Create an encrypted store with a local filesystem backend
let store = LocalFileSystem::new_with_prefix("my_store").unwrap();
let encrypted_store = EncryptedStoreBuilder::with_secret(store, 1000, secret)
    .with_chunk_size(1024 * 1024) // Set chunk size to 1 MB
    .with_conditional_put() // Enable conditional put operations
    .build();
```

## Integration with Anda DB

Anda Object Store is designed to work seamlessly with Anda DB, providing the storage layer for this specialized database for AI Agents:

```rust
use anda_db::{
    database::{AndaDB, DBConfig},
    storage::StorageConfig,
};
use anda_object_store::EncryptedStoreBuilder;
use object_store::local::LocalFileSystem;

// Create a secret key (in production, use a secure random key)
let secret = [0u8; 32];

// Create an encrypted store with a local filesystem backend
let object_store = LocalFileSystem::new_with_prefix("my_store").unwrap();
let object_store = EncryptedStoreBuilder::with_secret(object_store, 100000, secret)
    .with_chunk_size(1024 * 1024) // Set chunk size to 1 MB
    .with_conditional_put() // Enable conditional put operations
    .build();

// Configure and connect to Anda DB
let db_config = DBConfig {
    name: "anda_db_demo".to_string(),
    description: "Anda DB demo".to_string(),
    storage: StorageConfig {
        compress_level: 0, // no compression
        ..Default::default()
    },
};

// Connect to the database using the object store
let db = AndaDB::connect(Arc::new(object_store), db_config).await?;
```

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
anda_object_store = "0.3"
```

## License

Copyright © 2026 [LDC Labs](https://github.com/ldclabs).

`ldclabs/anda-db` is licensed under the MIT License. See [LICENSE](../../LICENSE) for the full license text.
