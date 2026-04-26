# anda_object_store

`anda_object_store` is the storage-wrapper layer of the AndaDB workspace. It
extends `object_store` with portable metadata handling and optional transparent
encryption so embedded deployments can keep the same storage model across local
and cloud backends.

## What This Crate Provides

- `MetaStore` for side-car metadata and portable conditional updates
- `EncryptedStore` for chunked AES-256-GCM encryption at rest
- compatibility with any backend implementing `object_store::ObjectStore`
- better correctness for backends that do not natively support optimistic concurrency control
- building blocks used by AndaDB for portable durable persistence

## When to Use It

Use `anda_object_store` when you need:

- a stable `object_store` abstraction with metadata side-cars
- conditional-update semantics on local filesystems or other simpler backends
- transparent encryption-at-rest for AI memory data
- one storage abstraction that can move from local development to cloud object storage

## Getting Started

Add the crate to your project:

```toml
[dependencies]
anda_object_store = "0.3"
object_store = { version = "0.13", features = ["fs"] }
```

Typical entry points:

- `MetaStoreBuilder` for metadata-aware storage
- `EncryptedStoreBuilder` for encrypted storage

## Technical Reference

Deep technical documentation for this crate lives in:

- [docs/anda_object_store.md](../../docs/anda_object_store.md)
- [docs/anda_db.md](../../docs/anda_db.md)

## Related Crates

- `anda_db` for the embedded database built on top of this storage layer
- `object_store` for backend integrations such as local filesystem, S3, and GCS

## License

MIT. See [LICENSE](../../LICENSE).
