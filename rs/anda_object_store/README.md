# anda_object_store

[![Crates.io](https://img.shields.io/crates/v/anda_object_store.svg)](https://crates.io/crates/anda_object_store)
[![Docs.rs](https://docs.rs/anda_object_store/badge.svg)](https://docs.rs/anda_object_store)

`anda_object_store` is the storage-wrapper layer of the
[AndaDB](https://github.com/ldclabs/anda-db) workspace. It extends the
[`object_store`](https://crates.io/crates/object_store) crate with portable
metadata, conditional updates and optional transparent encryption, so an
embedded deployment keeps the same storage model across local and cloud
backends.

## What this crate provides

- `MetaStore` (built with `MetaStoreBuilder`): side-car metadata and logical
  ETags, giving any `ObjectStore` backend — including the local filesystem,
  which has no native conditional writes — the `PutMode::Create` /
  `PutMode::Update` and `if_match` / `if_none_match` semantics AndaDB relies
  on.
- `EncryptedStore` (built with `EncryptedStoreBuilder`): chunked AES-256-GCM
  encryption at rest with authenticated metadata and seekable range reads.
- A metadata cache shared by clones of one instance, bounded by entry count,
  bytes (`with_meta_cache_bytes`) or TTL.
- `collect_garbage` on both wrappers for reclaiming unreferenced payloads.
- `FaultStore`: a fault-injecting wrapper for crash and error tests, used by
  AndaDB's crash-consistency harness.

## Getting started

```toml
[dependencies]
anda_object_store = "0.14"
object_store = { version = "0.14", features = ["fs"] }
tokio = { version = "1", features = ["full"] }
```

```rust
use anda_object_store::{EncryptedStoreBuilder, MetaStoreBuilder};
use object_store::{
    ObjectStoreExt, PutPayload, local::LocalFileSystem, memory::InMemory, path::Path,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Conditional updates on a local directory.
    std::fs::create_dir_all("./data")?;
    let local = MetaStoreBuilder::new(
        LocalFileSystem::new_with_prefix("./data")?.with_fsync(true),
        10_000, // metadata cache capacity (entries)
    )
    .build();
    local.put(&Path::from("hello"), PutPayload::from_static(b"world")).await?;

    // Transparent encryption at rest; keep the 32-byte key in a secret store.
    let secret = [7u8; 32];
    let encrypted = EncryptedStoreBuilder::with_secret(InMemory::new(), 10_000, secret).build();
    let path = Path::from("notes/1");
    encrypted.put(&path, PutPayload::from_static(b"private memory")).await?;
    let bytes = encrypted.get(&path).await?.bytes().await?;
    assert_eq!(&bytes[..], b"private memory");
    Ok(())
}
```

Pass either wrapper to `anda_db::database::AndaDB` as an
`Arc<dyn ObjectStore>`. `EncryptedStore` already provides conditional
updates, so it does not need a `MetaStore` underneath.

## Storage protocol

Both wrappers store one logical object as two backend objects:

- `meta/<location>`: a small metadata document and the **only commit
  point**, pointing at the current payload generation;
- `gen/<location>/<generation>`: an immutable payload.

A put writes a fresh generation, then switches the metadata pointer with a
single backend write. A crash before the switch leaves the previous version
intact; a crash after it means the put took effect; torn reads are
impossible because readers resolve the pointer and then read an immutable
object. Replaced generations are deleted best-effort and otherwise reclaimed
by `collect_garbage`, which is meant to run while the store is quiescent
(for example at open).

ETags are opaque commit identities derived from fresh generation ids, not
content hashes, so identical payloads written by successive commits get
distinct tokens and ABA lost updates are detected.

Stores written before 0.10 keep payloads at `data/<location>`; they stay
readable and migrate on their first overwrite. The format only rolls
forward: data written by 0.10 or later cannot be read by older versions.

## Contracts

- **Single writer per key.** Concurrent mutations of the same key must be
  coordinated by the caller; AndaDB runs one writer per store. Clones share
  the per-key critical section and the cache; separately built instances do
  not. A second `PutMode::Create` writer is rejected by the backend's
  conditional write, but `Overwrite` / `Update` writers and the garbage
  collector are only safe under the single-writer assumption.
- **Unknown outcomes.** A metadata mutation that reached the backend and then
  failed has an unknown outcome: the key is evicted from the cache, and a
  cancellation invalidates the whole cache.
- **Durability** still depends on the backend, such as `with_fsync(true)` on
  `LocalFileSystem`.
- **Metadata authentication.** Encrypted metadata is sealed to its logical
  path. Older unauthenticated metadata is accepted with a warning; call
  `with_strict_metadata_auth()` once every legacy object has been rewritten
  to close that downgrade path.

## Testing

```bash
cargo test -p anda_object_store
```

## Technical reference

- [docs/anda_object_store.md](../../docs/anda_object_store.md): design,
  conditional writes and the encryption format
- [Storage and recovery quick reference](../../skills/anda-db/references/storage_and_recovery.md):
  backends, encryption, cache budgets and shutdown
- [docs/anda_db.md](../../docs/anda_db.md)

## Related crates

- [`anda_db`](../anda_db): the embedded database built on this storage layer
- [`object_store`](https://crates.io/crates/object_store): the backend
  integrations (local filesystem, memory, S3, GCS, Azure, HTTP)

## License

MIT. See [LICENSE](../../LICENSE).
