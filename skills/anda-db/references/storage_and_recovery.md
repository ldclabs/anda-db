# Storage and Recovery

Source: [Storage](../../../rs/anda_db/src/storage.rs),
[database lifecycle](../../../rs/anda_db/src/database.rs),
[collection recovery](../../../rs/anda_db/src/collection/recovery.rs), and
[object-store guide](../../../docs/anda_object_store.md).

## Backend selection

Database startup takes `Arc<dyn object_store::ObjectStore>`.
`Arc::new(object_store::memory::InMemory::new())` works directly for ephemeral
tests. Cloud stores such as S3, GCS, and Azure require their `object_store`
features and a backend with the read/write/conditional operations AndaDB needs.
The HTTP backend is read-only and is not a writable database backend.
For direct convenience calls such as `store.get(...)` / `store.put(...)`,
import `object_store::ObjectStoreExt` in object_store 0.14; `ObjectStore`
alone does not bring those methods into scope.

For plain local storage, create the directory and wrap
`LocalFileSystem::new_with_prefix(path)?.with_fsync(true)` with
`MetaStoreBuilder::new(local, 10_000).build()`. The native filesystem backend
does not supply conditional PUT updates; the wrapper supplies logical ETags
and update semantics under the single-writer contract.

`EncryptedStore` includes its own sidecar metadata and conditional semantics,
so it can wrap the filesystem directly; a second `MetaStore` layer is not
required. Supply an application-managed 32-byte key, not a fixed example key:

```rust
use anda_object_store::{EncryptedStore, EncryptedStoreBuilder};
use object_store::local::LocalFileSystem;

fn encrypted_local(
    path: &str,
    secret: [u8; 32],
) -> Result<EncryptedStore<LocalFileSystem>, Box<dyn std::error::Error>> {
    std::fs::create_dir_all(path)?;
    let local = LocalFileSystem::new_with_prefix(path)?.with_fsync(true);
    Ok(EncryptedStoreBuilder::with_secret(local, 10_000, secret)
        .with_meta_cache_bytes(64 * 1024 * 1024)
        .with_strict_metadata_auth()
        .build())
}
```

Strict metadata authentication is suitable for a new encrypted store. It
rejects legacy unauthenticated metadata; for an existing store, migrate those
objects before enabling it. `with_conditional_put()` is a deprecated no-op
and should be omitted.

Logical ETags identify commits, not content hashes; identical successive
writes can have different ETags. Immutable payload generations are committed
by switching the metadata pointer. This prevents torn generations but does
not create multi-writer transactions. Use clones of the same store instance;
separately built wrappers do not share their per-key coordination.

Local fsync durability depends on the backend and platform. In the reviewed
`object_store 0.14.1`, `with_fsync(true)` covers writes but does not fsync
standalone deletes; do not describe it as complete host-power-loss durability.
Check the resolved dependency's implementation when that guarantee matters.

## Storage configuration and budgets

```rust
use anda_db::storage::StorageConfig;

let config = StorageConfig {
    cache_max_capacity: 10_000,
    cache_max_bytes: None,
    compress_level: 3,
    object_chunk_size: 256 * 1024,
    max_small_object_size: 2000 * 1024,
    bucket_overload_size: 1024 * 1024,
};
```

These are the defaults. `cache_max_capacity` counts entries, with 0 disabling
the cache. `cache_max_bytes: Some(n)` overrides the entry count; `Some(0)`
disables it. For a new namespace, prefer an explicit budget such as
`StorageConfig::default().with_cache_max_bytes(64 * 1024 * 1024)`.

**All StorageConfig fields are fixed at first initialization.** Reconnecting
uses persisted `storage_meta.cbor` settings and ignores supplied differences
(with a warning). Changing the startup config does not resize an existing
namespace's cache or change its compression. `compress_level: 0` disables
compression for a newly configured namespace.

Wrapper metadata caches have a separate byte budget:
`with_meta_cache_bytes` on either store builder retains the entry cap and
defaults to 64 MiB. `with_metadata_limits(MetadataLimits { ... })` controls
metadata bytes, logical object size, and encryption chunk count. These are
separate from AndaDB's object cache and stream limit.

`Storage::stream_reader` / `stream_writer` default to a 256 MiB plaintext
limit. Use matching `*_with_limit` budgets for larger objects. A stream writer
publishes on `AsyncWriteExt::shutdown().await?`; dropping it without shutdown
does not publish the object. Keep application blobs in a separate namespace
rather than writing into collection-owned document/index paths.

## Cancellation and shutdown

One live writer per database namespace is the deployment contract. A
conditional-write `Precondition` conflict indicates conflicting storage
mutation; stop the competing writer and reopen/recover, rather than treating
it as a routine retry. `DBConfig::lock` checks a persisted token and is not
an exclusive lease.

Mutations, flushes, closes, durable extension writes, and compactions must be
awaited to completion. Avoid racing them directly in `tokio::select!` or
`timeout`, or aborting their task: cancellation after mutation begins is
treated as a crash and can poison the handle. Unknown storage-write outcomes
can poison it as well.

Use `collection.is_poisoned()`, `collection.state()`, `DBError::is_poisoned()`,
or `DBError::collection_state()` instead of parsing error strings. Reopen a
poisoned collection with `db.open_collection(name, callback)`; that path drains
the old handle and recovers from storage without flushing its uncertain
in-memory state. Reinstall the same tokenizer/hooks. Reads on retired or
poisoned handles are best-effort and may lag storage. Reopening does not
determine whether an unacknowledged application write should be submitted
again; reconcile its identity before retrying an insert.

`db.flush().await?` checkpoints indexes, ids, and metadata. Successful document
mutations already write their document objects; recovery reconciles an
uncheckpointed generation. Use `db.close().await?` for explicit shutdown when
the caller needs to handle the close result.

For background flushes, `auto_flush` returns `()` and closes on cancellation:

```rust
use std::time::Duration;
use tokio_util::sync::CancellationToken;

let stop = CancellationToken::new();
let flush_task = tokio::spawn({
    let db = db.clone();
    let stop = stop.clone();
    async move { db.auto_flush(stop, Duration::from_secs(30)).await }
});
// Run application work; drain its mutations before stopping the flusher.
stop.cancel();
flush_task.await?;
```

The cancellation token stops the loop between operations; it does not cancel
an in-flight flush. Await the task instead of aborting it. Flush/close errors
are logged by `auto_flush`; its `JoinHandle<()>` does not return a `DBError`.

Read-only mode rejects document/index writes. Ordinary collection flushes
skip persistence while read-only; `close` still flushes pending state.
Database metadata changes can still be persisted by `db.flush()`. Thus
`set_read_only(true)` is not a blanket guarantee of no storage writes.

## Extensions and recovery maintenance

`set_extension` and `set_extension_from` stage small metadata values for a
later flush. `save_extension` / `save_extension_from` are async immediate
metadata writes; `remove_extension` is async and returns the removed value.
Collection and database expose these families, with lifecycle/read-only
details documented on each method. Do not use extensions for bulk blobs or
assume they create multi-document transactions.

Open recovers under the same tokenizer/hooks used for writes. Transient I/O
errors fail recovery and can be retried by reopening; corrupt/schema-invalid
objects are reported in `recovery_issues()`. The separate
`reconcile_storage` maintenance API performs a full document listing; see
the [core reference](anda_db_quick_ref.md#index-configuration-and-maintenance).

`MetaStore` and `EncryptedStore` expose `collect_garbage()` and
`collect_garbage_with_options(GarbageCollectionOptions)` for unreferenced
payload generations. Run them while the store is quiescent. Use a logical
prefix and explicit budgets for large stores; garbage collection is not a
replacement for collection recovery and should not race application traffic.
