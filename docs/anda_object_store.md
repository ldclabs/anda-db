# anda_object_store

`anda_object_store` is the storage substrate that AndaDB and the AI memory
brain build on top of. It extends the [`object_store`][object_store] crate
with two composable wrappers:

| Wrapper          | Purpose                                                                                                                                         |
| ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `MetaStore`      | Side-car metadata (size, content-addressable ETag, original backend tags). Provides uniform conditional-update semantics on top of any backend. |
| `EncryptedStore` | Transparent, chunked AES-256-GCM encryption-at-rest. Random per-object nonce, per-chunk authentication tags, range-get friendly.                |

Both wrappers implement the [`object_store::ObjectStore`] trait, so any
caller written against `object_store` (S3, GCS, Azure Blob, local filesystem,
in-memory, …) can drop them in transparently. They can also be layered:
`EncryptedStore<MetaStore<S>>` is **not** a typical configuration because
each wrapper already manages its own metadata; instead, choose one wrapper
based on whether the workload needs encryption.

[object_store]: https://docs.rs/object_store
[`object_store::ObjectStore`]: https://docs.rs/object_store/latest/object_store/trait.ObjectStore.html

---

## 1. Why this crate exists

AndaDB stores knowledge artifacts (KIP capsules, vector indexes, B-Tree
segments, full-text shards, encrypted memories) on top of the
`object_store` abstraction. Vanilla `object_store` is a great portable
abstraction, but two practical problems remain:

1. **Conditional updates are not portable.** S3 supports conditional puts via
   `If-Match`/`If-None-Match`, but `LocalFileSystem` does not. AndaDB's
   crash-safe write protocol depends on optimistic concurrency control, so
   we need a uniform implementation everywhere.
2. **Encryption-at-rest must be transparent and seekable.** AI memories
   often contain personal or sensitive data. We want one cipher key per
   logical store, GCM-level integrity, and the ability to read arbitrary
   byte ranges (vector index pages, BM25 postings) without downloading the
   whole object.

`MetaStore` solves (1). `EncryptedStore` solves (2) and inherits the
machinery for (1) optionally.

---

## 2. On-disk layout

Both wrappers split the underlying namespace into two prefixes. By default:

```
data/<logical-path>   — payload (plaintext for MetaStore, ciphertext for EncryptedStore)
meta/<logical-path>   — CBOR-encoded Metadata side-car
```

Callers always interact with the *logical* path (`<logical-path>`); the
wrapper rewrites paths transparently for every read, write, list, copy and
delete operation, and strips the prefix back when results are returned.

### 2.1 MetaStore metadata (CBOR)

```text
{ "s": <u64 size>,
  "e": <Option<String> base64url(SHA3-256(payload))>,
  "o": <Option<String> ETag from inner store>,
  "v": <Option<String> version from inner store> }
```

### 2.2 EncryptedStore metadata (CBOR)

```text
{ "s": <u64 ciphertext_size>,
  "e": <Option<String> base64url(SHA3-256(ciphertext))>,
  "o": <Option<String> inner ETag>,
  "v": <Option<String> inner version>,
  "n": <12-byte base nonce>,
  "t": [<16-byte chunk_0 tag>, <16-byte chunk_1 tag>, …] }
```

The `aes_tags` vector grows linearly with the object size; for a 1 GiB
object at the default 256 KiB chunk size, the metadata costs ~64 KiB.

---

## 3. `MetaStore`

```rust
use anda_object_store::MetaStoreBuilder;
use object_store::local::LocalFileSystem;

let store = MetaStoreBuilder::new(
        LocalFileSystem::new_with_prefix("./data")?,
        10_000, // metadata cache capacity
    )
    .build();
```

### 3.1 What it does

- Tracks a per-object `Metadata { size, e_tag, original_tag, original_version }`.
- Computes a content-addressable ETag (`base64url(SHA3-256(payload))`) on
  every put. This ETag is what `MetaStore` exposes to callers; the inner
  backend's ETag is preserved as `original_tag` and used internally to
  forward `if_match` / `if_none_match` preconditions.
- Implements `PutMode::Update(…)` precondition checks against the cached
  metadata, so `LocalFileSystem` (which has no native CAS) gains the same
  optimistic-concurrency guarantee as S3 or Azure Blob.
- Forwards `version` from the underlying backend through `PutResult.version`
  unchanged when the backend supports versioning.

### 3.2 Concurrency model

All metadata mutations go through `update_meta_with`, which uses
`moka::Cache::and_try_compute_with` to serialize concurrent writers on the
same key. The closure is invoked exactly once with the current cached
metadata (or the freshly loaded copy if not cached) and is expected to:

1. Validate caller preconditions.
2. Write the data object to the inner store.
3. Return the new `Metadata`.

If the closure returns an error or the inner write fails, the cache is left
untouched. On success, the metadata side-car is persisted **before** the
cache is updated, so a failed metadata put never leaves a stale entry in
front of the on-disk truth.

### 3.3 Semantic guarantees

| Operation                   | Behaviour                                                                                          |
| --------------------------- | -------------------------------------------------------------------------------------------------- |
| `put_opts`                  | Writes data, then metadata, then updates cache. Atomic from the cache's point of view.             |
| `put_multipart`             | Streams parts to the inner uploader; finalises metadata in `complete()`.                           |
| `get_opts`                  | Forwards range/preconditions, swaps the response ETag for the content-addressable one.             |
| `delete_stream`             | Deletes data first, then metadata. Tolerates missing metadata so partial failures are recoverable. |
| `copy_opts` / `rename_opts` | Performs the operation on both `data/` and `meta/` paths, invalidates caches.                      |
| `list*`                     | Lists data, fetches metadata concurrently (8-way), restores ETag.                                  |

> ⚠️ **Crash atomicity.** A crash between writing data and writing metadata
> can leave the side-car missing. Subsequent reads of that key surface as
> `Error::NotFound` for the metadata path; `delete_stream` tolerates this
> case explicitly. On the write path, the data is the source of truth: the
> caller can simply re-issue the put, which will rewrite both objects.

### 3.4 Path rewriting helpers

`MetaStore` exposes only the logical path; internally the wrapper uses:

| Helper                 | Maps                                      |
| ---------------------- | ----------------------------------------- |
| `full_path(loc)`       | `loc` → `data/<loc>`                      |
| `meta_path(loc)`       | `loc` → `meta/<loc>`                      |
| `strip_prefix(p)`      | `data/<loc>` → `<loc>` (else passthrough) |
| `strip_meta_prefix(p)` | `meta/<loc>` → `<loc>` (else passthrough) |

---

## 4. `EncryptedStore`

```rust
use anda_object_store::EncryptedStoreBuilder;
use object_store::local::LocalFileSystem;

let secret: [u8; 32] = /* 256-bit key from KMS / file / env */ [0; 32];

let store = EncryptedStoreBuilder::with_secret(
        LocalFileSystem::new_with_prefix("./data")?,
        10_000,        // metadata cache capacity
        secret,
    )
    .with_chunk_size(256 * 1024)        // 256 KiB plaintext chunks (default)
    .with_conditional_put()             // enable for LocalFileSystem
    .build();
```

### 4.1 Cipher and key

- Algorithm: **AES-256-GCM** via [`aes_gcm`].
- Key: a single 32-byte symmetric key per `EncryptedStore` instance. Inject
  through `with_secret([u8; 32])` or pass a pre-built `Arc<Aes256Gcm>` via
  `EncryptedStoreBuilder::new`.
- AAD: empty (`&[]`) for every chunk. The chunk index (encoded into the
  nonce) and the per-object base nonce together provide context binding.

[`aes_gcm`]: https://docs.rs/aes-gcm

### 4.2 Chunked encryption

Each object is split into fixed-size **plaintext** chunks (default 256 KiB,
configurable). Each chunk is encrypted independently with
`encrypt_in_place_detached`:

```text
ciphertext_chunk_i = AES-256-GCM_Enc(key, nonce_i, plaintext_chunk_i)
tag_i              = corresponding 16-byte authentication tag
```

The ciphertext is written contiguously to `data/<loc>`; the per-chunk tags
are stored in `meta.aes_tags[i]`. The ciphertext therefore has exactly the
same length as the plaintext, which is what makes range-get inexpensive.

#### Chunk-size trade-offs

| Chunk size | Throughput | Random-access cost                 | Metadata size               |
| ---------- | ---------- | ---------------------------------- | --------------------------- |
| 64 KiB     | lower      | best (smallest read amplification) | larger (4× tags vs 256 KiB) |
| 256 KiB ★  | balanced   | good                               | balanced                    |
| 1 MiB      | higher     | small reads pay 1 MiB I/O          | smaller                     |

★ default. Pick based on the typical access pattern of the workload (KV
look-ups, vector page reads, sequential scans, …).

### 4.3 Nonce derivation

```text
base_nonce  : 12 bytes, random per object
nonce_i     : derive_gcm_nonce(base_nonce, i) =
              base_nonce[0..4] || LE_u64(LE_u64(base_nonce[4..12]) + i)
```

The first 4 bytes act as a per-object random salt; the trailing 8 bytes are
a chunk-index counter. Because the salt is unique per object with
overwhelming probability (2⁻³² collision per *pair*, 2⁻¹⁶ collision birthday
bound for ~65k objects under the same key — and the counter portion further
disambiguates within an object), each `(key, nonce)` pair is unique across
all chunks. AES-GCM's nonce-uniqueness requirement is satisfied.

> ⚠️ **Key-rotation note.** AES-GCM tolerates ~2³² random nonces under one
> key before collision risk becomes meaningful. For very large stores
> (hundreds of millions of objects under one key), rotate keys periodically
> by re-encrypting under a new `Aes256Gcm` instance, or shard logical
> namespaces across multiple stores with distinct keys.

### 4.4 Range reads

`get_opts(GetOptions { range: Some(r), … })` is implemented seekably:

1. Convert the caller's plaintext range `r = [a, b)` to chunk indices
   `[a / chunk_size, ceil(b / chunk_size))`.
2. Issue a single ciphertext range request for those chunks to the inner
   store.
3. Stream-decrypt each chunk in place, trim leading bytes (`a % chunk_size`)
   on the first chunk, truncate the last chunk to `b - a` total bytes,
   yield as the result stream.

`get_ranges` does the same in non-streaming form and additionally caches
the most recently decrypted chunk so adjacent ranges within one chunk only
pay one decryption.

### 4.5 Multipart uploads

`EncryptedStoreUploader` buffers caller-supplied parts until at least one
full plaintext chunk is available, then encrypts the chunk in place and
forwards the ciphertext to the inner uploader's `put_part`. `complete()`
flushes any remaining (possibly short) tail chunk, persists the encryption
metadata, and returns a `PutResult` whose `e_tag` is the
content-addressable hash over the ciphertext.

Because GCM is non-streaming per chunk, abort/retry semantics are handled
by the underlying `MultipartUpload`; the encryption layer is stateless
across upload sessions.

### 4.6 Conditional put

`with_conditional_put()` mirrors `MetaStore`'s behaviour:

- `PutMode::Update(v)` checks `v.e_tag` against the cached metadata's
  content-addressable e_tag and rejects with `Error::Precondition` on
  mismatch.
- `if_match` / `if_none_match` on `get_opts` are translated into the inner
  backend's original ETag.
- `list*` results have their ETag rewritten to the content-addressable
  value (concurrent metadata fetches, 8-way buffered).

When `conditional_put` is **not** enabled, `put_opts` returns the inner
backend's ETag and version directly, and `list*` operations skip the
metadata fan-out (they only rewrite paths). Use this mode against backends
that already provide strong CAS semantics (S3, GCS, Azure Blob).

### 4.7 Semantic guarantees (deltas vs `MetaStore`)

| Aspect             | Behaviour                                                                                                                           |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------- |
| Plaintext exposure | Plaintext never crosses the inner-store boundary.                                                                                   |
| Integrity          | Tampering with any ciphertext chunk fails decryption with `Error::Generic("AES256 decrypt failed …")`.                              |
| Truncation attacks | A truncated object yields fewer ciphertext bytes than `meta.size` indicates and surfaces as a decrypt or explicit truncation error. |
| Reordering         | Each chunk's nonce is bound to its index, so swapping two chunks fails authentication.                                              |
| Random-access cost | One inner range get per request, decrypts only the touched chunks.                                                                  |

---

## 5. Metadata cache

Both wrappers use [`moka::future::Cache`] keyed by logical path:

- `MetaStoreBuilder::new(_, capacity)` — TTL 1h, custom TTL via
  `with_meta_cache_ttl`.
- `EncryptedStoreBuilder::new(_, capacity, _)` — TTL 1h, time-to-idle 20 min.
- `EncryptedStoreBuilder::with_meta_cache(custom)` — supply a fully-tuned
  cache (e.g. with eviction listeners for telemetry).

The cache is treated as an authoritative read-through layer for hot
metadata; mutations are written through the underlying store first. Cache
eviction simply forces a re-read on the next access — the on-disk metadata
is always the source of truth.

[`moka::future::Cache`]: https://docs.rs/moka/latest/moka/future/struct.Cache.html

---

## 6. Recommended composition with `LocalFileSystem`

`object_store::local::LocalFileSystem` does not implement conditional puts
or strong ETags. The recommended set-up for AndaDB on local disk is:

```rust
use anda_object_store::{EncryptedStoreBuilder, MetaStoreBuilder};
use object_store::local::LocalFileSystem;

// (a) Metadata-only — no encryption needed (e.g. shared cache disk):
let store = MetaStoreBuilder::new(
        LocalFileSystem::new_with_prefix("./db")?,
        10_000,
    )
    .build();

// (b) Encryption-at-rest — recommended for AI memory data:
let key: [u8; 32] = load_key_from_kms()?;
let store = EncryptedStoreBuilder::with_secret(
        LocalFileSystem::new_with_prefix("./db")?,
        10_000,
        key,
    )
    .with_conditional_put()   // enable CAS on top of the local FS
    .with_chunk_size(256 * 1024)
    .build();
```

For S3-like backends, omit `with_conditional_put()` to avoid the
metadata-fanout cost on `list*`, and rely on the backend's native ETag.

---

## 7. Threading and `Send`/`Sync`

- `MetaStore<T>` and `EncryptedStore<T>` are `Clone + Send + Sync` whenever
  `T: ObjectStore + Send + Sync` (which `ObjectStore` mandates). They share
  state through `Arc`.
- The metadata cache (`moka::future::Cache`) is internally
  thread-safe; `update_meta_with` serializes mutations per key.
- Streams returned from `list*`, `get_opts` and `delete_stream` are
  `Send + 'static` and can be moved across tasks.

---

## 8. Errors

Both wrappers return `object_store::Error`, preserving the variant from the
underlying backend wherever possible. Two additions are introduced:

- `Error::Generic { store: "MetaStore" | "EncryptedStore", source }` — for
  CBOR (de)serialization errors and AES-GCM cryptographic failures
  (decryption tag mismatch, tampered ciphertext, missing per-chunk tag,
  invalid range).
- `Error::Precondition { … }` — emitted when `PutMode::Update(v)` is
  rejected by the metadata-side e_tag comparison.

`map_arc_error` reconstructs path-bearing variants when `moka` returns a
shared `Arc<Error>` from a deduplicated loader; non-path variants collapse
into `Error::Generic`.

---

## 9. Limitations and future work

- **Crash-window between data and metadata writes.** The current ordering
  writes data first; an interrupted put may leave a data object without
  metadata. Reads of such an object surface as `NotFound` for the metadata
  side-car. Background reconciliation (rehash, rebuild metadata) is the
  caller's responsibility for now.
- **No envelope encryption / per-object DEKs.** All chunks of all objects
  share a single 256-bit key. Workloads that need per-tenant key isolation
  should layer multiple `EncryptedStore` instances on top of namespaced
  prefixes, or wait for a future envelope-encryption mode.
- **No content compression.** Compression-before-encryption is left to the
  caller, since blind compression interacts poorly with chunk-aligned
  range reads.
- **`copy_opts` / `rename_opts` are not atomic across `data/` + `meta/`.**
  A failure between the two operations can desynchronize the side-car.
  This matches the wider `object_store` contract, which doesn't promise
  atomic multi-object operations.

---

## 10. Quick API reference

### MetaStore

```rust
let store = MetaStoreBuilder::new(inner, 10_000)
    .with_meta_cache_ttl(Duration::from_secs(60 * 60))
    .build();
```

| Method (via `ObjectStore`)                          | Notes                                                        |
| --------------------------------------------------- | ------------------------------------------------------------ |
| `put_opts`                                          | Computes content ETag; honours `PutMode::Update` everywhere. |
| `put_multipart_opts`                                | Streams to inner, finalises metadata in `complete()`.        |
| `get_opts`                                          | Range, `if_match`, `if_none_match` all supported.            |
| `get_ranges`                                        | Forwarded as-is to the inner store.                          |
| `delete` / `delete_stream`                          | Deletes data + metadata; tolerates missing metadata.         |
| `list` / `list_with_offset` / `list_with_delimiter` | Concurrent metadata fan-out (8-way).                         |
| `copy_opts` / `rename_opts`                         | Both prefixes are mirrored; cache invalidated.               |

### EncryptedStore

```rust
let store = EncryptedStoreBuilder::with_secret(inner, 10_000, key)
    .with_chunk_size(256 * 1024)
    .with_conditional_put()
    .with_meta_cache(custom_cache)
    .build();
```

Supports the full `ObjectStore` surface; range reads decrypt only the
chunks that intersect the request.

---

## 11. Examples

### 11.1 In-memory smoke test

```rust
use anda_object_store::EncryptedStoreBuilder;
use object_store::{ObjectStore, memory::InMemory, path::Path};

#[tokio::main]
async fn main() -> object_store::Result<()> {
    let store = EncryptedStoreBuilder::with_secret(InMemory::new(), 1_000, [7u8; 32])
        .build();

    let path = Path::from("memory/note-001");
    store.put(&path, b"hello, anda".as_ref().into()).await?;

    let body = store.get(&path).await?.bytes().await?;
    assert_eq!(&body[..], b"hello, anda");
    Ok(())
}
```

### 11.2 Range-aware decryption against local FS

```rust
use anda_object_store::EncryptedStoreBuilder;
use object_store::{
    GetOptions, GetRange, ObjectStore, local::LocalFileSystem, path::Path,
};

#[tokio::main]
async fn main() -> object_store::Result<()> {
    let key = [42u8; 32];
    let store = EncryptedStoreBuilder::with_secret(
            LocalFileSystem::new_with_prefix("./data")?,
            10_000,
            key,
        )
        .with_chunk_size(64 * 1024)
        .with_conditional_put()
        .build();

    let path = Path::from("vec/segments/0001.bin");
    let payload = vec![0u8; 4 * 1024 * 1024]; // 4 MiB
    store.put(&path, payload.into()).await?;

    // Read bytes 1_000_000..1_000_512 — only one ciphertext chunk fetched.
    let opts = GetOptions {
        range: Some(GetRange::Bounded(1_000_000..1_000_512)),
        ..Default::default()
    };
    let res = store.get_opts(&path, opts).await?;
    let bytes = res.bytes().await?;
    assert_eq!(bytes.len(), 512);
    Ok(())
}
```

---

## 12. Cargo features

The crate itself has no Cargo features; the underlying `object_store`
backends are gated by their own features (`fs`, `aws`, `gcp`, `azure`, …).
Enable whichever backend(s) you need at the application layer:

```toml
[dependencies]
anda_object_store = "0.3"
object_store      = { version = "*", features = ["aws", "fs"] }
```

The crate's own test suite runs against `InMemory` and `LocalFileSystem`
(the latter under `#[ignore]` so it's opt-in via `cargo test -- --ignored`).
