# anda_object_store

`anda_object_store` is the storage substrate that AndaDB and the AI memory
brain build on top of. It extends the [`object_store`][object_store] crate
with two composable wrappers:

| Wrapper          | Purpose                                                                                                                                          |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `MetaStore`      | Side-car metadata (size, per-commit logical ETag). Provides uniform conditional-update semantics on top of any backend.                          |
| `EncryptedStore` | Transparent, chunked AES-256-GCM encryption-at-rest. Random per-object nonce, per-chunk authentication tags, range-get friendly.                 |

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
machinery for (1).

---

## 2. On-disk layout and write protocol

Both wrappers split the underlying namespace into three prefixes:

```
meta/<logical-path>              — CBOR-encoded Metadata document: the COMMIT POINT
gen/<logical-path>/<generation>  — immutable payload (plaintext for MetaStore, ciphertext for EncryptedStore)
data/<logical-path>              — legacy payload location (pre-0.10 layout, read-only)
```

Callers always interact with the *logical* path (`<logical-path>`); the
wrapper rewrites paths transparently for every read, write, list, copy and
delete operation.

### 2.1 Immutable generations + pointer switch

A logical object exists **iff** its metadata document exists. The document
carries a *generation* pointer identifying the payload object. A write is:

1. Write the payload to a **fresh, immutable** generation object
   `gen/<path>/<generation>` (generations are
   `<16-hex ms timestamp>-<8-hex random>`; a generation path is written
   exactly once and never overwritten).
2. Commit by writing `meta/<path>` with the new pointer — a **single backend
   put**, which is the only atomicity the protocol needs.
3. Best-effort delete the replaced payload (previous generation, or the
   legacy `data/` object). Failures are logged and left to garbage
   collection.

Crash semantics follow directly:

- **Crash before the pointer switch** — the previous version stays fully
  intact and readable; the new generation is unreferenced garbage.
- **Crash after the pointer switch** — the write took effect; the replaced
  payload is garbage.
- **Torn reads are impossible by construction.** A reader resolves the
  pointer and then reads an immutable object, so it can never observe "old
  metadata + new payload" (which, for `EncryptedStore`, used to surface as an
  AES-GCM authentication failure indistinguishable from tampering). Even a
  `GetResult` stream that outlives the request stays consistent, because the
  generation it reads from is never rewritten.

If a reader resolves a *cached* pointer whose generation has just been
replaced and reclaimed by a concurrent in-process writer, the payload read
reports `NotFound`; the wrappers then invalidate the cached document and
re-resolve once, so the reader observes the new committed version.

### 2.2 Conditional writes

- `PutMode::Create` — rejected with `AlreadyExists` when a decodable
  metadata document exists. When no document exists, the metadata put itself
  is forwarded with `PutMode::Create`, so **cross-process** `Create` races
  are arbitrated by the backend's conditional write: exactly one winner.
- `PutMode::Update(v)` — `v.e_tag` is compared against the current
  document's logical ETag inside the per-key critical section. This works
  uniformly on every backend, including `LocalFileSystem`.
- `if_match` / `if_none_match` on reads are evaluated against the logical
  ETag and then stripped: once the precondition holds against the current
  commit point, the immutable payload cannot change under the reader.
- `if_modified_since` / `if_unmodified_since` on reads are evaluated in the
  wrapper too, against the same commit-point `last_modified` the call
  reports, and stripped before the request reaches the backend — otherwise
  the backend would answer them against the payload object's own mtime, a
  different clock. RFC 9110 §13.2.2 precedence is honoured: when `if_match`
  is present `if_unmodified_since` is ignored, and when `if_none_match` is
  present `if_modified_since` is ignored, so neither can reach the backend
  and produce a spurious `Precondition` / `NotModified`. Pre-0.10 documents
  carry no generation, so their date conditions are still left to the backend
  — which evaluates them against the legacy payload object, the very
  timestamp such a read reports.

Versions are **not** reported (`PutResult::version`, `ObjectMeta::version`
are `None`): replaced generations are reclaimed eagerly, so version-addressed
reads cannot be honoured. Conditional updates rely on the ETag.

#### The logical ETag is a CAS token, not a content fingerprint

The ETag is **not** a hash of the payload. Every put mints a fresh generation
and the ETag is `base64url(SHA3-256(generation ‖ payload))` — the generation
first, so the token identifies the *commit*, not the bytes.

This is load-bearing. `PutMode::Update(UpdateVersion { e_tag })` asks "is this
still the version I read?". A bare content hash answers the different question
"does it still hold the bytes I read?", and the two diverge on any A → B → A
rewrite: a writer holding the token for A would still pass the precondition
after two intervening commits and silently clobber them — a classic ABA lost
update. Seeding with the per-commit generation closes it.

Consequences:

- **Two puts of identical bytes produce different ETags.** The token is not a
  deduplication key and must not be used as a content digest.
- **`copy` / `rename` mint a new token for the target** rather than
  propagating the source's, so two distinct keys never share a CAS token and
  the target never inherits a token it may already have retired.
- `EncryptedStore` has always done the equivalent, seeding with its
  per-commit random nonce instead of the generation (§4.6).

### 2.3 Garbage collection

`MetaStore::collect_garbage` / `EncryptedStore::collect_garbage` run an
explicit mark-sweep pass:

1. **Mark** — read *every* metadata document first; nothing is deleted
   before the full referenced set is known.
2. **Sweep** — a payload object (`gen/` generation or legacy `data/` object)
   is a candidate only if the marked state does not reference it. Right
   before each deletion the key's metadata is re-read from the backend; a
   payload that is referenced *now* is never deleted. Generations minted at
   or after the collection started are skipped (in-flight writes), as are
   unrecognizable objects under `gen/` and all payloads of a key whose
   metadata exists but does not decode (conservative).

Run the collector when the store is otherwise quiescent (e.g. at open), in
line with the single-writer contract below. The collector holds the
referenced set in memory (one entry per logical key).

### 2.4 Single-writer contract

Concurrent mutations of the **same key** must be coordinated by the caller
(AndaDB deploys one writer per store). Within one process, the per-key
metadata critical section (the cache's compute entry) serializes writers.
Across processes, a second `Create` writer is rejected by the conditional
metadata write; `Overwrite`/`Update` writers and the garbage collector are
only safe under the single-writer assumption.

### 2.5 Backward compatibility (pre-0.10 layout)

Deployments written by anda_object_store < 0.10 store the payload directly
at `data/<logical-path>` and their metadata carries no generation pointer
(`generation: None`). Such objects stay fully readable (get, range get,
list, copy, rename), and the first overwrite migrates the key to the
generation layout: the new generation is written, the pointer switches, and
the legacy `data/` object is deleted best-effort. The legacy prefix is kept
separate from `gen/` so that, on a real filesystem, the legacy *file*
`data/<path>` and the generation *directory* `gen/<path>/` can coexist
during migration.

The format only rolls forward: metadata written by ≥ 0.10 carries the
generation pointer, which older releases do not understand. **Do not roll
back to a pre-0.10 binary after writing with this version.**

### 2.6 MetaStore metadata (CBOR)

```text
{ "s": <u64 size>,
  "e": <Option<String> base64url(SHA3-256(generation ‖ payload))>,
  "g": <Option<String> generation pointer; absent = legacy data/ layout> }
```

(Legacy documents additionally contain `"o"`/`"v"` — the pre-0.10 backend
ETag/version fields. They still decode; new writes omit them.)

### 2.7 EncryptedStore metadata (CBOR)

```text
{ "s": <u64 ciphertext_size>,
  "e": <Option<String> base64url(SHA3-256(base_nonce ‖ ciphertext))>,
  "o": <Option<String> legacy field, None for new writes>,
  "v": <Option<String> legacy field, None for new writes>,
  "n": <12-byte base nonce>,
  "t": [<16-byte chunk_0 tag>, <16-byte chunk_1 tag>, …],
  "c": <Option<u64> plaintext chunk size used at write time>,
  "av": <Option<u8> chunk AAD version>,
  "an": <Option<12-byte metadata auth nonce>>,
  "at": <Option<16-byte metadata auth tag>>,
  "g": <Option<String> generation pointer; absent = legacy data/ layout> }
```

The `aes_tags` vector grows linearly with the object size; for a 1 GiB
object at the default 256 KiB chunk size, the metadata costs ~64 KiB.

The chunk size is recorded per object (`"c"`), so objects stay readable
after the store is reconfigured with a different `with_chunk_size` value.
Metadata written by older versions lacks the field; readers then fall back
to the store's configured chunk size.

The metadata document is sealed with an AES-GCM tag over the logical path
and every field. The generation pointer participates in that AAD **only when
present**, so documents sealed by 0.9.x (no generation) keep verifying
byte-for-byte, while stripping or forging the pointer on a new document
fails authentication.

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

- Tracks a per-object
  `Metadata { size, e_tag, generation, committed_at_ms }`.
- Computes a per-commit logical ETag
  (`base64url(SHA3-256(generation ‖ payload))`) on every put — see
  [§2.2](#22-conditional-writes) for why the generation is mixed in. This
  ETag is what `MetaStore` exposes to callers and what `PutMode::Update` /
  `if_match` / `if_none_match` are checked against.
- Reports the *logical* object on `get` / `head` / `list`: the committed
  (authenticated) size from the metadata document rather than the backing
  generation object's length, and one commit-point `last_modified` that
  is identical across all three calls.
- Implements the immutable-generation write protocol of §2, so
  `LocalFileSystem` (which has no native CAS) gains the same
  optimistic-concurrency guarantee as S3 or Azure Blob.

### 3.2 Concurrency model

All metadata mutations go through `update_meta_with`, which uses
`moka::Cache::and_try_compute_with` to serialize concurrent writers on the
same key. The closure is invoked exactly once with the current committed
metadata (or the freshly loaded copy if not cached) and is expected to:

1. Validate caller preconditions.
2. Write the new payload generation to the inner store.
3. Return the new `Metadata`.

If the closure returns an error or the inner write fails, the cache is left
untouched and the previous version remains committed. On success, the
metadata document is persisted **before** the cache is updated, so a failed
metadata put never leaves a stale entry in front of the on-disk truth.

### 3.3 Semantic guarantees

| Operation                   | Behaviour                                                                                                                                       |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `put_opts`                  | Writes a fresh generation, then commits the pointer, then reclaims the replaced payload best-effort. Atomic at the commit point.               |
| `put_multipart`             | Streams parts into a fresh generation; `complete()` materializes it and switches the pointer. An unfinished upload never affects readers.       |
| `get_opts`                  | Resolves the pointer, evaluates the logical-ETag *and* date preconditions, reads the immutable payload (with one retry on a stale cached pointer). Reports the committed size and the commit-point `last_modified`. |
| `delete_stream`             | Per location: deletes the metadata document (the logical delete), then the payload best-effort. `NotFound` iff no commit point exists.         |
| `copy_opts` / `rename_opts` | Copies the payload into a fresh generation of the target, then commits the target pointer with a **new** logical ETag (never the source's); rename then deletes the source commit point. |
| `list*`                     | Enumerates `meta/` (commit points; 8-way concurrent decode). Uncommitted generations and crash leftovers are invisible by construction. Reports the same commit-point `last_modified` as `get`/`head`. |

### 3.4 Path mapping helpers

`MetaStore` exposes only the logical path; internally the wrapper uses:

| Helper                       | Maps                              |
| ---------------------------- | --------------------------------- |
| `meta_path(loc)`             | `loc` → `meta/<loc>`              |
| `generation_path(loc, gen)`  | `loc` → `gen/<loc>/<gen>`         |
| `legacy_path(loc)`           | `loc` → `data/<loc>` (pre-0.10)   |

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
    .build();
```

### 4.1 Cipher and key

- Algorithm: **AES-256-GCM** via [`aes_gcm`].
- Key: a single 32-byte symmetric key per `EncryptedStore` instance. Inject
  through `with_secret([u8; 32])` or pass a pre-built `Arc<Aes256Gcm>` via
  `EncryptedStoreBuilder::new`.
- Chunk AAD: new objects bind the chunk size and chunk index into every
  chunk tag (`"av": 1`); objects written before that used an empty AAD and
  remain readable.
- Metadata AAD: the document is sealed against its logical path and fields
  (see §2.7).

[`aes_gcm`]: https://docs.rs/aes-gcm

### 4.2 Chunked encryption

Each object is split into fixed-size **plaintext** chunks (default 256 KiB,
configurable). Each chunk is encrypted independently with
`encrypt_inout_detached`:

```text
ciphertext_chunk_i = AES-256-GCM_Enc(key, nonce_i, plaintext_chunk_i)
tag_i              = corresponding 16-byte authentication tag
```

The ciphertext is written contiguously to the generation object; the
per-chunk tags are stored in `meta.aes_tags[i]`. The ciphertext therefore
has exactly the same length as the plaintext, which is what makes range-get
inexpensive.

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

`get_ranges` fetches each requested range's chunk span with a single inner
range request, decrypts it in place, and caches the most recently decrypted
span so subsequent ranges that fall inside it pay no further I/O or
decryption.

### 4.5 Multipart uploads

`EncryptedStoreUploader` buffers caller-supplied parts until at least one
full plaintext chunk is available, then encrypts all complete chunks in
place and forwards them to the inner uploader as a single part. This keeps
the caller's part granularity, which matters for backends with minimum
part sizes (e.g. S3). The parts stream into a fresh immutable generation;
`complete()` flushes the remaining (possibly short) tail chunk, materializes
the generation, and switches the metadata pointer — a crash or failure
before the switch leaves the previous version fully readable.

Because GCM is non-streaming per chunk, abort/retry semantics are handled
by the underlying `MultipartUpload`; the encryption layer is stateless
across upload sessions.

### 4.6 Conditional semantics

`PutMode::Update`, `if_match` and `if_none_match` are always honoured (see
§2.2); they are evaluated against the logical ETag, which is
`base64url(SHA3-256(base_nonce ‖ ciphertext))` — the per-commit random nonce
first, so the token identifies the commit rather than the ciphertext. Two
puts of identical plaintext therefore produce different ETags, and a stale
token cannot pass a precondition after an A → B → A rewrite.

`EncryptedStoreBuilder::with_conditional_put()` is **`#[deprecated]`**:
delete the call. It has done nothing since the immutable-generation refactor
— the semantics it used to gate are unconditional on every backend, because
the protocol never forwards preconditions to the backend. The method still
compiles and is scheduled for removal.

### 4.7 Semantic guarantees (deltas vs `MetaStore`)

| Aspect             | Behaviour                                                                                                                             |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------- |
| Plaintext exposure | Plaintext never crosses the inner-store boundary.                                                                                     |
| Integrity          | Tampering with any ciphertext chunk fails decryption with `Error::Generic("AES256 decrypt failed …")`.                                |
| Metadata integrity | The sidecar document is sealed (path-bound GMAC); swapping, mutating or re-pointing it fails authentication.                          |
| Truncation attacks | A truncated object yields fewer ciphertext bytes than `meta.size` indicates and surfaces as a decrypt or explicit truncation error.   |
| Reordering         | Each chunk's nonce (and, for new objects, its AAD) is bound to its index, so swapping two chunks fails authentication.                |
| Random-access cost | One inner range get per request, decrypts only the touched chunks.                                                                    |
| Copy/rename        | The ciphertext is copied verbatim (chunk AAD is not path-bound); only the metadata document is resealed for the target path.          |

---

## 5. Metadata cache

Both wrappers use [`moka::future::Cache`] keyed by logical path:

- `MetaStoreBuilder::new(_, capacity)` — TTL 1h, custom TTL via
  `with_meta_cache_ttl`.
- `EncryptedStoreBuilder::new(_, capacity, _)` — TTL 1h, time-to-idle 20 min.
- `EncryptedStoreBuilder::with_meta_cache_ttl(ttl)` — rebuilds the built-in
  cache with that TTL, **keeping the capacity passed to `new`** (it used to
  rebuild at the default capacity, silently discarding it) and the default
  time-to-idle. It also discards a cache previously supplied through
  `with_meta_cache`, so call the two in the order you mean.
- `EncryptedStoreBuilder::with_meta_cache(custom)` — supply a fully-tuned
  cache (e.g. with eviction listeners for telemetry). It replaces the
  built-in cache wholesale, so the custom cache's own capacity and eviction
  policy apply instead of the capacity passed to `new`.

The cache is treated as an authoritative read-through layer for hot
metadata; mutations are written through the underlying store first, inside
the cache's per-key compute entry (which is also what serializes in-process
writers). Cache eviction simply forces a re-read on the next access — the
on-disk metadata is always the source of truth. Listings deliberately do not
seed the cache, so a slow listing can never clobber a newer committed
document.

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
    .with_chunk_size(256 * 1024)
    .build();
```

Consider calling `collect_garbage()` at open (or on a maintenance schedule)
to reclaim payloads abandoned by crashes.

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
  (decryption tag mismatch, tampered ciphertext or metadata, missing
  per-chunk tag, invalid range).
- `Error::Precondition { … }` — emitted when `PutMode::Update(v)` or an
  `if_match` precondition is rejected by the logical-ETag comparison.

A metadata document that exists but does not decode makes reads fail loudly
(`Error::Generic`); overwriting the key rebuilds it, compatibility-mode
listings skip it with a warning, and garbage collection keeps its payloads
(conservative) until the key is rewritten or deleted.

`map_arc_error` reconstructs path-bearing variants when `moka` returns a
shared `Arc<Error>` from a deduplicated loader; non-path variants collapse
into `Error::Generic`.

---

## 9. Limitations and future work

- **Garbage requires collection.** Crash-abandoned generations are invisible
  but occupy space until `collect_garbage` runs (the eager post-commit
  cleanup handles the common case). The collector is designed for
  quiescent/single-writer operation.
- **No version-addressed reads.** Replaced generations are reclaimed
  eagerly, so `GetOptions::version` cannot be honoured and versions are not
  reported; conditional updates use the per-commit logical ETag.
- **No envelope encryption / per-object DEKs.** All chunks of all objects
  share a single 256-bit key. Workloads that need per-tenant key isolation
  should layer multiple `EncryptedStore` instances on top of namespaced
  prefixes, or wait for a future envelope-encryption mode.
- **No content compression.** Compression-before-encryption is left to the
  caller, since blind compression interacts poorly with chunk-aligned
  range reads.
- **`rename_opts` is not atomic.** It is copy-then-delete at the commit
  level: a crash in between can leave both source and target committed
  (never a torn or missing object). This matches the wider `object_store`
  contract, which doesn't promise atomic multi-object operations.

---

## 10. Quick API reference

### MetaStore

```rust
let store = MetaStoreBuilder::new(inner, 10_000)
    .with_meta_cache_ttl(Duration::from_secs(60 * 60))
    .build();
let reclaimed = store.collect_garbage().await?;
```

| Method                                              | Notes                                                             |
| --------------------------------------------------- | ------------------------------------------------------------------ |
| `put_opts`                                          | Mints a per-commit logical ETag; honours `PutMode::Create/Update` everywhere. |
| `put_multipart_opts`                                | Streams into a generation; commits the pointer in `complete()`.   |
| `get_opts`                                          | Range, `if_match`, `if_none_match` all supported.                 |
| `get_ranges`                                        | Validated against the logical size, then forwarded.               |
| `delete` / `delete_stream`                          | Deletes the commit point, then the payload best-effort.           |
| `list` / `list_with_offset` / `list_with_delimiter` | Enumerates commit points; concurrent metadata decode (8-way).     |
| `copy_opts` / `rename_opts`                         | Payload copied into a fresh target generation; pointer committed. |
| `collect_garbage`                                   | Mark-sweep reclamation of unreferenced payloads.                  |

### EncryptedStore

```rust
let store = EncryptedStoreBuilder::with_secret(inner, 10_000, key)
    .with_chunk_size(256 * 1024)
    .with_meta_cache(custom_cache)
    .build();
```

Supports the full `ObjectStore` surface; range reads decrypt only the
chunks that intersect the request. `with_strict_metadata_auth()` rejects
pre-authentication legacy metadata once all objects have been resealed.

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
anda_object_store = "0.11"
object_store      = { version = "*", features = ["aws", "fs"] }
```

The crate's own test suite runs against `InMemory` and `LocalFileSystem`.
