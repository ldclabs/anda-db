# anda_object_store

[中文版](anda_object_store.zh.md)

`MetaStore` adds logical metadata and conditional updates to an `ObjectStore`.
`EncryptedStore` adds chunked AES-256-GCM encryption and uses the same commit
protocol. Choose one wrapper; nesting both usually adds redundant metadata I/O.

## Storage layout and commit protocol

```text
meta/<key>              CBOR metadata: the logical object's commit point
gen/<key>/<generation> payload, immutable once published
data/<key>             legacy payload for metadata without a generation
```

A normal put writes a new generation, publishes its metadata, then removes the
replaced payload best-effort. Generation writes prefer `PutMode::Create` and
retry a collision with another ID, up to eight attempts. If a backend explicitly
reports conditional create as unsupported, generation payloads use overwrite
mode instead; their strong IDs and the single-writer contract make collision
with an unseen path negligible.

Multipart allocates and registers a fresh ID, verifies that no payload already
uses it, then starts the backend upload directly. It does not persist an empty
reservation object, so abort and setup failure cannot leak placeholders or add
an extra version. Copy likewise prefers `CopyMode::Create`, retries collisions,
and uses overwrite mode only when conditional copy-create is unsupported.
Metadata remains the logical commit point in every case.

New generation IDs contain a 16-digit hexadecimal millisecond timestamp, a
32-digit random salt (128 bits), and a 16-digit process-wide monotonic sequence.
The sequence prevents reuse within the process even if the clock moves backward
or the random source repeats. Randomness separates independent processes.
Readers and GC also recognize the older `<16 hex>-<8 hex>` form. Both formats
require hexadecimal characters and a single path component.

A failure before metadata publication leaves the previous committed object
readable. A failure after publication may mean the new write took effect even
though the caller received an error. When a metadata put or delete has an
unknown outcome, or its future is cancelled, a synchronous drop guard invalidates
the shared metadata cache. The next lookup reloads the backend's committed state.
This deliberately makes the whole cache cold on rare uncertain outcomes; normal
successful operations update only their own key.

Readers resolve a commit point and read its immutable payload. If that payload
has been replaced and reclaimed, `get`, range reads and copy refresh metadata and
retry once on `NotFound`. This is not a general network retry policy. Older
payloads are reclaimed, so historical version addressing is unavailable.

## Concurrency and conditional operations

Clones of a wrapper share their metadata cache, per-key compute locks and
in-flight generation registry. The backend itself need not implement `Clone`.
Separately built wrappers do not automatically share this coordination, even if
they are in the same process.

Use one coordinated writer per logical store. Within a shared wrapper instance,
mutations of the same key are serialized. Across independent instances,
`PutMode::Create` on an absent commit point is arbitrated by the backend's
conditional metadata put. Cross-instance `Overwrite`, `Update` and GC require
external single-writer coordination.

The compatibility fallback for generation payloads does not emulate a backend
conditional write. A backend without conditional metadata puts therefore cannot
provide cross-instance logical `Create`; use external coordination when that
operation is required.

- `Create` never replaces an existing commit point, even if its metadata cannot
  be decoded or validated. A wrong encryption key or changed resource limits
  must not turn an existing object into an absent one. Explicit `Overwrite` may
  rebuild invalid metadata; its untrusted pointer is never followed for cleanup.
- `Update` requires the current logical ETag, freshly checked against the
  backend inside the per-key section. Invalid or absent metadata cannot satisfy
  it. `UpdateVersion::version` is rejected.
- `if_match`, `if_none_match`, and date conditions are evaluated against the
  logical object. ETag conditions take precedence over their corresponding date
  conditions, as in `GetOptions::check_preconditions`.
- `GetOptions::version` returns `NotSupported`; results expose `version: None`.
- Rename is copy-then-delete, not a multi-key atomic transaction. Self-rename
  validates existence and mode without deleting the object.

Conditional copy/rename checks the target commit point before copying any payload.
An existing target therefore fails without allocating a garbage generation. The
final conditional metadata put still arbitrates a target created after this check.

### Logical ETags

An ETag is an opaque **commit identity**, not a content checksum. New writes,
copies and renames calculate padded URL-safe Base64 of
`SHA3-256("anda_object_store.commit.v2:" || generation)`.
No full payload hash is needed to mint the token. Identical bytes in successive
commits have different ETags, preventing stale tokens from surviving an A → B → A
rewrite. Conditional operations continue to accept existing metadata's older
payload-derived ETags as opaque strings. Do not recalculate ETags from content.

`MetaStore` does not authenticate payload bytes. `EncryptedStore` verifies
ciphertext through AES-GCM chunk tags and authenticates its sidecar separately.

## Metadata formats and compatibility

Metadata uses compact CBOR and `cbor2` serialization. Common fields are:

| Field | Meaning |
| --- | --- |
| `s` | Logical size in bytes |
| `e` | Optional opaque logical ETag |
| `g` | Optional generation; absent means legacy `data/<key>` |
| `m` | Optional commit timestamp in Unix milliseconds |
| `o`, `v` | Legacy backend ETag/version fields |

`MetaStore` no longer writes absent `o`/`v`. Encrypted metadata retains them in
its serialization because older authentication AAD includes them. Encryption
adds these fields:

| Field | Meaning |
| --- | --- |
| `n` | 12-byte base nonce |
| `t` | Array of 16-byte chunk tags, in order |
| `c` | Chunk size used to encrypt this object |
| `av` | Chunk AAD version: 0 for legacy empty AAD, 1 for bound AAD |
| `an`, `at` | Sidecar authentication nonce and tag |

The sidecar's authenticated fields include its logical path, size, ETag, nonce,
tags, chunk settings, generation and commit time. New optional fields are added
to the AAD only when present, preserving verification of older sealed documents.
The in-memory validation certificate is never serialized; cloning metadata
clears it, and resealing clears any previous certificate.

Pre-0.10 documents without `g` remain readable. An overwrite migrates them to a
new generation and removes the legacy payload best-effort. Copy and rename
retain the source's chunk-AAD mode, explicitly record its actual chunk size (even
when old metadata lacked `c`), and reseal for the target path. Reconfiguring a
store's default chunk size therefore does not change how migrated objects read.
A pre-0.10 binary cannot read the generational layout.

`m` provides the same logical timestamp for get/head/list. Early generational
metadata without `m` falls back to the generation timestamp. Older metadata
without either falls back to the resolved backend object's timestamp; legacy
listing and payload timestamps may differ.

## Encryption and range access

The cipher is AES-256-GCM. Ciphertext has the plaintext's length because tags are
stored in the sidecar. The default encryption chunk size is 256 KiB; zero is
normalized to one byte. Choose a chunk size suited to expected range accesses
and configure metadata limits if using unusually small chunks.

Each object receives a random 96-bit base nonce. A chunk nonce keeps the first
four bytes and adds the chunk index to the trailing 64-bit counter. Indices
remain unique within an object; randomness across objects is probabilistic.
A 32-bit salt alone has about a 39.3% birthday collision probability at 65,536
samples; a salt match does **not** itself mean complete 96-bit nonces or counter
ranges collide. Key-use planning must account for chunk ranges and sidecar GMAC
invocations, rather than claiming that the salt can never repeat.

New chunk AAD binds the chunk size and index with domain separation. The fixed
52-byte AAD is built on the stack. Plaintext is yielded only after the relevant
chunk tag has been verified. A malformed/truncated chunk fails the read.

`get_opts(range)` expands to whole crypto chunks and trims the verified
plaintext. The decryption stream copies bounded batches (at most 64 KiB or one
crypto chunk, whichever is larger) out of each upstream buffer; retaining a small output chunk does not pin an entire large
plaintext allocation. The upstream backend may still retain its own buffer.
After range trimming, a result smaller than half its plaintext allocation is
copied into an independent buffer. Retaining a one-byte range therefore does not
retain a whole crypto chunk; full streaming batches keep their zero-copy handoff.
HEAD and empty-object reads do not decrypt a body.

`get_ranges` plans all requested ranges together, deduplicates overlapping
chunks, and coalesces adjacent spans without filling holes. Requests are bounded
to 8 MiB, or one crypto chunk when a configured chunk is larger. Up to eight
requests run concurrently. Fragments are returned in original input order, and
small results are copied out to avoid retaining large buffers. Large requested
results necessarily require memory proportional to the returned bytes.

## Multipart lifecycle

Encrypted uploads separate crypto chunks from transport parts. The default
physical part size is 8 MiB, configurable with `with_multipart_part_size`; values
below 5 MiB are normalized upward. All non-final physical parts are the same
size, even when callers provide differently sized or non-aligned input. This
matches the equal-size requirement documented for
[R2 multipart uploads](https://developers.cloudflare.com/r2/objects/upload-objects/#part-size-limits).
After emitting complete physical parts, a short residual tail is copied into an
independent allocation so it cannot retain a much larger ciphertext buffer.
Backend-specific maximum object/part limits still apply.

The wrapper reserves part order synchronously; callers may await returned part
futures concurrently. Every returned part future must succeed before completion.
A failed or dropped part, or cancelled/failed payload materialization, makes the
upload terminal: abort it and start a new upload. A cancelled tail cannot be
silently omitted by a later successful complete.

Once payload completion succeeds, metadata publication can be retried without
calling the backend's complete twice. The first attempt records the prior commit
identity. A retry succeeds only when the commit point is unchanged or already
references this upload, and it also verifies that the completed payload still
exists with the expected size. This prevents a stale retry from replacing a
newer acknowledged commit even when cleanup left the older payload in storage.
A repeated complete after a reported success returns the original result.

The commit timestamp is assigned under the per-key lock immediately before each
new metadata publication, after any retry checks. Encrypted metadata is sealed
there as well. A retry after a failed publication gets a fresh timestamp; if the
generation was already committed but its acknowledgement was lost, the retry
preserves the committed timestamp and ETag.

Abort performs backend cleanup before marking the handle aborted, so a definite
abort or delete failure remains retryable. If payload materialization has
finished, abort re-reads metadata under the per-key writer lock and deletes the
generation only when it is uncommitted. An uncertain metadata response therefore
cannot make abort delete a committed object. Successful completion, successful
abort and failed materialization release the GC registration; a completed handle
need not be dropped to permit GC. Abort a failed-part upload to release its
remaining backend resources. Dropping it releases the local GC registration,
while provider-side incomplete multipart sessions remain subject to that
provider's lifecycle cleanup. GC does not enumerate or abort those sessions.

`with_conditional_put()` remains a deprecated no-op. The wrappers always check
logical preconditions; backend conditional metadata writes provide the
cross-instance arbitration described above. Remove calls to this method.

## Metadata cache and resource limits

Both builders retain the historical entry-count parameter and also use a default
64 MiB **estimated key/value byte budget**. Weighted admission limits large tag
tables. Moka eviction is asynchronous and bookkeeping/allocator overhead is
additional; this is not an exact process-RSS bound.

- Metadata TTL defaults to one hour.
- Encrypted metadata additionally has a 20-minute idle timeout.
- `with_meta_cache_bytes(bytes)` selects the built-in weighted cache budget.
- `with_meta_cache_ttl(ttl)` preserves the original entry and byte budgets.
- `EncryptedStoreBuilder::with_meta_cache(custom)` replaces the cache wholesale;
  its own capacity/eviction policy applies. A subsequent TTL/byte-budget setter
  replaces that custom cache, so builder order is meaningful.

Builders defer constructing the built-in cache until `build()`, so repeated
configuration calls do not allocate and discard caches.

Decoded encrypted metadata is certified only after authentication, encoded-size
checks and layout checks succeed. The private certificate binds the exact
validation context (cipher, policy, configured legacy chunk size and limits) and
logical path. Repeated reads of a valid cached value skip scanning and
reauthenticating all tags. A raw external cache, another key/path, or a strict
reader cannot inherit unearned trust from an earlier context.

Cold reads and listings load and validate metadata under the same per-key lock as
commits, then fill the cache. Repeated listings and subsequent reads can reuse
those entries without refetching sidecars. A listing cannot insert its older
snapshot after a newer commit. Missing entries and compatibility-mode CBOR
failures are skipped without caching; semantic/authentication failures propagate.

`with_metadata_limits(MetadataLimits { ... })` configures these defaults:

| Limit | Default |
| --- | --- |
| Encoded metadata bytes | 64 MiB |
| Logical object size | 1 TiB |
| Encryption chunks per object | 4,194,304 |

Limits apply together. The encoded limit may be reached before the chunk-count
limit. Bodies are checked while streaming, including when their reported length
is inaccurate. Authenticated size/chunk-size/tag-count relationships must agree.
Increase limits explicitly for larger valid objects; very small chunks can make
metadata larger than the payload. A bound failure returns an error rather than
publishing an unreadable object.

## Garbage collection and corrupt metadata

`collect_garbage()` uses `GarbageCollectionOptions::default()`; the configurable
variant accepts a logical prefix, concurrency and memory-related cardinality
budgets. Defaults are eight concurrent requests, one million marked metadata
entries and 100,000 candidate payloads. A budget failure occurs before any
payload deletion. Use a narrower prefix or explicitly raise the budget.

GC completes its mark phase, then collects candidates, then sweeps. Metadata
loads are concurrent; candidates are grouped by key. Each key's current metadata
is reread and authenticated under the per-key mutation lock before its candidates
are deleted. This avoids a metadata read for every historical generation.

Unrecognized generation IDs, generations minted at/after collection start, and
registered in-flight payloads are skipped. A decodable but unauthenticated or
structurally invalid document is treated conservatively, just like undecodable
metadata: all its payloads are retained. Fetch failures abort collection.

The same validation policy governs old-payload cleanup after overwrite/delete.
An explicit repair can replace/remove a corrupt commit point while leaving its
untrusted payload references alone. A forged generation containing a path
separator cannot redirect cleanup into another key.

Strict encrypted listings reject legacy and undecodable metadata. Compatibility
listings skip undecodable documents with a warning but still reject failed
metadata authentication. Compatibility reads accept genuine unauthenticated
legacy objects; enable strict mode after migration to close that fallback.

## Request context and errors

Caller Extensions are forwarded to metadata get/put subrequests as well as
payload requests for APIs that accept them, including copy and multipart
publication. Get results preserve payload response Extensions. Put/complete
results expose the metadata commit response Extensions. Delimiter listing
preserves the backend listing response Extensions.

The ObjectStore delete/list/get_ranges interfaces do not carry request
Extensions; do not depend on them for per-request authorization that must execute
on every metadata cache hit. Configure authorization at the caller/store boundary.

Errors use `object_store::Error`: logical NotFound/AlreadyExists/Precondition
paths are reported where remapped by the wrapper; backend and validation errors
retain their useful diagnostics. A backend failure is not proof that no mutation
occurred. Invalid version addressing returns NotSupported for get and
Precondition for update.

## Local filesystem composition and durability

```rust,no_run
use anda_object_store::{EncryptedStoreBuilder, MetaStoreBuilder};
use object_store::local::LocalFileSystem;

# fn configure() -> object_store::Result<()> {
let plain = MetaStoreBuilder::new(
    LocalFileSystem::new_with_prefix("./db")?.with_fsync(true),
    10_000,
).build();
let encrypted = EncryptedStoreBuilder::with_secret(
    LocalFileSystem::new_with_prefix("./encrypted-db")?.with_fsync(true),
    10_000,
    [7; 32], // Supply a managed secret key in an application.
).with_meta_cache_bytes(64 * 1024 * 1024).build();
# let _ = (plain, encrypted);
# Ok(())
# }
```

The reviewed service entry points and storage demo enable fsync for local writes.
In object_store 0.14.1, this synchronizes written files and affected directories
on supported platforms for put/copy/rename/multipart completion. It trades write
latency for durability. **It does not fsync standalone deletes**, and directory
fsync is platform dependent. Accordingly, neither this option nor FaultStore
proves end-to-end host-power-loss durability for every operation. Deployments
requiring durable standalone deletion need a backend that provides that guarantee.
Actual device/controller power-loss testing is separate from process/future
failure tests.

## Fault injection and verification

`FaultStore` is a test/chaos wrapper with shared `FaultHandle` controls. Budgets
count attempted mutation stages, including MultipartStart, MultipartPart,
MultipartComplete and MultipartAbort. For compatibility, Put rules also match
MultipartStart. Targeted rules can match source or destination of copy/rename.

Fault kinds include pre-operation Error/Crash, ordinary-Put-only TornWrite,
ErrorAfter (backend succeeds, response is lost), and one-shot PauseBefore/
PauseAfter gates. PauseBefore for a multipart part gates polling after the backend
has reserved its part number; eager backends may have buffered the part already.
Complete remains a separate intercepted publication event. Already admitted
requests may finish after another request triggers a simulated crash.

`mutation_log()` records admitted requests, including backend failures. Use
`event_log()` to distinguish Attempted, BackendSucceeded, BackendFailed,
ResponseFailed and Cancelled. BackendSucceeded means the backend returned Ok,
not that hardware has made a durable write. Reset controls only when requests
are quiescent.

```bash
cargo test -p anda_db_utils -p anda_object_store --all-features --locked
cargo clippy -p anda_db_utils -p anda_object_store --all-targets --all-features --locked -- -D warnings
CARGO_PROFILE_BENCH_LTO=false CARGO_PROFILE_BENCH_OPT_LEVEL=3 cargo bench -p anda_object_store --bench storage
```

The suite includes InMemory/LocalFileSystem trait conformance, legacy formats,
CAS/ABA and concurrency checks, authenticated cleanup, unknown outcomes and
cancellation, multipart retries, cache trust boundaries, size budgets, range
oracles and request/response Extensions. The benchmark harness records latency,
allocation count, cumulative allocated bytes and largest allocation. Tiny-range
cases additionally report outstanding allocator bytes while holding the result;
conditional-copy cases report backend copies and remaining target generations.
Cloud integration and real power-loss tests remain deployment-specific.

### Review-fix measurements (2026-09-23)

Compared the implementation at `308c5cd` with these changes, using the same
extended benchmark and resolved dependencies on macOS arm64, Rust 1.98.1.
Both builds used `CARGO_PROFILE_BENCH_LTO=false CARGO_PROFILE_BENCH_OPT_LEVEL=3`.
Three alternating before/after runs each used two warmups and 15 samples per
case. Times below are medians of the three per-run medians. These are local,
allocator-instrumented microbenchmarks, not cloud-throughput estimates.

| Case | Before | After |
| --- | ---: | ---: |
| One-byte range, default 256 KiB chunk, offset 100: retained heap bytes | 262,168 | 1 |
| Same range: latency (µs) | 119.38 | 119.04 |
| Conditional copy to existing target: latency (µs) | 4.46 | 0.96 |
| Conditional copy: backend copies over 17 attempts | 17 | 0 |
| Encrypted listing, 100 keys, repeated after cold listing (µs) | 180.42 | 61.29 |
| Encrypted listing, 100 keys, cold cache (µs) | 179.38 | 297.42 |
| Plain listing, 100 already-cached keys (µs) | 52.92 | 58.04 |
| Encrypted full read, 4 MiB (µs) | 2,341.92 | 2,371.33 |

The range change removes retained memory without avoiding the required full
chunk decryption. Listing cache population adds work to the first scan; repeated
scans save sidecar reads and authentication. Regression request counters verify
three metadata GETs for two listings plus a head over three cached keys, versus
seven with caching disabled. Conditional-copy rejection leaves only the original
target generation. Successful conditional copies pay one extra metadata HEAD to
avoid unnecessary payload copies when the target already exists.
