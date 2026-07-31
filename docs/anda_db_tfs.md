# anda_db_tfs - Full-Text Search Engine for AndaDB

`anda_db_tfs` is the embedded full-text search component of [AndaDB](../README.md). It implements the classic **Okapi BM25** ranking algorithm and is designed for long-term textual memory in AI agents. It is written in pure Rust, thread-safe, and has very few dependencies. It can be used on its own or reused directly by the upper-layer database as the `anda_db::BM25` index.

---

## 1. Design Goals

| Goal                                       | Implementation                                                                                     |
| ------------------------------------------ | -------------------------------------------------------------------------------------------------- |
| Embedded, zero external services           | Pure Rust library; no Elasticsearch or Tantivy process required                                    |
| Friendly to mixed Chinese and English text | Pluggable `Tokenizer` pipeline; built-in Porter stemmer and jieba tokenization                     |
| High-concurrency reads and writes          | `DashMap` plus atomic counters; `insert` / `remove` / `search` can run concurrently across threads |
| Incremental persistence                    | The inverted index is sharded into **buckets**; only dirty buckets are flushed                     |
| Small memory footprint                     | `UniqueVec`, `FxHashMap`, and compact CBOR encoding                                                |
| Boolean queries                            | `AND / OR / NOT` syntax with parentheses for agent retrieval                                       |

---

## 2. Algorithm: Okapi BM25 at a Glance

For a multi-term query `q`, the score of document `d` is:

$$
	ext{score}(d, q) \;=\; \sum_{t \in q} \text{idf}(t) \cdot \frac{tf_{t,d}\,(k_1 + 1)}{tf_{t,d} + k_1\!\left(1 - b + b \cdot \dfrac{|d|}{\text{avgdl}}\right)}
$$

- $tf_{t,d}$: the number of times term $t$ appears in document $d$.
- $|d|$: the total number of tokens in document $d$; $\text{avgdl}$ is the average document length across the corpus.
- $\text{idf}(t) = \ln\!\left(1 + \dfrac{N - df_t + 0.5}{df_t + 0.5}\right)$, the classic Okapi IDF smoothing formula.
- The hyperparameters $k_1$ and $b$ are configured through `BM25Params`, with defaults `k1=1.2` and `b=0.75`.

Before scoring, the library defensively clamps user-provided parameters: `k1` has a lower bound of `0.0`, and `b` is clamped to `[0, 1]` to avoid generating `NaN` or `inf`.

---

## 3. In-Memory Data Structures

```text
BM25Index
├── doc_tokens       DashMap<doc_id, token_count>           // document length table
├── postings         DashMap<token, (bucket_id, UniqueVec<(doc_id, tf)>)>  // inverted index
├── buckets          DashMap<bucket_id, Bucket>             // shard metadata
├── metadata         RwLock<BM25Metadata>                   // name / config / stats
└── atomic counters  max_bucket_id, max_document_id, total_tokens, ...
```

- **Posting `(bucket_id, UniqueVec<(doc_id, tf)>)`**: `bucket_id` identifies the bucket currently owning the token; `UniqueVec` guarantees uniqueness for `(doc_id, tf)` and supports constant-time deletion through `swap_remove_if`.
- **Bucket**: a serializable unit containing a group of tokens and the `doc_ids` they cover. A bucket tracks dirty state with dual version counters:

  | Field           | Meaning                                           |
  | --------------- | ------------------------------------------------- |
  | `dirty_version` | Incremented on every modification                 |
  | `saved_version` | Version last persisted successfully               |
  | `size`          | Approximate byte size under current CBOR encoding |
  | `tokens`        | The list of terms owned by this bucket            |
  | `doc_ids`       | The set of document IDs seen in this bucket       |

`is_dirty()` is true if and only if `dirty_version > saved_version`, which gives `flush` an **idempotent and linearly incremental** behavior: a successfully written bucket only records the `dirty_version` observed at write time. If it is modified again concurrently, it remains dirty and will be written again on the next flush.

---

## 4. Bucket Sharding Strategy

`BM25Config.bucket_overload_size` is the **soft upper limit** for the serialized size of a single bucket, defaulting to `512 KiB`. For each posting that needs to be added to a bucket:

1. If the bucket already owns the token, only `size` is increased.
2. If the bucket is empty, or the bucket remains `< limit` after adding the token, the token is appended.
3. Otherwise, **migration** is triggered:
   - Increment `max_bucket_id` to obtain `next_bucket_id`.
   - Remove the token from the old bucket and update `bucket_id = next_bucket_id` in `postings`.
   - If the next bucket is still too full, continue advancing until placement succeeds.

Note: a token that is **already registered in a bucket** is not migrated again on the next `insert`. This is a key invariant that avoids the degenerate case where every insertion creates a new bucket. The regression test `test_no_excessive_small_buckets` covers this behavior.

### 4.1 Defragmentation: `compact_buckets()`

For fragmented indexes left over from earlier versions, or buckets hollowed out by long-running `remove` operations, `compact_buckets()` can repack everything in one pass:

- Scan all `postings` and estimate each token's CBOR size.
- Use the **Best-Fit-Decreasing** algorithm, with `BTreeMap<remaining_capacity, bin_index>` for `O(n log n)` packing.
- Rebuild `buckets`: bucket IDs are reassigned from `0` upward, all buckets are marked dirty, and the next `flush` rewrites the full on-disk layout.

The return value `(old_count, new_count)` is useful for monitoring. Concurrent `insert` / `remove` calls are safe: `compact_buckets` takes the index's internal mutation gate exclusively, so no mutation can observe — or add to — the half-rebuilt bucket map. It must still not overlap a `flush` (see [§5](#5-concurrency-model)).

---

## 5. Concurrency Model

- All shared state is stored in `DashMap`, `RwLock`, and atomic counters, so **concurrent `insert` / `remove` / `search` can freely overlap across threads** without an outer lock.
- In `insert` and `remove`, the critical regions involving bucket sizing and splitting use fine-grained entry locks via `DashMap::entry().or_default()`, avoiding holding a lock across `.await`.
- Average document length is **derived, never cached**. There is no `avg_doc_tokens` field: the value is computed as `total_tokens / doc_tokens.len()` at its two read sites — `score_term` (once per query, not per document) and `refresh_live_stats` (once per `stats()` call). A cached quotient had to be resynchronized on every `insert` / `remove`, disagreed with its own inputs in between, and was wrong outright after a `load_metadata` that had not yet loaded any documents. Deriving it makes the reported value exactly consistent with the counters it comes from — there is no convergence window.
- **`compact_buckets` is exclusive with mutations, and the crate enforces that.** `insert` / `insert_array` / `remove` / `remove_array` / `purge_ids` take an internal `mutation_gate` **shared** (so they still run concurrently with each other) and `compact_buckets` takes it **exclusively**, because it rebuilds the bucket map non-atomically: a posting created after compaction snapshotted `postings` would otherwise be re-binned into nothing and silently dropped by the next flush. The gate is the first lock a mutation acquires, so it never nests inside a `DashMap` shard guard. Callers do **not** need to serialize compaction against writes.
- **Coordinating `flush` / `flush_with` against mutations, against compaction, and against another flush is the caller's responsibility** (`anda_db`'s `Collection` holds an exclusive operation gate across every flush). A single writer per durable index is a deployment contract; the crate does not defend against a second writer.
- `flush` serializes every dirty bucket and the metadata into owned buffers before the first `.await`. It never holds a `DashMap` `Ref` across `.await`, which avoids deadlocks.
- `top_k_results` uses `select_nth_unstable_by` for partial sorting (`O(n + k log k)`), then performs a final `sort` on the top-k tail, making queries significantly faster on large result sets.

---

## 6. Persistence Layout

Physically, one index consists of **one metadata blob** plus **multiple bucket blobs**. The upper layer is free to store them in files, object storage, a KV store, or any other backend.

```text
<root>/
  metadata                # CBOR-encoded BM25Metadata (carries the bucket manifest)
  buckets/
    b_0_7                 # CBOR-encoded BucketOwned, addressed by (bucket_id, generation)
    b_1_9
    b_2                   # legacy (pre-manifest) object at generation 0, read-only
    ...
```

Bucket objects are **immutable once referenced**: every flush writes replaced
buckets to a fresh `(bucket_id, generation)` object and the metadata's
manifest is the loader's single source of truth.

### 6.1 `BM25Metadata`

```rust
pub struct BM25Metadata {
    pub name: String,
    pub config: BM25Config,
    pub stats: BM25Stats,          // version / counts / timestamps / watermarks
    /// Bucket manifest: bucket_id -> generation of the current durable object
    /// (0 = legacy un-suffixed object). Empty when loading pre-manifest data.
    pub buckets: BTreeMap<u32, u64>,
}
```

`flush` uses `last_saved_version` as an idempotency guard: when neither `stats.version` has increased nor any bucket is dirty, it returns immediately with `saved == false`.

### 6.2 Bucket CBOR

```rust
struct BucketOwned {
    #[serde(rename = "p")] postings:  FxHashMap<String, PostingValue>,
    #[serde(rename = "d")] doc_tokens: FxHashMap<u64,    usize>,
}
```

The short field names (`"p"`, `"d"`) are chosen to reduce CBOR size. Note that `PostingValue` embeds `bucket_id`, so loading does not require any extra bookkeeping.

### 6.3 Incremental Flush Flow

```rust
let outcome = index.flush(metadata_writer, now_ms, |object, bytes| {
    // write bytes to the storage object for (object.bucket_id, object.generation)
    std::future::ready(Ok(()))
}).await?;
for object in &outcome.obsolete {
    // best-effort: delete the objects the new manifest no longer references
}
```

- `flush` first serializes every dirty bucket (only postings whose current owner is that bucket are written; the serialized `doc_tokens` table is derived from those postings, so stale bucket-side document IDs are not re-persisted).
- Each dirty bucket is written to a **new** object keyed by `(bucket_id, generation)`; the generation is this flush's metadata version, so committed objects are never mutated in place.
- The metadata — whose manifest maps every live bucket id to its current generation — is written **last**. That single write is the atomic commit point: a crash or error before it leaves the previous snapshot fully intact (the new objects are unreferenced garbage); after it, the replaced objects are garbage and are returned in `FlushOutcome::obsolete` for best-effort deletion.
- `compact_buckets` needs no special ordering: the repacked layout becomes visible atomically with the next manifest commit, and every pre-compaction object is reported obsolete.

### 6.4 Startup and Partial Loading

```rust
let idx = BM25Index::load_all(tokenizer, metadata_reader, async |object| {
    Ok(read_bucket(object).await?) // Ok(None) means this bucket is not loaded
}).await?;
```

- `load_metadata` restores metadata only, which is useful for lightweight scenarios that need just statistics.
- With a manifest present, `load_buckets` reads exactly the referenced `(bucket_id, generation)` objects. Metadata persisted by pre-manifest releases has no manifest; the loader falls back to scanning bucket ids `0..=max_bucket_id` at generation `0` (the legacy un-suffixed objects), and the first flush upgrades the durable layout to the manifest format.
- `load_buckets` can skip buckets (read-only partial loads) when the closure returns `Ok(None)`. During search, `score_term` automatically ignores documents that were not loaded. A partially loaded index must not be flushed: a flush persists exactly the loaded content.
- If the same token appears in more than one loaded bucket (possible only in legacy data written by the old multi-phase flush), the later bucket id wins. The loader removes that token from the older bucket, rebuilds bucket document-id sets from the winning postings, and marks repaired buckets dirty so the next flush removes stale on-disk ownership.

---

## 7. Query Language

`search_advanced(query, top_k, params)` accepts boolean expressions, and `QueryType::parse` turns the input string into an AST:

```text
expr     := or_expr
or_expr  := and_expr ( " OR " and_expr )*
and_expr := not_expr ( " AND " not_expr )*
not_expr := "NOT " term | term
term     := "(" or_expr ")" | word ( whitespace word )*
```

Precedence is `OR < AND < NOT`. Key properties:

- **Multi-term queries default to OR**: `"quick fox"` and `"quick OR fox"` return the same results in `search` and `search_advanced`.
- **Score merging**: `AND` sums the BM25 scores of its subqueries; `OR` does the same; `NOT` produces a zero-scored placeholder set used only for filtering, and in an `AND` context it **removes** matching items from the result set.
- **Robust parsing**: unbalanced parentheses do not panic. They are treated as ordinary characters, which makes direct forwarding of user input safe.
- **Multi-byte safe**: the delimiters `" AND "` and `" OR "` are ASCII, so byte-wise scanning remains safe under UTF-8. Mixed CJK text does not require extra handling.

Example:

```rust
let hits = index.search_advanced(
    "(brown AND fox) AND NOT (rare OR sleeps)",
    10,
    None,
);
```

---

## 8. Tokenizers

All tokenizers implement `tantivy_tokenizer_api::Tokenizer` and are composed through `TokenizerChain`. Important APIs:

| Function                                                      | Role                                                                                                |
| ------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `TokenizerChain::builder(base).filter(f1).filter(f2).build()` | Compose a tokenization pipeline                                                                     |
| `default_tokenizer()`                                         | `SimpleTokenizer -> RemoveLongFilter(32) -> LowerCaser -> Stemmer` (requires the `tantivy` feature) |
| `jieba_tokenizer()`                                           | Prepends `JiebaMergeFilter` to the pipeline above (requires the `tantivy-jieba` feature)            |
| `collect_tokens(tok, text, inclusive)`                        | Standalone tokenization helper for upper-layer preprocessing or non-BM25 matching statistics        |
| `flat_full_text_search(tok, query, text)`                     | Naive matching without building an index, useful for short passages                                 |
| `detect_script(text)`                                         | Detects the dominant script by character frequency (`Latin / Cyrillic / Arabic / Cjk / Other`)      |

### 8.1 `JiebaMergeFilter`

In mixed-script scenarios such as Chinese, English, Russian, and Arabic text together, a plain `SimpleTokenizer` will treat consecutive Chinese characters as a single token. `JiebaMergeFilter` re-segments tokens where `detect_script == Cjk` with jieba, merges offsets and `position`, and finally sorts by `(offset_from, offset_to, position, text)`, guaranteeing that:

- Chinese text is segmented correctly (`"北京市东城区长安街"` -> `北京`, `东城区`, `长安街`);
- English, Russian, Arabic, and other scripts retain the stemmed and lowercased output of the primary pipeline;
- The resulting `TokenStream` is still monotonic, so downstream BM25 consumption remains correct.

> **Note**: `collect_tokens` filters out single-byte tokens where `token.text.len() <= 1` (length measured in bytes), which removes punctuation and isolated ASCII letters. Single Chinese characters are unaffected because their UTF-8 length is at least 3 bytes.

---

## 9. Error Handling

```rust
pub enum BM25Error {
    Generic       { name: String, source: BoxError },
    Serialization { name: String, source: BoxError },
    NotFound      { name: String, id: u64 },
    AlreadyExists { name: String, id: u64 },
    TokenizeFailed{ name: String, id: u64, text: String },
}
```

`Generic` is used for errors returned by I/O closures; `Serialization` wraps `cbor2` failures; `AlreadyExists` and `TokenizeFailed` occur during `insert`; `NotFound` is left to upper-layer APIs for idempotent validation.

---

## 10. Configuration and Tuning

```rust
let cfg = BM25Config {
    bm25: BM25Params { k1: 1.5, b: 0.6 },
    bucket_overload_size: 1024 * 1024,     // 1 MiB
};
let index = BM25Index::new("mem".into(), jieba_tokenizer(), Some(cfg));
```

Tuning guidance:

- **`k1` in [1.2, 2.0]**: for shorter documents with denser keywords, increasing it slightly can amplify the effect of high-frequency terms.
- **`b` in [0.5, 0.9]**: choose `0.75` to `0.9` when document lengths vary significantly; drop closer to `0.5` when most documents are similar in length.
- **`bucket_overload_size`**:
  - Small (for example `64KiB`): lower I/O amplification for incremental flushes, suitable for frequent checkpoints.
  - Large (for example `2MiB`): fewer total buckets and faster full loads, suitable for read-heavy AI memory stores.
- **Periodic compaction**: call `compact_buckets()` periodically in a background task to keep bucket counts stable. It is safe against concurrent `insert` / `remove` (the crate gates it internally); schedule it so it does not overlap a `flush`.

---

## 11. Testing and Benchmarks

Test coverage includes:

- `cargo test -p anda_db_tfs --features full --lib` (31 unit tests)
- Correctness of insert / remove / search, bucket serialization and partial loading, result invariance after compaction, the regression test `test_no_excessive_small_buckets`, UTF-8 query parsing, and more.

Benchmark command: `cargo bench -p anda_db_tfs --features full --bench tfs_tokenizer`.

---

## 12. Quick Start

```rust
use anda_db_tfs::{BM25Index, default_tokenizer};

let idx = BM25Index::new("notes".into(), default_tokenizer(), None);
idx.insert(1, "The quick brown fox jumps over the lazy dog", 0).unwrap();
idx.insert(2, "A fast brown fox runs past the lazy dog",     0).unwrap();
idx.insert(3, "The lazy dog sleeps all day",                 0).unwrap();

for (id, score) in idx.search("fox", 10, None) {
    println!("doc {id}: {score:.3}");
}

for (id, score) in idx.search_advanced("(brown AND fox) AND NOT sleeps", 10, None) {
    println!("doc {id}: {score:.3}");
}
```

Persisting to the local filesystem:

```rust
use std::{fs, io::Write};

let metadata = fs::File::create("./idx/metadata.cbor")?;
let outcome = idx.flush(metadata, now_ms, |object, bytes| {
    let write = || {
        let mut f = fs::File::create(format!(
            "./idx/b_{}_{}.cbor", object.bucket_id, object.generation
        ))?;
        f.write_all(&bytes)?;
        Ok(())
    };
    std::future::ready(write())
}).await?;
for object in &outcome.obsolete {
    let _ = fs::remove_file(format!(
        "./idx/b_{}_{}.cbor", object.bucket_id, object.generation
    ));
}
```

Loading:

```rust
use std::{fs, io::Read};

let metadata = fs::File::open("./idx/metadata.cbor")?;
let idx = BM25Index::load_all(default_tokenizer(), metadata, async |object| {
    // generation 0 denotes a legacy (pre-manifest) `b_{id}.cbor` object.
    let path = if object.generation == 0 {
        format!("./idx/b_{}.cbor", object.bucket_id)
    } else {
        format!("./idx/b_{}_{}.cbor", object.bucket_id, object.generation)
    };
    match fs::File::open(path) {
        Ok(mut f) => { let mut buf = Vec::new(); f.read_to_end(&mut buf)?; Ok(Some(buf)) }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}).await?;
```

---

## 13. Usage Notes

1. **Removal requires the original text**: `remove(id, text, now_ms)` relies on re-tokenizing the original text to locate postings. Historical misuse does not affect search correctness, but it may leave redundant postings that can be cleaned up with `compact_buckets()`. When the text is genuinely unrecoverable — a repair path whose document bodies are gone — use `purge_ids(&BTreeSet<u64>, now_ms)` instead: it sweeps every posting list once for the whole set, drops the ids from `doc_tokens` and `total_tokens`, and marks the affected buckets dirty. It is a maintenance-path `O(index size)` operation, so pass all the dead ids in one call rather than looping.
2. **`top_k = 0`**: kept for API compatibility. It returns an empty set and does not trigger sorting.
3. **Flush coordination**: the crate does not serialize flushes internally. The caller must ensure a flush never overlaps mutations, compaction, or another flush (`anda_db`'s `Collection` already guarantees this); a single writer per durable index is a deployment contract.
4. **Search semantics under partial loading**: if `load_buckets` skips a posting bucket, terms owned by that bucket are unavailable. Loaded buckets also carry the document lengths needed to score their postings, so `len()` may include every document touched by those loaded terms even when other buckets are skipped. Search results remain the natural subset of the loaded postings.
5. **Embedded-only**: this library is intended for in-process embedding inside AndaDB and does not provide HTTP or gRPC services. For remote access, use `anda_db_server` or `anda_db_shard_proxy`.

---

## 14. References

- Robertson & Zaragoza. *The Probabilistic Relevance Framework: BM25 and Beyond*, 2009.
- [`tantivy_tokenizer_api`](https://docs.rs/tantivy-tokenizer-api) - tokenizer trait.
- For regression cases and design discussion, see the integration tests at the end of `rs/anda_db_tfs/src/bm25.rs`, and [anda_db_btree.md](anda_db_btree.md) for the related bucket strategy.
