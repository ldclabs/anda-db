# AndaDB Core API Reference

Use with the dependencies and working example in [SKILL.md](../SKILL.md).
Snippets are fragments inside an async application unless they define a complete
function. Source: [database](../../../rs/anda_db/src/database.rs),
[collection](../../../rs/anda_db/src/collection/),
[query types](../../../rs/anda_db/src/query.rs), and
[index helpers](../../../rs/anda_db/src/index/mod.rs).

## Database and collection lifecycle

| API | Behavior |
| --- | --- |
| `AndaDB::connect(store, config)` | Open; create only on `NotFound` |
| `AndaDB::create(store, config)` | Fail if already present |
| `AndaDB::open(store, config)` | Fail if missing |
| `db.open_or_create_collection(schema, config, callback)` | Open/create; apply a higher schema version on a fresh open |
| `db.create_collection(schema, config, callback)` | Require a new collection |
| `db.open_collection(name: String, callback)` | Open the persisted schema |
| `db.contains_collection(name: &str)` | Check logical existence without cloning metadata |
| `db.get_open_collection(name: &str)` | Get an active cached handle without I/O; use `open_collection` on a miss |
| `db.close_collection(name: &str)` | Flush and retire the registered handle |
| `db.delete_collection(name: &str)` | Remove the collection and its storage |

Database constructors accept `Arc<dyn object_store::ObjectStore>` and return
`AndaDB`. Collection constructors return `Arc<Collection>`. The callback
receives `&mut Collection` and returns `Result<(), DBError>`.

```rust
let collection = db
    .open_or_create_collection(
        Article::schema()?,
        CollectionConfig {
            name: "articles".into(),
            description: "Searchable articles".into(),
        },
        async |c| {
            c.set_tokenizer(anda_db::index::jieba_tokenizer());
            c.set_io_concurrency(8)?;
            c.create_btree_index_nx(&["status"]).await?;
            c.create_btree_index_nx(&["slug"]).await?;
            c.create_bm25_index_nx(&["title", "body"]).await?;
            c.create_hnsw_index_nx(
                "embedding",
                HnswConfig { dimension: 384, ..Default::default() },
            ).await?;
            Ok(())
        },
    )
    .await?;
```

`Article` is defined in the [schema reference](schema_and_cbor.md). On a fresh
open, install the same tokenizer and deterministic `IndexHooks` **before** the
first query, mutation, or index creation triggers recovery. The open path
finishes recovery even if the callback does no work. Tokenizers/hooks and I/O
concurrency are runtime settings; reinstall them after restarting. Changing
tokenization or hook behavior requires rebuilding affected indexes.

An active cached collection skips the callback entirely. Use
`db.close_collection("articles").await?` then reopen for schema/index changes;
retained old handles are permanently retired. Do not recursively open, create,
close, or delete the callback's own collection name: its lifecycle lock is
not reentrant. Other collection names are allowed.

## Typed and raw CRUD

```rust
use anda_db::schema::{Document, Fv};
use std::collections::BTreeMap;

let id = collection.add_from(&article).await?;
let loaded: Article = collection.get_as(id).await?;
let raw: Document = collection.get(id).await?;

let updated: Document = collection.update(id, BTreeMap::from([
    ("status".into(), Fv::Text("published".into())),
])).await?;
let updated_article: Article = updated.try_into()?;
let removed: Option<Document> = collection.remove(id).await?;
```

- `_id: 0` is a placeholder on insertion. `add`/`add_from` always allocate a
  new id, return it, and leave the original Rust struct unchanged. IDs can
  have gaps; inserts are not upserts.
- `update` is a partial update by field **name**. An empty update or an `_id`
  update is an error. `Fv::Null` clears an optional value; it is not a
  general field-deletion operation. Missing documents produce `NotFound`.
  After normalization, an unchanged update returns the current document without
  writing an intent/document, rebuilding indexes, or incrementing mutation stats.
  Encoded-size rejection happens before any index or durable mutation.
- `remove` returns `None` for an absent id. It can also purge an indexed id
  whose stored object is missing/corrupt, returning `None` after cleanup.
- `add(document)` needs a schema-compatible `Document`. Build raw documents
  with `collection.new_document()` or
  `Document::try_from(collection.schema(), &value)?`, especially after upgrades;
  field indexes from a freshly derived schema may differ from persisted ones.
  `new_document()` initializes the internal ID placeholder; fill business fields
  and call `add` without setting `_id` yourself.
- `#[unique]` declares a constraint; the corresponding B-Tree index must
  exist for inserts and updates to enforce it.

`DBError::unique_index_conflict()` identifies a typed B-Tree duplicate-key
error and returns its logical index name. Use it at a document-write boundary
instead of classifying every `AlreadyExists` as a uniqueness conflict: storage
collisions also use that outer variant.

## B-Tree indexes and filters

Create indexes with `&mut Collection`, normally in the open callback:

```rust
c.create_btree_index_nx(&["status"]).await?;
c.create_btree_index_nx(&["tenant", "slug"]).await?;
```

Single-field indexes support `U64`, `I64`, `Text`, and `Bytes`. One `Option`
layer is allowed; null values are skipped. Homogeneous arrays index their
elements; wildcard maps index their keys. Fixed-field nested structs are not
wildcard maps, even with only one field. Bool, float, JSON, and nested struct
fields are not scalar B-Tree keys.

Multi-field indexes are **always unique**, independent of individual fields'
`#[unique]` attributes. Field order defines identity; the index name is
`virtual_field_name(&["tenant", "slug"])`, i.e. `tenant-slug`. Build the key
with the same helper the default hooks use:

```rust
use anda_db::index::{virtual_field_name, virtual_field_value};
use anda_db::query::{Filter, RangeQuery};
use anda_db::schema::Fv;

let tenant = Fv::Text("acme".into());
let slug = Fv::Text("first-post".into());
let key = virtual_field_value(&[Some(&tenant), Some(&slug)]).unwrap();
let filter = Filter::Field((
    virtual_field_name(&["tenant", "slug"]),
    RangeQuery::Eq(key),
));
```

Composite keys are concatenated deterministic CBOR bytes. Equality has tuple
semantics; byte ordering is **not** lexicographic tuple ordering. Use separate
indexes and `Filter::And` for independent range constraints.

`Filter::Field` takes the index name and `RangeQuery<Fv>`. `_id` is handled by
the built-in id bitmap; `create_btree_index_nx(&["_id"])` is rejected.

| Range condition | Construction |
| --- | --- |
| Equal / comparison | `Eq(v)`, `Gt(v)`, `Ge(v)`, `Lt(v)`, `Le(v)` |
| Inclusive range | `Between(lo, hi)` |
| Membership | `Include(vec![v1, v2])` |
| Logical ranges | `And(vec![Box::new(r1), Box::new(r2)])`, `Or(...)`, `Not(Box::new(r))` |

Filter composition uses `Filter::And(vec![Box::new(f1), Box::new(f2)])`,
`Filter::Or(...)`, and `Filter::Not(Box::new(f))`. A positive `Eq(Null)` on
an optional B-Tree field does not find skipped nulls; negation is evaluated
against the collection's live id universe.

## Text, vector, and hybrid search

```rust
use anda_db::query::{Filter, Query, RangeQuery, Search};
use anda_db::schema::Fv;

let query = Query {
    search: Some(Search {
        text: Some("agent memory".into()),
        vector: Some(vec![0.1_f32; 384]),
        ..Default::default()
    }),
    filter: Some(Filter::Field((
        "status".into(),
        RangeQuery::Eq(Fv::Text("published".into())),
    ))),
    limit: Some(20),
};
let results: Vec<Article> = collection.search_as(query).await?;
```

- `search`, `search_as::<T>`, and `search_ids` take `Query` **by value** and
  return documents, typed values, or ids respectively. They do not return
  relevance scores.
- Text search visits all BM25 indexes. Enable `logical_search: true` for
  logical operators and use `bm25_params` for BM25 tuning. A text query
  without any BM25 index returns an error.
- Vector search visits every HNSW index matching the query vector length.
  Vector-only search errors if none matches. In a mixed text/vector query,
  the current implementation can return text results if no vector index
  matches; explicitly check the intended index/dimension when vector recall
  is required.
- Rankings are fused with `RRFReranker::default()` (`k = 60`) even when the
  `reranker` field is omitted. Use `RRFReranker::new(k)` for a positive custom
  value. Raw HNSW scores are distances (smaller is closer); RRF scores are
  fused ranks, not distances.
- `Query::limit` defaults to 10, is capped at
  `Collection::MAX_SEARCH_LIMIT` (1000), and `Some(0)` means no results.
  A query with neither search nor filter returns no results; it is not a scan.

### Recall and resource options

`SearchOptions` defaults to `oversample = 10`, `max_candidates = 4096`,
`prefilter_limit = 4096`, and `adaptive = true`. Selective filters can be
scored exactly within the matching subset. Adaptive candidate expansion helps
when post-filtering leaves too few hits, up to the candidate cap.

For a query built as above, select the options API instead of `search_as`:

```rust
use anda_db::query::SearchOptions;

let documents = collection.search_with_options(query, SearchOptions {
    oversample: 20,
    max_candidates: 2048,
    prefilter_limit: 0,
    adaptive: true,
}).await?;
let results: Vec<Article> = documents.into_iter()
    .map(|doc| doc.try_into())
    .collect::<Result<_, _>>()?;
```

Use `search_ids_with_options` for ids only. There is no
`search_as_with_options` method. `prefilter_limit: 0` disables exact subset
scoring and retains global-rank-then-filter semantics; subset ranking can
change hybrid ordering. The collection clamps candidate limits to at most
4096 and at least the requested result limit.

### ID scans and pagination

| Method | Bound and ordering |
| --- | --- |
| `query_ids(filter, limit)` | Lowest matching ids, ascending; default/cap 1000 |
| `query_last_ids(filter, limit)` | Highest matching ids, still returned ascending; default/cap 1000 |
| `query_all_ids(filter)` | Every matching id, ascending; caller owns the memory bound |

For forward pagination add `_id > last_id`. For newest-first pages, combine
the application filter with `_id < cursor` and call `query_last_ids`; reverse
the returned page for descending presentation. `Some(0)` returns an empty
page. `None` is bounded, not unlimited. Use `query_all_ids` for complete
in-process maintenance such as cascades; do not expose its unbounded result
directly to untrusted requests. `ids()` materializes all ids as well.

## Index configuration and maintenance

`create_*_index` backfills existing documents before registering an index.
Unique conflicts can fail backfill. `_nx` avoids duplicate creation, but
`create_hnsw_index_nx` **errors on any persisted configuration mismatch**;
remove/recreate the index to change its settings. HNSW fields accept
`FieldType::Vector` or `Option<Vector>`. Missing/null optional vectors are
skipped; update the field later to add or clear its indexed vector.

| HNSW setting | Default |
| --- | --- |
| `dimension` | 512 |
| `max_layers` | 16 |
| `max_connections` | 32 |
| `ef_construction` / `ef_search` | 200 / 50 |
| `distance_metric` | `DistanceMetric::Euclidean` |
| `scale_factor` | `None` (effective 1.0) |
| `select_neighbors_strategy` | `SelectNeighborsStrategy::Heuristic` |
| `reconnect_on_delete` | `false` |

`anda_db_hnsw::DistanceMetric` supports Euclidean, Cosine (`1 - similarity`),
InnerProduct (negative dot product), and Manhattan. Configure dimension and
metric for the actual embeddings. `reconnect_on_delete: true` trades deletion
work for improved graph connectivity under churn; it does not guarantee recall.
See [HNSW configuration](../../../rs/anda_db_hnsw/src/config.rs) for valid bounds.

Remove indexes via `remove_btree_index(&[...])`, `remove_bm25_index(&[...])`,
or `remove_hnsw_index("embedding")`; these require `&mut Collection` and
return `bool`. `get_btree_index`, `get_bm25_index`, and `get_hnsw_index` return
read-only views; the internal `find_*_index` methods are not public. Update data through
collection CRUD so indexing, recovery journals, and lifecycle checks stay in
sync. Custom hooks must derive from the declared index fields: `update`
refreshes an index only when one of those fields changes.

`compact_btree_index(&[...])` and `compact_bm25_index(&[...])` take `&self`,
drain mutations through the exclusive operation gate, and persist their
compaction. Await them to completion.

`collection.reconcile_storage().await?` is an explicit full storage scan,
returning `(recovered, dropped)` counts. It repairs missing bitmap entries and
dead ids; persist the repairs with `db.flush().await?`. Inspect
`collection.recovery_issues()` for corrupt/schema-invalid skipped objects,
and `collection.stats()` / `storage_stats()` / `db.stats()` for diagnostics.
See [storage and recovery](storage_and_recovery.md) before handling failures.
