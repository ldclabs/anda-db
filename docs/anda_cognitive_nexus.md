# `anda_cognitive_nexus` — Technical Reference

> The reference **Knowledge Interaction Protocol (KIP)** executor — a
> persistent knowledge graph that turns Anda DB into the long-term
> *memory brain* of an AI agent.

|                       |                                                                                                                                                       |
| :-------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------- |
| Crate                 | [`anda_cognitive_nexus`](../rs/anda_cognitive_nexus/)                                                                                                 |
| Version               | `0.7.x`                                                                                                                                               |
| Implements            | KIP **v1.0-RC6** [`Executor`](../rs/anda_kip/src/executor.rs) ([SPECIFICATION.md](../rs/anda_kip/SPECIFICATION.md))                                   |
| Storage backend       | [Anda DB](../rs/anda_db/) — embedded document store with B-Tree + BM25 + HNSW indexes                                                                 |
| Other implementations | [`anda_cognitive_nexus_server`](../rs/anda_cognitive_nexus_server/) (HTTP/JSON-RPC), [`anda_cognitive_nexus_py`](../py/anda_cognitive_nexus_py/) (Py) |
| Status                | KIP v1.0-RC6 conformant: KQL/KML/META full support, protected-concept enforcement (`KIP_3004`), transitive `DELETE CONCEPT` cascade, 43 unit tests.   |

---

## Contents

- [`anda_cognitive_nexus` — Technical Reference](#anda_cognitive_nexus--technical-reference)
  - [Contents](#contents)
  - [1. Overview](#1-overview)
    - [1.1 Role in the Anda stack](#11-role-in-the-anda-stack)
    - [1.2 What this crate provides](#12-what-this-crate-provides)
    - [1.3 Quick start](#13-quick-start)
  - [2. Crate layout](#2-crate-layout)
  - [3. Storage architecture](#3-storage-architecture)
    - [3.1 Collections](#31-collections)
    - [3.2 Indexes](#32-indexes)
    - [3.3 Virtual composite fields](#33-virtual-composite-fields)
    - [3.4 Full-text search and the jieba tokenizer](#34-full-text-search-and-the-jieba-tokenizer)
  - [4. Entity model](#4-entity-model)
    - [4.1 `Concept`](#41-concept)
    - [4.2 `Proposition`](#42-proposition)
    - [4.3 `Properties` and the `a` / `m` field renaming](#43-properties-and-the-a--m-field-renaming)
    - [4.4 `EntityID` encoding](#44-entityid-encoding)
    - [4.5 Higher-order propositions](#45-higher-order-propositions)
  - [5. Bootstrap and Genesis capsules](#5-bootstrap-and-genesis-capsules)
    - [5.1 `CognitiveNexus::connect`](#51-cognitivenexusconnect)
    - [5.2 `capsule_version` schema migration](#52-capsule_version-schema-migration)
    - [5.3 `$self` / `$system` are not auto-created](#53-self--system-are-not-auto-created)
  - [6. Concurrency model](#6-concurrency-model)
  - [7. Per-query execution context](#7-per-query-execution-context)
  - [8. Executing KQL](#8-executing-kql)
    - [8.1 Pipeline overview](#81-pipeline-overview)
    - [8.2 WHERE clause executors](#82-where-clause-executors)
    - [8.3 Multi-hop matching](#83-multi-hop-matching)
    - [8.4 Filter semantics](#84-filter-semantics)
    - [8.5 FIND clause and grouped aggregation](#85-find-clause-and-grouped-aggregation)
  - [9. Executing KML](#9-executing-kml)
    - [9.1 `UPSERT`](#91-upsert)
    - [9.2 `DELETE ATTRIBUTES` / `DELETE METADATA`](#92-delete-attributes--delete-metadata)
    - [9.3 `DELETE PROPOSITIONS`](#93-delete-propositions)
    - [9.4 `DELETE CONCEPT` and the protected scope](#94-delete-concept-and-the-protected-scope)
    - [9.5 Dry-run semantics](#95-dry-run-semantics)
  - [10. Executing META](#10-executing-meta)
  - [11. Performance notes](#11-performance-notes)
  - [12. Operational caveats](#12-operational-caveats)
  - [13. Testing](#13-testing)
  - [14. Compatibility](#14-compatibility)

---

## 1. Overview

### 1.1 Role in the Anda stack

[`anda_kip`](../rs/anda_kip/) defines the **protocol** (parser, AST,
`Executor` trait, request/response envelope). `anda_cognitive_nexus`
is the **reference backend** that fulfils that protocol on top of
[Anda DB](../rs/anda_db/), turning a generic embedded document store
into a domain-aware knowledge graph that an LLM agent can query and
mutate through KQL/KML/META instructions.

```text
┌──────────────────────────────────────┐
│ LLM agent / function-calling caller  │
└──────────────────┬───────────────────┘
                   │ KIP commands (JSON envelope)
                   ▼
┌──────────────────────────────────────┐
│        anda_kip (parser, AST)        │
└──────────────────┬───────────────────┘
                   │ Command (Kql | Kml | Meta)
                   ▼
┌──────────────────────────────────────┐
│  anda_cognitive_nexus (this crate)   │
│  CognitiveNexus : impl Executor      │
└──────────────────┬───────────────────┘
                   │ Collection ops (BTree / BM25 / docs)
                   ▼
┌──────────────────────────────────────┐
│              Anda DB                 │
└──────────────────────────────────────┘
```

### 1.2 What this crate provides

- [`CognitiveNexus`](../rs/anda_cognitive_nexus/src/db.rs) — a clonable
  handle that owns the `concepts` and `propositions` collections plus
  the KML lock.
- An `impl Executor for CognitiveNexus` covering **all** of KIP
  v1.0-RC6:
  - **KQL** with multi-hop graph traversal, optional zero-hop
    (`{0,n}`), filters with `IN`/`IS_NULL`/`IS_NOT_NULL`, `OPTIONAL` /
    `NOT` / `UNION` scopes, implicit `GROUP BY`, regex caching, and
    cursor-based pagination.
  - **KML** `UPSERT` (concept blocks, proposition blocks, handle
    references, default `metadata.source`) and `DELETE` of
    `ATTRIBUTES` / `METADATA` / `PROPOSITIONS` / `CONCEPT` with
    protected-concept enforcement and a transitive cascade for
    higher-order propositions.
  - **META** `DESCRIBE PRIMER/DOMAINS/CONCEPT TYPE[S]/PROPOSITION
    TYPE[S]` and BM25-backed `SEARCH CONCEPT/PROPOSITION`.
- A small set of public helpers (`has_concept`, `get_concept`,
  `get_or_init_concept`, `capsule_version`) that are useful for
  bootstrapping, migrations and direct integrations.
- Re-exported entity types (`Concept`, `Proposition`, `Properties`,
  `EntityID`) and execution scaffolding (`ConceptPK`, `PropositionPK`,
  `EntityPK`, `QueryContext`, `TargetEntities`).

### 1.3 Quick start

```toml
[dependencies]
anda_cognitive_nexus = "0.7"
anda_db = "0.7"
anda_kip = "0.7"
object_store = "0.11"
tokio = { version = "1", features = ["full"] }
```

```rust,ignore
use std::sync::Arc;
use anda_cognitive_nexus::{CognitiveNexus, db_to_kip_error};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{parse_kml, parse_kql, Executor, Command};
use object_store::memory::InMemory;

#[tokio::main]
async fn main() -> Result<(), anda_kip::KipError> {
    // 1. Open Anda DB (in-memory for tests; LocalFileSystem / S3 in production).
    let db = AndaDB::connect(Arc::new(InMemory::new()), DBConfig::default())
        .await
        .map_err(db_to_kip_error)?;

    // 2. Bootstrap the cognitive nexus. The closure runs after the
    //    bundled Genesis capsules have been applied; use it to seed
    //    application-specific concept types.
    let nexus = CognitiveNexus::connect(Arc::new(db), async |_n| Ok(())).await?;

    // 3. Add some knowledge.
    let kml = parse_kml(r#"
        UPSERT {
            CONCEPT ?drug { {type: "$ConceptType", name: "Drug"} }
            CONCEPT ?aspirin {
                {type: "Drug", name: "Aspirin"}
                SET ATTRIBUTES { dose_mg: 100 }
            }
            PROPOSITION ?p {
                (?aspirin, "is_a", ?drug)
            }
        }
    "#)?;
    nexus.execute(Command::Kml(kml), false).await;

    // 4. Query it back.
    let kql = parse_kql(r#"
        FIND(?d.name, ?d.attributes.dose_mg)
        WHERE { ?d {type: "Drug"} }
    "#)?;
    let response = nexus.execute(Command::Kql(kql), false).await;
    println!("{response:#?}");

    Ok(())
}
```

---

## 2. Crate layout

```
rs/anda_cognitive_nexus/
├── Cargo.toml
├── README.md
├── examples/
│   └── kip_demo.rs            # End-to-end demo (LocalFileSystem store)
└── src/
    ├── lib.rs                 # Crate root, re-exports
    ├── db.rs                  # CognitiveNexus + Executor impl (the core)
    ├── entity.rs              # Concept, Proposition, Properties, EntityID
    ├── helper.rs              # field extraction, ORDER BY, predicate match
    └── types.rs               # ConceptPK / PropositionPK / EntityPK,
                               # QueryContext, QueryCache, TargetEntities,
                               # PropositionsMatchResult, GraphPath
```

| Module      | Responsibility                                                                                                                                                                                                                                                                                                                                            |
| :---------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `db.rs`     | The `CognitiveNexus` struct, `Executor` impl, all `execute_*` routines, WHERE-clause sub-executors, UPSERT/DELETE pipelines, META command pipeline, helper queries (`query_concept_id`, `find_propositions`, …).                                                                                                                                          |
| `entity.rs` | Persisted graph data model. `Concept` carries `{type, name, attributes, metadata}`; `Proposition` carries `{subject, object, predicates, properties}`; `Properties` is a compact `{a, m}` shape (renamed via serde) shared by per-predicate attribute/metadata storage; `EntityID` is the canonical reference encoding (`C:{id}` / `P:{id}:{predicate}`). |
| `helper.rs` | Pure helpers: extract a field value from a `Concept` / `Proposition`, sort result rows by `ORDER BY`, match a predicate descriptor against a stored proposition row, map `DBError` → `KipError`, and a `FilterExpressionExt` trait that distinguishes selective vs. open filter shapes for query planning.                                                |
| `types.rs`  | Execution scaffolding. `ConceptPK` / `PropositionPK` / `EntityPK` are typed primary-key shapes; `QueryContext` carries variable bindings and the per-query cache; `QueryCache` deduplicates row loads inside one execution; `TargetEntities` is the result of resolving the `target` clause of a `DELETE`; `GraphPath` records multi-hop traversals.      |
| `lib.rs`    | Re-exports the public API of the modules above and contains the crate-level docs.                                                                                                                                                                                                                                                                         |

---

## 3. Storage architecture

### 3.1 Collections

`CognitiveNexus::connect` creates (or opens) two Anda DB collections:

| Collection     | Document type | Notes                                                                                                                                                                                                                                       |
| :------------- | :------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `concepts`     | `Concept`     | One row per concept node. `_id` is auto-assigned. `(type, name)` is logically unique and is enforced via the composite virtual-field BTree index.                                                                                           |
| `propositions` | `Proposition` | One row per `(subject, object)` pair. **All predicates linking the same pair are collapsed into the same row**, in the `predicates: BTreeMap<...>` field, so the link space stays compact and a single index lookup yields every predicate. |

### 3.2 Indexes

`concepts`:

- BTree composite (virtual): `["type", "name"]` — the canonical concept lookup.
- BTree singleton: `["type"]` — used for `DESCRIBE CONCEPT TYPES` and bulk-by-type scans.
- BTree singleton: `["name"]` — used for handle-only resolution paths.
- BM25 composite: `["name", "attributes", "metadata"]` — backs `SEARCH CONCEPT`.

`propositions`:

- BTree composite (virtual): `["subject", "object"]` — exact triple lookup.
- BTree singleton: `["subject"]` and `["object"]` — fan-out from a node.
- BTree singleton: `["predicates"]` — keyed by the *map key*, used to enumerate all rows that mention a given predicate (NOT-clauses, `DESCRIBE PROPOSITION TYPE[S]`).
- BM25 composite: `["properties"]` — backs `SEARCH PROPOSITION`.

### 3.3 Virtual composite fields

Anda DB lets a BTree index be declared over a *virtual* concatenation of
real fields. The crate uses
[`virtual_field_name(&["type", "name"])`](../rs/anda_db/src/index/btree.rs)
to build the lookup key
`virtual_field_value(&[Fv::Text(ty), Fv::Text(name)])`. The same pattern is
applied to `(subject, object)` for proposition rows. This keeps the index
fan-out tight while preserving both single-field (e.g. *all concepts of
type X*) and exact-tuple (e.g. *concept named Y of type X*) access paths.

### 3.4 Full-text search and the jieba tokenizer

After each collection is opened, `set_tokenizer(jieba_tokenizer())` is
applied. `anda_db_tfs::jieba_tokenizer` produces a CJK-aware tokenizer
that gracefully handles mixed-language strings, which matters because
KIP encourages mixing English type names (e.g. `"Drug"`) with
domain-localised concept names (e.g. `"阿司匹林"`).

---

## 4. Entity model

### 4.1 `Concept`

```rust,ignore
pub struct Concept {
    pub _id: u64,
    pub r#type: String,
    pub name: String,
    pub attributes: Map<String, Json>,
    pub metadata: Map<String, Json>,
}
```

Domain semantics live in `attributes`; provenance / freshness lives in
`metadata`. Both are open-shaped (`serde_json::Map`) so application
schemas are not constrained by the storage layer. KIP's `DESCRIBE
CONCEPT TYPE` projects each instance through its declaring
`$ConceptType` definition — see §10.

### 4.2 `Proposition`

```rust,ignore
pub struct Proposition {
    pub _id: u64,
    pub subject: EntityID,                        // "C:42"  | "P:7:authored"
    pub object:  EntityID,
    pub predicates: BTreeMap<String, Properties>, // predicate -> {a, m}
    pub properties: Properties,                   // row-level fallback / search target
}
```

Storing all predicates between the same `(subject, object)` pair in one
row means: (a) `KIP_3001` "duplicate proposition" amounts to a key
already present in `predicates`; (b) the BM25 index on the row's
`properties` field is enough to make every predicate searchable; and
(c) `DELETE PROPOSITIONS` decides whether to drop the predicate key,
update `properties`, or remove the entire row based on whether the
remaining predicate map is empty.

### 4.3 `Properties` and the `a` / `m` field renaming

```rust,ignore
pub struct Properties {
    #[serde(rename = "a", default, skip_serializing_if = "Map::is_empty")]
    pub attributes: Map<String, Json>,
    #[serde(rename = "m", default, skip_serializing_if = "Map::is_empty")]
    pub metadata:   Map<String, Json>,
}
```

The renamings keep storage compact for sparsely-populated propositions
(common in long tail facts) — most rows serialise to two-letter keys.

### 4.4 `EntityID` encoding

```rust,ignore
pub enum EntityID {
    Concept(u64),                  // serialises to "C:{id}"
    Proposition(u64, String),      // serialises to "P:{id}:{predicate}"
}
```

A proposition's `subject` / `object` columns store one of these strings
verbatim. Multi-hop and cascade-delete BFS traversals rely on the
fact that the encoding is reversible (`EntityID::from_str`).

### 4.5 Higher-order propositions

KIP allows `subject` or `object` to *be* another proposition — this is
how an agent records *meta-claims* like "Alice **believes** that aspirin
treats headaches". In the storage layer this is a row whose `subject` is
`P:{id}:{predicate}`. Every cascade-delete path in `db.rs` enumerates
these higher-order references using a BFS over `subject` / `object`
indexes seeded with the canonical `EntityID::to_string()` of the row
being removed, so deleting a base proposition transitively removes any
proposition that referenced it.

> **KML caveat.** When you author a higher-order `UPSERT`, the nested
> subject/object **must** be written with a literal `{type, name}` form,
> not a `?handle`. The internal `PropositionPK::try_from` rejects
> `Variable("…")` in nested position; this is enforced by tests.

---

## 5. Bootstrap and Genesis capsules

### 5.1 `CognitiveNexus::connect`

```rust,ignore
let nexus = CognitiveNexus::connect(db, async |_n| Ok(())).await?;
```

`connect` performs the following steps **idempotently** — re-running
against an existing database is safe:

1. Create / open the `concepts` and `propositions` collections with the
   schemas described in §3.1.
2. Register the BTree and BM25 indexes from §3.2 and install the jieba
   tokenizer.
3. Ensure the meta-type `$ConceptType` exists (it is its own definition).
4. Apply the bundled Genesis capsules, in order:
   `GENESIS_KIP` → `PERSON_KIP` → `PREFERENCE_KIP` → `EVENT_KIP` →
   `SLEEP_TASK_KIP` → `INSIGHT_KIP`. Each capsule is a KML `UPSERT`
   payload that defines a built-in `$ConceptType` /
   `$PropositionType` family and any required `Domain` anchors.
5. Call the user-supplied initialiser closure (typically used to seed
   application-specific concept types or to register `$self` /
   `$system`).
6. Persist the new `capsule_version` so subsequent runs can skip the
   capsule replay if there is nothing newer to apply.

### 5.2 `capsule_version` schema migration

`capsule_version()` reads the value stored as a collection extension on
`concepts`. A return of `0` means "no version recorded yet". The
bundled Genesis capsules currently target version `1`; when the crate
ships a new RC of the spec the version is bumped and `connect`
re-applies only the new capsules. Application code can also call
`save_capsule_version(n)` to gate its own migrations.

### 5.3 `$self` / `$system` are not auto-created

The Genesis capsule set defines the `Person` *type* but does **not**
create the canonical `$self` and `$system` instances — those are the
agent's identity tuples and the host application is responsible for
inserting them, typically inside the `connect` initialiser closure or
right after it returns. The `anda_kip` crate exposes `PERSON_SELF_KIP`
and `PERSON_SYSTEM_KIP` capsule strings ready to feed into `parse_kml`.

---

## 6. Concurrency model

`CognitiveNexus` is `Clone` (cheap, all internal state is `Arc`) and
fully `Send + Sync`. Concurrency is regulated by a single
`tokio::sync::RwLock<()>` (`kml_lock`):

| Command kind | Lock acquired | Effect                                                                                               |
| :----------- | :------------ | :--------------------------------------------------------------------------------------------------- |
| KQL (`FIND`) | read          | Multiple queries proceed in parallel.                                                                |
| META         | read          | Same as KQL.                                                                                         |
| KML          | write         | Serialised against any other KML, and exclusive against KQL/META — guarantees a consistent snapshot. |

The lock guards *KIP-level* atomicity. Within a single command the
underlying Anda DB collection operations may still be batched and
parallelised (e.g. `try_join!` in `connect` to open both collections at
once), but no other KIP command can observe a partial KML write.

---

## 7. Per-query execution context

Every `execute_kql` / `execute_kml` / `execute_meta` call constructs a
fresh [`QueryContext`](../rs/anda_cognitive_nexus/src/types.rs):

```rust,ignore
pub struct QueryContext {
    pub bindings:  FxHashMap<String, BTreeSet<EntityPK>>, // ?var → entity ids
    pub predicates: FxHashMap<String, FxHashSet<String>>, // ?var → predicate names
    pub groups:    FxHashMap<String, GroupedBindings>,    // ?var → grouping rows
    pub cache:     QueryCache,
    pub regex_cache: FxHashMap<String, regex::Regex>,
}

pub struct QueryCache {
    pub concepts:     parking_lot::RwLock<FxHashMap<u64, Concept>>,
    pub propositions: parking_lot::RwLock<FxHashMap<u64, Proposition>>,
}
```

`QueryCache` is small but critical: a single KIP command may touch the
same row many times (multi-hop traversals, NOT scopes, FILTER
re-evaluation), and without the cache each touch would round-trip to
Anda DB.

The `regex_cache` memoises `regex::Regex` compilations within a single
query — cheap if the same pattern appears in multiple `FILTER` calls,
and free of the global lock contention you would get from
`once_cell::sync::Lazy`.

---

## 8. Executing KQL

### 8.1 Pipeline overview

```text
parse_kql() ──► KqlQuery {
    where_clauses: [WhereClause],
    find_clause:   FindClause,
    order_by, cursor, limit
}
                   │
                   ▼
   execute_kql:
     1.  for clause in where_clauses:
             execute_where_clause(&mut ctx, clause)
     2.  execute_find_clause(&mut ctx, find_clause, order_by, cursor, limit)
     3.  return (json, next_cursor)
```

WHERE clauses populate `ctx.bindings` (and, where relevant,
`ctx.predicates` / `ctx.groups`). FIND projects those bindings through
the user's expressions, applies `ORDER BY`, then paginates. When the
`FIND` list contains a single expression the response payload is the
expression's value directly; otherwise it is a JSON array of column
arrays, preserving alignment across rows.

### 8.2 WHERE clause executors

`execute_where_clause` dispatches into:

| Clause shape                                 | Routine                                                                 |
| :------------------------------------------- | :---------------------------------------------------------------------- |
| `?c {type: "T", name: "N"}`                  | `execute_concept_clause` — composite `(type,name)` BTree lookup.        |
| `(?s, "p", ?o)`                              | `execute_proposition_clause` — see §8.3.                                |
| `FILTER(...)`                                | `execute_filter_clause` — see §8.4.                                     |
| `OPTIONAL { ... }` / `NOT { ... }` / `UNION` | `execute_optional_scope` / `execute_not_scope` / `execute_union_scope`. |

Each routine is allowed to *narrow* an existing variable binding or to
*introduce* new ones; it never widens an already-bound variable.

### 8.3 Multi-hop matching

A proposition pattern with a `{m,n}` repetition compiles to a BFS over
the `subject`-keyed BTree index, using `EntityID::to_string()` as the
seed. The BFS skips zero-hop unless the user explicitly wrote `{0,n}`
(introduced in RC6 §3.1.3); each hop merges newly-reachable entities
into the current frontier and stops when (a) the frontier stabilises
or (b) the upper bound is reached.

The matching helpers
`handle_subject_object_ids_matching`,
`handle_subject_ids_any_matching`, and
`handle_any_to_object_ids_matching` issue **a single OR-query** to Anda
DB — `RangeQuery::Or<Box<RangeQuery<Fv>>>` over the relevant
field — instead of N or N×M sequential queries. A cardinality-1 fast
path falls through to a plain `RangeQuery::Eq`. This keeps multi-hop
costs sub-linear in graph fan-out.

### 8.4 Filter semantics

`FILTER(...)` accepts arbitrary boolean expressions over bound
variables. The interesting RC6 features are:

- **`IN [a, b, c]`** — set membership, evaluated in O(log n) when the
  comparison field is indexed.
- **`IS NULL` / `IS NOT NULL`** — used to express "attribute exists" /
  "attribute absent"; routed through `extract_concept_field_value` /
  `extract_proposition_field_value` so missing-vs-null is handled
  uniformly.
- **Short-circuit evaluation** — `&&` and `||` are evaluated lazily,
  which is essential for filters that mix cheap predicates with regex
  or list lookups.
- **Predicate fast-path for NOT** — when a `NOT { (s,p,o) }` scope
  reduces to "this subject has no proposition with predicate p", the
  executor uses the `predicates` BTree index to skip loading any
  rows.

### 8.5 FIND clause and grouped aggregation

`FIND(...)` is a list of expressions; each expression is one of:

- **Variable projection** (`?c`, `?c.attributes.dose_mg`).
- **Aggregate** (`COUNT(?c)`, `COLLECT(?p.predicates.author)`, etc.).
- **Computed value** (literal arithmetic over bound values).

When at least one expression is an aggregate, the executor implicitly
groups by all *non-aggregate* expressions in the FIND list (RC6 §3.4).
Counting aggregates avoid IO entirely when the underlying binding is
already known; collection aggregates draw from `ctx.groups`.

`ORDER BY` is applied via `helper::apply_order_by`; pagination uses an
opaque cursor that encodes the last emitted row offset.

---

## 9. Executing KML

### 9.1 `UPSERT`

`execute_upsert` walks each `UpsertBlock` (concept or proposition) in
declaration order, building a *handle table* (`?h → resolved id`). When
a block does not declare attributes/metadata explicitly, a default
`metadata.source` is injected (`KIP v1.0-RC6 §4.1.1`) so provenance is
never lost. Concept blocks resolve via
`get_or_init_concept`; proposition blocks resolve subject and object
through `PropositionPK::try_from`, which rejects unbound
`Variable("...")` in nested position (the higher-order caveat from §4.5).

The result is an `UpsertResult` JSON payload with counters
(`upserted_concepts`, `upserted_propositions`, `errors`) — see RC6
§4.1.

### 9.2 `DELETE ATTRIBUTES` / `DELETE METADATA`

Both operate on a `target` clause that resolves to a `TargetEntities`
set of concept and/or proposition ids. The executor walks each id,
loads the row through the per-query cache, removes the requested keys,
and writes back. **After every successful `update` it invalidates the
cached row** — see §12 for why.

### 9.3 `DELETE PROPOSITIONS`

Per RC6 §4.2.3 the unit of deletion is the `(subject, predicate,
object)` triple, not the underlying row. The executor:

1. Resolves the target propositions.
2. For each row, removes the matching predicate key from the
   `predicates` map. If the map becomes empty, the entire row is
   removed. Otherwise the row is updated and the BM25 `properties`
   field is rebuilt from the surviving predicates.
3. Invalidates the cache entry for the row id, even when only one
   predicate of many was removed (avoids the resurrection bug from §12).

### 9.4 `DELETE CONCEPT` and the protected scope

`is_protected_concept` rejects deletions that would damage the agent's
identity or the type system itself, returning `KIP_3004` (`KIP v1.0-RC6
§4.2.4`). The protected set is:

- `{type: "$ConceptType", name: "$ConceptType"}`
- `{type: "$ConceptType", name: "$PropositionType"}`
- `{type: "Person", name: "$self"}`
- `{type: "Person", name: "$system"}`
- `{type: "Domain", name: "CoreSchema"}`

After the pre-flight check passes, the executor performs a transitive
cascade: every concept id is BFS-expanded via the `subject` and
`object` indexes (each newly discovered proposition is enqueued so any
proposition referring to *it* is also collected), and the resulting
sets of concept and proposition ids are deleted in one Anda DB
transaction.

### 9.5 Dry-run semantics

When the caller passes `dry_run = true`:

- `UPSERT` validates referenced types and handle resolution **without**
  writing anything; every variable binding still gets its conceptual id
  so the response shape matches a real run.
- `DELETE CONCEPT` runs the `KIP_3004` check (the most-likely user
  mistake) and reports the cascade scope as if it were going to
  execute, but performs no IO.
- Other delete variants short-circuit with zero counters.

---

## 10. Executing META

| Sub-command                          | Implementation                                                                                                                                 |
| :----------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------- |
| `DESCRIBE PRIMER`                    | `execute_describe_primer` — projects `(Person, $self)` plus core domain anchors. Returns a single JSON object; `next_cursor` is always `None`. |
| `DESCRIBE DOMAINS`                   | `execute_describe_domains` — enumerates `Domain` concepts. `next_cursor` is always `None`.                                                     |
| `DESCRIBE CONCEPT TYPE[S]`           | `execute_describe_concept_type[s]` — paginated by `_id`; each entry is a `ConceptInfo` projection (definition + member counts).                |
| `DESCRIBE PROPOSITION TYPE[S]`       | Same shape as above but for `$PropositionType` definitions; uses the `predicates` BTree index to estimate cardinality.                         |
| `SEARCH CONCEPT "..." [LIMIT N]`     | BM25 over `concepts.["name", "attributes", "metadata"]`. Results are deserialised through `Concept`, then projected.                           |
| `SEARCH PROPOSITION "..." [LIMIT N]` | BM25 over `propositions.["properties"]`. Filters can post-prune by predicate / subject type / etc.                                             |

---

## 11. Performance notes

- **Single OR-query batching** — proposition matching helpers issue one
  `RangeQuery::Or` rather than per-id queries (§8.3). When the input
  cardinality is 1 the planner falls through to a single
  `RangeQuery::Eq`, avoiding the OR overhead.
- **COUNT skips IO** — when a counting aggregate references a binding
  whose ids are already known, the executor returns `len()` directly
  instead of loading rows.
- **Predicate fast-path for NOT** — `NOT { (?s, "p", ?o) }` consults the
  `predicates` index and short-circuits when the predicate is unused.
- **Per-query cache** — the `QueryCache` collapses redundant row loads
  inside one execution; ID-based concept lookups become `O(1)` after
  the first hit.
- **Regex memoisation** — `ctx.regex_cache` avoids re-compiling identical
  filter patterns inside a single query, while still allowing a fresh
  cache for the next query (which keeps memory bounded).

---

## 12. Operational caveats

- **Stale-cache pitfall.** `QueryCache` is keyed on row id. A delete
  loop where `target_entities` lists the *same* proposition id under
  several predicates would, without invalidation, read the pre-update
  state on the second iteration and write back already-removed
  predicates / attributes. Every `propositions.update(...) /
  remove(...)` call inside `execute_delete_*` invalidates the cache
  entry **after** the update succeeds. The same applies to concepts.
  This is covered by the regression test
  `test_kml_delete_propositions_multi_predicate_no_resurrection`.
- **Higher-order references in UPSERT.** Use literal `{type, name}` for
  nested subject/object — variable handles only resolve at the
  top-level position.
- **Capsule version drift.** If you replace bundled Genesis capsules
  with a customised set, bump `capsule_version` accordingly so future
  upgrades don't re-apply older versions on top.
- **`$self` / `$system` lifecycle.** Because they are not auto-created,
  agent code must insert them once at first boot and treat their
  identities as protected (KIP_3004 already does so on the storage
  side).
- **Lock granularity.** `kml_lock` is one global RwLock per
  `CognitiveNexus`. If you need finer-grained concurrency, shard the
  knowledge base across multiple `CognitiveNexus` instances and route
  KML by domain.

---

## 13. Testing

Run the focused suite:

```bash
cargo test -p anda_cognitive_nexus --lib
```

The suite covers parser-to-executor round-trips, KQL multi-hop, all
four `DELETE` variants (including `KIP_3004` and the multi-predicate
resurrection regression), `UPSERT` idempotency, and the META
introspection commands. The bundled `examples/kip_demo.rs` is excluded
from the `--lib` run because it links against `object_store::local`,
which is gated behind a feature flag in this workspace.

---

## 14. Compatibility

| Component         | Version pinned                                                       |
| :---------------- | :------------------------------------------------------------------- |
| KIP specification | `v1.0-RC6` (see [SPECIFICATION.md](../rs/anda_kip/SPECIFICATION.md)) |
| `anda_kip` crate  | `0.7.x`                                                              |
| `anda_db` crate   | `0.7.x`                                                              |
| Rust edition      | 2024 (workspace-default; `async fn` in traits, `let-else`, …)        |
| Tokio             | `1.x` with `sync` and `rt-multi-thread`                              |

RC6 features explicitly verified by the test suite: zero-hop
`{0,n}` repetitions, `FILTER` with `IN` / `IS_NULL` / `IS_NOT_NULL`,
`OPTIONAL` / `NOT` / `UNION` scopes, implicit `GROUP BY`, BM25-backed
`SEARCH` for both entity kinds, `KIP_3004` protected-scope enforcement
across both real and `dry_run` calls, and the transitive `DELETE
CONCEPT` cascade through higher-order propositions.
