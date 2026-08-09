# @ldclabs/kip-do

A [KIP](https://github.com/ldclabs/KIP) knowledge-graph engine running inside a
SQLite-backed **Cloudflare Durable Object**. One KIP database per Durable
Object.

This is a sibling of the Rust reference engine
[`anda_cognitive_nexus`](../../rs/anda_cognitive_nexus), not a binding to it.
The storage engine is SQLite instead of `anda_db`, and the grammar is
[`@ldclabs/kip-lang`](https://github.com/ldclabs/KIP/tree/main/packages/kip-lang)
— a native TypeScript implementation of the same KIP revision, held to the Rust
grammar's behaviour by a differential test.

```bash
npm install @ldclabs/kip-do
```

## How the two engines are kept in step

Two implementations of one language drift unless something makes them agree.
Here that something is a test, not a shared binary.

`@ldclabs/kip-lang` parses to a syntax tree and then **lowers** it to the
executable AST — the same wire shape `anda_kip`'s Rust AST serializes to. So
`test/parser-oracle.test.ts` can compare them field for field: for every
command in a corpus harvested from the conformance fixtures, the Rust parser's
own tests and the bundled capsules, `parseKip(src)` must produce exactly what
the Rust grammar produces, or both must reject it. `rs/anda_kip` compiled to
WebAssembly is that oracle. It is a test dependency in `vendor/`, not part of
the shipped package.

A failure there is a divergence between the two KIP engines, which is the most
expensive kind of bug this project can have: the same command would succeed on
one deployment and fail on the other, or mean two different things.

Dropping the WASM module from the runtime buys a ~141 KB smaller gzipped
bundle, no `wasm-pack` in the release path, real source positions in syntax
errors, and no Rust toolchain for a JavaScript contributor.

The executor is TypeScript because it is the part that must change: it is
written against SQL and `ctx.storage.sql`, not against `anda_db`'s B-Tree,
BM25 and object-store layers.

The error taxonomy is **generated** from the Rust source
(`scripts/codegen-errors.mjs` → `src/errors.generated.ts`). Hand-copying 13
codes, names and agent-facing recovery hints produces a table that compiles,
passes tests, and is quietly wrong — and a mismatched `hint` breaks an agent's
self-correction loop with nothing to detect it. The oracle test also checks the
generated table against the catalog the reference grammar reports at runtime.

## Quick start

```ts
// src/index.ts
import { KipDatabase } from '@ldclabs/kip-do'

export class MyKipDatabase extends KipDatabase<Env> {}

export interface Env {
  KIP_DB: DurableObjectNamespace<MyKipDatabase>
  TOKENIZER: Fetcher
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const name = new URL(request.url).pathname.slice(1) || 'default'
    return env.KIP_DB.getByName(name).fetch(request)
  },
}
```

```jsonc
// wrangler.jsonc
{
  "name": "my-kip-app",
  "main": "src/index.ts",
  "compatibility_date": "2025-01-01",
  "durable_objects": {
    "bindings": [{ "name": "KIP_DB", "class_name": "MyKipDatabase" }]
  },
  // `new_sqlite_classes` is required — the KV-backed backend has no SQL API.
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["MyKipDatabase"] }],
  "services": [{ "binding": "TOKENIZER", "service": "alink-tokenizer" }]
}
```

Then, over RPC or `POST` JSON-RPC (`{"method":"execute_kip","params":{"command":"..."}}`):

```ts
const db = env.KIP_DB.getByName('agent-42')

await db.executeKip(`
  UPSERT {
    CONCEPT ?headache { {type: "Symptom", name: "Headache"} }
    CONCEPT ?aspirin {
      {type: "Drug", name: "Aspirin"}
      SET ATTRIBUTES { risk_level: 1 }
      SET PROPOSITIONS { ("treats", ?headache) }
    }
  }
`)

await db.executeKip(`
  FIND(?d.name)
  WHERE {
    ?d {type: "Drug"}
    ?s {name: "Headache"}
    (?d, "treats", ?s)
    FILTER(?d.attributes.risk_level < 3)
  }
`)
```

## Chinese text and the tokenizer service

Durable Object SQLite ships FTS5 with only the built-in tokenizers (`ascii`,
`unicode61`, `porter`, `trigram`), and a Worker cannot load a C extension, so
jieba is unavailable inside SQLite. Under `unicode61` a whole Han run collapses
into a single token and realistic Chinese queries return nothing.

This engine therefore treats
[`alink-tokenizer`](https://github.com/ldclabs/alink/tree/main/alink-tokenizer)
as the **sole segmentation authority**, called on both the write path and the
read path:

- FTS5 columns use `tokenize = 'ascii'` so SQLite does *no* linguistic work of
  its own — it splits on ASCII punctuation and treats every byte ≥ 0x80 as a
  token character, so a pre-segmented CJK token survives intact. `unicode61`
  would re-apply Unicode case folding on top of the service's NFKC +
  lowercasing, letting the two paths diverge for exactly the inputs where it
  matters.
- Every response's `X-Tokenizer-Version` is persisted per row in `tok_ver`. A
  `TOKENIZER_VERSION` bump makes previously indexed rows' vocabulary
  incomparable, so `reindexStale()` finds and rebuilds them; an alarm drives
  this off the request path.
- If the service is unreachable, writes **fail**. There is no fallback to local
  segmentation on purpose: degrading silently would write tokens that disagree
  with everything indexed before and after, and the damage would only surface
  as queries quietly returning nothing.

Without a `TOKENIZER` binding the package falls back to `SimpleTokenizer`,
which is fine for ASCII corpora and useless for Chinese. Bind the service in
production.

## Where this engine is stronger than the Rust one

| | `anda_cognitive_nexus` | this package |
|---|---|---|
| KML statement atomicity | no WAL; a failed multi-block UPSERT "may leave a prefix of its blocks applied" | `transactionSync` — commits whole or rolls back |
| Preflight pass | required, doubling every UPSERT's index lookups | not needed |
| `EXPECT VERSION` | check-then-act, safe only within one process | globally safe (the object *is* the serialization point) |
| Multi-hop traversal | hand-rolled BFS, one storage round trip per frontier node | one recursive CTE |
| `DESCRIBE PRIMER` | ~150 object GETs for a 50-type schema | 3 indexed queries |
| Deletion cascade | BFS with per-predicate frontier amplification | one recursive CTE |
| Operational surface | flush invariants, poison recovery, manifests, compaction, shard routing | none of it |
| Point-in-time recovery | — | 30 days, built in |

## Coverage

Implemented and tested:

- **KQL** — concept and proposition patterns, meta-statement (nested)
  endpoints, predicate variables and alternatives, `FILTER` (comparisons,
  `AND`/`OR`/`NOT`, `CONTAINS`/`STARTS_WITH`/`ENDS_WITH`/`REGEX`), `NOT`,
  `OPTIONAL`, `UNION`, multi-hop `{m,n}`, `ORDER BY`, `LIMIT`, `CURSOR`,
  global aggregation (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`, with `DISTINCT`).
- **KML** — `UPSERT` (concepts, propositions, `SET PROPOSITIONS`, block
  metadata, `EXPECT VERSION`), `UPDATE` (including `ADD`/`MUL`/`CLAMP`/
  `COALESCE`), `MERGE CONCEPT ... INTO` with link repointing, `DELETE
  ATTRIBUTES` / `METADATA` / `PROPOSITIONS` / `CONCEPT ... DETACH`.
- **META** — `DESCRIBE PRIMER` / `DOMAINS` / `CONCEPT TYPES` / `CONCEPT TYPE` /
  `PROPOSITION TYPES` / `PROPOSITION TYPE`, `SEARCH` with `WITH TYPE`,
  `THRESHOLD` and `LIMIT`.

Also implemented and covered by the shared conformance fixtures:

- **Schema-first enforcement.** A concept type must be declared as a
  `$ConceptType` concept and a predicate as a `$PropositionType` before
  anything may use them; otherwise `KIP_2001`.
- **Bootstrap capsules.** The bundled `.kip` capsules — the *same files* the
  Rust engine ships — are applied on first construction, so a fresh Durable
  Object starts with the base schema rather than empty. `persons/self.kip`
  and `persons/system.kip` are deliberately excluded: `$self` evolves with the
  agent and must never be reset to a template.
- **Protected scope.** The self-defining meta-types, the `Domain` type, the
  `belongs_to_domain` predicate, the core domain and the `$self` / `$system`
  actors reject any write that would change them (`KIP_3004`). A bare
  re-declaration that changes nothing stays a no-op, which is what lets a
  capsule be re-applied.

**Not implemented** — these report `KIP_3001` with an explicit message rather
than answering wrongly:

- `EXPORT`.
- Grouped aggregation: a plain column beside an aggregate, e.g.
  `FIND(?symptom.name, COUNT(?drug))`. Global aggregation
  (`FIND(COUNT(?x))`, several aggregates together) works. This is the one
  remaining gap the conformance suite skips — 10 cases, all marked with the
  reason in the fixtures.

## Known divergences from the Rust engine

These are deliberate, and each one changes an observable result. Do not assume
a workload ports without re-testing against them.

1. **`THRESHOLD` values do not carry over.** FTS5's `bm25()` is scaled
   differently from `anda_db`'s BM25. `normalizeScore` applies the same
   `score / (score + 2)` saturation shape, but the input distribution differs,
   so a `THRESHOLD 0.4` calibrated against the Rust engine means something else
   here. Re-calibrate.

2. **Multi-hop returns reachable nodes, not distinct paths.** The recursive CTE
   keys `visited` on the node, so a node reachable by three routes yields one
   binding with its minimum hop count. The Rust engine enumerates paths and can
   return the same node several times.

3. **One cursor scheme, not five.** Cursors are numeric offsets over a
   deterministic order. The Rust engine uses entity-anchored keyset cursors for
   some projection shapes and offsets for others, and its tokens are base64
   deterministic CBOR. **Cursors are not interchangeable between the two
   engines.**

4. **Comparison semantics are stricter.** The Rust `ComparisonOperator::compare`
   implements loose equality (`3.0 == 3`, numeric strings, RFC 3339 datetime
   instants). This engine compares numbers to numbers and strings to strings,
   and returns `false` for mixed or unordered pairs.

5. **`UPDATE ... LIMIT` picks a different subset.** Neither engine defines
   which elements survive the cap without `ORDER BY`; they simply choose
   differently.

6. **Cross-statement atomicity.** Each statement in a `commands` batch is
   individually atomic. The Rust engine checkpoints its two collections
   independently, so it can leave one applied and not the other — a batch that
   relied on that partial-application behavior will behave differently here.

7. **`FIND` over an OPTIONAL-padded variable.** Projecting a single variable
   that OPTIONAL may have left unbound yields one value per solution row here,
   including `null` for the padded rows; the Rust engine projects that
   variable's distinct entity domain and so drops them. Both readings are
   defensible and the spec does not settle it — tracked as an open question in
   `fixtures/kip-conformance/smoke-basics.json`.

**Multi-column `FIND` is column-major** in both engines: `FIND(?a, ?b)` returns
one array per projected variable, index-aligned across arrays — not one array
per row. This package was row-major until the conformance suite caught it. A
variable named twice (`FIND(?d.name, ?d.attributes.risk)`) is a *single* column
whose entries are tuples.

## Conformance suite

`fixtures/kip-conformance/` holds language-neutral behavioural fixtures that
**both** engines run — this package via `pnpm run test:conformance`, the Rust
engine via `cargo test -p anda_cognitive_nexus --test conformance`. A case that
passes in one and fails in the other is a divergence report. See
[the fixture README](../../fixtures/kip-conformance/README.md) for the format
and the normalization rules.

## Platform limits you will hit

| Limit | Value | Consequence |
|---|---|---|
| Storage per Durable Object | 10 GB | one database's hard ceiling |
| Bound parameters per query | **100** | every id-set query goes through `json_each(?)`; never write `IN (?, ?, …)` |
| String / row size | 2 MB | guarded by `checkValueSize`, reported as `KIP_4002` |
| `LIKE` / `GLOB` pattern | 50 bytes | why `FILTER` is evaluated in TypeScript, never pushed into SQL |
| KIP command length | 256 KB | enforced by the grammar |
| CPU per request | 30 s (configurable to 5 min) | the traversal caps matter more here than in a server process |

A Durable Object is single-threaded, so all queries against one database
serialize. That suits one-agent-one-database; a shared hot database is a
throughput ceiling, and Durable Objects have no read replicas.

## Development

```bash
pnpm install
pnpm run codegen:errors          # regenerate the error taxonomy from error.rs
pnpm run codegen:oracle-corpus   # re-harvest the differential corpus
pnpm run build:oracle-wasm       # rebuild the oracle from rs/anda_kip (needs wasm-pack)
pnpm test                        # runs inside workerd via @cloudflare/vitest-pool-workers
pnpm run build
```

Tests run in **workerd**, not Node. The engine's contract is the platform's —
`transactionSync`, FTS5, the 100-parameter ceiling — and none of it is
reproducible against a Node SQLite shim.

`vendor/anda_kip_wasm/` is committed but **not** shipped: it is the oracle
`test/parser-oracle.test.ts` compares against. Rebuild and commit it whenever
the Rust grammar changes, together with `pnpm run codegen:oracle-corpus` —
that is the moment a divergence is meant to surface.

## License

MIT. See [LICENSE](../../LICENSE).
