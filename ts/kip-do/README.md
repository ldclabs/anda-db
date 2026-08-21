# @ldclabs/kip-do

A [KIP](https://github.com/ldclabs/KIP) Cognitive Nexus running inside a
SQLite-backed **Cloudflare Durable Object**. One Nexus per Durable Object.

This is a sibling of the Rust reference engine
[`anda_cognitive_nexus`](../../rs/anda_cognitive_nexus), not a binding to it.
The storage engine is SQLite instead of `anda_db`, and the grammar is
[`@ldclabs/kip-lang`](https://github.com/ldclabs/KIP/tree/main/packages/kip-lang)
— a native TypeScript implementation of the same KIP revision, held to the Rust
grammar's behaviour by a differential test.

```bash
npm install @ldclabs/kip-do
```

## Status

The 1.x executor was **deleted, not ported**. KIP 2.0 is a different data model
— the whole point of the version is that *a Proposition existing is not the
Proposition being true* — so a renamed 1.x engine would have been a worse lie
than an absent one. It is recoverable from this branch's history.

What works today: the storage layer, Schema Packages and symbol resolution,
transactions and the KML mutation clauses, KQL, the Epistemic Projection, META,
Capsule export and verification, the Governance control plane, and the
historical read path. **All 62 shared conformance cases pass** — the list in
`test/conformance.test.ts` names what is not built rather than counting it, so
closing the last gap meant deleting a name and a new one cannot hide inside a
number that happens to match.

`DESCRIBE CAPABILITIES` reports every gap with a reason. Two are worth naming
here:

- **Governance is enforced per element, on reads and on writes.** There are
  Principals, groups, Grants, Delegations, Policy versions and Approvals. Every
  command is authorized before it runs, and every element it reaches is
  authorized again individually: an element outside the Grant is not in the query
  universe at all, a masked field is invisible to `FILTER` as well as to the
  projection, a sweep that reaches something it may not touch fails rather than
  doing less, and a `RETRACT` needs the standing to say the source withdrew its
  claim. Classification is enforced in both directions: a derived element joins
  its inputs' labels upward at commit — so *read secret Evidence, summarize,
  write public summary* is not an exfiltration path — while raising a label needs
  only `update` and lowering one needs `declassify`, because it is disclosure
  that requires authority rather than caution. The audit records every
  control-plane change and every decision §172 asks for, and `accessAsOf` answers
  who had access at a past instant without claiming anything about today.

  The plane's own records are written through host APIs and are reachable from no
  KML clause, which is what keeps a prompt injection into ordinary memory
  formation off the control plane. `DESCRIBE CAPABILITIES` names what that costs
  and what is still missing, rather than letting the word "governance" imply
  more than is there.
- **Semantic and hybrid `SEARCH`.** There is no embedding model here, so
  `MODE "semantic"` and `MODE "hybrid"` are refused by name. Keyword search is
  built and is the portable baseline §66.3 asks for.
- **Historical `SEARCH`.** The index is maintained against current state and
  keeps no history of itself, so `AS OF SEQ` is refused rather than answered
  from today's index under a past coordinate.
- **`SEARCH ASSERTION | ACTIVITY`.** An Assertion carries a stance, a mode and a
  number; an Activity a class and two timestamps. Neither has free text to
  index, and an empty answer would read as "no such claim exists".

Reading the past works on both axes, and they are deliberately kept apart:
`AS OF SEQ | TX | TIME` asks what this Brain *held* then, `FOR TIME` asks what
was *true* then. One read answers at one coordinate — a request pinned by a
`snapshot_token` whose command names a different one is refused — and symbols
resolve through the Schema Environment that was in force at the coordinate
rather than today's.

A multi-tenant host authenticates its callers by overriding
`KipDatabase.authenticate`, which returns the identity the *host observed* about
the connection. It deliberately cannot be read off the request body: a request
body is exactly what an Agent under prompt injection controls.

The Rust engine is the reference for all of it, and
`fixtures/kip-conformance-2.0/` is the shared acceptance suite both engines
run — the same bytes, inlined into TypeScript because workerd has no
filesystem.

## How the two engines are kept in step

Two implementations of one language drift unless something makes them agree.
Here that something is a test, not a shared binary.

`@ldclabs/kip-lang` parses to a syntax tree and then **lowers** it to the
executable AST — the same wire shape `anda_kip`'s Rust AST serializes to. So
`test/parser-oracle.test.ts` can compare them field for field: for every
command in a corpus harvested from the conformance fixtures and the Rust
sources and tests, `parseKip(src)` must produce exactly what the Rust grammar
produces, or both must reject it. `rs/anda_kip` compiled to WebAssembly is that
oracle. It is a test dependency in `vendor/`, not part of the shipped package.

A failure there is a divergence between the two KIP engines, which is the most
expensive kind of bug this project can have: the same command would succeed on
one deployment and fail on the other, or mean two different things.

Dropping the WASM module from the runtime buys a smaller bundle, no
`wasm-pack` in the release path, real source positions in syntax errors, and no
Rust toolchain for a JavaScript contributor.

The executor is TypeScript because it is the part that must change: it is
written against SQL and `ctx.storage.sql`, not against `anda_db`'s B-Tree,
BM25 and object-store layers.

The oracle currently reports no divergences in either direction across the
whole corpus. The four it found on the way — the worst of which rounded an
out-of-range integer instead of refusing it, so a command executed with a
different number than it said — were fixed upstream in
`@ldclabs/kip-lang@2.0.1`.

### The error registry

KIP 2.0 replaced 1.x's numeric `KIP_xxxx` codes with a registry of stable
names, each carrying a category, a retry class and an agent-facing recovery
hint. The table is **generated** from the Rust source
(`scripts/codegen-errors.mjs` → `src/errors.generated.ts`), read through
`anda_kip_wasm::error_catalog()` so it enumerates `KipErrorCode::ALL` rather
than whatever a text scraper could see. Hand-copying 79 codes with their hints
and retry classes produces a table that compiles, passes tests, and is quietly
wrong — a mismatched `hint` breaks an agent's self-correction loop, and a
widened `retry` class turns a lost write into a duplicated one.

## Quick start

```ts
// src/index.ts
import { KipDatabase } from '@ldclabs/kip-do'

export class MyKipDatabase extends KipDatabase<Env> {}

export interface Env {
  KIP_DB: DurableObjectNamespace<MyKipDatabase>
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
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["MyKipDatabase"] }]
}
```

Then `POST` a KIP request envelope:

```json
{
  "kip": "2.0",
  "operations": [
    { "command": "MUTATE {\n  CREATE CONCEPT ?alice { TYPE \"Person\" NAME \"Alice\" }\n  CREATE CONCEPT ?dark { TYPE \"Preference\" NAME \"Dark\" }\n  ENSURE PROPOSITION ?p (?alice, \"prefers\", ?dark)\n  CREATE ASSERTION ?a { SET FIELDS { proposition: ?p, asserted_by: ?alice, stance: \"support\", mode: \"stated\", confidence: 0.9 } }\n}" },
    { "command": "FIND(?b.status) WHERE { ?p PROPOSITION (?s, \"prefers\", ?o) ?b BELIEF (?p) }" }
  ]
}
```

The second operation answers `["accepted"]` — not because the Proposition
exists, but because an Assertion supports it and the projection's policy says
that is enough. Delete the Assertion's support and the same Proposition
projects as `insufficient`, which is the distinction the whole version is for.

### The response status is not cosmetic

A KIP error carries a retry class, and the HTTP status is mapped from *that*
rather than from the error's name. Two cases matter:

- **A partial batch is 207.** Earlier operations have already committed and are
  durable; reporting the whole request as a failure invites the client to
  re-send writes that landed.
- **`outcome_lookup_required` is never 500.** The write may well have landed.
  500 reads as "nothing happened", and a client acting on that writes again.

## Full-text search and multilingual text

`SEARCH CONCEPT | PROPOSITION | EVIDENCE | COGNITION` is built, in keyword mode,
over SQLite FTS5 with BM25 ranking. The corpus mirrors the Rust engine's field
for field — Concept `name` / `aliases` / `attributes`, Proposition
`predicate_ref` / `attributes`, Evidence `payload_inline` — because two engines
ranking the same corpus differently is a quality difference a caller can live
with, and two engines searching *different text* is a correctness difference
nobody can debug from the outside.

**The index is maintained inside the write transaction.** Maintenance hangs off
`Store.put`, the single funnel every write passes through, so an index entry
commits or rolls back with the row it describes. That is what lets the answer
report `index_seq` equal to `current_space_seq` rather than hedging: there is no
window in which the index lags (§66.5, §79).

### Segmentation

Durable Object SQLite ships FTS5 with only the built-in tokenizers (`ascii`,
`unicode61`, `porter`, `trigram`), and a Worker cannot load a C extension.
`unicode61` finds word boundaries from Unicode categories, which works for
scripts that write spaces and fails completely for the ones that do not: a whole
Han run collapses into one token, so `深色模式` would index as a single term that
no realistic query matches.

So the boundaries are inserted before the text reaches SQLite, by
`Intl.Segmenter` — ICU's dictionary breaking, in process and synchronous. FTS5
still does the final tokenization on both paths, which keeps the index and the
query in step through everything the segmenter does not touch (case folding,
apostrophes, hyphens).

KIP 1.x delegated this to [`cf-tokenizer`](../../rs/cf-tokenizer), an external
jieba-rs service that was the sole segmentation authority for both paths. **That
client is gone, and so is the `TOKENIZER` binding.** It could not survive into
2.0: the engine commits inside `ctx.storage.transactionSync`, and an HTTP call
is not something a synchronous transaction can make. The alternatives were to
make every write async — losing the all-or-none commit the platform hands us —
or to index out of band and then report a freshness the index does not have,
which §66.5 and §79 both forbid.

The cost is that ICU's dictionary is not jieba's, so this engine and the Rust
one segment the same Chinese sentence slightly differently and rank the same
corpus slightly differently. That is a recall difference *between* two engines,
not an asymmetry *inside* either one — and an asymmetry inside one is the
failure that actually loses data, because a document indexed under boundaries
the query path does not reproduce is unreachable forever.

ICU's dictionary can change when the runtime upgrades. `segmenterMark()` makes
that detectable: it is stored beside the index and a mismatch rebuilds it, on
the same principle — tokens from two vocabularies are not comparable, and a row
indexed under the old one is unreachable rather than merely ranked worse.

### What a score is

`retrieval.score` is `-bm25()`, so bigger is better and the default
`THRESHOLD 0.0` keeps everything. It is relevance, never confidence: copying one
into an Assertion would invent an epistemic commitment out of a text match
(§2.10). Scores may be compared *within* one answer and never across engines.

A `SEARCH` miss does not prove absence (§66.6). Ground with `SEARCH`, then read
with `FIND` or `BELIEF`.

## Platform limits you will hit

| Limit | Value | Consequence |
|---|---|---|
| Storage per Durable Object | 10 GB | one database's hard ceiling |
| Bound parameters per query | **100** | every id-set query goes through `json_each(?)`; never write `IN (?, ?, …)` |
| String / row size | 2 MB | guarded by `checkValueSize`, reported as `ResourceExhausted` |
| `LIKE` / `GLOB` pattern | 50 bytes | why `FILTER` is evaluated in TypeScript, never pushed into SQL |
| KIP command length | 256 KB | enforced by the grammar |
| CPU per request | 30 s (configurable to 5 min) | traversal caps matter more here than in a server process |

A Durable Object is single-threaded, so all queries against one database
serialize. That suits one-agent-one-database; a shared hot database is a
throughput ceiling, and Durable Objects have no read replicas.

## Development

```bash
pnpm install
pnpm run codegen:errors          # regenerate the error registry from the oracle
pnpm run codegen:profiles        # re-vendor the Schema Package artifacts
pnpm run codegen:fixtures        # re-inline the shared conformance fixtures
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
