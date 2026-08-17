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
Capsule export and verification, and the Governance control plane at command
scope. **56 of the 62 shared conformance cases pass**; the remaining 6 are all
the historical read path (`AS OF`), and `test/conformance.test.ts` names them
individually so closing the gap has to be acknowledged rather than absorbed into
a number.

`DESCRIBE CAPABILITIES` reports every gap with a reason. Two are worth naming
here:

- **Governance is enforced at command scope, not yet at element scope.** There
  are Principals, groups, Grants, Delegations, Policy versions and Approvals,
  and every command is authorized against them before it runs — a caller with no
  Grants can do nothing, and a revoked Grant stops working on the next command.
  What is *not* built is the narrowing a permitted command then carries: a Grant
  scoped to a kind, a type or a classification gates the command and does not yet
  narrow what that command reaches. `DESCRIBE ACCESS` reports the same
  granularity, because a half-built plane that does not say so is worse than an
  absent one.
- **`SEARCH`, in every mode.** No search index is built here. A keyword search
  over unsegmented text would silently disagree with the reference engine about
  which documents match, and a caller cannot tell a narrow index from a narrow
  world.

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

## Multilingual text and the tokenizer service

**Nothing indexes yet.** `SEARCH` is refused in every mode, and the tokenizer
client below ships ahead of the index it will feed. The reasoning is recorded
here because it is what the eventual index has to be built against, and getting
it wrong is not visible from the outside — a mis-segmented corpus returns
plausible results for the wrong reason.

Durable Object SQLite ships FTS5 with only the built-in tokenizers (`ascii`,
`unicode61`, `porter`, `trigram`), and a Worker cannot load a C extension, so
jieba is unavailable inside SQLite. Under `unicode61` a whole Han run collapses
into a single token and realistic Chinese queries return nothing.

This engine therefore treats the public
[`cf-tokenizer`](../../rs/cf-tokenizer) service as the **sole segmentation
authority**, called on both the write path and the read path. It applies NFKC,
lowercasing, script-aware segmentation (jieba search mode for Han, Unicode UAX
#29 word boundaries for other scripts), and targeted Russian and Arabic search
folding:

- FTS5 columns use `tokenize = 'ascii'` so SQLite does *no* linguistic work of
  its own — it splits on ASCII punctuation and treats every byte ≥ 0x80 as a
  token character, so a pre-segmented CJK token survives intact. `unicode61`
  would apply a second Unicode normalization and folding policy, so the
  vocabulary actually stored would no longer be exactly the service's
  versioned output.
- If the service is unreachable, writes **fail**. There is no fallback to local
  segmentation on purpose: degrading silently would write tokens that disagree
  with everything indexed before and after, and the damage would only surface
  as queries quietly returning nothing.

Without a `TOKENIZER` binding the package falls back to `SimpleTokenizer`.
That is useful for tests and basic ASCII-oriented deployments, but it emits one
token per Han code point and cannot provide dictionary-based Chinese search.
Bind `cf-tokenizer` in production whenever the corpus is multilingual.

`cf-tokenizer` is a container image, not a Worker binding by itself. Deploy it
behind a small Cloudflare Container Worker, then add that Worker's service
binding as `TOKENIZER`. The tokenizer's
[deployment guide](../../rs/cf-tokenizer/README.md#deploy-on-cloudflare) includes
the Container class, Wrangler configuration, registry workflow, health check,
limits, and version-rollout checklist. A service binding is preferred because
the tokenizer HTTP server does not implement authentication and need not be
publicly reachable.

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
