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

## Status: being rebuilt for KIP 2.0

The 1.x executor has been **deleted, not ported**. KIP 2.0 is a different data
model — the whole point of the version is that *a Proposition existing is not
the Proposition being true* — so a renamed 1.x engine would have been a worse
lie than an absent one. It is recoverable from this branch's history.

What ships today is the language boundary: the parser bridge, the KIP 2.0 error
registry, the tokenizer client and the SQLite helpers. The executor and the
Durable Object class return in later stages, in the order the Rust engine was
rebuilt: storage, Schema Packages, KML, KQL, projection, META, Capsules,
Governance.

The Rust engine is the reference for all of it, and
`fixtures/kip-conformance-2.0/` is the shared acceptance suite both engines run.

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

### Open divergences

The oracle currently reports four commands the Rust grammar rejects and
`@ldclabs/kip-lang@2.0.0` accepts. They are listed with their reasons in
`KNOWN_DIVERGENCES` in `test/parser-oracle.test.ts`, and the test fails if one
is silently fixed upstream so the list cannot rot. The first is the serious
one: an integer past the representable range is **rounded rather than
refused**, so the command executes with a different number than it says. This
package cannot defend against that on its own — by the time it sees the lowered
AST the digits are gone.

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

## Multilingual text and the tokenizer service

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
