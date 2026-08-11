# cf-tokenizer

`cf-tokenizer` is a stateless, high-throughput search tokenizer for
[Cloudflare Workers](https://developers.cloudflare.com/workers/) and Durable
Objects. It exposes a small batch HTTP API around `jieba-rs` and Unicode word
segmentation, packaged as a `linux/amd64` container for
[Cloudflare Containers](https://developers.cloudflare.com/containers/).

The service is the public successor to the private `alink-tokenizer` referenced
by older `@ldclabs/kip-do` documentation. Its first consumer is
[`@ldclabs/kip-do`](../../ts/kip-do), where the same service tokenizes text on
both the index and query paths so Chinese and other multilingual search terms
use one vocabulary.

The process is deliberately small: it listens on port `8080`, keeps only the
read-only jieba dictionary in memory, and does not persist or request-log input.
It has no built-in authentication, so expose it through a private Worker
service binding or add authentication in the Worker that fronts the container.

## HTTP API

### `GET /healthz`

Returns `200 OK` with body `ok` and the current tokenizer version:

```http
X-Tokenizer-Version: 1
```

### `POST /tokenize`

Requests must use `Content-Type: application/json`:

```json
{
  "texts": [
    "我在找深圳的硬件供应链合作伙伴",
    "Looking for a hardware supply-chain partner in Shenzhen"
  ],
  "mode": "search"
}
```

The response contains one token list per input string, in the same order. A
successful response always includes `X-Tokenizer-Version`:

```json
{
  "tokens": [
    [
      "我",
      "在",
      "找",
      "深圳",
      "的",
      "硬件",
      "供应",
      "供应链",
      "合作",
      "伙伴",
      "合作伙伴"
    ],
    [
      "looking",
      "for",
      "a",
      "hardware",
      "supply",
      "chain",
      "partner",
      "in",
      "shenzhen"
    ]
  ]
}
```

Only `mode: "search"` is supported. Unsupported modes and batches larger than
256 texts return `400 Bad Request` with `{ "error": "..." }`.

### Limits

| Limit                        |       Value | Behaviour                                                                   |
| ---------------------------- | ----------: | --------------------------------------------------------------------------- |
| Texts per request            |         256 | The request is rejected; clients should chunk larger batches.               |
| Characters examined per text |   1,048,576 | Later characters are ignored.                                               |
| Distinct tokens per text     |         256 | Later tokens are ignored.                                                   |
| HTTP request body            | about 2 MiB | Axum's default body limit rejects a larger body before per-text truncation. |

An empty `texts` array is valid. Empty, whitespace-only, punctuation-only, or
emoji-only strings produce an empty token list.

## Tokenization contract

For each input string, the service performs this deterministic pipeline:

1. Truncate to the per-text character limit.
2. Apply Unicode NFKC normalization and Unicode lowercasing.
3. Fold common Russian and Arabic spelling variants: Russian `ё` to `е`;
   Arabic tatweel and vowel marks are removed, and common alef, alef maksura,
   and teh marbuta variants are unified.
4. Split the text into Han and non-Han runs. Han text uses jieba's search-mode
   segmentation; other scripts use Unicode UAX #29 word boundaries.
5. Drop tokens containing no letter or digit, deduplicate while preserving the
   first occurrence, and apply the token cap.

Search-mode jieba intentionally emits overlapping terms such as `供应`, `供应链`,
and `合作伙伴` to improve recall. Latin diacritics are preserved (`café` remains
`café`); applications that need accent-insensitive matching must add that rule
here, use it on both index and query paths, and bump the tokenizer version.

The golden tests in [`src/tokenizer.rs`](src/tokenizer.rs) pin behaviour for
Chinese, English, mixed CJK/Latin, Russian, Arabic, Spanish, French, accented
Latin, empty input, deduplication, and both caps.

## Run locally

The crate is a standalone Cargo workspace with its own lockfile:

```bash
cd rs/cf-tokenizer
cargo test --locked
cargo run --locked
```

In another terminal:

```bash
curl -i http://127.0.0.1:8080/healthz

curl -i http://127.0.0.1:8080/tokenize \
  -H 'content-type: application/json' \
  --data '{"texts":["AI创业者 seeking GPU credits 🚀"],"mode":"search"}'
```

Or build and run the same minimal image used in production:

```bash
docker build --platform linux/amd64 -t cf-tokenizer:dev .
docker run --rm -p 8080:8080 cf-tokenizer:dev
```

The final image is `FROM scratch`: it contains only the static musl binary and
the jieba dictionary compiled into it.

## Deploy on Cloudflare

Cloudflare Containers are controlled by a Durable Object class inside a
Worker. A separate Worker can then reach that Worker through a service binding.
The following is a minimal stateless pool; adjust the pool size and idle timeout
to match traffic and acceptable cold-start latency.

Install the Container helper in the Worker project that fronts the image:

```bash
npm install @cloudflare/containers
```

```ts
// src/index.ts in the Worker that fronts the container
import { Container, getRandom } from '@cloudflare/containers'

export class TokenizerContainer extends Container {
  defaultPort = 8080
  pingEndpoint = 'localhost/healthz'
  sleepAfter = '10m'
  enableInternet = false
}

interface Env {
  TOKENIZER_CONTAINERS: DurableObjectNamespace<TokenizerContainer>
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const path = new URL(request.url).pathname
    if (path !== '/healthz' && path !== '/tokenize') {
      return new Response('not found', { status: 404 })
    }

    const container = await getRandom(env.TOKENIZER_CONTAINERS, 3)
    return container.fetch(request)
  },
}
```

Use the image URI returned by `wrangler containers build` in the fronting
Worker's configuration:

```bash
cd rs/cf-tokenizer
npx wrangler containers build -p -t cf-tokenizer:1.0.0 .
```

```jsonc
// wrangler.jsonc for the Worker named `cf-tokenizer`
{
  "$schema": "./node_modules/wrangler/config-schema.json",
  "name": "cf-tokenizer",
  "main": "src/index.ts",
  "compatibility_date": "2026-08-11",
  "workers_dev": false,
  "preview_urls": false,
  "containers": [
    {
      "name": "cf-tokenizer",
      "class_name": "TokenizerContainer",
      "image": "registry.cloudflare.com/<ACCOUNT_ID>/cf-tokenizer:1.0.0",
      "instance_type": "lite",
      "max_instances": 3
    }
  ],
  "durable_objects": {
    "bindings": [
      {
        "name": "TOKENIZER_CONTAINERS",
        "class_name": "TokenizerContainer"
      }
    ]
  },
  "migrations": [
    {
      "tag": "v1",
      "new_sqlite_classes": ["TokenizerContainer"]
    }
  ]
}
```

Deploy and inspect the Container application:

```bash
npx wrangler deploy
npx wrangler containers list
```

Deploy that Worker, then bind it from a consumer such as `kip-do`:

```jsonc
{
  "services": [
    { "binding": "TOKENIZER", "service": "cf-tokenizer" }
  ]
}
```

`workers_dev: false` keeps the tokenizer off a public `workers.dev` hostname;
the service binding still gives consumer Workers a `Fetcher`. If you add a
public route, protect it in the fronting Worker because the Rust service itself
does not authenticate requests.

The repository's
[`build-cf-tokenizer.yml`](../../.github/workflows/build-cf-tokenizer.yml)
workflow also publishes
`ghcr.io/ldclabs/cf-tokenizer_amd64:<tag>`. Cloudflare Containers do not pull
directly from GHCR at the time of writing; pull that image locally and push it
to Cloudflare's registry with `wrangler containers push`, or build from this
Dockerfile as shown above. See Cloudflare's
[image-management documentation](https://developers.cloudflare.com/containers/platform-details/image-management/)
for the currently supported registries.

## Versioning and index rebuilds

`TOKENIZER_VERSION` in [`src/tokenizer.rs`](src/tokenizer.rs) identifies the
token vocabulary, independently of the crate or image version. Change it for
any behaviour change, including normalization or folding rules, a `jieba-rs`
upgrade, or a dictionary change.

Consumers must persist `X-Tokenizer-Version` with indexed rows and rebuild rows
whose version differs from the live service. Never silently fall back to a
different tokenizer when this service is unavailable: that mixes incompatible
vocabularies and turns a visible outage into silent search misses.

Deploy one tokenizer vocabulary at a time. A deliberately mixed old/new pool
can return different versions for consecutive chunks of one logical write;
`@ldclabs/kip-do` detects and rejects that condition so the caller can retry.
After a version rollout, allow the consumer's stale-index job to finish before
considering the rollout complete.

## Release checklist

1. Run `cargo fmt --check` and `cargo test --locked`.
2. Commit `Cargo.lock` with any intentional dependency change; the Docker build
   uses `--locked`.
3. If token output changed, update the golden tests and bump
   `TOKENIZER_VERSION`.
4. Build an immutable `linux/amd64` image tag; do not deploy `latest` as the
   only rollback reference.
5. Check `/healthz` and a representative multilingual `/tokenize` request.
6. Deploy the fronting Worker and verify a consumer service binding.
7. Monitor stale-row reindexing until every consumer index uses the new
   tokenizer version.

## License

MIT. See [LICENSE](../../LICENSE).
