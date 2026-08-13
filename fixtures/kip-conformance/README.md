# KIP conformance fixtures

Language-neutral behavioural tests for KIP engines. Every fixture here is run
by **both** implementations in this repository:

| Engine | Runner |
|---|---|
| `anda_cognitive_nexus` (Rust, `anda_db`) | `rs/anda_cognitive_nexus/tests/conformance.rs` |
| `@ldclabs/kip-do` (TypeScript, Durable Object SQLite) | `ts/kip-do/test/conformance.test.ts` |

The fixtures describe **KIP semantics**, not either implementation. That is the
whole point: a case that passes in one engine and fails in the other is a
divergence report, and a case that has to be marked `skip` for one engine is a
divergence that has been *acknowledged in writing* rather than discovered by a
user.

## Format

One JSON file per scenario group.

```jsonc
{
  "name": "kql-find-concepts",
  "description": "What this group establishes.",

  // Applied in order to a fresh, empty database before the cases run.
  // Fixtures must be self-contained: the Rust engine seeds bootstrap capsules
  // on connect and the TypeScript engine starts empty, so nothing may be
  // assumed to pre-exist.
  "setup": [
    "UPSERT { CONCEPT ?d { {type: \"Drug\", name: \"Aspirin\"} } }"
  ],

  "cases": [
    {
      "name": "projects a scalar column",
      "command": "FIND(?d.name) WHERE { ?d {type: \"Drug\"} }",
      "expect": { "result": ["Aspirin"] }
    },
    {
      "name": "rejects a malformed cursor",
      "command": "FIND(?d) WHERE { ?d {type: \"Drug\"} } LIMIT 1 CURSOR \"x\"",
      "expect": { "error": { "code": "KIP_1001" } }
    }
  ]
}
```

### Case fields

| Field | Meaning |
|---|---|
| `name` | Unique within the file. Becomes the test name. |
| `command` | A single KIP command (KQL, KML or META). |
| `expect.result` | Expected result after normalization (below). |
| `expect.error.code` | Expected KIP error code. `message` may also be given as a substring match. |
| `expect.next_cursor` | Expected cursor. Omit to ignore. |
| `ordered` | `true` when the command's own semantics fix the order (`ORDER BY`, pagination). Default `false` — results are sorted before comparison. |
| `skip` | `{ "rust": "why", "ts": "why" }`. **A skipped case still executes** — only its assertions are dropped. Cases accumulate state, so not running one would leave the two engines' databases in different states and silently invalidate every later case. |

Cases run **in order** against the same database and their effects accumulate,
so a KML case can set up the state a later KQL case queries.

## Normalization

Raw engine output is not comparable across implementations. Both runners apply
the identical transform before asserting, and getting this wrong in one runner
would silently weaken the whole suite — so it is specified here, not left to
each runner's judgement.

**1. Entity ids become positional tokens.**

Document ids depend on how many rows the engine created before this one, which
differs (the Rust engine seeds bootstrap capsules; a `$ConceptType` definition
consumes an id). Ids are therefore replaced in **first-appearance order** while
walking the value depth-first with object keys in sorted order:

```
"C:41"        -> "C:<1>"
"P:7:treats"  -> "P:<2>:treats"
```

The mapping is per case, and repeated occurrences of the same id map to the
same token — so *identity relationships* (this link's subject is that concept)
are still asserted, only the absolute numbers are not.

The **sorted key order matters** and is easy to get wrong by hand. In a
proposition link the keys are visited as `_type, attributes, id, metadata,
object, predicate, subject`, so the row's own `id` is numbered first, then
`object`, and `subject` last:

```jsonc
{ "id": "P:<1>:treats", "object": "C:<2>", "subject": "C:<3>" }
```

Do not hand-compute these. Write the case, run it, and copy the normalized
`actual` value out of the failure message — both runners print it in canonical
form for exactly this reason.

**2. Volatile engine metadata is dropped.**

`_created_at` and `_updated_at` are wall-clock timestamps. They are removed
from every `metadata` map at any depth.

`_version` is **kept**. It is semantically meaningful — `EXPECT VERSION`
depends on it — and dropping it would silence a whole class of bug.

**3. Unordered results are sorted.**

Unless the case sets `"ordered": true`, arrays are sorted by the canonical JSON
encoding of their elements (object keys sorted). Only the top-level result
array is sorted; nested arrays are data and keep their order.

Together these make the assertion "the same knowledge, shaped the same way",
which is what KIP actually specifies.

## Adding a fixture

1. Write the case against the semantics you want, not against whichever engine
   you happen to be looking at.
2. Run both suites:
   ```bash
   cargo test -p anda_cognitive_nexus --test conformance
   cd ts/kip-do && pnpm test
   ```
3. If they disagree, that is the finding. Either fix the engine, or add `skip`
   with a reason and record it under "Known divergences" in
   `ts/kip-do/README.md`.

Never adjust an expectation so that whichever engine you are working on turns
green. The fixture is the specification.
