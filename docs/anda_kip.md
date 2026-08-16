# `anda_kip` — Technical Reference

`anda_kip` is the protocol layer of the AndaDB workspace: it turns KIP 2.0
command text into a closed, executable AST, models the runtime envelope, and
defines the seam an engine implements. It holds no state and makes no storage
decisions.

- **Crate**: `rs/anda_kip`
- **Protocol**: KIP 2.0 (`SPECIFICATION.md` ships with the crate)
- **Reference implementation cross-check**: `@ldclabs/kip-lang`

## Contents

1. [Why KIP 2.0 is a different protocol](#1-why-kip-20-is-a-different-protocol)
2. [Crate layout](#2-crate-layout)
3. [The language at a glance](#3-the-language-at-a-glance)
4. [The executable AST](#4-the-executable-ast)
5. [Parser](#5-parser)
6. [What the parser rejects, and why](#6-what-the-parser-rejects-and-why)
7. [Errors](#7-errors)
8. [Runtime envelope](#8-runtime-envelope)
9. [Executor framework](#9-executor-framework)
10. [Core data model](#10-core-data-model)
11. [Cognitive Capsules](#11-cognitive-capsules)
12. [Function-calling integration](#12-function-calling-integration)
13. [Implementer's checklist](#13-implementers-checklist)
14. [`kip_cli`](#14-kip_cli)
15. [Migrating from `anda_kip` 0.11 (KIP 1.x)](#15-migrating-from-anda_kip-011-kip-1x)

---

## 1. Why KIP 2.0 is a different protocol

KIP 1.x stored a self-describing Concept/Proposition graph in which a
Proposition carried `metadata.confidence`, an `author` string and an
`access_level`. That single plane made four different questions look like one:

- what does this *mean*?
- who *claims* it, and how sure are they?
- what was actually *observed*?
- who is *allowed* to see it?

KIP 2.0 separates them, and everything in this crate follows from one line:

```text
a Proposition existing  ≠  the Proposition being true
```

| Element | What it is | Mutability |
| --- | --- | --- |
| **Concept** | a referable typed entity | mutable state |
| **Proposition** | truth-neutral `(subject, predicate, object)` | tuple immutable |
| **Assertion** | one actor's commitment about a Proposition | payload immutable; revise by supersession |
| **Evidence** | what was actually observed | payload immutable; correct via lineage |
| **Activity** | provenance for a process | immutable once terminal |

Belief is **projected** from Assertions under a named policy. It is never
stored, which is why `BELIEF` exists only in KQL — a Projection can never be a
mutation target or an export selector.

Distinctions the API deliberately keeps apart:

```text
missing            ≠ false          (open world: insufficient, not rejected)
search score       ≠ confidence
confidence         ≠ trust ≠ memory strength ≠ salience
name               ≠ identity
Principal (caller) ≠ semantic actor (whose claim it is)
cognitive content  ≠ authority
batch              ≠ transaction
timeout            ≠ abort
progress           ≠ commit
```

---

## 2. Crate layout

```text
rs/anda_kip/
├── src/
│   ├── lib.rs          re-exports, bundled prompts and tool schemas
│   ├── ast.rs          the executable AST (mirrors kip-lang's exec-ast.ts)
│   ├── parser.rs       entry points, input budget, quoting helpers
│   ├── parser/
│   │   ├── common.rs   lexical layer + rules shared by the three grammars
│   │   ├── kql.rs      FIND
│   │   ├── kml.rs      mutations, ASSERT desugaring, mutation guards
│   │   ├── meta.rs     DESCRIBE / LIST / SEARCH / VERIFY / … / EXPORT CAPSULE
│   │   └── json.rs     the model-friendly JSON dialect
│   ├── error.rs        Core Error Registry, categories, retry classes
│   ├── request.rs      request/response envelope
│   ├── types.rs        Core data model
│   ├── capsule.rs      Cognitive Capsules
│   ├── executor.rs     the engine seam
│   └── bin/kip_cli.rs  syntax checker
├── SPECIFICATION.md    the normative KIP 2.0 specification
├── KIPSyntax.md        LLM-facing syntax reference
├── SelfInstructions.md how an Agent should use its memory
└── SystemInstructions.md what a runtime owes its callers
```

---

## 3. The language at a glance

### 3.1 KQL — reads

```prolog
FIND(?drug.name, COUNT(DISTINCT ?trial))
WHERE {
    ?drug CONCEPT {type: "Drug"}
    (?drug, "studied_in", ?trial)
    OPTIONAL { ?edge STRUCTURAL (?drug, "has_step", ?step) }
    FILTER(?drug.attributes.risk_level < 3)
}
AS OF SEQ 4200            // which cognitive history
FOR TIME :world_time      // which world-valid time — an independent axis
WITH EPISTEMIC { explain: "summary" }
ORDER BY COUNT(?trial) DESC
LIMIT 10
CURSOR :page
```

Pattern families: `CONCEPT`, `PROPOSITION`, `ASSERTION`, `EVIDENCE`,
`ACTIVITY`, `STRUCTURAL`, `BELIEF`, `BELIEF SLOT`, plus `FILTER`, `NOT`,
`OPTIONAL`, `UNION`.

Raw reads and belief are different questions:

```prolog
// every claim ever made, truth-neutral
?p (:alice, "timezone", ?tz)

// what is believed now, and how contested it is
?b BELIEF (:alice, "timezone", ?tz)
?slot BELIEF SLOT (:alice, "timezone")
```

`BELIEF` never walks a raw path: alternation and hop quantifiers are traversal
syntax, and belief does not propagate along a traversal.

### 3.2 KML — mutations

```prolog
MUTATE {
    CREATE CONCEPT ?alice { TYPE "Person" CLIENT KEY "person:alice" NAME "Alice" }
    CREATE EVIDENCE ?msg  { SET FIELDS { evidence_class: "user_statement" } }
    ASSERT ?claim (?alice, "prefers", :dark_mode) {
        by: ?alice, mode: "stated", confidence: 0.9, evidence: ?msg
    }
    UPSERT CONCEPT ?drug {
        MATCH {key: "drug:aspirin"}
        EXPECT VERSION 3
        SET ATTRIBUTES { risk_level: 2 }
    }
}
```

Families: `CREATE CONCEPT` / `EVIDENCE` / `ASSERTION` / `ACTIVITY`,
`UPSERT CONCEPT`, `ENSURE PROPOSITION`, `ASSERT`, `UPDATE`, `RETRACT ASSERTION`,
`SUPERSEDE ASSERTION`, `CORRECT EVIDENCE`, `TRANSITION ACTIVITY`,
`SET RETENTION`, `ARCHIVE`, `TOMBSTONE`, `PURGE`, `MERGE CONCEPT`.

A statement written on its own is still a one-clause transaction;
`explicit_transaction` only records which spelling the source used.

### 3.3 META — grounding and introspection

```prolog
DESCRIBE PRIMER MODE "compact"
DESCRIBE TYPE "Person"
LIST SCHEMA PACKAGES STATUS "active" LIMIT 20
SEARCH COGNITION "dark mode" MODE "hybrid" THRESHOLD 0.7 LIMIT 5
HISTORY ELEMENT "C-1" FROM SEQ 1 TO SEQ 99
VERIFY CAPSULE :artifact
VALIDATE KML :command
PREVIEW IMPORT CAPSULE :capsule INTO "space-1"
EXPORT CAPSULE :out WHERE { ?c CONCEPT {type: "Experience"} } AS OF SEQ 7
```

META is semantically read-only. `VERIFY`, `VALIDATE`, `PREVIEW` and commit are
four different things and must not be collapsed.

---

## 4. The executable AST

`anda_kip::ast` is the *executable* tree, not a syntax tree. Every open-ended
grammar position is already closed:

- a predicate is `PredTerm::Atom` or `PredTerm::Path`, never a nested
  alternation/quantifier tree;
- a filter is a comparison, a logical node, a negation, or a call to one of the
  registered `FilterFunction`s;
- a variable is a name plus a path of `PathStep`s;
- `ASSERT` is gone — the parser desugared it.

A consumer matching on these enums is total.

### 4.1 Wire compatibility

The encoding is serde's default externally-tagged representation, matching
`exec-ast.ts` in `@ldclabs/kip-lang` field for field:

```rust
use anda_kip::{KipValue, Scalar, SymbolRef};

assert_eq!(serde_json::to_string(&KipValue::Null)?, r#""Null""#);
assert_eq!(serde_json::to_string(&Scalar::Param("limit".into()))?, r#"{"Param":"limit"}"#);
assert_eq!(serde_json::to_string(&SymbolRef::Name("has_step".into()))?, r#"{"Name":"has_step"}"#);
# Ok::<(), serde_json::Error>(())
```

That is what lets the Rust engine and the TypeScript toolkit be differentially
tested against the same conformance fixtures.

### 4.2 Value slots

| Type | Grammar position | Notes |
| --- | --- | --- |
| `KipValue` | a literal | `Null` is a unit variant |
| `Scalar` | `parameter \| literal` | an unbound `:name` survives as `Param` |
| `BoundValue` | `data_value` | collapses to `Value` when nothing needs binding |
| `MutationValue` | assignment right-hand side | adds `Expr` for `ADD`/`MUL`/`CLAMP`/`COALESCE` |
| `MatchValue` | `pattern_value` | inside an `ObjectMatcher` |
| `Term` | tuple endpoint | may itself be a Proposition |
| `SymbolRef` | `schema_symbol` | quoted name or parameter |
| `ElementRef` | `target_ref` | `Handle` / `Param` / `Id` |

One asymmetry worth knowing: in a **value** position a bare `?x` is a
`Handle` — it names an element the plan created. `?x.field` is a *read* of that
element's own field and keeps its path. In a **match** position `?x` is a
binding.

---

## 5. Parser

### 5.1 Entry points

```rust
use anda_kip::{parse_kip, parse_kql, parse_kml, parse_meta, parse_json};

let command = parse_kip(r#"DESCRIBE PRIMER"#)?;          // any surface
let query   = parse_kql(r#"FIND(?x) WHERE { ?x {a: 1} }"#)?;
let mutation = parse_kml(r#"ARCHIVE :old"#)?;
let meta    = parse_meta(r#"SNAPSHOT"#)?;
let value   = parse_json(r#"{ a: 1, /* not JSON5 */ }"#).is_err();
# Ok::<(), anda_kip::KipError>(())
```

Every entry point is all-consuming: one input is one command. Two commands in
one string is an error, not a silent "run the first one".

### 5.2 Lexical rules

- **Keywords are ASCII case-insensitive** (`find` == `FIND`), canonically
  uppercase.
- **Keywords are contextual, not reserved.** `by`, `mode`, `key`, `name`,
  `type` and `status` are all field names in the Spec's own examples, and
  `?a.lifecycle.status` is a legal dot path.
- **`true` / `false` / `null` are case-sensitive** — they are JSON literals, not
  protocol keywords. So is `id` in `(id: "P-1")`.
- Line comments start with `//` and count as a token separator.
- Whitespace is insignificant between tokens.

### 5.3 Input budget

```rust
use anda_kip::{MAX_KIP_INPUT_LEN, MAX_KIP_NESTING_DEPTH, MAX_KIP_BATCH_COMMANDS};
```

Length and bracket depth are checked by a pre-scan before any parsing work, so a
hostile input cannot exhaust the stack. The pre-scan skips string literals and
line comments exactly as the parser does — otherwise a `"` inside a comment
would latch string mode and every bracket after it would go uncounted.

### 5.4 Error reporting

Parse failures carry a line, a column and what was expected:

```text
InvalidSyntax: at line 3, column 12: expected a literal or a :parameter, found "}"
```

---

## 6. What the parser rejects, and why

These are schema-independent rules, so they are enforced here rather than left
for an engine to discover. Each one exists because the alternative is silent
corruption of the epistemic record.

| Rejected | Reason |
| --- | --- |
| `UPSERT CONCEPT ?c { MATCH {name: "Alice"} }` | `name` is mutable and duplicable; "the Concept named X" can silently address a different node over time. Match on `id` or `key`. |
| `UPDATE ?a SET FIELDS {confidence: …}` where `?a` is an Assertion | Epistemic payload is immutable. Record a new Assertion with `SUPERSEDING`. |
| `UPDATE ?e SET FIELDS {payload: …}` where `?e` is Evidence | Use `CORRECT EVIDENCE :old BY :new`. |
| `UPDATE ?p SET FIELDS {subject: …}` where `?p` is a Proposition | A different tuple is a different Proposition. |
| `SET STRUCTURAL` on an Assertion / Evidence / Activity | Record topology is immutable payload; a pending Activity finalizes through `TRANSITION ACTIVITY`. |
| Any assignment naming `_system`, `governance`, `space_id`, `space_seq` | Engine-owned state; external cognition cannot self-escalate authority. |
| `ENSURE PROPOSITION (id: "P-1")` | `(id: …)` is match-only — no structure can be created from an id. |
| `ASSERT` without `by` or without `mode` | Guessing the actor forges attribution; guessing the mode turns hearsay into observation. |
| `BELIEF` in KML or in `EXPORT CAPSULE` | A Projection is virtual and read-only. |
| `BELIEF (?s, "a"\|"b", ?o)` | Belief does not propagate along a raw path. |
| `ADD(?other.n, 1)` in an `UPDATE` on `?c` | An update expression may read only the element being updated; otherwise the result depends on a join the statement never declared. |
| Two clauses claiming the same `?handle` | Every forward reference to it would be ambiguous. |
| A `?handle` bound by neither the plan nor the clause's `WHERE` | `ReferenceError` rather than a dangling write. |
| `PURGE … CONFIRM "purge"` | The confirmation spelling is frozen. |
| Duplicate keys in any block | A duplicate key is almost always a generation slip; last-write-wins would hide it. |

### 6.1 The `ASSERT` desugaring

`ASSERT` is normative sugar (§55.1), and the parser expands it to exactly what
it is defined as — nothing more is fabricated:

```prolog
ASSERT ?a (:alice, "prefers", :dark_mode) {
    by: :alice, mode: "stated", confidence: 0.9, evidence: [:e1, :e2]
} SUPERSEDING :old
```

becomes three clauses:

1. `EnsureProposition { handle: "a#proposition", … }`
2. `CreateAssertion { handle: "a", set_fields: [proposition, asserted_by, mode,
   stance, confidence], set_structural: [("evidence", e1){role}, ("evidence", e2){role}] }`
3. `SupersedeAssertion { target: :old, by: Handle("a") }`

Details that matter:

- `stance` defaults to `"support"` and is **materialized**, not left for the
  engine to re-derive;
- `evidence` is a reserved *structural* field, so an array cites one
  role-qualified edge per artifact;
- a handle-less `ASSERT` gets a synthetic handle `#assert{N}` keyed on its
  clause position, so two of them in one `MUTATE` cannot collide (`#` cannot
  appear in a KIP identifier, so it cannot collide with a user handle either).

---

## 7. Errors

KIP 2.0 replaces 1.x's numeric `KIP_xxxx` codes with a registry of stable names.

```rust
use anda_kip::{ErrorCategory, KipError, KipErrorCode, RetryClass};

let err = KipError::version_conflict("element changed since you read it");
assert_eq!(err.name(), "VersionConflict");
assert_eq!(err.category(), ErrorCategory::Transaction);
assert_eq!(err.retry_class(), RetryClass::RequiresRefresh);
assert!(err.effective_hint().contains("EXPECT VERSION"));
```

The wire shape (§86.1):

```json
{
  "code": "SchemaSymbolAmbiguous",
  "category": "schema",
  "message": "…",
  "hint": "…",
  "retry": {"class": "requires_different_input"},
  "details": {}
}
```

Retry classes are a contract with the caller's recovery logic:

| Class | Meaning |
| --- | --- |
| `safe_same_request` | nothing durable happened; resend as-is |
| `requires_refresh` | re-read current state, then retry |
| `requires_different_input` | the request must change |
| `requires_authority` | the caller lacks authority, not information |
| `requires_new_snapshot` | acquire a fresh coordinate |
| `requires_reacquire_artifact` | re-stage the bytes |
| `outcome_lookup_required` | the write's fate is undecided — **do not re-issue** |
| `non_retryable` | retrying cannot help |

`KipErrorCode::ALL` enumerates all 79 registry codes; `KipErrorCode::from_name`
parses one back. Use `NotFoundOrNotVisible` wherever distinguishing "absent"
from "forbidden" would leak protected existence.

---

## 8. Runtime envelope

### 8.1 Request

```rust
use anda_kip::{Execution, ExecutionMode, Operation, Request};

let request = Request {
    request_id: Some("req-1".into()),
    execution: Some(Execution {
        idempotency_key: Some("logical-write-key".into()),
        ..Execution::new(ExecutionMode::Atomic)
    }),
    operations: vec![Operation::new(r#"ARCHIVE :old"#).with_op_id("op-1")],
    ..Default::default()
};
request.validate()?;
# Ok::<(), anda_kip::KipError>(())
```

`Request::validate` enforces the envelope invariants that need no engine:
protocol version, a non-empty and bounded `operations[]`, a declared
`execution.mode` whenever there is more than one operation, `atomic` never
paired with `on_error: continue`, unique `op_id`s, exactly one of
`command`/`ast` per operation, well-formed parameter and ingest binding names.

### 8.2 Execution modes

| Mode | Guarantee |
| --- | --- |
| `independent` | separate snapshots and transactions; failures isolated |
| `sequence` | ordered; each state change commits separately and **earlier commits are not rolled back** |
| `atomic` | one transaction, one snapshot, read-your-writes, all-or-none |

A batch is not a transaction unless the mode says `atomic`. A `sequence` run
that fails partway is reported as `partial`, never `failed` — a caller that
reads it as a total failure will re-issue writes that already landed.

### 8.3 Ingestion

Observed material should reach Evidence from the transport envelope, not
through model-generated command text:

```json
{
  "kip": "2.0",
  "ingest": {"evidence": [{
    "key": "msg",
    "evidence_class": "user_statement",
    "payload": "I prefer dark mode.",
    "observed_at": "2026-08-14T01:00:00Z"
  }]},
  "operations": [{
    "language": "KML",
    "command": "ASSERT (:alice, \"prefers\", :dark_mode) { by: :alice, mode: \"stated\", evidence: :msg }"
  }]
}
```

Each `key` binds as a parameter naming the minted Evidence. Minting is
transactional: if the transaction aborts, no Evidence is durably created.

### 8.4 Response

```rust
use anda_kip::{OperationResult, Response, TopLevelStatus};

let response = Response::from_results(vec![OperationResult::ok(serde_json::json!({"n": 1}))]);
assert_eq!(response.status, TopLevelStatus::Succeeded);
```

Top-level status is `succeeded` / `failed` / `partial` / `outcome_unknown`;
operation status is `succeeded` / `failed` / `skipped` / `rolled_back` /
`no_effect`. A `Receipt` carries `tx_id`, `space_seq`, digests and proofs;
`ResultContext.search` reports `index_seq` against `current_space_seq` so a
lagging index is never passed off as snapshot-consistent.

---

## 9. Executor framework

```rust
use anda_kip::{Command, Executor, Operation, Request, Response};
use async_trait::async_trait;

struct MyNexus;

#[async_trait]
impl Executor for MyNexus {
    async fn execute(
        &self,
        command: Command,
        request: &Request,
        operation: &Operation,
    ) -> Response {
        let dry_run = request.is_dry_run();
        let _operation_parameters = &operation.parameters;
        match command {
            Command::Kql(_query) => todo!("run the read"),
            Command::Kml(_statement) => todo!("run the transaction"),
            Command::Meta(_command) => todo!("answer the introspection"),
        }
    }
}
```

Helpers:

- `execute_kip(executor, text, dry_run)` — parse, classify, execute;
- `execute_readonly(executor, text, dry_run)` — the same, but rejects
  state-changing semantics by *what the command is*, never by a declared label;
- `execute_request(executor, &request)` — runs `independent` and `sequence`.
  `atomic` is deliberately refused rather than emulated: one transaction, one
  snapshot and all-or-none commit are engine properties, and a loop over an
  `Executor` cannot provide them. The executor receives the complete request
  and operation context, and must honor every applicable envelope field or
  fail explicitly.

---

## 10. Core data model

`anda_kip::types` models the element envelope and the Core kinds:
`Concept`, `Proposition`, `Assertion`, `Evidence`, `Activity`, each carrying an
`ElementEnvelope` with `governance`, `retention`, `facets` and `_system`.

There is **no universal author-writable metadata bag**. Data goes where it
belongs:

```text
semantic payload       → typed fields / attributes
epistemic state        → Assertion
observations           → Evidence
provenance             → Activity / _system.origin
governance             → Governance state
storage lifecycle      → retention
mnemonic/profile state → Facets
engine truth           → _system
```

Vocabularies: `Stance` (`support`/`reject`/`uncertain`), `AssertionMode`
(`observed`/`stated`/`inferred`/`predicted`/`hypothetical`/`imported`),
`AssertionStatus`, `BeliefStatus`
(`accepted`/`rejected`/`contested`/`uncertain`/`insufficient`), plus the
recommended `EVIDENCE_CLASSES` and `ACTIVITY_CLASSES` lists.

`BeliefStatus::is_decided()` is `true` only for `accepted` and `rejected`.
`insufficient` is the open-world unknown and must never be read as "no".

---

## 11. Cognitive Capsules

```text
Capsule bytes  ≠  destination mutation authority
```

`anda_kip::capsule` models the artifact frame — manifest, source, schema
dependencies, records, external refs, blobs, handling, integrity — and the
import vocabulary. Record payloads stay as JSON because which fields a record
has is the active Schema Packages' decision, validated by the destination.

`Capsule::validate_frame` is the cheap structural gate: format, a content
digest, and `base_seq`/`target_seq` on a delta Capsule. It is not
`VALIDATE CAPSULE`, which needs an engine and a destination Space.

`ImportMode` is `preview` / `isolate` / `merge` / `restore`, and
`ImportMode::may_map_self()` is true only for `restore`: source `$self` must
never silently become destination `$self`. `IdentityResolution::ORDER` encodes
the conservative resolution sequence, ending in "create new".
`ExternalRefKind` keeps `redacted` (the source withheld it) distinguishable
from `unavailable` (the source does not have it).

---

## 12. Function-calling integration

```rust
use anda_kip::{KIP_FUNCTION_DEFINITION, KIP_READONLY_FUNCTION_DEFINITION};

let write_tool = KIP_FUNCTION_DEFINITION.clone();      // execute_kip
let read_tool  = KIP_READONLY_FUNCTION_DEFINITION.clone(); // execute_kip_readonly
```

Also bundled, for system prompts:

- `KIP_SYNTAX` — the LLM-facing syntax reference;
- `SELF_INSTRUCTIONS` — how an Agent should use its own memory;
- `SYSTEM_INSTRUCTIONS` — what a runtime owes its callers.

---

## 13. Implementer's checklist

Protocol invariants an executor must uphold; `anda_kip` enforces none of them
for you because each needs state.

**Identity**
- engine-assigned, opaque, never-reused local `id`s
- `key` immutable and unique per `(space_id, schema_ref, key)`
- `client_key` conflicts → `ClientKeyConflict`
- `MERGE CONCEPT` is non-destructive; raw historical references keep resolving

**Epistemics**
- Proposition tuples immutable; one canonical Proposition per semantic tuple
- Assertion payload immutable; revision is a new Assertion + supersession
- `RETRACT` only by the assertor or an authorized representative
- never produce `rejected` merely because support is absent
- N copies of one message are one evidential basis, not N

**Governance**
- deny overrides; protocol invariants override policy
- `_system`, `governance`, `space_id`, `space_seq` never author-writable
- `NotFoundOrNotVisible` where existence itself is protected
- watch aggregate leakage: a `COUNT` over invisible elements is disclosure

**Transactions**
- `atomic` means one `tx_id`, one snapshot, one state-changing `space_seq`
- honour `EXPECT VERSION` / `EXPECT STATE` at commit, not just at plan time
- idempotency keys scoped by Space and authority; same key + different request
  → `IdempotencyConflict`
- when the outcome cannot be established, answer `outcome_unknown`

**Reads**
- `AS OF` and `FOR TIME` are independent axes
- historical reads use the historical Schema, or fail with
  `HistoricalSchemaUnavailable`
- cursors unforgeable, and invalidated rather than silently re-anchored
- report SEARCH index lag instead of implying snapshot consistency

**Bounded mutation**
- `UPDATE`, `RETRACT`, `SET RETENTION`, `ARCHIVE`, `TOMBSTONE` and `PURGE`
  accept a `LIMIT`; an unbounded selection should be refused, not guessed at
- no destructive cascade by default

---

## 14. `kip_cli`

```bash
cargo run -p anda_kip --bin kip_cli -- path/to/file.kip path/to/dir
```

Walks files and directories, parses every `.kip` file, prints the classified
language on success and the error plus its recovery hint on failure. Exits
non-zero if anything failed.

---

## 15. Migrating from `anda_kip` 0.11 (KIP 1.x)

This is a breaking rewrite. The 1.x API is gone rather than deprecated, because
the semantics behind it are gone.

| 0.11 (KIP 1.x) | 0.12 (KIP 2.0) |
| --- | --- |
| `UPSERT { CONCEPT ?c {…} }` | `CREATE CONCEPT` / `UPSERT CONCEPT` / `ASSERT` |
| `DELETE` | `ARCHIVE` / `TOMBSTONE` / `PURGE` / `RETRACT` — classify the intent |
| `metadata.confidence` on a Proposition | `Assertion.confidence`; a Proposition has no confidence |
| `metadata.author` | `Assertion.asserted_by` (semantic actor) and `_system.origin` (engine truth) — different things |
| `access_level` | Governance classification and policy |
| numeric `KIP_3002` | named `KipErrorCode::NotFoundOrNotVisible` with category and retry class |
| `Response::Ok/Err` enum | `Response` struct with `status` + `results[]` |
| `ConceptNode` / `PropositionLink` | `Concept` / `Proposition` / `Assertion` / `Evidence` / `Activity` |
| genesis `.kip` capsules (`GENESIS_KIP`, …) | removed — Schema is immutable Package state, not graph nodes |
| `capsule.rs` = bootstrap sources | `capsule.rs` = portable Cognitive Capsules |

Data migration is semantic decomposition, not a field rename: a legacy fact
Proposition becomes a Proposition **plus** a migrated positive Assertion, and
ambiguous legacy values (a decayed `confidence`, a bare `author` string) must be
preserved as explicitly legacy rather than reinterpreted. The normative guidance
is in `SPECIFICATION.md` §103 and the KIP repository's
`v2/migration/KIP-2.0-Migration-from-1.x.md`.
