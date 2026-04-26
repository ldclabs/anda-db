# `anda_kip` — Technical Reference

> A pure-Rust SDK for the [**Knowledge Interaction Protocol (KIP)**](https://github.com/ldclabs/kip) — the
> declarative protocol that lets Large Language Models query, evolve and
> introspect a long-lived knowledge graph (the *Cognitive Nexus*).

|                       |                                                                                                                                                                    |
| :-------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Crate                 | [`anda_kip`](../rs/anda_kip/)                                                                                                                                      |
| Version               | `0.7.x`                                                                                                                                                            |
| Spec                  | KIP **v1.0-RC6** ([SPECIFICATION.md](../rs/anda_kip/SPECIFICATION.md))                                                                                             |
| Reference executor    | [`anda_cognitive_nexus`](../rs/anda_cognitive_nexus/) (graph store backed by [Anda DB](../rs/anda_db/))                                                            |
| Other implementations | [`anda_cognitive_nexus_server`](../rs/anda_cognitive_nexus_server/) (HTTP/JSON-RPC), [`anda_cognitive_nexus_py`](../py/anda_cognitive_nexus_py/) (Python bindings) |
| Status                | Library is feature-complete for KIP v1.0-RC6 (parser + AST + request/response + executor trait). Bundled `kip_cli` provides an interactive REPL.                   |

---

## Contents

- [`anda_kip` — Technical Reference](#anda_kip--technical-reference)
  - [Contents](#contents)
  - [1. Overview](#1-overview)
    - [1.1 Why KIP?](#11-why-kip)
    - [1.2 What this crate provides](#12-what-this-crate-provides)
    - [1.3 Quick start](#13-quick-start)
  - [2. Crate layout](#2-crate-layout)
  - [3. The KIP language at a glance](#3-the-kip-language-at-a-glance)
    - [3.1 KQL — Knowledge Query Language](#31-kql--knowledge-query-language)
    - [3.2 KML — Knowledge Manipulation Language](#32-kml--knowledge-manipulation-language)
    - [3.3 META — schema introspection](#33-meta--schema-introspection)
  - [4. AST (`anda_kip::ast`)](#4-ast-anda_kipast)
    - [4.1 Top-level](#41-top-level)
    - [4.2 KQL nodes](#42-kql-nodes)
    - [4.3 KML nodes](#43-kml-nodes)
    - [4.4 META nodes](#44-meta-nodes)
    - [4.5 Value / `Json` aliases](#45-value--json-aliases)
  - [5. Parser (`anda_kip::parser`)](#5-parser-anda_kipparser)
    - [5.1 Public entry points](#51-public-entry-points)
    - [5.2 Whitespace and keywords](#52-whitespace-and-keywords)
    - [5.3 Error reporting](#53-error-reporting)
  - [6. Executor framework (`anda_kip::executor`)](#6-executor-framework-anda_kipexecutor)
    - [6.1 The `Executor` trait](#61-the-executor-trait)
    - [6.2 Convenience helpers](#62-convenience-helpers)
    - [6.3 Implementing a backend](#63-implementing-a-backend)
  - [7. Request / Response (`anda_kip::request`)](#7-request--response-anda_kiprequest)
    - [7.1 `Request`](#71-request)
    - [7.2 `Request::execute`](#72-requestexecute)
    - [7.3 `Response`](#73-response)
    - [7.4 End-to-end](#74-end-to-end)
  - [8. Errors (`anda_kip::error`)](#8-errors-anda_kiperror)
  - [9. Genesis capsules (`anda_kip::capsule`)](#9-genesis-capsules-anda_kipcapsule)
  - [10. Entity types (`anda_kip::types`)](#10-entity-types-anda_kiptypes)
  - [11. Function-calling integration](#11-function-calling-integration)
  - [12. Executor implementer's checklist (RC6 semantics)](#12-executor-implementers-checklist-rc6-semantics)
  - [13. Cookbook](#13-cookbook)
    - [13.1 Parse and inspect](#131-parse-and-inspect)
    - [13.2 Single command via `execute_kip`](#132-single-command-via-execute_kip)
    - [13.3 Read-only sandbox](#133-read-only-sandbox)
    - [13.4 Batch with shared and per-item parameters](#134-batch-with-shared-and-per-item-parameters)
    - [13.5 Implementing an executor](#135-implementing-an-executor)
    - [13.6 Building the function-calling tool definition](#136-building-the-function-calling-tool-definition)
  - [14. `kip_cli`](#14-kip_cli)
  - [15. Compatibility notes](#15-compatibility-notes)

---

## 1. Overview

### 1.1 Why KIP?

LLMs are stateless and probabilistic; production agents need a **persistent,
deterministic, traceable** memory. KIP closes that gap by exposing a
graph-shaped knowledge store through a **declarative, model-friendly DSL** and
a **JSON request/response envelope** that maps cleanly onto LLM
function-calling.

A KIP backend (a *Cognitive Nexus*) stores two kinds of entity:

- **Concept node** — `{type, name}` plus `attributes` and `metadata`.
- **Proposition link** — a triple `(subject, predicate, object)` plus
  `attributes` and `metadata`. `subject` / `object` may themselves reference
  another proposition link, enabling *higher-order* facts (provenance,
  beliefs, meta-claims).

KIP defines three instruction families:

| Family   | Purpose                          | Statements                                                                |
| :------- | :------------------------------- | :------------------------------------------------------------------------ |
| **KQL**  | Knowledge retrieval & reasoning  | `FIND … WHERE { … } [ORDER BY] [LIMIT] [CURSOR]`                          |
| **KML**  | Knowledge evolution / writes     | `UPSERT { … }`, `DELETE CONCEPT/PROPOSITIONS/ATTRIBUTES/METADATA … `      |
| **META** | Schema introspection & grounding | `DESCRIBE PRIMER/DOMAINS/CONCEPT TYPE[S]/PROPOSITION TYPE[S]`, `SEARCH …` |

### 1.2 What this crate provides

`anda_kip` is the **protocol-only** layer (no storage, no I/O):

- A complete, RC6-compliant **parser** (`nom` + `nom-language`, no regex).
- A strongly-typed **AST** for every KIP construct.
- A small **`Executor` trait** (`async fn execute(Command, dry_run) -> Response`)
  that any backend can implement.
- A standardized **`Request` / `Response`** envelope including parameter
  substitution (`:name` placeholders) and **batch execution semantics**.
- KIP **standard error codes** (`KIP_1xxx`–`KIP_4xxx`) with recovery hints.
- The canonical **Genesis capsules** (`$ConceptType`, `$PropositionType`,
  `Domain`, `Person`, `Event`, `Insight`, `Preference`, `SleepTask`, `$self`,
  `$system`).
- LLM-facing **function-calling JSON schemas** for `execute_kip` and
  `execute_kip_readonly`.
- An interactive REPL: `kip_cli` (built from the bundled `bin/kip_cli.rs`).

### 1.3 Quick start

```toml
[dependencies]
anda_kip = "0.7"
```

```rust
use anda_kip::{parse_kip, Command, KqlQuery};

let cmd: Command = parse_kip(r#"
    FIND(?drug.name, ?drug.attributes.risk_level)
    WHERE {
        ?drug   {type: "Drug"}
        ?head   {name: "Headache"}
        (?drug, "treats", ?head)
        FILTER(?drug.attributes.risk_level < 3)
    }
    ORDER BY ?drug.attributes.risk_level ASC
    LIMIT 10
"#)?;

assert!(matches!(cmd, Command::Kql(KqlQuery { .. })));
# Ok::<_, anda_kip::KipError>(())
```

Parsing is pure and synchronous; execution is delegated to whatever
[`Executor`](#6-executor-framework-anda_kipexecutor) you wire up.

---

## 2. Crate layout

```text
rs/anda_kip/src/
├── lib.rs            # Public re-exports + LLM function-calling schema constants
├── ast.rs            # Strongly-typed AST for KQL / KML / META
├── capsule.rs        # Bootstrapping capsules and reserved-name constants
├── error.rs          # KipError, KipErrorCode (KIP_1xxx–4xxx)
├── executor.rs       # Executor trait + execute_kip / execute_readonly
├── parser.rs         # parse_kip / parse_kql / parse_kml / parse_meta / parse_json
├── parser/
│   ├── common.rs     # Shared combinators (identifier, ws, keyword/keywords, …)
│   ├── json.rs       # JSON value parser (Value, Object, Array, …)
│   ├── kql.rs        # FIND / WHERE / FILTER / ORDER BY / LIMIT / CURSOR
│   ├── kml.rs        # UPSERT / DELETE / SET ATTRIBUTES / SET PROPOSITIONS
│   └── meta.rs       # DESCRIBE / SEARCH
├── request.rs        # Request / CommandItem / Response / ErrorObject
├── types.rs          # Entity / ConceptNode / PropositionLink (+ borrowed refs)
└── bin/kip_cli.rs    # Interactive REPL using anda_cognitive_nexus
```

Bundled assets:

- `capsules/*.kip` — KIP source for the Genesis capsules and standard types.
- `FunctionDefinition.json`, `FunctionDefinitionReadonly.json` — JSON schema
  for the OpenAI / generic function-calling envelope.
- `SystemInstructions.md`, `SelfInstructions.md` — prompt scaffolding for
  agents calling KIP.
- `SPECIFICATION.md`, `KIPSyntax.md` — the normative spec and a condensed
  syntax cheat sheet.

---

## 3. The KIP language at a glance

This section is a **fast tour** for readers who already write SQL / Cypher /
SPARQL. The full grammar is in
[`KIPSyntax.md`](../rs/anda_kip/KIPSyntax.md); the normative semantics live in
[`SPECIFICATION.md`](../rs/anda_kip/SPECIFICATION.md).

### 3.1 KQL — Knowledge Query Language

```kip
FIND( ?drug.name, COUNT(?symptom) )
WHERE {
    ?drug   {type: "Drug"}
    ?symptom {type: "Symptom"}
    (?drug, "treats", ?symptom)
    FILTER( ?drug.attributes.risk_level < 3 )
}
ORDER BY COUNT(?symptom) DESC
LIMIT 20
CURSOR "<opaque-token>"
```

| Construct                             | Notes                                                                                                                         |
| :------------------------------------ | :---------------------------------------------------------------------------------------------------------------------------- |
| `FIND( … )`                           | Projection list. Variables, dot-paths (`?x.attributes.k`), and aggregates (`COUNT`/`COUNT_DISTINCT`/`SUM`/`AVG`/`MIN`/`MAX`). |
| Concept matcher `?v {type, name, id}` | Either `{id: "C:…"}`, `{type, name}`, `{type}`, or `{name}`.                                                                  |
| Proposition matcher `(?s, "p", ?o)`   | Either `(id: "P:…")` or `(subject, predicate, object)`. Subject and object may themselves be matchers — *higher-order* facts. |
| Predicate term                        | Literal `"treats"`, alternative `"treats" \| "cures"`, or **multi-hop** `"follows"{1,3}` (path operator).                     |
| `OPTIONAL { … }`                      | Left-join semantics; missing variables project as `null`.                                                                     |
| `NOT { … }`                           | Negation-as-failure; internal variable bindings stay private.                                                                 |
| `UNION { … } UNION { … }`             | Row-wise union; branches have **independent** scopes; missing columns are filled with `null`.                                 |
| `FILTER( expr )`                      | Comparisons, arithmetic, `&&`/`\|\|`/`!`, `CONTAINS`, `STARTS_WITH`, `ENDS_WITH`, `REGEX`, `IN`, `IS_NULL`, `IS_NOT_NULL`.    |
| Pagination                            | `LIMIT n` (cap row count) and `CURSOR "<token>"` (resumable).                                                                 |
| Aggregation                           | When `FIND` mixes plain variables with aggregates, the plain variables form an **implicit `GROUP BY`** key.                   |
| Path operator zero-hop                | `"p"{0,n}` includes the reflexive case `?s == ?o`.                                                                            |

### 3.2 KML — Knowledge Manipulation Language

```kip
UPSERT {
    CONCEPT ?aspirin {
        {type: "Drug", name: "Aspirin"}              // match-or-create
        SET ATTRIBUTES { risk_level: 1, formula: "C9H8O4" }
        SET PROPOSITIONS {
            ("treats",        {type: "Symptom", name: "Headache"})
            ("belongs_to",    ?nsaid_class) WITH METADATA { source: "PubMed:1" }
        }
    }
    CONCEPT ?nsaid_class {
        {type: "DrugClass", name: "NSAID"}
    }
    PROPOSITION ?claim {
        ({type: "Person", name: "$self"}, "stated",
         ({type: "Drug", name: "Aspirin"}, "treats", {type: "Symptom", name: "Headache"}))
    }
} WITH METADATA { source: "agent_v1", confidence: 0.95 }
```

Key rules:

1. **Sequential, top-to-bottom**; handles must be defined before use; the
   dependency graph must be a DAG.
2. **`SET ATTRIBUTES`** does shallow merge; **`SET PROPOSITIONS`** is
   *additive* (it never deletes unspecified links).
3. `WITH METADATA` precedence: per-link override > inner block override >
   outer `UPSERT { … } WITH METADATA { … }` default.
4. `DELETE CONCEPT ?x DETACH WHERE { … }` removes the concept **and
   transitively** every proposition that references it (directly or via
   higher-order chains).
5. Modifying or deleting the *protected scope* (`$ConceptType`,
   `$PropositionType`, `$self`, `$system`, core domains like `CoreSchema`)
   returns `KIP_3004 ImmutableTarget`.

### 3.3 META — schema introspection

```kip
DESCRIBE PRIMER                        -- agent self-summary + domain map
DESCRIBE DOMAINS                       -- list of Domain concepts
DESCRIBE CONCEPT TYPES                 -- all concept type names
DESCRIBE CONCEPT TYPE "Drug"           -- single type definition
DESCRIBE PROPOSITION TYPES
DESCRIBE PROPOSITION TYPE "treats"
SEARCH CONCEPT "antiinflammatory" WITH TYPE "Drug" LIMIT 10
SEARCH PROPOSITION "treats" LIMIT 5
```

---

## 4. AST (`anda_kip::ast`)

The AST is the lossless internal representation produced by the parser.
Every node implements `Clone + Debug + Serialize + Deserialize + PartialEq`.

### 4.1 Top-level

| Type           | Variants / fields                                            |
| :------------- | :----------------------------------------------------------- |
| `Command`      | `Kql(KqlQuery)`, `Kml(KmlStatement)`, `Meta(MetaCommand)`    |
| `CommandType`  | `Kql`, `Kml`, `Meta`, `Unknown` (used by `Request::execute`) |
| `KqlQuery`     | `find`, `where_clauses`, `order_by`, `limit`, `cursor`       |
| `KmlStatement` | `Upsert(Vec<UpsertBlock>)` \| `Delete(DeleteStatement)`      |
| `MetaCommand`  | `Describe(DescribeCommand)` \| `Search(SearchCommand)`       |

### 4.2 KQL nodes

- `FindClause` — list of `FindExpression`s.
- `FindExpression::Variable(DotPathVar)` and `FindExpression::Aggregation { function, distinct, expression }`.
- `AggregationFunction` — `Count | Sum | Avg | Min | Max`.
- `WhereClause` —
  - `Concept(ConceptClause)` `?x {…}`
  - `Proposition(PropositionClause)` `?l (s, p, o)` or `(id: "…")`
  - `Filter(FilterClause)`
  - `Optional(Vec<WhereClause>)`
  - `Not(Vec<WhereClause>)`
  - `Union(Vec<Vec<WhereClause>>)`
- `ConceptMatcher` — `ID(String)`, `Type(String)`, `Name(String)`,
  `Object { type, name }`.
- `PropositionMatcher` — `ID(String)` or `Object { subject, predicate, object }`.
- `TargetTerm` — `Variable(String)`, `Concept(ConceptMatcher)`,
  `Proposition(Box<PropositionMatcher>)`.
- `PredTerm` —
  - `Literal(String)`,
  - `Variable(String)`,
  - `Alternative(Vec<String>)` (`pred1 | pred2`),
  - `MultiHop { predicates, min: u16, max: u16 }`. Zero-hop is encoded as
    `min == 0` and is bound by the executor (RC6 §3.3).
- `FilterExpression`, `FilterOperand`, `ComparisonOperator`, `LogicalOperator`,
  `FilterFunction` (`Contains | StartsWith | EndsWith | Regex | In | IsNull | IsNotNull`).
- `OrderClause`, `OrderDirection`.

### 4.3 KML nodes

- `UpsertBlock { items: Vec<UpsertItem>, metadata: Option<Map> }`.
- `UpsertItem::Concept(ConceptBlock)` / `::Proposition(PropositionBlock)`.
- `ConceptBlock { handle, matcher, attributes, propositions, metadata }`.
- `PropositionBlock { handle, proposition, attributes, metadata }`.
- `DeleteStatement` —
  - `DeleteAttributes { attributes, target, where_clauses }`,
  - `DeleteMetadata { keys, target, where_clauses }`,
  - `DeletePropositions { target, where_clauses }`,
  - `DeleteConcept { target, where_clauses }` (RC6 mandates `DETACH`).

### 4.4 META nodes

- `DescribeCommand::Primer | Domains | ConceptTypes { limit, cursor }
  | ConceptType(String) | PropositionTypes { limit, cursor } | PropositionType(String)`.
- `SearchCommand { target: SearchTarget, term, with_type, limit }` where
  `SearchTarget = Concept | Proposition`.

### 4.5 Value / `Json` aliases

- `pub type Json = serde_json::Value;` — used for free-form attribute /
  metadata payloads.
- `pub use serde_json::{Map, Number};`.
- `Value` — restricted primitive type used in identifiers, parameters, and
  filter literals (`Null | Bool | Number | String`). Implements `From<&str>`,
  `From<bool>`, `From<Number>`, `TryFrom<Json>`, plus `into_opt_string` /
  `into_opt_number` / `into_opt_bool` / `is_null` accessors.

---

## 5. Parser (`anda_kip::parser`)

The parser is a single hand-written `nom` pipeline. There is no separate
lexer: tokens are matched directly inside combinators, which keeps error
messages source-anchored.

### 5.1 Public entry points

| Function                                        | Returns                               | Notes                                         |
| :---------------------------------------------- | :------------------------------------ | :-------------------------------------------- |
| `parse_kip(&str) -> Result<Command, KipError>`  | `Command` (auto-detects KQL/KML/META) | Used by `execute_kip` / `Request::execute`.   |
| `parse_kql(&str) -> Result<KqlQuery, KipError>` | strict KQL                            | Useful when the input is known to be a query. |
| `parse_kml(&str) -> Result<KmlStatement, _>`    | strict KML                            | Used internally to load Genesis capsules.     |
| `parse_meta(&str) -> Result<MetaCommand, _>`    | strict META                           |                                               |
| `parse_json(&str) -> Result<Json, _>`           | a single JSON value                   | Useful for parameter payloads.                |
| `quote_str(&str) -> String`                     | JSON-escaped quoted form              | Re-uses `serde_json` escaping rules.          |
| `unquote_str(&str) -> Option<String>`           | inverse of `quote_str`                | Returns `None` if not a valid JSON string.    |

All entry points enforce `all_consuming(ws(...))` — trailing garbage is
rejected as `KIP_1001 InvalidSyntax`.

### 5.2 Whitespace and keywords

The parser treats *any* Unicode whitespace as a separator between tokens.
The two combinators worth knowing about (in `parser::common`) are:

- `keyword(&'static str)` — match a single keyword followed by **at least
  one** whitespace char; rejects `tag("WORD ")` style ambiguity around
  newlines and tabs.
- `keywords(&'static [&'static str])` — match a sequence of keywords with
  required whitespace **between** tokens but not after the last one. Wrap
  with `ws(...)` if trailing whitespace is also expected.

This is what makes commands like the following parse identically:

```kip
ORDER          BY ?x ASC
ORDER
  BY ?x ASC
ORDER\tBY ?x ASC
```

### 5.3 Error reporting

`parse_kip` emits `KipError { code: KIP_1001 (or 1002), message }`. The
message is produced by `format_nom_error`, which:

- preserves the offending source slice,
- includes the `nom_language::VerboseError` context stack (top-most context
  first),
- highlights identifier-rule violations as `KIP_1002 InvalidIdentifier` so
  the LLM can self-correct without ambiguity.

---

## 6. Executor framework (`anda_kip::executor`)

### 6.1 The `Executor` trait

```rust
#[async_trait::async_trait]
pub trait Executor: Send + Sync {
    async fn execute(&self, command: Command, dry_run: bool) -> Response;
}
```

That's it. The trait is intentionally minimal — the rich plumbing is in
`Request` / `Response`. `Executor` is implemented out of the box for
`Box<dyn Executor>`, `Arc<dyn Executor>`, and `&dyn Executor`, so any owned
or shared executor is usable wherever `impl Executor` is required.

### 6.2 Convenience helpers

```rust
pub async fn execute_kip(
    executor: &impl Executor,
    command: &str,
    dry_run: bool,
) -> (CommandType, Response);

pub async fn execute_readonly(
    executor: &impl Executor,
    command: &str,
    dry_run: bool,
) -> (CommandType, Response);
```

- Both parse the command first; a parse error short-circuits with
  `CommandType::Unknown` and a `Response::Err`.
- `execute_readonly` rejects `Command::Kml` with `KIP_1001 InvalidSyntax`
  ("Only KQL and META commands are allowed in read-only mode"). It is the
  recommended path for untrusted callers, evaluator pipelines, or sandboxed
  tool calls.

### 6.3 Implementing a backend

```rust
use anda_kip::{Command, Executor, Json, Response};
use async_trait::async_trait;

struct Mem;

#[async_trait]
impl Executor for Mem {
    async fn execute(&self, cmd: Command, _dry_run: bool) -> Response {
        match cmd {
            Command::Kql(_)  => Response::ok(Json::Array(vec![])),
            Command::Kml(_)  => Response::ok(Json::Object(Default::default())),
            Command::Meta(_) => Response::ok(Json::Null),
        }
    }
}
```

For a production-quality implementation, see
[`anda_cognitive_nexus`](../rs/anda_cognitive_nexus/) which:

- runs KQL with index-backed lookups, BFS multi-hop traversal,
  short-circuiting `FILTER` evaluation, regex caching, and BTree-backed
  `CURSOR` pagination;
- runs KML under a single `RwLock` write guard (KQL/META take the read
  guard) and enforces RC6 protected-scope and transitive-cascade rules;
- runs META through BM25 (CJK-aware via `jieba_tokenizer`) and BTree-backed
  type listings.

---

## 7. Request / Response (`anda_kip::request`)

This is the JSON envelope LLMs see when KIP is exposed via function-calling.

### 7.1 `Request`

```rust
pub struct Request {
    pub command:    String,                 // single-command mode
    pub commands:   Vec<CommandItem>,       // batch mode (mutually exclusive)
    pub parameters: Map<String, Json>,      // shared :param substitution
    pub dry_run:    bool,
    pub readonly:   bool,
}

pub enum CommandItem {
    Simple(String),
    WithParams { command: String, parameters: Map<String, Json> },
}
```

Behavior summary:

- Exactly one of `command` / `commands` must be set; both populated returns
  `KIP_1001`.
- `parameters` keys are referenced as `:name` in command text. They MUST sit
  at a JSON value position (`name: :n`, `LIMIT :k`); embedding placeholders
  inside string literals is rejected with a syntax-friendly hint.
- `iter_commands()` yields `(Cow<'_, str>, Cow<'_, Map<String, Json>>)` for
  every item in the request, merging shared parameters with per-item ones
  (per-item wins).
- `to_command()` returns a fully substituted command for single-command
  requests (no allocation when `parameters` is empty).
- `readonly()` is a builder helper that toggles read-only execution.

### 7.2 `Request::execute`

```rust
pub async fn execute(&self, nexus: &impl Executor) -> (CommandType, Response);
```

Single-command path:

1. Substitute `:placeholders`, validate them.
2. Dispatch through `execute_kip` or `execute_readonly`.
3. On `KIP_1xxx`, scan the original command for misplaced placeholders inside
   strings and append a remediation hint.

Batch path (`commands` non-empty):

1. Iterate items in order.
2. Each item is parsed and executed independently.
3. **`Response::Ok` and `Response::Err` are appended verbatim to the result
   array** — so the caller sees a heterogeneous list mirroring `commands`.
4. **Stop-on-write-error**: the first KML failure stops the batch; the
   partial array is wrapped in `Response::ok(json!(results))` so the caller
   knows what *did* execute.
5. KQL / META / parse errors do **not** stop the batch (RC6 §6).

### 7.3 `Response`

```rust
#[serde(untagged)]
pub enum Response {
    Ok  { result: Json, next_cursor: Option<String> },
    Err { error: ErrorObject, result: Option<Json> },
}

pub struct ErrorObject {
    pub code:    String,    // e.g. "KIP_2001"
    pub message: String,
    pub hint:    Option<String>,
    pub data:    Option<Json>,
}
```

- `Response::ok(Json)` and `Response::err(impl Into<ErrorObject>)` are the
  ergonomic constructors; `into_result()` collapses to
  `Result<Json, ErrorObject>`.
- `next_cursor` is set by KQL responses that support `CURSOR` resumption.
- `result` may be set on errors when a partial outcome is meaningful (e.g.
  the prefix of a batch).

`KipError` implements `Into<ErrorObject>`, so executors can simply `?` and
the SDK does the rest.

### 7.4 End-to-end

```rust
use anda_kip::Request;
use serde_json::json;

let req: Request = serde_json::from_value(json!({
    "command": "FIND(?d.name) WHERE { ?d {type: :ty} } LIMIT :n",
    "parameters": { "ty": "Drug", "n": 10 },
    "dry_run": false
}))?;

let (cmd_type, resp) = req.execute(&nexus).await;
# Ok::<_, anda_kip::KipError>(())
```

---

## 8. Errors (`anda_kip::error`)

KIP standardizes error codes so LLM agents can self-correct.

| Code       | Name                  | Trigger                                                                  | Hint excerpt                                                           |
| :--------- | :-------------------- | :----------------------------------------------------------------------- | :--------------------------------------------------------------------- |
| `KIP_1001` | `InvalidSyntax`       | Parse failure                                                            | "Check parenthesis matching, keyword spelling…"                        |
| `KIP_1002` | `InvalidIdentifier`   | Identifier breaks `[a-zA-Z_][a-zA-Z0-9_]*`                               | "Identifiers must match regex …"                                       |
| `KIP_2001` | `TypeMismatch`        | Concept/Proposition type not defined in schema                           | "Execute `DESCRIBE` to confirm type names…"                            |
| `KIP_2002` | `ConstraintViolation` | Constraint violation (e.g. subject == object, missing required field)    | "Supply the missing required attributes."                              |
| `KIP_2003` | `InvalidValueType`    | JSON value type mismatches schema                                        | "Correct the JSON value type."                                         |
| `KIP_3001` | `ReferenceError`      | Reference to undefined variable / handle                                 | "Ensure the variable is bound in `WHERE` / `CONCEPT` block placement…" |
| `KIP_3002` | `NotFound`            | Target not in graph                                                      | "Try `SEARCH`/`FIND` first."                                           |
| `KIP_3003` | `DuplicateExists`     | Uniqueness violation                                                     | "Use `UPSERT` instead of create-only."                                 |
| `KIP_3004` | `ImmutableTarget`     | Modifying meta-types, system actors (`$self`/`$system`), or core domains | "**Operation Prohibited.** Do not modify system definitions."          |
| `KIP_4001` | `ExecutionTimeout`    | Query time budget exceeded                                               | "Reduce `UNION`, lower `LIMIT`, simplify regex/multi-hop."             |
| `KIP_4002` | `ResourceExhausted`   | Result set / memory budget exceeded                                      | "Use `LIMIT` and `CURSOR` for pagination."                             |
| `KIP_4003` | `InternalError`       | Unknown backend error                                                    | "Contact administrator or retry."                                      |

Constructor helpers — one per code — live on `KipError`:

```rust
KipError::invalid_syntax("…");
KipError::type_mismatch("…");
KipError::reference_error("…");
KipError::immutable_target("…");
// …etc.
```

`KipError` round-trips through `ErrorObject` (`From<KipError>` is provided),
so an `Executor` typically writes `?`-flavored Rust:

```rust
let id = self.query_id(&pk).await
    .ok_or_else(|| KipError::not_found(format!("{pk:?} missing")))?;
```

`format_nom_error` adapts a `nom_language::VerboseError` to `KipError`,
preserving the context stack and the offending source slice.

---

## 9. Genesis capsules (`anda_kip::capsule`)

The capsule module exposes the canonical bootstrap KIP source as `&'static
str` constants plus the reserved-name sigils used by RC6's protected-scope
guard.

| Constant                                                                          | Purpose                                                               |
| :-------------------------------------------------------------------------------- | :-------------------------------------------------------------------- |
| `META_CONCEPT_TYPE`                                                               | `"$ConceptType"` — root of the concept-type meta hierarchy            |
| `META_PROPOSITION_TYPE`                                                           | `"$PropositionType"` — root of the predicate hierarchy                |
| `META_SELF_NAME`                                                                  | `"$self"` — the agent's waking persona name                           |
| `META_SYSTEM_NAME`                                                                | `"$system"` — the agent's sleeping/maintenance persona                |
| `DOMAIN_TYPE`                                                                     | `"Domain"`                                                            |
| `EVENT_TYPE`, `INSIGHT_TYPE`, `PERSON_TYPE`, `PREFERENCE_TYPE`, `SLEEP_TASK_TYPE` | standard concept types defined in the bundled capsules                |
| `BELONGS_TO_DOMAIN_TYPE`                                                          | `"belongs_to_domain"` — predicate used by all CoreSchema entries      |
| `GENESIS_KIP`                                                                     | bootstraps `$ConceptType`, `$PropositionType`, `Domain`, `CoreSchema` |
| `EVENT_KIP`, `INSIGHT_KIP`, `PERSON_KIP`, `PREFERENCE_KIP`, `SLEEP_TASK_KIP`      | standard concept-type definitions                                     |
| `PERSON_SELF_KIP`, `PERSON_SYSTEM_KIP`                                            | the `$self` and `$system` actor instances                             |

A typical bootstrap loop is therefore:

```rust
nexus.execute_kml(parse_kml(GENESIS_KIP)?, false).await?;
nexus.execute_kml(parse_kml(PERSON_KIP)?, false).await?;
nexus.execute_kml(parse_kml(EVENT_KIP)?,  false).await?;
// …etc
```

---

## 10. Entity types (`anda_kip::types`)

Concrete data structures returned by an executor when it needs to surface a
real concept or proposition (for example, by `DESCRIBE CONCEPT TYPE …`).

```rust
pub enum Entity { ConceptNode(ConceptNode), PropositionLink(PropositionLink) }

pub struct ConceptNode {
    pub id: String, pub r#type: String, pub name: String,
    pub attributes: Map<String, Json>,
    pub metadata:   Map<String, Json>,
}

pub struct PropositionLink {
    pub id: String,
    pub subject: String, pub predicate: String, pub object: String,
    pub attributes: Map<String, Json>,
    pub metadata:   Map<String, Json>,
}
```

Each owned struct has a borrowed twin (`ConceptNodeRef<'a>`,
`PropositionLinkRef<'a>`, `EntityRef<'a>`) for zero-copy serialization on
hot paths. The `Entity` enum uses `#[serde(tag = "_type")]` so JSON output is
self-describing.

`EntityType { ConceptNode, PropositionLink }` is a tag-only enum used in
contexts (e.g. cache keys, error builders) where the full payload is not
needed.

---

## 11. Function-calling integration

`anda_kip::lib` exposes two `LazyLock<Json>` constants, populated from
bundled JSON-schema files:

```rust
pub static KIP_FUNCTION_DEFINITION:          LazyLock<Json>; // execute_kip
pub static KIP_READONLY_FUNCTION_DEFINITION: LazyLock<Json>; // execute_kip_readonly
```

Both schemas mirror the `Request` struct (single + batch + parameters +
`dry_run`). They are designed to drop directly into an OpenAI-compatible
`tools` array, into an MCP `tool` definition, or into a custom function
registry.

Two complementary prompt assets ship with the crate:

- `SYSTEM_INSTRUCTIONS` — high-level system prompt that teaches an LLM what
  KIP is and when to call the tool.
- `SELF_INSTRUCTIONS` — agent-side instructions for managing memory through
  KIP idiomatically (when to use `UPSERT` vs `SET ATTRIBUTES`, how to query
  `DESCRIBE PRIMER`, etc.).

Wire them into your prompt the same way you would a system message:

```rust
let system = format!(
    "{}\n\n{}",
    anda_kip::SYSTEM_INSTRUCTIONS,
    anda_kip::SELF_INSTRUCTIONS,
);
```

---

## 12. Executor implementer's checklist (RC6 semantics)

When writing a new backend (or porting an old one to RC6), these are the
points the parser does **not** enforce — they are the executor's contract:

**KQL**

- [ ] Aggregates: when `FIND` mixes plain variables and aggregates, treat
      plain variables as the **implicit `GROUP BY`** key and emit one row per
      group.
- [ ] `COUNT(?x)` with no joins should be answerable directly from index
      cardinality without materializing rows.
- [ ] `OPTIONAL`: missing bindings project as JSON `null`; `IS_NULL(?x)` on
      such bindings must return `true`.
- [ ] `NOT`: variables introduced inside `NOT` must not leak outside.
- [ ] `UNION`: branches have **independent** scopes; rows are concatenated
      with `null` filling for variables present in only one branch.
- [ ] Path operator: `"p"{m,n}` traverses `m..=n` hops; `m == 0` includes the
      reflexive `?s == ?o` row.
- [ ] `CURSOR` tokens must be opaque, deterministic, and resumable (typical
      implementation: BTree key prefix).
- [ ] `expires_at` is a **signal**, not an automatic filter. Agents add
      `FILTER(IS_NULL(?x.metadata.expires_at) || ?x.metadata.expires_at > <now>)`
      explicitly.

**KML**

- [ ] `UPSERT` items execute top-to-bottom; handles must be defined before
      use; cycles are illegal.
- [ ] `WITH METADATA` precedence: per-link > inner block > outer default.
- [ ] `SET ATTRIBUTES`: shallow merge; unspecified keys are preserved.
- [ ] `SET PROPOSITIONS`: additive only; never deletes unspecified links.
- [ ] `DELETE CONCEPT … DETACH`:
  - Reject with `KIP_3004` if any target is in the protected scope
    (`$ConceptType`, `$PropositionType`, `$self`, `$system`,
    `Domain:CoreSchema` — the executor SHOULD also extend this to other
    declared core domains and `core_directives`).
  - Cascade through **all** propositions referencing the concept, then
    through propositions referencing those propositions (transitively).
- [ ] Report cascade counts so agents can audit impact (e.g. `{"deleted_concepts":
      n, "deleted_propositions": m}`).

**META**

- [ ] `DESCRIBE PRIMER` returns the agent identity, learned concept-type
      counts per domain, and a domain map.
- [ ] `DESCRIBE … TYPES` paginates via opaque `next_cursor`.
- [ ] `SEARCH … WITH TYPE …` filters by concept type / predicate; `LIMIT`
      should default to a small bound (e.g. 10) and cap at a documented
      maximum.

**Concurrency**

- [ ] KQL / META acquire a *read* guard, KML acquires an exclusive *write*
      guard. This is what `anda_cognitive_nexus` does and what the spec
      assumes.

---

## 13. Cookbook

### 13.1 Parse and inspect

```rust
use anda_kip::{parse_kip, Command, FindExpression, AggregationFunction};

let cmd = parse_kip("FIND(COUNT(?x)) WHERE { ?x {type: \"Drug\"} }")?;
if let Command::Kql(q) = cmd {
    matches!(q.find.expressions[0],
             FindExpression::Aggregation { function: AggregationFunction::Count, .. });
}
# Ok::<_, anda_kip::KipError>(())
```

### 13.2 Single command via `execute_kip`

```rust
use anda_kip::{execute_kip, Executor};
let (ty, resp) = execute_kip(&nexus, "DESCRIBE PRIMER", false).await;
```

### 13.3 Read-only sandbox

```rust
let (_ty, resp) = anda_kip::execute_readonly(
    &nexus,
    "UPSERT { CONCEPT ?d { {type: \"Drug\", name: \"X\"} } }",
    false,
).await;
// resp is Response::Err with code KIP_1001 — KML rejected.
```

### 13.4 Batch with shared and per-item parameters

```json
{
  "commands": [
    "DESCRIBE PRIMER",
    "FIND(?t.name) WHERE { ?t {type: \"$ConceptType\"} } LIMIT :limit",
    {
      "command": "UPSERT { CONCEPT ?e { {type:\"Event\", name: :name} } }",
      "parameters": { "name": "TodaysEvent" }
    }
  ],
  "parameters": { "limit": 50 },
  "dry_run": false
}
```

The result `Response::Ok { result: Json::Array([...]) }` contains three
entries: a META response, a KQL response (cap 50), and a KML response in
order. If the third entry had failed with a KML error, the batch would have
stopped and returned an array of length 3 ending in `Response::Err`.

### 13.5 Implementing an executor

See [§6.3](#63-implementing-a-backend) and the production reference in
[`rs/anda_cognitive_nexus/src/db.rs`](../rs/anda_cognitive_nexus/src/db.rs).

### 13.6 Building the function-calling tool definition

```rust
let tool = serde_json::json!({
    "type": "function",
    "function": *anda_kip::KIP_FUNCTION_DEFINITION,
});
```

---

## 14. `kip_cli`

`anda_kip` ships an interactive REPL implemented in
[`rs/anda_kip/src/bin/kip_cli.rs`](../rs/anda_kip/src/bin/kip_cli.rs). It
opens an in-memory or local file-system Cognitive Nexus and lets you type
KIP commands directly:

```bash
cargo run -p anda_kip --bin kip_cli
```

The REPL is convenient for verifying parser behavior, exercising the
function-calling envelope, and for ad-hoc exploration of bootstrapped
capsules.

---

## 15. Compatibility notes

- **MSRV**: tracks the workspace toolchain (`edition.workspace = true`).
- **Async runtime**: tokio is required only for tests; the trait itself is
  runtime-agnostic.
- **Serde**: the AST and request/response types serialize to stable JSON; the
  on-the-wire shape matches the KIP specification exactly. Cross-language
  bindings (e.g. the Python module under `py/anda_cognitive_nexus_py`) rely
  on this.
- **Spec drift**: the parser, AST, and request/response surface are aligned
  with KIP **v1.0-RC6** (2026-04-25). When the spec advances, breaking
  parser/AST changes will be released as a minor version bump and the
  `Compatibility notes` section will track the diff.
- **Performance posture**: the parser is allocation-light (combinator-based,
  no regex); the request envelope uses `Cow` to avoid copying when no
  parameter substitution is required; entity types ship borrowed twins for
  zero-copy serialization. Heavy lifting (graph traversal, BM25, multi-hop
  BFS) belongs in the executor, not in this crate.

---

*Last updated: KIP v1.0-RC6 (2026-04-25), `anda_kip` 0.7.10.*
