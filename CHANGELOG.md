# Changelog

All notable changes to this workspace are documented in this file.

## [KIP 2.0] — 2026-08-17

`anda_kip` 0.12.0, `anda_cognitive_nexus` 0.12.0,
`anda_cognitive_nexus_server` 0.12.0, `@ldclabs/kip-do` 0.13.0.

KIP 2.0 across the workspace. This is a rewrite, not an upgrade: 1.x and 2.0 are
semantically different protocols, so the old API is gone rather than deprecated.
Every downstream has been ported — `anda_cognitive_nexus`,
`anda_cognitive_nexus_server`, `anda_kip_wasm`, `py/anda_cognitive_nexus_py`
and `ts/kip-do`. Nothing in the workspace still speaks 1.x.

The minor bump on `anda_cognitive_nexus` and `anda_cognitive_nexus_server`
matters more than it looks: under cargo's 0.x rules a `0.11.1` would have been
a compatible upgrade from the 1.x engine, so anyone pinning `"0.11"` would have
been moved onto a different protocol without being asked.

The workspace now declares `rust-version = "1.88"`, which is what let-chains in
edition 2024 need. Without it an older toolchain reports a syntax error instead
of a version mismatch.

### Why it is a rewrite

KIP 1.x kept meaning, belief, evidence, provenance, retention and governance in
one self-describing graph, where a Proposition carried `metadata.confidence`, an
`author` string and an `access_level`. KIP 2.0 separates those planes, and
everything else follows from one line:

```text
a Proposition existing  ≠  the Proposition being true
```

A Proposition is now a truth-neutral tuple. An Assertion is one actor's
commitment about it — stance, mode, confidence, Evidence, valid time. What is
*currently believed* is projected from those Assertions and never stored, which
is why correcting a claim records a new Assertion with `SUPERSEDING` instead of
rewriting the old one.

### Added

- **Three new grammars** implementing `v2/grammar/KIP-2.0-{KQL,KML,META}.ebnf`.
  KQL gains `ASSERTION` / `EVIDENCE` / `ACTIVITY` / `STRUCTURAL` patterns,
  `BELIEF` and `BELIEF SLOT` projections, raw predicate paths with alternation
  and hop quantifiers, `AS OF SEQ|TX|TIME` and `FOR TIME` as independent axes,
  and `WITH EPISTEMIC`. KML gains `CREATE CONCEPT/EVIDENCE/ASSERTION/ACTIVITY`,
  `UPSERT CONCEPT`, `ENSURE PROPOSITION`, the `ASSERT` sugar, `UPDATE`,
  `RETRACT` / `SUPERSEDE` / `CORRECT`, `TRANSITION ACTIVITY`, `SET RETENTION`,
  `ARCHIVE` / `TOMBSTONE` / `PURGE`, non-destructive `MERGE CONCEPT`, and
  `SET`/`UNSET` for fields, attributes, facets and structural references. META
  gains the full `DESCRIBE` / `LIST` / `SEARCH` / `VERIFY` / `VALIDATE` /
  `PREVIEW` / `HISTORY` / `CHANGES` / `SNAPSHOT` / `EXPORT CAPSULE` families.
  Keywords are now ASCII case-insensitive and contextual — `by`, `mode`, `key`,
  `type` and `status` are all legal field names, as the spec's own examples
  require — while `true` / `false` / `null` and `id` stay case-sensitive.
- **Schema-independent validation at parse time**, so a command that would ask
  an engine to corrupt the epistemic record never reaches one: `UPSERT CONCEPT`
  must match `id` or `key` (a name is mutable and duplicable); an `UPDATE` may
  not rewrite immutable Assertion, Evidence or Proposition payload; structural
  mutation reaches Concept topology only; `_system`, `governance`, `space_id`
  and `space_seq` are never author-writable; `ENSURE PROPOSITION (id: ...)` is
  rejected because `(id: ...)` is match-only; `ASSERT` requires `by` and `mode`,
  neither of which has a safe default; an update expression may read only the
  element being updated; local handles must be unique and resolvable.
- **`ast`** — a closed executable AST matching `exec-ast.ts` from
  `@ldclabs/kip-lang` field for field under serde's externally-tagged encoding,
  so the Rust and TypeScript implementations can be differentially tested.
- **`error`** — the 79-code Core Error Registry (§87) with categories and retry
  classes. `outcome_lookup_required` is the one to wire into client recovery: a
  lost response is not proof a write failed, and re-issuing it is how duplicate
  cognition gets created.
- **`request`** — the 2.0 envelope (§71–§85): `operations[]` with per-operation
  parameters and idempotency keys, `execution.mode` of
  `independent`/`sequence`/`atomic`, ingestion contexts that mint Evidence from
  the transport rather than from model-generated command text, snapshot
  binding, preconditions, receipts, and the `partial` top-level status that
  keeps a half-committed `sequence` from being reported as a total failure.
- **`types`** — the Core data model (§6–§19) with no universal metadata bag.
- **`capsule`** — portable Cognitive Capsules (§37–§41), import modes, identity
  resolution order, and the `redacted` vs `unavailable` distinction.
- **`executor::execute_request`** — runs `independent` and `sequence`, and
  refuses `atomic` rather than emulating it.
- **A differential test against `@ldclabs/kip-lang`** —
  `tests/fixtures/kip_lang_ast.json` holds 76 command → AST pairs produced by
  the reference TypeScript implementation, covering every KQL pattern family,
  every KML mutation family and every META statement family. `anda_kip` decodes
  all of them to byte-identical trees, in both directions. Plus
  `tests/syntax_docs.rs`, which parses every executable example in the bundled
  `KIPSyntax.md`.

### Changed

- **`Response` is a struct, not an enum**: `{status, results[], receipt,
  warnings, ...}` per §81, with `succeeded` / `failed` / `partial` /
  `outcome_unknown` at the top level.
- **`execute_readonly` rejects writes by parsed semantics**, and now reports
  `ReadonlyViolation` instead of borrowing the syntax-error code.
- **A declared operation `language` can no longer relabel a write as a read** —
  a mismatch is `LanguageMismatch` (§73.1, §88.3).
- **Bundled prompts and tool schemas rewritten for 2.0**: `SPECIFICATION.md` and
  `KIPSyntax.md` are copies of the v2 documents; `SelfInstructions.md`,
  `SystemInstructions.md`, `FunctionDefinition.json` and
  `FunctionDefinitionReadonly.json` are new, and `KIP_SYNTAX` is exported
  alongside the existing statics.

### Removed

- **The genesis capsules** (`capsules/*.kip`, `GENESIS_KIP`, `PERSON_KIP`, the
  `*_PROP_KIP` predicate sources, `META_CONCEPT_TYPE` and friends). KIP 2.0
  Schema is immutable Package state; a schema graph node is not authoritative
  Schema (§103.9), and these files are 1.x `UPSERT` scripts the 2.0 parser
  cannot read. `capsule.rs` now models Cognitive Capsules instead.
- **`ConceptNode` / `PropositionLink` / `Entity` / `UpsertResult` and the
  `metadata.*` constants**, superseded by the five Core element kinds.
- **The numeric `KIP_xxxx` error codes**, superseded by the named registry.

### Downstream ports

- **`anda_cognitive_nexus` was rewritten, not migrated.** The 1.x engine that
  lived here is deleted; 2.0 is a different data model, and a renamed 1.x engine
  would have been a worse lie than an absent one. The new engine implements
  `Executor` over `anda_db` collections and runs KML transactions, KQL reads,
  the Epistemic Projection, the META families, Cognitive Capsule
  export/verification/import, the historical read path, and the full Governance
  control plane. What it still does **not** do — trust and evidence-quality
  evaluation, semantic and historical `SEARCH`, Capsule signatures, the Capsule
  `restore` mode, Space-level retention defaults, atomic multi-operation
  batches, grouped aggregation, `STRUCTURAL` over Core reference fields,
  idempotent replay, and the permissions no gate yet asks for — is reported by
  `DESCRIBE CAPABILITIES` as structured data with a reason, and refused rather
  than answered wrongly. The crate now ships
  the baseline cognitive-memory profile as `profiles/cognitive-memory-2.0.0.json`
  (`profiles::COGNITIVE_MEMORY`), vendored verbatim from the spec repository,
  and `CognitiveNexus::ensure_schema` activates a Schema Lock only when it
  differs from the one already in force.
- **Both time axes now answer.** `AS OF SEQ | TX | TIME` reads the Space at a
  past coordinate, and `SNAPSHOT` issues a token a later request binds to
  through `read.snapshot_token`. This needed state the engine did not keep: a
  row is updated in place, so version 3 overwrites version 2 and version 2 is
  gone. Every commit now appends the complete row it wrote to a version log —
  in the same commit as the row, because a history written afterwards can be
  missing exactly the write a crash interrupted, and a history with a hole
  answers wrongly instead of refusing. A historical pattern cannot use the
  indexes (they describe the present), so it reconstructs its candidates from
  that log and re-checks every constraint against the element as it stood;
  the cost is charged to the same query budget. `AS OF` resolves symbols
  through the Schema Environment that was in force *then* (§144), the
  projection sees only the Assertions of its coordinate, and a request bound to
  one coordinate whose command names another is refused rather than silently
  preferring one. `SEARCH ... AS OF SEQ` stays unsupported: the index reflects
  the present, and reporting today's matches as if they were then's is the one
  answer worse than none.
- **Hop-quantified paths traverse.** `(?a, "leads_to"{1,3}, ?b)` walks the raw
  Proposition graph, from whichever end is pinned — including by an earlier
  pattern in the same block, which is what made `Context::bound` real rather
  than a field nothing wrote. Binding a variable to a multi-hop path is
  refused: the walk is not a Proposition, and naming one of the tuples it
  crossed would name a claim the query never asked about. A path reports
  reachability, never belief (§45).
- **Capsule import performs the semantic merge.** Identity resolves in the
  spec's order (§38.2) — a prior import of the same artifact, a trusted
  `canonical_id`, then a Proposition's own tuple — and the mapping lives on the
  elements as their `client_key`, so a re-import after a restart resolves to
  what the first import created rather than duplicating it. Every reference is
  rewritten onto destination ids; a Capsule citing something it does not carry
  is refused whole. The source's Space-local `key` stays at the source (§5.3),
  the epistemic payload arrives unchanged, and what *this* runtime observed —
  the import — is stamped into engine origin (§27). Import is a host API
  (`CognitiveNexus::import_capsule`), not a command: KML has no import clause
  and META is read-only, so no prompt decides that a Space accepts another
  Brain's cognition. Two bugs surfaced on the way: an `EvidenceRef` spells its
  target `evidence_id`, so citations were being written through unrewritten,
  and the derived `evidence_ids` index column came out empty for the same
  reason.
- **The mutation path selects what it acts on.** `UPDATE` and `MERGE CONCEPT`
  are implemented, and `WHERE` selection blocks with `LIMIT` now work on
  `UPDATE`, `RETRACT ASSERTION`, `SET RETENTION`, `ARCHIVE` and `TOMBSTONE`.
  Three decisions a second engine has to match, all pinned by
  `fixtures/kip-conformance-2.0/mutation-selection.json`: a selection block
  reads the state the **transaction started from**, so a sweep cannot act on
  what the same `MUTATE` just created (clause order carries no mutation
  semantics, §24); `LIMIT` cuts in **ascending element id**, which §52.7 permits
  a runtime to document and which makes a bounded sweep repeatable; and a block
  that matches nothing is a `no_effect`, not an error — `UPDATE` never creates.
  `UPDATE`'s reach is enforced against the element the engine loaded, not
  against what the command looked like, because `UPDATE :A-7` names an id and
  only the engine knows what wears it: an Assertion answers
  `EpistemicRevisionRequired`, Evidence `EvidenceCorrectionRequired`, an
  Activity `InvalidLifecycleTransition`, each naming the ritual that *is* legal.
  `MERGE CONCEPT` is non-destructive (§11.1) — the source keeps its state and
  gains `merged_into`, cycles and re-pointing are refused — and a new write
  canonicalizes a merged reference to the survivor (§11.3), without rewriting
  the history that referred to it (§11.2). `PURGE` remains refused: erasure is a
  Governance decision this engine cannot make.
- **Two defects surfaced by that work**: `UPSERT CONCEPT` parsed
  `UNSET FACET` / `SET STRUCTURAL` / `UNSET STRUCTURAL` and silently dropped
  them (both clause families now run through one applier), and a structural
  reference written from a `:parameter` was stored as a bare id string rather
  than `{"id": …}`, so the edge existed but no `STRUCTURAL (…)` pattern could
  follow it. A Facet's local name now also resolves in a dot path —
  `?m.facets["MnemonicState"].salience` — on both the read and the write side,
  instead of quietly reading `null` outside its canonical `kip://…` spelling.
- **`anda_cognitive_nexus_server` speaks the 2.0 envelope.** `execute_kip`'s
  `params` is now the KIP 2.0 request envelope rather than a bare command:
  `{"kip": "2.0", "operations": [{"command": "..."}]}`. Execution goes through
  `execute_request`, so `independent` and `sequence` batches run and `atomic` is
  refused with `UnsupportedCapability` instead of being downgraded. HTTP status
  mapping follows the named registry (§87) — including `207` for a `partial`
  batch, whose earlier commits are durable, and `501` for a capability this
  runtime declares it lacks. The `$self` genesis KML and `SELF_PRINCIPAL_ID` are
  **removed**: a `Person` is not a Principal (§88.1), and Principals are
  Governance control-plane state written through host APIs, never minted by
  cognitive content. In their place, the server
  installs and activates the bundled cognitive-memory profile in the default
  Space, extensible with `SCHEMA_PACKAGE` / `--schema-package`. The `kip_logs`
  audit document records `languages` (classified from the parsed commands, never
  from the advisory `operation.language`) and the response's status, `tx_id` and
  errors; the size cap now also bounds `ingest` payloads and client-supplied
  correlation metadata. A mutation that overruns its *response* deadline now
  answers `outcome_unknown` instead of `ExecutionTimeout`: the execution is
  deliberately not cancelled, so it may still commit, and `ExecutionTimeout`'s
  registered retry class (`safe_same_request`) would have invited the client to
  write the same cognition twice.
- **`anda_kip_wasm` reports the 2.0 error shape.** `error_catalog()` enumerates
  `KipErrorCode::ALL` with each code's category, retry class and hint, so a code
  added to `anda_kip` cannot be missed by the generated TypeScript table, and
  parse failures carry the full `ErrorObject` — `retry` included, because
  flattening `outcome_lookup_required` into "it failed" is how a lost write
  becomes a duplicated one. `ts/kip-do` consumes this: its
  `scripts/codegen-errors.mjs` now loads the vendored WASM and calls
  `error_catalog()` rather than parsing `error.rs` as text, so a code added to
  `anda_kip` reaches the TypeScript table without anyone remembering to add it.
- **`py/anda_cognitive_nexus_py` builds a 2.0 envelope.** `execute_kip` wraps
  the command in a single-operation request, binds parameters as request-level
  bindings, and returns `(CommandType, Response)` instead of a `Result` whose
  `Err` never happened — a KIP failure is an answer with a code, a hint and a
  retry class, and flattening it into a Python string throws away everything
  the caller needs to recover. `create_kip_db` activates the bundled
  cognitive-memory profile, so `$ConceptType` bootstrapping is gone: a type is
  a Schema Package symbol now, not something a write can invent.
- **`ts/kip-do` was rewritten too, and is a sibling engine rather than a
  binding.** The 1.x executor is deleted for the same reason the Rust one was.
  Storage is SQLite inside a Durable Object instead of `anda_db`, and the
  grammar is `@ldclabs/kip-lang` — a native TypeScript parser, not a port —
  which means nothing structural forces the two engines to agree on what a
  command means. `test/parser-oracle.test.ts` is what does: it loads `anda_kip`
  compiled to WebAssembly and compares both directions over a generated corpus,
  because a divergence here is the most expensive bug this project can have —
  the same command succeeding on one deployment and failing on the other, or
  worse, meaning two different things. That oracle found four real defects in
  `kip-lang`, the worst being an out-of-range integer *rounded* rather than
  rejected, which would have executed a command carrying a different number
  than the one written. All are fixed upstream in 2.0.1.
  The engine now runs Schema Packages, transactions and the KML clauses, KQL,
  the Epistemic Projection, META, Capsule export and verification, the
  Governance control plane at the same per-element granularity as the reference
  engine, and the historical read path. It runs the same 62 shared conformance
  cases from `fixtures/kip-conformance-2.0/`, inlined by a codegen step rather
  than transcribed. Its gaps are named in `DESCRIBE CAPABILITIES` with reasons,
  and the ones it shares with the reference engine say so.
- **Both engines' capability documents were corrected.** Three gaps were real
  but undeclared on the Rust side (`grouped_aggregation`,
  `structural_core_fields`, `ungated_permissions`), and one was declared
  backwards on both: `transactions.idempotency` reported `true` while neither
  engine replays. A key is recorded and findable through
  `DESCRIBE TRANSACTION BY IDEMPOTENCY KEY`, but the write path never looks it
  up, so a resend re-executes. Both now report `recorded_not_replayed` and
  carry an `idempotent_replay` gap, and both have a test pinning what a resend
  actually does — the reference engine commits a duplicate, `ts/kip-do` trips a
  unique index and fails. That asymmetry is named too. Reporting `true` here is
  worse than reporting nothing: it is the field a retry policy reads before
  deciding a resend is free.
- **`ORDER BY` over an aggregate is refused instead of silently mis-answered.**
  `rs/anda_cognitive_nexus` was dropping the aggregation and sorting by the
  bare variable, which returns a plausible-looking answer to a question nobody
  asked — the one failure mode this project refuses everywhere else.
  `ts/kip-do` already refused it; now both do, and both declare it.
- **A KIP 1.x database now migrates itself on the first 2.0 start.** Before
  this, pointing 0.12.0 at a 1.x directory failed — but only by accident.
  `Schema::needs_upgrade` compares version numbers, both layouts derive to 0, so
  the 2.0 schema was silently ignored and the collection kept the 1.x one; the
  engine then failed building an index on a field the old schema never had.
  Safe, and unreadable as a diagnosis.
  `CognitiveNexus::connect` now detects the 1.x layout from the schema the
  collection actually carries and migrates in three phases with a durable
  staging area between them, so a crash resumes instead of losing: extract the
  1.x rows verbatim into `kip_legacy_v1`, drop the two colliding collections,
  then load them through the ordinary engine — real KML, real validation, real
  Governance, because a migration that wrote rows directly would be the one
  writer allowed to produce elements the engine would have refused. The 1.x
  rows are kept afterwards.
  The shape change is the substance. A 1.x Proposition row is a
  multi-predicate edge; a 2.0 Proposition is one tuple, so one row fans out
  into one Proposition per predicate, each with its own Assertion, because each
  carried its own `confidence`. 1.x types and predicates were free strings with
  no package to resolve against, so the migration reads the vocabulary off the
  data and publishes `kip://legacy/nexus@1.0.0` (§8) — generated types are
  open-attribute and generated predicates are never functional, since a
  constraint invented here would be one the old data was never checked against
  and its first act would be to reject the deployment's own history.
  What it refuses to invent is the point (§2). Every migrated Assertion carries
  `mode: "imported"`, the registered mode for *carried in from another system*.
  `asserted_by` is a generated actor Concept standing for the engine, not a
  fabricated speaker. Legacy `confidence` is carried onto the Assertion and
  also preserved verbatim, because 1.x deployments used that field for truth,
  staleness and importance and only the operator knows which (§13, §14). And
  `access_level` stays a legacy attribute rather than becoming a
  classification: 1.x's annotated where 2.0's enforces, and promoting one to
  the other silently would either over- or under-protect every migrated
  element (§21).
  `migrate::plan` is the dry run: it works out what the migration would do —
  element counts after the fan-out, the vocabulary that would be published,
  and what would block it — and writes nothing, not even the staging area,
  because a dry run that staged would leave the database different for having
  been asked. It reports the ambiguous legacy fields as an inventory rather
  than as advice: §13 and §21 are unactionable in the abstract and become
  actionable when an operator can see that *this* deployment has 2 tuples
  carrying confidence between 0.25 and 0.90 and one `access_level` value.
  A legacy `author` that names exactly one migrated Concept now becomes that
  Concept's Assertion rather than the migration actor's — that is a speaker
  the old system really did record, and dropping it would lose attribution the
  data actually had. A name shared by two Concepts identifies neither, so it
  is left alone rather than resolved by picking one (§12).
  One consequence worth knowing: `ensure_schema` now retains the generated
  package unless a caller deactivates it by name. A host activates its own
  baseline lock on every start, that lock cannot name a package this engine
  generated, and without the retention the next ordinary restart would orphan
  every migrated element's `schema_ref` — the elements still there, nothing
  able to read them.
- **`ts/kip-do` wrote Capsules the reference engine could not open.** Found by
  putting the shared fixtures through the request envelope, which is where the
  artifact actually shows. Two breaks, both fatal to the one thing a Capsule is
  for: the frame said `format: "KIP-Capsule"` / `format_version: "2.0-draft"`
  where §37.6 and `anda_kip::Capsule` say `KIP-Cognitive-Capsule` / `2.0` — and
  `validate_frame` refuses any other `format` outright — and each schema
  dependency was a single `package_ref`, where `anda_kip::SchemaDependency`
  requires `package` and `version` as separate non-optional fields, so the
  whole payload failed to decode. Both are now the canonical shape, pinned by a
  test that says why. The remaining differences between the two engines'
  Capsule payloads are additive (`installed_here`, `digest_profile`, empty
  record arrays, `nexus_id`) and do not stop a decode, but they still have to
  converge before a Capsule fixture can compare the artifacts byte for byte.
- **The shared conformance suite grew a Transactions fixture**, and `ts/kip-do`
  now runs the whole suite **through the request envelope** rather than by
  calling `nexus.find` and `nexus.mutate`. The old harness proved the two
  engines agreed about everything except the layer a client actually talks to.
  71 cases across 8 fixtures, both engines.
- **CI runs the TypeScript engine.** It previously ran neither its tests, its
  conformance suite, nor the parser oracle, so the cross-engine agreement the
  oracle exists to prove was checked only by whoever last remembered to run it.
  The new job also regenerates the four generated files and fails on drift —
  which is how the oracle corpus had quietly lost ten commands.

## [KIP v1.0-RC11] — 2026-08-14

`anda_kip` 0.11.1 · `anda_cognitive_nexus` 0.11.1 · `@ldclabs/kip-do` 0.12.2.

Titled by protocol revision rather than by a workspace version: `0.11.1` was
already used on 2026-07-31 for `anda_db` / `anda_db_btree` / `anda_db_server`,
and the two KIP crates are only now catching up to that number. No other crate
changes, and the inter-crate requirements (`anda_kip = "0.11"`) are unaffected.

### Added

- **KIP v1.0-RC11 — the Experience Learning cognitive profile** — RC11 adds no
  KQL/KML syntax; it extends the recommended cognitive memory profile and the
  metadata catalog, and both engines pick that up through their bundled
  capsules. `anda_kip` now bundles three new concept types — `Experience` (a
  goal-directed state/action/observation trajectory), `ExperienceStep` (one
  ordered unit of it) and `Skill` (procedural memory compiled from
  Experiences) — plus eight standalone predicate capsules. Four of those are
  not new predicates but a **relocation**: `involves`, `mentions`,
  `consolidated_to` and `derived_from` used to be declared inside
  `Event.kip` and now ship as their own capsules, widened so `Event` **and**
  `Experience` are legal subjects (`derived_from` gains `Experience` as a
  legal object). The other four are Experience-specific: `has_step`,
  `caused_by` (effect → cause, and only with evidence beyond temporal
  adjacency), `derived_insight` and `compiled_to`. New `anda_kip` constants:
  `EXPERIENCE_TYPE` / `EXPERIENCE_STEP_TYPE` / `SKILL_TYPE`, the eight
  `*_TYPE` predicate names, and the matching `EXPERIENCE_KIP` /
  `EXPERIENCE_STEP_KIP` / `SKILL_KIP` / `*_PROP_KIP` capsule sources.
- **`memory_strength` as an orthogonal memory axis** — Appendix 1 of the spec
  now separates mnemonic accessibility (`memory_strength`, raised by
  reinforcement and lowered by disuse) from epistemic support (`confidence`,
  which changes on evidence, contradiction or retraction — never on the mere
  passage of time). The canonical sleep-cycle decay sweep in §4.3 was
  retargeted accordingly. This is a guidance change: no engine field is
  reserved or enforced, and both metadata keys remain ordinary user metadata.

### Changed

- **Bundled-capsule anchors carry their meta-type** — The self-healing
  existence check that runs beside each capsule's content hash used to look
  the anchor up as a `$ConceptType` unconditionally, which cannot express a
  capsule that owns a predicate. `anda_cognitive_nexus`'s `BUNDLED_CAPSULES`
  entries and `kip-do`'s generated `Capsule` records now carry the anchor's
  meta-type (`$ConceptType` or `$PropositionType`) beside its name, so
  `kip-do`'s `Capsule.anchor` becomes `anchorType` + `anchorName`. Both are
  internal — neither `BUNDLED_CAPSULES` nor `Capsule` is re-exported from
  `@ldclabs/kip-do`'s entry point — so no published API changes.
- **Existing databases upgrade on the next connect** — `Event.kip`,
  `Genesis.kip`, `Insight.kip`, `Preference.kip` and `SleepTask.kip` all
  changed content, and the ten new capsules have no recorded hash, so
  `CognitiveNexus::connect` (and the Durable Object's bootstrap-version
  check) re-applies exactly the affected capsules. Capsules are idempotent
  `UPSERT` scripts, so no user data is touched. `Preference` gains a
  `source_memory` attribute (`source_event` is kept and documented as legacy),
  and `SleepTask.requested_action` gains `compile_to_skill`.

### Documentation

- `docs/anda_kip.md` and `docs/anda_cognitive_nexus.md` synced to KIP
  v1.0-RC11, including the expanded capsule constant table and the capsule
  dependency order (concept types before the predicates that reference them);
  `SPECIFICATION.md`, `KIPSyntax.md`, `SystemInstructions.md` and
  `SelfInstructions.md` mirrored from the RC11 upstream.

### Known gap

- `kip-do`'s `DESCRIBE PRIMER` still reports `spec_revision: "v1.0-RC10"`. The
  value comes from `KIP_SPEC_REVISION` in the upstream `@ldclabs/kip-lang`
  package (0.4.0), which has not been republished for RC11; it will correct
  itself when that dependency is bumped.

## [0.11.1] — 2026-07-31

### Fixed

- **Which end of a page you get is now the method's contract, not the
  filter's shape** — A bounded filter query used to keep whichever end its
  scan happened to reach: a bare `_id Lt cursor` walks backwards and returned
  the **newest** ids, while the same predicate inside `And([user Eq u, _id Lt
  cursor])` evaluates unbounded and returned the **oldest**. Newest-first
  cursor pagination therefore broke the moment a second condition was added —
  the first page came back as the oldest rows and the next cursor pointed
  below them, ending the walk after one page. `Collection::query_ids` (and
  the filter-only path of `search_ids` / `search` / `search_as`) now returns
  the **smallest** matching ids for every filter shape, and the new
  `Collection::query_last_ids` returns the **largest** — exposed over HTTP as
  `doc.query_last_ids`. Scans walk in the requested direction, so both ends
  still stop as soon as the page is full (`BTreeIndex::range_query_rev_with`
  is the new reverse-walking primitive). Callers who relied on a bare
  `Lt`/`Le` filter returning the newest page must switch to `query_last_ids`.

## [0.11.0] — 2026-07-31

Workspace-wide audit pass. Shipped as a minor bump rather than a patch: it
removes cargo features, changes public Rust signatures, and tightens request
validation in ways an existing caller can observe. It also repairs the semver
mistake made by 0.10.1 (see the note in that section).

Every crate in `rs/` moves to 0.11.0 together, and the Python binding
(`py/anda_cognitive_nexus_py`) to 0.5.0.

### Breaking — public Rust API

- **`anda_kip::ErrorObject` is `#[non_exhaustive]`** — Struct-literal
  construction is no longer possible from outside the crate; build errors with
  `ErrorObject::new(code, message)` plus the `with_name` / `with_hint` /
  `with_data` setters, or convert from `KipError` (which fills in the matching
  `name` and `hint`). This closes the hole that made 0.10.1's added `name`
  field a source-breaking change on a patch release, and lets future fields be
  added without another break. The JSON wire shape is unchanged.
- **`anda_kip::CommandItem::WithParams` is a tuple variant** — Its inline
  struct body moved into the new public `ParameterizedCommand`
  (`CommandItem::WithParams(ParameterizedCommand { command, parameters })`).
  Rust pattern matches must be updated; the JSON wire shape is unchanged.
  The move exists so `#[serde(deny_unknown_fields)]` can apply to the item:
  `#[serde(untagged)]` struct variants ignore unknown fields, so a misspelled
  `"params"` key used to deserialize into a valid-looking item with an *empty*
  parameter map, and batch execution then silently substituted the shared
  parameters instead.
- **`anda_db_schema::as_wildcard_map` is public and returns the key too** —
  Signature is now `Option<(&FieldKey, &FieldType)>` (previously a private
  `Option<&FieldType>`). Index key-type resolution has to agree with schema
  validation about which maps are wildcards, and a one-entry approximation
  outside the crate did not.
- **`anda_db` drops the `tantivy` and `tantivy-jieba` cargo features** — Both
  gated nothing: they toggled a second, unused copy of the dependency, while
  `anda_db` depended on `anda_db_tfs` with `features = ["full"]`
  unconditionally, so `cargo check -p anda_db --no-default-features` always
  compiled `tantivy`, `tantivy-jieba` and `jieba-rs`. Tantivy + Jieba
  tokenization is a hard requirement of the crate (`index::bm25` re-exports
  `default_tokenizer` / `jieba_tokenizer` unconditionally and every
  `Collection` installs `default_tokenizer()` on open), so the honest
  representation is no feature at all. `full` remains and now means exactly
  `object_store/fs`. Manifests naming the removed features fail with
  "does not contain this feature" instead of silently doing nothing; drop them.
- **`EncryptedStoreBuilder::with_conditional_put()` is `#[deprecated]`** — It
  has been a no-op since the 0.10.0 immutable-generation refactor
  (`PutMode::Update` / `if_match` / `if_none_match` are always evaluated
  against the logical ETag on every backend). Remove the call; the method
  still compiles and is scheduled for removal.
- **`anda_db_server::ServerInfo::primary_db` is `Option<String>`** — It is
  `None` when `info` is answered for a per-database key, which must not learn
  the instance's layout; an admin still gets `Some(name)`, so the JSON/CBOR
  shape an existing (necessarily admin) client sees is unchanged.
  `AppState::register_db` also takes a fourth argument, the optional API key
  to bind to a newly created database.

### Breaking — request validation and response shape

- **`anda_kip::Request` and `ParameterizedCommand` reject unknown fields** —
  Both carry `#[serde(deny_unknown_fields)]`; JSON bodies that previously had
  extra keys silently ignored now fail to deserialize. This is deliberate:
  `readonly` and `dry_run` default to the *unsafe* value, so a typo
  (`"read_only"`, `"dryRun"`) silently downgraded a validate-only request into
  a committed write.
- **Batch command count is capped** — More than
  `anda_kip::MAX_KIP_BATCH_COMMANDS` (256) commands in one `Request` is
  rejected with `KIP_4002` before any command runs; the count is
  attacker-controlled and drives the result vector's pre-allocation.
- **Parameter substitution output is bounded** — Substitution stops once the
  expanded command exceeds `MAX_KIP_INPUT_LEN`; the oversized string was
  rejected by the parser budget anyway, but a small body could expand without
  limit first (`occurrences × value_len`).
- **`doc.get` JSON responses no longer carry `"field": null` keys** —
  `Document`'s named-map serialization (`Document::try_into`, the
  `doc.get` HTTP payload) omits fields that are absent from the document
  instead of emitting an explicit `null`. `#[serde(default)]` only fills in
  *missing* keys, so the old shape made `Document::try_from` → `try_into` a
  one-way trip for any type that skips a field on serialization. A field that
  *is* present holding `Fv::Null` still serializes as `null`.
- **KML writes to protected schema nodes are rejected everywhere** — `UPSERT`
  concept blocks and `DELETE PROPOSITIONS` now enforce `KIP_3004` on the
  engine-owned schema identities, matching the guards `UPDATE` and
  `DELETE ATTRIBUTES` already had. Previously an `UPSERT` could graft
  attributes or metadata onto the meta-type nodes or the `CoreSchema` domain,
  and `DELETE PROPOSITIONS` could sever e.g.
  `$ConceptType belongs_to_domain CoreSchema` (which `DESCRIBE PRIMER` builds
  its domain map from). Bootstrap keeps working through a private privileged
  path, so the bundled capsules still re-apply on crate upgrade — the
  identical statement submitted by a caller is rejected.
- **KIP JSON literals are case-sensitive** — `TRUE` / `FaLsE` / `NULL` are no
  longer accepted as spellings of `true` / `false` / `null`; the protocol is
  case-sensitive (§2.8.2).
- **KIP keywords require a trailing word boundary** — Multi-word keywords no
  longer glue to the next token: `DESCRIBE CONCEPT TYPESLIMIT 5` used to parse
  as `DESCRIBE CONCEPT TYPES LIMIT 5`. `INTO?b` and `WITH TYPE"Drug"` are
  likewise two tokens again.
- **Out-of-range integer literals are rejected** — `serde_json` (without
  `arbitrary_precision`) degrades an integer literal that does not fit into
  `f64`, storing a *different* number (`18446744073709551617` became
  `1.8446744073709552e19`) so `EXPORT` capsules stopped round-tripping. The
  exact value is recovered when it fits and rejected otherwise, matching how
  overflowing float literals were already handled. `-0` now yields the integer
  `0` rather than the float `-0.0`.
- **`\uXXXX` escapes require exactly four hex digits** —
  `u16::from_str_radix` accepts a leading sign, so `\u+041` used to decode to
  `A`.
- **`AndaDBSchema` requires a serialized `_id: u64` field** — The schema
  builder injects `_id` as a *required* entry and `Document::try_from` reads it
  back out of the serialized value, so a struct without `_id` (or with it
  hidden by `#[serde(skip)]` / `#[serde(skip_serializing)]`) could be derived
  but never stored — it failed at runtime with `field "_id" is required`. It is
  now a compile error with that explanation.
- **`Collection::create_hnsw_index_nx` reports configuration conflicts** —
  When the field already carries an HNSW index whose *persisted* configuration
  differs from the request, it returns an error instead of silently keeping the
  old one; a discarded `dimension` change used to surface much later as a
  `DimensionMismatch` on every insert. Remove and recreate the index to change
  its configuration. `create_btree_index_nx` / `create_bm25_index_nx` are
  unaffected — their field list is their whole configuration.
- **`Collection::query_ids` clamps `limit`** — `limit` is clamped to
  `Collection::MAX_SEARCH_LIMIT` and `None` means "as many as that bound
  allows" rather than "every match"; the call is reachable over HTTP as
  `doc.query_ids` and an unbounded filter materialized one `u64` per matching
  document. `Some(0)` returns an empty result, matching `search_ids`.
- **`anda_db_server` requires an explicit request `Content-Type`** — An absent
  or unrecognized type is answered with `415 unsupported_media_type` instead
  of being parsed as CBOR. Accepting `text/plain` made every RPC endpoint
  reachable as a browser "simple request", i.e. a CSRF surface in the
  supported loopback / `--insecure-no-api-key` modes. Send
  `application/cbor` or `application/json`; response negotiation is unchanged.
- **`anda_db_server` caps `doc.query_ids` and `doc.get_many`** —
  `doc.query_ids` returns at most 1 000 IDs and an omitted `limit` now means
  that bound rather than "every match" (`limit: 0` still returns nothing); a
  `limit` above it is a `400` rather than a silent clamp. `doc.get_many`
  accepts at most 1 000 IDs, each of which costs one object-store fetch. IDs
  are also fetched with the same bounded concurrency `doc.search` uses instead
  of one serial round trip each.
- **`anda_db_server` bounds the database registry** — At most
  `--max-databases` (default 64) non-primary databases may be registered;
  `db.create` / `db.open` / `db.connect` past the limit return `409
  limit_exceeded`. Every registered database costs a permanent background
  flush task. Reopening at startup is never blocked by the bound, so lowering
  it cannot break a restart.
- **`anda_cognitive_nexus_server` no longer returns HTTP 200 for a failed KIP
  execution** — The JSON body is unchanged, but the status now reflects the
  KIP error class: `400` for syntax/schema/reference errors and
  `ResourceExhausted`, `404` for `NotFound`, `409` for `DuplicateExists` /
  `VersionConflict`, `403` for `ImmutableTarget`, `408` for
  `ExecutionTimeout`, `500` for internal or unrecognized codes. A failed KML
  mutation used to be indistinguishable from success to load balancers, retry
  policies, uptime probes, and 5xx alerting.
- **`anda_cognitive_nexus_server` authenticates before parsing the body** —
  `/kip` runs its API-key check as a route layer, so an unauthenticated caller
  always receives `401` instead of a `400`/`413` that revealed how its body
  was processed.
- **`anda_cognitive_nexus_server` bounds the KIP audit log by default** —
  `LOG_RETENTION_DAYS` now defaults to `30`; unbounded retention (`0`) must be
  chosen explicitly. The stored request is capped by the new
  `MAX_LOGGED_REQUEST_BYTES` (default 8 KiB) and truncated above it into a
  still-parseable stand-in; raise it to `MAX_BODY_SIZE` to keep full bodies.
  Previously each request appended its whole body (up to 2 MiB) forever.
- **`anda_db_shard_proxy` rejects unroutable requests** — A request whose path
  carries no valid database name is answered with `404` instead of being
  forwarded to `--default-backend-addr`. With the default `--path-prefix /`,
  `POST /` is the backend's root scope (`db.list`, `db.create`, `db.open`,
  `db.close`), so any tenant able to reach the proxy could enumerate, create,
  or close databases on the shared shard; malformed and percent-encoded names
  were silently funnelled there too. The default backend still serves
  requests that name a database.
- **`anda_db_shard_proxy` drops the `read_only` flag** — It is gone from
  `ShardBackend`, `ResolvedRoute`, the `PUT /_admin/shard_backends` body, the
  NOTIFY payload, and the `shard_backends` DDL. Nothing enforced it: the RPC
  protocol is POST-based with the method inside the body, so the proxy cannot
  classify a request without buffering and parsing every body. Read-only
  enforcement belongs to the backend (`db.set_read_only`). Existing rows keep
  their column, which is simply no longer read.
- **`anda_db_shard_proxy` rejects out-of-range shard ids** — `shard_id` is
  stored in a signed `INT`, so an id above `i32::MAX` was silently written
  negative and became invisible to external tooling; it is now a `400`. A
  negative id read back is logged and ignored instead of wrapping into a huge
  `u32` that routes to a shard no operator configured, and newly created
  tables carry `CHECK (shard_id >= 0)`.

### Breaking — object store format and observable behavior

Metadata now carries an optional commit timestamp. Data written by 0.10.0 and
early 0.11 builds stays readable; new encrypted metadata authenticates the
timestamp, so older binaries cannot verify objects written after this upgrade.
Do not roll back after writing with 0.11.0.

- **`MetaStore`'s logical ETag is a per-commit CAS token, not a content
  hash** — It is now `base64url(SHA3-256(generation ‖ payload))`, seeded with
  the freshly minted per-commit generation, mirroring what `EncryptedStore`
  already did with its per-put nonce. A bare content hash made
  `PutMode::Update(UpdateVersion { e_tag })` a content-*equality* check rather
  than a version CAS: after an A → B → A rewrite a writer holding the token
  for the first A still passed the precondition and silently clobbered both
  intervening commits — a classic ABA lost update.
  **Migration:** every ETag changes on upgrade, and **two puts of identical
  bytes now produce different tokens**. Any consumer that persisted ETags
  across the upgrade must re-read them; a persisted token will no longer match
  and its next conditional update will fail with `Precondition` (safely — it
  errors rather than clobbering). Do not use the ETag as a content digest or
  deduplication key.
- **`copy` / `rename` mint a new logical ETag for the target** — Previously
  the source's token was propagated, so two distinct keys shared one CAS token
  and the target could be handed a token it had already retired; either let a
  stale `PutMode::Update` precondition pass. The target's token is now derived
  from its own fresh generation, exactly as a put's is.
- **`get` / `head` report the committed size** — The authenticated size from
  the metadata commit point, not the backing generation object's length.
- **`list` / `get` / `head` report one `last_modified`** — All three now
  read the explicit commit timestamp from the metadata pointer. Previously
  listings used the `meta/<loc>` object's mtime while `get` / `head` used the
  payload object's — two different clocks for one logical object, so a listing
  and a subsequent read could disagree about when the same object was written.
  Deriving the timestamp from generation creation was also incorrect for long
  puts and multipart uploads, whose generation exists before commit.
  Pre-0.10 documents carry no generation or commit timestamp and still report
  the backend's timestamp; metadata written by early 0.11 builds falls back to
  its generation timestamp.
- **Date preconditions are evaluated in-wrapper and stripped** —
  `if_modified_since` / `if_unmodified_since` are answered against the same
  commit-point `last_modified` the call reports, and removed before the
  request reaches the backend, which would otherwise evaluate them against the
  payload object's own mtime. RFC 9110 §13.2.2 precedence is honoured: an
  `if_match` suppresses `if_unmodified_since` and an `if_none_match`
  suppresses `if_modified_since`, so a paired date condition can no longer
  reach the backend and produce a spurious `Precondition` / `NotModified`.
  Pre-0.10 documents (no generation) still defer to the backend, which matches
  the timestamp such a read reports.

### Added

- **`anda_kip::ErrorObject::new` / `with_name` / `with_hint` / `with_data`** —
  The supported construction path now that the type is `#[non_exhaustive]`.
- **`anda_db_server::ApiError::from_collection_state`** — Classifies a
  collection handle's lifecycle state into the status a client can act on.
- **`anda_db_server` `--max-databases` / `MAX_DATABASES`** — Bounds the
  registry of non-primary databases (default 64).
- **`anda_cognitive_nexus_server` `--max-logged-request-bytes` /
  `MAX_LOGGED_REQUEST_BYTES`** — Caps the KIP request stored in each audit log
  document (default 8 KiB).
- **`anda_kip::MAX_KIP_BATCH_COMMANDS`** — Public batch-size cap (256).
- **`anda_db_schema::FieldValue::try_into_cbor`** — Depth-bounded counterpart
  of the infallible `From<FieldValue> for Cbor`, which truncates an over-deep
  subtree to `Cbor::Null` instead of recursing until the stack is exhausted.
  Both directions now report the same `SchemaError::FieldValue`.
- **`anda_db_tfs::BM25Params::MAX_K1`** — Largest `k1` honored at scoring time
  (1000).
- **`EncryptedStoreBuilder::with_meta_cache_ttl`** — Sets the metadata cache
  TTL while preserving the capacity passed to `EncryptedStoreBuilder::new`.
- **`anda_cognitive_nexus::compare_sort_key` / `compare_order_row`** — The
  total order `ORDER BY` sorts with, exposed for downstream executors.
- **`anda_db_shard_proxy::MAX_SHARD_ID`** — The largest shard id representable
  in the PostgreSQL `int4` column.
- **`anda_db_schema::FieldType::prune_undeclared` /
  `FieldType::is_compatible_upgrade_of`** — Nested-struct schema evolution:
  the first drops stale keys of a removed nested field on the read path, the
  second states which nested-map changes are compatible upgrades.
- **Per-database API keys** (`anda_db_server`) — `db.create` accepts an
  `api_key` for the new database, and the new root-scope methods
  `db.set_api_key` (rotates; generates a CSPRNG key and returns it once when
  none is supplied) and `db.remove_api_key` manage it afterwards. Only the
  SHA3-256 hash is persisted, alongside the database registry in the primary
  database's extensions (`server:api_keys`), and it is compared in constant
  time. The new `anda_db_server::auth` module documents the precedence rules.
  Provisioning requires an admin key, the primary database cannot be
  delegated, and a server started without `API_KEY` while bindings exist in
  storage refuses to start rather than silently serving them unauthenticated.
- **`anda_db_tfs::BM25Index::purge_ids`** — Erases a *set* of document ids
  without their indexed text, for repair paths whose document bodies are gone
  and which therefore cannot call `remove(id, text, now_ms)`. Sweeps every
  posting list once for the whole set, keeps `doc_tokens` / `total_tokens` (and
  the average document length derived from them) consistent with the surviving
  postings, and marks the affected buckets dirty so the purge survives a flush
  and reload. Re-exported as `anda_db::index::BM25::purge_ids`.
- **`anda_db::error::CollectionState` / `CollectionStateError`,
  `DBError::collection_state` / `DBError::is_poisoned`,
  `Collection::state`, and `Collection::is_poisoned` (was `pub(crate)`)** — The
  collection lifecycle state (closing, closed, deleting, deleted, poisoned) is
  now structurally detectable. It travels as a typed source inside the
  `DBError::Generic` a rejected call returns, so downstream crates can classify
  a poisoned handle — and tell "reopen and retry"
  (`CollectionState::is_recoverable`) from "give up" — without matching on the
  message text. No `DBError` variant was added and the rendered message is
  unchanged, so this is purely additive.

### Fixed

- **`Collection::query_all_ids` restores completeness for in-process
  callers** — This release's `query_ids` clamp silently truncated the
  `anda_cognitive_nexus` call sites whose correctness depends on the full
  result set: `DELETE CONCEPT` cascades and `MERGE` re-pointing missed links
  beyond the first 1 000 (leaving dangling references while reporting
  success), KQL type/name matching, full scans and `NOT` evaluation returned
  silently incomplete answers, and the nexus's own `KIP_4002` budget guards
  sat above the clamp and could never fire. The new unbounded
  `query_all_ids` serves those integrity-critical paths; the HTTP-facing
  `query_ids` keeps its documented bound.
- **`Or` filters evaluate every operand unbounded** — `filter_by_id`'s
  `RangeQuery::Or` stopped evaluating operands once the union exceeded
  `limit`, and `Filter::Or` passed the caller's `limit` into each branch even
  though a `Lt`/`Le` branch scans descending and collects the largest keys.
  Both made a page depend on operand order — logically equivalent filters
  returned different pages, and some matching documents were unreachable at
  any page size. `Or` operands now evaluate with no bound, like `And` and
  `Not`, before the canonical ascending trim.
- **A 0.10 B-tree index on a one-field nested struct no longer bricks the
  collection** — 0.10's `BTree::new` accepted any one-entry `Map` field and
  resolved it to the first key's own type; 0.11's stricter resolution
  rejected that persisted shape during `bootstrap`, so `Collection::open`
  failed forever with no way to remove the index. `bootstrap` (and only
  `bootstrap`) now resolves the legacy shape as 0.10 did, with a warning
  recommending the index be recreated.
- **Intent replay sweeps phantom postings by id** — When a retained mutation
  intent's recorded image no longer decodes against the schema, the replay
  skipped it, but index removal is value-keyed, so a posting written under
  the old image's value survived forever: a unique B-tree key kept rejecting
  new documents (`AlreadyExists`) and queries kept matching a value the
  document no longer holds. The replay now sweeps such ids out of every
  index (the same by-id purge `reconcile_storage` uses) before re-indexing
  the stored document.
- **`stream_reader` scales its decompression bound to the object** — The new
  decompression-bomb bound applied the buffered-fetch cap
  (`max_small_object_size * 16`) to the streaming path, whose whole purpose
  is objects larger than `max_small_object_size` — a 100 MiB document
  written by `stream_writer` (or by 0.10) failed its own read path. The
  streaming bound is now 16× the on-disk size (never below the buffered
  cap), which still rejects the tiny-input/huge-output bomb shape.
- **Schema-invalid stored documents stay manageable** — A document persisted
  by an earlier release that a later validation tightening rejects (e.g. a
  wildcard-map key variant 0.10 write paths accepted) made every matching
  `search` fail wholesale and could be neither updated nor removed — a
  permanently poisoned document with no in-band way out. `search` now skips
  such a document with a warning, and `remove` deletes it by sweeping its
  postings by id; `get`/`update` stay strict.
- **`reopen_collections` is serialized against KML execution** — The public
  recovery entry point swapped the two collection slots without `kml_lock`,
  so a host-driven reopen during a running statement let that statement
  finish "successfully" across two collection generations, and a reader
  could observe one fresh and one stale handle. It now takes `kml_lock`
  (internal callers already held it), `save_capsule_version` holds the lock
  like every other mutating step, and both slots swap under their guards in
  one step.
- **Endpoint ID batches use `RangeQuery::Include`** — The subject/object
  batching built an `Or` of per-id `Eq` nodes chunked at 4 096, but filter
  validation caps `Or` at 1 024 branches, so batches of 1 025–4 096 ids were
  rejected at query time — the exact queries the chunking exists to serve.
  `Include` carries up to 4 096 keys as a single node.
- **A read RPC can no longer poison a collection** — `anda_db_server` opened
  collections inline on the cancellable read path, but
  `AndaDB::open_collection` finishes a cold open with `Collection::flush`,
  whose cancel guard poisons the handle when its future is dropped. The first
  `doc.get` on a cold collection that hit the request timeout (or a shutdown)
  therefore abandoned a storage write mid-flight and invalidated a healthy
  collection. The open now runs on its own task and completes even when the
  request that started it is gone.
- **The cancellation policy is declared with the dispatch entry** — The list
  of mutating method names in `anda_db_server` was maintained by hand next to
  the dispatch match, so a new mutating method that nobody added to the list
  silently ran on the cancellable path — a durability classification the
  compiler could not check. Method name, side-effect class, and handler now
  come from one table per scope, and the dispatch match is exhaustive over it.
- **A poisoned or deleted collection is reported honestly** — Every non-active
  collection handle state used to surface as an opaque `internal` 500.
  `anda_db_server` now answers `503 collection_unavailable` for the states a
  reopen recovers (poisoned by a cancelled operation, closing, closed) and
  `410 gone` for a deleted one, using the engine's own
  `CollectionState::is_recoverable` split. A `_id` B-Tree index, which the
  engine rejects, is also refused as a `400` at definition time.
- **`anda_db_server` authorizes before reading the body** — Authorization ran
  inside the RPC handlers, after the `Bytes` extractor had already buffered
  the request body, so an anonymous caller could make the server buffer up to
  `max_body_size` per request and could tell the body-limit `413` apart from
  the `401`. A route layer now rejects unauthenticated `POST`s from the
  headers and the matched path alone — an anonymous oversized body answers
  `401`, matching `anda_cognitive_nexus_server` — and the in-handler check
  remains as defense in depth. `GET /` stays unauthenticated.
- **`collection.ensure` reports HNSW configuration drift as a `409`** —
  `Collection::create_hnsw_index_nx` now refuses a request whose
  configuration differs from the persisted one (see *Breaking*), but
  `collection.ensure` passes the client's configuration on every call and the
  engine's `DBError::Index` is sanitized into an opaque `500` — one that only
  fired on the first load after a restart, making it look intermittent. The
  server now proves the conflict inside the create/open callback and answers
  `409 conflict` naming the field and both configurations, with the
  remove-and-recreate remediation.
- **Database-scope authorization does constant work** — When the named
  database had no per-database key binding (or did not exist),
  `anda_db_server::auth::authorize` skipped the hash-and-compare a bound
  database performs on the presented token, so response timing could reveal
  which names carry per-database keys despite the deliberately uniform `401`.
  The unbound branch now verifies the token against a fixed dummy digest and
  discards the result.
- **Cold-open failures are logged after request cancellation** —
  `anda_db_server` runs `AndaDB::open_collection` on a detached task so a
  cancelled request cannot poison the handle (see above), but when the
  awaiting request was gone its `JoinHandle` was dropped, and the engine open
  paths log nothing — a failed cold open was observed by nobody. The spawned
  task now logs its own failure.
- **`anda_cognitive_nexus_server` shuts down on runaway KIP executions** —
  A timed-out execution was detached with no deadline while
  still holding its bounded mutation permit, so `MAX_CONCURRENT_MUTATIONS`
  expensive requests could exhaust capacity for the rest of the process's
  life (every later request answering "server mutation capacity is
  exhausted", and shutdown escalating to an abort). Detached executions now
  carry a hard deadline of four times the response timeout. Reaching it closes
  admission and initiates process shutdown while continuing to poll the
  mutation; the existing drain path either lets it finish or terminates at a
  crash-recoverable point. The server therefore never cancels a database
  mutation and then continues serving a poisoned collection.
- **Already-registered databases can reopen at the registry cap** —
  A database retained in the registry after a transient startup-open failure
  now reuses its existing slot when `db.open` retries it. Previously a full
  registry rejected the recovery attempt as if it were a new registration.
- **Unusable mutation intents are retired after recovery** — Undecodable
  records and records targeting reserved document id `0` are tracked by path
  and deleted after the next successful checkpoint instead of being skipped
  and logged again on every reopen.
- **Shard-backend notifications remain rolling-upgrade compatible** — 0.11
  listeners still ignore the removed `read_only` routing flag, but upsert
  notifications carry `read_only:false` for 0.10 listeners that require the
  field. Mixed 0.10/0.11 proxy fleets therefore converge on backend moves.
- **`anda_db_shard_proxy` no longer answers 404 during a routing-cache
  resync** — `reload_backend_cache` cleared the live cache before refilling
  it, so every request during the window (any `PgListener` reconnect,
  i.e. a routine network blip) reported "no backend found" instead of the 503
  a routing-store problem is supposed to produce.
- **`anda_db_shard_proxy` validates backend addresses wherever the cache is
  filled** — `validate_backend_addr` ran only in the admin handler, while the
  startup reload and the NOTIFY listener inserted rows verbatim; the admin
  path itself re-entered through the listener. A row written by a DBA, a
  migration, or an older build (`https://…`, `host:port`) made every request
  for every database on that shard fail. Bad rows are now rejected and logged.
- **`anda_db_shard_proxy` range-checks shard ids in NOTIFY events** — The
  NOTIFY listener validated the backend address but accepted the event's
  shard id unchecked, while the startup reload rejects out-of-range rows. A
  hand-crafted `pg_notify` payload with an id above `MAX_SHARD_ID` — which no
  real `shard_backends` row can carry — could insert a cache entry the reload
  path would never load. Such events are now ignored and logged like
  out-of-range rows.
- **`anda_db_shard_proxy` logs backend request failures** — A proxied request
  that timed out or failed answered 504/502 without any log line, unlike
  every route-resolution failure above it, leaving no way to tell which
  backend was unreachable. Both arms now log the backend address (and the
  hyper error for failures).
- **`anda_db_shard_proxy` rejects unroutable database names at assignment** —
  `PUT /_admin/db_shards` accepted any `db_name`, but routing only matches
  `[a-z0-9_]{1,64}`, so assigning a name like `My-DB` answered 200 and
  NOTIFYed yet could never route a single request. Such names are now
  refused with 400. `DELETE /_admin/db_shards` deliberately stays
  unvalidated so rows predating the check can still be removed.
- **`reconcile_storage` repairs index postings for missing documents** —
  Dropping a dead document id from the bitmap is not a repair on its own: the
  derived index postings survive it. A unique B-tree key kept rejecting new
  documents with `AlreadyExists` forever, and `query_ids` / `search_ids` kept
  returning the dead id (`search_ids` does not filter BM25 hits against the
  bitmap), while the dead document's length kept skewing every BM25 score. The
  bodies are gone, so the indexed values cannot be recomputed; each index is
  now purged as far as it can be purged *by id*: HNSW directly, B-tree by one
  sweep of the key space for the whole dead set, and BM25 by one sweep of the
  inverted index through the new `BM25Index::purge_ids`, which also drops the
  dead documents from the counters BM25 scores are derived from. Each sweep is
  `O(index size)` per index, proportional to the `data/` listing
  `reconcile_storage` already performs.

- **B-Tree flush no longer persists postings owned by another bucket** — A
  bucket's key list could name a key whose posting had since migrated
  elsewhere; the flush wrote a stale copy into the non-owning bucket, and the
  higher-numbered bucket then won on load, resurrecting deleted ids. The flush
  now only serializes postings the bucket actually owns.
- **B-Tree scan direction is reported, not guessed** — `Lt`/`Le` range scans
  walk the key space backwards and are trimmed from the head; every other path
  collects ascending and is trimmed from the tail. The direction cannot be
  re-derived from the filter AST (composite `And`/`Or`/`Not` filters evaluate
  their operands unbounded and always yield ascending results), so guessing
  from the AST made logically equivalent filters return disjoint pages —
  `Between(5, 14)` and `And(Ge(5), Lt(15))` returned opposite ends of the same
  key range. The scan now reports its own direction, and `query_ids` returns
  the same ids for equivalent filters. A `Filter::Id` inverted `Between` that
  matches nothing now returns empty instead of a spurious page.
- **HNSW forward edges never point below their layer** — Neighbor selection
  could record a forward edge to a node that does not exist at the current
  build layer (the reverse edge was correctly skipped), leaving a dangling
  half-edge that search had to defend against. Candidates below the build layer
  are now skipped on both sides, and neighbor selection skips candidates that
  are no longer present in the node map.
- **BM25 average document length is derived, not cached** — The cached
  `avg_doc_tokens` could disagree with `total_tokens` / `doc_tokens.len()` (in
  particular after a `load_metadata` with no documents loaded yet, which seeded
  it from persisted stats). It is now computed on demand, once per query and
  once per `stats()` call.
- **BM25 parameters can no longer produce `NaN` scores** — `k1` and `b` arrive
  straight from deserialized queries; a finite-but-huge `k1` (`f32::MAX` passes
  `is_finite`) overflowed the scoring formula to `inf` and turned the score into
  `inf / inf = NaN`, which broke the ranking comparator's total order. `k1` is
  clamped to `[0, MAX_K1]`, `b` to `[0, 1]`, and non-finite values fall back to
  the defaults. Score comparison is a total order in the presence of `NaN`.
- **BM25 token counters stay consistent under concurrent insert/remove**, and
  `remove` keeps a bucket token that a concurrent insert recreated.
- **`compact_buckets` is exclusive with mutations again** (`anda_db_tfs`,
  `anda_db_btree`) — 0.10.0 deleted the internal mutation gate and made
  "coordinate compaction against writes" the caller's problem (see *Changed —
  de-complexity pass* under 0.10.0). That contract was not safe to hand out:
  compaction rebuilds the bucket map non-atomically, so a posting created
  after it snapshotted `postings` was re-binned into nothing and silently
  dropped by the next flush. Both crates restore a `mutation_gate` that
  `insert` / `insert_array` / `remove` / `remove_array` take **shared** (they
  still run concurrently with each other) and `compact_buckets` takes
  **exclusively**; it is the first lock a mutation acquires, so it never nests
  inside a `DashMap` shard guard. **Callers no longer need to serialize
  compaction against writes.** Excluding `flush` against mutations,
  compaction and other flushes remains the caller's responsibility, unchanged.
- **KIP parser budget cannot be defeated by a quote inside a comment** — The
  budget pre-scan did not skip line comments, so a lone `"` in a comment
  latched its in-string state for the rest of the input: every bracket after it
  went uncounted, the depth guard was bypassed entirely, and the parser then
  recursed once per bracket and overflowed the stack. The mirror failure
  (brackets inside a string literal counted against the budget, rejecting valid
  input) is fixed by the same change.
- **Truncated KIP escapes report a location** — Streaming `take`/`tag`
  combinators returned `Incomplete`, which `format_nom_error` renders without
  line, column or context; the `complete` variants are used so a truncated
  `\uXX` is a located parse error.
- **Batch responses keep their partial result** — A batch item that failed with
  a partial result (e.g. an over-budget KQL query carrying the page it did
  manage to produce) had the response rebuilt without it, silently dropping the
  data.
- **`"error": null` deserializes as success** — The canonical JSON-RPC success
  shape spells an absent error as an explicit `null`; `Response`'s
  deserializer treated it as an unparseable error payload.
- **Placeholder-misuse hints are produced up front** — The post-hoc hint
  rewriting that ran after a `KIP_1xxx` failure is replaced by the existing
  `validate_placeholder_usage` pre-check, so the hint no longer depends on the
  parser having failed in a particular way.
- **`Json` field values survive a storage round trip** — A `FieldType::Json`
  field reads back as its plain `Map` / `Array` / primitive shape;
  normalization now rebuilds the declared `Fv::Json` variant instead of leaving
  the read-back shape in place.
- **Wildcard maps enforce the declared key variant** — A `Map<Text, T>` could
  be filled with integer keys: the value validated structurally but could never
  be deserialized back into its declared Rust type. Non-wildcard maps are
  rejected rather than indexed by name.
- **`FieldValue` → CBOR conversion is depth-bounded** — Deeply nested values no
  longer recurse until the stack is exhausted (see `try_into_cbor` above).
- **An unusable write-ahead intent no longer makes a collection unopenable** —
  Nothing cleared such a record, so every reopen replayed it and failed
  identically with no operator escape hatch. Unusable records are logged and
  skipped (the treatment a schema-mismatched document already got) and retired
  by the next flush.
- **`Storage::connect` adopts the caller's path after relocation** — A database
  directory moved on disk kept the recorded path and read from the old
  location.
- **`AndaDB::auto_flush` no longer swallows its final close error** — The
  cancellation path discarded the result of `close()`, and callers observe the
  task only through a `JoinHandle<()>`, so a failed shutdown flush was reported
  as a clean shutdown. It is now logged at error level. (`auto_flush` still
  returns `()`; it is not a `Result`.) `set_extension_from` likewise logs the
  values it drops instead of discarding them silently.
- **`read_only` is live handle state, not durable state** — `Collection::open`
  always starts a handle writable, so the flag is written as `false` in every
  persisted snapshot; only `stats()` / `metadata()` report the live value.
- **Object-store garbage collection cannot reclaim an in-flight generation** —
  A freshly written payload is unreferenced between the payload write and the
  pointer switch; it is now registered as in-flight for that window (and for
  the whole of a multipart upload), so a concurrent `collect_garbage` cannot
  delete a generation that is about to be committed. A cold read can no longer
  resurrect a replaced pointer.
- **Legacy-layout objects heal the stale-pointer `NotFound` like generational
  objects do** (`anda_object_store`) — The read paths' backend-`NotFound`
  re-resolve was gated on the cached document carrying a generation, so a
  cached pre-0.10 (generation-less) pointer was never refreshed after the
  key's first overwrite migrated it to the generation layout and deleted
  `data/<location>`: `get` / `get_ranges` / copy / rename through that cache
  answered a spurious `NotFound` for an object that exists, sticky until the
  cache entry expired (up to an hour). The re-resolve now runs once regardless
  of the cached pointer's layout.
- **`EncryptedStoreBuilder::with_meta_cache_ttl` keeps the configured
  capacity** — It used to rebuild the cache with the default capacity,
  silently discarding the value passed to `new`. It still discards a cache
  previously supplied through `with_meta_cache` (which replaces the built-in
  cache wholesale, capacity and eviction policy included), so the two are
  order-sensitive; both behaviors are now documented.
- **KQL `FIND` columns that mix entity ids and predicate names** — Reachable
  through `UNION` (branches merge by variable name, and a variable in the
  predicate position binds a name). One kind used to be dropped, which also
  contradicted `COUNT(?x)`. Mixed columns now paginate with a numeric offset
  cursor over one deterministic order. An invalid cursor on a single-column
  projection is rejected instead of being misread.
- **KQL `ORDER BY` is a total order** — Sort keys are partitioned into the
  classes on which `compare_json` applies a single rule (booleans, numbers and
  numeric-looking strings, datetime strings, other strings, containers, null),
  so sorting is deterministic across mixed-type columns.
- **Removing a field from a nested struct no longer bricks stored documents** —
  A non-wildcard `FieldType::Map` (what `#[derive(FieldTyped)]` emits for a
  nested struct) names its keys one by one and `validate` rejects undeclared
  ones, so dropping one nested field made every already-stored document
  unreadable. Undeclared keys are now pruned on the read path just before
  validation — the same treatment a value stored under a retired top-level
  field index already got — and disappear on the next rewrite. Schema upgrade
  compatibility follows the same rule as top-level fields: a nested map may
  gain an *optional* key or lose a key; a changed key type, a new *required*
  key, and any change of wildcard-ness or key variant remain incompatible.
- **Shard ids round-trip through PostgreSQL** — Conversion to and from the
  `int4` column is checked against `MAX_SHARD_ID` instead of wrapping; an id
  above `i32::MAX` used to be stored negative and become invisible to every
  reader.
- **Unroutable shard-proxy requests are rejected, not sent to the default
  backend** (security) — A request whose database name could not be resolved
  fell through to `default_backend`. With the default `--path-prefix /`,
  `POST /` carries no database name at all and is the backend's *root* scope
  (`db.list`, `db.create`, `db.open`, `db.close`), so any tenant that could
  reach the proxy could enumerate, create or close databases on the shared
  shard; the same catch-all also swallowed every malformed or percent-encoded
  name. The default backend keeps its legitimate purpose: a valid database
  name with no routing row is still served by it. `ResolvedRoute` drops its
  `read_only` field — read-only is backend RPC state
  (`db.set_read_only` / `collection.set_read_only`), never proxy routing state.
- **One API key no longer opens every database** (`anda_db_server`, security)
  — The single process-global key authorized *all* scopes, so any holder
  could `POST /{any_db}` (including `doc.remove` and `collection.delete`) and
  reach the root scope. Keys now have two tiers: the configured `API_KEY` is
  the **admin** key (root scope plus every database), and a database may be
  given its **own** key, accepted on `POST /{that_db}` only and never at the
  root scope. Unauthorized database-scope requests all return the same `401`,
  so a caller cannot probe which databases exist, and database-scoped `info`
  stops enumerating the instance for a per-database key. Existing deployments
  are unaffected until they provision a per-database key: with none bound,
  every database falls back to the admin key exactly as before, and the
  keyless loopback mode is unchanged.

### Documentation and packaging

- **The Python binding builds again** (`py/anda_cognitive_nexus_py`) — Its
  manifest inherited `pyo3`, `pyo3-asyncio`, `serde-pyobject` and
  `pyo3-build-config` from `[workspace.dependencies]`, where none of them was
  declared, so `cargo metadata` on it failed outright; and it pinned its four
  sibling crates at `"0.9"`, which cannot match `0.10.x`. The pyo3 family is
  now declared at the workspace root (pinned to the 0.20 line, the last one
  `pyo3-asyncio` supports — CPython 3.7–3.12; set `PYO3_PYTHON` when the
  `python3` on PATH is newer) and the sibling requirements are `"0.10"`. The
  documented `cargo test -p anda_cognitive_nexus_py --lib` flow, broken for two
  minor releases, passes again; `make test-py` now checks that the workspace
  member is uncommented first.
- **`AGENTS.md` no longer drifts from `CLAUDE.md`** — The two files are the
  same document (`AGENTS.md` was stale by two minor versions, telling every
  non-Claude harness to pin `anda_db` 0.8 and `object_store` 0.13). `CLAUDE.md`
  is the source; `AGENTS.md` is a byte-identical copy regenerated by
  `make sync-agents-doc`, and `make lint` fails when they differ.
- **`README.md`** — The copy-paste dependency block pinned `anda_db = "0.8"`,
  two minor versions behind the example ten lines below it.
- **`skills/anda-db/references/anda_db_quick_ref.md`** — Pinned `0.9` while
  `SKILL.md` in the same skill said `0.10`, and told readers to call
  `EncryptedStoreBuilder::with_conditional_put()` for compare-and-swap
  semantics that have been unconditional since 0.10.0.

## [0.10.1] — 2026-07-20

### Semver note — 0.10.1 should have been 0.11.0

This release added the public field `name` to `anda_kip::ErrorObject`, a fully
public, non-`#[non_exhaustive]` struct, and shipped it as a **patch** bump. Any
consumer that built an `ErrorObject` with a struct literal — the only
documented way at the time — stops compiling with ``missing field `name` `` on
a plain `cargo update` while pinned to `anda_kip = "0.10"`.

The same release changed
`anda_cognitive_nexus::normalize_search_score` from `(f32, f32) -> f64` to
`(f32) -> f64`, also source-breaking, also on a patch bump.

Both are addressed in 0.11.0: `ErrorObject` becomes `#[non_exhaustive]` with an
`ErrorObject::new` + `with_*` construction path, so this cannot recur.
Consumers pinned to `anda_kip = "0.10"` who cannot take a source break should
pin `=0.10.0`.

### Added

- **`ErrorObject::name`** (`anda_kip`) — Optional semantic error name
  accompanying the `KIP_xxxx` code (`"InvalidSyntax"`, `"TypeMismatch"`, …), so
  an LLM does not have to memorize the code table. Serialized only when
  present (`skip_serializing_if`), and `#[serde(default)]` on the way in, so
  the JSON wire shape stays backward compatible in both directions.
  `From<KipError>` fills it in automatically. **Source-breaking for struct
  literals** — see the semver note above.

### Changed

- **BM25 search scores are absolute, not relative to the top hit**
  (`anda_cognitive_nexus`; breaking behavior) — `META SEARCH` scores were
  normalized as `score / max_score` over the returned set, so the best hit
  always scored `1.0` however weak the match, and the same document scored
  differently depending on what else came back. Scores now use the
  corpus-independent saturation curve `score / (score + 2)`, rounded to six
  decimals, which makes `THRESHOLD` the honest-miss gate KIP §5.2.2 describes.
  **Every existing `THRESHOLD` value now means something different** and must
  be recalibrated: values tuned against the relative curve (where the top hit
  was pinned at `1.0`) will be far too aggressive against the absolute one.
  `normalize_search_score` loses its `max_score` parameter —
  `(f32, f32) -> f64` becomes `(f32) -> f64`, source-breaking; see the semver
  note above.
- **Unbounded `{m,}` multi-hop queries fail instead of answering partially**
  (`anda_cognitive_nexus`; breaking behavior) — An unbounded quantifier is
  still capped at the 10-hop engine limit, but if the transitive closure is
  incomplete at the cap the query now fails with `KIP_4002` rather than
  returning the paths found so far. A partial closure is a silently wrong
  answer (KIP §3.4.2). Queries that used to return partial results now error;
  bound the quantifier explicitly (`{m,n}` with `n <= 10`) or traverse in
  stages.
- **Read-only mode rejects KML with an actionable hint** (`anda_kip`) — The
  generic "check parentheses" syntax hint is replaced by a message naming the
  rejected statement kinds and the recovery action. The code stays `KIP_1001`
  until the spec defines a permission-denied code.

### Documentation

- `docs/anda_kip.md` and `docs/anda_cognitive_nexus.md` synced to KIP v1.0-RC10
  and the row-based solution model; `KIPSyntax.md`, `SPECIFICATION.md`, the
  system/self instruction documents and the `Insight` / `Preference` capsules
  refreshed.

## [0.10.0] — 2026-07-16

De-complexity release. 0.9.2 was never published; its review-driven hardening
(see "0.9.2 (unpublished)" below) ships here together with a follow-up review
that fixed defects the hardening itself introduced, and a structural
simplification pass that removes the machinery built for extreme edge cases.
The release rests on three explicit contracts:

1. **Single writer per database** is a deployment contract; each metadata
   object keeps one conditional PUT as the last defense against a second
   writer, and a `Precondition` conflict is never reconciled in place.
2. **Cancellation is a crash (poison-on-cancel).** Dropping a mutating future
   mid-operation — or a storage write failing with an unknown outcome —
   poisons the collection handle; every further operation on it errors.
3. **Recovery happens only on reopen**: write-ahead intent replay plus a
   repair scan bounded exactly by the new allocation watermark.

### Migration note: poison-on-cancel

Do not wrap AndaDB mutating calls (`add*`, `update`, `remove`, `flush`,
`close`, extension writes, compactions) in `tokio::select!`/`timeout`.
A cancelled mutation poisons the collection handle; reopen it via
`AndaDB::open_collection`, which discards the poisoned handle and recovers
from storage. The built-in drivers (`auto_flush`, both HTTP servers) never
cancel mutating futures.

### Migration note: on-disk formats roll forward only

Four durable formats gained fields or layouts this release. All of them read
existing data and upgrade it lazily; **binaries older than 0.10 cannot read
data written by 0.10** (do not roll back after writing):

- object store immutable-generation layout (see the dedicated note below),
- BM25/B-Tree bucket manifests (see Changed),
- the collection allocation watermark object (`alloc_watermark.cbor`),
- the schema allocation watermark (`next_idx`, serialized with each schema).

### Migration note: HNSW deletion repair is now opt-in

`HnswConfig::reconnect_on_delete` now defaults to **`false`** for new
configurations *and* for persisted metadata that lacks the field — which
matches the behavior those indexes were built with, since `remove()` in every
published release through 0.9.1 pruned the reverse edges without any re-link
step. (The unconditional re-link, and the `true` default that preserved it,
existed only in the unpublished 0.9.2.) Neighbor repair keeps recall stable under delete-heavy
workloads but runs O(M²·L) distance computations while holding the
structural write lock; enable it explicitly when recall stability matters
more than deletion throughput.

### Migration note: object store immutable-generation layout

`anda_object_store` replaced the mutable dual-object sidecar protocol with an
immutable-generation layout. A logical put now writes the payload to a fresh,
never-overwritten object `gen/<path>/<generation>` and commits by atomically
switching the pointer inside the metadata document `meta/<path>` (a single
backend put). Consequences:

- **Torn states are gone by construction.** A crash before the pointer switch
  leaves the previous version fully readable (previously the old version was
  already overwritten and unrecoverable, and `EncryptedStore` surfaced the
  window as an AES-GCM authentication failure); a crash after it means the
  write took effect.
- **Format rolls forward only.** Pre-0.10 data (payload at `data/<path>`, no
  generation pointer) stays fully readable, and the first overwrite of a key
  migrates it to the new layout. Metadata written by this version carries the
  generation pointer, which older releases do not understand — **do not roll
  back to a pre-0.10 binary after writing with this version.**
- **Garbage collection.** Replaced payloads are deleted best-effort right
  after each commit; crash leftovers are reclaimed by the new explicit
  mark-sweep `MetaStore::collect_garbage` / `EncryptedStore::collect_garbage`
  (run it at open or on a maintenance schedule). The collector reads every
  commit point before deleting anything and re-checks each key immediately
  before each deletion, so referenced payloads are never deleted.
- **Versions are no longer reported.** `PutResult::version` and
  `ObjectMeta::version` are `None` (replaced generations are reclaimed
  eagerly, so version-addressed reads cannot be honoured); conditional
  updates use the content-addressable ETag. Persisted
  `ObjectVersion { version: None }` tokens from `LocalFileSystem` deployments
  keep working unchanged.
- **Conditional semantics unified.** `PutMode::Create` is now arbitrated
  cross-process by a conditional write of the commit point (strictly stronger
  than the old orphan self-heal, which weakened `Create`);
  `PutMode::Update` / `if_match` / `if_none_match` are evaluated against the
  logical ETag on every backend. `EncryptedStore`'s `with_conditional_put()`
  is a retained no-op: those semantics are always on, and `EncryptedStore`
  now always exposes the logical (ciphertext-hash) ETag.
- **Listings enumerate commit points** (`meta/`), so uncommitted payloads and
  crash leftovers are invisible; entries report the logical size and ETag.
  A metadata document that no longer decodes is skipped with a warning in
  compatibility mode (strict `EncryptedStore` mode still fails the listing),
  reads of such a key keep failing loudly, and an overwrite rebuilds it.
- **Removed machinery**: the per-key mutation-lease table, the
  `heal_create` orphan self-heal branches, the conditional-GET rewrite of
  backend ETags, and the listing orphan-tolerance dance are all gone —
  the immutable-generation protocol makes them unnecessary.

### Changed — de-complexity pass

- **Poison-on-cancel recovery model** (`anda_db`; breaking behavior) — The retained-write/read-back state machines (kept payload snapshots, expected-version slots, `Precondition` read-back comparison — four copies across collection metadata, B-Tree, BM25 and HNSW adapters), the pending-checkpoint retry state and the in-flight-add checkpoint clamp are deleted. In their place: a cancelled mutating future or an unknown-outcome storage failure moves the handle to a poisoned lifecycle state, `AndaDB::open_collection` transparently discards a poisoned handle after draining it, and reopening converges from storage (intent replay + repair scan). Handles no longer attempt to "resume in place" after cancellation.
- **`add` writes no per-mutation WAL record** (`anda_db`) — A durable **allocation watermark** (`alloc_watermark.cbor`, published one small PUT per 64 allocations) guarantees every id that may have a document object lies inside `checkpoint+1 ..= max(metadata max, watermark)`. The reopen repair scan probes exactly that window — the consecutive-miss heuristics (and their "more than N consecutive holes can hide documents" failure mode) are gone. Updates and removes keep their durable intents.
- **Flush no longer replays mutation intents** (`anda_db`) — Under the poison contract a live handle only ever holds completed intents, so the normal flush path persists dirty state and retires the intent log without the delete-and-rebuild reconciliation the unpublished 0.9.2 follow-up ran on every flush (which re-inserted every pending document into every index, including re-randomizing HNSW placements). Reconciliation now runs only on reopen.
- **KQL solver rewritten on a row-based solution model** (`anda_cognitive_nexus`) — The parallel representations (columnar entity/predicate bindings, fixed 3+1-slot relation rows, grouped-pair maps, alignment marker sets), the four FIND fallback projection paths and the three FILTER sub-paths are replaced by a single `SolutionTable` (header + rows, columnar layout) with relational operators: natural join, left join (OPTIONAL), padded union (UNION), row-level FILTER, tuple anti-join (NOT), group-aggregate and §3.3 projection dedup. `kql.rs` shrinks from 3,326 to 1,482 lines. Fixed by construction, each with a regression test: concept-only UNION no longer degenerates into a cartesian product; UNION branches with multi-pattern equi-joins no longer lose solutions; NOT anti-joins with 3+ shared variables remove exact tuples only; FILTER after UNION filters both branches; UNION branches are no longer limited to 3 entity / 1 predicate variables.
- **Shard proxy routing caches are invalidate-only** (`anda_db_shard_proxy`) — Administrative writes and NOTIFY events no longer back-fill caches from request payloads (racing back-fills could re-apply an older commit); they only invalidate, and lookups re-resolve against PostgreSQL. The `route_generation` epoch machinery is deleted; the positive route TTL (now 30s) is the convergence bound for the one remaining stale-insert race, and the backend mirror is maintained solely by commit-ordered NOTIFY events.
- **Index-crate concurrency contracts narrowed** (`anda_db_tfs`, `anda_db_btree`, `anda_db_hnsw`) — The internal mutation gates, the hand-written persistence gates (three verbatim copies of an async mutex), BM25/B-Tree empty-posting flush healing and B-Tree's dirty-bucket counter with its drift-healing CAS are deleted. Coordinating mutations against flush/compaction — and flushes against each other — is the caller's responsibility; `anda_db`'s `Collection` already holds an exclusive operation gate across every flush. HNSW additionally drops its per-layer id tracker (entry-point replacement falls back to an O(N) scan on the rare paths that need it).
- **Schema tracks an allocation watermark** (`anda_db_schema`) — `Schema` persists `next_idx`, the exclusive upper bound of every field index the schema lineage ever allocated. This fixes a latent index-reuse bug (removing the highest-indexed field let the next upgrade reallocate its index, so stale values of the removed field could be misread as the new field) and lets `try_from_doc`/`set_doc` distinguish retired-field leftovers (still silently dropped) from indexes the lineage never allocated — which are now **rejected** instead of silently deleted on the next rewrite.
- **`Filter::Or` is order-independent** (`anda_db`) — Every branch is evaluated (each bounded by `limit` on its own) and the union is returned in canonical order; previously evaluation stopped once the union reached `limit`, so equal boolean sets returned different results depending on operand order.
- **Server registry persistence is part of RPC success** (`anda_db_server`) — `db.create`/`db.open` unwind their registration (and close the database) when the registry write fails, and `db.close` reports the failure and stays retryable; previously the registry write was best-effort and a "successful" lifecycle transition could silently not survive a restart.
- **HNSW bootstrap sweeps orphan node blobs** (`anda_db`) — A crash between the ids PUT and the metadata PUT used to leak the removed node's blob forever (no load or purge would ever visit it); bootstrap now deletes node blobs that neither the id set nor the tombstone set references.
- **`EncryptedStore` ETags are seeded with the per-commit nonce** — The logical ETag is now SHA3-256 over the random per-commit nonce followed by the ciphertext. Hashing the bare ciphertext collided for short payloads (a one-byte ciphertext has only 256 possible values), which let a stale CAS token pass a conditional update and silently lose writes once conditional updates switched to logical ETags; found by an OCC stress test that now guards the property.
- **UPSERT self-loop preflight propagates read errors** (`anda_cognitive_nexus`) — Only a definite NotFound degrades to "new concept"; storage or index failures during the preflight now propagate instead of silently skipping the self-loop check it exists to perform.

### Fixed

- **I64 read-back shape no longer corrupts B-Tree indexes** — Stored documents deserialize non-negative `i64` values as `U64` (and `f32` as `F64`); 0.9.2 accepted these shapes at validation but left them un-normalized, so single-field `I64` B-Tree index maintenance silently no-opped: `update` left stale keys behind (queries returned documents by their *old* values), `remove` leaked index entries forever, and creating an index over existing data failed outright. Read-back shapes are now normalized to the declared variant at every document materialization boundary (`try_from_doc`, `set_doc`, `set_field`), and the B-Tree scalar paths additionally tolerate in-range `U64` for `I64` indexes as defense in depth.
- **Collection lifecycle deadlock and double-writer windows** — The global `create_lock` introduced in 0.9.2 was held across the user callback, so creating another collection from inside an `open_or_create_collection` callback deadlocked forever; it also serialized unrelated creations. Replaced with per-name lifecycle locks shared by create / open (slow path) / close / delete: nested creation of a *different* collection works again, `close_collection` no longer races a concurrent `open_collection` into two live writers on one storage prefix (the 0.9.2 ordering removed the handle from the registry *before* flushing, letting an open load a second writable instance whose index writes the closing flush then overwrote), and both create paths re-check `dropping_collections` after acquiring the lock. Lifecycle operations on the *same* name from inside a creation callback are documented as unsupported. `close_collection` failure now puts the (read-only) handle back into the registry so the un-flushed state can be retried instead of being silently dropped.
- **`save_extension` durability window and write amplification** — 0.9.2 routed `save_extension`/`remove_extension` through a full collection flush; a concurrent flusher that had already claimed the metadata version made the call return `Ok` while the winner's write could still fail or be lost to a crash, and every call rewrote the ids bitmap and flushed all indexes. They now perform one unconditional small metadata put (with version precondition and bounded retry) that does not advance the flush watermark: `Ok` means durably written, and the next full flush still persists everything else.
- **Storage cache capacity semantics restored** — 0.9.2 reinterpreted the *persisted* `cache_max_capacity` from entry count to KiB weight, collapsing read-cache hit rates on existing deployments with no way to override (the persisted value wins). `cache_max_capacity` is entry-count again; byte-based bounding is now the separate, optional `cache_max_bytes` (serde-default, backward compatible with existing `storage_meta.cbor`).
- **Hybrid search no longer discards BM25 hits** — A `text`+`vector` query against a collection with no dimension-matched HNSW index degrades to the full-text results with a warning (0.9.2 made the whole query error, dropping valid BM25 hits); a *vector-only* query still errors as intended.
- **F32 JSON read-back** — The 0.9.2 "F64 read-back" acceptance only covered CBOR's exact widening; JSON serializes `f32` via shortest-decimal, so values like `2.71` failed validation on round-trip while `1.25` passed. Both read-back forms are now accepted (and normalized); out-of-range or genuinely lossy `f64` values are still rejected.
- **KIP exact numeric comparison** — 0.9.2's unified `==`/`!=` compared all numbers as `f64`, so integers above 2^53 falsely compared equal (`9007199254740993 == 9007199254740992` was true) and `MIN`/`MAX` could select the wrong element. Integer×integer comparisons now use `i128` (matching `SUM`), integer×float comparisons avoid rounding the integer, and `MIN`/`MAX` are correct at the extremes.
- **KIP loose-equality semantics tightened** — String×string equality is exact again (`"1.10" == "1.1"`, `"1e3" == "1000"`, `"3" == "3.0"` are all false now; datetimes representing the same instant remain equal), while number×string comparisons coerce numerically (`3 == "3.0"` is true). Arrays/objects compare by structural equality again (0.9.2 made `[1] == [1]` false and `[1] != [1]` true — a value unequal to itself). `==`/`!=` are exact negations, exposed as the new public helper `anda_kip::loose_equal`, and the KQL `IN` filter now uses the same equality so `IN(?x, [v])` and `?x == v` cannot disagree. Datetime comparison works across RFC 3339 and RFC 2822 spellings of the same instant.
- **KIP parse-error quality** — `LIMIT 0` (or any invalid LIMIT operand) reports "LIMIT takes a positive integer" at the right position instead of a misleading "unexpected trailing content"; duplicate-key errors point at the duplicated key instead of the start of the object.
- **Derive: bare generic fields with a `FieldTyped` bound compile again** — 0.9.2 unconditionally rejected bare type-parameter fields; the pre-0.9.2 pattern (a `FieldTyped` trait bound supplying `field_type()`) is accepted again, and only unbounded bare generics without `#[field_type]` are compile errors.
- **KQL NOT/grouped aggregation consistency** — The 0.9.2 per-solution anti-join left the old column-wise `ctx.groups` cleanup in place, so grouped `COUNT` over a `NOT`-filtered pattern returned 0 even when members survived; group membership is now narrowed to surviving rows. `NOT` blocks with multiple clauses no longer over-exclude: the excluded tuple set is narrowed by the block's other clauses (`NOT { (?a,"blocked",?b) ?b {type:"Bot"} }` only excludes pairs where `?b` *is* a Bot).
- **KQL dangling-id graceful degradation** — 0.9.2's existence preflight threw `KIP_3002` from *any* execution path; per spec §3.4.7, a dangling `{id:}`/`(id:)` inside `NOT` now makes the clause succeed, inside `OPTIONAL` preserves the outer solution with `null` bindings, and inside a `UNION` branch contributes an empty set — only main-pattern references still fail fast. Storage I/O errors are still propagated everywhere.
- **KQL row-wise UNION with hetero-signature siblings** — A UNION branch's rows were only merged into pattern relations with an *identical* variable signature, so adding a semantically redundant clause that additionally bound a proposition variable silently dropped branch solutions; branch rows now pad into every compatible relation with `null` for unbound slots.
- **KQL solution dedup keys** — Three dedup paths keyed solutions by `|`-joined strings, merging distinct solutions when a predicate contained `|`; keys are structural now.
- **BM25 crash-safe flush ordering (grow-then-shrink)** — 0.9.2's "buckets before metadata" ordering *widened* the crash window for token migration (a token moved to a freshly allocated bucket could vanish with all its postings, and the orphaned file was later overwritten); `flush` now writes migration-target buckets first, then metadata, then rewrites of already-referenced buckets, with the inverse ordering auto-detected after compaction shrinks. The `anda_db` production path now delegates to this single implementation (it had silently kept the old metadata-first order). *Superseded in this release by the bucket-manifest protocol (see Changed), which removes the ordering problem by construction.*
- **HNSW purge persists** — `purge_removed_nodes` now bumps the metadata version so the cleared tombstone set is persisted by the next flush; previously purged deletions were replayed on every reload and the tombstone set grew without bound (a warning now fires past 10k pending tombstones).
- **`UniqueVec` strict panic safety** — The 0.9.2 reordering still had a window (set insertion unwinding after the vec push) that could later admit duplicates, violating the uniqueness invariant B-Tree postings rely on; mutations are now guarded on both sides, including the `remove` variants.
- **B-Tree depth-cap visibility** — Range queries exceeding the depth cap log a warning (index name and depth) instead of only returning a silently empty — and for `Not` roots, semantically inverted — result.
- **Server request timeouts are cancel-safe** — Both `anda_db_server` and `anda_cognitive_nexus_server` wrapped mutating dispatch in `tokio::time::timeout`, truncating non-cancel-safe operations at arbitrary await points; in the worst case a timed-out `db.close` left a detached auto-flush task holding the database open while a retried `db.open` created a second writer. Dispatch now runs on its own spawned task (timeout returns 408, the operation completes in the background), `close_db` cancels the flush token before any await, and `DbEntry` cancels on drop as a backstop.
- **Server shutdown and slow-body hardening** — A shutting-down server rejects new RPCs with 503 instead of racing the database close; an outer route-level timeout (2× request timeout) bounds the whole request including body reading, so a slow-transmitting client cannot hold a connection open indefinitely.
- **Non-loopback startup requires `API_KEY` everywhere** — 0.9.2 added the check only to the shard proxy; `anda_db_server` (which exposes full CRUD) and `anda_cognitive_nexus_server` (arbitrary KML mutation) now also refuse to start on a non-loopback address without an API key unless `INSECURE_NO_API_KEY` explicitly opts in; empty keys are always rejected.
- **Shard proxy negative-cache DoS** — 0.9.2 started caching negative db-route lookups in an unbounded map that only expired logically; unauthenticated requests probing random names could grow proxy memory without limit. The db→shard cache is now a bounded moka cache (100k entries) with per-variant physical TTLs.
- **Shard proxy reconnect resync actually runs** — sqlx's `PgListener::recv()` reconnects transparently and silently drops NOTIFY events, so the 0.9.2 "clear caches on reconnect" never fired for common network blips; the listener now uses `try_recv()` and resyncs the routing caches when it reports a reconnect.
- **Sidecar listings tolerate corrupted metadata** — A sidecar document that exists but fails to decode (torn write before a crash) is treated like a missing one during `list` (entry surfaces with no logical e-tag) instead of failing the whole scan — matching the write path, which already self-heals such objects on the next put. The cross-process weakening of `PutMode::Create` by the orphan self-heal path is now documented as part of the single-writer assumption.
- **`list_logs` limit=0 and `prune_logs` liveness** — `limit=0` returns an empty page (0.9.2 clamped it to 1 and returned data, contradicting KIP's `LIMIT 0` parse error); `prune_logs` exits when a round deletes nothing instead of spinning on a pathological index/document mismatch.

### Changed

- **BM25 & B-Tree bucket persistence replaced by a manifest protocol** (`anda_db_tfs`, `anda_db_btree`; breaking API, rolling on-disk format) — The multi-phase "grow-then-shrink" write ordering, the saved-bucket watermark, and the crash-repair heuristics are gone. Every flush now writes dirty buckets to **fresh immutable objects** keyed by `(bucket_id, generation)` and then commits the metadata, whose new `buckets` manifest (`bucket_id -> generation`) is the loader's single source of truth. The metadata write is the only atomic commit point: a crash before it leaves the previous snapshot fully intact (new objects are unreferenced garbage); after it, the replaced objects are returned as `FlushOutcome::obsolete` for best-effort deletion. Compaction no longer has a special persistence contract — its layout becomes durable atomically with the next manifest commit.
  - **Format rolls forward**: metadata written by earlier releases (no manifest) still loads — the loader falls back to scanning bucket ids `0..=max_bucket_id` at generation `0` (the legacy un-suffixed objects, e.g. `b_3.cbor`) and keeps the legacy higher-bucket-wins/tombstone reconciliation; the first flush upgrades the durable layout to the manifest format and retires rewritten legacy objects. Earlier binaries cannot read data written by this release.
  - **API**: bucket callbacks now receive a `BucketObject { bucket_id, generation }` and return `Result<(), _>` (the cooperative `Ok(false)` stop is gone); `flush`/`flush_with`/`flush_owned_with` return `FlushOutcome { saved, obsolete }` instead of `bool`; `load_all`/`load_buckets` callbacks take `BucketObject`. The low-level `store_metadata`, `store_metadata_with` and `store_dirty_buckets` building blocks are removed — the coordinated flush is the only persistence path. `BTreeIndex::flush_with` (borrowing variant) is folded into `flush_owned_with`.
  - **Concurrency contract narrowed**: both crates no longer serialize flushes internally or defend a flush against concurrent mutations (the internal mutation gate, persistence gate, dirty-bucket counter and empty-posting flush defenses are deleted). Coordinating mutations vs. flush/compaction, and flushes against each other, is the caller's responsibility — `anda_db`'s `Collection` already holds an exclusive operation gate across every flush; a single writer per durable index is a deployment contract.
- **BM25 flush callback signatures** — `BM25Index::flush` / `flush_with` callbacks are now plain `FnMut`/`FnOnce` closures returning a future and receive owned `Vec<u8>` blobs (previously `AsyncFn*` over `&[u8]`); the `AsyncFn*`-over-borrow pattern made every downstream `tokio::spawn` of a flush fail to prove `Send` (rustc "implementation of `Send` is not general enough").
- **HNSW delete-time reconnection is configurable** — New `HnswConfig::reconnect_on_delete` lets recall-sensitive workloads opt into the O(M²·L) under-lock neighbor repair introduced by the unpublished 0.9.2. It now defaults to `false` (see the migration note above).
- **`anda_kip::loose_equal` is public** — The engine's `==`/`!=`/`IN` equality is exposed for downstream executors.

### 0.9.2 (unpublished, 2026-07-10) — folded into this release

Workspace-wide hardening driven by a full per-crate code review. All 13
crates were audited and fixed; ~60 regression tests were added. This version
was never published; entries contradicted by the follow-up review above are
annotated.

### Added

- **`AndaDB::close_collection`** — Closes a collection and releases its handle from the database registry so the collection can be reopened later; previously a closed collection's handle lingered forever and the name could never be reopened in-process.
- **Multi-dimension HNSW routing** — A collection with several HNSW indexes now routes `Search.vector` to the index whose dimension matches the query vector (new `Hnsw::dimension()` accessor); previously any vector search on such a collection always errored.
- **`BTree::try_range_query_ids` and `BTree::flush_with`** — Error-reporting range-query variant (type-mismatched filter values surface `DBError::Index` instead of a silent empty set) and a persist-callback flush that only advances the saved-version watermark after the external write succeeds.
- **Strict metadata authentication mode** — `EncryptedStoreBuilder::with_strict_metadata_auth()` rejects even genuine legacy (pre-auth) metadata; the default mode is now fail-closed against auth stripping (see Fixed) and logs a warning when it accepts legacy metadata.
- **Server operational controls** — `anda_db_server`: `REQUEST_TIMEOUT_SECS` (408 on expiry), `MAX_BODY_SIZE`, `SHUTDOWN_TIMEOUT_SECS` drain deadline. `anda_cognitive_nexus_server`: background auto-flush task (`FLUSH_INTERVAL_SECS`), `kip_logs` retention pruning (`LOG_RETENTION_DAYS`, default off), `SELF_PRINCIPAL_ID`, request timeout and body-limit knobs. Shard proxy: `X-Forwarded-For/-Host/-Proto` injection.
- **RangeQuery depth cap** — `RangeQuery::MAX_DEPTH` (64) with an iterative depth check; hostile deeply-nested filters return an error (or an empty result on the infallible path) instead of overflowing the stack.
- **Derive-macro trybuild UI tests** — Compile-pass and compile-fail coverage for generated code, including shadowed user type names and bare generic fields.

### Changed

- **Search and filter misuse now error instead of returning empty results** — `Search.text` without a BM25 index, `Search.vector` without a dimension-matching HNSW index, and filter values whose type does not match the B-Tree index key now return `DBError::Index`; previously all three silently returned empty result sets.
- **Hybrid search truncation keeps the most-relevant results** — With `Lt`/`Le` filters, result truncation now keeps the head of the RRF-ranked candidates; the tail-keeping strategy only applies to the pure-filter (id-ascending) path. Previously hybrid queries returned the *least* relevant `limit` documents.
- **KQL default result order is deterministic** — Solutions without `ORDER BY` are returned in ascending `EntityID` order (previously clause-insertion order), which also fixes cursor pagination skipping pages over unordered bindings.
- **KIP comparison semantics unified** — `==`/`!=` now use the same loose numeric/datetime comparison as the ordering operators (`3.0 == 3` is true); previously `3.0 == 3` was false while `3.0 <= 3` was true. *The "arrays/objects never compare equal" part was reverted by the follow-up review (structural equality again), and exact big-integer comparison landed there too.*
- **KIP aggregate integer semantics** — `SUM`/`MIN`/`MAX` over all-integer inputs return integers (i128 accumulation, no precision loss above 2^53); `SUM` of an empty set is integer `0`, `AVG` of an empty set is `null`.
- **KIP duplicate keys are parse errors** — Duplicate keys in concept matchers, `SET ATTRIBUTES`/`WITH METADATA` blocks, and nested JSON objects now fail with `KIP_1001` instead of silently keeping the last value; `SEARCH ... LIMIT 0` is likewise rejected at parse time.
- **`UPDATE` response `matched` counts pre-truncation hits** — `updated < matched` now signals `LIMIT` truncation; dangling `{id:}`/`(id:)` references fail fast with `KIP_3002` instead of matching an empty set.
- **Shard proxy refuses insecure exposure** — Listening on a non-loopback address without `API_KEY` now aborts startup unless `INSECURE_NO_API_KEY` is set; `backend_addr` must be an absolute `http://` URI; backend PostgreSQL failures return 503 instead of being misreported as 404, with a 5s negative cache and db-name pre-validation protecting the connection pool.
- **Server 500 responses are generic** — Internal error details go to logs; query-usage errors surfaced by the engine's new `DBError::Index` behavior map to 400.

### Fixed

- **Collection flush checkpoint vs. concurrent adds** — `flush` clamps the crash-recovery checkpoint below the smallest in-flight `add` (ids allocated but not yet in the bitmap), so a crash between id allocation and bitmap update can no longer permanently hide the document from `auto_repair_indexes`. *Superseded in this release by the allocation watermark plus the exclusive flush gate.*
- **`set_extension` persistence** — `set_extension`/`set_extension_with`/`set_extension_from_with` now bump the metadata version so extension data actually persists on the next flush; the prior test passed spuriously against the in-memory cache and has been rewritten to reconnect from storage.
- **Collection lifecycle races** — `delete_collection` vs. `open_collection` can no longer resurrect a zombie collection; `open_or_create_collection` serializes creation and falls back to open on `AlreadyExists`; `inner_drop_prefix` bumps `write_seq` so deleted objects stop being served from cache.
- **Grouped aggregation respects FILTER/NOT narrowing** — Group members are intersected with the narrowed bindings before aggregation, so `COUNT` no longer includes filtered-out members; grouped `FIND` also honors `ORDER BY ?group_var` (previously a silent no-op).
- **Row-wise UNION and sequential pattern joins** — Multi-variable `FIND` no longer keeps only the last covering relation: UNION branches with the same variable signature merge row-wise per KIP §3.4.7.3 (disjoint branches null-pad), and sequential dual patterns over the same variable pair perform a true row-level equi-join instead of an endpoint approximation that produced phantom solutions.
- **Cross-variable NOT anti-join** — `NOT` blocks referencing multiple outer variables prune per solution tuple instead of over-pruning by column; literal-predicate branches retain the `AnyPropositions` constraint.
- **`SET PROPOSITIONS` self-loop preflight** — Self-loops are rejected during validation, before any concept writes, honoring the documented reject-before-write contract; UPSERT preflight was hardened so all detectable failures reject before mutation (true rollback still requires a WAL and remains out of scope).
- **Cascade paths no longer swallow storage errors** — DELETE/UPDATE cascades propagate real storage failures (only "already gone" is tolerated) and removal counts reflect actual deletions; predicates containing `:` now round-trip through `EntityID`.
- **I64/F32 stored-document read-back** — Untyped CBOR deserialization restores non-negative integers as `U64` and `f32` as `F64`; validation and typed conversion now accept these read-back forms (mirroring the existing `Vector` compatibility branch), so such documents no longer fail to load after a flush.
- **JSON prefix escaping inside `FieldValue::Json`** — Strings embedded in `Json` values are `b64:`/`txt:`-escaped symmetrically with deserialization, so values like `"b64:AQID"` survive a human-readable round trip instead of being mangled into `Bytes`.
- **Deep-nesting stack overflow guards** — `FieldValue` recursive conversions enforce `MAX_CONVERSION_DEPTH` (128); the BM25 logical-query parser handles trailing-`)` floods iteratively with a 64-level nesting budget (an 8K-`)` input previously aborted the process); KIP nesting/input limits are now exported as public constants.
- **HNSW deletion repairs the graph** — Removing a node reconnects its former neighbors through neighbor-selection repair, preventing monotonic recall degradation over delete-heavy workloads (recall@10 after deleting 50% of nodes: 0.81 → 0.90 in the new regression test); tombstones persist with metadata so purge survives reload. *The repair now defaults to off and the per-layer tracker was removed again in this release.*
- **BM25 statistics consistency** — `avg_doc_tokens` updates are serialized under one lock so concurrent insert/remove can no longer leave a permanently skewed average; flush persists buckets before advancing the metadata version watermark (HNSW likewise).
- **B-Tree ghost keys after crash** — Empty postings persisted during a remove/flush crash window are treated as tombstones on load (skipped, marked dirty for self-heal), so reloaded trees no longer report keys with no documents; serialization failures during insert return errors instead of panicking.
- **Encrypted metadata auth stripping** — Metadata carrying an auth version but missing `auth_nonce`/`auth_tag` is rejected by default (previously it silently downgraded to legacy verification, allowing cross-path object moves and chunk-boundary truncation by an attacker with storage write access).
- **Sidecar store self-healing** — Corrupted (torn-write) sidecar metadata no longer permanently bricks a key: overwrite puts rebuild it; orphaned data objects (crash between data and meta writes) no longer fail whole `list` scans; `rename(from == to)` no longer deletes the object; concurrent multipart completes serialize per key.
- **`list_logs` remote panic** — `limit=0` (or an empty page) in `anda_cognitive_nexus_server` no longer panics the handler via `rt.last().unwrap()`; limits are clamped and cursors derived safely.
- **Shard proxy stale routing** — The PostgreSQL LISTEN reconnect path clears `db_cache` (missed NOTIFY events could route tenants to shards that no longer own them), with a TTL as a last line of defense; management mutations write and notify inside one transaction.
- **Server registry durability** — `anda_db_server` performs open/create I/O outside the registry lock (a slow S3 open no longer stalls all RPCs), keeps databases that failed to reopen in the registry for retry after restart, and refuses to start on a corrupted registry instead of silently overwriting it.
- **Derive-macro diagnostics and hygiene** — Bare generic fields produce a targeted compile error at the field span (instead of a misleading E0599); generated code uses fully-qualified paths so user types named `FieldType`/`Schema` no longer collide; `#[cbor(key)]` on `AndaDBSchema` top-level fields and container-level `#[unique]` are compile errors.

## [0.9.1] — 2026-07-05

### Changed

- **AES-GCM dependency migrated 0.10 → 0.11** — `anda_object_store` moved to the `aes-gcm` 0.11 `AeadInOut` API; no wire-format change.
- **Workspace dependency alignment** — Internal crate dependency requirements aligned to the `0.9.x` line.

## [0.9.0] — 2026-07-05

### Added

- **KIP v1.0-RC10 specification** — Per-command result shapes (columnar `FIND` result model, solution-set deduplication), `CURSOR` pagination for `EXPORT`, structural `(s, "p", o)` references for higher-order endpoints, `SEARCH PROPOSITION ... WITH TYPE` predicate semantics, `MERGE` source provenance chaining and "already merged" replay self-diagnosis, zero-hop path semantics, bare-variable `ORDER BY` keys, `System`/`Unsorted`/`Archived` operational domains in Genesis bootstrap.
- **EXPORT pagination** — `EXPORT` now supports `CURSOR` with deterministic, idempotent pages; each page is an independently valid capsule.
- **FILTER predicate-variable support** — FILTER expressions over predicate variables (`(?s, ?p, ?o)` patterns) evaluate per-binding without per-iteration cloning.
- **`handle_full_scan_matching`** — Supports the unconstrained `(?s, ?p, ?o)` proposition pattern for memory-metabolism operations (e.g., confidence-decay UPDATE).
- **`compare_order_key`** — Numeric, boolean, datetime-aware string, and null-last ORDER BY semantics matching KIP §3.5.
- **MERGE `diagnose_already_merged`** — Replay of an already-applied MERGE self-diagnoses when the source no longer exists but the target's `_merged_from` already records it.
- **Genesis bootstrap: operational domains** — `Unsorted`, `Archived`, and `System` domains created by Genesis.kip; `Commitment` added to key concept types; `committed_to`/`owed_to` added to key predicates.

### Changed

- **`QueryRelationRow` fields are `Option`** — Enables OPTIONAL left-join padded rows: unbound positions project `null` per KIP §3.4.7.2.
- **`ConceptPK`/`PropositionPK` Display emits valid KIP syntax** — `{id: "C:7"}` instead of `{id: Concept(7)}`, so error messages are directly reusable by self-correcting agents.
- **`$system` type corrected** — From `System` to `Person`, consistent with `$self`.
- **PRIMER degrades gracefully** — A nexus without `$self` still returns a complete domain map; `search_modes` advertises keyword-only capability out-of-band.
- **Multi-hop traversal capped** — Explicit hop bounds beyond 10 return `KIP_4002`; `{m,}` unbounded quantifier is soft-capped.
- **`instance_schema` meta-keys expanded** — Added `item_type` (for arrays), `enum`, and `default_value`; `Person.id` made optional.
- **SleepTask domain** — Moved from CapsuleCreate to Genesis bootstrap; `SleepTask` instances belong to the `System` domain.
- **Lightweight NOT/OPTIONAL sub-context** — `QueryContext::scoped_child` clones only variable bindings and shares the entity cache; `NOT` / `OPTIONAL` no longer clone the (potentially large) relation row sets, groups, or regex cache on every clause.

### Fixed

- **JSON `FieldValue`/`FieldKey` prefix disambiguation (breaking wire change)** — Human-readable serialization now uses explicit prefixes: `Bytes` is `"b64:<url-safe base64>"`, `I64` keys stay `"i64:<n>"`, and text that itself starts with a reserved prefix is escaped as `"txt:<original>"`. The old heuristic promoted *any* Base64-decodable string to `Bytes`, silently corrupting ordinary text like `"test"` on the JSON path (e.g. `anda_db_server` JSON requests). Malformed payloads after a prefix are now hard errors. CBOR encoding is unchanged.
- **Row-based FIND pagination over proposition-less rows** — The relation-row FIND path now paginates with a numeric offset cursor (same convention as the cartesian path). The previous cursor was anchored to the row's proposition id, which multi-hop paths, OPTIONAL-padded rows, and synthetic FILTER relations do not have — a page boundary landing on such a row silently truncated the result with no `next_cursor`.
- **Strict offset-cursor parsing** — The relation-row / cartesian FIND paths and predicate-variable pagination reject a cursor token that is not a plain decimal offset (`KIP_1001`) instead of silently restarting from page one and handing the client duplicate data.
- **Unconstrained `(?s, ?p, ?o)` scan capped** — The full-scan pattern rejects graphs with more propositions than the solution-materialization cap (`KIP_4002`) instead of materializing an unbounded row set; `LIMIT` only bounds projection, not the scan itself.
- **`LIMIT 0` rejected at parse time** — The engine's internal "no limit" sentinel is `0`, so `LIMIT 0` used to silently mean *unlimited* in `FIND`/`EXPORT`; the parser now requires a positive integer (omit `LIMIT` for unlimited).
- **`apply_order_by` JSON pointer construction** — Uses `DotPathVar::to_pointer()` so path components containing `/` or `~` are escaped and a bare-variable key maps to the whole value (`""`) instead of the `""` object key (`"/"`).
- **`reconcile_storage` concurrent-add guard** — The dead-id sweep only considers ids at or below the `max_document_id` snapshot taken before the storage listing, so a document added while the listing runs can no longer be mistaken for a dead id and dropped from the bitmap.
- **Response deserialization** — Custom `Deserialize` dispatches on `error` key presence so a response with both `error` and partial `result` always deserializes as `Err`, not silently as `Ok`.
- **Cross-variable FILTER join semantics** — `FILTER` comparing two different variables (e.g. `FILTER(?a.risk > ?b.risk)`) now evaluates per solution (join) instead of positionally zipping each variable's bindings, which silently returned wrong or empty results. Predicate-variable filters (`FILTER(?p != "…")`) narrow the covering proposition (`?link`) itself, so the memory-metabolism `UPDATE` idiom operates on the correct target set.
- **Multi-variable FIND column alignment** — When no single relation connects all projected variables, the solution set is materialized as their (capped) cartesian product so the columnar `FIND` result stays index-aligned across solutions per KIP §6.2.2; previously the columns could have mismatched lengths and could not be zipped back into rows.
- **Constant FILTER no longer loops** — A variable-free `FILTER` (e.g. `FILTER("a" == "b")`) is evaluated once and clears the solution set on `false`; the previous consume-based evaluator could loop indefinitely on it.
- **Disconnected-solution guard** — Cross-variable `FILTER` and cartesian `FIND` over disconnected variables are capped (`KIP_4002`) rather than materializing an unbounded product.
- **NOT variable scoping** — Only narrows outer-bound variables the NOT block actually references; unrelated variables are preserved intact.
- **OPTIONAL left-join padding** — Outer-bound entities without a match now receive padded rows (projecting `null` for unbound positions) instead of being silently dropped.
- **MERGE provenance chaining** — Source's own `_merged_from` entries are carried forward (duplicates dropped) so provenance survives chains of merges.
- **DELETE PREDICATE cascade** — Higher-order propositions referencing a deleted link are transitively removed; no dangling references.
- **Self-loop proposition error clarity** — Error message now explains the engine storage-model limitation and suggests reification.
- **PRIMER domain type performance** — Populates key schema types in O(#type definitions) instead of O(#members × #domains).
- **Striped document-level concurrency locks** — Added 128-stripe async lock set serializing `update`/`remove` per document id so concurrent mutations of the same document cannot race between index mutations and the versioned storage write, eliminating phantom index entries.
- **`Collection::reconcile_storage` maintenance API** — Full data-directory scan that recovers orphaned documents into the id bitmap and drops dead ids whose objects no longer exist; complements the bounded crash-recovery scan by covering gaps beyond large id discontinuities.
- **HNSW `removed_nodes` tombstone tracking and `purge_removed_nodes`** — Records removed node ids so the persistence layer can delete the corresponding on-disk node blobs during flush cleanup; without this, removed node files accumulated forever.
- **BM25 `store_metadata_with` atomic persistence** — Persist callback variant that only advances the saved-version watermark after the external write succeeds, preventing stale metadata on a failed object-store write.
- **`FieldValue::bytes_from` array coercion** — Accepts CBOR integer arrays in addition to byte strings so `Vec<u8>` and `[u8; N]` struct fields (whose serde serializers emit integer sequences) can populate `FieldType::Bytes` fields.
- **`json_to_cbor` helper** — Replaces the panicking `Cbor::serialized(&obj).expect(...)` path for `FieldValue::Json` with a total conversion function.
- **Derive macro auto-imports schema types** — Added `schema_crate_path()` resolution so `#[derive(AndaDBSchema)]` and `#[derive(FieldTyped)]` import `Schema`, `FieldType`, `FieldKey`, etc. through the correct crate path; callers no longer need explicit `use` statements.
- **CJK tokenizer detection expanded** — `Script::Cjk` now also matches Japanese kana and Hangul syllables alongside Han ideographs, so mixed Japanese/Korean text routes correctly to the CJK tokenizer pipeline.
- **HNSW `ef_search` capped** — `search_layer` caps the user-supplied top-k against `MAX_EF_SEARCH` so a large `Query::limit` cannot force an arbitrarily expensive beam search.
- **BTree bucket size drift** — Unified `posting_entry_size` to account for the field-value key's serialized size in every bucket-size estimate (create, migrate, remove-last, compaction); long string keys previously caused buckets to overshoot `bucket_overload_size`.
- **`FieldValue::deserialized` ownership** — Takes `&self` instead of consuming `self`, allowing reuse of the same value for multiple deserialization targets.
- **Re-insert safety** — HNSW `add` / `insert` clears any pending tombstone for the id so a re-inserted document does not have its new node blob deleted by a stale `removed_nodes` entry.

## [0.8.4] — 2026-06-26

### Added

- **Resource-exhaustion guards** — Added structural complexity validation for runtime field values, query filters, BM25 logical queries, searchable text extraction, KIP parsing input, and HNSW public configuration limits.
- **Authenticated encrypted object metadata** — Added AES-GCM authentication for encrypted object sidecar metadata and chunk associated data so swapped or tampered encrypted payload metadata is rejected, including copy/rename metadata rebinding.

### Changed

- **Workspace crates prepared for 0.8.4** — Bumped `anda_db`, `anda_db_schema`, `anda_db_hnsw`, `anda_db_tfs`, `anda_kip`, `anda_object_store`, and `anda_cognitive_nexus` to `0.8.4` for the new hardening release.
- **Object store dependency updated** — Updated `object_store` from `0.13` to `0.14` and filled default extension metadata in wrapper object-store responses.
- **Server and shard proxy request validation tightened** — Rejected empty API keys, required explicit `Authorization: Bearer <key>` headers, and ignored client-supplied shard headers in favor of server-side/path routing metadata.

### Fixed

- **Read-only extension mutation checks** — Enforced read-only mode for database and collection extension save/remove operations.

## [0.8.3] — 2026-06-19

### Added

- **Integer schema map keys** — Added `FieldKey::I64`, integer wildcard map support via `I64_WILDCARD_KEY`, and signed-integer `Map<I64, T>` / `BTreeMap<i64, T>` schema inference.
- **CBOR integer-keyed nested schemas** — Added `#[cbor(key = N)]` support to `FieldTyped` so nested CBOR-native structs can model integer map labels while keeping schema validation aligned with `cbor2` serialization.

### Changed

- **Workspace crates prepared for 0.8.3** — Bumped `anda_db`, `anda_db_schema`, `anda_db_derive`, and `anda_cognitive_nexus` to `0.8.3`, refreshed schema/derive documentation, and added `cbor2` derive test coverage for integer-keyed CBOR maps.
- **Developer fix target formats first** — Updated `make fix` to run `cargo fmt --all` before applying clippy fixes.

## [0.8.2] — 2026-06-14

### Changed

- **Workspace crates aligned for the 0.8.2 release** — Bumped the published Rust crates to `0.8.2` and kept internal workspace dependency requirements on the matching `0.8` line.
- **Repository metadata normalized** — Updated Cargo package repository and homepage URLs from the old `anda_db` path to the canonical `anda-db` GitHub repository path.
- **Canonical CBOR encoding consolidated on `cbor2`** — Switched index key and virtual-field encoding paths to call `cbor2::to_canonical_vec` directly.
- **Index runtime stats refreshed consistently** — Reused live counter overlays for BM25 and HNSW metadata/stat snapshots so callers observe current element, search, bucket, document, and token statistics.
- **BM25 query execution streamlined** — Reused per-token dedup buffers and merged conjunctive scores during intersection to reduce avoidable allocations and passes.

### Fixed

- **B-Tree posting size accounting** — Reworked existing-posting append and bucket-migration accounting to avoid repeated full-posting measurement while preserving exact source-bucket size updates at CBOR size boundaries.
- **Storage streaming writer sendability** — Made `Storage::stream_writer` return a `Send` async writer so callers can hold it across await points and spawned tasks.
- **Zstd streaming decompression edge cases** — Prevented oversized preallocation for small `max_size` limits and returned an error for truncated frames instead of spinning without progress.
- **HNSW entry-point repair** — Repaired dangling persisted entry points even when the entry id is `0`, treating node id 0 as valid rather than as an unset sentinel.

## [0.8.1] — 2026-06-13

### Added

- **Python binding close API** — Added an idempotent async `PyAndaDB.close()` method so Python clients can explicitly flush and close file-backed Cognitive Nexus stores.
- **CBOR-first Anda DB server RPC API** — Added the `anda_db_server` 0.2.0 RPC surface with root/database-scoped dotted methods, JSON fallback, content negotiation, `GET /` health info, structured HTTP error envelopes, and explicit database lifecycle methods.
- **Server database registry and lifecycle management** — Added multi-database registration, restart-time auto-reopen from primary database extension metadata, per-database background flush tasks, and graceful close/shutdown handling.
- **Quality-assurance test infrastructure** — Added crash-consistency fault injection, on-disk format compatibility fixtures, B-Tree/BM25 property tests, HNSW recall floors, and KIP parser fuzz/proptest coverage.
- **Regression coverage for server RPC and Python binding behavior** — Added HTTP integration and Python tests for server CBOR/JSON/auth/lifecycle behavior, parameter conversion failures, nested parameters, and close idempotency.

### Changed

- **Python binding moved to the 0.3 line** — Bumped `anda_cognitive_nexus_py` to `0.3.0`, updated it to depend on the `0.8` Rust crates, and switched the Python package metadata to derive its version from the binding crate manifest.
- **Python wheel build profile clarified** — Added a `release-py` profile for PyO3 extension wheels and pointed maturin at the binding crate manifest.
- **Anda DB server API modernized** — Replaced the legacy method-name payload handlers with focused `api`, `encoding`, `error`, and `state` modules; updated the README around the new CBOR-first protocol and `local --path` CLI usage.
- **Testing workflow documented and instrumented** — Added testing standards documentation, Makefile coverage targets, and an informational CI coverage job that uploads LCOV artifacts without gating releases.
- **Workspace crate versions aligned for the 0.8 line** — Bumped the supporting database, schema, index, object-store, server, and Cognitive Nexus crates to matching `0.8.x` dependency requirements for the 0.8.1 release train.
- **CBOR stack migrated to `cbor2`** — Replaced direct `ciborium` usage with `cbor2`, updated CBOR encoding/decoding and serialized-size accounting across storage, B-Tree, BM25, HNSW, schema, server, and sidecar code, and updated `ic_auth_types` to the 0.9 line.
- **Developer guidance refreshed for the cbor2-era APIs** — Updated repository agent instructions, docs, README snippets, and the AndaDB skill reference to avoid outdated `ciborium` and removed `cbor_size` examples.

### Fixed

- **Safer Python parameter handling** — Replaced panic-prone JSON string round-tripping with direct JSON-compatible Python value conversion and clear `ValueError` failures for unsupported values, non-finite floats, non-string keys, and excessive nesting.
- **Lossless server parameter decoding** — Kept CBOR and JSON RPC params in their original wire format until typed handler decoding, avoiding lossy cross-format conversion for CBOR-only values such as byte strings.
- **Negotiated server error responses** — Returned authentication, parsing, validation, not-found, conflict, precondition, payload-too-large, and internal failures as structured RPC error envelopes in the negotiated response encoding.
- **Python extension import/build robustness** — Made logger initialization non-fatal when a host process already installed a logger, added PyO3 macOS extension link arguments, and documented the correct module import path in the Python README.

## [0.8.0] — 2026-06-11

### Added

- **KIP mutation primitives** — Added `EXPECT VERSION` optimistic concurrency guards for `UPSERT`, pattern-matched `UPDATE` statements with numeric update expressions, and `MERGE CONCEPT` support for atomic entity consolidation.
- **KIP recall and portability commands** — Extended `SEARCH` with retrieval modes (`keyword`, `semantic`, `hybrid`) and score thresholds, and added `EXPORT` for serializing matched knowledge into idempotent UPSERT capsules.
- **Commitment capsule** — Added the `Commitment.kip` capsule and updated built-in capsule metadata so agents can model durable commitments alongside events and people.
- **Cognitive Nexus KIP execution coverage** — Implemented KML/KQL/META support for the expanded KIP surface, including update execution, merge handling, search scoring, export generation, and version-conflict reporting.
- **Regression coverage for KIP and Cognitive Nexus behavior** — Added parser and executor tests for optimistic concurrency, update expressions, merge semantics, search modes, export capsules, and the split database implementation.

### Changed

- **Workspace crates moved to the 0.8 line** — Bumped `anda_db`, `anda_kip`, and `anda_cognitive_nexus` to `0.8.0`, and updated dependent workspace crates to require the matching `0.8` APIs.
- **Cognitive Nexus database implementation split by responsibility** — Replaced the monolithic `db.rs` with focused modules for KML execution, KQL execution, proposition matching, META commands, shared database setup, and tests.
- **KIP specification and tool schemas refreshed** — Updated the specification, syntax guide, self/system instructions, and function definition JSON files to describe the RC KIP semantics and the new read/write command set.
- **System metadata semantics clarified** — Documented reserved engine-maintained `_` metadata fields, versioning behavior, and protected-scope constraints for write operations.

### Fixed

- **Safer endpoint matching syntax** — Tightened embedded endpoint clause handling so nested concept/proposition endpoints remain unnamed, with explicit guidance for binding endpoints through separate clauses.
- **More robust query and mutation behavior** — Hardened Cognitive Nexus helper/type paths around KIP execution, protected scopes, cache invalidation, and proposition matching while preserving concurrent read and exclusive write semantics.
