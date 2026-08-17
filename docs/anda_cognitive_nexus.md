# `anda_cognitive_nexus` — Technical Reference

> The reference **KIP 2.0** Cognitive Nexus — an embedded memory brain for AI
> agents, built on Anda DB.

|                       |                                                                                                                                                      |
| :-------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------- |
| Crate                 | [`anda_cognitive_nexus`](../rs/anda_cognitive_nexus/)                                                                                                |
| Version               | `0.11.x`                                                                                                                                             |
| Implements            | KIP **2.0** [`Executor`](../rs/anda_kip/src/executor.rs) ([SPECIFICATION.md](../rs/anda_kip/SPECIFICATION.md))                                        |
| Storage backend       | [Anda DB](../rs/anda_db/) — embedded document store with B-Tree + BM25 + HNSW indexes                                                                 |
| Other implementations | [`anda_cognitive_nexus_server`](../rs/anda_cognitive_nexus_server/) (HTTP/JSON-RPC), [`anda_cognitive_nexus_py`](../py/anda_cognitive_nexus_py/) (Py) |

> The KIP 1.x engine that used to live in this crate was **deleted, not
> ported**. KIP 2.0 is a different data model, and a renamed 1.x engine would
> have been a worse lie than an absent one. Nothing in this document describes
> `DELETE PROPOSITIONS`, `_version` metadata or Domains; if you are looking for
> those, you are looking at the wrong major version.

---

## Contents

1. [The idea everything hangs on](#1-the-idea-everything-hangs-on)
2. [Crate layout](#2-crate-layout)
3. [Storage](#3-storage)
4. [Schema Packages](#4-schema-packages)
5. [Transactions](#5-transactions)
6. [Executing KQL](#6-executing-kql)
7. [Executing KML](#7-executing-kml)
8. [The Epistemic Projection](#8-the-epistemic-projection)
9. [Executing META](#9-executing-meta)
10. [Governance](#10-governance)
11. [Capsules](#11-capsules)
12. [History](#12-history)
13. [Host APIs](#13-host-apis)
14. [What this engine does not do](#14-what-this-engine-does-not-do)
15. [Testing](#15-testing)

---

## 1. The idea everything hangs on

```text
a Proposition existing  ≠  the Proposition being true
```

A **Proposition** is a truth-neutral tuple. An **Assertion** is one actor's
commitment about it, carrying a stance, a mode, a confidence, its Evidence and
its valid time. What is *currently believed* is **projected** from Assertions
under a named policy and is never stored.

That is why [`PropositionRow`](../rs/anda_cognitive_nexus/src/store/rows.rs) has
no `confidence` column and `AssertionRow` does, and why correcting a claim
records a *new* Assertion with a supersession link rather than rewriting the old
one.

Distinctions the code deliberately keeps apart, each of which a well-meaning
simplification would collapse:

```text
missing              ≠ false
confidence           ≠ trust ≠ memory strength
retention.expires_at ≠ valid_time.until
Space                ≠ Domain
Principal            ≠ semantic actor
search score         ≠ confidence
batch                ≠ transaction
VALIDATE ≠ PREVIEW   ≠ Receipt
FOR TIME             ≠ AS OF        (what was true then ≠ what this Brain held then)
epistemic trust      ≠ influence authority ≠ tool permission
```

---

## 2. Crate layout

```text
src/
├── id.rs          ElementId: `C-1` `P-1` `A-1` `E-1` `X-1` — the kind is in the id
├── term.rs        references, Core Literals, tuple_key
├── time.rs        one normalized UTC form; lexicographic order == chronological
├── view.rs        the raw Core view (§53.1) — what a KQL dot path reads
├── profiles.rs    the bundled cognitive-memory profile, vendored from the spec
├── store/         ten anda_db collections: rows, the write path, Spaces,
│   └── history.rs   the journal, and the element version log AS OF reads
├── schema/        symbol identity, package artifacts, per-Space environment
├── governance/    the protected control plane — see §10
├── tx.rs          transactions: staging, handles, one version per transaction
├── kml/           mutation clauses, value evaluation, target selection
├── kql/           solutions, pattern matching, traversal, filters, projection
├── projection/    the Epistemic Projection and its policy
├── meta/          DESCRIBE / LIST / SEARCH / VALIDATE / PREVIEW / HISTORY / …
├── capsule/       export, verify, and the import semantic merge
└── nexus.rs       CognitiveNexus and Session: the Executor impls
```

---

## 3. Storage

Ten collections for cognitive state, plus eight for Governance (§10). One per
Core element kind, because they have genuinely different columns and genuinely
different hot paths: a projection starts by fetching every Assertion about one
Proposition, while a grounding `SEARCH` looks only at Concept names.

| Collection                        | Holds                                        |
| :-------------------------------- | :------------------------------------------- |
| `concepts` `propositions` `assertions` `evidence` `activities` | the Core elements   |
| `spaces`                          | the MemorySpace registry and its sequence     |
| `transactions`                    | the commit journal                            |
| `schema_packages` `schema_envs`   | installed artifacts, and per-Space activation |
| `element_versions`                | one row per element version — what `AS OF` reads |

Four decisions worth stating once.

**Every index is single-field.** An `anda_db` composite B-Tree index is built on
a virtual field created `with_unique()`, so a composite index is *also* a
uniqueness constraint. Declaring one over `(space, state)` would assert that a
Space contains at most one active element. Only `tuple_key`, `space_id` and
`tx_id` are genuinely unique; everything else is indexed per column and
intersected with `Filter::And`.

**Absence is the empty string, not `Option`.** An `Option<T>` column is a
`FieldType::Option`, which a B-Tree index cannot range over as one ordered
domain. No legal value of these columns is ever empty, so `""` is an
unambiguous "unset" that still sorts.

**Every reference gets a key column beside its JSON.** The JSON is the record;
the key is `Endpoint::key`, a deterministic string that makes reference equality
an index lookup rather than a scan-and-compare.

**Timestamps are one normalized UTC form** — `YYYY-MM-DDTHH:MM:SS.sssZ` — so
lexicographic order *is* chronological order and a temporal range query is a
B-Tree range over text.

---

## 4. Schema Packages

In KIP 1.x, authoritative schema was graph state, so an ordinary write could
change what a type meant. In 2.0 it is an immutable versioned artifact resolved
through a per-Space **Schema Environment**, and every persisted `schema_ref`
names an exact version — which is why an element's meaning cannot drift when
somebody publishes something.

```text
install_package   the artifact exists here
activate_schema   these packages are in force in this Space
```

Installing is not activating. The same `package_id@version` arriving with
different content is a `DigestMismatch`, not an update. Environment versions are
appended and never rewritten, so a transaction that recorded which version it
ran under keeps meaning what it meant.

A `functional` predicate does **not** reject a competing write. Two rival
objects are a *disagreement*, and refusing to store one would make disagreement
unrecordable; the conflict is expanded by the projection instead (§8).

---

## 5. Transactions

One KML statement is one transaction, whether or not the source wrote
`MUTATE { … }`.

```text
Phase 1   declare every handle
Phase 2   interpret every clause, with all handles bound
Commit    write once, version once, journal once
```

Splitting the phases is what makes forward references legal, and forward
references are what make atomic provenance formation possible: an Evidence
record generated by an Activity that lists it as an output is a legitimate
cycle.

**`anda_db` cannot reserve an element id** — `add_impl` calls `fetch_add`
itself — so a handle's element is inserted as a `state: "pending"` shell and
filled in at commit. Nothing reads a pending element, which makes
`sweep_pending()` on open recovery *by construction* rather than by replay. It
is also why a selection block cannot see its own transaction's writes: clause
order carries no mutation semantics, so a sweep that could see them would mean
different things depending on where its author put it.

**Versions are assigned at commit**, not per write: one element, one increment
per transaction, however many clauses touched it. A clause that computes the
state an element is already in changes nothing and produces a `no_effect`
receipt rather than claiming a transition that did not happen.

**Atomic visibility comes from the `CognitiveNexus` `RwLock`**, not from
`anda_db`. In-process only — and `anda_db` allows one live writer per database,
so the guarantee is not weaker than the storage underneath it.

---

## 6. Executing KQL

A query is a `WHERE` block joined into one set of solutions, then projected.

Supported: element, tuple and structural patterns, hop-quantified path
traversal, `FILTER` (all 11 functions), `NOT` / `OPTIONAL` / `UNION`, dot-path
projection, aggregates, `ORDER BY`, paging, and both time axes — `FOR TIME`
(what was applicable then) and `AS OF SEQ | TX | TIME` (what this Brain held
then).

Two rules a caller will otherwise get wrong:

- a KQL result is a **bare array**: one projected variable ⇒ each row is a
  scalar; several ⇒ each row is an **array**, never an object;
- patterns match `active` elements unless the pattern says otherwise. That is
  what archiving *means*.

Every candidate a pattern loads is charged against one budget
(`MAX_CANDIDATES`), and exhausting it is an explicit `ResourceExhausted` rather
than an engine that stops responding.

**`Context::load` is the read path's authorization choke point.** Every pattern,
filter, projection, aggregate, search hit and capsule root reaches an element
through it, so an element the caller may not read is outside the query universe
for the whole query — see §10.

### A known gap

`STRUCTURAL (?src, "field", ?dst)` walks **Profile** structural fields only. The
Core reference fields — an Assertion's `evidence`/`context`, an Evidence
record's `source`/`generated_by`, an Activity's `inputs`/`outputs` — are
unreachable, so *which Assertions cite this Evidence* cannot be asked even
though `evidence_ids` is indexed.

---

## 7. Executing KML

`CREATE CONCEPT` / `UPSERT CONCEPT` / `ENSURE PROPOSITION` /
`CREATE EVIDENCE|ASSERTION|ACTIVITY` / `ASSERT` (desugared) / `UPDATE` /
`RETRACT ASSERTION` / `SUPERSEDE ASSERTION` / `CORRECT EVIDENCE` /
`TRANSITION ACTIVITY` / `SET RETENTION` / `ARCHIVE` / `TOMBSTONE` / `PURGE` /
`MERGE CONCEPT`, each with an optional `WHERE` selection block and `LIMIT`, plus
handles, `EXPECT VERSION` / `EXPECT STATE`, idempotency keys, receipts and dry
runs.

Planning runs in three passes (`clauses::plan_pass`): `CREATE CONCEPT`, then
`UPSERT`/`ENSURE`, then everything else. `ENSURE` needs to see a Concept the
same transaction created before it can check a predicate's subject type, and the
`CREATE ASSERTION` that `ASSERT` desugars to needs the handle `ENSURE` bound.

**`LIMIT` cuts in ascending element id.** §52.7 permits a runtime to document an
order, and documenting one is what makes a bounded sweep repeatable.

`UPDATE` reaches mutable, non-protected state only. Its reachable surface is
decided by *the element the engine loaded*, not by how the command looked:
an Assertion answers `EpistemicRevisionRequired`, an Evidence record
`EvidenceCorrectionRequired`, a terminal Activity `ActivityTerminal`.

`MERGE CONCEPT` is non-destructive: the source keeps all its state plus
`merged_into` and `state: "merged"`, and future writes canonicalize to the
survivor.

---

## 8. The Epistemic Projection

`BELIEF` and `BELIEF SLOT` project rather than read. Three rules carry most of
the weight:

- **silence is `insufficient`, never `rejected`** — an open world does not
  answer "no" to a question nobody addressed;
- **repetition is not corroboration** — Assertions are grouped by corroboration
  group and each group contributes once;
- **shared Evidence merges groups** — two people relaying one observation are
  one observation, and a third Assertion citing both collapses what looked like
  two independent groups into one. That is exactly the shape of manufactured
  corroboration.

A functional predicate triggers conflict-set expansion: nobody said "not
healthy", but somebody said "degraded", and the schema says there can be only
one.

The policy is named and versioned and travels with the answer. Changing a
threshold changes the reported policy id — otherwise the audit trail would be
fiction.

What is *missing* is stated in every projection's warnings: no trust model, no
evidence-quality evaluation, so every eligible corroboration group counts
equally. Do not remove that warning without implementing the stages.

---

## 9. Executing META

The five-layer discipline, which the module layout follows because collapsing
any two of these is how a caller ends up believing something the engine never
said:

```text
DESCRIBE / SEARCH   find        — what is here
VERIFY              integrity   — is this artifact what it claims to be
VALIDATE            legality    — would this be accepted
PREVIEW             effect      — what would it do
Receipt             fact        — what actually committed
```

`DESCRIBE CAPABILITIES` reports what this engine supports **and what it does
not**, as structured data with a reason for each gap. An Agent that has to
discover a gap by triggering an error has wasted a turn; one that never
discovers it will read an absent feature as an absent fact.

`PREVIEW KML` runs the real dry-run path rather than a separate simulation, so
the preview cannot drift from what a commit would do.

---

## 10. Governance

```text
Cognitive content may describe authority.
Only the Governance Control Plane can grant it.
```

A Space can hold a Proposition saying *Alice is an administrator*, an Assertion
supporting it with high confidence, and Evidence for both — and Alice
administers nothing. Without that separation, any path that can write memory is
a path to privilege escalation, and an agent memory system has such a path by
construction.

### The plane

Eight collections beside the cognitive ones, in the same database and behind the
same flush, and **reachable from no KML clause**: `gov_principals`,
`gov_principal_groups`, `gov_actor_bindings`, `gov_grants`, `gov_delegations`,
`gov_policies`, `gov_approvals`, `gov_audit`.

Grant and Delegation are separate row types because they are evaluated
differently. A Grant stands on its own; a Delegation is only ever as good as its
delegator's authority *right now*, so revoking the parent disables the child
even though the child's record still says `active`.

Policy versions append and never overwrite, so an audit can still answer *which
version authorized this* after the policy has moved on. Revocation is a status
change, never a delete, for the same reason.

### Who is asking

A caller reaches the engine through `CognitiveNexus::session(auth)`. The
`AuthContext` is built by the **host** from authenticated transport state, never
deserialized from the request body — the envelope's own `context` block is
documented as non-authoritative because an Agent under prompt injection can
write anything into it. The one place the two meet is purpose, and
asymmetrically: a declared purpose fills a gap at `declared` assurance and can
never replace what the host bound.

Authority is re-resolved on every request, which is what makes revocation take
effect for a session that started before it.

An embedded host that executes against the `CognitiveNexus` directly runs as the
system Principal, which owns the default Space. That is a real authorization
through the same path, not a bypass.

### The decision

```text
protocol invariant
    ↓
matching explicit deny
    ↓
matching allow: owner, Grant, Delegation, or Policy statement
    ↓
default deny
```

Several authorities may permit the same operation; each is independently
sufficient, so the **least restrictive** matching allow is chosen and its
constraints are the decision's. Obligations go the other way and accumulate.

Two scopes, asking different questions: the command gate asks *may this
Principal do this here at all*, and the element check asks *may it do it to
that*. A Grant narrowed to a classification still lets a query run; the
narrowing applies element by element.

### What that buys, per element

- an element the caller may not read is outside the query universe — not
  matched, not counted, not ranked, not paged over;
- a field mask is applied to the cached view, so a masked field cannot be probed
  through which rows come back;
- `_system.origin` needs `read_raw_origin`, and is *withheld* rather than
  removed — dropping it would claim no origin was recorded;
- a Space-wide count is refused with a reason for a Principal whose authority is
  narrower than the Space, and a transaction's change list is filtered for the
  same reason;
- every mutation target is authorized individually, and a sweep that reaches one
  it may not touch fails rather than doing less.

### Attribution and retraction

Which epistemic permission a new Assertion needs is decided by the writer's
ActorBinding, not by the command: bound as the actor needs `assert`, bound as
representing it needs `assert_as_actor`, no binding needs
`record_attributed_assertion`. Recording someone else's claim is not
impersonation and must stay ordinary.

`RETRACT` and `SUPERSEDE` record that the *source* withdrew or replaced its
claim. Two ways to hold that standing — you wrote it, or a binding says you
represent its actor — and `ARCHIVE`/`TOMBSTONE` for everyone else, which say
what they mean.

### Classification, authority, quarantine

An element's `governance` block is refused by `anda_kip`'s parser in every
assignment, on the text path and the pre-parsed AST path alike. It changes
through `Session::classify` / `elevate_authority` / `quarantine`, where the
*revealing* or *empowering* direction is the privileged one: raising a
classification needs `update` and lowering it needs `declassify`; lowering an
authority ceiling needs no approval and raising one may.

Classification joins upward along derivation links at commit — an Assertion's
cited Evidence, an Evidence record's sources, an Activity's inputs, and its
outputs from its inputs. Authority never amplifies along the same links: the
lineage recorded at commit is what an elevation is checked against.

Quarantine is a state ordinary recall excludes and a reviewer can still read,
carrying why. It is neither archival nor retraction: it says this Brain does not
currently allow ordinary use.

### Erasure

`PURGE` is guarded four ways: the `purge` permission, a legal hold that stops it
outright, a `REFERENCE POLICY` defaulting to `deny_if_referenced`, and an
erasure that leaves an identity stub carrying a content digest.

Every recorded version is destroyed with the content — a purge that left the
version log behind would leave the element fully readable through `AS OF`.
History is destroyed first: a crash between the two steps is recoverable by
purging again, where the other order leaves a readable stub and nothing saying
to look.

### Audit

Every control-plane mutation is mirrored into the audit with the complete new
record, and authorization decisions are recorded beside them. `read_audit` and
`read_governance_history` are separate permissions from `read`: one is what
people *did*, the other is what the control plane *was*.

`EffectiveAuthority::resolve_at` answers *who had access at time T*, which is
never a claim about today. High-impact receipts carry the identity and policy
version that authorized them.

---

## 11. Capsules

`EXPORT CAPSULE` selects roots with the KQL solver, walks the reference closure,
and ships exact schema refs and package digests. `VERIFY CAPSULE` recomputes the
digest and reports `signed` **separately from** `valid`, because nothing here is
signed and calling an unverified artifact valid would cancel the point of
asking.

Import is a **host operation**, not a command: KML has no import clause and META
is read-only, so no prompt decides that a Space accepts another Brain's
cognition. Modes: `preview`, `merge`, `isolate` (lands in quarantine).

Identity resolution goes prior import → `canonical_id` → Proposition tuple. The
idempotency mapping lives on the elements themselves, as the `client_key`
`kip:import:{digest}:{source id}` — a side table can survive a crash the
elements did not, and then a re-import resolves to things that are not there.

The source's Space-local `key` is **not** imported: two Spaces' `person:alice`
may be two people.

---

## 12. History

Every commit appends the complete row it wrote to `element_versions`, in the
same commit as the row. A history written afterwards can be missing exactly the
write a crash interrupted, and a history with a hole answers `AS OF` wrongly
instead of refusing. Whole rows, not diffs.

A historical read **cannot use the indexes** — they describe the present — so it
reconstructs candidates from the version log and re-checks every constraint,
charged to the same query budget. Three things must hold: symbols resolve
through the Schema Environment of *that* coordinate, the projection sees only
the Assertions that existed then, and one read has exactly one coordinate.

---

## 13. Host APIs

Deliberately outside the command surface, because none of them is a decision a
prompt should make:

```rust
nexus.install_package(&package, "source")      // installing is not activating
nexus.activate_schema(space_id, lock)
nexus.ensure_schema(space_id, lock)            // no-op when the lock is unchanged
nexus.install_and_activate(&artifacts, space)  // the ordinary bootstrap
nexus.import_capsule(&capsule, space_id)
nexus.import_capsule_isolated(&capsule, space) // lands in quarantine
nexus.governance()                             // the control plane, host-trusted
nexus.session(auth)                            // an authenticated caller
```

`ensure_schema` exists because every activation mints a new environment version:
a host that unconditionally re-activated its baseline lock would walk the
version forward on each restart, invalidating clients' preconditions and filling
`HISTORY` with schema changes that changed nothing.

---

## 14. What this engine does not do

Reported by `DESCRIBE CAPABILITIES` as structured data with a reason, and
refused rather than answered wrongly:

| Gap                          | Why                                                                        |
| :--------------------------- | :------------------------------------------------------------------------- |
| semantic / hybrid `SEARCH`   | no embedding model; refused rather than silently downgraded to keyword      |
| `SEARCH … AS OF SEQ`         | the index reflects the present; today's matches are not then's              |
| atomic batches               | one transaction across several operations is an engine property a loop lacks |
| Capsule signatures           | nothing is signed, and `VERIFY` says so separately from `valid`             |
| the `restore` import mode    | its point is mapping a source `$self` onto the destination's, which is the one thing an import must never do by resemblance |
| `DESCRIBE TRUST`             | no trust evaluation; an empty trust report reads as "nothing is trusted"    |
| trust / evidence quality     | stages 9 and 10 of the projection; every projection says so in its warnings |
| Space-level retention policy | retention is set and enforced per element, not defaulted by kind or class   |

---

## 15. Testing

```bash
cargo test -p anda_cognitive_nexus --all-features
```

Eleven suites plus a cross-engine conformance harness. **Write end-to-end tests
through the real parser**, not hand-built ASTs: every interesting defect in this
project has been found that way and would have been invisible to a unit test of
the same function — a clause the parser accepts, the engine ignores, and the
receipt reports as success.

`tests/conformance.rs` runs the plain-data fixtures in
[`fixtures/kip-conformance-2.0/`](../fixtures/kip-conformance-2.0/), which exist
so a second engine can run the same cases. Ids are normalized to `C:<1>`;
timestamps, `tx_id`s and scores are dropped.

`tests/governance.rs` includes the §236–§247 threat fixtures — content
self-escalation, policy injection, delegation amplification, actor
impersonation, retraction honesty, search side channels, derived classification,
derived authority, trust self-escalation, revocation, approval and audit
integrity — written as the design writes them: a setup an attacker controls, and
an outcome the engine owes.
